import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, StringType


KAFKA_BOOTSTRAP_SERVERS = (
    "hp124.utah.cloudlab.us:9092,"
    "hp138.utah.cloudlab.us:9092,"
    "hp149.utah.cloudlab.us:9092"
)

RAW_OUTPUT_PATH = "/users/Jiangwei/6761/raw_stream"
CHECKPOINT_PATH = "/users/Jiangwei/6761/raw_checkpoint"


spark = (
    SparkSession.builder
    .appName("BiodiversityKafkaRawIngest")
    .config("spark.sql.shuffle.partitions", "6")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


parsed_schema = StructType([
    StructField("kingdom", StringType(), True),
    StructField("scientific_name", StringType(), True),
])


def clean_value(value):
    if value is None:
        return None
    value = str(value).strip()
    if value == "" or value.lower() in {"null", "none", "nan"}:
        return None
    return value.lower()


def pick_field(record, possible_keys):
    """
    Robust field picker:
    - ignores case
    - supports keys like dwc:scientificName
    - supports scientificName / scientificname / scientific_name
    """
    if not isinstance(record, dict):
        return None

    lowered = {}
    for k, v in record.items():
        lowered[str(k).lower()] = v

    for key in possible_keys:
        key_lower = key.lower()
        if key_lower in lowered:
            value = clean_value(lowered[key_lower])
            if value is not None:
                return value

    return None


def parse_biodiversity_record(json_str):
    try:
        record = json.loads(json_str)
    except Exception:
        return (None, None)

    if not isinstance(record, dict):
        return (None, None)

    # Some APIs may wrap useful fields under "data".
    candidates = [record]
    if isinstance(record.get("data"), dict):
        candidates.append(record.get("data"))

    kingdom_keys = [
        "dwc:kingdom",
        "kingdom",
        "taxonKingdom",
        "taxon_kingdom",
    ]

    scientific_name_keys = [
        "dwc:scientificName",
        "dwc:scientificname",
        "scientificName",
        "scientificname",
        "scientific_name",
        "acceptedScientificName",
        "acceptedscientificname",
        "canonicalName",
        "canonicalname",
        "taxonName",
        "taxonname",
        "species",
    ]

    kingdom = None
    scientific_name = None

    for candidate in candidates:
        if kingdom is None:
            kingdom = pick_field(candidate, kingdom_keys)
        if scientific_name is None:
            scientific_name = pick_field(candidate, scientific_name_keys)

    return (kingdom, scientific_name)


parse_record_udf = udf(parse_biodiversity_record, parsed_schema)


raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", "idigbio,gbif,obis")
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    raw.selectExpr(
        "CAST(value AS STRING) AS json_str",
        "topic",
        "timestamp"
    )
    .withColumn("parsed", parse_record_udf(col("json_str")))
)

cleaned = (
    parsed.select(
        col("topic").alias("source"),
        col("timestamp"),
        col("parsed.kingdom").alias("kingdom"),
        col("parsed.scientific_name").alias("scientific_name")
    )
    .filter(
        col("kingdom").isNotNull() | col("scientific_name").isNotNull()
    )
)

query = (
    cleaned.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", RAW_OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

query.awaitTermination()
