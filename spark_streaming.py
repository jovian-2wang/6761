from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lower, coalesce, trim
from pyspark.sql.types import StructType, StructField, StringType

spark = (
    SparkSession.builder
    .appName("BiodiversityKafkaRawIngest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("dwc:kingdom", StringType(), True),
    StructField("kingdom", StringType(), True),
    StructField("dwc:scientificName", StringType(), True),
    StructField("scientificname", StringType(), True),
])

raw = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "node-0.project-jw.ufl-eel6761-sp26-pg0.wisc.cloudlab.us:9092,"
        "node-1.project-jw.ufl-eel6761-sp26-pg0.wisc.cloudlab.us:9092,"
        "node-2.project-jw.ufl-eel6761-sp26-pg0.wisc.cloudlab.us:9092"
    )
    .option("subscribe", "idigbio,gbif,obis")
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    raw.selectExpr(
        "CAST(value AS STRING) as json_str",
        "topic",
        "timestamp"
    )
    .withColumn("data", from_json(col("json_str"), schema))
)

cleaned = (
    parsed.select(
        col("topic").alias("source"),
        col("timestamp"),
        trim(lower(coalesce(col("data.`dwc:kingdom`"), col("data.kingdom")))).alias("kingdom"),
        trim(lower(coalesce(col("data.`dwc:scientificName`"), col("data.scientificname")))).alias("scientific_name")
    )
    .filter(
        col("kingdom").isNotNull() | col("scientific_name").isNotNull()
    )
)

query = (
    cleaned.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "/users/Jiangwei/6761/raw_stream")
    .option("checkpointLocation", "/users/Jiangwei/6761/raw_checkpoint")
    .start()
)

query.awaitTermination()
