from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, lower, coalesce, window, count, approx_count_distinct,
    when, lit, concat_ws, date_format
)
from pyspark.sql.types import StructType, StructField, StringType

# -----------------------------
# Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("BiodiversityKafkaStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Kafka input
# -----------------------------
raw = (
    spark.readStream
    .format("kafka")
    .option(
    "kafka.bootstrap.servers",
    "node-0.project-jw.ufl-eel6761-sp26-pg0.wisc.cloudlab.us:9092,"
    "node-1.project-jw.ufl-eel6761-sp26-pg0.wisc.cloudlab.us:9092,"
    "node-2.project-jw.ufl-eel6761-sp26-pg0.wisc.cloudlab.us:9092"
    )
    .option("subscribe", "gbif")
    .option("startingOffsets", "latest")
    .load()
)


schema = StructType([
    StructField("dwc:kingdom", StringType(), True),
    StructField("kingdom", StringType(), True),
    StructField("scientificname", StringType(), True),
])



parsed = (
    raw.selectExpr(
        "CAST(value AS STRING) as json_str",
        "topic",
        "timestamp"
    )
    .withColumn("data", from_json(col("json_str"), schema))
)

cleaned = parsed.select(
    col("topic").alias("source"),
    col("timestamp"),
    lower(
        coalesce(
            col("data.`dwc:kingdom`"),
            col("data.kingdom")
        )
    ).alias("kingdom"),
    lower(
        col("data.scientificname")
    ).alias("scientific_name")
)
cleaned = cleaned.withWatermark("timestamp", "2 minutes")

cleaned = cleaned.filter(
    col("kingdom").isNotNull() | col("scientific_name").isNotNull()
)
species_counts = (
    cleaned.groupBy(
        window(col("timestamp"), "2 minutes")
    )
    .agg(
        approx_count_distinct("scientific_name").alias("total_species")
    )
    .select(
        date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("window_start"),
        date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").alias("window_end"),
        col("total_species")
    )
)
# -----------------------------
# 1) Source counts per 2-minute window
# -----------------------------
source_counts = (
    cleaned.groupBy(
        window(col("timestamp"), "2 minutes"),
        col("source")
    )
    .agg(
        count("*").alias("record_count")
    )
    .select(
        date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("window_start"),
        date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").alias("window_end"),
        col("source"),
        col("record_count")
    )
)

# -----------------------------
# 2) Kingdom counts per 2-minute window
# -----------------------------
kingdom_counts = (
    cleaned.groupBy(
        window(col("timestamp"), "2 minutes")
    )
    .agg(
        count(when(col("kingdom") == "plantae", True)).alias("plant_count"),
        count(when(col("kingdom") == "animalia", True)).alias("animal_count"),
        count(when(col("kingdom") == "fungi", True)).alias("fungi_count")
    )
    .select(
        date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("window_start"),
        date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").alias("window_end"),
        col("plant_count"),
        col("animal_count"),
        col("fungi_count")
    )
)



# -----------------------------
# Output paths
# -----------------------------
base_out = "file:///users/Jiangwei/6761/output"
base_ckpt = "file:///users/Jiangwei/6761/checkpoints"

q1 = (
    source_counts.writeStream
    .outputMode("append")
    .format("csv")
    .option("path", f"{base_out}/source_counts")
    .option("checkpointLocation", f"{base_ckpt}/source_counts")
    .option("header", "true")
    .trigger(processingTime="2 minutes")
    .start()
)

q2 = (
    kingdom_counts.writeStream
    .outputMode("append")
    .format("csv")
    .option("path", f"{base_out}/kingdom_counts")
    .option("checkpointLocation", f"{base_ckpt}/kingdom_counts")
    .option("header", "true")
    .trigger(processingTime="2 minutes")
    .start()
)

q3 = (
    species_counts.writeStream
    .outputMode("append")
    .format("csv")
    .option("path", f"{base_out}/species_counts")
    .option("checkpointLocation", f"{base_ckpt}/species_counts")
    .option("header", "true")
    .trigger(processingTime="2 minutes")
    .start()
)

spark.streams.awaitAnyTermination()

