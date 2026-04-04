from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, expr, window, date_format,
    approx_count_distinct, sum, when, count
)

spark = (
    SparkSession.builder
    .appName("BiodiversityKafkaStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
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

parsed = (
    df.selectExpr(
        "topic",
        "timestamp",
        "CAST(value AS STRING) as json_str"
    )
    .selectExpr(
        "topic",
        "timestamp",
        "lower(get_json_object(json_str, '$.kingdom')) as kingdom",
        "lower(get_json_object(json_str, '$.scientificname')) as scientific_name"
    )
    .filter(col("kingdom").isNotNull() & col("scientific_name").isNotNull())
)

base_out = "/users/Jiangwei/6761/output"
base_ckpt = "/users/Jiangwei/6761/checkpoints"

source_counts = (
    parsed
    .withWatermark("timestamp", "2 minutes")
    .groupBy(window(col("timestamp"), "2 minutes"), col("topic"))
    .agg(count("*").alias("record_count"))
    .select(
        date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("window_start"),
        date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").alias("window_end"),
        col("topic").alias("source"),
        col("record_count")
    )
)

kingdom_counts = (
    parsed
    .withWatermark("timestamp", "2 minutes")
    .groupBy(window(col("timestamp"), "2 minutes"))
    .agg(
        sum(when(col("kingdom") == "plantae", 1).otherwise(0)).alias("plant_count"),
        sum(when(col("kingdom") == "animalia", 1).otherwise(0)).alias("animal_count"),
        sum(when(col("kingdom") == "fungi", 1).otherwise(0)).alias("fungi_count"),
    )
    .select(
        date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("window_start"),
        date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").alias("window_end"),
        col("plant_count"),
        col("animal_count"),
        col("fungi_count")
    )
)

species_counts = (
    parsed
    .withWatermark("timestamp", "2 minutes")
    .groupBy(window(col("timestamp"), "2 minutes"))
    .agg(
        approx_count_distinct("scientific_name").alias("total_species")
    )
    .select(
        date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("window_start"),
        date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").alias("window_end"),
        col("total_species")
    )
)

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
