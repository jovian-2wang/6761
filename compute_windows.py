from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, when, approx_count_distinct, date_format
)

spark = (
    SparkSession.builder
    .appName("BiodiversityWindowAggregation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("/users/Jiangwei/6761/raw_stream")

source_counts = (
    df.groupBy(
        window(col("timestamp"), "2 minutes"),
        col("source")
    )
    .agg(count("*").alias("record_count"))
    .select(
        date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("window_start"),
        date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").alias("window_end"),
        col("source"),
        col("record_count")
    )
)

kingdom_counts = (
    df.groupBy(window(col("timestamp"), "2 minutes"))
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

species_counts = (
    df.filter(col("scientific_name").isNotNull())
    .groupBy(window(col("timestamp"), "2 minutes"))
    .agg(approx_count_distinct("scientific_name").alias("species"))
    .select(
        date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("window_start"),
        date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").alias("window_end"),
        col("species")
    )
)

source_counts.coalesce(1).write.mode("overwrite").option("header", True).csv("/users/Jiangwei/6761/output/source_counts")
kingdom_counts.coalesce(1).write.mode("overwrite").option("header", True).csv("/users/Jiangwei/6761/output/kingdom_counts")
species_counts.coalesce(1).write.mode("overwrite").option("header", True).csv("/users/Jiangwei/6761/output/species_counts")

spark.stop()
