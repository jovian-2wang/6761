import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    window,
    count,
    countDistinct,
    when,
    date_format,
)


RAW_INPUT_PATH = "/users/Jiangwei/6761/raw_stream"
OUTPUT_DIR = "/users/Jiangwei/6761/output_final"


spark = (
    SparkSession.builder
    .appName("BiodiversityWindowAggregationFinal")
    .config("spark.sql.shuffle.partitions", "6")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


if not os.path.exists(RAW_INPUT_PATH):
    raise FileNotFoundError(f"Missing raw input path: {RAW_INPUT_PATH}")


df = spark.read.parquet(RAW_INPUT_PATH)

cleaned = (
    df.select(
        col("source"),
        col("timestamp"),
        col("kingdom"),
        col("scientific_name"),
    )
    .filter(col("source").isNotNull())
)

per_source = (
    cleaned.groupBy(
        window(col("timestamp"), "2 minutes"),
        col("source")
    )
    .agg(
        count(when(col("kingdom") == "plantae", True)).alias("plant_records"),
        count(when(col("kingdom") == "animalia", True)).alias("animal_records"),
        count(when(col("kingdom") == "fungi", True)).alias("fungi_records"),
        countDistinct("scientific_name").alias("unique_species"),
        count("*").alias("total_records"),
    )
    .select(
        date_format(col("window.start"), "HH:mm").alias("window_start"),
        date_format(col("window.end"), "HH:mm").alias("window_end"),
        col("source"),
        col("plant_records"),
        col("animal_records"),
        col("fungi_records"),
        col("unique_species"),
        col("total_records"),
    )
    .orderBy("window_start", "source")
)

if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Save Spark output directory
per_source.coalesce(1).write.mode("overwrite").option("header", True).csv(
    f"{OUTPUT_DIR}/per_source_window_counts"
)

# Also save easy-to-read local CSV files
pdf = per_source.toPandas()
pdf.to_csv(f"{OUTPUT_DIR}/per_source_window_counts.csv", index=False)

for source in ["idigbio", "gbif", "obis"]:
    sub = pdf[pdf["source"] == source].copy()
    sub.to_csv(f"{OUTPUT_DIR}/{source}_table.csv", index=False)

print("Saved outputs:")
print(f"{OUTPUT_DIR}/per_source_window_counts.csv")
print(f"{OUTPUT_DIR}/idigbio_table.csv")
print(f"{OUTPUT_DIR}/gbif_table.csv")
print(f"{OUTPUT_DIR}/obis_table.csv")

spark.stop()
