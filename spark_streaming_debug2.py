from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, coalesce, lower
from pyspark.sql.types import StructType, StructField, StringType

spark = (
    SparkSession.builder
    .appName("BiodiversityKafkaDebug")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("dwc:kingdom", StringType(), True),
    StructField("kingdom", StringType(), True),
    StructField("scientificname", StringType(), True),
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",
            "node-0.project-jw.ufl-eel6761-sp26-pg0.wisc.cloudlab.us:9092,"
            "node-1.project-jw.ufl-eel6761-sp26-pg0.wisc.cloudlab.us:9092,"
            "node-2.project-jw.ufl-eel6761-sp26-pg0.wisc.cloudlab.us:9092")
    .option("subscribe", "gbif")
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    df.selectExpr("CAST(value AS STRING) as json_str", "topic", "timestamp")
      .select(
          col("topic"),
          col("timestamp"),
          from_json(col("json_str"), schema).alias("data"),
          col("json_str")
      )
      .select(
          col("topic"),
          col("timestamp"),
          lower(coalesce(col("data.`dwc:kingdom`"), col("data.kingdom"))).alias("kingdom"),
          lower(col("data.scientificname")).alias("scientific_name"),
          col("json_str")
      )
)

debug_query = (
    parsed.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 20)
    .start()
)

spark.streams.awaitAnyTermination()
