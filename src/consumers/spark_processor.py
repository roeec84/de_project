import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, when, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("PricingEngineProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.checkpointLocation", "./checkpoints") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("shop_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_ts", DoubleType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce_events") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("event_ts").cast(TimestampType()))

metrics_df = parsed_df \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"), # 1-min window, updates every 30s
        "product_id", 
        "product_name"
    ) \
    .agg(
        count(when(col("event_type") == "view", True)).alias("total_views"),
        count(when(col("event_type") == "buy", True)).alias("total_buys")
    )

query = metrics_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()