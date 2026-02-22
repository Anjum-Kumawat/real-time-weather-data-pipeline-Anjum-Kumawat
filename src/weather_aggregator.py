from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import os

# --------------------------
# Config - Absolute Paths for Reliability
# --------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "raw_api_events"
OUTPUT_DIR = "/home/vboxuser/realtime-weather-pipeline/output/aggregated/"
CHECKPOINT_DIR = "/home/vboxuser/realtime-weather-pipeline/checkpoints/agg"

# Ensure directories exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

# --------------------------
# Spark Session
# --------------------------
spark = SparkSession.builder \
    .appName("WeatherAggregator") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --------------------------
# Define schema for incoming JSON
# --------------------------
schema = StructType([
    StructField("city", StringType(), True),
    StructField("timestamp", LongType(), True),  # epoch seconds
    StructField("temp", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("weather", StringType(), True)
])

# --------------------------
# Read stream from Kafka
# --------------------------
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Kafka value is bytes -> convert to string
df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")

# Parse JSON
df_parsed = df_json.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Convert epoch timestamp (seconds) to Spark Timestamp type
df_parsed = df_parsed.withColumn("event_time", col("timestamp").cast("timestamp"))

# --------------------------
# Watermarking & Aggregation
# --------------------------
df_agg = df_parsed \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        col("city"),
        window(col("event_time"), "1 minute", "1 minute")
    ).agg(
        avg("temp").alias("avg_temp"),
        avg("humidity").alias("avg_humidity")
    ).select(
        col("city"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temp"),
        col("avg_humidity")
    )

# --------------------------
# Write stream using foreachBatch
# --------------------------
# This function solves the "Parquet does not support Complete mode" issue
def save_to_parquet(batch_df, batch_id):
    """
    Overwrites the output directory with the latest aggregated results
    so the Streamlit dashboard always sees the current state.
    """
    batch_df.write.mode("overwrite").parquet(OUTPUT_DIR)
    print(f">>> Batch {batch_id} processed. Updated files in {OUTPUT_DIR}")

query = df_agg.writeStream \
    .outputMode("complete") \
    .foreachBatch(save_to_parquet) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .trigger(processingTime="10 seconds") \
    .start()

print(f"Streaming started. Sending updates to: {OUTPUT_DIR}")
query.awaitTermination()