from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Kafka and Spark settings
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"  # Use 'kafka:9092' if running inside Docker
RAW_TOPIC = "raw_api_events"

spark = SparkSession.builder \
    .appName("RealTimeWeatherStreaming") \
    .getOrCreate()

# Schema for incoming JSON
schema = StructType([
    StructField("city", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("temp", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("weather", StringType(), True)
])

# Read from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", RAW_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast("timestamp"))

# Add watermark to handle late data for windowed aggregation
agg_df = raw_df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        col("city"),
        window(col("event_time"), "10 minutes")  # 10-minute tumbling window
    ).agg(
        avg("temp").alias("avg_temp"),
        avg("humidity").alias("avg_humidity")
    )

# Write aggregated data to Parquet (or any sink you want)
query = agg_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "./parquet/agg") \
    .option("checkpointLocation", "./checkpoints/agg") \
    .start()

query.awaitTermination()
