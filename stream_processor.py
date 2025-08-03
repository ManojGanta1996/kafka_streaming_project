from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StringType, FloatType, TimestampType

# 1. Define schema for stock data
schema = StructType() \
    .add("ticker", StringType()) \
    .add("open", FloatType()) \
    .add("high", FloatType()) \
    .add("low", FloatType()) \
    .add("close", FloatType()) \
    .add("volume", FloatType()) \
    .add("timestamp", TimestampType())

# 2. Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaStockStreamProcessor") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 3. Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock-prices") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Extract and parse JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_value") \
    .withColumn("data", from_json(col("json_value"), schema)) \
    .select("data.*")

# 5. Add watermark & deduplication logic
df_clean = df_parsed \
    .withWatermark("timestamp", "2 minutes") \
    .dropDuplicates(["ticker", "timestamp"])

# 6. Write to Delta Lake
df_clean.writeStream \
    .format("delta") \
    .outputMode("append") \
    .partitionBy("ticker") \
    .option("checkpointLocation", "/tmp/delta/stock_streaming_ckpt") \
    .option("path", "/tmp/delta/stock_data") \
    .start() \
    .awaitTermination()
