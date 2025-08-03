from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, ArrayType, StructField

# ✅ Step 1: Create Spark session
spark = SparkSession.builder \
    .appName("ReliableKafkaConsumer") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ✅ Step 2: Define schema for nested JSON
schema = StructType([
    StructField("user", StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("address", StructType([
            StructField("street", StringType()),
            StructField("city", StringType()),
            StructField("country", StringType())
        ]))
    ])),
    StructField("transaction", StructType([
        StructField("id", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("timestamp", StringType()),
        StructField("items", ArrayType(StructType([
            StructField("item_id", StringType()),
            StructField("description", StringType()),
            StructField("price", DoubleType())
        ])))
    ]))
])

print("✅ Starting Spark Kafka streaming job...")

# ✅ Step 3: Read from Kafka topic
try:
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "stock-prices") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    print("✅ Successfully connected to Kafka topic")
except Exception as e:
    print(f"❌ Failed to load Kafka source: {e}")
    spark.stop()
    exit(1)

# ✅ Step 4: Parse and flatten the JSON message
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_value") \
    .withColumn("data", from_json(col("json_value"), schema)) \
    .select("data.*")

# ✅ Step 5: Stream parsed data to console every 2 seconds
query = df_parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime="2 seconds") \
    .option("truncate", False) \
    .option("checkpointLocation", "/tmp/kafka-checkpoint/stock-prices") \
    .start()

query.awaitTermination()
