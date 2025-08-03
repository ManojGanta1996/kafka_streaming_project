from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaStructuredStreamingConsumer") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define schema for Kafka message (adjust according to your data)
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

# 3. Read from Kafka topic
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stream-prices") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# 4. Parse JSON from Kafka 'value'
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_value") \
    .withColumn("data", from_json(col("json_value"), schema)) \
    .select("data.*")

# 5. Write output to console
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/tmp/kafka-checkpoint/stream-prices") \
    .start()

query.awaitTermination()
