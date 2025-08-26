from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType

#Define schema for individual coin
coin_schema = StructType([
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("market_cap", LongType(), True),
    StructField("total_volume", DoubleType(), True),
    StructField("high_24h", DoubleType(), True),
    StructField("low_24h", DoubleType(), True),
    StructField("last_updated", StringType(), True)
])

# Define schema for the incoming Kafka messages
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("data", ArrayType(coin_schema), True)
])

# create Spark session
spark = SparkSession.builder \
    .appName("KafkaCryptoConsumer") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()


spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_prices") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# convert Kafka messages from binary to string
json_df = kafka_stream.selectExpr("CAST(value AS STRING) as json")

# parse JSON and apply schema
parsed_stream = json_df.select(from_json(col("json"), schema).alias("parsed"))

# flatten structure
flattened_df = parsed_stream.select(
    col("parsed.timestamp").alias("timestamp"),
    explode(col("parsed.data")).alias("coin")
).select(
    "timestamp",
    col("coin.id").alias("id"),
    col("coin.symbol").alias("symbol"),
    col("coin.current_price").alias("price"),
    col("coin.market_cap"),
    col("coin.total_volume"),
    col("coin.high_24h"),
    col("coin.low_24h"),
    col("coin.last_updated")
)

# define the output path for the parquet files
output_path = "/Users/amarachiordor/Documents/kafka_learn/Marketflow_Analysis/dataframes"
checkpoint_path = "/Users/amarachiordor/Documents/kafka_learn/Marketflow_Analysis/checkpoints"

# write to parquet files
query = flattened_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

query.awaitTermination()
