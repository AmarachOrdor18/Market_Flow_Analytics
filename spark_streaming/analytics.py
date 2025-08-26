import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, avg, stddev, lag, round, when
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import traceback
import pyspark.sql.functions as F

# ================================
# Spark Session
# ================================
spark = SparkSession.builder \
    .appName("CryptoMetricsCalculator") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ================================
# PostgreSQL connection
# ================================
jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
main_table = "crypto_table"
gainers_table = "top_5_gainers"
losers_table = "top_5_losers"
db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# ================================
# Main Loop
# ================================
while True:
    try:
        print("Starting data processing...")

        # Load recent parquet files and drop unused columns
        df = spark.read.parquet("/Users/amarachiordor/Documents/kafka_learn/Marketflow_Analysis/dataframes")
        df = df.drop("market_cap", "total_volume", "high_24h", "low_24h", "last_updated")

        # Explicit type casting
        df = df.withColumn("timestamp", to_timestamp("timestamp")) \
               .withColumn("price", col("price").cast(DoubleType()))
        
        # Filter data from last 7 minutes
        latest_data = df.filter(col("timestamp") >= F.current_timestamp() - F.expr("INTERVAL 7 MINUTES"))
        row_count = latest_data.count()

        if row_count == 0:
            print("No new data available. Waiting for the next cycle...")
        else:
            # Window partition by coin
            coin_window = Window.partitionBy("id").orderBy("timestamp")

            # Price changes (1 min, 5 min)
            latest_data = latest_data \
                .withColumn("price_change_1m", lag("price", 1).over(coin_window)) \
                .withColumn("price_change_5m", lag("price", 10).over(coin_window)) \
                .withColumn(
                    "change_1min",
                    when(
                        col("price_change_1m").isNull(), None
                    ).otherwise(round((col("price") - col("price_change_1m")) / col("price_change_1m") * 100, 2))
                ) \
                .withColumn(
                    "change_5min",
                    when(
                        col("price_change_5m").isNull(), None
                    ).otherwise(round((col("price") - col("price_change_5m")) / col("price_change_5m") * 100, 2))
                ) \
                .drop("price_change_1m", "price_change_5m")
            
            # SMA and EMA (Rolling Windows)
            latest_data = latest_data \
                .withColumn("SMA", avg("price").over(coin_window.rowsBetween(-4, 0))) \
                .withColumn("EMA", avg("price").over(coin_window.rowsBetween(-2, 0)))
            
            # Volatility
            latest_data = latest_data \
                .withColumn("volatility", stddev("price").over(coin_window.rowsBetween(-4, 0)))
            
            # ================================
            # Snapshot: latest row per coin
            # ================================
            latest_per_coin = latest_data.withColumn(
                "rn", F.row_number().over(Window.partitionBy("id").orderBy(F.col("timestamp").desc()))
            ).filter("rn = 1").drop("rn")

            # ================================
            # Rank gainers and losers
            # ================================
            global_rank_gain = Window.orderBy(col("change_5min").desc())
            gainers = latest_per_coin \
                .withColumn("rank", F.row_number().over(global_rank_gain)) \
                .filter(col("rank") <= 5) \
                .select("rank", "id", "symbol")

            global_rank_loss = Window.orderBy(col("change_5min").asc())
            losers = latest_per_coin \
                .withColumn("rank", F.row_number().over(global_rank_loss)) \
                .filter(col("rank") <= 5) \
                .select("rank", "id", "symbol")

            # ================================
            # Show Output
            # ================================
            print("\n == All Metrics (Latest Snapshot) == ")
            latest_data.select("timestamp", "id", "symbol", "price", "change_1min", "change_5min", "SMA", "EMA", "volatility").orderBy("timestamp").show(truncate=False)

            print("\n == Snapshot used for ranking ==")
            latest_per_coin.select("timestamp","id","symbol","price").show(truncate=False)

            print("\n == Top 5 Gainers (Last 5 Minutes) == ")
            gainers.show(truncate=False)

            print("\n == Top 5 Losers (Last 5 Minutes) == ")
            losers.show(truncate=False)

            # ================================
            # Write to PostgreSQL
            # ================================
            # Ensure lowercase column names
            for col_name in latest_data.columns:
                if col_name.lower() != col_name:
                    latest_data = latest_data.withColumnRenamed(col_name, col_name.lower())

            # Append all rows to main crypto_table
            latest_data.write.jdbc(
                url=jdbc_url,
                table=main_table,
                mode="append",
                properties=db_properties
            )

            # Overwrite with only the latest snapshot for gainers/losers
            gainers.write.jdbc(
                url=jdbc_url,
                table=gainers_table,
                mode="overwrite",
                properties=db_properties
            )

            losers.write.jdbc(
                url=jdbc_url,
                table=losers_table,
                mode="overwrite",
                properties=db_properties
            )

            print("Data processing and storage completed successfully.")

        spark.catalog.clearCache()

    except Exception as e:
        print("Error:", e)
        traceback.print_exc()

    # Sleep before next cycle
    total_seconds = 360
    print(f"Waiting for {total_seconds//60} minutes before the next cycle...")
    for remaining in range(total_seconds, 0, -1):
        minutes, seconds = divmod(remaining, 60)
        print(f"next run in: {minutes} minutes, {seconds} seconds")
        time.sleep(1)
    print("\n")
