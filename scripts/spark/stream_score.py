# file: scripts/spark/stream_score.py
"""
Spark Structured Streaming consumer + rule-based scorer.
Run with spark-submit (see run command in docs).
Reads from Kafka topic transactions.raw, expects JSON value (UTF-8).
Outputs:
 - console (sample)
 - parquet files under data/transactions_scored/
"""
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, lit, udf, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType

# Schema that matches generator
tx_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", StringType(), True),  # parse below
    StructField("user_id", StringType(), True),
    StructField("card_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("is_high_risk_country", BooleanType(), True),
    StructField("labels", StringType(), True)
])

def create_spark_session():
    return SparkSession.builder \
        .appName("adaptive-fraud-stream-scorer") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    # read from Kafka
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transactions.raw") \
        .option("startingOffsets", "latest") \
        .load()

    # kafka value is binary; convert to string
    df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")

    # parse JSON
    df_parsed = df_json.select(from_json(col("json_str"), tx_schema).alias("tx")) \
        .selectExpr("tx.*")

    # convert timestamp string to TimestampType
    df_parsed = df_parsed.withColumn("ts", to_timestamp(col("timestamp"))).drop("timestamp")

    # simple rule-based scoring function:
    # base_score = min(1.0, amount / 1000)
    # + 0.5 if is_high_risk_country true
    # + 0.4 if amount > 700
    df_scored = df_parsed.withColumn(
        "base_score", expr("CASE WHEN amount IS NULL THEN 0.0 ELSE LEAST(1.0, amount/1000.0) END")
    ).withColumn(
        "high_risk_flag", expr("CASE WHEN is_high_risk_country = true THEN 1 ELSE 0 END")
    ).withColumn(
        "large_amount_flag", expr("CASE WHEN amount > 700 THEN 1 ELSE 0 END")
    ).withColumn(
        "score",
        expr("ROUND(base_score + high_risk_flag*0.5 + large_amount_flag*0.4, 3)")
    ).withColumn(
        "ingest_ts", expr("current_timestamp()")
    ).select(
        "transaction_id", "ts", "user_id", "card_id", "amount", "currency",
        "merchant", "device_id", "ip", "is_high_risk_country",
        "base_score", "high_risk_flag", "large_amount_flag", "score", "ingest_ts"
    )

    # console sink for quick debug — prints a small sample to stdout
    console_query = df_scored.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 5) \
        .start()

    # parquet sink (micro-batch append) for persisted results
    parquet_query = df_scored.writeStream \
        .format("parquet") \
        .option("path", "data/transactions_scored") \
        .option("checkpointLocation", "data/checkpoints/transactions_scored") \
        .outputMode("append") \
        .start()

    # await termination
    console_query.awaitTermination()
    parquet_query.awaitTermination()

if __name__ == "__main__":
    main()
