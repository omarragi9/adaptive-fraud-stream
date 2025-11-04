# file: scripts/spark/stream_score_with_alerts.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, to_timestamp, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

tx_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", StringType(), True),
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

def create_spark():
    return SparkSession.builder.appName("adaptive-fraud-stream-scorer-alerts").getOrCreate()

def main():
    spark = create_spark()
    df_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transactions.raw") \
        .option("startingOffsets", "latest") \
        .load()

    df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")
    df = df_json.select(from_json(col("json_str"), tx_schema).alias("tx")).selectExpr("tx.*")
    df = df.withColumn("ts", to_timestamp(col("timestamp"))).drop("timestamp")

    # scoring
    df_scored = df.withColumn("base_score", expr("CASE WHEN amount IS NULL THEN 0.0 ELSE LEAST(1.0, amount/1000.0) END")) \
                  .withColumn("high_risk_flag", expr("CASE WHEN is_high_risk_country = true THEN 1 ELSE 0 END")) \
                  .withColumn("large_amount_flag", expr("CASE WHEN amount > 700 THEN 1 ELSE 0 END")) \
                  .withColumn("score", expr("ROUND(base_score + high_risk_flag*0.5 + large_amount_flag*0.4, 3)")) \
                  .withColumn("ingest_ts", expr("current_timestamp()"))

    # Persist all scored records to Parquet (same as before)
    parquet_query = df_scored.select("transaction_id","ts","user_id","card_id","amount","currency",
                                     "merchant","device_id","ip","is_high_risk_country",
                                     "base_score","high_risk_flag","large_amount_flag","score","ingest_ts") \
        .writeStream.format("parquet") \
        .option("path", "data/transactions_scored") \
        .option("checkpointLocation", "data/checkpoints/transactions_scored") \
        .outputMode("append") \
        .start()

    # ALERTS: filter suspicious transactions (threshold = 1.0)
    alerts = df_scored.filter("score >= 1.0") \
        .selectExpr("transaction_id","user_id","card_id","amount","currency","merchant","device_id","ip","score","ingest_ts")

    # convert alert struct to JSON string and write to kafka topic transactions.alerts
    alerts_kafka = alerts.withColumn("value", to_json(struct([alerts[x] for x in alerts.columns]))) \
                         .selectExpr("CAST(value AS STRING) as value")

    kafka_query = alerts_kafka.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "transactions.alerts") \
        .option("checkpointLocation", "data/checkpoints/transactions_alerts_kafka") \
        .outputMode("append") \
        .start()

    # also show alerts on console for dev
    console_q = alerts.writeStream.format("console").option("truncate", False).option("numRows", 5).start()

    # wait
    parquet_query.awaitTermination()
    kafka_query.awaitTermination()
    console_q.awaitTermination()

if __name__ == "__main__":
    main()
