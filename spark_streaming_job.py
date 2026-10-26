import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, current_timestamp, when, lit
from pyspark.sql.functions import sum as _sum, count as _count, max as _max
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, TimestampType, BooleanType
)

# Define the schema for the incoming Kafka data (accommodates Indian CPCB fields & provenance)
SCHEMA = StructType([
    StructField("station_id", StringType(), True),
    StructField("station_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("timestamp", IntegerType(), True),
    StructField("source_timestamp", StringType(), True),
    StructField("pm25", FloatType(), True),
    StructField("pm10", FloatType(), True),
    StructField("no2", FloatType(), True),
    StructField("so2", FloatType(), True),
    StructField("co", FloatType(), True),
    StructField("data_source", StringType(), True),
    StructField("is_synthetic", BooleanType(), True),
    StructField("scenario_id", StringType(), True)
])

# --- KAFKA and MONGODB ATLAS Configuration ---
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "air_quality_events")
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:29092")

# Prioritize MONGODB_URI (standard for Atlas), with fallback to MONGO_URI
MONGO_URI = os.getenv("MONGODB_URI", os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
MONGO_DB = os.getenv("MONGODB_DATABASE", os.getenv("MONGO_DB", "air_quality_db"))
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "aggregated_metrics")
ALERTS_COLLECTION = os.getenv("ALERTS_COLLECTION", "alerts")
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/app/checkpoints")

# --- Spark Packages ---
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
MONGO_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"


def main():
    print(f"Starting Spark Streaming job with master '{SPARK_MASTER}'...")
    print(f"Target MongoDB Database: '{MONGO_DB}' in Atlas / Cloud persistent storage.")

    # Build the SparkSession with Atlas TLS support
    spark = SparkSession.builder \
        .appName("AirQualityStreaming") \
        .master(SPARK_MASTER) \
        .config("spark.mongodb.write.connection.uri", MONGO_URI) \
        .config("spark.jars.packages", f"{KAFKA_PACKAGE},{MONGO_PACKAGE}") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print(f"SparkSession created. Connecting to Kafka at {KAFKA_SERVER}...")

    # 1. Read from Kafka
    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    print("Successfully reading from Kafka topic:", KAFKA_TOPIC)

    # 2. Transform the Data
    json_df = raw_kafka_df.select(
        from_json(col("value").cast("string"), SCHEMA).alias("data")
    ).select("data.*")

    # Convert Unix integer timestamp to TimestampType
    # Watermark set to 2 minutes for late data handling
    events_df = json_df.withColumn(
        "event_timestamp",
        col("timestamp").cast(TimestampType())
    ).withWatermark("event_timestamp", "2 minutes")

    # Data validation: flag out-of-range readings while handling nullable sensor channels
    valid_rule = (
        (col("pm25").isNull() | col("pm25").between(0, 500)) &
        (col("pm10").isNull() | col("pm10").between(0, 600)) &
        (col("no2").isNull() | col("no2").between(0, 400)) &
        (col("so2").isNull() | col("so2").between(0, 100)) &
        (col("co").isNull() | col("co").between(0, 50)) &
        (col("pm25").isNotNull() | col("pm10").isNotNull())
    )
    events_df = events_df.withColumn("is_valid", valid_rule)

    # 3. Perform Aggregations with station granularity & data-quality metrics
    aggregated_df = (
        events_df
        .groupBy(
            window(col("event_timestamp"), "10 minutes", "1 minute"),
            col("city"),
            col("station_name"),
            col("station_id")
        )
        .agg(
            avg(when(col("is_valid") & col("pm25").isNotNull(), col("pm25"))).alias("avg_pm25"),
            avg(when(col("is_valid") & col("pm10").isNotNull(), col("pm10"))).alias("avg_pm10"),
            avg(when(col("is_valid") & col("no2").isNotNull(), col("no2"))).alias("avg_no2"),
            avg(when(col("is_valid") & col("so2").isNotNull(), col("so2"))).alias("avg_so2"),
            avg(when(col("is_valid") & col("co").isNotNull(), col("co"))).alias("avg_co"),
            _max(when(col("is_valid") & col("pm25").isNotNull(), col("pm25"))).alias("max_pm25"),
            _sum(when(col("is_valid"), 1).otherwise(0)).alias("valid_count"),
            _count("*").alias("total_count")
        )
        .withColumn("valid_rate", col("valid_count") / col("total_count"))
        .withColumn("data_source", lit("CPCB historical dataset"))
    )

    # 4. Add Alerting Logic (aligned with Indian CPCB severe/warning levels)
    alerts_df = (
        aggregated_df
        .withColumn(
            "severity",
            when((col("avg_pm25") >= 150) | (col("max_pm25") >= 150), lit("CRITICAL"))
            .when((col("avg_pm25") >= 100) | (col("max_pm25") >= 100), lit("WARNING"))
            .otherwise(lit("NORMAL"))
        )
        .withColumn(
            "alert_reason",
            when(
                (col("avg_pm25") >= 150) | (col("max_pm25") >= 150),
                when((col("avg_pm25") >= 150) & (col("max_pm25") >= 150), lit("avg_pm25>=150 and max_pm25>=150 (Severe)"))
                .when(col("avg_pm25") >= 150, lit("avg_pm25>=150 (Severe)"))
                .otherwise(lit("max_pm25>=150 (Severe Spike)"))
            )
            .when(
                (col("avg_pm25") >= 100) | (col("max_pm25") >= 100),
                when((col("avg_pm25") >= 100) & (col("max_pm25") >= 100), lit("avg_pm25>=100 and max_pm25>=100 (Poor)"))
                .when(col("avg_pm25") >= 100, lit("avg_pm25>=100 (Poor)"))
                .otherwise(lit("max_pm25>=100 (Poor Spike)"))
            )
            .otherwise(lit(None))
        )
        .filter(col("severity") != "NORMAL")
        .withColumn("processing_time", current_timestamp())
    )

    print("Streaming transformations and aggregations defined.")

    # 5. Write to MongoDB Atlas Micro-Batch Sinks
    def write_aggregates_to_mongo(batch_df, epoch_id):
        print(f"--- Writing aggregates batch {epoch_id} to MongoDB Atlas ---")
        (batch_df.write
            .format("mongodb")
            .mode("append")
            .option("database", MONGO_DB)
            .option("collection", MONGO_COLLECTION)
            .save())

    def write_alerts_to_mongo(batch_df, epoch_id):
        print(f"--- Writing alerts batch {epoch_id} to MongoDB Atlas ---")
        (batch_df.write
            .format("mongodb")
            .mode("append")
            .option("database", MONGO_DB)
            .option("collection", ALERTS_COLLECTION)
            .save())

    aggs_checkpoint = os.path.join(CHECKPOINT_DIR, "aggregates")
    alerts_checkpoint = os.path.join(CHECKPOINT_DIR, "alerts")

    q_aggs = (aggregated_df.writeStream
        .foreachBatch(write_aggregates_to_mongo)
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", aggs_checkpoint)
        .start())

    q_alerts = (alerts_df.writeStream
        .foreachBatch(write_alerts_to_mongo)
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", alerts_checkpoint)
        .start())

    print("Writing streams to MongoDB Atlas. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()