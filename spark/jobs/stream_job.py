"""QuakeStream Spark Structured Streaming job.

Reads earthquake events from Kafka, parses the JSON payload, enriches with
timestamps, and writes to a Delta table on MinIO (S3A) using a checkpoint
so the job is restart-safe.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, IntegerType,
)

# --- config from env ---
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "events")
DELTA_PATH = os.environ.get("DELTA_TABLE_PATH", "s3a://lakehouse/quakes")
CHECKPOINT = os.environ.get("CHECKPOINT_PATH", "s3a://lakehouse/_checkpoints/quakes")
TRIGGER_SECONDS = int(os.environ.get("SPARK_TRIGGER_SECONDS", "30"))

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

# --- schema of the flattened records the producer publishes ---
schema = StructType([
    StructField("id", StringType()),
    StructField("mag", DoubleType()),
    StructField("place", StringType()),
    StructField("time", LongType()),        # epoch millis
    StructField("updated", LongType()),     # epoch millis
    StructField("tsunami", IntegerType()),
    StructField("sig", IntegerType()),
    StructField("magType", StringType()),
    StructField("type", StringType()),
    StructField("title", StringType()),
    StructField("url", StringType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("depth_km", DoubleType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("quakestream-delta")
        # Delta
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # S3A -> MinIO
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # keep small-scale local runs light
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw.select(from_json(col("value").cast("string"), schema).alias("d"))
        .select("d.*")
        .withColumn("event_time", to_timestamp(col("time") / 1000))
        .withColumn("ingest_time", current_timestamp())
    )

    query = (
        parsed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start(DELTA_PATH)
    )

    print(f"[quakestream] streaming Kafka '{KAFKA_TOPIC}' -> Delta '{DELTA_PATH}'"
          f" (trigger {TRIGGER_SECONDS}s)")
    query.awaitTermination()


if __name__ == "__main__":
    main()
