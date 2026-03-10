"""Structured Streaming ingestion from Redpanda into Bronze clickstream Parquet.

This module supports both:
1. continuous streaming mode for long-running ingestion
2. bounded micro-batch mode for Airflow-triggered Bronze ingestion
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, from_json, to_timestamp
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
import structlog

from config.settings import AppSettings, get_settings
from config.spark import SparkSessionBuilder


LOGGER = structlog.get_logger(__name__)


def get_clickstream_schema() -> StructType:
    """Return the Bronze clickstream event schema.

    Returns:
        Spark StructType for raw clickstream event parsing.
    """
    return StructType(
        [
            StructField("event_id", StringType(), nullable=False),
            StructField("session_id", StringType(), nullable=False),
            StructField("user_id", StringType(), nullable=False),
            StructField("event_type", StringType(), nullable=False),
            StructField("event_timestamp", StringType(), nullable=False),
            StructField("ingestion_timestamp", StringType(), nullable=False),
            StructField("page_url", StringType(), nullable=False),
            StructField("product_id", IntegerType(), nullable=True),
            StructField("category_name", StringType(), nullable=True),
            StructField("country_code", StringType(), nullable=False),
            StructField("device_type", StringType(), nullable=False),
            StructField("traffic_source", StringType(), nullable=False),
            StructField("event_position", IntegerType(), nullable=False),
            StructField("is_late_event", BooleanType(), nullable=False),
            StructField("original_event_id", StringType(), nullable=True),
        ]
    )


def read_kafka_stream(spark: SparkSession, settings: AppSettings) -> DataFrame:
    """Read the Redpanda topic as a structured stream.

    For Airflow micro-batch ingestion, using `earliest` is correct because
    Spark will use checkpointed offsets after the first successful run.

    Args:
        spark: Active Spark session.
        settings: Application settings.

    Returns:
        Raw Kafka stream dataframe.
    """
    LOGGER.info(
        "bronze_clickstream_read_start",
        topic=settings.kafka.clickstream_topic,
        brokers=settings.kafka.bootstrap_servers,
    )

    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", settings.kafka.clickstream_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_clickstream_events(raw_stream: DataFrame) -> DataFrame:
    """Parse Kafka JSON payload into typed event columns.

    Args:
        raw_stream: Raw Kafka dataframe.

    Returns:
        Parsed dataframe with business columns.
    """
    schema = get_clickstream_schema()

    return (
        raw_stream.selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "CAST(value AS STRING) AS kafka_value",
            "timestamp AS kafka_message_timestamp",
        )
        .withColumn("parsed_json", from_json(col("kafka_value"), schema))
        .select(
            "kafka_key",
            "kafka_message_timestamp",
            col("parsed_json.*"),
        )
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
        .withColumn("ingestion_timestamp", to_timestamp(col("ingestion_timestamp")))
        .withColumn("bronze_ingested_at", current_timestamp())
        .withColumn("partition_date", date_format(col("event_timestamp"), "yyyy-MM-dd"))
    )


def apply_event_time_watermark(parsed_stream: DataFrame) -> DataFrame:
    """Apply a 10-minute event-time watermark.

    Args:
        parsed_stream: Parsed clickstream dataframe.

    Returns:
        Watermarked dataframe.
    """
    return parsed_stream.withWatermark("event_timestamp", "10 minutes")


def get_bronze_output_path(settings: AppSettings) -> str:
    """Return Bronze output path for clickstream parquet.

    Args:
        settings: Application settings.

    Returns:
        S3A output path.
    """
    return f"{settings.minio.bronze_uri}/clickstream/events"


def get_continuous_checkpoint_path(settings: AppSettings) -> str:
    """Return checkpoint path for continuous clickstream ingestion.

    Args:
        settings: Application settings.

    Returns:
        S3A checkpoint path.
    """
    return f"{settings.minio.bronze_uri}/checkpoints/clickstream_bronze_continuous"


def get_airflow_batch_checkpoint_path(settings: AppSettings) -> str:
    """Return checkpoint path for Airflow micro-batch clickstream ingestion.

    Args:
        settings: Application settings.

    Returns:
        S3A checkpoint path.
    """
    return f"{settings.minio.bronze_uri}/checkpoints/clickstream_bronze_airflow"


def write_bronze_stream_continuous(
    stream_dataframe: DataFrame,
    settings: AppSettings,
) -> StreamingQuery:
    """Write the structured stream continuously to Bronze partitioned Parquet.

    Args:
        stream_dataframe: Parsed and watermarked clickstream dataframe.
        settings: Application settings.

    Returns:
        Active StreamingQuery instance.
    """
    output_path = get_bronze_output_path(settings)
    checkpoint_path = get_continuous_checkpoint_path(settings)

    LOGGER.info(
        "bronze_clickstream_write_start_continuous",
        output_path=output_path,
        checkpoint_path=checkpoint_path,
    )

    return (
        stream_dataframe.writeStream.format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("partition_date")
        .trigger(processingTime="30 seconds")
        .start()
    )


def write_bronze_stream_available_now(
    stream_dataframe: DataFrame,
    settings: AppSettings,
) -> StreamingQuery:
    """Write a bounded micro-batch from Redpanda to Bronze for Airflow.

    Args:
        stream_dataframe: Parsed and watermarked clickstream dataframe.
        settings: Application settings.

    Returns:
        Active StreamingQuery instance.
    """
    output_path = get_bronze_output_path(settings)
    checkpoint_path = get_airflow_batch_checkpoint_path(settings)

    LOGGER.info(
        "bronze_clickstream_write_start_available_now",
        output_path=output_path,
        checkpoint_path=checkpoint_path,
    )

    return (
        stream_dataframe.writeStream.format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("partition_date")
        .trigger(availableNow=True)
        .start()
    )


def start_clickstream_bronze_stream(
    settings: AppSettings | None = None,
) -> StreamingQuery:
    """Start the Bronze clickstream streaming ingestion query in continuous mode.

    Args:
        settings: Optional application settings.

    Returns:
        Running StreamingQuery.
    """
    active_settings = settings or get_settings()
    spark = SparkSessionBuilder(settings=active_settings).build(
        app_name="bronze_clickstream_ingestion"
    )

    raw_stream = read_kafka_stream(spark=spark, settings=active_settings)
    parsed_stream = parse_clickstream_events(raw_stream=raw_stream)
    watermarked_stream = apply_event_time_watermark(parsed_stream=parsed_stream)

    query = write_bronze_stream_continuous(
        stream_dataframe=watermarked_stream,
        settings=active_settings,
    )

    LOGGER.info(
        "bronze_clickstream_stream_started",
        query_id=str(query.id),
        topic=active_settings.kafka.clickstream_topic,
    )

    return query


def ingest_clickstream_bronze_batch(
    settings: AppSettings | None = None,
) -> dict[str, object]:
    """Run one bounded Bronze ingestion batch from Redpanda into MinIO.

    This is the Airflow-friendly Bronze ingestion entrypoint.

    Args:
        settings: Optional application settings.

    Returns:
        Result payload including target paths and rows ingested.
    """
    active_settings = settings or get_settings()
    spark = SparkSessionBuilder(settings=active_settings).build(
        app_name="bronze_clickstream_ingestion_batch"
    )

    try:
        raw_stream = read_kafka_stream(spark=spark, settings=active_settings)
        parsed_stream = parse_clickstream_events(raw_stream=raw_stream)
        watermarked_stream = apply_event_time_watermark(parsed_stream=parsed_stream)

        query = write_bronze_stream_available_now(
            stream_dataframe=watermarked_stream,
            settings=active_settings,
        )
        query.awaitTermination()

        progress = query.lastProgress if query.lastProgress is not None else {}
        num_input_rows = int(progress.get("numInputRows", 0)) if progress else 0

        if num_input_rows == 0:
            result = {
                "topic": active_settings.kafka.clickstream_topic,
                "bronze_path": get_bronze_output_path(active_settings),
                "checkpoint_path": get_airflow_batch_checkpoint_path(active_settings),
                "query_id": str(query.id),
                "progress": progress,
                "rows_ingested": 0,
                "status": "no_new_data",
            }
            LOGGER.warning("bronze_clickstream_batch_no_new_data", **result)
            return result

        result = {
            "topic": active_settings.kafka.clickstream_topic,
            "bronze_path": get_bronze_output_path(active_settings),
            "checkpoint_path": get_airflow_batch_checkpoint_path(active_settings),
            "query_id": str(query.id),
            "progress": progress,
            "rows_ingested": num_input_rows,
            "status": "ingested",
        }

        LOGGER.info("bronze_clickstream_batch_complete", **result)
        return result
    finally:
        spark.stop()


def run_clickstream_bronze_stream(settings: AppSettings | None = None) -> None:
    """Start and await the clickstream Bronze stream indefinitely.

    Args:
        settings: Optional application settings.
    """
    query = start_clickstream_bronze_stream(settings=settings)
    query.awaitTermination()


if __name__ == "__main__":
    run_clickstream_bronze_stream(settings=get_settings())