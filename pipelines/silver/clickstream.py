"""Silver transformations for Bronze clickstream data."""

from __future__ import annotations

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import AppSettings, get_settings
from config.spark import SparkSessionBuilder


LOGGER = structlog.get_logger(__name__)


def get_bronze_clickstream_path(settings: AppSettings) -> str:
    """Return Bronze clickstream parquet path.

    Args:
        settings: Application settings.

    Returns:
        Bronze clickstream path.
    """
    return f"{settings.minio.bronze_uri}/clickstream/events"


def get_silver_clickstream_path(settings: AppSettings) -> str:
    """Return Silver clickstream Delta path.

    Args:
        settings: Application settings.

    Returns:
        Silver clickstream path.
    """
    return f"{settings.minio.silver_uri}/clickstream/events"


def read_bronze_clickstream_partition(
    spark: SparkSession,
    settings: AppSettings,
    partition_date: str,
) -> DataFrame:
    """Read one Bronze clickstream date partition.

    Args:
        spark: Active Spark session.
        settings: Application settings.
        partition_date: Date partition.

    Returns:
        Bronze clickstream dataframe with partition_date restored explicitly.
    """
    bronze_path = get_bronze_clickstream_path(settings)
    partition_path = f"{bronze_path}/partition_date={partition_date}"

    LOGGER.info(
        "silver_clickstream_read_bronze_partition",
        bronze_path=bronze_path,
        partition_path=partition_path,
        partition_date=partition_date,
    )

    return (
        spark.read.parquet(partition_path)
        .withColumn("partition_date", F.lit(partition_date))
    )

def validate_clickstream_schema(dataframe: DataFrame) -> DataFrame:
    """Validate expected clickstream Bronze columns.

    Args:
        dataframe: Bronze clickstream dataframe.

    Returns:
        Input dataframe if valid.

    Raises:
        ValueError: If required columns are missing.
    """
    expected_columns = {
        "event_id",
        "session_id",
        "user_id",
        "event_type",
        "event_timestamp",
        "ingestion_timestamp",
        "page_url",
        "country_code",
        "device_type",
        "traffic_source",
        "event_position",
        "is_late_event",
        "partition_date",
    }
    missing_columns = expected_columns.difference(dataframe.columns)
    if missing_columns:
        raise ValueError(f"Missing clickstream columns: {sorted(missing_columns)}")
    return dataframe


def standardize_clickstream_types(dataframe: DataFrame) -> DataFrame:
    """Cast clickstream fields into standardized Silver types.

    Args:
        dataframe: Bronze clickstream dataframe.

    Returns:
        Standardized clickstream dataframe.
    """
    dataframe = validate_clickstream_schema(dataframe)

    return (
        dataframe.select(
            F.col("event_id").cast("string"),
            F.col("session_id").cast("string"),
            F.col("user_id").cast("string"),
            F.lower(F.trim(F.col("event_type"))).alias("event_type"),
            F.to_timestamp("event_timestamp").alias("event_timestamp"),
            F.to_timestamp("ingestion_timestamp").alias("ingestion_timestamp"),
            F.to_timestamp("bronze_ingested_at").alias("bronze_ingested_at"),
            F.col("page_url").cast("string"),
            F.col("product_id").cast("int"),
            F.col("category_name").cast("string"),
            F.upper(F.trim(F.col("country_code"))).alias("country_code"),
            F.lower(F.trim(F.col("device_type"))).alias("device_type"),
            F.lower(F.trim(F.col("traffic_source"))).alias("traffic_source"),
            F.col("event_position").cast("int"),
            F.col("is_late_event").cast("boolean"),
            F.col("original_event_id").cast("string"),
            F.col("partition_date").cast("string"),
        )
    )


def drop_malformed_clickstream_rows(dataframe: DataFrame) -> DataFrame:
    """Drop malformed clickstream rows.

    Args:
        dataframe: Standardized clickstream dataframe.

    Returns:
        Filtered dataframe.
    """
    return dataframe.dropna(
        subset=[
            "event_id",
            "session_id",
            "user_id",
            "event_type",
            "event_timestamp",
            "ingestion_timestamp",
            "partition_date",
        ]
    )


def deduplicate_clickstream_events(dataframe: DataFrame) -> DataFrame:
    """Deduplicate clickstream records by event_id, keeping first ingestion.

    Args:
        dataframe: Clickstream dataframe.

    Returns:
        Deduplicated dataframe.
    """
    window_spec = Window.partitionBy("event_id").orderBy(
        F.col("ingestion_timestamp").asc(),
        F.col("bronze_ingested_at").asc(),
    )

    return (
        dataframe.withColumn("row_number", F.row_number().over(window_spec))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )


def filter_events_outside_watermark_window(dataframe: DataFrame) -> DataFrame:
    """Filter out events that are too old relative to ingestion time.

    This implements the Silver rule to remove events outside the effective
    watermark tolerance window.

    Args:
        dataframe: Clickstream dataframe.

    Returns:
        Filtered dataframe.
    """
    return dataframe.filter(
        F.col("event_timestamp") >= F.col("ingestion_timestamp") - F.expr("INTERVAL 10 MINUTES")
    )


def build_session_aggregates(dataframe: DataFrame) -> DataFrame:
    """Build session-level aggregate metrics and enrich each event.

    Args:
        dataframe: Filtered clickstream dataframe.

    Returns:
        Enriched dataframe with session metrics.
    """
    session_aggregates = (
        dataframe.groupBy("session_id")
        .agg(
            F.count("*").alias("session_event_count"),
            F.min("event_timestamp").alias("session_start_time"),
            F.max("event_timestamp").alias("session_end_time"),
            F.max(F.when(F.col("event_type") == "purchase", F.lit(1)).otherwise(F.lit(0))).alias(
                "has_purchase"
            ),
        )
        .withColumn("is_conversion", F.col("has_purchase") == F.lit(1))
        .withColumn(
            "session_duration_seconds",
            F.unix_timestamp("session_end_time") - F.unix_timestamp("session_start_time"),
        )
        .drop("has_purchase")
    )

    return dataframe.join(session_aggregates, on="session_id", how="left")


def transform_clickstream_partition(dataframe: DataFrame) -> DataFrame:
    """Apply the full Silver clickstream transformation pipeline.

    Args:
        dataframe: Bronze clickstream dataframe.

    Returns:
        Silver-ready dataframe.
    """
    standardized = standardize_clickstream_types(dataframe)
    filtered = drop_malformed_clickstream_rows(standardized)
    deduplicated = deduplicate_clickstream_events(filtered)
    within_window = filter_events_outside_watermark_window(deduplicated)
    enriched = build_session_aggregates(within_window)

    return enriched.select(
        "event_id",
        "session_id",
        "user_id",
        "event_type",
        "event_timestamp",
        "ingestion_timestamp",
        "bronze_ingested_at",
        "page_url",
        "product_id",
        "category_name",
        "country_code",
        "device_type",
        "traffic_source",
        "event_position",
        "is_late_event",
        "original_event_id",
        "session_event_count",
        "session_start_time",
        "session_end_time",
        "session_duration_seconds",
        "is_conversion",
        "partition_date",
    )


def write_silver_clickstream_delta(
    dataframe: DataFrame,
    settings: AppSettings,
    partition_date: str,
) -> int:
    """Write Silver clickstream partition as Delta, idempotently.

    Args:
        dataframe: Silver clickstream dataframe.
        settings: Application settings.
        partition_date: Partition date.

    Returns:
        Number of written rows.
    """
    silver_path = get_silver_clickstream_path(settings=settings)
    row_count = dataframe.count()

    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"partition_date = '{partition_date}'")
        .save(silver_path)
    )

    LOGGER.info(
        "silver_clickstream_partition_written",
        partition_date=partition_date,
        row_count=row_count,
        silver_path=silver_path,
    )
    return int(row_count)


def process_clickstream_silver_partition(
    partition_date: str,
    settings: AppSettings | None = None,
) -> dict[str, int | str]:
    """Process a Bronze clickstream date partition into Silver Delta.

    Args:
        partition_date: Date partition.
        settings: Optional application settings.

    Returns:
        Processing summary.
    """
    active_settings = settings or get_settings()
    spark = SparkSessionBuilder(settings=active_settings).build("silver_clickstream_processing")

    try:
        bronze_dataframe = read_bronze_clickstream_partition(
            spark=spark,
            settings=active_settings,
            partition_date=partition_date,
        )
        silver_dataframe = transform_clickstream_partition(bronze_dataframe)
        rows_written = write_silver_clickstream_delta(
            dataframe=silver_dataframe,
            settings=active_settings,
            partition_date=partition_date,
        )

        result = {
            "partition_date": partition_date,
            "rows_written": rows_written,
        }
        LOGGER.info("silver_clickstream_partition_complete", **result)
        return result
    finally:
        spark.stop()


if __name__ == "__main__":
    from datetime import datetime, timezone

    today_partition = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(process_clickstream_silver_partition(partition_date=today_partition, settings=get_settings()))