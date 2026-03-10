"""Gold hourly traffic aggregations."""

from __future__ import annotations

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.settings import AppSettings


LOGGER = structlog.get_logger(__name__)


def get_silver_clickstream_path(settings: AppSettings) -> str:
    """Return Silver clickstream Delta path."""
    return f"{settings.minio.silver_uri}/clickstream/events"


def get_gold_traffic_path(settings: AppSettings) -> str:
    """Return Gold hourly traffic Delta path."""
    return f"{settings.minio.gold_uri}/traffic/hourly_traffic"


def read_silver_clickstream(spark: SparkSession, settings: AppSettings) -> DataFrame:
    """Read Silver clickstream Delta table."""
    return spark.read.format("delta").load(get_silver_clickstream_path(settings))


def filter_clickstream_for_date(dataframe: DataFrame, target_date: str) -> DataFrame:
    """Filter events for a target date."""
    return dataframe.filter(F.to_date("event_timestamp") == F.lit(target_date))


def aggregate_hourly_traffic(dataframe: DataFrame) -> DataFrame:
    """Aggregate hourly traffic metrics."""
    session_level = (
        dataframe.groupBy("session_id")
        .agg(
            F.min("event_timestamp").alias("session_start_time"),
            F.max("session_duration_seconds").alias("session_duration_seconds"),
            F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("has_purchase"),
            F.max(F.when(F.col("session_event_count") == 1, 1).otherwise(0)).alias("is_bounce"),
        )
    )

    session_with_hour = session_level.withColumn("event_date", F.to_date("session_start_time")).withColumn(
        "event_hour",
        F.hour("session_start_time"),
    )

    aggregated_sessions = session_with_hour.groupBy("event_date", "event_hour").agg(
        F.countDistinct("session_id").alias("active_users"),
        F.sum("is_bounce").alias("bounce_sessions"),
        F.sum("has_purchase").alias("purchases"),
        F.round(F.avg("session_duration_seconds"), 2).alias("avg_session_duration"),
    )

    total_events = dataframe.withColumn("event_date", F.to_date("event_timestamp")).withColumn(
        "event_hour",
        F.hour("event_timestamp"),
    ).groupBy("event_date", "event_hour").agg(F.count("*").alias("total_events"))

    return (
        aggregated_sessions.join(total_events, on=["event_date", "event_hour"], how="left")
        .withColumn(
            "bounce_rate",
            F.when(F.col("active_users") > 0, F.col("bounce_sessions") / F.col("active_users")).otherwise(F.lit(0.0)),
        )
        .drop("bounce_sessions")
    )


def write_hourly_traffic(dataframe: DataFrame, settings: AppSettings, target_date: str) -> int:
    """Write Gold hourly traffic idempotently for one event_date."""
    output_path = get_gold_traffic_path(settings)
    row_count = dataframe.count()

    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"event_date = '{target_date}'")
        .save(output_path)
    )

    LOGGER.info(
        "gold_hourly_traffic_written",
        target_date=target_date,
        row_count=row_count,
        output_path=output_path,
    )
    return int(row_count)


def build_hourly_traffic(
    spark: SparkSession,
    settings: AppSettings,
    date: str,
) -> dict[str, int | str]:
    """Build Gold hourly traffic for a given date."""
    clickstream_dataframe = read_silver_clickstream(spark, settings)
    filtered_dataframe = filter_clickstream_for_date(clickstream_dataframe, date)
    gold_dataframe = aggregate_hourly_traffic(filtered_dataframe)

    rows_written = write_hourly_traffic(gold_dataframe, settings, date)

    result = {
        "gold_table": "hourly_traffic",
        "date": date,
        "rows_written": rows_written,
    }
    LOGGER.info("gold_hourly_traffic_complete", **result)
    return result