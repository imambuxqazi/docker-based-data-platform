"""Unit tests for Silver transformations."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipelines.silver.clickstream import (
    deduplicate_clickstream_events,
    drop_malformed_clickstream_rows,
    filter_events_outside_watermark_window,
    transform_clickstream_partition,
)
from pipelines.silver.orders import (
    deduplicate_latest_by_key,
    standardize_orders,
)


def test_orders_deduplication_keeps_latest_record(spark_session: SparkSession) -> None:
    """Ensure deduplication keeps only the latest record per key."""
    dataframe = spark_session.createDataFrame(
        [
            (1, "pending", "2026-03-08 10:00:00", "2026-03-08 10:05:00", "2026-03-08"),
            (1, "shipped", "2026-03-08 10:00:00", "2026-03-08 10:10:00", "2026-03-08"),
            (2, "pending", "2026-03-08 11:00:00", "2026-03-08 11:05:00", "2026-03-08"),
        ],
        ["order_id", "order_status", "order_timestamp", "updated_at", "partition_date"],
    ).withColumn("order_timestamp", F.to_timestamp("order_timestamp")).withColumn(
        "updated_at", F.to_timestamp("updated_at")
    ).withColumn("ingested_at", F.to_timestamp(F.lit("2026-03-08 12:00:00")))

    deduplicated = deduplicate_latest_by_key(dataframe, "order_id")

    result = {
        row["order_id"]: row["order_status"]
        for row in deduplicated.select("order_id", "order_status").collect()
    }

    assert deduplicated.count() == 2
    assert result[1] == "shipped"
    assert result[2] == "pending"


def test_clickstream_schema_validation_drops_malformed_rows(spark_session: SparkSession) -> None:
    """Ensure malformed clickstream rows with null required fields are dropped."""
    dataframe = spark_session.createDataFrame(
        [
            ("event-1", "session-1", "user-1", "page_view", "2026-03-08 10:00:00", "2026-03-08 10:01:00", "/home", "PK", "mobile", "organic", 1, False, "2026-03-08"),
            (None, "session-2", "user-2", "page_view", "2026-03-08 10:00:00", "2026-03-08 10:01:00", "/home", "PK", "mobile", "organic", 1, False, "2026-03-08"),
        ],
        [
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
        ],
    ).withColumn("event_timestamp", F.to_timestamp("event_timestamp")).withColumn(
        "ingestion_timestamp", F.to_timestamp("ingestion_timestamp")
    ).withColumn("bronze_ingested_at", F.to_timestamp(F.lit("2026-03-08 10:02:00")))

    filtered = drop_malformed_clickstream_rows(dataframe)

    assert filtered.count() == 1
    assert filtered.collect()[0]["event_id"] == "event-1"


def test_clickstream_deduplication_keeps_first_occurrence(spark_session: SparkSession) -> None:
    """Ensure clickstream deduplication keeps the earliest ingested event per event_id."""
    dataframe = spark_session.createDataFrame(
        [
            ("event-1", "session-1", "user-1", "page_view", "2026-03-08 10:00:00", "2026-03-08 10:01:00", "2026-03-08 10:01:00", "2026-03-08"),
            ("event-1", "session-1", "user-1", "page_view", "2026-03-08 10:00:00", "2026-03-08 10:02:00", "2026-03-08 10:02:00", "2026-03-08"),
        ],
        [
            "event_id",
            "session_id",
            "user_id",
            "event_type",
            "event_timestamp",
            "ingestion_timestamp",
            "bronze_ingested_at",
            "partition_date",
        ],
    ).withColumn("event_timestamp", F.to_timestamp("event_timestamp")).withColumn(
        "ingestion_timestamp", F.to_timestamp("ingestion_timestamp")
    ).withColumn("bronze_ingested_at", F.to_timestamp("bronze_ingested_at"))

    deduplicated = deduplicate_clickstream_events(dataframe)

    assert deduplicated.count() == 1
    assert deduplicated.collect()[0]["ingestion_timestamp"].strftime("%H:%M:%S") == "10:01:00"


def test_clickstream_filters_events_outside_watermark_window(spark_session: SparkSession) -> None:
    """Ensure events older than 10 minutes relative to ingestion are filtered out."""
    dataframe = spark_session.createDataFrame(
        [
            ("event-1", "2026-03-08 10:00:00", "2026-03-08 10:05:00"),
            ("event-2", "2026-03-08 09:30:00", "2026-03-08 10:05:00"),
        ],
        ["event_id", "event_timestamp", "ingestion_timestamp"],
    ).withColumn("event_timestamp", F.to_timestamp("event_timestamp")).withColumn(
        "ingestion_timestamp", F.to_timestamp("ingestion_timestamp")
    )

    filtered = filter_events_outside_watermark_window(dataframe)

    event_ids = [row["event_id"] for row in filtered.select("event_id").collect()]
    assert event_ids == ["event-1"]


def test_orders_silver_standardization_is_idempotent(spark_session: SparkSession) -> None:
    """Ensure standardization plus deduplication yields stable row counts across reruns."""
    dataframe = spark_session.createDataFrame(
        [
            (
                1,
                "external-1",
                11,
                "Pending",
                "2026-03-08 10:00:00",
                100.0,
                "usd",
                "2026-03-08 09:55:00",
                "2026-03-08 10:05:00",
                "orders",
                "2026-03-08 10:06:00",
                "2026-03-08",
            ),
            (
                1,
                "external-1",
                11,
                "Shipped",
                "2026-03-08 10:00:00",
                100.0,
                "usd",
                "2026-03-08 09:55:00",
                "2026-03-08 10:10:00",
                "orders",
                "2026-03-08 10:11:00",
                "2026-03-08",
            ),
        ],
        [
            "order_id",
            "order_external_id",
            "customer_id",
            "order_status",
            "order_timestamp",
            "total_amount",
            "currency_code",
            "created_at",
            "updated_at",
            "source_table",
            "ingested_at",
            "partition_date",
        ],
    ).withColumn("order_timestamp", F.to_timestamp("order_timestamp")).withColumn(
        "created_at", F.to_timestamp("created_at")
    ).withColumn("updated_at", F.to_timestamp("updated_at")).withColumn(
        "ingested_at", F.to_timestamp("ingested_at")
    )

    standardized_once = standardize_orders(dataframe)
    deduplicated_once = deduplicate_latest_by_key(standardized_once, "order_id")

    standardized_twice = standardize_orders(dataframe)
    deduplicated_twice = deduplicate_latest_by_key(standardized_twice, "order_id")

    assert deduplicated_once.count() == deduplicated_twice.count() == 1
    assert deduplicated_once.collect()[0]["order_status"] == deduplicated_twice.collect()[0]["order_status"]