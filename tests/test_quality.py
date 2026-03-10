"""Unit tests for data quality functions and runner."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from quality.checks import (
    check_duplicate_rate,
    check_freshness,
    check_null_rate,
    check_referential_integrity,
    check_row_count,
    check_value_range,
)
from quality.runner import QualityRunner


def test_check_functions_pass_with_clean_data(spark_session: SparkSession) -> None:
    """Ensure core quality checks pass on valid data."""
    dataframe = spark_session.createDataFrame(
        [
            (1, "A", 10.0),
            (2, "B", 20.0),
        ],
        ["id", "category", "amount"],
    )

    assert check_row_count(dataframe, "clean_table", 1).passed is True
    assert check_null_rate(dataframe, "clean_table", "category", 0.0).passed is True
    assert check_duplicate_rate(dataframe, "clean_table", ["id"], 0.0).passed is True
    assert check_value_range(dataframe, "clean_table", "amount", 0, 100).passed is True


def test_check_functions_fail_with_bad_data(spark_session: SparkSession) -> None:
    """Ensure core quality checks fail on invalid data."""
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("category", StringType(), True),
            StructField("amount", DoubleType(), True),
        ]
    )

    dataframe = spark_session.createDataFrame(
        [
            (1, None, -10.0),
            (1, None, 200.0),
        ],
        schema=schema,
    )

    assert check_row_count(dataframe, "bad_table", 3).passed is False
    assert check_null_rate(dataframe, "bad_table", "category", 0.1).passed is False
    assert check_duplicate_rate(dataframe, "bad_table", ["id"], 0.0).passed is False
    assert check_value_range(dataframe, "bad_table", "amount", 0, 100).passed is False


def test_check_freshness_passes_and_fails(spark_session: SparkSession) -> None:
    """Ensure freshness check correctly distinguishes recent and stale timestamps."""
    recent_time = datetime.now(timezone.utc) - timedelta(minutes=5)
    stale_time = datetime.now(timezone.utc) - timedelta(minutes=120)

    recent_dataframe = spark_session.createDataFrame([(recent_time,)], ["updated_at"])
    stale_dataframe = spark_session.createDataFrame([(stale_time,)], ["updated_at"])

    assert check_freshness(recent_dataframe, "fresh_table", "updated_at", 30).passed is True
    assert check_freshness(stale_dataframe, "stale_table", "updated_at", 30).passed is False


def test_check_referential_integrity_passes_and_fails(spark_session: SparkSession) -> None:
    """Ensure referential integrity catches orphan keys."""
    parent_dataframe = spark_session.createDataFrame([(1,), (2,)], ["customer_id"])
    good_child_dataframe = spark_session.createDataFrame([(1,), (2,)], ["customer_id"])
    bad_child_dataframe = spark_session.createDataFrame([(1,), (3,)], ["customer_id"])

    assert (
        check_referential_integrity(
            good_child_dataframe,
            parent_dataframe,
            "child_table",
            "customer_id",
            "customer_id",
        ).passed
        is True
    )

    assert (
        check_referential_integrity(
            bad_child_dataframe,
            parent_dataframe,
            "child_table",
            "customer_id",
            "customer_id",
        ).passed
        is False
    )


def test_quality_runner_calls_alert_manager_on_failure(spark_session: SparkSession) -> None:
    """Ensure failed checks trigger alert manager calls."""
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("category", StringType(), True),
            StructField("amount", DoubleType(), True),
        ]
    )

    dataframe = spark_session.createDataFrame(
        [
            (1, None, -10.0),
            (1, None, 200.0),
        ],
        schema=schema,
    )

    alert_manager = Mock()
    runner = QualityRunner(alert_manager=alert_manager)

    report = runner.run_suite(
        dataframe=dataframe,
        table_name="bad_table",
        checks_config=[
            {"check_type": "row_count", "min_row_count": 3},
            {"check_type": "null_rate", "column_name": "category", "max_null_rate": 0.1},
        ],
    )

    assert report.passed is False
    assert report.failed_checks == 2
    assert alert_manager.send_alert.call_count == 2


def test_quality_runner_does_not_alert_when_all_checks_pass(spark_session: SparkSession) -> None:
    """Ensure passing checks do not trigger alerts."""
    dataframe = spark_session.createDataFrame(
        [
            (1, "A", 10.0),
            (2, "B", 20.0),
        ],
        ["id", "category", "amount"],
    )

    alert_manager = Mock()
    runner = QualityRunner(alert_manager=alert_manager)

    report = runner.run_suite(
        dataframe=dataframe,
        table_name="good_table",
        checks_config=[
            {"check_type": "row_count", "min_row_count": 1},
            {"check_type": "null_rate", "column_name": "category", "max_null_rate": 0.1},
            {"check_type": "duplicate_rate", "key_columns": ["id"], "max_duplicate_rate": 0.0},
        ],
    )

    assert report.passed is True
    assert report.failed_checks == 0
    alert_manager.send_alert.assert_not_called()