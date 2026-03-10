"""Reusable data quality check functions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class CheckResult:
    """Result of a single data quality check."""

    check_name: str
    passed: bool
    metric_value: float | int | str
    threshold_value: float | int | str
    message: str
    table_name: str


def check_null_rate(
    dataframe: DataFrame,
    table_name: str,
    column_name: str,
    max_null_rate: float,
) -> CheckResult:
    """Check whether a column null rate is within threshold.

    Args:
        dataframe: Input Spark DataFrame.
        table_name: Logical table name.
        column_name: Column to assess.
        max_null_rate: Maximum allowed null rate between 0 and 1.

    Returns:
        CheckResult for the null-rate rule.
    """
    total_count = dataframe.count()

    if total_count == 0:
        return CheckResult(
            check_name="check_null_rate",
            passed=False,
            metric_value="empty_dataframe",
            threshold_value=max_null_rate,
            message=f"Table {table_name} is empty, cannot evaluate null rate for {column_name}.",
            table_name=table_name,
        )

    null_count = dataframe.filter(F.col(column_name).isNull()).count()
    null_rate = null_count / total_count
    passed = null_rate <= max_null_rate

    return CheckResult(
        check_name="check_null_rate",
        passed=passed,
        metric_value=round(null_rate, 6),
        threshold_value=max_null_rate,
        message=(
            f"Column {column_name} null rate is {null_rate:.6f}; "
            f"maximum allowed is {max_null_rate:.6f}."
        ),
        table_name=table_name,
    )


def check_duplicate_rate(
    dataframe: DataFrame,
    table_name: str,
    key_columns: list[str],
    max_duplicate_rate: float,
) -> CheckResult:
    """Check whether duplicate rate on key columns is within threshold.

    Args:
        dataframe: Input Spark DataFrame.
        table_name: Logical table name.
        key_columns: Columns defining uniqueness.
        max_duplicate_rate: Maximum allowed duplicate rate between 0 and 1.

    Returns:
        CheckResult for the duplicate-rate rule.
    """
    total_count = dataframe.count()

    if total_count == 0:
        return CheckResult(
            check_name="check_duplicate_rate",
            passed=False,
            metric_value="empty_dataframe",
            threshold_value=max_duplicate_rate,
            message=f"Table {table_name} is empty, cannot evaluate duplicate rate.",
            table_name=table_name,
        )

    distinct_count = dataframe.select(*key_columns).distinct().count()
    duplicate_count = total_count - distinct_count
    duplicate_rate = duplicate_count / total_count
    passed = duplicate_rate <= max_duplicate_rate

    return CheckResult(
        check_name="check_duplicate_rate",
        passed=passed,
        metric_value=round(duplicate_rate, 6),
        threshold_value=max_duplicate_rate,
        message=(
            f"Duplicate rate on keys {key_columns} is {duplicate_rate:.6f}; "
            f"maximum allowed is {max_duplicate_rate:.6f}."
        ),
        table_name=table_name,
    )


def check_row_count(
    dataframe: DataFrame,
    table_name: str,
    min_row_count: int,
) -> CheckResult:
    """Check whether row count meets the minimum threshold.

    Args:
        dataframe: Input Spark DataFrame.
        table_name: Logical table name.
        min_row_count: Minimum expected row count.

    Returns:
        CheckResult for the row-count rule.
    """
    row_count = dataframe.count()
    passed = row_count >= min_row_count

    return CheckResult(
        check_name="check_row_count",
        passed=passed,
        metric_value=row_count,
        threshold_value=min_row_count,
        message=(
            f"Row count for table {table_name} is {row_count}; "
            f"minimum expected is {min_row_count}."
        ),
        table_name=table_name,
    )


def check_value_range(
    dataframe: DataFrame,
    table_name: str,
    column_name: str,
    min_value: float | int,
    max_value: float | int,
) -> CheckResult:
    """Check whether all non-null values fall inside a numeric range.

    Args:
        dataframe: Input Spark DataFrame.
        table_name: Logical table name.
        column_name: Column to assess.
        min_value: Minimum allowed numeric value.
        max_value: Maximum allowed numeric value.

    Returns:
        CheckResult for the value-range rule.
    """
    invalid_count = dataframe.filter(
        F.col(column_name).isNotNull()
        & ((F.col(column_name) < F.lit(min_value)) | (F.col(column_name) > F.lit(max_value)))
    ).count()

    passed = invalid_count == 0

    return CheckResult(
        check_name="check_value_range",
        passed=passed,
        metric_value=invalid_count,
        threshold_value=0,
        message=(
            f"Column {column_name} has {invalid_count} out-of-range rows; "
            f"expected range is [{min_value}, {max_value}]."
        ),
        table_name=table_name,
    )


def check_freshness(
    dataframe: DataFrame,
    table_name: str,
    timestamp_column: str,
    max_age_minutes: int,
) -> CheckResult:
    """Check whether the most recent timestamp is fresh enough.

    Args:
        dataframe: Input Spark DataFrame.
        table_name: Logical table name.
        timestamp_column: Timestamp column to evaluate.
        max_age_minutes: Maximum acceptable age in minutes.

    Returns:
        CheckResult for the freshness rule.
    """
    max_timestamp_row = dataframe.select(F.max(F.col(timestamp_column)).alias("max_timestamp")).collect()[0]
    max_timestamp = max_timestamp_row["max_timestamp"]

    if max_timestamp is None:
        return CheckResult(
            check_name="check_freshness",
            passed=False,
            metric_value="null_max_timestamp",
            threshold_value=max_age_minutes,
            message=(
                f"Table {table_name} has no max timestamp value in column {timestamp_column}."
            ),
            table_name=table_name,
        )

    if max_timestamp.tzinfo is None:
        max_timestamp = max_timestamp.replace(tzinfo=timezone.utc)

    current_time = datetime.now(timezone.utc)
    age_minutes = (current_time - max_timestamp.astimezone(timezone.utc)).total_seconds() / 60
    passed = age_minutes <= max_age_minutes

    return CheckResult(
        check_name="check_freshness",
        passed=passed,
        metric_value=round(age_minutes, 2),
        threshold_value=max_age_minutes,
        message=(
            f"Freshness age for {timestamp_column} is {age_minutes:.2f} minutes; "
            f"maximum allowed is {max_age_minutes} minutes."
        ),
        table_name=table_name,
    )


def check_referential_integrity(
    child_dataframe: DataFrame,
    parent_dataframe: DataFrame,
    table_name: str,
    child_key_column: str,
    parent_key_column: str,
) -> CheckResult:
    """Check that child keys exist in the parent key set.

    Args:
        child_dataframe: Child Spark DataFrame.
        parent_dataframe: Parent Spark DataFrame.
        table_name: Logical child table name.
        child_key_column: Foreign key column in child dataframe.
        parent_key_column: Primary key column in parent dataframe.

    Returns:
        CheckResult for referential integrity.
    """
    parent_keys = parent_dataframe.select(F.col(parent_key_column).alias("parent_key")).distinct()
    child_keys = child_dataframe.select(F.col(child_key_column).alias("child_key")).filter(
        F.col(child_key_column).isNotNull()
    )

    orphan_count = (
        child_keys.join(
            parent_keys,
            child_keys["child_key"] == parent_keys["parent_key"],
            how="left_anti",
        ).count()
    )

    passed = orphan_count == 0

    return CheckResult(
        check_name="check_referential_integrity",
        passed=passed,
        metric_value=orphan_count,
        threshold_value=0,
        message=(
            f"Referential integrity check found {orphan_count} orphan keys in "
            f"{table_name}.{child_key_column} against parent key {parent_key_column}."
        ),
        table_name=table_name,
    )