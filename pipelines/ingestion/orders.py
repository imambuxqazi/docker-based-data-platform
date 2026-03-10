"""Polling-based CDC ingestion for PostgreSQL orders data into Bronze storage.

This module implements watermark-based extraction using the `updated_at` column.
It reads source rows in batches, writes partitioned Parquet files to MinIO,
and persists watermark state locally so restarts resume correctly.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
import structlog
from psycopg2.extras import RealDictCursor
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from config.settings import AppSettings, get_settings
from config.spark import SparkSessionBuilder


LOGGER = structlog.get_logger(__name__)

STATE_DIRECTORY = Path("/opt/airflow/.state")
STATE_FILE_PATH = STATE_DIRECTORY / "orders_cdc_watermark.json"

SOURCE_TABLES: tuple[str, ...] = (
    "customers",
    "products",
    "orders",
    "order_items",
)


def ensure_state_directory_exists() -> None:
    """Create the local state directory if it does not already exist."""
    STATE_DIRECTORY.mkdir(parents=True, exist_ok=True)


def default_watermark() -> str:
    """Return the default starting watermark in ISO format."""
    return "1970-01-01T00:00:00+00:00"


def load_watermark_state(state_file_path: Path = STATE_FILE_PATH) -> dict[str, str]:
    """Load per-table watermark state from disk.

    Args:
        state_file_path: Location of the watermark state file.

    Returns:
        Dictionary keyed by table name with ISO timestamp values.
    """
    ensure_state_directory_exists()

    if not state_file_path.exists():
        return {table_name: default_watermark() for table_name in SOURCE_TABLES}

    state_payload = json.loads(state_file_path.read_text(encoding="utf-8"))
    return {
        table_name: state_payload.get(table_name, default_watermark())
        for table_name in SOURCE_TABLES
    }


def save_watermark_state(
    watermark_state: dict[str, str],
    state_file_path: Path = STATE_FILE_PATH,
) -> None:
    """Persist per-table watermark state to disk.

    Args:
        watermark_state: Updated watermark state payload.
        state_file_path: Location of the watermark state file.
    """
    ensure_state_directory_exists()
    state_file_path.write_text(
        json.dumps(watermark_state, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def normalize_datetime_value(value: Any) -> str | None:
    """Normalize a datetime-like value into ISO 8601 string format.

    Args:
        value: Source value to normalize.

    Returns:
        ISO-formatted UTC timestamp string or None.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()

    return str(value)


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(psycopg2.Error),
)
def get_postgres_connection(settings: AppSettings) -> psycopg2.extensions.connection:
    """Create a PostgreSQL connection with retry.

    Args:
        settings: Application settings.

    Returns:
        Active psycopg2 connection.
    """
    postgres_settings = settings.postgres
    return psycopg2.connect(
        host=postgres_settings.host,
        port=postgres_settings.port,
        dbname=postgres_settings.database,
        user=postgres_settings.user,
        password=postgres_settings.password,
    )


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(psycopg2.Error),
)
def fetch_changed_rows(
    settings: AppSettings,
    table_name: str,
    last_watermark: str,
    batch_size: int,
) -> list[dict[str, Any]]:
    """Fetch rows changed after the last watermark.

    Args:
        settings: Application settings.
        table_name: Source table name.
        last_watermark: Exclusive lower-bound watermark timestamp.
        batch_size: Maximum rows to fetch.

    Returns:
        List of changed rows as dictionaries.
    """
    query = f"""
        SELECT *
        FROM {table_name}
        WHERE updated_at > %s
        ORDER BY updated_at ASC
        LIMIT %s;
    """

    connection = get_postgres_connection(settings)
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (last_watermark, batch_size))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    finally:
        connection.close()


def convert_rows_to_pandas(
    rows: list[dict[str, Any]],
    table_name: str,
) -> pd.DataFrame:
    """Convert source rows to a pandas DataFrame and enrich ingestion metadata.

    Args:
        rows: Extracted source rows.
        table_name: Logical source table name.

    Returns:
        pandas DataFrame ready for Spark ingestion.
    """
    dataframe = pd.DataFrame(rows)

    if dataframe.empty:
        return dataframe

    ingestion_time = datetime.now(timezone.utc)
    dataframe["source_table"] = table_name
    dataframe["ingested_at"] = ingestion_time

    datetime_columns = [
        column_name
        for column_name in dataframe.columns
        if dataframe[column_name].dtype == "datetime64[ns, UTC]"
        or dataframe[column_name].dtype == "datetime64[ns]"
    ]

    for column_name in datetime_columns:
        dataframe[column_name] = pd.to_datetime(dataframe[column_name], utc=True)

    return dataframe


def add_partition_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Add partition columns derived from `updated_at`.

    Args:
        dataframe: Source batch dataframe.

    Returns:
        Dataframe with `partition_date` column added.
    """
    if dataframe.empty:
        return dataframe

    dataframe = dataframe.copy()
    dataframe["updated_at"] = pd.to_datetime(dataframe["updated_at"], utc=True)
    dataframe["partition_date"] = dataframe["updated_at"].dt.strftime("%Y-%m-%d")
    return dataframe


def get_latest_watermark(rows: list[dict[str, Any]], fallback_watermark: str) -> str:
    """Compute the new watermark from the extracted rows.

    Args:
        rows: Extracted rows for the current batch.
        fallback_watermark: Prior watermark to use if rows are empty.

    Returns:
        Latest updated_at value in ISO format.
    """
    if not rows:
        return fallback_watermark

    updated_values = [
        normalize_datetime_value(row.get("updated_at"))
        for row in rows
        if row.get("updated_at") is not None
    ]

    if not updated_values:
        return fallback_watermark

    return max(updated_values)


def write_bronze_parquet(
    settings: AppSettings,
    table_name: str,
    dataframe: pd.DataFrame,
) -> int:
    """Write a batch dataframe to Bronze Parquet storage.

    Args:
        settings: Application settings.
        table_name: Source table name.
        dataframe: Batch dataframe.

    Returns:
        Number of written rows.
    """
    if dataframe.empty:
        return 0

    spark = SparkSessionBuilder(settings=settings).build(app_name=f"bronze_orders_{table_name}")

    try:
        spark_dataframe = spark.createDataFrame(dataframe)
        output_path = f"{settings.minio.bronze_uri}/orders/{table_name}"

        (
            spark_dataframe.write.mode("append")
            .partitionBy("partition_date")
            .parquet(output_path)
        )

        row_count = spark_dataframe.count()

        LOGGER.info(
            "bronze_orders_batch_written",
            table_name=table_name,
            row_count=row_count,
            output_path=output_path,
        )
        return int(row_count)
    finally:
        spark.stop()


def ingest_table_batch(
    settings: AppSettings,
    table_name: str,
    watermark_state: dict[str, str],
) -> dict[str, Any]:
    """Ingest one CDC batch for a single table.

    Args:
        settings: Application settings.
        table_name: Source table name.
        watermark_state: Current mutable watermark state.

    Returns:
        Batch execution summary.
    """
    last_watermark = watermark_state.get(table_name, default_watermark())
    batch_size = settings.batch.cdc_batch_size

    LOGGER.info(
        "bronze_orders_batch_start",
        table_name=table_name,
        last_watermark=last_watermark,
        batch_size=batch_size,
    )

    rows = fetch_changed_rows(
        settings=settings,
        table_name=table_name,
        last_watermark=last_watermark,
        batch_size=batch_size,
    )

    if not rows:
        LOGGER.info("bronze_orders_no_changes", table_name=table_name)
        return {
            "table_name": table_name,
            "rows_extracted": 0,
            "rows_written": 0,
            "new_watermark": last_watermark,
        }

    dataframe = convert_rows_to_pandas(rows=rows, table_name=table_name)
    dataframe = add_partition_columns(dataframe=dataframe)
    rows_written = write_bronze_parquet(
        settings=settings,
        table_name=table_name,
        dataframe=dataframe,
    )

    new_watermark = get_latest_watermark(rows=rows, fallback_watermark=last_watermark)
    watermark_state[table_name] = new_watermark

    LOGGER.info(
        "bronze_orders_batch_complete",
        table_name=table_name,
        rows_extracted=len(rows),
        rows_written=rows_written,
        new_watermark=new_watermark,
    )

    return {
        "table_name": table_name,
        "rows_extracted": len(rows),
        "rows_written": rows_written,
        "new_watermark": new_watermark,
    }


def ingest_all_orders_tables(settings: AppSettings | None = None) -> dict[str, Any]:
    """Run one polling cycle across all source tables.

    Args:
        settings: Optional application settings.

    Returns:
        Summary payload for the polling cycle.
    """
    active_settings = settings or get_settings()
    watermark_state = load_watermark_state()

    batch_summaries: list[dict[str, Any]] = []
    total_rows_written = 0

    for table_name in SOURCE_TABLES:
        summary = ingest_table_batch(
            settings=active_settings,
            table_name=table_name,
            watermark_state=watermark_state,
        )
        batch_summaries.append(summary)
        total_rows_written += int(summary["rows_written"])

    save_watermark_state(watermark_state)

    cycle_summary = {
        "tables_processed": len(SOURCE_TABLES),
        "total_rows_written": total_rows_written,
        "watermarks": watermark_state,
        "batches": batch_summaries,
    }

    LOGGER.info("bronze_orders_cycle_complete", **cycle_summary)
    return cycle_summary


if __name__ == "__main__":
    result = ingest_all_orders_tables(settings=get_settings())
    print(json.dumps(result, indent=2, sort_keys=True))