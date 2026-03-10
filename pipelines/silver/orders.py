"""Silver transformations for orders-domain Bronze data."""

from __future__ import annotations

from typing import Iterable

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import AppSettings, get_settings
from config.spark import SparkSessionBuilder


LOGGER = structlog.get_logger(__name__)


BRONZE_ORDERS_TABLES: tuple[str, ...] = (
    "customers",
    "products",
    "orders",
    "order_items",
)


def get_bronze_table_path(settings: AppSettings, table_name: str) -> str:
    """Return Bronze path for a source table.

    Args:
        settings: Application settings.
        table_name: Source table name.

    Returns:
        Bronze parquet path.
    """
    return f"{settings.minio.bronze_uri}/orders/{table_name}"


def get_silver_table_path(settings: AppSettings, table_name: str) -> str:
    """Return Silver Delta path for a source table.

    Args:
        settings: Application settings.
        table_name: Source table name.

    Returns:
        Silver Delta path.
    """
    return f"{settings.minio.silver_uri}/orders/{table_name}"

def read_bronze_partition(
    spark: SparkSession,
    settings: AppSettings,
    table_name: str,
    partition_date: str,
) -> DataFrame:
    """Read a Bronze parquet date partition.

    Args:
        spark: Active Spark session.
        settings: Application settings.
        table_name: Source table name.
        partition_date: Partition date in YYYY-MM-DD format.

    Returns:
        Bronze Spark DataFrame with partition_date restored explicitly.
    """
    bronze_path = get_bronze_table_path(settings=settings, table_name=table_name)
    partition_path = f"{bronze_path}/partition_date={partition_date}"

    LOGGER.info(
        "silver_orders_read_bronze_partition",
        table_name=table_name,
        partition_date=partition_date,
        bronze_path=bronze_path,
        partition_path=partition_path,
    )

    return (
        spark.read.parquet(partition_path)
        .withColumn("partition_date", F.lit(partition_date))
    )

def validate_expected_columns(
    dataframe: DataFrame,
    expected_columns: Iterable[str],
    table_name: str,
) -> DataFrame:
    """Validate that all expected columns exist.

    Args:
        dataframe: Input dataframe.
        expected_columns: Required columns.
        table_name: Logical table name.

    Returns:
        Input dataframe unchanged.

    Raises:
        ValueError: If required columns are missing.
    """
    missing_columns = [column_name for column_name in expected_columns if column_name not in dataframe.columns]
    if missing_columns:
        raise ValueError(
            f"Missing expected columns for table '{table_name}': {missing_columns}"
        )
    return dataframe


def drop_malformed_rows(dataframe: DataFrame, required_columns: list[str]) -> DataFrame:
    """Drop rows missing required values.

    Args:
        dataframe: Input dataframe.
        required_columns: Columns that must be non-null.

    Returns:
        Filtered dataframe.
    """
    return dataframe.dropna(subset=required_columns)


def standardize_customers(dataframe: DataFrame) -> DataFrame:
    """Standardize customers schema and values.

    Args:
        dataframe: Bronze customers dataframe.

    Returns:
        Standardized customers dataframe.
    """
    required_columns = [
        "customer_id",
        "email",
        "country_code",
        "created_at",
        "updated_at",
        "partition_date",
    ]
    dataframe = validate_expected_columns(dataframe, required_columns, "customers")
    dataframe = drop_malformed_rows(dataframe, ["customer_id", "email", "updated_at"])

    return (
        dataframe.select(
            F.col("customer_id").cast("long"),
            F.col("customer_external_id").cast("string"),
            F.initcap(F.trim(F.col("first_name"))).alias("first_name"),
            F.initcap(F.trim(F.col("last_name"))).alias("last_name"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.upper(F.trim(F.col("country_code"))).alias("country_code"),
            F.initcap(F.trim(F.col("city"))).alias("city"),
            F.to_timestamp("created_at").alias("created_at"),
            F.to_timestamp("updated_at").alias("updated_at"),
            F.col("source_table").cast("string"),
            F.to_timestamp("ingested_at").alias("ingested_at"),
            F.col("partition_date").cast("string"),
        )
    )


def standardize_products(dataframe: DataFrame) -> DataFrame:
    """Standardize products schema and values.

    Args:
        dataframe: Bronze products dataframe.

    Returns:
        Standardized products dataframe.
    """
    required_columns = [
        "product_id",
        "product_sku",
        "unit_price",
        "updated_at",
        "partition_date",
    ]
    dataframe = validate_expected_columns(dataframe, required_columns, "products")
    dataframe = drop_malformed_rows(dataframe, ["product_id", "product_sku", "updated_at"])

    return (
        dataframe.select(
            F.col("product_id").cast("long"),
            F.upper(F.trim(F.col("product_sku"))).alias("product_sku"),
            F.trim(F.col("product_name")).alias("product_name"),
            F.trim(F.col("category_name")).alias("category_name"),
            F.round(F.col("unit_price").cast("decimal(12,2)"), 2).alias("unit_price"),
            F.col("is_active").cast("boolean"),
            F.to_timestamp("created_at").alias("created_at"),
            F.to_timestamp("updated_at").alias("updated_at"),
            F.col("source_table").cast("string"),
            F.to_timestamp("ingested_at").alias("ingested_at"),
            F.col("partition_date").cast("string"),
        )
    )


def standardize_orders(dataframe: DataFrame) -> DataFrame:
    """Standardize orders schema and values.

    Args:
        dataframe: Bronze orders dataframe.

    Returns:
        Standardized orders dataframe.
    """
    required_columns = [
        "order_id",
        "customer_id",
        "order_status",
        "order_timestamp",
        "total_amount",
        "updated_at",
        "partition_date",
    ]
    dataframe = validate_expected_columns(dataframe, required_columns, "orders")
    dataframe = drop_malformed_rows(dataframe, ["order_id", "customer_id", "order_status", "updated_at"])

    return (
        dataframe.select(
            F.col("order_id").cast("long"),
            F.col("order_external_id").cast("string"),
            F.col("customer_id").cast("long"),
            F.lower(F.trim(F.col("order_status"))).alias("order_status"),
            F.to_timestamp("order_timestamp").alias("order_timestamp"),
            F.round(F.col("total_amount").cast("decimal(14,2)"), 2).alias("total_amount"),
            F.upper(F.trim(F.col("currency_code"))).alias("currency_code"),
            F.to_timestamp("created_at").alias("created_at"),
            F.to_timestamp("updated_at").alias("updated_at"),
            F.col("source_table").cast("string"),
            F.to_timestamp("ingested_at").alias("ingested_at"),
            F.col("partition_date").cast("string"),
        )
    )


def standardize_order_items(dataframe: DataFrame) -> DataFrame:
    """Standardize order_items schema and values.

    Args:
        dataframe: Bronze order_items dataframe.

    Returns:
        Standardized order_items dataframe.
    """
    required_columns = [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "updated_at",
        "partition_date",
    ]
    dataframe = validate_expected_columns(dataframe, required_columns, "order_items")
    dataframe = drop_malformed_rows(dataframe, ["order_item_id", "order_id", "product_id", "updated_at"])

    return (
        dataframe.select(
            F.col("order_item_id").cast("long"),
            F.col("order_id").cast("long"),
            F.col("product_id").cast("long"),
            F.col("quantity").cast("int"),
            F.round(F.col("unit_price").cast("decimal(12,2)"), 2).alias("unit_price"),
            F.round(F.col("line_amount").cast("decimal(14,2)"), 2).alias("line_amount"),
            F.to_timestamp("created_at").alias("created_at"),
            F.to_timestamp("updated_at").alias("updated_at"),
            F.col("source_table").cast("string"),
            F.to_timestamp("ingested_at").alias("ingested_at"),
            F.col("partition_date").cast("string"),
        )
    )


def deduplicate_latest_by_key(dataframe: DataFrame, key_column: str) -> DataFrame:
    """Deduplicate records by key keeping the latest updated_at row.

    Args:
        dataframe: Input dataframe.
        key_column: Business or primary key column.

    Returns:
        Deduplicated dataframe.
    """
    window_spec = Window.partitionBy(key_column).orderBy(
        F.col("updated_at").desc(),
        F.col("ingested_at").desc(),
    )

    return (
        dataframe.withColumn("row_number", F.row_number().over(window_spec))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )


def transform_orders_table(dataframe: DataFrame, table_name: str) -> DataFrame:
    """Apply table-specific standardization and deduplication.

    Args:
        dataframe: Bronze dataframe.
        table_name: Source table name.

    Returns:
        Silver dataframe.
    """
    if table_name == "customers":
        standardized = standardize_customers(dataframe)
        return deduplicate_latest_by_key(standardized, "customer_id")

    if table_name == "products":
        standardized = standardize_products(dataframe)
        return deduplicate_latest_by_key(standardized, "product_id")

    if table_name == "orders":
        standardized = standardize_orders(dataframe)
        return deduplicate_latest_by_key(standardized, "order_id")

    if table_name == "order_items":
        standardized = standardize_order_items(dataframe)
        return deduplicate_latest_by_key(standardized, "order_item_id")

    raise ValueError(f"Unsupported orders silver table: {table_name}")


def write_silver_delta(
    dataframe: DataFrame,
    settings: AppSettings,
    table_name: str,
    partition_date: str,
) -> int:
    """Write Silver dataframe as Delta, idempotent for a date partition.

    Args:
        dataframe: Silver dataframe.
        settings: Application settings.
        table_name: Source table name.
        partition_date: Partition date.

    Returns:
        Number of written rows.
    """
    silver_path = get_silver_table_path(settings=settings, table_name=table_name)
    row_count = dataframe.count()

    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"partition_date = '{partition_date}'")
        .save(silver_path)
    )

    LOGGER.info(
        "silver_orders_partition_written",
        table_name=table_name,
        partition_date=partition_date,
        row_count=row_count,
        silver_path=silver_path,
    )
    return int(row_count)


def process_orders_table_partition(
    spark: SparkSession,
    settings: AppSettings,
    table_name: str,
    partition_date: str,
) -> dict[str, int | str]:
    """Process one Bronze table partition into Silver.

    Args:
        spark: Active Spark session.
        settings: Application settings.
        table_name: Source table name.
        partition_date: Date partition.

    Returns:
        Processing summary.
    """
    bronze_dataframe = read_bronze_partition(
        spark=spark,
        settings=settings,
        table_name=table_name,
        partition_date=partition_date,
    )
    silver_dataframe = transform_orders_table(bronze_dataframe, table_name=table_name)
    rows_written = write_silver_delta(
        dataframe=silver_dataframe,
        settings=settings,
        table_name=table_name,
        partition_date=partition_date,
    )
    return {
        "table_name": table_name,
        "partition_date": partition_date,
        "rows_written": rows_written,
    }


def process_orders_silver_partition(
    partition_date: str,
    settings: AppSettings | None = None,
) -> dict[str, object]:
    """Process all Bronze order-domain tables for a partition date.

    Args:
        partition_date: Partition date in YYYY-MM-DD format.
        settings: Optional application settings.

    Returns:
        Partition processing summary.
    """
    active_settings = settings or get_settings()
    spark = SparkSessionBuilder(settings=active_settings).build("silver_orders_processing")

    try:
        summaries = [
            process_orders_table_partition(
                spark=spark,
                settings=active_settings,
                table_name=table_name,
                partition_date=partition_date,
            )
            for table_name in BRONZE_ORDERS_TABLES
        ]

        result = {
            "partition_date": partition_date,
            "tables_processed": len(summaries),
            "total_rows_written": sum(int(item["rows_written"]) for item in summaries),
            "tables": summaries,
        }
        LOGGER.info("silver_orders_partition_complete", **result)
        return result
    finally:
        spark.stop()


if __name__ == "__main__":
    from datetime import datetime, timezone

    today_partition = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(process_orders_silver_partition(partition_date=today_partition, settings=get_settings()))