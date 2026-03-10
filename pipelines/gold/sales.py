"""Gold daily sales aggregations."""

from __future__ import annotations

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.settings import AppSettings, get_settings


LOGGER = structlog.get_logger(__name__)


def get_silver_orders_path(settings: AppSettings) -> str:
    """Return Silver orders Delta path."""
    return f"{settings.minio.silver_uri}/orders/orders"


def get_silver_customers_path(settings: AppSettings) -> str:
    """Return Silver customers Delta path."""
    return f"{settings.minio.silver_uri}/orders/customers"


def get_gold_sales_path(settings: AppSettings) -> str:
    """Return Gold sales Delta path."""
    return f"{settings.minio.gold_uri}/sales/daily_sales"


def read_silver_orders(spark: SparkSession, settings: AppSettings) -> DataFrame:
    """Read Silver orders Delta table."""
    return spark.read.format("delta").load(get_silver_orders_path(settings))


def read_silver_customers(spark: SparkSession, settings: AppSettings) -> DataFrame:
    """Read Silver customers Delta table."""
    return spark.read.format("delta").load(get_silver_customers_path(settings))


def filter_orders_for_date(dataframe: DataFrame, target_date: str) -> DataFrame:
    """Filter orders for a target order date."""
    return dataframe.filter(F.to_date("order_timestamp") == F.lit(target_date))


def join_orders_with_customers(orders_dataframe: DataFrame, customers_dataframe: DataFrame) -> DataFrame:
    """Join orders with customers for country analytics."""
    return orders_dataframe.join(
        customers_dataframe.select("customer_id", "country_code"),
        on="customer_id",
        how="left",
    )


def aggregate_daily_sales(dataframe: DataFrame) -> DataFrame:
    """Aggregate daily sales metrics by order date and country."""
    return (
        dataframe.withColumn("order_date", F.to_date("order_timestamp"))
        .groupBy("order_date", "country_code")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumnRenamed("country_code", "country")
    )


def write_daily_sales(dataframe: DataFrame, settings: AppSettings, target_date: str) -> int:
    """Write Gold daily sales table idempotently for one order_date."""
    output_path = get_gold_sales_path(settings)
    row_count = dataframe.count()

    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"order_date = '{target_date}'")
        .save(output_path)
    )

    LOGGER.info(
        "gold_daily_sales_written",
        target_date=target_date,
        row_count=row_count,
        output_path=output_path,
    )
    return int(row_count)


def build_daily_sales(
    spark: SparkSession,
    settings: AppSettings,
    date: str,
) -> dict[str, int | str]:
    """Build Gold daily sales aggregation for a given date."""
    orders_dataframe = read_silver_orders(spark, settings)
    customers_dataframe = read_silver_customers(spark, settings)

    filtered_orders = filter_orders_for_date(orders_dataframe, date)
    joined_dataframe = join_orders_with_customers(filtered_orders, customers_dataframe)
    gold_dataframe = aggregate_daily_sales(joined_dataframe)

    rows_written = write_daily_sales(gold_dataframe, settings, date)

    result = {
        "gold_table": "daily_sales",
        "date": date,
        "rows_written": rows_written,
    }
    LOGGER.info("gold_daily_sales_complete", **result)
    return result