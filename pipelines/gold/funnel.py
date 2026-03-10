"""Gold product funnel aggregations."""

from __future__ import annotations

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.settings import AppSettings, get_settings


LOGGER = structlog.get_logger(__name__)


def get_silver_clickstream_path(settings: AppSettings) -> str:
    """Return Silver clickstream Delta path."""
    return f"{settings.minio.silver_uri}/clickstream/events"


def get_gold_funnel_path(settings: AppSettings) -> str:
    """Return Gold product funnel Delta path."""
    return f"{settings.minio.gold_uri}/funnel/product_funnel"


def read_silver_clickstream(spark: SparkSession, settings: AppSettings) -> DataFrame:
    """Read Silver clickstream Delta table."""
    return spark.read.format("delta").load(get_silver_clickstream_path(settings))


def filter_clickstream_for_date(dataframe: DataFrame, target_date: str) -> DataFrame:
    """Filter clickstream events for a given date."""
    return dataframe.filter(F.to_date("event_timestamp") == F.lit(target_date))


def aggregate_product_funnel(dataframe: DataFrame) -> DataFrame:
    """Aggregate product funnel stage counts and rates."""
    aggregated = (
        dataframe.filter(F.col("product_id").isNotNull())
        .groupBy(F.to_date("event_timestamp").alias("event_date"), "product_id")
        .agg(
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
            F.sum(F.when(F.col("event_type") == "product_click", 1).otherwise(0)).alias("product_click_count"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_count"),
            F.sum(F.when(F.col("event_type") == "checkout", 1).otherwise(0)).alias("checkout_count"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
        )
    )

    return (
        aggregated.withColumn(
            "view_to_purchase_rate",
            F.when(F.col("page_view_count") > 0, F.col("purchase_count") / F.col("page_view_count"))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "cart_abandonment_rate",
            F.when(F.col("cart_count") > 0, (F.col("cart_count") - F.col("purchase_count")) / F.col("cart_count"))
            .otherwise(F.lit(0.0)),
        )
    )


def write_product_funnel(dataframe: DataFrame, settings: AppSettings, target_date: str) -> int:
    """Write Gold product funnel idempotently for one event_date."""
    output_path = get_gold_funnel_path(settings)
    row_count = dataframe.count()

    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"event_date = '{target_date}'")
        .save(output_path)
    )

    LOGGER.info(
        "gold_product_funnel_written",
        target_date=target_date,
        row_count=row_count,
        output_path=output_path,
    )
    return int(row_count)


def build_product_funnel(
    spark: SparkSession,
    settings: AppSettings,
    date: str,
) -> dict[str, int | str]:
    """Build Gold product funnel aggregation for a date."""
    clickstream_dataframe = read_silver_clickstream(spark, settings)
    filtered_dataframe = filter_clickstream_for_date(clickstream_dataframe, date)
    gold_dataframe = aggregate_product_funnel(filtered_dataframe)

    rows_written = write_product_funnel(gold_dataframe, settings, date)

    result = {
        "gold_table": "product_funnel",
        "date": date,
        "rows_written": rows_written,
    }
    LOGGER.info("gold_product_funnel_complete", **result)
    return result