"""Analytics query functions for Gold Delta tables."""

from __future__ import annotations

from typing import Any

import pandas as pd
import structlog
from pyspark.sql import DataFrame, SparkSession

from config.settings import AppSettings, get_settings
from config.spark import SparkSessionBuilder


LOGGER = structlog.get_logger(__name__)


def register_gold_views(spark: SparkSession, settings: AppSettings) -> None:
    """Register Gold Delta tables as temporary Spark SQL views.

    Args:
        spark: Active Spark session.
        settings: Application settings.
    """
    spark.read.format("delta").load(
        f"{settings.minio.gold_uri}/sales/daily_sales"
    ).createOrReplaceTempView("gold_daily_sales")

    spark.read.format("delta").load(
        f"{settings.minio.gold_uri}/funnel/product_funnel"
    ).createOrReplaceTempView("gold_product_funnel")

    spark.read.format("delta").load(
        f"{settings.minio.gold_uri}/segments/customer_segments"
    ).createOrReplaceTempView("gold_customer_segments")

    spark.read.format("delta").load(
        f"{settings.minio.gold_uri}/traffic/hourly_traffic"
    ).createOrReplaceTempView("gold_hourly_traffic")

    spark.read.format("delta").load(
        f"{settings.minio.gold_uri}/alerts/inventory_alerts"
    ).createOrReplaceTempView("gold_inventory_alerts")

    LOGGER.info("gold_views_registered")


def convert_to_pandas(dataframe: DataFrame) -> pd.DataFrame:
    """Convert a Spark DataFrame to pandas DataFrame.

    Args:
        dataframe: Spark dataframe.

    Returns:
        pandas DataFrame.
    """
    return dataframe.toPandas()


def print_formatted_table(dataframe: pd.DataFrame, title: str) -> None:
    """Print a clean formatted table to console.

    Args:
        dataframe: pandas DataFrame to print.
        title: Display title.
    """
    print(f"\n=== {title} ===")
    if dataframe.empty:
        print("No rows returned.")
        return

    print(dataframe.to_string(index=False))


def top_revenue_products(
    spark: SparkSession,
    settings: AppSettings,
    start_date: str,
    end_date: str,
    top_n: int = 10,
) -> pd.DataFrame:
    """Return top products by revenue between two dates.

    Args:
        spark: Active Spark session.
        settings: Application settings.
        start_date: Inclusive start date in YYYY-MM-DD format.
        end_date: Inclusive end date in YYYY-MM-DD format.
        top_n: Number of products to return.

    Returns:
        pandas DataFrame of top revenue products.
    """
    register_gold_views(spark, settings)

    query = f"""
        SELECT
            product_id,
            SUM(purchase_count) AS purchases,
            SUM(cart_count) AS carts,
            ROUND(AVG(view_to_purchase_rate), 4) AS avg_view_to_purchase_rate,
            ROUND(AVG(cart_abandonment_rate), 4) AS avg_cart_abandonment_rate
        FROM gold_product_funnel
        WHERE event_date BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        GROUP BY product_id
        ORDER BY purchases DESC, carts DESC
        LIMIT {int(top_n)}
    """

    pandas_dataframe = convert_to_pandas(spark.sql(query))
    print_formatted_table(pandas_dataframe, "Top Revenue Products Proxy")
    return pandas_dataframe


def conversion_funnel(
    spark: SparkSession,
    settings: AppSettings,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Return aggregated funnel metrics for a date range.

    Args:
        spark: Active Spark session.
        settings: Application settings.
        start_date: Inclusive start date.
        end_date: Inclusive end date.

    Returns:
        pandas DataFrame of funnel metrics.
    """
    register_gold_views(spark, settings)

    query = f"""
        SELECT
            SUM(page_view_count) AS total_page_views,
            SUM(product_click_count) AS total_product_clicks,
            SUM(cart_count) AS total_add_to_cart,
            SUM(checkout_count) AS total_checkouts,
            SUM(purchase_count) AS total_purchases,
            ROUND(
                CASE
                    WHEN SUM(page_view_count) > 0
                    THEN SUM(purchase_count) * 1.0 / SUM(page_view_count)
                    ELSE 0
                END,
                4
            ) AS overall_view_to_purchase_rate,
            ROUND(
                CASE
                    WHEN SUM(cart_count) > 0
                    THEN (SUM(cart_count) - SUM(purchase_count)) * 1.0 / SUM(cart_count)
                    ELSE 0
                END,
                4
            ) AS overall_cart_abandonment_rate
        FROM gold_product_funnel
        WHERE event_date BETWEEN DATE('{start_date}') AND DATE('{end_date}')
    """

    pandas_dataframe = convert_to_pandas(spark.sql(query))
    print_formatted_table(pandas_dataframe, "Conversion Funnel")
    return pandas_dataframe


def hourly_traffic(
    spark: SparkSession,
    settings: AppSettings,
    date: str,
) -> pd.DataFrame:
    """Return hourly traffic metrics for a date.

    Args:
        spark: Active Spark session.
        settings: Application settings.
        date: Target date.

    Returns:
        pandas DataFrame of hourly traffic metrics.
    """
    register_gold_views(spark, settings)

    query = f"""
        SELECT
            event_date,
            event_hour,
            active_users,
            total_events,
            ROUND(bounce_rate, 4) AS bounce_rate,
            purchases,
            ROUND(avg_session_duration, 2) AS avg_session_duration
        FROM gold_hourly_traffic
        WHERE event_date = DATE('{date}')
        ORDER BY event_hour ASC
    """

    pandas_dataframe = convert_to_pandas(spark.sql(query))
    print_formatted_table(pandas_dataframe, "Hourly Traffic")
    return pandas_dataframe


def customer_segments(
    spark: SparkSession,
    settings: AppSettings,
    snapshot_date: str,
) -> pd.DataFrame:
    """Return customer segment distribution for a snapshot date.

    Args:
        spark: Active Spark session.
        settings: Application settings.
        snapshot_date: Snapshot date.

    Returns:
        pandas DataFrame of customer segment counts.
    """
    register_gold_views(spark, settings)

    query = f"""
        SELECT
            snapshot_date,
            segment_label,
            COUNT(*) AS customer_count,
            ROUND(AVG(recency_days), 2) AS avg_recency_days,
            ROUND(AVG(frequency), 2) AS avg_frequency,
            ROUND(AVG(monetary), 2) AS avg_monetary
        FROM gold_customer_segments
        WHERE snapshot_date = DATE('{snapshot_date}')
        GROUP BY snapshot_date, segment_label
        ORDER BY customer_count DESC, segment_label ASC
    """

    pandas_dataframe = convert_to_pandas(spark.sql(query))
    print_formatted_table(pandas_dataframe, "Customer Segments")
    return pandas_dataframe


def cart_abandonment(
    spark: SparkSession,
    settings: AppSettings,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Return products with the highest cart abandonment in a date range.

    Args:
        spark: Active Spark session.
        settings: Application settings.
        start_date: Inclusive start date.
        end_date: Inclusive end date.

    Returns:
        pandas DataFrame of cart abandonment metrics.
    """
    register_gold_views(spark, settings)

    query = f"""
        SELECT
            product_id,
            SUM(cart_count) AS total_cart_count,
            SUM(purchase_count) AS total_purchase_count,
            ROUND(
                CASE
                    WHEN SUM(cart_count) > 0
                    THEN (SUM(cart_count) - SUM(purchase_count)) * 1.0 / SUM(cart_count)
                    ELSE 0
                END,
                4
            ) AS cart_abandonment_rate
        FROM gold_product_funnel
        WHERE event_date BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        GROUP BY product_id
        HAVING SUM(cart_count) > 0
        ORDER BY cart_abandonment_rate DESC, total_cart_count DESC
    """

    pandas_dataframe = convert_to_pandas(spark.sql(query))
    print_formatted_table(pandas_dataframe, "Cart Abandonment")
    return pandas_dataframe


if __name__ == "__main__":
    from datetime import datetime, timezone

    active_settings = get_settings()
    spark_session = SparkSessionBuilder(settings=active_settings).build("analytics_queries")

    try:
        today_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        top_revenue_products(spark_session, active_settings, today_date, today_date, top_n=10)
        conversion_funnel(spark_session, active_settings, today_date, today_date)
        hourly_traffic(spark_session, active_settings, today_date)
        customer_segments(spark_session, active_settings, today_date)
        cart_abandonment(spark_session, active_settings, today_date, today_date)
    finally:
        spark_session.stop()