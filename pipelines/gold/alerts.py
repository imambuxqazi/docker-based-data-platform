"""Gold cart-abandonment alert aggregations."""

from __future__ import annotations

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.settings import AppSettings


LOGGER = structlog.get_logger(__name__)


def get_gold_funnel_path(settings: AppSettings) -> str:
    """Return Gold product funnel Delta path."""
    return f"{settings.minio.gold_uri}/funnel/product_funnel"


def get_gold_alerts_path(settings: AppSettings) -> str:
    """Return Gold inventory alerts Delta path."""
    return f"{settings.minio.gold_uri}/alerts/inventory_alerts"


def read_gold_product_funnel(spark: SparkSession, settings: AppSettings) -> DataFrame:
    """Read Gold product funnel Delta table."""
    return spark.read.format("delta").load(get_gold_funnel_path(settings))


def filter_inventory_alerts(dataframe: DataFrame) -> DataFrame:
    """Filter product funnel rows that should trigger alerts."""
    return (
        dataframe.filter((F.col("cart_abandonment_rate") > F.lit(0.6)) & (F.col("cart_count") > F.lit(10)))
        .withColumn(
            "severity",
            F.when(F.col("cart_abandonment_rate") > F.lit(0.8), F.lit("HIGH")).otherwise(F.lit("MEDIUM")),
        )
    )


def write_inventory_alerts(dataframe: DataFrame, settings: AppSettings) -> int:
    """Write full Gold inventory alerts table."""
    output_path = get_gold_alerts_path(settings)
    row_count = dataframe.count()

    dataframe.write.format("delta").mode("overwrite").save(output_path)

    LOGGER.info(
        "gold_inventory_alerts_written",
        row_count=row_count,
        output_path=output_path,
    )
    return int(row_count)


def build_inventory_alerts(
    spark: SparkSession,
    settings: AppSettings,
) -> dict[str, int | str]:
    """Build Gold inventory alerts from product funnel metrics."""
    funnel_dataframe = read_gold_product_funnel(spark, settings)
    alerts_dataframe = filter_inventory_alerts(funnel_dataframe)

    rows_written = write_inventory_alerts(alerts_dataframe, settings)

    result = {
        "gold_table": "inventory_alerts",
        "rows_written": rows_written,
    }
    LOGGER.info("gold_inventory_alerts_complete", **result)
    return result