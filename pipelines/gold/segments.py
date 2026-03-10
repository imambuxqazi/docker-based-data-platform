"""Gold customer segmentation using RFM scoring."""

from __future__ import annotations

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import AppSettings


LOGGER = structlog.get_logger(__name__)


def get_silver_orders_path(settings: AppSettings) -> str:
    """Return Silver orders Delta path."""
    return f"{settings.minio.silver_uri}/orders/orders"


def get_gold_segments_path(settings: AppSettings) -> str:
    """Return Gold customer segments Delta path."""
    return f"{settings.minio.gold_uri}/segments/customer_segments"


def read_silver_orders(spark: SparkSession, settings: AppSettings) -> DataFrame:
    """Read Silver orders Delta table."""
    return spark.read.format("delta").load(get_silver_orders_path(settings))


def build_rfm_base(dataframe: DataFrame, snapshot_date: str) -> DataFrame:
    """Build customer-level recency, frequency, and monetary features."""
    snapshot_date_column = F.to_date(F.lit(snapshot_date))

    return (
        dataframe.groupBy("customer_id")
        .agg(
            F.max(F.to_date("order_timestamp")).alias("last_order_date"),
            F.countDistinct("order_id").alias("frequency"),
            F.round(F.sum("total_amount"), 2).alias("monetary"),
        )
        .withColumn("snapshot_date", snapshot_date_column)
        .withColumn("recency_days", F.datediff(F.col("snapshot_date"), F.col("last_order_date")))
    )


def score_rfm_dimensions(dataframe: DataFrame) -> DataFrame:
    """Score recency, frequency, and monetary dimensions using NTILE(5)."""
    recency_window = Window.orderBy(F.col("recency_days").desc())
    frequency_window = Window.orderBy(F.col("frequency").asc())
    monetary_window = Window.orderBy(F.col("monetary").asc())

    return (
        dataframe.withColumn("recency_score", F.ntile(5).over(recency_window))
        .withColumn("frequency_score", F.ntile(5).over(frequency_window))
        .withColumn("monetary_score", F.ntile(5).over(monetary_window))
    )


def label_segments(dataframe: DataFrame) -> DataFrame:
    """Assign segment labels from RFM scores."""
    return dataframe.withColumn(
        "segment_label",
        F.when(
            (F.col("recency_score") >= 4) & (F.col("frequency_score") >= 4) & (F.col("monetary_score") >= 4),
            F.lit("Champions"),
        )
        .when(
            (F.col("recency_score") >= 3) & (F.col("frequency_score") >= 3),
            F.lit("Loyal"),
        )
        .when(
            (F.col("recency_score") <= 2) & (F.col("frequency_score") >= 3),
            F.lit("At_Risk"),
        )
        .when(
            (F.col("recency_score") == 1) & (F.col("frequency_score") <= 2),
            F.lit("Lost"),
        )
        .otherwise(F.lit("New_Customer"))
    )


def write_customer_segments(dataframe: DataFrame, settings: AppSettings, snapshot_date: str) -> int:
    """Write Gold customer segments idempotently for one snapshot_date."""
    output_path = get_gold_segments_path(settings)
    row_count = dataframe.count()

    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"snapshot_date = '{snapshot_date}'")
        .save(output_path)
    )

    LOGGER.info(
        "gold_customer_segments_written",
        snapshot_date=snapshot_date,
        row_count=row_count,
        output_path=output_path,
    )
    return int(row_count)


def build_customer_segments(
    spark: SparkSession,
    settings: AppSettings,
    snapshot_date: str,
) -> dict[str, int | str]:
    """Build Gold customer segments for a snapshot date."""
    orders_dataframe = read_silver_orders(spark, settings)
    rfm_base = build_rfm_base(orders_dataframe, snapshot_date)
    scored_dataframe = score_rfm_dimensions(rfm_base)
    labeled_dataframe = label_segments(scored_dataframe)

    rows_written = write_customer_segments(labeled_dataframe, settings, snapshot_date)

    result = {
        "gold_table": "customer_segments",
        "snapshot_date": snapshot_date,
        "rows_written": rows_written,
    }
    LOGGER.info("gold_customer_segments_complete", **result)
    return result