"""Unit tests for Gold aggregations."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipelines.gold.alerts import filter_inventory_alerts
from pipelines.gold.funnel import aggregate_product_funnel
from pipelines.gold.segments import label_segments, score_rfm_dimensions


def test_rfm_scores_are_between_1_and_5(spark_session: SparkSession) -> None:
    """Ensure NTILE-based RFM scores are always in the 1..5 range."""
    dataframe = spark_session.createDataFrame(
        [
            (1, 5, 100.0),
            (2, 10, 200.0),
            (3, 15, 300.0),
            (4, 20, 400.0),
            (5, 25, 500.0),
            (6, 30, 600.0),
        ],
        ["recency_days", "frequency", "monetary"],
    )

    scored = score_rfm_dimensions(dataframe)

    score_bounds = scored.select(
        F.min("recency_score").alias("min_recency"),
        F.max("recency_score").alias("max_recency"),
        F.min("frequency_score").alias("min_frequency"),
        F.max("frequency_score").alias("max_frequency"),
        F.min("monetary_score").alias("min_monetary"),
        F.max("monetary_score").alias("max_monetary"),
    ).collect()[0]

    assert 1 <= score_bounds["min_recency"] <= 5
    assert 1 <= score_bounds["max_recency"] <= 5
    assert 1 <= score_bounds["min_frequency"] <= 5
    assert 1 <= score_bounds["max_frequency"] <= 5
    assert 1 <= score_bounds["min_monetary"] <= 5
    assert 1 <= score_bounds["max_monetary"] <= 5


def test_funnel_conversion_rate_formula_is_correct(spark_session: SparkSession) -> None:
    """Ensure view-to-purchase and cart-abandonment rates are computed correctly."""
    dataframe = spark_session.createDataFrame(
        [
            ("2026-03-08 10:00:00", 101, "page_view"),
            ("2026-03-08 10:01:00", 101, "page_view"),
            ("2026-03-08 10:02:00", 101, "page_view"),
            ("2026-03-08 10:03:00", 101, "add_to_cart"),
            ("2026-03-08 10:04:00", 101, "add_to_cart"),
            ("2026-03-08 10:05:00", 101, "purchase"),
        ],
        ["event_timestamp", "product_id", "event_type"],
    ).withColumn("event_timestamp", F.to_timestamp("event_timestamp"))

    result = aggregate_product_funnel(dataframe).collect()[0]

    assert result["page_view_count"] == 3
    assert result["cart_count"] == 2
    assert result["purchase_count"] == 1
    assert round(result["view_to_purchase_rate"], 4) == round(1 / 3, 4)
    assert round(result["cart_abandonment_rate"], 4) == round((2 - 1) / 2, 4)


def test_cart_abandonment_threshold_filter_works_correctly(spark_session: SparkSession) -> None:
    """Ensure alert filter keeps only products meeting abandonment threshold criteria."""
    dataframe = spark_session.createDataFrame(
        [
            ("2026-03-08", 101, 20, 2, 0.90),
            ("2026-03-08", 102, 11, 4, 0.63),
            ("2026-03-08", 103, 9, 1, 0.89),
            ("2026-03-08", 104, 15, 8, 0.20),
        ],
        ["event_date", "product_id", "cart_count", "purchase_count", "cart_abandonment_rate"],
    )

    filtered = filter_inventory_alerts(dataframe)

    result = {
        row["product_id"]: row["severity"]
        for row in filtered.select("product_id", "severity").collect()
    }

    assert 101 in result
    assert 102 in result
    assert 103 not in result
    assert 104 not in result
    assert result[101] == "HIGH"
    assert result[102] == "MEDIUM"


def test_segment_labels_are_assigned_from_scores(spark_session: SparkSession) -> None:
    """Ensure segment labels map correctly from RFM score combinations."""
    dataframe = spark_session.createDataFrame(
        [
            (5, 5, 5),
            (4, 4, 3),
            (1, 4, 3),
            (1, 2, 2),
            (3, 1, 1),
        ],
        ["recency_score", "frequency_score", "monetary_score"],
    )

    labeled = label_segments(dataframe)
    labels = [row["segment_label"] for row in labeled.select("segment_label").collect()]

    assert "Champions" in labels
    assert "Loyal" in labels
    assert "At_Risk" in labels
    assert "Lost" in labels
    assert "New_Customer" in labels