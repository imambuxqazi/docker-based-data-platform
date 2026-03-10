"""Airflow DAG for the clickstream Bronze/Silver/Gold/Quality pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import socket

import structlog
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule

from alerts.manager import AlertManager
from config.settings import get_settings
from config.spark import SparkSessionBuilder
from pipelines.ingestion.clickstream import ingest_clickstream_bronze_batch
from pipelines.gold.alerts import build_inventory_alerts
from pipelines.gold.funnel import build_product_funnel
from pipelines.gold.traffic import build_hourly_traffic
from pipelines.silver.clickstream import process_clickstream_silver_partition
from quality.runner import QualityRunner


LOGGER = structlog.get_logger(__name__)


DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "sla": timedelta(minutes=30),
}


@dag(
    dag_id="clickstream_pipeline",
    description="Clickstream Bronze/Silver/Gold/Quality pipeline.",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 3, 8, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["clickstream", "bronze", "silver", "gold"],
)
def clickstream_pipeline():
    """Define the clickstream pipeline DAG."""

    @task()
    def check_redpanda_health() -> dict[str, str | bool]:
        """Validate Redpanda broker socket connectivity.

        Returns:
            Health check result payload.

        Raises:
            AirflowFailException: If Redpanda is unreachable.
        """
        settings = get_settings()
        broker = settings.kafka.bootstrap_servers.split(",")[0].strip()
        host, port = broker.split(":")
        port_number = int(port)

        try:
            with socket.create_connection((host, port_number), timeout=5):
                pass
        except OSError as error:
            LOGGER.exception("clickstream_redpanda_health_check_failed")
            raise AirflowFailException(f"Redpanda health check failed: {error}") from error

        result = {"service": "redpanda", "healthy": True}
        LOGGER.info("clickstream_redpanda_health_check_passed", **result)
        return result

    @task()
    def ingest_bronze(_: dict[str, str | bool]) -> dict[str, object]:
        """Run one bounded Bronze ingestion batch from Redpanda to MinIO.

        Args:
            _: Redpanda health check result.

        Returns:
            Bronze ingestion result payload.

        Raises:
            AirflowFailException: If Bronze ingestion fails or ingests zero rows.
        """
        try:
            result = ingest_clickstream_bronze_batch(settings=get_settings())

            if int(result.get("rows_ingested", 0)) == 0:
                raise AirflowFailException(
                    "Clickstream Bronze ingestion completed but no new rows were ingested. "
                    "Generate clickstream events before running the DAG."
                )

            LOGGER.info("clickstream_bronze_ingestion_complete", **result)
            return result
        except Exception as error:
            LOGGER.exception("clickstream_bronze_ingestion_failed")
            raise AirflowFailException(f"Clickstream Bronze ingestion failed: {error}") from error

    @task()
    def process_silver(bronze_result: dict[str, object]) -> dict[str, object]:
        """Process today's Bronze clickstream partition into Silver.

        Args:
            bronze_result: Bronze ingestion result payload.

        Returns:
            Silver processing result payload.
        """
        del bronze_result
        partition_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        result = process_clickstream_silver_partition(
            partition_date=partition_date,
            settings=get_settings(),
        )
        LOGGER.info("clickstream_silver_processing_complete", **result)
        return result

    @task()
    def process_gold(silver_result: dict[str, object]) -> dict[str, object]:
        """Build clickstream Gold marts.

        Args:
            silver_result: Silver processing result payload.

        Returns:
            Gold processing result payload.
        """
        del silver_result
        settings = get_settings()
        spark = SparkSessionBuilder(settings=settings).build("airflow_clickstream_gold")
        target_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        try:
            funnel_result = build_product_funnel(spark, settings, target_date)
            traffic_result = build_hourly_traffic(spark, settings, target_date)
            alerts_result = build_inventory_alerts(spark, settings)

            result = {
                "funnel": funnel_result,
                "traffic": traffic_result,
                "alerts": alerts_result,
            }
            LOGGER.info("clickstream_gold_processing_complete", **result)
            return result
        finally:
            spark.stop()

    @task()
    def run_quality(gold_result: dict[str, object]) -> dict[str, object]:
        """Run data quality checks on clickstream Gold outputs.

        Args:
            gold_result: Gold processing result payload.

        Returns:
            Quality result summary.

        Raises:
            AirflowFailException: If the quality suite fails.
        """
        del gold_result
        settings = get_settings()
        spark = SparkSessionBuilder(settings=settings).build("airflow_clickstream_quality")
        alert_manager = AlertManager(settings=settings)
        quality_runner = QualityRunner(alert_manager=alert_manager)

        target_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        try:
            funnel_dataframe = (
                spark.read.format("delta")
                .load(f"{settings.minio.gold_uri}/funnel/product_funnel")
                .filter(f"event_date = DATE('{target_date}')")
            )

            quality_report = quality_runner.run_suite(
                dataframe=funnel_dataframe,
                table_name="gold_product_funnel",
                checks_config=[
                    {"check_type": "row_count", "min_row_count": 1},
                    {"check_type": "null_rate", "column_name": "product_id", "max_null_rate": 0.0},
                    {
                        "check_type": "duplicate_rate",
                        "key_columns": ["event_date", "product_id"],
                        "max_duplicate_rate": 0.0,
                    },
                    {
                        "check_type": "value_range",
                        "column_name": "cart_abandonment_rate",
                        "min_value": 0,
                        "max_value": 1,
                    },
                    {
                        "check_type": "value_range",
                        "column_name": "view_to_purchase_rate",
                        "min_value": 0,
                        "max_value": 1,
                    },
                ],
            )

            result = {
                "table_name": quality_report.table_name,
                "passed": quality_report.passed,
                "total_checks": quality_report.total_checks,
                "passed_checks": quality_report.passed_checks,
                "failed_checks": quality_report.failed_checks,
            }

            LOGGER.info("clickstream_quality_complete", **result)

            if not quality_report.passed:
                raise AirflowFailException(f"Clickstream quality suite failed: {result}")

            return result
        finally:
            spark.stop()

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def notify_on_failure() -> None:
        """Persist a pipeline failure alert if any upstream task fails."""
        settings = get_settings()
        alert_manager = AlertManager(settings=settings)
        alert_manager.send_pipeline_alert(
            pipeline_name="clickstream_pipeline",
            severity="CRITICAL",
            alert_message="Clickstream pipeline encountered a task failure.",
            alert_payload={"dag_id": "clickstream_pipeline"},
        )
        LOGGER.error("clickstream_pipeline_failure_alert_sent")

    redpanda_health = check_redpanda_health()
    bronze_result = ingest_bronze(redpanda_health)
    silver_result = process_silver(bronze_result)
    gold_result = process_gold(silver_result)
    quality_result = run_quality(gold_result)
    notify_on_failure()

    redpanda_health >> bronze_result >> silver_result >> gold_result >> quality_result


clickstream_pipeline()