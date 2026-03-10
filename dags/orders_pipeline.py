"""Airflow DAG for the PostgreSQL orders Bronze/Silver/Gold pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg2
import structlog
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule

from alerts.manager import AlertManager
from config.settings import get_settings
from config.spark import SparkSessionBuilder
from pipelines.ingestion.orders import ingest_all_orders_tables
from pipelines.silver.orders import process_orders_silver_partition
from pipelines.gold.sales import build_daily_sales
from pipelines.gold.segments import build_customer_segments
from quality.runner import QualityRunner


LOGGER = structlog.get_logger(__name__)


DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "sla": timedelta(minutes=30),
}


@dag(
    dag_id="orders_pipeline",
    description="Polling CDC pipeline for PostgreSQL orders into Bronze, Silver, and Gold layers.",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 3, 8, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["orders", "bronze", "silver", "gold"],
)
def orders_pipeline():
    """Define the orders pipeline DAG."""

    @task()
    def check_postgres_health() -> dict[str, str | bool]:
        """Validate PostgreSQL connectivity before pipeline execution."""
        settings = get_settings()
        postgres_settings = settings.postgres

        try:
            connection = psycopg2.connect(
                host=postgres_settings.host,
                port=postgres_settings.port,
                dbname=postgres_settings.database,
                user=postgres_settings.user,
                password=postgres_settings.password,
            )
            connection.close()
        except psycopg2.Error as error:
            LOGGER.exception("orders_postgres_health_check_failed")
            raise AirflowFailException(f"PostgreSQL health check failed: {error}") from error

        result = {"service": "postgres", "healthy": True}
        LOGGER.info("orders_postgres_health_check_passed", **result)
        return result

    @task()
    def poll_and_ingest(_: dict[str, str | bool]) -> dict[str, object]:
        """Run one Bronze polling CDC cycle for order-domain tables."""
        result = ingest_all_orders_tables(settings=get_settings())
        LOGGER.info("orders_bronze_ingestion_complete", **result)
        return result

    @task()
    def process_silver(ingestion_result: dict[str, object]) -> dict[str, object]:
        """Process today's orders Bronze partition into Silver."""
        del ingestion_result
        partition_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        result = process_orders_silver_partition(
            partition_date=partition_date,
            settings=get_settings(),
        )
        LOGGER.info("orders_silver_processing_complete", **result)
        return result

    @task()
    def process_gold(silver_result: dict[str, object]) -> dict[str, object]:
        """Build Gold marts for the orders domain."""
        del silver_result
        settings = get_settings()
        spark = SparkSessionBuilder(settings=settings).build("airflow_orders_gold")

        target_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        try:
            sales_result = build_daily_sales(spark, settings, target_date)
            segments_result = build_customer_segments(spark, settings, target_date)

            result = {
                "sales": sales_result,
                "segments": segments_result,
            }
            LOGGER.info("orders_gold_processing_complete", **result)
            return result
        finally:
            spark.stop()

    @task()
    def run_quality(gold_result: dict[str, object]) -> dict[str, object]:
        """Run data quality checks on orders Gold outputs."""
        del gold_result
        settings = get_settings()
        spark = SparkSessionBuilder(settings=settings).build("airflow_orders_quality")
        alert_manager = AlertManager(settings=settings)
        quality_runner = QualityRunner(alert_manager=alert_manager)

        target_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        try:
            daily_sales_dataframe = (
                spark.read.format("delta")
                .load(f"{settings.minio.gold_uri}/sales/daily_sales")
                .filter(f"order_date = DATE('{target_date}')")
            )

            quality_report = quality_runner.run_suite(
                dataframe=daily_sales_dataframe,
                table_name="gold_daily_sales",
                checks_config=[
                    {"check_type": "row_count", "min_row_count": 1},
                    {"check_type": "null_rate", "column_name": "country", "max_null_rate": 0.2},
                    {
                        "check_type": "duplicate_rate",
                        "key_columns": ["order_date", "country"],
                        "max_duplicate_rate": 0.0,
                    },
                    {
                        "check_type": "value_range",
                        "column_name": "total_revenue",
                        "min_value": 0,
                        "max_value": 1_000_000_000,
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

            LOGGER.info("orders_quality_complete", **result)

            if not quality_report.passed:
                raise AirflowFailException(f"Orders quality suite failed: {result}")

            return result
        finally:
            spark.stop()

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def notify_on_failure() -> None:
        """Persist a pipeline failure alert if any upstream task fails."""
        settings = get_settings()
        alert_manager = AlertManager(settings=settings)
        alert_manager.send_pipeline_alert(
            pipeline_name="orders_pipeline",
            severity="CRITICAL",
            alert_message="Orders pipeline encountered a task failure.",
            alert_payload={"dag_id": "orders_pipeline"},
        )
        LOGGER.error("orders_pipeline_failure_alert_sent")

    postgres_health = check_postgres_health()
    bronze_result = poll_and_ingest(postgres_health)
    silver_result = process_silver(bronze_result)
    gold_result = process_gold(silver_result)
    quality_result = run_quality(gold_result)
    notify_on_failure()

    postgres_health >> bronze_result >> silver_result >> gold_result >> quality_result


orders_pipeline()