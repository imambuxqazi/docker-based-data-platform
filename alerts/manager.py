"""Alert management for pipeline and quality failures."""

from __future__ import annotations

import json
from typing import Any

import psycopg2
import structlog
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from config.settings import AppSettings, get_settings


LOGGER = structlog.get_logger(__name__)


class AlertManager:
    """Persist alerts into PostgreSQL for operational visibility."""

    def __init__(self, settings: AppSettings | None = None) -> None:
        """Initialize the alert manager.

        Args:
            settings: Optional application settings.
        """
        self.settings = settings or get_settings()
        self.logger = LOGGER.bind(component="alert_manager")

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(psycopg2.Error),
    )
    def send_alert(
        self,
        alert_source: str,
        alert_type: str,
        severity: str,
        table_name: str | None,
        pipeline_name: str | None,
        alert_message: str,
        alert_payload: dict[str, Any] | None = None,
    ) -> None:
        """Persist an alert row into PostgreSQL.

        Args:
            alert_source: Source component generating the alert.
            alert_type: Alert classification.
            severity: Alert severity.
            table_name: Related table name if applicable.
            pipeline_name: Related pipeline name if applicable.
            alert_message: Human-readable alert message.
            alert_payload: Optional structured payload.
        """
        postgres_settings = self.settings.postgres

        connection = psycopg2.connect(
            host=postgres_settings.host,
            port=postgres_settings.port,
            dbname=postgres_settings.database,
            user=postgres_settings.user,
            password=postgres_settings.password,
        )

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO alerts (
                        alert_source,
                        alert_type,
                        severity,
                        table_name,
                        pipeline_name,
                        alert_message,
                        alert_payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb);
                    """,
                    (
                        alert_source,
                        alert_type,
                        severity,
                        table_name,
                        pipeline_name,
                        alert_message,
                        json.dumps(alert_payload or {}),
                    ),
                )
                connection.commit()

            self.logger.info(
                "alert_persisted",
                alert_source=alert_source,
                alert_type=alert_type,
                severity=severity,
                table_name=table_name,
                pipeline_name=pipeline_name,
            )
        finally:
            connection.close()

    def send_pipeline_alert(
        self,
        pipeline_name: str,
        severity: str,
        alert_message: str,
        alert_payload: dict[str, Any] | None = None,
    ) -> None:
        """Persist a pipeline-level operational alert.

        Args:
            pipeline_name: Pipeline or DAG name.
            severity: Alert severity.
            alert_message: Human-readable message.
            alert_payload: Optional structured payload.
        """
        self.send_alert(
            alert_source="airflow_pipeline",
            alert_type="pipeline_failure",
            severity=severity,
            table_name=None,
            pipeline_name=pipeline_name,
            alert_message=alert_message,
            alert_payload=alert_payload,
        )