"""Quality suite runner and reporting orchestration."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Callable

import structlog
from pyspark.sql import DataFrame

from alerts.manager import AlertManager
from quality.checks import (
    CheckResult,
    check_duplicate_rate,
    check_freshness,
    check_null_rate,
    check_referential_integrity,
    check_row_count,
    check_value_range,
)


LOGGER = structlog.get_logger(__name__)


@dataclass
class QualityReport:
    """Aggregated result of a quality suite run."""

    table_name: str
    passed: bool
    total_checks: int
    passed_checks: int
    failed_checks: int
    results: list[CheckResult]


class QualityRunner:
    """Run quality suites against Spark DataFrames and alert on failures.

    This class is stateful because it owns an AlertManager dependency and
    orchestrates multiple checks with shared reporting behavior.
    """

    def __init__(self, alert_manager: AlertManager) -> None:
        """Initialize the quality runner.

        Args:
            alert_manager: Alert manager used for failed checks.
        """
        self.alert_manager = alert_manager
        self.logger = LOGGER.bind(component="quality_runner")

    def _run_single_check(
        self,
        dataframe: DataFrame,
        table_name: str,
        check_config: dict[str, Any],
    ) -> CheckResult:
        """Execute a single configured check.

        Args:
            dataframe: Input dataframe.
            table_name: Logical table name.
            check_config: Check configuration dictionary.

        Returns:
            CheckResult for the executed rule.
        """
        check_type = check_config["check_type"]

        if check_type == "null_rate":
            return check_null_rate(
                dataframe=dataframe,
                table_name=table_name,
                column_name=check_config["column_name"],
                max_null_rate=check_config["max_null_rate"],
            )

        if check_type == "duplicate_rate":
            return check_duplicate_rate(
                dataframe=dataframe,
                table_name=table_name,
                key_columns=check_config["key_columns"],
                max_duplicate_rate=check_config["max_duplicate_rate"],
            )

        if check_type == "row_count":
            return check_row_count(
                dataframe=dataframe,
                table_name=table_name,
                min_row_count=check_config["min_row_count"],
            )

        if check_type == "value_range":
            return check_value_range(
                dataframe=dataframe,
                table_name=table_name,
                column_name=check_config["column_name"],
                min_value=check_config["min_value"],
                max_value=check_config["max_value"],
            )

        if check_type == "freshness":
            return check_freshness(
                dataframe=dataframe,
                table_name=table_name,
                timestamp_column=check_config["timestamp_column"],
                max_age_minutes=check_config["max_age_minutes"],
            )

        if check_type == "referential_integrity":
            return check_referential_integrity(
                child_dataframe=dataframe,
                parent_dataframe=check_config["parent_dataframe"],
                table_name=table_name,
                child_key_column=check_config["child_key_column"],
                parent_key_column=check_config["parent_key_column"],
            )

        raise ValueError(f"Unsupported check type: {check_type}")

    def _alert_on_failure(self, result: CheckResult) -> None:
        """Send an alert for a failed check.

        Args:
            result: Failed check result.
        """
        self.alert_manager.send_alert(
            alert_source="quality_runner",
            alert_type=result.check_name,
            severity="HIGH",
            table_name=result.table_name,
            pipeline_name="data_quality",
            alert_message=result.message,
            alert_payload=asdict(result),
        )

    def run_suite(
        self,
        dataframe: DataFrame,
        table_name: str,
        checks_config: list[dict[str, Any]],
    ) -> QualityReport:
        """Run a suite of configured checks against a dataframe.

        Args:
            dataframe: Input dataframe.
            table_name: Logical table name.
            checks_config: List of check configuration dictionaries.

        Returns:
            QualityReport containing all results.
        """
        results: list[CheckResult] = []

        for check_config in checks_config:
            result = self._run_single_check(
                dataframe=dataframe,
                table_name=table_name,
                check_config=check_config,
            )
            results.append(result)

            if result.passed:
                self.logger.info(
                    "quality_check_passed",
                    table_name=table_name,
                    check_name=result.check_name,
                    metric_value=result.metric_value,
                    threshold_value=result.threshold_value,
                    message=result.message,
                )
            else:
                self.logger.error(
                    "quality_check_failed",
                    table_name=table_name,
                    check_name=result.check_name,
                    metric_value=result.metric_value,
                    threshold_value=result.threshold_value,
                    message=result.message,
                )
                self._alert_on_failure(result)

        total_checks = len(results)
        passed_checks = sum(1 for result in results if result.passed)
        failed_checks = total_checks - passed_checks

        report = QualityReport(
            table_name=table_name,
            passed=failed_checks == 0,
            total_checks=total_checks,
            passed_checks=passed_checks,
            failed_checks=failed_checks,
            results=results,
        )

        self.logger.info(
            "quality_suite_complete",
            table_name=table_name,
            passed=report.passed,
            total_checks=report.total_checks,
            passed_checks=report.passed_checks,
            failed_checks=report.failed_checks,
        )

        return report