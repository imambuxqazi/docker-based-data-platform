"""Shared pytest fixtures for the test suite."""

from __future__ import annotations

import os

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    """Provide a local Spark session for unit tests.

    Returns:
        A SparkSession configured for fast local unit testing.
    """
    os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python"

    spark = (
        SparkSession.builder.appName("data-platform-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )

    yield spark

    spark.stop()