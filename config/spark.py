"""Spark session builder for the data platform.

This module centralizes SparkSession construction so every pipeline
uses a consistent runtime configuration for Delta Lake, S3A, and MinIO.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
import structlog
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from config.settings import AppSettings, get_settings


LOGGER = structlog.get_logger(__name__)


class SparkSessionBuilder:
    """Build and configure Spark sessions for batch and streaming workloads.

    This class wraps SparkSession creation because it manages external
    runtime configuration and depends on environment-backed settings.
    """

    def __init__(self, settings: AppSettings | None = None) -> None:
        """Initialize the Spark session builder.

        Args:
            settings: Optional application settings. If not provided,
                settings are loaded from the cached singleton.
        """
        self.settings = settings or get_settings()
        self.logger = LOGGER.bind(component="spark_session_builder")

    @property
    def spark_packages(self) -> str:
        """Return the Maven packages required for Delta and S3A support."""
        return ",".join(
            [
                "io.delta:delta-spark_2.12:3.2.0",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "org.postgresql:postgresql:42.7.3",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            ]
        )

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception_type(Exception),
    )
    def build(self, app_name: str) -> SparkSession:
        """Build and return a configured SparkSession.

        Args:
            app_name: Spark application name.

        Returns:
            A configured SparkSession instance.

        Raises:
            Exception: Raised if SparkSession creation fails after retries.
        """
        minio_settings = self.settings.minio
        spark_settings = self.settings.spark

        self.logger.info(
            "building_spark_session",
            app_name=app_name,
            spark_master_url=spark_settings.master_url,
            s3a_endpoint=minio_settings.s3a_endpoint,
            bronze_bucket=minio_settings.bronze_bucket,
            silver_bucket=minio_settings.silver_bucket,
            gold_bucket=minio_settings.gold_bucket,
        )

        spark = (
            SparkSession.builder.appName(app_name)
            .master(spark_settings.master_url)
            .config("spark.jars.packages", self.spark_packages)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", str(spark_settings.sql_shuffle_partitions))
            .config("spark.driver.memory", spark_settings.driver_memory)
            .config("spark.executor.memory", spark_settings.executor_memory)
            .config("spark.pyspark.python", "/opt/conda/bin/python")
            .config("spark.pyspark.driver.python", "/opt/conda/bin/python")
            .config("spark.executorEnv.PYSPARK_PYTHON", "/opt/conda/bin/python")
            .config("spark.executorEnv.PYSPARK_DRIVER_PYTHON", "/opt/conda/bin/python")
            .config("spark.hadoop.fs.s3a.endpoint", minio_settings.s3a_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", minio_settings.access_key)
            .config("spark.hadoop.fs.s3a.secret.key", minio_settings.secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.hadoop.fs.s3a.multipart.size", "67108864")
            .config("spark.hadoop.fs.s3a.connection.maximum", "100")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel(self.settings.log_level.upper())

        self.logger.info(
            "spark_session_built",
            app_name=app_name,
            application_id=spark.sparkContext.applicationId,
            spark_version=spark.version,
        )

        return spark