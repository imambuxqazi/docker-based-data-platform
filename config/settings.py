"""Application settings loaded from environment variables.

This module provides strongly typed configuration models for the
data platform. It uses Pydantic v2 and pydantic-settings to load
environment variables from the project .env file.

The main entry point is get_settings(), which returns a cached
AppSettings instance for singleton-style access across the codebase.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE_PATH = PROJECT_ROOT / ".env"


class PostgresSettings(BaseModel):
    """PostgreSQL connection and operational settings."""

    host: str = Field(..., description="PostgreSQL hostname or service name.")
    port: int = Field(..., description="PostgreSQL port.")
    database: str = Field(..., description="PostgreSQL database name.")
    user: str = Field(..., description="PostgreSQL username.")
    password: str = Field(..., description="PostgreSQL password.")

    @property
    def connection_url(self) -> str:
        """Return a SQLAlchemy-compatible PostgreSQL connection URL."""
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @property
    def psycopg2_dsn(self) -> str:
        """Return a psycopg2-compatible DSN string."""
        return (
            f"host={self.host} "
            f"port={self.port} "
            f"dbname={self.database} "
            f"user={self.user} "
            f"password={self.password}"
        )


class MinIOSettings(BaseModel):
    """MinIO / S3A storage settings."""

    endpoint: str = Field(..., description="MinIO endpoint URL.")
    api_port: int = Field(..., description="MinIO API port exposed on host.")
    console_port: int = Field(..., description="MinIO console port exposed on host.")
    root_user: str = Field(..., description="MinIO administrative username.")
    root_password: str = Field(..., description="MinIO administrative password.")
    access_key: str = Field(..., description="MinIO access key for S3A.")
    secret_key: str = Field(..., description="MinIO secret key for S3A.")
    bronze_bucket: str = Field(..., description="Bronze bucket name.")
    silver_bucket: str = Field(..., description="Silver bucket name.")
    gold_bucket: str = Field(..., description="Gold bucket name.")

    @property
    def s3a_endpoint(self) -> str:
        """Return the MinIO endpoint formatted for Spark S3A."""
        return self.endpoint

    @property
    def bronze_uri(self) -> str:
        """Return the S3A URI for the bronze bucket."""
        return f"s3a://{self.bronze_bucket}"

    @property
    def silver_uri(self) -> str:
        """Return the S3A URI for the silver bucket."""
        return f"s3a://{self.silver_bucket}"

    @property
    def gold_uri(self) -> str:
        """Return the S3A URI for the gold bucket."""
        return f"s3a://{self.gold_bucket}"


class KafkaSettings(BaseModel):
    """Kafka / Redpanda broker settings."""

    bootstrap_servers: str = Field(..., description="Kafka bootstrap servers.")
    redpanda_brokers: str = Field(..., description="Redpanda broker list.")
    clickstream_topic: str = Field(..., description="Clickstream topic name.")

    @property
    def bootstrap_servers_list(self) -> list[str]:
        """Return bootstrap servers as a cleaned list."""
        return [server.strip() for server in self.bootstrap_servers.split(",") if server.strip()]


class SparkRuntimeSettings(BaseModel):
    """Spark runtime tuning and cluster settings."""

    master_url: str = Field(..., description="Spark master URL.")
    driver_memory: str = Field(..., description="Spark driver memory allocation.")
    executor_memory: str = Field(..., description="Spark executor memory allocation.")
    sql_shuffle_partitions: int = Field(..., description="Spark SQL shuffle partition count.")
    worker_cores: int = Field(..., description="Spark worker cores.")
    worker_memory: str = Field(..., description="Spark worker memory allocation.")


class BatchSettings(BaseModel):
    """Application batch processing controls."""

    cdc_batch_size: int = Field(..., description="CDC polling batch size.")
    clickstream_batch_size: int = Field(..., description="Clickstream batch size.")
    orders_generator_sleep_seconds: int = Field(
        ..., description="Sleep interval between order generator batches."
    )
    clickstream_generator_sleep_seconds: int = Field(
        ..., description="Sleep interval between clickstream generator batches."
    )


class AirflowSettings(BaseModel):
    """Airflow settings used by the project."""

    executor: str = Field(..., description="Airflow executor type.")
    load_examples: bool = Field(..., description="Whether Airflow loads example DAGs.")
    dags_are_paused_at_creation: bool = Field(
        ..., description="Whether DAGs start paused when first created."
    )
    default_timezone: str = Field(..., description="Airflow default timezone.")
    fernet_key: str = Field(..., description="Airflow fernet key.")
    database_sqlalchemy_conn: str = Field(..., description="Airflow metadata database URL.")
    webserver_secret_key: str = Field(..., description="Airflow webserver secret key.")
    api_auth_backends: str = Field(..., description="Airflow API auth backends.")
    uid: int = Field(..., description="Airflow container user ID.")
    gid: int = Field(..., description="Airflow container group ID.")
    pythonpath: str = Field(..., description="Airflow PYTHONPATH.")
    admin_username: str = Field(..., description="Airflow admin username.")
    admin_password: str = Field(..., description="Airflow admin password.")
    admin_firstname: str = Field(..., description="Airflow admin first name.")
    admin_lastname: str = Field(..., description="Airflow admin last name.")
    admin_email: str = Field(..., description="Airflow admin email.")
    webserver_port: int = Field(..., description="Airflow host webserver port.")


class AppSettings(BaseSettings):
    """Top-level application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE_PATH),
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    log_level: str = Field(..., alias="LOG_LEVEL")
    app_env: str = Field(..., alias="APP_ENV")

    postgres_host: str = Field(..., alias="POSTGRES_HOST")
    postgres_port: int = Field(..., alias="POSTGRES_PORT")
    postgres_db: str = Field(..., alias="POSTGRES_DB")
    postgres_user: str = Field(..., alias="POSTGRES_USER")
    postgres_password: str = Field(..., alias="POSTGRES_PASSWORD")

    minio_endpoint: str = Field(..., alias="MINIO_ENDPOINT")
    minio_api_port: int = Field(..., alias="MINIO_API_PORT")
    minio_console_port: int = Field(..., alias="MINIO_CONSOLE_PORT")
    minio_root_user: str = Field(..., alias="MINIO_ROOT_USER")
    minio_root_password: str = Field(..., alias="MINIO_ROOT_PASSWORD")
    minio_access_key: str = Field(..., alias="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(..., alias="MINIO_SECRET_KEY")
    bronze_bucket: str = Field(..., alias="BRONZE_BUCKET")
    silver_bucket: str = Field(..., alias="SILVER_BUCKET")
    gold_bucket: str = Field(..., alias="GOLD_BUCKET")

    kafka_bootstrap_servers: str = Field(..., alias="KAFKA_BOOTSTRAP_SERVERS")
    redpanda_brokers: str = Field(..., alias="REDPANDA_BROKERS")
    redpanda_topic_clickstream: str = Field(..., alias="REDPANDA_TOPIC_CLICKSTREAM")

    spark_master_url: str = Field(..., alias="SPARK_MASTER_URL")
    spark_driver_memory: str = Field(..., alias="SPARK_DRIVER_MEMORY")
    spark_executor_memory: str = Field(..., alias="SPARK_EXECUTOR_MEMORY")
    spark_sql_shuffle_partitions: int = Field(..., alias="SPARK_SQL_SHUFFLE_PARTITIONS")
    spark_worker_cores: int = Field(..., alias="SPARK_WORKER_CORES")
    spark_worker_memory: str = Field(..., alias="SPARK_WORKER_MEMORY")

    cdc_batch_size: int = Field(..., alias="CDC_BATCH_SIZE")
    clickstream_batch_size: int = Field(..., alias="CLICKSTREAM_BATCH_SIZE")
    orders_generator_sleep_seconds: int = Field(..., alias="ORDERS_GENERATOR_SLEEP_SECONDS")
    clickstream_generator_sleep_seconds: int = Field(
        ..., alias="CLICKSTREAM_GENERATOR_SLEEP_SECONDS"
    )

    airflow_core_executor: str = Field(..., alias="AIRFLOW__CORE__EXECUTOR")
    airflow_core_load_examples: bool = Field(..., alias="AIRFLOW__CORE__LOAD_EXAMPLES")
    airflow_core_dags_are_paused_at_creation: bool = Field(
        ..., alias="AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION"
    )
    airflow_core_default_timezone: str = Field(..., alias="AIRFLOW__CORE__DEFAULT_TIMEZONE")
    airflow_core_fernet_key: str = Field(..., alias="AIRFLOW__CORE__FERNET_KEY")
    airflow_database_sqlalchemy_conn: str = Field(
        ..., alias="AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
    )
    airflow_webserver_secret_key: str = Field(..., alias="AIRFLOW__WEBSERVER__SECRET_KEY")
    airflow_api_auth_backends: str = Field(..., alias="AIRFLOW__API__AUTH_BACKENDS")
    airflow_uid: int = Field(..., alias="AIRFLOW_UID")
    airflow_gid: int = Field(..., alias="AIRFLOW_GID")
    airflow_pythonpath: str = Field(..., alias="AIRFLOW_PYTHONPATH")
    airflow_admin_username: str = Field(..., alias="AIRFLOW_ADMIN_USERNAME")
    airflow_admin_password: str = Field(..., alias="AIRFLOW_ADMIN_PASSWORD")
    airflow_admin_firstname: str = Field(..., alias="AIRFLOW_ADMIN_FIRSTNAME")
    airflow_admin_lastname: str = Field(..., alias="AIRFLOW_ADMIN_LASTNAME")
    airflow_admin_email: str = Field(..., alias="AIRFLOW_ADMIN_EMAIL")
    airflow_webserver_port: int = Field(..., alias="AIRFLOW_WEBSERVER_PORT")

    @property
    def postgres(self) -> PostgresSettings:
        """Return grouped PostgreSQL settings."""
        return PostgresSettings(
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
            user=self.postgres_user,
            password=self.postgres_password,
        )

    @property
    def minio(self) -> MinIOSettings:
        """Return grouped MinIO settings."""
        return MinIOSettings(
            endpoint=self.minio_endpoint,
            api_port=self.minio_api_port,
            console_port=self.minio_console_port,
            root_user=self.minio_root_user,
            root_password=self.minio_root_password,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            bronze_bucket=self.bronze_bucket,
            silver_bucket=self.silver_bucket,
            gold_bucket=self.gold_bucket,
        )

    @property
    def kafka(self) -> KafkaSettings:
        """Return grouped Kafka / Redpanda settings."""
        return KafkaSettings(
            bootstrap_servers=self.kafka_bootstrap_servers,
            redpanda_brokers=self.redpanda_brokers,
            clickstream_topic=self.redpanda_topic_clickstream,
        )

    @property
    def spark(self) -> SparkRuntimeSettings:
        """Return grouped Spark runtime settings."""
        return SparkRuntimeSettings(
            master_url=self.spark_master_url,
            driver_memory=self.spark_driver_memory,
            executor_memory=self.spark_executor_memory,
            sql_shuffle_partitions=self.spark_sql_shuffle_partitions,
            worker_cores=self.spark_worker_cores,
            worker_memory=self.spark_worker_memory,
        )

    @property
    def batch(self) -> BatchSettings:
        """Return grouped batch processing settings."""
        return BatchSettings(
            cdc_batch_size=self.cdc_batch_size,
            clickstream_batch_size=self.clickstream_batch_size,
            orders_generator_sleep_seconds=self.orders_generator_sleep_seconds,
            clickstream_generator_sleep_seconds=self.clickstream_generator_sleep_seconds,
        )

    @property
    def airflow(self) -> AirflowSettings:
        """Return grouped Airflow settings."""
        return AirflowSettings(
            executor=self.airflow_core_executor,
            load_examples=self.airflow_core_load_examples,
            dags_are_paused_at_creation=self.airflow_core_dags_are_paused_at_creation,
            default_timezone=self.airflow_core_default_timezone,
            fernet_key=self.airflow_core_fernet_key,
            database_sqlalchemy_conn=self.airflow_database_sqlalchemy_conn,
            webserver_secret_key=self.airflow_webserver_secret_key,
            api_auth_backends=self.airflow_api_auth_backends,
            uid=self.airflow_uid,
            gid=self.airflow_gid,
            pythonpath=self.airflow_pythonpath,
            admin_username=self.airflow_admin_username,
            admin_password=self.airflow_admin_password,
            admin_firstname=self.airflow_admin_firstname,
            admin_lastname=self.airflow_admin_lastname,
            admin_email=self.airflow_admin_email,
            webserver_port=self.airflow_webserver_port,
        )


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    """Return a cached singleton instance of application settings."""
    return AppSettings()