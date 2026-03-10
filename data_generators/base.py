"""Base generator abstractions for long-running data simulation jobs."""

from __future__ import annotations

import abc
import signal
import time
from datetime import datetime, timezone
from typing import Any

import structlog
from pydantic import BaseModel, Field

from config.settings import AppSettings, get_settings


LOGGER = structlog.get_logger(__name__)


class GeneratorStats(BaseModel):
    """Track operational statistics for a running generator."""

    rows_generated: int = Field(default=0, description="Total rows or events generated.")
    batches_run: int = Field(default=0, description="Total batches processed.")
    errors: int = Field(default=0, description="Total errors encountered.")
    started_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC start time for the generator process.",
    )

    @property
    def uptime_seconds(self) -> float:
        """Return generator uptime in seconds."""
        return (datetime.now(timezone.utc) - self.started_at).total_seconds()


class BaseGenerator(abc.ABC):
    """Abstract base class for long-running batch-oriented generators.

    This class centralizes signal handling, lifecycle management, statistics,
    and periodic metrics logging. Concrete generators must implement setup(),
    generate_batch(), and cleanup().
    """

    def __init__(self, generator_name: str, settings: AppSettings | None = None) -> None:
        """Initialize the base generator.

        Args:
            generator_name: Logical generator name for logging.
            settings: Optional application settings instance.
        """
        self.settings = settings or get_settings()
        self.generator_name = generator_name
        self.running = True
        self.stats = GeneratorStats()
        self.logger = LOGGER.bind(component="generator", generator_name=generator_name)
        self._last_stats_log_time = time.monotonic()
        self._register_signal_handlers()

    def _register_signal_handlers(self) -> None:
        """Register SIGINT and SIGTERM handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self._handle_stop_signal)
        signal.signal(signal.SIGTERM, self._handle_stop_signal)

    def _handle_stop_signal(self, signum: int, frame: Any) -> None:
        """Handle process stop signals.

        Args:
            signum: Signal number.
            frame: Current execution frame.
        """
        del frame
        self.logger.info("stop_signal_received", signum=signum)
        self.running = False

    def increment_success(self, rows_generated: int) -> None:
        """Update statistics after a successful batch.

        Args:
            rows_generated: Number of rows/events generated in the batch.
        """
        self.stats.rows_generated += rows_generated
        self.stats.batches_run += 1

    def increment_error(self) -> None:
        """Update statistics after a failed batch."""
        self.stats.errors += 1

    def maybe_log_stats(self) -> None:
        """Log generator statistics every 30 seconds."""
        current_time = time.monotonic()
        if current_time - self._last_stats_log_time >= 30:
            self.logger.info(
                "generator_stats",
                rows_generated=self.stats.rows_generated,
                batches_run=self.stats.batches_run,
                errors=self.stats.errors,
                uptime_seconds=round(self.stats.uptime_seconds, 2),
            )
            self._last_stats_log_time = current_time

    @abc.abstractmethod
    def setup(self) -> None:
        """Initialize required external resources."""

    @abc.abstractmethod
    def generate_batch(self) -> int:
        """Generate one batch of data.

        Returns:
            Number of rows/events generated in the batch.
        """

    @abc.abstractmethod
    def cleanup(self) -> None:
        """Release external resources gracefully."""

    def run(self) -> None:
        """Run the generator loop until a stop signal is received."""
        self.logger.info("generator_starting")
        self.setup()

        try:
            while self.running:
                rows_generated = self.generate_batch()
                self.increment_success(rows_generated)
                self.maybe_log_stats()
        except Exception:
            self.increment_error()
            self.logger.exception("generator_run_failed")
            raise
        finally:
            self.cleanup()
            self.logger.info(
                "generator_stopped",
                rows_generated=self.stats.rows_generated,
                batches_run=self.stats.batches_run,
                errors=self.stats.errors,
                uptime_seconds=round(self.stats.uptime_seconds, 2),
            )