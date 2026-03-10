"""Redpanda clickstream event generator.

This generator emits realistic user session events across a funnel:
page_view -> product_click -> add_to_cart -> checkout -> purchase

It also injects:
- duplicate events (~3%)
- late-arriving events (~2%)
"""

from __future__ import annotations

import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from confluent_kafka import Producer
from faker import Faker
from pydantic import BaseModel, Field
import structlog
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from config.settings import AppSettings, get_settings
from data_generators.base import BaseGenerator


LOGGER = structlog.get_logger(__name__)

FUNNEL_STAGES: list[str] = [
    "page_view",
    "product_click",
    "add_to_cart",
    "checkout",
    "purchase",
]


class ClickstreamEvent(BaseModel):
    """Schema for an emitted clickstream event."""

    event_id: str
    session_id: str
    user_id: str
    event_type: str
    event_timestamp: datetime
    ingestion_timestamp: datetime
    page_url: str
    product_id: int | None = None
    category_name: str | None = None
    country_code: str
    device_type: str
    traffic_source: str
    event_position: int
    is_late_event: bool = False
    original_event_id: str | None = None

    def to_kafka_payload(self) -> dict[str, Any]:
        """Return a JSON-serializable payload."""
        payload = self.model_dump()
        payload["event_timestamp"] = self.event_timestamp.isoformat()
        payload["ingestion_timestamp"] = self.ingestion_timestamp.isoformat()
        return payload


class ClickstreamGeneratorConfig(BaseModel):
    """Runtime configuration for the clickstream generator."""

    sessions_per_batch_min: int = Field(default=60)
    sessions_per_batch_max: int = Field(default=140)
    duplicate_rate: float = Field(default=0.03)
    late_event_rate: float = Field(default=0.02)
    late_event_max_minutes: int = Field(default=20)
    delivery_flush_interval: int = Field(default=500)
    product_id_min: int = Field(default=1)
    product_id_max: int = Field(default=100)


class ClickstreamGenerator(BaseGenerator):
    """Generate clickstream events into Kafka / Redpanda."""

    def __init__(
        self,
        settings: AppSettings | None = None,
        config: ClickstreamGeneratorConfig | None = None,
    ) -> None:
        """Initialize the clickstream generator.

        Args:
            settings: Optional application settings.
            config: Optional generator configuration.
        """
        super().__init__(generator_name="clickstream_generator", settings=settings)
        self.config = config or ClickstreamGeneratorConfig()
        self.fake = Faker()
        self.random = random.Random(99)
        self.producer: Producer | None = None
        self.logger = LOGGER.bind(component="clickstream_generator")
        self._messages_since_flush = 0

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
    )
    def setup(self) -> None:
        """Initialize the Kafka producer."""
        self.producer = Producer(
            {
                "bootstrap.servers": self.settings.kafka.bootstrap_servers,
                "client.id": "clickstream-generator",
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "snappy",
                "linger.ms": 100,
                "batch.num.messages": self.settings.batch.clickstream_batch_size,
            }
        )
        self.logger.info("kafka_producer_created", topic=self.settings.kafka.clickstream_topic)

    def _delivery_callback(self, error: Any, message: Any) -> None:
        """Handle Kafka delivery callbacks.

        Args:
            error: Delivery error if present.
            message: Kafka message metadata.
        """
        if error is not None:
            self.logger.error("event_delivery_failed", error=str(error))
        else:
            self.logger.debug(
                "event_delivered",
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
            )

    def _should_progress_stage(self, stage_name: str) -> bool:
        """Determine whether a session continues to the next stage.

        Args:
            stage_name: Current funnel stage.

        Returns:
            True if the session advances.
        """
        progression_probabilities = {
            "page_view": 0.72,
            "product_click": 0.58,
            "add_to_cart": 0.64,
            "checkout": 0.77,
        }
        return self.random.random() <= progression_probabilities.get(stage_name, 1.0)

    def _build_session_events(self) -> list[ClickstreamEvent]:
        """Build a funnel-shaped set of events for one session.

        Returns:
            Ordered session events.
        """
        now_utc = datetime.now(timezone.utc)
        session_id = str(uuid.uuid4())
        user_id = f"user-{self.random.randint(1, 5000):05d}"
        country_code = self.fake.country_code(representation="alpha-2")
        device_type = self.random.choice(["desktop", "mobile", "tablet"])
        traffic_source = self.random.choice(["organic", "paid_search", "email", "social", "direct"])
        product_id = self.random.randint(
            self.config.product_id_min,
            self.config.product_id_max,
        )
        category_name = self.random.choice(
            ["Electronics", "Accessories", "Home", "Sports", "Books"]
        )

        base_event_time = now_utc - timedelta(seconds=self.random.randint(0, 90))
        events: list[ClickstreamEvent] = []

        for event_position, stage_name in enumerate(FUNNEL_STAGES, start=1):
            if stage_name != "page_view":
                previous_stage = FUNNEL_STAGES[event_position - 2]
                if not self._should_progress_stage(previous_stage):
                    break

            page_url = (
                "/home"
                if stage_name == "page_view"
                else f"/product/{product_id}"
                if stage_name in {"product_click", "add_to_cart"}
                else "/checkout"
                if stage_name == "checkout"
                else "/confirmation"
            )

            event_timestamp = base_event_time + timedelta(
                seconds=event_position * self.random.randint(3, 12)
            )

            event = ClickstreamEvent(
                event_id=str(uuid.uuid4()),
                session_id=session_id,
                user_id=user_id,
                event_type=stage_name,
                event_timestamp=event_timestamp,
                ingestion_timestamp=now_utc,
                page_url=page_url,
                product_id=product_id if stage_name != "page_view" else None,
                category_name=category_name if stage_name != "page_view" else None,
                country_code=country_code,
                device_type=device_type,
                traffic_source=traffic_source,
                event_position=event_position,
                is_late_event=False,
                original_event_id=None,
            )
            events.append(event)

        return events

    def _inject_duplicates(self, events: list[ClickstreamEvent]) -> list[ClickstreamEvent]:
        """Inject duplicate events at the configured duplicate rate.

        Args:
            events: Base event list.

        Returns:
            Event list including duplicates.
        """
        output_events = list(events)
        duplicate_candidates = [
            event for event in events if self.random.random() < self.config.duplicate_rate
        ]

        for event in duplicate_candidates:
            duplicate_payload = event.model_dump()
            duplicate_payload["ingestion_timestamp"] = datetime.now(timezone.utc)
            duplicate_payload["original_event_id"] = event.event_id
            duplicate_event = ClickstreamEvent(**duplicate_payload)
            output_events.append(duplicate_event)

        return output_events

    def _inject_late_arrivals(self, events: list[ClickstreamEvent]) -> list[ClickstreamEvent]:
        """Mark a subset of events as late-arriving.

        Args:
            events: Event list.

        Returns:
            Event list including late-arriving timestamps.
        """
        output_events: list[ClickstreamEvent] = []

        for event in events:
            if self.random.random() < self.config.late_event_rate:
                late_payload = event.model_dump()
                late_payload["event_timestamp"] = event.event_timestamp - timedelta(
                    minutes=self.random.randint(11, self.config.late_event_max_minutes)
                )
                late_payload["ingestion_timestamp"] = datetime.now(timezone.utc)
                late_payload["is_late_event"] = True
                late_event = ClickstreamEvent(**late_payload)
                output_events.append(late_event)
            else:
                output_events.append(event)

        return output_events

    def _build_batch_events(self) -> list[ClickstreamEvent]:
        """Build one clickstream batch across multiple sessions.

        Returns:
            Full event list for publication.
        """
        session_count = self.random.randint(
            self.config.sessions_per_batch_min,
            self.config.sessions_per_batch_max,
        )
        events: list[ClickstreamEvent] = []

        for _ in range(session_count):
            events.extend(self._build_session_events())

        events = self._inject_duplicates(events)
        events = self._inject_late_arrivals(events)
        return sorted(events, key=lambda event: event.ingestion_timestamp)

    def _publish_events(self, events: list[ClickstreamEvent]) -> int:
        """Publish a batch of events to Kafka.

        Args:
            events: Events to publish.

        Returns:
            Number of published events.
        """
        if self.producer is None:
            raise RuntimeError("Kafka producer is not initialized.")

        topic_name = self.settings.kafka.clickstream_topic

        for event in events:
            self.producer.produce(
                topic=topic_name,
                key=event.session_id,
                value=json.dumps(event.to_kafka_payload()),
                on_delivery=self._delivery_callback,
            )
            self._messages_since_flush += 1

            if self._messages_since_flush >= self.config.delivery_flush_interval:
                self.producer.flush()
                self._messages_since_flush = 0

        self.producer.flush()
        self._messages_since_flush = 0

        duplicate_count = sum(1 for event in events if event.original_event_id is not None)
        late_count = sum(1 for event in events if event.is_late_event)

        self.logger.info(
            "clickstream_batch_published",
            topic=topic_name,
            event_count=len(events),
            duplicate_count=duplicate_count,
            late_event_count=late_count,
        )
        return len(events)

    def generate_batch(self) -> int:
        """Generate and publish one clickstream batch.

        Returns:
            Number of events published.
        """
        events = self._build_batch_events()
        published_count = self._publish_events(events)
        time.sleep(self.settings.batch.clickstream_generator_sleep_seconds)
        return published_count

    def cleanup(self) -> None:
        """Flush producer buffers on shutdown."""
        if self.producer is not None:
            self.producer.flush()
            self.logger.info("kafka_producer_flushed")


if __name__ == "__main__":
    ClickstreamGenerator(settings=get_settings()).run()