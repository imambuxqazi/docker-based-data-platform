"""Unit tests for data generators."""

from __future__ import annotations

from datetime import datetime, timezone

from data_generators.clickstream import ClickstreamEvent, ClickstreamGenerator, ClickstreamGeneratorConfig
from data_generators.orders import OrdersGeneratorConfig


def test_clickstream_event_to_kafka_payload_serializes_timestamps() -> None:
    """Ensure clickstream payload serializes timestamps as ISO strings."""
    event = ClickstreamEvent(
        event_id="event-1",
        session_id="session-1",
        user_id="user-1",
        event_type="page_view",
        event_timestamp=datetime(2026, 3, 8, 10, 0, tzinfo=timezone.utc),
        ingestion_timestamp=datetime(2026, 3, 8, 10, 1, tzinfo=timezone.utc),
        page_url="/home",
        product_id=None,
        category_name=None,
        country_code="PK",
        device_type="mobile",
        traffic_source="organic",
        event_position=1,
        is_late_event=False,
        original_event_id=None,
    )

    payload = event.to_kafka_payload()

    assert payload["event_timestamp"] == "2026-03-08T10:00:00+00:00"
    assert payload["ingestion_timestamp"] == "2026-03-08T10:01:00+00:00"


def test_clickstream_generator_builds_session_events_with_expected_structure() -> None:
    """Ensure generated session events follow the expected event schema."""
    generator = ClickstreamGenerator(config=ClickstreamGeneratorConfig())

    events = generator._build_session_events()

    assert len(events) >= 1
    assert events[0].event_type == "page_view"
    assert all(event.session_id == events[0].session_id for event in events)
    assert all(event.user_id == events[0].user_id for event in events)
    assert all(event.event_position >= 1 for event in events)


def test_duplicate_injection_rate_is_within_expected_range() -> None:
    """Ensure duplicate injection stays near the configured rate."""
    config = ClickstreamGeneratorConfig(duplicate_rate=0.03, late_event_rate=0.0)
    generator = ClickstreamGenerator(config=config)

    base_events = []
    for index in range(1000):
        base_events.append(
            ClickstreamEvent(
                event_id=f"event-{index}",
                session_id=f"session-{index}",
                user_id=f"user-{index}",
                event_type="page_view",
                event_timestamp=datetime(2026, 3, 8, 10, 0, tzinfo=timezone.utc),
                ingestion_timestamp=datetime(2026, 3, 8, 10, 1, tzinfo=timezone.utc),
                page_url="/home",
                product_id=None,
                category_name=None,
                country_code="PK",
                device_type="mobile",
                traffic_source="organic",
                event_position=1,
                is_late_event=False,
                original_event_id=None,
            )
        )

    output_events = generator._inject_duplicates(base_events)
    duplicate_count = sum(1 for event in output_events if event.original_event_id is not None)
    duplicate_rate = duplicate_count / len(base_events)

    assert 0.0 <= duplicate_rate <= 0.08


def test_late_events_have_past_event_timestamps() -> None:
    """Ensure late events move event_timestamp into the past relative to ingestion."""
    config = ClickstreamGeneratorConfig(duplicate_rate=0.0, late_event_rate=1.0, late_event_max_minutes=20)
    generator = ClickstreamGenerator(config=config)

    base_event = ClickstreamEvent(
        event_id="event-1",
        session_id="session-1",
        user_id="user-1",
        event_type="page_view",
        event_timestamp=datetime(2026, 3, 8, 10, 30, tzinfo=timezone.utc),
        ingestion_timestamp=datetime(2026, 3, 8, 10, 31, tzinfo=timezone.utc),
        page_url="/home",
        product_id=None,
        category_name=None,
        country_code="PK",
        device_type="mobile",
        traffic_source="organic",
        event_position=1,
        is_late_event=False,
        original_event_id=None,
    )

    output_events = generator._inject_late_arrivals([base_event])

    assert len(output_events) == 1
    assert output_events[0].is_late_event is True
    assert output_events[0].event_timestamp < base_event.event_timestamp


def test_orders_generator_config_defaults_are_sane() -> None:
    """Ensure orders generator config defaults are valid for batch generation."""
    config = OrdersGeneratorConfig()

    assert config.initial_customer_count > 0
    assert config.initial_product_count > 0
    assert config.new_orders_per_batch_min <= config.new_orders_per_batch_max
    assert config.items_per_order_min <= config.items_per_order_max
    assert 0 <= config.status_update_fraction <= 1