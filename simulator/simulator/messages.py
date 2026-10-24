"""Kafka messages in the format described in the README's "Messages" section."""

import uuid
from datetime import datetime, timezone

TEST_RESULTS_TOPIC = "machine.test-results"
HEARTBEATS_TOPIC = "machine.heartbeats"
SCHEMA_VERSION = 1

# Columns of a mock-data CSV row that are not measurements.
CSV_FIELDS = ("event_id", "machine_id", "product_id", "result", "timestamp")


def iso_utc(moment: datetime) -> str:
    """ISO 8601 in UTC with milliseconds and a Z suffix, e.g. 2026-10-15T08:30:12.345Z."""
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def test_result(
    machine_id: str,
    product_id: str,
    result: str,
    measurements: dict[str, float],
    timestamp: datetime,
    event_id: str | None = None,
) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id or str(uuid.uuid4()),
        "machine_id": machine_id,
        "product_id": product_id,
        "result": result,
        "timestamp": iso_utc(timestamp),
        "measurements": measurements,
    }


def heartbeat(machine_id: str, timestamp: datetime) -> dict:
    return {"schema_version": SCHEMA_VERSION, "machine_id": machine_id, "timestamp": iso_utc(timestamp)}


def from_csv_row(row: dict[str, str]) -> dict:
    """Turn a mock-data CSV row into a test-result message, keeping its event_id and timestamp."""
    return {
        "schema_version": SCHEMA_VERSION,
        **{field: row[field] for field in CSV_FIELDS},
        "measurements": {name: float(value) for name, value in row.items() if name not in CSV_FIELDS},
    }
