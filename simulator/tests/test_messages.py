"""Tests for the message format."""

import csv
from datetime import datetime, timezone
from pathlib import Path

from simulator import messages

MOCK_DATA = Path(__file__).resolve().parents[2] / "mock-data"


def test_iso_utc_has_milliseconds_and_z():
    moment = datetime(2026, 10, 15, 8, 30, 12, 345678, tzinfo=timezone.utc)
    assert messages.iso_utc(moment) == "2026-10-15T08:30:12.345Z"


def test_csv_row_becomes_test_result_message():
    with (MOCK_DATA / "ideal" / "machine-2.csv").open(newline="") as f:
        row = next(csv.DictReader(f))
    message = messages.from_csv_row(row)
    assert message["schema_version"] == 1
    assert message["event_id"] == row["event_id"]
    assert message["timestamp"] == row["timestamp"]
    assert set(message["measurements"]) == {"cycle_time_s", "temperature_c", "leak_rate_ml_min"}
    assert all(isinstance(value, float) for value in message["measurements"].values())


def test_generated_test_result_matches_readme_fields():
    message = messages.test_result("machine-1", "P-1", "PASS", {"length_mm": 120.0}, datetime.now(timezone.utc))
    assert set(message) == {"schema_version", "event_id", "machine_id", "product_id", "result", "timestamp", "measurements"}
    assert message["timestamp"].endswith("Z")
