"""Tests for the product line time computed from a product document."""

from datetime import datetime, timedelta, timezone

from pulseops.storage import line_seconds

T0 = datetime(2026, 10, 15, 8, 0, tzinfo=timezone.utc)


def test_line_seconds_is_first_to_last_result():
    doc = {"results": {
        "machine-1": {"result": "PASS", "timestamp": T0},
        "machine-3": {"result": "PASS", "timestamp": T0 + timedelta(seconds=30)},
        "machine-2": {"result": "PASS", "timestamp": T0 + timedelta(seconds=15)},
    }}
    assert line_seconds(doc) == 30.0


def test_line_seconds_needs_two_results():
    assert line_seconds({"results": {"machine-1": {"result": "PASS", "timestamp": T0}}}) is None
    assert line_seconds(None) is None
