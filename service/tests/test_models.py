"""Tests for message validation and the rejection reasons."""

import json

import pytest

from pulseops.models import Heartbeat, Rejected, TestResult, parse

LINE = ("machine-1", "machine-2", "machine-3")
VALID = {
    "schema_version": 1, "event_id": "4f9c2a7e-8b1d-4c3e-9a6f-2d5b7e1c0a93", "machine_id": "machine-2",
    "product_id": "P-000123", "result": "PASS", "timestamp": "2026-10-15T08:30:12.345Z",
    "measurements": {"cycle_time_s": 8.6, "temperature_c": 36.2, "leak_rate_ml_min": 0.14},
}


def reason(model, raw: bytes) -> str:
    with pytest.raises(Rejected) as e:
        parse(model, raw, LINE)
    return e.value.reason


def test_readme_examples_are_valid():
    assert parse(TestResult, json.dumps(VALID).encode(), LINE).product_id == "P-000123"
    heartbeat = {"schema_version": 1, "machine_id": "machine-2", "timestamp": "2026-10-15T08:30:10.000Z"}
    assert parse(Heartbeat, json.dumps(heartbeat).encode(), LINE).machine_id == "machine-2"


def test_rejection_reasons():
    assert reason(TestResult, b"{not json") == "invalid_json"
    assert reason(TestResult, json.dumps({**VALID, "schema_version": 2}).encode()) == "unsupported_version"
    assert reason(TestResult, json.dumps({**VALID, "result": "MAYBE"}).encode()) == "invalid_schema"
    assert reason(TestResult, json.dumps({**VALID, "timestamp": "2026-10-15T08:30:12"}).encode()) == "invalid_schema"
    assert reason(TestResult, json.dumps({**VALID, "machine_id": "machine-9"}).encode()) == "unknown_machine"
    assert reason(Heartbeat, None) == "invalid_json"
