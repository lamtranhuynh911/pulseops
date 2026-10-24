"""Tests for loading a mock-data scenario for replay."""

from pathlib import Path

from simulator.replay import load_scenario

MOCK_DATA = Path(__file__).resolve().parents[2] / "mock-data"


def test_scenario_is_merged_in_timestamp_order():
    results = load_scenario(MOCK_DATA / "qa-violations")
    assert len(results) == 180  # 60 rows per machine
    timestamps = [message["timestamp"] for message in results]
    assert timestamps == sorted(timestamps)
    assert len({message["event_id"] for message in results}) == 180


def test_a_product_reaches_machines_in_line_order():
    results = load_scenario(MOCK_DATA / "ideal")
    machines = [message["machine_id"] for message in results if message["product_id"] == "P-000001"]
    assert machines == ["machine-1", "machine-2", "machine-3"]
