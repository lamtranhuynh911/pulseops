"""Tests for the spec zones of test measurements."""

from pulseops.specs import MARGINAL, OK, OUT, zone


def test_two_sided_spec_zones():
    assert zone("machine-1", "length_mm", 120.000) == OK
    assert zone("machine-1", "length_mm", 120.119) == OK
    assert zone("machine-1", "length_mm", 120.130) == MARGINAL
    assert zone("machine-1", "length_mm", 119.870) == MARGINAL
    assert zone("machine-1", "length_mm", 120.150) == MARGINAL    # the limit itself still passes
    assert zone("machine-1", "length_mm", 120.151) == OUT
    assert zone("machine-3", "output_voltage_v", 4.85) == OUT
    assert zone("machine-3", "output_voltage_v", 4.91) == MARGINAL


def test_one_sided_spec_zones():
    assert zone("machine-2", "leak_rate_ml_min", 0.12) == OK
    assert zone("machine-2", "leak_rate_ml_min", 0.45) == MARGINAL
    assert zone("machine-2", "leak_rate_ml_min", 0.70) == OUT


def test_measurements_without_a_spec_have_no_zone():
    assert zone("machine-1", "temperature_c", 34.0) is None
    assert zone("machine-2", "length_mm", 120.0) is None   # spec belongs to machine-1 only


def test_mock_data_failures_are_out_of_spec_and_passes_are_not():
    from conftest import load_scenario

    for message in load_scenario("qa-violations") + load_scenario("ideal"):
        test_value = {k: v for k, v in message.measurements.items() if k not in ("cycle_time_s", "temperature_c")}
        (name, value), = test_value.items()
        assert (zone(message.machine_id, name, value) == OUT) == (message.result == "FAIL"), (message, value)
