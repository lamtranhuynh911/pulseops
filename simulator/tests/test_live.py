"""Tests for the live simulation's journey rules."""

import random

from simulator.live import Faults, plan_journey


def journey(**faults):
    return plan_journey(Faults(**faults), random.Random(1))


def test_no_faults_passes_every_machine():
    assert journey() == ("PASS", "PASS", "PASS")


def test_failed_product_leaves_the_line():
    assert journey(fail_rate=1.0) == ("FAIL", None, None)


def test_leaked_product_keeps_moving_after_a_fail():
    assert journey(fail_rate=1.0, leak_rate=1.0) == ("FAIL", "FAIL", "FAIL")


def test_skip_bypasses_machines_1_and_2_only():
    assert journey(skip_rate=1.0) == (None, None, "PASS")


def test_rates_are_roughly_respected():
    rng = random.Random(7)
    journeys = [plan_journey(Faults(fail_rate=0.2), rng) for _ in range(5000)]
    first_machine_fails = sum(j[0] == "FAIL" for j in journeys) / len(journeys)
    assert 0.17 < first_machine_fails < 0.23
