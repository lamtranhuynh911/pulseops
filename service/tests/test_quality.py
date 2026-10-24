"""Tests for the QA check; the expected outcomes come from mock-data/README.md."""

import asyncio
from collections import Counter
from datetime import datetime, timedelta, timezone

from conftest import LINE, MemoryStore, load_scenario, result
from pulseops.checks.quality import (
    FAILED, FAILED_UPSTREAM, IN_PROGRESS, MISSING_UPSTREAM, NON_CONFORMING, PASSED, QualityCheck,
    decide, product_status,
)

GRACE_S = 60
T0 = datetime(2026, 10, 15, 9, 0, tzinfo=timezone.utc)


def run_scenario(name: str, deliveries: int = 1) -> MemoryStore:
    """Process a scenario as it would arrive in real time, then let every grace period expire."""
    store = MemoryStore()
    check = QualityCheck(store, LINE, GRACE_S)
    messages = load_scenario(name)

    async def replay() -> None:
        for _ in range(deliveries):
            for message in messages:
                await check.on_result(message, message.timestamp)
        await check.expire_pending(messages[-1].timestamp + timedelta(seconds=GRACE_S + 1))

    asyncio.run(replay())
    return store


def test_decide():
    assert decide("PASS") is None
    assert decide("FAIL") == FAILED_UPSTREAM
    assert decide(None) == MISSING_UPSTREAM


def test_product_status_precedence():
    assert product_status({"machine-1": "PASS"}, LINE, has_violation=False, has_pending=False) == IN_PROGRESS
    assert product_status(dict.fromkeys(LINE, "PASS"), LINE, has_violation=False, has_pending=False) == PASSED
    assert product_status({"machine-1": "FAIL"}, LINE, has_violation=False, has_pending=False) == FAILED
    assert product_status(dict.fromkeys(LINE, "PASS"), LINE, has_violation=False, has_pending=True) == IN_PROGRESS
    assert product_status({"machine-1": "FAIL", "machine-2": "PASS"}, LINE, has_violation=True,
                          has_pending=False) == NON_CONFORMING


def test_ideal_scenario_has_no_violations():
    store = run_scenario("ideal")
    assert store.violations == {}
    assert Counter(store.statuses.values()) == {PASSED: 60}


def test_qa_violations_scenario_matches_mock_data_readme():
    store = run_scenario("qa-violations")
    found = {(v.product_id, v.machine_id, v.type) for v in store.violations.values()}
    assert found == {
        ("P-000108", "machine-2", MISSING_UPSTREAM), ("P-000131", "machine-2", MISSING_UPSTREAM),
        ("P-000152", "machine-2", MISSING_UPSTREAM),
        ("P-000115", "machine-3", MISSING_UPSTREAM), ("P-000144", "machine-3", MISSING_UPSTREAM),
        ("P-000112", "machine-2", FAILED_UPSTREAM), ("P-000127", "machine-2", FAILED_UPSTREAM),
        ("P-000158", "machine-2", FAILED_UPSTREAM),
        ("P-000120", "machine-3", FAILED_UPSTREAM), ("P-000137", "machine-3", FAILED_UPSTREAM),
        ("P-000149", "machine-3", FAILED_UPSTREAM),
    }
    assert not any(v.upstream_machine_down for v in store.violations.values())
    assert Counter(store.statuses.values()) == {PASSED: 48, FAILED: 4, NON_CONFORMING: 11}
    per_machine = Counter((machine, outcome) for latest in store.results.values()
                          for machine, (outcome, _) in latest.items())
    assert per_machine == {
        ("machine-1", "PASS"): 56, ("machine-1", "FAIL"): 4,
        ("machine-2", "PASS"): 55, ("machine-2", "FAIL"): 5,
        ("machine-3", "PASS"): 59, ("machine-3", "FAIL"): 1,
    }
    assert store.pending == {}


def test_redelivering_everything_changes_nothing():
    once, twice = run_scenario("qa-violations"), run_scenario("qa-violations", deliveries=2)
    assert twice.violations.keys() == once.violations.keys()
    assert twice.statuses == once.statuses


def test_out_of_order_upstream_within_grace_is_resolved_normally():
    store = MemoryStore()
    check = QualityCheck(store, LINE, GRACE_S)

    async def scenario() -> None:
        await check.on_result(result("machine-2", "P-1", "PASS", T0, "00000000-0000-4000-8000-000000000002"), T0)
        assert store.statuses["P-1"] == IN_PROGRESS  # waiting for machine 1
        late = T0 + timedelta(seconds=30)
        await check.on_result(result("machine-1", "P-1", "FAIL", T0 - timedelta(seconds=15),
                                     "00000000-0000-4000-8000-000000000001"), late)
        await check.expire_pending(T0 + timedelta(seconds=GRACE_S + 1))

    asyncio.run(scenario())
    assert [(v.machine_id, v.type) for v in store.violations.values()] == [("machine-2", FAILED_UPSTREAM)]
    assert store.pending == {}


def test_missing_upstream_waits_for_the_grace_period_and_flags_a_down_machine():
    store = MemoryStore()
    store.machine_statuses["machine-1"] = "DOWN"
    check = QualityCheck(store, LINE, GRACE_S)

    async def scenario() -> None:
        await check.on_result(result("machine-2", "P-1", "PASS", T0, "00000000-0000-4000-8000-000000000002"), T0)
        assert await check.expire_pending(T0 + timedelta(seconds=GRACE_S - 1)) == 0
        assert store.violations == {}
        assert await check.expire_pending(T0 + timedelta(seconds=GRACE_S)) == 1

    asyncio.run(scenario())
    (violation,) = store.violations.values()
    assert (violation.type, violation.upstream_machine_down) == (MISSING_UPSTREAM, True)
    assert store.statuses["P-1"] == NON_CONFORMING


def test_retest_at_previous_machine_wins():
    store = MemoryStore()
    check = QualityCheck(store, LINE, GRACE_S)

    async def scenario() -> None:
        await check.on_result(result("machine-1", "P-1", "FAIL", T0, "00000000-0000-4000-8000-000000000001"), T0)
        retest = T0 + timedelta(seconds=10)
        await check.on_result(result("machine-1", "P-1", "PASS", retest, "00000000-0000-4000-8000-000000000003"), retest)
        later = T0 + timedelta(seconds=25)
        await check.on_result(result("machine-2", "P-1", "PASS", later, "00000000-0000-4000-8000-000000000002"), later)

    asyncio.run(scenario())
    assert store.violations == {}


def test_upstream_down_at_arrival_stays_flagged_after_the_machine_is_back():
    # SIM_OFFLINE_MACHINES demo: machine-1's data is lost; it comes back before the grace period ends.
    store = MemoryStore()
    store.machine_statuses["machine-1"] = "DOWN"
    check = QualityCheck(store, LINE, GRACE_S)

    async def scenario() -> None:
        await check.on_result(result("machine-2", "P-1", "PASS", T0, "00000000-0000-4000-8000-000000000002"), T0)
        store.machine_statuses["machine-1"] = "UP"
        await check.expire_pending(T0 + timedelta(seconds=GRACE_S))

    asyncio.run(scenario())
    (violation,) = store.violations.values()
    assert (violation.type, violation.upstream_machine_down) == (MISSING_UPSTREAM, True)


def test_a_verdict_lost_with_its_replica_is_judged_again_after_the_lease():
    import pytest
    from pymongo.errors import AutoReconnect

    from pulseops.storage import PENDING_LEASE

    store = MemoryStore()
    check = QualityCheck(store, LINE, GRACE_S)
    due = T0 + timedelta(seconds=GRACE_S)

    async def scenario() -> None:
        await check.on_result(result("machine-2", "P-1", "PASS", T0, "00000000-0000-4000-8000-000000000002"), T0)
        store.fail_next_insert_violation = True   # e.g. the replica loses MongoDB, or dies, mid-verdict
        with pytest.raises(AutoReconnect):
            await check.expire_pending(due)
        assert store.violations == {} and len(store.pending) == 1   # not lost: still pending, leased
        assert await check.expire_pending(due + timedelta(seconds=1)) == 0   # the lease protects it
        assert await check.expire_pending(due + PENDING_LEASE) == 1           # then another replica takes it

    asyncio.run(scenario())
    assert [v.type for v in store.violations.values()] == [MISSING_UPSTREAM]
    assert store.pending == {}


def test_one_verdict_per_hop_even_if_the_upstream_state_changes_before_a_redelivery():
    store = MemoryStore()
    check = QualityCheck(store, LINE, GRACE_S)
    machine_2 = result("machine-2", "P-1", "PASS", T0, "00000000-0000-4000-8000-000000000002")

    async def scenario() -> None:
        await check.on_result(machine_2, T0)
        await check.expire_pending(T0 + timedelta(seconds=GRACE_S))              # MISSING_UPSTREAM
        late = T0 + timedelta(seconds=GRACE_S + 5)                               # after the grace period
        await check.on_result(result("machine-1", "P-1", "FAIL", T0 - timedelta(seconds=15),
                                     "00000000-0000-4000-8000-000000000001"), late)
        await check.on_result(machine_2, late)                                   # redelivered: would be FAILED_UPSTREAM

    asyncio.run(scenario())
    assert [v.type for v in store.violations.values()] == [MISSING_UPSTREAM]


def test_outage_entirely_inside_the_grace_period_is_flagged():
    # UP when the result arrives, DOWN for a while, UP again before the grace period ends.
    store = MemoryStore()
    store.machine_statuses["machine-1"] = "UP"
    check = QualityCheck(store, LINE, GRACE_S)

    async def scenario() -> None:
        await check.on_result(result("machine-2", "P-1", "PASS", T0, "00000000-0000-4000-8000-000000000002"), T0)
        store.machine_statuses["machine-1"] = "UP"                      # back, after being DOWN
        store.machine_changed_at["machine-1"] = T0 + timedelta(seconds=40)
        await check.expire_pending(T0 + timedelta(seconds=GRACE_S))

    asyncio.run(scenario())
    (violation,) = store.violations.values()
    assert violation.upstream_machine_down is True
