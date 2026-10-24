"""Tests for the MongoDB queries, against a real MongoDB.

Skipped unless MONGODB_TEST_URI is set, e.g.:
    docker run -d --rm --name pulseops-test-mongo -p 27017:27017 mongo:7.0.45
    MONGODB_TEST_URI=mongodb://localhost:27017 uv run pytest tests/test_storage.py
"""

import asyncio
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from conftest import LINE, load_scenario, result
from pulseops.checks.quality import NON_CONFORMING, PASSED, QualityCheck
from pulseops.storage import Storage

URI = os.environ.get("MONGODB_TEST_URI")
pytestmark = pytest.mark.skipif(not URI, reason="MONGODB_TEST_URI not set")
T0 = datetime(2026, 10, 15, 8, 0, tzinfo=timezone.utc)


def with_storage(test):
    """Run an async test body against a fresh, throw-away database."""
    def run():
        async def body():
            storage = Storage(URI, f"pulseops_test_{uuid.uuid4().hex[:8]}", LINE)
            try:
                await storage.prepare(T0)
                await test(storage)
            finally:
                await storage.client.drop_database(storage.db.name)
                await storage.close()
        asyncio.run(body())
    run.__name__ = test.__name__
    return run


@with_storage
async def test_last_seen_never_moves_backwards(storage):
    await storage.touch_machine("machine-1", T0 + timedelta(seconds=10))
    await storage.touch_machine("machine-1", T0 + timedelta(seconds=5))  # late or redelivered message
    last_seen, _ = await storage.machine_times()
    assert last_seen["machine-1"] == T0 + timedelta(seconds=10)


@with_storage
async def test_status_change_is_recorded_once_and_needs_evidence(storage):
    timeout = timedelta(seconds=15)
    await storage.touch_machine("machine-1", T0)
    later = T0 + timedelta(seconds=20)
    assert await storage.set_machine_status("machine-1", "DOWN", later, later - timeout)
    assert not await storage.set_machine_status("machine-1", "DOWN", later, later - timeout)  # the other replica
    # A stale UP verdict (read before the DOWN, written after it) can't undo it: no message since.
    stale = T0 + timedelta(seconds=10)
    assert not await storage.set_machine_status("machine-1", "UP", stale, stale - timeout)
    # Data again: UP is recorded.
    back = T0 + timedelta(seconds=25)
    await storage.touch_machine("machine-1", back)
    assert await storage.set_machine_status("machine-1", "UP", back, back - timeout)
    # A stale DOWN verdict computed before that message can't undo the UP either.
    assert not await storage.set_machine_status("machine-1", "DOWN", later, later - timeout)


@with_storage
async def test_expired_pending_check_is_leased_until_its_verdict_is_stored(storage):
    from pulseops.checks.quality import PendingCheck
    from pulseops.storage import PENDING_LEASE

    check = PendingCheck("00000000-0000-4000-8000-000000000002", "P-1", "machine-2", "machine-1", T0)
    await storage.add_pending(check)
    claimed = await storage.claim_expired(T0)
    assert claimed == check
    assert await storage.claim_expired(T0 + timedelta(seconds=1)) is None  # leased by the first claim
    assert await storage.claim_expired(T0 + PENDING_LEASE) == check         # the claimer died: lease ran out
    await storage.finish_pending(check)
    assert await storage.db.pending_qa_checks.count_documents({}) == 0


@with_storage
async def test_one_verdict_per_downstream_result_and_hop(storage):
    from pulseops.checks.quality import Violation

    def verdict(kind):
        return Violation(f"E2:{kind}", kind, "P-1", "machine-2", "machine-1", "E2", False, T0)

    assert await storage.insert_violation(verdict("MISSING_UPSTREAM"))
    assert not await storage.insert_violation(verdict("FAILED_UPSTREAM"))   # same hop: the first verdict stays
    assert not await storage.insert_violation(verdict("MISSING_UPSTREAM"))  # redelivery
    assert await storage.db.qa_violations.count_documents({}) == 1


@with_storage
async def test_rejected_message_at_a_reused_offset_is_stored_again(storage):
    args = ("machine.test-results", 1, 42)
    assert await storage.insert_rejected(*args, 1000, "invalid_json", "x", b"{", T0)
    assert not await storage.insert_rejected(*args, 1000, "invalid_json", "x", b"{", T0)  # redelivery
    assert await storage.insert_rejected(*args, 2000, "invalid_json", "x", b"[", T0)      # topic recreated


@with_storage
async def test_duplicate_event_id_is_stored_once(storage):
    message = result("machine-1", "P-1", "PASS", T0, "00000000-0000-4000-8000-000000000001")
    assert await storage.insert_test_result(message, T0)
    assert not await storage.insert_test_result(message, T0)
    assert await storage.db.test_results.count_documents({}) == 1


@with_storage
async def test_older_result_does_not_replace_a_newer_retest(storage):
    newer = result("machine-1", "P-1", "PASS", T0 + timedelta(seconds=10), "00000000-0000-4000-8000-000000000002")
    older = result("machine-1", "P-1", "FAIL", T0, "00000000-0000-4000-8000-000000000001")
    assert await storage.record_result(newer) == {"machine-1": "PASS"}
    assert await storage.record_result(older) == {"machine-1": "PASS"}


@with_storage
async def test_qa_violations_scenario_end_to_end(storage):
    check = QualityCheck(storage, LINE, grace_s=60)
    messages = load_scenario("qa-violations")
    for _ in range(2):  # the second pass is a full redelivery
        for message in messages:
            await check.on_result(message, message.timestamp)
    await check.expire_pending(messages[-1].timestamp + timedelta(seconds=61))
    assert await storage.db.qa_violations.count_documents({}) == 11
    assert await storage.db.qa_violations.count_documents({"type": "MISSING_UPSTREAM"}) == 5
    assert await storage.db.pending_qa_checks.count_documents({}) == 0
    statuses = {doc["_id"]: doc["status"] async for doc in storage.db.products.find()}
    assert sum(s == PASSED for s in statuses.values()) == 48
    assert sum(s == NON_CONFORMING for s in statuses.values()) == 11
    assert len(statuses) == 63
