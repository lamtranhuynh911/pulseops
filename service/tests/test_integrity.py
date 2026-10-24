"""Tests for the data integrity check."""

import asyncio
from datetime import datetime, timedelta, timezone

from pulseops.checks.integrity import DOWN, UP, IntegrityCheck, evaluate

T0 = datetime(2026, 10, 15, 8, 0, tzinfo=timezone.utc)
TIMEOUT_S = 15.0


def test_silence_up_to_the_timeout_is_up_beyond_it_down():
    tracked = {"machine-1": T0, "machine-2": T0}
    last_seen = {"machine-1": T0, "machine-2": T0 - timedelta(seconds=0.1)}
    states = {s.machine_id: s for s in evaluate(last_seen, tracked, T0 + timedelta(seconds=TIMEOUT_S), TIMEOUT_S)}
    assert states["machine-1"].status == UP
    assert states["machine-2"].status == DOWN
    assert states["machine-2"].silence_s == 15.1


def test_machine_never_heard_from_is_down_after_the_timeout():
    tracked = {"machine-3": T0}
    assert evaluate({}, tracked, T0 + timedelta(seconds=10), TIMEOUT_S)[0].status == UP
    assert evaluate({"machine-3": None}, tracked, T0 + timedelta(seconds=16), TIMEOUT_S)[0].status == DOWN


class MemoryMachines:
    def __init__(self):
        self.last_seen = {"machine-1": T0}
        self.tracked_since = {"machine-1": T0}
        self.status: dict[str, str] = {}

    async def machine_times(self):
        return self.last_seen, self.tracked_since

    async def set_machine_status(self, machine_id, status, now):
        if self.status.get(machine_id) == status:
            return False
        self.status[machine_id] = status
        return True


def test_status_changes_are_recorded_once(caplog):
    store = MemoryMachines()
    check = IntegrityCheck(store, TIMEOUT_S)
    caplog.set_level("INFO")

    async def scenario():
        for seconds in (1, 2, 16, 17, 18):
            await check.run_once(T0 + timedelta(seconds=seconds))
        store.last_seen["machine-1"] = T0 + timedelta(seconds=19)
        await check.run_once(T0 + timedelta(seconds=20))

    asyncio.run(scenario())
    assert [r.getMessage() for r in caplog.records] == ["machine-1 is UP", "machine-1 is DOWN: no data for 16 s", "machine-1 is UP"]
