"""Shared test helpers: mock-data loading and an in-memory store with the same semantics as MongoDB."""

import csv
from datetime import datetime
from pathlib import Path

from pymongo.errors import AutoReconnect

from pulseops.checks.quality import PendingCheck, Violation, product_status
from pulseops.models import TestResult
from pulseops.storage import PENDING_LEASE

MOCK_DATA = Path(__file__).resolve().parents[2] / "mock-data"
LINE = ("machine-1", "machine-2", "machine-3")
NOT_MEASUREMENTS = ("event_id", "machine_id", "product_id", "result", "timestamp")


def load_scenario(name: str) -> list[TestResult]:
    """A mock-data scenario as test-result messages, merged in timestamp order."""
    messages = []
    for path in sorted((MOCK_DATA / name).glob("machine-*.csv")):
        with path.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                messages.append(TestResult.model_validate({
                    "schema_version": 1,
                    **{key: row[key] for key in NOT_MEASUREMENTS},
                    "measurements": {k: float(v) for k, v in row.items() if k not in NOT_MEASUREMENTS},
                }))
    return sorted(messages, key=lambda message: message.timestamp)


def result(machine_id: str, product_id: str, outcome: str, timestamp: datetime, event_id: str) -> TestResult:
    return TestResult(schema_version=1, event_id=event_id, machine_id=machine_id, product_id=product_id,
                      result=outcome, timestamp=timestamp)


class MemoryStore:
    """Implements the QaStore protocol in memory, with the semantics of the MongoDB implementation."""

    def __init__(self, line: tuple[str, ...] = LINE):
        self.line = line
        self.results: dict[str, dict[str, tuple[str, datetime]]] = {}
        self.pending: dict[str, PendingCheck] = {}
        self.leased: dict[str, datetime] = {}          # event_id -> lease end
        self.violations: dict[tuple[str, str], Violation] = {}  # (event_id, upstream machine) -> verdict
        self.statuses: dict[str, str] = {}
        self.machine_statuses: dict[str, str] = {}
        self.machine_changed_at: dict[str, datetime] = {}
        self.fail_next_insert_violation = False
        self.fail_next_finish_pending = False   # the delete is applied, then the call fails

    async def record_result(self, message: TestResult) -> dict[str, str]:
        latest = self.results.setdefault(message.product_id, {})
        stored = latest.get(message.machine_id)
        if stored is None or stored[1] <= message.timestamp:
            latest[message.machine_id] = (message.result, message.timestamp)
        return await self.latest_results(message.product_id)

    async def latest_results(self, product_id: str) -> dict[str, str]:
        return {machine: entry[0] for machine, entry in self.results.get(product_id, {}).items()}

    async def add_pending(self, check: PendingCheck) -> None:
        self.pending.setdefault(check.event_id, check)

    async def pending_checks(self, product_id: str, machine_id: str) -> list[PendingCheck]:
        return [c for c in self.pending.values() if c.product_id == product_id and c.machine_id == machine_id]

    async def claim_expired(self, now: datetime) -> PendingCheck | None:
        for event_id, check in self.pending.items():
            if check.due_at <= now and self.leased.get(event_id, now) <= now:
                self.leased[event_id] = now + PENDING_LEASE
                return check
        return None

    async def finish_pending(self, check: PendingCheck) -> None:
        self.pending.pop(check.event_id, None)
        self.leased.pop(check.event_id, None)
        if self.fail_next_finish_pending:
            self.fail_next_finish_pending = False
            raise AutoReconnect("connection lost after the delete")

    async def insert_violation(self, violation: Violation) -> bool:
        if self.fail_next_insert_violation:
            self.fail_next_insert_violation = False
            raise AutoReconnect("connection lost")
        key = (violation.event_id, violation.upstream_machine_id)
        if key in self.violations:
            return False
        self.violations[key] = violation
        return True

    async def touch_product(self, product_id: str) -> None:
        pass

    async def machine_state(self, machine_id: str) -> tuple[str | None, datetime | None]:
        return self.machine_statuses.get(machine_id), self.machine_changed_at.get(machine_id)

    async def refresh_product_status(self, product_id: str) -> str:
        status = product_status(
            await self.latest_results(product_id),
            self.line,
            has_violation=any(v.product_id == product_id for v in self.violations.values()),
            has_pending=any(c.product_id == product_id for c in self.pending.values()),
        )
        self.statuses[product_id] = status
        return status
