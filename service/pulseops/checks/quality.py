"""QA check: flags products that reach a machine without passing the previous one.

On a result from machine N, only the product's latest result at machine N-1 is checked, so each hop
is checked exactly once. A missing upstream result becomes a pending check for QA_GRACE_SECONDS,
because machines publish independently and results can arrive out of order. Pending checks live in
MongoDB; any replica's timer may claim an expired one (find_one_and_delete), so exactly one records
the MISSING_UPSTREAM violation. Violation IDs are deterministic, so redelivered messages are harmless.
"""

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from ..models import TestResult

log = logging.getLogger(__name__)

FAILED_UPSTREAM = "FAILED_UPSTREAM"
MISSING_UPSTREAM = "MISSING_UPSTREAM"

IN_PROGRESS = "IN_PROGRESS"
PASSED = "PASSED"
FAILED = "FAILED"
NON_CONFORMING = "NON_CONFORMING"


@dataclass(frozen=True)
class PendingCheck:
    event_id: str               # the downstream result waiting for its upstream result
    product_id: str
    machine_id: str
    upstream_machine_id: str
    due_at: datetime


@dataclass(frozen=True)
class Violation:
    id: str                     # "<event_id>:<type>": the same message always yields the same ID
    type: str
    product_id: str
    machine_id: str             # where the product arrived without a passing upstream result
    upstream_machine_id: str
    event_id: str
    upstream_machine_down: bool
    detected_at: datetime


def decide(upstream_result: str | None) -> str | None:
    """The violation for a downstream result, given the upstream result (None = no result yet)."""
    if upstream_result is None:
        return MISSING_UPSTREAM
    return FAILED_UPSTREAM if upstream_result == "FAIL" else None


def product_status(results: dict[str, str], line: tuple[str, ...], has_violation: bool, has_pending: bool) -> str:
    """Status from the latest result at each machine, and whether the product has violations or pending checks."""
    if has_violation:
        return NON_CONFORMING
    if "FAIL" in results.values():
        return FAILED            # failed a test and (correctly) left the line
    if has_pending:
        return IN_PROGRESS       # not judged until the upstream result arrives or the grace period ends
    return PASSED if results.get(line[-1]) == "PASS" else IN_PROGRESS


class QaStore(Protocol):
    async def record_result(self, result: TestResult) -> dict[str, str]:
        """Store the result as the product's latest at its machine (unless a newer one is stored).
        Returns the product's latest result per machine."""

    async def latest_results(self, product_id: str) -> dict[str, str]: ...

    async def add_pending(self, check: PendingCheck) -> None: ...

    async def claim_pending(self, product_id: str, machine_id: str) -> PendingCheck | None:
        """Atomically remove and return one pending check of this product at this machine."""

    async def claim_expired(self, now: datetime) -> PendingCheck | None:
        """Atomically remove and return one pending check that is due."""

    async def add_violation(self, violation: Violation) -> bool:
        """Store the violation; True only the first time this ID is stored."""

    async def machine_status(self, machine_id: str) -> str | None: ...

    async def refresh_product_status(self, product_id: str) -> str: ...


class QualityCheck:
    def __init__(self, store: QaStore, line: tuple[str, ...], grace_s: float,
                 on_violation: Callable[[Violation], None] = lambda violation: None):
        self.store = store
        self.line = line
        self.grace = timedelta(seconds=grace_s)
        self.on_violation = on_violation

    async def on_result(self, result: TestResult, now: datetime) -> None:
        latest = await self.store.record_result(result)
        position = self.line.index(result.machine_id)
        # Checking this result against the previous machine, and resolving checks at the next machine
        # that waited for this result, are independent: run them concurrently to save round trips.
        await asyncio.gather(self._check_upstream(result, position, latest, now),
                             self._resolve_downstream(result, position, latest, now))
        await self.store.refresh_product_status(result.product_id)

    async def _check_upstream(self, result: TestResult, position: int, latest: dict[str, str], now: datetime) -> None:
        if position == 0:
            return  # machine 1 has no previous machine: its results are only recorded
        upstream = self.line[position - 1]
        event_id = str(result.event_id)
        if upstream in latest:
            await self._judge(result.product_id, result.machine_id, upstream, event_id, latest[upstream], now)
        else:
            await self.store.add_pending(
                PendingCheck(event_id, result.product_id, result.machine_id, upstream, now + self.grace)
            )

    async def _resolve_downstream(self, result: TestResult, position: int, latest: dict[str, str],
                                  now: datetime) -> None:
        if position == len(self.line) - 1:
            return
        downstream = self.line[position + 1]  # results there may be waiting for this one
        while check := await self.store.claim_pending(result.product_id, downstream):
            await self._judge(check.product_id, check.machine_id, check.upstream_machine_id, check.event_id,
                              latest.get(result.machine_id), now)

    async def expire_pending(self, now: datetime) -> int:
        """Turn every due pending check into a verdict. Returns how many were handled."""
        handled = 0
        while check := await self.store.claim_expired(now):
            # The upstream result may have arrived just before the claim.
            upstream_result = (await self.store.latest_results(check.product_id)).get(check.upstream_machine_id)
            await self._judge(check.product_id, check.machine_id, check.upstream_machine_id, check.event_id,
                              upstream_result, now)
            await self.store.refresh_product_status(check.product_id)
            handled += 1
        return handled

    async def _judge(self, product_id: str, machine_id: str, upstream_machine_id: str, event_id: str,
                     upstream_result: str | None, now: datetime) -> None:
        violation_type = decide(upstream_result)
        if violation_type is None:
            return
        violation = Violation(
            id=f"{event_id}:{violation_type}",
            type=violation_type,
            product_id=product_id,
            machine_id=machine_id,
            upstream_machine_id=upstream_machine_id,
            event_id=event_id,
            upstream_machine_down=await self.store.machine_status(upstream_machine_id) == "DOWN",
            detected_at=now,
        )
        if await self.store.add_violation(violation):
            log.warning("%s at %s for %s (upstream %s%s)", violation.type, machine_id, product_id,
                        upstream_machine_id, " is DOWN" if violation.upstream_machine_down else "")
            self.on_violation(violation)
