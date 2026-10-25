"""QA check: flags products that reach a machine without passing the previous one.

On a result from machine N, only the product's latest result at machine N-1 is checked, so each hop
is checked exactly once. A missing upstream result becomes a pending check for QA_GRACE_SECONDS,
because machines publish independently and results can arrive out of order. Pending checks live in
MongoDB. Any replica's timer may lease an expired one; it is deleted only after its verdict is stored,
so a replica that dies halfway loses nothing: the lease runs out and another replica judges it again.
There is at most one verdict per downstream result and hop (the first one stored wins), so
redelivered messages and repeated judgements are harmless.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from ..concurrency import gather_all
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
    # The upstream machine's state when the downstream result arrived, so that a MISSING_UPSTREAM
    # caused by a data outage is flagged even if the machine is back when the grace period ends.
    # (Defaults: documents stored by older versions lack these fields.)
    upstream_down_at_arrival: bool = False
    created_at: datetime | None = None


@dataclass(frozen=True)
class Violation:
    id: str                     # "<event_id>:<type>"
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

    async def pending_checks(self, product_id: str, machine_id: str) -> list[PendingCheck]:
        """The checks of this product at this machine that wait for their upstream result."""

    async def claim_expired(self, now: datetime) -> PendingCheck | None:
        """Lease one pending check that is due and not leased by another replica."""

    async def finish_pending(self, check: PendingCheck) -> None:
        """Delete a pending check whose verdict is stored."""

    async def insert_violation(self, violation: Violation) -> bool:
        """Store the verdict unless this downstream result already has one for this hop; True if stored."""

    async def touch_product(self, product_id: str) -> None:
        """Record that the product changed (its status must be recomputed)."""

    async def machine_state(self, machine_id: str) -> tuple[str | None, datetime | None]:
        """The machine's integrity status and when it last changed."""

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
        await gather_all(self._check_upstream(result, position, latest, now),
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
            status, _ = await self.store.machine_state(upstream)
            await self.store.add_pending(PendingCheck(event_id, result.product_id, result.machine_id, upstream,
                                                      due_at=now + self.grace, upstream_down_at_arrival=status == "DOWN",
                                                      created_at=now))

    async def _resolve_downstream(self, result: TestResult, position: int, latest: dict[str, str],
                                  now: datetime) -> None:
        if position == len(self.line) - 1:
            return
        downstream = self.line[position + 1]  # results there may be waiting for this one
        for check in await self.store.pending_checks(result.product_id, downstream):
            await self._judge(check.product_id, check.machine_id, check.upstream_machine_id, check.event_id,
                              latest.get(result.machine_id), now, check)
            await self.store.finish_pending(check)

    async def expire_pending(self, now: datetime) -> int:
        """Turn every due pending check into a verdict. Returns how many were handled."""
        handled = 0
        while check := await self.store.claim_expired(now):
            # The upstream result may have arrived just before the claim.
            upstream_result = (await self.store.latest_results(check.product_id)).get(check.upstream_machine_id)
            await self._judge(check.product_id, check.machine_id, check.upstream_machine_id, check.event_id,
                              upstream_result, now, check)
            # Refreshed before the delete too: a stored violation makes the status final (NON_CONFORMING),
            # so a failure or SIGTERM after the delete can't leave it IN_PROGRESS. A failure before the
            # delete leaves the leased check, which is judged and refreshed again.
            await self.store.refresh_product_status(check.product_id)
            await self.store.finish_pending(check)  # only now: until here the lease protects it
            await self.store.refresh_product_status(check.product_id)  # no longer pending
            handled += 1
        return handled

    async def _judge(self, product_id: str, machine_id: str, upstream_machine_id: str, event_id: str,
                     upstream_result: str | None, now: datetime, pending: PendingCheck | None = None) -> None:
        violation_type = decide(upstream_result)
        if violation_type is None:
            return
        status, changed_at = await self.store.machine_state(upstream_machine_id)
        upstream_down = status == "DOWN"
        if pending is not None:
            # DOWN at any time while the check was pending: when the result arrived, now, or in
            # between (a status that changed since, now UP, was DOWN before that change).
            upstream_down = (upstream_down or pending.upstream_down_at_arrival
                             or (changed_at is not None and pending.created_at is not None
                                 and changed_at > pending.created_at))
        violation = Violation(
            id=f"{event_id}:{violation_type}",
            type=violation_type,
            product_id=product_id,
            machine_id=machine_id,
            upstream_machine_id=upstream_machine_id,
            event_id=event_id,
            upstream_machine_down=upstream_down,
            detected_at=now,
        )
        if await self.store.insert_violation(violation):
            # Counted right after the insert: if what follows fails, the retry finds the verdict
            # stored and must not count it again.
            log.warning("%s at %s for %s (upstream %s%s)", violation.type, machine_id, product_id,
                        upstream_machine_id, " is DOWN" if violation.upstream_machine_down else "")
            self.on_violation(violation)
        await self.store.touch_product(product_id)
