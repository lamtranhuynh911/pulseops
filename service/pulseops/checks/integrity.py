"""Data integrity check: tracks when each machine was last heard from and marks it UP or DOWN.

Last-seen times live in MongoDB (machines.last_seen_at, the service's processing time), because a
replica only consumes some partitions and would otherwise miss other machines' heartbeats. Every
replica runs this check on a timer; a status change is a conditional update, so exactly one replica
records and logs it.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

log = logging.getLogger(__name__)

UP = "UP"
DOWN = "DOWN"


@dataclass(frozen=True)
class MachineState:
    machine_id: str
    status: str
    silence_s: float


def evaluate(
    last_seen: dict[str, datetime | None],
    tracked_since: dict[str, datetime],
    now: datetime,
    timeout_s: float,
) -> list[MachineState]:
    """Judge every tracked machine. A machine never heard from counts as silent since tracking began."""
    states = []
    for machine_id, since in tracked_since.items():
        seen = last_seen.get(machine_id) or since
        silence = max(0.0, (now - seen).total_seconds())
        states.append(MachineState(machine_id, UP if silence <= timeout_s else DOWN, silence))
    return states


class MachineStore(Protocol):
    async def machine_times(self) -> tuple[dict[str, datetime | None], dict[str, datetime]]:
        """(last_seen_at by machine, tracked_since by machine) for every machine in LINE_SEQUENCE."""

    async def set_machine_status(self, machine_id: str, status: str, now: datetime, cutoff: datetime) -> bool:
        """Store the status only if it differs and the stored times (last seen before or after cutoff)
        still support it; True if this call changed it."""


class IntegrityCheck:
    def __init__(self, store: MachineStore, timeout_s: float):
        self.store = store
        self.timeout_s = timeout_s

    async def run_once(self, now: datetime) -> list[MachineState]:
        last_seen, tracked_since = await self.store.machine_times()
        states = evaluate(last_seen, tracked_since, now, self.timeout_s)
        cutoff = now - timedelta(seconds=self.timeout_s)
        for state in states:
            if await self.store.set_machine_status(state.machine_id, state.status, now, cutoff):
                if state.status == DOWN:
                    log.warning("%s is DOWN: no data for %.0f s", state.machine_id, state.silence_s)
                else:
                    log.info("%s is UP", state.machine_id)
        return states
