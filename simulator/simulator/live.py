"""Live simulation: products move through machines 1-3 while each machine sends heartbeats.

Faults are injected with the SIM_* settings (see the README's "Demo scenarios").
"""

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer

from . import messages
from .kafka import send
from .machines import MACHINES, STAGE_S

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Faults:
    """Per-machine probabilities (see "Demo scenarios" in the README)."""

    fail_rate: float = 0.0           # the test fails and the product (correctly) leaves the line
    skip_rate: float = 0.0           # the product bypasses machine 1 or 2 (MISSING_UPSTREAM next)
    leak_rate: float = 0.0           # the product fails at machine 1 or 2 but keeps moving (FAILED_UPSTREAM next)
    offline: frozenset[str] = frozenset()  # machines that publish nothing at all


def plan_journey(faults: Faults, rng: random.Random) -> tuple[str | None, ...]:
    """The result a product gets at each machine, or None where the machine has no record of it."""
    journey: list[str | None] = []
    left_line = False
    for stage in range(len(MACHINES)):
        if left_line:
            journey.append(None)
            continue
        # Skipping or leaking past the last machine can't be detected (no later machine checks it),
        # so these faults only apply to machines 1 and 2.
        if stage < len(MACHINES) - 1:
            if rng.random() < faults.skip_rate:
                journey.append(None)
                continue
            if rng.random() < faults.leak_rate:
                journey.append("FAIL")
                continue
        result = "FAIL" if rng.random() < faults.fail_rate else "PASS"
        journey.append(result)
        left_line = result == "FAIL"
    return tuple(journey)


class LiveLine:
    def __init__(self, producer: AIOKafkaProducer, product_interval_s: float, heartbeat_interval_s: float,
                 faults: Faults, rng: random.Random | None = None):
        self.producer = producer
        self.product_interval_s = product_interval_s
        self.heartbeat_interval_s = heartbeat_interval_s
        self.faults = faults
        self.rng = rng or random.Random()
        self.last_product = 0
        self.published = 0

    async def run(self) -> None:
        async with asyncio.TaskGroup() as tasks:
            for machine in MACHINES:
                tasks.create_task(self._heartbeats(machine.id))
            tasks.create_task(self._products(tasks))

    async def _heartbeats(self, machine_id: str) -> None:
        while True:
            if machine_id not in self.faults.offline:
                now = datetime.now(timezone.utc)
                await send(self.producer, messages.HEARTBEATS_TOPIC, machine_id, messages.heartbeat(machine_id, now))
            await asyncio.sleep(self.heartbeat_interval_s)

    async def _products(self, tasks: asyncio.TaskGroup) -> None:
        while True:
            product_id = self._new_product_id()
            tasks.create_task(self._move(product_id, plan_journey(self.faults, self.rng)))
            await asyncio.sleep(self.product_interval_s)

    def _new_product_id(self) -> str:
        """A millisecond timestamp, strictly increasing: unique across restarts at any product rate,
        and far from the mock-data ranges (P-000001 to P-000163)."""
        self.last_product = max(self.last_product + 1, time.time_ns() // 1_000_000)
        return f"P-{self.last_product}"

    async def _move(self, product_id: str, journey: tuple[str | None, ...]) -> None:
        entered = asyncio.get_running_loop().time()
        for stage, (machine, result) in enumerate(zip(MACHINES, journey)):
            if result is None:
                continue
            measurements = machine.measurements(result, self.rng)
            finish = entered + stage * STAGE_S + measurements["cycle_time_s"]
            await asyncio.sleep(max(0.0, finish - asyncio.get_running_loop().time()))
            if machine.id in self.faults.offline:
                continue  # the machine still tests the product, but its data never arrives
            message = messages.test_result(machine.id, product_id, result, measurements, datetime.now(timezone.utc))
            await send(self.producer, messages.TEST_RESULTS_TOPIC, product_id, message)
            self.published += 1
            if self.published % 50 == 0:
                log.info("published %d test results (latest %s at %s: %s)", self.published, product_id, machine.id, result)
