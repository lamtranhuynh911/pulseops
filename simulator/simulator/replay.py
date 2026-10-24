"""Replay a mock-data scenario (one CSV per machine) into Kafka, with heartbeats.

Results keep their event_id and timestamp, so replaying twice must not change anything in the
service. The CSVs contain no heartbeats, so the replay sends one per machine every 5 s; without them,
gaps of up to ~20 s between rows would mark machines DOWN.
"""

import asyncio
import csv
import logging
from datetime import datetime, timezone
from pathlib import Path

from aiokafka import AIOKafkaProducer

from . import messages
from .kafka import send

log = logging.getLogger(__name__)
HEARTBEAT_INTERVAL_S = 5.0


def load_scenario(directory: Path) -> list[dict]:
    """All test-result messages of a scenario, in timestamp order."""
    rows = []
    for path in sorted(directory.glob("machine-*.csv")):
        with path.open(newline="", encoding="utf-8") as f:
            rows.extend(messages.from_csv_row(row) for row in csv.DictReader(f))
    if not rows:
        raise SystemExit(f"no machine-*.csv files in {directory}")
    return sorted(rows, key=lambda message: message["timestamp"])


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


async def replay(producer: AIOKafkaProducer, directory: Path, speed: float) -> int:
    results = load_scenario(directory)
    machine_ids = sorted({message["machine_id"] for message in results})
    log.info("replaying %d results from %s for %s at %gx speed", len(results), directory, ", ".join(machine_ids), speed)
    heartbeats = asyncio.create_task(_heartbeats(producer, machine_ids))
    try:
        loop = asyncio.get_running_loop()
        started = loop.time()
        first = parse_timestamp(results[0]["timestamp"])
        for message in results:
            due = started + (parse_timestamp(message["timestamp"]) - first).total_seconds() / speed
            await asyncio.sleep(max(0.0, due - loop.time()))
            await send(producer, messages.TEST_RESULTS_TOPIC, message["product_id"], message)
    finally:
        heartbeats.cancel()
    log.info("replay done: %d results published", len(results))
    return len(results)


async def _heartbeats(producer: AIOKafkaProducer, machine_ids: list[str]) -> None:
    while True:
        now = datetime.now(timezone.utc)
        for machine_id in machine_ids:
            await send(producer, messages.HEARTBEATS_TOPIC, machine_id, messages.heartbeat(machine_id, now))
        await asyncio.sleep(HEARTBEAT_INTERVAL_S)
