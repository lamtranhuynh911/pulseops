"""Entry point: python -m simulator live | python -m simulator replay <directory>."""

import argparse
import asyncio
import contextlib
import logging
import os
import signal
from pathlib import Path

from .kafka import start_producer
from .live import Faults, LiveLine
from .replay import replay

DEFAULT_BOOTSTRAP = "kafka-0.kafka.pulseops.svc.cluster.local:9092"


def env_float(name: str, default: float) -> float:
    value = os.environ.get(name, "").strip()
    return float(value) if value else default


def faults_from_env() -> Faults:
    offline = os.environ.get("SIM_OFFLINE_MACHINES", "")
    return Faults(
        fail_rate=env_float("SIM_FAIL_RATE", 0.03),
        skip_rate=env_float("SIM_SKIP_RATE", 0.0),
        leak_rate=env_float("SIM_LEAK_RATE", 0.0),
        offline=frozenset(m.strip() for m in offline.split(",") if m.strip()),
    )


async def run(args: argparse.Namespace) -> int:
    """Run the command until it ends or the process gets SIGTERM/SIGINT. Returns the exit status."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    async def work() -> None:
        producer = await start_producer(os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP))
        try:
            if args.command == "replay":
                await replay(producer, Path(args.directory), env_float("REPLAY_SPEED", 10.0))
            else:
                faults = faults_from_env()
                logging.getLogger(__name__).info("live simulation with %s", faults)
                await LiveLine(
                    producer,
                    product_interval_s=env_float("SIM_PRODUCT_INTERVAL_SECONDS", 2.0),
                    heartbeat_interval_s=env_float("SIM_HEARTBEAT_INTERVAL_SECONDS", 5.0),
                    faults=faults,
                ).run()
        finally:
            await producer.stop()  # flushes what was already sent

    # Waiting for Kafka is part of the work, so a signal also stops a simulator that hasn't connected.
    task = asyncio.create_task(work())
    stopper = asyncio.create_task(stop.wait())
    await asyncio.wait({task, stopper}, return_when=asyncio.FIRST_COMPLETED)
    stopper.cancel()
    if task.done():
        task.result()  # re-raise a failure
        return 0
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    # A replay stopped halfway published only part of its scenario: fail, so the Job retries it
    # (republishing the same event_ids is harmless) instead of reporting success.
    return 143 if args.command == "replay" else 0


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(prog="simulator")
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("live", help="simulate machines 1-3 (default)")
    replay_parser = commands.add_parser("replay", help="replay a mock-data scenario directory")
    replay_parser.add_argument("directory")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
