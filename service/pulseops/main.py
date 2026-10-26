"""Application entry point: the FastAPI app, which starts the Kafka consumer and the check timers.

Run one uvicorn worker per pod (the metrics registry is per process) and scale with replicas.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pymongo.errors import PyMongoError

from . import metrics
from .api import router
from .checks.integrity import UP, IntegrityCheck
from .checks.quality import QualityCheck
from .config import Settings
from .consumer import Consumer, ServiceState
from .storage import Storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("pulseops")
HEALTH_INTERVAL_S = 5.0


async def every(interval_s: float, tick: Callable[[], Awaitable[None]]) -> None:
    """Run tick forever, every interval_s. A failed tick is logged and retried at the next one."""
    while True:
        try:
            await tick()
        except PyMongoError as e:
            log.warning("%s skipped, MongoDB operation failed: %s", tick.__name__, e)
        except Exception:
            log.exception("%s failed", tick.__name__)
        await asyncio.sleep(interval_s)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings.from_env()
    storage = Storage(settings.mongodb_uri, settings.mongodb_database, settings.line_sequence)
    state = ServiceState()
    quality = QualityCheck(
        storage, settings.line_sequence, settings.qa_grace_s,
        on_violation=lambda v: metrics.QA_VIOLATIONS.labels(v.machine_id, v.type).inc(),
    )
    integrity = IntegrityCheck(storage, settings.integrity_timeout_s)
    app.state.service = state
    app.state.storage = storage

    # Both timers run on every replica, against the shared state in MongoDB. They stop while paused,
    # and wait a full integrity timeout after (re)start or resume, so an unprocessed backlog isn't
    # mistaken for silent machines or missing upstream results.
    async def integrity_check() -> None:
        now = datetime.now(timezone.utc)
        if not state.can_judge(now):
            return
        for machine in await integrity.run_once(now):
            metrics.MACHINE_UP.labels(machine.machine_id).set(1 if machine.status == UP else 0)
            metrics.MACHINE_SILENCE.labels(machine.machine_id).set(machine.silence_s)

    async def qa_check() -> None:
        now = datetime.now(timezone.utc)
        if state.can_judge(now):
            await quality.expire_pending(now)
            # Shared state in MongoDB, like machine_up: every replica reports the same value.
            metrics.PENDING_QA_CHECKS.set(await storage.count_pending())

    async def mongodb_health() -> None:
        try:
            await storage.ping()
            state.mongodb_ok = True
        except PyMongoError:
            state.mongodb_ok = False

    async def consumer() -> None:
        # The consumer handles Kafka and MongoDB outages itself. If it fails anyway (a bug), restart it
        # rather than leave a pod that is alive but consumes nothing: the TCP liveness probe would never
        # notice.
        while True:
            try:
                await Consumer(settings, storage, quality, state).run()
            except Exception:
                log.exception("Kafka consumer failed; restarting it in 5 s")
                await asyncio.sleep(5)

    # Started in the background, not awaited: the port must open right away for the TCP liveness
    # probe, even while Kafka or MongoDB is still unreachable.
    tasks = [
        asyncio.create_task(consumer(), name="consumer"),
        asyncio.create_task(every(settings.integrity_interval_s, integrity_check)),
        asyncio.create_task(every(settings.qa_interval_s, qa_check)),
        asyncio.create_task(every(HEALTH_INTERVAL_S, mongodb_health)),
    ]
    yield
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)  # the consumer leaves the group on its way out
    await storage.close()


app = FastAPI(title="PulseOps", lifespan=lifespan)
app.include_router(router)


@app.exception_handler(PyMongoError)
async def mongodb_unavailable(request: Request, exc: PyMongoError) -> JSONResponse:
    return JSONResponse({"detail": "MongoDB is unavailable"}, status_code=503)
