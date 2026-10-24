"""HTTP routes: /health, /metrics, /machines, /products/{product_id}, and /violations."""

from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse

from . import metrics

router = APIRouter()


@router.get("/health")
async def health(request: Request) -> JSONResponse:
    """200 only if the Kafka consumer runs and reaches the broker, MongoDB answers, and consumption
    isn't paused.

    It reads state kept up to date in the background, so a probe never waits on MongoDB. This is the
    readiness probe; the liveness probe is a TCP check, so a MongoDB outage never restarts the pod.
    """
    state = request.app.state.service
    healthy = state.kafka_running and state.kafka_ok and state.mongodb_ok and not state.paused
    body = {
        "status": "ok" if healthy else "unavailable",
        "kafka_consumer": ("running" if state.kafka_ok else "broker unreachable") if state.kafka_running else "not running",
        "mongodb": "ok" if state.mongodb_ok else "unreachable",
        "paused": state.paused,
    }
    return JSONResponse(body, status_code=200 if healthy else 503)


@router.get("/metrics")
def prometheus_metrics() -> Response:
    # A plain route: mounting prometheus_client's ASGI app would redirect /metrics to /metrics/.
    return Response(metrics.generate_latest(), media_type=metrics.CONTENT_TYPE_LATEST)


@router.get("/machines")
async def machines(request: Request) -> list[dict]:
    """Integrity status and last-seen time of each machine in LINE_SEQUENCE."""
    now = datetime.now(timezone.utc)
    result = []
    for doc in await request.app.state.storage.machines():
        seen = doc.get("last_seen_at")
        result.append({
            "machine_id": doc["_id"],
            "status": doc.get("status", "UNKNOWN"),
            "last_seen_at": seen,
            "silence_seconds": round((now - (seen or doc["tracked_since"])).total_seconds(), 1),
        })
    return result


@router.get("/products/{product_id}")
async def product(product_id: str, request: Request) -> dict:
    """The product's latest result at each machine, and its status."""
    doc = await request.app.state.storage.product(product_id)
    if doc is None:
        raise HTTPException(404, f"unknown product {product_id}")
    return {"product_id": doc["_id"], "status": doc.get("status"), "results": doc.get("results", {})}


@router.get("/violations")
async def violations(
    request: Request,
    since: datetime | None = Query(None, description="Only violations detected at or after this time"),
    type: Literal["MISSING_UPSTREAM", "FAILED_UPSTREAM"] | None = None,
    limit: int = Query(100, ge=1, le=1000),
) -> list[dict]:
    """Recent QA violations, newest first."""
    docs = await request.app.state.storage.violations(since, type, limit)
    return [{"id": doc.pop("_id"), **doc} for doc in docs]
