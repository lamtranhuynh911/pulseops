"""MongoDB access: collections, indexes, and idempotent writes keyed by event_id.

Every write can be repeated without changing the result, because Kafka delivers at least once.
Any pymongo error (PyMongoError) is a storage failure, not a problem with the message: the consumer
pauses and retries the message once MongoDB answers again.
"""

import asyncio
import functools
import time
from dataclasses import asdict, fields
from datetime import datetime, timedelta

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError

from . import metrics
from .checks.quality import FAILED, NON_CONFORMING, PASSED, PendingCheck, Violation, product_status
from .models import TestResult

# A message whose raw value is bigger than this is stored truncated in rejected_messages.
MAX_RAW_CHARS = 10_000
# How long a replica's claim on an expired pending check lasts. If the replica dies before storing the
# verdict, another replica claims the check once the lease has run out.
PENDING_LEASE = timedelta(seconds=30)
_PENDING_FIELDS = {f.name for f in fields(PendingCheck)}
# At most one verdict per downstream result and hop, whatever its type.
_VERDICT_KEY = [("event_id", ASCENDING), ("upstream_machine_id", ASCENDING)]
FINAL_STATUSES = (PASSED, FAILED, NON_CONFORMING)


def timed(operation: str):
    """Record the duration of a MongoDB operation, failed ones included, in pulseops_mongodb_operation_seconds."""
    def decorator(method):
        @functools.wraps(method)
        async def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return await method(*args, **kwargs)
            finally:
                metrics.MONGODB_OPERATION.labels(operation).observe(time.perf_counter() - start)
        return wrapper
    return decorator


class DuplicateVerdicts(Exception):
    """qa_violations holds several verdicts for one downstream result and hop, so its unique index can't
    be built. Versions before that index stored one verdict per type. The extra ones are not deleted
    automatically (they are someone's data); README "Upgrading a database" shows how to remove them."""


class Storage:
    def __init__(self, uri: str, database: str, line: tuple[str, ...]):
        # Timeouts of 5 s: an outage is noticed in seconds, not after the 30 s default, while a distant
        # server whose new connections sometimes take over 2 s (lost SYN packets) isn't mistaken for
        # an outage. No retryable writes: the consumer retries failed writes itself (every write is
        # idempotent), and some standalone servers and proxies refuse them ("Transaction numbers are
        # only allowed on a replica set member or mongos").
        # socketTimeoutMS: when MongoDB disappears without closing connections (packets dropped, e.g. a
        # deleted pod or a network partition), an operation in flight would otherwise wait forever
        # instead of failing, and the consumer would hang instead of pausing.
        self.client = AsyncMongoClient(uri, tz_aware=True, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000,
                                       socketTimeoutMS=10000, retryWrites=False)
        self.db = self.client[database]
        self.line = line

    async def close(self) -> None:
        await self.client.close()

    async def ping(self) -> None:
        await self.client.admin.command("ping")

    async def prepare(self, now: datetime) -> None:
        """Create the indexes and the machine documents. Idempotent, so it runs at every (re)start."""
        db = self.db
        await db.test_results.create_index("event_id", unique=True)
        await db.test_results.create_index([("product_id", ASCENDING), ("timestamp", ASCENDING)])
        await db.test_results.create_index([("machine_id", ASCENDING), ("timestamp", ASCENDING)])
        await db.test_results.create_index("timestamp")
        await db.qa_violations.create_index([("detected_at", DESCENDING)])
        await db.qa_violations.create_index("product_id")
        try:
            await db.qa_violations.create_index(_VERDICT_KEY, unique=True)
        except DuplicateKeyError as e:
            raise DuplicateVerdicts(
                "qa_violations has several verdicts for one downstream result and hop (written by an older "
                'version), so its unique index can\'t be built: remove the extra ones, see README "Upgrading '
                f'a database" ({e})') from e
        await db.pending_qa_checks.create_index("due_at")
        await db.pending_qa_checks.create_index([("product_id", ASCENDING), ("machine_id", ASCENDING)])
        await db.rejected_messages.create_index([("received_at", DESCENDING)])
        for machine_id in self.line:
            await db.machines.update_one({"_id": machine_id}, {"$setOnInsert": {"tracked_since": now}}, upsert=True)

    # --- messages -------------------------------------------------------------------------------

    @timed("insert_test_result")
    async def insert_test_result(self, result: TestResult, received_at: datetime) -> bool:
        """Store the result; False if this event_id is already stored (a redelivery)."""
        document = result.model_dump(mode="python")
        document["event_id"] = str(result.event_id)
        document["received_at"] = received_at
        try:
            await self.db.test_results.insert_one(document)
            return True
        except DuplicateKeyError:
            return False

    @timed("touch_machine")
    async def touch_machine(self, machine_id: str, now: datetime) -> None:
        """Record a sign of life. $max: a late or redelivered message never moves last_seen_at back."""
        await self.db.machines.update_one(
            {"_id": machine_id}, {"$max": {"last_seen_at": now}, "$setOnInsert": {"tracked_since": now}}, upsert=True
        )

    @timed("insert_rejected")
    async def insert_rejected(self, topic: str, partition: int, offset: int, kafka_timestamp: int, reason: str,
                              detail: str, raw: bytes | None, received_at: datetime) -> bool:
        """Store a message that failed validation; False if it was already stored (a redelivery).

        The key includes the record's Kafka timestamp: offsets start again at 0 when the topics are
        recreated (a new cluster), and a new message at an old position must not look like a redelivery.
        """
        result = await self.db.rejected_messages.update_one(
            {"_id": f"{topic}:{partition}:{offset}:{kafka_timestamp}"},
            {"$setOnInsert": {
                "topic": topic, "partition": partition, "offset": offset, "kafka_timestamp": kafka_timestamp,
                "reason": reason, "detail": detail,
                "raw": (raw or b"").decode("utf-8", errors="replace")[:MAX_RAW_CHARS], "received_at": received_at,
            }},
            upsert=True,
        )
        return result.upserted_id is not None

    # --- integrity check (MachineStore) ---------------------------------------------------------

    @timed("machine_times")
    async def machine_times(self) -> tuple[dict[str, datetime | None], dict[str, datetime]]:
        last_seen, tracked_since = {}, {}
        async for doc in self.db.machines.find({"_id": {"$in": list(self.line)}}):
            last_seen[doc["_id"]] = doc.get("last_seen_at")
            tracked_since[doc["_id"]] = doc["tracked_since"]
        return last_seen, tracked_since

    @timed("set_machine_status")
    async def set_machine_status(self, machine_id: str, status: str, now: datetime, cutoff: datetime) -> bool:
        """Store the status if it differs and the stored times still support it; True if this call changed it.

        cutoff is now - INTEGRITY_TIMEOUT_SECONDS. Checking the evidence in the filter means a verdict
        computed from older reads (another replica, a slow round trip) can't undo a newer one: DOWN
        needs no message since the cutoff, and UP needs a message after the last status change (the
        DOWN it replaces).
        """
        if status == "DOWN":
            evidence = {"$or": [{"last_seen_at": {"$lt": cutoff}},
                                {"last_seen_at": {"$exists": False}, "tracked_since": {"$lt": cutoff}}]}
        else:
            evidence = {"last_seen_at": {"$gte": cutoff},
                        "$or": [{"status_changed_at": {"$exists": False}},
                                {"$expr": {"$gt": ["$last_seen_at", "$status_changed_at"]}}]}
        result = await self.db.machines.update_one(
            {"_id": machine_id, "status": {"$ne": status}, **evidence},
            {"$set": {"status": status, "status_changed_at": now}},
        )
        return result.modified_count == 1

    @timed("machine_state")
    async def machine_state(self, machine_id: str) -> tuple[str | None, datetime | None]:
        doc = await self.db.machines.find_one({"_id": machine_id}, {"status": 1, "status_changed_at": 1})
        return (doc.get("status"), doc.get("status_changed_at")) if doc else (None, None)

    async def machines(self) -> list[dict]:
        return [doc async for doc in self.db.machines.find({"_id": {"$in": list(self.line)}}).sort("_id")]

    # --- QA check (QaStore) ---------------------------------------------------------------------
    # products: {_id: product_id, results: {machine_id: {result, event_id, timestamp}}, status, rev}.
    # Every change to a product (results, pending checks, violations, status) increments rev, after the
    # change itself, so refresh_product_status can write the derived status with a compare-and-set on rev.

    @timed("record_result")
    async def record_result(self, result: TestResult) -> dict[str, str]:
        field = f"results.{result.machine_id}"
        entry = {"result": result.result, "event_id": str(result.event_id), "timestamp": result.timestamp}
        # Only replace the stored result at this machine if this one is at least as recent (retests win).
        newer_or_missing = {"$or": [{field: {"$exists": False}}, {f"{field}.timestamp": {"$lte": result.timestamp}}]}
        try:
            doc = await self.db.products.find_one_and_update(
                {"_id": result.product_id, **newer_or_missing},
                {"$set": {field: entry}, "$inc": {"rev": 1}},
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
        except DuplicateKeyError:
            # The product exists with a newer result at this machine: keep it.
            doc = await self.db.products.find_one({"_id": result.product_id})
        return _latest(doc)

    @timed("latest_results")
    async def latest_results(self, product_id: str) -> dict[str, str]:
        return _latest(await self.db.products.find_one({"_id": product_id}))

    @timed("add_pending")
    async def add_pending(self, check: PendingCheck) -> None:
        await self.db.pending_qa_checks.update_one({"_id": check.event_id}, {"$setOnInsert": asdict(check)}, upsert=True)
        await self.touch_product(check.product_id)  # after the insert (see refresh_product_status)

    @timed("pending_checks")
    async def pending_checks(self, product_id: str, machine_id: str) -> list[PendingCheck]:
        cursor = self.db.pending_qa_checks.find({"product_id": product_id, "machine_id": machine_id})
        return [_pending(doc) async for doc in cursor]

    @timed("claim_expired")
    async def claim_expired(self, now: datetime) -> PendingCheck | None:
        doc = await self.db.pending_qa_checks.find_one_and_update(
            {"due_at": {"$lte": now}, "$or": [{"leased_until": {"$exists": False}}, {"leased_until": {"$lte": now}}]},
            {"$set": {"leased_until": now + PENDING_LEASE}},
        )
        return _pending(doc) if doc else None

    @timed("finish_pending")
    async def finish_pending(self, check: PendingCheck) -> None:
        await self.db.pending_qa_checks.delete_one({"_id": check.event_id})
        await self.touch_product(check.product_id)

    @timed("insert_violation")
    async def insert_violation(self, violation: Violation) -> bool:
        document = asdict(violation)
        document["_id"] = document.pop("id")
        try:
            result = await self.db.qa_violations.update_one(
                {"event_id": violation.event_id, "upstream_machine_id": violation.upstream_machine_id},
                {"$setOnInsert": document},
                upsert=True,
            )
        except DuplicateKeyError:
            return False  # another replica stored a verdict for this hop at the same moment
        return result.upserted_id is not None

    async def touch_product(self, product_id: str) -> None:
        await self.db.products.update_one({"_id": product_id}, {"$inc": {"rev": 1}}, upsert=True)

    @timed("refresh_product_status")
    async def refresh_product_status(self, product_id: str) -> str:
        while True:
            # Why this is safe with concurrent writers (other replicas' QA timers): each writer
            # increments rev after its change, then refreshes. The product is read before the counts,
            # so a change the counts miss happened after that read, and its rev increment either makes
            # the compare-and-set below fail, or comes after it, in which case the writer's own
            # refresh runs later and corrects the status. Status writes increment rev too.
            doc = await self.db.products.find_one({"_id": product_id})
            if doc is None:
                return ""
            violations, pending = await asyncio.gather(
                self.db.qa_violations.count_documents({"product_id": product_id}, limit=1),
                self.db.pending_qa_checks.count_documents({"product_id": product_id}, limit=1),
            )
            status = product_status(_latest(doc), self.line, has_violation=violations > 0, has_pending=pending > 0)
            if doc.get("status") == status:
                return status
            written = await self.db.products.update_one(
                {"_id": product_id, "rev": doc["rev"]}, {"$set": {"status": status}, "$inc": {"rev": 1}}
            )
            if written.modified_count == 1:
                # Exactly one writer wins the compare-and-set, so each status change is counted once.
                if status in FINAL_STATUSES:
                    metrics.PRODUCT_STATUS_CHANGES.labels(status).inc()
                if status == PASSED and (seconds := line_seconds(doc)) is not None:
                    metrics.PRODUCT_LINE_TIME.observe(seconds)
                return status
            # Another writer changed the product meanwhile: recompute from the new state.

    @timed("count_pending")
    async def count_pending(self) -> int:
        return await self.db.pending_qa_checks.count_documents({})

    # --- reads for the HTTP API -----------------------------------------------------------------

    async def product(self, product_id: str) -> dict | None:
        return await self.db.products.find_one({"_id": product_id}, {"rev": 0})

    async def violations(self, since: datetime | None, violation_type: str | None, limit: int) -> list[dict]:
        query: dict = {}
        if since is not None:
            query["detected_at"] = {"$gte": since}
        if violation_type is not None:
            query["type"] = violation_type
        cursor = self.db.qa_violations.find(query).sort("detected_at", DESCENDING).limit(limit)
        return [doc async for doc in cursor]


def _pending(doc: dict) -> PendingCheck:
    """A pending check from its document (older documents lack the newer, defaulted fields)."""
    return PendingCheck(**{key: value for key, value in doc.items() if key in _PENDING_FIELDS})


def line_seconds(doc: dict | None) -> float | None:
    """Seconds from a product's first to its last machine result (machine timestamps), or None."""
    times = [entry["timestamp"] for entry in (doc or {}).get("results", {}).values() if entry.get("timestamp")]
    if len(times) < 2:
        return None
    return max(0.0, (max(times) - min(times)).total_seconds())


def _latest(doc: dict | None) -> dict[str, str]:
    """The latest result per machine from a product document."""
    return {machine: entry["result"] for machine, entry in (doc or {}).get("results", {}).items()}
