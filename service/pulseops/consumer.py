"""Kafka consumer: validates each message, runs the checks, writes to MongoDB, then commits the offset.

Delivery is at-least-once: an offset is committed only after its MongoDB writes succeeded, and every
write is idempotent, so a redelivered message changes nothing.

Each assigned partition has its own worker that processes its records in order and commits after
each one; one fetch loop hands the records to the workers. Order only matters within a partition
(all of a product's results are on one), so partitions never wait for each other: heartbeats are
processed promptly even when test-result writes are slow, for example with a distant MongoDB.

A MongoDB outage pauses the consumer instead of crashing it: the worker whose write failed holds
that message, all assigned partitions are paused, and nothing past the held message is committed.
The fetch loop keeps calling getmany(), so the group doesn't kick this replica out
(max_poll_interval_ms), and pings MongoDB with backoff. Once MongoDB answers, every worker retries
its held message and the partitions resume.
"""

import asyncio
import logging
import socket
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener, TopicPartition
from aiokafka.errors import CommitFailedError, KafkaError, NoOffsetForPartitionError, OffsetOutOfRangeError
from pymongo.errors import ConnectionFailure, PyMongoError

from . import metrics
from .checks.quality import QualityCheck
from .config import Settings
from .models import Heartbeat, Rejected, TestResult, parse
from .storage import Storage

log = logging.getLogger(__name__)
MAX_BACKOFF_S = 30.0
# A partition whose worker has this many records waiting is paused in Kafka until half have been
# processed, so a slow MongoDB doesn't make records pile up in memory.
MAX_QUEUED = 500
# How often to ask for a partition's committed offset before concluding it has none (see
# Consumer.position_partitions).
OFFSET_LOOKUP_ATTEMPTS = 5
OFFSET_LOOKUP_DELAY_S = 2.0
# How often to check that the broker answers, and how long to wait for it.
KAFKA_PROBE_INTERVAL_S = 5.0
KAFKA_PROBE_TIMEOUT_S = 3.0


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ServiceState:
    """Shared by the consumer, the check timers and /health."""

    kafka_running: bool = False
    kafka_ok: bool = False              # the broker answered the last probe
    mongodb_ok: bool = False
    prepared: bool = False              # indexes and machine documents created
    paused: bool = True                 # until MongoDB has answered once
    judge_after: datetime = field(default_factory=utcnow)  # the check timers wait until then

    def can_judge(self, now: datetime) -> bool:
        return self.prepared and self.kafka_ok and not self.paused and now >= self.judge_after


class Consumer:
    def __init__(self, settings: Settings, storage: Storage, quality: QualityCheck, state: ServiceState):
        self.settings = settings
        self.storage = storage
        self.quality = quality
        self.state = state
        self.models = {settings.test_results_topic: TestResult, settings.heartbeats_topic: Heartbeat}
        self.kafka = AIOKafkaConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_group_id,
            client_id=socket.gethostname(),   # the pod name, shown by kafka-consumer-groups.sh
            enable_auto_commit=False,
            # Never reset a partition silently: the consumer positions partitions itself
            # (position_partitions), and this makes aiokafka raise instead of resetting.
            auto_offset_reset="none",
        )
        self.kafka.subscribe(list(self.models), listener=_Rebalance(self))
        self.workers: dict[TopicPartition, PartitionWorker] = {}
        self.resumed = asyncio.Event()      # set while consuming, cleared while paused
        self.throttled: set[TopicPartition] = set()
        self._next_ping = 0.0
        self._ping_backoff = 1.0

    async def run(self) -> None:
        metrics.CONSUMER_PAUSED.set(1)  # paused until MongoDB has answered once
        await self._start_kafka()
        probe = asyncio.create_task(self._probe_kafka(), name="kafka probe")
        try:
            await self._resume_if_mongodb_answers()  # a replica that starts with MongoDB down starts paused
            while True:
                try:
                    batch = await self.kafka.getmany(timeout_ms=500)
                except NoOffsetForPartitionError as e:
                    # Safety net, in case a partition was left without a position.
                    await self.position_partitions([tp for tp in e.args if isinstance(tp, TopicPartition)])
                    continue
                except OffsetOutOfRangeError as e:
                    # The committed offset is older than the topic's retention: the data before it is gone.
                    partitions = list(e.args[0]) if e.args and isinstance(e.args[0], dict) else []
                    log.warning("committed offset no longer in the topic for %s: starting from the oldest record",
                                _names(partitions))
                    await self.kafka.seek_to_beginning(*partitions)
                    continue
                for tp, records in batch.items():
                    if worker := self.workers.get(tp):
                        worker.add(records)
                self._throttle()
                if self.state.paused:
                    await self._resume_if_mongodb_answers()
        finally:
            probe.cancel()
            await self.stop_workers(list(self.workers))
            self.state.kafka_running = False
            self.state.kafka_ok = False
            await self.kafka.stop()  # leaves the group, so the other replicas take over at once

    async def _start_kafka(self) -> None:
        backoff = 1.0
        while True:
            try:
                await self.kafka.start()
                self.state.kafka_running = True
                log.info("consuming %s as group %s", ", ".join(self.models), self.settings.kafka_group_id)
                return
            except KafkaError as e:
                log.warning("Kafka not reachable (%s), retrying in %.0f s", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_S)

    async def _probe_kafka(self) -> None:
        """Check that the broker answers. While it doesn't, no machine data can arrive, so the check
        timers stop (a Kafka outage must not look like every machine going silent), /health is 503,
        and after the broker is back the timers wait a full integrity timeout, as after a MongoDB
        outage, so the backlog can be processed first."""
        probe_partition = TopicPartition(self.settings.heartbeats_topic, 0)
        while True:
            try:
                await asyncio.wait_for(self.kafka.end_offsets([probe_partition]), KAFKA_PROBE_TIMEOUT_S)
                answered = True
            except (KafkaError, TimeoutError) as e:
                answered, error = False, e
            if answered and not self.state.kafka_ok:
                self.state.judge_after = max(self.state.judge_after,
                                             utcnow() + timedelta(seconds=self.settings.integrity_timeout_s))
                log.info("Kafka broker answers")
            elif not answered and self.state.kafka_ok:
                log.warning("Kafka broker unreachable (%r): checks paused", error)
            self.state.kafka_ok = answered
            await asyncio.sleep(KAFKA_PROBE_INTERVAL_S)

    async def position_partitions(self, partitions) -> None:
        """Start each partition at its committed offset, or at the beginning if the group has none.

        aiokafka 0.13 and 0.14 read committed offsets as missing while a restarted broker is still
        loading them (aiokafka#1206); with auto_offset_reset="earliest" that would replay the whole
        topic, which takes hours against a distant MongoDB. So a missing offset is asked for again a
        few times before the partition is started from the beginning, which is only right for a
        partition this group has never committed.
        """
        missing = set(partitions)
        if missing:
            # An empty partition has nothing to replay: start it right away instead of waiting.
            first, end = await asyncio.gather(self.kafka.beginning_offsets(missing), self.kafka.end_offsets(missing))
            empty = {tp for tp in missing if first.get(tp) == end.get(tp)}
            if empty:
                await self.kafka.seek_to_beginning(*empty)
                missing -= empty
        for attempt in range(OFFSET_LOOKUP_ATTEMPTS):
            for tp in sorted(missing):
                offset = await self.kafka.committed(tp)
                if offset is not None:
                    self.kafka.seek(tp, offset)
                    missing.discard(tp)
            if not missing:
                return
            if attempt < OFFSET_LOOKUP_ATTEMPTS - 1:
                await asyncio.sleep(OFFSET_LOOKUP_DELAY_S)
        log.info("no committed offset for %s: starting from the beginning", _names(missing))
        await self.kafka.seek_to_beginning(*missing)

    # --- partition workers ----------------------------------------------------------------------

    def start_workers(self, partitions) -> None:
        for tp in partitions:
            if tp not in self.workers:
                self.workers[tp] = PartitionWorker(self, tp)

    async def stop_workers(self, partitions) -> None:
        """Stop the workers before their partitions go to another replica, so they commit nothing more.
        A message interrupted halfway is processed again by the new owner, which is harmless."""
        workers = [self.workers.pop(tp) for tp in partitions if tp in self.workers]
        for worker in workers:
            worker.task.cancel()
        await asyncio.gather(*(worker.task for worker in workers), return_exceptions=True)
        self.throttled.difference_update(partitions)

    def _throttle(self) -> None:
        """Pause partitions whose worker is far behind, and resume them once it has caught up."""
        for tp, worker in self.workers.items():
            if tp not in self.throttled and len(worker.queue) >= MAX_QUEUED:
                self.throttled.add(tp)
                self.kafka.pause(tp)
            elif tp in self.throttled and len(worker.queue) <= MAX_QUEUED // 2:
                self.throttled.discard(tp)
                if not self.state.paused:
                    self.kafka.resume(tp)

    # --- processing one message -----------------------------------------------------------------

    async def handle(self, record) -> None:
        """Process one message. Raises PyMongoError if a MongoDB operation failed."""
        now = utcnow()
        try:
            message = parse(self.models[record.topic], record.value, self.settings.line_sequence)
            await self._apply(message, now)
        except Rejected as rejected:
            await self._reject(record, rejected, now)
        except PyMongoError:
            raise  # a storage failure, not a bad message: the worker holds it and retries
        except Exception as e:  # a bug or unexpected data: record it rather than block the partition
            log.exception("failed to process %s[%d]@%d", record.topic, record.partition, record.offset)
            await self._reject(record, Rejected("processing_error", repr(e)), now)

    async def _apply(self, message: TestResult | Heartbeat, now: datetime) -> None:
        touch = self.storage.touch_machine(message.machine_id, now)  # any message is a sign of life
        if isinstance(message, Heartbeat):
            await touch
        else:
            # Independent writes run concurrently, to save round trips. Even for a duplicate the QA
            # check runs again: a crash between the insert and the check must not lose the check, and
            # every step in it is idempotent.
            _, first_time, _ = await asyncio.gather(
                touch, self.storage.insert_test_result(message, now), self.quality.on_result(message, now)
            )
            if not first_time:
                return  # counted the first time; counters must count each message once
            metrics.TESTS.labels(message.machine_id, message.result).inc()
            for name, value in message.measurements.items():
                metrics.MACHINE_MEASUREMENT.labels(message.machine_id, name).set(value)
        metrics.EVENT_LATENCY.observe(max(0.0, (now - message.timestamp).total_seconds()))

    async def _reject(self, record, rejected: Rejected, now: datetime) -> None:
        if await self.storage.insert_rejected(record.topic, record.partition, record.offset,
                                              rejected.reason, rejected.detail, record.value, now):
            metrics.MESSAGES_REJECTED.labels(record.topic, rejected.reason).inc()
            log.warning("rejected %s[%d]@%d: %s", record.topic, record.partition, record.offset, rejected)

    async def commit(self, tp: TopicPartition, next_offset: int) -> None:
        try:
            await self.kafka.commit({tp: next_offset})
        except (CommitFailedError, KafkaError) as e:
            # Typically a rebalance took the partition away; its new owner re-reads from the last
            # committed offset, which is harmless because every write is idempotent.
            log.warning("commit failed (%s); those messages may be processed again", e)

    # --- pausing while MongoDB is unavailable ---------------------------------------------------

    def pause(self, reason: str) -> None:
        if not self.state.paused:
            log.error("pausing consumption: %s", reason)
        self.state.paused = True
        self.state.mongodb_ok = False
        self.resumed.clear()
        metrics.CONSUMER_PAUSED.set(1)
        self.kafka.pause(*self.kafka.assignment())
        self._next_ping = 0.0
        self._ping_backoff = 1.0

    async def _resume_if_mongodb_answers(self) -> None:
        """While paused: ping MongoDB (with backoff) and resume once it answers."""
        loop = asyncio.get_running_loop()
        if loop.time() < self._next_ping:
            return
        try:
            await self.storage.ping()
            if not self.state.prepared:
                await self.storage.prepare(utcnow())
                self.state.prepared = True
        except ConnectionFailure as e:
            log.warning("MongoDB unavailable (%s), next check in %.0f s", e, self._ping_backoff)
            self.state.mongodb_ok = False
            self._next_ping = loop.time() + self._ping_backoff
            self._ping_backoff = min(self._ping_backoff * 2, MAX_BACKOFF_S)
            return
        self.state.mongodb_ok = True
        self.state.paused = False
        metrics.CONSUMER_PAUSED.set(0)
        self.kafka.resume(*(self.kafka.assignment() - self.throttled))
        self._ping_backoff = 1.0
        # The backlog hasn't been processed yet: give machines a full timeout before judging them.
        self.state.judge_after = utcnow() + timedelta(seconds=self.settings.integrity_timeout_s)
        self.resumed.set()
        log.info("MongoDB available: consuming")


class PartitionWorker:
    """Processes one partition's records in order, committing after each one."""

    def __init__(self, consumer: Consumer, tp: TopicPartition):
        self.consumer = consumer
        self.tp = tp
        self.queue: deque = deque()
        self._wake = asyncio.Event()
        self.task = asyncio.create_task(self._run(), name=f"worker {tp.topic}[{tp.partition}]")

    def add(self, records: list) -> None:
        self.queue.extend(records)
        self._wake.set()

    async def _run(self) -> None:
        while True:
            if not self.queue:
                self._wake.clear()
                await self._wake.wait()
                continue
            await self.consumer.resumed.wait()
            record = self.queue[0]
            try:
                await self.consumer.handle(record)
            except PyMongoError as e:
                # Hold the message: it stays first in the queue and is retried after the resume.
                self.consumer.pause(f"MongoDB operation failed: {e}")
                continue
            self.queue.popleft()
            await self.consumer.commit(self.tp, record.offset + 1)


class _Rebalance(ConsumerRebalanceListener):
    """Starts and stops partition workers, and keeps new partitions paused while MongoDB is down."""

    def __init__(self, consumer: Consumer):
        self.consumer = consumer

    async def on_partitions_revoked(self, revoked) -> None:
        await self.consumer.stop_workers(revoked)

    async def on_partitions_assigned(self, assigned) -> None:
        # Partitions assigned in a rebalance start unpaused: pause them again while MongoDB is down.
        if self.consumer.state.paused and assigned:
            self.consumer.kafka.pause(*assigned)
        try:
            await self.consumer.position_partitions(assigned)
        finally:
            # Even if positioning failed (the fetch loop's safety net retries it), every assigned
            # partition needs a worker, or its records would be fetched and never processed.
            self.consumer.start_workers(assigned)


def _names(partitions) -> str:
    return ", ".join(f"{tp.topic}[{tp.partition}]" for tp in sorted(partitions)) or "(none)"
