"""Kafka consumer: validates each message, runs the checks, writes to MongoDB, then commits the offset.

Delivery is at-least-once: an offset is committed only after its MongoDB writes succeeded, and every
write is idempotent, so a redelivered message changes nothing.

Each assigned partition has its own worker that processes its records in order and commits after
each one; one fetch loop hands the records to the workers. Order only matters within a partition
(all of a product's results are on one), so partitions never wait for each other: heartbeats are
processed promptly even when test-result writes are slow, for example with a distant MongoDB. The
workers of newly assigned partitions are started in the rebalance listener, before any of their
records can be fetched, so no record is ever fetched without a worker to take it.

A MongoDB outage pauses the consumer instead of crashing it: the worker whose write failed holds
that message, all assigned partitions are paused, and nothing past the held message is committed.
The fetch loop keeps calling getmany(), so the group doesn't kick this replica out
(max_poll_interval_ms), and pings MongoDB with a short backoff. Once MongoDB answers, every worker
retries its held message and the partitions resume.
"""

import asyncio
import logging
import socket
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener, TopicPartition
from aiokafka.errors import CommitFailedError, KafkaError, NoOffsetForPartitionError, OffsetOutOfRangeError
from pymongo.errors import PyMongoError

from . import metrics, specs
from .checks.quality import QualityCheck
from .concurrency import gather_all
from .config import Settings
from .models import Heartbeat, Rejected, TestResult, parse
from .storage import DuplicateVerdicts, Storage

log = logging.getLogger(__name__)
# Kafka reconnects: backoff between attempts.
MAX_BACKOFF_S = 30.0
# MongoDB pings while paused: kept short, so that after an outage all replicas resume within a few
# seconds of each other (a failed ping itself takes up to 5 s), well inside the
# INTEGRITY_TIMEOUT_SECONDS that the check timers wait.
MAX_PING_BACKOFF_S = 2.0
# When MongoDB answers but the setup can't finish (the data needs a cleanup by hand): retry slowly, as
# each attempt scans qa_violations.
SETUP_RETRY_S = 60.0
# A partition whose worker has this many records waiting is paused in Kafka until half have been
# processed, so a slow MongoDB doesn't make records pile up in memory. A fetch adds at most
# MAX_PARTITION_FETCH_BYTES (about 200 messages) per partition on top.
MAX_QUEUED = 500
MAX_PARTITION_FETCH_BYTES = 64 * 1024
# How often to ask for a partition's committed offset before concluding it has none (see
# Consumer._position).
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
            # Never reset a partition silently: when aiokafka finds no committed offset, it raises,
            # and _position decides (see there for why).
            auto_offset_reset="none",
            max_partition_fetch_bytes=MAX_PARTITION_FETCH_BYTES,
        )
        self.kafka.subscribe(list(self.models), listener=_Rebalance(self))
        self.workers: dict[TopicPartition, PartitionWorker] = {}
        self.resumed = asyncio.Event()      # set while consuming, cleared while paused
        self.throttled: set[TopicPartition] = set()
        self.positioning: dict[TopicPartition, asyncio.Task] = {}
        self._next_ping = 0.0
        self._ping_backoff = 1.0

    async def run(self) -> None:
        # Every run starts paused, also a restart by the supervisor that shares a ServiceState left
        # unpaused: the workers wait for self.resumed, which only a MongoDB answer sets, and the
        # listener (it runs inside the Kafka start) pauses the partitions it is given.
        self.state.paused = True
        await self._start_kafka()
        probe = asyncio.create_task(self._probe_kafka(), name="kafka probe")
        try:
            await self._resume_if_mongodb_answers()  # a replica that starts with MongoDB down starts paused
            while True:
                try:
                    batch = await self.kafka.getmany(timeout_ms=500)
                except NoOffsetForPartitionError as e:
                    for tp in e.args:
                        if isinstance(tp, TopicPartition):
                            self._position_later(tp)
                    continue
                except OffsetOutOfRangeError as e:
                    await self._restart_expired(e)
                    continue
                except KafkaError as e:
                    # aiokafka hands coordination errors to getmany(); it expects to be called again.
                    log.warning("Kafka error while fetching (%r); retrying", e)
                    await asyncio.sleep(1)
                    continue
                self._dispatch(batch)
                self._throttle()
                if self.state.paused:
                    await self._resume_if_mongodb_answers()
        finally:
            probe.cancel()
            await self.stop_workers(list(self.workers))
            self.state.kafka_running = False
            self.state.kafka_ok = False
            await self.kafka.stop()  # leaves the group, so the other replicas take over at once

    def _dispatch(self, batch: dict[TopicPartition, list]) -> None:
        for tp, records in batch.items():
            if worker := self.workers.get(tp):
                worker.add(records)
            elif tp in self.kafka.assignment():
                # Should not happen (workers start before fetching), but never drop records.
                log.warning("no worker for %s: fetching its records again", _names([tp]))
                self.kafka.seek(tp, records[0].offset)

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

    # --- positioning partitions -----------------------------------------------------------------

    def _position_later(self, tp: TopicPartition) -> None:
        """Position a partition aiokafka found no committed offset for, without holding up the others."""
        if tp in self.positioning or tp not in self.kafka.assignment():
            return
        self.kafka.pause(tp)
        self.positioning[tp] = asyncio.create_task(self._position(tp), name=f"position {_names([tp])}")

    async def _position(self, tp: TopicPartition) -> None:
        """Start a partition at its committed offset, or at the beginning if the group has none.

        aiokafka 0.13 and 0.14 read committed offsets as missing while a restarted broker is still
        loading them (aiokafka#1206); with auto_offset_reset="earliest" that would replay the whole
        topic, which takes hours against a distant MongoDB. So a missing offset is asked for again a
        few times before the partition is started from the beginning, which is only right for a
        partition this group has never committed. An empty partition has nothing to replay and starts
        at once.
        """
        try:
            first, end = await asyncio.gather(self.kafka.beginning_offsets([tp]), self.kafka.end_offsets([tp]))
            offset = None
            if first.get(tp) != end.get(tp):
                for attempt in range(OFFSET_LOOKUP_ATTEMPTS):
                    offset = await self.kafka.committed(tp)
                    if offset is not None or attempt == OFFSET_LOOKUP_ATTEMPTS - 1:
                        break
                    await asyncio.sleep(OFFSET_LOOKUP_DELAY_S)
            if tp not in self.kafka.assignment():
                return  # revoked meanwhile: its new owner positions it
            if offset is None:
                log.info("no committed offset for %s: starting from the beginning", _names([tp]))
                await self.kafka.seek_to_beginning(tp)
            else:
                self.kafka.seek(tp, offset)
        except KafkaError as e:
            log.warning("could not position %s (%r); retrying when it is reported again", _names([tp]), e)
        finally:
            self.positioning.pop(tp, None)
            if tp in self.kafka.assignment() and not self.state.paused and tp not in self.throttled:
                self.kafka.resume(tp)

    async def _restart_expired(self, error: OffsetOutOfRangeError) -> None:
        """The committed offset is older than the topic's retention: the data before it is gone."""
        out_of_range = error.args[0] if error.args and isinstance(error.args[0], dict) else {}
        partitions = [tp for tp in out_of_range if tp in self.kafka.assignment()]
        log.warning("committed offset no longer in the topic for %s: starting from the oldest record",
                    _names(partitions))
        try:
            await self.kafka.seek_to_beginning(*partitions)
        except KafkaError as e:
            log.warning("could not move %s to the oldest record (%r)", _names(partitions), e)

    # --- partition workers ----------------------------------------------------------------------

    def start_workers(self, partitions) -> None:
        for tp in partitions:
            if tp not in self.workers:
                self.workers[tp] = PartitionWorker(self, tp)

    async def stop_workers(self, partitions) -> None:
        """Stop the workers before their partitions go to another replica, so they commit nothing more.
        A message interrupted halfway is processed again by the new owner, which is harmless."""
        tasks = [self.workers.pop(tp).task for tp in partitions if tp in self.workers]
        tasks += [self.positioning.pop(tp) for tp in partitions if tp in self.positioning]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.throttled.difference_update(partitions)

    def _throttle(self) -> None:
        """Pause partitions whose worker is far behind, and resume them once it has caught up."""
        for tp, worker in self.workers.items():
            if tp not in self.throttled and len(worker.queue) >= MAX_QUEUED:
                self.throttled.add(tp)
                self.kafka.pause(tp)
            elif tp in self.throttled and len(worker.queue) <= MAX_QUEUED // 2:
                self.throttled.discard(tp)
                if not self.state.paused and tp not in self.positioning:
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
            _observe_latency(message)
            return
        # Independent writes run concurrently, to save round trips; gather_all lets all of them finish
        # before a failure is raised. Even for a duplicate the QA check runs again: a crash between the
        # insert and the check must not lose the check, and every step in it is idempotent.
        _, first_time, _ = await gather_all(touch, self._store_result(message, now), self.quality.on_result(message, now))
        if first_time:
            _observe_latency(message)

    async def _store_result(self, message: TestResult, now: datetime) -> bool:
        first_time = await self.storage.insert_test_result(message, now)
        if first_time:
            # Counted right after the first insert, whatever happens to the other steps: a retry finds
            # the result stored and must not count it again.
            metrics.TESTS.labels(message.machine_id, message.result).inc()
            for name, value in message.measurements.items():
                metrics.MACHINE_MEASUREMENT.labels(message.machine_id, name).set(value)
        return first_time

    async def _reject(self, record, rejected: Rejected, now: datetime) -> None:
        if await self.storage.insert_rejected(record.topic, record.partition, record.offset, record.timestamp,
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
        # Don't reset the backoff here: if writes keep failing while pings succeed, each pause waits
        # longer before the next attempt instead of flapping every second.
        self._next_ping = asyncio.get_running_loop().time() + self._ping_backoff
        self._ping_backoff = min(self._ping_backoff * 2, MAX_PING_BACKOFF_S)

    def write_succeeded(self) -> None:
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
        except DuplicateVerdicts as e:
            # MongoDB answers, but its data needs a one-off cleanup: stay paused (and say so) until then.
            log.error("%s; consumption stays paused, next attempt in %.0f s", e, SETUP_RETRY_S)
            metrics.CONSUMER_PAUSED.set(1)
            self._next_ping = loop.time() + SETUP_RETRY_S
            return
        except PyMongoError as e:  # also e.g. ConfigurationError when a mongodb+srv:// lookup fails
            log.warning("MongoDB unavailable (%s), next check in %.0f s", e, self._ping_backoff)
            self.state.mongodb_ok = False
            metrics.CONSUMER_PAUSED.set(1)
            self._next_ping = loop.time() + self._ping_backoff
            self._ping_backoff = min(self._ping_backoff * 2, MAX_PING_BACKOFF_S)
            return
        self.state.mongodb_ok = True
        self.state.paused = False
        metrics.CONSUMER_PAUSED.set(0)
        self.kafka.resume(*(self.kafka.assignment() - self.throttled - set(self.positioning)))
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
        self.task = asyncio.create_task(self._run(), name=f"worker {_names([tp])}")

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
            self.consumer.write_succeeded()
            self.queue.popleft()
            await self.consumer.commit(self.tp, record.offset + 1)


class _Rebalance(ConsumerRebalanceListener):
    """Starts and stops partition workers, and keeps new partitions paused while MongoDB is down.

    Nothing here waits on the network: aiokafka doesn't send group heartbeats while the listener
    runs, so a slow listener would get this replica expelled from the group.
    """

    def __init__(self, consumer: Consumer):
        self.consumer = consumer

    async def on_partitions_revoked(self, revoked) -> None:
        await self.consumer.stop_workers(revoked)

    async def on_partitions_assigned(self, assigned) -> None:
        # Partitions assigned in a rebalance start unpaused: pause them again while MongoDB is down.
        if self.consumer.state.paused and assigned:
            self.consumer.kafka.pause(*assigned)
        self.consumer.start_workers(assigned)


def _observe_latency(message: TestResult | Heartbeat) -> None:
    """Time from the machine's timestamp until the message has been processed."""
    metrics.EVENT_LATENCY.observe(max(0.0, (utcnow() - message.timestamp).total_seconds()))


def _names(partitions) -> str:
    return ", ".join(f"{tp.topic}[{tp.partition}]" for tp in sorted(partitions)) or "(none)"
