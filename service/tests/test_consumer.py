"""Tests for the consumer's commit, hold and pause behaviour, with a fake Kafka client."""

import asyncio
from collections import namedtuple

import pytest
from aiokafka import TopicPartition
from pymongo.errors import ServerSelectionTimeoutError

from pulseops import consumer as consumer_module
from pulseops.config import Settings
from pulseops.consumer import Consumer, ServiceState, _Rebalance

Record = namedtuple("Record", "topic partition offset value")
A = TopicPartition("machine.test-results", 0)
B = TopicPartition("machine.heartbeats", 1)


@pytest.fixture(autouse=True)
def no_offset_lookup_delay(monkeypatch):
    monkeypatch.setattr(consumer_module, "OFFSET_LOOKUP_DELAY_S", 0)


class FakeKafka:
    def __init__(self):
        self.paused, self.commits = set(), []
        self.committed_answers = {}  # committed offsets the broker reports, call by call (None = none)
        self.seeks, self.from_beginning = [], []
        self.log_offsets = {}  # tp -> (first offset, end offset); a missing tp is (0, 10), i.e. not empty

    def assignment(self):
        return frozenset({A, B})

    def pause(self, *tps):
        self.paused.update(tps)

    def resume(self, *tps):
        self.paused.difference_update(tps)

    async def commit(self, offsets):
        self.commits.extend(offsets.items())

    async def committed(self, tp):
        answers = self.committed_answers.get(tp, [])
        return answers.pop(0) if answers else None

    def seek(self, tp, offset):
        self.seeks.append((tp, offset))

    async def seek_to_beginning(self, *tps):
        self.from_beginning.extend(tps)

    async def beginning_offsets(self, tps):
        return {tp: self.log_offsets.get(tp, (0, 10))[0] for tp in tps}

    async def end_offsets(self, tps):
        return {tp: self.log_offsets.get(tp, (0, 10))[1] for tp in tps}


class FakeStorage:
    def __init__(self):
        self.up = True

    async def ping(self):
        if not self.up:
            raise ServerSelectionTimeoutError("down")

    async def prepare(self, now):
        pass


async def make_consumer(storage=None):
    # AIOKafkaConsumer must be created inside a running event loop (it never connects here).
    consumer = Consumer(Settings(mongodb_uri="mongodb://unused"), storage or FakeStorage(), quality=None,
                        state=ServiceState(paused=False, mongodb_ok=True, prepared=True))
    consumer.kafka = FakeKafka()
    consumer.resumed.set()
    return consumer


def records(tp, offsets):
    return [Record(tp.topic, tp.partition, offset, b"") for offset in offsets]


async def until(condition, timeout_s=2.0):
    async with asyncio.timeout(timeout_s):
        while not condition():
            await asyncio.sleep(0.005)


def test_each_partition_is_processed_in_order_and_committed_per_message():
    async def scenario():
        consumer = await make_consumer()
        handled = []

        async def handle(record):
            handled.append((record.partition, record.offset))

        consumer.handle = handle
        consumer.start_workers({A, B})
        consumer.workers[A].add(records(A, [10, 11]))
        consumer.workers[B].add(records(B, [5]))
        await until(lambda: len(consumer.kafka.commits) == 3)
        await consumer.stop_workers([A, B])
        return handled, consumer.kafka.commits

    handled, commits = asyncio.run(scenario())
    assert [h for h in handled if h[0] == 0] == [(0, 10), (0, 11)]
    assert sorted(commits) == sorted([(A, 11), (A, 12), (B, 6)])


def test_slow_partition_does_not_hold_up_another():
    async def scenario():
        consumer = await make_consumer()
        slow = asyncio.Event()

        async def handle(record):
            if record.topic == A.topic:
                await slow.wait()  # e.g. test-result writes to a distant MongoDB

        consumer.handle = handle
        consumer.start_workers({A, B})
        consumer.workers[A].add(records(A, [10]))
        consumer.workers[B].add(records(B, [5, 6]))
        await until(lambda: (B, 7) in consumer.kafka.commits)  # heartbeats got through
        slow.set()
        await until(lambda: (A, 11) in consumer.kafka.commits)
        await consumer.stop_workers([A, B])

    asyncio.run(scenario())


def test_mongodb_failure_holds_the_message_pauses_and_retries_after_resume():
    async def scenario():
        storage = FakeStorage()
        consumer = await make_consumer(storage)
        attempts = []

        async def handle(record):
            attempts.append(record.offset)
            if record.offset == 11 and not storage.up:
                raise ServerSelectionTimeoutError("MongoDB down")

        consumer.handle = handle
        storage.up = False
        consumer.start_workers({A})
        consumer.workers[A].add(records(A, [10, 11, 12]))
        await until(lambda: consumer.state.paused)
        assert consumer.kafka.paused == {A, B}
        assert consumer.kafka.commits == [(A, 11)]   # never past the held message
        await asyncio.sleep(0.05)
        assert attempts == [10, 11]                    # held, not retried while paused

        storage.up = True
        await consumer._resume_if_mongodb_answers()
        await until(lambda: len(consumer.kafka.commits) == 3)
        await consumer.stop_workers([A])
        return consumer, attempts

    consumer, attempts = asyncio.run(scenario())
    assert attempts == [10, 11, 11, 12]
    assert consumer.kafka.commits == [(A, 11), (A, 12), (A, 13)]
    assert not consumer.state.paused and consumer.kafka.paused == set()


def test_rebalance_starts_and_stops_workers_and_repauses_while_paused():
    async def scenario():
        consumer = await make_consumer()
        consumer.state.paused = True
        listener = _Rebalance(consumer)
        await listener.on_partitions_assigned({A})
        assert consumer.kafka.paused == {A} and set(consumer.workers) == {A}
        await listener.on_partitions_revoked({A})
        assert consumer.workers == {}

    asyncio.run(scenario())


def test_partition_far_behind_is_throttled_until_it_catches_up(monkeypatch):
    monkeypatch.setattr(consumer_module, "MAX_QUEUED", 4)

    async def scenario():
        consumer = await make_consumer()
        gate = asyncio.Event()

        async def handle(record):
            await gate.wait()

        consumer.handle = handle
        consumer.start_workers({A})
        consumer.workers[A].add(records(A, range(5)))
        consumer._throttle()
        assert A in consumer.kafka.paused
        gate.set()
        await until(lambda: len(consumer.workers[A].queue) <= 2)
        consumer._throttle()
        assert A not in consumer.kafka.paused
        await consumer.stop_workers([A])

    asyncio.run(scenario())


def test_mongodb_operation_failure_is_held_not_rejected(monkeypatch):
    from pymongo.errors import OperationFailure

    monkeypatch.setattr(consumer_module, "parse", lambda model, raw, line: object())

    async def scenario():
        consumer = await make_consumer()
        failures = iter([OperationFailure("Transaction numbers are only allowed on a replica set member")])

        async def apply(message, now):
            if error := next(failures, None):
                raise error

        async def reject(record, rejected, now):
            raise AssertionError(f"rejected: {rejected}")

        consumer._apply, consumer._reject = apply, reject
        consumer.start_workers({A})
        consumer.workers[A].add(records(A, [10]))
        await until(lambda: consumer.state.paused)
        assert consumer.kafka.commits == []   # held, not skipped
        await consumer._resume_if_mongodb_answers()
        await until(lambda: consumer.kafka.commits == [(A, 11)])
        await consumer.stop_workers([A])

    asyncio.run(scenario())


def test_offset_missing_while_the_broker_loads_is_asked_for_again():
    # aiokafka#1206: right after a broker restart, committed offsets can look missing for a moment.

    async def scenario():
        consumer = await make_consumer()
        kafka = consumer.kafka
        kafka.committed_answers = {A: [None, None, 42]}
        await consumer.position_partitions({A, B})
        return kafka

    kafka = asyncio.run(scenario())
    assert kafka.seeks == [(A, 42)]          # resumed where the group left off, not replayed
    assert kafka.from_beginning == [B]       # never committed: starts from the beginning


def test_workers_start_even_if_positioning_fails():
    async def scenario():
        consumer = await make_consumer()

        async def failing(partitions):
            raise RuntimeError("coordinator unavailable")

        consumer.position_partitions = failing
        try:
            await _Rebalance(consumer).on_partitions_assigned({A})
        except RuntimeError:
            pass
        assert set(consumer.workers) == {A}
        await consumer.stop_workers([A])

    asyncio.run(scenario())


def test_empty_partition_starts_at_once_without_waiting(monkeypatch):
    monkeypatch.setattr(consumer_module, "OFFSET_LOOKUP_DELAY_S", 60)  # would time out if it waited

    async def scenario():
        consumer = await make_consumer()
        consumer.kafka.log_offsets = {B: (0, 0)}
        async with asyncio.timeout(1):
            await consumer.position_partitions({B})
        return consumer.kafka

    kafka = asyncio.run(scenario())
    assert kafka.from_beginning == [B] and kafka.seeks == []


def test_kafka_outage_pauses_the_checks_and_delays_them_after_recovery(monkeypatch):
    from aiokafka.errors import KafkaConnectionError

    monkeypatch.setattr(consumer_module, "KAFKA_PROBE_INTERVAL_S", 0.01)

    async def scenario():
        consumer = await make_consumer()
        state = consumer.state
        broker_up = [True]

        async def end_offsets(tps):
            if not broker_up[0]:
                raise KafkaConnectionError("no broker")
            return {tp: 0 for tp in tps}

        consumer.kafka.end_offsets = end_offsets
        probe = asyncio.create_task(consumer._probe_kafka())
        await until(lambda: state.kafka_ok)
        broker_up[0] = False
        await until(lambda: not state.kafka_ok)
        now = consumer_module.utcnow()
        assert not state.can_judge(now)
        broker_up[0] = True
        await until(lambda: state.kafka_ok)
        assert not state.can_judge(consumer_module.utcnow())  # waits a full integrity timeout first
        assert state.judge_after >= now + consumer_module.timedelta(seconds=14)
        probe.cancel()

    asyncio.run(scenario())
