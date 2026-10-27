"""Tests for the consumer's commit, hold, pause and positioning behaviour, with a fake Kafka client."""

import asyncio
from collections import namedtuple
from datetime import datetime, timezone

import pytest
from aiokafka import TopicPartition
from pymongo.errors import OperationFailure, ServerSelectionTimeoutError

from pulseops import consumer as consumer_module
from pulseops import metrics
from pulseops.concurrency import gather_all
from pulseops.config import Settings
from pulseops.consumer import Consumer, ServiceState, _Rebalance
from pulseops.models import Heartbeat, TestResult
from pulseops.storage import DuplicateVerdicts

Record = namedtuple("Record", "topic partition offset value timestamp")
A = TopicPartition("machine.test-results", 0)
B = TopicPartition("machine.heartbeats", 1)


@pytest.fixture(autouse=True)
def no_offset_lookup_delay(monkeypatch):
    monkeypatch.setattr(consumer_module, "OFFSET_LOOKUP_DELAY_S", 0)


class FakeKafka:
    def __init__(self):
        self.assigned = {A, B}
        self.paused, self.commits = set(), []
        self.committed_answers = {}  # committed offsets the broker reports, call by call (None = none)
        self.log_offsets = {}        # tp -> (first offset, end offset); a missing tp is (0, 10), not empty
        self.seeks, self.from_beginning = [], []

    def assignment(self):
        return frozenset(self.assigned)

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
    return [Record(tp.topic, tp.partition, offset, b"", 0) for offset in offsets]


async def until(condition, timeout_s=2.0):
    async with asyncio.timeout(timeout_s):
        while not condition():
            await asyncio.sleep(0.005)


async def resume_now(consumer):
    consumer._next_ping = 0.0  # skip the backoff wait
    await consumer._resume_if_mongodb_answers()


def test_each_partition_is_processed_in_order_and_committed_per_message():
    async def scenario():
        consumer = await make_consumer()
        handled = []

        async def handle(record):
            handled.append((record.partition, record.offset))

        consumer.handle = handle
        consumer.start_workers({A, B})
        consumer._dispatch({A: records(A, [10, 11]), B: records(B, [5])})
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
        consumer._dispatch({A: records(A, [10]), B: records(B, [5, 6])})
        await until(lambda: (B, 7) in consumer.kafka.commits)  # heartbeats got through
        slow.set()
        await until(lambda: (A, 11) in consumer.kafka.commits)
        await consumer.stop_workers([A, B])

    asyncio.run(scenario())


def test_records_without_a_worker_are_fetched_again_not_dropped():
    async def scenario():
        consumer = await make_consumer()
        consumer._dispatch({A: records(A, [7, 8])})
        return consumer.kafka

    assert asyncio.run(scenario()).seeks == [(A, 7)]


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
        consumer._dispatch({A: records(A, [10, 11, 12])})
        await until(lambda: consumer.state.paused)
        assert consumer.kafka.paused == {A, B}
        assert consumer.kafka.commits == [(A, 11)]   # never past the held message
        await asyncio.sleep(0.05)
        assert attempts == [10, 11]                    # held, not retried while paused

        storage.up = True
        await resume_now(consumer)
        await until(lambda: len(consumer.kafka.commits) == 3)
        await consumer.stop_workers([A])
        return consumer, attempts

    consumer, attempts = asyncio.run(scenario())
    assert attempts == [10, 11, 11, 12]
    assert consumer.kafka.commits == [(A, 11), (A, 12), (A, 13)]
    assert not consumer.state.paused and consumer.kafka.paused == set()


def test_mongodb_operation_failure_is_held_not_rejected(monkeypatch):
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
        consumer._dispatch({A: records(A, [10])})
        await until(lambda: consumer.state.paused)
        assert consumer.kafka.commits == []   # held, not skipped
        await resume_now(consumer)
        await until(lambda: consumer.kafka.commits == [(A, 11)])
        await consumer.stop_workers([A])

    asyncio.run(scenario())


def test_a_restarted_consumer_starts_paused_and_pings_until_mongodb_answers(monkeypatch):
    monkeypatch.setattr(consumer_module, "MAX_PING_BACKOFF_S", 0.01)

    async def scenario():
        storage = FakeStorage()
        storage.up = False
        pings = []
        ping = storage.ping

        async def counted_ping():
            pings.append(1)
            await ping()

        async def nothing():
            pass

        async def getmany(timeout_ms):
            await asyncio.sleep(0.005)
            return {}

        storage.ping = counted_ping
        consumer = await make_consumer(storage)   # the state a crashed run leaves: unpaused, prepared
        consumer.resumed.clear()
        consumer._ping_backoff = 0.01
        consumer._start_kafka, consumer.kafka.getmany, consumer.kafka.stop = nothing, getmany, nothing
        run = asyncio.create_task(consumer.run())   # what the supervisor in main.py does after a crash
        await until(lambda: len(pings) >= 3)         # keeps asking while MongoDB is down
        assert consumer.state.paused and not consumer.resumed.is_set()
        storage.up = True
        await until(lambda: not consumer.state.paused)
        assert consumer.resumed.is_set()
        run.cancel()
        await asyncio.gather(run, return_exceptions=True)

    asyncio.run(scenario())


def test_duplicate_verdicts_keep_the_replica_paused_until_they_are_removed():
    async def scenario():
        storage = FakeStorage()
        cleaned = [False]

        async def prepare(now):
            if not cleaned[0]:
                raise DuplicateVerdicts("qa_violations has several verdicts for one downstream result and hop")

        storage.prepare = prepare
        consumer = await make_consumer(storage)
        consumer.state.paused, consumer.state.prepared = True, False   # a new replica on an older database
        consumer.resumed.clear()
        metrics.CONSUMER_PAUSED.set(0)
        await resume_now(consumer)
        assert consumer.state.paused and not consumer.state.prepared
        assert metrics.CONSUMER_PAUSED._value.get() == 1    # visible in Datadog, not only in the log
        assert consumer._next_ping - asyncio.get_running_loop().time() > consumer_module.SETUP_RETRY_S - 1
        cleaned[0] = True                                    # removed by hand, as the README shows
        await resume_now(consumer)
        assert not consumer.state.paused and consumer.state.prepared and consumer.resumed.is_set()

    asyncio.run(scenario())


def test_ping_backoff_is_capped_and_only_reset_by_a_successful_write():
    async def scenario():
        consumer = await make_consumer()
        backoffs = []
        for _ in range(3):
            consumer.pause("write failed")   # e.g. writes fail while pings succeed
            backoffs.append(consumer._ping_backoff)
        consumer.write_succeeded()
        return backoffs, consumer._ping_backoff

    backoffs, after_success = asyncio.run(scenario())
    assert backoffs == [2.0, 2.0, 2.0]   # replicas resume within seconds of each other after an outage
    assert after_success == 1.0


def test_consumer_paused_metric_is_only_1_during_an_outage():
    async def scenario():
        storage = FakeStorage()
        consumer = await make_consumer(storage)
        consumer.state.paused = True               # a replica that is starting
        metrics.CONSUMER_PAUSED.set(0)
        await resume_now(consumer)
        after_start = metrics.CONSUMER_PAUSED._value.get()
        storage.up = False
        consumer.pause("write failed")
        during_outage = metrics.CONSUMER_PAUSED._value.get()
        return after_start, during_outage

    assert asyncio.run(scenario()) == (0, 1)


def test_rebalance_starts_and_stops_workers_and_repauses_while_paused():
    async def scenario():
        consumer = await make_consumer()
        consumer.state.paused = True
        listener = _Rebalance(consumer)
        async with asyncio.timeout(0.1):  # must not wait on the network (no group heartbeats meanwhile)
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
        consumer._dispatch({A: records(A, range(5))})
        consumer._throttle()
        assert A in consumer.kafka.paused
        gate.set()
        await until(lambda: len(consumer.workers[A].queue) <= 2)
        consumer._throttle()
        assert A not in consumer.kafka.paused
        await consumer.stop_workers([A])

    asyncio.run(scenario())


def test_offset_missing_while_the_broker_loads_is_asked_for_again():
    # aiokafka#1206: right after a broker restart, committed offsets can look missing for a moment.
    async def scenario():
        consumer = await make_consumer()
        consumer.kafka.committed_answers = {A: [None, None, 42]}
        consumer._position_later(A)
        assert A in consumer.kafka.paused         # only this partition waits
        await consumer.positioning[A]
        return consumer.kafka

    kafka = asyncio.run(scenario())
    assert kafka.seeks == [(A, 42)]               # resumed where the group left off, not replayed
    assert kafka.from_beginning == [] and A not in kafka.paused


def test_never_committed_partition_starts_from_the_beginning():
    async def scenario():
        consumer = await make_consumer()
        consumer._position_later(A)
        await consumer.positioning[A]
        return consumer.kafka

    kafka = asyncio.run(scenario())
    assert kafka.from_beginning == [A] and kafka.seeks == []


def test_empty_partition_starts_at_once_without_waiting(monkeypatch):
    monkeypatch.setattr(consumer_module, "OFFSET_LOOKUP_DELAY_S", 60)  # would time out if it waited

    async def scenario():
        consumer = await make_consumer()
        consumer.kafka.log_offsets = {B: (0, 0)}
        consumer._position_later(B)
        async with asyncio.timeout(1):
            await consumer.positioning[B]
        return consumer.kafka

    kafka = asyncio.run(scenario())
    assert kafka.from_beginning == [B] and kafka.seeks == []


def test_partition_revoked_while_positioning_is_left_alone():
    async def scenario():
        consumer = await make_consumer()
        consumer.kafka.committed_answers = {A: [None, 42]}
        consumer._position_later(A)
        consumer.kafka.assigned.discard(A)        # revoked while the lookups run
        await consumer.positioning[A]
        return consumer.kafka

    kafka = asyncio.run(scenario())
    assert kafka.seeks == [] and kafka.from_beginning == []


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
        assert (state.judge_after - now).total_seconds() >= 14
        probe.cancel()

    asyncio.run(scenario())


def test_a_test_result_is_counted_once_even_if_a_later_step_fails():
    message = TestResult(schema_version=1, event_id="00000000-0000-4000-8000-000000000001", machine_id="machine-3",
                         product_id="P-1", result="PASS", timestamp=datetime(2026, 10, 15, tzinfo=timezone.utc))

    class Storage:
        def __init__(self):
            self.stored = False

        async def touch_machine(self, machine_id, now):
            pass

        async def insert_test_result(self, result, now):
            first, self.stored = not self.stored, True
            return first

    class Quality:
        def __init__(self):
            self.calls = 0

        async def on_result(self, result, now):
            self.calls += 1
            if self.calls == 1:
                raise ServerSelectionTimeoutError("MongoDB lost during the QA check")

    async def scenario():
        consumer = await make_consumer(Storage())
        consumer.quality = Quality()
        counter = metrics.TESTS.labels("machine-3", "PASS")
        before = counter._value.get()
        with pytest.raises(ServerSelectionTimeoutError):
            await consumer._apply(message, consumer_module.utcnow())
        await consumer._apply(message, consumer_module.utcnow())   # the retry after the resume
        return counter._value.get() - before

    assert asyncio.run(scenario()) == 1


def test_heartbeats_are_counted_after_the_write_succeeds():
    heartbeat = Heartbeat(schema_version=1, machine_id="machine-2", timestamp=datetime(2026, 10, 15, tzinfo=timezone.utc))

    class Storage:
        def __init__(self):
            self.fail = True

        async def touch_machine(self, machine_id, now):
            if self.fail:
                self.fail = False
                raise ServerSelectionTimeoutError("MongoDB lost")

    async def scenario():
        consumer = await make_consumer(Storage())
        counter = metrics.HEARTBEATS.labels("machine-2")
        before = counter._value.get()
        with pytest.raises(ServerSelectionTimeoutError):
            await consumer._apply(heartbeat, consumer_module.utcnow())
        assert counter._value.get() == before                       # a failed write counts nothing
        await consumer._apply(heartbeat, consumer_module.utcnow())   # the retry after the resume
        return counter._value.get() - before

    assert asyncio.run(scenario()) == 1


def test_measurement_zones_are_counted_once_per_result_and_only_for_measurements_with_a_spec():
    def message(event_id, length_mm):
        return TestResult(schema_version=1, event_id=event_id, machine_id="machine-1", product_id="P-1", result="PASS",
                          timestamp=datetime(2026, 10, 15, tzinfo=timezone.utc),
                          measurements={"length_mm": length_mm, "temperature_c": 34.0})

    class Storage:
        def __init__(self):
            self.stored = set()

        async def touch_machine(self, machine_id, now):
            pass

        async def insert_test_result(self, result, now):
            first = result.event_id not in self.stored
            self.stored.add(result.event_id)
            return first

    class Quality:
        async def on_result(self, result, now):
            pass

    def zone_count(zone):
        return metrics.MEASUREMENT_ZONE.labels("machine-1", "length_mm", zone)._value.get()

    async def scenario():
        consumer = await make_consumer(Storage())
        consumer.quality = Quality()
        before = {zone: zone_count(zone) for zone in ("ok", "marginal", "out")}
        ok = message("00000000-0000-4000-8000-000000000001", 120.0)
        for m in (ok, ok,                                                   # the second is a redelivery
                  message("00000000-0000-4000-8000-000000000002", 120.13),
                  message("00000000-0000-4000-8000-000000000003", 120.2)):
            await consumer._apply(m, consumer_module.utcnow())
        return {zone: zone_count(zone) - before[zone] for zone in before}

    assert asyncio.run(scenario()) == {"ok": 1, "marginal": 1, "out": 1}
    # temperature_c has no spec, so it never gets a zone series.
    assert not any(sample.labels.get("name") == "temperature_c"
                   for family in metrics.MEASUREMENT_ZONE.collect() for sample in family.samples)


def test_gather_all_lets_every_write_finish_before_raising():
    finished = []

    async def fails():
        raise ServerSelectionTimeoutError("lost")

    async def slow_write():
        await asyncio.sleep(0.02)
        finished.append("write")

    async def scenario():
        with pytest.raises(ServerSelectionTimeoutError):
            await gather_all(fails(), slow_write())
        assert finished == ["write"]   # nothing from this attempt is still running

    asyncio.run(scenario())
