"""Kafka producer shared by the live simulation and the replay, with retries while Kafka starts."""

import asyncio
import json
import logging

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

log = logging.getLogger(__name__)
MAX_BACKOFF_S = 30.0


async def start_producer(bootstrap_servers: str) -> AIOKafkaProducer:
    """Start a producer, retrying until the broker answers (it may still be starting)."""
    backoff = 1.0
    while True:
        producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            key_serializer=str.encode,
            value_serializer=lambda value: json.dumps(value).encode(),
        )
        try:
            await producer.start()
            log.info("connected to Kafka at %s", bootstrap_servers)
            return producer
        except KafkaError as e:
            await producer.stop()
            log.warning("Kafka not reachable (%s), retrying in %.0f s", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_S)


async def send(producer: AIOKafkaProducer, topic: str, key: str, value: dict) -> None:
    """Send one message and wait for the broker's ack, retrying if the topic isn't there yet."""
    backoff = 1.0
    while True:
        try:
            await producer.send_and_wait(topic, value=value, key=key)
            return
        except KafkaError as e:
            log.warning("send to %s failed (%s), retrying in %.0f s", topic, e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_S)
