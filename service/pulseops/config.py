"""Settings read from environment variables: Kafka, MongoDB, LINE_SEQUENCE, and the check timeouts."""

import os
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    mongodb_uri: str
    mongodb_database: str = "pulseops"
    kafka_bootstrap_servers: str = "kafka-0.kafka.pulseops.svc.cluster.local:9092"
    kafka_group_id: str = "pulseops-service"
    test_results_topic: str = "machine.test-results"
    heartbeats_topic: str = "machine.heartbeats"
    # The machines the integrity check expects, in line order; also each machine's predecessor for QA.
    line_sequence: tuple[str, ...] = ("machine-1", "machine-2", "machine-3")
    integrity_timeout_s: float = 15.0   # three missed 5 s heartbeats
    integrity_interval_s: float = 1.0
    qa_grace_s: float = 60.0            # how long to wait for an out-of-order upstream result
    qa_interval_s: float = 5.0

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> "Settings":
        defaults = cls(mongodb_uri="")
        line = env.get("LINE_SEQUENCE")
        uri = env.get("MONGODB_URI")
        if not uri:
            raise SystemExit("MONGODB_URI is not set")
        return cls(
            mongodb_uri=uri,
            mongodb_database=env.get("MONGODB_DATABASE", defaults.mongodb_database),
            kafka_bootstrap_servers=env.get("KAFKA_BOOTSTRAP_SERVERS", defaults.kafka_bootstrap_servers),
            kafka_group_id=env.get("KAFKA_GROUP_ID", defaults.kafka_group_id),
            test_results_topic=env.get("TEST_RESULTS_TOPIC", defaults.test_results_topic),
            heartbeats_topic=env.get("HEARTBEATS_TOPIC", defaults.heartbeats_topic),
            line_sequence=tuple(m.strip() for m in line.split(",") if m.strip()) if line else defaults.line_sequence,
            integrity_timeout_s=float(env.get("INTEGRITY_TIMEOUT_SECONDS", defaults.integrity_timeout_s)),
            integrity_interval_s=float(env.get("INTEGRITY_CHECK_INTERVAL_SECONDS", defaults.integrity_interval_s)),
            qa_grace_s=float(env.get("QA_GRACE_SECONDS", defaults.qa_grace_s)),
            qa_interval_s=float(env.get("QA_CHECK_INTERVAL_SECONDS", defaults.qa_interval_s)),
        )
