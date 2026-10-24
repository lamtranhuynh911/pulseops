"""Prometheus metrics, served on /metrics for the Datadog Agent to scrape.

Labels are bounded (machine, result, type, name, topic, reason), never a product ID: every label
value is a separate Datadog custom metric.
"""

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, disable_created_metrics, generate_latest

disable_created_metrics()  # no *_created series; Datadog doesn't need them

TESTS = Counter("pulseops_tests", "Test results received", ["machine", "result"])
QA_VIOLATIONS = Counter("pulseops_qa_violations", "QA violations detected", ["machine", "type"])
MESSAGES_REJECTED = Counter("pulseops_messages_rejected", "Messages that failed validation", ["topic", "reason"])
MACHINE_UP = Gauge("pulseops_machine_up", "1 if the machine sent data within the integrity timeout", ["machine"])
MACHINE_SILENCE = Gauge("pulseops_machine_silence_seconds", "Seconds since the machine's last message", ["machine"])
MACHINE_MEASUREMENT = Gauge("pulseops_machine_measurement", "Latest value of each measurement", ["machine", "name"])
CONSUMER_PAUSED = Gauge("pulseops_consumer_paused", "1 while consumption is paused because MongoDB is unavailable")
EVENT_LATENCY = Histogram(
    "pulseops_event_latency_seconds",
    "Time from the machine's timestamp until the service has processed the message",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300),
)

__all__ = ["CONTENT_TYPE_LATEST", "generate_latest"]
