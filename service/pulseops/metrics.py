"""Prometheus metrics, served on /metrics for the Datadog Agent to scrape.

Labels are bounded (machine, result, type, name, topic, reason, status, zone, operation), never a
product ID: every label value is a separate Datadog custom metric. Every name listed here must also be
listed in the OpenMetrics annotation in k8s/base/service.yaml, or the Agent doesn't collect it.
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

# --- line and product quality -------------------------------------------------------------------
PRODUCT_STATUS_CHANGES = Counter(
    "pulseops_product_status_changes",
    "Products that reached a final status (PASSED, FAILED, NON_CONFORMING); a product counts again if its status changes",
    ["status"],
)
PRODUCT_LINE_TIME = Histogram(
    "pulseops_product_line_seconds",
    "For a product that PASSED: time from its first to its last machine result",
    buckets=(15, 30, 45, 60, 90, 120, 180, 300, 600),
)
MEASUREMENT_ZONE = Counter(
    "pulseops_measurement_zone",
    "Test measurements by spec zone: ok, marginal (inside spec, close to a limit) or out (outside spec)",
    ["machine", "name", "zone"],
)

# --- machines -----------------------------------------------------------------------------------
HEARTBEATS = Counter("pulseops_heartbeats", "Heartbeats received", ["machine"])
MACHINE_STATUS_CHANGES = Counter("pulseops_machine_status_changes", "Machine status changes (to UP or DOWN)",
                                 ["machine", "status"])

# --- pipeline -----------------------------------------------------------------------------------
PENDING_QA_CHECKS = Gauge("pulseops_pending_qa_checks", "QA checks waiting for an upstream result (shared state)")
MONGODB_OPERATION = Histogram(
    "pulseops_mongodb_operation_seconds",
    "Duration of the service's MongoDB operations, including failed ones",
    ["operation"],
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)

__all__ = ["CONTENT_TYPE_LATEST", "generate_latest"]
