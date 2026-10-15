# PulseOps

Real-time data integrity and quality assurance monitoring for a production line: **Kafka → Python → MongoDB**, observed with **Datadog**.

> **Status:** proof of concept, in early development. This README describes the target design and is ahead of the code; names, schemas, and defaults may change while the PoC is built.

Machines 1, 2, and 3 on a production line publish their test results to Kafka. The PulseOps service, written in Python, consumes them and:

1. **Checks data integrity:** is every machine still sending data?
2. **Checks production quality:** did each product pass the test at the previous machine before it arrived at the next one?
3. **Stores** every test result, each product's progress through the line, and every violation in MongoDB for later querying.
4. **Serves an HTTP endpoint for Datadog**, which scrapes it to visualize the machine data and alert on problems.

Datadog also monitors the health of the pipeline itself: the service, the machines, and MongoDB.

## PoC goals

The PoC is done when it shows, end to end, that:

- A machine that stops sending data is flagged within about 15 seconds, and Datadog alerts on it.
- A product that skips a machine, or keeps moving after failing a test, is caught and recorded as a QA violation.
- Every test result is stored in MongoDB and can be queried by product, machine, or time range.
- One Datadog dashboard shows the line (machine status, throughput, pass rate, violations) next to the health of the service, the Kafka consumer, and MongoDB.

Production hardening (security, high availability, connecting real machines) is out of scope; see [Beyond the PoC](#beyond-the-poc).

## Architecture

```
               product             product
  ┌───────────┐       ┌───────────┐       ┌───────────┐
  │ Machine 1 │──────>│ Machine 2 │──────>│ Machine 3 │
  └─────┬─────┘       └─────┬─────┘       └─────┬─────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                            │  test results + heartbeats (JSON)
                            v
             ┌─────────────────────────────┐
             │            Kafka            │
             │  machine.test-results       │
             │  machine.heartbeats         │
             └──────────────┬──────────────┘
                            │  consume
                            v
             ┌─────────────────────────────┐   write    ┌───────────┐
             │      PulseOps service       │───────────>│  MongoDB  │
             │  - data integrity check     │            └─────┬─────┘
             │  - quality (QA) check       │                  │
             │  - HTTP API                 │                  │
             └──────────────┬──────────────┘                  │
                            │  /metrics, /health              │
                            v                                 │
             ┌─────────────────────────────┐                  │
             │        Datadog Agent        │<─────────────────┘
             │  also: Kafka consumer lag   │  MongoDB integration
             └──────────────┬──────────────┘
                            v
             ┌─────────────────────────────┐
             │           Datadog           │
             │    dashboards + monitors    │
             └─────────────────────────────┘
```

1. Each machine publishes a message to `machine.test-results` every time it tests a product, and a heartbeat to `machine.heartbeats` every 5 seconds. Heartbeats are sent even when a machine is idle, so "no products right now" is never mistaken for "machine offline".
2. The service consumes both topics as the consumer group `pulseops-service` and validates every message against its schema. Invalid messages are stored in `rejected_messages` instead of being dropped silently.
3. Every valid message updates its machine's last-seen time for the integrity check. Test results also go through the QA check.
4. Test results, product state, and violations are written to MongoDB. Kafka offsets are committed only after the write succeeds (at-least-once delivery), and redelivered messages are deduplicated by `event_id`.
5. The service keeps its metrics in memory and serves them on `/metrics`. Every 15 seconds the Datadog Agent scrapes them, probes `/health`, and collects MongoDB and Kafka consumer-lag metrics through its integrations.

### Components

| Component | Role |
|---|---|
| **Machines 1–3** (simulated in the PoC) | Publish test results and heartbeats to Kafka as JSON. |
| **Kafka** (single broker for the PoC) | Decouples the machines from the service; buffers data while the service is down and allows replay. |
| **PulseOps service** (Python 3.12 with FastAPI, aiokafka, PyMongo, Pydantic, prometheus-client) | Consumes and validates messages, runs both checks, writes to MongoDB, and serves the HTTP API. |
| **MongoDB** | Stores test results, product progress, QA violations, and machine status. |
| **Datadog Agent** | Scrapes the service's metrics, runs the health checks and the MongoDB and Kafka integrations, and sends everything to Datadog. |

## The two checks

### 1. Data integrity: are all machines sending data?

- The service tracks every machine listed in `LINE_SEQUENCE`, not only the ones it has heard from, so a machine that never sends anything is caught as well as one that stops.
- Any message from a machine, whether a test result or a heartbeat, counts as a sign of life.
- A machine that has been silent for longer than `INTEGRITY_TIMEOUT_SECONDS` (default 15 s, three missed heartbeats) is marked `DOWN`, and goes back to `UP` when its data resumes. Status changes are logged, and the current status is stored in the `machines` collection.
- The check runs on a timer, not on incoming messages, because a silent machine sends nothing that could trigger it.

### 2. Production quality: did the product pass the previous machine?

Products move through the machines in the order set by `LINE_SEQUENCE` (`machine-1` → `machine-2` → `machine-3`). When a machine reports a result for a product, the service looks up the product's latest result at the previous machine:

| Result at the previous machine | Outcome |
|---|---|
| `PASS` | OK: the product followed the line. |
| `FAIL` | `FAILED_UPSTREAM` violation: a product that failed a test kept moving down the line. |
| No result within the grace period | `MISSING_UPSTREAM` violation: the product skipped a machine, or its upstream data was lost. |

- Machine 1 has no previous machine, so its results are only recorded.
- Only the previous machine is checked. Every hop is checked when the product reaches the next machine, so the whole route is covered and one problem is not reported twice.
- A `FAIL` on its own is not a violation; it means the line is doing its job. Results are counted per machine and outcome, which gives Datadog each machine's pass rate.
- Machines publish independently, so a machine 2 result can arrive before the matching machine 1 result. A missing upstream result is held as pending for `QA_GRACE_SECONDS` (default 60 s) before it becomes `MISSING_UPSTREAM`.
- If the integrity check has the previous machine marked `DOWN`, the violation is flagged with `upstream_machine_down: true`, so a data outage can be told apart from products actually skipping a machine.
- Retests are allowed: the latest result at a machine counts, and the full history stays in `test_results`.

## Data model

### Kafka topics

| Topic | Message key | Content |
|---|---|---|
| `machine.test-results` | `product_id` | One message each time a machine tests a product |
| `machine.heartbeats` | `machine_id` | One message per machine every 5 s |

Keying test results by `product_id` puts all of a product's results on the same partition, so one consumer processes them in the order they reached Kafka.

### Messages

Test result:

```json
{
  "schema_version": 1,
  "event_id": "4f9c2a7e-8b1d-4c3e-9a6f-2d5b7e1c0a93",
  "machine_id": "machine-2",
  "product_id": "P-000123",
  "result": "PASS",
  "timestamp": "2026-10-15T08:30:12.345Z",
  "measurements": { "cycle_time_s": 8.6, "temperature_c": 36.2, "leak_rate_ml_min": 0.14 }
}
```

Heartbeat:

```json
{ "schema_version": 1, "machine_id": "machine-2", "timestamp": "2026-10-15T08:30:10.000Z" }
```

- `event_id` is unique per message, so a redelivered message is not stored twice.
- `result` is `PASS` or `FAIL`.
- `timestamp` is when the machine produced the message, in ISO 8601 UTC.
- `measurements` holds the machine's numeric readings: cycle time, temperature, and the value its test measured. They are stored with the result and exposed to Datadog as `pulseops.machine_measurement`.
- `schema_version` lets the format change while older machines still send the previous version.

## HTTP API

| Endpoint | Used by | Returns |
|---|---|---|
| `GET /metrics` | Datadog Agent (OpenMetrics check) | Metrics in Prometheus/OpenMetrics text format |
| `GET /health` | Datadog HTTP check, Docker health check | `200` if the Kafka consumer and MongoDB are healthy, otherwise `503`; the body lists each dependency's status |
| `GET /machines` | Operators, debugging | Integrity status and last-seen time of each machine |
| `GET /products/{product_id}` | Operators, debugging | The product's result at each machine, and its status |
| `GET /violations` | Operators, debugging | Recent QA violations, filterable by `since` and `type` |

Interactive API docs are served at `/docs`.

### Metrics

| Datadog metric | Type | Tags | Meaning |
|---|---|---|---|
| `pulseops.machine_up` | gauge | `machine` | `1` if the machine sent data within the integrity timeout, otherwise `0` |
| `pulseops.machine_silence_seconds` | gauge | `machine` | Seconds since the machine's last message |
| `pulseops.tests.count` | count | `machine`, `result` | Test results received; the basis for throughput and pass rate |
| `pulseops.qa_violations.count` | count | `machine`, `type` | QA violations detected |
| `pulseops.machine_measurement` | gauge | `machine`, `name` | Latest value of each measurement, such as `temperature_c` |
| `pulseops.messages_rejected.count` | count | `topic`, `reason` | Messages that failed validation |
| `pulseops.event_latency_seconds.*` | histogram | none | Time from the machine's timestamp until the service has processed the message |

On `/metrics` the names follow Prometheus conventions: a `pulseops_` prefix, and `_total` on counters. For example, `pulseops_tests_total` arrives in Datadog as `pulseops.tests.count`.

Metrics are tagged by machine, never by product. Product-level detail lives in MongoDB; a product ID tag would create a new metric series for every product and drive up Datadog custom-metric costs.

### Demo scenarios

Faults are injected through the simulator settings in `.env`. Apply a change with `docker compose up -d simulator`.

| Scenario | Setting | What you should see |
|---|---|---|
| A machine stops sending data | `SIM_OFFLINE_MACHINES=machine-2` | `machine-2` goes `DOWN` within about 15 s and *Machine down* fires. Products that reach machine 3 are flagged `MISSING_UPSTREAM` with `upstream_machine_down: true`. |
| A product skips a machine | `SIM_SKIP_RATE=0.05` | `MISSING_UPSTREAM` violations |
| A failed product keeps moving | `SIM_LEAK_RATE=0.05` | `FAILED_UPSTREAM` violations |
| Quality drops | `SIM_FAIL_RATE=0.2` | The pass rate falls and *Pass-rate drop* fires |

## Repository layout

Planned structure:

```
pulseops/
├── service/             PulseOps service: consumer, checks, MongoDB writer, HTTP API
├── simulator/           Simulated machines 1–3, with fault injection
├── mock-data/           Fixed CSV datasets: an ideal run and one with QA violations
├── datadog/             Agent check configs; dashboard and monitor definitions (JSON)
├── docker-compose.yml   Local stack: Kafka, MongoDB, service, simulator, Datadog Agent
└── .env.example         Datadog credentials and simulator settings
```
