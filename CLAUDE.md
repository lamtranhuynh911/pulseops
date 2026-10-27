# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

PulseOps is a proof of concept: machines 1–3 publish test results to Kafka, a Python service checks data integrity and production quality, stores everything in MongoDB, and exposes metrics that the Datadog Agent scrapes. Everything runs on a local Kubernetes cluster created with Kind. `README.md` is the design spec. Implemented: the service (`service/`), the simulator with its replay mode (`simulator/`), the Kustomize manifests (`k8s/`), and the replay Job (`k8s/replay/`). Datadog: Agent values in `datadog/values.yaml` (plus `datadog/values-external-mongodb.yaml` for an external MongoDB), autodiscovery annotations on the pods, and the dashboard and monitors (`datadog/dashboard.json`, `datadog/monitors/*.json`), which `datadog/apply.sh` creates or updates through the Datadog API (idempotent, matched by title/name; edit the JSON, not the UI). The stack has been deployed on Kind from scratch by following the README, and every README demo scenario was checked; the owner's deployment uses the `external-mongodb` overlay.

The README's "Deployment on Kind" is the owner's step-by-step deployment guide (steps 1–7, then updating, troubleshooting, teardown, external MongoDB). Its commands must run as written, in order, from the repo root, with `.env` loaded. When the deployment changes, update the affected step, the step 7 checks and the troubleshooting table.

The owner is using this project to learn Kubernetes. When writing manifests, prefer plain Kustomize manifests over third-party charts or operators for our own components (the Datadog Agent's official Helm chart is the exception), and briefly explain the Kubernetes concepts and choices involved. When implementing, follow the README, and update it if the design changes.

Target runtime is Python 3.12 (`requires-python = ">=3.12"`); the dev machine's default `python3` may be newer.

## Commands

```bash
python3 mock-data/generate.py     # regenerate the mock CSVs (stdlib only, fixed seed, deterministic)
cd service && uv run pytest       # service tests (uv uses Python 3.12 from .python-version; pytest is in the dev group)
cd service && uv run pytest tests/test_quality.py::test_name   # a single test
cd simulator && uv run pytest     # simulator tests
# service storage tests against a real MongoDB (skipped without MONGODB_TEST_URI):
docker run -d --rm --name pulseops-test-mongo -p 127.0.0.1:27018:27017 mongo:7.0.45
cd service && MONGODB_TEST_URI=mongodb://127.0.0.1:27018 uv run pytest
```

Cluster commands (the full bring-up, including the Datadog Helm install, is in the README's "Deployment on Kind"):

```bash
kind create cluster --config k8s/kind-config.yaml        # cluster name: pulseops
docker build -t pulseops-service:dev service && kind load docker-image pulseops-service:dev --name pulseops
docker build -t pulseops-simulator:dev simulator && kind load docker-image pulseops-simulator:dev --name pulseops
kubectl apply -k k8s/overlays/local                      # apply manifests; also re-applies k8s/base/simulator.env
kubectl apply -k k8s/overlays/external-mongodb           # the same without the in-cluster MongoDB (owner's deployment)
helm upgrade datadog-agent datadog/datadog -n datadog --version 3.261.0 \
  -f datadog/values.yaml -f datadog/values-external-mongodb.yaml --set datadog.site="$DD_SITE"   # Agent values changed
kubectl exec -n datadog ds/datadog-agent -c agent -- agent status   # check results (openmetrics, http_check, kafka_consumer, mongo)
kubectl create -f k8s/replay/replay-job.yaml             # replay the scenario in the mock-data-replay ConfigMap (README)
set -a; . ./.env; set +a; datadog/apply.sh               # create/update the Datadog dashboard and monitors
kubectl rollout restart -n pulseops deployment/pulseops-service   # pick up a freshly loaded image
kubectl port-forward -n pulseops svc/pulseops-service 8000:8000
```

There is no linter or formatter configured yet.

## Architecture that spans several files

**Two Kafka topics, keyed differently.** `machine.test-results` is keyed by `product_id`, so all of a product's results land on one partition; `machine.heartbeats` is keyed by `machine_id`. The service consumes both as consumer group `pulseops-service` (the Datadog consumer-lag check watches this group name).

**Delivery semantics.** Offsets are committed only after the MongoDB write succeeds (at-least-once). Redelivery must be harmless: `test_results.event_id` is unique and writes are idempotent; a duplicate `event_id` must not skip the later steps (the QA check still runs), but counters only count a message the first time. Invalid messages go to `rejected_messages`, never dropped silently. Each assigned partition has its own worker (`PartitionWorker`) that processes its records in order and commits after each one, so partitions never wait for each other and heartbeats never wait behind slow test-result writes (this matters with a distant external MongoDB). Workers are started in the rebalance listener before any of their records can be fetched, and the fetch loop never drops records for an assigned partition. Memory is bounded by `max_partition_fetch_bytes` (64 KiB) plus a 500-record queue per partition (the partition is paused in Kafka beyond that). **The rebalance listener must not await network calls:** aiokafka sends no group heartbeats while it runs, and a slow listener gets the replica expelled. Counters are incremented right after the insert that stores a message the first time, so a later failing step can't make it uncounted.

**`LINE_SEQUENCE` drives both checks** (`machine-1,machine-2,machine-3`): it is the list of machines the integrity check expects, and it defines each machine's predecessor for the QA check.

**The service runs as several replicas** (2 by default, at most 6 = partitions of both topics together), so a replica only sees the partitions it's assigned. **Never keep cross-message check state in memory**; it lives in MongoDB:
- `machines.last_seen_at` is updated with `$max` using the service's processing time (not the machine's `timestamp`), so late or redelivered messages can't move it back.
- Every replica runs the integrity and QA timers. Status changes use a conditional update that also checks the evidence (DOWN only if nothing arrived since the cutoff, UP only if a message arrived after the last change), so a verdict from stale reads can't undo a newer one and exactly one replica records each change. Expired `pending_qa_checks` are leased (`leased_until`, 30 s) and deleted only after the verdict is stored, so a replica that dies halfway loses nothing. There is at most one verdict per downstream result and hop (unique index on `qa_violations` `(event_id, upstream_machine_id)`; the first one stored wins), because of at-least-once redelivery and repeated judgements.
- `upstream_machine_down` is true if the previous machine was `DOWN` at any time while the check was pending: stored with the pending check at arrival, read at the verdict, or changed in between (`machines.status_changed_at`).
- Each replica exports its own metric series. Counters sum correctly across pods; `machine_up`/`machine_silence_seconds`/`pending_qa_checks` come from shared state and are identical on every replica.

**Data integrity check** (`service/pulseops/checks/integrity.py`): any message from a machine counts as a sign of life. It runs on a timer, not on message arrival, because a silent machine produces nothing to trigger it. Silent longer than `INTEGRITY_TIMEOUT_SECONDS` (15 s, three missed 5 s heartbeats) means `DOWN`. Machines that have never sent anything must be caught too.

**QA check** (`service/pulseops/checks/quality.py`): on a result from machine N, look up the product's latest result at machine N-1 only (each hop is checked once, so nothing is reported twice). Previous `FAIL` gives `FAILED_UPSTREAM`; no previous result gives `MISSING_UPSTREAM`, but only after `QA_GRACE_SECONDS` (60 s), because machines publish independently and can arrive out of order. The pending check is stored in `pending_qa_checks` and resolved normally if the upstream result arrives in time. If the previous machine is `DOWN`, set `upstream_machine_down: true` on the violation. A `FAIL` by itself is not a violation. The latest result at a machine wins (retests allowed). Product status: `IN_PROGRESS`, `PASSED`, `FAILED`, or `NON_CONFORMING`.

**Metrics and Datadog naming** (`metrics.py`, and the `ad.datadoghq.com/...checks` annotation on the service Deployment in `k8s/base/service.yaml`): expose Prometheus names with a `pulseops_` prefix (`pulseops_tests_total`). The OpenMetrics check uses `namespace: pulseops` with `raw_metric_prefix: pulseops_` and lists counters without `_total`; Datadog then shows `pulseops.tests.count`. Tag metrics by `machine` (and bounded labels like `result`, `type`, `status`, `zone`, `operation`), **never by product ID**: that would create a series per product and drive up custom-metric cost. A metric added to `metrics.py` must also be listed in that annotation, or the Agent doesn't collect it. `measurement_zone` uses the spec limits in `specs.py`, which must stay in sync with the spec table in `mock-data/README.md`; `mongodb_operation_seconds` comes from the `@timed("...")` decorator on the `Storage` methods, so a new `Storage` method that touches MongoDB needs it too. Datadog keeps filling a gauge with its last value for about 5 minutes after its series stops, so a replaced pod's series lingers: queries that aggregate gauges across pods use `.fill(null)` (otherwise a deleted pod's `machine_up` of 1 hides a `DOWN`), and monitors grouped by pod use `on_missing_data: resolve` and a window longer than a pod's shutdown. Datadog can't change a monitor's type in place; `apply.sh` replaces a monitor whose type changed.

**`/health`** returns `200` only when the Kafka consumer runs and reaches the broker and MongoDB is healthy, otherwise `503`, so the Datadog HTTP check also covers those dependencies. While the broker is unreachable the integrity and QA timers stop (a Kafka outage must not mark every machine `DOWN`) and wait `INTEGRITY_TIMEOUT_SECONDS` after it is back. It is the pod's readiness probe; do not use it as the liveness probe (use a TCP check), or a MongoDB outage would put the service into a restart loop.

**A MongoDB outage pauses the service; it must never exit or restart because of it.** On a failed write: `consumer.pause()` all assigned partitions, hold the failed message, don't commit its offset, ping MongoDB with backoff, then retry the held message and `resume()`. Keep calling `getmany()` while paused, or aiokafka's `max_poll_interval_ms` expires and the replica is kicked out of the group. Partitions assigned during a rebalance start unpaused, so re-pause them in the rebalance listener. Every `Consumer.run()` starts paused until MongoDB answers, also a restart by the supervisor in `main.py` (it shares the `ServiceState`), so a replica that starts with MongoDB down starts paused. While paused, `/health` is `503`, `pulseops_consumer_paused` is `1`, and the integrity and QA timers stop. After resuming, the integrity check waits `INTEGRITY_TIMEOUT_SECONDS` before judging, so the unprocessed backlog doesn't mark machines `DOWN`.

**MongoDB can be in-cluster or external.** The service only knows `MONGODB_URI` (from the `pulseops-mongodb` Secret, key `uri`) and `MONGODB_DATABASE` (default `pulseops`). Don't hard-code in-cluster hostnames, and create all indexes at startup so an empty external database works. A database written by an older version can hold two verdicts for one hop; then the unique index can't be built, `prepare()` raises `DuplicateVerdicts`, and the replica stays paused and retries every minute. Never delete them automatically (it's someone's data); the README shows the cleanup.

## Kubernetes (Kind)

- **Kustomize layout:** `k8s/base/` has everything except MongoDB; `k8s/overlays/local/` adds the MongoDB StatefulSet (and its Datadog annotation), and `k8s/overlays/external-mongodb/` is the base alone. For an external MongoDB the Datadog `mongo` check goes in `datadog/values-external-mongodb.yaml`, not an annotation.
- **Topics have 3 partitions** (created by the `kafka-topics` Job). aiokafka's default round-robin assignor deals out the 6 partitions of both topics together, so up to 6 service replicas each get work (checked by scaling to 6); a 7th would sit idle. Machines' heartbeats may not fill all 3 partitions of `machine.heartbeats` (3 keys hashed onto 3 partitions; one was empty in the check).
- **Images are tagged `:dev` and loaded with `kind load docker-image`**; set `imagePullPolicy: IfNotPresent`. A `:latest` tag makes Kubernetes try to pull from a registry and fail with `ErrImagePull`.
- **In-cluster MongoDB is 7.0, not 8.x.** MongoDB 8.0+ refuses to start on Linux kernel 6.19+ (SERVER-121912), and Kind pods run on the host kernel. The tag is pinned in `k8s/overlays/local/kustomization.yaml`.
- **Kafka is a hand-written single-broker KRaft StatefulSet** with a headless Service. It must advertise `kafka-0.kafka.pulseops.svc.cluster.local:9092`, not `localhost`, or clients in other pods connect to themselves. Topics are created by the `kafka-topics` Job.
- **Simulator settings live in `k8s/base/simulator.env`**, which a Kustomize `configMapGenerator` turns into the `simulator-config` ConfigMap. The name gets a content hash, so `kubectl apply -k k8s/overlays/local` rolls the simulator after every change. `.env` (git-ignored) holds `DD_API_KEY`/`DD_SITE` (into the `datadog-secret` Secret, `datadog` namespace) and `MONGODB_URI` (into the `pulseops-mongodb` Secret, `pulseops` namespace); both Secrets are created with `kubectl`, not Kustomize, so credentials never land in Git.
- **Consumer offsets: `auto_offset_reset="none"`, positioned by the service.** aiokafka 0.13/0.14 misread committed offsets while a restarted broker loads them (aiokafka#1206) and would reset partitions; when aiokafka reports a partition without a committed offset, `Consumer._position` (a background task, that partition paused meanwhile) asks again for about 8 s before starting it from the beginning; empty partitions start at once. Keep this until a fixed aiokafka is released.
- **Datadog checks are autodiscovery annotations on the monitored pods** (OpenMetrics and HTTP checks on the service, `mongo` on the in-cluster MongoDB, `kafka_consumer` on Kafka), not `conf.d` files. The Agent's Helm values are in `datadog/values.yaml` (chart pinned to 3.261.0); Kind needs `datadog.kubelet.tlsVerify: false`, and `datadog.checksCardinality: orchestrator` is what gives check metrics their `pod_name` tag.

## Mock data

`mock-data/` holds fixed scenarios (`ideal/`, `qa-violations/`), one CSV per machine, exactly 60 rows each; `generate.py` refuses to write otherwise. Edit the `SCENARIOS` table in `generate.py` and regenerate rather than editing CSVs by hand. `mock-data/README.md` lists the expected outcome for every special product (11 violations, statuses 48/4/11, PASS/FAIL counts per machine). Use it as the ground truth for QA-check tests.

- Columns after `timestamp` are measurements: they go into the message's `measurements` object, plus `"schema_version": 1`. Each machine has its own test column (`length_mm`, `leak_rate_ml_min`, `output_voltage_v`); keep those names in sync with the README's message example.
- The two scenarios use separate product ranges (`P-000001`–`060`, `P-000101`–`163`) and time windows so they can be loaded into the same database. Reusing IDs would hide the bypass cases.
- The CSVs contain no heartbeats. Replaying `qa-violations` without heartbeats leaves gaps of up to about 20 s, which exceeds the 15 s integrity timeout and falsely marks machines `DOWN`.
