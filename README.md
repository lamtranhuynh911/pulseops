# PulseOps

Real-time data integrity and quality assurance monitoring for a production line: **Kafka → Python → MongoDB**, observed with **Datadog**, running on a local Kubernetes cluster created with **Kind**.

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
- The service runs as several replicas, and a MongoDB outage pauses it without losing data or restarting it.

Production hardening (security, multi-broker Kafka, connecting real machines) is out of scope; see [Beyond the PoC](#beyond-the-poc).

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
                            │  consume (partitions split between replicas)
                            v
             ┌─────────────────────────────┐ read/write ┌───────────────────┐
             │ PulseOps service (N copies) │<──────────>│      MongoDB      │
             │  - data integrity check     │            │ in-cluster or     │
             │  - quality (QA) check       │            │ external          │
             │  - HTTP API                 │            └─────────┬─────────┘
             └──────────────┬──────────────┘                      │
                            │  /metrics, /health                  │
                            v                                     │
             ┌─────────────────────────────┐                      │
             │        Datadog Agent        │<─────────────────────┘
             │  also: Kafka consumer lag   │  MongoDB integration
             └──────────────┬──────────────┘
                            v
             ┌─────────────────────────────┐
             │           Datadog           │
             │    dashboards + monitors    │
             └─────────────────────────────┘
```

Everything except Datadog runs as pods in a single-node Kind cluster: the simulated machines, Kafka, the service, and (by default) MongoDB in the `pulseops` namespace, and the Datadog Agent in the `datadog` namespace. MongoDB can be replaced with an external instance; see [Using an external MongoDB](#using-an-external-mongodb).

1. Each machine publishes a message to `machine.test-results` every time it tests a product, and a heartbeat to `machine.heartbeats` every 5 seconds. Heartbeats are sent even when a machine is idle, so "no products right now" is never mistaken for "machine offline".
2. The service runs as several replicas in the consumer group `pulseops-service`. Kafka divides each topic's partitions between the replicas, so every message is processed by exactly one replica. Each message is validated against its schema; invalid messages are stored in `rejected_messages` instead of being dropped silently.
3. Every valid message updates its machine's last-seen time in MongoDB, where all replicas can see it. Test results also go through the QA check.
4. Test results, product state, and violations are written to MongoDB. Kafka offsets are committed only after the write succeeds (at-least-once delivery), and redelivered messages are deduplicated by `event_id`. If MongoDB is unavailable, the service pauses consumption until it is back; see [When MongoDB is unavailable](#when-mongodb-is-unavailable).
5. Each replica serves its metrics on `/metrics`. Every 15 seconds the Datadog Agent scrapes every replica, probes `/health`, and collects MongoDB and Kafka consumer-lag metrics through its integrations. The Agent finds these targets through Kubernetes autodiscovery: each check is declared as an annotation on the pod it monitors.

### Components

| Component | Role |
|---|---|
| **Machines 1–3** (simulated in the PoC) | Publish test results and heartbeats to Kafka as JSON. |
| **Kafka** (single broker in KRaft mode for the PoC) | Decouples the machines from the service; buffers data while the service is down or paused, and allows replay. |
| **PulseOps service** (Python 3.12 with FastAPI, aiokafka, PyMongo, Pydantic, prometheus-client) | Consumes and validates messages, runs both checks, writes to MongoDB, and serves the HTTP API. Runs as several replicas that share their state through MongoDB. |
| **MongoDB** (in-cluster by default, or external) | Stores test results, product progress, QA violations, and machine status, including the state the replicas share. |
| **Datadog Agent** (official Helm chart) | Scrapes the service's metrics, runs the health checks and the MongoDB and Kafka integrations, and sends everything to Datadog. Also reports the cluster's own metrics: nodes, pods, restarts. |
| **Kind** | Runs a local Kubernetes cluster inside Docker, which hosts all of the above. |

## The two checks

### 1. Data integrity: are all machines sending data?

- The service tracks every machine listed in `LINE_SEQUENCE`, not only the ones it has heard from, so a machine that never sends anything is caught as well as one that stops.
- Any message from a machine, whether a test result or a heartbeat, counts as a sign of life.
- A machine that has been silent for longer than `INTEGRITY_TIMEOUT_SECONDS` (default 15 s, three missed heartbeats) is marked `DOWN`, and goes back to `UP` when its data resumes. Status changes are logged, and the current status is stored in the `machines` collection.
- The check runs on a timer, not on incoming messages, because a silent machine sends nothing that could trigger it.
- The last-seen times live in MongoDB, not in each replica's memory. With several replicas, each one receives only some machines' heartbeats, so a replica that judged from its own messages would mark the other machines `DOWN`. See [Running several replicas](#running-several-replicas).

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
- Machines publish independently, so a machine 2 result can arrive before the matching machine 1 result. A missing upstream result is stored in MongoDB as a pending check for `QA_GRACE_SECONDS` (default 60 s) before it becomes `MISSING_UPSTREAM`. If the upstream result arrives in time, the pending check is resolved as if the results had arrived in order.
- If the integrity check has the previous machine marked `DOWN` at any time while the check is pending (or when a `FAILED_UPSTREAM` is detected), the violation is flagged with `upstream_machine_down: true`, so a data outage can be told apart from products actually skipping a machine.
- Retests are allowed: the latest result at a machine counts, and the full history stays in `test_results`.

## Running several replicas

The service Deployment runs 2 replicas by default, and can be scaled up to the number of partitions per topic (3). Kafka gives each partition to exactly one replica of the consumer group; a replica beyond the partition count gets none and stays idle until another replica leaves.

Because a replica only sees part of the data, any state that a check needs across messages is kept in MongoDB:

| State | Where | How replicas coordinate |
|---|---|---|
| Machine last-seen time | `machines.last_seen_at` | Every replica updates it with `$max` on each message, so a late or redelivered message never moves it backwards. |
| Machine status (`UP`/`DOWN`) | `machines.status` | Every replica runs the integrity timer against the shared last-seen times. A status change is written with a conditional update (only if the stored status differs), so exactly one replica records and logs each change. |
| Pending QA checks | `pending_qa_checks` | Every replica runs a timer that leases expired pending checks for 30 seconds and deletes a check only after its verdict is stored, so a replica that dies halfway loses nothing: the lease runs out and another replica judges the check. A downstream result gets at most one verdict per hop (a unique index), so judging a check twice records one violation. |
| A product's latest result per machine | `products` | All of a product's results are on one partition (the key is `product_id`), so one replica handles them in order. |

Each replica exports its own metrics, and Datadog keeps a series per pod. Counters (`tests`, `qa_violations`, `messages_rejected`) count each message once in total, so dashboards sum them across pods. `machine_up` and `machine_silence_seconds` are computed from the shared state, so every replica reports the same value; dashboards take the `max` by machine. `machine_measurement` comes from whichever replica processed a machine's latest result, so dashboards take the `avg` by machine.

When a pod is replaced, its series stop, but Datadog by default keeps filling in a gauge's last value for up to 5 minutes. A deleted pod's `machine_up` of `1` would then hide a machine that goes `DOWN` right afterwards, so the dashboard and the monitors query these gauges with `.fill(null)`. The monitors grouped by pod (*Service unhealthy*, *Consumption paused*) resolve a pod's group once the pod stops reporting. *Service unhealthy* fires only after `/health` has failed for a full minute, longer than a pod takes to shut down, so neither a rolling update nor a deleted pod sets it off.

## When MongoDB is unavailable

The service never exits or restarts because MongoDB is down. Instead it stops consuming and waits:

1. When a MongoDB write fails, the replica pauses all of its Kafka partitions. It keeps the message whose write failed and does not commit its offset.
2. It checks MongoDB every few seconds. While paused, `/health` returns `503`, so the readiness probe marks the pod not ready; the liveness probe only checks that the process responds, so Kubernetes does not restart it. The `pulseops.consumer_paused` metric is `1`.
3. Kafka keeps the machines' messages. Consumer lag grows, but nothing is lost: offsets were not committed, so even a pod that is deleted while paused resumes from the right place.
4. When MongoDB answers again, the replica retries the held message, resumes its partitions, and works through the backlog.
5. The integrity and QA timers stop while the replica is paused, because they need MongoDB. After resuming, the integrity check waits `INTEGRITY_TIMEOUT_SECONDS` before it judges again, so machines are not marked `DOWN` just because their backlog has not been processed yet.

A replica that starts while MongoDB is unavailable starts paused in the same way.

A Kafka outage is handled the same way for the checks: every few seconds each replica checks that the broker answers. While it doesn't, no machine data can arrive, so the integrity and QA timers stop instead of marking every machine `DOWN`, and `/health` returns `503`. When the broker is back, the timers wait `INTEGRITY_TIMEOUT_SECONDS` before judging again.

## Data model

### Kafka topics

| Topic | Message key | Partitions | Content |
|---|---|---|---|
| `machine.test-results` | `product_id` | 3 | One message each time a machine tests a product |
| `machine.heartbeats` | `machine_id` | 3 | One message per machine every 5 s |

Keying test results by `product_id` puts all of a product's results on the same partition, so one replica processes them in the order they reached Kafka.

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
- `timestamp` is when the machine produced the message, in ISO 8601 UTC. The integrity check uses the time the service processed the message instead, so it does not depend on the machines' clocks.
- `measurements` holds the machine's numeric readings: cycle time, temperature, and the value its test measured. They are stored with the result and exposed to Datadog as `pulseops.machine_measurement`.
- `schema_version` lets the format change while older machines still send the previous version.

### MongoDB collections

| Collection | Content |
|---|---|
| `test_results` | Every valid test result; `event_id` is a unique index |
| `products` | Each product's latest result per machine, and its status: `IN_PROGRESS`, `PASSED`, `FAILED`, or `NON_CONFORMING` |
| `qa_violations` | Every QA violation |
| `pending_qa_checks` | Results whose upstream result has not arrived yet, with the time they become `MISSING_UPSTREAM` |
| `machines` | Each machine's last-seen time and integrity status, shared by all replicas |
| `rejected_messages` | Messages that failed validation, with the reason |

The service creates its indexes at startup, so it can start against an empty database.

## HTTP API

| Endpoint | Used by | Returns |
|---|---|---|
| `GET /metrics` | Datadog Agent (OpenMetrics check) | Metrics in Prometheus/OpenMetrics text format |
| `GET /health` | Datadog HTTP check, Kubernetes readiness probe | `200` if the Kafka consumer is running and reaches the broker, and MongoDB is reachable; otherwise `503` (including while paused); the body lists each dependency's status |
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
| `pulseops.consumer_paused` | gauge | none | `1` while the replica has paused consumption because MongoDB is unavailable |
| `pulseops.event_latency_seconds.*` | histogram | none | Time from the machine's timestamp until the service has processed the message |

On `/metrics` the names follow Prometheus conventions: a `pulseops_` prefix, and `_total` on counters. For example, `pulseops_tests_total` arrives in Datadog as `pulseops.tests.count`. The Agent adds a `pod_name` tag (`datadog.checksCardinality: orchestrator` in `datadog/values.yaml`; at the Agent's default, `low`, the replicas' series would collide), so each replica is its own series (see [Running several replicas](#running-several-replicas)).

Metrics are tagged by machine, never by product. Product-level detail lives in MongoDB; a product ID tag would create a new metric series for every product and drive up Datadog custom-metric costs.

### Demo scenarios

Faults are injected through the simulator settings in `k8s/base/simulator.env`. Apply a change with `kubectl apply -k` and the overlay you deployed (`k8s/overlays/local` or `k8s/overlays/external-mongodb`): Kustomize turns the file into a new ConfigMap, and because the ConfigMap's name changes, Kubernetes rolls out a new simulator pod that uses it.

| Scenario | Setting | What you should see |
|---|---|---|
| A machine stops sending data | `SIM_OFFLINE_MACHINES=machine-2` | `machine-2` goes `DOWN` within about 15 s and *Machine down* fires. Products that reach machine 3 are flagged `MISSING_UPSTREAM` with `upstream_machine_down: true`. |
| A product skips a machine | `SIM_SKIP_RATE=0.05` | `MISSING_UPSTREAM` violations |
| A failed product keeps moving | `SIM_LEAK_RATE=0.05` | `FAILED_UPSTREAM` violations |
| Quality drops | `SIM_FAIL_RATE=0.2` | The pass rate falls and *Pass-rate drop* fires |
| The whole line goes offline | `kubectl scale -n pulseops deployment/simulator --replicas=0` | All three machines go `DOWN`. Scale back to `1` to recover. |
| A service replica crashes | `kubectl delete pod -n pulseops <one pulseops-service pod>` | The other replica takes over its partitions and Kubernetes starts a replacement. No machine is marked `DOWN` and no results are lost. |
| MongoDB goes down | `kubectl scale -n pulseops statefulset/mongodb --replicas=0` | Service pods become not ready but are not restarted, `consumer_paused` is `1`, and consumer lag grows. Scale back to `1`: the service resumes, lag drains, and every result is stored. |

With an external MongoDB there is no StatefulSet to scale. Simulate the outage by dropping the Kind node's traffic to it instead (replace `<mongodb-host>` with the IP address in `MONGODB_URI`), and delete the rule to end it:

```bash
docker exec pulseops-control-plane iptables -I FORWARD 1 -d <mongodb-host> -j DROP   # outage starts
docker exec pulseops-control-plane iptables -D FORWARD -d <mongodb-host> -j DROP     # outage ends
```

### Replaying mock data

The fixed scenarios in [`mock-data/`](mock-data/README.md) can be published into the running stack, so the result can be compared with the expected outcome listed there (for `qa-violations`: 11 violations and product statuses 48 `PASSED`, 4 `FAILED`, 11 `NON_CONFORMING`). The simulator image has a `replay` command that publishes a scenario's results in timestamp order, 10 times faster than recorded, with their original `event_id`s and timestamps, and sends a heartbeat per machine every 5 seconds while it runs. It runs as a one-off Kubernetes Job:

```bash
# Load one scenario into a ConfigMap; each CSV becomes a file in the Job's /mock-data directory
kubectl create configmap mock-data-replay -n pulseops --from-file=mock-data/qa-violations/ \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create -f k8s/replay/replay-job.yaml       # create, not apply: every run gets a new Job name
kubectl logs -n pulseops -l app.kubernetes.io/name=replay -f
```

`MISSING_UPSTREAM` violations appear once the 60-second grace period has passed. Running the same replay again changes nothing, because every result is deduplicated by `event_id`. The two scenarios use separate product IDs, so both can be replayed into the same database, next to the simulator's live data. Replayed timestamps are from 2026-10-15, so `pulseops.event_latency_seconds` spikes during a replay.

## Deployment on Kind

The whole stack runs on one local Kubernetes cluster created with [kind](https://kind.sigs.k8s.io/), which runs Kubernetes nodes as Docker containers. Follow the steps in order. Each step says what it creates. The full bring-up takes a few minutes; most of it is building the images the first time.

### Prerequisites

- **Tools:** Docker, kind, `kubectl` and `helm` (for example `brew install kind kubernetes-cli helm`). You also need [uv](https://docs.astral.sh/uv/) to run the tests, and `mongosh` only to prepare an external MongoDB.
- **Resources:** about 4–5 GB of free memory and 10 GB of disk. With Docker Desktop, give its VM at least 6–8 GB.
- **A Datadog account:** an API key and an application key (*Organization Settings → API Keys / Application Keys*), and your Datadog site, for example `datadoghq.com` or `datadoghq.eu`.
- **On Linux, raise the inotify limits** first, or pods can fail with "too many open files" ([kind known issue](https://kind.sigs.k8s.io/docs/user/known-issues/#pod-errors-due-to-too-many-open-files)):

  ```bash
  printf 'fs.inotify.max_user_watches=524288\nfs.inotify.max_user_instances=512\n' | sudo tee /etc/sysctl.d/99-kind.conf
  sudo sysctl --system
  ```

### Step 1: Configure `.env`

```bash
cp .env.example .env
```

Fill in `.env`. It is git-ignored, so credentials stay out of Git; `.env.example` is committed, so leave its values empty.

| Variable | Needed | Purpose |
|---|---|---|
| `DD_API_KEY` | Yes | The Agent's API key. Stored in the cluster as the `datadog-secret` Secret. |
| `DD_SITE` | Yes | Your Datadog site, for example `datadoghq.com`. |
| `DD_APP_KEY` | Yes | Used only by `datadog/apply.sh` on your machine to create the dashboard and monitors. Never stored in the cluster. |
| `DD_NOTIFY` | No | A handle such as `@you@example.com`, appended to every monitor message. |
| `MONGODB_URI` | Yes | Keep `mongodb://mongodb:27017` for the in-cluster MongoDB, or set the connection string of an external one (see [Using an external MongoDB](#using-an-external-mongodb)). |
| `DD_MONGODB_PASSWORD` | External MongoDB only | Password of the read-only `datadog` MongoDB user that the Datadog check logs in as. |

Wrap a value in single quotes if it contains `$`, quotes, backslashes or spaces. Then load the file into your shell; every later step reads these variables, so repeat this in each new terminal:

```bash
set -a; . ./.env; set +a
```

### Step 2: Create the cluster

```bash
kind create cluster --config k8s/kind-config.yaml
kubectl get nodes                          # pulseops-control-plane   Ready
```

This starts one Docker container, `pulseops-control-plane`, that is both the control plane and the node your pods run on, and points `kubectl` at it (context `kind-pulseops`).

### Step 3: Build and load the images

```bash
docker build -t pulseops-service:dev service
docker build -t pulseops-simulator:dev simulator
kind load docker-image pulseops-service:dev pulseops-simulator:dev --name pulseops
```

The Kind node has its own container runtime and can't see the images on your machine, so they are copied into it. They are tagged `:dev`, not `:latest`, so Kubernetes uses the loaded image instead of trying to pull it from a registry. If `kind load` fails with "content digest ... not found" (Docker's containerd image store), load an archive instead:

```bash
docker save -o /tmp/images.tar pulseops-service:dev pulseops-simulator:dev
kind load image-archive /tmp/images.tar --name pulseops
```

### Step 4: Install the Datadog Agent

```bash
kubectl create namespace datadog
kubectl create secret generic datadog-secret -n datadog --from-literal api-key="$DD_API_KEY"
helm repo add datadog https://helm.datadoghq.com
helm repo update
helm install datadog-agent datadog/datadog -n datadog --version 3.261.0 \
  -f datadog/values.yaml --set datadog.site="$DD_SITE"
```

The chart installs the Agent as a DaemonSet (one Agent pod per node), a Cluster Agent and kube-state-metrics. The API key goes into a Secret created with `kubectl`, so it is never in a file in Git.

**With an external MongoDB**, create its read-only `datadog` user first ([see below](#create-the-read-only-datadog-user)). Then give the Agent the host and that password through the `datadog-mongodb` Secret, and add the second values file:

```bash
kubectl create secret generic datadog-mongodb -n datadog \
  --from-literal host="$(printf '%s' "$MONGODB_URI" | sed -E 's#^mongodb(\+srv)?://([^@/]*@)?([^/?]*).*#\3#')" \
  --from-literal password="$DD_MONGODB_PASSWORD"
helm install datadog-agent datadog/datadog -n datadog --version 3.261.0 \
  -f datadog/values.yaml -f datadog/values-external-mongodb.yaml --set datadog.site="$DD_SITE"
```

If the Agent is already installed, run the same command with `helm upgrade` instead of `helm install`.

### Step 5: Deploy PulseOps

```bash
kubectl apply -f k8s/base/namespace.yaml   # the namespace first: the Secret goes into it
kubectl create secret generic pulseops-mongodb -n pulseops --from-literal uri="$MONGODB_URI"
kubectl apply -k k8s/overlays/local        # with an external MongoDB: k8s/overlays/external-mongodb

kubectl rollout status -n pulseops statefulset/kafka --timeout=300s
kubectl rollout status -n pulseops statefulset/mongodb --timeout=300s     # local overlay only
kubectl wait -n pulseops --for=condition=complete job/kafka-topics --timeout=300s
kubectl rollout status -n pulseops deployment/pulseops-service --timeout=300s
kubectl rollout status -n pulseops deployment/simulator --timeout=300s
```

`kubectl apply -k` builds the manifests with Kustomize: the `local` overlay is the base (Kafka, the topic Job, the service and the simulator) plus an in-cluster MongoDB, and `external-mongodb` is the base alone. The [Kubernetes resources](#kubernetes-resources) table lists what it creates. The namespace is created with `apply`, not `kubectl create namespace`, because `apply -k` manages it afterwards. The MongoDB connection string is a Secret created with `kubectl`, because an external URI contains credentials.

The service pods stay not ready (`0/1`) until Kafka and MongoDB answer, usually for less than a minute. That is the readiness probe on `/health` at work; the pods are not restarted.

### Step 6: Create the Datadog dashboard and monitors

```bash
datadog/apply.sh
```

This creates the *PulseOps* dashboard and the six monitors from `datadog/dashboard.json` and `datadog/monitors/*.json` through the Datadog API, using `DD_API_KEY`, `DD_APP_KEY` and `DD_SITE`. It finds existing objects by title or name and updates them, so it is safe to run again; change them by editing the JSON, not the Datadog UI.

### Step 7: Check the deployment

```bash
kubectl get pods -n pulseops
```

`kafka-0`, both `pulseops-service` pods, `simulator` (and `mongodb-0` with the `local` overlay) are `Running` and `1/1` ready, and `kafka-topics` is `Completed`. Then open the API on your machine; the port-forward keeps running, so use a second terminal for the rest:

```bash
kubectl port-forward -n pulseops svc/pulseops-service 8000:8000
```

```bash
curl -s localhost:8000/health     # {"status":"ok","kafka_consumer":"running","mongodb":"ok","paused":false}
curl -s localhost:8000/machines   # machine-1, machine-2 and machine-3, each with "status": "UP"
kubectl exec -n pulseops kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group pulseops-service   # LAG near 0 ("-" for an empty partition), split over both pods
kubectl exec -n datadog ds/datadog-agent -c agent -- agent status       # see below
```

In the `agent status` output, the `openmetrics` and `http_check` checks each have one `[OK]` instance per service pod, and `kafka_consumer` and `mongo` are `[OK]`. In Datadog, the *PulseOps* dashboard fills within a few minutes and every monitor is *OK*. The interactive API documentation is at <http://localhost:8000/docs>.

### Updating the stack

| What changed | What to run |
|---|---|
| Service or simulator code | Rebuild the image and `kind load` it again (step 3), then `kubectl rollout restart -n pulseops deployment/pulseops-service` (or `deployment/simulator`). |
| Manifests or `k8s/base/simulator.env` | `kubectl apply -k` with your overlay. A changed `simulator.env` gives the ConfigMap a new name, so a new simulator pod starts. |
| Agent values | The `helm upgrade` form of the step 4 command, with the same values files. |
| Dashboard or monitors | Edit the JSON and run `datadog/apply.sh` again. |

### Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `kind load` fails with "content digest ... not found" | Docker's containerd image store. Load an archive instead (step 3). |
| Pods fail with "too many open files" | The inotify limits are too low. Raise them (see [Prerequisites](#prerequisites)). |
| `ErrImagePull` or `ImagePullBackOff` on the service or simulator | The image isn't in the Kind node. Run `kind load` again (step 3), and keep the `:dev` tag. |
| `CreateContainerConfigError` on the service | The `pulseops-mongodb` Secret is missing. Create it (step 5). |
| Service pods `Running` but `0/1` ready for minutes | `/health` is `503`. Its body (`curl` through the port-forward) and `kubectl logs -n pulseops deployment/pulseops-service` say whether Kafka or MongoDB is unreachable. With an external MongoDB, check `MONGODB_URI` and that the Kind node can reach the server. |
| `mongodb-0` in `CrashLoopBackOff`, log mentions "Linux kernel versions 6.19 and newer" | MongoDB 8 on a new kernel ([SERVER-121912](https://jira.mongodb.org/browse/SERVER-121912)). Keep the 7.0 tag pinned in `k8s/overlays/local/kustomization.yaml`. |
| `agent status` shows the `mongo` check with `Authentication failed` | The `datadog` user is missing, was created in a database other than `admin`, or its password differs from `DD_MONGODB_PASSWORD`. See [Create the read-only datadog user](#create-the-read-only-datadog-user). |
| Nothing arrives in Datadog | The API key or `DD_SITE` is wrong. `agent status` shows the site it uses (`config_site`) and, under the endpoints, the last characters of the API key; *API Keys status* says whether Datadog accepts it. |
| Service log: `qa_violations has several verdicts for one downstream result and hop` | The database was written by an earlier version. See [Upgrading a database written by an earlier version](#upgrading-a-database-written-by-an-earlier-version). |

### Tearing down

```bash
kind delete cluster --name pulseops
```

This deletes everything in the cluster, including the in-cluster MongoDB's and Kafka's data. An external MongoDB and the Datadog dashboard and monitors are not touched: `apply.sh` reuses them after the next bring-up, and you can delete them in the Datadog UI if you no longer need them.

### Using an external MongoDB

The service reads its connection string from the `uri` key of the `pulseops-mongodb` Secret, and its database name from `MONGODB_DATABASE` (default `pulseops`). Nothing else in the service knows where MongoDB runs, so an external instance (MongoDB Atlas, or a MongoDB on another server) only needs a different `MONGODB_URI` and the `external-mongodb` overlay, which leaves out the in-cluster MongoDB.

Requirements for the external instance:

- **Reachable from the Kind node.** Pods can reach the internet through Docker. A MongoDB running on your own machine is not at `localhost` (inside a pod that is the pod itself); use the Docker bridge gateway address, usually `172.18.0.1` for Kind's network.
- **A user with `readWrite` on the database** for the service, in `MONGODB_URI`. The service creates its own collections and indexes, so the database can start empty.
- **A separate read-only user for the Datadog check,** whose password comes from a Kubernetes Secret, not from the values file.

An annotation can only describe a pod in the cluster, so for an external MongoDB the Datadog `mongo` check is configured in the Agent's Helm values instead (`datadog/values-external-mongodb.yaml`). The Agent reads the host and password from the `datadog-mongodb` Secret, the only Secret it is allowed to read.

#### Create the read-only datadog user

Create it once, in the `admin` database, with an admin login. `passwordPrompt()` asks for the new user's password: choose a strong one (for example from `openssl rand -hex 16`), and put the same value in `.env` as `DD_MONGODB_PASSWORD=<the password>`. The check needs all four roles; with only `read` on `pulseops` it can't run `serverStatus`, for example.

```bash
mongosh "mongodb://<admin-user>@<host>:<port>/admin" --eval '
db.getSiblingDB("admin").createUser({user: "datadog", pwd: passwordPrompt(), roles: [
  {role: "read", db: "admin"}, {role: "clusterMonitor", db: "admin"},
  {role: "read", db: "local"}, {role: "read", db: "pulseops"}]})'
```

The Agent reads the `datadog-mongodb` Secret when it starts. If you change the password later, recreate the Secret and restart the Agent with `kubectl rollout restart -n datadog daemonset/datadog-agent`.

#### Switching a running deployment to an external MongoDB

1. Set `MONGODB_URI` in `.env`, load `.env` again, and replace the Secret:
   ```bash
   kubectl create secret generic pulseops-mongodb -n pulseops --from-literal uri="$MONGODB_URI" \
     --dry-run=client -o yaml | kubectl apply -f -
   ```
2. Apply the `external-mongodb` overlay, remove the in-cluster MongoDB, and restart the service so it reads the new Secret:
   ```bash
   kubectl apply -k k8s/overlays/external-mongodb
   kubectl delete -n pulseops statefulset/mongodb service/mongodb --ignore-not-found
   kubectl rollout restart -n pulseops deployment/pulseops-service
   ```
   `kubectl apply` only creates and updates resources, so the StatefulSet has to be deleted explicitly. Its PersistentVolumeClaim (the data) is kept until you delete it too.
3. Create the [read-only datadog user](#create-the-read-only-datadog-user), then the `datadog-mongodb` Secret and the `helm upgrade` from step 4 (add `--dry-run=client -o yaml | kubectl apply -f -` to the Secret command if it already exists).

To switch back, set `MONGODB_URI` to `mongodb://mongodb:27017`, replace the Secret, apply `k8s/overlays/local`, restart the service, and upgrade the Agent with `datadog/values.yaml` only.

### Upgrading a database written by an earlier version

The service allows at most one QA verdict per downstream result and hop, enforced by a unique index on `qa_violations` (`event_id`, `upstream_machine_id`). Earlier versions stored one verdict per violation type, so a database they wrote can hold two verdicts for one hop, and then the index can't be built. The service doesn't delete them itself. Instead it logs `qa_violations has several verdicts for one downstream result and hop`, stays paused (`/health` returns `503`, `pulseops.consumer_paused` is `1`), and tries again every minute. Finish the rollout first (or scale the old version to 0): a pod still running an earlier version can store new duplicates. Then keep the first verdict stored for each hop and delete the others (`MONGODB_DATABASE` defaults to `pulseops`, as in the service):

```bash
mongosh "$MONGODB_URI" --eval '
const verdicts = db.getSiblingDB(process.env.MONGODB_DATABASE || "pulseops").qa_violations;
verdicts.aggregate([
  {$sort: {detected_at: 1, _id: 1}},
  {$group: {_id: {e: "$event_id", u: "$upstream_machine_id"}, ids: {$push: "$_id"}}},
  {$match: {"ids.1": {$exists: true}}}
]).forEach(hop => verdicts.deleteMany({_id: {$in: hop.ids.slice(1)}}))'
```

The service resumes on its own within a minute. If the log line comes back, an old pod stored another duplicate: run the cleanup again.

### Kubernetes resources

| Resource | Kind | Notes |
|---|---|---|
| `kafka` | StatefulSet (1 replica) + headless Service | KRaft mode, no ZooKeeper. The broker advertises its stable DNS name, `kafka-0.kafka.pulseops.svc.cluster.local:9092`, which only a StatefulSet pod gets. |
| `kafka-topics` | Job | Creates `machine.test-results` and `machine.heartbeats` with 3 partitions each once Kafka is up. |
| `mongodb` | StatefulSet (1 replica) + headless Service, `local` overlay only | MongoDB 7.0: MongoDB 8.0 and later refuse to start on Linux kernel 6.19 and newer ([SERVER-121912](https://jira.mongodb.org/browse/SERVER-121912)), and pods run on the host's kernel. Data on a PersistentVolumeClaim from Kind's default `standard` StorageClass, so it survives pod restarts but not `kind delete cluster`. Carries the Datadog annotation for the MongoDB check. |
| `pulseops-mongodb` | Secret, created with `kubectl` | The MongoDB connection string. Kept out of Git because an external URI contains credentials. |
| `pulseops-service` | Deployment (2 replicas) + Service | Readiness probe on `/health`; liveness probe is a TCP check on the port. Carries the Datadog annotations for the OpenMetrics and HTTP checks. |
| `simulator` | Deployment (1 replica) | Settings from the `simulator-config` ConfigMap, generated from `k8s/base/simulator.env`. |
| `datadog-agent` | DaemonSet (Helm chart, `datadog` namespace) | API key from the `datadog-secret` Secret. |

Some of these choices are deliberate:

- **Readiness and liveness probes check different things.** Readiness asks "can this pod do its job right now?": if MongoDB is down, `/health` returns `503` and Kubernetes stops routing requests to the pod but leaves it running. Liveness asks "is the process stuck?": it only checks that the port answers, so a MongoDB outage never restarts the service.
- **Images are tagged `:dev`, not `:latest`.** Kubernetes always tries to pull `:latest` images from a registry and fails, because the images exist only on the Kind node. With any other tag it uses the loaded image (`imagePullPolicy: IfNotPresent`).
- **MongoDB is swapped with Kustomize overlays.** `k8s/base/` holds everything except MongoDB; `k8s/overlays/local/` adds the in-cluster MongoDB, and `k8s/overlays/external-mongodb/` uses the base as it is.
- **Datadog checks are pod annotations,** not files mounted into the Agent. When a pod is rescheduled and its IP changes, the Agent follows it automatically. The exception is the check for an external MongoDB, which has no pod.
- **Kind needs `datadog.kubelet.tlsVerify: false`** in `datadog/values.yaml`, because Kind's kubelet uses a self-signed certificate. The values also set `datadog.clusterName` (Kind's can't be detected) and turn off the Datadog Operator, which recent charts install by default; this chart manages the Agent itself.

## Repository layout

```
pulseops/
├── service/             PulseOps service: consumer, checks, MongoDB writer, HTTP API
├── simulator/           Simulated machines 1–3, with fault injection
├── mock-data/           Fixed CSV datasets: an ideal run and one with QA violations
├── k8s/
│   ├── kind-config.yaml   Kind cluster definition (single node, cluster name "pulseops")
│   ├── base/              Namespace, Kafka, service, simulator; simulator.env (fault injection)
│   ├── replay/            One-off Job that replays a mock-data scenario
│   └── overlays/
│       ├── local/             base + in-cluster MongoDB (the default)
│       └── external-mongodb/  base only; the service connects to MONGODB_URI
├── datadog/             Helm values for the Agent (values.yaml, values-external-mongodb.yaml);
│                        dashboard.json and monitors/*.json, applied by apply.sh
└── .env.example         Datadog credentials and the MongoDB connection string
```

## Known limitations

- **aiokafka can misread committed offsets right after a Kafka restart** ([aiokafka#1206](https://github.com/aio-libs/aiokafka/issues/1206), open in 0.13 and 0.14): while the broker is still loading them, they look missing, and aiokafka would reset the partition to the beginning, replaying the whole topic. The service works around it: it uses `auto_offset_reset="none"`, and when a partition has no committed offset it pauses just that partition and asks again for about 8 seconds before starting it from the beginning (an empty partition starts at once). Remove the workaround once a fixed aiokafka is released.

## Beyond the PoC

- **Run Kafka with an operator** such as Strimzi, with several brokers, instead of a hand-written StatefulSet.
- **Package the service as a Helm chart** so it can be deployed to a real cluster with environment-specific values.
- **Secure the pipeline:** TLS and authentication for Kafka and MongoDB, secrets from a secret manager instead of `kubectl create secret`, and NetworkPolicies between pods.
- **Connect real machines** in place of the simulator.
