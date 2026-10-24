# Plan: build and deploy PulseOps on Kind

> DRAFT — items marked (TBD) are filled in from the research workflow before review.

## Context

`README.md` is the design spec and is ahead of the code. Today only `mock-data/generate.py` works: the
`service/` modules and `service/Dockerfile` are one-line placeholders, `simulator/` and `datadog/` are
empty, `k8s/` and `.env` don't exist. On this machine Docker 29.8 (containerd image store, cgroup v2)
and Homebrew are installed, but **kind, kubectl and helm are not**, and the inotify limits
(65536 watches / 128 instances) are below what kind recommends.

Goal: demonstrate the README's PoC goals end to end on a local Kind cluster, built in milestones that
are each deployed and checked before the next one starts. The owner is learning Kubernetes, so every
manifest milestone ends with a short explanation of the Kubernetes concepts it introduced.

Decisions taken with the user:
- **Scope: everything**: service, simulator, Kustomize manifests, Datadog, in milestones.
- **Replay Job**: the simulator image gets a replay mode. A one-off Job publishes a mock-data
  scenario into the running stack, and the result is checked against `mock-data/README.md`. This is
  a new feature, so the README is updated.
- **Dashboard and monitors** live as JSON in `datadog/` and are applied by an idempotent script
  through the Datadog API, which adds `DD_APP_KEY` to `.env`.

## Milestones

| # | Milestone | What runs afterwards | Checked by |
|---|---|---|---|
| 0 | Prerequisites | tools, sysctl, `.env` | `kind/kubectl/helm version` |
| 1 | Kind cluster | 1-node cluster `pulseops` | `kubectl get nodes,sc` |
| 2 | Kafka | namespace, Kafka StatefulSet, `kafka-topics` Job | both topics, 3 partitions |
| 3 | MongoDB | `local` overlay, Secret | `mongosh` ping, PVC survives scale 0→1 |
| 4 | Simulator | simulator Deployment | messages in both topics, keyed correctly |
| 5 | Service | 2 service replicas | `/health` 200, 3 machines `UP`, data in MongoDB |
| 6 | Replay Job | one-off Jobs | 11 violations, statuses 48/4/11 |
| 7 | Datadog Agent | Agent DaemonSet + annotations | `agent status`, metrics in Datadog |
| 8 | Dashboard + monitors | JSON + `datadog/apply.sh` | dashboard populated, monitors OK |
| 9 | External MongoDB | `external-mongodb` overlay | service writes to a host MongoDB |
| 10 | Demo scenarios + docs | — | every README demo row, clean bring-up |

Commit after each milestone (asking first). Docs (README, CLAUDE.md) are updated in the milestone
that changes the design, and checked once more in M10.

---

### M0 — Prerequisites

- Install tools: `brew install kind kubernetes-cli helm` (versions (TBD)).
- Raise inotify limits (kind's known "too many open files" failure), user runs with sudo:
  `fs.inotify.max_user_watches=524288`, `fs.inotify.max_user_instances=512`, persisted in
  `/etc/sysctl.d/99-kind.conf` (TBD: confirm values).
- `.env.example`: add `DD_APP_KEY=` (application key, used only by `datadog/apply.sh` on the host,
  never stored in the cluster) and optional `DD_NOTIFY=` (a handle such as `@you@example.com` that
  `apply.sh` appends to monitor messages). Then `cp .env.example .env` and fill it in.
- Python: `service/.python-version` and `simulator/.python-version` = `3.12` so `uv` uses the
  installed CPython 3.12.15, not the system 3.14.

### M1 — Kind cluster

- `k8s/kind-config.yaml`: `name: pulseops`, one `control-plane` node, node image pinned by digest
  (TBD), no port mappings (the API is reached with `kubectl port-forward`).
- Verify: `kind create cluster --config k8s/kind-config.yaml`; `kubectl get nodes` Ready;
  `kubectl get storageclass` shows `standard (default)`.
- Concepts: a Kind node is a Docker container running kubelet + containerd; it can't see host
  images (hence `kind load`); a single node runs both control plane and workloads; the `standard`
  StorageClass provisions PersistentVolumes as directories on the node.

### M2 — Kafka (namespace `pulseops`)

Files:
- `k8s/base/namespace.yaml`, `k8s/base/kustomization.yaml` (`namespace: pulseops`; `resources`;
  `images:` pins the Kafka tag once for the StatefulSet and the Job; `labels:` with
  `includeSelectors: false`, because `commonLabels` would also change Deployment selectors, which
  are immutable).
- `k8s/base/kafka.yaml`:
  - Headless Service `kafka` (`clusterIP: None`, port 9092, `publishNotReadyAddresses: true`).
  - StatefulSet `kafka`, 1 replica, `serviceName: kafka`, image `apache/kafka:(TBD)`, combined
    broker+controller KRaft node configured by env vars: node id 1; listeners `PLAINTEXT://:9092`,
    `CONTROLLER://:9093`; advertised `PLAINTEXT://kafka-0.kafka.pulseops.svc.cluster.local:9092`;
    controller quorum on `localhost:9093` (no DNS dependency for its own controller); replication
    factor 1 / min ISR 1 for internal topics; **`auto.create.topics.enable=false`** and
    `num.partitions=3`, so a client that starts before the Job can't create 1-partition topics;
    fixed `CLUSTER_ID`; `log.dirs` on the PVC; heap 512 MB. (TBD: exact env names for the version.)
  - `enableServiceLinks: false` (the image turns `KAFKA_*` env vars into broker config).
  - `volumeClaimTemplates` 2 Gi; readiness + liveness `tcpSocket: 9092`; memory request/limit.
- `k8s/base/kafka-topics-job.yaml`: same image; shell loop waits until
  `kafka-topics.sh --list` succeeds, then creates both topics with `--if-not-exists --partitions 3
  --replication-factor 1` and prints `--describe`. `restartPolicy: OnFailure`, `backoffLimit`,
  `activeDeadlineSeconds`.
- `k8s/overlays/local/kustomization.yaml` (`resources: [../../base]` for now) and
  `k8s/overlays/external-mongodb/kustomization.yaml` (`resources: [../../base]`).
- All clients use `KAFKA_BOOTSTRAP_SERVERS=kafka-0.kafka.pulseops.svc.cluster.local:9092`.

Verify:
```bash
kubectl create namespace pulseops
kubectl apply -k k8s/overlays/local
kubectl -n pulseops rollout status statefulset/kafka
kubectl -n pulseops wait --for=condition=complete job/kafka-topics --timeout=180s
kubectl -n pulseops exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
kubectl -n pulseops delete pod kafka-0   # comes back as kafka-0 with the same PVC; topics still there
```

Concepts: StatefulSet vs Deployment (stable pod name and storage), headless Service and per-pod DNS,
why the broker advertises its FQDN, `volumeClaimTemplates`, Jobs (run to completion; a changed Job
spec can't be re-applied, delete it first), Kustomize base/overlays and the `images` transformer.

### M3 — MongoDB (`local` overlay)

- `k8s/overlays/local/mongodb.yaml`: headless Service `mongodb` (27017) + StatefulSet `mongodb`,
  image `mongo:(TBD 8.x)`, `volumeClaimTemplates` 1 Gi at `/data/db`, readiness exec
  `mongosh --quiet --eval "db.adminCommand('ping')"`, liveness `tcpSocket: 27017`,
  `--wiredTigerCacheSizeGB 0.25` with a matching memory limit. No auth (in-cluster PoC).
- `k8s/overlays/local/kustomization.yaml`: add `mongodb.yaml`.
- Secret, from `.env`: `kubectl create secret generic pulseops-mongodb -n pulseops --from-literal uri="$MONGODB_URI"`.

Verify: `kubectl -n pulseops exec mongodb-0 -- mongosh --quiet --eval "db.adminCommand('ping')"`;
`kubectl -n pulseops scale statefulset/mongodb --replicas=0`, then back to 1: same PVC, data kept.

Concepts: overlays add resources to a base; Secret vs ConfigMap; why Secrets are created with
`kubectl` (credentials stay out of Git); PVC lifecycle (survives scale-to-0, not `kind delete`).

### M4 — Simulator

Files: `simulator/pyproject.toml` (aiokafka), `simulator/uv.lock`, `simulator/.python-version`,
`simulator/Dockerfile`, `simulator/.dockerignore`, package `simulator/simulator/`
(`__main__.py`, `config.py`, `machines.py`, `messages.py`, `live.py`, `replay.py`),
`simulator/tests/`; `k8s/base/simulator.env`, `k8s/base/simulator.yaml`.

- `machines.py`: the three machine definitions (cycle time, temperature, measurement name, pass and
  fail ranges) taken from `MACHINES` in `mock-data/generate.py`, with the same measurement names
  (`length_mm`, `leak_rate_ml_min`, `output_voltage_v`). The simulator is a separate image and can't
  import from `mock-data/`, so the table is copied; comment both places to keep them in sync.
- `messages.py`: builds test-result and heartbeat messages exactly as in the README (`schema_version`
  1, UUID4 `event_id`, ISO 8601 UTC with milliseconds and `Z`). Also converts a mock-data CSV row:
  the columns after `timestamp` go into `measurements` as floats.
- `live.py` (default command): a product enters every `SIM_PRODUCT_INTERVAL_SECONDS` and reaches each
  next machine a fixed stage time later (as in `generate.py`). Per machine, `FAIL` with
  `SIM_FAIL_RATE`; a failed product leaves the line unless `SIM_LEAK_RATE` lets it continue;
  `SIM_SKIP_RATE` skips machine 1 or 2; machines in `SIM_OFFLINE_MACHINES` publish nothing (no
  results, no heartbeats) while products still move on. Heartbeats every
  `SIM_HEARTBEAT_INTERVAL_SECONDS` per online machine. Product IDs are unique across restarts and
  never in the mock ranges (counter seeded from Unix time, e.g. `P-1792713600`). Keys: `product_id`
  for results, `machine_id` for heartbeats. Retries with backoff until Kafka and the topics exist.
- `replay.py` (`python -m simulator replay /mock-data`): reads `machine-*.csv`, merges rows by
  `timestamp`, publishes them with their original `event_id` and `timestamp`, spaced by the CSV gaps
  divided by `REPLAY_SPEED` (default 10), sends a heartbeat per machine every 5 s while it runs, then
  flushes and exits 0.
- `simulator.env` (the old `.env.example` values): `SIM_PRODUCT_INTERVAL_SECONDS=2`,
  `SIM_HEARTBEAT_INTERVAL_SECONDS=5`, `SIM_FAIL_RATE=0.03`, `SIM_SKIP_RATE=0`, `SIM_LEAK_RATE=0`,
  `SIM_OFFLINE_MACHINES=`. Base kustomization: `configMapGenerator` `simulator-config` from it.
- `simulator.yaml`: Deployment, 1 replica, **`strategy: Recreate`** (a rolling update would briefly
  run two simulators), `envFrom` the ConfigMap, `KAFKA_BOOTSTRAP_SERVERS` env, image
  `pulseops-simulator:dev`, `imagePullPolicy: IfNotPresent`. No Service: it only produces.
- Dockerfile: same uv pattern as the service (M5); `ENTRYPOINT ["python", "-m", "simulator"]`,
  `CMD ["live"]`.
- Tests: CSV row → message conversion, merge order, journey rules with a seeded RNG.

Verify:
```bash
(cd simulator && uv run pytest)
docker build -t pulseops-simulator:dev simulator
kind load docker-image pulseops-simulator:dev --name pulseops      # (TBD: containerd image store workaround)
kubectl apply -k k8s/overlays/local
kubectl -n pulseops exec kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic machine.heartbeats --from-beginning --max-messages 6 --property print.key=true
# edit SIM_FAIL_RATE in k8s/base/simulator.env, apply again: a new simulator-config-<hash> and a new pod
```

Concepts: `configMapGenerator` and the hash suffix that triggers a rollout; `envFrom`; `Recreate`
vs `RollingUpdate`; loading local images into Kind and why the tag is `:dev`.

### M5 — Service

Files: every module under `service/pulseops/`, `service/tests/`, `service/Dockerfile`,
`service/.dockerignore`, `service/uv.lock`, `service/.python-version`, `service/pyproject.toml`
(dev group may gain `pytest-asyncio` (TBD)), `k8s/base/service.yaml`.

Modules (the CLAUDE.md invariants are binding; the notes below are the non-obvious parts):
- `config.py`: frozen dataclass from env: `KAFKA_BOOTSTRAP_SERVERS`, group `pulseops-service`, the
  two topic names, `MONGODB_URI` (required), `MONGODB_DATABASE=pulseops`, `LINE_SEQUENCE`,
  `INTEGRITY_TIMEOUT_SECONDS=15`, `QA_GRACE_SECONDS=60`, timer intervals (integrity 1 s, QA 5 s).
- `models.py`: Pydantic `TestResult` / `Heartbeat` (`schema_version: Literal[1]`,
  `result: Literal["PASS","FAIL"]`, timezone-aware `timestamp`, `measurements: dict[str, float]`),
  `machine_id` must be in `LINE_SEQUENCE`. A small fixed set of rejection reasons (`invalid_json`,
  `invalid_schema`, `unsupported_version`, `unknown_machine`) keeps the `reason` label bounded.
- `storage.py`: PyMongo async client (TBD: `AsyncMongoClient`), short `serverSelectionTimeoutMS` so
  an outage is noticed quickly. `ensure_indexes()` at startup: unique `test_results.event_id`, plus
  indexes for queries by product, machine and time range, and `pending_qa_checks.due_at`.
  Idempotent writes only: `$max` on `machines.last_seen_at` with processing time; status change as a
  conditional update; violations upserted with `$setOnInsert` and a deterministic `_id`
  (`<triggering event_id>:<type>`); pending checks claimed with `find_one_and_delete`.
- `checks/integrity.py`: pure `evaluate(last_seen, now, timeout)` + a timer on every replica: read
  the shared `machines`, set `machine_up` / `machine_silence_seconds`, write status changes
  conditionally and log only when this replica's update changed the document. Machines never heard
  from count as silent since start. The timer doesn't judge for `INTEGRITY_TIMEOUT_SECONDS` after
  startup and after a resume, and stops while paused.
- `checks/quality.py`: pure decision (`PASS` → ok, `FAIL` → `FAILED_UPSTREAM`, missing → pending
  until `now + QA_GRACE_SECONDS`) and pure `product_status()`: `NON_CONFORMING` if any violation,
  else `FAILED` if a latest result is `FAIL`, else `PASSED` after passing the last machine, else
  `IN_PROGRESS` (a pending check also keeps it `IN_PROGRESS`). `NON_CONFORMING` is written
  conditionally so a QA timer on another replica and the partition owner can't overwrite each other.
  When the upstream result arrives, the pending check is claimed (`find_one_and_delete`) and checked
  normally. The timer claims expired checks, re-reads the upstream result (it may have just arrived),
  and only then writes `MISSING_UPSTREAM`, with `upstream_machine_down` from `machines.status`.
- `consumer.py`: aiokafka, `enable_auto_commit=False`, commit offset+1 after the MongoDB writes.
  Every step is idempotent and a duplicate `event_id` does **not** skip the later steps (a crash
  between steps must not lose a QA check); counters only increase on a first insert, so redelivery
  doesn't double-count. Invalid messages go to `rejected_messages`, then commit. On a MongoDB error:
  pause all assigned partitions, keep the failed message (or seek back to it), don't commit, keep
  calling `getmany()` while pinging MongoDB with backoff, then retry and `resume()`. The rebalance
  listener re-pauses newly assigned partitions while paused; startup with MongoDB down starts
  paused. Kafka unavailable at startup: retry in the background, don't crash.
- `metrics.py`: the README metric table with `pulseops_` names, `disable_created_metrics()`, labels
  `machine`, `result`, `type`, `name`, `topic`, `reason` only, never product ID.
- `api.py`: `/health` (cached dependency status refreshed every few seconds, so probes never block;
  200 only if consumer running, MongoDB reachable and not paused, else 503 with per-dependency body),
  `/machines`, `/products/{product_id}`, `/violations?since=&type=`, and `/metrics` as a plain route
  (TBD: confirm `mount()` redirect issue).
- `main.py`: FastAPI lifespan starts the consumer and both timers as background tasks **without
  waiting for Kafka or MongoDB**, so the port opens at once for the TCP liveness probe; on shutdown it
  stops the consumer so the group rebalances right away. One uvicorn worker per pod (the metrics
  registry is per process); scale with replicas.

Tests (`cd service && uv run pytest`):
- `test_quality.py`: replay both mock scenarios, merged by timestamp, through the QA logic with an
  in-memory store: `ideal` → 0 violations, 60 `PASSED`; `qa-violations` → 11 violations (5
  `MISSING_UPSTREAM`, 6 `FAILED_UPSTREAM`, at the machines in the `mock-data/README.md` table),
  statuses 48/4/11, PASS/FAIL per machine 56/4, 55/5, 59/1. Plus: out-of-order arrival within the
  grace period resolves cleanly; redelivering every message changes nothing.
- `test_integrity.py`: the 15 s boundary, never-seen machine, grace after start/resume, `$max`
  semantics (a late message doesn't move `last_seen_at` back).
- `test_models.py`: valid messages, each rejection reason.
- Optional `test_storage.py` against a real MongoDB, skipped unless `MONGODB_TEST_URI` is set
  (`docker run --rm -p 27017:27017 mongo:8`), to cover the actual queries.

Dockerfile: `python:3.12-slim` (TBD tag), uv binary copied from `ghcr.io/astral-sh/uv` (TBD), deps
layer `uv sync --frozen --no-dev --no-install-project`, then the code, non-root user,
`uvicorn pulseops.main:app --host 0.0.0.0 --port 8000`.

`k8s/base/service.yaml`: Deployment `pulseops-service`, 2 replicas, container name
`pulseops-service`, `MONGODB_URI` from `secretKeyRef` `pulseops-mongodb`/`uri`, the other settings
as plain `env`, readiness `httpGet /health`, liveness `tcpSocket: 8000`, resources,
`terminationGracePeriodSeconds: 30`; Service `pulseops-service` (ClusterIP, 8000). Base
kustomization adds it.

Verify:
```bash
docker build -t pulseops-service:dev service && kind load docker-image pulseops-service:dev --name pulseops
kubectl apply -k k8s/overlays/local && kubectl -n pulseops get pods -w        # 2/2 Ready, 0 restarts
kubectl port-forward -n pulseops svc/pulseops-service 8000:8000
curl -s localhost:8000/health; curl -s localhost:8000/machines                 # 200; 3 machines UP
kubectl -n pulseops exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group pulseops-service                                          # 6 partitions over 2 consumers, lag ~0
kubectl -n pulseops exec mongodb-0 -- mongosh pulseops --quiet --eval "db.test_results.countDocuments()"
```

Concepts: readiness vs liveness (and why `/health` must not be the liveness probe), Secret → env
var, Service and its endpoints (not-ready pods drop out), rolling updates and
`kubectl rollout restart` after reloading a `:dev` image.

### M6 — Replay Job

- `k8s/replay/replay-job.yaml`: Job with `metadata.generateName: replay-` (so every run is a new Job;
  works with `kubectl create`, not `apply`), image `pulseops-simulator:dev`, args
  `["replay", "/mock-data"]`, ConfigMap `mock-data-replay` mounted read-only at `/mock-data`,
  `backoffLimit: 2` (a retry republishes the same `event_id`s, which the service deduplicates),
  `ttlSecondsAfterFinished: 3600`, `restartPolicy: Never`.
- The CSVs come from a ConfigMap created with `kubectl` (Kustomize can't read `../../mock-data`, and
  the image stays independent of the data):
```bash
kubectl create configmap mock-data-replay -n pulseops --from-file=mock-data/qa-violations/ \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create -f k8s/replay/replay-job.yaml
kubectl -n pulseops logs -f -l app.kubernetes.io/name=replay
```
- Verify (wait out the 60 s grace after the Job ends), with mongosh on products `P-000101`–`P-000163`:
  11 violations, 5 `MISSING_UPSTREAM` (all `upstream_machine_down: false`) and 6 `FAILED_UPSTREAM`;
  statuses 48 `PASSED` / 4 `FAILED` / 11 `NON_CONFORMING`; PASS/FAIL per machine 56/4, 55/5, 59/1.
  Run the same Job again: every count unchanged (idempotency). Then load `ideal`: 0 violations, 60
  `PASSED`.
- Known side effect: replayed timestamps are from 2026-10-15, so `event_latency_seconds` spikes
  during a replay. Documented, not special-cased.
- Docs: README gets a "Replaying mock data" section and the layout gains `k8s/replay/`;
  `mock-data/README.md` points to it.

Concepts: Job vs Deployment, ConfigMap mounted as files (vs `envFrom` in M4), `generateName`,
`ttlSecondsAfterFinished`.

### M7 — Datadog Agent

- `datadog/values.yaml` (chart version pinned (TBD)): `datadog.apiKeyExistingSecret: datadog-secret`,
  `datadog.clusterName: pulseops`, `datadog.kubelet.tlsVerify: false`, kube-state-metrics core,
  Cluster Agent on, logs/APM off (TBD: other Kind needs).
- Annotations (pod templates; they only take effect once the Agent runs, and changing a pod template
  rolls the pods):
  - `k8s/base/service.yaml`: `ad.datadoghq.com/pulseops-service.checks` with `openmetrics`
    (`openmetrics_endpoint: http://%%host%%:8000/metrics`, `namespace: pulseops`,
    `raw_metric_prefix: pulseops_`, metrics listed without `_total`) and `http_check` on
    `http://%%host%%:8000/health` (TBD: exact option names, histogram handling).
  - `k8s/base/kafka.yaml`: `kafka_consumer` with `kafka_connect_str: %%host%%:9092`,
    `consumer_groups: {pulseops-service: {}}` (TBD).
  - `k8s/overlays/local/mongodb.yaml`: `mongo` with `hosts: ["%%host%%:27017"]`.
- Install exactly as in the README (namespace, `datadog-secret`, `helm repo add`, `helm install ...
  -f datadog/values.yaml --set datadog.site="$DD_SITE"`), plus `--version` (TBD).
- Verify: `kubectl -n datadog exec ds/datadog-agent -c agent -- agent status` lists `openmetrics`,
  `http_check`, `mongo`, `kafka_consumer` without errors (`agent configcheck` shows what
  autodiscovery resolved); Metrics Explorer shows `pulseops.tests.count` (summed over two
  `pod_name`s), `pulseops.machine_up`, `kafka.consumer_lag`, `mongodb.*`.

Concepts: DaemonSet, Helm chart vs Kustomize, autodiscovery annotations and template variables, why
checks target pod IPs (not the Service): each replica is scraped and probed on its own.

### M8 — Dashboard and monitors

- `datadog/dashboard.json`: one dashboard. **Line**: machine status (`max` by machine of
  `machine_up`), silence, throughput (sum by machine), pass rate by machine, violations by type,
  measurements (`avg` by machine). **Pipeline**: HTTP check, `consumer_paused` by pod, Kafka consumer
  lag, rejected messages, event latency, container restarts in `pulseops`, MongoDB connections and
  opcounters.
- `datadog/monitors/*.json`: *Machine down* (`machine_up` < 1, by machine, short window), *Pass-rate
  drop* (`tests{result:pass}` / `tests` with `.as_count()`, by machine), *QA violations* (> 0),
  *Service unhealthy* (HTTP check), *Consumer paused*, *Consumer lag*. Tag values are lowercase in
  queries (`result:pass`) (TBD: confirm; exact queries and windows).
- `datadog/apply.sh` (bash, curl, jq): API host from `DD_SITE`; for each file find the existing
  dashboard/monitor by title, `PUT` if found else `POST`; appends `DD_NOTIFY` to monitor messages
  when set. Running it twice creates nothing new.
- Verify: run `apply.sh` twice (no duplicates); dashboard populated; all monitors OK with defaults.

### M9 — External MongoDB

- `k8s/overlays/external-mongodb/` stays the base only (from M2).
- `datadog/values-external-mongodb.yaml`: the `mongo` check for the external host (TBD: node-Agent
  `confd` vs cluster check) with the read-only user's password taken from a Kubernetes Secret via the
  Agent's secret backend (TBD: exact values and RBAC).
- Test with a local stand-in instead of Atlas: `docker run -d --name pulseops-external-mongo
  -p 172.18.0.1:27017:27017` with root credentials (bound to the Kind gateway only, not the LAN),
  create a `readWrite` user for the service and a read-only `datadog` user, set `MONGODB_URI`
  (`mongodb://pulseops:...@172.18.0.1:27017/pulseops?authSource=pulseops`), and follow the README's
  steps. Check `ufw` doesn't block the Kind subnet. Verify the service is Ready and documents land in
  the external instance, then the Datadog `mongo` check, then switch back per the README.

### M10 — Demo scenarios, clean bring-up, docs

- Run every row of the README's "Demo scenarios" table and check the expected result (offline
  machine → `DOWN` in ~15 s + *Machine down*; skip/leak rates → violations; fail rate → *Pass-rate
  drop*; simulator scaled to 0 → all `DOWN`; delete a service pod → no `DOWN`, no lost results;
  MongoDB scaled to 0 → pods not ready but `RESTARTS` unchanged, `consumer_paused` 1, lag grows,
  then drains after scaling back and `test_results` holds every result).
- Clean-room check: `kind delete cluster --name pulseops`, then follow README "Bring the stack up"
  verbatim.
- Docs: README (prerequisites incl. inotify and uv, `DD_APP_KEY` and `apply.sh` in the bring-up, the
  replay section, any version/config deviations found while building, repository layout), CLAUDE.md
  (project state, commands: `uv run pytest`, simulator tests, replay, `apply.sh`),
  `mock-data/README.md`.

## Verification against the PoC goals

| PoC goal | Shown by |
|---|---|
| Silent machine flagged in ~15 s, Datadog alerts | `SIM_OFFLINE_MACHINES=machine-2`, *Machine down* |
| Skipped / failed-and-moved products recorded | M6 replay (exact counts), `SIM_SKIP_RATE`, `SIM_LEAK_RATE` |
| Results queryable by product, machine, time range | `/products/{id}`, mongosh queries on the M5 indexes |
| One dashboard for line + pipeline health | M8 |
| Several replicas; MongoDB outage pauses without loss or restart | M10 pod deletion and MongoDB scale-to-0 |
