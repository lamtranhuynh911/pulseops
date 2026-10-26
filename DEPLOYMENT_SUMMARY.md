# PulseOps deployment: what is completed

Status on 2026-10-26, branch `deploy-kind-stack` (in sync with `origin`, nothing uncommitted).

## At a glance

The whole stack runs on a local Kind cluster and was brought up from scratch by following the README.

- **Production line:** the simulator publishes data for machines 1–3, Kafka carries it, and two service replicas check it and store it in the external MongoDB.
- **Datadog:** the Agent watches all of it, and the dashboard and six monitors are in place.
- **Live check, after a host restart:** both service replicas answer `/health` with `200`, all three machines are `UP`, and every Datadog check is `OK` (OpenMetrics ×2, HTTP ×2, `kafka_consumer`, `mongo`).

Milestones M0–M10 are done, and every review has finished: no high- or medium-severity finding is left open.

## Milestones

| # | Milestone | Status | Notes |
|---|---|---|---|
| M0 | Prerequisites | Done | kind, kubectl, helm; inotify limits; `.env` |
| M1 | Kind cluster | Done | One node, `kindest/node:v1.37.0` pinned by digest |
| M2 | Kafka | Done | Single-broker KRaft StatefulSet; `kafka-topics` Job creates the two topics with 3 partitions each |
| M3 | MongoDB | Done | In-cluster overlay works (MongoDB 7.0); the deployment uses the external MongoDB (M9) |
| M4 | Simulator | Done | Live mode with fault injection through `k8s/base/simulator.env` |
| M5 | Service | Done | 2 replicas; consumer, integrity and QA checks, HTTP API, metrics |
| M6 | Replay Job | Done | Matches `mock-data/README.md` exactly; re-running changes nothing |
| M7 | Datadog Agent | Done | Helm chart 3.261.0 with autodiscovery annotations |
| M8 | Dashboard and monitors | Done | `datadog/apply.sh`, idempotent |
| M9 | External MongoDB | Done | Service data and the Datadog `mongo` check both work against it |
| M10 | Demo scenarios, clean-room bring-up, docs | Done | Every README demo was run and checked, see below |

## Verified results

| Scenario | Result |
|---|---|
| Replay `qa-violations` | 11 violations (5 `MISSING_UPSTREAM`, 6 `FAILED_UPSTREAM`); statuses 48 `PASSED` / 4 `FAILED` / 11 `NON_CONFORMING`; replaying it again changes no count |
| Replay `ideal` | 0 violations, 60 `PASSED` |
| `machine-2` offline for 150 s | `DOWN` 15 s after its last message, back `UP` on its first message after the restore. All 51 `MISSING_UPSTREAM` verdicts at machine-3 have `upstream_machine_down: true`, including those issued after it was back. *Machine down* fired and recovered |
| Skips, leaks and fail rate 0.2 (8 min) | Both violation types appear, with no false `DOWN`. Pass rates fell to 0.74–0.83, and *QA violations* and *Pass-rate drop* fired |
| Whole line offline (simulator scaled to 0) | All 3 machines `DOWN` within about 16 s; *Machine down* fired for each and recovered after the restore |
| MongoDB unreachable for 90 s (packets dropped) | Pods not ready, 0 restarts, `consumer_paused` = 1. Lag peaked at 219 and drained in 50 s; 157/157 results stored, 0 duplicates, 0 false violations |
| Service pod deleted (graceful, then forced) | 107/107 and 106/106 results stored, no false `DOWN` |
| Pod deleted just before a machine outage (retest after the monitor fix) | *Machine down* fired; *Service unhealthy* stayed quiet |
| Kafka broker restart | No rewind of committed offsets, no false `DOWN` |
| Clean-room bring-up (`kind delete cluster`, then the README) | Whole stack running in under 3 minutes, pods ready within 80 s, no restarts |

Memory: the cluster with the Datadog Agent uses about 2 GB, well within a 16 GB machine.

## Changes beyond the original plan

These go beyond or differ from the README as first written. The README and CLAUDE.md were updated for each.

- **MongoDB 7.0 instead of 8.x.** 8.0+ refuses to start on Linux kernel 6.19+ (SERVER-121912), and Kind pods run on the host kernel.
- **External MongoDB** (`k8s/overlays/external-mongodb`). The Datadog `mongo` check for it lives in `datadog/values-external-mongodb.yaml` and reads only the `datadog-mongodb` Secret.
- **aiokafka#1206 workaround.** `auto_offset_reset="none"`, and the service positions partitions itself, so a Kafka restart doesn't replay from the beginning.
- **Kafka-outage gating.** While the broker is unreachable, `/health` is `503` and the integrity and QA timers stop, so an outage doesn't mark every machine `DOWN`.
- **`SIM_LEAK_RATE` semantics.** The product fails at machine 1 or 2 but keeps moving, which gives `FAILED_UPSTREAM` at the next machine.
- **Datadog monitors that survive replaced pods.** Gauges use `.fill(null)`. *Service unhealthy* is now a metric monitor that needs a full minute of failures, and per-pod groups resolve once their pod is gone. `apply.sh` replaces a monitor whose type changed.

## Review findings fixed

Four independent review rounds; every confirmed finding is fixed, with regression tests.

- **Round 1:** records dropped during rebalances; network waits inside the rebalance listener; pending QA checks lost with a dying replica (now leased); one verdict per hop (unique index); stale status writes (evidence filters); every MongoDB error now holds the message instead of rejecting it.
- **Round 2 (monitors):** a deleted pod's stale `machine_up` hid *Machine down*, and a pod's shutdown left *Service unhealthy* stuck in Alert.
- **Round 3:**
  - A database holding duplicate verdicts from an older version now produces a clear error and stays paused, without deleting anything. The README has the cleanup command.
  - A timer verdict no longer loses its product status.
  - A restarted consumer now always starts paused.

- **Independent review of round 3 (`b4b933c`):** no high or medium defect. Each fix closes its failure scenario, and each new test fails without its fix. Two low README points were fixed: the cleanup command now uses `MONGODB_DATABASE`, and it says to finish the rollout first.
- **Claim checked and refuted:** "`use_process_start_time` recounts a running pod's counters when the Agent restarts." The Agent's `first_scrape_handler` reports existing counter values only for a process that started after the Agent did. Datadog's data agrees: no spike around the Agent restart at 16:24 on 10-24, and the largest 5-minute total in two days is 550, against an average of 371.

Tests: 46 service tests (with a real MongoDB) and 11 simulator tests pass.

## Commits on `deploy-kind-stack`

`7dda82a` service fixes · `dd459f7` simulator fixes · `f4ac439` Datadog monitors · `d0e27c7` docs · `8f1b43a` `.env.example` (yours) · `c0a39df` docs · `b4b933c` round-3 fixes and tests (yours).

## Open items

- **Low-severity findings from round 2, not verified:**
  - The fetch loop retries a broken Kafka coordinator forever.
  - Positioning one partition briefly holds up fetches for the others.
  - A half-started Kafka consumer isn't closed when it fails to start.
  - Product IDs could repeat above 1000 products per second.
- **Security decisions you made.** The `datadog` MongoDB user is read-only. Its password is a guessable value that is also committed in `.env.example`, which you chose to keep. The service connects to MongoDB as `root`; the README recommends a user with only `readWrite` on `pulseops`.
- **Older data.** The external database still holds the earlier test data and about 25 `processing_error` rejects from before the fix. They were left as you asked.
- **API access.** The port-forward for `http://localhost:8000` stopped with the session. Restart it with `kubectl port-forward -n pulseops svc/pulseops-service 8000:8000`.
