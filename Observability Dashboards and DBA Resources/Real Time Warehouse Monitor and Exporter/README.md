# Real Time Warehouse Monitor and Exporter

Near-real-time observability for Databricks SQL Warehouses. A monitor polls the Databricks
REST APIs (Query History and Warehouses) on an interval and computes per-warehouse metrics,
throughput, queue and runtime percentiles, concurrency, failure and spill rates, health, and
state. It ships those metrics to pluggable sinks and runs either as a notebook or as a
long-lived service.

The monitor's built-in sinks are Console, Datadog, Delta, and the Prometheus sink added here.
This folder wires that Prometheus path end to end, an exporter that serves the metrics on a
`/metrics` endpoint for Prometheus to scrape into Grafana. That path is the fresh, API-based
counterpart to the system-tables dashboard advisors in this repo, for teams whose
observability lives in Prometheus and Grafana rather than a Databricks-native dashboard.

The monitor is usable on its own with any sink. The sections below focus on the Prometheus
exporter, since that is what this folder adds and deploys.

## Contents

- `warehouse_monitor.py` — the poller (the former `Real Time DBSQL Warehouse Monitor`, now
  an importable module and still a runnable notebook). Calls the Query History API and the
  Warehouses Get API, computes per-warehouse metrics, and ships them to a pluggable set of
  sinks (Console, Datadog, Delta, and the Prometheus sink below).
- `prometheus_sink.py` — a `PrometheusSink` implementing the monitor's `emit(events)`
  protocol. Builds per-warehouse gauges and serves `/metrics`.
- `exporter.py` — the entrypoint. Wires the monitor to the sink, installs auth, and runs a
  resilient poll loop. Configured via environment variables.
- `auth.py` — a static-token getter for local testing and an OAuth M2M (service principal)
  provider that caches and refreshes a token per workspace host.
- `test_prometheus_sink.py`, `test_auth.py` — unit tests.
- `Dockerfile`, `deploy/` — container image and Kubernetes manifests (Deployment, Service,
  ServiceMonitor, secret example, example Prometheus alert rules).

## Why the API, not system tables

`system.query.history` records are typically available within about an hour, and system
tables are documented as not supporting real-time monitoring. Polling the Query History API
returns sub-second and runs no SQL on the warehouse, so it costs no warehouse DBUs to
collect. That is the freshness this exporter is built around.

## How the Prometheus exporter delivers (scrape, not push)

This applies to the Prometheus sink only. The monitor's other sinks push, Datadog posts to
its API and Delta writes rows to a table. The Prometheus path is instead scraped. The
exporter is a long-running service that, on its poll interval, calls the APIs, computes
metrics, and writes them into in-memory Prometheus gauges. Prometheus scrapes `/metrics`, and
hitting it does not trigger an API call, it just serializes the latest values. Freshness is
bounded by the poll interval, not the scrape.

```
Query History API + Warehouses API
        |  (poll every 15-30s, control-plane REST, no warehouse compute)
        v
exporter (warehouse_monitor + prometheus_sink)
        |  exposes /metrics
        v
Prometheus  --scrape-->  Grafana
```

## Quickstart

```bash
pip install -r requirements.txt

export WAREHOUSE_WORKSPACE_MAP='{"<warehouse_id>": "<workspace_host>"}'
export POLL_INTERVAL_SECONDS=30
export METRICS_PORT=9877

# Auth, pick one. Service principal (for deployment):
export DATABRICKS_CLIENT_ID="<sp application id>"
export DATABRICKS_CLIENT_SECRET="<sp oauth secret>"
# ...or a static token (convenient for local testing):
# export DATABRICKS_TOKEN="<PAT or bearer with read access>"

python exporter.py
# then, in another shell:
curl http://localhost:9877/metrics
```

To see the sink render without any credentials, run its built-in demo:

```bash
python prometheus_sink.py     # binds 127.0.0.1, emits a synthetic warehouse event
curl http://127.0.0.1:9877/metrics
```

## Configuration

| Env var | Required | Default | Meaning |
|---|---|---|---|
| `DATABRICKS_CLIENT_ID` | one auth mode | | Service principal application (client) id for OAuth M2M. |
| `DATABRICKS_CLIENT_SECRET` | one auth mode | | Service principal OAuth secret. Pair with the client id. |
| `DATABRICKS_OAUTH_SCOPE` | no | `all-apis` | OAuth scope requested for M2M tokens. |
| `DATABRICKS_TOKEN` | one auth mode | | Static PAT/bearer with read access. Alternative to the client id/secret, mainly for local testing. |
| `WAREHOUSE_WORKSPACE_MAP` | yes | | JSON of `{"<warehouse_id>": "<workspace_host>"}`. Start with one, widen later. |
| `POLL_INTERVAL_SECONDS` | no | 30 | How often to poll the APIs. |
| `METRICS_PORT` | no | 9877 | Port for the `/metrics` endpoint. |
| `METRICS_NAMESPACE` | no | `dbsql` | Metric name prefix. |
| `MAX_BACKOFF_SECONDS` | no | 300 | Cap on the exponential backoff after a failed poll. |

No control warehouse is required. The monitor's dynamic p99 lookback refresh (which runs SQL
against `system.query.history`) is skipped unless `control_workspace_host` and
`control_warehouse_id` are set; the exporter uses a fixed lookback instead.

## Run the tests

```bash
python test_prometheus_sink.py     # sink schema, encoding, stale-series pruning
python test_auth.py                # token cache, refresh, per-host injection
# or run both with: pytest
```

## Metrics

Every numeric metric becomes a gauge `dbsql_<key>`, labeled by `warehouse_id`,
`workspace_host`, and `monitor`. Examples: `dbsql_qps`, `dbsql_qpm`,
`dbsql_current_running_queries`, `dbsql_queued_p99_sec`, `dbsql_runtime_p95_sec`,
`dbsql_running_concurrency_p95`, `dbsql_failure_rate_pct`, `dbsql_spilled_query_pct`,
`dbsql_warehouse_current_clusters`, `dbsql_warehouse_max_num_clusters`.

Encoded string fields:

- `dbsql_warehouse_health_status`: `HEALTHY=0`, `DEGRADED=1`, `FAILED=2`,
  unknown/unreachable=`-1`. Alert on `>= 1`. Set every poll, so a failed status call flips
  it to `-1` rather than holding the last good value.
- `dbsql_warehouse_state`: `STOPPED=0`, `STARTING=1`, `RUNNING=2`, `STOPPING=3`,
  `DELETING=4`, `DELETED=5`, unknown=`-1`.

Freshness: `dbsql_last_poll_unixtime` per warehouse. Alert on
`time() - dbsql_last_poll_unixtime` alongside the scrape `up` signal to catch a stopped
exporter. Stale series are pruned each poll.

Feed health: `dbsql_query_history_ok` per warehouse (`1` ok, `0` the Query History API failed
for that warehouse's workspace on the last poll). A history failure is isolated to its
workspace host, so those warehouses still report warehouse status with query metrics zeroed,
other workspaces keep reporting, and `dbsql_last_poll_unixtime` keeps advancing. Alert on
`dbsql_query_history_ok == 0` so a broken feed does not read as a genuinely idle warehouse.
See `deploy/prometheus-alerts.yaml`.

## Service principal setup (least-privilege)

The deployment authenticates as a service principal over OAuth M2M. The minimal, non-admin
permission it needs is CAN MONITOR on each SQL warehouse it watches.

1. Create a service principal (or reuse one) and add it to the workspace.
2. Generate an OAuth secret for it (account console → the service principal → OAuth secrets).
   This yields a client id and a client secret.
3. On each SQL warehouse to be monitored, grant the service principal CAN MONITOR (SQL
   Warehouses → the warehouse → Permissions), with no ability to start, stop, or run.
4. Inject the client id and secret as a k8s secret and expose them as `DATABRICKS_CLIENT_ID`
   / `DATABRICKS_CLIENT_SECRET`.

> **Validate the service principal before relying on the query metrics.** Warehouses Get
> returns the same details for anyone with CAN MONITOR, but Query History is identity-scoped,
> and there are cases where a service principal sees only its own queries even with a
> warehouse grant. If that happens, the query gauges (`dbsql_qps`, `dbsql_runtime_*`,
> `dbsql_queued_*`) read empty or near-zero while `dbsql_warehouse_health_status` and
> `dbsql_warehouse_state` look fine. Confirm with the actual SP against a warehouse that has
> other users' traffic, and check that `dbsql_qps` reflects it.

## Deploying in Kubernetes

Build the image with the `Dockerfile`, then apply the manifests in `deploy/`:

```bash
docker build -t <registry>/dbsql-warehouse-exporter:<tag> .
docker push <registry>/dbsql-warehouse-exporter:<tag>

kubectl create secret generic dbsql-exporter-sp -n monitoring \
  --from-literal=DATABRICKS_CLIENT_ID=<sp-application-id> \
  --from-literal=DATABRICKS_CLIENT_SECRET=<sp-oauth-secret>

# edit deploy/deployment.yaml: set the image and WAREHOUSE_WORKSPACE_MAP
kubectl apply -f deploy/deployment.yaml -f deploy/service.yaml \
  -f deploy/servicemonitor.yaml -f deploy/prometheus-alerts.yaml
```

- `deploy/deployment.yaml` runs a single non-root replica (a second would double-poll and duplicate series).
- `deploy/service.yaml` is ClusterIP only, keeping `/metrics` off any public load balancer since the payload includes warehouse IDs and workspace hosts.
- `deploy/servicemonitor.yaml` wires it to a Prometheus Operator install; use a plain scrape job if you do not run the operator.
- `deploy/prometheus-alerts.yaml` ships example rules (health, staleness, query-history-down, queue depth, autoscale saturation) with thresholds to tune.
- `deploy/secret.example.yaml` documents the secret shape; do not commit real credentials.

Metrics are attributed per warehouse. The APIs report queries against a warehouse rather than
an internal autoscaling cluster, so there is no per-cluster breakdown.
