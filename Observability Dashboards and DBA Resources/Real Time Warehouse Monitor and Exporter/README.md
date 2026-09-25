# Real Time Warehouse Monitor

Near-real-time observability for Databricks SQL Warehouses. The monitor polls the Databricks
REST APIs (Query History and Warehouses) on an interval, computes per-warehouse metrics, and
ships them to a pluggable sink. It runs either as a notebook or as a long-lived service.

## Why the API, not system tables

`system.query.history` records are typically available within about an hour, and system
tables are documented as not supporting real-time monitoring. Polling the Query History API
returns sub-second and runs no SQL on the warehouse, so it costs no warehouse DBUs to collect.
That freshness is the point of this monitor, and every sink below inherits it.

## Metrics it computes

Per warehouse, per poll: throughput (QPS, QPM), runtime percentiles, queue-wait percentiles,
running and queued concurrency, failure rate, spill rate, warehouse health and state, and
active versus max cluster count.

## Sinks

`warehouse_monitor.py` ships four sinks implementing a common `emit(events)` protocol. Choose
one or several when you construct the monitor.

- Console - prints events to stdout. Good for a notebook run or a quick look.
- Datadog - posts metrics to the Datadog API.
- Delta - appends events to a Delta table.
- Prometheus - serves metrics on a `/metrics` endpoint for Prometheus to scrape into Grafana.
  Wired end to end as a deployable exporter, documented separately in
  [PROMETHEUS_EXPORTER.md](./PROMETHEUS_EXPORTER.md).

## Running the monitor

Import the monitor, choose a sink, and poll. Example with the console sink:

```python
from warehouse_monitor import (
    DatabricksSQLMonitor,
    DatabricksSQLMonitorSettings,
    ConsoleSink,
    MonitorRunner,
)

settings = DatabricksSQLMonitorSettings(
    databricks_token="<token>",
    warehouse_workspace_map={"<warehouse_id>": "<workspace_host>"},
)
monitor = DatabricksSQLMonitor(settings=settings, sinks=[ConsoleSink()])
MonitorRunner(monitors=[monitor], poll_interval_seconds=30).run_forever()
```

For a token that refreshes (for example an OAuth service principal), pass
`token_getter=<callable host -> token>` instead of `databricks_token`. The monitor calls it on
every request and never writes a token to the environment.

No control warehouse is required. The dynamic p99 lookback refresh runs SQL against
`system.query.history` only when `control_workspace_host` and `control_warehouse_id` are both
set. Otherwise it is skipped and a fixed lookback is used, so the monitor never starts a
warehouse.

## Contents

- `warehouse_monitor.py` - the monitor (the former `Real Time DBSQL Warehouse Monitor`, now an
  importable module and still a runnable notebook), with the Console, Datadog, and Delta sinks.
- `prometheus_sink.py` - the Prometheus sink (per-warehouse gauges served on `/metrics`).
- `exporter.py`, `auth.py`, `Dockerfile`, `deploy/` - the deployable Prometheus exporter and
  its Kubernetes manifests. See [PROMETHEUS_EXPORTER.md](./PROMETHEUS_EXPORTER.md).
- `test_warehouse_monitor.py`, `test_prometheus_sink.py`, `test_auth.py` - unit tests.

## Tests

```bash
python test_warehouse_monitor.py   # control-warehouse skip, history isolation, auth hook
python test_prometheus_sink.py     # sink schema, encoding, pruning, /healthz
python test_auth.py                # token cache, refresh, per-host tokens
# or run them all with: pytest
```
