"""
PrometheusSink for the Real Time DBSQL Warehouse Monitor.

Plugs into the monitor's existing Sink protocol, a single method `emit(events)`.
On each poll the monitor hands us a batch of MetricEvent objects; we update
in-memory Prometheus gauges, labeled per warehouse, and a background HTTP server
exposes /metrics for Prometheus to scrape.

Design (scrape / pull):
  The exporter is a long-running service. It holds the latest gauge values in
  process memory and Prometheus scrapes /metrics on its own interval. Nothing is
  pushed, and hitting /metrics does not trigger a Databricks API call; it just
  serializes what the poll loop last wrote. Freshness is bounded by the monitor's
  poll interval, not the scrape.

Key names here are taken from the monitor's actual output (`_compute_metrics`,
`_snapshot_query_counts_from_history`, and `_fetch_warehouse_status`), not invented.
The sink reads event fields by attribute so it does not need to import the monitor.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from prometheus_client import CollectorRegistry, Gauge, start_http_server


# The monitor emits warehouse_health_status as a string. Encode it numerically so it
# can be graphed and alerted on. Missing/unknown (e.g. a failed status poll, which
# omits the key) maps to -1 so a previously-HEALTHY series does not stick.
_HEALTH_KEY = "warehouse_health_status"
_HEALTH_ENCODING: Dict[str, float] = {"HEALTHY": 0.0, "DEGRADED": 1.0, "FAILED": 2.0}

# warehouse_state is also a string; encode it. On a failed status poll the monitor
# emits warehouse_state="UNKNOWN". Anything unrecognized also maps to -1.
_STATE_KEY = "warehouse_state"
_STATE_ENCODING: Dict[str, float] = {
    "STOPPED": 0.0,
    "STARTING": 1.0,
    "RUNNING": 2.0,
    "STOPPING": 3.0,
    "DELETING": 4.0,
    "DELETED": 5.0,
    "UNKNOWN": -1.0,
}

# Handled explicitly below (not in the generic numeric loop).
_SPECIAL_KEYS = {_HEALTH_KEY, _STATE_KEY}

# String / non-metric fields that are not exported as gauges.
# (warehouse_name / warehouse_size could become labels later; see README.)
_SKIP_KEYS = {
    "queue_life_p95_sentence",
    "warehouse_name",
    "warehouse_size",
    "warehouse_status_error",
}

_LABELS: Tuple[str, ...] = ("warehouse_id", "workspace_host", "monitor")
_LabelTuple = Tuple[str, str, str]


@dataclass
class PrometheusSink:
    """A Sink that exposes monitor metrics as Prometheus gauges over /metrics.

    Example:
        sink = PrometheusSink(namespace="dbsql", port=9877)
        sink.start_server()                       # start /metrics once, at startup
        monitor = DatabricksSQLMonitor(settings=settings, sinks=[sink])
        MonitorRunner(monitors=[monitor], poll_interval_seconds=30).run_forever()
    """

    namespace: str = "dbsql"
    port: int = 9877
    registry: CollectorRegistry = field(default_factory=CollectorRegistry)

    # internal state
    _gauges: Dict[str, Gauge] = field(default_factory=dict, repr=False)
    _active: Dict[str, Set[_LabelTuple]] = field(default_factory=lambda: defaultdict(set), repr=False)
    _server_started: bool = field(default=False, repr=False)

    # ------------------------------------------------------------------ server
    def start_server(self, port: Optional[int] = None, addr: str = "0.0.0.0") -> None:
        """Start the /metrics HTTP endpoint. Safe to call once; later calls no-op.

        addr defaults to 0.0.0.0 for in-cluster scraping behind a ServiceMonitor.
        Bind 127.0.0.1 when running locally, and keep /metrics off the public
        network in EKS; the payload includes warehouse IDs and workspace hosts.
        """
        if self._server_started:
            return
        if port is not None:
            self.port = port
        start_http_server(self.port, addr=addr, registry=self.registry)
        self._server_started = True

    # ------------------------------------------------------------ Sink protocol
    def emit(self, events: List[Any]) -> None:
        """Receive a batch of MetricEvents from the monitor and update gauges."""
        if not events:
            return

        seen: Dict[str, Set[_LabelTuple]] = defaultdict(set)

        for e in events:
            wid = str(getattr(e, "entity_id", "") or "")
            host = str(getattr(e, "workspace_host", "") or "")
            mon = str(getattr(e, "monitor_name", "") or "")
            label_tuple: _LabelTuple = (wid, host, mon)
            labels = {"warehouse_id": wid, "workspace_host": host, "monitor": mon}
            metrics: Dict[str, Any] = getattr(e, "metrics", {}) or {}

            # numeric metrics
            for key, value in metrics.items():
                if key in _SKIP_KEYS or key in _SPECIAL_KEYS:
                    continue
                num = self._coerce(value)
                if num is None:
                    continue  # dropped this poll; stale-cleanup removes any prior series
                name = self._metric_name(key)
                self._gauge(name, f"DBSQL warehouse monitor metric: {key}").labels(**labels).set(num)
                seen[name].add(label_tuple)

            # health: always set, so a failed/absent status flips to -1 instead of sticking
            hname = self._metric_name(_HEALTH_KEY)
            hval = _HEALTH_ENCODING.get(str(metrics.get(_HEALTH_KEY, "")).strip().upper(), -1.0)
            self._gauge(hname, "warehouse health: HEALTHY=0 DEGRADED=1 FAILED=2 unknown/unreachable=-1").labels(**labels).set(hval)
            seen[hname].add(label_tuple)

            # state: always set
            sname = self._metric_name(_STATE_KEY)
            sval = _STATE_ENCODING.get(str(metrics.get(_STATE_KEY, "")).strip().upper(), -1.0)
            self._gauge(sname, "warehouse state: STOPPED=0 STARTING=1 RUNNING=2 STOPPING=3 DELETING=4 DELETED=5 unknown=-1").labels(**labels).set(sval)
            seen[sname].add(label_tuple)

            # freshness: alert on (time() - this) to catch a stopped exporter
            fname = self._metric_name("last_poll_unixtime")
            self._gauge(fname, "unix timestamp of the last poll that produced this warehouse's sample").labels(**labels).set(self._event_ts(e))
            seen[fname].add(label_tuple)

        self._prune(seen)

    # ------------------------------------------------------------------ helpers
    def _prune(self, seen: Dict[str, Set[_LabelTuple]]) -> None:
        """Remove label sets not present this poll (dropped warehouses, absent metrics)."""
        for name, gauge in self._gauges.items():
            stale = self._active.get(name, set()) - seen.get(name, set())
            for lt in stale:
                try:
                    gauge.remove(*lt)
                except KeyError:
                    pass
            self._active[name] = seen.get(name, set())

    @staticmethod
    def _coerce(value: Any) -> Optional[float]:
        if isinstance(value, bool):
            return 1.0 if value else 0.0
        if isinstance(value, (int, float)):
            return float(value)
        return None  # strings handled explicitly; None/other are skipped

    @staticmethod
    def _event_ts(e: Any) -> float:
        ts = getattr(e, "ts_utc", None)
        if ts is None:
            return time.time()
        try:
            if ts.tzinfo is None:  # treat naive as UTC, not local
                return ts.replace(tzinfo=timezone.utc).timestamp()
            return ts.timestamp()
        except Exception:
            return time.time()

    def _metric_name(self, key: str) -> str:
        safe = "".join(c if (c.isalnum() or c == "_") else "_" for c in key)
        return f"{self.namespace}_{safe}"

    def _gauge(self, name: str, help_text: str) -> Gauge:
        g = self._gauges.get(name)
        if g is None:
            g = Gauge(name, help_text, labelnames=_LABELS, registry=self.registry)
            self._gauges[name] = g
        return g


if __name__ == "__main__":
    # Runnable demo with a synthetic event using the monitor's REAL metric keys, so
    # you can see /metrics before wiring the real monitor.
    #   python "prometheus_sink.py"   then   curl http://localhost:9877/metrics
    from datetime import datetime
    from types import SimpleNamespace

    sink = PrometheusSink(namespace="dbsql", port=9877)
    sink.start_server(addr="127.0.0.1")  # local only for the demo

    demo_event = SimpleNamespace(
        monitor_name="databricks.warehouse",
        workspace_host="example.cloud.databricks.com",
        entity_id="abc123warehouse",
        ts_utc=datetime.now(timezone.utc),
        metrics={
            "qps": 12.0,
            "qpm": 720.0,
            "current_running_queries": 4,
            "current_queued_queries": 2,
            "queued_p95_sec": 1.2,
            "queued_p99_sec": 3.4,
            "runtime_p95_sec": 8.5,
            "runtime_p99_sec": 22.0,
            "running_concurrency_p95": 6.0,
            "queued_concurrency_p95": 2.0,
            "failure_rate_pct": 0.0,
            "spilled_query_pct": 1.5,
            "queue_life_pct_p95": 12.0,
            "warehouse_current_clusters": 2,
            "warehouse_max_num_clusters": 4,
            "warehouse_min_num_clusters": 1,
            "warehouse_active_sessions": 5,
            "warehouse_auto_stop_mins": 10,
            "warehouse_health_status": "HEALTHY",
            "warehouse_state": "RUNNING",
            "warehouse_name": "analytics-wh",
            "warehouse_size": "MEDIUM",
            "queue_life_p95_sentence": "95% of completed queries spent <= 12% of their life queued.",
        },
    )

    print("Serving metrics at http://127.0.0.1:9877/metrics  (Ctrl-C to stop)")
    while True:
        sink.emit([demo_event])
        time.sleep(15)
