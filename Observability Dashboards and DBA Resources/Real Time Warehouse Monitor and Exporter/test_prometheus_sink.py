"""
Tests for PrometheusSink. Runs with pytest, or standalone:  python test_prometheus_sink.py

Guards the thing that bit us once: the sink must speak the monitor's REAL metric
schema (warehouse_health_status, warehouse_state, runtime_p95_sec, ...), encode
health/state correctly, skip free-text fields, and not leave stale series behind.
"""

import os
import sys
from datetime import datetime, timezone
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from prometheus_sink import PrometheusSink  # noqa: E402

LABELS = {"warehouse_id": "wh1", "workspace_host": "h", "monitor": "m"}


def _event(entity_id, metrics, host="h", monitor="m"):
    return SimpleNamespace(
        monitor_name=monitor,
        workspace_host=host,
        entity_id=entity_id,
        ts_utc=datetime.now(timezone.utc),
        metrics=metrics,
    )


def _healthy_metrics():
    return {
        "qps": 12.0,
        "runtime_p95_sec": 8.5,
        "queued_p99_sec": 3.4,
        "running_concurrency_p95": 6.0,
        "warehouse_current_clusters": 2,
        "warehouse_max_num_clusters": 4,
        "warehouse_health_status": "HEALTHY",
        "warehouse_state": "RUNNING",
        "warehouse_name": "analytics-wh",
        "warehouse_size": "MEDIUM",
        "queue_life_p95_sentence": "free text that must not become a gauge",
    }


def test_real_keys_and_encoding():
    sink = PrometheusSink()
    sink.emit([_event("wh1", _healthy_metrics())])
    g = sink.registry.get_sample_value

    # numeric metrics use the monitor's real names
    assert g("dbsql_qps", LABELS) == 12.0
    assert g("dbsql_runtime_p95_sec", LABELS) == 8.5
    assert g("dbsql_queued_p99_sec", LABELS) == 3.4
    assert g("dbsql_warehouse_current_clusters", LABELS) == 2.0
    assert g("dbsql_warehouse_max_num_clusters", LABELS) == 4.0

    # health/state are encoded (this is the bug the first version shipped)
    assert g("dbsql_warehouse_health_status", LABELS) == 0.0  # HEALTHY
    assert g("dbsql_warehouse_state", LABELS) == 2.0          # RUNNING

    # invented names from the first version must NOT exist
    assert g("dbsql_warehouse_health", LABELS) is None
    assert g("dbsql_queue_depth", LABELS) is None
    assert g("dbsql_num_clusters", LABELS) is None

    # free-text fields are skipped
    assert g("dbsql_warehouse_name", LABELS) is None
    assert g("dbsql_warehouse_size", LABELS) is None
    assert g("dbsql_queue_life_p95_sentence", LABELS) is None

    # freshness gauge exists
    assert g("dbsql_last_poll_unixtime", LABELS) is not None


def test_failed_status_flips_health_and_state_to_unknown():
    # mirrors the monitor's failure path: warehouse_state UNKNOWN + error, no health key
    sink = PrometheusSink()
    sink.emit([_event("wh1", _healthy_metrics())])
    assert sink.registry.get_sample_value("dbsql_warehouse_health_status", LABELS) == 0.0

    failed = {
        "qps": 0.0,
        "warehouse_state": "UNKNOWN",
        "warehouse_status_error": "HTTPError 500",
    }
    sink.emit([_event("wh1", failed)])
    # health must not stick at HEALTHY; it flips to unknown/unreachable
    assert sink.registry.get_sample_value("dbsql_warehouse_health_status", LABELS) == -1.0
    assert sink.registry.get_sample_value("dbsql_warehouse_state", LABELS) == -1.0


def test_stale_series_removed_when_warehouse_drops():
    sink = PrometheusSink()
    sink.emit([_event("wh1", _healthy_metrics())])
    assert sink.registry.get_sample_value("dbsql_qps", LABELS) == 12.0

    # next poll only sees wh2; wh1 must be removed, not left at its last value
    sink.emit([_event("wh2", _healthy_metrics())])
    assert sink.registry.get_sample_value("dbsql_qps", LABELS) is None
    wh2 = {"warehouse_id": "wh2", "workspace_host": "h", "monitor": "m"}
    assert sink.registry.get_sample_value("dbsql_qps", wh2) == 12.0


def test_missing_numeric_key_is_pruned_without_dropping_warehouse():
    # A warehouse that stays but stops reporting one metric (e.g. query_history_ok
    # flips a query metric absent) must not freeze that gauge at its last value.
    sink = PrometheusSink()
    sink.emit([_event("wh1", {"qps": 12.0, "runtime_p95_sec": 8.5})])
    assert sink.registry.get_sample_value("dbsql_qps", LABELS) == 12.0

    # same warehouse, next poll: qps absent, runtime still present
    sink.emit([_event("wh1", {"runtime_p95_sec": 9.0})])
    assert sink.registry.get_sample_value("dbsql_qps", LABELS) is None
    assert sink.registry.get_sample_value("dbsql_runtime_p95_sec", LABELS) == 9.0


def test_naive_timestamp_is_treated_as_utc():
    sink = PrometheusSink()
    naive = SimpleNamespace(
        monitor_name="m", workspace_host="h", entity_id="wh1",
        ts_utc=datetime(2026, 1, 1, 0, 0, 0),  # naive
        metrics={"qps": 1.0},
    )
    aware = SimpleNamespace(
        monitor_name="m", workspace_host="h", entity_id="wh1",
        ts_utc=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        metrics={"qps": 1.0},
    )
    sink.emit([naive])
    naive_ts = sink.registry.get_sample_value("dbsql_last_poll_unixtime", LABELS)
    sink.emit([aware])
    aware_ts = sink.registry.get_sample_value("dbsql_last_poll_unixtime", LABELS)
    assert naive_ts == aware_ts  # naive interpreted as UTC, no local-time shift


def test_info_series_carries_name_and_size_and_survives_failed_status():
    sink = PrometheusSink()
    sink.emit([_event("wh1", _healthy_metrics())])
    info = dict(LABELS, warehouse_name="analytics-wh", warehouse_size="MEDIUM")
    assert sink.registry.get_sample_value("dbsql_warehouse_info", info) == 1.0

    # a failed status poll omits name/size; the info series keeps the last known values
    sink.emit([_event("wh1", {"qps": 0.0, "warehouse_state": "UNKNOWN"})])
    assert sink.registry.get_sample_value("dbsql_warehouse_info", info) == 1.0

    # a dropped warehouse loses its info series too
    sink.emit([_event("wh2", _healthy_metrics())])
    assert sink.registry.get_sample_value("dbsql_warehouse_info", info) is None


def test_healthz_and_metrics_endpoints():
    import urllib.request

    sink = PrometheusSink(port=0)
    sink.start_server(addr="127.0.0.1")
    sink.emit([_event("wh1", _healthy_metrics())])
    base = f"http://127.0.0.1:{sink.port}"
    with urllib.request.urlopen(base + "/healthz", timeout=5) as r:
        assert r.status == 200 and r.read() == b"ok\n"
    with urllib.request.urlopen(base + "/metrics", timeout=5) as r:
        body = r.read().decode()
    assert 'dbsql_qps{monitor="m",warehouse_id="wh1",workspace_host="h"} 12.0' in body


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print(f"ok  {t.__name__}")
    print(f"\n{len(tests)} passed")
