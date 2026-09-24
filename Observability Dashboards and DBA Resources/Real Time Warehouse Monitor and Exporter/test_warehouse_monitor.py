"""
Tests for the monitor behavior the exporter relies on. Runs with pytest, or standalone:
python test_warehouse_monitor.py

Covers the control-warehouse skip for the dynamic p99 refresh, per-host isolation of
Query History failures, and the token_getter auth hook.
"""

import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from warehouse_monitor import (  # noqa: E402
    DatabricksSQLMonitor,
    DatabricksSQLMonitorSettings,
    _TokenGetterAuth,
)

TWO_HOSTS = {"wh1": "host-a.example.com", "wh2": "host-b.example.com"}


def _monitor(**kwargs):
    kwargs.setdefault("token_getter", lambda host: f"tok-{host}")
    kwargs.setdefault("warehouse_workspace_map", dict(TWO_HOSTS))
    return DatabricksSQLMonitor(settings=DatabricksSQLMonitorSettings(**kwargs), sinks=[])


def _healthy_status(host, ids):
    return {i: {"warehouse_state": "RUNNING", "warehouse_health_status": "HEALTHY"} for i in ids}


def test_p99_refresh_skipped_without_control_warehouse():
    m = _monitor()
    called = []
    m._fetch_p99_wall_ms_by_warehouse_from_system_table = lambda **k: called.append(k) or {}
    m._refresh_dynamic_lookbacks()
    assert called == []


def test_p99_refresh_runs_with_control_warehouse():
    m = _monitor(control_workspace_host="ctl.example.com", control_warehouse_id="ctl-wh")
    called = []
    m._fetch_p99_wall_ms_by_warehouse_from_system_table = lambda **k: called.append(k) or {}
    m._refresh_dynamic_lookbacks()
    assert len(called) == 1


def test_history_failure_is_isolated_to_its_host():
    m = _monitor()
    import pandas as pd

    def history(workspace_host, **_):
        if workspace_host == "host-a.example.com":
            raise RuntimeError("500 from history API")
        return pd.DataFrame()

    m._fetch_query_history_df_multi = history
    m._fetch_warehouse_status_batch = _healthy_status
    by_wh = {e.entity_id: e.metrics for e in m.poll_once()}

    # both warehouses still emit, with status intact
    assert set(by_wh) == {"wh1", "wh2"}
    assert by_wh["wh1"]["warehouse_state"] == "RUNNING"
    # only the failing host is flagged
    assert by_wh["wh1"]["query_history_ok"] == 0.0
    assert by_wh["wh2"]["query_history_ok"] == 1.0
    # the failed host's query metrics are zeroed, not missing
    assert by_wh["wh1"]["current_running_queries"] == 0


def test_token_getter_does_not_require_or_write_a_token():
    os.environ.pop("DATABRICKS_TOKEN", None)
    m = _monitor()
    assert "DATABRICKS_TOKEN" not in os.environ
    assert "Authorization" not in m.session.headers
    assert isinstance(m.session.auth, _TokenGetterAuth)


def test_token_getter_auth_sets_header_per_request_host():
    seen = []
    auth = _TokenGetterAuth(lambda host: seen.append(host) or f"tok-{host}")
    req = SimpleNamespace(url="https://host-a.example.com/api/2.0/sql/history/queries", headers={})
    out = auth(req)
    assert seen == ["host-a.example.com"]
    assert out.headers["Authorization"] == "Bearer tok-host-a.example.com"


def test_missing_auth_raises():
    try:
        DatabricksSQLMonitor(settings=DatabricksSQLMonitorSettings(warehouse_workspace_map=dict(TWO_HOSTS)), sinks=[])
    except ValueError:
        return
    raise AssertionError("expected ValueError when neither token nor token_getter is set")


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print(f"ok  {t.__name__}")
    print(f"\n{len(tests)} passed")
