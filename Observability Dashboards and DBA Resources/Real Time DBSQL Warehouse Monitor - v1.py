# Databricks notebook source
import os
import time
from dataclasses import dataclass, field
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Tuple, Optional, Protocol

import requests
import pandas as pd


# =========================================================
# Core types
# =========================================================

@dataclass
class MetricEvent:
    monitor_name: str
    workspace_host: str
    entity_id: str              # e.g. warehouse_id
    ts_utc: datetime
    metrics: Dict[str, Any]


class Sink(Protocol):
    """A sink consumes metric events."""
    def emit(self, events: List[MetricEvent]) -> None: ...


class Monitor(Protocol):
    """A monitor produces metric events."""
    name: str
    def poll_once(self) -> List[MetricEvent]: ...


# =========================================================
# Sink implementations
# =========================================================

@dataclass
class ConsoleSink:
    def emit(self, events: List[MetricEvent]) -> None:
        for e in events:
            print(f"\n[{e.monitor_name}] {e.entity_id} @ {e.ts_utc.isoformat()} (workspace={e.workspace_host})")
            for k, v in sorted(e.metrics.items()):
                print(f"  {k}: {v}")


@dataclass
class DatadogSink:
    api_key: str
    api_url: str = "https://api.datadoghq.com/api/v1/series"
    timeout_sec: int = 60
    session: requests.Session = field(default_factory=requests.Session)

    def emit(self, events: List[MetricEvent]) -> None:
        if not events:
            return

        ts = int(time.time())
        series = []
        for e in events:
            for k, v in e.metrics.items():
                if isinstance(v, (int, float)):
                    series.append({
                        "metric": f"{e.monitor_name}.{k}",
                        "points": [[ts, float(v)]],
                        "tags": [
                            f"workspace:{e.workspace_host}",
                            f"entity_id:{e.entity_id}",
                            f"monitor:{e.monitor_name}",
                        ],
                    })

        if not series:
            return

        headers = {"Content-Type": "application/json", "DD-API-KEY": self.api_key}
        self.session.post(self.api_url, json={"series": series}, headers=headers, timeout=self.timeout_sec)


@dataclass
class DeltaTableSink:
    table: str
    # expects to run inside Databricks with spark present

    def emit(self, events: List[MetricEvent]) -> None:
        if not events:
            return

        from pyspark.sql import Row

        rows = []
        for e in events:
            row_dict = {
                "monitor_name": e.monitor_name,
                "workspace_host": e.workspace_host,
                "entity_id": e.entity_id,
                "ts_utc": e.ts_utc.isoformat(),
            }
            for k, v in e.metrics.items():
                row_dict[k] = float(v) if isinstance(v, (int, float)) else str(v)
            rows.append(Row(**row_dict))

        df = spark.createDataFrame(rows)
        df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(self.table)


# =========================================================
# Databricks SQL monitor settings
# =========================================================

@dataclass
class DatabricksSQLMonitorSettings:
    # identity
    name: str = "databricks.warehouse"

    # auth
    databricks_token: str = ""

    # poll timing
    poll_interval_seconds: int = 60

    # analysis window
    rolling_window_minutes: int = 10
    lookback_buffer_minutes: int = 2

    # long lookback tuning
    long_lb_min_minutes: int = 30
    long_lb_max_minutes: int = 24 * 60
    long_lb_safety_factor: float = 2.0
    long_lb_extra_padding_minutes: int = 10

    # refresh p99 every N polls
    p99_refresh_every_n_polls: int = 100
    p99_horizon_hours: int = 24
    wait_timeout_sec: int = 30

    # control plane for system.query.history query
    control_workspace_host: str = ""
    control_warehouse_id: str = ""

    # which warehouses to monitor
    warehouse_workspace_map: Dict[str, str] = field(default_factory=dict)

    # perf
    max_results_per_page: int = 1000
    http_timeout_sec: int = 60
    max_workers_status: int = 16

    # semantics
    qps_mode: str = "arrival"   # "arrival" or "completion"

    # epoch sanity clamp
    min_epoch_ms: int = 946684800000      # 2000-01-01
    max_epoch_ms: int = 4102444800000     # 2100-01-01


# =========================================================
# Databricks SQL monitor implementation
# =========================================================

class DatabricksSQLMonitor:
    TERMINAL_STATUSES = {"FINISHED", "FAILED", "CANCELED"}
    SUCCESS_STATUS = "FINISHED"
    RUNNING_STATES = {"STARTED", "COMPILING", "COMPILED", "RUNNING"}
    QUEUED_STATE = "QUEUED"

    def __init__(self, settings: DatabricksSQLMonitorSettings, sinks: List[Sink]):
        self.settings = settings
        self.name = settings.name
        self.sinks = sinks

        if not settings.databricks_token:
            raise ValueError("Databricks token is required in settings.databricks_token")

        os.environ["DATABRICKS_TOKEN"] = os.environ.get("DATABRICKS_TOKEN", settings.databricks_token)

        self.session = requests.Session()
        self.session.headers.update(self._auth_headers())

        self.poll_count = 0
        self.long_lookback_by_wh = {wid: settings.long_lb_min_minutes for wid in settings.warehouse_workspace_map.keys()}

        self.grouped = self._group_warehouses_by_workspace(settings.warehouse_workspace_map)
        self.all_warehouse_ids = list(settings.warehouse_workspace_map.keys())

    # ----------------------------
    # helpers
    # ----------------------------
    @staticmethod
    def _utc_now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _to_ms(dt: datetime) -> int:
        return int(dt.timestamp() * 1000)

    @staticmethod
    def _to_utc_ts(dt: datetime) -> pd.Timestamp:
        ts = pd.Timestamp(dt)
        if ts.tzinfo is None:
            return ts.tz_localize("UTC")
        return ts.tz_convert("UTC")

    @staticmethod
    def _fast_json_loads(content: bytes) -> Dict[str, Any]:
        try:
            import orjson
            return orjson.loads(content)
        except Exception:
            import json
            return json.loads(content)

    def _auth_headers(self) -> Dict[str, str]:
        token = os.environ.get("DATABRICKS_TOKEN")
        if not token:
            raise RuntimeError("Missing DATABRICKS_TOKEN env var.")
        return {"Authorization": f"Bearer {token}", "Accept-Encoding": "gzip"}

    @staticmethod
    def _group_warehouses_by_workspace(warehouse_workspace_map: Dict[str, str]) -> Dict[str, List[str]]:
        grouped = defaultdict(list)
        for wid, host in warehouse_workspace_map.items():
            grouped[host].append(wid)
        return dict(grouped)

    @staticmethod
    def _sql_string_list(values: List[str]) -> str:
        if not values:
            return "('')"
        escaped = [v.replace("'", "''") for v in values]
        return "(" + ",".join([f"'{v}'" for v in escaped]) + ")"

    # ----------------------------
    # main public API
    # ----------------------------
    def poll_once(self) -> List[MetricEvent]:
        s = self.settings
        self.poll_count += 1

        window_end = self._utc_now()
        window_start = window_end - timedelta(minutes=s.rolling_window_minutes)

        # 1) refresh dynamic lookback occasionally (batched)
        if self.poll_count == 1 or (self.poll_count % s.p99_refresh_every_n_polls == 0):
            self._refresh_dynamic_lookbacks()

        # 2) fetch history and status per workspace host, compute metrics per warehouse
        events: List[MetricEvent] = []

        for workspace_host, warehouse_ids in self.grouped.items():
            max_lb = max(self.long_lookback_by_wh.get(wid, s.long_lb_min_minutes) for wid in warehouse_ids)
            fetch_start = window_start - timedelta(minutes=(s.lookback_buffer_minutes + max_lb))
            fetch_end = window_end

            df_hist = self._fetch_query_history_df_multi(
                workspace_host=workspace_host,
                warehouse_ids=warehouse_ids,
                start_time=fetch_start,
                end_time=fetch_end,
                include_metrics=True,
            )

            status_map = self._fetch_warehouse_status_batch(workspace_host, warehouse_ids)

            by_wh = dict(tuple(df_hist.groupby("warehouse_id", dropna=False))) if df_hist is not None and not df_hist.empty else {}

            for wid in warehouse_ids:
                df_wh = by_wh.get(wid, pd.DataFrame()).copy()

                metrics = self._compute_metrics(df_wh, window_start, window_end)
                metrics.update(self._snapshot_query_counts_from_history(df_wh))
                metrics.update(status_map.get(wid, {}))

                # Debug context (useful for UI)
                metrics["analysis_window_minutes"] = float(s.rolling_window_minutes)
                metrics["long_query_lookback_minutes"] = float(self.long_lookback_by_wh.get(wid, s.long_lb_min_minutes))
                metrics["lookback_buffer_minutes"] = float(s.lookback_buffer_minutes)
                metrics["rows_fetched_history"] = float(len(df_wh)) if df_wh is not None else 0.0
                metrics["rows_fetched_history_batch"] = float(len(df_hist)) if df_hist is not None else 0.0
                metrics["poll_count"] = float(self.poll_count)

                events.append(MetricEvent(
                    monitor_name=self.name,
                    workspace_host=workspace_host,
                    entity_id=wid,
                    ts_utc=window_end,
                    metrics=metrics
                ))

        return events

    def emit(self, events: List[MetricEvent]) -> None:
        for sink in self.sinks:
            sink.emit(events)

    # ----------------------------
    # dynamic lookback
    # ----------------------------
    def _refresh_dynamic_lookbacks(self) -> None:
        s = self.settings
        p99_map = self._fetch_p99_wall_ms_by_warehouse_from_system_table(
            control_workspace_url=s.control_workspace_host,
            control_warehouse_id=s.control_warehouse_id,
            warehouse_ids_to_measure=self.all_warehouse_ids,
            horizon_hours=s.p99_horizon_hours,
            wait_timeout_sec=s.wait_timeout_sec,
        )

        for wid in self.all_warehouse_ids:
            p99 = p99_map.get(wid, float("nan"))
            self.long_lookback_by_wh[wid] = self._compute_dynamic_lookback_minutes(
                p99_wall_ms=p99,
                poll_interval_seconds=s.poll_interval_seconds,
            )

    def _compute_dynamic_lookback_minutes(self, p99_wall_ms: float, poll_interval_seconds: int) -> int:
        s = self.settings
        if p99_wall_ms != p99_wall_ms:  # NaN
            return s.long_lb_min_minutes

        p99_min = p99_wall_ms / 1000.0 / 60.0
        base = int(s.long_lb_safety_factor * p99_min + s.long_lb_extra_padding_minutes)

        poll_interval_min = max(1, int(poll_interval_seconds / 60))
        base = max(base, 2 * poll_interval_min, s.long_lb_min_minutes)
        return min(base, s.long_lb_max_minutes)

    def _fetch_p99_wall_ms_by_warehouse_from_system_table(
        self,
        control_workspace_url: str,
        control_warehouse_id: str,
        warehouse_ids_to_measure: List[str],
        horizon_hours: int,
        wait_timeout_sec: int,
    ) -> Dict[str, float]:
        wh_in = self._sql_string_list(warehouse_ids_to_measure)

        sql = f"""
        WITH base AS (
          SELECT
            compute.warehouse_id AS warehouse_id,
            unix_millis(end_time) - unix_millis(start_time) AS wall_ms
          FROM system.query.history
          WHERE compute.warehouse_id IN {wh_in}
            AND execution_status IN ('FINISHED','FAILED','CANCELED')
            AND end_time >= now() - INTERVAL {int(horizon_hours)} HOURS
            AND end_time IS NOT NULL
            AND start_time IS NOT NULL
            AND unix_millis(end_time) >= unix_millis(start_time)
        )
        SELECT
          warehouse_id,
          percentile_approx(wall_ms, 0.99) AS p99_wall_ms
        FROM base
        GROUP BY warehouse_id
        """

        url = f"https://{control_workspace_url}/api/2.0/sql/statements"
        payload = {
            "warehouse_id": control_warehouse_id,
            "statement": sql,
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
            "wait_timeout": f"{wait_timeout_sec}s",
        }

        r = self.session.post(url, json=payload, timeout=wait_timeout_sec + 10)
        r.raise_for_status()
        data = r.json()

        state = (data.get("status") or {}).get("state")
        statement_id = data.get("statement_id")

        if state != "SUCCEEDED" and statement_id:
            status_url = f"https://{control_workspace_url}/api/2.0/sql/statements/{statement_id}"
            for _ in range(12):
                sresp = self.session.get(status_url, timeout=wait_timeout_sec + 10)
                sresp.raise_for_status()
                sj = sresp.json()
                st = (sj.get("status") or {}).get("state")
                if st == "SUCCEEDED":
                    data = sj
                    break
                if st in ("FAILED", "CANCELED"):
                    return {wid: float("nan") for wid in warehouse_ids_to_measure}
                time.sleep(1.0)

        result = (data.get("result") or {})
        arr = result.get("data_array") or []
        out = {wid: float("nan") for wid in warehouse_ids_to_measure}

        for row in arr:
            if not row or len(row) < 2:
                continue
            wid, p99 = row[0], row[1]
            try:
                out[str(wid)] = float(p99) if p99 is not None else float("nan")
            except Exception:
                out[str(wid)] = float("nan")
        return out

    # ----------------------------
    # Query history batch fetch
    # ----------------------------
    def _build_history_params_multi(
        self,
        warehouse_ids: List[str],
        start_time: datetime,
        end_time: datetime,
        include_metrics: bool,
        page_token: Optional[str],
    ) -> Dict[str, Any]:
        s = self.settings
        params: Dict[str, Any] = {
            "filter_by.warehouse_ids": warehouse_ids,  # repeated params
            "filter_by.query_start_time_range.start_time_ms": self._to_ms(start_time),
            "filter_by.query_start_time_range.end_time_ms": self._to_ms(end_time),
            "include_metrics": "true" if include_metrics else "false",
            "max_results": s.max_results_per_page,
        }
        if page_token:
            params["page_token"] = page_token
        return params

    def _fetch_query_history_df_multi(
        self,
        workspace_host: str,
        warehouse_ids: List[str],
        start_time: datetime,
        end_time: datetime,
        include_metrics: bool,
    ) -> pd.DataFrame:
        s = self.settings
        url = f"https://{workspace_host}/api/2.0/sql/history/queries"
        all_rows: List[Dict[str, Any]] = []
        page_token: Optional[str] = None

        while True:
            params = self._build_history_params_multi(warehouse_ids, start_time, end_time, include_metrics, page_token)
            resp = self.session.get(url, params=params, timeout=s.http_timeout_sec)
            resp.raise_for_status()

            payload = self._fast_json_loads(resp.content)
            rows = payload.get("res", []) or []
            if rows:
                all_rows.extend(rows)

            page_token = payload.get("next_page_token") or payload.get("page_token")
            if not page_token or len(rows) == 0:
                break

        if not all_rows:
            return pd.DataFrame()

        raw = pd.DataFrame.from_records(all_rows)
        return self._normalize_query_history_df(raw)

    # =========================================================
    # NORMALIZATION: THE ONLY PLACE WE CONVERT EPOCH-MS -> DATETIME
    # =========================================================
    def _normalize_query_history_df(self, raw: pd.DataFrame) -> pd.DataFrame:
        """
        Output contract:
        - Keeps *_ms numeric fields (clamped) for wall-clock arithmetic when convenient.
        - Adds *_dt datetime fields (tz-aware UTC) for all time logic.
        - NO downstream function should call pd.to_datetime(unit="ms") again.
        """
        s = self.settings
        df = raw.copy()

        df["statement_id"] = df.get("query_id")
        df["execution_status"] = df.get("status").astype("string").str.upper()
        df["warehouse_id"] = df.get("warehouse_id").astype("string")

        m = df.get("metrics")

        def mget(key: str) -> pd.Series:
            if m is None:
                return pd.Series([None] * len(df))
            return m.map(lambda x: x.get(key) if isinstance(x, dict) else None)

        # ---- epoch-ms extraction (numeric) ----
        # top-level
        df["query_start_time_ms"] = pd.to_numeric(df.get("query_start_time_ms"), errors="coerce")
        df["query_end_time_ms"] = pd.to_numeric(df.get("query_end_time_ms"), errors="coerce")
        df["execution_end_time_ms"] = pd.to_numeric(df.get("execution_end_time_ms"), errors="coerce")

        # metrics timestamps (epoch-ms)
        df["compile_start_ms"] = pd.to_numeric(mget("query_compilation_start_timestamp"), errors="coerce")
        df["provisioning_queue_start_ms"] = pd.to_numeric(mget("provisioning_queue_start_timestamp"), errors="coerce")
        df["overloading_queue_start_ms"] = pd.to_numeric(mget("overloading_queue_start_timestamp"), errors="coerce")

        # ---- clamp all epoch-ms to safe range (prevents overflow) ----
        EPOCH_COLS = [
            "query_start_time_ms",
            "query_end_time_ms",
            "execution_end_time_ms",
            "compile_start_ms",
            "provisioning_queue_start_ms",
            "overloading_queue_start_ms",
        ]
        for col in EPOCH_COLS:
            ser = pd.to_numeric(df[col], errors="coerce")
            df[col] = ser.where((ser >= s.min_epoch_ms) & (ser <= s.max_epoch_ms))

        # ---- single conversion to datetime (UTC) ----
        df["start_time_dt"] = pd.to_datetime(df["query_start_time_ms"], unit="ms", utc=True, errors="coerce")
        df["end_time_dt"] = pd.to_datetime(df["query_end_time_ms"], unit="ms", utc=True, errors="coerce")
        df["execution_end_time_dt"] = pd.to_datetime(df["execution_end_time_ms"], unit="ms", utc=True, errors="coerce")
        df["compile_start_dt"] = pd.to_datetime(df["compile_start_ms"], unit="ms", utc=True, errors="coerce")
        df["provisioning_queue_start_dt"] = pd.to_datetime(df["provisioning_queue_start_ms"], unit="ms", utc=True, errors="coerce")
        df["overloading_queue_start_dt"] = pd.to_datetime(df["overloading_queue_start_ms"], unit="ms", utc=True, errors="coerce")

        # ---- duration metrics (ms) ----
        def mnum(key: str) -> pd.Series:
            return pd.to_numeric(mget(key), errors="coerce").fillna(0.0)

        df["total_duration_ms"] = mnum("total_time_ms")
        df["execution_duration_ms"] = mnum("execution_time_ms")
        df["compilation_duration_ms"] = mnum("compilation_time_ms")
        df["result_fetch_time_ms"] = mnum("result_fetch_time_ms")

        # ---- queue durations from datetime diffs (safe) ----
        # waiting_for_compute = compile_start - provisioning_queue_start
        # waiting_at_capacity = compile_start - overloading_queue_start
        # if queue start missing -> 0
        wfc = (df["compile_start_dt"] - df["provisioning_queue_start_dt"]).dt.total_seconds() * 1000.0
        wac = (df["compile_start_dt"] - df["overloading_queue_start_dt"]).dt.total_seconds() * 1000.0
        df["waiting_for_compute_duration_ms"] = wfc.where(df["provisioning_queue_start_dt"].notna(), 0.0).clip(lower=0.0).fillna(0.0)
        df["waiting_at_capacity_duration_ms"] = wac.where(df["overloading_queue_start_dt"].notna(), 0.0).clip(lower=0.0).fillna(0.0)

        # I/O and spill
        df["spilled_local_bytes"] = mnum("spill_to_disk_bytes")
        df["read_bytes"] = mnum("read_bytes")
        df["written_bytes"] = mnum("write_remote_bytes")

        # Keep a stable schema
        cols = [
            "warehouse_id", "statement_id", "execution_status",

            # epoch-ms (clamped)
            "query_start_time_ms", "query_end_time_ms", "execution_end_time_ms",
            "compile_start_ms", "provisioning_queue_start_ms", "overloading_queue_start_ms",

            # datetime (UTC) - used everywhere downstream
            "start_time_dt", "end_time_dt", "execution_end_time_dt",
            "compile_start_dt", "provisioning_queue_start_dt", "overloading_queue_start_dt",

            # durations
            "total_duration_ms", "execution_duration_ms", "compilation_duration_ms", "result_fetch_time_ms",
            "waiting_for_compute_duration_ms", "waiting_at_capacity_duration_ms",

            # bytes
            "spilled_local_bytes", "read_bytes", "written_bytes",
        ]
        return df[cols].copy()

    # ----------------------------
    # Warehouse status batch
    # ----------------------------
    def _fetch_warehouse_status(self, warehouse_id: str, workspace_host: str) -> Dict[str, Any]:
        s = self.settings
        url = f"https://{workspace_host}/api/2.0/sql/warehouses/{warehouse_id}"
        resp = self.session.get(url, timeout=s.http_timeout_sec)
        resp.raise_for_status()
        w = resp.json()
        health = w.get("health") or {}
        return {
            "warehouse_name": w.get("name"),
            "warehouse_state": w.get("state") or w.get("status"),
            "warehouse_current_clusters": w.get("num_clusters", 0),
            "warehouse_max_num_clusters": w.get("max_num_clusters", 0),
            "warehouse_min_num_clusters": w.get("min_num_clusters", 0),
            "warehouse_size": w.get("cluster_size", "UNKNOWN"),
            "warehouse_health_status": health.get("status"),
            "warehouse_active_sessions": w.get("num_active_sessions"),
            "warehouse_auto_stop_mins": w.get("auto_stop_mins", 0),
        }

    def _fetch_warehouse_status_batch(self, workspace_host: str, warehouse_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        s = self.settings
        out: Dict[str, Dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=min(s.max_workers_status, max(1, len(warehouse_ids)))) as ex:
            futs = {ex.submit(self._fetch_warehouse_status, wid, workspace_host): wid for wid in warehouse_ids}
            for fut in as_completed(futs):
                wid = futs[fut]
                try:
                    out[wid] = fut.result()
                except Exception as e:
                    out[wid] = {"warehouse_state": "UNKNOWN", "warehouse_status_error": str(e)}
        return out

    # ----------------------------
    # Window filtering (datetime-only)
    # ----------------------------
    def _filter_queries_touching_window(self, df: pd.DataFrame, window_start: datetime, window_end: datetime) -> pd.DataFrame:
        """
        Touch window means: [start, end] overlaps [window_start, window_end].
        For running queries (end_time_dt is null), we cap end at now().
        """
        if df is None or df.empty:
            return df

        ws = self._to_utc_ts(window_start)
        we = self._to_utc_ts(window_end)
        now_ts = self._to_utc_ts(self._utc_now())

        start_dt = df["start_time_dt"]
        end_dt = df["end_time_dt"].fillna(now_ts)

        touches = (start_dt < we) & (end_dt > ws)
        return df.loc[touches].copy()

    def _df_terminal_ended_in_window(self, df: pd.DataFrame, window_start: datetime, window_end: datetime) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        ws = self._to_utc_ts(window_start)
        we = self._to_utc_ts(window_end)

        d = df[df["execution_status"].isin(self.TERMINAL_STATUSES)].copy()
        d = d[d["end_time_dt"].notna()]
        return d[(d["end_time_dt"] >= ws) & (d["end_time_dt"] <= we)]

    def _df_arrivals_in_window(self, df: pd.DataFrame, window_start: datetime, window_end: datetime) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        ws = self._to_utc_ts(window_start)
        we = self._to_utc_ts(window_end)

        d = df[df["start_time_dt"].notna()].copy()
        return d[(d["start_time_dt"] >= ws) & (d["start_time_dt"] <= we)]

    # ----------------------------
    # Snapshot counts from history
    # ----------------------------
    def _snapshot_query_counts_from_history(self, df: pd.DataFrame) -> Dict[str, int]:
        if df is None or df.empty:
            return {"current_running_queries": 0, "current_queued_queries": 0}
        status = df["execution_status"].astype("string").str.upper()
        return {
            "current_running_queries": int(status.isin(self.RUNNING_STATES).sum()),
            "current_queued_queries": int((status == self.QUEUED_STATE).sum()),
        }

    # ----------------------------
    # Concurrency sweep-line
    # ----------------------------
    @staticmethod
    def _qkey(q: float) -> str:
        return f"p{int(q * 100) if q < 1 else 100}"

    def _time_weighted_quantiles_from_intervals(
        self,
        intervals: List[Tuple[datetime, datetime]],
        window_start: datetime,
        window_end: datetime,
        quantiles=(0.5, 0.95, 0.99, 1.0),
    ) -> Dict[str, float]:
        if not intervals:
            return {self._qkey(q): 0.0 for q in quantiles}

        events: List[Tuple[datetime, int]] = []
        for s, e in intervals:
            if s is None or e is None:
                continue
            s2 = max(s, window_start)
            e2 = min(e, window_end)
            if e2 <= s2:
                continue
            events.append((s2, +1))
            events.append((e2, -1))

        if not events:
            return {self._qkey(q): 0.0 for q in quantiles}

        events.sort(key=lambda x: x[0])

        cur = 0
        prev_t = window_start
        idx = 0
        buckets: Dict[int, float] = {}

        while prev_t < window_end:
            next_t = events[idx][0] if idx < len(events) else window_end
            next_t = min(next_t, window_end)

            if next_t > prev_t:
                dur = (next_t - prev_t).total_seconds()
                buckets[cur] = buckets.get(cur, 0.0) + dur
                prev_t = next_t

            while idx < len(events) and events[idx][0] == prev_t:
                cur += events[idx][1]
                idx += 1

            if idx >= len(events) and prev_t >= window_end:
                break

        total = sum(buckets.values())
        if total <= 0:
            return {self._qkey(q): 0.0 for q in quantiles}

        levels = sorted(buckets.items(), key=lambda kv: kv[0])
        running = 0.0
        cdf: List[Tuple[int, float]] = []
        for level, sec in levels:
            running += sec
            cdf.append((level, running / total))

        def quantile_value(q: float) -> float:
            if q >= 1.0:
                return float(max(buckets.keys()))
            for level, p in cdf:
                if p >= q:
                    return float(level)
            return float(max(buckets.keys()))

        return {self._qkey(q): quantile_value(q) for q in quantiles}

    # ----------------------------
    # Interval builders (datetime-only, no conversions)
    # ----------------------------
    def _build_running_intervals(self, df_touch: pd.DataFrame, window_end: datetime) -> List[Tuple[datetime, datetime]]:
        if df_touch is None or df_touch.empty:
            return []

        end_cap = min(self._utc_now(), window_end)
        end_cap_ts = self._to_utc_ts(end_cap)

        # Running interval start is compile_start_dt if present else start_time_dt
        start_dt = df_touch["compile_start_dt"].fillna(df_touch["start_time_dt"])

        # End dt is end_time_dt if terminal else capped at end_cap
        is_terminal = df_touch["execution_status"].isin(self.TERMINAL_STATUSES)
        end_dt = df_touch["end_time_dt"].where(is_terminal, end_cap_ts)

        intervals: List[Tuple[datetime, datetime]] = []
        for sdt, edt in zip(start_dt, end_dt):
            if pd.isna(sdt) or pd.isna(edt):
                continue
            s_py = sdt.to_pydatetime()
            e_py = edt.to_pydatetime()
            if e_py > s_py:
                intervals.append((s_py, e_py))
        return intervals

    def _build_queued_intervals(self, df_touch: pd.DataFrame) -> List[Tuple[datetime, datetime]]:
        if df_touch is None or df_touch.empty:
            return []

        compile_dt = df_touch["compile_start_dt"]
        prov_dt = df_touch["provisioning_queue_start_dt"]
        over_dt = df_touch["overloading_queue_start_dt"]

        intervals: List[Tuple[datetime, datetime]] = []

        def add_pairs(starts: pd.Series):
            for sdt, edt in zip(starts, compile_dt):
                if pd.isna(sdt) or pd.isna(edt):
                    continue
                s_py = sdt.to_pydatetime()
                e_py = edt.to_pydatetime()
                if e_py > s_py:
                    intervals.append((s_py, e_py))

        add_pairs(prov_dt)
        add_pairs(over_dt)
        return intervals

    # ----------------------------
    # Metrics core
    # ----------------------------
    def _compute_metrics(self, df_all: pd.DataFrame, window_start: datetime, window_end: datetime) -> Dict[str, Any]:
        s = self.settings

        if df_all is None or df_all.empty:
            return {
                "runtime_p50_sec": 0.0, "runtime_p90_sec": 0.0, "runtime_p95_sec": 0.0, "runtime_p99_sec": 0.0, "runtime_p100_sec": 0.0,
                "failure_rate_pct": 0.0,
                "qps": 0.0, "qpm": 0.0,
                "queued_p50_sec": 0.0, "queued_p95_sec": 0.0, "queued_p99_sec": 0.0, "queued_p100_sec": 0.0,
                "queue_life_pct_p50": 0.0, "queue_life_pct_p95": 0.0, "queue_life_pct_p99": 0.0, "queue_life_pct_p100": 0.0,
                "running_concurrency_p50": 0.0, "running_concurrency_p95": 0.0, "running_concurrency_p99": 0.0, "running_concurrency_p100": 0.0,
                "queued_concurrency_p50": 0.0, "queued_concurrency_p95": 0.0, "queued_concurrency_p99": 0.0, "queued_concurrency_p100": 0.0,
                "spilled_query_pct": 0.0,
            }

        df_touch = self._filter_queries_touching_window(df_all, window_start, window_end)
        df_done_end = self._df_terminal_ended_in_window(df_touch, window_start, window_end)
        df_ok_end = df_done_end[df_done_end["execution_status"] == self.SUCCESS_STATUS].copy()
        df_arrivals = self._df_arrivals_in_window(df_touch, window_start, window_end)

        window_seconds = (window_end - window_start).total_seconds()
        metrics: Dict[str, Any] = {}

        # QPS/QPM
        q_count = len(df_done_end) if s.qps_mode == "completion" else len(df_arrivals)
        metrics["qps"] = (q_count / window_seconds) if window_seconds > 0 else 0.0
        metrics["qpm"] = (q_count / (window_seconds / 60.0)) if window_seconds > 0 else 0.0

        # Failure rate (terminal ended in window)
        if df_done_end.empty:
            metrics["failure_rate_pct"] = 0.0
        else:
            st = df_done_end["execution_status"].astype("string").str.upper()
            failed = int((st == "FAILED").sum())
            finished = int((st == "FINISHED").sum())
            canceled = int((st == "CANCELED").sum())
            denom = failed + finished + canceled
            metrics["failure_rate_pct"] = (failed / denom * 100.0) if denom > 0 else 0.0

        # Runtime percentiles (FINISHED ended in window): wall clock
        if not df_ok_end.empty:
            runtime_ms = (
                df_ok_end["query_end_time_ms"].astype(float)
                - df_ok_end["query_start_time_ms"].astype(float)
            ).where(lambda x: x >= 0)
            runtimes_sec = (runtime_ms / 1000.0).dropna()
        else:
            runtimes_sec = pd.Series([], dtype=float)

        metrics["runtime_p50_sec"] = float(runtimes_sec.quantile(0.50)) if len(runtimes_sec) else 0.0
        metrics["runtime_p90_sec"] = float(runtimes_sec.quantile(0.90)) if len(runtimes_sec) else 0.0
        metrics["runtime_p95_sec"] = float(runtimes_sec.quantile(0.95)) if len(runtimes_sec) else 0.0
        metrics["runtime_p99_sec"] = float(runtimes_sec.quantile(0.99)) if len(runtimes_sec) else 0.0
        metrics["runtime_p100_sec"] = float(runtimes_sec.max()) if len(runtimes_sec) else 0.0

        # Queued duration percentiles (terminal ended in window)
        if df_done_end.empty:
            queued_sec = pd.Series([], dtype=float)
        else:
            queued_ms = (
                df_done_end["waiting_at_capacity_duration_ms"].fillna(0).astype(float)
                + df_done_end["waiting_for_compute_duration_ms"].fillna(0).astype(float)
            ).clip(lower=0.0)
            queued_sec = queued_ms / 1000.0

        metrics["queued_p50_sec"] = float(queued_sec.quantile(0.50)) if len(queued_sec) else 0.0
        metrics["queued_p95_sec"] = float(queued_sec.quantile(0.95)) if len(queued_sec) else 0.0
        metrics["queued_p99_sec"] = float(queued_sec.quantile(0.99)) if len(queued_sec) else 0.0
        metrics["queued_p100_sec"] = float(queued_sec.max()) if len(queued_sec) else 0.0

        # % of query life spent queued (terminal ended in window), life = wall clock
        if df_done_end.empty:
            queue_life_pct = pd.Series([], dtype=float)
        else:
            wall_ms = (
                df_done_end["query_end_time_ms"].astype(float)
                - df_done_end["query_start_time_ms"].astype(float)
            ).where(lambda x: x >= 0)

            queued_ms = (
                df_done_end["waiting_at_capacity_duration_ms"].fillna(0).astype(float)
                + df_done_end["waiting_for_compute_duration_ms"].fillna(0).astype(float)
            ).clip(lower=0.0)

            denom = wall_ms.replace({0.0: float("nan")})
            queue_life_pct = (queued_ms / denom).fillna(0.0).clip(lower=0.0, upper=1.0) * 100.0

        metrics["queue_life_pct_p50"] = float(queue_life_pct.quantile(0.50)) if len(queue_life_pct) else 0.0
        metrics["queue_life_pct_p95"] = float(queue_life_pct.quantile(0.95)) if len(queue_life_pct) else 0.0
        metrics["queue_life_pct_p99"] = float(queue_life_pct.quantile(0.99)) if len(queue_life_pct) else 0.0
        metrics["queue_life_pct_p100"] = float(queue_life_pct.max()) if len(queue_life_pct) else 0.0

        metrics["queue_life_p95_sentence"] = (
            f"95% of completed queries spent ≤ {metrics['queue_life_pct_p95']:.2f}% of their life queued."
        )

        # Spill % over df_touch (succeeded + running + failed)
        if df_touch is None or df_touch.empty:
            metrics["spilled_query_pct"] = 0.0
        else:
            spilled = int((df_touch["spilled_local_bytes"].fillna(0).astype(float) > 0).sum())
            metrics["spilled_query_pct"] = (spilled / len(df_touch) * 100.0) if len(df_touch) else 0.0

        # Concurrency (time-weighted) using touch-window queries, including active
        running_intervals = self._build_running_intervals(df_touch, window_end)
        queued_intervals = self._build_queued_intervals(df_touch)

        run_q = self._time_weighted_quantiles_from_intervals(running_intervals, window_start, window_end)
        q_q = self._time_weighted_quantiles_from_intervals(queued_intervals, window_start, window_end)

        metrics["running_concurrency_p50"] = run_q["p50"]
        metrics["running_concurrency_p95"] = run_q["p95"]
        metrics["running_concurrency_p99"] = run_q["p99"]
        metrics["running_concurrency_p100"] = run_q["p100"]

        metrics["queued_concurrency_p50"] = q_q["p50"]
        metrics["queued_concurrency_p95"] = q_q["p95"]
        metrics["queued_concurrency_p99"] = q_q["p99"]
        metrics["queued_concurrency_p100"] = q_q["p100"]

        return metrics


# =========================================================
# Runner (supports multiple monitors)
# =========================================================

@dataclass
class MonitorRunner:
    monitors: List[Monitor]
    poll_interval_seconds: int = 60

    def run_forever(self) -> None:
        while True:
            for m in self.monitors:
                events = m.poll_once()
                # Monitor owns sinks in this design
                if hasattr(m, "emit"):
                    m.emit(events)  # type: ignore[attr-defined]
            time.sleep(self.poll_interval_seconds)


# =========================================================
# Example wiring
# =========================================================

if __name__ == "__main__":
  
    settings = DatabricksSQLMonitorSettings(
        databricks_token=os.environ.get("DATABRICKS_TOKEN", ""),
        poll_interval_seconds=60,
        rolling_window_minutes=10,
        lookback_buffer_minutes=2,
        control_workspace_host="e2-demo-field-eng.cloud.databricks.com",
        control_warehouse_id="4b9b953939869799",
        warehouse_workspace_map={
            "4b9b953939869799": "e2-demo-field-eng.cloud.databricks.com",
            "862f1d757f0424f7": "e2-demo-field-eng.cloud.databricks.com",
        },
        qps_mode="arrival",
    )

    sinks: List[Sink] = [
        ConsoleSink(),
        # DatadogSink(api_key="...")
        # DeltaTableSink(table="main.default.warehouse_live_performance_metrics"),
    ]

    dbsql_monitor = DatabricksSQLMonitor(settings=settings, sinks=sinks)
    runner = MonitorRunner(monitors=[dbsql_monitor], poll_interval_seconds=settings.poll_interval_seconds)
    runner.run_forever()
