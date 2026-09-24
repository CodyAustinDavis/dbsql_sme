"""Standalone entrypoint for the DBSQL warehouse Prometheus exporter.

Wires the warehouse monitor (poller) to the PrometheusSink and serves /metrics for
Prometheus to scrape. Configure entirely through environment variables.

Auth (pick one):
    OAuth M2M (service principal, for deployment):
        DATABRICKS_CLIENT_ID      service principal application (client) id
        DATABRICKS_CLIENT_SECRET  service principal OAuth secret
        DATABRICKS_OAUTH_SCOPE    optional, default "all-apis"
    Static token (convenient for local testing):
        DATABRICKS_TOKEN          a PAT or bearer with read access

Other:
    WAREHOUSE_WORKSPACE_MAP   required. JSON of {"<warehouse_id>": "<workspace_host>"}.
    POLL_INTERVAL_SECONDS     optional, default 30.
    METRICS_PORT              optional, default 9877.
    METRICS_NAMESPACE         optional, default "dbsql".
    MAX_BACKOFF_SECONDS       optional, default 300. Cap for poll-failure backoff.

Run:
    python exporter.py
    # then Prometheus scrapes http://<host>:9877/metrics
"""

import json
import os
import time

from warehouse_monitor import (
    DatabricksSQLMonitor,
    DatabricksSQLMonitorSettings,
)
from prometheus_sink import PrometheusSink
from auth import BearerAuth, OAuthM2MTokenProvider, TokenGetter, static_token_getter

# Sentinel token used to satisfy the monitor constructor when auth is OAuth M2M.
# The monitor bakes an Authorization header from settings.databricks_token at init;
# we override that header with BearerAuth before any request is sent, so this value
# never reaches an API. See _install_auth.
_OAUTH_SENTINEL = "oauth-m2m"


def _env_map(name: str) -> dict:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{name} must be valid JSON: {exc}")


def _normalize_host(raw: str) -> str:
    """Reduce a workspace host to a bare netloc.

    Strips surrounding whitespace, a leading scheme, and any trailing slash or path,
    so 'https://ws.cloud.databricks.com/' and 'ws.cloud.databricks.com ' both become
    'ws.cloud.databricks.com'. The monitor interpolates this straight into
    'https://{host}/api/...', so a scheme or slash here would produce a broken URL.
    """
    h = (raw or "").strip()
    if "://" in h:
        h = h.split("://", 1)[1]
    return h.split("/", 1)[0].strip()


def _build_token_getter() -> TokenGetter:
    """Choose an auth mode from the environment and return a per-host token getter.

    OAuth M2M wins if both client id and secret are present; otherwise fall back to a
    static token. Exactly one must be configured.
    """
    client_id = os.environ.get("DATABRICKS_CLIENT_ID", "").strip()
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "").strip()
    token = os.environ.get("DATABRICKS_TOKEN", "").strip()

    if client_id and client_secret:
        if token:
            print(
                "[exporter] both OAuth client id/secret and DATABRICKS_TOKEN are set; "
                "using OAuth M2M and ignoring DATABRICKS_TOKEN.",
                flush=True,
            )
        scope = os.environ.get("DATABRICKS_OAUTH_SCOPE", "all-apis").strip() or "all-apis"
        provider = OAuthM2MTokenProvider(client_id, client_secret, scope=scope)
        print(f"[exporter] auth: OAuth M2M service principal (scope={scope})", flush=True)
        return provider.token_for
    if token:
        print("[exporter] auth: static token (DATABRICKS_TOKEN)", flush=True)
        return static_token_getter(token)

    raise SystemExit(
        "No credentials found. Set DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET "
        "for a service principal (recommended), or DATABRICKS_TOKEN for a static "
        "token (local testing)."
    )


def _install_auth(monitor: DatabricksSQLMonitor, token_getter: TokenGetter) -> None:
    """Route the monitor's session through BearerAuth so every request gets a fresh
    token, and drop the static Authorization header the constructor baked in."""
    monitor.session.auth = BearerAuth(token_getter)
    monitor.session.headers.pop("Authorization", None)


def _prewarm(token_getter: TokenGetter, hosts) -> None:
    """Fetch a token for each host up front so misconfigured credentials or missing
    warehouse grants fail loudly at startup instead of silently on the first poll."""
    for host in sorted(set(hosts)):
        try:
            token_getter(host)
        except Exception as exc:  # noqa: BLE001 - surface any auth failure clearly
            raise SystemExit(
                f"Failed to obtain a token for {host}: {type(exc).__name__}: {exc}. "
                "Check the service principal credentials and that it has CAN MONITOR "
                "on the warehouses in this workspace."
            )


def _run_resilient(monitor: DatabricksSQLMonitor, poll_interval: int, max_backoff: int) -> None:
    """Poll forever, surviving transient failures.

    A single warehouse being unreachable is already handled inside poll_once (that
    warehouse reports state UNKNOWN and its health flips to -1). This loop guards the
    broader failures - a query-history error, a token refresh failure, a network blip -
    that would otherwise crash the process. On failure we skip the emit, so the freshness
    gauge (dbsql_last_poll_unixtime) stops advancing and the staleness alert fires, and
    we back off exponentially up to max_backoff before retrying.
    """
    consecutive_failures = 0
    while True:
        try:
            events = monitor.poll_once()
            monitor.emit(events)
            consecutive_failures = 0
            sleep_for = poll_interval
        except Exception as exc:  # noqa: BLE001 - keep the exporter alive across blips
            consecutive_failures += 1
            # Cap the exponent so a long outage does not build an absurd shift; the
            # sleep is capped by max_backoff anyway, this just keeps the math sane.
            sleep_for = min(poll_interval * (2 ** min(consecutive_failures, 16)), max_backoff)
            print(
                f"[exporter] poll failed ({consecutive_failures}x): "
                f"{type(exc).__name__}: {exc}. backing off {sleep_for}s",
                flush=True,
            )
        time.sleep(sleep_for)


def main() -> None:
    warehouse_map = _env_map("WAREHOUSE_WORKSPACE_MAP")
    if not warehouse_map:
        raise SystemExit(
            'WAREHOUSE_WORKSPACE_MAP is required as JSON, e.g. '
            '{"abc123def456": "myworkspace.cloud.databricks.com"}'
        )

    # Normalize hosts so a scheme or trailing slash in config does not produce a
    # broken 'https://https://.../api/...' URL. Fail loudly on an empty host.
    warehouse_map = {str(wid).strip(): _normalize_host(host) for wid, host in warehouse_map.items()}
    empty_hosts = [wid for wid, host in warehouse_map.items() if not host]
    if empty_hosts:
        raise SystemExit(
            "WAREHOUSE_WORKSPACE_MAP has an empty workspace host for warehouse id(s): "
            + ", ".join(empty_hosts)
        )

    is_oauth = bool(
        os.environ.get("DATABRICKS_CLIENT_ID", "").strip()
        and os.environ.get("DATABRICKS_CLIENT_SECRET", "").strip()
    )
    token_getter = _build_token_getter()

    poll = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
    port = int(os.environ.get("METRICS_PORT", "9877"))
    namespace = os.environ.get("METRICS_NAMESPACE", "dbsql")
    max_backoff = int(os.environ.get("MAX_BACKOFF_SECONDS", "300"))

    # Fail fast on bad credentials / missing grants before we start serving.
    _prewarm(token_getter, warehouse_map.values())

    settings = DatabricksSQLMonitorSettings(
        # A non-empty token is required by the constructor; the real Authorization
        # header is supplied per-request by BearerAuth (see _install_auth).
        databricks_token=_OAUTH_SENTINEL,
        poll_interval_seconds=poll,
        warehouse_workspace_map=warehouse_map,
        # control_workspace_host / control_warehouse_id left unset on purpose:
        # the monitor skips the dynamic p99 refresh without them, so the exporter needs
        # no SQL warehouse. Set both to enable dynamic lookback tuning.
    )

    sink = PrometheusSink(namespace=namespace, port=port)
    sink.start_server()  # binds 0.0.0.0 for in-cluster scrape; keep off the public net

    monitor = DatabricksSQLMonitor(settings=settings, sinks=[sink])
    _install_auth(monitor, token_getter)

    # On the OAuth path the monitor constructor copied the sentinel into
    # os.environ["DATABRICKS_TOKEN"]. _install_auth has already replaced the baked
    # header with per-request OAuth, so drop the sentinel rather than leave a value in
    # the environment that looks like a real token.
    if is_oauth and os.environ.get("DATABRICKS_TOKEN") == _OAUTH_SENTINEL:
        os.environ.pop("DATABRICKS_TOKEN", None)

    print(
        f"Serving /metrics on :{port}, polling {len(warehouse_map)} warehouse(s) "
        f"every {poll}s (namespace={namespace})",
        flush=True,
    )
    _run_resilient(monitor, poll_interval=poll, max_backoff=max_backoff)


if __name__ == "__main__":
    main()
