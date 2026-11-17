"""limit-tracking REST scan: count objects that information_schema doesn't expose, then write them
to limit_usage through sql/04_write_rest_usage.sql (one atomic INSERT).

Read-only against the workspace: only list/get calls. Runs locally with a CLI profile, or inside
a Databricks job with default (job identity) auth.

  python3 scripts/scan_rest.py --warehouse <id> --catalog <catalog> --schema <schema> [--profile <p>] [--strict]

--strict exits non-zero (after writing) if any collector was skipped; use it for scheduled jobs
where every feature is expected to be enabled.
"""
import argparse
import json
import os
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied

# Serverless job tasks exec the script without __file__; fall back to the script path in argv.
HERE = os.path.dirname(os.path.abspath(globals().get("__file__") or sys.argv[0]))
sys.path.insert(0, HERE)  # job tasks don't add the script dir to sys.path
from lt_sql import StatementAborted, StatementFailed, execute, load_sql  # noqa: E402

TOP_N = 100  # per-parent limits keep the top N parents, same as the UC scan
WORKERS = 8  # parallel list calls (per-model versions, per-endpoint indexes, collectors)
WRITE_SQL = os.path.join(HERE, "..", "sql", "04_write_rest_usage.sql")


def count(it):
    return sum(1 for _ in it)


def top(limit_id, counts, scope_kind, complete=True):
    return [
        {"limit_id": limit_id, "parent_name": k, "usage_count": v, "scope_kind": scope_kind, "complete": complete}
        for k, v in Counter(counts).most_common(TOP_N)
    ]


def one(limit_id, n, scope_kind, parent=None, complete=True):
    """complete=False marks a count that skipped some items (e.g. one unreadable model); the
    writer then sets full_visibility = FALSE for that row."""
    return [{"limit_id": limit_id, "parent_name": parent, "usage_count": n, "scope_kind": scope_kind,
             "complete": complete}]


def collect(w):
    """Return (rows, warnings). Each collector is independent; one failing API doesn't stop the rest."""
    ws = None  # workspace-scoped rows carry workspace_id in its own column
    rows, warnings = [], []

    def uc_models():
        models = list(w.registered_models.list(include_browse=True, max_results=1000))
        per_schema = Counter(f"{m.catalog_name}.{m.schema_name}" for m in models)

        def versions(m):  # one unreadable model shouldn't drop every model limit
            try:
                return count(w.model_versions.list(m.full_name, include_browse=True, max_results=1000))
            except Exception as e:
                warnings.append(f"model versions for {m.full_name}: {type(e).__name__}")
                return None

        with ThreadPoolExecutor(WORKERS) as pool:
            n_versions = list(pool.map(versions, models))
        versions_per_model = {m.full_name: n for m, n in zip(models, n_versions) if n is not None}
        complete = len(versions_per_model) == len(models)
        return (
            top("uc_registered_models_per_schema", per_schema, "metastore")
            + one("uc_registered_models_per_metastore", len(models), "metastore")
            + top("uc_model_versions_per_model", versions_per_model, "metastore")
            + one("uc_model_versions_per_metastore", sum(versions_per_model.values()), "metastore",
                  complete=complete)
        )

    def clean_rooms():
        return one("cleanrooms_per_metastore", count(w.clean_rooms.list()), "metastore")

    def ai_search():
        eps = [e.name for e in w.vector_search_endpoints.list_endpoints()]

        def indexes(e):
            try:
                return count(w.vector_search_indexes.list_indexes(endpoint_name=e))
            except Exception as ex:
                warnings.append(f"indexes for endpoint {e}: {type(ex).__name__}")
                return None

        with ThreadPoolExecutor(WORKERS) as pool:
            n_idx = list(pool.map(indexes, eps))
        idx = {f"endpoint {e}": n for e, n in zip(eps, n_idx) if n is not None}
        return (one("ws_ai_search_endpoints_per_workspace", len(eps), "workspace", ws)
                + top("ai_search_indexes_per_endpoint", idx, "workspace"))

    def ip_entries():
        lists = list(w.ip_access_lists.list())
        return one("ws_ip_access_entries_per_workspace", sum(len(l.ip_addresses or []) for l in lists), "workspace", ws)

    collectors = {
        "registered models / model versions": uc_models,
        "clean rooms": clean_rooms,
        "apps": lambda: one("ws_apps_per_workspace", count(w.apps.list()), "workspace", ws),
        "jobs": lambda: one("ws_saved_jobs_per_workspace", count(w.jobs.list(limit=100)), "workspace", ws),
        "pipelines": lambda: one("ws_saved_pipelines_per_workspace", count(w.pipelines.list_pipelines(max_results=100)), "workspace", ws),
        "SQL warehouses": lambda: one("ws_sql_warehouses_per_workspace", count(w.warehouses.list()), "workspace", ws),
        "secret scopes": lambda: one("ws_secret_scopes_per_workspace", count(w.secrets.list_scopes()), "workspace", ws),
        # Databricks-provided Foundation Model endpoints (databricks-*) are excluded: the limit covers
        # custom model and agent endpoints.
        "serving endpoints": lambda: one("ws_serving_endpoints_per_workspace",
                                         count(e for e in w.serving_endpoints.list() if not (e.name or "").startswith("databricks-")),
                                         "workspace", ws),
        "AI Search": ai_search,
        "IP access lists": ip_entries,
    }
    def run(item):
        name, fn = item
        t = time.monotonic()
        try:
            return name, fn(), None, time.monotonic() - t
        except Exception as e:  # report and continue: one disabled feature shouldn't block the scan
            return name, [], f"{name}: {type(e).__name__}: {str(e)[:200]}", time.monotonic() - t

    with ThreadPoolExecutor(WORKERS) as pool:
        for name, got, warn, secs in pool.map(run, collectors.items()):
            if warn:
                warnings.append(warn)
                print(f"  {name}: SKIPPED ({warn})")
            else:
                rows += got
                print(f"  {name}: {len(got)} row(s) in {secs:.1f}s")
    return rows, warnings


def is_workspace_admin(w, attempts=3):
    """Workspace admins (directly or through nested groups) can read workspace settings; others get
    PERMISSION_DENIED. Other errors are retried, then raised, so a transient failure can't mislabel
    complete counts as lower bounds."""
    for i in range(attempts):
        try:
            w.workspace_conf.get_status(keys="enableIpAccessLists")
            return True
        except PermissionDenied:
            return False
        except Exception:
            if i == attempts - 1:
                raise
            time.sleep(2 * (i + 1))


def write(w, a, rows, ws_admin, workspace_id):
    params = {"catalog": a.catalog, "schema": a.schema, "rows": json.dumps(rows),
              "workspace_admin": str(ws_admin).lower(), "workspace_id": workspace_id}
    try:
        execute(w, load_sql(WRITE_SQL), a.warehouse, params, timeout_s=600)
    except (StatementFailed, StatementAborted) as e:
        sys.exit(f"FAILED to write REST usage: {e}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--profile")
    p.add_argument("--strict", action="store_true", help="exit 2 after writing if any collector was skipped")
    for f in ("warehouse", "catalog", "schema"):
        p.add_argument(f"--{f}", required=True)
    a = p.parse_args()
    w = WorkspaceClient(profile=a.profile) if a.profile else WorkspaceClient()
    ws_admin = is_workspace_admin(w)  # checked first, so a late failure can't waste the scan
    workspace_id = str(w.get_workspace_id())
    print("Collecting:")
    rows, warnings = collect(w)
    if not rows:
        sys.exit("No REST usage collected; nothing written.\n" + "\n".join(warnings))
    write(w, a, rows, ws_admin, workspace_id)
    print(f"ok: wrote {len(rows)} REST usage rows (workspace admin: {ws_admin})")
    if warnings:
        print("Skipped collectors (counts missing for these limits):\n  " + "\n  ".join(warnings))
        if a.strict:
            sys.exit(2)


if __name__ == "__main__":
    main()
