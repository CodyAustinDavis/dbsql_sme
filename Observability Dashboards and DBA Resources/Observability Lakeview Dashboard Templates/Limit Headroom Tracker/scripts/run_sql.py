"""Run a limit-tracking SQL file via the Statement Execution API.

Each file in sql/ is a single SQL scripting block (BEGIN ... END), sent as one statement with
`catalog` / `schema` parameters. Dev helper only; the bundle job runs the same files as SQL tasks.

  python3 scripts/run_sql.py sql/01_tables.sql --warehouse <id> --catalog <catalog> \
      --schema <schema> [--profile <profile>]
"""
import argparse
import sys

from databricks.sdk import WorkspaceClient

from lt_sql import StatementAborted, StatementFailed, execute, load_sql


def main():
    p = argparse.ArgumentParser()
    p.add_argument("file")
    p.add_argument("--profile")
    for f in ("warehouse", "catalog", "schema"):
        p.add_argument(f"--{f}", required=True)
    a = p.parse_args()
    w = WorkspaceClient(profile=a.profile) if a.profile else WorkspaceClient()
    try:
        r = execute(w, load_sql(a.file), a.warehouse, {"catalog": a.catalog, "schema": a.schema})
    except StatementFailed as e:
        sys.exit(f"FAILED: {a.file}\n{e}")
    except StatementAborted as e:
        sys.exit(f"ABORTED: {a.file}\n{e}")
    rows = r.result.data_array if r.result else None
    print(f"ok: {a.file}" + (f"  -> {rows[:5]}" if rows else ""))


if __name__ == "__main__":
    main()
