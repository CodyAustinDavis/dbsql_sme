# Databricks Limits Headroom Scanner

See how close your Databricks account is to documented platform limits before one blocks work.
The scanner reads your own `system.information_schema` (read-only), compares counts to a
reference table of limits from the public Databricks docs, and records usage over time.

## What's here

| File | Purpose |
|---|---|
| `sql/01_tables.sql` | Creates `limits_reference`, `limit_changes`, `limit_usage`, and the `scan_context` / `latest_usage` views |
| `sql/02_seed_limits_reference.sql` | Loads documented limits (AWS / Azure / GCP); logs value changes to `limit_changes` |
| `sql/03_scan_uc.sql` | Counts Unity Catalog and Delta Sharing objects and writes usage vs limit to `limit_usage` |
| `sql/04_write_rest_usage.sql` | Writes REST-collected usage to `limit_usage` (called by `scan_rest.py`) |
| `scripts/scan_rest.py` | Counts objects `information_schema` doesn't expose (models, model versions, clean rooms, apps, jobs, pipelines, warehouses, secret scopes, serving and AI Search endpoints, IP access entries) |
| `scripts/run_sql.py` | Dev helper: runs a SQL file with `catalog` / `schema` parameters |
| `databricks.yml`, `resources/` | Bundle: the scheduled `limit-tracking-scan` job and the headroom dashboard |
| `dashboards/` | Dashboard definition (`limits_headroom.lvdash.json`) and its dataset SQL |
| `tests/` | `python3 -m unittest discover -s tests` |

## Deploy as a scheduled job (recommended)

```bash
databricks bundle deploy -t prod --var catalog=<catalog> --var schema=limit_tracking \
  --var warehouse_id=<warehouse-id>
databricks bundle run limit_tracking_scan -t prod
```

This deploys the `limit-tracking-scan` job: setup → seed → the Unity Catalog and REST scans in
parallel, daily at 06:00 UTC, one run at a time. Run it as a metastore admin who is also a
workspace admin (for example, a service principal via `run_as` in `databricks.yml`) so
`full_visibility` is TRUE. Tables from earlier versions are upgraded in place on the next job run
(a `bundle deploy` alone doesn't run SQL). The dev target writes to `limit_tracking_dev`.

The bundle also deploys the **Databricks limits headroom** dashboard: KPIs, the limits closest to
their ceiling, a daily trend (since the first scan, up to 90 days), every tracked limit with a docs link, usage by parent object, and
documented limit changes.

## Run it by hand

```bash
pip install -r requirements.txt   # databricks-sdk, used by both scripts
for f in sql/01_tables.sql sql/02_seed_limits_reference.sql sql/03_scan_uc.sql; do
  python3 scripts/run_sql.py $f --profile <profile> --warehouse <warehouse-id> \
    --catalog <catalog> --schema <schema> || break
done
```

Then the REST scan:

```bash
python3 scripts/scan_rest.py --profile <profile> --warehouse <warehouse-id> \
  --catalog <catalog> --schema <schema>
```

Then check headroom:

```sql
SELECT source, limit_id, parent_name, usage_count, limit_value, pct_used, full_visibility
FROM <catalog>.<schema>.latest_usage
ORDER BY pct_used DESC NULLS LAST;
```

## Notes

- **Run as a metastore admin.** `information_schema` only returns objects the caller can see,
  so other identities undercount (flagged by `full_visibility = false`).
- **Your account's limits may differ from the docs.** If a limit was raised for your account,
  account admins can read the actual values from the
  [Unity Catalog Resource Quotas API](https://docs.databricks.com/aws/en/data-governance/unity-catalog/resource-quotas).
- **Limits marked `adjustable = true`** can be raised through your Databricks account team.
- Databricks-managed catalogs (`system`, `hive_metastore`, `samples`, `__databricks_internal*`)
  are excluded from all counts. Foreign and Delta Sharing catalogs are counted.
- Connection counts exclude system-managed (`MANAGED_*`) connection types.
- Each SQL file is one SQL scripting block (`BEGIN … END`), so it needs a SQL warehouse or a
  compute version that supports SQL scripting. Catalog and schema names are backtick-quoted, so
  names with hyphens work.
- The scan only reads metadata, then writes `limit_usage` in one atomic INSERT. It fails before
  scanning if it can't read the metastore's cloud or a scanned limit has no `limits_reference` row.
- `full_visibility` on each usage row is TRUE when the scanner can see everything in scope (metastore
  owner for Unity Catalog rows, workspace admin for workspace rows). When FALSE,
  counts are lower bounds.
- Don't run the seed concurrently with itself; the change-log diff assumes one run at a time.
- Keep `limits_reference` and `limit_changes` together: dropping only the reference makes the
  next seed look like a retry of the original `added` rows, so nothing is logged.
- `limit_changes` records limits being added or removed, and changes to a value (per cloud) or
  to whether it is adjustable. Each `limits_reference` row cites the docs page for each cloud.
- Limits with `measure_method = 'rest_api'` are counted by `scan_rest.py`, which only makes list/get
  calls. If one API fails (for example, a feature that isn't enabled), it skips that limit, reports
  it, and writes the rest. Account-level limits (`account_api`) need an account admin and aren't
  measured yet.

## Assumptions to know about

These choices affect some counts. They follow the public docs where the docs are explicit and are
noted here where they aren't:

- **Connections** leave out system-managed (`MANAGED_*`) connection types, such as those Lakebase creates.
- **Catalog, table and volume counts** include foreign (federated) and Delta Sharing catalogs, and
  leave out Databricks-managed catalogs (`system`, `samples`, `__databricks_internal*`).
- **Tables** count every row in `information_schema.tables`, including views.
- **Serving endpoints** leave out Databricks-provided Foundation Model endpoints (`databricks-*`); the
  documented limit covers custom model and agent endpoints.
- **All counts are taken from the workspace the job runs in.** A catalog bound only to other
  workspaces isn't visible from it, so its schemas, tables, volumes and models aren't counted, even
  when `full_visibility` is TRUE. Run the job from a workspace every catalog is bound to.
- **Usage that drops to zero** is written as a 0 row, so removed objects stop showing. Rows more
  than 7 days older than a scanner's newest scan (a collector that keeps failing, or a workspace that
  stopped running the job) drop out of `latest_usage`.
- **The trend chart** shows the 6 highest limits overall; the Area and Level filters don't apply to it.
- **Limit values** come from the public docs. If your account team has raised a limit for you, the
  dashboard will overstate % used for it.

Your account team can confirm any of these for your account.

## Limits listed but not measured

The dashboard's **Limits tracked** count is every limit in `limits_reference`. **Limits measured**
is the subset the scans count usage for. These five are tracked (limit value, whether it can be
raised, docs link) but show as **Not measured**:

| Limit | Why it isn't measured |
|---|---|
| Groups per account | Needs an account admin; the job runs at workspace level |
| Users and service principals per account | Needs an account admin; the job runs at workspace level |
| Privileges per parent object (metastore, catalog, schema) | Not counted yet; the `information_schema.*_privileges` views could support it |
| Privileges per non-parent object (table, view, share) | Not counted yet; the `information_schema.*_privileges` views could support it |
| Policies per metastore | Not counted yet; `information_schema.abac_policy_definitions` could support it |

Sources: [AWS](https://docs.databricks.com/aws/en/resources/limits) ·
[Azure](https://learn.microsoft.com/en-us/azure/databricks/resources/limits) ·
[GCP](https://docs.databricks.com/gcp/en/resources/limits)
