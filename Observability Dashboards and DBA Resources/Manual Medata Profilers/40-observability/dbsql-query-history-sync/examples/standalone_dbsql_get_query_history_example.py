#!/usr/bin/env python3

import sys, os
import datetime, dateutil.parser
import pickle

sys.path.append('../src')

import dbsql_query_history_sync.queries_api as queries_api

def ts():
  return datetime.datetime.now().strftime("%Y%m%d%H%M%S")

def main():
  # change as required.
  workspace_url = os.getenv("DATABRICKS_HOST", "e2-demo-field-eng.cloud.databricks.com")
  warehouse_ids = ["771c2bee30209f22"]
  start_ts_ms = dateutil.parser.parse('2023-01-01').timestamp() * 1000
  dbx_token = os.getenv('DATABRICKS_ACCESS_TOKEN')

  qh = queries_api.get_query_history(
        dbx_token=dbx_token,
        workspace_url=workspace_url,
        warehouse_ids=warehouse_ids,
        start_ts_ms=start_ts_ms)
  print(len(qh))
  fname = f'/tmp/queries_{ts()}.pkl'
  with open(fname, 'wb') as f:
    pickle.dump(qh, f)
  print(f"created pkl file {fname}")

if __name__ == "__main__":
  main()
