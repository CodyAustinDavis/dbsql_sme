Sync a Delta table with the query history from a dbsql warehouse.

As easy as:

pip install from dist/dbsql_query_history_sync-0.0.1-py3-none-any.whl or dist/dbsql_query_history_sync-0.0.1.tar.gz

To download the query history without Databricks environment or pyspark (need to change the dbsql host, warehouse_ids and access token):
```
> cd examples
> ./standalone_dbsql_get_query_history_example.py
```

To create a Delta table and continuously sync queries from the dbsql warehouses to it:

```
import dbsql_query_history_sync.delta_sync as delta_sync

# create the object
udbq = delta_sync.UpdateDBQueries(dbx_token=DBX_TOKEN, 
                       warehouse_ids=warehouse_ids_list, earliest_query_ts_ms=dt_ts, table_name=sync_table)
udbq.update_db_repeat(interval_secs=10)
```

See examples/dbsql_query_sync_example.py.

For questions contact nishant.deshpande@databricks.com.

