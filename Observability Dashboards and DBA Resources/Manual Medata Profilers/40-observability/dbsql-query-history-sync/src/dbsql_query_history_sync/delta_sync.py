import datetime, dateutil
import sys, os
import json
import time

from delta.tables import DeltaTable

import pyspark.sql.types as T
import pyspark.sql.functions as F

from . import queries_api

class UpdateDBQueries:
  def __init__(self, spark_session, dbx_token, workspace_url,
               warehouse_ids, earliest_query_ts_ms, table_name, 
               user_ids=None, max_queries_batch=1000):
    self.spark = spark_session
    self.dbx_token = dbx_token
    self.workspace_url = workspace_url
    self.warehouse_ids = warehouse_ids
    self.earliest_query_ts_ms = earliest_query_ts_ms
    self.table_name = table_name
    self.user_ids = user_ids
    self.max_queries_batch = max_queries_batch

    # hidden state for optimization of update_db_repeat
    self._next_update_ts_ms_d = {}

    self._do_init()

  def _do_init(self):
    '''Check if table_name exists, and if it does not, create it.
    '''
    # We will use schema evolution when we need to add data.
    # That way we don't have to assume the returned results keep the same schema.
    # Add the minimum columns required for things to work.
    self.spark.sql(f"""
      create table if not exists {self.table_name}
      (query_id string, status string, query_start_time_ms bigint)""")
    # This is somewhat 'invasive' but this is not a open source api used by the unsuspecting masses
    # so I think this is ok.
    self.spark.sql(f"alter table {self.table_name} SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")

  def _merge_db_queries(self, from_ts_ms):
    print(f'_merge_db_queries(from_ts_ms={from_ts_ms})')
    start_ts_ms = from_ts_ms if from_ts_ms else self.earliest_query_ts_ms
    c = queries_api.sync_query_history(
      dbx_token=self.dbx_token, workspace_url=self.workspace_url,
      warehouse_ids=self.warehouse_ids, start_ts_ms=start_ts_ms,
      query_sink_fn=self._merge_results, sink_batch_size=self.max_queries_batch,
      user_ids=self.user_ids)
    return c

  def _merge_results(self, query_history):
    qh_df = self.spark.createDataFrame(query_history)
    ame = self.spark.conf.get('spark.databricks.delta.schema.autoMerge.enabled')
    if ame != 'true':
      self.spark.conf.set('spark.databricks.delta.schema.autoMerge.enabled', True)
    _table = DeltaTable.forName(self.spark, self.table_name)
    (_table.alias('t1').merge(qh_df.alias('n1'), 't1.query_id = n1.query_id')
      .whenMatchedUpdateAll()
      .whenNotMatchedInsertAll()
      .execute())
    if ame != 'true':
      self.spark.conf.set('spark.databricks.delta.schema.autoMerge.enabled', ame)
    qh_df.createOrReplaceTempView('qh_df')
    # Optimization.
    d = self._get_existing_ts(table_name='qh_df')
    if not self._next_update_ts_ms_d.get('pending'):
      print(f'_merge_results: updating _next_update_ts_ms_d with {d}')
      self._next_update_ts_ms_d.update(d)
    else:
      print(f'already have a pending {self._next_update_ts_ms_d}')

  def update_db(self):
    '''Update the db with queries. Check the table for existing data and query newer queries accordingly.
    '''
    d = self._get_existing_ts()
    existing_ts_ms = d['pending'] if d['pending'] else d['all']
    print(f'existing_ts_ms: {existing_ts_ms}')
    c = self._merge_db_queries(existing_ts_ms)
    print(f"got {c} queries")

  def _get_existing_ts(self, table_name=None):
    '''
    Get the timestamp that should be used to get the next queries.
    '''
    if not table_name:
      table_name = self.table_name
    r = self.spark.sql(
      f"""
        select * from (
        select 'pending' as status, min(query_start_time_ms) as ts_ms
        from {table_name}
        where lower(status) in ('queued', 'running'))
        union all
        (select 'all' as status, max(query_start_time_ms) as ts_ms
        from {table_name})            
      """).collect()
    assert(r[0][0] == 'pending' and r[1][0] == 'all')
    #ts_ms = r[0][1] if r[0][1] else r[1][1]
    #return ts_ms
    return dict(r)

  def update_db_repeat(self, interval_secs):
    '''Update the db with new queries every interval_secs.
    '''
    d = self._get_existing_ts()
    print(f'got initial state: {d}')
    ts_ms = d['pending'] if d['pending'] else d['all']
    while True:
      c = self._merge_db_queries(ts_ms)
      # self._next_update_ts_ms is kept updated inside self._merge_db_queries as an optimization
      print(f'self._next_update_ts_ms_d: {self._next_update_ts_ms_d}')
      ts_ms = self._next_update_ts_ms_d.get('pending') if self._next_update_ts_ms_d.get('pending') else self._next_update_ts_ms_d.get('all')
      self._next_update_ts_ms_d = {}
      print(f'merged {c} queries, updated ts_ms to {ts_ms}')
      print(f'sleeping {interval_secs}...')
      time.sleep(interval_secs)
