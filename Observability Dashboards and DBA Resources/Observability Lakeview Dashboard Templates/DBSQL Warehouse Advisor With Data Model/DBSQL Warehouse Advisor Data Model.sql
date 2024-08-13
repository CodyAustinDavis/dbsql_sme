
--DROP SCHEMA IF EXISTS main.dbsql_warehouse_advisor CASCADE;
CREATE SCHEMA IF NOT EXISTS main.dbsql_warehouse_advisor;
USE CATALOG main;
USE SCHEMA dbsql_warehouse_advisor;


CREATE MATERIALIZED VIEW IF NOT EXISTS main.dbsql_warehouse_advisor.warehouse_query_history
COMMENT 'SQL Warehouse Query History with cleaned up exeuction metrics and query tags'
SCHEDULE CRON '0 0 0 * * ? *'
TBLPROPERTIES ('pipelines.autoOptimize.zOrderCols'= 'warehouse_id,start_time')
AS 
(
SELECT
account_id, 
workspace_id, 
statement_id,
executed_by,
statement_text,
compute.warehouse_id AS warehouse_id,
execution_status,
statement_type,
COALESCE(client_application, 'Unknown') AS client_application,
COALESCE(try_divide(total_duration_ms, 1000), 0) AS QueryRuntimeSeconds,
COALESCE(try_divide(total_task_duration_ms, 1000), 0) AS CPUTotalExecutionTime,
COALESCE(try_divide(execution_duration_ms, 1000), 0) AS ExecutionQueryTime, -- Included in Cost Per Query
COALESCE(try_divide(compilation_duration_ms, 1000), 0) AS CompilationQueryTime, -- Included in Cost Per Query
COALESCE(try_divide(waiting_at_capacity_duration_ms, 1000), 0) AS QueueQueryTime,
COALESCE(try_divide(waiting_for_compute_duration_ms, 1000), 0) AS StartUpQueryTime,
COALESCE(try_divide(result_fetch_duration_ms, 1000), 0) AS ResultFetchTime,
-- Metric for query cost allocation - -- exclude metadata operations
CASE WHEN COALESCE(try_divide(total_task_duration_ms, 1000),0) = 0 
    THEN 0 
    ELSE COALESCE(try_divide(total_duration_ms, 1000), 0)  + COALESCE(try_divide(compilation_duration_ms, 1000), 0) -- Query total time is compile time + execution time
    END AS TotalResourceTimeUsedForAllocation,
start_time,
end_time,
update_time,
COALESCE(read_bytes, 0) AS read_bytes,
COALESCE(written_bytes, 0) AS written_bytes, -- New!
COALESCE(read_io_cache_percent, 0) AS read_io_cache_percent,
from_result_cache,
COALESCE(spilled_local_bytes, 0) AS spilled_local_bytes,
COALESCE(total_task_duration_ms / total_duration_ms, NULL) AS TotalCPUTime_To_Execution_Time_Ratio, --execution time does seem to vary across query type, using total time to standardize
COALESCE(waiting_at_capacity_duration_ms / total_duration_ms, 0) AS ProportionQueueTime,
AVG(try_divide(total_duration_ms, 1000))  OVER () AS WarehouseAvgQueryRuntime,
AVG(try_divide(waiting_at_capacity_duration_ms, 1000)) OVER () AS WarehouseAvgQueueTime,
AVG(COALESCE(try_divide(waiting_at_capacity_duration_ms, 1000) / try_divide(total_duration_ms, 1000), 0)) OVER () AS WarehouseAvgProportionTimeQueueing,
-- Can use this to chargeback (as long as you know denominator is only USED task time, not including idele time)
CASE WHEN read_bytes > 0 THEN try_divide(read_bytes,(1024*1024*1024))ELSE 0 END AS ReadDataAmountInGB,
CASE WHEN read_io_cache_percent > 0 THEN 'Used Cache' ELSE 'No Cache' END AS UsedCacheFlag,
CASE WHEN spilled_local_bytes > 0 THEN 'Spilled Data To Disk' ELSE 'No Spill' END AS HasSpillFlag,
CASE WHEN read_bytes > 0 THEN 'Did Read Data' ELSE 'No Data Read' END AS ReadDataFlag,
CASE WHEN CPUTotalExecutionTime > 0 THEN 'UsedWorkerTasks' ELSE 'NoWorkers' END AS UsedWorkerTasksFlag,

--CASE WHEN QueryRuntimeSeconds >= :long_running_seconds_treshold::float THEN 'Long Running' ELSE 'Short Running' END AS LongRunningQueryFlag,
CASE WHEN spilled_local_bytes > 0 THEN 1 ELSE 0 END AS Calc_HasSpillFlag,
CASE WHEN read_bytes > 0 THEN 0.25 ELSE 0 END AS Calc_ReadDataFlag,
CASE WHEN CPUTotalExecutionTime > 0 THEN 0.25 ELSE 0 END AS Calc_UsedWorkerTasksFlag,
--CASE WHEN QueryRuntimeSeconds >= :long_running_seconds_treshold::float THEN 1 ELSE 0 END AS Calc_LongRunningQueryFlag,
-- Query Tagging 
regexp_replace(
regexp_extract(
statement_text, 
r'/\*.*?QUERY_TAG:(.*?)(?=\*/)',
1
), 
'QUERY_TAG:',
''
) AS raw_tagged,
substr(statement_text, INSTR(statement_text, '/*') + 2, INSTR(statement_text, '*/') - INSTR(statement_text, '/*') - 2) AS dbt_metadata_json,
-- Optional DBT Metadata
COALESCE(dbt_metadata_json:app, 'None') AS dbt_app,
COALESCE(dbt_metadata_json:node_id, 'None') AS dbt_node_id,
COALESCE(dbt_metadata_json:profile_name, 'None') AS dbt_profile_name,
COALESCE(dbt_metadata_json:target_name, 'None') AS dbt_target_name,
COALESCE(dbt_metadata_json:dbt_version, 'None') AS dbt_version,
COALESCE(dbt_metadata_json:dbt_databricks_version, 'None') AS dbt_databricks_version,
from_json(dbt_metadata_json, 'map<string,string>') AS parsed_dbt_comment,
CASE WHEN (lower(dbt_metadata_json:app) = "dbt" OR client_application LIKE ('%dbt%')) THEN 'DBT Query' ELSE 'Other Query Type' END AS IsDBTQuery
FROM system.query.history
WHERE compute.warehouse_id IS NOT NULL -- Only SQL Warehouse Compute
AND statement_type IS NOT NULL
);


-- Warehouse Usage
CREATE MATERIALIZED VIEW IF NOT EXISTS main.dbsql_warehouse_advisor.warehouse_usage
COMMENT 'SQL Warehouse Usage'
SCHEDULE CRON '0 0 0 * * ? *'
TBLPROPERTIES ('pipelines.autoOptimize.zOrderCols'= 'warehouse_id,usage_start_time')
AS 
SELECT 
usage_metadata.warehouse_id AS warehouse_id,
*
FROM system.billing.usage
WHERE usage_metadata.warehouse_id IS NOT NULL;


-- Warehouse Scaling History
CREATE MATERIALIZED VIEW IF NOT EXISTS main.dbsql_warehouse_advisor.warehouse_scaling_events
COMMENT 'SQL Warehouse Scaling Events from warehouse_events table'
SCHEDULE CRON '0 0 0 * * ? *'
TBLPROPERTIES ('pipelines.autoOptimize.zOrderCols'= 'warehouse_id,event_time')
AS
SELECT * FROM system.compute.warehouse_events;


-- Warehouse SCD History
-- Audit logs warehouse SCD history table (for names and other warehouse metadata such as sizing, owner, etc. )
CREATE MATERIALIZED VIEW IF NOT EXISTS main.dbsql_warehouse_advisor.warehouse_scd
COMMENT 'SQL Warehouse SCD Change History'
SCHEDULE CRON '0 0 0 * * ? *'
TBLPROPERTIES ('pipelines.autoOptimize.zOrderCols'= 'event_time,warehouse_id')
AS (

WITH warehouse_raw_events AS (
  
    SELECT 
    event_time, 
    workspace_id, 
    account_id, 
    action_name,
    response,
    request_params,
    user_identity.email AS warehouse_editor_user
    from system.access.audit
    where service_name = 'databrickssql'
    and action_name in ('createWarehouse', 'createEndpoint', 'editWarehouse', 'editEndpoint', 'deleteWarehouse', 'deleteEndpoint')
    AND response.status_code = '200'
),

edit_history AS (
    
    SELECT 
    event_time, 
    workspace_id, 
    account_id, 
    action_name,
    from_json(response ['result'], 'Map<STRING, STRING>') ["id"] as warehouse_id, 
    request_params.name as warehouse_name, 
    request_params.warehouse_type AS warehouse_type,
    request_params.auto_stop_mins AS auto_stop_mins,
    request_params.cluster_size AS warehouse_size,
    request_params.min_num_clusters::int AS min_cluster_scaling,
    request_params.max_num_clusters::int AS max_cluster_scaing,
    request_params.channel:name AS warehouse_channel,
    warehouse_editor_user
    from warehouse_raw_events
    WHERE action_name in ('createWarehouse', 'createEndpoint')
    QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY warehouse_id
        ORDER BY
          event_time DESC
      ) = 1

      UNION ALL 

      SELECT 
      event_time, 
      workspace_id, 
      account_id, 
      action_name,
      request_params.id as warehouse_id,
      request_params.name as warehouse_name,
      request_params.warehouse_type AS warehouse_type,
      request_params.auto_stop_mins AS auto_stop_mins,
      request_params.cluster_size AS warehouse_size,
      request_params.min_num_clusters::int AS min_cluster_scaling,
      request_params.max_num_clusters::int AS max_cluster_scaing,
      request_params.channel:name AS warehouse_channel,
      warehouse_editor_user
      from warehouse_raw_events
      where
      action_name in ('editWarehouse', 'editEndpoint')

    UNION ALL
    SELECT 
    event_time, 
    workspace_id, 
    account_id, 
    action_name,
    request_params.id as warehouse_id,
    NULL as warehouse_name, 
    NULL AS warehouse_type,
    NULL AS auto_stop_mins,
    NULL AS warehouse_size,
    NULL AS min_cluster_scaling,
    NULL AS max_cluster_scaing,
    NULL AS warehouse_channel,
    warehouse_editor_user
    from warehouse_raw_events
    where
    action_name in ('deleteWarehouse', 'deleteEndpoint')
)

SELECt *,
ROW_NUMBER() OVER (PARTITION BY account_id, workspace_id, warehouse_id ORDER BY event_time) AS ChangeRank,
ROW_NUMBER() OVER (PARTITION BY account_id, workspace_id, warehouse_id ORDER BY event_time DESC) AS RecencyRank,
CASE WHEN RecencyRank = 1 AND action_name in ('deleteWarehouse', 'deleteEndpoint') THEN 'Deleted' ELSE 'Active' END AS IsDeleted
FROM edit_history
);


--DROP MATERIALIZED VIEW  main.dbsql_warehouse_advisor.warehouse_current;

CREATE MATERIALIZED VIEW IF NOT EXISTS main.dbsql_warehouse_advisor.warehouse_current
COMMENT 'SQL Warehouse Current Definition - including deleted flag'
SCHEDULE CRON '0 0 0 * * ? *'
TBLPROPERTIES ('pipelines.autoOptimize.zOrderCols'= 'event_time,warehouse_id')
AS (

WITH active_warehouses AS (
select --warehouse_id, warehouse_name 
*,
MIN_BY(warehouse_editor_user, event_time) OVER (PARTITION BY account_id, workspace_id, warehouse_id) as warehouse_creator,
MAX_BY(warehouse_editor_user, event_time) OVER (PARTITION BY account_id, workspace_id, warehouse_id) as warehouse_latest_editor
from (SELECT * FROM main.dbsql_warehouse_advisor.warehouse_scd WHERE action_name in ('createWarehouse', 'createEndpoint', 'editWarehouse', 'editEndpoint')) -- Exclude delete events
QUALIFY (ROW_NUMBER() OVER (PARTITION BY account_id, workspace_id, warehouse_id ORDER BY event_time DESC) = 1)
),

with_delete_flag AS (
SELECT *,
CASE WHEN (SELECT COUNT(0) 
        from main.dbsql_warehouse_advisor.warehouse_scd AS innr
        where
         action_name in ('deleteWarehouse', 'deleteEndpoint')
        AND innr.warehouse_id = aw.warehouse_id ) > 0 THEN 'Deleted' ELSE 'Active' END AS IsDeletedCurrently
FROM active_warehouses AS aw
)

SELECT * FROM with_delete_flag
);