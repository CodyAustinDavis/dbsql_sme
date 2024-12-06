-- MUST Run with a Serverless Warehouse
DROP SCHEMA IF EXISTS main.dbsql_warehouse_advisor CASCADE;
CREATE SCHEMA IF NOT EXISTS main.dbsql_warehouse_advisor;
  -- LOCATION 's3://<location>/'; -- Optional location parameter
USE CATALOG main;
USE SCHEMA dbsql_warehouse_advisor;


CREATE OR REFRESH STREAMING TABLE main.dbsql_warehouse_advisor.warehouse_query_history
--TBLPROPERTIES ('pipelines.channel' = 'PREVIEW')
SCHEDULE CRON '0 0 * * * ? *' -- hourly
CLUSTER BY (workspace_id, warehouse_id, start_time)
COMMENT 'SQL Warehouse Query History with cleaned up exeuction metrics and query tags'
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
    COALESCE(CAST(total_duration_ms AS FLOAT) / 1000, 0) AS QueryRuntimeSeconds,
    COALESCE(CAST(total_task_duration_ms AS FLOAT) / 1000, 0) AS CPUTotalExecutionTime,
    COALESCE(CAST(execution_duration_ms AS FLOAT) / 1000, 0) AS ExecutionQueryTime, -- Included in Cost Per Query
    COALESCE(CAST(compilation_duration_ms AS FLOAT) / 1000, 0) AS CompilationQueryTime, -- Included in Cost Per Query
    COALESCE(CAST(waiting_for_compute_duration_ms AS FLOAT) / 1000, 0) + COALESCE(CAST(waiting_at_capacity_duration_ms AS FLOAT) / 1000, 0) AS QueueQueryTime,
    COALESCE(CAST(waiting_for_compute_duration_ms AS FLOAT) / 1000, 0) AS StartUpQueryTime,
    COALESCE(CAST(result_fetch_duration_ms AS FLOAT) / 1000, 0) AS ResultFetchTime,

    --- Query Work that is NOT dollar based allocation
        COALESCE(COALESCE(CAST(compilation_duration_ms AS FLOAT) / 1000, 0)
            + COALESCE(CAST(total_task_duration_ms AS FLOAT) / 1000, 0)
            + COALESCE(CAST(result_fetch_duration_ms AS FLOAT) / 1000, 0)
        ) AS QueryWork,
    -- Metric for query cost allocation - exclude metadata operations
    CASE 
        WHEN COALESCE(CAST(total_task_duration_ms AS FLOAT) / 1000, 0) = 0 
        THEN 0 
        ELSE QueryWork
    END AS TotalResourceTimeUsedForAllocation,
    -- Metric for query cost allocation - include metadata operations
    QueryWork AS TotalResourceTimeUsedForAllocationWithMetadata,
    start_time,
    end_time,
    update_time,
    COALESCE(read_bytes, 0) AS read_bytes,
    COALESCE(written_bytes, 0) AS written_bytes, -- New!
    COALESCE(read_io_cache_percent, 0) AS read_io_cache_percent,
    from_result_cache,
    COALESCE(spilled_local_bytes, 0) AS spilled_local_bytes,
    
    COALESCE(CAST(total_task_duration_ms AS FLOAT) / NULLIF(total_duration_ms, 0), NULL) AS TotalCPUTime_To_Execution_Time_Ratio, 
    (COALESCE(CAST(waiting_for_compute_duration_ms AS FLOAT), 0) + COALESCE(CAST(waiting_at_capacity_duration_ms AS FLOAT), 0)) / NULLIF(total_duration_ms, 0) AS ProportionQueueTime,
    (COALESCE(CAST(result_fetch_duration_ms AS FLOAT), 0) / NULLIF(total_duration_ms, 0)) AS ProportionResultFetchTime,
    -- Can use this to chargeback
    read_files AS FilesRead,
    pruned_files AS FilesPruned,
    CASE WHEN read_files > 0 OR pruned_files > 0
        THEN CAST(pruned_files AS FLOAT) / (CAST(read_files AS FLOAT) + CAST(pruned_files AS FLOAT))
    END AS FilesPrunedProportion,

    CASE 
        WHEN read_bytes > 0 
        THEN CAST(read_bytes AS FLOAT) / (1024 * 1024 * 1024) 
        ELSE 0 
    END AS ReadDataAmountInGB,

    CASE 
        WHEN written_bytes > 0 
        THEN CAST(read_bytes AS FLOAT) / (1024 * 1024 * 1024) 
        ELSE 0 
    END AS WrittenDataAmountInGB,

    CASE 
        WHEN spilled_local_bytes > 0 
        THEN CAST(read_bytes AS FLOAT) / (1024 * 1024 * 1024) 
        ELSE 0 
    END AS SpilledDataAmountInGB,


    CASE 
        WHEN read_io_cache_percent > 0 THEN 'Used Cache' 
        ELSE 'No Cache' 
    END AS UsedCacheFlag,

    CASE 
        WHEN spilled_local_bytes > 0 THEN 'Spilled Data To Disk' 
        ELSE 'No Spill' 
    END AS HasSpillFlag,

    CASE 
        WHEN read_bytes > 0 THEN 'Did Read Data' 
        ELSE 'No Data Read' 
    END AS ReadDataFlag,

    CASE 
        WHEN COALESCE(CAST(total_task_duration_ms AS FLOAT) / 1000, 0) > 0 THEN 'UsedWorkerTasks' 
        ELSE 'NoWorkers' 
    END AS UsedWorkerTasksFlag,

    -- Calculation Flags
    CASE 
        WHEN spilled_local_bytes > 0 THEN 1 
        ELSE 0 
    END AS Calc_HasSpillFlag,

    CASE 
        WHEN read_bytes > 0 THEN 0.25 
        ELSE 0 
    END AS Calc_ReadDataFlag,

    CASE 
        WHEN COALESCE(CAST(total_task_duration_ms AS FLOAT) / 1000, 0) > 0 THEN 0.25 
        ELSE 0 
    END AS Calc_UsedWorkerTasksFlag,

    -- Query Tagging
    REGEXP_REPLACE(
        REGEXP_EXTRACT(statement_text, '/\\*.*?QUERY_TAG:(.*?)(?=\\*/)', 1), 
        'QUERY_TAG:', ''
    ) AS raw_tagged,

    SUBSTR(statement_text, 
           INSTR(statement_text, '/*') + 2, 
           INSTR(statement_text, '*/') - INSTR(statement_text, '/*') - 2) AS dbt_metadata_json,

    -- Error Messages
    error_message,
    COALESCE(REGEXP_EXTRACT(error_message, '\\[(.*?)\\]', 1), 'NO ERROR') AS error_type,

    -- Optional DBT Metadata
    COALESCE(dbt_metadata_json:app, 'None') AS dbt_app,
    COALESCE(dbt_metadata_json:node_id, 'None') AS dbt_node_id,
    COALESCE(dbt_metadata_json:profile_name, 'None') AS dbt_profile_name,
    COALESCE(dbt_metadata_json:target_name, 'None') AS dbt_target_name,
    COALESCE(dbt_metadata_json:dbt_version, 'None') AS dbt_version,
    COALESCE(dbt_metadata_json:dbt_databricks_version, 'None') AS dbt_databricks_version,
    FROM_JSON(dbt_metadata_json, 'map<string,string>') AS parsed_dbt_comment,

    CASE 
        WHEN (LOWER(dbt_metadata_json:app) = 'dbt' OR client_application LIKE '%dbt%') 
        THEN 'DBT Query' 
        ELSE 'Other Query Type' 
    END AS IsDBTQuery,

-- NEW - Query source
    CASE WHEN query_source.job_info.job_id IS NOT NULL THEN 'JOB'
    WHEN query_source.legacy_dashboard_id IS NOT NULL THEN 'LEGACY DASHBOARD'
    WHEN query_source.dashboard_id IS NOT NULL THEN 'AI/BI DASHBOARD'
    WHEN query_source.alert_id IS NOT NULL THEN 'ALERT'
    WHEN query_source.notebook_id IS NOT NULL THEN 'NOTEBOOK'
    WHEN query_source.sql_query_id IS NOT NULL THEN 'SQL QUERY'
    WHEN query_source.genie_space_id IS NOT NULL THEN 'GENIE SPACE'
    ELSE 'UNKNOWN'
    END AS query_source_type,
coalesce(query_source.job_info.job_id, query_source.legacy_dashboard_id, query_source.dashboard_id, query_source.alert_id, query_source.notebook_id, query_source.sql_query_id, query_source.genie_space_id, 'UNKNOWN') AS query_source_id


FROM STREAM(system.query.history)
WHERE compute.warehouse_id IS NOT NULL -- Only SQL Warehouse Compute
AND statement_type IS NOT NULL
);

-- Warehouse Usage

CREATE OR REFRESH STREAMING TABLE main.dbsql_warehouse_advisor.warehouse_usage
SCHEDULE CRON '0 0 * * * ? *' -- hourly
CLUSTER BY (workspace_id, warehouse_id,usage_start_time)
COMMENT 'SQL Warehouse Usage'
AS 
SELECT 
usage_metadata.warehouse_id AS warehouse_id,
*
FROM STREAM(system.billing.usage)
WHERE usage_metadata.warehouse_id IS NOT NULL;


-- Warehouse Scaling History
CREATE OR REFRESH STREAMING TABLE main.dbsql_warehouse_advisor.warehouse_scaling_events
SCHEDULE CRON '0 0 * * * ? *' -- hourly
CLUSTER BY (warehouse_id,event_time)
COMMENT 'SQL Warehouse Scaling Events from warehouse_events table'
AS
SELECT * FROM STREAM(system.compute.warehouse_events);


-- Warehouse SCD History
-- Audit logs warehouse SCD history table (for names and other warehouse metadata such as sizing, owner, etc. )
CREATE OR REFRESH STREAMING TABLE main.dbsql_warehouse_advisor.warehouse_raw_events
SCHEDULE CRON '0 0 * * * ? *' -- hourly
CLUSTER BY (workspace_id, warehouse_id, event_time)
AS 
    SELECT 
        event_time, 
        workspace_id, 
        account_id, 
        action_name,
        response,
        request_params,
        user_identity.email AS warehouse_editor_user,
        CASE WHEN action_name IN ('createWarehouse', 'createEndpoint') THEN FROM_JSON(response['result'], 'Map<STRING, STRING>')['id'] 
            ELSE request_params.id
            END AS warehouse_id, 
        request_params.name AS warehouse_name, 
        request_params.warehouse_type AS warehouse_type,
        request_params.auto_stop_mins AS auto_stop_mins,
        request_params.cluster_size AS warehouse_size,
        CAST(request_params.min_num_clusters AS INTEGER) AS min_cluster_scaling,
        CAST(request_params.max_num_clusters AS INTEGER) AS max_cluster_scaing,
        CAST(request_params.channel:name AS STRING) AS warehouse_channel
    FROM STREAM(system.access.audit)
    WHERE service_name = 'databrickssql'
    AND action_name IN ('createWarehouse', 'createEndpoint', 'editWarehouse', 'editEndpoint', 'deleteWarehouse', 'deleteEndpoint')
    AND response.status_code = '200'
;


CREATE OR REPLACE VIEW main.dbsql_warehouse_advisor.warehouse_scd
COMMENT 'SQL Warehouse SCD Change History'
AS (
WITH edit_history AS (
    SELECT 
        *
    FROM main.dbsql_warehouse_advisor.warehouse_raw_events
    WHERE action_name IN ('createWarehouse', 'createEndpoint')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY warehouse_id
        ORDER BY event_time DESC
    ) = 1

    UNION ALL

    SELECT 
        *
    FROM main.dbsql_warehouse_advisor.warehouse_raw_events
    WHERE action_name IN ('editWarehouse', 'editEndpoint')

    UNION ALL

    SELECT 
        *
    FROM main.dbsql_warehouse_advisor.warehouse_raw_events
    WHERE action_name IN ('deleteWarehouse', 'deleteEndpoint')
)

SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY account_id, workspace_id, warehouse_id ORDER BY event_time) AS ChangeRank,
    ROW_NUMBER() OVER (PARTITION BY account_id, workspace_id, warehouse_id ORDER BY event_time DESC) AS RecencyRank,
    CASE 
        WHEN RecencyRank = 1 AND action_name IN ('deleteWarehouse', 'deleteEndpoint') 
        THEN 'Deleted' 
        ELSE 'Active' 
    END AS IsDeleted
FROM edit_history
);