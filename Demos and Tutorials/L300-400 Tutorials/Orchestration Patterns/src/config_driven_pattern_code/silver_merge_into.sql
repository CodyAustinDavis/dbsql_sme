USE CATALOG identifier(:tgt_catalog);
USE SCHEMA identifier(:tgt_schema);

CREATE TABLE IF NOT EXISTS IDENTIFIER(:merge_tgt_table)
TBLPROPERTIES (
    'delta.targetFileSize' = '1mb'
  , 'delta.feature.allowColumnDefaults' = 'supported'
  , 'delta.columnMapping.mode' = 'name'
  , 'delta.enableChangeDataFeed'= 'true'
  )
AS 
SELECT *, 0 as _src_commit_version FROM IDENTIFIER(:merge_src_table) LIMIT 0;

DECLARE OR REPLACE VARIABLE _most_recent_commit_version BIGINT;
DECLARE OR REPLACE VARIABLE merge_sql STRING;

SET VAR _most_recent_commit_version = (SELECT COALESCE(MAX(_src_commit_version),1) FROM IDENTIFIER(:merge_tgt_table));

SELECT _most_recent_commit_version;

MERGE WITH SCHEMA EVOLUTION INTO IDENTIFIER(:merge_tgt_table) AS target
USING (
    SELECT * EXCEPT (_commit_version), _commit_version AS _src_commit_version -- rename _commit_version so it's easier to understand in target table
    FROM table_changes(:merge_src_table, _most_recent_commit_version)
    WHERE 1=1
    AND _change_type != 'update_preimage'
    AND _commit_version > _most_recent_commit_version -- Only bring in new data from new CDF version, if no new data, gracefully merge no data
    QUALIFY ROW_NUMBER() OVER (PARTITION BY IDENTIFIER(:merge_primary_key) ORDER BY _commit_timestamp DESC) = 1 -- Get the most recent CDF behavior per unique record identifier just in case data source throws dup updates/deletes/inserts
) AS source
ON IDENTIFIER('source.' || :merge_primary_key) = IDENTIFIER('target.' || :merge_primary_key)
WHEN MATCHED 
    AND source._change_type IN ('insert' , 'update_postimage')
    AND ((source._last_modified_ts > target._last_modified_ts) OR target._last_modified_ts IS NULL) -- Check to make sure older / late records do not sneak in
THEN UPDATE
 SET * EXCEPT(_change_type, _commit_timestamp)
WHEN MATCHED 
    AND source._change_type = 'delete' -- NON SCD Type 2 Deletion - has some considerations and potential to re-run old data if not a single record retains the new CDF commit version
    AND ((source._last_modified_ts > target._last_modified_ts) OR source._last_modified_ts > target._last_modified_ts) 

THEN DELETE

WHEN NOT MATCHED AND source._change_type IN ('insert', 'update_postimage') -- Inserts + update for same record can be in same initial batch, where update wins as the most recent record
    -- Ignore 'delete' not matches, because you cant delete something that isnt there
    --- Then get most recent record if there are duplicate/multiple record updates in the source CDF batch
    THEN 
        INSERT * EXCEPT(_change_type, _commit_timestamp);
