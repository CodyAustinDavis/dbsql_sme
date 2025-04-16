-- Declare Variables
DECLARE OR REPLACE VARIABLE _create_table_sql STRING;
DECLARE OR REPLACE VARIABLE _copy_into_sql STRING;

-- Create Table
SET VAR _create_table_sql = 'CREATE TABLE IF NOT EXISTS ' || :tgt_catalog || '.' || :tgt_schema || '.' || :tgt_table || ' (' || :src_column_list || ' , _last_modified_ts timestamp' || ')
TBLPROPERTIES (delta.enableChangeDataFeed = true)';

SELECT _create_table_sql;
EXECUTE IMMEDIATE _create_table_sql;

-- COPY INTO Table
SET VAR _copy_into_sql = "COPY INTO " || :tgt_catalog || '.' || :tgt_schema || '.' || :tgt_table || "
BY POSITION
FROM (
  SELECT 
      *
      , now() AS _last_modified_ts
  FROM '" || :src_path || "'
  )
FILEFORMAT = "|| :src_format || "
FORMAT_OPTIONS ('mergeSchema' = 'true', 'sep' = '"|| :src_sep || "')
COPY_OPTIONS('mergeSchema' = 'true')";

SELECT _copy_into_sql;
EXECUTE IMMEDIATE _copy_into_sql;