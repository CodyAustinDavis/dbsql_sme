SELECT task_id
  , task_params.src_format
  , ("/Volumes/" || :catalog_name || "/" || :schema_name || "/raw/" || task_params.src_path)::string AS src_path
  , task_params.src_header
  , task_params.src_sep
  , task_params.src_column_list
  , :catalog_name as bronze_catalog
  , :schema_name as bronze_schema
  , task_params.bronze_table
  , :catalog_name as silver_catalog
  , :schema_name as silver_schema
  , task_params.silver_table
  , task_params.primary_key
FROM read_files("/Volumes/" || :catalog_name || "/" || :schema_name || "/config/task.json"
  , format => 'json'
  , multiLine => 'true')
