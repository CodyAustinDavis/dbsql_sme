# Databricks notebook source
# DBTITLE 1,Check if spark can read the table
def check_table_exist(db_tbl_name):
    table_exist = False
    try:
        spark.read.table(db_tbl_name) # Check if spark can read the table
        table_exist = True        
    except:
        pass
    return table_exist

# COMMAND ----------

# DBTITLE 1,Current time in milliseconds
def current_time_in_millis():
    return round(time.time() * 1000)

# COMMAND ----------

# DBTITLE 1,True False fix
def get_boolean_keys(arrays):
  # A quirk in Python's and Spark's handling of JSON booleans requires us to converting True and False to true and false
  boolean_keys_to_convert = []
  for array in arrays:
    for key in array.keys():
      if type(array[key]) is bool:
        boolean_keys_to_convert.append(key)
  #print(boolean_keys_to_convert)
  return boolean_keys_to_convert

# COMMAND ----------

# DBTITLE 1,Turn API results into json
def result_to_json(result):
  return json.dumps(result.json())

# COMMAND ----------

# DBTITLE 1,Get specific page results (Dashboards API only)
def get_page_result(base_url, page, auth):
  return requests.get(f'{base_url}&page={page}&order=executed_at', headers=auth)

# COMMAND ----------

# DBTITLE 1,Get specific offset results (Workflows API only)
def get_offset_result(base_url, offest, auth):
  return requests.get(f'{base_url}&offset={offest}', headers=auth)
