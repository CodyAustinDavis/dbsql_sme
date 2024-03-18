import re
import requests
import time


## Function to block Create or REFRESH of ST or MV statements to wait until it is finishing before moving to next task

## Similar to the awaitTermination() method in a streaming pipeline

## Only supports 1 sql statement at a time on purpose

def orchestrate_stmv_statement(spark, dbutils, sql_statement, host_name=None, token=None):

  host_name = None
  token = None

  ## Infer hostname from same workspace
  if host_name is not None:
    host_name = host_name

  else:
    host_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None).replace("https://", "")

  ## Automatically get user token if none provided
  if token is not None:
    token = token
  else: 
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)


  ## Get current catalogs/schemas from outside USE commands
  current_schema = spark.sql("SELECT current_schema()").collect()[0][0]
  current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]

  if current_catalog == 'spark_catalog':
    current_catalog = 'hive_metastore'


  ## Check for multiple statements, if more than 1, than raise too many statement exception
  all_statements = re.split(";", sql_statement)

  if (len(all_statements) > 1):
    print("WARNING: There are more than one statements in this sql command, this function will just pick and try to run the first statement and ignore the rest.")


  sql_statement = all_statements[0]


  try:

    ## Get table/mv that is being refreshed
    table_match = re.split("CREATE OR REFRESH STREAMING TABLE\s|REFRESH STREAMING TABLE\s|CREATE OR REFRESH MATERIALIZED VIEW\s|REFRESH MATERIALIZED VIEW\s", sql_statement.upper())[1].split(" ")[0]

  except Exception as e:

    ## If it was not able to find a REFRESH statement, ignore and unblock the operation and move on (i.e. if its not an ST/MV or if its just a CREATE)

    print("WARNING: No ST / MV Refresh statements found. Moving on.")
    return
  
  ## If ST/MV refresh was found
  
  if (len(table_match.split(".")) == 3):
    ## fully qualified, dont change it
    pass
  elif (len(table_match.split(".")) == 2):
    table_match = current_catalog + "." + table_match

  elif(len(table_match.split(".")) == 1):
    table_match = current_catalog + "." + current_schema + "." + table_match


  ## Step 2 - Execute SQL Statement
  spark.sql(sql_statement)


  ## Step 3 - Get pipeline Id for table 
  active_pipeline_id = (spark.sql(f"DESCRIBE DETAIL {table_match}")
    .selectExpr("properties").take(1)[0][0]
    .get("pipelines.pipelineId")
  )

  ## Poll for pipeline status
  

  current_state = "UNKNOWN"

  ## Pipeline is active 
  while current_state not in ("FAILED", "IDLE"):

    url = "https://" + host_name + "/api/2.0/pipelines/"
    headers_auth = {"Authorization":f"Bearer {token}"}

    check_status_resp = requests.get(url + active_pipeline_id , headers=headers_auth).json()

    current_state = check_status_resp.get("state")

    if current_state == "IDLE":
      print(f"STMV Pipeline {active_pipeline_id} completed! \n Moving on")
      return
    
    elif current_state == "FAILED":
      raise(BaseException(f"PIPELINE {active_pipeline_id} FAILED!"))
    

    else:
      ## Wait before polling again
      ## TODO: Do exponential backoff
      time.sleep(5)

    