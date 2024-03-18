# Databricks notebook source
# MAGIC %pip install -r helperfunctions/requirements.txt

# COMMAND ----------

from helperfunctions.dbsqlclient import ServerlessClient

# COMMAND ----------

# DBTITLE 1,Example Inputs For Client


token = None ## optional
host_name = None ## optional
warehouse_id = "475b94ddc7cd5211"

## Single Query Example
sql_statement = "SELECT 1"

## Multi Query Example
multi_statement = "SELECT 1; SELECT 2; SELECT concat_ws('-', M.id, N.id, random()) as ID FROM range(1000) AS M, range(1000) AS N LIMIT 10000000"

# COMMAND ----------

serverless_client = ServerlessClient(warehouse_id = warehouse_id, token=token, host_name=host_name) ## token=<optional>, host_name=<optional>verbose=True for print statements and other debugging messages

# COMMAND ----------

# DBTITLE 1,Basic sql drop-in command
"""
Optional Params:
1. full_results
2. use_catalog = <catalog> - this is a command specific USE CATALOG statement for the single SQL command
3. use_schema = <schema> - this is a command specific USE SCHEMA 

"""

result_df = serverless_client.sql(sql_statement=sql_statement) ## OPTIONAL: use_catalog="hive_metastore", use_schema="default"


display(result_df)

# COMMAND ----------

# DBTITLE 1,Multi Statement Command - No Results just Status - Recommended for production
"""
Optional Params:
1. full_results
2. use_catalog = <catalog> - this is a command specific USE CATALOG statement for the single SQL command
3. use_schema = <schema> - this is a command specific USE SCHEMA 

"""

result = serverless_client.submit_multiple_sql_commands(sql_statements = multi_statement, full_results=False) #session_catalog, session_schema are also optional parameters that will simulate a USE statement. True full_results just returns the whole API response for each query

# COMMAND ----------

# DBTITLE 1,Multi Statement Command Returning Results of Last Command - Best for simple processes
result_multi_df = serverless_client.submit_multiple_sql_commands_last_results(sql_statements = multi_statement)

# COMMAND ----------

display(result_multi_df)

# COMMAND ----------

# DBTITLE 1,If Multi Statement Fails, this is how to access the result chain
## The function save the state of each command in the chain, even if it fails to return results for troubleshooting

last_saved_multi_statement_state = serverless_client.multi_statement_result_state
print(last_saved_multi_statement_state)
