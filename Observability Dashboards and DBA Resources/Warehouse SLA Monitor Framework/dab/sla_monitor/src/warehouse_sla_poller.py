# Databricks notebook source
dbutils.widgets.text("workspace_name", "my-workspace.cloud.databricks.com")
dbutils.widgets.text("database_name", "hive_metastore.main.query_alert_manager")
dbutils.widgets.text("policy_id", "1")

# COMMAND ----------

workspace_name = dbutils.widgets.get("workspace_name")
pat_token = dutils.secrets.get("my-secret-scope", "my-pat-token")
database_name = dbutils.widgets.get("database_name")
policy_id = dbutils.widgets.get("policy_id")

# COMMAND ----------

# MAGIC %run ./query_sla_manager

# COMMAND ----------

# Initialize an SLA Policy Manager
query_manager = QueryHistoryAlertManager(host=workspace_name, 
                                         dbx_token = pat_token, 
                                         database_name=database_name
                                         )

# COMMAND ----------

# Polling Loop - Schedule in a Job for Each Policy
batch_results = (query_manager.poll_with_policy(
  policy_id=1,
  polling_frequency_seconds=30,
  hard_fail_after_n_attempts=3,
  start_over=False))

# COMMAND ----------


