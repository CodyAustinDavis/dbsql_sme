# Databricks notebook source

# MAGIC %run ../src/query_sla_manager

# COMMAND ----------

# DBTITLE 1,Notebook Secrets / Params
host = "<host_name>"
dbx_token = "<dbx_token>"
database_name = "main.query_alert_manager"

# COMMAND ----------

# DBTITLE 1,Initialize an SLA Policy Manager
query_manager = QueryHistoryAlertManager(host=host, 
                                         dbx_token = dbx_token, 
                                         database_name=database_name ## Optional - default is main.query_alert_manager
                                         )

# COMMAND ----------

# DBTITLE 1,Idempotent Adding of Policies (by unique policy)
query_manager.add_sla_policy(warehouse_ids=["475b94ddc7cd5211"], sla_seconds=10)

# COMMAND ----------

query_manager.optimize_database()

# COMMAND ----------

# DBTITLE 1,How to access, remove, and get all SLA policies
#active_policy = query_manager.get_sla_policy(policy_id = 1)
all_policies = query_manager.get_all_policies_df()
#query_manager.remove_sla_policy(policy_id=1)
display(all_policies)

# COMMAND ----------

# DBTITLE 1,Ability to drop database state
#query_manager.drop_database_if_exists()

# COMMAND ----------

# DBTITLE 1,Command to Create a new DB if deleted / not exists
#query_manager.initialize_database()

# COMMAND ----------

# DBTITLE 1,Polling Loop - Schedule in a Job for Each Policy
batch_results = query_manager.poll_with_policy(policy_id=3, polling_frequency_seconds=10, hard_fail_after_n_attempts=1, start_over=False)
