# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient

from databricks.sdk.service.sql import *

w = WorkspaceClient()
me = w.current_user.me()
print(f'My email is {me.user_name}')

# COMMAND ----------

job = w.jobs.create(name=f'Job created from SDK by {w.current_user.me().user_name}',
                    tasks=[Task(task_key='main',
                                notebook_task=NotebookTask(notebook_path),
                                existing_cluster_id=info.cluster_id)])

# COMMAND ----------

job = w.jobs.create(
  name=f"Orchestrating_SQL_Files_on_DBSQL",
  tasks=[{task_key="Create_Tables","run_if"="ALL_SUCCESS","sql_task"={"file"={"path"="Demos and Tutorials/L100-200 Tutorials/1.Create_Tables.sql","source"="GIT"},"warehouse_id"="d1184b8c2a8a87eb"},"timeout_seconds"=0,"email_notifications"={},},
         {task_key="Load_Data","depends_on"=[{"task_key"="Create_Tables"}],"run_if"="ALL_SUCCESS","sql_task"={"file"={"path"="/Repos/saurabh.shukla@databricks.com/dbsql_sme/Demos and Tutorials/L100-200 Tutorials/2.Load_Data.sql","source"="WORKSPACE"},"warehouse_id"="d1184b8c2a8a87eb"},"timeout_seconds"=0,"email_notifications"={},"notification_settings"={"no_alert_for_skipped_runs"=False,"no_alert_for_canceled_runs"=False,"alert_on_last_attempt"=False},"webhook_notifications"={}},{"task_key"="Query_Fact_Sales","depends_on"=[{"task_key"="Load_Data"}],"run_if"="ALL_SUCCESS","sql_task"={"file"={"path"="/Repos/saurabh.shukla@databricks.com/dbsql_sme/Demos and Tutorials/L100-200 Tutorials/3.Query_Fact_Sales.sql","source"="WORKSPACE"},"warehouse_id"="d1184b8c2a8a87eb"},"timeout_seconds"=0,"email_notifications"={},"notification_settings"={"no_alert_for_skipped_runs"=False,"no_alert_for_canceled_runs"=False,"alert_on_last_attempt"=False},"webhook_notifications"={}}],"git_source"={"git_url"="https=//github.com/saurabhshukla-db/dbsql_sme.git","git_provider"="gitHub","git_branch"="feature_branch_sqlfiles"},"queue"={"enabled"=True},"parameters"=[{"name"="catalog","default"="samples"},{"name"="schema","default"="tpch"},{"name"="table","default"="customer"}],"run_as"={"user_name"="saurabh.shukla@databricks.com"}
)
print(f'Created job with id= {job.job_id}')

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks current-user me

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks jobs create --json '
# MAGIC {"name":"Orchestrating_SQL_Files_on_DBSQL_WAREHOUSE",
# MAGIC "tasks":[{
# MAGIC   "task_key":"Create_Tables","run_if":"ALL_SUCCESS","sql_task":{"file":{"path":"Demos and Tutorials/L100-200 Tutorials/1.Create_Tables.sql","source":"GIT"},"warehouse_id":"d1184b8c2a8a87eb"}},
# MAGIC   {"task_key":"Load_Data","depends_on":[{"task_key":"Create_Tables"}],"run_if":"ALL_SUCCESS","sql_task":{"file":{"path":"/Repos/saurabh.shukla@databricks.com/dbsql_sme/Demos and Tutorials/L100-200 Tutorials/2.Load_Data.sql","source":"WORKSPACE"},"warehouse_id":"d1184b8c2a8a87eb"}},
# MAGIC   {"task_key":"Query_Fact_Sales","depends_on":[{"task_key":"Load_Data"}],"run_if":"ALL_SUCCESS","sql_task":{"file":{"path":"/Repos/saurabh.shukla@databricks.com/dbsql_sme/Demos and Tutorials/L100-200 Tutorials/3.Query_Fact_Sales.sql","source":"WORKSPACE"},"warehouse_id":"d1184b8c2a8a87eb"}}],"git_source":{"git_url":"https://github.com/saurabhshukla-db/dbsql_sme.git","git_provider":"gitHub","git_branch":"feature_branch_sqlfiles"},"run_as":{"user_name":"saurabh.shukla@databricks.com"}}'

# COMMAND ----------


databricks jobs create --json '
{
  "name": "Orchestrating_SQL_Files_on_DBSQL_WAREHOUSE",
  "tasks": [
    {
      "task_key": "Create_Tables",
      "run_if": "ALL_SUCCESS",
      "sql_task": {
        "file": {
          "path": "Demos and Tutorials/L100-200 Tutorials/1.Create_Tables.sql",
          "source": "GIT"
        },
        "warehouse_id": "d1184b8c2a8a87eb"
      }
    },
    {
      "task_key": "Load_Data",
      "depends_on": [
        {
          "task_key": "Create_Tables"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "sql_task": {
        "file": {
          "path": "/Repos/saurabh.shukla@databricks.com/dbsql_sme/Demos and Tutorials/L100-200 Tutorials/2.Load_Data.sql",
          "source": "WORKSPACE"
        },
        "warehouse_id": "d1184b8c2a8a87eb"
      }
    },
    {
      "task_key": "Query_Fact_Sales",
      "depends_on": [
        {
          "task_key": "Load_Data"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "sql_task": {
        "file": {
          "path": "/Repos/saurabh.shukla@databricks.com/dbsql_sme/Demos and Tutorials/L100-200 Tutorials/3.Query_Fact_Sales.sql",
          "source": "WORKSPACE"
        },
        "warehouse_id": "d1184b8c2a8a87eb"
      }
    }
  ],
  "git_source": {
    "git_url": "https://github.com/saurabhshukla-db/dbsql_sme.git",
    "git_provider": "gitHub",
    "git_branch": "feature_branch_sqlfiles"
  },
  "run_as": {
    "user_name": "saurabh.shukla@databricks.com"
  }
}
'

# COMMAND ----------

root@0416-033219-vyl9ydk-10-139-64-137:/databricks/driver# 
databricks jobs create --json '
{
  "name": "Orchestrating_SQL_Files_on_DBSQL_WAREHOUSE",
  "tasks": [
    {
      "task_key": "Create_Tables",
      "run_if": "ALL_SUCCESS",
      "sql_task": {
        "file": {
          "path": "Demos and Tutorials/L100-200 Tutorials/1.Create_Tables.sql",
          "source": "GIT"
        },
        "warehouse_id": "d1184b8c2a8a87eb"
      }
    },
    {
      "task_key": "Load_Data",
      "depends_on": [
        {
          "task_key": "Create_Tables"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "sql_task": {
        "file": {
          "path": "/Repos/saurabh.shukla@databricks.com/dbsql_sme/Demos and Tutorials/L100-200 Tutorials/2.Load_Data.sql",
          "source": "WORKSPACE"
        },
        "warehouse_id": "d1184b8c2a8a87eb"
      }
    },
    {
      "task_key": "Query_Fact_Sales",
      "depends_on": [
        {
          "task_key": "Load_Data"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "sql_task": {
        "file": {
          "path": "/Repos/saurabh.shukla@databricks.com/dbsql_sme/Demos and Tutorials/L100-200 Tutorials/3.Query_Fact_Sales.sql",
          "source": "WORKSPACE"
        },
        "warehouse_id": "d1184b8c2a8a87eb"
      }
    }
  ],
  "git_source": {
    "git_url": "https://github.com/saurabhshukla-db/dbsql_sme.git",
    "git_provider": "gitHub",
    "git_branch": "feature_branch_sqlfiles"
  },
  "run_as": {
    "user_name": "saurabh.shukla@databricks.com"
  }
}
'
