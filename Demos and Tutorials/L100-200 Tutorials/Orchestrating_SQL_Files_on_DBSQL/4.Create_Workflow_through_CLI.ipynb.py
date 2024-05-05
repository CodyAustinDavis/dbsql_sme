# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Job Create using CLI

# COMMAND ----------

databricks jobs create --json '{
    "name": "Orchestrating_SQL_Files_on_DBSQL",
    "email_notifications": {
        "no_alert_for_skipped_runs": false
    },
    "webhook_notifications": {},
    "timeout_seconds": 0,
    "max_concurrent_runs": 1,
    "tasks": [
        {
            "task_key": "Create_Tables",
            "run_if": "ALL_SUCCESS",
            "sql_task": {
                "file": {
                    "path": "<GIT PATH>/1.Create_Tables.sql",
                    "source": "GIT"
                },
                "warehouse_id": "<WAREHOUSE ID>"
            },
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "webhook_notifications": {}
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
                    "path": "<GIT PATH>/2.Load_Data.sql",
                    "source": "WORKSPACE"
                },
                "warehouse_id": "<WAREHOUSE ID>"
            },
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "webhook_notifications": {}
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
                    "path": "<GIT PATH>/3.Query_Fact_Sales.sql",
                    "source": "WORKSPACE"
                },
                "warehouse_id": "<WAREHOUSE ID>"
            },
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "webhook_notifications": {}
        }
    ],
    "git_source": {
        "git_url": "https://github.com/<GITUSERNAME>/dbsql_sme.git",
        "git_provider": "gitHub",
        "git_branch": "feature_branch_sqlfiles"
    },
    "queue": {
        "enabled": true
    },
    "parameters": [
        {
            "name": "catalog_param",
            "default": "main"
        },
        {
            "name": "schema_param",
            "default": "default"
        }
    ],
    "run_as": {
        "user_name": "<username@domain>.com"
    }
}'
