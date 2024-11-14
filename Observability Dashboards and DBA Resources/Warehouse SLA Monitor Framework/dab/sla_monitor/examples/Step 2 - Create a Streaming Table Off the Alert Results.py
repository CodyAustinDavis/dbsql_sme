# Databricks notebook source

# DBTITLE 1,Define Alert Stream Paramters
database_name = "main.query_alert_manager"
checkpoint_location = "/dbfs/tmp/query_alert_manager/checkpoints" ## Define you checkpoint location for your SLA alert
trigger_interval = "30 seconds"

# COMMAND ----------

# DBTITLE 1,Define Alert ReadStream
alerts_df = spark.readStream.table(f"{database_name}.query_history_statistics")

# COMMAND ----------

# DBTITLE 1,Logic to package webhook messages
def send_webhook_alert(alerts_df):

    import requests
    import json
    webhook_url = 'https://your.webhook.url'

    alerts_df = alerts_df.select("query_id", "policy_duration_seconds", "policy_id","policy_sla_fail_reason", "executed_as_user_name").collect()

    alerts_json_payload = json.dumps([{"query_id": i[0], "policy_duration_at_time_of_poll": i[1], "policy_id": i[2], "policy_sla_fail_reason": i[3], "query_user": i[4]} for i in alerts_df])


    payload = {
        "message": f"SLA ALERTS: The alert query count is {alerts_df.count()}. Here are the following queries currently out of policy \n {alerts_json_payload}"
    }
    response = requests.post(webhook_url, json=payload)

    if response.status_code != 200:
        print(f"Failed to send webhook: {response.text}")
    else:
        print(f"Alert Webhook Sent for {alerts_df.count()} alerts.")

# COMMAND ----------

# DBTITLE 1,Define Alerting Foreach Batch
def send_alert(alerts_df, id):
  alert_count = alerts_df.count()
  print(f"Sending alerts for {alert_count}")

  ### Uncomment after you have configured your webhook
  #send_webhook_alert(alerts_df)

  return

# COMMAND ----------

# DBTITLE 1,Write Stream with Configured Trigger Interval
(alerts_df
 .writeStream
 .foreachBatch(send_alert)
 .trigger(processingTime=trigger_interval)
 .start()
)
