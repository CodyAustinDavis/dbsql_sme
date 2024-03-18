# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Delta Merge Helpers:
# MAGIC
# MAGIC <p4> This is class with a set of static methods that help the user easily perform retry statements on operataions that may be cause a lot of conflicting transactions (usually in MERGE / UPDATE statements). 
# MAGIC   
# MAGIC <li> <b> 1 Method: retrySqlStatement(spark: SparkSession, operation_name: String, sqlStatement: String) </b> - the spark param is your existing Spark session, the operation name is simply an operation to identify your transaction, the sqlStatement parameter is the SQL statement you want to retry. 

# COMMAND ----------

# MAGIC %pip install -r helperfunctions/requirements.txt

# COMMAND ----------

from helperfunctions.deltahelpers import DeltaMergeHelpers

# COMMAND ----------


sql_statement = """
MERGE INTO iot_dashboard.silver_sensors AS target
USING (SELECT Id::integer,
              device_id::integer,
              user_id::integer,
              calories_burnt::decimal,
              miles_walked::decimal,
              num_steps::decimal,
              timestamp::timestamp,
              value::string
              FROM iot_dashboard.bronze_sensors) AS source
ON source.Id = target.Id
AND source.user_id = target.user_id
AND source.device_id = target.device_id
WHEN MATCHED THEN UPDATE SET 
  target.calories_burnt = source.calories_burnt,
  target.miles_walked = source.miles_walked,
  target.num_steps = source.num_steps,
  target.timestamp = source.timestamp
WHEN NOT MATCHED THEN INSERT *;
"""

DeltaMergeHelpers.retrySqlStatement(spark, "merge_sensors", sqlStatement=sql_statement)
