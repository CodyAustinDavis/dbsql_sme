# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Using Delta Helpers Materialization Class. 
# MAGIC
# MAGIC <p3> This class is for the purpose of materializing tables with delta onto cloud storage. This is often helpful for debugging and for simplifying longer, more complex query pipelines that would otherwise require highly nested CTE statements. Often times, the plan is simplified and performane is improved by removing the lazy evaluation and creating "checkpoint" steps with a materialized temp_db. Currently spark temp tables are NOT materialized, and thus not evaluated until called which is identical to a subquery. 
# MAGIC   
# MAGIC #### Initialization
# MAGIC   
# MAGIC   <li> <b> deltaHelpers = DeltaHelpers(temp_root_path= "dbfs:/delta_temp_db", db_name="delta_temp") </b> - The parameters are defaults and can be changed to a customer db name or s3 path
# MAGIC     
# MAGIC #### There are 4 methods: 
# MAGIC   
# MAGIC   <li> <b> createOrReplaceTempDeltaTable(df: DataFrame, table_name: String) </b> - This creates or replaces materialized delta table in the default location in dbfs or in your provided s3 path
# MAGIC   <li> <b> appendToTempDeltaTable(df: DataFrame, table_name: String) </b> - This appends to an existing delta table or creates a new one if not exists in dbfs or your provided s3 path
# MAGIC   <li> <b> removeTempDeltaTable(table_name) </b> - This removes the delta table from your delta_temp database session
# MAGIC   <li> <b> removeAllTempTablesForSession() </b> - This truncates the initialized temp_db session. It does NOT run a DROP DATABASE command because the database can be global. It only removes the session path it creates. 

# COMMAND ----------

# MAGIC %pip install -r helperfunctions/requirements.txt

# COMMAND ----------

# DBTITLE 1,Import
from helperfunctions.deltahelpers import DeltaHelpers

# COMMAND ----------

# DBTITLE 1,Initialize
## 2 Params [Optional - db_name, temp_root_path]
deltaHelpers = DeltaHelpers()

# COMMAND ----------

# DBTITLE 1,Create or Replace Temp Delta Table
df = spark.read.format("json").load("/databricks-datasets/iot-stream/data-device/")

## Methods return the cached dataframe so you can continue on as needed without reloading source each time AND you can reference in SQL (better for foreachBatch)
## No longer lazy -- this calls an action
df = deltaHelpers.createOrReplaceTempDeltaTable(df, "iot_data")

## Build ML Models

display(df)

# COMMAND ----------

# DBTITLE 1,Read cached table quickly in python or SQL
# MAGIC %sql
# MAGIC -- Read cahced table quickly in python or SQL
# MAGIC SELECT * FROM delta_temp.iot_data

# COMMAND ----------

df.count()

# COMMAND ----------

# DBTITLE 1,Append to Temp Delta Table
## Data is 1,000,000 rows
df_doubled = deltaHelpers.appendToTempDeltaTable(df, "iot_data")

## Be CAREFUL HERE! Since the function calls an action, it is NOT lazily evaluated. So running it multiple times can append the same data
df_doubled.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY delta_temp.iot_data

# COMMAND ----------

# DBTITLE 1,Remove Temp Delta Table
deltaHelpers.removeTempDeltaTable("iot_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM delta_temp.iot_data

# COMMAND ----------

# DBTITLE 1,Truncate Session
## Deletes all tables in session path but does not drop that delta_temp database
deltaHelpers.removeAllTempTablesForSession()
