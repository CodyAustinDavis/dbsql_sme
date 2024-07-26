# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Delta Helpers for Temp Tables
# MAGIC This notebooks shows users how to persist data frames to temp tables that can be returned as "cached" dataframes as well as referenced in SQL queries. This is a way to eagerly evaluate spark temp tables (createOrReplaceTempView) instead of the default lazy evalution. This is helpful for many data warehousing, multi-step processes 
# MAGIC
# MAGIC ### Temp DB
# MAGIC This temp table manager can create catalogs and databases in which temp tables can live. It utilizes Unity catalog data asset tags to tag the user, code path, and session id that creates the tables to govern access so long as the users are using this client to manage their temp tables. 
# MAGIC
# MAGIC It is a recommended best practice to create separate catalog/databases for the temp database and use only this client to access them. 

# COMMAND ----------

from helperfunctions.delta_helpers_uc import DeltaHelpers

# COMMAND ----------

input_df = spark.read.table("samples.tpch.customer").limit(100)

# COMMAND ----------

# DBTITLE 1,Define Delta Helpers Instance with Same Issue - gotta do it inside a foreachBatch as well
catalog = "dev"
notebook_name = "test_delta"
db_name_temp = f"{notebook_name}_temp_uc"

#### There are 3 different temp table privacy scopes:
"""
SESSION: Session isolate the temp table to the current user and current session. It is the default and strongest level of isolation. This means that no other entity can read/write to the created temp table from the original session except for the same session. (Only if using the client)

CODE_PATH_USER: This means that a process can read or alter a temp table created by another session AS LONG AS the code path is the same (usign the notebook entry point via dbutils). The same user in a DIFFERENT notebook cannot access other notebooks temp tables. 


USER: This is the least strict level of isolation. A user can read/alter temp tables created by themselves, regardless of which code path is calling the table. 

"""
deltaHelpers = DeltaHelpers(catalog=catalog, db_name=db_name_temp, scope="CODE_PATH_USER")

# COMMAND ----------

print(deltaHelpers.getAllSessionInformation())

# COMMAND ----------

## Drops the temp dB only if it has the right scope access
deltaHelpers.dropTempDb(force=True)

# COMMAND ----------

## This is the command that can 'cache' a data frame, reguster it in Unity Catalog to be used in SQL commands, as well as returned the eagerly evaluated cached data frame. 

## Replace spark.createOrReplaceTempView() with this command to eagerly evaluate temp views cached to delta. 

result_df = deltaHelpers.saveToDeltaTempTable(input_df, "input_df", mode="overwrite", auto_return_results_df=True)

# COMMAND ----------

print(deltaHelpers.getSessionTableInfo("input_df"))

# COMMAND ----------

df = deltaHelpers.getSessionTableDf("input_df")

display(df)

# COMMAND ----------

deltaHelpers.removeAllTempTablesForSession()

# COMMAND ----------

display(result_df)
