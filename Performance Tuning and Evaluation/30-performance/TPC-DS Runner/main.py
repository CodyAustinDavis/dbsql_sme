# Databricks notebook source
# DBTITLE 1,Import Constants
# MAGIC %run ./constants

# COMMAND ----------

# DBTITLE 1,Add Widgets to Notebook
create_widgets(dbutils)

# COMMAND ----------

# DBTITLE 1,Pull Variables from Notebook Widgets
constants = Constants(
  **get_widget_values(dbutils)
)

# COMMAND ----------

# DBTITLE 1,Create and Run TPC-DS Benchmark
from utils.run import run

run(spark, dbutils, constants)
