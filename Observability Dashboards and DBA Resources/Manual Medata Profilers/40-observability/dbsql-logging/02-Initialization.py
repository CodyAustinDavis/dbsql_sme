# Databricks notebook source
# MAGIC %run ./00-Config

# COMMAND ----------

spark.sql(f'CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}') 
# optional: add location 
# spark.sql(f'CREATE DATABASE IF NOT EXISTS {DATABASE_NAME} LOCATION {DATABASE_LOCATION}') 
