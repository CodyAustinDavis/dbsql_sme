# Databricks notebook source
# MAGIC %md
# MAGIC ###Delta Lakehouse Data Profiler #
# MAGIC
# MAGIC <img src="https://i.imgur.com/NNyw4Md.png" width="10%">
# MAGIC
# MAGIC #### Scope
# MAGIC This notebook analyzes Delta lake tables stored in a List of Schema/Databases.
# MAGIC It scans the tables and gathers important statistics to help you find tables that meet certain criteria 
# MAGIC
# MAGIC Here are the steps we execute
# MAGIC * This code works only with DBR 12.2 and above databricks runtime clusters
# MAGIC * Read the catalog name entered by user at the top of the notebook
# MAGIC * Setup a list of schema names in Cmd 3
# MAGIC * Gather table stats in using "describe detail" on each table in the database
# MAGIC * Persist the metadata to delta tables in a database called 'data_profile'
# MAGIC * Summarize the findings
# MAGIC
# MAGIC #### Table Statistics
# MAGIC These statistics are captured at table level:
# MAGIC - Table Size in GB
# MAGIC - Avg. File Size
# MAGIC - Partition Columns
# MAGIC
# MAGIC #### Disaster Recovery (DR) Helpers
# MAGIC * These queries capture the location of Unity Catalog (UC) Managed Table Locations
# MAGIC * You can generate SQL automatically to create new External Tables on top of Managed Table locations in the event of creating a new UC Metastore from scratch.
# MAGIC
# MAGIC
# MAGIC #### Instructions
# MAGIC
# MAGIC 1. Spin up a new cluster with DBR 12.2 LTS runtime 
# MAGIC 2. Run the Cmd 2 first, this will create the text boxes at the top of the notebook
# MAGIC 3. Enter the catalog name and schema/database name in the text box on the top. If the workspace is NOT Unity Catalog enabled then set the catalog name to spark_catalog
# MAGIC 4. Run All commands below Cmd 3
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# Setup Notebook Parameters
#dbutils.widgets.removeAll()
#dbutils.widgets.text("catalogName", "spark_catalog","Catalog Name") # By default this is setup for non unity catalog workspaces.


# COMMAND ----------

# Get the database name from the notebook widget at the top
catalogName = getArgument("catalogName")

##### CHANGE THIS LIST TO HAVE THE DATABASES YOU NEED TO SCAN #######
schemaList= ["abe_demo","abe_dbsql"]

print("This profiler will analyze tables in Catalog: {} and database: {}".format(catalogName,schemaList))

# COMMAND ----------

import json, requests
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Get Table Statistics

# COMMAND ----------

# Function to go through each table in the database and gather the statistics
tbl_list = []
schemaLocations = []
# Limit analysis to one database
catalogSelected = spark.sql("USE CATALOG "+catalogName)
for schemaName in schemaList:
  schemaSelected = spark.sql("USE "+schemaName)
  # Get schema location
  schm = spark.sql("describe database "+schemaName)
  locDF = schm.select("database_description_value").filter("database_description_item='Location'")
  schemaLocations.extend(locDF.rdd.collect()[0])
  
  # Get table list
  tbls = spark.sql("show tables")
  # Get a list of all the tables
  tbls = tbls.select("database","tableName").filter("database <>''")
  tbl_list.extend(tbls.rdd.collect())
print("Total Table count: "+str(len(tbl_list)))

# COMMAND ----------

tbl_details = []
tbl_col_details = []
for tableRow in tbl_list:
  try:
    tableName = tableRow.tableName
    schemaName = tableRow.database
    print("Processing table: "+schemaName+"."+tableName)
    tblDetails = spark.sql("describe detail "+schemaName+"."+tableName )
    rowCount = spark.sql("select count(*) from "+schemaName+"."+tableName ).rdd.collect()[0][0]
    tblDetails=tblDetails.withColumn("catalogName",lit(catalogName)).withColumn("schemaName",lit(schemaName)).withColumn("rowCount",lit(rowCount))
    tbl_details.extend(tblDetails.rdd.collect())

    # Describe each table to get column details
    tblColDetails = spark.sql("describe "+schemaName+"."+tableName )
    tblColDetails = tblColDetails.withColumn("catalogName",lit(catalogName)).withColumn("schemaName",lit(schemaName)).withColumn("tableName",lit(tableName))
    tbl_col_details.extend(tblColDetails.filter("col_name NOT LIKE '#%'").rdd.collect())
  except Exception as e:
    pass                   

# COMMAND ----------

tSchema = StructType([StructField("format", StringType())\
                      ,StructField("id", StringType())\
                      ,StructField("name", StringType())\
                      ,StructField("description", StringType())\
                      ,StructField("location", StringType())\
                      ,StructField("createdAt", DateType())\
                      ,StructField("lastModified", DateType())\
                      ,StructField("partitionColumns", StringType())\
                      ,StructField("numFiles", IntegerType())\
                      ,StructField("sizeInBytes", LongType())\
                      ,StructField("properties", StringType())\
                      ,StructField("minReaderVersion", StringType())\
                      ,StructField("minWriterVersion", StringType())\
                      ,StructField("tableFeatures", StringType())\
                      ,StructField("statistics", StringType())\
                      ,StructField("catalogName", StringType())\
                      ,StructField("schemaName", StringType())\
                      ,StructField("rowCount", StringType())])
if len(tbl_details)>0:
  tbldetDF = spark.createDataFrame(tbl_details,schema=tSchema)
else :
  tbldetDF = spark.createDataFrame(spark.sparkContext.emptyRDD(), tSchema)

# Create a temporary table for analysis purposes
tbldetDF.createOrReplaceTempView("tables_tmp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tables_tmp

# COMMAND ----------

tcSchema = StructType([StructField("colName", StringType())\
                      ,StructField("dataType", StringType())\
                      ,StructField("comment", StringType())\
                      ,StructField("catalogName", StringType())\
                      ,StructField("schemaName", StringType())\
                      ,StructField("tableName", StringType())])

if len(tbl_col_details)>0:
  tblcolDF = spark.createDataFrame(tbl_col_details,schema=tcSchema)
else :
  tblcolDF = spark.createDataFrame(spark.sparkContext.emptyRDD(), tSchema)

# Create a temporary table for analysis purposes
tblcolDF.createOrReplaceTempView("table_cols_tmp")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Create Temporary Stats Table

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view table_stats
# MAGIC as
# MAGIC select 
# MAGIC catalogName,
# MAGIC schemaName,
# MAGIC format,
# MAGIC split(name, '[.]')[2] as tableName,
# MAGIC location,
# MAGIC sizeInBytes,
# MAGIC numFiles,
# MAGIC cast(rowCount as integer) as rowCount,
# MAGIC round(sizeInBytes/1024/1024/1024,3) as sizeInGB,
# MAGIC round(sizeInBytes/1024/1024/numFiles,2) avgFileSizeInMB,
# MAGIC partitionColumns,
# MAGIC case when length(replace(partitionColumns,"[]",""))>0 then 1 else 0 end as partitionFlag 
# MAGIC from tables_tmp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_stats

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_cols_tmp

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. (OPTIONAL) Persist the data to a delta table

# COMMAND ----------

# %sql
# CREATE SCHEMA IF NOT EXISTS data_profile;

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS data_profile.table_profiles;

# CREATE TABLE IF NOT EXISTS data_profile.table_profiles AS
# SELECT DISTINCT * FROM table_stats;


# COMMAND ----------

# %sql
# MERGE INTO data_profile.table_profiles t
# USING (SELECT DISTINCT * FROM table_stats) s
# on t.catalogName = s.catalogName and t.schemaName = s.schemaName and t.tableName = s.tableName
# WHEN MATCHED THEN UPDATE SET *
# WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. (OPTIONAL) Create SQL for migrating UC Managed Tables to a new Metastore as External Tables

# COMMAND ----------

# This portion of the code is relevant if you intend on moving your UC tables from one metastore to another in a disaster recovery situation
# Specify the catalog and schema name in the new Metastore
newCatalogName = 'new_cat'
newSchemaName = 'new_db'
tableSQL = spark.sql("select concat('CREATE TABLE ','"+newCatalogName+"."+newSchemaName+".',tableName,' LOCATION \\'',location,'\\';') as sql_query from table_stats")
#Optionally save the sql to a file
#pdf = tableSQL.toPandas()
#pdf.to_csv("/dbfs/...../table_sql.csv",index=False)
display(tableSQL)

# COMMAND ----------



# COMMAND ----------

dbutils.fs.mounts()     

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select 
# MAGIC -- regexp_extract(location, '^.*/(?=[^/]*$)',0) db_location,
# MAGIC -- count(*) table_count
# MAGIC -- from table_stats
# MAGIC -- group by 1
# MAGIC --select replace(location,'dbfs:/mnt/files/','abfss://files@oneenvadls.dfs.core.windows.net/') as abfss_location from table_stats

# COMMAND ----------

schemaLocations
