# Databricks notebook source
# MAGIC %md
# MAGIC ###Hive-Metastore to Unity Catalog Migration Accelerator #
# MAGIC
# MAGIC <img src="https://i.imgur.com/NNyw4Md.png" width="10%">
# MAGIC
# MAGIC #### Scope
# MAGIC This notebook helps you create scripts to migrate HMS tables from a list of database to Unity Catalog Catalog defined in the widget at the top
# MAGIC
# MAGIC Here are the steps we execute
# MAGIC * This code works only with DBR 12.2 and above databricks runtime clusters
# MAGIC * Read the catalog name entered by user at the top of the notebook
# MAGIC * Setup a list of schema names in Cmd 3
# MAGIC * Gather table stats in using "describe detail" on each table in the database
# MAGIC * Persist the metadata to delta tables in a database called 'data_profile'
# MAGIC * Gather mount point locations into a table
# MAGIC * Create scripts to migrate tables
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# Setup Notebook Parameters
#dbutils.widgets.removeAll()
#dbutils.widgets.text("targetCatalogName", "main","Target Catalog Name") # By default this is setup for non unity catalog workspaces.

# COMMAND ----------

# Get the database name from the notebook widget at the top
targetCatalogName = getArgument("targetCatalogName")
hmsCatalogName = "spark_catalog"

##### CHANGE THIS LIST TO HAVE THE DATABASES YOU NEED TO SCAN #######
schemaList= ["abe_demo"]

print("This profiler will analyze tables in databases: {}".format(schemaList))

# COMMAND ----------

import json, requests
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Get Table Details

# COMMAND ----------

# Function to go through each table in the database and gather the statistics
tbl_list = []
schemaLocations = []
# Limit analysis to one database
catalogSelected = spark.sql("USE CATALOG "+hmsCatalogName)
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
    tblDetails=tblDetails.withColumn("catalogName",lit(hmsCatalogName)).withColumn("schemaName",lit(schemaName)).withColumn("rowCount",lit(rowCount))
    tbl_details.extend(tblDetails.rdd.collect())

    # Describe each table to get column details
    tblColDetails = spark.sql("describe "+schemaName+"."+tableName )
    tblColDetails = tblColDetails.withColumn("catalogName",lit(hmsCatalogName)).withColumn("schemaName",lit(schemaName)).withColumn("tableName",lit(tableName))
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
# MAGIC create or replace temporary view tables_summary as
# MAGIC select catalogName,schemaName,split_part(name,'.',3) as tableName,location,
# MAGIC case when location like 'dbfs:/mnt/%' then 'External Mount' 
# MAGIC      when location like 'dbfs:/%' then 'Managed' 
# MAGIC      else 'External Non-mount' end as table_type
# MAGIC from tables_tmp

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Get Mount points Details

# COMMAND ----------

mountPoints = dbutils.fs.mounts()
mountPointsDF = spark.createDataFrame(mountPoints)
mountPointsDF.createOrReplaceTempView("mount_points_tmp")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Managed Tables Migration Script (CTAS)

# COMMAND ----------

# MAGIC %sql
# MAGIC select concat('CREATE TABLE $targetCatalogName.',schemaName,'.',tablename,' AS SELECT * FROM hive_metastore.',schemaName,'.',tablename,';') as create_script 
# MAGIC from tables_summary where table_type = 'Managed' 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. External Mounted Tables Migration Script (LOCATION Specification)

# COMMAND ----------

ext_mnt_lst = []
df = spark.sql("select * from tables_summary where table_type = 'External Mount'")
for row in df.rdd.collect():
  table_loc = row.location.replace("dbfs:", "" )
  for mount in mountPoints:
    if mount.mountPoint+'/' in table_loc:
      new_loc = table_loc.replace(mount.mountPoint+'/',mount.source+'/')
      script = 'CREATE TABLE '+targetCatalogName+'.'+row.schemaName+'.'+row.tableName+' LOCATION \''+new_loc+'\';'
      ext_mnt_lst.append([row.catalogName,row.schemaName,row.tableName,table_loc,new_loc,script])
      

# COMMAND ----------

tSchema = StructType([StructField("catalogName", StringType())\
                      ,StructField("schemaName", StringType())\
                      ,StructField("tableName", StringType())\
                      ,StructField("mnt_location", StringType())\
                      ,StructField("ext_location", StringType())\
                      ,StructField("script", StringType())])


if len(tbl_details)>0:
  mnttblDF = spark.createDataFrame(ext_mnt_lst,schema=tSchema)
else :
  mnttblDF = spark.createDataFrame(spark.sparkContext.emptyRDD(), tSchema)

# Create a temporary table for analysis purposes
mnttblDF.createOrReplaceTempView("mounted_tables_tmp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select script from mounted_tables_tmp 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. External Tables Migration Script (LIKE...COPY LOCATION)

# COMMAND ----------

# MAGIC %sql
# MAGIC select concat('CREATE TABLE $targetCatalogName.',schemaName,'.',tablename,' LIKE hive_metastore.',schemaName,'.',tablename,' COPY LOCATION;') as create_script 
# MAGIC from tables_summary where table_type = 'External Non-mount'
