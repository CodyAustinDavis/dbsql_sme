# Databricks notebook source
# MAGIC %md
# MAGIC ###Delta Lakehouse DBFS Profiler #
# MAGIC
# MAGIC <img src="https://i.imgur.com/NNyw4Md.png" width="10%">
# MAGIC
# MAGIC #### Scope
# MAGIC This notebook analyzes dbfs files.
# MAGIC It scans the dbfs locations to help analyze the dbfs locations with biggest files
# MAGIC
# MAGIC Here are the steps we execute
# MAGIC * This code works only with DBR 12.2 and above databricks runtime clusters
# MAGIC * Setup a dbfs location in Cmd 3
# MAGIC * Gather file stats using dbutils.fs.ls
# MAGIC * Persist the metadata to delta tables in a database called 'data_profile'
# MAGIC * Summarize the findings
# MAGIC
# MAGIC #### File Statistics
# MAGIC These statistics are captured at table level:
# MAGIC - File Sizes in MB
# MAGIC - Avg. File Size
# MAGIC - Directory Growth
# MAGIC - Partition Skew
# MAGIC
# MAGIC #### Instructions
# MAGIC
# MAGIC 1. Spin up a new cluster with DBR 12.2 LTS runtime 
# MAGIC 2. Setup a dbfs location in Cmd 3
# MAGIC 3. Run All commands below Cmd 3
# MAGIC

# COMMAND ----------

import json, requests
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/files/abe/lakehouse/tables

# COMMAND ----------

# SET THIS PARAMETER
# Provide the dbfs file path which you want to analyze here

parentDir = "/mnt/files/abe/lakehouse/tables"


# COMMAND ----------

# Function to walk through the directory structure to gather file stats
def analyzeDir(parentDir):
  fileList = []
  badDirList = []
  dbfsPath = parentDir
  try:
    sourceFiles = dbutils.fs.ls(parentDir)
  except:
    badDirList.append(subDir)

  #Get list of files which have some data in them
  while len(sourceFiles)>0:
    fileDetails = sourceFiles.pop()
    # Ensure it's not a directory by checking size and the last character
    if fileDetails.size>0 or fileDetails.name[-1:]!='/':
      fileDetailsList = list(fileDetails)
      fileDetailsList.append(dbfsPath)
      fileList.append(fileDetailsList)
    else:
      try:
        #print("processing subdir :"+fileDetails.path)
        subDirFiles = dbutils.fs.ls(fileDetails.path)
        #print("File count found :"+str(len(subDirFiles)))
        sourceFiles.extend(subDirFiles)
      except:
        badDirList.append(fileDetails.path) 
  return fileList

# COMMAND ----------

# Run the directory crawl
fileList = analyzeDir(parentDir)


# COMMAND ----------

# Create a temp table with the data for analysis

tSchema = StructType([StructField("file_path", StringType())\
                      ,StructField("file_name", StringType())\
                      ,StructField("file_size", LongType())\
                      ,StructField("modified_time", LongType())\
                      ,StructField("parent_dir", StringType())])

if len(fileList)>0:
  filesDF = spark.createDataFrame(fileList,schema=tSchema)
  filesDF = filesDF.withColumn("scan_date",lit(current_timestamp()))
  filesDF.createOrReplaceTempView("files_tmp")
else:
  print("No files found")
  
print("Number of files analyzed: "+str(len(fileList)))

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC scan_date,
# MAGIC split(file_path,'/')[1] fileSeg1,
# MAGIC split(file_path,'/')[2] fileSeg2,
# MAGIC split(file_path,'/')[3] fileSeg3, -- You can remove these if your folder structure is not this deep
# MAGIC split(file_path,'/')[4] fileSeg4,
# MAGIC split(file_path,'/')[5] fileSeg5,
# MAGIC split(file_path,'/')[6] fileSeg6,
# MAGIC round((sum(file_size)/1024)/1024/1024,2) sizeInGB,
# MAGIC round((avg(file_size)/1024)/1024,2) avgSizeInMB 
# MAGIC from files_tmp
# MAGIC group by 1,2,3,4,5,6,7 order by 8 desc

# COMMAND ----------

# %sql
# -- Persist the Data to a Delta Table
# -- CHANGE THE TABLE NAME TO SUIT YOUR NEEDS
# DROP TABLE IF EXISTS dbr_files;

# CREATE TABLE IF NOT EXISTS dbr_files
# USING delta
# AS 
# SELECT date_trunc('day',scan_date) as scan_date,parent_dir,file_path,file_name,from_unixtime(modified_time/1000, 'yyyy-MM-dd HH:mm:ss') AS modified_timestamp,file_size FROM files_tmp;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert/Update New and Changed Data.
# MAGIC MERGE INTO dbr_files AS tgt
# MAGIC USING (SELECT date_trunc('day',scan_date) as scan_date,parent_dir,file_path,file_name,from_unixtime(modified_time/1000, 'yyyy-MM-dd HH:mm:ss') AS modified_timestamp,file_size FROM files_tmp) AS src
# MAGIC ON src.scan_date = tgt.scan_date AND src.file_path = tgt.file_path AND src.file_name = tgt.file_name
# MAGIC   WHEN MATCHED THEN UPDATE SET *
# MAGIC   WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# DBTITLE 1,File Sizes
# MAGIC %sql
# MAGIC select 
# MAGIC scan_date,
# MAGIC split(file_path,'/')[1] fileSeg1,
# MAGIC split(file_path,'/')[2] fileSeg2,
# MAGIC split(file_path,'/')[3] fileSeg3, -- You can remove these if your folder structure is not this deep
# MAGIC round((sum(file_size)/1024)/1024/1024,2) sizeInGB,
# MAGIC round((avg(file_size)/1024)/1024,2) avgSizeInMB 
# MAGIC from default.dbr_files
# MAGIC group by 1,2,3,4 order by 5 desc

# COMMAND ----------

# DBTITLE 1,Weekly Growth
# MAGIC %sql
# MAGIC select split(curr.file_path,'/')[2] fileSeg2,split(curr.file_path,'/')[3] fileSeg3, curr.file_path,round((curr.totalSize-nvl(lastwk.totalSize,0))/curr.totalSize,4)*100 growthRatePct from 
# MAGIC (select file_path,sum(file_size) totalSize from default.dbr_files where scan_date = current_date() group by file_path) curr
# MAGIC left outer join
# MAGIC (select file_path,sum(file_size) totalSize from default.dbr_files where scan_date = current_date()-7 group by file_path) lastwk
# MAGIC on curr.file_path = lastwk.file_path
# MAGIC order by 2 desc
