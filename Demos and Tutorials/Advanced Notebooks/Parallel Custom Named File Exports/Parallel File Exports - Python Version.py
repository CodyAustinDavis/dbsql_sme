# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Python Version
# MAGIC ### Author: Cody Austin Davis
# MAGIC ### Date: 2/22/2023
# MAGIC
# MAGIC This notebook shows users how to rename/move files from one s3 location/file name to another in parallel using spark. 

# COMMAND ----------

# MAGIC %pip install boto

# COMMAND ----------

# DBTITLE 1,Define Function To Dynamically Rename File Paths
@udf("string")
def getRenamedFilePath(source_path):

  source_path_root = "/".join(source_path.split("/")[:-1])
  source_path_file_name = "".join(source_path.split("/")[-1])

  ## insert any arbitrary file renaming logic here
  new_path_file_name = "/renamed/" + source_path_file_name

  new_path = source_path_root + new_path_file_name

  return new_path

# COMMAND ----------

# DBTITLE 1,Create test data set not in dbutils
(spark.read.json("dbfs:/databricks-datasets/iot-stream/data-device/")
.write.format("json").mode("overwrite").save('s3://<bucket_name/parallelfile_source/')
)

# COMMAND ----------

# DBTITLE 1,Define python udf that renames / copies files in parallel with boto or HTTP request
from pyspark.sql.functions import *
import boto3

## This UDF can be adjusted to accept access keys as another parameter

@udf("string")
def mv_s3_object(source_path, target_path):

  ## Get SOURCE bucket name and source path separately for boto
  source_bucket_name = "/".join(source_path.split("/")[0:3]).split("//")[1]
  source_file_path = "/".join(source_path.split("/")[3:])
    
  ## Get TARGET bucket name and source path separately for boto
  target_bucket_name = "/".join(target_path.split("/")[0:3]).split("//")[1]
  target_file_path = "/".join(target_path.split("/")[3:])
  
  ## Prep boto request copy params
  source_dict = {'Bucket': source_bucket_name, 'Key': source_file_path}
  
  ## Try copying the file over, return SUCCESS or error message in pyspark data frame
  s3 = boto3.resource('s3')
  msg = 'NOOP'
  try:
    s3.Object(target_bucket_name, target_file_path).copy_from(CopySource=source_dict)
    ## This delete is optional, you might want to separate this out into another job. This just represents the 2 commands to simulate a "move"
    s3.Object(source_bucket_name, source_file_path).delete()

    msg = 'SUCCESS'

  except Exception as e:
    msg = f'FAIL: {str(e)} \n BUCKET: {source_bucket_name}, SOURCE: {source_file_path}, TARGET: {target_file_path}'
          
  return msg

# COMMAND ----------

# DBTITLE 1,Chose a source path (either dynamically or manually) and move / rename files with the udfs in parallel
input_path_to_move = 's3://oetrta/codyaustindavis/parallelfile_source/'

filesDf = (spark.createDataFrame(dbutils.fs.ls(input_path_to_move))
           .filter(~col("name").startswith("_"))  ## exclude out-of-scope files
           .withColumn("target_path", getRenamedFilePath(col("path"))) ## Python udf to create the new file path with any logic inside function
           .selectExpr("path AS source_path", "target_path") ## select 2 paths needed
           .withColumn("WasMoved", mv_s3_object(col("source_path"), col("target_path"))) ## Push source and target paths to udf to execute in parallel and return msg
          )

display(filesDf)

# COMMAND ----------

# DBTITLE 1,Confirm rename
dbutils.fs.ls("s3://<bucket_name/parallelfile_source/renamed/"
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Scala Version

# COMMAND ----------

# DBTITLE 1,Define scala file renaming function
# MAGIC %scala 
# MAGIC
# MAGIC
# MAGIC def getNewFilePath(sourcePath: String): String = {
# MAGIC   val source_path = sourcePath;
# MAGIC
# MAGIC   val slice_len = source_path.split("/").length - 1;
# MAGIC   val source_path_root = source_path.split("/").slice(0, slice_len);
# MAGIC   val source_path_file_name = source_path.split("/").last;
# MAGIC
# MAGIC   // any arbitrary file rename logic
# MAGIC   val new_path_file_name = "renamed/"+source_path_file_name;
# MAGIC   
# MAGIC   
# MAGIC   val new_path = source_path_root.mkString("/") + "/" + new_path_file_name;
# MAGIC
# MAGIC   return new_path
# MAGIC }

# COMMAND ----------

# DBTITLE 1,Test Scala File Renaming function
# MAGIC %scala 
# MAGIC
# MAGIC val test_new_path = getNewFilePath("dbfs:/databricks-datasets/iot-stream/data-device/part-00003.json.gz")
# MAGIC
# MAGIC println(test_new_path)

# COMMAND ----------

# DBTITLE 1,Broadcast Configs to Executors
# MAGIC %scala 
# MAGIC import org.apache.hadoop.fs
# MAGIC
# MAGIC // maybe we need to register access keys here? not sure yet. Still dealing with Auth issues
# MAGIC val conf = new org.apache.spark.util.SerializableConfiguration(sc.hadoopConfiguration)
# MAGIC
# MAGIC val broadcastConf = sc.broadcast(conf)
# MAGIC
# MAGIC print(conf.value)

# COMMAND ----------

# DBTITLE 1,Run file renaming and moving for each row (need to add AUTH)
# MAGIC %scala 
# MAGIC
# MAGIC import org.apache.hadoop.fs._
# MAGIC
# MAGIC // root bucket of where original files were dropped
# MAGIC val filesToCopy = dbutils.fs.ls("dbfs:/databricks-datasets/iot-stream/data-device/").map(_.path)
# MAGIC
# MAGIC spark.sparkContext.parallelize(filesToCopy).foreachPartition(rows => rows.foreach {
# MAGIC   
# MAGIC   file => 
# MAGIC   
# MAGIC   println(file)
# MAGIC   val fromPath = new Path(file)
# MAGIC   
# MAGIC   val tempNewPath = getNewFilePath(file)
# MAGIC   
# MAGIC   val toPath = new Path(tempNewPath)
# MAGIC   
# MAGIC   val fromFs = toPath.getFileSystem(conf.value)
# MAGIC   
# MAGIC   val toFs = toPath.getFileSystem(conf.value)
# MAGIC   
# MAGIC   FileUtil.copy(fromFs, fromPath, toFs, toPath, false, conf.value)
# MAGIC   
# MAGIC })

# COMMAND ----------

# DBTITLE 1,Look at files to Copy
# MAGIC %scala
# MAGIC
# MAGIC val filesToCopy = dbutils.fs.ls("dbfs:/databricks-datasets/iot-stream/data-device/").map(_.path)
# MAGIC
# MAGIC
# MAGIC val filesDf = spark.sparkContext.parallelize(filesToCopy).toDF()
# MAGIC
# MAGIC display(filesDf)
