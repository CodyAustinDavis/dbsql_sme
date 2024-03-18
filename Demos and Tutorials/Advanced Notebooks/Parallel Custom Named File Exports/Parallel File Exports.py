# Databricks notebook source
# DBTITLE 1,helper function to dynamically build target path for each file
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
# MAGIC   val new_path = source_path_root.mkString("/") + "/" + new_path_file_name;
# MAGIC
# MAGIC   return new_path
# MAGIC }

# COMMAND ----------

# DBTITLE 1,Test New Function to dynamically build target path for each row (file)
# MAGIC %scala 
# MAGIC
# MAGIC val test_new_path = getNewFilePath("dbfs:/databricks-datasets/iot-stream/data-device/part-00003.json.gz")
# MAGIC
# MAGIC println(test_new_path)

# COMMAND ----------

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

# MAGIC %scala
# MAGIC
# MAGIC val filesToCopy = dbutils.fs.ls("dbfs:/databricks-datasets/iot-stream/data-device/").map(_.path)
# MAGIC
# MAGIC
# MAGIC val filesDf = spark.sparkContext.parallelize(filesToCopy).toDF()
# MAGIC
# MAGIC display(filesDf)
