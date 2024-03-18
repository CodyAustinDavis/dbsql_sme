// Databricks notebook source
// DBTITLE 1,Get parameters from job
// name of the user, formatted to be passable as a schema
val userName = dbutils.widgets.get("current_user_name")

// The scaleFactor defines the size of the dataset to generate (in GB)
val scaleFactor = dbutils.widgets.get("scale_factor")

// Location to store the queries
val queryDir = dbutils.widgets.get("query_directory")

// Location to store the data
val dataDir = dbutils.widgets.get("data_directory")

// Name of the database to write the tpcds data
val catalogName = dbutils.widgets.get("catalog_name")

// Name of the database to write the tpcds data
val schemaName = dbutils.widgets.get("schema_name")

// Determine if tables with the same parameters have already been written
val tablesAlreadyExist = dbutils.widgets.get("tables_already_exist")

// COMMAND ----------

// DBTITLE 1,Write Data
if (tablesAlreadyExist == "false") {
  // source: https://github.com/deepaksekaranz/TPCDSDataGen/tree/master/TPCDS-Kit
  import com.databricks.spark.sql.perf.tpcds.TPCDSTables

  // The scaleFactor defines the size of the dataset to generate (in GB)
  val scaleFactorInt = scaleFactor.toInt

  // Set the file type
  val fileFormat = "delta"

  // Initialize TPCDS tables with given parameters
  val tables = new TPCDSTables(
    sqlContext = sqlContext,
    dsdgenDir = "/usr/local/bin/tpcds-kit/tools",
    scaleFactor = scaleFactor,
    useDoubleForDecimal = false, // If true, replaces DecimalType with DoubleType
    useStringForDate = false // If true, replaces DateType with StringType
  )

  // Generate TPC-DS data
  tables.genData(
    location = dataDir,
    format = "delta",
    overwrite = true, // overwrite the data that is already there
    partitionTables = false, // create the partitioned fact tables 
    clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files. 
    filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
    tableFilter = "", // "" means generate all tables
    numPartitions = 20 // how many dsdgen partitions to run - number of input tasks.
  ) 

  // Create the specified database if it doesn't exist
  sql(s"create schema if not exists $schemaName")

  // Create metastore tables in a specified database for your data. The current database will be switched to the specified database.
  // Once tables are created, the current database will be switched to the specified database.
  tables.createExternalTables(dataDir, fileFormat, schemaName, overwrite = true, discoverPartitions = false)

  // Convert the tables to managed
  val tableInfo = dbutils.fs.ls(dataDir).map(x => (x.name.stripSuffix("/"), x.path))

  for ((tableName, tablePath) <- tableInfo) {
    spark.sql(s"DROP TABLE IF EXISTS ${catalogName}.${schemaName}.${tableName}")
    spark.sql(s"""
      CREATE TABLE ${catalogName}.${schemaName}.${tableName}
      LOCATION '$tablePath'
    """)
  }
}

// COMMAND ----------

// DBTITLE 1,Write Queries
import scala.util.Try
import com.databricks.spark.sql.perf.tpcds.TPCDS
import com.databricks.spark.sql.perf.Query

def writeQueriesToDBFS(dbfsPath: String, queries: Map[String, Query]): Unit = {
  queries.foreach { case (fileName, query) =>
    val dbfsFilePath = s"$dbfsPath/$fileName.sql"
    val putResult = Try(dbutils.fs.put(dbfsFilePath, query.sqlText.getOrElse(""), overwrite = true))

    putResult match {
      case scala.util.Success(_) => println(s"Successfully written to $dbfsFilePath")
      case scala.util.Failure(exception) => println(s"Failed to write to $dbfsFilePath: ${exception.getMessage}")
    }
  }
}

val tpcds = new TPCDS (sqlContext = sqlContext)
val sqlQueries = tpcds.tpcds2_4QueriesMap

writeQueriesToDBFS(queryDir, sqlQueries)
