from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, date_trunc

class RedshiftChecker():
    

    """
    
    This class reads a table with optional filters from a Redshift environment, validates schema, rows counts, data types, and returns a diff
    Dependencies: 
    1. Assumes Redshift Connector is installed on the running cluster
    2. Assumes cluster has IAM Instance profile access to the requested Databricks tables
    
    ## TO DO: 
    1. Add Data Type Comparisons
    2. Add Row-level comparisons
    """

    def __init__(self, connectionString, iamRole, tempDir):

        print(f"Initialized Redshift Data Checker")
        self.spark = SparkSession.getActiveSession()
        self.connectionString = connectionString
        self.iamRole = iamRole
        self.tempDir = tempDir
    
    #### Build a Query and return the result  

    def getSpark(self):
        return self.spark

    def getQuery(self, tableName, dateFilterColumn=None, startDateTime=None, endDateTime=None, limit=None):
        
        tableName = tableName

        sqlQuery = f"""SELECT * FROM {tableName}"""
        dateFilterColumn = dateFilterColumn
        startDateTime = startDateTime
        endDateTime = endDateTime

        try: 
            
            if dateFilterColumn is not None:

                if (endDateTime is not None) and (startDateTime is not None):
                    sqlFilter = f""" WHERE {dateFilterColumn} BETWEEN 
                                    (CASE WHEN '{startDateTime}' = 'None' THEN now() ELSE '{startDateTime}'::timestamp END) 
                                    AND 
                                    (CASE WHEN '{endDateTime}' = 'None' THEN now() ELSE '{endDateTime}'::timestamp END)"""
                    filteredQuery = sqlQuery + sqlFilter

                elif startDateTime is not None:
                    sqlFilter = f""" WHERE {dateFilterColumn} BETWEEN 
                                    (CASE WHEN '{startDateTime}' = 'None' THEN now() ELSE '{startDateTime}'::timestamp END) 
                                    AND 
                                    now()"""
                    filteredQuery = sqlQuery + sqlFilter
                    
                else:
                    filteredQuery = sqlQuery

            else: 
                filteredQuery = sqlQuery
            
            ## filteredQuery
            ## Limit query if supplied
            if isinstance(limit, int):
                limitStr = f""" LIMIT {limit}"""
                finalQuery = filteredQuery + limitStr
                
            elif limit is None: 
                finalQuery = filteredQuery
            else: 
                finalQuery = filteredQuery
                print("No valid limit provided... not limiting table...")
                
        except Exception as e:
            print(f"ERROR: Please provide a valid date filter or limit: {str(e)}")
            
        return finalQuery
    
    #### Get Redshift Table from a query
    def getRedshiftQueryResult(self, query):
        
        rsh_query = query
        redshift_df = ( self.spark.read
           .format("com.databricks.spark.redshift")
           .option("url", self.connectionString)
           .option("query", rsh_query)
           .option("tempdir", self.tempDir)
           .option("aws_iam_role", self.iamRole)
           .load()
                      )
        
        return redshift_df   
    
    #### Get Databricks Table from a query
    def getDatabricksQueryResults(self, query):
        
        dbx_query = query
        databricks_df =  self.spark.sql(dbx_query)
        
        return databricks_df
    
    #### Get Databricks Table
    def getDatabricksTable(self, tableName, dateFilterColumn=None, startDateTime=None, endDateTime=None, limit=None):
        
        finalQuery = self.getQuery(tableName, dateFilterColumn, startDateTime, endDateTime, limit)
        databricks_df = self.getDatabricksQueryResults(finalQuery)
        return databricks_df
    
    #### Get Redshift Table
    def getRedshiftTable(self, tableName, dateFilterColumn=None, startDateTime=None, endDateTime=None, limit=None):
        
        finalQuery = self.getQuery(tableName, dateFilterColumn, startDateTime, endDateTime, limit)
        redshift_df = self.getRedshiftQueryResult(finalQuery)
        return redshift_df      
        
        
    def compareColumnsOfTable(self, redshiftTableName, databricksTableName):
    
        redshift_table = self.getRedshiftTable(redshiftTableName).columns
        dbx_table = self.getDatabricksTable(databricksTableName).columns
        
        int_cols = ','.join(list(set(redshift_table).intersection(set(dbx_table))))
        in_dbx_not_redshift = ','.join([i for i in dbx_table if i not in int_cols])
        in_redshift_not_dbx = ','.join([i for i in redshift_table if i not in int_cols])

        cols_schema = ['in_both', 'in_redshift_not_databricks', 'in_databricks_not_redshift']
        data = [[int_cols, in_redshift_not_dbx, in_dbx_not_redshift]]

        cols_comp_df =  self.spark.createDataFrame(data, cols_schema)

        return cols_comp_df
        
        
    def compareRowCountOfTable(self, redsfhitTableName, databricksTableName, dateFilterColumn=None, startDateTime=None, endDateTime = None, limit=None, groupByAgg='all'):
        
        from pyspark.sql.functions import date_trunc
        ## Group by agg options 
        #None -- All Rows will be counted and compared
        #all -- same as None, all rows will be counted
        #day -- All rows within the range will be counted and grouped by day
        #hour -- All rows within the range will be counted and grouped by hour
        #minute -- All rows within the range will be counted and grouped by minute
        
        ## If dateFilter column is None, just count whole table
        redshift_table = self.getRedshiftTable(redsfhitTableName, dateFilterColumn, startDateTime, endDateTime, limit)
        dbx_table = self.getDatabricksTable(databricksTableName, dateFilterColumn, startDateTime, endDateTime, limit)
        
        if (groupByAgg.lower() == 'all') or (groupByAgg is None) or (dateFilterColumn is None):
    
            red_times = (redshift_table
                         .agg(count("*").alias("RedshiftRowCount"))
                         .withColumn("condition", lit("Full Table Row Counts"))
                        )

            dbx_times = (dbx_table
                         .agg(count("*").alias("DatabricksRowCount"))
                         .withColumn("condition", lit("Full Table Row Counts"))
                        )

            final_df = red_times.join(dbx_times, on="condition", how="full_outer")
            return final_df

        elif groupByAgg.lower() in ['day', 'hour', 'minute', 'month', 'year']:

            red_times = (redshift_table
                         .withColumn("date_col", date_trunc(groupByAgg, col(dateFilterColumn)))
                         .groupBy("date_col")
                         .agg(count(dateFilterColumn).alias("RedshiftRowCount"))
                         .orderBy("date_col")
                        )

            dbx_times = (dbx_table
                         .withColumn("date_col", date_trunc(groupByAgg, col(dateFilterColumn)))
                         .groupBy("date_col")
                         .agg(count(dateFilterColumn).alias("DatabricksRowCount"))
                         .orderBy("date_col")
                        )


            final_df = red_times.join(dbx_times, on="date_col", how="full_outer")
            return final_df

        else: 
            print("ERROR: please provide valid grouping, or dont provide one at all :)")
            return