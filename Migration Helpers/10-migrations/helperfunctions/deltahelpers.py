import json
import requests
import re
import os
from datetime import datetime, timedelta
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, max
from pyspark.sql.types import *


### Helps Materialize temp tables during ETL pipelines
class DeltaHelpers():

    
    def __init__(self, db_name="delta_temp", temp_root_path="dbfs:/delta_temp_db"):
        
        self.spark = SparkSession.getActiveSession()
        self.db_name = db_name
        self.temp_root_path = temp_root_path

        self.dbutils = None
      
        #if self.spark.conf.get("spark.databricks.service.client.enabled") == "true":
        try:     
            from pyspark.dbutils import DBUtils
            self.dbutils = DBUtils(self.spark)
        
        except:
            
            import IPython
            self.dbutils = IPython.get_ipython().user_ns["dbutils"]

        self.session_id =self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        self.temp_env = self.temp_root_path + self.session_id
        self.spark.sql(f"""DROP DATABASE IF EXISTS {self.db_name} CASCADE;""")
        self.spark.sql(f"""CREATE DATABASE IF NOT EXISTS {self.db_name} LOCATION '{self.temp_env}'; """)
        print(f"Initializing Root Temp Environment: {self.db_name} at {self.temp_env}")
        
        return
    

    def createOrReplaceTempDeltaTable(self, df, table_name):
        
        tblObj = {}
        new_table_id = table_name
        write_path = self.temp_env + new_table_id
        
        self.spark.sql(f"DROP TABLE IF EXISTS {self.db_name}.{new_table_id}")
        self.dbutils.fs.rm(write_path, recurse=True)
        
        df.write.format("delta").mode("overwrite").option("path", write_path).saveAsTable(f"{self.db_name}.{new_table_id}")
        
        persisted_df = self.spark.read.format("delta").load(write_path)
        return persisted_df
 
    def appendToTempDeltaTable(self, df, table_name):
        
        tblObj = {}
        new_table_id = table_name
        write_path = self.temp_env + new_table_id
        
        df.write.format("delta").mode("append").option("path", write_path).saveAsTable(f"{self.db_name}.{new_table_id}")
        
        persisted_df = self.spark.read.format("delta").load(write_path)
        return persisted_df
      
    def removeTempDeltaTable(self, table_name):
        
        table_path = self.temp_env + table_name
        self.dbutils.fs.rm(table_path, recurse=True)
        self.spark.sql(f"""DROP TABLE IF EXISTS {self.db_name}.{table_name}""")
        
        print(f"Temp Table: {table_name} has been deleted.")
        return
    
    def removeAllTempTablesForSession(self):
        
        self.dbutils.fs.rm(self.temp_env, recurse=True)
        ##spark.sql(f"""DROP DATABASE IF EXISTS {self.db_name} CASCADE""") This temp db name COULD be global, never delete without separate method
        print(f"All temp tables in the session have been removed: {self.temp_env}")
        return
        


class SchemaHelpers():
    
    def __init__():
        import json
        return
    
    @staticmethod
    def getDDLString(structObj):
        import json
        ddl = []
        for c in json.loads(structObj.json()).get("fields"):

            name = c.get("name")
            dType = c.get("type")
            ddl.append(f"{name}::{dType} AS {name}")

        final_ddl = ", ".join(ddl)
        return final_ddl
    
    @staticmethod
    def getDDLList(structObj):
        import json
        ddl = []
        for c in json.loads(structObj.json()).get("fields"):

            name = c.get("name")
            dType = c.get("type")
            ddl.append(f"{name}::{dType} AS {name}")

        return ddl
    
    @staticmethod
    def getFlattenedSqlExprFromValueColumn(structObj):
        import json
        ddl = []
        for c in json.loads(structObj.json()).get("fields"):

            name = c.get("name")
            dType = c.get("type")
            ddl.append(f"value:{name}::{dType} AS {name}")

        return ddl
      
      
      
      
class DeltaMergeHelpers():
 
    def __init__(self):
        return
 
    @staticmethod
    def retrySqlStatement(spark, operationName, sqlStatement, maxRetries = 10, maxSecondsBetweenAttempts=60):
 
        import time
        maxRetries = maxRetries
        numRetries = 0
        maxWaitTime = maxSecondsBetweenAttempts
        ### Does not check for existence, ensure that happens before merge
 
        while numRetries <= maxRetries:
 
            try: 
 
                print(f"SQL Statement Attempt for {operationName} #{numRetries + 1}...")
 
                spark.sql(sqlStatement)
 
                print(f"SQL Statement Attempt for {operationName} #{numRetries + 1} Successful!")
                break
 
            except Exception as e:
                error_msg = str(e)
 
                print(f"Failed SQL Statment Attmpet for {operationName} #{numRetries} with error: {error_msg}")
 
                numRetries += 1
                if numRetries > maxRetries:
                    break
 
            waitTime = waitTime = 2**(numRetries-1) ## Wait longer up to max wait time for failed operations
 
            if waitTime > maxWaitTime:
                waitTime = maxWaitTime
 
            print(f"Waiting {waitTime} seconds before next attempt on {operationName}...")
            time.sleep(waitTime)