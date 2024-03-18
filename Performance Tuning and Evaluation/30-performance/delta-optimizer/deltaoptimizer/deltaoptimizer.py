import json
import sqlparse
from sql_metadata import Parser
import requests
import re
import os
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SparkSession


##################################################################
""" 
Name: Delta Optimizer
Author: Cody Austin Davis
Date: 9/20/2022

Overview: This file provides 4 Main Classes: 

1. DeltaOptimizerBase - This class is just for initializing a delta optimizer database independently and performing maintanence commands on the DB
2. Query Profiler - This class initializes a delta optimizer DB and builds a query profile on one or many DBSQL Warehouses. It can be incremental.
3. Delta Profiler - This class monitors the Delta logs of one or many Databases to profile table metadata such as: file size, table size, operation predicates, etc.
4. Delta Optimizer - This class combines the results (if any, you do not NEED to run both), into a cohesive strategy for monitored tables and saves into a config file. 
"""
##################################################################

class DeltaOptimizerBase():
    
    def __init__(self, database_name="hive_metastore.delta_optimizer", 
                 database_location=None,
                 catalog_filter_mode='all', 
                 catalog_filter_list=[], 
                 database_filter_mode='all', 
                 database_filter_list = [], 
                 table_filter_mode='all', 
                 table_filter_list=[], 
                 shuffle_partitions=None,
                 query_history_partition_cols:[str] = None):

        ## Assumes running on a spark environment

        self.database_name = database_name
        self.database_location = database_location
        self.spark = SparkSession.getActiveSession()
        self.shuffle_partitions = shuffle_partitions if None else self.spark.sparkContext.defaultParallelism*2
        self.query_history_partition_cols = query_history_partition_cols
        ## set parallelism based on cluster
        
        if shuffle_partitions is None:
          self.spark.conf.set("spark.sql.shuffle.partitions", self.shuffle_partitions)
        
        
        #### Store information across optimizer classes that specify what scope of catalogs, databases, and tables are being monitored in this optimizer instance
        self.catalog_filter_list = catalog_filter_list
        #self.database_filter_list = database_filter_list
        self.table_filter_list = table_filter_list
        
        ## Add double check to make sure its fully qualified
        self.database_filter_list = list(set([x.strip() if (x != '' and len(x.split(".")) >=2) else 'hive_metastore.' + x.strip() for x in database_filter_list]))
        
        ## These are the final outputs from the get_tables_to_monitor() function
        self.clean_catalog_list = []
        self.clean_database_list = []
        self.clean_table_list = []
        
        ## Further refines tables to profile. 
        ## TABLE NAMES IN LIST MUST BE FULLY QUALIFIED (catalog.database.table)
        
        if catalog_filter_mode in ['all', 'include_list', 'exclude_list']:
          self.catalog_filter_mode = catalog_filter_mode
        else:
          
          self.catalog_filter_mode = 'all'
          print(f"WARNING - Catalog Filter Mode not correct ({catalog_filter_mode})... must be: all, include_list, exclude_list. If none of these, it will default to all mode.")
         
        
        if database_filter_mode in ['all', 'include_list', 'exclude_list']:
          self.database_filter_mode = database_filter_mode
        else:
          
          self.database_filter_mode = 'all'
          print(f"WARNING - Database Filter Mode not correct ({database_filter_mode})... must be: all, include_list, exclude_list. If none of these, it will default to all mode.")
         
        
        if table_filter_mode in ['all', 'include_list', 'exclude_list']:
          self.table_filter_mode = table_filter_mode
        else:
          
          self.table_filter_mode = 'all'
          print(f"WARNING - Table Filter Mode not correct ({table_filter_mode})... must be: all, include_list, exclude_list. If none of these, it will default to all mode.")
          
        
        
        print(f"Initializing Delta Optimizer at database: {self.database_name} \n with location: {self.database_location}")
        ### Initialize Tables on instantiation

        ## Create Database
        
        try: 

          if self.database_location is not None:
            self.spark.sql(f"""CREATE DATABASE IF NOT EXISTS {self.database_name} LOCATION '{self.database_location}';""")
          else:
            self.spark.sql(f"""CREATE DATABASE IF NOT EXISTS {self.database_name};""")
          
        except Exception as e:
          print(f"""Creating the database at location: {self.database_name} did not work...\n 
          Please ensure the cluster is a UC Enabled cluster or just use hive_metastore""")
          raise(e)
        
        ## Query Profiler Tables
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.query_history_log 
                           (Id BIGINT GENERATED ALWAYS AS IDENTITY,
                           WarehouseIds ARRAY<STRING>,
                           WorkspaceName STRING,
                           StartTimestamp TIMESTAMP,
                           EndTimestamp TIMESTAMP)
                           USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported',
                         'delta.columnMapping.mode' = 'name', 
                         'delta.enableDeletionVectors' = true)
                           """)

        ## v1.5.5 this can be a very big table so adding capabilities to customerize partitioning
        if self.query_history_partition_cols is not None:
          partition_str = ", ".join([i for i in self.query_history_partition_cols if len(i)> 0])
        else:
          partition_str = "update_date"

        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.raw_query_history_statistics
                        (Id BIGINT GENERATED ALWAYS AS IDENTITY,
                          query_id STRING,
                          query_hash STRING, -- v.1.4.0 - MUST PARSE DISTINCT Queryies cause DBX query id is not by query
                          query_start_time_ms FLOAT ,
                          query_end_time_ms FLOAT,
                          duration FLOAT ,
                          status STRING,
                          statement_type STRING,
                          error_message STRING,
                          executed_as_user_id FLOAT,
                          executed_as_user_name STRING,
                          rows_produced FLOAT,
                          metrics MAP<STRING, FLOAT>,
                          endpoint_id STRING,
                          channel_used STRING,
                          lookup_key STRING,
                          update_timestamp TIMESTAMP,
                          update_date DATE,
                          query_text STRING)
                        USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported',
                         'delta.columnMapping.mode' = 'name', 
                         'delta.enableDeletionVectors' = true)
                        PARTITIONED BY ({partition_str})
              """)

        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.parsed_distinct_queries
                        (
                        Id BIGINT GENERATED ALWAYS AS IDENTITY,
                        query_hash STRING, -- v.1.4.0 took out query_id because its NOT distinct
                        query_text STRING,
                        profiled_columns ARRAY<STRING>
                        )
                        USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported',
                         'delta.columnMapping.mode' = 'name', 
                         'delta.enableDeletionVectors' = true)
                        """)
        
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.read_statistics_scaled_results
                (
                TableName STRING,
                ColumnName STRING,
                isUsedInJoin INTEGER,
                isUsedInFilter INTEGER,
                isUsedInGroup INTEGER,
                NumberOfQueriesUsedInJoin LONG,
                NumberOfQueriesUsedInFilter LONG,
                NumberOfQueriesUsedInGroup LONG,
                QueryReferenceCount LONG,
                RawTotalRuntime DOUBLE,
                AvgQueryDuration DOUBLE,
                TotalColumnOccurrencesForAllQueries LONG,
                AvgColumnOccurrencesInQueryies DOUBLE,
                QueryReferenceCountScaled DOUBLE,
                RawTotalRuntimeScaled DOUBLE,
                AvgQueryDurationScaled DOUBLE,
                TotalColumnOccurrencesForAllQueriesScaled DOUBLE,
                AvgColumnOccurrencesInQueriesScaled DOUBLE
                )
                USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported',
                         'delta.columnMapping.mode' = 'name', 
                         'delta.enableDeletionVectors' = true)
                """)

        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.read_statistics_column_level_summary
                (
                TableName STRING,
                ColumnName STRING,
                isUsedInJoin INTEGER,
                isUsedInFilter INTEGER,
                isUsedInGroup INTEGER,
                NumberOfQueriesUsedInJoin LONG,
                NumberOfQueriesUsedInFilter LONG,
                NumberOfQueriesUsedInGroup LONG,
                QueryReferenceCount LONG,
                RawTotalRuntime DOUBLE,
                AvgQueryDuration DOUBLE,
                TotalColumnOccurrencesForAllQueries LONG,
                AvgColumnOccurrencesInQueryies DOUBLE
                )
                USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported',
                         'delta.columnMapping.mode' = 'name', 
                         'delta.enableDeletionVectors' = true)
                """)

        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.query_summary_statistics (
                query_id STRING, -- !!THIS IS THE QUERY HASH - SELF BUILT UNIQUE QUERY ID
                AverageQueryDuration DOUBLE,
                AverageRowsProduced DOUBLE,
                TotalQueryRuns LONG,
                DurationTimesRuns DOUBLE
                )
                USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported',
                         'delta.columnMapping.mode' = 'name', 
                         'delta.enableDeletionVectors' = true)
                """)

        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.query_column_statistics
                (
                TableName STRING,
                ColumnName STRING,
                query_text STRING,
                query_id STRING,
                query_hash STRING,
                AverageQueryDuration DOUBLE,
                AverageRowsProduced DOUBLE,
                TotalQueryRuns LONG,
                DurationTimesRuns DOUBLE,
                NumberOfColumnOccurrences INTEGER,
                isUsedInJoin INTEGER,
                isUsedInFilter INTEGER,
                isUsedInGroup INTEGER
                )
                USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported',
                         'delta.columnMapping.mode' = 'name', 
                         'delta.enableDeletionVectors' = true)
                """)


        ## Transaction Log Analysis Tables
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.write_statistics_merge_predicate
            (
            TableName STRING,
            ColumnName STRING,
            HasColumnInMergePredicate INTEGER,
            NumberOfVersionsPredicateIsUsed INTEGER,
            AvgMergeRuntimeMs INTEGER,
            UpdateTimestamp TIMESTAMP)
            USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported',
                         'delta.columnMapping.mode' = 'name', 
                         'delta.enableDeletionVectors' = true)
            """)
        
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.all_tables_cardinality_stats
             (TableName STRING,
             ColumnName STRING,
             SampleSize INTEGER,
             TotalCountInSample INTEGER,
             DistinctCountOfColumnInSample INTEGER,
             CardinalityProportion FLOAT,
             CardinalityProportionScaled FLOAT,
             IsUsedInReads INTEGER,
             IsUsedInWrites INTEGER,
             UpdateTimestamp TIMESTAMP)
             USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported',
                         'delta.columnMapping.mode' = 'name', 
                         'delta.enableDeletionVectors' = true)
            """)



        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.all_tables_table_stats
                     (TableName STRING,
                     sizeInBytes FLOAT,
                     sizeInGB FLOAT,
                     partitionColumns ARRAY<STRING>,
                     mappedFileSizeInMb STRING,
                     UpdateTimestamp TIMESTAMP)
                     USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported',
                         'delta.columnMapping.mode' = 'name', 
                         'delta.enableDeletionVectors' = true)
                      """)
        
        ## Optimization Strategy Tables
        
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.final_ranked_cols_by_table
            (
            TableName STRING,
            ColumnName STRING ,
            SampleSize INTEGER,
            TotalCountInSample INTEGER,
            DistinctCountOfColumnInSample INTEGER,
            CardinalityProportion FLOAT,
            CardinalityProportionScaled FLOAT,
            IsUsedInReads INTEGER,
            IsUsedInWrites INTEGER,
            UpdateTimestamp TIMESTAMP,
            IsPartitionCol INTEGER,
            QueryReferenceCountScaled DOUBLE,
            RawTotalRuntimeScaled DOUBLE,
            AvgQueryDurationScaled DOUBLE,
            TotalColumnOccurrencesForAllQueriesScaled DOUBLE,
            AvgColumnOccurrencesInQueriesScaled DOUBLE,
            isUsedInJoin INTEGER,
            isUsedInFilter INTEGER,
            isUsedInGroup INTEGER,
            RawScore DOUBLE,
            ColumnRank INTEGER,
            RankUpdateTimestamp TIMESTAMP
            )
            USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported',
                         'delta.columnMapping.mode' = 'name', 
                         'delta.enableDeletionVectors' = true)
        """)


        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS  {self.database_name}.final_optimize_config
                (TableName STRING NOT NULL,
                ZorderCols ARRAY<STRING>,
                OptimizeCommandString STRING, --OPTIMIZE OPTIONAL < ZORDER BY >
                AlterTableCommandString STRING, --delta.targetFileSize, delta.tuneFileSizesForRewrites
                AnalyzeTableCommandString STRING, -- ANALYZE TABLE COMPUTE STATISTICS
                ColumnOrderingCommandString ARRAY<STRING>,
                UpdateTimestamp TIMESTAMP
                )
                USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported',
                         'delta.columnMapping.mode' = 'name', 
                         'delta.enableDeletionVectors' = true)
        """)
        
        return
    
    def get_catalog_list(self):
      return list(set(self.clean_catalog_list))
    
    def get_database_list(self):
      return list(set(self.clean_database_list))
    
    def get_table_list(self):
      return list(set(self.clean_table_list))
    
    
    ## This cleanly gets all tables to monitor by going through all the set logic of inclusion/exclusion lists
    def get_all_tables_to_monitor(self):
       
      ## This process goes through each filtering level rule by catalog --> database --> table. 
      ## No matter which rule, the lower level in the hieracrhy is ALWAYS a subset of the output of the above level. 
      print("Getting Tables to monitor...")
      try:
        ## initialize the empty table list data frame, we will union to this local version
        tbl_df = self.tbl_df
        
        
        if self.catalog_filter_mode == 'include_list': 
          catalogs_to_run = self.spark.sql("""SHOW CATALOGS""").filter(F.col("catalog").isin(self.catalog_filter_list)).collect()

        elif self.catalog_filter_mode == 'exclude_list':
          catalogs_to_run = self.spark.sql("""SHOW CATALOGS""").filter(~F.col("catalog").isin(self.catalog_filter_list)).collect()

        elif self.catalog_filter_mode == 'all':
          catalogs_to_run = self.spark.sql("""SHOW CATALOGS""").collect()

        self.clean_catalog_list = list(set(self.clean_catalog_list + [i[0] for i in catalogs_to_run]))
        

        for ct in catalogs_to_run:
          ## Initialize Catalog default
          cat = ct.catalog
          #print(cat)

          if len(cat) <= 1:
            cat = 'hive_metastore'

          if cat == "spark_catalog":

            print("This cluster is not UC enabled, assuming hive_metastore only")
            cat = 'hive_metastore'


          ## Perform the database filtering

          if self.database_filter_mode == 'include_list': 
            databases_to_run = self.spark.sql(f"""show databases IN {cat}""").filter((F.col("databaseName").isin(self.database_filter_list)) |  (F.concat(F.lit(cat), F.lit('.'), F.col("databaseName")).isin(self.database_filter_list))).collect()

          elif self.database_filter_mode == 'exclude_list':
            databases_to_run = self.spark.sql(f"""show databases IN {cat}""").filter(~((F.col("databaseName").isin(self.database_filter_list)) |  (F.concat(F.lit(cat), F.lit('.'), F.col("databaseName")).isin(self.database_filter_list)))).collect()

          elif self.database_filter_mode == 'all':
            databases_to_run = self.spark.sql(f"""show databases IN `{cat}`""").collect()

          ## add these databases to run to the class property
          self.clean_database_list = list(set(self.clean_database_list + [str(cat) + '.' + i[0] for i in databases_to_run]))

          ## go through databases to run and add tables to monitor
          for db in databases_to_run:

                    #create a dataframe with list of tables from the database
                    df = self.spark.sql(f"show tables in `{cat}`.`{db.databaseName}`")
                    #union the tables list dataframe with main dataframe 
                    tbl_df = tbl_df.union(df)

                    ## Exclude temp views/tables
                    tbl_df = (tbl_df.filter(F.col("isTemporary") == F.lit('false')))

                    tbl_df.createOrReplaceTempView("all_tables")

                    ## Need to add catalog level, for now, make this handled in the joins in the strategy level
                    df_tables = self.spark.sql(f"""
                    SELECT 
                    concat(COALESCE('{cat}', 'hive_metastore'),'.', database, '.', tableName) AS fully_qualified_table_name
                    FROM all_tables
                    """).collect()

                    ## Check Table List Filters- THIS ENSURE ONLY TABLES IN THE PROVIDED DATABASE LISTS ARE ALLOWED

                    if self.table_filter_mode == 'include_list':
                      new_tables = [i[0] for i in df_tables if i[0] in self.table_filter_list]

                    elif self.table_filter_mode == 'exclude_list':
                      new_tables = [i[0] for i in df_tables if i[0] not in self.table_filter_list]

                    elif self.table_filter_mode == 'all':
                      new_tables = [i[0] for i in df_tables]

                     ## Add new table batch to table list
                    self.clean_table_list = self.clean_table_list + new_tables                                     


      except Exception as e:
        raise(e)

      return
        
        
    ## Clear database and start over
    def truncate_delta_optimizer_results(self):
        
        print(f"Truncating database (and all tables within): {self.database_name}...")
        df = self.spark.sql(f"""SHOW TABLES IN {self.database_name}""").filter(F.col("database") == F.lit(self.database_name)).select("tableName").collect()

        for i in df: 
            table_name = i[0]
            print(f"Deleting Table: {table_name}...")
            self.spark.sql(f"""TRUNCATE TABLE {self.database_name}.{table_name}""")
            
        print(f"Database: {self.database_name} successfully Truncated!")
        return
      
    def drop_delta_optimizer(self):
        
        print(f"Truncating database (and all tables within): {self.database_name}...")
        df = self.spark.sql(f"""SHOW TABLES IN {self.database_name}""").filter(F.col("database") == F.lit(self.database_name)).select("tableName").collect()

        self.spark.sql(f"""DROP DATABASE {self.database_name} CASCADE;""")
        
        return
    
    ## Add functions to truncate or remove tables
    
    def vacuum_optimizer_tables(self):
        
        print(f"vacuuming tables database (and all tables within): {self.database_name}...")
        df = self.spark.sql(f"""SHOW TABLES IN {self.database_name}""").filter(F.col("database") == F.lit(self.database_name)).select("tableName").collect()

        for i in df: 
            table_name = i[0]
            print(f"Vacuuming Table: {table_name}...")
            self.spark.sql(f"""VACUUM {self.database_name}.{table_name};""")
            
        print(f"Database: {self.database_name} successfully cleaned up!")
        return  

    
##################################

######## Query Profiler ##########

class QueryProfiler(DeltaOptimizerBase):
    
    def __init__(self, workspace_url, warehouse_ids, 
                 database_name="hive_metastore.delta_optimizer", 
                 database_location=None,
                 catalogs_to_check_views=["hive_metastore"], 
                 catalog_filter_mode='all', 
                 catalog_filter_list=[], 
                 database_filter_mode='all', 
                 database_filter_list = [], 
                 table_filter_mode='all', 
                 table_filter_list=[], 
                 query_history_partition_cols:[str] = None,
                 scrub_views=True, 
                 shuffle_partitions=None):
        
        ## Assumes running on a spark environment
        ## This is getting the subset of tables to filter for given all the subset logic
        super().__init__(database_name=database_name, 
                         database_location=database_location,
                         catalog_filter_mode = catalog_filter_mode, 
                         catalog_filter_list=catalog_filter_list, 
                         database_filter_mode=database_filter_mode, 
                         database_filter_list = database_filter_list, 
                         table_filter_mode=table_filter_mode, 
                         table_filter_list=table_filter_list, 
                         shuffle_partitions=shuffle_partitions)
        
        self.workspace_url = workspace_url.strip()
        self.warehouse_ids_list = [i.strip() for i in warehouse_ids]
        self.warehouse_ids = ",".join(self.warehouse_ids_list)
        self.catalogs_to_check_list = catalogs_to_check_views
        self.scrub_views = scrub_views ## optional param to opt out of new functionality for v1.2.0
        self.tbl_df = self.spark.sql(f"show tables in {self.database_name} like 'xxx_delta_optimizer'")

        print(f"Initializing Delta Optimizer for: {self.workspace_url}\n Monitoring SQL Warehouses: {self.warehouse_ids} \n Database Location: {self.database_name}")
        ### Initialize Tables on instantiation
  
        return
    
    
    ## Spark SQL Tree Parsing Udfs    
    ## Input Filter Type can be : where, join, group_by
    @staticmethod
    @F.udf("array<string>")
    def getParsedFilteredColumnsinSQL(sqlString):

        ## output ["table_name:column_name,table_name:colunmn:name"]
        final_table_map = []

        try: 
            results = Parser(sqlString)

            final_columns = []

            ## If just a select, then skip this and just return the table
            try:
                final_columns.append(results.columns_dict.get("where"))
                final_columns.append(results.columns_dict.get("join"))
                final_columns.append(results.columns_dict.get("group_by"))

            except:
                for tbl in results.tables:
                    final_table_map.append(f"{tbl}:")

            final_columns_clean = [i for i in final_columns if i is not None]
            flatted_final_cols = list(set([x for xs in final_columns_clean for x in xs]))

            ## Try to map columns to specific tables for simplicity downstream

            """Rules -- this isnt a strict process cause we will filter things later, 
            what needs to happen is we need to get all possible columns on a given table, even if not true

            ## Aliases are already parsed so the column is either fully qualified or fully ambiguous
            ## Assign a column to table if: 
            ## 1. column has an explicit alias to it
            ## 2. column is not aliased
            """

            for tbl in results.tables:
                found_cols = []
                for st in flatted_final_cols:

                    ## Get Column Part
                    try:
                        column_val = st[st.rindex('.')+1:] 
                    except: 
                        column_val = st

                    ## Get Table Part
                    try:
                        table_val = st[:st.rindex('.')] or None
                    except:
                        table_val = None

                    ## Logic that add column if tbl name is found or if there was no specific table name for the column
                    if st.find(tbl) >= 0 or (table_val is None):
                        if column_val is not None and len(column_val) > 1:
                            final_table_map.append(f"{tbl}:{column_val}")

        except Exception as e:
            final_table_map = [str(f"ERROR: {str(e)}")]

        return final_table_map

      
    @staticmethod
    @F.udf("integer")
    def checkIfJoinColumn(sqlString, columnName):
        try: 
            results = Parser(sqlString)

            ## Can be a fully qualified table name or just a column name
            ## If just a select, then skip this and just return the table
            if columnName in results.columns_dict.get("join"):
                return 1
            elif columnName.split(".")[-1] in results.columns_dict.get("join"):
                return 1

            else:
                co = results.columns_dict
                co["error_col"] = columnName
                return str(co)

        except Exception as e:
            st_err = str(e)
            ### Eventually we want to be able to return this be return the full json instead of just a 0 or 1
            return 0

          
          
    @staticmethod
    @F.udf("integer")
    def checkIfFilterColumn(sqlString, columnName):
        try: 
            results = Parser(sqlString)

            ## Can be a fully qualified table name or just a column name
            ## If just a select, then skip this and just return the table
            if columnName in results.columns_dict.get("where"):
                return 1
            elif columnName.split(".")[-1] in results.columns_dict.get("where"):
                return 1

            else:
                co = results.columns_dict
                co["error_col"] = columnName
                return str(co)

        except Exception as e:
            st_err = str(e)
            ### Eventually we want to be able to return this be return the full json instead of just a 0 or 1
            return 0
          
        
        
    @staticmethod
    @F.udf("integer")
    def checkIfGroupColumn(sqlString, columnName):
        try: 
            results = Parser(sqlString)

            ## Can be a fully qualified table name or just a column name
            ## If just a select, then skip this and just return the table
            if columnName in results.columns_dict.get("group_by"):
                return 1
            elif columnName.split(".")[-1] in results.columns_dict.get("group_by"):
                return 1

            else:
                co = results.columns_dict
                co["error_col"] = columnName
                return str(co)

        except Exception as e:
            st_err = str(e)
            ### Eventually we want to be able to return this be return the full json instead of just a 0 or 1
            return 0

          
    ## Function to recursively replace views with their underlying tables in text... lazy I know Im sorry I dont have time :'(
    @staticmethod
    @F.udf("string")
    def replace_query_with_views(query_text, view_name_list, view_text_list):

        try:
          query = query_text

          if view_name_list is None or len(view_name_list) == 0:
                  return query
          else:
            
              for i, v in enumerate(view_name_list): 
                  new_text = "( " + view_text_list[i] + " )"

                  query = re.sub(v, new_text, query, flags=re.IGNORECASE)

              return query
            
        except:
          return query_text
          
          
    ## Convert timestamp to milliseconds for API
    @staticmethod
    def ms_timestamp(dt):
        return int(round(dt.replace(tzinfo=timezone.utc).timestamp() * 1000, 0))
    
    
    
    ## Get Start and End range for Query History API
    def get_time_series_lookback(self, lookback_period):
        
        ## Gets time series from current timestamp to now - lookback period - if overrride
        end_timestamp = datetime.now()
        start_timestamp = end_timestamp - timedelta(days = lookback_period)
        ## Convert to ms
        start_ts_ms = self.ms_timestamp(start_timestamp)
        end_ts_ms = self.ms_timestamp(end_timestamp)
        print(f"Getting Query History to parse from period: {start_timestamp} to {end_timestamp}")
        return start_ts_ms, end_ts_ms
 

    ## If no direct time ranges provided (like after a first load, just continue where the log left off)
    def get_most_recent_history_from_log(self, mode='auto', lookback_period=3):
      
        ## This function gets the most recent end timestamp of the query history range, and returns new range from then to current timestamp
        
        start_timestamp = self.spark.sql(f"""SELECT MAX(EndTimestamp) FROM {self.database_name}.query_history_log""").collect()[0][0]
        end_timestamp = datetime.now()
        
        if (start_timestamp is None or mode != 'auto'): 
            if mode == 'auto' and start_timestamp is None:
                print(f"""Mode is auto and there are no previous runs in the log... using lookback period from today: {lookback_period}""")
            elif mode != 'auto' and start_timestamp is None:
                print(f"Manual time interval with lookback period: {lookback_period}")
                
            return self.get_time_series_lookback(lookback_period)
        
        else:
            start_ts_ms = self.ms_timestamp(start_timestamp)
            end_ts_ms = self.ms_timestamp(end_timestamp)
            print(f"Getting Query History to parse from most recent pull at: {start_timestamp} to {end_timestamp}")
            return start_ts_ms, end_ts_ms
    
    
    ## Insert a query history pull into delta log to track state
    def insert_query_history_delta_log(self, start_ts_ms, end_ts_ms):

        ## Upon success of a query history pull, this function logs the start_ts and end_ts that it was pulled into the logs

        try: 
            self.spark.sql(f"""INSERT INTO {self.database_name}.query_history_log (WarehouseIds, WorkspaceName, StartTimestamp, EndTimestamp)
                               VALUES(split('{self.warehouse_ids}', ','), 
                               '{self.workspace_url}', ('{start_ts_ms}'::double/1000)::timestamp, 
                               ('{end_ts_ms}'::double/1000)::timestamp)
            """)

            #v1.5.5
            self.spark.sql(f"OPTIMIZE {self.database_name}.query_history_log ZORDER BY StartTimestamp, EndTimestamp")
        except Exception as e:
            raise(e)
    
    
    ## given a list of catalogs (or just hive_metastore) get a df of all views and their definitions to ensure we profile the source tables
    def get_all_view_texts_df(self):

      catalogs_to_check_list = self.catalogs_to_check_list
      self.spark.conf.set("spark.sql.shuffle.partitions", "1")
      full_view_list = []

      ### TECH DEBT: For now, this is not filtering on the same databases as the views, it goes much broader, but in the future let the user specificy to just the databases. It goes broader because many users do not keep good track of where the source database tables and views are across databases. 
      
      ## Confirm catalogs exist
      catalog_list = list(set([i[0] for i in self.spark.sql("""SHOW CATALOGS""").filter(F.col("catalog").isin(*catalogs_to_check_list)).collect()]))

      for c in catalog_list: 

          database_list = list(set([i[0] for i in self.spark.sql(f"SHOW databases IN {c}").collect() if i != 'information_schema']))

          for d in database_list:
              if d == 'information_schema':
                  pass
              else: 
                
                try:
                    
                    print(d)
                    self.spark.sql(f"""USE CATALOG {c};""")

                    views_df = list(set([i[0] for i in self.spark.sql(f"""SHOW VIEWS IN `{c}`.`{d}`""")
                                                         .filter((F.col("isTemporary") == False) & (F.length(F.col("namespace")) >=1))
                                                         .withColumn("full_view_name", F.concat(F.lit(f'{c}'),F.lit('.'),F.col('namespace'),F.lit('.'),F.col('viewName')))
                                                         .select("full_view_name").collect()]))

                    full_view_list.append(views_df)
                  
                except Exception as e:
                  #TECH DEBT v1.2.0 - Skipping these, people shouldnt name databases poorly anyways
                  print(f"Was not able to process database {d}  skipping file(some special characters are not yet supported in database names (i.e. -! )")
                  pass
                  
                  


      clean_view_list = list(set([item for sublist in full_view_list for item in sublist if len(item) > 0]))


      all_views = []
      self.spark.conf.set("spark.sql.shuffle.partitions", "1")

      print("Finding all views in list and replacing queries with view defintions for profiling...")
      for v in clean_view_list: 

          try:
            
            df_view_text = self.spark.sql(f"""DESCRIBE TABLE EXTENDED {v}""")

            new_view = df_view_text.filter(F.col("col_name") == F.lit("View Text")).withColumn("view_name", F.lit(v)).select("view_name","data_type").collect()[0]

            view_view_dict = {"view_name":new_view[0], "view_text": new_view[1]}

            all_views.append(view_view_dict)
            
          except Exception as e:
            print(f"Getting view definition failed with error: {str(e)}")
            pass
        
          


      print(f"View List: \n {all_views}")
      ## If there are no views, just send back empty df
      if len(all_views) == 0:
        all_views = [{"view_name":None, "view_text": None}]
    
    
      view_struct = StructType(
                      [StructField("view_name", StringType(), True),
                      StructField("view_text", StringType(), True)]
                      )

      views_df = self.spark.createDataFrame(all_views, schema=view_struct)

      ## Go back to a normal parallelism
      self.spark.conf.set("spark.sql.shuffle.partitions", self.shuffle_partitions)

      return views_df

  

    ## Run the Query History Pull (main function)
    def load_query_history(self, dbx_token: str, mode: str ='auto', lookback_period_days: int =3, included_statuses: [str] =["QUEUED" "RUNNING" "CANCELED" "FAILED" "FINISHED"]):
        
        ## Modes are 'auto' and 'manual' - auto, means it manages its own state, manual means you override the time frame to analyze no matter the history
        lookback_period = int(lookback_period_days)
        warehouse_ids_list = self.warehouse_ids_list
        workspace_url = self.workspace_url

        print(f"""Loading Query Profile to delta from workspace: {workspace_url} \n 
              from Warehouse Ids: {warehouse_ids_list} \n for the last {lookback_period} days...""")
        
        ## Get time series range based on 
        ## If override = True, then choose lookback period in days
        start_ts_ms, end_ts_ms = self.get_most_recent_history_from_log(mode, lookback_period)
        
        ## Put together request 

        ## v1.5.5 validate custom included statuses with enum
        
        request_string = {
            "filter_by": {
              "query_start_time_range": {
              "end_time_ms": end_ts_ms,
              "start_time_ms": start_ts_ms
            },
            "statuses": included_statuses,
            "warehouse_ids": warehouse_ids_list
            },
            "include_metrics": "true",
            "max_results": "1000"
        }

        ## Convert dict to json
        v = json.dumps(request_string)
        
        uri = f"https://{workspace_url}/api/2.0/sql/history/queries"
        headers_auth = {"Authorization":f"Bearer {dbx_token}"}

        
        ## This file could be large
        ## Convert response to dict
        
        #### Get Query History Results from API
        endp_resp = requests.get(uri, data=v, headers=headers_auth).json()
        
        initial_resp = endp_resp.get("res")
        
        if initial_resp is None:
            print(f"DBSQL Has no queries on the warehouse for these times:{start_ts_ms} - {end_ts_ms}")
            initial_resp = []
            ## Continue anyways cause there could be old queries and we want to still compute aggregates
        
        
        next_page = endp_resp.get("next_page_token")
        has_next_page = endp_resp.get("has_next_page")
        

        if has_next_page is True:
            print(f"Has next page?: {has_next_page}")
            print(f"Getting next page: {next_page}")

        ## Page through results   
        page_responses = []

        while has_next_page is True: 

            print(f"Getting results for next page... {next_page}")

            raw_page_request = {
            "include_metrics": "true",
            "max_results": 1000,
            "page_token": next_page
            }

            json_page_request = json.dumps(raw_page_request)

            ## This file could be large
            current_page_resp = requests.get(uri,data=json_page_request, headers=headers_auth).json()
            current_page_queries = current_page_resp.get("res")

            ## Add Current results to total results or write somewhere (to s3?)

            page_responses.append(current_page_queries)

            ## Get next page
            next_page = current_page_resp.get("next_page_token")
            has_next_page = current_page_resp.get("has_next_page")

            if has_next_page is False:
                break

                
        ## Coaesce all responses     
        all_responses = [x for xs in page_responses for x in xs] + initial_resp
        print(f"Saving {len(all_responses)} Queries To Delta for Profiling")

        
        ## Start Profiling Process
        try: 
            ## v.1.3.1 skip insert if no responses from API
            if (all_responses is None or len(all_responses) == 0):
              print("No Results to insert. Skipping.")
              pass
            
            else:
              ## Get responses and save to Delta 
              raw_queries_df = (self.spark.createDataFrame(all_responses))
              raw_queries_df.createOrReplaceTempView("raw")

              self.spark.sql(f"""INSERT INTO {self.database_name}.raw_query_history_statistics (query_id,
                          query_hash, -- v.1.4.0 - MUST PARSE DISTINCT Queryies cause DBX query id is not by query
                          query_start_time_ms,
                          query_end_time_ms,
                          duration,
                          status,
                          statement_type,
                          error_message,
                          executed_as_user_id,
                          executed_as_user_name,
                          rows_produced,
                          metrics,
                          endpoint_id,
                          channel_used,
                          lookup_key,
                          update_timestamp,
                          update_date,
                          query_text)

                          SELECT
                          query_id,
                          sha(query_text) AS query_hash, -- v.1.4.0 - MUST PARSE DISTINCT Queryies cause DBX query id is not by query
                          query_start_time_ms,
                          query_end_time_ms,
                          duration,
                          status,
                          statement_type,
                          error_message,
                          executed_as_user_id,
                          executed_as_user_name,
                          rows_produced,
                          metrics,
                          endpoint_id,
                          channel_used::string AS channel_used,
                          lookup_key::string,
                          now() AS update_timestamp,
                          now()::date AS update_date,
                          query_text
                          FROM raw
                          -- v1.5.5 removed WHERE statement_type IN ('SELECT', 'INSERT', 'REPLACE'), we want raw table to have everything and then filter for performance later
                          ;
                          """)
            
            
            ## If successfull, insert log
            self.insert_query_history_delta_log(start_ts_ms, end_ts_ms)
            
            ## Add optimization to reduce small file problem
            ## v1.5.5 - added ZORDER columns to this
            self.spark.sql(f"""OPTIMIZE {self.database_name}.raw_query_history_statistics ZORDER BY (query_start_time_ms, update_timestamp)""")
            return

        except Exception as e:
          raise(e)
  

    ### v.1.5.5 build profile aggregates for performance separately from pulling history
    ## TO DO: Make Aggregates Incremental
    def build_query_history_profile(self, dbx_token: str, mode: str ='auto', lookback_period_days: int =3, included_statuses: [str] =["QUEUED" "RUNNING" "CANCELED" "FAILED" "FINISHED"]):

      ## Loads Query History First
      self.load_query_history(dbx_token=dbx_token, mode=mode, lookback_period_days=lookback_period_days, included_statuses=included_statuses)

      ## Now built the profile / separately
      try:
        ## Build Aggregate Summary Statistics with old and new queries
        self.spark.sql(f"""
            --Calculate Query Statistics to get importance Rank by Query (duration, rows_returned)
            -- This is an AGGREGATE table that needs to be rebuilt every time from the source -- not incremental
            CREATE OR REPLACE TABLE {self.database_name}.query_summary_statistics
            AS (
              WITH raw_query_stats AS (
                SELECT query_hash AS query_id, -- v.1.4.0 replace id with hash to actually aggregate by distinct query text
                AVG(duration) AS AverageQueryDuration,
                AVG(rows_produced) AS AverageRowsProduced,
                COUNT(*) AS TotalQueryRuns,
                AVG(duration)*COUNT(*) AS DurationTimesRuns
                FROM {self.database_name}.raw_query_history_statistics 
                WHERE  -- v1.5.5 Added filter so we dont query a HUGE table
                query_start_time_ms >= (unix_timestamp((now() - INTERVAL '{lookback_period_days} DAYS')::timestamp) * 1000)
                AND status IN('FINISHED', 'CANCELED')
                AND statement_type IN ('SELECT', 'INSERT', 'REPLACE') -- v1.5.5 - removed this filter in raw logs and kept everything and only filtered here for performance
                GROUP BY query_hash
              )
              SELECT 
              *
              FROM raw_query_stats
            )
            """)
        
        ## Optional param to clean and replace views with table definitions
        if self.scrub_views == True:
          
          ## Parse SQL Query and Save into parsed distinct queries table
          ## v.1.4.0 replace query_id with query_hash - this should speed the parsing process a LOT
          ## v1.5.5 - Add lookback filter to only get new queries that are recent to profile
          df_pre_clean = self.spark.sql(f"""SELECT DISTINCT query_hash, query_text 
                                        FROM {self.database_name}.raw_query_history_statistics
                                        WHERE
                                        query_start_time_ms >= (unix_timestamp((now() - INTERVAL '{lookback_period_days} DAYS')::timestamp) * 1000)
                                        
                                        """)

          ## Add View Definition Replacement Here before Profiling

          views_df = self.get_all_view_texts_df()

          df_pre_clean.createOrReplaceTempView("queries")
          views_df.createOrReplaceTempView("views")

          ## Clean Query Text and Replace Views with Underlying Definitions to profile accurately
          
          ### Register the udf for SQL 
          self.spark.udf.register("replace_query_with_views", self.replace_query_with_views)

          df = self.spark.sql("""WITH view_int AS (
                  SELECT q.*,
                  REGEXP_REPLACE(q.query_text, '`', '') AS scrubbed_query,
                  collect_list(view_name) AS view_name_list,
                  collect_list(view_text) AS view_text_list
                  FROM queries AS q
                  LEFT JOIN views AS v ON REGEXP_REPLACE(q.query_text, '`', '') ILIKE (CONCAT('%', v.view_name, '%')) --this is expensive but we gotta do it for now
                  GROUP BY q.query_hash, q.query_text, REGEXP_REPLACE(q.query_text, '`', '')
                  )
                  --clean up views and replace with table definition
                  SELECT
                  query_hash,
                  replace_query_with_views(scrubbed_query, view_name_list, view_text_list) AS query_text
                  FROM view_int""")
          
          
        elif self.scrub_views == False:
          ## v.1.4.0 replace query_id with self generated query_hash
          ## v1.5.5 Add where clause so we dont filter on ALL the queries
          df = self.spark.sql(f"""SELECT DISTINCT query_hash, query_text FROM {self.database_name}.raw_query_history_statistics
                              WHERE
                              query_start_time_ms >= (unix_timestamp((now() - INTERVAL '{lookback_period_days} DAYS')::timestamp) * 1000)
                              """)
          
        ## Profile full tables

        df_profiled = (df.withColumn("profiled_columns", self.getParsedFilteredColumnsinSQL(F.col("query_text")))
                )

        df_profiled.createOrReplaceTempView("new_parsed")
        
        
        self.spark.sql(f"""
            MERGE INTO {self.database_name}.parsed_distinct_queries AS target
            USING new_parsed AS source
            ON source.query_hash = target.query_hash
            WHEN MATCHED THEN UPDATE SET target.query_text = source.query_text
            WHEN NOT MATCHED THEN 
                INSERT (target.query_hash, target.query_text, target.profiled_columns) 
                VALUES (source.query_hash, source.query_text, source.profiled_columns)""")
        
        ## v1.5.5 Add optimization to this table
        self.spark.sql(f"""OPTIMIZE {self.database_name}.parsed_distinct_queries ZORDER BY (query_hash)""")
        ## Calculate statistics on profiled queries

        ## v1.4.1 Adding table filtering earlier in the process, need to fully qualify table names. Currently optimizer will NOT do a good job at finding the tables when a USE statement is used instead of the real names
        pre_stats_df = (self.spark.sql(f"""
              WITH exploded_parsed_cols AS (SELECT DISTINCT
              explode(profiled_columns) AS explodedCols,
              query_hash,
              query_text
              FROM {self.database_name}.parsed_distinct_queries
              ),

              step_2 AS (SELECT DISTINCT
              CONCAT(COALESCE(REVERSE(SPLIT(split(explodedCols, ":")[0], '[.]'))[2], 'hive_metastore'), 
                '.', COALESCE(REVERSE(SPLIT(split(explodedCols, ":")[0], '[.]'))[1], 'default'), 
                '.', REVERSE(SPLIT(split(explodedCols, ":")[0], '[.]'))[0]) AS TableName, -- v1.4.1 !! Since we filter now, we have to handle table names that are not fully qualified
              split(explodedCols, ":")[1] AS ColumnName,
              root.query_text,
              hist.*
              FROM exploded_parsed_cols AS root
              LEFT JOIN {self.database_name}.query_summary_statistics AS hist ON root.query_hash = hist.query_id --SELECT statements only included (v1.4.0 Query ID is Query Hash here now)
              )

              SELECT *,
              size(split(query_text, ColumnName)) - 1 AS NumberOfColumnOccurrences
              FROM step_2
            """)
                        )
        
        
        ## Add a database/catalog level inclusion/exclusion statement
        
        ## Add table filter inclusion/exclusion statements here
        ## This inclusion logic is pre processing on class initialization, so just abide by the list
        self.get_all_tables_to_monitor()
        subset_tables_to_parse = self.get_table_list()
        
        pre_stats_df = pre_stats_df.filter(F.col("TableName").isin(subset_tables_to_parse))


            
        pre_stats_df  = (pre_stats_df.withColumn("isUsedInJoin", self.checkIfJoinColumn(F.col("query_text"), F.concat(F.col("TableName"), F.lit("."), F.col("ColumnName"))))
            .withColumn("isUsedInFilter", self.checkIfFilterColumn(F.col("query_text"), F.concat(F.col("TableName"), F.lit("."), F.col("ColumnName"))))
            .withColumn("isUsedInGroup", self.checkIfGroupColumn(F.col("query_text"), F.concat(F.col("TableName"), F.lit("."), F.col("ColumnName"))))
                        )
            

        pre_stats_df.createOrReplaceTempView("withUseFlags")

        self.spark.sql(f"""
        CREATE OR REPLACE TABLE {self.database_name}.query_column_statistics
        AS SELECT * FROM withUseFlags
        """)

        #### Calculate more statistics

        self.spark.sql(f"""CREATE OR REPLACE TABLE {self.database_name}.read_statistics_column_level_summary
                AS
                WITH test_q AS (
                    SELECT * FROM {self.database_name}.query_column_statistics
                    WHERE length(ColumnName) >= 1 -- filter out queries with no joins or predicates TO DO: Add database filtering here

                ),
                step_2 AS (
                    SELECT 
                    TableName,
                    ColumnName,
                    MAX(isUsedInJoin) AS isUsedInJoin,
                    MAX(isUsedInFilter) AS isUsedInFilter,
                    MAX(isUsedInGroup) AS isUsedInGroup,
                    SUM(isUsedInJoin) AS NumberOfQueriesUsedInJoin,
                    SUM(isUsedInFilter) AS NumberOfQueriesUsedInFilter,
                    SUM(isUsedInGroup) AS NumberOfQueriesUsedInGroup,
                    COUNT(DISTINCT query_id) AS QueryReferenceCount, -- This is the number if DISTINCT queries the column it used in, NOT runs
                    SUM(DurationTimesRuns) AS RawTotalRuntime,
                    AVG(AverageQueryDuration) AS AvgQueryDuration,
                    SUM(NumberOfColumnOccurrences) AS TotalColumnOccurrencesForAllQueries,
                    AVG(NumberOfColumnOccurrences) AS AvgColumnOccurrencesInQueryies
                    FROM test_q
                    WHERE length(ColumnName) >=1
                    GROUP BY TableName, ColumnName
                )
                SELECT
                CONCAT(COALESCE(REVERSE(SPLIT(spine.TableName, '[.]'))[2], 'hive_metastore'), 
                '.', COALESCE(REVERSE(SPLIT(spine.TableName, '[.]'))[1], 'default'), 
                '.', REVERSE(SPLIT(spine.TableName, '[.]'))[0]) AS TableName,
                ColumnName,
                isUsedInJoin,
                isUsedInFilter,
                isUsedInGroup,
                NumberOfQueriesUsedInJoin,
                NumberOfQueriesUsedInFilter,
                NumberOfQueriesUsedInGroup,
                QueryReferenceCount,
                RawTotalRuntime,
                AvgQueryDuration,
                TotalColumnOccurrencesForAllQueries,
                AvgColumnOccurrencesInQueryies
                FROM step_2 AS spine
                ; """)


        #### Standard scale the metrics 
        df = self.spark.sql(f"""SELECT * FROM {self.database_name}.read_statistics_column_level_summary""")

        columns_to_scale = ["QueryReferenceCount", 
                            "RawTotalRuntime", 
                            "AvgQueryDuration", 
                            "TotalColumnOccurrencesForAllQueries", 
                            "AvgColumnOccurrencesInQueryies"]

        min_exprs = {x: "min" for x in columns_to_scale}
        max_exprs = {x: "max" for x in columns_to_scale}

        ## Apply basic min max scaling by table for now

        dfmin = df.groupBy("TableName").agg(min_exprs)
        dfmax = df.groupBy("TableName").agg(max_exprs)

        df_boundaries = dfmin.join(dfmax, on="TableName", how="inner")

        df_pre_scaled = df.join(df_boundaries, on="TableName", how="inner")

        df_scaled = (df_pre_scaled
                  .withColumn("QueryReferenceCountScaled", F.coalesce((F.col("QueryReferenceCount") - F.col("min(QueryReferenceCount)"))/(F.col("max(QueryReferenceCount)") - F.col("min(QueryReferenceCount)")), F.lit(0)))
                  .withColumn("RawTotalRuntimeScaled", F.coalesce((F.col("RawTotalRuntime") - F.col("min(RawTotalRuntime)"))/(F.col("max(RawTotalRuntime)") - F.col("min(RawTotalRuntime)")), F.lit(0)))
                  .withColumn("AvgQueryDurationScaled", F.coalesce((F.col("AvgQueryDuration") - F.col("min(AvgQueryDuration)"))/(F.col("max(AvgQueryDuration)") - F.col("min(AvgQueryDuration)")), F.lit(0)))
                  .withColumn("TotalColumnOccurrencesForAllQueriesScaled", F.coalesce((F.col("TotalColumnOccurrencesForAllQueries") - F.col("min(TotalColumnOccurrencesForAllQueries)"))/(F.col("max(TotalColumnOccurrencesForAllQueries)") - F.col("min(TotalColumnOccurrencesForAllQueries)")), F.lit(0)))
                  .withColumn("AvgColumnOccurrencesInQueriesScaled", F.coalesce((F.col("AvgColumnOccurrencesInQueryies") - F.col("min(AvgColumnOccurrencesInQueryies)"))/(F.col("max(AvgColumnOccurrencesInQueryies)") - F.col("min(AvgColumnOccurrencesInQueryies)")), F.lit(0)))
                  .selectExpr("TableName", "ColumnName", "isUsedInJoin", "isUsedInFilter","isUsedInGroup","NumberOfQueriesUsedInJoin","NumberOfQueriesUsedInFilter","NumberOfQueriesUsedInGroup","QueryReferenceCount", "RawTotalRuntime", "AvgQueryDuration", "TotalColumnOccurrencesForAllQueries", "AvgColumnOccurrencesInQueryies", "QueryReferenceCountScaled", "RawTotalRuntimeScaled", "AvgQueryDurationScaled", "TotalColumnOccurrencesForAllQueriesScaled", "AvgColumnOccurrencesInQueriesScaled")
                    )


        df_scaled.createOrReplaceTempView("final_scaled_reads")

        self.spark.sql(f"""CREATE OR REPLACE TABLE {self.database_name}.read_statistics_scaled_results 
        AS
        SELECT * FROM final_scaled_reads""")


        print(f"""Completed Query Profiling! Results can be found here:\n
        SELECT * FROM {self.database_name}.read_statistics_scaled_results""")

        return
        
      except Exception as e:

        print(f"Could not profile query history of ({len(all_responses)}) queries. :( There may not be any queries to profile since last run, either ignore or switch to manual mode with a further lookback window...")
        raise(e)


##################################
  
######## Delta Profiler ##########
class DeltaProfiler(DeltaOptimizerBase):
    
    def __init__(self, catalog_filter_mode='all', 
                 catalog_filter_list=[], 
                 database_filter_mode ='all', 
                 database_filter_list=[], 
                 table_filter_mode='all', 
                 table_filter_list=[],
                 database_name="hive_metastore.delta_optimizer", 
                 database_location=None,
                 shuffle_partitions=None):
        
        ## TO DO: MUST eventually deal with if someone says "all" dbs and config is not a subset, cause then it will monitor all catalogs and all databases
        ## Right now, we just default for 
        
        ## Makes fully qualified database name use hive_metastore by default
        full_qualitfied_delta_optimizer_db_name = 'hive_metastore.delta_optimizer'
        
        if len(database_name.split(".")) == 1:
          full_qualitfied_delta_optimizer_db_name = 'hive_metastore.' + database_name
        else: 
          full_qualitfied_delta_optimizer_db_name = database_name

        ## Initializes the Core optimizer tables and defines the list of tables it is supposed to track and profile for this instance
        super().__init__(database_name=full_qualitfied_delta_optimizer_db_name, 
                         database_location=database_location,
                         catalog_filter_mode = catalog_filter_mode, 
                         catalog_filter_list=catalog_filter_list, 
                         database_filter_mode=database_filter_mode, 
                         database_filter_list = database_filter_list, 
                         table_filter_mode=table_filter_mode, 
                         table_filter_list=table_filter_list, 
                         shuffle_partitions=shuffle_partitions)
        
        
        ## File size map
        self.file_size_map = [{"max_table_size_gb": 8, "file_size": '16mb'},
                 {"max_table_size_gb": 16, "file_size": '32mb'},
                 {"max_table_size_gb": 32, "file_size": '64mb'},
                 {"max_table_size_gb": 64, "file_size": '64mb'},
                 {"max_table_size_gb": 128, "file_size": '128mb'},
                 {"max_table_size_gb": 256, "file_size": '128mb'},
                 {"max_table_size_gb": 512, "file_size": '256mb'},
                 {"max_table_size_gb": 1024, "file_size": '256mb'},
                 {"max_table_size_gb": 2560, "file_size": '512mb'},
                 {"max_table_size_gb": 3072, "file_size": '716mb'},
                 {"max_table_size_gb": 5120, "file_size": '1gb'},
                 {"max_table_size_gb": 7168, "file_size": '1gb'},
                 {"max_table_size_gb": 10240, "file_size": '1gb'},
                 {"max_table_size_gb": 51200, "file_size": '1gb'},
                 {"max_table_size_gb": 102400, "file_size": '1gb'},
                {"max_table_size_gb": 204800, "file_size": '1gb'},
                {"max_table_size_gb": 409600, "file_size": '1gb'},
                {"max_table_size_gb": 819200, "file_size": '1gb'},
                {"max_table_size_gb": 1638400, "file_size": '2gb'}]


        self.file_size_df = (self.spark.createDataFrame(self.file_size_map))
        
        ## df of tables initilization
        self.tbl_df = self.spark.sql(f"show tables in {self.database_name} like 'xxx_delta_optimizer'")

        return
    
    
    ### Static methods / udfs
    @staticmethod
    @F.udf("string")
    def buildCardinalitySampleSQLStatement(table_name, column_list, sample_size:float):

        ## This query ensures that it does not scan the whole table and THEN limits
        sample_string = f"WITH sample AS (SELECT * FROM {table_name} LIMIT {sample_size})"
        sql_from = f" FROM sample"
        str2 = [" SELECT COUNT(0) AS TotalCount"]

        for i in column_list:
            sql_string = f"COUNT(DISTINCT {i}) AS DistinctCountOf_{i}"
            str2.append(sql_string)


        final_sql = sample_string + ", ".join(str2) + sql_from

        return final_sql

    
    ## Parse Transaction Log
    def parse_stats_for_tables(self):
        
        ## If you only run this, must get tables to monitor first
        self.get_all_tables_to_monitor()
        
        ## De dups
        table_list = self.get_table_list()

        for tbl in table_list:
            print(f"Running History Analysis for Table: {tbl}")

            try: 

                ## Get Transaction log with relevant transactions
                hist_df = self.spark.sql(f"""
                WITH hist AS
                (DESCRIBE HISTORY {tbl}
                )

                SELECT version, timestamp,
                operationParameters.predicate,
                operationMetrics.executionTimeMs
                FROM hist
                WHERE operation IN ('MERGE', 'DELETE')
                ;
                """)

                hist_df.createOrReplaceTempView("hist_df")

                ## Get DF of Columns for that table

                df_cols = [Row(i) for i in self.spark.sql(f"""SELECT * FROM {tbl}""").columns]

                df = self.spark.sparkContext.parallelize(df_cols).toDF().withColumnRenamed("_1", "Columns")

                df.createOrReplaceTempView("df_cols")

                ## Calculate stats for this table

                df_stats = (self.spark.sql("""
                -- Full Cartesian product small table.. maybe since one table at a time... parallelize later 
                WITH raw_results AS (
                SELECT 
                *,
                predicate LIKE (concat('%',`Columns`::string,'%')) AS HasColumnInMergePredicate
                FROM df_cols
                JOIN hist_df
                )

                SELECT Columns AS ColumnName,
                CASE WHEN MAX(HasColumnInMergePredicate) = 'true' THEN 1 ELSE 0 END AS HasColumnInMergePredicate,
                COUNT(DISTINCT CASE WHEN HasColumnInMergePredicate = 'true' THEN `version` ELSE NULL END)::integer AS NumberOfVersionsPredicateIsUsed,
                AVG(executionTimeMs::integer) AS AvgMergeRuntimeMs
                FROM raw_results
                GROUP BY Columns
                """)
                  .withColumn("TableName", F.lit(tbl))
                  .withColumn("UpdateTimestamp", F.current_timestamp())
                  .select("TableName", "ColumnName", "HasColumnInMergePredicate", "NumberOfVersionsPredicateIsUsed", "AvgMergeRuntimeMs", "UpdateTimestamp")
                 )

                (df_stats.createOrReplaceTempView("source_stats"))

                self.spark.sql(f"""MERGE INTO {self.database_name}.write_statistics_merge_predicate AS target
                USING source_stats AS source
                ON source.TableName = target.TableName AND source.ColumnName = target.ColumnName
                WHEN MATCHED THEN UPDATE SET * 
                WHEN NOT MATCHED THEN INSERT *
                """
                              )
                
                ## TO DO: Add another column that looks for columns ALREADY ZORDERed for table
                print(f"History analysis successful! (will be empty if no tables using predicates) You can see results here: SELECT * FROM {self.database_name}.write_statistics_merge_predicate")

            except Exception as e:
                print(f"Skipping analysis for table {tbl} for error: {str(e)}")
                pass
            
        return
    
    
    
    ## Create final transaction log stats tables
    def build_all_tables_stats(self):
        
        ## If you only run this, must get tables to monitor first
        self.get_all_tables_to_monitor()
        
        table_list = self.get_table_list()

        for tbl in table_list:

            print(f"Prepping Delta Table Stats: {tbl}")

            try: 
                df_cols = [Row(i) for i in self.spark.sql(f"""SELECT * FROM {tbl}""").columns]

                df = self.spark.sparkContext.parallelize(df_cols).toDF().withColumnRenamed("_1", "ColumnName").withColumn("TableName", F.lit(tbl))

                df.createOrReplaceTempView("df_cols")

                ## This needs to be re-worked
                self.spark.sql(f"""INSERT INTO {self.database_name}.all_tables_cardinality_stats
                SELECT CONCAT(COALESCE(REVERSE(SPLIT(source.TableName, '[.]'))[2], 'hive_metastore'), 
                            '.', COALESCE(REVERSE(SPLIT(source.TableName, '[.]'))[1], 'default'), 
                            '.', REVERSE(SPLIT(source.TableName, '[.]'))[0]) AS TableName, 
                      ColumnName, NULL, NULL, NULL, NULL, NULL, NULL, NULL, current_timestamp() FROM df_cols AS source
                WHERE NOT EXISTS (SELECT 1 FROM {self.database_name}.all_tables_cardinality_stats ss 
                                  WHERE (CONCAT(COALESCE(REVERSE(SPLIT(ss.TableName, '[.]'))[2], 'hive_metastore'), 
                            '.', COALESCE(REVERSE(SPLIT(ss.TableName, '[.]'))[1], 'default'), 
                            '.', REVERSE(SPLIT(ss.TableName, '[.]'))[0])) = 
                            
                            (CONCAT(COALESCE(REVERSE(SPLIT(source.TableName, '[.]'))[2], 'hive_metastore'), 
                            '.', COALESCE(REVERSE(SPLIT(source.TableName, '[.]'))[1], 'default'), 
                            '.', REVERSE(SPLIT(source.TableName, '[.]'))[0])) AND ss.ColumnName = source.ColumnName)
                """)

            except Exception as e:
                print(f"Skipping analysis for table {tbl} for error: {str(e)}")
                pass

            print(f"Collecting Size and Partition Stats for : {tbl}")


            try: 
              
              ## For some reason, DESCRIBE DETAIL likes to use spark_catalog instead of the hive_metastore, which nothing else does
                table_df = (self.spark.sql(f"""DESCRIBE DETAIL {tbl}""")
                            .where("format = 'delta'") ## v1.3.1 - Only run this analysis for delta tables
                            .selectExpr("replace(name, 'spark_catalog', 'hive_metastore') AS name", 
                                "sizeInBytes", "sizeInBytes/(1024*1024*1024) AS sizeInGB", 
                                "partitionColumns")
                            )

                table_df.createOrReplaceTempView("table_core")
                self.file_size_df.createOrReplaceTempView("file_size_map")

                ## !!! Not idempotent, must choose must recent version to work off of
                ## Map to file size mapping config
                
                self.spark.sql(f"""
                WITH ss AS (
                    SELECT 
                    spine.*,
                    file_size AS mapped_file_size,
                    ROW_NUMBER() OVER (PARTITION BY name ORDER BY max_table_size_gb) AS SizeRank
                    FROM table_core AS spine 
                    LEFT JOIN file_size_map AS fs ON spine.sizeInGB::integer <= fs.max_table_size_gb::integer
                    )
                    -- Pick smaller file size config by table size
                    INSERT INTO {self.database_name}.all_tables_table_stats
                    SELECT
                    CONCAT(COALESCE(REVERSE(SPLIT(name::string, '[.]'))[2], 'hive_metastore'), 
                            '.', COALESCE(REVERSE(SPLIT(name::string, '[.]'))[1], 'default'), 
                            '.', REVERSE(SPLIT(name::string, '[.]'))[0]) AS TableName, 
                    sizeInBytes::float AS sizeInBytes,
                    sizeInGB::float AS sizeInGB,
                    partitionColumns::array<string> AS partitionColumns,
                    mapped_file_size::string AS mappedFileSize,
                    current_timestamp() AS UpdateTimestamp
                    FROM ss WHERE SizeRank = 1 
                  """)
                
            except Exception as e:

                print(f"Failed to parse stats for {tbl} with error: {str(e)}")
                continue
                
              
            ## This is now inside the for loop so the tables optimize after every run
            self.spark.sql(f"""OPTIMIZE {self.database_name}.all_tables_cardinality_stats""")
            self.spark.sql(f"""OPTIMIZE {self.database_name}.all_tables_table_stats""")
        
        return
    
    
    def build_cardinality_stats(self, sample_size = 1000000):
        
        
        ## Check and see which tables/columns you even need to build statements for
        self.spark.sql(f"""WITH filter_cols AS (
            SELECT DISTINCT
            spine.FullTableName AS TableName,
            spine.ColumnName,
            CASE WHEN reads.QueryReferenceCount >= 1 THEN 1 ELSE 0 END AS IsUsedInReads,
            CASE WHEN writes.HasColumnInMergePredicate >= 1 THEN 1 ELSE 0 END AS IsUsedInWrites
            FROM (SELECT 
                  CONCAT(COALESCE(REVERSE(SPLIT(s.TableName, '[.]'))[2], 'hive_metastore'), 
                            '.', COALESCE(REVERSE(SPLIT(s.TableName, '[.]'))[1], 'default'), 
                            '.', REVERSE(SPLIT(s.TableName, '[.]'))[0]) AS FullTableName, * 
                 FROM {self.database_name}.all_tables_cardinality_stats s) AS spine
            LEFT JOIN (SELECT 
            CONCAT(COALESCE(REVERSE(SPLIT(s.TableName, '[.]'))[2], 'hive_metastore'), 
                            '.', COALESCE(REVERSE(SPLIT(s.TableName, '[.]'))[1], 'default'), 
                            '.', REVERSE(SPLIT(s.TableName, '[.]'))[0]) AS FullTableName, *
                            FROM {self.database_name}.read_statistics_scaled_results AS s) AS reads 
                     ON spine.FullTableName = reads.FullTableName 
                        AND spine.ColumnName = reads.ColumnName
            LEFT JOIN (SELECT
            CONCAT(COALESCE(REVERSE(SPLIT(w.TableName, '[.]'))[2], 'hive_metastore'), 
                            '.', COALESCE(REVERSE(SPLIT(w.TableName, '[.]'))[1], 'default'), 
                            '.', REVERSE(SPLIT(w.TableName, '[.]'))[0]) AS FullTableName, *
                            FROM {self.database_name}.write_statistics_merge_predicate AS w) AS writes ON spine.FullTableName = writes.FullTableName
                                        AND spine.ColumnName = writes.ColumnName
            )
            
        MERGE INTO {self.database_name}.all_tables_cardinality_stats AS target
        USING filter_cols AS source ON source.TableName = target.TableName AND source.ColumnName = target.ColumnName --this table name is already made FullTableName
        WHEN MATCHED THEN UPDATE SET
        target.IsUsedInReads = source.IsUsedInReads,
        target.IsUsedInWrites = source.IsUsedInWrites
        """)
        
        
        ## Build Cardinality Stats
        df_cardinality = (
                self.spark.sql(f"""
                    SELECT TableName, collect_list(ColumnName) AS ColumnList
                    FROM {self.database_name}.all_tables_cardinality_stats
                    WHERE (IsUsedInReads > 0 OR IsUsedInWrites > 0) --If columns is not used in any joins or predicates, lets not do cardinality stats
                    GROUP BY TableName
                """)
                .withColumn("cardinalityStatsStatement", self.buildCardinalitySampleSQLStatement(F.col("TableName"), F.col("ColumnList"), F.lit(sample_size)))
        )
        # Take sample size of 1M, if table is smaller, index on the count
        
        ## Build config to loop through inefficiently like a noob
        cardinality_statement = df_cardinality.collect()
        cardinality_config = {i[0]: {"columns": i[1], "sql": i[2]} for i in cardinality_statement}
        
        ## Loop through and build samples
        for i in cardinality_config:
            try:

                print(f"Building Cardinality Statistics for {i} ... \n")

                ## Gets cardinality stats on tables at a time, but all columns in parallel, then pivots results to long form
                wide_df = (self.spark.sql(cardinality_config.get(i).get("sql")))
                table_name = i
                clean_list = [ "'" + re.search('[^_]*_(.*)', i).group(1) + "'" + ", " + i for i in wide_df.columns if re.search('[^_]*_(.*)', i) is not None]
                clean_expr = ", ".join(clean_list)
                unpivot_Expr = f"stack({len(clean_list)}, {clean_expr}) as (ColumnName,ColumnDistinctCount)"	

                unpivot_DataFrame = (wide_df.select(F.expr(unpivot_Expr), "TotalCount").withColumn("TableName", F.lit(table_name))
                                     .withColumn("CardinalityProportion", F.col("ColumnDistinctCount").cast("double")/F.col("TotalCount").cast("double"))
                                    )

                ## Standard Mix/Max Scale Proportion
                columns_to_scale = ["CardinalityProportion"]
                min_exprs = {x: "min" for x in columns_to_scale}
                max_exprs = {x: "max" for x in columns_to_scale}

                ## Apply basic min max scaling by table for now

                dfmin = unpivot_DataFrame.groupBy("TableName").agg(min_exprs)
                dfmax = unpivot_DataFrame.groupBy("TableName").agg(max_exprs)

                df_boundaries = dfmin.join(dfmax, on="TableName", how="inner")

                df_pre_scaled = unpivot_DataFrame.join(df_boundaries, on="TableName", how="inner")

                df_scaled = (df_pre_scaled
                         .withColumn("CardinalityScaled", F.coalesce((F.col("CardinalityProportion") - F.col("min(CardinalityProportion)"))/(F.col("max(CardinalityProportion)") - F.col("min(CardinalityProportion)")), F.lit(0)))
                            )

                #display(df_scaled.orderBy("TableName"))

                df_scaled.createOrReplaceTempView("card_stats")

                self.spark.sql(f"""
                    MERGE INTO {self.database_name}.all_tables_cardinality_stats AS target 
                    USING card_stats AS source ON source.TableName = target.TableName AND source.ColumnName = target.ColumnName
                    WHEN MATCHED THEN UPDATE SET
                    target.SampleSize = CAST({sample_size} AS INTEGER),
                    target.TotalCountInSample = source.TotalCount,
                    target.DistinctCountOfColumnInSample = source.ColumnDistinctCount,
                    target.CardinalityProportion = (CAST(ColumnDistinctCount AS DOUBLE) / CAST(TotalCount AS DOUBLE)),
                    target.CardinalityProportionScaled = source.CardinalityScaled::double,
                    target.UpdateTimestamp = current_timestamp()
                """)

            except Exception as e:
                print(f"Skipping table {i} due to error {str(e)} \n")
                pass
        
        ## Optimize tables (ironic? ahah)
        self.spark.sql(f"""OPTIMIZE {self.database_name}.all_tables_cardinality_stats""")
        self.spark.sql(f"""OPTIMIZE {self.database_name}.all_tables_table_stats""")
        
        return
        
#####################################################

######## Delta Optimizer Class ##########

class DeltaOptimizer(DeltaOptimizerBase):
    
    def __init__(self, database_name="hive_metastore.delta_optimizer", database_location=None, shuffle_partitions=None):
        
        super().__init__(database_name=database_name, database_location=database_location, shuffle_partitions=shuffle_partitions)
        
        return
    
    @staticmethod
    @F.udf("string")
    def getAnalyzeTableCommand(inputTableName, tableSizeInGb, relativeColumns):

        ### Really basic heuristic to calculate statistics, can increase nuance in future versions
        sqlExpr = f"ANALYZE TABLE {inputTableName} COMPUTE STATISTICS FOR ALL COLUMNS;"
        try: 
          
          tableSizeInGbLocal = float(tableSizeInGb)

          if tableSizeInGbLocal > 100:
            
            rel_Cols = str(relativeColumns).split(",")
            colExpr = ", ".join(rel_Cols)
            
            ## If there are no columns to filter on, skip
            if len(colExpr) >= 1:
              sqlExpr = f"ANALYZE TABLE {inputTableName} COMPUTE STATISTICS FOR COLUMNS {colExpr};"
              
            return sqlExpr

          else:

              
              return sqlExpr
            
        except:
          ## just return default if there are any problems (i.e. table size is not valid number - usually because of non delta tables)
          return sqlExpr

        
        
    @staticmethod
    @F.udf("string")   
    def getAlterTableCommand(inputTableName, fileSizeMapInMb, isMergeUsed):

      defaultAlterExpr = "ALTER TABLE {inputTableName} SET TBLPROPERTIES ('delta.tuneFileSizesForRewrites' = 'false');"
      try:
        
        if float(isMergeUsed) >=1:

            alterExpr = f"ALTER TABLE {inputTableName} SET TBLPROPERTIES ('delta.targetFileSize' = '{fileSizeMapInMb}', 'delta.tuneFileSizesForRewrites' = 'true');"
            return alterExpr
        else: 
            alterExpr = f"ALTER TABLE {inputTableName} SET TBLPROPERTIES ('delta.targetFileSize' = '{fileSizeMapInMb}', 'delta.tuneFileSizesForRewrites' = 'false');"
            return alterExpr
          
      except:
        return defaultAlterExpr
        
    
    
    @staticmethod
    @F.udf("array<string>")
    def get_column_ordering_statements(table_name, zorder_cols):

      alter_list = []

      for c in zorder_cols:
        ac_element = str(f"""ALTER TABLE {table_name} ALTER COLUMN {c} FIRST;""")
        alter_list.append(ac_element)

      return alter_list
      
          
    
    def build_optimization_strategy(self, optimize_method="both"):
        
        optimize_method = optimize_method.lower()
        
        print("Building Optimization Plan for Monitored Tables...")
        
        try:
            self.spark.sql(f"""INSERT INTO {self.database_name}.final_ranked_cols_by_table
         
              WITH most_recent_table_stats AS (
                        SELECT CONCAT(COALESCE(REVERSE(SPLIT(s1.TableName, '[.]'))[2], 'hive_metastore'), 
                    '.', COALESCE(REVERSE(SPLIT(s1.TableName, '[.]'))[1], 'default'), 
                    '.', REVERSE(SPLIT(s1.TableName, '[.]'))[0]) AS FullTableName,
                        s1.*
                        FROM {self.database_name}.all_tables_table_stats s1
                        WHERE UpdateTimestamp = (SELECT MAX(UpdateTimestamp) FROM {self.database_name}.all_tables_table_stats s2 WHERE s1.TableName = s2.TableName)
                        ),
         final_stats AS (    
         SELECT DISTINCT
            spine.FullTableName AS TableName,
            spine.ColumnName,
            spine.SampleSize,
            spine.TotalCountInSample,
            spine.DistinctCountOfColumnInSample,
            spine.CardinalityProportion,
            spine.CardinalityProportionScaled,
            spine.IsUsedInReads,
            spine.IsUsedInWrites,
            spine.UpdateTimestamp,
            CASE WHEN array_contains(tls.partitionColumns, spine.ColumnName) THEN 1 ELSE 0 END AS IsPartitionCol,
            QueryReferenceCountScaled,
            RawTotalRuntimeScaled,
            AvgQueryDurationScaled,
            TotalColumnOccurrencesForAllQueriesScaled,
            AvgColumnOccurrencesInQueriesScaled,
            isUsedInJoin,
            isUsedInFilter,
            isUsedInGroup
            FROM (SELECT CONCAT(COALESCE(REVERSE(SPLIT(sub_card.TableName, '[.]'))[2], 'hive_metastore'), 
                            '.', COALESCE(REVERSE(SPLIT(sub_card.TableName, '[.]'))[1], 'default'), 
                            '.', REVERSE(SPLIT(sub_card.TableName, '[.]'))[0]) AS FullTableName,
                              *
                             FROM {self.database_name}.all_tables_cardinality_stats AS sub_card) AS spine
            LEFT JOIN most_recent_table_stats AS tls ON tls.FullTableName = spine.FullTableName /* !! not idempotent, always choose most recent table stats !! */
            -- These are re-created every run
            LEFT JOIN (SELECT 
                    CONCAT(COALESCE(REVERSE(SPLIT(sub_reads.TableName, '[.]'))[2], 'hive_metastore'), 
                            '.', COALESCE(REVERSE(SPLIT(sub_reads.TableName, '[.]'))[1], 'default'), 
                            '.', REVERSE(SPLIT(sub_reads.TableName, '[.]'))[0]) AS FullTableName,
                            *
                              FROM {self.database_name}.read_statistics_scaled_results AS sub_reads) AS reads ON spine.FullTableName = reads.FullTableName 
                                                                                            AND reads.ColumnName = spine.ColumnName),
                                                                                            
            raw_scoring AS (
            -- THIS IS THE CORE SCORING EQUATION
            SELECT 
            *,
             CASE WHEN IsPartitionCol = 1 THEN 0 
             ELSE 
                 CASE 
                 WHEN '{optimize_method}' = 'both'
                       THEN IsUsedInReads*(1 + COALESCE(QueryReferenceCountScaled,0) + COALESCE(RawTotalRuntimeScaled,0) + COALESCE(AvgQueryDurationScaled, 0) + COALESCE(TotalColumnOccurrencesForAllQueriesScaled, 0) + COALESCE(isUsedInFilter,0) + COALESCE(isUsedInJoin,0) + COALESCE(isUsedInGroup, 0))*(0.001+ COALESCE(CardinalityProportionScaled,0)) + (IsUsedInWrites*(0.001+COALESCE(CardinalityProportionScaled, 0)))
                 WHEN '{optimize_method}' = 'read'
                       THEN IsUsedInReads*(1 + COALESCE(QueryReferenceCountScaled,0) + COALESCE(RawTotalRuntimeScaled,0) + COALESCE(AvgQueryDurationScaled, 0) + COALESCE(TotalColumnOccurrencesForAllQueriesScaled, 0) + COALESCE(isUsedInFilter,0) + COALESCE(isUsedInJoin,0) + COALESCE(isUsedInGroup, 0))*(0.001+ COALESCE(CardinalityProportionScaled,0)) /* If Read, do not add merge predicate to score */
                WHEN '{optimize_method}' = 'write'
                        THEN IsUsedInReads*(1 + COALESCE(QueryReferenceCountScaled,0) + COALESCE(RawTotalRuntimeScaled,0) + COALESCE(AvgQueryDurationScaled, 0) + COALESCE(TotalColumnOccurrencesForAllQueriesScaled, 0) + COALESCE(isUsedInFilter,0) + COALESCE(isUsedInJoin,0) + COALESCE(isUsedInGroup, 0))*(0.001+ COALESCE(CardinalityProportionScaled,0)) + (5*IsUsedInWrites*(0.001+COALESCE(CardinalityProportionScaled, 0))) /* heavily weight the column such that it is always included */
                END
            END AS RawScore
            FROM final_stats
            WHERE IsPartitionCol = 0
            ),
            ranked_scores AS (
            SELECT 
            *,
            ROW_NUMBER() OVER( PARTITION BY TableName ORDER BY RawScore DESC) AS ColumnRank
            FROM raw_scoring
            )

            SELECT DISTINCT
            *,
            current_timestamp() AS RankUpdateTimestamp /* not going to replace results each time, let optimizer choose most recent version and look at how it changes */
            FROM ranked_scores
            WHERE (ColumnRank <= 2::integer AND (IsUsedInReads + IsUsedInWrites) >= 1 AND DistinctCountOfColumnInSample >= 1000) /* 2 or less but must be high enough cardinality */
            OR (CardinalityProportion >= 0.2 AND RawScore IS NOT NULL AND DistinctCountOfColumnInSample >= 1000) -- filter out max ZORDER cols, we will then collect list into OPTIMIZE string to run
            OR (CardinalityProportion >= 0.01 AND TotalCountInSample >= 1000000 AND RawScore IS NOT NULL AND (IsUsedInReads + IsUsedInWrites) >= 1)
            ORDER BY TableName, ColumnRank
    
            """)
            
            ##
            
            print(f"Completed Optimization Strategy Rankings! Results can be found here... \n SELECT * FROM {self.database_name}.final_ranked_cols_by_table")

            print(f"Now building final config file...")

            final_df = (self.spark.sql(f"""
                         WITH most_recent_table_stats AS (
                                  SELECT DISTINCT s1.*
                                  FROM {self.database_name}.all_tables_table_stats s1
                                  WHERE UpdateTimestamp = (SELECT MAX(UpdateTimestamp) FROM {self.database_name}.all_tables_table_stats s2 WHERE s1.TableName = s2.TableName)
                                  ),

                        final_results AS (
                                      SELECT DISTINCT spine.TableName, s1.ColumnName
                                      FROM most_recent_table_stats spine
                                      LEFT JOIN {self.database_name}.final_ranked_cols_by_table s1 ON spine.TableName = s1.TableName AND RankUpdateTimestamp = (SELECT MAX(RankUpdateTimestamp) FROM {self.database_name}.final_ranked_cols_by_table s2)

                                      ),
                                        tt AS 
                                                (
                                                    SELECT 
                                                    TableName, collect_list(DISTINCT ColumnName) AS ZorderCols
                                                    FROM final_results
                                                    GROUP BY TableName
                                                )
                                                SELECT 
                                                *,
                                                CASE WHEN size(ZorderCols) >=1 
                                                        THEN concat("OPTIMIZE ", TableName, " ZORDER BY (", concat_ws(", ",ZorderCols), ");")
                                                    ELSE concat("OPTIMIZE ", TableName, ";")
                                                    END AS OptimizeCommandString,
                                                NULL AS AlterTableCommandString,
                                                NULL AS AnalyzeTableCommandString,
                                                current_timestamp() AS UpdateTimestamp
                                                FROM tt
                        """)
             )

            ### Save as single partition so collect is simple cause this should just be a config table
            ### This can have multiple recs per
            final_df.repartition(1).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{self.database_name}.final_optimize_config")

            #### Build Analyze Table String

            col_order_df = (self.spark.sql(f"""WITH final_results AS (
                                      SELECT s1.*
                                      FROM {self.database_name}.final_optimize_config s1
                                      WHERE UpdateTimestamp = (SELECT MAX(UpdateTimestamp) FROM {self.database_name}.final_optimize_config s2 WHERE s1.TableName = s2.TableName)
                                      
                                      )
                                      SELECT * FROM final_results""")
                            .persist()
                            .withColumn("ColumnOrderingCommandString", self.get_column_ordering_statements(F.col("TableName"), F.col("ZorderCols")))
                            .persist()
                           )
            
            col_order_df.createOrReplaceTempView("col_order")
            
            self.spark.sql(f"""MERGE INTO {self.database_name}.final_optimize_config AS target
                            USING col_order AS source
                            ON source.TableName = target.TableName
                            WHEN MATCHED THEN 
                            UPDATE SET 
                            target.ColumnOrderingCommandString = source.ColumnOrderingCommandString
                            
                      """)
                            
            ## Build an ANALYZE TABLE command with the following heuristics: 

            ## 1. IF: table less than 100GB Run COMPUTE STATISTICS FOR ALL COLUMNS
            ## 2. ELSE IF: table great than 100GB, run COMPUTE STATISTICS FOR COLUMNS used in GROUP, FILTER, OR JOINS ONLY

            analyze_stats_df = (self.spark.sql(f"""
                WITH most_recent_table_stats AS (
                        SELECT CONCAT(COALESCE(REVERSE(SPLIT(s1.TableName, '[.]'))[2], 'hive_metastore'), 
                    '.', COALESCE(REVERSE(SPLIT(s1.TableName, '[.]'))[1], 'default'), 
                    '.', REVERSE(SPLIT(s1.TableName, '[.]'))[0]) AS FullTableName,
                        s1.*
                        FROM {self.database_name}.all_tables_table_stats s1
                        WHERE UpdateTimestamp = (SELECT MAX(UpdateTimestamp) FROM {self.database_name}.all_tables_table_stats s2 WHERE s1.TableName = s2.TableName)
                        ),
               stats_cols AS (
                    SELECT DISTINCT
                    spine.FullTableName AS TableName,
                    card_stats.ColumnName
                    FROM most_recent_table_stats spine
                    LEFT JOIN (
                            SELECT CONCAT(COALESCE(REVERSE(SPLIT(sub_card.TableName, '[.]'))[2], 'hive_metastore'), 
                            '.', COALESCE(REVERSE(SPLIT(sub_card.TableName, '[.]'))[1], 'default'), 
                            '.', REVERSE(SPLIT(sub_card.TableName, '[.]'))[0]) AS FullTableName,
                              *
                             FROM {self.database_name}.all_tables_cardinality_stats AS sub_card
                    
                            ) AS card_stats ON card_stats.FullTableName = spine.FullTableName
                    LEFT JOIN (SELECT 
                    CONCAT(COALESCE(REVERSE(SPLIT(sub_reads.TableName, '[.]'))[2], 'hive_metastore'), 
                            '.', COALESCE(REVERSE(SPLIT(sub_reads.TableName, '[.]'))[1], 'default'), 
                            '.', REVERSE(SPLIT(sub_reads.TableName, '[.]'))[0]) AS FullTableName,
                            *
                              FROM {self.database_name}.read_statistics_scaled_results AS sub_reads) AS reads ON card_stats.FullTableName = reads.FullTableName 
                                                                                            AND reads.ColumnName = card_stats.ColumnName
                    WHERE card_stats.IsUsedInWrites = 1
                        OR (reads.isUsedInJoin + reads.isUsedInFilter + reads.isUsedInGroup ) >= 1
                    )
                    SELECT 
                    spine.TableName AS TableName, -- This is the FullTableName under the hood
                    MAX(spine.sizeInGB) AS sizeInGB, -- We already chose most recnt file stats of each table
                    MAX(spine.mappedFileSizeInMb) AS fileSizeMap,
                    CASE WHEN MAX(IsUsedInWrites)::integer >= 1 THEN 1 ELSE 0 END AS ColumnsUsedInMerges, 
                    -- If table has ANY columns used in a merge predicate, tune file sizes for re-writes
                    concat_ws(',', array_distinct(collect_list(reads.ColumnName)))  AS ColumnsToCollectStatsOn
                    FROM most_recent_table_stats spine
                    LEFT JOIN (SELECT 
                            CONCAT(COALESCE(REVERSE(SPLIT(sub_card.TableName, '[.]'))[2], 'hive_metastore'), 
                            '.', COALESCE(REVERSE(SPLIT(sub_card.TableName, '[.]'))[1], 'default'), 
                            '.', REVERSE(SPLIT(sub_card.TableName, '[.]'))[0]) AS FullTableName,
                              *
                              FROM {self.database_name}.all_tables_cardinality_stats AS sub_card
                              ) AS card_stats ON card_stats.FullTableName = spine.TableName
                    LEFT JOIN stats_cols AS reads ON card_stats.FullTableName = reads.TableName AND reads.ColumnName = card_stats.ColumnName
                    GROUP BY spine.TableName
                    """)
                               )

            analyze_stats_completed = (analyze_stats_df
                                       .withColumn("AlterTableCommandString", self.getAlterTableCommand(F.col("TableName"), F.col("fileSizeMap"), F.col("ColumnsUsedInMerges")))
                                       .withColumn("AnalyzeTableCommandString", self.getAnalyzeTableCommand(F.col("TableName"), F.col("sizeInGB"), F.col("ColumnsToCollectStatsOn")))
                                      )

            analyze_stats_completed.createOrReplaceTempView("analyze_stats")

            self.spark.sql(f"""MERGE INTO {self.database_name}.final_optimize_config AS target
                            USING analyze_stats AS source
                            ON source.TableName = target.TableName
                            WHEN MATCHED THEN 
                            UPDATE SET 
                            target.AlterTableCommandString = source.AlterTableCommandString,
                            target.AnalyzeTableCommandString = source.AnalyzeTableCommandString
                      """)
        
            
            print(f"Table analysis complete!! Get results (dataFrame) by calling deltaOptimzer.get_results()!!")
            
            self.spark.sql(f"""CREATE OR REPLACE VIEW {self.database_name}.optimizer_results AS 
            WITH final_results AS (
            SELECT s1.*
            FROM {self.database_name}.final_optimize_config s1
            WHERE UpdateTimestamp = (SELECT MAX(UpdateTimestamp) FROM {self.database_name}.final_optimize_config s2 WHERE s1.TableName = s2.TableName)
            )
            SELECT * FROM final_results;
            """)
            
            self.spark.sql(f"""CREATE OR REPLACE VIEW {self.database_name}.optimize_recent_rankings AS 
            WITH final_results AS (
            SELECT s1.*
            FROM {self.database_name}.final_ranked_cols_by_table s1
            WHERE RankUpdateTimestamp = (SELECT MAX(RankUpdateTimestamp) FROM {self.database_name}.final_ranked_cols_by_table s2 WHERE s1.TableName = s2.TableName)
            )
            SELECT * FROM final_results;
            """)
            
        except Exception as e:
            raise(e)
        
    
    def get_results(self):
        
        results_df =  (self.spark.sql(f""" 
         WITH final_results AS (
            SELECT s1.*
            FROM {self.database_name}.final_optimize_config s1
            WHERE UpdateTimestamp = (SELECT MAX(UpdateTimestamp) FROM {self.database_name}.final_optimize_config s2)
            )
        SELECT * FROM final_results;
        """))
        
        return results_df
    
    
    def get_rankings(self):
        
        rankings_df = (self.spark.sql(f"""
        WITH final_results AS (
        SELECT s1.*
        FROM {self.database_name}.final_ranked_cols_by_table s1
        WHERE RankUpdateTimestamp = (SELECT MAX(RankUpdateTimestamp) FROM {self.database_name}.final_ranked_cols_by_table s2 WHERE s1.TableName = s2.TableName)
        )
        SELECT * FROM final_results;
        """)
                   )

        return rankings_df

###############################################################
