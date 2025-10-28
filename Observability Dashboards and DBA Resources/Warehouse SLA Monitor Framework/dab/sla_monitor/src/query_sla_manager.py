# Databricks notebook source

from pyspark.sql import SparkSession
from datetime import time, date, timedelta, timezone, datetime
from typing import List
from dataclasses import dataclass, field
from typing import ClassVar
import json
import requests
import hashlib
import pandas as pd
import time

# COMMAND ----------

class QueryHistoryAlertManager():

    @dataclass
    class WarehousePolicy:
        warehouse_ids: List[str]
        sla_seconds: int = 120
        policy_mode: str = "ALL"
        included_statuses: List[str] = field(default_factory=lambda: ["QUEUED", "RUNNING", "FAILED", "FINISHED"])
        
        _allowed_policy_modes: ClassVar[List[str]] = ["ALL", "SLA_BURST", "ERROR"]
        policy_hash: str = field(init=False, default="")

        def __post_init__(self):
            if not isinstance(self.warehouse_ids, list) or not all(isinstance(warehouse_id, str) for warehouse_id in self.warehouse_ids):
                raise TypeError(f"warehouse_ids must be a list of strings")
            if not isinstance(self.sla_seconds, int):
                raise TypeError(f"sla_seconds must be an int, got {type(self.sla_seconds).__name__}")
            if self.policy_mode not in self._allowed_policy_modes:
                raise ValueError(f"policy_mode must be one of {self._allowed_policy_modes}, got {self.policy_mode}")

            self.policy_hash = self.compute_hash()

        def compute_hash(self) -> str:
            hasher = hashlib.sha256()
            hasher.update(",".join(self.warehouse_ids).encode('utf-8'))
            hasher.update(str(self.sla_seconds).encode('utf-8'))
            hasher.update(self.policy_mode.encode('utf-8'))
            return hasher.hexdigest()

    
    def __init__(self, host: str,
                 dbx_token:  str, 

                 #### These are all base level defaults, they can be overridden later
                 database_name="main.query_alert_manager", 
                 database_location=None,
                 polling_frequency_seconds=60,
                 alert_destinations=[],
                 query_history_partition_cols:[str] = None,
                 parallelism: int = None,
                 optimize_every_n_batches = 20,
                 hard_fail_after_n_attempts = 10,
                 cold_start_lookback_period_seconds = 600):
        
        ## Assumes running on a spark environment

        self.workspace_url = host
        self.internal_tables_to_manage = ["query_history_log", "query_alerts", "query_sla_policies", "query_history_statistics"]
        self.database_name = database_name
        self.dbx_token = dbx_token
        self.database_location = database_location
        self.spark = SparkSession.getActiveSession()
        self.parallelism = parallelism if not None else self.spark.sparkContext.defaultParallelism*2
        self.query_history_partition_cols = query_history_partition_cols


        #### State Batch managements
        self.optimize_every_n_batches = optimize_every_n_batches ## this variable defines how many times to optimize the tables while polling
        self.hard_fail_after_n_attempts = hard_fail_after_n_attempts ## this variable allows user to specify how many errors to accumulate before exiting the loop
        self.cold_start_lookback_period_seconds = cold_start_lookback_period_seconds 
        self.active_batch_cycle_number = 0
        self.num_cumulative_errors = 0
        self.cumulative_cycles = 0
        self.activate_batch_start_time = None
        self.active_batch_end_time = None
        self.polling_frequency_seconds_default = polling_frequency_seconds


        ####
        
        if self.parallelism:
          self.spark.conf.set("spark.sql.shuffle.partitions", self.shuffle_partitions)

        
        if self.database_location:
          print(f"Initializing Query History Alert Manager at database: {self.database_name} \n with location: {self.database_location}")
        else:
          print(f"Initializing Query History Alert Manager at database: {self.database_name} \n with location: MANAGED")
        
        ## Create Database
        self.initialize_database()
  
        return


    ######### Database Management Operations ###########
    ### Create database if not exists
    def initialize_database(self):
      try: 

        if self.database_location is not None:
          self.spark.sql(f"""CREATE DATABASE IF NOT EXISTS {self.database_name} LOCATION '{self.database_location}';""")
        else:
          self.spark.sql(f"""CREATE DATABASE IF NOT EXISTS {self.database_name};""")
        
      except Exception as e:
        print(f"""Creating the database at location: {self.database_name} did not work...""")
        raise(e)
      

      try: 
        ## Query Profiler Tables
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.query_history_log 
                          (Id BIGINT GENERATED ALWAYS AS IDENTITY,
                          policy_id BIGINT,
                          policy_hash STRING,
                          warehouse_ids ARRAY<STRING>,
                          workspace_id STRING,
                          start_timestamp TIMESTAMP,
                          end_timestamp TIMESTAMP)
                          USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported',
                        'delta.columnMapping.mode' = 'name', 
                        'delta.enableDeletionVectors' = true)
                            CLUSTER BY (policy_id, end_timestamp)
                          """)
        
        ### Policy Stoage table to allow storing of policies 
        #### SLA_BURST - Policy Mode that triggers alerts if ANY query, reguardless of status or success, goes above the SLA policy
        #### ERROR - Policy Mode that triggers an alert for any query that errors
        #### ALL - Policy Mode that triggers an alert for any query that is ABOVE and SLA OR errors during any point
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.query_sla_policies
                          (policy_id BIGINT GENERATED ALWAYS AS IDENTITY,
                          policy_hash STRING, -- Unique Hash
                          warehouse_ids ARRAY<STRING>,
                          sla_seconds INTEGER, -- SLA limit for generating SLA policy alerts by warehouse
                          query_statuses ARRAY<STRING>,
                          policy_mode STRING -- Policy type the defines ways in which a query can trigger an alert. Options are: SLA_BURST, ERROR, ALL
                          )
                          USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported',
                        'delta.columnMapping.mode' = 'name', 
                        'delta.enableDeletionVectors' = true)
                          CLUSTER BY (policy_hash)
                          """)
        

        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.query_alerts
                          (Id BIGINT GENERATED ALWAYS AS IDENTITY,
                          query_alert_id STRING, 
                          alert_type STRING,
                          alert_message STRING, -- JSON blob of alerts
                          start_timestamp TIMESTAMP,
                          end_timestamp TIMESTAMP)
                          USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported',
                        'delta.columnMapping.mode' = 'name', 
                        'delta.enableDeletionVectors' = true)
                          CLUSTER BY (end_timestamp)
                          """)

        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.query_history_statistics
                        (Id BIGINT GENERATED ALWAYS AS IDENTITY,
                          query_id STRING,
                          query_hash STRING,
                          -- Policy SLA Info
                          policy_poll_timestamp TIMESTAMP,
                          policy_duration_seconds FLOAT,
                          policy_id BIGINT,
                          policy_mode STRING, 
                          policy_sla_fail_reason STRING,
                          query_start_time TIMESTAMP ,
                          query_end_time TIMESTAMP,
                          duration FLOAT ,
                          status STRING,
                          statement_type STRING,
                          error_message STRING,
                          executed_as_user_id FLOAT,
                          executed_as_user_name STRING,
                          rows_produced FLOAT,
                          metrics STRING,
                          endpoint_id STRING,
                          channel_used STRING,
                          lookup_key STRING,
                          update_timestamp TIMESTAMP,
                          update_date DATE,
                          query_text STRING)
                        USING delta TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported',
                        'delta.columnMapping.mode' = 'name', 
                        'delta.enableDeletionVectors' = true)
                        CLUSTER BY (query_start_time, update_timestamp) -- update_timestamp
              """)

      except Exception as e:
        print(f"""Creating the tables at location: {self.database_name} did not work...""")
        raise(e)
      
      return
    

    def drop_database_if_exists(self):
      try: 
        self.spark.sql(f"""DROP DATABASE IF EXISTS {self.database_name} CASCADE""")
      except Exception as e:
        print(f"Failed to drop database with error: {str(e)}")

      return
    
    def optimize_database(self):
      tbls_under_management = self.internal_tables_to_manage

      for tbl in tbls_under_management:
        print(f"Optimizing table: {tbl}")
        self.spark.sql(f""" OPTIMIZE {self.database_name}.{tbl};""")
        self.spark.sql(f""" VACUUM {self.database_name}.{tbl};""")

      print(f"Database Optimized!")
      return
    
    ######### END Database Management Operations ###########


    ## Convert timestamp to milliseconds for API
    @staticmethod
    def ms_timestamp(dt):
        return int(round(dt.replace(tzinfo=timezone.utc).timestamp() * 1000, 0))
    
    
    
    ## Get Start and End range for Query History API
    def get_time_series_lookback(self, lookback_period):
        
        ## Gets time series from current timestamp to now - lookback period - if overrride
        end_timestamp = datetime.now()
        start_timestamp = end_timestamp - timedelta(seconds = lookback_period)
        ## Convert to ms
        start_ts_ms = self.ms_timestamp(start_timestamp)
        end_ts_ms = self.ms_timestamp(end_timestamp)
        print(f"Getting Query History to parse from period: {start_timestamp} to {end_timestamp}")
        return start_ts_ms, end_ts_ms
 

    ## If no direct time ranges provided (like after a first load, just continue where the log left off)
    def get_most_recent_history_from_log(self, mode='auto', lookback_period=600, policy_id=None, policy_hash=None):
      
        ## This function gets the most recent end timestamp of the query history range, and returns new range from then to current timestamp
        start_timestamp = None

        if policy_id:
          start_timestamp = self.spark.sql(f"""SELECT MAX(end_timestamp) FROM {self.database_name}.query_history_log WHERE policy_id = {policy_id}""").collect()[0][0]

        elif policy_hash:
          start_timestamp = self.spark.sql(f"""SELECT MAX(end_timestamp) FROM {self.database_name}.query_history_log WHERE policy_hash = '{policy_hash}'""").collect()[0][0]
        else:
          start_timestamp = self.spark.sql(f"""SELECT MAX(end_timestamp) FROM {self.database_name}.query_history_log""").collect()[0][0]

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
    def insert_query_history_delta_log(self, start_ts_ms, end_ts_ms, warehouse_ids, policy_id=None, policy_hash=None):

      warehouses = ",".join(warehouse_ids)

      ## Upon success of a query history pull, this function logs the start_ts and end_ts that it was pulled into the logs

      try: 
        if policy_id is None:
          policy_id = "NULL"
        if policy_hash is None:
          policy_hash = "NULL"

        self.spark.sql(f"""INSERT INTO {self.database_name}.query_history_log (policy_id, policy_hash, warehouse_ids, workspace_id, start_timestamp, end_timestamp)
                            VALUES({policy_id}, {policy_hash}, split('{warehouses}', ','), 
                            '{self.workspace_url}', ('{start_ts_ms}'::double/1000)::timestamp, 
                            ('{end_ts_ms}'::double/1000)::timestamp)
        """)

      except Exception as e:
          raise(e)


    def add_sla_policy(self, warehouse_ids: List[str], sla_seconds: int, policy_mode:str = "SLA_BURST", included_statuses: [str] =["QUEUED","RUNNING","FAILED","FINISHED"]):

      new_policy = self.WarehousePolicy(warehouse_ids = warehouse_ids,
                                         sla_seconds = sla_seconds, 
                                         policy_mode = policy_mode, 
                                         included_statuses = included_statuses)

      ##### Insert if not exists - UPSERT - then return policy Id

      self.spark.sql(f"""WITH new_policy AS (
                       SELECT 
                       '{new_policy.policy_hash}'::string AS policy_hash,
                       split('{",".join(new_policy.warehouse_ids)}', ",")::array<string> AS warehouse_ids,
                       '{new_policy.sla_seconds}'::integer AS sla_seconds,
                       split('{",".join(new_policy.included_statuses)}', ",")::array<string> AS query_statuses,
                       '{new_policy.policy_mode}'::string AS policy_mode
                     )
                     MERGE INTO {self.database_name}.query_sla_policies AS t
                     USING new_policy AS s ON t.policy_hash = s.policy_hash
                     -- No need to update a hashed unique policy - just insert a new one or delete one
                     WHEN NOT MATCHED THEN INSERT  (policy_hash, warehouse_ids, sla_seconds, query_statuses, policy_mode)
                     VALUES (s.policy_hash, s.warehouse_ids, s.sla_seconds, s.query_statuses, s.policy_mode)
                     """)
      
      policy_id = self.spark.sql(f""" SELECT MAX(policy_id) AS id FROM {self.database_name}.query_sla_policies WHERE policy_hash = '{new_policy.policy_hash}'""").first()[0]

      ###
      print(f"Added Policy with Id: {policy_id}")

      return

    def remove_sla_policy(self, policy_id = None, policy_hash = None):

      ##### Insert if not exists - UPSERT - then return policy Id

      if policy_id:
        self.spark.sql(f"""
                       WITH delete_policy AS (
                       SELECT
                       policy_id
                       FROM {self.database_name}.query_sla_policies
                       WHERE policy_id = {policy_id}::bigint
                     )
                      MERGE INTO {self.database_name}.query_sla_policies AS t
                      USING delete_policy AS s ON t.policy_id = s.policy_id
                      WHEN MATCHED THEN DELETE
                      """)
      elif policy_hash:

        self.spark.sql(f"""
                      WITH delete_policy AS (
                       SELECT
                       policy_hash
                       FROM {self.database_name}.query_sla_policies
                       WHERE policy_hash = '{policy_hash}'::string
                     )
                      MERGE INTO {self.database_name}.query_sla_policies AS t
                      USING new_policy AS s ON t.policy_hash = s.policy_hash
                      WHEN MATCHED THEN DELETE
                      """)
        
      else:
        print(f"No policy id or hash selected. Skipping Delete")
      return    
    

    def get_query_history_request(self, warehouse_ids: List[str], start_ts_ms, end_ts_ms, included_statuses: List[str]):
            ## Put together request
            
            request_string = {
                "filter_by": {
                  "query_start_time_range": {
                  "end_time_ms": end_ts_ms,
                  "start_time_ms": start_ts_ms
                },
                "statuses": included_statuses,
                "warehouse_ids": warehouse_ids
                },
                "include_metrics": "true",
                "max_results": "1000"
            }

            ## Convert dict to json
            v = json.dumps(request_string)
            
            uri = f"https://{self.workspace_url}/api/2.0/sql/history/queries"
            headers_auth = {"Authorization":f"Bearer {self.dbx_token}"}
            
            #### Get Query History Results from API
            endp_resp = requests.get(uri, data=v, headers=headers_auth).json()


            #### Page through data
            initial_resp = endp_resp.get("res")

            #print(initial_resp)
            
            if initial_resp is None:
                print(f"DBSQL Has no queries on the warehouses {', '.join(warehouse_ids)} for these times:{start_ts_ms} - {end_ts_ms}")
                initial_resp = []
                ## Continue anyways cause there could be old queries and we want to still compute aggregates
            
            
            next_page = endp_resp.get("next_page_token")
            has_next_page = endp_resp.get("has_next_page")

            #print(has_next_page)

            ## Page through results   
            page_responses = []

            while has_next_page is True: 

                #print(f"Getting results for next page... {next_page}")

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


            all_responses = [x for xs in page_responses for x in xs] + initial_resp
            return all_responses
            

    ### Returns WarehousePolicy Object
    def get_sla_policy(self, policy_id = None, policy_hash = None):
      active_policy = None

      if policy_id:
        active_policy_raw = self.spark.sql(f""" SELECT * FROM {self.database_name}.query_sla_policies WHERE policy_id = '{policy_id}'""").first()
        
        print(active_policy_raw)
        
        policy_hash = active_policy_raw["policy_hash"]
        warehouse_ids = active_policy_raw["warehouse_ids"]
        sla_seconds = active_policy_raw["sla_seconds"]
        policy_mode = active_policy_raw["policy_mode"]
        query_statuses = active_policy_raw["query_statuses"]

        active_policy = self.WarehousePolicy(warehouse_ids = warehouse_ids,
                                             sla_seconds = sla_seconds,
                                             policy_mode = policy_mode,
                                             included_statuses = query_statuses)

        return active_policy

      elif policy_hash:
        active_policy_raw = self.spark.sql(f""" SELECT * FROM {self.database_name}.query_sla_policies WHERE policy_hash = '{policy_hash}'""").first()

      else: 
        print("No policy id or hash provided. Skipping")
        return


      policy_hash = active_policy_raw["policy_hash"]
      warehouse_ids = active_policy_raw["warehouse_ids"]
      sla_seconds = active_policy_raw["sla_seconds"]
      policy_mode = active_policy_raw["policy_mode"]
      query_statuses = active_policy_raw["query_statuses"]

      active_policy = self.WarehousePolicy(warehouse_ids = warehouse_ids,
                                            sla_seconds = sla_seconds,
                                            policy_mode = policy_mode,
                                            included_statuses = query_statuses)

      return active_policy
    

    def get_all_policies_df(self):
      return self.spark.table(f"{self.database_name}.query_sla_policies")
    

    #### Main Loop that polls query history with a loaded policy, separates normal queries from alert queries, saves alerts to db
    def poll_with_policy(self, 
                         policy_id = None, 
                         policy_hash = None,
                         start_over = False,
                         mode: str ='auto', 
                         cold_start_lookback_period_seconds: int = 600, 
                         query_stats_retention_mode = "ALERTS", ## ALL, ALERTS
                         polling_frequency_seconds = 60, 
                         optimize_every_n_batches = None, 
                         hard_fail_after_n_attempts = None):
      
      if start_over:
        self.drop_database_if_exists()
        self.initialize_database()
        self.optimize_database()
      
      ##### This whole poller needs to be in a loop waiting N seconds before running again.
      
      workspace_url = self.workspace_url
      lookback_period = int(cold_start_lookback_period_seconds)
      ### Policy Filtering Info
      active_policy = self.get_sla_policy(policy_id = policy_id, policy_hash = policy_hash)

      warehouse_ids_list = active_policy.warehouse_ids
      included_statuses = active_policy.included_statuses
      sla_in_seconds = active_policy.sla_seconds      
      policy_mode = active_policy.policy_mode
      active_policy_id = policy_id

      ### Poller Settings
      if polling_frequency_seconds:
        polling_frequency_seconds = polling_frequency_seconds
      else:
        polling_frequency_seconds = self.polling_frequency_seconds_default

      if hard_fail_after_n_attempts:
        self.hard_fail_after_n_attempts = hard_fail_after_n_attempts

      if optimize_every_n_batches:
        self.optimize_every_n_batches = optimize_every_n_batches

      print(f"""Loading Query Profile to delta from workspace: {workspace_url} \n 
            from Warehouse Ids: {warehouse_ids_list} \n for the last {lookback_period} seconds...""")
      
      print(f"Monitoring Query History with Policy id => {active_policy_id}: {active_policy.policy_hash} \n every {polling_frequency_seconds} seconds.")
      
      ## Modes are 'auto' and 'manual' - auto, means it manages its own state, manual means you override the time frame to analyze no matter the history

      #### Main Poller Loop

      while True:

        ### MAIN Loop with Try attemp ceiling
        print(f"Running Batch: {self.cumulative_cycles}")
        print(f"...within sub optimization cycle batch: {self.active_batch_cycle_number}")

        try:

          ## Get time series range based on 
          ## If override = True, then choose lookback period in days
          start_ts_ms, end_ts_ms = self.get_most_recent_history_from_log(mode, lookback_period, policy_id = active_policy_id, policy_hash = policy_hash)

          ### Tag this time frame as the active batch window
          self.activate_batch_start_time = start_ts_ms
          self.active_batch_end_time = end_ts_ms

          
          ## Build request and get initial response
          all_responses = self.get_query_history_request(warehouse_ids = warehouse_ids_list, start_ts_ms = start_ts_ms, end_ts_ms = end_ts_ms, included_statuses = included_statuses)

          
          ## Start Profiling Process
          try: 
              if (all_responses is None or len(all_responses) == 0):
                print("No Results to insert. Skipping.")
                pass
              
              else:
                ## Get responses and save to Delta 
                raw_queries_df = self.spark.createDataFrame(pd.DataFrame(all_responses))
                raw_queries_df.createOrReplaceTempView("raw")

                #### optional WHERE clause construction - if statuses are limited or mode is Alert Only
                
                
                ### In Included Statuses
                filter_str = """ WHERE 1=1"""

                ### Add filtering to query history to only include failed SLA policy queries
                if query_stats_retention_mode == "ALERTS":

                  #### If 
                  if policy_mode == "SLA_BURST":
                    filter_str += f"""\n AND datediff(SECOND, from_unixtime(query_start_time_ms / 1000), COALESCE(from_unixtime(query_end_time_ms / 1000), current_timestamp())) > {sla_in_seconds} """

                  elif policy_mode == "ERROR":
                    filter_str += f""" \n AND status IN ('FAILED') """

                  elif policy_mode == "ALL":
                    filter_str += f"""\n AND (datediff(SECOND, from_unixtime(query_start_time_ms / 1000), COALESCE(from_unixtime(query_end_time_ms / 1000), current_timestamp())) > {sla_in_seconds} 
                    OR status IN ('FAILED') )"""

                base_query = f"""
                            SELECT
                            query_id,
                            sha(query_text) AS query_hash,
                            current_timestamp() AS policy_poll_timestamp,
                            datediff(SECOND, from_unixtime(query_start_time_ms / 1000), 
                            COALESCE(from_unixtime(query_end_time_ms / 1000), current_timestamp())) AS policy_duration_seconds,
                            {policy_id} AS policy_id,
                            '{policy_mode}' AS policy_mode,
                            CASE WHEN policy_mode = 'ERROR'
                                  THEN CASE WHEN status = 'FAILED' THEN 'OUT_OF_POLICY_ERROR' END
                                WHEN policy_mode = 'SLA_BURST'
                                  THEN CASE WHEN policy_duration_seconds > {sla_in_seconds} THEN 'OUT_OF_POLICY_SLA_BURST' END
                                WHEN policy_mode = 'ALL'
                                  THEN CASE WHEN policy_duration_seconds > {sla_in_seconds} AND status = 'FAILED'
                                          THEN 'OUT_OF_POLICY_SLA_BURST_AND_ERROR'
                                        WHEN  policy_duration_seconds > {sla_in_seconds} 
                                          THEN 'OUT_OF_POLICY_SLA_BURST' 
                                        WHEN status = 'FAILED' THEN 'OUT_OF_POLICY_ERROR'
                                        END
                                ELSE 'IN_POLICY' END AS policy_sla_fail_reason,
                            from_unixtime(query_start_time_ms / 1000) AS query_start_time,
                            from_unixtime(query_end_time_ms / 1000) AS query_end_time,
                            duration,
                            status,
                            statement_type,
                            error_message,
                            executed_as_user_id,
                            executed_as_user_name,
                            rows_produced,
                            metrics::string,
                            endpoint_id,
                            channel_used::string AS channel_used,
                            lookup_key::string,
                            now() AS update_timestamp,
                            now()::date AS update_date,
                            query_text
                            FROM raw
                """


                #print(base_query + filter_str)

                ### Fail Alert Threshold
                ######

                alerts_df = self.spark.sql(base_query + filter_str)
                alerts_df.createOrReplaceTempView("alerts_df")

                #return alerts_df

                print(f"Saving {alerts_df.count()} records to query history table")

                self.spark.sql(f"""INSERT INTO {self.database_name}.query_history_statistics BY NAME SELECT * FROM alerts_df""")
                

              #### Trigger / Save Alerts
              ### Collect list of statement_ids, executed_by_as_user_name, along with SLA reason. List of string / dicts

  
              
              ## If successfully, insert log
              self.insert_query_history_delta_log(start_ts_ms, end_ts_ms, warehouse_ids= warehouse_ids_list, policy_id = active_policy_id, policy_hash = policy_hash)

              ### Iterate active batch number
              self.active_batch_cycle_number += 1
              self.cumulative_cycles += 1

              ### If active_batch_number > N batch per optimize, run optimize and reset batch number
              if self.active_batch_cycle_number > self.optimize_every_n_batches:

                self.optimize_database()
                ## restart cycle
                self.active_batch_cycle_number = 0

              ### Pause to poll every N seconds in batch
              time.sleep(polling_frequency_seconds)
              
              

          except Exception as e:
            print(f"""Polling failed with exception: {e}. \n Current failed attempts is {self.num_cumulative_errors}, will hard fail after {self.hard_fail_after_n_attempts} attempts.""")
            self.num_cumulative_errors += 1

            if self.num_cumulative_errors >= self.hard_fail_after_n_attempts:
              raise(e)
              break
          

        except Exception as e:
          print(f"""Polling failed with exception: {e}. \n Current failed attempts is {self.num_cumulative_errors}, will hard fail after {self.hard_fail_after_n_attempts} attempts.""")
          self.num_cumulative_errors += 1

          if self.num_cumulative_errors >= self.hard_fail_after_n_attempts:
            raise(e)
            break

# COMMAND ----------
