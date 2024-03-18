from pyspark.sql import SparkSession
import json
import re
from datetime import datetime, timedelta
import os
import uuid
from delta.tables import DeltaTable
import IPython

"""
Delta Logger
Author: Cody Austin Davis
Purpose: Easy OOTB logging class to log run information for any databricks notebook or job to a delta table and track any metrics. 


TO DO: 
1. Consider using WAL or commiting ALL changes into a single operations ONLY during the complete_run() or fail_run() stage when the run is "committed". Otherwise, we are updating delta records a lot, which can be less performant. 


"""

class DeltaLogger():

  ## User can pass in the fully qualified table name, use the spark session defaults, or pass in catalog and database overrides to the parameters. Pick one. 
  def __init__(self, logger_table_name = "delta_logger", 
               logger_location=None, 
               session_process_name = None, 
               session_batch_id = None, ## Very optional, prefer to allow it to be automatically generated
               catalog=None, 
               database=None, 
               partition_columns:[str] = None):

    from pyspark.dbutils import DBUtils

    self.spark = SparkSession.builder.getOrCreate()

    ## Initialize DBUtils
    if self.spark.conf.get("spark.databricks.service.client.enabled") == "true":
      self.dbutils = DBUtils(self.spark)
    else:
      self.dbutils = IPython.get_ipython().user_ns["dbutils"]
      
    ## If session_proces not provided try to get the notebook or spark application id
    if session_process_name is None:

      notebook_path = self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
      self.session_process_name = notebook_path
      self.default_process_name = os.path.basename(notebook_path)

    else:
      self.session_process_name = session_process_name
      self.default_process_name = session_process_name

    self.default_batch_id = str(uuid.uuid4())

    self.logger_location = logger_location
    self.logger_table_name = logger_table_name
    self.logger_partition_cols = ['start_date', 'session_process_name', 'process_name']
    self.logger_zorder_cols = ['start_timestamp', 'process_name' , 'run_id']

    self.session_catalog = catalog
    self.session_database = database
    self.full_table_name = self.logger_table_name

    ## State for the active run
    if session_batch_id is not None:
      self.active_batch_id = session_batch_id
    else:
      self.active_batch_id = self.default_batch_id

    self.active_run_id = None
    self.active_run_status = None
    self.active_run_start_ts = None
    self.active_run_end_ts = None 
    self.active_run_metadata = None
    self.active_process_name = None
    self.active_start_date = None
  
    ## 
    print(f"""INITIALIZING Delta Logger {self.logger_table_name}""")

    ## Add database and schema if provided
    if self.session_database is not None:
      self.full_table_name = self.session_database+ "." + self.full_table_name
    if self.session_catalog is not None:
      self.full_table_name = self.session_catalog + "." + self.full_table_name


    ## If table already exists, we have to just use those as the partition columns for optimizations
    try: 
        detailDF = DeltaTable.forName(self.spark, self.full_table_name).detail()
        current_partitions = detailDF.select("partitionColumns").collect()[0][0]

    ## Table does not yet exists and we can customize partition structure
    except:
      print(f"CREATING LOGGER TABLE {self.full_table_name} because this is the first time it is being used. ")
      self.create_logger(logger_table_name=self.full_table_name, logger_location=self.logger_location, catalog = self.session_catalog, database = self.session_database, partition_cols = partition_columns)

      ## Confirm creation and get partitions
      detailDF = DeltaTable.forName(self.spark, self.full_table_name).detail()
      current_partitions = detailDF.select("partitionColumns").collect()[0][0]

    print(f"LOGGER INTIALIZTED {self.full_table_name} already exists with partitions: {', '.join(current_partitions)}. \n Active session_name: {self.session_process_name} \n Active Batch Id: {self.active_batch_id}")

    return



  ### Resolve processs name between session and manually provided names
  def resolve_process_name(self, process_name=None):
    ## Resolve Process Name
    if process_name is None:
      try:
        process_name = self.active_process_name
      except:
        raise(ValueError("WARNING - NO ACTIVE PROCESS: There is no active process name and none was explicitly provided. Start a run or provide a specific process name."))
    else: 
      process_name = process_name

    return process_name
  

  def resolve_run_id(self, run_id=None):

    ## Resolve Run Id
    if run_id is None:
      try: 
        active_run_id = int(self.active_run_id)
      except:
        raise(ValueError("WARNING - NO ACTIVE RUN: There is no active run id and no run was explicitly provided. Start a run or provide a specific historical run id."))
    else: 
      active_run_id = int(run_id)

    return active_run_id
  
  
  ## This is the "batch_id" representing the parent session grouping if a user wants to run a particular sub-process in a notebook/job
  ## A session_process_name can have multiple sub processes/run_ids
  def resolve_batch_id(self, batch_id=None):
    ## Resolve Run Id
    if batch_id is None:
      try:
        active_batch_id = self.active_batch_id
      except:
        raise(ValueError("WARNING - NO ACTIVE BATCH ID: There is no active batch id and no batch was explicitly provided. Start a run or provide a specific historical batch id."))
    else: 
      active_batch_id = batch_id

    return active_batch_id
  

  ## Resolve full table name
  def resolve_full_table_name(self, full_table_name=None):
    ## Resolve Run Id
    if full_table_name is None:
      try:
        full_table_name = self.full_table_name
      except:
        raise(ValueError("WARNING - NO ACTIVE LOGGER TABLE. Please Initialize a logger at a specific location or manually provide a name. "))
    else: 
      full_table_name = full_table_name

    return full_table_name
  
  
  ## Create a logger instance 
  def create_logger(self,logger_table_name=None,logger_location=None, catalog=None, database=None, partition_cols:[str] = None):

    full_table_name = self.resolve_full_table_name(logger_table_name)

    ## Add database and schema if provided
    if logger_table_name is not None:
      if database is not None:
        full_table_name = database+ "." + full_table_name
      if database is not None:
        full_table_name = catalog + "." + full_table_name


    ddl_sql = f"""CREATE TABLE IF NOT EXISTS {full_table_name} (
      run_id BIGINT GENERATED BY DEFAULT AS IDENTITY,
      batch_id STRING, 
      session_process_name STRING NOT NULL,
      process_name STRING NOT NULL,
      status STRING NOT NULL, -- RUNNING, FAIL, SUCCESS, STALE
      start_timestamp TIMESTAMP NOT NULL,
      end_timestamp TIMESTAMP,
      duration_seconds DECIMAL,
      duration_ms DECIMAL,
      run_metadata STRING, -- String formatted like JSON
      update_timestamp TIMESTAMP,
      update_date DATE GENERATED ALWAYS AS (update_timestamp::date),
      start_date DATE GENERATED ALWAYS AS (start_timestamp::date),
      end_date DATE GENERATED ALWAYS AS (end_timestamp::date)
    )
    USING DELTA 
    """

    partition_cols_default = self.logger_partition_cols

    ## If table already exists, we have to just use those as the partition columns for optimizations
    try: 
        detailDF = DeltaTable.forName(self.spark, full_table_name).detail()
        current_partitions = detailDF.select("partitionColumns").collect()[0][0]

        print(f"This logger table {full_table_name} already exists with partitions: {', '.join(current_partitions)}")
        self.logger_partition_cols = current_partitions

    ## Table does not yet exists and we can customize partition structure
    except:
      if partition_cols is not None:

        ## Allows users to just customize and overwrite partitions completely if they want. 
        full_partition_cols = list(set(partition_cols))
        self.logger_partition_cols = full_partition_cols
        print(f"Logger table is new, creating table PARTITIONED BY: {' ,'.join(self.logger_partition_cols)}")


    partition_cols_str = " ,".join(self.logger_partition_cols)

    location_sql = f" LOCATION '{logger_location}' "
    partition_sql = f" PARTITIONED BY ({partition_cols_str})"

    if logger_location is not None:
      final_sql = ddl_sql + location_sql + partition_sql
    else: 
      final_sql = ddl_sql + partition_sql

    ## Try to initialize
    try: 

      self.spark.sql(final_sql)

      print(f"SUCCESS: Delta Logger Successfully Initialized:\n Table Name: {full_table_name}")
    
    except Exception as e: 
      msg = str(e)
      print(f"\n FAILED: DELTA LOGGER INIT FAILED with below error. Check you table name and scope: Table Name: {full_table_name}. \n  Error: \n {msg}")

      raise(e)
  

  ## Drop a logger instance
  def drop_logger(self, logger_table_name=None, catalog=None, database=None):
    
    full_table_name = self.resolve_full_table_name(logger_table_name)

    ## Add database and schema if provided
    if logger_table_name is not None:
      if database is not None:
        full_table_name = database+ "." + full_table_name
      if database is not None:
        full_table_name = catalog + "." + full_table_name


    try: 
      print(f"DROPPING logger table: {full_table_name}...\n")
      self.spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")

      print(f"SUCCESS: DROPPED LOGGER TABLE \n Table Name: {full_table_name}")
    
    except Exception as e: 
      msg = str(e)
      print(f"\n FAILED: DROP LOGGER FAILED with below error. Check you table name and scope: Table Name: {full_table_name}. \n  Error: \n {msg}")

      raise(e)


  ## Clear a logger table
  def truncate_logger(self, logger_table_name=None, catalog=None, database=None):
    
    full_table_name = self.resolve_full_table_name(logger_table_name)

    ## Add database and schema if provided
    if logger_table_name is not None:
      if database is not None:
        full_table_name = database+ "." + full_table_name
      if database is not None:
        full_table_name = catalog + "." + full_table_name


    try: 
      print(f"TRUNCATING logger table: {full_table_name}...\n")
      self.spark.sql(f"TRUNCATE TABLE {full_table_name}")
      self.spark.sql(f"VACUUM {full_table_name}")

      print(f"SUCCESS: TRUNCATED LOGGER TABLE \n Table Name: {full_table_name}")
    
    except Exception as e: 
      msg = str(e)
      print(f"\n FAILED: TRUNCATE LOGGER FAILED with below error. Check you table name and scope: Table Name: {full_table_name}. \n  Error: \n {msg}")

      raise(e)
    
  ## Optimize logger. Tries to be smart and only optimize active partitions (on end run and start run) but it needs the specific active info for it to work. If there is no active run_id then it will optimize the default partitions for the given session_process_name only. 
  ## A specific logger must be initialized at a specific session
  def get_optimize_sql(self):

    ### Optimization process to ensure the ZORDER columns are NOT part of the partition columns
    z_order_col_str = ", ".join([i for i in self.logger_zorder_cols if i not in self.logger_partition_cols])

    partition_filter_rules = ''

    ## Only filter on partitions where it possibly makes sense (no timestamps or run id columns
    
    for i, c in enumerate(self.logger_partition_cols):
      if (c == 'session_process_name' and self.session_process_name is not None):
        partition_filter_rules += f" AND session_process_name = '{self.session_process_name}'"

      elif (c == 'batch_id' and self.active_batch_id is not None):
        partition_filter_rules += f" AND batch_id = '{self.active_batch_id}'"

      elif (c == 'start_date' and self.active_start_date is not None):
        partition_filter_rules += f" AND start_date = '{self.active_start_date}'"

      elif (c == 'process_name' and self.active_process_name is not None):
        partition_filter_rules += f" AND process_name = '{self.active_process_name}'"

      else:
        pass

    ## Optimize table as well on initilization
    optimize_sql_str = f"""OPTIMIZE {self.full_table_name} WHERE 1=1 {partition_filter_rules} ZORDER BY ({z_order_col_str})"""
    return optimize_sql_str
  


  def optimize_logger(self):

    optimize_script = self.get_optimize_sql()
    try:
      #print(f"Optimizing logger {self.full_table_name} with script:\n {optimize_script}")  
      ## Optimize table as well on initilization
      self.spark.sql(optimize_script)
    except Exception as e: 
      print(f"FAILED to optimize logger table {self.full_table_name}")
      raise(e)

    return
  


  ## Create a new run and return the state for it
  def start_run(self, process_name = None, batch_id = None, metadata: dict[str] = None, allow_concurrent_runs=False):

    ## Check for specific process override, otherwise look for session process

    ## If specific process_name is not provided, then make session_process_name = child_process_name
    process_name = self.resolve_process_name(process_name) ## This returns active process name if not provided, but on creation, we need to default it to the session if none provided
    
    if process_name is None:
      process_name = self.session_process_name

    session_process_name = self.session_process_name
    batch_id = self.resolve_batch_id(batch_id)
    
    ## Make start timestamp
    ts = datetime.now()
    start_ts = ts.strftime("%Y-%m-%d %H:%M:%S.%f")
    
    print(f"CREATING RUN for process: {process_name} under session {session_process_name} with batch session id {batch_id} \n Start Run at: {start_ts}")
    
    ## Parse and prepare any metadata
    status_key = "status"
    status_value = "CREATED"

    status_event_json = {status_key : status_value,
                        "event_timestamp": start_ts}


    default_metadata_struct = {"status_changes": [], "metadata": []}
    default_metadata_struct["status_changes"].append(status_event_json)
    default_metadata_struct["metadata"].append(metadata)

    default_metadata_struct["status_changes"] = list(filter(None, default_metadata_struct["status_changes"]))
    default_metadata_struct["metadata"] = list(filter(None, default_metadata_struct["metadata"]))

    pre_metadata = json.dumps(default_metadata_struct)


    try: 

      run_sql = f"""
        INSERT INTO {self.full_table_name} (batch_id, session_process_name, process_name, start_timestamp, status, run_metadata, update_timestamp) VALUES ('{batch_id}','{session_process_name}', '{process_name}', '{start_ts}', 'RUNNING', '{pre_metadata}', now())
      """
      ## Create run
      self.spark.sql(run_sql)

      ## Every time a logger initializes, it allows a user to alter the default partition columns of the table. 
      ## Every time logger inits, the logger checks the existing table for the existing partitions cols
      ## So this is so we dont optimize a table on a different set of columns

    except Exception as e:
      msg = str(e)
      print(f"\n FAILED: START LOGGER RUN with below error. Check you table name and scope: Table Name: {self.full_table_name}. \n  Error: \n {msg}")
      raise(e)
      
    try:

      ## Get run Metadata
      self.active_run_id, self.active_batch_id, self.active_run_status, self.active_run_start_ts, self.active_start_date, self.active_run_end_ts, active_run_metadata, self.active_process_name = [i for i in self.spark.sql(f"""  
          SELECT
          run_id,
          batch_id,
          status, 
          start_timestamp,
          start_date,
          end_timestamp,
          run_metadata,
          process_name
          FROM {self.full_table_name}
          WHERE 
          session_process_name = '{session_process_name}'
          AND process_name = '{process_name}'
          AND batch_id = '{batch_id}'
          AND start_timestamp = '{start_ts}'
          AND status = 'RUNNING'
          ORDER BY run_id DESC
          LIMIT 1
              """).collect()[0]]
      
      active_run_dict = json.loads(active_run_metadata)
      clean_run_metadata = re.sub("\'", "\"", active_run_metadata)
      active_run_dict = json.loads(clean_run_metadata)

      self.active_run_metadata = active_run_dict

      self.optimize_logger()

      print(f"RUN CREATED with run_id = {self.active_run_id} for process = {process_name}")
    
      ### This automatically marks existing active runs as stale, thus only allowing one concurrent run per process
      if not allow_concurrent_runs: 
        print(f"WARNING: Cleaning up previous active runs for this process before creating new run because allow_concurrent_runs is FALSE")
        self.clean_up_stale_runs(self.active_process_name)


    except Exception as e:
      print(f"\n CREATE RUN FAILED: Error getting new run metadata from: {self.full_table_name}. \n Error: \n {str(e)}")
      raise(e)

      
    return



  ## Get status for run id or active run if
  def get_status_for_run_id(self, run_id=None, process_name=None):

    ## Resolve Process Name
    process_name = self.resolve_process_name(process_name = process_name)

    ## Resolve Run Id
    run_id = self.resolve_run_id(run_id = run_id)

    sql_query = f"""SELECT MAX(status) FROM {self.full_table_name} 
                                        WHERE
                                        AND session_process_name = '{self.session_process_name}'
                                        AND run_id = {run_id}
                                        """

    if process_name is not None:
      sql_query += f" AND process_name = '{process_name}'"
    else: 
      print(f"No active process name, so looking for most recent run status for session process scope. To narrow, provide a process name or run this command during an active run.")
    try:

      new_active_run_status = int(self.spark.sql(sql_query).collect()[0][0])
    
    except Exception as e:
      print(f"FAIL: Unable to get metadata for run id {run_id} in process: {process_name} with error: \n {str(e)}")
      raise(e)
    
    return new_active_run_status
  

  ## Get start time for a given run id
  ## Defaults to getting the active run in the process
  def get_start_time_for_run_id(self, run_id=None, process_name=None):

    ## Resolve Process Name
    process_name = self.resolve_process_name(process_name = process_name)

    ## Resolve Run Id
    run_id = self.resolve_run_id(run_id = run_id)

    sql_query = f"""SELECT MAX(start_timestamp) FROM {self.full_table_name} 
                                        WHERE session_process_name = '{self.session_process_name}'
                                        AND run_id = {run_id}
                                        """
    if process_name is not None:
      sql_query += f" AND process_name = '{process_name}'"
    else: 
      print(f"No active process name, so looking for most recent run start time for session process scope. To narrow, provide a process name or run this command during an active run.")

    try:

      active_run_start_time = self.spark.sql(sql_query).collect()[0][0]
    
    except Exception as e:
      print(f"FAIL: Unable to get start timestamp for run id {run_id} in process: {process_name} with error: \n {str(e)}")
      raise(e)
    
    return active_run_start_time
  

  ## Get start time for a given run id
  ## Defaults to getting the active run in the process
  def get_end_time_for_run_id(self, run_id=None, process_name=None):

    ## Resolve Process Name
    process_name = self.resolve_process_name(process_name = process_name)
    ## Resolve Run Id
    run_id = self.resolve_run_id(run_id = run_id)

    sql_query = f"""SELECT MAX(end_timestamp) FROM {self.full_table_name} 
                                        WHERE
                                        session_process_name = '{self.session_process_name}'
                                        AND run_id = {run_id}
                                        """

    if process_name is not None:
      sql_query += f" AND process_name = '{process_name}'"
    else: 
      print(f"No active process name, so looking for most recent run end time for session process scope. To narrow, provide a process name or run this command during an active run.")

    try:

      active_run_end_time = self.spark.sql(sql_query).collect()[0][0]
    
    except Exception as e:
      print(f"FAIL: Unable to get start timestamp for run id {run_id} in process: {process_name} with error: \n {str(e)}")
      raise(e)
    
    return active_run_end_time
  


  ## Get metadata for run id for a given process or active run id
  def get_metadata_for_run_id(self, run_id=None, process_name=None):

    ## Resolve Process Name
    process_name = self.resolve_process_name(process_name = process_name)
    ## Resolve Run Id
    run_id = self.resolve_run_id(run_id = run_id)

    sql_query = f"""SELECT MAX(run_metadata) FROM {self.full_table_name} 
                                        WHERE
                                        session_process_name = '{self.session_process_name}'
                                        AND run_id = {run_id}
                                        """

    if process_name is not None:
      sql_query += f" AND process_name = '{process_name}'"
    else: 
      print(f"No active process name, so looking for most recent run metadata for session process scope. To narrow, provide a process name or run this command during an active run.")

    try:
    
      new_active_run_metadata = json.loads(self.spark.sql(sql_query).collect()[0][0])
    
    except Exception as e:
      print(f"UPDATE: No run metadata found for run id: {run_id} on process: {process_name}. Create new dataset")
      new_active_run_metadata = {"status_changes": [], "metadata": []}

    return new_active_run_metadata


  ## Get most recent run id for a given process
  def get_most_recent_run_id(self, process_name=None, status=None):

    ## Resolve Process Name
    process_name = self.resolve_process_name(process_name = process_name)

    sql_query = f"""SELECT MAX(run_id) FROM {self.full_table_name}
                                        WHERE session_process_name = '{self.session_process_name}'
    """

    if process_name is not None:
      sql_query += f" AND process_name = '{process_name}'"
    else: 
      print(f"No active process name, so looking for most recent run id for session process scope. To narrow, provide a process name or run this command during an active run.")


    ## Resolve optional status filter
    if status is None:
      pass
    else: 
      if status not in ["RUNNING", "SUCCESS", "FAILED", "STALE"]:
        raise(ValueError(f"{status} not in allowed values: RUNNING, SUCCESS, FAILED, STALE"))
      else: 
        sql_query += f" AND status = '{status}'"

    try: 
      try:
        new_active_run_id = int(self.spark.sql(sql_query).collect()[0][0])

      except Exception as e:
        new_active_run_id = -1

    except Exception as e:
      print(f"FAIL: Unable to get status of most recent run id for process {process_name} with error: \n {str(e)}")
      raise(e)

    return new_active_run_id
  


  ## Functions important for watermarking
  def get_most_recent_success_run_start_time(self, process_name=None):

    process_name = self.resolve_process_name(process_name = process_name)

    try:
      most_recent_run_id = self.get_most_recent_run_id(process_name=process_name, status="SUCCESS")

      if most_recent_run_id == -1:
        return "1900-01-01 00:00:00.000"
      
      else: 
        latest_success_start_timetamp = self.get_start_time_for_run_id(run_id=most_recent_run_id, process_name=process_name)


    except Exception as e:
      print(f"FAIL: Unable to get success start time of most recent run for process: {process_name} with error: \n {str(e)}")
      raise(e)  
    
    print(f"LAST SUCCESSFUL RUN IS: {most_recent_run_id} at {latest_success_start_timetamp}")

    return latest_success_start_timetamp
  

  def get_most_recent_success_run_end_time(self, process_name=None):
    process_name = self.resolve_process_name(process_name = process_name)

    try:
      most_recent_run_id = self.get_most_recent_run_id(process_name=process_name, status="SUCCESS")

      if most_recent_run_id == -1:
        return "1900-01-01 00:00:00.000"
      
      else:
        latest_success_end_timetamp = self.get_end_time_for_run_id(run_id=most_recent_run_id, process_name=process_name)

    except Exception as e:
      print(f"FAIL: Unable to get success end time of most recent run for process: {process_name} with error: \n {str(e)}")
      raise(e)  
    
    return latest_success_end_timetamp
  

  def get_most_recent_fail_run_start_time(self, process_name=None):

    process_name = self.resolve_process_name(process_name = process_name)

    try:
      most_recent_run_id = self.get_most_recent_run_id(process_name=process_name, status="FAIL")
      
      if most_recent_run_id == -1:
        return "1900-01-01 00:00:00.000"
      
      else:
        latest_success_start_timetamp = self.get_start_time_for_run_id(run_id=most_recent_run_id, process_name=process_name)

    except Exception as e:
      print(f"FAIL: Unable to get failed start time of most recent run for process: {process_name} with error: \n {str(e)}")
      raise(e)  
    
    return latest_success_start_timetamp
  

  def get_most_recent_fail_run_end_time(self, process_name=None):
    process_name = self.resolve_process_name(process_name = process_name)

    try:
      most_recent_run_id = self.get_most_recent_run_id(process_name=process_name, status="FAIL")

      if most_recent_run_id == -1:
        return "1900-01-01 00:00:00.000"
      
      else:
        latest_success_end_timetamp = self.get_end_time_for_run_id(run_id=most_recent_run_id, process_name=process_name)

    except Exception as e:
      print(f"FAIL: Unable to get failed end time of most recent run for process: {process_name} with error: \n {str(e)}")
      raise(e)   
    
    return latest_success_end_timetamp


  #### Stateful functions to get data for most recent run in a process
  ## This is DIFFERENT than whatever the active run id is
  ## Get status of most recent run
  def get_most_recent_run_status(self, process_name=None):

    ## Resolve Process Name
    process_name = self.resolve_process_name(process_name = process_name)

    try: 
      most_recent_run_id = self.get_most_recent_run_id(process_name=process_name)
      most_recent_run_status = self.get_status_for_run_id(run_id=most_recent_run_id, process_name = process_name)

    except Exception as e: 
      print(f"FAIL: Unable to get status of most recent run for process: {process_name} with error: \n {str(e)}")
      raise(e)

    return most_recent_run_status


  ## Get status of most recent run
  def get_most_recent_run_metadata(self, process_name=None):

    most_recent_run_metadata = None

    ## Resolve Process Name
    process_name = self.resolve_process_name(process_name = process_name)

    try: 
      most_recent_run_id = self.get_most_recent_run_id(process_name=process_name)
      most_recent_run_metadata = self.get_metadata_for_run_id(run_id=most_recent_run_id, process_name = process_name)

    except Exception as e: 
      print(f"FAIL: Unable to get status of most recent run metadata for process: {process_name} with error: \n {str(e)}")
      raise(e)

    return most_recent_run_metadata
  
  
  ## INTERNAL ONLY, this can update anything. We dont need to expose this
  def _update_run_id(self, process_name = None, run_id = None, status=None, start_timestamp=None, end_timestamp=None, duration_seconds=None, duration_ms=None, run_metadata:dict[str]=None, batch_id=None, run_metrics:dict[str]=None):

    process_name = self.resolve_process_name(process_name)
    run_id = self.resolve_run_id(run_id)

    current_run_metadata = self.get_metadata_for_run_id(process_name=process_name, run_id = run_id)

    ## Root SQL

    base_sql = f"""UPDATE {self.full_table_name} AS tbl 
                  SET  
    """

    if status is not None:
      base_sql += f" status = '{status}', "

      ts = datetime.now()
      status_ts = ts.strftime("%Y-%m-%d %H:%M:%S.%f")

      ## Parse and prepare any metadata
      status_key = "status"
      status_value = status

      status_event_json = {status_key : status_value,
                        "event_timestamp": status_ts}
      
      current_run_metadata["status_changes"].append(status_event_json)


    if start_timestamp is not None: 
      base_sql += f" start_timestamp = '{start_timestamp}', "

    if end_timestamp is not None: 
      base_sql += f" end_timestamp = '{end_timestamp}', "

    if run_metadata is not None: 
      current_run_metadata["metadata"].append(run_metadata)

    ## Adds new keys to run_metadata column
    if run_metrics is not None: 
      try:
        current_run_metadata.update(run_metrics)
      except Exception as e:
        print("FAILED TO ADD METRICS: Make sure your metrics data is a dictionary.")
        raise(e)
    
    if duration_seconds is not None:
      base_sql += f" duration_seconds = {duration_seconds}, "

    if duration_ms is not None: 
      base_sql += f" duration_ms = {duration_ms}, "

    if batch_id is not None:
      base_sql += f" batch_id = '{batch_id}', "

    ## run metadata update
    if status is not None or run_metadata is not None or run_metrics is not None:
      prepped_run_metadata = json.dumps(current_run_metadata)
      base_sql += f" run_metadata = '{prepped_run_metadata}', "


    base_sql += f""" update_timestamp = now()  
                WHERE process_name = '{process_name}'
                AND session_process_name = '{self.session_process_name}'
                AND run_id = {run_id}
                ;
                """

    try: 

      print(f"UPDATE RUN {run_id} for process {process_name}")
      self.spark.sql(base_sql)

    except Exception as e:
      print(f"UPDATE FAILED: Unable to update run_id {run_id} for process: {process_name} with error: \n {str(e)}")
      raise(e)

    return
  
  
  ## Clean up any older run ids for a given process that are still running but have no end timestamp
  ## TO DO: Make this set an identifier in run metadata
  def clean_up_stale_runs(self, process_name=None):

    ## Resolve Process Name
    process_name = self.resolve_process_name(process_name = process_name)

    ## Resolve Run Id
    active_run_id = self.get_most_recent_run_id(process_name=process_name, status="RUNNING")

    try:

      print(f"CLEANING STALE runs for process {process_name} IN SCOPE {self.session_process_name}")

      self.spark.sql(f"""
                     UPDATE {self.full_table_name} AS tbl
                      SET 
                      status = 'STALE',
                      end_timestamp = now(),
                      update_timestamp = now()
                     WHERE process_name = '{process_name}'
                     AND session_process_name = '{self.session_process_name}'
                     AND run_id < {active_run_id}
                     AND (status NOT IN ('FAIL', 'SUCCESS', 'STALE') OR end_timestamp IS NULL);
                     """)
      
      ## Any time we update we want to optimize the logger, maybe we make it a config
      self.optimize_logger()

    except Exception as e:
      print(f"FAILED to clean stale runs with error: {str(e)}")

    ## Now clean up stale runs
    """
    1. With active run id / status - update all runs with run_id < active_run_id AND status != 'SUCCESS'/'FAILED'
    2. Set all flagged run ids with STATUS = 'STALE', add key in metadata to add that it was flagged as stale because of new concurrent run of same process
    """
    return
  
  
  ## Finally, complete the run!
  def complete_run(self, process_name=None, run_id=None, metadata=None):
    status = "SUCCESS"
    process_name = self.resolve_process_name(process_name)
    run_id = self.resolve_run_id(run_id)

    ## TESTING
    if process_name != self.active_process_name:
      msg = f"WARNING: Trying to fail a run for a process that is not the active process run. Can only complete/fail active runs one at a time per session scope. \nCurrent active process is {self.active_process_name}"
      raise(ValueError(msg))
    
    ## Make end timestamp
    e_ts = datetime.now()
    end_ts = e_ts.strftime("%Y-%m-%d %H:%M:%S.%f")

    start_ts = self.active_run_start_ts.strftime("%Y-%m-%d %H:%M:%S.%f")

    ee_s = self.active_run_start_ts
    ee_e = e_ts

    td = ee_e - ee_s
    run_duration_seconds = td/timedelta(milliseconds=1)/1000
    run_duration_ms = run_duration_seconds*1000

    try:

      self._update_run_id(process_name=process_name, run_id=run_id, status=status, end_timestamp=end_ts, run_metadata=metadata, duration_ms=run_duration_ms, duration_seconds = run_duration_seconds)

      ## Optimize logger for the specific process is cleared
      self.optimize_logger()


      self.active_run_end_ts = None
      self.active_run_id = None
      self.active_run_metadata = None
      self.active_run_start_ts = None
      self.active_run_status = None
      self.active_process_name = None

      print(f"COMPLETED RUN {run_id} for process {process_name} at {end_ts}!")

    except Exception as e:
      print(f"FAILED to mark run complete for run_id {run_id} in process {process_name} with error: {str(e)}")
      raise(e)
    
    return
  

  def fail_run(self, process_name=None, run_id=None, metadata=None):

    status = "FAIL"
    process_name = self.resolve_process_name(process_name)

    ## TESTING
    if process_name != self.active_process_name:
      msg = f"WARNING: Trying to fail a run for a process that is not the active process run. Can only complete/fail active runs one at a time per session scope. \nCurrent active process is {self.active_process_name}"
      raise(ValueError(msg))
    
    run_id = self.resolve_run_id(run_id)

    ## Make end timestamp
    e_ts = datetime.now()
    end_ts = e_ts.strftime("%Y-%m-%d %H:%M:%S.%f")

    start_ts = self.active_run_start_ts.strftime("%Y-%m-%d %H:%M:%S.%f")
    ee_s = self.active_run_start_ts
    ee_e = e_ts

    td = ee_e - ee_s
    run_duration_seconds = td/timedelta(milliseconds=1)/1000
    run_duration_ms = run_duration_seconds*1000

    try:

      self._update_run_id(process_name=process_name, run_id=run_id, status=status, end_timestamp=end_ts, run_metadata=metadata, duration_ms=run_duration_ms, duration_seconds = run_duration_seconds)

      ## Optimize logger for the specific process is cleared
      self.optimize_logger()

      self.active_run_end_ts = None
      self.active_run_id = None
      self.active_run_metadata = None
      self.active_run_start_ts = None
      self.active_run_status = None
      self.active_process_name = None
      

      print(f"FAILED RUN {run_id} for process {process_name} at {end_ts}!")

    except Exception as e:
      print(f"FAIL to mark run FAIL for run_id {run_id} in process {process_name} with error: {str(e)}")
      raise(e)

    return
  

  ## Add logging events to active runs
  ## Commit immediately = True will update the delta table record synchronously, False will store update in the instance and commit on complete/fail
  ## Options for level INFO/WARN/DEBUG/CRITICAL
  def log_run_info(self, log_level:str ="INFO", msg:str = None, process_name=None, run_id=None):

    if log_level not in ["DEBUG", "INFO", "WARN", "CRITICAL"]:
      raise(ValueError("log_level must be one of the following values: DEBUG/INFO/WARN/CRITICAL"))

    process_name = self.resolve_process_name(process_name)
    run_id = self.resolve_run_id(run_id)

    if process_name != self.active_process_name:
      err_msg = f"WARNING: Trying to log info run for a process that is not the active process run. You can technically log info for any historical run but do so selectively.\n Process will log the info for the run if the run_id exists and is provided or will try to log in the active run id. \n Current active process is {self.active_process_name} \nCurrent run id is {run_id}"

    if run_id is None:
      err_msg = f"WARNING: Trying to log for a process where there is no active or manually provided run. Start a run first or provide a run_id manually to update and older run. \nCurrent active process is {self.active_process_name} \n Current run id is {run_id}"
      raise(ValueError(err_msg))
    
    ts = datetime.now()
    log_ts = ts.strftime("%Y-%m-%d %H:%M:%S.%f")

    current_run_metadata = self.get_metadata_for_run_id(run_id)
    run_metadata = {"log_level": log_level, "log_data": {"event_ts": log_ts, "msg": msg}}
    
    try: 

      if msg is not None:

        self._update_run_id(process_name=process_name, run_id=run_id, run_metadata=run_metadata)
        print(f"{log_level} - {log_ts} for {run_id} in process {process_name}. MSG: {run_metadata}")
        
      else:
        print("No msg to log for run. Skipping. ")

    except Exception as e:
      print(f"FAILED to log event for run_id {run_id} and process_name {process_name} with error: {str(e)}")
      raise(e)
    
    return
  
  
  ## Add specific metadata named parameters/metrics
  ## This does not add info under the nested "metadata" key. Instead this ADDs a key in the run_metadata column so that it can be queried directly in SQL easily. This is good for important metrics that need to be accessed and analyzed frequently (like rows affected, etc.)
  def log_run_metric(self, process_name=None, run_id=None, run_metrics_dict=None):

    process_name = self.resolve_process_name(process_name)
    active_run_id = self.resolve_run_id(run_id)

    if (process_name != self.active_process_name and run_id is None):
      err_msg = f"WARNING: Trying to log metrics for a process that is not the active process run. You can technically log info for any historical run but do so selectively.\n Process will log the metrics for the run if the run_id exists and is provided or will try to log in the active run id. \n Current active process is {self.active_process_name} \nCurrent run id is {run_id}"
      raise(ValueError(err_msg))

    if active_run_id is None:
      err_msg = f"WARNING: Trying to log metrics for a process where there is no active or manually provided run. Start a run first or provide a run_id manually to update and older run. \nCurrent active process is {self.active_process_name} \n Current run id is {run_id}"
      raise(ValueError(err_msg))
    try: 

      if run_metrics_dict is not None:

        self._update_run_id(process_name=process_name, run_id=active_run_id, run_metrics=run_metrics_dict)
        print(f"ADDED METRCS to run_metadata for {active_run_id} in process {process_name}. Metrics: {json.dumps(run_metrics_dict)}")
        
      else:
        print("No Metrics to Log. Skipping. ")

    except Exception as e:
      print(f"FAILED to log event for run_id {active_run_id} and process_name {process_name} with error: {str(e)}")
      raise(e)
    
    return

