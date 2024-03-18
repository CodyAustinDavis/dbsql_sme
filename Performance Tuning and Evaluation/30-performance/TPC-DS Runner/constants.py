# Databricks notebook source
pip install --upgrade databricks-sdk -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import math
from dataclasses import dataclass
from utils.general import tables_already_exist, get_widget_values, create_widgets

@dataclass
class Constants:
    ############### Variables dependant upon user parameters ##############
    # Number of GBs of TPCDS data to write
    number_of_gb_of_data: int

    # Name of the catalog to write TPCDS data to
    catalog_name: str

    # Prefex of the schma to write TPCDS data to
    schema_prefix: str

    # Size of the warehouse cluster
    warehouse_size: str

    # Maximum number of clusters to scale to in the warehouse
    maximum_number_of_clusters: int

    # Warehouse channel name
    channel: str

    # Number of concurrent threads
    concurrency: int

    # Number of times to duplicate the benchmarking run
    query_repetition_count: int

    ############### Variables indepenet of user parameters #############
    # Name of the job
    job_name = f"[AUTOMATED] Create and run TPC-DS"

    # Dynamic variables that are used to create downstream variables
    _current_user_email = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .userName()
        .get()
    )
    _cwd = os.getcwd().replace("/Workspace", "")

    # User-specific parameters, which are used to create directories and cluster single-access-mode
    current_user_email = _current_user_email
    current_user_name = (
        _current_user_email.replace(".", "_").replace("-", "_").split("@")[0]
    )

    # Base directory where all data and queries will be written
    root_directory = f"dbfs:/Benchmarking/TPCDS/{current_user_name}"

    # Additional subdirectories within the above root_directory
    script_path = os.path.join(root_directory, "scripts")
    data_path = os.path.join(root_directory, "data")
    query_path = os.path.join(root_directory, "queries")

    # Location of the spark-sql-perf jar, which is used to create TPC-DS data and queries
    jar_path = os.path.join(script_path, "jars/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar")

    # Location of the init script, which is responsible for installing the above jar and other prerequisites
    init_script_path = os.path.join(script_path, "tpcds-install.sh")

    # Location of the dist whl for beaker
    beaker_whl_path = os.path.join(script_path, "beaker-0.0.1-py3-none-any.whl")

    # Location of the notebook that creates data and queries
    create_data_and_queries_notebook_path = os.path.join(
        _cwd, "notebooks/create_data_and_queries"
    )

    # Location of the notebook that runs TPC-DS queries against written data using the beaker library
    run_tpcds_benchmarking_notebook_path = os.path.join(
        _cwd, "notebooks/run_tpcds_benchmarking"
    )

    # Name of the current databricks host
    host = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/"

    def _validate_concurrency_will_utilize_cluster(self):
        required_number_of_clusters = math.ceil(self.concurrency / 10)
        if self.maximum_number_of_clusters > required_number_of_clusters:

            print(
                "Warning:\n"
                "\tFor optimal performance, we recommend using 1 cluster per 10 levels of concurrency. Your currrent\n"
                "\tconfiguration will underutilize the warehouse and a cheaper configuration shuold exhibit the same performance.\n"
                f"\tPlease try using {required_number_of_clusters} clusters instead."
            )

    def __post_init__(self):
        # Create a schema prefix if '' to ensure unrelated schemas are not deleted
        if self.schema_prefix == "":
            self.schema_prefix = "tpcds_benchmark"

        # Name of the schema that tpcds data and benchmarking metrics will be written to
        self.schema_name: str = (
            f"{self.schema_prefix.rstrip('_')}_{self.number_of_gb_of_data}_gb"
        )

        # Add schema to data path
        self.data_path = os.path.join(self.data_path, self.schema_name)

        # Determine if TPC-DS tables already exist
        self.tables_already_exist = tables_already_exist(spark, self.catalog_name, self.schema_name)

        # Param validations/warnings
        self._validate_concurrency_will_utilize_cluster()

