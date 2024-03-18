from utils.general import setup_files
from utils.databricks_client import DatabricksClient


def run(spark, dbutils, constants):
    # Step 0: drop write schema if exists
    spark.sql(
        f"create schema if not exists {constants.catalog_name}.{constants.schema_name}"
    )

    # Step 1: write init script, jar, and beaker whl to DBFS
    setup_files(
        dbutils,
        constants.jar_path,
        constants.init_script_path,
        constants.beaker_whl_path,
    )

    # Step 2: create the client
    client = DatabricksClient(constants)

    # Step 3: create a warehouse to benchmark against
    warehouse_id = client.create_warehouse().id
    constants.warehouse_id = warehouse_id

    # Step 4: create a job to create TPCDS data and queries at a given location and runs benchmarks
    job_id = client.create_job().job_id
    run_id = client.run_job(job_id).run_id

    # Step 5: monitor the job run until completion
    url = f"{constants.host.replace('www.','')}#job/{job_id}/run/{run_id}"
    print(f"\nA TPC-DS benchmarking job was created at the following url:\n\t{url}\n")
    print(f"It will write TPC-DS data to {constants.data_path}.")
    print(
        "The job may take several hours depending upon data size, so please check back when it's complete.\n"
    )