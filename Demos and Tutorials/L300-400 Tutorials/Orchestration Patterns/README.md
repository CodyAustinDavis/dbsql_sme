# dbsql_workflows

This repository contains example data orchestration patterns using Databricks Workflows and Databricks SQL. These examples are available as Databricks Asset Bundles, making them easy to reproduce in your own workspace. Follow the instructions below for more details. 

If you have comments, questions, or feedback, reach out to justin.kolpak@databricks.com. 

## Getting started

1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

3. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project.
    You can find the jobs by opening your workpace and clicking on **Workflows**.

4. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```

5. Update your project-specific variables in the databricks.yml file. See the sections for **variables** and **targets**.

6. Once the project is configured, run the data loader and configuration initiatlization jobs
   ```
   databricks bundle run init_tpch_historical_load --profile <YOUR_PROFILE>
   databricks bundle run init_move_config_to_volume  --profile <YOUR_PROFILE>
   ```
   
   * init_tpch_historical_load: This is used to copy the TPC-H dataset from the /databricks-datasets/ location into a Unity Catalog Volume.
   * init_move_config_to_volume: This is used to copy the configuration JSON file from this Repo into a Unity Catalog Volume, which is used in the *get_ingest_metadata* task. Any time a change is made to the `config.json` file, you'll need to re-run this job. In a produciton setting, this should be built in as part of a CI/CD process. 

7. For documentation on the Databricks asset bundles format used
   for this project, and for CI/CD configuration, see
   https://docs.databricks.com/dev-tools/bundles/index.html.
