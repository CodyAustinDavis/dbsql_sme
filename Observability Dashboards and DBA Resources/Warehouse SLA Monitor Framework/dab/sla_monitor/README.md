# Warehouse SLA Monitor DAB

The `Warehouse SLA Monitor` framework is packaged as a Databricks Asset Bundle (DAB), making it simple to deploy an SLA monitor to a target Databricks workspace.

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

4. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```

   Note that the default job from the template has a schedule that runs every day
   (defined in resources/sla_monitor_job.yml). The schedule
   is paused when deploying in development mode (see
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).

5. To run a job or pipeline, use the "run" command and supply the notebook params for the Warehouse SLA poller using a comma-separated string:

- `workspace_name` - the name of the target Databricks workspace (without the 'https://`)
- `database_name` - the fully-qualified (catalog name) and database name to save the metrics tables to
- `policy_id` - the SLA policy identifier

   ```
   $ databricks bundle run --notebook-params workspace_name=my-db-workspace.cloud.databricks.com,database_name=my_catalog.monitoring,policy_id=2
   ```

6. Optionally, install developer tools such as the Databricks extension for Visual Studio Code from
   https://docs.databricks.com/dev-tools/vscode-ext.html.

7. For documentation on the Databricks asset bundles format used
   for this project, and for CI/CD configuration, see
   https://docs.databricks.com/dev-tools/bundles/index.html.
