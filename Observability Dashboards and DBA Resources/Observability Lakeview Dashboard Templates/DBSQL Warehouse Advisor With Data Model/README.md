## DBSQL Warehouse Advisor with Materilized Data Model

Description: This folder contains a SQL Query and a Lakeview Dashboard template to build the Databricks SQL warehouse adivisor. 

## Steps: 

<b> 1. </b> Run SQL Query to create materialized views and schema. 
The SQL Query will create a schema called 'dbsql_warehouse_advisor' in the 'main' catalog. You can alter/edit the location of these MVs but you will then need to change the queries in the Dashboard template. This step makes the more complex dashboard calculations happen daily and materialized so the dashboard does not need to do as much work each time it is queried. Creating a Materialized View Layer is considered a best-practice for building performant dashboards using MVs as your 'gold' layer. 

<b> 2. </b> Import the DBSQL Warehouse Advisor Template. Import the JSON file in the 'Dashbaords' tab of your Databricks SQL workspace. 

<b> 3. </b> Update Defailt Paramters. Open the imported Dashboard, go to the 'Data' tab of the dashboard, and fill out the default parameters you would like for each query when your dashboard is published. This includes default warehouses, SKU discounts, etc. 

<b> 4. </b> Run and publihs the dashboard! There are 4 sections that come as 1 single dashboard, but you can split the dash into multiple dashboards for better performance (tabbed dashboards coming soon to make this automatic). Review the sections and instructions in the dashbaord itself and publish!

### Notes: 

1. The Materialized Views are defaults to be on a schedule automatically on a daily basis - this incurs costs. You can remove or adjust the schedule depending on your requirements. The costs is small and are all built on system table abstractions. 