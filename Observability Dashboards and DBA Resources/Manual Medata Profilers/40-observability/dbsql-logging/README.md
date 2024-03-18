### dbsql-logging
This tool is a collection notebooks that pulls together data from 4 different APIs to produce useful metrics to monitor DBSQL usage:
* [SQL Warehouses APIs 2.0](https://docs.databricks.com/sql/api/sql-endpoints.html), referred to as the Warehouse API
* [Query History API 2.0](https://docs.databricks.com/sql/api/query-history.html), referred to as the Queries API
* [Jobs API 2.1](https://docs.databricks.com/dev-tools/api/latest/jobs.html), referred to as the Workflows API
* [Queries and Dashboards API](https://docs.databricks.com/sql/api/queries-dashboards.html), referred to as the Dashboards API  - ❗️in preview, known issues, deprecated soon❗️

Creator: holly.smith@databricks.com

#### Setup
This tool has been tested with the following 
Cluster config: 
* 11.3 LTS 
* Driver: i3.xlarge
* Workers: 2 x i3.xlarge  - the data here is fairly small

Profile:
Must be an **admin** in your workspace for the Dashboards API

#### Notebooks

##### 00-Config
This is the configuration of:
* Workspace URL
* Authetication options
* Database and Table storage
* `OPTIMIZE`, `ZORDER` and `VACUUM` settings

##### 01-Functions
* Reuable functions created, all pulled out for code readability

##### 02-Initialisation
* Creates the database if it doesn't exist
* Optional: specify a location for the Database
Dependent on: `00-Config`

##### 03-APIs_to_Delta

**Warehouses API:** Appends each call of the api and uses a snapshot time to identify 

**Query History API:** Upserts / merges new queries to the original table

**Workflows API:** Upserts / merges new workflows to the original table

**Dashboards API:**  . I have tried my best to refer to it as a preview in every step of the code to reflect how this is a preview

Dependent on: `00-Config`, `01-Functions`, `02-Initialisation`

##### 04-Metrics
* Dashboards & Queries with owner, useful for finding orphaned records
* Queries to Optimise
* Warehouse Metrics
* Per User Metrics


Dependent on: `00-Config`


##### 99-Maintenance
Runs `OPTIMIZE`, `ZORDER` and `VACUUM` against tables

Dependent on: `00-Config`


##### Troubleshooting

###### Cluster OOM
The data used here was very small, even for a Databricks demo workspace with thousands of users. Parts of 03-APIs_to_Delta involves pulling a json to the driver, in the highly unlikely event of the driver OOM you have two choices:
1. The quick option: select a larger driver
2. The robust option: loop through reading in only one page at a time and write to a spark dataframe at a time

###### Dashboards API not sorting in new queries
There are known issues with the API. Where possible, try to use the 

###### Dashboards API has stopped working
This API will go through stages of deprecation, unfortunately with no hard timelines as of yet. Here is the rough process:
1. When DBSQL Pro comes out, the API will be officially deprecated
2. It should (*should*) be removed from the documentation at that point
3. Later it will become totally unavailable 

The Query History API captures data shown below

![Query History](https://i.imgur.com/fZaQYzT.png)
