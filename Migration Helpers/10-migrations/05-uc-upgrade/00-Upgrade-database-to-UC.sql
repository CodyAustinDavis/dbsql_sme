-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ### A cluster has been created for this demo
-- MAGIC To run this demo, just select the cluster `dbdemos-uc-05-upgrade-abraham_pabbathi` from the dropdown menu ([open cluster configuration](https://e2-demo-field-eng.cloud.databricks.com/#setting/clusters/0728-225445-kxf11shs/configuration)). <br />
-- MAGIC *Note: If the cluster was deleted after 30 days, you can re-create it with `dbdemos.create_cluster('uc-05-upgrade')` or re-install the demo: `dbdemos.install('uc-05-upgrade')`*

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # Upgrade your tables to Databricks Unity Catalog
-- MAGIC
-- MAGIC Unity catalog provides all the features required to your data governance & security:
-- MAGIC
-- MAGIC - Table ACL
-- MAGIC - Row level access with dynamic view
-- MAGIC - Secure access to external location (blob storage)
-- MAGIC - Lineage at row & table level for data traceability
-- MAGIC - Traceability with audit logs
-- MAGIC
-- MAGIC Because unity catalog is added as a supplement to your existing account, migrating your existing data to the new UC is very simple.
-- MAGIC
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-base-1.png" style="float: right" width="700px"/> 
-- MAGIC
-- MAGIC Unity Catalog works with 3 layers:
-- MAGIC
-- MAGIC * CATALOG
-- MAGIC * SCHEMA (or DATABASE)
-- MAGIC * TABLE
-- MAGIC
-- MAGIC The table created without Unity Catalog are available under the default `hive_metastore` catalog, and they're scoped at a workspace level.
-- MAGIC
-- MAGIC New tables created with Unity Catalog will available at the account level, meaning that they're cross-workspace.
-- MAGIC
-- MAGIC <!-- tracking, please Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&aip=1&t=event&ec=dbdemos&ea=VIEW&dp=%2F_dbdemos%2Fgovernance%2Fuc-05-upgrade%2F00-Upgrade-database-to-UC&cid=1444828305810485&uid=553895811432007">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Cluster setup for UC
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-cluster-setup-single-user.png" style="float: right"/>
-- MAGIC
-- MAGIC
-- MAGIC To be able to run this demo, make sure you create a cluster with the security mode enabled.
-- MAGIC
-- MAGIC Go in the compute page, create a new cluster.
-- MAGIC
-- MAGIC Select "Single User" and your UC-user (the user needs to exist at the workspace and the account level)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Understanding the upgrade process
-- MAGIC
-- MAGIC ### External table and Managed table
-- MAGIC
-- MAGIC Before your upgrade, it's important to understand the difference between tables created with `External Location` vs `Managed table`
-- MAGIC
-- MAGIC * Managed tables are table created without any `LOCATION` instruction. They use the default database storage location which is usually your root bucket `/user/hive/warehouse/your_database/your_table`  (note that you can also change this property at the database level). If you drop a managed table it'll delete the actual data.
-- MAGIC * External tables are tables with data stored to a given LOCATION. Any table created with an instruction like `LOCATION 's3a:/xxx/xxx'` is external. Dropping an External table won't delete the underlying data.
-- MAGIC
-- MAGIC
-- MAGIC ### UC Access control
-- MAGIC
-- MAGIC * UC is in charge of securing your data. Tables in UC can be stored in 2 locations:
-- MAGIC   * Saved under the UC metastore bucket (the one you created to setup the metastore), typically as Managed table. This is the recommended thing to do by default.
-- MAGIC   * Saved under an [EXTERNAL LOCATION](https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-location.html) (one of your S3 bucket, ADLS...) secured with a STORAGE CREDENTIAL: `CREATE EXTERNAL LOCATION location_name URL url WITH (STORAGE CREDENTIAL credential_name)` 
-- MAGIC   
-- MAGIC In both case, **only the Unity Catalog should have the permission to access the underlying files on the cloud storage**. If a user can access the data directly at a file level (ex: in a workspace root bucket, with an instance profile already existing, or a SP) they could bypass the UC.
-- MAGIC
-- MAGIC ### 2 Upgrade options: pointing metadata to the external location or copying data to UC storage
-- MAGIC
-- MAGIC Knowing that, you have 2 options to upgrade your tables to UC:
-- MAGIC * **Moving the data:** If your data resides in a cloud storage and you know users will have direct file access (ex: a managed table in your root bucket or a cloud storage on which you want to keep file access), then you should move the data to a safe location in the UC (use your UC metastore default location as a recommended choice)
-- MAGIC * **Pointing to the existing data with an External Location**: If your data is in a cloud storage and you can ensure that only the UC process will have access (by removing any previously existing direct permission on instance profile/SP), you can just re-create the table metadata in the UC with an EXTERNAL LOCATION (without any data copy) 
-- MAGIC
-- MAGIC *Notes:*
-- MAGIC * *External tables saved in a bucket you can’t secure with UC external location (ex: saved in the workspace root storage `dbfs:/...` , or a storage with instance profile / Service Principal granting direct file access you can't remove) should be fully cloned too.* 
-- MAGIC * *Alternatively, Managed tables stored in an external bucket (this can be the case changing the default LOCATION of the DATABASE) can be upgrade using an External Location (without copying data).*

-- COMMAND ----------

-- MAGIC %run ./_resources/00-setup $catalog=dbdemos

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Migrating a single table to UC
-- MAGIC Let's see how your upgrade can be executed with a simple SQL query with the 2 options.
-- MAGIC
-- MAGIC *Note: Databricks will provide a `SYNC`command to simplify these operation over the next few months: `SYNC SCHEMA hive_metastore.syncdb TO SCHEMA main.syncdb_uc2 DRY RUN`*

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 1/ Moving the data to the UC (ex: MANAGED table in hive_metastore)
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/migration/uc-migration-managed.png" width="600px" style="float: right" />
-- MAGIC
-- MAGIC Let's assume you have a MANAGED table in your legacy hive_metastore (which is by default under your root storage `dbfs:/xx/xx`), and you want to move this table to UC.
-- MAGIC
-- MAGIC Because all your users have access to the root storage, your only option is to copy all the data to the UC:
-- MAGIC
-- MAGIC * To the UC default cloud storage (recommended, that's the default for any table you create without location)
-- MAGIC * To an external location that you have previously created.
-- MAGIC
-- MAGIC *As this is a hard copy, any update done on the legacy table after your DEEP CLONE won't be reflected. See below for more avanced options.*

-- COMMAND ----------

-- DBTITLE 1,We'll upgrade our tables to dbdemos catalog
USE CATALOG dbdemos;
SHOW TABLES;

-- COMMAND ----------

-- the table we want to upgrade is under hive_metastore.uc_database_to_upgrade.users:
SELECT * FROM hive_metastore.uc_database_to_upgrade.users

-- COMMAND ----------

-- As you can see this is a Managed table under our root cloud storage (dbfs:/):
DESCRIBE EXTENDED hive_metastore.uc_database_to_upgrade.users

-- Location            dbfs:/user/hive/warehouse/uc_database_to_upgrade.db/users
-- Is_managed_location true
-- Type                MANAGED

-- COMMAND ----------

-- DBTITLE 1,Cloning the hive_metastore table under the UC catalog
-- Create the new Database in the current UC catalog
CREATE DATABASE IF NOT EXISTS database_upgraded_on_uc;
-- As it's a managed table in the root bucket, we use DEEP CLONE to recopy the data in the UC. This will preserve all the table properties.
CREATE OR REPLACE TABLE database_upgraded_on_uc.users DEEP CLONE hive_metastore.uc_database_to_upgrade.users;
-- Set permission / ownership as needed 
ALTER TABLE database_upgraded_on_uc.users OWNER TO `account users`;

-- That's it, our table has been upgraded and fully moved to the UC. 
SELECT * FROM database_upgraded_on_uc.users;

--Note: Once your upgrade is completed your can delete the legacy table: DROP TABLE hive_metastore.uc_database_to_upgrade.users

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ### 2/ Upgrade to UC using an External Location without moving data (Ex: External table in a S3 bucket)
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/migration/uc-migration-external.png" width="600px" style="float: right" />
-- MAGIC
-- MAGIC Let's now assume that you have a table using a Location in a specific cloud storage (ex: `s3a:/my-bucket/my/table_folder`).
-- MAGIC
-- MAGIC You can create a new External table in the UC pointing to this location, without moving the data:
-- MAGIC
-- MAGIC * As admin, create a [STORAGE CREDENTIAL](https://docs.databricks.com/data-governance/unity-catalog/manage-external-locations-and-credentials.html) in UC and an [EXTERNAL LOCATION](https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-location.html) using this credential.
-- MAGIC This will ensure only UC engine can access the cloud storage in a secure fashion.
-- MAGIC * Create a UC table pointing to the same external location
-- MAGIC * Make sure no user can further access the cloud storage natively (`s3a:/my-bucket/my/table_folder`, using instance profile, SP etc)
-- MAGIC
-- MAGIC *As this is a link pointing to the data, no copy is done.*
-- MAGIC
-- MAGIC *Note: Again, if you external location is in your root storage you should instead copy the data as users will have direct access*

-- COMMAND ----------

-- the external table we want to upgrade is under hive_metastore.uc_database_to_upgrade.transactions:
SELECT * FROM hive_metastore.uc_database_to_upgrade.transactions

-- COMMAND ----------

-- As you can see this is an External table saved under s3a://databricks-e2demofieldengwest/external_location_uc_upgrade/transactions
DESCRIBE EXTENDED hive_metastore.uc_database_to_upgrade.transactions

-- Location            s3a://databricks-e2demofieldengwest/external_location_uc_upgrade/transactions
-- External            true
-- Type                EXTERNAL

-- COMMAND ----------

-- DBTITLE 1,Create the external location
-- Create the external location
CREATE EXTERNAL LOCATION IF NOT EXISTS `field_demos_external_location_uc_upgrade`
  URL 's3a://databricks-e2demofieldengwest/external_location_uc_upgrade' 
  WITH (CREDENTIAL `field_demos_credential`)
  COMMENT 'External Location of our legacy external tables' ;

-- let's make everyone owner for the demo to be able to change the permissions easily. DO NOT do that for real usage.
ALTER EXTERNAL LOCATION `field_demos_external_location_uc_upgrade`  OWNER TO `account users`;
-- Note: do not grant read files to any users as this will bypass the UC security.

-- COMMAND ----------

-- DBTITLE 1,Create the new table in the UC, pointing to the same underlying data
CREATE DATABASE IF NOT EXISTS database_upgraded_on_uc;

-- We can now create a link to existing reusing the same LOCATION as our legacy table:
CREATE TABLE IF NOT EXISTS database_upgraded_on_uc.transactions LIKE hive_metastore.uc_database_to_upgrade.transactions COPY LOCATION ;
-- for the demo only, let's make sure all users have access to the created UC table to be able to change files on the external location.
ALTER TABLE database_upgraded_on_uc.transactions OWNER TO `account users`;

-- COMMAND ----------

-- That's it! our table has been upgraded without cloning the actual data (just metadata creation)
SELECT * FROM database_upgraded_on_uc.transactions ;

--Note: Once your upgrade is completed your can delete the legacy table: DROP TABLE hive_metastore.uc_database_to_upgrade.users
--Note: Make sure you remove any other direct file access you might have to your external location (other than the UC credential, like custom Instance profile or Service Principal)
--Note: You can also add a note in the metastore table to know that it has been  and keep a reference:
-- ALTER TABLE hive_metastore.uc_database_to_upgrade.transactions SET TBLPROPERTIES ('upgraded_to' = 'uc_demos_quentin_ambard.database__on_uc.transactions');

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Migrating a table using Databricks UI
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/migration/uc-migration-ui.gif" style="float: right" />
-- MAGIC
-- MAGIC You can use the "Data" menu on the left to upgrade a specific table and generate the above queries for you.
-- MAGIC
-- MAGIC - Select the database you want to upgrade to UC 
-- MAGIC - Click on "Upgrade"
-- MAGIC - Select the tables to upgrade
-- MAGIC - Select the source destination & permissions
-- MAGIC - Run the upgrade!
-- MAGIC
-- MAGIC *Note: Currently, Upgrading a table to the UC using the UI only supports External location.*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Advanced upgrade options

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Programatically migrating existing database (under `hive_metastore`) to UC
-- MAGIC
-- MAGIC Let's see how we can take this one step further and programatically upgrade an entire database at once.
-- MAGIC
-- MAGIC We need a script that will automatically detect the table type (External or Managed) to pick our upgrade strategy:
-- MAGIC
-- MAGIC * Copy the data for managed location or external location in our root cloud storage
-- MAGIC * Keep data and leverage EXTERNAL LOCATION for the other table
-- MAGIC * Move non-delta tables to delta table
-- MAGIC * Support view upgrade
-- MAGIC
-- MAGIC This is implemented in the `should_copy_table` function.
-- MAGIC
-- MAGIC The next step is to loop over the tables in the database we want to upgrade and run one of the 2 upgrade SQL queries: `DEEP CLONE` or using the `COPY LOCATION`.
-- MAGIC
-- MAGIC *Note: Remember that you'll have to setup your `EXTERNAL LOCATION` + `CREDENTIAL` for `COPY LOCATION` to work with UC.*

-- COMMAND ----------

-- We want to upgrade these 2 tables in uc_database_to_upgrade. Note that we have a view
SHOW TABLES IN hive_metastore.uc_database_to_upgrade;

-- COMMAND ----------

-- DBTITLE 1,Cleanup our UC for a fresh upgrade
DROP DATABASE IF EXISTS dbdemos.database_upgraded_on_uc CASCADE

-- COMMAND ----------

-- DBTITLE 1,Detect table type to define the strategy type (clone or use external location)
-- MAGIC %python
-- MAGIC
-- MAGIC #Return true if the table is either managed or using the the root cloud storage, or using a Azure blob storage.
-- MAGIC def should_copy_table(table_name):
-- MAGIC   managed = False
-- MAGIC   is_view = False
-- MAGIC   is_delta = False
-- MAGIC   location = None
-- MAGIC   for r in spark.sql(f'DESCRIBE EXTENDED {table_name}').collect():
-- MAGIC     if r['col_name'] == 'Provider' and r['data_type'] == 'delta':
-- MAGIC       is_delta = True
-- MAGIC     if r['col_name'] == 'Type' and r['data_type'] == 'VIEW':
-- MAGIC       is_view = True
-- MAGIC     if r['col_name'] == 'Is_managed_location' and r['data_type'] == 'true':
-- MAGIC       managed = True
-- MAGIC     if r['col_name'] == 'Location':
-- MAGIC       location = r['data_type']
-- MAGIC   is_root_storage = location is not None and (location.startswith('dbfs:/') or location.startswith('wasb')) and not location.startswith('dbfs:/mnt/') 
-- MAGIC   should_copy = is_root_storage or managed
-- MAGIC   return location, is_view, is_delta, should_copy
-- MAGIC
-- MAGIC # should_copy as it's a managed table 
-- MAGIC print(should_copy_table("hive_metastore.uc_database_to_upgrade.users"))
-- MAGIC # don't copy as it's an external table 
-- MAGIC print(should_copy_table("hive_metastore.uc_database_to_upgrade.transactions"))
-- MAGIC # The last one is a view
-- MAGIC print(should_copy_table("hive_metastore.uc_database_to_upgrade.users_view_to_upgrade"))

-- COMMAND ----------

-- DBTITLE 1,Loop over the tables and upgrade them to UC
-- MAGIC %python
-- MAGIC from delta.tables import *
-- MAGIC import re
-- MAGIC
-- MAGIC #Copy the data or reuse external location when possible.
-- MAGIC def upgrade_table(full_table_name_source, full_table_name_destination, owner_to, privilege, privilege_principal):
-- MAGIC   location, is_view, is_delta, should_copy = should_copy_table(full_table_name_source)
-- MAGIC   if is_view:
-- MAGIC     return False
-- MAGIC   if should_copy:
-- MAGIC     print("Managed table or table in root workspace bucket. Will run a full new copy.")
-- MAGIC     if is_delta:
-- MAGIC       spark.sql(f'CREATE OR REPLACE TABLE {full_table_name_destination} DEEP CLONE {full_table_name_source}')
-- MAGIC     else :
-- MAGIC       spark.read.table(full_table_name_source).write.mode('overwrite').save(full_table_name_destination)
-- MAGIC       clone_table_properties(full_table_name_source, full_table_name_destination)
-- MAGIC   else:
-- MAGIC     print(f'Migrating table {full_table_name_source} to UC database {full_table_name_destination} using the same location')
-- MAGIC     print(f'This requires an EXTERNAL LOCATION with valid CREDENTIAL exists for location {location}')
-- MAGIC     #only copy the data. Make sure an EXTERNAL LOCATION exists to the destination.
-- MAGIC     spark.sql(f'CREATE TABLE IF NOT EXISTS {full_table_name_destination} LIKE {full_table_name_source} COPY LOCATION')
-- MAGIC     #Uncomment when ES-417481 is solved 
-- MAGIC     #clone_table_properties(full_table_name_source, full_table_name_destination)
-- MAGIC   if owner_to is not None:
-- MAGIC     spark.sql(f'ALTER TABLE {full_table_name_destination} OWNER TO `{owner_to}`')
-- MAGIC   if privilege is not None:
-- MAGIC     spark.sql(f'GRANT {privilege} ON TABLE {full_table_name_destination} TO `{privilege_principal}`');
-- MAGIC   return True
-- MAGIC
-- MAGIC #copy the table properties from the source to the new one
-- MAGIC def clone_table_properties(full_table_name_source, full_table_name_destination):  
-- MAGIC   tbl_properties = []
-- MAGIC   for r in spark.sql(f'SHOW TBLPROPERTIES {full_table_name_source}').collect():
-- MAGIC     if r['key'] != 'Type':
-- MAGIC       tbl_properties.append(f"'{r['key']}' = '{r['value']}'")  
-- MAGIC   properties = ','.join(tbl_properties)
-- MAGIC   print(f'Alter table with source properties: {properties}.')
-- MAGIC   sqlContext.sql(f"ALTER TABLE {full_table_name_destination} SET TBLPROPERTIES({properties})")
-- MAGIC
-- MAGIC #upgrade the views (at the end)   
-- MAGIC def upgrade_views(full_table_name_source, database_to_upgrade, catalog_destination, database_destination, views, owner_to, privilege, privilege_principal):
-- MAGIC   for table_name in views:
-- MAGIC     view_definition = spark.sql(f"describe extended {full_table_name_source}").where("col_name = 'View Text'").collect()[0]['data_type']
-- MAGIC     view_definition = re.sub(rf"(`?hive_metastore`?\.`?{database_to_upgrade}`?)", f"`{catalog_destination}`.`{database_destination}`", view_definition)
-- MAGIC     print(f"creating view {table_name} as {view_definition}")
-- MAGIC     spark.sql(f"CREATE OR REPLACE VIEW `{catalog_destination}`.`{database_destination}`.`{table_name}` AS {view_definition}")
-- MAGIC     if owner_to is not None:
-- MAGIC       spark.sql(f'ALTER VIEW `{catalog_destination}`.`{database_destination}`.`{table_name}` OWNER TO `{owner_to}`')
-- MAGIC     if privilege is not None and "SELECT" in privilege or "ALL PRIVILEGES" in privilege:
-- MAGIC       spark.sql(f'GRANT SELECT ON VIEW `{catalog_destination}`.`{database_destination}`.`{table_name}` TO `{privilege_principal}`');
-- MAGIC
-- MAGIC def upgrade_database(database_to_upgrade, catalog_destination, database_destination = None, 
-- MAGIC                      table_owner_to = None, table_privilege = None, table_privilege_principal = None,
-- MAGIC                      database_owner_to = None, database_privilege = None, database_privilege_principal = None):
-- MAGIC   """Move all tabes from one databse to the UC.
-- MAGIC   Args:
-- MAGIC       database_to_upgrade (str):          database source to upgrade (in hive_metastore)
-- MAGIC       catalog_destination (str):          catalog destination (in unity catalog)
-- MAGIC       database_destination (str):         name of the destibation database. If not defined will use the same as the source.
-- MAGIC       table_owner_to (str):               Principal Owner of the tables (default is None)
-- MAGIC       table_privilege  (str):             Privilege to be applyed ("ALL PRIVILEGE" or "SELECT, UPDATE") of the tables (default is None)
-- MAGIC       table_privilege_principal (str):    Principal of the privilege (default is None)
-- MAGIC       database_owner_to (str):            Principal Owner of the database (default is None)
-- MAGIC       database_privilege (str):           Privilege to be applyed ("ALL PRIVILEGE" or "SELECT, UPDATE") of the database (default is None)
-- MAGIC       database_privilege_principal (str): Principal of the privilege (default is None)
-- MAGIC   """
-- MAGIC   if table_privilege is not None or table_privilege_principal is not None:
-- MAGIC     assert table_privilege is not None and table_privilege_principal is not None, "Error: both table_privilege and table_privilege_principal must be set"
-- MAGIC   if database_privilege is not None or database_privilege_principal is not None:
-- MAGIC     assert database_privilege is not None and database_privilege_principal is not None, "Error: both database_privilege and database_privilege_principal must be set"
-- MAGIC     
-- MAGIC   #First create the new CATALOG in UC if it doesn't exist.
-- MAGIC   spark.sql(f'CREATE CATALOG IF NOT EXISTS `{catalog_destination}`')
-- MAGIC   #Then we create the database in the new UC catalog:
-- MAGIC   if database_destination == None:
-- MAGIC     database_destination = database_to_upgrade
-- MAGIC   spark.sql(f'CREATE DATABASE IF NOT EXISTS `{catalog_destination}`.`{database_destination}`')
-- MAGIC   if database_owner_to is not None:
-- MAGIC     spark.sql(f'ALTER DATABASE `{catalog_destination}`.`{database_destination}` OWNER TO `{database_owner_to}`')
-- MAGIC   if database_privilege is not None:
-- MAGIC     spark.sql(f'GRANT {database_privilege} ON DATABASE `{catalog_destination}`.`{database_destination}` TO `{database_privilege_principal}`');
-- MAGIC
-- MAGIC   print(f'Migrating database under `hive_metastore`.`{database_to_upgrade}` to UC database `{catalog_destination}`.`{database_destination}`.')
-- MAGIC   #Now iterate over all the table to synch them with the UC databse
-- MAGIC   views = []
-- MAGIC   for row in spark.sql(f"SHOW TABLES IN hive_metastore.{database_to_upgrade}").collect():
-- MAGIC     table_name = row['tableName']
-- MAGIC     full_table_name_source = f'`hive_metastore`.`{database_to_upgrade}`.`{table_name}`'
-- MAGIC     full_table_name_destination = f'`{catalog_destination}`.`{database_destination}`.`{table_name}`'
-- MAGIC     if not upgrade_table(full_table_name_source, full_table_name_destination, table_owner_to, table_privilege, table_privilege_principal):
-- MAGIC       views.append(table_name)
-- MAGIC   #Once all tables have been created, loop over the views (we need table to be  as the view depend of them)
-- MAGIC   upgrade_views(full_table_name_source, database_to_upgrade, catalog_destination, database_destination, views, table_owner_to, table_privilege, table_privilege_principal)
-- MAGIC   
-- MAGIC #let's migrate the table and ensure all users will be able to use them for our demo
-- MAGIC upgrade_database(database_to_upgrade = 'uc_database_to_upgrade', catalog_destination = "dbdemos", database_destination= 'database_upgraded_on_uc', 
-- MAGIC                  table_owner_to = 'account users',    table_privilege = 'ALL PRIVILEGES',    table_privilege_principal = 'account users',
-- MAGIC                  database_owner_to = 'account users', database_privilege = 'ALL PRIVILEGES', database_privilege_principal = 'account users')

-- COMMAND ----------

-- That's it, our external table, managed table and view have been upgdraded to UC:
SHOW TABLES IN database_upgraded_on_uc;

-- COMMAND ----------

-- DBTITLE 1,Migrate all the databases at once
-- MAGIC %python
-- MAGIC from concurrent.futures import ThreadPoolExecutor
-- MAGIC from collections import deque
-- MAGIC
-- MAGIC def parallel_migrate(database_name):
-- MAGIC   print(f'moving database hive_metastore.{database_name} to UC dbdemos.{database_name} ...')
-- MAGIC   #PLEASE DO NOT RUN this in our env as it'll actually move all the database.
-- MAGIC   #migrate_database(database_name, "dbdemos") , ...optional: add table owners and permission option here...
-- MAGIC   
-- MAGIC print('moving all databases to the UC:')
-- MAGIC databases = [row['databaseName'] for row in spark.sql(f"SHOW DATABASES IN hive_metastore").collect()]
-- MAGIC #upgrades 3 databases in parallel to speedup migration
-- MAGIC with ThreadPoolExecutor(max_workers=3) as executor:
-- MAGIC     deque(executor.map(parallel_migrate, databases))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Advanced upgrade script - expert mode
-- MAGIC
-- MAGIC Let's finish with a complete upgrade script allowing for more flexibility.
-- MAGIC
-- MAGIC We want to support the following options:
-- MAGIC
-- MAGIC * Option to force data copy for all tables, regardless of source table (managed or external)
-- MAGIC * Safe to run multiple times:
-- MAGIC   * Skip table if the table is already upgraded and no data have been added to the tables since the last run
-- MAGIC   * Support incremental copy option: when run multiple time, will incrementally synchronize the new data without recopying everyting (using spark streaming)
-- MAGIC   * detect any new table added
-- MAGIC * Add table optimization on all the tables (auto compaction and optimize write)

-- COMMAND ----------

-- DBTITLE 1,Cleanup for a fresh upgrade
-- MAGIC %python
-- MAGIC spark.sql(f'DROP DATABASE IF EXISTS dbdemos.database_upgraded_on_uc CASCADE')
-- MAGIC dbutils.fs.rm("/dbdemos/uc/upgrade/checkpoint/", True)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The following script can be copy-pasted and used directly:

-- COMMAND ----------

-- DBTITLE 1,Advanced upgrade script supporting incremental load - FULL SCRIPT
-- MAGIC %python
-- MAGIC
-- MAGIC #Return true if the table is either managed or using the the root cloud storage, or using a Azure blob storage.
-- MAGIC def should_copy_table(table_name):
-- MAGIC   managed = False
-- MAGIC   is_view = False
-- MAGIC   is_delta = False
-- MAGIC   location = None
-- MAGIC   for r in spark.sql(f'DESCRIBE EXTENDED {table_name}').collect():
-- MAGIC     if r['col_name'] == 'Provider' and r['data_type'] == 'delta':
-- MAGIC       is_delta = True
-- MAGIC     if r['col_name'] == 'Type' and r['data_type'] == 'VIEW':
-- MAGIC       is_view = True
-- MAGIC     if r['col_name'] == 'Is_managed_location' and r['data_type'] == 'true':
-- MAGIC       managed = True
-- MAGIC     if r['col_name'] == 'Location':
-- MAGIC       location = r['data_type']
-- MAGIC   is_root_storage = location is not None and (location.startswith('dbfs:/') or location.startswith('wasb')) and not location.startswith('dbfs:/mnt/') 
-- MAGIC   should_copy = is_root_storage or managed
-- MAGIC   return location, is_view, is_delta, should_copy
-- MAGIC
-- MAGIC #copy the table properties from the source to the new one
-- MAGIC def clone_table_properties(full_table_name_source, full_table_name_destination):  
-- MAGIC   tbl_properties = []
-- MAGIC   for r in spark.sql(f'SHOW TBLPROPERTIES {full_table_name_source}').collect():
-- MAGIC     if r['key'] != 'Type':
-- MAGIC       tbl_properties.append(f"'{r['key']}' = '{r['value']}'")  
-- MAGIC   properties = ','.join(tbl_properties)
-- MAGIC   print(f'Alter table with source properties: {properties}.')
-- MAGIC   sqlContext.sql(f"ALTER TABLE {full_table_name_destination} SET TBLPROPERTIES({properties})")
-- MAGIC   
-- MAGIC   
-- MAGIC #Fully copy the data to the destination. Won't do anything if table exists and no new data have been added in-between.
-- MAGIC def copy_table(full_table_name_source, full_table_name_destination, catalog_destination, database_destination, table_name, is_delta, overwrite_if_exists):
-- MAGIC   table_exists = spark.sql(f'show tables in `{catalog_destination}`.`{database_destination}`').filter(F.col("tableName") == table_name).count() > 0
-- MAGIC   if not table_exists or overwrite_if_exists:
-- MAGIC     if is_delta:
-- MAGIC       spark.sql(f'CREATE OR REPLACE TABLE {full_table_name_destination} DEEP CLONE {full_table_name_source}')
-- MAGIC     else:
-- MAGIC       spark.read.table(full_table_name_source).write.mode('overwrite').saveAsTable(full_table_name_destination)
-- MAGIC       clone_table_properties(full_table_name_source, full_table_name_destination)
-- MAGIC   else:
-- MAGIC     print("Table already existing. Won't do anything. Set overwrite_if_exists=True to force a new recopy")
-- MAGIC
-- MAGIC     
-- MAGIC #Copy the table. Will use incremental load if checkpoint location is not None. 
-- MAGIC #If table data has been updated (DELETE/UPADTE) or checkpoint_location is None it will perform a full copy. 
-- MAGIC #Full copy won't do anything if no new data have been added in-between.
-- MAGIC def incremental_load_or_full_table_copy(full_table_name_source, full_table_name_destination, catalog_destination, 
-- MAGIC                                         database_destination, table_name, is_delta, checkpoint_location, overwrite_if_exists):
-- MAGIC   print(f'Migrating table {full_table_name_source} to UC database {full_table_name_destination} as a copy')
-- MAGIC   if checkpoint_location is not None:
-- MAGIC     print(f'Incrementally migrating table {full_table_name_source} to {full_table_name_destination}, please wait while data is moved...')
-- MAGIC     try:
-- MAGIC       (spark.readStream
-- MAGIC               .table(full_table_name_source)
-- MAGIC            .writeStream
-- MAGIC               .option('checkpointLocation', f'{checkpoint_location}/{database_destination}/{table_name}')
-- MAGIC               .trigger(availableNow = True)
-- MAGIC               .table(full_table_name_destination).awaitTermination())
-- MAGIC       #Streaming won't clone the table properties. Let's force them
-- MAGIC       clone_table_properties(full_table_name_source, full_table_name_destination)
-- MAGIC     except Exception as e:
-- MAGIC       if "Detected a data update" in str(e):
-- MAGIC         print(f"ERROR: Couldn't incrementally load table {full_table_name_source} has data has been updated. Running full copy instead.")
-- MAGIC         copy_table(full_table_name_source, full_table_name_destination, catalog_destination, database_destination, table_name, is_delta, True)
-- MAGIC       else:
-- MAGIC         raise e
-- MAGIC   else:
-- MAGIC     copy_table(full_table_name_source, full_table_name_destination, catalog_destination, database_destination, table_name, is_delta, overwrite_if_exists)
-- MAGIC     
-- MAGIC       
-- MAGIC #Copy the data or reuse external location when possible.
-- MAGIC def upgrade_table_advanced(full_table_name_source, full_table_name_destination, catalog_destination, database_destination, table_name, continue_on_exception,
-- MAGIC                            copy_all_data, checkpoint_location, overwrite_if_exists, owner_to, privilege, privilege_principal):
-- MAGIC   location, is_view, is_delta, should_copy = should_copy_table(full_table_name_source)
-- MAGIC   if is_view:
-- MAGIC     return "VIEW"
-- MAGIC   try:
-- MAGIC     #Delta table can have smarter upgrade path
-- MAGIC     if should_copy or copy_all_data:
-- MAGIC       incremental_load_or_full_table_copy(full_table_name_source, full_table_name_destination, catalog_destination, database_destination, table_name, 
-- MAGIC                                           is_delta, checkpoint_location, overwrite_if_exists)
-- MAGIC     else:
-- MAGIC       print(f'Migrating table {full_table_name_source} to UC database {full_table_name_destination} using the same location')
-- MAGIC       #print(f'this requires an EXTERNAL LOCATION with valid CREDENTIAL exists for location {location}')
-- MAGIC       #only copy the data. Make sure an EXTERNAL LOCATION exists to the destination.
-- MAGIC       spark.sql(f'CREATE TABLE IF NOT EXISTS {full_table_name_destination} LIKE {full_table_name_source} COPY LOCATION')
-- MAGIC     #Uncomment when ES-417481 is solved 
-- MAGIC     #clone_table_properties(full_table_name_source, full_table_name_destination)
-- MAGIC     if owner_to is not None:
-- MAGIC       spark.sql(f'ALTER TABLE {full_table_name_destination} OWNER TO `{owner_to}`')
-- MAGIC     if privilege is not None:
-- MAGIC       spark.sql(f'GRANT {privilege} ON TABLE {full_table_name_destination} TO `{privilege_principal}`');
-- MAGIC   except Exception as e:
-- MAGIC     if continue_on_exception:
-- MAGIC       print(f"ERROR UPGRADING TABLE {full_table_name_source}: {str(e)}. Continue.")
-- MAGIC       return "ERROR"
-- MAGIC     else:
-- MAGIC       raise e
-- MAGIC   return "SUCCESS"
-- MAGIC
-- MAGIC def upgrade_database_views_advanced(database_to_upgrade, catalog_destination, database_destination = None, databases_upgraded = None,
-- MAGIC                                     continue_on_exception = False, owner_to = None, privilege = None, privilege_principal = None):
-- MAGIC   if database_destination == None:
-- MAGIC     database_destination = database_to_upgrade
-- MAGIC   for row in spark.sql(f"SHOW TABLES IN hive_metastore.{database_to_upgrade}").collect():
-- MAGIC     table_name = row['tableName']
-- MAGIC     full_table_name_source = f'`hive_metastore`.`{database_to_upgrade}`.`{table_name}`'
-- MAGIC     full_table_name_destination = f'`{catalog_destination}`.`{database_destination}`.`{table_name}`'
-- MAGIC     properties = spark.sql(f"describe extended {full_table_name_source}").where("col_name = 'View Text'").collect()
-- MAGIC     if len(properties) > 0:
-- MAGIC       try:
-- MAGIC         view_definition = properties[0]['data_type']
-- MAGIC         #Try to replace all view definition with the one being merged on the new catalog
-- MAGIC         view_definition = re.sub(rf"(`?hive_metastore`?\.`?{database_to_upgrade}`?)", f"`{catalog_destination}`.`{database_destination}`", view_definition)
-- MAGIC         for db_source, db_destibation in databases_upgraded:
-- MAGIC           view_definition = re.sub(rf"(`?hive_metastore`?\.`?{db_source}`?)", f"`{catalog_destination}`.`{db_destibation}`", view_definition)
-- MAGIC         print(f"creating view {table_name} as {view_definition}")
-- MAGIC         spark.sql(f"CREATE OR REPLACE VIEW `{catalog_destination}`.`{database_destination}`.`{table_name}` AS {view_definition}")
-- MAGIC         if owner_to is not None:
-- MAGIC           spark.sql(f'ALTER VIEW `{catalog_destination}`.`{database_destination}`.`{table_name}` OWNER TO `{owner_to}`')
-- MAGIC         if privilege is not None and "SELECT" in privilege or "ALL PRIVILEGES" in privilege:
-- MAGIC           spark.sql(f'GRANT SELECT ON VIEW `{catalog_destination}`.`{database_destination}`.`{table_name}` TO `{privilege_principal}`');
-- MAGIC       except Exception as e:
-- MAGIC         if continue_on_exception:
-- MAGIC           print(f"ERROR UPGRADING VIEW`{database_destination}`.`{table_name}`: {str(e)}. Continue")
-- MAGIC         else:
-- MAGIC           raise e
-- MAGIC         
-- MAGIC def upgrade_database_advanced(database_to_upgrade, catalog_destination, database_destination = None, continue_on_exception = False,
-- MAGIC                               copy_all_data = False, force_table_optimization = True, overwrite_if_exists = False, checkpoint_location = None, 
-- MAGIC                               table_owner_to = None, table_privilege = None, table_privilege_principal = None,
-- MAGIC                               database_owner_to = None, database_privilege = None, database_privilege_principal = None):
-- MAGIC   """Move all tabes from one databse to the UC.
-- MAGIC   Args:
-- MAGIC       database_to_upgrade (str):          database source to upgrade (in hive_metastore)
-- MAGIC       catalog_destination (str):          catalog destination (in unity catalog)
-- MAGIC       database_destination (str):         name of the destibation database. If not defined will use the same as the source.
-- MAGIC       continue_on_exception (bool):       Will stop if we encounter an error (ex: external loc doesn't exist). When set to True will print a message and continue. 
-- MAGIC                                           (default is False) 
-- MAGIC       copy_all_data (bool):               when set to False, will only copy managed tables or tables in the root bucket, reuse the location for the others. 
-- MAGIC                                           When True will recopy all data (default is False)
-- MAGIC       force_table_optimization (bool):    when set to True sets delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true.
-- MAGIC                                           (default is True)
-- MAGIC       overwrite_if_exists (bool):         when set to False, won't overwrite the table if it already exists (if no checkpointLocation=no incremental is used)
-- MAGIC       table_owner_to (str):               Principal Owner of the tables (default is None)
-- MAGIC       table_privilege  (str):             Privilege to be applyed ("ALL PRIVILEGE" or "SELECT, UPDATE") of the tables (default is None)
-- MAGIC       table_privilege_principal (str):    Principal of the privilege (default is None)
-- MAGIC       database_owner_to (str):            Principal Owner of the database (default is None)
-- MAGIC       database_privilege (str):           Privilege to be applyed ("ALL PRIVILEGE" or "SELECT, UPDATE") of the database (default is None)
-- MAGIC       database_privilege_principal (str): Principal of the privilege (default is None)
-- MAGIC   """      
-- MAGIC   if table_privilege is not None or table_privilege_principal is not None:
-- MAGIC     assert table_privilege is not None and table_privilege_principal is not None, "Error: both table_privilege and table_privilege_principal must be set"
-- MAGIC   if database_privilege is not None or database_privilege_principal is not None:
-- MAGIC     assert database_privilege is not None and database_privilege_principal is not None, "Error: both database_privilege and database_privilege_principal must be set"
-- MAGIC
-- MAGIC   #First create the new CATALOG in UC if it doesn't exist.
-- MAGIC   spark.sql(f'CREATE CATALOG IF NOT EXISTS `{catalog_destination}`')
-- MAGIC   #Then we create the database in the new UC catalog:
-- MAGIC   if database_destination == None:
-- MAGIC     database_destination = database_to_upgrade
-- MAGIC   spark.sql(f'CREATE DATABASE IF NOT EXISTS `{catalog_destination}`.`{database_destination}`')
-- MAGIC   
-- MAGIC   print(f'Migrating database under `hive_metastore`.`{database_to_upgrade}` to UC database `{catalog_destination}`.`{database_destination}`.')
-- MAGIC   #Now iterate over all the table to synch them with the UC databse
-- MAGIC   errors = []
-- MAGIC   tables_migrated = []
-- MAGIC   for row in spark.sql(f"SHOW TABLES IN hive_metastore.{database_to_upgrade}").collect():
-- MAGIC     table_name = row['tableName']
-- MAGIC     full_table_name_source = f'`hive_metastore`.`{database_to_upgrade}`.`{table_name}`'
-- MAGIC     full_table_name_destination = f'`{catalog_destination}`.`{database_destination}`.`{table_name}`'
-- MAGIC     status = upgrade_table_advanced(full_table_name_source, full_table_name_destination, catalog_destination, database_destination, table_name, continue_on_exception,
-- MAGIC                                      copy_all_data, checkpoint_location, overwrite_if_exists, table_owner_to, table_privilege, table_privilege_principal)
-- MAGIC     if status == "ERROR":
-- MAGIC       errors.append((database_to_upgrade, table_name))
-- MAGIC     elif status == "SUCCESS":
-- MAGIC       tables_migrated.append((full_table_name_source, full_table_name_destination))
-- MAGIC       # Extra: While we move our tables to UC, let's make sure auto-compaction and optimized writes are enable in all our UC tables to remove any small files issue
-- MAGIC       # Disable for now, see ES-417481
-- MAGIC       # if force_table_optimization:
-- MAGIC       #   spark.sql(f'ALTER TABLE {full_table_name_destination} SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)')
-- MAGIC   print("database upgrade completed. Please run upgrade views to complete your migration (views depends on tables and require to be run last)")
-- MAGIC   return (database_to_upgrade, database_destination), tables_migrated, errors
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC #---------------------------------------------------#
-- MAGIC #------------Calling the migration script ----------#
-- MAGIC #---------------------------------------------------#
-- MAGIC databases_upgraded = []
-- MAGIC #First migrate all the tables
-- MAGIC db, tables_migrated, errors = upgrade_database_advanced(database_to_upgrade = 'uc_database_to_upgrade', catalog_destination = "dbdemos", database_destination = 'database_upgraded_on_uc', 
-- MAGIC                                   continue_on_exception = False, checkpoint_location = "/dbdemos/uc/upgrade/checkpoint/", force_table_optimization = True,
-- MAGIC                                   table_owner_to = 'account users',    table_privilege = 'ALL PRIVILEGES',    table_privilege_principal = 'account users',
-- MAGIC                                   database_owner_to = 'account users', database_privilege = 'ALL PRIVILEGES', database_privilege_principal = 'account users')
-- MAGIC databases_upgraded.append(db)
-- MAGIC assert len(errors) == 0 , "something is wrong, please check error message" 
-- MAGIC print(databases_upgraded)
-- MAGIC #Once all tables have been created, loop over the views (we need table to be upgraded first as the view depend of them, potentially cross-database)
-- MAGIC upgrade_database_views_advanced(database_to_upgrade = 'uc_database_to_upgrade', catalog_destination = "dbdemos", database_destination = 'database_upgraded_on_uc', 
-- MAGIC                                 databases_upgraded = databases_upgraded, continue_on_exception = False, 
-- MAGIC                                 owner_to = 'account users', privilege = 'ALL PRIVILEGES', privilege_principal = 'account users')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Migrate ALL YOUR DATABASES to UC
-- MAGIC Let's see how to run this in parallel and make sure we move all the databases first and the the views:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f'DROP DATABASE IF EXISTS dbdemos.database_upgraded_on_uc CASCADE')
-- MAGIC dbutils.fs.rm("/dbdemos/uc/upgrade/checkpoint/", True)

-- COMMAND ----------

-- DBTITLE 1,Migrate ALL YOUR DATABASES to UC
-- MAGIC %python
-- MAGIC from concurrent.futures import ThreadPoolExecutor
-- MAGIC from collections import deque
-- MAGIC
-- MAGIC databases_upgraded = []
-- MAGIC #---------------------------------------------------#
-- MAGIC #--------------- First upgrade tables  -------------#
-- MAGIC #---------------------------------------------------#
-- MAGIC def parallel_migrate(database_name):
-- MAGIC   #PLEASE DO NOT RUN this in our env as it'll actually move all the database.
-- MAGIC   #remove me for real run, this mock the results to avoid doing a real migration in our demo env
-- MAGIC   if database_name != "uc_database_to_upgrade":
-- MAGIC     return (database_name, database_name), [], []
-- MAGIC   else:
-- MAGIC     print(f'moving database hive_metastore.{database_name} to UC dbdemos.{database_name} ...')
-- MAGIC     return upgrade_database_advanced(database_to_upgrade = database_name, catalog_destination = "dbdemos", database_destination = database_name, 
-- MAGIC                                     continue_on_exception = False, checkpoint_location = "/dbdemos/uc/upgrade/checkpoint/", force_table_optimization = True,
-- MAGIC                                     table_owner_to = 'account users',    table_privilege = 'ALL PRIVILEGES',    table_privilege_principal = 'account users',
-- MAGIC                                     database_owner_to = 'account users', database_privilege = 'ALL PRIVILEGES', database_privilege_principal = 'account users')
-- MAGIC
-- MAGIC print('moving all databases to the UC:')
-- MAGIC databases = [row['databaseName'] for row in spark.sql(f"SHOW DATABASES IN hive_metastore").collect()]
-- MAGIC #upgrades 3 databases in parallel to speedup migration
-- MAGIC with ThreadPoolExecutor(max_workers=3) as executor:
-- MAGIC     for db, tables_migrated, errors in executor.map(parallel_migrate, databases):
-- MAGIC       databases_upgraded.append(db)
-- MAGIC       
-- MAGIC #---------------------------------------------------#
-- MAGIC #--------------- Then upgrade views  ---------------#
-- MAGIC #---------------------------------------------------#
-- MAGIC def parallel_view_migrate(database_name):
-- MAGIC   #PLEASE DO NOT RUN this in our env as it'll actually move all the database.
-- MAGIC   #remove me for real run, this limit to 1 database to avoid doing a real migration in our demo env
-- MAGIC   if database_name == "uc_database_to_upgrade":
-- MAGIC     print(f'moving views hive_metastore.{database_name} to UC dbdemos.{database_name} ...')
-- MAGIC     upgrade_database_views_advanced(database_to_upgrade = database_name, catalog_destination = "dbdemos", database_destination = database_name, 
-- MAGIC                                     databases_upgraded = databases_upgraded, continue_on_exception = False, 
-- MAGIC                                     owner_to = 'account users', privilege = 'ALL PRIVILEGES', privilege_principal = 'account users')
-- MAGIC with ThreadPoolExecutor(max_workers=3) as executor:
-- MAGIC   deque(executor.map(parallel_view_migrate, databases))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conclusion
-- MAGIC
-- MAGIC Unity Catalog can easily be added as an addition to your workspace-level databases.
-- MAGIC
-- MAGIC You can easily upgrade your table using the UI or with simple SQL command.
-- MAGIC
-- MAGIC For more detailed upgrade, you can leverage a more advanced upgrade script or contact your account team for more help.
