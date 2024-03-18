# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

dbutils.widgets.text("catalog", "dbdemos", "UC Catalog")
dbutils.widgets.text("external_location_path", "s3a://databricks-e2demofieldengwest/external_location_uc_upgrade", "External location path")
external_location_path = dbutils.widgets.get("external_location_path")

# COMMAND ----------

import pyspark.sql.functions as F
import re
catalog = dbutils.widgets.get("catalog")

catalog_exists = False
for r in spark.sql("SHOW CATALOGS").collect():
    if r['catalog'] == catalog:
        catalog_exists = True

#As non-admin users don't have permission by default, let's do that only if the catalog doesn't exist (an admin need to run it first)     
if not catalog_exists:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"ALTER CATALOG {catalog} OWNER TO `account users`")
    spark.sql(f"GRANT CREATE, USAGE on CATALOG {catalog} TO `account users`")
spark.sql(f"USE CATALOG {catalog}")

database = 'database_upgraded_on_uc'
print(f"creating {database} database")
spark.sql(f"DROP DATABASE IF EXISTS {catalog}.{database} CASCADE")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")
spark.sql(f"GRANT CREATE, USAGE on DATABASE {catalog}.{database} TO `account users`")
spark.sql(f"ALTER SCHEMA {catalog}.{database} OWNER TO `account users`")

# COMMAND ----------

folder = "/dbdemos/uc/delta_dataset"
spark.sql('drop database if exists hive_metastore.uc_database_to_upgrade cascade')
#fix a bug from legacy version
spark.sql(f'drop database if exists {catalog}.uc_database_to_upgrade cascade')
dbutils.fs.rm("/transactions", True)

print("generating the data...")
from pyspark.sql import functions as F
from faker import Faker
from collections import OrderedDict 
import uuid
import random
fake = Faker()

fake_firstname = F.udf(fake.first_name)
fake_lastname = F.udf(fake.last_name)
fake_email = F.udf(fake.ascii_company_email)
fake_date = F.udf(lambda:fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S"))
fake_address = F.udf(fake.address)
fake_credit_card_expire = F.udf(fake.credit_card_expire)

fake_id = F.udf(lambda: str(uuid.uuid4()))
countries = ['FR', 'USA', 'SPAIN']
fake_country = F.udf(lambda: countries[random.randint(0,2)])

df = spark.range(0, 10000)
df = df.withColumn("id", F.monotonically_increasing_id())
df = df.withColumn("creation_date", fake_date())
df = df.withColumn("customer_firstname", fake_firstname())
df = df.withColumn("customer_lastname", fake_lastname())
df = df.withColumn("country", fake_country())
df = df.withColumn("customer_email", fake_email())
df = df.withColumn("address", fake_address())
df = df.withColumn("gender", F.round(F.rand()+0.2))
df = df.withColumn("age_group", F.round(F.rand()*10))
df.repartition(3).write.mode('overwrite').format("delta").save(folder+"/users")


df = spark.range(0, 10000)
df = df.withColumn("id", F.monotonically_increasing_id())
df = df.withColumn("customer_id",  F.monotonically_increasing_id())
df = df.withColumn("transaction_date", fake_date())
df = df.withColumn("credit_card_expire", fake_credit_card_expire())
df = df.withColumn("amount", F.round(F.rand()*1000+200))

df = df.cache()
spark.sql('create database if not exists hive_metastore.uc_database_to_upgrade')
df.repartition(3).write.mode('overwrite').format("delta").saveAsTable("hive_metastore.uc_database_to_upgrade.users")

#Note: this requires hard-coded external location.
df.repartition(3).write.mode('overwrite').format("delta").save(external_location_path+"/transactions")

# COMMAND ----------

df.repartition(3).write.mode('overwrite').format("delta").save(external_location_path+"/transactions")

# COMMAND ----------

#Need to switch to hive metastore to avoid having a : org.apache.spark.SparkException: Your query is attempting to access overlapping paths through multiple authorization mechanisms, which is not currently supported.
spark.sql("USE CATALOG hive_metastore")
spark.sql(f"create table if not exists hive_metastore.uc_database_to_upgrade.transactions location '{external_location_path}/transactions'")
spark.sql(f"create or replace view `hive_metastore`.`uc_database_to_upgrade`.users_view_to_upgrade as select * from hive_metastore.uc_database_to_upgrade.users where id is not null")

spark.sql(f"USE CATALOG {catalog}")
