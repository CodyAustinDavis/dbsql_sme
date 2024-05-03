-- Databricks notebook source
DECLARE OR REPLACE VARIABLE catalog_name STRING DEFAULT 'samples';
DECLARE OR REPLACE VARIABLE schema_name STRING DEFAULT 'tpch';

-- COMMAND ----------

-- DBTITLE 1,.sql file tests
--use catalog identifier({{ catalog }});
--use schema {{schema}};

--select * from concat(identifier({{catalog}}), '.', identifier({{schema}})).customer

DECLARE OR REPLACE VARIABLE catalog_name STRING DEFAULT 'samples';
DECLARE OR REPLACE VARIABLE schema_name STRING DEFAULT 'tpch';
DECLARE OR REPLACE VARIABLE table_name STRING DEFAULT {{catalog}} || '.' || {{schema}}  || '.' || {{table}};

select table_name ;
-- SELECT * FROM IDENTIFIER(catalog_name || '.' || schema_name  || '.' || 'customer');

-- SELECT * FROM IDENTIFIER({{catalog}} || '.' || {{schema}}  || '.' || {{table}});

-- SELECT * FROM IDENTIFIER(table_name);

-- COMMAND ----------

USE CATALOG samples;

-- COMMAND ----------

USE schema schema_name

-- COMMAND ----------

-- Get max process timestamp in silver table
DECLARE OR REPLACE VARIABLE watermark_timestamp TIMESTAMP DEFAULT '1900-01-01 00:00:00'::timestamp'

-- COMMAND ----------


use catalog samples;
use schema tpch;

-- COMMAND ----------

DECLARE stmtStr = 'SELECT current_timestamp() + :later, :x * :x AS square';
EXECUTE IMMEDIATE stmtStr USING INTERVAL '3' HOURS AS later, 15.0 AS x;

-- COMMAND ----------

DECLARE mySQL = 'SELECT * from samples.tpch.customer where c_nationkey = :c_nationkey_val and c_mktsegment ="BUILDING"'

-- COMMAND ----------

SET VAR p_nationkey = "21"; 
SET VAR p_mktsegment = "BUILDING"; 
SET VAR mySQL = 'SELECT * from samples.tpch.customer where c_nationkey = VALUES(:c_nationkey_val)'; 
EXECUTE IMMEDIATE mySQL USING "21" as c_nationkey_val;

-- COMMAND ----------

DECLARE mySQL = 'SELECT * from main.samples.customer where c_nationkey = :c_nationkey_val and c_mktsegment =:c_mktsegment_val'; 
DECLARE p_nationkey = "21"; 
DECLARE p_mktsegment = "BUILDING"; 
EXECUTE IMMEDIATE mySQL USING p_nationkey as c_nationkey_val, p_mktsegment as c_mktsegment_val;

-- COMMAND ----------

DECLARE mySQL = 'SELECT * from main.samples.customer where c_nationkey = :c_nationkey_val and c_mktsegment =c_mktsegment_val'

-- COMMAND ----------

SELECT * from samples.tpch.customer where c_nationkey = "21" and c_mktsegment ="BUILDING"

-- COMMAND ----------


