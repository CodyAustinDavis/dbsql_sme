-- Databricks notebook source
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


