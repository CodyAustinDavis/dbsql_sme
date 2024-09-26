-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Data Modeling & Performance 101 on Databricks + Delta!
-- MAGIC
-- MAGIC This notebook and workshop will take you through how to use the best of Databricks - all on Databricsk SQL with NO Spark Knowledge Required!
-- MAGIC
-- MAGIC #### The goals of this notebook are:
-- MAGIC  - Build a solid data model with primary keys, foriegn key, and proper clustering keys
-- MAGIC  - Turn on predictive optimization
-- MAGIC  - Run Queries on this model and use the DBSQL Query Profile to check and make sure your tables are skipping data well!

-- COMMAND ----------

-- DBTITLE 1,Build New Custom Data Model in your own env with the modern features
-- Step 1: Create the new schema
DROP SCHEMA IF EXISTS main.model_tpch CASCADE;

CREATE SCHEMA IF NOT EXISTS main.model_tpch;

-- Turn on predictive optimization
ALTER SCHEMA main.model_tpch ENABLE PREDICTIVE OPTIMIZATION;

-- Step 2: Generate DDL statements with primary keys and CLUSTER BY
-- Customer table
CREATE TABLE IF NOT EXISTS main.model_tpch.customer (
    c_custkey INT PRIMARY KEY,
    c_name STRING,
    c_address STRING,
    c_nationkey INT,
    c_phone STRING,
    c_acctbal DECIMAL(18, 2),
    c_mktsegment STRING,
    c_comment STRING
)
CLUSTER BY (c_custkey);

-- Orders table
CREATE TABLE IF NOT EXISTS main.model_tpch.orders (
    o_orderkey INT PRIMARY KEY,
    o_custkey INT,
    o_orderstatus STRING,
    o_totalprice DECIMAL(18, 2),
    o_orderdate DATE,
    o_orderpriority STRING,
    o_clerk STRING,
    o_shippriority INT,
    o_comment STRING
)
CLUSTER BY (o_orderdate, o_custkey);

-- Lineitem table
CREATE TABLE IF NOT EXISTS main.model_tpch.lineitem (
    l_orderkey INT,
    l_partkey INT,
    l_suppkey INT,
    l_linenumber INT,
    l_quantity DECIMAL(18, 2),
    l_extendedprice DECIMAL(18, 2),
    l_discount DECIMAL(18, 2),
    l_tax DECIMAL(18, 2),
    l_returnflag STRING,
    l_linestatus STRING,
    l_shipdate DATE,
    l_commitdate DATE,
    l_receiptdate DATE,
    l_shipinstruct STRING,
    l_shipmode STRING,
    l_comment STRING,
    PRIMARY KEY (l_orderkey, l_linenumber)
)
CLUSTER BY (l_orderkey, l_linenumber);

-- Part table
CREATE TABLE IF NOT EXISTS main.model_tpch.part (
    p_partkey INT PRIMARY KEY,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DECIMAL(18, 2),
    p_comment STRING
)
CLUSTER BY (p_partkey);

-- Partsupp table
CREATE TABLE IF NOT EXISTS main.model_tpch.partsupp (
    ps_partkey INT,
    ps_suppkey INT,
    ps_availqty INT,
    ps_supplycost DECIMAL(18, 2),
    ps_comment STRING,
    PRIMARY KEY (ps_partkey, ps_suppkey)
)
CLUSTER BY (ps_partkey, ps_suppkey);

-- Supplier table
CREATE TABLE IF NOT EXISTS main.model_tpch.supplier (
    s_suppkey INT PRIMARY KEY,
    s_name STRING,
    s_address STRING,
    s_nationkey INT,
    s_phone STRING,
    s_acctbal DECIMAL(18, 2),
    s_comment STRING
)
CLUSTER BY (s_suppkey);

-- Nation table
CREATE TABLE IF NOT EXISTS main.model_tpch.nation (
    n_nationkey INT PRIMARY KEY,
    n_name STRING,
    n_regionkey INT,
    n_comment STRING
)
CLUSTER BY (n_nationkey);

-- Region table
CREATE TABLE IF NOT EXISTS main.model_tpch.region (
    r_regionkey INT PRIMARY KEY,
    r_name STRING,
    r_comment STRING
)
CLUSTER BY (r_regionkey);

-- Step 3: Add foreign key constraints
ALTER TABLE main.model_tpch.customer
ADD CONSTRAINT fk_customer_nation FOREIGN KEY (c_nationkey) REFERENCES main.model_tpch.nation(n_nationkey);

ALTER TABLE main.model_tpch.orders
ADD CONSTRAINT fk_orders_customer FOREIGN KEY (o_custkey) REFERENCES main.model_tpch.customer(c_custkey);

ALTER TABLE main.model_tpch.lineitem 
ADD CONSTRAINT fk_lineitem_orders FOREIGN KEY (l_orderkey) REFERENCES main.model_tpch.orders(o_orderkey);

ALTER TABLE main.model_tpch.lineitem 
ADD CONSTRAINT fk_lineitem_part FOREIGN KEY (l_partkey) REFERENCES main.model_tpch.part(p_partkey);

ALTER TABLE main.model_tpch.lineitem 
ADD CONSTRAINT fk_lineitem_supplier FOREIGN KEY (l_suppkey) REFERENCES main.model_tpch.supplier(s_suppkey);

ALTER TABLE main.model_tpch.partsupp
ADD CONSTRAINT fk_partsupp_part FOREIGN KEY (ps_partkey) REFERENCES main.model_tpch.part(p_partkey);

ALTER TABLE main.model_tpch.partsupp
ADD CONSTRAINT fk_partsupp_supplier FOREIGN KEY (ps_suppkey) REFERENCES main.model_tpch.supplier(s_suppkey);

ALTER TABLE main.model_tpch.supplier
ADD CONSTRAINT fk_supplier_nation FOREIGN KEY (s_nationkey) REFERENCES main.model_tpch.nation(n_nationkey);

ALTER TABLE main.model_tpch.nation
ADD CONSTRAINT fk_nation_region FOREIGN KEY (n_regionkey) REFERENCES main.model_tpch.region(r_regionkey);

-- Step 4: Insert data from source tables into the new tables
INSERT INTO main.model_tpch.customer
SELECT * FROM samples.tpch.customer;

INSERT INTO main.model_tpch.orders
SELECT * FROM samples.tpch.orders;

INSERT INTO main.model_tpch.lineitem
SELECT * FROM samples.tpch.lineitem;

INSERT INTO main.model_tpch.part
SELECT * FROM samples.tpch.part;

INSERT INTO main.model_tpch.partsupp
SELECT * FROM samples.tpch.partsupp;

INSERT INTO main.model_tpch.supplier
SELECT * FROM samples.tpch.supplier;

INSERT INTO main.model_tpch.nation
SELECT * FROM samples.tpch.nation;

INSERT INTO main.model_tpch.region
SELECT * FROM samples.tpch.region;

--- Calcuation of stats that can run very infrequently
ANALYZE TABLE main.model_tpch.customer COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE main.model_tpch.orders COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE main.model_tpch.lineitem COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE main.model_tpch.part COMPUTE STATISTICS FOR ALL COLUMNS;



-- COMMAND ----------

-- DBTITLE 1,Use Schema for rest of session
USE CATALOG main;
USE SCHEMA model_tpch;

-- COMMAND ----------

-- DBTITLE 1,Look At Small Unclustered Table
SELECT * FROM customer

-- COMMAND ----------

SHOW CREATE TABLE customer

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

SHOW CREATE TABLE lineitem

-- COMMAND ----------

DESCRIBE DETAIL lineitem;

-- COMMAND ----------

DESCRIBE EXTENDED lineitem;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Now lets write some queries on this data model!

-- COMMAND ----------

-- DBTITLE 1,Show Predictive IO - Works no matter how table is clustered
SELECT 
    c.c_name, 
    o.o_orderkey, 
    o.o_totalprice, 
    o.o_orderdate
FROM 
    main.model_tpch.customer c
JOIN 
    main.model_tpch.orders o 
ON 
    c.c_custkey = o.o_custkey
WHERE 
    c.c_custkey = 86392;

-- COMMAND ----------

-- DBTITLE 1,Show File Pruning AND Predictive IO Working Together for Larger Tables
SELECT 
    o.o_orderkey, 
    o.o_orderdate, 
    l.l_partkey, 
    l.l_quantity, 
    l.l_extendedprice
FROM 
    main.model_tpch.orders o
JOIN 
    main.model_tpch.lineitem l 
ON 
    o.o_orderkey = l.l_orderkey
WHERE 
    o.o_orderkey = 29138820;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Aggregations + Joins

-- COMMAND ----------

-- DBTITLE 1,Running Manual Optimize
OPTIMIZE main.model_tpch.orders; -- Does clustering and more

-- COMMAND ----------

-- DBTITLE 1,Data Filters Working In Aggregation With Liquid File Skipping
SELECT 
    c.c_name, 
    SUM(o.o_totalprice) AS total_order_price
FROM 
    main.model_tpch.customer c
JOIN 
    main.model_tpch.orders o 
ON 
    c.c_custkey = o.o_custkey
WHERE 
    o.o_orderdate BETWEEN '1992-01-01'::date AND '1993-12-31'::date
GROUP BY 
    c.c_name
ORDER BY 
    total_order_price DESC;

-- COMMAND ----------

-- DBTITLE 1,Optional - Manual or automated - let AI predict the best time to optimize
OPTIMIZE main.model_tpch.customer

-- COMMAND ----------

SELECT 
    p.p_name, 
    AVG(l.l_quantity) AS avg_quantity
FROM 
    main.model_tpch.part p
JOIN 
    main.model_tpch.lineitem l 
ON 
    p.p_partkey = l.l_partkey
JOIN 
    main.model_tpch.orders o 
ON 
    l.l_orderkey = o.o_orderkey
JOIN 
    main.model_tpch.customer c 
ON 
    o.o_custkey = c.c_custkey
WHERE 
    c.c_custkey IN (1, 2, 3)
GROUP BY 
    p.p_name
ORDER BY 
    avg_quantity DESC;

-- COMMAND ----------

OPTIMIZE main.model_tpch.lineitem;

-- COMMAND ----------

-- DBTITLE 1,More Complex Aggregation & Visualization
SELECT 
    o.o_orderkey, 
    SUM(l.l_extendedprice) AS total_extended_price, 
    SUM(l.l_discount) AS total_discount
FROM 
    main.model_tpch.orders o
JOIN 
    main.model_tpch.lineitem l 
ON 
    o.o_orderkey = l.l_orderkey
WHERE 
    o.o_orderdate BETWEEN '1992-01-01'::date AND '1993-01-01'::date
GROUP BY 
    o.o_orderkey
ORDER BY 
    total_extended_price DESC;
