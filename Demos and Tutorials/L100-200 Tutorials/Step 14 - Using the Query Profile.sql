-- Databricks notebook source
-- QUERY PROFILE DEMO
-- SHOWS HOW TO SPOT AND RESOLVE BOTTLENECKS AND PERFORMANCE CHALLNEGES IN DBSQL WITH QUERY PROFILE

/* DEMO FLOW

1. Show 4 Unoptimized Queries and Assess the query profile
2. Optimize the 4 queries for different needs with PARTITION, ZORDER, CLUSTER BY (optional)
3. Show Updated Query Profiles and improved bottlenecks


Query Profile Things to Look for at the top level in the profile:

1. % Time in Photon - top level 
2. % execution time vs optimizing/pruning files
3. Spilling - If ANY, than query is very bad or cluster is too small

Node-level things to look for:

1. Dark colored nodes -- indicate where time/effort is going 
2. Large arrows -- indicate high data transfer across nodes
3. File / Partition Pruning metrics
4. Runtime for each node (correlated to darkness of color)


*/
USE CATALOG main;
USE DATABASE tpc_edw_demo;


--============================================================================--
/*
Step 1: Start with unoptimized data model

Look at 4 queries: 
1. Single point lookups -- Specific trade id -- File pruning bottleneck
2. Big Joins and Large Selects -- Showing different types of bottleneck -- Shuffle
2. Range lookups -- Analytics for a specific date range -- Complex file pruning and shuffle!
3. Aggregates -- Aggregates on a specific date range - more complex question -- More complex query plans

*/


--===== QUERY 1: Point selection -- look for specific trade id

SELECT
h.currenttradeid,
h.currentprice,
h.currentholding,
h.currentholding*h.currentprice AS CurrentMarketValue,
c.lastname,
c.firstname,
c.status
FROM main.tpc_edw_demo.factholdings h
INNER JOIN main.tpc_edw_demo.dimcustomer c ON c.sk_customerid = h.sk_customerid
INNER JOIN main.tpc_edw_demo.dimcompany comp ON comp.sk_companyid = h.sk_companyid
WHERE h.currenttradeid = 527764963
AND c.status = 'Active'

-- Look at unoptimized query profile - No file pruning!

-- Query 1 Profile Output: https://e2-demo-field-eng.cloud.databricks.com/sql/history?lookupKey=CiQwMWVlNzJiMi00MjZhLTEzNGYtODE3OC1hNGNmNjhmOGU1MzIQiLKmnLYxGhA0NzViOTRkZGM3Y2Q1MjExILyFvZC14AQ%3D&o=1444828305810485&uiQueryProfileVisible=true&userId=20904982561468


-- Look at Row and time spent output in the DAG in the query profile -- Fatter arrows mean more data movement and bottlenecks
-- Darker nodes in profile show bottlenecks AUTOMATICALLY

/* Scan Node for factholding

Files pruned	0	
Files read	448	
Number of output batches	449	
Number of output rows	1	
Peak memory usage	3.50	GB
Size of files pruned	0	
Size of files read	49.03	GB
*/

/* Scan Node for dimcompany
Files pruned	0	
Files read	16	
Number of output batches	1,232	
Number of output rows	5,000,000	
Peak memory usage	288.37	MB
Size of files pruned	0	
Size of files read	709.25	MB
*/



--===== QUERY 2: Big Joins and Select

SELECT
h.tradeid,
h.currentprice,
h.currentholding,
h.currentholding*h.currentprice AS CurrentMarketValue,
c.lastname,
c.firstname,
c.status,
*
FROM main.tpc_edw_demo.factholdings h
INNER JOIN main.tpc_edw_demo.dimcustomer c ON c.sk_customerid = h.sk_customerid
INNER JOIN main.tpc_edw_demo.dimcompany comp ON comp.sk_companyid = h.sk_companyid

-- What is the bottleneck here? SHUFFLE
-- Not only lots of rows, but lots of data to shuffle around

-- Query 2 Profile Output: https://e2-demo-field-eng.cloud.databricks.com/sql/history?lookupKey=CiQwMWVlNzJiMy00ZmRjLTE2NjUtYmYwZC05ZWY5NGVlNzJkZjIQ4f3BnLYxGhA0NzViOTRkZGM3Y2Q1MjExILyFvZC14AQ%3D&o=1444828305810485&uiQueryProfileVisible=true&userId=20904982561468



--===== QUERY 3: Range Timestamp/Date Filters

SELECT
to_date(sk_dateid::string, "yyyyMMdd") AS Date,
AVG(h.currentholding*h.currentprice) AS CurrentMarketValue,
MAX(h.currentholding*h.currentprice) AS MaxHoldingValue
FROM main.tpc_edw_demo.factholdings h
INNER JOIN main.tpc_edw_demo.dimcustomer c ON c.sk_customerid = h.sk_customerid
INNER JOIN main.tpc_edw_demo.dimcompany comp ON comp.sk_companyid = h.sk_companyid
WHERE sk_dateid BETWEEN 20130101 AND 20131201
GROUP BY to_date(sk_dateid::string, "yyyyMMdd")
ORDER BY Date


-- What is the bottleneck here? File Pruning on a range
-- Lots of downstream aggregations, we want to minimize records BEFORE those transformations
-- Look at time spent nodes and Rows node

-- Query 3 Profile Output: https://e2-demo-field-eng.cloud.databricks.com/sql/history?lookupKey=CiQwMWVlNzJiNy0xYWU4LTExMjUtOTU3YS1mNjgyMGNhZGEwZmMQuLWlnbYxGhA0NzViOTRkZGM3Y2Q1MjExILyFvZC14AQ%3D&o=1444828305810485&uiQueryProfileVisible=true&userId=20904982561468


--===== QUERY 4: Complex Query Aggregates

-- For a given year, who were the top 10 holding customers?

WITH year_selected_holding AS (

SELECT
h.tradeid,
h.currentprice,
h.currentholding,
h.currentholding*h.currentprice AS CurrentMarketValue,
c.lastname,
c.firstname,
c.status,
comp.name AS company_name,
to_date(sk_dateid::string, "yyyyMMdd") AS Date
FROM main.tpc_edw_demo.factholdings h
INNER JOIN main.tpc_edw_demo.dimcustomer c ON c.sk_customerid = h.sk_customerid
INNER JOIN main.tpc_edw_demo.dimcompany comp ON comp.sk_companyid = h.sk_companyid
WHERE h.sk_dateid BETWEEN 20150101 AND 20151201
)
,
holding_customer_agg AS (

SELECT
CONCAT(lastname, ', ', firstname) AS CustomerName,
SUM(CurrentMarketValue) AS TotalHoldingsValue
FROM year_selected_holding
GROUP BY CONCAT(lastname, ', ', firstname)
),
customer_rank AS (

SELECT
  *,
  DENSE_RANK() OVER (ORDER BY TotalHoldingsValue DESC) AS CustomerRank
FROM holding_customer_agg
)
SELECT * FROM customer_rank ORDER BY CustomerRank LIMIT 10

-- What is bottleneck here? -- SHUFFLE
-- No file pruning happening still

-- Query 4 Profile Output: https://e2-demo-field-eng.cloud.databricks.com/sql/history?lookupKey=CiQwMWVlNzJiOS1jYjU2LTE3YTItYmFiMy0xNjY3YWRlMmRlOTcQ6%2FTrnbYxGhA0NzViOTRkZGM3Y2Q1MjExILyFvZC14AQ%3D&o=1444828305810485&uiQueryProfileVisible=true&userId=20904982561468



--============================================================================--
/*
Step 2: OPTIMIZE / ZORDER the core source tables

Questions to ask: 

1. What did we filter on above? 
2. What is reused in filters? 
3. How can we do smarter joins to reduce those shuffle bottlenecks? 

*/

-- Table: main.tpc_edw_demo.factholdings
-- Columns Used Often in Filters: sk_dateid, currenttradeid

OPTIMIZE main.tpc_edw_demo.factholdings ZORDER BY (sk_dateid, currenttradeid);
-- Large fact table so be careful here, be selective
ANALYZE TABLE main.tpc_edw_demo.factholdings COMPUTE STATISTICS FOR COLUMNS sk_dateid, currenttradeid, sk_customerid, sk_companyid;


-- Table: main.tpc_edw_demo.dimcustomer
-- Columns Used Often in Joins as dim tables: sk_customerid

OPTIMIZE main.tpc_edw_demo.dimcustomer ZORDER BY (sk_customerid);
-- Dim table so not really expensive to calculate
ANALYZE TABLE main.tpc_edw_demo.dimcustomer COMPUTE STATISTICS FOR ALL COLUMNS;


-- Table: main.tpc_edw_demo.dimcompany
-- Columns Used Often in Joins as dim tables: sk_companyid

OPTIMIZE main.tpc_edw_demo.dimcompany ZORDER BY (sk_companyid);
-- Dim table so not really expensive to calculate
ANALYZE TABLE main.tpc_edw_demo.dimcompany COMPUTE STATISTICS FOR ALL COLUMNS;




--============================================================================--
/*
Step 3: Look at updated query profiles! 

Updated Query Profiles re-run from above queries: 



Query 1 Profile: https://e2-demo-field-eng.cloud.databricks.com/sql/history?lookupKey=CiQwMWVlNzJiYi00MWVmLTExNDAtYWIwMi0yZDQwMjNmNWU0MTQQ4KKSnrYxGhA0NzViOTRkZGM3Y2Q1MjExILyFvZC14AQ%3D&o=1444828305810485&uiQueryProfileVisible=true&userId=20904982561468

Original Runtime: 3 seconds
Optimize Runtime: 1.3 seconds

WHAT IS DIFFERENT? -- Pruned MUCH more --  no more scanning all of the table

Files pruned	1,018	
Files read	6	
Number of output batches	2	
Number of output rows	1	
Peak memory usage	98.89	MB
Size of files pruned	47.88	GB
Size of files read	293.59	MB


---===== 

Query 2 Profile: https://e2-demo-field-eng.cloud.databricks.com/sql/history?lookupKey=CiQwMWVlNzJiMy00ZmRjLTE2NjUtYmYwZC05ZWY5NGVlNzJkZjIQ4f3BnLYxGhA0NzViOTRkZGM3Y2Q1MjExILyFvZC14AQ%3D&o=1444828305810485&uiQueryProfileVisible=true&userId=20904982561468

Original Runtime: 5 min 39 seconds
Optimized Runtime: 4 min 36 seconds

WHAT IS DIFFERENT? 
Selecting everything from large tables is just wasteful :/

Shuffle node went down a little bit from 3.7 hours in total runtime to 3.4, but selecting that much data with no filters requires more tuning


---=====
Query 3 Profile: https://e2-demo-field-eng.cloud.databricks.com/sql/history?lookupKey=CiQwMWVlNzJiYy1jOThlLTFiYjAtOWMwNS04NDIwYjA1MTE0M2QQ66%2B6nrYxGhA0NzViOTRkZGM3Y2Q1MjExILyFvZC14AQ%3D&o=1444828305810485&uiQueryProfileVisible=true&userId=20904982561468

Original Runtime: 9.2 seconds, 0 files pruned
Optimized Runtime: 9.3 seconds, many files pruned, but now more work



WHAT IS DIFFERENT? 
Files pruned	822	
Files read	202	
Number of output batches	57,690	
Number of output rows	224,054,102

Look at shuffle



---=====
Query 4 Profile: https://e2-demo-field-eng.cloud.databricks.com/sql/history?lookupKey=CiQwMWVlNzJiZC0wZmZlLTE1ZjgtOWNmNy0xNWU1OTVlYmM1NmQQh8vBnrYxGhA0NzViOTRkZGM3Y2Q1MjExILyFvZC14AQ%3D&o=1444828305810485&uiQueryProfileVisible=true&userId=20904982561468

Original Runtime: 8.5 seconds, 0 files pruned
Optimized Runtime: 12.3 seconds, Many files pruned

NEW BOTTLENECK: SCAN instead of shuffle. 

WHAT IS DIFFERENT?

Files pruned	826	
Files read	198	
Number of output batches	56,824	
Number of output rows	224,077,524	
Peak memory usage	5.76	GB
Size of files pruned	38.86	GB
Size of files read	9.30	GB

LESSON: When doing longer term queries, make sure the file sizing is in proportion to the queries that are run on it. 
There are trade offs for optimizing for a few longer "historical" queries vs many "current" queries.
 Typically current queries are prioritized since you can spin up a serverless backfill cluster. 

*/

