-- Databricks notebook source
USE CATALOG main;
CREATE SCHEMA IF NOT EXISTS sql_max_demo;
USE SCHEMA sql_max_demo;

-- COMMAND ----------

DROP TABLE IF EXISTS bronze_customers;
DROP TABLE IF EXISTS silver_customers;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS bronze_customers;

-- Super simple Raw to Bronze Pipeline
COPY INTO bronze_customers
FROM '/databricks-datasets/retail-org/customers/customers.csv'
FILEFORMAT = csv
FORMAT_OPTIONS ('header'='true')
COPY_OPTIONS('mergeSchema' = 'true', 'force' = 'true');

-- Super simple Bronze to Silver Pipeline
CREATE TABLE IF NOT EXISTS silver_customers AS 
SELECT
*, 'last'::string AS last_name,
'first'::string AS first_name,
'mid'::string AS middle_initial,
'ssn'::string AS ssn, 'phone'::string AS phone_number ,
''::string AS master_customer_id,
current_timestamp() AS update_timestamp
FROM bronze_customers 
WHERE 1=0;

-- Adding sample SSN/Phone Number logic
MERGE INTO silver_customers AS target
USING (
  SELECT 
  *,
    TRIM(SUBSTRING(customer_name, 1, INSTR(customer_name, ',') - 1)) AS last_name,
    TRIM(SUBSTRING(customer_name, INSTR(customer_name, ',') + 1, INSTR(customer_name, ' ') - INSTR(customer_name, ',') - 1)) AS first_name,
    TRIM(SUBSTRING(customer_name, INSTR(customer_name, ' ') + 1)) AS middle_initial,
      -- Generate SSN
    CONCAT(
        LPAD(CAST(FLOOR(RAND() * 1000) AS INT), 3, '0'), '-',  -- Generates a number between 900 and 999
        LPAD(CAST(FLOOR(RAND() * 10 + 10) AS INT), 2, '0'), '-',     -- Generates a number between 10 and 19
        LPAD(CAST(FLOOR(RAND() * 1000 + 1000) AS INT), 4, '0')       -- Generates a number between 1000 and 1999
    ) AS ssn,

    -- Generate Phone Number
    CONCAT(
        '(555)-',
        LPAD(CAST(FLOOR(RAND() * 1000) AS INT), 3, '0'), '-',        -- Generates a number between 000 and 999
        LPAD(CAST(FLOOR(RAND() * 1000) AS INT), 4, '0')              -- Generates a number between 0000 and 9999
    ) AS phone_number,
  
  NULL AS master_customer_id,
  current_timestamp() AS update_timestamp
  FROM bronze_customers

 )
  AS source 
ON source.customer_id = target.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;


-- Truncate Bronze Batch once loaded
--TRUNCATE TABLE bronze_customers;


-- COMMAND ----------

-- DBTITLE 1,Creating some fake customers to try to match
-- Generate Example Various Matches
-- EXample Customer Id: 20560687 - Steven O'leary

WITH new_records AS (
SELECT 9999999999 AS customer_id, 'Steven Leary' AS customer_name, 'FL' AS state, 'Orlando' AS city, null AS postcode, 'Montrose' AS street, 12711 AS `number`, '13004 Montrose Grove, Orlando FL 33579' AS ship_to_address, null AS ssn, null AS phone_number, 'AAAAAAAA' AS master_customer_id,  current_timestamp() AS update_timestamp
)
INSERT INTO silver_customers BY NAME
SELECT * FROM new_records;

WITH new_records AS (
SELECT 1111111111 AS customer_id, 'Steve T. OLeary' AS customer_name, null AS state, 'Orlando' AS city, null AS postcode, 'Montrose' AS street, 12711 AS `number`, '13004 Montrose Grove, Orlando FL' AS ship_to_address, '019-12-1162' AS ssn, '(555)-261-0325' AS phone_number, 'AAAAAAAA' AS master_customer_id,   current_timestamp() AS update_timestamp
)
INSERT INTO silver_customers BY NAME
SELECT * FROM new_records
;


WITH new_records AS (
SELECT 2222222222 AS customer_id, 'Steve T. OLeary' AS customer_name, 'TX' AS state, 'Dallas' AS city, null AS postcode, 'Lone Star' AS street, 1515 AS `number`, '1515 Lone Star Avenue' AS ship_to_address, null AS ssn, '(555)-261-0325' AS phone_number, 'AAAAAAAA' AS master_customer_id,   current_timestamp() AS update_timestamp
)
INSERT INTO silver_customers BY NAME
SELECT * FROM new_records




-- COMMAND ----------

UPDATE silver_customers SET
master_customer_id = 'AAAAAAAA'
WHERE customer_id = 20560687

-- COMMAND ----------

-- DBTITLE 1,Try to generate a performant system to match examples like this one!
SELECT DISTINCT
master_customer_id, customer_id, update_timestamp, customer_name, state, city, postcode, street, number, lon, lat, ship_to_address, last_name, first_name, middle_initial, ssn, phone_number
FROM silver_customers
WHERE customer_id IN (20560687, 9999999999, 1111111111, 2222222222)
ORDER BY update_timestamp DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating a "Best Record" View of multiple records where each record may have different data availability

-- COMMAND ----------

-- DBTITLE 1,Option 1 - Most recent record
--CREATE OR REPLACE VIEW customer_most_recent_record AS
WITH raw_customers AS (
SELECT DISTINCT
master_customer_id, customer_id, update_timestamp, customer_name, state, city, postcode, street, number, lon, lat, ship_to_address, last_name, first_name, middle_initial, ssn, phone_number
FROM silver_customers
WHERE customer_id IN (20560687, 9999999999, 1111111111, 2222222222)
ORDER BY update_timestamp DESC
)
-- Add filtering rules to ensure bad data doesnt make it into the "final" cleaned up master view
SELECT
*
FROM raw_customers
QUALIFY ROW_NUMBER() OVER (PARTITION BY master_customer_id ORDER BY update_timestamp DESC) = 1;



-- COMMAND ----------

-- DBTITLE 1,Option 2 - Group by with MIN/MAX
--CREATE OR REPLACE VIEW customer_most_recent_record AS
WITH raw_customers AS (
SELECT DISTINCT
master_customer_id, customer_id, update_timestamp, customer_name, state, city, postcode, street, number, lon, lat, ship_to_address, last_name, first_name, middle_initial, ssn, phone_number
FROM silver_customers
WHERE customer_id IN (20560687, 9999999999, 1111111111, 2222222222)
ORDER BY update_timestamp DESC
)

SELECT
  master_customer_id,
  MAX(customer_name) AS customer_name,
  MAX(state) AS state,
  MAX(city) AS city,
  MAX(postcode) AS postcode,
  MAX(street) AS street,
  MAX(`number`) AS number,
  MAX(ship_to_address) AS ship_to_address,
  MAX(ssn) AS ssn,
  MAX(phone_number) AS phone_number
FROM raw_customers
GROUP BY master_customer_id;

-- COMMAND ----------

-- DBTITLE 1,Option 3 - Using MAX_BY function
--CREATE OR REPLACE VIEW customer_aggregated AS
WITH raw_customers AS (
SELECT DISTINCT
master_customer_id, customer_id, update_timestamp, customer_name, state, city, postcode, street, number, lon, lat, ship_to_address, last_name, first_name, middle_initial, ssn, phone_number
FROM silver_customers
WHERE customer_id IN (20560687, 9999999999, 1111111111, 2222222222)
ORDER BY update_timestamp DESC
)
-- Add filtering rules to ensure bad data doesnt make it into the "final" cleaned up master view
SELECT
  master_customer_id,
  MAX_BY(customer_name, update_timestamp) FILTER (WHERE customer_name IS NOT NULL) AS customer_name,
  -- Check formatting
  MAX_BY(state, update_timestamp) FILTER (WHERE state IS NOT NULL) AS state,
  MAX_BY(city, update_timestamp) FILTER (WHERE city IS NOT NULL) AS city,
  MAX_BY(postcode, update_timestamp) FILTER (WHERE postcode IS NOT NULL) AS postcode,
  MAX_BY(street, update_timestamp) FILTER (WHERE street IS NOT NULL) AS street,
  MAX_BY(`number`, update_timestamp) FILTER (WHERE `number` IS NOT NULL) AS number,
  MAX_BY(ship_to_address, update_timestamp) FILTER (WHERE ship_to_address IS NOT NULL) AS ship_to_address,
  -- Check formatting for example
  MAX_BY(ssn, update_timestamp) FILTER (WHERE ssn IS NOT NULL AND length(regexp_replace(ssn, '\\D','')) = 9 ) AS ssn,
  MAX_BY(phone_number, update_timestamp) FILTER (WHERE phone_number IS NOT NULL AND length(regexp_replace(phone_number, '\\D','')) = 10) AS phone_number
FROM raw_customers
GROUP BY master_customer_id;


