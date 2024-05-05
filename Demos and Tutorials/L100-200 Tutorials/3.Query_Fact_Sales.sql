--USE CATALOG MAIN;
--USE SCHEMA DEFAULT;


DECLARE OR REPLACE VARIABLE catalog_name STRING DEFAULT 'main';
DECLARE OR REPLACE VARIABLE schema_name STRING DEFAULT 'default';

SET VAR catalog_name = {{catalog}};
SET VAR schema_name = {{schema}};


-- ### Query the tables joining data
SELECT * FROM IDENTIFIER(catalog_name || '.' || schema_name  || '.' || 'fact_sales') 
  INNER JOIN IDENTIFIER(catalog_name || '.' || schema_name  || '.' || 'dim_product')   USING (product_id)
  INNER JOIN IDENTIFIER(catalog_name || '.' || schema_name  || '.' || 'dim_customer')  USING (customer_id)
  INNER JOIN IDENTIFIER(catalog_name || '.' || schema_name  || '.' || 'dim_store')     USING (store_id);