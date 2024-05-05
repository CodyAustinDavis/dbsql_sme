DECLARE OR REPLACE VARIABLE catalog_name STRING DEFAULT 'main';
DECLARE OR REPLACE VARIABLE schema_name STRING DEFAULT 'default';

SET VAR catalog_name = 'main' ; --{{catalog}};
SET VAR schema_name = 'default';  --{{schema}};

USE CATALOG main;
USE SCHEMA default;

--  1/ Create a Dimension & Fact Tables In Unity Catalog
--STORE DIMENSION
--CREATE OR REPLACE  TABLE IDENTIFIER(catalog_name || '.' || schema_name  || '.' || 'dim_store') (
CREATE OR REPLACE  TABLE dim_store (
  store_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  store_name STRING,
  address STRING
);

--PRODUCT DIMENSION
--CREATE OR REPLACE  TABLE IDENTIFIER(catalog_name || '.' || schema_name  || '.' || 'dim_product')  (
CREATE OR REPLACE  TABLE dim_product  (
  product_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sku STRING,
  description STRING,
  category STRING
);

--CUSTOMER DIMENSION
--CREATE OR REPLACE  TABLE IDENTIFIER(catalog_name || '.' || schema_name  || '.' || 'dim_customer') (
CREATE OR REPLACE  TABLE dim_customer (
  customer_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 10) PRIMARY KEY,
  customer_name STRING,
  customer_profile STRING,
  address STRING
);

-- Fact Sales 
--CREATE OR REPLACE TABLE IDENTIFIER(catalog_name || '.' || schema_name  || '.' || 'fact_sales')  (
CREATE OR REPLACE TABLE fact_sales (
  sales_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  product_id BIGINT NOT NULL CONSTRAINT dim_product_fk FOREIGN KEY REFERENCES dim_product,
  store_id BIGINT NOT NULL CONSTRAINT dim_store_fk FOREIGN KEY REFERENCES dim_store,
  customer_id BIGINT NOT NULL CONSTRAINT dim_customer_fk FOREIGN KEY REFERENCES dim_customer,
  price_sold DOUBLE,
  units_sold INT,
  dollar_cost DOUBLE
);