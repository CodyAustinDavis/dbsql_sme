DECLARE OR REPLACE VARIABLE catalog_name STRING DEFAULT 'main';
DECLARE OR REPLACE VARIABLE schema_name STRING DEFAULT 'default';

SET VAR catalog_name = {{catalog_param}};
SET VAR schema_name = {{schema_param}};

-- 2/ Let's add some data to the Dimension Tables
INSERT INTO
  IDENTIFIER(catalog_name || '.' || schema_name  || '.' || 'dim_store')  
  (store_name, address)
VALUES
  ('City Store', '1 Main Rd, Whoville');
  
INSERT INTO
  IDENTIFIER(catalog_name || '.' || schema_name  || '.' || 'dim_product')  
  (sku, description, category)
VALUES
  ('1000001', 'High Tops', 'Ladies Shoes'),
  ('7000003', 'Printed T', 'Ladies Fashion Tops');
  
INSERT INTO
  IDENTIFIER(catalog_name || '.' || schema_name  || '.' || 'dim_customer')  
  (customer_name, customer_profile, address)
VALUES
  ('Al', 'Al profile', 'Databricks - Queensland Australia'),
  ('Quentin', 'REDACTED_PROFILE', 'Databricks - Paris France');
-- 3/ Let's add some data to the Fact Tables
INSERT INTO
  IDENTIFIER(catalog_name || '.' || schema_name  || '.' || 'fact_sales')  
  (product_id, store_id, customer_id, price_sold, units_sold, dollar_cost)
VALUES
  (1, 1, 0, 100.99, 2, 2.99),
  (2, 1, 0, 10.99, 2, 2.99),
  (1, 1, 0, 100.99, 2, 2.99),
  (1, 1, 10, 100.99, 2, 2.99),
  (2, 1, 10, 10.99, 2, 2.99);