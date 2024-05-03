USE CATALOG MAIN;
USE SCHEMA DEFAULT;

-- ### Query the tables joining data
SELECT * FROM fact_sales
  INNER JOIN dim_product  USING (product_id)
  INNER JOIN dim_customer USING (customer_id)
  INNER JOIN dim_store    USING (store_id)