-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Using Row and Column Masks Securely for View and Table Governance

-- COMMAND ----------

DROP FUNCTION IF EXISTS row_filter;

CREATE FUNCTION row_filter(user_id INT, country STRING)
  RETURN
    (SELECT ((v.user_id  = user_id) AND (v.country_map = country))
    FROM user_perms v
    WHERE v.active_user = CURRENT_USER()
    AND v.country_map = country
    LIMIT 1
    )
;

ALTER TABLE cdf_demo_silver_sensors SET ROW FILTER row_filter ON (user_id, country);
