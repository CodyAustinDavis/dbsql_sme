CREATE CATALOG IF NOT EXISTS main;
CREATE SCHEMA IF NOT EXISTS account_usage_dashboard;

CREATE MATERIALIZED VIEW IF NOT EXISTS main.account_usage_dashboard.workspaces_latest
SCHEDULE EVERY 1 HOUR
AS 
SELECT * FROM system.access.workspaces_latest;

CREATE OR REPLACE MATERIALIZED VIEW main.account_usage_dashboard.usage
SCHEDULE EVERY 1 HOUR
CLUSTER BY (usage_date, workspace_id, sku_name)
AS
-- For serverless DLT, custom_tags is always empty because there is no user-managed
-- cluster to propagate tags through. We fall back to pipeline-level tags from
-- system.lakeflow.pipelines, joined via usage_metadata.dlt_pipeline_id.
WITH pipeline_tags AS (
  SELECT pipeline_id, tags
  FROM (
    SELECT pipeline_id, tags,
           ROW_NUMBER() OVER (PARTITION BY pipeline_id ORDER BY change_time DESC) AS rn
    FROM system.lakeflow.pipelines
    WHERE delete_time IS NULL
  )
  WHERE rn = 1
)
SELECT
  u.account_id,
  u.workspace_id,
  u.record_id,
  u.sku_name,
  u.cloud,
  u.usage_start_time,
  u.usage_end_time,
  u.usage_date,
  CASE
    WHEN u.custom_tags IS NULL OR size(u.custom_tags) = 0 THEN pt.tags
    ELSE u.custom_tags
  END AS custom_tags,
  u.usage_unit,
  u.usage_quantity,
  u.usage_metadata,
  u.identity_metadata,
  u.record_type,
  u.ingestion_date,
  u.billing_origin_product,
  u.product_features,
  u.usage_type
FROM system.billing.usage u
LEFT JOIN pipeline_tags pt
  ON u.usage_metadata.dlt_pipeline_id = pt.pipeline_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS main.account_usage_dashboard.list_prices
CLUSTER BY (sku_name)
SCHEDULE EVERY 1 HOUR
AS 
SELECT * FROM system.billing.list_prices;

CREATE MATERIALIZED VIEW IF NOT EXISTS main.account_usage_dashboard.account_prices
CLUSTER BY (sku_name)
SCHEDULE EVERY 1 HOUR
AS 
SELECT * FROM system.billing.account_prices;


CREATE MATERIALIZED VIEW IF NOT EXISTS main.account_usage_dashboard.clean_room_events
CLUSTER BY (central_clean_room_id)
SCHEDULE EVERY 1 HOUR
AS 
SELECT * FROM system.access.clean_room_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS main.account_usage_dashboard.served_entities
CLUSTER BY (endpoint_id)
SCHEDULE EVERY 1 HOUR
AS 
SELECT * FROM system.serving.served_entities;

CREATE MATERIALIZED VIEW IF NOT EXISTS main.account_usage_dashboard.clusters
CLUSTER BY (cluster_id)
SCHEDULE EVERY 1 HOUR
AS 
SELECT * FROM system.compute.clusters;

CREATE MATERIALIZED VIEW IF NOT EXISTS main.account_usage_dashboard.jobs
CLUSTER BY (job_id)
SCHEDULE EVERY 1 HOUR
AS 
SELECT * FROM system.lakeflow.jobs;

CREATE MATERIALIZED VIEW IF NOT EXISTS main.account_usage_dashboard.pipelines
CLUSTER BY (pipeline_id)
SCHEDULE EVERY 1 HOUR
AS 
SELECT * FROM system.lakeflow.pipelines;


CREATE MATERIALIZED VIEW IF NOT EXISTS main.account_usage_dashboard.warehouses
CLUSTER BY (warehouse_id)
SCHEDULE EVERY 1 HOUR
AS 
SELECT * FROM system.compute.warehouses;









