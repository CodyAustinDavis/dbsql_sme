CREATE CATALOG IF NOT EXISTS main;
CREATE SCHEMA IF NOT EXISTS account_usage_dashboard;

CREATE MATERIALIZED VIEW IF NOT EXISTS main.account_usage_dashboard.workspaces_latest
SCHEDULE EVERY 1 HOUR
AS 
SELECT * FROM system.access.workspaces_latest;

CREATE MATERIALIZED VIEW IF NOT EXISTS main.account_usage_dashboard.usage
SCHEDULE EVERY 1 HOUR
CLUSTER BY (usage_date, workspace_id, sku_name)
AS 
(SELECT * FROM system.billing.usage);

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









