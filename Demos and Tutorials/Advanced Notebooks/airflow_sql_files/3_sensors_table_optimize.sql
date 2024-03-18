OPTIMIZE main.iot_dashboard_airflow.silver_sensors ZORDER BY (timestamp);

ANALYZE TABLE main.iot_dashboard_airflow.silver_sensors COMPUTE STATISTICS FOR ALL COLUMNS;
