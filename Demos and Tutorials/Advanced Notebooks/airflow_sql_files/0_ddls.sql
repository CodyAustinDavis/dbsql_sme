CREATE DATABASE IF NOT EXISTS main.iot_dashboard_airflow;

CREATE TABLE IF NOT EXISTS main.iot_dashboard_airflow.bronze_sensors
(
Id BIGINT GENERATED BY DEFAULT AS IDENTITY,
device_id INT,
user_id INT,
calories_burnt DECIMAL(10,2), 
miles_walked DECIMAL(10,2), 
num_steps DECIMAL(10,2), 
timestamp TIMESTAMP,
value STRING
)
USING DELTA
TBLPROPERTIES("delta.targetFileSize"="128mb")
;

CREATE TABLE IF NOT EXISTS main.iot_dashboard_airflow.bronze_users
(
userid BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
gender STRING,
age INT,
height DECIMAL(10,2), 
weight DECIMAL(10,2),
smoker STRING,
familyhistory STRING,
cholestlevs STRING,
bp STRING,
risk DECIMAL(10,2),
update_timestamp TIMESTAMP
)
USING DELTA 
TBLPROPERTIES("delta.targetFileSize"="128mb")
;

CREATE TABLE IF NOT EXISTS main.iot_dashboard_airflow.silver_sensors
(
Id BIGINT GENERATED BY DEFAULT AS IDENTITY,
device_id INT,
user_id INT,
calories_burnt DECIMAL(10,2), 
miles_walked DECIMAL(10,2), 
num_steps DECIMAL(10,2), 
timestamp TIMESTAMP,
value STRING
)
USING DELTA 
PARTITIONED BY (user_id)
TBLPROPERTIES("delta.targetFileSize"="128mb")
;

CREATE TABLE IF NOT EXISTS main.iot_dashboard_airflow.silver_users
(
userid BIGINT GENERATED BY DEFAULT AS IDENTITY,
gender STRING,
age INT,
height DECIMAL(10,2), 
weight DECIMAL(10,2),
smoker STRING,
familyhistory STRING,
cholestlevs STRING,
bp STRING,
risk DECIMAL(10,2),
update_timestamp TIMESTAMP
)
USING DELTA 
TBLPROPERTIES("delta.targetFileSize"="128mb")
;