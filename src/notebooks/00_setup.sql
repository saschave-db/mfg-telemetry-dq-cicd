-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Environment setup (idempotent, runs in every target)
-- MAGIC The bundle creates the schema and the landing volume. This notebook adds what a bundle resource cannot:
-- MAGIC * the **Zerobus target table**. Zerobus writes into a pre-created managed Delta table and never creates or alters it.
-- MAGIC * **table-level grants** for the Zerobus service principal (MODIFY + SELECT on that one table only)
-- MAGIC * the **station master** reference table (synthetic): 3 plants x 4 lines x 8 stations

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT '';
CREATE WIDGET TEXT schema DEFAULT '';
CREATE WIDGET TEXT zerobus_sp DEFAULT '';

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);
SELECT current_catalog() AS catalog, current_schema() AS schema, :zerobus_sp AS zerobus_sp;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS telemetry_zerobus_raw (
  event_id STRING COMMENT 'station + second; de-duplication key (Zerobus is at-least-once)',
  gateway_id STRING, plant_id STRING, line_id STRING, station_id STRING,
  ts TIMESTAMP COMMENT 'sensor sample time (UTC)',
  seq BIGINT, firmware STRING,
  run_tag STRING COMMENT 'generator run that produced the record (test traceability)',
  vibration_rms DOUBLE, bearing_temp_c DOUBLE, motor_current_a DOUBLE, spindle_rpm DOUBLE,
  hydraulic_pressure_bar DOUBLE, acoustic_db DOUBLE, cycle_time_s DOUBLE
)
COMMENT 'Bronze: 1 Hz station telemetry pushed by plant gateways through Lakeflow Connect Zerobus Ingest (synthetic).';

-- COMMAND ----------

-- The principal name can't be a parameter marker in GRANT, so the statement is built as a string.
EXECUTE IMMEDIATE 'GRANT MODIFY, SELECT ON TABLE telemetry_zerobus_raw TO `' || :zerobus_sp || '`';
SHOW GRANTS ON TABLE telemetry_zerobus_raw;

-- COMMAND ----------

CREATE OR REPLACE TABLE ref_station_master
COMMENT 'Station master: 3 plants x 4 lines x 8 stations (synthetic). Same layout as the generator.'
AS
WITH plants(plant_id, plant_name) AS (VALUES ('PLT-N', 'Plant North'), ('PLT-S', 'Plant South'), ('PLT-E', 'Plant East')),
     lines(line) AS (VALUES ('A'), ('B'), ('C'), ('D')),
     positions(position, station_type) AS (VALUES (1, 'press'), (2, 'cnc_mill'), (3, 'welder'), (4, 'robot_arm'),
                                                  (5, 'conveyor'), (6, 'cnc_mill'), (7, 'welder'), (8, 'press'))
SELECT concat(plant_id, '-', line, lpad(CAST(position AS STRING), 2, '0')) AS station_id,
       plant_id, plant_name, concat(plant_id, '-', line) AS line_id, position, station_type,
       concat('gw-', lower(plant_id)) AS gateway_id
FROM plants CROSS JOIN lines CROSS JOIN positions;

SELECT plant_name, gateway_id, count(*) AS stations FROM ref_station_master GROUP BY ALL ORDER BY ALL;
