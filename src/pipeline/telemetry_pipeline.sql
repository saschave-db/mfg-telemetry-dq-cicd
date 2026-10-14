-- Telemetry pipeline (Spark Declarative Pipelines, SQL).
--
--   Zerobus table (Delta, written by gateways) ──┐  per-source dedup (watermark)
--                                                ├─► bronze_telemetry (both sources, source column)
--   Volume files ── read_files (Auto Loader) ────┘          │
--                                             telemetry_checked (temporary view: every rule evaluated once)
--                                                ├─► telemetry_silver     CONSTRAINT ... DROP ROW (hard) + warn (soft)
--                                                └─► telemetry_quarantine rows failing a hard rule, with the rule names
--   ref_station_master ─► station_master (CONSTRAINT ... FAIL UPDATE: data contract)
--   telemetry_silver + station_master ─► station_minute_metrics (completeness expectation)
--
-- ${tlm.*} values come from the pipeline configuration in resources/telemetry.pipeline.yml.

-- ---- Bronze: raw records from both ingestion paths, de-duplicated per source -------------------------------------
-- The two source views are defined here. The flows that de-duplicate them into bronze_telemetry are the only
-- Python in the pipeline (bronze_dedup.py): SQL has no streaming de-duplication that emits rows immediately.
-- SELECT DISTINCT with a WATERMARK runs as an aggregation and holds the newest rows back until the watermark passes.
-- _wm_ts is the event time capped at "now", so a gateway with a clock far in the future can't push the watermark ahead.

CREATE OR REFRESH STREAMING TABLE bronze_telemetry
COMMENT 'Bronze: 1 Hz telemetry from Zerobus gateways and historian files with unmodified values, exact repeats '
        'removed per source. source tells the path; _rescued_data holds file values that did not match the schema.';

CREATE TEMPORARY VIEW zerobus_source AS
SELECT 'zerobus' AS source, event_id, gateway_id, plant_id, line_id, station_id, ts, seq, firmware, run_tag,
       vibration_rms, bearing_temp_c, motor_current_a, spindle_rpm, hydraulic_pressure_bar, acoustic_db, cycle_time_s,
       CAST(NULL AS STRING) AS _rescued_data, CAST(NULL AS STRING) AS _source_file,
       least(coalesce(ts, current_timestamp()), current_timestamp()) AS _wm_ts
FROM STREAM(${tlm.zerobus_table});

-- Auto Loader with an explicit schema: type mismatches and unexpected new fields go to _rescued_data
-- (schemaEvolutionMode rescue), so schema drift never stops the stream. *.json skips the generator's .tmp files.
CREATE TEMPORARY VIEW files_source AS
SELECT 'files' AS source, event_id, gateway_id, plant_id, line_id, station_id, ts, seq, firmware, run_tag,
       vibration_rms, bearing_temp_c, motor_current_a, spindle_rpm, hydraulic_pressure_bar, acoustic_db, cycle_time_s,
       _rescued_data, _metadata.file_path AS _source_file,
       least(coalesce(ts, current_timestamp()), current_timestamp()) AS _wm_ts
FROM STREAM read_files(
  '${tlm.landing_path}',
  format => 'json',
  schema => 'event_id STRING, gateway_id STRING, plant_id STRING, line_id STRING, station_id STRING, ts TIMESTAMP,
             seq BIGINT, firmware STRING, run_tag STRING, vibration_rms DOUBLE, bearing_temp_c DOUBLE,
             motor_current_a DOUBLE, spindle_rpm DOUBLE, hydraulic_pressure_bar DOUBLE, acoustic_db DOUBLE,
             cycle_time_s DOUBLE, _rescued_data STRING',
  rescuedDataColumn => '_rescued_data',
  schemaEvolutionMode => 'rescue',
  pathGlobFilter => '*.json'
);

-- ---- Rules: defined once ----------------------------------------------------------------------------------------
-- Every rule is wrapped in coalesce(..., false): a NULL input is a violation, not an unknown that slips through.
-- Hard rules: the row is dropped from silver and kept in quarantine. Soft rules: the row passes and is counted.

CREATE TEMPORARY VIEW telemetry_checked AS
SELECT * EXCEPT (_ingested_at),
  filter(array(
    IF(coalesce(station_id IS NOT NULL, false), NULL, 'station_id_present'),
    IF(coalesce(ts IS NOT NULL, false), NULL, 'ts_present'),
    IF(coalesce(_rescued_data IS NULL, false), NULL, 'no_rescued_data'),
    IF(coalesce(vibration_rms BETWEEN 0 AND 50, false), NULL, 'vibration_rms_in_range'),
    IF(coalesce(bearing_temp_c BETWEEN -20 AND 250, false), NULL, 'bearing_temp_c_in_range'),
    IF(coalesce(motor_current_a BETWEEN 0 AND 1000, false), NULL, 'motor_current_a_in_range'),
    IF(coalesce(spindle_rpm BETWEEN 0 AND 20000, false), NULL, 'spindle_rpm_in_range'),
    IF(coalesce(hydraulic_pressure_bar BETWEEN 0 AND 400, false), NULL, 'hydraulic_pressure_bar_in_range'),
    IF(coalesce(acoustic_db BETWEEN 0 AND 140, false), NULL, 'acoustic_db_in_range'),
    IF(coalesce(cycle_time_s BETWEEN 0 AND 600, false), NULL, 'cycle_time_s_in_range')
  ), x -> x IS NOT NULL) AS dq_hard_failures,
  filter(array(
    IF(coalesce(ts <= current_timestamp() + INTERVAL 5 MINUTES, false), NULL, 'not_from_future'),
    IF(coalesce(firmware IN ('gw-fw-3.2.1', 'hist-exp-1.4'), false), NULL, 'known_firmware')
  ), x -> x IS NOT NULL) AS dq_soft_failures
FROM STREAM(bronze_telemetry);

-- ---- Silver: validated telemetry ---------------------------------------------------------------------------------
-- One constraint per rule, so the event log reports pass/fail counts per rule.

CREATE OR REFRESH STREAMING TABLE telemetry_silver (
  CONSTRAINT station_id_present EXPECT (NOT array_contains(dq_hard_failures, 'station_id_present')) ON VIOLATION DROP ROW,
  CONSTRAINT ts_present EXPECT (NOT array_contains(dq_hard_failures, 'ts_present')) ON VIOLATION DROP ROW,
  CONSTRAINT no_rescued_data EXPECT (NOT array_contains(dq_hard_failures, 'no_rescued_data')) ON VIOLATION DROP ROW,
  CONSTRAINT vibration_rms_in_range EXPECT (NOT array_contains(dq_hard_failures, 'vibration_rms_in_range')) ON VIOLATION DROP ROW,
  CONSTRAINT bearing_temp_c_in_range EXPECT (NOT array_contains(dq_hard_failures, 'bearing_temp_c_in_range')) ON VIOLATION DROP ROW,
  CONSTRAINT motor_current_a_in_range EXPECT (NOT array_contains(dq_hard_failures, 'motor_current_a_in_range')) ON VIOLATION DROP ROW,
  CONSTRAINT spindle_rpm_in_range EXPECT (NOT array_contains(dq_hard_failures, 'spindle_rpm_in_range')) ON VIOLATION DROP ROW,
  CONSTRAINT hydraulic_pressure_bar_in_range EXPECT (NOT array_contains(dq_hard_failures, 'hydraulic_pressure_bar_in_range')) ON VIOLATION DROP ROW,
  CONSTRAINT acoustic_db_in_range EXPECT (NOT array_contains(dq_hard_failures, 'acoustic_db_in_range')) ON VIOLATION DROP ROW,
  CONSTRAINT cycle_time_s_in_range EXPECT (NOT array_contains(dq_hard_failures, 'cycle_time_s_in_range')) ON VIOLATION DROP ROW,
  CONSTRAINT not_from_future EXPECT (NOT array_contains(dq_soft_failures, 'not_from_future')),
  CONSTRAINT known_firmware EXPECT (NOT array_contains(dq_soft_failures, 'known_firmware'))
)
CLUSTER BY (station_id)
COMMENT 'Silver: validated telemetry from both sources. Rows failing a hard rule are dropped here and kept in '
        'telemetry_quarantine. dq_soft_failures lists the soft rules a row violates (it still passes).'
AS SELECT *, current_timestamp() AS _processed_at
FROM STREAM(telemetry_checked);

-- ---- Quarantine: nothing is silently lost ------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE telemetry_quarantine
COMMENT 'Rows that failed at least one hard rule, with the failed rule names in dq_hard_failures. '
        'Same rules as the silver constraints, so silver + quarantine account for every distinct input row.'
AS SELECT *, current_timestamp() AS _processed_at
FROM STREAM(telemetry_checked)
WHERE size(dq_hard_failures) > 0;

-- ---- Reference data contract -------------------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW station_master (
  CONSTRAINT station_id_present EXPECT (coalesce(station_id IS NOT NULL, false)) ON VIOLATION FAIL UPDATE,
  CONSTRAINT station_type_known EXPECT (coalesce(station_type IN ('press', 'cnc_mill', 'welder', 'robot_arm', 'conveyor'), false)) ON VIOLATION FAIL UPDATE,
  CONSTRAINT gateway_assigned EXPECT (coalesce(gateway_id IS NOT NULL, false)) ON VIOLATION FAIL UPDATE
)
COMMENT 'Station master data. A data contract: if a rule is violated the update fails instead of publishing '
        'metrics joined to broken reference data.'
AS SELECT * FROM ${tlm.ref_table};

-- ---- Gold: per-station minute metrics ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW station_minute_metrics (
  CONSTRAINT complete_minute EXPECT (n_readings >= 50)
)
CLUSTER BY (station_id)
COMMENT 'Gold: per-station 1-minute metrics. Rows with a future timestamp (soft rule) are excluded. complete_minute '
        'measures sensor completeness: a healthy 1 Hz station sends about 60 readings a minute.'
AS SELECT s.station_id, s.source, window(s.ts, '1 minute').start AS minute,
          m.plant_name, s.line_id, m.station_type,
          count(*) AS n_readings,
          round(avg(s.vibration_rms), 3) AS avg_vibration_rms,
          round(max(s.vibration_rms), 3) AS max_vibration_rms,
          round(avg(s.bearing_temp_c), 3) AS avg_bearing_temp_c,
          round(avg(s.motor_current_a), 3) AS avg_motor_current_a,
          round(avg(s.spindle_rpm), 3) AS avg_spindle_rpm,
          round(avg(s.hydraulic_pressure_bar), 3) AS avg_hydraulic_pressure_bar,
          round(avg(s.acoustic_db), 3) AS avg_acoustic_db,
          round(avg(s.cycle_time_s), 3) AS avg_cycle_time_s
FROM telemetry_silver s
LEFT JOIN station_master m ON m.station_id = s.station_id
WHERE s.ts <= current_timestamp() + INTERVAL 5 MINUTES
GROUP BY s.station_id, s.source, window(s.ts, '1 minute'), m.plant_name, s.line_id, m.station_type;
