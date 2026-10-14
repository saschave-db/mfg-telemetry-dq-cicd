# Table queries after the dev integration test

Run 2026-10-14T00:36:54Z with `databricks experimental aitools tools query`, schema `<catalog>.dev_<user>_telemetry`, generator run `89927789530603`.

## Tables and views of the schema (internal `__materialization_*` tables of SDP omitted)

```sql
SELECT table_name, table_type, left(comment, 90) AS comment FROM <catalog>.information_schema.tables WHERE table_schema = 'dev_<user>_telemetry' AND table_name NOT LIKE '\\_\\_%' ORDER BY table_name
```

| comment | table_name | table_type |
|---|---|---|
| Bronze: 1 Hz telemetry from Zerobus gateways and historian files with unmodified values, e | bronze_telemetry | STREAMING_TABLE |
|  | dq_gate_last_checks | MANAGED |
|  | dq_gate_manifests | MANAGED |
|  | dq_gate_results | MANAGED |
|  | pipeline_event_log | MANAGED |
| Station master: 3 plants x 4 lines x 8 stations (synthetic). Same layout as the generator. | ref_station_master | MANAGED |
| Station master data. A data contract: if a rule is violated the update fails instead of pu | station_master | MATERIALIZED_VIEW |
| Gold: per-station 1-minute metrics. Rows with a future timestamp (soft rule) are excluded. | station_minute_metrics | MATERIALIZED_VIEW |
| Rows that failed at least one hard rule, with the failed rule names in dq_hard_failures. S | telemetry_quarantine | STREAMING_TABLE |
| Silver: validated telemetry from both sources. Rows failing a hard rule are dropped here a | telemetry_silver | STREAMING_TABLE |
| Bronze: 1 Hz station telemetry pushed by plant gateways through Lakeflow Connect Zerobus I | telemetry_zerobus_raw | MANAGED |

## Rows per layer for this run

```sql
SELECT 'bronze_telemetry' AS layer, source, count(*) AS rows FROM <catalog>.dev_<user>_telemetry.bronze_telemetry WHERE ((source = 'zerobus' AND run_tag = '89927789530603') OR (source = 'files' AND _source_file LIKE '%\\_89927789530603\\_%')) GROUP BY ALL UNION ALL SELECT 'telemetry_silver', source, count(*) FROM <catalog>.dev_<user>_telemetry.telemetry_silver WHERE ((source = 'zerobus' AND run_tag = '89927789530603') OR (source = 'files' AND _source_file LIKE '%\\_89927789530603\\_%')) GROUP BY ALL UNION ALL SELECT 'telemetry_quarantine', source, count(*) FROM <catalog>.dev_<user>_telemetry.telemetry_quarantine WHERE ((source = 'zerobus' AND run_tag = '89927789530603') OR (source = 'files' AND _source_file LIKE '%\\_89927789530603\\_%')) GROUP BY ALL ORDER BY layer, source
```

| layer | rows | source |
|---|---|---|
| bronze_telemetry | 5792 | files |
| bronze_telemetry | 11584 | zerobus |
| telemetry_quarantine | 80 | files |
| telemetry_quarantine | 82 | zerobus |
| telemetry_silver | 5712 | files |
| telemetry_silver | 11502 | zerobus |

## Duplicates removed in bronze (Zerobus): raw rows vs bronze rows for this run

```sql
SELECT (SELECT count(*) FROM <catalog>.dev_<user>_telemetry.telemetry_zerobus_raw WHERE run_tag = '89927789530603') AS raw_rows, (SELECT count(DISTINCT event_id) FROM <catalog>.dev_<user>_telemetry.telemetry_zerobus_raw WHERE run_tag = '89927789530603') AS distinct_event_ids, (SELECT count(*) FROM <catalog>.dev_<user>_telemetry.bronze_telemetry WHERE source = 'zerobus' AND run_tag = '89927789530603') AS bronze_rows
```

| bronze_rows | distinct_event_ids | raw_rows |
|---|---|---|
| 11584 | 11584 | 11619 |

## Quarantine by source and failed rules

```sql
SELECT source, array_join(dq_hard_failures, ', ') AS failed_rules, count(*) AS rows FROM <catalog>.dev_<user>_telemetry.telemetry_quarantine WHERE ((source = 'zerobus' AND run_tag = '89927789530603') OR (source = 'files' AND _source_file LIKE '%\\_89927789530603\\_%')) GROUP BY ALL ORDER BY source, rows DESC
```

| failed_rules | rows | source |
|---|---|---|
| station_id_present | 21 | files |
| no_rescued_data, bearing_temp_c_in_range | 17 | files |
| station_id_present, ts_present, vibration_rms_in_range, bearing_temp_c_in_range, motor_cur | 15 | files |
| vibration_rms_in_range | 14 | files |
| no_rescued_data | 13 | files |
| station_id_present | 47 | zerobus |
| vibration_rms_in_range | 35 | zerobus |

## Quarantine samples: what a bad row looks like

```sql
SELECT source, station_id, ts, vibration_rms, left(_rescued_data, 60) AS rescued_data, regexp_extract(_source_file, '[^/]+$', 0) AS source_file, dq_hard_failures FROM <catalog>.dev_<user>_telemetry.telemetry_quarantine WHERE ((source = 'zerobus' AND run_tag = '89927789530603') OR (source = 'files' AND _source_file LIKE '%\\_89927789530603\\_%')) ORDER BY source, ts LIMIT 12
```

| dq_hard_failures | rescued_data | source | source_file | station_id | ts | vibration_rms |
|---|---|---|---|---|---|---|
| ["station_id_present","ts_present","vibration_rms_in_range","bearing_temp_c_in_range","mot |  | files | gw-plt-e_89927789530603_00012.json |  |  |  |
| ["station_id_present","ts_present","vibration_rms_in_range","bearing_temp_c_in_range","mot |  | files | gw-plt-e_89927789530603_00005.json |  |  |  |
| ["station_id_present","ts_present","vibration_rms_in_range","bearing_temp_c_in_range","mot |  | files | gw-plt-e_89927789530603_00008.json |  |  |  |
| ["station_id_present","ts_present","vibration_rms_in_range","bearing_temp_c_in_range","mot |  | files | gw-plt-e_89927789530603_00009.json |  |  |  |
| ["station_id_present","ts_present","vibration_rms_in_range","bearing_temp_c_in_range","mot |  | files | gw-plt-e_89927789530603_00013.json |  |  |  |
| ["station_id_present","ts_present","vibration_rms_in_range","bearing_temp_c_in_range","mot |  | files | gw-plt-e_89927789530603_00002.json |  |  |  |
| ["station_id_present","ts_present","vibration_rms_in_range","bearing_temp_c_in_range","mot |  | files | gw-plt-e_89927789530603_00015.json |  |  |  |
| ["station_id_present","ts_present","vibration_rms_in_range","bearing_temp_c_in_range","mot |  | files | gw-plt-e_89927789530603_00016.json |  |  |  |
| ["station_id_present","ts_present","vibration_rms_in_range","bearing_temp_c_in_range","mot |  | files | gw-plt-e_89927789530603_00014.json |  |  |  |
| ["station_id_present","ts_present","vibration_rms_in_range","bearing_temp_c_in_range","mot |  | files | gw-plt-e_89927789530603_00015.json |  |  |  |
| ["station_id_present","ts_present","vibration_rms_in_range","bearing_temp_c_in_range","mot |  | files | gw-plt-e_89927789530603_00014.json |  |  |  |
| ["station_id_present","ts_present","vibration_rms_in_range","bearing_temp_c_in_range","mot |  | files | gw-plt-e_89927789530603_00007.json |  |  |  |

## Soft-rule flags in silver (rows pass, flagged)

```sql
SELECT source, array_join(dq_soft_failures, ', ') AS soft_failures, count(*) AS rows FROM <catalog>.dev_<user>_telemetry.telemetry_silver WHERE ((source = 'zerobus' AND run_tag = '89927789530603') OR (source = 'files' AND _source_file LIKE '%\\_89927789530603\\_%')) AND size(dq_soft_failures) > 0 GROUP BY ALL ORDER BY source, rows DESC
```

| rows | soft_failures | source |
|---|---|---|
| 24 | not_from_future | files |
| 12 | known_firmware | files |
| 45 | known_firmware | zerobus |
| 41 | not_from_future | zerobus |

## Gold completeness expectation: readings per station-minute (all runs)

```sql
SELECT source, count(*) AS station_minutes, count_if(n_readings >= 50) AS complete, count_if(n_readings < 50) AS incomplete, min(n_readings) AS min_readings, max(n_readings) AS max_readings FROM <catalog>.dev_<user>_telemetry.station_minute_metrics GROUP BY source
```

| complete | incomplete | max_readings | min_readings | source | station_minutes |
|---|---|---|---|---|---|
| 443 | 653 | 60 | 1 | files | 1096 |
| 704 | 869 | 60 | 1 | zerobus | 1573 |

## Contract on reference data (FAIL UPDATE) and completeness, from the event log

```sql
SELECT e.dataset, e.name AS expectation, sum(e.passed_records) AS passed, sum(e.failed_records) AS failed FROM (SELECT explode(from_json(details:flow_progress:data_quality:expectations, 'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e FROM <catalog>.dev_<user>_telemetry.pipeline_event_log WHERE event_type = 'flow_progress' AND origin.update_id = (SELECT origin.update_id FROM <catalog>.dev_<user>_telemetry.pipeline_event_log WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1)) WHERE e.dataset LIKE '%station_master' OR e.dataset LIKE '%station_minute_metrics' GROUP BY ALL ORDER BY ALL
```

| dataset | expectation | failed | passed |
|---|---|---|---|
| <catalog>.dev_<user>_telemetry.station_master | gateway_assigned | 0 | 96 |
| <catalog>.dev_<user>_telemetry.station_master | station_id_present | 0 | 96 |
| <catalog>.dev_<user>_telemetry.station_master | station_type_known | 0 | 96 |
| <catalog>.dev_<user>_telemetry.station_minute_metrics | complete_minute | 1522 | 1147 |

## Zerobus writes during this run: Delta history of the raw table

```sql
SELECT operation, count(*) AS commits, min(timestamp) AS first_commit, max(timestamp) AS last_commit, first(engineInfo) AS engine FROM (DESCRIBE HISTORY <catalog>.dev_<user>_telemetry.telemetry_zerobus_raw) WHERE timestamp BETWEEN (SELECT min(ts) - INTERVAL 1 MINUTE FROM <catalog>.dev_<user>_telemetry.telemetry_zerobus_raw WHERE run_tag = '89927789530603') AND (SELECT max(ts) + INTERVAL 2 MINUTES FROM <catalog>.dev_<user>_telemetry.telemetry_zerobus_raw WHERE run_tag = '89927789530603' AND ts < current_timestamp()) GROUP BY operation
```

| commits | engine | first_commit | last_commit | operation |
|---|---|---|---|---|
| 36 | Zerobus | 2026-10-14T00:30:09.000Z | 2026-10-14T00:33:05.000Z | WRITE |
