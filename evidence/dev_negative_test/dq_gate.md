# Executed notebook: dq_gate

Exported from Databricks job run `10545323928911` (task `dq_gate`, task run `918634447391973`).

Result: **FAILED** · start 2026-10-14T00:43:19.957000+00:00 · end 2026-10-14T00:43:49.489000+00:00

Run URL: https://<workspace-host>/?o=<workspace-id>#job/<job-id>/run/10545323928911


# DQ gate: data quality as a release and operations check
Used two ways, by the same SQL:
* **CI/CD integration test** (`mode=update`, `reconcile=true`): check the expectation results of the latest pipeline
  update, then reconcile every injected defect from the generator's manifest against silver and quarantine.
  A mismatch fails the job, so a broken rule or a pipeline change that loses rows blocks promotion.
* **Production monitor** (`mode=window`, scheduled): check the rows of the last N hours of a continuous pipeline.
  Fail (and notify) if the hard-rule violation rate crosses the threshold.

Every check lands in `dq_gate_last_checks`; the verdict of every run is appended to `dq_gate_results`.

```python
CREATE WIDGET TEXT catalog DEFAULT '';
CREATE WIDGET TEXT schema DEFAULT '';
CREATE WIDGET TEXT mode DEFAULT 'update';
CREATE WIDGET TEXT lookback_hours DEFAULT '1';
CREATE WIDGET TEXT max_hard_violation_rate DEFAULT '0.05';
CREATE WIDGET TEXT reconcile DEFAULT 'true';
CREATE WIDGET TEXT run_tag DEFAULT '';
CREATE WIDGET TEXT manifest_path DEFAULT '';
```

Output:

```text
{"application/vnd.databricks.empty-table+json": {"directive_name": "TextWidgetSpec"}}
```

```python
USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

DECLARE OR REPLACE VARIABLE v_mode STRING;
DECLARE OR REPLACE VARIABLE v_reconcile BOOLEAN;
DECLARE OR REPLACE VARIABLE v_max_rate DOUBLE;
DECLARE OR REPLACE VARIABLE v_since TIMESTAMP;
DECLARE OR REPLACE VARIABLE v_update STRING;
DECLARE OR REPLACE VARIABLE v_run_tag STRING;

SET VAR v_mode = :mode;
SET VAR v_reconcile = lower(:reconcile) = 'true';
SET VAR v_max_rate = CAST(:max_hard_violation_rate AS DOUBLE);
SET VAR v_since = current_timestamp() - make_dt_interval(0, CAST(:lookback_hours AS INT), 0, 0);
SET VAR v_update = (SELECT origin.update_id FROM pipeline_event_log WHERE event_type = 'create_update'
                    ORDER BY timestamp DESC LIMIT 1);
```

Output:

|  |
|

## 1 · Generator manifest (integration test only)
The generator writes one JSON manifest per run with the number of defects it injected per sink.

```python
CREATE TABLE IF NOT EXISTS dq_gate_manifests (
  run_tag STRING, finished_at TIMESTAMP,
  records_sent STRUCT<zerobus: BIGINT, files: BIGINT>,
  expected_quarantine STRUCT<zerobus: BIGINT, files: BIGINT>,
  expected_soft_violations STRUCT<zerobus: BIGINT, files: BIGINT>,
  expected_duplicates STRUCT<zerobus: BIGINT, files: BIGINT>
);

-- Production has no generator and no manifests: only read them when reconciling.
EXECUTE IMMEDIATE CASE WHEN v_reconcile THEN
  -- Inner quotes are backslash-escaped: in Spark SQL, '' splits a literal instead of escaping a quote.
  'CREATE OR REPLACE TABLE dq_gate_manifests AS SELECT run_tag, finished_at, records_sent, expected_quarantine, '
  || 'expected_soft_violations, expected_duplicates FROM read_files(\'' || :manifest_path || '\', format => \'json\', '
  || 'multiLine => true, schema => \'run_tag STRING, finished_at TIMESTAMP, '
  || 'records_sent STRUCT<zerobus: BIGINT, files: BIGINT>, expected_quarantine STRUCT<zerobus: BIGINT, files: BIGINT>, '
  || 'expected_soft_violations STRUCT<zerobus: BIGINT, files: BIGINT>, expected_duplicates STRUCT<zerobus: BIGINT, files: BIGINT>\')'
  ELSE 'SELECT 1' END;

SET VAR v_run_tag = CASE WHEN v_reconcile THEN coalesce(nullif(:run_tag, ''),
                      (SELECT run_tag FROM dq_gate_manifests ORDER BY finished_at DESC LIMIT 1)) END;

SELECT v_mode AS mode, v_reconcile AS reconcile, v_run_tag AS run_tag, v_update AS latest_update, v_since AS window_start;
```

Output:

| mode | reconcile | run_tag | latest_update | window_start |
|---|---|---|---|---|
| update | True | 1116505020220577 | ca0a2404-2607-4527-ace2-05bd8df681fe | 2026-10-13T23:43:26.208Z |

```python
SELECT * FROM dq_gate_manifests WHERE run_tag = v_run_tag;
```

Output:

| run_tag | finished_at | records_sent | expected_quarantine | expected_soft_violations | expected_duplicates |
|---|---|---|---|---|---|
| 1116505020220577 | 2026-10-14T00:40:18.269Z | [11909, 5908] | [663, 553] | [723, 217] | [325, 116] |

## 2 · Expectation results per rule (from the event log in Unity Catalog)

```python
SELECT e.dataset, e.name AS expectation, sum(e.passed_records) AS passed, sum(e.failed_records) AS failed
FROM (
  SELECT origin, timestamp, explode(from_json(details:flow_progress:data_quality:expectations,
         'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
  FROM pipeline_event_log WHERE event_type = 'flow_progress'
)
WHERE CASE WHEN v_mode = 'update' THEN origin.update_id = v_update ELSE timestamp >= v_since END
GROUP BY ALL ORDER BY dataset, expectation;
```

Output:

| dataset | expectation | passed | failed |
|---|---|---|---|
| <catalog>.dev_<user>_telemetry.station_master | gateway_assigned | 96 | 0 |
| <catalog>.dev_<user>_telemetry.station_master | station_id_present | 96 | 0 |
| <catalog>.dev_<user>_telemetry.station_master | station_type_known | 96 | 0 |
| <catalog>.dev_<user>_telemetry.station_minute_metrics | complete_minute | 1334 | 1719 |
| <catalog>.dev_<user>_telemetry.telemetry_silver | acoustic_db_in_range | 17264 | 112 |
| <catalog>.dev_<user>_telemetry.telemetry_silver | bearing_temp_c_in_range | 17162 | 214 |
| <catalog>.dev_<user>_telemetry.telemetry_silver | cycle_time_s_in_range | 17264 | 112 |
| <catalog>.dev_<user>_telemetry.telemetry_silver | hydraulic_pressure_bar_in_range | 17264 | 112 |
| <catalog>.dev_<user>_telemetry.telemetry_silver | known_firmware | 16802 | 574 |
| <catalog>.dev_<user>_telemetry.telemetry_silver | motor_current_a_in_range | 17264 | 112 |
| <catalog>.dev_<user>_telemetry.telemetry_silver | no_rescued_data | 17166 | 210 |
| <catalog>.dev_<user>_telemetry.telemetry_silver | not_from_future | 16786 | 590 |
| <catalog>.dev_<user>_telemetry.telemetry_silver | spindle_rpm_in_range | 17264 | 112 |
| <catalog>.dev_<user>_telemetry.telemetry_silver | station_id_present | 16801 | 575 |
| <catalog>.dev_<user>_telemetry.telemetry_silver | ts_present | 17264 | 112 |
| <catalog>.dev_<user>_telemetry.telemetry_silver | vibration_rms_in_range | 16833 | 543 |

## 3 · All checks
* pipeline update completed (update mode) and expectation metrics are present
* hard violation rate per source ≤ threshold (rows of the tested run, or of the lookback window)
* reconciliation (integration test): hard defects → quarantine, soft defects → flagged in silver, duplicates removed

```python
CREATE OR REPLACE TABLE dq_gate_last_checks AS
WITH
silver AS (
  SELECT * FROM telemetry_silver
  WHERE CASE WHEN v_run_tag IS NOT NULL
             THEN (source = 'zerobus' AND run_tag = v_run_tag) OR (source = 'files' AND _source_file LIKE concat('%\\_', v_run_tag, '\\_%'))
             ELSE _processed_at >= v_since END),
quarantine AS (
  SELECT * FROM telemetry_quarantine
  WHERE CASE WHEN v_run_tag IS NOT NULL
             THEN (source = 'zerobus' AND run_tag = v_run_tag) OR (source = 'files' AND _source_file LIKE concat('%\\_', v_run_tag, '\\_%'))
             ELSE _processed_at >= v_since END),
actual AS (
  SELECT source, sum(silver_rows) AS silver_rows, sum(soft_rows) AS soft_rows, sum(quarantined_rows) AS quarantined_rows
  FROM (SELECT source, count(*) AS silver_rows, count_if(size(dq_soft_failures) > 0) AS soft_rows, 0 AS quarantined_rows
        FROM silver GROUP BY source
        UNION ALL
        SELECT source, 0, 0, count(*) FROM quarantine GROUP BY source)
  GROUP BY source),
expected AS (
  SELECT * FROM (
    SELECT stack(2,
      'zerobus', records_sent.zerobus, expected_quarantine.zerobus, expected_soft_violations.zerobus, expected_duplicates.zerobus,
      'files', records_sent.files, expected_quarantine.files, expected_soft_violations.files, expected_duplicates.files
    ) AS (source, sent, exp_quarantine, exp_soft, exp_duplicates)
    FROM dq_gate_manifests WHERE run_tag = v_run_tag)
  WHERE sent IS NOT NULL),
pipeline_state AS (
  SELECT max_by(details:update_progress:state::STRING, timestamp) AS state
  FROM pipeline_event_log WHERE event_type = 'update_progress' AND origin.update_id = v_update),
metrics AS (
  SELECT count(*) AS n FROM pipeline_event_log
  WHERE event_type = 'flow_progress' AND details:flow_progress:data_quality IS NOT NULL
    AND CASE WHEN v_mode = 'update' THEN origin.update_id = v_update ELSE timestamp >= v_since END)
SELECT 'pipeline' AS scope, 'latest update completed' AS check_name,
       coalesce(state, 'none') AS actual, 'COMPLETED' AS expected, coalesce(state = 'COMPLETED', false) AS ok
FROM pipeline_state WHERE v_mode = 'update'
UNION ALL
SELECT 'event log', 'expectation metrics present', CAST(n AS STRING), '> 0', n > 0 FROM metrics
UNION ALL
SELECT source, 'hard violation rate',
       CAST(round(quarantined_rows / (silver_rows + quarantined_rows), 4) AS STRING), concat('<= ', v_max_rate),
       quarantined_rows / (silver_rows + quarantined_rows) <= v_max_rate
FROM actual
UNION ALL
SELECT e.source, c.check_name, CAST(c.actual_n AS STRING), CAST(c.expected_n AS STRING), c.actual_n = c.expected_n
FROM expected e LEFT JOIN actual a USING (source)
LATERAL VIEW stack(3,
  'quarantined rows = injected hard defects', coalesce(a.quarantined_rows, 0), e.exp_quarantine,
  'silver rows flagged by soft rules = injected soft defects', coalesce(a.soft_rows, 0), e.exp_soft,
  'silver rows = sent - quarantined - duplicates', coalesce(a.silver_rows, 0), e.sent - e.exp_quarantine - e.exp_duplicates
) c AS check_name, actual_n, expected_n;

SELECT * FROM dq_gate_last_checks ORDER BY scope, check_name;
```

Output:

| scope | check_name | actual | expected | ok |
|---|---|---|---|---|
| event log | expectation metrics present | 8 | > 0 | True |
| files | hard violation rate | 0.0955 | <= 0.05 | False |
| files | quarantined rows = injected hard defects | 553 | 553 | True |
| files | silver rows = sent - quarantined - duplicates | 5239 | 5239 | True |
| files | silver rows flagged by soft rules = injected soft defects | 217 | 217 | True |
| pipeline | latest update completed | COMPLETED | COMPLETED | True |
| zerobus | hard violation rate | 0.0572 | <= 0.05 | False |
| zerobus | quarantined rows = injected hard defects | 663 | 663 | True |
| zerobus | silver rows = sent - quarantined - duplicates | 10921 | 10921 | True |
| zerobus | silver rows flagged by soft rules = injected soft defects | 723 | 723 | True |

## 4 · Quarantine by failed rule (same scope)

```python
SELECT source, failed_rule, count(*) AS rows
FROM (SELECT source, explode(dq_hard_failures) AS failed_rule FROM telemetry_quarantine
      WHERE CASE WHEN v_run_tag IS NOT NULL
                 THEN (source = 'zerobus' AND run_tag = v_run_tag) OR (source = 'files' AND _source_file LIKE concat('%\\_', v_run_tag, '\\_%'))
                 ELSE _processed_at >= v_since END)
GROUP BY ALL ORDER BY source, rows DESC;
```

Output:

| source | failed_rule | rows |
|---|---|---|
| files | station_id_present | 228 |
| files | vibration_rms_in_range | 227 |
| files | bearing_temp_c_in_range | 214 |
| files | no_rescued_data | 210 |
| files | hydraulic_pressure_bar_in_range | 112 |
| files | acoustic_db_in_range | 112 |
| files | ts_present | 112 |
| files | spindle_rpm_in_range | 112 |
| files | cycle_time_s_in_range | 112 |
| files | motor_current_a_in_range | 112 |
| zerobus | station_id_present | 347 |
| zerobus | vibration_rms_in_range | 316 |

## 5 · Verdict
A failed check raises an error, which fails the task and the job: in CI that blocks the prod deploy, in prod it
triggers the job's failure notification.

```python
CREATE TABLE IF NOT EXISTS dq_gate_results (checked_at TIMESTAMP, mode STRING, verdict STRING, failures STRING, params STRING);

INSERT INTO dq_gate_results
SELECT current_timestamp(), v_mode,
       CASE WHEN count_if(NOT ok) = 0 AND count(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
       to_json(filter(collect_list(CASE WHEN NOT ok THEN concat(scope, ': ', check_name, ': ', actual, ' (expected ', expected, ')') END), x -> x IS NOT NULL)),
       to_json(named_struct('catalog', :catalog, 'schema', :schema, 'mode', :mode, 'lookback_hours', :lookback_hours,
                            'max_hard_violation_rate', :max_hard_violation_rate, 'reconcile', :reconcile, 'run_tag', v_run_tag))
FROM dq_gate_last_checks;

SELECT CASE WHEN count_if(NOT ok) = 0 AND count(*) > 0 THEN 'DQ GATE PASS'
            ELSE raise_error(concat('DQ GATE FAIL: ', array_join(collect_list(
                   CASE WHEN NOT ok THEN concat(scope, ': ', check_name, ': ', actual, ' (expected ', expected, ')') END), '; ')))
       END AS verdict
FROM dq_gate_last_checks;
```

Output:

```text
ERROR: [USER_RAISED_EXCEPTION] DQ GATE FAIL: zerobus: hard violation rate: 0.0572 (expected <= 0.05); files: hard violation rate: 0.0955 (expected <= 0.05) SQLSTATE: P0001
```
