# Evidence

Executed runs of the SQL version of the pipeline, setup and DQ gate, exported as text: notebook outputs as `.md` and `.ipynb`, job stdout as `.log`, run metadata as `.json`. All data is synthetic. Workspace-specific identifiers were replaced with placeholders after the runs: `<workspace-host>`, `<workspace-id>`, `<catalog>`, `<user>`, `<your-email>`, `<job-id>`, `<dev-pipeline-id>` and `<staging-pipeline-id>`. Run IDs (file names and generator `run_tag` values) are kept, because the reconciliation checks refer to them.

| Folder | What it proves |
|---|---|
| `dev_integration_test/` | `tlm_integration_test` in the `dev` target: setup, then 3 min of generator data (Zerobus for Plant North and South, files for Plant East, 2% injected defects), then a pipeline update, then the DQ gate. Result: **PASS**, all 10 checks true. Zerobus: 82/82 hard defects quarantined, 86/86 soft defects flagged, 35 duplicates removed (11,619 sent, 11,584 in bronze, 11,502 in silver). Files: 80/80 and 36/36. `table_queries.md` shows rows per layer, duplicates, quarantine reasons and samples, soft flags in silver, gold completeness, the reference-data contract (96/96 pass), and Zerobus as the writer of the raw table (`engineInfo = Zerobus`, 36 commits). |
| `dev_negative_test/` | The same job with `bad_record_rate=0.15`. Reconciliation is still exact (663/663 and 553/553 quarantined), but the hard violation rate is 5.7% (Zerobus) and 9.6% (files), above the 5% threshold. **The gate raises an error, so the job fails.** In CI this blocks the prod deploy. |
| `staging_integration_test/` | The same bundle promoted to the `staging` target (`mode: production`, own schema `telemetry_staging`, no name prefixes). **PASS**. The counts equal dev's because the generator seed is fixed: same input, same result, in a different environment. |
| `deployed_targets/` | `bundle summary` and the resolved config per target. Dev: `[dev <user>]` prefixes, schema `dev_<user>_telemetry`, pipeline `development: true`, schedules paused. Staging: no prefixes, schema `telemetry_staging`, pipeline in production mode. |

The `prod` target is defined but not deployed. It points to a separate workspace with placeholder values (see `docs/ENVIRONMENTS.md`).

**Bug found while moving to SQL.** The first SQL version removed duplicates with `SELECT DISTINCT ... WATERMARK`. The DQ gate failed it: silver was missing the newest 2 minutes of every run. In streaming SQL, `DISTINCT` with a watermark runs as an aggregation, which holds rows back until the watermark passes them. De-duplication therefore stays in Python (`src/pipeline/bronze_dedup.py`, `dropDuplicatesWithinWatermark`), and every other step is SQL. The reconciliation checks are what caught this, which is the point of the gate.
