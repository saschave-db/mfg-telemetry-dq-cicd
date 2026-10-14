# High-volume plant telemetry: Zerobus + Auto Loader, SDP expectations in production, CI/CD with bundles

A focused reference project for three topics:
1. **Manufacturing IoT, high-volume telemetry.** 96 stations at 1 Hz arrive two ways. Live gateways push through **Lakeflow Connect Zerobus**. A legacy historian drops JSON files that **Auto Loader** ingests.
2. **Data quality with Expectations in production.** Three rule tiers (drop, warn, fail), a quarantine table so no bad row is silently lost, the event log as a UC table, a DQ gate, and an hourly monitor.
3. **CI/CD and promotion across environments.** One Declarative Automation Bundle deploys everything to dev, staging and prod, plus a GitHub Actions workflow with an integration test as the release gate.

It is derived from the `mfg-predictive-maintenance` (`https://github.com/<github-org>/mfg-predictive-maintenance`) demo and keeps only the parts these topics need: the physics-lite simulator and the ingestion and silver patterns. The ML, Lakebase, app and Genie parts are left out. All data is synthetic.

**Language:** SQL wherever possible:
- the SDP pipeline, the setup notebook and the DQ gate are SQL
- Python is used only where SQL has no equivalent: the generator (Zerobus Ingest SDK, file writing) and the pipeline's streaming de-duplication flows

## Where to find it

Evidence is executed output exported as text: notebook outputs as `.md`/`.ipynb`, job stdout as `.log`, and SQL results. The index is in [`evidence/README.md`](evidence/README.md).

### 1 · Manufacturing / IoT / high-volume telemetry

| What | Where |
|---|---|
| **Code:** fleet and sensor model (96 stations, 1 Hz, degradation physics) | [`src/telemetry/config.py`](src/telemetry/config.py), [`src/telemetry/physics.py`](src/telemetry/physics.py) |
| **Code:** generator with two sinks, Zerobus (Plant North, South) and JSON files (Plant East historian) | [`src/generator/generate.py`](src/generator/generate.py) |
| **Code:** Zerobus target table, least-privilege grants, reference data (SQL notebook) | [`src/notebooks/00_setup.sql`](src/notebooks/00_setup.sql), [`resources/telemetry.schema.yml`](resources/telemetry.schema.yml) |
| **Code:** SDP in SQL: `read_files` (Auto Loader) and the Zerobus table as two source views into one `bronze_telemetry` table | [`src/pipeline/telemetry_pipeline.sql`](src/pipeline/telemetry_pipeline.sql) (`zerobus_source`, `files_source`, `bronze_telemetry`) |
| **Code:** per-source de-duplication flows: the only Python in the pipeline, because SQL has no streaming equivalent of `dropDuplicatesWithinWatermark` | [`src/pipeline/bronze_dedup.py`](src/pipeline/bronze_dedup.py) |
| **Instructions** | [Architecture](#architecture), [Configure before you run](#configure-before-you-run), [Run it](#run-it) |
| **Evidence:** 3 min of telemetry: 11,619 records through 2 Zerobus streams, 5,805 records in 19 files | [`evidence/dev_integration_test/generate.log`](evidence/dev_integration_test/generate.log) (`opened Zerobus stream`, `MANIFEST`) |
| **Evidence:** Zerobus is the writer of the raw table (36 commits, `engineInfo = Zerobus`). 35 at-least-once duplicates removed in bronze | [`evidence/dev_integration_test/table_queries.md`](evidence/dev_integration_test/table_queries.md), sections "Zerobus writes", "Duplicates removed" and "Rows per layer" |

### 2 · Data quality with Expectations in production

| What | Where |
|---|---|
| **Code:** all row rules, evaluated once in one SQL view: hard (drop + quarantine) and soft (warn) | [`src/pipeline/telemetry_pipeline.sql`](src/pipeline/telemetry_pipeline.sql), view `telemetry_checked` |
| **Code:** SQL expectations: `CONSTRAINT ... EXPECT ... ON VIOLATION DROP ROW` (hard), `EXPECT` (soft, warn), `ON VIOLATION FAIL UPDATE` (contract), plus quarantine table and rescued data | [`src/pipeline/telemetry_pipeline.sql`](src/pipeline/telemetry_pipeline.sql) (`telemetry_silver`, `telemetry_quarantine`, `station_master`, `station_minute_metrics`) |
| **Code:** event log published to UC, failure notifications | [`resources/telemetry.pipeline.yml`](resources/telemetry.pipeline.yml) |
| **Code:** DQ gate in SQL (CI mode: reconcile against the manifest; prod mode: hourly threshold check; fails via `raise_error`) | [`src/notebooks/dq_gate.sql`](src/notebooks/dq_gate.sql), [`resources/dq_monitor.job.yml`](resources/dq_monitor.job.yml) |
| **Instructions** | [Data quality design](#data-quality-design) |
| **Evidence:** expectation pass/fail counts per rule, read from the event log | [`evidence/dev_integration_test/dq_gate.md`](evidence/dev_integration_test/dq_gate.md), section 2 |
| **Evidence:** every injected defect reconciled exactly (82/82, 86/86, 80/80, 36/36), all 10 checks true | same file, section 3 |
| **Evidence:** rows per layer, quarantine reasons, sample bad rows, soft flags, completeness, contract 96/96 | [`evidence/dev_integration_test/table_queries.md`](evidence/dev_integration_test/table_queries.md) |
| **Evidence:** the gate blocks bad data. At 15% defects the rate (5.7%, 9.6%) exceeds the 5% threshold, so the job fails | [`evidence/dev_negative_test/dq_gate.md`](evidence/dev_negative_test/dq_gate.md), sections 3 and 5 |

### 3 · CI/CD and promotion across environments

| What | Where |
|---|---|
| **Code:** targets `dev` / `staging` / `prod`: modes, variables, `run_as`, permissions, test harness only in dev and staging | [`databricks.yml`](databricks.yml) |
| **Code:** all deployed resources (schema, volume, jobs, pipeline) | [`resources/`](resources/) |
| **Code:** CI/CD workflow: validate, then staging deploy and integration test, then prod after approval | [`.github/workflows/bundle-ci-cd.yml`](.github/workflows/bundle-ci-cd.yml) |
| **Instructions:** how targets, modes, variables, `run_as`, permissions and promotion work, and how to move to separate workspaces | [`docs/ENVIRONMENTS.md`](docs/ENVIRONMENTS.md) |
| **Evidence:** the same bundle resolved per target (dev: `[dev user]` prefixes, per-user schema, paused schedules; staging: production mode, own schema) | [`evidence/deployed_targets/`](evidence/deployed_targets/) |
| **Evidence:** promotion to staging. Integration test PASS, 6/6 reconciliation checks | [`evidence/staging_integration_test/dq_gate.md`](evidence/staging_integration_test/dq_gate.md) |
| **Evidence:** the release gate fails a bad build | [`evidence/dev_negative_test/dq_gate.md`](evidence/dev_negative_test/dq_gate.md) |
| **Not executed:** the GitHub Actions workflow (needs a GitHub repo and one service principal per environment) and the `prod` target (placeholder workspace) | see [`docs/ENVIRONMENTS.md`](docs/ENVIRONMENTS.md), "Moving to separate workspaces" |

## Architecture

```
                         ┌── Plant North, South gateways ── Zerobus Ingest SDK ──► telemetry_zerobus_raw (Delta)
tlm_generator (job) ─────┤                                                              │
96 stations, 1 Hz,       └── Plant East historian ── JSON files ──► /Volumes/.../landing/telemetry
injected defects,                                                                       │
manifest of defects                                                read_files (Auto Loader)
                                                                                        │
SDP pipeline (tlm_telemetry_pipeline, serverless, SQL) ◄────────────────────────────────┘
  ├─ bronze_telemetry        2 append flows (zerobus, files) · per-source dedup on event_id (bronze_dedup.py)
  ├─ telemetry_checked       temporary view: every rule evaluated once → dq_hard_failures, dq_soft_failures
  ├─ telemetry_silver        CONSTRAINT ... ON VIOLATION DROP ROW (hard) · CONSTRAINT ... EXPECT (soft, warn)
  ├─ telemetry_quarantine    rows with dq_hard_failures · nothing is lost
  ├─ station_master          CONSTRAINT ... ON VIOLATION FAIL UPDATE (data contract on reference data)
  ├─ station_minute_metrics  gold MV · completeness expectation (>= 50 readings per minute)
  └─ pipeline_event_log      event log published to UC (expectation metrics as a table)

dq_gate (notebook) ── CI: update mode + reconciliation vs manifest · Prod: hourly window mode, threshold
```

## Data quality design

The pipeline is SQL. Every row rule is written once, in the temporary view `telemetry_checked` in [`src/pipeline/telemetry_pipeline.sql`](src/pipeline/telemetry_pipeline.sql). The view records failed rules in two arrays, `dq_hard_failures` and `dq_soft_failures`. Silver declares one `CONSTRAINT` per rule on those arrays, so the event log still reports pass and fail counts per rule. Quarantine selects the rows with hard failures. The DQ gate only reads the arrays, so no rule exists in two places.

| Tier | Action | Rules | Why |
|---|---|---|---|
| Hard | `CONSTRAINT ... ON VIOLATION DROP ROW` on silver, plus routing to quarantine | key fields present, every sensor in its physical range, no Auto Loader rescued data | Bad values must not reach analytics or models. They are kept with the failed rule names for root cause and replay |
| Soft | `CONSTRAINT ... EXPECT` (warn) | timestamp not in the future, firmware known | Plausible but suspicious data stays. It is counted in the event log and watched by the monitor |
| Contract | `CONSTRAINT ... ON VIOLATION FAIL UPDATE` on reference data | station master keys and types valid | Broken reference data stops the update rather than publishing wrong joins |
| Completeness | `CONSTRAINT ... EXPECT` on gold | at least 50 of about 60 readings per station-minute | Measures sensor dropouts, which row-level rules can't see |

Production details that matter:
- **NULL never passes.** Every rule is wrapped as `coalesce(rule, false)`. Without this, `NULL BETWEEN 0 AND 50` is NULL, and the row would not count as a violation.
- **Schema drift doesn't break the stream.** Auto Loader runs with an explicit schema, `schemaEvolutionMode=rescue` and `_rescued_data`. Type mismatches (`"85.2C"` for a double) and new fields land in `_rescued_data`. A hard rule quarantines them.
- **At-least-once is handled.** Zerobus can deliver twice, and exports can be re-sent. Each source flow removes duplicates on `event_id` within a 2-minute watermark, with one watermark per source. This step is in Python on purpose. In streaming SQL, `SELECT DISTINCT` with a `WATERMARK` runs as an aggregation and holds the newest 2 minutes back until the watermark passes them. We measured this: in a triggered run, the last 2 minutes of data were missing from silver. `dropDuplicatesWithinWatermark` emits every first occurrence immediately.
- **A broken gateway clock can't drop good data.** The dedup watermark uses `least(ts, now)`. One gateway sending timestamps 30 minutes ahead can't push the watermark forward and make on-time rows of other gateways look late.
- **Atomic file landing.** The historian sink writes `.name.tmp` and then renames it. Auto Loader reads `*.json` only, so it never ingests half a file.
- **Observability.** The event log is a UC table (`pipeline_event_log`). The DQ gate, dashboards and SQL alerts query expectation metrics with plain SQL. Results of every gate run are appended to `dq_gate_results`.

## Results (see `evidence/`)

- **Dev integration test: PASS.** Every injected defect is reconciled exactly:
  - Zerobus: 82/82 hard defects quarantined, 86/86 soft defects flagged, 35 duplicates removed.
  - Files: 80/80 and 36/36.
- **Negative test: the gate blocks the release.** At 15% injected defects the rows are still fully accounted for, but the violation rate (5.7% and 9.6%) crosses the 5% threshold. The gate fails the job.
- **Promotion to staging:** the same bundle in `mode: production` with its own schema. PASS with identical counts.
- **The gate caught a real bug during the move to SQL.** A first version removed duplicates with `SELECT DISTINCT ... WATERMARK`, and silver was missing the newest 2 minutes of each run. The reconciliation checks failed it. De-duplication is now the one Python step (see Data quality design).
- **Gold completeness:** in a 3-minute test run, half the station-minutes are flagged incomplete. These are the partial first and last minutes of the run, which is what the warn-level expectation is meant to make visible.

## Bundle layout

```
databricks.yml                  targets dev / staging / prod, variables, permissions, test harness (dev+staging only)
resources/
  telemetry.schema.yml          schema (+ USE_SCHEMA grant for the Zerobus principal)
  landing.volume.yml            landing volume for historian files and manifests
  setup.job.yml                 tlm_setup: Zerobus target table, table grants, reference data (idempotent)
  telemetry.pipeline.yml        tlm_pipeline: serverless SDP, continuous via variable, event log in UC, notifications
  dq_monitor.job.yml            tlm_dq_monitor: hourly DQ gate, paused outside prod
src/
  telemetry/                    config and physics simulator (used by the generator)
  generator/generate.py         Python (Zerobus SDK): two sinks, Zerobus and files, defect injection, manifest
  pipeline/telemetry_pipeline.sql   SDP in SQL: sources, rules, silver, quarantine, contract, gold
  pipeline/bronze_dedup.py      SDP in Python: the two de-duplication flows (no SQL equivalent)
  notebooks/00_setup.sql        SQL: Zerobus table, grants, reference data
  notebooks/dq_gate.sql         SQL: DQ gate and monitor
.github/workflows/bundle-ci-cd.yml
docs/ENVIRONMENTS.md            how targets, modes, variables, run_as and promotion work
evidence/                       executed runs (job outputs, DQ gate results)
```

## Configure before you run

The repo ships with placeholders in angle brackets. Replace them in `databricks.yml` before the first deploy. The same placeholders appear in `evidence/`, where workspace-specific identifiers were replaced after the runs; leave those as they are. Names like `dev_<user>_telemetry` in the evidence stand for the per-user prefix that development mode adds.

### 1 · One-time setup in each workspace

Prerequisites:
- Databricks CLI v0.294 or later
- a serverless workspace with Zerobus
- an existing Unity Catalog catalog where you can create schemas

Then set up the gateway identity, using a CLI profile for that workspace:

```bash
# Service principal the plant gateways (and the test generator) use for Zerobus.
databricks service-principals create --display-name tlm-zerobus-gateway
#   -> note "id" (numeric) and "applicationId" (UUID) from the output

# OAuth secret for it
databricks service-principal-secrets-proxy create <id>
#   -> note the secret value (shown only once)

# Secret scope the generator reads (scope name = variable secret_scope, default pdm-demo)
databricks secrets create-scope pdm-demo
databricks secrets put-secret pdm-demo zerobus_client_id --string-value <applicationId>
databricks secrets put-secret pdm-demo zerobus_client_secret --string-value <secret>
```

Then, in SQL, allow the principal into the catalog. The bundle grants `USE_SCHEMA`, and `tlm_setup` grants `MODIFY, SELECT` on the Zerobus table.

```sql
GRANT USE CATALOG ON CATALOG <catalog> TO `<applicationId>`;
```

### 2 · Fill in `databricks.yml`

| Placeholder | Where (`databricks.yml`) | Value | How to find it |
|---|---|---|---|
| `<workspace-host>` | `targets.dev.workspace.host`, `targets.staging.workspace.host` | e.g. `my-workspace.cloud.databricks.com` | the browser URL of the workspace |
| `<catalog>` | `targets.dev.variables.catalog`, `targets.staging.variables.catalog` | an existing catalog | Catalog Explorer |
| `<workspace-id>` | `zerobus_endpoint` in `dev` and `staging` | numeric workspace id | the `o=` parameter in the workspace URL, e.g. `?o=1234567890123456` |
| `<region>` | `zerobus_endpoint` in `dev` and `staging` | e.g. `<region>` | workspace settings, or the cloud region of the workspace |
| `<gateway-sp-application-id>` | `zerobus_sp_app_id` in `dev` and `staging` | the `applicationId` from step 1 | `databricks service-principals list` |
| `<your-email>` | `variables.notification_email.default` | address for pipeline and DQ-gate failure mails | |
| `pdm-demo` (optional) | `variables.secret_scope.default` | your scope name, if different | |

The Zerobus endpoint format shown is for AWS. See the [Zerobus docs](https://docs.databricks.com/ingestion/zerobus-ingest) for other clouds.

Staging can use the same workspace as dev; it gets its own schema (`telemetry_staging`). For a separate staging workspace, use that workspace's values in `targets.staging`.

To keep values out of the file, variables can also be passed at deploy time: `--var catalog=my_catalog` or `BUNDLE_VAR_catalog=my_catalog`. `workspace.host` can't be a variable. Either edit it, or delete the `host` line and let your CLI profile (`--profile`, `DATABRICKS_CONFIG_PROFILE`) choose the workspace.

### 3 · Production (only when you deploy `prod`)

| Placeholder | Where (`targets.prod`) | Value |
|---|---|---|
| `<prod-workspace>` | `workspace.host` | prod workspace host |
| `<prod-deployer-sp-application-id>` | `run_as.service_principal_name` | service principal that owns and runs prod jobs and the pipeline |
| `<prod-workspace-id>`, `<region>` | `variables.zerobus_endpoint` | prod Zerobus endpoint |
| `<prod-gateway-sp-application-id>` | `variables.zerobus_sp_app_id` | gateway principal in prod (repeat step 1 there) |
| `mfg_prod`, `mfg-data-oncall@example.com` | `variables.catalog`, `variables.notification_email` | prod catalog and on-call address |

### 4 · CI/CD (GitHub Actions)

In the GitHub repo, go to **Settings → Environments** and create `staging` and `prod`. In each, add these secrets for a deployer service principal of that workspace:
- `DATABRICKS_HOST`
- `DATABRICKS_CLIENT_ID`
- `DATABRICKS_CLIENT_SECRET`

Add **required reviewers** to `prod`; that is the manual approval gate. Until these exist, the workflow fails at the validate step. Details: [docs/ENVIRONMENTS.md](docs/ENVIRONMENTS.md).

## Run it

```bash
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev
databricks bundle run tlm_integration_test -t dev     # setup -> 3 min of data -> pipeline update -> DQ gate
databricks bundle deploy -t staging && databricks bundle run tlm_integration_test -t staging
```

To try one sink only: `databricks bundle run tlm_generator -t dev --params sink=files,duration_min=2`.

If the CLI picks the wrong profile for the workspace, add `--profile <name>` or set `DATABRICKS_CONFIG_PROFILE`.

Environments and promotion: see [docs/ENVIRONMENTS.md](docs/ENVIRONMENTS.md).
