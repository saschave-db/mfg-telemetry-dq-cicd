# Deploying across environments with Declarative Automation Bundles

One bundle (`databricks.yml` + `resources/` + `src/`) is the single source of truth. Each **target** is one environment. A target picks the workspace, the deployment mode, the identity and the variable values. The code and resource definitions stay identical, so what was tested in staging is exactly what runs in prod.

## The three targets

| | `dev` | `staging` | `prod` |
|---|---|---|---|
| Purpose | Each developer's isolated copy | Pre-production, integration test, release gate | Live plants |
| Workspace (`workspace.host`) | dev workspace | staging workspace (in this demo: the dev workspace) | prod workspace |
| `mode` | `development` | `production` | `production` |
| Deployed by | the developer | CI service principal | CI service principal (`run_as`) |
| Catalog / schema | dev catalog, `dev_<user>_telemetry` | dev catalog, `telemetry_staging` | `mfg_prod.telemetry` |
| Pipeline | triggered, development mode | triggered | **continuous** |
| Test harness (generator, integration test) | yes | yes | **no** |
| Hourly DQ monitor | paused | paused | **running**, threshold 1% |
| Bundle files | user folder | user folder (CI SP's home in real use) | `/Workspace/Shared/.bundle/...` |

## What each mechanism does

**`mode: development`** (dev):
- Prefixes job and pipeline names with `[dev <user>]` and schemas with `dev_<user>_`. Two developers never collide. In this deploy: `dev_<user>_telemetry`.
- Pauses all schedules, so a dev deploy never starts an hourly job.
- Sets pipelines to `development: true` for fast iteration, for example without automatic retries.

**`mode: production`** (staging, prod):
- No name prefixes. Schedules run as defined, here through the `monitor_pause_status` variable.
- Pipelines run in production mode.

**Variables** (`variables:` in `databricks.yml`, overridden per target):
- **Values:** catalog, schema, Zerobus endpoint, gateway service principal, `pipeline_continuous`, DQ threshold, notification address.
- **Where they flow:** into resource definitions, for example `continuous: ${var.pipeline_continuous}`, and into pipeline configuration and job parameters. The code never hard-codes an environment.
- **Overrides at deploy time:** `--var key=value` or `BUNDLE_VAR_<name>` environment variables, useful in CI.

**Target-scoped resources:**
- The generator and the integration-test job are defined under `targets.dev.resources` and reused in staging through a YAML anchor (`*test_harness`).
- `prod` doesn't define them, so a prod deploy can't create a job that writes synthetic data into production tables.

**Resource references** (`${resources.schemas.telemetry.name}`, `${resources.jobs.tlm_setup.id}`, `${resources.pipelines.tlm_pipeline.id}`) wire resources together. They resolve to the deployed name or ID in each target. That's how the dev prefix and the staging schema reach every job parameter and the pipeline configuration without any `if env == ...` code.

**`run_as`** (prod):
- Jobs and the pipeline run as a service principal, not as the person or CI identity that deployed them.
- Ownership and permissions don't depend on any individual.

**`permissions` and `grants`:**
- `permissions` (top level) sets who can view or manage the deployed jobs and pipeline.
- `grants` on the schema resource gives the Zerobus gateway principal `USE_SCHEMA`.
- Table-level `MODIFY, SELECT` for Zerobus is granted by the idempotent `tlm_setup` job, because there's no bundle resource for a table that Zerobus writes into.

**Deployment state:**
- Each target keeps its own state under its `root_path`.
- `databricks bundle deploy -t staging` only touches staging resources.
- `databricks bundle destroy -t <target>` removes exactly what that target created.

## Promotion flow (`.github/workflows/bundle-ci-cd.yml`)

```
pull request ──► bundle validate --strict (staging, prod)
merge to main ──► deploy staging ──► run tlm_integration_test ──► DQ gate PASS? ──► [approval] ──► deploy prod ──► run tlm_setup
```

1. **Validate:** `bundle validate --strict` for every target catches unresolved variables, bad references and warnings before anything is deployed.
2. **Staging integration test** (`tlm_integration_test`). It runs in four steps:
   - `tlm_setup` creates the Zerobus table, the grants and the reference data.
   - `tlm_generator` sends 3 minutes of data through Zerobus and files, with 2% injected defects.
   - A pipeline update processes it.
   - The `dq_gate` notebook checks the event-log expectation results and the quarantine rate. It then reconciles every injected defect from the generator's manifest:
     - hard defects must be in quarantine
     - soft defects must be in silver, flagged by the soft rules
     - duplicates must be gone

   Any mismatch fails the job, and the prod deploy never starts.
3. **Prod:** manual approval (GitHub environment protection), then `bundle deploy -t prod`. The continuous pipeline picks up the new code. `tlm_dq_monitor` then checks the live data every hour.

**Identity.** Use one service principal per environment, stored as GitHub environment secrets: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`. Prefer workload identity federation (GitHub OIDC) over secrets where the account supports it. Don't use personal access tokens in CI.

## Commands

```bash
databricks bundle validate --strict -t dev           # resolve and check config for a target
databricks bundle deploy -t dev                       # deploy your isolated dev copy
databricks bundle run tlm_integration_test -t dev     # end-to-end test with the DQ gate
databricks bundle summary -t staging                  # what is deployed where
databricks bundle deploy -t staging --var max_hard_violation_rate=0.02   # one-off override
databricks bundle destroy -t dev                      # remove your dev copy
```

## Moving to separate workspaces

Only the target blocks change:

```yaml
  staging:
    mode: production
    workspace:
      host: https://<staging-workspace>.cloud.databricks.com
      root_path: /Workspace/Shared/.bundle/${bundle.name}/${bundle.target}
    run_as:
      service_principal_name: <staging-deployer-sp-application-id>
    variables:
      catalog: mfg_staging
      zerobus_endpoint: <staging-workspace-id>.zerobus.<region>.cloud.databricks.com
      zerobus_sp_app_id: <staging-gateway-sp-application-id>
```

Per workspace, create the following once: the catalog, a gateway service principal for Zerobus, and a secret scope for the test generator (staging only). Everything else is created by the bundle and `tlm_setup`.

The `prod` target in `databricks.yml` holds placeholders (`<prod-workspace>`, `<prod-deployer-sp-application-id>` and others). Replace them before the first prod deploy. Until then, `bundle validate -t prod` fails on the placeholder host.
