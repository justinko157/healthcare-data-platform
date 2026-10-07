# Healthcare Data Platform: Design

**Date:** 2026-10-06
**Owner:** Justin Ko (github.com/justinko157)
**Status:** Approved design, pending implementation plan

## 1. Purpose

A public portfolio project demonstrating end-to-end data engineering in the **healthcare** domain, with a focus on **Snowflake**, **Grafana live monitoring**, and **PHI/PII handling with role-based access and masking**. It also exercises **Airflow**, **dbt with tests**, **Python**, **Docker**, and **GitHub Actions CI**.

### Success criteria

- Public repo with a green CI badge and a README a reader can understand in two minutes.
- Runs from the README with one command (`docker compose up`) and no paid account, using DuckDB.
- Runs end to end against a Snowflake trial account, demonstrating masking and role-based access.
- Every component is easy to explain; the design favors clarity over cleverness.
- Built in about one weekend (10-15 hours).

### Constraints

- **Synthetic data only** (Synthea). No real patient data, ever.
- **Snowflake is a 30-day trial.** DuckDB is a first-class fallback so the repo keeps working, and CI never needs Snowflake credentials.
- **Out of scope for this milestone:** Terraform, OpenTelemetry, Kubernetes. The README lists them as next milestones.

## 2. Architecture

Approach A: a local Docker Compose stack with Snowflake (or DuckDB) as the warehouse.

| Service | Role |
|---|---|
| `airflow` | Airflow standalone (single container), runs the DAG |
| `postgres` | Airflow metadata plus an `observability` schema for run metrics |
| `grafana` | Reads `observability` from Postgres; datasource and dashboard provisioned from files |

Snowflake is external. DuckDB is a file under `data/`. One environment variable selects the target: `WAREHOUSE=duckdb` (default) or `WAREHOUSE=snowflake`.

### Repository layout

```
healthcare-data-platform/
├── docker-compose.yml
├── airflow/dags/patient_pipeline.py      # the DAG
├── ingest/                               # Python package: generate, load, record metrics
│   ├── synthea.py   loaders.py   metrics.py
├── dbt/                                  # dbt project with snowflake + duckdb profiles
│   ├── models/staging/  intermediate/  marts/
│   └── tests/
├── snowflake/                            # versioned SQL: roles, grants, masking policies, demo queries
├── grafana/provisioning/                 # datasource + dashboard JSON + alert rule
├── sample_data/                          # small Synthea extract for CI and the DuckDB demo
├── tests/                                # pytest for ingest/
└── .github/workflows/ci.yml
```

Each unit has one job: `ingest/` moves data and records metrics; `dbt/` transforms and tests; `snowflake/` manages security; Grafana only reads.

## 3. Data flow

DAG `patient_pipeline`: daily schedule, also manually triggerable. Five tasks:

1. **`generate`.** Runs Synthea (Java, in its own small Docker image) for about 2,000 patients, writing CSVs to a dated batch folder. If Synthea is unavailable, `--use-sample` uses `sample_data/` instead, so the demo always works.
2. **`load_raw`.** Loads each CSV into `RAW.<table>`: patients, encounters, conditions, medications, claims, claims_transactions (line level, needed for the claim-totals test), providers, payers.
   - Idempotent: deletes the batch's rows (by `_batch_id`) before reloading.
   - Adds `_batch_id` and `_loaded_at` to every row.
   - Snowflake path: internal stage + `COPY INTO`. DuckDB path: `read_csv`. Both implement one `Loader` interface.
3. **`dbt_build`.** `dbt build` (models and tests together):
   - **staging:** `stg_*`, renamed and typed; PII columns kept but tagged.
   - **intermediate:** e.g. `int_encounters_enriched`.
   - **marts:** `fct_encounters`, `dim_patients` (PII, masked), `dim_providers`, `fct_readmissions_30d`, `agg_daily_utilization`.
4. **`record_metrics`.** Writes observability rows (row counts, freshness, dbt results parsed from `run_results.json`, task durations and states). Uses `trigger_rule=all_done` so it runs even when upstream tasks fail.
5. **`apply_security`** (Snowflake only). Applies the `snowflake/` SQL. Idempotent (`CREATE OR REPLACE` / `IF NOT EXISTS`), so it is safe to run every time.

Healthcare KPIs (from `agg_daily_utilization` and `fct_readmissions_30d`): encounters per day, 30-day readmission rate, average claim cost by payer.

## 4. PHI protection (Snowflake)

**PII columns** (Synthea): name fields, birthdate, SSN, address, phone, driver's license. Tagged in dbt schema YAML with `meta: {pii: true}`, so the protected-field list lives in one place.

**Roles** (`snowflake/roles.sql`), least privilege:

| Role | Access | Use |
|---|---|---|
| `LOADER` | write `RAW` only | Airflow ingest service account |
| `TRANSFORMER` | read `RAW`, write `ANALYTICS` | dbt |
| `ANALYST` | read `ANALYTICS` marts; PII masked | analysts, dashboards |
| `PHI_READER` | read `ANALYTICS` unmasked | the few people allowed to see identities |

**Masking policies** (`snowflake/masking.sql`) on `dim_patients`:

- Name, SSN, phone, license: real value for `PHI_READER`; `***MASKED***` for everyone else.
- Birthdate: birth year only for roles other than `PHI_READER` (age analysis still works).
- Patient ID: SHA-256 hash for `ANALYST`, so joins and counts still work.

**Proof:** `snowflake/demo_queries.sql` runs the same `SELECT` as `ANALYST` and as `PHI_READER`; the README shows both outputs side by side.

**DuckDB fallback:** no masking support. The README states this plainly. A dbt test still asserts that every PII-tagged column appears in a repo-checked masking-policy manifest, so the DuckDB path also catches an unprotected field.

## 5. Observability and Grafana

### Tables (`observability` schema, Postgres)

| Table | Grain | Key columns |
|---|---|---|
| `pipeline_runs` | DAG run | run_id, started_at, finished_at, status, warehouse |
| `task_runs` | task × run | run_id, task_id, state, duration_s |
| `load_stats` | table × run | run_id, table_name, rows_loaded, loaded_at |
| `dbt_results` | node × run | run_id, node, type (model/test), status, execution_s |
| `kpi_snapshots` | metric × run | run_id, metric, value |

Only `ingest/metrics.py` writes these. The schema is created by one init SQL file when Postgres starts.

### Dashboard: "Patient Pipeline Health" (provisioned JSON)

- **Row 1, status:** last run status (green/red); hours since last successful load (red above 26h); dbt test pass rate for the latest run.
- **Row 2, trends:** rows loaded per table over time; DAG and task durations over time; dbt test failures over time by test name.
- **Row 3, healthcare:** encounters per day; 30-day readmission rate; average claim cost by payer.

**Alert:** a provisioned Grafana alert rule fires when freshness exceeds 26 hours or a run fails. No notification channel by default; the README shows how to add email or Slack.

**Backfill:** a `backfill` command runs the pipeline for about 14 past dates, so trend panels have history on first startup.

## 6. Error handling

- Load and dbt tasks retry twice with backoff. `generate` falls back to the sample instead of retrying.
- `load_raw` loads each table independently and records each result; if any table fails, the task fails after recording all of them.
- `record_metrics` always runs (`all_done`), so failed runs appear on the dashboard.
- Missing Snowflake credentials fail fast with a clear message.
- Batch-based delete-then-load makes reruns safe: rerunning a day never duplicates rows.

## 7. Testing and CI

**dbt tests**
- Generic: `unique` / `not_null` on keys; `relationships` between facts and dimensions; `accepted_values` on encounter class.
- Custom singular tests:
  - encounter stop >= start
  - claim totals equal the sum of their lines
  - readmission rate between 0 and 1
  - every PII-tagged column is covered by a masking policy
- Source freshness on `RAW`.

**pytest**
- `Loader` idempotency, run on DuckDB.
- Metrics parsing against a fixture `run_results.json`.
- DAG import integrity.
- Warehouse selection logic.

**GitHub Actions** (on push and pull request): ruff → pytest → `dbt build` on DuckDB with `sample_data/`. No secrets required, so forks pass too. Status badge in the README.

## 8. README outline

1. One-paragraph pitch, plus an architecture diagram.
2. Quickstart: `docker compose up` (DuckDB), then open Grafana and Airflow.
3. Snowflake mode: trial signup, `.env`, `snowflake/` scripts, and the masked-vs-unmasked demo with outputs.
4. Data model diagram (staging → intermediate → marts).
5. Observability: dashboard screenshot and the alert rule.
6. Design decisions: idempotent loads, least-privilege roles, why Postgres instead of Prometheus for metrics, why DuckDB fallback.
7. Next milestones: Terraform for Snowflake resources, OpenTelemetry tracing, data contracts.

## 9. Decisions log

| Decision | Choice | Why |
|---|---|---|
| Scope | Weekend core; Terraform and OpenTelemetry deferred | Ship a working core quickly, then iterate |
| Warehouse | Snowflake trial + DuckDB fallback | Closes the Snowflake gap; repo keeps running after the trial; CI needs no secrets |
| Data | Synthea | Credible in health tech; realistic PII to protect; relational structure for dbt |
| Grafana focus | Mostly pipeline health, plus one healthcare KPI row | Pipeline health is the gap; the KPI row shows domain knowledge |
| Architecture | Docker Compose (Airflow standalone, Postgres, Grafana) | One-command run, free, production-shaped |
| Metrics store | Postgres tables, not Prometheus | Grafana reads Postgres natively; Prometheus is unneeded for a weekend scope |
| Location | Standalone public repo | Self-contained; no private or employer files |
