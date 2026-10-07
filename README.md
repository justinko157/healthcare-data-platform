# Healthcare Data Platform

[![ci](https://github.com/justinko157/healthcare-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/justinko157/healthcare-data-platform/actions/workflows/ci.yml)

A daily pipeline for synthetic patient records: **Synthea → Airflow → Snowflake (or DuckDB) → dbt**.
It **masks direct identifiers and enforces role-based access** in Snowflake and is monitored live in **Grafana**.
Every record is synthetic; no real patient data is ever used.

```mermaid
flowchart LR
  S[Synthea<br/>2,000 patients/day] --> G[generate]
  subgraph dag["Airflow DAG: patient_pipeline"]
    G --> L[load_raw] --> P[apply_security] --> D[dbt_build] --> M[record_metrics]
  end
  L --> RAW[(RAW)]
  D --> STG[(STAGING / INTERMEDIATE)] --> MARTS[(MARTS<br/>masked)]
  P -. masking policies .-> MARTS
  G & L & P & D & M -. run metrics .-> PG[(Postgres<br/>observability)]
  PG --> GF[Grafana<br/>dashboard + alert]
  MARTS --> A[ANALYST: masked]
  MARTS --> R[PHI_READER: unmasked]
```

## Quickstart (no accounts needed)

```bash
git clone https://github.com/justinko157/healthcare-data-platform
cd healthcare-data-platform
docker compose up -d --build
```

- Airflow: http://localhost:8080 (no login in this local demo). `patient_pipeline` starts on its own. On the first `up`, the `seed-history` service backfills 14 days from the bundled sample data. It re-checks that window on every `up` and fills any missing dates with sample data.
- Grafana: http://localhost:3000. "Patient Pipeline Health" is the home dashboard (admin / admin).

The first build takes a few minutes (Java and Synthea are installed). The scheduled daily run uses real Synthea with 2,000 patients; only the backfilled history comes from sample data.
Built on Airflow 3.1.0, dbt-core 1.12, DuckDB 1.4, Grafana 12.2 and Synthea v3.3.0.
On Linux, run `echo "AIRFLOW_UID=$(id -u)" >> .env` first so the containers can write `./data`.

Local development without Docker:

```bash
uv sync
uv run pytest
uv run python -m ingest.cli generate --batch-date 2026-01-01 --use-sample
uv run python -m ingest.cli load --batch-date 2026-01-01
uv run python -m ingest.cli dbt-build --batch-date 2026-01-01
```

These commands use the same `data/warehouse.duckdb` and `data/batches/` as the running stack. Stop the stack first (`docker compose stop`), or point them somewhere else:

```bash
export DATA_DIR=/tmp/hdp-dev DUCKDB_PATH=/tmp/hdp-dev/warehouse.duckdb
```

**Disk use.** Real Synthea writes about 1 GB of CSV per day at 2,000 patients. The pipeline keeps only the 3 newest batch directories under `data/batches/` and 30 days of batches in RAW. DuckDB doesn't shrink its file when rows are deleted; run `CHECKPOINT` or recreate `data/warehouse.duckdb` to reclaim space. To generate less, set a smaller `SYNTHEA_POPULATION` in `.env` (for example `SYNTHEA_POPULATION=500`) and run `docker compose up -d`.

## Snowflake mode: masking and least-privilege roles

1. Start a Snowflake trial and run `./scripts/snowflake_keygen.sh`.
2. In Snowsight, as ACCOUNTADMIN, run `snowflake/bootstrap.sql` (fill in the public key and your login name).
3. Copy `.env.example` to `.env` (on Linux, keep your `AIRFLOW_UID` line: uncomment it there), set `WAREHOUSE=snowflake`, `SNOWFLAKE_ACCOUNT` and `SNOWFLAKE_PRIVATE_KEY_PATH=/opt/project/secrets/hdp_service_key.p8` (the key from step 1), then run `docker compose up -d`.
4. Trigger `patient_pipeline`, then run `snowflake/demo_queries.sql`.

| Role | Can do |
|---|---|
| `LOADER` | write `RAW` (Airflow ingest) |
| `TRANSFORMER` | read `RAW`, build `STAGING`/`INTERMEDIATE`/`MARTS` (dbt) |
| `ANALYST` | read `MARTS`; direct identifiers masked (names, SSN, license, passport, street address, ZIP); birth and death dates cut to the year; patient ID hashed |
| `PHI_READER` | read `MARTS` unmasked |
| `PLATFORM_ADMIN` | own and update masking policies |

### Masked vs unmasked

The sample outputs for the `ANALYST` and `PHI_READER` roles will be added after the first Snowflake run. Until then, [`snowflake/demo_queries.sql`](snowflake/demo_queries.sql) has the exact queries: the same three patients as `ANALYST` (masked) and as `PHI_READER` (unmasked), a join-and-count as `ANALYST`, and a check that `ANALYST` cannot read `RAW`.

Analysts can still join and count, because `patient_id` is hashed the same way in every mart.

This masks direct identifiers, which makes the `ANALYST` view a limited data set. It is **not** HIPAA Safe Harbor de-identification: analysts still see exact encounter, discharge and readmission timestamps, city, county and state, exact age in years, and gender, race, ethnicity and marital status.

**DuckDB has no masking.** In DuckDB mode the data is unmasked. The PII declarations are still tested: `assert_pii_columns_are_masked` fails the build if any mart column holding PII lacks a masking policy, or if a mart has a column not documented in YAML.

## Data model

```mermaid
flowchart LR
  subgraph staging
    sp[stg_patients] & se[stg_encounters] & sc[stg_claims] & sct[stg_claims_transactions] & spr[stg_providers] & spy[stg_payers]
  end
  sc & sct --> ict[int_encounter_claim_totals]
  se & spr & spy & ict --> iee[int_encounters_enriched]
  iee --> fe[fct_encounters] & fr[fct_readmissions_30d] & ad[agg_daily_utilization]
  sp --> dp[dim_patients]
  spr --> dpr[dim_providers]
```

Tests: `unique`/`not_null` on keys, `relationships` from facts to dimensions, and `accepted_values` on encounter class. Custom tests check that encounter stop ≥ start, that claim totals equal the sum of their lines, that the readmission rate is in [0, 1], and PII coverage. Source freshness warns after 26 hours.

## Observability

![Patient Pipeline Health dashboard](docs/img/dashboard.png)

Every pipeline step records its own outcome in Postgres (`observability` schema): run status, per-task duration, rows loaded per table, every dbt node result, and KPIs. Grafana reads it through a read-only login.

- **Status:** last run, hours since the last successful load (red after 26 hours), dbt test pass rate.
- **Trends:** rows loaded, durations, failing dbt tests.
- **Healthcare:** encounters per day, 30-day readmission rate, average claim cost by payer.

The alert **"Patient pipeline stale or failed"** fires when there has been no successful load for 26 hours or the latest run failed. To get notified, add a contact point in Grafana (Alerting → Contact points → email or Slack webhook) and route the `severity=critical` label to it.

## Design decisions

- **Idempotent loads.** Each day is a batch (`_BATCH_ID`). A load deletes that batch and reloads it in one transaction, so rerunning a day never duplicates rows.
- **No unmasked window.** dbt rebuilds marts with `CREATE OR REPLACE`, which drops masking policies. The `secure_model` post-hook re-attaches the policies **before** granting `SELECT` to analysts.
- **Masking that can't be bypassed by role inheritance.** Policies check `CURRENT_ROLE() = 'PHI_READER'`, so neither SYSADMIN's role hierarchy nor secondary roles unmask data. Policies are updated with `ALTER ... SET BODY`, because Snowflake refuses `CREATE OR REPLACE` on a policy that is attached to a column.
- **The pipeline never holds ACCOUNTADMIN.** Account setup (`bootstrap.sql`) is a one-time human step. The service user is key-pair only and holds LOADER, TRANSFORMER and PLATFORM_ADMIN.
- **Airflow orchestrates; the CLI does the work.** Every task runs `python -m ingest.cli ...` in its own venv, so dbt and the Snowflake connector never conflict with Airflow's packages, and everything runs the same locally.
- **The DAG run fails when any step fails.** `record_metrics` runs after failures (`all_done`) so they reach the dashboard. It then exits non-zero so Airflow's run state matches.
- **Postgres instead of Prometheus for metrics.** These are per-run batch facts, not scraped time series. Grafana reads Postgres natively, and it's already in the stack.
- **One DuckDB writer, one Airflow pool slot.** DuckDB allows a single writer at a time, so the default pool has one slot and backfill runs share the warehouse with the scheduled run instead of failing on the file lock.
- **DuckDB fallback.** The repo keeps working after the Snowflake trial ends, and CI needs no secrets.
- **Unsalted SHA-256 for patient IDs** is fine here because Synthea IDs are random UUIDs. Real systems should use a keyed hash (HMAC) so low-entropy identifiers can't be brute-forced.

## Next milestones

- Terraform for Snowflake roles, warehouses and grants
- OpenTelemetry tracing across the Airflow tasks
- Data contracts on the RAW layer
