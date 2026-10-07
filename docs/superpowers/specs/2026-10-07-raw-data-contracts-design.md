# Data Contracts on RAW: Design

**Date:** 2026-10-07
**Status:** Approved design, pending implementation plan
**Builds on:** `2026-10-06-healthcare-data-platform-design.md` ("Data contracts on the RAW layer" under next milestones)

## 1. Purpose

Check every batch's CSVs against a declared contract before anything is loaded to RAW. Today the
pipeline checks only headers (in `generate`) and, after transformation, the keys in the dbt staging
tests. Nothing checks the values that land in RAW. With contracts, a broken batch (unparseable dates,
null or duplicate keys, unknown codes, orphaned rows, a near-empty file) stops before the warehouse,
or is recorded as a warning, and either way shows in Grafana.

### Success criteria

- An `error` violation fails the new `check_contracts` task. `load_raw` and everything after it
  don't run, and RAW keeps the last good batch.
- A `warn` violation is recorded and logged, and the run continues.
- Every rule's result, passing or failing, is in `observability.contract_results` and visible on the
  Grafana dashboard.
- Example values are never stored or logged for columns marked `pii: true`.
- The committed `sample_data/` and a real-Synthea run both pass every `error` rule.
- The contracts are the only place RAW's expected columns are declared: `REQUIRED_COLUMNS` is removed.

### Constraints

- No new dependencies. Checks use DuckDB (in-memory) and PyYAML, both already in the project.
- Behaves the same in DuckDB and Snowflake mode, because it reads the local CSVs and never the warehouse.
- Rule names follow dbt's (`not_null`, `unique`, `accepted_values`, `relationships`).

### Out of scope

Statistical or distribution checks (null-rate drift, value ranges over time), contracts on the
staging or mart layers, the Open Data Contract Standard format, and alerting beyond Grafana.

## 2. Decisions

| Decision | Choice | Why |
|---|---|---|
| On violation | Per-rule severity, `error` (default) or `warn` | Mirrors dbt's severity model; one odd value need not stop the nightly run when we choose so |
| Where checks run | Pre-load, on the CSVs, in a new task | Bad data never reaches RAW; no warehouse credits; same for both warehouses |
| Engine | Own checker: YAML compiled to DuckDB SQL | About 200 lines, no new dependency; 1 GB/day of CSV checks in seconds; reasoning stays visible |
| Rejected | Soda Core; dbt source tests; row-by-row Pydantic | Heavy dependency; data lands in RAW first and costs credits; too slow at 1 GB/day |

## 3. Contract files

`contracts/<table>.yml`, one per RAW table: `patients`, `encounters`, `conditions`,
`medications`, `claims`, `claims_transactions`, `providers`, `payers`. Example:

```yaml
table: encounters
min_rows: 1
columns:
  ID:             {type: uuid, tests: [not_null, unique]}
  START:          {type: timestamp, tests: [not_null]}
  STOP:           {type: timestamp}
  PATIENT:        {type: uuid, pii: true, tests: [not_null, {relationships: {to: patients, field: ID}}]}
  ENCOUNTERCLASS: {type: string, tests: [{accepted_values: {values_from: {dbt_var: encounter_classes}, severity: warn}}]}
  BASE_ENCOUNTER_COST: {type: decimal}
```

Semantics:
- **Columns:** every listed column must be in the CSV header (case-insensitive, matching the
  loader's upper-casing). Extra columns are allowed and loaded as today. The listed columns are
  those dbt reads, i.e. what `REQUIRED_COLUMNS` lists today.
- **Types:** `string`, `uuid`, `date`, `timestamp`, `decimal`, `integer`. A non-empty value that
  doesn't parse as the type is a violation. An empty value is null. Type checks are always `error`.
- **Tests:** `not_null`, `unique`, `accepted_values` (inline `values: [...]` or
  `values_from: {dbt_var: <name>}`, read from `dbt/dbt_project.yml`; comparison is case-insensitive),
  `relationships` (`to: <table>, field: <column>`, checked within the same batch; nulls are skipped).
  Each test may set `severity: warn`; the default is `error`.
- **`min_rows`:** the table's minimum row count (default 1), `error`.
- **`pii: true`:** marks a column whose example values must never be stored or logged.

`load_contracts` rejects an unknown key, rule, type or severity, a `relationships` target that has
no contract, and a file whose `table:` doesn't match its file name. The error names the file and key.

## 4. Pipeline and components

```
generate → check_contracts → load_raw → apply_security → dbt_build → record_metrics
```

- **`ingest/contracts.py`**
  - `load_contracts(directory) -> dict[str, Contract]`
  - `check_batch(batch_dir, contracts, dbt_vars) -> list[RuleResult]`. One in-memory DuckDB
    connection; one view per CSV with every column read as text (`all_varchar`); one SQL query per
    rule returning the failing row count and up to 5 distinct example values.
  - `RuleResult(table, column, rule, severity, failing_rows, examples)`. `column` is empty for
    table-level rules (`file_present`, `readable`, `columns`, `min_rows`).
- **`ingest/cli.py`:** `check-contracts` command, wrapped in `_track(...)` like the other tasks, so it
  appears in `task_runs`. It logs one line per failing rule and a summary (`2 errors, 1 warning`),
  writes results to Postgres best-effort, and exits non-zero only when an `error` result failed.
- **`airflow/dags/patient_pipeline.py`:** a new BashOperator between `generate` and `load_raw`.
- **`ingest/synthea.py`:** `REQUIRED_COLUMNS` is removed; `validate_headers` takes its column lists
  from the contracts, so `generate` keeps its fast header check.

## 5. Results and observability

New table (added to `ingest/sql/observability.sql`):

```sql
create table if not exists observability.contract_results (
    run_id        text    not null,
    table_name    text    not null,
    column_name   text    not null default '',
    rule          text    not null,
    severity      text    not null,
    failing_rows  bigint  not null,
    examples      text[]  not null default '{}',
    primary key (run_id, table_name, column_name, rule)
);
```

One row per rule per run, passing rules included. Re-running a task for the same `run_id`
replaces that run's rows.

Grafana gets two panels: a stat for the latest run's failing rules (red if any `error` failed,
yellow for warnings only, green otherwise), and a table of the latest run's failing rules.

## 6. Error handling

| Condition | Result |
|---|---|
| Contract file invalid | `check_contracts` fails before reading data; message names the file and key |
| CSV missing | `file_present` violation, `error` |
| CSV unparseable by DuckDB | `readable` violation, `error` |
| Listed column missing | `columns` violation, `error`; the table's other rules are skipped |
| `relationships` target table missing or unreadable | that rule is skipped (the target's own `file_present`/`readable` error already fails the run) |
| Postgres unavailable | Results not stored (logged warning); checks still gate the load |
| Failing rule on a `pii: true` column | Count recorded and logged; `examples` empty |

## 7. Testing

- **Unit (`tests/test_contracts.py`), tiny CSVs in `tmp_path`:** each type and rule passes and fails
  with exact `failing_rows` and examples; `warn` exits 0 and `error` exits 1; `pii: true` keeps
  counts but no examples; `file_present`, `readable` and the skip-after-missing-column behavior;
  `load_contracts` rejects typos with the file named.
- **Real contracts:** `sample_data/` passes every `error` rule. Every column dbt tags
  `meta: {pii: true}` in `_staging.yml`, mapped back to its RAW column through the staging SQL
  (`"RAW" as name`), is `pii: true` in its contract, as is every patient-reference column. The
  existing `test_sample_encounter_classes_are_all_accepted` is replaced by the contract check.
- **Pipeline:** the dag-integrity check covers the new task order; `cmd_check_contracts` is tested
  for its exit code and, with `TEST_POSTGRES_URL`, the stored rows.
- **Live, once:** a real-Synthea run passes on the running stack, and the Grafana panels show its
  results. A rule that real Synthea breaks but the sample passes is a finding: loosen it or mark it
  `warn`, and record why in the contract file.

## 8. Documentation

README: the pipeline diagram and task list gain `check_contracts`; a short "Data contracts" section
explains the files, severities and PII handling; a design-decisions bullet records pre-load checks
and the rejected alternatives; *Next milestones* drops data contracts.
