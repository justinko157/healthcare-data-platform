-- Observability schema for the patient pipeline. Idempotent: safe to run on every connect.
create schema if not exists observability;

create table if not exists observability.pipeline_runs (
    run_id      text primary key,
    batch_date  date        not null,
    warehouse   text        not null,
    started_at  timestamptz not null,
    finished_at timestamptz,
    status      text        not null default 'running'
);
-- Added after first release. Guarded so the ALTER (and its exclusive lock) only runs on a volume
-- that doesn't have the column yet, not on every connect.
do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_schema = 'observability' and table_name = 'pipeline_runs'
          and column_name = 'data_source'
    ) then
        alter table observability.pipeline_runs add column data_source text;
    end if;
end $$;

create table if not exists observability.task_runs (
    run_id     text not null,
    task_id    text not null,
    state      text not null,
    started_at timestamptz,
    duration_s double precision,
    error      text,
    primary key (run_id, task_id)
);

create table if not exists observability.load_stats (
    run_id      text        not null,
    table_name  text        not null,
    rows_loaded bigint,
    status      text        not null,
    error       text,
    loaded_at   timestamptz not null default now(),
    primary key (run_id, table_name)
);

create table if not exists observability.dbt_results (
    run_id        text not null,
    node          text not null,
    resource_type text not null,
    status        text not null,
    execution_s   double precision,
    failures      integer,
    primary key (run_id, node)
);

create table if not exists observability.kpi_snapshots (
    run_id      text        not null,
    metric      text        not null,
    value       double precision,
    captured_at timestamptz not null default now(),
    primary key (run_id, metric)
);
