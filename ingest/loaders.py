"""Load a batch of Synthea CSVs into RAW.<table>, idempotently, on DuckDB or Snowflake.

Idempotency: each load deletes the batch's rows (by _BATCH_ID) and reloads them inside one
transaction, so rerunning a day never duplicates rows.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

import duckdb

from ingest.synthea import TABLES, read_header

if TYPE_CHECKING:
    from ingest.config import Settings, SnowflakeSettings

log = logging.getLogger(__name__)
_BATCH_ID = re.compile(r"^\d{8}$")
RAW_RETENTION_DAYS = 30  # RAW keeps this many days of batches before the current one


class Loader(Protocol):
    def load_table(self, table: str, csv_path: Path, batch_id: str) -> int: ...
    def prune_before(self, table: str, batch_id: str) -> int: ...
    def query(self, sql: str) -> list[tuple]: ...
    def close(self) -> None: ...


@dataclass(frozen=True)
class LoadResult:
    table: str
    status: str  # "success" | "failed"
    rows_loaded: int | None = None
    error: str | None = None


def _check(table: str, batch_id: str) -> None:
    # Both values end up in SQL text, so only known tables and YYYYMMDD ids are allowed.
    if table not in TABLES:
        raise ValueError(f"unknown table {table!r}")
    if not _BATCH_ID.match(batch_id):
        raise ValueError(f"batch_id must be YYYYMMDD; got {batch_id!r}")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class DuckDBLoader:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(path))
        self.con.execute("create schema if not exists raw")

    def load_table(self, table: str, csv_path: Path, batch_id: str) -> int:
        _check(table, batch_id)
        select_cols = ", ".join(f'"{c}" as "{c.upper()}"' for c in read_header(csv_path))
        source = f"read_csv({_sql_literal(str(csv_path))}, header = true, all_varchar = true)"
        self.con.execute("begin transaction")
        try:
            self.con.execute(
                f"create table if not exists raw.{table} as select {select_cols}, "
                "cast(null as varchar) as _BATCH_ID, cast(null as timestamptz) as _LOADED_AT "
                f"from {source} limit 0"
            )
            self.con.execute(f"delete from raw.{table} where _BATCH_ID = ?", [batch_id])
            self.con.execute(
                f"insert into raw.{table} by name select {select_cols}, "
                f"? as _BATCH_ID, now() as _LOADED_AT from {source}",
                [batch_id],
            )
            (rows,) = self.con.execute(
                f"select count(*) from raw.{table} where _BATCH_ID = ?", [batch_id]
            ).fetchone()
            self.con.execute("commit")
        except Exception:
            self.con.execute("rollback")
            raise
        return int(rows)

    def prune_before(self, table: str, batch_id: str) -> int:
        """Delete RAW rows from batches older than batch_id. Returns rows deleted."""
        _check(table, batch_id)
        (rows,) = self.con.execute(
            f"delete from raw.{table} where _BATCH_ID < ?", [batch_id]
        ).fetchone()
        return int(rows)

    def query(self, sql: str) -> list[tuple]:
        return self.con.execute(sql).fetchall()

    def close(self) -> None:
        self.con.close()


class SnowflakeLoader:
    STAGE = "RAW.LOAD_STAGE"

    def __init__(self, conn):
        self.conn = conn
        self._stage_ready = False

    def _ensure_stage(self, cur) -> None:
        # Lazy: only LOADER may create the stage, and KPI queries use this class as TRANSFORMER.
        if not self._stage_ready:
            cur.execute(
                f"create stage if not exists {self.STAGE} file_format = (type = csv "
                "skip_header = 1 field_optionally_enclosed_by = '\"' empty_field_as_null = true)"
            )
            self._stage_ready = True

    def load_table(self, table: str, csv_path: Path, batch_id: str) -> int:
        _check(table, batch_id)
        columns = [c.upper() for c in read_header(csv_path)]
        target = f"RAW.{table.upper()}"
        col_defs = ", ".join(f'"{c}" varchar' for c in columns)
        quoted = ", ".join(f'"{c}"' for c in columns)
        positions = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
        staged = f"@{self.STAGE}/{batch_id}/{csv_path.name}.gz"
        cur = self.conn.cursor()
        try:
            self._ensure_stage(cur)
            cur.execute(
                f"create table if not exists {target} "
                f"({col_defs}, _BATCH_ID varchar, _LOADED_AT timestamp_tz)"
            )
            # PUT cannot run inside a transaction, so it goes first.
            cur.execute(
                f"put 'file://{csv_path.resolve().as_posix()}' @{self.STAGE}/{batch_id}/ "
                "overwrite = true auto_compress = true"
            )
            cur.execute("begin")
            cur.execute(f"delete from {target} where _BATCH_ID = %s", (batch_id,))
            cur.execute(
                f"copy into {target} ({quoted}, _BATCH_ID, _LOADED_AT) "
                f"from (select {positions}, '{batch_id}', current_timestamp() from {staged}) "
                "force = true"
            )
            cur.execute(f"select count(*) from {target} where _BATCH_ID = %s", (batch_id,))
            (rows,) = cur.fetchone()
            cur.execute("commit")
        except Exception:
            cur.execute("rollback")
            raise
        finally:
            cur.close()
        return int(rows)

    def prune_before(self, table: str, batch_id: str) -> int:
        """Delete RAW rows from batches older than batch_id. Returns rows deleted."""
        _check(table, batch_id)
        cur = self.conn.cursor()
        try:
            cur.execute(f"delete from RAW.{table.upper()} where _BATCH_ID < %s", (batch_id,))
            return int(cur.rowcount or 0)
        finally:
            cur.close()

    def query(self, sql: str) -> list[tuple]:
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            return cur.fetchall()
        finally:
            cur.close()

    def close(self) -> None:
        self.conn.close()


def load_all(loader: Loader, batch_dir: Path, batch_id: str) -> list[LoadResult]:
    """Load every table independently; one failure doesn't stop the others."""
    results = []
    for table in TABLES:
        path = batch_dir / f"{table}.csv"
        try:
            if not path.is_file():
                raise FileNotFoundError(f"{path} not found")
            results.append(LoadResult(table, "success", loader.load_table(table, path, batch_id)))
        except Exception as exc:
            log.exception("loading %s failed", table)
            results.append(LoadResult(table, "failed", error=f"{type(exc).__name__}: {exc}"[:2000]))
    return results


def prune_raw(loader: Loader, before_batch_id: str) -> dict[str, int]:
    """Delete every RAW table's rows from batches older than before_batch_id.

    Unlike load_all, any error propagates: retention is data management, not best effort.
    """
    return {table: loader.prune_before(table, before_batch_id) for table in TABLES}


def snowflake_connection(sf: SnowflakeSettings, role: str):
    import snowflake.connector  # imported lazily so DuckDB mode never needs it

    return snowflake.connector.connect(
        account=sf.account,
        user=sf.user,
        authenticator="SNOWFLAKE_JWT",
        private_key_file=str(sf.private_key_path),
        role=role,
        warehouse=sf.warehouse,
        database=sf.database,
    )


def get_loader(settings: Settings, role: str) -> Loader:
    """role is a Snowflake role (LOADER for ingest, TRANSFORMER for KPI reads).

    DuckDB ignores it.
    """
    if settings.warehouse == "duckdb":
        return DuckDBLoader(settings.duckdb_path)
    return SnowflakeLoader(snowflake_connection(settings.require_snowflake(), role))
