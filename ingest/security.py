"""Apply snowflake/policies.sql. Re-run every pipeline run; every statement is idempotent."""

from __future__ import annotations

import logging
from pathlib import Path

log = logging.getLogger(__name__)
POLICY_FILE = "policies.sql"


def split_statements(sql: str) -> list[str]:
    """Split on ';' after stripping '--' comments. Fine for our policy file, which has no
    semicolons or '--' inside string literals."""
    without_comments = "\n".join(line.split("--", 1)[0] for line in sql.splitlines())
    return [" ".join(s.split()) for s in without_comments.split(";") if s.strip()]


def apply_security(conn, sql_dir: Path) -> int:
    statements = split_statements((sql_dir / POLICY_FILE).read_text())
    cur = conn.cursor()
    try:
        for statement in statements:
            log.info("snowflake: %s", statement[:120])
            cur.execute(statement)
    finally:
        cur.close()
    return len(statements)
