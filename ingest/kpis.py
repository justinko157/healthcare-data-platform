"""Healthcare KPIs read from the marts. The SQL is portable across DuckDB and Snowflake."""

from __future__ import annotations

from collections.abc import Callable

SCALAR_KPIS = {
    "encounters_per_day": """
        select avg(encounter_count) from marts.agg_daily_utilization
        where encounter_date > (select max(encounter_date) from marts.agg_daily_utilization) - 30
    """,
    "readmission_rate_30d": """
        select avg(case when is_readmitted then 1.0 else 0.0 end) from marts.fct_readmissions_30d
    """,
}

CLAIM_COST_BY_PAYER = """
    select payer_name, avg(claim_line_total) from marts.fct_encounters
    where claim_line_total is not null and payer_name is not null
    group by payer_name
"""


def compute_kpis(query: Callable[[str], list[tuple]]) -> list[tuple[str, float]]:
    out = []
    for name, sql in SCALAR_KPIS.items():
        ((value,),) = query(sql)
        if value is not None:
            out.append((name, float(value)))
    for payer, value in query(CLAIM_COST_BY_PAYER):
        out.append((f"avg_claim_cost:{payer}", float(value)))
    return out
