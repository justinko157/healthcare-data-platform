import textwrap
from pathlib import Path

import pytest

from ingest import contracts


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text).lstrip(), encoding="utf-8")
    return path


PARENT = """
    table: parent
    columns:
      ID: {type: uuid, tests: [not_null, unique]}
"""


def test_load_contracts_parses_columns_tests_and_defaults(tmp_path):
    write(tmp_path / "parent.yml", PARENT)
    write(
        tmp_path / "child.yml",
        """
        table: child
        min_rows: 5
        columns:
          Parent_Id: {type: uuid, pii: true, tests: [{relationships: {to: parent, field: id}}]}
          KIND: {type: string, tests: [{accepted_values: {values: [a, b], severity: warn}}]}
          CLASS: {type: string, tests: [{accepted_values: {values_from: {dbt_var: classes}}}]}
        """,
    )
    loaded = contracts.load_contracts(tmp_path)
    assert set(loaded) == {"parent", "child"}
    child = loaded["child"]
    assert child.min_rows == 5
    assert loaded["parent"].min_rows == 1
    parent_id, kind, cls = child.columns
    assert (parent_id.name, parent_id.type, parent_id.pii) == ("PARENT_ID", "uuid", True)
    assert parent_id.tests == (contracts.Test("relationships", to="parent", field="ID"),)
    assert kind.tests == (contracts.Test("accepted_values", "warn", values=("a", "b")),)
    assert cls.tests == (contracts.Test("accepted_values", values_from_var="classes"),)
    assert loaded["parent"].columns[0].tests == (
        contracts.Test("not_null"),
        contracts.Test("unique"),
    )


def test_required_columns_lists_each_tables_columns(tmp_path):
    write(tmp_path / "parent.yml", PARENT)
    assert contracts.required_columns(contracts.load_contracts(tmp_path)) == {"parent": ("ID",)}


def test_dbt_vars_reads_the_project_vars(tmp_path):
    write(tmp_path / "dbt_project.yml", "name: x\nvars:\n  classes: [a, b]\n")
    assert contracts.dbt_vars(tmp_path) == {"classes": ["a", "b"]}


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ("table: other\ncolumns:\n  ID: {type: uuid}\n", "table: must be 'bad'"),
        ("table: bad\ncolumns:\n  ID: {type: uuid}\ncolour: red\n", "unknown key(s): colour"),
        ("table: bad\ncolumns:\n  ID: {type: guid}\n", "type must be one of"),
        ("table: bad\ncolumns:\n  ID: {type: uuid, tests: [not_nul]}\n", "unknown test 'not_nul'"),
        (
            "table: bad\ncolumns:\n  ID: {type: uuid, tests: [{unique: {severity: eror}}]}\n",
            "severity must be error or warn, not 'eror'",
        ),
        (
            "table: bad\ncolumns:\n"
            "  ID: {type: uuid, tests: [{relationships: {to: nowhere, field: ID}}]}\n",
            "relationships target 'nowhere' has no contract",
        ),
        (
            "table: bad\ncolumns:\n  K: {type: string, tests: [{accepted_values: {}}]}\n",
            "set exactly one of values, values_from",
        ),
        ("table: bad\nmin_rows: -1\ncolumns:\n  ID: {type: uuid}\n", "min_rows must be"),
        ("table: bad\ncolumns: {}\n", "columns: must be a non-empty mapping"),
        ("table: [bad\n", "invalid YAML"),
    ],
)
def test_load_contracts_rejects_mistakes_naming_the_file(tmp_path, body, message):
    (tmp_path / "bad.yml").write_text(body, encoding="utf-8")
    with pytest.raises(contracts.ContractError) as exc:
        contracts.load_contracts(tmp_path)
    assert "bad.yml" in str(exc.value)
    assert message in str(exc.value)


def test_relationships_field_must_exist_in_the_target(tmp_path):
    write(tmp_path / "parent.yml", PARENT)
    write(
        tmp_path / "child.yml",
        """
        table: child
        columns:
          P: {type: uuid, tests: [{relationships: {to: parent, field: NAME}}]}
        """,
    )
    with pytest.raises(contracts.ContractError, match="child.yml.*parent has no column NAME"):
        contracts.load_contracts(tmp_path)


def test_an_empty_contracts_directory_is_an_error(tmp_path):
    with pytest.raises(contracts.ContractError, match="no contracts"):
        contracts.load_contracts(tmp_path)


def run_check(tmp_path, contract_files: dict[str, str], csvs: dict[str, str], dbt_vars=None):
    cdir, bdir = tmp_path / "contracts", tmp_path / "batch"
    bdir.mkdir(parents=True, exist_ok=True)
    for table, body in contract_files.items():
        write(cdir / f"{table}.yml", body)
    for table, body in csvs.items():
        write(bdir / f"{table}.csv", body)
    return contracts.check_batch(bdir, contracts.load_contracts(cdir), dbt_vars or {})


def result(results, table, column, rule):
    matches = [r for r in results if (r.table, r.column, r.rule) == (table, column, rule)]
    assert len(matches) == 1, f"{table}.{column} {rule}: {matches}"
    return matches[0]


U1 = "00000000-0000-0000-0000-000000000001"
U2 = "00000000-0000-0000-0000-000000000002"

TYPED = """
    table: t
    columns:
      U: {type: uuid}
      D: {type: date}
      TS: {type: timestamp}
      N: {type: decimal}
      I: {type: integer}
      S: {type: string}
"""


def test_type_checks_flag_values_that_do_not_parse_and_treat_empty_as_null(tmp_path):
    csv = f"""
        U,D,TS,N,I,S
        {U1},2025-12-01,2025-12-01T10:00:00Z,100.25,3,anything
        not-a-uuid,2025-13-40,not-a-time,"1,5",2.5,
        ,,,,,
    """
    results = run_check(tmp_path, {"t": TYPED}, {"t": csv})
    for column, type_, bad in [
        ("U", "uuid", "not-a-uuid"),
        ("D", "date", "2025-13-40"),
        ("TS", "timestamp", "not-a-time"),
        ("N", "decimal", "1,5"),
        ("I", "integer", "2.5"),
    ]:
        r = result(results, "t", column, f"type:{type_}")
        assert (r.failing_rows, r.examples, r.severity) == (1, (bad,), "error"), column
    assert not [r for r in results if r.column == "S" and r.failed]


def test_not_null_and_unique(tmp_path):
    contract = """
        table: t
        columns:
          ID: {type: string, tests: [not_null, unique]}
    """
    results = run_check(tmp_path, {"t": contract}, {"t": "ID\na\na\nb\n\n\n"})
    assert result(results, "t", "ID", "not_null").failing_rows == 2
    unique = result(results, "t", "ID", "unique")
    assert (unique.failing_rows, unique.examples) == (2, ("a",))


def test_accepted_values_inline_is_case_insensitive_and_skips_nulls(tmp_path):
    contract = """
        table: t
        columns:
          K: {type: string, tests: [{accepted_values: {values: [x, y]}}]}
    """
    r = result(
        run_check(tmp_path, {"t": contract}, {"t": "K\nX\ny\nz\n\n"}), "t", "K", "accepted_values"
    )
    assert (r.failing_rows, r.examples) == (1, ("z",))


def test_accepted_values_from_a_dbt_var(tmp_path):
    contract = """
        table: t
        columns:
          K: {type: string, tests: [{accepted_values: {values_from: {dbt_var: kinds}}}]}
    """
    results = run_check(tmp_path, {"t": contract}, {"t": "K\nx\nq\n"}, dbt_vars={"kinds": ["x"]})
    assert result(results, "t", "K", "accepted_values").examples == ("q",)


def test_an_unknown_dbt_var_is_a_contract_error(tmp_path):
    contract = """
        table: t
        columns:
          K: {type: string, tests: [{accepted_values: {values_from: {dbt_var: kinds}}}]}
    """
    with pytest.raises(contracts.ContractError, match="dbt var 'kinds'"):
        run_check(tmp_path, {"t": contract}, {"t": "K\nx\n"})


CHILD = """
    table: child
    columns:
      P: {type: string, tests: [{relationships: {to: parent, field: ID}}]}
"""
PARENT_S = """
    table: parent
    columns:
      ID: {type: string}
"""


def test_relationships_flag_orphans_within_the_batch(tmp_path):
    results = run_check(
        tmp_path,
        {"child": CHILD, "parent": PARENT_S},
        {"child": "P\np1\np9\n\n", "parent": "Id\np1\np2\n"},
    )
    r = result(results, "child", "P", "relationships:parent.ID")
    assert (r.failing_rows, r.examples) == (1, ("p9",))


def test_relationships_are_skipped_when_the_target_file_is_missing(tmp_path):
    results = run_check(tmp_path, {"child": CHILD, "parent": PARENT_S}, {"child": "P\np1\n"})
    assert result(results, "parent", "", "file_present").failing_rows == 1
    assert not [r for r in results if r.rule.startswith("relationships")]


def test_min_rows_counts_the_shortfall(tmp_path):
    contract = "table: t\nmin_rows: 3\ncolumns:\n  A: {type: string}\n"
    r = result(run_check(tmp_path, {"t": contract}, {"t": "A\nx\n"}), "t", "", "min_rows")
    assert (r.failing_rows, r.examples, r.severity) == (2, ("1 rows",), "error")


def test_a_missing_file_is_a_file_present_error(tmp_path):
    results = run_check(tmp_path, {"t": "table: t\ncolumns:\n  A: {type: string}\n"}, {})
    assert [(r.rule, r.failing_rows, r.severity) for r in results] == [("file_present", 1, "error")]


def test_an_unparseable_file_is_a_readable_error(tmp_path):
    # DuckDB's sniffer tolerates most malformed text (it reads 0 rows, which min_rows catches);
    # binary garbage is what it refuses outright.
    (tmp_path / "batch").mkdir()
    (tmp_path / "batch" / "t.csv").write_bytes(b"\xff\xfe\x00\x01garbage\x00\n\x00\x00")
    results = run_check(tmp_path, {"t": "table: t\ncolumns:\n  A: {type: string}\n"}, {})
    r = result(results, "t", "", "readable")
    assert (r.failing_rows, r.examples) == (1, ())
    assert {x.rule for x in results} == {"file_present", "readable"}


def test_a_missing_column_skips_the_tables_other_rules(tmp_path):
    contract = """
        table: t
        columns:
          A: {type: string, tests: [not_null]}
          B: {type: integer}
    """
    results = run_check(tmp_path, {"t": contract}, {"t": "A\n\n"})
    r = result(results, "t", "", "columns")
    assert (r.failing_rows, r.examples) == (1, ("B",))
    assert {x.rule for x in results} == {"file_present", "readable", "columns"}


def test_header_matching_is_case_insensitive(tmp_path):
    contract = "table: t\ncolumns:\n  ID: {type: string, tests: [not_null]}\n"
    results = run_check(tmp_path, {"t": contract}, {"t": "Id\nx\n"})
    assert result(results, "t", "", "columns").failing_rows == 0
    assert result(results, "t", "ID", "not_null").failing_rows == 0


def test_pii_columns_report_counts_but_never_examples(tmp_path):
    contract = """
        table: t
        columns:
          SSN: {type: integer, pii: true, tests: [unique]}
    """
    results = run_check(tmp_path, {"t": contract}, {"t": "SSN\n999-00\n999-00\n"})
    for rule in ("type:integer", "unique"):
        r = result(results, "t", "SSN", rule)
        assert (r.failing_rows, r.examples) == (2, ()), rule


def test_severity_comes_from_the_test_and_passing_rules_are_reported(tmp_path):
    contract = """
        table: t
        columns:
          K: {type: string, tests: [{not_null: {severity: warn}}, unique]}
    """
    results = run_check(tmp_path, {"t": contract}, {"t": "K\n\nx\n"})
    assert result(results, "t", "K", "not_null").severity == "warn"
    unique = result(results, "t", "K", "unique")
    assert (unique.severity, unique.failed) == ("error", False)
