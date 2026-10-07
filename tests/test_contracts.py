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
