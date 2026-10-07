import csv

import yaml

from ingest.config import REPO_ROOT

PROJECT = yaml.safe_load((REPO_ROOT / "dbt" / "dbt_project.yml").read_text())


def test_sample_encounter_classes_are_all_accepted():
    accepted = set(PROJECT["vars"]["encounter_classes"])
    with (REPO_ROOT / "sample_data" / "encounters.csv").open(newline="", encoding="utf-8") as f:
        seen = {row["ENCOUNTERCLASS"].lower() for row in csv.DictReader(f)}
    assert seen <= accepted, f"add to vars.encounter_classes: {sorted(seen - accepted)}"


POLICIES_SQL = (REPO_ROOT / "snowflake" / "policies.sql").read_text().lower()


def test_every_dbt_masking_policy_is_defined_updatable_and_grantable():
    for name in PROJECT["vars"]["masking_policies"]:
        assert f"create masking policy if not exists {name} " in POLICIES_SQL
        # ALTER ... SET BODY, because CREATE OR REPLACE fails while the policy is attached.
        assert f"alter masking policy {name} set body" in POLICIES_SQL
        assert f"grant apply on masking policy {name} to role transformer" in POLICIES_SQL
    assert "create or replace masking policy" not in POLICIES_SQL


def test_policies_unmask_only_for_the_exact_phi_reader_role():
    # CURRENT_ROLE, not IS_ROLE_IN_SESSION: the role hierarchy and secondary roles must not unmask.
    assert "is_role_in_session" not in POLICIES_SQL
    assert POLICIES_SQL.count("current_role() = 'phi_reader'") == len(
        PROJECT["vars"]["masking_policies"]
    )


def test_bootstrap_never_grants_marts_tables_ahead_of_masking():
    bootstrap = (REPO_ROOT / "snowflake" / "bootstrap.sql").read_text().lower()
    # Table-level SELECT on MARTS comes only from secure_model(), after masking is attached.
    assert "future tables in schema healthcare.marts" not in bootstrap
    assert "all tables in schema healthcare.marts" not in bootstrap


def test_service_user_cannot_borrow_roles_through_secondary_roles():
    bootstrap = (REPO_ROOT / "snowflake" / "bootstrap.sql").read_text().lower()
    # Otherwise a LOADER connection also carries TRANSFORMER and PLATFORM_ADMIN privileges.
    assert "alter user hdp_service set default_secondary_roles = ()" in bootstrap
