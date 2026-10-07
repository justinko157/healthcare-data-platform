import yaml

from ingest.config import REPO_ROOT

PROJECT = yaml.safe_load((REPO_ROOT / "dbt" / "dbt_project.yml").read_text())


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
