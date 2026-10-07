"""Rules that span the Terraform config and the rest of the repo (offline; no Snowflake)."""

import re

import hcl2

from ingest.config import REPO_ROOT

TF = REPO_ROOT / "terraform"


def _unquote(value):
    # python-hcl2 keeps the quotes around string literals.
    return value.strip('"') if isinstance(value, str) else value


def _load(name: str) -> dict:
    with (TF / name).open(encoding="utf-8") as f:
        return hcl2.load(f)


def declared_roles() -> set[str]:
    return {_unquote(r) for block in _load("roles.tf")["locals"] for r in block.get("roles", [])}


def test_terraform_never_uses_accountadmin():
    roles = [_unquote(cfg["role"]) for p in _load("providers.tf")["provider"] for cfg in p.values()]
    assert sorted(roles) == ["SECURITYADMIN", "SYSADMIN"]


def test_declared_roles_cover_every_role_masking_relies_on():
    policies = (REPO_ROOT / "snowflake" / "policies.sql").read_text()
    used = set(re.findall(r"current_role\(\) = '([A-Z_]+)'", policies))
    used |= set(re.findall(r"to role ([A-Z_]+)", policies))

    secure_model = (REPO_ROOT / "dbt" / "macros" / "secure_model.sql").read_text()
    loop = re.search(r"for role in \[([^\]]*)\]", secure_model)
    assert loop, "secure_model's grant loop not found"
    used |= set(re.findall(r"'([A-Z_]+)'", loop.group(1)))

    assert used == {"PHI_READER", "TRANSFORMER", "ANALYST"}
    assert used <= declared_roles()


def test_bootstrap_ownership_transfers_keep_existing_grants():
    # Without COPY CURRENT GRANTS, Snowflake refuses to move a role that is granted to SYSADMIN
    # ("Dependent grant of privilege 'USAGE' ... exists").
    snippet = (TF / "bootstrap_terraform_user.sql").read_text().lower()
    transfers = [line for line in snippet.splitlines() if line.startswith("grant ownership")]
    assert len(transfers) == 13
    assert all(line.endswith("copy current grants;") for line in transfers)
