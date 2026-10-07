# Terraform for Snowflake Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the hand-run `snowflake/bootstrap.sql` with a Terraform config that declares the Snowflake account setup, adopts the existing trial account through gated `import` blocks, and is checked offline in CI.

**Architecture:** One flat Terraform config in `terraform/`, using the `snowflakedb/snowflake` provider through two aliases: `sysadmin` for the warehouse, database and schemas, and `securityadmin` for roles, users and grants. Both authenticate as a key-pair service user `TERRAFORM`. Roles and grants are maps driven by `for_each`. Import IDs are computed unconditionally in locals, and the `import` blocks use them only when `var.adopt_existing_account` is true. Terraform always runs in the pinned Docker image through `scripts/tf.sh`.

**Tech Stack:** Terraform 1.16.5 (`hashicorp/terraform:1.16.5` image), provider `snowflakedb/snowflake` 2.21.x, tflint v0.64.0 (`ghcr.io/terraform-linters/tflint:v0.64.0`), `terraform test` with `mock_provider`, pytest + `python-hcl2`, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-10-07-terraform-snowflake-design.md`

## Global Constraints

- Neither Terraform nor the pipeline ever uses ACCOUNTADMIN. Only a human uses it, once, for `terraform/bootstrap_terraform_user.sql`.
- No new local installs: Terraform and tflint run only in Docker (`scripts/tf.sh`, CI). Python deps go through `uv add`.
- CI needs no secrets. Every Terraform check in CI is offline (`init -backend=false`, `validate`, `test` with mocked providers, tflint).
- Terraform owns account setup only. `snowflake/policies.sql` and the pipeline's `apply_security` step are not touched.
- Role, user, warehouse, database and schema names are exactly: roles `LOADER`, `TRANSFORMER`, `ANALYST`, `PHI_READER`, `PLATFORM_ADMIN`; user `HDP_SERVICE`; warehouse `HDP_WH`; database `HEALTHCARE`; schemas `RAW`, `STAGING`, `INTERMEDIATE`, `MARTS`, `SECURITY`.
- No SELECT grant and no future grant on MARTS for ANALYST or PHI_READER. `HDP_SERVICE` has secondary roles off (`default_secondary_roles_option = "NONE"`).
- State is local (`terraform/terraform.tfstate`) and gitignored. So are `terraform/terraform.tfvars` and `terraform/.terraform/`. `terraform/.terraform.lock.hcl` is committed.
- This repo uses the GitHub account justinko157. For `gh` commands, prefix `GH_TOKEN=$(gh auth token -u justinko157)`. No Claude co-author trailers in commits.
- Windows/Git Bash: any `docker run` that passes container paths needs `MSYS_NO_PATHCONV=1`. `scripts/tf.sh` sets it.

## Deviations from the spec (decided while verifying the provider docs)

1. **The PLATFORM_ADMIN warehouse-usage cut is done in the bootstrap snippet, not shown by `plan`.** A grant resource only tracks privileges that are in its config. Terraform cannot see, or revoke, a grant that no resource describes. The alternative, `strict_privilege_management`, is a provider *experimental preview* feature, so it isn't used. The TRANSFORMER CREATE TABLE cut **does** appear in `plan`, because its import IDs list the live privileges, including CREATE TABLE.
2. **The "no ACCOUNTADMIN in providers" rule is checked by pytest, not `terraform test`.** Provider blocks can't be referenced in `terraform test` assertions.
3. **The "no imports when not adopting" rule is tested as two separate checks.** `terraform test` checks that the gated import maps are empty and that the unconditional `local.adoption_ids` maps cover every managed instance. Running imports against a mocked provider isn't reliable.

## Review Focus

1. **Plan noise after adoption.** Optional fields such as comments, `initially_suspended` and user defaults must not produce diffs on imported objects, or the "second plan shows no changes" success check fails. Task 7 pins this live. Tasks 2 and 4 avoid setting fields the old `bootstrap.sql` didn't set.
2. **Import-ID drift.** Every import ID must match the provider's exact format and the live object's case. For example, the human user is stored upper-case (`JUSTINKO`). Task 5's `terraform test` asserts the ID map keys equal the resource keys. Task 7's live `plan` is the ground truth.
3. **Running against the wrong account state.** If `adopt_existing_account` is wrong, `apply` fails with "already exists" or "does not exist". The README (Task 6) and `scripts/tf.sh` messaging (Task 1) say which flag to use.
4. **Missing key file.** `scripts/tf.sh plan` or `apply` without `secrets/terraform_key.p8` must fail before Docker starts, and print the keygen command (Task 1 test).
5. **Pipeline breakage from the PLATFORM_ADMIN cut.** If `apply_security` needs warehouse usage after the revoke, the live pipeline run in Task 7 fails. Re-add the grant to `local.warehouse_users` and record why (Task 7 step).

## File structure

```
terraform/
  versions.tf                 # terraform + provider version pins
  providers.tf                # sysadmin and securityadmin aliases, SNOWFLAKE_JWT as TERRAFORM
  variables.tf                # account ids, key paths, human_user, adopt_existing_account
  warehouse.tf                # HDP_WH
  database.tf                 # HEALTHCARE + 5 schemas, local.schema_fqn
  roles.tf                    # 5 roles (local.roles) + each granted to SYSADMIN
  grants.tf                   # privilege matrix
  users.tf                    # HDP_SERVICE, its roles, human user's roles
  imports.tf                  # local.adoption_ids (always) + gated import blocks
  outputs.tf
  .tflint.hcl
  terraform.tfvars.example
  bootstrap_terraform_user.sql
  .terraform.lock.hcl         # committed
  tests/fixtures/dummy_key.p8, tests/fixtures/dummy_key.pub
  tests/foundation.tftest.hcl, tests/grants.tftest.hcl, tests/users.tftest.hcl, tests/imports.tftest.hcl
scripts/tf.sh                 # pinned Terraform in Docker
scripts/snowflake_keygen.sh   # gains a key-name argument
tests/test_terraform.py       # pytest: provider roles, role-set drift vs policies.sql/secure_model
.github/workflows/ci.yml      # new "terraform" job
README.md                     # Snowflake mode via Terraform
snowflake/bootstrap.sql       # deleted (Task 6)
```

---

### Task 1: Terraform scaffold, `tf.sh`, keygen key names

**Files:**
- Create: `terraform/versions.tf`, `terraform/providers.tf`, `terraform/variables.tf`, `terraform/outputs.tf`, `terraform/terraform.tfvars.example`, `terraform/.tflint.hcl`, `terraform/tests/fixtures/dummy_key.p8`, `terraform/tests/fixtures/dummy_key.pub`, `scripts/tf.sh`, `terraform/.terraform.lock.hcl` (generated)
- Modify: `scripts/snowflake_keygen.sh`, `.gitignore`

**Interfaces:**
- Produces: provider aliases `snowflake.sysadmin` and `snowflake.securityadmin`. Variables: `organization_name`, `account_name`, `terraform_user` (default `"TERRAFORM"`), `terraform_private_key_path` (default `"../secrets/terraform_key.p8"`), `service_public_key_path` (default `"../secrets/hdp_service_key.pub"`), `human_user` (string), `adopt_existing_account` (bool, default `false`).
- Produces: `./scripts/tf.sh <terraform args>` runs Terraform with the repo mounted at `/work` and working dir `/work/terraform`. `./scripts/snowflake_keygen.sh [name]` writes `secrets/<name>_key.p8` and `.pub`; the default name is `hdp_service`.

- [ ] **Step 1: Update `.gitignore`** by appending:

```
# Terraform
terraform/.terraform/
terraform/terraform.tfstate
terraform/terraform.tfstate.*
terraform/terraform.tfvars
terraform/crash.log
```

- [ ] **Step 2: Write `scripts/tf.sh`**

```bash
#!/usr/bin/env bash
# Run the pinned Terraform image against terraform/. The repo is mounted at /work, so the
# config's default key paths (../secrets/...) resolve inside the container.
#   ./scripts/tf.sh init | plan | apply | test | fmt -check -recursive | validate
set -euo pipefail
cd "$(dirname "$0")/.."
TF_IMAGE="hashicorp/terraform:1.16.5"

case "${1:-}" in
  plan|apply|destroy|import|refresh|state|output)
    if [ ! -f secrets/terraform_key.p8 ]; then
      echo "secrets/terraform_key.p8 not found. Create it with: ./scripts/snowflake_keygen.sh terraform" >&2
      echo "then run terraform/bootstrap_terraform_user.sql in Snowsight (see README 'Snowflake mode')." >&2
      exit 2
    fi
    ;;
esac

TTY=()
if [ -t 0 ] && [ -t 1 ]; then TTY=(-t); fi
export MSYS_NO_PATHCONV=1
exec docker run --rm -i "${TTY[@]}" -v "$PWD:/work" -w /work/terraform "$TF_IMAGE" "$@"
```

Run: `chmod +x scripts/tf.sh && git update-index --add --chmod=+x scripts/tf.sh` (after the first `git add`).

- [ ] **Step 3: Test the key guard** (the key doesn't exist yet in a fresh checkout; skip if it does)

Run: `mv secrets/terraform_key.p8 /tmp/ 2>/dev/null; ./scripts/tf.sh plan; echo "exit=$?"`
Expected: the two-line message and `exit=2`. No Docker output. Move the key back if you moved one.

- [ ] **Step 4: Give the keygen script a key-name argument.** Replace `scripts/snowflake_keygen.sh` with:

```bash
#!/usr/bin/env bash
# Create a Snowflake key pair in ./secrets (gitignored) and print the public key.
#   ./scripts/snowflake_keygen.sh              -> secrets/hdp_service_key.{p8,pub}  (pipeline user)
#   ./scripts/snowflake_keygen.sh terraform    -> secrets/terraform_key.{p8,pub}    (Terraform user)
set -euo pipefail
cd "$(dirname "$0")/.."
NAME="${1:-hdp_service}"
KEY="secrets/${NAME}_key.p8"
PUB="secrets/${NAME}_key.pub"
mkdir -p secrets
if [ -f "$KEY" ]; then
  echo "$KEY already exists; not overwriting." >&2
else
  openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out "$KEY" -nocrypt
  chmod 600 "$KEY"
fi
openssl rsa -in "$KEY" -pubout -out "$PUB" 2>/dev/null
echo "Public key for ${NAME} (paste where the bootstrap snippet asks for it):"
grep -v -- '-----' "$PUB" | tr -d '\n'
echo
```

Run: `./scripts/snowflake_keygen.sh | tail -2` (the existing key is kept)
Expected: "secrets/hdp_service_key.p8 already exists; not overwriting.", then the same public key as before. Compare with `grep -v -- '-----' secrets/hdp_service_key.pub | tr -d '\n'`.

- [ ] **Step 5: Write `terraform/versions.tf`**

```hcl
terraform {
  required_version = "~> 1.16"

  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.21"
    }
  }
}
```

- [ ] **Step 6: Write `terraform/variables.tf`**

```hcl
variable "organization_name" {
  description = "Snowflake organization name: the part before the dash in the account identifier (e.g. MVIMJXA)."
  type        = string
}

variable "account_name" {
  description = "Snowflake account name: the part after the dash in the account identifier (e.g. PA76665)."
  type        = string
}

variable "terraform_user" {
  description = "Key-pair service user Terraform logs in as (created by bootstrap_terraform_user.sql)."
  type        = string
  default     = "TERRAFORM"
}

variable "terraform_private_key_path" {
  description = "Private key for terraform_user, relative to terraform/."
  type        = string
  default     = "../secrets/terraform_key.p8"
}

variable "service_public_key_path" {
  description = "Public key for HDP_SERVICE (the pipeline's user), relative to terraform/."
  type        = string
  default     = "../secrets/hdp_service_key.pub"
}

variable "human_user" {
  description = "Your own Snowflake user. Terraform grants it ANALYST and PHI_READER but never manages the user itself."
  type        = string
}

variable "adopt_existing_account" {
  description = "true only for an account first set up with the old snowflake/bootstrap.sql: import its objects instead of creating them."
  type        = bool
  default     = false
}
```

- [ ] **Step 7: Write `terraform/providers.tf`**

```hcl
# Two aliases, so each object is owned by the role Snowflake recommends for it:
# SYSADMIN owns the warehouse, database and schemas; SECURITYADMIN owns roles, users and grants.
# Neither is ACCOUNTADMIN (tests/test_terraform.py enforces that).
locals {
  terraform_private_key = file(var.terraform_private_key_path)
}

provider "snowflake" {
  alias             = "sysadmin"
  organization_name = var.organization_name
  account_name      = var.account_name
  user              = var.terraform_user
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = local.terraform_private_key
  role              = "SYSADMIN"
}

provider "snowflake" {
  alias             = "securityadmin"
  organization_name = var.organization_name
  account_name      = var.account_name
  user              = var.terraform_user
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = local.terraform_private_key
  role              = "SECURITYADMIN"
}
```

- [ ] **Step 8: Write `terraform/outputs.tf`** (empty for now; later tasks add outputs)

```hcl
# Outputs are added alongside the resources they describe.
```

- [ ] **Step 9: Write `terraform/terraform.tfvars.example`**

```hcl
# Copy to terraform.tfvars (gitignored) and fill in.
# Account identifier from Snowsight (profile menu -> Account -> Copy account identifier),
# e.g. "MVIMJXA-PA76665" -> organization_name = "MVIMJXA", account_name = "PA76665".
organization_name = "MYORG"
account_name      = "MYACCOUNT"

# Your Snowflake user name as Snowflake stores it (SHOW USERS; usually upper case).
human_user = "MY_LOGIN"

# true only if this account was first set up with the old snowflake/bootstrap.sql.
adopt_existing_account = false
```

- [ ] **Step 10: Write `terraform/.tflint.hcl`**

```hcl
plugin "terraform" {
  enabled = true
  preset  = "recommended"
}
```

- [ ] **Step 11: Write the test fixtures** (fake keys, never real ones)

`terraform/tests/fixtures/dummy_key.p8`:
```
not-a-real-key: offline terraform tests use mock providers and never connect
```
`terraform/tests/fixtures/dummy_key.pub`:
```
-----BEGIN PUBLIC KEY-----
AAAAdummy
BBBBdummy
-----END PUBLIC KEY-----
```

- [ ] **Step 12: Init, lock both Linux platforms, format, validate**

```bash
./scripts/tf.sh init -backend=false -input=false
./scripts/tf.sh providers lock -platform=linux_amd64 -platform=linux_arm64
./scripts/tf.sh fmt -check -recursive
./scripts/tf.sh validate
MSYS_NO_PATHCONV=1 docker run --rm -v "$PWD:/data" -w /data/terraform ghcr.io/terraform-linters/tflint:v0.64.0
```

Expected: init installs `snowflakedb/snowflake v2.21.x`, then `fmt` prints nothing, `validate` prints "Success! The configuration is valid.", and tflint prints nothing (exit 0). tflint may warn that the variables in `variables.tf` are unused. That's acceptable at this step only, because Tasks 2–5 use them. Record the warning text in your report.

- [ ] **Step 13: Commit**

```bash
git add .gitignore scripts/tf.sh scripts/snowflake_keygen.sh terraform/
git update-index --chmod=+x scripts/tf.sh
git commit -m "feat(terraform): scaffold with pinned Docker runner and key-pair providers"
```

---

### Task 2: Warehouse, database, schemas, roles

**Files:**
- Create: `terraform/warehouse.tf`, `terraform/database.tf`, `terraform/roles.tf`, `terraform/tests/foundation.tftest.hcl`
- Modify: `terraform/outputs.tf`

**Interfaces:**
- Consumes: the provider aliases from Task 1.
- Produces: `snowflake_warehouse.hdp`, `snowflake_database.healthcare`, `snowflake_schema.this["RAW"|"STAGING"|"INTERMEDIATE"|"MARTS"|"SECURITY"]`, `local.schemas` (list), `local.schema_fqn` (map schema → `"\"HEALTHCARE\".\"<S>\""`, built from resource names so it is known at plan time), `local.roles` (list of the 5 role names), `snowflake_account_role.this[<role>]`, `snowflake_grant_account_role.to_sysadmin[<role>]`.

- [ ] **Step 1: Write the failing test** `terraform/tests/foundation.tftest.hcl`

```hcl
mock_provider "snowflake" {
  alias = "sysadmin"
}

mock_provider "snowflake" {
  alias = "securityadmin"
}

variables {
  organization_name          = "TESTORG"
  account_name               = "TESTACCOUNT"
  human_user                 = "TESTUSER"
  terraform_private_key_path = "tests/fixtures/dummy_key.p8"
  service_public_key_path    = "tests/fixtures/dummy_key.pub"
}

run "foundation" {
  command = plan

  assert {
    condition     = snowflake_warehouse.hdp.name == "HDP_WH" && snowflake_warehouse.hdp.warehouse_size == "XSMALL" && snowflake_warehouse.hdp.auto_suspend == 60
    error_message = "HDP_WH must be XSMALL with auto_suspend 60."
  }

  assert {
    condition     = snowflake_database.healthcare.name == "HEALTHCARE"
    error_message = "Database must be HEALTHCARE."
  }

  assert {
    condition     = toset(keys(snowflake_schema.this)) == toset(["RAW", "STAGING", "INTERMEDIATE", "MARTS", "SECURITY"])
    error_message = "Exactly the five pipeline schemas."
  }

  assert {
    condition     = local.schema_fqn["MARTS"] == "\"HEALTHCARE\".\"MARTS\""
    error_message = "schema_fqn must be the quoted fully qualified name."
  }

  assert {
    condition     = toset(keys(snowflake_account_role.this)) == toset(["LOADER", "TRANSFORMER", "ANALYST", "PHI_READER", "PLATFORM_ADMIN"])
    error_message = "Exactly the five pipeline roles."
  }

  assert {
    condition     = alltrue([for g in snowflake_grant_account_role.to_sysadmin : g.parent_role_name == "SYSADMIN"])
    error_message = "Every pipeline role rolls up to SYSADMIN."
  }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./scripts/tf.sh test -filter=tests/foundation.tftest.hcl`
Expected: FAIL with errors like `Reference to undeclared resource ... snowflake_warehouse.hdp`.

- [ ] **Step 3: Write `terraform/warehouse.tf`**

```hcl
resource "snowflake_warehouse" "hdp" {
  provider            = snowflake.sysadmin
  name                = "HDP_WH"
  warehouse_size      = "XSMALL"
  auto_suspend        = 60
  auto_resume         = "true"
  initially_suspended = true

  lifecycle {
    # Only meaningful at creation; an adopted warehouse must not show a diff for it.
    ignore_changes = [initially_suspended]
  }
}
```

- [ ] **Step 4: Write `terraform/database.tf`**

```hcl
locals {
  schemas = ["RAW", "STAGING", "INTERMEDIATE", "MARTS", "SECURITY"]

  # Quoted fully qualified names, built from resource names (known at plan time, and they keep
  # the dependency on the schema). Grants and imports use these.
  schema_fqn = {
    for s in local.schemas :
    s => "\"${snowflake_database.healthcare.name}\".\"${snowflake_schema.this[s].name}\""
  }
}

resource "snowflake_database" "healthcare" {
  provider = snowflake.sysadmin
  name     = "HEALTHCARE"
}

resource "snowflake_schema" "this" {
  for_each = toset(local.schemas)
  provider = snowflake.sysadmin
  database = snowflake_database.healthcare.name
  name     = each.key
}
```

- [ ] **Step 5: Write `terraform/roles.tf`**

```hcl
locals {
  # LOADER: Airflow ingest, writes RAW only. TRANSFORMER: dbt, reads RAW and builds the modeled
  # schemas. ANALYST: reads MARTS with PII masked. PHI_READER: reads MARTS unmasked.
  # PLATFORM_ADMIN: owns the masking policies (snowflake/policies.sql).
  # tests/test_terraform.py checks this list covers every role policies.sql and secure_model use.
  roles = ["LOADER", "TRANSFORMER", "ANALYST", "PHI_READER", "PLATFORM_ADMIN"]
}

resource "snowflake_account_role" "this" {
  for_each = toset(local.roles)
  provider = snowflake.securityadmin
  name     = each.key
}

# Roll every custom role up to SYSADMIN (Snowflake's recommended role hierarchy).
resource "snowflake_grant_account_role" "to_sysadmin" {
  for_each         = toset(local.roles)
  provider         = snowflake.securityadmin
  role_name        = snowflake_account_role.this[each.key].name
  parent_role_name = "SYSADMIN"
}
```

- [ ] **Step 6: Add outputs** by replacing `terraform/outputs.tf` with:

```hcl
output "roles" {
  description = "The pipeline's account roles."
  value       = sort(keys(snowflake_account_role.this))
}

output "warehouse" {
  description = "Warehouse the pipeline and analysts use."
  value       = snowflake_warehouse.hdp.name
}
```

- [ ] **Step 7: Run tests, fmt, validate**

Run: `./scripts/tf.sh test -filter=tests/foundation.tftest.hcl && ./scripts/tf.sh fmt -check -recursive && ./scripts/tf.sh validate`
Expected: `1 passed, 0 failed`, no fmt output, and "Success! The configuration is valid."

- [ ] **Step 8: Commit**

```bash
git add terraform/
git commit -m "feat(terraform): warehouse, database, schemas and roles"
```

---

### Task 3: Privilege matrix

**Files:**
- Create: `terraform/grants.tf`, `terraform/tests/grants.tftest.hcl`

**Interfaces:**
- Consumes: `snowflake_account_role.this[<role>]`, `snowflake_warehouse.hdp`, `snowflake_database.healthcare`, `local.roles`, and `local.schema_fqn` from Task 2.
- Produces:
  - `local.warehouse_users` (list of role names)
  - `local.schema_grants` (map `"<ROLE>/<SCHEMA>"` → list of privileges)
  - `snowflake_grant_privileges_to_account_role.warehouse_usage[<role>]`, `.database_usage[<role>]`, `.schema["<ROLE>/<SCHEMA>"]`, `.transformer_raw_all_tables`, `.transformer_raw_future_tables`

- [ ] **Step 1: Write the failing test** `terraform/tests/grants.tftest.hcl`

```hcl
mock_provider "snowflake" {
  alias = "sysadmin"
}

mock_provider "snowflake" {
  alias = "securityadmin"
}

variables {
  organization_name          = "TESTORG"
  account_name               = "TESTACCOUNT"
  human_user                 = "TESTUSER"
  terraform_private_key_path = "tests/fixtures/dummy_key.p8"
  service_public_key_path    = "tests/fixtures/dummy_key.pub"
}

run "privilege_matrix" {
  command = plan

  # Masking invariant: analysts get only USAGE on MARTS; table SELECT comes from dbt's
  # secure_model post-hook, after masking is attached.
  assert {
    condition     = snowflake_grant_privileges_to_account_role.schema["ANALYST/MARTS"].privileges == toset(["USAGE"]) && snowflake_grant_privileges_to_account_role.schema["PHI_READER/MARTS"].privileges == toset(["USAGE"])
    error_message = "ANALYST and PHI_READER must have only USAGE on MARTS."
  }

  assert {
    condition     = length([for k, v in local.schema_grants : k if contains(v, "SELECT")]) == 0
    error_message = "No schema-level SELECT grants."
  }

  # The only schema-object (all/future) grants are TRANSFORMER's reads of RAW.
  assert {
    condition = (
      snowflake_grant_privileges_to_account_role.transformer_raw_future_tables.account_role_name == "TRANSFORMER" &&
      snowflake_grant_privileges_to_account_role.transformer_raw_future_tables.on_schema_object[0].future[0].in_schema == "\"HEALTHCARE\".\"RAW\"" &&
      snowflake_grant_privileges_to_account_role.transformer_raw_all_tables.on_schema_object[0].all[0].in_schema == "\"HEALTHCARE\".\"RAW\""
    )
    error_message = "Future/all table grants exist only for TRANSFORMER on RAW."
  }

  # Spec section 5 cut: view-only layers get no CREATE TABLE.
  assert {
    condition     = !contains(local.schema_grants["TRANSFORMER/STAGING"], "CREATE TABLE") && !contains(local.schema_grants["TRANSFORMER/INTERMEDIATE"], "CREATE TABLE")
    error_message = "TRANSFORMER must not get CREATE TABLE on STAGING or INTERMEDIATE."
  }

  # Spec section 5 cut: policy DDL needs no warehouse.
  assert {
    condition     = !contains(local.warehouse_users, "PLATFORM_ADMIN")
    error_message = "PLATFORM_ADMIN gets no warehouse usage."
  }

  assert {
    condition     = toset(keys(snowflake_grant_privileges_to_account_role.database_usage)) == toset(local.roles)
    error_message = "Every pipeline role gets USAGE on HEALTHCARE."
  }

  assert {
    condition     = snowflake_grant_privileges_to_account_role.schema["LOADER/RAW"].privileges == toset(["USAGE", "CREATE TABLE", "CREATE STAGE"])
    error_message = "LOADER writes RAW: USAGE, CREATE TABLE, CREATE STAGE."
  }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./scripts/tf.sh test -filter=tests/grants.tftest.hcl`
Expected: FAIL with `Reference to undeclared resource` / `undeclared local value`.

- [ ] **Step 3: Write `terraform/grants.tf`**

```hcl
locals {
  # PLATFORM_ADMIN is left out on purpose: creating masking policies is DDL and needs no warehouse.
  warehouse_users = ["LOADER", "TRANSFORMER", "ANALYST", "PHI_READER"]

  # "<ROLE>/<SCHEMA>" => privileges on that schema.
  # ANALYST and PHI_READER get only USAGE on MARTS: SELECT on mart tables is granted by dbt's
  # secure_model post-hook after masking policies are attached. Never add SELECT or future
  # grants on MARTS here (tests/grants.tftest.hcl enforces it).
  schema_grants = {
    "LOADER/RAW"               = ["USAGE", "CREATE TABLE", "CREATE STAGE"]
    "TRANSFORMER/RAW"          = ["USAGE"]
    "TRANSFORMER/STAGING"      = ["USAGE", "CREATE VIEW"]
    "TRANSFORMER/INTERMEDIATE" = ["USAGE", "CREATE VIEW"]
    "TRANSFORMER/MARTS"        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
    "TRANSFORMER/SECURITY"     = ["USAGE"]
    "ANALYST/MARTS"            = ["USAGE"]
    "PHI_READER/MARTS"         = ["USAGE"]
    "PLATFORM_ADMIN/SECURITY"  = ["USAGE", "CREATE MASKING POLICY"]
  }
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  for_each          = toset(local.warehouse_users)
  provider          = snowflake.securityadmin
  account_role_name = snowflake_account_role.this[each.key].name
  privileges        = ["USAGE"]

  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.hdp.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  for_each          = toset(local.roles)
  provider          = snowflake.securityadmin
  account_role_name = snowflake_account_role.this[each.key].name
  privileges        = ["USAGE"]

  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.healthcare.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema" {
  for_each          = local.schema_grants
  provider          = snowflake.securityadmin
  account_role_name = snowflake_account_role.this[split("/", each.key)[0]].name
  privileges        = each.value

  on_schema {
    schema_name = local.schema_fqn[split("/", each.key)[1]]
  }
}

# TRANSFORMER reads every RAW table: those that exist now (all) and those the loader creates later (future).
resource "snowflake_grant_privileges_to_account_role" "transformer_raw_all_tables" {
  provider          = snowflake.securityadmin
  account_role_name = snowflake_account_role.this["TRANSFORMER"].name
  privileges        = ["SELECT"]

  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = local.schema_fqn["RAW"]
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_raw_future_tables" {
  provider          = snowflake.securityadmin
  account_role_name = snowflake_account_role.this["TRANSFORMER"].name
  privileges        = ["SELECT"]

  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = local.schema_fqn["RAW"]
    }
  }
}
```

- [ ] **Step 4: Run all Terraform tests, fmt, validate**

Run: `./scripts/tf.sh test && ./scripts/tf.sh fmt -check -recursive && ./scripts/tf.sh validate`
Expected: `2 passed, 0 failed` (foundation and grants), no fmt output, and "Success!".

- [ ] **Step 5: Prove the masking assertion can fail.** Temporarily change `"ANALYST/MARTS" = ["USAGE"]` to `["USAGE", "SELECT"]` and run `./scripts/tf.sh test -filter=tests/grants.tftest.hcl`.
Expected: FAIL naming "ANALYST and PHI_READER must have only USAGE on MARTS." and "No schema-level SELECT grants." Revert with `git checkout terraform/grants.tf` if you had committed it; otherwise undo the edit by hand. Re-run and expect a pass.

- [ ] **Step 6: Commit**

```bash
git add terraform/grants.tf terraform/tests/grants.tftest.hcl
git commit -m "feat(terraform): privilege matrix with masking invariants tested offline"
```

---

### Task 4: Users

**Files:**
- Create: `terraform/users.tf`, `terraform/tests/users.tftest.hcl`
- Modify: `terraform/outputs.tf`

**Interfaces:**
- Consumes: `snowflake_account_role.this`, `snowflake_warehouse.hdp`, and the variables `service_public_key_path` and `human_user`.
- Produces:
  - `snowflake_service_user.hdp_service`
  - `local.service_roles = ["LOADER", "TRANSFORMER", "PLATFORM_ADMIN"]` and `local.human_roles = ["ANALYST", "PHI_READER"]`
  - `local.human_user_name = upper(var.human_user)`
  - `snowflake_grant_account_role.service[<role>]` and `snowflake_grant_account_role.human[<role>]`

- [ ] **Step 1: Write the failing test** `terraform/tests/users.tftest.hcl`

```hcl
mock_provider "snowflake" {
  alias = "sysadmin"
}

mock_provider "snowflake" {
  alias = "securityadmin"
}

variables {
  organization_name          = "TESTORG"
  account_name               = "TESTACCOUNT"
  human_user                 = "TestUser"
  terraform_private_key_path = "tests/fixtures/dummy_key.p8"
  service_public_key_path    = "tests/fixtures/dummy_key.pub"
}

run "users" {
  command = plan

  # Otherwise a LOADER connection would also carry TRANSFORMER and PLATFORM_ADMIN privileges.
  assert {
    condition     = snowflake_service_user.hdp_service.default_secondary_roles_option == "NONE"
    error_message = "HDP_SERVICE must have secondary roles off."
  }

  assert {
    condition     = snowflake_service_user.hdp_service.rsa_public_key == "AAAAdummyBBBBdummy"
    error_message = "The public key must be the PEM body on one line, without header/footer."
  }

  assert {
    condition     = toset([for g in snowflake_grant_account_role.service : g.role_name]) == toset(["LOADER", "TRANSFORMER", "PLATFORM_ADMIN"]) && alltrue([for g in snowflake_grant_account_role.service : g.user_name == "HDP_SERVICE"])
    error_message = "HDP_SERVICE holds exactly LOADER, TRANSFORMER and PLATFORM_ADMIN."
  }

  assert {
    condition     = toset([for g in snowflake_grant_account_role.human : g.role_name]) == toset(["ANALYST", "PHI_READER"]) && alltrue([for g in snowflake_grant_account_role.human : g.user_name == "TESTUSER"])
    error_message = "The human user gets ANALYST and PHI_READER, by its upper-case name."
  }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./scripts/tf.sh test -filter=tests/users.tftest.hcl`
Expected: FAIL with `Reference to undeclared resource ... snowflake_service_user.hdp_service`.

- [ ] **Step 3: Write `terraform/users.tf`**

```hcl
locals {
  service_roles = ["LOADER", "TRANSFORMER", "PLATFORM_ADMIN"]
  human_roles   = ["ANALYST", "PHI_READER"]

  # Snowflake stores unquoted user names in upper case.
  human_user_name = upper(var.human_user)

  # rsa_public_key must be the PEM body on one line, without the BEGIN/END lines.
  service_public_key = join("", [
    for line in split("\n", trimspace(file(var.service_public_key_path))) :
    trimspace(line) if !startswith(line, "-----")
  ])
}

# The pipeline's key-pair user (Airflow + dbt). Its private key stays in secrets/.
resource "snowflake_service_user" "hdp_service" {
  provider                       = snowflake.securityadmin
  name                           = "HDP_SERVICE"
  default_warehouse              = snowflake_warehouse.hdp.name
  default_role                   = "LOADER"
  default_secondary_roles_option = "NONE"
  rsa_public_key                 = local.service_public_key
}

resource "snowflake_grant_account_role" "service" {
  for_each  = toset(local.service_roles)
  provider  = snowflake.securityadmin
  role_name = snowflake_account_role.this[each.key].name
  user_name = snowflake_service_user.hdp_service.name
}

# Only the grants: the human user itself is never managed here, so Terraform can't lock you out.
resource "snowflake_grant_account_role" "human" {
  for_each  = toset(local.human_roles)
  provider  = snowflake.securityadmin
  role_name = snowflake_account_role.this[each.key].name
  user_name = local.human_user_name
}
```

On Windows, the `.pub` file can contain `\r`. `trimspace(line)` strips it, so the key stays on one clean line.

- [ ] **Step 4: Add the user output** by appending to `terraform/outputs.tf`:

```hcl
output "service_user" {
  description = "Key-pair user the pipeline logs in as."
  value       = snowflake_service_user.hdp_service.name
}
```

- [ ] **Step 5: Run all Terraform tests, fmt, validate**

Run: `./scripts/tf.sh test && ./scripts/tf.sh fmt -check -recursive && ./scripts/tf.sh validate`
Expected: `3 passed, 0 failed`, no fmt output, and "Success!".

- [ ] **Step 6: Commit**

```bash
git add terraform/users.tf terraform/outputs.tf terraform/tests/users.tftest.hcl
git commit -m "feat(terraform): HDP_SERVICE user and role grants"
```

---

### Task 5: Adoption imports and the bootstrap snippet

**Files:**
- Create: `terraform/imports.tf`, `terraform/bootstrap_terraform_user.sql`, `terraform/tests/imports.tftest.hcl`

**Interfaces:**
- Consumes: every resource and local from Tasks 2–4.
- Produces:
  - `local.adoption_ids`: an object with one map per resource kind, always computed. Its keys equal the resource's `for_each` keys; single resources use key `"this"`.
  - `local.adopt`: the same object when `var.adopt_existing_account` is true, otherwise every map is empty.
  - One gated `import` block per resource kind.

Import ID formats (from provider v2.21 docs; quote every identifier part):
- warehouse / database / role / service user: `"\"NAME\""`
- schema: `"\"HEALTHCARE\".\"RAW\""`
- role grant: `"\"ROLE\"|ROLE|\"PARENT\""` or `"\"ROLE\"|USER|\"USER_NAME\""`
- privileges on account object: `"\"ROLE\"|false|false|USAGE|OnAccountObject|WAREHOUSE|\"HDP_WH\""`
- privileges on schema: `"\"ROLE\"|false|false|P1,P2|OnSchema|OnSchema|\"HEALTHCARE\".\"S\""`
- all tables in schema: `"\"ROLE\"|false|false|SELECT|OnSchemaObject|OnAll|TABLES|InSchema|\"HEALTHCARE\".\"RAW\""`
- future tables in schema: `"\"ROLE\"|false|false|SELECT|OnSchemaObject|OnFuture|TABLES|InSchema|\"HEALTHCARE\".\"RAW\""`

- [ ] **Step 1: Write the failing test** `terraform/tests/imports.tftest.hcl`

```hcl
mock_provider "snowflake" {
  alias = "sysadmin"
}

mock_provider "snowflake" {
  alias = "securityadmin"
}

variables {
  organization_name          = "TESTORG"
  account_name               = "TESTACCOUNT"
  human_user                 = "TestUser"
  terraform_private_key_path = "tests/fixtures/dummy_key.p8"
  service_public_key_path    = "tests/fixtures/dummy_key.pub"
}

run "fresh_account_imports_nothing" {
  command = plan

  assert {
    condition     = alltrue([for kind, ids in local.adopt : length(ids) == 0])
    error_message = "With adopt_existing_account = false no import may run (they would fail on a fresh account)."
  }
}

run "adoption_ids_cover_every_managed_instance" {
  command = plan

  assert {
    condition = (
      toset(keys(local.adoption_ids.schemas)) == toset(keys(snowflake_schema.this)) &&
      toset(keys(local.adoption_ids.roles)) == toset(keys(snowflake_account_role.this)) &&
      toset(keys(local.adoption_ids.roles_to_sysadmin)) == toset(keys(snowflake_grant_account_role.to_sysadmin)) &&
      toset(keys(local.adoption_ids.warehouse_usage)) == toset(keys(snowflake_grant_privileges_to_account_role.warehouse_usage)) &&
      toset(keys(local.adoption_ids.database_usage)) == toset(keys(snowflake_grant_privileges_to_account_role.database_usage)) &&
      toset(keys(local.adoption_ids.schema_grants)) == toset(keys(snowflake_grant_privileges_to_account_role.schema)) &&
      toset(keys(local.adoption_ids.service_roles)) == toset(keys(snowflake_grant_account_role.service)) &&
      toset(keys(local.adoption_ids.human_roles)) == toset(keys(snowflake_grant_account_role.human))
    )
    error_message = "Every for_each resource needs an import ID for each instance."
  }

  # The TRANSFORMER cut must appear in the adoption plan: its import IDs list the live
  # privileges (including CREATE TABLE), so plan shows CREATE TABLE being revoked.
  assert {
    condition     = strcontains(local.adoption_ids.schema_grants["TRANSFORMER/STAGING"], "|USAGE,CREATE TABLE,CREATE VIEW|")
    error_message = "Adoption must import what bootstrap.sql granted so plan shows the CREATE TABLE cut."
  }

  assert {
    condition     = local.adoption_ids.human_roles["ANALYST"] == "\"ANALYST\"|USER|\"TESTUSER\""
    error_message = "Human-user grant IDs use the upper-case user name."
  }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./scripts/tf.sh test -filter=tests/imports.tftest.hcl`
Expected: FAIL with `undeclared local value ... adopt` / `adoption_ids`.

- [ ] **Step 3: Write `terraform/imports.tf`**

```hcl
# Adopting an account first set up with the old snowflake/bootstrap.sql.
# The IDs are always computed (tests check they cover every managed instance); the import blocks
# only use them when var.adopt_existing_account is true, because imports fail on a fresh account.
locals {
  q = "\""

  # What bootstrap.sql granted on each schema. It equals local.schema_grants except for the two
  # view layers, where it also granted CREATE TABLE. Importing the live privileges makes the first
  # plan show that cut as an in-place update (CREATE TABLE revoked).
  bootstrap_schema_privileges = merge(local.schema_grants, {
    "TRANSFORMER/STAGING"      = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
    "TRANSFORMER/INTERMEDIATE" = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  })

  adoption_ids = {
    warehouse = { this = "${local.q}HDP_WH${local.q}" }
    database  = { this = "${local.q}HEALTHCARE${local.q}" }
    schemas   = { for s in local.schemas : s => "${local.q}HEALTHCARE${local.q}.${local.q}${s}${local.q}" }
    roles     = { for r in local.roles : r => "${local.q}${r}${local.q}" }
    roles_to_sysadmin = {
      for r in local.roles : r => "${local.q}${r}${local.q}|ROLE|${local.q}SYSADMIN${local.q}"
    }
    warehouse_usage = {
      for r in local.warehouse_users :
      r => "${local.q}${r}${local.q}|false|false|USAGE|OnAccountObject|WAREHOUSE|${local.q}HDP_WH${local.q}"
    }
    database_usage = {
      for r in local.roles :
      r => "${local.q}${r}${local.q}|false|false|USAGE|OnAccountObject|DATABASE|${local.q}HEALTHCARE${local.q}"
    }
    schema_grants = {
      for k, privs in local.bootstrap_schema_privileges :
      k => "${local.q}${split("/", k)[0]}${local.q}|false|false|${join(",", privs)}|OnSchema|OnSchema|${local.q}HEALTHCARE${local.q}.${local.q}${split("/", k)[1]}${local.q}"
    }
    raw_all_tables    = { this = "${local.q}TRANSFORMER${local.q}|false|false|SELECT|OnSchemaObject|OnAll|TABLES|InSchema|${local.q}HEALTHCARE${local.q}.${local.q}RAW${local.q}" }
    raw_future_tables = { this = "${local.q}TRANSFORMER${local.q}|false|false|SELECT|OnSchemaObject|OnFuture|TABLES|InSchema|${local.q}HEALTHCARE${local.q}.${local.q}RAW${local.q}" }
    service_user      = { this = "${local.q}HDP_SERVICE${local.q}" }
    service_roles = {
      for r in local.service_roles : r => "${local.q}${r}${local.q}|USER|${local.q}HDP_SERVICE${local.q}"
    }
    human_roles = {
      for r in local.human_roles : r => "${local.q}${r}${local.q}|USER|${local.q}${local.human_user_name}${local.q}"
    }
  }

  # A filtered comprehension (not a ?: on maps) keeps every kind's type identical either way.
  adopt = {
    for kind, ids in local.adoption_ids :
    kind => { for k, id in ids : k => id if var.adopt_existing_account }
  }
}

import {
  for_each = local.adopt.warehouse
  to       = snowflake_warehouse.hdp
  id       = each.value
}

import {
  for_each = local.adopt.database
  to       = snowflake_database.healthcare
  id       = each.value
}

import {
  for_each = local.adopt.schemas
  to       = snowflake_schema.this[each.key]
  id       = each.value
}

import {
  for_each = local.adopt.roles
  to       = snowflake_account_role.this[each.key]
  id       = each.value
}

import {
  for_each = local.adopt.roles_to_sysadmin
  to       = snowflake_grant_account_role.to_sysadmin[each.key]
  id       = each.value
}

import {
  for_each = local.adopt.warehouse_usage
  to       = snowflake_grant_privileges_to_account_role.warehouse_usage[each.key]
  id       = each.value
}

import {
  for_each = local.adopt.database_usage
  to       = snowflake_grant_privileges_to_account_role.database_usage[each.key]
  id       = each.value
}

import {
  for_each = local.adopt.schema_grants
  to       = snowflake_grant_privileges_to_account_role.schema[each.key]
  id       = each.value
}

import {
  for_each = local.adopt.raw_all_tables
  to       = snowflake_grant_privileges_to_account_role.transformer_raw_all_tables
  id       = each.value
}

import {
  for_each = local.adopt.raw_future_tables
  to       = snowflake_grant_privileges_to_account_role.transformer_raw_future_tables
  id       = each.value
}

import {
  for_each = local.adopt.service_user
  to       = snowflake_service_user.hdp_service
  id       = each.value
}

import {
  for_each = local.adopt.service_roles
  to       = snowflake_grant_account_role.service[each.key]
  id       = each.value
}

import {
  for_each = local.adopt.human_roles
  to       = snowflake_grant_account_role.human[each.key]
  id       = each.value
}
```

- [ ] **Step 4: Write `terraform/bootstrap_terraform_user.sql`**

```sql
-- Run once in Snowsight as ACCOUNTADMIN (README "Snowflake mode"). Terraform does the rest.
-- Before running, replace <TERRAFORM_PUBLIC_KEY> with the output of:
--   ./scripts/snowflake_keygen.sh terraform
use role accountadmin;

-- ── Part 1: always ──────────────────────────────────────────────────────────────────────
-- The user Terraform logs in as. SYSADMIN owns infrastructure objects and SECURITYADMIN owns
-- roles, users and grants; Terraform never holds ACCOUNTADMIN.
create user if not exists TERRAFORM
    type = service
    default_role = SYSADMIN
    default_secondary_roles = ()
    rsa_public_key = '<TERRAFORM_PUBLIC_KEY>';
grant role SYSADMIN to user TERRAFORM;
grant role SECURITYADMIN to user TERRAFORM;

-- ── Part 2: only for an account first set up with the old snowflake/bootstrap.sql ────────
-- That script created everything as ACCOUNTADMIN, so ACCOUNTADMIN owns it, and SYSADMIN or
-- SECURITYADMIN can't alter it. Move ownership to the roles Terraform uses, keeping existing grants.
-- Skip this whole part on a fresh account.
grant ownership on warehouse HDP_WH to role SYSADMIN copy current grants;
grant ownership on database HEALTHCARE to role SYSADMIN copy current grants;
grant ownership on schema HEALTHCARE.RAW to role SYSADMIN copy current grants;
grant ownership on schema HEALTHCARE.STAGING to role SYSADMIN copy current grants;
grant ownership on schema HEALTHCARE.INTERMEDIATE to role SYSADMIN copy current grants;
grant ownership on schema HEALTHCARE.MARTS to role SYSADMIN copy current grants;
grant ownership on schema HEALTHCARE.SECURITY to role SYSADMIN copy current grants;
grant ownership on role LOADER to role SECURITYADMIN;
grant ownership on role TRANSFORMER to role SECURITYADMIN;
grant ownership on role ANALYST to role SECURITYADMIN;
grant ownership on role PHI_READER to role SECURITYADMIN;
grant ownership on role PLATFORM_ADMIN to role SECURITYADMIN;
grant ownership on user HDP_SERVICE to role SECURITYADMIN;
-- The old script gave PLATFORM_ADMIN warehouse usage it doesn't need. Terraform can't revoke a
-- grant it doesn't describe, so the cut is made here; the Terraform config never grants it.
revoke usage on warehouse HDP_WH from role PLATFORM_ADMIN;
```

- [ ] **Step 5: Run all Terraform tests, fmt, validate, tflint**

```bash
./scripts/tf.sh test
./scripts/tf.sh fmt -check -recursive
./scripts/tf.sh validate
MSYS_NO_PATHCONV=1 docker run --rm -v "$PWD:/data" -w /data/terraform ghcr.io/terraform-linters/tflint:v0.64.0
```

Expected: `5 passed, 0 failed` (five runs: foundation, privilege_matrix, users, and two in imports), no fmt output, "Success!", and tflint with no findings. Every variable is used now.

- [ ] **Step 6: Commit**

```bash
git add terraform/imports.tf terraform/bootstrap_terraform_user.sql terraform/tests/imports.tftest.hcl
git commit -m "feat(terraform): gated adoption imports and the one-time bootstrap snippet"
```

---

### Task 6: Cross-file tests, CI job, retire bootstrap.sql, README

**Files:**
- Create: `tests/test_terraform.py`
- Modify: `pyproject.toml` / `uv.lock` (via `uv add`), `tests/test_dbt_project.py` (remove 2 tests), `.github/workflows/ci.yml`, `README.md`, `ingest/config.py:12` (comment), `docs/superpowers/specs/2026-10-06-healthcare-data-platform-design.md` is NOT touched
- Delete: `snowflake/bootstrap.sql`

**Interfaces:**
- Consumes: `terraform/roles.tf` (`locals { roles = [...] }`), `terraform/providers.tf` (two `provider "snowflake"` blocks with `role`), `snowflake/policies.sql`, `dbt/macros/secure_model.sql` (line `{%- for role in ['ANALYST', 'PHI_READER'] -%}`).
- Produces: a CI job `terraform` that runs `scripts/tf.sh` and tflint.

- [ ] **Step 1: Add the dependency**

Run: `uv add --dev python-hcl2`
Expected: `pyproject.toml` dev group gains `python-hcl2` (8.x), and `uv.lock` is updated.

- [ ] **Step 2: Write the failing test** `tests/test_terraform.py`

```python
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
```

- [ ] **Step 3: Run it, and prove the drift check bites**

Run: `uv run pytest tests/test_terraform.py -q`
Expected: `2 passed`. These tests are written against Task 2's existing files, so they pass immediately. To see them fail, temporarily rename `"PHI_READER"` in `terraform/roles.tf` to `"PHI_READERS"` and rerun. Expect `test_declared_roles_cover_every_role_masking_relies_on` to FAIL. Revert the rename and expect `2 passed` again.

- [ ] **Step 4: Retire bootstrap.sql and its tests**

```bash
git rm snowflake/bootstrap.sql
```

In `tests/test_dbt_project.py`, delete the two functions `test_bootstrap_never_grants_marts_tables_ahead_of_masking` and `test_service_user_cannot_borrow_roles_through_secondary_roles` entirely. Their rules now live in `terraform/tests/grants.tftest.hcl` and `terraform/tests/users.tftest.hcl`. In `ingest/config.py`, change the comment on line 12 from `# Fixed: profiles.yml, bootstrap.sql and the masking policies all name HEALTHCARE.` to `# Fixed: profiles.yml, terraform/database.tf and the masking policies all name HEALTHCARE.`

Run: `grep -rn "bootstrap.sql" --include=*.py --include=*.sh --include=*.yml --include=*.md . | grep -v "^./docs/superpowers/" | grep -v "^./.superpowers/"`
Expected: no output except `README.md` lines, which Step 6 rewrites. Re-run the grep after Step 6; it must print nothing. `terraform/bootstrap_terraform_user.sql` mentions "the old snowflake/bootstrap.sql" in comments; that's intended. The grep ignores `.sql` files.

- [ ] **Step 5: Add the CI job.** Append to `.github/workflows/ci.yml`, under `jobs:`, at the same indentation as `dag-integrity:`:

```yaml
  terraform:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - name: fmt, init, validate
        run: |
          ./scripts/tf.sh fmt -check -recursive
          ./scripts/tf.sh init -backend=false -input=false
          ./scripts/tf.sh validate
      - name: Offline tests (mocked provider)
        run: ./scripts/tf.sh test
      - name: tflint
        run: docker run --rm -v "$PWD:/data" -w /data/terraform ghcr.io/terraform-linters/tflint:v0.64.0
```

Run: `uv run python -c "import yaml; d=yaml.safe_load(open('.github/workflows/ci.yml')); print(sorted(d['jobs']))"`
Expected: `['dag-integrity', 'python-and-dbt', 'terraform']`

- [ ] **Step 6: Rewrite the README's Snowflake setup.** Replace the four numbered steps under `## Snowflake mode: masking and least-privilege roles` (steps 1–4 from "1. Start a Snowflake trial" through "then run `snowflake/demo_queries.sql`.") with:

```markdown
Account setup is Terraform ([`terraform/`](terraform/)), run through Docker so there is nothing to install:

1. Start a Snowflake trial (Enterprise edition: masking policies need it).
2. Create two key pairs: `./scripts/snowflake_keygen.sh` (the pipeline's user) and `./scripts/snowflake_keygen.sh terraform` (the user Terraform logs in as).
3. In Snowsight, as ACCOUNTADMIN, run **part 1** of [`terraform/bootstrap_terraform_user.sql`](terraform/bootstrap_terraform_user.sql), pasting in the `terraform` public key. This is the only SQL run by hand.
4. Copy `terraform/terraform.tfvars.example` to `terraform/terraform.tfvars` and fill in your account identifier and user name. Then run:
   ```bash
   ./scripts/tf.sh init
   ./scripts/tf.sh plan
   ./scripts/tf.sh apply
   ```
5. Copy `.env.example` to `.env` (on Linux, keep your `AIRFLOW_UID` line: uncomment it there), set `WAREHOUSE=snowflake`, `SNOWFLAKE_ACCOUNT` and `SNOWFLAKE_PRIVATE_KEY_PATH=/opt/project/secrets/hdp_service_key.p8`, then run `docker compose up -d`.
6. Trigger `patient_pipeline`, then run `snowflake/demo_queries.sql`.

**Adopting an account set up before Terraform** (with the old `bootstrap.sql`): also run **part 2** of the snippet. It moves ownership to SYSADMIN and SECURITYADMIN, so Terraform can manage the objects. Then set `adopt_existing_account = true` in `terraform.tfvars`. The first `plan` imports everything and shows only one change: CREATE TABLE revoked from TRANSFORMER on the view-only schemas. Leave the flag `false` on a fresh account: imports of objects that don't exist fail.
```

Replace the design-decision bullet that starts `- **The pipeline never holds ACCOUNTADMIN.**` with:

```markdown
- **Neither the pipeline nor Terraform holds ACCOUNTADMIN.** Terraform logs in as a key-pair service user with SYSADMIN (warehouse, database, schemas) and SECURITYADMIN (roles, users, grants). ACCOUNTADMIN is used once, by a human, to create that user. The pipeline's service user is key-pair only and holds LOADER, TRANSFORMER and PLATFORM_ADMIN.
- **Access as code, drift made visible.** Roles and grants are maps in `terraform/grants.tf`, and offline `terraform test` runs in CI pin the masking invariants: analysts never get SELECT or future grants on MARTS, and the service user has no secondary roles. Moving the hand-run setup into Terraform surfaced two over-grants (CREATE TABLE on view-only schemas, a warehouse grant for a role that only runs DDL), and both were cut. State is a local file; a team would use a remote backend.
```

In `## Next milestones`, delete the line `- Terraform for Snowflake roles, warehouses and grants`.

- [ ] **Step 7: Run everything**

```bash
uv run pytest -q
uv run ruff check . && uv run ruff format --check .
./scripts/tf.sh fmt -check -recursive && ./scripts/tf.sh validate && ./scripts/tf.sh test
```

Expected: pytest shows all passing (Postgres tests skipped without `TEST_POSTGRES_URL`; that's fine here), ruff is clean, Terraform says `Success!`, and the test summary reads `5 passed, 0 failed`.

- [ ] **Step 8: Commit**

```bash
git add -A tests/ pyproject.toml uv.lock .github/workflows/ci.yml README.md ingest/config.py snowflake/
git commit -m "feat(terraform): CI job, cross-file role checks; README uses Terraform; retire bootstrap.sql"
```

---

### Task 7: Adopt the live trial account (human-assisted)

This task changes the live Snowflake account. It needs the user for one Snowsight step and runs against trial account `MVIMJXA-PA76665` (user `JUSTINKO`). The controller runs it with the user. Don't dispatch it to an unattended subagent.

**Files:**
- Create (gitignored, never committed): `secrets/terraform_key.p8`, `secrets/terraform_key.pub`, `terraform/terraform.tfvars`, `terraform/terraform.tfstate`
- Possibly modify: `terraform/*.tf` and the spec, only per the fallback rules in Steps 4 and 7

- [ ] **Step 1: Generate the Terraform key**

Run: `./scripts/snowflake_keygen.sh terraform`
Expected: `secrets/terraform_key.p8` and `.pub` are created, and the public key is printed.

- [ ] **Step 2: The user runs the bootstrap snippet.** Write a filled copy to `secrets/bootstrap_terraform_filled.sql`, with `<TERRAFORM_PUBLIC_KEY>` replaced. The user runs **both parts** in Snowsight as ACCOUNTADMIN, all statements ("Run All"), and confirms all succeeded. Give them the same click-by-click Snowsight steps used for the original bootstrap.

- [ ] **Step 3: Write `terraform/terraform.tfvars`**

```hcl
organization_name      = "MVIMJXA"
account_name           = "PA76665"
human_user             = "JUSTINKO"
adopt_existing_account = true
```

Before writing it, confirm the stored user name with a query as the user. If it's not `JUSTINKO`, use the exact value.

- [ ] **Step 4: Init and plan**

Run: `./scripts/tf.sh init -input=false && ./scripts/tf.sh plan -input=false -out=adopt.tfplan`
Expected: `Plan: 43 to import, 0 to add, 2 to change, 0 to destroy.` (or `3 to change`, when the third is `rsa_public_key` on `snowflake_service_user.hdp_service`: the provider doesn't read the key on import. Before applying, confirm `DESC USER HDP_SERVICE` RSA_PUBLIC_KEY_FP equals the fingerprint of `secrets/hdp_service_key.pub`.) The 2 changes are `schema["TRANSFORMER/STAGING"]` and `schema["TRANSFORMER/INTERMEDIATE"]`, with privileges going from `CREATE TABLE, CREATE VIEW, USAGE` to `CREATE VIEW, USAGE`.

Fallback rules:
- If other in-place changes appear on imported objects (fields `bootstrap.sql` never set, shown changing from a live value to the config's or to null), add those attribute names to a `lifecycle { ignore_changes = [...] }` block on that resource. Record each one in your report, then re-plan.
- If `transformer_raw_all_tables` shows a change on every plan, delete that resource and its import entry: existing RAW tables are already granted, and the future grant covers new ones. Record that as a spec deviation.
- Never apply a plan that shows a destroy.

- [ ] **Step 5: Apply, then prove idempotence**

Run: `./scripts/tf.sh apply -input=false adopt.tfplan && ./scripts/tf.sh plan -input=false -detailed-exitcode; echo "exit=$?"`
Expected: apply reports `43 imported, 0 added, 2 changed, 0 destroyed`, and the second plan prints `No changes.` with `exit=0`.

- [ ] **Step 6: Confirm the cuts live** (as TRANSFORMER via the service key, using the pattern from Task 13 of the original plan):

```bash
docker compose exec -T airflow bash -c 'cd /opt/project && /opt/venv/bin/python -c "
from ingest.config import load_settings
from ingest.loaders import snowflake_connection
c = snowflake_connection(load_settings().require_snowflake(), \"TRANSFORMER\"); cur = c.cursor()
for s in (\"STAGING\", \"INTERMEDIATE\", \"MARTS\"):
    cur.execute(f\"show grants on schema HEALTHCARE.{s}\")
    print(s, sorted(r[1] for r in cur.fetchall() if r[5] == \"TRANSFORMER\"))
"' 2>&1 | grep -v Warning
```

Expected: `STAGING ['CREATE VIEW', 'USAGE']`, `INTERMEDIATE ['CREATE VIEW', 'USAGE']`, `MARTS ['CREATE TABLE', 'CREATE VIEW', 'USAGE']`.
If `SHOW GRANTS ON SCHEMA` returns nothing for TRANSFORMER (it lacks ownership), ask the user to run `show grants on schema HEALTHCARE.STAGING;` in Snowsight as ACCOUNTADMIN instead.

- [ ] **Step 7: Full pipeline run on Snowflake**

Run: `MSYS_NO_PATHCONV=1 docker compose exec -T airflow airflow dags trigger patient_pipeline --conf '{"use_sample": true}'`, then wait for the run as in earlier live checks.
Expected: all four pipeline tasks succeed, including `apply_security` as PLATFORM_ADMIN without warehouse usage.

Fallback: if `apply_security` fails on a missing warehouse, add `"PLATFORM_ADMIN"` back to `local.warehouse_users` and remove its exclusion test assertion in `terraform/tests/grants.tftest.hcl`. Re-grant it with `./scripts/tf.sh apply`, update spec Section 5 and the README bullet to say PLATFORM_ADMIN needs warehouse usage to connect, then re-run the pipeline.

- [ ] **Step 8: Masking re-check.** Ask the user to run query 1 of `snowflake/demo_queries.sql` (ANALYST) in Snowsight. Expect the same `***MASKED***` names, SSNs and hashed IDs as `docs/snowflake_demo_output.md`.

- [ ] **Step 9: Commit any fallback edits.** If Steps 4 or 7 changed `terraform/*.tf`, the spec or the README, commit them with a message naming the fallback. Then run `git status --short`. No secrets, tfvars or state may appear; they're gitignored.

---

## Self-review notes

- **Spec coverage:**
  - §1 success criteria: Tasks 5 and 7 (fresh-vs-adopt, idempotent second plan, pipeline run, masking check) and Task 6 (CI offline).
  - §2 decisions: Tasks 1, 2 and 5.
  - §3 layout: Tasks 1–5.
  - §4 first-time setup: Task 5 snippet and Task 6 README.
  - §5 matrix and invariants: Task 3 (tests) and Task 4 (secondary roles).
  - §6 error handling: Task 1 key guard and Task 6 README flag guidance.
  - §7 testing: Tasks 2–6 and Task 7 live.
  - §8 docs: Task 6.
- **Deviations** (listed at the top): PLATFORM_ADMIN cut via the snippet; ACCOUNTADMIN check in pytest; import gating tested through locals.
- **Names used across tasks:** `local.roles`, `local.schemas`, `local.schema_fqn`, `local.warehouse_users`, `local.schema_grants`, `local.service_roles`, `local.human_roles`, `local.human_user_name`, `local.adoption_ids`, `local.adopt`, and resource addresses as listed in each task's Interfaces block.
