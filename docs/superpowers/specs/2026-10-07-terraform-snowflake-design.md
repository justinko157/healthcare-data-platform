# Terraform for Snowflake: Design

**Date:** 2026-10-07
**Status:** Approved design, pending implementation plan
**Builds on:** `2026-10-06-healthcare-data-platform-design.md` (the "next milestone" listed there)

## 1. Purpose

Replace the hand-run `snowflake/bootstrap.sql` with Terraform. The Snowflake account setup
(warehouse, database, schemas, roles, grants, service user) becomes reviewable code, and
`terraform plan` shows any drift between the code and the live account.

### Success criteria

- On a fresh account: one short SQL snippet run by hand, then `terraform apply` creates
  everything `bootstrap.sql` created, minus two deliberate privilege cuts (Section 5).
- On the existing trial account: `terraform plan` shows only imports and the two cuts. After
  `apply`, a second `plan` shows **no changes**.
- After apply, a full `WAREHOUSE=snowflake` pipeline run succeeds, and the masking demo still
  shows masked values for ANALYST.
- CI checks the Terraform without any Snowflake credentials.

### Constraints

- Neither Terraform nor the pipeline ever uses ACCOUNTADMIN. ACCOUNTADMIN is used only by a
  human, once, for the bootstrap snippet.
- No new local installs. Terraform runs in Docker at a pinned version.
- CI still needs no secrets.

### Out of scope

Masking policies (they stay in `snowflake/policies.sql`, applied by the pipeline each run),
remote state, managing the human user itself, and multiple environments.

## 2. Decisions

| Decision | Choice | Why |
|---|---|---|
| What Terraform owns | Account setup only | Infra changes rarely and needs an admin role; masking policies are application logic owned by PLATFORM_ADMIN and applied every run |
| Terraform auth | Dedicated key-pair service user `TERRAFORM` with SYSADMIN + SECURITYADMIN | Works in Docker and CI; no MFA prompts; avoids ACCOUNTADMIN |
| State | Local `terraform.tfstate`, gitignored | Single owner; no extra account. README notes a team would use a remote backend |
| Structure | One flat config split by concern, roles and grants as `for_each` maps | Readable access model; modules would add indirection with no reuse at this size |
| Existing objects | `import` blocks gated by `var.adopt_existing_account` | Imports fail on a fresh account, so they run only when adopting |
| Provider | `snowflakedb/snowflake`, pinned in `.terraform.lock.hcl` | Official provider |

## 3. Layout

```
terraform/
  versions.tf      # required_version, required_providers (snowflakedb/snowflake, pinned)
  providers.tf     # two aliases, sysadmin and securityadmin; SNOWFLAKE_JWT key-pair auth as TERRAFORM
  variables.tf     # organization_name, account_name, terraform_user, terraform_private_key_path,
                   # service_public_key_path, human_user, adopt_existing_account (default false)
  warehouse.tf     # HDP_WH: XSMALL, auto_suspend 60, auto_resume, initially_suspended
  database.tf      # HEALTHCARE + schemas RAW, STAGING, INTERMEDIATE, MARTS, SECURITY
  roles.tf         # LOADER, TRANSFORMER, ANALYST, PHI_READER, PLATFORM_ADMIN; each granted to SYSADMIN
  grants.tf        # the privilege matrix (Section 5) as maps through for_each
  users.tf         # HDP_SERVICE + its 3 role grants; ANALYST and PHI_READER granted to var.human_user
  imports.tf       # import blocks, for_each over a map that is empty unless adopt_existing_account
  outputs.tf       # role names and the service user name
  bootstrap_terraform_user.sql   # the one hand-run snippet (Section 4)
  terraform.tfvars.example       # copied to the gitignored terraform.tfvars
  tests/security.tftest.hcl      # offline tests with mock_provider (Section 7)
scripts/tf.sh      # runs the pinned Terraform image with terraform/ and secrets/ mounted
```

Ownership: **SYSADMIN** owns the warehouse, database and schemas. **SECURITYADMIN** owns the five
roles, `HDP_SERVICE` and all grants. Each resource names its provider alias explicitly.

## 4. First-time setup

`terraform/bootstrap_terraform_user.sql` is run once in Snowsight as ACCOUNTADMIN. It has two
parts:

1. **Always:** create user `TERRAFORM` (type service, key-pair, `default_secondary_roles = ()`)
   and grant it SYSADMIN and SECURITYADMIN. The key comes from
   `scripts/snowflake_keygen.sh terraform`. The keygen script gains an optional key-name
   argument and defaults to `hdp_service`, so today's behavior is unchanged.
2. **Only for accounts set up with the old `bootstrap.sql`:** move ownership, keeping current
   grants. The warehouse, database and five schemas go to SYSADMIN. The five roles and
   `HDP_SERVICE` go to SECURITYADMIN. This step exists because objects owned by ACCOUNTADMIN
   can't be changed by SYSADMIN or SECURITYADMIN.

Then:
- **Fresh account:** `./scripts/tf.sh apply`.
- **Existing account:** set `adopt_existing_account = true` in `terraform.tfvars`, run
  `./scripts/tf.sh plan` (expect imports plus the Section 5 cuts), then `./scripts/tf.sh apply`.

`HDP_SERVICE` keeps its name, roles and public key (read from `secrets/hdp_service_key.pub`),
so the running pipeline is unaffected.

## 5. Privilege matrix

| Role | Privileges | Change from bootstrap.sql |
|---|---|---|
| LOADER | USAGE on HDP_WH and HEALTHCARE; RAW: USAGE, CREATE TABLE, CREATE STAGE | none |
| TRANSFORMER | USAGE on HDP_WH and HEALTHCARE; RAW: USAGE, SELECT on all and future tables; STAGING and INTERMEDIATE: USAGE, CREATE VIEW; MARTS: USAGE, CREATE TABLE, CREATE VIEW; SECURITY: USAGE | drops CREATE TABLE on STAGING and INTERMEDIATE (both are view layers) |
| ANALYST | USAGE on HDP_WH and HEALTHCARE; MARTS: USAGE | none |
| PHI_READER | USAGE on HDP_WH and HEALTHCARE; MARTS: USAGE | none |
| PLATFORM_ADMIN | USAGE on HEALTHCARE; SECURITY: USAGE, CREATE MASKING POLICY | drops USAGE on HDP_WH |
| all five | granted to role SYSADMIN | none |
| HDP_SERVICE (user) | LOADER, TRANSFORMER, PLATFORM_ADMIN | none |
| `var.human_user` | ANALYST, PHI_READER | none (the user itself is not managed) |

Invariants kept from the original design:
- No SELECT grant and no future grant on MARTS for ANALYST or PHI_READER. Table SELECT comes
  only from dbt's `secure_model` post-hook, after masking is attached.
- `HDP_SERVICE` has secondary roles off.

**Risk:** PLATFORM_ADMIN connects with HDP_WH as its warehouse. If removing its USAGE breaks the
`apply_security` step, the live check (Section 7) will fail. In that case, keep the grant and
record why in this spec.

## 6. Error handling

- A missing or wrong key path fails at `plan`, with the provider's auth error. `tf.sh` checks
  first that `secrets/terraform_key.p8` exists and prints the keygen command if not.
- Running against an existing account without `adopt_existing_account = true` makes `apply` fail
  with "already exists" errors. The README says to set the flag.
- Running with the flag on a fresh account fails on import. The README says to leave it off.
- Ownership not transferred makes plan or apply fail with "insufficient privileges". The README
  points to part 2 of the snippet.

## 7. Testing and CI

**Offline, in CI** (new job `terraform`, no secrets), using the pinned Terraform image:
- `terraform fmt -check -recursive`
- `terraform init -backend=false` and `terraform validate`
- `terraform test`: `tests/security.tftest.hcl` uses `mock_provider "snowflake"` and asserts on a
  planned run:
  1. no grant gives ANALYST or PHI_READER SELECT on MARTS, and no MARTS grant is a future grant;
  2. `HDP_SERVICE` has secondary roles off;
  3. neither provider alias uses role ACCOUNTADMIN;
  4. TRANSFORMER has no CREATE TABLE on STAGING or INTERMEDIATE;
  5. with `adopt_existing_account = false`, no imports are planned.
- tflint with the default Terraform ruleset.

**pytest (cross-file drift):** one test parses `terraform/roles.tf` with `python-hcl2` (a new
dev dependency) and checks that the declared role set contains every role named in
`snowflake/policies.sql` and `dbt/macros/secure_model.sql`. This replaces
`test_bootstrap_never_grants_marts_tables_ahead_of_masking` and
`test_service_user_cannot_borrow_roles_through_secondary_roles`, whose rules move into the
`terraform test` assertions above.

**Live, once, on the trial account:**
1. The user runs the bootstrap snippet (both parts).
2. `plan` shows only imports and the two Section 5 cuts.
3. `apply`, then a second `plan` shows no changes.
4. A full `WAREHOUSE=snowflake` pipeline run succeeds.
5. The ANALYST demo query still returns masked values.

## 8. Documentation

- The README "Snowflake mode" section becomes: generate both keys, run the bootstrap snippet,
  copy `terraform.tfvars.example`, `./scripts/tf.sh apply`, set `.env`, then run the pipeline.
- New design-decisions bullet: SYSADMIN/SECURITYADMIN instead of ACCOUNTADMIN, imports to adopt
  an existing account, and the two privilege cuts that plan made visible.
- *Next milestones* drops Terraform.
- `snowflake/bootstrap.sql` is deleted; `docs/snowflake_demo_output.md` stays as is.
