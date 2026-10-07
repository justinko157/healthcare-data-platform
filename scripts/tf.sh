#!/usr/bin/env bash
# Run the pinned Terraform image against terraform/. The repo is mounted at /work, so the
# config's default key paths (../secrets/...) resolve inside the container.
#   ./scripts/tf.sh init | plan | apply | test | fmt -check -recursive | validate
set -euo pipefail
cd "$(dirname "$0")/.."
TF_IMAGE="hashicorp/terraform:1.16.5"

# Commands that connect to Snowflake need the Terraform user's key, and the pipeline user's
# public key (HDP_SERVICE's rsa_public_key). Fail before Docker starts, with the fix.
case "${1:-}" in
  plan|apply|destroy|import|refresh|state|output)
    missing=0
    if [ ! -f secrets/terraform_key.p8 ]; then
      echo "secrets/terraform_key.p8 not found. Create it with: ./scripts/snowflake_keygen.sh terraform" >&2
      echo "then run terraform/bootstrap_terraform_user.sql in Snowsight (see README 'Snowflake mode')." >&2
      missing=1
    fi
    if [ ! -f secrets/hdp_service_key.pub ]; then
      echo "secrets/hdp_service_key.pub not found. Create it with: ./scripts/snowflake_keygen.sh" >&2
      missing=1
    fi
    if [ "$missing" -ne 0 ]; then exit 2; fi
    ;;
esac

DOCKER_ARGS=(--rm -i -v "$PWD:/work" -w /work/terraform)
if [ -t 0 ] && [ -t 1 ]; then DOCKER_ARGS+=(-t); fi
case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*) ;; # Docker Desktop on Windows maps file ownership itself.
  *)
    # Run as the calling user, so .terraform/ and the state file aren't owned by root.
    DOCKER_ARGS+=(-u "$(id -u):$(id -g)" -e HOME=/tmp)
    ;;
esac

export MSYS_NO_PATHCONV=1
exec docker run "${DOCKER_ARGS[@]}" "$TF_IMAGE" "$@"
