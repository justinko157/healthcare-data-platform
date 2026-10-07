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
