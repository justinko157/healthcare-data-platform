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
