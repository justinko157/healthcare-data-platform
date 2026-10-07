#!/usr/bin/env bash
# Create the HDP_SERVICE key pair in ./secrets (gitignored) and print the public key
# to paste into snowflake/bootstrap.sql as <PUBLIC_KEY>.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p secrets
if [ -f secrets/hdp_service_key.p8 ]; then
  echo "secrets/hdp_service_key.p8 already exists; not overwriting." >&2
else
  openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out secrets/hdp_service_key.p8 -nocrypt
  chmod 600 secrets/hdp_service_key.p8
fi
openssl rsa -in secrets/hdp_service_key.p8 -pubout -out secrets/hdp_service_key.pub 2>/dev/null
echo "Public key for bootstrap.sql (<PUBLIC_KEY>):"
grep -v -- '-----' secrets/hdp_service_key.pub | tr -d '\n'
echo
