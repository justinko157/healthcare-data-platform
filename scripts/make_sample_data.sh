#!/usr/bin/env bash
# Regenerate sample_data/ with Synthea in a throwaway Java container. Needs Docker only.
set -euo pipefail
cd "$(dirname "$0")/.."

# Stop Git Bash on Windows from rewriting container paths like /work. No-op elsewhere.
export MSYS_NO_PATHCONV=1

SYNTHEA_VERSION="${SYNTHEA_VERSION:-v3.3.0}"
POPULATION="${POPULATION:-60}"
JAR=.cache/synthea-with-dependencies.jar

mkdir -p .cache
if [ ! -f "$JAR" ]; then
  curl -fsSL -o "$JAR" \
    "https://github.com/synthetichealth/synthea/releases/download/${SYNTHEA_VERSION}/synthea-with-dependencies.jar"
fi

rm -rf .cache/sample-out
docker run --rm -v "$PWD/.cache:/work" -w /work eclipse-temurin:17-jre \
  java -Xmx2g -jar synthea-with-dependencies.jar -p "$POPULATION" -s 42 -cs 42 -r 20260101 -e 20260101 \
  --exporter.baseDirectory /work/sample-out --exporter.years_of_history 3 \
  --exporter.csv.export true --exporter.fhir.export false \
  --exporter.hospital.fhir.export false --exporter.practitioner.fhir.export false

mkdir -p sample_data
for t in patients encounters conditions medications claims claims_transactions providers payers; do
  cp ".cache/sample-out/csv/${t}.csv" "sample_data/${t}.csv"
done
du -sh sample_data
