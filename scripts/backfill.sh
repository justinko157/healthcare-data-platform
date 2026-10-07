#!/usr/bin/env bash
# Backfill the last $DAYS days (default 14) so Grafana's trend panels have history.
# Runs inside the Airflow container (the seed-history service does this on first start):
#   docker compose exec airflow bash /opt/project/scripts/backfill.sh
# Uses sample_data/ by default (fast). USE_SAMPLE=false runs real Synthea for each day.
set -euo pipefail
DAYS="${DAYS:-14}"
USE_SAMPLE="${USE_SAMPLE:-true}"

echo "waiting for patient_pipeline to be parsed..."
for _ in $(seq 1 60); do
  airflow dags details patient_pipeline >/dev/null 2>&1 && break
  sleep 5
done

FROM=$(date -u -d "${DAYS} days ago" +%Y-%m-%d)
TO=$(date -u -d "1 day ago" +%Y-%m-%d)
echo "backfilling ${FROM} .. ${TO} (use_sample=${USE_SAMPLE})"
# Equivalent to:
#   airflow backfill create --dag-id patient_pipeline --from-date "$FROM" --to-date "$TO" \
#     --max-active-runs 1 --reprocess-behavior none --dag-run-conf '{"use_sample": true}'
# but Airflow 3.1.0's CLI passes --dag-run-conf through as an unparsed string
# (ValueError in Param.update), so call the function behind it with a parsed dict.
python - "$FROM" "$TO" "$USE_SAMPLE" <<'PY'
import json
import sys
from datetime import datetime, timezone

from airflow.models.backfill import AlreadyRunningBackfill, ReprocessBehavior, _create_backfill

start, end, use_sample = sys.argv[1:4]
try:
    backfill = _create_backfill(
        dag_id="patient_pipeline",
        from_date=datetime.fromisoformat(start).replace(tzinfo=timezone.utc),
        to_date=datetime.fromisoformat(end).replace(tzinfo=timezone.utc),
        max_active_runs=1,
        reverse=False,
        dag_run_conf={"use_sample": json.loads(use_sample)},
        triggering_user_name="seed-history",
        reprocess_behavior=ReprocessBehavior.NONE,
    )
except AlreadyRunningBackfill:
    # `docker compose up` on an existing stack restarts seed-history; let the running one finish.
    print("a backfill is already running for patient_pipeline; leaving it to finish")
    sys.exit(0)
print(f"backfill {backfill.id} created for {start} .. {end}" if backfill else "nothing to backfill")
PY
