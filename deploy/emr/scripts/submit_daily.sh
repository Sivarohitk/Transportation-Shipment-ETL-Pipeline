#!/usr/bin/env bash
# Submits daily ETL step to an existing EMR cluster.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

AWS_REGION="${AWS_REGION:-us-east-1}"
CLUSTER_ID="${CLUSTER_ID:-}"
RUN_DATE="${RUN_DATE:-}"
DATA_BUCKET="${DATA_BUCKET:-YOUR_DATA_BUCKET}"

TEMPLATE_FILE="${PROJECT_ROOT}/deploy/emr/steps/daily_etl_step.json"
TMP_FILE="$(mktemp "/tmp/transport-etl-daily-step-XXXXXX.json")"

if ! command -v aws >/dev/null 2>&1; then
  echo "[submit-daily] ERROR: aws CLI not found."
  exit 1
fi

if [[ -z "${CLUSTER_ID}" && -f "${PROJECT_ROOT}/.emr_cluster_id" ]]; then
  CLUSTER_ID="$(cat "${PROJECT_ROOT}/.emr_cluster_id")"
fi

if [[ -z "${CLUSTER_ID}" ]]; then
  echo "[submit-daily] ERROR: CLUSTER_ID is required."
  exit 1
fi

if [[ -z "${RUN_DATE}" ]]; then
  echo "[submit-daily] ERROR: RUN_DATE is required (YYYY-MM-DD)."
  exit 1
fi

if [[ ! -f "${TEMPLATE_FILE}" ]]; then
  echo "[submit-daily] ERROR: Missing template ${TEMPLATE_FILE}"
  exit 1
fi

sed \
  -e "s/__RUN_DATE__/${RUN_DATE}/g" \
  -e "s/__DATA_BUCKET__/${DATA_BUCKET}/g" \
  "${TEMPLATE_FILE}" > "${TMP_FILE}"

echo "[submit-daily] Submitting daily step to cluster ${CLUSTER_ID}"
STEP_IDS="$(
  aws emr add-steps \
    --region "${AWS_REGION}" \
    --cluster-id "${CLUSTER_ID}" \
    --steps "file://${TMP_FILE}" \
    --query "StepIds" \
    --output text
)"

echo "[submit-daily] Step IDs: ${STEP_IDS}"
rm -f "${TMP_FILE}"
