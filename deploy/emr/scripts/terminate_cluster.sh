#!/usr/bin/env bash
# Terminates an EMR cluster for Transportation Shipment ETL.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

AWS_REGION="${AWS_REGION:-us-east-1}"
CLUSTER_ID="${CLUSTER_ID:-}"
WAIT_FOR_TERMINATION="${WAIT_FOR_TERMINATION:-false}"

if ! command -v aws >/dev/null 2>&1; then
  echo "[terminate-cluster] ERROR: aws CLI not found."
  exit 1
fi

if [[ -z "${CLUSTER_ID}" && -f "${PROJECT_ROOT}/.emr_cluster_id" ]]; then
  CLUSTER_ID="$(cat "${PROJECT_ROOT}/.emr_cluster_id")"
fi

if [[ -z "${CLUSTER_ID}" ]]; then
  echo "[terminate-cluster] ERROR: CLUSTER_ID is required."
  exit 1
fi

echo "[terminate-cluster] Terminating cluster ${CLUSTER_ID}"
aws emr terminate-clusters \
  --region "${AWS_REGION}" \
  --cluster-ids "${CLUSTER_ID}" >/dev/null

if [[ "${WAIT_FOR_TERMINATION}" == "true" ]]; then
  echo "[terminate-cluster] Waiting for cluster termination..."
  aws emr wait cluster-terminated \
    --region "${AWS_REGION}" \
    --cluster-id "${CLUSTER_ID}"
fi

echo "[terminate-cluster] Termination request submitted for ${CLUSTER_ID}"
