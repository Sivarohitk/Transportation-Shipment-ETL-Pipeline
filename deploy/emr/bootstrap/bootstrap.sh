#!/usr/bin/env bash
# EMR bootstrap script for Transportation Shipment ETL.
# Args:
#   1) ARTIFACTS_S3_URI (optional)
#      Example: s3://YOUR_ARTIFACT_BUCKET/transport-etl/releases/latest

set -euo pipefail

ARTIFACTS_S3_URI="${1:-s3://YOUR_ARTIFACT_BUCKET/transport-etl/releases/latest}"
APP_HOME="/opt/transport-etl"
TMP_DIR="/tmp/transport-etl-bootstrap"
LOG_FILE="/var/log/transport-etl-bootstrap.log"

mkdir -p "${APP_HOME}" "${TMP_DIR}"
exec > >(tee -a "${LOG_FILE}") 2>&1

echo "[bootstrap] Starting bootstrap at $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "[bootstrap] ARTIFACTS_S3_URI=${ARTIFACTS_S3_URI}"

if ! command -v aws >/dev/null 2>&1; then
  echo "[bootstrap] ERROR: aws CLI not found on EMR node."
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "[bootstrap] ERROR: python3 not found on EMR node."
  exit 1
fi

echo "[bootstrap] Upgrading pip/setuptools/wheel"
sudo python3 -m pip install --upgrade pip setuptools wheel

echo "[bootstrap] Downloading artifact bundle (if available)"
if aws s3 ls "${ARTIFACTS_S3_URI}/dist/" >/dev/null 2>&1; then
  LATEST_BUNDLE="$(aws s3 ls "${ARTIFACTS_S3_URI}/dist/" | awk '{print $4}' | grep '\.zip$' | sort | tail -n 1 || true)"
  if [[ -n "${LATEST_BUNDLE}" ]]; then
    aws s3 cp "${ARTIFACTS_S3_URI}/dist/${LATEST_BUNDLE}" "${TMP_DIR}/${LATEST_BUNDLE}"
    unzip -o "${TMP_DIR}/${LATEST_BUNDLE}" -d "${APP_HOME}"
  else
    echo "[bootstrap] WARNING: No zip artifact found under ${ARTIFACTS_S3_URI}/dist/"
  fi
else
  echo "[bootstrap] WARNING: Artifact dist directory not found. Skipping bundle download."
fi

echo "[bootstrap] Syncing config/sql/src templates (best effort)"
aws s3 sync "${ARTIFACTS_S3_URI}/config/" "${APP_HOME}/config/" --delete || true
aws s3 sync "${ARTIFACTS_S3_URI}/sql/" "${APP_HOME}/sql/" --delete || true
aws s3 sync "${ARTIFACTS_S3_URI}/src/" "${APP_HOME}/src/" --delete || true

if [[ -f "${APP_HOME}/requirements.txt" ]]; then
  echo "[bootstrap] Installing Python dependencies from requirements.txt"
  sudo python3 -m pip install -r "${APP_HOME}/requirements.txt"
else
  echo "[bootstrap] WARNING: requirements.txt not found under ${APP_HOME}"
fi

echo "[bootstrap] Ensuring runtime directories exist"
sudo mkdir -p /var/log/transport-etl
sudo chown -R hadoop:hadoop "${APP_HOME}" /var/log/transport-etl

echo "[bootstrap] Bootstrap completed at $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
