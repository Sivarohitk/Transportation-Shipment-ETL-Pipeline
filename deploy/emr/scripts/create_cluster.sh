#!/usr/bin/env bash
# Creates an EMR cluster for Transportation Shipment ETL.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

AWS_REGION="${AWS_REGION:-us-east-1}"
EMR_CLUSTER_NAME="${EMR_CLUSTER_NAME:-transport-etl-prod}"
EMR_RELEASE_LABEL="${EMR_RELEASE_LABEL:-emr-7.0.0}"
EMR_LOG_URI="${EMR_LOG_URI:-s3://YOUR_LOG_BUCKET/emr/logs}"
EMR_SERVICE_ROLE="${EMR_SERVICE_ROLE:-EMR_DefaultRole}"
EMR_EC2_ROLE="${EMR_EC2_ROLE:-EMR_EC2_DefaultRole}"
EMR_SUBNET_ID="${EMR_SUBNET_ID:-subnet-xxxxxxxx}"
EMR_EC2_KEY_NAME="${EMR_EC2_KEY_NAME:-YOUR_EC2_KEY_NAME}"

MASTER_INSTANCE_TYPE="${MASTER_INSTANCE_TYPE:-m5.xlarge}"
CORE_INSTANCE_TYPE="${CORE_INSTANCE_TYPE:-m5.xlarge}"
CORE_INSTANCE_COUNT="${CORE_INSTANCE_COUNT:-2}"

BOOTSTRAP_SCRIPT_S3_URI="${BOOTSTRAP_SCRIPT_S3_URI:-s3://YOUR_ARTIFACT_BUCKET/transport-etl/releases/latest/bootstrap/bootstrap.sh}"
BOOTSTRAP_ARTIFACT_URI="${BOOTSTRAP_ARTIFACT_URI:-s3://YOUR_ARTIFACT_BUCKET/transport-etl/releases/latest}"

CONFIG_FILE="${PROJECT_ROOT}/deploy/emr/conf/emr_configurations.json"

if ! command -v aws >/dev/null 2>&1; then
  echo "[create-cluster] ERROR: aws CLI not found."
  exit 1
fi

if [[ ! -f "${CONFIG_FILE}" ]]; then
  echo "[create-cluster] ERROR: Missing configuration file ${CONFIG_FILE}"
  exit 1
fi

echo "[create-cluster] Creating cluster in region ${AWS_REGION}"
echo "[create-cluster] Name: ${EMR_CLUSTER_NAME}"
echo "[create-cluster] Log URI: ${EMR_LOG_URI}"

CLUSTER_ID="$(
  aws emr create-cluster \
    --region "${AWS_REGION}" \
    --name "${EMR_CLUSTER_NAME}" \
    --release-label "${EMR_RELEASE_LABEL}" \
    --applications Name=Spark Name=Hadoop Name=Hive \
    --log-uri "${EMR_LOG_URI}" \
    --service-role "${EMR_SERVICE_ROLE}" \
    --ec2-attributes "InstanceProfile=${EMR_EC2_ROLE},SubnetId=${EMR_SUBNET_ID},KeyName=${EMR_EC2_KEY_NAME}" \
    --instance-groups "InstanceGroupType=MASTER,InstanceType=${MASTER_INSTANCE_TYPE},InstanceCount=1" \
                      "InstanceGroupType=CORE,InstanceType=${CORE_INSTANCE_TYPE},InstanceCount=${CORE_INSTANCE_COUNT}" \
    --configurations "file://${CONFIG_FILE}" \
    --bootstrap-actions "Path=${BOOTSTRAP_SCRIPT_S3_URI},Name=transport-etl-bootstrap,Args=[${BOOTSTRAP_ARTIFACT_URI}]" \
    --query "ClusterId" \
    --output text
)"

echo "[create-cluster] Cluster created: ${CLUSTER_ID}"
echo "${CLUSTER_ID}" > "${PROJECT_ROOT}/.emr_cluster_id"
echo "[create-cluster] Cluster ID saved to ${PROJECT_ROOT}/.emr_cluster_id"
