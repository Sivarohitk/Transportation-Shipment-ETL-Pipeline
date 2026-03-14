# EMR Run Instructions

This guide shows how to package, upload, and run the ETL pipeline on Amazon EMR with safe placeholders.

## 1. Prerequisites
- AWS CLI v2 configured (`aws sts get-caller-identity` succeeds)
- IAM roles available for EMR service and EC2 profile
- S3 buckets created for:
  - artifacts
  - raw/staging/curated data
  - EMR/Spark logs
- Local tooling:
  - PowerShell 7+ (for packaging/upload scripts on Windows)
  - Bash shell (for EMR create/submit/terminate scripts)

No credentials should be hardcoded in scripts or JSON files.

## 2. Configure Environment Variables
Set placeholders before running scripts:

```bash
export AWS_REGION="us-east-1"
export ARTIFACT_BUCKET="YOUR_ARTIFACT_BUCKET"
export DATA_BUCKET="YOUR_DATA_BUCKET"
export LOG_BUCKET="YOUR_LOG_BUCKET"
export ARTIFACT_PREFIX="transport-etl"
export EMR_CLUSTER_NAME="transport-etl-prod"
export EMR_SUBNET_ID="subnet-xxxxxxxx"
export EMR_EC2_KEY_NAME="YOUR_EC2_KEY_NAME"
export EMR_SERVICE_ROLE="EMR_DefaultRole"
export EMR_EC2_ROLE="EMR_EC2_DefaultRole"
```

## 3. Package Artifacts
From project root:

```powershell
pwsh ./deploy/emr/scripts/package_artifacts.ps1 `
  -ProjectRoot . `
  -OutputDir ./deploy/emr/dist `
  -VersionTag 2026.03.13
```

Expected outputs:
- zipped artifact bundle in `deploy/emr/dist`
- `.sha256` checksum file

## 4. Upload Artifacts to S3
```powershell
pwsh ./deploy/emr/scripts/upload_to_s3.ps1 `
  -BucketName YOUR_ARTIFACT_BUCKET `
  -S3Prefix transport-etl/releases/2026.03.13 `
  -ArtifactPath ./deploy/emr/dist/transport-etl-2026.03.13.zip `
  -Region us-east-1
```

This uploads:
- artifact bundle
- EMR config templates
- EMR step templates
- bootstrap script

## 5. Create EMR Cluster
```bash
bash ./deploy/emr/scripts/create_cluster.sh
```

The script prints a `CLUSTER_ID` and writes it to `.emr_cluster_id`.

## 6. Submit Daily Job
```bash
export CLUSTER_ID="j-XXXXXXXXXXXXX"
export RUN_DATE="2026-01-01"
bash ./deploy/emr/scripts/submit_daily.sh
```

## 7. Submit Backfill Job
```bash
export CLUSTER_ID="j-XXXXXXXXXXXXX"
export START_DATE="2026-01-01"
export END_DATE="2026-01-07"
bash ./deploy/emr/scripts/submit_backfill.sh
```

## 8. Monitor and Validate
- EMR step status:
```bash
aws emr list-steps --cluster-id "$CLUSTER_ID" --region "$AWS_REGION"
```
- Spark/YARN logs in configured log S3 path
- Validate curated outputs under:
  - `s3://YOUR_DATA_BUCKET/transport/curated/dim_carrier/`
  - `s3://YOUR_DATA_BUCKET/transport/curated/fct_shipment/`
  - `s3://YOUR_DATA_BUCKET/transport/curated/fct_delivery_event/`
  - `s3://YOUR_DATA_BUCKET/transport/curated/agg_shipment_daily/`
  - `s3://YOUR_DATA_BUCKET/transport/curated/kpi_delivery_daily/`

## 9. Terminate Cluster
```bash
export CLUSTER_ID="j-XXXXXXXXXXXXX"
bash ./deploy/emr/scripts/terminate_cluster.sh
```

## 10. Cost and Safety Checklist
- Use non-production account for demos and portfolio walkthroughs
- Use transient clusters when possible
- Always terminate cluster after run validation
- Use placeholder values in docs/JSON before publishing repository
