<#
.SYNOPSIS
  Uploads ETL deployment artifacts to S3.

.DESCRIPTION
  Copies packaged zip artifacts and EMR templates to S3 using AWS CLI.
  Bucket and prefix are parameterized placeholders.

.NOTES
  No credentials are embedded. AWS CLI profile/environment should be configured externally.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$BucketName,

    [Parameter(Mandatory = $false)]
    [string]$S3Prefix = "transport-etl/releases/latest",

    [Parameter(Mandatory = $true)]
    [string]$ArtifactPath,

    [Parameter(Mandatory = $false)]
    [string]$Region = "us-east-1",

    [Parameter(Mandatory = $false)]
    [switch]$UploadTemplates = $true
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command aws -ErrorAction SilentlyContinue)) {
    throw "AWS CLI is required but was not found in PATH."
}

if (-not (Test-Path $ArtifactPath)) {
    throw "Artifact path not found: $ArtifactPath"
}

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
$baseUri = "s3://$BucketName/$S3Prefix"
$artifactName = Split-Path -Path $ArtifactPath -Leaf

Write-Host "[upload] Region    : $Region"
Write-Host "[upload] Bucket    : $BucketName"
Write-Host "[upload] Prefix    : $S3Prefix"
Write-Host "[upload] Artifact  : $artifactName"

aws s3 cp $ArtifactPath "$baseUri/dist/$artifactName" --region $Region

$checksumPath = "$ArtifactPath.sha256"
if (Test-Path $checksumPath) {
    aws s3 cp $checksumPath "$baseUri/dist/$artifactName.sha256" --region $Region
}

if ($UploadTemplates) {
    Write-Host "[upload] Uploading deployment templates"
    aws s3 sync (Join-Path $projectRoot "deploy\emr\conf") "$baseUri/conf/" --region $Region --delete
    aws s3 sync (Join-Path $projectRoot "deploy\emr\steps") "$baseUri/steps/" --region $Region --delete
    aws s3 sync (Join-Path $projectRoot "deploy\emr\bootstrap") "$baseUri/bootstrap/" --region $Region --delete
    aws s3 sync (Join-Path $projectRoot "src") "$baseUri/src/" --region $Region --delete
    aws s3 sync (Join-Path $projectRoot "config") "$baseUri/config/" --region $Region --delete
    aws s3 sync (Join-Path $projectRoot "sql") "$baseUri/sql/" --region $Region --delete
    aws s3 cp (Join-Path $projectRoot "requirements.txt") "$baseUri/requirements.txt" --region $Region
}

Write-Host "[upload] Upload complete."
Write-Host "[upload] Artifact URI: $baseUri/dist/$artifactName"
