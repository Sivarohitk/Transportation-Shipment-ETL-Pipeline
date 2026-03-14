<#
.SYNOPSIS
  Packages Transportation Shipment ETL artifacts for EMR deployment.

.DESCRIPTION
  Creates a versioned zip bundle containing source code, config, SQL, docs, and
  deployment assets. Also emits a SHA256 checksum file.

.NOTES
  This script is portfolio-safe and uses no credentials.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path,

    [Parameter(Mandatory = $false)]
    [string]$OutputDir = (Join-Path $ProjectRoot "deploy\emr\dist"),

    [Parameter(Mandatory = $false)]
    [string]$VersionTag = (Get-Date -Format "yyyy.MM.dd-HHmmss"),

    [Parameter(Mandatory = $false)]
    [switch]$Clean
)

$ErrorActionPreference = "Stop"

Write-Host "[package] ProjectRoot: $ProjectRoot"
Write-Host "[package] OutputDir : $OutputDir"
Write-Host "[package] VersionTag: $VersionTag"

if ($Clean -and (Test-Path $OutputDir)) {
    Write-Host "[package] Cleaning output directory"
    Remove-Item -Path $OutputDir -Recurse -Force
}

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

$stagingDir = Join-Path $OutputDir "staging-$VersionTag"
if (Test-Path $stagingDir) {
    Remove-Item -Path $stagingDir -Recurse -Force
}
New-Item -ItemType Directory -Path $stagingDir -Force | Out-Null

$includePaths = @(
    "src",
    "config",
    "sql",
    "deploy\emr\bootstrap",
    "deploy\emr\conf",
    "deploy\emr\steps",
    "requirements.txt",
    "requirements-dev.txt",
    "pyproject.toml",
    "README.md",
    "docs"
)

foreach ($relativePath in $includePaths) {
    $sourcePath = Join-Path $ProjectRoot $relativePath
    if (-not (Test-Path $sourcePath)) {
        Write-Warning "[package] Path not found, skipping: $relativePath"
        continue
    }

    $targetPath = Join-Path $stagingDir $relativePath
    $targetParent = Split-Path -Path $targetPath -Parent
    New-Item -ItemType Directory -Path $targetParent -Force | Out-Null

    if ((Get-Item $sourcePath).PSIsContainer) {
        Copy-Item -Path $sourcePath -Destination $targetPath -Recurse -Force
    }
    else {
        Copy-Item -Path $sourcePath -Destination $targetPath -Force
    }
}

$zipName = "transport-etl-$VersionTag.zip"
$zipPath = Join-Path $OutputDir $zipName
if (Test-Path $zipPath) {
    Remove-Item -Path $zipPath -Force
}

Write-Host "[package] Creating zip: $zipPath"
Compress-Archive -Path (Join-Path $stagingDir "*") -DestinationPath $zipPath -Force

$checksum = (Get-FileHash -Path $zipPath -Algorithm SHA256).Hash
$checksumPath = "$zipPath.sha256"
Set-Content -Path $checksumPath -Value "$checksum  $zipName"

Write-Host "[package] Bundle created: $zipPath"
Write-Host "[package] Checksum file: $checksumPath"

# Keep staging directory for inspectability during demos.
Write-Host "[package] Staging directory retained: $stagingDir"
