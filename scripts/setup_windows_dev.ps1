[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$venvPath = Join-Path $repoRoot ".venv"
$venvPython = Join-Path $venvPath "Scripts\python.exe"
$venvScripts = Join-Path $venvPath "Scripts"

function Get-JavaMajorVersion {
    param([string]$JavaHome)

    $javaExecutable = Join-Path $JavaHome "bin\java.exe"
    if (-not (Test-Path -LiteralPath $javaExecutable -PathType Leaf)) {
        return $null
    }

    $versionText = (& $javaExecutable -version 2>&1 | Out-String).Trim()
    $versionMatch = [regex]::Match($versionText, 'version\s+"(?<major>\d+)')
    if ($versionMatch.Success) {
        return [int]$versionMatch.Groups["major"].Value
    }
    return $null
}

function Find-Jdk17 {
    $candidates = [System.Collections.Generic.List[string]]::new()
    if (-not [string]::IsNullOrWhiteSpace($env:JAVA_HOME)) {
        $candidates.Add($env:JAVA_HOME)
    }

    $searchPatterns = @(
        "$env:ProgramFiles\Eclipse Adoptium\jdk-17*",
        "$env:ProgramFiles\Microsoft\jdk-17*",
        "$env:ProgramFiles\Java\jdk-17*",
        "$env:ProgramFiles\Amazon Corretto\jdk17*",
        "$env:ProgramFiles\Zulu\zulu-17*"
    )
    foreach ($pattern in $searchPatterns) {
        Get-ChildItem -Path $pattern -Directory -ErrorAction SilentlyContinue |
            Sort-Object LastWriteTime -Descending |
            ForEach-Object { $candidates.Add($_.FullName) }
    }

    foreach ($candidate in $candidates | Select-Object -Unique) {
        if ((Get-JavaMajorVersion -JavaHome $candidate) -eq 17) {
            return $candidate
        }
    }
    return $null
}

$pyLauncher = Get-Command py -ErrorAction SilentlyContinue
if ($null -eq $pyLauncher) {
    throw "The Windows py launcher was not found. Install Python 3.12 with the launcher, then rerun this script."
}
$pyExecutable = $pyLauncher.Source

$python312Version = & $pyExecutable -3.12 -c "import sys; print(sys.version_info[:2] == (3, 12)); print(sys.version.split()[0])" 2>&1
if ($LASTEXITCODE -ne 0 -or $python312Version[0] -ne "True") {
    Write-Host "Detected Python installations:"
    & $pyExecutable -0p
    throw "Python 3.12 is required but unavailable. Install Python 3.12, ensure 'py -3.12' works, and rerun this script."
}

Push-Location $repoRoot
try {
    if (-not (Test-Path -LiteralPath $venvPython -PathType Leaf)) {
        Write-Host "Creating .venv with Python $($python312Version[1])..."
        & $pyExecutable -3.12 -m venv .venv
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to create .venv with Python 3.12."
        }
    } else {
        $existingVersion = & $venvPython -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
        if ($existingVersion -ne "3.12") {
            throw "Existing .venv uses Python $existingVersion. Remove it explicitly, then rerun this script to create a Python 3.12 environment."
        }
        Write-Host "Reusing existing Python 3.12 .venv."
    }

    $env:VIRTUAL_ENV = $venvPath
    if (-not (($env:PATH -split ";") -contains $venvScripts)) {
        $env:PATH = "$venvScripts;$env:PATH"
    }

    $jdk17 = Find-Jdk17
    if ($null -ne $jdk17) {
        $env:JAVA_HOME = $jdk17
        $jdkBin = Join-Path $jdk17 "bin"
        if (-not (($env:PATH -split ";") -contains $jdkBin)) {
            $env:PATH = "$jdkBin;$env:PATH"
        }
        Write-Host "Using JDK 17 for this PowerShell process: $jdk17"
    } else {
        Write-Warning "JDK 17 was not found. JDK 21 is not the supported local runtime for PySpark 3.5.2."
        Write-Host "Install Temurin 17 without removing JDK 21, then set this shell explicitly:"
        Write-Host '  $env:JAVA_HOME = "<actual JDK17 path>"'
        Write-Host '  $env:PATH = "$env:JAVA_HOME\bin;$env:PATH"'
    }

    & $venvPython -m pip --version *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "pip is missing from the existing .venv; restoring it with ensurepip..."
        & $venvPython -m ensurepip --upgrade
        if ($LASTEXITCODE -ne 0) {
            throw "Unable to install pip into .venv with ensurepip."
        }
    }

    & $venvPython -m pip install --upgrade pip
    if ($LASTEXITCODE -ne 0) {
        throw "pip upgrade failed."
    }
    & $venvPython -m pip install -e ".[dev]"
    if ($LASTEXITCODE -ne 0) {
        throw "Development dependency installation failed."
    }

    & (Join-Path $PSScriptRoot "check_environment.ps1") -PythonExecutable $venvPython
    if ($LASTEXITCODE -ne 0) {
        Write-Host ""
        Write-Host "Setup is incomplete. Correct the reported environment issue and rerun:" -ForegroundColor Red
        Write-Host "  .\scripts\check_environment.ps1"
        exit 1
    }
} finally {
    Pop-Location
}
