[CmdletBinding()]
param(
    [string]$PythonExecutable
)

$ErrorActionPreference = "Continue"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$expectedVenv = Join-Path $repoRoot ".venv"
$errors = [System.Collections.Generic.List[string]]::new()

function Test-SamePath {
    param(
        [string]$Left,
        [string]$Right
    )

    if ([string]::IsNullOrWhiteSpace($Left) -or [string]::IsNullOrWhiteSpace($Right)) {
        return $false
    }

    return [System.IO.Path]::GetFullPath($Left).TrimEnd("\") -ieq `
        [System.IO.Path]::GetFullPath($Right).TrimEnd("\")
}

function Get-JavaVersionInfo {
    param([string]$Executable)

    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $Executable
    $startInfo.Arguments = "-version"
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $process = [System.Diagnostics.Process]::Start($startInfo)
    $standardOutput = $process.StandardOutput.ReadToEnd()
    $standardError = $process.StandardError.ReadToEnd()
    $process.WaitForExit()
    $versionText = "$standardOutput$standardError".Trim()
    $versionMatch = [regex]::Match($versionText, 'version\s+"(?<major>\d+)')
    $major = if ($versionMatch.Success) { [int]$versionMatch.Groups["major"].Value } else { $null }
    return [pscustomobject]@{
        VersionText = $versionText
        Major = $major
    }
}

if ([string]::IsNullOrWhiteSpace($PythonExecutable)) {
    $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
    if ($null -eq $pythonCommand) {
        Write-Host "Python executable: NOT FOUND"
        Write-Host "Repo root: $repoRoot"
        Write-Error "Python is not available on PATH. Activate .venv or run scripts/setup_windows_dev.ps1."
        exit 1
    }
    $PythonExecutable = $pythonCommand.Source
}

$pythonInfoRaw = & $PythonExecutable -c `
    "import json, sys; print(json.dumps({'executable': sys.executable, 'version': sys.version.split()[0], 'prefix': sys.prefix}))" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Error "Unable to run Python at '$PythonExecutable': $pythonInfoRaw"
    exit 1
}
$pythonInfo = $pythonInfoRaw | ConvertFrom-Json

$pysparkVersion = & $PythonExecutable -c "import pyspark; print(pyspark.__version__)" 2>&1
if ($LASTEXITCODE -ne 0) {
    $pysparkVersion = "MISSING"
    $errors.Add("PySpark is not installed in the selected Python environment.")
} elseif ($pysparkVersion -ne "3.5.2") {
    $errors.Add("PySpark must be 3.5.2; found $pysparkVersion.")
}

$pytestInfoRaw = & $PythonExecutable -c `
    "import json, pathlib, pytest; print(json.dumps({'version': pytest.__version__, 'module': str(pathlib.Path(pytest.__file__).resolve())}))" 2>&1
if ($LASTEXITCODE -ne 0) {
    $pytestVersion = "MISSING"
    $pytestModule = "MISSING"
    $errors.Add("pytest is not installed in the selected Python environment.")
} else {
    $pytestInfo = $pytestInfoRaw | ConvertFrom-Json
    $pytestVersion = $pytestInfo.version
    $pytestModule = $pytestInfo.module
}

$pytestCommand = Get-Command pytest -ErrorAction SilentlyContinue
$pytestExecutable = if ($null -eq $pytestCommand) { "NOT FOUND" } else { $pytestCommand.Source }

$javaCommand = Get-Command java -ErrorAction SilentlyContinue
if ($null -eq $javaCommand) {
    $javaExecutable = "NOT FOUND"
    $javaVersionText = "NOT FOUND"
    $javaMajor = $null
    $errors.Add("Java is not available on PATH; JDK 17 is required for local Spark tests.")
} else {
    $javaExecutable = $javaCommand.Source
    $javaInfo = Get-JavaVersionInfo -Executable $javaExecutable
    $javaVersionText = $javaInfo.VersionText
    if ($javaInfo.Major -ne 17) {
        $errors.Add("Java must be JDK 17 for the supported local Spark toolchain; found '$javaVersionText'.")
    }
}

if ([string]::IsNullOrWhiteSpace($env:JAVA_HOME)) {
    $javaHomeVersionText = "NOT SET"
    $errors.Add("JAVA_HOME is not set to a JDK 17 installation.")
} else {
    $javaHomeExecutable = Join-Path $env:JAVA_HOME "bin\java.exe"
    if (-not (Test-Path -LiteralPath $javaHomeExecutable -PathType Leaf)) {
        $javaHomeVersionText = "INVALID JAVA_HOME"
        $errors.Add("JAVA_HOME does not contain bin\java.exe: $env:JAVA_HOME")
    } else {
        $javaHomeInfo = if (Test-SamePath -Left $javaHomeExecutable -Right $javaExecutable) {
            $javaInfo
        } else {
            Get-JavaVersionInfo -Executable $javaHomeExecutable
        }
        $javaHomeVersionText = $javaHomeInfo.VersionText
        if ($javaHomeInfo.Major -ne 17) {
            $errors.Add("JAVA_HOME must reference JDK 17; found '$javaHomeVersionText'.")
        }
    }
}

$pythonVersion = [version]$pythonInfo.version
if ($pythonVersion.Major -ne 3 -or $pythonVersion.Minor -ne 12) {
    $errors.Add("Python must be 3.12.x; found $($pythonInfo.version).")
}

$venvPrefixMatches = Test-SamePath -Left $pythonInfo.prefix -Right $expectedVenv
$activeVenvMatches = Test-SamePath -Left $env:VIRTUAL_ENV -Right $expectedVenv
if (-not ($venvPrefixMatches -and $activeVenvMatches)) {
    $errors.Add("The repository .venv is not active.")
}

if ($pytestModule -ne "MISSING" -and -not $pytestModule.StartsWith($expectedVenv, [System.StringComparison]::OrdinalIgnoreCase)) {
    $errors.Add("pytest resolves outside the repository .venv: $pytestModule")
}
if ($pytestExecutable -ne "NOT FOUND" -and -not $pytestExecutable.StartsWith($expectedVenv, [System.StringComparison]::OrdinalIgnoreCase)) {
    $errors.Add("The pytest launcher on PATH resolves outside the repository .venv: $pytestExecutable")
}

Write-Host "Python executable: $($pythonInfo.executable)"
Write-Host "Python version: $($pythonInfo.version)"
Write-Host "PySpark version: $pysparkVersion"
Write-Host "pytest executable/version: $pytestExecutable / $pytestVersion"
Write-Host "pytest module: $pytestModule"
Write-Host "Java executable: $javaExecutable"
Write-Host "Java version:"
Write-Host $javaVersionText
Write-Host "JAVA_HOME: $env:JAVA_HOME"
Write-Host "JAVA_HOME Java version:"
Write-Host $javaHomeVersionText
Write-Host "VIRTUAL_ENV: $env:VIRTUAL_ENV"
Write-Host "Repo root: $repoRoot"

if ($errors.Count -gt 0) {
    Write-Host ""
    Write-Host "Environment check FAILED:" -ForegroundColor Red
    foreach ($environmentError in $errors) {
        Write-Host "- $environmentError" -ForegroundColor Red
    }
    exit 1
}

Write-Host ""
Write-Host "Environment check PASSED." -ForegroundColor Green
