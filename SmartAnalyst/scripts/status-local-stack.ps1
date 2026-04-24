[CmdletBinding()]
param(
    [string]$ApiHost = "127.0.0.1",
    [int]$Port = 8000
)

$ErrorActionPreference = "Stop"

function Resolve-ProjectPython {
    param([string]$ProjectRoot)

    $candidates = @(
        (Join-Path $ProjectRoot ".venv\Scripts\python.exe"),
        (Join-Path (Split-Path -Parent $ProjectRoot) ".venv\Scripts\python.exe")
    )

    return $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
}

$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Resolve-ProjectPython -ProjectRoot $projectRoot

$processes = Get-CimInstance Win32_Process |
    Where-Object {
        $_.Name -eq "python.exe" -and
        $_.CommandLine -and
        (
            $_.CommandLine -match "service\.api:app" -or
            $_.CommandLine -match "service\.celery_app\.celery_app worker" -or
            $_.CommandLine -match "service\.celery_app\.celery_app beat"
        )
    } |
    Select-Object ProcessId, CommandLine

if ($processes) {
    Write-Host "Running SmartAnalyst processes:"
    $processes | Format-Table -AutoSize
} else {
    Write-Host "No SmartAnalyst API/worker/beat processes are running."
}

if ($pythonExe) {
    $versionInfo = & $pythonExe -c "import sys, openai, httpx; print(sys.executable); print(openai.__version__); print(httpx.__version__)"
    $versionLines = @($versionInfo)
    Write-Host ("Interpreter: {0}" -f $versionLines[0])
    Write-Host ("Pinned package versions: openai={0}, httpx={1}" -f $versionLines[1], $versionLines[2])
}

$healthUrl = "http://{0}:{1}/healthz" -f $ApiHost, $Port
try {
    $response = Invoke-WebRequest -UseBasicParsing $healthUrl -TimeoutSec 3
    Write-Host ("Health: {0} ({1})" -f $response.StatusCode, $healthUrl)
} catch {
    Write-Warning ("Health check failed: {0}" -f $healthUrl)
}
