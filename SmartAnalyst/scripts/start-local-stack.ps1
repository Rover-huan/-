[CmdletBinding()]
param(
    [string]$ApiHost = "127.0.0.1",
    [int]$Port = 8000,
    [switch]$SkipBeat
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

function Get-SmartAnalystProcesses {
    Get-CimInstance Win32_Process |
        Where-Object {
            $_.Name -eq "python.exe" -and
            $_.CommandLine -and
            (
                $_.CommandLine -match "service\.api:app" -or
                $_.CommandLine -match "service\.celery_app\.celery_app worker" -or
                $_.CommandLine -match "service\.celery_app\.celery_app beat"
            )
        }
}

$projectRoot = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $projectRoot ".codex-logs"
$pythonExe = Resolve-ProjectPython -ProjectRoot $projectRoot

if (-not $pythonExe) {
    throw "Could not find .venv\\Scripts\\python.exe. Checked project-local and parent-level .venv paths."
}

New-Item -ItemType Directory -Path $logDir -Force | Out-Null

$existing = @(Get-SmartAnalystProcesses)
if ($existing.Count -gt 0) {
    Write-Host "Stopping existing SmartAnalyst service processes..."
    foreach ($process in $existing) {
        Stop-Process -Id $process.ProcessId -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 2
}

$redisReady = $false
try {
    $redisReady = Test-NetConnection -ComputerName "127.0.0.1" -Port 6379 -InformationLevel Quiet
} catch {
    $redisReady = $false
}
if (-not $redisReady) {
    Write-Warning "Redis does not appear to be listening on 127.0.0.1:6379. Worker startup may fail until Redis is available."
}

$versionInfo = & $pythonExe -c "import sys, openai, httpx; print(sys.executable); print(openai.__version__); print(httpx.__version__)"
$versionLines = @($versionInfo)
Write-Host ("Using interpreter: {0}" -f $versionLines[0])
Write-Host ("Pinned package versions: openai={0}, httpx={1}" -f $versionLines[1], $versionLines[2])

$apiProcess = Start-Process -FilePath $pythonExe `
    -ArgumentList @("-m", "uvicorn", "service.api:app", "--host", $ApiHost, "--port", "$Port") `
    -WorkingDirectory $projectRoot `
    -RedirectStandardOutput (Join-Path $logDir "api.out.log") `
    -RedirectStandardError (Join-Path $logDir "api.err.log") `
    -PassThru

$workerProcess = Start-Process -FilePath $pythonExe `
    -ArgumentList @("-m", "celery", "-A", "service.celery_app.celery_app", "worker", "--loglevel=info", "--pool=solo") `
    -WorkingDirectory $projectRoot `
    -RedirectStandardOutput (Join-Path $logDir "worker.out.log") `
    -RedirectStandardError (Join-Path $logDir "worker.err.log") `
    -PassThru

$beatProcess = $null
if (-not $SkipBeat) {
    $beatProcess = Start-Process -FilePath $pythonExe `
        -ArgumentList @("-m", "celery", "-A", "service.celery_app.celery_app", "beat", "--loglevel=info") `
        -WorkingDirectory $projectRoot `
        -RedirectStandardOutput (Join-Path $logDir "beat.out.log") `
        -RedirectStandardError (Join-Path $logDir "beat.err.log") `
        -PassThru
}

$healthUrl = "http://{0}:{1}/healthz" -f $ApiHost, $Port
$deadline = (Get-Date).AddSeconds(20)
$healthy = $false
while ((Get-Date) -lt $deadline) {
    try {
        $response = Invoke-WebRequest -UseBasicParsing $healthUrl -TimeoutSec 3
        if ($response.StatusCode -eq 200) {
            $healthy = $true
            break
        }
    } catch {
        Start-Sleep -Milliseconds 500
    }
}

Write-Host ("API PID: {0}" -f $apiProcess.Id)
Write-Host ("Worker PID: {0}" -f $workerProcess.Id)
if ($beatProcess) {
    Write-Host ("Beat PID: {0}" -f $beatProcess.Id)
}

if ($healthy) {
    Write-Host ("Health check passed: {0}" -f $healthUrl)
} else {
    Write-Warning ("Health check did not succeed within 20 seconds: {0}" -f $healthUrl)
}

Write-Host "Logs:"
Write-Host ("  {0}" -f (Join-Path $logDir "api.err.log"))
Write-Host ("  {0}" -f (Join-Path $logDir "worker.err.log"))
if (-not $SkipBeat) {
    Write-Host ("  {0}" -f (Join-Path $logDir "beat.err.log"))
}
