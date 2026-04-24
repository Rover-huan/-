[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$targets = Get-CimInstance Win32_Process |
    Where-Object {
        $_.Name -eq "python.exe" -and
        $_.CommandLine -and
        (
            $_.CommandLine -match "service\.api:app" -or
            $_.CommandLine -match "service\.celery_app\.celery_app worker" -or
            $_.CommandLine -match "service\.celery_app\.celery_app beat"
        )
    }

if (-not $targets) {
    Write-Host "No SmartAnalyst API/worker/beat processes are running."
    exit 0
}

foreach ($process in $targets) {
    Write-Host ("Stopping PID {0}: {1}" -f $process.ProcessId, $process.CommandLine)
    Stop-Process -Id $process.ProcessId -Force -ErrorAction SilentlyContinue
}

Write-Host "SmartAnalyst local stack stopped."
