# Register a Windows Scheduled Task to run Sentinel LAN auto-deploy every 10 minutes.
# Run from an elevated PowerShell if your policy requires it for schtasks.
#
# Prerequisites:
#   - pythonw.exe on PATH (or edit $Pythonw below)
#   - Master Hub reachable (NEXUS_MASTER_HUB_URL) and Redis (REDIS_URL) for dashboard events
#   - WORKER_SSH_* credentials in .env for deploy targets

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$ScriptPy = Join-Path $RepoRoot "scripts\sentinel_service.py"

$pyCmd = Get-Command pythonw.exe -ErrorAction SilentlyContinue
$Pythonw = if ($pyCmd) { $pyCmd.Source } else { $null }
if (-not $Pythonw) {
    $Pythonw = (where.exe pythonw 2>$null | Select-Object -First 1)
}
if (-not $Pythonw -or -not (Test-Path -LiteralPath $Pythonw)) {
    throw "pythonw.exe not found. Install Python or add it to PATH."
}

if (-not (Test-Path -LiteralPath $ScriptPy)) {
    throw "Missing $ScriptPy"
}

$TaskName = "TeleFix-Sentinel"
# Task Scheduler runs this string; no console is shown for pythonw.
$TaskRun = "`"cmd.exe`" /c cd /d `"$RepoRoot`" && `"$Pythonw`" `"$ScriptPy`" autodeploy --once"

schtasks /Delete /TN $TaskName /F 2>$null | Out-Null
schtasks /Create `
    /TN $TaskName `
    /TR $TaskRun `
    /SC MINUTE `
    /MO 10 `
    /RL HIGHEST `
    /F | Out-Host

Write-Host "Registered task '$TaskName' (every 10 min, runs: sentinel_service.py autodeploy --once)." -ForegroundColor Green
Write-Host "Optional: set NEXUS_SENTINEL_CIDR, NEXUS_MASTER_HUB_URL, NEXUS_SENTINEL_SKIP_IPS on the task (Environment tab or setx)." -ForegroundColor Yellow
