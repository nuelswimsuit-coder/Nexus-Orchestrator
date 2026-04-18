# NEXUS SUPREME - Check service status
# Run: powershell -ExecutionPolicy Bypass -File "C:\Users\Yarin\Desktop\Nexus-Orchestrator\STATUS_NEXUS.ps1"

$ROOT = "C:\Users\Yarin\Desktop\Nexus-Orchestrator"

Write-Host "================================================" -ForegroundColor Cyan
Write-Host "   NEXUS SUPREME - Service Status"              -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""

function Test-Port {
    param([int]$Port)
    try {
        $c = New-Object System.Net.Sockets.TcpClient
        $c.Connect("127.0.0.1", $Port)
        $c.Close()
        return $true
    } catch { return $false }
}

function Get-ServiceInfo {
    param([string]$Pattern)
    $procs = Get-WmiObject Win32_Process | Where-Object {
        $_.CommandLine -and $_.CommandLine -like "*$Pattern*"
    }
    # Use Measure-Object — works on both single objects and arrays in PS5
    $cnt  = ($procs | Measure-Object).Count
    $pids = ($procs | ForEach-Object { $_.ProcessId }) -join ", "
    return @{ Count = $cnt; Pids = $pids }
}

function Show-Row {
    param([string]$Label, [int]$Count, [string]$Detail = "")
    if ($Count -gt 0) {
        $tag   = "[ UP ]  "
        $color = "Green"
        $extra = if ($Detail) { "  PID: $Detail" } else { "" }
    } else {
        $tag   = "[ DOWN ]"
        $color = "Red"
        $extra = ""
    }
    Write-Host ("  {0,-28} {1}{2}" -f $Label, $tag, $extra) -ForegroundColor $color
}

# --- Port-based checks ---
$redisUp    = if (Test-Port -Port 6379) { 1 } else { 0 }
$apiUp      = if ((Test-Port -Port 8001) -or (Test-Port -Port 8000)) { 1 } else { 0 }
$frontendUp = if (Test-Port -Port 3000)  { 1 } else { 0 }

Show-Row -Label "Redis          :6379" -Count $redisUp
Show-Row -Label "FastAPI        :8001" -Count $apiUp

Show-Row -Label "Frontend       :3000" -Count $frontendUp
if ($frontendUp -gt 0) {
    Write-Host "    --> http://localhost:3000/dashboard" -ForegroundColor Cyan
}

# --- Process-based checks ---
$master  = Get-ServiceInfo "start_master"
$worker  = Get-ServiceInfo "start_worker"
$gui     = Get-ServiceInfo "Launch_NexusSupreme"

Show-Row -Label "Master + Bot" -Count $master.Count  -Detail $master.Pids
Show-Row -Label "Worker"       -Count $worker.Count  -Detail $worker.Pids
Show-Row -Label "Desktop GUI"  -Count $gui.Count     -Detail $gui.Pids

Write-Host ""
Write-Host "------------------------------------------------" -ForegroundColor DarkGray

# --- Recent log lines ---
foreach ($entry in @(
    @{ File = "master.log";     Label = "master (last 3)" },
    @{ File = "master.err.log"; Label = "master ERRORS" },
    @{ File = "api.log";        Label = "api (last 2)" },
    @{ File = "frontend.log";   Label = "frontend (last 2)" }
)) {
    $logPath = Join-Path $ROOT "logs\$($entry.File)"
    if (Test-Path $logPath) {
        $lines = @(Get-Content $logPath -Tail 3 -ErrorAction SilentlyContinue)
        if ($lines.Count -gt 0) {
            Write-Host "  [$($entry.Label)]" -ForegroundColor DarkYellow
            foreach ($line in $lines) {
                $short = if ($line.Length -gt 130) { $line.Substring(0,130) + "..." } else { $line }
                Write-Host "    $short" -ForegroundColor DarkGray
            }
        }
    }
}

Write-Host ""
Write-Host "  Dashboard : http://localhost:3000/dashboard"   -ForegroundColor Cyan
Write-Host "  API docs  : http://localhost:8001/docs"        -ForegroundColor Cyan
Write-Host ""
