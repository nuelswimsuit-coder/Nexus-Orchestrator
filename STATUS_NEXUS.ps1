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

function Get-ProcsByScript {
    param([string]$ScriptName)
    # @() wrapping ensures array even when WMI returns a single object (PS5 quirk)
    return @(Get-WmiObject Win32_Process | Where-Object {
        $_.CommandLine -and $_.CommandLine -like "*$ScriptName*"
    })
}

function Show-Row {
    param([string]$Label, [bool]$Up, [string]$Detail = "")
    $tag   = if ($Up) { "[ UP ]  " } else { "[ DOWN ]" }
    $color = if ($Up) { "Green"   } else { "Red"     }
    $extra = if ($Detail) { "  PID: $Detail" } else { "" }
    Write-Host ("  {0,-28} {1}{2}" -f $Label, $tag, $extra) -ForegroundColor $color
}

# --- Ports ---
Show-Row -Label "Redis          :6379" -Up (Test-Port -Port 6379)
Show-Row -Label "FastAPI        :8001" -Up ((Test-Port -Port 8001) -or (Test-Port -Port 8000))

# --- Frontend ---
$frontendUp = Test-Port -Port 3000
Show-Row -Label "Frontend       :3000" -Up $frontendUp
if ($frontendUp) {
    Write-Host "    --> http://localhost:3000/dashboard" -ForegroundColor Cyan
}

# --- Master ---
$masterProcs = Get-ProcsByScript "start_master"
$masterPids  = ($masterProcs | ForEach-Object { $_.ProcessId }) -join ", "
Show-Row -Label "Master + Bot" -Up ($masterProcs.Count -gt 0) -Detail $masterPids

# --- Worker ---
$workerProcs = Get-ProcsByScript "start_worker"
$workerPids  = ($workerProcs | ForEach-Object { $_.ProcessId }) -join ", "
Show-Row -Label "Worker" -Up ($workerProcs.Count -gt 0) -Detail $workerPids

# --- GUI ---
$guiProcs = Get-ProcsByScript "Launch_NexusSupreme"
$guiPids  = ($guiProcs | ForEach-Object { $_.ProcessId }) -join ", "
Show-Row -Label "Desktop GUI" -Up ($guiProcs.Count -gt 0) -Detail $guiPids

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
