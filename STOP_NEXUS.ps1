# NEXUS SUPREME - Stop all services
# Run: powershell -ExecutionPolicy Bypass -File "C:\Users\Yarin\Desktop\Nexus-Orchestrator\STOP_NEXUS.ps1"

Write-Host "================================================" -ForegroundColor Red
Write-Host "   NEXUS SUPREME - Stopping all services"       -ForegroundColor Red
Write-Host "================================================" -ForegroundColor Red
Write-Host ""

$patterns = @(
    "start_telegram_bot",
    "start_master",
    "start_worker",
    "uvicorn",
    "hot_reload_watcher",
    "Launch_NexusSupreme"
)

foreach ($pattern in $patterns) {
    $procs = @(Get-WmiObject Win32_Process | Where-Object {
        $_.CommandLine -and $_.CommandLine -like "*$pattern*"
    })
    foreach ($p in $procs) {
        try {
            Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
            Write-Host "  STOPPED  $pattern  (PID $($p.ProcessId))" -ForegroundColor Yellow
        } catch {
            Write-Host "  FAILED   $pattern  (PID $($p.ProcessId))" -ForegroundColor Red
        }
    }
}

# Stop Next.js frontend (node process on port 3000)
$nodeProcs = @(Get-WmiObject Win32_Process | Where-Object {
    $_.CommandLine -and (
        ($_.CommandLine -like "*next*start*") -or
        ($_.CommandLine -like "*next-server*")
    )
})
foreach ($p in $nodeProcs) {
    Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
    Write-Host "  STOPPED  frontend (PID $($p.ProcessId))" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "  Done - all Nexus services stopped." -ForegroundColor Green
Write-Host ""
