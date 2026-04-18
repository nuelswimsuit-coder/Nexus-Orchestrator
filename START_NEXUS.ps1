# NEXUS SUPREME - Start all services
# Run: powershell -ExecutionPolicy Bypass -File "C:\Users\Yarin\Desktop\Nexus-Orchestrator\START_NEXUS.ps1"

$ROOT     = "C:\Users\Yarin\Desktop\Nexus-Orchestrator"
$FRONTEND = "C:\Users\Yarin\Desktop\Nexus-Orchestrator\frontend"
$PYTHON   = "python"
$NODE     = "node"
$NPM      = "npm"

Set-Location $ROOT
$env:PYTHONUTF8 = "1"

New-Item -ItemType Directory -Force -Path "$ROOT\logs" | Out-Null

Write-Host "================================================" -ForegroundColor Cyan
Write-Host "   NEXUS SUPREME - Starting services"           -ForegroundColor Cyan
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

function Wait-Port {
    param([int]$Port, [string]$Label, [int]$TimeoutSec = 30)
    $elapsed = 0
    while ($elapsed -lt $TimeoutSec) {
        if (Test-Port -Port $Port) {
            Write-Host "  OK - $Label ready on :$Port" -ForegroundColor Green
            return
        }
        Start-Sleep -Seconds 1
        $elapsed++
    }
    Write-Host "  WARN - $Label not ready after ${TimeoutSec}s" -ForegroundColor Yellow
}

# ================================================
# [0] Kill leftover processes + clear Telegram sessions
# ================================================
Write-Host "[0/5] Cleaning up leftover processes..." -ForegroundColor DarkGray

$killPatterns = @("start_telegram_bot", "start_master", "start_worker")
$killedAny = $false
foreach ($pat in $killPatterns) {
    $procs = @(Get-WmiObject Win32_Process | Where-Object {
        $_.CommandLine -and $_.CommandLine -like "*$pat*"
    })
    foreach ($p in $procs) {
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
        Write-Host "  Killed $pat (PID $($p.ProcessId))" -ForegroundColor DarkGray
        $killedAny = $true
    }
}

if ($killedAny) {
    Write-Host "  Waiting 5s before clearing Telegram sessions..." -ForegroundColor DarkGray
    Start-Sleep -Seconds 5
}

# Step 1: deleteWebhook + getUpdates to actively release long-poll session
Write-Host "  Clearing Telegram session locks..." -ForegroundColor DarkGray
$envFile = Join-Path $ROOT ".env"
if (Test-Path $envFile) {
    $lines = Get-Content $envFile
    foreach ($line in $lines) {
        if ($line -match "^(TELEGRAM_BOT_TOKEN|TELEGRAM_NEXUS_BOT_TOKEN)\s*=\s*(.+)") {
            $token = $matches[2].Trim()
            if ($token -and -not $token.StartsWith("#")) {
                $shortId = ($token -split ':')[0]
                try {
                    # 1. Delete webhook
                    $url = "https://api.telegram.org/bot$token/deleteWebhook?drop_pending_updates=true"
                    Invoke-WebRequest -Uri $url -TimeoutSec 5 -UseBasicParsing -ErrorAction SilentlyContinue | Out-Null
                    # 2. getUpdates with timeout=0 — forces Telegram to close any existing long-poll session
                    $url2 = "https://api.telegram.org/bot$token/getUpdates?timeout=0&offset=-1"
                    Invoke-WebRequest -Uri $url2 -TimeoutSec 8 -UseBasicParsing -ErrorAction SilentlyContinue | Out-Null
                    Write-Host "  Session cleared ($shortId...)" -ForegroundColor DarkGray
                } catch {}
            }
        }
    }
}

# Step 2: Wait for Telegram to fully release the old polling connection (30s TTL)
if ($killedAny) {
    Write-Host "  Waiting 30s for Telegram polling TTL to expire..." -ForegroundColor DarkGray
    Start-Sleep -Seconds 30
} else {
    Start-Sleep -Seconds 2
}

# ================================================
# [1] Redis
# ================================================
Write-Host "[1/5] Redis (port 6379)" -ForegroundColor Yellow
if (Test-Port -Port 6379) {
    Write-Host "  OK - Redis already running" -ForegroundColor Green
} else {
    Write-Host "  Starting Redis..." -ForegroundColor DarkYellow
    $redisScript = Join-Path $ROOT "scripts\start_redis_windows.ps1"
    if (Test-Path $redisScript) {
        Start-Process powershell -ArgumentList "-ExecutionPolicy Bypass -File `"$redisScript`"" -WindowStyle Minimized
    } else {
        Start-Process "redis-server" -WindowStyle Minimized -ErrorAction SilentlyContinue
    }
    Wait-Port -Port 6379 -Label "Redis" -TimeoutSec 20
}

# ================================================
# [2] FastAPI server (port 8001)
# ================================================
Write-Host "[2/5] FastAPI server (port 8001)" -ForegroundColor Yellow
if (Test-Port -Port 8001) {
    Write-Host "  OK - FastAPI already running on :8001" -ForegroundColor Green
} else {
    # Kill any stale instance on :8000 (old manual starts)
    $stale = @(Get-WmiObject Win32_Process | Where-Object {
        $_.CommandLine -and ($_.CommandLine -like "*uvicorn*" -or $_.CommandLine -like "*start_api*")
    })
    foreach ($p in $stale) {
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
        Write-Host "  Killed stale API process (PID $($p.ProcessId))" -ForegroundColor DarkGray
    }
    Start-Sleep -Seconds 1

    $apiLog = Join-Path $ROOT "logs\api.log"
    $apiErr = Join-Path $ROOT "logs\api.err.log"
    # Use start_api.py — picks up correct port (8001) from config automatically
    Start-Process -FilePath $PYTHON `
        -ArgumentList (Join-Path $ROOT "scripts\start_api.py") `
        -WorkingDirectory $ROOT `
        -RedirectStandardOutput $apiLog `
        -RedirectStandardError  $apiErr `
        -WindowStyle Hidden
    Wait-Port -Port 8001 -Label "FastAPI" -TimeoutSec 30
}

# ================================================
# [3] Master dispatcher (runs Telegram bot inside)
# ================================================
Write-Host "[3/5] Master dispatcher + Telegram bot" -ForegroundColor Yellow

$masterProcs = @(Get-WmiObject Win32_Process | Where-Object {
    $_.CommandLine -and $_.CommandLine -like "*start_master*"
})
if ($masterProcs.Count -gt 0) {
    Write-Host "  OK - Master already running (PID $($masterProcs[0].ProcessId))" -ForegroundColor Green
} else {
    $masterScript = Join-Path $ROOT "scripts\start_master.py"
    $masterLog    = Join-Path $ROOT "logs\master.log"
    $masterErr    = Join-Path $ROOT "logs\master.err.log"
    $proc = Start-Process -FilePath $PYTHON `
        -ArgumentList $masterScript `
        -WorkingDirectory $ROOT `
        -RedirectStandardOutput $masterLog `
        -RedirectStandardError  $masterErr `
        -WindowStyle Hidden -PassThru
    Start-Sleep -Seconds 4
    if ($proc.HasExited) {
        Write-Host "  ERROR - Master crashed! See: logs\master.err.log" -ForegroundColor Red
        @(Get-Content $masterErr -Tail 5 -ErrorAction SilentlyContinue) | ForEach-Object {
            Write-Host "    $_" -ForegroundColor Red
        }
    } else {
        Write-Host "  OK - Master running (PID $($proc.Id))" -ForegroundColor Green
    }
}

# ================================================
# [4] Next.js Frontend Dashboard (port 3000)
# ================================================
Write-Host "[4/5] Frontend Dashboard (port 3000)" -ForegroundColor Yellow

if (Test-Port -Port 3000) {
    Write-Host "  OK - Frontend already running" -ForegroundColor Green
} else {
    # Check Node.js
    $nodeOk = $null
    try {
        $nodeOk = & node --version 2>$null
    } catch {}

    if (-not $nodeOk) {
        Write-Host "  SKIP - Node.js not found. Install from https://nodejs.org" -ForegroundColor Yellow
    } else {
        $frontendLog = Join-Path $ROOT "logs\frontend.log"
        # npm is a .cmd file on Windows — must run via cmd.exe /c
        Start-Process -FilePath "cmd.exe" `
            -ArgumentList "/c", "npm start -- -p 3000 > `"$frontendLog`" 2>&1" `
            -WorkingDirectory $FRONTEND `
            -WindowStyle Hidden
        Wait-Port -Port 3000 -Label "Frontend" -TimeoutSec 25
        Write-Host "  Dashboard: http://localhost:3000" -ForegroundColor Cyan
    }
}

# ================================================
# [5] Nexus Supreme GUI (one instance only)
# ================================================
Write-Host "[5/5] Nexus Supreme GUI" -ForegroundColor Yellow
$guiRunning = (Get-WmiObject Win32_Process | Where-Object {
    $_.CommandLine -and $_.CommandLine -like "*Launch_NexusSupreme*"
} | Measure-Object).Count
if ($guiRunning -gt 0) {
    Write-Host "  OK - GUI already running" -ForegroundColor Green
} else {
    $guiScript = Join-Path $ROOT "Launch_NexusSupreme.py"
    Start-Process -FilePath $PYTHON -ArgumentList $guiScript -WorkingDirectory $ROOT -WindowStyle Normal
    Write-Host "  Started desktop GUI" -ForegroundColor Green
}

# ================================================
# Summary
# ================================================
Write-Host ""
Write-Host "================================================" -ForegroundColor Green
Write-Host "   All services started!"                        -ForegroundColor Green
Write-Host "================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Dashboard : http://localhost:3000"             -ForegroundColor Cyan
Write-Host "  API docs  : http://localhost:8001/docs"        -ForegroundColor Cyan
Write-Host "  Redis     : redis://[::1]:6379"                -ForegroundColor Cyan
Write-Host "  Logs dir  : $ROOT\logs\"                       -ForegroundColor Cyan
Write-Host ""
Write-Host "  Status : powershell -ExecutionPolicy Bypass -File $ROOT\STATUS_NEXUS.ps1" -ForegroundColor DarkGray
Write-Host "  Stop   : powershell -ExecutionPolicy Bypass -File $ROOT\STOP_NEXUS.ps1"   -ForegroundColor DarkGray
Write-Host ""

# Auto-open dashboard in browser
Start-Process "http://localhost:3000/dashboard"
