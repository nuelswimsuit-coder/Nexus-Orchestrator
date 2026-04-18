# NEXUS SUPREME - Install dependencies
# Run: powershell -ExecutionPolicy Bypass -File "C:\Users\Yarin\Desktop\Nexus-Orchestrator\INSTALL_NEXUS.ps1"

$ROOT   = "C:\Users\Yarin\Desktop\Nexus-Orchestrator"
$PYTHON = "python"

Set-Location $ROOT
$env:PYTHONUTF8 = "1"

Write-Host "================================================" -ForegroundColor Cyan
Write-Host "   NEXUS SUPREME - Installing dependencies"      -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""

# --- Python check ---
Write-Host "[1/4] Checking Python..." -ForegroundColor Yellow
& $PYTHON --version
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Python not found. Download from: https://python.org" -ForegroundColor Red
    exit 1
}
Write-Host "  OK - Python found" -ForegroundColor Green

# --- Core requirements ---
Write-Host "[2/4] Installing core requirements (requirements.txt)..." -ForegroundColor Yellow
$coreReq = Join-Path $ROOT "requirements.txt"
if (Test-Path $coreReq) {
    & $PYTHON -m pip install -r $coreReq --quiet
    Write-Host "  OK - requirements.txt" -ForegroundColor Green
} else {
    Write-Host "  SKIP - requirements.txt not found" -ForegroundColor DarkGray
}

# --- Nexus Supreme requirements ---
Write-Host "[3/4] Installing Nexus Supreme requirements..." -ForegroundColor Yellow
$supReq = Join-Path $ROOT "requirements_supreme.txt"
& $PYTHON -m pip install -r $supReq --quiet
Write-Host "  OK - requirements_supreme.txt" -ForegroundColor Green

# --- Optional extras ---
Write-Host "[4/4] Installing optional extras..." -ForegroundColor Yellow

& $PYTHON -m pip install Pillow   --quiet
Write-Host "  OK - Pillow (image processing)" -ForegroundColor Green

& $PYTHON -m pip install watchdog --quiet
Write-Host "  OK - watchdog (hot-reload)" -ForegroundColor Green

& $PYTHON -m pip install psutil   --quiet
Write-Host "  OK - psutil (system monitor)" -ForegroundColor Green

& $PYTHON -m pip install structlog --quiet
Write-Host "  OK - structlog (logging)" -ForegroundColor Green

Write-Host ""
Write-Host "================================================" -ForegroundColor Green
Write-Host "   All dependencies installed successfully!"     -ForegroundColor Green
Write-Host "================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Next step:" -ForegroundColor Cyan
Write-Host "  powershell -ExecutionPolicy Bypass -File C:\Users\Yarin\Desktop\Nexus-Orchestrator\START_NEXUS.ps1" -ForegroundColor White
Write-Host ""
