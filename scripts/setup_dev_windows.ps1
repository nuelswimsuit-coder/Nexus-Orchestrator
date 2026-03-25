# Nexus Orchestrator — Windows dev install (always uses .venv, never Store Python by accident)
# Run from repo root:  powershell -ExecutionPolicy Bypass -File scripts/setup_dev_windows.ps1

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $RepoRoot

$VenvPy = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$VenvPip = Join-Path $RepoRoot ".venv\Scripts\pip.exe"

if (-not (Test-Path $VenvPy)) {
    Write-Host "Creating .venv ..."
    & py -3 -m venv .venv
    if (-not (Test-Path $VenvPy)) {
        & python -m venv .venv
    }
}
if (-not (Test-Path $VenvPy)) {
    throw "Could not create .venv or find $VenvPy"
}

Write-Host "Using: $VenvPy"
& $VenvPy -m pip install --upgrade pip
& $VenvPy -m pip install -e ".[dev]"

Write-Host ""
Write-Host "Done. Activate this venv in new terminals:"
Write-Host "  .\.venv\Scripts\Activate.ps1"
Write-Host ""
Write-Host "CLI entrypoints (after activate):"
Write-Host "  nexus-master   nexus-worker   nexus-api   nexus-telegram"
Write-Host "Or without PATH:"
Write-Host "  .\.venv\Scripts\python.exe scripts\start_master.py"
Write-Host ""
