# Whole-PC Telegram session scanner (AKBARGAY.py).
# Requires API_ID and API_HASH set at the top of that file.
$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $RepoRoot

Write-Host "Installing dependencies (telethon, rarfile)..." -ForegroundColor Cyan
python -m pip install -q telethon rarfile

Write-Host "Starting full-disk scan + audit..." -ForegroundColor Cyan
python .\AKBARGAY.py
