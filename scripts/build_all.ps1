#Requires -Version 5.1
$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Push-Location -LiteralPath $RepoRoot
try {
    python -m PyInstaller --onefile --name NEXUS_MASTER scripts/nexus_launcher.py
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
