<#
.SYNOPSIS
  One-time Windows ↔ WSL Redis bridge: PortProxy 6379 + inbound firewall.

.DESCRIPTION
  Finds the default WSL distro IPv4, forwards Windows 0.0.0.0:6379 → WSL:6379,
  and ensures an inbound TCP 6379 firewall allow rule is enabled so Linux
  workers / other hosts can reach Redis running inside WSL.

  Run once from an elevated PowerShell (Run as Administrator), then set worker
  REDIS_URL to redis://<this-PC-LAN-IP>:6379 (or your public IP if applicable).

.NOTES
  Project: REDIS-NETWORK-BRIDGE — Yaakov Hatan / Nexus Orchestrator
#>

#Requires -RunAsAdministrator

$ErrorActionPreference = "Stop"

$ListenPort = 6379
$ListenAddress = "0.0.0.0"
$RuleDisplayName = "Nexus Redis Bridge — TCP 6379 (WSL)"

function Get-WslIpv4 {
    $null = & wsl.exe -e true 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "WSL is not available or failed to start. Install WSL and a distro, then retry."
    }
    $raw = (& wsl.exe -e sh -c "hostname -I" 2>$null).Trim()
    if ([string]::IsNullOrWhiteSpace($raw)) {
        throw "Could not read WSL IPv4 (hostname -I empty). Is your default distro running?"
    }
    $first = ($raw -split '\s+', 0, "RemoveEmptyEntries")[0]
    if ($first -notmatch '^\d{1,3}(\.\d{1,3}){3}$') {
        throw "WSL IP did not look like IPv4: '$raw'"
    }
    return $first
}

Write-Host "Nexus Redis network bridge (WSL PortProxy + firewall)" -ForegroundColor Cyan

$wslIp = Get-WslIpv4
Write-Host "WSL IPv4: $wslIp" -ForegroundColor Green

# Remove stale proxy on same listen endpoint (ignore if missing)
$null = & netsh.exe interface portproxy delete v4tov4 `
    listenport=$ListenPort listenaddress=$ListenAddress 2>&1

$addArgs = @(
    "interface", "portproxy", "add", "v4tov4",
    "listenport=$ListenPort",
    "listenaddress=$ListenAddress",
    "connectport=$ListenPort",
    "connectaddress=$wslIp"
)
& netsh.exe @addArgs
if ($LASTEXITCODE -ne 0) {
    throw "netsh portproxy add failed (exit $LASTEXITCODE)."
}
Write-Host "PortProxy: $ListenAddress`:$ListenPort -> ${wslIp}:$ListenPort" -ForegroundColor Green

# Inbound firewall: create if missing, always enable
$rule = Get-NetFirewallRule -DisplayName $RuleDisplayName -ErrorAction SilentlyContinue |
    Where-Object { $_.Direction -eq 'Inbound' }
if (-not $rule) {
    New-NetFirewallRule -DisplayName $RuleDisplayName `
        -Direction Inbound `
        -Action Allow `
        -Protocol TCP `
        -LocalPort $ListenPort `
        -Profile Any `
        | Out-Null
    Write-Host "Created firewall rule: $RuleDisplayName" -ForegroundColor Green
} else {
    Enable-NetFirewallRule -DisplayName $RuleDisplayName
    Write-Host "Firewall rule active: $RuleDisplayName" -ForegroundColor Green
}

Write-Host ""
Write-Host "Current v4tov4 port proxies (excerpt):" -ForegroundColor Cyan
& netsh.exe interface portproxy show v4tov4
Write-Host ""
Write-Host "Done. Workers can use redis://<Windows-LAN-or-public-IP>:$ListenPort" -ForegroundColor Green
Write-Host "If WSL gets a new IP after reboot, run this script again." -ForegroundColor Yellow
