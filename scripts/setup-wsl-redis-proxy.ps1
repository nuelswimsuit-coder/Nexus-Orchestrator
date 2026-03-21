# Nexus: forward Windows :6379 -> WSL Redis (firewall step needs Administrator)
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator)

netsh interface portproxy reset
if ($isAdmin) {
    Remove-NetFirewallRule -DisplayName "Nexus Redis" -ErrorAction SilentlyContinue
    New-NetFirewallRule -DisplayName "Nexus Redis" -Direction Inbound -LocalPort 6379 -Protocol TCP -Action Allow
} else {
    Write-Warning "Not running as Administrator: skipped firewall rule. Re-run elevated for inbound access from LAN."
}

$wsl_ip = (wsl -u root hostname -I).Trim().Split(" ")[0]
if (-not $wsl_ip) {
    Write-Error "Could not resolve WSL IP (is WSL running?)."
    exit 1
}
netsh interface portproxy add v4tov4 listenport=6379 listenaddress=0.0.0.0 connectport=6379 connectaddress=$wsl_ip

Write-Host "--- SUCCESS ---" -ForegroundColor Green
Write-Host "Port proxy: 0.0.0.0:6379 -> WSL $wsl_ip:6379"
