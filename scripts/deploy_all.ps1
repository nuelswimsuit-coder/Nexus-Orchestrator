#Requires -Version 5.1
<#
.SYNOPSIS
  Scan local IPv4 subnet, ping hosts, push TeleFix_Setup.exe via admin share, run silent install.

.NOTES
  For authorized administration on networks you own. Requires admin credentials valid on targets
  (SMB to C$ + remote WMI/CIM DCOM). Targets often need File and Printer Sharing + WMI rules.
#>
param(
    [string] $InstallerPath = "",
    [string] $MasterIp = "",
    [int] $PingTimeoutMs = 750,
    [switch] $IncludeLocalHost
)

$ErrorActionPreference = "Stop"

function Get-PrimaryIPv4Subnet {
    $nic = Get-NetIPConfiguration -ErrorAction SilentlyContinue |
        Where-Object { $_.IPv4DefaultGateway -and $_.NetAdapter.Status -eq "Up" } |
        Select-Object -First 1
    if (-not $nic) {
        throw "Could not find an active interface with an IPv4 default gateway."
    }
    $addr = @($nic.IPv4Address)[0]
    return @{
        IPAddress      = $addr.IPAddress
        PrefixLength   = $addr.PrefixLength
        InterfaceAlias = $nic.InterfaceAlias
    }
}

function Get-IPv4HostAddresses {
    param([string] $IPAddress, [int] $PrefixLength)
    $ip = [System.Net.IPAddress]::Parse($IPAddress)
    $bytes = $ip.GetAddressBytes()
    [Array]::Reverse($bytes)
    $addrInt = [System.BitConverter]::ToUInt32($bytes, 0)
    $hostBits = 32 - $PrefixLength
    if ($hostBits -le 0 -or $hostBits -gt 30) {
        throw "Prefix /$PrefixLength is not supported (need a LAN-sized subnet, e.g. /24)."
    }
    $mask = [uint32]([math]::Pow(2, 32) - [math]::Pow(2, $hostBits))
    $network = $addrInt -band $mask
    $broadcast = $network + [uint32]([math]::Pow(2, $hostBits) - 1)
    $first = $network + 1
    $last = $broadcast - 1
    $list = New-Object System.Collections.Generic.List[string]
    for ($n = $first; $n -le $last; $n++) {
        $b = [System.BitConverter]::GetBytes([uint32]$n)
        [Array]::Reverse($b)
        $list.Add(([System.Net.IPAddress]$b).IPAddressToString)
    }
    return $list
}

function Copy-InstallerToRemoteTemp {
    param(
        [string] $TargetIp,
        [string] $LocalInstaller,
        [System.Management.Automation.PSCredential] $Credential
    )
    $driveName = "TF" + ([guid]::NewGuid().ToString("N").Substring(0, 6))
    $root = "\\$TargetIp\c$"
    try {
        New-PSDrive -Name $driveName -PSProvider FileSystem -Root $root -Credential $Credential -Scope Script -ErrorAction Stop | Out-Null
        $dest = "${driveName}:\Windows\Temp\TeleFix_Setup.exe"
        Copy-Item -LiteralPath $LocalInstaller -Destination $dest -Force -ErrorAction Stop
    }
    finally {
        if (Get-PSDrive -Name $driveName -ErrorAction SilentlyContinue) {
            Remove-PSDrive -Name $driveName -Force -ErrorAction SilentlyContinue
        }
    }
}

function Test-IPv4Alive {
    param([string] $Ip, [int] $TimeoutMs)
    $p = New-Object System.Net.NetworkInformation.Ping
    try {
        $r = $p.Send($Ip, $TimeoutMs)
        return ($r.Status -eq [System.Net.NetworkInformation.IPStatus]::Success)
    }
    catch {
        return $false
    }
    finally {
        $p.Dispose()
    }
}

function Start-RemoteSilentSetup {
    param(
        [string] $TargetIp,
        [string] $Master,
        [System.Management.Automation.PSCredential] $Credential
    )
    if ($Master -notmatch '^\d{1,3}(\.\d{1,3}){3}$') {
        throw "MasterIp must be dotted IPv4 (got: $Master)"
    }
    # Runs on target; %COMPUTERNAME% expands in cmd on the remote host.
    $cmdLine = "cmd.exe /c `"C:\Windows\Temp\TeleFix_Setup.exe /S --master_ip $Master --worker_name %COMPUTERNAME%`""
    $opt = New-CimSessionOption -Protocol Dcom
    $sess = $null
    try {
        $sess = New-CimSession -ComputerName $TargetIp -Credential $Credential -SessionOption $opt -ErrorAction Stop
        $result = Invoke-CimMethod -CimSession $sess -ClassName Win32_Process -MethodName Create -Arguments @{
            CommandLine = $cmdLine
        } -ErrorAction Stop
        if ($result.ReturnValue -ne 0) {
            throw "Win32_Process.Create ReturnValue=$($result.ReturnValue)"
        }
    }
    finally {
        if ($sess) { Remove-CimSession $sess -ErrorAction SilentlyContinue }
    }
}

# --- main ---
Write-Host "========================================"  -ForegroundColor Cyan
Write-Host "  TELEFIX SUBNET DEPLOY (authorized use) " -ForegroundColor Cyan
Write-Host "========================================"  -ForegroundColor Cyan

$repoRoot = Split-Path -Parent $PSScriptRoot
if (-not $InstallerPath) {
    $InstallerPath = Join-Path $repoRoot "TeleFix_Setup.exe"
}
$InstallerPath = (Resolve-Path -LiteralPath $InstallerPath -ErrorAction Stop).Path

if (-not $MasterIp) {
    $sub = Get-PrimaryIPv4Subnet
    $MasterIp = $sub.IPAddress
    Write-Host "[INFO] Master IP not passed; using this machine: $MasterIp" -ForegroundColor DarkGray
}

Write-Host "[INFO] Installer: $InstallerPath" -ForegroundColor DarkGray
Write-Host "[INFO] Master IP for workers: $MasterIp" -ForegroundColor DarkGray

$cred = Get-Credential -Message "Enter credentials for remote Windows machines (e.g. DOMAIN\Admin or .\Administrator)"
if (-not $cred) {
    Write-Host "[ABORT] No credentials provided." -ForegroundColor Red
    exit 1
}

$sub = Get-PrimaryIPv4Subnet
Write-Host "[SCAN] Interface $($sub.InterfaceAlias) — $($sub.IPAddress)/$($sub.PrefixLength)" -ForegroundColor Yellow

$hosts = Get-IPv4HostAddresses -IPAddress $sub.IPAddress -PrefixLength $sub.PrefixLength
if (-not $IncludeLocalHost) {
    $hosts = $hosts | Where-Object { $_ -ne $sub.IPAddress }
}

$deployed = New-Object System.Collections.Generic.List[string]
$failed = New-Object System.Collections.Generic.List[string]

foreach ($ip in $hosts) {
    if (-not (Test-IPv4Alive -Ip $ip -TimeoutMs $PingTimeoutMs)) { continue }

    Write-Host "[FOUND] $ip — deploying..." -ForegroundColor Green
    try {
        Copy-InstallerToRemoteTemp -TargetIp $ip -LocalInstaller $InstallerPath -Credential $cred
        Start-RemoteSilentSetup -TargetIp $ip -Master $MasterIp -Credential $cred
        $deployed.Add($ip) | Out-Null
        Write-Host "        OK (copy + remote start)" -ForegroundColor DarkGreen
    }
    catch {
        $failed.Add("$ip — $($_.Exception.Message)") | Out-Null
        Write-Host "        FAIL: $($_.Exception.Message)" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "========== DEPLOYED ($($deployed.Count)) ==========" -ForegroundColor Green
if ($deployed.Count -eq 0) { Write-Host "(none)" -ForegroundColor DarkGray }
else { $deployed | ForEach-Object { Write-Host "  $_" } }

Write-Host ""
Write-Host "========== FAILED ($($failed.Count)) ==========" -ForegroundColor Red
if ($failed.Count -eq 0) { Write-Host "(none)" -ForegroundColor DarkGray }
else { $failed | ForEach-Object { Write-Host "  $_" } }

Write-Host ""
Write-Host "[DONE] Sweep finished. Confirm workers on the dashboard." -ForegroundColor Cyan
