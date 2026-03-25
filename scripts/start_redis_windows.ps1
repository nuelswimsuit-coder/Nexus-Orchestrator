# Start Redis on Windows when Docker is unavailable.
# Order: already listening on 6379 -> Docker -> redis-server on PATH -> hints.
# Run from repo root:  powershell -ExecutionPolicy Bypass -File scripts/start_redis_windows.ps1

$ErrorActionPreference = "Continue"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$Port = 6379

function Test-PortOpen {
    param([int]$P)
    try {
        $c = New-Object System.Net.Sockets.TcpClient
        $c.Connect("127.0.0.1", $P)
        $c.Close()
        return $true
    } catch {
        return $false
    }
}

if (Test-PortOpen -P $Port) {
    Write-Host "Redis already accepting connections on 127.0.0.1:$Port - nothing to do."
    exit 0
}

# Docker (Docker Desktop must be running; engine ready)
$dockerCmd = Get-Command docker -ErrorAction SilentlyContinue
if ($dockerCmd) {
    docker version 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Starting Redis via Docker (redis:7-alpine) ..."
        docker start nexus-redis 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) {
            Start-Sleep -Seconds 1
            if (Test-PortOpen -P $Port) {
                Write-Host "OK (existing container nexus-redis). REDIS_URL=redis://127.0.0.1:$Port/0"
                exit 0
            }
        }
        docker run -d -p "${Port}:6379" --name nexus-redis redis:7-alpine
        if ($LASTEXITCODE -eq 0) {
            Write-Host "OK. REDIS_URL=redis://127.0.0.1:$Port/0"
            exit 0
        }
        Write-Host "Docker run failed. If the name is taken: docker rm -f nexus-redis"
    }
}

# Native Redis for Windows (e.g. chocolatey: choco install redis-64)
$redisServer = Get-Command redis-server -ErrorAction SilentlyContinue
$conf = Join-Path $RepoRoot "redis-local\redis.windows.conf"
if ($redisServer -and (Test-Path $conf)) {
    Write-Host "Starting redis-server with $conf ..."
    Start-Process -FilePath $redisServer.Source -ArgumentList "`"$conf`"" -WindowStyle Minimized
    Start-Sleep -Seconds 2
    if (Test-PortOpen -P $Port) {
        Write-Host "OK. REDIS_URL=redis://127.0.0.1:$Port/0"
        exit 0
    }
}

Write-Host ""
Write-Host "Could not start Redis automatically. Pick one:"
Write-Host '  1) Install and start Docker Desktop, wait until it says Running, then re-run this script.'
Write-Host '  2) Install Redis for Windows (e.g. choco install redis-64) and add redis-server to PATH, then re-run.'
Write-Host '  3) Install Memurai (Redis-compatible) from https://www.memurai.com/ and set REDIS_URL in .env.'
Write-Host '  4) Use WSL: wsl -d Ubuntu -- redis-server (or your distro) and point REDIS_URL at the WSL IP if needed.'
Write-Host ""
exit 1
