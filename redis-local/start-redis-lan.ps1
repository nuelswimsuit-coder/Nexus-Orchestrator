# One-shot: stop bundled Redis and start with LAN bind (0.0.0.0 via redis.windows-lan.conf).
$here = $PSScriptRoot
$exe = Join-Path $here "redis-server.exe"
$conf = Join-Path $here "redis.windows-lan.conf"
if (-not (Test-Path $exe)) { Write-Error "Missing $exe"; exit 1 }
if (-not (Test-Path $conf)) { Write-Error "Missing $conf"; exit 1 }
Stop-Process -Name redis-server -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 400
Start-Process -FilePath $exe -ArgumentList "`"$conf`"" -WorkingDirectory $here -WindowStyle Minimized
Start-Sleep -Seconds 1
$cl = (Get-CimInstance Win32_Process -Filter "Name='redis-server.exe'" | Select-Object -First 1).CommandLine
Write-Host "CommandLine: $cl"
if ($cl -match 'redis\.windows-lan\.conf') { Write-Host "OK: LAN config is active." } else { Write-Warning "Config path not visible on CommandLine; check process manually." }
