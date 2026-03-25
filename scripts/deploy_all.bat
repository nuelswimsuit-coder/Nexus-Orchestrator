@echo off
REM Deploy cluster worker to a single host (IP). POSTs to Master Hub deploy API.
REM Usage: deploy_all.bat <IPv4|IPv6>
REM Env: NEXUS_MASTER_HUB_URL (default http://127.0.0.1:8001)

setlocal
cd /d "%~dp0\.."
if "%~1"=="" exit /b 2
python "%~dp0deploy_cluster_target.py" "%~1"
exit /b %ERRORLEVEL%
