@echo off
setlocal
cd /d "%~dp0.."
echo Installing dependencies (telethon, rarfile)...
python -m pip install -q telethon rarfile
echo Starting full-disk scan + audit...
python 12495623230.py
endlocal
