@echo off
timeout /t 10 /nobreak >nul
taskkill /F /IM "DeepSeek Harness.exe" >nul 2>&1
timeout /t 3 /nobreak >nul
start "" "E:\DSH\DeepSeek Harness.exe"
