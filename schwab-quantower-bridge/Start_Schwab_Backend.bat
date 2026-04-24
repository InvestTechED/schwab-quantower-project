@echo off
setlocal
cd /d "D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge"
python -m uvicorn --app-dir backend app.main:app
