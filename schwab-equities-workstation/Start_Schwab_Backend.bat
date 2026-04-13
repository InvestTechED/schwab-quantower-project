@echo off
setlocal
cd /d "D:\GitHub\Claude Code\apps\schwab-equities-workstation\backend"
python -m uvicorn app.main:app --reload
