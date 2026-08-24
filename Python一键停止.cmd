@echo off
chcp 65001 >nul
rem ============================================================
rem  Pandora backend one-click stop -- PYTHON stack
rem  (double-click to run)
rem ------------------------------------------------------------
rem  Stops, in order:
rem     1. all Pandora Python service processes
rem        (only `python -m pandorapy.services.*`; other python.exe untouched)
rem     2. the docker infrastructure containers
rem
rem  CLI usage (args forwarded to dev_all_python.ps1 -Down):
rem     this script -SkipInfra           rem stop services, keep docker up
rem     this script -Exclude inventory   rem stop all but that one
rem
rem  It ALSO kills local Windows DS processes (PandoraServer.exe), hub + battle.
rem  Normally an allocator kills the DS it spawned when it exits, but a one-click
rem  stop is a hard kill (Stop-Process -Force) -- the allocator never gets to clean
rem  up, so the DS is orphaned and keeps holding UDP 7777 / 7800+. The next start
rem  then collides on those ports. Pass -KeepDs to leave DS running.
rem  UnrealEditor and the game client are never touched.
rem ============================================================
setlocal
cd /d "%~dp0"

rem Prefer PowerShell 7 (pwsh), fall back to Windows PowerShell if missing
where pwsh >nul 2>nul && (set "PS=pwsh") || (set "PS=powershell")

%PS% -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\dev_all_python.ps1" -Down %*
set "RC=%ERRORLEVEL%"

rem When double-clicked (no args) keep the window open to read output
if "%~1"=="" pause
exit /b %RC%
