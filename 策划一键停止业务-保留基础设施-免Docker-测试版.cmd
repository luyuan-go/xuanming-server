@echo off
rem ASCII-ONLY FILE - cmd.exe may re-read batch files with the active console
rem code page, so non-ASCII bytes can corrupt execution.
rem ============================================================
rem  Pandora backend - planner STOP BUSINESS, KEEP INFRA (TEST)
rem ------------------------------------------------------------
rem  Stops all registered Go services and their local Hub/Battle DS
rem  child processes.
rem
rem  Does NOT stop or start MySQL / Redis / Kafka / Envoy.
rem  Use the existing full-stop CMD when those processes must stop too.
rem ============================================================
setlocal
cd /d "%~dp0"

echo [planner] action=stop-business
echo [planner] keep-infrastructure=mysql,redis,kafka,envoy

rem Reuse the same portable PowerShell 7 bootstrap as the start/full-stop
rem entries. It does not install software or change the machine PATH.
call "%~dp0tools\scripts\bootstrap_pwsh.cmd"
if errorlevel 1 (
  if not defined PANDORA_NONINTERACTIVE pause
  exit /b 1
)
set "PS=%PANDORA_PWSH%"

rem run_services owns the exact PID files and stops allocator-owned local DS
rem before stopping the allocator. It never manages infrastructure processes.
"%PS%" -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\run_services.ps1" -Action down
set "RC=%ERRORLEVEL%"

if "%RC%"=="0" (
  echo.
  echo [planner] Business services and local DS stopped.
  echo [planner] MySQL / Redis / Kafka / Envoy were not stopped.
  echo [planner] The next one-click start can reuse components that are still running.
) else (
  echo.
  echo [ERROR] Some business services or local DS could not be stopped.
  echo [planner] Infrastructure was not stopped by this entry.
)

if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
