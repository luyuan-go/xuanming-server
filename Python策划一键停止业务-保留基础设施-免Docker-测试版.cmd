@echo off
rem ASCII-ONLY FILE - do NOT put Chinese (or any non-ASCII) text in here, and do
rem NOT add `chcp`. cmd.exe re-reads the batch file after every line using the
rem CURRENT console code page; start.ps1 switches the console to UTF-8, which
rem shifts cmd's saved offset by one byte per multi-byte character and makes cmd
rem execute fragments of comment lines (2026-08-06 bug).
rem ============================================================
rem  Pandora backend - planner STOP BUSINESS, KEEP INFRA,
rem  PYTHON stack, NO DOCKER (TEST BUILD)
rem ------------------------------------------------------------
rem  Stops the 22 PYTHON services (only `python -m pandorapy.services.*`)
rem  and their local editor-form DS child processes.
rem
rem  Does NOT stop or start MySQL / Redis / Kafka / Envoy.
rem  Use the Python full-stop CMD when those processes must stop too.
rem ============================================================
setlocal
cd /d "%~dp0"

echo [planner-python] action=stop-business
echo [planner-python] keep-infrastructure=mysql,redis,kafka,envoy

rem This project requires PowerShell 7 (pwsh) and does NOT run on Windows
rem PowerShell 5.1. If the machine has no pwsh, bootstrap_pwsh.cmd unpacks the
rem official portable build under run\localinfra - no installer, no admin, no
rem change to the machine. Read that file for why it is not the .msi.
call "%~dp0tools\scripts\bootstrap_pwsh.cmd"
if errorlevel 1 (
  if not defined PANDORA_NONINTERACTIVE pause
  exit /b 1
)
rem Quote it: with the portable build this is a full path, which can contain spaces.
set "PS=%PANDORA_PWSH%"

rem dev_all_python.ps1 -Down -SkipInfra stops the python services + local DS
rem and never touches the infrastructure processes. -NoDocker keeps the
rem scoped stop matching aligned with the no-docker start (social four run
rem the -dev.yaml MySQL configs there).
"%PS%" -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\dev_all_python.ps1" -Down -SkipInfra -NoDocker
set "RC=%ERRORLEVEL%"

if "%RC%"=="0" (
  echo.
  echo [planner-python] Python services and local DS stopped.
  echo [planner-python] MySQL / Redis / Kafka / Envoy were not stopped.
) else (
  echo.
  echo [ERROR] Some python services or local DS could not be stopped.
  echo [planner-python] Infrastructure was not stopped by this entry.
)

if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
