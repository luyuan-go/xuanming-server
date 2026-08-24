@echo off
rem ASCII-ONLY FILE - do NOT put Chinese (or any non-ASCII) text in here, and do
rem NOT add `chcp`. cmd.exe re-reads the batch file after every line using the
rem CURRENT console code page; start.ps1 switches the console to UTF-8, which
rem shifts cmd's saved offset by one byte per multi-byte character and makes cmd
rem execute fragments of comment lines (2026-08-06 bug).
rem ============================================================
rem  Pandora backend - planner one-click: restart the local editor-form DS,
rem  PYTHON stack, NO DOCKER (TEST BUILD)   (double-click to run)
rem ------------------------------------------------------------
rem  Python counterpart of the regular planner "restart DS" entry:
rem
rem    start.ps1 -Python -Mode local -NoDocker -DsLauncher editor -GenTables -DsOnly
rem
rem  KNOWN DIFFERENCE from the go entry: the go -DsOnly fast path (restart the
rem  DS + only the table-reading services) is go-stack machinery
rem  (run_services.ps1), so with -Python this entry falls back to the FULL
rem  python start. That is still correct and idempotent - the infrastructure
rem  is left running, tables are re-exported, all python services restart
rem  (which reloads the tables), and the DS is killed and relaunched - it is
rem  just slower than the go fast path. A python-native fast path can be added
rem  later without changing this entry.
rem
rem  If the backend is not running yet, this simply IS the full start.
rem ============================================================
setlocal
cd /d "%~dp0"
echo [planner-python] launcher=%~f0
if exist "%~dp0installers\planner-db\central-mysql.json" (
  echo [ERROR] central-managed database bundle detected. The PYTHON no-docker
  echo         entry does not support the central planner database yet.
  echo         Use the regular go entry, or remove the enrollment bundle.
  if not defined PANDORA_NONINTERACTIVE pause >nul
  exit /b 3
)

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
set "PANDORA_PLANNER_FAST_START=1"

"%PS%" -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\start.ps1" -Python -Mode local -NoDocker -DsLauncher editor -GenTables -DsOnly
set "RC=%ERRORLEVEL%"

if not "%RC%"=="0" (
  echo [ERROR] Pandora python stack is not playable yet. See the error above.
  if not defined PANDORA_NONINTERACTIVE pause >nul
  exit /b %RC%
)

if not defined PANDORA_NONINTERACTIVE pause
exit /b 0
