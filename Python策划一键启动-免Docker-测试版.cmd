@echo off
rem ASCII-ONLY FILE - do NOT put Chinese (or any non-ASCII) text in here, and do
rem NOT add `chcp`. cmd.exe re-reads the batch file after every line using the
rem CURRENT console code page; start.ps1 switches the console to UTF-8, which
rem shifts cmd's saved offset by one byte per multi-byte character and makes cmd
rem execute fragments of comment lines (2026-08-06 bug).
rem ============================================================
rem  Pandora backend - planner one-click start, PYTHON stack,
rem  NO DOCKER (TEST BUILD)   (double-click to run)
rem ------------------------------------------------------------
rem  Same as the regular planner no-docker start, except the 22 business
rem  services run the PYTHON implementation (python/pandorapy):
rem
rem    start.ps1 -Python -Mode local -NoDocker -DsLauncher editor -GenTables
rem
rem  Everything else is the SAME orchestration, not a copy:
rem    * infrastructure: native Windows processes under run\localinfra
rem      (MySQL dynamic port / Redis 6380 / Kafka 9093 / Envoy 8443)
rem    * DS: the editor-form DS (UnrealEditor.exe -server, uncooked assets)
rem    * tables: planner xlsx exported up-front (-GenTables)
rem    * the final green light still means "login + Envoy + Hub DS playable"
rem
rem  Python-stack specifics you should know:
rem    * social four (friend/chat/guild/mail) run on the LOCAL MySQL
rem      pandora_social (TiKV has no native Windows build), same as the go
rem      no-docker entry.
rem    * requires the venv at python\.venv (see python/README.md); the
rem      services are `python -m pandorapy.services.<name>.main` processes.
rem    * the central planner database (central-mysql.json bundle) is NOT
rem      supported by the python entry yet - it fails fast with a clear
rem      message instead of silently using local data.
rem
rem  Stop: the Python planner one-click STOP entry (same folder).
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
echo [planner-python] database=local-owned

rem This project requires PowerShell 7 (pwsh) and does NOT run on Windows
rem PowerShell 5.1. If the machine has no pwsh, bootstrap_pwsh.cmd unpacks the
rem official portable build under run\localinfra - no installer, no admin, no
rem change to the machine. Read that file for why it is not the .msi.
for /f "tokens=1-4 delims=:., " %%A in ("%TIME: =0%") do set /a "_PANDORA_BOOT_START_CS=(1%%A-100)*360000+(1%%B-100)*6000+(1%%C-100)*100+(1%%D-100)"
set "PANDORA_CMD_STARTED_CS=%_PANDORA_BOOT_START_CS%"
call "%~dp0tools\scripts\bootstrap_pwsh.cmd"
set "_PANDORA_BOOTSTRAP_RC=%ERRORLEVEL%"
for /f "tokens=1-4 delims=:., " %%A in ("%TIME: =0%") do set /a "_PANDORA_BOOT_END_CS=(1%%A-100)*360000+(1%%B-100)*6000+(1%%C-100)*100+(1%%D-100)"
set /a "_PANDORA_BOOT_ELAPSED_CS=_PANDORA_BOOT_END_CS-_PANDORA_BOOT_START_CS"
if %_PANDORA_BOOT_ELAPSED_CS% LSS 0 set /a "_PANDORA_BOOT_ELAPSED_CS+=8640000"
set /a "PANDORA_PWSH_BOOTSTRAP_MS=_PANDORA_BOOT_ELAPSED_CS*10"
if not "%_PANDORA_BOOTSTRAP_RC%"=="0" (
  echo [timing] powershell-bootstrap %PANDORA_PWSH_BOOTSTRAP_MS% ms failed
  echo [ERROR] PowerShell bootstrap failed. See the error above.
  if not defined PANDORA_NONINTERACTIVE pause >nul
  exit /b %_PANDORA_BOOTSTRAP_RC%
)
rem Quote it: with the portable build this is a full path, which can contain spaces.
set "PS=%PANDORA_PWSH%"
set "PANDORA_PLANNER_FAST_START=1"

"%PS%" -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\start.ps1" -Python -Mode local -NoDocker -DsLauncher editor -GenTables
set "RC=%ERRORLEVEL%"

rem Only RC=0 means start.ps1 proved login + Envoy + Hub DS playable.
if not "%RC%"=="0" (
  echo [ERROR] Pandora python stack is not playable yet. See the error above.
  if not defined PANDORA_NONINTERACTIVE pause >nul
  exit /b %RC%
)

rem Keep the successful window open only for interactive double-click runs.
if not defined PANDORA_NONINTERACTIVE pause
exit /b 0
