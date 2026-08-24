@echo off
rem ASCII-ONLY FILE - do NOT put Chinese (or any non-ASCII) text in here, and do
rem NOT add `chcp`. cmd.exe re-reads the batch file after every line using the
rem CURRENT console code page; start.ps1 switches the console to UTF-8, which
rem shifts cmd's saved offset by one byte per multi-byte character and makes cmd
rem execute fragments of comment lines (2026-08-06 bug).
rem ============================================================
rem  Pandora backend - planner one-click STOP, PYTHON stack,
rem  NO DOCKER (TEST BUILD)
rem ------------------------------------------------------------
rem  Stops the 22 PYTHON services (only `python -m pandorapy.services.*`;
rem  other python.exe untouched), the local editor-form DS, and the native
rem  infrastructure processes (MySQL / Redis / Kafka / Envoy) started by the
rem  Python no-docker start entry. Data under run\localinfra\data is KEPT.
rem
rem  To wipe the local databases as well:
rem    pwsh tools\scripts\local_infra.ps1 -Action reset
rem ============================================================
setlocal
cd /d "%~dp0"

call "%~dp0tools\scripts\bootstrap_pwsh.cmd"
if errorlevel 1 (
  if not defined PANDORA_NONINTERACTIVE pause
  exit /b 1
)
rem Quote it: with the portable build this is a full path, which can contain spaces.
set "PS=%PANDORA_PWSH%"

"%PS%" -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\start.ps1" -Python -Mode local -NoDocker -Down
set "RC=%ERRORLEVEL%"

if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
