@echo off
rem ASCII-ONLY FILE - do NOT put Chinese (or any non-ASCII) text in here, and do
rem NOT add `chcp`. cmd.exe re-reads the batch file after every line using the
rem CURRENT console code page; start.ps1 switches the console to UTF-8, which
rem shifts cmd's saved offset by one byte per multi-byte character and makes cmd
rem execute fragments of comment lines (2026-08-06 bug).
rem ============================================================
rem  Pandora backend - planner one-click start, NO DOCKER (TEST BUILD)
rem  (double-click to run)
rem ------------------------------------------------------------
rem  This is a TEST entry point. It does exactly what the regular
rem  "live asset" one-click start does, EXCEPT that the infrastructure
rem  (MySQL / Redis / Kafka / Envoy) runs as plain native Windows
rem  processes instead of Docker containers:
rem
rem    start.ps1 -Mode local -NoDocker -DsLauncher editor -GenTables
rem
rem  Why: planner machines cannot reasonably run Docker Desktop (WSL2,
rem  admin rights, slow boot, huge disk usage). The no-Docker path uses
rem  portable binaries unpacked under run\localinfra\ - nothing is
rem  installed into Windows, nothing is registered as a service, and
rem  removing that folder removes everything.
rem
rem  SQL uses the remote planner workspace when central-mysql.json is present.
rem  A machine that has never enrolled remotely uses its exact-owned local
rem  MySQL; once central state exists, failures never fall back to local data.
rem  Runtime config copies stay under run\localinfra; tracked YAML is unchanged:
rem    MySQL local-owned:auto(13307..13398) or central-managed
rem    Redis 127.0.0.1:6380   Kafka 127.0.0.1:9093
rem    Envoy :8443 (client) / 127.0.0.1:8444 (DS)
rem
rem  Differences you should know about:
rem    * The current central enrollment contract supports Oracle MySQL only;
rem      do not point it at TiDB. TiDB needs its own validated adapter.
rem    * Prometheus / Grafana / Loki are NOT started (planners do not
rem      use them; saves ~1 GB of RAM).
rem    * The local Envoy is v1.28.0 (the last official Windows build).
rem      Only ONE field of the production envoy.yaml is unsupported and
rem      it is stripped explicitly; anything else fails the start on
rem      purpose. This binary is for 127.0.0.1 development only - the
rem      intranet / k8s / production edge keeps using v1.38.
rem
rem  First run downloads roughly 400 MB of portable binaries. Put them on
rem  a share and set PANDORA_LOCALINFRA_MIRROR to that folder to skip the
rem  download on every other machine.
rem
rem  What a planner machine has to install: NOTHING. Go is not needed
rem  (prebuilt exes under run\artifacts\windows\bin are used), Docker is not
rem  needed, and mkcert plus - since 2026-08-18 - PowerShell 7 itself are
rem  fetched into run\localinfra\dist\ on first run. Both are only added to
rem  the PATH of the running script, never to the system PATH, so a machine
rem  that already has pwsh installed keeps using its own.
rem
rem  Stop: pwsh tools\scripts\start.ps1 -Mode local -NoDocker -Down
rem ============================================================
setlocal
cd /d "%~dp0"
echo [planner] launcher=%~f0
set "PANDORA_PLANNER_REQUIRE_CENTRAL_MYSQL="
if exist "%~dp0installers\planner-db\central-mysql.json" (
  set "PANDORA_PLANNER_REQUIRE_CENTRAL_MYSQL=1"
  echo [planner] database=central-managed
) else (
  echo [planner] database=local-owned ^(remote bundle not installed^)
)

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
  rem The web admin runs this headless; pausing there would hang it forever.
  rem Keep an interactive failure window visible, but suppress the standard
  rem "Press any key" success-looking prompt.
  echo [ERROR] PowerShell bootstrap failed. See the error above.
  if not defined PANDORA_NONINTERACTIVE pause >nul
  exit /b %_PANDORA_BOOTSTRAP_RC%
)
rem Quote it: with the portable build this is a full path, which can contain spaces.
set "PS=%PANDORA_PWSH%"
set "PANDORA_PLANNER_FAST_START=1"

"%PS%" -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\start.ps1" -Mode local -NoDocker -DsLauncher editor -GenTables
set "RC=%ERRORLEVEL%"

rem A failed start stays visible without printing cmd.exe's standard success-like
rem prompt. Only RC=0 means start.ps1 proved login + Envoy + Hub DS playable.
if not "%RC%"=="0" (
  echo [ERROR] Pandora is not playable yet. See the error above.
  if not defined PANDORA_NONINTERACTIVE pause >nul
  exit /b %RC%
)

rem Keep the successful window open only for interactive double-click runs.
rem start.ps1 has already printed the explicit playable message immediately
rem before returning RC=0. The web admin is headless and must never block.
if not defined PANDORA_NONINTERACTIVE pause
exit /b 0
