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
rem  Accounts and DB schema are identical to the Docker path. MySQL uses a
rem  verified private port selected from 13307..13398, so an existing Docker
rem  MySQL on 3307 is never reused, migrated, or stopped. Runtime config copies
rem  are generated under run\localinfra; tracked service YAML stays unchanged:
rem    MySQL 127.0.0.1:auto(13307..13398)   Redis 127.0.0.1:6380
rem    Kafka 127.0.0.1:9093   Envoy :8443 (client) / 127.0.0.1:8444 (DS)
rem
rem  Differences you should know about:
rem    * TiDB is NOT started (TiKV has no usable native Windows build).
rem      friend / chat / guild / mail connect to the local MySQL
rem      pandora_social database instead. Same schema, same code path.
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

rem This project requires PowerShell 7 (pwsh) and does NOT run on Windows
rem PowerShell 5.1. If the machine has no pwsh, bootstrap_pwsh.cmd unpacks the
rem official portable build under run\localinfra - no installer, no admin, no
rem change to the machine. Read that file for why it is not the .msi.
call "%~dp0tools\scripts\bootstrap_pwsh.cmd"
if errorlevel 1 (
  rem The web admin runs this headless; pausing there would hang it forever.
  rem Keep an interactive failure window visible, but suppress the standard
  rem "Press any key" success-looking prompt.
  echo [ERROR] PowerShell bootstrap failed. See the error above.
  if not defined PANDORA_NONINTERACTIVE pause >nul
  exit /b 1
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
