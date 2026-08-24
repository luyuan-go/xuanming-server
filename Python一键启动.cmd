@echo off
chcp 65001 >nul
rem ============================================================
rem  Pandora backend one-click launcher -- PYTHON stack
rem  (double-click to run)
rem ------------------------------------------------------------
rem  Brings up, in order:
rem     1. docker infrastructure (MySQL / Redis / Kafka / etcd / Envoy / TiDB)
rem     2. database schema upgrade (only missing migrations, idempotent)
rem     3. all 22 Pandora services, PYTHON implementation
rem
rem  This is the Python counterpart of start.cmd (which runs the Go stack).
rem  Steps 1 and 2 reuse the very same dev_up.ps1 / dev_migrate.ps1 as the Go
rem  path -- both stacks share one MySQL, one Redis and one schema, so they
rem  must never diverge.
rem
rem  Readiness rule: a service counts as up only when its port is listening
rem  AND its log printed service_ready. "Process alive" alone is not enough --
rem  a service that bound its port and is now exiting would look healthy.
rem
rem  Requires a packaged Windows DS at
rem     F:\work\Packages\Server_Win64_Development\WindowsServer\PandoraServer.exe
rem  (hub_allocator / ds_allocator run mode=local and exec it). Build it with
rem  the client repo's Tool\Build\Package_Server_Win64_Dev.bat.
rem
rem  CLI usage (args forwarded to dev_all_python.ps1):
rem     this script -SkipInfra          rem services only, skip docker+migrate
rem     this script -Exclude inventory  rem leave one service for debugging
rem     this script -LocalOnly          rem bind client face to loopback only
rem     this script -Pull               rem pull latest images first
rem
rem  By default the Envoy client face 8443 is opened to the LAN so a packaged
rem  client can reach this machine by its intranet IP. The unauthenticated DS
rem  face 8444 and the admin port 9901 always stay on loopback.
rem ============================================================
setlocal
cd /d "%~dp0"

rem Prefer PowerShell 7 (pwsh), fall back to Windows PowerShell if missing
where pwsh >nul 2>nul && (set "PS=pwsh") || (set "PS=powershell")

%PS% -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\dev_all_python.ps1" %*
set "RC=%ERRORLEVEL%"

rem When double-clicked (no args) keep the window open to read the ready table
if "%~1"=="" pause
exit /b %RC%
