@echo off
rem ============================================================
rem  Pandora backend - intranet one-click STOP (k8s cluster), PYTHON stack
rem  (double-click to run)
rem ------------------------------------------------------------
rem  ASCII-ONLY FILE - do NOT put Chinese (or any non-ASCII) text in here.
rem
rem  Wraps: tools/scripts/start.ps1 -Python -Mode k8s -Down
rem
rem  The k8s Down path deletes business Deployments BY NAME (rendered from the
rem  base manifests) and the overlay never renames them, so this stops the
rem  python-image Deployments exactly like the go entry stops the go ones.
rem  Infra teardown behavior is identical to the go k8s stop entry.
rem ============================================================
setlocal
cd /d "%~dp0"

where pwsh >nul 2>nul
if errorlevel 1 (
  echo.
  echo  [ERR] PowerShell 7 pwsh not found. This script requires PowerShell 7.
  echo        Install: https://aka.ms/powershell  or  winget install Microsoft.PowerShell
  echo.
  pause
  exit /b 1
)
set "PS=pwsh"

%PS% -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\start.ps1" -Python -Mode k8s -Down
set "RC=%ERRORLEVEL%"

if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
