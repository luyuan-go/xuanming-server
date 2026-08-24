@echo off
rem ============================================================
rem  Pandora backend - intranet one-click START (k8s cluster), PYTHON stack
rem  (double-click to run)
rem ------------------------------------------------------------
rem  ASCII-ONLY FILE - do NOT put Chinese (or any non-ASCII) text in here, and
rem  do NOT add `chcp` (see the go k8s entry for why).
rem
rem  Same real Kubernetes dev cluster as the go k8s entry - minikube + Agones,
rem  infra, the REAL Linux DS on an Agones Fleet, edge Envoy - but the 22
rem  business services run the PYTHON implementation:
rem
rem    tools/scripts/start.ps1 -Python -Mode k8s -GenTables
rem
rem  What -Python changes (and ONLY this):
rem    * builds pandora-py/<svc>:dev images (deploy/services/Dockerfile.python,
rem      context=python/, one shared image parameterized by SERVICE_MODULE)
rem    * loads those into minikube instead of the go images
rem    * applies deploy/k8s/overlays/python (image-swap overlay over the same
rem      base manifests - Deployment/Service names, ports, config mounts all
rem      identical)
rem  Everything else - infra, Agones + the UE Linux DS Fleet, Envoy, config
rem  Secret + configtable ConfigMap, tidb-init - is the same code path as go.
rem  Social four (friend/chat/guild/mail) use the in-cluster TiDB (tidb:4000),
rem  same as the go k8s path since 2026-08-24.
rem
rem  Prereqs: same as the go k8s entry (docker/kubectl/minikube; the go
rem  toolchain and the local python venv are NOT needed - the image build
rem  runs inside docker).
rem
rem  Stop: the Python intranet one-click stop (k8s) entry.
rem ============================================================
setlocal
cd /d "%~dp0"

rem This project requires PowerShell 7 (pwsh). If missing, error out clearly; do
rem not fall back to Windows PowerShell 5.1.
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

%PS% -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\start.ps1" -Python -Mode k8s -GenTables
set "RC=%ERRORLEVEL%"

if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
