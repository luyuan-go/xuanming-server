@echo off
rem ASCII-ONLY FILE - do NOT put Chinese (or any non-ASCII) text in here, and do
rem NOT add `chcp`. cmd.exe re-reads the batch file after every line using the
rem CURRENT console code page; a multi-byte character shifts cmd's saved offset
rem and makes cmd execute fragments of comment lines (2026-08-06 bug).
rem ============================================================
rem  Pandora - make a PowerShell 7 interpreter available WITHOUT installing
rem  anything into Windows.
rem ------------------------------------------------------------
rem  Usage from a one-click entry, after its own `setlocal`:
rem
rem      call "%~dp0tools\scripts\bootstrap_pwsh.cmd"
rem      if errorlevel 1 ( ... bail out ... )
rem      "%PANDORA_PWSH%" -NoProfile -ExecutionPolicy Bypass -File ...
rem
rem  On success PANDORA_PWSH holds the interpreter to run - either the bare name
rem  `pwsh`, or the full path of the portable copy. In the portable case the
rem  folder is also prepended to the PATH of THIS process tree, because several
rem  of the project's .ps1 files re-enter themselves with a bare `& pwsh` and
rem  would otherwise die half-way through. The machine PATH is never touched.
rem
rem  Resolution order:
rem    1. `pwsh` already on PATH             -> use it, download nothing.
rem    2. run\localinfra\dist\pwsh\pwsh.exe  -> reuse only when its package
rem       marker matches the currently pinned archive SHA256.
rem    3. Official portable ZIP -> verify SHA256 -> unpack into (2). Sources for
rem       (3), in order: run\localinfra\cache, an explicit offline share in
rem       %PANDORA_LOCALINFRA_MIRROR% or the repository bundle at
rem       installers\localinfra, then github.com. The local repository bundle
rem       uses a run-owned hardlink snapshot; shares/network are cached. Every source uses the
rem       same pinned-hash rule as MySQL / Redis / Kafka / JRE / Envoy / mkcert.
rem
rem  Why the portable ZIP and not the .msi: the .msi is a per-machine install -
rem  it needs local admin, raises UAC, and would hang the headless web-admin
rem  runs (PANDORA_NONINTERACTIVE). The ZIP is the same official build: no
rem  admin, no registry, no service, no machine PATH change, and uninstall is
rem  "delete run\localinfra". Anyone who prefers a real install can still do it
rem  the normal way - step 1 then wins and nothing in here ever runs.
rem
rem  We deliberately do NOT fall back to Windows PowerShell 5.1. The project's
rem  scripts do not run on it, and a 5.1 fallback would turn a clear failure
rem  here into a confusing one several minutes later.
rem
rem  Pinned version / SHA256 / URL live in lib\pwsh_bootstrap.pin, shared with
rem  local_infra.ps1 so that `-Action provision` pre-stages the very same file.
rem ============================================================

set "PANDORA_PWSH="

rem ---- 1. already installed -------------------------------------------------
where pwsh >nul 2>nul
if not errorlevel 1 (
  set "PANDORA_PWSH=pwsh"
  goto :eof
)

rem %%~fI collapses the ..\.. so PATH and the messages stay readable.
for %%I in ("%~dp0..\..") do set "_PB_ROOT=%%~fI"
set "_PB_CACHE=%_PB_ROOT%\run\localinfra\cache"
set "_PB_DIST=%_PB_ROOT%\run\localinfra\dist\pwsh"
set "_PB_PUBLISH_LOCK=%_PB_DIST%.publish-lock"
set "_PB_MIRROR=%PANDORA_LOCALINFRA_MIRROR%"
set "_PB_MIRROR_KIND=mirror"
if not defined _PB_MIRROR (
  set "_PB_MIRROR=%_PB_ROOT%\installers\localinfra"
  set "_PB_MIRROR_KIND=bundle"
)

set "_PB_PIN=%~dp0lib\pwsh_bootstrap.pin"
if not exist "%_PB_PIN%" goto :err_pin
for /f "usebackq eol=# tokens=1,* delims==" %%A in ("%_PB_PIN%") do set "%%A=%%B"
if not defined PWSH_FILE goto :err_pin
if not defined PWSH_SHA256 goto :err_pin
if not defined PWSH_URL goto :err_pin

rem ---- 2. portable copy from an earlier run ---------------------------------
rem The executable alone is not proof that this dist belongs to the current
rem pin. A pin update must refresh an older portable copy automatically.
set "_PB_MARKER=%_PB_DIST%\.pandora-package.sha256"
if exist "%_PB_DIST%\pwsh.exe" if exist "%_PB_MARKER%" (
  call :marker_matches
  if not errorlevel 1 goto :use_portable
)
if exist "%_PB_DIST%\pwsh.exe" echo   [WARN] portable PowerShell is stale; refreshing it from the pinned archive.

rem ---- 3. fetch, verify, unpack ---------------------------------------------

rem The whole no-Docker stack (MySQL / Redis / Kafka / JRE / Envoy) is x64-only,
rem so there is nothing to gain from unpacking an arm64 / x86 pwsh here.
set "_PB_ARCH=%PROCESSOR_ARCHITECTURE%"
if defined PROCESSOR_ARCHITEW6432 set "_PB_ARCH=%PROCESSOR_ARCHITEW6432%"
if /i not "%_PB_ARCH%"=="AMD64" goto :err_arch

rem tar.exe and certutil.exe ship with Windows 10 1803+ / 11.
where tar.exe >nul 2>nul
if errorlevel 1 goto :err_tools
where certutil.exe >nul 2>nul
if errorlevel 1 goto :err_tools

if not exist "%_PB_CACHE%" mkdir "%_PB_CACHE%" >nul 2>nul
if not exist "%_PB_CACHE%\.pandora-runs" mkdir "%_PB_CACHE%\.pandora-runs" >nul 2>nul
set "_PB_CACHE_ZIP=%_PB_CACHE%\%PWSH_FILE%"
set "_PB_CACHE_LOCK=%_PB_CACHE_ZIP%.pandora-publish-lock"

rem Every run owns its download/snapshot directory. The same id also names its
rem dist stage and backup, and is rejected if EITHER name already exists.
set "_PB_RUN_TRIES=0"
:make_run
set /a _PB_RUN_TRIES+=1 >nul
set "_PB_RUN_ID=%RANDOM%-%RANDOM%-%RANDOM%"
set "_PB_RUN_DIR=%_PB_CACHE%\.pandora-runs\pwsh-%_PB_RUN_ID%"
set "_PB_STAGE=%_PB_DIST%.tmp-%_PB_RUN_ID%"
set "_PB_OLD=%_PB_DIST%.old-%_PB_RUN_ID%"
set "_PB_CACHE_PUBLISH=%_PB_CACHE%\.%PWSH_FILE%.publish-%_PB_RUN_ID%"
if exist "%_PB_RUN_DIR%" goto :run_collision
if exist "%_PB_STAGE%" goto :run_collision
if exist "%_PB_OLD%" goto :run_collision
if exist "%_PB_CACHE_PUBLISH%" goto :run_collision
mkdir "%_PB_RUN_DIR%" >nul 2>nul
if errorlevel 1 goto :run_collision
set "_PB_RUN_OWNED=1"
set "_PB_ZIP=%_PB_RUN_DIR%\%PWSH_FILE%"
goto :run_ready

:run_collision
if %_PB_RUN_TRIES% LSS 20 goto :make_run
goto :err_unpack

:run_ready

set "_PB_SRC=cache"
if exist "%_PB_CACHE_ZIP%" (
  call :snapshot_archive "%_PB_CACHE_ZIP%"
  if errorlevel 1 goto :retry_bad_cache
  goto :verify
)

:try_offline
rem Repository bundle or explicit offline share. An empty directory / missing
rem exact file falls through to the network. A same-name file with a bad hash is
rem a hard stop: silently routing around a damaged bundle would hide the issue.
if not exist "%_PB_MIRROR%\%PWSH_FILE%" goto :fetch_net
if /i "%_PB_MIRROR_KIND%"=="bundle" goto :use_bundle
set "_PB_SRC=%_PB_MIRROR_KIND%"
echo   [pwsh] snapshotting the pinned archive from the selected offline source ^(source: %_PB_SRC%^)
call :snapshot_archive "%_PB_MIRROR%\%PWSH_FILE%"
if errorlevel 1 goto :err_copy
goto :verify

:use_bundle
rem Pin the repository pathname to a run-owned snapshot BEFORE hashing. A
rem same-volume hardlink costs no extra archive bytes and survives atomic SVN
rem replacement of the source name; cross-volume filesystems fall back to copy.
set "_PB_SRC=bundle"
echo   [pwsh] snapshotting the pinned archive from the repository bundle ^(source: bundle^)
call :snapshot_archive "%_PB_MIRROR%\%PWSH_FILE%"
if errorlevel 1 goto :err_copy
goto :verify

:fetch_net
set "_PB_SRC=net"
where curl.exe >nul 2>nul
if errorlevel 1 goto :err_tools
echo.
echo   [pwsh] PowerShell 7 is not installed on this machine.
echo   [pwsh] Fetching the official portable build once (about 100 MB):
echo   [pwsh]   %PWSH_URL%
echo   [pwsh] Nothing gets installed into Windows - it is unpacked into
echo   [pwsh]   run\localinfra\dist\pwsh   (delete that folder to undo).
echo   [pwsh] No network here? Update the repository bundle, or set
echo   [pwsh] PANDORA_LOCALINFRA_MIRROR to an offline share.
echo.
rem Download to this run's unique .part. Parallel launchers never append to,
rem move, or delete one another's partial download.
curl.exe -fSL --retry 3 --retry-delay 2 -o "%_PB_ZIP%.part" "%PWSH_URL%"
if errorlevel 1 goto :err_download
move /y "%_PB_ZIP%.part" "%_PB_ZIP%" >nul
if errorlevel 1 goto :err_download

:verify
rem Verify the run-owned snapshot, then unpack that exact same pathname.
call :hash_file "%_PB_ZIP%"
if /i "%_PB_GOT%"=="%PWSH_SHA256%" goto :hash_ok
if /i "%_PB_SRC%"=="cache" goto :retry_bad_cache
goto :err_hash

:hash_ok
if /i "%_PB_SRC%"=="mirror" (
  call :publish_cache
  if errorlevel 1 goto :err_cache_publish
)
if /i "%_PB_SRC%"=="net" (
  call :publish_cache
  if errorlevel 1 goto :err_cache_publish
)

rem Unpack into the already collision-checked unique stage and rename, so a
rem half-unpacked folder is never mistaken for current by the next run.
if exist "%_PB_STAGE%" goto :err_unpack
if exist "%_PB_OLD%" goto :err_unpack
mkdir "%_PB_STAGE%" >nul 2>nul
if errorlevel 1 goto :err_unpack
set "_PB_STAGE_OWNED=1"
echo   [pwsh] unpacking PowerShell %PWSH_VERSION% into run\localinfra\dist\pwsh
tar.exe -x -f "%_PB_ZIP%" -C "%_PB_STAGE%"
if errorlevel 1 goto :err_unpack
rem A hardlink also observes hostile in-place writes. Re-hash after extraction so
rem even that non-SVN mutation cannot publish a stage built from changing bytes.
call :hash_file "%_PB_ZIP%"
if /i not "%_PB_GOT%"=="%PWSH_SHA256%" goto :err_snapshot_changed
if not exist "%_PB_STAGE%\pwsh.exe" goto :err_layout
> "%_PB_STAGE%\.pandora-package.sha256" echo %PWSH_SHA256%
if errorlevel 1 goto :err_unpack

rem A second double-click may have published the same pin while we extracted.
rem Serialize only the millisecond-scale recheck/swap window. Extraction stays
rem outside the lock, so a second double-click does not wait for ZIP I/O.
set "_PB_LOCK_TRIES=0"
:acquire_publish_lock
mkdir "%_PB_PUBLISH_LOCK%" >nul 2>nul
if not errorlevel 1 goto :publish_lock_acquired
set /a _PB_LOCK_TRIES+=1 >nul
if %_PB_LOCK_TRIES% GEQ 5 goto :err_lock
ping.exe 127.0.0.1 -n 2 -w 1000 >nul
goto :acquire_publish_lock

:publish_lock_acquired
set "_PB_LOCK_OWNED=1"
rem Re-check while holding the publish lock; equivalent current content wins.
if exist "%_PB_DIST%\pwsh.exe" if exist "%_PB_MARKER%" (
  call :marker_matches
  if not errorlevel 1 goto :peer_ready
)
if exist "%_PB_DIST%" move "%_PB_DIST%" "%_PB_OLD%" >nul
if exist "%_PB_DIST%" goto :err_swap
rem REN is an atomic same-parent name change and fails if a peer already created
rem `pwsh`; MOVE would instead nest our stage inside that existing directory.
ren "%_PB_STAGE%" "pwsh" >nul
if errorlevel 1 goto :err_promote
set "_PB_STAGE_OWNED="
call :release_publish_lock
if errorlevel 1 goto :err_release_lock
if exist "%_PB_OLD%" rd /s /q "%_PB_OLD%" >nul 2>nul
echo   [pwsh] ready.
goto :use_portable

:peer_ready
call :release_publish_lock
if errorlevel 1 goto :err_release_lock
if defined _PB_STAGE_OWNED rd /s /q "%_PB_STAGE%" >nul 2>nul
set "_PB_STAGE_OWNED="
echo   [pwsh] another launcher already published the pinned portable build.

:use_portable
set "PANDORA_PWSH=%_PB_DIST%\pwsh.exe"
set "PATH=%_PB_DIST%;%PATH%"
goto :done

rem ---- failures -------------------------------------------------------------
:err_pin
echo.
echo  [ERR] missing or unreadable tools\scripts\lib\pwsh_bootstrap.pin
echo        The working copy is incomplete - update the repository.
goto :fail

:err_arch
echo.
echo  [ERR] this machine is not AMD64; the no-Docker stack is x64 only.
echo        Install PowerShell 7 yourself: https://aka.ms/powershell
goto :fail

:err_tools
echo.
echo  [ERR] need curl.exe / tar.exe / certutil.exe (Windows 10 1803+ ships all
echo        three). Update Windows, or install PowerShell 7 yourself:
echo        https://aka.ms/powershell
goto :fail

:err_copy
echo.
echo  [ERR] could not snapshot the pinned archive from the selected offline source
goto :fail

:err_download
echo.
echo  [ERR] could not download %PWSH_FILE%
echo        %PWSH_URL%
echo        Check the network / proxy, update the repository bundle, or set
echo        PANDORA_LOCALINFRA_MIRROR to an offline share.
del /f /q "%_PB_ZIP%.part" >nul 2>nul
goto :fail

:retry_bad_cache
echo   [WARN] cached %PWSH_FILE% failed SHA256; removing it and trying the offline source.
del /f /q "%_PB_ZIP%" >nul 2>nul
call :remove_bad_cache
set "_PB_GOT="
goto :try_offline

:err_hash
echo.
echo  [ERR] SHA256 mismatch for the pinned archive   (source: %_PB_SRC%)
echo          expected %PWSH_SHA256%
echo          actual   %_PB_GOT%
if /i "%_PB_SRC%"=="mirror" echo        The explicit offline mirror is wrong. Ask the backend team to check it.
if /i "%_PB_SRC%"=="bundle" echo        The repository bundle is damaged. Run svn update or ask the backend team.
if /i not "%_PB_SRC%"=="bundle" del /f /q "%_PB_ZIP%" >nul 2>nul
goto :fail

:err_snapshot_changed
echo.
echo  [ERR] the verified %PWSH_FILE% snapshot changed while it was unpacked.
echo        Refusing to publish files produced from changing archive bytes.
if defined _PB_STAGE_OWNED rd /s /q "%_PB_STAGE%" >nul 2>nul
goto :fail

:err_cache_publish
echo.
echo  [ERR] could not safely publish %PWSH_FILE% into run\localinfra\cache.
echo        Close other launchers and retry. If none are open, inspect only
echo        the matching .pandora-publish-lock directory before removing it.
goto :fail

:err_unpack
echo.
echo  [ERR] could not unpack %PWSH_FILE% into run\localinfra\dist\pwsh
echo        Check free disk space (needs about 250 MB) and that no antivirus is
echo        holding the folder.
if defined _PB_STAGE_OWNED rd /s /q "%_PB_STAGE%" >nul 2>nul
goto :fail

:err_swap
if exist "%_PB_DIST%\pwsh.exe" if exist "%_PB_MARKER%" (
  call :marker_matches
  if not errorlevel 1 goto :promote_peer_ready
)
echo.
echo  [ERR] could not replace the stale portable PowerShell directory.
echo        Close processes using run\localinfra\dist\pwsh and try again.
call :release_publish_lock
if defined _PB_STAGE_OWNED rd /s /q "%_PB_STAGE%" >nul 2>nul
goto :fail

:err_promote
rem If a peer won the target path with the same pin, use it and discard only
rem this run's stale backup. Otherwise restore this run's backup when possible.
if exist "%_PB_DIST%\pwsh.exe" if exist "%_PB_MARKER%" (
  call :marker_matches
  if not errorlevel 1 goto :promote_peer_ready
)
if not exist "%_PB_DIST%" if exist "%_PB_OLD%" ren "%_PB_OLD%" "pwsh" >nul
call :release_publish_lock
echo.
echo  [ERR] could not promote the verified portable PowerShell directory.
echo        The previous directory was restored when possible. Any unrecovered
echo        backup is preserved under a unique .old-* name; retry the launch.
if defined _PB_STAGE_OWNED rd /s /q "%_PB_STAGE%" >nul 2>nul
goto :fail

:promote_peer_ready
call :release_publish_lock
if errorlevel 1 goto :err_release_lock
if exist "%_PB_OLD%" rd /s /q "%_PB_OLD%" >nul 2>nul
if defined _PB_STAGE_OWNED rd /s /q "%_PB_STAGE%" >nul 2>nul
goto :use_portable

:err_lock
echo.
echo  [ERR] could not acquire the portable PowerShell publish lock.
echo        Close other launchers and retry. If none are open, remove only
echo        run\localinfra\dist\pwsh.publish-lock, then retry. The launcher
echo        never guesses that an ownerless fixed lock is stale.
if defined _PB_STAGE_OWNED rd /s /q "%_PB_STAGE%" >nul 2>nul
goto :fail

:err_release_lock
echo.
echo  [ERR] portable PowerShell was prepared, but its publish lock could not be released.
echo        Close other launchers. If none are open, remove only
echo        run\localinfra\dist\pwsh.publish-lock, then retry.
if defined _PB_STAGE_OWNED rd /s /q "%_PB_STAGE%" >nul 2>nul
goto :fail

:err_layout
echo.
echo  [ERR] %PWSH_FILE% unpacked without a pwsh.exe at its root - the upstream
echo        archive layout changed. Tell the backend team; do not bypass this.
rd /s /q "%_PB_STAGE%" >nul 2>nul
goto :fail

:snapshot_archive
rem Pin the source name to this run. Hardlink is zero-copy on the same volume;
rem COPY is the cross-volume/filesystem fallback. Hashing happens afterwards.
if exist "%_PB_ZIP%" del /f /q "%_PB_ZIP%" >nul 2>nul
mklink /h "%_PB_ZIP%" "%~1" >nul 2>nul
if exist "%_PB_ZIP%" exit /b 0
copy /y "%~1" "%_PB_ZIP%" >nul 2>nul
if exist "%_PB_ZIP%" exit /b 0
exit /b 1

:hash_file
rem findstr selects the pure hex line so localized headers cannot shift it.
set "_PB_GOT="
for /f "delims=" %%H in ('certutil -hashfile "%~1" SHA256 ^| findstr /r /i "^[0-9a-f][0-9a-f]*$"') do if not defined _PB_GOT set "_PB_GOT=%%H"
set "_PB_GOT=%_PB_GOT: =%"
exit /b 0

:acquire_cache_lock
set "_PB_CACHE_LOCK_TRIES=0"
:acquire_cache_lock_retry
mkdir "%_PB_CACHE_LOCK%" >nul 2>nul
if not errorlevel 1 goto :cache_lock_acquired
set /a _PB_CACHE_LOCK_TRIES+=1 >nul
if %_PB_CACHE_LOCK_TRIES% GEQ 5 exit /b 1
ping.exe 127.0.0.1 -n 2 -w 1000 >nul
goto :acquire_cache_lock_retry
:cache_lock_acquired
set "_PB_CACHE_LOCK_OWNED=1"
exit /b 0

:release_cache_lock
if not defined _PB_CACHE_LOCK_OWNED exit /b 0
rmdir "%_PB_CACHE_LOCK%" >nul 2>nul
if exist "%_PB_CACHE_LOCK%" exit /b 1
set "_PB_CACHE_LOCK_OWNED="
exit /b 0

:remove_bad_cache
rem Removal is opportunistic and serialized. Never delete a fixed name while a
rem cooperating peer is publishing it, and re-hash after the lock is acquired.
call :acquire_cache_lock
if errorlevel 1 exit /b 0
if not exist "%_PB_CACHE_ZIP%" goto :remove_bad_cache_done
call :hash_file "%_PB_CACHE_ZIP%"
if /i "%_PB_GOT%"=="%PWSH_SHA256%" goto :remove_bad_cache_done
del /f /q "%_PB_CACHE_ZIP%" >nul 2>nul
:remove_bad_cache_done
call :release_cache_lock
exit /b 0

:publish_cache
rem Publish a verified snapshot via a unique same-directory name and a short
rem fixed-name lock. A peer that already published the same hash wins cleanly.
call :hash_file "%_PB_ZIP%"
if /i not "%_PB_GOT%"=="%PWSH_SHA256%" exit /b 1
if exist "%_PB_CACHE_PUBLISH%" del /f /q "%_PB_CACHE_PUBLISH%" >nul 2>nul
mklink /h "%_PB_CACHE_PUBLISH%" "%_PB_ZIP%" >nul 2>nul
if exist "%_PB_CACHE_PUBLISH%" goto :publish_cache_candidate_ready
copy /y "%_PB_ZIP%" "%_PB_CACHE_PUBLISH%" >nul 2>nul
if not exist "%_PB_CACHE_PUBLISH%" exit /b 1
:publish_cache_candidate_ready
call :hash_file "%_PB_CACHE_PUBLISH%"
if /i not "%_PB_GOT%"=="%PWSH_SHA256%" goto :publish_cache_failed
call :acquire_cache_lock
if errorlevel 1 goto :publish_cache_failed
if not exist "%_PB_CACHE_ZIP%" goto :publish_cache_replace
call :hash_file "%_PB_CACHE_ZIP%"
if /i "%_PB_GOT%"=="%PWSH_SHA256%" goto :publish_cache_peer_ready
:publish_cache_replace
move /y "%_PB_CACHE_PUBLISH%" "%_PB_CACHE_ZIP%" >nul 2>nul
if errorlevel 1 goto :publish_cache_failed_locked
call :hash_file "%_PB_CACHE_ZIP%"
if /i not "%_PB_GOT%"=="%PWSH_SHA256%" goto :publish_cache_failed_locked
:publish_cache_peer_ready
call :release_cache_lock
if errorlevel 1 goto :publish_cache_failed
del /f /q "%_PB_CACHE_PUBLISH%" >nul 2>nul
exit /b 0
:publish_cache_failed_locked
call :release_cache_lock
:publish_cache_failed
del /f /q "%_PB_CACHE_PUBLISH%" >nul 2>nul
exit /b 1

:marker_matches
for %%S in ("%_PB_MARKER%") do if not "%%~zS"=="66" exit /b 1
findstr /x /i /c:"%PWSH_SHA256%" "%_PB_MARKER%" >nul 2>nul
exit /b %ERRORLEVEL%

:release_publish_lock
if not defined _PB_LOCK_OWNED exit /b 0
rmdir "%_PB_PUBLISH_LOCK%" >nul 2>nul
if exist "%_PB_PUBLISH_LOCK%" exit /b 1
set "_PB_LOCK_OWNED="
exit /b 0

:fail
set "PANDORA_PWSH="
call :cleanup
exit /b 1

:done
call :cleanup
goto :eof

:cleanup
if defined _PB_LOCK_OWNED rmdir "%_PB_PUBLISH_LOCK%" >nul 2>nul
if defined _PB_CACHE_LOCK_OWNED rmdir "%_PB_CACHE_LOCK%" >nul 2>nul
if defined _PB_RUN_OWNED if exist "%_PB_RUN_DIR%" rd /s /q "%_PB_RUN_DIR%" >nul 2>nul
if defined _PB_CACHE_PUBLISH del /f /q "%_PB_CACHE_PUBLISH%" >nul 2>nul
set "_PB_ROOT="
set "_PB_CACHE="
set "_PB_CACHE_ZIP="
set "_PB_CACHE_LOCK="
set "_PB_CACHE_LOCK_TRIES="
set "_PB_CACHE_LOCK_OWNED="
set "_PB_CACHE_PUBLISH="
set "_PB_DIST="
set "_PB_PUBLISH_LOCK="
set "_PB_RUN_ID="
set "_PB_RUN_DIR="
set "_PB_RUN_TRIES="
set "_PB_RUN_OWNED="
set "_PB_STAGE="
set "_PB_OLD="
set "_PB_STAGE_OWNED="
set "_PB_LOCK_TRIES="
set "_PB_LOCK_OWNED="
set "_PB_MIRROR="
set "_PB_MIRROR_KIND="
set "_PB_PIN="
set "_PB_MARKER="
set "_PB_ARCH="
set "_PB_ZIP="
set "_PB_SRC="
set "_PB_GOT="
set "PWSH_VERSION="
set "PWSH_FILE="
set "PWSH_SHA256="
set "PWSH_URL="
goto :eof
