<#
.SYNOPSIS
  Windows 正式二进制必须从可追溯的干净源码整批发布。

.DESCRIPTION
  在临时仓库中通过脚本 CLI 观察退出码与落盘结果。测试使用 git/go 系统边界替身，
  不执行真实编译，也不读写 run/artifacts 正式目录。

.EXAMPLE
  pwsh tools/scripts/tests/release_artifact_publish_contract_test.ps1
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
$SourceBuildScript = Join-Path $ProjectRoot 'tools/scripts/build_release_binaries.ps1'
$script:Failures = [System.Collections.Generic.List[string]]::new()
$ExpectedBinaryNames = @(
    'auction.exe', 'battle_result.exe', 'chat.exe', 'configtable-gen.exe',
    'data_service.exe', 'dialogue.exe', 'ds_allocator.exe', 'friend.exe',
    'guild.exe', 'hub_allocator.exe', 'inventory.exe', 'leaderboard.exe',
    'login.exe', 'mail.exe', 'matchmaker.exe', 'matchmaker_pve.exe',
    'mission.exe', 'owner.exe', 'pandora-migrate.exe', 'player.exe',
    'player_locator.exe', 'push.exe', 'team.exe', 'trade.exe'
)

function Assert-True([bool]$Condition, [string]$Message) {
    if ($Condition) {
        Write-Host "  [ ok ] $Message" -ForegroundColor DarkGray
    } else {
        $script:Failures.Add($Message)
        Write-Host "  [FAIL] $Message" -ForegroundColor Red
    }
}

function New-TestRepository {
    param([Parameter(Mandatory)][string]$Root)

    $scriptDir = Join-Path $Root 'tools/scripts'
    $stubDir = Join-Path $Root 'test-bin'
    New-Item -ItemType Directory -Force -Path $scriptDir, $stubDir | Out-Null
    Copy-Item -LiteralPath $SourceBuildScript -Destination (Join-Path $scriptDir 'build_release_binaries.ps1')
    @(
        'services/runtime/player_locator', 'services/battle/hub_allocator',
        'services/account/player', 'services/battle/ds_allocator', 'services/runtime/push',
        'services/matchmaking/team', 'services/social/friend', 'services/social/chat',
        'services/social/guild', 'services/social/mail', 'services/social/dialogue',
        'services/social/mission', 'services/data/data_service', 'services/economy/trade',
        'services/economy/inventory', 'services/runtime/leaderboard', 'services/runtime/owner',
        'services/economy/auction', 'services/battle/battle_result',
        'services/matchmaking/matchmaker', 'services/account/login',
        'tools/configtable-gen', 'tools/migrate'
    ) | ForEach-Object {
        New-Item -ItemType Directory -Force -Path (Join-Path $Root $_) | Out-Null
    }

    $runServicesStub = @'
[IO.File]::AppendAllText($env:PANDORA_TEST_RUN_SERVICES_LOG, (($args -join ' ') + [Environment]::NewLine))
'@
    [IO.File]::WriteAllText(
        (Join-Path $scriptDir 'run_services.ps1'),
        $runServicesStub,
        [Text.UTF8Encoding]::new($false))

    $gitStub = @'
@echo off
if /I "%~3"=="rev-parse" (
  if defined PANDORA_TEST_GIT_REVISION (
    echo %PANDORA_TEST_GIT_REVISION%
  ) else (
    echo 0123456789abcdef0123456789abcdef01234567
  )
  exit /b 0
)
if /I "%~3"=="status" (
  if defined PANDORA_TEST_DIRTY_AFTER_MARKER (
    if exist "%PANDORA_TEST_DIRTY_AFTER_MARKER%" (
      echo  M changed-during-build.go
    ) else (
      type nul > "%PANDORA_TEST_DIRTY_AFTER_MARKER%"
    )
    exit /b 0
  )
  if defined PANDORA_TEST_GIT_STATUS echo %PANDORA_TEST_GIT_STATUS%
  exit /b 0
)
exit /b 2
'@
    [IO.File]::WriteAllText(
        (Join-Path $stubDir 'git.cmd'),
        $gitStub,
        [Text.ASCIIEncoding]::new())

    $goStub = @'
@echo off
if /I "%~1"=="version" if /I "%~2"=="-m" goto metadata
if /I "%~1"=="version" goto print_version
if /I not "%~1"=="build" exit /b 2
if exist ".pandora-test-go-fail" exit /b 23
set "HAS_BUILDVCS=0"
shift
:find_output
if "%~1"=="" exit /b 3
if /I "%~1"=="-buildvcs=true" set "HAS_BUILDVCS=1"
rem PowerShell 调用 .cmd 替身时会把 name=value 暴露为相邻的 name/value 参数；
rem 真正的 go.exe 仍收到单个 -buildvcs=true。替身同时接受两种形态，避免夹具误报。
if /I "%~1"=="-buildvcs" if /I "%~2"=="true" set "HAS_BUILDVCS=1"
if /I "%~1"=="-o" goto output_found
shift
goto find_output
:output_found
if defined PANDORA_TEST_REQUIRE_BUILD_IDENTITY if not "%HAS_BUILDVCS%"=="1" exit /b 31
if defined PANDORA_TEST_REQUIRE_BUILD_IDENTITY if /I not "%GOOS%"=="windows" exit /b 32
if defined PANDORA_TEST_REQUIRE_BUILD_IDENTITY if /I not "%GOARCH%"=="amd64" exit /b 33
if defined PANDORA_TEST_REQUIRE_BUILD_IDENTITY if /I not "%CGO_ENABLED%"=="0" exit /b 34
shift
set "OUT=%~1"
for %%F in ("%OUT%") do if not exist "%%~dpF" mkdir "%%~dpF"
> "%OUT%" echo binary:%OUT%
if exist ".pandora-test-go-extra" > "%~dp1stale-extra.exe" echo stale-extra
exit /b 0
:print_version
echo go version go1.26.0 windows/amd64
exit /b 0
:metadata
if /I "%PANDORA_TEST_GO_METADATA_MODE%"=="missing-vcs" goto metadata_missing_vcs
if /I "%PANDORA_TEST_GO_METADATA_MODE%"=="wrong-revision" goto metadata_wrong_revision
if /I "%PANDORA_TEST_GO_METADATA_MODE%"=="modified" goto metadata_modified
if /I "%PANDORA_TEST_GO_METADATA_MODE%"=="wrong-goos" goto metadata_wrong_goos
if /I "%PANDORA_TEST_GO_METADATA_MODE%"=="wrong-goarch" goto metadata_wrong_goarch
if /I "%PANDORA_TEST_GO_METADATA_MODE%"=="cgo-enabled" goto metadata_cgo_enabled
:metadata_good
echo {"GoVersion":"go1.26.0","Path":"pandora/test","Settings":[{"Key":"vcs","Value":"git"},{"Key":"vcs.revision","Value":"0123456789abcdef0123456789abcdef01234567"},{"Key":"vcs.modified","Value":"false"},{"Key":"GOOS","Value":"windows"},{"Key":"GOARCH","Value":"amd64"},{"Key":"CGO_ENABLED","Value":"0"}]}
exit /b 0
:metadata_missing_vcs
echo {"GoVersion":"go1.26.0","Path":"pandora/test","Settings":[{"Key":"GOOS","Value":"windows"},{"Key":"GOARCH","Value":"amd64"},{"Key":"CGO_ENABLED","Value":"0"}]}
exit /b 0
:metadata_wrong_revision
echo {"GoVersion":"go1.26.0","Path":"pandora/test","Settings":[{"Key":"vcs","Value":"git"},{"Key":"vcs.revision","Value":"ffffffffffffffffffffffffffffffffffffffff"},{"Key":"vcs.modified","Value":"false"},{"Key":"GOOS","Value":"windows"},{"Key":"GOARCH","Value":"amd64"},{"Key":"CGO_ENABLED","Value":"0"}]}
exit /b 0
:metadata_modified
echo {"GoVersion":"go1.26.0","Path":"pandora/test","Settings":[{"Key":"vcs","Value":"git"},{"Key":"vcs.revision","Value":"0123456789abcdef0123456789abcdef01234567"},{"Key":"vcs.modified","Value":"true"},{"Key":"GOOS","Value":"windows"},{"Key":"GOARCH","Value":"amd64"},{"Key":"CGO_ENABLED","Value":"0"}]}
exit /b 0
:metadata_wrong_goos
echo {"GoVersion":"go1.26.0","Path":"pandora/test","Settings":[{"Key":"vcs","Value":"git"},{"Key":"vcs.revision","Value":"0123456789abcdef0123456789abcdef01234567"},{"Key":"vcs.modified","Value":"false"},{"Key":"GOOS","Value":"linux"},{"Key":"GOARCH","Value":"amd64"},{"Key":"CGO_ENABLED","Value":"0"}]}
exit /b 0
:metadata_wrong_goarch
echo {"GoVersion":"go1.26.0","Path":"pandora/test","Settings":[{"Key":"vcs","Value":"git"},{"Key":"vcs.revision","Value":"0123456789abcdef0123456789abcdef01234567"},{"Key":"vcs.modified","Value":"false"},{"Key":"GOOS","Value":"windows"},{"Key":"GOARCH","Value":"arm64"},{"Key":"CGO_ENABLED","Value":"0"}]}
exit /b 0
:metadata_cgo_enabled
echo {"GoVersion":"go1.26.0","Path":"pandora/test","Settings":[{"Key":"vcs","Value":"git"},{"Key":"vcs.revision","Value":"0123456789abcdef0123456789abcdef01234567"},{"Key":"vcs.modified","Value":"false"},{"Key":"GOOS","Value":"windows"},{"Key":"GOARCH","Value":"amd64"},{"Key":"CGO_ENABLED","Value":"1"}]}
exit /b 0
'@
    [IO.File]::WriteAllText(
        (Join-Path $stubDir 'go.cmd'),
        $goStub,
        [Text.ASCIIEncoding]::new())

    return [pscustomobject]@{
        Root = $Root
        BuildScript = Join-Path $scriptDir 'build_release_binaries.ps1'
        StubDir = $stubDir
        RunServicesLog = Join-Path $Root 'run-services.log'
    }
}

function Invoke-TestBuild {
    param(
        [Parameter(Mandatory)]$Repository,
        [string]$GitStatus = '',
        [string]$GitRevision = '0123456789abcdef0123456789abcdef01234567',
        [string]$FailBinaryName = '',
        [string]$ExtraBinaryModule = '',
        [switch]$DirtyAfterBuild,
        [string]$GoMetadataMode = 'good',
        [switch]$RequireBuildIdentity,
        [string[]]$Arguments = @()
    )

    $oldPath = $env:PATH
    $oldStatus = $env:PANDORA_TEST_GIT_STATUS
    $oldRevision = $env:PANDORA_TEST_GIT_REVISION
    $oldDirtyAfterMarker = $env:PANDORA_TEST_DIRTY_AFTER_MARKER
    $oldRunServicesLog = $env:PANDORA_TEST_RUN_SERVICES_LOG
    $oldGoMetadataMode = $env:PANDORA_TEST_GO_METADATA_MODE
    $oldRequireBuildIdentity = $env:PANDORA_TEST_REQUIRE_BUILD_IDENTITY
    $oldGoFlags = $env:GOFLAGS
    try {
        $env:PATH = "$($Repository.StubDir);$oldPath"
        $env:PANDORA_TEST_GIT_STATUS = $GitStatus
        $env:PANDORA_TEST_GIT_REVISION = $GitRevision
        $env:PANDORA_TEST_DIRTY_AFTER_MARKER = if ($DirtyAfterBuild) {
            Join-Path $Repository.Root 'dirty-after-build.marker'
        } else { '' }
        $env:PANDORA_TEST_RUN_SERVICES_LOG = $Repository.RunServicesLog
        $env:PANDORA_TEST_GO_METADATA_MODE = $GoMetadataMode
        $env:PANDORA_TEST_REQUIRE_BUILD_IDENTITY = if ($RequireBuildIdentity) { '1' } else { '' }
        if ($RequireBuildIdentity) { $env:GOFLAGS = '-buildvcs=false' }
        if ($FailBinaryName) {
            $failModule = switch ($FailBinaryName) {
                'pandora-migrate' { 'tools/migrate' }
                'configtable-gen' { 'tools/configtable-gen' }
                default { throw "测试夹具不认识失败目标:$FailBinaryName" }
            }
            [IO.File]::WriteAllText(
                (Join-Path $Repository.Root "$failModule/.pandora-test-go-fail"),
                'fail',
                [Text.UTF8Encoding]::new($false))
        }
        if ($ExtraBinaryModule) {
            [IO.File]::WriteAllText(
                (Join-Path $Repository.Root "$ExtraBinaryModule/.pandora-test-go-extra"),
                'extra',
                [Text.UTF8Encoding]::new($false))
        }
        $output = (& pwsh -NoProfile -File $Repository.BuildScript @Arguments 2>&1 | Out-String)
        return [pscustomobject]@{ ExitCode = $LASTEXITCODE; Output = $output }
    } finally {
        $env:PATH = $oldPath
        $env:PANDORA_TEST_GIT_STATUS = $oldStatus
        $env:PANDORA_TEST_GIT_REVISION = $oldRevision
        $env:PANDORA_TEST_DIRTY_AFTER_MARKER = $oldDirtyAfterMarker
        $env:PANDORA_TEST_RUN_SERVICES_LOG = $oldRunServicesLog
        $env:PANDORA_TEST_GO_METADATA_MODE = $oldGoMetadataMode
        $env:PANDORA_TEST_REQUIRE_BUILD_IDENTITY = $oldRequireBuildIdentity
        $env:GOFLAGS = $oldGoFlags
    }
}

function Get-TreeReceipt([Parameter(Mandatory)][string]$Root) {
    if (-not (Test-Path -LiteralPath $Root -PathType Container)) { return '<missing>' }
    return (@(Get-ChildItem -LiteralPath $Root -Recurse -File | Sort-Object FullName | ForEach-Object {
                $relative = [IO.Path]::GetRelativePath($Root, $_.FullName)
                "${relative}:$((Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash)"
            }) -join "`n")
}

function New-OldCompleteBatch {
    param([Parameter(Mandatory)]$Repository)

    $windowsRoot = Join-Path $Repository.Root 'run/artifacts/windows'
    $binRoot = Join-Path $windowsRoot 'bin'
    New-Item -ItemType Directory -Force -Path $binRoot | Out-Null
    foreach ($name in $ExpectedBinaryNames) {
        [IO.File]::WriteAllText((Join-Path $binRoot $name), "old:$name", [Text.UTF8Encoding]::new($false))
    }
    [IO.File]::WriteAllText(
        (Join-Path $windowsRoot 'manifest.json'),
        '{"revision":"old-complete-batch"}',
        [Text.UTF8Encoding]::new($false))
    return $windowsRoot
}

$tempRoot = Join-Path ([IO.Path]::GetTempPath()) ("pandora-release-contract-{0}" -f [guid]::NewGuid().ToString('N'))
try {
    Write-Host '[1] 脏工作树不得进入正式构建' -ForegroundColor Cyan
    $repo = New-TestRepository -Root (Join-Path $tempRoot 'case-one')
    $result = Invoke-TestBuild -Repository $repo -GitStatus '?? untracked-source.go'
    Assert-True ($result.ExitCode -ne 0) 'dirty/untracked revision 返回非零'
    Assert-True ($result.Output -match '\[ERR\].*工作区.*不干净') '错误明确指出工作区不干净'
    Assert-True (-not (Test-Path -LiteralPath $repo.RunServicesLog)) '拒绝发生在任何服务构建之前'
    Assert-True (-not (Test-Path -LiteralPath (Join-Path $repo.Root 'run/artifacts/windows'))) `
        '拒绝时不创建或改写正式批次'

    Write-Host '[2] 任一编译失败必须保留旧完整批次' -ForegroundColor Cyan
    $repo = New-TestRepository -Root (Join-Path $tempRoot 'case-two')
    $windowsRoot = New-OldCompleteBatch -Repository $repo
    $before = Get-TreeReceipt -Root $windowsRoot
    $result = Invoke-TestBuild -Repository $repo -FailBinaryName 'pandora-migrate'
    $after = Get-TreeReceipt -Root $windowsRoot
    if ($result.ExitCode -eq 0) {
        Write-Host "  [diag] 未预期成功的构建输出:`n$($result.Output)" -ForegroundColor DarkYellow
    }
    Assert-True ($result.ExitCode -ne 0) '中途编译失败返回非零'
    Assert-True ($after -ceq $before) '失败后旧 bin 与 manifest 逐字节保持不变'

    Write-Host '[3] 成功只发布 exact 24 与可追溯 clean manifest' -ForegroundColor Cyan
    $repo = New-TestRepository -Root (Join-Path $tempRoot 'case-three')
    $windowsRoot = New-OldCompleteBatch -Repository $repo
    [IO.File]::WriteAllText(
        (Join-Path $windowsRoot 'bin/stale-from-previous.exe'),
        'must-disappear',
        [Text.UTF8Encoding]::new($false))
    $foreignStage = Join-Path $repo.Root 'run/artifacts/.windows-build-foreign'
    New-Item -ItemType Directory -Force -Path $foreignStage | Out-Null
    [IO.File]::WriteAllText(
        (Join-Path $foreignStage 'owner.txt'),
        'foreign',
        [Text.UTF8Encoding]::new($false))

    $result = Invoke-TestBuild -Repository $repo
    Assert-True ($result.ExitCode -eq 0) '干净 commit 的全量构建成功'
    $actualNames = @(Get-ChildItem -LiteralPath (Join-Path $windowsRoot 'bin') -File | ForEach-Object Name | Sort-Object)
    Assert-True (($actualNames -join "`n") -ceq (($ExpectedBinaryNames | Sort-Object) -join "`n")) `
        '正式 bin 恰好是 24 项白名单，旧 stale.exe 未混入'
    $manifestPath = Join-Path $windowsRoot 'manifest.json'
    $manifest = if (Test-Path -LiteralPath $manifestPath) {
        [IO.File]::ReadAllText($manifestPath) | ConvertFrom-Json
    } else { $null }
    Assert-True ($null -ne $manifest) '正式批次包含 manifest.json'
    if ($manifest) {
        Assert-True ("$($manifest.vcs)" -ceq 'git') 'manifest 记录 vcs=git'
        Assert-True ("$($manifest.revision)" -ceq '0123456789abcdef0123456789abcdef01234567') `
            'manifest 记录完整 40 位 commit SHA'
        Assert-True ("$($manifest.revision_short)" -ceq '0123456789ab') `
            'manifest 同时记录 12 位短 SHA'
        Assert-True (-not [bool]$manifest.vcs_modified -and [bool]$manifest.clean) `
            'manifest 明确声明 vcs_modified=false 且 clean=true'
        Assert-True ([int]$manifest.binary_count -eq 24 -and @($manifest.binaries).Count -eq 24) `
            'manifest 恰好记录 24 个二进制'
        $hashesMatch = $true
        foreach ($entry in @($manifest.binaries)) {
            $binaryPath = Join-Path $windowsRoot "bin/$($entry.name).exe"
            if (-not (Test-Path -LiteralPath $binaryPath -PathType Leaf) -or
                "$($entry.sha256)" -notmatch '^[0-9A-Fa-f]{64}$' -or
                -not [string]::Equals(
                    (Get-FileHash -LiteralPath $binaryPath -Algorithm SHA256).Hash,
                    "$($entry.sha256)",
                    [StringComparison]::OrdinalIgnoreCase)) {
                $hashesMatch = $false
                break
            }
        }
        Assert-True $hashesMatch 'manifest 的 24 个 SHA256 与正式落盘字节逐项一致'
    }
    Assert-True (Test-Path -LiteralPath (Join-Path $foreignStage 'owner.txt') -PathType Leaf) `
        '唯一 staging 清理不会碰其他并发构建目录'
    Assert-True (-not (Test-Path -LiteralPath $repo.RunServicesLog)) `
        '正式全量构建不经硬编码正式目录的 run_services 发布路径'

    Write-Host '[4] unknown revision 与 staging 多余 exe 都 fail-closed' -ForegroundColor Cyan
    $repo = New-TestRepository -Root (Join-Path $tempRoot 'case-four-revision')
    $result = Invoke-TestBuild -Repository $repo -GitRevision 'unknown'
    Assert-True ($result.ExitCode -ne 0 -and $result.Output -match '完整 Git commit') `
        'unknown revision 在构建前被拒绝'

    $repo = New-TestRepository -Root (Join-Path $tempRoot 'case-four-extra')
    $windowsRoot = New-OldCompleteBatch -Repository $repo
    $before = Get-TreeReceipt -Root $windowsRoot
    $result = Invoke-TestBuild -Repository $repo -ExtraBinaryModule 'tools/configtable-gen'
    $after = Get-TreeReceipt -Root $windowsRoot
    Assert-True ($result.ExitCode -ne 0 -and $result.Output -match 'exact 24') `
        'staging 出现白名单外 exe 时拒绝发布'
    Assert-True ($after -ceq $before) '白名单核验失败仍保留旧完整批次'

    Write-Host '[4a] 构建期间源码变化不得伪装成 clean revision' -ForegroundColor Cyan
    $repo = New-TestRepository -Root (Join-Path $tempRoot 'case-four-source-race')
    $windowsRoot = New-OldCompleteBatch -Repository $repo
    $before = Get-TreeReceipt -Root $windowsRoot
    $result = Invoke-TestBuild -Repository $repo -DirtyAfterBuild
    $after = Get-TreeReceipt -Root $windowsRoot
    Assert-True ($result.ExitCode -ne 0 -and $result.Output -match '构建期间|工作区.*不干净') `
        '构建结束时重新确认同一 clean commit'
    Assert-True ($after -ceq $before) '源码竞态失败仍保留旧完整批次'

    Write-Host '[4b] 24 个 exe 必须以内嵌 BuildInfo 自证来源与目标平台' -ForegroundColor Cyan
    $repo = New-TestRepository -Root (Join-Path $tempRoot 'case-four-buildinfo-good')
    $windowsRoot = New-OldCompleteBatch -Repository $repo
    $before = Get-TreeReceipt -Root $windowsRoot
    $result = Invoke-TestBuild -Repository $repo -RequireBuildIdentity
    $after = Get-TreeReceipt -Root $windowsRoot
    if ($result.ExitCode -ne 0) { Write-Host "  [diag] 正确 BuildInfo 输出：$($result.Output)" -ForegroundColor DarkYellow }
    Assert-True ($result.ExitCode -eq 0 -and $after -cne $before) `
        '外部 GOFLAGS=-buildvcs=false 时仍显式构建并验证正确 BuildInfo'

    $buildInfoMutants = @(
        [pscustomobject]@{ Mode = 'missing-vcs';    Pattern = 'vcs' },
        [pscustomobject]@{ Mode = 'wrong-revision'; Pattern = 'revision' },
        [pscustomobject]@{ Mode = 'modified';       Pattern = 'vcs\.modified' },
        [pscustomobject]@{ Mode = 'wrong-goos';     Pattern = 'GOOS' },
        [pscustomobject]@{ Mode = 'wrong-goarch';   Pattern = 'GOARCH' },
        [pscustomobject]@{ Mode = 'cgo-enabled';    Pattern = 'CGO_ENABLED' }
    )
    foreach ($mutant in $buildInfoMutants) {
        $repo = New-TestRepository -Root (Join-Path $tempRoot "case-four-buildinfo-$($mutant.Mode)")
        $windowsRoot = New-OldCompleteBatch -Repository $repo
        $before = Get-TreeReceipt -Root $windowsRoot
        $result = Invoke-TestBuild -Repository $repo -RequireBuildIdentity -GoMetadataMode $mutant.Mode
        $after = Get-TreeReceipt -Root $windowsRoot
        # 只接受发布脚本真正抛出的 BuildInfo 错误行，不能被 case-four-buildinfo-*
        # 这类临时目录名里的 mutant 名称误判为成功拒绝。
        $expectedDiagnostic = "(?m)^\[ERR\] 正式制品构建/发布失败：BuildInfo[^\r\n]*$($mutant.Pattern)"
        if ($result.ExitCode -eq 0 -or $result.Output -notmatch $expectedDiagnostic) {
            Write-Host "  [diag] $($mutant.Mode) 输出：$($result.Output)" -ForegroundColor DarkYellow
        }
        Assert-True ($result.ExitCode -ne 0 -and $result.Output -match $expectedDiagnostic) `
            "BuildInfo 反例 $($mutant.Mode) 被精确拒绝"
        Assert-True ($after -ceq $before) "BuildInfo 反例 $($mutant.Mode) 不改旧完整批次"
    }

    Write-Host '[5] -Service 只能写开发输出，不能改正式批次' -ForegroundColor Cyan
    $repo = New-TestRepository -Root (Join-Path $tempRoot 'case-five')
    $windowsRoot = New-OldCompleteBatch -Repository $repo
    $before = Get-TreeReceipt -Root $windowsRoot
    $result = Invoke-TestBuild -Repository $repo -GitStatus '?? intentionally-dirty.go' `
        -Arguments @('-Service', 'login')
    $after = Get-TreeReceipt -Root $windowsRoot
    $serviceArgs = if (Test-Path -LiteralPath $repo.RunServicesLog) {
        [IO.File]::ReadAllText($repo.RunServicesLog)
    } else { '' }
    Assert-True ($result.ExitCode -eq 0) '-Service 作为开发定向构建可独立执行'
    Assert-True ($serviceArgs -match '-Action build -Service login' -and
        $serviceArgs -notmatch 'PublishArtifacts') '定向构建不向 run_services 传 PublishArtifacts'
    Assert-True ($after -ceq $before) '-Service 不改正式 bin/manifest'
    $result = Invoke-TestBuild -Repository $repo -Arguments @('-Service', 'login', '-Zip')
    Assert-True ($result.ExitCode -ne 0 -and $result.Output -match '不能同时使用 -Zip') `
        '-Service 与 -Zip 的混批请求被拒绝'
    Assert-True ((Get-TreeReceipt -Root $windowsRoot) -ceq $before) `
        '混批请求失败仍不改正式批次'
} finally {
    Remove-Item -LiteralPath $tempRoot -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Host ''
if ($script:Failures.Count -gt 0) {
    Write-Host "[ERR ] $($script:Failures.Count) 项正式制品发布契约未满足:" -ForegroundColor Red
    $script:Failures | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}

Write-Host '[ OK ] Windows 正式制品发布契约全部满足。' -ForegroundColor Green
