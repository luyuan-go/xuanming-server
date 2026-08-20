# build_release_binaries.ps1 — 构建 Windows 正式预编译二进制，供没装 Go 的策划机直接运行。
#
# 正式全量构建的发布边界是 run/artifacts/windows 整个目录：24 个 exe 与 manifest
# 先在唯一 staging 中全部构建、核验，再整目录切换。任何构建/核验失败都不改旧批次。
#
# 用法：
#   pwsh tools/scripts/build_release_binaries.ps1
#   pwsh tools/scripts/build_release_binaries.ps1 -Zip
#
# 开发定向构建（不属于正式发布，不写 run/artifacts/windows 或正式 manifest）：
#   pwsh tools/scripts/build_release_binaries.ps1 -Service login

[CmdletBinding()]
param(
    # 只做开发定向构建，输出仍是 run/dev/bin；不会发布或改写正式 manifest。
    [string]$Service,
    # 全量正式批次通过后，另生成一个包含 windows/ 内容的分发 zip。
    [switch]$Zip
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ScriptDir = $PSScriptRoot
$ProjectRoot = (Resolve-Path "$ScriptDir/../..").Path
$ArtifactParent = Join-Path $ProjectRoot 'run/artifacts'
$PublishedRoot = Join-Path $ArtifactParent 'windows'

function Stop-ReleaseBuild([Parameter(Mandatory)][string]$Message) {
    Write-Host "[ERR] $Message" -ForegroundColor Red
    exit 1
}

function Get-RequiredGoBuildSetting {
    param(
        [Parameter(Mandatory)]$BuildInfo,
        [Parameter(Mandatory)][string]$Key,
        [Parameter(Mandatory)][string]$BinaryName
    )

    $matches = @($BuildInfo.Settings | Where-Object { "$($_.Key)" -ceq $Key })
    if ($matches.Count -ne 1) {
        throw "BuildInfo $BinaryName 缺少或重复 $Key。"
    }
    return "$($matches[0].Value)"
}

function Assert-ReleaseBinaryBuildInfo {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$ExpectedRevision
    )

    $binaryName = [IO.Path]::GetFileName($Path)
    $raw = (& go version -m -json $Path 2>&1 | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or -not $raw) {
        throw "BuildInfo $binaryName 无法读取。"
    }
    try {
        $info = $raw | ConvertFrom-Json -ErrorAction Stop
    } catch {
        throw "BuildInfo $binaryName 不是有效 JSON：$($_.Exception.Message)"
    }

    $vcs = Get-RequiredGoBuildSetting -BuildInfo $info -Key 'vcs' -BinaryName $binaryName
    $revision = Get-RequiredGoBuildSetting -BuildInfo $info -Key 'vcs.revision' -BinaryName $binaryName
    $modified = Get-RequiredGoBuildSetting -BuildInfo $info -Key 'vcs.modified' -BinaryName $binaryName
    $goos = Get-RequiredGoBuildSetting -BuildInfo $info -Key 'GOOS' -BinaryName $binaryName
    $goarch = Get-RequiredGoBuildSetting -BuildInfo $info -Key 'GOARCH' -BinaryName $binaryName
    $cgo = Get-RequiredGoBuildSetting -BuildInfo $info -Key 'CGO_ENABLED' -BinaryName $binaryName

    if ($vcs -cne 'git') { throw "BuildInfo $binaryName 的 vcs=$vcs，不是 git。" }
    if ($revision.ToLowerInvariant() -cne $ExpectedRevision) {
        throw "BuildInfo $binaryName 的 vcs.revision=$revision，与完整源码 SHA 不一致。"
    }
    if ($modified -cne 'false') { throw "BuildInfo $binaryName 的 vcs.modified=$modified，不是 false。" }
    if ($goos -cne 'windows') { throw "BuildInfo $binaryName 的 GOOS=$goos，不是 windows。" }
    if ($goarch -cne 'amd64') { throw "BuildInfo $binaryName 的 GOARCH=$goarch，不是 amd64。" }
    if ($cgo -cne '0') { throw "BuildInfo $binaryName 的 CGO_ENABLED=$cgo，不是 0。" }
}

if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    Stop-ReleaseBuild '本机没装 Go，无法生成预编译产物。该脚本只在后端开发机或 CI 上运行。'
}

# -Service 是开发人员的快捷入口，不具备“正式整批”的语义。尤其不能复用旧正式 bin，
# 也不能重写代表全量批次的 manifest。
if ($Service) {
    if ($Zip) {
        Stop-ReleaseBuild '-Service 是开发定向构建，不能同时使用 -Zip。请先做干净的正式全量构建。'
    }
    Write-Host "===== 开发定向构建：$Service =====" -ForegroundColor Cyan
    & (Join-Path $ScriptDir 'run_services.ps1') -Action build -Service $Service
    $serviceBuildSucceeded = $?
    if (-not $serviceBuildSucceeded) {
        Stop-ReleaseBuild "开发定向构建失败：$Service"
    }
    Write-Host "[ OK ] 开发二进制 -> run/dev/bin/$Service.exe（正式制品未改动）" -ForegroundColor Green
    exit 0
}

if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    Stop-ReleaseBuild '找不到 git，无法证明源码 revision，拒绝生成正式制品。'
}

# 必须在任何 go build 和正式目录写入之前锁定来源。未跟踪文件同样可能参与 workspace
# 构建，因此也属于 modified，不能被正式 manifest 冒充成某个 clean commit。
$revisionFull = (& git -C $ProjectRoot rev-parse --verify 'HEAD^{commit}' 2>$null | Out-String).Trim().ToLowerInvariant()
if ($LASTEXITCODE -ne 0 -or $revisionFull -notmatch '^[0-9a-f]{40}$') {
    Stop-ReleaseBuild '无法解析完整 Git commit，拒绝生成正式制品。'
}
$vcsChanges = (& git -C $ProjectRoot status --porcelain=v1 --untracked-files=all 2>$null | Out-String).Trim()
if ($LASTEXITCODE -ne 0) {
    Stop-ReleaseBuild '无法确认 Git 工作区状态，拒绝生成正式制品。'
}
if ($vcsChanges) {
    Stop-ReleaseBuild 'Git 工作区不干净（含未跟踪文件），拒绝生成正式制品。'
}
$revisionShort = $revisionFull.Substring(0, 12)

# 这是正式发布的唯一白名单。Name 同时决定最终文件名；同一 matchmaker module
# 会按两个运行角色产出两个独立命名的 exe。
$ReleaseBinaries = @(
    [pscustomobject]@{ Name = 'player_locator';  Dir = 'services/runtime/player_locator';       Package = './cmd/locator' }
    [pscustomobject]@{ Name = 'hub_allocator';   Dir = 'services/battle/hub_allocator';         Package = './cmd/hub_allocator' }
    [pscustomobject]@{ Name = 'player';          Dir = 'services/account/player';               Package = './cmd/player' }
    [pscustomobject]@{ Name = 'ds_allocator';    Dir = 'services/battle/ds_allocator';          Package = './cmd/ds_allocator' }
    [pscustomobject]@{ Name = 'push';            Dir = 'services/runtime/push';                 Package = './cmd/push' }
    [pscustomobject]@{ Name = 'team';            Dir = 'services/matchmaking/team';             Package = './cmd/team' }
    [pscustomobject]@{ Name = 'friend';          Dir = 'services/social/friend';                Package = './cmd/friend' }
    [pscustomobject]@{ Name = 'chat';            Dir = 'services/social/chat';                  Package = './cmd/chat' }
    [pscustomobject]@{ Name = 'guild';           Dir = 'services/social/guild';                 Package = './cmd/guild' }
    [pscustomobject]@{ Name = 'mail';            Dir = 'services/social/mail';                  Package = './cmd/mail' }
    [pscustomobject]@{ Name = 'dialogue';        Dir = 'services/social/dialogue';              Package = './cmd/dialogue' }
    [pscustomobject]@{ Name = 'mission';         Dir = 'services/social/mission';               Package = './cmd/mission' }
    [pscustomobject]@{ Name = 'data_service';    Dir = 'services/data/data_service';            Package = './cmd/data_service' }
    [pscustomobject]@{ Name = 'trade';           Dir = 'services/economy/trade';                Package = './cmd/trade' }
    [pscustomobject]@{ Name = 'inventory';       Dir = 'services/economy/inventory';            Package = './cmd/inventory' }
    [pscustomobject]@{ Name = 'leaderboard';     Dir = 'services/runtime/leaderboard';          Package = './cmd/leaderboard' }
    [pscustomobject]@{ Name = 'owner';           Dir = 'services/runtime/owner';                Package = './cmd/owner' }
    [pscustomobject]@{ Name = 'auction';         Dir = 'services/economy/auction';              Package = './cmd/auction' }
    [pscustomobject]@{ Name = 'battle_result';   Dir = 'services/battle/battle_result';         Package = './cmd/battle_result' }
    [pscustomobject]@{ Name = 'matchmaker';      Dir = 'services/matchmaking/matchmaker';       Package = './cmd/matchmaker' }
    [pscustomobject]@{ Name = 'matchmaker_pve';  Dir = 'services/matchmaking/matchmaker';       Package = './cmd/matchmaker' }
    [pscustomobject]@{ Name = 'login';           Dir = 'services/account/login';                Package = './cmd/login' }
    [pscustomobject]@{ Name = 'configtable-gen'; Dir = 'tools/configtable-gen';                 Package = '.' }
    [pscustomobject]@{ Name = 'pandora-migrate'; Dir = 'tools/migrate';                         Package = '.' }
)

$expectedNames = @($ReleaseBinaries | ForEach-Object { "$($_.Name).exe" } | Sort-Object)
if ($ReleaseBinaries.Count -ne 24 -or @($expectedNames | Select-Object -Unique).Count -ne 24) {
    Stop-ReleaseBuild '正式二进制白名单必须恰好包含 24 个不重名文件。'
}

New-Item -ItemType Directory -Force -Path $ArtifactParent | Out-Null
$runId = "{0}-{1}" -f $PID, [guid]::NewGuid().ToString('N')
$stageRoot = Join-Path $ArtifactParent ".windows-build-$runId"
$stageBin = Join-Path $stageRoot 'bin'
$backupRoot = Join-Path $ArtifactParent ".windows-backup-$runId"
$discardRoot = Join-Path $ArtifactParent ".windows-discard-$runId"
$stageZip = Join-Path $ArtifactParent ".pandora-server-bin-$runId.zip"
$zipPath = $null
$oldBatchMoved = $false
$newBatchMoved = $false
$zipMoved = $false

try {
    New-Item -ItemType Directory -Path $stageBin | Out-Null
    Write-Host '===== 生成 Windows 正式预编译产物 =====' -ForegroundColor Cyan
    Write-Host "来源：$revisionFull（clean）" -ForegroundColor DarkGray
    Write-Host "staging：$stageRoot" -ForegroundColor DarkGray

    $previousGoos = $env:GOOS
    $previousGoarch = $env:GOARCH
    $previousCgoEnabled = $env:CGO_ENABLED
    try {
        # 只影响本脚本及其 go 子进程，finally 精确恢复；-buildvcs=true 使用命令行参数，
        # 因此外部 GOFLAGS=-buildvcs=false 也不能静默剥掉正式二进制的 VCS 身份。
        $env:GOOS = 'windows'
        $env:GOARCH = 'amd64'
        $env:CGO_ENABLED = '0'
        foreach ($binary in $ReleaseBinaries) {
            $sourceDir = Join-Path $ProjectRoot $binary.Dir
            if (-not (Test-Path -LiteralPath $sourceDir -PathType Container)) {
                throw "正式二进制源码目录不存在：$($binary.Dir)"
            }
            $outputPath = Join-Path $stageBin "$($binary.Name).exe"
            Write-Host "  [build] $($binary.Name)" -ForegroundColor DarkGray
            Push-Location $sourceDir
            try {
                & go build -buildvcs=true -o $outputPath $binary.Package
                $buildExit = $LASTEXITCODE
            } finally {
                Pop-Location
            }
            if ($buildExit -ne 0) {
                throw "go build 失败：$($binary.Name)（exit $buildExit）"
            }
            if (-not (Test-Path -LiteralPath $outputPath -PathType Leaf)) {
                throw "go build 未生成约定文件：$($binary.Name).exe"
            }
        }
    } finally {
        $env:GOOS = $previousGoos
        $env:GOARCH = $previousGoarch
        $env:CGO_ENABLED = $previousCgoEnabled
    }

    $actualFiles = @(Get-ChildItem -LiteralPath $stageBin -File | Sort-Object Name)
    $actualNames = @($actualFiles | ForEach-Object Name)
    $unexpected = @($actualNames | Where-Object { $expectedNames -notcontains $_ })
    $missing = @($expectedNames | Where-Object { $actualNames -notcontains $_ })
    if ($actualFiles.Count -ne 24 -or $unexpected.Count -gt 0 -or $missing.Count -gt 0) {
        throw "staging 不是 exact 24 白名单；缺少=[$($missing -join ',')]，多出=[$($unexpected -join ',')]。"
    }
    foreach ($file in $actualFiles) {
        Assert-ReleaseBinaryBuildInfo -Path $file.FullName -ExpectedRevision $revisionFull
    }

    # 编译可能持续数分钟；起点 clean 不代表终点仍是同一份源码。发布前必须再次确认
    # HEAD 未移动且工作区仍无 tracked/untracked 变化，否则 manifest 的 clean 声明会撒谎。
    $revisionAfterBuild = (& git -C $ProjectRoot rev-parse --verify 'HEAD^{commit}' 2>$null | Out-String).Trim().ToLowerInvariant()
    if ($LASTEXITCODE -ne 0 -or $revisionAfterBuild -cne $revisionFull) {
        throw 'Git HEAD 在构建期间发生变化，拒绝发布本批次。'
    }
    $changesAfterBuild = (& git -C $ProjectRoot status --porcelain=v1 --untracked-files=all 2>$null | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw '构建结束时无法确认 Git 工作区状态，拒绝发布本批次。'
    }
    if ($changesAfterBuild) {
        throw 'Git 工作区在构建期间变得不干净，拒绝发布本批次。'
    }

    $goVersion = (& go version 2>&1 | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or -not $goVersion) {
        throw '无法读取 go version，拒绝发布正式批次。'
    }
    $manifest = [ordered]@{
        built_at       = (Get-Date).ToUniversalTime().ToString('o')
        built_on       = "$env:COMPUTERNAME"
        go_version     = $goVersion
        vcs            = 'git'
        revision       = $revisionFull
        revision_short = $revisionShort
        vcs_modified   = $false
        clean          = $true
        binary_count   = 24
        binaries       = @($actualFiles | ForEach-Object {
            [ordered]@{
                name   = $_.BaseName
                size   = [int64]$_.Length
                sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash
            }
        })
    }
    $manifestPath = Join-Path $stageRoot 'manifest.json'
    [IO.File]::WriteAllText(
        $manifestPath,
        ($manifest | ConvertTo-Json -Depth 5),
        [Text.UTF8Encoding]::new($false))

    # 发布前从真实落盘字节回读一次，避免“内存对象正确、实际 manifest 不完整”。
    $savedManifest = [IO.File]::ReadAllText($manifestPath) | ConvertFrom-Json
    $savedNames = @($savedManifest.binaries | ForEach-Object { "$($_.name).exe" } | Sort-Object)
    if ("$($savedManifest.revision)" -cne $revisionFull -or
        "$($savedManifest.revision_short)" -cne $revisionShort -or
        [bool]$savedManifest.vcs_modified -or -not [bool]$savedManifest.clean -or
        [int]$savedManifest.binary_count -ne 24 -or
        (Compare-Object -ReferenceObject $expectedNames -DifferenceObject $savedNames)) {
        throw '落盘 manifest 未通过 revision/clean/exact-24 回读核验。'
    }
    foreach ($entry in @($savedManifest.binaries)) {
        $path = Join-Path $stageBin "$($entry.name).exe"
        $actualHash = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash
        if (-not [string]::Equals($actualHash, "$($entry.sha256)", [StringComparison]::OrdinalIgnoreCase)) {
            throw "manifest SHA256 回读不一致：$($entry.name).exe"
        }
    }

    if ($Zip) {
        $zipPath = Join-Path $ArtifactParent ("pandora-server-bin-{0}-{1}.zip" -f `
                (Get-Date -Format 'yyyyMMdd-HHmmss'), $revisionShort)
        if (Test-Path -LiteralPath $zipPath) {
            throw "分发 zip 已存在，拒绝覆盖：$zipPath"
        }
        Compress-Archive -Path (Join-Path $stageRoot '*') -DestinationPath $stageZip
    }

    # 整目录切换保证 bin 与 manifest 永不分批发布。Windows 不能覆盖非空目录，
    # 因此先把旧批次改名为唯一 backup；新批次切换失败时立即恢复旧目录。
    if (Test-Path -LiteralPath $PublishedRoot) {
        [IO.Directory]::Move($PublishedRoot, $backupRoot)
        $oldBatchMoved = $true
    }
    try {
        [IO.Directory]::Move($stageRoot, $PublishedRoot)
        $newBatchMoved = $true
        if ($Zip) {
            [IO.File]::Move($stageZip, $zipPath)
            $zipMoved = $true
        }
    } catch {
        if ($zipMoved -and $zipPath -and (Test-Path -LiteralPath $zipPath)) {
            Remove-Item -LiteralPath $zipPath -Force -ErrorAction SilentlyContinue
        }
        if ($newBatchMoved -and (Test-Path -LiteralPath $PublishedRoot)) {
            [IO.Directory]::Move($PublishedRoot, $discardRoot)
            $newBatchMoved = $false
        }
        if ($oldBatchMoved -and -not (Test-Path -LiteralPath $PublishedRoot)) {
            [IO.Directory]::Move($backupRoot, $PublishedRoot)
            $oldBatchMoved = $false
        }
        throw
    }

    if ($oldBatchMoved -and (Test-Path -LiteralPath $backupRoot)) {
        Remove-Item -LiteralPath $backupRoot -Recurse -Force -ErrorAction SilentlyContinue
        $oldBatchMoved = $false
    }
    if (Test-Path -LiteralPath $discardRoot) {
        Remove-Item -LiteralPath $discardRoot -Recurse -Force -ErrorAction SilentlyContinue
    }

    Write-Host "[ OK ] exact 24 二进制 -> $(Join-Path $PublishedRoot 'bin')" -ForegroundColor Green
    Write-Host "[ OK ] 清单 -> $(Join-Path $PublishedRoot 'manifest.json')" -ForegroundColor Green
    if ($Zip) { Write-Host "[ OK ] 分发包 -> $zipPath" -ForegroundColor Green }
} catch {
    Write-Host "[ERR] 正式制品构建/发布失败：$($_.Exception.Message)" -ForegroundColor Red
    exit 1
} finally {
    # 只清理本轮唯一 staging；绝不使用通配符碰别的并发构建或正式目录。
    if (Test-Path -LiteralPath $stageRoot) {
        Remove-Item -LiteralPath $stageRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
    if (Test-Path -LiteralPath $stageZip) {
        Remove-Item -LiteralPath $stageZip -Force -ErrorAction SilentlyContinue
    }
    if (Test-Path -LiteralPath $discardRoot) {
        Remove-Item -LiteralPath $discardRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
    # 兜底恢复只在正式目录尚未出现时执行；不能覆盖一个已完整切换的新批次。
    if ($oldBatchMoved -and -not (Test-Path -LiteralPath $PublishedRoot) -and
        (Test-Path -LiteralPath $backupRoot)) {
        [IO.Directory]::Move($backupRoot, $PublishedRoot)
    }
}

Write-Host ''
Write-Host '怎么用：把 run/artifacts 整个目录同步给策划，他们照常双击一键启动即可。' -ForegroundColor Cyan
Write-Host 'run_services.ps1 会在无 Go 的机器上核对 manifest 后使用这批预编译二进制。' -ForegroundColor Cyan
