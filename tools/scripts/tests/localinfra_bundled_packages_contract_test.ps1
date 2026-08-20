# 免 Docker 第三方安装包的“SVN 本地优先、Git 缺包联网”契约测试。
#
# 只抽取 local_infra.ps1 的取包函数，在临时目录和内存下载桩上运行；不访问公网，
# 不启动 MySQL/Redis/Kafka/Envoy，也不修改真实 run/localinfra/cache。

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ScriptsDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
$Infra = Join-Path $ScriptsDir 'local_infra.ps1'
$Bootstrap = Join-Path $ScriptsDir 'bootstrap_pwsh.cmd'
$PinFile = Join-Path $ScriptsDir 'lib/pwsh_bootstrap.pin'
$InstallerDir = Join-Path $ProjectRoot 'installers/localinfra'

$script:Failures = [System.Collections.Generic.List[string]]::new()
function Assert-True([bool]$Condition, [string]$Message) {
    if ($Condition) { Write-Host "  [ok] $Message" -ForegroundColor Green }
    else { $script:Failures.Add($Message); Write-Host "  [FAIL] $Message" -ForegroundColor Red }
}

$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($Infra, [ref]$null, [ref]$parseErrors)
if ($parseErrors -and $parseErrors.Count -gt 0) { throw "local_infra.ps1 语法错误:$($parseErrors[0].Message)" }

function Get-InfraFunctionText([string]$Name) {
    $matches = @($ast.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $Name
    }, $true))
    if ($matches.Count -ne 1) { throw "local_infra.ps1 里找不到唯一的 $Name(找到 $($matches.Count) 个)" }
    return $matches[0].Extent.Text
}

foreach ($name in @(
    'Resolve-LocalInfraPackageMirror',
    'Test-FileSha256',
    'New-ArchiveRunFile',
    'New-ArchiveSnapshot',
    'Remove-ArchiveSnapshot',
    'Enter-ArchiveCachePublishLock',
    'Exit-ArchiveCachePublishLock',
    'Publish-ArchiveCacheFile',
    'Get-Archive'
)) {
    Invoke-Expression (Get-InfraFunctionText $name)
}

Write-Host '[1] 安装包目录解析' -ForegroundColor Cyan
$defaultMirror = Resolve-LocalInfraPackageMirror -RepositoryRoot 'C:\repo' -ExplicitMirror ''
Assert-True ($defaultMirror.Path -eq 'C:\repo\installers\localinfra') '未显式设置时使用仓库 installers/localinfra'
Assert-True ($defaultMirror.Kind -eq '仓库安装包') '默认来源标成仓库安装包'
$explicitMirror = Resolve-LocalInfraPackageMirror -RepositoryRoot 'C:\repo' -ExplicitMirror '  D:\team-share  '
Assert-True ($explicitMirror.Path -eq 'D:\team-share') '显式 PANDORA_LOCALINFRA_MIRROR 优先且去掉首尾空白'
Assert-True ($explicitMirror.Kind -eq '显式离线镜像') '显式来源不会被误报成仓库包'

# 不能只测 resolver 本身：生产脚本级赋值若漏接/接错，函数单测仍会假绿。
$mirrorAssignments = @($ast.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.AssignmentStatementAst] -and $node.Left.Extent.Text -eq '$PackageMirror'
}, $true))
Assert-True ($mirrorAssignments.Count -eq 1) '生产脚本存在唯一的 $PackageMirror 接线赋值'
if ($mirrorAssignments.Count -eq 1) {
    function Invoke-ProductionMirrorAssignment([string]$RepositoryRoot, [AllowNull()][string]$Explicit) {
        $oldMirror = $env:PANDORA_LOCALINFRA_MIRROR
        try {
            $ProjectRoot = $RepositoryRoot
            $env:PANDORA_LOCALINFRA_MIRROR = $Explicit
            Invoke-Expression $mirrorAssignments[0].Extent.Text
            return $PackageMirror
        } finally {
            $env:PANDORA_LOCALINFRA_MIRROR = $oldMirror
        }
    }
    $wiredDefault = Invoke-ProductionMirrorAssignment -RepositoryRoot 'C:\wired-repo' -Explicit $null
    Assert-True ($wiredDefault.Path -eq 'C:\wired-repo\installers\localinfra') '生产接线把空环境变量解析到仓库目录'
    $wiredExplicit = Invoke-ProductionMirrorAssignment -RepositoryRoot 'C:\wired-repo' -Explicit 'D:\wired-share'
    Assert-True ($wiredExplicit.Path -eq 'D:\wired-share') '生产接线把显式环境变量传给 resolver'
}

Write-Host '[2] cache → 仓库包 → 公网与 SHA256 闸门' -ForegroundColor Cyan
$sandbox = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-package-contract-' + [guid]::NewGuid().ToString('N'))
$CacheDir = Join-Path $sandbox 'cache'
$bundleDir = Join-Path $sandbox 'installers/localinfra'
New-Item -ItemType Directory -Force -Path $CacheDir, $bundleDir | Out-Null
$PackageMirror = [pscustomobject]@{ Path = $bundleDir; Kind = '仓库安装包' }
$Force = $false
$fixtureName = 'fixture.pkg'
$goodBytes = [System.Text.Encoding]::UTF8.GetBytes('pandora-offline-package-fixture-v1')
$badBytes = [System.Text.Encoding]::UTF8.GetBytes('tampered-package')
$hashProbe = Join-Path $sandbox 'hash-probe'
[System.IO.File]::WriteAllBytes($hashProbe, $goodBytes)
$fixtureHash = (Get-FileHash -LiteralPath $hashProbe -Algorithm SHA256).Hash.ToLowerInvariant()
Remove-Item -LiteralPath $hashProbe -Force
$script:RemoteCalls = 0
$script:RemoteOutFiles = [System.Collections.Generic.List[string]]::new()
function Get-RemoteFile {
    param([string]$Uri, [string]$OutFile, [hashtable]$Headers = @{})
    $script:RemoteCalls++
    $script:RemoteOutFiles.Add([IO.Path]::GetFullPath($OutFile))
    [System.IO.File]::WriteAllBytes($OutFile, $goodBytes)
}
function Write-Warn2([string]$Message) {}
function Fail([string]$Message) { throw "PANDORA_EXPECTED_FAIL:$Message" }

try {
    $cacheFile = Join-Path $CacheDir $fixtureName
    $bundleFile = Join-Path $bundleDir $fixtureName

    [System.IO.File]::WriteAllBytes($cacheFile, $goodBytes)
    [System.IO.File]::WriteAllBytes($bundleFile, $badBytes)
    $script:RemoteCalls = 0
    $got = Get-Archive -File $fixtureName -Urls @('https://invalid.example/fixture') -Sha256 $fixtureHash
    Assert-True ($got -ne $cacheFile -and (Test-FileSha256 -Path $got -Expected $fixtureHash)) `
        '有效 cache 优先并返回本轮稳定快照，不访问安装包目录或公网'
    Assert-True ($script:RemoteCalls -eq 0) 'cache 命中时公网调用为 0'
    Remove-ArchiveSnapshot -Path $got

    Remove-Item -LiteralPath $cacheFile -Force
    [System.IO.File]::WriteAllBytes($bundleFile, $goodBytes)
    $script:RemoteCalls = 0
    $got = Get-Archive -File $fixtureName -Urls @('https://invalid.example/fixture') -Sha256 $fixtureHash
    $replacement = Join-Path $bundleDir 'fixture-replacement.pkg'
    [System.IO.File]::WriteAllBytes($replacement, $badBytes)
    [System.IO.File]::Move($replacement, $bundleFile, $true)
    Assert-True ($got -ne $bundleFile) '本机 SVN 安装包返回独立稳定快照，不直接暴露可替换的工作副本路径'
    Assert-True (Test-FileSha256 -Path $got -Expected $fixtureHash) '源包在校验后被原子替换，已验证快照仍保持原字节'
    Assert-True (-not (Test-Path -LiteralPath $cacheFile)) 'SVN 安装包直读时不制造重复 cache'
    Assert-True ($script:RemoteCalls -eq 0) 'SVN 安装包命中时公网调用为 0'
    Assert-True (Test-Path -LiteralPath $bundleFile) '只读 SVN 安装包没有被移动或删除'
    Remove-ArchiveSnapshot -Path $got

    [System.IO.File]::WriteAllBytes($bundleFile, $goodBytes)

    $explicitDir = Join-Path $sandbox 'team-share'
    New-Item -ItemType Directory -Force -Path $explicitDir | Out-Null
    $explicitFile = Join-Path $explicitDir $fixtureName
    [System.IO.File]::WriteAllBytes($explicitFile, $goodBytes)
    $PackageMirror = [pscustomobject]@{ Path = $explicitDir; Kind = '显式离线镜像' }
    $got = Get-Archive -File $fixtureName -Urls @('https://invalid.example/fixture') -Sha256 $fixtureHash
    Assert-True ($got -ne $cacheFile -and (Test-FileSha256 -Path $got -Expected $fixtureHash) -and
        (Test-FileSha256 -Path $cacheFile -Expected $fixtureHash)) '显式/UNC 镜像仍落到本机 cache，但本轮使用独立快照'
    Assert-True (Test-Path -LiteralPath $explicitFile) '显式镜像源保持只读'
    Remove-ArchiveSnapshot -Path $got

    Remove-Item -LiteralPath $cacheFile -Force
    $PackageMirror = [pscustomobject]@{ Path = $bundleDir; Kind = '仓库安装包' }
    Remove-Item -LiteralPath $bundleFile -Force
    $script:RemoteCalls = 0
    $got = Get-Archive -File $fixtureName -Urls @('https://invalid.example/fixture') -Sha256 $fixtureHash
    Assert-True ($got -ne $cacheFile -and (Test-FileSha256 -Path $got -Expected $fixtureHash) -and
        (Test-FileSha256 -Path $cacheFile -Expected $fixtureHash)) '目录为空/缺当前文件时正常回退公网并返回独立快照'
    Assert-True ($script:RemoteCalls -eq 1) '缺包时只调用一次下载桩'
    $firstNetworkSnapshot = $got

    # 两个重叠 run 不能共用 cache/<file>.part 或同一下载目标。第二轮强制刷新
    # 和清理自己的快照后，第一轮已验证的快照仍必须可用。
    $Force = $true
    $secondNetworkSnapshot = Get-Archive -File $fixtureName -Urls @('https://invalid.example/fixture') -Sha256 $fixtureHash
    $Force = $false
    $downloadTargets = @($script:RemoteOutFiles | Select-Object -Unique)
    Assert-True ($downloadTargets.Count -eq 2 -and $downloadTargets -notcontains ([IO.Path]::GetFullPath($cacheFile))) `
        '两个重叠 run 各自下载到唯一路径，不共用固定 cache/.part'
    Assert-True ((Test-FileSha256 -Path $firstNetworkSnapshot -Expected $fixtureHash) -and
        (Test-FileSha256 -Path $secondNetworkSnapshot -Expected $fixtureHash)) `
        '后启动的 run 发布 cache 时不改变前一轮已验证快照'
    Remove-ArchiveSnapshot -Path $secondNetworkSnapshot
    Assert-True (Test-FileSha256 -Path $firstNetworkSnapshot -Expected $fixtureHash) `
        '一轮清理自己的 run 目录不会删除另一轮快照'
    Remove-ArchiveSnapshot -Path $firstNetworkSnapshot

    Remove-Item -LiteralPath $cacheFile -Force
    [System.IO.File]::WriteAllBytes($bundleFile, $badBytes)
    $script:RemoteCalls = 0
    $caught = $null
    try { Get-Archive -File $fixtureName -Urls @('https://invalid.example/fixture') -Sha256 $fixtureHash | Out-Null }
    catch { $caught = $_.Exception.Message }
    Assert-True ($caught -like 'PANDORA_EXPECTED_FAIL:*校验不通过*') '同名 SVN 包 SHA256 不符时硬失败'
    Assert-True ($script:RemoteCalls -eq 0) '坏 SVN 包不会被公网静默绕过'
    Assert-True (-not (Test-Path -LiteralPath $cacheFile)) '坏副本不留在可变 cache'
    Assert-True (Test-Path -LiteralPath $bundleFile) '坏 SVN 源文件保留，便于 svn update/排查'

    Write-Host '[2b] 两个真并发 run 安全收敛到固定 cache' -ForegroundColor Cyan
    $candidateA = New-ArchiveRunFile -File $fixtureName
    $candidateB = New-ArchiveRunFile -File $fixtureName
    [IO.File]::WriteAllBytes($candidateA, $goodBytes)
    [IO.File]::WriteAllBytes($candidateB, $goodBytes)
    $cachePublishLock = "$cacheFile.pandora-publish-lock"
    New-Item -ItemType Directory -Path $cachePublishLock | Out-Null
    $publishInitText = @(
        'Test-FileSha256',
        'New-ArchiveRunFile',
        'New-ArchiveSnapshot',
        'Remove-ArchiveSnapshot',
        'Enter-ArchiveCachePublishLock',
        'Exit-ArchiveCachePublishLock',
        'Publish-ArchiveCacheFile'
    ) | ForEach-Object { Get-InfraFunctionText $_ }
    $publishInit = [scriptblock]::Create($publishInitText -join "`n")
    $publishJob = {
        param($Candidate, $Destination, $Expected, $CacheRoot)
        $CacheDir = $CacheRoot
        Publish-ArchiveCacheFile -VerifiedPath $Candidate -Destination $Destination -ExpectedSha256 $Expected
        'PANDORA_PUBLISH_OK'
    }
    $jobs = @(
        Start-ThreadJob -InitializationScript $publishInit -ScriptBlock $publishJob -ArgumentList $candidateA, $cacheFile, $fixtureHash, $CacheDir
        Start-ThreadJob -InitializationScript $publishInit -ScriptBlock $publishJob -ArgumentList $candidateB, $cacheFile, $fixtureHash, $CacheDir
    )
    try {
        Start-Sleep -Milliseconds 500
        Remove-Item -LiteralPath $cachePublishLock -Force
        $jobOutput = @($jobs | Receive-Job -Wait)
        $jobFailures = @($jobs | Where-Object State -ne 'Completed')
        Assert-True ($jobFailures.Count -eq 0 -and @($jobOutput | Where-Object { $_ -eq 'PANDORA_PUBLISH_OK' }).Count -eq 2) `
            '两个真并发 publisher 都在有界等待后成功收敛'
        Assert-True (Test-FileSha256 -Path $cacheFile -Expected $fixtureHash) `
            '并发收敛后固定 cache 只包含完整已验证字节'
        Assert-True ((Test-FileSha256 -Path $candidateA -Expected $fixtureHash) -and
            (Test-FileSha256 -Path $candidateB -Expected $fixtureHash)) `
            '任一 publisher 都不移动或删除 peer 仍在使用的 verified snapshot'
        Assert-True (-not (Test-Path -LiteralPath $cachePublishLock) -and
            @(Get-ChildItem -LiteralPath $CacheDir -Filter ".$fixtureName.pandora-publish-*" -ErrorAction SilentlyContinue).Count -eq 0) `
            '并发 cache 发布后不残留锁或任一 run 的 publish 文件'
    } finally {
        $jobs | Remove-Job -Force -ErrorAction SilentlyContinue
        Remove-Item -LiteralPath $cachePublishLock -Force -ErrorAction SilentlyContinue
        Remove-ArchiveSnapshot -Path $candidateA
        Remove-ArchiveSnapshot -Path $candidateB
    }
}
finally {
    if (Test-Path -LiteralPath $sandbox) { Remove-Item -LiteralPath $sandbox -Recurse -Force -ErrorAction SilentlyContinue }
}

Write-Host '[3] 已安装目录必须绑定当前固定包身份' -ForegroundColor Cyan
$markerFunctions = @{}
foreach ($name in 'Get-PackageMarkerPath', 'Test-PackageMarker', 'Write-PackageMarker') {
    $matches = @($ast.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $name
    }, $true))
    Assert-True ($matches.Count -eq 1) "生产脚本存在唯一的 $name"
    if ($matches.Count -eq 1) {
        $markerFunctions[$name] = $matches[0].Extent.Text
        Invoke-Expression $matches[0].Extent.Text
    }
}
if ($markerFunctions.Count -eq 3) {
    $markerSandbox = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-package-marker-' + [guid]::NewGuid().ToString('N'))
    $oldHash = '1' * 64
    $currentHash = '2' * 64
    try {
        New-Item -ItemType Directory -Force -Path $markerSandbox | Out-Null
        Assert-True (-not (Test-PackageMarker -Directory $markerSandbox -ExpectedSha256 $currentHash)) `
            '已有 dist 但没有 marker 时判为旧安装，必须重新备料'
        Write-PackageMarker -Directory $markerSandbox -Sha256 $oldHash
        Assert-True (-not (Test-PackageMarker -Directory $markerSandbox -ExpectedSha256 $currentHash)) `
            'marker 属于旧固定包时判为旧安装，必须重新备料'
        Write-PackageMarker -Directory $markerSandbox -Sha256 $currentHash.ToUpperInvariant()
        Assert-True (Test-PackageMarker -Directory $markerSandbox -ExpectedSha256 $currentHash) `
            'marker 与当前固定包 SHA256 一致时才允许复用 dist'
        $markerPath = Get-PackageMarkerPath -Directory $markerSandbox
        Assert-True (([IO.File]::ReadAllText($markerPath)).Trim() -ceq $currentHash) `
            'marker 统一写成小写 SHA256'
        Assert-True (-not (Test-Path -LiteralPath "$markerPath.tmp")) 'marker 原子替换后不残留临时文件'
    } finally {
        if (Test-Path -LiteralPath $markerSandbox) {
            Remove-Item -LiteralPath $markerSandbox -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
}
foreach ($functionName in 'Invoke-ProvisionAll', 'Confirm-MkcertBinary', 'Confirm-EnvoyBinary') {
    $functionText = Get-InfraFunctionText $functionName
    Assert-True ($functionText -match 'Test-PackageMarker') "$functionName 复用 dist 前核对固定包 marker"
    Assert-True ($functionText -match 'Write-PackageMarker') "$functionName 成功安装后写固定包 marker"
    Assert-True ($functionText -match 'New-PackageStagingDirectory') "$functionName 先在 dist 同盘 staging 备料"
    Assert-True ($functionText -match 'Publish-StagedPackageDirectory') "$functionName 通过统一目录 swap 发布"
}
$provisionText = Get-InfraFunctionText 'Invoke-ProvisionAll'
$mkcertText = Get-InfraFunctionText 'Confirm-MkcertBinary'
$envoyInstallText = Get-InfraFunctionText 'Confirm-EnvoyBinary'
Assert-True ($provisionText -match '(?s)Expand-Archive2.*?Test-FileSha256\s+-Path\s+\$ar') `
    '普通压缩包解包后复核同一 verified snapshot'
Assert-True ($mkcertText -match '(?s)Copy-Item.*?Test-FileSha256\s+-Path\s+\$src') `
    'mkcert 复制到 staging 后复核同一 verified snapshot'
Assert-True ($envoyInstallText -match '(?s)tar\.exe.*?Test-FileSha256\s+-Path\s+\$blobFile') `
    'Envoy layer 解包后复核同一 verified snapshot'

Write-Host '[3b] staging 发布不得覆盖运行中的本工作区组件' -ForegroundColor Cyan
$publishFunctions = @(
    'Get-PidFile',
    'Get-ProcessCommandLine',
    'Get-LivePidFileProcess',
    'Test-ProcessReferencesDirectory',
    'Get-PackageDirectoryConsumers',
    'Assert-PackageDirectoryNotInUse',
    'Move-PackageDirectoryAtomic',
    'Publish-StagedPackageDirectory'
)
foreach ($name in $publishFunctions) { Invoke-Expression (Get-InfraFunctionText $name) }

$publishSandbox = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-package-publish-' + [guid]::NewGuid().ToString('N'))
$target = Join-Path $publishSandbox 'redis'
$staged = Join-Path $publishSandbox '.redis-stage'
$oldHash = '1' * 64
$currentHash = '2' * 64
try {
    $PidDir = Join-Path $publishSandbox 'pids'
    New-Item -ItemType Directory -Force -Path $target, $staged, $PidDir | Out-Null
    [IO.File]::WriteAllText((Join-Path $PidDir 'redis.pid'), "$PID")
    Assert-True (@(Get-PackageDirectoryConsumers -Component 'redis' -TargetDirectory $target).Count -eq 0) `
        '陈旧 PID 即使指向外部活进程，也不会冒充目标 dist 使用者'
    Remove-Item -LiteralPath (Join-Path $PidDir 'redis.pid') -Force
    [IO.File]::WriteAllText((Join-Path $target 'old.txt'), 'old')
    [IO.File]::WriteAllText((Join-Path $staged 'new.txt'), 'new')
    Write-PackageMarker -Directory $target -Sha256 $oldHash
    Write-PackageMarker -Directory $staged -Sha256 $currentHash

    $fakeOwned = [pscustomobject]@{
        Id = 12345
        ProcessName = 'redis-server'
        Path = (Join-Path $target 'redis-server.exe')
    }
    $fakeExternal = [pscustomobject]@{
        Id = 23456
        ProcessName = 'redis-server'
        Path = (Join-Path $publishSandbox 'external/redis-server.exe')
    }
    function Get-ProcessCommandLine([int]$ProcessId) { return '' }
    Assert-True (Test-ProcessReferencesDirectory -Proc $fakeOwned -Directory $target) `
        '只把映像位于目标 dist 的进程识别为使用者'
    Assert-True (-not (Test-ProcessReferencesDirectory -Proc $fakeExternal -Directory $target)) `
        'Docker/外部同名进程不冒充本工作区 dist 使用者'

    $script:FakePackageConsumers = @($fakeOwned)
    function Get-PackageDirectoryConsumers {
        param([string]$Component, [string]$TargetDirectory)
        return @($script:FakePackageConsumers)
    }
    $caught = $null
    try { Publish-StagedPackageDirectory -Component 'redis' -StagedDirectory $staged -TargetDirectory $target -ExpectedSha256 $currentHash }
    catch { $caught = $_.Exception.Message }
    Assert-True ($caught -like 'PANDORA_EXPECTED_FAIL:*local_infra.ps1 -Action down*') `
        '运行中的本工作区组件 fail-closed，并明确提示先 down'
    Assert-True ((Test-Path -LiteralPath (Join-Path $target 'old.txt')) -and (Test-Path -LiteralPath (Join-Path $staged 'new.txt'))) `
        '运行中拒绝发布时旧目录和 staging 都未被覆盖'

    $script:FakePackageConsumers = @()
    Publish-StagedPackageDirectory -Component 'redis' -StagedDirectory $staged -TargetDirectory $target -ExpectedSha256 $currentHash
    Assert-True ((Test-Path -LiteralPath (Join-Path $target 'new.txt')) -and -not (Test-Path -LiteralPath (Join-Path $target 'old.txt'))) `
        '停止后通过目录 swap 发布完整 staging'
    Assert-True (Test-PackageMarker -Directory $target -ExpectedSha256 $currentHash) `
        'swap 后 current marker 与 staging 一起生效'

    # 第二次 rename 注入失败：旧目录必须从 backup 原样回滚，不能留下半发布目标。
    Remove-Item -LiteralPath $target -Recurse -Force
    New-Item -ItemType Directory -Force -Path $target, $staged | Out-Null
    [IO.File]::WriteAllText((Join-Path $target 'old.txt'), 'old')
    [IO.File]::WriteAllText((Join-Path $staged 'new.txt'), 'new')
    Write-PackageMarker -Directory $target -Sha256 $oldHash
    Write-PackageMarker -Directory $staged -Sha256 $currentHash
    $script:MoveDirectoryCalls = 0
    function Move-PackageDirectoryAtomic {
        param([string]$Source, [string]$Destination)
        $script:MoveDirectoryCalls++
        if ($script:MoveDirectoryCalls -eq 2) { throw 'PANDORA_INJECTED_SWAP_FAILURE' }
        [IO.Directory]::Move($Source, $Destination)
    }
    $caught = $null
    try { Publish-StagedPackageDirectory -Component 'redis' -StagedDirectory $staged -TargetDirectory $target -ExpectedSha256 $currentHash }
    catch { $caught = $_.Exception.Message }
    Assert-True ($caught -like '*PANDORA_INJECTED_SWAP_FAILURE*') '目录 swap 失败向上报告，不假装成功'
    Assert-True ((Test-Path -LiteralPath (Join-Path $target 'old.txt')) -and -not (Test-Path -LiteralPath (Join-Path $target 'new.txt'))) `
        '目录 swap 失败后旧版本已回滚到原路径'
    Assert-True (Test-PackageMarker -Directory $target -ExpectedSha256 $oldHash) `
        '回滚后旧 marker 仍与旧目录一致'
    Assert-True (@(Get-ChildItem -LiteralPath $publishSandbox -Directory -Filter '*.pandora-backup-*').Count -eq 0) `
        '回滚成功后不残留孤立 backup'

    # 连回滚 rename 都失败时，backup 是唯一完整旧版本，必须保留供人工恢复，finally 不能误删。
    $script:MoveDirectoryCalls = 0
    function Move-PackageDirectoryAtomic {
        param([string]$Source, [string]$Destination)
        $script:MoveDirectoryCalls++
        if ($script:MoveDirectoryCalls -in @(2, 3)) { throw "PANDORA_INJECTED_MOVE_FAILURE_$script:MoveDirectoryCalls" }
        [IO.Directory]::Move($Source, $Destination)
    }
    $caught = $null
    try { Publish-StagedPackageDirectory -Component 'redis' -StagedDirectory $staged -TargetDirectory $target -ExpectedSha256 $currentHash }
    catch { $caught = $_.Exception.Message }
    $preservedBackups = @(Get-ChildItem -LiteralPath $publishSandbox -Directory -Filter '*.pandora-backup-*')
    Assert-True ($caught -like '*旧目录回滚失败*不要启动服务*') `
        '回滚也失败时给出禁止启动和人工恢复提示'
    Assert-True ($preservedBackups.Count -eq 1 -and (Test-Path -LiteralPath (Join-Path $preservedBackups[0].FullName 'old.txt'))) `
        '回滚也失败时完整旧目录保留在 backup'
    Assert-True (Test-PackageMarker -Directory $preservedBackups[0].FullName -ExpectedSha256 $oldHash) `
        '回滚也失败时 backup 的旧 marker 未丢失'
} finally {
    if (Test-Path -LiteralPath $publishSandbox) {
        Remove-Item -LiteralPath $publishSandbox -Recurse -Force -ErrorAction SilentlyContinue
    }
}

$consumerText = Get-InfraFunctionText 'Get-PackageDirectoryConsumers'
Assert-True ($consumerText -match "'jre'.*'kafka'" -or $consumerText -match "'kafka'.*'jre'") `
    'JRE 升级把正在运行的 Kafka 视为消费者'
foreach ($functionName in 'Invoke-ProvisionAll', 'Confirm-MkcertBinary', 'Confirm-EnvoyBinary') {
    $functionText = Get-InfraFunctionText $functionName
    Assert-True ($functionText -notmatch 'Remove-Item\s+-LiteralPath\s+\$target\s+-Recurse') `
        "$functionName 不再先删正式 dist"
}

Write-Host '[4] Envoy 离线时不得先访问 Docker Hub token' -ForegroundColor Cyan
$envoyText = Get-InfraFunctionText 'Confirm-EnvoyBinary'
Assert-True ($envoyText -match '\$PackageMirror\.Path') 'Envoy 预检读取统一安装包来源'
Assert-True ($envoyText -notmatch '\$env:PANDORA_LOCALINFRA_MIRROR') 'Envoy 不再绕开仓库默认安装包目录'
Invoke-Expression $envoyText
$envoySandbox = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-envoy-package-contract-' + [guid]::NewGuid().ToString('N'))
$CacheDir = Join-Path $envoySandbox 'cache'
$DistDir = Join-Path $envoySandbox 'dist'
$bundleDir = Join-Path $envoySandbox 'installers/localinfra'
New-Item -ItemType Directory -Force -Path $CacheDir, $DistDir, $bundleDir | Out-Null
$PackageMirror = [pscustomobject]@{ Path = $bundleDir; Kind = '仓库安装包' }
$EnvoyImageRepo = 'example/envoy'
$EnvoyImageTag = 'v-test'
$EnvoyLayerDigest = 'sha256:' + ('0' * 64)
$EnvoyExeInLayer = 'Files/Program Files/envoy/envoy.exe'
$Force = $false
$script:TokenCalls = 0
$script:ArchiveCalls = 0
function Write-Step([string]$Message) {}
function Write-Ok([string]$Message) {}
function Invoke-RestMethod {
    param([string]$Uri, [int]$TimeoutSec)
    $script:TokenCalls++
    return [pscustomobject]@{ token = 'stub-token' }
}
function Get-Archive {
    param([string]$File, [string[]]$Urls, [string]$Sha256, [hashtable]$Headers = @{})
    $script:ArchiveCalls++
    throw 'PANDORA_ARCHIVE_PROBE_STOP'
}
try {
    $layerName = "envoy-windows-$EnvoyImageTag-layer.tar.gz"
    $layerPath = Join-Path $bundleDir $layerName
    [System.IO.File]::WriteAllText($layerPath, 'fixture')
    $caught = $null
    try { Confirm-EnvoyBinary } catch { $caught = $_.Exception.Message }
    Assert-True ($caught -eq 'PANDORA_ARCHIVE_PROBE_STOP') 'Envoy 已走到统一 Get-Archive seam'
    Assert-True ($script:ArchiveCalls -eq 1) '本地 layer 存在时 Get-Archive 调用一次'
    Assert-True ($script:TokenCalls -eq 0) '本地 layer 存在时 Docker Hub token 调用为 0'

    Remove-Item -LiteralPath $layerPath -Force
    $script:TokenCalls = 0
    $script:ArchiveCalls = 0
    $caught = $null
    try { Confirm-EnvoyBinary } catch { $caught = $_.Exception.Message }
    Assert-True ($caught -eq 'PANDORA_ARCHIVE_PROBE_STOP') 'Envoy 缺包联网后仍进入 Get-Archive'
    Assert-True ($script:TokenCalls -eq 1) '本地 layer 缺失时才请求一次 Docker Hub token'
    Assert-True ($script:ArchiveCalls -eq 1) '拿到 token 后只调用一次 Get-Archive'
}
finally {
    if (Test-Path -LiteralPath $envoySandbox) { Remove-Item -LiteralPath $envoySandbox -Recurse -Force -ErrorAction SilentlyContinue }
}

Write-Host '[5] Git 空目录 / SVN 完整目录契约' -ForegroundColor Cyan
Assert-True (Test-Path -LiteralPath (Join-Path $InstallerDir 'README.md')) '安装包目录在 Git 中由 README 占位'
$gitignore = [System.IO.File]::ReadAllText((Join-Path $ProjectRoot '.gitignore'))
Assert-True ($gitignore -match '(?m)^/installers/localinfra/\*$') 'Git 忽略安装包 payload'
Assert-True ($gitignore -match '(?m)^!/installers/localinfra/README\.md$') 'Git 只放行安装包目录说明'
$bootstrapText = [System.IO.File]::ReadAllText($Bootstrap)
Assert-True ($bootstrapText -match 'installers\\localinfra') 'pwsh 自举也自动读取仓库安装包目录'

# 从运行时代码/pin提取期望值，测试里不再手抄第二份版本和 SHA。
$componentsAssign = @($ast.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.AssignmentStatementAst] -and $node.Left.Extent.Text -eq '$Components'
}, $true))
if ($componentsAssign.Count -ne 1) { throw "找不到唯一的 `$Components 赋值(找到 $($componentsAssign.Count) 个)" }
Invoke-Expression $componentsAssign[0].Extent.Text
Invoke-Expression (Get-InfraFunctionText 'Find-ToolInDirectory')
Invoke-Expression (Get-InfraFunctionText 'Find-Tool')

Write-Host '[5] 已安装工具按固定路径快速探测' -ForegroundColor Cyan
$PlannerFastStart = $true
$toolSandbox = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-tool-fast-path-' + [guid]::NewGuid().ToString('N'))
$DistDir = Join-Path $toolSandbox 'dist'
try {
    foreach ($componentName in $Components.Keys) {
        foreach ($tool in $Components[$componentName].Tools.GetEnumerator()) {
            $toolPath = Join-Path (Join-Path $DistDir $componentName) $tool.Value
            New-Item -ItemType Directory -Force -Path (Split-Path -Parent $toolPath) | Out-Null
            [IO.File]::WriteAllText($toolPath, 'fixture')
            Assert-True ((Find-Tool $componentName $tool.Key) -eq [IO.Path]::GetFullPath($toolPath)) `
                "$componentName/$($tool.Key) 按固定相对路径命中"
        }
    }

    $jreExpected = Join-Path (Join-Path $DistDir 'jre') $Components.jre.Tools['java.exe']
    Remove-Item -LiteralPath $jreExpected -Force
    $decoy = Join-Path (Join-Path $DistDir 'jre') '任意深层目录\java.exe'
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $decoy) | Out-Null
    [IO.File]::WriteAllText($decoy, 'decoy')
    Assert-True (-not (Find-Tool 'jre' 'java.exe')) '固定入口缺失时不递归扫描整个 JRE，也不误用同名文件'
    $PlannerFastStart = $false
    Assert-True ((Find-Tool 'jre' 'java.exe') -eq [IO.Path]::GetFullPath($decoy)) `
        '非策划极速入口保持原来的递归探测行为'
    $PlannerFastStart = $true
}
finally {
    if (Test-Path -LiteralPath $toolSandbox) { Remove-Item -LiteralPath $toolSandbox -Recurse -Force -ErrorAction SilentlyContinue }
}
foreach ($varName in 'MkcertFile', 'MkcertSha256', 'EnvoyImageTag', 'EnvoyLayerDigest') {
    $assign = @($ast.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.AssignmentStatementAst] -and $node.Left.Extent.Text -eq ('$' + $varName)
    }, $true))
    if ($assign.Count -ne 1) { throw "找不到唯一的 `$${varName} 赋值(找到 $($assign.Count) 个)" }
    Invoke-Expression $assign[0].Extent.Text
}
$pin = @{}
foreach ($line in [System.IO.File]::ReadAllLines($PinFile)) {
    $text = $line.Trim()
    if (-not $text -or $text.StartsWith('#')) { continue }
    $split = $text.IndexOf('=')
    if ($split -gt 0) { $pin[$text.Substring(0, $split)] = $text.Substring($split + 1) }
}

$expected = [ordered]@{}
foreach ($component in $Components.Values) { $expected[$component.File] = $component.Sha256.ToLowerInvariant() }
$expected[$MkcertFile] = $MkcertSha256.ToLowerInvariant()
$expected["envoy-windows-$EnvoyImageTag-layer.tar.gz"] = ($EnvoyLayerDigest -replace '^sha256:', '').ToLowerInvariant()
$expected[$pin.PWSH_FILE] = $pin.PWSH_SHA256.ToLowerInvariant()
Assert-True ($expected.Count -eq 7) '运行时固定清单恰好包含 7 个第三方包'
$infraText = [IO.File]::ReadAllText($Infra)
foreach ($functionName in 'Get-PlannerPackageSetFingerprint', 'Test-PlannerPackageSetReady', 'Write-PlannerPackageSetReceipt') {
    Assert-True ($infraText -match "function\s+$functionName\b") "存在整套安装收据函数:$functionName"
}
Assert-True ($infraText -match 'Test-PlannerPackageSetReady[\s\S]*?极速跳过备料检查') '备料入口命中总收据时整段快速返回'
Assert-True ($infraText -match 'Confirm-EnvoyBinary\s*\r?\n\s*Write-PlannerPackageSetReceipt') '只有全部组件完成后才写总收据'
$plannerCmd = [IO.File]::ReadAllText((Join-Path $ProjectRoot '策划一键启动-免Docker-测试版.cmd'))
Assert-True ($plannerCmd -match 'set "PANDORA_PLANNER_FAST_START=1"') '只有策划免 Docker 双击入口显式开启安装极速路径'
$installerReadme = [System.IO.File]::ReadAllText((Join-Path $InstallerDir 'README.md'))
$documentedFiles = @([regex]::Matches($installerReadme, '(?m)^\|[^|]+\|\s*`([^`]+)`\s*\|\s*$') |
    ForEach-Object { $_.Groups[1].Value } | Sort-Object -Unique)
$expectedFiles = @($expected.Keys | Sort-Object)
Assert-True (($documentedFiles -join "`n") -eq ($expectedFiles -join "`n")) `
    ('README 的 7 个文件与运行时 pin 精确一致' + $(if (($documentedFiles -join "`n") -ne ($expectedFiles -join "`n")) {
        ";README=$($documentedFiles -join ',');pin=$($expectedFiles -join ',')"
    }))

$payloads = @(Get-ChildItem -LiteralPath $InstallerDir -File -ErrorAction SilentlyContinue | Where-Object Name -ne 'README.md')
if ($payloads.Count -eq 0) {
    Write-Host '  [ok] Git 形态无二进制 payload；运行时将逐项联网' -ForegroundColor Green
}
else {
    $unexpected = @($payloads | Where-Object { -not $expected.Contains($_.Name) })
    Assert-True ($unexpected.Count -eq 0) ('SVN 目录不含旧版/未知包' + $(if ($unexpected.Count) { ':' + ($unexpected.Name -join ',') }))
    Assert-True ($payloads.Count -eq $expected.Count) "SVN 形态包含完整 $($expected.Count) 包(实际 $($payloads.Count))"
    foreach ($entry in $expected.GetEnumerator()) {
        $path = Join-Path $InstallerDir $entry.Key
        Assert-True (Test-Path -LiteralPath $path -PathType Leaf) "SVN 包存在:$($entry.Key)"
        if (Test-Path -LiteralPath $path -PathType Leaf) {
            Assert-True (Test-FileSha256 -Path $path -Expected $entry.Value) "SVN 包哈希匹配:$($entry.Key)"
        }
    }
}

if ($script:Failures.Count -gt 0) {
    Write-Host "`n[ERR ] 安装包契约失败 $($script:Failures.Count) 项。" -ForegroundColor Red
    exit 1
}
Write-Host "`n[PASS] 免 Docker SVN 安装包 / Git 联网回退契约" -ForegroundColor Green
