# 中心 MySQL 服务运行态 YAML 渲染契约。
#
# 读取仓库真实 13 份 MySQL dev 配置，在临时私有目录渲染后逐条验收；不启动服务/数据库。

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ScriptsDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptsDir '../..')).Path
$RendererLib = Join-Path $ScriptsDir 'lib/mysql_service_runtime_config.ps1'
$StateLib = Join-Path $ScriptsDir 'lib/local_infra_state.ps1'
$script:Failures = [Collections.Generic.List[string]]::new()
function Assert-True([bool]$Condition, [string]$Message) {
    if ($Condition) { Write-Host "  [ok] $Message" -ForegroundColor Green }
    else { $script:Failures.Add($Message); Write-Host "  [FAIL] $Message" -ForegroundColor Red }
}
if (-not (Test-Path -LiteralPath $RendererLib -PathType Leaf)) {
    Write-Host "[FAIL] 缺少中心 MySQL 服务配置 renderer:$RendererLib" -ForegroundColor Red
    exit 1
}
. $RendererLib
. $StateLib

$sandbox = Join-Path ([IO.Path]::GetTempPath()) ("pandora-mysql-config-{0}-{1}" -f $PID, [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $sandbox | Out-Null
try {
    $workspaceId = '01jabcdefghjkmnpqrstvwxyz0'
    $identityPath = Join-Path $sandbox 'identity.json'
    $profilePath = Join-Path $sandbox 'profile.json'
    $credentialRoot = Join-Path $sandbox 'credentials'
    $outputRoot = Join-Path $sandbox 'runtime-yaml'
    $caPath = Join-Path $sandbox 'planner-db-ca.pem'
    [IO.File]::WriteAllText($caPath, "fixture-ca`n", [Text.UTF8Encoding]::new($false))
    Set-PandoraPlannerDbIdentity -WorkspaceId $workspaceId -IdentityPath $identityPath | Out-Null
    $profile = Publish-PandoraMysqlRuntimeProfile -ProjectRoot $RepoRoot -Mode central-managed `
        -Endpoint ([ordered]@{ host = 'pandora-planner-db.intra'; port = 3306; tls_server_name = 'pandora-planner-db.intra'; ca_file = $caPath }) `
        -CredentialRef ([ordered]@{ provider = 'dpapi-current-user'; target = "Pandora/PlannerDB/$workspaceId/app"; version = 5 }) `
        -IdentityPath $identityPath -OutputPath $profilePath -ComputerName 'PLAN-PC' -UserName 'planner'
    $password = ConvertTo-SecureString 'PANDORA_RUNTIME_PASSWORD_123456' -AsPlainText -Force
    Save-PandoraPlannerDbCredential -WorkspaceId $workspaceId -Target "Pandora/PlannerDB/$workspaceId/app" `
        -UserName "p_app_$workspaceId" -Password $password -Version 5 -CredentialRoot $credentialRoot | Out-Null
    $credential = Get-PandoraPlannerDbCredential -WorkspaceId $workspaceId `
        -Target "Pandora/PlannerDB/$workspaceId/app" -CredentialRoot $credentialRoot

    $sourceFiles = @(Get-ChildItem -LiteralPath (Join-Path $RepoRoot 'services') -Recurse -File -Filter '*-dev.yaml' |
        Where-Object { [IO.File]::ReadAllText($_.FullName) -match '@tcp\(127\.0\.0\.1:3307\)/pandora_' } |
        Sort-Object FullName)
    $beforeHashes = @{}
    foreach ($source in $sourceFiles) {
        $beforeHashes[$source.FullName] = (Get-FileHash -LiteralPath $source.FullName -Algorithm SHA256).Hash
    }

    Write-Host '[1] 真实 13 份配置的 14 条 DSN 全量映射到同一 workspace' -ForegroundColor Cyan
    $results = @()
    foreach ($source in $sourceFiles) {
        $serviceName = Split-Path (Split-Path $source.DirectoryName -Parent) -Leaf
        $results += New-PandoraMysqlServiceRuntimeConfig -ProjectRoot $RepoRoot -ServiceName $serviceName `
            -SourcePath $source.FullName -Profile $profile -Credential $credential -OutputDirectory $outputRoot
    }
    Assert-True ($sourceFiles.Count -eq 13) '真实仓库恰有 13 份本机 MySQL 服务配置'
    Assert-True ((@($results | Measure-Object -Property DsnCount -Sum).Sum) -eq 14) '14 条 DSN 一条不漏'
    $renderedText = @($results | ForEach-Object { Get-Content -LiteralPath $_.Path -Raw -Encoding utf8 }) -join "`n"
    foreach ($set in @(Get-PandoraMysqlMigrationSets -ProjectRoot $RepoRoot)) {
        $sourceUsesSet = @($sourceFiles | Where-Object { [IO.File]::ReadAllText($_.FullName) -match "/$([regex]::Escape($set))\?" }).Count -gt 0
        if ($sourceUsesSet) {
            Assert-True ($renderedText -match "/$([regex]::Escape("${set}_w_$workspaceId"))\?") "$set 映射到 exact workspace 物理库"
        }
    }
    Assert-True (@([regex]::Matches($renderedText, "(?m)^\s+tls_ca_file:\s+'" )).Count -eq 14) '每个 MySQL 连接块显式注入 CA'
    Assert-True (@([regex]::Matches($renderedText, "(?m)^\s+tls_server_name:\s+'pandora-planner-db\.intra'" )).Count -eq 14) '每个 MySQL 连接块显式注入证书名'
    Assert-True (@([regex]::Matches($renderedText, '(?m)^\s+max_open_conns:\s+4\s*$')).Count -eq 14) '中心模式每条池上限固定为 4'
    Assert-True (@([regex]::Matches($renderedText, '(?m)^\s+max_idle_conns:\s+1\s*$')).Count -eq 14) `
        'Go 每条空闲保留上限固定为 1（Python 不把它冒充 minsize）'
    Assert-True ($renderedText -notmatch '@tcp\(127\.0\.0\.1:3307\)' -and $renderedText -notmatch 'pandora_dev_pwd') '不残留本机 endpoint 或 dev 弱口令'
    Assert-True ($renderedText -notmatch '/pandora_(?:account|player|social|battle|trade|auction|leaderboard|bag|owner|mission)\?') '不残留 canonical 公共库名'

    Write-Host '[2] secret YAML ACL 私有，源配置零改动，用后可精确清理' -ForegroundColor Cyan
    $private = $true
    foreach ($result in $results) {
        $acl = Get-Acl -LiteralPath $result.Path
        if (-not $acl.AreAccessRulesProtected) { $private = $false }
        $broad = @($acl.Access | Where-Object {
            $sid = try { $_.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value } catch { '' }
            $sid -in @('S-1-1-0', 'S-1-5-11', 'S-1-5-32-545') -and $_.AccessControlType -eq 'Allow'
        })
        if ($broad.Count -gt 0) { $private = $false }
    }
    Assert-True $private '所有含密码 YAML 仅当前 Windows 用户可读'
    $sourcesUnchanged = $true
    foreach ($source in $sourceFiles) {
        if ((Get-FileHash -LiteralPath $source.FullName -Algorithm SHA256).Hash -cne $beforeHashes[$source.FullName]) { $sourcesUnchanged = $false }
    }
    Assert-True $sourcesUnchanged '仓库 YAML 一个字节不改'
    $lockedResult = $results[0]
    $secretLock = [IO.File]::Open($lockedResult.Path, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read)
    $lockedCleanupBlocked = $false
    try { Remove-PandoraMysqlServiceRuntimeConfig -RuntimeConfig $lockedResult } catch { $lockedCleanupBlocked = $true }
    Assert-True ($lockedCleanupBlocked -and (Test-Path -LiteralPath $lockedResult.Path)) '明文 YAML 被锁无法删除时 fail-closed，不误报启动成功'
    $secretLock.Dispose()
    Remove-PandoraMysqlServiceRuntimeConfig -RuntimeConfig $lockedResult
    foreach ($result in @($results | Select-Object -Skip 1)) { Remove-PandoraMysqlServiceRuntimeConfig -RuntimeConfig $result }
    Assert-True (@(Get-ChildItem -LiteralPath $outputRoot -Recurse -File -ErrorAction SilentlyContinue).Count -eq 0) '服务读完后 secret YAML 全部删除'

    Write-Host '[3] 无 MySQL 服务不生成秘密文件，profile/credential 漂移 fail-closed' -ForegroundColor Cyan
    $dialogue = Join-Path $RepoRoot 'services/social/dialogue/etc/dialogue-dev.yaml'
    $plainResult = New-PandoraMysqlServiceRuntimeConfig -ProjectRoot $RepoRoot -ServiceName dialogue `
        -SourcePath $dialogue -Profile $profile -Credential $credential -OutputDirectory $outputRoot
    Assert-True (-not $plainResult.Ephemeral -and $plainResult.Path -ceq [IO.Path]::GetFullPath($dialogue) -and $plainResult.DsnCount -eq 0) `
        '不连 MySQL 的服务继续直接读仓库配置'
    $wrongCredential = $credential.PSObject.Copy()
    $wrongCredential.Version = 6
    $blocked = $false
    try {
        New-PandoraMysqlServiceRuntimeConfig -ProjectRoot $RepoRoot -ServiceName login `
            -SourcePath (Join-Path $RepoRoot 'services/account/login/etc/login-dev.yaml') `
            -Profile $profile -Credential $wrongCredential -OutputDirectory $outputRoot | Out-Null
    } catch { $blocked = $true }
    Assert-True $blocked '凭据版本与 profile fingerprint 不一致时拒绝生成'

    $badSource = Join-Path $RepoRoot 'run/localinfra/cfg/test-unsafe-dsn.yaml'
    New-Item -ItemType Directory -Path (Split-Path -Parent $badSource) -Force | Out-Null
    try {
        [IO.File]::WriteAllText($badSource, "node:`n  mysql_client:`n    dsn: 'pandora:x@tcp(127.0.0.1:3307)/unknown_db?tls=skip-verify'`n", [Text.UTF8Encoding]::new($false))
        $blocked = $false
        try {
            New-PandoraMysqlServiceRuntimeConfig -ProjectRoot $RepoRoot -ServiceName unsafe `
                -SourcePath $badSource -Profile $profile -Credential $credential -OutputDirectory $outputRoot | Out-Null
        } catch { $blocked = $true }
        Assert-True $blocked '未知库或源 DSN 自带 TLS 逃生参数时拒绝'
    } finally {
        Remove-Item -LiteralPath $badSource -Force -ErrorAction SilentlyContinue
    }

    Write-Host '[4] renderer 任一步失败都清理本轮明文 session' -ForegroundColor Cyan
    $failureOutputRoot = Join-Path $sandbox 'runtime-yaml-failure'
    $realSetPrivateAcl = ${function:Set-PandoraPlannerPrivateAcl}
    try {
        function Set-PandoraPlannerPrivateAcl {
            param([Parameter(Mandatory)][string]$Path, [switch]$Directory)
            if (-not $Directory -and [IO.Path]::GetExtension($Path) -ceq '.yaml') {
                throw 'fixture file ACL failure'
            }
            & $realSetPrivateAcl -Path $Path -Directory:$Directory
        }
        $creationBlocked = $false
        try {
            New-PandoraMysqlServiceRuntimeConfig -ProjectRoot $RepoRoot -ServiceName login `
                -SourcePath (Join-Path $RepoRoot 'services/account/login/etc/login-dev.yaml') `
                -Profile $profile -Credential $credential -OutputDirectory $failureOutputRoot | Out-Null
        } catch { $creationBlocked = $_.Exception.Message -match 'ACL|清理|fixture' }
        Assert-True ($creationBlocked -and
            @(Get-ChildItem -LiteralPath $failureOutputRoot -Recurse -File -ErrorAction SilentlyContinue).Count -eq 0) `
            '写入后设置文件 ACL 失败时不遗留含明文 DSN 的本轮文件'
    } finally {
        Set-Item -LiteralPath function:Set-PandoraPlannerPrivateAcl -Value $realSetPrivateAcl
    }

    Write-Host '[5] 只在真实项目编排锁内 fail-closed 扫除历史 secret session' -ForegroundColor Cyan
    $sweepProject = Join-Path $sandbox 'sweep-project'
    $serviceSecretRoot = Join-Path $sweepProject 'run/localinfra/cfg/service-secrets'
    $preflightSecretRoot = Join-Path $sweepProject 'run/localinfra/cfg/preflight-secrets'
    New-Item -ItemType Directory -Path $serviceSecretRoot -Force | Out-Null
    New-Item -ItemType Directory -Path $preflightSecretRoot -Force | Out-Null
    $unlockedSession = Join-Path $serviceSecretRoot '101-login-0123456789abcdef0123456789abcdef'
    New-Item -ItemType Directory -Path $unlockedSession | Out-Null
    [IO.File]::WriteAllText((Join-Path $unlockedSession 'login.yaml'), 'fixture-secret')
    $sweepWithoutLockBlocked = $false
    try { Invoke-PandoraPlannerSecretSessionSweep -ProjectRoot $sweepProject }
    catch { $sweepWithoutLockBlocked = $_.Exception.Message -match '编排锁|lock' }
    Assert-True ($sweepWithoutLockBlocked -and (Test-Path -LiteralPath $unlockedSession)) `
        '未持有当前项目真实编排锁时拒绝触碰旧 secret'

    $lockHeld = $false
    $crashProcess = $null
    $lockedStream = $null
    $junction = ''
    try {
        Enter-PandoraOrchestrationLock -ProjectRoot $sweepProject -Operation 'secret sweep contract'
        $lockHeld = $true
        $preflightSession = Join-Path $preflightSecretRoot '102-fedcba9876543210fedcba9876543210'
        New-Item -ItemType Directory -Path $preflightSession | Out-Null
        [IO.File]::WriteAllText((Join-Path $preflightSession 'pandora_account.dsn'), 'fixture-secret')
        Invoke-PandoraPlannerSecretSessionSweep -ProjectRoot $sweepProject
        Assert-True (-not (Test-Path -LiteralPath $unlockedSession) -and -not (Test-Path -LiteralPath $preflightSession)) `
            '锁内同时清扫 service/preflight 两类合法旧 session'

        $readyFile = Join-Path $sweepProject 'crash-ready.txt'
        $env:PANDORA_SECRET_CRASH_ROOT = $serviceSecretRoot
        $env:PANDORA_SECRET_CRASH_READY = $readyFile
        $crashSource = @'
$root = $env:PANDORA_SECRET_CRASH_ROOT
$session = Join-Path $root ("{0}-login-{1}" -f $PID, [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $session -Force | Out-Null
[IO.File]::WriteAllText((Join-Path $session 'login.yaml'), 'fixture-crash-secret')
[IO.File]::WriteAllText($env:PANDORA_SECRET_CRASH_READY, $session)
Start-Sleep -Seconds 120
'@
        $encodedCrashSource = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($crashSource))
        $crashProcess = Start-Process -FilePath (Get-Command pwsh).Source `
            -ArgumentList @('-NoLogo', '-NoProfile', '-EncodedCommand', $encodedCrashSource) `
            -PassThru -WindowStyle Hidden
        $deadline = [DateTime]::UtcNow.AddSeconds(10)
        while (-not (Test-Path -LiteralPath $readyFile) -and [DateTime]::UtcNow -lt $deadline) {
            Start-Sleep -Milliseconds 50
        }
        Assert-True (Test-Path -LiteralPath $readyFile) '动态子进程已真实写出明文 session'
        if (Test-Path -LiteralPath $readyFile) {
            $crashSession = [IO.File]::ReadAllText($readyFile)
            Stop-Process -Id $crashProcess.Id -Force -ErrorAction Stop
            $crashProcess.WaitForExit(5000) | Out-Null
            Invoke-PandoraPlannerSecretSessionSweep -ProjectRoot $sweepProject
            Assert-True (-not (Test-Path -LiteralPath $crashSession)) '强杀进程遗留的明文 session 在下一轮生成前被清除'
        }

        $lockedSession = Join-Path $serviceSecretRoot '103-login-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        $otherSession = Join-Path $serviceSecretRoot '104-login-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
        New-Item -ItemType Directory -Path $lockedSession -Force | Out-Null
        New-Item -ItemType Directory -Path $otherSession -Force | Out-Null
        $lockedFile = Join-Path $lockedSession 'login.yaml'
        [IO.File]::WriteAllText($lockedFile, 'fixture-locked-secret')
        [IO.File]::WriteAllText((Join-Path $otherSession 'login.yaml'), 'fixture-other-secret')
        $lockedStream = [IO.File]::Open($lockedFile, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::None)
        $aggregateBlocked = $false
        try { Invoke-PandoraPlannerSecretSessionSweep -ProjectRoot $sweepProject }
        catch { $aggregateBlocked = $_.Exception.Message -match '103-login|清理失败|删除失败' }
        Assert-True ($aggregateBlocked -and (Test-Path -LiteralPath $lockedSession) -and -not (Test-Path -LiteralPath $otherSession)) `
            '逐项聚合删除失败且继续清除其余 session，最终 fail-closed'
        $lockedStream.Dispose(); $lockedStream = $null
        Invoke-PandoraPlannerSecretSessionSweep -ProjectRoot $sweepProject

        $external = Join-Path $sweepProject 'external-sentinel'
        New-Item -ItemType Directory -Path $external -Force | Out-Null
        $sentinel = Join-Path $external 'keep.txt'
        [IO.File]::WriteAllText($sentinel, 'must-stay')
        New-Item -ItemType Directory -Path $serviceSecretRoot -Force | Out-Null
        $junction = Join-Path $serviceSecretRoot '105-login-cccccccccccccccccccccccccccccccc'
        New-Item -ItemType Junction -Path $junction -Target $external | Out-Null
        $reparseBlocked = $false
        try { Invoke-PandoraPlannerSecretSessionSweep -ProjectRoot $sweepProject }
        catch { $reparseBlocked = $_.Exception.Message -match 'reparse|越界|链接' }
        Assert-True ($reparseBlocked -and (Test-Path -LiteralPath $sentinel)) '拒绝沿 reparse point 越界删除外部内容'
    } finally {
        if ($lockedStream) { $lockedStream.Dispose() }
        if ($crashProcess -and -not $crashProcess.HasExited) {
            Stop-Process -Id $crashProcess.Id -Force -ErrorAction SilentlyContinue
            $crashProcess.WaitForExit(5000) | Out-Null
        }
        Remove-Item Env:PANDORA_SECRET_CRASH_ROOT -ErrorAction SilentlyContinue
        Remove-Item Env:PANDORA_SECRET_CRASH_READY -ErrorAction SilentlyContinue
        if ($lockHeld) { Exit-PandoraOrchestrationLock }
        if (-not [string]::IsNullOrWhiteSpace($junction) -and (Test-Path -LiteralPath $junction)) {
            Remove-Item -LiteralPath $junction -Force -ErrorAction SilentlyContinue
        }
    }
} finally {
    Remove-Item -LiteralPath $sandbox -Recurse -Force -ErrorAction SilentlyContinue
}

if ($script:Failures.Count -gt 0) {
    Write-Host ''
    Write-Host "[FAIL] MySQL 服务运行态配置契约失败 $($script:Failures.Count) 项" -ForegroundColor Red
    exit 1
}
Write-Host ''
Write-Host '[PASS] MySQL 服务运行态 YAML 契约' -ForegroundColor Green
