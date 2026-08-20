# MySQL 运行态 profile 公共 seam 契约。
#
# 测试只使用临时目录，不访问真实 Credential Manager、MySQL 或用户 identity。

$ErrorActionPreference = 'Stop'
Set-StrictMode -Off

$ScriptsDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptsDir '../..')).Path
$ProfileLib = Join-Path $ScriptsDir 'lib/mysql_runtime_profile.ps1'
if (-not (Test-Path -LiteralPath $ProfileLib -PathType Leaf)) {
    throw "缺少 MySQL 运行态 profile 公共库:$ProfileLib"
}
. $ProfileLib
$strictModeLeaked = $false
try { $null = $pandoraMysqlProfileUndefinedVariableProbe } catch { $strictModeLeaked = $true }
if ($strictModeLeaked) { throw 'mysql_runtime_profile.ps1 不得改变 dot-source 调用方的 StrictMode' }
Set-StrictMode -Version Latest

$script:Failures = [Collections.Generic.List[string]]::new()
function Assert-True([bool]$Condition, [string]$Message) {
    if ($Condition) { Write-Host "  [ok] $Message" -ForegroundColor Green }
    else { $script:Failures.Add($Message); Write-Host "  [FAIL] $Message" -ForegroundColor Red }
}

$sandbox = Join-Path ([IO.Path]::GetTempPath()) ("pandora-mysql-profile-{0}" -f [guid]::NewGuid().ToString('N'))
$identityPath = Join-Path $sandbox 'user/identity.json'
$workspaceId = '0123456789abcdefghjkmnpqrs'
$profilePath = Join-Path $sandbox 'repo/run/localinfra/cfg/mysql-runtime-profile.json'
$caPath = Join-Path $sandbox 'certs/planner-db-ca.pem'
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $identityPath) | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $caPath) | Out-Null
Set-Content -LiteralPath $caPath -Value 'test-only-ca-fixture' -Encoding ascii -NoNewline
New-Item -ItemType Directory -Force -Path (Join-Path $sandbox 'repo/tools/migrate/migrations/pandora_account') | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $sandbox 'repo/tools/migrate/migrations/pandora_player') | Out-Null

try {
    Write-Host '[1] 中心分配的 identity 只允许首次绑定' -ForegroundColor Cyan
    $identity = Set-PandoraPlannerDbIdentity -WorkspaceId $workspaceId -IdentityPath $identityPath
    $firstBytes = [IO.File]::ReadAllBytes($identityPath)
    $sameIdentity = Set-PandoraPlannerDbIdentity -WorkspaceId $workspaceId -IdentityPath $identityPath
    Assert-True ($identity.workspace_id -ceq $workspaceId -and $sameIdentity.workspace_id -ceq $workspaceId) '同一中心 workspace_id 可幂等重试'
    Assert-True ([Convert]::ToHexString([IO.File]::ReadAllBytes($identityPath)) -ceq [Convert]::ToHexString($firstBytes)) '幂等重试不改写长期 identity'

    $replacementBlocked = $false
    try {
        Set-PandoraPlannerDbIdentity -WorkspaceId '1123456789abcdefghjkmnpqrs' -IdentityPath $identityPath | Out-Null
    } catch { $replacementBlocked = $_.Exception.Message -match '更换|不一致|workspace' }
    Assert-True $replacementBlocked '不同 workspace_id 不能覆盖已绑定 identity'
    Assert-True ((Get-PandoraPlannerDbIdentity -IdentityPath $identityPath).workspace_id -ceq $workspaceId) '覆盖失败后仍保留原 workspace_id'

    $invalidPath = Join-Path $sandbox 'user/invalid.json'
    $invalidBlocked = $false
    try {
        Set-PandoraPlannerDbIdentity -WorkspaceId 'ADMIN-PC' -IdentityPath $invalidPath | Out-Null
    } catch { $invalidBlocked = $true }
    Assert-True ($invalidBlocked -and -not (Test-Path -LiteralPath $invalidPath)) '用户名/电脑名不能冒充权威 workspace_id'
    foreach ($nonCanonical in @('8123456789abcdefghjkmnpqrs', 'z123456789abcdefghjkmnpqrs')) {
        $nonCanonicalPath = Join-Path $sandbox "user/noncanonical-$($nonCanonical[0]).json"
        $nonCanonicalBlocked = $false
        try { Set-PandoraPlannerDbIdentity -WorkspaceId $nonCanonical -IdentityPath $nonCanonicalPath | Out-Null } catch { $nonCanonicalBlocked = $true }
        Assert-True ($nonCanonicalBlocked -and -not (Test-Path -LiteralPath $nonCanonicalPath)) `
            "128-bit Crockford workspace_id 拒绝首位 $($nonCanonical[0])"
    }

    Write-Host '[1b] identity 首次绑定跨进程原子收敛' -ForegroundColor Cyan
    $identityRunner = Join-Path $sandbox 'identity-runner.ps1'
    @'
param(
    [Parameter(Mandatory)][string]$Library,
    [Parameter(Mandatory)][string]$IdentityPath,
    [Parameter(Mandatory)][string]$WorkspaceId
)
$ErrorActionPreference = 'Stop'
. $Library
try {
    Set-PandoraPlannerDbIdentity -WorkspaceId $WorkspaceId -IdentityPath $IdentityPath | Out-Null
    exit 0
} catch {
    exit 17
}
'@ | Set-Content -LiteralPath $identityRunner -Encoding utf8NoBOM
    $pwshExe = (Get-Command pwsh -ErrorAction Stop).Source
    function Start-IdentityBindProcess([string]$Path, [string]$Id) {
        return Start-Process -FilePath $pwshExe -ArgumentList @(
            '-NoLogo', '-NoProfile', '-File', $identityRunner,
            '-Library', $ProfileLib, '-IdentityPath', $Path, '-WorkspaceId', $Id
        ) -WindowStyle Hidden -PassThru
    }
    $sameRacePath = Join-Path $sandbox 'user/same-race.json'
    $sameA = Start-IdentityBindProcess $sameRacePath $workspaceId
    $sameB = Start-IdentityBindProcess $sameRacePath $workspaceId
    $sameA.WaitForExit(); $sameB.WaitForExit()
    Assert-True ($sameA.ExitCode -eq 0 -and $sameB.ExitCode -eq 0 -and
        (Get-PandoraPlannerDbIdentity -IdentityPath $sameRacePath).workspace_id -ceq $workspaceId) `
        '同 ID 并发 enrollment 两边都幂等成功'

    $differentRacePath = Join-Path $sandbox 'user/different-race.json'
    $otherWorkspaceId = '1123456789abcdefghjkmnpqrs'
    $differentA = Start-IdentityBindProcess $differentRacePath $workspaceId
    $differentB = Start-IdentityBindProcess $differentRacePath $otherWorkspaceId
    $differentA.WaitForExit(); $differentB.WaitForExit()
    $differentExitCodes = @($differentA.ExitCode, $differentB.ExitCode) | Sort-Object
    $raceWinner = (Get-PandoraPlannerDbIdentity -IdentityPath $differentRacePath).workspace_id
    Assert-True (($differentExitCodes -join ',') -ceq '0,17' -and $raceWinner -cin @($workspaceId, $otherWorkspaceId)) `
        '不同 ID 并发 enrollment 只有一个原子赢家'
    Assert-True (@(Get-ChildItem -LiteralPath (Split-Path -Parent $differentRacePath) -Filter 'identity.json.*.tmp').Count -eq 0) `
        '并发 enrollment 不遗留半截 identity 临时文件'

    Write-Host '[2] central-managed profile 映射 workspace 专属物理库' -ForegroundColor Cyan
    $profile = Publish-PandoraMysqlRuntimeProfile `
        -ProjectRoot (Join-Path $sandbox 'repo') `
        -Mode central-managed `
        -Endpoint ([ordered]@{
            host = 'pandora-dev-db.intra'
            port = 3306
            tls_server_name = 'pandora-dev-db.intra'
            ca_file = $caPath
        }) `
        -CredentialRef ([ordered]@{
            provider = 'windows-credential-manager'
            target = "Pandora/planner-db/$workspaceId"
            version = 1
        }) `
        -IdentityPath $identityPath `
        -OutputPath $profilePath `
        -ComputerName 'PLAN-PC-02' `
        -UserName 'planner'

    Assert-True (Test-Path -LiteralPath $profilePath -PathType Leaf) 'profile 原子发布到约定路径'
    Assert-True ($profile.schema_version -eq 1 -and $profile.mode -ceq 'central-managed') 'schema v1 与模式写入正确'
    Assert-True ($profile.workspace_id -ceq $workspaceId) '使用中心登记的稳定 workspace_id'
    Assert-True ($profile.display_name -ceq 'PLAN-PC-02\planner') '显示标签来自电脑名与用户名'
    Assert-True ($profile.databases.pandora_account -ceq "pandora_account_w_$workspaceId") '账号逻辑库映射到 workspace 物理库'
    Assert-True ($profile.databases.pandora_player -ceq "pandora_player_w_$workspaceId") '玩家逻辑库映射到同一 workspace'
    Assert-True ($profile.credential_ref.provider -ceq 'windows-credential-manager') 'profile 只保存 Credential Manager 引用'
    Assert-True ($profile.credential_ref.version -eq 1) 'profile 指纹输入包含凭据版本'
    Assert-True ($profile.fingerprint -cmatch '^sha256:[0-9a-f]{64}$') 'profile 带确定性 SHA-256 指纹'

    $onDisk = Get-Content -LiteralPath $profilePath -Raw -Encoding utf8 | ConvertFrom-Json
    Assert-True ($onDisk.fingerprint -ceq $profile.fingerprint) '返回值与已发布 JSON 完全一致'

    Write-Host '[3] local-owned 保持 canonical 库且不要求 enrollment' -ForegroundColor Cyan
    $localIdentityPath = Join-Path $sandbox 'user/local-mode-must-not-create.json'
    $localProfile = Publish-PandoraMysqlRuntimeProfile `
        -ProjectRoot (Join-Path $sandbox 'repo') `
        -Mode local-owned `
        -Endpoint ([ordered]@{
            host = '127.0.0.1'
            port = 13307
            tls_server_name = ''
            ca_file = ''
        }) `
        -CredentialRef ([ordered]@{
            provider = 'dpapi-current-user'
            target = 'Pandora/local-owned/mysql'
            version = 1
        }) `
        -IdentityPath $localIdentityPath `
        -OutputPath $profilePath `
        -ComputerName 'PLAN-PC-02' `
        -UserName 'planner'
    Assert-True ($localProfile.workspace_id -ceq '') '本机模式 workspace_id 固定为空'
    Assert-True ($localProfile.databases.pandora_account -ceq 'pandora_account' -and
        $localProfile.databases.pandora_player -ceq 'pandora_player') '本机模式逻辑库保持 canonical 映射'
    Assert-True ($localProfile.credential_ref.provider -ceq 'dpapi-current-user') '本机模式也只保存内置安全存储引用'
    Assert-True (-not (Test-Path -LiteralPath $localIdentityPath)) '本机模式不会自行生成假的中心 identity'

    Write-Host '[4] central-managed 的 TLS / 凭据 / inline secret 全部 fail-closed' -ForegroundColor Cyan
    $dpapiProfile = Publish-PandoraMysqlRuntimeProfile `
        -ProjectRoot (Join-Path $sandbox 'repo') -Mode central-managed `
        -Endpoint ([ordered]@{ host = 'pandora-dev-db.intra'; port = 3306; tls_server_name = 'pandora-dev-db.intra'; ca_file = $caPath }) `
        -CredentialRef ([ordered]@{ provider = 'dpapi-current-user'; target = "Pandora/planner-db/$workspaceId"; version = 1 }) `
        -IdentityPath $identityPath -OutputPath $profilePath -ComputerName 'RENAMED-PC' -UserName 'newname'
    Assert-True ($dpapiProfile.workspace_id -ceq $workspaceId -and
        $dpapiProfile.display_name -ceq 'RENAMED-PC\newname') '电脑/用户名改名只更新显示标签，不更换 workspace_id'
    Assert-True ($dpapiProfile.credential_ref.provider -ceq 'dpapi-current-user') '中心模式允许当前用户 DPAPI 引用'

    $beforeRejectedPublishes = [Convert]::ToHexString([IO.File]::ReadAllBytes($profilePath))
    $baseEndpoint = [ordered]@{ host = 'pandora-dev-db.intra'; port = 3306; tls_server_name = 'pandora-dev-db.intra'; ca_file = $caPath }
    $baseCredential = [ordered]@{ provider = 'windows-credential-manager'; target = "Pandora/planner-db/$workspaceId"; version = 1 }
    $oversizedCa = Join-Path $sandbox 'certs/oversized-ca.pem'
    [IO.File]::WriteAllBytes($oversizedCa, [byte[]]::new(1MB + 1))
    $rejectedCases = @(
        @{ Name = '中心 endpoint 拒绝裸 IPv4，必须使用稳定 DNS'; Endpoint = [ordered]@{ host = '192.168.2.10'; port = 3306; tls_server_name = '192.168.2.10'; ca_file = $caPath }; Credential = $baseCredential },
        @{ Name = '中心 CA 超过 1 MiB 拒绝'; Endpoint = [ordered]@{ host = 'pandora-dev-db.intra'; port = 3306; tls_server_name = 'pandora-dev-db.intra'; ca_file = $oversizedCa }; Credential = $baseCredential },
        @{ Name = '证书名不能与 endpoint 分离'; Endpoint = [ordered]@{ host = 'pandora-dev-db.intra'; port = 3306; tls_server_name = 'other.intra'; ca_file = $caPath }; Credential = $baseCredential },
        @{ Name = 'CA 相对路径拒绝'; Endpoint = [ordered]@{ host = 'pandora-dev-db.intra'; port = 3306; tls_server_name = 'pandora-dev-db.intra'; ca_file = '.\ca.pem' }; Credential = $baseCredential },
        @{ Name = '不存在的 CA 拒绝'; Endpoint = [ordered]@{ host = 'pandora-dev-db.intra'; port = 3306; tls_server_name = 'pandora-dev-db.intra'; ca_file = (Join-Path $sandbox 'missing-ca.pem') }; Credential = $baseCredential },
        @{ Name = 'inline provider 拒绝'; Endpoint = $baseEndpoint; Credential = [ordered]@{ provider = 'inline-password'; target = 'pandora_dev_pwd'; version = 1 } },
        @{ Name = 'endpoint password 字段拒绝'; Endpoint = [ordered]@{ host = 'pandora-dev-db.intra'; port = 3306; tls_server_name = 'pandora-dev-db.intra'; ca_file = $caPath; password = 'must-not-leak' }; Credential = $baseCredential },
        @{ Name = 'credential secret 字段拒绝'; Endpoint = $baseEndpoint; Credential = [ordered]@{ provider = 'windows-credential-manager'; target = "Pandora/planner-db/$workspaceId"; version = 1; secret = 'must-not-leak' } },
        @{ Name = 'credential admin 字段拒绝'; Endpoint = $baseEndpoint; Credential = [ordered]@{ provider = 'windows-credential-manager'; target = "Pandora/planner-db/$workspaceId"; version = 1; admin = 'root' } },
        @{ Name = 'credential root 字段拒绝'; Endpoint = $baseEndpoint; Credential = [ordered]@{ provider = 'windows-credential-manager'; target = "Pandora/planner-db/$workspaceId"; version = 1; root = 'yes' } },
        @{ Name = 'credential version 0 拒绝'; Endpoint = $baseEndpoint; Credential = [ordered]@{ provider = 'windows-credential-manager'; target = "Pandora/planner-db/$workspaceId"; version = 0 } }
    )
    foreach ($case in $rejectedCases) {
        $blocked = $false
        try {
            Publish-PandoraMysqlRuntimeProfile `
                -ProjectRoot (Join-Path $sandbox 'repo') -Mode central-managed `
                -Endpoint $case.Endpoint -CredentialRef $case.Credential `
                -IdentityPath $identityPath -OutputPath $profilePath `
                -ComputerName 'PLAN-PC-02' -UserName 'planner' | Out-Null
        } catch { $blocked = $true }
        Assert-True $blocked $case.Name
    }
    Assert-True ([Convert]::ToHexString([IO.File]::ReadAllBytes($profilePath)) -ceq $beforeRejectedPublishes) `
        '所有拒绝分支都发生在原子替换前，保留最后一个有效 profile'

    Write-Host '[5] 消费 profile 时复核 fingerprint / identity / database 映射' -ForegroundColor Cyan
    $loaded = Get-PandoraMysqlRuntimeProfile `
        -ProjectRoot (Join-Path $sandbox 'repo') -ProfilePath $profilePath -IdentityPath $identityPath
    Assert-True ($loaded.fingerprint -ceq $dpapiProfile.fingerprint -and
        $loaded.databases.pandora_account -ceq "pandora_account_w_$workspaceId") '有效 profile 可通过同一公开 seam 读取'

    $beforeCaRotationFingerprint = $dpapiProfile.fingerprint
    [IO.File]::WriteAllText($caPath, 'test-only-ca-fixture-rotated', [Text.UTF8Encoding]::new($false))
    $oldCaProfileBlocked = $false
    try {
        Get-PandoraMysqlRuntimeProfile -ProjectRoot (Join-Path $sandbox 'repo') `
            -ProfilePath $profilePath -IdentityPath $identityPath | Out-Null
    } catch { $oldCaProfileBlocked = $true }
    Assert-True $oldCaProfileBlocked '同路径 CA 内容轮换后旧 profile fail-closed'
    $dpapiProfile = Publish-PandoraMysqlRuntimeProfile `
        -ProjectRoot (Join-Path $sandbox 'repo') -Mode central-managed -Endpoint $baseEndpoint `
        -CredentialRef ([ordered]@{ provider = 'dpapi-current-user'; target = "Pandora/planner-db/$workspaceId"; version = 1 }) `
        -IdentityPath $identityPath -OutputPath $profilePath -ComputerName 'RENAMED-PC' -UserName 'newname'
    Assert-True ($dpapiProfile.fingerprint -cne $beforeCaRotationFingerprint) '重新发布 profile 后 CA 内容变更使指纹漂移'

    $version2Path = Join-Path $sandbox 'repo/run/localinfra/cfg/mysql-runtime-profile-v2.json'
    $version2 = Publish-PandoraMysqlRuntimeProfile `
        -ProjectRoot (Join-Path $sandbox 'repo') -Mode central-managed `
        -Endpoint $baseEndpoint `
        -CredentialRef ([ordered]@{ provider = 'dpapi-current-user'; target = "Pandora/planner-db/$workspaceId"; version = 2 }) `
        -IdentityPath $identityPath -OutputPath $version2Path -ComputerName 'RENAMED-PC' -UserName 'newname'
    Assert-True ($version2.credential_ref.version -eq 2 -and $version2.fingerprint -cne $dpapiProfile.fingerprint) `
        '凭据轮换版本变化会改变 profile fingerprint，强制全服务统一刷新'

    $validProfileBytes = [IO.File]::ReadAllBytes($profilePath)
    $tampered = Get-Content -LiteralPath $profilePath -Raw -Encoding utf8 | ConvertFrom-Json
    $tampered.databases.pandora_player = 'pandora_player_w_1123456789abcdefghjkmnpqrs'
    $tampered | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $profilePath -Encoding utf8NoBOM
    $tamperBlocked = $false
    try {
        Get-PandoraMysqlRuntimeProfile -ProjectRoot (Join-Path $sandbox 'repo') `
            -ProfilePath $profilePath -IdentityPath $identityPath | Out-Null
    } catch { $tamperBlocked = $true }
    Assert-True $tamperBlocked '物理库映射被手改后 fail-closed'
    [IO.File]::WriteAllBytes($profilePath, $validProfileBytes)

    $identityBytes = [IO.File]::ReadAllBytes($identityPath)
    [ordered]@{ schema_version = 1; workspace_id = '1123456789abcdefghjkmnpqrs' } |
        ConvertTo-Json | Set-Content -LiteralPath $identityPath -Encoding utf8NoBOM
    $crossWorkspaceBlocked = $false
    try {
        Get-PandoraMysqlRuntimeProfile -ProjectRoot (Join-Path $sandbox 'repo') `
            -ProfilePath $profilePath -IdentityPath $identityPath | Out-Null
    } catch { $crossWorkspaceBlocked = $true }
    Assert-True $crossWorkspaceBlocked 'profile workspace 与长期 identity 不一致时拒绝串库'
    [IO.File]::WriteAllBytes($identityPath, $identityBytes)

    Write-Host '[6] 真实仓库 migration set 全量进入映射' -ForegroundColor Cyan
    $realProfilePath = Join-Path $sandbox 'real-repo-profile.json'
    $realProfile = Publish-PandoraMysqlRuntimeProfile `
        -ProjectRoot $RepoRoot -Mode local-owned `
        -Endpoint ([ordered]@{ host = '127.0.0.1'; port = 13307; tls_server_name = ''; ca_file = '' }) `
        -CredentialRef ([ordered]@{ provider = 'dpapi-current-user'; target = 'Pandora/test/local'; version = 1 }) `
        -OutputPath $realProfilePath -ComputerName 'TEST-PC' -UserName 'planner'
    $expectedSets = @(Get-ChildItem -LiteralPath (Join-Path $RepoRoot 'tools/migrate/migrations') -Directory |
        Select-Object -ExpandProperty Name | Sort-Object)
    $actualSets = @($realProfile.databases.PSObject.Properties.Name | Sort-Object)
    Assert-True ($expectedSets.Count -ge 10 -and ($actualSets -join ',') -ceq ($expectedSets -join ',')) `
        'profile 不维护第二份手写库清单，自动覆盖真实仓库全部 migration set'
    $realRoundTrip = Get-PandoraMysqlRuntimeProfile -ProjectRoot $RepoRoot -ProfilePath $realProfilePath
    Assert-True ($realRoundTrip.fingerprint -ceq $realProfile.fingerprint) '真实全量映射可通过消费侧复核'
} finally {
    Remove-Item -LiteralPath $sandbox -Recurse -Force -ErrorAction SilentlyContinue
}

if ($script:Failures.Count -gt 0) {
    Write-Host ''
    Write-Host "[FAIL] MySQL 运行态 profile 契约失败 $($script:Failures.Count) 项" -ForegroundColor Red
    exit 1
}

Write-Host ''
Write-Host '[PASS] MySQL 运行态 profile 契约' -ForegroundColor Green
