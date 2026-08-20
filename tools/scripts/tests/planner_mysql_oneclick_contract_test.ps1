# 中心 MySQL 一键启动模式选择与生命周期契约。
#
# 公开 seam：planner_mysql_startup.ps1 的模式/生命周期计划，以及
# local_infra_state.ps1 的全服务 applied profile 状态。

$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
. (Join-Path $projectRoot 'tools/scripts/lib/planner_mysql_startup.ps1')
. (Join-Path $projectRoot 'tools/scripts/lib/local_infra_state.ps1')

$script:failed = 0
function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) {
        Write-Host "  [fail] $Message" -ForegroundColor Red
        $script:failed++
    } else { Write-Host "  [ok] $Message" -ForegroundColor Green }
}
function Assert-Throws([scriptblock]$Action, [string]$Pattern, [string]$Message) {
    try {
        & $Action
        Write-Host "  [fail] $Message（未抛错）" -ForegroundColor Red
        $script:failed++
    } catch {
        if ($_.Exception.Message -notmatch $Pattern) {
            Write-Host "  [fail] $Message（错误不匹配：$($_.Exception.Message)）" -ForegroundColor Red
            $script:failed++
        } else { Write-Host "  [ok] $Message" -ForegroundColor Green }
    }
}

$tempRoot = Join-Path ([IO.Path]::GetTempPath()) ("pandora-oneclick-contract-{0}" -f [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $tempRoot | Out-Null
try {
    Write-Host '[1] bundle 存在即选 central，缺失才保持 local' -ForegroundColor Cyan
    $local = Get-PandoraPlannerMysqlStartupMode -ProjectRoot $tempRoot
    Assert-True ($local -ceq 'local-owned') '无 central-mysql.json 时保持 local-owned'
    $bundleDir = Join-Path $tempRoot 'installers/planner-db'
    New-Item -ItemType Directory -Path $bundleDir -Force | Out-Null
    $bundle = Join-Path $bundleDir 'central-mysql.json'
    [IO.File]::WriteAllText($bundle, '{bad json', [Text.UTF8Encoding]::new($false))
    $central = Get-PandoraPlannerMysqlStartupMode -ProjectRoot $tempRoot
    Assert-True ($central -ceq 'central-managed') '配置即使损坏也不能回退 local-owned'
    Assert-Throws {
        Initialize-PandoraPlannerMysqlRuntime -ProjectRoot $tempRoot | Out-Null
    } 'central mysql bundle|central-mysql|JSON|解析|损坏' '损坏 bundle 在初始化时 fail-closed'

    Write-Host '[2] localinfra 生命周期计划在 central 模式对 MySQL 零动作' -ForegroundColor Cyan
    $centralPlan = Get-PandoraLocalInfraLifecyclePlan -ProjectRoot $tempRoot
    Assert-True ($centralPlan.Mode -ceq 'central-managed') '计划标记 central-managed'
    Assert-True ($centralPlan.ProvisionComponents -notcontains 'mysql') 'central 不下载/解包 MySQL'
    Assert-True ($centralPlan.StartComponents -notcontains 'mysql') 'central 不启动 MySQL'
    Assert-True ($centralPlan.StopComponents -notcontains 'mysql') 'central down 不停止 MySQL'
    Assert-True ($centralPlan.ResetComponents -notcontains 'mysql') 'central reset 不重置 MySQL 数据'

    Remove-Item -LiteralPath $bundle -Force
    $localPlan = Get-PandoraLocalInfraLifecyclePlan -ProjectRoot $tempRoot
    Assert-True ($localPlan.ProvisionComponents -contains 'mysql') 'local-owned 仍备料 MySQL'
    Assert-True ($localPlan.StartComponents -contains 'mysql') 'local-owned 仍启动 MySQL'
    Assert-True ($localPlan.StopComponents -contains 'mysql') 'local-owned 仍停止自己的 MySQL'
    Assert-True ($localPlan.ResetComponents -contains 'mysql') 'local-owned 仍重置自己的 MySQL 数据'

    Write-Host '[3] applied state 记录 exact profile fingerprint 并兼容原 local v2' -ForegroundColor Cyan
    $stateRoot = Join-Path $tempRoot 'state-project'
    New-Item -ItemType Directory -Path $stateRoot | Out-Null
    $fpA = 'sha256:' + ('a' * 64)
    $fpB = 'sha256:' + ('b' * 64)
    Set-PandoraServiceAppliedMysqlProfile -ProjectRoot $stateRoot -Mode central `
        -MysqlPort 3306 -SocialOnMysql $true -ProfileFingerprint $fpA | Out-Null
    $state = Get-PandoraServiceAppliedMysqlState $stateRoot
    Assert-True ($state.Mode -ceq 'central') 'central applied mode 可往返'
    Assert-True ($state.ProfileFingerprint -ceq $fpA) 'profile fingerprint 可往返'
    Assert-True ($state.ProfileFingerprint -cne $fpB) '凭据/endpoint 漂移能被 exact fingerprint 区分'
    Assert-Throws {
        Get-PandoraPlannerMysqlStartupMode -ProjectRoot $stateRoot | Out-Null
    } 'central.*bundle|bundle.*central|中心.*bundle' '已登记 central applied state 后缺 bundle 必须 fail-closed'

    Set-PandoraServiceAppliedMysqlPort -ProjectRoot $stateRoot -Mode nodocker `
        -MysqlPort 13307 -SocialOnMysql $true | Out-Null
    $localState = Get-PandoraServiceAppliedMysqlState $stateRoot
    Assert-True ($localState.SchemaVersion -eq 2 -and $localState.Mode -ceq 'nodocker') '原 local v2 marker 保持兼容'
    Assert-True ((Get-PandoraPlannerMysqlStartupMode -ProjectRoot $stateRoot) -ceq 'local-owned') `
        '明确 local applied state 且无 central profile 时仍保持 local-owned'

    $profileRoot = Join-Path $tempRoot 'profile-project'
    New-Item -ItemType Directory -Path $profileRoot | Out-Null
    $profileMigrationsRoot = Join-Path $profileRoot 'tools/migrate/migrations'
    foreach ($set in @(Get-PandoraMysqlMigrationSets -ProjectRoot $projectRoot)) {
        New-Item -ItemType Directory -Path (Join-Path $profileMigrationsRoot $set) -Force | Out-Null
    }
    $workspaceId = '01arz3ndektsv4rrffq69g5fav'
    $identityPath = Join-Path $profileRoot 'identity.json'
    Set-PandoraPlannerDbIdentity -WorkspaceId $workspaceId -IdentityPath $identityPath | Out-Null
    $caPath = Join-Path $profileRoot 'planner-ca.pem'
    [IO.File]::WriteAllText($caPath, 'central-ca-contract', [Text.UTF8Encoding]::new($false))
    Publish-PandoraMysqlRuntimeProfile -ProjectRoot $profileRoot -Mode central-managed `
        -Endpoint ([ordered]@{ host = 'planner-db.intra'; port = 3306; tls_server_name = 'planner-db.intra'; ca_file = $caPath }) `
        -CredentialRef ([ordered]@{ provider = 'dpapi-current-user'; target = "Pandora/PlannerDB/$workspaceId/app"; version = 1 }) `
        -IdentityPath $identityPath -ComputerName 'PC01' -UserName 'planner' | Out-Null
    Assert-Throws {
        Get-PandoraPlannerMysqlStartupMode -ProjectRoot $profileRoot | Out-Null
    } 'central.*bundle|bundle.*central|中心.*bundle' '已发布 central runtime profile 后缺 bundle 必须 fail-closed'

    Write-Host '[4] 四条脚本真实接线到同一模式 seam' -ForegroundColor Cyan
    $devAll = [IO.File]::ReadAllText((Join-Path $projectRoot 'tools/scripts/dev_all.ps1'))
    $localInfra = [IO.File]::ReadAllText((Join-Path $projectRoot 'tools/scripts/local_infra.ps1'))
    $runServices = [IO.File]::ReadAllText((Join-Path $projectRoot 'tools/scripts/run_services.ps1'))
    $start = [IO.File]::ReadAllText((Join-Path $projectRoot 'tools/scripts/start.ps1'))
    Assert-True ($devAll -match 'Initialize-PandoraPlannerMysqlRuntime') 'dev_all first-run 复用 enrollment/profile seam'
    Assert-True ($localInfra -match 'Get-PandoraLocalInfraLifecyclePlan') 'local_infra 生命周期由动态计划驱动'
    Assert-True ($runServices -match 'New-PandoraMysqlServiceRuntimeConfig') 'run_services 使用中心 secret YAML renderer'
    Assert-True ($runServices -match 'Remove-PandoraMysqlServiceRuntimeConfig') 'run_services 启动读取后清理 secret YAML'
    Assert-True ($start -match 'ProfileFingerprint') '-DsOnly 对比 profile fingerprint'
} finally {
    Remove-Item -LiteralPath $tempRoot -Recurse -Force -ErrorAction SilentlyContinue
}

if ($script:failed -gt 0) {
    Write-Host "`n[FAIL] $script:failed 项失败" -ForegroundColor Red
    exit 1
}
Write-Host "`n[PASS] 中心 MySQL 一键启动契约" -ForegroundColor Green
