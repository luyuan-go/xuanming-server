# gen_cluster_team_resume_auth_contract_test — Team→Matchmaker 服务身份 key 的生成器契约。
#
# 背景:matchmaker 的 ResolvePlayerMatchContext 有两个合法内部调用方(login / team),
# 各持**独立**密钥。team 侧漏配不会回落到 login 那把 —— 它只会被 matchmaker 拒(code=7),
# 表现为「入队闸门 fail-closed 拒绝入队 + 招募列表恒空」,而两边进程都启动成功、看着很健康。
# 这正是必须由生成器机械保证、而不是靠运维记得改两个文件的原因。
#
# 契约:
#   1. 两端成对:matchmaker.match.team_resume_auth_secret 与 team.team.match_resume_auth_secret
#      必须是同一个值。只写一端 = 全部 team 调用被拒。
#   2. 与 login 那把 match_resume_auth_secret **必须不同**:共用会把两个服务的信任域合并
#      (任一方可冒充另一方),且轮换变成全有全无。
#   3. -Prod 必须显式提供,且拒绝仓库公开 dev key / 拒绝与其它权限域复用。
#   4. 非 -Prod 保持 dev 模板值不变(本地一键起不需要额外参数)。
#   5. 普通 online 发布是发布、不是换钥:这把 key 漂移或单端改写必须在 apply 前被拦下。
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$env:PANDORA_OWNER_TIDB_DSN = $null
$env:PANDORA_ACCOUNT_TIDB_DSN = $null
$env:PANDORA_MATCH_RESUME_AUTH_SECRET = $null
$env:PANDORA_TEAM_RESUME_AUTH_SECRET = $null
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$Generator = Join-Path $ProjectRoot 'tools/scripts/gen_cluster_config.ps1'
. (Join-Path $ProjectRoot 'tools/scripts/lib/online_manifest_contract.ps1')
$OutDirProd = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-teamauth-prod-' + [guid]::NewGuid().ToString('N'))
$OutDirDev = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-teamauth-dev-' + [guid]::NewGuid().ToString('N'))
$OutDirDevRerun = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-teamauth-devrerun-' + [guid]::NewGuid().ToString('N'))
$OutDirReject = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-teamauth-reject-' + [guid]::NewGuid().ToString('N'))
$OutDirs = @($OutDirProd, $OutDirDev, $OutDirDevRerun, $OutDirReject)

# 与 gen_cluster_config.ps1 的 $DevTeamResumeAuthSecret / dev 模板保持同步(漂移即测试失败)。
$DevTeamResumeAuthSecret = 'pandora-dev-team-resume-auth-key-v1!'
$DevMatchResumeAuthSecret = 'pandora-dev-match-resume-auth-key-v1!'
$ProdMatchResumeAuth = 'prod-match-resume-auth-0123456789abcdef-008'
$ProdTeamResumeAuth = 'prod-team-resume-auth-0123456789abcdef-012'

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

function Assert-Throws([scriptblock]$Action, [string]$Message) {
    $thrown = $false
    try { & $Action } catch { $thrown = $true }
    Assert-True $thrown $Message
}

# online 连续性门禁读的是整套服务 YAML(跨权限域不相交需要 hmac / abort 那几份)。
# 这份清单必须覆盖 online_manifest_contract.ps1 的 $PandoraDsCallbackHmacServices —— 漏一个服务,
# 门禁自己会抛「缺 xxx 配置」,测试变成确定性红,却和被测契约无关。
function Get-TeamAuthConfigs([string]$TargetDir) {
    $configs = [ordered]@{}
    foreach ($service in @('login', 'matchmaker', 'matchmaker-pve', 'hub-allocator',
            'ds-allocator', 'battle-result', 'player-locator', 'team', 'player', 'guild')) {
        $configs[$service] = Get-Content -LiteralPath (Join-Path $TargetDir "$service.yaml") -Raw
    }
    return $configs
}

# 取 <section>.<child> 的单行双引号标量值;缺节点/格式漂移直接失败,不做模糊匹配。
function Get-YamlDirectString([string]$Yaml, [string]$Section, [string]$Child) {
    $pattern = '(?m)^' + [regex]::Escape($Section) + ':[ \t]*(?:#.*)?\r?\n(?:(?:[ \t]+.*|[ \t]*#.*|[ \t]*)\r?\n)*?[ \t]+' +
        [regex]::Escape($Child) + '[ \t]*:[ \t]*"((?:\\.|[^"])*)"'
    $m = [regex]::Match($Yaml, $pattern)
    if (-not $m.Success) { throw "ASSERT FAILED:找不到 $Section.$Child 的双引号标量" }
    return $m.Groups[1].Value
}

# -Prod 生成的公共参数(除 team resume key 外全部合法),便于负向用例只变动一个维度:
# 若因为别的必填项缺失而失败,用例照样 PASS 却再也证明不了它声称的东西。
function Get-ProdArgs([string]$TargetDir) {
    return @(
        '-OutDir', $TargetDir, '-AllocatorMode', 'agones', '-Prod',
        '-Secret', 'prod-player-key-0123456789abcdef-001',
        '-DsSecret', 'prod-ds-callback-key-0123456789abcdef-002',
        '-PlacementAccountBootstrapSecret', 'prod-placement-bootstrap-0123456789abcdef-003',
        '-PlacementMatchStartSecret', 'prod-placement-match-start-0123456789abcdef-004',
        '-PlacementBattleExitSecret', 'prod-placement-battle-exit-0123456789abcdef-005',
        '-PlacementHubTransferSecret', 'prod-placement-hub-transfer-0123456789abcdef-006',
        '-PlacementBattleDepartureSecret', 'prod-placement-battle-departure-0123456789abcdef-007',
        '-MatchResumeAuthSecret', $ProdMatchResumeAuth,
        '-PlayerNoResolveAuthSecret', 'prod-player-no-resolve-auth-0123456789abcdef-013',
        '-PlayerNameResolveAuthSecret', 'prod-player-name-resolve-auth-0123456789abcdef-014',
        '-FriendPlayerNameResolveAuthSecret', 'prod-friend-player-name-auth-0123456789abcdef-101',
        '-FriendPlayerNoResolveAuthSecret', 'prod-friend-player-no-auth-0123456789abcdef-102',
        '-GuildPlayerNameResolveAuthSecret', 'prod-guild-player-name-auth-0123456789abcdef-103',
        '-GuildPlayerNoResolveAuthSecret', 'prod-guild-player-no-auth-0123456789abcdef-104',
        '-AllocationAbortAuthSecret', 'prod-allocation-abort-auth-0123456789abcdef-009',
        '-DsAuthMode', 'enforce', '-DsAuthorityMode', 'redis',
        '-DsFenceEtcdEndpoints', 'https://etcd.pandora.svc:2379',
        '-DsFenceKeysetRevision', 'pandora-ds-auth-v2-prod-r1',
        '-DsTicketActiveKid', ('P' * 43), '-DsTicketKeysetRevision', '9',
        '-OwnerStoreDsn', 'prod_owner:prod-owner-pwd-010@tcp(tidb.pandora.svc:4000)/pandora_owner?parseTime=true&loc=UTC',
        '-AccountStoreDsn', 'prod_login:prod-acct-pwd-011@tcp(tidb.pandora.svc:4000)/pandora_account?parseTime=true&loc=UTC')
}

function Assert-ProdRejected([string[]]$ExtraArgs, [string]$Reason) {
    & pwsh -NoProfile -File $Generator @((Get-ProdArgs $OutDirReject) + $ExtraArgs) *> $null
    Assert-True ($LASTEXITCODE -ne 0) $Reason
}

try {
    # ── 1) -Prod 负向:漏配 / dev key / 跨域复用都必须在生成前失败 ──
    Assert-ProdRejected @() '-Prod 必须拒绝缺失 Team resume service key(否则 team 入队闸门出厂即 fail-closed)'
    Assert-ProdRejected @('-TeamResumeAuthSecret', $DevTeamResumeAuthSecret) `
        '-Prod 必须拒绝仓库公开的 team resume dev key'
    Assert-ProdRejected @('-TeamResumeAuthSecret', 'short-team-key') `
        '-Prod 必须拒绝 <32 字节的 team resume key'
    Assert-ProdRejected @('-TeamResumeAuthSecret', $ProdMatchResumeAuth) `
        '-Prod 必须拒绝 team 与 login 复用同一把 resume key(信任域合并 + 轮换全有全无)'
    Assert-ProdRejected @('-TeamResumeAuthSecret', 'prod-player-key-0123456789abcdef-001') `
        '-Prod 必须拒绝 team resume key 复用玩家面 JWT 密钥'

    # ── 2) -Prod 正向:两端成对写入且与 login 那把不同 ──
    & pwsh -NoProfile -File $Generator @((Get-ProdArgs $OutDirProd) + @('-TeamResumeAuthSecret', $ProdTeamResumeAuth)) *> $null
    if ($LASTEXITCODE -ne 0) { throw "gen_cluster_config -Prod 生成失败(exit=$LASTEXITCODE)" }

    $matchmakerProd = Get-Content -LiteralPath (Join-Path $OutDirProd 'matchmaker.yaml') -Raw
    $teamProd = Get-Content -LiteralPath (Join-Path $OutDirProd 'team.yaml') -Raw
    $verifierKey = Get-YamlDirectString $matchmakerProd 'match' 'team_resume_auth_secret'
    $signerKey = Get-YamlDirectString $teamProd 'team' 'match_resume_auth_secret'
    Assert-True ($verifierKey -ceq $ProdTeamResumeAuth) '-Prod matchmaker 未写入注入的 team resume key'
    Assert-True ($signerKey -ceq $ProdTeamResumeAuth) '-Prod team 未写入注入的 team resume key'
    Assert-True ($signerKey -ceq $verifierKey) '签名端(team)与验签端(matchmaker)的 team resume key 必须成对一致'

    $loginResumeKey = Get-YamlDirectString $matchmakerProd 'match' 'match_resume_auth_secret'
    Assert-True ($loginResumeKey -ceq $ProdMatchResumeAuth) '-Prod matchmaker 未写入注入的 login resume key'
    Assert-True ($verifierKey -cne $loginResumeKey) 'team 与 login 的 resume key 必须是两把独立密钥'

    # audience 两端也必须一致:错配同样只表现为「静默被拒」。
    Assert-True ((Get-YamlDirectString $teamProd 'team' 'match_resume_auth_audience') -ceq
        (Get-YamlDirectString $matchmakerProd 'match' 'match_resume_auth_audience')) `
        'team 的 match_resume_auth_audience 必须等于 matchmaker 的 match_resume_auth_audience'

    Assert-True (-not $teamProd.Contains($DevTeamResumeAuthSecret)) '-Prod team 产物不得残留 dev team resume key'
    Assert-True (-not $matchmakerProd.Contains($DevTeamResumeAuthSecret)) '-Prod matchmaker 产物不得残留 dev team resume key'

    # ── 3) 非 -Prod:保持 dev 模板值,且两端依然成对 ──
    & pwsh -NoProfile -File $Generator -OutDir $OutDirDev -AllocatorMode agones `
        -AllocatorAdvertiseHost 127.0.0.1 -AllowDevSecrets `
        -DsAuthMode enforce -DsAuthorityMode redis -DsFenceEtcdEndpoints 'etcd.pandora.svc:2379' `
        -DsFenceKeysetRevision 'pandora-ds-auth-v2-local-r1' `
        -DsTicketActiveKid ('A' * 43) -DsTicketKeysetRevision 7 *> $null
    if ($LASTEXITCODE -ne 0) { throw "gen_cluster_config dev 生成失败(exit=$LASTEXITCODE)" }

    $matchmakerDev = Get-Content -LiteralPath (Join-Path $OutDirDev 'matchmaker.yaml') -Raw
    $teamDev = Get-Content -LiteralPath (Join-Path $OutDirDev 'team.yaml') -Raw
    Assert-True ((Get-YamlDirectString $matchmakerDev 'match' 'team_resume_auth_secret') -ceq $DevTeamResumeAuthSecret) `
        'dev matchmaker 的 team resume key 应保持 dev 模板值'
    Assert-True ((Get-YamlDirectString $teamDev 'team' 'match_resume_auth_secret') -ceq $DevTeamResumeAuthSecret) `
        'dev team 的 team resume key 应保持 dev 模板值'
    Assert-True ($DevTeamResumeAuthSecret -cne $DevMatchResumeAuthSecret) `
        'dev 模板里 team 与 login 的 resume key 也必须是两把(否则本地跑不出真实鉴权行为)'

    # ── 4) 普通 online 发布的连续性门禁 ──
    # 这把 key 换了就必须走「先 verifier 后 signer」的独立换钥流程;普通发布若静默带上新 key,
    # 集群会在滚动窗口内出现签名端/验签端各半的状态 —— 表现同样是招募列表恒空,且没人知道为什么。
    & pwsh -NoProfile -File $Generator -OutDir $OutDirDevRerun -AllocatorMode agones `
        -AllocatorAdvertiseHost 127.0.0.1 -AllowDevSecrets `
        -DsAuthMode enforce -DsAuthorityMode redis -DsFenceEtcdEndpoints 'etcd.pandora.svc:2379' `
        -DsFenceKeysetRevision 'pandora-ds-auth-v2-local-r1' `
        -DsTicketActiveKid ('A' * 43) -DsTicketKeysetRevision 7 *> $null
    if ($LASTEXITCODE -ne 0) { throw "gen_cluster_config dev 幂等重跑生成失败(exit=$LASTEXITCODE)" }

    $devConfigs = Get-TeamAuthConfigs $OutDirDev
    $rerunConfigs = Get-TeamAuthConfigs $OutDirDevRerun
    $prodConfigs = Get-TeamAuthConfigs $OutDirProd

    $devTeamAuth = Get-PandoraOnlineTeamResumeAuthContract -Configs $devConfigs
    Assert-True (-not [string]::IsNullOrEmpty($devTeamAuth)) 'Team resume 契约必须产出非空指纹'
    Assert-True ($devTeamAuth -cne (Get-PandoraOnlineMatchResumeAuthContract -Configs $devConfigs)) `
        'Team resume 与 Match resume 的契约指纹必须不同(同值说明两个信任域已合并)'
    # 幂等重跑必须放行,否则每次普通发布都会被自己的门禁拦死。
    Assert-PandoraOnlineTeamResumeAuthContinuity -LiveConfigs $devConfigs -CandidateConfigs $rerunConfigs | Out-Null

    Assert-Throws {
        Assert-PandoraOnlineTeamResumeAuthContinuity -LiveConfigs $devConfigs -CandidateConfigs $prodConfigs | Out-Null
    } '普通发布必须拒绝 Team resume service identity key 漂移'

    # 生成器产物被外部流程单点改写(只换 team 或只换 matchmaker)必须在 apply 前失败,
    # 而不是等到线上招募列表恒空才被发现。
    foreach ($drifted in @('team', 'matchmaker')) {
        $singleEnd = [ordered]@{}
        foreach ($entry in $prodConfigs.GetEnumerator()) { $singleEnd[$entry.Key] = [string]$entry.Value }
        $singleEnd[$drifted] = $singleEnd[$drifted].Replace($ProdTeamResumeAuth, 'prod-team-resume-drift-0123456789abcdef')
        Assert-Throws {
            Get-PandoraOnlineTeamResumeAuthContract -Configs $singleEnd | Out-Null
        } "普通发布必须拒绝 $drifted 单端改写 Team resume key"
    }

    # 两端一起改成 login 那把 = 信任域合并,契约同样必须拒。
    $mergedDomain = [ordered]@{}
    foreach ($entry in $prodConfigs.GetEnumerator()) { $mergedDomain[$entry.Key] = [string]$entry.Value }
    foreach ($service in @('team', 'matchmaker')) {
        $mergedDomain[$service] = $mergedDomain[$service].Replace($ProdTeamResumeAuth, $ProdMatchResumeAuth)
    }
    Assert-Throws {
        Get-PandoraOnlineTeamResumeAuthContract -Configs $mergedDomain | Out-Null
    } '普通发布必须拒绝 team 与 login 复用同一把 resume key'

    # 门禁必须真的被 online 发布路径调用,且在 BuildPush(推镜像)之前 —— 否则只是库里躺着的死代码。
    $startSource = [System.IO.File]::ReadAllText((Join-Path $ProjectRoot 'tools/scripts/start.ps1'),
        [System.Text.UTF8Encoding]::new($false))
    $teamGate = $startSource.IndexOf('Assert-PandoraOnlineTeamResumeAuthContinuity', [StringComparison]::Ordinal)
    $generate = $startSource.IndexOf('& "$ScriptDir/gen_cluster_config.ps1" @genArgs', [StringComparison]::Ordinal)
    $buildPush = $startSource.IndexOf('if ($BuildPush)', [StringComparison]::Ordinal)
    Assert-True ($teamGate -ge 0 -and $generate -ge 0 -and $buildPush -ge 0) `
        'online 发布路径必须存在 Team resume 连续性门禁与其顺序 marker'
    Assert-True ($teamGate -gt $generate -and $teamGate -lt $buildPush) `
        'Team resume 连续性门禁必须在候选生成之后、BuildPush 之前执行'
    # apply 前与互斥锁内各一次:早期预检到锁内重验之间 live Secret 可能被别的流程改写。
    $teamGateCount = ([regex]::Matches($startSource, [regex]::Escape('Assert-PandoraOnlineTeamResumeAuthContinuity'))).Count
    Assert-True ($teamGateCount -ge 2) 'Team resume 连续性门禁必须在早期预检与锁内重验各执行一次'
    Assert-True ($startSource.Contains('PANDORA_TEAM_RESUME_AUTH_SECRET')) `
        'online -Prod 必须在推镜像前预检 PANDORA_TEAM_RESUME_AUTH_SECRET(否则留下半推未部署的脏状态)'
} finally {
    foreach ($dir in $OutDirs) {
        if (-not (Test-Path -LiteralPath $dir -PathType Container)) { continue }
        $resolved = [System.IO.Path]::GetFullPath($dir)
        $temp = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
        if (-not $resolved.StartsWith($temp, [StringComparison]::OrdinalIgnoreCase) -or
            (Split-Path -Leaf $resolved) -notmatch '^pandora-gen-teamauth-(?:prod|dev|devrerun|reject)-[0-9a-f]{32}$') {
            throw "拒绝清理未验证测试目录:$resolved"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}

Write-Host 'gen_cluster_team_resume_auth_contract_test: PASS' -ForegroundColor Green
