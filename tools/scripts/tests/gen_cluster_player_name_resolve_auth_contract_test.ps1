# gen_cluster_player_name_resolve_auth_contract_test — Team→Player 玩家名字解析鉴权生成契约。
#
# 契约:
#   1. player.player_name_resolve_auth_secret 与
#      team.team.player_name_resolver_auth_secret 必须成对写入同一把独立密钥。
#   2. -Prod 必须显式注入，拒绝公开 dev key、弱 key 及与其它权限域复用。
#   3. team resolver 地址在集群产物中必须从 loopback 改写为 player:20002。
#   4. 两端 audience 固定为 player:name；非 -Prod 保留公开 dev 凭据，便于本地联调。
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$env:PANDORA_OWNER_TIDB_DSN = $null
$env:PANDORA_ACCOUNT_TIDB_DSN = $null
$env:PANDORA_PLAYER_NAME_RESOLVE_AUTH_SECRET = $null
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$Generator = Join-Path $ProjectRoot 'tools/scripts/gen_cluster_config.ps1'
$OutDirProd = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-playername-prod-' + [guid]::NewGuid().ToString('N'))
$OutDirDev = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-playername-dev-' + [guid]::NewGuid().ToString('N'))
$OutDirReject = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-playername-reject-' + [guid]::NewGuid().ToString('N'))
$OutDirs = @($OutDirProd, $OutDirDev, $OutDirReject)

$DevPlayerNameResolveAuthSecret = 'pandora-dev-team-player-name-auth-key-v1!'
$ProdPlayerNameResolveAuthSecret = 'prod-player-name-resolve-auth-0123456789abcdef-014'
$ProdPlayerNoResolveAuthSecret = 'prod-player-no-resolve-auth-0123456789abcdef-013'
$ProdTeamResumeAuthSecret = 'prod-team-resume-auth-0123456789abcdef-012'
$PlayerNameResolveAudience = 'player:name'

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

function Get-YamlDirectString([string]$Yaml, [string]$Section, [string]$Child) {
    $pattern = '(?m)^' + [regex]::Escape($Section) + ':[ \t]*(?:#.*)?\r?\n(?:(?:[ \t]+.*|[ \t]*#.*|[ \t]*)\r?\n)*?[ \t]+' +
        [regex]::Escape($Child) + '[ \t]*:[ \t]*"((?:\\.|[^"])*)"'
    $m = [regex]::Match($Yaml, $pattern)
    if (-not $m.Success) { throw "ASSERT FAILED:找不到 $Section.$Child 的双引号标量" }
    return $m.Groups[1].Value
}

# 除 player-name key 外均为合法生产参数，保证负向用例只验证目标维度。
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
        '-MatchResumeAuthSecret', 'prod-match-resume-auth-0123456789abcdef-008',
        '-AllocationAbortAuthSecret', 'prod-allocation-abort-auth-0123456789abcdef-009',
        '-TeamResumeAuthSecret', $ProdTeamResumeAuthSecret,
        '-PlayerNoResolveAuthSecret', $ProdPlayerNoResolveAuthSecret,
        '-FriendPlayerNameResolveAuthSecret', 'prod-friend-player-name-auth-0123456789abcdef-101',
        '-FriendPlayerNoResolveAuthSecret', 'prod-friend-player-no-auth-0123456789abcdef-102',
        '-GuildPlayerNameResolveAuthSecret', 'prod-guild-player-name-auth-0123456789abcdef-103',
        '-GuildPlayerNoResolveAuthSecret', 'prod-guild-player-no-auth-0123456789abcdef-104',
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
    Assert-ProdRejected @() '-Prod 必须拒绝缺失 Team→Player player-name service key'
    Assert-ProdRejected @('-PlayerNameResolveAuthSecret', $DevPlayerNameResolveAuthSecret) `
        '-Prod 必须拒绝仓库公开的 player-name dev key'
    Assert-ProdRejected @('-PlayerNameResolveAuthSecret', 'short-player-name-key') `
        '-Prod 必须拒绝 <32 字节的 player-name key'
    Assert-ProdRejected @('-PlayerNameResolveAuthSecret', $ProdTeamResumeAuthSecret) `
        '-Prod 必须拒绝 player-name key 与 Team resume 权限域复用'
    Assert-ProdRejected @('-PlayerNameResolveAuthSecret', $ProdPlayerNoResolveAuthSecret) `
        '-Prod 必须拒绝 player-name key 与 player-no 权限域复用'

    & pwsh -NoProfile -File $Generator @((Get-ProdArgs $OutDirProd) + @(
            '-PlayerNameResolveAuthSecret', $ProdPlayerNameResolveAuthSecret)) *> $null
    if ($LASTEXITCODE -ne 0) { throw "gen_cluster_config -Prod 生成失败(exit=$LASTEXITCODE)" }

    $playerProd = Get-Content -LiteralPath (Join-Path $OutDirProd 'player.yaml') -Raw
    $teamProd = Get-Content -LiteralPath (Join-Path $OutDirProd 'team.yaml') -Raw
    $playerKey = Get-YamlDirectString $playerProd 'player' 'player_name_resolve_auth_secret'
    $teamKey = Get-YamlDirectString $teamProd 'team' 'player_name_resolver_auth_secret'
    Assert-True ($playerKey -ceq $ProdPlayerNameResolveAuthSecret) '-Prod player 未写入 player-name verifier key'
    Assert-True ($teamKey -ceq $ProdPlayerNameResolveAuthSecret) '-Prod team 未写入 player-name signer key'
    Assert-True ($playerKey -ceq $teamKey) 'Team→Player player-name key 两端必须成对一致'
    Assert-True (-not $playerProd.Contains($DevPlayerNameResolveAuthSecret)) '-Prod player 不得残留 player-name dev key'
    Assert-True (-not $teamProd.Contains($DevPlayerNameResolveAuthSecret)) '-Prod team 不得残留 player-name dev key'
    Assert-True ((Get-YamlDirectString $teamProd 'team' 'player_name_resolver_addr') -ceq 'player:20002') `
        '集群 team resolver 地址必须改写为 player:20002'
    Assert-True ((Get-YamlDirectString $playerProd 'player' 'player_name_resolve_auth_audience') -ceq $PlayerNameResolveAudience) `
        'player player-name verifier audience 必须保持 canonical 值'
    Assert-True ((Get-YamlDirectString $teamProd 'team' 'player_name_resolver_auth_audience') -ceq $PlayerNameResolveAudience) `
        'team player-name signer audience 必须保持 canonical 值'

    $playerExample = Get-Content -LiteralPath (Join-Path $ProjectRoot 'services/account/player/etc/player-prod.yaml.example') -Raw
    $teamExample = Get-Content -LiteralPath (Join-Path $ProjectRoot 'services/matchmaking/team/etc/team-prod.yaml.example') -Raw
    $playerExampleKey = Get-YamlDirectString $playerExample 'player' 'player_name_resolve_auth_secret'
    $teamExampleKey = Get-YamlDirectString $teamExample 'team' 'player_name_resolver_auth_secret'
    Assert-True ($playerExampleKey -ceq $teamExampleKey) '生产示例的 player-name key 占位符必须成对一致'
    Assert-True ($playerExampleKey.Contains('CHANGE_ME')) '生产示例必须保留明确的 CHANGE_ME 占位符，禁止提交真实 key'
    Assert-True ((Get-YamlDirectString $teamExample 'team' 'player_name_resolver_addr') -ceq
        'player.pandora.svc.cluster.local:20002') '生产示例 team resolver 必须指向 player Service'

    & pwsh -NoProfile -File $Generator -OutDir $OutDirDev -AllocatorMode agones `
        -AllocatorAdvertiseHost 127.0.0.1 -AllowDevSecrets `
        -DsAuthMode enforce -DsAuthorityMode redis -DsFenceEtcdEndpoints 'etcd.pandora.svc:2379' `
        -DsFenceKeysetRevision 'pandora-ds-auth-v2-local-r1' `
        -DsTicketActiveKid ('A' * 43) -DsTicketKeysetRevision 7 *> $null
    if ($LASTEXITCODE -ne 0) { throw "gen_cluster_config dev 生成失败(exit=$LASTEXITCODE)" }

    $playerDev = Get-Content -LiteralPath (Join-Path $OutDirDev 'player.yaml') -Raw
    $teamDev = Get-Content -LiteralPath (Join-Path $OutDirDev 'team.yaml') -Raw
    Assert-True ((Get-YamlDirectString $playerDev 'player' 'player_name_resolve_auth_secret') -ceq $DevPlayerNameResolveAuthSecret) `
        'dev player 应保留 player-name dev key'
    Assert-True ((Get-YamlDirectString $teamDev 'team' 'player_name_resolver_auth_secret') -ceq $DevPlayerNameResolveAuthSecret) `
        'dev team 应保留 player-name dev key'
    Assert-True ((Get-YamlDirectString $teamDev 'team' 'player_name_resolver_addr') -ceq 'player:20002') `
        'dev 集群产物也必须把 player-name resolver 地址改写为 player:20002'
    Assert-True ((Get-YamlDirectString $playerDev 'player' 'player_name_resolve_auth_audience') -ceq $PlayerNameResolveAudience) `
        'dev player audience 必须保持 canonical 值'
    Assert-True ((Get-YamlDirectString $teamDev 'team' 'player_name_resolver_auth_audience') -ceq $PlayerNameResolveAudience) `
        'dev team audience 必须保持 canonical 值'
} finally {
    foreach ($dir in $OutDirs) {
        if (-not (Test-Path -LiteralPath $dir -PathType Container)) { continue }
        $resolved = [System.IO.Path]::GetFullPath($dir)
        $temp = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
        if (-not $resolved.StartsWith($temp, [StringComparison]::OrdinalIgnoreCase) -or
            (Split-Path -Leaf $resolved) -notmatch '^pandora-gen-playername-(?:prod|dev|reject)-[0-9a-f]{32}$') {
            throw "拒绝清理未验证测试目录:$resolved"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}

Write-Host 'gen_cluster_player_name_resolve_auth_contract_test: PASS' -ForegroundColor Green
