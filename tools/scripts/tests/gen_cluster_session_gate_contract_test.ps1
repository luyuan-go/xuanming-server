# gen_cluster_session_gate_contract_test — -Prod 产物客户端面会话现行性门契约
# (R5 复审 P0-1,INC-20260722-004,2026-07-22)。
#
# 契约:
#   1. -Prod 产物中全部客户端面服务(friend/chat/mail/guild/trade/team/matchmaker/
#      matchmaker-pve/player/inventory/leaderboard/hub-allocator/mission)session_gate.require
#      必须且只能为 true —— 顶号后旧 JWT 在 exp 前必须失去全部按 player_id 定向能力,
#      不允许任何 -Prod 产物继承 dev 宽松档。
#   2. -Prod 产物 push.yaml require_session_gate: true(Subscribe 建流门,既有契约一并锁死)。
#   3. 非 -Prod(本地 dev)行为不变:session_gate.require: false 原样保留。
#   4. 上述服务产物必须带 node.redis_client 端点(会话权威可达;require=true 漏配拒启)。
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$env:PANDORA_OWNER_TIDB_DSN = $null
$env:PANDORA_ACCOUNT_TIDB_DSN = $null
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$Generator = Join-Path $ProjectRoot 'tools/scripts/gen_cluster_config.ps1'
$OutDirProd = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-sessgate-prod-' + [guid]::NewGuid().ToString('N'))
$OutDirDev = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-sessgate-dev-' + [guid]::NewGuid().ToString('N'))
$OutDirs = @($OutDirProd, $OutDirDev)

# 与 gen_cluster_config.ps1 内 $UnarySessionGateServiceNames 同步维护(漂移即测试失败)。
$SessionGateServices = @(
    'friend', 'chat', 'mail', 'guild', 'trade', 'team',
    'matchmaker', 'matchmaker-pve', 'player', 'inventory', 'leaderboard', 'hub-allocator',
    'mission'
)

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

try {
    # ── 1) -Prod 正向:全部客户端面服务 session_gate.require 机械置 true ──
    & pwsh -NoProfile -File $Generator -OutDir $OutDirProd -AllocatorMode agones -Prod `
        -Secret 'prod-player-key-0123456789abcdef-001' `
        -DsSecret 'prod-ds-callback-key-0123456789abcdef-002' `
        -PlacementAccountBootstrapSecret 'prod-placement-bootstrap-0123456789abcdef-003' `
        -PlacementMatchStartSecret 'prod-placement-match-start-0123456789abcdef-004' `
        -PlacementBattleExitSecret 'prod-placement-battle-exit-0123456789abcdef-005' `
        -PlacementHubTransferSecret 'prod-placement-hub-transfer-0123456789abcdef-006' `
        -PlacementBattleDepartureSecret 'prod-placement-battle-departure-0123456789abcdef-007' `
        -MatchResumeAuthSecret 'prod-match-resume-auth-0123456789abcdef-008' `
        -TeamResumeAuthSecret 'prod-team-resume-auth-0123456789abcdef-012' `
        -PlayerNoResolveAuthSecret 'prod-player-no-resolve-auth-0123456789abcdef-013' `
        -PlayerNameResolveAuthSecret 'prod-player-name-resolve-auth-0123456789abcdef-014' `
        -FriendPlayerNameResolveAuthSecret 'prod-friend-player-name-auth-0123456789abcdef-101' `
        -FriendPlayerNoResolveAuthSecret 'prod-friend-player-no-auth-0123456789abcdef-102' `
        -GuildPlayerNameResolveAuthSecret 'prod-guild-player-name-auth-0123456789abcdef-103' `
        -GuildPlayerNoResolveAuthSecret 'prod-guild-player-no-auth-0123456789abcdef-104' `
        -AllocationAbortAuthSecret 'prod-allocation-abort-auth-0123456789abcdef-009' `
        -DsAuthMode enforce -DsAuthorityMode redis `
        -DsFenceEtcdEndpoints 'https://etcd.pandora.svc:2379' `
        -DsFenceKeysetRevision 'pandora-ds-auth-v2-prod-r1' `
        -DsTicketActiveKid ('P' * 43) -DsTicketKeysetRevision 9 `
        -OwnerStoreDsn 'prod_owner:prod-owner-pwd-010@tcp(tidb.pandora.svc:4000)/pandora_owner?parseTime=true&loc=UTC' `
        -AccountStoreDsn 'prod_login:prod-acct-pwd-011@tcp(tidb.pandora.svc:4000)/pandora_account?parseTime=true&loc=UTC' *> $null
    if ($LASTEXITCODE -ne 0) { throw "gen_cluster_config -Prod 生成失败(exit=$LASTEXITCODE)" }

    foreach ($name in $SessionGateServices) {
        $yaml = Get-Content -LiteralPath (Join-Path $OutDirProd "$name.yaml") -Raw
        Assert-True (([regex]::Matches($yaml, '(?m)^session_gate:[ \t]*\r?\n[ \t]{2}require:[ \t]*true[ \t]*$')).Count -eq 1) `
            "-Prod $name.yaml 必须恰好一处 session_gate.require: true"
        Assert-True (-not [regex]::IsMatch($yaml, '(?m)^session_gate:[ \t]*\r?\n[ \t]{2}require:[ \t]*false')) `
            "-Prod $name.yaml 不得残留 session_gate.require: false"
        Assert-True ([regex]::IsMatch($yaml, '(?m)^[ \t]{2}redis_client:')) `
            "-Prod $name.yaml 必须带 node.redis_client(会话权威端点;require=true 漏配拒启)"
    }

    $pushProd = Get-Content -LiteralPath (Join-Path $OutDirProd 'push.yaml') -Raw
    Assert-True (([regex]::Matches($pushProd, '(?m)^[ \t]{2}require_session_gate:[ \t]*true[ \t]*$')).Count -eq 1) `
        '-Prod push.yaml 必须恰好一处 require_session_gate: true(Subscribe 建流门)'

    # ── 2) 非 -Prod(本地 dev)行为不变 ──
    & pwsh -NoProfile -File $Generator -OutDir $OutDirDev -AllocatorMode agones `
        -AllocatorAdvertiseHost 127.0.0.1 -AllowDevSecrets `
        -DsAuthMode enforce -DsAuthorityMode redis -DsFenceEtcdEndpoints 'etcd.pandora.svc:2379' `
        -DsFenceKeysetRevision 'pandora-ds-auth-v2-local-r1' `
        -DsTicketActiveKid ('A' * 43) -DsTicketKeysetRevision 7 *> $null
    if ($LASTEXITCODE -ne 0) { throw "gen_cluster_config dev 生成失败(exit=$LASTEXITCODE)" }

    foreach ($name in $SessionGateServices) {
        $yaml = Get-Content -LiteralPath (Join-Path $OutDirDev "$name.yaml") -Raw
        Assert-True ([regex]::IsMatch($yaml, '(?m)^session_gate:[ \t]*\r?\n[ \t]{2}require:[ \t]*false[ \t]*$')) `
            "dev $name.yaml session_gate.require: false 不得被非 -Prod 生成改写"
    }
} finally {
    foreach ($dir in $OutDirs) {
        if (-not (Test-Path -LiteralPath $dir -PathType Container)) { continue }
        $resolved = [System.IO.Path]::GetFullPath($dir)
        $temp = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
        if (-not $resolved.StartsWith($temp, [StringComparison]::OrdinalIgnoreCase) -or
            (Split-Path -Leaf $resolved) -notmatch '^pandora-gen-sessgate-(?:prod|dev)-[0-9a-f]{32}$') {
            throw "拒绝清理未验证测试目录:$resolved"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}

Write-Host 'gen_cluster_session_gate_contract_test: PASS' -ForegroundColor Green
