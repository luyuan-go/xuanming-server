# gen_cluster_prod_ratelimit_contract_test — -Prod 产物客户端面 BBR 限流契约
# (压测前审核门禁-A,2026-07-26)。
#
# 契约:
#   1. -Prod 产物中全部客户端面服务(session_gate 清单 + login + push)
#      server.grpc.enable_rate_limit 必须且只能为 true —— 过载时 server 侧必须能
#      按 CPU/inflight/RT 丢负载,不允许 -Prod 产物继承 dev 的零值 false
#      (否则登录/聊天洪峰只能靠 Envoy 隐式熔断 + OOMKill 兜底)。
#   2. 非 -Prod(本地 dev)行为不变:dev 模板不写 enable_rate_limit(零值 false)原样保留。
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$env:PANDORA_OWNER_TIDB_DSN = $null
$env:PANDORA_ACCOUNT_TIDB_DSN = $null
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$Generator = Join-Path $ProjectRoot 'tools/scripts/gen_cluster_config.ps1'
$OutDirProd = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-ratelimit-prod-' + [guid]::NewGuid().ToString('N'))
$OutDirDev = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-ratelimit-dev-' + [guid]::NewGuid().ToString('N'))
$OutDirs = @($OutDirProd, $OutDirDev)

# 与 gen_cluster_config.ps1 内 $GrpcRateLimitServiceNames 同步维护(漂移即测试失败)。
$RateLimitServices = @(
    'friend', 'chat', 'mail', 'guild', 'trade', 'team',
    'matchmaker', 'matchmaker-pve', 'player', 'inventory', 'leaderboard', 'hub-allocator',
    'mission', 'login', 'push'
)

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

try {
    # ── 1) -Prod 正向:全部客户端面服务 enable_rate_limit 机械置 true ──
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
        -AllocationAbortAuthSecret 'prod-allocation-abort-auth-0123456789abcdef-009' `
        -DsAuthMode enforce -DsAuthorityMode redis `
        -DsFenceEtcdEndpoints 'https://etcd.pandora.svc:2379' `
        -DsFenceKeysetRevision 'pandora-ds-auth-v2-prod-r1' `
        -DsTicketActiveKid ('P' * 43) -DsTicketKeysetRevision 9 `
        -OwnerStoreDsn 'prod_owner:prod-owner-pwd-010@tcp(tidb.pandora.svc:4000)/pandora_owner?parseTime=true&loc=UTC' `
        -AccountStoreDsn 'prod_login:prod-acct-pwd-011@tcp(tidb.pandora.svc:4000)/pandora_account?parseTime=true&loc=UTC' *> $null
    if ($LASTEXITCODE -ne 0) { throw "gen_cluster_config -Prod 生成失败(exit=$LASTEXITCODE)" }

    foreach ($name in $RateLimitServices) {
        $yaml = Get-Content -LiteralPath (Join-Path $OutDirProd "$name.yaml") -Raw
        Assert-True (([regex]::Matches($yaml, '(?m)^[ \t]+enable_rate_limit:[ \t]*true[ \t]*(?:#.*)?$')).Count -eq 1) `
            "-Prod $name.yaml 必须恰好一处 enable_rate_limit: true"
        Assert-True (-not [regex]::IsMatch($yaml, '(?m)^[ \t]+enable_rate_limit:[ \t]*false')) `
            "-Prod $name.yaml 不得残留 enable_rate_limit: false"
    }

    # ── 2) 非 -Prod(本地 dev)行为不变:不注入 enable_rate_limit ──
    & pwsh -NoProfile -File $Generator -OutDir $OutDirDev -AllocatorMode agones `
        -AllocatorAdvertiseHost 127.0.0.1 -AllowDevSecrets `
        -DsAuthMode enforce -DsAuthorityMode redis -DsFenceEtcdEndpoints 'etcd.pandora.svc:2379' `
        -DsFenceKeysetRevision 'pandora-ds-auth-v2-local-r1' `
        -DsTicketActiveKid ('A' * 43) -DsTicketKeysetRevision 7 *> $null
    if ($LASTEXITCODE -ne 0) { throw "gen_cluster_config dev 生成失败(exit=$LASTEXITCODE)" }

    foreach ($name in $RateLimitServices) {
        $yaml = Get-Content -LiteralPath (Join-Path $OutDirDev "$name.yaml") -Raw
        Assert-True (-not [regex]::IsMatch($yaml, '(?m)^[ \t]+enable_rate_limit:[ \t]*true')) `
            "dev $name.yaml 不得被非 -Prod 生成注入 enable_rate_limit: true(dev 保持零值 false)"
    }
} finally {
    foreach ($dir in $OutDirs) {
        if (-not (Test-Path -LiteralPath $dir -PathType Container)) { continue }
        $resolved = [System.IO.Path]::GetFullPath($dir)
        $temp = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
        if (-not $resolved.StartsWith($temp, [StringComparison]::OrdinalIgnoreCase) -or
            (Split-Path -Leaf $resolved) -notmatch '^pandora-gen-ratelimit-(?:prod|dev)-[0-9a-f]{32}$') {
            throw "拒绝清理未验证测试目录:$resolved"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}

Write-Host 'gen_cluster_prod_ratelimit_contract_test: PASS' -ForegroundColor Green
