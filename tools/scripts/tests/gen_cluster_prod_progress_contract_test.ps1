# gen_cluster_prod_progress_contract_test — -Prod 产物必须机械关断实时成长(审核 P0,2026-07-21)。
#
# 契约:生成链以 dev 模板为唯一输入,dev 的 progress_enabled: true 绝不允许被 -Prod
# 产物继承(混版双发掉落)。玩家经验曲线只走 configtable，集群路径固定且无 YAML 双数据源。
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$Generator = Join-Path $ProjectRoot 'tools/scripts/gen_cluster_config.ps1'
$OutDirProd = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-prodprog-prod-' + [guid]::NewGuid().ToString('N'))
$OutDirDev = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-prodprog-dev-' + [guid]::NewGuid().ToString('N'))
$OutDirs = @($OutDirProd, $OutDirDev)

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

try {
    # ── 1) -Prod 全量真(测试假)密钥生成必须成功,且实时成长被机械关断 ──
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
        -OwnerStoreDsn 'prod_owner:prod-owner-pwd-010@tcp(tidb.pandora.svc:4000)/pandora_owner?parseTime=true&loc=UTC' `
        -AccountStoreDsn 'prod_login:prod-acct-pwd-011@tcp(tidb.pandora.svc:4000)/pandora_account?parseTime=true&loc=UTC' `
        -DsAuthMode enforce -DsAuthorityMode redis -DsFenceEtcdEndpoints 'https://etcd.pandora.svc:2379' `
        -DsFenceKeysetRevision 'pandora-ds-auth-v2-prod-r1' `
        -DsTicketActiveKid ('P' * 43) -DsTicketKeysetRevision 9 *> $null
    if ($LASTEXITCODE -ne 0) { throw "gen_cluster_config -Prod 生成失败(exit=$LASTEXITCODE)" }

    $battleProd = Get-Content -LiteralPath (Join-Path $OutDirProd 'battle-result.yaml') -Raw
    Assert-True (([regex]::Matches($battleProd, '(?m)^[ \t]{2}progress_enabled:[ \t]*false[ \t]*$')).Count -eq 1) `
        '-Prod battle-result 必须恰好一处 progress_enabled: false'
    Assert-True (-not [regex]::IsMatch($battleProd, '(?m)^[ \t]{2}progress_enabled:[ \t]*true')) `
        '-Prod battle-result 不得残留 progress_enabled: true'

    $playerProd = Get-Content -LiteralPath (Join-Path $OutDirProd 'player.yaml') -Raw
    Assert-True (([regex]::Matches($playerProd, '(?m)^[ \t]{2}dir:[ \t]*"/app/configtable/active"[ \t]*$')).Count -eq 1) `
        '-Prod player 必须读取 Pod 挂载的 configtable active 目录'
    foreach ($matchmakerName in @('matchmaker', 'matchmaker-pve')) {
        $matchmakerProd = Get-Content -LiteralPath (Join-Path $OutDirProd "$matchmakerName.yaml") -Raw
        Assert-True (([regex]::Matches($matchmakerProd, '(?m)^[ \t]{2}dir:[ \t]*"/app/configtable/active"[ \t]*$')).Count -eq 1) `
            "-Prod $matchmakerName 必须读取 Pod 挂载的 configtable active 目录"
    }
    Assert-True (-not $playerProd.Contains('exp_curve:')) '-Prod player 不得残留 YAML exp_curve'
    Assert-True (([regex]::Matches($playerProd, '(?m)^[ \t]{2}experience_enabled:[ \t]*false[ \t]*$')).Count -eq 1) `
        '-Prod player 必须机械关闭 experience_enabled(策划数值尚未正式确认)'
    foreach ($cleanupKey in @('exp_history_cleanup_enabled', 'history_cleanup_enabled')) {
        Assert-True (([regex]::Matches($playerProd, '(?m)^[ \t]{2}' + $cleanupKey + ':[ \t]*false[ \t]*$')).Count -eq 1) `
            "-Prod player 必须恰好一处 ${cleanupKey}: false(上游无有界重试,清收据迟到重放会双发)"
        Assert-True (-not [regex]::IsMatch($playerProd, '(?m)^[ \t]{2}' + $cleanupKey + ':[ \t]*true')) `
            "-Prod player 不得残留 ${cleanupKey}: true"
    }
    # §9.24 总闸:产物里残留 "delete" 会让运维误以为清理已生效,且将来新表组若直接读
    # RetentionMode() 而没有自己的前置闸,生产会静默开始删。
    Assert-True (([regex]::Matches($playerProd, '(?m)^[ \t]{2}retention_mode:[ \t]*"report_only"[ \t]*$')).Count -eq 1) `
        '-Prod player 必须恰好一处 retention_mode: "report_only"(§9.24 默认只报告不删)'
    Assert-True (-not [regex]::IsMatch($playerProd, '(?m)^[ \t]{2}retention_mode:[ \t]*"delete"')) `
        '-Prod player 不得残留 retention_mode: "delete"'

    # P0#5 收口(2026-07-25):-Prod 的 login 必须指向 hub-allocator headless FQDN,
    # 否则 round_robin 拿不到多后端,AssignHub 重试会被钉在同一个(可能非-writer)Pod。
    $loginProd = Get-Content -LiteralPath (Join-Path $OutDirProd 'login.yaml') -Raw
    Assert-True (([regex]::Matches($loginProd, '(?m)^[ \t]{4}addr:[ \t]*"dns:///hub-allocator-headless\.pandora\.svc\.cluster\.local:20021"[ \t]*$')).Count -eq 1) `
        '-Prod login 必须恰好一处 hub.addr = dns:///hub-allocator-headless...:20021(单写者 round_robin 前提)'
    Assert-True (-not [regex]::IsMatch($loginProd, '(?m)^[ \t]{4}addr:[ \t]*"hub-allocator:20021"')) `
        '-Prod login 不得残留被 L4 钉住的 ClusterIP 短名 hub 地址'

    $pushProd = Get-Content -LiteralPath (Join-Path $OutDirProd 'push.yaml') -Raw
    Assert-True (([regex]::Matches($pushProd, '(?m)^[ \t]{2}require_session_gate:[ \t]*true[ \t]*$')).Count -eq 1) `
        '-Prod push 必须恰好一处 require_session_gate: true(P0 INC-20260722-004:旧 token 不得建流)'
    Assert-True (-not [regex]::IsMatch($pushProd, '(?m)^[ \t]{2}require_session_gate:[ \t]*false')) `
        '-Prod push 不得残留 require_session_gate: false'

    # ── 2) 非 -Prod(本地 dev):联调开关保留,配置表路径改为集群挂载点 ──
    & pwsh -NoProfile -File $Generator -OutDir $OutDirDev -AllocatorMode agones `
        -AllocatorAdvertiseHost 127.0.0.1 -AllowDevSecrets `
        -DsAuthMode enforce -DsAuthorityMode redis -DsFenceEtcdEndpoints 'etcd.pandora.svc:2379' `
        -DsFenceKeysetRevision 'pandora-ds-auth-v2-local-r1' `
        -DsTicketActiveKid ('A' * 43) -DsTicketKeysetRevision 7 *> $null
    if ($LASTEXITCODE -ne 0) { throw "gen_cluster_config dev 生成失败(exit=$LASTEXITCODE)" }

    $battleDev = Get-Content -LiteralPath (Join-Path $OutDirDev 'battle-result.yaml') -Raw
    Assert-True ([regex]::IsMatch($battleDev, '(?m)^[ \t]{2}progress_enabled:[ \t]*true')) `
        'dev battle-result 联调 progress_enabled: true 不得被非 -Prod 生成改写'
    $playerDev = Get-Content -LiteralPath (Join-Path $OutDirDev 'player.yaml') -Raw
    Assert-True (([regex]::Matches($playerDev, '(?m)^[ \t]{2}dir:[ \t]*"/app/configtable/active"[ \t]*$')).Count -eq 1) `
        'dev player 必须读取 Pod/容器挂载的 configtable active 目录'
    foreach ($matchmakerName in @('matchmaker', 'matchmaker-pve')) {
        $matchmakerDev = Get-Content -LiteralPath (Join-Path $OutDirDev "$matchmakerName.yaml") -Raw
        Assert-True (([regex]::Matches($matchmakerDev, '(?m)^[ \t]{2}dir:[ \t]*"/app/configtable/active"[ \t]*$')).Count -eq 1) `
            "dev $matchmakerName 必须读取 Pod/容器挂载的 configtable active 目录"
    }
    Assert-True (-not $playerDev.Contains('exp_curve:')) 'dev player 不得残留 YAML exp_curve'
    Assert-True (([regex]::Matches($playerDev, '(?m)^[ \t]{2}experience_enabled:[ \t]*true[ \t]*$')).Count -eq 1) `
        'dev player 应保留 experience_enabled 联调开关'
    foreach ($cleanupKey in @('exp_history_cleanup_enabled', 'history_cleanup_enabled')) {
        Assert-True ([regex]::IsMatch($playerDev, '(?m)^[ \t]{2}' + $cleanupKey + ':[ \t]*true')) `
            "dev player 联调 ${cleanupKey}: true 不得被非 -Prod 生成改写"
    }
    Assert-True ([regex]::IsMatch($playerDev, '(?m)^[ \t]{2}retention_mode:[ \t]*"delete"[ \t]*$')) `
        'dev player 联调 retention_mode: "delete" 不得被非 -Prod 生成改写(真删代码路径要有人跑)'
    # 非 -Prod 产物同时服务 docker-compose(headless FQDN 在 compose 内不可解析),必须保持短名。
    $loginDev = Get-Content -LiteralPath (Join-Path $OutDirDev 'login.yaml') -Raw
    Assert-True (([regex]::Matches($loginDev, '(?m)^[ \t]{4}addr:[ \t]*"hub-allocator:20021"[ \t]*$')).Count -eq 1) `
        'dev/compose 共用产物必须保持 hub-allocator 短名(headless FQDN 在 compose 内不可解析)'

    $pushDev = Get-Content -LiteralPath (Join-Path $OutDirDev 'push.yaml') -Raw
    Assert-True ([regex]::IsMatch($pushDev, '(?m)^[ \t]{2}require_session_gate:[ \t]*false[ \t]*$')) `
        'dev push 联调 require_session_gate: false 不得被非 -Prod 生成改写'
} finally {
    foreach ($dir in $OutDirs) {
        if (-not (Test-Path -LiteralPath $dir -PathType Container)) { continue }
        $resolved = [System.IO.Path]::GetFullPath($dir)
        $temp = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
        if (-not $resolved.StartsWith($temp, [StringComparison]::OrdinalIgnoreCase) -or
            (Split-Path -Leaf $resolved) -notmatch '^pandora-gen-prodprog-(?:prod|dev)-[0-9a-f]{32}$') {
            throw "拒绝清理未验证测试目录:$resolved"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}

Write-Host 'gen_cluster_prod_progress_contract_test: PASS' -ForegroundColor Green
