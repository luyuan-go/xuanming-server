# gen_cluster_prod_owner_contract_test — -Prod 产物 owner 权威库与 reflection 契约(§9.22,2026-07-22)。
#
# 契约:
#   1. -Prod 必须显式提供真 TiDB DSN(-OwnerStoreDsn / PANDORA_OWNER_TIDB_DSN);缺失、dev 凭据、
#      dev mysql 地址、非 pandora_owner 库一律拒绝生成 —— owner CAS 依赖线性一致 + 确认写不回滚,
#      MySQL 异步复制切换会回滚已确认写,回滚即可能双 owner(§9.22)。
#   2. -Prod 产物 owner.yaml:DSN 已注入 + require_tidb: true(owner 启动查 VERSION() 含 -TiDB-,
#      不符 fail-fast;与生成器 DSN 字符串校验构成双层防线)。
#   3. -Prod 产物**全部服务** enable_reflection: false(线上不暴露服务面探测)。
#   4. 非 -Prod(本地 dev)行为不变:dev mysql DSN / require_tidb: false / reflection true 原样保留。
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
# 清掉本进程的 owner DSN 环境变量:生成器参数默认取 PANDORA_OWNER_TIDB_DSN,
# 外部若设置会把「缺失必须拒绝」用例兜底成通过(测试以 pwsh -File 独立进程运行,不污染调用方)。
$env:PANDORA_OWNER_TIDB_DSN = $null
# 同理清掉 account DSN 环境变量(2026-07-27 新增 -AccountStoreDsn 后,外部设置会污染用例)。
$env:PANDORA_ACCOUNT_TIDB_DSN = $null
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$Generator = Join-Path $ProjectRoot 'tools/scripts/gen_cluster_config.ps1'
$OutDirProd = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-prodowner-prod-' + [guid]::NewGuid().ToString('N'))
$OutDirDev = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-prodowner-dev-' + [guid]::NewGuid().ToString('N'))
$OutDirNeg = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-prodowner-neg-' + [guid]::NewGuid().ToString('N'))
$OutDirs = @($OutDirProd, $OutDirDev, $OutDirNeg)

$GoodOwnerDsn = 'prod_owner:prod-owner-pwd-010@tcp(tidb.pandora.svc:4000)/pandora_owner?parseTime=true&loc=UTC'

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

# 组装一次完整的 -Prod 参数(密钥全为测试假值);$OwnerDsnArgs 由用例控制。
function Invoke-ProdGen([string[]]$OwnerDsnArgs, [string]$OutDir) {
    $prodArgs = @(
        '-OutDir', $OutDir, '-AllocatorMode', 'agones', '-Prod',
        '-Secret', 'prod-player-key-0123456789abcdef-001',
        '-DsSecret', 'prod-ds-callback-key-0123456789abcdef-002',
        '-PlacementAccountBootstrapSecret', 'prod-placement-bootstrap-0123456789abcdef-003',
        '-PlacementMatchStartSecret', 'prod-placement-match-start-0123456789abcdef-004',
        '-PlacementBattleExitSecret', 'prod-placement-battle-exit-0123456789abcdef-005',
        '-PlacementHubTransferSecret', 'prod-placement-hub-transfer-0123456789abcdef-006',
        '-PlacementBattleDepartureSecret', 'prod-placement-battle-departure-0123456789abcdef-007',
        '-MatchResumeAuthSecret', 'prod-match-resume-auth-0123456789abcdef-008',
        '-TeamResumeAuthSecret', 'prod-team-resume-auth-0123456789abcdef-012',
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
        # -Prod 自 2026-07-27 起同时强制 account 库 TiDB DSN。这里恒传**合法**值:
        # 本测试的负向用例要证明的是「owner DSN 不合规必须拒绝」,若因缺 account DSN 而拒绝,
        # 用例仍会 PASS 但证明的是另一回事(静默空转)。
        '-AccountStoreDsn', 'prod_login:prod-acct-pwd-011@tcp(tidb.pandora.svc:4000)/pandora_account?parseTime=true&loc=UTC')
    $prodArgs += $OwnerDsnArgs
    & pwsh -NoProfile -File $Generator @prodArgs *> $null
    return $LASTEXITCODE
}

try {
    # ── 1) -Prod 负向:缺失 / dev 凭据 / dev mysql 地址 / 非 pandora_owner 库 → 全部拒绝 ──
    Assert-True ((Invoke-ProdGen @() $OutDirNeg) -ne 0) `
        '-Prod 缺 owner TiDB DSN 必须拒绝生成'
    Assert-True ((Invoke-ProdGen @('-OwnerStoreDsn', 'pandora:pandora_dev_pwd@tcp(tidb.pandora.svc:4000)/pandora_owner?parseTime=true') $OutDirNeg) -ne 0) `
        '-Prod owner DSN 含公开 dev 凭据必须拒绝'
    Assert-True ((Invoke-ProdGen @('-OwnerStoreDsn', 'prod_owner:real-pwd-x@tcp(mysql:3306)/pandora_owner?parseTime=true') $OutDirNeg) -ne 0) `
        '-Prod owner DSN 指向 dev MySQL(mysql:3306)必须拒绝'
    Assert-True ((Invoke-ProdGen @('-OwnerStoreDsn', 'prod_owner:real-pwd-x@tcp(tidb.pandora.svc:4000)/pandora_player?parseTime=true') $OutDirNeg) -ne 0) `
        '-Prod owner DSN 未指向 pandora_owner 库必须拒绝'

    # ── 2) -Prod 正向:注入 TiDB DSN → owner.yaml DSN 落位 + require_tidb: true ──
    Assert-True ((Invoke-ProdGen @('-OwnerStoreDsn', $GoodOwnerDsn) $OutDirProd) -eq 0) `
        '-Prod 带合法 TiDB DSN 应生成成功'
    $ownerProd = Get-Content -LiteralPath (Join-Path $OutDirProd 'owner.yaml') -Raw
    Assert-True ($ownerProd.Contains('tidb.pandora.svc:4000') -and $ownerProd.Contains('/pandora_owner')) `
        '-Prod owner.yaml 必须注入 TiDB DSN'
    Assert-True (-not $ownerProd.Contains('pandora_dev_pwd')) `
        '-Prod owner.yaml 不得残留 dev 凭据'
    Assert-True (-not $ownerProd.Contains('mysql:3306')) `
        '-Prod owner.yaml 不得残留 dev MySQL 地址'
    Assert-True (([regex]::Matches($ownerProd, '(?m)^[ \t]{2}require_tidb:[ \t]*true[ \t]*$')).Count -eq 1) `
        '-Prod owner.yaml 必须恰好一处 require_tidb: true(服务端启动强校验 TiDB)'
    Assert-True (-not [regex]::IsMatch($ownerProd, '(?m)^[ \t]{2}require_tidb:[ \t]*false')) `
        '-Prod owner.yaml 不得残留 require_tidb: false'

    # ── 3) -Prod 全部服务 reflection 关断 ──
    $prodYamls = @(Get-ChildItem -LiteralPath $OutDirProd -Filter '*.yaml')
    Assert-True ($prodYamls.Count -ge 21) "-Prod 产物服务配置数异常(实际=$($prodYamls.Count))"
    foreach ($f in $prodYamls) {
        $content = Get-Content -LiteralPath $f.FullName -Raw
        Assert-True (-not [regex]::IsMatch($content, '(?m)^[ \t]+enable_reflection:[ \t]*true')) `
            "-Prod $($f.Name) 不得残留 enable_reflection: true"
    }

    # ── 4) 非 -Prod(本地 dev)行为不变 ──
    & pwsh -NoProfile -File $Generator -OutDir $OutDirDev -AllocatorMode agones `
        -AllocatorAdvertiseHost 127.0.0.1 -AllowDevSecrets `
        -DsAuthMode enforce -DsAuthorityMode redis -DsFenceEtcdEndpoints 'etcd.pandora.svc:2379' `
        -DsFenceKeysetRevision 'pandora-ds-auth-v2-local-r1' `
        -DsTicketActiveKid ('A' * 43) -DsTicketKeysetRevision 7 *> $null
    if ($LASTEXITCODE -ne 0) { throw "gen_cluster_config dev 生成失败(exit=$LASTEXITCODE)" }

    $ownerDev = Get-Content -LiteralPath (Join-Path $OutDirDev 'owner.yaml') -Raw
    Assert-True ($ownerDev.Contains('pandora_dev_pwd') -and $ownerDev.Contains('mysql:3306')) `
        'dev owner.yaml 应保留 dev mysql DSN(集群地址改写后)'
    Assert-True ([regex]::IsMatch($ownerDev, '(?m)^[ \t]{2}require_tidb:[ \t]*false[ \t]*$')) `
        'dev owner.yaml require_tidb: false 不得被非 -Prod 生成改写'
    Assert-True ([regex]::IsMatch($ownerDev, '(?m)^[ \t]+enable_reflection:[ \t]*true')) `
        'dev owner.yaml enable_reflection: true 不得被非 -Prod 生成改写'
} finally {
    foreach ($dir in $OutDirs) {
        if (-not (Test-Path -LiteralPath $dir -PathType Container)) { continue }
        $resolved = [System.IO.Path]::GetFullPath($dir)
        $temp = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
        if (-not $resolved.StartsWith($temp, [StringComparison]::OrdinalIgnoreCase) -or
            (Split-Path -Leaf $resolved) -notmatch '^pandora-gen-prodowner-(?:prod|dev|neg)-[0-9a-f]{32}$') {
            throw "拒绝清理未验证测试目录:$resolved"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}

Write-Host 'gen_cluster_prod_owner_contract_test: PASS' -ForegroundColor Green
