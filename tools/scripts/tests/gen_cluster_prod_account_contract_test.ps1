# gen_cluster_prod_account_contract_test — -Prod 产物 login 账号库与开发后门契约(2026-07-27)。
#
# 背景(本次审计实测发现,两条都属发布阻断级):
#   ① -Prod 产物的 login.yaml 原样继承 dev 模板的三个开关 dev_skip_password /
#      dev_auto_register / dev_allow_any_role = true —— **生产任意账号 + 任意密码都能登录
#      并自动开号**。生成器 Set-Prod* 家族此前完全没有覆盖这三项;名义上的检查器
#      release_preflight.ps1 无任何调用方、默认路径是失效的历史绝对路径、glob '*-prod.yaml'
#      在本仓匹配 0 个文件,且扫的是 services/ 下模板而非发布产物,三重失效。
#   ② pandora_account 是全服写压力汇聚点(每次登录一个定序事务,写 QPS = 全服登录 QPS),
#      而 -Prod 产物此前带着公开 dev 凭据 pandora_dev_pwd@mysql:3306。
#
# 契约:
#   1. -Prod 必须显式提供真 TiDB DSN(-AccountStoreDsn / PANDORA_ACCOUNT_TIDB_DSN);
#      缺失、dev 凭据、dev mysql 地址、非 pandora_account 库、collation=utf8mb4_bin 一律拒绝。
#      (utf8mb4_bin 单列出来:accounts.account 是客户端上报账号名 + 唯一键且 Go 侧零归一化,
#       改 _bin 会大小写敏感化 + PAD SPACE 化,老玩家登不进并可被同名抢注。)
#   2. -Prod 产物 login.yaml:DSN 已注入 + require_tidb: true + 三个 dev 开关全为 false。
#   3. -Prod 产物已接线 DSN 注入的服务(owner / login)不得残留 pandora_dev_pwd。
#   4. 非 -Prod(本地 dev)行为不变:dev mysql DSN / require_tidb: false / 三个开关仍为 true。
#
# 负向用例一律断言**稳定 ASCII 错误码**而不只是退出码非零:-Prod 校验项很多,只判 exit!=0
# 会让用例在「因为另一个原因被拒」时继续 PASS,变成证明不了任何事的空转测试。不能匹配
# 中文错误文本:Windows Jenkins 的 cmd OEM code page 会把子 pwsh 输出转成 `?`,造成假失败。
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$env:PANDORA_OWNER_TIDB_DSN = $null
$env:PANDORA_ACCOUNT_TIDB_DSN = $null

$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$Generator = Join-Path $ProjectRoot 'tools/scripts/gen_cluster_config.ps1'
$OutDirProd = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-prodaccount-prod-' + [guid]::NewGuid().ToString('N'))
$OutDirDev = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-prodaccount-dev-' + [guid]::NewGuid().ToString('N'))
$OutDirNeg = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-prodaccount-neg-' + [guid]::NewGuid().ToString('N'))
$OutDirs = @($OutDirProd, $OutDirDev, $OutDirNeg)

$GoodOwnerDsn = 'prod_owner:prod-owner-pwd-010@tcp(tidb.pandora.svc:4000)/pandora_owner?parseTime=true&loc=UTC'
$GoodAccountDsn = 'prod_login:prod-acct-pwd-011@tcp(tidb.pandora.svc:4000)/pandora_account?parseTime=true&loc=UTC&charset=utf8mb4&collation=utf8mb4_0900_ai_ci'

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

# 跑一次 -Prod 生成;$AccountDsnArgs 由用例控制。返回 @{ Code; Output }。
function Invoke-ProdGen([string[]]$AccountDsnArgs, [string]$OutDir) {
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
        '-AllocationAbortAuthSecret', 'prod-allocation-abort-auth-0123456789abcdef-009',
        '-DsAuthMode', 'enforce', '-DsAuthorityMode', 'redis',
        '-DsFenceEtcdEndpoints', 'https://etcd.pandora.svc:2379',
        '-DsFenceKeysetRevision', 'pandora-ds-auth-v2-prod-r1',
        '-DsTicketActiveKid', ('P' * 43), '-DsTicketKeysetRevision', '9',
        # owner DSN 恒传合法值:本测试证明的是 account 侧契约,不能因缺 owner DSN 而被拒。
        '-OwnerStoreDsn', $GoodOwnerDsn)
    $prodArgs += $AccountDsnArgs
    $output = (& pwsh -NoProfile -File $Generator @prodArgs 2>&1 | Out-String)
    return @{ Code = $LASTEXITCODE; Output = $output }
}

# 负向:必须拒绝,且错误文本包含 $MustContain(证明是因为**这个**原因被拒)。
function Assert-Rejected([string[]]$AccountDsnArgs, [string]$MustContain, [string]$Reason) {
    $r = Invoke-ProdGen $AccountDsnArgs $OutDirNeg
    Assert-True ($r.Code -ne 0) $Reason
    Assert-True ($r.Output.Contains($MustContain)) `
        "$Reason —— 已拒绝但原因不符,错误文本未包含 '$MustContain'(可能因其它校验被拒,用例空转)。实际输出:$($r.Output)"
}

try {
    # ── 1) -Prod 负向:缺失 / dev 凭据 / dev mysql / 错库 / 错 collation ──
    Assert-Rejected @() '[ACCOUNT_DSN_REQUIRED]' `
        '-Prod 缺 account TiDB DSN 必须拒绝生成'
    Assert-Rejected @('-AccountStoreDsn', 'pandora:pandora_dev_pwd@tcp(tidb.pandora.svc:4000)/pandora_account?parseTime=true') `
        '[ACCOUNT_DSN_DEV_CREDENTIAL]' '-Prod account DSN 含公开 dev 凭据必须拒绝'
    Assert-Rejected @('-AccountStoreDsn', 'prod_login:real-pwd-x@tcp(mysql:3306)/pandora_account?parseTime=true') `
        '[ACCOUNT_DSN_DEV_MYSQL]' '-Prod account DSN 指向 dev MySQL(mysql:3306)必须拒绝'
    Assert-Rejected @('-AccountStoreDsn', 'prod_login:real-pwd-x@tcp(tidb.pandora.svc:4000)/pandora_player?parseTime=true') `
        '[ACCOUNT_DSN_DATABASE]' '-Prod account DSN 未指向 pandora_account 库必须拒绝'
    Assert-Rejected @('-AccountStoreDsn', 'prod_login:real-pwd-x@tcp(tidb.pandora.svc:4000)/pandora_account?collation=utf8mb4_bin') `
        '[ACCOUNT_DSN_COLLATION]' '-Prod account DSN 用 utf8mb4_bin 必须拒绝(账号名唯一键语义翻转)'

    # ── 2) -Prod 正向:DSN 落位 + require_tidb + dev 后门全关 ──
    $ok = Invoke-ProdGen @('-AccountStoreDsn', $GoodAccountDsn) $OutDirProd
    Assert-True ($ok.Code -eq 0) "-Prod 带合法 account TiDB DSN 应生成成功。输出:$($ok.Output)"

    $loginProd = Get-Content -LiteralPath (Join-Path $OutDirProd 'login.yaml') -Raw
    Assert-True ($loginProd.Contains('tidb.pandora.svc:4000') -and $loginProd.Contains('/pandora_account')) `
        '-Prod login.yaml 必须注入 TiDB DSN'
    Assert-True (-not $loginProd.Contains('pandora_dev_pwd')) `
        '-Prod login.yaml 不得残留 dev 凭据'
    Assert-True (-not $loginProd.Contains('mysql:3306')) `
        '-Prod login.yaml 不得残留 dev MySQL 地址'
    Assert-True (([regex]::Matches($loginProd, '(?m)^[ \t]{2}require_tidb:[ \t]*true[ \t]*$')).Count -eq 1) `
        '-Prod login.yaml 必须恰好一处 require_tidb: true(服务端启动强校验 TiDB + collation 行为)'
    Assert-True (-not [regex]::IsMatch($loginProd, '(?m)^[ \t]{2}require_tidb:[ \t]*false')) `
        '-Prod login.yaml 不得残留 require_tidb: false'

    foreach ($key in @('dev_skip_password', 'dev_auto_register', 'dev_allow_any_role')) {
        Assert-True (([regex]::Matches($loginProd, "(?m)^[ \t]{2}$key`:[ \t]*false[ \t]*$")).Count -eq 1) `
            "-Prod login.yaml 必须恰好一处 $key`: false(生产开发后门必须关断)"
        Assert-True (-not [regex]::IsMatch($loginProd, "(?m)^[ \t]{2}$key`:[ \t]*true")) `
            "-Prod login.yaml 不得残留 $key`: true —— 该项为 true 时生产可任意账号/密码登录"
    }

    # ── 3) 已接线 DSN 注入的服务不得残留 dev 库凭据 ──
    foreach ($svc in @('owner', 'login')) {
        $t = Get-Content -LiteralPath (Join-Path $OutDirProd "$svc.yaml") -Raw
        Assert-True (-not $t.Contains('pandora_dev_pwd')) `
            "-Prod $svc.yaml 已接线 DSN 注入,不得残留公开 dev 数据库凭据"
    }

    # ── 4) 非 -Prod(本地 dev)行为不变 ──
    & pwsh -NoProfile -File $Generator -OutDir $OutDirDev -AllocatorMode agones `
        -AllocatorAdvertiseHost 127.0.0.1 -AllowDevSecrets `
        -DsAuthMode enforce -DsAuthorityMode redis -DsFenceEtcdEndpoints 'etcd.pandora.svc:2379' `
        -DsFenceKeysetRevision 'pandora-ds-auth-v2-local-r1' `
        -DsTicketActiveKid ('A' * 43) -DsTicketKeysetRevision 7 *> $null
    if ($LASTEXITCODE -ne 0) { throw "gen_cluster_config dev 生成失败(exit=$LASTEXITCODE)" }

    $loginDev = Get-Content -LiteralPath (Join-Path $OutDirDev 'login.yaml') -Raw
    Assert-True ($loginDev.Contains('pandora_dev_pwd') -and $loginDev.Contains('mysql:3306')) `
        'dev login.yaml 应保留 dev mysql DSN(集群地址改写后)'
    Assert-True ([regex]::IsMatch($loginDev, '(?m)^[ \t]{2}require_tidb:[ \t]*false[ \t]*$')) `
        'dev login.yaml require_tidb: false 不得被非 -Prod 生成改写'
    foreach ($key in @('dev_skip_password', 'dev_auto_register', 'dev_allow_any_role')) {
        Assert-True ([regex]::IsMatch($loginDev, "(?m)^[ \t]{2}$key`:[ \t]*true[ \t]*$")) `
            "dev login.yaml $key`: true 不得被非 -Prod 生成改写(本地联调依赖它)"
    }
} finally {
    foreach ($dir in $OutDirs) {
        if (-not (Test-Path -LiteralPath $dir -PathType Container)) { continue }
        $resolved = [System.IO.Path]::GetFullPath($dir)
        $temp = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
        if (-not $resolved.StartsWith($temp, [StringComparison]::OrdinalIgnoreCase) -or
            (Split-Path -Leaf $resolved) -notmatch '^pandora-gen-prodaccount-(?:prod|dev|neg)-[0-9a-f]{32}$') {
            throw "拒绝清理未验证测试目录:$resolved"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}

Write-Host 'gen_cluster_prod_account_contract_test: PASS' -ForegroundColor Green
