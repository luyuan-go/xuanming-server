<#
.SYNOPSIS
  后端 CI 构建入口:按 go.work 的 use 清单逐模块 go build + go test。

.DESCRIPTION
  供 Jenkins(仓库根 Jenkinsfile)或本机手工调用。不做镜像构建/发布 —— 那是
  publish_offline_images.ps1 的职责,由流水线在测试全绿后单独调用。
  任何模块失败立即整体失败,不吞错(AGENTS.md §8)。

.PARAMETER RequireDbTests
  强制 MySQL/TiDB 组的真实后端用例执行。Redis/Kafka/etcd 仍是可选组：缺环境会明确
  告警为 SKIP，但不会被本开关误升级为数据库门禁失败。

.PARAMETER CiDbStateFile
  ci_db.ps1 -Action Up 生成的状态 JSON。只从中导入三个数据库测试 DSN；值不打印，
  防止 Jenkins 子进程之间环境不继承导致“库已启动但 go test 看不到变量”。

.EXAMPLE
  pwsh tools/scripts/ci_backend.ps1

.EXAMPLE
  # 带真实数据库跑(依赖门控用例才会真正执行):
  $env:PANDORA_TEST_MYSQL_DSN = 'root:pandora_dev_root@tcp(127.0.0.1:3307)/'
  $env:PANDORA_TEST_TIDB_DSN  = 'root:@tcp(127.0.0.1:4000)/'
  pwsh tools/scripts/ci_backend.ps1 -RequireDbTests
#>
[CmdletBinding()]
param(
    [switch]$RequireDbTests,
    [string]$CiDbStateFile
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../..").Path

. (Join-Path $PSScriptRoot 'lib/go_test_skip_audit.ps1')

if (-not [string]::IsNullOrWhiteSpace($CiDbStateFile)) {
    $resolvedState = (Resolve-Path -LiteralPath $CiDbStateFile -ErrorAction Stop).Path
    $state = Get-Content -Raw -LiteralPath $resolvedState | ConvertFrom-Json -ErrorAction Stop
    # 数据库组:缺一个就 throw。它们是 -RequireDbTests 的硬门禁对象。
    $allowedDbEnv = @(
        'PANDORA_TEST_MYSQL_DSN'
        'PANDORA_TEST_TIDB_DSN'
        'PANDORA_TIDB_TEST_DSN'
    )
    # 可选依赖组(ci-db 状态 v2 起提供):缺失只降级为"本轮未验证"告警,不阻断,
    # 这样 v1 状态文件与临时精简栈仍能跑。**仍是白名单**——状态文件不能注入
    # 这两张表之外的任何变量。
    $allowedOptionalEnv = @(
        'PANDORA_TEST_REDIS_ADDR'
        'PANDORA_TEST_REDIS8_ADDR'
        'PANDORA_TEST_REDIS8_PASSWORD'
        'PANDORA_TEST_ETCD_ENDPOINTS'
        'PANDORA_TEST_KAFKA_BROKERS'
    )
    foreach ($envName in $allowedDbEnv) {
        $prop = $state.environment.PSObject.Properties[$envName]
        if ($null -eq $prop -or [string]::IsNullOrWhiteSpace([string]$prop.Value)) {
            throw "CI DB 状态缺少 $envName：$resolvedState"
        }
        [Environment]::SetEnvironmentVariable($envName, [string]$prop.Value)
    }
    $importedOptional = @()
    foreach ($envName in $allowedOptionalEnv) {
        $prop = $state.environment.PSObject.Properties[$envName]
        if ($null -eq $prop -or [string]::IsNullOrWhiteSpace([string]$prop.Value)) { continue }
        [Environment]::SetEnvironmentVariable($envName, [string]$prop.Value)
        $importedOptional += $envName
    }
    Write-Host "[INFO] 已从 CI DB 状态导入数据库测试环境（值已隐藏）：$resolvedState" -ForegroundColor Cyan
    if ($importedOptional.Count -gt 0) {
        Write-Host ("[INFO] 同时导入可选依赖环境（值已隐藏）：{0}" -f ($importedOptional -join ', ')) -ForegroundColor Cyan
    }
}

if (-not $RequireDbTests -and $env:PANDORA_CI_REQUIRE_DB_TESTS -in @('1', 'true', 'True', 'yes')) {
    $RequireDbTests = $true
}

# ---- 解析 go.work 的 use 清单(支持单行 use 与 use ( ... ) 块) ----
$goWork = Join-Path $ProjectRoot 'go.work'
if (-not (Test-Path -LiteralPath $goWork)) { throw "找不到 go.work:$goWork" }
$modules = @()
$inBlock = $false
foreach ($line in Get-Content -LiteralPath $goWork) {
    $t = ($line -replace '//.*$', '').Trim()
    if (-not $t) { continue }
    if ($t -match '^use\s*\($') { $inBlock = $true; continue }
    if ($inBlock) {
        if ($t -eq ')') { $inBlock = $false; continue }
        $modules += $t
        continue
    }
    if ($t -match '^use\s+(\S+)$') { $modules += $Matches[1] }
}
if ($modules.Count -eq 0) { throw 'go.work 未解析到任何 use 模块。' }

Write-Host "[INFO] go.work 模块数:$($modules.Count)" -ForegroundColor Cyan
$goVersion = (go env GOVERSION 2>$null | Out-String).Trim()
Write-Host "[INFO] Go:$goVersion" -ForegroundColor Cyan

# 依赖门控 DSN 一览:先打出来,让日志顶部就能看清本轮到底具备哪些验证能力。
Write-Host '[INFO] 依赖门控环境变量:' -ForegroundColor Cyan
foreach ($envName in (Get-PandoraGatedEnvNames)) {
    $isSet = -not [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($envName))
    $mark = if ($isSet) { '已设置' } else { '未设置(相关用例将被跳过)' }
    $color = if ($isSet) { 'Green' } else { 'DarkYellow' }
    Write-Host ("       {0,-30} {1}" -f $envName, $mark) -ForegroundColor $color
}

$failed = @()
$allGatedSkips = [System.Collections.Generic.List[pscustomobject]]::new()
$totalPassed = 0
$totalSkipped = 0

foreach ($m in $modules) {
    $dir = Join-Path $ProjectRoot ($m -replace '^\./', '' -replace '/', '\')
    if (-not (Test-Path -LiteralPath $dir)) { $failed += "$m(目录不存在)"; continue }
    Write-Host "`n===== $m =====" -ForegroundColor Magenta
    Push-Location $dir
    try {
        go build ./...
        if ($LASTEXITCODE -ne 0) { $failed += "$m(build)"; continue }

        # -json 而非裸 go test:裸输出对「全部用例都 Skip 的包」只会打一个 ok,
        # 与真跑过无法区分(见 lib/go_test_skip_audit.ps1 头注释)。
        # Console 字段把人类可读输出逐字还原,所以日志观感与改造前一致。
        $raw = & go test ./... -count=1 -json 2>&1 | ForEach-Object {
            if ($_ -is [System.Management.Automation.ErrorRecord]) { $_.ToString() } else { [string]$_ }
        }
        $testExit = $LASTEXITCODE
        $audit = Get-GoTestSkipAudit -JsonLines $raw
        $audit.Console | ForEach-Object { Write-Host $_ }
        $totalPassed += $audit.Passed
        $totalSkipped += $audit.Skipped
        foreach ($g in $audit.GatedSkips) { $allGatedSkips.Add($g) }
        if ($testExit -ne 0 -or $audit.BuildFailures.Count -gt 0) {
            if ($audit.BuildFailures.Count -gt 0) {
                Write-Host ("[ERR ] Go 1.26 build-fail: {0}" -f ($audit.BuildFailures -join ', ')) -ForegroundColor Red
            }
            $failed += "$m(test)"
            continue
        }
    } finally { Pop-Location }
}

if ($failed.Count -gt 0) {
    Write-Host "`n[ERR ] 以下模块未通过:" -ForegroundColor Red
    $failed | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}

# ---- 依赖门控跳过审计(2026-08-11)----
#
# 为什么这是门禁而不是日志:friend/mission 两条**确定性** 1213 死锁在真 MySQL 上必现、
# 在 TiDB 上不现,而 CI 从不设 DSN → 相关用例全 Skip → `go test` 打 ok → 流水线长期绿。
# 缺陷不是没被测试覆盖,是覆盖被"跳过等于通过"吃掉了。
$policy = Test-PandoraGatedSkipPolicy -GatedSkips $allGatedSkips.ToArray() -RequireDbTests:$RequireDbTests
if ($policy.Warnings.Count -gt 0) {
    Write-Host "`n[WARN] 依赖门控用例未执行 —— 本轮绿灯**不覆盖**下列范围:" -ForegroundColor Yellow
    $policy.Warnings | ForEach-Object { Write-Host "  ! $_" -ForegroundColor Yellow }
    Write-Host '       MySQL/TiDB 组可用 -RequireDbTests 强制；Redis/Kafka/etcd 可选组仍保持未验证告警。' -ForegroundColor Yellow
}
if ($policy.Violations.Count -gt 0) {
    Write-Host "`n[ERR ] 依赖门控门禁失败:" -ForegroundColor Red
    $policy.Violations | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}

$gatedCount = $allGatedSkips.Count
Write-Host ("`n[ OK ] 全部 {0} 个模块 build + test 通过(用例 通过={1} 跳过={2},其中依赖门控跳过={3})。" -f `
        $modules.Count, $totalPassed, $totalSkipped, $gatedCount) -ForegroundColor Green

# ---- 集群配置生成器契约测试(R11 复审 P0-3)----
#
# 为什么必须进 CI:这些 ps1 断言的是**生成器产物**的方向性契约(如 -Prod 必须把 login 的
# hub_allocator 地址改写成 dns:///hub-allocator-headless FQDN、非 -Prod 必须保留短名)。
# 生成的集群配置 **不入版本库**(.gitignore 的 run/),所以除了这些断言之外没有任何东西
# 能挡住生成器回归 —— 而在此之前 CI 只跑 go test,这些脚本一次都没被执行过,
# 等于"有测试但不是门禁"。
#
# 只登记**当前绿**的脚本。基线即红的(gen_cluster_b1_contract_test 卡在未实现的
# placement 分权 key 注入)不登记:把已知红的脚本塞进 CI 只会让整条流水线长期红,
# 从而掩盖真实回归。要登记它必须先把那条特性做完或明确退役。
# 2026-07-27:补登记三个此前一直绿却没进门禁的脚本 + 新增的 account 契约测试。
# 只写进 tools/scripts/README.md 的表格不算门禁 —— 那是文档,这个数组才是 CI 实际执行的清单。
$contractTests = @(
    'tools/scripts/tests/gen_cluster_prod_progress_contract_test.ps1'
    'tools/scripts/tests/gen_cluster_prod_owner_contract_test.ps1'
    'tools/scripts/tests/gen_cluster_prod_ratelimit_contract_test.ps1'
    'tools/scripts/tests/gen_cluster_session_gate_contract_test.ps1'
    # -Prod 账号库 TiDB DSN + login 开发后门关断(免密登录曾随 -Prod 产物出厂,见脚本头注释)。
    'tools/scripts/tests/gen_cluster_prod_account_contract_test.ps1'
    # Team→Matchmaker 服务身份 key 必须两端成对且与 login 那把不同(漏配只表现为
    # 「招募列表恒空 + 入队被拒」,两端进程都启动成功,人工 review 抓不住)。
    'tools/scripts/tests/gen_cluster_team_resume_auth_contract_test.ps1'
    # 策划一键导表失败时的 SVN 归因(取版本号 / 判未提交)。判错方向 = 把人指到错的地方。
    'tools/scripts/tests/configtable_gen_svn_status_test.ps1'
    # 依赖门控跳过审计本身的门禁(2026-08-11)。它守的是**本脚本上面那段审计会不会失灵**:
    # 误判成"全跑过"就等于把 friend/mission 那类只在真库复现的缺陷重新放回黑箱。
    'tools/scripts/tests/go_test_skip_audit_contract_test.ps1'
    # Jenkins 一次性 MySQL/TiDB 生命周期、回环隔离、无库名 DSN 与 post always 清理。
    'tools/scripts/tests/ci_db_contract_test.ps1'
    # 策划一键启动的两条护栏(2026-08-12):dev.env 自举 + 启动失败必须带出非零退出码。
    # 后者尤其需要门禁 —— 它的回归表现是「窗口报绿、后端没起来」,人眼 review 最容易放过。
    'tools/scripts/tests/oneclick_devenv_exitcode_contract_test.ps1'
    # MinIO 分发必须识别 mc "exit 0 + status=error"，且 latest.json 只能在内容完成后切换。
    'tools/scripts/tests/publish_to_minio_contract_test.ps1'
    # 免 Docker 三入口的 PowerShell 7 自举(2026-08-18):钉死的 sha256 必须真拦得住假包
    # (自举包会被直接执行,而取包路径有共享盘和公网两条不受 HTTPS 保护),且入口 .cmd 必须
    # 纯 ASCII —— 非 ASCII 字节会让 cmd 按当前代码页算错偏移去执行注释行碎片(2026-08-06 现场)。
    'tools/scripts/tests/pwsh_bootstrap_contract_test.ps1'
    # DS 面身份头剥离清单(2026-08-18):同一份清单被 deploy/envoy/envoy.yaml 的 :8444 与
    # deploy/k8s/agones/16-ds-envoy.yaml 各写一遍,加新身份头的人只会改自己在用的那份。
    # 集群那份漏剥 = 该头在生产上可被任意调用方伪造(实测曾漏 account-id 与 client-ip)。
    'tools/scripts/tests/envoy_ds_identity_header_strip_contract_test.ps1'
    # 客户端仓 / 策划表根目录定位(2026-08-18):本地目录名和开发机不一样就找不到表 =
    # 一键启动在**第一步导表**就中止,整套后端起不来。这个回归没有任何 go test 能挡
    # (逻辑全在 ps1 里),而且开发机 F:\work\Pandora-Client-SVN 永远是绿的 ——
    # 只有按 SVN 原名(Client)或自定义名检出的机器才炸,人工 review 也看不出来。
    'tools/scripts/tests/configtable_client_repo_resolve_test.ps1'
)
$contractFailed = @()
foreach ($rel in $contractTests) {
    $path = Join-Path $ProjectRoot ($rel -replace '/', '\')
    if (-not (Test-Path -LiteralPath $path)) { $contractFailed += "$rel(缺文件)"; continue }
    Write-Host "`n===== 契约测试 $rel =====" -ForegroundColor Magenta
    & pwsh -NoProfile -File $path
    if ($LASTEXITCODE -ne 0) { $contractFailed += $rel }
}
if ($contractFailed.Count -gt 0) {
    Write-Host "`n[ERR ] 以下契约测试未通过:" -ForegroundColor Red
    $contractFailed | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}
Write-Host "[ OK ] 契约测试全部通过($($contractTests.Count) 个)。" -ForegroundColor Green
