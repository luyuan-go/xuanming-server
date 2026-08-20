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
    # 头顶编号只允许 DS 面精确调用；客户端 catch-all 前必须显式 403，两份 DS Envoy 同步白名单。
    'tools/scripts/tests/envoy_login_ds_player_no_contract_test.ps1'
    # Team→Player 名字解析只走集群内 gRPC 服务身份，不得进入客户端或 DS Envoy。
    'tools/scripts/tests/envoy_player_internal_name_contract_test.ps1'
    # Team→Player 名字解析的独立 key/audience/address 必须由生产生成器成对注入并与其它权限域隔离。
    'tools/scripts/tests/gen_cluster_player_name_resolve_auth_contract_test.ps1'
    # 客户端仓 / 策划表根目录定位(2026-08-18):本地目录名和开发机不一样就找不到表 =
    # 一键启动在**第一步导表**就中止,整套后端起不来。这个回归没有任何 go test 能挡
    # (逻辑全在 ps1 里),而且开发机 F:\work\Pandora-Client-SVN 永远是绿的 ——
    # 只有按 SVN 原名(Client)或自定义名检出的机器才炸,人工 review 也看不出来。
    'tools/scripts/tests/configtable_client_repo_resolve_test.ps1'
    # 本机基础设施起不来时的诊断输出(2026-08-19)。这段代码**只在出故障时才执行**,
    # 正常跑一百次也碰不到一次,它自己有 bug 的表现是「报错处理里再崩一次」——
    # 把一个可查的故障变成不可查的。首版就踩了空数组被拆成 +''+ 导致"日志是空的"
    # 与"日志读不到"两条相反结论撞成同一个值,是这个测试当场抓出来的。
    'tools/scripts/tests/localinfra_failure_diagnostics_test.ps1'
    # 免 Docker MySQL 不能因 TCP 可连或陈旧 PID 就复用/停止机器上已有的 Docker/MySQL。
    'tools/scripts/tests/localinfra_mysql_ownership_test.ps1'
    # 独立端口必须贯穿状态、迁移、14 条服务 DSN 与 DsOnly restart，不能只改 mysqld 一端。
    'tools/scripts/tests/localinfra_mysql_port_flow_test.ps1'
    # 中心 MySQL 客户端：profile/CA 指纹、异步 enrollment+DPAPI/device、secret YAML 与一键生命周期。
    'tools/scripts/tests/mysql_runtime_profile_contract_test.ps1'
    'tools/scripts/tests/planner_mysql_enrollment_contract_test.ps1'
    'tools/scripts/tests/mysql_service_runtime_config_contract_test.ps1'
    'tools/scripts/tests/planner_mysql_oneclick_contract_test.ps1'
    'tools/scripts/tests/planner_mysql_preflight_contract_test.ps1'
    # Windows Get-NetTCPConnection 单次可阻塞数秒；快速 listener seam 仍须保留 PID/exe/my.ini 归属闸。
    'tools/scripts/tests/run_services_listener_query_contract_test.ps1'
    # 策划专用热启动：强指纹复用本机二进制，非 login/login 两波批量启动与 exact-PID 统一就绪。
    'tools/scripts/tests/run_services_planner_fast_start_contract_test.ps1'
    # 标准 Press any key 只能代表玩家链已可玩：login/Envoy/Hub exact owner 三门全过才允许成功退出。
    'tools/scripts/tests/planner_playable_exit_contract_test.ps1'
    # 策划本机 MySQL 热启动：SQL 强收据跳过重复 init DDL，miss 时单 mysql 进程批量重放。
    'tools/scripts/tests/dev_migrate_planner_fast_contract_test.ps1'
    # 策划已安装基础设施的冷启动：批量 launch、共享 listener 轮询与 direct/Kafka-child exact owner。
    'tools/scripts/tests/localinfra_planner_parallel_start_contract_test.ps1'
    # SVN 带包时全程离线、Git 空目录时逐项联网；所有本地来源仍必须过固定 SHA256。
    'tools/scripts/tests/localinfra_bundled_packages_contract_test.ps1'
    # 免 Go 策划机必须随发布包拿到 pandora-migrate.exe；否则旧数据目录会跳过增量迁移。
    'tools/scripts/tests/release_binaries_migrate_contract_test.ps1'
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
# ---- Python 侧门禁(2026-08-19)----
#
# 为什么必须进 CI:python/ 下已有约 2 万行实现 + 近 600 个测试,而在此之前 CI **一次都没跑过**。
# 问题不是"Python 侧没有测试",是"有测试但不是门禁" —— 与上面那段集群配置生成器契约测试
# 当初的处境完全一样。两条真实回归路径此前完全无人挡:
#
#   ① Go 侧改 pkg/errcode 的码值 / 加删错误码 → python/pandorapy/errcode.py 是**生成物**,
#      不同步就是两栈对同一个失败返回不同的码,客户端按码分支即静默走错。
#      python/tools/gen_errcode.py --check 正是为此写的门,没人跑等于白写。
#   ② configtable/dist 重新导表 → Python 侧加载器与跨语言 parity 测试断言的是**真实批次**
#      (checksum / 行数 / 起始节点唯一性),漂移只有跑测试才现形。
#
# 依赖门控沿用**同一套环境变量**(PANDORA_TEST_ETCD_ENDPOINTS / _MYSQL_DSN / _REDIS_ADDR),
# 所以 ci_db.ps1 起的那套库对 Python 用例同样生效,不需要第二套 DSN 管道。
#
# 环境自举:优先用 python/.venv;没有就用 uv 现建(uv 已列进 tools/devops/bootstrap-machine.ps1
# 的前置工具表)。**刻意不做**"没装 Python 就跳过" —— 那正是本文件反复在防的"跳过等于通过"。
$pyRoot = Join-Path $ProjectRoot 'python'
$pyExe = Join-Path $pyRoot '.venv\Scripts\python.exe'
if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    Write-Host "`n[ERR ] 本机没有 uv —— Python 侧门禁无法执行。" -ForegroundColor Red
    Write-Host '       装 uv:winget install --id astral-sh.uv  (或 pwsh tools/devops/bootstrap-machine.ps1 -Install)' -ForegroundColor Red
    exit 1
}
if (-not (Test-Path -LiteralPath $pyExe)) {
    Write-Host "`n===== Python 环境自举(uv venv)=====" -ForegroundColor Magenta
    & uv venv --python 3.13 (Join-Path $pyRoot '.venv')
    if ($LASTEXITCODE -ne 0) { Write-Host '[ERR ] uv venv 失败。' -ForegroundColor Red; exit 1 }
}
# ★ 依赖同步**每轮都跑**,不能只在 .venv 缺失时跑。
# 只在首次建环境时装的话,pyproject.toml 加了依赖之后 CI 机上那个旧 .venv 永远不会更新 ——
# 表现是「本机绿、CI 红」或更糟的「CI 用着旧依赖打绿」。uv 是幂等的且命中缓存时是秒级。
#
# ★ 按**锁文件**装,不按 pyproject 的 `>=` 下界装:后者意味着上游随便发一个新版
# 就能在仓库零改动的情况下把流水线打红(或悄悄换掉一个行为不同的实现)。
# `uv pip sync` 会把环境**收敛**到锁文件(多装的也卸掉),CI 机上因此不会攒出
# 与锁文件不一致的历史环境。改了依赖要重新 compile —— 见 requirements.lock 头注释。
$pyLock = Join-Path $pyRoot 'requirements.lock'
Write-Host "`n===== Python 依赖同步(uv pip sync requirements.lock)=====" -ForegroundColor Magenta
if (-not (Test-Path -LiteralPath $pyLock)) {
    Write-Host "[ERR ] 缺 $pyLock —— 依赖锁是入库文件,不该缺。" -ForegroundColor Red
    Write-Host '       重新生成:cd python && uv pip compile pyproject.toml --extra dev --extra storage --output-file requirements.lock' -ForegroundColor Red
    exit 1
}
& uv pip sync --python $pyExe $pyLock
if ($LASTEXITCODE -ne 0) { Write-Host '[ERR ] uv pip sync 失败。' -ForegroundColor Red; exit 1 }
# 本包自身用 --no-deps 接进去:依赖已由 sync 收敛,这一步只做 editable 安装。
& uv pip install --python $pyExe -e $pyRoot --no-deps
if ($LASTEXITCODE -ne 0) { Write-Host '[ERR ] uv pip install -e 失败。' -ForegroundColor Red; exit 1 }

$pyFailed = @()
$env:PYTHONUTF8 = '1'   # Windows stdout 默认 cp1252,中文日志会整条丢(见 python/README.md)
Push-Location $pyRoot
try {
    Write-Host "`n===== Python 门禁 1/2:errcode 与 Go 侧一致(tools/gen_errcode.py --check)=====" -ForegroundColor Magenta
    & $pyExe tools/gen_errcode.py --check
    if ($LASTEXITCODE -ne 0) { $pyFailed += 'gen_errcode.py --check' }

    Write-Host "`n===== Python 门禁 2/2:pytest =====" -ForegroundColor Magenta
    # -rs 打出全部跳过原因。Python 侧同样有依赖门控用例:实测**缺 etcd/MySQL/Redis 时
    # 592 个用例里有 83 个静默跳过**(14%),而 pytest 照样打绿 —— 与上面 go_test_skip_audit
    # 防的是同一件事,所以这里也按同一口径分组:数据库组随 -RequireDbTests 硬门禁,
    # 其余(etcd / Redis / go 可执行)只告警为"本轮未验证"。
    $pyOut = & $pyExe -m pytest tests/ -q -rs 2>&1
    $pyTestExit = $LASTEXITCODE
    $pyOut | ForEach-Object { Write-Host $_ }
    if ($pyTestExit -ne 0) { $pyFailed += 'pytest' }

    $pySkips = @($pyOut | Where-Object { $_ -match '^SKIPPED' })
    $pyDbSkips = @($pySkips | Where-Object { $_ -match 'MySQL|TiDB' })
    if ($pySkips.Count -gt 0) {
        Write-Host ("`n[WARN] Python 用例跳过 {0} 条 —— 本轮绿灯**不覆盖**这些范围:" -f $pySkips.Count) -ForegroundColor Yellow
        $pySkips | Select-Object -Unique | ForEach-Object { Write-Host "  ! $_" -ForegroundColor Yellow }
    }
    if ($RequireDbTests -and $pyDbSkips.Count -gt 0) {
        Write-Host "`n[ERR ] -RequireDbTests 已开,但下列 Python 数据库用例仍被跳过:" -ForegroundColor Red
        $pyDbSkips | Select-Object -Unique | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
        $pyFailed += 'pytest 数据库门控用例被跳过'
    }
} finally {
    Pop-Location
}
if ($pyFailed.Count -gt 0) {
    Write-Host "`n[ERR ] Python 侧门禁未通过:" -ForegroundColor Red
    $pyFailed | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}
Write-Host '[ OK ] Python 侧门禁通过(errcode 一致 + pytest 全绿)。' -ForegroundColor Green
