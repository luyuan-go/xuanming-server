# Pandora 一键开发环境 —— **Python 实现**(基础设施 + 22 个 Python 服务)
#
# 与 dev_all.ps1(Go 栈)平行的另一条入口。**基础设施与库结构升级两步完全复用**
# dev_up.ps1 / dev_migrate.ps1,不另抄一套编排 —— 两个栈跑的是同一套 MySQL / Redis /
# Kafka / etcd / TiDB 和同一份 schema,分叉了才是 bug。
#
# 用法:
#   # 起全部(基础设施 + 22 个 Python 服务)
#   pwsh tools/scripts/dev_all_python.ps1
#
#   # 只重启业务服务(基础设施已在跑,跳过 docker 与迁移,快很多)
#   pwsh tools/scripts/dev_all_python.ps1 -SkipInfra
#
#   # 排除某个服务(留给断点调试)
#   pwsh tools/scripts/dev_all_python.ps1 -Exclude inventory
#
#   # 全停(Python 服务 + 基础设施)
#   pwsh tools/scripts/dev_all_python.ps1 -Down
#
#   # 只停 Python 服务,基础设施留着
#   pwsh tools/scripts/dev_all_python.ps1 -Down -SkipInfra
#
# ⚠️ 本机 DS(mode=local)走 *-dev.yaml 里写死的 packaged 形态,即
#    F:\work\Packages\Server_Win64_Development\WindowsServer\PandoraServer.exe。
#    没出过包的话 hub_allocator / ds_allocator 会启动失败(executable_path not found),
#    先跑客户端仓库的 Tool\Build\Package_Server_Win64_Dev.bat。
#    要临时换 DS 形态(如 editor),照常用 PANDORA_DS_LAUNCHER / PANDORA_DS_EXE /
#    PANDORA_DS_UPROJECT / PANDORA_DS_DIR 注入,本脚本原样透传给子进程。

[CmdletBinding()]
param(
    # 逗号/数组形式排除若干服务(名字同 python/tools/run_stack.py 的服务名)
    [string[]]$Exclude = @(),

    # 传给 dev_up.ps1:先拉最新镜像
    [switch]$Pull,

    # 全停:Python 服务 +(除非 -SkipInfra)基础设施
    [switch]$Down,

    # 跳过 docker 基础设施与库结构升级,只处理业务服务
    [switch]$SkipInfra,

    # 只把 Envoy 客户端面 8443 绑回环(仅本机客户端可连)。
    # 默认**对局域网开放**,与 start.ps1 的 local 模式同口径:打包客户端连的是本机内网 IP
    # (登录服务器列表里那一项),绑成 127.0.0.1 的话客户端直接 Connection refused ——
    # 2026-08-23 就是这么卡住的:dev_up.ps1 直起时 dev.env 缺省 127.0.0.1,
    # 客户端连 192.168.2.28:8443 被拒,Envoy 访问日志上一条 Login 都看不到。
    # ⚠️ 未鉴权的 DS 面 8444 与 admin 9901 恒绑回环,不受本开关影响。
    [switch]$LocalOnly,

    # 不要动本机 DS 进程(PandoraServer.exe)。
    # 默认**会关**:allocator 本来负责"子进程随自己退出而 Kill",但一键停止是硬杀
    # (Stop-Process -Force),它来不及回收,DS 就成了孤儿继续占着 UDP 7777 / 7800+;
    # 下次启动 allocator 再 exec 一个新的就撞端口。实测停+起一轮后仍有 2 个残留。
    # 手工留着某个 DS 调试时才加本开关。
    [switch]$KeepDs,

    # 不要弹「DS 日志窗口」。默认**会弹**一个独立窗口实时跟随本机 DS(大厅 / 战斗)日志,
    # 因为 DS 自己是没有控制台的 —— 光看这张就绪表分不清「大厅服起没起来」「匹配之后
    # 战斗服有没有被拉起来」。
    # ⚠️ 那个窗口是**另一个进程在读日志文件**,不是给 DS 开控制台:UE DS 加 `-log` 会开真
    #    控制台,Windows 快速编辑模式下在里面点一下就阻塞 WriteConsole、冻住整个游戏线程,
    #    一次误点就让大厅永久不可进(成因见 python/tools/tail_ds_logs.py)。这条不许改回去。
    [switch]$NoDsWindow,

    # ---- 以下三个由 start.ps1 的 -Python 分支透传,语义与 dev_all.ps1 同名参数对齐 ----
    # 免 Docker:基础设施用本机原生进程(local_infra.ps1),不起 TiDB;社交四服改连本机
    # MySQL 的 pandora_social(run_stack --social-mysql),MySQL 端口取 local_infra 的动态
    # 端口(--mysql-port)。与 dev_all.ps1 -NoDocker / run_services.ps1 -SocialOnMysql 同口径。
    [switch]$NoDocker,

    # 本轮导表是否真的变了。Python 栈每次全量停起、必然重载配表,这里只为调用方签名
    # 兼容收下,不据此做选择性重启(那是 go 栈 run_services 的优化)。
    [switch]$ConfigTableChanged,

    # go 栈策划 fast 入口的「导表下沉到并行准备批次」。Python 栈没有 staging build,
    # 导表一律由 start.ps1 前置完成;这个开关传进来即坐标系错了,fail-fast。
    [switch]$GenerateTables
)

$ErrorActionPreference = 'Stop'
$ScriptDir = $PSScriptRoot
$ProjectRoot = (Resolve-Path "$ScriptDir/../..").Path
$PythonRoot = Join-Path $ProjectRoot 'python'
$VenvPy = Join-Path $PythonRoot '.venv/Scripts/python.exe'

# Get-PandoraLocalMysqlPort(免 Docker 动态 MySQL 端口)在这里。
. (Join-Path $ScriptDir 'lib/local_infra_state.ps1')

if ($GenerateTables) {
    Write-Host "[ERR] -GenerateTables 只属于 go 栈策划 fast 入口;Python 栈的导表由 start.ps1 前置完成。" -ForegroundColor Red
    exit 2
}

# 判别口径与 run_services.ps1(Go 栈)**逐条一致**,不另立一套。
# 背景:allocator 的 Close() 里确实有 Kill(),但那挂在 main 的 defer 上;一键停止用的是
# Stop-Process -Force(= TerminateProcess),defer 一行都不会跑;Windows 又没有进程组连坐,
# 父进程一被强杀,DS 立刻变成无主进程继续占着 UDP 7777 → 下一轮新 DS 起不来。
$LocalDsProcNames = @('UnrealEditor', 'PandoraServer')

function Test-IsLocalDsProcess($cim) {
    <#
      三重收敛防误杀(**绝不能碰策划自己开着的 UnrealEditor**):
        1. 只认上面两个进程名;
        2. 命令行必须**同时**含 `-server` 与关卡 URL `?game=/Script/Pandora.`
           —— 手工开的编辑器两者都没有,这一条是能安全把 UnrealEditor 纳入的前提;
        3. 孤儿模式再加一条:父进程必须已不存在。
    #>
    if (-not $cim) { return $false }
    $name = [System.IO.Path]::GetFileNameWithoutExtension($cim.Name)
    if ($LocalDsProcNames -notcontains $name) { return $false }
    $cmdline = $cim.CommandLine
    if (-not $cmdline) { return $false }
    return ($cmdline -match '(?i)(^|\s)-server(\s|$)') -and ($cmdline -match '(?i)\?game=/Script/Pandora\.')
}

function Stop-PandoraLocalDs {
    <#
      关掉本机 Windows DS(大厅 + 战斗)。
      -OrphansOnly:只清父进程已退出的无主 DS(启动前用),在跑的正常 DS 不动。
    #>
    param([switch]$OrphansOnly)
    if ($KeepDs) {
        Write-Host "  跳过本机 DS(-KeepDs)"
        return
    }
    $all = @()
    try { $all = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue) } catch { return }
    if (-not $all) { return }
    $livePids = @{}
    foreach ($p in $all) { $livePids[[int]$p.ProcessId] = $true }

    $killed = 0
    foreach ($p in $all) {
        if (-not (Test-IsLocalDsProcess $p)) { continue }
        if ($OrphansOnly -and $livePids.ContainsKey([int]$p.ParentProcessId)) { continue }
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
        $killed++
    }
    $scope = if ($OrphansOnly) { '无主 DS' } else { '本机 DS' }
    Write-Host "  $scope :关掉 $killed 个"
}

function Clear-PandoraLocalHubShardMirror {
    <#
      清掉 mode=local 的大厅分片镜像(`pandora:hub:shard:{pandora-hub-local-*}`)。

      ★ 为什么必须清:hub_allocator 的 ensure_shards 判定是「该 region+track 已有任意
        一条分片记录就提前返回」,**不看它是不是 draining**。一键停止硬杀 DS 后,上一轮
        那条镜像还带着 30 分钟 TTL 赖在 Redis 里,于是下次启动:
          · ensure_shards 认为大厅已存在 → 永远走不到 fleet.list_shards()
          · 而 list_shards 才是懒拉起 DS 的地方 → DS 再也不会被拉起来
          · AssignHub 只找得到那条心跳过期的 draining 记录 → ERR_HUB_NO_AVAILABLE
        表现就是「双击启动了,但 hub ds 没拉起来」,而且日志里一个字都没有。
        2026-08-23 实测:删掉这条键后 AssignHub 立刻 OK 并拉起 DS。

      ★ 这不是绕过权威:被清的镜像所描述的那个 DS 进程,正是上一行 Stop-PandoraLocalDs
        刚刚杀掉的;记录的主人已经不存在了。只清 `pandora-hub-local-*`(本机 local 形态),
        agones / mock 的分片一律不碰。
    #>
    $cleared = $null
    if ((Get-Command docker -ErrorAction SilentlyContinue) -and
        (docker ps --format '{{.Names}}' 2>$null | Select-String -SimpleMatch 'pandora-redis' -Quiet)) {
        $keys = @(docker exec pandora-redis redis-cli --scan --pattern 'pandora:hub:shard:*' 2>$null |
            Where-Object { $_ -like '*pandora-hub-local-*' })
        foreach ($k in $keys) { $null = docker exec pandora-redis redis-cli del $k 2>$null }
        $cleared = $keys.Count
    } elseif ($NoDocker -and (Test-Path -LiteralPath $VenvPy)) {
        # 免 Docker:Redis 是本机原生进程(127.0.0.1:6380),用 venv 里的 redis 客户端清。
        # 不清的后果与 docker 栈同一条:ensure_shards 撞上一键停止留下的 draining 镜像,
        # 大厅 DS 永远不被拉起、AssignHub 恒 ERR_HUB_NO_AVAILABLE(见上方 docstring)。
        $pyCode = @'
import redis
r = redis.Redis(host="127.0.0.1", port=6380, socket_connect_timeout=2)
try:
    keys = [k for k in r.scan_iter("pandora:hub:shard:*") if b"pandora-hub-local-" in k]
    for k in keys:
        r.delete(k)
    print(len(keys))
except Exception:
    print(0)
'@
        $cleared = (& $VenvPy -c $pyCode 2>$null | Select-Object -Last 1)
    }
    if ($null -eq $cleared) { return }   # Redis 没起(比如已经 dev_down 过),没什么可清
    Write-Host "  本机大厅分片镜像:清掉 $cleared 条"
}

function Invoke-RunStack {
    param([string[]]$ExtraArgs)
    if (-not (Test-Path -LiteralPath $VenvPy)) {
        Write-Host "[ERR] 找不到解释器 $VenvPy" -ForegroundColor Red
        Write-Host "      裸 python 在本机会弹 Microsoft Store;先按 python/README 建好 venv。" -ForegroundColor Red
        exit 2
    }
    $argv = @('tools/run_stack.py') + $ExtraArgs
    Push-Location $PythonRoot
    try {
        $env:PYTHONUTF8 = '1'   # 中文日志撞 cp1252 会 UnicodeEncodeError
        # ★ 必须 Out-Host:PowerShell 函数会把**写到输出流的一切**当成返回值。
        #   写成 `& $VenvPy @argv` 再 `$code = Invoke-RunStack ...`,run_stack.py 打的
        #   就绪表会被一起吸进 $code —— 表在终端上看不见,$code 还变成数组,
        #   `-ne 0` 恒真 → 明明 22/22 起来了却报失败,exit 一个数组又让 cmd 收到 0。
        #   Out-Host 直送宿主、不进输出流,函数就只返回下面这个退出码。
        & $VenvPy @argv | Out-Host
        return $LASTEXITCODE
    } finally { Pop-Location }
}

# ── 停 ────────────────────────────────────────────────────────────────────
if ($Down) {
    Write-Host "===== Pandora dev(Python)全停 =====" -ForegroundColor Cyan
    $stopArgs = @('--stop')
    if ($Exclude.Count -gt 0) { $stopArgs += @('--exclude', ($Exclude -join ',')) }
    # scoped 停靠「模块名+配置文件名」匹配;免 Docker 下社交四服跑的是 -dev.yaml,
    # 不带这个开关会拿 -dev-tidb.yaml 的文件名去匹配 —— 停了但没停到。
    if ($NoDocker) { $stopArgs += '--social-mysql' }
    $null = Invoke-RunStack $stopArgs   # 输出已由 Out-Host 直送,这里只丢掉退出码
    Stop-PandoraLocalDs
    Clear-PandoraLocalHubShardMirror
    if (-not $SkipInfra) {
        if ($NoDocker) { & "$ScriptDir/local_infra.ps1" -Action down } else { & "$ScriptDir/dev_down.ps1" }
        exit $LASTEXITCODE
    }
    exit 0
}

# ── 起 ────────────────────────────────────────────────────────────────────
$totalSteps = if ($SkipInfra) { 1 } else { 3 }
$step = 0

if (-not $SkipInfra) {
    $step++
    Write-Host ""
    if ($NoDocker) {
        Write-Host "===== [$step/$totalSteps] 基础设施(本机原生进程,免 Docker)=====" -ForegroundColor Cyan
        # 策划远端数据库(central-managed)的登记/迁移编排是 go 栈 dev_all.ps1 专有的,
        # 这里静默走本机库会让策划以为自己在操作中心 workspace —— fail-fast。
        if ($env:PANDORA_PLANNER_REQUIRE_CENTRAL_MYSQL -eq '1') {
            Write-Host "[ERR] Python 免 Docker 入口暂不支持策划远端数据库(central-managed)。" -ForegroundColor Red
            Write-Host "      本机存在 installers/planner-db/central-mysql.json;请用 go 入口,或先移除远端登记。" -ForegroundColor Red
            exit 2
        }
        & "$ScriptDir/local_infra.ps1" -Action up
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERR] 基础设施启动失败,中止" -ForegroundColor Red
            exit 1
        }
    } else {
        Write-Host "===== [$step/$totalSteps] 基础设施(docker)=====" -ForegroundColor Cyan
        # compose 用 ${PANDORA_EDGE_BIND_HOST:-127.0.0.1} 绑客户端面;这里显式导出,
        # dev_up.ps1 起 Envoy 时就会带上(dev.env 里的值会被进程环境覆盖)。
        $env:PANDORA_EDGE_BIND_HOST = if ($LocalOnly) { '127.0.0.1' } else { '0.0.0.0' }
        Write-Host "  客户端面 8443 绑定:$($env:PANDORA_EDGE_BIND_HOST)$(if (-not $LocalOnly) { '(局域网可连;加 -LocalOnly 只绑本机)' })"
        if ($Pull) { & "$ScriptDir/dev_up.ps1" -Pull } else { & "$ScriptDir/dev_up.ps1" }
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERR] 基础设施启动失败,中止" -ForegroundColor Red
            exit 1
        }
        # 社交四服(friend/chat/guild/mail)本地默认连 TiDB(etc/<svc>-dev-tidb.yaml,与
        # run_services.ps1 同口径),但 dev_up 的 compose 不含 TiDB(独立网络)。go 栈由
        # dev_all.ps1 拉起;Python 栈此前漏了这一步 —— 社交四服起来即
        # panic: ping mysql: dial tcp 127.0.0.1:4000 拒绝。tidb_up.ps1 幂等,已在跑则快速返回。
        & "$ScriptDir/tidb_up.ps1"
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERR] TiDB 启动失败,中止(社交四服连不上库)" -ForegroundColor Red
            exit 1
        }
    }

    $step++
    Write-Host ""
    Write-Host "===== [$step/$totalSteps] 数据库结构升级 =====" -ForegroundColor Cyan
    # 与 Go 栈同一个迁移器:以库内 schema_migrations 为准,只补缺的版本,天然幂等。
    if ($NoDocker) {
        # 免 Docker 本机路径用 local_infra 备料的 mysql.exe 作客户端,端口是动态的。
        # (与 dev_all.ps1 免 Docker 普通路径逐句同构。)
        $mysqlPort = Get-PandoraLocalMysqlPort $ProjectRoot -Required
        $mysqlClient = Get-ChildItem -Path (Join-Path $ScriptDir '../../run/localinfra/dist/mysql') `
            -Recurse -File -Filter 'mysql.exe' -ErrorAction SilentlyContinue | Select-Object -First 1
        if (-not $mysqlClient) {
            Write-Host "[ERR] 找不到本机 mysql.exe(备料应由 local_infra.ps1 完成),中止" -ForegroundColor Red
            exit 1
        }
        & "$ScriptDir/dev_migrate.ps1" -MysqlClient $mysqlClient.FullName -MysqlPort $mysqlPort -RequireMysql
    } else {
        & "$ScriptDir/dev_migrate.ps1" -RequireMysql
    }
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERR] 数据库结构升级失败,中止(继续启动只会让服务连着旧结构崩溃)" -ForegroundColor Red
        exit 1
    }
}

$step++
Write-Host ""
Write-Host "===== [$step/$totalSteps] 业务服务(Python)=====" -ForegroundColor Cyan
# ★ 顺序必须是「先停服务 → 再杀 DS → 再清镜像 → 最后起」,三步都不能调换:
#
#   最初写成「先 -OrphansOnly 清孤儿,再让 run_stack 内部停服务」,结果是:清孤儿那一刻
#   旧 DS 的父进程(旧 allocator)还活着,它不算孤儿、逃过一劫;等 allocator 随后被停掉,
#   它才变成孤儿,继续占着 UDP 7777 并沿用**旧 pod 身份**。新 allocator 起来后重新铸了
#   一个 pod,客户端拿着新 pod 的票连到 7777 却撞上旧 DS,于是:
#       hub_admission_failed: hub admission assignment is no longer current
#       heartbeat_unknown_hub_waiting_topology: pod=<旧 pod>
#   表现是「能进主城、几秒后被踢回登录」—— 2026-08-23 实测就是这么翻车的。
#
#   先把服务停掉,DS 就全部变成无主进程,这时再全杀(不是 -OrphansOnly)才干净。
$stopFirst = @('--stop')
if ($Exclude.Count -gt 0) { $stopFirst += @('--exclude', ($Exclude -join ',')) }
if ($NoDocker) { $stopFirst += '--social-mysql' }
$null = Invoke-RunStack $stopFirst
Stop-PandoraLocalDs
Clear-PandoraLocalHubShardMirror
$upArgs = @()
if ($Exclude.Count -gt 0) { $upArgs += @('--exclude', ($Exclude -join ',')) }
if (-not $NoDsWindow) { $upArgs += '--ds-window' }
if ($NoDocker) {
    # 动态 MySQL 端口 + 社交四服走本机 MySQL(TiKV 无 Windows 原生部署,起不了 TiDB)。
    $mysqlPort = Get-PandoraLocalMysqlPort $ProjectRoot -Required
    $upArgs += @('--social-mysql', '--mysql-port', "$mysqlPort")
}
$code = Invoke-RunStack $upArgs
if ($code -ne 0) {
    Write-Host ""
    Write-Host "[ERR] 有服务没起来 —— 上表 RESULT 列写了是哪一个、看哪份日志。" -ForegroundColor Red
    Write-Host "      判据是「端口在听 且 日志出现 service_ready」,只要有一条没过就算失败。" -ForegroundColor Red
    exit $code
}

Write-Host ""
Write-Host "===== 就绪 =====" -ForegroundColor Green
Write-Host "全链验收:cd python; `$env:PANDORA_E2E_NO_FAKE_DS=1; .venv\Scripts\python.exe tools\e2e_roundtrip.py"
exit 0
