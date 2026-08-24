#requires -Version 7.0
<#
.SYNOPSIS
    策划机免 Docker 本机基础设施(MySQL / Redis / Kafka / Envoy)。

.DESCRIPTION
    背景:策划机不装 Docker Desktop(装不动、要 WSL2、开机慢、IT 不给管理员),
    但本机跑一整套后端需要 MySQL / Redis / Kafka / Envoy 四件套。本脚本用**免安装**
    的原生 Windows 二进制把这四件套跑成宿主进程。Redis / Kafka / Envoy 保持 dev 固定端口；
    MySQL 刻意不用 Docker 路线的 3307，而是在 13307..13398 中选择可真实 bind 的独立端口，
    再由 run_services.ps1 派生运行态配置，避免复用或改写机器上已有的 Docker MySQL。

    与 docker 模式的对应关系(端口 / 账号必须一致,否则服务配置就得分叉):
      MySQL  127.0.0.1:自动选择(13307..13398) root/pandora_dev_root pandora/pandora_dev_pwd
      Redis  127.0.0.1:6380   无密码,appendonly yes,maxmemory 1gb noeviction
      Kafka  127.0.0.1:9093   KRaft 单节点(不需要 ZooKeeper),自动建 topic,4 分区
      Envoy  0.0.0.0:8443(客户端面) / 127.0.0.1:8444(DS 面) / 127.0.0.1:9901(admin)

    **不含** TiDB / Prometheus / Grafana / Loki / etcd:
      - TiDB   :TiKV 没有可用的 Windows 原生部署;social 四服(friend/chat/guild/mail)
                 本模式改走 etc/*-dev.yaml 直连本机 MySQL 的 pandora_social 库
                 (run_services.ps1 -SocialOnMysql)。
      - 观测栈 :策划不看 Grafana,省 1GB 内存和一堆磁盘。
      - etcd   :dev 配置里 etcd_endpoints 全是注释状态(snowflake 走 static、
                 authority_mode 非 redis),本机单副本用不上。

    ⚠️ Envoy 用的是 1.28.0(2023 年最后一版官方 Windows 构建;上游 2023-08 关闭了
       Windows CI,之后没有官方 Windows 二进制)。**只允许用于本机 127.0.0.1 开发边缘**,
       内网 / k8s / 线上一律继续用 v1.38(deploy/k8s/infra/edge-envoy.yaml)。
       生产 envoy.yaml 里只有 1 个字段是 1.28 不认识的(见 $EnvoyDropFields),
       派生配置时精确剔除并跑 --mode validate 卡关:出现白名单以外的未知字段 → 直接失败,
       绝不"自动跳过不认识的字段"(那会让策划机静默跑在比线上更弱的鉴权上)。

.PARAMETER Action
    up        : 备料(缺什么下什么)+ 启动 + 健康检查(默认)
    down      : 停止所有本机基础设施进程(保留数据)
    status    : 打印各组件端口 / 进程状态
    provision : 只备料不启动(适合提前在共享盘上做好离线包);比 up 多备一份 PowerShell 7
                免安装包,供「连 pwsh 都没有」的策划机用 bootstrap_pwsh.cmd 自举
    reset     : 停止并删除 data 目录(MySQL / Kafka / Redis 数据全清,下次 up 会重新初始化)

.PARAMETER Force
    provision / up 时强制重新下载并解包(默认已就位则跳过)。

.NOTES
    离线 / 内网分发:SVN 策划包会把固定版本压缩包放在仓库 installers/localinfra；脚本自动
    逐文件命中。Git 只有目录说明、没有包时会正常回退公网。也可设置 PANDORA_LOCALINFRA_MIRROR
    显式覆盖为本地目录或 UNC 共享。所有来源都必须通过固定 SHA256 才会执行。
#>

[CmdletBinding()]
param(
    [ValidateSet('up', 'down', 'status', 'provision', 'reset')]
    [string]$Action = 'up',

    [switch]$Force,

    # 仅供同一 runspace 的策划启动协调器使用。组件完成 exact listener + 协议探活后调用，
    # 让 MySQL 就绪即可触发 migration，而不必等待 Redis/Kafka/Envoy 全部完成。
    [scriptblock]$OnPlannerComponentReady
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'   # 不关的话 Invoke-WebRequest 下大文件慢十倍

$ProjectRoot = (Resolve-Path "$PSScriptRoot/../..").Path
$Root = Join-Path $ProjectRoot 'run/localinfra'
$DistDir = Join-Path $Root 'dist'      # 解包后的二进制
$DataDir = Join-Path $Root 'data'      # 各组件数据目录
$LogDir = Join-Path $Root 'logs'
$PidDir = Join-Path $Root 'pids'
$CfgDir = Join-Path $Root 'cfg'
$CacheDir = Join-Path $Root 'cache'     # 下载的压缩包
. (Join-Path $PSScriptRoot 'lib/local_infra_state.ps1')
. (Join-Path $PSScriptRoot 'lib/planner_mysql_startup.ps1')
. (Join-Path $PSScriptRoot 'lib/planner_infra_fast_start.ps1')
. (Join-Path $PSScriptRoot 'lib/planner_start_timing.ps1')
$LocalInfraLifecyclePlan = Get-PandoraLocalInfraLifecyclePlan -ProjectRoot $ProjectRoot
$CentralMysqlManaged = $LocalInfraLifecyclePlan.Mode -ceq 'central-managed'

# ===== 端口 / 账号 =====
# 3307 属于 Docker dev 路线，免 Docker 绝不把那个 listener 当成自己的。13399 已被
# pandora-mysql-itest 使用，所以候选池到 13398 为止；实际端口只在 MySQL 完成归属与账号
# 探活后写入 run/localinfra/cfg/ports.json。
$MysqlPort = 0
$MysqlPortMin = 13307
$MysqlPortMax = 13398
$RedisPort = 6380
$KafkaPort = 9093
$KafkaCtrlPort = 9094          # KRaft controller,仅本机内部
# Envoy admin 9901 由 deploy/envoy/envoy.yaml 自己声明,这里只负责把它的监听地址收敛到 127.0.0.1
$MysqlRootPwd = 'pandora_dev_root'
$MysqlUser = 'pandora'
$MysqlUserPwd = 'pandora_dev_pwd'

# ===== 组件清单 =====
#
# 每个组件都必须钉死【确定版本 + Sha256】,没有例外。原因不是"防下载出错"(那有 Content-Length
# 和解包失败兜着),而是**供应链**:这些包会在 100 台策划机上解开直接执行,而取包路径有三条 ——
# 公网 URL、任意可写的共享盘(PANDORA_LOCALINFRA_MIRROR)、本机 cache 目录。后两条完全不受
# HTTPS 保护,谁能写那个目录谁就能换掉一个必然被执行的二进制。所以校验放在 Get-Archive 里,
# 对三条路径一视同仁,校验不过绝不解包。
#
# Sha256 的来源必须是**上游权威值**,不能是"我下下来自己算的"(那样只是把第一次下到的东西
# 当成标准,中间人在第一次就成功的话照样写进常量)。2026-08-15 逐个核对如下:
#   mysql : 官方 https://cdn.mysql.com/archives/mysql-8.4/mysql-8.4.6-winx64.zip.md5
#           = 73022866eb641b8ea8b22b81f8be1694,与本机文件 MD5 一致(MySQL 归档只发 MD5 和
#           GPG .asc;这里用它作上游锚点,实际校验仍用下面的 SHA256)
#   redis : 官方 release notes 公布 SHA256 1A0741A8...980460,与下面一致
#   kafka : 官方 https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz.sha512
#           与本机文件 SHA512 逐位一致
#   jre   : Adoptium assets API 的 package.checksum = b8aa18fe...73ba,与下面一致
# 换版本时必须重新走一遍上述核对,不许直接把新算出来的哈希填进来。
$Components = [ordered]@{
    mysql = @{
        Version = '8.4.6'
        File    = 'mysql-8.4.6-winx64.zip'
        Urls    = @(
            'https://cdn.mysql.com/archives/mysql-8.4/mysql-8.4.6-winx64.zip'
            'https://dev.mysql.com/get/Downloads/MySQL-8.4/mysql-8.4.6-winx64.zip'
        )
        Sha256  = 'b6c152f9f3aaa7294eb47db698e47974d37b261bf3cab4f90dc1243bb5ecd204'
        Probe   = 'mysqld.exe'
        Tools   = @{
            'mysqld.exe'    = 'mysql-8.4.6-winx64\bin\mysqld.exe'
            'mysql.exe'     = 'mysql-8.4.6-winx64\bin\mysql.exe'
            'mysqladmin.exe'= 'mysql-8.4.6-winx64\bin\mysqladmin.exe'
        }
    }
    redis = @{
        # redis-windows/redis-windows:上游 Redis 源码的 msys2 原生 Windows 构建,
        # 免安装、无服务、无 UAC。选 8.8.x 对齐 compose 的 redis:8.8.0-alpine —— 版本要紧:
        # 项目用到 PEXPIRE LT / ZADD GT|XX 这类 6.2+ 语义,老的 Redis 3.x Windows 移植版跑不了。
        Version = '8.8.1'
        File    = 'Redis-8.8.1-Windows-x64-msys2.zip'
        Urls    = @(
            'https://github.com/redis-windows/redis-windows/releases/download/8.8.1/Redis-8.8.1-Windows-x64-msys2.zip'
        )
        Sha256  = '1a0741a8f997a50ad7a32370e9ddf719ed3d5d87701324c57b7b34518b980460'
        Probe   = 'redis-server.exe'
        Tools   = @{
            'redis-server.exe' = 'Redis-8.8.1-Windows-x64-msys2\redis-server.exe'
            'redis-cli.exe'    = 'Redis-8.8.1-Windows-x64-msys2\redis-cli.exe'
        }
    }
    kafka = @{
        # 3.9.x 对齐 compose 的 confluentinc/cp-kafka:7.9(= Kafka 3.9)。
        # 用 KRaft 模式,不起 ZooKeeper(compose 里那个 zookeeper 容器在本模式下不需要)。
        Version = '3.9.1'
        File    = 'kafka_2.13-3.9.1.tgz'
        Urls    = @(
            # 华为云 apache 镜像放国内,实测可用且带 Range;上游 archive.apache.org 作兜底。
            # 镜像站是第三方,正因如此下面的 Sha256 才是必须的 —— 它取自 apache 官方 .sha512
            # 对应的同一个文件,镜像站给的包对不上就会被拒。
            'https://mirrors.huaweicloud.com/apache/kafka/3.9.1/kafka_2.13-3.9.1.tgz'
            'https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz'
        )
        Sha256  = 'dd4399816e678946cab76e3bd1686103555e69bc8f2ab8686cda71aa15bc31a3'
        Probe   = 'kafka-server-start.bat'
        Tools   = @{
            'kafka-server-start.bat' = 'kafka_2.13-3.9.1\bin\windows\kafka-server-start.bat'
        }
    }
    jre   = @{
        # Kafka 要 JVM。策划机不一定装 Java,也不能假设装了就是 17+,所以自带一份免安装 JRE21。
        # 地址必须钉到具体 release(jdk-21.0.12+8),**不能**用 /v3/binary/latest/... ——
        # latest 是会漂的:Adoptium 一发新补丁版,同一个 URL 就换了内容,校验和当场失效,
        # 而且各台机器按备料时间不同装到不同 JVM,出问题无法复现。
        Version = 'temurin-21.0.12+8'
        File    = 'OpenJDK21U-jre_x64_windows_hotspot_21.0.12_8.zip'
        Urls    = @(
            'https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.12%2B8/OpenJDK21U-jre_x64_windows_hotspot_21.0.12_8.zip'
        )
        Sha256  = 'b8aa18fef5edb69bee8618f99677d66d0873d22cb40d974c15ac9ffcdecf73ba'
        Probe   = 'java.exe'
        Tools   = @{
            'java.exe' = 'jdk-21.0.12+8-jre\bin\java.exe'
        }
    }
}

# mkcert 单独处理:上游发布的是**裸 exe**(不是压缩包),所以不走 $Components 的解包流程。
# 为什么要自动备料而不是让策划 winget install:Envoy 的 TLS 叶子证书必须本机签(SAN 要含本机
# 局域网 IP),这是免 Docker 模式唯一还剩的外部工具依赖。它就是个 4.7MB 单文件、不写注册表、
# 不装服务、不要 UAC(只要不跑 `mkcert -install`)—— 完全没有理由让 100 台策划机各装一遍。
# 备料到 run/localinfra/dist/mkcert 后由 Register-LocalToolPath 挂进**本进程** PATH,
# 不改机器的用户/系统 PATH(策划机环境保持干净,卸载 = 删目录)。
$MkcertVersion = 'v1.4.4'
$MkcertFile = 'mkcert-v1.4.4-windows-amd64.exe'
$MkcertUrls = @(
    'https://github.com/FiloSottile/mkcert/releases/download/v1.4.4/mkcert-v1.4.4-windows-amd64.exe'
)
# 钉死 sha256:裸 exe 没有 registry digest 那种内容寻址,又允许走 PANDORA_LOCALINFRA_MIRROR
# 共享盘,共享盘上放了什么本脚本无从判断 —— 不校验就等于让任何能写共享盘的人换掉一个
# 会被执行、且专门用来签 TLS 证书的二进制。取值:2026-08-15 实测下载 4,896,256 字节,
# 运行 `-version` 输出 v1.4.4。换版本时必须同步更新这三个常量。
$MkcertSha256 = 'd2660b50a9ed59eada480750561c96abc2ed4c9a38c6a24d93e30e0977631398'

# Envoy 单独处理:没有官方 Windows 压缩包,只能从 2023 年最后一版官方 Windows 镜像里取 exe。
# 走纯 HTTPS 的 registry API(不需要本机有 docker),digest 固定 = 内容寻址,下载后校验 sha256。
$EnvoyImageRepo = 'envoyproxy/envoy-windows'
$EnvoyImageTag = 'v1.28.0'
$EnvoyLayerDigest = 'sha256:bbfb444bc8bd3ee4d1e11cb10b82fbc9f101a57fb223b0315ce14abb4c1c5b7d'
$EnvoyExeInLayer = 'Files/Program Files/envoy/envoy.exe'

# Envoy 1.28 不认识、但对本机开发无功能影响的字段白名单。
# 每一条都必须写清「线上什么行为 / 本机退化成什么」,不写清楚的不许加。
$EnvoyDropFields = @{
    # local_ratelimit 的「超限时回 gRPC RESOURCE_EXHAUSTED 而不是 HTTP 429」开关(1.30+ 才有)。
    # 影响面:只有 Login 接口的限流响应码(阈值 50rps / burst 100),本机单人压根触发不到。
    'rate_limited_as_resource_exhausted' = '1.30+ 字段;本机退化为超限回 HTTP 429(线上仍是 RESOURCE_EXHAUSTED)'
}

# ===== 输出 =====
function Write-Step([string]$m) { Write-Host "[infra] $m" -ForegroundColor Cyan }
function Write-Ok([string]$m) { Write-Host "  [ OK ] $m" -ForegroundColor Green }
function Write-Warn2([string]$m) { Write-Host "  [WARN] $m" -ForegroundColor Yellow }
function Write-Err([string]$m) { Write-Host "  [ERR ] $m" -ForegroundColor Red }

function Fail([string]$m) {
    Write-Err $m
    exit 1
}

function Resolve-LocalInfraPackageMirror {
    <#
      显式共享镜像优先；未设置时自动指向仓库内只读安装包目录。
      这里只选目录，不以“目录存在”冒充“包存在”：Get-Archive 会按当前固定文件名逐个判断，
      所以 Git 的空目录、SVN 的部分旧目录都能对缺失包正常回退公网。
    #>
    param(
        [Parameter(Mandatory)][string]$RepositoryRoot,
        [AllowEmptyString()][string]$ExplicitMirror
    )
    if (-not [string]::IsNullOrWhiteSpace($ExplicitMirror)) {
        return [pscustomobject]@{ Path = $ExplicitMirror.Trim(); Kind = '显式离线镜像' }
    }
    return [pscustomobject]@{
        Path = (Join-Path $RepositoryRoot 'installers/localinfra')
        Kind = '仓库安装包'
    }
}

$PackageMirror = Resolve-LocalInfraPackageMirror `
    -RepositoryRoot $ProjectRoot -ExplicitMirror $env:PANDORA_LOCALINFRA_MIRROR
$PlannerFastStart = ($env:PANDORA_PLANNER_FAST_START -eq '1')

# ===== 通用工具 =====

function Test-TcpEndpoint([string]$ComputerName, [int]$Port) {
    $c = [System.Net.Sockets.TcpClient]::new()
    try {
        $iar = $c.BeginConnect($ComputerName, $Port, $null, $null)
        if (-not $iar.AsyncWaitHandle.WaitOne(300)) { return $false }
        $c.EndConnect($iar)
        return $true
    } catch { return $false } finally { $c.Dispose() }
}

function Test-PortOpen([int]$Port) { return (Test-TcpEndpoint -ComputerName '127.0.0.1' -Port $Port) }

function Test-PortBindable([int]$Port) {
    <#
      用真正的 bind 判断候选端口，而不是只看 LISTEN 表。Windows 的 Hyper-V / WSL2 / Docker
      会通过 winnat 保留一段端口；这种端口没人监听，但 mysqld 仍会收到 WSAEACCES。
    #>
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $Port)
    try {
        $listener.Server.ExclusiveAddressUse = $true
        $listener.Start()
        return $true
    } catch {
        return $false
    } finally {
        try { $listener.Stop() } catch {}
    }
}

function Get-PortOwningProcessIds([int]$Port) {
    $ids = @()
    try {
        $ids = @(Get-PandoraTcpListenerProcessIds -Port $Port)
    } catch { $ids = @() }
    return ,$ids
}

function Test-AfUnixUsable([string]$Dir) {
    <#
      指定目录下 AF_UNIX(Unix 域套接字)能不能 bind+connect 通。
      为什么需要:Envoy 的 libevent 在 Windows 上优先用 AF_UNIX 造 socketpair 来做信号唤醒,
      被拦住时它连事件循环都建不起来,直接 assert 崩 —— 而崩溃文本里只字不提 AF_UNIX。
      为什么带目录参数:2026-08-16 实测,AF_UNIX 能不能用是**按路径**变的,有两条独立成因:
        1. %LOCALAPPDATA% 整棵树下 connect 报 WSAEINVAL(该目录本身、以及它下面的 Temp 都中招),
           而 C:\Windows\Temp、F: 盘、甚至同一用户的 Documents 都完全正常 —— 所以不是
           "用户目录被拦",范围就是 AppData\Local。成因未证实(本机装着 360,其内核过滤驱动
           360AntiSteal/360FsFlt 是头号嫌疑,且 Docker 的 socket 恰好也放在这棵树下 ——
           但没做过"卸载后复测"的对照实验,这里只当作**现象**处理,不写死是谁干的)。
        2. socket 全路径超过约 108 字节一律失败 —— 这是 AF_UNIX sun_path 的固有长度上限,
           跟安全软件无关,深层目录里的工作区会撞上。
      两条都靠这个探针挡掉,所以「AF_UNIX 不可用」永远不是全局结论,必须逐目录问。
      返回 $null=可用;否则返回失败原因串。
    #>
    if (-not $Dir) { $Dir = $env:TEMP }
    if (-not (Test-Path -LiteralPath $Dir)) { New-Item -ItemType Directory -Force -Path $Dir | Out-Null }
    $sock = Join-Path $Dir ("pandora-afunix-probe-" + [guid]::NewGuid().ToString('N').Substring(0, 8) + ".sock")
    $l = $null; $c = $null; $a = $null
    try {
        $l = [Net.Sockets.Socket]::new([Net.Sockets.AddressFamily]::Unix, [Net.Sockets.SocketType]::Stream, [Net.Sockets.ProtocolType]::Unspecified)
        $l.Bind([Net.Sockets.UnixDomainSocketEndPoint]::new($sock))
        $l.Listen(1)
        $c = [Net.Sockets.Socket]::new([Net.Sockets.AddressFamily]::Unix, [Net.Sockets.SocketType]::Stream, [Net.Sockets.ProtocolType]::Unspecified)
        $c.Connect([Net.Sockets.UnixDomainSocketEndPoint]::new($sock))
        $a = $l.Accept()
        return $null
    } catch {
        $e = $_.Exception; while ($e.InnerException) { $e = $e.InnerException }
        return $e.Message
    } finally {
        foreach ($s in @($a, $c, $l)) { if ($s) { try { $s.Dispose() } catch {} } }
        try { Remove-Item -LiteralPath $sock -Force -ErrorAction SilentlyContinue } catch {}
    }
}

function Resolve-EnvoyTempDir {
    <#
      给 Envoy 挑一个 AF_UNIX 真的能用的临时目录,并把 TMP/TEMP 指过去。
      背景见 Test-AfUnixUsable:libevent 在 TMP 目录里造 socketpair,而这台机器上
      %LOCALAPPDATA% 树(默认 TEMP 就在里面)下这个调用是不通的。换个目录不是"绕过安全软件",
      只是换个放临时文件的地方 —— 权限模型、拦截规则一点没动,换完照样受同一套防护管。
      优先用项目自己的 run/localinfra/tmp:跟着工作区走、可随目录一起删,不往系统目录里拉屎;
      它要是太深撞了 108 字节上限,自动落到 C:\Windows\Temp。
      全都不通才返回 $null,由调用方去报「AF_UNIX 在本机哪儿都用不了」。
    #>
    foreach ($d in @((Join-Path $Root 'tmp'), 'C:\Windows\Temp', $env:TEMP)) {
        if (-not $d) { continue }
        if (-not (Test-AfUnixUsable $d)) { return $d }
    }
    return $null
}

function Get-PidFile([string]$Name) { Join-Path $PidDir "$Name.pid" }

function Get-ProcessCommandLine([int]$ProcessId) {
    try {
        return [string](Get-CimInstance Win32_Process -Filter "ProcessId=$ProcessId" -ErrorAction Stop).CommandLine
    } catch {
        return ''
    }
}

function Test-ComponentProcessOwned([string]$Name, $Proc) {
    <#
      PID 文件只是一串会被系统复用的数字，不是归属证明。MySQL 的 down 路径会先发 shutdown，
      再 taskkill；因此必须同时核对 exe 与本工作区 my.ini。任一信息读不到都 fail closed。
      其他组件暂时保持既有行为，本次先封住会写数据库且有外部 Docker 共存诉求的 MySQL。
    #>
    if (-not $Proc) { return $false }
    if ($Name -ne 'mysql') { return $true }

    $expectedExe = Find-Tool 'mysql' 'mysqld.exe'
    if (-not $expectedExe) { return $false }
    $actualExe = $null
    try { $actualExe = $Proc.Path } catch { $actualExe = $null }
    if (-not $actualExe) { return $false }
    try {
        if ([IO.Path]::GetFullPath($actualExe) -ne [IO.Path]::GetFullPath($expectedExe)) { return $false }
    } catch { return $false }

    $cmd = Get-ProcessCommandLine $Proc.Id
    if (-not $cmd) { return $false }
    $actualIni = Get-PandoraDefaultsFileArgument $cmd
    if (-not $actualIni) { return $false }
    try {
        return [IO.Path]::GetFullPath($actualIni) -eq [IO.Path]::GetFullPath((Get-MysqlIniPath))
    } catch { return $false }
}

function Get-OwnedMysqlListenerRecords([int[]]$Ports) {
    $wanted = [Collections.Generic.HashSet[int]]::new()
    foreach ($port in $Ports) { if ($port -gt 0) { [void]$wanted.Add($port) } }
    if ($wanted.Count -eq 0) { return @() }

    $records = @()
    $seen = @{}
    try {
        foreach ($conn in @(Get-PandoraTcpListenerRecords)) {
            if (-not $wanted.Contains([int]$conn.LocalPort) -or -not $conn.OwningProcess) { continue }
            $key = "$($conn.LocalPort):$($conn.OwningProcess)"
            if ($seen.ContainsKey($key)) { continue }
            $seen[$key] = $true
            $proc = Get-Process -Id $conn.OwningProcess -ErrorAction SilentlyContinue
            if ($proc -and (Test-ComponentProcessOwned 'mysql' $proc)) {
                $records += [pscustomobject]@{ Port = [int]$conn.LocalPort; Process = $proc }
            }
        }
    } catch { throw "无法确认本机 MySQL listener 归属:$($_.Exception.Message)" }
    return @($records)
}

function Get-OwnedMysqlListenerProcess([int]$Port) {
    $record = @(Get-OwnedMysqlListenerRecords @($Port)) | Select-Object -First 1
    if ($record) { return $record.Process }
    return $null
}

function Get-OnlyOwnedMysqlListener([int[]]$Ports) {
    $records = @(Get-OwnedMysqlListenerRecords $Ports)
    if ($records.Count -gt 1) {
        Fail "同一工作区检测到多个原生 MySQL listener:$((@($records | ForEach-Object { ":$($_.Port)/PID=$($_.Process.Id)" })) -join ', ')。拒绝再启动，请先人工核对。"
    }
    if ($records.Count -eq 1) { return $records[0] }
    return $null
}

function Get-OwnedMysqlProcesses {
    $owned = @()
    foreach ($proc in @(Get-Process -Name 'mysqld' -ErrorAction SilentlyContinue)) {
        if (Test-ComponentProcessOwned 'mysql' $proc) { $owned += $proc }
    }
    return @($owned)
}

function Get-OnlyOwnedMysqlProcess {
    $owned = @(Get-OwnedMysqlProcesses)
    if ($owned.Count -gt 1) {
        Fail "同一工作区检测到多个原生 mysqld 进程:$((@($owned | ForEach-Object { 'PID={0}' -f $_.Id })) -join ', ')。拒绝自动停机或删数据，请先人工核对。"
    }
    if ($owned.Count -eq 1) { return $owned[0] }
    return $null
}

function Get-MysqlListenerRecordsForProcess($Proc) {
    if (-not $Proc) { return @() }
    $ports = @()
    try {
        $ports = @(Get-PandoraTcpListenerRecords |
            Where-Object { [int]$_.OwningProcess -eq [int]$Proc.Id } |
            Select-Object -ExpandProperty LocalPort -Unique)
    } catch { throw "无法确认 mysqld PID $($Proc.Id) 的 listener:$($_.Exception.Message)" }
    return @($ports | ForEach-Object { [pscustomobject]@{ Port = [int]$_; Process = $Proc } })
}

function Resolve-LocalMysqlPort {
    <#
      端口选择优先级:显式环境变量 > 已验证状态 > 独立候选池。外部 listener、系统保留端口、
      安全软件拒绝 bind 都只会让该候选被跳过；绝不连接它确认“密码碰巧对不对”。
    #>
    $explicitPort = 0
    $hasExplicit = -not [string]::IsNullOrWhiteSpace($env:PANDORA_LOCALINFRA_MYSQL_PORT)
    if ($hasExplicit) {
        if (-not [int]::TryParse($env:PANDORA_LOCALINFRA_MYSQL_PORT, [ref]$explicitPort) -or
            $explicitPort -lt 1024 -or $explicitPort -gt 49151 -or $explicitPort -eq 3307) {
            Fail 'PANDORA_LOCALINFRA_MYSQL_PORT 必须是 1024..49151 且不能是 Docker dev 专用的 3307。'
        }
    }

    $storedPort = Get-PandoraLocalMysqlPort $ProjectRoot
    $searchPorts = @(
        3307
        if ($hasExplicit) { $explicitPort }
        if ($storedPort) { $storedPort }
        $MysqlPortMin..$MysqlPortMax
    ) | Select-Object -Unique
    $existing = Get-OnlyOwnedMysqlListener $searchPorts
    if ($existing) {
        if ($hasExplicit -and $explicitPort -ne $existing.Port) {
            Fail "本工作区 MySQL 已在 :$($existing.Port) 运行；拒绝按显式 :$explicitPort 再启动第二实例。"
        }
        if ($existing.Port -eq 3307) {
            Write-Warn2 '检测到本工作区旧版原生 MySQL 仍在 :3307，本轮继续复用；下次停机后会迁到独立端口。'
        }
        return [int]$existing.Port
    }

    # 状态文件和显式环境变量都可能丢，但同一 data dir 的 mysqld 还活着。端口池扫描找不到它时，
    # 必须先按 exe + exact my.ini 找进程，再从该 PID 的所有 listener 恢复真实端口；尚未 listen
    # 说明它正在初始化或异常，宁可失败也不能用同一 data dir 启第二份。
    $ownedProcess = Get-OnlyOwnedMysqlProcess
    if ($ownedProcess) {
        $records = @(Get-MysqlListenerRecordsForProcess $ownedProcess)
        if ($records.Count -gt 1) {
            Fail "本工作区 mysqld PID $($ownedProcess.Id) 同时监听多个端口:$((@($records | ForEach-Object { $_.Port })) -join ', ')；拒绝猜测实例端口。"
        }
        if ($records.Count -eq 0) {
            Fail "本工作区 mysqld PID $($ownedProcess.Id) 仍存活但尚未监听；它可能正在初始化或异常，拒绝用同一 data dir 启动第二实例。"
        }
        $recovered = $records[0]
        if ($hasExplicit -and $explicitPort -ne $recovered.Port) {
            Fail "本工作区 MySQL 已在 :$($recovered.Port) 运行；拒绝按显式 :$explicitPort 再启动第二实例。"
        }
        Write-Warn2 "端口状态丢失，但已从本工作区 mysqld PID $($ownedProcess.Id) 恢复 listener :$($recovered.Port)。"
        return [int]$recovered.Port
    }

    $candidates = if ($hasExplicit) {
        @($explicitPort)
    } else {
        @(
            if ($storedPort -and $storedPort -ne 3307) { $storedPort }
            $MysqlPortMin..$MysqlPortMax
        ) | Select-Object -Unique
    }

    foreach ($candidate in $candidates) {
        if (Test-PortBindable $candidate) { return [int]$candidate }

        $holders = Get-PortHolder $candidate
        if ($holders.Count -gt 0) {
            Write-Warn2 ("MySQL 候选端口 :{0} 已由外部进程占用({1})，不会复用或停止，继续找空闲端口。" -f $candidate, ($holders -join '; '))
        } else {
            Write-Warn2 "MySQL 候选端口 :$candidate 无法 bind(可能被 Windows 保留)，继续找空闲端口。"
        }
    }

    if ($hasExplicit) {
        Fail "指定的免 Docker MySQL 端口 :$explicitPort 不可用；未启动、未停止、未连接该端口上的任何实例。"
    }
    Fail "免 Docker MySQL 独立端口池 $MysqlPortMin..$MysqlPortMax 全部不可用；未触碰 3307 上的 Docker/MySQL。"
}

function Get-RunningProcess([string]$Name) {
    $f = Get-PidFile $Name
    if (-not (Test-Path -LiteralPath $f)) { return $null }
    $procId = 0
    if (-not [int]::TryParse((Get-Content -LiteralPath $f -Raw).Trim(), [ref]$procId)) { return $null }
    $proc = Get-Process -Id $procId -ErrorAction SilentlyContinue
    if (-not $proc) {
        Remove-Item -LiteralPath $f -Force -ErrorAction SilentlyContinue
        return $null
    }
    if (-not (Test-ComponentProcessOwned $Name $proc)) {
        if ($Name -eq 'mysql') {
            # 活 PID 但归属读不到/不吻合时保留证据。down 不会杀它，reset 也必须被阻断；
            # 否则“无法证明安全”会被误当成“已经退出”，继而删除仍在使用的数据。
            Write-Warn2 "$Name 的 pid 文件指向仍存活的 PID $procId，但映像/启动参数未通过本工作区归属验证；保留登记且不会停止该进程。"
        } else {
            Write-Warn2 "$Name 的 pid 文件指向 PID $procId，但映像/启动参数不属于本工作区；忽略并删除陈旧登记，不会停止该进程。"
            Remove-Item -LiteralPath $f -Force -ErrorAction SilentlyContinue
        }
        return $null
    }
    return $proc
}

function Get-LivePidFileProcess([string]$Name) {
    $file = Get-PidFile $Name
    if (-not (Test-Path -LiteralPath $file -PathType Leaf)) { return $null }
    $registeredId = 0
    try { $raw = (Get-Content -LiteralPath $file -Raw).Trim() } catch { return $null }
    if (-not [int]::TryParse($raw, [ref]$registeredId) -or $registeredId -le 0) { return $null }
    return Get-Process -Id $registeredId -ErrorAction SilentlyContinue
}

function Set-MysqlStateStopped {
    if ($MysqlPort -le 0) { return }
    $state = Get-PandoraLocalInfraPortState $ProjectRoot
    $exe = if ($state) { $state.MysqlExecutable } else { Find-Tool 'mysql' 'mysqld.exe' }
    $ini = if ($state) { $state.MysqlDefaultsFile } else { Get-MysqlIniPath }
    if ($exe -and $ini) {
        Set-PandoraLocalInfraPortState -ProjectRoot $ProjectRoot -MysqlPort $MysqlPort `
            -MysqlProcessId 0 -MysqlExecutable $exe -MysqlDefaultsFile $ini | Out-Null
    }
}

function Invoke-BoundedTool {
    <#
      跑一个外部小工具,最多等 $TimeoutSec 秒,超时就把它本身杀掉。
      为什么不信工具自带的超时选项:mysqladmin 的 shutdown_timeout 默认 3600s,
      实测就是它把整个停止流程挂死了。等待边界必须由调用方掌握,不能交给被调方的默认值。
    #>
    param(
        [Parameter(Mandatory)][string]$FilePath,
        [Parameter(Mandatory)][string[]]$Arguments,
        [int]$TimeoutSec = 15
    )
    try {
        $p = Start-Process -FilePath $FilePath -ArgumentList $Arguments -PassThru -WindowStyle Hidden
        if (-not $p.WaitForExit($TimeoutSec * 1000)) {
            & taskkill.exe /PID $p.Id /T /F 2>&1 | Out-Null
        }
    } catch {
        # 优雅停机是「尽力而为」:失败就退回强杀,不能让它阻断停止流程。
    }
}

function Request-GracefulStop([string]$Name, $Proc = $null) {
    <#
      对齐 docker stop 的语义:compose 路径下 docker stop 会发 SIGTERM,MySQL 会 flush、
      Redis 会存盘。原生进程没人替我们发这个信号,只能自己调管理命令。
      直接 taskkill /F 等于拔电源:MySQL 下次启动要做崩溃恢复,Redis 丢掉上次存盘之后的写入
      —— 策划一天要重启好几次,不能每次都拔电源。
      返回 $true 表示「已经请求过优雅停机,值得多等一会儿」。
    #>
    switch ($Name) {
        'mysql' {
            # 网络 shutdown 的目标必须就是准备停止的那个已归属 PID。状态文件端口陈旧、
            # 被外部 MySQL 接管时宁可跳过优雅停机，也绝不能把 shutdown 发给别人。
            if (-not $Proc) { return $false }
            $listener = Get-OwnedMysqlListenerProcess $MysqlPort
            if (-not $listener -or [int]$listener.Id -ne [int]$Proc.Id) {
                Write-Warn2 "MySQL PID $($Proc.Id) 未被证明是 :$MysqlPort 的本项目 listener；跳过 mysqladmin，仅允许按进程身份回收本项目 PID。"
                return $false
            }
            $admin = Find-Tool 'mysql' 'mysqladmin.exe'
            if (-not $admin) { return $false }
            # 口令走 MYSQL_PWD,不进命令行(命令行密码会出现在进程列表里)。
            $old = $env:MYSQL_PWD
            try {
                $env:MYSQL_PWD = $MysqlRootPwd
                Invoke-BoundedTool -FilePath $admin -TimeoutSec 15 -Arguments @(
                    '--protocol=TCP', '--host=127.0.0.1', "--port=$MysqlPort", '--user=root',
                    '--connect-timeout=5', 'shutdown'
                )
            } finally { $env:MYSQL_PWD = $old }
            return $true
        }
        'redis' {
            $cli = Find-Tool 'redis' 'redis-cli.exe'
            if (-not $cli) { return $false }
            # SHUTDOWN SAVE:存盘后退出,等价 docker stop 时 redis 收到 SIGTERM 的行为。
            Invoke-BoundedTool -FilePath $cli -TimeoutSec 10 -Arguments @(
                '-h', '127.0.0.1', '-p', "$RedisPort", 'shutdown', 'save'
            )
            return $true
        }
        default {
            # envoy:无状态代理,杀掉即可。
            # kafka:KRaft 日志本身就是为崩溃设计的,重启会自愈;它没有等价的一句话停机命令
            #       (自带的 kafka-server-stop.bat 内部也就是 taskkill),没必要自己造一个。
            return $false
        }
    }
}

function Wait-ProcessExit([System.Diagnostics.Process]$Proc, [int]$TimeoutSec) {
    # 有界等待 + 到期重新观测进程状态(不是 sleep 完就假设它好了)。
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while (-not $Proc.HasExited -and (Get-Date) -lt $deadline) { Start-Sleep -Milliseconds 200 }
    return $Proc.HasExited
}

function Stop-OrphanPortHolder([string]$Name, [int[]]$Ports) {
    <#
      pid 文件丢了(pids 目录被清、上一轮进程被手工 taskkill、或换了工作区)时 Stop-Component
      认不出上一轮的进程,新进程起来就 bind 失败。这里按**端口 + 映像路径**兜底:只清理 dist
      目录下我们自己那个 exe,别人的进程一律不碰 —— 端口占用的判据不能只有端口号。
    #>
    $own = [IO.Path]::GetFullPath((Join-Path $DistDir "$Name/$Name.exe"))
    try { $listeners = @(Get-PandoraTcpListenerRecords) }
    catch {
        Write-Warn2 "无法查询 $Name 的残留 listener，不会猜测或停止进程:$($_.Exception.Message)"
        return
    }
    foreach ($port in $Ports) {
        foreach ($c in @($listeners | Where-Object { [int]$_.LocalPort -eq $port })) {
            $p = Get-Process -Id $c.OwningProcess -ErrorAction SilentlyContinue
            if (-not $p) { continue }
            $path = $null
            try { $path = $p.Path } catch { $path = $null }
            if (-not $path -or [IO.Path]::GetFullPath($path) -ne $own) { continue }
            Write-Warn2 "$Name(pid $($p.Id))还占着 :$port 但没有 pid 文件登记 —— 本项目上一轮的残留,清掉。"
            & taskkill.exe /PID $p.Id /T /F 2>&1 | Out-Null
            [void](Wait-ProcessExit $p 10)
        }
    }
}

function Stop-Component([string]$Name) {
    $p = Get-RunningProcess $Name
    # 防御式复核:即使未来调用方绕过/替换了 Get-RunningProcess，也不能凭一串 PID 触发
    # mysqladmin shutdown 或 taskkill。归属读不到时宁可留下自己的残进程，也不能误停别人。
    if ($p -and -not (Test-ComponentProcessOwned $Name $p)) {
        Write-Warn2 "$Name PID $($p.Id) 未通过本工作区归属复核；不会发送停机命令。"
        $p = $null
    }

    if ($Name -eq 'mysql') {
        $registeredLive = Get-LivePidFileProcess 'mysql'
        if ($registeredLive -and -not (Test-ComponentProcessOwned 'mysql' $registeredLive)) {
            Write-Err "mysql.pid 指向仍存活但无法证明属于本工作区的 PID $($registeredLive.Id)；不会停止它，且拒绝把 down/reset 报成功。"
            return $false
        }
    }

    if ($Name -eq 'mysql') {
        $stopPorts = (@(3307, $MysqlPort) + @($MysqlPortMin..$MysqlPortMax)) |
            Where-Object { $_ -gt 0 } | Select-Object -Unique
        $listenerRecord = Get-OnlyOwnedMysqlListener $stopPorts
        if (-not $p -and $listenerRecord) {
            $p = $listenerRecord.Process
            $script:MysqlPort = [int]$listenerRecord.Port
            Write-Warn2 "mysql.pid 缺失或失效，但 :$MysqlPort 的 listener 已通过 exe + my.ini 归属验证；按本项目进程回收。"
        } elseif (-not $p) {
            # mysqld 可能还在初始化、尚未 listen；reset 也不能在这种窗口删除 data。
            $p = Get-OnlyOwnedMysqlProcess
            if ($p) { Write-Warn2 "mysql.pid 缺失且尚未监听，但 PID $($p.Id) 已通过 exe + my.ini 归属验证；按本项目进程回收。" }
        } elseif ($listenerRecord -and [int]$listenerRecord.Process.Id -eq [int]$p.Id) {
            # ports.json 可能陈旧；只采用由同一已归属 PID 实际监听的端口。
            $script:MysqlPort = [int]$listenerRecord.Port
        } elseif ($listenerRecord -and [int]$listenerRecord.Process.Id -ne [int]$p.Id) {
            Write-Err "mysql.pid 与本工作区 listener 指向两个不同 PID；拒绝自动停止，避免误伤。"
            return $false
        }

        if (-not $p) {
            $state = Get-PandoraLocalInfraPortState $ProjectRoot
            if ($state -and [int]$state.MysqlProcessId -gt 0) {
                $stateProc = Get-Process -Id ([int]$state.MysqlProcessId) -ErrorAction SilentlyContinue
                if ($stateProc) {
                    Write-Err "ports.json 指向仍存活但无法证明安全退出的 PID $($state.MysqlProcessId)；拒绝把 down/reset 报成功。"
                    return $false
                }
            }
        }
    }

    if ($p) {
        $procId = $p.Id

        # 只对「已经请求过优雅停机」的组件多等;没请求过就干等纯属浪费(每个组件白等 20s,
        # 一次 down 就多花一分钟)。
        if ((Request-GracefulStop $Name $p)) { [void](Wait-ProcessExit $p 20) }

        if (-not $p.HasExited) {
            # /T 连子进程一起收:留着以防将来某个组件又套一层启动器。
            & taskkill.exe /PID $procId /T /F 2>&1 | Out-Null
            [void](Wait-ProcessExit $p 10)
        }

        # 必须等到它**真的**退出再返回:进程退出、端口释放、数据目录解锁都是异步的,
        # 不等就返回的话「停止后马上启动」会撞上端口被占 / 数据目录被锁 —— 而策划恰恰
        # 天天这么干(改完表点重启),实测就是这么炸的。
        if ($p.HasExited) {
            Write-Ok "$Name 已停止 (PID $procId)"
            Remove-Item -LiteralPath (Get-PidFile $Name) -Force -ErrorAction SilentlyContinue
            if ($Name -eq 'mysql') { Set-MysqlStateStopped }
            return $true
        } else {
            Write-Err "$Name (PID $procId) 30s 内没能停掉 —— 下次启动可能撞端口,请手工确认。"
            # 保留 pid 文件，下一轮仍能追踪；down/reset 必须据此失败。
            return $false
        }
    }
    Remove-Item -LiteralPath (Get-PidFile $Name) -Force -ErrorAction SilentlyContinue
    if ($Name -eq 'mysql') { Set-MysqlStateStopped }
    return $true
}

function Save-Pid([string]$Name, [int]$ProcessId) {
    Set-Content -LiteralPath (Get-PidFile $Name) -Value $ProcessId -Encoding ascii
}

function Read-LogTail([string]$Path, [int]$Lines = 40) {
    <#
      读日志尾部。**不能用 Get-Content**:组件进程可能还开着这个文件(超时那条路径上它还活着),
      Get-Content 默认按 FileShare.Read 打开,撞上写者的独占写句柄会直接抛异常 ——
      于是"诊断"本身失败,现场反而看不到。这里显式用 ReadWrite 共享读。
      返回 $null = 文件不存在 / 读不了;返回空数组 = 文件在但没有内容(这本身就是结论)。
    #>
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { return $null }
    $all = $null
    try {
        $fs = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
        try {
            $sr = [System.IO.StreamReader]::new($fs)
            try { $all = $sr.ReadToEnd() } finally { $sr.Dispose() }
        } finally { $fs.Dispose() }
    } catch { return $null }
    $rows = @($all -split "`r?`n" | Where-Object { $_.Trim() -ne '' })
    # 逗号不能省:PowerShell 会把返回的空数组拆成「什么都没返回」= $null,于是调用方那里
    # 「日志是空的」和「日志读不到」撞成同一个值 —— 而这两条结论正好相反(前者是"别在日志里
    # 找原因",后者是"路径不对")。诊断代码把人指错方向,比不诊断更糟。
    return , @($rows | Select-Object -Last $Lines)
}

function Get-PortHolder([int]$Port) {
    <# 谁在占这个端口。端口冲突是本机基础设施起不来的头号原因,而且"占用者是谁"必须报出来 ——
       只说"端口被占"人还得自己去 netstat 翻。 #>
    $rows = @()
    try { $listeners = @(Get-PandoraTcpListenerRecords | Where-Object { [int]$_.LocalPort -eq $Port }) }
    catch { return , @("listener 查询失败:$($_.Exception.Message)") }
    foreach ($c in $listeners) {
        $p = Get-Process -Id $c.OwningProcess -ErrorAction SilentlyContinue
        if (-not $p) { $rows += "pid $($c.OwningProcess)(进程查不到)"; continue }
        $exePath = $null
        try { $exePath = $p.Path } catch { $exePath = $null }
        $rows += ("{0}(pid {1}){2}" -f $p.ProcessName, $p.Id, $(if ($exePath) { " → $exePath" } else { '' }))
    }
    return , @($rows | Sort-Object -Unique)   # 逗号同上:没有占用者时必须是空数组,不是 $null
}

function Get-PortReservation([int]$Port) {
    <#
      这个端口是不是落在 Windows 的**保留端口区间**里。
      为什么必须单独问一次:Windows 上 bind 失败有两种完全不同的成因,而且报的错不一样 ——
        · WSAEADDRINUSE(10048,"通常每个套接字地址…只允许使用一次"):真有进程 LISTEN 着,
          Get-PortHolder 能指名道姓;
        · WSAEACCES  (10013,"以一种访问权限不允许的方式做了一个访问套接字的尝试"):
          **没有任何进程在 LISTEN**,是 Hyper-V / WSL2 / Docker Desktop 起来时 winnat 预留了
          大段动态端口,把我们的固定端口圈了进去。
      这两种情况的处置办法相反(前者去关那个进程,后者关进程一辈子也关不掉),所以诊断必须
      分得开。实测踩过:mysqld 自己也分不开 —— 它 bind 失败就无脑追问一句
      "Do you already have another mysqld server running on port",于是把人整整带偏一轮。
      返回 $null = 没被保留;否则返回命中的区间描述串。
    #>
    $out = $null
    try { $out = & netsh.exe int ipv4 show excludedportrange protocol=tcp 2>&1 } catch { return $null }
    if (-not $out) { return $null }
    foreach ($line in @($out | ForEach-Object { [string]$_ })) {
        if ($line -notmatch '^\s*(\d+)\s+(\d+)') { continue }
        $start = [int]$Matches[1]; $end = [int]$Matches[2]
        if ($start -le $end -and $Port -ge $start -and $Port -le $end) { return "$start-$end" }
    }
    return $null
}

function Show-PortDiagnosis([int]$Port) {
    <# bind 不上时唯一的权威判据:到底有没有人 LISTEN。没有 → 往保留区间那条路查。 #>
    if ($Port -le 0) { return }
    $holders = Get-PortHolder $Port
    if ($holders.Count -gt 0) {
        Write-Err ("端口 :{0} 的占用者:{1}" -f $Port, ($holders -join '; '))
        return
    }
    Write-Warn2 "端口 :$Port 上没有任何进程在 LISTEN —— 所以这不是「被别的进程占着」。"
    $range = Get-PortReservation $Port
    if ($range) {
        Write-Err ":$Port 落在 Windows 的保留端口区间 $range 里(Hyper-V / WSL2 / Docker Desktop 的 winnat 预留),bind 会直接被拒(WSAEACCES 10013)。"
        Write-Err '解法(管理员 PowerShell,二选一):① net stop winnat; net start winnat —— 让它重新分配预留区间,多数情况当场就好;'
        Write-Err ("② 永久把这个端口占为己有:netsh int ipv4 add excludedportrange protocol=tcp startport={0} numberofports=1 store=persistent(需重启生效)。" -f $Port)
    } else {
        Write-Warn2 "也不在 Windows 保留端口区间里。剩下的可能:安全软件拦了 bind、或别人以独占方式绑了这个端口。用管理员权限跑 netsh int ipv4 show excludedportrange protocol=tcp 再确认一次。"
    }
}

# 日志里认识的错 → 人话 + 该怎么办。命中几条就报几条(一次崩溃常常同时有因和果)。
# 只登记**判据明确**的:猜出来的"可能是杀软吧"只会把人带偏(见 Envoy 那段 AF_UNIX 的教训)。
$script:InfraLogHints = @(
    @{ Pattern = 'Address already in use'
       Message = '端口已被别的进程占着。上面「端口占用者」那行就是它;不是本项目的进程就换掉它或改端口。' }
    # 注意别把这条也写成"端口被占":mysqld 的 MY-010257 是它自己的**猜测**,bind 被拒于
    # WSAEACCES(没人 LISTEN,端口被系统保留)时它照样这么问,照抄就是把人往错方向送。
    @{ Pattern = 'Bind on TCP/IP port|Do you already have another mysqld server running'
       Message = '绑定端口失败。别信 mysqld 那句"是不是已经有一个 mysqld 在跑" —— 它是猜的。以上面「端口 :N」那两行为准:有占用者就去关它;写着「没有任何进程在 LISTEN」就按那里给的保留区间解法处理。' }
    @{ Pattern = "Can't create/write to file|Access is denied|Permission denied|OS errno 13|errno: 13"
       Message = '数据 / 日志目录写不进去。要么这个目录被安全软件锁了,要么工作区放在了需要管理员才能写的位置(如 C:\Program Files)。' }
    @{ Pattern = 'unknown variable|unknown option|Failed to set up|error while setting value'
       Message = '生成的 my.ini 里有本版本 mysqld 不认的配置项 —— 通常是 dist 下的二进制版本和脚本对不上。删掉 run\localinfra\dist\mysql 重跑 provision。' }
    @{ Pattern = 'Cannot allocate memory|Out of memory|mmap\(.*\) failed|Failed to allocate memory for the buffer pool'
       Message = '内存不够(InnoDB 缓冲池默认要 512M)。关掉占内存的程序重试;长期不够就得把 my.ini 的 innodb_buffer_pool_size 调小。' }
    @{ Pattern = 'TCP/IP, --shared-memory, or --named-pipe should be configured'
       Message = 'mysqld 认为所有网络通道都被关了(通常是配置里出现了 skip-networking)。my.ini 由脚本生成,出现这条说明配置被手改过或残留了旧文件 —— 删掉 run\localinfra\cfg\my.ini 重试。' }
    @{ Pattern = 'Execution of init_file|--init-file'
       Message = '启动时执行引导 SQL(建 pandora 账号)失败。日志上一条就是失败的语句;数据目录是旧版本残留时最常见,-Action reset 清掉数据目录可解。' }
    @{ Pattern = 'Data Dictionary initialization failed|The designated data directory .* is unusable|Corrupt|corrupted|Invalid redo log'
       Message = '数据目录坏了或与本版本不兼容。本机是开发数据,直接 -Action reset 清掉重建(会清空本机 MySQL/Kafka/Redis 数据)。' }
    @{ Pattern = 'Another process with pid \d+ is using unix socket file|Unable to lock .*ibdata1'
       Message = '上一轮的 mysqld 还活着占着数据目录。先 -Action down,确认任务管理器里没有 mysqld.exe 再重试。' }
)

function Test-MysqldConfig {
    <#
      mysqld 起不来又**没往日志里写一个字**时,唯一能拿到真话的办法:用 --validate-config
      在前台跑一次,把 stderr 抓回来。
      为什么必须有这一步:mysqld 只有在成功打开 log-error 之后才往日志里写;my.ini 本身有问题、
      或日志文件建不出来时,报错只去 stderr —— 而常驻启动那次刻意没做重定向(见 Kafka 那段
      关于句柄继承的说明),于是那段话谁也看不到,现场只剩一个 exit 1。
      只读、不落盘、几百毫秒。
    #>
    $mysqld = Find-Tool 'mysql' 'mysqld.exe'
    if (-not $mysqld) { return $null }
    $ini = Get-MysqlIniPath
    if (-not (Test-Path -LiteralPath $ini -PathType Leaf)) { return $null }
    try {
        $out = & $mysqld "--defaults-file=$ini" '--validate-config' 2>&1
    } catch { return $null }
    $text = @($out | ForEach-Object { [string]$_ } | Where-Object { $_.Trim() -ne '' })
    if ($text.Count -eq 0) { return $null }
    return , @($text)
}

function Show-ComponentFailure {
    <#
      组件起不来时,把**现场**打到窗口里,而不是丢一个日志路径了事。
      为什么这条很重要:这些一键入口跑在策划机 / 别人的机器上,写脚本的人当场看不到那台机器。
      只给路径 = 让不熟悉的人去翻一个几百行、混着历次启动记录的日志 —— 实际结果是把现场
      原样贴回来这一步就断了,排查从此靠猜。
    #>
    param([string]$Name, [int]$Port, [System.Diagnostics.Process]$Proc)

    if ($Proc -and $Proc.HasExited) {
        $code = $Proc.ExitCode
        # 进程压根没跑起来的两个经典退出码:DLL 缺失(0xC0000135)/ 映像格式不对(0xC000007B)。
        # 这两种情况日志里永远是空的,不单独认出来就会一路往"配置写错了"的方向查。
        if ($code -eq -1073741515) {
            Write-Err "$Name 的 exe 没能加载依赖 DLL (0xC0000135)。多半缺 Visual C++ 运行库,装一次 VC++ 2015-2022 x64 再试。"
        } elseif ($code -eq -1073741701) {
            Write-Err "$Name 的 exe 与本机架构不匹配 (0xC000007B) —— 备料包坏了,删掉 run\localinfra\dist\$Name 重跑 provision。"
        }
    }

    Show-PortDiagnosis $Port

    $logPath = Join-Path $LogDir "$Name.log"
    $tail = Read-LogTail -Path $logPath -Lines 40
    if ($null -eq $tail) {
        Write-Warn2 "读不到日志 $logPath(文件不存在或打不开)。"
    } elseif ($tail.Count -eq 0) {
        Write-Warn2 "日志 $logPath 是空的 —— 说明进程在能写日志之前就死了,别在日志里找原因。"
    } else {
        Write-Host "      ---- $logPath 末尾 $($tail.Count) 行 ----" -ForegroundColor DarkGray
        foreach ($line in $tail) { Write-Host "      $line" -ForegroundColor DarkGray }
        Write-Host "      ---- 日志结束 ----" -ForegroundColor DarkGray

        $text = $tail -join "`n"
        foreach ($h in $script:InfraLogHints) {
            if ($text -match $h.Pattern) { Write-Err $h.Message }
        }
    }

    # mysqld 专用兜底:日志没内容(或没命中任何已知模式)时再问一次配置本身。
    if ($Name -eq 'mysql') {
        $needProbe = ($null -eq $tail) -or ($tail.Count -eq 0)
        if (-not $needProbe) {
            $text = $tail -join "`n"
            $needProbe = -not (@($script:InfraLogHints | Where-Object { $text -match $_.Pattern }).Count -gt 0)
        }
        if ($needProbe) {
            $probe = Test-MysqldConfig
            if ($probe) {
                Write-Err 'mysqld --validate-config 的输出(配置层面的真实报错):'
                foreach ($line in $probe) { Write-Host "      $line" -ForegroundColor DarkGray }
            }
        }
    }

    Write-Host "      完整日志:$logPath" -ForegroundColor DarkGray
}

function Wait-Port([string]$Name, [int]$Port, [int]$TimeoutSec, [System.Diagnostics.Process]$Proc) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        if ($Proc -and $Proc.HasExited) {
            Write-Err "$Name 启动后立即退出 (exit $($Proc.ExitCode))。"
            Show-ComponentFailure -Name $Name -Port $Port -Proc $Proc
            exit 1
        }
        if (Test-PortOpen $Port) {
            if ($Name -ne 'mysql') { return }
            $owner = Get-OwnedMysqlListenerProcess $Port
            if ($owner -and (-not $Proc -or $owner.Id -eq $Proc.Id)) { return }

            Write-Err "端口 :$Port 已能连接，但监听者不是本次启动的本工作区 mysqld；不会把它当成就绪。"
            Show-PortDiagnosis $Port
            exit 1
        }
        Start-Sleep -Milliseconds 500
    }
    Write-Err "$Name 在 ${TimeoutSec}s 内没有监听 :$Port。"
    Show-ComponentFailure -Name $Name -Port $Port -Proc $Proc
    exit 1
}

function New-PlannerInfraStartState {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][int[]]$Ports,
        [Parameter(Mandatory)][System.Diagnostics.Process]$Process,
        [Parameter(Mandatory)][int]$TimeoutSeconds,
        [Parameter(Mandatory)][ValidateSet('direct', 'child')][string]$ListenerOwnerKind,
        [Parameter(Mandatory)][string]$ExpectedExecutable,
        [Parameter(Mandatory)][string[]]$RequiredCommandLineTokens,
        [AllowNull()]$CompletionData = $null,
        [bool]$Reused = $false,
        [int64]$StartedAtMilliseconds = [Environment]::TickCount64,
        [int64]$TimingStartedAtMilliseconds = $StartedAtMilliseconds
    )
    return [pscustomobject]@{
        Name = $Name
        Ports = @($Ports)
        Process = $Process
        StartedAtMilliseconds = $StartedAtMilliseconds
        TimingStartedAtMilliseconds = $TimingStartedAtMilliseconds
        TimeoutMilliseconds = [int64]$TimeoutSeconds * 1000
        ListenerOwnerKind = $ListenerOwnerKind
        ExpectedExecutable = $ExpectedExecutable
        RequiredCommandLineTokens = @($RequiredCommandLineTokens)
        CompletionData = $CompletionData
        Reused = $Reused
        Ready = $false
        ReadyAtMilliseconds = [int64]0
        ComponentFinishedAtMilliseconds = [int64]0
        FinishedAtMilliseconds = [int64]0
        CompletionHandled = $false
        Failure = ''
    }
}

function Get-PlannerInfraProcessIdentity([int]$ProcessId) {
    try {
        return Get-CimInstance Win32_Process -Filter "ProcessId=$ProcessId" -ErrorAction Stop |
            Select-Object -First 1 ProcessId, ParentProcessId, ExecutablePath, CommandLine
    } catch { return $null }
}

function Test-PlannerInfraStateReady($State, [object[]]$Listeners) {
    return Test-PandoraPlannerInfraListenerOwnership -State $State -Listeners $Listeners `
        -GetProcessIdentity { param([int]$ProcessId) Get-PlannerInfraProcessIdentity $ProcessId }
}

function Assert-PlannerInfraExistingState($State) {
    $listeners = @(Get-PandoraTcpListenerRecords)
    if (-not (Test-PlannerInfraStateReady -State $State -Listeners $listeners)) {
        Fail "$($State.Name) 端口已监听，但 PID/映像/启动参数不属于本工作区当前登记；拒绝极速复用。"
    }
    $State.Ready = $true
    $State.ReadyAtMilliseconds = [Environment]::TickCount64
    $State.FinishedAtMilliseconds = $State.ReadyAtMilliseconds
    return $State
}

function Complete-PlannerInfraStartState($State) {
    switch ([string]$State.Name) {
        'mysql' {
            try {
                Invoke-MysqlSql -Sql 'SELECT 1;' -User $MysqlUser -Password $MysqlUserPwd | Out-Null
            } catch {
                Write-Err "MySQL 起来了但账号 '$MysqlUser' 连不上:$($_.Exception.Message)"
                Show-ComponentFailure -Name 'mysql' -Port 0 -Proc $null
                throw 'MySQL 协议探活失败。'
            }
            $owner = Get-OwnedMysqlListenerProcess $MysqlPort
            if (-not $owner -or $owner.Id -ne $State.Process.Id) {
                throw "MySQL :$MysqlPort 虽可连接，但监听 PID 不等于本次启动的 $($State.Process.Id)；拒绝落端口状态。"
            }
            Set-PandoraLocalInfraPortState -ProjectRoot $ProjectRoot -MysqlPort $MysqlPort `
                -MysqlProcessId $owner.Id -MysqlExecutable $owner.Path -MysqlDefaultsFile (Get-MysqlIniPath) | Out-Null
            Write-Ok "MySQL :$MysqlPort(独立端口；不会占用 Docker 的 3307)"
        }
        'redis' {
            $cli = Find-Tool 'redis' 'redis-cli.exe'
            $pong = @(& $cli '-h' '127.0.0.1' '-p' "$RedisPort" 'ping' 2>&1)
            if ($LASTEXITCODE -ne 0 -or ($pong -join "`n").Trim() -cne 'PONG') {
                Write-Err "Redis :$RedisPort listener 已出现但 PING 失败:$($pong -join ' ')"
                Show-ComponentFailure -Name 'redis' -Port $RedisPort -Proc $State.Process
                throw 'Redis 协议探活失败。'
            }
            Write-Ok "Redis :$RedisPort"
        }
        'kafka' { Write-Ok "Kafka :$KafkaPort / controller :$KafkaCtrlPort" }
        'envoy' {
            Set-Content -LiteralPath $State.CompletionData.FingerprintFile `
                -Value $State.CompletionData.Fingerprint -Encoding ascii
            Write-Ok 'Envoy :8443 / :8444'
        }
    }
}

function Get-RemoteFile {
    <#
      断点续传下载。这不是"以防万一"的复杂化 —— 实测从国内拉 260MB 的 MySQL 包,
      连接会在中途被掐(Received an unexpected EOF),不续传就等于永远下不完。
      另外必须带 User-Agent:dev.mysql.com 对空 UA 直接回 403。
    #>
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][string]$OutFile,
        [hashtable]$Headers = @{},
        [int]$MaxAttempts = 12
    )
    $tmp = "$OutFile.part"
    $client = [System.Net.Http.HttpClient]::new()
    try {
        # 单次尝试封顶 5 分钟:HttpClient.Timeout 覆盖整个读流过程,设成"够大"(如 30 分钟)
        # 意味着链路半死不活时会静默卡半小时,策划只会看到一个不动的窗口。因为有断点续传,
        # 到点掐掉再续上没有任何损失 —— 这是把无界等待收敛成有界,不是靠 sleep 掩盖时序。
        $client.Timeout = [TimeSpan]::FromMinutes(5)
        $attempt = 0
        $lastPct = -1
        while ($true) {
            $attempt++
            $have = if (Test-Path -LiteralPath $tmp) { (Get-Item -LiteralPath $tmp).Length } else { 0L }
            try {
                $req = [System.Net.Http.HttpRequestMessage]::new('GET', $Uri)
                $req.Headers.TryAddWithoutValidation('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)') | Out-Null
                foreach ($k in $Headers.Keys) { $req.Headers.TryAddWithoutValidation($k, $Headers[$k]) | Out-Null }
                if ($have -gt 0) { $req.Headers.Range = [System.Net.Http.Headers.RangeHeaderValue]::new($have, $null) }

                $resp = $client.SendAsync($req, [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead).GetAwaiter().GetResult()
                if (-not $resp.IsSuccessStatusCode) { throw "HTTP $([int]$resp.StatusCode) $($resp.ReasonPhrase)" }

                # 服务端不支持 Range(回 200 而不是 206)时只能从头下,否则会把整包又追加一遍。
                $resume = ($have -gt 0 -and [int]$resp.StatusCode -eq 206)
                if (-not $resume) { $have = 0L; Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue }

                $total = if ($resp.Content.Headers.ContentLength) { $have + $resp.Content.Headers.ContentLength } else { 0L }
                $src = $resp.Content.ReadAsStreamAsync().GetAwaiter().GetResult()
                $mode = if ($resume) { [System.IO.FileMode]::Append } else { [System.IO.FileMode]::Create }
                $dst = [System.IO.FileStream]::new($tmp, $mode, [System.IO.FileAccess]::Write)
                try {
                    $buf = New-Object byte[] 1048576
                    $done = $have
                    while (($n = $src.Read($buf, 0, $buf.Length)) -gt 0) {
                        $dst.Write($buf, 0, $n)
                        $done += $n
                        if ($total -gt 0) {
                            $pct = [int](($done * 100) / $total)
                            if ($pct -ge $lastPct + 10) {
                                $lastPct = $pct
                                Write-Host ("    下载中 {0}%  ({1:N0}/{2:N0} MB)" -f $pct, ($done / 1MB), ($total / 1MB))
                            }
                        }
                    }
                } finally { $dst.Dispose(); $src.Dispose() }

                Move-Item -LiteralPath $tmp -Destination $OutFile -Force
                return
            } catch {
                $now = if (Test-Path -LiteralPath $tmp) { (Get-Item -LiteralPath $tmp).Length } else { 0L }
                if ($attempt -ge $MaxAttempts) { throw }
                # 本轮拿到新字节就不算一次"失败尝试",避免慢但可用的链路被误判成不可用。
                if ($now -gt $have) { $attempt-- }
                Write-Warn2 "下载中断($($_.Exception.Message)),已拿到 $([math]::Round($now/1MB))MB,续传重试(剩余 $($MaxAttempts - $attempt) 次)"
                Start-Sleep -Seconds 3
            }
        }
    } finally { $client.Dispose() }
}

function Test-FileSha256 {
    <# 文件哈希是否等于期望值(大小写无关)。文件不存在返回 $false。#>
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Expected
    )
    if (-not (Test-Path -LiteralPath $Path)) { return $false }
    $actual = (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
    return ($actual -eq $Expected.ToLowerInvariant())
}

function New-ArchiveRunFile {
    <# 为本轮创建唯一工作目录和文件路径；下载的 .part 也因此天然唯一。 #>
    param([Parameter(Mandatory)][string]$File)
    $safeName = ([IO.Path]::GetFileName($File) -replace '[^A-Za-z0-9_.-]', '_')
    $runsRoot = Join-Path $CacheDir '.pandora-runs'
    New-Item -ItemType Directory -Force -Path $runsRoot | Out-Null
    $runDirectory = Join-Path $runsRoot "$safeName-$PID-$([guid]::NewGuid().ToString('N'))"
    New-Item -ItemType Directory -Path $runDirectory -ErrorAction Stop | Out-Null
    return (Join-Path $runDirectory $safeName)
}

function New-ArchiveSnapshot {
    <#
      先把来源固定成本轮独享快照，再由调用方校验和解包。同卷优先 hardlink，
      因此 SVN bundle 不会多占一份几百 MB；跨卷或文件系统不支持时退回 copy。
      快照路径唯一，svn update / 并发 cache publish 后续原子替换来源名称时，
      本轮已打开的字节仍保持不变。
    #>
    param(
        [Parameter(Mandatory)][string]$Source,
        [Parameter(Mandatory)][string]$File
    )
    if (-not (Test-Path -LiteralPath $Source -PathType Leaf)) {
        throw "安装包来源不存在:$Source"
    }

    $snapshot = New-ArchiveRunFile -File $File
    $runDirectory = Split-Path -Parent $snapshot
    try {
        try {
            New-Item -ItemType HardLink -Path $snapshot -Target $Source -ErrorAction Stop | Out-Null
        } catch {
            Copy-Item -LiteralPath $Source -Destination $snapshot -ErrorAction Stop
        }
        if (-not (Test-Path -LiteralPath $snapshot -PathType Leaf)) {
            throw "安装包快照未生成:$snapshot"
        }
        return $snapshot
    } catch {
        Remove-Item -LiteralPath $runDirectory -Recurse -Force -ErrorAction SilentlyContinue
        throw
    }
}

function Remove-ArchiveSnapshot {
    <# 只删本轮 .pandora-runs 下的唯一目录，不碰来源、固定 cache 或其他进程的快照。 #>
    param([AllowNull()][string]$Path)
    if (-not $Path) { return }
    try {
        $fullPath = [IO.Path]::GetFullPath($Path)
        $runDirectory = Split-Path -Parent $fullPath
        $runsRoot = [IO.Path]::GetFullPath((Join-Path $CacheDir '.pandora-runs')).TrimEnd('\', '/')
        $runParent = [IO.Path]::GetFullPath((Split-Path -Parent $runDirectory)).TrimEnd('\', '/')
        if ($runParent.Equals($runsRoot, [StringComparison]::OrdinalIgnoreCase)) {
            Remove-Item -LiteralPath $runDirectory -Recurse -Force -ErrorAction SilentlyContinue
        }
    } catch {}
}

function Enter-ArchiveCachePublishLock {
    <#
      固定 cache 名只在毫秒级发布窗口串行。FileMode.CreateNew 提供原子抢占，
      FileShare.None 使 owner 生命期可见；无 fencing 的遗留锁不猜测删除。
    #>
    param([Parameter(Mandatory)][string]$Destination)
    $lock = "$Destination.pandora-publish-lock"
    for ($attempt = 0; $attempt -lt 50; $attempt++) {
        try {
            $stream = [IO.File]::Open($lock, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
            return [pscustomobject]@{ Path = $lock; Stream = $stream }
        } catch {
            Start-Sleep -Milliseconds 100
        }
    }
    throw "无法获取安装包 cache 发布锁:$lock。若已没有其它启动器，请人工核对后只删该锁文件。"
}

function Exit-ArchiveCachePublishLock {
    param([AllowNull()]$LockHandle)
    if (-not $LockHandle) { return }
    try { $LockHandle.Stream.Dispose() } catch {}
    Remove-Item -LiteralPath $LockHandle.Path -Force -ErrorAction SilentlyContinue
    if (Test-Path -LiteralPath $LockHandle.Path) {
        throw "安装包 cache 已处理，但发布锁无法释放:$($LockHandle.Path)"
    }
}

function Publish-ArchiveCacheFile {
    <#
      已验证的本轮快照不直接变成固定 cache。先在 cache 同目录生成唯一 publish
      文件并复核，再持短锁用同卷 replace 发布；并发同哈希的 peer 已发布时直接复用。
    #>
    param(
        [Parameter(Mandatory)][string]$VerifiedPath,
        [Parameter(Mandatory)][string]$Destination,
        [Parameter(Mandatory)][string]$ExpectedSha256
    )
    if (-not (Test-FileSha256 -Path $VerifiedPath -Expected $ExpectedSha256)) {
        throw "拒绝发布未通过 SHA256 的 cache 候选:$VerifiedPath"
    }

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Destination) | Out-Null
    $leaf = ([IO.Path]::GetFileName($Destination) -replace '[^A-Za-z0-9_.-]', '_')
    $publish = Join-Path (Split-Path -Parent $Destination) ".$leaf.pandora-publish-$PID-$([guid]::NewGuid().ToString('N'))"
    $lock = $null
    try {
        try {
            New-Item -ItemType HardLink -Path $publish -Target $VerifiedPath -ErrorAction Stop | Out-Null
        } catch {
            Copy-Item -LiteralPath $VerifiedPath -Destination $publish -ErrorAction Stop
        }
        if (-not (Test-FileSha256 -Path $publish -Expected $ExpectedSha256)) {
            throw "cache 发布候选复核失败:$publish"
        }

        $lock = Enter-ArchiveCachePublishLock -Destination $Destination
        if (Test-Path -LiteralPath $Destination -PathType Leaf) {
            $peerSnapshot = $null
            try {
                $peerSnapshot = New-ArchiveSnapshot -Source $Destination -File ([IO.Path]::GetFileName($Destination))
                if (Test-FileSha256 -Path $peerSnapshot -Expected $ExpectedSha256) { return }
            } finally {
                Remove-ArchiveSnapshot -Path $peerSnapshot
            }
        }

        [IO.File]::Move($publish, $Destination, $true)
        if (-not (Test-FileSha256 -Path $Destination -Expected $ExpectedSha256)) {
            throw "cache 原子发布后复核失败:$Destination"
        }
    } finally {
        Remove-Item -LiteralPath $publish -Force -ErrorAction SilentlyContinue
        Exit-ArchiveCachePublishLock -LockHandle $lock
    }
}

function Get-PackageMarkerPath {
    <# 每个已解包目录都用同一个 marker 文件记录它对应的固定包身份。 #>
    param([Parameter(Mandatory)][string]$Directory)
    return (Join-Path $Directory '.pandora-package.sha256')
}

function Test-PackageMarker {
    <# 只有 marker 精确指向当前固定包时，已有 dist 才能复用。旧安装没有 marker，必须重备。 #>
    param(
        [Parameter(Mandatory)][string]$Directory,
        [Parameter(Mandatory)][string]$ExpectedSha256
    )
    $marker = Get-PackageMarkerPath -Directory $Directory
    if (-not (Test-Path -LiteralPath $marker -PathType Leaf)) { return $false }
    try {
        $actual = ([IO.File]::ReadAllText($marker)).Trim()
        return ($actual -match '^[0-9a-fA-F]{64}$' -and
            $actual.Equals($ExpectedSha256.Trim(), [StringComparison]::OrdinalIgnoreCase))
    } catch {
        return $false
    }
}

function Write-PackageMarker {
    <# marker 最后写：同目录临时文件原子改名，失败或半途退出都不会把未完成安装标成 current。 #>
    param(
        [Parameter(Mandatory)][string]$Directory,
        [Parameter(Mandatory)][string]$Sha256
    )
    $normalized = $Sha256.Trim().ToLowerInvariant()
    if ($normalized -notmatch '^[0-9a-f]{64}$') { throw "无效的安装包 SHA256: $Sha256" }

    New-Item -ItemType Directory -Force -Path $Directory | Out-Null
    $marker = Get-PackageMarkerPath -Directory $Directory
    $temporary = "$marker.tmp"
    try {
        [IO.File]::WriteAllText($temporary, "$normalized`n", [Text.UTF8Encoding]::new($false))
        [IO.File]::Move($temporary, $marker, $true)
    } finally {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
    }
}

function Test-PackageDirectoryMatchesStaging {
    <# 旧安装没有 marker 时，用当前已校验归档解出的 staging 做逐文件强校验。
       仅在策划极速入口的一次性升级路径使用；完全相同才能就地补 marker。 #>
    param(
        [Parameter(Mandatory)][string]$ExistingDirectory,
        [Parameter(Mandatory)][string]$StagedDirectory
    )
    if (-not [IO.Directory]::Exists($ExistingDirectory) -or -not [IO.Directory]::Exists($StagedDirectory)) {
        return $false
    }
    $markerName = [IO.Path]::GetFileName((Get-PackageMarkerPath -Directory $StagedDirectory))
    $existingFiles = @(Get-ChildItem -LiteralPath $ExistingDirectory -Recurse -File -Force |
        Where-Object Name -ne $markerName |
        ForEach-Object {
            [pscustomobject]@{
                Relative = [IO.Path]::GetRelativePath($ExistingDirectory, $_.FullName).Replace('\', '/')
                Path = $_.FullName
                Length = $_.Length
            }
        } | Sort-Object Relative)
    $stagedFiles = @(Get-ChildItem -LiteralPath $StagedDirectory -Recurse -File -Force |
        Where-Object Name -ne $markerName |
        ForEach-Object {
            [pscustomobject]@{
                Relative = [IO.Path]::GetRelativePath($StagedDirectory, $_.FullName).Replace('\', '/')
                Path = $_.FullName
                Length = $_.Length
            }
        } | Sort-Object Relative)
    if ($existingFiles.Count -ne $stagedFiles.Count) { return $false }
    for ($i = 0; $i -lt $existingFiles.Count; $i++) {
        $left = $existingFiles[$i]
        $right = $stagedFiles[$i]
        if ($left.Relative -cne $right.Relative -or $left.Length -ne $right.Length) { return $false }
        $leftHash = (Get-FileHash -LiteralPath $left.Path -Algorithm SHA256).Hash
        $rightHash = (Get-FileHash -LiteralPath $right.Path -Algorithm SHA256).Hash
        if ($leftHash -cne $rightHash) { return $false }
    }
    return $true
}

function Test-ProcessReferencesDirectory {
    <# 进程映像或命令行是否精确引用目标 dist。只看进程名 / 端口会把 Docker 或外部实例误判成本工作区。 #>
    param(
        [Parameter(Mandatory)]$Proc,
        [Parameter(Mandatory)][string]$Directory
    )
    if (-not $Proc) { return $false }
    try {
        $root = [IO.Path]::GetFullPath($Directory).TrimEnd('\', '/') -replace '/', '\'
    } catch { return $false }
    $prefix = "$root\"

    $processPath = $null
    try { $processPath = [string]$Proc.Path } catch { $processPath = $null }
    if ($processPath) {
        try {
            $fullProcessPath = [IO.Path]::GetFullPath($processPath) -replace '/', '\'
            if ($fullProcessPath.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) { return $true }
        } catch {}
    }

    $commandLine = Get-ProcessCommandLine ([int]$Proc.Id)
    if (-not $commandLine) { return $false }
    $normalizedCommandLine = $commandLine -replace '/', '\'
    return ($normalizedCommandLine.IndexOf($prefix, [StringComparison]::OrdinalIgnoreCase) -ge 0)
}

function Get-PackageDirectoryConsumers {
    <#
      返回真实引用目标 dist 的进程。PID 文件只是候选，不是归属证明；同时按精确映像 / 命令行
      扫描可找回 PID 文件丢失的本工作区进程。JRE 与 Kafka 是共享消费关系，升级任一都要拦 Kafka。
    #>
    param(
        [Parameter(Mandatory)][string]$Component,
        [Parameter(Mandatory)][string]$TargetDirectory
    )

    $serviceNames = @()
    $processNames = @()
    switch ($Component) {
        'mysql'  { $serviceNames = @('mysql');  $processNames = @('mysqld') }
        'redis'  { $serviceNames = @('redis');  $processNames = @('redis-server') }
        'kafka'  { $serviceNames = @('kafka');  $processNames = @('java', 'javaw') }
        'jre'    { $serviceNames = @('kafka');  $processNames = @('java', 'javaw') }
        'envoy'  { $serviceNames = @('envoy');  $processNames = @('envoy') }
        'mkcert' { $processNames = @('mkcert') }
        default  { return @() }
    }

    $hits = @{}
    foreach ($serviceName in $serviceNames) {
        $registered = Get-LivePidFileProcess $serviceName
        if ($registered -and (Test-ProcessReferencesDirectory -Proc $registered -Directory $TargetDirectory)) {
            $hits[[int]$registered.Id] = $registered
        }
    }
    foreach ($processName in $processNames) {
        foreach ($proc in @(Get-Process -Name $processName -ErrorAction SilentlyContinue)) {
            if (Test-ProcessReferencesDirectory -Proc $proc -Directory $TargetDirectory) {
                $hits[[int]$proc.Id] = $proc
            }
        }
    }
    return @($hits.Values)
}

function Assert-PackageDirectoryNotInUse {
    param(
        [Parameter(Mandatory)][string]$Component,
        [Parameter(Mandatory)][string]$TargetDirectory
    )
    $consumers = @(Get-PackageDirectoryConsumers -Component $Component -TargetDirectory $TargetDirectory)
    if ($consumers.Count -eq 0) { return }
    $details = @($consumers | ForEach-Object {
        $name = try { $_.ProcessName } catch { $Component }
        "${name}(PID=$($_.Id))"
    }) -join ', '
    Fail "$Component 的当前 dist 正被本工作区进程使用:$details。拒绝覆盖或强杀；请先运行 pwsh tools/scripts/local_infra.ps1 -Action down，确认停止后再重试。"
}

function New-PackageStagingDirectory {
    <# staging 与正式目录同属 DistDir，后续 Directory.Move 才是同卷目录 rename，而不是跨卷复制。 #>
    param([Parameter(Mandatory)][string]$Component)
    $safeName = $Component -replace '[^A-Za-z0-9_.-]', '_'
    $path = Join-Path $DistDir ".$safeName.pandora-stage-$PID-$([guid]::NewGuid().ToString('N'))"
    New-Item -ItemType Directory -Path $path -ErrorAction Stop | Out-Null
    return $path
}

function Move-PackageDirectoryAtomic {
    <# 同一父目录内的目录 rename；不允许隐式退化成跨卷复制。 #>
    param(
        [Parameter(Mandatory)][string]$Source,
        [Parameter(Mandatory)][string]$Destination
    )
    $sourceFull = [IO.Path]::GetFullPath($Source)
    $destinationFull = [IO.Path]::GetFullPath($Destination)
    $sourceParent = [IO.Path]::GetFullPath((Split-Path -Parent $sourceFull))
    $destinationParent = [IO.Path]::GetFullPath((Split-Path -Parent $destinationFull))
    if (-not $sourceParent.Equals($destinationParent, [StringComparison]::OrdinalIgnoreCase)) {
        throw "安装包目录 swap 必须位于同一父目录: $sourceFull -> $destinationFull"
    }
    if (Test-Path -LiteralPath $destinationFull) { throw "安装包目录 swap 目标已存在:$destinationFull" }
    $fastStart = [bool](Get-Variable -Name PlannerFastStart -ValueOnly -ErrorAction SilentlyContinue)
    if (-not $fastStart) {
        [IO.Directory]::Move($sourceFull, $destinationFull)
        return
    }
    # 仅策划极速入口：Defender/索引器可能在解包结束后的极短窗口仍持有新文件句柄。
    # 只对 UnauthorizedAccess/IO 短暂退避 3 次；其他入口保持原来的单次失败行为。
    $delays = @(0, 150, 500)
    for ($attempt = 0; $attempt -lt $delays.Count; $attempt++) {
        if ($delays[$attempt] -gt 0) { Start-Sleep -Milliseconds $delays[$attempt] }
        try {
            [IO.Directory]::Move($sourceFull, $destinationFull)
            return
        } catch [UnauthorizedAccessException], [IO.IOException] {
            if ($attempt -eq $delays.Count - 1) { throw }
        }
    }
}

function Publish-StagedPackageDirectory {
    <#
      staging 已完成解包、probe 和 marker 后才进入这里。先验证当前目录没有消费者，再把旧目录
      rename 成 backup、staging rename 成正式目录；第二步失败时立刻把旧目录原路径回滚。
    #>
    param(
        [Parameter(Mandatory)][string]$Component,
        [Parameter(Mandatory)][string]$StagedDirectory,
        [Parameter(Mandatory)][string]$TargetDirectory,
        [Parameter(Mandatory)][string]$ExpectedSha256
    )
    if (-not (Test-Path -LiteralPath $StagedDirectory -PathType Container)) {
        throw "安装包 staging 不存在:$StagedDirectory"
    }
    if (-not (Test-PackageMarker -Directory $StagedDirectory -ExpectedSha256 $ExpectedSha256)) {
        throw "安装包 staging 缺少当前 SHA256 marker:$StagedDirectory"
    }

    # 紧贴 rename 再查一次，缩小「检查后才启动」的竞争窗口；工作区编排锁会拦住正常入口并发。
    Assert-PackageDirectoryNotInUse -Component $Component -TargetDirectory $TargetDirectory

    $backup = "$TargetDirectory.pandora-backup-$PID-$([guid]::NewGuid().ToString('N'))"
    $oldMoved = $false
    $published = $false
    try {
        if (Test-Path -LiteralPath $TargetDirectory) {
            Move-PackageDirectoryAtomic -Source $TargetDirectory -Destination $backup
            $oldMoved = $true
        }
        try {
            Move-PackageDirectoryAtomic -Source $StagedDirectory -Destination $TargetDirectory
            $published = $true
        } catch {
            $publishError = $_.Exception
            if ($oldMoved) {
                try {
                    if (Test-Path -LiteralPath $TargetDirectory) {
                        throw "失败后正式目录意外存在，拒绝覆盖:$TargetDirectory"
                    }
                    Move-PackageDirectoryAtomic -Source $backup -Destination $TargetDirectory
                    $oldMoved = $false
                } catch {
                    throw "发布 $Component 失败且旧目录回滚失败。旧目录保留在 $backup；不要启动服务，先人工恢复。发布错误:$($publishError.Message)；回滚错误:$($_.Exception.Message)"
                }
            }
            throw $publishError
        }
    } finally {
        # 只有新目录已完整就位才清旧备份；清理失败不回退已成功发布的新版本，保留路径供人工清理。
        if ($published -and $oldMoved -and (Test-Path -LiteralPath $TargetDirectory) -and (Test-Path -LiteralPath $backup)) {
            try { Remove-Item -LiteralPath $backup -Recurse -Force -ErrorAction Stop }
            catch { Write-Warn2 "新 $Component 已发布，但旧目录清理失败，保留在 $backup：$($_.Exception.Message)" }
        }
    }
}

function Get-Archive {
    <# 取得一个可用归档，并保证返回的本轮独享快照 sha256 == $Sha256。
       仓库 bundle 同卷 hardlink 为快照，不重复占用几百 MB；显式镜像和公网来源仍落 cache。

       三条取包路径(本机 cache / 选定的仓库包或显式镜像 / 公网 URL)统一在这里校验,少校验任何一条
       都等于留了个后门:cache 和本地目录都是普通可写目录,不受 HTTPS 保护,而这些包解开
       就直接执行。校验不过的本轮快照一律清理,绝不返回给调用方解包。 #>
    param(
        [Parameter(Mandatory)][string]$File,
        [Parameter(Mandatory)][string[]]$Urls,
        [Parameter(Mandatory)][string]$Sha256,
        [hashtable]$Headers = @{}
    )
    $dest = Join-Path $CacheDir $File

    # ① 本机 cache:命中也要校验。上次下到一半、磁盘坏块、或者有人手工往 cache 里塞了个包,
    #    都会在这里被挡下;本轮忽略坏固定名并重新获取，不与并发 publisher 互删。
    if (Test-Path -LiteralPath $dest) {
        $snapshot = New-ArchiveSnapshot -Source $dest -File $File
        if (-not $Force -and (Test-FileSha256 -Path $snapshot -Expected $Sha256)) { return $snapshot }
        Remove-ArchiveSnapshot -Path $snapshot
        if (-not $Force) { Write-Warn2 "缓存的 $File 校验不通过(已损坏或被替换),忽略它并重新获取。" }
    }

    # ② SVN 仓库安装包或显式离线镜像。按**具体文件**存在才命中；空目录 / 缺这个版本会继续公网。
    #    同名文件校验不过则直接失败,不静默回退公网 —— 这表示 SVN 包 / 共享盘放错版本或被替换,
    #    偷偷绕过只会掩盖供应链问题。
    #
    #    仓库 bundle 就在本机工作副本，先 hardlink/copy 成本轮快照再校验；同卷 hardlink
    #    不重复占用 544.5 MiB。显式镜像可能是会断开的 UNC / 移动盘，仍先复制到本轮本地快照，
    #    再安全发布 cache。两条路径的执行闸都是同一个固定 SHA256。
    if ($PackageMirror -and $PackageMirror.Path) {
        $src = Join-Path $PackageMirror.Path $File
        if (Test-Path -LiteralPath $src) {
            if ($PackageMirror.Kind -eq '仓库安装包') {
                Write-Host "    从仓库安装包创建稳定快照: $src"
                $snapshot = New-ArchiveSnapshot -Source $src -File $File
                if (Test-FileSha256 -Path $snapshot -Expected $Sha256) { return $snapshot }
                Remove-ArchiveSnapshot -Path $snapshot
                Fail "仓库安装包里的 $File 校验不通过(期望 sha256 $Sha256)。请先 svn update；确认前不要绕过或手改源文件。"
            }

            Write-Host "    从$($PackageMirror.Kind)拷贝到本机 cache: $src"
            $snapshot = New-ArchiveRunFile -File $File
            try {
                Copy-Item -LiteralPath $src -Destination $snapshot -ErrorAction Stop
                if (Test-FileSha256 -Path $snapshot -Expected $Sha256) {
                    Publish-ArchiveCacheFile -VerifiedPath $snapshot -Destination $dest -ExpectedSha256 $Sha256
                    return $snapshot
                }
            } catch {
                Remove-ArchiveSnapshot -Path $snapshot
                throw
            }
            Remove-ArchiveSnapshot -Path $snapshot
            Fail "$($PackageMirror.Kind)里的 $File 校验不通过(期望 sha256 $Sha256)。请找后端同学确认 $($PackageMirror.Path) 中的文件,确认前不要绕过。"
        }
    }

    # ③ 公网,按顺序试。某个源给的包对不上就换下一个源,全都不行才报错。
    $lastErr = $null
    foreach ($u in $Urls) {
        Write-Host "    下载 $File  <- $u"
        $snapshot = New-ArchiveRunFile -File $File
        $keepSnapshot = $false
        try {
            Get-RemoteFile -Uri $u -OutFile $snapshot -Headers $Headers
            if (Test-FileSha256 -Path $snapshot -Expected $Sha256) {
                Publish-ArchiveCacheFile -VerifiedPath $snapshot -Destination $dest -ExpectedSha256 $Sha256
                $keepSnapshot = $true
                return $snapshot
            }
            $actual = (Get-FileHash -LiteralPath $snapshot -Algorithm SHA256).Hash.ToLowerInvariant()
            Write-Warn2 "该地址下到的 $File 校验不通过(期望 $Sha256,实际 $actual),换下一个源。"
            $lastErr = [Exception]::new("sha256 不匹配($u)")
        } catch {
            $lastErr = $_.Exception
            Write-Warn2 "该地址不可用:$($_.Exception.Message)"
        } finally {
            # 换源必须丢掉本轮唯一的半截文件；绝不删其它 run 的 .part。
            if (-not $keepSnapshot) { Remove-ArchiveSnapshot -Path $snapshot }
        }
    }
    throw "所有下载地址都失败或校验不通过($File):$($lastErr.Message)。先 svn update 获取 installers/localinfra；也可让后端同学提供共享盘并设置 PANDORA_LOCALINFRA_MIRROR。"
}

function Expand-Archive2 {
    <# 用 Windows 自带 bsdtar 解包(zip / tgz 通吃,比 Expand-Archive 快一个数量级)。#>
    param(
        [Parameter(Mandatory)][string]$Archive,
        [Parameter(Mandatory)][string]$Destination
    )
    New-Item -ItemType Directory -Force -Path $Destination | Out-Null
    & tar.exe -x -f $Archive -C $Destination
    if ($LASTEXITCODE -ne 0) { throw "解包失败: $Archive (tar exit=$LASTEXITCODE)" }
}

function Find-ToolInDirectory([string]$Directory, [string]$Exe, [string]$RelativePath = '') {
    if (-not [IO.Directory]::Exists($Directory)) { return $null }
    if ($RelativePath) {
        $candidate = Join-Path $Directory $RelativePath
        if ([IO.File]::Exists($candidate)) {
            return [IO.Path]::GetFullPath($candidate)
        }
        return $null
    }
    $hit = Get-ChildItem -LiteralPath $Directory -Recurse -File -Filter $Exe -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($hit) { return $hit.FullName }
    return $null
}

function Find-Tool([string]$Component, [string]$Exe) {
    $relativePath = ''
    $fastStart = [bool](Get-Variable -Name PlannerFastStart -ValueOnly -ErrorAction SilentlyContinue)
    if ($fastStart -and $Components.Contains($Component) -and $Components[$Component].Tools.Contains($Exe)) {
        $relativePath = $Components[$Component].Tools[$Exe]
    }
    return (Find-ToolInDirectory -Directory (Join-Path $DistDir $Component) -Exe $Exe -RelativePath $relativePath)
}

function Get-PlannerPackageSetFingerprint {
    $lines = [Collections.Generic.List[string]]::new()
    $lines.Add("mysql-mode|$($LocalInfraLifecyclePlan.Mode)")
    foreach ($name in @($Components.Keys | Where-Object { $LocalInfraLifecyclePlan.ProvisionComponents -contains $_ })) {
        $lines.Add("$name|$($Components[$name].File)|$($Components[$name].Sha256.ToLowerInvariant())")
    }
    $lines.Add("mkcert|$MkcertFile|$($MkcertSha256.ToLowerInvariant())")
    $lines.Add("envoy|$EnvoyImageTag|$(($EnvoyLayerDigest -replace '^sha256:', '').ToLowerInvariant())")
    $bytes = [Text.UTF8Encoding]::new($false).GetBytes(($lines -join "`n"))
    return [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($bytes)).ToLowerInvariant()
}

function Get-PlannerPackageSetReceiptPath {
    return (Join-Path $CfgDir 'package-set-ready.sha256')
}

function Test-PlannerPackageSetReady {
    if (-not $PlannerFastStart -or $Force) { return $false }
    $receipt = Get-PlannerPackageSetReceiptPath
    if (-not [IO.File]::Exists($receipt)) { return $false }
    try {
        if (-not ([IO.File]::ReadAllText($receipt).Trim().Equals(
            (Get-PlannerPackageSetFingerprint), [StringComparison]::OrdinalIgnoreCase))) { return $false }
    } catch { return $false }
    foreach ($name in @($Components.Keys | Where-Object { $LocalInfraLifecyclePlan.ProvisionComponents -contains $_ })) {
        if (-not (Find-Tool $name $Components[$name].Probe)) { return $false }
    }
    return [IO.File]::Exists((Join-Path $DistDir 'mkcert/mkcert.exe')) -and
        [IO.File]::Exists((Join-Path $DistDir 'envoy/envoy.exe'))
}

function Write-PlannerPackageSetReceipt {
    if (-not $PlannerFastStart) { return }
    New-Item -ItemType Directory -Force -Path $CfgDir | Out-Null
    $receipt = Get-PlannerPackageSetReceiptPath
    $temporary = "$receipt.$PID.$([guid]::NewGuid().ToString('N')).tmp"
    try {
        [IO.File]::WriteAllText($temporary, "$(Get-PlannerPackageSetFingerprint)`n", [Text.UTF8Encoding]::new($false))
        [IO.File]::Move($temporary, $receipt, $true)
    } finally {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
    }
}

function Register-LocalToolPath {
    <# 把自带工具目录挂进**本进程** PATH,让 envoy_cert.ps1 里的 `Get-Command mkcert` 能命中。
       只改 $env:PATH,不碰用户 / 系统环境变量 —— 策划机环境保持干净,卸载 = 删 run/localinfra。
       放在前面(prepend)是刻意的:本机若真装过老版本 mkcert,也以我们钉死版本的这份为准。 #>
    $mkcertDir = Join-Path $DistDir 'mkcert'
    if (-not (Test-Path -LiteralPath $mkcertDir)) { return }
    $parts = $env:PATH -split ';'
    if ($parts -notcontains $mkcertDir) { $env:PATH = "$mkcertDir;$env:PATH" }
}

# ===== 备料 =====

function Invoke-ProvisionAll {
    New-Item -ItemType Directory -Force -Path $DistDir, $DataDir, $LogDir, $PidDir, $CfgDir, $CacheDir | Out-Null

    if (Test-PlannerPackageSetReady) {
        Write-Ok '第三方便携包整套已安装，极速跳过备料检查'
        return
    }

    if (-not (Get-Command tar.exe -ErrorAction SilentlyContinue)) {
        Fail ' 本机没有 tar.exe(Windows 10 1803+ 自带)。请升级系统或手工解包到 run/localinfra/dist/。'
    }

    foreach ($name in @($Components.Keys | Where-Object { $LocalInfraLifecyclePlan.ProvisionComponents -contains $_ })) {
        $c = $Components[$name]
        $target = Join-Path $DistDir $name
        $have = Find-Tool $name $c.Probe
        if ($have -and -not $Force -and (Test-PackageMarker -Directory $target -ExpectedSha256 $c.Sha256)) {
            Write-Ok "$name $($c.Version) 已就位"
            continue
        }

        Write-Step "备料 $name $($c.Version)"
        $ar = $null
        $staged = $null
        try {
            $ar = Get-Archive -File $c.File -Urls $c.Urls -Sha256 $c.Sha256
            $staged = New-PackageStagingDirectory -Component $name
            Expand-Archive2 -Archive $ar -Destination $staged
            if (-not (Test-FileSha256 -Path $ar -Expected $c.Sha256)) {
                Fail "$name 解包期间安装包快照发生变化，拒绝发布 staging:$ar"
            }
            $fastStart = [bool](Get-Variable -Name PlannerFastStart -ValueOnly -ErrorAction SilentlyContinue)
            $probeRelativePath = if ($fastStart) { $c.Tools[$c.Probe] } else { '' }
            if (-not (Find-ToolInDirectory -Directory $staged -Exe $c.Probe -RelativePath $probeRelativePath)) {
                Fail "$name 解包后找不到 $($c.Probe),压缩包结构可能变了: $ar"
            }
            Write-PackageMarker -Directory $staged -Sha256 $c.Sha256
            $adopted = $false
            if ($fastStart -and [IO.Directory]::Exists($target) -and
                -not [IO.File]::Exists((Get-PackageMarkerPath -Directory $target)) -and
                (Test-PackageDirectoryMatchesStaging -ExistingDirectory $target -StagedDirectory $staged)) {
                Assert-PackageDirectoryNotInUse -Component $name -TargetDirectory $target
                Write-PackageMarker -Directory $target -Sha256 $c.Sha256
                $adopted = $true
                Write-Ok "$name 旧目录逐文件校验一致，已就地升级 marker"
            }
            if (-not $adopted) {
                Publish-StagedPackageDirectory -Component $name -StagedDirectory $staged `
                    -TargetDirectory $target -ExpectedSha256 $c.Sha256
            }
        } finally {
            if ($staged -and (Test-Path -LiteralPath $staged)) {
                Remove-Item -LiteralPath $staged -Recurse -Force -ErrorAction SilentlyContinue
            }
            Remove-ArchiveSnapshot -Path $ar
        }
        Write-Ok "$name $($c.Version) 备料完成"
    }

    Confirm-MkcertBinary
    Confirm-EnvoyBinary
    Write-PlannerPackageSetReceipt
}

function Confirm-MkcertBinary {
    <# 备料 mkcert(裸 exe)。已装了系统级 mkcert 也照样备一份自己的:版本钉死,
       免得各机器 mkcert 版本不一导致签出来的证书行为有差。 #>
    $dir = Join-Path $DistDir 'mkcert'
    $exe = Join-Path $dir 'mkcert.exe'
    if ((Test-Path -LiteralPath $exe) -and -not $Force -and (Test-PackageMarker -Directory $dir -ExpectedSha256 $MkcertSha256)) {
        Register-LocalToolPath
        Write-Ok "mkcert $MkcertVersion 已就位"
        return
    }
    Write-Step "备料 mkcert $MkcertVersion(签 Envoy 本机 TLS 证书用,策划机不用自己装)"
    # 校验在 Get-Archive 里做(cache / 共享盘 / 公网三条路径统一过一道),拿到就是对的。
    $src = $null
    $staged = $null
    try {
        $src = Get-Archive -File $MkcertFile -Urls $MkcertUrls -Sha256 $MkcertSha256
        $staged = New-PackageStagingDirectory -Component 'mkcert'
        $stagedExe = Join-Path $staged 'mkcert.exe'
        Copy-Item -LiteralPath $src -Destination $stagedExe -Force
        if (-not (Test-FileSha256 -Path $src -Expected $MkcertSha256) -or
            -not (Test-FileSha256 -Path $stagedExe -Expected $MkcertSha256)) {
            Fail "mkcert 复制到 staging 后校验不通过:$stagedExe"
        }
        Write-PackageMarker -Directory $staged -Sha256 $MkcertSha256
        Publish-StagedPackageDirectory -Component 'mkcert' -StagedDirectory $staged `
            -TargetDirectory $dir -ExpectedSha256 $MkcertSha256
    } finally {
        if ($staged -and (Test-Path -LiteralPath $staged)) {
            Remove-Item -LiteralPath $staged -Recurse -Force -ErrorAction SilentlyContinue
        }
        Remove-ArchiveSnapshot -Path $src
    }
    Register-LocalToolPath
    Write-Ok "mkcert $MkcertVersion 备料完成"
}

function Confirm-EnvoyBinary {
    $dir = Join-Path $DistDir 'envoy'
    $exe = Join-Path $dir 'envoy.exe'
    $blobName = "envoy-windows-$EnvoyImageTag-layer.tar.gz"
    $wantSha = ($EnvoyLayerDigest -replace '^sha256:', '')
    if ((Test-Path -LiteralPath $exe) -and -not $Force -and (Test-PackageMarker -Directory $dir -ExpectedSha256 $wantSha)) {
        Write-Ok "envoy $EnvoyImageTag 已就位"
        return
    }
    Write-Step "备料 envoy $EnvoyImageTag(从官方 Windows 镜像层取 exe,不需要本机装 docker)"

    # registry 的 blob digest 就是内容的 sha256,直接当期望值交给 Get-Archive 统一校验。
    # 拿 token 要先看 cache / 仓库安装包 / 显式镜像有没有:已经有包的机器必须做到零公网请求。
    $blobFile = Join-Path $CacheDir $blobName
    $headers = @{}
    $needNetwork = $Force -or -not (Test-FileSha256 -Path $blobFile -Expected $wantSha)
    if ($needNetwork -and -not ($PackageMirror -and $PackageMirror.Path -and (Test-Path -LiteralPath (Join-Path $PackageMirror.Path $blobName)))) {
        $tokUri = "https://auth.docker.io/token?service=registry.docker.io&scope=repository:${EnvoyImageRepo}:pull"
        $tok = (Invoke-RestMethod -Uri $tokUri -TimeoutSec 60).token
        if (-not $tok) { Fail '拿不到 docker registry 匿名 token,检查网络 / 代理。' }
        $headers = @{ Authorization = "Bearer $tok" }
    }
    $blobFile = $null
    $extractStage = Join-Path $CacheDir "envoy-extract-$PID-$([guid]::NewGuid().ToString('N'))"
    $staged = $null
    try {
        $blobFile = Get-Archive -File $blobName `
            -Urls @("https://registry-1.docker.io/v2/$EnvoyImageRepo/blobs/$EnvoyLayerDigest") `
            -Sha256 $wantSha -Headers $headers
        $staged = New-PackageStagingDirectory -Component 'envoy'
        New-Item -ItemType Directory -Force -Path $extractStage | Out-Null
        & tar.exe -x -z -f $blobFile -C $extractStage $EnvoyExeInLayer
        if ($LASTEXITCODE -ne 0) { Fail "从镜像层解出 envoy.exe 失败 (tar exit=$LASTEXITCODE)" }
        if (-not (Test-FileSha256 -Path $blobFile -Expected $wantSha)) {
            Fail "envoy 解包期间镜像层快照发生变化，拒绝发布 staging:$blobFile"
        }

        $src = Join-Path $extractStage $EnvoyExeInLayer
        if (-not (Test-Path -LiteralPath $src -PathType Leaf)) { Fail "镜像层里没有 $EnvoyExeInLayer" }
        $stagedExe = Join-Path $staged 'envoy.exe'
        Copy-Item -LiteralPath $src -Destination $stagedExe -Force
        if (-not (Test-Path -LiteralPath $stagedExe -PathType Leaf)) { Fail "envoy 复制到 staging 后不存在:$stagedExe" }
        Write-PackageMarker -Directory $staged -Sha256 $wantSha
        Publish-StagedPackageDirectory -Component 'envoy' -StagedDirectory $staged `
            -TargetDirectory $dir -ExpectedSha256 $wantSha
    } finally {
        if (Test-Path -LiteralPath $extractStage) {
            Remove-Item -LiteralPath $extractStage -Recurse -Force -ErrorAction SilentlyContinue
        }
        if ($staged -and (Test-Path -LiteralPath $staged)) {
            Remove-Item -LiteralPath $staged -Recurse -Force -ErrorAction SilentlyContinue
        }
        Remove-ArchiveSnapshot -Path $blobFile
    }
    Write-Ok "envoy $EnvoyImageTag 备料完成"
}

function Get-PwshBootstrapPin {
    <# 读 lib/pwsh_bootstrap.pin —— 与 bootstrap_pwsh.cmd 共用的版本 / 校验和唯一来源。
       刻意不用 ConvertFrom-StringData:那套有自己的转义语义,而这份文件同时要被 cmd.exe 的
       `for /f` 读,两边的解析规则必须一样笨(KEY=VALUE,# 开头是注释),才不会出现
       「PowerShell 这边读出来是一个值、cmd 那边读出来是另一个」的漂移。 #>
    $pin = Join-Path $PSScriptRoot 'lib/pwsh_bootstrap.pin'
    if (-not (Test-Path -LiteralPath $pin)) { Fail "缺少 $pin(工作区不完整)" }
    $map = @{}
    foreach ($line in [System.IO.File]::ReadAllLines($pin)) {
        $t = $line.Trim()
        if (-not $t -or $t.StartsWith('#')) { continue }
        $i = $t.IndexOf('=')
        if ($i -lt 1) { continue }
        $map[$t.Substring(0, $i).Trim()] = $t.Substring($i + 1).Trim()
    }
    foreach ($k in 'PWSH_VERSION', 'PWSH_FILE', 'PWSH_SHA256', 'PWSH_URL') {
        if (-not $map[$k]) { Fail "$pin 缺 $k" }
    }
    return $map
}

function Save-PwshBootstrapArchive {
    <# 确保 PowerShell 7 免安装包可用 —— 给「连 pwsh 都没有」的机器自举用。

       为什么只归 provision、不归 up:所有一键入口本身就是 pwsh 脚本,能跑到 up 的机器必然
       已经有解释器了,再下 100MB 纯属浪费。真正需要这个包的是 bootstrap_pwsh.cmd,而它是
       cmd.exe 在「pwsh 还不存在」的时候跑的,只能自己从 cache / 仓库包 / 共享盘 / 公网取。所以这一
       步的意义是让维护者把它和其它压缩包一起备进 run/localinfra/cache,再更新 SVN 的
       installers/localinfra(或共享盘);策划机 svn update 后就能完全离线地自举解释器。

       本机不解包:解包路径归 bootstrap_pwsh.cmd(只有它知道本机到底缺不缺 pwsh)。
       版本 / SHA256 只在 lib/pwsh_bootstrap.pin 写一份,这里不抄(抄了迟早漂)。 #>
    $pin = Get-PwshBootstrapPin
    Write-Step "备料 PowerShell $($pin.PWSH_VERSION)(给没装 pwsh 的机器自举用,本机不解包)"
    $ar = $null
    try {
        $ar = Get-Archive -File $pin.PWSH_FILE -Urls @($pin.PWSH_URL) -Sha256 $pin.PWSH_SHA256
        Write-Ok "PowerShell $($pin.PWSH_VERSION) 自举归档快照可用:$ar"
    } finally {
        Remove-ArchiveSnapshot -Path $ar
    }
}

# ===== MySQL =====

function Get-MysqlIniPath { Join-Path $CfgDir 'my.ini' }

function New-MysqlIni([string]$BaseDir) {
    $data = (Join-Path $DataDir 'mysql') -replace '\\', '/'
    $base = $BaseDir -replace '\\', '/'
    $errlog = (Join-Path $LogDir 'mysql.log') -replace '\\', '/'
    # sql_mode 必须含 STRICT_TRANS_TABLES:pkg/dbguard.AssertStrictMode 启动即断言,
    # 非严格模式下超长写入会被静默截断(CLAUDE.md §9.24)。8.4 默认就是严格,这里显式钉死。
    # mysqlx=OFF:免安装版默认还会开 33060,本机没人用,省一个端口和一份内存。
    @"
# 由 tools/scripts/local_infra.ps1 生成,勿手改(改了下次 up 会被覆盖)。
[mysqld]
basedir=$base
datadir=$data
port=$MysqlPort
bind-address=127.0.0.1
mysqlx=OFF
log-error=$errlog
skip-name-resolve
character-set-server=utf8mb4
collation-server=utf8mb4_0900_ai_ci
max_connections=500
sql_mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION
innodb_buffer_pool_size=512M
innodb_flush_log_at_trx_commit=2
"@ | Set-Content -LiteralPath (Get-MysqlIniPath) -Encoding utf8NoBOM
}

function Invoke-MysqlSql {
    <# 用免安装版自带的 mysql.exe 执行 SQL(替代 docker exec)。#>
    param(
        [Parameter(Mandatory)][string]$Sql,
        [string]$User = 'root',
        [string]$Password = '',
        [string]$Database = ''
    )
    $mysql = Find-Tool 'mysql' 'mysql.exe'
    if (-not $mysql) { Fail '找不到 mysql.exe,先跑 -Action provision。' }
    $cliArgs = @('--protocol=TCP', '--host=127.0.0.1', "--port=$MysqlPort", "--user=$User", '--default-character-set=utf8mb4', '--batch', '--silent')
    if ($Database) { $cliArgs += "--database=$Database" }
    # 密码走 MYSQL_PWD 环境变量,不进命令行(命令行密码会出现在进程列表里)。
    $old = $env:MYSQL_PWD
    try {
        $env:MYSQL_PWD = $Password
        $out = $Sql | & $mysql @cliArgs 2>&1
        if ($LASTEXITCODE -ne 0) { throw "mysql 执行失败: $($out -join "`n")" }
        return $out
    } finally { $env:MYSQL_PWD = $old }
}

function Get-MysqlBootstrapSqlPath { Join-Path $CfgDir 'mysql-bootstrap.sql' }

function New-MysqlBootstrapSql {
    <#
      建账号用 mysqld 的 --init-file,不用客户端连上去执行 —— 这是 MySQL 官方在 Windows 上
      重置 root 口令的标准做法,原因是它**不需要先能连上**:

        1. `--initialize-insecure` 只会建出 'root'@'localhost';
        2. 我们开了 skip-name-resolve(不做反解,账号 host 必须按字面匹配),
           于是从 TCP 127.0.0.1 连进来的客户端匹配不到 'localhost',
           MySQL 在**认证之前**就回 "Host '127.0.0.1' is not allowed to connect";
        3. 结果是「想建账号得先连上,想连上得先有账号」的死锁。

      更糟的是原来那版把建账号挂在「数据目录是不是刚建出来」这个一次性条件上:第一次失败,
      数据目录却已经建好了,之后每次启动都判定为「不是首次」直接跳过,账号永远补不回来
      —— 表现就是 mysqladmin / 迁移脚本 / 各服务全都连不上,而启动日志一片绿。

      现在改成**每次启动都跑**这个文件,全部语句幂等(IF NOT EXISTS + ALTER),
      账号被误删或口令被改了下次启动自动修回来,没有「一次性初始化」这种脆弱状态。

      注意 --init-file 的硬性格式要求(MySQL 手册):**一行一条语句,且不能写注释**。
    #>
    @"
ALTER USER 'root'@'localhost' IDENTIFIED BY '$MysqlRootPwd';
CREATE USER IF NOT EXISTS 'root'@'127.0.0.1' IDENTIFIED BY '$MysqlRootPwd';
ALTER USER 'root'@'127.0.0.1' IDENTIFIED BY '$MysqlRootPwd';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1' WITH GRANT OPTION;
CREATE USER IF NOT EXISTS '$MysqlUser'@'%' IDENTIFIED BY '$MysqlUserPwd';
ALTER USER '$MysqlUser'@'%' IDENTIFIED BY '$MysqlUserPwd';
CREATE USER IF NOT EXISTS '$MysqlUser'@'127.0.0.1' IDENTIFIED BY '$MysqlUserPwd';
ALTER USER '$MysqlUser'@'127.0.0.1' IDENTIFIED BY '$MysqlUserPwd';
"@ | Set-Content -LiteralPath (Get-MysqlBootstrapSqlPath) -Encoding utf8NoBOM
}

function Start-LocalMysql {
    param([switch]$DeferReady)
    $componentStartedAt = [Environment]::TickCount64
    $mysqld = Find-Tool 'mysql' 'mysqld.exe'
    if (-not $mysqld) { Fail '找不到 mysqld.exe,先跑 -Action provision。' }
    $baseDir = Split-Path -Parent (Split-Path -Parent $mysqld)   # <dist>/mysql/mysql-8.4.6-winx64
    $dataMysql = Join-Path $DataDir 'mysql'

    if (Test-PortOpen $MysqlPort) {
        $owned = Get-OwnedMysqlListenerProcess $MysqlPort
        if (-not $owned) {
            $holders = Get-PortHolder $MysqlPort
            $who = if ($holders.Count -gt 0) { $holders -join '; ' } else { '监听进程身份不可确认' }
            Fail "MySQL 端口 :$MysqlPort 已由非本项目实例占用($who)；不会复用、迁移或停止它。"
        }
        try {
            Invoke-MysqlSql -Sql 'SELECT 1;' -User $MysqlUser -Password $MysqlUserPwd | Out-Null
        } catch {
            Fail "本工作区 MySQL :$MysqlPort 在监听，但账号 '$MysqlUser' 探活失败；不会继续迁移:$($_.Exception.Message)"
        }
        # pid 文件可能被手工清掉；既然 listener/exe/my.ini 已精确验明归属，就恢复登记，
        # 否则本轮能复用、下一次 down 却停不掉它。
        Save-Pid 'mysql' $owned.Id
        Set-PandoraLocalInfraPortState -ProjectRoot $ProjectRoot -MysqlPort $MysqlPort `
            -MysqlProcessId $owned.Id -MysqlExecutable $owned.Path -MysqlDefaultsFile (Get-MysqlIniPath) | Out-Null
        Write-Ok "MySQL :$MysqlPort 已在运行(归属与账号已验证)"
        if ($DeferReady) {
            $existingState = New-PlannerInfraStartState -Name 'mysql' -Ports @($MysqlPort) `
                -Process $owned -TimeoutSeconds 90 -ListenerOwnerKind direct `
                -ExpectedExecutable $owned.Path -RequiredCommandLineTokens @(
                    (Get-MysqlIniPath), '--no-monitor') -Reused $true `
                -StartedAtMilliseconds $componentStartedAt
            Assert-PlannerInfraExistingState $existingState
        }
        return
    }

    New-MysqlIni -BaseDir $baseDir
    New-MysqlBootstrapSql

    $firstRun = -not (Test-Path -LiteralPath (Join-Path $dataMysql 'mysql'))
    if ($firstRun) {
        Write-Step "首次初始化 MySQL 数据目录(约 30s,只有第一次)"
        Remove-Item -LiteralPath $dataMysql -Recurse -Force -ErrorAction SilentlyContinue
        New-Item -ItemType Directory -Force -Path $dataMysql | Out-Null
        # --initialize-insecure:建出空密码的 root@localhost(不是过期密码),
        # 口令和 pandora 账号交给下面启动时的 --init-file 补齐。
        $p = Start-Process -FilePath $mysqld -ArgumentList "--defaults-file=`"$(Get-MysqlIniPath)`"", '--initialize-insecure' `
            -WindowStyle Hidden -PassThru -Wait
        if ($p.ExitCode -ne 0) {
            Write-Err "MySQL 初始化失败 (exit $($p.ExitCode))。"
            Show-ComponentFailure -Name 'mysql' -Port 0 -Proc $p
            exit 1
        }
    }

    Write-Step "启动 MySQL :$MysqlPort"
    # --no-monitor 必须加。Windows 上 mysqld 默认再 fork 一个「监控进程」,子进程一退出它就
    # **自动重新拉起**一个 mysqld。后果有两个,都实测踩过:
    #   1. Save-Pid 存的是监控进程(父),不是真正在服务的那个,pid 追踪从一开始就是错的;
    #   2. mysqladmin shutdown 关掉子进程后监控立刻补一个新的,mysqladmin 永远等不到
    #      「服务器消失」,于是挂在它自己默认的 shutdown_timeout=3600s 上 —— 表现就是
    #      「点停止之后整个脚本不动了」。日志里同时能看到两个实例抢 ibdata1 的报错。
    # 策划机是「一个脚本管生死」的模型,不需要 MySQL 自作主张重启;要重启由脚本负责。
    # --init-file:每次启动都幂等补齐账号,见 New-MysqlBootstrapSql 的说明。
    $bootstrapSql = (Get-MysqlBootstrapSqlPath) -replace '\\', '/'
    $startedAt = [Environment]::TickCount64
    $proc = Start-Process -FilePath $mysqld -ArgumentList `
        "--defaults-file=`"$(Get-MysqlIniPath)`"", '--no-monitor', "--init-file=`"$bootstrapSql`"" `
        -WindowStyle Hidden -PassThru
    Save-Pid 'mysql' $proc.Id
    if ($DeferReady) {
        return New-PlannerInfraStartState -Name 'mysql' -Ports @($MysqlPort) -Process $proc `
            -TimeoutSeconds 90 -ListenerOwnerKind direct -ExpectedExecutable $mysqld `
            -RequiredCommandLineTokens @((Get-MysqlIniPath), '--no-monitor') `
            -StartedAtMilliseconds $startedAt -TimingStartedAtMilliseconds $componentStartedAt
    }
    Wait-Port -Name 'mysql' -Port $MysqlPort -TimeoutSec 90 -Proc $proc

    # 端口开了不等于账号对。这里真连一次 —— 服务和迁移脚本用的就是这个账号,
    # 连不上就当场报错,不要让「基础设施一片绿、服务起来全连不上」再发生一次。
    try {
        Invoke-MysqlSql -Sql 'SELECT 1;' -User $MysqlUser -Password $MysqlUserPwd | Out-Null
    } catch {
        Write-Err "MySQL 起来了但账号 '$MysqlUser' 连不上:$($_.Exception.Message)"
        Show-ComponentFailure -Name 'mysql' -Port 0 -Proc $null
        exit 1
    }
    $owner = Get-OwnedMysqlListenerProcess $MysqlPort
    if (-not $owner -or $owner.Id -ne $proc.Id) {
        Fail "MySQL :$MysqlPort 虽可连接，但监听 PID 不等于本次启动的 $($proc.Id)；拒绝落端口状态。"
    }
    Set-PandoraLocalInfraPortState -ProjectRoot $ProjectRoot -MysqlPort $MysqlPort `
        -MysqlProcessId $owner.Id -MysqlExecutable $owner.Path -MysqlDefaultsFile (Get-MysqlIniPath) | Out-Null
    Write-Ok "MySQL :$MysqlPort(独立端口；不会占用 Docker 的 3307)"
}

# ===== Redis =====

function Start-LocalRedis {
    param([switch]$DeferReady)
    $componentStartedAt = [Environment]::TickCount64
    if (Test-PortOpen $RedisPort) {
        if ($DeferReady) {
            $existingExe = Find-Tool 'redis' 'redis-server.exe'
            $existingProc = Get-RunningProcess 'redis'
            if (-not $existingExe -or -not $existingProc) {
                Fail "Redis :$RedisPort 已监听，但缺少本工作区可验证的 exe/PID 登记；拒绝极速复用。"
            }
            $existingState = New-PlannerInfraStartState -Name 'redis' -Ports @($RedisPort) `
                -Process $existingProc -TimeoutSeconds 30 -ListenerOwnerKind direct `
                -ExpectedExecutable $existingExe -RequiredCommandLineTokens @(
                    '--port', "$RedisPort", (Join-Path $DataDir 'redis')) -Reused $true `
                -StartedAtMilliseconds $componentStartedAt
            Assert-PlannerInfraExistingState $existingState
            return
        }
        Write-Ok "Redis :$RedisPort 已在运行"
        return
    }
    $exe = Find-Tool 'redis' 'redis-server.exe'
    if (-not $exe) { Fail '找不到 redis-server.exe,先跑 -Action provision。' }
    $dataRedis = Join-Path $DataDir 'redis'
    New-Item -ItemType Directory -Force -Path $dataRedis | Out-Null

    Write-Step "启动 Redis :$RedisPort"
    # 参数逐条对齐 compose 的 redis command:appendonly yes / maxmemory 1gb /
    # maxmemory-policy noeviction(本实例承载会话权威,LRU 驱逐 = 静默放行旧会话)。
    $cliArgs = @(
        '--port', "$RedisPort"
        '--bind', '127.0.0.1'
        '--appendonly', 'yes'
        '--dir', $dataRedis
        '--maxmemory', '1gb'
        '--maxmemory-policy', 'noeviction'
        '--logfile', (Join-Path $LogDir 'redis.log')
    )
    $startedAt = [Environment]::TickCount64
    $proc = Start-Process -FilePath $exe -ArgumentList $cliArgs -WorkingDirectory (Split-Path -Parent $exe) `
        -WindowStyle Hidden -PassThru
    Save-Pid 'redis' $proc.Id
    if ($DeferReady) {
        return New-PlannerInfraStartState -Name 'redis' -Ports @($RedisPort) -Process $proc `
            -TimeoutSeconds 30 -ListenerOwnerKind direct -ExpectedExecutable $exe `
            -RequiredCommandLineTokens @('--port', "$RedisPort", $dataRedis) `
            -StartedAtMilliseconds $startedAt -TimingStartedAtMilliseconds $componentStartedAt
    }
    Wait-Port -Name 'redis' -Port $RedisPort -TimeoutSec 30 -Proc $proc
    Write-Ok "Redis :$RedisPort"
}

# ===== Kafka(KRaft 单节点,不需要 ZooKeeper)=====

function Get-KafkaHome {
    $bat = Find-Tool 'kafka' 'kafka-server-start.bat'
    if (-not $bat) { return $null }
    return (Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $bat)))  # <...>/kafka_2.13-3.9.1
}

function New-KafkaProperties {
    $logs = (Join-Path $DataDir 'kafka') -replace '\\', '/'
    New-Item -ItemType Directory -Force -Path (Join-Path $DataDir 'kafka') | Out-Null
    # 单节点 KRaft:一个进程同时当 broker 和 controller。分区数 / 副本因子对齐 compose
    # (KAFKA_NUM_PARTITIONS=4,单副本),auto-create 打开 —— pkg/kafkax 的 topic 名是硬编码常量,
    # 靠自动建 topic 免掉一步初始化。
    @"
# 由 tools/scripts/local_infra.ps1 生成,勿手改。
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:$KafkaCtrlPort
listeners=PLAINTEXT://127.0.0.1:$KafkaPort,CONTROLLER://127.0.0.1:$KafkaCtrlPort
advertised.listeners=PLAINTEXT://127.0.0.1:$KafkaPort
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$logs
num.partitions=4
default.replication.factor=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=true
group.initial.rebalance.delay.ms=0
# 策划机由停止脚本强制结束 JVM；默认 9 秒 broker lease 会让下一次 KRaft 注册反复收到
# DUPLICATE_BROKER_REGISTRATION。这里只服务本机单节点测试链，2 秒 lease + 500ms heartbeat
# 已用“同一数据目录强停后立即重启”对照验证；不进入 Docker/K8s/线上配置。
broker.session.timeout.ms=2000
broker.heartbeat.interval.ms=500
# 默认每 500ms 写一条空闲 metadata no-op，长期开着会无意义膨胀单机元数据日志。
metadata.max.idle.interval.ms=0
num.network.threads=3
num.io.threads=8
log.retention.hours=48
# Windows 不允许重命名本进程自己 mmap 着的索引文件。log cleaner 压缩 __consumer_offsets 时要把
# *.timeindex.cleaned 改名成 *.timeindex.swap，在 Windows 上必然拿到“文件被另一进程占用”；而 Kafka
# 把 log dir 的 IOException 当致命错误，单 log dir 直接“Shutdown broker because all log dirs have
# failed” —— broker 在启动后约 21 秒自杀，业务侧只看到 9093 拒连(2026-08-24 事故:matchmaker /
# matchmaker_pve / battle_result 三个强依赖 Kafka 的服务同时 exit 1)。脏比越过阈值后每次启动必复现。
# 关掉 cleaner 后 __consumer_offsets 不再压缩、只会缓慢变大，策划机用 -Action reset 清即可；保留期
# 删除路径(log.retention.hours)不走这条 rename，实测已连续删了多天没触发 log dir 失败。
# 只服务本机 127.0.0.1 单节点免 Docker 链;Docker/K8s/线上跑 Linux，不带这行。
log.cleaner.enable=false
"@ | Set-Content -LiteralPath (Join-Path $CfgDir 'kafka.properties') -Encoding utf8NoBOM
}

function Get-KafkaJavaArgs {
    <#
      直接用 java 起 Kafka,**不走 bin/windows/*.bat**。

      为什么:kafka-run-class.bat 会把 libs 下 120 个 jar 的**完整路径**逐个拼成 CLASSPATH,
      本机实测 12051 字符,远超 cmd.exe 的 8191 上限,于是 kafka-storage 直接报
      "The input line is too long." 而这个长度是「路径前缀 x 120」,随仓库放得多深而变 ——
      同一份代码在 D:\p 下能跑、在 F:\work\XuanMing-Server\... 下就炸,属于最难查的偶发失败,
      不能靠「让策划把仓库放浅一点」来赌。

      java 自己支持 `libs/*` 通配符(由 JVM 在启动时展开,不经过命令行),命令行长度从此
      与路径深度无关。JVM 参数照抄 kafka-run-class.bat 的默认值,只把堆调小。
    #>
    param(
        [Parameter(Mandatory)][string]$KafkaHome,
        [Parameter(Mandatory)][string]$Log4jName,
        [Parameter(Mandatory)][string[]]$HeapOpts
    )
    $l4j = (Join-Path $KafkaHome "config/$Log4jName") -replace '\\', '/'
    return @(
        $HeapOpts
        '-server'
        '-XX:+UseG1GC'
        '-XX:MaxGCPauseMillis=20'
        '-XX:InitiatingHeapOccupancyPercent=35'
        '-XX:+ExplicitGCInvokesConcurrent'
        '-Djava.awt.headless=true'
        "-Dkafka.logs.dir=$LogDir"
        "-Dlog4j.configuration=file:$l4j"
        '-cp'
        (Join-Path $KafkaHome 'libs/*')
    )
}

function ConvertTo-ProcArg {
    # Start-Process -ArgumentList 是按空格拼回一整行的,含空格的路径必须自己加引号
    # (策划机的仓库可能落在 "D:\我的文档\..." 这种带空格的目录下)。
    param([string]$Value)
    if ($Value -match '\s') { return "`"$Value`"" }
    return $Value
}

function Start-LocalKafka {
    param([switch]$DeferReady)
    $componentStartedAt = [Environment]::TickCount64
    if (Test-PortOpen $KafkaPort) {
        if ($DeferReady) {
            $existingJava = Find-Tool 'jre' 'java.exe'
            $existingProc = Get-RunningProcess 'kafka'
            if (-not $existingJava -or -not $existingProc) {
                Fail "Kafka :$KafkaPort 已监听，但缺少本工作区可验证的 JRE/PID 登记；拒绝极速复用。"
            }
            $existingState = New-PlannerInfraStartState -Name 'kafka' -Ports @($KafkaPort, $KafkaCtrlPort) `
                -Process $existingProc -TimeoutSeconds 120 -ListenerOwnerKind child `
                -ExpectedExecutable $existingJava -RequiredCommandLineTokens @(
                    'kafka.Kafka', (Join-Path $CfgDir 'kafka.properties')) -Reused $true `
                -StartedAtMilliseconds $componentStartedAt
            Assert-PlannerInfraExistingState $existingState
            return
        }
        Write-Ok "Kafka :$KafkaPort 已在运行"
        return
    }
    $home2 = Get-KafkaHome
    if (-not $home2) { Fail '找不到 kafka 发行包,先跑 -Action provision。' }
    $java = Find-Tool 'jre' 'java.exe'
    if (-not $java) { Fail '找不到自带 JRE,先跑 -Action provision。' }

    New-KafkaProperties
    $props = Join-Path $CfgDir 'kafka.properties'
    $meta = Join-Path $DataDir 'kafka/meta.properties'

    if (-not (Test-Path -LiteralPath $meta)) {
        Write-Step 'Kafka 首次格式化存储(KRaft)'
        $toolArgs = Get-KafkaJavaArgs -KafkaHome $home2 -Log4jName 'tools-log4j.properties' -HeapOpts @('-Xmx256M')
        $uuidArgs = $toolArgs + @('kafka.tools.StorageTool', 'random-uuid')
        $uuid = & $java @uuidArgs 2>&1
        if ($LASTEXITCODE -ne 0 -or -not $uuid) { Fail "kafka-storage random-uuid 失败: $($uuid -join "`n")" }
        $uuid = ($uuid | Where-Object { $_ -match '^[A-Za-z0-9_\-]{22}$' } | Select-Object -Last 1)
        if (-not $uuid) { Fail 'kafka-storage random-uuid 没有返回可用的 cluster id' }
        $fmtArgs = $toolArgs + @('kafka.tools.StorageTool', 'format', '-t', $uuid.Trim(), '-c', $props)
        $out = & $java @fmtArgs 2>&1
        if ($LASTEXITCODE -ne 0) { Fail "kafka-storage format 失败: $($out -join "`n")" }
    }

    Write-Step "启动 Kafka :$KafkaPort"
    $srvArgs = @(Get-KafkaJavaArgs -KafkaHome $home2 -Log4jName 'log4j.properties' -HeapOpts @('-Xmx512M', '-Xms256M')) +
    @('kafka.Kafka', $props)

    # 这里刻意**不用** Start-Process 的 -RedirectStandardOutput,改成 cmd /c 里做重定向。
    # 原因是 Windows 的进程创建机制:一旦用了 -Redirect*,PowerShell 会走
    # UseShellExecute=false + bInheritHandles=TRUE,于是**父进程所有可继承句柄**(包括
    # 调用方读取本脚本输出用的那根管道)都会被复制给这些常驻服务进程。结果是:本脚本
    # 早就退出了,调用方却永远等不到管道 EOF —— 表现为「基础设施明明起来了,一键启动脚本
    # 却卡住不往下走」。实测四个服务的 ppid 已不存在而父脚本仍挂着,就是这个原因。
    # 不带重定向参数时 Start-Process 默认 UseShellExecute=true,走 ShellExecuteEx,
    # 不传递任何句柄 —— mysql / redis / envoy 因此都保持无重定向(它们各自有日志文件)。
    # Kafka 需要留住 JVM 早期的 stdout/stderr(log4j 起来之前的失败只在那里可见),
    # 所以由 cmd 自己做重定向:重定向发生在 cmd 内部,与我们的句柄无关。
    $quoted = ($srvArgs | ForEach-Object { ConvertTo-ProcArg $_ }) -join ' '
    $logFile = Join-Path $LogDir 'kafka.log'
    $cmdLine = "`"$java`" $quoted > `"$logFile`" 2>&1"
    $startedAt = [Environment]::TickCount64
    $proc = Start-Process -FilePath 'cmd.exe' -ArgumentList '/c', "`"$cmdLine`"" `
        -WorkingDirectory $home2 -WindowStyle Hidden -PassThru
    Save-Pid 'kafka' $proc.Id
    if ($DeferReady) {
        return New-PlannerInfraStartState -Name 'kafka' -Ports @($KafkaPort, $KafkaCtrlPort) -Process $proc `
            -TimeoutSeconds 120 -ListenerOwnerKind child -ExpectedExecutable $java `
            -RequiredCommandLineTokens @('kafka.Kafka', $props) -StartedAtMilliseconds $startedAt `
            -TimingStartedAtMilliseconds $componentStartedAt
    }
    Wait-Port -Name 'kafka' -Port $KafkaPort -TimeoutSec 120 -Proc $proc
    Write-Ok "Kafka :$KafkaPort"
}

# ===== Envoy =====

function New-LocalEnvoyConfig {
    <#
      从 deploy/envoy/envoy.yaml **派生**出本机版(唯一权威仍是 deploy/envoy/envoy.yaml,
      不许在仓库里再放第二份 yaml —— 两份必然漂移)。派生只做四件事:
        1. host.docker.internal -> 127.0.0.1(容器里才需要那个特殊域名)
        2. /etc/envoy/*.pem     -> deploy/envoy/ 下的真实路径
        3. 监听地址按 PANDORA_*_BIND_HOST 收敛(compose 是靠端口映射限制的,原生进程得自己限)
        4. 剔除 $EnvoyDropFields 里登记的、1.28 不认识的字段
      然后 --mode validate 卡关:任何白名单以外的报错一律硬失败。
    #>
    $srcPath = Join-Path $ProjectRoot 'deploy/envoy/envoy.yaml'
    if (-not (Test-Path -LiteralPath $srcPath)) { Fail "找不到 $srcPath" }

    $certPath = (Join-Path $ProjectRoot 'deploy/envoy/cert.pem') -replace '\\', '/'
    $keyPath = (Join-Path $ProjectRoot 'deploy/envoy/key.pem') -replace '\\', '/'
    foreach ($p in @($certPath, $keyPath)) {
        if (-not (Test-Path -LiteralPath $p)) { Fail "缺少 Envoy 证书 $p(应由 Confirm-EnvoyDevCert 自动签发,检查 mkcert 是否可用)。" }
    }

    # 客户端面默认 127.0.0.1;start.ps1 的 local 模式会导出 0.0.0.0 让手机 / 同事连。
    # DS 面 8444 没有玩家鉴权,默认恒绑本机(与 compose 的 PANDORA_DS_EDGE_BIND_HOST 语义一致)。
    $edgeHost = if ($env:PANDORA_EDGE_BIND_HOST) { $env:PANDORA_EDGE_BIND_HOST } else { '127.0.0.1' }
    $dsEdgeHost = if ($env:PANDORA_DS_EDGE_BIND_HOST) { $env:PANDORA_DS_EDGE_BIND_HOST } else { '127.0.0.1' }

    $lines = Get-Content -LiteralPath $srcPath
    $out = New-Object System.Collections.Generic.List[string]
    $dropped = @{}
    # 监听器区分:admin / pandora_listener / pandora_ds_listener 的 address 各自替换。
    # 状态机只吃「离我最近的一个 name/admin 标记」,yaml 里三块的 address 各只出现一次。
    $pending = 'admin'
    foreach ($line in $lines) {
        if ($line -match '^\s*-?\s*name:\s*pandora_listener\s*$') { $pending = 'edge' }
        elseif ($line -match '^\s*-?\s*name:\s*pandora_ds_listener\s*$') { $pending = 'ds' }

        $m = [regex]::Match($line, '^(?<i>\s*)address:\s*0\.0\.0\.0\s*$')
        if ($m.Success -and $pending) {
            $repl = switch ($pending) {
                'admin' { '127.0.0.1' }   # compose 里 9901 没有映射到宿主,原生进程必须自己收敛
                'edge' { $edgeHost }
                'ds' { $dsEdgeHost }
            }
            $out.Add("$($m.Groups['i'].Value)address: $repl")
            $pending = $null
            continue
        }

        # 1.28 不认识的字段:只剔白名单里的,别的一律留着让 validate 去炸。
        $isDropped = $false
        foreach ($f in $EnvoyDropFields.Keys) {
            if ($line -match "^\s*$([regex]::Escape($f))\s*:") {
                $dropped[$f] = $true
                $isDropped = $true
                break
            }
        }
        if ($isDropped) { continue }

        $line = $line -replace 'host\.docker\.internal', '127.0.0.1'
        $line = $line -replace '/etc/envoy/cert\.pem', $certPath
        $line = $line -replace '/etc/envoy/key\.pem', $keyPath
        $out.Add($line)
    }

    $dst = Join-Path $CfgDir 'envoy.yaml'
    $header = @(
        '# 【自动生成,勿手改】由 tools/scripts/local_infra.ps1 从 deploy/envoy/envoy.yaml 派生。'
        '# 唯一权威是 deploy/envoy/envoy.yaml;要改路由 / 鉴权请改那一份,本文件每次启动重新生成。'
        "# 本机 Envoy 版本 $EnvoyImageTag(官方最后一版 Windows 构建),剔除的字段见下:"
    )
    foreach ($f in $EnvoyDropFields.Keys) {
        $mark = if ($dropped.ContainsKey($f)) { '已剔除' } else { '未出现' }
        $header += "#   - $f  [$mark]  $($EnvoyDropFields[$f])"
    }
    Set-Content -LiteralPath $dst -Value (($header + $out) -join "`n") -Encoding utf8NoBOM
    return $dst
}

function Get-EnvoyFingerprint([string]$CfgPath) {
    <#
      指纹 = 派生配置 + 证书 + 私钥。这三样里任何一样变了,当前跑着的 Envoy 就是过期的,
      必须重启才生效;三样都没变,则它跑的就是同一份配置,没有任何重启的理由。
    #>
    $parts = foreach ($p in @($CfgPath,
        (Join-Path $ProjectRoot 'deploy/envoy/cert.pem'),
        (Join-Path $ProjectRoot 'deploy/envoy/key.pem'))) {
        if (Test-Path -LiteralPath $p) { (Get-FileHash -LiteralPath $p -Algorithm SHA256).Hash } else { 'missing' }
    }
    return ($parts -join ':')
}

function Start-LocalEnvoy {
    param([switch]$DeferReady)
    $componentStartedAt = [Environment]::TickCount64
    $exe = Join-Path $DistDir 'envoy/envoy.exe'
    if (-not (Test-Path -LiteralPath $exe)) { Fail ' 找不到 envoy.exe,先跑 -Action provision。' }

    # 证书自愈:与 docker 模式共用 deploy/envoy/cert.pem —— 复用 dev_up.ps1 的同一套函数,
    # 不另写一份签发逻辑(签发规则、SAN 列表、共享 dev CA 只能有一处权威)。
    . "$PSScriptRoot/envoy_cert.ps1"
    Confirm-SharedDevCa -ProjectRoot $ProjectRoot | Out-Null
    Confirm-EnvoyDevCert -EnvoyDir (Join-Path $ProjectRoot 'deploy/envoy')

    # 先派生再决定要不要重启(派生是纯函数,只写文件,不碰进程)。
    # 原来这里是无条件 Stop-Component 'envoy',理由写的是「配置每次都重生成所以必须重启」——
    # 但派生是**确定性**的:deploy/envoy/envoy.yaml 和证书没变时,派生结果逐字节相同,
    # 重启就纯粹是白白踢掉所有在连的客户端和 DS。而策划的日常是「基础设施开着不动,
    # 反复重启 go / DS」,这条无条件重启正好破坏了那个前提。
    $cfg = New-LocalEnvoyConfig
    $fpFile = Join-Path $CfgDir 'envoy.fingerprint'
    $fp = Get-EnvoyFingerprint $cfg
    $oldFp = if (Test-Path -LiteralPath $fpFile) { (Get-Content -LiteralPath $fpFile -Raw).Trim() } else { '' }

    $existingProc = Get-RunningProcess 'envoy'
    if ($fp -eq $oldFp -and $existingProc -and (Test-PortOpen 8443) -and (Test-PortOpen 8444)) {
        if ($DeferReady) {
            $existingState = New-PlannerInfraStartState -Name 'envoy' -Ports @(8443, 8444) `
                -Process $existingProc -TimeoutSeconds 30 -ListenerOwnerKind direct `
                -ExpectedExecutable $exe -RequiredCommandLineTokens @($cfg) `
                -CompletionData ([pscustomobject]@{ FingerprintFile = $fpFile; Fingerprint = $fp }) -Reused $true `
                -StartedAtMilliseconds $componentStartedAt
            Assert-PlannerInfraExistingState $existingState
            return
        }
        Write-Ok 'Envoy :8443 / :8444 已在运行(配置与证书均未变化,不重启)'
        return
    }

    $null = Stop-Component 'envoy'
    Stop-OrphanPortHolder 'envoy' @(8443, 8444)

    # Envoy(libevent)在 TMP 目录里造 AF_UNIX socketpair,而本机安全软件只拦用户 TEMP 树。
    # 先挑一个 AF_UNIX 真能用的目录再动 envoy —— 校验和正式启动必须用**同一个**,否则会出现
    # 「校验过了、起的时候崩」这种最难查的分裂。
    $envoyTmp = Resolve-EnvoyTempDir
    $tmpSaved = @{ TMP = $env:TMP; TEMP = $env:TEMP }
    if ($envoyTmp) {
        if ($envoyTmp -ne $tmpSaved.TEMP) { Write-Warn2 "Envoy 临时目录改用 $envoyTmp(默认 TEMP 下 AF_UNIX 造 socketpair 不可用)" }
        $env:TMP = $envoyTmp; $env:TEMP = $envoyTmp
    }
    try {

    Write-Step ' 校验派生的 Envoy 配置'
    $val = & $exe --mode validate -c $cfg 2>&1
    if ($LASTEXITCODE -ne 0) {
        $text = $val -join "`n"

        # Envoy 连自己的事件循环都没建起来时,压根没走到读配置那一步 —— 这不是配置问题。
        # 不分流的话这里会报「配置校验不通过」,再把人指向 $EnvoyDropFields 去 envoy.yaml 里
        # 找不存在的未知字段,方向完全是反的。libevent 在 Windows 上优先用 AF_UNIX 造
        # socketpair,而本机安全软件(360 主动防御 ZhuDongFangYu 等)会拦 AF_UNIX 的 connect,
        # 于是 evsig_init_ 拿不到 event_base 直接 assert 崩掉。
        if ($text -match 'evsig_init_|Failed to initialize libevent event_base') {
            Write-Err 'Envoy 起不来:libevent 事件循环初始化失败(不是配置问题,配置根本没被读到)。'
            # 不猜,现场探一次 —— 探针失败即坐实是 AF_UNIX 被拦,而不是「大概是杀软吧」。
            $afunix = Test-AfUnixUsable
            if ($afunix) {
                Write-Err "本机 AF_UNIX(Unix 域套接字)不可用:$afunix"
                $guards = @(Get-Process -ErrorAction SilentlyContinue |
                    Where-Object { $_.ProcessName -match 'ZhuDongFangYu|360|QHSafe' } |
                    ForEach-Object { "$($_.ProcessName)(pid $($_.Id))" } | Sort-Object -Unique)
                if ($guards.Count -gt 0) { Write-Err "在跑的安全软件:$($guards -join ', ')" }
                Write-Host @"
      Envoy 的 libevent 在 Windows 上用 AF_UNIX 造 socketpair 做信号唤醒,这一步不通就直接崩。
      本脚本已经自动换过临时目录了(见 Resolve-EnvoyTempDir),走到这里说明**候选目录全都不通**,
      是机器级的问题,不是选错目录。
      排查方向(按可能性):
        1. 安全软件的内核过滤驱动。本机常见的是 360(`fltmc` 看 360AntiSteal/360FsFlt/360Box64)。
           注意:2026-07-28 本机实测过,加白名单 / 在界面上「关闭」都不卸载内核驱动,基本无效;
           真要验证是不是它,只能卸载后复测 —— 这一步得人来做,脚本不越权代劳。
        2. 工作区路径太深:AF_UNIX 的 sun_path 上限约 108 字节,超了必失败,跟杀软无关。
           把仓库挪到浅一点的路径即可。
"@ -ForegroundColor Yellow
            } else {
                # 探针能过却仍崩:别把责任推给杀软,原样把现场交出去。
                Write-Warn2 'AF_UNIX 探针本身是通的 —— 不是已知的杀软拦截,按下面的原始输出查。'
            }
            Write-Host $text -ForegroundColor DarkGray
            exit 1
        }

        Write-Err '派生配置在本机 Envoy 上校验不通过 —— 拒绝启动(fail-closed)。'
        $unknown = [regex]::Match($text, "no such field: '(?<f>[^']+)'")
        if ($unknown.Success) {
            Write-Err "未知字段: $($unknown.Groups['f'].Value)"
            Write-Host @"
      deploy/envoy/envoy.yaml 用到了本机 Envoy $EnvoyImageTag 不支持的新字段。
      **不要**顺手把它加进 `$EnvoyDropFields 就完事 —— 先确认剔掉它之后本机的鉴权 / 路由
      不会比线上更宽松;确认无害再登记进白名单并写清退化说明,否则策划机会跑在一套
      看起来正常、实际少了一道门的配置上。
"@ -ForegroundColor Yellow
        } else {
            Write-Host $text -ForegroundColor DarkGray
        }
        exit 1
    }
    Write-Ok '配置校验通过'

    Write-Step " 启动 Envoy :8443(客户端面) / :8444(DS 面)"
    $startedAt = [Environment]::TickCount64
    $proc = Start-Process -FilePath $exe `
        -ArgumentList '-c', "`"$cfg`"", '--log-level', 'warn', '--log-path', "`"$(Join-Path $LogDir 'envoy.log')`"" `
        -WorkingDirectory $CfgDir -WindowStyle Hidden -PassThru
    Save-Pid 'envoy' $proc.Id
    if ($DeferReady) {
        return New-PlannerInfraStartState -Name 'envoy' -Ports @(8443, 8444) -Process $proc `
            -TimeoutSeconds 30 -ListenerOwnerKind direct -ExpectedExecutable $exe `
            -RequiredCommandLineTokens @($cfg) -CompletionData ([pscustomobject]@{
                FingerprintFile = $fpFile
                Fingerprint = $fp
            }) -StartedAtMilliseconds $startedAt -TimingStartedAtMilliseconds $componentStartedAt
    }
    Wait-Port -Name 'envoy' -Port 8443 -TimeoutSec 30 -Proc $proc

    } finally {
        # 只是给 envoy 子进程挑目录,别把本脚本后续步骤(以及被它拉起的 go 服务)也带偏。
        $env:TMP = $tmpSaved.TMP; $env:TEMP = $tmpSaved.TEMP
    }
    # 指纹在**启动成功之后**才落盘:启动失败时不留指纹,下次必然重来一遍,
    # 不会出现「指纹说没变、其实上次根本没起来」的情况。
    Set-Content -LiteralPath $fpFile -Value $fp -Encoding ascii
    Write-Ok 'Envoy :8443 / :8444'
}

# ===== 动作 =====

function Add-PlannerInfraStateTiming($State, [string]$Status, [string]$Detail = '') {
    $displayName = switch ([string]$State.Name) {
        'mysql' { 'MySQL' }
        'redis' { 'Redis' }
        'kafka' { 'Kafka' }
        'envoy' { 'Envoy' }
        default { [string]$State.Name }
    }
    $finishedAt = if ($State.PSObject.Properties['ComponentFinishedAtMilliseconds'] -and
        [int64]$State.ComponentFinishedAtMilliseconds -gt 0) {
        [int64]$State.ComponentFinishedAtMilliseconds
    } elseif ([int64]$State.FinishedAtMilliseconds -gt 0) {
        [int64]$State.FinishedAtMilliseconds
    } elseif ($Status -ne '失败' -and [int64]$State.ReadyAtMilliseconds -gt 0) {
        [int64]$State.ReadyAtMilliseconds
    } else {
        [Environment]::TickCount64
    }
    Add-PandoraPlannerTiming -Name "基础设施·$displayName" `
        -ElapsedMilliseconds ([Math]::Max([int64]0, $finishedAt - [int64]$State.TimingStartedAtMilliseconds)) `
        -Status $Status -Detail $Detail
}

function Complete-PlannerInfraReadyState($State) {
    if ([bool]$State.CompletionHandled) { return }
    $status = '失败'
    $detail = '组件就绪后的协议探活/收尾失败'
    try {
        Complete-PlannerInfraStartState $State
        # 组件明细到协议探活/自身收尾为止。随后 callback 可能同步跑 MySQL migration，
        # 那是独立阶段；仍由 callback 的成功/失败决定整批是否可继续。
        $State.ComponentFinishedAtMilliseconds = [Environment]::TickCount64
        if ($null -ne $OnPlannerComponentReady) {
            $null = & $OnPlannerComponentReady $State
        }
        $status = if ([bool]$State.Reused) { '复用' } else { '完成' }
        $detail = if ([bool]$State.Reused) { '已在运行' } else { '' }
    } catch {
        $detail = "$detail`:$($_.Exception.Message)"
        throw
    } finally {
        # callback 是就绪边的一部分：只有协议探活和上层触发都完成，组件才真正可供后续依赖使用。
        $State.FinishedAtMilliseconds = [Environment]::TickCount64
        $State.CompletionHandled = $true
        Add-PlannerInfraStateTiming -State $State -Status $status -Detail $detail
    }
}

function Invoke-PlannerExternalComponentReady([string]$Name) {
    if ($null -eq $OnPlannerComponentReady) { return }
    $null = & $OnPlannerComponentReady ([pscustomobject]@{
            Name = $Name; Reused = $false; CompletionHandled = $true
        })
}

function Invoke-PlannerInfraFastStart {
    $batchStartedAt = [Environment]::TickCount64
    $launchers = [Collections.Generic.List[scriptblock]]::new()
    if (-not $CentralMysqlManaged) { $launchers.Add({ Start-LocalMysql -DeferReady }) }
    $launchers.Add({ Start-LocalRedis -DeferReady })
    $launchers.Add({ Start-LocalKafka -DeferReady })
    $launchers.Add({ Start-LocalEnvoy -DeferReady })

    $states = @(Invoke-PandoraPlannerInfraBatch -Launchers $launchers.ToArray() `
        -GetListenerRecords { Get-PandoraTcpListenerRecords } `
        -TestProcessExited {
            param($State)
            try { return [bool]$State.Process.HasExited } catch { return $true }
        } -TestStateReady {
            param($State, [object[]]$Listeners)
            return Test-PlannerInfraStateReady -State $State -Listeners $Listeners
        } -OnFailure {
            param($State, [string]$Reason)
            if (-not $State.PSObject.Properties['CompletionHandled'] -or -not [bool]$State.CompletionHandled) {
                Add-PlannerInfraStateTiming -State $State -Status '失败' -Detail $Reason
            }
            if ($Reason -eq 'process-exited') {
                $exitCode = try { $State.Process.ExitCode } catch { '?' }
                Write-Err "$($State.Name) 启动后立即退出 (exit $exitCode)。"
            } elseif ($Reason -eq 'ready-callback-failed') {
                $callbackMessage = if ($State.PSObject.Properties['FailureException']) {
                    $State.FailureException.Exception.Message
                } else { '未知错误' }
                Write-Err "$($State.Name) listener 就绪后的协议探活/依赖触发失败:$callbackMessage"
            } else {
                Write-Err "$($State.Name) 在 $([int]($State.TimeoutMilliseconds / 1000))s 内没有完成精确 listener 归属验证:$($State.Ports -join ',')。"
            }
            Show-ComponentFailure -Name $State.Name -Port ([int]$State.Ports[0]) -Proc $State.Process
        } -Sleep {
            param([int]$Milliseconds)
            Start-Sleep -Milliseconds $Milliseconds
        } -GetElapsedMilliseconds { return [int64][Environment]::TickCount64 } `
        -OnReady { param($State) Complete-PlannerInfraReadyState $State } `
        -StopOnFirstFailure -PollMilliseconds 100)

    $infraFailed = $false
    foreach ($state in $states) {
        if ($state.Failure) {
            $infraFailed = $true
            $failureDetail = if ($state.Failure -eq 'batch-aborted') {
                '同批其他组件失败，停止等待'
            } elseif ($state.Failure -eq 'ready-callback-failed' -and $state.PSObject.Properties['FailureException']) {
                "协议探活/依赖触发失败:$($state.FailureException.Exception.Message)"
            } else { [string]$state.Failure }
            if (-not [bool]$state.CompletionHandled) {
                Add-PlannerInfraStateTiming -State $state -Status '失败' -Detail $failureDetail
            }
            continue
        }
        if (-not [bool]$state.CompletionHandled) {
            try {
                Complete-PlannerInfraReadyState $state
            } catch {
                $infraFailed = $true
            }
        }
    }
    if ($infraFailed) { exit 1 }
    Write-Ok ("策划极速基础设施批量就绪:{0:N2}s" -f (([Environment]::TickCount64 - $batchStartedAt) / 1000.0))
}

function Invoke-Up {
    if ($CentralMysqlManaged) {
        # 先验证 bundle 形态；存在但损坏时必须在任何本机 MySQL 动作之前 fail-closed。
        Get-PandoraPlannerCentralMysqlConfig -ConfigPath (Get-PandoraPlannerCentralMysqlBundlePath -ProjectRoot $ProjectRoot) | Out-Null
    }
    # receipt miss 必须让本轮完整走一次旧串行路径；不能由本轮 provision 刚写出的 receipt
    # 反过来把首次安装/修复误判为“已安装、已初始化”的并发场景。
    $receiptReadyBeforeProvision = Test-PlannerPackageSetReady
    Invoke-PandoraPlannerTimedStep -Name '依赖准备' -Action { Invoke-ProvisionAll }
    if (-not $CentralMysqlManaged) {
        $script:MysqlPort = Resolve-LocalMysqlPort
        Write-Ok "免 Docker MySQL 选用独立端口 :$MysqlPort(Docker dev 的 :3307 保持原样)"
    } else {
        Write-Ok 'MySQL 使用中心受管 profile；本机不下载、不解包、不启动 mysqld'
    }
    $plannerBatchEligible = Test-PandoraPlannerInfraBatchEligibility `
        -PlannerFastStart $PlannerFastStart -Force ([bool]$Force) `
        -ReceiptReady $receiptReadyBeforeProvision -CentralMysqlManaged $CentralMysqlManaged `
        -MysqlInitialized ([IO.Directory]::Exists((Join-Path $DataDir 'mysql/mysql'))) `
        -KafkaInitialized ([IO.File]::Exists((Join-Path $DataDir 'kafka/meta.properties')))
    if ($plannerBatchEligible) {
        Set-PandoraPlannerInfraTimingMode -Mode parallel
        Invoke-PlannerInfraFastStart
    } else {
        Set-PandoraPlannerInfraTimingMode -Mode serial
        if (-not $CentralMysqlManaged) {
            Invoke-PandoraPlannerTimedStep -Name '基础设施·MySQL' -Action { Start-LocalMysql }
            Invoke-PlannerExternalComponentReady 'mysql'
        }
        Invoke-PandoraPlannerTimedStep -Name '基础设施·Redis' -Action { Start-LocalRedis }
        Invoke-PlannerExternalComponentReady 'redis'
        Invoke-PandoraPlannerTimedStep -Name '基础设施·Kafka' -Action { Start-LocalKafka }
        Invoke-PlannerExternalComponentReady 'kafka'
        Invoke-PandoraPlannerTimedStep -Name '基础设施·Envoy' -Action { Start-LocalEnvoy }
        Invoke-PlannerExternalComponentReady 'envoy'
    }
    Write-Host ''
    Write-Host '  本机基础设施已就绪(免 Docker)' -ForegroundColor Green
    # fast coordinator 已用 exact listener PID/exe/参数 + 协议探活验过全部组件；
    # 立刻再跑一遍 status 只会重复 netstat/CIM/TCP，不增加任何新证据。
    if (-not $PlannerFastStart) { Invoke-Status }
}

function Invoke-Down {
    $failed = @()
    foreach ($n in @($LocalInfraLifecyclePlan.StopComponents)) {
        if (-not (Stop-Component $n)) { $failed += $n }
    }
    if ($failed.Count -gt 0) {
        Write-Err "本机基础设施未能全部停止:$($failed -join ', ')。数据与 PID 登记均保留，不会继续 reset。"
        return $false
    }
    Write-Ok '本机基础设施已停止(数据保留)'
    return $true
}

function Invoke-Status {
    $rows = @(
        @{ Name = 'redis'; Port = $RedisPort }
        @{ Name = 'kafka'; Port = $KafkaPort }
        @{ Name = 'envoy'; Port = 8443 }
        @{ Name = 'envoy-ds'; Port = 8444 }
    )
    Write-Host ''
    Write-Host '  组件      端口   状态' -ForegroundColor Gray
    if ($CentralMysqlManaged) {
        try {
            $centralConfig = Get-PandoraPlannerCentralMysqlConfig -ConfigPath (Get-PandoraPlannerCentralMysqlBundlePath -ProjectRoot $ProjectRoot)
            $centralUp = Test-TcpEndpoint -ComputerName $centralConfig.endpoint.host -Port ([int]$centralConfig.endpoint.port)
            $centralState = if ($centralUp) { 'EXTERNAL-UP' } else { 'EXTERNAL-DOWN' }
            $centralColor = if ($centralUp) { 'Green' } else { 'Red' }
            Write-Host ("  {0,-9} {1,-6} {2}" -f 'mysql', $centralConfig.endpoint.port, $centralState) -ForegroundColor $centralColor
        } catch {
            Write-Host ("  {0,-9} {1,-6} {2}" -f 'mysql', '-', 'CENTRAL-CONFIG-INVALID') -ForegroundColor Red
        }
    } elseif ($MysqlPort -le 0) {
        Write-Host ("  {0,-9} {1,-6} {2}" -f 'mysql', '-', 'UNCONFIGURED') -ForegroundColor DarkGray
    } else {
        $ownedMysql = Get-OwnedMysqlListenerProcess $MysqlPort
        $mysqlState = if ($ownedMysql) { 'OWNED' } elseif (Test-PortOpen $MysqlPort) { 'FOREIGN' } else { 'DOWN' }
        $mysqlColor = if ($mysqlState -eq 'OWNED') { 'Green' } elseif ($mysqlState -eq 'FOREIGN') { 'Yellow' } else { 'Red' }
        Write-Host ("  {0,-9} {1,-6} {2}" -f 'mysql', $MysqlPort, $mysqlState) -ForegroundColor $mysqlColor
    }
    foreach ($r in $rows) {
        $ok = Test-PortOpen $r.Port
        $txt = if ($ok) { 'UP  ' } else { 'DOWN' }
        $color = if ($ok) { 'Green' } else { 'Red' }
        Write-Host ("  {0,-9} {1,-6} {2}" -f $r.Name, $r.Port, $txt) -ForegroundColor $color
    }
    Write-Host ''
}

function Test-MysqlDataDirUnlocked {
    if (-not (Test-Path -LiteralPath $DataDir -PathType Container)) { return $true }
    # PID/CIM 都可能因权限或陈旧登记而不可见；reset 再用 MySQL 长期开启的核心文件做最后一道
    # 独占打开探针。任何一个仍被占用或无法确认时都 fail closed，不冒险递归删 data。
    $probes = @(
        (Join-Path $DataDir 'mysql/ibdata1'),
        (Join-Path $DataDir 'mysql/undo_001'),
        (Join-Path $DataDir 'mysql/undo_002')
    )
    $redoDir = Join-Path $DataDir 'mysql/#innodb_redo'
    if (Test-Path -LiteralPath $redoDir -PathType Container) {
        $probes += @(Get-ChildItem -LiteralPath $redoDir -File -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty FullName)
    }
    foreach ($path in @($probes | Where-Object { $_ -and (Test-Path -LiteralPath $_ -PathType Leaf) })) {
        $stream = $null
        try {
            $stream = [IO.File]::Open($path, [IO.FileMode]::Open, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
        } catch {
            Write-Err "MySQL 数据文件仍被占用或无法独占验证:$path；拒绝 reset 删除数据。"
            return $false
        } finally {
            if ($stream) { $stream.Dispose() }
        }
    }
    return $true
}

function Invoke-Reset {
    if (-not (Invoke-Down)) { return $false }
    if (-not $CentralMysqlManaged) {
        $ownedMysql = @(Get-OwnedMysqlProcesses)
        if ($ownedMysql.Count -gt 0) {
            Write-Err "仍检测到属于本工作区的 mysqld PID:$((@($ownedMysql | ForEach-Object { $_.Id })) -join ', ')；拒绝删除仍可能被使用的数据目录。"
            return $false
        }
        if (-not (Test-MysqlDataDirUnlocked)) { return $false }
        Write-Step '删除本机基础设施数据目录'
        if (Test-Path -LiteralPath $DataDir) {
            try {
                Remove-Item -LiteralPath $DataDir -Recurse -Force -ErrorAction Stop
            } catch {
                Write-Err "删除本机基础设施数据目录失败:$($_.Exception.Message)"
                return $false
            }
        }
        if (Test-Path -LiteralPath $DataDir) {
            Write-Err "数据目录仍存在，reset 未完成:$DataDir"
            return $false
        }
        Write-Ok "已删除 $DataDir(下次 up 会重新初始化)"
        return $true
    }
    Write-Step '删除 Redis/Kafka 本机数据（中心 MySQL 与本机 MySQL 目录均不触碰）'
    foreach ($component in @($LocalInfraLifecyclePlan.ResetComponents)) {
        $componentData = Join-Path $DataDir $component
        if (Test-Path -LiteralPath $componentData) {
            try { Remove-Item -LiteralPath $componentData -Recurse -Force -ErrorAction Stop }
            catch { Write-Err "删除 $componentData 失败:$($_.Exception.Message)"; return $false }
        }
    }
    Write-Ok '已重置 Redis/Kafka；中心 MySQL 与本机 MySQL 数据保持原样'
    return $true
}

New-Item -ItemType Directory -Force -Path $Root, $LogDir, $PidDir, $CfgDir | Out-Null
# 已备料过就先挂 PATH:down / status 这类不走 provision 的动作也能用上自带工具。
Register-LocalToolPath
$lifecycleLock = $null
$orchestrationLockEntered = $false
try {
    if ($Action -in @('up', 'down', 'reset', 'provision')) {
        Enter-PandoraOrchestrationLock -ProjectRoot $ProjectRoot -Operation "免 Docker 基础设施 $Action"
        $orchestrationLockEntered = $true
        $lockPath = Join-Path $Root 'lifecycle.lock'
        try {
            $lifecycleLock = [IO.File]::Open($lockPath, [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
        } catch {
            Fail "另一轮免 Docker 基础设施生命周期操作正在进行；本轮 $Action 不会并发执行。"
        }
    }

    $knownMysqlPort = if ($CentralMysqlManaged) { 0 } else { Get-PandoraLocalMysqlPort $ProjectRoot }
    if (-not $CentralMysqlManaged -and $knownMysqlPort -gt 0) {
        $script:MysqlPort = $knownMysqlPort
    } elseif (-not $CentralMysqlManaged -and $Action -in @('down', 'status', 'reset')) {
        # 兼容没有 ports.json / pid 文件的运行实例；先扫整个私有池和旧版 3307，
        # 只有 exe + 本工作区 my.ini 都吻合才认，外部 listener 一律不碰。
        $knownOwned = Get-OnlyOwnedMysqlListener (@(3307) + @($MysqlPortMin..$MysqlPortMax))
        if ($knownOwned) {
            $script:MysqlPort = [int]$knownOwned.Port
        } else {
            # 上次通过环境变量选过池外端口、随后 ports.json 丢失时，不能只扫固定池。
            # 先按 exe + exact my.ini 找唯一进程，再从该 PID 的 listener 恢复真实端口。
            $knownProcess = Get-OnlyOwnedMysqlProcess
            if ($knownProcess) {
                $records = @(Get-MysqlListenerRecordsForProcess $knownProcess)
                if ($records.Count -gt 1) {
                    Fail "本工作区 mysqld PID $($knownProcess.Id) 同时监听多个端口:$((@($records | ForEach-Object { $_.Port })) -join ', ')；拒绝猜测状态/停机目标。"
                }
                if ($records.Count -eq 1) { $script:MysqlPort = [int]$records[0].Port }
            }
        }
    }

    switch ($Action) {
        'up' { Invoke-Up }
        'down' { if (-not (Invoke-Down)) { exit 1 } }
        # status 也显式返回退出码，供 start.ps1 这类父脚本可靠汇总；否则成功路径可能
        # 继承调用者残留的 $LASTEXITCODE，失败路径又可能被后续 status 子命令覆盖。
        'status' { Invoke-Status; exit 0 }
        # provision 比 up 多备一份 PowerShell 7 免安装包:它只服务于「本机连 pwsh 都没有」的
        # 策划机(由 bootstrap_pwsh.cmd 在 cmd.exe 里自举),能跑到 up 的机器用不上。
        'provision' { Invoke-ProvisionAll; Save-PwshBootstrapArchive }
        'reset' { if (-not (Invoke-Reset)) { exit 1 } }
    }
} finally {
    if ($lifecycleLock) { $lifecycleLock.Dispose() }
    if ($orchestrationLockEntered) { Exit-PandoraOrchestrationLock }
}
