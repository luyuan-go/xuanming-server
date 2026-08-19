#requires -Version 7.0
<#
.SYNOPSIS
    策划机免 Docker 本机基础设施(MySQL / Redis / Kafka / Envoy)。

.DESCRIPTION
    背景:策划机不装 Docker Desktop(装不动、要 WSL2、开机慢、IT 不给管理员),
    但本机跑一整套后端需要 MySQL / Redis / Kafka / Envoy 四件套。本脚本用**免安装**
    的原生 Windows 二进制把这四件套跑成宿主进程,端口 / 账号 / 配置与
    deploy/docker-compose.dev.yml 完全一致,因此 services/*/etc/*-dev.yaml 一个字都不用改。

    与 docker 模式的对应关系(端口 / 账号必须一致,否则服务配置就得分叉):
      MySQL  127.0.0.1:3307   root/pandora_dev_root   pandora/pandora_dev_pwd
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
    离线 / 内网分发:设置 PANDORA_LOCALINFRA_MIRROR 指向一个已放好压缩包的目录
    (本地目录或 UNC 共享),脚本会优先从那里拷,不走公网。文件名见 $Components 的 File 字段。
#>

[CmdletBinding()]
param(
    [ValidateSet('up', 'down', 'status', 'provision', 'reset')]
    [string]$Action = 'up',

    [switch]$Force
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

# ===== 与 docker-compose.dev.yml 对齐的端口 / 账号(改这里就得同步改 compose)=====
$MysqlPort = 3307
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

# ===== 通用工具 =====

function Test-PortOpen([int]$Port) {
    $c = [System.Net.Sockets.TcpClient]::new()
    try {
        $iar = $c.BeginConnect('127.0.0.1', $Port, $null, $null)
        if (-not $iar.AsyncWaitHandle.WaitOne(300)) { return $false }
        $c.EndConnect($iar)
        return $true
    } catch { return $false } finally { $c.Dispose() }
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

function Get-RunningProcess([string]$Name) {
    $f = Get-PidFile $Name
    if (-not (Test-Path -LiteralPath $f)) { return $null }
    $procId = 0
    if (-not [int]::TryParse((Get-Content -LiteralPath $f -Raw).Trim(), [ref]$procId)) { return $null }
    return Get-Process -Id $procId -ErrorAction SilentlyContinue
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

function Request-GracefulStop([string]$Name) {
    <#
      对齐 docker stop 的语义:compose 路径下 docker stop 会发 SIGTERM,MySQL 会 flush、
      Redis 会存盘。原生进程没人替我们发这个信号,只能自己调管理命令。
      直接 taskkill /F 等于拔电源:MySQL 下次启动要做崩溃恢复,Redis 丢掉上次存盘之后的写入
      —— 策划一天要重启好几次,不能每次都拔电源。
      返回 $true 表示「已经请求过优雅停机,值得多等一会儿」。
    #>
    switch ($Name) {
        'mysql' {
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
    foreach ($port in $Ports) {
        foreach ($c in @(Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue)) {
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
    if ($p) {
        $procId = $p.Id

        # 只对「已经请求过优雅停机」的组件多等;没请求过就干等纯属浪费(每个组件白等 20s,
        # 一次 down 就多花一分钟)。
        if ((Request-GracefulStop $Name)) { [void](Wait-ProcessExit $p 20) }

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
        } else {
            Write-Err "$Name (PID $procId) 30s 内没能停掉 —— 下次启动可能撞端口,请手工确认。"
        }
    }
    Remove-Item -LiteralPath (Get-PidFile $Name) -Force -ErrorAction SilentlyContinue
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
    foreach ($c in @(Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue)) {
        $p = Get-Process -Id $c.OwningProcess -ErrorAction SilentlyContinue
        if (-not $p) { $rows += "pid $($c.OwningProcess)(进程查不到)"; continue }
        $exePath = $null
        try { $exePath = $p.Path } catch { $exePath = $null }
        $rows += ("{0}(pid {1}){2}" -f $p.ProcessName, $p.Id, $(if ($exePath) { " → $exePath" } else { '' }))
    }
    return , @($rows | Sort-Object -Unique)   # 逗号同上:没有占用者时必须是空数组,不是 $null
}

# 日志里认识的错 → 人话 + 该怎么办。命中几条就报几条(一次崩溃常常同时有因和果)。
# 只登记**判据明确**的:猜出来的"可能是杀软吧"只会把人带偏(见 Envoy 那段 AF_UNIX 的教训)。
$script:InfraLogHints = @(
    @{ Pattern = 'Address already in use|Bind on TCP/IP port|Do you already have another mysqld server running'
       Message = '端口已被别的进程占着。上面「端口占用者」那行就是它;不是本项目的进程就换掉它或改端口。' }
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

    if ($Port -gt 0) {
        $holders = Get-PortHolder $Port
        if ($holders.Count -gt 0) { Write-Err ("端口 :{0} 的占用者:{1}" -f $Port, ($holders -join '; ')) }
    }

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
        if (Test-PortOpen $Port) { return }
        Start-Sleep -Milliseconds 500
    }
    Write-Err "$Name 在 ${TimeoutSec}s 内没有监听 :$Port。"
    Show-ComponentFailure -Name $Name -Port $Port -Proc $Proc
    exit 1
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

function Get-Archive {
    <# 备料一个压缩包到 cache,并保证返回的文件 sha256 == $Sha256。

       三条取包路径(本机 cache / 离线镜像共享盘 / 公网 URL)统一在这里校验,少校验任何一条
       都等于留了个后门:cache 和共享盘目录都是普通可写目录,不受 HTTPS 保护,而这些包解开
       就直接执行。校验不过的文件一律删掉,绝不返回给调用方解包。 #>
    param(
        [Parameter(Mandatory)][string]$File,
        [Parameter(Mandatory)][string[]]$Urls,
        [Parameter(Mandatory)][string]$Sha256,
        [hashtable]$Headers = @{}
    )
    $dest = Join-Path $CacheDir $File

    # ① 本机 cache:命中也要校验。上次下到一半、磁盘坏块、或者有人手工往 cache 里塞了个包,
    #    都会在这里被挡下;删掉重新走正常流程,不要求人工介入。
    if (Test-Path -LiteralPath $dest) {
        if (-not $Force -and (Test-FileSha256 -Path $dest -Expected $Sha256)) { return $dest }
        if (-not $Force) { Write-Warn2 "缓存的 $File 校验不通过(已损坏或被替换),删除后重新获取。" }
        Remove-Item -LiteralPath $dest -Force -ErrorAction SilentlyContinue
    }

    # ② 离线镜像共享盘。校验不过**直接失败,不静默回退公网** —— 共享盘上的包对不上号是必须
    #    有人去看一眼的事(放错版本 or 被人换了),偷偷改从公网下会让 100 台机器各自默默绕过它,
    #    既掩盖了问题又白费了共享盘的意义。
    $mirror = $env:PANDORA_LOCALINFRA_MIRROR
    if ($mirror) {
        $src = Join-Path $mirror $File
        if (Test-Path -LiteralPath $src) {
            Write-Host "    从离线镜像拷贝: $src"
            Copy-Item -LiteralPath $src -Destination $dest -Force
            if (Test-FileSha256 -Path $dest -Expected $Sha256) { return $dest }
            Remove-Item -LiteralPath $dest -Force -ErrorAction SilentlyContinue
            Fail "离线镜像里的 $File 校验不通过(期望 sha256 $Sha256)。请找后端同学确认 $mirror 上这个文件,确认前不要绕过。"
        }
    }

    # ③ 公网,按顺序试。某个源给的包对不上就换下一个源,全都不行才报错。
    $lastErr = $null
    foreach ($u in $Urls) {
        Write-Host "    下载 $File  <- $u"
        try {
            Get-RemoteFile -Uri $u -OutFile $dest -Headers $Headers
            if (Test-FileSha256 -Path $dest -Expected $Sha256) { return $dest }
            $actual = (Get-FileHash -LiteralPath $dest -Algorithm SHA256).Hash.ToLowerInvariant()
            Write-Warn2 "该地址下到的 $File 校验不通过(期望 $Sha256,实际 $actual),换下一个源。"
            Remove-Item -LiteralPath $dest -Force -ErrorAction SilentlyContinue
            $lastErr = [Exception]::new("sha256 不匹配($u)")
        } catch {
            $lastErr = $_.Exception
            Write-Warn2 "该地址不可用:$($_.Exception.Message)"
            # 换源必须丢掉半截文件:不同源的字节偏移不保证一致,续传会拼出坏包。
            Remove-Item -LiteralPath "$dest.part" -Force -ErrorAction SilentlyContinue
        }
    }
    throw "所有下载地址都失败或校验不通过($File):$($lastErr.Message)。可让后端同学把压缩包放到共享盘,再设 PANDORA_LOCALINFRA_MIRROR 指过去。"
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

function Find-Tool([string]$Component, [string]$Exe) {
    $dir = Join-Path $DistDir $Component
    if (-not (Test-Path -LiteralPath $dir)) { return $null }
    $hit = Get-ChildItem -LiteralPath $dir -Recurse -File -Filter $Exe -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($hit) { return $hit.FullName }
    return $null
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

    if (-not (Get-Command tar.exe -ErrorAction SilentlyContinue)) {
        Fail ' 本机没有 tar.exe(Windows 10 1803+ 自带)。请升级系统或手工解包到 run/localinfra/dist/。'
    }

    foreach ($name in $Components.Keys) {
        $c = $Components[$name]
        $have = Find-Tool $name $c.Probe
        if ($have -and -not $Force) {
            # 已解包就跳过。校验的边界划在「取包」这一步:凡是从 cache / 共享盘 / 公网拿进来的
            # 压缩包,解包前一定过 sha256。已经解开的 dist 目录不再逐文件复验 —— 能改那里的人
            # 同样能改本脚本自身,再验一遍并不提高安全性,只会让每次启动多跑几秒哈希。
            Write-Ok "$name $($c.Version) 已就位"
            continue
        }
        Write-Step "备料 $name $($c.Version)"
        $ar = Get-Archive -File $c.File -Urls $c.Urls -Sha256 $c.Sha256
        $target = Join-Path $DistDir $name
        Remove-Item -LiteralPath $target -Recurse -Force -ErrorAction SilentlyContinue
        Expand-Archive2 -Archive $ar -Destination $target
        if (-not (Find-Tool $name $c.Probe)) {
            Fail "$name 解包后找不到 $($c.Probe),压缩包结构可能变了: $ar"
        }
        Write-Ok "$name $($c.Version) 备料完成"
    }

    Confirm-MkcertBinary
    Confirm-EnvoyBinary
}

function Confirm-MkcertBinary {
    <# 备料 mkcert(裸 exe)。已装了系统级 mkcert 也照样备一份自己的:版本钉死,
       免得各机器 mkcert 版本不一导致签出来的证书行为有差。 #>
    $dir = Join-Path $DistDir 'mkcert'
    $exe = Join-Path $dir 'mkcert.exe'
    if ((Test-Path -LiteralPath $exe) -and -not $Force) {
        Register-LocalToolPath
        Write-Ok "mkcert $MkcertVersion 已就位"
        return
    }
    Write-Step "备料 mkcert $MkcertVersion(签 Envoy 本机 TLS 证书用,策划机不用自己装)"
    # 校验在 Get-Archive 里做(cache / 共享盘 / 公网三条路径统一过一道),拿到就是对的。
    $src = Get-Archive -File $MkcertFile -Urls $MkcertUrls -Sha256 $MkcertSha256
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    Copy-Item -LiteralPath $src -Destination $exe -Force
    Register-LocalToolPath
    Write-Ok "mkcert $MkcertVersion 备料完成"
}

function Confirm-EnvoyBinary {
    $exe = Join-Path $DistDir 'envoy/envoy.exe'
    if ((Test-Path -LiteralPath $exe) -and -not $Force) {
        Write-Ok "envoy $EnvoyImageTag 已就位"
        return
    }
    Write-Step "备料 envoy $EnvoyImageTag(从官方 Windows 镜像层取 exe,不需要本机装 docker)"

    # registry 的 blob digest 就是内容的 sha256,直接当期望值交给 Get-Archive 统一校验。
    # 拿 token 要先看 cache / 共享盘有没有:已经有包的机器没必要再跑一趟 docker registry。
    $blobName = "envoy-windows-$EnvoyImageTag-layer.tar.gz"
    $blobFile = Join-Path $CacheDir $blobName
    $wantSha = ($EnvoyLayerDigest -replace '^sha256:', '')
    $headers = @{}
    $needNetwork = $Force -or -not (Test-FileSha256 -Path $blobFile -Expected $wantSha)
    if ($needNetwork -and -not ($env:PANDORA_LOCALINFRA_MIRROR -and (Test-Path -LiteralPath (Join-Path $env:PANDORA_LOCALINFRA_MIRROR $blobName)))) {
        $tokUri = "https://auth.docker.io/token?service=registry.docker.io&scope=repository:${EnvoyImageRepo}:pull"
        $tok = (Invoke-RestMethod -Uri $tokUri -TimeoutSec 60).token
        if (-not $tok) { Fail '拿不到 docker registry 匿名 token,检查网络 / 代理。' }
        $headers = @{ Authorization = "Bearer $tok" }
    }
    $blobFile = Get-Archive -File $blobName `
        -Urls @("https://registry-1.docker.io/v2/$EnvoyImageRepo/blobs/$EnvoyLayerDigest") `
        -Sha256 $wantSha -Headers $headers

    $stage = Join-Path $CacheDir 'envoy-stage'
    Remove-Item -LiteralPath $stage -Recurse -Force -ErrorAction SilentlyContinue
    New-Item -ItemType Directory -Force -Path $stage | Out-Null
    & tar.exe -x -z -f $blobFile -C $stage $EnvoyExeInLayer
    if ($LASTEXITCODE -ne 0) { Fail "从镜像层解出 envoy.exe 失败 (tar exit=$LASTEXITCODE)" }

    $src = Join-Path $stage $EnvoyExeInLayer
    if (-not (Test-Path -LiteralPath $src)) { Fail "镜像层里没有 $EnvoyExeInLayer" }
    New-Item -ItemType Directory -Force -Path (Join-Path $DistDir 'envoy') | Out-Null
    Move-Item -LiteralPath $src -Destination $exe -Force
    Remove-Item -LiteralPath $stage -Recurse -Force -ErrorAction SilentlyContinue
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
    <# 把 PowerShell 7 免安装包备到 cache —— 给「连 pwsh 都没有」的机器自举用。

       为什么只归 provision、不归 up:所有一键入口本身就是 pwsh 脚本,能跑到 up 的机器必然
       已经有解释器了,再下 100MB 纯属浪费。真正需要这个包的是 bootstrap_pwsh.cmd,而它是
       cmd.exe 在「pwsh 还不存在」的时候跑的,只能自己从 cache / 共享盘 / 公网取。所以这一
       步的意义是让「程序一键出策划包」把它和其它压缩包一起备进 run/localinfra/cache,一并
       放上共享盘;策划机设了 PANDORA_LOCALINFRA_MIRROR 就能完全离线地自举出解释器。

       本机不解包:解包路径归 bootstrap_pwsh.cmd(只有它知道本机到底缺不缺 pwsh)。
       版本 / SHA256 只在 lib/pwsh_bootstrap.pin 写一份,这里不抄(抄了迟早漂)。 #>
    $pin = Get-PwshBootstrapPin
    Write-Step "备料 PowerShell $($pin.PWSH_VERSION)(给没装 pwsh 的机器自举用,本机不解包)"
    $ar = Get-Archive -File $pin.PWSH_FILE -Urls @($pin.PWSH_URL) -Sha256 $pin.PWSH_SHA256
    Write-Ok "PowerShell $($pin.PWSH_VERSION) 已备到 cache:$ar"
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
    $mysqld = Find-Tool 'mysql' 'mysqld.exe'
    if (-not $mysqld) { Fail '找不到 mysqld.exe,先跑 -Action provision。' }
    $baseDir = Split-Path -Parent (Split-Path -Parent $mysqld)   # <dist>/mysql/mysql-8.4.6-winx64
    $dataMysql = Join-Path $DataDir 'mysql'

    if (Test-PortOpen $MysqlPort) {
        Write-Ok "MySQL :$MysqlPort 已在运行"
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
        $p = Start-Process -FilePath $mysqld -ArgumentList "--defaults-file=$(Get-MysqlIniPath)", '--initialize-insecure' `
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
    $proc = Start-Process -FilePath $mysqld -ArgumentList `
        "--defaults-file=$(Get-MysqlIniPath)", '--no-monitor', "--init-file=$bootstrapSql" `
        -WindowStyle Hidden -PassThru
    Save-Pid 'mysql' $proc.Id
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
    Write-Ok "MySQL :$MysqlPort"
}

# ===== Redis =====

function Start-LocalRedis {
    if (Test-PortOpen $RedisPort) {
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
    $proc = Start-Process -FilePath $exe -ArgumentList $cliArgs -WorkingDirectory (Split-Path -Parent $exe) `
        -WindowStyle Hidden -PassThru
    Save-Pid 'redis' $proc.Id
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
num.network.threads=3
num.io.threads=8
log.retention.hours=48
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
    if (Test-PortOpen $KafkaPort) {
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
    $proc = Start-Process -FilePath 'cmd.exe' -ArgumentList '/c', "`"$cmdLine`"" `
        -WorkingDirectory $home2 -WindowStyle Hidden -PassThru
    Save-Pid 'kafka' $proc.Id
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

    if ($fp -eq $oldFp -and (Get-RunningProcess 'envoy') -and (Test-PortOpen 8443) -and (Test-PortOpen 8444)) {
        Write-Ok 'Envoy :8443 / :8444 已在运行(配置与证书均未变化,不重启)'
        return
    }

    Stop-Component 'envoy'
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
    $proc = Start-Process -FilePath $exe `
        -ArgumentList '-c', "`"$cfg`"", '--log-level', 'warn', '--log-path', "`"$(Join-Path $LogDir 'envoy.log')`"" `
        -WorkingDirectory $CfgDir -WindowStyle Hidden -PassThru
    Save-Pid 'envoy' $proc.Id
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

function Invoke-Up {
    Invoke-ProvisionAll
    Start-LocalMysql
    Start-LocalRedis
    Start-LocalKafka
    Start-LocalEnvoy
    Write-Host ''
    Write-Host '  本机基础设施已就绪(免 Docker)' -ForegroundColor Green
    Invoke-Status
}

function Invoke-Down {
    foreach ($n in @('envoy', 'kafka', 'redis', 'mysql')) { Stop-Component $n }
    Write-Ok '本机基础设施已停止(数据保留)'
}

function Invoke-Status {
    $rows = @(
        @{ Name = 'mysql'; Port = $MysqlPort }
        @{ Name = 'redis'; Port = $RedisPort }
        @{ Name = 'kafka'; Port = $KafkaPort }
        @{ Name = 'envoy'; Port = 8443 }
        @{ Name = 'envoy-ds'; Port = 8444 }
    )
    Write-Host ''
    Write-Host '  组件      端口   状态' -ForegroundColor Gray
    foreach ($r in $rows) {
        $ok = Test-PortOpen $r.Port
        $txt = if ($ok) { 'UP  ' } else { 'DOWN' }
        $color = if ($ok) { 'Green' } else { 'Red' }
        Write-Host ("  {0,-9} {1,-6} {2}" -f $r.Name, $r.Port, $txt) -ForegroundColor $color
    }
    Write-Host ''
}

function Invoke-Reset {
    Invoke-Down
    Write-Step '删除本机基础设施数据目录'
    Remove-Item -LiteralPath $DataDir -Recurse -Force -ErrorAction SilentlyContinue
    Write-Ok "已删除 $DataDir(下次 up 会重新初始化)"
}

New-Item -ItemType Directory -Force -Path $Root, $LogDir, $PidDir, $CfgDir | Out-Null
# 已备料过就先挂 PATH:down / status 这类不走 provision 的动作也能用上自带工具。
Register-LocalToolPath

switch ($Action) {
    'up' { Invoke-Up }
    'down' { Invoke-Down }
    'status' { Invoke-Status }
    # provision 比 up 多备一份 PowerShell 7 免安装包:它只服务于「本机连 pwsh 都没有」的
    # 策划机(由 bootstrap_pwsh.cmd 在 cmd.exe 里自举),能跑到 up 的机器用不上。
    'provision' { Invoke-ProvisionAll; Save-PwshBootstrapArchive }
    'reset' { Invoke-Reset }
}
