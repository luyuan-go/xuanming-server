<#
.SYNOPSIS
  Pandora 后端一键启动器(策划/开发都能用)。

.DESCRIPTION
  一条命令把后端跑起来,覆盖 5 套环境(DS 分配模式随环境变):
    local    本地 windows 调试 —— 基础设施在 docker,22 个 go 服务以宿主进程跑(可断点);DS=local(Windows PandoraServer.exe)
    docker   本地 docker 启动   —— 基础设施 + 22 个 go 服务全跑在本机 docker;DS=mock(容器内无真 DS)
    intranet 内网测试服     —— 同 docker 全容器,但绑定内网 IP 供多人联调;DS=mock
    online   线上 k8s 集群   —— kustomize 部署到远端 k8s + Agones 真 Linux DS;DS=agones
                             用 -Env test|prod 区分「测试服集群」与「生产 kbs 集群」(不同 kube-context)

  还有两个本地联调辅助模式:
    battle   【已废弃,2026-07-14】原含战斗混合版(18 业务容器 + 宿主 allocator exec Windows DS)。
                             Windows DS 只保留给 local 断点调试;要真 DS 一律用 k8s(Agones Linux DS)。
                             仅 -Down/-Status 可用(清理/查看旧机器遗留环境),启动会拒绝。
                             见 docs/design/decision-revisit-retire-battle-mode.md。
    k8s      本地 minikube 联调 Agones —— 本机起 minikube + Agones,验证真 Linux DS 链路;DS=agones(默认 advertise 本机局域网 IP + 自动宿主桥接/UDP 中继)

  启动前会检查必要工具(go / docker / kubectl / minikube)。默认只提示缺失项,不改本机环境;
  只有显式传 -Install 才会尝试用 winget 安装。-Check 只检查不启动。

.EXAMPLE
  pwsh tools/scripts/start.ps1                      # 默认 local 模式(本地 windows 调试,全部服务)
  pwsh tools/scripts/start.ps1 -Mode docker
  pwsh tools/scripts/start.ps1 -Mode intranet                       # 内网测试服(全容器,绑内网 IP)
  pwsh tools/scripts/start.ps1 -Mode k8s                            # 本地 minikube + Agones 真 DS 联调
  pwsh tools/scripts/start.ps1 -Mode online -Env test -TestKubeContext pandora-test -Registry registry.mycorp.com -Tag v1.2.3-b5a5a95 -BattleDsImage registry.mycorp.com/pandora/battle-ds@sha256:<digest> -HubDsImage registry.mycorp.com/pandora/hub-ds@sha256:<digest> -DsGatewayAddr pandora-envoy.pandora.svc:8444  # 测试服
  pwsh tools/scripts/start.ps1 -Mode online -Env prod -ProdKubeContext pandora-prod -Registry registry.mycorp.com -Tag v1.2.3-b5a5a95 -BattleDsImage registry.mycorp.com/pandora/battle-ds@sha256:<digest> -HubDsImage registry.mycorp.com/pandora/hub-ds@sha256:<digest> -DsGatewayAddr pandora-envoy.pandora.svc:8444  # 生产(双重确认)
  pwsh tools/scripts/start.ps1 -Mode docker -Down  # 停
  pwsh tools/scripts/start.ps1 -Status             # 看状态
  pwsh tools/scripts/start.ps1 -Check              # 只检查工具
  pwsh tools/scripts/start.ps1 -Install            # 缺工具时才尝试 winget 安装

.EXAMPLE
  # 只更新了客户端仓(改资源 / 重编编辑器 DLL),go 服务没动 —— 只重启本机 DS,秒级:
  pwsh tools/scripts/start.ps1 -Mode local -DsOnly
  # 基础设施、数据库迁移、22 个 go 服务全部原样不动;后端没在跑时自动回落到完整启动。

.EXAMPLE
  # 电脑重启后『快速恢复』上次的环境(不重建镜像,把停掉的集群/容器拉回来):
  pwsh tools/scripts/start.ps1 -Mode k8s    -Resume   # minikube start + 等 Pod + 自动重建宿主桥接/UDP 中继
  pwsh tools/scripts/start.ps1 -Mode docker -Resume   # docker compose up -d(不 --build)
  pwsh tools/scripts/start.ps1 -Mode local  -Resume   # 基础设施随 Docker 恢复 + 重起宿主 go 服务

  # 环境乱了想『一键重置』再全新起(彻底清掉旧状态):
  pwsh tools/scripts/start.ps1 -Mode k8s    -Reset    # minikube delete 后全新部署
  pwsh tools/scripts/start.ps1 -Mode docker -Reset    # 容器全清后重建启动
#>
[CmdletBinding()]
param(
    [ValidateSet('local', 'docker', 'intranet', 'battle', 'k8s', 'online')]
    [string]$Mode = 'local',

    # online 环境:test=测试服集群 / prod=生产 kbs 集群(不同 kube-context,prod 双重确认)
    [ValidateSet('test', 'prod')]
    [string]$Env = 'test',

    # intranet 对外广告 IP(内网其它机器连本机用;留空自动取本机内网 IPv4)
    [string]$AdvertiseHost = '',

    # 局域网模式默认只把【客户端面 8443】开到局域网;未鉴权的 DS 面 8444 恒绑本机。
    # 仅当真有【异机 UE DS】需要回连本机 Envoy :8444 时才加此开关显式开放(务必配合网络隔离)。
    [switch]$ExposeDsFace,

    # -LocalOnly(仅 -Mode local):恢复「只绑回环 127.0.0.1」的老行为。
    # 不加此开关时,local 模式默认把【客户端面 8443】绑到 0.0.0.0 并把 DS advertise 指向本机
    # 局域网 IP(自动解析,可用 -AdvertiseHost 覆盖),局域网里其它人能直接连进你的服务器。
    # 未鉴权的 DS 面 8444 与 admin 9901 不受影响,仍恒绑本机。
    [switch]$LocalOnly,

    # ---- 本机 DS「进程形态」(仅 -Mode local 生效;不传 = 完全沿用 yaml,原有链路零变化)----
    # packaged:跑打包好的 PandoraServer.exe(内容已 cook,改 .uasset 要重新出包才生效)。
    # editor  :跑引擎的 UnrealEditor.exe + Pandora.uproject,仍带 -server(NetMode 依旧是
    #           NM_DedicatedServer),直接读工程里未 cook 的 Content/ —— 策划存盘后重进大厅/重开一局
    #           即生效,免出包免编译。登录/大厅/匹配链路一字不改,后端不感知这个差别。
    #           代价:DS 启动慢(加载一大批编辑器模块 + 读未 cook 的散装资产),首次进图可能等一两分钟;
    #           allocator 会自动把 ready 等待/心跳超时放宽到 300s/120s。
    #           (不包括编 shader:-server 下引擎跳过全局与材质着色器编译。)
    [ValidateSet('', 'packaged', 'editor')]
    [string]$DsLauncher = '',
    # editor:Pandora.uproject 路径。留空则自动探测「与本仓库平级的客户端仓」,策划零手改。
    [string]$DsProject = '',
    # editor:UnrealEditor.exe 路径。留空则按 .uproject 里的 EngineAssociation 查注册表定位引擎,
    # 所以策划机引擎装在哪个盘/哪个目录都行(源码版走 HKCU Builds GUID,发行版走 HKLM 版本号)。
    [string]$DsEditorExe = '',
    # -GenTables:起服务/重启服务之前,先把策划 xlsx 导成服务端配置表(configtable/dist)。
    # 策划改完表就是为了测这一改动,让他们再记一个"先双击导表"的步骤迟早会漏。
    # 适用于 local / docker / intranet / k8s —— 这几条路吃的都是磁盘上同一份 configtable/dist
    # (compose 是 bind mount,k8s 是由该目录生成 pandora-configtable ConfigMap),表并不在镜像里。
    # 非 local 时若本机没有客户端源表,则跳过导表、沿用仓库里已有的 dist,不拦启动。
    # -Mode online 明确不支持:线上部署的表必须能从 git 检出复现。不传时行为逐字节不变。
    [switch]$GenTables,
    # -DsOnly(仅 -Mode local):**只重启本机 DS**,不动 docker 基础设施、不动其余 go 服务。
    # 给「只更新了客户端仓(改资源 / 重编编辑器 DLL),go 服务一行没改」的日常循环用:
    # 完整启动要走基础设施健康等待 → TiDB → 数据库迁移 → 22 个 go 服务逐个 build/起/探活,
    # 这个场景下全是白跑。后端没在跑时自动回落到完整启动(见 Invoke-LocalDsOnly)。
    [switch]$DsOnly,

    # -NoDocker(仅 -Mode local):本机基础设施改用**原生 Windows 进程**(tools/scripts/local_infra.ps1)
    # 而不是 docker-compose —— MySQL/Redis/Kafka/Envoy 免安装二进制,不装 Docker Desktop、不开 WSL2。
    # 同时不起 TiDB(TiKV 没有可用的 Windows 原生部署),社交四服改连本机 MySQL 的 pandora_social。
    # 端口 / 账号 / schema 与 docker 模式逐项一致,业务配置不分叉。默认关,不传时行为逐字节不变。
    [switch]$NoDocker,

    [switch]$Down,        # 停止该模式
    [switch]$Resume,      # 电脑重启后快速恢复:不重建镜像,把上次停掉的集群/容器拉回来
    [switch]$Reset,       # 一键重置:彻底清掉旧状态再全新启动(线上 online 模式禁用)
    [switch]$Status,      # 查看状态
    [switch]$Check,       # 只检查工具链,不启动
    [switch]$BuildOnly,   # 只构建业务镜像后退出(供 export_images.ps1 离线打包用)
    [switch]$Rebuild,     # 强制重新构建镜像,忽略 deploy/offline-images 离线包(开发机改代码后用)

    # 镜像构建方式(方案 A / 方案 B,可选):
    #   incontainer 方案A:在 Docker 容器里 go build(环境隔离,无需本机 Go,冷缓存较慢)——默认,保持既有行为
    #   host        方案B:本机交叉编译 linux 二进制再塞进 scratch 镜像(享受宿主增量缓存,单服务重建秒级,需装 Go)
    [ValidateSet('incontainer', 'host')]
    [string]$BuildMode = 'incontainer',
    [string[]]$Only = @(), # 配合 -BuildOnly:只构建指定服务(连字符名,如 battle-result);留空=全部业务镜像
    [switch]$Install,     # 工具缺失时尝试 winget 安装(默认不安装;不含 Docker Desktop)
    [switch]$InstallDocker, # 显式同意用 winget 装 Docker Desktop(装前确认虚拟化;装后仍需重启+手动启动)
    [switch]$NoInstall,   # 兼容旧参数;等同于不传 -Install

    # online 模式参数
    [string]$Registry,    # 镜像仓库地址,如 registry.mycorp.com
    [string]$Tag,         # online 不可变 tag,必须含 git SHA 段,如 v1.2.3-b5a5a95
    [string]$BattleDsImage, # online:Stable 战斗 DS 镜像,最终会解析并固定为 repo@sha256:digest
    [string]$HubDsImage,    # online:Stable 大厅 DS 镜像,最终会解析并固定为 repo@sha256:digest
    [string]$CanaryBattleDsImage, # online:Canary 战斗 DS 独立镜像；启用 Battle 灰度时必填且 digest 必须不同
    [string]$CanaryHubDsImage,    # online:Canary 大厅 DS 独立镜像；启用 Hub 灰度时必填且 digest 必须不同
    [ValidateRange(0, 100)][int]$BattleCanaryPercent = 0,
    [ValidateRange(0, 100)][int]$HubCanaryPercent = 0,
    [ValidateRange(0, 1000)][int]$BattleCanaryReplicas = 1,
    [ValidateRange(0, 1000)][int]$HubCanaryReplicas = 1,
    # online:Battle FleetAutoscaler 覆写。maxReplicas = 同时最大局数护栏,设为节点池上限
    # 对应的容量(真弹性由集群 Cluster Autoscaler 加节点提供);bufferSize 支持百分比(如 10%)。
    # 0/空 = 用 25-fleetautoscaler-battle.yaml 的本地 dev 默认值。
    [ValidateRange(0, 1000000)][int]$BattleMaxReplicas = 0,
    [ValidatePattern('^([0-9]+%?)?$')][string]$BattleBufferSize = '',
    [string]$CanarySeed = $env:PANDORA_DS_CANARY_SEED,
    [string]$DsGatewayAddr, # online:DS 回调入口(如 pandora-envoy.pandora.svc:8444)
    # online 安全映射:不同环境必须显式绑定各自 kube-context；也可用同名环境变量持久配置。
    [string]$TestKubeContext = $env:PANDORA_K8S_TEST_CONTEXT,
    [string]$ProdKubeContext = $env:PANDORA_K8S_PROD_CONTEXT,
    [ValidateSet('0', '1')]
    # online:DS 回调是否 TLS。权威口径(gateway-decision.md §3.5/§16):DS 面 :8444 是集群内明文,
    # 本地/线上同构默认 0;仅当线上把 DS 面挂到集群外 TLS 边缘时才显式传 1
    # (2026-07-10 已统一旧版互相冲突的默认值)。
    [string]$DsGatewayTls = '0',
    [ValidateSet('', 'enforce')]
    # online:Redis authority 只允许 enforce；permissive/off 不再是生产逃生口。
    [string]$DsAuthMode = '',
    [ValidateSet('', 'redis')]
    [string]$DsAuthorityMode = '',
    [string]$DsFenceEtcdEndpoints = $env:PANDORA_DS_AUTH_FENCE_ETCD_ENDPOINTS,
    [string]$DsFenceKeysetRevision = $env:PANDORA_DS_AUTH_KEYSET_REVISION,
    # 生产 DS auth etcd 身份材料只接受外部文件路径；脚本绝不创建、修改或打印其内容。
    [string]$DsFenceEtcdIdentityRevision = $env:PANDORA_DS_AUTH_ETCD_IDENTITY_REVISION,
    [string]$DsFenceEtcdServerName = $env:PANDORA_DS_AUTH_ETCD_SERVER_NAME,
    [string]$DsFenceEtcdAuditorCAFile = $env:PANDORA_DS_AUTH_ETCD_AUDITOR_CA_FILE,
    [string]$DsFenceEtcdAuditorCertFile = $env:PANDORA_DS_AUTH_ETCD_AUDITOR_CERT_FILE,
    [string]$DsFenceEtcdAuditorKeyFile = $env:PANDORA_DS_AUTH_ETCD_AUDITOR_KEY_FILE,
    [string]$DsFenceEtcdAuditorIdentity = $env:PANDORA_DS_AUTH_ETCD_AUDITOR_IDENTITY,
    [string]$DsFenceEtcdForbiddenReadPrefix = $env:PANDORA_DS_AUTH_ETCD_FORBIDDEN_READ_PREFIX,
    [string]$DsFenceEtcdAuditorUsernameFile = $env:PANDORA_DS_AUTH_ETCD_AUDITOR_USERNAME_FILE,
    [string]$DsFenceEtcdAuditorPasswordFile = $env:PANDORA_DS_AUTH_ETCD_AUDITOR_PASSWORD_FILE,
    # DSTicket v2(方案 B,RS256)active kid:online 可选显式期望值；实际值永远从
    # immutable Secret 私钥 + JWKS 顶层 active_kid 对账得到，不读 keys[0]。
    [string]$DsTicketActiveKid = $env:PANDORA_DSTICKET_ACTIVE_KID,
    # 玩家 DSTicket 公钥 keyset revision，和 DS callback auth 的 keyset revision 是两套独立值。
    [string]$DsTicketKeysetRevision = $env:PANDORA_DSTICKET_KEYSET_REVISION,
    [switch]$BuildPush    # online:本地构建并推送 22 个镜像到 -Registry(远端发布动作,需人工授权)
)

$ErrorActionPreference = 'Stop'
$ScriptDir   = $PSScriptRoot
$ProjectRoot = (Resolve-Path "$ScriptDir/../..").Path

# 本脚本也会由长期运行的 web 进程拉起。Windows 进程只在启动时继承一次 PATH；之后安装
# minikube 等工具，即使已经写入机器/用户 PATH，web 创建的新控制台仍会继承旧快照。
# 每次启动都把当前持久化 PATH 合并进本进程，避免要求重启 web 或整机。
function Sync-ProcessPathFromRegistry {
    if (-not $IsWindows) { return }

    $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    $entries = [Collections.Generic.List[string]]::new()
    foreach ($pathValue in @(
        $env:PATH,
        [Environment]::GetEnvironmentVariable('Path', 'Machine'),
        [Environment]::GetEnvironmentVariable('Path', 'User')
    )) {
        if ([string]::IsNullOrWhiteSpace($pathValue)) { continue }
        foreach ($entry in $pathValue.Split(';', [StringSplitOptions]::RemoveEmptyEntries)) {
            $expanded = [Environment]::ExpandEnvironmentVariables($entry.Trim())
            if (-not [string]::IsNullOrWhiteSpace($expanded) -and $seen.Add($expanded)) {
                $entries.Add($expanded)
            }
        }
    }
    $env:PATH = $entries -join ';'
}

# kubectl/helm/docker/minikube 的 stdout 一律是 UTF-8。控制台默认代码页(中文机器为 GBK/936)
# 会让 PowerShell 按 GB2312 解码这些字节，含中文的 JSON(如 pandora-configtable 的配表数据)
# 被解成乱码后 ConvertFrom-Json 直接报 "Invalid property identifier character"。
# 这里把本进程的控制台解码固定成 UTF-8，保证捕获原生命令输出与写文件一致。
try { [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false) } catch { }

Sync-ProcessPathFromRegistry
. (Join-Path $ScriptDir 'lib/online_manifest_contract.ps1')
. (Join-Path $ScriptDir 'lib/ds_auth_activation_contract.ps1')
. (Join-Path $ScriptDir 'lib/dsticket_keyset_contract.ps1')
. (Join-Path $ScriptDir 'lib/dsticket_rotation_contract.ps1')
# 本机 dev.env 自举(被 git 忽略 → 新机器必然缺)。start.ps1 自己也在恢复 / 状态查询处
# 用 --env-file $EnvFile,所以入口这一层也要接。
. (Join-Path $ScriptDir 'dev_env_file.ps1')
# 客户端仓(SVN 工作副本)定位。与 configtable_gen.ps1 / configtable_sync.ps1 同一份,
# 保证「跑 DS 的那个仓」和「导表的那个仓」永远是同一个。
. (Join-Path $ScriptDir 'client_repo_lib.ps1')
# rotation contract 自身在加载期开启 StrictMode；start.ps1 是历史运维入口，
# 未全面满足 StrictMode Latest，因此只共享其纯函数契约后恢复本脚本既有语义。
Set-StrictMode -Off
$ComposeInfra    = Join-Path $ProjectRoot 'deploy/docker-compose.dev.yml'
$ComposeServices = Join-Path $ProjectRoot 'deploy/docker-compose.services.yml'
$EnvFile         = Join-Path $ProjectRoot 'deploy/env/dev.env'
$ClusterEtcDir   = Join-Path $ProjectRoot 'run/cluster/etc'
$K8sNamespace    = 'pandora'

# ===== 输出辅助 =====
function Write-Info($m) { Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Write-Ok($m)   { Write-Host "[ OK ] $m" -ForegroundColor Green }
function Write-Skip($m) { Write-Host "[SKIP] $m" -ForegroundColor DarkGray }
function Write-Warn($m) { Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Write-Err($m)  { Write-Host "[ERR ] $m" -ForegroundColor Red }
function Write-Step($m) { Write-Host "`n===== $m =====" -ForegroundColor Magenta }

# native 命令(kubectl/minikube/docker 等)fail-fast:执行后立刻检查 $LASTEXITCODE,
# 非 0 直接抛错中止,避免「某步骤失败但脚本继续往下跑、最后还打印 [OK]」的假成功。
function Assert-LastExit([string]$what) {
    if ($LASTEXITCODE -ne 0) { throw "$what 失败(exit=$LASTEXITCODE)" }
}

# ---- Down 幂等性:Down 必须能被反复执行 ----------------------------------------
# 真实现场里 Down 极少「从头干净跑到尾」:人会中途关窗口、按 Ctrl+C、拔电源;别人可能
# 同时在另一个窗口 minikube stop/delete。于是下一次双击「一键停止」面对的必然是一个
# **拆了一半**的集群。这些残缺状态对 Down 的目标(集群内不再有我们起的东西)而言全部
# 等价于「已经没有残留可删」,不是错误——重跑必须照样成功。
#
# 旧写法 `kubectl delete ... --ignore-not-found 2>$null; Assert-LastExit '...'` 有三个硬伤:
#   ① `--ignore-not-found` 只忽略「对象不存在」,不忽略「namespace 不存在」「CRD 不存在」;
#   ② stderr 被 `2>$null` 丢掉,真出错时只剩一句「失败(exit=1)」,人完全不知道发生了什么;
#   ③ 集群在 Down 执行中途被停掉时,剩下的每一条 delete 都 connection refused 硬抛,
#      于是「点了一半关掉」的机器,下次重跑还是一片红字。
# 下面这些谓词把「良性残缺」与「真故障」分开,是 Down 可重入的根据。
function Test-K8sDownBenignMissingText([string]$Text) {
    return ($Text -match '(?i)not found' -or
        $Text -match '(?i)no matches for kind' -or
        $Text -match '(?i)unable to recognize' -or
        $Text -match '(?i)the server doesn''t have a resource type')
}

# apiserver 已经不可达(集群被停/被删/正在关闭中)。此时集群内对象已随集群一并消失,
# Down 的目标天然达成;继续 delete 只会刷屏 connection refused。
function Test-K8sApiserverUnreachableText([string]$Text) {
    return ($Text -match '(?i)connection refused' -or
        $Text -match '(?i)connection was refused' -or
        $Text -match '(?i)no such host' -or
        $Text -match '(?i)i/o timeout' -or
        $Text -match '(?i)dial tcp' -or
        $Text -match '(?i)couldn''t get current server API group list' -or
        $Text -match '(?i)the server is currently unable to handle the request' -or
        $Text -match '(?i)did you specify the right host or port' -or
        $Text -match '(?i)unable to connect to the server')
}

# 一旦确认 apiserver 没了,后续所有集群侧删除整体跳过,让 Down 平静收尾而不是刷一屏红字。
$script:K8sDownApiserverGone = $false

function Invoke-K8sDownDelete {
    param(
        [Parameter(Mandatory = $true)][string]$What,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )
    if ($script:K8sDownApiserverGone) { Write-Skip "${What}:集群 apiserver 已不可达,跳过。"; return }

    # apiserver 偶发抖动(minikube 刚起/正在换 leader)与「集群真没了」在文本上无法区分,
    # 只能靠重试区分:连试 3 次仍不可达才判定集群已消失。
    $text = ''
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        $lines = @(& kubectl @Arguments 2>&1)
        if ($LASTEXITCODE -eq 0) {
            foreach ($line in @($lines)) { if (-not [string]::IsNullOrWhiteSpace($line)) { Write-Host $line } }
            return
        }
        $text = ((@($lines) | ForEach-Object { $_.ToString() }) -join "`n")
        if (-not (Test-K8sApiserverUnreachableText $text)) { break }
        if ($attempt -lt 3) {
            Write-Info "  ${What}:apiserver 暂不可达,${attempt}/3 重试..."
            Start-Sleep -Seconds 2
        }
    }
    if (Test-K8sApiserverUnreachableText $text) {
        $script:K8sDownApiserverGone = $true
        Write-Ok "${What}:集群 apiserver 不可达,集群内对象已随集群一并停止,后续集群侧删除跳过。"
        return
    }
    if (Test-K8sDownBenignMissingText $text) {
        Write-Skip "${What}:目标对象/资源类型/namespace 已不存在,无残留可删,跳过。"
        return
    }
    throw "$What 失败(exit=$LASTEXITCODE):$text"
}

# 普通 -Down 只停止本地基础设施，不得删除 namespace/pandora 与 etcd-data PVC。
# required_writer_epoch、V3 activation record 与 writer capability 都以 etcd 为权威；
# 删 namespace 会级联清空该 PVC，删 PVC 会丢基线，下次启动都会形成
# “旧 minikube profile + 全新空 etcd”，既无法 Resume，也不满足 fresh-profile 自动 genesis 门。
#
# 多文档清单（infra.yaml 用内联 flow 写 metadata，kustomize 渲染又是多份 `---` 拼接）不能靠
# `kubectl create -o json` 一次拿 List——它输出的是并列 JSON 对象流，ConvertFrom-Json 会在
# 第二个对象处报“Additional text encountered”。因此逐文档用客户端 dry-run 取 kind/name/namespace，
# 稳定识别后再把非保留对象合并成一份清单批量删除。
function Get-K8sManifestObjectIdentities {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$ManifestText
    )
    $docs = @([regex]::Split($ManifestText, "(?m)^\s*---\s*$") | Where-Object {
        (-not [string]::IsNullOrWhiteSpace($_)) -and ($_ -match '(?m)^\s*kind\s*:')
    })
    $result = New-Object System.Collections.Generic.List[object]
    foreach ($doc in $docs) {
        $idLine = $doc | & kubectl --context $KubeContext create --dry-run=client -f - `
            -o 'jsonpath={.kind}|{.metadata.name}|{.metadata.namespace}' 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "解析 k8s 清单文档失败:$(@($idLine) -join [Environment]::NewLine)"
        }
        $flat = ((@($idLine) | ForEach-Object { $_.ToString() }) -join '').Trim()
        $parts = $flat -split '\|', 3
        $result.Add([pscustomobject]@{
            Kind      = $parts[0].Trim()
            Name      = if ($parts.Count -ge 2) { $parts[1].Trim() } else { '' }
            Namespace = if ($parts.Count -ge 3) { $parts[2].Trim() } else { '' }
            Doc       = $doc.Trim()
        })
    }
    return , $result.ToArray()
}

# 删除清单中除“保留谓词”命中对象外的全部对象。ShouldPreserve 接收单个身份对象，返回 $true 表示保留。
# 容忍 namespace 已不存在时的“not found”——普通 Down 可能对着一个早已被拆掉的集群空跑。
function Remove-K8sManifestObjectsPreserving {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$ManifestText,
        [Parameter(Mandatory = $true)][scriptblock]$ShouldPreserve,
        [Parameter(Mandatory = $true)][string]$What
    )
    # ⚠️ 这里**绝不能**套 @()。Get-K8sManifestObjectIdentities 用 `return , $array` 防止
    # 单元素结果被解包;外面再套 @() 得到的是「只含一个数组的数组」(Count 恒为 1)。
    # 后果不是少删几个对象,而是保留谓词整体失效:管道只喂给 Where-Object 一个对象,
    # `[string]$o.Kind` 变成 "Namespace Service Deployment ..." 这样的空格串,
    # 既不 -ceq 'Namespace' 也不 -ceq 'PersistentVolumeClaim' → 判定为"不保留" →
    # payload = 整份清单 → 连 namespace/pandora 与 PVC/etcd-data 一起删掉,
    # 每次 Down 都以「namespace 缺失」的 fail-fast 收场(实测复现)。
    # 同理 $identities.Count 也会恒为 1,让下面的"清单为空"守卫一并失效。
    $identities = Get-K8sManifestObjectIdentities -KubeContext $KubeContext -ManifestText $ManifestText
    if ($identities.Count -eq 0) { throw "${What}:清单为空，拒绝在未知状态下继续 Down。" }
    $toDelete = @($identities | Where-Object { -not (& $ShouldPreserve $_) })
    if ($toDelete.Count -eq 0) { return , $identities }
    if ($script:K8sDownApiserverGone) { Write-Skip "${What}:集群 apiserver 已不可达,跳过。"; return , $identities }
    $payload = ($toDelete | ForEach-Object { $_.Doc }) -join "`n---`n"
    $deleteLines = @($payload | & kubectl --context $KubeContext delete -f - --ignore-not-found 2>&1)
    if ($LASTEXITCODE -ne 0) {
        $text = ((@($deleteLines) | ForEach-Object { $_.ToString() }) -join "`n")
        # 集群已随 minikube stop/delete 消失 → 集群内对象天然无残留,平静收尾。
        if (Test-K8sApiserverUnreachableText $text) {
            $script:K8sDownApiserverGone = $true
            Write-Ok "${What}:集群 apiserver 不可达,集群内对象已随集群一并停止,后续集群侧删除跳过。"
            return , $identities
        }
        # namespace 早已不存在 → 其内 namespaced 对象天然无残留，等价 not-found，容忍。
        if (Test-K8sDownBenignMissingText $text) {
            Write-Skip "${What}:目标 namespace/对象已不存在，无残留可删，跳过。"
            return , $identities
        }
        throw "$What 失败:$text"
    }
    foreach ($line in @($deleteLines)) { if (-not [string]::IsNullOrWhiteSpace($line)) { Write-Host $line } }
    return , $identities
}

# 读取本地 etcd-data PVC 是否存在。namespace 缺失、集群已不可达都按“不存在”处理，不抛错——
# 这个探针只用来决定「Down 后要不要校验保留」，在拆了一半的集群上不该成为重跑的障碍。
function Test-LocalEtcdDataPvcExists {
    param([Parameter(Mandatory = $true)][string]$KubeContext)
    $out = @(& kubectl --context $KubeContext get pvc/etcd-data -n $K8sNamespace --ignore-not-found -o name 2>&1)
    $flat = ((@($out) | ForEach-Object { $_.ToString() }) -join "`n").Trim()
    if ($flat -match 'persistentvolumeclaim/etcd-data') { return $true }
    if ($LASTEXITCODE -eq 0) { return $false }
    if (Test-K8sApiserverUnreachableText $flat) { $script:K8sDownApiserverGone = $true; return $false }
    if (Test-K8sDownBenignMissingText $flat) { return $false }
    throw "读取 pvc/etcd-data 状态失败:$flat"
}

# 普通 Down 结束后回读 namespace/pandora 与 PVC/etcd-data，二者缺一则说明 DS auth 权威已丢，fail-fast。
function Assert-LocalEtcdDataPreservedAfterDown {
    param([Parameter(Mandatory = $true)][string]$KubeContext)
    $nsName = ((@(& kubectl --context $KubeContext get "namespace/$K8sNamespace" --ignore-not-found -o name 2>&1) |
            ForEach-Object { $_.ToString() }) -join '').Trim()
    if ($LASTEXITCODE -ne 0 -or $nsName -cne "namespace/$K8sNamespace") {
        throw "普通 Down 后 namespace/$K8sNamespace 缺失；etcd-data PVC 已随之级联删除，DS auth 权威状态可能已丢失。"
    }
    $pvcName = ((@(& kubectl --context $KubeContext get pvc/etcd-data -n $K8sNamespace --ignore-not-found -o name 2>&1) |
            ForEach-Object { $_.ToString() }) -join '').Trim()
    if ($LASTEXITCODE -ne 0 -or $pvcName -cne 'persistentvolumeclaim/etcd-data') {
        throw '普通 Down 后无法回读 PVC/etcd-data；DS auth 权威状态可能已丢失。'
    }
    Write-Ok "本地 k8s 基础设施已停止；namespace/$K8sNamespace 与 PVC/etcd-data 已保留供下次启动恢复 DS auth 基线。"
}

# pandora-config Secret 只收录 22 份服务 YAML。envoy-jwks.json 是给外部边缘网关的产物，
# OutDir 中其它运维文件也不应被 --from-file=<目录> 意外灌进 Pod Secret。
function Apply-PandoraConfigSecret {
    param(
        [string]$KubeContext,
        [string]$ConfigDir = $ClusterEtcDir,
        [string]$Action
    )

    $kubectlContextArgs = @('--context', $KubeContext)
    $expectedNames = @()
    $expectedPaths = @{}
    $fileArgs = @(
        foreach ($svc in (Get-ServiceList)) {
            $name = "$($svc.Name).yaml"
            $expectedNames += $name
            $path = Join-Path $ConfigDir $name
            if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
                throw "pandora-config Secret 缺少必需配置:$path"
            }
            $expectedPaths[$name] = $path
            "--from-file=$name=$path"
        }
    )
    # 期望份数从 Get-ServiceList 推导,**不写字面量**:上一版硬编码 21,新增 mission 后
    # 一键启动在这里直接抛错。计数仍保留(而不是删掉)是为了挡住 foreach 里因异常吞掉
    # 而少收一份的情况 —— 但基线必须跟着服务清单走。
    $expectedServiceCount = @(Get-ServiceList).Count
    if ($fileArgs.Count -ne $expectedServiceCount) {
        throw "pandora-config Secret 期望 $expectedServiceCount 份服务配置,实际=$($fileArgs.Count)"
    }

    $manifest = @(kubectl @KubectlContextArgs create secret generic pandora-config @fileArgs `
        -n $K8sNamespace --dry-run=client -o yaml)
    Assert-LastExit "$Action(create secret manifest)"
    $manifest | kubectl @KubectlContextArgs apply -f -
    Assert-LastExit $Action

    # client-side apply 遇到缺 last-applied annotation 的人工对象时可能保留旧 data key；回读服务端
    # 严格核对，任何多余/缺失项都阻断 rollout，避免把陈旧配置伪装成“精确 21 文件”。
    $secret = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'secret/pandora-config', '-n', $K8sNamespace, '-o', 'json') `
        -Action '回读 pandora-config Secret'
    $actualNames = if ($null -eq $secret.data) { @() } else { @($secret.data.PSObject.Properties.Name | Sort-Object) }
    $keyDiff = @(Compare-Object -ReferenceObject @($expectedNames | Sort-Object) -DifferenceObject $actualNames -CaseSensitive)
    if ($keyDiff.Count -ne 0) {
        $missing = @($keyDiff | Where-Object SideIndicator -eq '<=' | ForEach-Object InputObject)
        $extra = @($keyDiff | Where-Object SideIndicator -eq '=>' | ForEach-Object InputObject)
        throw "pandora-config Secret 服务端 key 集不精确:缺少=[$($missing -join ', ')],多余=[$($extra -join ', ')]。" +
              '请先清理漂移 key 后重跑；当前不会继续 rollout。'
    }

    # 只核对 key 名不够 —— 名字齐全但**内容陈旧**同样是致命漂移,而且更隐蔽。
    # 2026-08-05 实测事故:磁盘已是 :20001 的集群配置,Secret 里却still是 :50001 的旧端口方案,
    # 而 `kubectl apply` 报的是 `unchanged`(客户端侧 apply 以 last-applied 注解为基线做三方合并,
    # live 被带外改动过时可能算不出 patch)。结果 22 个服务全部按错端口启动、gRPC readiness 探针
    # 全红,直到 [7/8] rollout 180s 超时才暴露,且报错指向"新 Pod 未就绪"这种无关症状。
    # 因此这里逐 key 比对**内容字节**;漂移就用 replace 强制收敛(整份覆盖 data,不走三方合并),
    # 再复验一次,仍不一致才 fail-fast。
    $driftedKeys = @(
        foreach ($name in $expectedNames) {
            $liveBytes = [Convert]::FromBase64String([string]$secret.data.$name)
            $diskBytes = [System.IO.File]::ReadAllBytes($expectedPaths[$name])
            $liveHash = [BitConverter]::ToString([System.Security.Cryptography.SHA256]::HashData($liveBytes))
            $diskHash = [BitConverter]::ToString([System.Security.Cryptography.SHA256]::HashData($diskBytes))
            if ($liveHash -cne $diskHash) { $name }
        }
    )
    if ($driftedKeys.Count -gt 0) {
        Write-Warn "pandora-config Secret 内容与磁盘不一致($($driftedKeys.Count)/$expectedServiceCount 项:$($driftedKeys -join ', ')),apply 未收敛;改用 replace 强制覆盖。"
        $manifest | kubectl @kubectlContextArgs replace -f -
        Assert-LastExit "$Action(内容漂移 → replace 强制覆盖)"
        $secret = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', 'secret/pandora-config', '-n', $K8sNamespace, '-o', 'json') `
            -Action '复验 pandora-config Secret'
        $stillDrifted = @(
            foreach ($name in $expectedNames) {
                $liveBytes = [Convert]::FromBase64String([string]$secret.data.$name)
                $diskBytes = [System.IO.File]::ReadAllBytes($expectedPaths[$name])
                $liveHash = [BitConverter]::ToString([System.Security.Cryptography.SHA256]::HashData($liveBytes))
                $diskHash = [BitConverter]::ToString([System.Security.Cryptography.SHA256]::HashData($diskBytes))
                if ($liveHash -cne $diskHash) { $name }
            }
        )
        if ($stillDrifted.Count -gt 0) {
            throw "pandora-config Secret 内容在 replace 后仍与磁盘不一致:$($stillDrifted -join ', ')。" +
                  '继续 rollout 只会让 Pod 加载陈旧配置,已中止。'
        }
        Write-Ok "pandora-config Secret 已强制收敛到磁盘内容($expectedServiceCount/$expectedServiceCount 项逐字节一致)。"
    }
}

# player 的等级经验表从独立 ConfigMap 挂载；只收录 manifest 声明的精确批次文件。
# 该对象不含密钥，不混入 pandora-config Secret 的 22 份服务 YAML 契约。
# 发布纪律与 configtable_publish.ps1 一致:先把候选完整读入内存快照并校验 checksum/rows/语义，
# 再按 version 单调 + resourceVersion CAS 写固定 ConfigMap；同版本不同内容与低版本均拒绝。
# ConfigMap 一旦升版就只向前收敛:player 进程内 Store 拒绝降版，失败时若只回滚文件会造成
# 文件与已启动进程的快照分裂。因此 rollout 失败保留候选批次，重跑同版本继续完成收敛。

function Get-PandoraConfigTableSha256Hex([byte[]]$Bytes) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try { return ([BitConverter]::ToString($sha.ComputeHash($Bytes))).Replace('-', '').ToLowerInvariant() }
    finally { $sha.Dispose() }
}

function ConvertTo-PandoraConfigTableUInt32([object]$Value, [string]$What) {
    if ($null -eq $Value) { return [uint32]0 }
    $text = ([string]$Value).Trim()
    if ($text -cnotmatch '^(?:0|[1-9][0-9]*)$') { throw "$What 必须是 uint32,实为 '$text'" }
    try {
        $number = [uint64]::Parse($text, [Globalization.NumberStyles]::None, [Globalization.CultureInfo]::InvariantCulture)
    } catch { throw "$What 必须是 uint32,实为 '$text'" }
    if ($number -gt [uint32]::MaxValue) { throw "$What 超过 uint32 上限:$number" }
    return [uint32]$number
}

function ConvertTo-PandoraConfigTableJsonUInt64 {
    param(
        [object]$Value,
        [string]$What,
        [uint64]$Max = [uint64]::MaxValue
    )
    if ($null -eq $Value) { throw "$What 缺失" }
    if ($Value -is [System.Numerics.BigInteger]) {
        $bigMax = [System.Numerics.BigInteger]::Parse([string]$Max, [Globalization.CultureInfo]::InvariantCulture)
        if ($Value -lt [System.Numerics.BigInteger]::Zero -or $Value -gt $bigMax) {
            throw "$What 超过无符号整数范围 0..${Max}:$Value"
        }
        return [uint64]$Value
    }
    $typeCode = [Type]::GetTypeCode($Value.GetType())
    $integerTypes = @(
        [TypeCode]::Byte, [TypeCode]::SByte, [TypeCode]::Int16, [TypeCode]::UInt16,
        [TypeCode]::Int32, [TypeCode]::UInt32, [TypeCode]::Int64, [TypeCode]::UInt64
    )
    if ($typeCode -notin $integerTypes) {
        throw "$What 必须是 JSON 整数,实为 $($Value.GetType().FullName)"
    }
    if ($typeCode -in @([TypeCode]::SByte, [TypeCode]::Int16, [TypeCode]::Int32, [TypeCode]::Int64) -and
        [int64]$Value -lt 0) {
        throw "$What 必须是非负 JSON 整数,实为 $Value"
    }
    try { $number = [uint64]$Value }
    catch { throw "$What 超出 uint64:$Value" }
    if ($number -gt $Max) { throw "$What 超过上限 ${Max}:$number" }
    return $number
}

function Assert-PandoraConfigTableCurrentSemantics([System.Collections.IDictionary]$TableDocs) {
    foreach ($required in @('level', 'player_level_exp')) {
        if (-not $TableDocs.Contains($required)) { throw "pandora-configtable 缺少当前二进制必需表:$required" }
    }

    $levelDoc = $TableDocs['level']
    $levelRowsProperty = $levelDoc.PSObject.Properties['rows']
    $levelRows = if ($null -eq $levelRowsProperty -or $null -eq $levelRowsProperty.Value) { @() } else { @($levelRowsProperty.Value) }
    if ($levelRows.Count -eq 0) { throw 'level 表为空。' }
    $levelIDs = @{}
    foreach ($row in $levelRows) {
        $id = ConvertTo-PandoraConfigTableUInt32 $row.id 'level.id'
        if ($id -eq 0 -or $levelIDs.ContainsKey([string]$id)) { throw "level.id 非法或重复:$id" }
        $levelIDs[[string]$id] = $true
        if ([string]::IsNullOrWhiteSpace([string]$row.asset_path)) { throw "level id=$id asset_path 为空" }
        if ((ConvertTo-PandoraConfigTableUInt32 $row.category "level id=$id category") -eq 0) {
            throw "level id=$id category 未填"
        }
    }

    $playerDoc = $TableDocs['player_level_exp']
    $playerRowsProperty = $playerDoc.PSObject.Properties['rows']
    $playerRows = if ($null -eq $playerRowsProperty -or $null -eq $playerRowsProperty.Value) { @() } else { @($playerRowsProperty.Value) }
    if ($playerRows.Count -lt 2 -or $playerRows.Count -gt 200) {
        throw "player_level_exp 等级数必须在 [2,200],实为 $($playerRows.Count)"
    }
    $byLevel = @{}
    foreach ($row in $playerRows) {
        $id = ConvertTo-PandoraConfigTableUInt32 $row.id 'player_level_exp.id'
        $level = ConvertTo-PandoraConfigTableUInt32 $row.level "player_level_exp id=$id level"
        if ($id -eq 0 -or $id -ne $level -or $byLevel.ContainsKey([string]$id)) {
            throw "player_level_exp id=$id/level=$level 非法、不一致或重复"
        }
        $byLevel[[string]$id] = $row
    }
    [uint64]$expectedCumulative = 0
    for ([uint32]$level = 1; $level -le [uint32]$playerRows.Count; $level++) {
        $key = [string]$level
        if (-not $byLevel.ContainsKey($key)) { throw "player_level_exp 缺少 Lv$level" }
        $row = $byLevel[$key]
        $upgrade = ConvertTo-PandoraConfigTableUInt32 $row.upgrade_exp "player_level_exp Lv$level upgrade_exp"
        $cumulative = ConvertTo-PandoraConfigTableUInt32 $row.cumulative_exp "player_level_exp Lv$level cumulative_exp"
        if ([uint64]$cumulative -ne $expectedCumulative) {
            throw "player_level_exp Lv$level cumulative_exp=$cumulative,期望 $expectedCumulative"
        }
        if ($level -eq [uint32]$playerRows.Count) {
            if ($upgrade -ne 0) { throw "player_level_exp 末级 Lv$level upgrade_exp 必须为 0" }
            continue
        }
        if ($upgrade -eq 0) { throw "player_level_exp 非末级 Lv$level upgrade_exp 必须大于 0" }
        $expectedCumulative += [uint64]$upgrade
        if ($expectedCumulative -gt [uint32]::MaxValue) {
            throw "player_level_exp Lv$level 后累计经验超过 uint32 上限:$expectedCumulative"
        }
    }
}

function Get-PandoraConfigTableCandidate {
    param([string]$ConfigTableDir = (Join-Path $ProjectRoot 'configtable/dist'))

    $dir = [System.IO.Path]::GetFullPath($ConfigTableDir)
    $manifestPath = Join-Path $dir 'manifest.json'
    if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
        throw "pandora-configtable 缺少 manifest:$manifestPath"
    }
    $strictUtf8 = [System.Text.UTF8Encoding]::new($false, $true)
    try { $manifestText = $strictUtf8.GetString([System.IO.File]::ReadAllBytes($manifestPath)) }
    catch { throw "pandora-configtable manifest 不是合法 UTF-8:$($_.Exception.Message)" }
    try { $manifest = $manifestText | ConvertFrom-Json -ErrorAction Stop }
    catch { throw "pandora-configtable manifest 非法:$($_.Exception.Message)" }

    $version = ConvertTo-PandoraConfigTableJsonUInt64 $manifest.version 'pandora-configtable version'
    if ($version -eq 0) { throw 'pandora-configtable version 必须大于 0' }
    $null = ConvertTo-PandoraConfigTableJsonUInt64 $manifest.generated_at_ms 'pandora-configtable generated_at_ms'
    if ($manifest.generator -isnot [string] -or [string]::IsNullOrWhiteSpace([string]$manifest.generator)) {
        throw 'pandora-configtable generator 必须是非空 JSON 字符串'
    }
    if ($manifest.source_rev -isnot [string]) { throw 'pandora-configtable source_rev 必须是 JSON 字符串' }
    $sourceRev = ([string]$manifest.source_rev).Trim()
    if ([string]::IsNullOrWhiteSpace($sourceRev) -or $sourceRev -ieq 'unknown') {
        throw "pandora-configtable source_rev 不可追溯:'$sourceRev'"
    }
    if ($manifest.tables -isnot [System.Array]) { throw 'pandora-configtable manifest.tables 必须是 JSON 数组' }
    $tables = @($manifest.tables)
    if ($tables.Count -eq 0) { throw 'pandora-configtable manifest.tables 为空。' }

    $data = [ordered]@{ 'manifest.json' = $manifestText }
    $tableDocs = [ordered]@{}
    $seen = @{}
    foreach ($table in $tables) {
        foreach ($field in @('name', 'file', 'proto', 'checksum')) {
            if ($table.$field -isnot [string]) { throw "pandora-configtable manifest 表字段 $field 必须是 JSON 字符串" }
        }
        $name = ([string]$table.name).Trim()
        $file = ([string]$table.file).Trim()
        if ($name -cnotmatch '^[a-z0-9_]+$' -or $seen.ContainsKey($name)) {
            throw "pandora-configtable manifest 表名非法或重复:'$name'"
        }
        $seen[$name] = $true
        if ($file -cne "$name.json" -or [System.IO.Path]::GetFileName($file) -cne $file) {
            throw "pandora-configtable 表 $name 的 file 非法:'$file'"
        }
        if ([string]::IsNullOrWhiteSpace([string]$table.proto)) { throw "pandora-configtable 表 $name 缺 proto 全名" }
        $declaredRows = ConvertTo-PandoraConfigTableJsonUInt64 $table.rows "pandora-configtable 表 $name rows" ([uint32]::MaxValue)
        if ($declaredRows -eq 0) { throw "pandora-configtable 表 $name rows 必须大于 0" }
        $checksum = ([string]$table.checksum).Trim()
        if ($checksum -cnotmatch '^sha256:[0-9a-f]{64}$') { throw "pandora-configtable 表 $name checksum 非法:$checksum" }

        $path = Join-Path $dir $file
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "pandora-configtable 缺少表文件:$path" }
        $bytes = [System.IO.File]::ReadAllBytes($path)
        $actualChecksum = 'sha256:' + (Get-PandoraConfigTableSha256Hex $bytes)
        if ($actualChecksum -cne $checksum) {
            throw "pandora-configtable 表 $name checksum 不匹配:声明=$checksum 实际=$actualChecksum"
        }
        try { $text = $strictUtf8.GetString($bytes) }
        catch { throw "pandora-configtable 表 $name 不是合法 UTF-8:$($_.Exception.Message)" }
        try { $doc = $text | ConvertFrom-Json -ErrorAction Stop }
        catch { throw "pandora-configtable 表 $name JSON 非法:$($_.Exception.Message)" }
        $rowsProperty = $doc.PSObject.Properties['rows']
        $actualRows = if ($null -eq $rowsProperty -or $null -eq $rowsProperty.Value) { 0 } else { @($rowsProperty.Value).Count }
        if ($actualRows -ne $declaredRows) {
            throw "pandora-configtable 表 $name 行数 $actualRows 与 manifest 声明 $declaredRows 不一致"
        }
        $data[$file] = $text
        $tableDocs[$name] = $doc
    }
    $actualNames = @(Get-ChildItem -LiteralPath $dir -File -Filter '*.json' | ForEach-Object Name | Sort-Object)
    $expectedNames = @($data.Keys | Sort-Object)
    $keyDiff = @(Compare-Object -ReferenceObject $expectedNames -DifferenceObject $actualNames -CaseSensitive)
    if ($keyDiff.Count -ne 0) { throw "pandora-configtable JSON 文件集与 manifest 不一致:$($keyDiff | Out-String)" }
    Assert-PandoraConfigTableCurrentSemantics $tableDocs
    return [pscustomobject]@{
        Version = $version
        SourceRev = $sourceRev
        Data = $data
        Directory = $dir
    }
}

function Assert-PandoraConfigTableDataEqual {
    param(
        [System.Collections.IDictionary]$Expected,
        [object]$Actual,
        [string]$What
    )
    $expectedNames = @($Expected.Keys | Sort-Object)
    $actualNames = if ($null -eq $Actual) { @() } else { @($Actual.PSObject.Properties.Name | Sort-Object) }
    $diff = @(Compare-Object -ReferenceObject $expectedNames -DifferenceObject $actualNames -CaseSensitive)
    if ($diff.Count -ne 0) { throw "$What key 集不精确:$($diff | Out-String)" }
    foreach ($name in $expectedNames) {
        $property = $Actual.PSObject.Properties[$name]
        if ($null -eq $property -or [string]$Expected[$name] -cne [string]$property.Value) {
            throw "$What 文件 $name 内容与候选批次不一致"
        }
    }
}

function Assert-PandoraConfigTableSameVersionCompatible {
    param(
        [System.Collections.IDictionary]$Expected,
        [object]$Actual,
        [string]$What
    )
    $expectedNames = @($Expected.Keys | Sort-Object)
    $actualNames = if ($null -eq $Actual) { @() } else { @($Actual.PSObject.Properties.Name | Sort-Object) }
    $diff = @(Compare-Object -ReferenceObject $expectedNames -DifferenceObject $actualNames -CaseSensitive)
    if ($diff.Count -ne 0) { throw "$What key 集不精确:$($diff | Out-String)" }
    foreach ($name in $expectedNames) {
        if ($name -ceq 'manifest.json') { continue }
        $property = $Actual.PSObject.Properties[$name]
        if ($null -eq $property -or [string]$Expected[$name] -cne [string]$property.Value) {
            throw "$What 文件 $name 内容不同;同版本只允许纠正 manifest 溯源字段"
        }
    }

    try { $expectedManifest = ([string]$Expected['manifest.json']) | ConvertFrom-Json -ErrorAction Stop }
    catch { throw "$What 候选 manifest.json 非法:$($_.Exception.Message)" }
    try { $actualManifest = ([string]$Actual.PSObject.Properties['manifest.json'].Value) | ConvertFrom-Json -ErrorAction Stop }
    catch { throw "$What live manifest.json 非法:$($_.Exception.Message)" }
    $topLevelFields = @('generated_at_ms', 'generator', 'source_rev', 'tables', 'version')
    foreach ($item in @(@{ Name = '候选'; Manifest = $expectedManifest }, @{ Name = 'live'; Manifest = $actualManifest })) {
        $fields = @($item.Manifest.PSObject.Properties.Name | Sort-Object)
        $fieldDiff = @(Compare-Object -ReferenceObject $topLevelFields -DifferenceObject $fields -CaseSensitive)
        if ($fieldDiff.Count -ne 0) { throw "$What $($item.Name) manifest 字段集异常:$($fieldDiff | Out-String)" }
        if ($item.Manifest.tables -isnot [System.Array]) { throw "$What $($item.Name) manifest.tables 必须是 JSON 数组" }
    }
    $expectedVersion = ConvertTo-PandoraConfigTableJsonUInt64 $expectedManifest.version "$What 候选 manifest.version"
    $actualVersion = ConvertTo-PandoraConfigTableJsonUInt64 $actualManifest.version "$What live manifest.version"
    $null = ConvertTo-PandoraConfigTableJsonUInt64 $expectedManifest.generated_at_ms "$What 候选 manifest.generated_at_ms"
    $null = ConvertTo-PandoraConfigTableJsonUInt64 $actualManifest.generated_at_ms "$What live manifest.generated_at_ms"
    foreach ($item in @(@{ Name = '候选'; Manifest = $expectedManifest }, @{ Name = 'live'; Manifest = $actualManifest })) {
        foreach ($field in @('generator', 'source_rev')) {
            if ($item.Manifest.$field -isnot [string]) { throw "$What $($item.Name) manifest.$field 必须是 JSON 字符串" }
        }
    }
    if ($expectedVersion -ne $actualVersion) {
        throw "$What manifest version 不同"
    }

    $tableFields = @('checksum', 'file', 'name', 'proto', 'rows')
    $expectedByName = @{}
    $actualByName = @{}
    foreach ($side in @(
        @{ Name = '候选'; Tables = @($expectedManifest.tables); Map = $expectedByName },
        @{ Name = 'live'; Tables = @($actualManifest.tables); Map = $actualByName }
    )) {
        foreach ($table in $side.Tables) {
            $fields = @($table.PSObject.Properties.Name | Sort-Object)
            $fieldDiff = @(Compare-Object -ReferenceObject $tableFields -DifferenceObject $fields -CaseSensitive)
            if ($fieldDiff.Count -ne 0) { throw "$What $($side.Name) manifest.tables 字段集异常:$($fieldDiff | Out-String)" }
            foreach ($field in @('name', 'file', 'proto', 'checksum')) {
                if ($table.$field -isnot [string]) { throw "$What $($side.Name) manifest.tables.$field 必须是 JSON 字符串" }
            }
            $null = ConvertTo-PandoraConfigTableJsonUInt64 $table.rows "$What $($side.Name) manifest.tables.rows" ([uint32]::MaxValue)
            $name = [string]$table.name
            if ([string]::IsNullOrWhiteSpace($name) -or $side.Map.ContainsKey($name)) {
                throw "$What $($side.Name) manifest.tables 表名为空或重复:'$name'"
            }
            $side.Map[$name] = $table
        }
    }
    $tableNameDiff = @(Compare-Object -ReferenceObject @($expectedByName.Keys | Sort-Object) `
        -DifferenceObject @($actualByName.Keys | Sort-Object) -CaseSensitive)
    if ($tableNameDiff.Count -ne 0) { throw "$What manifest.tables 表名集不同:$($tableNameDiff | Out-String)" }
    foreach ($name in $expectedByName.Keys) {
        foreach ($field in @('file', 'proto', 'checksum')) {
            if ([string]$expectedByName[$name].$field -cne [string]$actualByName[$name].$field) {
                throw "$What manifest.tables[$name].$field 运行语义不同;必须生成更高版本"
            }
        }
        $expectedRows = ConvertTo-PandoraConfigTableJsonUInt64 $expectedByName[$name].rows `
            "$What 候选 manifest.tables[$name].rows" ([uint32]::MaxValue)
        $actualRows = ConvertTo-PandoraConfigTableJsonUInt64 $actualByName[$name].rows `
            "$What live manifest.tables[$name].rows" ([uint32]::MaxValue)
        if ($expectedRows -ne $actualRows) {
            throw "$What manifest.tables[$name].rows 运行语义不同;必须生成更高版本"
        }
    }
}

function Get-PandoraConfigTableVersionFromData([object]$Data, [string]$What) {
    if ($null -eq $Data -or $null -eq $Data.PSObject.Properties['manifest.json']) {
        throw "$What 缺少 manifest.json"
    }
    try { $manifest = ([string]$Data.PSObject.Properties['manifest.json'].Value) | ConvertFrom-Json -ErrorAction Stop }
    catch { throw "$What manifest.json 非法:$($_.Exception.Message)" }
    $version = ConvertTo-PandoraConfigTableJsonUInt64 $manifest.version "$What version"
    if ($version -eq 0) { throw "$What version 必须大于 0" }
    return $version
}

function New-PandoraConfigTableConfigMapObject {
    param(
        [System.Collections.IDictionary]$Data,
        [string]$ResourceVersion = ''
    )
    $metadata = [ordered]@{ name = 'pandora-configtable'; namespace = $K8sNamespace }
    if (-not [string]::IsNullOrWhiteSpace($ResourceVersion)) { $metadata.resourceVersion = $ResourceVersion }
    return [ordered]@{ apiVersion = 'v1'; kind = 'ConfigMap'; metadata = $metadata; data = $Data }
}

function Apply-PandoraConfigTableConfigMap {
    param(
        [string]$KubeContext,
        [string]$Action,
        [object]$Candidate = $null,
        [string]$ConfigTableDir = ''
    )
    if ($null -eq $Candidate) {
        if ([string]::IsNullOrWhiteSpace($ConfigTableDir)) { $ConfigTableDir = Join-Path $ProjectRoot 'configtable/dist' }
        $Candidate = Get-PandoraConfigTableCandidate -ConfigTableDir $ConfigTableDir
    }

    $kubectlContextArgs = @('--context', $KubeContext)
    $liveLines = @(& kubectl @kubectlContextArgs get configmap/pandora-configtable -n $K8sNamespace --ignore-not-found -o json 2>$null)
    if ($LASTEXITCODE -ne 0) { throw "$Action(读取 live ConfigMap)失败(exit=$LASTEXITCODE)" }
    $liveText = (($liveLines | ForEach-Object { $_.ToString() }) -join "`n").Trim()
    $live = $null
    if (-not [string]::IsNullOrWhiteSpace($liveText)) {
        try { $live = $liveText | ConvertFrom-Json -ErrorAction Stop }
        catch { throw "$Action(live ConfigMap JSON 非法):$($_.Exception.Message)" }
        if ($null -ne $live.binaryData -and $live.binaryData.PSObject.Properties.Count -gt 0) {
            throw "$Action:live ConfigMap 含未管理的 binaryData,拒绝覆盖。"
        }
        $liveVersion = Get-PandoraConfigTableVersionFromData $live.data 'live pandora-configtable'
        if ([uint64]$Candidate.Version -lt $liveVersion) {
            throw "$Action:候选 version $($Candidate.Version) 低于 live $liveVersion,拒绝回退。"
        }
        if ([uint64]$Candidate.Version -eq $liveVersion) {
            try {
                Assert-PandoraConfigTableDataEqual -Expected $Candidate.Data -Actual $live.data `
                    -What "pandora-configtable 同版本 v$liveVersion"
                Write-Ok "pandora-configtable 已是相同批次 v$liveVersion,ConfigMap no-op。"
                return $Candidate
            } catch {
                Assert-PandoraConfigTableSameVersionCompatible -Expected $Candidate.Data -Actual $live.data `
                    -What "pandora-configtable 同版本 v$liveVersion"
                $candidateManifest = ([string]$Candidate.Data['manifest.json']) | ConvertFrom-Json
                $liveManifest = ([string]$live.data.PSObject.Properties['manifest.json'].Value) | ConvertFrom-Json
                if ([string]$candidateManifest.source_rev -ceq [string]$liveManifest.source_rev -and
                    [string]$candidateManifest.generator -ceq [string]$liveManifest.generator) {
                    Write-Ok "pandora-configtable 已是相同运行批次 v$liveVersion;仅 manifest 格式/时间不同,ConfigMap no-op。"
                    return $Candidate
                }
                Write-Warn "pandora-configtable v$liveVersion 表内容未变,将以 CAS 同步 manifest source_rev/generator 溯源修正(并携带候选 generated_at_ms)。"
            }
        }
    }

    $resourceVersion = if ($null -eq $live) { '' } else { [string]$live.metadata.resourceVersion }
    $previousUID = if ($null -eq $live) { '' } else { [string]$live.metadata.uid }
    if ($null -ne $live -and [string]::IsNullOrWhiteSpace($resourceVersion)) {
        throw "$Action:live ConfigMap resourceVersion 为空,无法 CAS 更新。"
    }
    if ($null -ne $live -and [string]::IsNullOrWhiteSpace($previousUID)) {
        throw "$Action:live ConfigMap UID 为空,无法校验对象身份。"
    }
    $object = New-PandoraConfigTableConfigMapObject -Data $Candidate.Data -ResourceVersion $resourceVersion
    $objectJson = $object | ConvertTo-Json -Depth 10
    if ($null -eq $live) {
        $writeLines = @($objectJson | kubectl @kubectlContextArgs create -f - -o json 2>$null)
        if ($LASTEXITCODE -ne 0) {
            throw "$Action(create-only)失败(exit=$LASTEXITCODE),写入结果可能未知;禁止回退,请以同一候选批次重跑。"
        }
    } else {
        $writeLines = @($objectJson | kubectl @kubectlContextArgs replace -f - -o json 2>$null)
        if ($LASTEXITCODE -ne 0) {
            throw "$Action(resourceVersion CAS replace)失败(exit=$LASTEXITCODE),写入结果可能未知;禁止回退,请以同一候选批次重跑。"
        }
    }

    $writeText = (($writeLines | ForEach-Object { $_.ToString() }) -join "`n").Trim()
    if ([string]::IsNullOrWhiteSpace($writeText)) { throw "$Action:写入成功但 kubectl 未返回对象 JSON。" }
    try { $applied = $writeText | ConvertFrom-Json -ErrorAction Stop }
    catch { throw "$Action:写入返回非法 JSON:$($_.Exception.Message)" }
    $appliedUID = [string]$applied.metadata.uid
    if ([string]::IsNullOrWhiteSpace($appliedUID)) { throw "$Action:写入返回的 ConfigMap UID 为空。" }
    if ($null -ne $live -and $appliedUID -cne $previousUID) {
        throw "$Action:replace 返回 UID=$appliedUID,写前 UID=$previousUID,对象身份已变化。"
    }
    Assert-PandoraConfigTableDataEqual -Expected $Candidate.Data -Actual $applied.data `
        -What "pandora-configtable 写入返回 v$($Candidate.Version)"

    $current = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'configmap/pandora-configtable', '-n', $K8sNamespace, '-o', 'json') `
        -Action '回读 pandora-configtable ConfigMap'
    if ([string]$current.metadata.uid -cne $appliedUID) {
        throw "$Action:写后回读 UID=$([string]$current.metadata.uid),写入返回 UID=$appliedUID,对象已被并发替换。"
    }
    Assert-PandoraConfigTableDataEqual -Expected $Candidate.Data -Actual $current.data `
        -What "pandora-configtable 写后 v$($Candidate.Version)"
    Write-Ok "pandora-configtable 已前向 CAS 切到 v$($Candidate.Version)(source_rev=$($Candidate.SourceRev));rollout 失败时保留本批次供同版本重跑。"
    return $Candidate
}

function Get-KubectlJsonObject {
    param(
        [string]$KubeContext,
        [string[]]$Arguments,
        [string]$Action
    )

    # stderr 不并入 JSON，避免 kubectl warning 污染解析；退出码仍严格检查。
    $raw = @(& kubectl --context $KubeContext @Arguments 2>$null)
    if ($LASTEXITCODE -ne 0) { throw "$Action 失败(exit=$LASTEXITCODE)" }
    $text = (($raw | ForEach-Object { $_.ToString() }) -join "`n").Trim()
    if ([string]::IsNullOrWhiteSpace($text)) { throw "$Action 返回空 JSON" }
    try { return ($text | ConvertFrom-Json -ErrorAction Stop) }
    catch { throw "$Action 返回非法 JSON:$($_.Exception.Message)" }
}

# ===== DSTicket v2(方案 B)本地 dev 钥料自举 =====
# 生成/复用 dev RSA 钥对(私钥落 run/,不入版本库,见 tools/dsticketkeys 头注),并把:
#   - 私钥 → immutable Secret pandora-dsticket-signer-r1(ns pandora，四个签发方经 services.yaml
#     挂到稳定路径 /run/secrets/pandora-dsticket)；
#   - 公钥 JWKS → 两份同 hash immutable ConfigMap pandora-dsticket-jwks-r1：default 给 Fleet，
#     pandora 给 Login 的兼容/诊断 verifier。
# 返回由 private.pem 推导、并与 JWKS 中同 kid 公钥参数对账后的 active kid；绝不依赖 keys[0]。
# 线上不走本函数:真钥对由受控密钥管线生成,按 deploy/k8s/agones/15-dsticket-jwks.yaml 的纪律建
# 不可变 Secret/ConfigMap；dev 同样只 create missing + 回读对账，绝不原地覆盖。
function Ensure-DsTicketDevKeyMaterial {
    param([Parameter(Mandatory = $true)][string]$KubeContext)
    $kubectlContextArgs = @('--context', $KubeContext)
    $keyDir = Join-Path $ProjectRoot 'run/cluster/dsticket'
    $privPath = Join-Path $keyDir 'private.pem'
    $jwksPath = Join-Path $keyDir 'jwks.json'
    if (-not ((Test-Path -LiteralPath $privPath -PathType Leaf) -and (Test-Path -LiteralPath $jwksPath -PathType Leaf))) {
        # 半套残留(只有其一)时 dsticketkeys 会拒绝覆盖 —— 这是刻意的:密钥文件不自动删,
        # 请人工确认后移走残件再重跑(防脚本静默换钥导致在跑集群票据全废)。
        Write-Info "生成 DSTicket v2 dev 钥对(run/cluster/dsticket,revision=1)..."
        Push-Location $ProjectRoot
        try {
            # Native stdout must not escape this function: callers assign the sole return value as
            # DsTicketActiveKid.  A fresh bootstrap otherwise returns [generator output, kid] and
            # PowerShell cannot bind that array to gen_cluster_config.ps1's string parameter.
            & go run ./tools/dsticketkeys -out $keyDir -revision 1 | Out-Host
            Assert-LastExit 'dsticketkeys 生成 dev 钥对'
        } finally {
            Pop-Location
        }
    }
    $privatePem = Get-Content -LiteralPath $privPath -Raw
    $jwksText = Get-Content -LiteralPath $jwksPath -Raw
    $keyContract = Get-PandoraDSTicketKeyMaterialContract -PrivateKeyPem $privatePem -JwksText $jwksText -ExpectedRevision 1
    $kid = $keyContract.ActiveKid
    $keysetAnnotations = [ordered]@{
        'pandora.dev/dsticket-active-kid' = $keyContract.ActiveKid
        'pandora.dev/dsticket-keyset-revision' = '1'
        'pandora.dev/dsticket-jwks-sha256' = $keyContract.JwksSha256
    }
    $signerAnnotations = [ordered]@{
        'pandora.dev/dsticket-signer-kid' = $keyContract.SignerKid
        'pandora.dev/dsticket-signer-revision' = '1'
        'pandora.dev/dsticket-private-pem-sha256' = $keyContract.PrivatePemSha256
    }
    $labels = [ordered]@{
        'app.kubernetes.io/part-of' = 'pandora'
        'app.kubernetes.io/component' = 'dsticket-keyset'
    }
    function New-LocalDSTicketObjectIfMissing([object]$Object, [string]$KindName, [string]$Namespace) {
        $existing = @(& kubectl @kubectlContextArgs get $KindName -n $Namespace --ignore-not-found -o name 2>&1)
        if ($LASTEXITCODE -ne 0) { throw "读取 $Namespace/$KindName 失败:$($existing -join [Environment]::NewLine)" }
        if ([string]::IsNullOrWhiteSpace((($existing | ForEach-Object { $_.ToString() }) -join '').Trim())) {
            $json = $Object | ConvertTo-Json -Depth 20 -Compress
            $created = @($json | & kubectl @kubectlContextArgs create -f - 2>&1)
            if ($LASTEXITCODE -ne 0) { throw "create-only 创建 $Namespace/$KindName 失败:$($created -join [Environment]::NewLine)" }
        }
    }

    # 开发集群也遵守 create-only + immutable：已有对象绝不 apply/patch，防止重跑
    # start 时静默换钥。ConfigMap 是 namespaced 对象，default 供 Fleet/DS，pandora 供 Login verifier；
    # 两份公钥对象的内容/hash/active kid 必须完全相同。
    $secretObject = [ordered]@{
        apiVersion = 'v1'; kind = 'Secret'; immutable = $true; type = 'Opaque'
        metadata = [ordered]@{ name = 'pandora-dsticket-signer-r1'; namespace = $K8sNamespace; labels = $labels; annotations = $signerAnnotations }
        data = [ordered]@{ 'private.pem' = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($privatePem)) }
    }
    New-LocalDSTicketObjectIfMissing $secretObject 'secret/pandora-dsticket-signer-r1' $K8sNamespace
    foreach ($namespace in @('default', $K8sNamespace)) {
        $cmObject = [ordered]@{
            apiVersion = 'v1'; kind = 'ConfigMap'; immutable = $true
            metadata = [ordered]@{ name = 'pandora-dsticket-jwks-r1'; namespace = $namespace; labels = $labels; annotations = $keysetAnnotations }
            data = [ordered]@{ 'jwks.json' = $jwksText }
        }
        New-LocalDSTicketObjectIfMissing $cmObject 'configmap/pandora-dsticket-jwks-r1' $namespace
    }
    try {
        $liveSecret = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', 'secret/pandora-dsticket-signer-r1', '-n', $K8sNamespace, '-o', 'json') -Action '回读 dev DSTicket revisioned signer 私钥'
        foreach ($namespace in @('default', $K8sNamespace)) {
            $liveCM = Get-KubectlJsonObject -KubeContext $KubeContext `
                -Arguments @('get', 'configmap/pandora-dsticket-jwks-r1', '-n', $namespace, '-o', 'json') -Action "回读 $namespace DSTicket JWKS"
            $null = Assert-PandoraDSTicketKubernetesObjects -SecretObject $liveSecret -ConfigMapObject $liveCM `
                -ExpectedRevision 1 -ExpectedActiveKid $kid -ExpectedConfigMapNamespace $namespace
        }
    } catch {
        throw "本地集群存在旧版/漂移的 DSTicket 对象，拒绝原地覆盖:$($_.Exception.Message) 请显式用 -Mode k8s -Reset 重建开发集群。"
    }
    Write-Ok "DSTicket v2 dev 钥料就绪(kid=$kid,signer=r1,keyset=r1；immutable JWKS=default+pandora)。"
    return $kid
}

# 本地 minikube 的 DS callback Model-B 基线。业务配置使用集群 DNS
# etcd.pandora.svc:2379；启动器从宿主做线性预检/CAS 时通过临时 port-forward。
$script:LocalDsFenceEndpoint = 'etcd.pandora.svc:2379'
$script:LocalDsFenceKeysetRevision = 'pandora-ds-auth-v2-local-r1'
$script:LocalFreshDsAuthMarkerName = 'pandora-local-fresh-dsauth-genesis-v1'
$script:LocalFreshDsAuthMarkerStateAnnotation = 'pandora.dev/dsauth-bootstrap-state'
$script:LocalFreshDsAuthMarkerPvcAnnotation = 'pandora.dev/dsauth-etcd-pvc-uid'
$script:LocalFreshDsAuthMarkerEvidenceCompletedAnnotation = 'pandora.dev/dsauth-evidence-completed-at-ms'
$script:LocalFreshDsAuthMarkerCompletedAnnotation = 'pandora.dev/dsauth-bootstrap-completed-at-ms'
$script:LocalFreshDsAuthContinuityTokenField = 'genesis_continuity_token'
$script:LocalFreshDsAuthEvidenceSha256 = 'sha256:cc675844fffad7d16bfaf31bcbc31a8f4d8bd8ffa0306c8e34d461161b573130'
$script:LocalFreshDsAuthOriginFresh = 'fresh-pandora-install-v1'
$script:LocalFreshDsAuthOriginAdopted = 'legacy-infra-only-adoption-v1'
$script:LocalLegacyAdoptionCohortFingerprintField = 'legacy_adoption_cohort_sha256'
$script:LocalLegacyAdoptionCohortMaxAgeMS = 2L * 60L * 60L * 1000L
$script:LocalLegacyAdoptionCohortMaxSpanMS = 10L * 60L * 1000L
$script:LocalObservedV3WitnessName = 'pandora-local-observed-dsauth-v3-v1'
$script:LocalFreshNamespaceAnchorSchemaAnnotation = 'pandora.dev/dsauth-fresh-anchor-schema'
$script:LocalFreshNamespaceAnchorProfileAnnotation = 'pandora.dev/dsauth-fresh-anchor-profile'
$script:LocalFreshNamespaceAnchorKubeSystemUidAnnotation = 'pandora.dev/dsauth-fresh-anchor-kube-system-uid'
$script:LocalFreshNamespaceAnchorEvidenceAnnotation = 'pandora.dev/dsauth-fresh-anchor-evidence-sha256'

function New-LocalGenesisContinuityToken {
    $bytes = [byte[]]::new(32)
    [Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    return 'nonce:' + [Convert]::ToHexString($bytes).ToLowerInvariant()
}

# `minikube profile list -o json` 的**唯一**解析入口。两个坑都是实测出来的:
#  ① minikube 会把诊断行写进 **stderr**,例如
#       E0804 08:42:43 status.go:457] kubeconfig endpoint: got: 127.0.0.1:54356, want: 127.0.0.1:60129
#     —— 只要本机有一个 kubeconfig endpoint 已过期的旧 profile(停过的集群很常见)就稳定出现,
#     且命令仍退出 0。谁用 `2>&1` 把它并进 stdout 再 ConvertFrom-Json,必炸
#     「Unexpected character encountered while parsing value: E」—— 一键停止就是死在这里。
#  ② 个别版本还会往 stdout 打前导提示行。
# 故统一:只取 stdout,并从第一个 '{' 截起。返回 JSON 文本(拿不到则返回空串);
# 退出码存进 $script:MinikubeProfileListExitCode,供需要 fail-fast 的调用方区分
# 「本机确实一个 profile 都没有」与「minikube 自身坏了」。
$script:MinikubeProfileListExitCode = 0
function Get-MinikubeProfileListJsonText {
    $text = ((& minikube profile list -o json 2>$null | ForEach-Object { $_.ToString() }) -join "`n")
    $script:MinikubeProfileListExitCode = $LASTEXITCODE
    $brace = $text.IndexOf('{')
    if ($brace -lt 0) { return '' }
    return $text.Substring($brace).Trim()
}

# 解析后的 profile 对象(valid + invalid 合并后的名字列表调用方自己取)。
# 拿不到/解析不了一律返回 $null——容错路径用,不做 fail-fast。
function Get-MinikubeProfileListObject {
    $text = Get-MinikubeProfileListJsonText
    if ([string]::IsNullOrWhiteSpace($text)) { return $null }
    try { return ($text | ConvertFrom-Json -ErrorAction Stop) } catch { return $null }
}

# 本机所有 minikube profile 名(读不到时返回空数组,不抛:调用方要能在「没装过集群」的机器上空跑)。
function Get-MinikubeProfileNames {
    $parsed = Get-MinikubeProfileListObject
    if ($null -eq $parsed) { return @() }
    return @(@($parsed.valid) + @($parsed.invalid) |
        ForEach-Object { [string]$_.Name } |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
        Sort-Object -Unique)
}

function Test-MinikubeProfileExists {
    param([Parameter(Mandatory = $true)][string]$Profile)
    $text = Get-MinikubeProfileListJsonText
    if ([string]::IsNullOrWhiteSpace($text)) {
        # 退出 0 + 无 JSON = 本机真的一个 profile 都没有(全新机器),这是合法状态。
        # 退出非 0 才是「minikube 不可用」——那时不能假装知道集群是否 fresh(会影响 DS auth genesis 判定)。
        if ($script:MinikubeProfileListExitCode -ne 0) {
            throw "minikube profile list 执行失败(exit=$script:MinikubeProfileListExitCode),不能安全判定是否 fresh cluster。"
        }
        return $false
    }
    try { $profiles = $text | ConvertFrom-Json -ErrorAction Stop }
    catch { throw "minikube profile list 返回非法 JSON:$($_.Exception.Message)" }
    foreach ($entry in @($profiles.valid) + @($profiles.invalid)) {
        if ([string]$entry.Name -ceq $Profile) { return $true }
    }
    return $false
}

function Test-KubernetesNamespaceExists {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$Namespace
    )
    $out = @(& kubectl --context $KubeContext get "namespace/$Namespace" --ignore-not-found -o name 2>&1)
    $flat = ((@($out) | ForEach-Object { $_.ToString() }) -join "`n").Trim()
    if ($LASTEXITCODE -ne 0) { throw "读取 namespace/$Namespace 状态失败:$flat" }
    if ([string]::IsNullOrWhiteSpace($flat)) { return $false }
    if ($flat -cne "namespace/$Namespace") { throw "读取 namespace/$Namespace 返回意外对象:$flat" }
    return $true
}

# Namespace 是 fresh 安装的第一个 Kubernetes 写入。若 namespace create 成功后进程恰好在
# namespaced marker create 前退出，单靠“下次看到 namespace 已存在”会丢失 fresh 事实。
# 因此首次 create Namespace 时把最小 anchor 作为同一个 API 对象的 annotations 原子落盘；
# 正式 immutable marker 建好并回读后立刻移除 anchor。namespace 被删除时 anchor 与 marker
# 一起消失，不会跨新 namespace UID 复用。
function Get-LocalFreshNamespaceAnchor {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$MinikubeProfile
    )
    $namespace = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', "namespace/$K8sNamespace", '-o', 'json') -Action '读取 fresh namespace anchor'
    $annotationsProperty = $namespace.metadata.PSObject.Properties['annotations']
    $annotations = if ($null -eq $annotationsProperty) { $null } else { $annotationsProperty.Value }
    function Read-AnchorAnnotation([string]$Name) {
        if ($null -eq $annotations) { return '' }
        $property = $annotations.PSObject.Properties[$Name]
        if ($null -eq $property) { return '' }
        return [string]$property.Value
    }
    $schema = Read-AnchorAnnotation $script:LocalFreshNamespaceAnchorSchemaAnnotation
    $profile = Read-AnchorAnnotation $script:LocalFreshNamespaceAnchorProfileAnnotation
    $kubeSystemUid = Read-AnchorAnnotation $script:LocalFreshNamespaceAnchorKubeSystemUidAnnotation
    $evidence = Read-AnchorAnnotation $script:LocalFreshNamespaceAnchorEvidenceAnnotation
    $values = @($schema, $profile, $kubeSystemUid, $evidence)
    if (@($values | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -eq 0) { return $false }
    $actualKubeSystemUid = Get-KubernetesNamespaceUid -KubeContext $KubeContext -Namespace 'kube-system'
    if ($schema -cne '1' -or $profile -cne $MinikubeProfile -or
        $kubeSystemUid -cne $actualKubeSystemUid -or $evidence -cne $script:LocalFreshDsAuthEvidenceSha256) {
        throw 'fresh namespace anchor 不完整或 profile/kube-system UID/evidence 漂移，拒绝自动 genesis。'
    }
    return $true
}

function New-LocalFreshAnchoredNamespace {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$MinikubeProfile
    )
    if (Test-KubernetesNamespaceExists -KubeContext $KubeContext -Namespace $K8sNamespace) {
        throw "create-only fresh namespace 前 namespace/$K8sNamespace 已存在。"
    }
    $kubeSystemUid = Get-KubernetesNamespaceUid -KubeContext $KubeContext -Namespace 'kube-system'
    $namespaceObject = [ordered]@{
        apiVersion = 'v1'; kind = 'Namespace'
        metadata = [ordered]@{
            name = $K8sNamespace
            labels = [ordered]@{ 'app.kubernetes.io/part-of' = 'pandora' }
            annotations = [ordered]@{
                $script:LocalFreshNamespaceAnchorSchemaAnnotation = '1'
                $script:LocalFreshNamespaceAnchorProfileAnnotation = $MinikubeProfile
                $script:LocalFreshNamespaceAnchorKubeSystemUidAnnotation = $kubeSystemUid
                $script:LocalFreshNamespaceAnchorEvidenceAnnotation = $script:LocalFreshDsAuthEvidenceSha256
            }
        }
    }
    $json = $namespaceObject | ConvertTo-Json -Depth 10 -Compress
    $created = @($json | & kubectl --context $KubeContext create -f - 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "create-only 创建带 fresh anchor 的 namespace/$K8sNamespace 失败:$($created -join [Environment]::NewLine)"
    }
    if (-not (Get-LocalFreshNamespaceAnchor -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile)) {
        throw 'fresh namespace 创建后 anchor 回读缺失。'
    }
    Write-Ok "namespace/$K8sNamespace 已 create-only 创建并原子携带 fresh anchor。"
}

function Remove-LocalFreshNamespaceAnchor {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$MinikubeProfile
    )
    $marker = Get-LocalFreshGenesisIntent -KubeContext $KubeContext
    if ($null -eq $marker) { throw '正式 fresh marker 尚未建立，禁止移除 namespace anchor。' }
    $null = Assert-LocalFreshGenesisIntent -Marker $marker -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile
    if (-not (Get-LocalFreshNamespaceAnchor -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile)) { return }
    $namespace = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', "namespace/$K8sNamespace", '-o', 'json') -Action '移除前回读 fresh namespace anchor'
    $patchObject = [ordered]@{ metadata = [ordered]@{
        resourceVersion = [string]$namespace.metadata.resourceVersion
        annotations = [ordered]@{
            $script:LocalFreshNamespaceAnchorSchemaAnnotation = $null
            $script:LocalFreshNamespaceAnchorProfileAnnotation = $null
            $script:LocalFreshNamespaceAnchorKubeSystemUidAnnotation = $null
            $script:LocalFreshNamespaceAnchorEvidenceAnnotation = $null
        }
    } }
    $patchJson = $patchObject | ConvertTo-Json -Depth 10 -Compress
    $patched = @(& kubectl --context $KubeContext patch "namespace/$K8sNamespace" --type merge -p $patchJson 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "移除 fresh namespace anchor 失败:$($patched -join [Environment]::NewLine)" }
    if (Get-LocalFreshNamespaceAnchor -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile) {
        throw 'fresh namespace anchor 移除后仍可回读，拒绝继续配置/infra。'
    }
    Write-Ok '正式 immutable marker 已建立；临时 namespace fresh anchor 已移除。'
}

# fresh DS-auth genesis 不能只依赖“本次 start 前 profile 不存在”这个内存判断：镜像下载或
# 基础设施启动若中途失败，下一次运行时 profile 已存在，启动器会失去 fresh 事实。这里在
# 新 namespace 创建后立刻写入 create-only + immutable intent，并把它绑定到本次 minikube
# profile、kube-system UID 和 pandora namespace UID。可变状态只放 annotation，通过
# resourceVersion CAS 按 preinfra -> pending -> complete 前进。
function Get-LocalFreshGenesisIntent {
    param([Parameter(Mandatory = $true)][string]$KubeContext)
    $lines = @(& kubectl --context $KubeContext get "configmap/$($script:LocalFreshDsAuthMarkerName)" `
        -n $K8sNamespace --ignore-not-found -o json 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "读取本地 fresh DS-auth marker 失败:$($lines -join [Environment]::NewLine)"
    }
    $text = (($lines | ForEach-Object { $_.ToString() }) -join "`n").Trim()
    if ([string]::IsNullOrWhiteSpace($text)) { return $null }
    try { return ($text | ConvertFrom-Json -ErrorAction Stop) }
    catch { throw "本地 fresh DS-auth marker 返回非法 JSON:$($_.Exception.Message)" }
}

function Get-KubernetesNamespaceUid {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$Namespace
    )
    $ns = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', "namespace/$Namespace", '-o', 'json') -Action "读取 namespace/$Namespace UID"
    $uid = [string]$ns.metadata.uid
    if ([string]::IsNullOrWhiteSpace($uid)) { throw "namespace/$Namespace 缺少 UID，拒绝建立 fresh 证据。" }
    return $uid
}

# 旧版本可能已经有 canonical V3、但没有 fresh marker。首次线性观察到该状态时写一个
# create-only terminal witness；以后同 namespace/PVC 若变成 missing，必须按数据丢失阻断，
# 不能在 legacy cohort 时间窗内再次解释成未完成安装。
function Get-LocalObservedV3Witness {
    param([Parameter(Mandatory = $true)][string]$KubeContext)
    $lines = @(& kubectl --context $KubeContext get "configmap/$($script:LocalObservedV3WitnessName)" `
        -n $K8sNamespace --ignore-not-found -o json 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "读取 observed V3 witness 失败:$($lines -join [Environment]::NewLine)" }
    $text = (($lines | ForEach-Object { $_.ToString() }) -join "`n").Trim()
    if ([string]::IsNullOrWhiteSpace($text)) { return $null }
    try { return ($text | ConvertFrom-Json -ErrorAction Stop) }
    catch { throw "observed V3 witness 返回非法 JSON:$($_.Exception.Message)" }
}

function Assert-LocalObservedV3Witness {
    param(
        [Parameter(Mandatory = $true)][object]$Witness,
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$MinikubeProfile
    )
    $pvc = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'pvc/etcd-data', '-n', $K8sNamespace, '-o', 'json') -Action '校验 observed V3 witness 绑定 PVC'
    $kubeSystemUid = Get-KubernetesNamespaceUid -KubeContext $KubeContext -Namespace 'kube-system'
    $pandoraUid = Get-KubernetesNamespaceUid -KubeContext $KubeContext -Namespace $K8sNamespace
    if ([string]$Witness.metadata.name -cne $script:LocalObservedV3WitnessName -or
        [string]$Witness.metadata.namespace -cne $K8sNamespace -or $Witness.immutable -ne $true -or
        [string]$Witness.data.schema_version -cne '1' -or
        [string]$Witness.data.minikube_profile -cne $MinikubeProfile -or
        [string]$Witness.data.kube_system_namespace_uid -cne $kubeSystemUid -or
        [string]$Witness.data.pandora_namespace_uid -cne $pandoraUid -or
        [string]$Witness.data.etcd_pvc_uid -cne [string]$pvc.metadata.uid -or
        [string]$pvc.status.phase -cne 'Bound' -or
        [string]$Witness.data.observed_required_value -cne '2@ds-auth-v2-hub-successor-lease-v1' -or
        [string]$Witness.data.observed_policy_generation -cne '3') {
        throw 'observed V3 witness 的 profile/namespace/PVC/required 绑定漂移，拒绝信任。'
    }
}

function Ensure-LocalObservedV3Witness {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$MinikubeProfile
    )
    $existing = Get-LocalObservedV3Witness -KubeContext $KubeContext
    if ($null -ne $existing) {
        Assert-LocalObservedV3Witness -Witness $existing -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile
        return $false
    }
    $pvc = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'pvc/etcd-data', '-n', $K8sNamespace, '-o', 'json') -Action '建立 observed V3 witness 前读取 PVC'
    if ([string]$pvc.status.phase -cne 'Bound' -or [string]::IsNullOrWhiteSpace([string]$pvc.metadata.uid)) {
        throw '建立 observed V3 witness 前 PVC/etcd-data 必须 Bound 且 UID 非空。'
    }
    $witnessObject = [ordered]@{
        apiVersion = 'v1'; kind = 'ConfigMap'; immutable = $true
        metadata = [ordered]@{
            name = $script:LocalObservedV3WitnessName; namespace = $K8sNamespace
            labels = [ordered]@{ 'app.kubernetes.io/part-of' = 'pandora'; 'app.kubernetes.io/component' = 'dsauth-observed-v3-witness' }
        }
        data = [ordered]@{
            schema_version = '1'; minikube_profile = $MinikubeProfile
            kube_system_namespace_uid = Get-KubernetesNamespaceUid -KubeContext $KubeContext -Namespace 'kube-system'
            pandora_namespace_uid = Get-KubernetesNamespaceUid -KubeContext $KubeContext -Namespace $K8sNamespace
            etcd_pvc_uid = [string]$pvc.metadata.uid
            observed_required_value = '2@ds-auth-v2-hub-successor-lease-v1'
            observed_policy_generation = '3'
            observed_at_ms = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds().ToString()
        }
    }
    $json = $witnessObject | ConvertTo-Json -Depth 20 -Compress
    $created = @($json | & kubectl --context $KubeContext create -f - 2>&1)
    if ($LASTEXITCODE -ne 0) {
        $raced = Get-LocalObservedV3Witness -KubeContext $KubeContext
        if ($null -eq $raced) { throw "create-only 建立 observed V3 witness 失败:$($created -join [Environment]::NewLine)" }
    }
    $readback = Get-LocalObservedV3Witness -KubeContext $KubeContext
    Assert-LocalObservedV3Witness -Witness $readback -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile
    Write-Ok '已为 markerless canonical V3 建立 immutable observed witness；未来 missing 将按数据丢失阻断。'
    return $true
}

function Assert-LocalFreshGenesisIntent {
    param(
        [Parameter(Mandatory = $true)][object]$Marker,
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$MinikubeProfile
    )
    if ([string]$Marker.metadata.name -cne $script:LocalFreshDsAuthMarkerName -or
        [string]$Marker.metadata.namespace -cne $K8sNamespace -or $Marker.immutable -ne $true) {
        throw 'fresh DS-auth marker 名称/namespace/immutable 契约漂移，拒绝自动 genesis。'
    }
    $kubeSystemUid = Get-KubernetesNamespaceUid -KubeContext $KubeContext -Namespace 'kube-system'
    $pandoraUid = Get-KubernetesNamespaceUid -KubeContext $KubeContext -Namespace $K8sNamespace
    if ([string]$Marker.data.schema_version -cne '1' -or
        [string]$Marker.data.minikube_profile -cne $MinikubeProfile -or
        [string]$Marker.data.kube_system_namespace_uid -cne $kubeSystemUid -or
        [string]$Marker.data.pandora_namespace_uid -cne $pandoraUid -or
        [string]$Marker.data.intent_origin -cnotin @(
            $script:LocalFreshDsAuthOriginFresh,
            $script:LocalFreshDsAuthOriginAdopted
        ) -or
        [string]$Marker.data.target_writer_epoch -cne '2' -or
        [string]$Marker.data.target_policy_generation -cne '3' -or
        [string]$Marker.data.target_policy_id -cne 'ds-auth-v2-hub-successor-lease-v1' -or
        [string]$Marker.data.activation_evidence_sha256 -cne $script:LocalFreshDsAuthEvidenceSha256) {
        throw 'fresh DS-auth marker 的集群 UID/profile/目标策略证据不匹配，拒绝把旧数据冒充 fresh。'
    }
    $annotations = $Marker.metadata.annotations
    function Read-MarkerAnnotation([string]$Name) {
        if ($null -eq $annotations) { return '' }
        $property = $annotations.PSObject.Properties[$Name]
        if ($null -eq $property) { return '' }
        return [string]$property.Value
    }
    $state = Read-MarkerAnnotation $script:LocalFreshDsAuthMarkerStateAnnotation
    $pvcAnnotation = Read-MarkerAnnotation $script:LocalFreshDsAuthMarkerPvcAnnotation
    $evidenceCompletedAnnotation = Read-MarkerAnnotation $script:LocalFreshDsAuthMarkerEvidenceCompletedAnnotation
    $completedAnnotation = Read-MarkerAnnotation $script:LocalFreshDsAuthMarkerCompletedAnnotation
    $origin = [string]$Marker.data.intent_origin
    $cohortProperty = $Marker.data.PSObject.Properties[$script:LocalLegacyAdoptionCohortFingerprintField]
    $cohortFingerprint = if ($null -eq $cohortProperty) { '' } else { [string]$cohortProperty.Value }
    $continuityProperty = $Marker.data.PSObject.Properties[$script:LocalFreshDsAuthContinuityTokenField]
    $continuityToken = if ($null -eq $continuityProperty) { '' } else { [string]$continuityProperty.Value }
    if ($continuityToken -cnotmatch '^nonce:[0-9a-f]{64}$') {
        throw 'fresh DS-auth marker 缺少合法的 immutable genesis continuity token。'
    }
    if ($state -cnotin @('preinfra', 'adopting', 'pending', 'complete')) {
        throw "fresh DS-auth marker 状态非法:$state"
    }
    if (($origin -ceq $script:LocalFreshDsAuthOriginFresh -and $state -ceq 'adopting') -or
        ($origin -ceq $script:LocalFreshDsAuthOriginAdopted -and $state -ceq 'preinfra') -or
        ($state -ceq 'preinfra' -and (-not [string]::IsNullOrWhiteSpace($pvcAnnotation) -or
            -not [string]::IsNullOrWhiteSpace($evidenceCompletedAnnotation) -or
            -not [string]::IsNullOrWhiteSpace($completedAnnotation))) -or
        ($state -ceq 'adopting' -and ([string]::IsNullOrWhiteSpace($pvcAnnotation) -or
            -not [string]::IsNullOrWhiteSpace($evidenceCompletedAnnotation) -or
            -not [string]::IsNullOrWhiteSpace($completedAnnotation))) -or
        ($state -ceq 'pending' -and ([string]::IsNullOrWhiteSpace($pvcAnnotation) -or
            -not [string]::IsNullOrWhiteSpace($completedAnnotation)))) {
        throw "fresh DS-auth marker 的 origin/state/PVC/completed annotations 组合非法(origin=$origin,state=$state)。"
    }
    if (($origin -ceq $script:LocalFreshDsAuthOriginAdopted -and
            $cohortFingerprint -cnotmatch '^sha256:[0-9a-f]{64}$') -or
        ($origin -ceq $script:LocalFreshDsAuthOriginFresh -and
            -not [string]::IsNullOrWhiteSpace($cohortFingerprint))) {
        throw "fresh DS-auth marker 的 legacy cohort fingerprint 与 origin 不匹配(origin=$origin)。"
    }
    $evidenceCompletedAtMS = 0L
    if (-not [string]::IsNullOrWhiteSpace($evidenceCompletedAnnotation) -and
        (-not [long]::TryParse($evidenceCompletedAnnotation, [ref]$evidenceCompletedAtMS) -or $evidenceCompletedAtMS -le 0)) {
        throw 'fresh DS-auth marker evidence-completed-at-ms 非法。'
    }
    if ($state -cin @('adopting', 'pending', 'complete')) {
        $pvc = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', 'pvc/etcd-data', '-n', $K8sNamespace, '-o', 'json') -Action '读取 fresh marker 绑定的 PVC/etcd-data'
        $markerPvcUid = $pvcAnnotation
        if ([string]$pvc.status.phase -cne 'Bound' -or
            [string]::IsNullOrWhiteSpace([string]$pvc.metadata.uid) -or
            [string]::IsNullOrWhiteSpace($markerPvcUid) -or
            $markerPvcUid -cne [string]$pvc.metadata.uid) {
            throw 'fresh DS-auth marker 绑定的 etcd-data PVC UID 已漂移，拒绝自动 genesis。'
        }
    }
    if ($state -ceq 'complete') {
        $completedAtMS = 0L
        if ($evidenceCompletedAtMS -le 0 -or [string]::IsNullOrWhiteSpace($completedAnnotation) -or
            -not [long]::TryParse($completedAnnotation, [ref]$completedAtMS) -or $completedAtMS -le 0) {
            throw 'fresh DS-auth marker 已标 complete 但完成时间缺失/非法，拒绝信任。'
        }
    }
    return $state
}

function New-LocalFreshGenesisIntent {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$MinikubeProfile
    )
    if ($null -ne (Get-LocalFreshGenesisIntent -KubeContext $KubeContext)) {
        throw 'fresh profile 上意外已存在 DS-auth marker；create-only 契约拒绝覆盖。'
    }
    $kubeSystemUid = Get-KubernetesNamespaceUid -KubeContext $KubeContext -Namespace 'kube-system'
    $pandoraUid = Get-KubernetesNamespaceUid -KubeContext $KubeContext -Namespace $K8sNamespace
    $now = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds().ToString()
    $continuityToken = New-LocalGenesisContinuityToken
    $markerObject = [ordered]@{
        apiVersion = 'v1'; kind = 'ConfigMap'; immutable = $true
        metadata = [ordered]@{
            name = $script:LocalFreshDsAuthMarkerName; namespace = $K8sNamespace
            labels = [ordered]@{ 'app.kubernetes.io/part-of' = 'pandora'; 'app.kubernetes.io/component' = 'dsauth-bootstrap-intent' }
            annotations = [ordered]@{ $script:LocalFreshDsAuthMarkerStateAnnotation = 'preinfra' }
        }
        data = [ordered]@{
            schema_version = '1'; minikube_profile = $MinikubeProfile
            kube_system_namespace_uid = $kubeSystemUid; pandora_namespace_uid = $pandoraUid
            intent_origin = $script:LocalFreshDsAuthOriginFresh
            target_writer_epoch = '2'; target_policy_generation = '3'
            target_policy_id = 'ds-auth-v2-hub-successor-lease-v1'
            activation_evidence_sha256 = $script:LocalFreshDsAuthEvidenceSha256
            $script:LocalFreshDsAuthContinuityTokenField = $continuityToken
            created_at_ms = $now
        }
    }
    $json = $markerObject | ConvertTo-Json -Depth 20 -Compress
    $created = @($json | & kubectl --context $KubeContext create -f - 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "create-only 创建 fresh DS-auth marker 失败:$($created -join [Environment]::NewLine)"
    }
    $marker = Get-LocalFreshGenesisIntent -KubeContext $KubeContext
    $state = Assert-LocalFreshGenesisIntent -Marker $marker -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile
    if ($state -cne 'preinfra') { throw 'fresh DS-auth marker 创建后未处于 preinfra。' }
    Write-Ok 'fresh DS-auth intent 已持久化并绑定当前 minikube/namespace UID(state=preinfra)。'
}

# 兼容 6aff5dd 之前的半成品启动现场：旧脚本可能已创建 namespace、PVC 和纯基础设施，
# 但在 required policy bootstrap 前退出，因此没有 preinfra marker。收养先把 state=adopting、
# 当前 Bound PVC UID、cohort fingerprint、随机 continuity token 与 origin 放在同一个 create-only
# ConfigMap 中；只有同 token 已 create-only 写进 etcd 数据盘后，才允许 CAS patch 到 pending。
function New-LocalAdoptedGenesisIntent {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$MinikubeProfile,
        [Parameter(Mandatory = $true)][string]$ExpectedPvcUid,
        [Parameter(Mandatory = $true)][string]$ExpectedCohortFingerprintSha256
    )
    if ($ExpectedCohortFingerprintSha256 -cnotmatch '^sha256:[0-9a-f]{64}$') {
        throw 'legacy infra-only 收养要求合法的 cohort sha256 fingerprint。'
    }
    if ($null -ne (Get-LocalFreshGenesisIntent -KubeContext $KubeContext)) {
        throw 'legacy infra-only 收养时意外已存在 DS-auth marker；create-only 契约拒绝覆盖。'
    }
    $pvc = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'pvc/etcd-data', '-n', $K8sNamespace, '-o', 'json') -Action '收养 legacy infra-only 现场前读取 PVC/etcd-data'
    $pvcUid = [string]$pvc.metadata.uid
    if ([string]$pvc.status.phase -cne 'Bound' -or [string]::IsNullOrWhiteSpace($pvcUid) -or
        $pvcUid -cne $ExpectedPvcUid) {
        throw "legacy infra-only 收养前 PVC/etcd-data 未 Bound 或 UID 已漂移(expected=$ExpectedPvcUid,actual=$pvcUid)。"
    }
    $kubeSystemUid = Get-KubernetesNamespaceUid -KubeContext $KubeContext -Namespace 'kube-system'
    $pandoraUid = Get-KubernetesNamespaceUid -KubeContext $KubeContext -Namespace $K8sNamespace
    $now = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds().ToString()
    $continuityToken = New-LocalGenesisContinuityToken
    $markerObject = [ordered]@{
        apiVersion = 'v1'; kind = 'ConfigMap'; immutable = $true
        metadata = [ordered]@{
            name = $script:LocalFreshDsAuthMarkerName; namespace = $K8sNamespace
            labels = [ordered]@{ 'app.kubernetes.io/part-of' = 'pandora'; 'app.kubernetes.io/component' = 'dsauth-bootstrap-intent' }
            annotations = [ordered]@{
                $script:LocalFreshDsAuthMarkerStateAnnotation = 'adopting'
                $script:LocalFreshDsAuthMarkerPvcAnnotation = $pvcUid
            }
        }
        data = [ordered]@{
            schema_version = '1'; minikube_profile = $MinikubeProfile
            kube_system_namespace_uid = $kubeSystemUid; pandora_namespace_uid = $pandoraUid
            intent_origin = $script:LocalFreshDsAuthOriginAdopted
            target_writer_epoch = '2'; target_policy_generation = '3'
            target_policy_id = 'ds-auth-v2-hub-successor-lease-v1'
            activation_evidence_sha256 = $script:LocalFreshDsAuthEvidenceSha256
            $script:LocalFreshDsAuthContinuityTokenField = $continuityToken
            $script:LocalLegacyAdoptionCohortFingerprintField = $ExpectedCohortFingerprintSha256
            created_at_ms = $now
        }
    }
    $json = $markerObject | ConvertTo-Json -Depth 20 -Compress
    $created = @($json | & kubectl --context $KubeContext create -f - 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "create-only 收养 legacy infra-only DS-auth marker 失败:$($created -join [Environment]::NewLine)"
    }
    $marker = Get-LocalFreshGenesisIntent -KubeContext $KubeContext
    $state = Assert-LocalFreshGenesisIntent -Marker $marker -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile
    $cohortProperty = $marker.data.PSObject.Properties[$script:LocalLegacyAdoptionCohortFingerprintField]
    if ($state -cne 'adopting' -or [string]$marker.data.intent_origin -cne $script:LocalFreshDsAuthOriginAdopted -or
        $null -eq $cohortProperty -or [string]$cohortProperty.Value -cne $ExpectedCohortFingerprintSha256) {
        throw 'legacy infra-only DS-auth marker 创建后未精确处于 adopted/adopting。'
    }
    Write-Ok "legacy infra-only DS-auth 现场已 create-only 收养并绑定 PVC UID=$pvcUid(state=adopting)。"
}

function Set-LocalFreshGenesisIntentState {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$MinikubeProfile,
        [Parameter(Mandatory = $true)][ValidateSet('pending', 'complete')][string]$TargetState
    )
    $marker = Get-LocalFreshGenesisIntent -KubeContext $KubeContext
    if ($null -eq $marker) { throw "缺少 fresh DS-auth marker，不能推进到 $TargetState。" }
    $state = Assert-LocalFreshGenesisIntent -Marker $marker -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile
    if ($state -ceq $TargetState -or ($state -ceq 'complete' -and $TargetState -ceq 'pending')) { return }
    if (($TargetState -ceq 'pending' -and $state -cnotin @('preinfra', 'adopting')) -or
        ($TargetState -ceq 'complete' -and $state -cne 'pending')) {
        throw "fresh DS-auth marker 非法状态转换:$state->$TargetState"
    }

    $annotations = [ordered]@{ $script:LocalFreshDsAuthMarkerStateAnnotation = $TargetState }
    if ($TargetState -ceq 'pending') {
        $pvc = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', 'pvc/etcd-data', '-n', $K8sNamespace, '-o', 'json') -Action '绑定 fresh marker 到 PVC/etcd-data'
        $pvcUid = [string]$pvc.metadata.uid
        if ([string]$pvc.status.phase -cne 'Bound' -or [string]::IsNullOrWhiteSpace($pvcUid)) {
            throw 'fresh marker 推进 pending 前 PVC/etcd-data 必须为 Bound 且 UID 非空。'
        }
        $annotations[$script:LocalFreshDsAuthMarkerPvcAnnotation] = $pvcUid
    } else {
        $annotations[$script:LocalFreshDsAuthMarkerCompletedAnnotation] = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds().ToString()
    }
    $patchObject = [ordered]@{ metadata = [ordered]@{ resourceVersion = [string]$marker.metadata.resourceVersion; annotations = $annotations } }
    $patchJson = $patchObject | ConvertTo-Json -Depth 10 -Compress
    $patched = @(& kubectl --context $KubeContext patch "configmap/$($script:LocalFreshDsAuthMarkerName)" `
        -n $K8sNamespace --type merge -p $patchJson 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "CAS 推进 fresh DS-auth marker $state->$TargetState 失败:$($patched -join [Environment]::NewLine)"
    }
    $readback = Get-LocalFreshGenesisIntent -KubeContext $KubeContext
    $actual = Assert-LocalFreshGenesisIntent -Marker $readback -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile
    if ($actual -cne $TargetState) { throw "fresh DS-auth marker 推进后回读状态不一致:$actual" }
    Write-Ok "fresh DS-auth marker 已推进:$state->$TargetState"
}

# evidence completion time 必须在 etcd CAS 前持久化到 pending marker；这样 CAS 已提交但
# complete 尚未写入时，重跑可以用同一个 sha+time 精确验证 genesis record，而不是接受任意
# canonical V3 migration record 冒充本次 fresh genesis。
function Get-OrSetLocalFreshGenesisEvidenceCompletedAtMS {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$MinikubeProfile
    )
    $marker = Get-LocalFreshGenesisIntent -KubeContext $KubeContext
    if ($null -eq $marker) { throw '缺少 pending marker，不能持久化 genesis evidence time。' }
    $state = Assert-LocalFreshGenesisIntent -Marker $marker -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile
    if ($state -cne 'pending') { throw "只有 pending marker 可建立 genesis evidence time，实际=$state。" }
    $property = $marker.metadata.annotations.PSObject.Properties[$script:LocalFreshDsAuthMarkerEvidenceCompletedAnnotation]
    if ($null -ne $property -and -not [string]::IsNullOrWhiteSpace([string]$property.Value)) {
        $existing = 0L
        if (-not [long]::TryParse([string]$property.Value, [ref]$existing) -or $existing -le 0) {
            throw 'pending marker 中已有非法 genesis evidence time。'
        }
        return $existing
    }
    $completedAtMS = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $patchObject = [ordered]@{ metadata = [ordered]@{
        resourceVersion = [string]$marker.metadata.resourceVersion
        annotations = [ordered]@{
            $script:LocalFreshDsAuthMarkerEvidenceCompletedAnnotation = $completedAtMS.ToString()
        }
    } }
    $patchJson = $patchObject | ConvertTo-Json -Depth 10 -Compress
    $patched = @(& kubectl --context $KubeContext patch "configmap/$($script:LocalFreshDsAuthMarkerName)" `
        -n $K8sNamespace --type merge -p $patchJson 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "CAS 持久化 pending marker genesis evidence time 失败:$($patched -join [Environment]::NewLine)"
    }
    $readback = Get-LocalFreshGenesisIntent -KubeContext $KubeContext
    $actualState = Assert-LocalFreshGenesisIntent -Marker $readback -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile
    $actualProperty = $readback.metadata.annotations.PSObject.Properties[$script:LocalFreshDsAuthMarkerEvidenceCompletedAnnotation]
    if ($actualState -cne 'pending' -or $null -eq $actualProperty -or [string]$actualProperty.Value -cne $completedAtMS.ToString()) {
        throw 'pending marker genesis evidence time 写入后回读不一致。'
    }
    Write-Ok "pending marker 已持久化 genesis evidence completed-at=$completedAtMS。"
    return $completedAtMS
}

# pending marker 允许 missing->V3 之前，pandora namespace 只能存在本启动器声明的第三方
# 基础设施 workload；任何未知（包括缩到 0 的业务 Deployment）或终止中的 workload 都阻断。
# 其它 namespace 只按 Pandora/DS 身份筛查，避免把用户无关的本地 workload 误当 writer。
function Assert-NoLocalFreshGenesisWriters {
    param([Parameter(Mandatory = $true)][string]$KubeContext)
    $allowedImages = [ordered]@{
        mysql = 'mysql:8.4'; redis = 'redis:8.8.0-alpine'
        zookeeper = 'confluentinc/cp-zookeeper:7.9.7'; kafka = 'confluentinc/cp-kafka:7.9.7'
        etcd = 'quay.io/coreos/etcd:v3.6.12'; loki = 'grafana/loki:3.4.1'; alloy = 'grafana/alloy:v1.7.1'
        prometheus = 'prom/prometheus:v2.55.1'; grafana = 'grafana/grafana:11.3.1'
        pd = 'pingcap/pd:v8.5.1'; tikv = 'pingcap/tikv:v8.5.1'; tidb = 'pingcap/tidb:v8.5.1'
        # Sentinel 栈(2026-07-28 P2):值为 hashtable 时显式声明期望副本数(默认 1);
        # 校验语义不变——名字/镜像/副本数三者都必须与声明逐项一致,仍是封闭清单。
        'redis-master' = 'redis:8.8.0-alpine'
        'redis-replica' = @{ image = 'redis:8.8.0-alpine'; replicas = 2 }
        'redis-sentinel' = @{ image = 'redis:8.8.0-alpine'; replicas = 3 }
    }
    $writerIdentities = @('login', 'player-locator', 'ds-allocator', 'hub-allocator', 'battle-result')
    function Get-OptionalPropertyValue([object]$Object, [string]$Name) {
        if ($null -eq $Object) { return $null }
        $property = $Object.PSObject.Properties[$Name]
        if ($null -eq $property) { return $null }
        return $property.Value
    }
    function Get-WorkloadPodSpec([object]$Item) {
        $kind = [string]$Item.kind
        if ($kind -ceq 'Pod') { return $Item.spec }
        if ($kind -ceq 'CronJob') { return $Item.spec.jobTemplate.spec.template.spec }
        return $Item.spec.template.spec
    }
    function Get-WorkloadImages([object]$PodSpec) {
        $images = [System.Collections.Generic.List[string]]::new()
        foreach ($field in @('containers', 'initContainers', 'ephemeralContainers')) {
            $containers = Get-OptionalPropertyValue -Object $PodSpec -Name $field
            foreach ($container in @($containers)) {
                if ($null -ne $container) { $images.Add([string]$container.image) }
            }
        }
        return $images.ToArray()
    }

    $allWorkloads = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'deployment,statefulset,daemonset,replicaset,replicationcontroller,pod,job,cronjob', '-A', '-o', 'json') `
        -Action 'fresh genesis 前枚举全局 workload'
    $seenInfraDeployments = @{}
    foreach ($item in @($allWorkloads.items)) {
        $namespace = [string]$item.metadata.namespace
        $kind = [string]$item.kind
        $name = [string]$item.metadata.name
        $labels = Get-OptionalPropertyValue -Object $item.metadata -Name 'labels'
        $app = [string](Get-OptionalPropertyValue -Object $labels -Name 'app')
        $podSpec = Get-WorkloadPodSpec -Item $item
        $images = @(Get-WorkloadImages -PodSpec $podSpec)
        if ($namespace -ceq $K8sNamespace) {
            if ($kind -notin @('Deployment', 'ReplicaSet', 'Pod')) {
                throw "fresh genesis 前检测到不允许的 pandora workload:$kind/$name"
            }
            if (-not $allowedImages.Contains($app)) {
                throw "fresh genesis 前检测到未知 pandora workload:$kind/$name(app=$app)"
            }
            $deletionTimestamp = [string](Get-OptionalPropertyValue -Object $item.metadata -Name 'deletionTimestamp')
            if (-not [string]::IsNullOrWhiteSpace($deletionTimestamp)) {
                throw "fresh genesis 前基础设施 workload 正在终止:$kind/$name"
            }
            $allowedEntry = $allowedImages[$app]
            $expectedImage = if ($allowedEntry -is [System.Collections.IDictionary]) { [string]$allowedEntry['image'] } else { [string]$allowedEntry }
            $expectedReplicas = if ($allowedEntry -is [System.Collections.IDictionary]) { [int]$allowedEntry['replicas'] } else { 1 }
            if ($images.Count -ne 1 -or $images[0] -cne $expectedImage) {
                throw "fresh genesis 前基础设施 workload 镜像/init/ephemeral 容器漂移:$kind/$name"
            }
            $ownerReferenceValue = Get-OptionalPropertyValue -Object $item.metadata -Name 'ownerReferences'
            $ownerReferences = if ($null -eq $ownerReferenceValue) { @() } else { @($ownerReferenceValue) }
            $controllerOwner = @($ownerReferences | Where-Object {
                (Get-OptionalPropertyValue -Object $_ -Name 'controller') -eq $true
            })
            if ($kind -ceq 'Deployment') {
                if ([int]$item.spec.replicas -ne $expectedReplicas -or $name -cne $app -or $controllerOwner.Count -ne 0) {
                    throw "fresh genesis 前基础设施 Deployment 结构/replicas 漂移:$name(期望 replicas=$expectedReplicas)"
                }
                if ($seenInfraDeployments.ContainsKey($app)) {
                    throw "fresh genesis 前基础设施 Deployment 重复:$app"
                }
                $seenInfraDeployments[$app] = $true
            } elseif ($kind -ceq 'ReplicaSet') {
                if ($controllerOwner.Count -ne 1 -or [string]$controllerOwner[0].kind -cne 'Deployment' -or
                    [string]$controllerOwner[0].name -cne $app) {
                    throw "fresh genesis 前基础设施 ReplicaSet owner 漂移:$name"
                }
            } else {
                if ($controllerOwner.Count -ne 1 -or [string]$controllerOwner[0].kind -cne 'ReplicaSet' -or
                    -not ([string]$controllerOwner[0].name).StartsWith("$app-", [StringComparison]::Ordinal)) {
                    throw "fresh genesis 前基础设施 Pod owner 漂移:$name"
                }
            }
            continue
        }
        # 控制面静态(mirror)Pod 由 Node 拥有(kube-apiserver-<节点名> 等);minikube profile 名
        # 含 "pandora"(pandora-agones)时其节点名后缀会撞上下方 Pandora 身份正则——它们是
        # k8s 控制面本体,按「kube-system + owner=Node」精确豁免(2026-07-28 重建实测抓获:
        # 本门 07-25 加入,晚于旧集群创世,首次 fresh 全量跑才暴露)。其余对象照常筛查。
        if ($kind -ceq 'Pod' -and $namespace -ceq 'kube-system') {
            $mirrorOwnerValue = Get-OptionalPropertyValue -Object $item.metadata -Name 'ownerReferences'
            $nodeOwners = @(@($mirrorOwnerValue) | Where-Object {
                $null -ne $_ -and (Get-OptionalPropertyValue -Object $_ -Name 'controller') -eq $true -and [string]$_.kind -ceq 'Node'
            })
            if ($nodeOwners.Count -eq 1) { continue }
        }
        $identity = @($namespace, $name, $app, ($images -join ',')) -join ' '
        $writerEpochLabel = [string](Get-OptionalPropertyValue -Object $labels -Name 'pandora.dev/ds-auth-writer-epoch')
        if ($name -in $writerIdentities -or $app -in $writerIdentities -or
            -not [string]::IsNullOrWhiteSpace($writerEpochLabel) -or
            $identity -match '(?i)pandora|battle-ds|hub-ds|ds-allocator|hub-allocator|player-locator|matchmaker') {
            throw "fresh genesis 前其它 namespace 仍有 Pandora/DS workload:$namespace/$kind/$name"
        }
    }
    foreach ($requiredInfra in @('mysql', 'redis', 'zookeeper', 'kafka', 'etcd')) {
        if (-not $seenInfraDeployments.ContainsKey($requiredInfra)) {
            throw "fresh genesis 前缺少 canonical 基础设施 Deployment/$requiredInfra"
        }
    }
    foreach ($resourceType in @('fleet', 'fleetautoscaler', 'gameserver', 'gameserverset', 'gameserverallocation')) {
        $lines = @(& kubectl --context $KubeContext get $resourceType -A -o name 2>&1)
        $text = (($lines | ForEach-Object { $_.ToString() }) -join "`n").Trim()
        if ($LASTEXITCODE -ne 0) {
            if ($text -match '(?i)the server doesn''t have a resource type|could not find the requested resource') { continue }
            throw "fresh genesis 前读取 Agones $resourceType 失败:$text"
        }
        if (-not [string]::IsNullOrWhiteSpace($text)) {
            throw "fresh genesis 前仍有 Agones $resourceType，拒绝 zero-writer genesis:$text"
        }
    }
    Write-Ok 'fresh DS-auth zero-writer workload 门禁通过（仅基础设施，无 Pandora/Agones writer）。'
}

# 6aff5dd 之前的旧启动器没有 fresh marker，只能对“刚刚由同一批 apply 创建”的本地
# minikube 半成品做一次窄兼容。这个 cohort 是 local-only heuristic，不声称数学证明卷连续性；
# 它只证明当前对象同批且当前 etcd 未写。adopting 后的数据盘连续性由随机 sentinel、pending
# 不可重建门禁与最终 etcd CAS 共同收口。
function Get-LocalLegacyInfraOnlyAdoptionCohort {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][long]$CollectionTimeUnixMS,
        [Parameter(Mandatory = $true)][string]$ExpectedPvcUid,
        [switch]$AllowNotReady
    )
    if ($CollectionTimeUnixMS -le 0) { throw 'legacy cohort collection time 必须为正 Unix ms。' }
    if ([string]::IsNullOrWhiteSpace($ExpectedPvcUid)) { throw 'legacy cohort 必须绑定预先读取的 PVC UID。' }

    $allowedImages = [ordered]@{
        mysql = 'mysql:8.4'; redis = 'redis:8.8.0-alpine'
        zookeeper = 'confluentinc/cp-zookeeper:7.9.7'; kafka = 'confluentinc/cp-kafka:7.9.7'
        etcd = 'quay.io/coreos/etcd:v3.6.12'
    }
    $createdTimes = [System.Collections.Generic.List[long]]::new()
    $fingerprintLines = [System.Collections.Generic.List[string]]::new()

    function Get-OptionalPropertyValue([object]$Object, [string]$Name) {
        if ($null -eq $Object) { return $null }
        $property = $Object.PSObject.Properties[$Name]
        if ($null -eq $property) { return $null }
        return $property.Value
    }
    function Get-CanonicalUid([object]$Metadata, [string]$What) {
        $raw = [string](Get-OptionalPropertyValue -Object $Metadata -Name 'uid')
        $parsed = [guid]::Empty
        if (-not [guid]::TryParse($raw, [ref]$parsed) -or $raw -cne $parsed.ToString('D')) {
            throw "$What 缺少 canonical UID:$raw"
        }
        return $raw
    }
    function Get-ControllerOwners([object]$Metadata) {
        $raw = Get-OptionalPropertyValue -Object $Metadata -Name 'ownerReferences'
        if ($null -eq $raw) { return @() }
        return @(@($raw) | Where-Object {
            (Get-OptionalPropertyValue -Object $_ -Name 'controller') -eq $true
        })
    }
    function Get-AllOwnerReferences([object]$Metadata) {
        $raw = Get-OptionalPropertyValue -Object $Metadata -Name 'ownerReferences'
        if ($null -eq $raw) { return @() }
        return @($raw)
    }
    function Assert-NoDeletion([object]$Metadata, [string]$What) {
        $deleting = [string](Get-OptionalPropertyValue -Object $Metadata -Name 'deletionTimestamp')
        if (-not [string]::IsNullOrWhiteSpace($deleting)) { throw "$What 正在删除，不能进入 legacy cohort。" }
    }
    function Add-CohortEvidence([object]$Item, [string]$Identity, [string]$Extra) {
        if ($null -eq $Item -or $null -eq $Item.metadata) { throw "$Identity 缺少 metadata。" }
        Assert-NoDeletion -Metadata $Item.metadata -What $Identity
        $uid = Get-CanonicalUid -Metadata $Item.metadata -What $Identity
        $rawCreated = Get-OptionalPropertyValue -Object $Item.metadata -Name 'creationTimestamp'
        if ($null -eq $rawCreated -or [string]::IsNullOrWhiteSpace([string]$rawCreated)) {
            throw "$Identity 缺少 API creationTimestamp。"
        }
        try {
            if ($rawCreated -is [DateTimeOffset]) {
                $created = ([DateTimeOffset]$rawCreated).ToUniversalTime()
            } elseif ($rawCreated -is [DateTime]) {
                $created = [DateTimeOffset]([DateTime]$rawCreated).ToUniversalTime()
            } else {
                $styles = [Globalization.DateTimeStyles]::AssumeUniversal -bor [Globalization.DateTimeStyles]::AdjustToUniversal
                $created = [DateTimeOffset]::Parse([string]$rawCreated, [Globalization.CultureInfo]::InvariantCulture, $styles)
            }
        } catch {
            throw "$Identity creationTimestamp 非法:$rawCreated"
        }
        $createdMS = $created.ToUnixTimeMilliseconds()
        $ageMS = $CollectionTimeUnixMS - $createdMS
        if ($ageMS -lt 0 -or $ageMS -gt $script:LocalLegacyAdoptionCohortMaxAgeMS) {
            throw "$Identity 不属于最近 2 小时 cohort(created=$createdMS,collected=$CollectionTimeUnixMS)。"
        }
        $null = $createdTimes.Add($createdMS)
        $null = $fingerprintLines.Add("$Identity|uid=$uid|created_at_ms=$createdMS|$Extra")
        return [pscustomobject]@{ Uid = $uid; CreatedAtMS = $createdMS }
    }
    function Assert-CanonicalContainerSpec([object]$PodSpec, [string]$App, [string]$ExpectedImage, [string]$What) {
        if ($null -eq $PodSpec) { throw "$What 缺少 pod spec。" }
        $rawContainers = Get-OptionalPropertyValue -Object $PodSpec -Name 'containers'
        $rawInitContainers = Get-OptionalPropertyValue -Object $PodSpec -Name 'initContainers'
        $rawEphemeralContainers = Get-OptionalPropertyValue -Object $PodSpec -Name 'ephemeralContainers'
        $containers = @(if ($null -ne $rawContainers) { $rawContainers })
        $initContainers = @(if ($null -ne $rawInitContainers) { $rawInitContainers })
        $ephemeralContainers = @(if ($null -ne $rawEphemeralContainers) { $rawEphemeralContainers })
        if ($containers.Count -ne 1 -or [string]$containers[0].name -cne $App -or
            [string]$containers[0].image -cne $ExpectedImage -or
            $initContainers.Count -ne 0 -or $ephemeralContainers.Count -ne 0) {
            throw "$What container/name/image/init/ephemeral 结构漂移。"
        }
    }
    function Assert-CanonicalEtcdStorage([object]$PodSpec, [string]$What) {
        $containers = @(Get-OptionalPropertyValue -Object $PodSpec -Name 'containers')
        if ($containers.Count -ne 1) { throw "$What etcd container 数量漂移。" }
        $container = $containers[0]
        $rawArgs = Get-OptionalPropertyValue -Object $container -Name 'args'
        $actualArgs = @(if ($null -ne $rawArgs) { $rawArgs })
        if ($actualArgs.Count -ne 0) { throw "$What etcd container.args 必须为空，禁止覆盖固定 command。" }
        $expectedCommand = @(
            '/usr/local/bin/etcd', '--name=pandora-etcd', '--data-dir=/etcd-data',
            '--listen-client-urls=http://0.0.0.0:2379', '--advertise-client-urls=http://etcd:2379',
            '--listen-peer-urls=http://0.0.0.0:2380', '--initial-advertise-peer-urls=http://etcd:2380',
            '--initial-cluster=pandora-etcd=http://etcd:2380', '--initial-cluster-token=pandora-etcd-cluster',
            '--initial-cluster-state=new'
        )
        $actualCommand = @($container.command)
        if ($actualCommand.Count -ne $expectedCommand.Count) { throw "$What etcd command 长度漂移。" }
        for ($i = 0; $i -lt $expectedCommand.Count; $i++) {
            if ([string]$actualCommand[$i] -cne [string]$expectedCommand[$i]) {
                throw "$What etcd command 漂移(index=$i)。"
            }
        }
        $dataMounts = @($container.volumeMounts | Where-Object {
            $mountPath = [string]$_.mountPath
            [string]$_.name -ceq 'data' -or $mountPath -ceq '/etcd-data' -or
                $mountPath.StartsWith('/etcd-data/', [StringComparison]::Ordinal)
        })
        $dataVolumes = @($PodSpec.volumes | Where-Object { [string]$_.name -ceq 'data' })
        if ($dataMounts.Count -ne 1 -or [string]$dataMounts[0].name -cne 'data' -or
            [string]$dataMounts[0].mountPath -cne '/etcd-data' -or
            (Get-OptionalPropertyValue -Object $dataMounts[0] -Name 'readOnly') -eq $true -or
            -not [string]::IsNullOrEmpty([string](Get-OptionalPropertyValue -Object $dataMounts[0] -Name 'subPath')) -or
            -not [string]::IsNullOrEmpty([string](Get-OptionalPropertyValue -Object $dataMounts[0] -Name 'subPathExpr')) -or
            $dataVolumes.Count -ne 1 -or
            [string]$dataVolumes[0].persistentVolumeClaim.claimName -cne 'etcd-data' -or
            (Get-OptionalPropertyValue -Object $dataVolumes[0].persistentVolumeClaim -Name 'readOnly') -eq $true) {
            throw "$What etcd 必须把 PVC/etcd-data 精确挂载到 /etcd-data。"
        }
    }
    function Assert-AppAndHashLabels([object]$Labels, [string]$App, [string]$ExpectedHash, [string]$What) {
        $actualApp = [string](Get-OptionalPropertyValue -Object $Labels -Name 'app')
        $actualHash = [string](Get-OptionalPropertyValue -Object $Labels -Name 'pod-template-hash')
        if ($actualApp -cne $App -or
            (-not [string]::IsNullOrWhiteSpace($ExpectedHash) -and $actualHash -cne $ExpectedHash)) {
            throw "$What app/pod-template-hash labels 漂移。"
        }
    }

    $namespace = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', "namespace/$K8sNamespace", '-o', 'json') -Action '采集 legacy cohort namespace'
    if ([string]$namespace.kind -cne 'Namespace' -or [string]$namespace.metadata.name -cne $K8sNamespace) {
        throw 'legacy cohort namespace 身份漂移。'
    }
    $namespaceOwners = @(Get-AllOwnerReferences -Metadata $namespace.metadata)
    if ($namespaceOwners.Count -ne 0) { throw 'legacy cohort namespace 不得有 controller owner。' }
    $null = Add-CohortEvidence -Item $namespace -Identity "namespace/$K8sNamespace" -Extra 'kind=Namespace'

    $pvc = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'pvc/etcd-data', '-n', $K8sNamespace, '-o', 'json') -Action '采集 legacy cohort PVC/etcd-data'
    $pvcUid = Get-CanonicalUid -Metadata $pvc.metadata -What 'pvc/etcd-data'
    $pvcAccessModes = @($pvc.spec.accessModes)
    $pvName = [string]$pvc.spec.volumeName
    if ([string]$pvc.kind -cne 'PersistentVolumeClaim' -or [string]$pvc.metadata.name -cne 'etcd-data' -or
        [string]$pvc.metadata.namespace -cne $K8sNamespace -or [string]$pvc.status.phase -cne 'Bound' -or
        $pvcUid -cne $ExpectedPvcUid -or [string]$pvc.spec.storageClassName -cne 'standard' -or
        [string]$pvc.spec.volumeMode -cne 'Filesystem' -or $pvcAccessModes.Count -ne 1 -or
        [string]$pvcAccessModes[0] -cne 'ReadWriteOnce' -or [string]::IsNullOrWhiteSpace($pvName)) {
        throw 'legacy cohort PVC/etcd-data 的身份/Bound/UID/storageClass/volumeMode/accessMode 漂移。'
    }
    if (@(Get-AllOwnerReferences -Metadata $pvc.metadata).Count -ne 0) { throw 'legacy cohort PVC 不得有 ownerReference。' }
    $null = Add-CohortEvidence -Item $pvc -Identity "pvc/$K8sNamespace/etcd-data" `
        -Extra "pv=$pvName;storage_class=standard;mode=Filesystem;access=ReadWriteOnce"

    $pv = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', "persistentvolume/$pvName", '-o', 'json') -Action '采集 legacy cohort 绑定 PV'
    $pvUid = Get-CanonicalUid -Metadata $pv.metadata -What "pv/$pvName"
    $pvProvisioner = [string](Get-OptionalPropertyValue -Object $pv.metadata.annotations -Name 'pv.kubernetes.io/provisioned-by')
    $pvAccessModes = @($pv.spec.accessModes)
    $hostPath = [string](Get-OptionalPropertyValue -Object $pv.spec.hostPath -Name 'path')
    if ([string]$pv.kind -cne 'PersistentVolume' -or [string]$pv.metadata.name -cne $pvName -or
        [string]$pv.status.phase -cne 'Bound' -or [string]$pv.spec.claimRef.kind -cne 'PersistentVolumeClaim' -or
        [string]$pv.spec.claimRef.namespace -cne $K8sNamespace -or [string]$pv.spec.claimRef.name -cne 'etcd-data' -or
        [string]$pv.spec.claimRef.uid -cne $pvcUid -or [string]$pv.spec.storageClassName -cne 'standard' -or
        $pvProvisioner -cne 'k8s.io/minikube-hostpath' -or
        [string]$pv.spec.persistentVolumeReclaimPolicy -cne 'Delete' -or
        [string]$pv.spec.volumeMode -cne 'Filesystem' -or $pvAccessModes.Count -ne 1 -or
        [string]$pvAccessModes[0] -cne 'ReadWriteOnce' -or [string]::IsNullOrWhiteSpace($hostPath)) {
        throw 'legacy cohort PV 的 Bound/claimRef/storageClass/provisioner/reclaimPolicy/volume source 漂移。'
    }
    if (@(Get-AllOwnerReferences -Metadata $pv.metadata).Count -ne 0) { throw 'legacy cohort PV 不得有 ownerReference。' }
    $null = Add-CohortEvidence -Item $pv -Identity "pv/$pvName" `
        -Extra "claim_uid=$pvcUid;storage_class=standard;provisioner=$pvProvisioner;reclaim=Delete;host_path=$hostPath"

    $workloads = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'deployment,replicaset,pod', '-n', $K8sNamespace, '-o', 'json') `
        -Action '采集 legacy cohort 基础设施 Deployment/ReplicaSet/Pod'
    $items = @($workloads.items)
    foreach ($app in @($allowedImages.Keys)) {
        $expectedImage = [string]$allowedImages[$app]
        $deployments = @($items | Where-Object {
            [string]$_.kind -ceq 'Deployment' -and [string]$_.metadata.namespace -ceq $K8sNamespace -and
            [string](Get-OptionalPropertyValue -Object $_.metadata.labels -Name 'app') -ceq $app
        })
        if ($deployments.Count -ne 1 -or [string]$deployments[0].metadata.name -cne $app) {
            throw "legacy cohort 要求 app=$app 的唯一 canonical Deployment/$app，实际=$($deployments.Count)。"
        }
        $deployment = $deployments[0]
        Assert-NoDeletion -Metadata $deployment.metadata -What "Deployment/$app"
        if (@(Get-AllOwnerReferences -Metadata $deployment.metadata).Count -ne 0) {
            throw "legacy cohort Deployment/$app 不得有 ownerReference。"
        }
        Assert-AppAndHashLabels -Labels $deployment.metadata.labels -App $app -ExpectedHash '' -What "Deployment/$app"
        Assert-AppAndHashLabels -Labels $deployment.spec.selector.matchLabels -App $app -ExpectedHash '' -What "Deployment/$app selector"
        Assert-AppAndHashLabels -Labels $deployment.spec.template.metadata.labels -App $app -ExpectedHash '' -What "Deployment/$app template"
        Assert-CanonicalContainerSpec -PodSpec $deployment.spec.template.spec -App $app -ExpectedImage $expectedImage -What "Deployment/$app"
        $revision = [string](Get-OptionalPropertyValue -Object $deployment.metadata.annotations -Name 'deployment.kubernetes.io/revision')
        if ($revision -cne '1' -or [long]$deployment.metadata.generation -ne 1 -or
            [int]$deployment.spec.replicas -ne 1) {
            throw "legacy cohort Deployment/$app 的 revision/generation/spec 漂移。"
        }
        if (-not $AllowNotReady -and (
            [long](Get-OptionalPropertyValue -Object $deployment.status -Name 'observedGeneration') -ne [long]$deployment.metadata.generation -or
            [int](Get-OptionalPropertyValue -Object $deployment.status -Name 'replicas') -ne 1 -or
            [int](Get-OptionalPropertyValue -Object $deployment.status -Name 'updatedReplicas') -ne 1 -or
            [int](Get-OptionalPropertyValue -Object $deployment.status -Name 'readyReplicas') -ne 1 -or
            [int](Get-OptionalPropertyValue -Object $deployment.status -Name 'availableReplicas') -ne 1)) {
            throw "legacy cohort Deployment/$app 非唯一完成 rollout。"
        }
        if ($app -ceq 'etcd') {
            Assert-CanonicalEtcdStorage -PodSpec $deployment.spec.template.spec -What 'Deployment/etcd'
        }
        $deploymentEvidence = Add-CohortEvidence -Item $deployment -Identity "deployment/$K8sNamespace/$app" `
            -Extra "generation=$([long]$deployment.metadata.generation);revision=$revision;image=$expectedImage"

        $appReplicaSets = @($items | Where-Object {
            [string]$_.kind -ceq 'ReplicaSet' -and [string]$_.metadata.namespace -ceq $K8sNamespace -and
            [string](Get-OptionalPropertyValue -Object $_.metadata.labels -Name 'app') -ceq $app
        })
        if ($appReplicaSets.Count -ne 1) {
            throw "legacy cohort app=$app 必须只有一个（且无 orphan/历史）ReplicaSet，实际=$($appReplicaSets.Count)。"
        }
        $replicaSet = $appReplicaSets[0]
        $rsOwners = @(Get-ControllerOwners -Metadata $replicaSet.metadata)
        $rsHash = [string](Get-OptionalPropertyValue -Object $replicaSet.metadata.labels -Name 'pod-template-hash')
        $rsRevision = [string](Get-OptionalPropertyValue -Object $replicaSet.metadata.annotations -Name 'deployment.kubernetes.io/revision')
        if ($rsOwners.Count -ne 1 -or @(Get-AllOwnerReferences -Metadata $replicaSet.metadata).Count -ne 1 -or
            [string]$rsOwners[0].kind -cne 'Deployment' -or [string]$rsOwners[0].name -cne $app -or
            [string]$rsOwners[0].uid -cne $deploymentEvidence.Uid -or $rsRevision -cne $revision -or
            [string]::IsNullOrWhiteSpace($rsHash) -or -not ([string]$replicaSet.metadata.name).StartsWith("$app-", [StringComparison]::Ordinal) -or
            [long]$replicaSet.metadata.generation -ne 1 -or [int]$replicaSet.spec.replicas -ne 1) {
            throw "legacy cohort current ReplicaSet/$app 的 owner/UID/revision/spec 漂移。"
        }
        if (-not $AllowNotReady -and (
            [long](Get-OptionalPropertyValue -Object $replicaSet.status -Name 'observedGeneration') -ne [long]$replicaSet.metadata.generation -or
            [int](Get-OptionalPropertyValue -Object $replicaSet.status -Name 'replicas') -ne 1 -or
            [int](Get-OptionalPropertyValue -Object $replicaSet.status -Name 'readyReplicas') -ne 1 -or
            [int](Get-OptionalPropertyValue -Object $replicaSet.status -Name 'availableReplicas') -ne 1)) {
            throw "legacy cohort current ReplicaSet/$app 的 owner/UID/revision/replicas 漂移。"
        }
        Assert-AppAndHashLabels -Labels $replicaSet.metadata.labels -App $app -ExpectedHash $rsHash -What "ReplicaSet/$app"
        Assert-AppAndHashLabels -Labels $replicaSet.spec.selector.matchLabels -App $app -ExpectedHash $rsHash -What "ReplicaSet/$app selector"
        Assert-AppAndHashLabels -Labels $replicaSet.spec.template.metadata.labels -App $app -ExpectedHash $rsHash -What "ReplicaSet/$app template"
        Assert-CanonicalContainerSpec -PodSpec $replicaSet.spec.template.spec -App $app -ExpectedImage $expectedImage -What "ReplicaSet/$app"
        if ($app -ceq 'etcd') {
            Assert-CanonicalEtcdStorage -PodSpec $replicaSet.spec.template.spec -What 'ReplicaSet/etcd'
        }
        $replicaSetEvidence = Add-CohortEvidence -Item $replicaSet `
            -Identity "replicaset/$K8sNamespace/$([string]$replicaSet.metadata.name)" `
            -Extra "owner_uid=$($deploymentEvidence.Uid);generation=$([long]$replicaSet.metadata.generation);revision=$revision;hash=$rsHash;image=$expectedImage"

        $pods = @($items | Where-Object {
            [string]$_.kind -ceq 'Pod' -and [string]$_.metadata.namespace -ceq $K8sNamespace -and
            [string](Get-OptionalPropertyValue -Object $_.metadata.labels -Name 'app') -ceq $app
        })
        if ($pods.Count -ne 1) { throw "legacy cohort app=$app 必须只有一个（且无 orphan）Pod，实际=$($pods.Count)。" }
        $pod = $pods[0]
        $podOwners = @(Get-ControllerOwners -Metadata $pod.metadata)
        $readyConditions = @($pod.status.conditions | Where-Object {
            [string]$_.type -ceq 'Ready' -and [string]$_.status -ceq 'True'
        })
        if ($podOwners.Count -ne 1 -or @(Get-AllOwnerReferences -Metadata $pod.metadata).Count -ne 1 -or
            [string]$podOwners[0].kind -cne 'ReplicaSet' -or
            [string]$podOwners[0].name -cne [string]$replicaSet.metadata.name -or
            [string]$podOwners[0].uid -cne $replicaSetEvidence.Uid) {
            throw "legacy cohort Pod/$app 的 owner/UID 结构漂移。"
        }
        if ((-not $AllowNotReady -and ([string]$pod.status.phase -cne 'Running' -or $readyConditions.Count -ne 1)) -or
            ($AllowNotReady -and [string]$pod.status.phase -cnotin @('Pending', 'Running'))) {
            throw "legacy cohort Pod/$app 的 Running/Ready 状态不满足当前采集阶段。"
        }
        Assert-AppAndHashLabels -Labels $pod.metadata.labels -App $app -ExpectedHash $rsHash -What "Pod/$app"
        Assert-CanonicalContainerSpec -PodSpec $pod.spec -App $app -ExpectedImage $expectedImage -What "Pod/$app"
        if ($app -ceq 'etcd') {
            Assert-CanonicalEtcdStorage -PodSpec $pod.spec -What 'Pod/etcd'
        }
        $null = Add-CohortEvidence -Item $pod -Identity "pod/$K8sNamespace/$([string]$pod.metadata.name)" `
            -Extra "owner_uid=$($replicaSetEvidence.Uid);hash=$rsHash;image=$expectedImage"
    }

    if ($createdTimes.Count -ne 18) { throw "legacy cohort 对象数量必须为 18，实际=$($createdTimes.Count)。" }
    $minCreatedMS = ($createdTimes | Measure-Object -Minimum).Minimum
    $maxCreatedMS = ($createdTimes | Measure-Object -Maximum).Maximum
    if (($maxCreatedMS - $minCreatedMS) -gt $script:LocalLegacyAdoptionCohortMaxSpanMS) {
        throw "legacy cohort creationTimestamp 全体跨度超过 10 分钟(min=$minCreatedMS,max=$maxCreatedMS)。"
    }
    $canonicalLines = @("schema=legacy-infra-only-cohort-v1", "collected_at_ms=$CollectionTimeUnixMS") +
        @($fingerprintLines.ToArray() | Sort-Object -CaseSensitive)
    $canonical = ($canonicalLines -join "`n") + "`n"
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        $hex = ([BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($canonical)))).Replace('-', '').ToLowerInvariant()
    } finally { $sha.Dispose() }
    return [pscustomobject]@{
        FingerprintSha256 = "sha256:$hex"
        CollectionTimeUnixMS = $CollectionTimeUnixMS
        MinCreatedAtMS = [long]$minCreatedMS
        MaxCreatedAtMS = [long]$maxCreatedMS
        PvcUid = $pvcUid
        PvUid = $pvUid
    }
}

# 旧脚本没有来得及写 marker 的兼容收养必须额外证明这块 etcd 数据盘从未发生过任何写入。
# revision=1 + 整个 ds-auth prefix 无 key 只证明“当前数据盘尚未发生 etcd 写入”，不能单独
# 证明宿主目录的历史连续性。它只用于 markerless legacy 的窄收养前置；adopting 之后必须由
# 随机 continuity sentinel 证明同一数据内容一直延续到 pending/genesis CAS。
function Assert-LocalEtcdStorePristineForGenesis {
    param([Parameter(Mandatory = $true)][string]$KubeContext)
    $lines = @(& kubectl --context $KubeContext exec -n $K8sNamespace deployment/etcd -- `
        /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 `
        get /pandora/ds-auth/ --prefix --limit=1 --consistency=l -w json 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "读取 legacy infra-only etcd revision/prefix 失败:$($lines -join [Environment]::NewLine)"
    }
    $text = (($lines | ForEach-Object { $_.ToString() }) -join "`n").Trim()
    try { $result = $text | ConvertFrom-Json -ErrorAction Stop }
    catch { throw "legacy infra-only etcd revision/prefix 返回非法 JSON:$($_.Exception.Message)" }
    $header = $result.PSObject.Properties['header']
    if ($null -eq $header -or [long]$header.Value.revision -ne 1) {
        $actualRevision = if ($null -eq $header) { '<missing>' } else { [string]$header.Value.revision }
        throw "legacy infra-only 自动收养只接受从未写入的 etcd revision=1，实际=$actualRevision；请 -Reset 或受控迁移。"
    }
    $countProperty = $result.PSObject.Properties['count']
    $kvsProperty = $result.PSObject.Properties['kvs']
    $count = if ($null -ne $countProperty) { [long]$countProperty.Value } elseif ($null -ne $kvsProperty) { @($kvsProperty.Value).Count } else { 0 }
    if ($count -ne 0) {
        throw "legacy infra-only 自动收养要求 /pandora/ds-auth/ 整个前缀为空，实际 key count=$count。"
    }
    Write-Ok 'legacy infra-only 收养门禁通过（etcd revision=1，ds-auth prefix 为空）。'
}

function Invoke-WithLocalEtcdPortForward {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][scriptblock]$Action
    )
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    $listener.Start()
    try { $port = ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port }
    finally { $listener.Stop() }
    $stdout = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-etcd-pf-' + [guid]::NewGuid().ToString('N') + '.out')
    $stderr = $stdout + '.err'
    $proc = $null
    try {
        $proc = Start-Process -FilePath 'kubectl' -ArgumentList @(
            '--context', $KubeContext, '-n', $K8sNamespace, 'port-forward', 'service/etcd', "${port}:2379"
        ) -PassThru -WindowStyle Hidden -RedirectStandardOutput $stdout -RedirectStandardError $stderr
        $ready = $false
        $deadline = [DateTime]::UtcNow.AddSeconds(15)
        do {
            if ($proc.HasExited) { break }
            $client = [System.Net.Sockets.TcpClient]::new()
            try {
                $task = $client.ConnectAsync([System.Net.IPAddress]::Loopback, $port)
                if ($task.Wait(250) -and $client.Connected) { $ready = $true; break }
            } catch { } finally { $client.Dispose() }
            Start-Sleep -Milliseconds 200
        } while ([DateTime]::UtcNow -lt $deadline)
        if (-not $ready) {
            $detail = @(
                if (Test-Path $stdout) { Get-Content $stdout -Raw }
                if (Test-Path $stderr) { Get-Content $stderr -Raw }
            ) -join [Environment]::NewLine
            throw "etcd port-forward 未就绪(context=$KubeContext):$detail"
        }
        & $Action "127.0.0.1:$port"
    } finally {
        if ($null -ne $proc -and -not $proc.HasExited) {
            Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
            try { $proc.WaitForExit(5000) | Out-Null } catch { }
        }
        Remove-Item -LiteralPath $stdout, $stderr -Force -ErrorAction SilentlyContinue
    }
}

function Assert-LocalDsAuthBaseline {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][bool]$AllowFreshBootstrap,
        [string]$MinikubeProfile = '',
        [bool]$AllowLegacyInfraOnlyAdoption = $false,
        [string]$ExpectedAdoptionPvcUid = '',
        [string]$ExpectedAdoptionCohortFingerprintSha256 = '',
        [long]$LegacyAdoptionCollectionTimeUnixMS = 0,
        [string]$LegacyAdoptionCohortPreflightError = ''
    )
    if ($AllowFreshBootstrap -and [string]::IsNullOrWhiteSpace($MinikubeProfile)) {
        throw '允许本地 genesis 时必须显式提供已锁定的 minikube profile。'
    }

    # 同一台宿主机的 minikube 只允许一个 baseline/genesis 状态机运行。命名 Mutex 在进程
    # 异常退出时由 OS 自动标记 abandoned，不会像持久 ConfigMap 锁那样把下一次一键启动
    # 永久卡死；同时它封住两个 start.ps1 都曾读到 preinfra/adopting 后重放 prepare 的窗口。
    $mutexMaterial = "$KubeContext|$MinikubeProfile|$K8sNamespace"
    $mutexSha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $mutexHex = ([BitConverter]::ToString($mutexSha.ComputeHash(
                    [Text.Encoding]::UTF8.GetBytes($mutexMaterial)))).Replace('-', '').ToLowerInvariant()
    } finally { $mutexSha.Dispose() }
    $mutexScope = if ([Environment]::OSVersion.Platform -eq [PlatformID]::Win32NT) { 'Global\' } else { '' }
    $mutexName = "${mutexScope}PandoraDsAuthBaseline-$mutexHex"
    $baselineMutex = [System.Threading.Mutex]::new($false, $mutexName)
    $baselineMutexOwned = $false
    try {
        Write-Info "等待本机 DS-auth 一键状态机互斥锁(profile=$MinikubeProfile)..."
        try {
            $baselineMutexOwned = $baselineMutex.WaitOne([TimeSpan]::FromMinutes(30))
        } catch [System.Threading.AbandonedMutexException] {
            $baselineMutexOwned = $true
            Write-Warn '检测到上次启动进程异常退出；已安全接管 abandoned DS-auth 状态机互斥锁。'
        }
        if (-not $baselineMutexOwned) {
            throw '等待同一 minikube 的 DS-auth 一键状态机超过 30 分钟；拒绝并发执行 genesis。'
        }

        Invoke-WithLocalEtcdPortForward -KubeContext $KubeContext -Action {
        param($endpoint)
        Push-Location $ProjectRoot
        try {
            $readLines = @(& go run ./pkg/dsauthfence/cmd/dsauth-required --endpoints $endpoint `
                --min-epoch 2 --max-epoch 2 --min-policy-generation 3 --max-policy-generation 3 `
                --require-v3-activation-record 2>&1)
            $readExit = $LASTEXITCODE
            $marker = $null
            $markerState = ''
            $continuityToken = ''
            $observedV3Witness = $null
            if (-not [string]::IsNullOrWhiteSpace($MinikubeProfile)) {
                $marker = Get-LocalFreshGenesisIntent -KubeContext $KubeContext
                if ($null -ne $marker) {
                    $markerState = Assert-LocalFreshGenesisIntent -Marker $marker -KubeContext $KubeContext `
                        -MinikubeProfile $MinikubeProfile
                    $continuityToken = [string]$marker.data.PSObject.Properties[$script:LocalFreshDsAuthContinuityTokenField].Value
                }
                $observedV3Witness = Get-LocalObservedV3Witness -KubeContext $KubeContext
                if ($null -ne $observedV3Witness) {
                    Assert-LocalObservedV3Witness -Witness $observedV3Witness -KubeContext $KubeContext `
                        -MinikubeProfile $MinikubeProfile
                }
            }
            if ($readExit -eq 0) {
                # pending + exact V3 是 CAS 已提交、complete annotation 尚未写入时崩溃的正常恢复窗。
                # 精确 evidence 回读成功后，无论普通启动还是 Resume 都必须补齐 complete；否则
                # Resume 会让 writers 在永久 pending 上运行，后续数据丢失可能被误判为可重试 genesis。
                if ($markerState -cin @('preinfra', 'adopting')) {
                    throw "required policy 已是精确 V3，但 fresh marker 仍为 $markerState；该状态不可能由完整启动事务产生，拒绝伪造 genesis provenance。"
                }
                if ($markerState -cin @('pending', 'complete')) {
                    $evidenceTimeProperty = $marker.metadata.annotations.PSObject.Properties[$script:LocalFreshDsAuthMarkerEvidenceCompletedAnnotation]
                    $evidenceCompletedAtMS = if ($null -eq $evidenceTimeProperty) { 0L } else { [long]$evidenceTimeProperty.Value }
                    if ($evidenceCompletedAtMS -le 0) {
                        throw "marker=$markerState 且 required V3 已存在，但 marker 缺少 CAS 前持久化的 evidence time；拒绝把其它 V3 record 冒充本次 genesis。"
                    }
                    $exactEvidenceLines = @(& go run ./pkg/dsauthfence/cmd/dsauth-required --endpoints $endpoint `
                        --min-epoch 2 --max-epoch 2 --min-policy-generation 3 --max-policy-generation 3 `
                        --require-v3-activation-record `
                        --require-activation-evidence-sha256 $script:LocalFreshDsAuthEvidenceSha256 `
                        --require-activation-evidence-completed-at-ms $evidenceCompletedAtMS `
                        --require-genesis-continuity-token $continuityToken 2>&1)
                    if ($LASTEXITCODE -ne 0) {
                        throw "marker=$markerState 的 V3 genesis evidence 精确回读失败:$($exactEvidenceLines -join [Environment]::NewLine)"
                    }
                    $readLines = $exactEvidenceLines
                } elseif ($null -eq $marker -and -not [string]::IsNullOrWhiteSpace($MinikubeProfile)) {
                    $witnessCreated = Ensure-LocalObservedV3Witness -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile
                    if ($witnessCreated) {
                        # witness 创建与 etcd 不是同一事务；创建后必须再次线性证明 V3。若中间盘丢失，
                        # witness 已经终止后续自动 adoption，本次也在任何 writer 前 fail closed。
                        $witnessRecheck = @(& go run ./pkg/dsauthfence/cmd/dsauth-required --endpoints $endpoint `
                            --min-epoch 2 --max-epoch 2 --min-policy-generation 3 --max-policy-generation 3 `
                            --require-v3-activation-record 2>&1)
                        if ($LASTEXITCODE -ne 0) {
                            throw "observed V3 witness 建立后线性复查失败:$($witnessRecheck -join [Environment]::NewLine)"
                        }
                        $readLines = $witnessRecheck
                    }
                }
                if ($markerState -ceq 'pending') {
                    Set-LocalFreshGenesisIntentState -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile -TargetState complete
                }
                $readLines | ForEach-Object { Write-Host $_ }
                return
            }
            $readText = ($readLines | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
            $requiredIsMissing = $readText -match '(?i)required epoch missing'
            if ($requiredIsMissing -and $null -ne $observedV3Witness) {
                throw '同一 namespace/PVC 已有 immutable observed V3 witness，但 required policy 变成 missing；这是权威数据丢失，禁止 legacy 自动收养。请从快照恢复或显式 -Reset。'
            }
            if ($requiredIsMissing -and $markerState -ceq 'complete') {
                throw 'fresh DS-auth marker 已 complete，但同一绑定 PVC 的 required policy 变成 missing；这是权威数据丢失，禁止再次 genesis。请从快照恢复或显式 -Reset。'
            }
            if (-not $AllowFreshBootstrap) {
                throw "本地 required policy 不是精确 V3；Resume 禁止让 Hub 以 staging-only 卡住。请执行受控 V2->V3 迁移，或显式 -Reset。详情:$readText"
            }
            if (-not $requiredIsMissing) {
                throw "fresh 自动 genesis 只接受 missing；检测到 V1/V2/非法状态时拒绝猜测恢复，需 -Reset 或受控 V2->V3 迁移:$readText"
            }

            if ($null -eq $marker) {
                if (-not $AllowLegacyInfraOnlyAdoption -or [string]::IsNullOrWhiteSpace($ExpectedAdoptionPvcUid)) {
                    throw 'required policy missing 且没有可信 fresh marker；当前现场不满足一键兼容收养条件。请显式 -Reset，禁止仅凭已有 profile 猜测恢复。'
                }
                if ($ExpectedAdoptionCohortFingerprintSha256 -cnotmatch '^sha256:[0-9a-f]{64}$' -or
                    $LegacyAdoptionCollectionTimeUnixMS -le 0) {
                    $cohortDetail = if ([string]::IsNullOrWhiteSpace($LegacyAdoptionCohortPreflightError)) {
                        '启动写入前没有形成合法 cohort fingerprint。'
                    } else { $LegacyAdoptionCohortPreflightError }
                    throw "required policy missing；legacy infra-only 短时同批 cohort 门禁未通过:$cohortDetail"
                }
                # 初次 cohort 必须在任何 apply 前采集；此处沿用完全相同的采集时刻重算，UID/owner/
                # creationTimestamp/PV 绑定任一变化都会改变 fingerprint，禁止把 apply 后的新对象收养。
                $recheckedCohort = Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext $KubeContext `
                    -CollectionTimeUnixMS $LegacyAdoptionCollectionTimeUnixMS -ExpectedPvcUid $ExpectedAdoptionPvcUid
                if ([string]$recheckedCohort.FingerprintSha256 -cne $ExpectedAdoptionCohortFingerprintSha256) {
                    throw "legacy infra-only cohort fingerprint 在 apply 前后漂移(expected=$ExpectedAdoptionCohortFingerprintSha256,actual=$($recheckedCohort.FingerprintSha256))。"
                }
                # 只对旧脚本刚留下的同批 cohort 开放：revision=1、整个 ds-auth 前缀为空、
                # 全局没有 Pandora/Agones writer。先 create-only 落 adopting+PVC+cohort+随机 token；
                # exact continuity sentinel 建立前 adopting 绝不具备 genesis CAS 权限。
                Assert-LocalEtcdStorePristineForGenesis -KubeContext $KubeContext
                Assert-NoLocalFreshGenesisWriters -KubeContext $KubeContext
                New-LocalAdoptedGenesisIntent -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile `
                    -ExpectedPvcUid $ExpectedAdoptionPvcUid `
                    -ExpectedCohortFingerprintSha256 $ExpectedAdoptionCohortFingerprintSha256
                $marker = Get-LocalFreshGenesisIntent -KubeContext $KubeContext
                $markerState = Assert-LocalFreshGenesisIntent -Marker $marker -KubeContext $KubeContext `
                    -MinikubeProfile $MinikubeProfile
                $continuityToken = [string]$marker.data.PSObject.Properties[$script:LocalFreshDsAuthContinuityTokenField].Value
            }

            if ($markerState -cin @('preinfra', 'adopting')) {
                # preinfra/adopting 允许 create/recover continuity sentinel，但尚无权执行 genesis。
                # 若 sentinel 不存在，必须紧邻 prepare 再做 pristine -> zero-writer -> pristine；
                # 若已存在，只允许 Go prepare 以同 token + 空完整 authority prefix 幂等回读。
                $sentinelReadLines = @(& go run ./pkg/dsauthfence/cmd/dsauth-activate --endpoints $endpoint `
                    --verify-genesis-continuity --genesis-continuity-token $continuityToken 2>&1)
                $sentinelReadExit = $LASTEXITCODE
                $sentinelReadText = ($sentinelReadLines | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
                if ($sentinelReadExit -ne 0) {
                    if ($sentinelReadText -notmatch '(?i)genesis continuity sentinel missing') {
                        throw "读取 genesis continuity sentinel 失败/漂移:$sentinelReadText"
                    }
                    Assert-LocalEtcdStorePristineForGenesis -KubeContext $KubeContext
                    Assert-NoLocalFreshGenesisWriters -KubeContext $KubeContext
                    Assert-LocalEtcdStorePristineForGenesis -KubeContext $KubeContext
                }
                # Mutex 已排除其它新版一键进程；prepare 紧前仍重新线性回读 marker，防止旧分支
                # 越过已推进的 pending。直接人工改 etcd/调用内部 CLI 不属于一键状态机的安全契约。
                $prepareMarker = Get-LocalFreshGenesisIntent -KubeContext $KubeContext
                $prepareMarkerState = Assert-LocalFreshGenesisIntent -Marker $prepareMarker `
                    -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile
                $prepareTokenProperty = $prepareMarker.data.PSObject.Properties[$script:LocalFreshDsAuthContinuityTokenField]
                if ($prepareMarkerState -cnotin @('preinfra', 'adopting') -or
                    $null -eq $prepareTokenProperty -or [string]$prepareTokenProperty.Value -cne $continuityToken) {
                    throw "continuity prepare 紧前 marker 已漂移(state=$prepareMarkerState)；禁止旧分支重放 sentinel。"
                }
                # Txn 可能已在服务端提交、客户端却因瞬时断链拿到失败。prepare 本身是同 token +
                # authority-prefix-empty 的幂等事务；在 Mutex 内有界重试可在本次双击中消解 ambiguous
                # commit，且每次重试前都重新核对 marker state/token，绝不跨过 pending。
                $prepareDeadline = [DateTime]::UtcNow.AddSeconds(45)
                while ($true) {
                    $prepareMarker = Get-LocalFreshGenesisIntent -KubeContext $KubeContext
                    $prepareMarkerState = Assert-LocalFreshGenesisIntent -Marker $prepareMarker `
                        -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile
                    $prepareTokenProperty = $prepareMarker.data.PSObject.Properties[$script:LocalFreshDsAuthContinuityTokenField]
                    if ($prepareMarkerState -cnotin @('preinfra', 'adopting') -or
                        $null -eq $prepareTokenProperty -or [string]$prepareTokenProperty.Value -cne $continuityToken) {
                        throw "continuity prepare 重试前 marker 已漂移(state=$prepareMarkerState)；禁止旧分支重放 sentinel。"
                    }
                    $prepareLines = @(& go run ./pkg/dsauthfence/cmd/dsauth-activate --endpoints $endpoint `
                        --prepare-zero-writer-genesis-v3 --apply --genesis-continuity-token $continuityToken 2>&1)
                    if ($LASTEXITCODE -eq 0) { break }
                    $prepareText = ($prepareLines | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
                    if ([DateTime]::UtcNow -ge $prepareDeadline) {
                        throw "45 秒内无法 create-only 建立/精确回读 genesis continuity sentinel:$prepareText"
                    }
                    Write-Warn 'continuity prepare 返回瞬时/不确定结果；同一键入口内按 exact token 有界重试...'
                    Start-Sleep -Seconds 2
                }
                $sentinelVerifyLines = @(& go run ./pkg/dsauthfence/cmd/dsauth-activate --endpoints $endpoint `
                    --verify-genesis-continuity --genesis-continuity-token $continuityToken 2>&1)
                if ($LASTEXITCODE -ne 0) {
                    throw "pending 前无法线性证明 exact genesis continuity sentinel:$($sentinelVerifyLines -join [Environment]::NewLine)"
                }
                Set-LocalFreshGenesisIntentState -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile -TargetState pending
                $marker = Get-LocalFreshGenesisIntent -KubeContext $KubeContext
                $markerState = Assert-LocalFreshGenesisIntent -Marker $marker -KubeContext $KubeContext `
                    -MinikubeProfile $MinikubeProfile
            }
            if ($markerState -cne 'pending') {
                throw "required policy missing 时只允许绑定同一 PVC 的 pending marker 执行 genesis，实际 state=$markerState。"
            }

            # pending 以后 sentinel 只许精确读取，绝不重建。若 PVC 内容被清空，即使 PVC UID 未变，
            # sentinel missing 也会在 writer/CAS 前 fail closed。Go genesis CAS 还会在同一事务比较
            # exact sentinel、activation lock，以及除 lock 外的完整 DS-auth authority prefix 为空。
            $pendingSentinelLines = @(& go run ./pkg/dsauthfence/cmd/dsauth-activate --endpoints $endpoint `
                --verify-genesis-continuity --genesis-continuity-token $continuityToken 2>&1)
            if ($LASTEXITCODE -ne 0) {
                throw "pending marker 绑定的数据盘 continuity sentinel 缺失/漂移，禁止再次 genesis:$($pendingSentinelLines -join [Environment]::NewLine)"
            }
            Assert-NoLocalFreshGenesisWriters -KubeContext $KubeContext
            $evidenceText = 'pandora-local-fresh-zero-writer-v1-to-v3/v1'
            $sha = [System.Security.Cryptography.SHA256]::Create()
            try {
                $evidenceHex = ([BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($evidenceText)))).Replace('-', '').ToLowerInvariant()
            } finally { $sha.Dispose() }
            $evidence = "sha256:$evidenceHex"
            if ($evidence -cne $script:LocalFreshDsAuthEvidenceSha256) {
                throw "本地 genesis evidence 常量与计算值漂移(expected=$($script:LocalFreshDsAuthEvidenceSha256),actual=$evidence)。"
            }
            $completedAtMS = Get-OrSetLocalFreshGenesisEvidenceCompletedAtMS -KubeContext $KubeContext `
                -MinikubeProfile $MinikubeProfile
            Write-Info '在任何 writer 启动前执行 missing->V3 zero-writer genesis 单事务 CAS（exact continuity + required/record create-only + activation lock + 除 lock 外完整 authority prefix 为空）...'
            $advanceDeadline = [DateTime]::UtcNow.AddSeconds(45)
            while ($true) {
                $advanceLines = @(& go run ./pkg/dsauthfence/cmd/dsauth-activate --endpoints $endpoint `
                    --zero-writer-genesis-v3 --apply --activation-evidence-sha256 $evidence `
                    --activation-evidence-completed-at-ms $completedAtMS `
                    --genesis-continuity-token $continuityToken 2>&1)
                if ($LASTEXITCODE -eq 0) { break }
                $advanceText = ($advanceLines | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
                # 并发的一键进程可能已用同 marker/evidence 完成 CAS；先做同一事务 exact 回读。
                $concurrentVerify = @(& go run ./pkg/dsauthfence/cmd/dsauth-required --endpoints $endpoint `
                    --min-epoch 2 --max-epoch 2 --min-policy-generation 3 --max-policy-generation 3 `
                    --require-v3-activation-record --require-activation-evidence-sha256 $evidence `
                    --require-activation-evidence-completed-at-ms $completedAtMS `
                    --require-genesis-continuity-token $continuityToken 2>&1)
                if ($LASTEXITCODE -eq 0) {
                    $advanceLines = @('并发启动器已完成同一份 exact genesis CAS；按幂等成功继续。')
                    break
                }
                if ($advanceText -notmatch '(?i)activation lock is held' -or [DateTime]::UtcNow -ge $advanceDeadline) {
                    throw "fresh minikube missing->V3 zero-writer CAS 失败:$advanceText"
                }
                Write-Warn '检测到上一进程遗留的短租约 activation lock；在一键入口内有界等待后自动重试...'
                Start-Sleep -Seconds 3
            }
            $verifyLines = @(& go run ./pkg/dsauthfence/cmd/dsauth-required --endpoints $endpoint `
                --min-epoch 2 --max-epoch 2 --min-policy-generation 3 --max-policy-generation 3 `
                --require-v3-activation-record `
                --require-activation-evidence-sha256 $evidence `
                --require-activation-evidence-completed-at-ms $completedAtMS `
                --require-genesis-continuity-token $continuityToken 2>&1)
            if ($LASTEXITCODE -ne 0) {
                throw "bootstrap 后无法线性证明 required policy 精确 V3:$($verifyLines -join [Environment]::NewLine)"
            }
            $advanceLines | ForEach-Object { Write-Host $_ }
            $verifyLines | ForEach-Object { Write-Host $_ }
            Set-LocalFreshGenesisIntentState -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile -TargetState complete
            } finally { Pop-Location }
        }
    } finally {
        if ($baselineMutexOwned) {
            try { $baselineMutex.ReleaseMutex() } catch { }
        }
        $baselineMutex.Dispose()
    }
}

function Assert-NoLegacyDsFleets {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [switch]$LocalDevelopment
    )
    $legacy = [System.Collections.Generic.List[string]]::new()
    foreach ($name in @('pandora-battle', 'pandora-hub')) {
        $lines = @(& kubectl --context $KubeContext get "fleet/$name" -n default --ignore-not-found -o name 2>&1)
        if ($LASTEXITCODE -ne 0) {
            $errText = (($lines | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine)
            # 全新集群(如 -Reset 后)Agones 尚未安装,Fleet CRD 不存在。CRD 都没有就不可能残留
            # legacy Fleet,视为“无”继续,而非当成读取失败中止。
            if ($errText -match '(?i)the server doesn''t have a resource type\s+"?fleet"?') {
                continue
            }
            throw "读取 legacy Fleet/$name 失败:$errText"
        }
        if (-not [string]::IsNullOrWhiteSpace((($lines | ForEach-Object { $_.ToString() }) -join '').Trim())) {
            $legacy.Add($name)
        }
    }
    if ($legacy.Count -eq 0) { return }
    if ($LocalDevelopment) {
        throw "检测到旧单轨 Fleet:$($legacy -join ',')。为防新旧 allocator 同时分配，本地不自动删除在跑 DS；请用 -Mode k8s -Reset 显式重建。"
    }
    throw "检测到旧单轨 Fleet:$($legacy -join ',')，拒绝与 Stable/Canary 四 Fleet 静默并存。" +
          'Battle 只能在确认无 Allocated 后删除；Hub 本仓没有可机械证明玩家已排空的查询，发布脚本永不自动删除。' +
          '请先走现有 drain/迁移流程，保存审计证据并由运维显式移除 legacy Fleet，然后重跑；本次尚未 apply/push。'
}

function Assert-NoLegacyDSTicketSignerSecret {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [switch]$LocalDevelopment
    )
    $lines = @(& kubectl --context $KubeContext get secret/pandora-dsticket -n $K8sNamespace `
        --ignore-not-found -o name 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "读取 legacy Secret/pandora-dsticket 失败:$($lines -join [Environment]::NewLine)"
    }
    if ([string]::IsNullOrWhiteSpace((($lines | ForEach-Object { $_.ToString() }) -join '').Trim())) { return }
    if ($LocalDevelopment) {
        throw '检测到 legacy Secret/pandora-dsticket；新契约只接受 pandora-dsticket-signer-rN。为防旧私钥残留或误挂载，请用 -Mode k8s -Reset 显式重建本地集群。'
    }
    throw '检测到 legacy Secret/pandora-dsticket；online 发布拒绝让非 revisioned 私钥与 pandora-dsticket-signer-rN 并存。请审计迁移并由运维显式移除旧对象后重跑；本次尚未 push/apply。'
}

function Assert-ExistingLocalEtcdPersistence {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [bool]$AllowPendingPvcForPreinfra = $false
    )
    $lines = @(& kubectl --context $KubeContext get deployment/etcd -n $K8sNamespace --ignore-not-found -o json 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "读取本地 Deployment/etcd 失败:$($lines -join [Environment]::NewLine)" }
    $text = (($lines | ForEach-Object { $_.ToString() }) -join "`n").Trim()
    if ([string]::IsNullOrWhiteSpace($text)) { return }
    try { $deploy = $text | ConvertFrom-Json -ErrorAction Stop }
    catch { throw "Deployment/etcd 返回非法 JSON:$($_.Exception.Message)" }
    $container = @($deploy.spec.template.spec.containers | Where-Object name -eq 'etcd')
    $mount = @($container.volumeMounts | Where-Object { $_.name -ceq 'data' -and $_.mountPath -ceq '/etcd-data' })
    $volume = @($deploy.spec.template.spec.volumes | Where-Object {
        $_.name -ceq 'data' -and [string]$_.persistentVolumeClaim.claimName -ceq 'etcd-data'
    })
    if ([string]$deploy.spec.strategy.type -cne 'Recreate' -or $container.Count -ne 1 -or
        $mount.Count -ne 1 -or $volume.Count -ne 1) {
        throw '现有本地 etcd 仍是旧版非持久布署；直接 apply PVC 会重建 Pod 并丢失 required_writer_epoch/capability。' +
              '已在任何集群写入前中止。本地可用 -Mode k8s -Reset 显式重建；若必须保留现场，先做 etcd snapshot + restore 到 PVC 并审计 required policy，禁止无证据自动 missing=>V3。'
    }
    # Deployment 已存在却找不到它声明的数据盘，说明旧权威盘被删/漂移。必须在 apply infra
    # 自动重建同名空 PVC 之前阻断，否则一个“看起来配置正确”的新空盘会被误当成 fresh。
    $pvcLines = @(& kubectl --context $KubeContext get pvc/etcd-data -n $K8sNamespace --ignore-not-found -o json 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "读取现有本地 PVC/etcd-data 失败:$($pvcLines -join [Environment]::NewLine)" }
    $pvcText = (($pvcLines | ForEach-Object { $_.ToString() }) -join "`n").Trim()
    if ([string]::IsNullOrWhiteSpace($pvcText)) {
        throw '现有 Deployment/etcd 引用了 PVC/etcd-data，但该 PVC 已缺失；这是权威数据盘丢失，禁止 apply 自动创建空盘。请从快照恢复或显式 -Reset。'
    }
    try { $pvc = $pvcText | ConvertFrom-Json -ErrorAction Stop }
    catch { throw "现有 PVC/etcd-data 返回非法 JSON:$($_.Exception.Message)" }
    $pvcPhase = [string]$pvc.status.phase
    if ([string]::IsNullOrWhiteSpace([string]$pvc.metadata.uid) -or
        ($pvcPhase -cne 'Bound' -and -not ($AllowPendingPvcForPreinfra -and $pvcPhase -ceq 'Pending'))) {
        throw '现有 PVC/etcd-data 必须为 Bound 且 UID 非空；只有已验证 preinfra marker 的中断恢复可暂时等待 Pending PVC。'
    }
}

function Wait-KubeApiServerReady {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [ValidateRange(1, 60)][int]$TimeoutSeconds = 45
    )
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $lines = @(& kubectl --context $KubeContext get --raw=/readyz 2>&1)
        $readExit = $LASTEXITCODE
        $text = (($lines | ForEach-Object { $_.ToString() }) -join "`n").Trim()
        if ($readExit -eq 0 -and $text -ceq 'ok') {
            Write-Ok "kube-apiserver readyz 已就绪(context=$KubeContext)。"
            return
        }
        Start-Sleep -Milliseconds 500
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "kube-apiserver 在 ${TimeoutSeconds}s 内未通过 /readyz；Resume 尚未等待旧业务 Ready、apply 或 rollout。"
}

function Get-RegistryImageDescriptor {
    param([Parameter(Mandatory = $true)][string]$Reference)
    $lines = @(& docker buildx imagetools inspect $Reference 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "读取 registry manifest 失败:$Reference;$($lines -join [Environment]::NewLine)"
    }
    $text = ($lines | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
    return ConvertFrom-PandoraImagetoolsInspect -Reference $Reference -Output $text
}

function Assert-RemoteImageTagAbsent {
    param([Parameter(Mandatory = $true)][string]$Reference)
    $lines = @(& docker buildx imagetools inspect $Reference 2>&1)
    if ($LASTEXITCODE -eq 0) {
        throw "不可变发布 tag 已存在，禁止覆盖:$Reference"
    }
    $text = ($lines | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
    if (-not (Test-PandoraManifestNotFoundOutput -Output $text)) {
        throw "无法证明远端 tag 不存在（可能是鉴权/TLS/网络错误），禁止继续 push:$Reference;$($lines -join [Environment]::NewLine)"
    }
}

function Push-ImageAndResolveDigest {
    param(
        [Parameter(Mandatory = $true)][string]$Local,
        [Parameter(Mandatory = $true)][string]$Remote
    )
    docker tag $Local $Remote
    Assert-LastExit "docker tag $Local -> $Remote"
    $pushLines = @(& docker push $Remote 2>&1)
    $pushExit = $LASTEXITCODE
    foreach ($line in $pushLines) { Write-Host $line }
    if ($pushExit -ne 0) { throw "推送失败:$Remote" }
    $pushText = ($pushLines | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
    $pushDigest = Get-PandoraPushDigest -Output $pushText
    $registry = Get-RegistryImageDescriptor $Remote
    if ($registry.Digest -cne $pushDigest) {
        throw "push digest 与 registry 回读不一致:$Remote push=$pushDigest registry=$($registry.Digest)"
    }
    return $registry
}

function Assert-CleanOnlineReleaseSource {
    $lines = @(& git -C $ProjectRoot status --porcelain=v1 --untracked-files=normal 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "online BuildPush 无法读取 git worktree 状态:$($lines -join [Environment]::NewLine)" }
    $text = ($lines | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
    Assert-PandoraCleanGitStatus -Output $text
}

function Assert-LocalImageRevision {
    param(
        [Parameter(Mandatory = $true)][string]$Reference,
        [Parameter(Mandatory = $true)][string]$Expected
    )
    $lines = @(& docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' $Reference 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "无法读取本地镜像 provenance:$Reference;$($lines -join [Environment]::NewLine)" }
    $actual = (($lines | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine).Trim()
    Assert-PandoraImageRevision -Reference $Reference -Actual $actual -Expected $Expected
}

function Invoke-KubectlClientContract {
    param(
        [Parameter(Mandatory = $true)][string]$Manifest,
        [Parameter(Mandatory = $true)][string]$JsonPath,
        [Parameter(Mandatory = $true)][string]$Action
    )
    $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-contract-' + [guid]::NewGuid().ToString('N') + '.yaml')
    try {
        [System.IO.File]::WriteAllText($tmp, $Manifest, [System.Text.UTF8Encoding]::new($false))
        $lines = @(& kubectl create --dry-run=client --validate=false -f $tmp -o "jsonpath=$JsonPath" 2>&1)
        if ($LASTEXITCODE -ne 0) { throw "$Action 结构化解析失败:$($lines -join [Environment]::NewLine)" }
        return @($lines | ForEach-Object { $_.ToString() })
    } finally {
        Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
    }
}

function Get-PandoraWorkloadContractRows {
    param([Parameter(Mandatory = $true)][string]$Manifest)
    $jsonPath = '{.kind}{"\t"}{.metadata.name}{"\t"}{.spec.template.spec.containers[*].name}{"\t"}{.spec.template.spec.containers[*].image}{"\t"}{.spec.template.metadata.annotations.pandora\.dev/image-digest}{"\n"}'
    return @(Invoke-KubectlClientContract -Manifest $Manifest -JsonPath $jsonPath -Action 'online Deployment manifest')
}

function Get-PandoraHubAllocatorSingleWriterContractRows {
    param([Parameter(Mandatory = $true)][string]$Manifest)
    $jsonPath = '{.kind}{"\t"}{.metadata.name}{"\t"}{.spec.replicas}{"\t"}{.spec.strategy.type}{"\t"}{.spec.strategy.rollingUpdate}{"\n"}'
    return @(Invoke-KubectlClientContract -Manifest $Manifest -JsonPath $jsonPath -Action 'hub-allocator single-writer manifest')
}

function Get-PandoraDSTicketSignerContractRows {
    param([Parameter(Mandatory = $true)][string]$Manifest)
    $jsonPath = '{.kind}{"\t"}{.metadata.name}{"\t"}{.spec.template.spec.volumes[?(@.name=="dsticket")].secret.secretName}{"\t"}{.spec.template.spec.volumes[?(@.name=="dsticket-jwks")].configMap.name}{"\n"}'
    return @(Invoke-KubectlClientContract -Manifest $Manifest -JsonPath $jsonPath -Action 'online DSTicket signer manifest')
}

function Get-OnlineOptionalKubectlJsonObject {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$Action
    )
    $raw = @(& kubectl --context $KubeContext @Arguments 2>$null)
    if ($LASTEXITCODE -ne 0) { throw "$Action 失败(exit=$LASTEXITCODE)" }
    $text = (($raw | ForEach-Object { $_.ToString() }) -join "`n").Trim()
    if ([string]::IsNullOrWhiteSpace($text)) { return $null }
    try { return ($text | ConvertFrom-Json -ErrorAction Stop) }
    catch { throw "$Action 返回非法 JSON:$($_.Exception.Message)" }
}

function Get-OnlineDSTicketKeyMaterialContract {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][ValidateRange(1, 2147483647)][int]$Revision,
        [AllowEmptyString()][string]$ExpectedActiveKid = '',
        [AllowNull()]$SignerObject = $null
    )
    $dstTicketSignerSecretName = "pandora-dsticket-signer-r$Revision"
    $signer = $SignerObject
    if ($null -eq $signer) {
        $signer = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', "secret/$dstTicketSignerSecretName", '-n', $K8sNamespace, '-o', 'json') `
            -Action "读取 immutable DSTicket signer Secret/$dstTicketSignerSecretName"
    }
    $jwksName = "pandora-dsticket-jwks-r$Revision"
    $contract = $null
    foreach ($namespace in @('default', $K8sNamespace)) {
        $jwks = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', "configmap/$jwksName", '-n', $namespace, '-o', 'json') `
            -Action "读取 $namespace immutable DSTicket 公钥 JWKS ConfigMap"
        $current = Assert-PandoraDSTicketKubernetesObjects -SecretObject $signer `
            -ConfigMapObject $jwks -ExpectedRevision $Revision `
            -ExpectedActiveKid $ExpectedActiveKid -ExpectedConfigMapNamespace $namespace
        if ($null -ne $contract -and
            ($current.ActiveKid -cne $contract.ActiveKid -or $current.JwksSha256 -cne $contract.JwksSha256)) {
            throw 'default(DS) 与 pandora(Login) namespace 的 DSTicket JWKS 副本不一致。'
        }
        $contract = $current
    }
    return $contract
}

# 一次读取普通发布所需的完整 DSTicket 集群快照。不用 field selector 排除
# deletionTimestamp；terminating Pod/GameServer/marker 依然可以签票或验票，必须进契约。
function Get-OnlineLiveDSTicketRevisionRows {
    param([Parameter(Mandatory = $true)][string]$KubeContext)

    $expectedSigners = @('login', 'matchmaker', 'matchmaker-pve', 'hub-allocator')
    $expectedFleets = @('pandora-battle-stable', 'pandora-battle-canary', 'pandora-hub-stable', 'pandora-hub-canary')
    $namespace = Get-OnlineOptionalKubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', "namespace/$K8sNamespace", '--ignore-not-found', '-o', 'json') `
        -Action "读取 Namespace/$K8sNamespace"
    if ($null -ne $namespace -and (Test-PandoraKubernetesObjectDeleting $namespace)) {
        throw "Namespace/$K8sNamespace 正在终止，拒绝普通发布。"
    }

    $allDeployments = @()
    $pandoraPods = @()
    $allReplicaSets = @()
    $activationMarkers = @()
    $terminalMarkers = @()
    $fixedConfig = $null
    if ($null -ne $namespace) {
        $deploymentList = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', 'deployments', '-n', $K8sNamespace, '-o', 'json') -Action '读取全部 pandora Deployment'
        $allDeployments = @($deploymentList.items)
        $podList = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', 'pods', '-n', $K8sNamespace, '-o', 'json') -Action '读取全部 pandora Pod（含 terminating）'
        $pandoraPods = @($podList.items)
        $replicaSetList = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', 'replicasets', '-n', $K8sNamespace, '-o', 'json') `
            -Action '读取全部 pandora ReplicaSet（含 terminating）'
        $allReplicaSets = @($replicaSetList.items)
        $configMaps = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', 'configmaps', '-n', $K8sNamespace, '-o', 'json') `
            -Action '读取全部 pandora ConfigMap（核对 DSTicket marker 集）'
        foreach ($configMap in @($configMaps.items)) {
            $name = [string]$configMap.metadata.name
            $component = [string]$configMap.metadata.labels.'app.kubernetes.io/component'
            $looksLikeMarker = $name -cmatch '^pandora-dsticket-(?:activation-signer-r|retired-r)' -or
                $component -ceq 'dsticket-rotation-audit'
            if (-not $looksLikeMarker) { continue }
            if (Test-PandoraKubernetesObjectDeleting $configMap) {
                throw "DSTicket marker ConfigMap/$name 正在终止，拒绝普通发布。"
            }
            if ($name -cmatch '^pandora-dsticket-activation-signer-r[1-9][0-9]*$') {
                $activationMarkers += $configMap
            } elseif ($name -cmatch '^pandora-dsticket-retired-r[1-9][0-9]*$') {
                $terminalMarkers += $configMap
            } else {
                throw "未知/漂移的 DSTicket marker ConfigMap/$name，拒绝普通发布。"
            }
        }
        $fixedConfig = Get-OnlineOptionalKubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', 'secret/pandora-config', '-n', $K8sNamespace, '--ignore-not-found', '-o', 'json') `
            -Action '读取 fixed Secret/pandora-config'
    }

    $fleetList = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'fleets.agones.dev', '-n', 'default', '-o', 'json') `
        -Action '读取全部 Agones Fleet'
    # parent controller 必须在 RS/GSS/Pod 尚未生成前就完成全量判定；相关但不属于
    # 四 signer/四 Fleet 的 paused/zero 对象不能留到异步补出子对象后才发现。
    $controllerScope = Assert-PandoraOrdinaryDSTicketControllerScope `
        -DeploymentObjects $allDeployments -FleetObjects @($fleetList.items)
    $deploymentObjects = @($controllerScope.DeploymentObjects)
    $fleetObjects = @($controllerScope.FleetObjects)
    $deploymentRows = [System.Collections.Generic.List[string]]::new()
    foreach ($service in $expectedSigners) {
        $matches = @($deploymentObjects | Where-Object { [string]$_.metadata.name -ceq $service })
        if ($matches.Count -eq 0) { continue }
        if ($matches.Count -ne 1) { throw "Deployment/$service 数量=$($matches.Count)，拒绝普通发布。" }
        $volumes = @($matches[0].spec.template.spec.volumes)
        $configRefs = @($volumes | Where-Object { [string]$_.name -ceq 'conf' } | ForEach-Object { [string]$_.secret.secretName })
        $signerRefs = @($volumes | Where-Object { [string]$_.name -ceq 'dsticket' } | ForEach-Object { [string]$_.secret.secretName })
        $jwksRefs = @($volumes | Where-Object { [string]$_.name -ceq 'dsticket-jwks' } | ForEach-Object { [string]$_.configMap.name })
        $deploymentRows.Add("$service`t$($configRefs -join ',')`t$($signerRefs -join ',')`t$($jwksRefs -join ',')")
    }

    $fleetRows = [System.Collections.Generic.List[string]]::new()
    foreach ($fleet in $expectedFleets) {
        $matches = @($fleetObjects | Where-Object { [string]$_.metadata.name -ceq $fleet })
        if ($matches.Count -eq 0) { continue }
        if ($matches.Count -ne 1) { throw "Fleet/$fleet 数量=$($matches.Count)，拒绝普通发布。" }
        $containers = @($matches[0].spec.template.spec.template.spec.containers)
        $revisions = @($containers | ForEach-Object { @($_.env) } |
            Where-Object { [string]$_.name -ceq 'PANDORA_DSTICKET_KEYSET_REVISION' } | ForEach-Object { [string]$_.value })
        $volumes = @($matches[0].spec.template.spec.template.spec.volumes)
        $jwksRefs = @($volumes | Where-Object { [string]$_.name -ceq 'dsticket-jwks' } | ForEach-Object { [string]$_.configMap.name })
        $fleetRows.Add("$fleet`t$($revisions -join ',')`t$($jwksRefs -join ',')")
    }

    $gameServers = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'gameservers.agones.dev', '-n', 'default', '-o', 'json') `
        -Action '读取全部 GameServer（含 terminating）'
    $gameServerSets = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'gameserversets.agones.dev', '-n', 'default', '-o', 'json') `
        -Action '读取全部 GameServerSet（含 terminating）'
    $defaultPods = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'pods', '-n', 'default', '-o', 'json') `
        -Action '读取 default Pod（含 terminating/orphan DS）'
    $childScope = Get-PandoraOrdinaryDSTicketChildScope `
        -PandoraPodObjects $pandoraPods -ReplicaSetObjects $allReplicaSets `
        -GameServerObjects @($gameServers.items) -GameServerSetObjects @($gameServerSets.items) `
        -DefaultPodObjects @($defaultPods.items)

    return [pscustomobject]@{
        DeploymentRows = @($deploymentRows)
        FleetRows = @($fleetRows)
        DeploymentObjects = @($deploymentObjects)
        FleetObjects = @($fleetObjects)
        SignerPodObjects = @($childScope.SignerPodObjects)
        ReplicaSetObjects = @($childScope.ReplicaSetObjects)
        GameServerObjects = @($gameServers.items)
        GameServerSetObjects = @($childScope.GameServerSetObjects)
        DSPodObjects = @($childScope.DSPodObjects)
        ActivationMarkers = @($activationMarkers)
        TerminalMarkers = @($terminalMarkers)
        FixedConfigSecret = $fixedConfig
        LegacyControllerObjects = @($controllerScope.LegacyControllerObjects) + @($childScope.LegacyControllerObjects)
    }
}

function Assert-OnlineOrdinaryDSTicketState {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][ValidateRange(1, 2147483647)][int]$RequestedRevision
    )
    $snapshot = Get-OnlineLiveDSTicketRevisionRows -KubeContext $KubeContext
    return Assert-PandoraOrdinaryReleaseDSTicketState `
        -DeploymentRows $snapshot.DeploymentRows -FleetRows $snapshot.FleetRows `
        -RequestedRevision $RequestedRevision -DeploymentObjects $snapshot.DeploymentObjects `
        -FleetObjects $snapshot.FleetObjects -SignerPodObjects $snapshot.SignerPodObjects `
        -ReplicaSetObjects $snapshot.ReplicaSetObjects -GameServerObjects $snapshot.GameServerObjects `
        -GameServerSetObjects $snapshot.GameServerSetObjects -DSPodObjects $snapshot.DSPodObjects `
        -ActivationMarkers $snapshot.ActivationMarkers -TerminalMarkers $snapshot.TerminalMarkers `
        -FixedConfigSecret $snapshot.FixedConfigSecret -LegacyControllerObjects $snapshot.LegacyControllerObjects
}

function Enter-OnlineDSTicketOperationLock {
    param([Parameter(Mandatory = $true)][string]$KubeContext)
    $holderId = [guid]::NewGuid().ToString('N')
    $operation = 'ordinary-online'
    $object = New-PandoraDSTicketOperationLockObject -HolderId $holderId -Operation $operation
    $json = $object | ConvertTo-Json -Depth 20 -Compress
    $createdLines = @($json | & kubectl --context $KubeContext create -f - -o json 2>$null)
    if ($LASTEXITCODE -ne 0) {
        throw '获取 DSTicket 操作互斥锁失败；可能已有普通发布/轮换正在进行，或存在崩溃残留锁。' +
              '请只读审计 pandora/ConfigMap/pandora-dsticket-operation-lock；禁止自动抢锁/按本机时间判过期。'
    }
    $createdText = (($createdLines | ForEach-Object { $_.ToString() }) -join "`n").Trim()
    try { $created = $createdText | ConvertFrom-Json -ErrorAction Stop }
    catch { throw "DSTicket 操作锁 create 返回非法 JSON:$($_.Exception.Message)" }
    $identity = Assert-PandoraDSTicketOperationLockContract -LockObject $created `
        -HolderId $holderId -Operation $operation -RequireLiveIdentity
    $live = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'configmap/pandora-dsticket-operation-lock', '-n', $K8sNamespace, '-o', 'json') `
        -Action '回读 DSTicket 操作互斥锁'
    $liveIdentity = Assert-PandoraDSTicketOperationLockContract -LockObject $live `
        -HolderId $holderId -Operation $operation -RequireLiveIdentity
    if ($liveIdentity.Uid -cne $identity.Uid -or $liveIdentity.ResourceVersion -cne $identity.ResourceVersion) {
        throw 'DSTicket 操作锁 create/回读身份漂移，拒绝进入集群写阶段。'
    }
    return [pscustomobject]@{
        HolderId = $holderId
        Operation = $operation
        Uid = $identity.Uid
        ResourceVersion = $identity.ResourceVersion
    }
}

function Assert-OnlineDSTicketOperationLockHeld {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)]$Identity
    )
    $live = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'configmap/pandora-dsticket-operation-lock', '-n', $K8sNamespace, '-o', 'json') `
        -Action '核对 DSTicket 操作互斥锁持有权'
    $contract = Assert-PandoraDSTicketOperationLockContract -LockObject $live `
        -HolderId ([string]$Identity.HolderId) -Operation ([string]$Identity.Operation) -RequireLiveIdentity
    if ($contract.Uid -cne [string]$Identity.Uid -or
        $contract.ResourceVersion -cne [string]$Identity.ResourceVersion) {
        throw 'DSTicket 操作锁 UID/resourceVersion 已漂移，拒绝继续任何集群写入。'
    }
    return $contract
}

function Exit-OnlineDSTicketOperationLock {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)]$Identity
    )
    $held = Assert-OnlineDSTicketOperationLockHeld -KubeContext $KubeContext -Identity $Identity
    # kubectl v1.34 没有 delete --preconditions flag；raw DELETE 的 body 使用 Kubernetes
    # DeleteOptions UID+resourceVersion 双前置条件，防旧进程在 ABA 场景误删后继 holder。
    $deleteOptions = [ordered]@{
        apiVersion = 'v1'
        kind = 'DeleteOptions'
        preconditions = [ordered]@{ uid = $held.Uid; resourceVersion = $held.ResourceVersion }
    } | ConvertTo-Json -Depth 10 -Compress
    $deletePath = '/api/v1/namespaces/pandora/configmaps/pandora-dsticket-operation-lock'
    $deleted = @($deleteOptions | & kubectl --context $KubeContext delete "--raw=$deletePath" -f - 2>$null)
    if ($LASTEXITCODE -ne 0) {
        throw '带 UID/resourceVersion 前置条件释放 DSTicket 操作锁失败；锁按 fail-closed 语义保留，须人工审计。'
    }
    $after = Get-OnlineOptionalKubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'configmap/pandora-dsticket-operation-lock', '-n', $K8sNamespace, '--ignore-not-found', '-o', 'json') `
        -Action '复查 DSTicket 操作锁释放结果'
    if ($null -eq $after) { return }
    $afterUid = [string]$after.metadata.uid
    if ($afterUid -ceq [string]$Identity.Uid) {
        throw 'DSTicket 操作锁 DELETE 返回后同 UID 对象仍存在；拒绝把释放误报成功。'
    }
    $afterHolder = [string]$after.metadata.annotations.'pandora.dev/dsticket-lock-holder-id'
    $afterOperation = [string]$after.metadata.annotations.'pandora.dev/dsticket-lock-operation'
    $null = Assert-PandoraDSTicketOperationLockContract -LockObject $after `
        -HolderId $afterHolder -Operation $afterOperation -RequireLiveIdentity
}

function Get-PandoraFleetContractRows {
    param([Parameter(Mandatory = $true)][string]$Manifest)
    $jsonPath = '{.kind}{"\t"}{.metadata.name}{"\t"}{.spec.template.spec.template.spec.containers[*].name}{"\t"}{.spec.template.spec.template.spec.containers[*].image}{"\t"}{.spec.template.spec.template.spec.containers[*].imagePullPolicy}{"\t"}{.spec.template.metadata.annotations.pandora\.dev/image-digest}{"\t"}{.spec.template.spec.template.metadata.annotations.pandora\.dev/image-digest}{"\t"}{.metadata.labels.pandora\.dev/release-track}{"\t"}{.spec.template.metadata.labels.pandora\.dev/release-track}{"\t"}{.spec.template.spec.template.metadata.labels.pandora\.dev/release-track}{"\n"}'
    return @(Invoke-KubectlClientContract -Manifest $Manifest -JsonPath $jsonPath -Action 'Fleet manifest')
}

function New-OnlineRuntimeOverlay {
    param(
        [Parameter(Mandatory = $true)][string]$SourceOverlay,
        [Parameter(Mandatory = $true)][string]$Registry,
        [Parameter(Mandatory = $true)][hashtable]$Digests,
        [Parameter(Mandatory = $true)][string[]]$ServiceNames,
        [Parameter(Mandatory = $true)][string[]]$WriterServices,
        [Parameter(Mandatory = $true)][string]$EnvironmentName,
        [Parameter(Mandatory = $true)][ValidateRange(1, 2147483647)][int]$DSTicketKeysetRevision,
        [string]$EtcdIdentityRevision = '',
        [string]$EtcdServerName = '',
        [string]$EtcdForbiddenReadPrefix = '',
        [hashtable]$EtcdPasswordAuthByService = @{},
        [hashtable]$GreenDesiredReplicas = @{},
        [hashtable]$GreenDeploymentObjects = @{},
        [switch]$CanonicalDsAuthGreen
    )
    $parent = Split-Path -Parent $SourceOverlay
    $runtime = Join-Path $parent ('.online-runtime-' + $EnvironmentName + '-' + [guid]::NewGuid().ToString('N'))
    try {
        Copy-Item -LiteralPath $SourceOverlay -Destination $runtime -Recurse
        $kustomizationPath = Join-Path $runtime 'kustomization.yaml'
        $text = Get-Content -LiteralPath $kustomizationPath -Raw
        $text = ConvertTo-PandoraDigestKustomization -Template $text -Registry $Registry -Digests $Digests -ServiceNames $ServiceNames
        $dsticketPatchName = 'dsticket-keyset-login.yaml'
        $dsticketSignerServices = @('login', 'matchmaker', 'matchmaker-pve', 'hub-allocator')
        $dsticketSignerPatchNames = @($dsticketSignerServices | ForEach-Object { "dsticket-signer-$_.yaml" })
        $etcdIdentityPatchNames = if ([string]::IsNullOrWhiteSpace($EtcdIdentityRevision)) { @() } else {
            @($WriterServices | ForEach-Object { "ds-auth-etcd-identity-$_.yaml" })
        }
        $terminalPatchNames = if ($CanonicalDsAuthGreen) {
            @($WriterServices | ForEach-Object { "ds-auth-dormant-$_.yaml" }) +
            @($WriterServices | ForEach-Object { "ds-auth-service-green-$_.yaml" })
        } else { @() }
        $text = Add-PandoraWriterPatchEntries -Kustomization $text -WriterServices $WriterServices `
            -AdditionalPatchPaths (@($dsticketPatchName) + $dsticketSignerPatchNames + $etcdIdentityPatchNames +
                $terminalPatchNames)
        [System.IO.File]::WriteAllText($kustomizationPath, $text, [System.Text.UTF8Encoding]::new($false))
        foreach ($writer in $WriterServices) {
            $patchText = New-PandoraWriterDigestPatch -Service $writer -Digest ([string]$Digests[$writer])
            [System.IO.File]::WriteAllText((Join-Path $runtime "writer-digest-$writer.yaml"), $patchText, [System.Text.UTF8Encoding]::new($false))
        }
        $dsticketPatch = New-PandoraLoginDSTicketJWKSRevisionPatch -Revision $DSTicketKeysetRevision
        [System.IO.File]::WriteAllText((Join-Path $runtime $dsticketPatchName), $dsticketPatch, [System.Text.UTF8Encoding]::new($false))
        foreach ($signer in $dsticketSignerServices) {
            $signerPatch = New-PandoraDSTicketSignerSecretRevisionPatch -Service $signer -Revision $DSTicketKeysetRevision
            [System.IO.File]::WriteAllText((Join-Path $runtime "dsticket-signer-$signer.yaml"), $signerPatch, [System.Text.UTF8Encoding]::new($false))
        }
        if (-not [string]::IsNullOrWhiteSpace($EtcdIdentityRevision)) {
            foreach ($writer in $WriterServices) {
                if (-not $EtcdPasswordAuthByService.ContainsKey($writer)) { throw "缺 writer/$writer etcd password-auth contract。" }
                $identityPatch = New-PandoraDsAuthEtcdIdentityPatch -App $writer -Revision $EtcdIdentityRevision `
                    -ServerName $EtcdServerName -ForbiddenReadPrefix $EtcdForbiddenReadPrefix `
                    -UsesPasswordAuth ([bool]$EtcdPasswordAuthByService[$writer])
                [System.IO.File]::WriteAllText((Join-Path $runtime "ds-auth-etcd-identity-$writer.yaml"), $identityPatch, [System.Text.UTF8Encoding]::new($false))
            }
        }
        $greenPatchPaths = @{}
        if ($CanonicalDsAuthGreen) {
            if ([string]::IsNullOrWhiteSpace($EtcdIdentityRevision)) { throw 'canonical green 必须配置 etcd identity revision。' }
            foreach ($writer in $WriterServices) {
                if (-not $GreenDesiredReplicas.ContainsKey($writer)) { throw "缺 canonical green/$writer desired replicas。" }
                if (-not $GreenDeploymentObjects.ContainsKey($writer)) { throw "缺 canonical green/$writer live Deployment object。" }
                $dormantPatch = New-PandoraDsAuthDormantBluePatch -App $writer
                [System.IO.File]::WriteAllText((Join-Path $runtime "ds-auth-dormant-$writer.yaml"), $dormantPatch, [System.Text.UTF8Encoding]::new($false))
                $servicePatch = New-PandoraDsAuthGreenServicePatch -App $writer
                [System.IO.File]::WriteAllText((Join-Path $runtime "ds-auth-service-green-$writer.yaml"), $servicePatch, [System.Text.UTF8Encoding]::new($false))
                $pin = New-PandoraPinnedImageReference -Reference ($Registry.TrimEnd('/') + "/pandora/$writer") -Digest ([string]$Digests[$writer])
                $greenObject = New-PandoraDsAuthCanonicalGreenObject -LiveDeployment $GreenDeploymentObjects[$writer] `
                    -App $writer -Revision $EtcdIdentityRevision `
                    -ServerName $EtcdServerName -ForbiddenReadPrefix $EtcdForbiddenReadPrefix `
                    -UsesPasswordAuth ([bool]$EtcdPasswordAuthByService[$writer]) `
                    -DesiredReplicas ([int]$GreenDesiredReplicas[$writer]) -PinnedImage $pin -Digest ([string]$Digests[$writer])
                $greenPatchPath = Join-Path $runtime "ds-auth-green-release-$writer.json"
                $greenJSON = $greenObject | ConvertTo-Json -Depth 40
                [System.IO.File]::WriteAllText($greenPatchPath, $greenJSON, [System.Text.UTF8Encoding]::new($false))
                $greenPatchPaths[$writer] = $greenPatchPath
            }
        }
        $renderedLines = @(& kubectl kustomize $runtime 2>&1)
        if ($LASTEXITCODE -ne 0) {
            throw "最终 online overlay 渲染失败:$($renderedLines -join [Environment]::NewLine)"
        }
        $rendered = ($renderedLines | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
        $pins = @{}
        foreach ($name in $ServiceNames) {
            $pins[$name] = New-PandoraPinnedImageReference -Reference ($Registry.TrimEnd('/') + "/pandora/$name") -Digest ([string]$Digests[$name])
        }
        $contractRows = Get-PandoraWorkloadContractRows -Manifest $rendered
        Assert-PandoraRenderedOnlineContract -ContractRows $contractRows -Pins $pins -Digests $Digests -ServiceNames $ServiceNames -WriterServices $WriterServices
        $hubSingleWriterRows = Get-PandoraHubAllocatorSingleWriterContractRows -Manifest $rendered
        Assert-PandoraHubAllocatorSingleWriterContract -ContractRows $hubSingleWriterRows
        $dsticketRows = Get-PandoraDSTicketSignerContractRows -Manifest $rendered
        Assert-PandoraDSTicketSignerRevisionContract -ContractRows $dsticketRows -Revision $DSTicketKeysetRevision
        return [pscustomobject]@{ Path = $runtime; Rendered = $rendered; GreenPatchPaths = $greenPatchPaths }
    } catch {
        if (Test-Path -LiteralPath $runtime -PathType Container) {
            $resolved = [System.IO.Path]::GetFullPath($runtime)
            if ((Split-Path -Parent $resolved).Equals([System.IO.Path]::GetFullPath($parent), [System.StringComparison]::OrdinalIgnoreCase) -and
                (Split-Path -Leaf $resolved) -match ('^\.online-runtime-' + [regex]::Escape($EnvironmentName) + '-[0-9a-f]{32}$')) {
                Remove-Item -LiteralPath $resolved -Recurse -Force -ErrorAction SilentlyContinue
            }
        }
        throw
    }
}

function Get-OnlineDsAuthEndpointUIDs([string]$KubeContext, [string]$ServiceName, $Service, $ExpectedPods) {
    $slices = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'endpointslices', '-n', $K8sNamespace, '-l', "kubernetes.io/service-name=$ServiceName", '-o', 'json') `
        -Action "回读 Service/$ServiceName EndpointSlice"
    return Get-PandoraDsAuthVerifiedEndpointUIDSet $slices $Service $ExpectedPods $K8sNamespace
}

function Get-OnlineDsAuthCanonicalState {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string[]]$WriterServices,
        [Parameter(Mandatory = $true)][string]$Revision,
        [Parameter(Mandatory = $true)][string]$ServerName,
        [Parameter(Mandatory = $true)][string]$ForbiddenReadPrefix,
        [hashtable]$ExpectedDigests = @{}
    )
    $capabilityNames = @{
        'login' = 'login'; 'player-locator' = 'player_locator'; 'ds-allocator' = 'ds_allocator'
        'hub-allocator' = 'hub_allocator'; 'battle-result' = 'battle_result'
    }
    $counts = [ordered]@{}
    $instances = [ordered]@{}
    $allowed = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
    $serviceDigests = [ordered]@{}
    $passwordAuth = @{}
    $desiredReplicas = @{}
    $greenDeploymentObjects = @{}
    foreach ($writer in $WriterServices) {
        $secretName = Get-PandoraDsAuthIdentitySecretName $writer $Revision
        $secret = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', "secret/$secretName", '-n', $K8sNamespace, '-o', 'json') `
            -Action "终态回读 DS auth etcd identity Secret/$secretName"
        $contract = Assert-PandoraDsAuthIdentitySecretContract $secret $writer $Revision
        $passwordAuth[$writer] = [bool]$contract.UsesPasswordAuth

        $blue = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', "deployment/$writer", '-n', $K8sNamespace, '-o', 'json') `
            -Action "回读 dormant blue Deployment/$writer"
        if ([int]$blue.spec.replicas -ne 0 -or [string]$blue.spec.selector.matchLabels.app -cne $writer -or
            [string]$blue.spec.selector.matchLabels.'pandora.dev/ds-auth-writer-set' -cne 'blue' -or
            [string]$blue.spec.selector.matchLabels.'pandora.dev/ds-auth-writer-epoch' -cne '1' -or
            @($blue.spec.selector.matchLabels.PSObject.Properties).Count -ne 3) {
            throw "Deployment/$writer 不是 exact dormant blue epoch=1。"
        }
        $bluePods = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', 'pods', '-n', $K8sNamespace, '-l', "app=$writer,pandora.dev/ds-auth-writer-set=blue", '-o', 'json') `
            -Action "回读 dormant blue writer/$writer Pod"
        if (@($bluePods.items).Count -ne 0) { throw "writer/$writer blue Pod 必须为 0。" }

        $greenName = "$writer-ds-auth-green"
        $deployment = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', "deployment/$greenName", '-n', $K8sNamespace, '-o', 'json') `
            -Action "回读 canonical green Deployment/$greenName"
        $desiredRaw = [string]$deployment.metadata.annotations.'pandora.dev/ds-auth-green-desired-replicas'
        if ($desiredRaw -cnotmatch '^[1-9][0-9]?$') { throw "Deployment/$greenName desired replicas annotation 非 canonical。" }
        $want = [int]$desiredRaw
        $desiredReplicas[$writer] = $want
        $greenDeploymentObjects[$writer] = $deployment
        if ([int]$deployment.spec.replicas -ne $want -or [string]$deployment.spec.selector.matchLabels.app -cne $writer -or
            [string]$deployment.spec.selector.matchLabels.'pandora.dev/ds-auth-writer-set' -cne 'green' -or
            [string]$deployment.spec.selector.matchLabels.'pandora.dev/ds-auth-writer-epoch' -cne '2' -or
            @($deployment.spec.selector.matchLabels.PSObject.Properties).Count -ne 3) {
            throw "Deployment/$greenName immutable selector/replicas 不是 canonical green。"
        }
        if ($writer -ceq 'hub-allocator') {
            Assert-PandoraHubAllocatorSingleWriterDeploymentContract $deployment
        }
        $template = [pscustomobject]@{ metadata = $deployment.spec.template.metadata; spec = $deployment.spec.template.spec }
        Assert-PandoraDsAuthIdentityPodContract $template $writer $Revision $ServerName $ForbiddenReadPrefix ([bool]$contract.UsesPasswordAuth)
        if ($want -lt 1 -or [int]$deployment.status.readyReplicas -ne $want -or [int]$deployment.status.updatedReplicas -ne $want) {
            throw "writer/$greenName rollout 未稳定。"
        }
        $templateDigest = [string]$deployment.spec.template.metadata.annotations.'pandora.dev/image-digest'
        if ($templateDigest -cnotmatch '^sha256:[0-9a-f]{64}$') { throw "writer/$greenName template digest 非 immutable。" }
        if ($ExpectedDigests.Count -gt 0 -and [string]$ExpectedDigests[$writer] -cne $templateDigest) { throw "writer/$greenName 未命中本次 release digest。" }
        $podList = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', 'pods', '-n', $K8sNamespace, '-l', "app=$writer,pandora.dev/ds-auth-writer-set=green", '-o', 'json') `
            -Action "回读 canonical green writer/$writer Pod"
        $pods = @($podList.items | Where-Object { -not (Test-PandoraKubernetesObjectDeleting $_) })
        if ($pods.Count -ne $want) { throw "writer/$writer live Pod 数不等于 desired。" }
        $uids = [System.Collections.Generic.List[string]]::new()
        foreach ($pod in $pods) {
            Assert-PandoraDsAuthIdentityPodContract $pod $writer $Revision $ServerName $ForbiddenReadPrefix ([bool]$contract.UsesPasswordAuth)
            if ([string]$pod.metadata.labels.'pandora.dev/ds-auth-writer-epoch' -cne '2') { throw "writer/$writer Pod epoch 不是 2。" }
            if ($writer -in @('battle-result', 'ds-allocator')) { Assert-PandoraDsTerminalMeshPodContract $pod $writer }
            $status = @($pod.status.containerStatuses | Where-Object name -eq $writer)
            if ($status.Count -ne 1 -or -not ([string]$status[0].imageID).EndsWith($templateDigest, [StringComparison]::Ordinal)) {
                throw "writer/$writer Pod imageID 未命中 canonical green digest。"
            }
            $uids.Add([string]$pod.metadata.uid)
        }
        $serviceObject = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', "service/$writer", '-n', $K8sNamespace, '-o', 'json') `
            -Action "回读 canonical green Service/$writer"
        if ([string]$serviceObject.spec.selector.app -cne $writer -or
            [string]$serviceObject.spec.selector.'pandora.dev/ds-auth-writer-set' -cne 'green' -or
            [string]$serviceObject.spec.selector.'pandora.dev/ds-auth-writer-epoch' -cne '2' -or
            @($serviceObject.spec.selector.PSObject.Properties).Count -ne 3) { throw "Service/$writer selector 不是 exact canonical green。" }
        $endpointUIDs = Get-OnlineDsAuthEndpointUIDs -KubeContext $KubeContext -ServiceName $writer `
            -Service $serviceObject -ExpectedPods $pods
        if ($endpointUIDs.Count -ne $uids.Count) { throw "Service/$writer Endpoint UID 数不等于 green Pod。" }
        foreach ($uid in $uids) { if (-not $endpointUIDs.Contains($uid)) { throw "Service/$writer Endpoint UID 集不等于 green Pod。" } }
        $capabilityName = [string]$capabilityNames[$writer]
        $counts[$capabilityName] = $want
        $instances[$capabilityName] = (@($uids | Sort-Object) -join '|')
        $null = $allowed.Add($templateDigest)
        $serviceDigests[$capabilityName] = $templateDigest
    }
    $peer = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'peerauthentication.security.istio.io/pandora-ds-allocator-terminal-permissive', '-n', $K8sNamespace, '-o', 'json') `
        -Action '终态回读 ReleaseBattle PeerAuthentication'
    $policy = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'authorizationpolicy.security.istio.io/pandora-ds-terminal-release-exact-deny', '-n', $K8sNamespace, '-o', 'json') `
        -Action '终态回读 ReleaseBattle AuthorizationPolicy'
    $service = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'service/ds-allocator', '-n', $K8sNamespace, '-o', 'json') `
        -Action '终态回读 ds-allocator gRPC Service'
    Assert-PandoraDsTerminalMeshPolicyContract $peer $policy $service

    return [pscustomobject]@{
        ExpectedServices = (($counts.Keys | ForEach-Object { "$_=$($counts[$_])" }) -join ',')
        ExpectedInstances = (($instances.Keys | ForEach-Object { "$_=$($instances[$_])" }) -join ',')
        AllowedDigests = (@($allowed | Sort-Object) -join ',')
        ExpectedDigests = (($serviceDigests.Keys | ForEach-Object { "$_=$($serviceDigests[$_])" }) -join ',')
        PasswordAuth = $passwordAuth
        DesiredReplicas = $desiredReplicas
        GreenDeploymentObjects = $greenDeploymentObjects
    }
}

function Invoke-OnlineDsAuthCapabilityAudit {
    param(
        [Parameter(Mandatory = $true)]$State,
        [Parameter(Mandatory = $true)][string]$EtcdEndpoints,
        [Parameter(Mandatory = $true)][string]$KeysetRevision,
        [Parameter(Mandatory = $true)][string]$Revision,
        [Parameter(Mandatory = $true)][string[]]$SecureGoArgs,
        [Parameter(Mandatory = $true)]$ActivationEvidence
    )
    $deadline = [datetime]::UtcNow.AddSeconds(45)
    if ([string]$ActivationEvidence.PolicyV3RequiredFeatures -cne $script:PandoraDsAuthRequiredFeaturesV3) {
        throw 'ordinary release V3 evidence feature policy is not canonical.'
    }
    Push-Location $ProjectRoot
    try {
        do {
            & go run ./pkg/dsauthfence/cmd/dsauth-activate --endpoints $EtcdEndpoints `
                --expected-services $State.ExpectedServices --expected-instances $State.ExpectedInstances `
                --expected-epoch 2 --target-epoch 2 --keyset-revision $KeysetRevision `
                --etcd-identity-revision $Revision --allowed-image-digests $State.AllowedDigests `
                --expected-image-digests $State.ExpectedDigests `
                --required-features $script:PandoraDsAuthRequiredFeaturesV3 --policy-v3 `
                --activation-evidence-sha256 $ActivationEvidence.PolicyV3EvidenceSHA256 `
                --activation-evidence-completed-at-ms $ActivationEvidence.PolicyV3CompletedAtUnixMS @SecureGoArgs
            if ($LASTEXITCODE -eq 0) { return }
            if ([datetime]::UtcNow -ge $deadline) { throw 'online writer exact capability/features 终态审计失败。' }
            Start-Sleep -Seconds 2
        } while ($true)
    } finally { Pop-Location }
}

function Assert-OnlineDsAuthRuntimeAndCapabilities {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string[]]$WriterServices,
        [Parameter(Mandatory = $true)][string]$Revision,
        [Parameter(Mandatory = $true)][string]$ServerName,
        [Parameter(Mandatory = $true)][string]$ForbiddenReadPrefix,
        [Parameter(Mandatory = $true)][string]$EtcdEndpoints,
        [Parameter(Mandatory = $true)][string]$KeysetRevision,
        [Parameter(Mandatory = $true)][string[]]$SecureGoArgs,
        [hashtable]$ExpectedDigests = @{},
        [Parameter(Mandatory = $true)]$ActivationEvidence
    )
    $state = Get-OnlineDsAuthCanonicalState -KubeContext $KubeContext -WriterServices $WriterServices `
        -Revision $Revision -ServerName $ServerName -ForbiddenReadPrefix $ForbiddenReadPrefix -ExpectedDigests $ExpectedDigests
    Invoke-OnlineDsAuthCapabilityAudit -State $state -EtcdEndpoints $EtcdEndpoints -KeysetRevision $KeysetRevision `
        -Revision $Revision -SecureGoArgs $SecureGoArgs -ActivationEvidence $ActivationEvidence
    return $state
}

function Get-OnlineDsAuthActivationEvidenceState {
    param([Parameter(Mandatory = $true)][string]$KubeContext)
    $lock = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'configmap/pandora-ds-auth-activation-v2', '-n', $K8sNamespace, '-o', 'json') `
        -Action '读取 DS auth activation lock'
    $runID = [string]$lock.data.run_id
    if ($lock.immutable -ne $true -or $runID -cnotmatch '^[a-z0-9][a-z0-9-]{7,23}$' -or
        (Test-PandoraKubernetesObjectDeleting $lock)) {
        throw 'DS auth activation lock 缺 immutable canonical RunId 或正在删除；ordinary release 禁止继续。'
    }
    $evidenceName = "pandora-pod-uid-evidence-v2-$runID"
    $switchName = "pandora-ds-auth-switch-v2-$runID"
    $cleanupRequiredName = "pandora-pod-uid-acl-cleanup-required-v2-$runID"
    $cleanupCompleteName = "pandora-pod-uid-acl-cleanup-complete-v2-$runID"
    $evidence = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', "configmap/$evidenceName", '-n', $K8sNamespace, '-o', 'json') `
        -Action '读取完成的 pod_uid activation evidence'
    $switch = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', "configmap/$switchName", '-n', $K8sNamespace, '-o', 'json') `
        -Action '读取完成的 DS auth switch marker'
    $cleanupRequired = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', "configmap/$cleanupRequiredName", '-n', $K8sNamespace, '-o', 'json') `
        -Action '读取 CAS 后临时 Redis ACL cleanup required marker'
    $cleanupComplete = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', "configmap/$cleanupCompleteName", '-n', $K8sNamespace, '-o', 'json') `
        -Action '读取 CAS 后临时 Redis ACL cleanup complete marker'
    $policyV3Marker = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', "configmap/$script:PandoraDsAuthPolicyV3EvidenceName", '-n', $K8sNamespace, '-o', 'json') `
        -Action '读取 immutable DS auth V3 successor-policy activation evidence'
    $policyV3Evidence = Assert-PandoraDsAuthPolicyV3EvidenceContract $policyV3Marker `
        ([string]$policyV3Marker.data.expected_services) ([string]$policyV3Marker.data.expected_instances) `
        ([string]$policyV3Marker.data.expected_image_digests) $KubeContext $K8sNamespace
    $policyV3Counts = @{}
    foreach ($item in ([string]$policyV3Evidence.ExpectedServices).Split(',')) {
        $parts = $item.Split('=', 2)
        if ($parts.Count -ne 2 -or $parts[0] -cnotin @('login', 'player_locator', 'ds_allocator', 'hub_allocator', 'battle_result') -or
            $parts[1] -cnotmatch '^[1-9][0-9]?$' -or $policyV3Counts.ContainsKey($parts[0])) {
            throw 'DS auth V3 evidence expected_services is not the exact unique five-service count map.'
        }
        $policyV3Counts[$parts[0]] = [int]$parts[1]
    }
    if ($policyV3Counts.Count -ne 5 -or [int]$policyV3Counts['hub_allocator'] -ne 1) {
        throw 'DS auth V3 evidence expected_services is not exact five services with a single Hub.'
    }
    $policyV3CompletionName = "pandora-ds-auth-policy-v3-complete-$($policyV3Evidence.RunID)"
    $policyV3CompletionMarker = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', "configmap/$policyV3CompletionName", '-n', $K8sNamespace, '-o', 'json') `
        -Action '读取 immutable DS auth V3 successor-policy completion marker'
    $policyV3Completion = Assert-PandoraDsAuthV3CompletionContract $policyV3CompletionMarker `
        $policyV3CompletionName $K8sNamespace $policyV3Evidence $policyV3Counts
    if ([int64]$policyV3Completion.CompletedAtUnixMS -lt [int64]$policyV3Evidence.CompletedAtUnixMS) {
        throw 'DS auth V3 completion predates its immutable staging evidence.'
    }
    $digest = [string]$evidence.data.evidence_sha256
    $completionMS = [string]$evidence.data.final_completion_time_unix_ms
    if ($evidence.immutable -ne $true -or $switch.immutable -ne $true -or
        [string]$evidence.data.contract -cne 'pod-uid-activation-evidence-v1' -or
        [string]$evidence.data.run_id -cne $runID -or [string]$evidence.data.target_epoch -cne '2' -or
        [string]$evidence.data.target_required_value -cne '2@ds-auth-v2-pod-uid-write-invariant-v1' -or
        [string]$evidence.data.required_policy_id -cne 'ds-auth-v2-pod-uid-write-invariant-v1' -or
        [string]$switch.data.run_id -cne $runID -or $digest -cnotmatch '^sha256:[0-9a-f]{64}$' -or
        $completionMS -cnotmatch '^[1-9][0-9]{12}$' -or
        (Test-PandoraKubernetesObjectDeleting $evidence) -or
        (Test-PandoraKubernetesObjectDeleting $switch)) {
        throw 'DS auth activation 尚未形成 immutable evidence+switch 终态；ordinary release 禁止替换配置。'
    }
    $finalCompleted = [datetimeoffset]::FromUnixTimeMilliseconds([int64]$completionMS)
    $evidenceCreated = [datetimeoffset]::Parse([string]$evidence.metadata.creationTimestamp)
    $switchCreated = [datetimeoffset]::Parse([string]$switch.metadata.creationTimestamp)
    if ($finalCompleted -gt $evidenceCreated -or $evidenceCreated -gt $switchCreated) {
        throw 'DS auth activation 时间链 final<=evidence<=switch 非法；ordinary release 禁止继续。'
    }
    foreach ($field in @('pod_uid_source_secret_uid', 'pod_uid_source_secret_resource_version',
        'pod_uid_raw_config_sha256', 'pod_uid_raw_snapshot_name', 'pod_uid_raw_snapshot_uid',
        'pod_uid_raw_snapshot_resource_version', 'pod_uid_ro_secret_name', 'pod_uid_ro_secret_uid',
        'pod_uid_ro_secret_resource_version', 'pod_uid_ro_config_sha256',
        'pod_uid_redis_config_identity', 'pod_uid_redis_config_topology',
        'pod_uid_config_helper_source_sha256')) {
        if ([string]::IsNullOrWhiteSpace([string]$lock.data.$field)) {
            throw "DS auth activation lock 缺 config binding field=$field。"
        }
    }
    foreach ($binding in ([ordered]@{
        expected_required_value = '1'
        target_required_value = '2@ds-auth-v2-pod-uid-write-invariant-v1'
        required_policy_id = 'ds-auth-v2-pod-uid-write-invariant-v1'
    }).GetEnumerator()) {
        if ([string]$lock.data.$($binding.Key) -cne [string]$binding.Value -or
            [string]$switch.data.$($binding.Key) -cne [string]$binding.Value) {
            throw "DS auth activation marker 缺 versioned rollback policy binding=$($binding.Key)。"
        }
    }
    $cleanupRequiredKeys = @('contract', 'run_id', 'target_epoch', 'evidence_sha256', 'method',
        'target_user', 'redis_config_identity', 'redis_topology', 'helper_source_sha256',
        'ro_secret_uid', 'ro_secret_resource_version', 'target_required_value', 'required_policy_id')
    $cleanupCompleteKeys = @('contract', 'run_id', 'target_epoch', 'evidence_sha256',
        'required_marker_uid', 'required_marker_resource_version', 'method', 'target_user',
        'redis_config_identity', 'redis_topology', 'helper_source_sha256', 'proof_sha256',
        'visited_nodes', 'completed_at_unix_ms', 'target_required_value', 'required_policy_id')
    $cleanupCompletedMS = [int64]0
    if ($cleanupRequired.immutable -ne $true -or $cleanupComplete.immutable -ne $true -or
        (Test-PandoraKubernetesObjectDeleting $cleanupRequired) -or
        (Test-PandoraKubernetesObjectDeleting $cleanupComplete) -or
        [string]$cleanupRequired.metadata.uid -cnotmatch '^[0-9a-f][0-9a-f-]{7,127}$' -or
        [string]$cleanupComplete.metadata.uid -cnotmatch '^[0-9a-f][0-9a-f-]{7,127}$' -or
        [string]$cleanupRequired.metadata.resourceVersion -cnotmatch '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$' -or
        [string]$cleanupComplete.metadata.resourceVersion -cnotmatch '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$' -or
        [string]$cleanupRequired.data.contract -cne 'pod-uid-acl-cleanup-required-v1' -or
        [string]$cleanupComplete.data.contract -cne 'pod-uid-acl-cleanup-complete-v1' -or
        [string]$cleanupRequired.data.run_id -cne $runID -or
        [string]$cleanupComplete.data.run_id -cne $runID -or
        [string]$cleanupRequired.data.target_epoch -cne '2' -or
        [string]$cleanupComplete.data.target_epoch -cne '2' -or
        [string]$cleanupRequired.data.target_required_value -cne '2@ds-auth-v2-pod-uid-write-invariant-v1' -or
        [string]$cleanupComplete.data.target_required_value -cne '2@ds-auth-v2-pod-uid-write-invariant-v1' -or
        [string]$cleanupRequired.data.required_policy_id -cne 'ds-auth-v2-pod-uid-write-invariant-v1' -or
        [string]$cleanupComplete.data.required_policy_id -cne 'ds-auth-v2-pod-uid-write-invariant-v1' -or
        [string]$cleanupRequired.data.evidence_sha256 -cne $digest -or
        [string]$cleanupComplete.data.evidence_sha256 -cne $digest -or
        [string]$cleanupRequired.data.method -cne 'DELUSER' -or
        [string]$cleanupComplete.data.method -cne 'DELUSER' -or
        [string]$cleanupRequired.data.target_user -cne 'pandora-pod-uid-release-preflight-ro' -or
        [string]$cleanupComplete.data.target_user -cne 'pandora-pod-uid-release-preflight-ro' -or
        [string]$cleanupRequired.data.redis_config_identity -cne [string]$lock.data.pod_uid_redis_config_identity -or
        [string]$cleanupComplete.data.redis_config_identity -cne [string]$lock.data.pod_uid_redis_config_identity -or
        [string]$cleanupRequired.data.redis_topology -cne [string]$lock.data.pod_uid_redis_config_topology -or
        [string]$cleanupComplete.data.redis_topology -cne [string]$lock.data.pod_uid_redis_config_topology -or
        [string]$cleanupRequired.data.helper_source_sha256 -cne [string]$lock.data.pod_uid_config_helper_source_sha256 -or
        [string]$cleanupComplete.data.helper_source_sha256 -cne [string]$lock.data.pod_uid_config_helper_source_sha256 -or
        [string]$cleanupRequired.data.ro_secret_uid -cne [string]$lock.data.pod_uid_ro_secret_uid -or
        [string]$cleanupRequired.data.ro_secret_resource_version -cne [string]$lock.data.pod_uid_ro_secret_resource_version -or
        [string]$cleanupComplete.data.required_marker_uid -cne [string]$cleanupRequired.metadata.uid -or
        [string]$cleanupComplete.data.required_marker_resource_version -cne [string]$cleanupRequired.metadata.resourceVersion -or
        [string]$cleanupComplete.data.proof_sha256 -cnotmatch '^sha256:[0-9a-f]{64}$' -or
        [string]$cleanupComplete.data.visited_nodes -cnotmatch '^[1-9][0-9]*$' -or
        [string]$cleanupComplete.data.completed_at_unix_ms -cnotmatch '^[1-9][0-9]{12}$' -or
        -not [int64]::TryParse([string]$cleanupComplete.data.completed_at_unix_ms, [ref]$cleanupCompletedMS) -or
        (@($cleanupRequired.data.PSObject.Properties.Name | Sort-Object) -join ',') -cne
            (@($cleanupRequiredKeys | Sort-Object) -join ',') -or
        (@($cleanupComplete.data.PSObject.Properties.Name | Sort-Object) -join ',') -cne
            (@($cleanupCompleteKeys | Sort-Object) -join ',')) {
        throw 'DS auth activation CAS 后临时 Redis ACL cleanup 证据缺失/漂移/PENDING；ordinary release 禁止继续。'
    }
    $cleanupRequiredCreated = [datetimeoffset]::Parse([string]$cleanupRequired.metadata.creationTimestamp)
    $cleanupCompleteCreated = [datetimeoffset]::Parse([string]$cleanupComplete.metadata.creationTimestamp)
    $cleanupProofCompleted = [datetimeoffset]::FromUnixTimeMilliseconds($cleanupCompletedMS)
    if ($evidenceCreated -gt $cleanupRequiredCreated) {
        throw 'DS auth activation cleanup 时间链 evidence<=required<=switch<=proof<=complete 非法。'
    }
    Assert-PandoraPodUIDACLCleanupTimeline $cleanupRequiredCreated $switchCreated `
        $cleanupProofCompleted $cleanupCompleteCreated
    return [pscustomobject][ordered]@{
        RunID = $runID
        LockUID = [string]$lock.metadata.uid
        LockResourceVersion = [string]$lock.metadata.resourceVersion
        EvidenceUID = [string]$evidence.metadata.uid
        EvidenceResourceVersion = [string]$evidence.metadata.resourceVersion
        EvidenceSHA256 = $digest
        FinalCompletionTimeUnixMS = [int64]$completionMS
        SwitchUID = [string]$switch.metadata.uid
        SwitchResourceVersion = [string]$switch.metadata.resourceVersion
        CleanupRequiredUID = [string]$cleanupRequired.metadata.uid
        CleanupRequiredResourceVersion = [string]$cleanupRequired.metadata.resourceVersion
        CleanupCompleteUID = [string]$cleanupComplete.metadata.uid
        CleanupCompleteResourceVersion = [string]$cleanupComplete.metadata.resourceVersion
        CleanupProofSHA256 = [string]$cleanupComplete.data.proof_sha256
        CleanupCompletedAtUnixMS = $cleanupCompletedMS
        PolicyV3EvidenceUID = $policyV3Evidence.UID
        PolicyV3EvidenceResourceVersion = $policyV3Evidence.ResourceVersion
        PolicyV3EvidenceSHA256 = $policyV3Evidence.EvidenceSHA256
        PolicyV3CompletedAtUnixMS = $policyV3Evidence.CompletedAtUnixMS
        PolicyV3ExpectedServices = $policyV3Evidence.ExpectedServices
        PolicyV3ExpectedInstances = $policyV3Evidence.ExpectedInstances
        PolicyV3ExpectedImageDigests = $policyV3Evidence.ExpectedImageDigests
        PolicyV3RequiredFeatures = $policyV3Evidence.RequiredFeatures
        PolicyV3RunID = $policyV3Evidence.RunID
        PolicyV3KubeContext = $policyV3Evidence.KubeContext
        PolicyV3Namespace = $policyV3Evidence.Namespace
        PolicyV3CompletionName = $policyV3Completion.Name
        PolicyV3CompletionUID = $policyV3Completion.UID
        PolicyV3CompletionResourceVersion = $policyV3Completion.ResourceVersion
        PolicyV3CompletionFinalInstances = $policyV3Completion.FinalInstances
        PolicyV3CompletionCompletedAtUnixMS = $policyV3Completion.CompletedAtUnixMS
    }
}

function Assert-OnlineDsAuthActivationEvidenceUnchanged($Expected, $Actual) {
    foreach ($field in @('RunID', 'LockUID', 'LockResourceVersion', 'EvidenceUID',
        'EvidenceResourceVersion', 'EvidenceSHA256', 'FinalCompletionTimeUnixMS',
        'SwitchUID', 'SwitchResourceVersion', 'CleanupRequiredUID',
        'CleanupRequiredResourceVersion', 'CleanupCompleteUID',
        'CleanupCompleteResourceVersion', 'CleanupProofSHA256', 'CleanupCompletedAtUnixMS',
        'PolicyV3EvidenceUID', 'PolicyV3EvidenceResourceVersion', 'PolicyV3EvidenceSHA256',
        'PolicyV3CompletedAtUnixMS', 'PolicyV3ExpectedServices', 'PolicyV3ExpectedInstances',
        'PolicyV3ExpectedImageDigests', 'PolicyV3RequiredFeatures', 'PolicyV3RunID',
        'PolicyV3KubeContext', 'PolicyV3Namespace', 'PolicyV3CompletionName',
        'PolicyV3CompletionUID', 'PolicyV3CompletionResourceVersion',
        'PolicyV3CompletionFinalInstances', 'PolicyV3CompletionCompletedAtUnixMS')) {
        if ([string]$Expected.$field -cne [string]$Actual.$field) {
            throw "ordinary release 窗口内 DS auth activation evidence field=$field 漂移。"
        }
    }
}

function Assert-OnlineDeploymentImageState {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][hashtable]$Pins,
        [Parameter(Mandatory = $true)][hashtable]$Digests,
        [Parameter(Mandatory = $true)][string[]]$WriterServices,
        [switch]$CanonicalGreen
    )
    foreach ($svc in (Get-ServiceList)) {
        $name = [string]$svc.Name
        $deploymentName = if ($CanonicalGreen -and $WriterServices -contains $name) { "$name-ds-auth-green" } else { $name }
        $deployment = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', "deployment/$deploymentName", '-n', $K8sNamespace, '-o', 'json') `
            -Action "回读 Deployment/$deploymentName 镜像"
        $want = [int]$deployment.spec.replicas
        if ([int]$deployment.status.updatedReplicas -ne $want -or [int]$deployment.status.readyReplicas -ne $want -or
            [int]$deployment.status.availableReplicas -ne $want -or [int]$deployment.status.unavailableReplicas -ne 0) {
            throw "Deployment/$deploymentName rollout 未稳定到 $want 个新副本。"
        }
        $podSelector = if ($CanonicalGreen -and $WriterServices -contains $name) { "app=$name,pandora.dev/ds-auth-writer-set=green" } else { "app=$name" }
        $pods = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', 'pods', '-n', $K8sNamespace, '-l', $podSelector, '-o', 'json') `
            -Action "回读 Deployment/$deploymentName Pod"
        $live = @($pods.items | Where-Object { -not (Test-PandoraKubernetesObjectDeleting $_) })
        if ($live.Count -ne $want) { throw "Deployment/$name 非终止 Pod 数=$($live.Count)，expected=$want。" }
        foreach ($pod in $live) {
            $containers = @($pod.spec.containers | Where-Object name -eq $name)
            $statuses = @($pod.status.containerStatuses | Where-Object name -eq $name)
            if ($containers.Count -ne 1 -or [string]$containers[0].image -cne [string]$Pins[$name]) {
                throw "Pod/$($pod.metadata.name) spec.image 未固定到本次 pin $($Pins[$name])。"
            }
            if ($statuses.Count -ne 1 -or -not ([string]$statuses[0].imageID).EndsWith([string]$Digests[$name], [StringComparison]::Ordinal)) {
                throw "Pod/$($pod.metadata.name) imageID 未命中本次 digest $($Digests[$name])。"
            }
            if ($WriterServices -contains $name) {
                $actual = [string]$pod.metadata.annotations.'pandora.dev/image-digest'
                if ($actual -cne [string]$Digests[$name]) {
                    throw "writer Pod/$($pod.metadata.name) digest annotation=$actual，expected=$($Digests[$name])。"
                }
            }
        }
    }
}

function Wait-OnlineReadyFleetImageState {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$Fleet,
        [Parameter(Mandatory = $true)][string]$Container,
        [Parameter(Mandatory = $true)][string]$Pin,
        [Parameter(Mandatory = $true)][string]$Digest,
        [Parameter(Mandatory = $true)][ValidateSet('stable', 'canary')][string]$ExpectedTrack,
        [timespan]$Timeout = [timespan]::FromMinutes(5)
    )
    Assert-PandoraDigest -Digest $Digest -Where "Fleet/$Fleet"
    $deadline = [DateTime]::UtcNow.Add($Timeout)
    $lastReason = '尚未检查'
    do {
        $fleetObject = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', "fleet/$Fleet", '-n', 'default', '-o', 'json') `
            -Action "回读 Fleet/$Fleet 状态"
        $list = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', 'gameservers', '-n', 'default', '-l', "agones.dev/fleet=$Fleet", '-o', 'json') `
            -Action "回读 Fleet/$Fleet GameServer"
        $desired = [int]$fleetObject.spec.replicas
        $reportedTotal = [int]$fleetObject.status.replicas
        $reportedReady = [int]$fleetObject.status.readyReplicas
        $liveServers = @($list.items | Where-Object { -not (Test-PandoraKubernetesObjectDeleting $_) })
        $ready = @($liveServers | Where-Object { [string]$_.status.state -ceq 'Ready' })
        $allocated = @($liveServers | Where-Object { [string]$_.status.state -ceq 'Allocated' })
        $reserved = @($liveServers | Where-Object { [string]$_.status.state -ceq 'Reserved' })
        $stableCount = $ready.Count + $allocated.Count + $reserved.Count
        $wrongTrack = @($liveServers | Where-Object { [string]$_.metadata.labels.'pandora.dev/release-track' -cne $ExpectedTrack })
        if ($desired -lt 0) {
            $lastReason = "Fleet spec.replicas=$desired 非法"
        } elseif ($wrongTrack.Count -gt 0) {
            $lastReason = "GameServer release-track 漂移:$(@($wrongTrack | ForEach-Object { $_.metadata.name }) -join ',')"
        } elseif ($reportedTotal -ne $liveServers.Count -or $stableCount -ne $liveServers.Count) {
            $lastReason = "容量未稳定:desired-ready=$desired status.total=$reportedTotal live=$($liveServers.Count) ready/allocated/reserved=$($ready.Count)/$($allocated.Count)/$($reserved.Count)"
        } elseif ($reportedReady -ne $ready.Count) {
            $lastReason = "Fleet status.readyReplicas=$reportedReady 与实际 Ready=$($ready.Count) 不一致"
        } elseif ($desired -eq 0 -and $ready.Count -eq 0) {
            Write-Ok "Fleet/$Fleet 已缩到 0 Ready（不再接新分配）；旧 Allocated=$($allocated.Count) 可继续受控排空。"
            return
        } elseif ($ready.Count -lt $desired) {
            $lastReason = "Ready 池未补足:desired=$desired actual=$($ready.Count)"
        } elseif ($ready.Count -eq 0) {
            $lastReason = '没有 Ready GameServer（不能证明新分配池已切新镜像）'
        } else {
            $bad = [System.Collections.Generic.List[string]]::new()
            foreach ($gs in $ready) {
                $name = [string]$gs.metadata.name
                $gsDigest = [string]$gs.metadata.annotations.'pandora.dev/image-digest'
                if ($gsDigest -cne $Digest) { $bad.Add("GameServer/$name annotation=$gsDigest") }
                $pod = Get-KubectlJsonObject -KubeContext $KubeContext `
                    -Arguments @('get', "pod/$name", '-n', 'default', '-o', 'json') `
                    -Action "回读 Ready GameServer Pod/$name"
                $podDigest = [string]$pod.metadata.annotations.'pandora.dev/image-digest'
                $podTrack = [string]$pod.metadata.labels.'pandora.dev/release-track'
                $spec = @($pod.spec.containers | Where-Object name -eq $Container)
                $status = @($pod.status.containerStatuses | Where-Object name -eq $Container)
                if ($podDigest -cne $Digest) { $bad.Add("Pod/$name annotation=$podDigest") }
                if ($podTrack -cne $ExpectedTrack) { $bad.Add("Pod/$name release-track=$podTrack") }
                if ($spec.Count -ne 1 -or [string]$spec[0].image -cne $Pin) { $bad.Add("Pod/$name spec.image=$($spec[0].image)") }
                if ($status.Count -ne 1 -or -not ([string]$status[0].imageID).EndsWith($Digest, [StringComparison]::Ordinal)) {
                    $bad.Add("Pod/$name imageID=$($status[0].imageID)")
                }
            }
            if ($bad.Count -eq 0) {
                Write-Ok "Fleet/$Fleet 可新分配 Ready 池全部命中 $Digest（旧 Allocated 对局允许受控排空）。"
                return
            }
            $lastReason = $bad -join '; '
        }
        if ([DateTime]::UtcNow -lt $deadline) { Start-Sleep -Seconds 5 }
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Fleet/$Fleet 未在 $([int]$Timeout.TotalSeconds)s 内把 Ready 池收敛到本次 digest:$lastReason。" +
          '不会强删仍在对局的旧 Allocated GameServer；请等待其按 battle_ttl 排空后另做全收敛审计。'
}

function Get-WorkloadTemplateVolumes($item) {
    switch ([string]$item.kind) {
        'Deployment'  { return @($item.spec.template.spec.volumes) }
        'StatefulSet' { return @($item.spec.template.spec.volumes) }
        'DaemonSet'   { return @($item.spec.template.spec.volumes) }
        'Job'         { return @($item.spec.template.spec.volumes) }
        'CronJob'     { return @($item.spec.jobTemplate.spec.template.spec.volumes) }
        default       { return @() }
    }
}

function Test-HasLegacyPandoraConfigMapRef($Node) {
    # ConvertFrom-Json 会把 restartedAt 等 ISO 字符串还原为 DateTime。DateTime 不是 primitive，
    # 但它和 TimeSpan/enum 等值类型的适配属性会不断产生新的值；继续递归同样会溢出。
    if ($null -eq $Node -or $Node -is [string] -or $Node.GetType().IsValueType) { return $false }
    # PowerShell 的 [pscustomobject] 类型加速器对 Object[] 也会返回 true；旧条件因此把 JSON
    # 数组当普通对象遍历，并沿数组的 SyncRoot 自引用无限递归，最终让 Resume 在 rollout 后
    # 报 call depth overflow。字符串已在上面排除，其余 IEnumerable 必须先逐项处理。
    if ($Node -is [System.Collections.IEnumerable]) {
        foreach ($entry in $Node) {
            if (Test-HasLegacyPandoraConfigMapRef $entry) { return $true }
        }
        return $false
    }
    foreach ($property in $Node.PSObject.Properties) {
        if ($property.Name -in @('configMap', 'configMapRef', 'configMapKeyRef')) {
            $nameProperty = if ($null -eq $property.Value) { $null } else { $property.Value.PSObject.Properties['name'] }
            if ($null -ne $nameProperty -and [string]$nameProperty.Value -ceq 'pandora-config') { return $true }
        }
        if (Test-HasLegacyPandoraConfigMapRef $property.Value) { return $true }
    }
    return $false
}

function Get-LegacyPandoraConfigMapRefs([object[]]$Items) {
    return @(
        foreach ($item in $Items) {
            if ($null -ne $item -and (Test-HasLegacyPandoraConfigMapRef $item.spec)) {
                "$($item.kind)/$($item.metadata.name)"
            }
        }
    )
}

# Secret 迁移的最后一道门：当前 22 个 Deployment 都已改挂 Secret、控制器模板与存活 Pod
# 均不再引用旧 ConfigMap 后才删除。历史零副本 ReplicaSet 刻意不纳入检查；删除后迁移前 revision
# 不再可直接 rollout undo，故本次切换是明确的配置载体回退边界。
function Remove-LegacyPandoraConfigMapAfterRollout([string]$KubeContext) {
    $oldName = @(& kubectl --context $KubeContext get configmap/pandora-config -n $K8sNamespace --ignore-not-found -o name 2>$null)
    if ($LASTEXITCODE -ne 0) { throw "查询旧 pandora-config ConfigMap 失败(exit=$LASTEXITCODE)" }
    if ([string]::IsNullOrWhiteSpace((($oldName | ForEach-Object { $_.ToString() }) -join '').Trim())) {
        Write-Skip '旧 pandora-config ConfigMap 不存在,无需迁移清理。'
        return
    }

    $deployments = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'deployments', '-n', $K8sNamespace, '-o', 'json') `
        -Action '查询 Deployment 配置卷'
    $deploymentItems = @($deployments.items)
    $deploymentByName = @{}
    foreach ($deployment in $deploymentItems) { $deploymentByName[[string]$deployment.metadata.name] = $deployment }

    $missingSecretRefs = @(
        foreach ($svc in (Get-ServiceList)) {
            $name = [string]$svc.Name
            if (-not $deploymentByName.ContainsKey($name)) { "$name(Deployment 不存在)"; continue }
            $volumes = @(Get-WorkloadTemplateVolumes $deploymentByName[$name])
            $secretRefs = @($volumes | Where-Object {
                    $null -ne $_.secret -and [string]$_.secret.secretName -ceq 'pandora-config'
                })
            if ($secretRefs.Count -eq 0) { "$name(未引用 Secret/pandora-config)" }
        }
    )
    if ($missingSecretRefs.Count -ne 0) {
        throw "旧 ConfigMap 不可删除:当前业务 Deployment 尚未全部迁移到 Secret:$($missingSecretRefs -join ', ')"
    }

    $controllerRefs = @(Get-LegacyPandoraConfigMapRefs -Items $deploymentItems)
    $otherControllers = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'statefulsets,daemonsets,replicationcontrollers,jobs,cronjobs', '-n', $K8sNamespace, '-o', 'json') `
        -Action '查询其它控制器配置卷'
    $controllerRefs += @(Get-LegacyPandoraConfigMapRefs -Items @($otherControllers.items))
    $replicaSets = Get-KubectlJsonObject -KubeContext $KubeContext `
        -Arguments @('get', 'replicasets', '-n', $K8sNamespace, '-o', 'json') `
        -Action '查询 ReplicaSet 配置引用'
    foreach ($rs in @($replicaSets.items)) {
        if ($null -eq $rs -or -not (Test-HasLegacyPandoraConfigMapRef $rs.spec)) { continue }
        $deploymentOwners = @($rs.metadata.ownerReferences | Where-Object {
                [string]$_.kind -ceq 'Deployment' -and ($_.controller -eq $true)
            })
        $specReplicas = if ($null -eq $rs.spec.replicas) { 0 } else { [int]$rs.spec.replicas }
        $statusReplicas = if ($null -eq $rs.status.replicas) { 0 } else { [int]$rs.status.replicas }
        # 只豁免 Deployment 管理且 desired/status 均为 0 的历史 revision；独立或仍活跃 RS 必须阻断。
        if ($deploymentOwners.Count -eq 0 -or $specReplicas -ne 0 -or $statusReplicas -ne 0) {
            $controllerRefs += "ReplicaSet/$($rs.metadata.name)"
        }
    }
    if ($controllerRefs.Count -ne 0) {
        throw "旧 ConfigMap 不可删除:仍有控制器模板引用 pandora-config:$($controllerRefs -join ', ')"
    }

    # rollout status 返回时旧 Pod 可能仍短暂 Terminating；最多等 60s，查询失败或超时均 fail-closed。
    $deadline = (Get-Date).AddSeconds(60)
    do {
        $pods = Get-KubectlJsonObject -KubeContext $KubeContext `
            -Arguments @('get', 'pods', '-n', $K8sNamespace, '-o', 'json') `
            -Action '查询存活 Pod 配置卷'
        $livePods = @($pods.items | Where-Object { [string]$_.status.phase -notin @('Succeeded', 'Failed') })
        $podRefs = @(Get-LegacyPandoraConfigMapRefs -Items $livePods)
        if ($podRefs.Count -eq 0) { break }
        if ((Get-Date) -ge $deadline) {
            throw "旧 ConfigMap 不可删除:60s 后仍有存活 Pod 引用 pandora-config:$($podRefs -join ', ')"
        }
        Start-Sleep -Seconds 2
    } while ($true)

    kubectl --context $KubeContext delete configmap pandora-config -n $K8sNamespace --ignore-not-found
    Assert-LastExit '删除旧 pandora-config ConfigMap'
    $remaining = @(& kubectl --context $KubeContext get configmap/pandora-config -n $K8sNamespace --ignore-not-found -o name 2>$null)
    if ($LASTEXITCODE -ne 0) { throw "复查旧 pandora-config ConfigMap 失败(exit=$LASTEXITCODE)" }
    if (-not [string]::IsNullOrWhiteSpace((($remaining | ForEach-Object { $_.ToString() }) -join '').Trim())) {
        throw '旧 pandora-config ConfigMap 删除后仍存在。'
    }
    Write-Ok '22 个业务 Deployment 与存活 Pod 已迁移到 Secret;旧 pandora-config ConfigMap 已删除。'
}

function Test-CommandExists([string]$cmd) {
    return [bool](Get-Command $cmd -ErrorAction SilentlyContinue)
}

# ===== 工具检查 + 显式安装 =====
# 返回 $true=就绪 / $false=缺失(未能装上)
function Ensure-Tool {
    param(
        [string]$Name,
        [string]$CheckCmd,
        [string]$WingetId,
        [string]$ManualUrl,
        [switch]$Required
    )
    if (Test-CommandExists $CheckCmd) {
        Write-Ok "$Name 已就绪"
        return $true
    }
    Write-Warn "$Name 未安装"
    if ($Check -or $NoInstall -or -not $Install) {
        if ($ManualUrl) { Write-Host "       手动安装:$ManualUrl" -ForegroundColor Yellow }
        if (-not $Check -and -not $NoInstall -and -not $Install) {
            Write-Host "       如需脚本尝试安装,请显式追加 -Install。" -ForegroundColor Yellow
        }
        return $false
    }
    if (-not $WingetId) {
        Write-Err "$Name 无法自动安装,请手动装:$ManualUrl"
        return $false
    }
    if (-not (Test-CommandExists 'winget')) {
        Write-Err "未找到 winget,无法自动安装 $Name;请手动装:$ManualUrl"
        return $false
    }
    Write-Info "  winget 安装 $Name ($WingetId) ..."
    winget install --id $WingetId --silent --accept-source-agreements --accept-package-agreements | Out-Null
    # winget 装完当前会话 PATH 可能没刷新
    if (Test-CommandExists $CheckCmd) {
        Write-Ok "$Name 安装成功"
        return $true
    }
    Write-Warn "$Name 已尝试安装,但当前终端还找不到命令 —— 多半是 PATH 未刷新。"
    Write-Warn "       请『新开一个终端』后重跑本脚本。"
    return $false
}

function Test-DockerRunning {
    if (-not (Test-CommandExists 'docker')) { return $false }
    docker info *> $null
    return ($LASTEXITCODE -eq 0)
}

# 确认 Docker Desktop 跑得起来的前置:CPU 虚拟化已开 + WSL2 可用。
# 返回 $true=就绪;$false=有缺项(已打印指引)。仅做只读检查,不改本机环境。
function Test-DockerPrereq {
    $ok = $true

    # ① CPU 虚拟化(BIOS/固件层)。Win32_Processor.VirtualizationFirmwareEnabled。
    try {
        $virt = (Get-CimInstance Win32_Processor -ErrorAction Stop |
                 Select-Object -First 1 -ExpandProperty VirtualizationFirmwareEnabled)
        if ($virt) {
            Write-Ok "CPU 虚拟化已开启(VirtualizationFirmwareEnabled=True)"
        } else {
            Write-Err "CPU 虚拟化未开启 —— 请进 BIOS 开 Intel VT-x / AMD-V 后再装 Docker。"
            $ok = $false
        }
    } catch {
        Write-Warn "无法读取 CPU 虚拟化状态($($_.Exception.Message));跳过该项检查。"
    }

    # ② WSL2(Docker Desktop 默认后端)。有 wsl 命令即视为可用;没有给出启用指引。
    if (Test-CommandExists 'wsl') {
        Write-Ok "WSL 命令可用(Docker Desktop 走 WSL2 后端)"
    } else {
        Write-Warn "未检测到 wsl 命令 —— Docker Desktop 首次启动会引导启用 WSL2。"
        Write-Host "       如启动失败,管理员执行:wsl --install  然后重启。" -ForegroundColor Yellow
    }

    return $ok
}

# winget 安装 Docker Desktop(仅在显式 -InstallDocker 时调用)。
# 注意:装完仍需『重启系统 + 手动启动 Docker Desktop + 新开终端』,脚本替不了这步。
function Install-DockerDesktop {
    if (-not (Test-CommandExists 'winget')) {
        Write-Err "未找到 winget,无法自动装 Docker Desktop;请手动装:https://www.docker.com/products/docker-desktop/"
        return $false
    }
    Write-Step "用 winget 安装 Docker Desktop"
    if (-not (Test-DockerPrereq)) {
        Write-Err "虚拟化前置未满足,已中止 Docker 安装(见上方指引)。"
        return $false
    }
    Write-Info "  winget install --id Docker.DockerDesktop ..."
    winget install --id Docker.DockerDesktop --silent --accept-source-agreements --accept-package-agreements | Out-Null
    if (Test-CommandExists 'docker') {
        Write-Ok "Docker Desktop 已安装。"
    } else {
        Write-Warn "Docker Desktop 已尝试安装,但当前终端还找不到 docker 命令(PATH 未刷新属正常)。"
    }
    Write-Warn "Docker Desktop 装完还需手动收尾:"
    Write-Host "       ① 重启系统(首次装基本都要)" -ForegroundColor Yellow
    Write-Host "       ② 启动 Docker Desktop,等鲸鱼图标变绿(daemon 起来)" -ForegroundColor Yellow
    Write-Host "       ③ 新开一个终端,重跑:pwsh tools/scripts/start.ps1 -Mode $Mode" -ForegroundColor Yellow
    return $false  # 本次不继续启动,交回控制权让用户重启
}

# 确保 docker 命令存在且 daemon 在跑。
# Docker Desktop 不随 -Install 自动装(需重启+引导);只有显式 -InstallDocker 才用 winget 装。
function Ensure-Docker {
    if (-not (Test-CommandExists 'docker')) {
        Write-Warn "Docker 未安装"
        if ($InstallDocker -and -not $Check) {
            return (Install-DockerDesktop)
        }
        Write-Host "       手动安装:https://www.docker.com/products/docker-desktop/" -ForegroundColor Yellow
        Write-Host "       或让脚本用 winget 装(装前确认虚拟化):追加 -InstallDocker" -ForegroundColor Yellow
        return $false
    }
    Write-Ok "Docker 已就绪"
    if ($Check) { return $true }
    if (-not (Test-DockerRunning)) {
        Write-Err "Docker 已装但 daemon 没在跑 —— 请启动 Docker Desktop 后重试。"
        return $false
    }
    Write-Ok "Docker daemon 运行中"
    return $true
}

function Ensure-Go {
    return (Ensure-Tool -Name 'Go' -CheckCmd 'go' -WingetId 'GoLang.Go' -ManualUrl 'https://go.dev/dl/ (需 1.26.5+)')
}

# 检查给定模式需要的工具;返回 $true=全就绪
# Assert-LocalEdgePortsFree:local 模式起 Envoy 前,先确认边缘端口没被别人占着。
# 客户端面 8443 归 $PANDORA_EDGE_BIND_HOST(默认 127.0.0.1),DS 面 8444 归
# $PANDORA_DS_EDGE_BIND_HOST(默认 127.0.0.1),两者都由 deploy/docker-compose.dev.yml 直接发布。
# 只把「真正会撞的地址」判为冲突:绑 127.0.0.1 时 0.0.0.0 的监听也会挡路,反之亦然。

# 占用方是「local 模式自己上一轮留下的 pandora-envoy」时不算冲突 —— dev_up 的
# `docker compose up -d` 会直接复用/重建它。不排掉的话「一键启动」第二次必失败:
# 第一次起完不停、再点一次就报端口被占,而人根本没起别的东西(本机实测踩到)。
# 注:监听方显示的是 com.docker.backend / wslrelay 这类 Docker 转发进程,从 PID 根本
# 反查不到是哪个容器,故改从 docker 实际发布端口这一侧认。
function Test-EdgePortHeldByOwnEnvoy([int]$Port) {
    # 先按**进程映像路径**认自己人。免 Docker 路线的 Envoy 是宿主原生进程
    # (run/localinfra/dist/envoy/envoy.exe),`docker port` 永远查不到它 —— 上一轮没停干净时
    # 就会被当成「外人占端口」硬阻断一键启动,而 local_infra.ps1 起 Envoy 前本来就会把它停掉
    # 重建(见那边的 Stop-Component 'envoy' + Stop-OrphanPortHolder),纯属误报。
    $native = [IO.Path]::GetFullPath((Join-Path $ProjectRoot 'run/localinfra/dist/envoy/envoy.exe'))
    foreach ($c in @(Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue)) {
        $proc = Get-Process -Id $c.OwningProcess -ErrorAction SilentlyContinue
        if (-not $proc) { continue }
        # 受保护进程读 .Path 会抛;认不出来就当外人,不能让它中断整个前置检查。
        $exe = $null
        try { $exe = $proc.Path } catch { $exe = $null }
        if ($exe -and [IO.Path]::GetFullPath($exe) -eq $native) { return $true }
    }

    # docker 分支放最后,且只在**确实有 docker** 时问:策划机压根没装 docker,而本文件是
    # $ErrorActionPreference='Stop',裸 `& docker` 会抛 CommandNotFoundException 直接打断一键启动
    # (只有端口真被占时才会走到这里,平时不暴露)。免 Docker 模式也没有 pandora-envoy 容器可问。
    if ($NoDocker -or -not (Test-CommandExists 'docker')) { return $false }
    $published = (& docker port pandora-envoy 2>$null | Out-String)
    if ([string]::IsNullOrWhiteSpace($published)) { return $false }
    return ($published -match ('(?m)->\s*\S+:{0}\s*$' -f $Port))
}

function Assert-LocalEdgePortsFree {
    $targets = @(
        @{ Port = 8443; Host = $(if ($env:PANDORA_EDGE_BIND_HOST) { $env:PANDORA_EDGE_BIND_HOST } else { '127.0.0.1' }); Face = '客户端面' },
        @{ Port = 8444; Host = $(if ($env:PANDORA_DS_EDGE_BIND_HOST) { $env:PANDORA_DS_EDGE_BIND_HOST } else { '127.0.0.1' }); Face = 'DS 面   ' }
    )

    $ok = $true
    foreach ($t in $targets) {
        $listens = @(Get-NetTCPConnection -State Listen -LocalPort $t.Port -ErrorAction SilentlyContinue)
        if ($listens.Count -eq 0) { continue }

        # 宿主要绑 X,已有监听在 Y:仅当 X/Y 相同,或任一方是通配地址时才真正冲突。
        $wildcards = @('0.0.0.0', '::', '*')
        $blocking = @($listens | Where-Object {
                $_.LocalAddress -eq $t.Host -or ($wildcards -contains $_.LocalAddress) -or ($wildcards -contains $t.Host)
            })
        if ($blocking.Count -eq 0) { continue }

        if (Test-EdgePortHeldByOwnEnvoy -Port $t.Port) {
            Write-Info ("Envoy {0} {1}:{2} 当前是本项目自己的 Envoy 占着(上一轮没停),会被直接重建,不算冲突。" -f $t.Face.Trim(), $t.Host, $t.Port)
            continue
        }

        $who = ($blocking | ForEach-Object {
                $proc = Get-Process -Id $_.OwningProcess -ErrorAction SilentlyContinue
                if ($proc) { "$($proc.ProcessName)(pid $($proc.Id))" } else { "pid $($_.OwningProcess)" }
            } | Sort-Object -Unique) -join ', '

        Write-Warn ("Envoy {0} {1}:{2} 已被占用 —— {3}" -f $t.Face.Trim(), $t.Host, $t.Port, $who)
        $ok = $false
    }

    if ($ok) { return $true }

    Write-Info ''
    # -Check 是干跑,故意没执行 Stop-K8sStackForLocal;真正启动时 k8s 会被先停掉,
    # 此处若占用方正是本机集群,就不该判成"前置不通过"。
    if ($Check -and (Test-CommandExists 'minikube')) {
        $mkHolder = Find-MinikubeProfileHoldingEdgePort
        if (-not [string]::IsNullOrWhiteSpace($mkHolder)) {
            Write-Info "占用方是本机 k8s 集群『$mkHolder』(与 local 互斥)。-Check 是干跑不做停服;"
            Write-Info "真正启动 local 时会自动 minikube stop 把它停掉再起,无需手工处理。"
            return $true
        }
    }
    Write-Info 'local 模式的 Envoy 用固定宿主端口,被占就起不来。'
    # 本机 k8s 曾是最常见的占用方,现在 local 启动前会自动 minikube stop(见 Stop-K8sStackForLocal),
    # 所以走到这里的占用方通常是别的进程:老的 envoy 容器、别人手工起的服务、或另一个 IDE 终端。
    Write-Info '请先停掉上面点名的进程/容器再重跑;若那是上一轮没停干净的本项目服务,可先执行:'
    # 停机命令必须跟启动路线一致:免 Docker 起的服务,拿不带 -NoDocker 的 -Down 去停会走 docker
    # compose 分支,原生进程一个都停不掉 —— 照着提示做了还是起不来,比不给提示更伤人。
    if ($NoDocker) {
        Write-Info '    双击「策划一键停止-免Docker-测试版.cmd」'
        Write-Info '    (等价命令:pwsh tools/scripts/start.ps1 -Mode local -NoDocker -Down)'
    } else {
        Write-Info '    pwsh tools/scripts/start.ps1 -Mode local -Down'
    }
    Write-Info '只想自己本机玩、不对局域网开放时,可加 -LocalOnly 只绑回环。'
    return $false
}

function Resolve-Prerequisites([string]$mode) {
    Write-Step "检查必要工具($mode 模式)"
    # deploy/env/dev.env 命中 .gitignore 的 `*.env`,受版本控制的只有 dev.env.example ——
    # 新机器 / 新克隆第一次跑必然没有它,而 docker compose 的 --env-file 指到不存在的路径会
    # 直接失败(2026-08-12 现场:另一台机器刚更新完双击一键启动,断在 [1/4] 基础设施)。
    # 放在这一层而不是只放 dev_up.ps1:这一步就是策划眼里的「前置检查」,信息出现在他正看的
    # 那一段;dev_up / dev_down / dev_status 各自也接了同一个函数,单独跑它们同样自愈。
    # -Check 干跑也照建:缺这个文件时报「工具全部就绪」本身就是假的,建完才是真就绪。
    # online 是远端集群,不碰本机 compose,跳过。
    if ($mode -ne 'online') { Confirm-DevEnvFile -ProjectRoot $ProjectRoot }
    $allOk = $true
    switch ($mode) {
        'local' {
            # Go 只在需要现场编译时才是硬前置:仓库里带了 run/artifacts/windows/bin 预编译产物
            # (由 tools/scripts/build_release_binaries.ps1 在后端开发机/CI 上生成并随目录分发)时,
            # 策划机不装 Go 也能跑 local 模式 —— run_services.ps1 会自动改用这批二进制。
            if (-not (Test-CommandExists 'go')) {
                $prebuilt = @(Get-ChildItem -LiteralPath (Join-Path $ProjectRoot 'run/artifacts/windows/bin') -Filter '*.exe' -ErrorAction SilentlyContinue)
                if ($prebuilt.Count -gt 0) {
                    Write-Info "本机没装 Go,但找到 $($prebuilt.Count) 个预编译二进制(run/artifacts/windows/bin),将直接使用,免装 Go。"
                } elseif (-not (Ensure-Go)) {
                    Write-Info "或者:让后端同学在装了 Go 的机器上跑 pwsh tools/scripts/build_release_binaries.ps1,把 run/artifacts 目录同步过来,本机就不用装 Go。"
                    $allOk = $false
                }
            }
            if ($NoDocker) {
                # 免 Docker 模式:基础设施是本机原生进程(local_infra.ps1),不需要 Docker Desktop。
                # mkcert 仍要(Envoy 的 TLS 叶子证书得本机签,SAN 含本机局域网 IP),但**不在这里拦人**:
                # local_infra.ps1 -Action provision 会把它连同 MySQL/Redis/Kafka/Envoy 一起自动备料到
                # run/localinfra/dist/mkcert,并只挂进那个进程的 PATH。策划机的增量安装因此只剩 pwsh 7。
                # 这里在前置检查阶段就去下它没有意义:真正用到是在 [1/3] 起 Envoy 那一步,
                # 失败也在那里报,提前下反而把「检查工具」这一步变成一次几 MB 的联网操作。
                if (Test-CommandExists 'mkcert') {
                    Write-Ok 'mkcert 已在 PATH(本机自带的那份)'
                } else {
                    Write-Info 'mkcert 未安装 —— 不用管,起基础设施时会自动备料到 run/localinfra/dist/mkcert(不写系统 PATH)。'
                }
            } elseif (-not (Ensure-Docker)) { $allOk = $false }
            # 先定下 Envoy 边缘绑定地址(默认 0.0.0.0 对局域网开放 / -LocalOnly 只绑回环),
            # 后面的端口占用判定才能按真实绑定地址算。
            Initialize-LocalEdgeBinding
            # local 与 k8s 互斥(两者都占宿主 8443):先用 k8s 自己的关闭流程停掉集群。
            # -Check 是干跑,不做任何停服副作用。
            if (-not $Check) { Stop-K8sStackForLocal }
            # Envoy 的 8443/8444 是 compose 发布的固定宿主端口,被占时 dev_up 只会抛一句
            # "port is already allocated",看不出是谁占的。提前点名省得来回猜。
            if (-not (Assert-LocalEdgePortsFree)) { $allOk = $false }
        }
        'docker' {
            if (-not (Ensure-Docker)) { $allOk = $false }
            # mkcert:Envoy 本地 TLS 证书自动签发 / 共享 CA 安装都靠它,缺了 dev_up 起不来 Envoy。
            if (-not (Ensure-Tool -Name 'mkcert' -CheckCmd 'mkcert' -WingetId 'FiloSottile.mkcert' -ManualUrl 'https://github.com/FiloSottile/mkcert#installation')) { $allOk = $false }
        }
        'intranet' {
            if (-not (Ensure-Docker)) { $allOk = $false }
            # 内网服务器要给局域网策划发 TLS 证书,mkcert 必备(自动签叶子证书 + 装全队共享 CA)。
            if (-not (Ensure-Tool -Name 'mkcert' -CheckCmd 'mkcert' -WingetId 'FiloSottile.mkcert' -ManualUrl 'https://github.com/FiloSottile/mkcert#installation')) { $allOk = $false }
        }
        'battle' {
            # battle 模式已废弃(decision-revisit-retire-battle-mode.md):仅保留 -Down/-Status
            # 清理/查看旧机器遗留环境,前置只需 Docker(不再要求 Go / mkcert)。
            if (-not (Ensure-Docker)) { $allOk = $false }
        }
        'k8s' {
            if (-not (Ensure-Docker)) { $allOk = $false }
            if (-not (Ensure-Tool -Name 'kubectl'  -CheckCmd 'kubectl'  -WingetId 'Kubernetes.kubectl'  -ManualUrl 'https://kubernetes.io/docs/tasks/tools/')) { $allOk = $false }
            if (-not (Ensure-Tool -Name 'minikube' -CheckCmd 'minikube' -WingetId 'Kubernetes.minikube' -ManualUrl 'https://minikube.sigs.k8s.io/docs/start/')) { $allOk = $false }
            # -Down 只做「清宿主桥接 + 删集群内对象」:不装 Agones(helm)、不构建镜像(Go)、
            # 不签 Envoy 证书(mkcert)。再拿部署期的工具卡停止,只会让没装全的机器连停都停不下来。
            if (-not $Down) {
                if (-not (Ensure-Tool -Name 'helm' -CheckCmd 'helm' -WingetId 'Helm.Helm' -ManualUrl 'https://helm.sh/docs/intro/install/')) { $allOk = $false }
                # [5/8] 构建 22 个服务镜像 = 宿主 Go 交叉编译,Go 缺失会在最贵的步骤中途才炸。
                if (-not (Ensure-Tool -Name 'Go' -CheckCmd 'go' -WingetId 'GoLang.Go' -ManualUrl 'https://go.dev/dl/')) { $allOk = $false }
                # [7.5/8] 集群内边缘 Envoy 的 dev 证书(deploy/envoy/cert.pem,不入库)缺失时由
                # envoy_cert.ps1 用 mkcert 自动重签——新机器必经此路径,mkcert 缺失会中止部署。
                if (-not (Ensure-Tool -Name 'mkcert' -CheckCmd 'mkcert' -WingetId 'FiloSottile.mkcert' -ManualUrl 'https://github.com/FiloSottile/mkcert#installation')) { $allOk = $false }
            }
        }
        'online' {
            if (-not (Ensure-Tool -Name 'kubectl' -CheckCmd 'kubectl' -WingetId 'Kubernetes.kubectl' -ManualUrl 'https://kubernetes.io/docs/tasks/tools/')) { $allOk = $false }
        }
    }
    return $allOk
}

# ===== 本机 DS 进程形态(-DsLauncher)解析 =====

# Resolve-PandoraUProject:定位 Pandora.uproject。
# 优先级:-DsProject > $env:PANDORA_DS_UPROJECT > 与本仓库平级的客户端仓常见布局。
# 「平级探测」是为了策划机零手改:只要把客户端仓和服务器仓放在同一个父目录下就能自动找到,
# 不依赖任何写死的盘符(开发机 F:\work、策划机 D:\game 都一样能用)。
function Resolve-PandoraUProject {
    param([string]$Explicit)

    $candidates = @()
    if (-not [string]::IsNullOrWhiteSpace($Explicit)) { $candidates += $Explicit.Trim() }
    if (-not [string]::IsNullOrWhiteSpace($env:PANDORA_DS_UPROJECT)) { $candidates += $env:PANDORA_DS_UPROJECT.Trim() }

    # 客户端仓候选路径与导表共用一份公共件(client_repo_lib.ps1)。两处各写一套的后果是
    # 「DS 读 A 仓的资源、导表用 B 仓的表」,而且两边都不报错。候选里已含 SVN 原名 Client、
    # 文档里的 Pandora-Client-SVN、策划自取的仓名,以及 PANDORA_CLIENT_REPO 指定的目录。
    foreach ($repoRoot in (Get-PandoraClientRepoCandidate -ProjectRoot $ProjectRoot)) {
        $candidates += (Join-Path $repoRoot 'Pandora\Pandora.uproject')
    }

    foreach ($c in $candidates) {
        if ((-not [string]::IsNullOrWhiteSpace($c)) -and (Test-Path -LiteralPath $c -PathType Leaf)) {
            return (Resolve-Path -LiteralPath $c).Path
        }
    }
    return $null
}

# Resolve-UnrealEditorExe:按 .uproject 的 EngineAssociation 定位 UnrealEditor.exe。
# EngineAssociation 有两种形态,对应引擎的两条安装路线,都要支持(策划用发行版,程序用源码版):
#   "{GUID}" 源码/自建版 → HKCU:\Software\Epic Games\Unreal Engine\Builds 里 GUID→引擎根目录
#                          (由客户端仓的 Tool\Build\RegisterEngine.bat 写入)
#   "5.8"    Epic 发行版 → HKLM:\SOFTWARE\EpicGames\Unreal Engine\5.8 的 InstalledDirectory
#                          注册表项可能缺失(实测有机器装了 UE_5.8 但启动器没写 HKLM),
#                          故再兜一层启动器自己的安装清单 LauncherInstalled.dat。
# 全都查不到时再退回「已装引擎里版本号最高的一个发行版」,避免策划漏跑 RegisterEngine 就卡住。
function Get-LauncherInstalledEngineRoots {
    # 返回 [pscustomobject]{ Version; Root },按版本号从高到低。发行版条目形如 AppName="UE_5.8"。
    $dat = Join-Path ([Environment]::GetFolderPath('CommonApplicationData')) 'Epic\UnrealEngineLauncher\LauncherInstalled.dat'
    if (-not (Test-Path -LiteralPath $dat -PathType Leaf)) { return @() }
    try { $list = (Get-Content -LiteralPath $dat -Raw | ConvertFrom-Json).InstallationList } catch { return @() }
    $rows = foreach ($item in @($list)) {
        if (-not $item.AppName -or -not $item.InstallLocation) { continue }
        if ([string]$item.AppName -notmatch '^UE_(\d+\.\d+)$') { continue }
        [pscustomobject]@{ Version = $Matches[1]; Root = [string]$item.InstallLocation }
    }
    return @($rows | Sort-Object { try { [version]$_.Version } catch { [version]'0.0' } } -Descending)
}

function Resolve-UnrealEditorExe {
    param([string]$Explicit, [string]$UProjectPath)

    if (-not [string]::IsNullOrWhiteSpace($Explicit)) {
        if (Test-Path -LiteralPath $Explicit.Trim() -PathType Leaf) { return (Resolve-Path -LiteralPath $Explicit.Trim()).Path }
        throw "-DsEditorExe 指向的文件不存在: $Explicit"
    }
    if (-not [string]::IsNullOrWhiteSpace($env:PANDORA_DS_EXE)) {
        $fromEnv = $env:PANDORA_DS_EXE.Trim()
        if (Test-Path -LiteralPath $fromEnv -PathType Leaf) { return (Resolve-Path -LiteralPath $fromEnv).Path }
    }

    $roots = New-Object System.Collections.Generic.List[string]
    $launcherRoots = @(Get-LauncherInstalledEngineRoots)

    $assoc = $null
    if ($UProjectPath -and (Test-Path -LiteralPath $UProjectPath -PathType Leaf)) {
        try { $assoc = (Get-Content -LiteralPath $UProjectPath -Raw | ConvertFrom-Json).EngineAssociation } catch { $assoc = $null }
    }

    if (-not [string]::IsNullOrWhiteSpace($assoc)) {
        if ($assoc -match '^\{.*\}$') {
            $builds = Get-ItemProperty -Path 'HKCU:\Software\Epic Games\Unreal Engine\Builds' -ErrorAction SilentlyContinue
            if ($builds -and $builds.PSObject.Properties[$assoc]) { $roots.Add([string]$builds.$assoc) }
        } else {
            $key = Get-ItemProperty -Path "HKLM:\SOFTWARE\EpicGames\Unreal Engine\$assoc" -ErrorAction SilentlyContinue
            if ($key -and $key.InstalledDirectory) { $roots.Add([string]$key.InstalledDirectory) }
            # 注册表缺项时用启动器清单补齐:工程写了 5.8 就必须优先拿 5.8,不能掉进下面的兜底
            # 去挑源码版引擎 —— 血统对不上会被 Assert-PandoraEditorModulesMatch 拦下。
            foreach ($row in $launcherRoots) {
                if ($row.Version -eq ([string]$assoc).Trim()) { $roots.Add($row.Root) }
            }
        }
    }

    # 兜底:注册表里所有已装引擎(源码版 + 发行版),发行版按版本号从高到低。
    $builds = Get-ItemProperty -Path 'HKCU:\Software\Epic Games\Unreal Engine\Builds' -ErrorAction SilentlyContinue
    if ($builds) {
        foreach ($p in $builds.PSObject.Properties) {
            if ($p.Name -like '{*}' -and $p.Value) { $roots.Add([string]$p.Value) }
        }
    }
    $installed = @(Get-ChildItem -Path 'HKLM:\SOFTWARE\EpicGames\Unreal Engine' -ErrorAction SilentlyContinue |
        Sort-Object { try { [version]$_.PSChildName } catch { [version]'0.0' } } -Descending)
    foreach ($k in $installed) {
        $dir = (Get-ItemProperty -Path $k.PSPath -ErrorAction SilentlyContinue).InstalledDirectory
        if ($dir) { $roots.Add([string]$dir) }
    }
    foreach ($row in $launcherRoots) { $roots.Add($row.Root) }

    $candidates = New-Object System.Collections.Generic.List[string]
    foreach ($root in $roots) {
        if ([string]::IsNullOrWhiteSpace($root)) { continue }
        $exe = Join-Path $root 'Engine\Binaries\Win64\UnrealEditor.exe'
        if (Test-Path -LiteralPath $exe -PathType Leaf) { $candidates.Add((Resolve-Path -LiteralPath $exe).Path) }
    }
    if ($candidates.Count -eq 0) { return $null }

    # 血统优先:这台机器上往往同时装着「出包用的 Installed Build」和「Epic 发行版」,而工程的
    # 编辑器 DLL 只可能由其中一台编出来(Tool\Build\_Common.bat 固定用 Installed Build 出包,
    # 但编辑器二进制也可能是发行版编的)。谁编的就用谁跑,否则 BuildId 对不上,DS 会被
    # Assert-PandoraEditorModulesMatch 拦下 —— 与其让人工去猜,不如按 BuildId 自动挑。
    $projId = $null
    if ($UProjectPath) {
        $projModules = Join-Path (Split-Path -Parent $UProjectPath) 'Binaries\Win64\UnrealEditor.modules'
        if (Test-Path -LiteralPath $projModules -PathType Leaf) {
            try { $projId = [string](Get-Content -Raw -LiteralPath $projModules | ConvertFrom-Json).BuildId } catch { $projId = $null }
        }
    }
    if (-not [string]::IsNullOrWhiteSpace($projId)) {
        foreach ($exe in $candidates) {
            $engModules = Join-Path (Split-Path -Parent $exe) 'UnrealEditor.modules'
            if (-not (Test-Path -LiteralPath $engModules -PathType Leaf)) { continue }
            try { $engId = [string](Get-Content -Raw -LiteralPath $engModules | ConvertFrom-Json).BuildId } catch { continue }
            if ($engId -eq $projId) { return $exe }
        }
    }
    return $candidates[0]
}

# Set-LocalDsLauncherEnv:把 -DsLauncher 的选择翻译成 allocator 认的环境变量。
# 走环境变量而不是改 yaml:与既有 PANDORA_DS_EXE / PANDORA_DS_DIR / PANDORA_DS_ADVERTISE_HOST
# 的注入方式一致 —— 仓库里的 dev yaml 保持 packaged 不动,策划要免出包就加一个开关,
# 不传开关时这里一个环境变量都不设,原有链路逐字节不变。
function Set-LocalDsLauncherEnv {
    if ([string]::IsNullOrWhiteSpace($DsLauncher)) { return }

    if ($DsLauncher -eq 'packaged') {
        $env:PANDORA_DS_LAUNCHER = 'packaged'
        Write-Info "DS 进程形态:packaged(打包好的 PandoraServer.exe;改资源需重新出包)。"
        return
    }

    $uproject = Resolve-PandoraUProject -Explicit $DsProject
    if (-not $uproject) {
        throw "-DsLauncher editor 需要 Pandora.uproject,但没找到。请把客户端仓放到与本仓库平级的目录,或显式传 -DsProject <...\Pandora.uproject>。"
    }
    $editorExe = Resolve-UnrealEditorExe -Explicit $DsEditorExe -UProjectPath $uproject
    if (-not $editorExe) {
        throw "-DsLauncher editor 需要 UnrealEditor.exe,但按 $uproject 的 EngineAssociation 在注册表里没查到引擎。请先在客户端仓跑 Tool\Build\RegisterEngine.bat,或显式传 -DsEditorExe <...\Engine\Binaries\Win64\UnrealEditor.exe>。"
    }

    $env:PANDORA_DS_LAUNCHER = 'editor'
    $env:PANDORA_DS_UPROJECT = $uproject
    $env:PANDORA_DS_EXE      = $editorExe
    # 工作目录跟引擎走(与 packaged 时指向 staged 根目录同理)。
    $env:PANDORA_DS_DIR      = Split-Path -Parent $editorExe

    Write-Info "DS 进程形态:editor(免出包,直接读工程里未 cook 的资源)"
    Write-Info "  引擎  : $editorExe"
    Write-Info "  工程  : $uproject"
    Assert-PandoraEditorModulesMatch -EditorExe $editorExe -UProjectPath $uproject
    Write-Warn "editor 形态的 DS 启动慢(加载一大批编辑器模块 + 读未 cook 的散装资产),首次进大厅/进图等一两分钟属正常;allocator 已自动放宽 ready/心跳超时。"
}

# Assert-PandoraEditorModulesMatch:校验「工程编辑器 DLL 的血统」与「即将用来跑 DS 的引擎」一致。
#
# 为什么必须查:UE 用 .modules 里的 BuildId 判断工程 DLL 是不是这个引擎编的。对不上时编辑器要弹
# 「模块过期,是否重新编译」对话框 —— 但 DS 是 headless 跑的,弹不出来,进程直接退。表现只是
# allocator 干等到 ready 超时,是这个模式最难查的坑,所以在启动前就拦下并给出可执行的修复指引。
#
# 血统对应(客户端仓 Tool/Build/README.md):源码版血统的 BuildId 是 GUID,Epic 发行版是 CL 号。
# 两条血统各自都能跑 editor 形态的 DS —— 本函数不要求用哪条,只要求**工程 DLL 和引擎是同一条**。
# 读不到任何一边(工程没编过 / 引擎目录结构特殊)时只告警不拦截,避免误伤。
function Assert-PandoraEditorModulesMatch {
    param([string]$EditorExe, [string]$UProjectPath)

    function Get-ModulesBuildId([string]$ModulesPath) {
        if (-not (Test-Path -LiteralPath $ModulesPath)) { return $null }
        try { return [string](Get-Content -Raw -LiteralPath $ModulesPath | ConvertFrom-Json).BuildId } catch { return $null }
    }

    $projModules   = Join-Path (Split-Path -Parent $UProjectPath) 'Binaries\Win64\UnrealEditor.modules'
    $engineModules = Join-Path (Split-Path -Parent $EditorExe) 'UnrealEditor.modules'

    $projId   = Get-ModulesBuildId $projModules
    $engineId = Get-ModulesBuildId $engineModules

    if (-not $projId) {
        Write-Warn "没读到工程编辑器模块信息($projModules);若 DS 启动后立刻退出,多半是工程还没用这个引擎编译过编辑器二进制。"
        return
    }
    if (-not $engineId) {
        Write-Info "  血统  : 引擎侧未提供 BuildId,跳过血统校验(工程侧 = $projId)。"
        return
    }
    if ($projId -eq $engineId) {
        Write-Info "  血统  : 工程 DLL 与引擎一致($projId)。"
        return
    }

    $clientRepo = Split-Path -Parent (Split-Path -Parent $UProjectPath)
    $msg = @"
editor 形态无法启动:工程编辑器 DLL 与引擎不是同一血统(BuildId 对不上)。
  工程 DLL : $projId   ($projModules)
  引擎     : $engineId   ($EditorExe)
这种情况下 UE 会认为模块过期并要求重新编译,而 DS 是 headless 跑的、弹不出对话框,进程会直接退出
(表现为一直等不到 DS ready)。请先让两者对齐再重跑本脚本 —— 用哪条引擎都行,本模式不挑引擎
(自建源码版 / Epic 发行版都能跑 editor 形态的 DS),只要求工程 DLL 是这台引擎编出来的。
对齐办法:

"@
    # 客户端仓自带的重编入口;仓库结构变动时不显示死链。
    $buildEditor = Join-Path $clientRepo 'Tool\Build\BuildEditor.bat'
    if (Test-Path -LiteralPath $buildEditor) {
        $msg += "  用当前这台引擎重编一次工程的编辑器二进制(推荐):`n    $buildEditor`n"
    }
    else {
        $msg += "  用当前这台引擎重编一次工程的编辑器二进制(推荐)。`n"
    }
    $registerEngine = Join-Path $clientRepo 'Tool\Build\RegisterEngine.bat'
    if (Test-Path -LiteralPath $registerEngine) {
        $msg += "  或改用与工程 DLL 匹配的那台引擎(改本机注册):`n    $registerEngine ""<引擎目录>""`n"
    }
    $msg += "  也可以直接设环境变量 PANDORA_DS_EXE 指到匹配的 UnrealEditor.exe,或用 -DsEditorExe 传入。`n"
    throw $msg
}

# ===== local 模式(宿主 go 进程 + docker 基础设施)=====
# ===== local / k8s 互斥 =====
# 两种模式的边缘入口都占用宿主 8443(local 是 docker-compose 的 pandora-envoy;k8s 是 minikube
# 节点容器把 NodePort 31443 发布到宿主 8443),同一台机器不可能同时成立。
# 因此起一个之前,先用**另一个模式自己的关闭流程**把它停干净,而不是让用户去猜
# `port is already allocated` 是谁占的。
# 注意:k8s 的端口发布属于 minikube 节点容器本身,`-Mode k8s -Down`(只删集群内对象)
# 并不会释放它,必须 `minikube stop`——数据保留,下次 -Mode k8s 会自动 start 回来。
# 找出「当前正占着宿主 8443」的 minikube profile 名(找不到返回 $null)。
# 注意不能用 Get-K8sManagedProfile / `minikube profile`:那是**active profile**,是全局可变状态,
# 完全可能指向另一个(甚至已停的)集群 —— 实测本机 active=minikube(已停),真正占 8443 的却是
# pandora-agones。停错集群 = 端口没释放且用户毫不知情,故按 docker 实际发布端口反查。
function Find-MinikubeProfileHoldingEdgePort {
    foreach ($p in @(Get-MinikubeProfileNames)) {
        $ports = (& docker port $p 2>$null | Out-String)
        if ($ports -match '(?m)->\s*\S+:8443\s*$') { return $p }
    }
    return $null
}

function Stop-K8sStackForLocal {
    if (-not (Test-CommandExists 'minikube')) { return }
    $mkProfile = Find-MinikubeProfileHoldingEdgePort
    if ([string]::IsNullOrWhiteSpace($mkProfile)) { return }

    Write-Info "检测到本机 k8s 集群『$mkProfile』占着宿主 8443,local 模式与它互斥,先停掉..."
    # 宿主侧桥接(port-forward / relay 容器)也是 k8s 模式起的,一并清掉,否则残留进程继续占端口。
    Stop-K8sHostBridge
    & minikube stop -p $mkProfile 2>&1 | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "自动停止 minikube『$mkProfile』失败,local 模式的 Envoy 会因 8443 被占而起不来。请手动执行:minikube stop -p $mkProfile"
    }
    Write-Ok "k8s 集群已停止(数据保留,下次 -Mode k8s 会自动起回来)。"
}

function Stop-LocalStackForK8s {
    # local 模式 = docker 基础设施(含 pandora-envoy 占 8443)+ 宿主 go 进程(占 20001-20022)。
    # 用 local 自己的关闭流程停干净;没起过时这步是空跑。
    $envoyRunning = @(docker ps --filter 'name=pandora-envoy' --format '{{.Names}}' 2>$null | Where-Object { $_ })
    $hostSvc = @(Get-Process -Name 'gateway', 'login', 'player' -ErrorAction SilentlyContinue)
    if ($envoyRunning.Count -eq 0 -and $hostSvc.Count -eq 0) { return }

    Write-Info "检测到 local 模式的服务在跑(占着宿主 8443 / 20001-20022),k8s 模式与它互斥,先停掉..."
    & "$ScriptDir/dev_all.ps1" -Down
    Write-Ok "local 模式已停止。"
}

function Invoke-Local {
    if ($Down) {
        & "$ScriptDir/dev_all.ps1" -Down -NoDocker:$NoDocker
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        return
    }
    if ($NoDocker) {
        Write-Step "local 模式(免 Docker):基础设施(本机原生进程) + 22 个 go 服务(宿主进程)"
        Write-Info "MySQL/Redis/Kafka/Envoy 走免安装二进制;不装 Docker Desktop、不起 TiDB。"
    } else {
        Write-Step "local 模式:基础设施(docker) + 22 个 go 服务(宿主进程)"
        Write-Info "策划本地联调用这个;服务可在 VS Code 断点调试。"
    }

    # 与 k8s 模式互斥 + 局域网绑定已在 Resolve-Prerequisites 里做完(必须早于端口占用检查)。

    # 本机 DS 用打包 exe 还是引擎编辑器(不传 -DsLauncher 则完全沿用 yaml,行为不变)。
    Set-LocalDsLauncherEnv

    # docker 业务容器会占用同一批端口(20001-20022),与宿主 go 进程互斥。
    # 若上一轮跑过 docker / intranet(非战斗)模式,业务容器还在跑 → 宿主 hub_allocator 起来即
    # `listen tcp :20021: bind: 已被占用` 崩溃,进而拉不起本机 Hub DS(PandoraServer.exe),
    # 客户端登录后卡在连大厅。这里在起宿主进程前,先把 docker 业务容器停掉(与 Invoke-Docker 反向对称)。
    # 免 Docker 模式下本机可能压根没有 docker 命令,这段整体跳过。
    if (-not $NoDocker -and (Test-Path $ComposeServices)) {
        $svcContainers = @(docker compose -f $ComposeServices ps --quiet 2>$null | Where-Object { $_ })
        if ($svcContainers.Count -gt 0) {
            Write-Info "检测到 docker 业务容器在跑(会抢 20001-20022 端口),先停掉它们..."
            docker compose -f $ComposeServices down 2>$null | Out-Null
        }
    }

    & "$ScriptDir/dev_all.ps1" -NoDocker:$NoDocker
    # dev_all.ps1 每一步失败都会 exit 1,但 `&` 调子脚本**不会**让本脚本失败 —— 不透传的话
    # start.ps1 走完 switch 就正常结束,双击窗口 / Web 管理台拿到的是「完成(退出码 0)」,
    # 而基础设施其实压根没起来(2026-08-12 现场:另一台机器缺 dev.env,[1/4] 就断了,外层照报 0)。
    # 一键启动的绿灯必须等于「真的起来了」,否则策划只会去查客户端。docker 分支(见 Invoke-Docker)
    # 一直是有检查的,只有 local 这条漏了。
    if ($LASTEXITCODE -ne 0) {
        Write-Err "local 模式启动失败(见上方第一条 [ERR];退出码 $LASTEXITCODE)。后端没起来,别去查客户端。"
        exit $LASTEXITCODE
    }
}

# ===== 导表(-GenTables):策划 xlsx → 服务端配置表 =====
#
# 为什么要塞进一键入口:策划改表就是为了测这一改动,而 4 个读表的 go 服务是在**进程启动时**
# 从 configtable/dist 加载的。让策划自己记着"先双击导表、再双击启动"迟早会漏一次,
# 而漏掉的表现是"我明明改了表却没生效",最难自查。
#
# 为什么 docker / k8s 也要导:表**不在镜像里**。deploy/services/Dockerfile 只 COPY 了各服务
# 的 etc/,configtable 一个字节都没进镜像;docker-compose 是把 ../configtable/dist 以只读
# bind mount 挂到 /app/configtable/active,k8s 则由 Apply-PandoraConfigTableConfigMap 读
# **磁盘上的** configtable/dist 生成 pandora-configtable ConfigMap 再挂进去。也就是说所有
# 形态吃的都是同一份磁盘目录 —— 早先"docker/k8s 的表随镜像走"的说法是错的,正是那个错误
# 判断把非 local 入口挡在了导表之外。
#
# 真正的生成逻辑一行都不重写,直接调既有的 configtable_gen.ps1(它已经会自动定位客户端
# Table 目录、自动取 SVN 版本号、没装 Go 就用预编译 exe,并把生成器的报错翻译成人话)。
function Get-ConfigTableDistVersion {
    $p = Join-Path $ProjectRoot 'configtable/dist/manifest.json'
    if (-not (Test-Path -LiteralPath $p -PathType Leaf)) { return $null }
    try {
        $json = [System.Text.UTF8Encoding]::new($false).GetString([System.IO.File]::ReadAllBytes($p)) | ConvertFrom-Json
        return [string]$json.version
    } catch { return $null }
}

function Invoke-ConfigTableGen {
    # online 是唯一的例外:线上集群部署的表必须能从 git 检出原样复现,不能取决于"执行部署的
    # 那台机器上恰好检出了哪个版本的 SVN 工作副本"。线上要换表就走提交 configtable/dist。
    if ($Mode -eq 'online') {
        throw "-GenTables 不支持 -Mode online(线上部署的表必须来自 git 检出的 configtable/dist,不能就地生成)。"
    }

    Write-Step "导表:策划 xlsx → 服务端配置表(configtable/dist)"

    # 非 local(内网 / k8s / docker)跑在共享服务器上,那台机器不一定检出了客户端 SVN。
    # 拿不到源表时**不能**把启动拦下来:git 跟踪的 configtable/dist 本身就是完整可用的一批
    # (下游 Get-PandoraConfigTableCandidate 还会逐表验 checksum),"这次没得可导"不等于
    # "服务不该起" —— 为了一台没有策划源表的机器而拒绝启动,恰恰是把可用性问题造出来。
    # 用 configtable_gen.ps1 -Check 探测而不是在这里重写一遍定位逻辑,免得两处口径漂移。
    if ($Mode -ne 'local') {
        $probe = & "$ScriptDir/configtable_gen.ps1" -Check 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Warn "本机导不了表,跳过导表,沿用仓库里已有的 configtable/dist。"
            $probe | ForEach-Object { Write-Host "       $_" }
            Write-Info "想让这台机器也能导表:检出客户端 SVN,再设 PANDORA_CLIENT_TABLE_ROOT 指向它的 Table 目录。"
            return $false
        }
    }

    $before = Get-ConfigTableDistVersion

    & "$ScriptDir/configtable_gen.ps1"
    $rc = $LASTEXITCODE
    if ($rc -ne 0) {
        Write-Host ""
        Write-Err "导表没过,已中止(上面有具体原因和该找谁)。"
        Write-Info "为什么不带着旧表继续起:你改表就是为了测它,用旧表起来只会让你以为改动没生效 —— 那比起不来更难查。"
        Write-Info "配置表是全批原子的,现在 dist 一个字节没动,原来的表还能用。"
        Write-Info "确实想先不导表、把后端起起来:去掉 -GenTables 再跑一次(用仓库里已有的那批表)。"
        exit $rc
    }

    $after = Get-ConfigTableDistVersion
    if ($null -eq $after) {
        Write-Warn "导表报成功但读不到 configtable/dist/manifest.json,按「表有改动」处理(宁可多重启一次)。"
        return $true
    }
    if ($before -eq $after) {
        Write-Info "配置表内容与上一批相同(v$after),读表的服务不用动。"
        return $false
    }
    Write-Ok ("配置表批次 {0} -> v{1}。" -f $(if ($before) { "v$before" } else { '(无)' }), $after)
    return $true
}

# ===== local 快速通道:只重启本机 DS(-DsOnly)=====
#
# 场景:策划只更新了客户端仓(改资源 / 重编编辑器 DLL),后端 go 服务与基础设施一行没变。
# 这条路只做真正相关的三件事:
#   1) 重新解析 DS 形态(顺带做血统校验 —— 刚重编过编辑器 DLL 最容易在这里翻车);
#   2) 杀掉正在跑的本机 DS。editor 形态的 DS 是在**进程启动时**读未 cook 的 Content/,
#      已经跑着的那个进程内存里还是旧资源,不重启就看不到改动;
#   3) 重启两个 allocator。必须重启而不是只杀 DS:hub_allocator 的常驻 Hub DS 由
#      `sync.Once` 懒拉起(local_fleet.go ensureStarted),一个进程内只拉一次 —— 光杀 DS
#      不会有人再把它拉起来,表现成"登录后永远进不了大厅"。
#
# 前提是后端已经在跑。没在跑就不是「重启」而是「启动」:此时返回 $false,主流程照常走完整
# 启动 —— 双击的人不需要分清今天该点哪个图标。
function Test-LocalTcpPort([int]$Port) {
    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $conn = $client.BeginConnect('127.0.0.1', $Port, $null, $null)
        $ok = $conn.AsyncWaitHandle.WaitOne(400, $false)
        if ($ok) { $client.EndConnect($conn) }
        return $ok
    } catch {
        return $false
    } finally {
        $client.Close()
    }
}

# Get-LocalDsChildProcess:找出某个 allocator 直接拉起的那个本机 DS。
# 判据与 run_services.ps1 的 Test-IsLocalDsProcess 逐字同源(进程名 + `-server` + 关卡 URL),
# 保证绝不把策划自己手工开着的 UnrealEditor 认成 DS。
function Get-LocalDsChildProcess([int]$OwnerPid) {
    $all = @()
    try { $all = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue) } catch { return $null }
    foreach ($p in $all) {
        if ([int]$p.ParentProcessId -ne $OwnerPid) { continue }
        $name = [System.IO.Path]::GetFileNameWithoutExtension($p.Name)
        if (@('UnrealEditor', 'PandoraServer') -notcontains $name) { continue }
        $cl = $p.CommandLine
        if (-not $cl) { continue }
        if (($cl -match '(?i)(^|\s)-server(\s|$)') -and ($cl -match '(?i)\?game=/Script/Pandora\.')) { return $p }
    }
    return $null
}

# Wait-LocalHubDsReady:等常驻 Hub DS 真正能进,而不是重启完 allocator 就宣布"好了"。
#
# 为什么等得起:Hub DS **不需要有人登录**就会自己起来 —— reconcileShardTopology 挂在
# hub_allocator 的 sweep ticker 上(dev 配置 5s 一跳),每跳调一次 fleet.ListShards,
# 正好触发 local provider 的 sync.Once 懒拉起。实测:allocator 起来后约 8s DS 进程出现,
# 再约 26s 绑上端口。加载本来就在后台跑,脚本干脆等到能进为止,省得策划对着一个
# "成功"的黑窗口猜还要等多久 —— 这正是加这一步的全部意义。
#
# 就绪判据取「DS 把 NetDriver 端口绑上」:UE 专服是在 LoadMap 走完之后才 Listen,
# 端口绑上 ⇒ 关卡已经加载完。端口号**不写死也不读 yaml**,从 DS 自己的命令行 `-port=<N>`
# 上读(allocator 拼的就是它),配置改了这里不会漂。
#
# 超时 300s 与 allocator 给 editor 形态放宽的 ready_wait 同源,不另拍一个数字。
# 到期不假设成功:点名 DS 日志位置并返回失败(§16.10 —— 到期后给出出口,不是当成功往下走)。
function Wait-LocalHubDsReady {
    param([int]$SpawnTimeoutSeconds = 90, [int]$ReadyTimeoutSeconds = 300)

    $hub = @(Get-Process -Name 'hub_allocator' -ErrorAction SilentlyContinue)[0]
    if (-not $hub) {
        Write-Warn "hub_allocator 不在运行,没法等 Hub DS。"
        return $false
    }

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    Write-Info "等 Hub DS 自己起来(allocator 每 5s 对账一次分片拓扑会自动拉起,不需要有人登录)..."

    $ds = $null
    while ($sw.Elapsed.TotalSeconds -lt $SpawnTimeoutSeconds) {
        $ds = Get-LocalDsChildProcess $hub.Id
        if ($ds) { break }
        if ($hub.HasExited) {
            Write-Err "hub_allocator 自己退出了。看日志:run/dev/logs/hub_allocator.err.log"
            return $false
        }
        Start-Sleep -Seconds 1
    }
    if (-not $ds) {
        Write-Warn "${SpawnTimeoutSeconds}s 内 hub_allocator 没拉起 Hub DS。"
        Write-Info "看日志:run/dev/logs/hub_allocator.err.log(搜 local_hub_ds_start_failed)"
        return $false
    }
    Write-Ok ("Hub DS 进程已拉起(PID {0},+{1:n0}s)。" -f $ds.ProcessId, $sw.Elapsed.TotalSeconds)

    $port = 0
    if ($ds.CommandLine -match '(?i)-port=(\d+)') { $port = [int]$Matches[1] }
    if ($port -le 0) {
        Write-Warn "没能从 DS 命令行读出 -port=,跳过就绪等待(进程已起,加载完就能进)。"
        return $true
    }

    Write-Info "等它加载完关卡并监听 UDP :$port(editor 形态读未 cook 的散装资产,慢;首次进新图还要现场构 DDC,更慢)..."
    $lastTick = 0
    while ($sw.Elapsed.TotalSeconds -lt $ReadyTimeoutSeconds) {
        if (Get-NetUDPEndpoint -LocalPort $port -ErrorAction SilentlyContinue) {
            Write-Ok ("Hub DS 已监听 UDP :{0}(从重启算起 {1:n0}s)—— 现在可以进大厅了。" -f $port, $sw.Elapsed.TotalSeconds)
            return $true
        }
        # DS 中途死掉是这个模式最难查的坑(血统不匹配时 UE 想弹"模块过期"对话框,headless 弹不出来
        # 直接退)。每轮都确认它还活着,别干等到超时才说话。
        if (-not (Get-Process -Id $ds.ProcessId -ErrorAction SilentlyContinue)) {
            Write-Err "Hub DS 进程中途退出了(加载没走完)。"
            Write-Info "看它自己的日志:services/battle/hub_allocator/run/dev/logs/ds/"
            return $false
        }
        $elapsed = [int]$sw.Elapsed.TotalSeconds
        if ($elapsed - $lastTick -ge 15) {
            $lastTick = $elapsed
            Write-Info "  ... 还在加载(已等 ${elapsed}s)"
        }
        Start-Sleep -Seconds 2
    }
    Write-Warn ("等了 {0}s 仍未监听 UDP :{1};DS 进程还活着(PID {2}),多半是首次进新图在构 DDC。" -f $ReadyTimeoutSeconds, $port, $ds.ProcessId)
    Write-Info "可以直接去客户端试进大厅;卡住就看 DS 日志:services/battle/hub_allocator/run/dev/logs/ds/"
    return $false
}

function Invoke-LocalDsOnly {
    if ($Mode -ne 'local') { throw "-DsOnly 只支持 -Mode local(本机 DS 由宿主 allocator exec 拉起);当前 -Mode $Mode。" }
    if ($Down -or $Resume -or $Reset -or $BuildOnly) { throw "-DsOnly 不能与 -Down / -Resume / -Reset / -BuildOnly 同用。" }

    Write-Step "只重启本机 DS(基础设施与其余 go 服务原样不动)"

    # 端口即判据(与 run_services.ps1 的探活口径一致)。login 是启动顺序里最后一个,
    # 它在听就说明 22 个 go 服务这一批已经起完了。
    $dsSpawners = @(
        @{ Name = 'hub_allocator'; Port = 20021 }
        @{ Name = 'ds_allocator';  Port = 20020 }
    )
    $required = @(
        @{ Name = 'Redis'; Port = 6380 }
        @{ Name = 'MySQL'; Port = 3307 }
        @{ Name = 'Kafka'; Port = 9093 }
    )
    # etcd 只有 docker 路线才起:免 Docker 模式刻意不起它(dev 配置里 etcd_endpoints 全是
    # 注释状态,snowflake 走 static)。这里如果无条件要求 :2380,免 Docker 机器上这条快速
    # 通道会**永远**判定「后端没跑」而回落到完整启动 —— 恰好把「只重启 DS」这个最高频、
    # 本该最快的动作变成最慢的动作,策划每次改完资源都要重跑迁移和 22 个服务。
    if (-not $NoDocker) { $required += @{ Name = 'etcd'; Port = 2380 } }
    $required += $dsSpawners + @(
        @{ Name = 'login'; Port = 20001 }
    )

    $missing = @($required | Where-Object { -not (Test-LocalTcpPort $_.Port) })
    if ($missing.Count -gt 0) {
        Write-Info "后端还没跑起来(缺:$(($missing | ForEach-Object { "$($_.Name):$($_.Port)" }) -join ', '))。"
        Write-Info "那这次不是「重启 DS」而是「启动整套后端」,自动改走完整启动流程(会慢一些,属正常)。"
        return $false
    }
    Write-Ok "后端在跑(基础设施 + 22 个 go 服务),本次只重启 DS。"

    # DS advertise 地址必须与完整启动同源:hub_allocator / ds_allocator 重启后会重新读
    # PANDORA_DS_ADVERTISE_HOST,不重设的话它们回落到 *-dev.yaml 里的写死值,
    # 局域网客户端就会拿到回环地址去连自己。本函数只设环境变量,无任何启停副作用。
    Initialize-LocalEdgeBinding

    # 本入口天生服务于「改完资源立刻验证」,不显式传时按 editor 形态解析。
    # 重新出过服务器包、想跑打包 exe 的人显式传 -DsLauncher packaged 即可。
    if ([string]::IsNullOrWhiteSpace($DsLauncher)) {
        $script:DsLauncher = 'editor'
        Write-Info "未指定 -DsLauncher,本入口按 editor 形态解析(免出包,直接读工程里未 cook 的资源)。"
    }
    Set-LocalDsLauncherEnv

    # restart = Stop-Service(先杀该 allocator 拉起的 DS 子进程,再杀服务)
    #         + Start-Service(先清无主 DS,再起服务),所以"杀 DS"不必在这里另写一份扫描逻辑。
    # -NoBuild:go 代码没改是本入口的前提,跳过 go build 正是它比完整启动快的地方;
    #          二进制不存在时 run_services.ps1 仍会自己补编,不会静默起不来。
    Write-Warn "两个 allocator 会重启:本机正在进行的战斗 DS 会被一并终止(本地联调可接受)。"
    foreach ($svc in $dsSpawners) {
        & "$ScriptDir/run_services.ps1" -Action restart -Service $svc.Name -NoBuild
    }

    # 复核端口:run_services.ps1 的 restart 起不来时只打印一行告警、不返回非零码,
    # 淹在滚动输出里就成了"脚本说成功、客户端进不去"。这里按端口给出最终判据。
    $failed = @($dsSpawners | Where-Object { -not (Test-LocalTcpPort $_.Port) })
    if ($failed.Count -gt 0) {
        $names = ($failed | ForEach-Object { "$($_.Name):$($_.Port)" }) -join ', '
        throw "重启后端口没起来:$names。看日志:pwsh tools/scripts/run_services.ps1 -Action logs -Service <服务名>"
    }

    # 表变了才动读表的服务:它们在**进程启动时**从 configtable/dist 加载,不重启就还是旧表 ——
    # 只重启 DS 的话,策划改的关卡/道具/掉落数值在 matchmaker、player 这些服务里根本没生效。
    # (ds_allocator 也读表,但上面已经重启过,不重复。)
    # 表没变就一个都不碰:少一次无谓的断线。
    # 顺序上刻意排在 Wait-LocalHubDsReady 之前 —— Hub DS 这时正在后台加载,这几个重启白捡了重叠。
    if ($script:ConfigTableChanged) {
        $tableReaders = @(
            @{ Name = 'player';         Port = 20002 }
            @{ Name = 'battle_result';  Port = 20022 }
            @{ Name = 'matchmaker';     Port = 20011 }
            @{ Name = 'matchmaker_pve'; Port = 20018 }
        )
        Write-Host ""
        Write-Info "配置表这批有改动,顺带重启读表的 go 服务(它们只在启动时加载 dist)..."
        foreach ($svc in $tableReaders) {
            & "$ScriptDir/run_services.ps1" -Action restart -Service $svc.Name -NoBuild
        }
        $failedTables = @($tableReaders | Where-Object { -not (Test-LocalTcpPort $_.Port) })
        if ($failedTables.Count -gt 0) {
            $names = ($failedTables | ForEach-Object { "$($_.Name):$($_.Port)" }) -join ', '
            throw "读表服务重启后端口没起来:$names。多半是新表加载不过,看日志:pwsh tools/scripts/run_services.ps1 -Action logs -Service <服务名>"
        }
    }

    Write-Host ""
    # 等到真能进为止。不等的话这里就是一句"成功"+ 一个还要加载几十秒的 DS,
    # 策划只能靠反复试登录来猜什么时候好了。
    $ready = Wait-LocalHubDsReady

    Write-Host ""
    if ($ready) {
        Write-Ok "搞定。客户端重新登录进大厅,看到的就是你刚存盘的资源。"
    } else {
        $script:DsOnlyExitCode = 1
        Write-Warn "两个 allocator 已重启,但没能确认 Hub DS 就绪(原因见上)。"
        Write-Info "可以直接去客户端试;真进不去就按上面点名的日志排查。"
    }
    Write-Info "改了 go 服务代码 / 换了 run/artifacts 里的二进制 → 请改走完整启动(策划一键启动-改资源即时生效.cmd)。"
    return $true
}

# ===== docker 模式(全容器)=====
function Invoke-Docker {
    if ($Down) {
        Write-Step "停止 docker 业务服务"
        docker compose -f $ComposeServices down
        Write-Step "停止基础设施"
        & "$ScriptDir/dev_down.ps1"
        return
    }
    Write-Step "docker 模式:基础设施 + 22 个 go 服务全部容器化"

    # local 宿主进程会抢同一批端口,先停掉
    Write-Info "先停掉可能在跑的宿主 go 服务(避免端口冲突)..."
    & "$ScriptDir/run_services.ps1" -Action down 2>$null

    Write-Step "[1/3] 基础设施(建 pandora-net)"
    & "$ScriptDir/dev_up.ps1"
    if ($LASTEXITCODE -ne 0) { throw "基础设施启动失败" }

    Write-Step "[2/3] 生成集群版配置(allocator=mock:容器内无真 DS)"
    & "$ScriptDir/gen_cluster_config.ps1" -AllocatorMode mock

    Write-Step "[3/3] 构建带版本烙印的镜像并启动业务服务容器"
    # 走 Build-AllImages(带 git 版本 build-arg),再用已构建镜像编排,
    # 避免 compose --build 绕过版本烙印。镜像 tag 与 compose image: 一致。
    Build-AllImages
    docker compose -f $ComposeServices up -d
    if ($LASTEXITCODE -ne 0) { throw "业务服务容器启动失败" }

    Write-Host ""
    Write-Ok "docker 模式已启动。查看:docker compose -f deploy/docker-compose.services.yml ps"
}

# ===== intranet 模式(内网测试服:全容器,绑内网 IP 供多人联调)=====
# 与 docker 一致(基础设施 + 20 服务全容器,DS=mock),区别只是面向局域网:
#   - 导出 PANDORA_EDGE_BIND_HOST=0.0.0.0,只让 Envoy 客户端面 8443 对局域网开放
#   - DS 面 8444 未鉴权,默认仍固定在 127.0.0.1(admin 9901 也只绑本机)
#   - 内网其它机器可直接连本机内网 IP
#   - 打印内网访问地址,客户端把后端指向 <内网IP>:<port> 即可
function Resolve-LanIp {
    # 取本机对外那张网卡的 IPv4。关键:按默认路由(0.0.0.0/0)选网卡,
    # 避开 Docker/WSL/Hyper-V 虚拟网卡的 172.*/10.*/192.168.* 地址——否则内网客户端拿到不可达地址。
    $isUsable = { $_.IPAddress -notmatch '^(127\.|169\.254\.)' -and $_.PrefixOrigin -ne 'WellKnown' }
    # 1) 默认路由所在网卡 = 真正对外那张(按路由跃点 + 接口跃点升序取最优)
    $best = Get-NetRoute -DestinationPrefix '0.0.0.0/0' -AddressFamily IPv4 -ErrorAction SilentlyContinue |
        Sort-Object -Property RouteMetric, @{ Expression = { (Get-NetIPInterface -InterfaceIndex $_.InterfaceIndex -AddressFamily IPv4 -ErrorAction SilentlyContinue).InterfaceMetric } } |
        Select-Object -First 1
    if ($best) {
        $ip = Get-NetIPAddress -AddressFamily IPv4 -InterfaceIndex $best.InterfaceIndex -ErrorAction SilentlyContinue |
            Where-Object $isUsable | Select-Object -First 1 -ExpandProperty IPAddress
        if (-not [string]::IsNullOrWhiteSpace($ip)) { return $ip }
    }
    # 2) 回退:排除常见虚拟网卡后取第一个
    $virtual = 'vEthernet|WSL|Hyper-V|Docker|VirtualBox|VMware|Loopback|TAP-|VPN|tun'
    $ip = Get-NetIPAddress -AddressFamily IPv4 -ErrorAction SilentlyContinue |
        Where-Object $isUsable | Where-Object { $_.InterfaceAlias -notmatch $virtual } |
        Sort-Object -Property SkipAsSource | Select-Object -First 1 -ExpandProperty IPAddress
    if (-not [string]::IsNullOrWhiteSpace($ip)) { return $ip }
    # 3) 最后兜底:旧启发式(至少返回点东西)
    return Get-NetIPAddress -AddressFamily IPv4 -ErrorAction SilentlyContinue |
        Where-Object $isUsable | Sort-Object -Property SkipAsSource |
        Select-Object -First 1 -ExpandProperty IPAddress
}

# 设置 Envoy 边缘绑定:客户端面(8443)按 $ClientHost;DS 面(8444,未鉴权)默认恒绑本机,
# 只有显式 -ExposeDsFace 才跟随客户端面开到局域网,避免未鉴权 DS 面默认暴露给整个局域网。
function Set-EdgeBindHost([string]$ClientHost) {
    $env:PANDORA_EDGE_BIND_HOST = $ClientHost
    if ($ExposeDsFace) {
        $env:PANDORA_DS_EDGE_BIND_HOST = $ClientHost
        if ($ClientHost -eq '0.0.0.0') {
            Write-Warn "已按 -ExposeDsFace 把未鉴权的 DS 面 :8444 也开到局域网($ClientHost);仅在真有异机 DS 需回连时使用,并确保网络隔离/防火墙收敛。"
        }
    } else {
        $env:PANDORA_DS_EDGE_BIND_HOST = '127.0.0.1'
        if ($ClientHost -eq '0.0.0.0') {
            Write-Info "DS 面 :8444(未鉴权)保持只绑本机;异机 DS 需回连时加 -ExposeDsFace 显式开放。"
        }
    }
}

# ===== 局域网 IP 解析(local / k8s 共用)=====
# 两种模式都默认对局域网开放,对外地址一律取**自动解析**的本机局域网 IPv4:
# 不写死,DHCP 换了地址下次启动自动跟上。可用 -AdvertiseHost / $env:PANDORA_DS_ADVERTISE_HOST 覆盖。
function Resolve-EdgeAdvertiseHost {
    if (-not [string]::IsNullOrWhiteSpace($AdvertiseHost)) { return $AdvertiseHost.Trim() }
    if (-not [string]::IsNullOrWhiteSpace($env:PANDORA_DS_ADVERTISE_HOST)) { return $env:PANDORA_DS_ADVERTISE_HOST.Trim() }
    return Resolve-LanIp
}

# local 模式的边缘绑定决策。必须在 Assert-LocalEdgePortsFree 之前跑,
# 否则端口占用判定会按错误的绑定地址算。只设环境变量,不做任何启停副作用(-Check 也安全)。
function Initialize-LocalEdgeBinding {
    if ($LocalOnly) {
        Set-EdgeBindHost '127.0.0.1'
        Write-Info "已按 -LocalOnly 只绑回环 127.0.0.1:8443,仅本机可连。"
        return
    }
    $lan = Resolve-EdgeAdvertiseHost
    if ([string]::IsNullOrWhiteSpace($lan)) {
        Set-EdgeBindHost '127.0.0.1'
        Write-Warn "未解析到局域网 IPv4,回落只绑回环 127.0.0.1:8443(仅本机可连);可用 -AdvertiseHost 显式指定。"
        return
    }
    Set-EdgeBindHost '0.0.0.0'
    # DS(Hub/Battle)返回给客户端 NetDriver 的连接地址必须是局域网 IP —— 写 127.0.0.1 的话
    # 异机客户端拿到回环地址会去连自己。此环境变量优先级高于 *-dev.yaml 里的写死值。
    $env:PANDORA_DS_ADVERTISE_HOST = $lan
    Write-Ok "已对局域网开放:客户端面 https://${lan}:8443(证书 SAN 自动含该 IP);DS advertise = $lan"
    Write-Info "局域网其它人:把客户端后端地址改成 ${lan}:8443(需先装内网 CA);只想自己玩加 -LocalOnly。"
}

function Invoke-Intranet {
    if ($Down) { Invoke-Docker; return }

    $lan = if (-not [string]::IsNullOrWhiteSpace($AdvertiseHost)) { $AdvertiseHost } else { Resolve-LanIp }
    Write-Step "intranet 模式:内网测试服(全容器,内网 IP = $lan)"
    if ([string]::IsNullOrWhiteSpace($lan)) {
        Write-Warn "未能自动解析内网 IPv4,可用 -AdvertiseHost 显式指定。继续以 docker 全容器方式启动。"
    }

    # 内网测试服面向局域网:让 Envoy【客户端面 8443】绑 0.0.0.0(admin 9901 仍恒绑本机)。
    # DS 面 8444 未鉴权且 intranet=mock DS 根本用不到,恒绑本机(除非 -ExposeDsFace)。
    # 该环境变量会被 dev_up.ps1 的 docker compose 继承(进程环境优先级高于 --env-file)。
    Set-EdgeBindHost '0.0.0.0'

    # 复用 docker 全容器启动路径(基础设施 + 服务容器,allocator=mock)
    Invoke-Docker

    Write-Host ""
    Write-Ok "内网测试服已启动。其它机器把客户端后端指向:"
    if (-not [string]::IsNullOrWhiteSpace($lan)) {
        Write-Host "       客户端面(TLS)  https://${lan}:8443" -ForegroundColor Green
        if ($ExposeDsFace) {
            Write-Host "       DS 面          ${lan}:8444(-ExposeDsFace 已开放)" -ForegroundColor Green
        } else {
            Write-Host "       DS 面          127.0.0.1:8444(未鉴权,默认仅本机;intranet=mock DS 无需开放)" -ForegroundColor DarkGray
        }
    }
    Write-Warn "DS=mock(无真实 DS);需真实战斗/大厅 DS 请用 -Mode online(Agones)。"
}

# ===== battle 模式【已废弃 2026-07-14】(原:含战斗混合版,18 业务容器 + 宿主 allocator exec Windows DS)=====
# 决策(docs/design/decision-revisit-retire-battle-mode.md):Windows DS 只保留给 -Mode local 断点调试,
# 其他一切要真 DS 的场景一律 -Mode k8s(Agones Linux DS)。本函数仅保留 -Down 分支,
# 用于清理旧机器上遗留的 battle 栈(19 容器 + 2 宿主 allocator);启动路径一律拒绝。
function Invoke-Battle {
    if ($Down) {
        Write-Step "停止含战斗版遗留环境(宿主 allocator + 业务容器 + 基础设施)"
        & "$ScriptDir/run_services.ps1" -Action down 2>$null
        docker compose -f $ComposeServices down
        & "$ScriptDir/dev_down.ps1"
        return
    }

    Write-Err "battle 模式已废弃(2026-07-14):Windows DS 只在 -Mode local(断点调试)启动。"
    Write-Host "       要进真实 Hub/Battle DS 请用 k8s + Agones(Linux DS):" -ForegroundColor Yellow
    Write-Host "         本机联调:  pwsh tools/scripts/start.ps1 -Mode k8s" -ForegroundColor Yellow
    Write-Host "         内网服务器:双击 内网服务器一键启动-k8s集群.cmd" -ForegroundColor Yellow
    Write-Host "       清理本机遗留的 battle 环境:pwsh tools/scripts/start.ps1 -Mode battle -Down" -ForegroundColor Yellow
    Write-Host "       详见 docs/design/decision-revisit-retire-battle-mode.md" -ForegroundColor DarkGray
    exit 1
}


# ===== 共享:apply Agones(RBAC + Fleet),可选安装 Agones(minikube 本地用)=====
# 让 agones 链路端到端可用:RBAC 给 allocator in-cluster token 调 Agones API 的权限,
# Stable/Canary 四 Fleet(pandora-battle-* / pandora-hub-*)提供真实 Linux DS。namespace 须先存在(调用方保证)。
function Apply-AgonesManifests {
    param(
        [switch]$InstallAgones,
        [string]$BattleDsImage = '',
        [string]$HubDsImage = '',
        [string]$CanaryBattleDsImage = '',
        [string]$CanaryHubDsImage = '',
        [ValidateRange(0, 1000)][int]$BattleCanaryReplicaCount = 0,
        [ValidateRange(0, 1000)][int]$HubCanaryReplicaCount = 0,
        # Battle FleetAutoscaler 覆写(online 用):0/空 = 用 yaml 里的本地 dev 默认值。
        [ValidateRange(0, 1000000)][int]$BattleMaxReplicasOverride = 0,
        [ValidatePattern('^([0-9]+%?)?$')][string]$BattleBufferSizeOverride = '',
        [ValidateRange(1, 2147483647)][int]$DSTicketKeysetRevision = 1,
        [string]$DsGatewayAddr = '',
        [string]$DsGatewayTls = '',
        # 本地 minikube dev 专用。⚠️ 注意本开关的历史前提**已失效**:当年 Fleet 钉的是滚动 :dev tag,
        # 换了 DS 镜像 spec 也不变 → apply 报 unchanged → Agones 不滚动 → 旧 Pod 继续跑旧镜像,
        # 才需要靠强删 GameServer 逼它按 :dev 重建。现在四份 Fleet 已改钉**不可变 tag**
        # (INC-20260727-001 P1-4,见 deploy/k8s/agones/20-fleet-battle.yaml),换版必改 image 行 →
        # apply 即 spec 变更 → Agones 自动滚动,本开关对"换版生效"不再必要。
        # 它现在只剩一个用途:同 tag 下想强制重建 GameServer(如重灌了同名镜像内容)。
        # 线上(online)禁开(强删会中断线上战斗);本地开也要注意会把 hub 在线玩家踢去重连。
        [switch]$ForceRecreateGameServers,
        # 本地 minikube kube-context 名。ForceRecreateGameServers 删 GameServer 时,kubectl 必须
        # 显式 --context 钉在本机 minikube 上,绝不能落到用户 current-context 可能指向的远端集群。
        [string]$KubeContext = ''
    )
    $agonesDir = Join-Path $ProjectRoot 'deploy/k8s/agones'
    if ([string]::IsNullOrWhiteSpace($KubeContext)) {
        throw 'Apply-AgonesManifests 必须显式传 -KubeContext，禁止依赖可被其它终端切换的 current-context。'
    }
    $kubectlContextArgs = @('--context', $KubeContext)

    function Set-YamlEnvValue([string]$text, [string]$name, [string]$value) {
        # 用显式 ${1}/${2} 分组语法:避免 value 以数字开头时(如 TLS=1)$1+值拼成 $11 被当作第 11 组
        $pattern = '(?ms)(- name:\s*' + [regex]::Escape($name) + '\r?\n\s*value:\s*")[^"]+(")'
        return [regex]::Replace($text, $pattern, ('${1}' + $value + '${2}'))
    }

    function Set-FleetReplicaCount([string]$text, [int]$count) {
        $pattern = '(?m)^(spec:\r?\n\s{2}replicas:\s*)[0-9]+\s*$'
        if ([regex]::Matches($text, $pattern).Count -ne 1) { throw 'Fleet spec.replicas 字段数量不是 1。' }
        return [regex]::Replace($text, $pattern, ('${1}' + $count), 1)
    }

    function Apply-FleetManifest(
        [string]$fileName,
        [string]$image,
        [string]$track,
        [int]$replicaOverride,
        [string[]]$addrEnvNames,
        [string]$tlsEnvName) {
        $src = Join-Path $agonesDir $fileName
        $baseRaw = Get-Content $src -Raw
        Assert-PandoraFleetNoPlayerSigningMaterial -Manifest $baseRaw
        if ([string]::IsNullOrWhiteSpace($image) -and [string]::IsNullOrWhiteSpace($DsGatewayAddr) -and
            [string]::IsNullOrWhiteSpace($DsGatewayTls) -and $DSTicketKeysetRevision -eq 1 -and $replicaOverride -lt 0) {
            kubectl @kubectlContextArgs apply -f $src
            Assert-LastExit "kubectl apply Fleet $fileName"
            return
        }
        $raw = $baseRaw
        if (-not [string]::IsNullOrWhiteSpace($image)) {
            # online 只接受 registry digest pin；同时把 digest 写进 GameServer/Pod 两层 annotation。
            # 本地 minikube 不传 image，原文 apply yaml 里钉定的不可变 tag（不是 :dev），不触发本分支。
            $raw = Set-PandoraFleetImagePin -Manifest $raw -PinnedImage $image
        }
        if (-not [string]::IsNullOrWhiteSpace($DsGatewayAddr)) {
            foreach ($envName in $addrEnvNames) {
                $raw = Set-YamlEnvValue $raw $envName $DsGatewayAddr
            }
        }
        if (-not [string]::IsNullOrWhiteSpace($DsGatewayTls)) {
            $raw = Set-YamlEnvValue $raw $tlsEnvName $DsGatewayTls
        }
        $raw = Set-PandoraFleetDSTicketKeysetRevision -Manifest $raw -Revision $DSTicketKeysetRevision
        if ($replicaOverride -ge 0) { $raw = Set-FleetReplicaCount $raw $replicaOverride }
        if (-not [string]::IsNullOrWhiteSpace($image)) {
            $containerName = if ($fileName -match 'battle') { 'pandora-battle-ds' } elseif ($fileName -match 'hub') { 'pandora-hub-ds' } else { throw "未知 Fleet 清单:$fileName" }
            $expectedFleetName = if ($containerName -ceq 'pandora-battle-ds') { "pandora-battle-$track" } else { "pandora-hub-$track" }
            $contractRows = Get-PandoraFleetContractRows -Manifest $raw
            Assert-PandoraFleetManifestContract -Manifest $raw -ContractRows $contractRows -PinnedImage $image `
                -ContainerName $containerName -ExpectedTrack $track -ExpectedFleetName $expectedFleetName
        }

        $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ([System.IO.Path]::GetRandomFileName() + '-' + $fileName)
        [System.IO.File]::WriteAllText($tmp, $raw, (New-Object System.Text.UTF8Encoding($false)))
        try {
            kubectl @kubectlContextArgs apply -f $tmp
            Assert-LastExit "kubectl apply Fleet $fileName"
        } finally {
            Remove-Item $tmp -ErrorAction SilentlyContinue
        }
    }

    if ($InstallAgones) {
        # namespace 可能由一次失败/超时的 Helm install 提前创建，不能据此断言 Agones 已安装。
        # 只有 release 明确处于 deployed 才跳过；missing/failed 均走 upgrade --install 幂等修复。
        $agonesReleaseDeployed = $false
        $agonesStatusJson = (helm status agones --kube-context $KubeContext --namespace agones-system -o json 2>$null | Out-String)
        $agonesStatusExit = $LASTEXITCODE
        if ($agonesStatusExit -eq 0 -and -not [string]::IsNullOrWhiteSpace($agonesStatusJson)) {
            try {
                $agonesStatus = $agonesStatusJson | ConvertFrom-Json -ErrorAction Stop
                $agonesReleaseDeployed = ([string]$agonesStatus.info.status -ceq 'deployed')
            } catch {
                Write-Warn "Agones Helm release 状态无法解析，将执行 upgrade --install 修复:$($_.Exception.Message)"
            }
        }
        if (-not $agonesReleaseDeployed) {
            # 兼容历史上由 kubectl/旧安装器部署、但没有 Helm release 元数据的本地集群。
            # 不能只看 namespace；必须同时证明核心 CRD 存在且 controller 已可用，才可
            # 跳过 Helm 接管。否则 upgrade --install 会因既有 cluster-scoped 资源没有
            # Helm ownership 而失败，阻断本地恢复流程。
            $agonesCrdName = (kubectl @kubectlContextArgs get crd gameservers.agones.dev --ignore-not-found -o name 2>$null | Out-String).Trim()
            $agonesCrdReady = ($LASTEXITCODE -eq 0 -and $agonesCrdName -ceq 'customresourcedefinition.apiextensions.k8s.io/gameservers.agones.dev')
            $agonesAvailableText = (kubectl @kubectlContextArgs get deployment agones-controller -n agones-system -o jsonpath='{.status.availableReplicas}' 2>$null | Out-String).Trim()
            $agonesAvailable = 0
            $agonesControllerReady = ($LASTEXITCODE -eq 0 -and [int]::TryParse($agonesAvailableText, [ref]$agonesAvailable) -and $agonesAvailable -gt 0)
            if ($agonesCrdReady -and $agonesControllerReady) {
                $agonesReleaseDeployed = $true
                Write-Warn 'Agones 没有 Helm release 元数据，但核心 CRD 与 controller 已就绪；沿用现有本地安装。'
            }
        }
        if (-not $agonesReleaseDeployed) {
            Write-Info "安装/修复 Agones(helm,装到 agones-system)..."
            helm repo add agones https://agones.dev/chart/stable 2>$null | Out-Null
            helm repo update 2>$null | Out-Null
            # Agones chart 含 controller/allocator/extensions 等多个第三方镜像；新机器首次冷拉时
            # Helm 默认 5 分钟会先于镜像下载结束而失败。显式放宽到 30 分钟，缓存命中时不增加耗时。
            # 版本钉 1.58.0(2026-07-28):不钉的话每次重建装"当时最新",镜像(us-docker.pkg.dev,
            # 墙内不可达)与本地缓存/导出全部失配;升级 Agones 必须显式改本行并重验四 Fleet。
            helm upgrade --install agones agones/agones --version 1.58.0 --kube-context $KubeContext --namespace agones-system `
                --create-namespace --wait --timeout 30m
            if ($LASTEXITCODE -ne 0) { throw "Agones 安装/修复失败(首次冷拉最多等待 30 分钟)" }
        } else {
            Write-Ok "Agones Helm release 已部署"
        }
    }

    Write-Info "apply Agones RBAC(pandora-allocator)..."
    kubectl @kubectlContextArgs apply -f (Join-Path $agonesDir '10-rbac-allocator.yaml')
    Assert-LastExit 'kubectl apply Agones RBAC'

    # ===== 本地 minikube:部署 in-cluster Envoy「DS 面」网关(线上同款 pandora-envoy.pandora.svc:8444)=====
    # 线上真集群自带边缘 Envoy;本地 minikube 无边缘 Envoy 且 pod 解析不了 host.docker.internal，故集群内起等价 Envoy。
    # 只在本地(InstallAgones)部署;online 模式不带 -InstallAgones，用线上真 Envoy。
    if ($InstallAgones) {
        # 三级来源确保 Envoy 镜像进节点:节点已有 → 宿主 load → 联网 pull(断网友好,失败 fail-fast)。
        Ensure-EnvoyImageInMinikube -MinikubeProfile (Get-K8sManagedProfile)
        Write-Info "apply in-cluster Envoy(DS 面 :8444,上游=集群内 Service DNS)..."
        kubectl @kubectlContextArgs apply -f (Join-Path $agonesDir '16-ds-envoy.yaml')
        Assert-LastExit 'kubectl apply in-cluster Envoy'
        # ⚠️ Envoy 只在进程启动时读一次静态 envoy.yaml。仅改 ConfigMap(路由白名单 / 上游等)时,
        #    Deployment pod spec 不变 → kubectl apply 报 unchanged → 运行中的 Envoy 仍跑旧配置,
        #    挂载的 ConfigMap 卷即便刷新到盘上 Envoy 也不会热读(回应审核 P#4:ConfigMap 变更不触发重载)。
        #    故显式 rollout restart 让 Pod 重启重读新配置;首次部署时 restart 等价一次滚动,无害。
        kubectl @kubectlContextArgs rollout restart deploy/pandora-envoy -n pandora
        Assert-LastExit 'rollout restart pandora-envoy(强制重载 DS 面静态配置)'
        kubectl @kubectlContextArgs rollout status deploy/pandora-envoy -n pandora --timeout=120s
        if ($LASTEXITCODE -ne 0) { throw "pandora-envoy 未在 120s 内就绪;DS 心跳(:8444)打不通,已中止(排障:kubectl -n pandora describe deploy/pandora-envoy)。" }
    }

    Write-Info "apply Stable/Canary Fleet(pandora-battle-*/pandora-hub-* 真 Linux DS)..."
    Apply-FleetManifest '20-fleet-battle.yaml' $BattleDsImage 'stable' -1 @('PANDORA_DS_ALLOCATOR_ADDR', 'PANDORA_DS_ADMISSION_ADDR', 'PANDORA_PLAYER_LOCATOR_ADDR', 'PANDORA_BATTLE_RESULT_ADDR') 'PANDORA_DS_ALLOCATOR_TLS'
    Apply-FleetManifest '21-fleet-battle-canary.yaml' $CanaryBattleDsImage 'canary' $BattleCanaryReplicaCount @('PANDORA_DS_ALLOCATOR_ADDR', 'PANDORA_DS_ADMISSION_ADDR', 'PANDORA_PLAYER_LOCATOR_ADDR', 'PANDORA_BATTLE_RESULT_ADDR') 'PANDORA_DS_ALLOCATOR_TLS'
    Apply-FleetManifest '30-fleet-hub.yaml' $HubDsImage 'stable' -1 @('PANDORA_HUB_ALLOCATOR_ADDR', 'PANDORA_DS_ADMISSION_ADDR', 'PANDORA_PLAYER_LOCATOR_ADDR') 'PANDORA_DS_ALLOCATOR_TLS'
    Apply-FleetManifest '31-fleet-hub-canary.yaml' $CanaryHubDsImage 'canary' $HubCanaryReplicaCount @('PANDORA_HUB_ALLOCATOR_ADDR', 'PANDORA_DS_ADMISSION_ADDR', 'PANDORA_PLAYER_LOCATOR_ADDR') 'PANDORA_DS_ALLOCATOR_TLS'
    # Battle Stable 轨 FleetAutoscaler(Buffer 策略):维持常备 Ready 预热,分配走一个自动补一个,
    # 同时最大局数 = maxReplicas(见 25-fleetautoscaler-battle.yaml 注释)。Canary/Hub 不配,原因同文件注释。
    # online 可用 -BattleMaxReplicas/-BattleBufferSize 覆写(临时文件改写再 apply,仓库原文不脏)。
    Write-Info "apply Battle FleetAutoscaler(Buffer 策略,上限见 25-fleetautoscaler-battle.yaml)..."
    $autoscalerSrc = Join-Path $agonesDir '25-fleetautoscaler-battle.yaml'
    if ($BattleMaxReplicasOverride -gt 0 -or -not [string]::IsNullOrWhiteSpace($BattleBufferSizeOverride)) {
        $autoscalerRaw = Get-Content $autoscalerSrc -Raw
        if ($BattleMaxReplicasOverride -gt 0) {
            $maxPattern = '(?m)^(\s*maxReplicas:\s*)[0-9]+\s*$'
            if ([regex]::Matches($autoscalerRaw, $maxPattern).Count -ne 1) { throw 'FleetAutoscaler maxReplicas 字段数量不是 1。' }
            $autoscalerRaw = [regex]::Replace($autoscalerRaw, $maxPattern, ('${1}' + $BattleMaxReplicasOverride), 1)
        }
        if (-not [string]::IsNullOrWhiteSpace($BattleBufferSizeOverride)) {
            # 百分比必须带引号(yaml 字符串);纯整数裸写。
            $bufValue = if ($BattleBufferSizeOverride -like '*%') { '"' + $BattleBufferSizeOverride + '"' } else { $BattleBufferSizeOverride }
            $bufPattern = '(?m)^(\s*bufferSize:\s*)"?[0-9]+%?"?\s*$'
            if ([regex]::Matches($autoscalerRaw, $bufPattern).Count -ne 1) { throw 'FleetAutoscaler bufferSize 字段数量不是 1。' }
            $autoscalerRaw = [regex]::Replace($autoscalerRaw, $bufPattern, ('${1}' + $bufValue), 1)
        }
        $autoscalerTmp = Join-Path ([System.IO.Path]::GetTempPath()) ([System.IO.Path]::GetRandomFileName() + '-25-fleetautoscaler-battle.yaml')
        [System.IO.File]::WriteAllText($autoscalerTmp, $autoscalerRaw, (New-Object System.Text.UTF8Encoding($false)))
        try {
            kubectl @kubectlContextArgs apply -f $autoscalerTmp
            Assert-LastExit 'kubectl apply Battle FleetAutoscaler(override)'
        } finally {
            Remove-Item $autoscalerTmp -ErrorAction SilentlyContinue
        }
    } else {
        kubectl @kubectlContextArgs apply -f $autoscalerSrc
        Assert-LastExit 'kubectl apply Battle FleetAutoscaler'
    }
    Write-Warn "Fleet 用真 UE DS 镜像(fleet yaml 钉定不可变 tag=制品版本;宿主缺失时按制品库同版本自动重建)。"
    Write-Warn "  这些镜像由 UE 侧 Tool/Server/Agones 构建;minikube 需先 minikube image load,线上需 push 到 -Registry。"

    if ($ForceRecreateGameServers) {
        # ⚠️ 本分支的原始动机是「固定 :dev tag」场景:spec 未变 -> apply 报 unchanged -> Agones
        # 不滚动 -> 旧 DS Pod 继续跑旧镜像。Fleet 改钉不可变 tag 后,换版必改 image 行,apply 即
        # spec 变更,Agones 自行滚动,**换版不再需要本开关**。现在它只覆盖一种残留情形:tag 没变
        # 但同名镜像内容被重灌(如按制品重建覆盖了同 tag),此时 spec 相同、节点镜像已换,只能靠
        # 删 GameServer 让 GameServerSet 按 imagePullPolicy=IfNotPresent 现场重新解析。
        # Fleet/GameServer 都在 default namespace(见 20/30-fleet yaml metadata.namespace)。
        # 安全:删 GameServer 是不可逆动作,必须显式 --context 钉在本机 minikube,绝不落远端集群。
        if ([string]::IsNullOrWhiteSpace($KubeContext)) {
            throw "ForceRecreateGameServers 需要显式 -KubeContext(本机 minikube context),缺失即中止,防止误删远端集群 GameServer。"
        }
        $fleetNs = 'default'
        Write-Info "强制重建 GameServer(context=$KubeContext,让 :dev tag 的新 DS 镜像生效)..."
        # §9.21 硬线(2026-08-03 事故整改):**绝不删除 Allocated 的 GameServer**——Allocated
        # 本身就是「可能载人」的声明,强删等于把在场玩家踢下线(实锤:2026-08-03 清旧代把
        # 用户正在游戏的 battle DS 删了)。这里只删非 Allocated(Ready/Scheduled/RequestReady
        # 等)副本,GameServerSet 按当前 spec(:dev 已指向新镜像)自动补齐;Allocated 旧镜像
        # 副本以旧版本跑完当前对局后由 Agones 自然回收(§9.21 本就要求旧 Battle DS 打完退场)。
        # 若某台 **battle** Fleet 的 Allocated 实为泄漏残留(无任何权威分配记录引用),
        # ds_allocator 的孤儿对账清扫(biz/orphan_gameserver.go)会在观察阈值(默认 10m)后按
        # UID+resourceVersion 精确回收——**任何情况下都不需要、也不允许人工/脚本直接删
        # Allocated GameServer**。(该自动回收只覆盖 battle stable/canary/map 专用 Fleet;
        # hub Fleet 是常驻分片模型、不走 GameServerAllocation,原理上不会出现 Allocated,
        # 也不在清扫列举范围内。)
        # ⚠️ hub 的另一面:正因为 hub 不走 GSA,hub GS **载人时仍是 Ready**(容量在
        # hub_allocator 的 Redis 权威里判定)——本步骤删 Ready 副本时会把在线大厅玩家踢去
        # 重连(客户端恢复链自动回大厅)。这是 dev 换镜像流程的既定代价,下面对 hub Fleet
        # 打显式告警;有真人在线时避开本开关或先通知。
        foreach ($fleet in @('pandora-battle-stable', 'pandora-battle-canary', 'pandora-hub-stable', 'pandora-hub-canary')) {
            $gsListJson = (kubectl @kubectlContextArgs get gameservers -l "agones.dev/fleet=$fleet" -n $fleetNs -o json 2>$null | Out-String)
            if ($LASTEXITCODE -ne 0) {
                throw "列举 Fleet『$fleet』的 GameServer 失败(exit=$LASTEXITCODE);无法区分 Allocated/非 Allocated,已中止(绝不盲删)。请检查:kubectl --context $KubeContext get gameservers -l agones.dev/fleet=$fleet -n $fleetNs,排障后重跑。"
            }
            $gsItems = @()
            if (-not [string]::IsNullOrWhiteSpace($gsListJson)) {
                $parsed = $gsListJson | ConvertFrom-Json
                if ($null -ne $parsed.items) { $gsItems = @($parsed.items) }
            }
            $allocatedGs = @($gsItems | Where-Object { [string]$_.status.state -ceq 'Allocated' })
            $deletableGs = @($gsItems | Where-Object { [string]$_.status.state -cne 'Allocated' })
            if ($fleet -like 'pandora-hub-*' -and $deletableGs.Count -gt 0) {
                Write-Warn "Fleet『$fleet』将删除 $($deletableGs.Count) 台 Ready 副本:hub 载人时仍是 Ready(常驻分片,不走 GSA),在线大厅玩家会被踢去重连(客户端恢复链自动回大厅)。"
            }
            foreach ($gs in $deletableGs) {
                # LIST→DELETE 窗口防护(对抗审查 P1):快照时 Ready 的 GS 在轮到删除时可能已被
                # GSA 分配(Ready→Allocated 毫秒级,分配后立即签票、玩家在途)。kubectl delete
                # 表达不了 UID/resourceVersion precondition,故删除前紧贴着重查一次该 GS 的当前
                # 状态:已变 Allocated 的跳过(转入保留清单),已消失的跳过。重查只能把窗口从
                # 秒级压到毫秒级,不能归零——残余窗口由 Go 侧兜底:ds_allocator 分配失败会走
                # 既有补偿链重排,不会留下永久损伤;真正带 precondition 的精确删除只存在于
                # ds_allocator 的孤儿对账清扫(DeleteAllocatedGameServerExact)。
                $curStateRaw = (kubectl @kubectlContextArgs get gameserver $gs.metadata.name -n $fleetNs --ignore-not-found -o jsonpath='{.status.state}' 2>$null | Out-String)
                if ($LASTEXITCODE -ne 0) {
                    throw "重查 GameServer『$($gs.metadata.name)』状态失败(exit=$LASTEXITCODE);无法证明它仍非 Allocated,已中止(绝不盲删)。"
                }
                $curState = $curStateRaw.Trim()
                if ($curState -ceq 'Allocated') {
                    Write-Warn "跳过删除:GameServer『$($gs.metadata.name)』在列举后已被分配(Allocated,可能载人,§9.21)。"
                    $allocatedGs = @($allocatedGs) + @($gs)
                    continue
                }
                kubectl @kubectlContextArgs delete gameserver $gs.metadata.name -n $fleetNs --ignore-not-found
                if ($LASTEXITCODE -ne 0) {
                    # 删除失败 = 旧镜像 Pod 可能仍在跑;绝不能静默继续,否则会把「旧实例还在跑」
                    # 误判成「新镜像已部署成功」。直接 fail-fast,让调用方看到并处理。
                    throw "删除 Fleet『$fleet』的 GameServer『$($gs.metadata.name)』失败(exit=$LASTEXITCODE);旧 DS 镜像 Pod 可能仍在跑,已中止以免误判为新镜像部署成功。请检查:kubectl --context $KubeContext get gameservers -l agones.dev/fleet=$fleet -n $fleetNs,排障后重跑。"
                }
            }
            if ($allocatedGs.Count -gt 0) {
                $allocatedNames = @($allocatedGs | ForEach-Object { $_.metadata.name }) -join ', '
                Write-Warn "Fleet『$fleet』保留 $($allocatedGs.Count) 台 Allocated GameServer(可能载人,§9.21 禁止强删):$allocatedNames"
                Write-Warn "  它们将以旧镜像跑完当前对局后由 Agones 回收;新分配已落在新镜像副本上。"
                Write-Warn "  若确属泄漏残留,ds_allocator 孤儿对账清扫会在阈值后自动回收,无需(也不允许)人工删除。"
            }
        }
        Write-Ok "已删除旧的非 Allocated GameServer,Agones 将用最新 :dev 镜像重建(kubectl get gameservers 观察 Ready)。"
    }
}

# ===== 共享:宿主镜像同步进 minikube + 固定 tag 强制刷新 =====
# 背景(2026-07-07 实锤 bug):本地 dev 固定复用 :dev tag,`minikube image load` 在 minikube
# 侧已有同名 tag 时可能静默不生效(残留旧镜像),导致「宿主 docker 已是新 build,k8s Pod 却
# 还在跑旧 image」—— 例:ds-allocator 新版“等 DS ready 心跳才回地址”逻辑没生效,matchmaker
# 提前把地址发给客户端,客户端 UDP 握手 20s 超时被踢回登录。
# Docker buildx / desktop-linux 下宿主 .Id 可能是 manifest-list digest,而 minikube 节点内
# 看到的是镜像 config digest,二者不能直接相等比较。当前策略:先强制解绑旧 tag,再从宿主
# docker daemon 覆盖 load,最后确认 tag 存在;随后 rollout restart 让新 Pod 重新解析该 tag。

# ===== 共享:minikube profile / kube-context 解析 + 本地集群上下文锁 =====
# 本地 k8s 模式所有 kubectl 操作(尤其是删 GameServer 这类不可逆动作)必须锁定在本机 minikube 的
# kube-context 上,绝不能落到用户当前 kubectl current-context 指向的远端/生产集群。
# minikube 的 kube-context 名 == 其 profile 名(minikube start 会写入同名 context)。

# 返回当前 active minikube profile 名(解析失败回落 'minikube')。
# PANDORA_MINIKUBE_PROFILE 显式覆盖优先:多 profile 并存(旧 docker-driver 集群与新 hyperv/containerd
# 集群共存期)时,active profile 是全局可变状态,靠它路由会把镜像 load/delete 打到另一个集群
# (2026-07-07 有镜像落错 daemon 的实锤前科)。env 覆盖 + 下游 context 锁(Test-KubeContextIsLocalMinikube)
# 双保险;env 未设时行为与旧版完全一致。
# Pandora 本地 k8s 集群的规范 profile 名。仓库其它脚本(e2e_k8s.ps1 / udp_relay.ps1 /
# deploy/ds/build-image-minikube.ps1 / reset_data_service_schema.ps1)早就以它为默认值,
# start.ps1 是唯一的例外 —— 统一到这里。
$script:K8sCanonicalProfile = 'pandora-agones'

# 解析「本机 Pandora 集群」的 minikube profile。
# 绝不再用 `minikube profile`:那读的是 minikube 的**全局 active profile**,它是本机全局可变
# 状态,且在没人显式 `minikube profile <名>` 过的机器上恒为字面量 'minikube' —— 与本项目集群
# 毫无关系。据它去 delete/apply,就会对着一个不存在或早已停掉的集群狂报 connection refused,
# 「一键停止」在几乎每台机器上都必炸(本机实测:active=minikube 已停,真集群是 pandora-agones)。
# 改为按本机**实际证据**推断,顺序固定、可复现:
function Get-ActiveMinikubeProfile {
    $override = $env:PANDORA_MINIKUBE_PROFILE
    if (-not [string]::IsNullOrWhiteSpace($override)) { return $override.Trim() }

    # ① 正占着宿主 8443 的 profile = 正在跑的 Pandora 集群,证据最硬(与 local 互斥判定同源)。
    $holding = Find-MinikubeProfileHoldingEdgePort
    if (-not [string]::IsNullOrWhiteSpace($holding)) { return $holding }

    $profiles = @(Get-MinikubeProfileNames)
    # ② 规范名已存在(集群停着时 ① 探不到,走这条)。
    if ($profiles -ccontains $script:K8sCanonicalProfile) { return $script:K8sCanonicalProfile }
    # ③ 一个 profile 都没有:全新机器,按规范名创建。
    if ($profiles.Count -eq 0) { return $script:K8sCanonicalProfile }
    # ④ 本机只有一个 profile:它就是唯一可能的目标(兼容早年被建成 'minikube' 的机器)。
    if ($profiles.Count -eq 1) { return $profiles[0] }
    # ⑤ 多个 profile 且没有规范名:无从判断,交给人显式指定,绝不乱删乱建。
    throw ("本机存在多个 minikube profile($($profiles -join ', ')),且没有本项目规范名" +
        "『$($script:K8sCanonicalProfile)』,无法判定哪个是 Pandora 集群。请显式指定后重跑:" +
        "`$env:PANDORA_MINIKUBE_PROFILE='<profile 名>'")
}

# 本次运行内固定的 minikube profile(解析一次后缓存)。
# 所有 minikube start / status / delete / image load 都钉在同一个 profile 上,杜绝
# 「Reset 删了 profile A,却用默认 'minikube' 重建成 B」的 profile 漂移 —— delete 目标与
# rebuild 目标必须始终一致(否则重置后跑的是另一个集群,旧集群还残留占资源)。
$script:K8sManagedProfileResolved = $false
$script:K8sManagedProfile = $null
function Get-K8sManagedProfile {
    if (-not $script:K8sManagedProfileResolved) {
        $script:K8sManagedProfile = Get-ActiveMinikubeProfile
        $script:K8sManagedProfileResolved = $true
    }
    return $script:K8sManagedProfile
}

# 本机可给 minikube 节点的内存上限(MB)。docker driver 下真正的天花板是 Docker Desktop
# 的 Linux VM(WSL2)内存,不是物理内存,故优先读 `docker info` 的 MemTotal;读不到再退物理内存。
function Get-HostMinikubeMemoryCeilingMB {
    $bytes = 0
    $raw = (docker info --format '{{.MemTotal}}' 2>$null | Out-String).Trim()
    if ($raw -match '^\d+$') { $bytes = [int64]$raw }
    if ($bytes -le 0) {
        try { $bytes = [int64](Get-CimInstance Win32_ComputerSystem -ErrorAction Stop).TotalPhysicalMemory } catch { $bytes = 0 }
    }
    if ($bytes -le 0) { return 0 }
    return [int][math]::Floor($bytes / 1MB)
}

# minikube start 的拓扑参数集(driver/runtime/cni/版本/规格)。
# 关键语义:已存在的 profile 返回空参数——minikube 对既有集群忽略 cpus/memory 变更、
# 对冲突的 --driver/--container-runtime 直接报错,传了只有害无益;拓扑只在「首次创建」时生效。
#
# 默认值 = 2026-07-28 实测通过的「贴近生产」形态,任何机器首次创建都拿到同一套拓扑
# (换机器/换人跑一键启动即复现,不依赖谁记得导环境变量):
#   containerd(线上 CRI,非 dockershim) + calico(可强制 NetworkPolicy) + 钉定 K8s 版本
#   + 阿里云 kicbase(墙内可达;境外机器同样可拉) + 8443→NodePort 31443 端口发布
#   (集群内边缘 Envoy 的客户端入口;docker/podman driver 专属参数)。
# 规格默认随宿主自适应并 fail-fast:battle DS 单副本 limits=14Gi,节点内存不足时 DS 永远
# 调度不上,只会在 [8/8] 等 Fleet Ready 处超时——与其让人等半小时再排查,不如现在讲清楚。
# 逐项可用环境变量覆盖(设了就按你的来,脚本不二次判断):
#   PANDORA_MINIKUBE_DRIVER / CPUS / MEMORY / RUNTIME / CNI / K8S_VERSION
#   PANDORA_MINIKUBE_BASE_IMAGE / IMAGE_REPOSITORY / PORTS
function Get-MinikubeStartArgs([string]$Profile) {
    if (Test-MinikubeProfileExists -Profile $Profile) { return @() }
    $driver = if ([string]::IsNullOrWhiteSpace($env:PANDORA_MINIKUBE_DRIVER)) { 'docker' } else { $env:PANDORA_MINIKUBE_DRIVER.Trim() }

    # 内存:显式覆盖优先;否则取 min(宿主上限 × 0.85, 40960),并按 DS 规格做下限硬校验。
    if (-not [string]::IsNullOrWhiteSpace($env:PANDORA_MINIKUBE_MEMORY)) {
        $memory = $env:PANDORA_MINIKUBE_MEMORY.Trim()
    } else {
        $ceilingMB = Get-HostMinikubeMemoryCeilingMB
        if ($ceilingMB -le 0) {
            throw '无法探测本机可用内存(docker info / Win32_ComputerSystem 都读不到);请显式设置 PANDORA_MINIKUBE_MEMORY 后重试。'
        }
        $memoryMB = [math]::Min([int][math]::Floor($ceilingMB * 0.85), 40960)
        # 下限依据:battle DS limits=requests=14Gi(INC-20260727-002 量测围栏)+ 基础设施
        # (mysql/redis/kafka/zk/etcd/TiDB 三件套/监控)约 6~8Gi + 22 个业务服务约 2Gi。
        # 注意失败机理是 cgroup 层而非调度层:docker driver 下 kubelet 容量取自 VM /proc/meminfo
        # (审计实测 47Gi),不读 --memory 的 cgroup 限额——所以不会 Pending,而是节点容器被硬钉
        # 在小内存后 kubelet/etcd/kafka/DS 被内核 OOM killer 随机杀成 CrashLoop。低于 16Gi
        # 真 DS 闭环必失败,只是死法难看,故前置拦截。
        if ($memoryMB -lt 16384) {
            throw ("本机可给 minikube 的内存上限约 ${ceilingMB}MB,按 85% 只能分到 ${memoryMB}MB,不足以跑真 DS 闭环" +
                "(battle DS 单副本 limits 就是 14Gi)。处理方式:①Windows 上调大 WSL2 上限(%USERPROFILE%\.wslconfig 的 memory=) 后重启 Docker;" +
                "②换内存更大的机器;③确认只跑 Go 服务不跑真 DS 时,显式设 PANDORA_MINIKUBE_MEMORY 绕过本校验(DS Fleet 会一直 Pending)。")
        }
        $memory = "$memoryMB"
    }

    # CPU:显式覆盖优先;否则取 min(逻辑核数, 16),下限 4。
    if (-not [string]::IsNullOrWhiteSpace($env:PANDORA_MINIKUBE_CPUS)) {
        $cpus = $env:PANDORA_MINIKUBE_CPUS.Trim()
    } else {
        $hostCores = 0
        try { $hostCores = [int](Get-CimInstance Win32_ComputerSystem -ErrorAction Stop).NumberOfLogicalProcessors } catch { $hostCores = 0 }
        if ($hostCores -le 0) { $hostCores = 4 }
        $cpus = "$([math]::Max(4, [math]::Min($hostCores, 16)))"
    }

    $startArgs = @("--driver=$driver", "--cpus=$cpus", "--memory=$memory")
    $runtime = if ([string]::IsNullOrWhiteSpace($env:PANDORA_MINIKUBE_RUNTIME)) { 'containerd' } else { $env:PANDORA_MINIKUBE_RUNTIME.Trim() }
    $startArgs += "--container-runtime=$runtime"
    $cni = if ([string]::IsNullOrWhiteSpace($env:PANDORA_MINIKUBE_CNI)) { 'calico' } else { $env:PANDORA_MINIKUBE_CNI.Trim() }
    $startArgs += "--cni=$cni"
    $k8sVersion = if ([string]::IsNullOrWhiteSpace($env:PANDORA_MINIKUBE_K8S_VERSION)) { 'v1.35.1' } else { $env:PANDORA_MINIKUBE_K8S_VERSION.Trim() }
    $startArgs += "--kubernetes-version=$k8sVersion"
    # PANDORA_MINIKUBE_IMAGE_REPOSITORY:k8s 核心镜像仓库覆盖(墙内 registry.k8s.io 不可达时
    # 用 registry.cn-hangzhou.aliyuncs.com/google_containers;preload 可达时通常无需设置)。
    if (-not [string]::IsNullOrWhiteSpace($env:PANDORA_MINIKUBE_IMAGE_REPOSITORY)) {
        $startArgs += "--image-repository=$($env:PANDORA_MINIKUBE_IMAGE_REPOSITORY.Trim())"
    }
    # 节点底图:默认用阿里云 kicbase 镜像(墙内可达,境外机器同样能拉);gcr.io 在墙内不可达,
    # 走默认会在 "Pulling base image" 卡住直到超时。PANDORA_MINIKUBE_BASE_IMAGE 可覆盖。
    $baseImage = if ([string]::IsNullOrWhiteSpace($env:PANDORA_MINIKUBE_BASE_IMAGE)) {
        'registry.cn-hangzhou.aliyuncs.com/google_containers/kicbase:v0.0.50'
    } else { $env:PANDORA_MINIKUBE_BASE_IMAGE.Trim() }
    $startArgs += "--base-image=$baseImage"
    # PANDORA_MINIKUBE_IMAGE_REPOSITORY:k8s 核心镜像仓库覆盖。默认不设——containerd 变体
    # 有 preload 时不需要;preload 拉不到且 registry.k8s.io 不可达时设成
    # registry.cn-hangzhou.aliyuncs.com/google_containers 重建(2026-07-28 实测该源有 v1.35.1 全套)。
    if (-not [string]::IsNullOrWhiteSpace($env:PANDORA_MINIKUBE_IMAGE_REPOSITORY)) {
        $startArgs += "--image-repository=$($env:PANDORA_MINIKUBE_IMAGE_REPOSITORY.Trim())"
    }
    # 端口发布:把宿主 8443 发布到节点 NodePort 31443 —— 集群内边缘 Envoy(pandora-edge-envoy)
    # 的客户端入口,不再需要 22 条 kubectl port-forward 桥。
    # **docker/podman driver 专属参数**,别的 driver(hyperv/vmware 等)传了会直接报错:那些
    # driver 的节点有可路由 IP,客户端直连 <节点IP>:31443 即可,无需发布。
    # 默认绑 0.0.0.0(本机 + 局域网都能连),与 local 模式的默认对局域网开放保持一致;
    # 两种模式都占宿主 8443,故互为互斥(Invoke-K8s 会先停 local,Invoke-Local 会先停集群)。
    # PANDORA_MINIKUBE_PORTS 可显式覆盖(如只想本机可连就设 "127.0.0.1:8443:31443");
    # 既有集群无法追加/修改发布端口,改了必须 -Reset 重建才生效。
    $portSpecs = if ([string]::IsNullOrWhiteSpace($env:PANDORA_MINIKUBE_PORTS)) {
        if ($driver -in @('docker', 'podman')) { @('0.0.0.0:8443:31443') } else { @() }
    } else { @($env:PANDORA_MINIKUBE_PORTS -split ',') }
    foreach ($portSpec in $portSpecs) {
        if (-not [string]::IsNullOrWhiteSpace($portSpec)) { $startArgs += "--ports=$($portSpec.Trim())" }
    }
    return $startArgs
}

# 读取 profile 的节点容器运行时(docker/containerd/cri-o),缓存;解析失败回落 'docker'(旧行为)。
# 用途:节点内镜像操作(untag/pull/探测)在 docker 与 containerd 下命令不同,必须按 runtime 分流,
# 不能把 `minikube ssh -- docker ...` 打进没有 docker CLI 的 containerd 节点(静默失败或硬报错)。
$script:MinikubeRuntimeCache = @{}
function Get-MinikubeNodeRuntime([string]$Profile) {
    if ($script:MinikubeRuntimeCache.ContainsKey($Profile)) { return $script:MinikubeRuntimeCache[$Profile] }
    $runtime = 'docker'
    $parsed = Get-MinikubeProfileListObject
    if ($null -ne $parsed) {
        foreach ($p in @($parsed.valid)) {
            if ([string]$p.Name -ceq $Profile) {
                $cr = [string]$p.Config.KubernetesConfig.ContainerRuntime
                if (-not [string]::IsNullOrWhiteSpace($cr)) { $runtime = $cr.Trim() }
                break
            }
        }
    }
    $script:MinikubeRuntimeCache[$Profile] = $runtime
    return $runtime
}

# 校验 active minikube profile 对应的 kube-context 确实存在于 kubeconfig,返回该 context 名。
# 用于把本地 k8s 操作显式钉在 minikube 上(kubectl --context <ctx>),不依赖易被用户切走的
# current-context。context 不存在(minikube 没起/被删)时 fail-fast,绝不静默落到别的集群。
function Resolve-MinikubeKubeContext {
    $mkProfile = Get-K8sManagedProfile
    $contexts = @(kubectl config get-contexts -o name 2>$null)
    if ($LASTEXITCODE -ne 0) { throw "kubectl 不可用或 kubeconfig 读取失败,无法解析 minikube kube-context。" }
    if ($contexts -cnotcontains $mkProfile) {
        throw "kubeconfig 中找不到 minikube 的 kube-context『$mkProfile』(minikube 未启动或未创建集群?)。为防误操作远端集群,已中止。"
    }
    return $mkProfile
}

# 校验指定 kube-context 的 apiserver endpoint 确实指向本机 minikube(而不是同名的远端/生产集群)。
# 只比对 context 名不够安全:别人 kubeconfig 里完全可能存在同名 context 却指向线上集群,
# 一旦对它执行 delete 就是灾难。这里取该 context 关联 cluster 的 server URL,主机名必须是
# 回环(127.0.0.1/localhost/::1)或 minikube 节点 IP 才算本地;否则 fail-fast。
# 返回 $true 表示确认本地;不可解析或非本地一律返回 $false(调用方决定 throw/skip)。
function Test-KubeContextIsLocalMinikube([string]$Context) {
    if ([string]::IsNullOrWhiteSpace($Context)) { return $false }
    $cluster = (kubectl config view -o jsonpath="{.contexts[?(@.name==`"$Context`")].context.cluster}" 2>$null)
    if ([string]::IsNullOrWhiteSpace($cluster)) { return $false }
    $server = (kubectl config view -o jsonpath="{.clusters[?(@.name==`"$cluster`")].cluster.server}" 2>$null)
    if ([string]::IsNullOrWhiteSpace($server)) { return $false }
    $apiHost = $null
    try { $apiHost = ([System.Uri]$server).Host } catch { return $false }
    if ([string]::IsNullOrWhiteSpace($apiHost)) { return $false }
    # 回环地址 = 明确本机(minikube docker driver 常见 https://127.0.0.1:<port>)。
    if ($apiHost -in @('127.0.0.1', 'localhost', '::1', '[::1]')) { return $true }
    # 否则必须等于 minikube 节点 IP(docker/kvm driver 下的 192.168.x.x)。
    $mkIp = (& minikube -p $Context ip 2>$null)
    if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($mkIp) -and $apiHost -eq $mkIp.Trim()) { return $true }
    return $false
}

# 读 minikube 内镜像清单,返回 hashtable:镜像名(去 docker.io/ 前缀) -> 镜像 ID(sha256:config digest)。
# 该 ID 与宿主 `docker image inspect -f '{{.Id}}'` 同为镜像 config digest,可直接比对。
# minikube 版本过旧不支持 --format json 时返回 $null(调用方降级为告警)。
function Get-MinikubeImageIds([string[]]$MinikubeArgs = @()) {
    $json = (minikube @MinikubeArgs image ls --format json 2>$null | Out-String)
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($json)) { return $null }
    try { $list = $json | ConvertFrom-Json } catch { return $null }
    $map = @{}
    foreach ($entry in $list) {
        $id = [string]$entry.id
        # image ls --format json 的 id 是裸 64 位 hex(docker runtime 实测 2026-07-28,与
        # docker image inspect .Id 去前缀后逐位一致);统一补 sha256: 前缀对齐 inspect /
        # kubelet imageID 口径,已带前缀的形态原样保留。
        if ($id -cmatch '^[0-9a-f]{64}$') { $id = "sha256:$id" }
        foreach ($tag in @($entry.repoTags)) {
            if ([string]::IsNullOrWhiteSpace($tag)) { continue }
            $name = $tag -replace '^docker\.io/', ''
            $map[$name] = $id
        }
    }
    return $map
}

# 把宿主 deploy/envoy/envoy.yaml 机械变换为「集群内客户端面边缘 Envoy」配置(2026-07-28 P2)。
# 单一事实源:路由/JWT/超时等 1100 行配置只维护宿主一份,进集群走本变换,防两份配置漂移。
# 变换规则(其余逐字保留):
#   ① 剥掉 pandora_ds_listener(8444 DS 面):集群内已有加固版 pandora-envoy(16-ds-envoy.yaml,
#      带 x-pandora-ds-gateway 标记 + 方法白名单),边缘 Pod 不再暴露一个未加固副本;
#      被它独占引用的 cluster 留着不被引用,Envoy 允许未引用的静态 cluster,无副作用。
#   ② admin 0.0.0.0→127.0.0.1(与 DS 面同一收紧理由:admin 未鉴权,不得暴露 Pod IP 上)。
#   ③ 上游 address: host.docker.internal → 同 namespace Service 短名,按同一 socket_address
#      块里的 port_value 映射(端口=服务一一对应,见 infra.md §6);任何未映射端口 fail-fast。
function Convert-EdgeEnvoyConfigForCluster([string]$Text, [string]$Namespace = 'pandora') {
    $portSvc = @{
        20001 = 'login'; 20002 = 'player'; 20003 = 'data-service'; 20004 = 'friend'; 20005 = 'chat'
        20006 = 'player-locator'; 20007 = 'leaderboard'; 20008 = 'guild'; 20009 = 'mail'; 20010 = 'team'
        20011 = 'matchmaker'; 20012 = 'trade'; 20013 = 'dialogue'; 20014 = 'push'; 20015 = 'inventory'
        20016 = 'auction'; 20017 = 'owner'; 20018 = 'matchmaker-pve'; 20019 = 'mission'
        20020 = 'ds-allocator'; 20021 = 'hub-allocator'; 20022 = 'battle-result'
    }
    $lines = $Text -split "`r?`n"
    $out = New-Object 'System.Collections.Generic.List[string]'
    $inAdmin = $false; $skipDsListener = $false; $pendingAddrIdx = -1; $addrSeen = 0; $replaced = 0
    foreach ($line in $lines) {
        if ($line -cmatch '^admin:') { $inAdmin = $true }
        elseif ($line -cmatch '^static_resources:') { $inAdmin = $false }
        if ($line -cmatch '^\s*-\s+name:\s+pandora_ds_listener\s*$') { $skipDsListener = $true; continue }
        if ($skipDsListener) {
            if ($line -cmatch '^  clusters:') { $skipDsListener = $false } else { continue }
        }
        $emit = $line
        if ($inAdmin -and $line -cmatch '^(\s*address:\s*)0\.0\.0\.0\s*$') { $emit = "$($Matches[1])127.0.0.1" }
        if ($line -cmatch '^(\s*)address:\s*host\.docker\.internal\s*$') {
            $addrSeen++
            if ($pendingAddrIdx -ge 0) { throw 'edge envoy 变换:连续两个 host.docker.internal 地址之间没有 port_value,配置结构与预期不符,拒绝生成。' }
            $pendingAddrIdx = $out.Count
        }
        if ($pendingAddrIdx -ge 0 -and $line -cmatch '^\s*port_value:\s*(\d+)\s*$') {
            $upPort = [int]$Matches[1]
            if (-not $portSvc.ContainsKey($upPort)) { throw "edge envoy 变换:上游端口 $upPort 无 Service 映射,请补 Convert-EdgeEnvoyConfigForCluster 的 portSvc 表(infra.md §6)。" }
            $indent = [regex]::Match($out[$pendingAddrIdx], '^\s*').Value
            # 必须写**全限定名**,不能只写短名(login / team / …)。
            #
            # 短名要靠 Pod 的 DNS 搜索域补全;搜索域一旦没生效,解析会落到宿主 DNS,而
            # Docker Desktop 的 DNS 对任何未知名字都返回自己的网关 192.168.65.254 ——
            # 于是 18 个上游"解析成功"却全部指向一个没有这些服务的地址,Envoy 连不上,
            # 客户端拿到 503 且**看不出是 DNS 问题**(clusters 里明明有地址)。线上实测过一次:
            #     login_cluster::192.168.65.254:20001  cx_connect_fail=19  rq_error=20
            # 集群内的 DS 版网关(16-ds-envoy.yaml)一直写全限定名,从未出过这个故障。
            $out[$pendingAddrIdx] = "${indent}address: $($portSvc[$upPort]).$Namespace.svc.cluster.local"
            $pendingAddrIdx = -1; $replaced++
        }
        $out.Add($emit)
    }
    if ($skipDsListener) { throw 'edge envoy 变换:pandora_ds_listener 块直到文件尾都没遇到 clusters:,剥除失败,拒绝生成。' }
    if ($pendingAddrIdx -ge 0) { throw 'edge envoy 变换:最后一个 host.docker.internal 地址没有配套 port_value,拒绝生成。' }
    if ($addrSeen -eq 0 -or $replaced -ne $addrSeen) { throw "edge envoy 变换:上游地址改写数($replaced)与发现数($addrSeen)不一致或为零,输入不是预期的宿主 envoy 配置。" }
    return ($out -join "`n")
}

# 把一组宿主镜像 load 进 minikube。
# Docker buildx/desktop-linux 下宿主 .Id 可能是 manifest-list digest,而 minikube docker daemon 里
# 看到的是镜像 config digest,二者不能直接相等比较。这里用「强制解绑旧 tag + 从宿主 daemon 覆盖 load」
# 保证 :dev tag 指向最新构建;随后 rollout restart 会让新 Pod 重新解析该 tag。
function Sync-ImagesToMinikube {
    param(
        [string[]]$Images,
        [string[]]$MinikubeArgs = @()
    )

    # 1) 宿主没有该镜像 → 直接 fail-fast
    foreach ($img in $Images) {
        $id = (docker image inspect -f '{{.Id}}' $img 2>$null | Out-String).Trim()
        if ($LASTEXITCODE -ne 0 -or -not $id) { throw "宿主 docker 缺少镜像 $img(先构建再部署)" }
    }

    # 2) 逐个强制刷新同名 tag。旧 Pod 正在用旧镜像时,minikube image rm 会失败;节点内 untag
    # (docker rmi -f / crictl rmi 按 tag)只解绑 tag,不影响运行中容器,再 load 最新 tag。
    # untag 命令必须按节点 runtime 分流:containerd 节点没有 docker CLI,旧写法会静默失败,
    # 复现「宿主新 build、集群跑旧镜像」的 2026-07-07 事故;--overwrite 是否单独足够未实测,untag 保留。
    $syncProfileIdx = [array]::IndexOf($MinikubeArgs, '-p')
    $syncProfile = if ($syncProfileIdx -ge 0 -and $syncProfileIdx + 1 -lt $MinikubeArgs.Count) { $MinikubeArgs[$syncProfileIdx + 1] } else { Get-K8sManagedProfile }
    $syncRuntime = Get-MinikubeNodeRuntime $syncProfile
    foreach ($img in $Images) {
        Write-Info "  minikube image load --daemon=true --overwrite=true $img(runtime=$syncRuntime)"
        if ($syncRuntime -ceq 'docker') {
            minikube @MinikubeArgs ssh -- docker rmi -f $img 2>$null | Out-Null
        } else {
            # containerd/cri-o:crictl rmi 按 tag 即 untag 语义;镜像被运行容器引用时按 tag 解绑同样安全。
            minikube @MinikubeArgs ssh -- sudo crictl rmi "docker.io/$img" 2>$null | Out-Null
        }
        minikube @MinikubeArgs image load --daemon=true --overwrite=true $img
        Assert-LastExit "minikube image load $img"
    }

    # 3) 校验 minikube 内 tag 存在。新 Pod 是否换新镜像由后续 rollout restart + rollout status 兜住。
    $mkIds = Get-MinikubeImageIds $MinikubeArgs
    if ($null -eq $mkIds) {
        Write-Warn "minikube image ls --format json 不可用,无法校验 tag(建议升级 minikube)。"
        return
    }
    $missing = @($Images | Where-Object { -not $mkIds.ContainsKey($_) })
    if ($missing.Count -gt 0) {
        throw "以下镜像 load 后 minikube 内仍未找到 tag:$($missing -join ', ')"
    }
    Write-Ok "镜像 tag 已刷新到 minikube($($Images.Count) 个)。"
}

# DS-auth capability 的 image_digest 必须来自 minikube 节点实际的 immutable 镜像身份,
# 不能沿用旧 Deployment annotation,也不能使用宿主 buildx manifest-list digest。
# 取值口径(2026-07-28 改):统一走 `minikube image ls --format json` 的 CRI 视角 id——
# kubelet 的 containerStatuses.imageID 与 CRI image store 出自同一来源,该口径在 docker
# 与 containerd 节点上都与 Pod 实际上报一致(docker 下即原 config digest,行为不变);
# 旧写法 `ssh -- docker image inspect` 在 containerd 节点无 docker CLI 直接硬失败。
function Get-LocalMinikubeImageDigest {
    param(
        [Parameter(Mandatory = $true)][string]$MinikubeProfile,
        [Parameter(Mandatory = $true)][string]$Image
    )
    $mkIds = Get-MinikubeImageIds @('-p', $MinikubeProfile)
    if ($null -eq $mkIds) {
        throw "minikube image ls --format json 不可用,无法读取节点镜像 digest:$Image(升级 minikube 后重试)"
    }
    if (-not $mkIds.ContainsKey($Image)) {
        throw "minikube 节点镜像清单中找不到 $Image(先 load 再部署)"
    }
    $digest = ([string]$mkIds[$Image]).Trim()
    if ($digest -cnotmatch '^sha256:[0-9a-f]{64}$') {
        throw "minikube 节点镜像 digest 非 canonical sha256:$Image(实际值:$digest)"
    }
    return $digest
}

# writer Deployment 实际引用的容器镜像(digest provenance 的唯一合法来源)。
function Get-LocalDsAuthWriterImage {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$Writer
    )
    $img = [string](kubectl --context $KubeContext -n $K8sNamespace get "deployment/$Writer" `
        -o jsonpath='{.spec.template.spec.containers[0].image}' 2>$null)
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($img)) { throw "读取 Deployment/$Writer 容器镜像失败。" }
    return $img
}

function Set-LocalDsAuthImageDigestAnnotations {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$MinikubeProfile
    )
    # 修改 template 会触发 rollout;Hub ledger 是单写者。两种合法形态(2026-07-28 更新,
    # 本断言写于 R11 之前,全量流程自 R11 后首跑暴露其过时):
    #   ① 旧形态:replicas=1 + Recreate 无 rollingUpdate(部署策略保证同刻单进程);
    #   ② R11 P0-5(2026-07-25)后:RollingUpdate + pandora.dev/deploy-strategy 注解逐字一致
    #      (main_test.go 清单契约)。单写者由运行时 writerlease(选举+单调 fencing+存储层
    #      同事务校验)保证,进程读注解 fail-closed:RollingUpdate × writer_lease_mode!=enforce
    #      直接拒启,不存在无保护双写窗口——单写者约束不得靠部署策略实现(宪法金丝雀条款)。
    $hubLines = @(& kubectl --context $KubeContext -n $K8sNamespace get deployment/hub-allocator -o json 2>&1)
    if ($LASTEXITCODE -ne 0) { throw '读取 Deployment/hub-allocator strategy 失败。' }
    try { $hubDeployment = (($hubLines -join "`n") | ConvertFrom-Json -ErrorAction Stop) }
    catch { throw "Deployment/hub-allocator JSON 非法:$($_.Exception.Message)" }
    $rolling = $hubDeployment.spec.strategy.PSObject.Properties['rollingUpdate']
    $hubStrategyType = [string]$hubDeployment.spec.strategy.type
    $hubDeclaredStrategy = [string]$hubDeployment.spec.template.metadata.annotations.'pandora.dev/deploy-strategy'
    $hubRecreateOk = ([int]$hubDeployment.spec.replicas -eq 1 -and $hubStrategyType -ceq 'Recreate' -and
        ($null -eq $rolling -or $null -eq $rolling.Value))
    $hubRollingOk = ([int]$hubDeployment.spec.replicas -eq 1 -and $hubStrategyType -ceq 'RollingUpdate' -and
        $hubDeclaredStrategy -ceq 'RollingUpdate')
    if (-not ($hubRecreateOk -or $hubRollingOk)) {
        throw "Hub digest rollout 前必须为 replicas=1 且『Recreate 无 rollingUpdate』或『RollingUpdate 且 pandora.dev/deploy-strategy 注解逐字一致(writerlease 运行时单写者,R11 P0-5)』;实际 strategy=$hubStrategyType 注解=$hubDeclaredStrategy replicas=$($hubDeployment.spec.replicas)。"
    }
    $writers = @('login', 'player-locator', 'ds-allocator', 'hub-allocator', 'battle-result')
    foreach ($writer in $writers) {
        # 2026-07-28:writer 可能钉定不可变 tag(INC-20260727-001,如 ds-allocator);provenance
        # 必须描述 Deployment 实际引用的镜像,不得假定 :dev(否则注解写 :dev digest、Pod 跑
        # 钉定镜像,Assert 如实拦截)。
        $image = Get-LocalDsAuthWriterImage -KubeContext $KubeContext -Writer $writer
        $digest = Get-LocalMinikubeImageDigest -MinikubeProfile $MinikubeProfile -Image $image
        $patch = @{
            spec = @{ template = @{ metadata = @{ annotations = @{
                'pandora.dev/image-digest' = $digest
            } } } }
        } | ConvertTo-Json -Depth 8 -Compress
        kubectl --context $KubeContext -n $K8sNamespace patch "deployment/$writer" --type merge -p $patch | Out-Null
        Assert-LastExit "patch $writer minikube immutable image digest"
        $readback = [string](kubectl --context $KubeContext -n $K8sNamespace get "deployment/$writer" `
            -o jsonpath='{.spec.template.metadata.annotations.pandora\.dev/image-digest}')
        Assert-LastExit "readback $writer image digest annotation"
        if ($readback -cne $digest) {
            throw "Deployment/$writer image digest annotation 回读不一致。"
        }
    }
    Write-Ok '五个 DS-auth writer 已绑定 minikube 节点实际 immutable image digest。'
}

function Assert-LocalDsAuthImageDigestAnnotations {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$MinikubeProfile,
        [switch]$SkipPodCheck
    )
    $writers = @('login', 'player-locator', 'ds-allocator', 'hub-allocator', 'battle-result')
    foreach ($writer in $writers) {
        $deploymentLines = @(& kubectl --context $KubeContext -n $K8sNamespace get "deployment/$writer" -o json 2>&1)
        if ($LASTEXITCODE -ne 0) { throw "读取 Deployment/$writer 失败。" }
        try { $deployment = (($deploymentLines -join "`n") | ConvertFrom-Json -ErrorAction Stop) }
        catch { throw "Deployment/$writer JSON 非法:$($_.Exception.Message)" }
        # 与 Set 同口径:digest 按 Deployment 实际引用镜像取(可能是钉定不可变 tag,不假定 :dev)
        $writerImage = [string]$deployment.spec.template.spec.containers[0].image
        if ([string]::IsNullOrWhiteSpace($writerImage)) { throw "Deployment/$writer 容器镜像为空。" }
        $digest = Get-LocalMinikubeImageDigest -MinikubeProfile $MinikubeProfile -Image $writerImage
        $declared = [string]$deployment.spec.template.metadata.annotations.'pandora.dev/image-digest'
        if ($declared -cne $digest) {
            throw "Deployment/$writer 声明 digest 与 minikube 节点镜像『$writerImage』不一致；禁止以伪 provenance 启动。"
        }
        if ($SkipPodCheck) { continue }

        $podLines = @(& kubectl --context $KubeContext -n $K8sNamespace get pods -l "app=$writer" -o json 2>&1)
        if ($LASTEXITCODE -ne 0) { throw "读取 $writer Pod 失败。" }
        try { $podList = (($podLines -join "`n") | ConvertFrom-Json -ErrorAction Stop) }
        catch { throw "$writer Pod JSON 非法:$($_.Exception.Message)" }
        $pods = @($podList.items | Where-Object {
            $deleting = $_.metadata.PSObject.Properties['deletionTimestamp']
            $null -eq $deleting -or [string]::IsNullOrWhiteSpace([string]$deleting.Value)
        })
        if ($pods.Count -ne [int]$deployment.spec.replicas) {
            throw "$writer live Pod 数与 Deployment replicas 不一致。"
        }
        foreach ($pod in $pods) {
            if ([string]$pod.metadata.annotations.'pandora.dev/image-digest' -cne $digest) {
                throw "$writer Pod annotation 未命中 minikube immutable digest。"
            }
            $statuses = @($pod.status.containerStatuses | Where-Object { [string]$_.name -ceq $writer })
            if ($statuses.Count -ne 1 -or $null -eq $statuses[0].state.running -or
                -not ([string]$statuses[0].imageID).EndsWith($digest, [StringComparison]::Ordinal)) {
                $actualImageId = if ($statuses.Count -eq 1) { [string]$statuses[0].imageID } else { "(containerStatuses 数=$($statuses.Count))" }
                throw "$writer 运行容器 imageID 未命中声明的 minikube immutable digest。期望后缀:$digest;实际 imageID:$actualImageId(若两者 digest 形态不同,说明该节点 runtime 的 CRI 身份口径与 image ls 不一致,须先钉定口径,禁止放宽比对)。"
            }
        }
    }
    Write-Ok '五个 DS-auth writer Deployment/Pod/imageID provenance 对账通过。'
}

# ===== 共享:确保 in-cluster Envoy 镜像在 minikube 节点内(离线友好,三级来源)=====
# 顺序(2026-07-10,回应审核 P1「离线只会在 minikube 节点联网 pull,不复用宿主已有镜像」):
#   1) minikube 节点已有 → 直接用(cached);
#   2) 宿主 docker 已有 → minikube image load 灌进去(断网机 import_images -IncludeInfra 后走这条);
#   3) 都没有 → 才在节点内联网 pull(重试 6 次),仍失败 fail-fast 给出离线导入指引。
function Ensure-EnvoyImageInMinikube {
    param(
        [string]$MinikubeProfile,
        # 默认值维持 DS 面(16-ds-envoy.yaml)的既有 tag;边缘 Envoy(edge-envoy.yaml)钉定
        # v1.38.1(与 v1.38-latest 当日同 imageID,钉定只为杜绝"latest 漂移后新机器拉到不同构建")。
        [string]$Image = 'envoyproxy/envoy:v1.38-latest'
    )
    $envoyImg = $Image
    $mkArgs = @()
    if (-not [string]::IsNullOrWhiteSpace($MinikubeProfile)) { $mkArgs = @('-p', $MinikubeProfile) }
    # 节点内镜像探测/拉取按 runtime 分流(containerd 节点无 docker CLI);探测统一用
    # `minikube image ls`(runtime 无关),仅"节点内联网拉取"这一步需要区分命令。
    $resolvedProfile = if ([string]::IsNullOrWhiteSpace($MinikubeProfile)) { Get-K8sManagedProfile } else { $MinikubeProfile }
    $nodeRuntime = Get-MinikubeNodeRuntime $resolvedProfile

    $mkIds = Get-MinikubeImageIds $mkArgs
    if ($null -ne $mkIds -and $mkIds.ContainsKey($envoyImg)) {
        Write-Ok "in-cluster Envoy 镜像已在 minikube 节点($envoyImg)"
        return
    }

    docker image inspect $envoyImg *> $null
    if ($LASTEXITCODE -eq 0) {
        Write-Info "宿主 docker 已有 $envoyImg,直接 load 进 minikube(免联网)..."
        minikube @mkArgs image load --daemon=true $envoyImg
        Assert-LastExit "minikube image load $envoyImg"
    } else {
        Write-Info "宿主与 minikube 均无 $envoyImg,尝试在 minikube 节点联网拉取(断网机会失败,runtime=$nodeRuntime)..."
        if ($nodeRuntime -ceq 'docker') {
            minikube @mkArgs ssh -- "for i in 1 2 3 4 5 6; do docker pull $envoyImg && break || sleep 4; done" 2>$null | Out-Null
        } else {
            minikube @mkArgs ssh -- "for i in 1 2 3 4 5 6; do sudo crictl pull docker.io/$envoyImg && break || sleep 4; done" 2>$null | Out-Null
        }
    }

    # 拉取/加载可能静默失败(stderr 被吞)。显式确认镜像已在节点内,否则后面 pandora-envoy Pod
    # 会一直 ImagePullBackOff,DS 心跳(:8444)永远打不通 —— fail-fast 让调用方先解决镜像。
    $mkIds = Get-MinikubeImageIds $mkArgs
    if ($null -eq $mkIds -or -not $mkIds.ContainsKey($envoyImg)) {
        throw "in-cluster Envoy 镜像『$envoyImg』未能进入 minikube(断网/限流?)。DS 面 :8444 网关无法就绪,已中止。离线办法:在能联网机器 docker pull $envoyImg && docker save -o envoy.tar $envoyImg,拷到本机 docker load -i envoy.tar 后重跑(本函数会自动从宿主 load 进 minikube)。"
    }
    Write-Ok "in-cluster Envoy 镜像已就绪($envoyImg)"
}

# ===== k8s 模式(本地 minikube)=====
# ===== k8s 模式:把 UE Linux DS 打成镜像并落进 minikube =====
# DS 镜像(pandora/battle-ds:dev / pandora/hub-ds:dev)不是 22 个 go 业务镜像的一部分,
# 由本函数从【同级客户端仓库】的 Linux 打包产物构建。策略(2026-07-09 改为宿主构建 + load):
#   1) 调 build-image-minikube.ps1 -BuildOnHost:自动解析同级客户端仓库
#   1) 调 build-image-minikube.ps1 -BuildOnHost:优先从制品库取 Linux DS 包,没有时回退到
#      同级目录 <sibling>\Packages\Server_Linux_Development\LinuxServer(不写死路径),robocopy /MIR
#      同步进 deploy/ds/stage/LinuxServer,再在【宿主 docker daemon】docker build。
#   2) Sync-ImagesToMinikube 把宿主镜像 load 进 minikube(rmi+overwrite+校验,复用业务镜像同款安全路径)。
# 为什么不再在 minikube 内置 daemon 里直接 build:内网/断网机 minikube daemon 既没有 ubuntu:22.04
# 基础镜像、也拉不到公网 -> `FROM ubuntu:22.04` 报 connection refused 直接失败;宿主 docker 有
# ubuntu + apt 缓存层,离线只重跑 COPY 层,构建稳过。宿主构建用独立子进程跑,隔离 docker env。
function Build-DsImagesForMinikube {
    $dsBuild = Join-Path $ProjectRoot 'deploy/ds/build-image-minikube.ps1'
    if (-not (Test-Path $dsBuild)) { throw "找不到 DS 镜像构建脚本:$dsBuild" }
    Write-Info "在宿主 docker 构建 Battle/Hub DS 镜像(从同级客户端仓库取 Linux 包,离线友好)..."
    & pwsh -NoProfile -ExecutionPolicy Bypass -File $dsBuild -BuildOnHost
    Assert-LastExit 'DS 镜像构建(build-image-minikube.ps1 -BuildOnHost)'
    # 使用本次运行固定的 profile,避免构建期间 active profile 漂移后把镜像 load 到另一个集群。
    $mkProfile = Get-K8sManagedProfile
    Write-Info "把 DS 镜像 load 进 minikube(profile=$mkProfile,强制刷新 :dev tag)..."
    Sync-ImagesToMinikube -Images @('pandora/battle-ds:dev', 'pandora/hub-ds:dev') -MinikubeArgs @('-p', $mkProfile)
    # 2026-07-28:Fleet yaml 钉定不可变 tag(INC-20260727-001 防回滚,20/21/30/31-fleet-*.yaml)。
    # 钉定镜像不产自本次 :dev 构建——新节点(-Reset 重建)/新机器没有会让 GameServer 全量
    # ImagePullBackOff(下游 e2e 又被 -SkipImageLoad 跳过加载)。
    # 产出链(tag 命名契约=制品快照版本号):宿主 daemon 没有钉定镜像时,按 tag 定位制品库
    # <root>\snapshots\client\trunk_Client\Server_Linux_Development\<tag>\LinuxServer 重建同名
    # 镜像——任何机器只要可达制品库(PANDORA_ARTIFACT_ROOT,默认 F:\work\artifacts)即可复现;
    # 制品缺失才 fail-fast(绝不静默退回 :dev 冒充钉定内容)。
    $fleetPinned = @(Get-ChildItem (Join-Path $ProjectRoot 'deploy/k8s/agones') -Filter '*fleet*.yaml' |
        ForEach-Object { Select-String -LiteralPath $_.FullName -Pattern '^\s*image:\s*(pandora/\S+)' |
            ForEach-Object { $_.Matches[0].Groups[1].Value } } |
        Where-Object { $_ -notmatch ':dev$' } | Sort-Object -Unique)
    $pinnedMissing = @($fleetPinned | Where-Object {
        docker image inspect $_ *> $null; $LASTEXITCODE -ne 0
    })
    if ($pinnedMissing.Count -gt 0) {
        $artifactRoot = if (-not [string]::IsNullOrWhiteSpace($env:PANDORA_ARTIFACT_ROOT)) { $env:PANDORA_ARTIFACT_ROOT.Trim() } else { 'F:\work\artifacts' }
        foreach ($verGroup in ($pinnedMissing | Group-Object { ($_ -split ':', 2)[1] })) {
            $ver = $verGroup.Name
            # 制品库布局:<root>\{releases,snapshots}\client\<分支>\Server_Linux_Development\<版本>\LinuxServer。
            # **轨道与分支都不能写死**:PublishPackages.ps1 传 -Version 就落 releases 轨(否则 snapshots),
            # 分支名由 `svn info --show-item relative-url` 推导(^/trunk/Client → trunk_Client,换分支
            # 会变成 branches_xxx_Client)。曾写死 snapshots+trunk_Client,导致换分支 / 走 release 轨的
            # 机器在制品明明存在时也 fail-fast,与本函数"任何可达制品库的机器都能复现钉定镜像"的契约相悖。
            # 口径与 deploy/ds/build-image-minikube.ps1 的 Resolve-LinuxPkg 一致:两轨全扫 + 遍历全部分支。
            # 用 [IO.Path]::Combine 而非 Join-Path:制品根可能指向本机不存在的盘符(默认 F:\),
            # Join-Path 会先解析盘符并抛 "Cannot find drive",把下面那条可操作的 fail-fast 提示吞掉。
            $pkg = $null
            $probed = @()
            foreach ($track in @('releases', 'snapshots')) {
                $flavorRoot = [System.IO.Path]::Combine($artifactRoot, $track, 'client')
                if (-not (Test-Path -LiteralPath $flavorRoot -ErrorAction SilentlyContinue)) { continue }
                foreach ($branchDir in @(Get-ChildItem -LiteralPath $flavorRoot -Directory -ErrorAction SilentlyContinue)) {
                    $cand = [System.IO.Path]::Combine($branchDir.FullName, 'Server_Linux_Development', $ver, 'LinuxServer')
                    $probed += $cand
                    if (Test-Path -LiteralPath $cand -ErrorAction SilentlyContinue) { $pkg = $cand; break }
                }
                if ($pkg) { break }
            }
            if (-not $pkg) {
                $probedText = if ($probed.Count -gt 0) { "已探测的路径:`n  " + ($probed -join "`n  ") }
                              else { "制品根下没有 releases\client 或 snapshots\client 目录(制品根本身可能不存在或未挂载)。" }
                throw ("Fleet 钉定镜像 $($verGroup.Group -join ', ') 宿主缺失,且制品库找不到同版本 DS 包。`n" +
                    "$probedText`n" +
                    "处理方式:①确认 PANDORA_ARTIFACT_ROOT 指向共享制品根(当前=$artifactRoot);" +
                    "②在客户端仓库发布该版本(pwsh Tool\Build\PublishPackages.ps1);③改 fleet yaml 钉定为制品库里存在的版本。")
            }
            Write-Info "钉定制品命中:$pkg"
            Write-Info "钉定镜像宿主缺失,按制品 $ver 重建($($verGroup.Group -join ', '))..."
            & pwsh -NoProfile -ExecutionPolicy Bypass -File $dsBuild -BuildOnHost -Tag $ver -SourcePkg $pkg
            Assert-LastExit "按制品版本重建 Fleet 钉定 DS 镜像($ver)"
        }
    }
    if ($fleetPinned.Count -gt 0) {
        Write-Info "Fleet 钉定镜像一并 load:$($fleetPinned -join ', ')"
        Sync-ImagesToMinikube -Images $fleetPinned -MinikubeArgs @('-p', $mkProfile)
    }
    Write-Ok "DS 镜像已就绪(:dev + Fleet 钉定 tag 均已在 minikube)"
}

# ===== k8s 模式:宿主侧桥接/中继清理(与启动末尾自动跑的 e2e_k8s.ps1 对称)=====
# 启动会在宿主留下三类长驻资源:kubectl port-forward(pid 记录在 run/k8s-envoy-bridge/)、
# docker envoy(:8443/:8444)、pandora-udp-relay 容器。-Down 必须一并清掉,否则残留的
# port-forward/端口监听会让下次启动或其它模式(local/docker/battle)的流量走向不确定。
function Stop-K8sHostBridge {
    Write-Step "清理宿主 Envoy 桥接 + UDP 中继"

    # 1) 杀 bridge 起的 kubectl port-forward(pid 文件 + 进程名双重校验,防误杀)
    $stateDir = Join-Path $ProjectRoot 'run/k8s-envoy-bridge'
    if (Test-Path $stateDir) {
        foreach ($pidFile in @(Get-ChildItem $stateDir -Filter '*.pid' -ErrorAction SilentlyContinue)) {
            $procId = (Get-Content $pidFile.FullName -ErrorAction SilentlyContinue | Select-Object -First 1)
            if ("$procId" -match '^\d+$') {
                $proc = Get-Process -Id ([int]$procId) -ErrorAction SilentlyContinue
                if ($proc -and $proc.ProcessName -eq 'kubectl') {
                    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
                    Write-Info "  已停 port-forward $($pidFile.BaseName)(PID=$procId)"
                }
            }
            Remove-Item $pidFile.FullName -ErrorAction SilentlyContinue
        }
    }
    # 兜底:pid 文件丢失/陈旧时,按命令行签名扫残留的「kubectl port-forward --namespace pandora」
    $strays = @(Get-CimInstance Win32_Process -Filter "Name='kubectl.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -and $_.CommandLine -match 'port-forward' -and $_.CommandLine -match "--namespace\s+$K8sNamespace" })
    foreach ($p in $strays) {
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
        Write-Info "  已停残留 port-forward PID=$($p.ProcessId)"
    }

    # 2) 停并删宿主 docker envoy(bridge 用 compose 单起的 envoy,不碰其它基础设施容器)
    docker compose -f $ComposeInfra --env-file $EnvFile rm -sf envoy *> $null
    if ($LASTEXITCODE -eq 0) { Write-Info "  已停 envoy 容器(:8443/:8444)" }
    else { Write-Warn "  envoy 容器停止失败或不存在(可忽略)" }

    # 3) 删 UDP 回程中继容器(e2e_k8s.ps1 起的 dockerized relay)
    docker rm -f pandora-udp-relay *> $null
    if ($LASTEXITCODE -eq 0) { Write-Info "  已删 pandora-udp-relay 容器" }

    Write-Ok "宿主侧桥接/中继已清理"
}

# Assert-K8sEdgePublishReachable:核对既有集群的 8443 发布地址是不是对局域网开放的。
# minikube 的 --ports 只在**建集群时**生效,既有集群改不了 —— 老集群沿用旧默认
# 127.0.0.1:8443,集群一切正常但局域网就是连不进来,且全程无报错。这里如实点名并给出代价。
function Assert-K8sEdgePublishReachable([string]$Profile) {
    $publish = [regex]::Match((docker port $Profile 2>$null | Out-String), '(?m)^31443/tcp\s*->\s*(\S+):8443')
    if (-not $publish.Success) { return }
    $bind = $publish.Groups[1].Value
    if ($bind -in @('0.0.0.0', '[::]', '::')) { return }
    if (-not [string]::IsNullOrWhiteSpace($env:PANDORA_MINIKUBE_PORTS)) { return }  # 用户显式指定过,尊重之

    Write-Warn "既有集群『$Profile』的客户端入口只发布在 ${bind}:8443,局域网其它机器连不进来。"
    Write-Info "  minikube 的端口发布在建集群时钉死,既有集群改不了;要对局域网开放只能重建:"
    Write-Info "    pwsh tools/scripts/start.ps1 -Mode k8s -Reset"
    Write-Info "  代价:-Reset 会销毁集群(含正在跑的对局与集群内数据),请确认没人在打局再执行。"
    Write-Info "  只自己本机连则无需处理。"
}

function Invoke-K8s {
    $servicesDir = Join-Path $ProjectRoot 'deploy/k8s/services'
    $infraYaml   = Join-Path $ProjectRoot 'deploy/k8s/infra/infra.yaml'
    $lokiYaml    = Join-Path $ProjectRoot 'deploy/k8s/infra/loki.yaml'
    $monitoringYaml = Join-Path $ProjectRoot 'deploy/k8s/infra/monitoring.yaml'
    $tidbYaml       = Join-Path $ProjectRoot 'deploy/k8s/infra/tidb.yaml'
    $tidbInitJobYaml = Join-Path $ProjectRoot 'deploy/k8s/infra/tidb-init-job.yaml'
    $sentinelYaml   = Join-Path $ProjectRoot 'deploy/k8s/infra/redis-sentinel.yaml'
    $edgeEnvoyYaml  = Join-Path $ProjectRoot 'deploy/k8s/infra/edge-envoy.yaml'
    $tidbInitDir    = Join-Path $ProjectRoot 'deploy/tidb-init'
    $sentinelEntrypointFile = Join-Path $ProjectRoot 'deploy/redis/sentinel-entrypoint.sh'
    $grafanaAlertingDir = Join-Path $ProjectRoot 'deploy/grafana/provisioning/alerting'
    $hostEnvoyConfigFile = Join-Path $ProjectRoot 'deploy/envoy/envoy.yaml'
    $envoyCertDir   = Join-Path $ProjectRoot 'deploy/envoy'
    $mysqlInit   = Join-Path $ProjectRoot 'deploy/mysql-init'

    if ($Down) {
        # 删除是不可逆动作:先确认本机 minikube kube-context 存在,并把所有 delete 显式钉在它上面,
        # 绝不依赖(可能被切到远端/生产的)current-context。
        # 宿主侧桥接(kubectl port-forward / envoy 容器 / UDP relay 容器)跑在本机,与 k8s 集群
        # 是否可删无关——无论 context 在不在都要先清掉,否则残留进程/容器会占端口、误导后续启动。
        Stop-K8sHostBridge
        $mkProfile = Get-K8sManagedProfile
        # 集群不存在 / 没在跑 = 集群内没有任何东西在跑,Down 的目标本来就已经达成。
        # 这不是错误,是空跑:早期版本在这里硬抛(或对着停掉的集群狂 connection refused 后
        # 被 Assert-LastExit 抛出),让每台机器双击「一键停止」都以一片红字收场。
        if (-not (Test-MinikubeProfileExists -Profile $mkProfile)) {
            Write-Ok "本机没有 minikube 集群『$mkProfile』,集群侧无对象可删(宿主桥接已清理)。"
            return
        }
        & minikube -p $mkProfile status *> $null
        if ($LASTEXITCODE -ne 0) {
            Write-Ok "minikube 集群『$mkProfile』未在运行,集群内服务已随之全部停止(宿主桥接已清理)。"
            Write-Info "  数据保留,下次一键启动会自动拉回来;要彻底删掉集群(含数据):minikube delete -p $mkProfile"
            return
        }
        $contexts = @(kubectl config get-contexts -o name 2>$null)
        if ($contexts -cnotcontains $mkProfile) {
            throw "宿主桥接已清理，但 kubeconfig 中找不到 minikube context『$mkProfile』，无法证明集群侧对象已删除。为避免假报 Down 成功，已中止；请先修复该 profile/context 或确认集群已销毁。"
        }
        # 只校验 context 名不够:别人 kubeconfig 里可能存在同名 context 却指向远端/生产集群。
        # 必须确认该 context 的 apiserver endpoint 是本机 minikube(回环 IP 或 minikube 节点 IP),
        # 否则 fail-fast 且绝不 delete,避免误删同名远端集群或假报清理成功。
        if (-not (Test-KubeContextIsLocalMinikube $mkProfile)) {
            throw "宿主桥接已清理，但 kube-context『$mkProfile』的 endpoint 不是本机 minikube(可能是同名远端/生产集群)。为防误删且避免假报 Down 成功，集群侧删除未执行。"
        }
        Write-Step "删除 k8s 业务服务 + 基础设施(context=$mkProfile)"
        # Allocated 可见性(闭环审查确认 P2:Down 是第三条能杀载人 DS 的脚本路径,此前零守卫
        # 零告警也未登记同型扫描)。Down 的语义是**用户显式拆除整套本地栈**(后端一并删,
        # 保留 DS 无意义),故按 §9.21 豁免执行删除、不加开关;但必须点名将被级联杀掉的
        # Allocated 实例,让"我以为只是收工"与"正有人在打局"这两件事不再静默重合。
        $downAllocated = @()
        foreach ($downFleet in @('pandora-battle-stable', 'pandora-battle-canary', 'pandora-hub-stable', 'pandora-hub-canary')) {
            $downJson = (kubectl --context $mkProfile get gameservers -l "agones.dev/fleet=$downFleet" -n 'default' -o json 2>$null | Out-String)
            if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($downJson)) { continue }
            $downParsed = $downJson | ConvertFrom-Json
            if ($null -eq $downParsed.items) { continue }
            foreach ($gs in @($downParsed.items)) {
                if ([string]$gs.status.state -ceq 'Allocated') {
                    $downAllocated += "$downFleet/$($gs.metadata.name)"
                }
            }
        }
        if ($downAllocated.Count -gt 0) {
            Write-Warn "Down 将级联删除 $($downAllocated.Count) 台 Allocated GameServer(可能载人,对局会被打断):$($downAllocated -join ', ')"
            Write-Warn "  这是 Down「显式拆除整套本地栈」的既定代价(§9.21 对 Down 的豁免,已登记 INC-20260803-002 §6);若不想打断对局请 Ctrl+C,等对局结束再 Down。"
        }
        # Fleet 是启动时 Apply-AgonesManifests 起的,也要停干净(DS Pod 别留着空跑)。
        # 先删 FleetAutoscaler 再删 Fleet:避免删 Fleet 期间 autoscaler 还在按 buffer 补建 GameServer。
        foreach ($fleetFile in @('25-fleetautoscaler-battle.yaml', '31-fleet-hub-canary.yaml', '30-fleet-hub.yaml', '21-fleet-battle-canary.yaml', '20-fleet-battle.yaml')) {
            Invoke-K8sDownDelete -What "kubectl delete Fleet $fleetFile" -Arguments @(
                '--context', $mkProfile, 'delete', '-f', (Join-Path $ProjectRoot "deploy/k8s/agones/$fleetFile"), '--ignore-not-found'
            )
        }
        # Down 是用户明确要求停掉整套本地 DS，因此也清理旧版单轨对象。
        Invoke-K8sDownDelete -What 'kubectl delete legacy local Fleets' -Arguments @(
            '--context', $mkProfile, 'delete', 'fleet/pandora-battle', 'fleet/pandora-hub', '-n', 'default', '--ignore-not-found'
        )
        # in-cluster Envoy「DS 面」网关也是启动时 Apply-AgonesManifests 起的(本地专属),一并清理
        Invoke-K8sDownDelete -What 'kubectl delete in-cluster Envoy' -Arguments @(
            '--context', $mkProfile, 'delete', '-f', (Join-Path $ProjectRoot 'deploy/k8s/agones/16-ds-envoy.yaml'), '--ignore-not-found'
        )
        # 只有 Down 前确实存在 etcd-data PVC，才要求 Down 后仍保留；对早已拆空的集群空跑不误报数据丢失。
        $etcdPvcExistedBeforeDown = Test-LocalEtcdDataPvcExists -KubeContext $mkProfile
        # 业务服务:渲染 kustomize 后删除除 Namespace 外的全部对象。
        # 保留 namespace/pandora 是刻意的——services kustomize 含 00-namespace.yaml，
        # 直接 delete -k 会连 namespace 一起删掉，级联清空 PVC/etcd-data（DS auth V3 权威）。
        $servicesManifest = ((@(& kubectl --context $mkProfile kustomize $servicesDir 2>&1) |
                ForEach-Object { $_.ToString() }) -join "`n")
        Assert-LastExit 'kubectl kustomize k8s services'
        $null = Remove-K8sManifestObjectsPreserving -KubeContext $mkProfile -ManifestText $servicesManifest `
            -What 'kubectl delete k8s services（保留 namespace）' -ShouldPreserve {
            param($o)
            ([string]$o.Kind -ceq 'Namespace') -and ([string]$o.Name -ceq $K8sNamespace)
        }
        # Loki/Alloy 现在带数据盘(loki-data / alloy-data PVC)。普通 -Down 只停服务,不得删日志——
        # 与上面 namespace/etcd-data 同一政策:删了 PVC,下次起来是个空 Loki,历史排障线索(含已回收
        # 的短命 Battle DS 那几局)永久消失。要清盘请显式 `kubectl -n pandora delete pvc/loki-data`。
        $lokiManifestText = (Get-Content -LiteralPath $lokiYaml -Raw)
        $null = Remove-K8sManifestObjectsPreserving -KubeContext $mkProfile -ManifestText $lokiManifestText `
            -What 'kubectl delete Loki（保留日志数据盘）' -ShouldPreserve {
            param($o)
            [string]$o.Kind -ceq 'PersistentVolumeClaim'
        }
        Invoke-K8sDownDelete -What 'kubectl delete Prometheus/Grafana' -Arguments @('--context', $mkProfile, 'delete', '-f', $monitoringYaml, '--ignore-not-found')
        Invoke-K8sDownDelete -What 'kubectl delete TiDB' -Arguments @('--context', $mkProfile, 'delete', '-f', $tidbYaml, '--ignore-not-found')
        Invoke-K8sDownDelete -What 'kubectl delete tidb-init Job' -Arguments @('--context', $mkProfile, 'delete', '-f', $tidbInitJobYaml, '--ignore-not-found')
        Invoke-K8sDownDelete -What 'kubectl delete Redis Sentinel' -Arguments @('--context', $mkProfile, 'delete', '-f', $sentinelYaml, '--ignore-not-found')
        Invoke-K8sDownDelete -What 'kubectl delete edge Envoy' -Arguments @('--context', $mkProfile, 'delete', '-f', $edgeEnvoyYaml, '--ignore-not-found')
        Invoke-K8sDownDelete -What 'kubectl delete 附属 ConfigMap' -Arguments @(
            '--context', $mkProfile, 'delete', 'configmap', 'pandora-tidb-init', 'pandora-redis-sentinel-entrypoint',
            'pandora-edge-envoy-config', 'pandora-grafana-alerting', '-n', $K8sNamespace, '--ignore-not-found'
        )
        Invoke-K8sDownDelete -What 'kubectl delete 附属 Secret' -Arguments @(
            '--context', $mkProfile, 'delete', 'secret', 'pandora-edge-envoy-certs', 'pandora-grafana-alert-env',
            '-n', $K8sNamespace, '--ignore-not-found'
        )
        # 基础设施:删除除 PVC/etcd-data 外的全部对象，保留 etcd 数据卷。
        $infraManifest = (Get-Content -LiteralPath $infraYaml -Raw)
        $null = Remove-K8sManifestObjectsPreserving -KubeContext $mkProfile -ManifestText $infraManifest `
            -What 'kubectl delete k8s infra（保留 PVC/etcd-data）' -ShouldPreserve {
            param($o)
            ([string]$o.Kind -ceq 'PersistentVolumeClaim') -and
            ([string]$o.Name -ceq 'etcd-data') -and
            ([string]$o.Namespace -ceq $K8sNamespace)
        }
        if ($script:K8sDownApiserverGone) {
            Write-Ok "集群已在 Down 期间停止/消失，集群内服务已全部停止（宿主桥接已清理）。"
            Write-Info "  etcd 数据随集群磁盘保留，下次一键启动会自动拉回来。"
            return
        }
        if ($etcdPvcExistedBeforeDown) {
            Assert-LocalEtcdDataPreservedAfterDown -KubeContext $mkProfile
        }
        else {
            Write-Info "Down 前本地无 etcd-data PVC，无 DS auth 基线可保留（跳过保留校验）。"
        }
        Write-Info "minikube 仍在运行;彻底关:minikube stop -p $mkProfile"
        return
    }

    Write-Step "k8s 模式:minikube 本地集群"

    # profile 钉死:本次运行的 minikube 全部操作(status/start,以及 Reset 的 delete)都用同一个 profile,
    # 避免 delete 与 rebuild 目标漂移。
    $mkProfile = Get-K8sManagedProfile
    # 记录 profile 是否在本次运行前存在，仅用于区分全新 minikube 与“现有 minikube 上首次
    # 安装 Pandora”。真正的 genesis 授权不再依赖这个瞬时布尔值，而依赖持久 marker/PVC UID。
    $profileExistedBeforeStart = Test-MinikubeProfileExists -Profile $mkProfile

    # DS 包前置预检(2026-07-28,回应换机器审计):真 DS 是本模式的硬前置,但它的构建在
    # [4/8]——起集群+装基础设施之后。新机器若既无同级客户端仓库/制品库/环境变量指定的包、
    # 也无历史 stage,与其让人等十几分钟再在 [4/8] 中止,不如现在就按 -ResolveOnly(exit 2=
    # 彻底无源)讲清楚。只预检不构建,几秒完成;有任一来源即放行,防降级门仍在真实构建时把关。
    & pwsh -NoProfile -ExecutionPolicy Bypass -File (Join-Path $ProjectRoot 'deploy/ds/build-image-minikube.ps1') -ResolveOnly
    if ($LASTEXITCODE -eq 2) {
        throw ("未找到任何 UE Linux DS 包来源,真 DS 闭环无法构建。任选其一后重跑:`n" +
            "  ①同级客户端仓库打包:pwsh Tool\Build\PackageSet.ps1 -Flavors 'Server/Linux/Development'`n" +
            "  ②指向共享制品库:`$env:PANDORA_ARTIFACT_ROOT='<制品根>'(内有 snapshots\client\...\Server_Linux_Development)`n" +
            "  ③手动指定包目录:`$env:PANDORA_DS_LINUX_PKG='<...\LinuxServer>'")
    }

    # 1) minikube 起没起。拓扑参数由 Get-MinikubeStartArgs 决定:既有 profile 空参续跑,
    # 新建 profile 按默认(docker/4C/6144M)或 PANDORA_MINIKUBE_* 环境覆盖创建。
    # 起集群前先与 local 模式互斥:两者都要占宿主 8443,local 在跑就用它自己的关闭流程停掉。
    Stop-LocalStackForK8s
    minikube -p $mkProfile status *> $null
    if ($LASTEXITCODE -ne 0) {
        $mkStartArgs = Get-MinikubeStartArgs $mkProfile
        Write-Info "启动 minikube(profile=$mkProfile$(if ($mkStartArgs.Count) { ";新建参数:$($mkStartArgs -join ' ')" } else { ';既有集群,沿用已存拓扑' }))..."
        minikube start -p $mkProfile @mkStartArgs
        if ($LASTEXITCODE -ne 0) { throw "minikube 启动失败" }
    } else {
        Write-Ok "minikube 已在运行(profile=$mkProfile)"
    }
    # 发布端口是**建集群时**钉死的,既有集群改不了。老集群(建于默认还是 127.0.0.1:8443 的年代)
    # 会导致「集群跑着但局域网连不进来」,且没有任何报错——这里如实点名,给出 -Reset 的代价说明。
    Assert-K8sEdgePublishReachable -Profile $mkProfile

    # 上下文锁:本地 k8s 部署会 apply/删除大量对象,必须确认 current-context 就是本机 minikube,
    # 否则会把本地开发部署误发到用户 kubectl 当前指向的远端/生产集群。不匹配直接 fail-fast。
    $mkCtx = Resolve-MinikubeKubeContext
    $curCtx = (kubectl config current-context 2>$null)
    if ($curCtx -cne $mkCtx) {
        throw "当前 kubectl current-context『$curCtx』不是本机 minikube『$mkCtx』。为防止把本地 k8s 部署误发到远端/生产集群,已中止。请先执行:kubectl config use-context $mkCtx 再重跑。"
    }
    # 只比对 context 名不够:别人 kubeconfig 里可能存在同名 context 却指向远端/生产集群。
    # 必须确认该 context 的 apiserver endpoint 是本机 minikube(回环 / minikube 节点 IP),否则中止,
    # 避免后续 kubectl apply 落到同名远端集群。
    if (-not (Test-KubeContextIsLocalMinikube $mkCtx)) {
        throw "kube-context『$mkCtx』的 apiserver endpoint 不是本机 minikube(疑似同名远端/生产集群)。为防把本地部署误发到远端,已中止。"
    }
    Write-Ok "kube-context 已锁定本机 minikube:$mkCtx"
    $kubectlContextArgs = @('--context', $mkCtx)
    $pandoraNamespaceExistedBeforeStart = Test-KubernetesNamespaceExists -KubeContext $mkCtx -Namespace $K8sNamespace
    # 在任何 apply 前先分类 namespace anchor / marker。preinfra 不依赖 PVC，必须先识别它，
    # 才能让上次在动态 provision 期间留下的 Pending PVC 继续收敛；pending/complete 仍会
    # 在 marker 校验中严格绑定同一块 Bound PVC。
    $initialGenesisMarker = $null
    $initialGenesisMarkerState = ''
    $initialFreshNamespaceAnchor = $false
    if ($pandoraNamespaceExistedBeforeStart) {
        $initialGenesisMarker = Get-LocalFreshGenesisIntent -KubeContext $mkCtx
        if ($null -ne $initialGenesisMarker) {
            $initialGenesisMarkerState = Assert-LocalFreshGenesisIntent -Marker $initialGenesisMarker `
                -KubeContext $mkCtx -MinikubeProfile $mkProfile
        }
        $initialFreshNamespaceAnchor = Get-LocalFreshNamespaceAnchor -KubeContext $mkCtx -MinikubeProfile $mkProfile
    }
    Assert-ExistingLocalEtcdPersistence -KubeContext $mkCtx `
        -AllowPendingPvcForPreinfra:($initialGenesisMarkerState -ceq 'preinfra')
    Assert-NoLegacyDsFleets -KubeContext $mkCtx -LocalDevelopment
    Assert-NoLegacyDSTicketSignerSecret -KubeContext $mkCtx -LocalDevelopment

    # 在任何 apply 前回读既有 PVC。已有 Deployment 却缺 PVC 已由上面的 persistence
    # guard 阻断；无 marker 的旧半成品只有在本次运行前就存在同一个 Bound PVC 时，才有资格
    # 进入后面的 revision=1 + zero-writer 一次性兼容收养。
    $legacyAdoptionPvcUid = ''
    if ($pandoraNamespaceExistedBeforeStart) {
        if (Test-LocalEtcdDataPvcExists -KubeContext $mkCtx) {
            $initialPvc = Get-KubectlJsonObject -KubeContext $mkCtx `
                -Arguments @('get', 'pvc/etcd-data', '-n', $K8sNamespace, '-o', 'json') -Action '启动写入前读取 PVC/etcd-data'
            $initialPvcUid = [string]$initialPvc.metadata.uid
            $initialPvcPhase = [string]$initialPvc.status.phase
            if ([string]::IsNullOrWhiteSpace($initialPvcUid) -or
                ($initialPvcPhase -cne 'Bound' -and -not ($initialGenesisMarkerState -ceq 'preinfra' -and $initialPvcPhase -ceq 'Pending'))) {
                throw '启动写入前 PVC/etcd-data 必须为 Bound 且 UID 非空；只有可信 preinfra 可等待 Pending。'
            }
            if ($initialPvcPhase -ceq 'Bound') { $legacyAdoptionPvcUid = $initialPvcUid }
            else { Write-Info '检测到可信 preinfra + Pending PVC；继续 apply/rollout，待 Bound 后再绑定 marker。' }
        } elseif ($initialGenesisMarkerState -cin @('adopting', 'pending', 'complete')) {
            throw "marker=$initialGenesisMarkerState 却缺少绑定的 PVC/etcd-data，拒绝重建空盘。"
        } elseif ($initialFreshNamespaceAnchor -and $null -ne $initialGenesisMarker) {
            Write-Info 'fresh namespace anchor 与 preinfra marker 均在，继续完成 anchor 清理和 infra。'
        } elseif ($null -ne $initialGenesisMarker -and $initialGenesisMarkerState -cne 'preinfra') {
            throw "marker=$initialGenesisMarkerState 时 PVC/etcd-data 缺失，拒绝继续。"
        }
        if ($initialGenesisMarkerState -ceq 'preinfra' -and [string]::IsNullOrWhiteSpace($legacyAdoptionPvcUid)) {
            # preinfra 的 PVC 可以尚未创建或 Pending；它不是 after-the-fact legacy 收养候选。
            $legacyAdoptionPvcUid = ''
        }
    }
    $allowLegacyInfraOnlyAdoption = $pandoraNamespaceExistedBeforeStart -and
        $null -eq $initialGenesisMarker -and
        -not [string]::IsNullOrWhiteSpace($legacyAdoptionPvcUid)
    $legacyAdoptionCollectionTimeUnixMS = 0L
    $legacyAdoptionCohortFingerprintSha256 = ''
    $legacyAdoptionCohortPreflightError = ''
    if ($allowLegacyInfraOnlyAdoption) {
        # 必须在本轮任何 apply 前固定采集时刻和对象 identity。这里故意只记录失败而不立刻
        # throw：无 marker 的既有精确 V3 集群无需收养，稍后的线性 required read 成功即可继续；
        # 只有 read 证明 missing 时，baseline 才把 cohort 失败升级为阻断。
        $legacyAdoptionCollectionTimeUnixMS = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
        try {
            # apply 前先固定 UID/owner/spec/timestamp cohort；Pod 尚在拉镜像时允许未 Ready。infra
            # rollout 完成后的 baseline 会用同一 collectedAt 严格重算 Ready 且 fingerprint 必须不变。
            $legacyCohort = Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext $mkCtx `
                -CollectionTimeUnixMS $legacyAdoptionCollectionTimeUnixMS -ExpectedPvcUid $legacyAdoptionPvcUid `
                -AllowNotReady
            $legacyAdoptionCohortFingerprintSha256 = [string]$legacyCohort.FingerprintSha256
        } catch {
            $legacyAdoptionCohortPreflightError = $_.Exception.Message
        }
    }

    Write-Step "[1/8] namespace"
    if (-not $pandoraNamespaceExistedBeforeStart) {
        New-LocalFreshAnchoredNamespace -KubeContext $mkCtx -MinikubeProfile $mkProfile
    }
    kubectl @kubectlContextArgs apply -f (Join-Path $servicesDir '00-namespace.yaml')
    Assert-LastExit 'kubectl apply namespace'
    $freshNamespaceAnchor = Get-LocalFreshNamespaceAnchor -KubeContext $mkCtx -MinikubeProfile $mkProfile
    $currentGenesisMarker = Get-LocalFreshGenesisIntent -KubeContext $mkCtx
    if ($freshNamespaceAnchor -and $null -eq $currentGenesisMarker) {
        $installKind = if ($profileExistedBeforeStart) { '中断恢复/现有 minikube 上首次安装 Pandora' } else { '全新 minikube profile' }
        Write-Info "$installKind：从 namespace 原子 anchor 建立正式 fresh DS-auth intent。"
        New-LocalFreshGenesisIntent -KubeContext $mkCtx -MinikubeProfile $mkProfile
        $currentGenesisMarker = Get-LocalFreshGenesisIntent -KubeContext $mkCtx
    }
    if ($freshNamespaceAnchor) {
        Remove-LocalFreshNamespaceAnchor -KubeContext $mkCtx -MinikubeProfile $mkProfile
    } elseif (-not $pandoraNamespaceExistedBeforeStart -and $null -eq $currentGenesisMarker) {
        throw 'fresh namespace 已创建但 anchor/marker 均缺失，拒绝进入配置/infra。'
    }

    # DS advertise 地址:默认取本机局域网 IP,让内网其它机器的客户端能连到本机 DS(经容器版 UDP 中继回程)。
    # minikube docker driver 下 Pod IP 局域网不可达,但「advertise=本机局域网 IP + UDP 中继监听 0.0.0.0」
    # 这条回程链路可达:client(内网) -> 本机:<port>(UDP) -[Docker publish 0.0.0.0]-> relay 容器 -> minikube 节点 -> DS。
    # 覆盖优先级:-AdvertiseHost > $env:PANDORA_DS_ADVERTISE_HOST > Resolve-LanIp;都拿不到才回退 127.0.0.1(仅本机自测)。
    $k8sAdvHost =
        if (-not [string]::IsNullOrWhiteSpace($AdvertiseHost)) { $AdvertiseHost.Trim() }
        elseif (-not [string]::IsNullOrWhiteSpace($env:PANDORA_DS_ADVERTISE_HOST)) { $env:PANDORA_DS_ADVERTISE_HOST.Trim() }
        else { Resolve-LanIp }
    if ([string]::IsNullOrWhiteSpace($k8sAdvHost)) {
        $k8sAdvHost = '127.0.0.1'
        Write-Warn "未解析到局域网 IPv4,DS advertise 回退 127.0.0.1(只有本机客户端连得到)。内网多机联调请用 -AdvertiseHost <本机内网IP> 重跑。"
    }
    # advertise 非回环 => UDP 中继必须对局域网开放(publish 到 0.0.0.0),否则其它机器的 UDP 到不了本机中继。
    $script:K8sRelayBindHost = if ($k8sAdvHost -eq '127.0.0.1') { '127.0.0.1' } else { '0.0.0.0' }
    Write-Step "[2/8] 生成集群版配置 + Secret(allocator=agones,DS advertise=$k8sAdvHost)"
    if ($script:K8sRelayBindHost -eq '0.0.0.0') {
        Write-Info "DS advertise=$k8sAdvHost(局域网),UDP 中继将监听 0.0.0.0。"
        Write-Warn "内网多机联调前置:请确认本机防火墙已放行【入站 TCP 8443 + 入站 UDP 7000-8000】,否则其它机器连不进客户端入口/DS。DS 面 8444 固定只绑本机。"
    }
    # 本地 minikube 自测:agones 真 DS 链路但沿用公开 dev 密钥,显式 -AllowDevSecrets 承认(审核 P1:
    # 不再靠 advertise host 推断本地,防生产 IP/DNS 绕过 -Prod)。线上走 online 分支的 -Prod。
    # DSTicket v2(方案 B):先自举 dev 钥料(私钥 Secret + 公钥 ConfigMap,幂等),拿 kid 注入配置生成。
    $dsTicketKid = Ensure-DsTicketDevKeyMaterial -KubeContext $mkCtx
    & "$ScriptDir/gen_cluster_config.ps1" -AllocatorMode agones -AllocatorAdvertiseHost $k8sAdvHost -AllowDevSecrets `
        -DsAuthMode enforce -DsAuthorityMode redis -DsFenceEtcdEndpoints $script:LocalDsFenceEndpoint `
        -DsFenceKeysetRevision $script:LocalDsFenceKeysetRevision -DsTicketActiveKid $dsTicketKid `
        -DsTicketKeysetRevision 1
    Assert-LastExit '生成本地 k8s enforce/redis DSTicket v2 配置'
    # 配置含 HS256 密钥(即便本地 dev 也含 ds_auth secret),用 Secret 而非 ConfigMap 承载(P0:密钥不落明文 ConfigMap)。
    Apply-PandoraConfigSecret -KubeContext $mkCtx -Action 'kubectl apply secret pandora-config'
    $null = Apply-PandoraConfigTableConfigMap -KubeContext $mkCtx -Action 'kubectl apply configmap pandora-configtable'
    kubectl @kubectlContextArgs create configmap pandora-mysql-init --from-file=$mysqlInit -n $K8sNamespace `
        --dry-run=client -o yaml | kubectl @kubectlContextArgs apply -f -
    Assert-LastExit 'kubectl apply configmap pandora-mysql-init'

    Write-Step "[3/8] 基础设施(mysql/redis/zookeeper/kafka/etcd + loki/alloy 日志)"
    kubectl @kubectlContextArgs apply -f $infraYaml
    Assert-LastExit 'kubectl apply infra'
    # 日志采集(Loki + Alloy,infra.md §11.2):非关键路径,apply 失败只告警不阻断启动;
    # 也不等它 rollout(日志栈晚几十秒就绪不影响业务链路)。
    kubectl @kubectlContextArgs apply -f $lokiYaml
    if ($LASTEXITCODE -ne 0) { Write-Warn "loki/alloy 日志栈 apply 失败(不影响业务);可稍后手动 kubectl apply -f deploy/k8s/infra/loki.yaml" }
    # 监控(Prometheus kubernetes_sd + Grafana,2026-07-28 进集群):同日志栈,非关键路径只告警
    kubectl @kubectlContextArgs apply -f $monitoringYaml
    if ($LASTEXITCODE -ne 0) { Write-Warn "Prometheus/Grafana 监控栈 apply 失败(不影响业务);可稍后手动 kubectl apply -f deploy/k8s/infra/monitoring.yaml" }
    # Grafana 告警 provisioning(2026-07-28 P2):仅当宿主导出 PANDORA_ALERT_NTFY_URL 时进集群
    # (与 compose 侧 entrypoint 的 env 守卫等价——URL 缺失时装载告警配置会让 Grafana 拒起);
    # 未设置则跳过,grafana Deployment 的 alerting 挂载/envFrom 均 optional,数据源不受影响。
    if (-not [string]::IsNullOrWhiteSpace($env:PANDORA_ALERT_NTFY_URL)) {
        kubectl @kubectlContextArgs create configmap pandora-grafana-alerting --from-file=$grafanaAlertingDir -n $K8sNamespace `
            --dry-run=client -o yaml | kubectl @kubectlContextArgs apply -f -
        if ($LASTEXITCODE -ne 0) { Write-Warn 'pandora-grafana-alerting configmap 失败(不影响业务)' }
        kubectl @kubectlContextArgs create secret generic pandora-grafana-alert-env `
            --from-literal=PANDORA_ALERT_NTFY_URL=$env:PANDORA_ALERT_NTFY_URL -n $K8sNamespace `
            --dry-run=client -o yaml | kubectl @kubectlContextArgs apply -f -
        if ($LASTEXITCODE -ne 0) { Write-Warn 'pandora-grafana-alert-env secret 失败(不影响业务)' }
    } else {
        Write-Info 'PANDORA_ALERT_NTFY_URL 未设置,Grafana 告警 provisioning 未进集群(数据源不受影响)。'
    }
    # TiDB(pd/tikv/tidb)与 Redis Sentinel(一主两从三哨):2026-07-28 P2 进集群,
    # 与线上形态同构;dev 服务仍用单 mysql/redis,本栈非关键路径,失败只告警。
    # tidb-init Job 不在此处:fresh-genesis 门([3.5/8])只允许 Deployment/ReplicaSet/Pod,
    # Job 在门后由 [3.6/8] 创建(deploy/k8s/infra/tidb-init-job.yaml)。
    kubectl @kubectlContextArgs create configmap pandora-tidb-init --from-file=$tidbInitDir -n $K8sNamespace `
        --dry-run=client -o yaml | kubectl @kubectlContextArgs apply -f -
    if ($LASTEXITCODE -ne 0) { Write-Warn 'pandora-tidb-init configmap 失败(不影响业务)' }
    kubectl @kubectlContextArgs apply -f $tidbYaml
    if ($LASTEXITCODE -ne 0) { Write-Warn "TiDB 栈 apply 失败(不影响业务);可稍后手动 kubectl apply -f deploy/k8s/infra/tidb.yaml" }
    kubectl @kubectlContextArgs create configmap pandora-redis-sentinel-entrypoint --from-file=$sentinelEntrypointFile -n $K8sNamespace `
        --dry-run=client -o yaml | kubectl @kubectlContextArgs apply -f -
    if ($LASTEXITCODE -ne 0) { Write-Warn 'pandora-redis-sentinel-entrypoint configmap 失败(不影响业务)' }
    kubectl @kubectlContextArgs apply -f $sentinelYaml
    if ($LASTEXITCODE -ne 0) { Write-Warn "Redis Sentinel 栈 apply 失败(不影响业务);可稍后手动 kubectl apply -f deploy/k8s/infra/redis-sentinel.yaml" }
    Write-Info "等待基础设施就绪(第三方镜像首次冷拉每个最多 1800s/30 分钟)..."
    kubectl @kubectlContextArgs rollout status deploy/mysql     -n $K8sNamespace --timeout=1800s; Assert-LastExit 'mysql 就绪'
    kubectl @kubectlContextArgs rollout status deploy/redis     -n $K8sNamespace --timeout=1800s; Assert-LastExit 'redis 就绪'
    kubectl @kubectlContextArgs rollout status deploy/etcd      -n $K8sNamespace --timeout=1800s; Assert-LastExit 'etcd 就绪'
    # zookeeper / kafka 必须就绪,否则 player/push/battle-result 会因连不上 kafka:9092 CrashLoop
    kubectl @kubectlContextArgs rollout status deploy/zookeeper -n $K8sNamespace --timeout=1800s; Assert-LastExit 'zookeeper 就绪'
    kubectl @kubectlContextArgs rollout status deploy/kafka     -n $K8sNamespace --timeout=1800s; Assert-LastExit 'kafka 就绪'

    Write-Step '[3.5/8] DS callback auth required_writer_epoch 线性预检 / marker 授权的一键 CAS bootstrap'
    Assert-LocalDsAuthBaseline -KubeContext $mkCtx -MinikubeProfile $mkProfile -AllowFreshBootstrap:$true `
        -AllowLegacyInfraOnlyAdoption:$allowLegacyInfraOnlyAdoption -ExpectedAdoptionPvcUid $legacyAdoptionPvcUid `
        -ExpectedAdoptionCohortFingerprintSha256 $legacyAdoptionCohortFingerprintSha256 `
        -LegacyAdoptionCollectionTimeUnixMS $legacyAdoptionCollectionTimeUnixMS `
        -LegacyAdoptionCohortPreflightError $legacyAdoptionCohortPreflightError

    # [3.6/8] tidb-init Job:必须晚于 [3.5/8] fresh-genesis 门(门只允许 Deployment/ReplicaSet/Pod,
    # Job 属一次性任务放门后);Job spec 不可变,先删旧再 apply(SQL 幂等,重跑安全);非关键路径只告警。
    kubectl @kubectlContextArgs delete job tidb-init -n $K8sNamespace --ignore-not-found 2>$null | Out-Null
    kubectl @kubectlContextArgs apply -f $tidbInitJobYaml
    if ($LASTEXITCODE -ne 0) { Write-Warn 'tidb-init Job apply 失败(不影响业务);可稍后手动 kubectl apply -f deploy/k8s/infra/tidb-init-job.yaml' }

    Write-Step "[4/8] 安装 Agones + apply RBAC/Fleet(真 Linux DS)"
    Build-DsImagesForMinikube
    # -ForceRecreateGameServers:上一步刚把新 DS 镜像 build 进 minikube 的 :dev tag,
    # 但 Fleet spec 不变,kubectl apply 不会换掉已在跑的旧 GameServer Pod。删旧 GameServer
    # 让 Agones 用最新 :dev 重建,保证已运行集群上重跑也能换成最新 DS。
    # -KubeContext:把强删 GameServer 钉在本机 minikube,防误删远端集群。
    Apply-AgonesManifests -InstallAgones -ForceRecreateGameServers -KubeContext $mkCtx

    Write-Step "[5/8] 构建 22 个服务镜像"
    Build-AllImages

    Write-Step "[6/8] 把镜像 load 进 minikube(强制刷新固定 :dev tag)"
    # 与 DS 镜像同样显式钉死本次已校验的本地 profile。不能依赖 minikube 的
    # active profile：它可能与已锁定的 kubectl context 不同，导致新业务镜像被 load
    # 到另一个本地集群，而当前集群随后只重启出旧 :dev 镜像。
    Sync-ImagesToMinikube -Images (Get-ServiceImages) -MinikubeArgs @('-p', $mkProfile)
    # 2026-07-28:services.yaml 里存在钉定不可变 tag 的镜像(INC-20260727-001 防回滚,如
    # matchmaker geed8ce2c6b5d / ds-allocator geed8ce2-p03-*)。它们不在 :dev 构建清单里,
    # 但 Deployment 引用它们——新节点(-Reset 重建)没有就会 ImagePullBackOff。从宿主 daemon
    # 一并 load;宿主缺失即 fail-fast(钉定镜像是已验证产物,绝不静默跳过、绝不退回 :dev)。
    $pinnedImages = @(Select-String -LiteralPath (Join-Path $servicesDir 'services.yaml') -Pattern '^\s*image:\s*(pandora/\S+)' |
        ForEach-Object { $_.Matches[0].Groups[1].Value } | Where-Object { $_ -notmatch ':dev$' } | Sort-Object -Unique)
    if ($pinnedImages.Count -gt 0) {
        Write-Info "钉定镜像一并 load:$($pinnedImages -join ', ')"
        Sync-ImagesToMinikube -Images $pinnedImages -MinikubeArgs @('-p', $mkProfile)
    }

    Write-Step "[7/8] 部署业务服务"
    kubectl @kubectlContextArgs apply -k $servicesDir
    Assert-LastExit 'kubectl apply -k services'
    # capability 的镜像身份必须取自刚 load 进本次 minikube profile 的节点实际 digest。
    # 先 patch template annotation，再启动/等待 writer；绝不继承上次发布的旧 annotation。
    Set-LocalDsAuthImageDigestAnnotations -KubeContext $mkCtx -MinikubeProfile $mkProfile
    # 镜像 tag 固定为 :dev,重建/重 load 后 image 字符串不变 -> apply 报 unchanged,旧 Pod 不会换。
    # 按名强制滚动重启这 22 个业务 Deployment(不碰 infra,避免重启 kafka 又触发依赖服务 CrashLoop),
    # 确保跑的是刚 build 的新二进制。
    Write-Info "rollout restart 业务 Deployment(同 :dev tag 重建后强制换 Pod)..."
    foreach ($svc in (Get-ServiceList)) {
        kubectl @kubectlContextArgs rollout restart deploy/$($svc.Name) -n $K8sNamespace
        Assert-LastExit "rollout restart $($svc.Name)"
    }
    # 等滚动完成:确认新 Pod 真的起来了。imagePullPolicy=IfNotPresent 在 Pod 创建时按 :dev tag
    # 现场解析,上面 Sync-ImagesToMinikube 已保证 minikube 内 :dev == 宿主最新 build,
    # 故 rollout 完成 == 新 Pod 必为新镜像(校验+重启+等待三步合一堵死"旧镜像旧 Pod"链路)。
    Write-Info "等待业务 Deployment 滚动完成(每个最多 180s)..."
    foreach ($svc in (Get-ServiceList)) {
        kubectl @kubectlContextArgs rollout status deploy/$($svc.Name) -n $K8sNamespace --timeout=180s
        Assert-LastExit "rollout status $($svc.Name)(新 Pod 未就绪,查:kubectl describe/logs)"
    }
    Assert-LocalDsAuthImageDigestAnnotations -KubeContext $mkCtx -MinikubeProfile $mkProfile
    Remove-LegacyPandoraConfigMapAfterRollout -KubeContext $mkCtx

    Write-Step "[7.5/8] 集群内客户端面边缘 Envoy(pandora-edge-envoy,NodePort 31443)"
    # 2026-07-28 P2:客户端面 8443 进集群,退役宿主 compose envoy + 22 条 port-forward 桥。
    # 客户端仍连 127.0.0.1:8443:由 minikube 节点容器的端口发布(PANDORA_MINIKUBE_PORTS,
    # 创建时 --ports=127.0.0.1:8443:31443)转到 NodePort。旧集群没有该端口发布时本步照常部署,
    # e2e_k8s.ps1 会探测不到宿主 8443 发布而自动回落宿主桥接(他人路径不变)。
    $edgeCertPem = Join-Path $envoyCertDir 'cert.pem'
    $edgeKeyPem  = Join-Path $envoyCertDir 'key.pem'
    # 无条件校验(函数自身幂等):证书不只会"缺失",还会"过期于旧地址"——本机 IP 随 DHCP 变后,
    # 旧证书 SAN 仍是老 IP,文件俱在却让客户端 TLS 主机名校验失败。只在文件缺失时才校验,
    # 等于永远发现不了这种情况。
    # envoy_cert.ps1 是函数库(dot-source 后调 Confirm-EnvoyDevCert 才会校验/mkcert 重签);
    # 以 -File 直跑只定义函数就退出,是空操作——证书需要重签时必须走真实生成路径。
    . (Join-Path $ScriptDir 'envoy_cert.ps1')
    Confirm-EnvoyDevCert -EnvoyDir $envoyCertDir
    if (-not (Test-Path $edgeCertPem) -or -not (Test-Path $edgeKeyPem)) {
        throw 'Envoy dev 证书(deploy/envoy/cert.pem + key.pem)缺失且自动重签失败;边缘 Envoy 无法起 TLS,已中止。多半是 mkcert 不可用:winget install FiloSottile.mkcert 后重跑。'
    }
    # 边缘 Envoy 镜像钉定 tag(edge-envoy.yaml),新节点/新机器没有时从宿主 load 或联网补齐。
    Ensure-EnvoyImageInMinikube -MinikubeProfile $mkProfile -Image 'envoyproxy/envoy:v1.38.1'
    $edgeCfgText = Convert-EdgeEnvoyConfigForCluster -Text (Get-Content -LiteralPath $hostEnvoyConfigFile -Raw) -Namespace $K8sNamespace
    $edgeCfgTmp = Join-Path ([System.IO.Path]::GetTempPath()) "pandora-edge-envoy-$PID.yaml"
    Set-Content -LiteralPath $edgeCfgTmp -Value $edgeCfgText -Encoding utf8NoBOM
    kubectl @kubectlContextArgs create configmap pandora-edge-envoy-config --from-file=envoy.yaml=$edgeCfgTmp -n $K8sNamespace `
        --dry-run=client -o yaml | kubectl @kubectlContextArgs apply -f -
    Assert-LastExit 'kubectl apply configmap pandora-edge-envoy-config'
    Remove-Item $edgeCfgTmp -Force -ErrorAction SilentlyContinue
    kubectl @kubectlContextArgs create secret generic pandora-edge-envoy-certs `
        --from-file=cert.pem=$edgeCertPem --from-file=key.pem=$edgeKeyPem -n $K8sNamespace `
        --dry-run=client -o yaml | kubectl @kubectlContextArgs apply -f -
    Assert-LastExit 'kubectl apply secret pandora-edge-envoy-certs'
    kubectl @kubectlContextArgs apply -f $edgeEnvoyYaml
    Assert-LastExit 'kubectl apply edge-envoy'
    # 配置/证书变更不触发 Pod 重建,与 DS 面同款:显式滚动 + 等就绪。
    kubectl @kubectlContextArgs rollout restart deploy/pandora-edge-envoy -n $K8sNamespace
    Assert-LastExit 'rollout restart pandora-edge-envoy'
    kubectl @kubectlContextArgs rollout status deploy/pandora-edge-envoy -n $K8sNamespace --timeout=120s
    if ($LASTEXITCODE -ne 0) { throw "pandora-edge-envoy 未在 120s 内就绪;客户端面 8443 无法接入,已中止(排障:kubectl -n pandora describe deploy/pandora-edge-envoy)。" }

    Write-Host ""
    Write-Ok "k8s 模式已部署。查看:kubectl get pods -n $K8sNamespace"

    Write-Step "[8/8] 宿主 Envoy 桥接 + UDP 中继(e2e_k8s.ps1)"
    # 真 DS 闭环剩余的宿主侧步骤(port-forward 桥接 + docker envoy + udp-relay)直接接进一键启动。
    # DS 镜像已由 [4/8] Build-DsImagesForMinikube 直接构建进 minikube(宿主 docker 无该 tag),
    # 故传 -SkipImageLoad 跳过 e2e 的宿主->minikube image load。
    # 用独立子进程跑,与 DS 镜像构建同一隔离理由;profile 使用本次运行固定值。
    $mkProfile = Get-K8sManagedProfile
    $relayBind = if ($script:K8sRelayBindHost) { $script:K8sRelayBindHost } else { '127.0.0.1' }
    # k8s GameServer 经集群内 pandora-envoy 回连,宿主 DS 面永远无需对局域网开放。
    # 显式覆盖父环境遗留值,不能只依赖 compose 默认值。
    $env:PANDORA_DS_EDGE_BIND_HOST = '127.0.0.1'
    & pwsh -NoProfile -ExecutionPolicy Bypass -File (Join-Path $ScriptDir 'e2e_k8s.ps1') -SkipImageLoad -MinikubeProfile $mkProfile -KubeContext $mkCtx -RelayBindHost $relayBind
    Assert-LastExit "宿主桥接/中继(e2e_k8s.ps1);集群本身已部署好,修复后可单独重跑:pwsh tools/scripts/e2e_k8s.ps1 -SkipImageLoad -MinikubeProfile $mkProfile -KubeContext $mkCtx -RelayBindHost $relayBind"

    Write-Host ""
    # 8443 实际可达面取决于接入路径:集群内边缘 Envoy 时看节点端口发布的绑定地址(回环=仅本机),
    # 宿主桥路径看 PANDORA_EDGE_BIND_HOST。按 docker port 实测如实提示,不再无条件宣称局域网可达。
    $edgePublish = [regex]::Match((docker port $mkProfile 2>$null | Out-String), '(?m)^31443/tcp\s*->\s*(\S+):8443')
    $edgeLoopbackOnly = ($edgePublish.Success -and $edgePublish.Groups[1].Value -notin @('0.0.0.0', '[::]', '::'))
    if ($relayBind -eq '0.0.0.0' -and -not $edgeLoopbackOnly) {
        Write-Ok "真 DS 闭环就绪(局域网):内网其它机器客户端连 ${k8sAdvHost}:8443(TLS)即可登录进 Hub/战斗。"
        Write-Info "若其它机器连不进,先查本机防火墙是否放行 入站 TCP 8443 + 入站 UDP 7000-8000。宿主 DS 面 8444 仅回环可达。"
    } elseif ($relayBind -eq '0.0.0.0') {
        Write-Ok "真 DS 闭环就绪(仅本机业务面):客户端连 127.0.0.1:8443(TLS);UDP 中继已对局域网开放。"
        Write-Warn "集群内边缘 8443 端口发布为回环,内网其它机器连不到业务面;需多机时 `$env:PANDORA_MINIKUBE_PORTS='0.0.0.0:8443:31443' 后 -Reset 重建。"
    } else {
        Write-Ok "真 DS 闭环就绪:客户端连 127.0.0.1:8443(TLS)即可登录进 Hub/战斗(仅本机)。"
    }
}

# ===== online 模式(远端 k8s:-Env test 测试服集群 / prod 生产 kbs 集群)=====
function Invoke-Online {
    $overlay     = Join-Path $ProjectRoot 'deploy/k8s/overlays/online'

    # 安全:Env 与预先配置的 context 一一绑定，不能只信用户临时传入的 -Env。
    # 这样即使人在生产 context 上漏写 -Env prod，也会在任何变更前失败。
    $ctx = (kubectl config current-context 2>$null | Out-String).Trim()
    $expectedCtx = if ($Env -eq 'prod') { $ProdKubeContext } else { $TestKubeContext }
    if ([string]::IsNullOrWhiteSpace($expectedCtx)) {
        $paramName = if ($Env -eq 'prod') { '-ProdKubeContext / PANDORA_K8S_PROD_CONTEXT' } else { '-TestKubeContext / PANDORA_K8S_TEST_CONTEXT' }
        throw "online -Env $Env 必须配置期望 context($paramName)，禁止仅凭 current-context 猜目标集群。"
    }
    $expectedCtx = $expectedCtx.Trim()
    if ($ctx -cne $expectedCtx) {
        throw "online -Env $Env 只允许 kube-context『$expectedCtx』，当前却是『$ctx』，已在变更前中止。"
    }
    if (-not [string]::IsNullOrWhiteSpace($TestKubeContext) -and
        -not [string]::IsNullOrWhiteSpace($ProdKubeContext) -and
        $TestKubeContext.Trim() -ceq $ProdKubeContext.Trim()) {
        throw 'TestKubeContext 与 ProdKubeContext 不得相同，否则环境隔离与生产二次确认可被绕过。'
    }
    # 后续所有命令直接使用映射值，而不是再次依赖可变 current-context。
    $ctx = $expectedCtx
    $kubectlContextArgs = @('--context', $ctx)
    Write-Step "online 模式:-Env $Env  目标 kube-context = $ctx"
    if ($Env -eq 'prod') {
        Write-Warn "⚠️ 这是【生产 kbs 集群】部署。请确认当前 context『$ctx』确为生产集群。"
    } else {
        Write-Info "这是【测试服集群】部署。"
    }
    Write-Warn "这会对『$ctx』集群做变更。确认无误请输入该 context 名字以继续:"
    $confirm = Read-Host "  输入 context 名"
    if ($confirm -cne $ctx) {
        Write-Err "输入与当前 context 不一致,已中止(防误操作)。"
        return
    }
    if ($Env -eq 'prod') {
        $p = Read-Host "  生产环境二次确认,请输入大写 PROD 继续"
        if ($p -cne 'PROD') { Write-Err "生产二次确认失败,已中止。"; return }
    }

    if ($Down) {
        Write-Step "删除 online 业务服务 + Agones Fleet/RBAC($Env)"
        # 业务 overlay(pandora 命名空间 22 个 Deployment/Service/netpol 等)
        kubectl @kubectlContextArgs delete -k $overlay --ignore-not-found
        Assert-LastExit 'kubectl delete -k overlays/online'
        # 启动时 Apply-AgonesManifests 还 apply 了 Fleet(default ns)与 allocator RBAC,否则 Down 后
        # DS GameServer 继续占集群资源、孤儿 RBAC 残留(回应审核 P1:Down 只删业务 overlay)。
        # ⚠️ 删 Fleet 会终止在跑的战斗/大厅 DS —— Down 本身就是下线动作,语义一致。
        $downAgonesDir = Join-Path $ProjectRoot 'deploy/k8s/agones'
        # 25-fleetautoscaler 放最前:先停 autoscaler 再删 Fleet,避免删除期间 buffer 补建 GameServer。
        foreach ($f in @('25-fleetautoscaler-battle.yaml', '20-fleet-battle.yaml', '21-fleet-battle-canary.yaml', '30-fleet-hub.yaml', '31-fleet-hub-canary.yaml', '10-rbac-allocator.yaml')) {
            kubectl @kubectlContextArgs delete -f (Join-Path $downAgonesDir $f) --ignore-not-found
            Assert-LastExit "kubectl delete $f"
        }
        Write-Ok "online($Env)已下线(业务 overlay + Fleet + RBAC)。"
        return
    }

    # Shared with activate_ds_auth.ps1. Until the platform supplies a real
    # Redis/managed-service control-plane lease verifier, every online release
    # is stopped before build, registry push, kubectl apply, Secret, Fleet, or
    # Deployment mutation. A ConfigMap marker is not an execution lock.
    Assert-PandoraRedisTopologyChangeLockProvider `
        'online-before-build-push-apply' 'not-yet-authoritatively-locked' 'not-yet-authoritatively-bound'

    # 任何 Secret/Fleet/Deployment/registry 写入前先拒绝旧单轨 Fleet。Hub 无可靠的自动排空
    # 证明，所以这里只 fail-closed，永不在普通发布中代删。
    Assert-NoLegacyDsFleets -KubeContext $ctx
    Assert-NoLegacyDSTicketSignerSecret -KubeContext $ctx

    if (-not $Registry -or -not $Tag) {
        throw "online 模式必须指定 -Registry 和 -Tag(Go 服务镜像来源)。"
    }
    if (-not $BattleDsImage -or -not $HubDsImage -or -not $DsGatewayAddr) {
        throw "online 模式必须指定 -BattleDsImage / -HubDsImage / -DsGatewayAddr，避免把本地 dev 镜像或错误网关地址带到远端集群。"
    }
    if ($BattleCanaryPercent -gt 0 -and ([string]::IsNullOrWhiteSpace($CanaryBattleDsImage) -or $BattleCanaryReplicas -lt 1)) {
        throw 'BattleCanaryPercent > 0 时必须提供 -CanaryBattleDsImage 且 -BattleCanaryReplicas >= 1。'
    }
    if ($HubCanaryPercent -gt 0 -and ([string]::IsNullOrWhiteSpace($CanaryHubDsImage) -or $HubCanaryReplicas -lt 1)) {
        throw 'HubCanaryPercent > 0 时必须提供 -CanaryHubDsImage 且 -HubCanaryReplicas >= 1。'
    }
    # cohort seed 是发布状态，不是密钥。从已有 pandora-config 只读回收它，防普通
    # 发布因漏传参数把 seed 清空、导致灰度 cohort 漂移。旧权重>0 时禁止换 seed；
    # 先降两轨权重到 0 并验收后，下一次 campaign 才可显式换 seed。
    $existingConfig = $null
    $existingConfigLines = @(& kubectl @kubectlContextArgs get secret/pandora-config -n $K8sNamespace --ignore-not-found -o json 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "读取现有 pandora-config 检查 Canary cohort 失败:$($existingConfigLines -join [Environment]::NewLine)" }
    $existingConfigText = (($existingConfigLines | ForEach-Object { $_.ToString() }) -join "`n").Trim()
    if (-not [string]::IsNullOrWhiteSpace($existingConfigText)) {
        try { $existingConfig = $existingConfigText | ConvertFrom-Json -ErrorAction Stop }
        catch { throw "pandora-config 回读不是合法 JSON:$($_.Exception.Message)" }
        try {
            $oldBattleConfig = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String([string]$existingConfig.data.'ds-allocator.yaml'))
            $oldHubConfig = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String([string]$existingConfig.data.'hub-allocator.yaml'))
        } catch { throw "pandora-config 的 allocator 配置 base64 非法:$($_.Exception.Message)" }
        $oldCanary = Get-PandoraCanaryConfigContract -BattleConfig $oldBattleConfig -HubConfig $oldHubConfig
        if ([string]::IsNullOrWhiteSpace($CanarySeed) -and -not [string]::IsNullOrWhiteSpace($oldCanary.Seed)) {
            $CanarySeed = $oldCanary.Seed
            Write-Info '未显式传 CanarySeed，已从现有集群配置只读继承（不打印 seed）。'
        }
        if (($oldCanary.BattlePercent -gt 0 -or $oldCanary.HubPercent -gt 0) -and
            $CanarySeed -cne $oldCanary.Seed) {
            throw '现有 Canary 权重非 0 时禁止更换 cohort seed。请先用原 seed 将 Battle/Hub 权重降为 0 并验收，再开新 campaign。'
        }
    }
    if (($BattleCanaryPercent -gt 0 -or $HubCanaryPercent -gt 0) -and
        $CanarySeed -cnotmatch '^[A-Za-z0-9._-]{8,128}$') {
        throw '启用 DS Canary 时必须提供稳定的 -CanarySeed / PANDORA_DS_CANARY_SEED(8..128 字符，仅字母数字._-)。'
    }
    $currentCommit = ((& git -C $ProjectRoot rev-parse --short=7 HEAD 2>$null) | Out-String).Trim().ToLowerInvariant()
    if ($LASTEXITCODE -ne 0) { throw 'online 无法读取当前 git commit，不能生成不可变发布 tag。' }
    Assert-PandoraImmutableReleaseTag -Tag $Tag -CurrentCommit $currentCommit -RequireCurrentCommit:$BuildPush

    # ===== 生产密钥预检(P0 安全审核)=====
    # 必须在 BuildPush(推镜像到远端 registry)之前就确认玩家、DS callback、Match resume、
    # Team resume 与 allocation abort 服务身份五套真密钥齐全 —— 否则可能镜像已推、稍后
    # gen -Prod 才因缺密钥失败,留下「半推 + 未部署」的脏状态。玩家面 / DS 回调面必须分离:
    # 同一把密钥覆盖玩家 JWT 与 DS 回调令牌时,泄露玩家面即可伪造 DS 回调绕过范围绑定。
    $devPubSecret = 'pandora-dev-jwt-secret-change-me-32!'
    $devMatchResumeSecret = 'pandora-dev-match-resume-auth-key-v1!'
    $devTeamResumeSecret = 'pandora-dev-team-resume-auth-key-v1!'
    $devAllocationAbortSecret = 'pandora-dev-allocation-abort-auth-key-v1!'
    $playerSec = $env:PANDORA_JWT_SECRET
    $dsSec = $env:PANDORA_DS_JWT_SECRET
    $matchResumeSec = $env:PANDORA_MATCH_RESUME_AUTH_SECRET
    $teamResumeSec = $env:PANDORA_TEAM_RESUME_AUTH_SECRET
    $allocationAbortSec = $env:PANDORA_ALLOCATION_ABORT_AUTH_SECRET
    # additional_secrets 部分接线仅供非生产验证；两份轮换决策未拍板前 online 直接拒绝。
    $playerSecAdd = $env:PANDORA_JWT_SECRET_ADDITIONAL
    $dsSecAdd = $env:PANDORA_DS_JWT_SECRET_ADDITIONAL
    if (-not [string]::IsNullOrWhiteSpace($playerSecAdd) -or -not [string]::IsNullOrWhiteSpace($dsSecAdd)) {
        throw 'online 暂不允许 PANDORA_*_SECRET_ADDITIONAL：玩家/DS 轮换决策仍待人拍板，Edge 双 key 证明与统一 key-set gate 尚未获批准。' +
              '当前只允许在非生产环境验证部分接线；批准决策并补齐阶段验收后再开放生产轮换。'
    }
    $secretChecks = @(
        @{ n = 'PANDORA_JWT_SECRET(玩家面)';    v = $playerSec; required = $true },
        @{ n = 'PANDORA_DS_JWT_SECRET(DS 回调面)'; v = $dsSec; required = $true },
        @{ n = 'PANDORA_MATCH_RESUME_AUTH_SECRET(Login→Matchmaker 服务身份)'; v = $matchResumeSec; required = $true },
        @{ n = 'PANDORA_TEAM_RESUME_AUTH_SECRET(Team→Matchmaker 服务身份)'; v = $teamResumeSec; required = $true },
        @{ n = 'PANDORA_ALLOCATION_ABORT_AUTH_SECRET(Matchmaker→DS 销毁服务身份)'; v = $allocationAbortSec; required = $true },
        @{ n = 'PANDORA_JWT_SECRET_ADDITIONAL(玩家面轮换兼容密钥)';    v = $playerSecAdd; required = $false },
        @{ n = 'PANDORA_DS_JWT_SECRET_ADDITIONAL(DS 回调面轮换兼容密钥)'; v = $dsSecAdd; required = $false })
    foreach ($p in $secretChecks) {
        if ([string]::IsNullOrWhiteSpace($p.v)) {
            if ($p.required) {
                throw "online -Prod 部署必须先设环境变量 $($p.n)(真 HS256 密钥);缺失已中止,不推镜像不部署(P0)。"
            }
            continue
        }
        if ($p.v -eq $devPubSecret) { throw "$($p.n) 不能等于公开 dev 密钥,请换成真密钥。" }
        if ($p.v -eq $devMatchResumeSecret) { throw "$($p.n) 不能等于公开 Match resume dev 密钥,请换成真密钥。" }
        if ($p.v -eq $devTeamResumeSecret) { throw "$($p.n) 不能等于公开 Team resume dev 密钥,请换成真密钥。" }
        if ($p.v -eq $devAllocationAbortSecret) { throw "$($p.n) 不能等于公开 allocation abort dev 密钥,请换成真密钥。" }
        if ([System.Text.Encoding]::UTF8.GetByteCount($p.v) -lt 32) { throw "$($p.n) 至少需要 32 字节(HS256)。" }
        # C0/C1 控制字符防线(二审 #12):换行/回车/制表等混进密钥会被 YAML 双引号转义后原样进服务,
        # 与运维手里的密钥「看起来相同实际不同」,导致全端验签静默失败。
        if ($p.v -match '[\x00-\x1F\x7F-\x9F]') {
            throw "$($p.n) 含控制字符(换行/回车/制表等),多半是复制粘贴事故;已中止(P0)。"
        }
    }
    # 所有已注入 HMAC 两两不同(P0:任一交叉 = 泄露一面即可伪造另一面)。
    $secretPairs = @($secretChecks | Where-Object { -not [string]::IsNullOrWhiteSpace($_.v) })
    for ($i = 0; $i -lt $secretPairs.Count; $i++) {
        for ($j = $i + 1; $j -lt $secretPairs.Count; $j++) {
            if ($secretPairs[$i].v -ceq $secretPairs[$j].v) {
                throw "$($secretPairs[$i].n) 与 $($secretPairs[$j].n) 不得相同(P0:玩家面/DS 回调面/新旧轮换密钥必须各自独立)。"
            }
        }
    }
    # owner 权威库 DSN(§9.22):online 部署 owner 服务,必须在推镜像前确认真 TiDB DSN 已提供。
    # dev 单机 MySQL 无复制天然线性一致仅限本地;MySQL 异步复制切换会回滚已确认写,
    # owner CAS 回滚即可能双 owner(脑裂)。细校验(库名/格式)由 gen_cluster_config.ps1 复核。
    $ownerStoreDsn = $env:PANDORA_OWNER_TIDB_DSN
    if ([string]::IsNullOrWhiteSpace($ownerStoreDsn)) {
        throw 'online -Prod 部署必须先设环境变量 PANDORA_OWNER_TIDB_DSN(owner 权威库真 TiDB DSN,pandora_owner 库;§9.22 确认写不回滚);缺失已中止,不推镜像不部署(P0)。'
    }
    if ($ownerStoreDsn -match '[\x00-\x1F\x7F-\x9F]') {
        throw 'PANDORA_OWNER_TIDB_DSN 含控制字符(换行/回车/制表等),多半是复制粘贴事故;已中止(P0)。'
    }
    if ($ownerStoreDsn.Contains('pandora_dev_pwd') -or $ownerStoreDsn.Contains('mysql:3306') -or $ownerStoreDsn.Contains('127.0.0.1:3307')) {
        throw 'PANDORA_OWNER_TIDB_DSN 指向公开 dev 凭据或 dev MySQL 地址;生产必须连真 TiDB(deploy/tidb-init/02-owner-tidb.sql)。'
    }
    # Online 使用本次调用独占的不可复用快照。共享 run/cluster/etc 在 Edge 探测/BuildPush 的长窗口内
    # 可能被 docker/k8s/Resume 重新生成成 dev/mock 配置，不能作为生产预检后的发布源。
    $onlineSnapshotRoot = [System.IO.Path]::GetFullPath((Join-Path $ProjectRoot 'run/cluster'))
    $onlineConfigDir = Join-Path $onlineSnapshotRoot "online-$Env-$([guid]::NewGuid().ToString('N'))"
    $onlineConfigTableBatch = $null
    $runtimeOverlayDir = ''
    $dsticketOperationLock = $null
    try {
    # 所有本地确定性检查必须先于 BuildPush。生成器在独立 staging 中完成 21 文件精确校验，
    # 发布失败会回滚旧集合；因此源配置缺失、磁盘错误、JWKS/密钥异常都不会等到推镜像后才暴露。
    Write-Step "预生成并完整校验线上集群配置(allocator=agones)"
    $genArgs = @('-OutDir', $onlineConfigDir, '-AllocatorMode', 'agones', '-Prod')
    if (-not [string]::IsNullOrWhiteSpace($DsAuthMode)) {
        $genArgs += @('-DsAuthMode', $DsAuthMode)
    }
    if ($DsAuthorityMode -ne 'redis') { throw 'online 必须显式传 -DsAuthorityMode redis。' }
    if ([string]::IsNullOrWhiteSpace($DsFenceEtcdEndpoints)) { throw 'online 必须提供 -DsFenceEtcdEndpoints 或 PANDORA_DS_AUTH_FENCE_ETCD_ENDPOINTS。' }
    if ([string]::IsNullOrWhiteSpace($DsFenceKeysetRevision)) { throw 'online 必须提供 -DsFenceKeysetRevision 或 PANDORA_DS_AUTH_KEYSET_REVISION。' }
    # DSTicket v2(方案 B):普通发布只读核对集群里已 bootstrap 的当前 immutable keyset，不在发布流程生成/改钥。
    # 当前普通发布门禁要求 JWKS 顶层 active_kid 与同 revision signer Secret 私钥一致，不接收
    # public-only stage；独立轮换工具即使预投递了其它 revision，也不能借普通 online 发布切换。
    # active kid 由 Secret 私钥推导，再与 revisioned JWKS 中同 kid 的 n/e 对账；keys[0] 顺序没有语义。
    if ($DsTicketKeysetRevision -cnotmatch '^[1-9][0-9]*$' -or [int64]$DsTicketKeysetRevision -gt [int]::MaxValue) {
        throw 'online 必须提供正整数 -DsTicketKeysetRevision / PANDORA_DSTICKET_KEYSET_REVISION。'
    }
    $dstTicketRevision = [int]$DsTicketKeysetRevision
    $dstTicketSignerSecretName = "pandora-dsticket-signer-r$dstTicketRevision"
    $dstTicketSecret = Get-KubectlJsonObject -KubeContext $ctx `
        -Arguments @('get', "secret/$dstTicketSignerSecretName", '-n', $K8sNamespace, '-o', 'json') `
        -Action "读取 immutable DSTicket signer Secret/$dstTicketSignerSecretName"
    $dstTicketKeyContract = Get-OnlineDSTicketKeyMaterialContract -KubeContext $ctx `
        -Revision $dstTicketRevision -ExpectedActiveKid $DsTicketActiveKid -SignerObject $dstTicketSecret
    $DsTicketActiveKid = $dstTicketKeyContract.ActiveKid
    Write-Ok "DSTicket immutable keyset 对账通过:signer=$dstTicketSignerSecretName keyset=r$dstTicketRevision kid=$DsTicketActiveKid keys=$($dstTicketKeyContract.KeyCount)。"
    $ordinaryDSTicketState = Assert-OnlineOrdinaryDSTicketState -KubeContext $ctx -RequestedRevision $dstTicketRevision
    if (-not [string]::IsNullOrWhiteSpace([string]$ordinaryDSTicketState.ActiveKid) -and
        [string]$ordinaryDSTicketState.ActiveKid -cne $DsTicketActiveKid) {
        throw '普通发布早期状态的 fixed/terminal active kid 与 immutable key material 不一致。'
    }
    Write-Ok "普通发布 DSTicket 早期只读门禁通过:r$dstTicketRevision state=$($ordinaryDSTicketState.State)。集群写入前将在互斥锁内重新采样。"
    $genArgs += @(
        '-DsAuthorityMode', 'redis', '-DsFenceEtcdEndpoints', $DsFenceEtcdEndpoints,
        '-DsFenceKeysetRevision', $DsFenceKeysetRevision, '-DsTicketActiveKid', $DsTicketActiveKid,
        '-DsTicketKeysetRevision', [string]$dstTicketRevision,
        '-BattleCanaryPercent', [string]$BattleCanaryPercent, '-HubCanaryPercent', [string]$HubCanaryPercent)
    if (-not [string]::IsNullOrWhiteSpace($CanarySeed)) { $genArgs += @('-CanarySeed', $CanarySeed) }
    & "$ScriptDir/gen_cluster_config.ps1" @genArgs
    Assert-LastExit '生成 online 候选配置'
    # 与含密钥 YAML 相同，配置表也在任何镜像构建/registry 等待前冻结为本次调用独占的
    # 内存快照。后续 apply 不再读取共享 configtable/dist，导表重跑不能撕裂本次发布。
    $onlineConfigTableBatch = Get-PandoraConfigTableCandidate -ConfigTableDir (Join-Path $ProjectRoot 'configtable/dist')
    Write-Ok "online 配置表候选已冻结:v$($onlineConfigTableBatch.Version) source_rev=$($onlineConfigTableBatch.SourceRev)。"

    # P1:普通发布不是换钥流程。必须比较 live Secret/pandora-config 与本次生成结果中实际落盘的
    # 玩家 Session / DS callback HMAC 指纹及 additional keyset；只比较完整 SHA256，不打印密钥
    # 或指纹。这样环境变量误换、生成器漂移、additional 被普通发布增删都会在 push/apply 前中止。
    $hmacServiceNames = @((Get-ServiceList) | ForEach-Object { [string]$_.Name })
    if ($null -ne $existingConfig) {
        $liveHmacConfigs = ConvertFrom-PandoraConfigSecretObject -SecretObject $existingConfig `
            -ExpectedServiceNames $hmacServiceNames
        $candidateHmacConfigs = Get-PandoraGeneratedConfigObject -ConfigDir $onlineConfigDir `
            -ExpectedServiceNames $hmacServiceNames
        Assert-PandoraOnlineHmacContinuity -LiveConfigs $liveHmacConfigs `
            -CandidateConfigs $candidateHmacConfigs | Out-Null
        Assert-PandoraOnlinePlacementContinuity -LiveConfigs $liveHmacConfigs `
            -CandidateConfigs $candidateHmacConfigs | Out-Null
        Assert-PandoraOnlineMatchResumeAuthContinuity -LiveConfigs $liveHmacConfigs `
            -CandidateConfigs $candidateHmacConfigs | Out-Null
        Assert-PandoraOnlineTeamResumeAuthContinuity -LiveConfigs $liveHmacConfigs `
            -CandidateConfigs $candidateHmacConfigs | Out-Null
        Assert-PandoraOnlineAllocationAbortAuthContinuity -LiveConfigs $liveHmacConfigs `
            -CandidateConfigs $candidateHmacConfigs | Out-Null
        Write-Ok '普通发布 HMAC 连续性门禁通过（玩家 Session / DS callback / placement proof / Match resume / Team resume / allocation abort service identity / additional keyset 均未变化；不打印指纹）。'
    } else {
        # 首次 bootstrap 没有 live baseline 可比较；后续普通发布一律走上面的连续性门禁。
        # 若运行中的业务对象仍在但 Secret 被删，不能把它误判成首次 bootstrap。
        $liveServiceLines = @(& kubectl @kubectlContextArgs get deployments -n $K8sNamespace --ignore-not-found -o name 2>&1)
        if ($LASTEXITCODE -ne 0) {
            $namespaceLines = @(& kubectl @kubectlContextArgs get namespace/$K8sNamespace --ignore-not-found -o name 2>&1)
            if ($LASTEXITCODE -ne 0 -or
                -not [string]::IsNullOrWhiteSpace((($namespaceLines | ForEach-Object { $_.ToString() }) -join '').Trim())) {
                throw 'live Secret/pandora-config 缺失且无法证明这是空的新集群；拒绝普通发布，须走独立恢复/换钥流程。'
            }
            $liveServiceLines = @()
        }
        $expectedLiveDeploymentNames = @($hmacServiceNames | ForEach-Object { "deployment.apps/$_" })
        $knownDeployments = @($liveServiceLines | ForEach-Object { $_.ToString().Trim() } |
            Where-Object { $expectedLiveDeploymentNames -ccontains $_ })
        if ($knownDeployments.Count -gt 0) {
            throw 'live Secret/pandora-config 缺失但业务 Deployment 已存在；拒绝把密钥基线丢失误判为首次部署，须走独立恢复/换钥流程。'
        }
        Write-Warn 'live Secret/pandora-config 不存在且未发现既有业务 Deployment：按首次 bootstrap 继续；本次生成结果将成为后续普通发布的 HMAC 基线。'
    }
    # 边缘网关 JWKS 校验门(P0/P1:gen 产出的 JWKS 无仓库内 Envoy 消费,生产客户端面 :8443 由外部边缘
    # 网关校验)。若边缘网关未同步玩家面密钥派生的 JWKS,login 用新密钥签的 SessionToken 会被边缘网关
    # 全拒。分两级 —— 优先**真实探测**(证明当前 edge 确实在用当前密钥),否则退回**运维承诺**(布尔):
    #   1) 设 PANDORA_EDGE_PROBE_URL(edge 上一条受 pandora_session JWT 保护的路由)→ 三段现签探测:
    #        a. 无 token          应 401(证明该路由确受 JWT 保护)
    #        b. 错误密钥签的 token 应 401(证明 edge 真在**验签**,而非只看 token 是否存在 → 排除假阳性)
    #        c. 当前玩家面密钥签的 token 应**非 401**(证明当前 edge 就在用当前密钥)
    #      三者缺一即 fail-closed 中止(任一不符 = 无法证明 edge 已用当前密钥)。
    #   2) 未提供探测 URL 时退回 PANDORA_EDGE_JWKS_ACK=1(仅运维书面承诺,不构成证明,打印当前密钥指纹供审计)。
    #   3) 两者都无 → fail-closed 中止。
    # 当前玩家面密钥指纹(SHA256 前 12 hex):用于日志审计,让「真实探测通过」与「运维 ACK」都能对上具体密钥。
    $playerKeyFp = ([System.BitConverter]::ToString(
            [System.Security.Cryptography.SHA256]::Create().ComputeHash(
                [System.Text.Encoding]::UTF8.GetBytes($playerSec))) -replace '-', '').Substring(0, 12).ToLower()
    $edgeProbeUrl = $env:PANDORA_EDGE_PROBE_URL
    if (-not [string]::IsNullOrWhiteSpace($edgeProbeUrl)) {
        # fail-closed(审核 P1 #7):生产探测链路必须 HTTPS。HTTP 明文探测可被中间人伪造出
        # 预期的 401/401/非401 响应,让脚本误判「edge 已切当前密钥」。非 https 直接中止。
        if ($edgeProbeUrl -notmatch '^(?i)https://') {
            throw "边缘 JWKS 探测:PANDORA_EDGE_PROBE_URL 必须是 https:// 链路(当前=$edgeProbeUrl)。" +
                  "生产不接受 HTTP 明文探测(可被 MITM 伪造 401/非401 响应制造假阳性)。"
        }
        # base64url(bytes):无填充、+/→-_。
        function ConvertTo-B64Url([byte[]]$b) { [Convert]::ToBase64String($b).TrimEnd('=').Replace('+', '-').Replace('/', '_') }
        # 用给定密钥现签一枚 60s 有效的 HS256 探测 JWT(iss/aud 对齐 login SessionToken 与 envoy provider)。
        function New-ProbeJwt([string]$secret) {
            $nowSec = [int][double]::Parse((Get-Date -UFormat %s))
            $hdrJson = '{"alg":"HS256","typ":"JWT"}'
            $plJson = '{"iss":"pandora-login","aud":"pandora-client","sub":"edge-jwks-probe","iat":' + $nowSec + ',"exp":' + ($nowSec + 60) + '}'
            $enc = [System.Text.Encoding]::UTF8
            $signingInput = (ConvertTo-B64Url $enc.GetBytes($hdrJson)) + '.' + (ConvertTo-B64Url $enc.GetBytes($plJson))
            $hmac = [System.Security.Cryptography.HMACSHA256]::new($enc.GetBytes($secret))
            try { $sig = $hmac.ComputeHash($enc.GetBytes($signingInput)) } finally { $hmac.Dispose() }
            return $signingInput + '.' + (ConvertTo-B64Url $sig)
        }
        $probeJwt = New-ProbeJwt $playerSec
        # 错误密钥:与真密钥不同的另一把(用于负向探测,证明 edge 真在验签)。
        $wrongJwt = New-ProbeJwt ($playerSec + '-WRONG-KEY-EDGE-PROBE-NEGATIVE-CONTROL')

        $reqArgs = @{ Uri = $edgeProbeUrl; Method = 'Get'; TimeoutSec = 10; SkipHttpErrorCheck = $true; ErrorAction = 'Stop' }
        if ($env:PANDORA_EDGE_PROBE_INSECURE -eq '1') {
            # fail-closed(审核 P1 #7):生产严禁跳过 TLS 证书校验。跳过后 MITM 可伪造 edge 响应制造
            # 假阳性,让脚本误判 edge 已切当前密钥。online -Prod 路径遇到该开关直接中止,不降级。
            throw "边缘 JWKS 探测:online -Prod 不允许 PANDORA_EDGE_PROBE_INSECURE=1(跳过 TLS 校验会被 MITM 伪造响应)。" +
                  "请让边缘网关提供受信任证书,或在受控内网用可验证的证书链;跳过校验仅限非生产链路。"
        }
        try {
            $noTok = Invoke-WebRequest @reqArgs
            $wrongTok = Invoke-WebRequest @reqArgs -Headers @{ Authorization = "Bearer $wrongJwt" }
            $withTok = Invoke-WebRequest @reqArgs -Headers @{ Authorization = "Bearer $probeJwt" }
        } catch {
            throw "边缘 JWKS 探测请求失败(URL=$edgeProbeUrl):$($_.Exception.Message)。修正 PANDORA_EDGE_PROBE_URL/网络后重试;不带证明不部署。"
        }
        if ($noTok.StatusCode -ne 401) {
            throw "边缘 JWKS 探测:无 token 访问 $edgeProbeUrl 返回 $($noTok.StatusCode)(应为 401)。该路由未受 pandora_session JWT 保护,无法据此验证 edge 密钥。请把 PANDORA_EDGE_PROBE_URL 指向受 JWT 保护的 edge 路由。"
        }
        if ($wrongTok.StatusCode -ne 401) {
            throw "边缘 JWKS 探测:错误密钥签的 token 访问 $edgeProbeUrl 返回 $($wrongTok.StatusCode)(应为 401)。edge 未对签名做校验(疑似只判 token 是否存在),探测结论不可信 → 中止。请检查边缘 Envoy 的 jwt_authn 配置。"
        }
        if ($withTok.StatusCode -eq 401) {
            throw "边缘 JWKS 探测:当前玩家面密钥(指纹 $playerKeyFp)现签 token 访问 $edgeProbeUrl 仍返回 401 —— 当前边缘 Envoy 用的不是 PANDORA_JWT_SECRET 派生的 JWKS。请先通过受控密钥管线或单独运行 gen_cluster_config.ps1 -Prod 生成并同步 JWKS 后再部署(本次独占快照退出时会清理；否则 login 新密钥签发的 SessionToken 会被全拒)。"
        }
        Write-Ok "边缘 JWKS 真实探测通过(密钥指纹 $playerKeyFp):无 token→401、错误密钥→401、当前密钥→$($withTok.StatusCode),证明当前 edge 就在用当前玩家面密钥且确在验签。"
    } elseif ($env:PANDORA_EDGE_JWKS_ACK -eq '1') {
        Write-Warn "PANDORA_EDGE_JWKS_ACK=1 仅为运维书面承诺(未做真实探测,不能证明当前 edge 已用当前密钥)。"
        Write-Warn "本次玩家面密钥指纹:$playerKeyFp —— 请人工核对边缘 Envoy 当前 JWKS 与该指纹一致。"
        Write-Warn "强烈建议改设 PANDORA_EDGE_PROBE_URL(受 pandora_session JWT 保护的 edge 路由)让脚本现签探测 token 实测。"
    } else {
        throw "online -Prod 需验证边缘 Envoy(客户端 :8443 JWT 校验)的 JWKS 已同步为 PANDORA_JWT_SECRET 派生值(当前密钥指纹 $playerKeyFp)。" +
              " 本仓库不含生产边缘网关(外部自带同名 pandora-envoy Service)。请先通过受控密钥管线或单独运行 gen_cluster_config.ps1 -Prod 生成并同步 JWKS" +
              " (本次独占快照退出时会清理),再设 PANDORA_EDGE_PROBE_URL 做真实探测(推荐),或确认后设 PANDORA_EDGE_JWKS_ACK=1;" +
              " 否则 login 新密钥签发的 SessionToken 会被边缘网关全部拒绝。"
    }
    Write-Ok "生产密钥预检通过:玩家面 / DS 回调面为两把独立真密钥,边缘 JWKS 已校验。"

    $writerServices = @('login', 'player-locator', 'ds-allocator', 'hub-allocator', 'battle-result')
    $secureDsAuthGoArgs = @()
    $onlineDsAuthState = $null
    $onlineDsAuthActivationEvidence = $null
    Write-Step '只读验证 DS auth required_writer_epoch 已显式建立'
    $requiredMin = 1
    $requiredMax = 2
    if ($Env -eq 'prod') {
        $null = Assert-PandoraDsAuthHttpsEndpoints $DsFenceEtcdEndpoints
        Assert-PandoraDsAuthEtcdRevision $DsFenceEtcdIdentityRevision
        $secureDsAuthGoArgs = @(Get-PandoraDsAuthSecureGoArgs $DsFenceEtcdAuditorCAFile `
            $DsFenceEtcdAuditorCertFile $DsFenceEtcdAuditorKeyFile $DsFenceEtcdServerName `
            $DsFenceEtcdAuditorIdentity $DsFenceEtcdIdentityRevision $DsFenceEtcdForbiddenReadPrefix `
            $DsFenceEtcdAuditorUsernameFile $DsFenceEtcdAuditorPasswordFile)
        # 普通生产发布只允许复用已完成一次性 1→2 激活的终态，绝不在发布流程调用 activate/换身份。
        $requiredMin = 2
        $requiredMax = 2
    }
    Push-Location $ProjectRoot
    try {
        & go run ./pkg/dsauthfence/cmd/dsauth-required --endpoints $DsFenceEtcdEndpoints `
            --min-epoch $requiredMin --max-epoch $requiredMax `
            --min-policy-generation 3 --max-policy-generation 3 --require-v3-activation-record @secureDsAuthGoArgs
        $requiredExit = $LASTEXITCODE
    } finally {
        Pop-Location
    }
    if ($requiredExit -ne 0) {
        throw 'DS auth required policy 不存在、不是精确 V3、非法或 etcd 不可线性读取；已在 BuildPush/Secret/Fleet/Deployment 前停止。' +
              '禁止把 missing/V1/V2 默认成 V3。fresh 本地集群只能走 zero-writer genesis；已有 V2 集群须先执行 immutable evidence 绑定的专用 staging→V3 CAS 流程。'
    }
    if ($Env -eq 'prod') {
        # 这是 ordinary release 的最早证据快照。后续锁内、config apply 后和
        # 终态都只能与它比较，不得在窗口中途重置 baseline。
        $onlineDsAuthActivationEvidence = Get-OnlineDsAuthActivationEvidenceState -KubeContext $ctx
    }
    # 2026-07-13 的真实本地验收只证明了旧 K1 镜像上的正向准入与篡改拒绝；加入票据日志
    # 脱敏入口后，重试的 UE 未到达 DS 认证入口，K1→K2→K2-only（含 K1 旧票耗尽）没有完成。
    # 同轮 Inventory / DS-terminal Istio 素材也仅为独立候选，未安装或激活。不得把静态清单、
    # 单阶段结果或 ready Fleet 冒充完整生产验收；在补齐真链路前保持所有 online 写入为零。
    throw 'online 零写阻断：DSTicket K1→K2→K2-only 真 Kubernetes/UE E2E 尚未完成，' +
          'Inventory/DS-terminal mesh 也未独立激活。当前不得 BuildPush、写 Secret/Fleet/Deployment 或发布生产。'

    if ($Env -eq 'prod') {
        Write-Step '任何 registry/K8s 写前只读验证 canonical green、blue=0、Endpoint UID 与运行时 capability/features'
        $onlineDsAuthState = Assert-OnlineDsAuthRuntimeAndCapabilities -KubeContext $ctx `
            -WriterServices $writerServices -Revision $DsFenceEtcdIdentityRevision `
            -ServerName $DsFenceEtcdServerName -ForbiddenReadPrefix $DsFenceEtcdForbiddenReadPrefix `
            -EtcdEndpoints $DsFenceEtcdEndpoints -KeysetRevision $DsFenceKeysetRevision `
            -SecureGoArgs $secureDsAuthGoArgs -ActivationEvidence $onlineDsAuthActivationEvidence
        Write-Ok '生产 DS auth mTLS/CN/AuthStatus/ACL、immutable identity、canonical green 与 capability/features 前置审计通过。'
    }

    # 上述真链路闭环并由 Claude 复审后，才允许移除零写门并进入以下 immutable digest 管线。
    $serviceNames = @((Get-ServiceList) | ForEach-Object { [string]$_.Name })
    $registryRoot = $Registry.Trim().TrimEnd('/')
    $goDigests = @{}
    $goPins = @{}
    $remoteRefs = @{}
    foreach ($name in $serviceNames) { $remoteRefs[$name] = "$registryRoot/pandora/${name}:$Tag" }

    if ($BuildPush) {
        Assert-CleanOnlineReleaseSource
        if ($env:PANDORA_OFFLINE -eq '1') {
            throw 'online BuildPush 禁止 PANDORA_OFFLINE=1：发布必须从当前 clean commit 严格重建，不能导入/复用离线包。'
        }
        throw 'online BuildPush 仍阻断：必须先在目标 registry 启用并由平台验证 native immutable-tag/create-only 策略与发布锁。HEAD 预检存在 TOCTOU，不能作为不可变证明；本次未 build、未 push。'
        Write-Step '预检 22 个不可变 tag 均不存在（任一鉴权/网络不确定即阻断）'
        foreach ($name in $serviceNames) { Assert-RemoteImageTagAbsent -Reference $remoteRefs[$name] }
        Write-Step "从当前 clean commit 严格重建并推送 22 个 Go 服务镜像到 $Registry"
        Build-AllImages -StrictRelease
        foreach ($name in $serviceNames) {
            Assert-LocalImageRevision -Reference "pandora/${name}:dev" -Expected $currentCommit
        }
        foreach ($svc in (Get-ServiceList)) {
            $local  = "pandora/$($svc.Name):dev"
            $remote = [string]$remoteRefs[$svc.Name]
            $descriptor = Push-ImageAndResolveDigest -Local $local -Remote $remote
            $goDigests[$svc.Name] = $descriptor.Digest
            $goPins[$svc.Name] = $descriptor.Pinned
        }
    } else {
        Write-Step '从 registry 解析 22 个 Go 服务镜像 digest（不按 tag 部署）'
        foreach ($name in $serviceNames) {
            $descriptor = Get-RegistryImageDescriptor -Reference $remoteRefs[$name]
            $goDigests[$name] = $descriptor.Digest
            $goPins[$name] = $descriptor.Pinned
        }
    }

    $battleDescriptor = Get-RegistryImageDescriptor -Reference $BattleDsImage
    $hubDescriptor = Get-RegistryImageDescriptor -Reference $HubDsImage
    $battleCanaryDescriptor = if ([string]::IsNullOrWhiteSpace($CanaryBattleDsImage)) {
        $battleDescriptor
    } else {
        Get-RegistryImageDescriptor -Reference $CanaryBattleDsImage
    }
    $hubCanaryDescriptor = if ([string]::IsNullOrWhiteSpace($CanaryHubDsImage)) {
        $hubDescriptor
    } else {
        Get-RegistryImageDescriptor -Reference $CanaryHubDsImage
    }
    foreach ($pair in @(
        @{ Input = $BattleDsImage; Desc = $battleDescriptor },
        @{ Input = $HubDsImage; Desc = $hubDescriptor },
        @{ Input = $CanaryBattleDsImage; Desc = $battleCanaryDescriptor },
        @{ Input = $CanaryHubDsImage; Desc = $hubCanaryDescriptor })) {
        if ([string]::IsNullOrWhiteSpace([string]$pair.Input)) { continue }
        $inputDigest = Get-PandoraImageDigestFromReference -Reference $pair.Input
        if (-not [string]::IsNullOrWhiteSpace($inputDigest) -and $inputDigest -cne $pair.Desc.Digest) {
            throw "DS 输入 pin 与 registry 回读不一致:$($pair.Input) input=$inputDigest registry=$($pair.Desc.Digest)"
        }
    }
    $battlePin = $battleDescriptor.Pinned
    $hubPin = $hubDescriptor.Pinned
    $battleCanaryPin = $battleCanaryDescriptor.Pinned
    $hubCanaryPin = $hubCanaryDescriptor.Pinned
    if (-not [string]::IsNullOrWhiteSpace($CanaryBattleDsImage) -and
        $battleCanaryDescriptor.Digest -ceq $battleDescriptor.Digest) {
        throw 'Battle Canary 必须使用与 Stable 不同的独立 digest；同 digest 无法证明灰度轨道。'
    }
    if (-not [string]::IsNullOrWhiteSpace($CanaryHubDsImage) -and
        $hubCanaryDescriptor.Digest -ceq $hubDescriptor.Digest) {
        throw 'Hub Canary 必须使用与 Stable 不同的独立 digest；同 digest 无法证明灰度轨道。'
    }
    # 不传 Canary 镜像 = 仅创建 0 Ready 的休眠轨道，清单仍用 Stable pin 便于结构验证。
    # 显式传图则允许 percent=0 预热，等 Ready 后再通过后续配置 rollout 放量。
    $battleCanaryReplicaApply = if ([string]::IsNullOrWhiteSpace($CanaryBattleDsImage)) { 0 } else { $BattleCanaryReplicas }
    $hubCanaryReplicaApply = if ([string]::IsNullOrWhiteSpace($CanaryHubDsImage)) { 0 } else { $HubCanaryReplicas }

    Write-Step '生成独占 runtime overlay，并验证 22 个 digest pin + 5 个 writer annotation'
    $runtimeOverlayArgs = @{
        SourceOverlay = $overlay; Registry = $registryRoot; Digests = $goDigests
        ServiceNames = $serviceNames; WriterServices = $writerServices; EnvironmentName = $Env
        DSTicketKeysetRevision = $dstTicketRevision
    }
    if ($Env -eq 'prod') {
        $runtimeOverlayArgs.EtcdIdentityRevision = $DsFenceEtcdIdentityRevision
        $runtimeOverlayArgs.EtcdServerName = $DsFenceEtcdServerName
        $runtimeOverlayArgs.EtcdForbiddenReadPrefix = $DsFenceEtcdForbiddenReadPrefix
        $runtimeOverlayArgs.EtcdPasswordAuthByService = $onlineDsAuthState.PasswordAuth
        $runtimeOverlayArgs.GreenDesiredReplicas = $onlineDsAuthState.DesiredReplicas
        $runtimeOverlayArgs.GreenDeploymentObjects = $onlineDsAuthState.GreenDeploymentObjects
        $runtimeOverlayArgs.CanonicalDsAuthGreen = $true
    }
    $runtimeOverlay = New-OnlineRuntimeOverlay @runtimeOverlayArgs
    $runtimeOverlayDir = $runtimeOverlay.Path
    foreach ($fleetCheck in @(
        @{ File = '20-fleet-battle.yaml'; Pin = $battlePin; Container = 'pandora-battle-ds'; Track = 'stable'; Name = 'pandora-battle-stable' },
        @{ File = '21-fleet-battle-canary.yaml'; Pin = $battleCanaryPin; Container = 'pandora-battle-ds'; Track = 'canary'; Name = 'pandora-battle-canary' },
        @{ File = '30-fleet-hub.yaml'; Pin = $hubPin; Container = 'pandora-hub-ds'; Track = 'stable'; Name = 'pandora-hub-stable' },
        @{ File = '31-fleet-hub-canary.yaml'; Pin = $hubCanaryPin; Container = 'pandora-hub-ds'; Track = 'canary'; Name = 'pandora-hub-canary' })) {
        $fleetRaw = Get-Content -LiteralPath (Join-Path $ProjectRoot "deploy/k8s/agones/$($fleetCheck.File)") -Raw
        $fleetRendered = Set-PandoraFleetImagePin -Manifest $fleetRaw -PinnedImage $fleetCheck.Pin
        $fleetRendered = Set-PandoraFleetDSTicketKeysetRevision -Manifest $fleetRendered -Revision $dstTicketRevision
        $fleetContractRows = Get-PandoraFleetContractRows -Manifest $fleetRendered
        Assert-PandoraFleetManifestContract -Manifest $fleetRendered -ContractRows $fleetContractRows `
            -PinnedImage $fleetCheck.Pin -ContainerName $fleetCheck.Container -ExpectedTrack $fleetCheck.Track `
            -ExpectedFleetName $fleetCheck.Name
    }

    # 本地构建/registry 解析均在锁外；通过全部生产硬门后，在第一笔 DSTicket
    # 相关集群写前原子 create-only 抢锁。rotation 同样使用此锁，消除 preflight->apply TOCTOU。
    $dsticketOperationLock = Enter-OnlineDSTicketOperationLock -KubeContext $ctx
    $null = Assert-OnlineDSTicketOperationLockHeld -KubeContext $ctx -Identity $dsticketOperationLock
    $lockedKeyContract = Get-OnlineDSTicketKeyMaterialContract -KubeContext $ctx `
        -Revision $dstTicketRevision -ExpectedActiveKid $DsTicketActiveKid
    if ($lockedKeyContract.ActiveKid -cne $dstTicketKeyContract.ActiveKid -or
        $lockedKeyContract.JwksSha256 -cne $dstTicketKeyContract.JwksSha256 -or
        $lockedKeyContract.PrivatePemSha256 -cne $dstTicketKeyContract.PrivatePemSha256) {
        throw '锁内重读 DSTicket key material 与早期预检不一致，拒绝写集群。'
    }
    $lockedOrdinaryState = Assert-OnlineOrdinaryDSTicketState -KubeContext $ctx -RequestedRevision $dstTicketRevision
    if (-not [string]::IsNullOrWhiteSpace([string]$lockedOrdinaryState.ActiveKid) -and
        [string]$lockedOrdinaryState.ActiveKid -cne $lockedKeyContract.ActiveKid) {
        throw '锁内 ordinary state active kid 与 immutable key material 不一致。'
    }
    if ([string]$lockedOrdinaryState.State -cne [string]$ordinaryDSTicketState.State) {
        throw "早期预检到锁内重验期间 DSTicket state 已变化:$($ordinaryDSTicketState.State) -> $($lockedOrdinaryState.State)；拒绝用旧候选覆盖。"
    }
    if ($Env -eq 'prod') {
        $lockedActivationEvidence = Get-OnlineDsAuthActivationEvidenceState -KubeContext $ctx
        Assert-OnlineDsAuthActivationEvidenceUnchanged $onlineDsAuthActivationEvidence $lockedActivationEvidence
    }
    $earlyFixedRV = if ($null -eq $existingConfig) { '' } else { [string]$existingConfig.metadata.resourceVersion }
    $lockedFixedConfig = Get-OnlineOptionalKubectlJsonObject -KubeContext $ctx `
        -Arguments @('get', 'secret/pandora-config', '-n', $K8sNamespace, '--ignore-not-found', '-o', 'json') `
        -Action '锁内重读 fixed Secret/pandora-config'
    $lockedFixedRV = if ($null -eq $lockedFixedConfig) { '' } else { [string]$lockedFixedConfig.metadata.resourceVersion }
    if ($earlyFixedRV -cne $lockedFixedRV -or
        ($null -ne $existingConfig -and [string]::IsNullOrWhiteSpace($earlyFixedRV)) -or
        ($null -ne $lockedFixedConfig -and [string]::IsNullOrWhiteSpace($lockedFixedRV))) {
        throw '早期预检到锁内重验期间 fixed pandora-config resourceVersion/存在性已改变；拒绝覆盖新发布。'
    }
    if ($null -ne $lockedFixedConfig) {
        $lockedHmacConfigs = ConvertFrom-PandoraConfigSecretObject -SecretObject $lockedFixedConfig `
            -ExpectedServiceNames $hmacServiceNames
        $lockedCandidateHmacConfigs = Get-PandoraGeneratedConfigObject -ConfigDir $onlineConfigDir `
            -ExpectedServiceNames $hmacServiceNames
        Assert-PandoraOnlineHmacContinuity -LiveConfigs $lockedHmacConfigs `
            -CandidateConfigs $lockedCandidateHmacConfigs | Out-Null
        Assert-PandoraOnlinePlacementContinuity -LiveConfigs $lockedHmacConfigs `
            -CandidateConfigs $lockedCandidateHmacConfigs | Out-Null
        Assert-PandoraOnlineMatchResumeAuthContinuity -LiveConfigs $lockedHmacConfigs `
            -CandidateConfigs $lockedCandidateHmacConfigs | Out-Null
        Assert-PandoraOnlineTeamResumeAuthContinuity -LiveConfigs $lockedHmacConfigs `
            -CandidateConfigs $lockedCandidateHmacConfigs | Out-Null
    }
    if ($Env -eq 'prod') {
        $preConfigActivationEvidence = Get-OnlineDsAuthActivationEvidenceState -KubeContext $ctx
        Assert-OnlineDsAuthActivationEvidenceUnchanged $onlineDsAuthActivationEvidence $preConfigActivationEvidence
        Write-Ok 'ordinary release 已确认 DS auth activation immutable evidence+switch 终态；不存在 activation/drain 半窗口。'
    }
    $null = Assert-OnlineDSTicketOperationLockHeld -KubeContext $ctx -Identity $dsticketOperationLock
    Write-Ok "DSTicket 锁内权威门禁通过:r$dstTicketRevision state=$($lockedOrdinaryState.State)。"
    Write-Step "应用已校验的 namespace 基线($K8sNamespace)"
    kubectl @kubectlContextArgs apply -f (Join-Path $ProjectRoot 'deploy/k8s/services/00-namespace.yaml')
    Assert-LastExit 'kubectl apply 00-namespace'
    $null = Assert-OnlineDSTicketOperationLockHeld -KubeContext $ctx -Identity $dsticketOperationLock
    # 生产配置含两把真 HS256 密钥,用 Secret 承载(P0:严禁把真密钥写进明文 ConfigMap)。
    Apply-PandoraConfigSecret -KubeContext $ctx -ConfigDir $onlineConfigDir -Action 'kubectl apply secret pandora-config'
    $null = Apply-PandoraConfigTableConfigMap -KubeContext $ctx -Action 'kubectl apply configmap pandora-configtable' `
        -Candidate $onlineConfigTableBatch
    if ($Env -eq 'prod') {
        $postConfigActivationEvidence = Get-OnlineDsAuthActivationEvidenceState -KubeContext $ctx
        Assert-OnlineDsAuthActivationEvidenceUnchanged $onlineDsAuthActivationEvidence $postConfigActivationEvidence
    }

    Write-Step "apply Agones RBAC + Fleet(真 Linux DS)"
    # 线上 Agones 通常已由集群管理员预装;此处不自动 helm install,只 apply 业务 RBAC/Fleet
    kubectl @kubectlContextArgs get ns agones-system *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-Warn "未检测到 agones-system —— 线上 Agones 须由集群管理员预先安装,否则 Fleet/分配不可用。"
    }
    $null = Assert-OnlineDSTicketOperationLockHeld -KubeContext $ctx -Identity $dsticketOperationLock
    Apply-AgonesManifests -BattleDsImage $battlePin -HubDsImage $hubPin `
        -CanaryBattleDsImage $battleCanaryPin -CanaryHubDsImage $hubCanaryPin `
        -BattleCanaryReplicaCount $battleCanaryReplicaApply -HubCanaryReplicaCount $hubCanaryReplicaApply `
        -BattleMaxReplicasOverride $BattleMaxReplicas -BattleBufferSizeOverride $BattleBufferSize `
        -DSTicketKeysetRevision $dstTicketRevision -DsGatewayAddr $DsGatewayAddr -DsGatewayTls $DsGatewayTls `
        -KubeContext $ctx

    Write-Step "kubectl apply -k runtime online overlay($Env,digest pinned)"
    $null = Assert-OnlineDSTicketOperationLockHeld -KubeContext $ctx -Identity $dsticketOperationLock
    kubectl @kubectlContextArgs apply -k $runtimeOverlayDir
    Assert-LastExit 'kubectl apply -k runtime online overlay'
    if ($Env -eq 'prod') {
        # green 是 epoch=2 后唯一 canonical writer；blue 已由 runtime overlay 固定 replicas=0。
        # patch 携带预检 resourceVersion + exact immutable selector/desired，冲突即停，不回切 blue。
        foreach ($writer in $writerServices) {
            $null = Assert-OnlineDSTicketOperationLockHeld -KubeContext $ctx -Identity $dsticketOperationLock
            $greenPatchPath = [string]$runtimeOverlay.GreenPatchPaths[$writer]
            if ([string]::IsNullOrWhiteSpace($greenPatchPath) -or -not (Test-Path -LiteralPath $greenPatchPath -PathType Leaf)) {
                throw "缺 canonical green/$writer release patch。"
            }
            kubectl @kubectlContextArgs replace -f $greenPatchPath
            Assert-LastExit "CAS replace canonical green Deployment/$writer-ds-auth-green"
        }
    }

    # Secret 传播(审核 P1 #4):pandora-config 以 subPath 挂载,Secret 内容更新不会热感知;
    # 且镜像 tag 不变时 apply -k 判定 pod 模板 unchanged 不触发 rollout → Pod 继续用旧密钥/旧配置。
    # 故显式按名 rollout restart 22 个业务 Deployment,强制重挂最新 Secret(服务支持 SIGTERM 排空,
    # 滚动重启零停机,见 CLAUDE.md §9 不变量 16),再等关键服务就绪确认新配置生效。
    Write-Step "rollout restart 业务 Deployment(传播更新后的 pandora-config Secret)"
    foreach ($svc in (Get-ServiceList)) {
        $null = Assert-OnlineDSTicketOperationLockHeld -KubeContext $ctx -Identity $dsticketOperationLock
        $deploymentName = if ($Env -eq 'prod' -and $writerServices -contains [string]$svc.Name) { "$($svc.Name)-ds-auth-green" } else { [string]$svc.Name }
        kubectl @kubectlContextArgs rollout restart "deploy/$deploymentName" -n $K8sNamespace
        Assert-LastExit "rollout restart $deploymentName(Secret 传播)"
    }
    foreach ($svc in (Get-ServiceList)) {
        $deploymentName = if ($Env -eq 'prod' -and $writerServices -contains [string]$svc.Name) { "$($svc.Name)-ds-auth-green" } else { [string]$svc.Name }
        kubectl @kubectlContextArgs rollout status "deploy/$deploymentName" -n $K8sNamespace --timeout=180s
        Assert-LastExit "rollout status $deploymentName(Secret 传播后未就绪,查:kubectl describe/logs)"
    }
    Assert-OnlineDeploymentImageState -KubeContext $ctx -Pins $goPins -Digests $goDigests `
        -WriterServices $writerServices -CanonicalGreen:($Env -eq 'prod')
    if ($Env -eq 'prod') {
        Write-Step '终态复核 canonical green/blue=0/Endpoint UID 与 runtime capability/features'
        $onlineDsAuthState = Assert-OnlineDsAuthRuntimeAndCapabilities -KubeContext $ctx `
            -WriterServices $writerServices -Revision $DsFenceEtcdIdentityRevision `
            -ServerName $DsFenceEtcdServerName -ForbiddenReadPrefix $DsFenceEtcdForbiddenReadPrefix `
            -EtcdEndpoints $DsFenceEtcdEndpoints -KeysetRevision $DsFenceKeysetRevision `
            -SecureGoArgs $secureDsAuthGoArgs -ExpectedDigests $goDigests `
            -ActivationEvidence $onlineDsAuthActivationEvidence
        Write-Ok 'canonical green 普通发布终态审计通过；无 blue writer、无额外 capability。'
    }
    Wait-OnlineReadyFleetImageState -KubeContext $ctx -Fleet 'pandora-battle-stable' -Container 'pandora-battle-ds' `
        -Pin $battlePin -Digest $battleDescriptor.Digest -ExpectedTrack stable
    Wait-OnlineReadyFleetImageState -KubeContext $ctx -Fleet 'pandora-battle-canary' -Container 'pandora-battle-ds' `
        -Pin $battleCanaryPin -Digest $battleCanaryDescriptor.Digest -ExpectedTrack canary
    Wait-OnlineReadyFleetImageState -KubeContext $ctx -Fleet 'pandora-hub-stable' -Container 'pandora-hub-ds' `
        -Pin $hubPin -Digest $hubDescriptor.Digest -ExpectedTrack stable
    Wait-OnlineReadyFleetImageState -KubeContext $ctx -Fleet 'pandora-hub-canary' -Container 'pandora-hub-ds' `
        -Pin $hubCanaryPin -Digest $hubCanaryDescriptor.Digest -ExpectedTrack canary
    $null = Assert-OnlineDSTicketOperationLockHeld -KubeContext $ctx -Identity $dsticketOperationLock
    $finalKeyContract = Get-OnlineDSTicketKeyMaterialContract -KubeContext $ctx `
        -Revision $dstTicketRevision -ExpectedActiveKid $DsTicketActiveKid
    if ($finalKeyContract.JwksSha256 -cne $lockedKeyContract.JwksSha256 -or
        $finalKeyContract.PrivatePemSha256 -cne $lockedKeyContract.PrivatePemSha256) {
        throw '普通发布验收时 DSTicket immutable key material 漂移。'
    }
    $finalOrdinaryState = Assert-OnlineOrdinaryDSTicketState -KubeContext $ctx -RequestedRevision $dstTicketRevision
    if ([string]::IsNullOrWhiteSpace([string]$finalOrdinaryState.ActiveKid) -or
        [string]$finalOrdinaryState.ActiveKid -cne $finalKeyContract.ActiveKid) {
        throw '普通发布终态 fixed/terminal active kid 与 immutable key material 不一致。'
    }
    if ($Env -eq 'prod') {
        $finalActivationEvidence = Get-OnlineDsAuthActivationEvidenceState -KubeContext $ctx
        Assert-OnlineDsAuthActivationEvidenceUnchanged $onlineDsAuthActivationEvidence $finalActivationEvidence
    }
    $null = Assert-OnlineDSTicketOperationLockHeld -KubeContext $ctx -Identity $dsticketOperationLock
    Write-Ok "DSTicket 普通发布终态验收通过:r$dstTicketRevision state=$($finalOrdinaryState.State)。"
    Remove-LegacyPandoraConfigMapAfterRollout -KubeContext $ctx

    } finally {
        try {
        if (-not [string]::IsNullOrWhiteSpace($runtimeOverlayDir) -and (Test-Path -LiteralPath $runtimeOverlayDir -PathType Container)) {
            $resolvedRuntime = [System.IO.Path]::GetFullPath($runtimeOverlayDir)
            $runtimeParent = [System.IO.Path]::GetFullPath((Join-Path $ProjectRoot 'deploy/k8s/overlays'))
            $safeRuntimeParent = (Split-Path -Parent $resolvedRuntime).Equals($runtimeParent, [System.StringComparison]::OrdinalIgnoreCase)
            $safeRuntimeLeaf = (Split-Path -Leaf $resolvedRuntime) -match "^\.online-runtime-$([regex]::Escape($Env))-[0-9a-f]{32}$"
            if (-not $safeRuntimeParent -or -not $safeRuntimeLeaf) {
                throw "拒绝清理未经验证的 runtime overlay 路径:$resolvedRuntime"
            }
            try { Remove-Item -LiteralPath $resolvedRuntime -Recurse -Force }
            catch { throw "online 流程已结束,但 runtime overlay 清理失败:$resolvedRuntime;$($_.Exception.Message)" }
        }
        if (Test-Path -LiteralPath $onlineConfigDir -PathType Container) {
            $resolvedSnapshot = [System.IO.Path]::GetFullPath($onlineConfigDir)
            $resolvedParent = Split-Path -Parent $resolvedSnapshot
            $safeParent = $resolvedParent.Equals($onlineSnapshotRoot, [System.StringComparison]::OrdinalIgnoreCase)
            $safeLeaf = (Split-Path -Leaf $resolvedSnapshot) -match "^online-$([regex]::Escape($Env))-[0-9a-f]{32}$"
            if (-not $safeParent -or -not $safeLeaf) {
                throw "拒绝清理未经验证的 online 配置快照路径:$resolvedSnapshot"
            }
            try { Remove-Item -LiteralPath $resolvedSnapshot -Recurse -Force }
            catch { throw "online 流程已结束,但含生产密钥的临时配置快照清理失败:$resolvedSnapshot;$($_.Exception.Message)" }
        }
        } finally {
            if ($null -ne $dsticketOperationLock) {
                Exit-OnlineDSTicketOperationLock -KubeContext $ctx -Identity $dsticketOperationLock
                $dsticketOperationLock = $null
            }
        }
    }
    Write-Host ""
    Write-Ok "online($Env)部署已提交且临时密钥快照已清理。查看:kubectl --context $ctx get pods -n $K8sNamespace"
}

# ===== 共享:服务清单 / 镜像构建 =====
function Get-ServiceList {
    @(
        @{ Name = 'login';          Dir = 'services/account/login';            Cmd = 'login' }
        @{ Name = 'player';         Dir = 'services/account/player';           Cmd = 'player' }
        @{ Name = 'data-service';   Dir = 'services/data/data_service';        Cmd = 'data_service' }
        @{ Name = 'friend';         Dir = 'services/social/friend';            Cmd = 'friend' }
        @{ Name = 'chat';           Dir = 'services/social/chat';              Cmd = 'chat' }
        @{ Name = 'guild';          Dir = 'services/social/guild';             Cmd = 'guild' }
        @{ Name = 'mail';           Dir = 'services/social/mail';              Cmd = 'mail' }
        @{ Name = 'player-locator'; Dir = 'services/runtime/player_locator';   Cmd = 'locator' }
        @{ Name = 'leaderboard';    Dir = 'services/runtime/leaderboard';      Cmd = 'leaderboard' }
        @{ Name = 'owner';          Dir = 'services/runtime/owner';            Cmd = 'owner' }
        @{ Name = 'team';           Dir = 'services/matchmaking/team';         Cmd = 'team' }
        @{ Name = 'matchmaker';     Dir = 'services/matchmaking/matchmaker';   Cmd = 'matchmaker' }
        # PVE 直进匹配实例:与 matchmaker 同目录同二进制(镜像层全缓存,构建零成本),
        # 仅配置不同(etc/matchmaker-pve.yaml,gen_cluster_config.ps1 生成 matchmaker-pve.yaml)。
        @{ Name = 'matchmaker-pve'; Dir = 'services/matchmaking/matchmaker';   Cmd = 'matchmaker' }
        @{ Name = 'trade';          Dir = 'services/economy/trade';            Cmd = 'trade' }
        @{ Name = 'dialogue';       Dir = 'services/social/dialogue';          Cmd = 'dialogue' }
        @{ Name = 'mission';        Dir = 'services/social/mission';           Cmd = 'mission' }
        @{ Name = 'push';           Dir = 'services/runtime/push';             Cmd = 'push' }
        @{ Name = 'inventory';      Dir = 'services/economy/inventory';        Cmd = 'inventory' }
        @{ Name = 'auction';        Dir = 'services/economy/auction';          Cmd = 'auction' }
        @{ Name = 'ds-allocator';   Dir = 'services/battle/ds_allocator';      Cmd = 'ds_allocator' }
        @{ Name = 'hub-allocator';  Dir = 'services/battle/hub_allocator';     Cmd = 'hub_allocator' }
        @{ Name = 'battle-result';  Dir = 'services/battle/battle_result';     Cmd = 'battle_result' }
    )
}

function Get-ServiceImages {
    Get-ServiceList | ForEach-Object { "pandora/$($_.Name):dev" }
}

# 推导版本烙印信息(编译期注入二进制,实现「线上跑的 ↔ 源码某次提交」可追溯)。
# 取不到时回退占位值,不阻断构建 —— 本机裸跑不该因为没有版本库就起不来。
function Get-VersionInfo {
    $ver    = 'dev'
    $commit = 'unknown'
    $built  = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
    if (Test-CommandExists 'git') {
        Push-Location $ProjectRoot
        try {
            $d = (git describe --tags --always --dirty 2>$null)
            if ($LASTEXITCODE -eq 0 -and $d) { $ver = $d.Trim() }
            $c = (git rev-parse --short HEAD 2>$null)
            if ($LASTEXITCODE -eq 0 -and $c) { $commit = $c.Trim() }
        } finally {
            Pop-Location
        }
    }
    # 源码版本由上游注入(publish_offline_images.ps1 已按 SVN/git 判定并算好)。
    # 这里刻意不再自己探一次 VCS:标准 CI 从 GitHub 构建,但仍兼容历史 SVN 工作副本；
    # 从 SVN 检出构建时上面的 git 分支全部软失败,若不接受注入就会静默烙上 dev/unknown。
    # 只在 git 没能给出结果时采用,保证 git 轨行为完全不变。
    if ($env:PANDORA_RELEASE_COMMIT) {
        $injected = $env:PANDORA_RELEASE_COMMIT.Trim()
        if ($injected) {
            if ($commit -eq 'unknown') { $commit = $injected }
            if ($ver    -eq 'dev')     { $ver    = $injected }
        }
    }
    # 发布版本号显式覆盖(标准 CI:VERSION 优先于 describe,免打 tag)。
    # 由 publish_offline_images.ps1 -Version 设置;Commit 仍保留源码版本作溯源。
    if ($env:PANDORA_RELEASE_VERSION) { $ver = $env:PANDORA_RELEASE_VERSION.Trim() }
    return [pscustomobject]@{ Version = $ver; Commit = $commit; BuildTime = $built }
}

function Build-AllImages {
    param([string[]]$Only = @(), [switch]$StrictRelease)
    $dockerfile = Join-Path $ProjectRoot 'deploy/services/Dockerfile'
    $v = Get-VersionInfo
    Write-Info "  版本烙印:version=$($v.Version) commit=$($v.Commit) built=$($v.BuildTime)"
    Write-Info "  构建方式:$BuildMode(incontainer=容器内 go build / host=宿主交叉编译再打包)"
    $list = Get-ServiceList
    # -Only 非空时只构建指定服务(含战斗混合模式不构建 ds/hub allocator 镜像,它们跑宿主)。
    if ($Only.Count -gt 0) {
        $known = @($list | ForEach-Object { $_.Name })
        $unknown = @($Only | Where-Object { $known -notcontains $_ })
        if ($unknown.Count -gt 0) {
            throw "未知服务名:$($unknown -join ',')。可选:$($known -join ',')"
        }
        $list = @($list | Where-Object { $Only -contains $_.Name })
    }

    # ===== 离线镜像优先(免联网,拉不到 Docker Hub/加速站的机器双击一键脚本即用)=====
    # deploy/offline-images/pandora-images.tar 由构建机 publish_offline_images.ps1 发布到制品目录,
    # 目标机用 fetch_offline_images.ps1 拉到本路径(tar 不入库;见 docs/design/release-pipeline.md)。
    # 判定:本机缺业务镜像 + 无 golang 构建基础镜像(= 这台机器多半构建不了)→ 自动 docker load 离线包,
    # 导入后齐全就跳过 docker build 直接起服务。开发机(有 golang 基础镜像)照常构建最新;强制构建加 -Rebuild。
    $offlineTar = Join-Path $ProjectRoot 'deploy/offline-images/pandora-images.tar'

    # ===== 打包机/运行机专用:强制纯离线,直接用离线包、完全不 docker build =====
    # 这台机器不改代码(打包机 / 内网运行机),不该每次先试构建、DNS 抖了再兜底。
    # 设环境变量 PANDORA_OFFLINE=1(打包机一次性 `setx PANDORA_OFFLINE 1`)即走此路径:
    # 导入离线包 → 校验齐全 → 直接返回,不联网、不构建、不受 Docker DNS 抖动影响。
    if (($env:PANDORA_OFFLINE -eq '1') -and -not $Rebuild -and -not $StrictRelease) {
        if (-not (Test-Path $offlineTar)) {
            throw "PANDORA_OFFLINE=1 但找不到离线包:$offlineTar(先跑 tools/scripts/fetch_offline_images.ps1 从制品目录拉取)。"
        }
        Write-Step "纯离线模式(PANDORA_OFFLINE=1):直接导入离线镜像包,跳过所有 docker build"
        Write-Info "  $offlineTar"
        docker load -i $offlineTar
        if ($LASTEXITCODE -ne 0) { throw "离线镜像导入失败:$offlineTar" }
        $missing = @($list | Where-Object { docker image inspect "pandora/$($_.Name):dev" *> $null; $LASTEXITCODE -ne 0 })
        if ($missing.Count -gt 0) {
            Write-Err "离线包导入后仍缺以下镜像:"
            $missing | ForEach-Object { Write-Err "  - pandora/$($_.Name):dev" }
            throw "离线包与服务清单不一致,请在构建机重跑 publish_offline_images.ps1 后再 fetch_offline_images.ps1 拉取。"
        }
        Write-Ok "已用离线镜像包起服务(纯离线,未构建)。要改代码后重建请在开发机做,或临时 -Rebuild。"
        return
    }

    # 离线镜像优先:本机缺业务镜像 + 无「构建能力」时自动导入离线包(免联网)。
    # 「构建能力」两种模式判定不同:host 方式需本机装 Go;incontainer 方式需 golang 基础镜像。
    if (-not $StrictRelease -and -not $Rebuild -and (Test-Path $offlineTar)) {
        $missing = @($list | Where-Object { docker image inspect "pandora/$($_.Name):dev" *> $null; $LASTEXITCODE -ne 0 })
        if ($BuildMode -eq 'host') {
            $canBuild = [bool](Test-CommandExists 'go')
        } else {
            $canBuild = [bool](docker images --format '{{.Repository}}' 2>$null | Select-String -Quiet '(^|/)golang$')
        }
        if ($missing.Count -gt 0 -and -not $canBuild) {
            Write-Step "离线镜像优先:本机缺 $($missing.Count) 个业务镜像且无构建能力(host 缺 Go / incontainer 缺 golang 基础镜像),自动导入离线包(免联网)"
            Write-Info "  $offlineTar"
            docker load -i $offlineTar
            if ($LASTEXITCODE -ne 0) { throw "离线镜像导入失败:$offlineTar" }
            $missing = @($list | Where-Object { docker image inspect "pandora/$($_.Name):dev" *> $null; $LASTEXITCODE -ne 0 })
        }
        if (-not $canBuild) {
            if ($missing.Count -eq 0) {
                Write-Ok "业务镜像已齐全(离线导入/已存在),本机无构建能力,跳过构建直接使用。要强制构建加 -Rebuild。"
                return
            }
            Write-Err "离线导入后仍缺以下镜像,且本机无构建能力(host 缺 Go / incontainer 缺 golang 基础镜像):"
            $missing | ForEach-Object { Write-Err "  - pandora/$($_.Name):dev" }
            throw "请在构建机跑 publish_offline_images.ps1 重新发布,再用 fetch_offline_images.ps1 拉取到 deploy/offline-images/。"
        }
    }

    # 分派:方案 B(宿主编译 → 打包)/ 方案 A(容器内 go build)。
    if ($BuildMode -eq 'host') {
        Build-Images-Host -List $list -Version $v
    } else {
        Build-Images-InContainer -List $list -Version $v -Dockerfile $dockerfile -OfflineTar $offlineTar -StrictRelease:$StrictRelease
    }
}

# ===== 方案 A:容器内 go build(deploy/services/Dockerfile,环境隔离,无需本机 Go)=====
function Build-Images-InContainer {
    param([array]$List, $Version, [string]$Dockerfile, [string]$OfflineTar, [switch]$StrictRelease)
    $v = $Version

    function Get-GoImageGoversion([string]$Image) {
        $actual = (docker run --rm --entrypoint go $Image env GOVERSION 2>$null | Out-String).Trim()
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($actual)) { return $null }
        return $actual
    }

    # 国内镜像:基础镜像仓库前缀 + go 模块代理,避免卡在 Docker Hub / proxy.golang.org。
    # 默认走国内加速;可用 PANDORA_BASE_REGISTRY / PANDORA_GOPROXY 覆盖(官方仓库填 docker.io)。
    $baseRegistry = $env:PANDORA_BASE_REGISTRY
    if (-not $baseRegistry) {
        # 本机已有且容器内版本精确匹配时才复用。禁止把任意旧 golang 镜像改标成目标版本，
        # 否则 tag 看似正确，实际编译器仍可能是旧 patch 版本。
        $goVer = '1.26.5'
        $m = Select-String -Path $Dockerfile -Pattern '^ARG\s+GO_VERSION=(\S+)' -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($m) { $goVer = $m.Matches[0].Groups[1].Value }
        $goImageVariant = 'bookworm'
        $variantMatch = Select-String -Path $Dockerfile -Pattern '^ARG\s+GO_IMAGE_VARIANT=(\S+)' -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($variantMatch) { $goImageVariant = $variantMatch.Matches[0].Groups[1].Value }
        $goImageTag = "$goVer-$goImageVariant"
        $wantGo = "docker.io/library/golang:$goImageTag"
        docker image inspect $wantGo *> $null
        $wantActual = if ($LASTEXITCODE -eq 0) { Get-GoImageGoversion $wantGo } else { $null }
        if ($wantActual -eq "go$goVer") {
            $baseRegistry = 'docker.io'
        } else {
            if ($wantActual) {
                Write-Warn "  本机 $wantGo 实际为 $wantActual，不符合 go$goVer，拒绝复用。"
            }
            $tagPattern = '(^|/)golang:' + [regex]::Escape($goImageTag) + '$'
            $candidates = @(docker images --format '{{.Repository}}:{{.Tag}}' 2>$null | Select-String $tagPattern | ForEach-Object { "$($_)".Trim() })
            $picked = $null
            foreach ($candidate in $candidates) {
                if ((Get-GoImageGoversion $candidate) -eq "go$goVer") { $picked = $candidate; break }
            }
            if ($picked) {
                Write-Info "  发现容器内精确为 go$goVer 的 $picked，打标为 $wantGo 复用。"
                docker tag $picked $wantGo *> $null
                if ($LASTEXITCODE -ne 0) { throw "无法创建基础镜像标签:$wantGo" }
                $baseRegistry = 'docker.io'
            } else {
                $baseRegistry = 'docker.m.daocloud.io'
            }
        }
    }
    $goproxy = $env:PANDORA_GOPROXY
    if (-not $goproxy) {
        if ($env:GOPROXY -and $env:GOPROXY -notmatch 'proxy\.golang\.org') { $goproxy = $env:GOPROXY }
        # 分隔符是 `|` 不是 `,`:逗号只在代理返回 404/410 时才退到下一个源,网络层错误
        # (unexpected EOF / 超时 / 连接重置)直接失败,后面的 direct 根本轮不到。
        # 竖线才是「任何错误都回退」。同处注释见 deploy/services/Dockerfile。
        else { $goproxy = 'https://goproxy.cn|https://proxy.golang.org|direct' }
    }
    Write-Info "  基础镜像仓库:$baseRegistry(可用 PANDORA_BASE_REGISTRY 覆盖;官方用 docker.io)"
    Write-Info "  Go 模块代理:$goproxy(可用 PANDORA_GOPROXY 覆盖)"

    # 本轮已构建成功的镜像 id。离线兜底的 docker load 按 tag 覆盖同名镜像,必须留着 id 指回来。
    $builtImageIds = @{}
    $offlineFallbackDone = $false
    foreach ($svc in $List) {
        Write-Info "  docker build pandora/$($svc.Name):dev ..."
        docker build -f $Dockerfile `
            --build-arg "SERVICE_DIR=$($svc.Dir)" `
            --build-arg "CMD_NAME=$($svc.Cmd)" `
            --build-arg "VERSION=$($v.Version)" `
            --build-arg "GIT_COMMIT=$($v.Commit)" `
            --build-arg "BUILD_TIME=$($v.BuildTime)" `
            --build-arg "BASE_REGISTRY=$baseRegistry" `
            --build-arg "GOPROXY=$goproxy" `
            --label "org.opencontainers.image.revision=$($v.Commit)" `
            -t "pandora/$($svc.Name):dev" $ProjectRoot
        if ($LASTEXITCODE -eq 0) {
            $builtId = ((& docker image inspect --format '{{.Id}}' "pandora/$($svc.Name):dev" 2>$null) | Out-String).Trim()
            if ($builtId) { $builtImageIds[$svc.Name] = $builtId }
            continue
        }

        # 构建失败兜底:有离线包就自动导入,导入后该镜像存在则继续。
        # 导入只做一次(offlineFallbackDone),但之后每个失败服务都复查一次 inspect,避免第 2 个服务误报失败。
        if (-not $StrictRelease -and -not $Rebuild -and (Test-Path $OfflineTar)) {
            if (-not $offlineFallbackDone) {
                Write-Warn "  构建 $($svc.Name) 失败。检测到离线镜像包,自动导入兜底(离线包是历史构建产物,commit 通常旧于当前工作树)..."
                docker load -i $OfflineTar
                $offlineFallbackDone = $true
                # docker load 按 tag 覆盖:本轮**已经构建成功**的服务会被离线包里的同名 :dev 顶掉。
                # 2026-08-12 事故就是这样把 5 个刚构建好的镜像静默换回旧 commit(日志里只提示了
                # 实际用到离线包的那几个,被覆盖的那批一声不吭)。逐个指回本轮产物并逐条报告。
                foreach ($name in @($builtImageIds.Keys)) {
                    $nowId = ((& docker image inspect --format '{{.Id}}' "pandora/${name}:dev" 2>$null) | Out-String).Trim()
                    if ($nowId -and $nowId -cne $builtImageIds[$name]) {
                        docker tag $builtImageIds[$name] "pandora/${name}:dev" *> $null
                        if ($LASTEXITCODE -ne 0) {
                            throw "离线包覆盖了本轮已构建的 pandora/${name}:dev,且无法指回本轮产物(image=$($builtImageIds[$name]))。"
                        }
                        Write-Warn "  离线包覆盖了本轮已构建的 pandora/${name}:dev,已指回本轮产物。"
                    }
                }
            }
            docker image inspect "pandora/$($svc.Name):dev" *> $null
            if ($LASTEXITCODE -eq 0) {
                $rev = ((& docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "pandora/$($svc.Name):dev" 2>$null) | Out-String).Trim()
                if ($rev -eq '<no value>') { $rev = '' }
                if ($rev -and $rev -cne $v.Commit) {
                    # 旧二进制 + 新生成的配置 = 错位,而且症状离原因很远:2026-08-12 inventory
                    # 用离线包的 e253821 起来,配置已按新版删掉 default_identify_rule,进程启动
                    # 10ms 就退出,现象只表现为 rollout status 180s 超时「新 Pod 未就绪」。
                    Write-Warn "  使用离线镜像:pandora/$($svc.Name):dev(commit=$rev,本次配置生成源=$($v.Commit),两者不一致)"
                    Write-Warn "    旧二进制会配上新版生成的配置:可能要求已被删掉的配置键而启动即 CrashLoop,或读不到新增配表而静默降级。"
                    Write-Warn "    要跑当前工作树的代码,请先修好该服务的构建再重跑(常见原因是 go 模块代理抖动,可用 PANDORA_GOPROXY 换源)。"
                } else {
                    Write-Ok "  使用离线镜像:pandora/$($svc.Name):dev"
                }
                continue
            }
        }
        throw "镜像构建失败:$($svc.Name)"
    }
}

# ===== 方案 B:宿主交叉编译 → 只打包(deploy/services/Dockerfile.prebuilt)=====
# 本机用 GOOS=linux CGO_ENABLED=0 交叉编译 linux/amd64 静态二进制,享受宿主 go build 增量缓存
# (单服务重建秒级、不重下依赖),再用轻量 Dockerfile.prebuilt 把产物 + etc/ 塞进 scratch 镜像。
# 与方案 A 产出同名镜像 pandora/<name>:dev,版本烙印一致,可无缝喂给 compose / export_images。
function Build-Images-Host {
    param([array]$List, $Version)
    if (-not (Test-CommandExists 'go')) {
        throw "host 构建方式需要本机安装 Go 1.26.5。装好后重试,或改用 -BuildMode incontainer(容器内编译,无需本机 Go)。"
    }
    # go env GOVERSION 受 cwd 影响:在 go.work 所在目录下 Go 会按 toolchain 指令自动切到
    # go1.26.5,在仓库外则只报基础工具链版本(如 go1.26.4)。门禁必须按后端工程根目录判定,
    # 否则从别处调用本脚本会误报版本不符。
    Push-Location $ProjectRoot
    try { $hostGoVersion = ((& go env GOVERSION) | Out-String).Trim() } finally { Pop-Location }
    if ($LASTEXITCODE -ne 0 -or $hostGoVersion -ne 'go1.26.5') {
        throw "host 构建要求 go env GOVERSION=go1.26.5，当前=$hostGoVersion。"
    }
    Write-Info "  宿主 Go 版本已验证:$hostGoVersion"
    $v = $Version
    $prebuiltDockerfile = Join-Path $ProjectRoot 'deploy/services/Dockerfile.prebuilt'
    $stageRoot = Join-Path $ProjectRoot 'run/docker-build/prebuilt'
    if (-not (Test-Path $stageRoot)) { New-Item -ItemType Directory -Path $stageRoot -Force | Out-Null }

    # 预编译镜像只需 CA + zoneinfo。若 Dockerfile 对 22 个服务都直接 FROM 远程 golang tag，
    # BuildKit 即使已有 layer cache 仍可能逐次请求 registry manifest/token；网络抖动会在任意
    # 一个服务处 TLS timeout。循环前只准备一次本地固定 source tag，之后打包完全不访问 registry。
    $runtimeAssetsImage = 'pandora/runtime-assets:local'
    docker image inspect $runtimeAssetsImage *> $null
    if ($LASTEXITCODE -ne 0) {
        $localRuntimeSource = $null
        foreach ($candidate in @($List | ForEach-Object { "pandora/$($_.Name):dev" })) {
            docker image inspect $candidate *> $null
            if ($LASTEXITCODE -eq 0) { $localRuntimeSource = $candidate; break }
        }
        if ($null -eq $localRuntimeSource) {
            $goVer = '1.26.5'
            $goImageVariant = 'bookworm'
            $baseRegistry = if ($env:PANDORA_BASE_REGISTRY) { $env:PANDORA_BASE_REGISTRY.TrimEnd('/') } else { 'docker.m.daocloud.io' }
            $localRuntimeSource = "$baseRegistry/library/golang:$goVer-$goImageVariant"
            docker image inspect $localRuntimeSource *> $null
            if ($LASTEXITCODE -ne 0) {
                Write-Info "  首次准备本地 CA/时区资产镜像:$localRuntimeSource"
                docker pull $localRuntimeSource
                if ($LASTEXITCODE -ne 0) {
                    throw "无法一次性拉取 runtime assets source:$localRuntimeSource。可设置 PANDORA_BASE_REGISTRY=docker.io 后重试。"
                }
            }
        }
        Write-Info "  固定本地 runtime assets:$localRuntimeSource -> $runtimeAssetsImage"
        docker tag $localRuntimeSource $runtimeAssetsImage
        if ($LASTEXITCODE -ne 0) { throw "无法创建本地 runtime assets tag:$runtimeAssetsImage" }
    }
    $runtimeAssetsDir = Join-Path $stageRoot '_runtime-assets'
    if (Test-Path $runtimeAssetsDir) { Remove-Item -LiteralPath $runtimeAssetsDir -Recurse -Force }
    New-Item -ItemType Directory -Path (Join-Path $runtimeAssetsDir 'zoneinfo') -Force | Out-Null
    $assetsContainer = $null
    try {
        $assetsContainer = ((docker create $runtimeAssetsImage) | Out-String).Trim()
        if ($LASTEXITCODE -ne 0 -or $assetsContainer -cnotmatch '^[0-9a-f]{12,64}$') {
            throw "无法从 $runtimeAssetsImage 创建 runtime assets 临时容器。"
        }
        docker cp "${assetsContainer}:/etc/ssl/certs/ca-certificates.crt" (Join-Path $runtimeAssetsDir 'ca-certificates.crt')
        if ($LASTEXITCODE -ne 0) { throw "$runtimeAssetsImage 缺 CA 根证书。" }
        docker cp "${assetsContainer}:/usr/share/zoneinfo/." (Join-Path $runtimeAssetsDir 'zoneinfo')
        if ($LASTEXITCODE -ne 0) { throw "$runtimeAssetsImage 缺时区库。" }
    } finally {
        if (-not [string]::IsNullOrWhiteSpace($assetsContainer)) {
            docker rm -f $assetsContainer *> $null
        }
    }
    if (-not (Test-Path -LiteralPath (Join-Path $runtimeAssetsDir 'ca-certificates.crt') -PathType Leaf) -or
        @(Get-ChildItem -LiteralPath (Join-Path $runtimeAssetsDir 'zoneinfo') -File -Recurse).Count -eq 0) {
        throw '本地 runtime assets 提取结果不完整。'
    }
    # 宿主 go build 拉模块用的代理(默认 goproxy.cn;尊重已自定义的非公有 GOPROXY)。
    # 分隔符必须是 `|`:逗号只在 404/410 回退,网络层错误直接失败(详见 deploy/services/Dockerfile)。
    $goproxy = $env:PANDORA_GOPROXY
    if (-not $goproxy) {
        if ($env:GOPROXY -and $env:GOPROXY -notmatch 'proxy\.golang\.org') { $goproxy = $env:GOPROXY }
        else { $goproxy = 'https://goproxy.cn|https://proxy.golang.org|direct' }
    }
    Write-Info "  CA/时区资产镜像:$runtimeAssetsImage(本地固定 tag，服务循环内不访问 registry)"
    Write-Info "  Go 模块代理:$goproxy(宿主交叉编译用)"

    $ld = "-s -w " +
        "-X github.com/luyuancpp/pandora/pkg/version.Version=$($v.Version) " +
        "-X github.com/luyuancpp/pandora/pkg/version.Commit=$($v.Commit) " +
        "-X github.com/luyuancpp/pandora/pkg/version.BuildTime=$($v.BuildTime)"

    foreach ($svc in $List) {
        $stage  = Join-Path $stageRoot $svc.Name
        $etcSrc = Join-Path $ProjectRoot "$($svc.Dir)/etc"
        if (Test-Path $stage) { Remove-Item -Recurse -Force $stage }
        New-Item -ItemType Directory -Path $stage -Force | Out-Null

        Write-Info "  [host] go build $($svc.Name)(GOOS=linux CGO_ENABLED=0)..."
        Push-Location (Join-Path $ProjectRoot $svc.Dir)
        $rc = 1
        try {
            $env:GOOS = 'linux'; $env:GOARCH = 'amd64'; $env:CGO_ENABLED = '0'
            if ($goproxy) { $env:GOPROXY = $goproxy }
            & go build -trimpath -ldflags $ld -o (Join-Path $stage 'app') "./cmd/$($svc.Cmd)"
            $rc = $LASTEXITCODE
        } finally {
            Pop-Location
            Remove-Item Env:GOOS, Env:GOARCH, Env:CGO_ENABLED -ErrorAction SilentlyContinue
        }
        if ($rc -ne 0) { throw "宿主编译失败:$($svc.Name)(可改用 -BuildMode incontainer 在容器内编译)。" }

        # 拷该服务 etc/ 进 stage(镜像 COPY etc/;主配置运行期被集群版覆盖)。
        if (Test-Path $etcSrc) {
            Copy-Item -Recurse -Force $etcSrc (Join-Path $stage 'etc')
        } else {
            New-Item -ItemType Directory -Path (Join-Path $stage 'etc') -Force | Out-Null
        }
        Write-Info "  [host] docker build pandora/$($svc.Name):dev(打包预编译产物,秒级)..."
        docker build -f $prebuiltDockerfile `
            --build-context "runtime_assets=$runtimeAssetsDir" `
            --label "org.opencontainers.image.revision=$($v.Commit)" `
            -t "pandora/$($svc.Name):dev" $stage
        if ($LASTEXITCODE -ne 0) { throw "镜像打包失败:$($svc.Name)" }
    }
    Write-Ok "宿主编译 + 打包完成($($List.Count) 个镜像)。"
}

# ===== 电脑重启后快速恢复(不重建镜像,尽量把上次状态拉回来)=====
# k8s:minikube stop/重启只是停容器,集群状态+已 load 镜像都还在磁盘,minikube start 即恢复,Pod 自动重建。
function Resume-K8s {
    Write-Step "k8s 快速恢复(电脑重启后:只拉起 minikube + 等 Pod,不重建镜像)"
    $mkProfile = Get-K8sManagedProfile
    minikube -p $mkProfile status *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-Info "minikube 已停,minikube start 中(profile=$mkProfile;集群状态/镜像都在磁盘上,Pod 会自动恢复)..."
        # Resume 对象必然是既有 profile:Get-MinikubeStartArgs 返回空参,沿用集群已存拓扑,
        # 绝不带可能与既有 driver/runtime 冲突的创建参数(minikube 会硬报错)。
        $mkStartArgs = Get-MinikubeStartArgs $mkProfile
        minikube start -p $mkProfile @mkStartArgs
        if ($LASTEXITCODE -ne 0) { throw "minikube 启动失败(若集群已损坏,改用 -Reset 全新部署)" }
    } else {
        Write-Ok "minikube 已在运行(profile=$mkProfile)"
    }
    # 上下文锁:恢复流程也会 rollout restart/apply,必须确认 current-context 是本机 minikube。
    $mkCtx = Resolve-MinikubeKubeContext
    $curCtx = (kubectl config current-context 2>$null)
    if ($curCtx -cne $mkCtx) {
        throw "当前 kubectl current-context『$curCtx』不是本机 minikube『$mkCtx』。为防误操作远端集群,-Resume 已中止。请先 kubectl config use-context $mkCtx 再重试。"
    }
    if (-not (Test-KubeContextIsLocalMinikube $mkCtx)) {
        throw "kube-context『$mkCtx』的 apiserver endpoint 不是本机 minikube(疑似同名远端/生产集群)。为防 -Resume 把清单发到远端,已中止。"
    }

    # minikube start 返回与 apiserver 真正可读之间仍可能有短暂竞态；这里只等待 control-plane
    # /readyz，绝不先等待旧 login/allocator Ready。apiserver 一可读就完成所有 fail-closed 审计，
    # 审计通过后才允许任何业务 Ready 等待、apply 或 rollout。
    Wait-KubeApiServerReady -KubeContext $mkCtx
    # required policy 的线性审计依赖 etcd；电脑重启后 apiserver Ready 不代表 etcd Pod 已恢复。
    # 只先等基础设施 etcd，绝不先等可能带旧 capability provenance 的业务 writer。
    kubectl --context $mkCtx rollout status deploy/etcd -n $K8sNamespace --timeout=1800s
    Assert-LastExit 'etcd 恢复就绪（DS-auth baseline 前）'
    Write-Step 'Resume 最早只读审计（先于旧业务 Ready/apply/rollout）'
    Assert-ExistingLocalEtcdPersistence -KubeContext $mkCtx
    Assert-NoLegacyDsFleets -KubeContext $mkCtx -LocalDevelopment
    Assert-NoLegacyDSTicketSignerSecret -KubeContext $mkCtx -LocalDevelopment
    Assert-LocalDsAuthBaseline -KubeContext $mkCtx -MinikubeProfile $mkProfile -AllowFreshBootstrap:$false

    # advertise 地址重解析(-AdvertiseHost > env > 局域网 IP)。DHCP 换址后本机 IP 可能已变,
    # 但 Secret 里仍是旧地址 —— 必须重生配置 + 覆盖 Secret + 重启读该地址的 Deployment,
    # 否则 allocator 会继续把旧 IP 发给客户端,导致「恢复成功但连不进 DS」。
    $resumeAdvHost =
        if (-not [string]::IsNullOrWhiteSpace($AdvertiseHost)) { $AdvertiseHost.Trim() }
        elseif (-not [string]::IsNullOrWhiteSpace($env:PANDORA_DS_ADVERTISE_HOST)) { $env:PANDORA_DS_ADVERTISE_HOST.Trim() }
        else { Resolve-LanIp }
    if ([string]::IsNullOrWhiteSpace($resumeAdvHost)) { $resumeAdvHost = '127.0.0.1' }

    Write-Step "刷新 DS advertise 到 Secret(防 DHCP 换址后 allocator 继续发旧地址):$resumeAdvHost"
    # 本地 minikube resume(context=$mkCtx),沿用公开 dev 密钥,显式 -AllowDevSecrets(审核 P1)。
    # DSTicket v2:复用/补齐 dev 钥料(旧集群若缺 Secret/ConfigMap 会在此幂等补上,Fleet 挂载不悬空)。
    $resumeDsTicketKid = Ensure-DsTicketDevKeyMaterial -KubeContext $mkCtx
    & "$ScriptDir/gen_cluster_config.ps1" -AllocatorMode agones -AllocatorAdvertiseHost $resumeAdvHost -AllowDevSecrets `
        -DsAuthMode enforce -DsAuthorityMode redis -DsFenceEtcdEndpoints $script:LocalDsFenceEndpoint `
        -DsFenceKeysetRevision $script:LocalDsFenceKeysetRevision -DsTicketActiveKid $resumeDsTicketKid `
        -DsTicketKeysetRevision 1
    Assert-LastExit '恢复生成本地 k8s enforce/redis DSTicket v2 配置'
    Apply-PandoraConfigSecret -KubeContext $mkCtx -Action 'kubectl apply secret pandora-config(advertise 刷新)'
    $null = Apply-PandoraConfigTableConfigMap -KubeContext $mkCtx -Action 'kubectl apply configmap pandora-configtable(恢复刷新)'
    # 重新 apply 业务清单(审核 P1 #5):-Resume 若恢复的是「旧版本(挂 ConfigMap)部署的集群」,
    # 光 create Secret 不会让 Pod 改用它 —— 卷源仍指向旧 ConfigMap,新 Secret 完全被忽略。
    # 重 apply 当前清单把卷源纠正为 Secret(:dev tag 不变,未变的 Deployment 报 unchanged 不churn;
    # 卷源从 ConfigMap 改成 Secret 的旧集群会被自动 rollout 迁移),确保新 Secret 真正被挂载。
    $resumeServicesDir = Join-Path $ProjectRoot 'deploy/k8s/services'
    kubectl --context $mkCtx apply -k $resumeServicesDir
    Assert-LastExit 'kubectl apply -k services(Resume:纠正卷源为 Secret)'
    # 必须先让当前清单把 Hub strategy 收敛为 Recreate，之后才能改 template annotation；
    # 在旧 RollingUpdate 对象上先 patch digest 会短时启动两个 Hub ledger writer。
    # Resume 不构建镜像，节点现存 immutable image config digest 是实际 source of truth。
    Set-LocalDsAuthImageDigestAnnotations -KubeContext $mkCtx -MinikubeProfile $mkProfile
    # Secret 以 subPath 挂载不会热感知:按名 rollout restart 全部 22 个业务 Deployment,
    # 让刷新后的 pandora-config Secret(advertise + 其它配置)在每个 Pod 生效(滚动重启零停机)。
    # 必须逐个等待全部 22 个 Deployment；只等 allocator 会把其余服务的失败误报成“全部传播完成”。
    Write-Info "rollout restart 全部业务 Deployment(传播刷新后的 pandora-config Secret)..."
    foreach ($svc in (Get-ServiceList)) {
        kubectl --context $mkCtx rollout restart deploy/$($svc.Name) -n $K8sNamespace
        Assert-LastExit "rollout restart $($svc.Name)(Secret 刷新)"
    }
    foreach ($svc in (Get-ServiceList)) {
        kubectl --context $mkCtx rollout status deploy/$($svc.Name) -n $K8sNamespace --timeout=180s
        Assert-LastExit "rollout status $($svc.Name)(Secret 刷新后未就绪)"
    }
    Assert-LocalDsAuthImageDigestAnnotations -KubeContext $mkCtx -MinikubeProfile $mkProfile
    Remove-LegacyPandoraConfigMapAfterRollout -KubeContext $mkCtx
    Write-Ok "pandora-config Secret 已刷新为 advertise=$resumeAdvHost 并传播到全部业务 Pod(卷源已确保为 Secret)。"

    Write-Step "补部署 DS 面 Envoy + Fleet(让磁盘上的清单改动在恢复后生效)"
    # -Resume 靠 minikube start 拉回的是「停机前」集群内对象;若期间改过 16-ds-envoy.yaml(如 :8444
    # 路由白名单)或 Fleet 清单,光恢复只会拿回旧版本。这里幂等重 apply 补齐(回应审核 P#4:
    # -Resume 不补 Envoy/Fleet)。Envoy 只在启动读一次静态配置,故 rollout restart 强制重读;
    # Fleet 钉不可变 tag:磁盘 yaml 的 image 行没改过则 apply=unchanged,不会打断在跑的
    # GameServer;若停机期间有人换了钉定 tag,这次 apply 就会(按预期)触发 Agones 滚动换版。
    $resumeAgonesDir = Join-Path $ProjectRoot 'deploy/k8s/agones'
    # 旧集群(部署于 16-ds-envoy 引入前)节点内可能还没有 Envoy 镜像;断网下 Pod pull 必失败。
    # 先走三级来源(节点已有→宿主 load→联网 pull)把镜像备齐,再 apply(回应审核 P1:Resume 断网失败)。
    Ensure-EnvoyImageInMinikube -MinikubeProfile $mkProfile
    kubectl --context $mkCtx apply -f (Join-Path $resumeAgonesDir '16-ds-envoy.yaml'); Assert-LastExit 'apply 16-ds-envoy(恢复)'
    kubectl --context $mkCtx rollout restart deploy/pandora-envoy -n $K8sNamespace; Assert-LastExit 'rollout restart pandora-envoy(恢复:重载 DS 面配置)'
    kubectl --context $mkCtx rollout status  deploy/pandora-envoy -n $K8sNamespace --timeout=120s; Assert-LastExit 'pandora-envoy 恢复就绪'
    # Fleet 清单自带 namespace: default,不加 -n,让清单里的命名空间生效。
    foreach ($resumeFleet in @('20-fleet-battle.yaml', '21-fleet-battle-canary.yaml', '30-fleet-hub.yaml', '31-fleet-hub-canary.yaml')) {
        kubectl --context $mkCtx apply -f (Join-Path $resumeAgonesDir $resumeFleet); Assert-LastExit "apply Fleet $resumeFleet(恢复)"
    }
    Write-Ok "DS 面 Envoy 已重载配置,Fleet 清单已同步。"

    Write-Step "宿主 Envoy 桥接 + UDP 中继(e2e_k8s.ps1)"
    # 重启后宿主 port-forward/envoy/relay 必然全丢,自动重建(DS 镜像还在 minikube 磁盘,跳过 load)。
    $relayBind = if (-not [string]::IsNullOrWhiteSpace($resumeAdvHost) -and $resumeAdvHost -ne '127.0.0.1') { '0.0.0.0' } else { '127.0.0.1' }
    # 恢复路径同样锁死宿主 DS 面,避免继承用户环境中的 0.0.0.0。
    $env:PANDORA_DS_EDGE_BIND_HOST = '127.0.0.1'
    & pwsh -NoProfile -ExecutionPolicy Bypass -File (Join-Path $ScriptDir 'e2e_k8s.ps1') -SkipImageLoad -MinikubeProfile $mkProfile -KubeContext $mkCtx -RelayBindHost $relayBind
    Assert-LastExit "宿主桥接/中继(e2e_k8s.ps1);修复后可单独重跑:pwsh tools/scripts/e2e_k8s.ps1 -SkipImageLoad -MinikubeProfile $mkProfile -KubeContext $mkCtx -RelayBindHost $relayBind"
    if ($relayBind -eq '0.0.0.0') {
        Write-Ok "真 DS 闭环已恢复(局域网):内网其它机器客户端连 ${resumeAdvHost}:8443 即可(需防火墙放行 TCP 8443 + UDP 7000-8000;宿主 8444 仅回环)。"
    } else {
        Write-Ok "真 DS 闭环已恢复:客户端连 127.0.0.1:8443 即可(仅本机)。"
    }
}

function Invoke-Resume {
    Write-Step "$Mode 快速恢复(电脑重启后:尽量不重建,直接把上次的状态拉回来)"
    switch ($Mode) {
        'k8s' { Resume-K8s }
        'local' {
            Write-Info "基础设施容器随 Docker Desktop 自动恢复;这里重新拉起宿主 go 服务。"
            Invoke-Local
        }
        'battle' {
            Write-Err "battle 模式已废弃,无可恢复项;清理遗留环境用 -Mode battle -Down,真 DS 用 -Mode k8s -Resume。"
            exit 1
        }
        { $_ -in 'docker', 'intranet' } {
            Write-Info "重启已停的容器(不加 --build,不重建镜像)..."
            # 边缘绑定必须与全新启动一致,否则恢复后 Envoy 退回默认回环:
            #   intranet → 0.0.0.0(对局域网开放,admin 9901 仍恒绑本机);docker → 127.0.0.1(仅本机)。
            # 该环境变量要在 dev_up.ps1(起 envoy 的 infra compose)之前导出才生效。
            $lan = if (-not [string]::IsNullOrWhiteSpace($AdvertiseHost)) { $AdvertiseHost } else { Resolve-LanIp }
            if ($Mode -eq 'intranet') {
                Set-EdgeBindHost '0.0.0.0'
                if ([string]::IsNullOrWhiteSpace($lan)) {
                    Write-Warn "未能解析内网 IPv4;Envoy 客户端面已绑 0.0.0.0,但请用 -AdvertiseHost 显式告知客户端连哪个地址。"
                }
            } else {
                Set-EdgeBindHost '127.0.0.1'
            }
            & "$ScriptDir/dev_up.ps1"
            if ($LASTEXITCODE -ne 0) { throw "基础设施恢复失败(dev_up)" }
            docker compose -f $ComposeServices up -d
            if ($LASTEXITCODE -ne 0) { throw "业务服务容器恢复失败" }
            Write-Ok "$Mode 容器已恢复(Envoy 绑定 $env:PANDORA_EDGE_BIND_HOST)。"
            if ($Mode -eq 'intranet' -and -not [string]::IsNullOrWhiteSpace($lan)) {
                Write-Host "       内网地址  https://${lan}:8443(客户端面)" -ForegroundColor Green
                if ($ExposeDsFace) { Write-Host "       DS 面      ${lan}:8444(-ExposeDsFace 已开放)" -ForegroundColor Green }
            }
        }
        'online' { Write-Err "online 是远端集群,Pod 由集群自管,无需本机恢复。"; exit 1 }
    }
}

# ===== 一键重置:彻底清掉旧状态,再全新启动 =====
function Invoke-Reset {
    Write-Step "$Mode 一键重置:先彻底清理,再全新启动"
    switch ($Mode) {
        'k8s' {
            # 只销毁本次运行钉死的 minikube profile(与 Invoke-K8s 重建目标严格一致,避免删 A 建 B)。
            # minikube delete -p 只作用于本地 minikube,不触碰远端集群;但仍先确认该 profile 的 kube-context
            # 指向本机 minikube(而非同名远端集群),再删,防「Get-ActiveMinikubeProfile 回退 minikube」误删默认集群。
            $mkProfile = Get-K8sManagedProfile
            $contexts = @(kubectl config get-contexts -o name 2>$null)
            if ($contexts -ccontains $mkProfile -and -not (Test-KubeContextIsLocalMinikube $mkProfile)) {
                throw "reset 目标『$mkProfile』的 kube-context 指向的不是本机 minikube(疑似同名远端/生产集群),为防误删已中止。"
            }
            # kube-context 缺失不等于本地 profile 已不存在(损坏/半删除集群常见)。minikube delete -p
            # 只操作本机 profile,此时仍要执行，才能兑现 Reset 的“彻底清掉再重建”语义。
            Write-Warn "将 minikube delete -p $mkProfile(仅销毁本机集群『$mkProfile』,已 load 镜像一并清掉),然后全新部署。"
            minikube delete -p $mkProfile 2>$null
            # 「目标本来就不存在」不是失败:Reset 要的是「删干净」这个**终态**,不是「这次删到了东西」。
            # minikube delete 对不存在的 profile 返回非 0(实测 exit=80),此前直接 Assert-LastExit,
            # 于是上一轮在 delete 之后、重建之前中断的机器会永远卡死在 Reset 的第一步:
            # 集群已被删掉 → 再跑 Reset 又因「删不到」而中止 → 永远重建不回来(本机实测踩到)。
            # 故按终态判定:delete 后 profile 确已不存在就放行,仍存在才是真失败。
            if ($LASTEXITCODE -ne 0) {
                if (Test-MinikubeProfileExists -Profile $mkProfile) {
                    throw "minikube delete -p $mkProfile 失败(exit=$LASTEXITCODE)且该 profile 仍然存在,已中止(避免在没删干净的集群上重建)。"
                }
                Write-Info "minikube delete 返回 $LASTEXITCODE,但『$mkProfile』确认已不存在(多半是上一轮已删),按已清理继续重建。"
            }
            Invoke-K8s
        }
        'local' {
            & "$ScriptDir/dev_all.ps1" -Down 2>$null
            Invoke-Local
        }
        'battle' {
            # 已废弃:只做清理,不再重新启动(decision-revisit-retire-battle-mode.md)。
            & "$ScriptDir/run_services.ps1" -Action down 2>$null
            docker compose -f $ComposeServices down -v 2>$null
            & "$ScriptDir/dev_down.ps1" 2>$null
            Write-Ok "battle 遗留环境已清理。battle 模式已废弃,真 DS 请用 -Mode k8s。"
        }
        'docker' {
            docker compose -f $ComposeServices down -v 2>$null
            & "$ScriptDir/dev_down.ps1" 2>$null
            Invoke-Docker
        }
        'intranet' {
            docker compose -f $ComposeServices down -v 2>$null
            & "$ScriptDir/dev_down.ps1" 2>$null
            Invoke-Intranet
        }
        'online' { Write-Err "online 模式禁用 -Reset(线上集群不做销毁式重置);如需重发请用正常部署流程。"; exit 1 }
    }
}

# ===== 状态 =====
function Show-Status {
    switch ($Mode) {
        'local'  { & "$ScriptDir/run_services.ps1" -Action status }
        'battle' {
            Write-Warn "battle 模式已废弃(仅用于查看/清理遗留环境;真 DS 用 -Mode k8s)。"
            Write-Step "业务服务容器(遗留)"
            docker compose -f $ComposeServices ps
            Write-Step "宿主 allocator + 其它宿主 go 进程(遗留)"
            & "$ScriptDir/run_services.ps1" -Action status
        }
        { $_ -in 'docker', 'intranet' } {
            Write-Step "docker 业务服务"
            docker compose -f $ComposeServices ps
            Write-Step "基础设施"
            docker compose -f $ComposeInfra --env-file $EnvFile ps
        }
        { $_ -in 'k8s', 'online' } {
            kubectl get pods,svc -n $K8sNamespace
        }
    }
}

# ===== 主流程 =====
Write-Host ""
Write-Host "============================================" -ForegroundColor Magenta
Write-Host " Pandora 后端一键启动器  ( $Mode )" -ForegroundColor Magenta
Write-Host "============================================" -ForegroundColor Magenta

if ($Status) { Show-Status; exit 0 }

# -GenTables:先把策划 xlsx 导成服务端配置表。必须排在起服务 / 重启服务之前 —— 读表的
# go 服务是在进程启动时从 configtable/dist 加载的,表还没生成就把服务起起来,读到的是上一批。
# 导表失败会直接 exit(见 Invoke-ConfigTableGen:改表就是为了测它,用旧表跑更难查)。
# -Down / -Check 是停机和干跑,导表对它们没有意义。
$script:ConfigTableChanged = $false
if ($GenTables -and -not $Down -and -not $Check) {
    $script:ConfigTableChanged = Invoke-ConfigTableGen
}

# -DsOnly:只重启本机 DS 的快速通道。刻意排在 Resolve-Prerequisites 之前 —— 后端正跑着时
# 那一整套前置(docker 探活 / k8s 互斥停机 / 8443-8444 端口空闲断言)要么白跑要么语义不对
# (端口正被自己的 Envoy 占着)。后端没在跑时 Invoke-LocalDsOnly 返回 $false,直接落回下面
# 的完整启动流程,前置检查一步都不少。
# 返回值只表示"这条快速通道处理了没有";处理了但 Hub DS 没等到就绪时,由 $DsOnlyExitCode
# 带出非零码 —— 双击窗口绿了就等于能进大厅,这个契约不能靠人去读滚动输出。
$script:DsOnlyExitCode = 0
if ($DsOnly -and (Invoke-LocalDsOnly)) { exit $script:DsOnlyExitCode }

$prereqOk = Resolve-Prerequisites $Mode

if ($Check) {
    Write-Host ""
    if ($prereqOk) { Write-Ok "$Mode 模式所需工具全部就绪。"; exit 0 }
    else { Write-Warn "$Mode 模式有工具缺失,见上方提示。"; exit 1 }
}

if (-not $prereqOk) {
    Write-Err "工具未就绪,已中止。装好后重跑(或新开终端刷新 PATH)。"
    exit 1
}

if ($BuildOnly) {
    Write-Step "只构建业务镜像(离线打包用,不启动任何服务;构建方式=$BuildMode$(if ($Only.Count -gt 0) { ";只构建 $($Only -join ',')" }))"
    Build-AllImages -Only $Only
    Write-Ok "业务镜像构建完成。可用 tools/scripts/export_images.ps1 打包导出。"
    exit 0
}

if ($Reset)  { Invoke-Reset;  exit 0 }
if ($Resume) { Invoke-Resume; exit 0 }

switch ($Mode) {
    'local'    { Invoke-Local }
    'docker'   { Invoke-Docker }
    'intranet' { Invoke-Intranet }
    'battle'   { Invoke-Battle }
    'k8s'      { Invoke-K8s }
    'online'   { Invoke-Online }
}
