# 策划本地启动手册(Pandora 后端)

> 稳定入口仍是 Docker；没有 Docker 的策划机可用
> `策划一键启动-免Docker-测试版.cmd`。两条路线都不需要会编译；免 Docker 测试入口连
> PowerShell 7 和基础设施都放在仓库 `run/` 下自举，不安装系统服务。SVN 工作副本在
> `installers/localinfra` 自带固定安装包；Git 检出缺包时才从官方源自动下载。

## 一、第一次准备(只做一次)

1. 使用稳定 Docker 入口时，安装 **Docker Desktop**:https://www.docker.com/products/docker-desktop/
   - 装完按提示重启电脑。
   - 启动 Docker Desktop,等右下角**鲸鱼图标变绿**(表示 Docker 已就绪)。
   - (如果你机器有 `winget`,也可以直接双击下面的启动脚本,它会尝试自动安装。)
   使用下文“免 Docker 测试入口”时跳过这一步，不要为了本项目安装或改动 Docker。
2. 用 Git 把本仓库拉到本地(已有就 `git pull` 更新到最新)。

## 二、日常使用

| 操作 | 怎么做 |
|---|---|
| 启动整套后端 | 命令行执行 `pwsh tools/scripts/play.ps1`(docker 模式,DS=mock) |
| 停止 | 命令行执行 `pwsh tools/scripts/play.ps1 -Stop`;旧机器遗留的含战斗环境执行 `pwsh tools/scripts/play.ps1 -Battle -Stop` 清理 |

### 没装 Docker：测试入口

双击 `策划一键启动-免Docker-测试版.cmd`。它会导表，并以原生 Windows 进程启动
Redis / Kafka / Envoy 与 22 个后端服务；SVN 已带固定安装包，Git/不完整目录才逐项联网。
窗口只有在 login、Envoy 的 Login 上游和本机 Hub DS 都通过 exact 进程归属检查后，才会显示
`Press any key to continue`；**看到这行就是现在可以登录进游戏**。启动失败会保留错误窗口，但不会
显示这条标准成功提示。

策划双击入口会按已部署证据自动选择数据库模式。`installers/planner-db/central-mysql.json` 是当前唯一
受支持的中心数据库 bundle：文件存在即锁定中心 MySQL，
本机不下载、解包、启动、停止或重置 MySQL。首次会要求输入一次性 enrollment code，
中心建好独立 workspace 后，密码只保存在当前 Windows 用户的 DPAPI 凭据文件。建库/迁移
正在处理时会按服务端节奏等待，总截止为 10 分钟。配置损坏、DNS/TLS/认证/schema
预检不通都会中止，**不会静默回退到本机 MySQL**。从未登记过中心 workspace 的机器若没有该
bundle，则立即使用下文 `local-owned` 动态端口；已有 central applied state、runtime profile 或
workspace identity 后 bundle 丢失仍会 fail-closed。精确规则见
[`decision-revisit-planner-database-fallback.md`](../design/decision-revisit-planner-database-fallback.md)。

当前中心 provisioner 只支持 Oracle MySQL 8+，不是 TiDB。远端 TiDB 方案仍处于
[`decision-revisit-planner-central-tidb.md`](../design/decision-revisit-planner-central-tidb.md)
的待拍板/阻断状态，不能只改 endpoint 冒充完成。

这条路线不会占用或复用 Docker MySQL 的 `3307`：本项目原生 MySQL 会从 `13307..13398`
自动选择可用端口，验证确属本工作区后记录在 `run/localinfra/cfg/ports.json`，服务配置副本生成到
`run/localinfra/cfg/services/`。机器上已有的 Docker/MySQL 不会被迁移、停止或删除；停止本项目也
只认本工作区的 mysqld 映像和 `my.ini`。启动包还必须带
`run/artifacts/windows/bin/pandora-migrate.exe`；缺少它会明确失败，不会带旧表结构继续启动。

第一次 `svn update` 会多取约 544.5 MiB 的第三方便携包，之后双击不需要使用者再下载或安装
PowerShell、MySQL、Redis、Kafka、JRE、mkcert、Envoy。脚本逐文件校验固定 SHA256 后才执行；
空目录或缺少某个当前版本文件时，只对该文件回退公网。同名包损坏会明确失败，请先重新
`svn update`，不要关闭校验。仓库内的本机包校验后直接解包，不再额外复制一份 544.5 MiB cache；
显式共享镜像仍可通过 `PANDORA_LOCALINFRA_MIRROR` 覆盖，并会先复制到稳定的本机 cache。
以后 SVN 更新了固定包，下一次启动会根据 dist 里的 SHA256 marker 自动重备，不会继续误用旧版。
若旧基础设施进程还占用对应目录，脚本会保留旧版并明确提示；先执行
`pwsh tools/scripts/start.ps1 -Mode local -NoDocker -Down`，再重新双击即可。不会为了换包停止外部
Docker/MySQL，也不会把下载或解包一半的目录当成可用版本。

如果要核对启动耗时、已修复的端口查询瓶颈或后续业务 exe 压缩边界，见
[`性能优化-策划一键启动-20260820.md`](性能优化-策划一键启动-20260820.md)。
查看状态：

```powershell
pwsh tools/scripts/local_infra.ps1 -Action status
```

本机模式状态里的 MySQL 应为 `OWNED`；中心模式为 `EXTERNAL-UP`。`FOREIGN` 表示
本机端口后来被外部进程占用，脚本不会碰它。
Docker 与免 Docker 来回切换时，脚本会依据 `run/dev/mysql-port-applied.json` 中的模式、端口和
社交库配置自动完整重启宿主服务，避免旧进程继续连接上一次模式的数据库。
日常再次验收时**直接重复双击启动即可，不需要先点停止**：健康且输入未变化的基础设施和服务保持
原进程，只有 Go 依赖或策划表变化真正影响的服务才会更新；重启电脑后进程虽全没了，安装与有效构建
收据仍在，会直接拉起缺失进程而不重装、不重编未变化服务。想关闭游戏后端但保留热基础设施，双击
`策划一键停止业务-保留基础设施-免Docker-测试版.cmd`：它只停 22 个业务服务及 allocator 拉起的
本机 Hub/Battle DS，不停止或启动 MySQL、Redis、Kafka、Envoy。`策划一键停止-免Docker-测试版.cmd`
仍用于你明确想把整套本机后端完全关掉时。

连续双击启动/停止也不会并发穿插：整条导表、基础设施、迁移和业务服务链共用工作区编排锁；
已有一轮在执行时，第二轮会明确退出，不会停掉刚由另一轮启动的 MySQL。

中心凭据同时绑定首次登记的 `MachineGuid + Windows SID` 摘要；复制 identity/凭据到
摘要不同的电脑会 fail-closed，不自动改绑。这不是 TPM 证明：如果位级系统镜像同时复制了
MachineGuid、SID 且 DPAPI 也可解密，客户端单独无法可靠识别，须由中心重复使用监控/终端
证明补强。PowerShell 会尽快释放 token/响应字符串引用，但 immutable string 不能承诺
物理清零；安全承诺是不写入仓库、日志、identity/profile 或进程命令行。

> 【已废弃 2026-07-14】「策划一键启动-含战斗.cmd」已删除:Windows DS 只在开发机
> `local` 模式下启动;要真实战斗请直接用客户端连**内网 k8s 服务器**
> (服务器机双击 `内网服务器一键启动-k8s集群.cmd`)。

- **首次启动**:会在容器内编译镜像,稍慢(几分钟),请耐心等。
- **之后启动**:复用缓存,很快。
- **更新后启动**:`git pull` 之后再双击启动，会按每个服务的真实 Go 依赖闭包只重建并重启受影响的
  运行实例；共享同一二进制的 `matchmaker` / `matchmaker_pve` 只构建一次、两个实例一起更新。
- 启动成功后,客户端网关在 **https://127.0.0.1:8443**。

## 二之二、改了资源想立刻在真链路里验证(免出包)

> 场景:你改了关卡/蓝图/数值,想**存盘后马上进游戏看效果**,又不想等程序出一次服务器包,
> 而且必须走**真正的登录 → 大厅 → 匹配 → 战斗**链路(不是单机 PIE)。

做法:让本机 DS 用**引擎的 `UnrealEditor.exe`** 跑,而不是打包好的 `PandoraServer.exe`。
两者都带 `-server`,对后端来说完全是同一种 DS,**登录/大厅/匹配一行代码都没变**;
区别只是 editor 形态直接读工程里**未 cook 的 `Content/`**,所以你存盘就生效。

| 操作 | 怎么做 |
|---|---|
| 启动 | 双击 `策划一键启动-免Docker-测试版.cmd` |
| **改完资源后重来一次(日常最常用)** | 双击 `策划一键重启DS-免Docker-测试版.cmd` |
| 停业务与本机 DS，保留 MySQL/Redis/Kafka/Envoy | 双击 `策划一键停止业务-保留基础设施-免Docker-测试版.cmd` |
| 完全关掉本机后端（非日常必需） | 双击 `策划一键停止-免Docker-测试版.cmd` |

**两个入口都会自己先导表**(策划 xlsx → `configtable/dist`),不用另外双击导表脚本。
**也都不需要先双击停止** —— 后端在跑就就地重启该重启的,没在跑就自动转成完整启动。

等价的命令行:

```powershell
# 导表 + 起后端 + 本机 DS(DS 用引擎跑,免出包)
pwsh tools/scripts/start.ps1 -Mode local -DsLauncher editor -GenTables

# 导表 + 只重启本机 DS 和读表的服务(其余原样不动,快)
pwsh tools/scripts/start.ps1 -Mode local -DsLauncher editor -GenTables -DsOnly

# 只停业务服务和本机 DS，保留基础设施
pwsh tools/scripts/run_services.ps1 -Action down

# 完整停止
pwsh tools/scripts/start.ps1 -Mode local -NoDocker -Down
```

### 改了策划表怎么生效

直接双击上面那两个入口之一就行,**导表已经内建在里面**了。理由很简单:改表就是为了测这一改动,
把导表做成一个"要记得先点"的独立步骤,迟早会漏一次,而漏掉的表现是「我明明改了表却没生效」,
最难自查。

- **导表不过 → 直接中止,不会带着旧表把服务起起来**。配置表是**全批原子**的:任一张表校验不过
  就整批不产出,`configtable/dist` 一个字节不动,原来的表还能用。窗口里会写明原因和该找谁
  (未登记新列要找程序加字段;外键对不上、xlsx 被 Excel 占着这类自己就能改)。
- **表真的变了,才会重启读表的服务**(`player` / `battle_result` / `ds_allocator` / `inventory` /
  `dialogue` / `mission` / `matchmaker` / `matchmaker_pve`)—— 它们是在**进程启动时**加载
  `configtable/dist` 的,不重启就还是旧表。若同一服务的 Go 代码也变了，只重启一次。
  内容没变就一个都不碰,少一次无谓的断线。
- `策划一键导表.cmd` 仍然留着,但日常用不到了;它给「只想验证表能不能导过 / 要把产物提交给别人」
  这种不起服务的场景用。

### 客户端仓放哪儿、叫什么名字

**叫什么名字都行。** 客户端仓在 SVN 上的名字是 `Client`(`^/trunk/Client`),用
`svn checkout` 不指定目标目录时检出来就叫 `Client`;老文档里写的是 `Pandora-Client-SVN`;
你自己改成中文名也可以。脚本按"内容"认仓(底下有 `Table\*.xlsx`),不按名字认。

**放哪儿**:最省事的是**和后端仓放在同一个父目录下**,双击就能自动找到:

```
D:\game\
  ├─ XuanMing-Server\     ← 后端(你双击 .cmd 的地方)
  └─ Client\              ← 客户端 SVN(叫什么都行)
```

没放一块也能找到(脚本会在各个盘的根目录和它们下一层找 `Client` / `Pandora-Client-SVN`
这几个名字)。要是还是找不到,窗口里会把找过的地方全列出来,并给你两条选一条:

```powershell
# 设一次,以后双击就行(设完要重开窗口)
setx PANDORA_CLIENT_REPO D:\你的客户端目录
```

```powershell
# 只想这一次跑通,不改机器设置
pwsh tools\scripts\configtable_gen.ps1 -TableRoot D:\你的客户端目录\Table
```

**如果窗口提示「本机还有别的策划表目录」**:说明你机器上有不止一份客户端检出。它会写明这次
用的是哪一份 —— 确认那就是你正在改表的那一份;不是的话按上面两条之一指过去。这个提示很重要:
用错检出的表现是"我明明改了表却没生效",最难自己查出来。

**新电脑取不到 SVN 版本号**:导表会把源表 revision 写进产物,所以日常双击自动导表时 `Table`
必须来自真正的 SVN checkout,不能是别人复制的普通文件夹或 export。脚本会自动从
PATH 和 TortoiseSVN 注册表找 `svn.exe`(安装在非 C 盘也可以);只装了右键菜单、
没有 CLI 时,还会回退到 TortoiseSVN 自带的 SubWCRev。如果两者都读不到,先确认目录
真是 checkout;再重跑 TortoiseSVN 安装器并勾选 **command-line client tools**。也可以显式指定已安装的命令:

```powershell
setx PANDORA_SVN_EXE "D:\Program Files\TortoiseSVN\bin\svn.exe"
```

设完要重开窗口;不要为了绕过报错沿用旧表,否则会把新改动伪装成已生效。
命令行 `-SourceRev svn-r<N>` 只给「程序已核实普通导出快照的精确来源」的特殊流程用;
日常策划不要靠猜一个版本号来放行。

### 为什么日常该用「重启 DS」而不是再启动一次

一天里绝大多数改动都只在**客户端仓**(改资源,或重编了编辑器 DLL),后端 go 服务一行没动。
但完整启动每次都要走:等基础设施容器 healthy → 起 TiDB → 跑数据库迁移 → 21 个 go 服务
逐个 build / 启动 / 端口探活。这些步骤没有一步和你改的资源有关,纯属白等。

`策划一键重启DS-免Docker-测试版.cmd` 只做真正相关的事:

- **不碰** docker 基础设施、TiDB、数据库迁移、21 个 go 服务(它们继续跑,连重启都没有);
- 杀掉正在跑的本机 DS —— editor 形态是在**进程启动时**读未 cook 的 `Content/`,
  已经跑着的那个进程内存里还是旧资源,不重启就是看不到你的改动;
- 重启 `hub_allocator` / `ds_allocator` 两个进程,新的 Hub DS 会自己起来并读到最新资源。
  (为什么必须连 allocator 一起重启:常驻 Hub DS 在一个 allocator 进程里**只懒拉起一次**,
  光杀 DS 没人再把它拉起来,表现成"登录后永远进不了大厅"。)
- **等到真能进为止才结束**。窗口显示 `[ OK ] Hub DS 已监听 ...` 就是可以登录了,不用反复试。
  你也**不需要先登录**去"触发"它 —— allocator 每 5 秒对账一次分片拓扑,会自己把 DS 拉起来,
  加载全程在后台跑。

实测耗时(热机、图之前进过):

| 阶段 | 从双击算起 |
|---|---|
| 两个 allocator 起回来 | ~10s |
| Hub DS 进程被拉起 | ~18s |
| 关卡加载完、开始监听 → **可以进了** | ~44s |

首次进一张全新的图会明显更慢(DS 要现场构网格/贴图的 DDC),脚本最多等 300s
(和后端给 editor 形态放宽的就绪等待同一个数),超时不会假装成功,会告诉你去看哪个日志。

注意两点:

- **本机正在进行的战斗会被中断**(战斗 DS 是 `ds_allocator` 的子进程),重进即可;
- **改了 go 服务代码、或后端同学给了新的 `run/artifacts` 二进制** → 那就得走完整启动,
  双击 `策划一键启动-免Docker-测试版.cmd`。

后端还没起来时双击「重启」也没关系:脚本发现后端没在跑,会自己改走完整启动流程并说明原因,
不需要你分辨今天该点哪个图标。

- **引擎/工程路径不用填**:脚本按 `Pandora.uproject` 里的 `EngineAssociation` 查注册表定位引擎
  (源码版走 `HKCU\...\Unreal Engine\Builds` 的 GUID,Epic 发行版走 `HKLM\...\EpicGames\Unreal Engine\<版本>`),
  工程则在「与本仓库平级的客户端仓」里自动找。所以你引擎装在 `E:\Program Files\UE_5.8` 还是别的盘都行。
  实在找不到再显式指定:`-DsProject <...\Pandora.uproject>` / `-DsEditorExe <...\UnrealEditor.exe>`。
- **DS 启动会慢**:要加载一大批编辑器模块 + 读未 cook 的散装资产(首次进新图还可能现场构网格/贴图的 DDC),
  首次进大厅/进图等一两分钟是正常的
  (后端已自动把就绪等待放宽到 300s、心跳超时放宽到 120s,不会被误判掉线回收)。
  > 顺便澄清一个常见误解:这个形态**不会编 shader**。`-server` 下引擎认为自己是专服,
  > 会直接跳过全局着色器和材质着色器的编译(引擎源码 `AllowGlobalShaderLoad()` /
  > `FApp::CanEverRender()` 都带 `!IsRunningDedicatedServer()`)。要编 shader 的是
  > listen server / PIE 那类要出画面的形态。
- **不传 `-DsLauncher` 就是原来的行为**(跑打包好的 `PandoraServer.exe`),什么都没变。
- 改**代码**(C++/蓝图节点新增)仍需编译;这个模式解决的是**改资源**的即时验证。

### 本机没装 Go 怎么办

`local` 模式是「宿主跑 21 个 Go 进程 + Docker 跑基础设施」。默认要现场 `go build`,所以需要 Go 工具链。
不想装 Go 的话,让后端同学在他机器上跑一次:

```powershell
pwsh tools/scripts/build_release_binaries.ps1 -Zip
```

把生成的 `run/artifacts` 目录(或那个 zip 解开)放到你本地仓库的同名位置即可 ——
启动脚本检测到本机没有 Go 会**自动改用这批预编译二进制**,秒级启动。
以后升级也只需**替换 `run/artifacts` 这一个目录**,不用重装任何东西。

> 上述普通 `local` / Docker 入口绕不开 **Docker Desktop**。若机器不能安装 Docker，改用
> `策划一键启动-免Docker-测试版.cmd`；它不起 TiDB/观测栈，并用仓库内免安装二进制代替
> MySQL/Redis/Kafka/Envoy，具体限制以上文“没装 Docker：测试入口”为准。

## 三、机器拉不到镜像（内网 / 断网 / 镜像加速失效）

Docker 业务镜像已不再随 git/svn 提交；`deploy/offline-images/pandora-images.tar` 的旧方案已退役。
需要 Docker 离线镜像时，由后端通过 `publish_offline_images.ps1` 发布到制品目录，再在目标机运行
`fetch_offline_images.ps1` 校验取回。`installers/localinfra` 只服务于上面的**免 Docker**入口，
不包含任何业务 Docker 镜像。

## 四、常见问题

- **提示 Docker 没装 / 没运行**:把 Docker Desktop 启动起来,等鲸鱼图标变绿,再双击启动。
- **第一次特别慢**:正常,是在下载基础镜像 + 编译服务,只有第一次这样。
- **想看服务起没起来**:命令行执行 `pwsh tools/scripts/play.ps1 -Status`。
- **数据会丢吗**:停止不会删数据(MySQL/Redis 数据卷保留),下次启动数据还在。
- **报错了**:把窗口里**红色 `[ERR]`** 那几行截图发给后端同学。

## 五、原理(给好奇的人)

- 策划机器上**只要 Docker**:服务是在 Docker 容器里编译和运行的(多阶段 Dockerfile),
  所以不用在本机装 Go,也不会有"构建产物"提交进仓库。
- 这套脚本是 `tools/scripts/start.ps1 -Mode docker` 的"策划友好包装"
  (`tools/scripts/play.ps1`),真正的构建/启动复用已验证的链路。
- 开发同学要断点调试用 `local` 模式:见 `tools/scripts/start.ps1` 注释。
