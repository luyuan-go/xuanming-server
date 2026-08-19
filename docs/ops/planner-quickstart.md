# 策划本地启动手册(Pandora 后端)

> 给策划的最简上手:**只要装 Docker,双击一个文件,整套后端就跑起来。**
> 不需要装 Go、不需要会编译。

## 一、第一次准备(只做一次)

1. 安装 **Docker Desktop**:https://www.docker.com/products/docker-desktop/
   - 装完按提示重启电脑。
   - 启动 Docker Desktop,等右下角**鲸鱼图标变绿**(表示 Docker 已就绪)。
   - (如果你机器有 `winget`,也可以直接双击下面的启动脚本,它会尝试自动安装。)
2. 用 Git 把本仓库拉到本地(已有就 `git pull` 更新到最新)。

## 二、日常使用

| 操作 | 怎么做 |
|---|---|
| 启动整套后端 | 命令行执行 `pwsh tools/scripts/play.ps1`(docker 模式,DS=mock) |
| 停止 | 命令行执行 `pwsh tools/scripts/play.ps1 -Stop`;旧机器遗留的含战斗环境双击 `策划一键停止.cmd` 清理 |

> 【已废弃 2026-07-14】「策划一键启动-含战斗.cmd」已删除:Windows DS 只在开发机
> `local` 模式下启动;要真实战斗请直接用客户端连**内网 k8s 服务器**
> (服务器机双击 `内网服务器一键启动-k8s集群.cmd`)。

- **首次启动**:会在容器内编译镜像,稍慢(几分钟),请耐心等。
- **之后启动**:复用缓存,很快。
- **更新后启动**:`git pull` 之后再双击启动,会自动重建有改动的服务。
- 启动成功后,客户端网关在 **https://127.0.0.1:8443**。

## 二之二、改了资源想立刻在真链路里验证(免出包)

> 场景:你改了关卡/蓝图/数值,想**存盘后马上进游戏看效果**,又不想等程序出一次服务器包,
> 而且必须走**真正的登录 → 大厅 → 匹配 → 战斗**链路(不是单机 PIE)。

做法:让本机 DS 用**引擎的 `UnrealEditor.exe`** 跑,而不是打包好的 `PandoraServer.exe`。
两者都带 `-server`,对后端来说完全是同一种 DS,**登录/大厅/匹配一行代码都没变**;
区别只是 editor 形态直接读工程里**未 cook 的 `Content/`**,所以你存盘就生效。

| 操作 | 怎么做 |
|---|---|
| 启动 | 双击 `策划一键启动-改资源即时生效.cmd` |
| **改完资源后重来一次(日常最常用)** | 双击 `策划一键重启DS-读最新资源.cmd` |
| 停止 | 双击 `策划一键停止-改资源即时生效.cmd` |

**两个入口都会自己先导表**(策划 xlsx → `configtable/dist`),不用另外双击导表脚本。
**也都不需要先双击停止** —— 后端在跑就就地重启该重启的,没在跑就自动转成完整启动。

等价的命令行:

```powershell
# 导表 + 起后端 + 本机 DS(DS 用引擎跑,免出包)
pwsh tools/scripts/start.ps1 -Mode local -DsLauncher editor -GenTables

# 导表 + 只重启本机 DS 和读表的服务(其余原样不动,快)
pwsh tools/scripts/start.ps1 -Mode local -DsLauncher editor -GenTables -DsOnly

# 停止
pwsh tools/scripts/start.ps1 -Mode local -Down
```

### 改了策划表怎么生效

直接双击上面那两个入口之一就行,**导表已经内建在里面**了。理由很简单:改表就是为了测这一改动,
把导表做成一个"要记得先点"的独立步骤,迟早会漏一次,而漏掉的表现是「我明明改了表却没生效」,
最难自查。

- **导表不过 → 直接中止,不会带着旧表把服务起起来**。配置表是**全批原子**的:任一张表校验不过
  就整批不产出,`configtable/dist` 一个字节不动,原来的表还能用。窗口里会写明原因和该找谁
  (未登记新列要找程序加字段;外键对不上、xlsx 被 Excel 占着这类自己就能改)。
- **表真的变了,才会重启读表的服务**(`player` / `battle_result` / `matchmaker` / `matchmaker_pve`
  / `ds_allocator`)—— 它们是在**进程启动时**加载 `configtable/dist` 的,不重启就还是旧表。
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

### 为什么日常该用「重启 DS」而不是再启动一次

一天里绝大多数改动都只在**客户端仓**(改资源,或重编了编辑器 DLL),后端 go 服务一行没动。
但完整启动每次都要走:等基础设施容器 healthy → 起 TiDB → 跑数据库迁移 → 21 个 go 服务
逐个 build / 启动 / 端口探活。这些步骤没有一步和你改的资源有关,纯属白等。

`策划一键重启DS-读最新资源.cmd` 只做真正相关的事:

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
  双击 `策划一键启动-改资源即时生效.cmd`。

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

> 唯一绕不开的安装项仍然是 **Docker Desktop**:MySQL/Redis/Kafka/etcd 都跑在容器里,
> 而 Docker 在 Windows 上是系统级组件(装虚拟化 + 驱动 + 服务),没法塞进项目目录里"绿色免安装"。
> 除它之外的东西(镜像、二进制、配置、证书)都已经在仓库目录里,拷过去就能用。

## 三、机器拉不到镜像（内网 / 断网 / 镜像加速失效）

若这台机器连不上 Docker Hub / 国内加速站(双击启动时会卡在「拉 golang / alpine 镜像失败」,
TLS 超时 / EOF / 403),**不用做任何额外操作**:仓库里带了离线镜像包
`deploy/offline-images/pandora-images.tar`(随 git/svn 同步),双击启动脚本时会
**自动检测并导入**,导入后直接起服务。

你要做的只有:

1. `git pull` / `svn update` 确保拿到最新的 `deploy/offline-images/pandora-images.tar`。
2. 双击一键启动脚本即可(脚本自动导入离线镜像 + 起服务,无需手动命令)。

> 离线包由能联网的机器用 `pwsh tools/scripts/export_images.ps1 -Build` 生成并提交。
> 基础设施(mysql/redis/kafka 等)不在包内;若目标机基础设施也拉不到,在联网机用
> `-IncludeInfra -Out D:\pandora-full-images.tar` 另打仓库外的大包，不能覆盖仓库受管业务包。
> 若这台机器其实能联网、想强制重新构建最新镜像:命令行加 `-Rebuild`(如 `pwsh tools/scripts/start.ps1 -Mode docker -Rebuild`)。

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
