# 新机器搭建手册

把一台干净的 Windows 机器变成 **开发机 + 构建机 + CI 机**。

## 一键入口

先手工检出后端仓库（脚本在里面，鸡生蛋的部分只有这一步）：

```powershell
git clone https://github.com/luyuan-go/XuanMing-Server.git D:\XuanMing-Server
```

然后：

```powershell
cd D:\XuanMing-Server
pwsh tools\devops\bootstrap-machine.ps1 -WhatIfOnly      # 先看它要做什么，不改任何东西
pwsh tools\devops\bootstrap-machine.ps1 -Install         # 实际搭建，缺的工具用 winget 装
```

它按正确顺序串起：前置工具检查 → 机器级环境变量 → 源码工作副本校验 →
`up.ps1`（CI 栈）→ `setup-jenkins.ps1` + `install-agent-service.ps1` → `preflight-buildmachine.ps1`。
**幂等，可以反复跑**；有阻断项时会停在那里并说清缺什么，不会带着半套配置往下走。

只搭其中一部分用 `-Role Dev` / `-Role Build` / `-Role CI`（可组合）。

**脚本刻意不做的四件事**（都会在结尾的「仍需人工完成」里列出来）：

| 不做 | 为什么 |
|---|---|
| 下 51GB UE 引擎 | 体积大、来源因人而异，只报告缺不缺 |
| 装 Linux 交叉编译工具链 | 同上（3.3GB） |
| 检出客户端仓库 | 89GB 级别，"一键"不该静默拉一整天；要拉加 `-CheckoutClient` |
| 碰任何口令 | Jenkins 的客户端 SVN 凭据必须你本人在它界面上建；公开后端 Git 不需要凭据 |

下面是这些步骤各自在做什么、以及踩过的坑 —— 一键流程卡住时按这里排查。

---

## 0. 要搬过去的东西

| 内容 | 来源 | 体积 | 备注 |
|---|---|---|---|
| **UE 引擎** | `E:\GitRepos\PandoraEngine.git`（裸仓库） | **51 GB** | 已编译的 installed build，**clone 完不用再编译** |
| **Linux 交叉编译工具链** | `C:\UnrealToolchains\v26_clang-20.1.8-rockylinux8\` | 3.3 GB | ⚠️ **不在引擎仓库里**，必须单独装或拷 |
| **客户端源码** | SVN `^/trunk/Client` | 大 | `svn checkout` |
| **后端源码** | GitHub `luyuan-go/XuanMing-Server` | 小 | `backend-dev` 跟踪 `main`，版本戳为 `g<sha>` |

SVN 仓库根：`http://infinity-svn/svn/Pandora-Moba`

> **后端 CI 来源**（2026-08-11 调整）：`backend-dev` 以
> `https://github.com/luyuan-go/XuanMing-Server.git` 的 `main` 为准，提交推送后由 Jenkins
> `pollSCM` 发现并构建，后端制品版本戳为 `g<sha>`。客户端仍以 SVN `^/trunk/Client`
> 为准，客户端与 DS 包继续使用 `r<rev>`。

引擎那 51 GB 是大头，按内网带宽预留时间。

---

## 1. 引擎：clone + **注册**

```powershell
git clone E:\GitRepos\PandoraEngine.git D:\PandoraEngine
```

（路径按实际共享方式调整；也可以直接拷贝整个裸仓库过去再本地 clone。）

**注册这一步不能漏**：

```powershell
D:\PandoraEngine\Engine\Binaries\Win64\UnrealVersionSelector.exe /register
```

注册会往 `HKCU:\SOFTWARE\Epic Games\Unreal Engine\Builds` 写条目。
**不注册的后果**：`Tool\Build\PackageSet.ps1` 挑不到自制引擎，会退到 Epic Launcher 发行版，
然后 Server 目标直接失败：`Server targets are not currently supported from this engine distribution`。

---

## 2. Linux 工具链

装 UE 官方 Linux 交叉编译工具链（版本要与引擎匹配，本项目当前为 `v26_clang-20.1.8-rockylinux8`），
或直接从原机器拷贝整个目录过去。

---

## 3. 环境变量（**设完必须重开终端**）

```powershell
setx LINUX_MULTIARCH_ROOT "C:\UnrealToolchains\v26_clang-20.1.8-rockylinux8\"
```

```powershell
setx PANDORA_ARTIFACT_ROOT "D:\artifacts"
```

- `LINUX_MULTIARCH_ROOT`：不设或路径不存在 → 打 Linux DS 时 `PackageSet.ps1` 直接 throw。
- `PANDORA_ARTIFACT_ROOT`：制品库根目录，**按这台机器的实际盘符设**。
  不设会退到硬编码默认值 `F:\work\artifacts`，新机器上通常不存在 →
  发布落空、DS 包解析不到。

### ⚠️ 制品根必须是文件系统路径，不能是 URL

发布与解析脚本用的是 **文件系统 API**（`robocopy` / `Test-Path` / `Get-FileHash` / `Move-Item`）。
把制品根设成 URL **不会报错，只会静默失效**（`Test-Path` 直接返回 `False`，
表现为「找不到任何制品」），是最难排查的一类故障。

- ✅ 本地路径：`D:\artifacts`
- ✅ SMB 共享 UNC：`\\主机\共享\artifacts`（`robocopy` 原生支持）
- ❌ 任何 URL 形式

自检脚本会对 URL 形态直接判 FAIL。

> **团队怎么拿包**：制品根是本机的**权威制品库**，不直接对外。
> 分发走 MinIO —— `artifacts-sync` 流水线会在构建成功后自动把已发布产物同步上去，
> 详见 [`publish-to-minio.ps1`](./publish-to-minio.ps1) 与 [`Jenkinsfile.artifacts-sync`](./Jenkinsfile.artifacts-sync)。

---

## 4. 拉后端 Git 与客户端 SVN

```powershell
git clone https://github.com/luyuan-go/XuanMing-Server.git D:\XuanMing-Server
```

```powershell
svn checkout http://infinity-svn/svn/Pandora-Moba/trunk/Client D:\Pandora-Client-SVN
```

> 目录名 `Pandora-Client-SVN` 只是这里的例子,不是硬要求 —— 不写目标目录时 `svn checkout`
> 会按 SVN 原名检出成 `Client`,两种都行。导表 / 起 DS 的定位逻辑在
> `tools/scripts/client_repo_lib.ps1`,按内容认仓不按名字认;把客户端仓和后端仓放在同一个
> 父目录下最省事,否则设 `PANDORA_CLIENT_REPO` 指到仓根。

路径按这台机器的实际盘符定；下面第 5 步的 `-ClientRepo` 与 `.env` 里的
`PANDORA_PROTO_SERVER_ROOT` 要跟着改成实际路径。

> 命令行 SVN 客户端仍是客户端打包的硬前提：客户端发布脚本用 `svnversion` 取版本戳。
> **Jenkins 的 Subversion 插件不提供 `svn.exe`**，必须单独装（TortoiseSVN 勾选 command line tools，
> 或 `winget install --id CollabNet.Subversion`）。缺它客户端的 SVN 版本识别与打包前置检查会失败；
> 后端 Git 制品使用 `git` 生成 `g<sha>`，不依赖 `svnversion`。

---

## 5. 自检

```powershell
pwsh tools\devops\preflight-buildmachine.ps1 -ClientRepo <客户端工作副本路径>
```

逐项检查：PowerShell 7 / svn / git / docker / go、UE 编辑器是否占用、
`LINUX_MULTIARCH_ROOT`、已注册的源码引擎、客户端三个打包脚本是否齐全、制品根、磁盘空间。

退出码 `0` = 无阻断，`1` = 有阻断。**全绿再往下走。**

---

## 6. 打包 → 发布 → 打镜像

```powershell
pwsh Tool\Build\PackageSet.ps1 -Flavors 'Server/Linux/Development'
```

```powershell
pwsh Tool\Build\PublishPackages.ps1 -AllowDirty
```

```powershell
pwsh deploy\ds\build-image-minikube.ps1 -BuildOnHost
```

- 第 1 步硬前提：**UE 编辑器必须关闭**（会阻塞 UBT）、源码引擎已注册、`LINUX_MULTIARCH_ROOT` 已设。
- 第 2 步 `-AllowDirty`：工作副本非纯净版本时放行，版本号会带 `-dirty-<时间戳>` 并进 `snapshots/` 快照轨。
  纯净版本且要发正式版时改用 `-Version vX.Y.Z`，进 `releases/` 轨。
  **只有 `Package.bat` / `PackageSet.ps1` 的产出带 `BUILD_INFO.txt` 才能发布**，
  缺它会被发布器判为「不是完整打包产物」拒收。
- 第 3 步想先确认包来源，加 `-ResolveOnly`：只打印解析到哪个包、不同步不构建。
  要把镜像推到制品 registry 再加 `-PushRegistry`（必须同时带 `-BuildOnHost`）。

---

## 已知坑（都是踩过的）

1. **引擎会翻回 Epic 发行版**：工程 `EngineAssociation` 若指向 `"5.8"` 而非注册的自制引擎 GUID，
   重启后尤其容易翻，导致 Server 目标编译失败。
2. **DS 包来源已改为制品库**（2026-07-25）：客户端 `Packages` 目录不再参与分发，
   `build-image-minikube.ps1` 优先从制品库解析。解析优先级：
   `-SourcePkg` > `PANDORA_DS_LINUX_PKG` > 制品库（两轨取最新）> 同级 `Packages`（已退役，仅兼容）。
3. **降级守卫**：解析到的包若比 `deploy/ds/stage` 里已有的更旧，构建会 **fail-closed 拒绝**，
   避免 `robocopy /MIR` 用旧二进制静默覆盖新的、让镜像退回老代码。
   确需回滚加 `-AllowOlderPackage`。
4. **快速迭代产物进不了制品库**：`Tool\Server\Agones\build-linux-ds.ps1` 输出到 `PandoraDSArchive`，
   **不写 `BUILD_INFO.txt`**，会被发布器拒收。目前只有全量 `PackageSet.ps1` 的产出可发布。
5. **PowerShell 数组 splat 传 switch 参数会误绑**：`& script @('-Build','-BuildMode','host')`
   会把 `-Build` 当成 `-BuildMode` 的值。要显式传参或用 hashtable splat。
