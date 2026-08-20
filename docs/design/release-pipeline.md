# Pandora 打包发布线(制品不进版本库)

> 2026-07-23 落地。回应「Packages 提交进 SVN / pandora-images.tar 提交进 git」两个反模式,
> 按业界标准四层分离改造:版本库只放源码,构建产物进制品目录,发布按 manifest 可追溯可回滚。
> 决策登记:`pandora-arch.md` §11「镜像分发 2026-07-23」行;旧「离线镜像包随仓库同步」过渡方案同日退役。
>
> **受控例外(2026-08-20)**:免 Docker 策划机所需的固定第三方便携包可在策划 SVN 的
> `installers/localinfra/` 白名单目录随工作副本分发，Git 同路径不含二进制。它们不是 Pandora
> 构建产物，仍须固定运行时 SHA256，并由上游 checksum/签名/digest 独立核验；业务 exe、UE Packages、OCI/Docker 镜像
> 继续禁止回流版本库。决策、成本和更新门禁见 `decision-revisit-localinfra-svn-bundle.md`。
>
> 本文档聚焦**四层架构本身**(§1-§6b:版本库钩子、制品目录脚本、CI 流水线定义、版本化发布)。
> 业界标准工具(Jenkins/MinIO/Harbor/GoReleaser/ArgoCD)的**实际安装与操作**见 `tools/devops/README.md`
> (一键 docker compose 栈,含 Jenkins+registry+MinIO,及 Harbor/GoReleaser/ArgoCD 现状);
> 本文档 §7 只做工具选型的定位与状态摘要,不重复其操作细节。

## 1. 四层架构

```
版本库层(SVN/git,只有源码) → CI 构建层(Jenkins) → 制品目录层(不可变+版本号) → 发布层(manifest 晋级)
```

| 层 | 载体 | 本项目落点 |
|---|---|---|
| 版本库 | 客户端 SVN + 后端 git | Packages/ 已解除纳管+svn:ignore;镜像 tar 已移出 git;服务端钩子拒收回流 |
| CI | Jenkins | 客户端 `Tool/Build/Jenkinsfile`(已有,本次改造)+ 后端仓根 `Jenkinsfile`(新增) |
| 制品目录 | 本地/共享盘目录 | `PANDORA_ARTIFACT_ROOT`(默认 `F:\work\artifacts`);已可平移 MinIO/Harbor,见 §7 |
| 发布 | release manifest | `make_release.ps1` 产出 `releases/<name>.json`,离线交付按 manifest 取制品 |

游戏行业再具体一层的参照:Epic 自家和多数 UE 大厂用 **Perforce**(二进制资产锁文件 + streams)存源码/美术资产,
SVN 是可接受替代;后端纯 Go 服务用 git 是标准做法,两者都**不需要为了"标准"而换**。
构建分级(标准做法的一部分,本项目当前两轨对应前两级,第三级用 release 轨的手动 VERSION 触发实现):
per-commit 快验编译(dev 快照轨,§5)→ nightly 全量 cook(可用 dev 轨 pollSCM 间隔实现,未强制)→ release 打正式包(release 轨)。
制品库三条铁律里的"digest 寻址"(部署引用 sha256 摘要而非 `latest`)已在 `build-info.json`/`images-manifest.json`
落地(§2);K8s 侧 digest 寻址需接 Harbor/registry 后由部署清单显式钉 digest,当前离线 tar 模式下按 sha256sums 校验等效替代。

## 2. 制品目录布局与铁律

**两轨分仓**(Snapshot 快照轨 / Release 发布版本轨,类比 Nexus SNAPSHOT/RELEASE 仓):

```
<PANDORA_ARTIFACT_ROOT>\
├── snapshots\                                    dev 快照轨(来源戳命名,激进清理)
│   ├── client\<branch>\<flavor>\r<svn版本>\        UE 包
│   ├── images\<g+git短sha>\pandora-images.tar      业务镜像离线包
│   └── images\latest.json                          快照指针
└── releases\                                     发布版本轨(语义版本,不可变,永久保留)
    ├── client\<branch>\<flavor>\<版本>\             UE 包
    ├── images\<版本>\pandora-images.tar             业务镜像离线包
    ├── images\latest.json                          发布指针
    └── manifests\<版本>.json / <版本>.md            release manifest + 人可读 notes
```

轨道由**有没有版本号**决定:发布脚本带 `-Version` = release 轨(releases\,目录名=版本号);
不带 = snapshot 轨(snapshots\,目录名=git sha / svn rev)。每个制品目录都带 `build-info.json`(含 channel)+ `sha256sums.txt`。

三条铁律(脚本强制):

1. **不可变**:版本目录已存在即拒绝覆盖(CI 幂等重跑用 `-SkipIfExists` 静默跳过);
2. **原子发布**:内容先写 `.tmp-` staging,再整目录 rename 上线,不存在半截制品;
3. **可追溯**:snapshot 版本号=源码版本(SVN rev / git sha);release 版本号=语义版本。
   脏工作区在 release 轨默认拒绝(`-AllowDirty` 仅限内测);snapshot 轨允许脏(带 `-dirty-时间戳`)。

清理:`artifacts_retention.ps1` **只清 snapshots\**(每流留最近 N),`releases\` 永不触碰。

## 3. 脚本清单

| 脚本 | 仓库 | 职责 |
|---|---|---|
| `Tool/Build/PublishPackages.ps1` | 客户端 | Packages\<flavor> → `client\...\r<rev>`;svnversion 强校验 |
| `tools/scripts/publish_offline_images.ps1` | 后端 | 复用 `export_images.ps1` 出 tar → `images\<gitsha>`;从 tar manifest 提镜像 ID |
| `tools/scripts/fetch_offline_images.ps1` | 后端 | 制品目录 → `deploy/offline-images/pandora-images.tar`(校验后落地;下游一键启动/import 流程不变) |
| `tools/scripts/make_release.ps1` | 后端 | 生成 release manifest(镜像版本+UE 包引用+configtable manifest 摘要) |
| `tools/scripts/artifacts_retention.ps1` | 后端 | 每流保留最近 N 版(默认 10);release 引用的版本永不删;默认 dry-run |
| `tools/scripts/artifacts_lib.ps1` | 后端 | 公共函数(root 解析/sha256sums/原子发布),被上述脚本 dot-source |
| `tools/scripts/ci_backend.ps1` | 后端 | CI 构建入口:按 go.work use 清单逐模块 build+test |

## 4. 版本库防回流(服务端钩子,`tools/vcs-hooks/`)

本地 ignore 只是君子协定,强制力在服务器端:

- **SVN**(客户端仓):`svn-pre-commit.sh`(Linux svnserve/Apache)/ `svn-pre-commit.bat` + `.ps1`(VisualSVN)。
  黑名单:`Packages/`、任意层级 `Saved/ Intermediate/ DerivedDataCache/`、`*.tar *.pak *.ucas *.utoc`。
  **注意:本仓有意纳管 `Pandora/Binaries`(策划靠 svn 同步编辑器 DLL),Binaries 不拉黑。**
  救急放行:提交日志带 `[hook-override]`(仅管理员)。部署需 SVN 服务器管理员按脚本头部说明挂载。
- **git**(后端仓):`git-pre-receive.sh`(自建裸仓库);托管平台改用 GitHub push ruleset / GitLab push rules
  (路径 `*.tar` 拒收 + 单文件 50MB 上限)。

## 5. CI 流水线(两轨:dev 快照自动 / release 版本手动)

每个仓库两条流水线,同分支双触发:

| 轨 | 后端 | 客户端 | 触发 | 产出 |
|---|---|---|---|---|
| **dev 快照** | `Jenkinsfile` | `Tool/Build/Jenkinsfile` | pollSCM 自动 | snapshots\(git sha / r<rev>) |
| **release 版本** | `Jenkinsfile.release` | `Tool/Build/Jenkinsfile.release` | 手动传 VERSION | releases\(vX.Y.Z) + manifest |

- **dev 后端**:pollSCM → `ci_backend.ps1`(全模块 build+test)→ `publish_offline_images.ps1 -SkipIfExists`(无版本 → snapshot)。
- **dev 客户端**:改动检测 → Preflight → Package.bat → `PublishPackages.ps1 -SkipIfExists`(无版本 → snapshot);
  `Package.bat` 在 `BUILD_INFO.txt` 落 `Version=<ProjectVersion>` + `Revision=<svn rev>`。
- **release 后端**:手动 VERSION → build+test → `publish_offline_images.ps1 -Version <V>`(镜像自报版本)→ `make_release.ps1 -Version <V>`。
- **release 客户端**:手动 VERSION → `PackageSet.ps1 -Version <V>`(构建 + `PublishPackages -Version` 发 releases;DS 自动锁源码引擎)。
- **UE DS 引擎坑**:Server(DS)目标必须用能编 Server 的**源码/自制引擎**;Epic 发行版(launcher)引擎会报
  `Server targets are not currently supported from this engine distribution`。`PackageSet.ps1` 打 Server 时
  自动从注册表挑源码引擎(或 `-EnginePath` 指定),避免默认解析翻到发行版。
  构建机要求:Go 1.26.5、Docker Desktop、pwsh、git、svn 命令行(客户端节点另需 UE 引擎)。

镜像**在线发布**(推 registry)已有独立机制:`start.ps1 -BuildPush`(clean commit 强制 + 不可变 tag 门禁),
与本离线制品线并行,互不替代;`tools/devops` 一键栈已起 `registry:2`(Harbor 轻量替代)作为过渡,
`deploy/ds/build-image-minikube.ps1 -PushRegistry` 已接线;真正装 Harbor 后按 §7 表格切换即可,
离线 tar 流届时退化为"发布时从 registry 现场导出"。

## 6. 分发方式迁移对照

| 消费场景 | 旧方式 | 新方式 |
|---|---|---|
| 内网机起后端服务 | svn/git 同步拿入库 tar | `fetch_offline_images.ps1`(共享盘设 `PANDORA_ARTIFACT_ROOT`)→ 一键启动照常 |
| 拿 UE 打包产物 | `svn update` Packages | 制品目录 `client\<branch>\<flavor>\r<rev>\` 直接取(带校验和) |
| DS 镜像构建取 Linux 包 | 同级仓库 Packages 自动发现 | **已切换(2026-07-25)**:`build-image-minikube.ps1` 的 `Resolve-LinuxPkg` 优先级改为 ①`-SourcePkg` ②`PANDORA_DS_LINUX_PKG` ③制品库(两轨扫 `Server_Linux_Development` 最新发布) ④同级 `Packages\`(**已退役**,仅兼容旧机器,回退时打黄字提示去跑 `PublishPackages.ps1`);新增 `-ResolveOnly` 只打印来源不构建,便于验证。<br>2026-08-04:客户端把打包输出根从 SVN 工作副本内移到与客户端仓库【平级】(`F:\work\Packages`),④ 同时兼容新旧两种布局 |
| 正式发布 | 无 manifest | `make_release.ps1` → 按 `releases/<name>.json` 交付/回滚 |

## 6b. 版本化发布(语义版本 + 修复内容)

制品的目录名是**来源戳**(svn rev / git sha,溯源用),不是人可读的**发布版本号**。
正规发布另有一层语义版本:

- **版本号来源**:
  - 客户端 UE 包:`Pandora/Config/DefaultGame.ini` 的 `ProjectVersion`,cook 时烙进包内,运行时自报。发布前手动 bump。
  - 后端镜像:git tag,`git describe --tags` 自动注入 `pkg/version.Version`(机制已在,只需打 tag)。
- **修复内容来源**:仓库根 `CHANGELOG.md`(Keep a Changelog 格式),每次发布前在顶部新增版本段落。
- **绑定**:`make_release.ps1 -Version <版本>` 读 CHANGELOG 对应段 → 写进 `releases/<版本>.json`(机器可读)
  与 `releases/<版本>.md`(人可读),并记录每个制品的来源戳 / 镜像 digest。dirty 来源默认拒绝(`-AllowDirty` 放行内测)。

标准发布流:

```
① 定版本 v0.1.0:CHANGELOG.md 加 [0.1.0] 段 + 客户端 DefaultGame.ini ProjectVersion=0.1.0
② 后端:git tag v0.1.0 <clean commit> → publish_offline_images.ps1(镜像自报 v0.1.0)
③ 客户端:PackageSet.ps1(包自报 0.1.0)→ PublishPackages 发布
④ make_release.ps1 -Version v0.1.0 -ClientPackages <5 个包路径>
   → release manifest + notes(含修复内容)
```

版本号三处必须一致:`DefaultGame.ini ProjectVersion` = 后端 `git tag` = `make_release -Version` = `CHANGELOG` 段落。

## 7. 业界工具选型对照(现状用什么、标准工具在哪)

按环节列出业界主流开源工具,以及本项目**当前真实状态**(不是"以后可以换",很多已经落地,
见 `tools/devops/README.md` 详细操作,这里只做定位与状态标注):

| 环节 | 业界主流开源工具 | 本项目现状 |
|---|---|---|
| CI 构建触发 | **Jenkins**(~24k★,游戏行业事实标准,UE 项目配 `RunUAT BuildCookRun` 最常见)/ **Horde**(Epic 官方,UE5 源码自带,分布式编译+构建自动化+构建产物管理一体)/ GitHub Actions·GitLab CI(代码托管在对应平台时的默认选择) | ✅ **Jenkins 已用 `tools/devops` 一键 docker compose 起(JCasC 声明式初始化,两台机器配置一致)**;两仓 4 条 Jenkinsfile(dev+release)已就绪待挂 job;⚠️ 构建 agent(需装 UE 源码引擎+Go 1.26.5+svn+Docker 的宿主机)未接入,controller 本身 `numExecutors=0` 不亲自编译 |
| 制品库(存包) | **Harbor**(CNCF 毕业项目,~25k★,自建容器 Registry 首选,镜像扫描/复制/保留策略)/ **Sonatype Nexus Repository OSS**(通用制品库,二进制/zip/docker 都能存)/ **MinIO**(~50k★,自建 S3 对象存储,存 UE 大 zip 合适,生命周期规则自动清老版本) | ✅ **MinIO 已随一键栈起(桶 `pandora-artifacts` 已建,30 天保留策略)**,`releases/` 上传用 `mc cp`(手动,未自动化);✅ 本地目录 `PANDORA_ARTIFACT_ROOT`(§2)持续作为一手产出地;registry:2 已起作 Harbor 轻量替代(`localhost:5000`),DS 镜像构建脚本已接 `-PushRegistry`;📄 Harbor 安装脚本就绪但限 Linux 宿主机,未装;⚙️ Nexus 为 compose 可选 profile,默认关(与 registry/MinIO 职责重叠) |
| 发布/交付 | **GoReleaser**(~15k★,tag 一打自动交叉编译+打镜像+推 registry+生成 release,贴合纯 Go 后端)/ **ArgoCD**(~20k★,K8s GitOps 部署主流选择,git 里只存部署清单不存镜像) | ✅ **GoReleaser 已配好并实测通过**(`.goreleaser.yaml`,24 个二进制全量构建;`release:` 已 disable,不用 GitHub Releases,产物归宿仍是 `artifacts/` 与 MinIO);✅ **Argo CD 已装进 `pandora-agones` minikube**(v3.4.5,`deploy/k8s/argocd/`);`make_release.ps1` 手写 manifest(§6b)与 GoReleaser/ArgoCD 并行,尚未打通"tag → GoReleaser → ArgoCD 自动同步"的全自动链路 |

一个诚实的提醒:即便工具都已经装起来,**"制品不进版本库、版本可追溯、发布可回滚"这三点核心闭环**(§1-§6b)
才是大厂做法的本质,Jenkins/Harbor/ArgoCD 只是把这三点自动化、规模化的执行器;闭环脚本本身不依赖这些工具是否已装。

## 8. 剩余事项(诚实清单)

- SVN 服务端钩子需仓库管理员部署(本仓只提供脚本);git 托管平台规则需人配置。
- git 历史中的 177MB tar 仍在历史里(仅解除跟踪);要瘦身需 `git filter-repo` 重写历史并全员重新克隆,单独拍板。
- **Jenkins controller 已起,但真正编译的构建机 agent 未接入**(UE cook 不能在容器里跑,必须挂宿主机 inbound agent);
  4 条 Jenkinsfile 未挂 job(凭据需人工一次性配置,见 `tools/devops/README.md` §"接进现有流程")。
- Harbor 未装(脚本就绪,限 Linux 宿主机);"tag → GoReleaser → ArgoCD"全自动发布链路未打通,
  当前 `make_release.ps1` 手动 manifest 与三者并行而非串联。
- MinIO 制品上传(`mc cp`)未接入发布脚本自动化,仍是手动步骤。
- 制品根迁移到已起的 MinIO/registry 时:只改 `PANDORA_ARTIFACT_ROOT` 语义或本文档制品路径引用,
  §2-§6 的脚本布局与 build-info/sha256sums 契约不变。
