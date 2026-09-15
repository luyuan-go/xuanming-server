# Pandora

> 一款 MOBA 类型游戏的后端工程。
> 客户端与 DS 工程在独立仓库(UE 5.8),本仓库只负责 go 后端 + proto + 部署 + 设计文档。

## Python 后端独立仓库

Python 实现已迁至 [`lhl-go/XuanMing-Server`](https://github.com/lhl-go/XuanMing-Server)，
保留原 `python/` 目录的相关提交历史。本仓库继续维护共享协议、配置表、服务配置和部署脚本。

需要运行 Python 启动脚本、生成器或跨语言测试时，在本仓库根目录执行：

```powershell
git clone https://github.com/lhl-go/XuanMing-Server.git python
```

`python/` 是独立 Git 工作区，已由本仓库忽略；Python 改动请在该目录中提交。
安装环境与运行要求见 [Python 仓库说明](https://github.com/lhl-go/XuanMing-Server#readme)。

## 项目特点

- **5v5 MOBA 战斗**:固定 25 分钟一局,UE 战斗 DS 一局一进程
- **持续在线大厅**:UE 大厅 DS 常驻,500 人/实例,**全图自由 PvP**(玩家在大厅也能放技能、互打、对话 NPC、组队、交易)
- **基础设施**:MySQL 8 + Redis 7 + Kafka + etcd
- **DS 编排**:Agones on k8s

## 仓库结构

```
Pandora/
├── pkg/                   # Go 公共框架(log/metrics/grpc/kafka/redis lock 等)
├── proto/                 # 协议定义与生成产物
├── services/              # 14 个 go 服务(按 account/runtime/matchmaking/battle/social 等域分组)
├── deploy/                # docker-compose / k8s / Agones yaml
├── tools/scripts/         # 开发与压测脚本
├── docs/design/           # 架构与设计文档(必读)
├── docs/incidents/        # 崩溃与 P0 事故索引、模板和复盘档案
└── robot/                 # 压测客户端
```

## 必读文档

新人来到本项目,**第一周阅读顺序**:

1. [`CLAUDE.md`](./CLAUDE.md) — 项目宪法(规范、压测纪律、不变量)
2. [`docs/design/pandora-arch.md`](./docs/design/pandora-arch.md) — 总架构图与玩家流转
3. [`docs/design/go-services.md`](./docs/design/go-services.md) — 14 个 go 服务的职责边界
4. [`docs/design/ds-arch.md`](./docs/design/ds-arch.md) — UE DS(Hub / Battle)架构
5. [`docs/design/infra.md`](./docs/design/infra.md) — MySQL / Redis / Kafka / etcd 命名规范
6. [`docs/design/proto-design.md`](./docs/design/proto-design.md) — 协议设计
7. [`docs/design/stress-discipline.md`](./docs/design/stress-discipline.md) — 压测纪律
8. [`docs/design/pvp-rules.md`](./docs/design/pvp-rules.md) — PvP 规则待定项
9. [`docs/incidents/index.md`](./docs/incidents/index.md) — 崩溃/P0 建档、状态和关闭门
10. [`AGENTS.md`](./AGENTS.md) — AI 协作守则
11. [`PROGRESS.md`](./PROGRESS.md) — 当前进度

## 快速启动

### 0. 一键启动(推荐:策划/新人本地联调)

一条命令把后端跑起来,会先检查必要工具(go / docker / kubectl / minikube)。默认只提示缺失项,不改本机环境;确实要让脚本尝试用 winget 安装时,显式追加 `-Install`:

```powershell
# 默认 local 模式(基础设施 docker + 20 个业务进程,含独立 matchmaker-pve 实例,可断点调试)
pwsh tools/scripts/start.ps1

# 也可双击仓库根的 start.cmd(无参数 = local 模式)
```

UE 本机联调可以直接用同版本发行版 Editor 当客户端:先启动 `local`(宿主 exec Windows DS)
或 `k8s`(Agones 真 Linux DS),再在 Editor 里 Play/New Editor Window/Standalone 登录即可进
 Hub DS。不是必须启动已打包 Windows client;打包 client 主要用于更接近发行环境的最终回归。
当前本机约定:`F:\work\Pandora-Client-SVN\Pandora` 用源码引擎出 WindowsServer DS 包,
`C:\work\Pandora-Client-SVN\Pandora` 用发行版 Editor/客户端登录、匹配、进战斗。

> 【已废弃 2026-07-14】含战斗混合模式(`play.ps1 -Battle` / `start.ps1 -Mode battle`,宿主
> exec Windows DS)已退役:Windows DS 只保留给 `local` 断点调试,其他要真 DS 一律 `k8s`。
> 清理旧机器遗留环境用 `play.ps1 -Battle -Stop`。见 `docs/design/decision-revisit-retire-battle-mode.md`。

五种启动方式(`-Mode`):

| 模式      | 说明                                                          | DS   | 命令 |
|-----------|---------------------------------------------------------------|------|------|
| `local`   | 基础设施在 docker,go 服务宿主进程(可断点调试,**策划首选**)  | local | `start.ps1 -Mode local` |
| `docker`  | 基础设施 + 20 个业务容器(19 个 Go 服务,matchmaker 双实例)       | mock | `start.ps1 -Mode docker` |
| `intranet`| 同 docker 全容器,绑内网 IP 供多人联调                         | mock | `start.ps1 -Mode intranet` |
| `k8s`     | 本机 minikube + Agones,真 Linux DS(线上等价)                | agones | `start.ps1 -Mode k8s` |
| `online`  | 部署到远端 k8s(需人工授权并确认 kube-context,谨慎)         | agones | 见下方真 DS 参数 |

> **真 DS 闭环(无 mock)**:`docker`/`intranet` 因容器内无真 DS 只能 mock;要真 DS 用 `k8s`
> (本机 Agones)或 `local`(宿主直接 exec Windows DS)。`k8s` 模式起完后再跑
> `pwsh tools/scripts/e2e_k8s.ps1`(load DS 镜像 + 起宿主 Envoy 桥接 + 等 Fleet + UDP 中继),
> 详见 `deploy/k8s/agones/README.md`。
> `local` 模式依赖 cook 好的 WindowsServer staged 包。该 staged 包由客户端/UE 侧的打包
> 流水线产出(后端仓库不再维护 DS 编译脚本);拿到后让 allocator 配置指向
> `F:\work\Packages\Server_Win64_Development\WindowsServer\PandoraServer.exe`
> (打包输出目录在客户端仓库【同级】,不在 `Pandora-Client-SVN\` 里);
> 不能使用 `Pandora\Binaries\Win64` 下的裸 server 二进制,否则 DS 加载资产会崩。
> 本地 dev 的 DS 面 Envoy `:8444` 是明文 grpc-web,local DS env 里 `PANDORA_DS_ALLOCATOR_TLS`
> 应保持 `0`。
>
> **线上真集群**:Fleet 的 DS 镜像与回调地址必须按环境注入,否则远端拉不到镜像/回调打空,
> 故 `-Mode online` 强制要求 DS 三参数，并把 `test/prod` 各自绑定到预期 kube-context
> （参数或 `PANDORA_K8S_TEST_CONTEXT` / `PANDORA_K8S_PROD_CONTEXT`）；缺失/不匹配即 fail-fast:
>
> ```powershell
> start.ps1 -Mode online -Env test -TestKubeContext pandora-test `
>   -Registry registry.mycorp.com -Tag v1.2.3 `
>   -BattleDsImage registry.mycorp.com/pandora/battle-ds:v1.2.3 `
>   -HubDsImage    registry.mycorp.com/pandora/hub-ds:v1.2.3 `
>   -DsGatewayAddr pandora-envoy.pandora.svc:8444
> ```

常用:

```powershell
pwsh tools/scripts/start.ps1 -Check            # 只检查工具不启动
pwsh tools/scripts/start.ps1 -Install          # 缺工具时尝试 winget 安装
pwsh tools/scripts/start.ps1 -Status           # 看状态
pwsh tools/scripts/start.ps1 -Mode docker -Down # 停
pwsh tools/scripts/start.ps1 -Mode k8s -Resume  # 电脑重启后快速恢复(不重建镜像)
pwsh tools/scripts/start.ps1 -Mode k8s -Reset   # 状态损坏时一键重置(minikube delete 后全新部署)
```

> 关键产物:`deploy/services/Dockerfile`(16 服务共用)、`deploy/docker-compose.services.yml`、
> `deploy/k8s/`(infra + services + online overlay)、`tools/scripts/gen_cluster_config.ps1`
> (把 `127.0.0.1` 的 dev 配置转成容器服务名的集群版配置)。
>
> 下面 1~5 步是「手动分步」做法,想了解细节或单独调试时用。

### 1. 装开发工具链(首次)

一键装齐 buf / mkcert / grpcurl(已装的会自动跳过):

```powershell
pwsh tools/scripts/install_dev_tools.ps1
```

只检查不装:
```powershell
pwsh tools/scripts/install_dev_tools.ps1 -Check
```

强制重装:
```powershell
pwsh tools/scripts/install_dev_tools.ps1 -Force
```

工具版本锁定见脚本头部,**所有开发者用同一版本**避免环境漂移。

要自己装的(脚本不管):**Go 1.24+ / Docker Desktop / Git**。

### 2. 启动基础设施

```powershell
pwsh tools/scripts/dev_up.ps1
```

启动 MySQL 3307 / Redis 6380 / Kafka 9093 / etcd 2380 / Prometheus 9091 / Grafana 3001 / Envoy 8443(W2 起)。

### 3. 生成 proto 代码

```powershell
pwsh tools/scripts/proto_gen.ps1
```

第一次跑会从 buf.build 拉远程插件(`protocolbuffers/go` / `grpc/go`),需要外网。
Kratos HTTP 代码生成走本地 `protoc-gen-go-http`,需要先装 Go 并执行:

```powershell
go install github.com/go-kratos/kratos/cmd/protoc-gen-go-http/v2@latest
```

### 4. 编译 + 启动服务

```powershell
# 编译当前已启用 module(完整口径见 CLAUDE.md §4.1 / go.work)
go build ./pkg/... ./proto/... ./services/account/login/... ./services/account/player/... ./services/runtime/push/... ./services/runtime/player_locator/... ./services/matchmaking/team/... ./services/matchmaking/matchmaker/... ./services/battle/ds_allocator/... ./services/battle/hub_allocator/... ./services/battle/battle_result/...

# 启动 login(W2)
go run ./services/account/login/cmd/login -conf services/account/login/etc/login-dev.yaml
```

### 5. 端到端验证

```powershell
# 直连 login(绕过 Envoy)
grpcurl -plaintext -d '{"account":"test","password_hash":"abc","device_id":"d1"}' `
  127.0.0.1:20001 pandora.login.v1.LoginService/Login

# 经 Envoy(模拟客户端 gRPC-Web 路径)
grpcurl -insecure -d '{"account":"test","password_hash":"abc","device_id":"d1"}' `
  localhost:8443 pandora.login.v1.LoginService/Login
```

## License

MIT,见 [LICENSE](./LICENSE)。
