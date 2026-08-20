# [INC-20260820-001][P0] 策划一键启动假成功，登录入口在后端未就绪时返回 503

> **状态**：根因确认（永久修复与玩家 E2E 进行中，未关闭）  
> **类型**：`availability`  
> **环境**：Windows 本机进程（策划免 Docker 测试入口）  
> **首次发生时间（UTC）**：2026-08-20 10:25:02.984  
> **首次发现时间（UTC）**：2026-08-20 10:25:04.020  
> **负责人**：待指定  
> **受影响服务/版本**：`edge-envoy`、`login`、本地 Hub DS；Git `48fcadc7` 及其启动脚本工作副本  
> **最后更新**：2026-08-20

## 0. 一句话结论

策划一键入口把“启动编排脚本已经返回”错误地当成“玩家链路已经可玩”，并在成功和失败出口共用标准 `pause`；因此窗口显示 `Press any key to continue` 时，`login` 仍可能没有监听、Envoy 只能返回 503，Hub DS 也未被最终就绪门覆盖。永久修复必须让标准暂停提示只出现在 login、Envoy 上游和 exact Hub DS 均可用之后，并让重复一键启动只应用真实变化；当前尚未完成真实玩家 E2E，不能关闭。

## 1. 影响与范围

- 玩家影响：策划看到看似成功的标准暂停提示后立即点登录，连续收到 HTTP 503，无法登录，更无法进入 Hub。
- 影响人数/对局/请求数：本机策划验收环境，1 名已知测试者；附件中有 2 次顶层登录尝试，均失败，单次请求内部自动重试为 0。
- 服务影响：Envoy 入口仍监听，但当时没有可用的 `login` 上游；旧启动编排提前返回。稍后由另一轮启动编排拉起服务，不代表前一轮成功。
- 数据与安全影响：未发现错误写入或安全越界；请求没有形成有效 gRPC Login 响应，客户端未提交 session。
- 开始/结束时间：2026-08-20 10:25:02.984 UTC 起可证；`login` 约 10:27:50 UTC 才由后续独立编排开始监听。
- 是否仍可复发：修复和真实验收完成前可复发，尤其是重启电脑后的冷启动、Go 改动或某个服务启动失败场景。
- 严重级别判定理由：导致玩家无法登录/进场，命中 `docs/incidents/index.md` 的 P0 强制建档范围。

## 2. 第一现场与证据

### 2.1 症状

- 客户端症状：登录地图已加载；点击登录约 1 秒后报 `HTTP 503 without gRPC trailer`，仍留在登录页。
- 服务端症状：Envoy 8443 已存在，但 `login` 20001 当时未监听；Envoy 的 `login_cluster` 记录 503/connect-fail，稍后才恢复健康成员。
- 本地进程状态：06:27 的 21 个 Go 服务属于另一个父进程的新一轮编排，启动时间晚于两次客户端失败；不能反推 06:25 的启动成功。

### 2.2 原始证据

客户端原始附件受控位置：

```text
C:\Users\Administrator\.codex\attachments\7b50f9e7-2730-42e6-afc9-234ceaf5918b\pasted-text.txt
SHA256=4ED07D1A5CD164D2D82B3148C107CE5214B12D321EB71B3EB6BC1B397052C113
```

最小脱敏日志：

```text
2026.08.20-06.25.00-459  OnEnginePostLoadMap ... Lvl_Login
2026.08.20-06.25.02-984  后端请求发出: method=/pandora.login.v1.LoginService/Login auth=0 bytes=36
2026.08.20-06.25.04-020  后端请求失败: ... http=503 grpc=-1 elapsed_ms=1036
2026.08.20-06.25.04-020  Login failed: ... err=HTTP 503 without gRPC trailer
2026.08.20-06.25.04-020  登录失败后保持登录页: local_fallback=0 visible_error=1 role_select=0 session_commit=0
2026.08.20-06.25.48-229  第二次顶层登录请求发出
2026.08.20-06.25.49-247  再次 http=503 grpc=-1 elapsed_ms=1019，session_commit=0
```

本地现场只读取证：

```text
edge-envoy PID 28324 自本地时间 06:22:16 监听 TCP 8443
客户端两次请求均收到 Envoy HTTP 503
login 直到本地时间约 06:27:50 才监听 TCP 20001
Envoy login_cluster 累计 upstream_rq_503=6、upstream_cx_connect_fail=8；恢复后 membership_healthy=1
```

脚本结构证据（修复前基线）：

```text
策划一键启动-免Docker-测试版.cmd：无条件执行标准 pause，成功/失败不可区分
tools/scripts/start.ps1：完整 Invoke-Local 只检查 dev_all 退出码，没有最终 Hub DS 可玩门
tools/scripts/start.ps1：旧 Wait-LocalHubDsReady 在 DS 命令行缺 -port 时仍可能成功，UDP 监听也未绑定 exact DS PID
tools/scripts/run_services.ps1：策划 fast 路径对已有 PID 直接跳过，不判断对应 Go 输入是否变化
```

### 2.3 已排除的噪声

| 同时出现的日志 | 排除理由 |
|---|---|
| `aqProf.dll`、VTune、WinPix DLL 加载失败 | UE 可选分析工具未安装；登录地图和 HTTP 请求均已正常发出。 |
| Google `generate_204` 超时 | 外网连通性探测；本次登录请求已到达本机 Envoy，并收到明确 HTTP 503。 |
| EOS 日志 | 未参与 Pandora LoginService 本地调用。 |
| Zen/DDC/editor 模块加载 | 影响 Editor 启动耗时，不解释登录 RPC 的 Envoy 503。 |
| 账号、密码或数据库业务拒绝 | 请求没有得到有效 gRPC trailer，且 `grpc=-1/code=0/session_commit=0`；失败发生在入口到上游的传输层。 |

## 3. 时间线

客户端与 Windows 现场时间为 EDT（UTC-4），下表已统一换算为 UTC。

| UTC 时间 | 组件 | 事件 | 证据 |
|---|---|---|---|
| 10:22:16 | edge-envoy | 开始监听 TCP 8443 | 本机进程/端口现场 |
| 10:25:00.459 | UE 客户端 | 登录地图加载完成，可进行交互 | 附件 3017~3018 |
| 10:25:02.984 | UE 客户端 | 第一次发出 Login RPC | 附件 3036~3038 |
| 10:25:04.020 | Envoy/客户端 | 返回 HTTP 503，无 gRPC trailer；session 未提交 | 附件 3039~3043 |
| 10:25:48.229 | UE 客户端 | 人工发起第二次顶层登录 | 附件 3044~3046 |
| 10:25:49.247 | Envoy/客户端 | 第二次同型 HTTP 503 | 附件 3047~3051 |
| 10:27:00.586 ~ 10:27:50.349 | 后续独立编排 | 21 个 Go 服务顺序启动 | 本机 PID/父进程与创建时间 |
| ~10:27:50 | login | 开始监听 TCP 20001，Envoy 上游随后恢复健康 | 本机端口/Envoy admin 现场 |

## 4. 调用链与关键变量

```text
策划一键启动-免Docker-测试版.cmd
  → start.ps1 -GenTables -FastExistingProbe
    → Invoke-Local
      → dev_all.ps1
        → local_infra.ps1
        → dev_migrate.ps1
        → run_services.ps1
      ← 子脚本返回（旧逻辑没有最终 login/Envoy/Hub 可玩门）
  ← cmd 无条件 pause，失败也显示标准 Press any key

UE Login
  → https://本机:8443/pandora.login.v1.LoginService/Login
    → edge-envoy
      → login_cluster 无可用上游
        → HTTP 503 without gRPC trailer
```

| 变量/对象 | 创建位置 | 所有者与生命周期 | 是否共享/可变 | 事故中的作用 |
|---|---|---|---|---|
| 子脚本退出码 | CMD / `start.ps1` | 单次启动编排 | 可变 | 只证明某层脚本返回，旧逻辑没有把它与最终可玩状态绑定。 |
| `login` listener | `run_services.ps1` | login 进程生命周期 | 是 | 06:25 不存在，Envoy 无法代理 Login RPC。 |
| Envoy `login_cluster` | edge-envoy | Envoy 进程生命周期 | 是 | 入口活着但上游不可用时按设计返回 503。 |
| Hub DS UDP 7777 | UE Hub DS | DS 进程生命周期 | 是 | 旧完整启动成功出口未等待 exact PID 持有该端口。 |
| Go 构建收据 | 策划 fast helper | 跨启动持久化 | 是 | 旧实现为全仓单一指纹，且已有进程在比较前直接跳过，无法安全选择性重启。 |

## 5. 根因

### 5.1 直接根因

直接根因由两个缺陷共同构成：

1. **成功提示没有成功语义**：CMD 在正常和异常出口都执行未抑制的标准 `pause`，因此 `Press any key to continue` 仅表示批处理走到了暂停语句，不能证明后端可玩。
2. **启动成功条件过浅**：完整本机启动没有在返回前验证 exact `login` listener、Envoy 到 login 的健康上游、以及 exact Hub DS UDP listener；当服务编排提前返回或失败时，用户仍能看到同一提示并立即撞上 503。

客户端日志、端口时间线和脚本控制流三者闭环，能够解释全部已知症状。

### 5.2 触发条件

- 重启电脑后的冷启动，基础设施、Go 服务和 Hub DS 均需重新拉起；或
- 某个 Go 服务构建/启动失败，父脚本非零返回；或
- 登录服务尚未监听、Envoy 尚未观察到健康上游、Hub DS 尚未完成加载时，策划根据旧暂停提示立即验收。

### 5.3 故障放大因素

- 旧 fast 路径对“已有 PID”直接跳过，正在运行的服务即使源码已变也不更新。
- 全部 Go 服务共享一个全仓指纹，无法只重启真实受影响服务；这诱使使用者在“全停/全启”和“完全不应用变化”之间二选一。
- 策划表变化只在部分启动分支消费，完整一键启动不能可靠重启全部真实表读取者。
- Kafka/JVM、Go 服务与 Hub DS 的冷启动物理成本让“脚本返回”和“可玩”之间的窗口更明显。

### 5.4 为什么现有保护没有挡住

| 保护 | 为什么不足 |
|---|---|
| 单服务 TCP readiness | 只覆盖 run_services 内部分服务；不覆盖完整玩家链的 Envoy 上游与 Hub DS。 |
| `Wait-LocalHubDsReady` | 旧完整启动不调用；函数本身也没有把 UDP listener 严格绑定到 exact DS PID。 |
| Envoy 监听 8443 | 入口监听不等于上游健康；本事故正是入口活着但 login 上游不可用。 |
| 客户端可见错误 | 能避免假 session，但不能让策划完成验收；仍是 P0 可用性失败。 |
| 再点一次启动 | 旧逻辑不比较已有服务的逐服务输入，既可能不应用改动，也可能全量重建，不能作为可靠恢复协议。 |

## 6. 全仓同类问题扫描

- 扫描基线 commit：`48fcadc7`
- 扫描目录和文件类型：根目录策划 CMD、`tools/scripts/**/*.ps1`、22 个 `run_services.ps1` runtime targets、所有 `configtable.NewStore` 消费点。
- 搜索模式/工具：PowerShell AST、`rg "pause|Wait-LocalHubDsReady|FastExistingProbe|ConfigTableChanged|configtable.NewStore"`、纯内存进程/listener 契约桩。
- Confirmed 同型命中：标准 pause 假成功；完整 local 缺最终 Hub 门；existing 进程绕过 Go 变化判断；全仓共享 Go 指纹；表变化读取者清单遗漏 `inventory/dialogue/mission`。
- 结构性隐患：`matchmaker` 与 `matchmaker_pve` 共用一个构建目标但有两个运行实例，选择性更新必须只构建一次并重启两个实例。
- 已排除项及理由：普通 Docker、直接单服务 restart、非策划入口不应自动获得策划 fast 语义；本次保持原行为。
- 未覆盖边界：真实断电重启、杀软冷扫 PE、UE Editor 首次加载的墙钟耗时仍需目标机器 E2E。

## 7. 处置与永久修复

### 7.1 临时止血

| 动作 | 状态 | 证据 | 风险/回滚 |
|---|---|---|---|
| 登录前人工检查 login 20001、Envoy upstream 与 Hub 7777 | 可用但不可接受为最终方案 | 现场恢复后各端口/上游均健康 | 人工检查易遗漏，策划入口不能依赖此步骤。 |

### 7.2 永久修复

| 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|
| 标准 `Press any key` 只用于真正成功；失败显示不同提示并保留非零退出码 | 实施中 | 根策划 CMD | 针对性 CMD 契约待绿 |
| 完整策划启动返回前验证 exact login、Envoy login upstream 与 exact Hub DS | 实施中 | `start.ps1` / `dev_all.ps1` | 纯虚拟门禁 + 真实热启动/玩家 E2E 待完成 |
| 按真实 Go 依赖闭包生成逐构建目标强指纹，只更新受影响实例 | 实施中 | planner fast helper / `run_services.ps1` | 逐服务变化与构建失败不下线旧进程契约待绿 |
| 重启电脑后复用安装和有效构建收据，只启动缺失进程 | 实施中 | planner fast helper / `run_services.ps1` | missing-process + receipt-hit 契约待绿 |
| 策划表变化重启全部实际表读取者；与 Go 变化去重 | 实施中 | `start.ps1` → `dev_all.ps1` → `run_services.ps1` | 消费者扫描与组合变化契约待绿 |
| 成功提示前完成真实客户端登录与 Hub 进场验收 | 未执行 | 本机环境 | 本事故关闭硬门 |

### 7.3 防复发规则

- 策划入口的标准成功提示是可玩性契约，不得仅绑定脚本返回或单个进程存活。
- ready 必须验证 exact 进程所有权，不接受“任意 PID 占端口”。
- 选择性热更新必须先完成全部新构建和输入二次稳定校验，再停止旧实例；构建失败时旧服务继续可用。
- 表读取者清单必须由代码使用点机械校验，避免新增消费者后漏重启。

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| CMD 成功/失败提示契约 | 失败也出现标准 `Press any key` | 待验证 | `planner_playable_exit_contract_test.ps1` | 红测已设计，绿测待完成 |
| 完整启动最终可玩门 | `dev_all` 返回后不等 Hub；弱 PID 归属 | 待验证 | 同上 | 待完成 |
| 逐服务 Go 变化 | existing 直接 skip；任一 Go 输入影响全部服务 | 待验证 | `run_services_planner_fast_start_contract_test.ps1` | 红测已复现，绿测待完成 |
| 表变化读取者 | 完整启动不消费；清单漏 3 个服务 | 待验证 | 同上 + 全仓 `rg` | 待完成 |
| 重启电脑（进程全缺失、收据命中） | 未验 | 待验证 | 纯虚拟契约 + 实机重启后手测 | 实机重启待用户执行 |
| AST/既有脚本回归 | — | 待验证 | PowerShell AST + `tools/scripts/tests/*contract_test.ps1` | 待完成 |
| `go test -race` | 不适用 | 不适用 | 本次只改 PowerShell/CMD/文档，不改 Go 并发代码 | — |
| fatal/OOM/SIGKILL 重启注入 | 未执行 | 未执行 | 进程级故障注入 | 关闭前保留 |
| 玩家 E2E | 两次登录均 HTTP 503，无法进场 | 未执行 | 新一键入口 → Login → SelectRole → Hub exact ACK | 关闭硬门 |

## 9. 部署、回滚与观察

- 修复 commit：未提交；用户未授权 commit/push。
- 构建产物/镜像 digest：不适用；本轮只改本地策划启动脚本，且发布任务未重生成 EXE/镜像。
- 部署时间与目标环境：尚未部署；目标为 Windows 本机策划免 Docker 入口。
- 回滚条件和步骤：若新选择性编排不能证明 exact readiness 或破坏普通入口，应停止使用策划 fast flag并回滚本轮明确脚本 hunk；不得删除用户的 Python 工作副本。
- 观察窗口、指标与结果：未开始。至少记录表导出、基础设施、迁移、Go plan/build/launch、login/Envoy/Hub readiness 分段耗时，并执行玩家登录进 Hub。

## 10. 剩余风险与行动项

| ID | 严重级别 | 行动项 | 负责人 | 状态 | 目标/关联 Incident |
|---|---|---|---|---|---|
| A-1 | P0 | 完成成功专用提示与最终可玩门，跑修复前红/修复后绿契约 | 待指定 | 进行中 | 本事故 |
| A-2 | P0 | 完成逐服务 Go/表变化选择性重启；构建失败不停止旧实例 | 待指定 | 进行中 | 本事故 |
| A-3 | P0 | 新脚本真实执行 Login → SelectRole → Hub exact ACK | 用户/待指定 | 未开始 | 本事故关闭硬门 |
| A-4 | P1 | 重启电脑后执行一次冷进程/热收据验收，记录墙钟分段 | 用户/待指定 | 未开始 | 本事故 |
| A-5 | P1 | 注入 login 未监听、错误 PID 占端口、Hub DS 退出，确认标准成功提示永不出现 | 待指定 | 未开始 | 本事故 |

## 11. 关闭审核

- [x] 直接根因和放大因素均有证据
- [ ] 修复前失败、修复后通过的回归存在
- [ ] race/集成/故障注入达到本事故风险要求（race 对本次脚本改动不适用）
- [x] 同类代码扫描完成
- [ ] 目标环境已加载可追溯的新脚本
- [ ] 玩家路径、恢复和补偿路径验证通过
- [ ] 观察窗口无复发
- [ ] 剩余风险已解决或另建 Incident/任务
- [x] 文档已脱敏且时间线时区明确

**关闭结论与审批人**：未关闭；成功提示、选择性重启和真实玩家 E2E 均未完成。
