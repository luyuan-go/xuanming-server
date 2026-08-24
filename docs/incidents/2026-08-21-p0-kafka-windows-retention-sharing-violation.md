# [INC-20260821-001][P0] Windows 本机 Kafka retention 重命名索引遭 sharing violation 后退出

> **状态**：调查中（未关闭）
> **类型**：`crash / availability`
> **环境**：Windows 本机进程（策划免 Docker 测试链）
> **首次发生时间（UTC）**：2026-08-21 07:47:33.841
> **首次发现时间（UTC）**：未知；本任务于 2026-08-21 08:06:52 完成只读取证
> **负责人**：待指定
> **受影响服务/版本**：Apache Kafka 3.9.1 单节点 KRaft；Git `90e5d3eb` 对应工作副本
> **最后更新**：2026-08-21

## 0. 一句话结论

本机 Kafka 的 48 小时 retention 在删除 `pandora.player.presence-2` 旧 segment 时，无法把
`00000000000000000000.timeindex` 重命名为 `.timeindex.deleted`，Windows 返回“文件正被其他进程使用”；
Kafka 随即把唯一 `log.dirs` 判为失败并主动关闭 broker。**占用文件的具体进程尚未找到**，未重启、
未删数据、未完成恢复与玩家 E2E，因此状态保持调查中、未关闭。

## 1. 影响与范围

- 玩家影响：尚未执行真实 Login → Hub / Battle 玩家 E2E，实际玩家影响人数未知；Kafka 已不可用，依赖
  Kafka 的事件生产、消费、推送与补偿链不能视为可用。
- 影响人数/对局/请求数：未知；本次为单机策划测试环境，没有证据表明生产或 K8s 环境受影响。
- 服务影响：本机 Kafka broker/controller 均退出。2026-08-21 08:06:52 UTC 只读检查确认
  `127.0.0.1:9093` / `:9094` 均无 listener，Kafka JVM 数为 0。机器上另有一个与 Kafka 无关的
  Jenkins agent JVM，已按进程命令身份排除。
- 数据与安全影响：没有人工删除、reset 或重新格式化 Kafka 数据。现场存在部分完成的 retention rename：
  同一 segment 的 `.log.deleted`、`.index.deleted` 已存在，`.timeindex` 仍保留；是否存在消息或索引损坏
  尚未通过离线校验和受控重启证明，不能声称“数据未丢”。未发现安全边界突破证据。
- 开始/结束时间：2026-08-21 07:47:33.841 UTC 开始发生；截至 08:06:52 UTC 尚未恢复。
- 是否仍可复发：是。文件占用来源未知，未实施永久修复或故障回归。
- 严重级别判定理由：关键本机基础设施发生非预期进程退出，且可使策划玩家链不可用，命中
  [`docs/incidents/index.md`](index.md) 的强制 P0 建档范围。

本次事故文档动作**没有**启停 MySQL、Redis、Envoy、22 个业务进程或任何 DS；没有运行一键入口，
也没有触碰 K8s。

## 2. 第一现场与证据

### 2.1 症状

- 客户端症状：未执行玩家 E2E，未知。
- 服务端症状：Kafka retention scheduler 报 `KafkaStorageException`，唯一日志目录被标记失败，broker
  输出 `Shutdown broker because all log dirs ... have failed` 后退出。
- 本机进程状态：`9093` / `9094` 无 listener；无 Kafka JVM。MySQL、Redis、Envoy 和 22 个业务进程
  没有因本次文档动作被启停；业务进程存在不等于 Kafka 依赖健康。
- K8s/Agones 状态：不适用；本事故只发生在 Windows 免 Docker 本机链，未检查或修改 K8s。

### 2.2 原始证据

第一现场日志（运行态文件，不纳入 Git）：

```text
F:\work\XuanMing-Server\run\localinfra\logs\kafka.log
SHA256=97BED21B371F7428128E01AA562A9FAB38D2F8AFCC1F1672640CB3586E9EB2B1
取证哈希时间=2026-08-21 08:06:52 UTC
```

Kafka 日志时间使用本机 EDT（UTC-4）。以下为最小脱敏摘录，未包含凭证：

```text
[2026-08-21 03:47:33,843] INFO ... Deleting segment ... due to log retention time 172800000ms breach
[2026-08-21 03:47:33,848] WARN Failed atomic move of ...00000000000000000000.timeindex
  to ...00000000000000000000.timeindex.deleted retrying with a non-atomic move
java.nio.file.FileSystemException: ...timeindex -> ...timeindex.deleted:
  The process cannot access the file because it is being used by another process
[2026-08-21 03:47:33,853] ERROR Error while deleting segments for pandora.player.presence-2 ...
[2026-08-21 03:47:33,854] ERROR Uncaught exception in scheduled task 'kafka-log-retention'
[2026-08-21 03:47:33,854] WARN ... Stopping serving replicas ... because the log directory has failed
[2026-08-21 03:47:33,890] ERROR Shutdown broker because all log dirs ... have failed
```

生成的本机配置与磁盘现场：

```text
run/localinfra/cfg/kafka.properties:
  listeners=PLAINTEXT://127.0.0.1:9093,CONTROLLER://127.0.0.1:9094
  log.dirs=F:/work/XuanMing-Server/run/localinfra/data/kafka
  log.retention.hours=48

run/localinfra/data/kafka/pandora.player.presence-2:
  00000000000000000000.log.deleted
  00000000000000000000.index.deleted
  00000000000000000000.timeindex
  00000000000000000003.log
```

2026-08-21 08:06:52 UTC 的只读状态检查：

```text
9093/9094 listener count = 0
Kafka JVM count = 0
```

事后没有找到仍持有该 `.timeindex` 的进程。这个阴性结果只说明**取证时**没有定位到 holder；此前的
holder 可能已经释放句柄或退出，不能据此否定 Kafka 日志记录，也不能据此判定占用来源。

### 2.3 已排除的噪声

| 同时出现的事项 | 排除理由 |
|---|---|
| Python diff / 对拍任务已人工停止 | 它此前占用的是 `ds_allocator` 的 `20020/21020`；没有证据表明它打开过 Kafka data 文件。本事故有独立的 Kafka retention、Windows 文件共享错误与 broker shutdown 证据链。 |
| 机器上仍有一个 Java 进程 | 进程身份是 Jenkins agent，不包含 Kafka 入口或本工作区 Kafka 配置，且不监听 `9093/9094`；Kafka JVM 数仍为 0。 |
| MySQL、Redis、Envoy 与 22 个业务进程仍存在 | 这些进程没有被本次文档动作启停，但其存在不能证明 Kafka producer/consumer 或玩家路径健康。 |
| K8s / 生产 Kafka | 本次日志目录、端口和启动方式均指向 Windows 本机 `run/localinfra`；没有生产或 K8s 受影响证据。 |

## 3. 时间线

Kafka 日志为 EDT（UTC-4），下表已换算为 UTC。

| UTC 时间 | 组件 | 事件 | 证据 |
|---|---|---|---|
| 07:42:00.516 | Kafka broker | 本轮从 `SHUTDOWN` 转为 `STARTING` | `kafka.log:72` |
| 07:42:04.984 | `pandora.player.presence-2` | partition 加载完成，初始 high watermark 为 3 | `kafka.log:1382` |
| 07:47:33.841 | retention | 为 partition 2 滚出 offset 3 新 segment | `kafka.log:2694` |
| 07:47:33.843 | retention | 判定 offset 0 segment 超过 48 小时保留期并开始删除 | `kafka.log:2696` |
| 07:47:33.848 | Windows filesystem | atomic rename `.timeindex` 失败，fallback 非原子 rename 也遭 sharing violation | `kafka.log:2697-2698` 与后续异常 |
| 07:47:33.853 | Kafka `LogDirFailureChannel` | 删除 segment 失败，记录 `KafkaStorageException` | `kafka.log:2730-2773` |
| 07:47:33.854 | Kafka `ReplicaManager` | 唯一日志目录被判失败，停止服务 replicas | `kafka.log:2813` |
| 07:47:33.890 | Kafka `LogManager` | 因所有 log dirs 均失败而关闭 broker | `kafka.log:2818` |
| 08:06:52 | 只读取证 | `9093/9094` 无 listener、Kafka JVM 数 0；未执行恢复 | 本机 `netstat` 与进程身份检查 |

## 4. 调用链与关键变量

```text
KafkaScheduler: kafka-log-retention
  → LogManager.cleanupLogs
    → UnifiedLog.deleteOldSegments / deleteSegments
      → LocalLog 删除 segment 文件
        → Utils.atomicMoveWithFallback
          → Windows Files.move(.timeindex → .timeindex.deleted)
            → FileSystemException: file is being used by another process
      → LogDirFailureChannel 标记唯一 log.dirs 失败
        → ReplicaManager 停止该目录全部 replicas
          → LogManager: all log dirs failed
            → broker shutdown
```

| 变量/对象 | 创建位置 | 所有者与生命周期 | 是否共享/可变 | 事故中的作用 |
|---|---|---|---|---|
| `log.retention.hours=48` | `New-KafkaProperties` | 每次生成本机 Kafka 配置 | 配置可变 | 触发 offset 0 segment 的过期删除。 |
| `log.dirs` | `run/localinfra/cfg/kafka.properties` | 本工作区 Kafka 数据生命周期 | 单目录、可变 | 唯一目录失败后没有第二目录可继续服务。 |
| `000...000.timeindex` | Kafka segment | partition segment 生命周期 | Kafka 与未知 Windows holder 共享 | rename 被 sharing violation 拒绝，是直接失败点；holder 身份未知。 |
| retention scheduler | Kafka JVM | broker 生命周期 | 后台周期任务 | 把文件 rename 失败上报为 storage failure。 |
| `9093/9094` listeners | Kafka broker/controller | Kafka JVM 生命周期 | 运行态 | broker 退出后均消失，证明当前不可用。 |

## 5. 根因

### 5.1 直接根因

已证实的直接故障链是：Kafka retention 删除过期 segment 时，Windows 拒绝将该 segment 的
`.timeindex` 重命名为 `.timeindex.deleted`，atomic 与 fallback move 均失败；Kafka 3.9.1 将该
storage directory 标记为 failed。配置只有一个 `log.dirs`，所以 broker 按自身保护逻辑关闭。

**尚未闭合的根因**是“哪个进程、以何种共享模式、为什么在 07:47:33 UTC 持有 `.timeindex`”。
事后没有找到 holder，不能猜测为杀毒软件、索引服务、备份软件、Python diff 或 Kafka 自身句柄泄漏。

### 5.2 触发条件

- `pandora.player.presence-2` offset 0 segment 达到 `log.retention.hours=48`；
- Kafka 开始 segment roll 与异步删除；
- `.timeindex` 在 rename 窗口被某个尚未识别的进程以不允许删除/重命名的共享方式持有；
- 单节点本机 Kafka 只有一个 `log.dirs`。

### 5.3 故障放大因素

- 唯一日志目录一旦被 Kafka 判为 failed，broker 没有第二目录继续服务。
- `local_infra.ps1` 只在启动阶段拉起并探活 Kafka；本次静态扫描没有发现 broker 存活期的 supervisor
  或自动重启闭环，退出后维持无 listener 状态。
- 22 个业务进程可继续存在，容易把“进程还在”误当成“Kafka 依赖健康”；玩家 E2E 未执行。
- retention 已部分重命名同一 segment 文件，任何未经校验的删除/reset 都可能放大数据风险。

### 5.4 为什么现有保护没有挡住

| 保护 | 为什么不足 |
|---|---|
| 启动期 `9093/9094` exact ownership/ready 检查 | 只证明启动当刻就绪，不能发现 5 分钟后 retention 后台任务导致的退出。 |
| Kafka `atomicMoveWithFallback` | atomic move 和 fallback move 都受同一个 Windows sharing violation 阻断。 |
| Kafka log-dir failure 隔离 | 只有一个 log dir，隔离该目录等于关闭全部 broker 存储。 |
| 业务进程自己的重连/重试 | 尚未做实际恢复验证；Kafka 进程本身已经不存在，客户端重试不能重建 broker。 |
| 一键启动/停止编排 | 本任务按边界没有重跑入口；未发现常驻 supervisor 自动处理运行期 Kafka 退出。 |

## 6. 全仓同类问题扫描

- 扫描基线 commit：`90e5d3eb`（工作树另有共享 WIP，本次未覆盖或修改）。
- 扫描目录和文件类型：`tools/scripts/**/*.ps1`、`docs/design/**/*.md`、`docs/ops/**/*.md`、本机生成的
  `run/localinfra/cfg/kafka.properties` 与 Kafka 日志/partition 现场。
- 搜索模式/工具：`rg "log.retention|log.dirs|kafka-log-retention|9093|9094|timeindex|sharing violation|all log dirs"`，
  PowerShell `Select-String`、`netstat` 与只读进程身份检查。
- Confirmed 同型命中：免 Docker Kafka 使用单一 `log.dirs`、48 小时 retention，启动期有 exact readiness，
  但本次扫描范围内没有运行期 Kafka supervisor/自动恢复闭环。
- 结构性隐患：同类 Windows 文件占用若命中任意 partition retention，可再次把唯一 log dir 判失败；
  holder 取证只能事后做时容易丢失第一现场。
- 已排除项及理由：Docker/K8s Kafka 配置与本地 `run/localinfra` 数据目录不同；Python diff 端口冲突是独立问题。
- 未覆盖边界：没有安装或运行额外 handle tracing 工具；没有扫描杀软/索引/备份软件配置；没有受控重启、
  Kafka storage/checkpoint 校验、topic/consumer offset 对账、故障注入或玩家 E2E。

## 7. 处置与永久修复

### 7.1 临时止血

| 动作 | 状态 | 证据 | 风险/回滚 |
|---|---|---|---|
| 保留 Kafka data、cfg、logs 第一现场，不做 delete/reset | 已执行 | 现场文件与日志仍在 | 不恢复服务，但避免在根因不明时扩大数据风险。 |
| 不重启 Kafka，不运行一键入口 | 已执行 | `9093/9094` 无 listener、Kafka JVM 0 | 服务仍不可用；这是取证边界，不是恢复方案。 |
| 停止 Python diff 任务 | 已由上游任务执行，与本事故独立 | 其端口冲突证据为 `20020/21020` | 不能作为 Kafka 修复或 holder 定谳。 |

### 7.2 永久修复

| 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|
| 在复发窗口捕获 `.timeindex` 的 exact handle owner、访问模式与时间线 | 未开始 | Windows handle/ETW 取证方案待定 | 必须能给出 PID、映像和句柄共享模式；日志脱敏。 |
| 定谳 holder 后消除不兼容文件共享方式 | 未开始 | 未定；禁止先猜杀软排除或直接删目录 | 针对真实 holder 做修复前失败/修复后通过回归。 |
| 为本机 Kafka 增加运行期退出检测与明确不可玩状态 | 未开始 | `local_infra.ps1` / 策划可玩门待设计 | broker 退出后必须快速报错，不得继续显示可玩。 |
| 设计有数据校验与有界次数的安全恢复流程 | 未开始 | 运维脚本/手册待设计 | 保留并校验 topic、segment、consumer offsets；失败不得 reset。 |
| Windows retention sharing violation 故障注入回归 | 未开始 | 测试待设计 | 复现真实 rename 失败，验证告警、退出检测与恢复。 |

### 7.2.1 2026-08-24 后续缓解

- `598e1c15` 在策划机免 Docker Kafka 配置中增加 `log.cleaner.enable=false`，避免 Windows 上
  `__consumer_offsets` 压缩阶段的 `.timeindex.cleaned` → `.timeindex.swap` rename 再次触发唯一
  `log.dirs` 失效；Docker、K8s 与线上 Linux 配置不受影响。
- `a5768be8` 同时禁用按时间删段，`e4f40330` 为 Kafka JVM 显式复用已验证支持 AF_UNIX 的临时目录，
  分别覆盖 `.timeindex` 删除 rename 与 JDK 21 `java.nio.channels.Pipe` 初始化失败这两条 Windows 路径。
- `tools/scripts/tests/localinfra_kafka_planner_tuning_contract_test.ps1` 已通过，证明生成配置包含该开关。
- 这只是策划机缓解措施，不等于事故关闭：尚未完成原数据目录受控恢复、topic/offset 对账、故障注入、
  运行期退出检测和玩家 E2E，以下关闭闸保持未勾选。

### 7.3 防复发规则

- 启动期 ready 不能替代关键基础设施的运行期存活监控；Kafka 退出必须撤销“可玩”结论并给出明确现场。
- Kafka data 目录出现 rename/sharing violation 时，先捕获 exact holder 并保留现场；不得用删除 data/reset
  绕过根因和恢复验证。
- 本机优化不得修改 K8s/生产 Kafka 配置；如形成跨环境规则，另行评审并记录。

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| 原始失败链 | retention rename sharing violation 后 all log dirs failed，broker 退出 | 未修复 | Windows 本机 Kafka 3.9.1 | `kafka.log` SHA256 与行 2694-2818 |
| 当前 listener/process | `9093/9094` listener 0、Kafka JVM 0 | 未恢复 | 只读 `netstat` + 进程身份 | 2026-08-21 08:06:52 UTC 现场 |
| 数据/索引完整性 | 未执行；存在部分 `.deleted` 状态 | 未执行 | Kafka storage/segment 校验待定 | 阻断关闭 |
| 针对性回归 | 未执行 | 未执行 | Windows sharing violation 注入待设计 | 阻断关闭 |
| 策划机配置契约 | 无 cleaner 禁用约束 | PASS | `pwsh tools/scripts/tests/localinfra_kafka_planner_tuning_contract_test.ps1` | 仅静态缓解契约，不替代故障注入 |
| 集成恢复 | 未执行 | 未执行 | 原 data 目录受控重启 + topic/offset 对账 | 阻断关闭 |
| `go test -race` | 不适用 | 不适用 | 本事故未改 Go 代码 | — |
| fatal/进程退出注入 | 本次是真实非预期退出样本 | 未执行修复后注入 | Kafka broker 运行期退出 | 阻断关闭 |
| 玩家 E2E | 未执行 | 未执行 | 一键入口 → Login → SelectRole → Hub/Battle | 阻断关闭 |

## 9. 部署、回滚与观察

- 修复 commit：无；本次只新增事故文档和索引，未改生产代码/配置。
- 构建产物/镜像 digest：不适用。
- 部署时间与目标环境：未部署；Kafka 未重启。
- 实际 Pod `imageID` / GameServer provenance：不适用；本机原生进程，未碰 K8s/Agones。
- 回滚条件和步骤：当前无修复可回滚。任何未来恢复动作都必须先保留 `run/localinfra/data/kafka` 与
  日志证据，再制定不删除数据的回退路径。
- 观察窗口、指标与结果：未开始；当前仍为 `9093/9094` 无 listener。必须在受控恢复后覆盖至少一个
  retention 周期/强制 retention 故障路径，并完成玩家 E2E，不能仅凭端口重新监听关闭。

## 10. 剩余风险与行动项

| ID | 严重级别 | 行动项 | 负责人 | 状态 | 目标/关联 Incident |
|---|---|---|---|---|---|
| A-1 | P0 | 捕获并定谳 `.timeindex` exact handle owner；禁止猜测来源 | 待指定 | 未开始 | 本事故根因闭合 |
| A-2 | P0 | 在不删除/reset data 的前提下做 storage 校验、受控重启和 topic/offset 对账 | 待指定 | 未开始 | 实际恢复 |
| A-3 | P0 | 增加 Kafka 运行期退出检测，退出后撤销策划“可玩”状态并明确报错 | 待指定 | 未开始 | 永久修复 |
| A-4 | P0 | 做 Windows retention sharing violation 故障注入，验证检测、恢复和数据安全 | 待指定 | 未开始 | 防复发回归 |
| A-5 | P0 | 完成真实 Login → Hub/Battle E2E 与 Kafka producer/consumer 恢复验证 | 用户/待指定 | 未开始 | 关闭硬门 |
| A-6 | P1 | 完成同机杀软、索引、备份及其他文件扫描者的配置排查，只记录有证据的命中 | 待指定 | 未开始 | holder 排查 |
| A-7 | P1 | 评估并记录单 `log.dirs` 在策划本机链的恢复策略，不把多目录伪装成已修复 | 待指定 | 未开始 | 恢复设计 |

## 11. 关闭审核

- [ ] 直接根因和放大因素均有证据（broker 退出链已证实，文件 holder 未知）
- [ ] 修复前失败、修复后通过的回归存在
- [ ] race/集成/故障注入达到本事故风险要求（Go race 不适用，Kafka 故障注入未执行）
- [ ] 同类问题扫描完成
- [ ] 目标环境已加载可追溯的新产物
- [ ] 玩家路径、恢复和补偿路径验证通过
- [ ] 观察窗口无复发
- [ ] 剩余风险已解决或另建 Incident/任务
- [x] 文档已脱敏且时间线时区明确

**关闭结论与审批人**：未关闭；文件占用者、数据完整性、实际恢复、故障注入和玩家 E2E 均未完成。
