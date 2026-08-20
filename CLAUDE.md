# Pandora 项目规范

> 本文档是 Pandora 项目的"宪法",AI 协作和人类开发都须遵守。后端项目,适配 MOBA + UE DS + 双仓库架构。

## 1. 项目基本信息

- **类型**:MOBA(5v5)+ 持续在线大厅(全图自由 PvP,500 人/hub 实例)
- **后端**:Go(14 个服务 + 公共框架 pkg/)
- **客户端 + DS**:UE 5.8 + GAS + Iris,**独立仓库**(本仓库 `Pandora` 是后端)
- **DS 编排**:Agones on k8s
- **协议**:gRPC(同步) + Kafka(异步事件)
- **基础设施**:MySQL 8 + Redis 8 + Kafka 3 + etcd 3

## 2. 仓库结构与边界

```
E:/work/Pandora/                # 后端（本仓库）
UE 客户端 + DS                  # 独立仓库，工程统一为 Pandora
```

- UE 工程 / 模块 / 类命名统一为 `Pandora`
- proto cpp pb 同步目标仓库为 Pandora-Client（具体输出路径待接 buf.gen.cpp.yaml）

## 3. 中文回复

所有 AI 协作产出**用中文**。注释、commit message、文档全中文。

## 3.1 首次生产上线兼容闸（全仓唯一权威）

**首次生产上线日期（YYYY-MM-DD）：** `未填写`

本字段是全仓判断“是否必须兼容已发布版本与已存在生产数据”的**唯一事实源**。`未填写` =
项目尚未首次生产上线；替换为真实日期 = 从该日进入生产兼容模式。这里的“生产上线”指首次向真实玩家或
外部生产消费者开放，并产生必须长期保留、不能随开发环境一起重建的数据或协议契约；本地、CI、临时测试、
压测和 staging 不会自动改变该字段。**只有用户本人可以填写或纠正日期**，AI 不得根据 Pod、镜像、tag、
测试环境或提交历史自行推断、自动填写、清空或回退。首次生产发布前必须先由用户填写；一旦真实上线，不得
通过清空日期绕过兼容要求。

兼容闸只控制“为兼容已经发布的版本、客户端或持久化数据而存在”的约束，包括：

- protobuf / gRPC / HTTP / Kafka 的字段、编号、枚举、消息与 RPC 契约；
- MySQL / TiDB schema 与 migration 基线、Redis / Kafka / blob 等序列化格式、缓存 key / value 格式；
- 配置表 schema、生成物、UE 客户端 / DS 与服务器之间的协议版本和数据快照；
- Stable / Canary、新旧二进制、旧客户端与新服务的混跑兼容窗口，以及 expand → migrate → contract 顺序。

### 日期未填写：首次上线前的破坏性重整

允许为了得到更干净的首发基线而做**有意且整批闭环**的破坏性变更：proto 字段可改名、改编号、改类型、
改基数 / 语义、删除或复用编号；enum value、message、RPC、数据库基线、迁移、序列化格式和开发数据也可
随同重整。此时不需要兼容尚未发布的旧构建，也不把旧 dev / staging 快照当成生产包袱。

“允许破坏”不等于“只改一处”。同一交付批次必须完成并验证以下刷新闭环：

1. 同步修改所有生产者与消费者：Go 服务、UE 客户端 / DS、脚本、测试、fixture / golden、运维工具与部署模板；
2. proto 至少运行 `pwsh tools/scripts/proto_gen.ps1 -Cpp`，提交全部 Go / C++ 生成物并编译 / 测试所有启用模块；
   若本次就是有意 breaking，`-Breaking` 的失败只用来列出变化，不能代替生成与全链验证；非 breaking 变更仍跑
   `pwsh tools/scripts/proto_gen.ps1 -Cpp -Breaking`；
3. 枚举所有受影响数据与状态。要么提供转换 / migration，要么在用户授权的**明确 dev / test 目标**上重建；
   已执行过的 migration、`schema_migrations`、Redis / Kafka / blob、缓存、索引和本地存档必须与新格式一起刷新，
   不得让新旧字节混读，也不得把“刷新数据”扩大成未指明目标的删除；
4. 重新生成配置表 `dist` / manifest、客户端 DataTable / 资源、协议版本、缓存 / schema 版本和所有派生产物，
   保留真实 source revision / checksum；不能留下“源码已改、生成物或数据以后再刷”的半成品；
5. 在提交与 `PROGRESS.md` 明确写出 breaking 范围、刷新对象、验证证据和仍需用户执行的客户端编译 / 数据重建。
   含 proto 的 commit message 仍标 `[proto]`。

### 日期已填写：生产兼容模式

从填写日期起，`§5`、`§9.16`、`§9.17`、`§9.21` 与
[`docs/design/zero-downtime-update.md`](./docs/design/zero-downtime-update.md) 的兼容、滚更和数据演进规则全部作为
硬门禁：字段 / enum 编号不复用，删除走 `reserved`；RPC、事件与存储格式须支持共存窗口；schema 走版本化
migration 与 expand → migrate → contract；任何破坏性演进必须新建版本 / 字段 / 格式，附迁移、回滚和混版验证，
不能原地改旧契约，也不能用删库或清空日期代替迁移。

### 无论日期是否填写都不能放宽

本闸**不代表功能已完成或可上线**，也不豁免鉴权 / 授权、唯一 owner 与 fencing、原子性 / 幂等、事务边界、
数据不丢不串、容量 / 分页 / 保留期上限、严格 SQL、输入校验、超时 / 重试 / 可见恢复、玩家不卡死、事故建档、
测试与发布证据等正确性和安全红线。任何规则如果同时保护兼容性与正确性，只能放宽其中“兼容旧版本 / 旧数据”
的部分；不能用“尚未上线”解释脑裂、数据损坏、安全突破、无界增长、半成品接线或无出口等待。

## 4. 提交纪律

1. commit message 格式:`<type>(<scope>): <subject>`
   - type:feat / fix / refactor / test / docs / chore / perf
   - scope:服务名(login / matchmaker)/ pkg / docs / deploy
   - 例:`feat(matchmaker): MMR 撮合算法初版`
2. proto 改动要在 commit message 标注 `[proto]`,提醒同步到 UE 仓库
3. PR 描述必须含:动机 / 改动范围 / 测试方式 / 风险点

## 5. proto 同步流程(双仓库)

1. proto 改动后由 Codex 跑 `pwsh tools/scripts/proto_gen.ps1` 生成 go pb
2. cpp pb 同步到 UE 仓库 `Source/Pandora/Generated/Proto/`,由 Codex 执行
3. UE 客户端改动跟随后端 proto 同步,由 Codex 协助
4. 字段编号兼容阶段由 `§3.1` 的首次生产上线日期唯一决定：日期未填写时可在整批刷新闭环内删除 / 复用 / 重排；日期填写后**不复用**,只能 deprecate(`reserved 5;` + 注释原因)
5. `player_id` / `team_id` / `match_id` / `order_id` / `message_id` / `dialogue_id` / `hub_id` / `invite_id` 等 Snowflake 业务 ID **一律 `uint64`**,不准用 `int64` / `string`;未知/空值用 `0`,需 presence 时用 `optional uint64`
6. 配置表 / 静态表 ID **默认 `uint32`**(`npc_id` / `hero_id` / `skill_id` / `item_config_id` / `map_id` 等);易与运行时实体混淆时新协议命名为 `<entity>_config_id`
7. 状态 / 类型 / 原因等 proto 枚举**不属 ID 规则**;enum 底层是 `int32`,Go 优先用生成的 enum 类型,不因取值非负改 `uint32`
8. 新增业务数据结构**优先定义 proto message**,按下表四类各司其职,**不准手写与 proto 重复的并行 struct**:

   | 类别 | 命名 | 用途 |
   |---|---|---|
   | RPC 请求/响应 | `<Verb><Domain>Request` / `<Verb><Domain>Response` | gRPC unary/stream 出入参 |
   | 客户端可见结构 | `<Domain>` / `<Domain><Part>`(短名,如 `Team` / `TeamMember`) | RPC response、push payload 里给客户端看的字段 |
   | 服务端存储快照 | `<Domain>StorageRecord` + 子结构 `<Domain><Part>StorageRecord` | Redis value、Kafka 快照、MySQL **blob 列**里序列化成 bytes 的整块状态 |
   | 服务间事件 | `<Domain><Action>Event` | Kafka payload;可内嵌"客户端可见结构",但本身是服务内部消息,不是存储快照 |

9. 第 8 条的"快照用 proto bytes"**只针对快照/blob 场景**(Redis value、Kafka payload、MySQL blob 列):关系型 MySQL 表(结构化列)不强制 proto 化,列直接映射 proto 字段;临时小令牌(如 invite,2~3 字段、短 TTL)允许继续用 redis hash。核心是"消灭与 proto 漂移的并行 struct",不是"一切序列化成 bytes"
10. proto message 直接当存储 record 时:**禁止值拷贝**(`a := *rec` 会复制 state/mu/sizeCache),克隆一律 `proto.Clone`;存储与客户端结构**分两个 message**,存储侧独有字段(如 `updated_at_ms`)不外露
11. **禁止把存储快照原样返回/推送给客户端**。RPC response / push 只能用"客户端可见结构",由服务端从 `StorageRecord`/MySQL 行/Redis 状态按**最小数据单位**填充,必要时重算派生字段(ready、queue_seconds、mmr_delta、显示昵称)。例外只能是写入设计文档的运维/调试 RPC,且须鉴权、脱敏、不经 Envoy 对客户端开放。
12. **非负整型字段默认用无符号类型**:语义上不可能为负的整数(数量、计数、时长毫秒、容量、索引、金额、版本号、时间戳 ms、序号等)默认用 `uint32` / `uint64`,不用 `int32` / `int64`。选型:预期不超过 ~42 亿用 `uint32`,可能更大或属 Snowflake 业务 ID 用 `uint64`(遵循第 5、6 条)。**例外(仍用有符号)**:①语义上可能为负的差值 / 增量(如 `mmr_delta`、坐标偏移、余额变动 `delta`);②参与减法且可能下溢的字段(用有符号避免无符号回绕);③已属枚举 / 状态 / 原因(见第 7 条,保持 int32 语义)。跨语言注意:JSON / JS 场景 `uint64` 仍按字符串编码,与现有 ID 规则一致。

## 6. 服务命名 / 端口规范

详见 [`docs/design/infra.md`](./docs/design/infra.md)。**不允许 ad-hoc 起端口或 key**。

## 7. 决策记录入口

`CLAUDE.md` 只保留稳定规范和索引,不再维护长决策表,避免每次会话重复消耗 token。

- 架构级决策:见 [`docs/design/pandora-arch.md`](./docs/design/pandora-arch.md) §11。
- 服务级决策:写入对应 [`docs/design/<service>.md`](./docs/design/) 或服务 README。
- 压测结论:写入 `docs/design/stress-<round>-*.md`。
- 崩溃与 P0 事故:按 [`docs/incidents/index.md`](./docs/incidents/index.md) 当次建档并持续更新；稳定约束再同步到 design/ops，不能用设计文档或审核交接替代事故档案。
- **性能画像 / 性能优化 / profiling 工具链:一律写入 [`docs/ops/`](./docs/ops/)**——**含 UE 客户端 / DS 的性能工作,客户端仓库不另存一份**(避免两边漂移;客户端侧同条款见 `Pandora-Client-SVN/CLAUDE.md「性能优化文档归档规则」`)。落点:案例化的「坑 → 因 → 修 → 验」追加 `docs/ops/ue-ds-perf-pitfalls.md`;工具选型 / 采集流程进 `docs/ops/perf-profiling-toolchain.md`;一次完整专项(画像数据 + 修复清单 + 待办 + 决策记录)新建 `docs/ops/性能优化-<主题>-<YYYYMMDD>.md` 并在上述两份手册的「关联」里加一行指回去。每份必须写清**样本口径**(地图 / 镜像版本 / 窗口时长 / 玩家与怪物数量)、**原始数字与基线**、**验证判据**、**未验证项**;只写"优化了 XX"而无前后对比数字不算完成(同 §8 压测纪律)。埋点注入、临时抬阈值等运维操作必须写明还原步骤并注明是否已还原;画像期间发现的 P0 / 崩溃仍按事故档案流程走,性能文档只引用不替代。
- 周期进度与流水账:追加到 [`PROGRESS.md`](./PROGRESS.md),只追加不删旧条目。

## 8. 压测纪律

详见 [`docs/design/stress-discipline.md`](./docs/design/stress-discipline.md)。**核心**:跑测前必有 `prev-summary.txt` 且清空 redis/mysql/etcd/kafka offset/k8s GameServer;至少 3 次 prom snapshot(ramp 完/稳态中/稳态末);用 summarize 脚本输出五段二维表,不手 grep raw prom;没对比表不准声明"性能提升";压期不上传日志。**库增长断言(§9.24)**:压前用 `tools/migrate/cmd/dbcheck` 抓行数基线并核对登记清单/清理索引,压后 `-compare` 断言 outbox 排空、每表增量能用业务量解释、无未登记表;压后再用 `-pending` 记录各表待清理量(判断保留期与清理模式是否需要调整)。任一不过不许宣布本轮通过。**注意 dbcheck 永不删数据**(旧 `-force-sweep` 已移除),保留期清理默认只报告不删。

## 9. 不变量(数据一致性 / 安全)

跨服务必须保持的不变量。任何改动违反这些 → PR review 直接拒。

1. **玩家同一时刻只能在一个可操作 DS**(由唯一 owner authority 的每玩家 `owner_epoch`、短 owner lease fencing 与 Admission 交接屏障强制；player_locator 只作 presence / 最近活跃投影)
2. **战斗结果幂等**(同一 match_id 只落库一次)
3. **DS 票据是进场权威的唯一搬运通道,短时效且不得取消**:v2 RS256 生产档默认 TTL 120s、
   硬上限 180s(`pkg/auth/dsticket.go` 签发与验签双向强制;`ds_ticket_ttl: 5m` 只属 legacy
   HS256 路径,勿按 5min 计算安全窗口)。票据不是一套可裁撤的独立鉴权,而是把**已由权威算好
   的判定结果**搬到 DS 的唯一不可伪造通道:`player_id` / `role_id`(§9.6 五要件①身份)、
   `hub_assignment_id`(归属版本,Transfer / Release / 同名 Pod 重建后旧票自动失效,是容量与
   强制整合迁移的执行手段)、`ds_pod` / `ds_uid` / `ds_instance_epoch`(§9.22 exact 实例绑定)、
   `release_track`(§9.21 灰度轨道粘滞)、`jti` + 短 exp(B1 纯本地验票下**唯一**的吊销手段,
   时延上界 = TTL + DS 侧 leeway)、`sjti`(§9.23 会话 fencing)、`source_match_id`
   (Battle→Hub 回流 fence)。取消票据并不减少要搬的东西,只会同时打穿身份、容量、灰度与
   吊销四道门,且唯一自洽的替代(DS 每次 PreLogin 同步查后端,即 UE 侧已存在的
   `OnlineAuthority` 档)是用在途同步依赖换掉离线可验签名,按 §15.2 更复杂而非更简单。
   开发期嫌密钥体系麻烦应切 `local-off` 档,不得取消验票;`ShouldEnforceTicketForNetMode`
   对 `NM_DedicatedServer` 恒为 true,不可配置关闭。重开此议题必须先满足
   `docs/design/decision-revisit-hub-ds-ticket.md` §7 的四条举证门槛(替代不可伪造通道 /
   吊销时延测算 / §9.21 共存窗口验证 / §9.19 有界驱动证明),仅以"看起来更简单"为由的取消提案直接驳回
4. **DS 崩溃必有补偿**(Battle DS 15s 心跳超时 → abandoned → 段位回滚;Hub DS 默认 30s 超时 → draining/停止分配)
5. **proto 字段编号兼容规则以 `§3.1` 为准**：首次生产上线日期未填写时允许随整批刷新复用；日期填写后永久不复用
6. **派生数值一律服务端计算;DS 的写权限有范围、可验证、有额度**(原"DS 不可信",
   2026-07-21 MMO 化修订——原则内核"限制单实例被攻破/出 bug 的爆炸半径"不变):
   - **数值不信 DS**:MMR / 经验换算 / 掉落发放判定等派生数值仍只在 battle_result 等
     Go 服务由**事实**换算(DS 只报事实不报数值),换算表 / 白名单 / 上限在服务端;
   - **DS 可作受信写者**(玩家背包等在线权威态,MMO 化方向),但每笔写必须同时满足五要件:
     ①**身份**可验证(DS JWT / writer epoch);②**owner 授权**(该 DS 当前持有该玩家的
     owner lease,服务端按 owner 权威校验,不得只验"是合法 DS"——模板见 battle_result
     的 Guard + active match + roster 链);③**fencing**(写带 owner_epoch,失租旧写一律拒);
     ④**额度**(journal / 入账层保留速率与单场 / 单时段上限);⑤**审计**(写走 journal
     流水,可发现可修复)。任何绕过五要件的 DS 直写权威库一律拒。
7. **交易资源扣减必须原子 + 有补偿幂等键**
8. **所有写都要带 trace_id**
9. **kafka topic key = 业务实体 ID**(同一玩家 / 同一对局事件有序)
10. **Redis lock TTL ≤ 30s**,业务跑完主动释放
11. **Snowflake 业务 ID 一律 uint64**(`player_id` / `team_id` / `match_id` / `order_id` / `message_id` / `dialogue_id` / `hub_id` / `invite_id` 等),不准新增 `int64` / `string` 型业务 ID
12. **配置表 ID 默认 uint32**(`npc_id` / `hero_id` / `skill_id` / `item_config_id` / `map_id` 等),不准新增有符号配置 ID
13. **proto enum / 状态常量保持 enum/int32 语义**(`TEAM_STATE_*` / `STATE_*` / `*_REASON_*` 等),不准因枚举值非负改成 `uint32`
14. **客户端只拿客户端可见结构**:任何面向客户端的 response / push 不准直接返回 `*StorageRecord`、数据库整行、Redis value、内部 Kafka envelope 或内部审计字段;必须经服务端组装成最小视图,只包含客户端渲染 / 交互所需字段。
15. **配置表热更走标准流水线**:**版本号 + checksum + staging 目录 + reload 接口 + 加载成功才切换 + 失败保留旧配置**。新表加载+校验全过才原子替换内存指针,任一步失败保留旧表不影响线上;version 单调递增防回退;发布通知用 etcd version 键(复用 `etcdtable`/`etcdnode`,不存表体),**不引入 Apollo / Nacos**。详见 `docs/design/config-table-hotreload.md`。
16. **生产服务必须支持不停服更新(零停机滚动更新)**:go 服务全部 headless、无状态、可水平扩展,权威态在 Redis/MySQL/etcd,进程内只做缓存/Actor 邮箱,收到 `SIGTERM` 先摘流量→排空在途→flush write-behind 脏数据→退出,任意副本可随时被杀被替换。生效阶段按 `§3.1`：日期未填写时，明确的非生产环境可随整批破坏性重整而重建，不要求兼容旧 dev / staging 快照；日期填写后，**任何依赖「先停服再启动」才能上线或才能读数据的设计一律拒**。
17. **生产模式下 Redis 二进制 pb 存储只做兼容演进,支持不停服加玩家数据**:生效阶段按 `§3.1`；日期未填写时允许破坏性重整，但必须刷新全部读写者与受影响数据。日期填写后，滚动更新期间新旧版本副本同时在线,存储 pb 必须**双向兼容**——只允许**加新字段(新编号)/ 加 enum 值(带 `*_UNSPECIFIED` + fallback)/ `reserved` 删字段**;**禁止**改 field number、改类型、改基数/语义、复用 reserved 编号。**read-modify-write(读 Redis→改→写回)路径禁止 `DiscardUnknown` 丢弃 unknown fields**(否则旧副本回写会静默丢新字段)。加玩家数据 = 加 proto 字段 + 懒迁移(下次改写自然补齐或不停服 backfill),**不停服、不全量刷库**。详见 `docs/design/zero-downtime-update.md`。
18. **客户端可写入的累积列表必须有上限**:凡客户端能主动新增、可长期堆积、又会被客户端拉取展示的列表,必须同时具备**写入侧总量上限**和**读取侧分页 / 单次返回上限**。包括但不限于:好友、好友申请、公会申请、公会成员、组队 / 入群邀请、临时群成员、我所在的群、交易请求、黑名单、订阅 / 关注 / 待处理队列。只做唯一键防重复不够;只做列表分页也不够。新增此类功能时必须在服务端事务 / 原子写路径里校验单玩家、单目标或单实体的 pending / active 数量上限,配置有默认值,超限返回明确业务错误,并在 proto 列表接口提供 `cursor+limit+next_cursor` 或等价分页(列表被写入侧硬上限兜住到几百内时,单次全量返回 + 服务端 `SQL LIMIT` 兜底即算「单次返回上限」达标)。没有上限的方案一律拒。

    **现存受管列表清单**(新增同类列表照此登记 + 落上限):

    | 列表 | 写入侧总量上限 | 读取侧上限 | 超限错误码 |
    |---|---|---|---|
    | 好友列表 | `max_friends` 默认 200(AcceptFriend 事务原子校验) | `ListFriends` SQL LIMIT | `ERR_FRIEND_LIMIT` |
    | 好友申请(收件箱) | `max_incoming_requests` 默认 200(CreateRequest 事务校验 target pending 数) | `ListFriendRequests` SQL LIMIT | `ERR_FRIEND_REQUEST_LIMIT` |
    | 黑名单 | `max_blocks` 默认 200(Block 事务校验) | `ListBlocks` SQL LIMIT | `ERR_FRIEND_BLOCK_LIMIT` |
    | 公会成员 | `max_guild_members` 默认 100(ApproveJoin 事务原子校验) | `ListMembers` cursor 分页 | `ERR_GUILD_FULL` |
    | 公会申请(每公会 pending) | `max_pending_requests_per_guild` 默认 200(CreateJoinRequest 事务校验) | `ListJoinRequests` cursor 分页 | `ERR_GUILD_REQUEST_LIMIT` |
    | 临时群成员 | `max_group_members` 默认 50(建群 / AddMember 事务原子校验) | `ListGroupMembers` SQL LIMIT | `ERR_GROUP_FULL` |
    | 我所在的群 | `max_groups_per_player` 默认 50(建群 / AddMember 事务校验) | `ListMyGroups` SQL LIMIT | `ERR_GROUP_JOIN_LIMIT` |
    | 组队入队申请(每队 pending) | `max_applications_per_team` 默认 10(ApplyToTeam 在单个 Redis key 上用 Lua 原子完成「清过期 → 校验上限 → 占位」;重复申请同一队伍只刷新自身 score,不占新名额) | `ListTeamApplications` 单次全量返回,服务端按同一上限截断 | `ERR_TEAM_APPLY_PENDING_LIMIT`(3009) |
    | 交易订单(单玩家参与,买/卖两侧各计) | `max_orders_per_player` 默认 200(CreateOrder 用 Lua SCARD+SADD 原子预留双方反查索引名额;满时惰性清理已终态/已回收成员再重试一次) | `ListMyOrders` cursor 分页 + SMEMBERS 全量被写入侧硬上限兜住 | `ERR_TRADE_ORDER_LIMIT` |
    | 拍卖订单(单玩家 PENDING + 活跃) | `max_active_orders_per_player` 默认 200(Claim PENDING 后用 Redis Lua SCARD+SADD 原子预留含 market_id+order_id 的成员;满时按 MySQL 权威状态有界惰性清理) | `ListMyOrders` 按全局 order_id cursor 分页,默认 50/最大 100 | `ERR_AUCTION_ORDER_LIMIT` |
    | 活跃任务列表 | `max_active_missions` 默认 50(AcceptMission 与完成扇出自动接链在同一事务内 FOR UPDATE 校验;自动接链超限跳过该条不阻断) | `ListMissions` 全量返回被写入侧上限兜住 | `ERR_MISSION_ACTIVE_LIMIT`(11007) |
    | 已完成任务列表(`player_mission_done`) | 每玩家每任务至多 1 行,规模 = 任务表行数,由 `configtable.MaxMissionRows`=2000 **加载期拒批次**兜住(2026-08-11 补:此前"被任务表行数有界"只是描述,没有任何代码拒过批次) | `LoadPlayer`(只读路径)`ORDER BY mission_config_id LIMIT 2000`;**事务路径刻意不加 LIMIT** —— `MutatePlayer`/`ApplyFactsTx` 用完成集判"已完成不可重复接取"与领奖 CAS,截断会让已完成任务被判成可重新接取 → 重复发奖 | 无(截断不报错,超限在配置加载期就被拒) |

    **受管的客户端触发型内存容器**：UE DS 的已消费 DSTicket JTI cache 虽不对客户端分页展示，也必须
    按同一有界纪律维护：`JTI→exp+leeway` 到期清理，硬上限为
    `min(effective MaxPlayers×每玩家窗口预算, configured absolute max, 65536)`；满载 fail-closed，
    禁止驱逐未过期 JTI 后重新允许重放。契约见 `docs/design/agones-dev.md §5`。

19. **DS 过渡全程不卡玩家（no-freeze，需求级硬性要求）**：玩家在「登录 → 选角 → 进 Hub DS」或
    「匹配 → READY → 进 Battle DS」的**任意时点**切后台、断网或杀进程，回到前台/重启后**必须**能在
    有界时间内自动恢复，或停在带真实可点入口（重试/放弃）的显式 UI，并最终只进入唯一权威 DS 或
    显式登录态；**任何阶段都不允许出现无人驱动的静默等待（卡死）**。落地约束：
    - 客户端所有 DS `ClientTravel` 只允许 `UMyDsRecoveryCoordinator` 单写者发起；每个异步回调必须过
      generation + request-seq + phase 三元校验，迟到回调零副作用。
    - 服务端对 UNKNOWN owner / route 一律 fail-closed 且零副作用（不签票、不占座、不开 spawn gate），
      客户端只能退避重查；客户端本地超时/本地状态**不得**改写权威路由。
    - 每个恢复等待状态必须有 ticker / 回调 / 前台事件继续驱动；HTTP unary 必须有界超时，每个本地请求
      最终只进入 success / error / cancel / timeout 之一。业务必须容忍传输回调重复、迟到或完全不来，
      由 watchdog 继续驱动。无会话上下文（从未登录 / 显式登出 / 主动放弃重连）时，前台恢复不得触发
      任何强制关卡切换或静默重放登录。
    - `UMyDsRecoveryCoordinator` 用**一个共享 watchdog / ticker 按 phase 记录 deadline**；`ClientTravel`、
      地图加载、等待 PlayerController、Admission / 场景玩家态 ACK、旧连接 / 旧 Controller 清退每阶段
      都必须有界。不能只靠“理论上会到”的引擎事件或无限短周期轮询；阶段到期后只能重查权威并自动
      重试，或进入带真实重试 / 返回入口的 UI，不得再建一套 timer 状态机。
    `docs/design/battle-reconnect.md` 与历史 review 只提供上下文，**不能替代当前 HEAD 的代码、测试和
    故障注入证据**；违反本条的改动 PR review 直接拒。

20. **任何代码都不能让玩家卡死或进不去场景（全局硬性要求）**：第 19 条的有界驱动 / 恢复入口与第 23 条的单一幂等进场规则，适用于所有登录、选角、匹配、组队、传送、加载、重连、前后台切换、地图切换及进入 / 返回 Hub、Battle 的路径。禁止永久黑屏、无限 loading、无 deadline 等待或轮询、按钮不可用、只能杀进程恢复、残留状态阻止重新进场，以及客户端和服务端互相等待。新增或修改相关路径必须验证成功、超时、断网、重复 / 迟到 / 丢失回调、切后台、重启和部分失败；无法证明满足本条的改动不得合入。

21. **首次生产上线后，所有 Go 服务和 Hub / Battle DS 都必须支持金丝雀发布新版本且不停服（全局硬性要求）**：生效阶段按 `§3.1`；日期未填写时可把非生产环境、全部二进制和数据作为一个已验证批次重建，不要求与旧 dev 构建混跑，但金丝雀能力仍是首发架构与验收目标。日期填写后，Go 服务通过 stable / canary 新旧副本并存、readiness、按比例或按人群分流、优雅摘流量和在途排空发布；RPC、事件和存储格式必须在共存窗口双向兼容，异常时能立即把 Canary 权重归零，Stable 持续服务。已有 RPC / 字段 / 语义不得原地禁用或硬切；删除能力必须走 expand → migrate → contract，先让新旧调用方双向兼容并验证 Stable↔Canary、旧 DS↔新 Go、新 DS↔旧 Go 组合，最后一个旧调用者和旧 DS 排空后才能收缩。只有作用于**同一未分区权威 / 同一逻辑任务或分片**的单写者循环才共享 leader election；权威写同时原子校验单调 fencing token，失租旧 leader 永久不能补写。可并行 worker 应用 consumer group、行 claim、幂等 CAS 等标准机制，不得为金丝雀强行全局串行化。同一多步骤 operation 要么粘住 release track，要么其持久状态和迁移语义必须让新旧版本双向理解。DS 通过 Agones Stable / Canary 独立 Fleet 发布：Canary 必须先预热 Ready 后才接小比例**新分配**，同一玩家 / 对局固定 release track；回滚先停止 Canary 新分配，禁止删除或强杀仍承载玩家的 Allocated DS。旧 Battle DS 必须把当前对局跑完，旧 Hub DS 必须停止接新并安全迁移 / 排空玩家，确认空场后才退役。任何升级都不得要求全服停机、踢掉在场玩家、打断对局或让玩家无法进入场景；详见 `docs/design/zero-downtime-update.md` §6.3。

22. **状态优先查询唯一权威，不重复存储影子状态（全局硬性要求）**：先明确每个状态的唯一权威源，只在该处保存跨请求 / 重启仍影响正确性且无法可靠重建的**最小权威事实或必要租约**；其他服务需要状态时查询权威接口，不得各自持久化并信任一份副本。可由权威事实计算的展示状态、聚合状态和派生字段按需查询 / 计算；缓存与物化视图必须明确为非权威、带 TTL / version、可失效且可从权威源重建，不能参与扣减、准入、归属切换等权威写决策。关键迁移禁止普通“先查再存”（存在 TOCTOU 竞态），必须通过事务、CAS、条件更新、唯一键或等价原子机制完成检查与修改。查询缺失只有在契约明确时才能推导状态；查询超时、依赖故障或结果不确定必须返回 `UNKNOWN` / `UNAVAILABLE` 并重试或 fail-closed，禁止冒充 `OFFLINE`、空闲、成功或其它默认状态。

    状态必须按语义拆开，不能让一个易失 TTL 同时承担在线展示和玩家归属：
    - `LOCATION_STATE_HUB` / `BATTLE` 是 player_locator 的短期 **presence / 最近活跃投影**，按需查询且不在其它服务复制；key miss 只能说明 presence 不可见，**不能**单独证明玩家已离开旧 DS，也不能授权进入另一台 DS。
    - 选角结果查询角色权威，匹配阶段查询 matchmaker 权威；locator 中若暂留 `MATCHING`，也只能是投影，不能替代 durable match stage。
    - 当前哪台 DS 有权控制和修改玩家，必须有一份最小玩家 owner 权威：**每玩家**单调不回退的 `owner_epoch`（不同于 DS `instance_epoch`、writer epoch、session epoch）、owner 类型、exact DS identity、稳定 `operation_id`、短 lease 截止、迁移 `admit_not_before` 屏障和最多 `PENDING / ADMITTED` 两阶段；owner 记录不能因 TTL 消失，短 lease 只限制旧 DS 可操作的物理残留窗口，不得扩展成通用 placement saga、多套 proof 或影子状态。
    - 只有唯一 owner authority 能通过受控 transition API 原子修改 owner；Login、matchmaker、allocator、DS 等只能查询或提交绑定 `operation_id` 的命令，不得各自 raw CAS 同一记录。获得业务权限必须同时匹配当前 `owner_epoch` 且 owner lease 未过期；连续续租失败进入安全余量或已无法证明 lease 有效时，立即停止接收新输入、跨服务回调和持久化 / 外部权威写，到期必须 Kick / Despawn。票据、Admission、Heartbeat、Logout、跨服务回调和持久化写都继承 / 校验 epoch；局内纯内存操作从已绑定 epoch 的连接上下文继承即可。旧 epoch 不得影响当前 owner 或业务状态，但允许幂等、精确地清理仅属于自己 epoch 的旧连接、Pawn / 玩家态、seat、lease 和审计记录。
    - **脑裂下只能有一个可玩 DS**：owner authority 及其底层存储必须提供线性一致读 / CAS、法定多数侧单写和“已确认写在故障切换后不回滚”；`owner_epoch`、lease 截止、`admit_not_before` 与 `PENDING → ADMITTED` 必须处于同一个线性一致事务域，禁止把 owner 放 MySQL、准入 lease / 屏障放 Redis 或 etcd 后再跨存储“先查后写”。普通 Redis TTL / 主从异步复制、Redlock、locator key miss、Pod 存活或本机时间都不能充当 owner 权威。切换 exact target 时，唯一 authority 用一次 CAS 把 `E/旧 target` 推进为 `E+1/PENDING/新 target`，并把 CAS 线性化点观察到的旧 lease 最晚安全截止时间（含已先完成的并发续租和时钟 / 网络安全余量）写入 `admit_not_before`；CAS 后旧 epoch 的续租一律失败。新 DS 在屏障打开前只能预留目标和加载地图级资源，不得创建可操作 Pawn / 玩家态、处理输入、产生业务写、提交 `ADMITTED` 或向客户端确认 `PLAYABLE`；屏障打开后仍须线性重查并以 exact `(player_id, owner_epoch, operation_id, DS instance identity)` CAS `PENDING → ADMITTED`。Admission 回包丢失只幂等返回同一结果，不得再分配或创建第二 owner。
    - 旧 DS 只把**当前 epoch 的成功续租响应**视为续租成功；已发送请求、超时 / UNKNOWN、缓存、locator 心跳或客户端仍连着都不算。它必须用单调时钟保存一个比 authority 屏障更早的本地安全截止时间，失联或到期时先原子关闭该玩家输入与所有业务输出，再 Kick / Despawn；进程暂停 / 网络分区后恢复时，在处理任何积压输入或 Tick 前先重查 epoch / lease，旧 epoch 只能精确清理自身残留。Stable / Canary 必须共享同一 owner authority、epoch / lease 语义和 Admission 门，任何 release track 都不得绕过屏障。核心时序必须满足：`旧 DS 最晚停止可玩时间 < 新 DS 最早开始可玩时间`。

    `docs/design/go-services.md` §2.6 与 `docs/design/battle-reconnect.md` 已于 2026-07-17 按本条修订（presence 投影语义 + §8 DS 授权租约 fencing / 再入屏障契约）；battle-reconnect.md 中标注「历史方案」的段落仅作演进背景。规范冲突时仍以本条为准；本条要求的每玩家 `owner_epoch` 线性一致 owner authority 尚未实现（当前为 §8 的 lease fencing + 再入屏障子集 + session JTI / exact 实例绑定迟到写防护），实现前不得宣称本条全量达成。

23. **登录成功后必须由一条幂等进场链收敛到可玩场景（全局需求级硬约束）**：`Login OK` 是认证会话已建立的线性化点。只要客户端仍持有有效 session / refresh 凭据，且依赖与容量最终恢复，系统必须最终收敛到一个且仅一个可操作场景；持续外部故障时也只能停在可见、可重试 / 可退出的 UI，不能黑屏或静默等待。除 session 明确过期 / 被撤销、封禁、强制版本不兼容等终态外，路由、匹配、分配、签票、Travel、Admission 的暂时失败不得清空会话、要求重新输入账号密码，或另起一条本地 fallback 路由。

    - **单一入口**：登录返回、选角完成、前台恢复、断线、Travel / Admission 失败、匹配 READY、Battle 结算回 Hub 和持有有效凭据的冷启动恢复，都必须汇入同一**无状态逻辑 API**与客户端 `UMyDsRecoveryCoordinator`。该 API 优先复用现有 Login 恢复入口，以 query-first 方式组合角色、matchmaker 与 owner 权威；需要新归属时只向唯一 owner authority 提交一次绑定 operation 的 transition，不新增有状态编排微服务，不复制领域状态。客户端不得自行在 Hub / Battle 间降级或维护第二套恢复状态机。
    - **最小状态集**：统一入口只返回 `ROLE_REQUIRED`、`WAIT`（含权威 match stage 与 `retry_after`）、`TARGET`（`PENDING / ADMITTED` + exact target + `owner_epoch`，票据可选且可安全重签）、`REAUTH` 或 `TERMINAL`。查询失败返回 `WAIT / UNKNOWN`，不得默认 Hub；角色查询失败不等于 role=0，未选角时不得提前分配 Hub、占座或签进场票。`TARGET/ADMITTED` 且客户端当前连接精确匹配 owner 时必须幂等 no-op，不能再次 Travel / 占座。
    - **端到端幂等**：一次真实进场 / owner 迁移使用一个稳定 `operation_id`：登录→选角→首个 Hub、Hub→Battle、Battle→Hub 分别是新的 operation；同目标重连、重复点击、请求重试、响应丢失、回调重复 / 乱序、前后台切换和服务重启继续原 operation。ID 在首次请求前生成并保留到客户端 `PLAYABLE`、明确取消或终态；冷启动丢失本地 ID 时必须先查询并恢复服务端当前 operation，不能竞争创建第二个。分配、归属切换和 Admission 用 CAS / 唯一键原子推进；单次票据可以安全重签新 JTI，但票据本身不是幂等键。任何 exact owner identity 变化（Hub↔Battle、Hub→Hub、Battle 实例替换、Pod UID / instance epoch 变化或灾备接管）都必须递增 `owner_epoch`；同 epoch 重试不得重复占座、重复分配 DS 或产生第二个 owner。重复 Login / SelectRole 必须在换 coordinator generation 前 single-flight 合并或按同一 operation 幂等返回，迟到响应零副作用。
    - **会话 fencing**：若产品采用顶号，会话权威必须校验当前 session id / JTI（或等价单调 epoch）；旧 session 不能再签 DS 票，迟到 Logout 只能 compare-delete 自己，不能删除新会话。
    - **完成点分层**：返回票据、调用 `ClientTravel`、World BeginPlay 或 locator 出现 HUB / BATTLE 都不算完成。服务端完成点是当前 owner epoch 仍在有效 lease 内、Admission 幂等提交且该场景要求的角色 / Pawn / 玩家态创建成功，并把 owner 标记为 `ADMITTED`；客户端连接 exact DS 且收到 connection-specific ACK 后才进入本地 `PLAYABLE`。ACK 丢失时重查 / 重放同一 Admission 必须返回原 `ADMITTED` 并重新确认，不能再次分配、占座或盲目 Travel。每一阶段都有 deadline、退避 + jitter、前台唤醒驱动和可见恢复 UI；每次等待有界，整体可持续重试。
    - **脑裂时安全优先但不能永久卡流程**：为了守住一人一 DS，可以在新 owner 的 `admit_not_before` 前返回带明确原因、`retry_after` 和 deadline 的 `WAIT`，但必须保留 session 与原 `operation_id`，由同一 coordinator 的 watchdog 到期重查，不能等待旧 DS 某个可能永不到达的回调。旧 lease 到期且 authority / 健康 DS 恢复后必须自动继续，无需重登、手工清状态或另走 fallback。若 owner authority 永久失去法定多数或长期无容量，“立即进入新 DS”与“绝不双 DS”无法同时保证，此时只能停在可见、可交互、可持续重试 / 可退出的 UI；不得默认 Hub、清 session、黑屏或静默 loading。这里的“永不卡”是没有内部永久中间态或无出口等待，并以 authority 与容量最终恢复为收敛前提，不能靠放开第二个 DS 换取表面可用性。
    - **验收矩阵**：至少覆盖重复 Login / SelectRole、响应丢失、MATCHING 各阶段切后台 / 杀进程、locator / Redis / matchmaker 分区、READY push 丢失、地图加载无回调、Admission ACK 丢失、同 Hub 旧 Controller 不退出、旧 DS 分区后恢复、迟到 Logout / Heartbeat、服务进程重启以及 Stable / Canary 新旧组合。测试未覆盖前不得声称“永远不卡”“幂等进场”或“无 bug”。

24. **持久化数据增长必须有界,但"因为数据大了"删数据默认不许做(2026-07-22 用户指令)**:

    保留期清理**默认只报告不删**(`retention_mode` 留空 = `report_only`):周期统计"有多少行满足清理条件"并打 `WARN` 日志 + `pandora_db_retention_pending_rows` gauge,一行都不删。真删必须由配置显式写 `retention_mode: delete`。**唯一例外:battle_result 的战报清理默认真删**(见下方"按域覆盖")。理由:自动删生产数据不可逆,清理条件 / 保留期 / 幂等窗口任一处配错都会静默删掉不该删的玩家数据,而且**删完才发现**;把"何时删"的决定权交回人手里。代价是 report-only 下库会继续增长——所以待清理量必须持续可见(WARN + metric + `dbcheck -pending`),让人能判断何时开删。实现见 `pkg/dbguard/sweep.go`:`Mode` 零值即 `ModeReportOnly`;`ParseMode` 对无法识别的值**报错而非猜成 delete**(拼错一个字母就开始删生产数据是不可接受的失败模式),启动 `ValidateRetentionMode` fail-fast。`SweepTable` 强制 Count 与 Delete **共用同一 where**,从机制上排除"报告说 0 行、实际删了 10 万行"的条件漂移。

    **必须分清两类删除,别混为一谈**:

    | | **业务语义删除**(不受本条约束) | **运维语义删除**(默认只报告) |
    |---|---|---|
    | 含义 | 东西本来就该没了 | 数据还有效,只是占地方 |
    | 例子 | 道具过期、邮件失效、挂单置 EXPIRED、玩家主动丢弃/解散公会、出箱投递成功即删 | 幂等流水超 90 天、终态申请行、已结算成交流水、私聊历史 |
    | 处置 | 照常删,是正常业务逻辑 | `retention_mode` 默认 report_only |

    **按域覆盖:战报(battle_result)默认真删,保留六个月(2026-08-03 用户指令)**。

    "MySQL 里最多只存最近六个月的战报,超过六个月的就没有数据"是产品口径,不是运维口味 —— 战报是产品上就有寿命的数据,只报告不删交付不了这条(库会一直涨)。因此 `battle_result` 的 `retention_mode` **留空即 `delete`**,`history_retention_days` 默认 180 并钳在 `[30,180]`(`internal/conf/conf.go` 的 `HistoryRetentionMaxDays` / `HistoryRetentionMinDays`,`budgets.go` 容量预算直接引用上限常量,不另抄一份)。配套纪律:①同一个值也是**玩家战报可见窗口**(`ListPlayerHistory` 只读 MySQL,当前无冷存归档),要给运营 / 客服留更久必须另做归档,不是加大这个数;②启动 `ValidateRetentionMode` fail-fast(本服拼错 `delete` 会让六个月口径静默失效);③下限 30 天是防手滑护栏(真删域里把 180 写成 18 不可逆);④首次开删前先 `dbcheck -pending` 看积压规模,存量库第一轮会把全部超期数据按批删完,建议低峰期发布。**其它域不受此影响,仍是 report_only 默认。**

    以下为"运维语义删除"的通用要求:任何随时间或玩家活跃度线性增长的只增表(幂等流水、审计、托管、归档、领取记录、outbox、历史/日志类表等)必须同时具备**保留期配置(带默认值)**和**周期清理任务**;玩家已失去 / 已终态 / 已关闭的数据(用完道具的流水、closed escrow、过期邮件、已结算订单等)物理保留**默认且最多 90 天**。清理必须小批量(`DELETE ... LIMIT`)防长事务锁表,多副本并发跑必须幂等;清理列必须有可用索引。幂等 / 防重行的保留期必须**远大于对应操作的最大重试 / 回放窗口**,且删除后迟到重放必须 fail-closed 或 no-op(不得重复入账 / 重复发放 / 重复冻结)——依赖某流水"永久兜底"的防重设计一律改为在源头闭环(例:邮件寿命钳到 claim 保留期内,而不是靠 inventory 流水永存)。确需超过 90 天必须写明理由并在下表登记为例外。**新增只增表必须同步登记到下表,没有清理方案的只增表 PR review 直接拒**:

    | 只增表 | 清理条件 | 保留期 | 清理任务 |
    |---|---|---|---|
    | inventory_ledger | created_at 超期 | 90 天(`ledger_retention_days`) | inventory `biz/sweep.go` |
    | auction_escrow(closed) | status=closed 且 updated_at 超期(active 永不清) | 90 天(`escrow_retention_days`) | inventory `biz/sweep.go` |
    | player_mail / sys_mail / guild_mail | 过期 / 失效 + 缓冲期 | 过期后 7 天(`expired_retention_days`) | mail `biz/sweep.go` |
    | player_mail_archive | archived_at 超期 | 90 天(`archive_retention_days`) | mail `biz/sweep.go` |
    | player_mail_claim | mail_id 早于 cutoff(雪花时间) | **180 天(登记例外)**:发送侧已把邮件可领窗口钳到本值内,claim 行必须活得比可领窗口长 | mail `biz/sweep.go` |
    | battles + battle_player_stats | created_at(服务端落库时间;§9.6 不信 DS 上报的 ended_at_ms,防伪造提前删/永不删)超期,同事务批删 | **180 天(登记例外)**:战报保留期 = 玩家可见窗口,产品口径"最多存最近六个月"(`history_retention_days`,钳 `[30,180]`);**本域 `retention_mode` 默认 delete** | battle_result `biz/retention.go` |
    | battle_progress_stream + battle_progress_player + battle_progress_item_balance + battle_progress_action | 以 stream 的 settled_at_ms>0 且超期为锚点,同 match 成组删除(未结算行永不清:陈年未结算 = 补偿链 bug 证据) | 180 天(同上) | battle_result `biz/retention.go` |
    | exp_history | created_at 超期(**默认 report_only**;真删还需第二道闸 `exp_history_cleanup_enabled`:上游 progress 出箱无总重试期限,清收据会双发,上游有界后运维显式开启) | 7 天(`exp_history_retention`) | player `RunExpHistoryJanitor` |
    | mmr_history / attr_point_grants / talent_point_grants / skill_card_grants | created_at 超期(**默认 report_only**;真删还需第二道闸 `history_cleanup_enabled`,同上:kafka 重放 / 授予补扫须先有界) | 90 天(`history_retention_days`,钳 `[30,90]`) | player `RunHistoryJanitor` |
    | chat_private_messages | message_id 早于雪花 cutoff | 90 天(`history_retention_days`) | chat `biz/sweep.go` |
    | friend_requests(终态) | status≠pending 且 updated_at 超期(pending 永不清) | 90 天(`request_retention_days`) | friend `biz/sweep.go` |
    | guild_join_requests(终态) | status≠pending 且 updated_at 超期(pending 永不清) | 90 天(`request_retention_days`) | guild `biz/sweep.go` |
    | auction_orders(终态) | status∈{FILLED,CANCELED,EXPIRED} 且 release/match_pending=0 且 updated_at_ms 超期 | 90 天(`retention_days`) | auction `data/retention.go`(逐分片) |
    | auction_matches(已结算) | settlement=COMPLETED 且 event_pending=0 且 matched_at_ms 超期 | 90 天(同上) | auction `data/retention.go` |
    | auction_idempotency_keys | created_at_ms 超期 | 90 天(同上) | auction `data/retention.go` |
    | leaderboard_snapshot | created_at_ms 超期 | 90 天(`retention_days`) | leaderboard `runRetentionSweep` |
    | leaderboard_reward_log(GRANTED) | status=GRANTED 且 updated_at_ms 超期(PENDING/FAILED 是补发工作集,永不清) | 90 天(同上) | leaderboard `runRetentionSweep` |
    | account_devices | last_login_at 超期(下次登录 upsert 自然重建) | 90 天(`device_retention_days`) | login `PurgeStaleDevices` |
    | bag_journal | checkpoint 覆盖位之前且超期 | 90 天(`journal_retention_days`) | inventory(bag 域)journal sweep |
    | owner_transition_log | created_at 超期(idx_created_at 已预留) | 90 天 | owner 服务 sweep(§9.22 工作线落地中) |
    | mission_reward_log(GRANTED) | status=GRANTED 且 updated_at_ms 超期(PENDING/FAILED 是补发工作集,永不清) | 90 天(`reward_log_retention_days`) | mission `biz/sweep.go` |
    | mission_fact_receipts | created_at 超期(**默认关**:上游 battle_progress_outbox 重试无总期限,删收据会让迟到重放双计进度;上游有界后运维显式开启,同 exp_history) | 90 天(`receipt_retention_days`) | mission `biz/sweep.go` |
    | pandora_planner_registry.planner_enrollment_tokens | 未消费且 expires_at 超期；或 consumed_at 超 30 天（短期安全凭据 hash 的业务性失效） | 未消费到 expires_at；已消费 30 天 | plannerdb-provisioner 每次管理签发前清理；`audit` 报待清理量，清理失败则拒绝签发 |

    **登记豁免(慢增长 / 权威闸,不清理)**:`pandora_planner_registry.planner_workspaces`(每已登记设备 1 行，是 workspace/加密凭据的跨重启唯一权威；`audit` 报 inactive/stuck/failed，默认不按时间/IP 自动删除)、`leaderboard_settlement`(settle uk 是防重复结算的永久闸,每批次 1 行)、`auction_owner_guards`(每 owner 1 行,被玩家数有界)、`friend_player_guards` / `friend_pair_guards`(好友域写守卫行,R5 复审 P1-2/4:TiDB 无 gap 锁,限额与关系变更先锁守卫行再进临界区;每玩家/每关系对至多 1 行,被玩家数与社交图对数有界,无业务语义不清理)、`account_bans`(运营合规审计,量级 = 运营操作数)、各出箱表(投递成功即删,积压属告警问题)、`player_items` count=0 行(被 uk 有界,删行会漂移错误码语义)、`mail_transfer_escrow`(邮件 transfer 附件在途托管行,领取/释放即删;行是已扣出实例资产的唯一持有处,量级 = 在途 transfer 邮件数,被个人邮件 TTL + 归档补偿链兜底,不得按时间清理)、`bag_migration`(旧 inventory 存量迁移幂等闸,一玩家一行永久保留;删行会让迁移作业重跑双倍入账,decision-revisit-bag-replay-semantics.md D5)、`player_no_counter`(角色编号全局发号计数器,恒 1 行,发号权威闸;`docs/design/player-no-and-login-surge.md` §3.3)、`register_no_counter`(同一个发号权威闸的**旧名副本**,恒 1 行;000006 曾 RENAME 掉它导致滚动升级期 Stable/Canary 无法共存,000007 expand 重新建回并与 `player_no_counter` 双锁双写,contract 前不得删除;`docs/design/player-no-and-login-surge.md` §3.6.4)、`player_mmr`(段位分唯一权威,每玩家每池至多 1 行,池数被关卡表「段位池」列取值数有界;段位是玩家资产,任何情况都不按时间清理)、`player_skill_cards`(每玩家每卡至多 1 行,被技能卡配置表行数有界)、`player_skill_slots`(每玩家至多 `biz.SkillSlotCount`=4 行)、`player_mission_active`(每玩家 ≤ `max_active_missions`=50 行,§9.18 写入侧上限)、`player_mission_done`(每玩家每任务至多 1 行,被任务表行数有界)、`mission_player_guards`(任务域每玩家写守卫行,与 `friend_player_guards` 同因:TiDB 无 gap 锁,`FOR UPDATE` 在零行时不加锁,活跃数上限与类型互斥校验须先锁守卫行再进临界区;每玩家至多 1 行,被玩家数有界,无业务语义不清理)。
    存量库补清理索引统一走 `tools/migrate` 各库 `*_retention_indexes` 迁移(幂等条件建索引);未登记不等于已达标。

    **机械化检查(上线前 / 压测强制)**:`tools/migrate/cmd/dbcheck` 内嵌与本清单同步的登记表,对真实库枚举全部 `pandora_%` 表并断言:①无未登记表;②swept 表清理索引齐备;③outbox 无堆积;`-snapshot`/`-compare` 供压测前后行数对比(增量必须能用业务量解释),`-pending` 供待清理量报告(只 COUNT),`-size-check [-top-rows N]` 供大字段体检(见下条)。**本工具永不 DELETE**(旧 `-force-sweep` 按用户指令已整块移除,且刻意不留同名 flag)。**上线前对生产库跑一次 dbcheck 且 PASS 是发布门禁**;新增表必须同时登记本清单与 dbcheck 内嵌清单,漂移即检查失败。唯一边界是独立 admin trust domain 的 `pandora_planner_registry`：业务 `dbcheck` 不得持有中心 admin DSN，也不跨 registry DB；该库由 plannerdb-provisioner 启动 shape verifier（精确列/约束/清理索引）与只读 `audit` 作为同等级门禁。压测接线见 `stress-discipline.md` §4.1.1/§4.3。

    **增长有两个独立方向,只管一个必漏另一个**(2026-07-22 补充,排查手册见 `docs/design/db-capacity-guard.md`):

    | 方向 | 症状 | 信号 | 闸 |
    |---|---|---|---|
    | **广度**:行数变多 | 表越来越大 | `TABLE_ROWS` | 保留期清理(上表) |
    | **深度**:单行变胖 | 行数正常但表照样大 | `AVG_ROW_LENGTH` | 写入侧上限(下述三条) |

    凡 blob / JSON / CSV / 位图这类**集合序列化**列,写入路径必须同时具备三个上限,缺一个就有洞:①**单元素上限**(单个格子 / 单条附件 ≤ M 字节且子元素 ≤ K 个);②**集合条目上限**(格子数 / 附件数 / 位图条数 ≤ N);③**整体字节上限**(序列化后 payload ≤ P,`pkg/dbguard.CheckPayload`)。只设 ③ 会漏掉"单个格子胖到 60KB 但整体没超";只设 ①② 会漏掉"每项合规但项数×大小仍超列容量"。**上限值按设计期望定,不按列类型上限定**——写成列类型上限等于没设(数据涨到快撑爆才告警,业务语义早已崩坏)。列类型同理:能用 `VARBINARY(N)` 就不用 `LONGBLOB`,后者等于 DB 层不设防。真实教训:`bag` 管住了 items 条数却没管单个 item 的 attrs 条数(深度无闸);`rewardclaim` 管住了单条位图大小却没管位图条目数(广度无闸,已修)。

    **`sql_mode` 必须含 `STRICT_TRANS_TABLES`,服务启动时断言并 fail-fast**(`pkg/dbguard.AssertStrictMode`)。非严格模式下超长写入不报错而是**静默截断**(真 MySQL 8.4 实测:往 `VARBINARY(16)` 写 100 字节,严格 → Error 1406 写入失败;非严格 → `err=nil` 且实际只存 16 字节),玩家数据被无声砍断且无任何错误可观测。这是唯一允许因数据库检查而拒绝启动的场景——静默数据损坏远比服务起不来严重。注意断言 `@@session.sql_mode` 而非 `@@global`(DSN 参数可覆盖 session)。

    **超容量预算只告警不阻断**(`pkg/dbguard.Guard.Check`):各服务在 `internal/data/budgets.go` 声明自己表的行数 / 平均行长预算,启动时跑一轮拿基线、之后挂已有 sweep ticker 周期跑(**不新建 timer 状态机**),走 `information_schema` 估算(毫秒级不锁表,**禁止 `COUNT(*)`** 拖垮启动),超限打 `ERROR` 日志 + `pandora_db_budget_violations_total`。容量超限是"要去查的问题",不是"服务不能跑的理由";拒绝启动会把容量问题升级成可用性事故(违反验收底线第 1 条)。写入侧另有"逼近告警"三档:达上限拒写(ERROR)、达 80% 放行但 WARN(留出排查窗口)、否则静默。

## 10. AI 协作约定

AI 协作规则以 [`AGENTS.md`](./AGENTS.md) 为准,本文件不重复维护细则,避免双文档漂移。

## 11. UE 工程约束(写给 UE 仓库开发者参考)

1. 类前缀统一 `Pandora*`(GameMode / Character / PlayerController)
2. 服务端逻辑统一在 `PandoraHubServer` / `PandoraBattleServer`,不在 `Source/Pandora/` 客户端模块
3. 蓝图只做"胶水"(挂技能动画 / UMG 绑定),逻辑在 C++
4. 资源走 Git LFS(`.uasset / .umap / .fbx / .png / .wav / .ogg`)
5. **永远不要提交** `Binaries/ Intermediate/ DerivedDataCache/ Saved/`
6. **UE 编译由用户本人执行**：I can compile UE myself. AI 改完 UE 代码后不必代跑 UE 编译（本机编辑器常开 Live Coding 会阻塞 UBT），把 UE 编译/测试验证交给用户。
7. **DS→客户端同一状态只允许一条下发通道（复制属性 + OnRep）**：UE 可靠有序只在单个 Actor Channel 内成立；Client RPC（走 PlayerController Channel）与目标 Actor 属性复制之间没有任何到达顺序保证，且同帧内 RPC（TickDispatch 即写）几乎必然先于属性 bunch（TickFlush 才写）到达。凡有生命周期的状态（读条 / 携带 / 阶段 / 进度）一律由复制快照 OnRep 单通道收敛，客户端缓存只能是快照的确定性投影、可随时重建；Client RPC 只用于一次性通知（拒绝原因、瞬时提示）。**禁止双通道下发同一事实后在客户端用 revision / 世代守卫弥合顺序**——这是 §9.22（唯一权威 / 不重复影子状态）在客户端表现层的对应物。实例：2026-07-28 宝箱读条曾用 `ClientLootChestUnlockStarted` RPC + `InteractionSnapshot` 双通道下发，客户端为压"按钮闪回"在 Controller 与 MainView 各写一份 revision 守卫；已改为快照单通道并删除该 RPC 与全部守卫。细则见 `Pandora-Client-SVN/CLAUDE.md「网络下发单一事实通道规则」`。

## 12. 不要做的事

- ❌ 不要在 docs/design/ 之外随便建 README(集中维护)
- ❌ 不要 import 第三方 GUI 库到 go 服务(go 服务都是 headless gRPC)
- ❌ 不要把 player_id 当 prometheus label(高基数会爆)
- ❌ 不要在 W1 写业务逻辑,只搭骨架
- ❌ 不要混用 `Pandora` / `pandora` / `MOBA` / `moba` 命名 — 见 §13 命名大小写规则
- ❌ **首次生产上线后不要做破坏不停服更新的改动**:日期与首次上线前整批重整口径见 `§3.1`；日期填写后不要给 Redis pb 存储改字段编号/类型/语义、不要在 read-modify-write 路径丢弃 unknown fields、不要设计「必须停服才能上线/读数据」的方案 — 见 §9 不变量 16/17、`docs/design/zero-downtime-update.md`
- ❌ **UE 侧不要再用 `Xuanming` / `Xm` 命名任何工程 / 模块 / 类 / 文件 — 一律 `Pandora`**(见 §11.1)
- ❌ **不要交半成品**(TODO 占位 / 空实现 / “先留个钩子以后接”)— 见 §14 接线完整性铁律

## 13. 命名大小写规则(强制)

- **Pandora**(首字母大写):仓库名 / 本地路径 / 工程类前缀 / 文档项目名引用 / **UE 工程 / 模块 / 类前缀**
- **pandora**(全小写):kafka topic / mysql / redis key / docker 镜像 / go module
- **`Pandora-Client`**(CapitalCase,带连字符):UE 客户端仓库名。⚠️ **不要和 JWT audience `pandora-client`(全小写)混淆** —— 后者是 envoy / login / auth 配置里的鉴权受众,改仓库名时**绝不能**动它
- **`Xuanming` / `Xm`**:**已废弃命名**,**代码 / 工程 / 类 / 模块一律不再使用**

## 14. 接线完整性铁律（强制）

新功能 / 新能力接线**一次做到最终可上线版本**，不准留半成品。

1. **不准留 TODO 占位 / 空实现 / “以后再接”**：要么不动，要动就把功能链路全部接完（static 与目标模式两条路径都可用）。
2. **允许用配置开关默认关闭新行为**（临时开关 / feature flag），但默认值必须保证现有行为不变 / 一键启动不坏；开关打开后的分支必须是**完整可用的真实实现**，不是空壳。
   - 例：snowflake nodeID 由 `snowflake.node_id_source` 开关驱动：`""`/`static` 静态（默认，单副本/dev），`etcd` 自动抢占（多副本）。两条路径均已在 `etcdnode.MustProvideSnowflake` 内完整实现（含失租 fencing 退出），各服务 main.go 一行接入。
3. **隔离重依赖的独立 pkg module（如 `pkg/snowflake/etcdnode` / `pkg/killswitch/etcdkv` / `pkg/cellroute/etcdtable`）被服务 import 后**：Claude 负责写代码 + 补 go.mod 的 require/replace；`go mod tidy` 拉依赖生成 go.sum 是 **Codex 的活**（AGENTS.md §11.1）。接线后必须在交接里列出“哪些服务需 tidy”，不准默声留着让下个 AI 碰到 build 红才发现。

## 15. 设计简单性与标准化（强制）

设计与实现必须优先选择**简单、标准、直达、可维护**的方案。在满足正确性、安全性、性能、数据一致性、不停服更新等硬约束的前提下，按以下顺序取舍：

1. **标准能力优先**：先用 Go / UE、Kratos、gRPC、Envoy、MySQL、Redis、Kafka、etcd、Kubernetes 等现有技术栈的官方能力和业界通行模式；已有成熟标准能解决时，不自研协议、框架或基础设施。
2. **最少复杂度优先**：能用一个清晰模块解决，不拆成多层；能同步完成，不引入异步链路；能用本地事务完成，不升级为分布式事务；能复用现有组件，不新增中间件。这里的选择必须同时满足既有架构边界和硬性不变量，不能以“简单”为由牺牲正确性。
3. **拒绝预设性复杂化**：不得只因“以后可能扩展”“看起来更高级”而提前增加接口层、适配层、工厂层、事件总线、状态机、缓存、分片、兼容层或配置开关。出现真实需求、压测数据、故障证据或明确扩容门槛后再引入。
4. **复杂度必须举证**：确需采用更复杂方案时，设计说明必须写清简单方案不满足的已确认约束、复杂方案新增的组件和失败模式、验证方法及回退路径；无法说明则采用更简单的标准方案。
5. **代码路径保持直达**：命名表达业务含义，控制流和调用链尽量短，避免无意义封装、重复抽象和层层转发。抽象必须消除真实重复、隔离明确变化或守住架构边界，不能只为形式上的“分层”。

简单不等于简陋，也不允许绕过 `CLAUDE.md §9` 不变量、测试、错误处理、可观测性或 `§14` 接线完整性。目标是在完整可靠的前提下，用最少必要机制解决当前已确认的问题。

## 16. 隐蔽 bug 与分布式正确性（强制）

设计、实现、测试和 review 不能只覆盖正常路径，必须以“故障一定会发生、请求一定会重复、消息可能乱序、多副本会并发”为前提，主动寻找不易复现的边界 bug。目标是交付时没有已知 bug；任何未验证路径不得凭感觉声称“无 bug”或“分布式安全”。

1. **并发与原子性**：检查竞态、丢失更新、重复执行、ABA、检查后执行（TOCTOU）、锁过期和跨 Redis slot / 跨库非原子问题。共享写必须明确事务或原子边界；锁只能降低冲突时，最终正确性仍须由数据库条件更新、唯一键、CAS、Lua 或等价权威机制保证。
2. **幂等与顺序**：所有可能重试的写入、消息消费、回调和补偿必须有稳定幂等键；明确同一实体的顺序保证、版本号 / revision / fence 校验及迟到消息处理。不能假设 RPC、Kafka、定时任务或客户端回调只执行一次、严格有序或永不丢失。
3. **超时与部分失败**：逐项考虑请求已生效但响应丢失、跨服务只完成一半、依赖超时后迟到成功、补偿本身失败等情况；明确重试上限、退避、可恢复状态、补偿闭环和人工审计入口，禁止静默吞错或留下永久中间态。
4. **多副本与故障恢复**：覆盖进程崩溃、重启、主从切换、网络分区、租约丢失、时钟偏差、多实例重复调度和滚动升级中新旧版本共存。进程内状态不能被当作唯一权威；失去租约 / ownership 后必须 fencing，旧实例不得继续写。
5. **容量与退化边界**：检查空值、零值、最大值、溢出 / 下溢、分页边界、集合硬上限、队列积压、缓存击穿 / 雪崩、热点 key 和依赖不可用时的行为。降级不得破坏数据正确性、安全边界或 `§9` 不变量。
6. **验证必须对应风险**：每个已识别风险至少有一种可复现验证：单元测试、并发测试、race 检测、集成测试、故障注入、重启恢复测试或端到端测试。修 bug 必须补能在修复前失败、修复后通过的针对性回归测试；无法本地验证的路径必须在交接中明确列为剩余风险和人工验收项。
7. **Go 请求 context 与 transport 生命周期必须隔离**：gRPC / HTTP handler 的请求 `ctx` 含有 Kratos server transport、`metadata.MD`、鉴权头和其他请求级 Value，不得逃逸到 handler 生命周期之外。
   - `context.WithoutCancel(ctx)` 只移除取消信号和 deadline，仍完整保留 Value；禁止用它把请求 `ctx` 传入 goroutine、异步 fan-out、队列、缓存或长生命周期对象后再调用下游 gRPC。
   - fire-and-forget 必须从 `context.Background()` 或项目统一的 detached helper 派生，只白名单复制 trace / player / match / team 等不可变日志字段；业务参数、下游凭证和必要身份必须使用有类型的参数显式传递。捕获的切片、map、指针等可变值必须复制或明确同步，并设置有界 timeout 和 `cancel`。
   - server / client 共用 middleware 必须按**本次调用方向**互斥处理；上下文同时存在两种 transport 时，client interceptor 只能修改本次 client `RequestHeader`，绝不能修改继承的 server `ReplyHeader`。Metrics / Logging / Trace 等都不得用“只要存在 server transport 就算 server 调用”的优先级猜测方向。
   - review 必须追踪完整 context 来源、Value、异步边界和下游调用链。涉及异步下游 RPC 时，必须补“同一 context 同时含 server / client transport”的回归测试，以及真实 handler 返回与后台 RPC 并发的集成测试；`go test -race` 必须在支持 CGO 的 Linux / CI 环境执行，无法执行时明确阻断项，不得写成已验证。
8. **关键 writer 的租约与重启时间预算必须闭合**：使用 Pod UID / instance identity 注册 capability、writer fence 或 activation lock 时，必须覆盖 runtime fatal、OOM、SIGKILL 等无法执行 `defer Close()` 的退出。设计和故障测试必须把旧 lease TTL、容器启动、CrashLoopBackOff、重新抢占、下游本地自 fencing、heartbeat timeout 和启动 sweep 放在同一时间轴验证；恢复最坏耗时不得吃光业务安全租约。不得通过忽略重复 key、无条件覆盖或删除 fencing 来缩短恢复时间，必须在保持单 writer / 防脑裂的前提下设计安全接管。
9. **崩溃与 P0 必须建立可关闭的事故档案**：确认 runtime fatal、未恢复 panic、OOM/CrashLoop/非预期退出或任何 P0 后，必须在同一任务按 [`docs/incidents/template.md`](./docs/incidents/template.md) 创建/更新事故文档并登记 [`docs/incidents/index.md`](./docs/incidents/index.md)。先记录最小事实，未知根因明确写未知；后续持续补齐脱敏证据、UTC 时间线、调用链和变量生命周期、根因/放大因素、全仓同类扫描、修复 commit/镜像、部署/回滚、race/集成/故障注入、观察窗口与剩余风险。只有模板关闭清单全部满足才能标记已关闭，不能把在途 diff、普通测试或服务重新 Ready 包装成事故已修复。

10. **禁止用定时器掩盖时序问题（强制，与客户端 `F:\work\CLAUDE.md` 同源）**：Timer、Tick、Delay、`time.Sleep`、固定间隔轮询都不得用来掩盖生命周期、依赖就绪、组件创建或配置注入的顺序问题。依赖未就绪时必须修正创建 / 复制 / 回调 / 事件注册链，并用带完整上下文的错误日志或开发期断言暴露违反时序约束的入口；不得靠等一会儿碰运气。

    **判别标准（禁止项 vs 强制项，不准互相冒充）**：同样是"到时间做点什么"，两类性质相反 ——

    | | ❌ 禁止：掩盖时序 | ✅ 强制：阶段 deadline 兜底 |
    |---|---|---|
    | 意图 | 依赖**本可确定**先就绪，却用等待碰运气 | 依赖来自外部系统，完成事件**可能永不到达** |
    | 到期动作 | 假设已就绪并继续往下走 | 重查权威并重试，或 fail-closed / 进入可见可重试形态 |
    | 正确性 | 机器慢、网络抖就错 | 与时间无关，只把无界等待收敛成有界 |
    | 典型 | `sleep` 后就认为下游已注册、轮询"配置加载好了没" | `§9.19` 点名的 `ClientTravel` / 地图加载 / Admission ACK / 旧连接清退每阶段有界；HTTP/gRPC unary 的有界超时 |

    判别口诀：**到期后"假设成功"的是掩盖，到期后"重查权威"的是兜底。**

    落地要求：①**先修根因再谈兜底** —— 能通过修正调用链让事件正常到达的必须先修，只加超时不修根因就是掩盖；②超时 / 退避 / 重试必须有明确上限、可取消、可观测，并复用既有循环或 ticker，**不得为此新建第二套 timer 状态机或后台 goroutine**（`§15.2`）；③取值必须能说明推导依据（最坏合法到达时间），拿不到证据时取保守偏大值并在注释标注"待实测复核"，不得拍一个数字当已知；④**调度用的退避 / 节流状态不得写进权威或派生存储**，尤其不得改写带既定语义的字段（反例：INC-20260724-001 曾把 ds_allocator sweep 的队头退避写进 active ZSET score，而该 score 语义是 `last_heartbeat_ms`、且 `score==0` 是 abort fence / auth quarantine 要求"下一轮立即对账"的哨兵，索引重建专门用 `ZADD NX` 保护它 —— 正确做法是记在进程内并标注为非权威调度提示，重启即清空）。

分布式正确性不能靠增加层数或复杂框架自动获得。仍须遵守 `§15`：优先用最简单的标准机制建立可证明的权威、原子、幂等、fencing 和恢复闭环。

## 17. 进入场景 / 副本必须单一接口，差异表驱动（强制）

「进入一个可玩场景」（大厅、PVP 对局、PVE 副本、单人副本、未来的活动 / 挑战 / 秘境）**只允许有一个进入接口**。不同场景的人数、条件、带入数据、DS 逻辑差异，一律表达为**配置表数据**与**服务端按表判定**，不得表达为新的 RPC、新的字段分支或客户端分支。

现状基线（改动前必须先确认这些仍成立，不要重复建设）：
- 进入接口 = `MatchService.StartMatch(team_id, map_id)`；单人副本与 5v5 走同一条链，仅 `team_size=1`、`game_mode` 分池（`pvp` / `pve_coop`）。
- 每场景差异已表驱动：关卡表 `g_关卡.xlsx` → `configtable` level 表 / 客户端 `FCfgLevel`，现有列含 `LevelAsset`、`GameModeClass`（每副本可换 DS GameMode）、`Category`、`TeamSize`、`AllowExit`、`ShowInMatchList`。
- `matchmaker.teamSizeForMap(map_id)` 按表读并钳到 `[1, MaxLevelTeamSize]`，表缺失回退全局默认。
- `map_id` 全链透传：ticket / match 记录 → `MatchProgress` → 客户端预置关卡上下文 → `AllocateBattle` → Agones label `pandora.dev/map-id` → DS。

三条强制规矩：

1. **差异进表，不进接口签名。** 判定标准：**新增一个副本如果需要改 proto，就是走错了；只应该改表**。禁止出现按场景命名的 RPC（`EnterTowerDungeon` / `EnterTimeTrial` / `StartPveXxx`）、按场景分叉的 `if map_id == N` 业务逻辑、以及"某副本专用"的请求 message。确需新增一类差异时，加**关卡表的一列**（或与关卡表按 `map_id` 关联的子表），由服务端按 `map_id` 读表判定；新副本从此只是新增一行。

2. **客户端只传"选择"，不传"权威数据"。** 请求里允许携带玩家的**选择**（难度档位、使用哪张门票的道具配置 ID、带哪个助战、编成槽位），这些只是意图；**带入的实际数值、道具数量、属性加成、进度快照一律由服务端从权威源组装**（`§9.6`）。任何"客户端上报本次带入 X"的设计一律拒。带入数据经既有通道下发 DS（票据 claims / match 记录 / DS 主动查后端），不新开客户端→DS 直传通道。

3. **准入条件只有服务端一份权威判定。** 条件（等级下限、前置关卡、门票 / 消耗品、每日次数、CD、编成合法性）必须在服务端进入路径上 fail-closed 校验，读表判定，失败返回明确业务错误码。客户端可以拉取同一份条件用于灰化按钮与提示文案，但那只是展示，**不得实现第二份判定逻辑**，更不得因本地判定通过就跳过服务端校验。条件查询失败 = `UNKNOWN`，按 `§9.22` fail-closed，不得默认放行。

配套约束：
- **消耗型条件必须与进入同生共死**：扣门票 / 扣次数与"真正进入成功"必须绑定同一个 `§9.23` 稳定 `operation_id`，用既有托管 / 流水 / CAS 模式做到要么都成、要么都不成；进入失败或 DS 分配失败必须精确回滚**本次未交付**的扣减（验收底线第 4 条：只撤销未交付的写，绝不回退玩家已获得的东西）。禁止"先扣后进、进不去靠人工补"。
- **proto 演进阶段先按 `§3.1` 判定**：首次生产上线日期填写后按 `§9.21`，进入请求新增参数只能加字段 / 加 enum 值，禁止改编号、类型、语义；日期未填写时可随整批刷新做 breaking，但“差异进表、不进接口签名”与简单性要求不因此失效。用 `oneof` 承载分场景参数前必须先证明"加表列解决不了"，否则按 `§15.3` 拒绝——不得因"以后可能扩展"提前搭参数框架、规则引擎或适配层。
- **关卡表是跨仓共享事实**：DS 侧只有自己那份表，"同 `map_id` 在两仓指向不同地图"的漂移无法自检（历史事故：松林镇 4002 镜像关卡表漂移）。改表必须双仓同步发布，并遵守 `§9.15` 配置表热更流水线。

客户端侧对应条款见 `Pandora-Client-SVN/CLAUDE.md「进入场景 / 副本的客户端纪律」`。

## Agent skills

### Issue tracker

问题与规格记录在 `luyuan-go/XuanMing-Server` 的 GitHub Issues 中。详见 `docs/agents/issue-tracker.md`。

### Triage labels

问题分流使用 mattpocock/skills 的默认标签词汇。详见 `docs/agents/triage-labels.md`。

### Domain docs

本仓库使用单上下文领域文档布局。详见 `docs/agents/domain.md`。
