# 玩家重要消息：持久收件箱、拉取恢复与迁移窗口

> 记录日期：2026-10-14。用途：保存用户提供的大厂实践调研及仓库对照，供后续设计与验收引用。
>
> **记录边界**：下文行业材料及 C++ 仓库结论来自用户在本次会话提供的调研快照。原文称公开资料已回源核对；本次归档没有重新逐条复核全部外部资料。具体保留时间、重试次数、框架默认值与生产建议受版本和配置影响，不能直接作为本项目配置或验收事实。BigWorld、EVE、魔兽及国内中心服路线在原文中仍待补查，本页不声称已完成这些游戏案例的核实。
>
> **本仓库状态**：本文只记录调研和静态核对，不改业务实现、不更改现有架构决策，也不表示编译、测试、部署或端到端验收已通过。已有契约仍以 [协议顺序规则](protocol-ordering-rules.md)、[Owner Authority](owner-authority.md)、[邮件设计](mail.md)及各服务代码为准。

## 1. 核心结论

**重要消息存进按收件人身份组织的持久收件箱，接收方准备好之后按自己的进度去拉；推送只起提醒作用，丢了也不要紧。**

这句话是用户针对玩家跨节点迁移提出、供本仓库借鉴的设计建议，概括的是“持久事实与消费进度脱离临时位置”。它不是所有公司的唯一共同实现：Iris 可以向消费者推送实际更新，Temporal Signal 自身就是持久输入，不只是提醒。各例子支持不同环节，不能据此认定所有框架都只推提醒、只靠客户端拉取。

玩家从 A 迁到 B、再迁到 C，重要消息的持久归属始终是稳定的 `player_id`，不用让投递器追着位置变化搬消息。目标节点完成接管与 Admission 后，再按业务游标或幂等键消费；节点通知只改善及时性。业务库、事务 outbox、邮件表或专门的更新流都可能承载这一职责，不要求所有业务再复制进一张通用队列表。

“推送丢了也不要紧”的前提是权威数据已持久化、恢复入口持续可用，且接收端确实接上了拉取与失败恢复。对于驱动进场、换场景、匹配 READY 等会让玩家停住的状态，仍必须遵守本仓库已有的有界驱动与 watchdog 契约，见 [协议顺序规则 §12](protocol-ordering-rules.md)。

## 2. 用户调研记录：六条共同做法

### 2.1 收件箱按人存，不按位置存

- Meta Messenger 的 Iris：按用户全序排列的更新队列；手机端、存储层等消费者各有独立指针，成功消费后才推进。手机离线时指针停住，新消息仍入队。[2014 年官方文章](https://engineering.fb.com/2014/10/09/production-engineering/building-mobile-first-infrastructure-for-messenger/)描述这一结构；它是实际更新流，不限于提醒帧。
- Telegram：消息箱有递增的 `pts`，接收方据此维护进度。
- Temporal：signal 按 Workflow Id 写入持久历史，发送方不需要知道 worker 的物理位置；它本身就是异步持久消息。[官方消息传递文档](https://docs.temporal.io/develop/go/workflows/message-passing)支持该语义，不能把它概括为不可靠通知。

### 2.2 推送用于提醒，准确数据靠接收方按进度拉

- Telegram：实时更新不是唯一事实源；启动、发现序号断档或长时间无更新时用 `getDifference` 补齐。
- Pusher、Roblox：原文记录其推送不保证必达；重要数据由应用持久化，重连后回源。
- Riot Messaging Service：原文记录消息主要承担通知，收到后回业务服务读状态；断线消息只在内存短暂缓存，超窗要求重新同步。

### 2.3 去重靠序号或幂等键，确认在持久化之后

- Telegram：`local_pts + pts_count == pts` 才应用；结果较小时按重复处理，较大时补缺失更新。微信 seqsvr 的用户序号单调递增、允许跳号，适合做版本水位，不能直接当连续日志下标。
- 去重生命周期取决于产品，不能一概而论。原文将 Telegram `random_id` 与 Stripe Idempotency-Key 一并概括为有限窗口；本次公共来源核对时，[Telegram 当前官方更新文档](https://core.telegram.org/api/updates)明确其服务端 `random_id` 去重记录不失效，因此订正该泛化。有限窗口的论点仍适用于 Stripe（原文记录至少保留 24 小时）和下面的 SQS FIFO，不能套到 Telegram。
- AWS outbox 示例可用 SQS FIFO 去重，但原文记录窗口只有 5 分钟，接收端仍须持久去重。不能把中间件的短期去重当永久业务幂等。
- Cloudflare Durable Objects：[官方术语表](https://developers.cloudflare.com/durable-objects/reference/glossary/)的输出门说明会在写入确认持久化前扣住对外消息及回包。这里借鉴的是“先落权威结果，再确认”，不能据此推导所有 Redis/MySQL 配置都已提供同样的持久性。

### 2.4 旧节点能不能写，由存储在提交时判断

- Orleans 的位置目录默认最终一致，集群不稳定时可能出现同实体多实例；Akka 单写者也依赖正确的脑裂处理。不能仅凭路由或实例数量证明只有一个有效写者。
- Martin Kleppmann 讨论的 fencing token、Google Chubby sequencer、Kafka producer epoch 都体现同一原则：新任期生效后，存储或资源端拒绝旧任期写入。[Kafka 4.3 生产者文档](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)中的这一保证针对使用同一个 `transactional.id` 的事务生产者：新实例初始化事务后可 fence 旧实例；不能扩展为任意生产者重启都会拒绝旧实例。
- 流程约束也重要。原文记录微信 seqsvr 旧节点连续 N 秒读不到配置即停服、新节点等待 N 秒后接管；Discord 在 2026 年 3 月语音故障后加入交接准入检查，未完成交接不得缩容。后一个事故时间与措施仍按原调研快照保存，未在本次归档独立复核。

### 2.5 每条链路有上限，超限有明确退路

- 保留期：原文记录 WhatsApp 未送达消息最多 30 天。Iris“约一周”来自 [2014 年文章](https://engineering.fb.com/2014/10/09/production-engineering/building-mobile-first-infrastructure-for-messenger/)的历史实现参数；[2018 年存储迁移文章](https://engineering.fb.com/2018/06/26/core-infra/migrating-messenger-storage-to-optimize-performance/)描述保留数周，不能将“一周”当成其当前固定承诺。
- 重连续传窗口：原文记录 Ably 约 2 分钟，Discord 为几分钟。
- 超窗恢复：Discord Invalid Session 后重新 Identify；Telegram `differenceTooLong` 后重新获取对应消息箱状态；FCM 离线待发超过 100 条时清空并通知应用全量同步；LINE 落后过多走全量同步，并定期交换摘要对账。

以上数字是行业调研快照，不是本项目保留期。完整的业务契约还应明确最大未决条数、单次拉取条数、总字节数、重试期限、归档/补偿、告警和对账方式。资产未完成操作不能因投递窗到期就静默丢弃。

### 2.6 消息分级，瞬时通知与场景事件不进入可靠业务链

- Slack 的“正在输入”不写库。
- Xbox RTA 在进入场景时订阅、离开时退订；退订后迟到更新可丢弃，重连靠订阅时返回当前值恢复，不要求补全部历史。
- 本项目应继续区分业务权威变更、可重建状态通知、聊天历史与战斗内复制事件；不要把伤害、读条等瞬时场景事件塞进通用持久收件箱。

## 3. 迁移窗口里的在途消息

用户调研把迁移时的快速投递路径概括为三种。下表讨论位置切换时的拒收、有限转发与默认内存缓冲；它们不能代替业务持久化，也不能用来断言框架的全部可靠投递能力都只在内存里。

| 做法 | 原文代表 | 原文记录的边界 |
|---|---|---|
| 拒收，发送方刷新位置后重试 | Dapr、Service Fabric | Service Fabric 为尽力而为，可能重复 |
| 旧节点有限次数转发 | Orleans | 当前 [SiloMessagingOptions 源码](https://github.com/dotnet/orleans/blob/main/src/Orleans.Runtime/Configuration/Options/SiloMessagingOptions.cs)默认最多转发 2 次，超限失败返回调用方；该值可配置 |
| 路由层缓冲 | Akka Cluster Sharding | [官方 sharding 文档](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html)默认缓冲上限为**每个 ShardRegion 10 万条**，满后丢弃；交接时可能主动丢弃部分缓冲以避免乱序 |

Orleans 的 [官方投递保证](https://learn.microsoft.com/en-us/dotnet/orleans/implementation/messaging-delivery-guarantees)说明默认 at-most-once；开启重试后可能多次到达，框架不替应用做持久业务去重。[Akka 当前官方说明（2.10.24）](https://doc.akka.io/libraries/akka-core/current/typed/reliable-delivery.html)在本次公共来源核对时仍保留 Reliable Delivery 生产使用警告，但该模块可使用 `DurableProducerQueue` 持久化生产者队列；这与上表 sharding 默认内存缓冲是不同层，不能混写成“都在内存里、都可能丢”。**其他参数仍是原调研的框架版本结论，实施前须查对应版本文档与配置**，不能扩展成所有版本、所有可选模块的一致保证。

## 4. 与上一轮方案的三点差别

原方案已经包含稳定 `player_id` 寻址、持久待办、幂等去重、归属代际和消息分级，主要需要调整以下重心：

1. **从追位置推送转为持久数据不动、接管后拉取**：投递器“查位置 → 推送 → 被拒 → 刷新 → 重试”可改善延迟，但不应成为重要消息唯一恢复链；C 接管后按进度读就能恢复。
2. **补超窗退路与对账**：保留期到期、待办超限、增量不闭合都需要明确结果；能够回源重建的拉快照，需要保留业务事实的走归档/补偿，并能发现长期不一致。
3. **代际与交接流程一起约束**：提交时 fencing 是正确性防线；接管前不得开放业务写、交接未完成不得贸然下线等生命周期约束则减少卡住、双活和丢失窗口。流程不能替代 fencing。

## 5. 原 C++ 仓库对照快照

本节保留用户原文的来源结论，**所列 `cpp/`、`go/shared/assetop` 和 Unity 路径属于原 C++ 项目，均不是本 Go 仓库的本地路径**。原文行号是当时工作区定位，后续以符号和实际版本为准。原设计文档对多数代码还标注未编译/未测试，本页不提升其验收状态。

原文列出的已有能力：

- 资产通道 `go/shared/assetop` + C++ `PlayerAssetOpSystem`：业务变更与待发记录同事务；`(stream, epoch, seq)` 加有界滑动窗口去重；目标返回不在此处/稍后重试时退避重投；结果持久化后完成。
- 战斗结算 `cpp/libs/services/battle/settlement/settlement_outbox.h`：先存 Redis 再投递；每次重投查玩家位置；场景按 `battle_id` 条件删除，相当于确认；进场钩子补发。
- 归属围栏：`owner_epoch`、存盘 Lua 条件写及 `go/db` epoch 守卫。
- 分级：社交推送一次，客户端拉权威列表；交接中的技能伤害直接丢弃。

原文指出六项缺口：

| # | 原 C++ 项目问题 | 原文定位与优先级 |
|---|---|---|
| 1 | “提醒 + 落地后拉取”只做一半 | `docs/design/cross-zone-scene-travel.md:94` 记录跨 zone 最坏约 60 秒推送会丢且不补发；Unity 每次落地回源尚未实现。优先补拉取闭环 |
| 2 | MySQL 没有行内围栏 | `docs/design/db-write-fencing-mysql.md` 当时为待授权设计；Redis 持久确认不等于 MySQL 能拒绝旧数据。优先补提交时 fencing |
| 3 | 进场入口缺 epoch 校验 | `cpp/nodes/scene/handler/rpc/scene_handler.cpp:178` 的原注释记录缺口。优先补接管准入 |
| 4 | 通用玩家 RPC 静默丢消息 | 同文件 `SendMessageToPlayer:290` 找不到人只打日志；`MESSAGE_PRIORITY_IMPORTANT` 只影响日志级别。重要消息不能靠该接口保证投递 |
| 5 | 邮件仅有设计 | 原仓库不存在 `go/mail`；其业务库即收件箱、带幂等键的方向可复用 |
| 6 | 缺少超窗退路与对账 | 资产待办长期离线持续挂起，仅有未决条数上限阻挡新操作 |

原建议：**不再新建一个与资产通道、结算 outbox 重复的通用投递器，优先补 1–3，再明确 4–6 的边界和后续验收。** 这是原项目的优先级，不直接等同于本仓库仍缺同一份实现。

## 6. 本 Go 仓库落点与实现状态

2026-10-14 对本工作区做静态核对，得到下表。它只表明设计/代码存在，不证明部署配置、故障窗口或客户端行为已实测闭环。

| 能力 | 本仓库可核对落点 | 状态与边界 |
|---|---|---|
| 按玩家持久缓冲与游标拉取 | [push README](../../services/runtime/push/README.md)、[offline.go](../../services/runtime/push/internal/data/offline.go) 的 `offlineKey` / `AssignAndBuffer` / `Range` | Redis key 按 `player_id` 组织；Lua 原子分配单调游标、入队、窗口/数量修剪并记录丢失水位；进程连接管理只作唤醒。本缓冲用于有限续传，不替代资产/邮件的业务权威库 |
| 超窗信号与回源契约 | [push.go](../../services/runtime/push/internal/biz/push.go) 的 `drainBuffer`、[协议顺序规则 §12](protocol-ordering-rules.md) | 默认 5 分钟、512 帧；发送幸存帧前预检缺口，拉空后终检；合成 `pandora.push.resync` 不推进游标。首连要求先订阅后拉快照。已有 2026-08-05 客户端域矩阵；该矩阵仍标注静态核查、未编译/真机验证，不据此声称所有消费域均验收 |
| 邮件业务库充当收件箱 | [mail README](../../services/social/mail/README.md)、[mail_repo.go](../../services/social/mail/internal/data/mail_repo.go)、[mail.go](../../services/social/mail/internal/biz/mail.go) 的 `ListMail` / `ClaimMail` | 服务已存在：个人邮件写扩散；系统/公会 channel + watermark；个人邮件分页；领取先幂等发资产，再记 claim。DS 领取意图另有稳定 `claim_key` 与 journal 确认路径，旧直连和意图路径互斥。不能照搬“mail 未写”结论 |
| 业务变更与 outbox 同事务 | [battle_repo.go](../../services/battle/battle_result/internal/data/battle_repo.go) 的 `SaveResult` / `ApplyProgress`、[battle_result README](../../services/battle/battle_result/README.md) | 对局、战绩与下游出箱原子提交，进度水位与进度出箱同事务；资产发放由 inventory/mail 等现有领域接口负责。无须再复制一份通用可靠投递账本 |
| 归属与 Admission 屏障 | [owner-authority.md](owner-authority.md)、[owner_repo.go](../../services/runtime/owner/internal/data/owner_repo.go) | `owner_record`、实例 lease、`admit_not_before` 在同一 SQL 事务域；epoch/exact target/phase/lease 约束接管。现有设计仍标为 migrate，生产滚动兼容与旧门退役未完成，不声明全链路已上线 |
| SQL 层旧任期防线 | [bag_repo.go](../../services/economy/inventory/internal/data/bag_repo.go) 的 `lockBagMetaTx`、[owner_authorizer.go](../../services/economy/inventory/internal/data/owner_authorizer.go) | 背包事务先锁 `bag_meta`，拒绝低于存量的 epoch；授权还检查当前 owner、实例身份、ADMITTED、lease。只能据此说明该路径已有防线，不能推广为所有 MySQL 写路径均已完成围栏 |
| 容量、保留、归档与检查 | [邮件 sweep](../../services/social/mail/internal/biz/sweep.go)、[mail 配置](../../services/social/mail/etc/mail-dev.yaml)、[dbcheck](../../tools/migrate/cmd/dbcheck/main.go) | 邮箱默认上限 200，满后驱逐已领仍满则报错；邮件有效期/归档/领取记录有界；dbcheck 登记并检查 outbox 堆积。不同领域清理语义不同，仍遵守 `CLAUDE.md §9.24` 的保留及默认报告约束；不能表述为“整个仓库没有超窗退路/对账” |

后续在本仓库推进时，优先核实**现有消费域是否真正完成回源恢复、重要业务提交点是否有存储侧拒旧写、接管后开放业务是否受 owner/lease/Admission 共同约束**。对长期待发资产，还需按领域明确重试寿命与收据保留关系；不能为了清积压先删幂等收据，再让旧 outbox 重放。

## 7. 后续验收口径

本次没有实施下列检查；后续相关实现与评审按现有领域流程补齐：

1. 玩家 A→B→C 连续迁移、提醒全丢、落地后重启：从持久业务记录按进度恢复，重复拉取不重复发奖。
2. 新 owner 已接管，旧节点迟到写、迟到确认、迟到 Logout：在权威提交点拒绝或只精确清理本任期残留。
3. 事务提交成功但回包丢失、业务落库后发布进程崩溃：使用相同幂等键恢复，确认不早于相应权威提交。
4. 超保留窗、超条数、坏帧、增量缺口：给出明确 resync/归档/补偿/业务错误；有界恢复，不静默丢资产、不永久等待。
5. 定期比较权威结果、游标、未决 outbox 与业务收据，记录最老待办年龄及数量告警；仅有总量上限不等于完成对账。
6. 验证范围包含服务端、UE 客户端/DS 与实际配置。原 C++/Unity 项目的测试状态不能充当本 Go/UE 项目的验收证据。

## 8. 本次补核的公共来源

以下是本次归档中补充核对的来源及适用范围；链接和当前页内容核对时间为 2026-10-14。未列入表格的行业案例与参数仍按用户调研快照保存，不声称本次已独立核实。

| 来源 | 支持的结论及限制 |
|---|---|
| [Meta：Messenger 移动基础设施（2014）](https://engineering.fb.com/2014/10/09/production-engineering/building-mobile-first-infrastructure-for-messenger/) | 按用户的更新队列、消费者独立指针及当时保留期；不是“只推提醒”的证据 |
| [Meta：Messenger 存储迁移（2018）](https://engineering.fb.com/2018/06/26/core-infra/migrating-messenger-storage-to-optimize-performance/) | 队列保留数周，说明 2014“一周”不能当当前不变参数 |
| [Telegram：更新同步](https://core.telegram.org/api/updates) | `pts`、缺口恢复与差量同步；明确 `random_id` 服务端去重记录不失效，订正原文的有限窗口泛化 |
| [Temporal：Go 消息传递](https://docs.temporal.io/develop/go/workflows/message-passing) | Signal 的异步持久消息及 Workflow 寻址语义 |
| [Orleans：投递保证](https://learn.microsoft.com/en-us/dotnet/orleans/implementation/messaging-delivery-guarantees)、[默认配置源码](https://github.com/dotnet/orleans/blob/main/src/Orleans.Runtime/Configuration/Options/SiloMessagingOptions.cs) | 默认至多一次、可选重试不等于业务去重；默认最大转发 2 次可配置，main 分支源码随版本变化 |
| [Akka：Cluster Sharding](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html)、[Reliable Delivery](https://doc.akka.io/libraries/akka-core/current/typed/reliable-delivery.html) | 10 万条是每 ShardRegion 的默认缓冲上限；Reliable Delivery 的持久生产者队列与 sharding 默认缓冲不同；保留当前文档生产使用警告 |
| [Cloudflare Durable Objects：术语表](https://developers.cloudflare.com/durable-objects/reference/glossary/) | output gate 在存储写入持久化前阻止对外消息 |
| [Kafka 4.3：KafkaProducer](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html) | 同 `transactional.id` 的新事务生产者可 fence 旧实例；不能泛化为全部生产者重启语义 |
