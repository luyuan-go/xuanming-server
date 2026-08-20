# 崩溃与 P0 事故档案

本目录是 Pandora 服务端统一事故事实源，记录“具体一次事故发生了什么、为什么发生、如何恢复、修复是否真正上线并验证”。稳定架构规则写入 `docs/design/`，取证与处置方法写入 `docs/ops/`，静态审核与交接写入 `docs/reviews/`；这些文档可以链接事故档案，但不能替代事故档案。

从 `INC-20260721-001` 开始执行本规范。历史上散落在设计文档或审核文档中的事故说明暂不批量迁移，避免链接漂移；后续触及时可补索引或按新模板回填。

## 1. 强制建档范围

下列任一条件成立，必须在发现当次从 [`template.md`](template.md) 复制新文档并登记到本页：

- 任意 Go 服务、UE DS、sidecar 或关键工具发生 runtime fatal、未恢复 panic、SIGSEGV、abort/assert、OOMKilled、CrashLoopBackOff、非预期进程退出或无法解释的容器重启。
- 任意故障导致玩家掉线、被踢、无法登录/匹配/进场、永久中间态、数据错误/丢失、安全边界突破、脑裂、关键服务大面积不可用，并判定为 P0。
- 上线前发现但若上线会造成上述 P0 后果的问题，按 `near-miss` 建档；必须明确“未在线上发生”，不得伪装成线上事故。
- 一次事故即使由多个 bug 共同造成，也使用一个 Incident ID；独立时间、独立影响或独立根因的事件分别建档并互相链接。

这里的 `P0` 指事故严重级别，不是 `stress-p0` 压测阶段、开发优先级或普通代码审查标签。未形成事故/near-miss 的低风险静态问题继续记录在 `docs/reviews/`。

## 2. 命名与状态

- Incident ID：`INC-YYYYMMDD-NNN`，创建后永久不变。
- 文件名：`YYYY-MM-DD-p0-<service>-<short-slug>.md`；同日同服务多起事故时增加两位序号。
- 时间线以 UTC 为主；同时引用客户端或本地时间时必须写明时区和换算关系。
- 文档只能追加更正和状态变化，不得删除已确认的原始时间线、旧结论或失败验证；结论被推翻时写明证据和更正日期。

统一状态：

| 状态 | 含义 |
|---|---|
| 调查中 | 已确认事故/P0，但根因尚未闭合 |
| 根因确认 | 直接根因、触发条件和放大因素均有证据 |
| 已止血 | 当前影响停止，但永久修复尚未完成 |
| 已修复待部署 | 代码/配置已完成并验证，目标环境尚未加载新产物 |
| 已部署待验证 | 新产物已加载，仍需故障路径和观察窗口验证 |
| 已关闭 | 关闭门槛全部满足，且没有未披露的 P0 剩余项 |

## 3. 建档与关闭规则

事故一经确认，先填写最小事实：Incident ID、严重级别、类型、状态、环境、发现时间、受影响服务/版本、玩家影响、第一现场证据位置和当前负责人。根因尚不明确时必须写“未知”，不能用猜测填满文档。

每篇事故文档必须持续补齐：

- 原始症状与无关噪声排除；
- 完整时间线、调用链、关键变量及所有权/生命周期；
- 直接根因、必要触发条件、故障放大因素，以及 Recovery/重试/租约为何没有挡住；
- 全仓同类模式扫描范围、命中项、排除项和未扫描边界；
- 临时止血、永久修复、测试、部署、回滚、故障注入、观察窗口及剩余风险；
- 修复 commit、镜像 digest、Pod/GameServer UID、部署时间等可追溯产物证据。

P0 不能仅凭“代码已改”“普通单测通过”或“服务重新 Ready”关闭。至少需要：

1. 修复前失败、修复后通过的针对性回归；
2. 并发问题在支持环境完成 `go test -race`，或明确记录阻断且状态不得关闭；
3. 对应的集成/故障注入验证，覆盖进程 fatal、OOM、SIGKILL、重启或该事故的真实失败模式；
4. 目标环境实际加载的新 commit/镜像 digest/配置可追溯；
5. 同类代码扫描完成，相关 P0 一并修复或另建 Incident；
6. 观察窗口无复发，玩家路径和补偿/恢复路径均验证；
7. 所有未完成项都有负责人、状态和后续 Incident/任务链接，不把风险包装成已关闭。

## 4. 证据与脱敏

- 可以记录 trace ID、match ID、Pod/GameServer 名称和 UID，但不得写入 JWT、DSTicket、密码、私钥、Secret 原值、Authorization header 或完整个人数据。
- 含凭证的原始日志只记录受控存储位置、时间范围和哈希/查询条件；事故文档只摘录完成根因证明所需的脱敏片段。
- 日志时钟、集群时钟和客户端时钟不一致时，必须明确时区，不能按肉眼相近直接拼时间线。

## 5. 事故索引

| Incident ID | 日期 | 严重级别 | 类型 | 状态 | 服务/主题 | 文档 |
|---|---|---|---|---|---|---|
| INC-20260820-001 | 2026-08-20 | P0 | availability | 根因确认，永久修复与玩家 E2E 进行中（未关闭） | **策划一键启动假成功**：CMD 成功/失败共用标准 `pause`，完整本机启动缺少 login/Envoy/Hub 最终可玩门；06:25 两次 Login 均收到 HTTP 503，而 login 到 06:27:50 才由后续独立编排监听。同步修复逐服务 Go/策划表变化选择性重启与重启电脑收据复用；必须真实登录进 Hub 后才能关闭 | [事故报告](2026-08-20-p0-planner-oneclick-false-playable.md) |
| INC-20260818-003 | 2026-08-18 | P1 | release-safety / backward-compatibility / split-brain | 协议已落码，等待分阶段发布与生产演练（未关闭） | **Hub assignment 缺来源版本**：全新版本的 guarded retry 能收敛，但旧 binary 的 sign-before-CAS + blind retry 在滚动混版时仍可用两个在途请求打穿有限轮 guard。现有 UUID、墙钟、auth 代际、writer token、owner epoch 均不能充当 per-assignment 持久单调版本。**已落码**：`source revision`（写者任期高位 + 任期内序号低位，`pkg/placement`）+ `owner_record.hub_source_revision` 每玩家高水位 + 行锁事务内判定矩阵 + `ERR_OWNER_SOURCE_REVISION_STALE`(15006)。**未完成**：阶段 3~5 滚动兼容矩阵与生产 RollingUpdate 演练 | [事故报告](2026-08-18-p1-hub-assignment-source-revision-rollout.md) |
| INC-20260818-002 | 2026-08-18 | P1 | availability / distributed-commit uncertainty | 安全缓解已部署本机，持久自动恢复待实施（未关闭） | **Battle Owner Begin 不确定提交**：Owner 可能已 commit 但 gRPC 回包丢失；旧 DS Allocator 会 cleanup Pod，留下 Owner 指向死实例。已加显式 operation provenance + 独立 read-back，仍 unknown 时保留 allocation/Pod 且不交付 READY；claim loser 只读 exact 恢复。本机新 DS 双副本已运行；剩余缺口是 unknown 实际未提交时没有持久 owner-binding plan/reconciler，默认 no-show 回收有界但可能长时 fail-closed | [事故报告](2026-08-18-p1-ds-owner-bind-outcome-unknown.md) |
| INC-20260818-001 | 2026-08-18 | P0 | availability / split-brain | 本机已修复并闭环；生产滚动 P1 另案阻断 | **Hub 票据与 Owner assignment 分叉**：完整 target 变化推进 epoch+1/PENDING，只有 assignment CAS winner 才 bind Owner，ACK/Login 均有 exact gate；DS 同型竞态一并 fail-closed。四服务全模块、真 MySQL、Linux race、proto breaking/生成、后端 A→Release→B 与两轮真实 UE Hub E2E 均通过；第二轮 HUB confirmed 后观察 79.658 秒，续 Resume/Travel、mismatch、deadline、恢复面板均为 0。唯一 dirty 标签已安全部署且旧 ReplicaSet 全为 0；生产混版滚动仍由 INC-20260818-003 阻断，不得宣称 production-ready | [事故报告](2026-08-18-p0-owner-hub-assignment-divergence.md) |
| INC-20260814-001 | 2026-08-14 | P0 | availability | 已修复待部署（未关闭;与 INC-20260813-001 批次同车） | **隔夜幽灵票成局**:test0052 排队后直接关客户端,其单人票在队列里滞留 **16h53m** 后被拿去与次日新玩家 test3 成局(match=23739104583778304),DS 只进来 test3 一个人——对面是幽灵。全链零错误日志,locator 还把离线 16h 的人投影成 BATTLE(ds_allocator 按 roster 全员刷新,不看谁真连上了)。根因=**非终态排队票没有任何"人已离场"的回收路径**,四环齐失:①排队票 TTL 被持续续期事实上永生;②liveness 门全环境关闭(INC-20260724-001 结构性假阳性回退,且不可重开);③team offline_leave 刻意不取消匹配 + 跳过单人队;④成局装箱前无在场性检查(`ensureAllPresent` 只挂在入队时,管不到已在队里的票)。修复=新建独立**排队票离线回收**链:新配置 `queue_absence_reap_after=120s`(负值关闭,纯配置回滚)+ 周期扫除 `queueAbsenceSweepOnce`(**弱依赖**,presence 查询失败一张不删)+ 成局装箱前复查 `rejectAbsentTickets`(formMatch/formSoloMatch 双路径,**fail-open**,不给 locator 抖动阻断成局的权力)+ match_found 补票龄与 `stale_ticket_matched` 告警。判据复用 INC-20260813-001 证据链单点 `absentBeyond`——按「**离开了多久**」判,UNKNOWN 永不冒充 OFFLINE(§9.22);**刻意不重开 liveness 门、不走事件推送、不动 team 单人队行为**(有测试锁定)。10 个针对性单测全绿,变异测试(回收整条关闭)击杀 4 用例证明断言是活的,存量在线闸回归保持绿。未关闭原因:未提交未部署(部署依赖 08-13 批次的 presence 接线,单独上线=安全降级但零保护)/ 集成与玩家 E2E 零执行 / 观察窗口为零 / locator 投影语义失真(A-2)与 MATCHING 零保活(A-3)另案 | [事故报告](2026-08-14-p0-matchmaker-stale-ticket-ghost-opponent.md) |
| INC-20260813-001 | 2026-08-13 | P0 | availability | 修复方案复核阻断，禁止部署（未关闭;完整待修复清单见档案 §13） | **打完一局队伍不复位 ready**,队友还在「结算→回大厅→重登」路上就被冻进下一局票据。队员阵亡后**先退了战斗**(比该局结算还早 13s),队长在他退出仅 **75 秒**后就开了下一局 —— 而 team 侧**从来没有任何一条 match-ended 路径**(全仓只有 LeaveTeam/Kick/离线摘人会把 State 打回 FORMING),队伍仍停在 READY、全员 ready 标记原样保留;`StartMatch` 七道门又没有一道看「人还在不在」(唯一读 presence 的只拦 BATTLE)。结果 `match_found players=6` → DS 拿到 6 人 roster 只进来 5 个,3v3 打成 3v2,DS 终局判定还按 `roster=6` 算。**全链零错误日志**,所有服务都认为自己成功了。**这跟掉线无关** —— 缺席者全程正常,只是连打第二局的常态窗口,任何离线阈值都堵不住;`offline_leave.threshold=180s` 与本事故无关,别去调它。**v1 根因判断已被推翻并保留在档**:曾把 Hub 的 `玩家离开大厅，上报 player_locator 断线` 读成「退出客户端」,实际那是 travel 进**上一局** —— 误判源于只搜了当前 DS 日志、漏了换局 backup 出来的 `Pandora_N-backup-*.log`,该行日志对「退出」与「travel」输出完全相同的文本(已列 A-10 要求加去向字段)。修复七件套:**①对局结束复位准备**(新增 `TeamService.EndTeamMatch`,挂在 `ReleaseMatch` 上复用 battle_result outbox 的重投可靠性;放在 claim 释放之后、`DeleteMatch` 之前;**刻意不给开关** —— 它是补回一条本就该有的状态机边,不是可选闸);②locator `touchHubAliveScript` 漏传 `ARGV[3]` 致第 2 跳起必 Lua 报错(`cmdstat_eval failed_calls=1109/1152`);③心跳按 **census 全员**刷 `last_alive_ms`(原先只刷 `state==HUB`,撮合失败后停在 MATCHING 的玩家人坐在大厅却永久停更 —— 正是 INC-20260724-001 假阳性的根子);④`ensureAllPresent` 在线闸 + `ErrMatchMemberOffline=4011`,判据是「**离开了多久**」而非「此刻查不查得到」,UNKNOWN 放行(§9.22);⑤team 掉线软档清 ready 但不摘人(回调挂 `Observe` 而非 Sweep 的 waiting 分支 —— due 排在离开+阈值,新鲜离场在满阈值前根本扫不到,挂那儿是不可达代码);⑥ds_allocator 花名册到齐期限(按 census 与 roster 差集判、**只在开局阶段生效**(全员到齐过即永久豁免,否则局中掉线会判弃正打着的对局)、只罚缺席者;45s 是取舍不是安全上界);⑦`startMatchAdmitted` 全部拒绝分支补日志(此前裸 return,排查只能靠 envoy 响应体字节数反推)。结构性隐患四条,首条=**`Begin` 有、`End` 没有**(`BeginTeamMatch` 做得很仔细却只加了开始那一半,租约自净 + State 刻意不写让这个缺口在代码里完全看不出来)。定向测试已覆盖主要 happy path，但发布复核发现 End 缺跨代幂等、旧新 matchmaker 共存会漏复位、allocator 缺安全激活 generation；`-race` 各目标分包有通过证据，但五族合并首跑 exit=1，不能记作同一命令全绿。未关闭原因:A-11 发布阻断 / 未提交未部署 / 玩家 E2E 与 Model B 真集群零执行 / 观察窗口为零 | [事故报告](2026-08-13-p0-matchmaker-offline-member-frozen-into-roster.md) |
| INC-20260812-002 | 2026-08-12 | P0 | availability | 已修复待观察（未关闭） | 本地 MySQL 沿用生产级双 fsync(`trx_commit=1`+`sync_binlog=1`)而 `datadir` 在 emptyDir→Docker Desktop 虚拟磁盘,单次事务提交长尾 **19.4 秒**;`owner.QueryOwner` 因此超时 → `hub-allocator` 按 §9.22 fail-closed 返 `ErrUnavailable` → Hub DS **拒绝 SetLocation 并踢人**,玩家在大厅随机掉线。放大:被踢触发 Logout 清空 `PlayerSocialIds`,表象变成"队友血条不变色"——从 UI 一路倒查到磁盘。**最难点是所有健康检查全绿**(无崩溃/无重启/无 OOM,`rpc_ok` 全程),既有 `rpc_slow` 只打总延迟无法回答"慢在哪一段";为定位临时加的**分段计时**一击命中(`commit_ms` 等于 `total_ms`,`pool_wait/select/begin_tx` 全 0),同批补了 pprof(含 block/mutex 采样)。修复=本地放宽持久化(线上不引用 `infra/`)。实测 P90 538ms→2ms、最大 19397ms→266ms、踢出 6 次→0。**准入 fail-closed 本身无缺陷,不改**。同窗口并存一个独立故障:`start.ps1` 把 edge envoy 上游写成**短名**,DNS 搜索域失效时 18 个上游全解析到 Docker Desktop 网关 `192.168.65.254`(`cx_connect_fail=19`),登录 503——已改全限定名但**未生效**(需重跑 `start.ps1`)。未关闭原因:观察窗口仅 60 秒 / envoy 修复未验证 / 无自动化回归 / `-race` 阻断 / 剩余风险 A-1~A-7 全未开始 。**第二轮跟进**:`-race` 阻断**已解除**(此前三份档案均误判为环境不支持,实为只需带 CGO 的 Linux,本机 golang 镜像即可,已固化 `tools/scripts/go_test_race.ps1`;owner 全包零 data race);补上自动化回归 `tools/scripts/tests/infra_durability_contract_test.ps1`(9 条断言,含"线上 overlay 不得引用 infra/"这一放宽前提,经**变异测试**证明能抓回退:改回 trx_commit=1 命中 2 条、改回短名命中 3 条);A-2 结论=etcd 每次提交 fsync WAL 且**无等价放宽开关**,是同类残留风险(另立 A-8),redis/kafka 默认值可接受。**第三轮**:A-8 已修 —— 更正 A-2 的错误结论(etcd **有**官方 `--unsafe-no-fsync`,只是没有 `trx_commit=2` 那种"降级但仍持久"的中间档),本地整个关掉;逐项核对 etcd 存的 key 全部可重建(租约型/版本键/本地无意义的运维开关),且只在硬崩溃时丢(优雅重启仍 flush),比 tmpfs 每次重启必丢更保守。契约测试扩到 11 条,新增两条同样经变异验证 | [事故报告](2026-08-12-p0-owner-fsync-admission-kick.md) |
| INC-20260812-001 | 2026-08-12 | P0 | availability / near-miss | 已止血（未关闭） | 两个**已发布**迁移做的是 contract 而不是 expand:`pandora_account/000006` 用 `RENAME`+`DROP` 换掉角色编号三件套(`register_no` 列/`uk_register_no`/`register_no_counter`),`pandora_player/000007` 直接 `DROP players.mmr` —— 迁移一执行,尚未排空的旧 Go 副本读写的对象**当场消失**,违反 §9.16/§9.21「删除能力必须走 expand → migrate → contract」。§3.6.3「生产零注册路径、无存量数据 → 现在是改名成本最低点」只覆盖**数据**风险,不覆盖**二进制共存**风险。放大因素:`players.mmr` 上挂着 `idx_mmr` → `DROP COLUMN ... ALGORITHM=INSTANT` 在 MySQL 8.4 报 **1845**,留 `schema_migrations` v7 dirty,而 migrate Job 是 `backoffLimit:0` 硬门禁 + `rejectDirtyOrNewer` fail-closed → **一次 dirty 卡死此后全部发布**。修复=`account/000007` + `player/000008` 两个纯加法 expand 回补兼容面并双写 + 一次性精确 quarantine 收敛 v7 dirty。**线上零影响**(v7 版镜像因迁移门禁从未滚动上线;那份代码的 `EnsureProfile` 仍写已被删的 `mmr` 列,上线即 1054,物理上不可能作为 Stable 存在)。未关闭原因:contract 退出条件未定义 / 未部署 / 真库迁移矩阵与 E2E 零执行 / 防复发的 expand-only 机械门禁未加 / **本轮审查 5 个维度中 3 个 agent 中断,19 条发现仅 3 条完成裁决** | [事故报告](2026-08-12-p0-published-migration-rolling-upgrade-break.md) |
| INC-20260811-002 | 2026-08-11 | P0 | availability / near-miss | 已修复待部署（未关闭） | friend / mission 写事务在 MySQL 默认 RR 下**确定性** 1213 死锁:未命中的 `SELECT ... FOR UPDATE` 锁的是**键所在的间隙**而非某一行,N 个事务各持同一相容间隙锁后再 INSERT,插入意向互相阻塞成环 —— **互不相干的玩家会互相打死**,mission 那条打在 `ApplyFactsTx`(每场战斗结算必经)。**只在 MySQL 复现,TiDB 无 gap 锁**,而 CI 从不设 `PANDORA_TEST_MYSQL_DSN` → 相关用例全 Skip、`go test` 对全 Skip 的包打印 `ok`,缺陷被"跳过等于通过"长期遮蔽。修法=四条写事务显式 READ COMMITTED(该域正确性本就不依赖 gap 锁,守卫行的存在理由正是 TiDB 无 gap 锁)+ friend 守卫前移;三组反向变异证明 **RC 才是根治、守卫前移是纵深防御**。同批堵上 CI 的跳过审计(量化:friend 一个模块就有 14 个用例从未在 CI 执行)。**线上零影响**(未部署)。未关闭原因:未提交未部署 / 观察窗口为零 / `-race` 阻断 / 全仓同型 14 个文件**动态普查无结论**(阳性对照未复现,已 fail-closed 不输出假阴性) | [事故报告](2026-08-11-p0-friend-mission-rr-gap-lock-deadlock.md) |
| INC-20260811-001 | 2026-08-11 | P0 | data / near-miss | 已修复待部署（未关闭） | mission 域**上线前**审计发现五类奖励/进度缺陷，任一上线即 P0:①热更加条件→白送完成并发奖(min 槽截断,C++ 忠实移植在新语境下变 bug);②GT 比较符钳位把达标打回不达标→任务永久活锁,LE/LT 接取即恒真、EQ 越过目标即失联;③发放形态未冻结→`:stack`/`:inst` 双幂等键各发一次(或永久发不出去);④TiDB 无 gap 锁→接取上限实测放过 12 条、类型互斥 8 条全活;⑤装备件数加载期判单条而发放期判累计→坏配置永久发不出去 + FAILED 行无界增长。五条均已落码且有**先红后绿**实测回归。**线上零影响**(服务从未部署,`mission_addr` 全环境未配)。同型扫描命中 leaderboard `MarkReward`(另列 A-1)。未关闭原因:未部署 / 观察窗口为零 / `-race` 阻断 / 客户端零接线致玩家 E2E 物理不可执行 | [事故报告](2026-08-11-p0-mission-prelaunch-reward-integrity.md) |
| INC-20260806-001 | 2026-08-06 | P0 | availability / session-fencing / near-miss | 修复已实现待审核与集成验证（未关闭） | Hub 秒重连同 Pod 时旧 Disconnect 无 admission fencing，且 HUB 写与 last-seen 清理非原子；服务端已加 owner+Admission fence，UE/race/E2E 未完成，team 自动退队继续 fail-fast 关闭 | [事故报告](2026-08-06-p0-player-locator-reconnect-stale-disconnect.md) |
| INC-20260804-001 | 2026-08-04 | P0 | availability | 修复实施中(未关闭) | mode=local 全链断裂：Model B 落地后新增的 owner/admission 权威接线在 legacy 面**成片漏接**，玩家依次卡在进大厅被踢 / 进大厅确认不了 / PVE 对局判 FAILED / 进副本确认不了 / 退副本被拒。2026-08-13 新证据推翻旧 A-6「combat-factions 当前无消费者」结论：local 未投递权威阵营会让 UE 出生硬门 `RejectSpawn`，玩家进图无 Pawn（旧结论写下时属实，**2026-08-11 客户端 r1971 把阵营升为出生硬前置并删掉旧退化路径后失效，而无人回看本档行动项**）。Go 四件套投递修复、普通测试及 Linux race 已通过；UE 侧收下第四件，并顺带修好一条**自 r1971 起从未生效过**的 `-combat_factions=` 命令行兜底（原排在权威快照装载之前，被 `ApplyAgonesAdmissionMetadata` 整体替换抹掉——照日志提示自救那条路也是断的），但 **UE 未编译、玩家 E2E 未执行**。方法论产出 A-8：「当前无消费者」是会过期的判断，不能当长期安全前提。**第⑦处「对局正常结算不释放 owner」非 local 专属——Agones+legacy 与标准 Model B 生产同样缺失**，Model B 真集群仍未验证 | [事故报告](2026-08-04-p0-local-legacy-owner-wiring-gaps.md) / [待验证清单](2026-08-04-p0-local-legacy-owner-wiring-PENDING-VERIFICATION.md) |
| INC-20260803-002 | 2026-08-03 | P0 | availability | 已修复待部署（未关闭） | 载人 Allocated Battle DS 被当孤儿人工删除（日志窗口否定证据三重失真），玩家局内被踢；结构性根因=无记录孤儿 Allocated 无自动回收、人肉清理是唯一出路。已落码：ds_allocator 孤儿对账清扫（无权威引用+超阈值观察+UID/RV 双 precondition）、start.ps1 只删非 Allocated+删前重查、e2e 清场 fail-closed 显式开关；zero-downtime-update.md §6.3 设计复议废除人工清理条款。镜像未重建、真集群未验证 | [事故报告](2026-08-03-p0-ds-allocator-wrongful-allocated-delete.md) |
| INC-20260803-001 | 2026-08-03 | P0 | crash | 已部署待验证（未关闭） | Artic01 战斗 DS SIGSEGV：`UMyBTTask_SpecifySkill::TickTask` 对已 GC 的 MoveTask 弱指针调虚函数（中止移动跳过清理链→GC→null 虚调用），当日两崩、玩家局内被弹回。修复=四指针 fail-closed 守卫（UE r1648），已实证进入镜像 `r1647-dirty-20260802-221833` 并滚 fleet；完整一局+观察窗口未完成 | [事故报告](2026-08-03-p0-battle-ds-btask-sigsegv.md) |
| INC-20260729-002 | 2026-07-29（EDT；UTC 2026-07-30） | P0 | availability / observability | **根因已定谳**（07-30 07:11 复发抓到游戏线程堆栈）；玩家路径已闭环实测；次级根因未修，未关闭 | Artic01 对局已完成 ACTIVE/READY/Admission，最后一拍可绑定业务心跳后约 15.8s 被 stale 回收。**07-30 复查推翻「删除前 53s DS 没打日志」**：DS stdout 在容器里走 libc 块缓冲（UE 只在 `SSH_CONNECTION` 非空时才 `setvbuf(_IONBF)`），跨 10 分钟的日志曾单批到达，退出时 flush 被截断 → 解释停跳的整个窗口日志丢失；停跳因此收敛为 H1 游戏线程停摆 / H2 Pod 对外网络中断两个不可判别假设，A1 的阻塞项是取证管道。另定谳一条独立 P0：**§9.4 abandoned 被设计为「玩家解放出口」却无任何面向客户端的投递**（无 push、无 owner 释放、入场后无权威重查、退出 pending 无 deadline），无论根因如何玩家都会卡死。**07-30 07:11 同型复发被完整捕获并定谳**：挂起检测打出堆栈 —— 游戏线程卡在 `FMallocPoisonProxy::Realloc → FMallocBinned2::Realloc → libc`（调用自 `UWorld::Tick → RunTickGroup`），同期独立线程 health 零漏拍 → H1 成立、H2 排除。**次级根因**：DS 以 Development 配置构建，`UE_USE_MALLOC_FILL_BYTES` 为真 → poison malloc 每次 alloc/realloc 全量 memset（放大因素：节点内存吃紧，曾报 `Insufficient memory`）；**该项未修，是当前最高优先级 A6**。实测生效：stdbuf 行缓冲、`HangDuration=10` 堆栈、判弃释放 owner（released=1）、PIE 连接超时→权威恢复（断线到回大厅 2.96s）。未验证：心跳窗口摘要（未进 DS 二进制）、退出门闩有界化（未触发）。不是 warming 误删复发；本次为 Gate C 失败样本 | [事故报告](2026-07-29-p0-battle-ds-reclaimed-client-exit-stuck.md) |
| INC-20260729-001 | 2026-07-29 | P0 | availability | 修复已落码，未部署未验证（未关闭） | 节点落盘 I/O 卡顿（etcd WAL fdatasync 39.4s）致 ds_allocator 失 capability 保护性退出；因 replicas:1+Recreate，Heartbeat 断流 160s ≫ Battle DS 20s 授权租约 → DS 自我 fencing 踢人。结构性根因=重启预算未闭合（§16.8），任何升级都会打断全部在场对局（破底线 7）。已落码：2 副本+RollingUpdate+PDB、sweep 由 writerlease 选举串行化、fence 失效原因可辨、UE 等待窗口跨轮残留修复 | [事故报告](2026-07-29-p0-ds-allocator-single-replica-restart-kills-battles.md) |
| INC-20260727-002 | 2026-07-27 | P0 | crash / availability | 14Gi+maxReplicas=2 已生效（完整一局回归未跑，未关闭） | Battle DS 加载 Artic01 anon 内存顶死旧 limits 2Gi 被 memcg OOM 杀（dmesg 9 例）；实测 peak=10.43GiB/12Gi 围栏 0 OOM，limits=requests=14Gi（×1.34）+ autoscaler 上限 3 已落配置 | [事故报告](2026-07-27-p0-battle-ds-artic01-memcg-oom.md) |
| INC-20260727-001 | 2026-07-27 | P0 | availability | 门 A/B+pinger 硬门（含 canary）已过，剩门 C×3 与观察窗口（未关闭） | Artic01 冷加载期 warming DS 被单阈值 sweep 误回收；第二 P0=BeginPlay 过早宣告 running（心跳移 PostLoadMapWithWorld）；第三 P0=PostLoadMap 回调内首拍即激活/放行 ds_addr，回调后线程续阻塞 17s 被 ACTIVE 15s 回收（实测两例）——已下沉为 allocator 两阶段激活门（≥3 次实收心跳且跨度 ≥10s 才提升，期间保持 warming）+ DS 纯周期心跳 + NetDriver fail-closed 门；验收门 A/B/C、pinger 硬门与节点级抖动定谳（A10）OPEN | [事故报告](2026-07-27-p0-ds-allocator-warming-coldload-reclaim.md) |
| INC-20260726-003 | 2026-07-26 | P0 | availability / client-state / near-miss | 已修复待验证（未关闭） | UE SelectRole 旧请求被权威换代后门闩未释放；ROLE_REQUIRED 重进选角会永久吞掉后续确认 | [事故报告](2026-07-26-p0-client-select-role-stale-singleflight.md) |
| INC-20260726-002 | 2026-07-26 | P0 | session-fencing / data / availability / near-miss | 补偿已修复，仍有相邻交付缺口（未关闭） | A/B/C 未交付前代恢复已改无能力墓碑；post-Set placement 失败仍会扣留新 session | [事故报告](2026-07-26-p0-login-session-candidate-delivery.md) |
| INC-20260726-001 | 2026-07-26 | P0 | split-brain / data / availability / near-miss | 修复实施中（未关闭） | hub_allocator writer lease TTL 盲续/复活；assignment 补偿丢 operation identity/PTTL 与 tombstone ABA；CreateShard 绕 fence；Redis HA 已确认写回档仍 OPEN；canonical green strategy 漂移 | [事故报告](2026-07-26-p0-hub-writer-fencing-near-miss.md) |
| INC-20260724-001 | 2026-07-24 | P0 | availability | 修复实施中(未关闭) | 战斗中退出后进不去：DS 分配不可用（fleet churn 至 ready=0 + 控制面超时）叠加 matchmaker 成局最终门 **结构性 100% 假阳性**（MATCHING 投影零保活，30s TTL 后必然全员误判离线；实测 31.07s 判死）+ ALLOCATING 期玩家无出口；已落码 FIX-1（两道 presence 判死门回退关闭）+ FIX-2（pre-checkpoint 取消出口），单测绿、真集群未验证。原"孤儿 start-claim"与"travel churn 致 presence 失效"两假设均被推翻 | [事故报告](2026-07-24-p0-matchmaker-orphan-start-claim-freeze.md) |
| INC-20260722-004 | 2026-07-22 | P0 | security / session-fencing / near-miss | 修复实施中(未关闭) | push 旧/被顶号会话 token 仍能订阅私有推送流(建流无 jti 现行性校验,流寿命无界) | [事故报告](2026-07-22-p0-push-stale-session-subscribe.md) |
| INC-20260722-003 | 2026-07-22 | P0 | data / near-miss | 修复实施中(未关闭) | inventory Bag journal sweep 可删除 checkpoint 未覆盖的恢复尾部 | [事故报告](2026-07-22-p0-inventory-bag-journal-sweep.md) |
| INC-20260722-002 | 2026-07-22 | P0 | split-brain / near-miss | 修复实施中(未关闭) | locator 面已 fail-closed；Owner Authority 仍缺 Login+Hub+Battle 同批强 Begin/Admit/Release、稳定 operation/票据身份与 WAIT 恢复，禁止只启用 Hub contract | [事故报告](2026-07-22-p0-hub-allocator-locator-fail-open.md) |
| INC-20260722-001 | 2026-07-22 | P0 | data / near-miss | 修复实施中(未关闭) | trade 结算成功后 Redis 订单终态可被并发取消撕裂 | [事故报告](2026-07-22-p0-trade-settlement-state-race.md) |
| INC-20260721-001 | 2026-07-21 | P0 | crash | 根因确认 | ds_allocator Heartbeat 响应 metadata 并发写导致 fatal，租约恢复窗放大为玩家被踢 | [事故报告](2026-07-21-p0-ds-allocator-heartbeat-context-race.md) |
