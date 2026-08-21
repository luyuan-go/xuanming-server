<!-- 由一轮多 agent 审计生成并经主会话抽查，见文末「证据等级」。勿当作已验证事实全盘引用。 -->

> **这份文档的性质**：2026-08-19 一轮逐服务审计的产物 —— 14 个清点 agent（逐层读 Go 与
> Python 源码）+ 21 个对抗复核 agent（默认清点结果是错的，逐条取证证伪）。
> 229 条断言中 **212 CONFIRMED / 8 REFUTED / 9 ALREADY_MIGRATED / 0 UNCLEAR**。
>
> **证据等级**：所有结论来自静态读码 + 复核取证，**不是真实对拍**（例外见 §7.2）。
> 主会话已独立抽查过第 1 节的 #2（auction 枚举错位）与 #11（data_service 缓存格式），
> 两条与描述完全一致；其余未逐条复验。**引用前先看 §7.2 的覆盖边界与行号订正说明。**
>
> 总纲与已落码结论在 [`python-migration.md`](python-migration.md)；
> RPC 覆盖的机械口径在 [`python-migration-coverage.md`](python-migration-coverage.md)。

# Python 迁移剩余工作报告（逐服务）

**数据来源**：21 个服务的逐层清点 + 对抗复核（229 条待核项，213 CONFIRMED / 7 REFUTED / 9 ALREADY_MIGRATED / 0 UNCLEAR），外加复核阶段新发现的 89 条 `also_missed`。
**Go 侧总量**：非测试代码 104,948 行；两个 allocator 占 31,582 行（与文档数字一致），其余 19 个服务 73,366 行。启动闸合计约 318 道，其中约 2/3 是 fail-fast。

---

## 1. 第 0 优先级：已经落码、但是错的（12 处）

这一类比"没迁"危险一个量级：代码在、测试绿、日志正常，而且多数已被自洽的单测锁死。**动任何新服务之前先清掉这一批。**

| # | 位置 | 错在哪 | 后果 |
|---|---|---|---|
| 1 | `python/pandorapy/services/player_locator/biz.py:34-38` | LocationState 编码整体错位：HUB=1/BATTLE=2/MATCHING=3，proto 是 OFFLINE1/LOGIN_PENDING2/HUB3/MATCHING4/BATTLE5（`internal/biz/locator.go:41-48`）。`test_player_locator.py:79` 把 state=4（真 MATCHING）断言为"应被拒" | 真 HUB 被当 MATCHING 校验；真 MATCHING/BATTLE 被判 state_out_of_range 拒写；GetLocation 的 key-miss 占位值 1 在两侧语义正相反 |
| 2 | `services/auction/submit.py:33-43` | 订单状态与 Side 双错位。STATUS_FILLED=2 而 Go PARTIAL=2（`data/auction_repo.go:29`）；SIDE_SELL=0 / SIDE_BUY=1 而 proto SELL=1/BUY=2 | status=2 被 is_terminal 判终态并释放 owner 名额；**SIDE_BUY=1 恰好别名到 inventory 的 EscrowSideSell=1，买单冻的是道具不是金币，且绕开 safeMulInt64 溢出守卫**（`inventory/biz/inventory.go:776-784`） |
| 3 | `services/mission/engine.py:78,:84` | COMPLETE_MISSION 写成 1（proto/Go=8，`pkg/configtable/condition.go:29`）；saturating_add cap=2**31-1（Go MaxUint32，`biz/mission.go:780`）。两条都被 `test_mission_engine.py:73/:371` 固化 | 链式任务后环永远收不到"前置已完成"；槽位过滤集为空的杀怪条件被误推进度 |
| 4 | `services/battle_result/roster.py:136-139` | should_apply_rating 在 rating_mode 未定格时返回 **False**，Go `biz/battle_result.go:349-363` 两条 legacy 路径都返回 **true**。两边测试各自锁死相反行为 | 混跑期同一场对局在两侧算出不同段位；排位局白打 |
| 5 | `services/push/offline.py:179-184` | 坏 member 静默 `continue`，Go `data/offline.go:258-316` 是 Error 日志 + fl 哨兵折账 + 物理删除 + 折账失败时扣发坏帧之上好帧 | 好帧把游标推过坏帧，丢失既无记账也无 resync，永久静默漏报（Go 已在 R5 P2-1 / R7 P1 两轮修掉） |
| 6 | `services/friend/repo.py:181-185`、`:230-234` | ①再次申请不轮换 request_id、不刷 created_at（Go `friend_repo.go:326-328`），`test_friend_repo.py:291-297` 断言了错行为；②accept_request 先 `SELECT ... FOR UPDATE` 业务行再取守卫，与 create_request 的守卫→业务行**反序** | ①客户端按 (request_id, reason) 判重，新推送被当重投丢弃；②同一 pair 上"重新申请 vs 接受"并发成 ABBA 环，而 `_write_tx` 没有 1213/1205 重试兜底 |
| 7 | `services/guild/group_repo.py:189,:226,:243-265` | ①member 的 role 写 0（Go `GroupRoleMember=2`，`group_repo.go:24`），biz 是裸数值强转 → 客户端看到 UNSPECIFIED 且 `ORDER BY role ASC` 把成员排到群主前面；②remove_member 是"明细锁→计数行锁"，add_member 是"计数行→明细"，构成锁环 —— **该文件自己的 docstring 第 5-13 行就写着不能这么做** | role 显示错位；偶发 1213 且 `_run_tx` 只打 debug |
| 8 | `services/chat/data.py:154,:160` + `conf.py:63-72` | 两个限流 key 前缀与 Go 不一致（`pandora:chat:cd:world:*` vs Go `pandora:chat:world:cd:*`；`pandora:chat:cd:<ch>:*` vs Go 规范 `pandora:rl:chat:<ch>:*`）。~~四个默认值漂移~~ ⚠️ **2026-08-19 落码时证伪：这半条方向写反了**——Go 的真实默认就是 world 3s / non_world 500ms / sweep_interval 5m / sweep_batch 500（`chat/internal/conf/conf.go:99-113`），与 Python 原值**逐个相同**，不存在漂移。 | key 前缀那半成立：灰度期同一玩家在两侧各占一个 key，冷却翻倍放宽，巡检脚本对 Python 副本失明。已修 |
| 9 | `services/ds_allocator/orphan_reclaim.py` | 六处漂移：无 5min 下限钳制（Go `orphan_gameserver.go:119-141`）；单轮封顶 5 vs Go 3（`:101`）；引用集从三维（pod名∪UID∪allocation_id，`:151-162`）退化成一维；**首见表键用 `gs.name` 而 Go 用 `name+"/"+uid`（`:259`）**；无 `gs.Deleting` 过滤；无墙钟预算 | 名字复用的重建 GS 继承旧观察起点、第一轮即被删；DsPodName 为空但 UID/allocation_id 已写的在途记录被判孤儿 —— 删的是活着的 DS |
| 10 | `services/hub_allocator/capacity.py:71-84` | record_matches_instance 缺三条**前置**硬约束（uid!=""、epoch!=0、writer 必须恰等于 2，Go `hub_capacity_ledger.go:397-400`）；多条 successor 被 `set()` 静默去重（Go 视为 ErrInvalidState 整条拒）；HASH field ↔ 记录内 assignment_id 的身份互校缺失（Go `:184-196`） | 幽灵占座留在账本 → Hub 假性满员；Model B writer 代际门被整个拆掉 |
| 11 | `services/data_service/data.py:184-198` | 缓存值是**裸 pb**、无格式头，却与 Go 写同一个 key。Go 是 `'PDC'\x02` + BE uint32 位图长 + 字段号位图 + 超集判定（`data/cache.go:56,:137-183`）；另缺 player_id 串号校验（Go `:160-165`）与坏档 WARN（Go `:152-157`） | 实测 Python 读 Go 条目必抛 DecodeError 并被静默吞、Go 读 Python 判 miss 且不打日志 → **双向命中率塌成 0，零信号**；§9.16/17 的缓存投毒防护整个消失 |
| 12 | `services/trade/service.py:66` | ConfirmOrder 失败路径 `getattr(exc, "order_state", UNSPECIFIED)` —— `PandoraError.__slots__`（`errcode.py:28`）没有 order_state，biz 也从不 set，这条 getattr 恒取默认值 | Go 在失败时仍回真实状态（`service/trade.go:61-63`，四条路径 `biz/trade.go:351/398/403/424`）。客户端拿到 code=UNAVAILABLE + new_state=0，无法判断该重试还是该当订单没动 —— 而 `service.py:8-9` 的模块 docstring 恰好写着这条铁律 |

---

## 2. 已证伪，别重报

### 2.1 REFUTED（7 条）—— 声称的缺口不成立

| 服务 | 条目 | 为什么不成立 |
|---|---|---|
| login | `sessionTombstoneJTI = "logged-out"` 必须逐字一致 | 全仓只有两处**写**（`data/session_generation.go:224,:244`），**没有任何读路径把列值与该字面量比较**。真正的消费者 `data/role.go:88-99` 是裸相等比较，换成任何非 uuid 串结果完全一样 |
| ds_allocator | collectBattleGameServerRefs 的 fail-closed 契约未迁 | 已迁在 `orphan_reclaim.py:93-104` reset_all_observations，docstring 逐字对应 Go `orphan_gameserver.go:228-236`。且 Go 出错时同样返回填了一半的 refs，保护同样靠调用方纪律 |
| team | ClaimPlayer 的 SETNX 两次尝试循环 | 唯一调用点是 `biz/team.go:1583/:1617` 的 claimPlayerHealingOrphan，它自己会 Get→CAS删→再 Claim 一次。data 层循环是第二道防线，不是唯一依据 |
| friend | 启动期 mysqlx.CheckTables 守卫表存在性门 | 原语已迁（`mysqlx.py:116`，owner/main.py:181 已有调用）；缺表的失败形态是第一次 AddFriend 抛 1146 —— 立即、全量、显式，是最响的信号，不符合"静默"判据 |
| dialogue | 失租 → os.Exit(1) 的 fencing 缺失 | `snowflake_etcd.py:243-266` provide_node 三分支齐全、`:104-159` keepalive + 安全线 + NodeIDLostError 全在，模块 docstring 照抄了 Go 契约。缺的是**装配位置**（见 §4.5） |
| dialogue | KillSwitch 中间件未挂载 | **Go 侧今天也恒 fail-open**：`pkg/killswitch:148-154` defaultManager 为 nil 即返回 false，唯一赋值入口 `pkg/svc/base.go:90` 的 BaseContext 全仓零调用方，无任何 main 做过 etcdkv blank import。潜伏差异，不是既存缺口 |
| data_service | cellroute fail-fast + player_data_placement 观测缺失 | `_log_placement` 已实现并已接线（`biz.py:174-188`、调用点 `:140`）。缺的是 `pkg/cellroute/etcdtable` 这个跨服务基础件，且"没有 main.py"本身是最响的信号 |

### 2.2 ALREADY_MIGRATED（9 条）—— 不变量本体已在 Python，缺的只是接线

| 服务 | 条目 | 已迁位置 |
|---|---|---|
| login | account_token/session_token audience 严格分离 + Envoy token 不设 kid | `auth.py:54-56`(双必填字段)、`:82-84`(validate 启动期抛)、`:113/:135`(with_kid=False)、`:153`(内部 token True)。原报告的 go_ref `conf.go:244` 也指错了（宿主是 `pkg/auth/jwt.go:252/:334`） |
| hub_allocator | HeartbeatTimeout < 27s 强制抬到 DSFenceReentryBarrier | `fence_timeline.py:68` + `:113-129` 第③条不等式 + 回归 `test_fence_timeline.py:83-88`。残留：Python 校的是常量不是 yaml 值，等 hub conf.py 落地时按 `player_locator/biz.py:86-105` 的范式落成运行期 clamp |
| inventory / guild / leaderboard | retention_mode 默认口径与拼错拒启（3 条） | `dbguard.py:223-238` parse_mode：空→REPORT_ONLY、无法识别→**raise ValueError**（注释明写"与 Go ParseMode 同一决定"）。Go 那条静默降级路径（`RetentionMode()` 吞异常回落 report_only）在 Python 侧写不出来 |
| guild / leaderboard / data_service / mail | AssertStrictModeStartup（3 条）与 payload 16KB 闸（1 条） | `dbguard.py:48` assert_strict_mode（含 5.0s 探测超时常量对齐、@@session 而非 @@global）；`dbguard.py:298-326` check_payload 三档语义与 Go 一致。owner/main.py:174 已有可照抄的调用范例 |

**这两批（16 条）请勿在下一轮重新立项。** 它们对应的真实工作是"某个服务还没有 main.py/conf.py 去调它"，属于 §6 的装配清单。

### 2.3 判定口径不一致，需要拍板

同一件事在不同服务被判成了不同状态，下一轮必须统一：

- **AssertStrictModeStartup**：guild/leaderboard/data_service 判 ALREADY_MIGRATED，**chat 判 CONFIRMED**。实质相同（原语已迁、服务未接线）。建议统一按 ALREADY_MIGRATED 处理，转入装配清单。附带更正：chat 复核已证伪它的具体场景 —— `chat_private_messages.content` 是 `VARCHAR(512)` 计字符数，而 max_content_len=256 码点，那条"中文满配 768 字节越界"的路径不存在。
- **retention_mode fail-fast**：inventory/guild/leaderboard 判 ALREADY_MIGRATED，**chat 判 CONFIRMED**（理由是"校验存在但没有 main.py 调它"）。同上，统一转装配清单。
- **一并记一条真实差异**：Go `pkg/dbguard/sweep.go:57-66` ParseMode 接受 `""`/`report_only`/`report`/`report-only`；Python `dbguard.py:230-238` 只接受 `""`/`report_only`/`delete`，遇到 `report` / `report-only` 直接 raise。**同一份 etc/*.yaml 在 Go 侧正常启动、在 Python 侧启动即崩** —— 是响的，但会在切栈那一刻把纯配置问题升级成起不来。

---

## 3. 与主会话已落文档结论的出入

1. **player 的 RPC 数**：本轮实测 `proto/pandora/player/v1/player.proto` 共 **28** 条 rpc（player 侧复核项 18 明确核对过），文档记 29。差 1 条，需要确认是否把 ConfigTableAdminService 算进去了 —— 若算，则 inventory 的 29 也要同口径复核。
2. **sessiongate 的 13 个服务**不包含 push：push 的 Subscribe 是 server stream，**Kratos unary 中间件链对它一律不生效**，Go 是在 service 层手写补齐的（`service/push.go:66-104`、`biz/push.go:145-211`）。push 的会话门是第 14 处，且形态不同（含 30s 看门狗 + sessionFailClose=3）。
3. **killswitch**：文档未提，本轮复核发现 **Go 侧事实上也没接线**（见 §2.1）。这与"Python 没挂 = 缺口"的直觉相反，需要拍板是"两边都补"还是"与 namecheck/grpcstats 一样划掉"。
4. **battle_result 的 push writer lease，Go 仓库内部自相矛盾**：`biz/experience.go:11-13` 与 `docs/design/realtime-progression.md:200-202` 说"多副本发布器无需 fencing，客户端按 (level, exp_in_level) 单调不回退去重"，而 `cmd/player/main.go:303` 的 hint 说"旧快照后到会让经验条倒退"，租约自己的注释（`experience.go:124-129`）给的第三种定性是"只保证保序，不是防脑裂"。**别照抄任一句**，先拍板。Python `experience.py` 顶部那段"无需 claim/fencing"是逐字抄的 Go，不是它编的。

---

## 4. 跨服务共性缺口 —— 应下沉成基础件一次做掉

按"多少个服务被它卡住"排序。这一节的每一条都不该在逐服务清单里重复实现。

### 4.1 会话现行性门 sessiongate（14 个服务，含 push）
- 跨服务 key 契约：`pandora:sess:<player_id>` hash 字段 `jti`（`pkg/sessiongate/sessiongate.go:51`，注释明写"两侧同步改"）。全仓 `grep pandora:sess python/` 零命中。
- 语义：`require=true` 时端点漏配 panic 拒启（`sessiongate.go:77-92`）、权威不可达 fail-closed（`pkg/middleware/session.go:66-69`）、jti 不匹配 ErrSessionSuperseded（`:86`）。prod 生成器机械置 true。
- 漏掉的表现：被顶号（INC-20260722-004）的旧 JWT 在 exp 前继续按 player_id 操作，**全部返回 OK，没有拒绝码也没有拒绝日志**。dev 档 require=false 时测试永远绿，prod 打开那天全员建流被拒。
- push 需要额外的 stream 版：30s 看门狗、`sessionFailClose=3`、**裁决顺序必须先代际后到期**（`biz/push.go:184-211`，R5 P0-2：反了会让两台设备互踢死循环）。

### 4.2 redisx 限流族（7+ 服务，且已有一处迁错）
- 缺 `RLKey` / `ActionQuota` / `Cooldown` / `IncrWindow` / `PenaltyRemaining` 全部原语；`redisx.py` 只有 LuaScript 壳与分布式锁。
- key 规范 `pandora:rl:<域>:<动作>:<主体id>`（`pkg/redisx/ratelimit.go:30-32`，登记在 infra.md §3.2）。**chat 已经自造了前缀**（见 §1 第 8 条），下沉时一并纠偏。
- 原子性：`quotaScript` 必须是一段 Lua（`ratelimit.go:66-71`，`INCR` + 仅 n==1 时 `PEXPIRE`）—— 拆两条命令，中途崩一次就留下无 TTL 的永久计数键，那个玩家此后永久被限流，而这看起来跟"真的刷太快"一模一样。
- 方向：Redis 故障一律 **fail-open**（`:88-95` 返回 (true, err)；消费侧 `guild/biz/guild.go:75-92`、`friend/biz/friend.go:68-84` 只 Warn 后放行）。trade/chat 已把方向迁对，可作参照。
- **跨服务共享键**：`pandora:rl:match:noshow:<pid>` / `:noshowcd:<pid>` 由 ds_allocator 写、matchmaker 读（`ds_allocator/data/noshow_recorder.go:6-7`），退避公式 `penalty = base << (count-free-1)`、shift 钳 8（`biz/allocator.go:3114-3155`），全链 fail-open。两端必须经同一个构造器拼，不得各自拼字符串。

### 4.3 kafkax producer / consumer / topics（12 服务、18 topic）
- `kafkax.py` 只有 140 行一致性哈希 partitioner，**一个 topic 常量都没有**。
- 缺的具体件：18 个 topic 常量（`pkg/kafkax/topics.go`，`topics.go:50-58` 把"新事件必须开新 topic"写成硬纪律）、KeyOrderedProducer/Consumer、DLQ producer（`pandora.dlq.<topic>`）、RetryPolicy(MaxRetries=3 / Backoff=500ms)、`Poison` 毒丸、`PushToPlayers` 的 **callerPlayerID 剔除**（`producer_test.go:88/:108` 证明该函数按 callerID 剔人；matchmaker 的 READY 推送刻意传 0 = 发给所有人含发起方，`biz/match.go:4475/:4527`）。
- 消费侧的三态 event_type header 处置（`player/biz/consumer.go:31-41,:66-69`、`push/biz/consumer.go:362-370`）：缺失→当旧事件；合法非 0→skip+WARN；**存在但非法→毒丸进 DLQ，不得降级为 legacy 0**。直觉写法 `int(h.value or 0)` 会把毒丸当 MMR 事件解码入账。
- key 解析出 `player_id == 0` 必须毒丸（`push/biz/consumer.go:234-241`，R5 P2-2：`"0"` 能过 ParseUint，旧实现写进 player 0 的缓冲并 ACK = 静默吞掉一条定向消息）。

### 4.4 dsauthfence capability + writerlease 接线（6 服务）
- `writerlease.py`（343 行）**已迁且加固过**（本地安全截止早于服务端 lease、LeaseGoneError 即停宣告、激活钩子独立总期限），但 `grep -rln writerlease pandorapy/services` **零命中** —— 零服务接线。
- 缺的是 dsauthfence 的 **capability/epoch 注册面** 与 **"失租 → os.Exit(1)"** 这个更硬的处置（login `main.go:456`、ds_allocator `main.go:504-509`、hub_allocator `main.go:605`、battle_result `main.go:396`、player_locator `main.go:220`）。
- 配套的 **PANDORA_DEPLOY_STRATEGY × writer_lease_mode 机械门禁**（player `main.go:299-313`、ds_allocator `main.go:529-547`、hub_allocator `main.go:647-657`、mission `main.go:227`）：RollingUpdate × 非 enforce → 拒启；受管 k8s（KUBERNETES_SERVICE_HOST 非空）而 env 缺失 → 拒启；非 k8s → 只告警。进程看不到 Deployment 的 spec.strategy，所以靠 annotation 注入 env，**另一半约束在测试里**（`push_writer_lease_manifest_test.go`），迁移后由谁承接未定。

### 4.5 snowflake 三条独立缺口
1. **`MinIDAt(unixSec)` 未迁**：`(uint64(unixSec) - Epoch) << 32`，**时间粒度是秒不是毫秒**（`pkg/snowflake/snowflake.go:212`）。chat / mail 两条 sweep 全靠它算 cutoff（`chat/biz/sweep.go:23`、`mail/biz/sweep.go:68`）。照抄网上毫秒版 → 一次删光全部历史；Epoch 抄反 → 恒返 0 一行不删。`snowflake.py:121` 的 `timestamp_of` 是它的逆，有现成锚点可对拍。
2. **static node_id 下界**：Go `etcdnode/provider.go:95` 拒 `staticNodeID == 0`（0 保留给 UE DS 本地发号器），Python `snowflake.py:64` 是 `0 <= node_id <= NODE_MASK` —— **放开了一格**，而 `config.py:134` 的 pydantic 默认值恰好是 0。yaml 漏写这行，Go 拒启、Python 起来并铸与 DS 本地 guid 空间重叠的 ID。
3. **装配责任被下放**：Go 把"失租退出"收在 provider 内（`provider.go:114-122` 注释："集中在此实现，各服务无需自行 select Lost()，避免漏写导致同 nodeID 双活发重号"）；Python `provide_node` 返回 `(Node, holder)` 就结束，不调 start_keepalive、不注册 on_lost —— **21 个 main.py 就是 21 次漏写机会**。
4. 附带：`config.py:154` 自陈 `snowflake` 段**未建模**，所以 `etcd_service_name` override 读不出来；`snowflake_etcd.py:185-215` 把 key 拼成 `{prefix}{service_name}/{node_id}`，照抄 docstring 示例传裸服务名 → mail 与 inventory 分属两个命名空间，**跨服务 instance_id 互斥当场失效且零日志**（Go 正确口径在 `provider.go:181-188` etcdKeyService，override 优先）。

### 4.6 dbguard 半迁的五处
- `Outcome` 缺 `truncated` 字段（Go `sweep.go:181`），而 player `biz/experience.go:303`、battle_result `biz/retention.go:111` 都靠它写"删到追平为止"的循环 —— 缺了会退化成每轮只删一批且无法判断是否追平。
- delete 分支**一条日志都不打**（Go `sweep.go:184-189` 打 `db_retention_deleted`）。
- 两个 Prometheus 指标 `pandora_db_retention_pending_rows`(Gauge) / `pandora_db_retention_deleted_rows_total`(Counter)（Go `sweep.go:86-93,:156,:185`）**完全不存在**，面板画出来是 no data 不是 0。
- report-only 事件名不一致：Python `db_retention_pending`（无 db、无 mode 字段）vs Go `db_retention_pending_not_deleted`（`sweep.go:157`）。讽刺的是 `dbguard.py:179-187` 的注释刚警告过这类事故 —— 容量巡检那条修好了，保留期这条没修。
- delete 模式多跑一次无界 `SELECT COUNT(*)`（Python `dbguard.py:265-272`；Go delete 直接 DELETE，不写 pending gauge）。对 `chat_private_messages`（预算 1.35 亿行）是重协处理器扫描，不报错、只让 sweep 变慢变贵。

### 4.7 schema 探针只到"列存在"这一档
- 已有：`mysqlx.py:116 check_tables`、`:149-172 assert_column_exists`（**只查列名**）。
- Go 侧还有两档：
  - **列形状**：DATA_TYPE / COLUMN_TYPE(signedness) / IS_NULLABLE / COLUMN_DEFAULT / 复合主键。见 guild `data/schema.go:15-40`（函数注释原话就是要否掉"只凭同名列放行"）、login `main.go:534/:547`（accounts.account_id 必须 Nullable=YES，expand 窗口写成 NOT NULL 会打穿滚动升级）、battle_result `progress_repo.go:686-703`（stopped_at_ms 必须 bigint/NOT NULL/default 0，否则停流 fencing 与 CAS 同时静默失效）、owner `main.go:123`。
  - **索引形状**：`SEQ_IN_INDEX` 顺序 + `SUB_PART` 必须为 NULL。见 player `data/experience_repo.go:310-341`（exp_history.uk_player_idem）与 `equipment_repo.go:88-110`（player_equipment.uk_player_instance）。前缀唯一索引 `UNIQUE(player_id, idempotency_key(1))` 会让首字符相同的不同幂等键被判 duplicate → **玩家静默少拿经验**。
- **风险点**：迁移者极可能拿现有的 assert_column_exists 交差 —— 那正是 Go 注释点名要否掉的做法。
- 另需：`AssertTiDBVersionAtLeast(7,4)`（login `main.go:558`）、`AssertColumnCollationSemantics`（login `main.go:562`，行为探针：该列实际仍是大小写不敏感 + NO PAD）、auction 的分片拓扑 marker exact-match（`data/shard_topology.go:42-95`）。

### 4.8 TiDB 无 gap 锁的守卫行范式
- 已有先例：friend `repo.py:104-110`、guild `group_repo.py:89-107`，且 `mysqlx.py:49-52` 的注释**点名 friend / mission**。
- 但**每个域选哪张表当守卫行是 per-service 决定，没有任何地方记着**：player 复用 `players` 行（`mmr_repo.go:93-116`，注释 ⚠️ 明写"该池首战时 player_mmr 一行都没有，零行上的 FOR UPDATE 一把锁都不加"）；mission 用 `mission_player_guards`（`mission_repo.go:154`，且必须是事务第一把锁）；friend/guild 另建守卫表；mail 的收件箱上限（`mail_repo.go:414`）**没有任何锚点行可锁** —— 直译 SQL 会得到"看起来加了锁、TiDB 上仍可双穿"的假闸，比不迁更危险（它会让复核者打勾）。

### 4.9 日志事件名 / 字段名的跨实现契约
- 已发现的漂移：owner（4 个事件名 + 5 个 reason 串全部改名，其中 `owner_transition_begun` 被 `docs/ops/player-journey-log-map.md:119` 定为"player↔pod/allocation 的第一处强 join 点"）、data_service（placement 少 `shard_key` 且丢 `player_data_route_failed`）、chat/dbguard（见 4.6）、trade（`trade_settlement_routing` → `trade_settlement_placement` 且跨 region 不再抬级别，prod info 级下 **一条都不落盘**）。
- 建议：把"日志事件名 + 关键字段名"做成与 `python/tools/parity/coverage.py` 同级的门禁。这类缺陷日志一条不少、级别看上去完全正常，只有按 Go 名字建的看板与告警对 Python 副本返回零行。

### 4.10 其余共性
- **服务端 RPC 超时**：`config.py:71` 建模了 `timeout`、`:79-80` 还写了 `timeout_td()`，**全仓零调用**；Go `grpcserver.go:71-72` 会挂 `kgrpc.Timeout`，dialogue yaml 写着 15s。Python 侧没有任何服务端 deadline，慢 handler 的 task 在 server 上无限累积。字段"看起来配了"，最阴。
- ~~**未建模的 yaml 段被静默吞掉**~~：`cell_route` 已于 2026-08-20 在 `config.BaseConf` 建成**正式字段**（`cellroute.RouterConfig`），校验走 `validate_mode()`。其余（snowflake / session_gate / killswitch / registry）仍建议做启动期白名单告警。
- ~~**`pkg/cellroute/etcdtable`**~~：已于 2026-08-20 迁完 —— `pandorapy/cellroute_etcd.py`（全量 Get 铺初始表 + watch 整表替换 + 三条不变量）与 `cellroute.build_router`（off/static/etcd 三分支）。Go 的 `WireRouter` 没有对应物是**刻意的**：Python 侧统一返回 `(router, watcher)` 让 main 显式接线并在 `finally` 关 watcher，回调式注入会让"忘了关"没有任何信号。三处调用方（friend / player / data_service `main.py`）已于同日接线，建不起来一律 `cellroute_init_failed` 拒启 —— "配了分片却按单 Cell 跑"是运维以为分了、实际一片没分。
- ~~**`friend_sharding.go` / `profile_sharding.go` 的落点观测**~~：已于 2026-08-20 迁完 —— `pandorapy/services/friend/sharding.py`（`accept_idempotency_key` / `edge_build_key` 幂等键口径 + `distinct_edge_regions` / `cross_shard_friendship` 判定 + `friend_edge_sharding` 日志）与 `player/biz.py::_log_profile_placement`（`profile_placement`）。router 未注入时整条不执行，与 Go 同分支。
- **`redisx.lock` 无法表达 auction 的 key 前缀**：`redisx.py:130-132` 硬编码 `pandora:lock:`，而 auction 刻意覆盖成 `pandora:auction:market:`（`market_locker.go:72`）。传全名会被拼成 `pandora:lock:pandora:auction:market:5` → 两边各锁各的、都能拿到锁。**`redisx.py:122-127` 的注释亲自预见了这个失败形态**，但留下的 API 恰好堵死了唯一的覆盖用例。
- **proto 序列化的跨语言字节确定性未验证**：inventory `bagEntryFingerprint = sha256(proto.Marshal(entry))`（`bag_repo.go:214`）是对 protobuf 字节求哈希 —— unknown fields 保序、map 迭代序、默认值省略策略都可能不同。另 Go `json.Marshal` 默认对 `< > &` 做 HTML 转义（login `ds_admission.go:66` 的两个摘要），是除空格/字段序之外的第三个逐位差异点。
- **unknown fields 必须保留**：player `biz/reward.go:56` 复用读取时的 stored message 只覆盖两个字段，禁止 `ParseFromString→新建对象→SerializeToString`（等效 DiscardUnknown，混跑窗口内静默清掉新副本刚写的字段）。

---

## 5. 推进顺序与依赖拓扑

### 5.1 拓扑（文本，被依赖者在上）

```
[基础件层 — 全部服务阻塞在此，无前置]
  safego(已修) / sessiongate / kafkax(producer+consumer+topics) /
  redisx(RLKey+ActionQuota+Cooldown+lock前缀参数化) / cellroute.etcdtable /
  snowflake(MinIDAt + etcd装配收口 + node_id=0拒) / dsauthfence+writerlease接线 /
  dbguard(truncated + metrics + 事件名) / mysqlx(列形状+索引形状探针) /
  configtable(全表 Store + 跨表校验器) / auth(dsticket RS256+JWKS) /
  internalrpcauth(Verifier + Redis replay store) / DSCallbackGuard / svc.BaseContext

[无服务间前置 — 可并行]
  owner(已跑) ── 被 login / hub_allocator / inventory(bag) 依赖
  dialogue(已跑)
  data_service
  leaderboard ── 弱依赖 inventory gRPC(发奖)
  player_locator ── 被 login/team/matchmaker/hub_allocator/ds_allocator 依赖
  push ── 只依赖 redis/kafka/etcd

[inventory 是最大的解锁点]
  inventory ──┬─→ trade      (GrpcResourceLedger / SettlePlayerTrade)
              ├─→ mail       (GrantItems/GrantInstances/ClaimTransferInstances)
              ├─→ mission    (GrantItems/GrantInstances)
              ├─→ auction    (FreezeForOrder/EnsureAuctionEscrow/SettleAuctionMatch)
              ├─→ battle_result (掉落发放)
              ├─→ leaderboard (结算发奖)
              └─→ player     (CheckInstancesOwned)
  inventory ──→ owner(bag 逐写授权，已跑)

[社交链]
  team ──┬─→ chat (TEAM 频道成员解析)
         └─→ matchmaker (BeginTeamMatch/EndTeamMatch)
  guild(含同进程 GroupService) ──→ chat (GUILD/GROUP 两频道，共用 guild_addr)
  friend ── 无下游被依赖，只依赖 player_locator(在线态)

[撮合与战斗链]
  player_locator ──→ matchmaker / team / login / hub_allocator / ds_allocator
  team + player_locator + ds_allocator ──→ matchmaker
  player + inventory + matchmaker + ds_allocator + mail + mission ──→ battle_result
  player ──→ login (EnsureProfile 角色名播种)

[login 是终点]
  login 依赖：player_locator / hub_allocator / matchmaker / player / owner
             + etcd capability lease + 两套 JWT 信任域 + 11 个 google.api.http

[不迁或最后迁 — 31,582 行]
  ds_allocator(18,958) / hub_allocator(12,624)
  ↑ 它们被 login / matchmaker / battle_result 依赖，
    所以那三个在纯 Python 部署下无法闭环，只能混跑
```

### 5.2 推进档位与理由

**档 0 — 基础件（阻塞一切）**
理由：§4 的 10 条里，任何一条不补，下游服务的对应闸门只能实现成"恒放行"，而它与 Go 的合法降级档**在行为上无法区分**。先补 sessiongate + redisx 限流族 + kafkax topics + cellroute.etcdtable + snowflake 三条，这五件覆盖了最多服务。

**档 1 — 清 §1 的 12 处错码**
理由：它们已经在仓里、有测试锁死、看起来是绿的。留着它们做迁移基线，后续每个服务都会照着错的抄。特别是 mission 的 COMPLETE_MISSION=8、player_locator 的 LocationState、auction 的两套枚举 —— 这三处是别人会当作参考实现去 import 的。

**档 2 — 已跑服务补齐（owner / dialogue）+ 两个低耦合服务（data_service / leaderboard）**
理由：owner/dialogue 是唯二能起进程的，先在它们身上把 sessiongate / RPC timeout / killswitch 三条基础件接线跑通，作为后 19 个服务的模板。data_service 只有 3 个 RPC、1037 行、无 kafka；leaderboard 无跨服务前置（inventory 只是发奖客户端，`allow_noop_reward` 可先关）。

**档 3 — player_locator + push（运行时面）**
理由：player_locator 被 5 个服务依赖，且它的 Lua 与 guardTransition 状态机是"一人一处"不变量的载体，越早落越好。push 的 Lua 已逐字节对齐（我们做过 diff），剩下的是 stream 生命周期，与其它服务无耦合。

**档 4 — inventory（最大解锁点）**
理由：7 个服务的下游。它没有任何 Redis Lua（已 grep 确认），原子性全压在 MySQL 事务 + FOR UPDATE + 唯一键 1062 上 —— 真正的风险是**锁序**与 **1062 语义**，两类都不写在类型里、写错也不报错。建议先 conf + 18 道启动闸（决定能不能带病上线），再 `inventory_repo` 的 claimLedger/指纹族（所有写路径的公共底座），再 escrow 三件套，**bag 域可整体推迟**（`bag.dsn` 为空就是合法的"未启用"）。

**档 5 — trade / mission / auction（依赖 inventory）**
trade 是 21 个里最接近可跑的（partial，effort M，零后台循环、零 MySQL）—— 补 main.py + GrpcResourceLedger 就闭环。mission 与 auction 各自 XL，auction 还额外卡在分库 + 单写者锁 + outbox + 三类补偿。

**档 6 — player + 社交链（friend / guild+group / team）**
player 被 battle_result 与 login 依赖，5470 行、28 RPC、15 张表、27 道闸。team 与 guild 是 chat 的前置。

**档 7 — chat**
理由：TEAM/GUILD/GROUP 三路扇出需要 team + guild 的 gRPC reader。biz 层已迁得相当忠实（651 行），补完 §1 第 8 条的 key/默认值 + main.py + service.py + kafka 五 producer 即可。**注意 Go 的 GroupReader 与 GuildReader 共用 `cfg.chat.guild_addr`**（`main.go:150`），而 Python `conf.py:27` 自造了独立的 `group_addr` 字段，真实 yaml 只写了 guild_addr —— 按 Python 装配会让 GROUP 频道恒降级。

**档 8 — mail**
理由：只依赖 inventory gRPC，无 kafka、无配置表，effort L。可以提前到档 5 并行。

**档 9 — matchmaker**
理由：依赖 team（BeginTeamMatch）+ player_locator（在线闸）+ ds_allocator（AllocateBattle）。ds_allocator 不迁 → matchmaker 只能混跑调用 Go 副本。

**档 10 — battle_result**
理由：依赖面最广（player/inventory/matchmaker/ds_allocator/mail/mission）。同时承载四类硬约束：六表原子结算事务、Redis WATCH/EXEC 的 25 谓词 fencing 链 + etcd capability、八条各有独立故障域的出箱循环、一整套幂等键/时序常量。**进度水位 CAS + settleProgressStreamTx 必须整体迁、整体测**，拆开迁一半会产生"Go 已修好的 bug 被移植回来"的形状（与 owner 那次同构）。

**档 11 — login（最后）**
理由：唯一有 11 个 `google.api.http` REST 端点；唯一同时承载 MySQL 定序权威 + Redis 会话权威 + 5 个 gRPC 外部依赖 + 两套 JWT 信任域（HS256 会话/账号 + RS256 DS 票 v2 JWKS）+ etcd capability 租约。29 道闸里 25 道 fail-fast，其中 6 道是 schema/后端语义探针。前置基础件：`pkg/auth`(dsticket 660 行)、`pkg/dsauthfence`、`pkg/cellroute`、`pkg/dbguard` 探针、`pkg/internalrpcauth`。

**档 X — ds_allocator / hub_allocator：不迁**
若要迁，风险不在 RPC 数量，而在三条跨语言必须逐位一致的东西：Model-B 凭据五元组的 ACK 回显（错了 UE 静默丢**整个 Command** 含驱逐单）、teardown/departure 幂等键的 sha256 派生格式（错了变离场重试风暴）、no-show 退避的 Redis key 与指数公式（错了 matchmaker 侧惩罚静默失效）。三条 Go 侧都有历史事故背书。

---

## 6. 逐服务可执行清单

格式：`Go 行数 / RPC / 启动闸(fail-fast) / 后台循环 / 原子性载体` → 需要写的文件（按顺序）→ CONFIRMED 条数。`＋` 标记复核阶段新增、原清单没有的条目。

---

### 6.1 owner — runnable（已跑，只补可观测性）
`1779 / 5 / 8(7) / 2 / 6`｜Python 七件齐全，7 条 CONFIRMED **全部是可观测性**。

**要做的**（都在 `repo.py`）：
- `owner_transition_noop` INFO 未打（两条 no-op 早退分支 kind=idempotent_replay / same_target）— Go `owner_repo.go:125-132,:480,:520`；Python `repo.py:181-193,:203-214` 直接 return。后果：Begin 一直返回 OK 但 epoch 不动时，"权威认为已经在这台"与"权威根本没收到请求"日志上无法区分。**注意 Python 已迁了对偶的 `owner_release_noop`(:514)，这是不对称不是政策**
- `owner_admit_replayed` INFO 未打 — Go `:699`；Python `repo.py:346-350` 在 commit 后 return，走不到函数尾部的 `owner_transition_admit`
- `owner_lease_lapsed` WARN 未打（`now > storedDeadline && storedDeadline > 0`，带 prev_deadline_ms/lapsed_ms）— Go `:840-851` 明写这是 owner 侧**唯一**的失租证据；Python `repo.py:440-451` 完全没有失租检测
- `owner_lease_epoch_regressed` WARN 未打 — Go `:823-832`；Python `repo.py:442-450` 只 raise。fail-closed 方向本身迁对了，缺的是这条 WARN，而错误经 in-band code 返回、access log 只记 DEBUG
- 事件名与 reason 串全套改名（4+5 处）：`owner_transition_begun`→`owner_transition_begin`、`owner_admitted`→`owner_transition_admit`、`owner_admit_identity_mismatch`→`owner_admit_rejected`、`owner_admit_barrier_wait`→`owner_admit_barrier_not_open`；reason `record_absent|epoch_mismatch|operation_mismatch|owner_type_none|target_mismatch` → 5/5 改名且**成员集也变了**（去掉 owner_type_none、新增 phase_not_admittable，行为等价但词汇不同）。`docs/ops/player-journey-log-map.md:119-121` 按 Go 名字建的 join 会返回零行
- BeginTransition 缺 operationID 非空兜底 — Go `:538-541`（防绕过 biz 的内部调用写出无锚点记录）。**今日不可达**（biz.py:145 会铸），是纵深防御缺失
- 屏障锁范围过宽：Go `:572-573` 只在旧 owner 是 BATTLE 且 uid 非空时对旧 lease 行 FOR UPDATE；Python `repo.py:234-238` 只判 uid 非空 → 每次 HUB→BATTLE 都写锁那台 **hub 实例**的租约行（allocator 心跳持续续写、承载数百玩家的最热行），而 `compute_admit_not_before_ms` 对非 BATTLE 根本不用这个值 → 计算结果完全一致、任何单测测不出
- ＋`owner_renew_lease_slow` 完全缺失：Go `:47` 阈值常量 300ms、`:776-796` 分段计时（begin_tx/select_for_update/commit + pool_in_use/idle/wait_count/wait_ms）。`player-journey-log-map.md:127` 把它定为「大厅随机被踢」/INC-20260812-002 的判据。丢的 `pool_wait_count` 是区分"卡在拿连接"与"卡在 SQL 里"的唯一信号 —— 两种失败的修法完全不同
- ＋renew 每次都写 lease 行且重写身份列：Go `:851-859` 在 `newDeadline <= storedDeadline` 时**不发 UPDATE**，且语句只含 lease_deadline_ms+updated_at_ms；Python `repo.py:452-462` 无条件执行且 `SET pod_name, instance_epoch, release_track`，其中 release_track **无 `or` 兜底**。与上一条叠加，同时给 hub 最热行加锁 + 加写

**正控（别重查）**：5 道启动闸、审计清理循环、容量巡检、五个权威事务、来源版本判定矩阵、Release 的四条"刻意不清的列"、INSERT IGNORE 兜 TiDB 无 gap 锁、512 detail 钳制、屏障日志 5000ms 分级 —— 全部已迁并有 1053 行测试覆盖。

---

### 6.2 dialogue — runnable（补基础件接线）
`942 / 3 / 5(5) / 1 / 3`｜3 条 CONFIRMED。

**要做的**：
- snowflake `node_id_source=etcd` 未实现：整个 `snowflake` yaml 段被 pydantic `extra` 吞掉（`config.py:147-157`），`main.py:125` 直接 `Node(cfg.node.node_id)`。而 `run/cluster/etc/dialogue.yaml:34-39` 明写 `node_id_source: etcd` + 4 项配置，**读都没读**；同文件 node_id 恒为 1 → 多副本全部用 1 发号。撞号的唯一观测面（`create` 撞键 WARN）是**进程内**内存 map，跨副本撞不到一起
- static node_id 号段校验:Python 接受 0（见 §4.5）
- 配置表批次只校验 dialogue 一张表：Go `configtable/store.go:77-108` 是**整批**语义（逐表验 proto 名 + sha256 + 注册的 31 张表一张不少 + `validateCrossTables` 全批外键），Python `configtable.py:266` 只读 `name=="dialogue"` 那一条。dialogue 自己不参与跨表外键，所以坏批次进来时 Python 会**正常启动、正常服务** —— 丢的是"每个服务启动即对整批体检"这层冗余，而这层正是 dist 手改事故被抓住的方式
- ＋服务端 RPC 超时未接线（见 §4.10）
- ＋cellroute 未建模（与 snowflake 同构的静默吞段）

---

### 6.3 data_service — partial（M）
`1037 / 3 / 7(6) / 2 / 3`｜biz + 乐观锁写路径已完整，4 条 CONFIRMED。

**要写**：`data.py` 改造 → `budgets.py` → `conf.py` → `main.py` → `service.py`。**7 道闸**（6 fail-fast）。

- 缓存格式头（§1 第 11 条）—— **唯一一条会"读到错数据"级别的**，虽然实测是双向 miss
- 串号防御：pd.player_id 必须等于 key 里的 playerID，否则当 miss 并打 `player_cache_corrupt_entry(reason=player_id_mismatch)` — Go `cache.go:160-165`
- `player_cache_corrupt_entry` WARN（魔数/长度/位图全过了才 Unmarshal 失败 = 真坏档）— Go `cache.go:152-157`。Python `data.py:188-193` 静默 `return None, False`
- 容量巡检的**预算数值**未迁：Go `data/budgets.go:16-23`(player_data 30万行 / avg_row 4KB)，注释明说 proto2mysql 把 string 建成 MEDIUMTEXT、**MaxAvgRowBytes 是唯一的自动告警手段**。循环范式已有（owner/main.py:98-119）可照抄，缺的是 budgets.py
- ＋placement 日志少 `shard_key`（Go `data_sharding.go:69-74`，来自纯函数 `PlayerDataShardKey`）且路由失败静默吞（Go `:52-54` 打 DEBUG `player_data_route_failed`）
- ＋`cache_ttl` 默认 5m 未迁，且 **TTL=0 的失败方向两边相反**：Go `Set(...,0)` = 永不过期；Python `px=max(ms,1)` = 1 毫秒。同一个漏配得到"永久陈旧"与"缓存等于没有"两个相反坏结局
- ＋真 Redis 缓存实现零测试覆盖（22 个用例全用 FakeCache，`test_data_service.py:56`），这是上面三条能一直藏着的原因

---

### 6.4 leaderboard — core-only（XL）
`2192 / 7 / 11(9) / 3 / 4`｜Python 只有 `estimate.py`(107 行，纯算术 + 跨语言对拍)。7 条 CONFIRMED。

**要写**：`board_store.py`(两段 Lua) → `repo.py` → `biz.py` → `conf.py` → `service.py` → `main.py` → `budgets.py`。**11 道闸**（9 fail-fast）。

- `submitLua` 整段（5 KEYS z/t/m/s/h + 10 ARGV）— `board_store.go:176-303`，唯一写路径 `:319`。读改写序列复刻会丢更新且全返 OK
- 分数打包常量：`lbEpochMs=1767225600000`(`:42`)、tie-break 因子 1e-13、方向（降序 `real - normTs*1e-13` / 升序 `+`，`:242-243`）、`normTs<0` 归零、`unpackReal = floor(packed+0.5)`(`:104-106`)。灰度期两套打包混进同一个 `:z`，名次直接错乱且零报错。**注意同族的 `bucketOf` 已迁且有对拍**
- maxSize 截断方向：asc → `ZRANGE(zkey, maxSize, -1)`；desc → `ZRANGE(zkey, 0, n-maxSize-1)`(`:274/:276`)。写反把最优 N 名踢出精确榜，且 `Estimate` 会把他们钳到 `onBoard+1` = 显示成榜底
- `removeLua` 的直方图回扣（按 `:s` 记录的分算桶，HINCRBY -1，归零 HDEL）— `:541-569`
- 幂等键格式：`settle_idempotency_key = "lb:" + BoardKey.String()`，`BoardKey.String() = "%d:%d:%d:%s"`（period 空用 `"-"`，`:89-95`）；`grant_idempotency_key = "lb:%d:%d"`(settlement_id, entity_id，`biz/leaderboard.go:364`)。**另注 `isDupErr` 是字符串匹配 "1062"/"Duplicate entry"，Python 驱动异常形状完全不同**
- 启动闸 `reward_granter_missing`：inventory_addr 空且 `allow_noop_reward != true` 必须拒启 — `main.go:159-171`。缺了会以 NoopRewardGranter 起来、reward_log 全标 GRANTED、玩家一件没收到
- 发奖补扫 `RetryUngrantedRewards`（1m / grace 2m / 上限 200，覆盖 FAILED 与 ClaimReward 后 MarkReward 前崩残的 PENDING）— `main.go:226-247`，底层 SQL `repo.go:226-227`
- ＋**MarkReward 的失败标记必须带 `AND status <> GRANTED`，成功标记刻意不带** — `repo.go:212-220`（INC-20260811-001，与 mission 同款）。无条件 UPDATE 照样成功、影响行数照样是 1，后果 90 天后才显形
- ＋四个系统 RPC 的**反向**权限门（有玩家身份才拒）：`service/leaderboard.go:40/:102/:117/:139`。极易被写成 AuthOptional 就算完事，漏掉后玩家可自己 SubmitScore 刷榜、自己 SettleBoard 触发发奖
- ＋`leaderboard_settlement` **故意不进清理范围**（`repo.go:262-266`）：settle uk 是防重复结算的永久闸。"顺手把三张表统一纳入 sweep"是最自然的补全动作，做完一切正常，几个月后重放才炸
- ＋`rewardsForRank` 取**第一个**命中 tier 就 return，不累加重叠区间（`biz/leaderboard.go:489-501`）
- ＋SettleBoard 幂等命中后 winners 必须从 **MySQL 快照**回放，不能回 Redis（`biz:288-293`，首次结算 reset_after=true 已清空榜）
- ＋`rewardPayloadMaxBytes = 1536` 写入侧字节闸（`biz:451,:465-471`，列是 VARBINARY(2048)）

---

### 6.5 player_locator — core-only（XL）
`3115 / 16(7 个是刻意下线的 stub) / 12(11) / 2 / 6`｜Python 只有 214 行纯函数，且枚举是错的。9 条 CONFIRMED。

**要写**：修 `biz.py` 枚举 → `data/location.py`(四段 Lua + WATCH/MULTI/EXEC) → `biz.py` 状态机 → `service.py` → `conf.py` → `main.py`。**12 道闸**（11 fail-fast）。

- LocationState 枚举错位（§1 第 1 条）**先修这个**
- `guardTransition` 状态机守卫整体未迁 — `biz/locator.go:450-560`。cur=BATTLE 时只放行同 match_id 续期 / MATCHING 下一局 / 带正确 match_id 令牌的 HUB 回流,default 分支 `bare_write_evicts_active_battle` 拒 LOGIN_PENDING 裸登录（`:554-558`）；另有 HUB→HUB 代际子守卫（`:471-524`）。按覆盖式写 → 断线重登把 active BATTLE 冲成 LOGIN_PENDING → matchmaker 误判空闲 → 同一玩家派到第二台 DS
- HUB presence 两步时序 `ValidateHubPresence(只读) → SetGuarded CAS → ActivateHubPresence(commit)` + commit 被拒时的 `ShrinkHubTTL` 补偿 — `biz/locator.go:288/:319-336/:339/:344-361`。拆两步的理由在 `data/location.go:156-160`
- Redis key 与 hash 字段名契约：`pandora:locator:%d` / `:hubmeta:%d` / `:lastseen:%d`(`location.go:135-150`)、location hash 9 字段(`:295-303/:744-772`)、hubmeta 6 字段。**`shrinkHubTTLScript` 硬编码比较 `state == '3'`(`:594`)** —— 与枚举错位耦合，Python 按自己那套 HUB=1 写 hash，该脚本恒返 0、断线上报永久静默失效
- ReportDisconnect 的 legacy(全零 fence)降级分支必须 Error + `HubPresenceLegacyDegraded` 计数且 RPC 仍返 OK — `:682-703`，`impact` 字段原话："no ttl shrink / no last-seen / no departure event"
- 三个时序常量：`disconnectGrace=10s`(`:658`)、`defaultLastSeenRetention=1h`(`:665`,必须远大于 team.offline_leave.threshold=180s,否则消费方查到 UNKNOWN 按 fail-closed 永不动作)、`AliveTouchThrottle=30s`(`data/location.go:795`,**节流的只是写时间戳，PEXPIRE 每次必续**)
- `touchHubAliveScript` 必须对 census 全员调用（不受 state 判定约束）且必须传满 3 个 ARGV — `data/location.go:452-462`。挪进 `state==HUB` 的 if 里 → MATCHING 到期玩家永久失去 last_alive（INC-20260813-001）；少传 ARGV[3] → 第 2 次心跳起必炸 `attempt to compare number with nil`
- SetLocation 时 MATCHING/BATTLE 必须调 TouchAlive — `:371-376`
- `ds_auth.authority_mode` 启动期取值校验（只允许 legacy|redis）— `conf/conf.go:27-35` + `main.go:78-81`
- ＋Hub DS **active credential 终态门 `CheckActive`** 整层缺失 — `service/hub_credential.go:107-200`，九个 reason 全 fail-closed、`maxActiveHeartbeatAge=30s`、`subtle.ConstantTimeCompare`。JWT 验签只证明"签过"，这里再读 Redis 唯一授权权威证明此刻仍 active
- ＋`touchHubAliveScript` 的 `EXISTS` 守卫（`:577`）：HSET 会凭空建 key，而"有内容但没有 mode 字段"的 meta 会被 `hubPresenceScript` 判损坏并 fail-closed → 给 legacy 玩家造出**永远无法接受 HUB 写的毒 key**，不会自愈
- ＋`hubPresenceScript` 的"跨 assignment 一律接受"是**刻意 fail-open**（`data/location.go:154-155,:191-194`）：归属是 hub_allocator 的权威，locator 是投影。改成 fail-closed → 玩家换 Pod 重连后 HUB 写恒被拒、presence 永久空
- ＋`coarsePresence` 降采样映射未迁（`biz/presence.go:42-51`）+ PresenceHub 后台 tick(debounce 8s / coalesce 1s)
- ＋**GetLocation 与 BatchGetLocation 对 key miss 的处理故意相反**：前者回填 OFFLINE 占位(`:582`)，后者不回填(`:600-620`)。"统一"成任一侧都无报错，但统一成回填会踩 §9.22（key miss 当成"已离开旧 DS"而放行第二台 DS）

---

### 6.6 push — core-only（L）
`2198 / 1 / 11(10) / 5 / 5`｜Python 只有 `offline.py`(246 行)，那段 Lua 逐字节对齐（已 diff 确认）。10 条 CONFIRMED。

**要写**：修 `offline.py` 坏 member → `connection.py` → `biz.py`(stream 生命周期 + 会话门三件套 + resync 闭环) → `consumer.py`(13 topic) → `wake.py` → `service.py` → `conf.py` → `main.py`。**11 道闸**（10 fail-fast）。

- 坏 member 静默跳过（§1 第 5 条）。**更正**：`test_push_offline.py:238` 对两种实现都绿，不是"反向锁死"；真正没覆盖的是 fl 记账 + 物理删除 + 折账失败扣发
- `ResyncTopic = "pandora.push.resync"`(`biz/push.go:73`，契约同步在 push.proto:69/:107)与整套 gap 时序：每页发送前预检 + 拉空终检 + `baseline = lost` 同段只信号一次 + `cursor = lostBound` — `push.go:283-370`。**resync 必须先于越过缺口的帧到达**（`:305-320` 整段注释）
- gap 检测失败 fail-closed：`LostSince` 返错时 `return cursor, lerr` 游标一步不动 — `:309-311` 与 `:353-355` 两处同写法
- `recheckSession` 裁决顺序：先代际(`ErrSessionSuperseded`→ABORTED)后到期(`ErrUnauthorized`→UNAUTHENTICATED) — `:191-211`（R5 P0-2：反了会让两台设备互踢死循环）。**错误码常量 14 已迁**(`errcode.py:90`)但无人抛出，顺序写反更没信号
- `AuthorizeAndRegister` 的 64 条带锁(`:63,:126-141`) **+ 跨 Pod 的 `sessionFenceDelivery` 逐帧 fence**(`:213-229`,drainBuffer `:331` 调用) —— 两层要一起搬，只搬 asyncio 锁漏掉逐帧 fence 同样零信号
- 广播 topic(chat.world / system.notify)的帧必须 `TsMs = 0` — `consumer.go:194-202`（定向分支用 `msg.Timestamp.UnixMilli()`，两条路径确实不同）
- kafka key 解析出 0 必须毒丸；event_type 存在但非法同样毒丸不得降级 — `consumer.go:234-241`、`:362-370` + 两处调用点
- 启动期 Redis `maxmemory-policy=noeviction` 核验、CONFIG GET 失败缺省 fail-closed、Cluster **逐 master** — `main.go:183-233`（逃生阀是配置项 `allow_unverified_eviction_policy` 而非默认）
- 跨 Pod 唤醒必须**无条件** PUBLISH（不得以"本地有 slot"抑制，`SendTo` 只看索引不验活性）+ channel `pandora:push:wake` — `wake.go:30`、`consumer.go:326-336`
- `OfflineCacheMaxFrames` 默认 Go=512（`conf.go:74-76` 与 `offline.go:101-103` 双处兜底，yaml 没配 → 512 必然生效）vs Python 500，且该值同时喂写侧修剪与读侧 LIMIT
- ＋`AssignAndBuffer` 必须把 cursor 回写进入参 frame（`offline.go:228`）—— 补上实时投递路径后，同一条消息实时收和补推收的 ts_ms 会不同
- ＋cell 归属判定的**两个方向必须分开**：`known && !owned` → Poison 投 DLQ(`consumer.go:250-256`)；归属**未知** → 刻意 fail-open 继续投 + 限流 WARN(`:258-270`，注释明写"是否改 fail-closed 需按 §9.22 单独拍板")。写成同一方向在单 Cell 下都正常
- ＋四个常量：`pollFallbackInterval=30s`、`sessionRecheckInterval=30s`、`sessionFailClose=3`、`broadcastQueueSize=64`(满即丢是显式契约、离线不补推)

---

### 6.7 inventory — core-only（XL，最大解锁点）
`7320 / 28(23 inventory + 5 bag) + ConfigTableAdmin / 18(17) / 5 / 20+`｜Python 只有 `settle.py`(157 行，2 个 RPC 的入参校验层)。15 条 CONFIRMED。

**要写（按此顺序）**：`conf.py` + 18 道闸 → `data/inventory_repo.py`(claimLedger + 指纹族，所有写路径的底座) → escrow 三件套 → `data/inventory_instance.py` / `inventory_transfer.py` → `biz.py` → `service.py` → `main.py`；**bag 域(5 RPC + 7 表)可整体推迟**。

- **指纹族整族未迁**（9 个手拼 canonical 串 + hashHex）— `inventory_repo.go:238-325`。Python 只迁了幂等键没迁指纹。两个方向都无形状：写不一致 → 玩家道具发不出去；不写不比对 → 同 key 复用到不同内容被当"已处理"静默 no-op。**枚举不全**，还有 `EscrowOutFingerprint`(`inventory_transfer.go:44`)、`TransferClaimFingerprint`(`:55`)、`bagcap|...`(`bag_capacity.go:47`)、`bagEntryFingerprint`(`bag_repo.go:214`)
- `SellFingerprint` **刻意不含售价 gold**（唯一一个"少拼一个字段才对"的指纹，`:274-276`）+ `legacySaleLedgerMatches` 用 `fmt.Sscanf` 从 detail 反解首次 gold 的旧行回收路径(`:398-421`)
- `SettlePlayerTrade` 全局锁序：两条 ledger 按 player_id 升序(`:815-819`)、同玩家道具行按 item_config_id 升序**归并成单趟**(`:845-862`)、金币放腿尾、两腿按 player_id 升序(`:885-887`)。"先扣完再加"会让方向对调的两笔并发死锁
- `EnsureAuctionEscrow` 的 **1062 ≠ 幂等成功**协议：冲突后必须先 Rollback，再以**新事务** FOR UPDATE 严格核对整行，最多 3 次 — `:944-1005`。auction 侧确有实调用(`auction/data/settlement_client.go:78`)
- `bag_meta.owner_epoch` 单调 CAS fencing(`bag_repo.go:113-140`) —— §9.22 脑裂防线的存储侧最后一道，Go 特意在此加显式 WARN 留证
- `SweepJournal` 的删除资格谓词：必须 `INNER JOIN bag_checkpoint` 且 `journal_seq <= covered_journal_seq`，**时间只是附加条件** — `bag_repo.go:504-531`（INC-20260722-003）。按时间删是不可逆的数据丢失，炸在下一次 LoadBag
- `validBagGameplayAttr` 装备词条硬包络：只准 attr 3/9(flat ≤1_000_000)与 7(基点 ≤10_000) — `biz/bag.go:38-49`，消费点 `:263` 与 `:399`（原报告行号有误）。对象是**被攻破的 DS**
- `sectionAddItems` 三段式：`cnt >= limit` 脏堆**跳过且不做减法**、容量门按 MaxStack **向上取整**、全程 uint64 — `data/bag_apply.go:141-224`
- 未配置即 fail-closed 两处：`capacity(bagType)==0`(`bag_apply.go:91-93`)、`maxStackOf(config)==0`(`:165-169`)。"没配就用默认值"是最自然的兜底写法
- `AppendJournal` seq 三条语义：批内严格升序 → ErrBagSeqConflict；`seq <= 水位` 当重放**跳过单条**；`applied==0` 时提交并返回**当前水位** — `bag_repo.go:324-330,:390-395`。返 0 会让 DS 永久重发
- `PurchaseCapacity` 两步 saga：`tier = 服务端读到的 purchases + 1`、先 trade 库 Charge(`key="bagcap:<bag>:<tier>"`, op="buy_capacity")再 bag 库 CAS(`purchases>=tier` 回放 / `!= tier-1` fail-closed) — `biz/bag.go:472-505` + `bag_capacity.go:28-33,:104-116`
- op 字符串常量族：`grant/use/discard/battle_consume/battle_discard/sell/sell_inst/trade_sell/trade_buy/escrow_out/transfer_claim/buy_capacity` —— 全部由本服务钉死，且 op 参与 `claimSaleLedger` 的 `storedOp == intent.op` 判定
- `rollIdentifyAttrs` 的 fail-closed 出口（catalog 已装配但 roll 为空 → ErrInvalidState，`biz/inventory.go:262-266`）+ 加权不放回抽取口径(`:282-330`)。**一旦写成 identified=true 零词条，幂等回放保证它永远不会被修好**
- 配置表适配器把 `LobbyUsable` 硬置 false，只用 `item.usable` 填 `BattleUsable` — `cmd/inventory/configtable.go:26-33`（字段名与语义不一致，按 proto 字段名迁必踩）
- `claimLedger` 的 result 快照回放（幂等命中返回**首次**快照而非现查）— `:334-352,:425-431`
- ＋`AddFingerprint` 之外还有：`AppendJournal` 的**每小时 journal 配额**（`bag_repo.go:248-261`，fail-open 方向，配额没迁 = 闸门整个消失）
- ＋`sessions` HASH **绝不能带键级 TTL**（`bag_repo.go:466-541`：新格式 connected ownership 的 ExpiresAtMs 恒为 0 → sessionMaxExpiry 恒 0 → 该键永远拿不到 PExpireAt）。顺手给它加个 shardTTL，全部已连接归属静默蒸发 → 账本空 → Hub 报满座可用 → 超额分配
- ＋`inventory_ledger` 的 `SettlementLegKey`/`AuctionLegKey` 分片对账键（`biz/inventory_sharding.go:47-49`，leg 四常量 seller_deliver/seller_receive/buyer_pay/buyer_receive）

---

### 6.8 trade — partial（M，最接近可跑）
`1447 / 4 / 9(8) / 0 / 5`｜biz/data/conf/service 四层齐全，4 RPC 全实现，Lua 一字未改，INC-20260722-001 的结算围栏三处都在。3 条 CONFIRMED。

**要写**：`data/settlement_client.py`(GrpcResourceLedger) → `main.py`(9 道闸) → 修 `service.py` 的 new_state。

- ConfirmOrder 失败路径丢 new_state（§1 第 12 条）
- 会话现行性门整层缺失（§4.1）
- `SettlementLegKey = "order_id:player_id:leg"`(`biz/trade_settlement.go:46-48`，leg 四字面量 `:31-37`)。**降级为 P3**：Go 侧唯一非测试调用点是一行观测日志(`:111`)，文件头 `:15-16` 自陈桥与去重表"由 Codex/人接"；今天真正承载去重的 `trade:settle:<order_id>` **已迁**(`inventory/settle.py:56-58`)
- ＋**GrpcResourceLedger 整个没迁 + "拒绝空跑"启动闸无承载体** — `main.go:128-142`（漏配 inventory_addr 且未显式 allow_noop_ledger → os.Exit(1)）。`conf.py:41` 有 allow_noop_ledger 字段但**全仓无人读**。漏了：订单走到 COMPLETED、audit 照发、背包金币一分没动，全链零错误码零 Error。同文件还丢了结算返回码映射(`settlement_client.go:63-78`)——不足额的订单永远收敛不到 FAILED，永久停在 SELLER_CONFIRMED 被无限重试
- ＋频率配额的原子载体与键口径（§4.2）
- ＋`trade_settlement_routing` 事件名与级别被改（§4.9）

---

### 6.9 mail — core-only（L）
`2153 / 9 / 8(7) / 2 / 6`｜Python 只有 `biz.py`(193 行，ClaimMail 直连链)。9 条 CONFIRMED。

**要写**：`data/mail_repo.py` → 补 `biz.py`(ListMail / 三个 Send / DS 三段式) → `sweep.py` → `conf.py` → `service.py` → `main.py`。**8 道闸**（7 fail-fast）。

- `defaultEnd` 把 end_ms 钳到 `now + ClaimRetentionDays` 且钳后 `endMs<=startMs` 拒发 — `biz/mail.go:610-621` + 调用方 `:506/:529`。跨服务耦合有对侧证据：`inventory/conf.go:243-247` 注释明写"不依赖本流水永久兜底"
- `SweepExpired` 的 claim cutoff：`(now - retention*day)/1000 → snowflake.MinIDAt → DeleteClaimsBefore` — `biz/sweep.go:68-75`。claim 行按雪花 ID 区间删（表里**没有时间列**，`12-mail-tables.sql:79-88`）
- `InsertPersonalMail` 的 `SELECT COUNT(*) ... FOR UPDATE` 上限闸 + 驱逐最旧 claimed — `data/mail_repo.go:404-426`。**但照抄 FOR UPDATE 在 TiDB 上关不掉这个 TOCTOU**（见 §4.8，mail 没有可锁的锚点行）
- snowflake node_id 必须与 inventory 不同 —— 静态值那一半 Python 白拿（读同一份 yaml），**etcd 那一半成立**（见 §4.5）
- `buildPayload` 发送侧校验：transfer 仅个人邮件可带(`:646-651`)、transfer 必须 instance_id≠0 且 count==1(`:653-657`)、同封 instance_id 去重(`:658-663`)、`MaxInstancesPerMail`(`:679-687`)、`MaxStackCountPerAttachment`(`:692-696`);零值必须归一化成默认值(`conf.go:137-142` + `biz/mail.go:88-95`)
- DS 三段式：`claim_key = "mail_claim:%d:%d"`(`:327-329`)、`CreateClaimIntent` 必须 **INSERT IGNORE 绝不覆盖**(`mail_repo.go:339-349` + `biz:368-385` 的 created==false 重读)、Mark 前 `escrowConsumer==nil` 拒终结(`:476-478`)。**互斥消费侧已迁**(`biz.py:103-114`)，缺的是生产侧
- `partitionExpired`:带未领附件 / **payload 解码失败**必须先归档再删(`sweep.go:96` 的 `err != nil ||` 短路),已领才直删
- 五个 RPC 的 systemOnly 兜底(`service/mail.go:171-176`,五处调用)。**owner 有更严的可照抄版本**(`owner/service.py:87-105`,带 WARN)
- 库容量巡检 + `Budgets()`
- ＋**SendPersonalMail 根本不走 defaultEnd**（`biz/mail.go:553-555` 只在 expireMs==0 时填默认，非零原样透传）→ 第 1 条那条链在个人邮件上**今天就是敞开的**，而 `claim_mail` 已经迁了，它的去重前提就压在这上面
- ＋`player_mail_claim.claimed TINYINT NOT NULL DEFAULT 1` 是承重的:`RecordClaim` **故意不写这一列**(`mail_repo.go:369-371`),全靠 DDL 默认值。显式传 0 或抄错默认值 → 每次直连领取永久钉在 in-progress,落点正好是**已迁好的** `biz.py:108-114` ErrMailClaimInProgress 分支,排查会一路怀疑 DS 三段式
- ＋`ArchiveAndDeletePersonal` 的**单事务**(`mail_repo.go:467-503`)是"不静默丢失"的真正承载体,不是分类规则

---

### 6.10 mission — core-only（XL）
`2738 / 6 / 13(11) / 4 / 7`｜Python 只有 `engine.py`(267 行纯函数)，其中两个常量是错的。10 条 CONFIRMED。

**要写**：修 `engine.py` 两常量 → 补 `_accept_into` 两条拒绝 → `data/mission_repo.py` → `biz/reward.py` → `sweep.py` → `conf.py` → `service.py` → `main.py`。**13 道闸**（11 fail-fast）。

- COMPLETE_MISSION=8 与 saturating_add cap（§1 第 3 条）
- 事实收据幂等：`INSERT mission_fact_receipts` → 撞 uk 比对 `request_fingerprint` — `data/mission_repo.go:102-127`；指纹算法 `sha256("p=%d" + 每条"|c=%d;a=%d;s=" + 逐槽"%d,")` — `biz/mission.go:768-778`，注释明写**与 proto 编码无关**（照抄 SerializeToString 就是错的）。上游 battle_progress_outbox 重试**无总期限**。`errcode.py:283` 已有码但全仓无人抛
- `acquirePlayerGuard` 必须是事务第一把锁 — `mission_repo.go:106,:154`
- `buildRewardLog` 的 equipment **冻结位**（`biz/mission.go:652`，决定用 `:stack` 还是 `:inst` 键，两键互不相识）+ `CatalogSource.Snapshot` 的**一次领域操作钉一次批次**（`cmd/mission/configtable.go:27-42`）
- `buildRewardLog` 错误通道：`reward_id>0` 但配置查不到必须 **return error 整批回滚** — `biz/mission.go:644-648`。Python 把两种"空"压成一个 None(`engine.py:159-161`)，配置缺失被当"没有奖励"跳过、任务照样置完成、无 reward_log 可补扫
- `doneReadLimit=2000` **只加在只读路径**，事务路径刻意不截断 — `mission_repo.go:170-190`（在事务路径截断 = 已完成任务可重新接取 = 重复发奖）
- `ValidateMissionCrossTables` 四条加载期门禁：比较符只允许 GE/GT(`configtable/mission.go:136-149`,LE/LT 恒达标=白送,EQ 单点=永久完不成)、`next_mission_ids` 三色 DFS 链环(`:158-186`)、装备**按整条奖励累计** ≤64(`:188-212`)、≤2000 行。**Python 的 `configtable.py` 只有 DialogueTable**
- 发奖三分键：`row.Key+":stack"` / `+":inst"` / `"quest:%d:%d"`,base=`"mission:%d:%d"`(`biz/mission.go:664`);满包转邮件必须把 `:inst` 键原样当 `instance_grant_key` 传(`reward.go:120-128`)
- 推送出箱单写者:逐轮 `pushIsLeader()` + `DeletePushOutbox` 命中 0 行立刻 WARN 并中断(`reward.go:248,:275-287`);**反向纪律**:补扫与清理刻意不选举
- ＋`_accept_into` 丢掉 Go 五条拒绝里的两条:`max_active_missions`(`biz/mission.go:425-428`)与 `(type, sub_type)` 互斥(`:429-441`)。**完成扇出的链式自动接取路径**会静默突破每玩家上限与类型互斥——单副本单事务即可触发,不需要并发。`errcode.py:275/:281` 两个码已迁但无人抛
- ＋推送事件构造层整体缺失,内含两条静默不变量:下发前**补零 + 服务端算 targets**(`biz/mission.go:678-693`,漏了客户端逐槽渲染错位)、分片软上限 `pushPayloadSoftLimit=1800`(`:38-39`,列是 VARBINARY(2048))

---

### 6.11 chat — partial（L）
`1568 / 2 / 12(11) / 2 / 3`｜biz/data/conf 三层 651 行,业务面迁得忠实,但 key 与默认值漂移。10 条 CONFIRMED。

**要写**:修 `data.py` 两个 key + `conf.py` 四个默认值 → `sweep.py` → `service.py` → `main.py` → kafka 五 producer + 三个 gRPC reader。**12 道闸**(11 fail-fast)。

- 两个限流 key + 四个默认值(§1 第 8 条),含 `non_world` 负值=显式关闭的语义(Go `conf.go:102-104` 判 `== 0`,Python 判 `<= 0`;字符串写法在 Python 是硬失败,只有裸数字才静默)
- `RunHistorySweep` 整个缺失(`biz/sweep.go:21,:36`,由 `main.go:189` 驱动)——`sweep_messages_before` 已写好但**全仓无调用点**,链上没有驱动者
- `snowflake.MinIDAt` 未迁(§4.5)
- 容量巡检 + `Budgets()`(1.35 亿行 / 768B)
- 会话现行性门(`server/grpc.go:25` + `main.go:194`;原报告的 `grpc.go:104` 行号不存在)
- 启动期 retention_mode 与严格模式 fail-fast —— 按 §2.3 转入装配清单
- ＋dbguard 的四条共性缺口(§4.6)全部在 chat 首先暴露

---

### 6.12 friend — core-only（L）
`2362 / 10 / 11(9) / 2 / 7`｜Python `repo.py`(423 行)迁了写路径主干,两条并发不变量(显式 RC + pair→player→探针 锁序)迁得很准。8 条 CONFIRMED。

**要写**:修 `repo.py` 两处(request_id 轮换 / accept 锁序) → 补 blocks 复核与反向 pending 收敛 → `sweep.py` → `budgets.py` → `conf.py` → `biz.py` → `service.py` → `main.py`。**11 道闸**(9 fail-fast)。

- request_id 轮换 + accept 锁序反转(§1 第 6 条)
- AcceptRequest 在守卫锁内的 **blocks 权威复核**(`friend_repo.go:418-429`,必须是锁定读;Python 全函数无 blocks 查询)——R5 P1-4
- AcceptRequest 收尾把反向 pending 一并置 accepted(`:466-472`,R5 P2-8)
- 频率配额(§4.2)
- `friend_pair_guards` 30 天保留期**无条件真删,不受 retention_mode 门控**(`biz/sweep.go:24-35` 一轮两条,`friend_repo.go:865-874` 签名无 mode)——R9 P1,pair 守卫随社交图 O(n²) 无上界
- 三条列表读丢时间戳且改排序口径(`friend_repo.go:518/:540/:656` 都带 `UNIX_TIMESTAMP*1000` + `ORDER BY created_at DESC`;Python 只 SELECT ID 且按 ID 升序)
- 严格模式启动断言 —— 装配清单
- ＋`conf.py` 整个不存在,九个兜底常量无处落地(`conf.go:78-105`:MaxFriends/MaxIncomingRequests/MaxBlocks 各 200、RateQuotaPerMin=10、RecommendLimit=10 硬顶 20、RequestRetentionDays=90、SweepInterval=5m、SweepBatch=500、PairGuardRetentionDays=30)。Python 把它们做成**必填入参**由调用方给,而调用方一个都没迁
- ＋`budgets.go:14-30` 容量预算表未迁(planPlayers=100_000、五张表 MaxRows/MaxAvgRowBytes)。与上一条叠加,`friend_pair_guards` 的 O(n²) 膨胀在 Python 侧将**完全不可观测**
- ＋`block()` 缺"是否已拉黑"存在性探针(`friend_repo.go:588-604`,幂等命中不占新名额)——信号是响的,记进行为对照清单

---

### 6.13 guild（含 GroupService）— core-only（XL）
`3655 / 23(14 guild + 9 group) / 11(9) / 2 / 7`｜Python 只有 `group_repo.py`(293 行),公会侧 875+229 行零迁移。6 条 CONFIRMED。

**要写**:修 `group_repo.py`(role / 锁序 / name+max_members) → 补 kick/disband/transfer/get/list → `guild_repo.py`(875 行) → `cache.py`(229 行) → `sweep.py` → `conf.py` → `biz.py` → `service.py` → `main.py`。**11 道闸**(9 fail-fast)。

- role=0 与 remove_member 锁序(§1 第 7 条)
- AddMember 的**邀请者持锁复核**(`group_repo.go:225-236`,三审 P1-9;Python `add_member` 签名里根本没有 operator_id)
- RemoveMember 禁止移除现任群主(`:281-290`);Python 取了 owner_id 却不用,直接 DELETE → 悬空 owner_id 仍通过全部权限判定(判定全是 `curOwner == operatorID`,不校验 operator 是否还在成员表)
- 公会读缓存二进制帧('PGC'0x01 / 'PGM'0x01 + BE uint32 位图长 + 字段号位图 + hashtag key,`data/cache.go:62-160`)。位图是从 descriptor **现算**的(照样现算就自然一致),真正致命的是魔数/字节序/长度前缀/hashtag 四个手写常量
- 入会申请频率配额(§4.2,Domain="guild")
- 启动期 `ValidateRequiredSchema` 物理契约门(`data/schema.go:15-40`,RequiredSchemaVersion=2)——**注意 §4.7 的陷阱**
- 严格模式与 retention_mode —— 装配清单
- ＋create_group 丢了 `chat_groups.name`(DDL `NOT NULL` 无 DEFAULT)与 `max_members`
- ＋KickMember / TransferOwner / DisbandGroup 三个方法完全不存在,各自都有与上面同族的持锁复核(`group_repo.go:315-334/:372/:435-455`),其中 `:450/:455` 又是一处 role 数值必须逐位一致的地方
- ＋list_my_groups 排序(`created_at DESC` vs `group_id` 升序)与上限(500 vs 200)漂移

---

### 6.14 player — core-only（XL）
`5470 / 28 / 27(22) / 5 / 12`｜Python 只有 89 行(`experience.py` 两个纯函数 + 对拍测试)。19 条 CONFIRMED,全服务最多。

**要写**:`data/experience_repo.py` → `mmr_repo.py` → `attribute/talent/skill_card/equipment/profile/reward_repo.py` → `biz.py` → `conf.py` → `service.py`(28 RPC 的三种鉴权分配) → `main.py`(27 道闸) → outbox publisher + kafka consumer。**27 道闸**(22 fail-fast)。

- `exp_history.uk_player_idem` 索引形状探测(§4.7,`experience_repo.go:310-341`)。**Python 已有 `assert_column_exists` 但只查列名**
- `ApplyExperience` 事务本体(players FOR UPDATE → INSERT exp_history → UPDATE players → INSERT outbox 同事务,`:89-186`)
- 满级 no-op 仍消费幂等键、落 old_exp=0/new_exp=0 收据、重放返 already=true(`:115-134`)。照直觉"满级直接 return"当前一切正常,等等级上限从 60 扩到 80,滞留的老事件重试会重新入账
- 经验事件必须走独立 topic `pandora.player.experience`(`main.go:273`)。**后果描述需更新**:当前 Go 消费者会看 event_type header 并 skip(`biz/consumer.go:31-41`),爆炸半径只覆盖真正的老二进制;缺的其实是全服务共用的 topic 常量表
- 推送出箱单写者租约 + RollingUpdate 机械门禁(`main.go:288-325`)。**Go 仓库对后果自相矛盾,见 §3.4**
- `publishPushOutboxBatch` 三条纪律:投递失败立即中断本轮、成功才删行、按 id 升序 FIFO(`biz/experience.go:314-325`,`data/experience_repo.go:209-214`)+ 满批立即续批(`:180-190`)
- `mmr_history` 幂等键**刻意不含 rating_pool**(`mmr_repo.go:148-160`,rating_pool 是载荷列不是键列)
- `ApplyMMRChange` 必须锁 `players` 守卫行而非 `player_mmr`(`mmr_repo.go:93-131`,见 §4.8)
- `battleFlags(reason)` 纯函数:win→(T,T);lose/draw→(T,F);其它**含 abandon/rollback**→(F,F)(`biz/player.go:1112-1126`)
- `rating.Normalize` 写读两侧同一归一化(trim、空→"default"、**不做大小写折叠**)——行号更正为 `pkg/rating/pool.go:38`
- `equipmentAttributeMaxValue` 白名单与上限(3/9 flat ≤1_000_000;7 基点 ≤10_000),未知/非正/超限一律**拒整份快照**(`biz/player.go:111-116,:884-894,:915-921`)
- 保留期两道闸(总闸 × 组前置)与 `gateDelete` 降级为 report_only 而非不跑 janitor(`conf.go:287-290`)。chat 的单 retention_mode 正是要防的"合成一个"写法
- `HistoryRetentionOrDefault` 的 [30,90] 钳位(`conf.go:249-260`)。**"整数溢出"那半条对 Python 无效**,只迁钳位常量
- `ExpHistoryRetentionOrDefault` 的 [7,90] 钳位(`conf.go:230-241`,下限 7 天必须覆盖 battle_result progress 出箱的**永久重试链**)
- `EnsureProfile` 的 INSERT IGNORE 语义 + created=false 后**必须回读**(`profile_repo.go:18-24` + `biz/player.go:341-373`;昵称撞 uk 时不报错、只是不插入)
- `GetPlayerNames` 契约:查不到的 id 不出现、去重去 0、200 硬截断不隐式分页(`biz/player.go:385-427`)
- 领奖记录回写必须**复用 stored message** 只覆盖两字段(`biz/reward.go:35-58`,禁止 Parse→新建→Serialize)
- 28 个 RPC 的三种鉴权逐方法分配(`service/player.go:40/:69/:86`)+ GetLoadout/GetPlayerNames 的 DS 面双门(`:177/:185/:511/:515`)。**Go 侧有机械矩阵测试 `player_rpc_boundary_matrix_test.go` 钉住,那张矩阵也要迁**
- `ExperienceCurve()` 只取 Lv1..Lv(N-1),末级 upgrade_exp=0 **不进曲线**(`pkg/configtable/player_level_exp.go:70-85`,与已迁的 `advance_experience` 的 `max_level = len(curve)+1` 咬合)+ `ValidateCurve` 整表不变量(`:25-61`)
- ＋`player_equipment.uk_player_instance` 索引形状探测(`equipment_repo.go:73-110`)——与第 1 条同构、同样静默
- ＋**等级上限防降级两道闸**:持久化等级越界探测(`experience_repo.go:350-364` + `main.go:210`)与热更不得缩短上限(`main.go:117-122`)。两道都没有时换小表 → 高等级玩家**被静默降级且级内经验清零**。原清单 19 条完全没有
- ＋default 池的 `players.mmr` 兼容列必须双写、读侧以旧列为权威(`mmr_repo.go:20-32,:54-65,:188-193`)
- ＋领奖位图的 **version 乐观锁**(`reward_repo.go:33-59` + `biz/reward.go:33,:88`):整个 ClaimReward 没有事务也没有 FOR UPDATE,`AND version = ?` 是唯一的原子性载体
- ＋player.update 消费侧 event_type 的**三态**处置(§4.3)
- ＋`sessiongate` 整层(§4.1)
- ＋`rating.MaxPoolLen=32` 的加载期拒表(`pkg/rating/pool.go:31`,与 `player_mmr.rating_pool` 列宽同源)
- ＋GetLoadout 对 inventory 明细的**四组独立整份拒**(`biz/player.go:855-905`:非精确匹配 / 重复 instance_id / 未鉴定却带词条与已鉴定却无词条的双向对称 / 同实例内重复 attr_id / slots 有实例但明细缺失)

---

### 6.15 auction — core-only（XL）
`4500 / 5 / 15(13) / 7 / 7`｜Python 只有 `submit.py`(258 行),两套枚举错位。9 条 CONFIRMED。

**要写**:修 `submit.py` 枚举 → `data/auction_repo.py`(分片 + 拓扑门) → `owner_slot_limiter.py`(两段 Lua) → `market_locker.py` → `biz.py`(撮合本体) → `conf.py` → `service.py` → `main.py` → 三条补偿循环。**15 道闸**(13 fail-fast)。

- 状态与 Side 双枚举错位(§1 第 2 条)
- `escrow_verified` 撮合准入门整道没迁:`ConfirmOrderEscrow`(`biz/auction.go:444`,!confirmed 时 fail-closed)+ `ActivateOrder` 的 `AND escrow_verified = 1`(`auction_repo.go:425-429`)+ `FindBestActiveOrder`(`:571`)+ `validReservation`(`:665`)。Python 的 OrderRecord **连这个字段都没有**,`submit.py:225-228` 把 `activated is None` 当"保持原样"并 return 成功 → 客户端拿到 order_id,订单永远停在 PENDING、对撮合不可见、资产已冻
- 撮合引擎本体三条不变量:两单必须 `ORDER BY order_id ASC` 取 FOR UPDATE(`:598-602`)、成交价取**被动挂单价** `resting.Price`(`:632`)、`validReservation` 八条前置(`:662-673`)。**整体缺失不静默(单子永不成交,联调可见);静默的是"照着搬但搬错"**
- owner 配额三件套:`reserveOwnerSlotScript`(`owner_slot_limiter.go:68-76`)、`syncOwnerSlotsScript`(`:81-91`)、`ListOwnerActiveAndPending→Sync→Reserve→prune→重试一次`(`biz/auction.go:580-622`)+ `pruneOwnerSlots` 的双重 fail-closed(`:632-643`)
- market 单写者锁 + **续租失败即 os.Exit(1)**(`market_locker.go:45-49,:190-204`,先 fail-stop 再打日志)+ TTL 硬钳 [1s,30s](`:56-61`)。**Python 通用锁已迁但没有续租循环/Extend/failStop/maxWait**,现状恰是"只搬锁不搬 fail-stop"
- 四条持久副作用补偿循环(side effect / match event outbox / expiry / **保留期清理**,`main.go:308-403`)与"事务内只登记意图、事务外幂等补齐"契约(`biz/auction.go:1014-1049`,退避 30s)
- 分片拓扑门(`data/shard_topology.go:42-95`,generation + 有序 DSN 身份哈希 exact-match)+ 逐分片严格模式 + retention_mode 拼写
- HRW 路由 `hrwScore`(`biz/market_router.go:82-106`,三个魔数 + uint32 **大端 4 字节**喂哈希 + 权重并列按实例 ID 字典序较大者取胜)。哈希分叉时**两边都不告警**(各自给自己盖章),只剩 MARKET_BUSY 变多
- ＋owner 配额 SET 的 member 编码 `"%010d:%020d"`(`owner_slot_limiter.go:46-64`)——即便照搬了两段 Lua,不补零就是两个不同成员,Reserve 重复计数、Release 永远 SREM 不掉,配额 SET 单向膨胀
- ＋`redisx.lock` 无法表达 auction 的 key 前缀(§4.10)
- ＋完全成交时 `EscrowVerified = false` 的回落(`auction_repo.go:691-701`)——只搬 Status 与 ReleasePending 不会有任何测试变红

---

### 6.16 team — core-only（XL）
`5932 / 17 / 22(15) / 5 / 9`｜Python 只有 `ready_generation.py`(101 行纯函数,迁得很准)。18 条 CONFIRMED。

**要写**:`data/team_repo.py`(4 段 Lua + WATCH/MULTI/EXEC + SETNX) → `biz/team.py` → `biz/offline_leave.py`(roster 租约 + 收据重入) → `offlinewatch`(基础件) → `conf.py` → `service.py` → `main.py`。**22 道闸**(15 fail-fast)。

- 三段 Lua:`claimInviteSlotScript`(`data/team.go:465`)、`claimApplicationScript`(`:616`,**"ZSCORE 已存在则跳过 ZCARD 上限"是重复申请幂等的唯一依据**)、`takeApplicationScript`(`:657`,先 ZREM 再判过期)。3008/3009 不在 `IsServerFault` 内 → access log 只落 DEBUG
- `deletePlayerIndexScript` 的 CAS(`:432-436`)+ biz 层 6 处清理点一律走 CAS 版;`DeletePlayerIndex` 无条件版**不在接口内、仅供测试造数**
- `UpdateWithLock` 区分 fn 业务错误(**指针相等**判定)与 `redis.TxFailedErr`(`:336-364`)。Python 的 `except WatchError` 天然区分,风险等级低于其它项
- 三个租约常量:`matchLockMinLease=2s` / `matchLockMaxLease=15s` / `matchStartReceiptWindow=60s`(`biz/offline_leave.go:748,:749,:761`,含时钟回拨按"在窗内"处理)。**误抄后果有界**:team 消费侧会钳到 [2s,15s]
- `receiptReentry` 三条件(`:1017-1025`)。**最关键的一条已实证**:matchmaker 的 operation_id 是 `"startmatch:<team>:<captain>"` 纯派生、跨局复用(`matchmaker/biz/match.go:4556-4558`),少判代际会把"下一局的开局"认成"上一局的重试"并返回**旧名单**
- BeginTeamMatch 锁内四道判定顺序:重入 → 队长 → 租约冲突 → requireReady(`:876/:890/:895/:911`),另有第五处同类顺序(`:918-921` 先留冻结快照再清 ready)
- 收据必须在**代际推进之后**盖章(`:943-957`)。Python 的 `update_team` 保留了 stamp 参数(`ready_generation.py:61,:85-86`)但**全仓零生产调用方**
- 三处方向相反的失败语义:入队闸门 fail-closed(`biz/team.go:576-608`)/ ListOpenTeams 复核 fail-open(`:1298-1316`)/ 频率配额 fail-open(`:151-160`)——三者同函数体内互指
- `joinPolicy()` 运行期解析失败退回 approval(`:1081-1092`)
- `isOpenForRecruit` 是唯一口径,写索引与读复核必须同一函数,**READY 也算招募中**(`:1134-1143`)
- CreateTeam 写序铁律:先写主体后 ClaimPlayer(`:255-287`),两处互指
- joinTeam 入队后**双向重算**就绪态(`:530-534`)——只写单方向 → State=READY 但有人没准备,matchmaker 只校验 State(INC-20260813-001)
- `maybeTouchTeam` 续期必须同时刷 `syncOpenIndex`(`:212-228`,touchInterval 15min vs active_ttl 60min)
- service 层 systemOnly + **`plog.WithPlayerID` 必须写在 systemOnly 之后**(`service/team.go:450-458`,两处独立写死 `:481-484`/`:553-556`)
- 三档可降级验签 + `ErrUnavailable` 与 PermissionDeny 的区分(`service/match_call_auth.go:42-68`)。**更正**:`auth.py:181` 有 JWT 面的 verify,缺的是东西向签名 + 重放存储
- GetPlayerTeam 的 DS 令牌门,**刻意不绑 Type/Pod/MatchID**(`service/team.go:536-541`)
- `TestNoDirectUpdateWithLockInBiz` 源码扫描契约测试(`biz/ready_generation_test.go:169-193`)——`ready_generation.py:22` 只写了纪律没有机械执行。**更正**:Python 测试是 19 个不是 6 个
- `pkg/offlinewatch` 整套(848+81+73 行)+ 软硬两档 + `compensateIfCommittedDuringRemoval`。**迁一半更危险**:漏掉 `removeOfflineMember` 锁内看到租约就 ErrDeferred 那一步(`offline_leave.go:562-566`),已消除的 TOCTOU 会重新打开
- ＋两个**共用同一 nodeID** 的 snowflake 发号器(team_id / invite_id),`main.go:121-125` 明写"禁止跨空间放进同一容器比较"
- ＋`SetInvite` 的两段写序(`data/team.go:478-510`:先 Lua 占配额位再写令牌 hash)——就算三段 Lua 逐字照抄,写反了照样静默破限
- ＋Kafka producer 的**强依赖 fail-fast**(`main.go:132-147`,必须在 gRPC Ready 之前 exit)

---

### 6.17 matchmaker — core-only（XL）
`8547 / 7 / 15(12) / 11 / 16`｜Python 只有 `presence_gate.py`(160 行)。12 条 CONFIRMED。

**要写**:`data/match.py`(3 段 Lua + 6 处 WATCH/MULTI/EXEC + SETNX) → `biz/match.py`(撮合 + start saga + 分配 saga) → `conf.py` → `service.py` → `main.py` → 11 条循环。**15 道闸**(12 fail-fast)。

- `absentBeyond` 的**两跳结构**:先 `BatchOnline` 筛不在场者,只对该子集查 `BatchLastSeen`(`biz/match.go:845-877`)。Python 的 `absent_beyond` 直接吃 last_seen dict,少一维。**产出侧就写死了这个前提**:`player_locator/data/location.go:695-702` 注释原话——玩家在线时该时刻也一直在被刷新,"调用方永远先判是否在线"。17 个测试全自己构造 dict,永远测不出
- `rosterLockOperationID = "startmatch:<team>:<captain>"` 刻意不掺时间戳(`:4556-4558`)。**陷阱**:Python 已有的 `placement.valid_operation_id` 强制 canonical UUIDv4,照抄那个基础件去铸就会得到每次都不同的 uuid
- game_mode 命名空间划分:queue/active/start:active 三个索引 ZSET 按 mode 分,ticket/match/player-claim **刻意全局**(`data/match.go:31-81`)。划反任一侧都不报错
- player claim 与 start operation 一律 `SETNX ttl=0`(`data/match.go:210-232`,签名里 TTL 形参被显式丢弃 `_ time.Duration`)+ 两个 PERSIST 脚本的滚动升级用途
- StartMatch 三步写序:CreateTicketRecord → ClaimPlayer → EnqueueTicket(`data/match.go:112-118`,僵尸自愈以"票据主体不存在"为判据)
- `CreateStartOperation` 的"派生索引失败一律返回 nil"(`:637-689`,权威 SETNX 成功即已受理)
- `expireOnce` 三个不判失败分支 + CAS 出错只有 `ErrMatchNotFound` 才清索引(`biz/match.go:4109-4145`)
- `PushMatchProgress` 的 `callerPlayerID` 恒传 0(`:4475,:4527`,原则 3 的例外)
- `rejectAbsentTickets` 恒 fail-open,与入队闸门方向相反(`:4326-4344`,**无条件 return nil,不读任何配置**)。Python 把方向做成必填参数却没记录"复查路径必须恒 True"
- 时序常量组(`:4549,:1257-1260,:4169,:4401,:1262-1271,:2431-2443`)。**两处更正**:两个"封顶"(30s/10s)都是不可达死分支,真正起作用的只有 shift 上限(4 vs 3);`rosterLockLeaseMs` 误抄后果有界(team 侧会钳)
- `liveness_gate_enabled` 默认 false 是 INC-20260724-001 之后的**回退状态**,不是还没接线(`biz/match.go:4180-4190`,第二受害面:进过 MATCHING 后 key 到期消失且 RefreshHubLocations 只 EXPIRE 不创建 → 恒判离线)
- `ratingModeForMap` 拿不到表返回 UNSPECIFIED 绝不猜 ELO(`data/ds_allocator.go:72-95`);`ratingPoolForMap` 刻意不归一化。**★ 消费侧已迁但迁反了**,见 §1 第 4 条 —— 按原理由去修会修反
- ＋`exactAllocationSnapshot` 的**七合取代际围栏**(`biz/match.go:2445-2462`):少写任一合取项 → 旧一轮的 allocator 错误覆盖新权威 → 已拉起的 Battle DS 被遗弃。积木(`placement.valid_operation_id`)齐了、拼法没迁
- ＋`advanceAllocationAbort` 对"RPC 结果未知"的 fail-closed(`:2497-2501`,保留 ALLOCATING+ABORTING 与全部 ticket/claim/active)
- ＋三段 compare-then-act Lua:`deleteClaimScript`(`data/match.go:245`)、`refreshClaimScript`(`:259`)、`persistClaimScript`(`:268`),接口注释 `:90-94` 写明为什么不能退化成"先 GET 再 DEL"

---

### 6.18 battle_result — core-only（XL）
`7425 / 4 / 20(18) / 10 / 8`｜Python 只有 `roster.py`(140 行),其中一个函数方向与 Go 相反。19 条 CONFIRMED。

**要写**:修 `roster.py` → 纯函数层(mmr / applyExpShare / 三个幂等键 / prepareTerminalRelease 谓词 / conf 全部钳位)可先迁并与 Go 对拍 → **进度水位 CAS + settleProgressStreamTx 必须整体迁整体测** → `data/battle_repo.py`(六合一事务) → `battle_auth.py`(WATCH/EXEC 25 谓词) → 八条出箱循环 → `conf.py` → `service.py` → `main.py`。**20 道闸**(18 fail-fast)。

- `should_apply_rating` 方向相反(§1 第 4 条)
- `eloDeltas` 的 `math.Round` 半值方向(`biz/mmr.go:46`)。**严重性打折**:生效 K=32 时 k/2=16.0 不是半值,要 K 配成奇数才触发;是真实移植陷阱不是当前漂移
- `dropIdempotencyKey = "battle_drop:{match}:{player}"` 与 **仅当 stack 与 instance 同时存在才加 `:stack`/`:instance` 后缀**(`biz/battle_result.go:1324-1328,:1367-1369`);同键被 mail 溢出链复用(`:1340`)
- `progressIdempotencyKey = "progress:{match}:{seq}:{player}:{kind}"`(`biz/progress.go:1104-1109`)。**消费侧 player 也没迁**,两端都没有第二道网
- `SettlementKey = "{match}:{player}"`(`biz/settlement.go:33`)。**降级 P3**:文件头自陈桥与去重表由人接,当前无消费者
- 进度水位 CAS 三条件 `WHERE last_applied_seq=? AND settled_at_ms=0 AND stopped_at_ms=0`(`data/progress_repo.go:231-245`,RowsAffected==0 → ErrUnavailable 让调用方重读也是契约)
- `settleProgressStreamTx` 两条不变量:无行必 INSERT 终局标记(`:736-743`,含 FOR UPDATE)、已 settled 原样返回(`:751-753`);**幂等重放分支也要调**(`battle_repo.go:296-301`,审计 P0)
- `DropsSuppressed = last_applied_seq > 0`(`progress_repo.go:750`,不信 DS 声明的 final_progress_seq;`finalSeq` 入参只写进对账列,移植时最容易写成 `finalSeq > 0`)
- 未知事实:先 MarkProgressStopped 成功 → 再打日志 → 才返 ErrInvalidState;失败返 ErrUnavailable(`biz/progress.go:634-644`)
- `ClaimProgressLegacy` 必须 INSERT IGNORE 不得 upsert(`progress_repo.go:153-167`;**同文件下方的 MarkProgressStopped 刻意用 ON DUPLICATE KEY**,`:168-185` 注释标了 ⚠️,是最高危的对照点)
- WATCH/EXEC receipt + `resultAuthorityMatches` 全套谓词(`battle_auth.go:196-224`,一条不多一条不少)+ CAS 重试 4 次 + **即便只续期也必须走 TxPipelined**(`:164-170`)+ `validResultCredential` 前置闸(`:226-230`)
- `applyExpShare`(`biz/progress.go:189-198`)。**立项理由要重写**:两段式公式在 Python 是噪声;真正载荷的是①向下取整②归零份额不产出箱行③`sharePermille >= 1000` 的短路
- `prepareTerminalRelease` grace [5s,2m] **两处重复**(`conf.go:273-276` 与 `biz/battle_result.go:613-615`)+ ReleasedAtMs/CreatedAtMs 归零。**只迁 grace 等于没迁**:同函数 `:569-612` 还有一串十几条 `incomplete(...)` 完整性谓词 + `AuthorizedAtMs > nowMs` 拒
- `maxDropPerPlayerHardCap = 46`(`conf.go:296-298`,由 VARCHAR(512) 列宽反推,DB 侧没有任何断言把 512 与 46 绑起来)
- `RetentionMode()` 留空 = **ModeDelete**(全仓唯一)+ [30,180] 钳位。**两点更正**:未配置直接取**上限**180 而非中值;Python `dbguard.py:216` 的文档串已把这个例外写进移植者必经之处
- SaveResult 幂等重放必须用 `authoritativeRecoveryPlayerIDs` **绝不信本次重复 payload**(`battle_repo.go:280-292`)且必跑 settleProgressStreamTx
- `isolatedProgressAction`:action 独占一批 + count∈(0,1000] + **data 层重复校验比 biz 层多查三项**(`progress_repo.go:198-207`)
- 每场模式以水位行存在性固化 + killswitch fail-closed 裁决顺序(`biz/progress.go:274-354`,首读 4 分支 + 重读 4 分支同序再判)
- `authority_mode=redis` 时禁止订阅 `pandora.battle.result`(`conf.go:259-283`,该函数还捆了另外三条 fail-fast,是一个函数不是一行)
- ＋**不计分时必须强制 `mmr_delta=0` 而不是"跳过 assignMMR"**(`biz/battle_result.go:427-440`,assignMMR 是无条件覆写)。写成 skip → DS 请求体自带的 mmr_delta 原样流进出箱,失陷 DS 在一局 PVE 里就能给自己加任意段位。**比第 1、2 条严重一个量级,却不在原清单里**
- ＋授权同步路径**禁止 ABANDONED**(`:456-465`,过了全套 receipt 校验的 DS 只要把 outcome 设成 ABANDONED 就能拿到"delta 全 0 + 跳过掉落规则")
- ＋战内物品台账 CAS 的 `picked_count - spent_count >= ?` 谓词(`progress_repo.go:392-396` + 回滚 `:601-603`)
- ＋`ValidateProgressSchema` 对 `stopped_at_ms` 的**列属性**核对(`:686-703`,错类型/可空/坏默认会让第 6、10 条同时静默失效)
- ＋`buildDropOutbox` 的 `def.Droppable` fail-closed 过滤与按 `def.Equipment` 劈成两路(`:806-869`)

---

### 6.19 login — none（XL,最后)
`8986 / 10(+11 个 REST 端点) / 29(25) / 5 / 13`｜Python 侧**连目录都没有**。15 条 CONFIRMED。

**前置基础件**:`pkg/auth`(dsticket 660 行 RS256+JWKS)、`pkg/dsauthfence`、`pkg/cellroute`、`pkg/dbguard` 探针、`pkg/internalrpcauth`、`pkg/sessiongate`。

**29 道闸**(25 fail-fast),其中 6 道是 schema/后端语义探针(严格 sql_mode、6 表存在、3 处列形状、TiDB 版本、collation 行为)——这些探针的存在本身就是"不做会静默产生坏数据"的证据。

- `setIfNewerGenScript` 四态返回码 0/1/2/-1(`data/account.go:296-311`)。**漏掉 return 2 那一支**:go-redis 对结果不确定的命令自动重试,重试时若不认同 (jti,gen) 为幂等成功,会把自己已确认的写误报成被并发登录顶掉
- `fenceFailedSetScript` 按 `generation <= failedGen` fencing(failedJTI 不参与判定)+ 一次 HDEL 8 个字段(`:406-421`)
- admission marker 串 `admission-v4|<attempt64hex>|<credential64hex>|<accepted_at_ms>|<replay_until_ms>` + **时间戳一律取 `redis.call('TIME')`**(`:490-529`)。任一处写错走 return 3,而 `pkg/middleware/logging.go:37-45` 只对 IsServerFault 升 ERROR → 线上默认级别下"玩家进不去 DS"后端一行日志都没有
- `AdmissionAttemptOwner` / `AcceptedCredentialHash` = sha256(json) hex,**两者字段集刻意不同**(`data/ds_admission.go:66,:92`)。＋第三个逐位差异点:Go `json.Marshal` 默认对 `< > &` 做 HTML 转义
- `hashAccount = sha256(lower(trim(account)))[:8]` 的 16 hex(`data/login_ratelimit.go:43-47`,归一化对齐 utf8mb4_0900_ai_ci + NO PAD)
- `LockRemaining` 两维度独立读、各自 fail-open、聚合 err(`:54-79`)
- `SweepPlayerNo`:READ COMMITTED + 双计数器 FOR UPDATE + `NOW() - 10s` 水位 + 逐行 `RowsAffected==1` 复核(`data/player_no.go:117-203`,go_ref 订正)
- `persistMaxAttempts=3` 有界重试 + 耗尽返 **ErrUnavailable 而非 ErrInternal**(`data/session_generation.go:46,:124-140`)
- `ErrCommitAmbiguous` 与 `resolveAmbiguousSessionGeneration` 三态(`biz/login.go:2356-2386`,分支②的"零补偿"必须逐字照抄)
- `fenceUnresolvedSessionGeneration` 的"MySQL 墓碑未命中就绝不碰 Redis"(`biz/login.go:2329`,与普通 sessions.Set 失败路径**相反**)
- `strictBattleGateProfile() = requireHubAssignmentBinding || rs256DSTicketProfileEnabled()`(`biz/login.go:353-355`,5 个消费点)
- `resolveResumeFromOwner`:querier 为 nil 或出错**一律 WAIT**,绝不回落 locator(`biz/owner_query.go:97-128`)
- 三个时序常量:`ownerLeaseSkewMarginMs=2000` / `ownerRetryAfterCeilingMs=10000` / `ownerUnknownRetryAfterMs=1000`(`biz/owner_query.go:43-57`,不是配置项、无默认值兜底)
- EnterRole 的归属回查 + 封禁按 `accounts.FindByAccountID` 回查主角色(`biz/account_role.go:333-403`)
- Redis key 的 hash tag:`{pod}` / `{matchID}` 四处带、`pandora:sess:<pid>` 不带(`data/ds_admission.go:146-155`,同槽 MGet 实证在 `:191/:247`)
- ＋`RecordFailure` 达限布锁后必须**清零该维度计数器**(`login_ratelimit.go:95,:109`):计数窗 15m 长于锁窗 5m,不清零则攻击者以"每锁一次失败"把目标长锁到整个计数窗,共享 NAT 下连坐锁死同 IP 正常玩家(§9.20)
- ＋开关依赖门禁 fail-fast(`main.go:282-288`):`(session_generation_enforce || require_ticket_sjti)` 为真而 sessionRepo==nil 即 exit —— 安全开关静默变形为"永不强制"
- ＋配置耦合校验(`conf.go:325-345`):`authority_mode=redis` 强制 `require_hub_assignment_binding=true` 且 `sameFence(DSAuth.Fence, Login.HubAssignmentFence)` 完全相同(错误文案自陈 single capability lease)。两把 fence 分叉 = 两份能力租约 = 整套设计要防的脑裂
- ＋fence 失租即刻自杀(`main.go:456-461`)
- ＋两条启动探针的 fail-fast / fail-soft **方向刻意相反**:strict mode 失败 → exit;`EnsurePlayerNoCounter` 失败 → 只 Errorw 并停用补号任务、**不拦启动**(`main.go:161-164`,编号是展示功能)。反过来会让一个纯展示功能拦死整个登录服务
- ＋`admissionReplayWindow = 30s` 硬编码(`data/account.go:469`,不是配置项,没有任何地方会因取值不同而报错)

---

## 7. 我没能确认的部分 / 覆盖边界

### 7.1 UNCLEAR 状态：0 条
本轮 229 条待核项全部落在 CONFIRMED / REFUTED / ALREADY_MIGRATED 三档,**没有 UNCLEAR**。但 9 个服务的 `uncertain` 字段里有 40 余条"证据不足未立项"的观察,其中值得下一轮补证的:

| 服务 | 问题 | 需要什么证据 |
|---|---|---|
| inventory | `bagEntryFingerprint = sha256(proto.Marshal(entry))` 的跨语言字节确定性 | 实跑对拍:同一逻辑 entry 在 Go/Python 两侧的序列化字节是否逐位相同(unknown fields / map 序 / 默认值省略) |
| inventory | `isDupErr` 用错误串包含 "Error 1062" 匹配(`inventory_repo.go:1281`) | Python 驱动(aiomysql/asyncmy)的 IntegrityError errno 形状,以及是否会被自然写对 |
| owner | `repo.query()` 是唯一既不 commit 也不 rollback 的路径,连接池 autocommit=False | asyncmy 的 `pool.release` 是否自动 rollback;TiDB 上长开只读事务是否撞 GC life time |
| owner | renew 无条件重写 `release_track` | 是否有读取方(目前只有 `readLeaseDeadline` 读 lease_deadline_ms) |
| ds_allocator | `pkg/releasetrack.New(canary_percent, canary_seed)` 是否按 match_id 哈希分流 | 读 `pkg/releasetrack` 实现 —— 若是,则属"必须逐位一致"类 |
| ds_allocator | Agones 在删除宽限期内 exact DELETE 的确切返回 | 决定 `gs.Deleting` 跳过是"浪费一次 409"还是更糟 |
| player | `pandorapy/configtable.py` 是否计划复用同一个 Store 抽象 | 从代码看不出来;影响 6 个服务的配置表策略 |
| player | `dbguard.sweep_table` 的 where 拼接是否与 Go `SweepTable` 共用同一条件 | §9.24 要求报告与实删共用同一 where + 同一组参数,未逐字节核对 |
| mail | `mail-dev.yaml` 与 `mail-dev-tidb.yaml` 的 collation 差异(utf8mb4_bin vs 0900_ai_ci)对 Python 侧影响 | 注释断言语义无差但未实测 |
| ds_allocator | 三个独立二进制(`battle_auth_quarantine` 111 行 / `gmctl` 169 行 / `pod_uid_acl_cleanup` 427 行)+ `internalpoduidpreflight` 1499 行 | 是否属于本次迁移范围,没有依据可判 |
| player_locator | 7 个 placement RPC 是 Go 侧刻意下线的 stub(统一返 ERR_SERVICE_DISABLED) | 是否还有调用方在打这些方法 |

### 7.2 这份报告的覆盖边界

1. **只覆盖 21 个 `services/` 下的服务**。`pkg/` 基础件是从服务侧反推出来的清单(§4),没有对 `pkg/` 做独立的逐文件清点 —— 例如 `pkg/offlinewatch`(848+81+73 行)、`pkg/placement`、`pkg/redislock`、`pkg/configtable` 的 31 张表定义,只知道"缺",不知道内部还有多少条同级不变量。
2. **未跑任何真实对拍**。所有结论来自静态 grep/读码 + Go 注释,除三处例外:data_service 的 protobuf 解析实测(`PDC\x02` 必抛 DecodeError)、inventory 与 push 的 Lua 逐字节 diff、chat 的 DDL 字符/字节语义核对。**§4.10 的 proto 序列化确定性完全未验证**。
3. **部署层与 CI 未覆盖**:Envoy route(login 的 11 个 REST、player 的 :8444 DS 面、mail/leaderboard 的精确 403)、`gen_cluster_config.ps1` 的 `-Prod` 机械置位(session_gate.require / DsSecretServiceNames)、`push_writer_lease_manifest_test.go` 这类"机械门禁的另一半在测试里"的约束 —— 这些在 Go 侧承担了真实的安全责任,迁移后由谁承接**没有结论**。
4. **原始清单的行号有约 15 处偏差**,复核已逐条订正(如 login `player_no.go:6→:117`、matchmaker `ratelimit.go:30→:31`、chat `grpc.go:104` 不存在、mission `bag.go:288-292→:263/:399`、owner `owner_repo.go:611→:572`、ds_allocator `hub_capacity_ledger.go:929→:934`)。**引用前请以本报告的订正值为准**。
5. **未做的一件事**:没有把 213 条 CONFIRMED 按"漏了会怎样"的严重度重新排序。§1 的 12 条是我唯一给出的严重度断言(依据是"已落码 + 有测试锁死"这个客观形态),其余按服务组织。真正的 P0 排序需要结合各服务的上线时间表,不是静态审计能给的。
6. **两个 allocator(31,582 行)只做了清点没做迁移设计**。它们的 25+29 道启动闸、9+8 条后台循环、12+12 个原子性载体在报告里是完整的,但既然文档建议"永不迁或最后迁",我没有为它们规划文件顺序。