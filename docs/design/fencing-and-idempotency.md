# Fencing 与幂等：机制讲解与面试问答

> **用途**：讲清本仓库 fencing（拒绝旧写者）与幂等（重复执行无副作用）的实际实现，并整理成面试可用的讲法与追问应答。对应简历中“玩家唯一归属”“可恢复进场”“资产与数据一致性”三条。
>
> **核对基线**：代码 `bdf2e6a`（2026-09-14）。本文只做解读：不变量以 `CLAUDE.md` §9（尤其 §9.6 / §9.22 / §9.23）为准，Owner 设计以 [owner-authority.md](owner-authority.md) 为准，冲突时以它们为准。
>
> **状态**：Owner Authority 整体仍处于 migrate 阶段，不能表述为全量上线。签票 / READY 前的 Begin、Hub 准入 ACK 的 Admit 已按 contract 强度 fail-closed；实例续租双写（`owner_lease_required` 默认 false）、Battle 准入、旧再入门退役仍在迁移中。

## 一、fencing 是什么

**要解决的问题**（面试官最常用的例子）：

1. DS-A 持有玩家 X 的控制权，租约 20 秒。
2. DS-A 所在机器网络分区，或进程卡顿 25 秒（GC、宿主机抖动）。
3. 租约过期，服务端把 X 交给 DS-B。
4. DS-A 恢复后**不知道自己已经过期**，继续给 X 写背包。结果要么和 DS-B 互相覆盖，要么 X 在两台 DS 上同时可玩，可以刷道具。

**只靠租约或 TTL 挡不住**：“我过期了没有”由 A 自己判断，而 A 卡住的那段时间它自己感觉不到。

**fencing 的做法**：每次授权都发一个**单调递增的号**（fencing token）。写的时候带上这个号，由**被写的一方（存储）**记住见过的最大号，比它小的写一律拒绝。关键在于**检查放在存储端，不靠写的一方自觉**。

类比酒店房卡：新客人入住，门锁的号被改大，旧房卡刷门会被**门锁自己**拒掉，不管旧客人知不知道自己已经退房。

## 二、本项目里的 fencing

### ① 玩家归属：`owner_epoch`

owner 服务给每个玩家存一行 `owner_record`，其中 `owner_epoch` 就是 fencing token。以 Hub → Battle 迁移为例：

```text
BeginTransition(玩家, expect_epoch=E, operation_id, BATTLE, 新DS身份)
  一个事务:SELECT ... FOR UPDATE 锁住该玩家这一行
    同一 operation_id 重放 / 同一目标重复投递 → 原样返回(幂等,epoch 不 +1)
    当前 epoch ≠ E → EPOCH_CONFLICT(别人先迁了,重新查询再决定,不许盲目重试)
    写入 E+1 / PENDING / 新目标 / admit_not_before(屏障时间)

Admit(玩家, E+1, operation_id, 新DS身份)   ← 由 hub_allocator / ds_allocator 代 DS 调用
    epoch、operation_id、DS 身份五元组有任何一项不相等 → 拒绝
    now < admit_not_before → 屏障未开,返回还要等多少毫秒
    PENDING → ADMITTED,新 DS 到这一步才能创建可操作的 Pawn
```

- **DS 身份五元组**：pod_name + instance_uid + instance_epoch + assignment/allocation_id + release_track。Agones 会复用 Pod 名，同名重建的 Pod 不能被当成同一个 owner。
- Admit 的来源：Hub 走准入 ACK 强路径（Admit 失败就不放玩家生成角色）；Battle 目前由 ds_allocator 按心跳里的在场名单代提交，是近似准入。
- 旧 DS 迟到的 `Release(E)` 是 no-op，只能删自己那份，删不掉新的归属。
- **为什么放在 TiDB**：fencing 的前提是号**永远不倒退**。MySQL 异步复制在主从切换时可能丢掉已经确认的写，epoch 一回退，旧 DS 又成了合法写者。TiDB 用 Raft 多数派提交，确认过的写不会回滚。Redis 同样有这个问题，所以也不能用。

### ② 真正把写拒掉的地方：背包

DS 写背包时，背包服务按 `CLAUDE.md` §9.6 的“DS 写权威五要件”（身份、owner 授权、fencing、额度、审计）拦两层。注意：DS 票据里并不带 owner_epoch（Hub 票在 Begin 之前就签好了，那时 epoch 还不知道），请求里的 epoch 可以填 0，由背包服务向 owner 查出当前值后代填。

1. **查询 owner 权威**（`owner_authorizer.go` 的 `AuthorizeOwnerWrite`，每次写都查、不缓存）：调用方 DS（凭据里的 pod + uid）必须正是记录指向的那台，owner 类型是 HUB/BATTLE，phase 是 ADMITTED，实例租约没过期；DS 若自报了非零 epoch，也必须等于当前值。任何一项不满足就返回 `ErrBagEpochFenced`；查不到 owner 返回 `ErrUnavailable`，不冒充有权。生产必须配置 `owner_addr`，只有本地 / 单测能显式跳过（缺省 fail-closed）。
2. **存储端水位**（`bag_repo.go` 的 `lockBagMetaTx`），用上一步查出的当前 epoch 做单调 CAS，教科书写法：

```sql
SELECT owner_epoch FROM bag_meta WHERE player_id=? FOR UPDATE
-- 请求 epoch < 已存 → 拒绝(租约已失效的旧 DS 迟到写)
-- 请求 epoch > 已存 → 推进水位(新 owner 第一次写)
```

第 2 层用来补第 1 层“查完再写”之间的空档，见第六节追问。

### ③ 时间屏障：处理 epoch 管不到的“双可玩”

epoch 只能拦**写到存储的请求**。分区中的旧 DS 就算不写库，内存里也可能还在模拟这个玩家（别的玩家还在跟他打），号码拦不住这种情况。

- DS 租约上限 **20s**，续不上就**自我 fencing**：关闭输入、踢人、销毁 Pawn，计时用单调时钟。
- 服务端屏障：旧 owner 是 Battle 时，`admit_not_before = max(now, 旧实例租约截止) + 7s`；旧 owner 是 Hub 或没有旧 owner 时就是 now（理由见第六节）。释放 Battle 归属时会把这个屏障留存下来，下一次迁移再取 max，防止“先释放、再迁移”把屏障塌成 0。
- **7s 余量**的构成：心跳在途 4s + 检测粒度 1s + 时钟漂移 ≥2s。2026-07-18 从 5 调到 7，因为原来的 5 秒完全没给时钟漂移留余量。
- 核心不等式：**旧 DS 最晚停止可玩的时间 < 新 DS 最早开始可玩的时间**。
- 租约按 DS 实例续，不按玩家续：几百台 DS 每 5 秒左右续一次，而不是 60 万个玩家各续各的。

### ④ 服务单写者：writer token（被问“分布式锁”时讲这个）

hub_allocator 是 Hub 分配与容量账本的单写者。平时只有 1 个副本，但滚动更新（maxSurge=1）时新旧两个 Pod 会同时在线：

- 用 etcd 选主，token 取本届 leader key 的 **CreateRevision**：历届严格递增，但不连续（etcd 全局 revision 会跳号），下游只依赖“单调”。
- Redis 里每个 pod 一个水位键 `pandora:hub:wfence:{pod}`，和业务写放在**同一个 WATCH/MULTI/EXEC** 里比较：水位比我大就拒绝（零写入），比我小就顺手推到我，相等就放行。按 pod 分键，是因为 Redis Cluster 的事务只能落在同一个 slot 里。
- 当选不等于可写：新 leader 先把**所有已知 pod** 的水位推到自己的 token，再向 etcd 做一次 TimeToLive 证明（剩余寿命 > 3s），两步都通过才对外宣告持有。
- 本地安全截止是滚动的：`证明请求发出时刻 + etcd 报回的剩余 TTL − 3s`，每 4 秒重新证明一次；etcd TTL 15s 时本地最多信 12s。过了截止 `Current()` 直接返回不持有，不等任何回调。
- 每玩家归属记录带 `writer_token` 字段；删除时写带唯一 assignment_id 的墓碑（TTL 5 分钟），不直接 DEL。否则水位跟着记录一起消失，旧写者就能重新写回去。

### ⑤ 会话 jti（顶号）

Redis 里存当前会话代际 jti，重新登录或被顶号就换一个。所有面向客户端的服务都校验请求的 jti 是否是当前的。旧会话不能再签 DS 票，迟到的 Logout 只能删掉自己。

## 三、本项目的幂等

**定义**：同一个操作执行 1 次和执行 N 次，对系统的影响一样。

**为什么必须做**：超时不等于没执行，可能已经执行了、只是回包丢了，所以调用方必须重试。Kafka 和 outbox 本身也是“至少一次”投递。所谓“恰好一次”，就是**至少一次投递 + 幂等消费**。

**四个要素**（面试时就讲这四条）：

1. **稳定的幂等键**：第一次请求前生成，重试时沿用（UUIDv4 的 `operation_id`）；或者从业务身份派生，比如 `battle_drop:<match_id>:<player_id>`。
2. **用唯一键做裁决**，不“先查再写”。先查再写有竞态：两个并发请求都会查到“没有”。
3. **去重记录和业务修改在同一个事务里**提交。
4. **重复请求返回第一次的结果快照**，并用**请求指纹**防止同一个键被拿去干别的事。

背包流水的 `claimLedger` 就是这四条的标准实现：

```text
BEGIN
 INSERT inventory_ledger(player_id, idempotency_key, 请求指纹)   -- 唯一键 (player_id, key)
  ├ 插入成功 → 第一次执行:改道具/货币 → 把结果快照写回这一行 → COMMIT
  └ 撞唯一键 → 读回这一行:
       指纹不同 → IDEMPOTENCY_CONFLICT(同一个键被用来做别的事,拒绝并告警)
       指纹相同 → 返回第一次的结果快照(不去读当前余额)
```

为什么要返回快照：比如“使用道具”要返回剩余数量，重试时如果读当前值，而这期间又用掉了一个，返回的数字就和第一次对不上了。

| 场景 | 幂等手段 |
|---|---|
| 背包发放 / 使用 / 出售 | `inventory_ledger` 唯一 (player_id, key) + 指纹 + 结果快照 |
| DS 写背包流水 | `bag_journal` 两个唯一键 (player, seq) 和 (player, key)；seq ≤ 水位的当作重放跳过 |
| 战斗结算 | `battles` 主键就是 match_id，撞键即“已结算过” |
| 段位 | `mmr_history` 唯一 (player_id, idempotency_key)，键一般就是 match_id；Kafka 重复消费没有副作用 |
| 任务奖励 / 拍卖 | `mission_reward_log`、`auction_orders` 上的唯一幂等键 |
| 进场 / 迁移 | 同一 operation_id 重放原样返回；已经 ADMITTED 的 Admit 原样返回 |
| 续租 | deadline 只前进，乱序到达的旧续租不会把租约往回拨 |

**最值得讲的完整链路：战斗结算 → 发奖**

1. DS 上报结果。battle_result 在**一个事务**里写 `battles(match_id)`、战绩和 outbox 行，一起提交。match_id 撞键就说明已经结算过，DS 重复上报不会有副作用。
2. 后台发布器轮询 outbox，调用背包发放，幂等键由 `battle_drop:<match_id>:<player_id>` 派生（可堆叠道具和装备实例两条路由各用独立子键），**发放成功才删掉这行 outbox**。
3. 如果发奖成功后、删行之前进程崩了，下次会重发一遍。这时背包流水撞键、返回快照、再删行，**不会重复发奖**。
4. 为什么要用 outbox，而不是落库后直接发 Kafka：这是“双写”问题。库成功、Kafka 失败，奖励就丢了；Kafka 成功、库回滚，就凭空发了奖。

**进场链**（对应简历“可恢复进场”）：一次真实迁移使用一个 operation_id，登录→首个 Hub、Hub→Battle、Battle→Hub 各算一次。重连、重复点击、回包丢失、服务重启都沿用原来的 operation_id。READY 推送是至少一次：全部成员推送成功，才把对局移出 active 集合；没推成功的由撮合循环补推（重签新 jti），客户端必须能容忍重复回调。

## 四、两个概念的关系（收尾时说，很加分）

- **幂等**针对“同一件事做了两遍”：第二遍无效。比较的是**键**，也就是这件事做过没有。
- **fencing** 针对“已经失去资格的人还在做事”：他做的无效。比较的是**号**，也就是你是不是最新的授权。
- 共同点：都由**存储端原子地比较并拒绝**，不依赖调用方自觉。

## 五、面试话术（理解后用自己的话说）

**fencing，约 45 秒：**

> 租约或锁过期以后，旧的持有者可能因为 GC 或网络分区根本不知道自己已经失效，还在继续写，光靠 TTL 挡不住。fencing 就是每次授权发一个单调递增的号，写的时候带上，存储端记住见过的最大号，比它小的一律拒。我们每个玩家有一个 owner_epoch，放在 TiDB 里，因为这个号不能因为主从切换而倒退。迁移时在一个行锁事务里 CAS 成 E+1、PENDING；新 DS Admit 时要求 epoch、operation_id 和 DS 实例五元组全部相等。DS 写背包时，背包服务先向 owner 核对这台 DS 是不是当前 owner、是否已准入、租约是否有效，再用当前 epoch 在背包表上做单调 CAS，旧 owner 的迟到写直接被拒。另外，epoch 管不到旧 DS 在内存里继续模拟玩家的情况，所以还加了时间屏障：DS 租约 20 秒，续不上就自己踢人、销毁 Pawn；新 DS 要等旧租约到期再过 7 秒才能准入，保证旧 DS 停止可玩一定早于新 DS 开始可玩。

**幂等，约 45 秒：**

> 超时不代表没执行，Kafka 和 outbox 也都是至少一次投递，所以接收方必须去重。我们的做法有四条：幂等键稳定，首次请求前生成、重试时沿用，或者从 match_id 这类业务身份派生；用唯一键裁决，不先查再写；去重流水和业务修改在同一个事务里提交；撞键时比对请求指纹，一致就返回第一次的结果快照，不一致就报冲突。跨服务的场景用事务 outbox 加下游幂等键。比如战斗结算在同一个事务里写 battles 表和 outbox，发布器带着 battle_drop 加 match_id 加 player_id 去调背包，发成功才删 outbox；中途崩溃重发，也会被背包流水的唯一键挡住。

## 六、常见追问怎么接

**问：用 Redis 分布式锁（SETNX 加过期时间）不就行了？**

锁只能提高效率，比如避免重复干活，保证不了正确性：持锁的进程卡住、锁过期、别人拿到锁，卡住的那个醒来还会继续写。项目里的 redislock 用 UUID 标记持有者，释放前用 Lua 校验，这只能防止误删别人的锁，防不了过期后继续写。所以正确性都落在存储端的唯一键、CAS 和 fencing 上。项目规范里也写明了：Redis TTL 和 Redlock 都不能充当 owner 权威。

**问：先查 owner 再写背包，两步之间 owner 变了怎么办？**

所以存储端还有一道 `bag_meta.owner_epoch` 的单调 CAS。新 owner 第一次加载背包（checkout）时会把水位推到 E+1，之后旧 epoch 的写在存储事务里被原子地拒绝。checkout 之前旧 DS 写进去的内容，在语义上发生在交接之前，新 DS checkout 时会读到并接着用，所以不会丢，也不会互相覆盖。这和房卡一样：门锁在新客人第一次刷卡时才更新号码。

**问：玩家在旧 DS 上拿到一件好装备，被 fencing 挡掉没写进去，玩家觉得亏了怎么办？业界标准做法是什么？**

设计上是让它不发生，而不是事后补：

1. **把“捡到”的定义反过来：入包 = 已持久化**（[realtime-progression.md](realtime-progression.md) 拾取 ACK 门控）。玩家拾取时，DS 先认领锁定掉落物（别人不能捡、暂停消失计时）并预留背包格子，再把拾取事实报给后端；后端落库后回执，DS 才把物品放进背包、销毁地上的掉落物。上报被确定拒绝时，DS 释放认领，物品回到地上。网络分区时连回执都收不到，掉落物一直锁着（防止“迟到入账 + 别人重捡”变成复制），直到 DS-A 自我 fencing 把玩家踢下线。玩家视角里他从来没“拿到过”，不存在“背包里有了又消失”。不变量：**战斗背包 ⊆ 后端已入账**。
2. **分级持久化**（[bag-domain.md](bag-domain.md) §3）：判据是“操作效果会不会被本人回档范围之外的人观察到”。拾取、交易、邮件领取、拍卖冻结这类走同步 journal，零丢失；整理格子、穿脱装备、耐久、喝药走 checkpoint（周期 5~10 秒，外加迁移、下线等关键点），崩溃最多回档几秒，物品和效果一起回退，自洽。回档只会让布局变旧（比如装备从身上回到包里），不会让已入账的物品消失（§3.2 资产守恒 + 布局容忍）。
3. **宁可丢，不可复制**：不接受 DS-A 的迟到写，是因为那等于承认两台 DS 同时有权改这个玩家，同一件装备可能被拿两次。复制道具伤害的是整个经济系统，比单个玩家的一次损失严重得多。
4. **万一还有残留，可发现、可补偿**：journal 本身就是审计流水，fencing 拒绝都有 WARN 日志（`bag_owner_epoch_fenced` / `bag_owner_authz_rejected`）。核实后走系统邮件补发：邮件是本项目唯一的离线→在线资产通道，领取带幂等键，补偿本身不会重复。

这就是业界的标准做法：**服务端权威，经济类事件先落库再给玩家看；非关键的个人状态周期存盘，接受回档；宁可回档也不能复制；出了问题凭审计日志用邮件补偿，补偿本身幂等**。项目也评估过 DS 本地 WAL，被否掉了：Agones 临时 Pod 的本地盘在 Pod 替换或节点宕机时会消失，加持久卷和回捞的成本高，仍然做不到零丢失。

**问：epoch 和租约为什么两个都要？**

epoch 拦的是“写”；租约拦的是“时间”，也就是旧 DS 在内存里继续让玩家可玩、但不落库的那部分影响。只有 epoch，可能出现双可玩；只有租约，时钟误差和迟到的写会漏过去。

**问：机器之间时钟不同步怎么办？**

DS 本地用单调时钟计时，而且本地的截止时间比服务端屏障更早。服务端的 7 秒余量里，专门留了至少 2 秒给服务间的时钟漂移。

**问：从 Hub 进战斗也要等 27 秒吗？**

不用。旧 owner 是 Hub 时屏障就是 now：Hub 的写靠 epoch 拦，双可玩靠客户端只有一条连接（切走时旧连接会被拆掉）来拦。2026-08-03 真出过问题：每次进战斗都要干等约 27 秒，客户端 30 秒的等待窗口被耗光，玩家看到的就是“匹配没反应”，之后才改成按旧 owner 类型分流。旧 owner 是 Battle 时仍然必须等，这是有意用延迟换“一人一 DS”。等待期间返回 WAIT 和 retry_after，由客户端的恢复协调器驱动重查，不会卡死。

**问：幂等键是客户端生成的，能信吗？**

格式要校验，operation_id 必须是标准 UUIDv4；唯一键是 (player_id, key)，一个玩家碰不到别人的键；再加请求指纹，防止同一个键被拿去做别的事。

**问：幂等记录要保存多久？**

必须远长于最长的重试或重放窗口，背包流水保留 90 天。删早了，迟到的重放就会重复发奖。所以上游重试没有总期限的那几张收据表，默认只统计、不删除。

**问：fencing 出过问题吗？（可以当故事讲）**

上线前的复审发现过一次险情（[INC-20260726-001](../incidents/2026-07-26-p0-hub-writer-fencing-near-miss.md)），有两个问题：

- writer lease 把“本地还没收到失联通知”当成“租约仍然有效”的证据，网络分区恢复后可能把已经失效的任期续活。修复后改成：只有拿到 etcd 服务端的 TimeToLive 证明，才延长本地截止。2026-07-29 又修正了一次：持有期间偶发一次续证失败不算“证否”，保留任期到上一次证明给出的截止，期间继续重试；只有截止真的到了，或 etcd 明确回报剩余寿命 ≤3s，才自我 fencing 并让位。否则单副本一次慢响应就会自我 fencing，造成约 8 秒没有写者。
- 写入后的补偿逻辑拿事后读到的“当前 token”去匹配刚写的记录，而不是用“这次写入用的 token”。失去 leader 后读到 0，或者读到新一届的 token，就认不出自己刚写的值。修复后改成：用本次写入时捕获的 token、完整写入值和原剩余 TTL 去定位、撤销；事后读 `Current()` 只用来判断“还是不是写入的那一届”。

教训是：**token 必须跟着操作走，不能事后再猜；“读不到证据”和“证据说已过期”要分开处理。**

## 七、要注意的边界（别说过头）

- **不要说“已经全量上线”。** owner-authority.md 里写的状态是：设计定稿，主链路已接线，仍在迁移阶段（新旧两道门并行）。面试时照这个说更稳。
- 上面那次险情是**上线前审计发现的，没有在生产发生过**，不要讲成线上事故。
- Redis 上的 writer fence 有已知的残留风险：Sentinel 或 Cluster 主从切换可能回滚已经确认的写。所以按设计，“谁拥有玩家”的最终权威是 TiDB 上的 owner 服务（目前仍在迁移阶段），hub 的分配记录只算执行细节。被问“Redis 做 fencing 靠不靠谱”时就这么答，主动讲局限反而加分。
- 拾取 ACK 门控的执行端在 UE 侧（`AMyDropItemActor` / `UMyBagComponent::ReserveSpace`），不在本仓库；背包域 phase 2（DS 写权威切换）也仍在迁移阶段，UE 侧 DS 直写背包的开关默认关闭，别说成全部落地。
- Battle 侧的 Admit 目前是 ds_allocator 按心跳在场名单代提交的近似准入，只有 Hub 有准入 ACK 强路径；DS 租约到期自我 fencing、准入前不生成可操作角色这两条，本仓库只有契约和文档，执行端在 UE 侧。
- 简历写明代码主要由 Claude Code 完成，面试官一定会验证是否真懂。要能在白板上画出 **Begin → READY → Travel → Admit → 旧 DS 自我 fencing** 的时序图。
- 需要记住的数字：租约 20s、余量 7s、屏障 27s、etcd TTL 15s（本地最多信 12s，每 4s 续证）、流水保留 90 天、DS 身份五元组。

## 八、代码位置

- [owner_repo.go](../../services/runtime/owner/internal/data/owner_repo.go)：`BeginTransition` / `Admit` / `Release` / `RenewInstanceLease`
- [placement.go](../../pkg/placement/placement.go)：DS 租约与再入屏障常量（20s / 7s / 27s）
- [owner_authorizer.go](../../services/economy/inventory/internal/data/owner_authorizer.go)：`AuthorizeOwnerWrite`（背包写入的 owner 授权）
- [bag_repo.go](../../services/economy/inventory/internal/data/bag_repo.go)：`lockBagMetaTx`（背包 epoch 水位）
- [writer_fence.go](../../services/battle/hub_allocator/internal/data/writer_fence.go)：`guardWriterFence` / `AdvanceWriterFencesForToken`
- [writerlease.go](../../pkg/dsauthfence/writerlease/writerlease.go)：etcd 写者继任租约
- [sessiongate.go](../../pkg/sessiongate/sessiongate.go)：会话现行性（jti）
- [inventory_repo.go](../../services/economy/inventory/internal/data/inventory_repo.go)：`claimLedger` / `GrantItems`
- [battle_repo.go](../../services/battle/battle_result/internal/data/battle_repo.go)：`SaveResult`（match_id 幂等 + 同事务 outbox）
- [battle_result.go](../../services/battle/battle_result/internal/biz/battle_result.go)：`dropIdempotencyKey`
- [match.go](../../services/matchmaking/matchmaker/internal/biz/match.go)：READY 至少一次推送
- [bag-domain.md](bag-domain.md)：§0 需求、§3 journal / checkpoint 分层、§3.2 重放语义、§7 邮件资产通道
- [realtime-progression.md](realtime-progression.md)：拾取 ACK 门控、§9 残余风险
