# inventory

> 经济 / 背包服务:玩家货币 + 可堆叠道具 + 装备实例的**权威持久化**,大厅态发放 / 使用 / 出售 /
> 鉴定/丢弃/出售,拍卖与玩家交易的**原子结算 + escrow 冻结**,以及邮件 transfer 附件实例托管;可选承载
> 背包域(`pandora.bag.v1`,bag phase 1)。全部写走 MySQL 本地事务 + `inventory_ledger` 幂等键
> (不变量 §9.7)。战斗内即时效果走 UE GAS；对应持久消费/丢弃由 battle_result 事实出箱调本服
> 系统 RPC 最终扣减，绝不复用大厅 `UseItem`。
>
> 本 README 是**模块级说明**(职责 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见
> `docs/design` 的 [`bag-domain.md`](../../../docs/design/bag-domain.md)、
> [`decision-revisit-trade-storage.md`](../../../docs/design/decision-revisit-trade-storage.md)、
> [`decision-revisit-auction-engine.md`](../../../docs/design/decision-revisit-auction-engine.md);
> owner 权威(五要件②)见 [`owner-authority.md`](../../../docs/design/owner-authority.md)。
>
> 代码行号锚点截至当前 HEAD,以**函数名**为准(行号会随改动漂移)。

## 职责与边界

- **职责**:玩家 `gold` 货币余额、可堆叠道具持有、装备实例(instance)、拍卖 escrow、邮件
  transfer 托管的**唯一权威存储**;系统驱动幂等发放(掉落 / 活动 / 购买到账)、大厅态用 / 售 /
  鉴定 / 整理;拍卖成交与玩家点对点交易的原子结算。
- **权威态**:全在 **MySQL**(`pandora_trade` 库,强依赖,落库不可降级);进程内无缓存权威、无
  内存单写循环。可选背包域权威在 `pandora_bag` 库。
- **数值权威**(不变量 §9.6):出售单价 / 鉴定随机属性 / 容量档位价格全在服务端裁决,DS 只报事实
  不报数值;发放 / 扣减在 `SELECT ... FOR UPDATE` 锁行内校验数量,杜绝并发超扣。
- **鉴权双轨**:客户端 RPC(读 / 用 / 售 / 鉴定 / 整理)以 Envoy `jwt_authn` 注入的调用者身份为准,
  **不信任请求体 `player_id`**;系统 RPC(发放 / 结算 / 冻结 / 托管)只允许后端内网直连
  (`callerID==0`),带玩家 JWT 一律拒,且不在 Envoy 暴露路由。
- **不做的事**:不算 MMR / 经验 / 掉落判定(那是 battle_result 的活);战斗内实时用道具 / 出装 /
  局内购买走 GAS,不经 gRPC(ds-arch §0.1)。

## 端口(`docs/design/infra.md`)

端口取自 `internal/conf/conf.go` 的 `Defaults()`。

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:20015` | 客户端 RPC(经 Envoy)+ 后端内部系统 RPC(内网直连) |
| HTTP | `:21015` | 仅 `/metrics`(`inventory.proto` 无 `google.api.http` 注解,无 RESTful RPC) |

## 对外接口

两套 gRPC service 共用一个进程 / 端口:`InventoryService`(`inventoryv1`)+ 可选 `BagService`
(`bagv1`,仅当 `bag.dsn` 非空时注册)。gRPC server 装 `pmw.AuthOptional()`(从 Envoy 注入的
`x-pandora-player-id` 读 `player_id`)+ `pmw.SessionCurrent`(客户端面请求 jti 必须是 login 会话
权威当前一代,顶号后旧 JWT 立即失效);内部直连不带 JWT payload,系统 RPC 天然放行。

代码入口:`internal/service/inventory.go` / `transfer.go` / `bag.go`。

### InventoryService(`internal/service/inventory.go`,`transfer.go`)

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `GetInventory` | 客户端 | 读货币 + 道具堆叠 + 容量 + 装备实例 | JWT;`callerPlayerID` 校验请求体 `player_id`==调用者 |
| `UseItem` | 客户端 | 大厅使用入口；真实 `item.usable` 当前仅表示局内 GAS，故真实表全量 fail-closed、不扣物 | JWT;同上 |
| `SellItem` | 客户端 | 出售道具换金币(单价服务端裁决) | JWT;同上 |
| `DiscardItem` | 客户端 | 幂等丢弃可堆叠道具，返回 `remaining` | JWT;同上 |
| `IdentifyItem` | 客户端 | 鉴定一件未鉴定装备实例(服务端 roll 属性) | JWT;同上 |
| `DiscardInstance` | 客户端 | 丢弃一件装备实例 | JWT;同上 |
| `MoveInstance` | 客户端 | 移动装备实例到新格 | JWT;同上 |
| `SellInstance` | 客户端 | 按 `instance_id+item_config_id` 精确出售实例，bound 拒绝，返回金币余额 | JWT;同上 |
| `GrantItems` | 内部(掉落 / 活动 / 购买到账) | 幂等发放道具 + 货币 | **拒玩家 JWT**(`callerID` 必须 =0) |
| `GrantInstances` | 内部 | 幂等发放装备实例(snowflake 生成 `instance_id`) | **拒玩家 JWT** |
| `ConsumeBattleItem` | battle_result(内部) | 按进度事实幂等扣局内消耗品 | **拒玩家 JWT** |
| `DiscardBattleItem` | battle_result(内部) | 按进度事实幂等丢弃可堆叠物；副本装备不支持 | **拒玩家 JWT** |
| `CheckItemsOwned` | player(内部,兼容) | 按配置 ID 查询持有子集 | **拒玩家 JWT** |
| `CheckInstancesOwned` | player(内部) | 按 `instance_id+item_config_id` 精确查询归属；兼容回 IDs 并同时回鉴定详情快照 | **拒玩家 JWT** |
| `SettleAuctionMatch` | auction(内部) | 原子结算拍卖成交(双方 escrow 对转) | **拒玩家 JWT** |
| `SettlePlayerTrade` | trade(内部) | 原子结算点对点交易(无预冻,双方活跃余额扣转) | **拒玩家 JWT** |
| `FreezeForOrder` | auction(内部) | 挂单冻结资产进 escrow(SELL 冻道具 / BUY 冻金币) | **拒玩家 JWT** |
| `EnsureAuctionEscrow` | auction 补偿器(内部) | 补齐旧版遗留订单的 escrow | **拒玩家 JWT** |
| `ReleaseEscrow` | auction(内部) | 退还挂单 escrow 残余(撤单 / 过期 / 完全成交) | **拒玩家 JWT** |
| `EscrowOutInstances` | mail(内部) | 邮件 transfer 附件:从源玩家扣出实例并托管 | **拒玩家 JWT** |
| `ClaimTransferInstances` | mail(内部) | 领取托管实例(原样搬进领取人实例表) | **拒玩家 JWT** |
| `ReleaseTransferEscrow` | mail 发信 saga 补偿(内部) | 托管释放回源 | **拒玩家 JWT** |
| `ConsumeTransferEscrow` | mail(内部,bag phase 2 领取链) | 消托管行不物化(资产已 journal 入包) | **拒玩家 JWT** |

### BagService(`internal/service/bag.go`,`bag.dsn` 非空时注册)

全部是内部系统接口:调用方 = owner DS(经 `:8444` DS 面直连,无玩家 JWT),`service` 层拒客户端
JWT + Envoy `/pandora.bag.v1/` 前缀 403 双保险;五要件① DS 凭据身份经 `DSCallbackGuard` 验签抽取
`pod/uid`(`resolveDSCaller`,`bag.go:51`),五要件② owner 授权在 biz 层逐写校验。

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `LoadBag` | owner DS | 加载随身组:快照 + journal 尾部 + 权威水位 + 有效容量 | **拒玩家 JWT**;DS 凭据 + owner 授权 |
| `AppendJournal` | owner DS | 追加背包流水(同步入账,前缀确认) | 同上 |
| `SaveCheckpoint` | owner DS | 保存随身组快照(仅随身段,后端驻留段整批拒) | 同上 |
| `PurchaseCapacity` | owner DS | 购买容量扩容(档位 / 价格 / 封顶服务端权威) | 同上 |
| `GetSections` | owner DS | 读后端驻留段(仓库 / 活动段,活动段按 current generation 过滤) | 同上 |

## 目录结构(Kratos 标准分层,对齐 login / matchmaker)

```
cmd/inventory/main.go            启动入口(MySQL 装配 + 可选 snowflake / bag 库 / owner 授权 + 保留期 sweep goroutine)
etc/inventory-dev.yaml           开发期配置(trade 库强依赖 + bag 库联调开启 + session_gate.require=false)
etc/inventory-prod.yaml.example  生产配置样例
internal/
  conf/conf.go                   配置结构(InventoryConf + BagConf + DSAuthConf;含 Defaults / Validate)
  service/
    inventory.go                 InventoryService(读 / 发放 / 用 / 售 / 装备实例;callerPlayerID 鉴权下沉)
    transfer.go                  邮件 transfer 附件实例托管 RPC(4 个系统接口)
    bag.go                       BagService(bagv1;DS 凭据身份解析 + 委托 biz)
  biz/
    inventory.go                 InventoryUsecase(发放 / 用 / 售 / 鉴定 roll / 拍卖 / 交易 / escrow)
    inventory_sharding.go        SettleAuctionMatch 成交后跨分片对转落点观测(nil-safe)
    transfer.go                  transfer 托管用例(扣出 / 领取 / 释放 / 消费)
    bag.go                       BagUsecase(五要件② owner 授权 + op 形状校验 + 容量购买 saga)
    bag_migrate.go               旧 inventory 存量迁移用例(D5;默认关)
    sweep.go                     inventory_ledger / auction_escrow(closed) 保留期清理(§9.24)
  data/
    inventory_repo.go            MySQLInventoryRepo(货币 / 道具 / 幂等流水 / escrow 事务)
    inventory_instance.go        装备实例仓储(player_item_instance)
    inventory_transfer.go        mail_transfer_escrow 托管仓储(两表间原子搬移)
    bag_repo.go / bag_apply.go   背包域仓储(epoch fencing / journal / checkpoint / 段应用)
    bag_capacity.go              背包容量状态(base + 已购增量)
    bag_migration.go             背包迁移幂等闸(bag_migration 一玩家一行)
    owner_authorizer.go          owner 服务 gRPC client(五要件② 授权校验)
  server/
    grpc.go                      gRPC server 注册(InventoryService + 可选 BagService + 中间件)
    http.go                      HTTP server(仅 /metrics)
```

## 核心调用链

### 1. 幂等权威写—— ledger 先行 + 锁行扣减

所有改账写共用「先 claim 幂等流水(记指纹)→ 再原子改余额 → 回写结果快照」一条骨架。以 `UseItem`
为例:

```
service.UseItem (inventory.go:104)
  └─ callerPlayerID  取 JWT 身份,校验 req.player_id==调用者(inventory.go:34)
       └─ biz.UseItem (biz/inventory.go:397)
            ├─ itemDefinition(item) 现查 configtable item 表快照,校验 LobbyUsable
            │     (catalog 未注入或表里没这行 → fail-closed → ErrInventoryItemNotUsable)
            └─ repo.UseItem (data/inventory_repo.go:405)  —— 一个 MySQL 事务
                 ├─ claimLedger (inventory_repo.go:280)
                 │     INSERT inventory_ledger(player,key,op,fingerprint)
                 │       ├─ 命中 uk(1062):读回首次指纹 + 结果快照
                 │       │     ├─ 指纹不一致 → ErrInventoryIdempotencyConflict(防 key 复用串改账)
                 │       │     └─ 一致 → already=true,回放首次 remaining(不重扣)
                 │       └─ 首次 → already=false 继续
                 ├─ deductItemTx (inventory_repo.go:371)
                 │     SELECT count FOR UPDATE → 校验 >=n → 扣减(扣空即 DELETE 行)
                 ├─ updateLedgerResult  把 remaining 结果快照写回流水(供后续幂等回放稳定值)
                 └─ COMMIT
```

- **幂等键复用防串改**:`claimLedger` 把 `idempotency_key` 绑定到请求内容指纹(`GrantFingerprint`
  / `UseFingerprint` / `SellFingerprint`,`inventory_repo.go:215+`);同 key 换不同请求内容 → 冲突拒,
  不静默当 no-op。
- **`GrantItems`**(`biz/inventory.go` → `data`):同骨架,`ON DUPLICATE KEY UPDATE` 累加
  `player_items`;货币按 kind 逐笔走 `addCurrencyTx`(锁行→比较→写绝对值),回写发放后**全币种**余额快照。
- **`SellItem`**(`biz/inventory.go`):biz 层用 `data.SafeMulCurrency` 防 `单价×数量` 溢出,
  再走 data 层「扣道具 + 加货币」同事务。响应带 `earned`(本次收入),幂等重放回放首次金额而非 0。
- **真实配置同源**:`cmd/inventory/configtable.go` 每次从热更 `Store` 读取 `item.type/max_stack/
  sell_price/usable`。`GrantItems` 拒装备、`GrantInstances` 拒可堆叠物，未知 ID 一律拒；不再使用
  `2001/3001` 样例。`usable=true` 只授权内部 `ConsumeBattleItem`，不授权大厅 `UseItem`。
- **战斗事实扣减**:`ConsumeBattleItem` / `DiscardBattleItem` 使用独立 ledger op/fingerprint，和大厅
  use/discard 不混淆。battle_result 对普通拾取仍异步出箱；对独立 consume/discard action 只有本事务
  明确完成才回成功 ACK，并以持久 outcome 处理回包丢失。
- **phase0 资产边界**:battle_result 只允许 action 支出本场已接受的同玩家/同 item stack pickup，
  不允许 DS 用合法 match 凭证扣进入本场前的主库存；副本装备实例丢弃仍 fail-closed。
- **出售热更幂等**:`SellItem` 新指纹只含 `item+count`，`SellInstance` 只含 `instance+item`；售价只
  进入首次 ledger detail/result。升级前含价格的旧指纹仅在严格解析首次 detail 并重算旧 hash
  完整一致时回放，禁止任意旧 hash 放行。

### 2. 拍卖成交结算 SettleAuctionMatch —— 双方 escrow 对转

`biz.SettleAuctionMatch`(`biz/inventory.go:366`,幂等键 `auction:settle:<match_id>`)→
`repo.SettleAuctionMatch`(`data/inventory_repo.go:525`),一个本地事务内:

```
1) 幂等流水:按 player_id 升序给买卖双方各 claim 一条同 key 流水(防并发交叉插入死锁)
     任一命中 uk → already=true,整笔回放(资产只转一次,不变量 §9.2 / §9.7)
2) 卖家腿:consumeItemEscrowTx(消卖单道具 escrow)+ addGoldTx(卖家收 totalGold)
   买家腿:consumeGoldEscrowTx(消买单金币 escrow)+ addItemTx(买家收 quantity 道具)
     两腿按 player_id 升序执行、腿内「先 escrow 后入账」→ 全局锁序一致,防角色对调死锁
3) COMMIT
```

资产已在 `FreezeForOrder` 冻结进 escrow,成交不会因余额不足失败。成交后 `logAuctionSettlementRouting`
(`biz/inventory_sharding.go`)在分片部署时额外观测跨 Cell 落点(`router==nil` 单 Cell → 不打)。

- **`SettlePlayerTrade`**(`biz/inventory.go:417` → `data:593`):P2P 无预冻,直接从双方活跃余额扣转,
  任一方不足 → `ErrInventoryInsufficient` 整笔回滚;道具行按 `player_id`、再按 `item_config_id` 升序
  「扣加合并单趟」加锁,防买卖方向对调的并发交易成环死锁(`itemOps`,`data:633`)。

### 3. 冻结 / 补齐 / 退还 escrow

- `FreezeForOrder`(`biz/inventory.go:469` → `data:691`):先 INSERT `auction_escrow`(uk
  `player+order` 命中 → already 已冻),再从活跃余额扣减(不足则整笔回滚含 escrow 行)。
- `EnsureAuctionEscrow`(`biz/inventory.go:512` → `data:739`):补齐旧版遗留订单的 escrow;INSERT
  争用 uk,胜者扣活跃资产提交,败者收 1062 后**回滚再新事务锁行严格核对**整行(1062 只是「转校验
  路径」信号,绝不等价幂等成功,杜绝两事务都扣)。
- `ReleaseEscrow`(`biz/inventory.go:564` → `data:920`):退残余到活跃余额并置 `status=closed`;行
  不存在 / 已 closed → `already` 幂等 no-op。

### 4. 邮件 transfer 附件实例托管(`internal/biz/transfer.go`,`data/inventory_transfer.go`)

`player_item_instance` 与 `mail_transfer_escrow` 各以 `instance_id` 为 PK,行只经同一 MySQL 事务
`INSERT ... SELECT + DELETE` 在两表间搬移 —— 任一时刻实例恰存在于一处,鉴定态 / 词条 / 绑定逐字节
原样(零重铸)。`EscrowOutInstances` / `ClaimTransferInstances` 复用 `inventory_ledger`(op=
`escrow_out` / `transfer_claim`)幂等;`ReleaseTransferEscrow` 幂等由托管行存在性承担(行只能删一次)。

### 5. 背包域(BagService)—— owner 授权 fencing

`service.LoadBag`(`bag.go:69`)→ `resolveDSCaller` 验签抽取 DS `pod/uid` →
`biz.LoadBag`(`biz/bag.go:101`):

```
authorizeOwner (biz/bag.go:80)
  ├─ ownerAuth==nil 且 AllowUnverifiedOwner → 采用调用方声称 epoch(仅 dev/单测)
  └─ 否则 → OwnerAuthorizer.AuthorizeOwnerWrite(player, claimedEpoch, pod, uid)
        校验当前 owner 记录 ADMITTED + 租约在效 + record.target==调用方身份 + epoch 相符
        不符 / 失租 → ErrBagEpochFenced(停写重查);查询失败 / UNKNOWN → ErrUnavailable(fail-closed)
  → repo.LoadBag(player, 生效 epoch)  返回 快照 + journal 尾部 + lastSeq
```

`AppendJournal`(`biz/bag.go:113`)先校验批量上限 / 段类型合法 / op 形状(未知 op fail-closed,
§9.21 混版纪律),再同 `authorizeOwner` 后落库;`PurchaseCapacity`(`biz/bag.go:274`)是「定档 → 扣费
(经济域同进程直用 inventory repo 的 `ChargeBagCapacity`)→ 档数 CAS 落位」两步幂等 saga。

### 6. 后台保留期清理(main goroutine)

`main` 起 `runRetentionSweep`(`cmd/inventory/main.go:239`)每 `SweepInterval`(默认 5m)跑一轮
`uc.SweepRetention`(`biz/sweep.go:31`):`DeleteLedgerBefore` 删超期 `inventory_ledger`、
`DeleteClosedEscrowBefore` 删超期 closed `auction_escrow`(active 行永不清),单批 `SweepBatch`(默认
500)`DELETE ... LIMIT` 防长事务锁表。多副本各自跑、无锁(DELETE 幂等)。启用背包域时另起
`runBagJournalSweep`(`main.go:253`)清超期**且已被 checkpoint 覆盖**的 `bag_journal`。

## 存储布局

无 Redis 权威态(Redis 仅供 `SessionCurrent` 会话现行性门只读 `pandora:sess`)。

### `pandora_trade` 库(`node.mysql_client.dsn`,强依赖)

| 表 | 键 | 用途 |
|---|---|---|
| `player_wallet` | PK `player_id+currency_kind` | 多币种余额(`amount BIGINT UNSIGNED`;无行=该币种为 0) |
| `player_currency` | PK `player_id` | **legacy** 单币种 `gold`;000005 已搬入 `player_wallet`,只读待 contract 删表 |
| `player_items` | uk `player_id+item_config_id` | 可堆叠道具持有(扣空即删行) |
| `inventory_ledger` | uk `player_id+idempotency_key` | 发放 / 用 / 售 / 结算幂等流水 + `request_fingerprint` + 结果快照(§9.24 保留期 90 天) |
| `auction_escrow` | uk `player_id+order_id` | 拍卖挂单托管(`kind` 道具/货币、`frozen_qty`/`frozen_amount`+`currency_kind`、`status` active/closed;closed 超期 90 天清) |
| `player_item_instance` | PK `instance_id` | 装备实例(`capacity>0` 启用;鉴定态 / 词条 / slot / 绑定) |
| `mail_transfer_escrow` | PK `instance_id` | 邮件 transfer 附件实例在途托管 |

### `pandora_bag` 库(`bag.dsn`,可选;启用背包域时装配)

启动期 schema gate 检查 `bag_meta` / `bag_checkpoint` / `bag_section` / `bag_journal` /
`bag_generation` / `bag_migration` / `bag_capacity`(缺表 fail-fast 指向 `14-bag-tables.sql`)。

## 关键设计点 / 不变量

| 主题 | 约束 | 代码锚点 |
|---|---|---|
| 幂等改账 | 每笔写先 claim ledger(uk),命中即回放首次结果快照,不重复入账 | `claimLedger` |
| 幂等键防复用 | key 绑请求内容指纹,换内容 → `ErrInventoryIdempotencyConflict` | `*Fingerprint` |
| 并发不超扣 | 扣减在 `SELECT ... FOR UPDATE` 锁行内校验数量 | `deductItemTx` / `deductGoldTx` |
| 结算不死锁 | 双方流水 + 资产行按 `player_id`(道具再按 `item_config_id`)升序加锁 | `SettleAuctionMatch` / `SettlePlayerTrade` |
| 数值不信 DS | 出售单价 / 鉴定属性 / 容量档价服务端裁决(§9.6) | `itemDefinition`(configtable item 表) / `rollIdentifyAttrs` / `CapacityPurchaseRuleOf` |
| 溢出安全 | `单价×数量` int64 溢出直接拒,防反加金币 | `safeMulInt64` |
| 实例全局唯一 | 实例只经同事务在 `player_item_instance` / `mail_transfer_escrow` 间搬移 | `inventory_transfer.go` |
| 背包 owner fencing | 每笔背包写查 owner 权威,epoch/身份不符或失租拒 | `authorizeOwner` |
| 鉴权下沉 | 客户端 RPC 认 JWT 身份不认请求体 ID;系统 RPC 拒 `callerID>0` | `callerPlayerID` / `rejectClientCaller` |
| 只增表有界 | ledger / closed escrow / bag_journal 周期批删,保留期 90 天(§9.24) | `SweepRetention` / `RunJournalSweep` |

## 配置项(`internal/conf/conf.go`)

默认值来自 `Defaults()`。

### `inventory.*`

| 键 | 默认 | 说明 |
|---|---|---|
| `sweep_interval` | `5m` | 保留期清理轮询间隔 |
| `sweep_batch` | `500` | 每轮每表清理行数上限(小批量防长事务锁表) |
| `ledger_retention_days` | `90` | `inventory_ledger` 幂等流水保留天数(≫ 一切重试窗口;≥ mail 可领窗口) |
| `escrow_retention_days` | `90` | closed `auction_escrow` 保留天数(active 永不清) |
| `capacity` | `0` | 装备实例背包格容量;`<=0` = 未启用实例背包(`GrantInstances` 拒) |

### `bag.*`(`dsn` 为空 = 背包域未启用,不注册 BagService,安全默认)

| 键 | 默认 | 说明 |
|---|---|---|
| `dsn` | `""` | `pandora_bag` 库连接串;空 = 未启用 |
| `owner_addr` | `""` | owner 服务 gRPC 地址(五要件②);背包域启用时必填,否则须显式 `allow_unverified_owner` |
| `allow_unverified_owner` | `false` | 跳过 owner 授权(仅 dev/单测;生产禁止) |
| `max_journal_batch` | `64` | 单次 `AppendJournal` 最大条数 |
| `max_items_per_op` | `64` | 单条 op 物品列表上限 |
| `hourly_journal_quota` | `2000` | 单玩家每小时流水条数封顶(五要件④;`<=0` = 不限,仅测试) |
| `section_capacities` | 身上 `100` / 仓库 `200` | 后端驻留段容量(未配置段 fail-closed 拒写) |
| `default_max_stack` | `99` | 可堆叠道具默认单格堆叠上限(缺覆盖时用) |
| `item_max_stacks` | `[]` | 按道具覆盖堆叠上限 |
| `capacity_purchases` | 身上 10 档 / 仓库 15 档 | 容量购买阶梯(仅 `bag_type` 0/1 可买;数值 / 封顶服务端权威) |
| `journal_retention_days` | `90` | `bag_journal` 保留天数(§9.24) |
| `legacy_migration_enabled` | `false` | 旧 inventory 存量迁移作业开关(D5;仅旧写路径冻结后开) |
| `migration_batch` | `200` | 迁移作业单轮枚举玩家数 |

### 其它

| 键 | 默认 | 说明 |
|---|---|---|
| `ds_auth.mode` | `off` | DS 回调令牌校验(五要件①);生产 `enforce`(与 battle_result / hub_allocator 同密钥体系) |
| `session_gate.require` | `false`(dev) | 客户端面请求 jti 必须是 login 会话当前一代;prod 生成器机械置 `true`(漏配拒启) |
| `node.mysql_client.dsn` | 必填 | `pandora_trade` 库(强依赖,空则启动失败) |
| `node.redis_client` | — | 会话现行性门只读 login 会话权威 `pandora:sess`(共享实例) |
| `config_table.dir` | 必填 | 与 UE 同源的 `configtable/dist`；缺 `item/equipment_affix/role_attr_map`、checksum、池引用或玩法语义均拒启 |

装备鉴定不再读取 YAML。`item.identify_pool_id` 指向 `equipment_affix`，每个池按 `weight` 加权不放回
抽取固定 `attr_count` 条并在 `[min_value,max_value]` 闭区间 roll；Store 整批热更后只影响下一次首次鉴定，
已经落库的实例不重 roll。当前正式玩法单位为：`Atk(3)` 与 `Defense(9)` 整数平加，
`MoveSpeedRate(7)` 以万分比存储(`value/10000`)；`Hp(1)`/`Shield(5)` 仍只展示，不允许进入正式池。
启动/热更 validator 会拒绝重复属性、零权重、越界数值、孤儿池和没有 UE 应用/卸载对账语义的属性。

## 本地启动

```powershell
# 1. 基础设施(MySQL:pandora_trade 强依赖;dev yaml 同时开启 pandora_bag 背包域联调,
#    生产启用前须先重放 deploy/mysql-init/14-bag-tables.sql,启动 schema gate 会 fail-fast 提示)
pwsh tools/scripts/dev_up.ps1

# 2. 启 inventory(dev 配置:trade 库 + bag 库 + session_gate.require=false)
go run ./services/economy/inventory/cmd/inventory -conf services/economy/inventory/etc/inventory-dev.yaml
```

> `bag.dsn` 留空即只跑 InventoryService(现网行为不变);实例背包(`inventory.capacity>0`)启用时
> 启动期额外检查 `player_item_instance` 表并装配 snowflake `instance_id` 生成器。

## 关联文档

- [`go-services.md`](../../../docs/design/go-services.md) — 经济域服务清单(inventory = 服务 15)
- [`bag-domain.md`](../../../docs/design/bag-domain.md) — 背包域(段类型 / 容量 / journal / checkpoint / transfer §7.1;phase 1/2)
- [`decision-revisit-bag-replay-semantics.md`](../../../docs/design/decision-revisit-bag-replay-semantics.md) — D5 存量迁移语义 / 迁移幂等闸
- [`decision-revisit-trade-storage.md`](../../../docs/design/decision-revisit-trade-storage.md) — 货币 / 道具 / escrow 存储与幂等流水
- [`decision-revisit-auction-engine.md`](../../../docs/design/decision-revisit-auction-engine.md) — 拍卖引擎 / 冻结与成交
- [`decision-revisit-auction-match-authority.md`](../../../docs/design/decision-revisit-auction-match-authority.md) — 拍卖撮合权威与结算移交
- [`owner-authority.md`](../../../docs/design/owner-authority.md) — §9.22 owner 权威 / owner_epoch(背包写五要件②的权威本体)
- [`mail.md`](../../../docs/design/mail.md) — 邮件 transfer 附件领取 / 补偿链(对接 transfer 托管)
- [`ds-arch.md`](../../../docs/design/ds-arch.md) §0.1 — 战斗内即时道具走 GAS、不经本服务的边界
- [`scale-cellular-20m.md`](../../../docs/design/scale-cellular-20m.md) §4.2 — cell 路由与拍卖跨分片对转落点观测
