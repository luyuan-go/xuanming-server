# 多币种货币系统与 NPC 商店（2026-08-22）

> 服务级设计记录（`CLAUDE.md §7`）。本文只讲**决策与理由**；接口形状以
> `proto/pandora/common/v1/currency.proto`、`proto/pandora/inventory/v1/inventory.proto` 为准，
> 落库形状以 `deploy/mysql-init/08-inventory-tables.sql` 与
> `tools/migrate/migrations/pandora_trade/000005_multi_currency_wallet.up.sql` 为准。

---

## 1. 改造前的事实

摸底结论（不是猜测，逐条有代码出处）：

| 事项 | 改造前状态 |
|---|---|
| 货币 | 单列 `player_currency.gold BIGINT`（有符号），proto 里是 `int64 gold` |
| `CurrencyKind` 枚举 | proto 里存在，**Go / C++ 业务代码零使用**（死枚举） |
| 出售链 | 堆叠道具 `SellItem` + 装备实例 `SellInstance`，后端 → 客户端 UI **全链已通** |
| 战后金币 | `PlayerStats.gold` 只写进 `battle_player_stats` 战绩表，**从未发放到玩家钱包** |
| 主城 NPC 商店 | **整套跑在客户端**：本地 `SimulatedGold` 扣钱 + `UMyBagComponent::AddItem` 直接入包 |
| NPC 商店买入价 | 直接读道具表 `SellPrice`（**回收价**），即买价 == 卖价 |
| 交易（trade）价格上限 | **不存在**，只有一句 `if price < 0` |

后三行是这次改造里真正的缺陷，不是"需要升级的旧设计"。

---

## 2. 为什么值改成 uint64（以及它的代价）

`CLAUDE.md §5.12`：语义上不可能为负的整数默认无符号；金额属于此类。
但 §5.12 同时给了例外②「参与减法且可能下溢的字段用有符号」，所以要先确认前提：

**余额的三个减法点全部是「FOR UPDATE 锁行 → 先比较 → 再相减」**，
不存在会走到负数的路径，例外②的前提不成立，因此可以无符号。

代价是必须补三条硬纪律，否则无符号只是把静默 bug 换了个形态：

| # | 风险 | 处置 |
|---|---|---|
| ① | `UPDATE ... SET amount = amount - ?` 在 UNSIGNED 列上，非严格 sql_mode 会**静默截断成 0**（= 把"扣款失败"变成"余额清零"） | SQL 里**不出现** `amount - ?`：锁行读出→在 Go 里比较→写绝对值 |
| ② | 加法回绕会让首富瞬间变零元户 | `MaxCurrencyAmount = 2^62` 硬上限，越界返回 `ERR_INVENTORY_CURRENCY_OVERFLOW` |
| ③ | 单价 × 数量在算总价那一步就可能溢出 | `SafeMulCurrency` 溢出安全乘法 |

### 2.1 最容易漏的一类：无符号下**恒假**的旧校验

`if gold < 0`、`if price <= 0` 这类判断，在无符号类型下会恒为 false 或退化成 `== 0`，
**编译器和 `go vet` 都不报**。等于闸门被静默拆掉。改造时逐个重判：

- `biz/inventory.go` `GrantItems` 的 `gold < 0` → 改为逐币种校验（kind 合法 / amount>0 / kind 不重复 / 不超单笔上限）
- `trade/biz` 的 `price < 0` → 改为**上界闸** `MaxTradePrice`（此前全仓没有交易价格上限，
  一个 `price=-1` 的旧请求在 uint64 下解成 1.8e19，没有任何一层能拒）
- `EnsureAuctionEscrow` 的 int64 钳位 → **闸与强转一起删**。只删其一都是错的：
  留强转删闸 → 超 MaxInt64 的价格转成负数，一路穿到扣款处让 `have < n` 恒 false，把扣钱变成加钱。

### 2.2 刻意保持有符号的字段

| 字段 | 理由 |
|---|---|
| `quantity` / `filled_quantity` / `Remaining()` | 数量不是货币。`Remaining()` 是撮合循环、终态判定、min 选取的共同分母；改无符号后数据损坏会回绕成 1.8e19 → 循环永真、订单永远 PARTIAL、escrow 永不释放 |
| 拍卖撮合引擎内部 `price int64` | 订单簿用 ZSET score 表达价格优先级，买盘靠 `-float64(price)` 编成负分才能"升序取到最高价"。这正是 §5.12 例外①。符号转换只发生在 `service/auction.go priceToInternal` **一个入口**，且先判上界 |
| `mmr_delta` / 装备词条 `value` | 语义上可为负 |
| UE 蓝图视图层的全部金额 | **技术强制**：UHT 拒绝蓝图可见的 uint64（见 §5） |

---

## 3. 存储：为什么新建 `player_wallet` 而不是原地改

多币种需要 `PRIMARY KEY (player_id, currency_kind)`，旧表是 `PRIMARY KEY (player_id)`。

**TiDB 不支持 DROP 聚簇主键**（整数单列 PK 默认聚簇），`ALTER TABLE ... DROP PRIMARY KEY` 直接报
Unsupported。绕过它要走"建新表 + 搬数据 + RENAME"的换名舞，而 RENAME 序列中途被杀会留下
半迁移状态，重跑很难自愈。

因此直接新建终态表 `player_wallet`，旧 `player_currency` 留成只读存量 —— 这既是唯一在
MySQL 8 与 TiDB 上都幂等可重跑的路径，也正好是 expand → migrate → contract 的 expand 阶段。
contract（`DROP TABLE player_currency`）留给后续迁移。

### 3.1 幂等流水为什么要多两列

`inventory_ledger` 新增：

- `result_currencies`：操作后的**多币种余额快照**（pb）。旧的 `result_gold` 是单标量，
  多币种下一次操作可能同时改多个币种，只存一个数字会让重放结果与首次执行不一致。
- `result_currency_delta`：**本次变动额**。出售 / 购买的响应要回"本次获得 X / 花费 Y"，
  而幂等重放必须返回与首次执行相同的值；只存余额快照的话，重放时算不出当初那一笔是多少
  （中间可能已有别的收支）。记账本来就该记"变动 + 结果"，这一列把 ledger 补成真正的流水。

`result_gold` 保留只读兜底：pb 二进制**无法用 SQL 从整数转换**（varint 编码要在应用层做），
存量行没法就地转换。读侧按"`result_currencies` 非空则用它，否则把 `result_gold` 当金币余额"处理，
存量行的重放结果因此逐字节不变。等 `ledger_retention_days`(90) 清完存量行再走 contract 删列。

### 3.2 幂等指纹的向后兼容

`GrantFingerprint` / `AuctionSettleFingerprint` / `ChargeBagCapacity` 在**纯金币**时
仍生成旧格式字符串（`|gold=<n>`），只有出现非金币币种才用新格式。

理由：多币种上线前写下的存量流水行，指纹就是按旧格式算的。若无条件换新格式，
存量行遇到同 key 重试会被判成 `ErrInventoryIdempotencyConflict` ——
那是"同键不同请求"的**反作弊信号**，用它来报"我升级了协议"会掩盖真正的串账。

---

## 4. NPC 商店：为什么必须新建一张表

### 4.1 买价与卖价共用一列是无限刷钱

旧的客户端商店直接拿道具表 `SellPrice`（回收价）当买入价，于是**买价 == 卖价**。
在本地模拟阶段这只是"数值不合理"；但一旦买和卖**都变成服务端权威**，
它就是一条严格闭合的无限刷钱循环。

所以买入价必须与回收价分离，且明显高于回收价。当前商店表统一取回收价的 4 倍。

### 4.2 为什么不是往道具表加列

1. 同一件道具可以在不同 NPC 卖不同价（新手商店打折），道具表一行一道具，表达不了；
2. 商店的在售集合会被运营频繁增删，与道具本身的静态属性生命周期不同；
3. 道具表已有 25 列，继续加列要动既有源表版式（§9.15 热更流水线的整批风险）。

新表 `商店/d_商店.xlsx` → `pandora.config.v1.ShopTableData` → `configtable/dist/shop.json`，
与关卡 / 道具表同一条热更流水线。

### 4.3 定价链

```
客户端点击「买」
  → PurchaseShopItem{shop_id, item_config_id, unit_count, idempotency_key}   ← 刻意不含价格
  → 服务端查商店表 (shop_id, item_config_id) 取 unit_price / count_per_unit
  → 溢出安全算总价
  → 同一 MySQL 事务：扣货币 + 入包（堆叠计数 / 生成装备实例）+ 写 ledger
```

`GetShop` 把同一张表投影给客户端做展示，于是**展示口径与扣费口径同源**，
不会像旧的本地商店那样改表后两边静默漂移。

### 4.4 装备购买为什么不复用 GrantInstances

`GrantInstances` 是系统发放接口，自己开事务、自己写 ledger。
若购买"先扣钱再调它"，扣钱与发货就落在两个事务里，中间崩溃会出现**钱扣了货没到**且无补偿。
购买必须是一个事务，因此在 `shop_purchase.go` 里内联实例分配
（复用同一套 `lockPlayerInstances` / `lowestFreeSlot`，不另起一套格子分配规则）。

### 4.5 整套装备购买是**逐件**的

服务端购买 RPC 一次只处理一个道具档位，整套 6 件没有跨件事务。
与其假装原子，不如把语义讲清楚：客户端逐件串行购买，买到哪件算哪件，
中途失败即停止并明确告知已购入几件；已买到的保留（玩家为它们付过钱，回滚反而是抢东西）。

不为"整套"新增一个 RPC，是因为整套只是这个 NPC 面板的展示概念，
为它建协议会把 UI 分组固化进接口（§17「差异进表，不进接口签名」）。

---

## 5. UE 客户端：uint64 的承载方式

**UHT 允许 uint64 作为 C++ 成员，但拒绝蓝图可见的 uint64。** 证据：
`UhtUInt64Property` 的构造函数是空的（未声明 `IsMemberSupportedByBlueprint` 等 PropertyCaps），
而 `UhtInt64Property` 声明了；USTRUCT 成员 / UCLASS 成员 / UFUNCTION 参数三处都是硬 `LogError`。

沿用仓库既有的 player_id 方案，**分层承载 + 边界显式 cast**：

| 层 | 类型 | 说明 |
|---|---|---|
| wire 层（`PandoraWireTypes.h`，纯 POD 非 UObject） | 真 `uint64` | 按 proto 真实类型 |
| 蓝图视图层（`PandoraBackendTypes.h`） | `int64` + `// proto uint64` 注释 | 服务端把单币种余额钳在 2^62，恒 < int64 max，承载安全 |

`FPandoraInventory` 里 `Currencies` 是权威字段，`Gold` 是它在金币上的**只读投影**，
由解码层 `CopyCurrencies` 一并算出 —— 两者永远同源，不会出现"数组更新了但顶部金币条没更"。

---

## 6. 战后金币闭环

`PlayerStats.gold` 此前只是展示。修法是让它**搭既有的战后发放出箱**
（`battle_drop_outbox` 加一列 `currency_amount`），而不是另起一条金币发放链：
出箱表已经有幂等键、失败重试、投递成功才删行、容量预算与保留期登记，复用它等于免费拿到全套正确性保证。

金币与可堆叠道具**合并成一次 `GrantItems`**：共用一个幂等键、一个事务，
不会出现"道具到了钱没到"的半成功。

DS 不可信（§9.6）：上报值先过 `MaxBattleGoldPerPlayer`（默认 100 万）就地钳位，
**钳完写回 `result`**，让战绩表与钱包发放读到同一个数 ——
若只在出箱侧钳，战报会记着"本局 999 亿金币"而钱包只加了 100 万，客服无从解释。
超限**截断而不是拒整场**：战绩落库失败会连带段位、任务、掉落一起丢，代价远大于少发点钱；
但每次截断都留 `battle_gold_truncated` Warn。

---

## 7. 遗留与未做

| 项 | 状态 |
|---|---|
| `player_currency` 旧表 | 保留只读，contract 迁移待做 |
| `inventory_ledger.result_gold` | 保留只读兜底，等 90 天保留期清完存量行再删 |
| 装备词条参与定价 | **未做**。满词条史诗与白板同 ID 装备仍卖同样的钱（价格唯一来源是 `item.sell_price`） |
| 出售币种按道具配置 | **刻意未做**。当前无"卖某道具得钻石"的确认需求，用服务端配置 `currency.sell_kind`（默认金币）；真有需求时加表列即可，RPC 契约不用改（§15.3 拒绝预设性复杂化） |
| 拍卖 / 交易的多币种市场 | **未做**。两者仍固定金币计价，但已在 inventory 边界显式传 `CURRENCY_KIND_GOLD`，改成多币种时不用动 inventory |
| 邮件附件带货币 | **未做**。`mail.proto` 的 `MailAttachment` oneof 仍无货币分支 |
