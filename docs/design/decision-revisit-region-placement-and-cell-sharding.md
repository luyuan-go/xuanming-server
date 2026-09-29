# 决策复议：玩家数据分片与地理进场分开

> 状态：待人拍板；本文件只提出修订，不改变 2026-06-26 已拍板的 Region → Cell 扩容目标。
> 证据范围：2026-09-24 本地源码静态核对；未验证多地域部署、真实 RTT 或端到端玩家路径。

## 1. 旧决定与确认的问题

`scale-cellular-20m.md` §3.2、§4.2 把 `player_id` 用于计算数据落点，又把 Region 解释为可对齐国服/海外的地理区域；`decision-revisit-global-matchmaker.md` §2 进一步假设同一数据归属 Region 的玩家网络上接近，并用它决定撮合优先级和 Battle DS 所在 Region。这项假设不成立：ID 的数值与玩家接入位置、到各 DS 的 RTT 没有关系。

实际代码比“region = player_id % 4096”多一步：`pkg/cellroute/cellroute.go` 先以 `player_id % 4096` 计算 `logical_cell`，再查 `logical_cell → (RegionID, CellID)` 映射表。映射表可以重指数据落点，但没有玩家地理信息；`BuildBalancedEntries` 按配置的物理 Cell 顺序把逻辑 Cell 连续分段。两名同城玩家可被映射到不同大洲，两名跨洋玩家也可被映射到同一 Region。

取模还存在独立的容量风险：`pkg/snowflake/snowflake.go` 的 ID 布局是 `[time:32][node:17][step:15]`，`step` 每秒从零开始。`% 4096` 只读取低 12 位 `step`，完全不混合时间与节点。24 个 Cell 初铺时，第一个 Cell 拿到约 171 个连续逻辑分片。如果一个发号节点每秒给角色铸出的 ID 始终落在这些低序号，下一秒又从零开始，角色会持续集中到首个 Cell。`pkg/cellroute/cellroute_test.go` 的分布测试使用连续整数 ID，不能证明真实 Snowflake 序列均匀。

目前提交的服务配置没有启用 `cell_route`，默认 `mode=off`，单 Cell 不会因该哈希把玩家送往跨洋 DS。**启用多 Region 后语义错误会进入匹配主路径**：`matchmaker/internal/biz/match.go` 的 `formMatchesInPool` 按队长 `player_id` 算出的数据 Region 分桶，跨区比例也把队长 Region 计给所有队员。其 `battlePlacement` 多数 Cell 结果目前只打 Debug 日志，尚未传给 allocator。登录的 Hub 分配另用 `login.hub.region` 字符串配置，Hub 票据签名目前给 region/cell `0/0`；这些落点不能当作同一个已闭合的多 Region 契约。

## 2. 建议的三个独立概念

| 概念 | 权威与生命周期 | 用途 |
|---|---|---|
| `home_region` / `home_cell` | 每个角色 `player_id` 的持久数据归属；只有受控迁移能改变 | 档案、资产、结算与其唯一权威数据的寻址 |
| `access_region` | 当前会话的接入位置；旅行、网络变化后可重新选择 | 边缘入口与 Hub DS 的低延迟候选；它不是数据迁移指令 |
| `battle_region` / exact target | 每次对局基于全体参战玩家选出并持久绑定；重试沿原 `operation_id` 继续 | Battle DS 分配、票据核验、恢复与结算回流 |

`owner_epoch`、lease、`operation_id` 和 Admission 属于 `CLAUDE.md` §9.22/23 的**唯一玩家可玩 DS 权威**，不能从上述任何一个 Region、`player_locator` TTL 或映射表推断。这里的“数据归属”也不等于“当前 DS owner”。

## 3. 推荐修法

1. **先确定数据归属，后在归属 Region 内分 Cell。** 新角色在建立权威数据时，由用户明确选择的可用区域、服务端可验证的接入 RTT、容量和数据驻留规则共同确定稳定 `home_region`，持久记录在可线性一致读取/更新的角色路由目录；不能每次登录按 IP 或客户端自报位置直接重算。账号认证仍可全球入口，进入某个角色时读取该角色的归属。Region 内的 `logical_cell` 用固定、版本化、能充分混合 Snowflake 所有位的稳定哈希计算，再查 `(home_region, logical_cell) → home_cell`。因此原方案“全程只算不查”须明确改为“先查一次稳定归属，再在 Region 内算分片”；缓存只能按路由版本失效，不能在失败时猜默认 Region。
2. **网络放置独立计算。** 入口以服务端观测及可校验探测得到玩家到候选 Region 的 RTT，并考虑健康、容量、玩家选择和允许的延迟上限。匹配不能继续拿 `home_region` 或队长 ID 代替全队位置；Battle DS 在所有成员可接受的候选中选址，并把 exact target 持久写进本次 match/allocation。多地无法同时满足延迟上限时，返回可见的等待、改选或失败入口，不能静默选一洲。Hub 若放在 `access_region`，必须先证明远端 DS 到玩家唯一 owner authority 与权威数据的链路满足超时、fencing 和容量预算；否则先在 `home_region` 提供 Hub，并明确旅行玩家的延迟限制。
3. **拆清票据字段语义。** 登录返回的数据 home 落点、Hub/Battle DS 的物理 target、DS 票据中的 region/cell 应来自各自权威并彼此校验，不能让同名字段一会儿表示数据归属、一会儿表示目标 DS。跨 Region Battle 的 Admission、续租和结算仍须向每名玩家唯一权威提交带 `owner_epoch` 与稳定 `operation_id` 的请求；失败返回 `WAIT/UNKNOWN`，不得靠 locator key miss 或默认 `0/0` 放行。跨 Region 结算分别回原 `home_cell` 幂等入账。
4. **已有数据与路由变更走迁移。** 不能直接把 `% 4096` 换成新哈希、重排映射表或改 `home_region` 列，因为旧角色会被路由到空 Cell。须盘点所有权威数据与读写者，按版本记录旧/新路由，复制并核对数据，冻结或 fencing 旧写者，在唯一权威上原子切换路由版本，再开放新写者与回滚/恢复入口。具体搬迁协议另行设计并验证；在它完成前保持旧路由可读。首次生产上线兼容阶段由 `CLAUDE.md` §3.1 判定；即使仍是开发期，也必须整批刷新生产者、消费者和明确的测试数据目标。

## 4. 为什么不只修一处

- **只把 `% 4096` 改为另一种哈希**可以修低位热点，却仍不能推导地理位置，还会重定位现有角色。
- **只按 GeoIP 每次改 Region**会在旅行、VPN、移动网络切换时改动玩家数据归属；GeoIP 也不是 RTT 证明。
- **只给匹配增加 RTT 评分**但继续先按 ID Region 严格分桶，会让同城玩家在不同桶等待到溢出，且无法修 Hub/票据落点。
- **只把数据 Region 改成地理 Region**也不能代表跨洲组队各成员到同一 Battle DS 的实际 RTT。

## 5. 验收与需拍板项

1. 用真实 Snowflake 多节点、低/中/高每秒发号率样本验证 4096 分片在每 Region 的分布；固定哈希算法与跨语言 golden vectors，重启/扩 Cell 不重定位未迁移角色。
2. 两名同城但 ID 落不同旧 Region、两名跨洋但旧 Region 相同、跨洲组队且队长位置与队员不同、玩家旅行/重连：撮合与 Hub/Battle 选址分别按真实接入证据和延迟上限判断，数据仍回各自 home。
3. 路由表缺失、RTT 不可信、候选 Region 故障、票据回包丢失、matchmaker/allocator 重试、Owner authority 分区、旧 DS 迟到写：验证无第二可玩 DS、无默认 Region 猜测、无永久等待、无资产串区；跨 Region Battle 走真实 DS Admission 和结算端到端验收。
4. 多 Region 发布前核对登录、Hub allocator、matchmaker、ds_allocator、Battle 票据、owner authority、结算、边缘网关与 UE 客户端的字段语义及混版兼容；不能把单元测试或静态配置视作全球同服验收。

**需要人拍板**：是否采用“角色创建时选稳定地理 `home_region`，Region 内稳定哈希分 Cell；会话与对局网络放置单独选”的方案，以及旅行玩家的 Hub 是否允许在异于 home 的 Region 运行。此文档获批前不修改既有已拍板路由语义。获批后再修订 `pandora-arch.md`、`scale-cellular-20m.md`、`decision-revisit-global-matchmaker.md` 中混用的 Region 术语；`player_locator` 仍只作 presence 投影，以 `CLAUDE.md` §9.22 为准。
