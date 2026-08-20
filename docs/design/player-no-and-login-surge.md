# 角色编号(player_no)与开服登录/注册洪峰应对

> 2026-08-09。触发:策划要求给玩家一个**自增的角色编号**(对标梦幻西游的玩家编号),
> 与既有 snowflake `player_id` 并存。讨论过程中暴露出更大的前置问题:开服首日
> 「首登即注册」洪峰打在哪里、现有防线是什么、缺口是什么。本文一并定案。
> 状态:**设计稿,待拍板**(§6 拍板清单:策划三问 + bcrypt cost + 排队立项)。
> 证据口径:全部 file:line 为 2026-08-09 HEAD,引用前建议重新 grep(行号会漂移)。

---

## 0. 结论速览

0. **编号绑定「角色」不是「账号」**(§3.6.1,2026-08-10 用户拍板,**读本文先读这条**):
   项目**角色可交易(卖角色,按过户设计)**,编号是角色的资历凭证(编号小=老角色,是定价
   要素),过户时随角色走、值不变——故**一账号建 N 个角色 = N 个编号**。今天 `player_id`
   一身兼两职(账号身份+角色身份),现实现按 `player_id` 编号即已正确,代码零改动。
   ⚠️ 本文早期版本曾写「编号必须跟 account_id 走」,**该结论已作废**,勿据此推导。
1. **角色编号方案**:`accounts` 加 `player_no` 列(注册时留空)+ 全局计数器表 +
   **异步批量补号任务**(单事务「锁计数器行 → 批量编号 → 推进计数器」,多副本天然互斥,
   无需 leader election)。**发号序列**严格连续、严格按创建先后;登录/注册关键路径**零改动、零新热点**。
   (删角色不回收编号 → 存活集合可有洞、最大号 = 累计创建数,§3.6.2)
   明确否决:同步事务内取号(单行热点,开服洪峰下是登录路径上的全局串行点)、
   TiDB `AUTO_INCREMENT`(本仓已论证否决,见 §2.4)。
2. **架构定性**:本项目是**全区全服单一逻辑世界**,不是分区分服;region/cell 是部署维度不外露
   (scale-cellular-20m.md 不变量「客户端最小视图」),账号命名空间全局一套(`uk_account`)。
   所以「角色编号」语义上等价于一个巨服的服内序号——梦幻西游靠分服号段回避的全局问题,
   我们必须正面解,但解出来的结果也更强(真·全局连续,永无合服重编号)。
3. **关键现状**:生产环境**今天不存在注册写路径**(`dev_auto_register`/`dev_skip_password`
   prod 均 false,「正式注册流程不属 login 职责」且本仓无注册服务)。角色编号方案对
   「谁写 accounts」零假设——补号任务只看表,未来正式注册流程接入时无需改造。
4. **洪峰防线现状**:有 BBR 自适应限流(prod 机械强制 14 服务)、client 熔断、KillSwitch、
   TiDB schema 打散;**没有** Envoy 边缘速率闸、**没有**登录排队/开服放量机制、
   压测**从未**打过注册/登录洪峰场景。缺口与对策见 §5。

---

## 1. 需求与对标:策划要的到底是什么

### 1.1 需求

策划希望看到**角色编号**:自增、可读、反映注册先后。与 `player_id`(snowflake,
uint64,无业务含义)并存,不替代。

**编号的归属主体是「角色」**(2026-08-10 用户拍板,§3.6.1):本项目角色可交易(卖角色),
一账号建 N 个角色即有 N 个编号,过户时编号随角色走。下文凡出现「玩家编号」的旧措辞,
一律按「角色编号」理解。

### 1.2 对标拆解:梦幻西游的「自增编号」真实语义

梦幻西游是分区分服:编号空间**自带服标识**(号段偏移/前缀),「自增」只发生在服内。
合服不撞号靠的是号段隔离;代价是全局视角编号布满空洞、只在服内连续。玩家感知不到,
因为每人只看自己服的号段。

**关键佐证:梦幻的编号本来就是「角色」编号**(2026-08-10 补)。梦幻是一账号多角色,且
角色交易(藏宝阁)是其成熟且核心的经济玩法——编号跟着角色走、随过户转移,老号(编号靠前)
本身就是溢价要素。这与本项目「卖角色」的取向同源,**反向印证 §3.6.1 的归属判定**:
对标梦幻 = 对标「角色编号 + 角色可交易」,而不是「账号编号」。

推论:
- 策划的对标其实**弱于**「全局严格连续」——梦幻从来给不出全服第 N 名。
- 我们没有「服」,给出的是真·全局连续编号,**严格优于**对标;也永无合服重编号的历史包袱。
- 若策划哪天要「每服短号」(分区开服的运营仪式感),那是先造「服」概念的产品决策,
  编号只是跟随,不在本文范围。

### 1.3 我们是不是「单服」?

不是「一台服务器」,也不是分区分服,而是**全区全服**:

| 维度 | 事实 | 证据 |
|---|---|---|
| 产品定位 | 全区全服爆款 MOBA(CCU 系数按此取上界) | scale-cellular-20m.md §1 |
| region/cell | 部署维度,不外露内部拓扑;登录后由服务端从 player_id 确定性算落点 | scale-cellular-20m.md 不变量表「客户端最小视图」、§3.2 |
| 账号命名空间 | 全局一套,唯一性完全由 `accounts.account` 列 + `uk_account` 决定,无任何按服切分维度 | conf.go 排序规则注释、02-account-tables.sql |
| 账号库 | 全服共享单点(TiDB),「全国所有玩家的登录都写同一实例」 | 03-account-tidb.sql:3 |
| login 配置里的 Region | hub_allocator 的大厅分片参数(空=选最空分片),与玩家选服无关 | login conf.go `HubAllocatorConf.Region` |

**两库的扩容路线刻意相反,编号必须挂对边**(2026-08-10 补):

- **玩家库 pandora_player**:今天单机 MySQL(deploy/tidb-init 只有 social/owner/account
  三个,无 player 版);扩容路线是**应用层按玩家分片**——代码已预埋分片键口径
  `ProfileShardKey = player_id`(player biz/profile_sharding.go,档案锚定玩家 owner cell),
  cell 化后进 Cell 内 `MySQL ShardSet(player_id % N)`。它能这么切是因为访问天生单键:
  全服务 grep 无任何跨玩家查询(players 表的 `idx_mmr` 在 Go 代码中零引用,疑似闲置)。
- **账号库 pandora_account**:「全服单点」指**逻辑命名空间单一**(一个 schema、一套
  `uk_account`),不是一台机器——TiDB 物理上本就是多节点,加 TiKV 节点即扩容;
  2026-07-27 选 TiDB 而非应用层分库,买的正是「业务 SQL/Go 零改动」。逻辑上真拆多库
  技术可行(已核:accounts 只按账号名访问,session/roles/devices 只按 player_id 访问,
  accounts 与 player_session_generations **从不共事务**——可按不同键各自分片,同名账号
  必落同片故唯一性仍成立),但那是重新捡回 TiDB 已消掉的复杂度,§15.3 无真实需求不做。
- **编号挂账号库**的原因(2026-08-10 修正表述):**与「编号属于账号还是角色」无关**
  (归属主体是角色,§3.6.1),真正的理由是**落点的分片宿命**——玩家库的宿命是按
  `player_id` 切开,而「全局连续计数 + 全局唯一索引」需要一个「宿命是全局一份」的落点;
  账号库是逻辑单点(单一 schema)恰好满足。初稿此处写的「注册是账号事件」是被推翻前提的
  残留论证,结论(挂账号库)不变但理由作废。

  **由此导出的改造期硬约束**(多角色立项时必须先答):`player_no` + `uk_player_no`
  随 `player_id` 落到角色表后,**角色表本身落在哪个库**决定唯一性怎么保:
  - 角色表仍在**全局单点库** → 现状不变,`uk_player_no` 继续是全局唯一索引兜底;
  - 角色表进**按 player_id 分片的玩家库** → `uk_player_no` 退化为**分片内**唯一,
    数据库层不再能兜住跨分片重号。此时全局唯一性**只**由补号事务(计数器行锁 + 严格
    连续分配)保证,uk 从「防第二写者的硬兜底」降级为「分片内软兜底」——**必须显式接受
    这一降级并在改造文档中记录**,不能默认 uk 还在保护全局唯一(§16.1 检查后执行)。

**前瞻不确定点**:cell 化方案把 login 描述为「全局/区域薄层」,§4.3 按 region 拆了
social TiDB/总线/matchmaker 但**未点名账号库落点**。若未来按 region 切账号库(国服/海外),
要同时新解:①账号名唯一性跨 region(全局唯一性层或账号名带 region 命名空间);
②编号计数器落点——**那才是「独立编号权威(服务/小库)」的真实需求出现点**(§3.4 用户
提案在该终态是对的形状),迁移路径干净:计数器+补号任务整体搬走,消费方只认 `player_no`
字段。现状账号库是全服单点 TiDB,本方案按此设计。

---

## 2. 关键现状事实(带证据)

### 2.1 生产今天没有注册写路径

- `accounts` 的 INSERT 只存在于 dev 懒注册:`FindByAccount` 返回不存在且
  `(devAutoRegister || devSkipPassword)` 才走 `ensureAccount`(login biz/login.go)。
- prod 模板两开关均 false/未配(login-prod.yaml.example:84 `dev_skip_password: false`,
  `dev_auto_register` 无此键即默认 false);conf.go 注释明写「⚠️ 严禁上生产」。
- login README「不做的事」:**正式注册流程不属 login 职责(仅有 dev 假注册开关)**。
  全仓无其它写 `accounts.password_hash` 的服务——**正式注册流程尚不存在,待立项**。
- 但压测例外:robot/stress 每 VU 首登靠 `devAutoRegister` 自动建号(vu.go),
  即**压测环境会真实打注册路径**——这是补号任务的天然验证通道。

设计含义:角色编号**挂在表上而不是挂在某条代码路径上**。补号任务只认
「`accounts` 里 `player_no IS NULL` 的行」,无论行是 dev 懒注册、未来的正式注册服务、
还是运营导入写进来的,一视同仁。正式注册流程立项时**不需要**为编号做任何事。

### 2.2 一次登录的服务端账单(洪峰的分母)

| 项 | 性质 | 说明 |
|---|---|---|
| bcrypt ×1 | CPU | `passwd.Verify`,cost 由库中哈希内嵌值决定。**现状所有落库哈希实为 cost=4**(`ProdCost=10` 全仓零引用,压测前审核-20260724 已点名为口令强度弱点) |
| `player_session_generations` upsert | DB 写,**必经、fail-closed** | 事务内 `FOR UPDATE` + generation+1,失败拒登录;写 QPS = 全服登录 QPS,「本库写压力最高的表,恰恰是迁 TiDB 的主因」(03-account-tidb.sql) |
| Redis 会话 hash 条件写 | Redis 写 | Lua 仅更高代际可覆盖;失败(非定序输)走墓碑补偿 |
| `account_devices` TouchDevice | DB 写,异步旁路 | detached ctx,失败仅 WARN |
| 下游扇出 | RPC | 5s deadline 内**串行**扇出(压测前审核【必修-1】:任一下游变慢即大面积超时) |

### 2.3 账号库已有的 schema 级防线

- 雪花主键表(`accounts`/`player_roles`/`player_session_generations`):
  `NONCLUSTERED + SHARD_ROW_ID_BITS=4 + PRE_SPLIT_REGIONS=4` 打散时间序写热点。
- 代理主键表(`account_devices`/`account_bans`):`AUTO_RANDOM(5)`。
- 「开服首日是雪花 ID 单调递增的写入尖峰」已被点名并按上述打散(03-account-tidb.sql:6、:44)。

### 2.4 TiDB `AUTO_INCREMENT` 已被本仓否决(不重新论证)

> 「TiDB 的 AUTO_INCREMENT 按 TiDB Server 缓存分段分配,**跨节点非单调**,继续用它既有热点
> 又会让『按 id 单调』的假设失效。」——03-account-tidb.sql:14-17

即便 `AUTO_ID_CACHE=1` 集中发号,回滚/重启仍跳号(有空洞),且与 dev 单机 MySQL 行为不同构,
违背「只改存储属性,业务 SQL/Go 代码不变」约定(03-account-tidb.sql:10)。否决。

### 2.5 计数器表先例(照抄的模板)

social 库 `000002_guild_counter_tables` migration:计数列/计数表 + 从明细确定性 backfill;
guild 代码用法 =「INSERT..ON DUPLICATE 占位 → `SELECT..FOR UPDATE` 锁计数行 → 按明细校正」
(group_repo.go `reservePlayerGroupSlot`)。动机同样是 TiDB 无 gap 锁。本方案沿用同款模式。

### 2.6 洪峰防线与缺口现状

| 层 | 现状 | 证据 |
|---|---|---|
| Envoy 边缘 | **零速率闸**:`rate_limit`/`ext_authz` 明确列为不启用,无 circuit_breakers;anti-abuse-scene-entry.md 判「外挂的第一跳没有任何速率闸,完全缺失」;`local_ratelimit` 仅是该文档落地序 5 的**未实现设计** | deploy/envoy/envoy.yaml:18-21 |
| 服务层 | BBR 自适应限流 prod 机械强制 14 个客户端面服务(含 login,压测前审核门禁-A);客户端登录走 Envoy→login gRPC 链,被 BBR 覆盖。client 侧 SRE 熔断、KillSwitch 已有 | gen_cluster_config.ps1、grpcserver.go |
| 排队/放量 | **不存在**任何登录排队/开服放量机制。全仓「放量」只指金丝雀发布权重,「排队」只指匹配队列;DS 容量耗尽是终态硬失败 `ErrDSNoAvailable(5001)`(改 WAIT+retry_after 是 anti-abuse 落地序 3 待办) | 全仓 grep 查证 |
| login 业务级 | 无失败次数限制、无 IP/设备频率闸;dev 档 ensureAccount 使账号免费可无限刷 | anti-abuse-scene-entry.md §登录 |
| 压测 | 40 万 VU 设计 10 分钟线性爬坡(~650 VU/s),**刻意避开瞬时 connection storm;从未有注册/登录洪峰专项场景**;既有洪峰结论全部来自静态审计 | stress-single-cell-client.md:127 |
| 容量目标 | 2000 万 DAU 已拍板:登录峰值 ~十万级 QPS(早晚高峰+重连风暴),首日/版本尖峰再 ×1.3~1.5;注册总量 ~1 亿~2 亿 | scale-cellular-20m.md §1 |

---

## 3. 角色编号方案

### 3.1 语义先拍板(策划三问)

**四问均已拍板**(2026-08-10),下表保留选项与理由备查:

| 问题 | 选项 | 结论 |
|---|---|---|
| ⓪ **归属主体** | 账号 vs **角色** | ✅ **角色**(用户拍板,§3.6.1):项目卖角色/可过户,编号是角色资历凭证,随角色走;一账号建 N 角色 = N 个编号。代码零改动(今 `player_id` 即角色身份) |
| ① 要不要严格连续无空洞 | 严格连续(编号≈第 N 个创建,最大号≈累计总量) vs 大致递增有洞 | ✅ **分配时刻严格连续**(「自增编号」的直觉语义,实现代价不更高,见 §3.2)。**删角色不回收编号**(已拍板,§3.6.2)→ 删除会留洞,故「严格连续」的准确表述是**发号序列连续**、存活集合可有洞;最大号 = **累计创建**角色数 |
| ② 给谁看 | 仅 GM/运营后台 vs 客户端玩家可见 | ✅ **客户端玩家可见**(用户拍板)。服务端 + UE 本地链路已落码;编译/E2E 边界见 §3.5 |
| ③ 起始号 | 1 vs 好看号段(如 100001) | ✅ login 配置 `player_no_start`,**默认 1**;只在计数器首次初始化生效,之后改配置无效 |

### 3.2 选型:为什么是「异步批量补号」

| 方案 | 洪峰下行为 | 判定 |
|---|---|---|
| A. 注册事务内同步取号(`UPDATE counter SET v=v+1` 与 INSERT 同事务) | 计数器单行悲观锁排队,短事务实测量级**几百~1k TPS 到顶**;把全局串行点插进登录关键路径,开服洪峰下登录延迟整体抬升 | ❌ 否决(§16.5 容量边界) |
| B. `AUTO_INCREMENT`(含 `AUTO_ID_CACHE=1`) | 有空洞、跨节点非单调、与 dev MySQL 不同构 | ❌ 本仓已否决(§2.4) |
| C. **异步批量补号**(注册留空,后台任务批量编号) | 注册路径零改动零新热点;串行点被批量摊销(每批只碰一次计数器);积压时只是编号晚几秒出现,**优雅降级不雪崩** | ✅ 采用 |

方案 C 的吞吐上限是「批量 UPDATE 写 TiDB 的速度」(每秒数万行量级),真到 2000 万 DAU
开服体量,先到瓶颈的是 §2.2 那份每次登录都要付的账单,不是编号。

### 3.3 设计

**DDL**(mysql-init / tidb-init / tools/migrate 三处同步,遵循同构约定):

```sql
-- accounts 加列(NULL = 未编号;uk 兜底防双号,MySQL/TiDB unique 均允许多 NULL)
ALTER TABLE accounts
    ADD COLUMN player_no BIGINT UNSIGNED NULL COMMENT '角色编号(展示专用,禁作身份键/外键;NULL=待补号)',
    ADD UNIQUE KEY uk_player_no (player_no);

-- 全局计数器(单行;next_no = 下一个待发号)
CREATE TABLE IF NOT EXISTS player_no_counter (
    id       TINYINT UNSIGNED NOT NULL,
    next_no  BIGINT UNSIGNED  NOT NULL,
    PRIMARY KEY (id)
) COMMENT='角色编号全局计数器(单行 id=1;补号事务 FOR UPDATE 锁行即互斥)';
```

**补号任务**(挂 login 既有后台任务循环,与 `PurgeStaleDevices` 并列——§16.10 不新建
第二套 timer 状态机;周期建议 5s,批大小默认 500 对齐仓库 `DELETE..LIMIT 500` 惯例):

```
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;                   -- ⓪ 正确性要求,见要点
BEGIN;
SELECT next_no FROM player_no_counter WHERE id=1 FOR UPDATE;   -- ① 锁计数器 = 全局互斥
SELECT player_id FROM accounts WHERE player_no IS NULL
    AND created_at < NOW() - INTERVAL 10 SECOND                   -- ② 水位安全滞后(见要点)
    ORDER BY created_at, player_id LIMIT 500;                     --    + 稳定顺序取批(不加行锁,见要点)
UPDATE accounts SET player_no = <next_no + rank>
    WHERE player_id = ? AND player_no IS NULL;                  -- ③ 批内按序编号(复核仍为 NULL)
UPDATE player_no_counter SET next_no = next_no + <批大小>;      -- ④ 推进计数器
COMMIT;                                                           -- 回滚则①-④一起消失,无空洞
```

要点:
- **多副本安全,无需 leader election**:计数器行 `FOR UPDATE` 天然串行化并发副本
  (§15.1 标准能力优先;同 guild counter 先例)。事务原子保证崩溃无空洞、无双号。
- **排序键 `created_at + player_id`**:created_at 秒级、跨 TiDB 节点毫秒级时钟差下,
  编号先后与真实提交序可能差一两位——展示编号要的是**确定性**,不是物理精确序,可接受。
- **水位安全滞后 10s(2026-08-10 补,封死迟可见错序)**:INSERT 的 `created_at` 在语句
  执行时打戳,行对其它事务**可见**要等提交——若兜底贴着「现在」编号,一条打了早戳、
  晚几百毫秒才可见的行会被越过,补号时拿到更大的号(created_at 更早但编号更大的**可观测
  反例**)。加滞后后,任何行从打戳到被编号至少有 10s 可见窗口:只要它在窗口内可见,
  兜底扫到它的 created_at 位置时它必然在场,按序拿号。反例只剩「单语句 autocommit INSERT
  打戳后 >10s 才可见」——被语句/gRPC 超时(prod 5s)排除:超时即失败无行,玩家重试拿新戳。
  即:**编号全序与 created_at 全序严格一致,零反例**;10s = 5s 超时上界 ×2 保守,待实测复核。
  代价仅是编号可见延迟从 ~5s 变 ~15s,展示场景无感。
- **事务隔离必须 READ COMMITTED(落码修正 2026-08-10,真 TiDB 并发测试抓获)**:计数器
  行锁只串行化「写」,② 取批 SELECT 的可见性由隔离级别决定。TiDB 悲观事务在默认 RR 下用
  `BEGIN` 时刻的 start_ts 快照服务普通 SELECT——后到的 sweeper 在计数器锁上等前一批提交,
  拿到锁后 ① 的 `FOR UPDATE`(当前读)能读到推进后的 next_no,但 ② 仍按旧快照重扫刚被
  编号的同一批行,③ 的复核(当前读)恒 affected=0,把**正常并发**误判成"第二写者"整批
  回滚——两副本互相打回,fail-closed 护栏成了误报源。InnoDB RR 无症状纯属侥幸:read view
  迟到首个一致性读(② 在拿锁之后)才建。RC 下两端都是逐语句新快照,② 发生在拿锁之后即
  必然包含锁前驱批次的提交,affected=0 恢复「真第二写者」语义。刻意不用「给 ② 加
  `FOR UPDATE`」的修法:InnoDB 下会在 `uk_player_no` 的 NULL 范围产生间隙锁,正是下条
  要规避的(RC 还顺带免除该间隙锁)。回归:player_no_mysql_test.go ③(修复前 TiDB 必炸)。
- **不锁账号行(落码修正,2026-08-10)**:取批 SELECT 不加 `FOR UPDATE`——player_no
  只有本事务(已被计数器锁串行化)写,行锁冗余;且 InnoDB 下扫 `uk_player_no` 的 NULL
  范围会产生间隙锁,反向阻塞并发注册 INSERT。以 ③ 的 `AND player_no IS NULL` +
  RowsAffected==1 复核兜底,复核失败说明计数器锁外存在第二写者,整批回滚 fail-closed。
- **积压语义**:洪峰下任务落后只表现为「新玩家编号晚几秒~几分钟可见」;查询侧对
  `player_no IS NULL` 显示「分配中」。
- **回填不需要独立步骤(落码简化,2026-08-10)**:存量账号就是「待编号行」,补号任务
  首轮按同一全序自然追平(单轮 drain 上限 20 批 = 1 万行,大存量分多轮);`next_no` 由
  login 启动期 `INSERT IGNORE` 初始化为配置 `player_no_start`(= 策划三问③,默认 1,
  计数器已存在后改配置无效)。单一代码路径,无迁移/在线双实现(§15.2)。
- **查询/下发**:pb 字段 `uint64 player_no`(§5.12 非负默认无符号;0=未分配,
  与「NULL=待补号」对应,天然满足 proto3 零值语义)。仅当策划三问②选「客户端可见」时
  才加进客户端可见结构并同步 UE。

**红线**(2026-08-10 精确化:原表述「见到 `WHERE player_no =` 直接拒」过粗,会误伤
客服反查这一**预期用法**):`player_no` 是**纯展示字段**,身份永远是 `player_id`(§9.11)。
边界按「翻译」与「当键」划分:

| | 允许 | 禁止 |
|---|---|---|
| 形态 | **一次性翻译**:`player_no` → `player_id`,结果只在本次请求内用,不落库、不传给下游当参数 | **当键使用**:业务表存 `player_no` 列、拿它做 JOIN / 外键 / 幂等键 / 路由键 / 缓存键 |
| 典型 | 客服工单:玩家报编号 → 运营工具反查 `player_id` → 用 `player_id` 查各业务库流水(§3.6) | 订单表存 `player_no`、按 `player_no` 分片、用它做发放幂等键 |
| 为何 | 一次索引点查(`uk_player_no`),等价于「查号码簿」;下游只见 `player_id`,零传播 | 见下方四条 |

**禁止「当键」的四条理由**(任一条都足以致命):
1. **有未分配窗口**:新注册后约 15s 内 `player_no` 为 NULL/0(补号周期 + 水位滞后),
   业务逻辑依赖它会在这个窗口静默失败;`player_id` 注册瞬间即有。
2. **可重编**:迁移回滚、账号库拆分/region 化都可能重分配编号(§1.3 前瞻);
   `player_id` 是 snowflake,永久不变。
3. **展示语义可能改**:产品若要加号段前缀、按 region 切,存进业务表的旧值就改不动了。
4. **跨库**:编号在 `pandora_account`,流水在 inventory / trade / battle / social 各库
   (全部以 `player_id` 为键且有索引,已核)——业务库根本不该知道这个字段存在。

review 判据改为:**`player_no` 出现在 `pandora_account` 以外的任何表定义、或作为
非运营工具的服务间请求参数 / 身份键传播,直接拒**;运营工具里的
`WHERE player_no = ?` 是正当用法。

**只读展示投影例外(2026-08-20,组队成员编号需求)**:业务服务可以用一批
`player_id` 向 login/account 权威读取对应 `player_no`,并把结果只放进客户端可见响应。
这不是 `player_no → player_id` 反查,也不是拿 `player_no` 当服务参数或业务键。当前 team
链路使用独立且不经客户端 Envoy 暴露的 `LoginInternalService.ResolvePlayerNos`:

- 请求只含 `player_ids`,整包受 request-bound `internalrpcauth` 保护;原始批量最多 32 条,
  team 必须一次提交整支队伍,禁止逐成员 N+1;
- login 只做一次账号库 `IN` 查询;响应里的 `player_no` 只供 `TeamMember.player_no` 展示;
- team 使用独立 250ms 预算;权威不可达、超时或编号仍在补号时 fail-soft 为 0,
  不阻断组队核心读写;
- `TeamMemberStorageRecord`、team Redis、成员判断、邀请、踢人、路由和幂等仍只使用
  `player_id`,不得缓存这份展示投影。

### 3.4 变体评估:独立 ID 服务 + 逐个异步申请 + 映射表(2026-08-09 用户提案)

提案形状:创建玩家时向一个 **ID 服务**异步申请编号,未返回前客户端显示「ID 生成中」,
编号与 player_id 的关系存**独立映射表**。与 §3.3 逐项对比:

| 维度 | 提案 | §3.3 方案 | 评估 |
|---|---|---|---|
| 异步 + 「生成中」占位 | ✅ | ✅(「分配中」) | **完全一致**——两个方案独立收敛到同一结论:编号必须退出创建关键路径 |
| 存储形态 | 独立映射表(`player_no` PK ↔ `player_id` uk) | `accounts` 加列 | **等价可选**。映射表优点:不动 accounts DDL、不依赖「谁写 accounts」;缺点:发现待编号行不能再用 `player_no IS NULL` 索引扫,需按 `(created_at, player_id)` 水位游标推进 + 时钟偏差安全滞后(处理 `created_at < now()-10s` 的行),多一套水位状态。加列方案的发现查询天然正确、零状态 |
| 分发机制 | 每次创建**逐个请求**ID 服务(push) | 后台任务**批量扫表**(pull) | push 有先天缺陷:INSERT 与「发申请」是无事务的双写,进程在两步之间崩溃/ID 服务不可用 → 该玩家永远没编号,**必须再配一个兜底扫描**才完整;而兜底扫描自身已是完备方案。即 push 不能替代 pull,只能叠在 pull 上换「编号秒级可见」的低延迟——展示场景不需要,按 §15.3 暂不建,留作未来优化位 |
| 部署形态 | 新增独立服务 | 挂 login 既有后台循环 | 单客户、单功能、无独立状态的「服务」不满足 §15.4 复杂度举证;且编号权威属账号域,留在 login(pandora_account 权威)不新增权威边界。**否决独立服务,保留任务形态** |

**追问:「push + 兜底扫描」组合行不行?(2026-08-09)** 行——正确性上成立(两条路径
都走同一个「锁计数器行 + 仍为 NULL 才赋号」事务即无双赋号)。不选它是性价比问题,账如下:

| | 买到 | 付出 |
|---|---|---|
| push 叠加兜底 | 稳态下编号**亚秒可见**(纯兜底为扫描周期级,~5s) | ① **顺序破坏**:push 按请求到达序赋号,兜底按 `created_at` 序补号——丢请求的玩家被补号时会拿到比"晚于他注册的玩家"更大的号。若拍板项 A① 选「严格按注册先后」,push 直接与之冲突;要 push 保序只能让它全局按 created_at 排队处理,那它就退化回兜底扫描本身。② **洪峰下自动失效**:push 逐个碰计数器行(几百/s 到顶),兜底批量摊销(几万/s)反而更快——最需要低延迟的时刻,push 落后于兜底,等价于纯兜底。③ 多一条创建侧发请求链路 + 一个处理端组件要建、要运维、要压测 |

即:push 只在「稳态 + 接受大致先来后到」的窗口里买到几秒延迟改善,而展示场景对这几秒
不敏感。结论:**吸收「异步 + 占位显示」(本就一致)与「映射表」为可选存储形态(拍板项 A
追加④列 vs 映射表,默认加列);「独立服务 + 逐个申请 + 兜底」不否定其正确性,按 §15.2/15.3
先不建,纯兜底起步**;将来真出现「编号必须秒级可见」的产品需求,或第二个编号类需求
(公会号、靓号池),再以真实需求重议叠加 push / 服务化。

### 3.5 落地清单(2026-08-10 落码)

| # | 事项 | 落点 | 状态 |
|---|---|---|---|
| 1 | DDL:加列 + uk + 计数器表 | deploy/mysql-init/02、deploy/tidb-init/03(TiDB 版 `uk_player_no` 尾部热点在补号 QPS 量级下无碍,不需打散);迁移链保留 immutable `000004_register_no` 与 `000005_rename_player_no`,新增 `000006_reconcile_player_no` 收敛 old-only / target-only / 双对象库并统一注释 | ✅ |
| 2 | 回填 | **无独立步骤**:补号任务首轮自然追平(§3.3 要点);`next_no` 起始由 login 启动期初始化 | ✅(简化) |
| 3 | 补号任务 | login `internal/data/player_no.go`(事务)+ `cmd/login/main.go`(5s ticker,drain 上限 20 批,启动探针 fail-soft)+ conf `player_no_start` | ✅ |
| 4 | 真 MySQL / 真 TiDB 双后端测试 | `internal/data/player_no_mysql_test.go`:全序/跨批连续、水位滞后、双 sweeper 并发无重号无空洞(= TiDB start_ts 快照缺陷回归,见 §3.3 隔离级别要点)、起始号幂等、缺迁移探针失败(PANDORA_TEST_MYSQL_DSN / PANDORA_TEST_TIDB_DSN 双门控,friend/guild 同款) | ✅ |
| 5 | 容量/清单登记 | dbcheck registry + login budgets.go + CLAUDE.md §9.24 豁免段:`player_no_counter` 恒 1 行权威闸 | ✅ |
| 6 | 展示链路(A② 已拍板 2026-08-10:**客户端玩家可见**) | 服务端已落码:proto `LoginResponse.player_no = 13`、`AccountRepo.GetPlayerNo`(**fail-soft**:独立 250ms 查询预算,失败/超时置 0 且不取消登录父 ctx;刻意不并进 FindByAccount——列缺失不能打挂登录整链)、biz 主路径与 battle 重连路径都带出、service 组装 `PlayerNo`。0 = 补号中,客户端显示「生成中」 | ✅ 服务端 |
| 7 | UE 展示与交付验证(Codex,2026-08-10) | 服务端 C++ pb 以 `[proto]` 提交 `bea78b83`,客户端通过官方 `GenClientProto.ps1 -UpdateLock` 同步并以 `-VerifyOnly` 复验;登录解码→会话态→RoleInfo 全链带出 `player_no`,0 显示「生成中」;login `go build/vet/test`、`Pandora` 与 `PandoraEditor` Development 编译全绿 | ✅ 生成/编译;PIE 与真实登录 E2E 未跑 |
| 8 | 首次会话补拉闭环(2026-08-10 实测后补) | 服务端新增空请求 `LoginService.GetPlayerNo`、JWT 身份与 Envoy exact rule、`0/OK` 和错误分流;客户端以官方生成器同步协议,登录后用 CoreTicker 补拉并以 attempt / SessionGeneration / PlayerId 围栏保护写回;见 §3.7 | 🟡 服务端和 UE 客户端本地落码、`-UpdateLock` / `-VerifyOnly`、本任务源码编译及 PandoraTests DLL 链接已完成;完整 PandoraEditor 链接被无关 MyMainView 并行改动阻断,新测试 DLL 因旧主 DLL 缺本轮导出而加载失败,三组 Automation 未执行;Envoy 运行态与真实登录 E2E 未验收 |
| 9 | 组队成员展示编号(2026-08-20) | `TeamMember` 增加只读 `player_no`;team 以一次受 internalrpcauth 保护的批量 RPC 从 login/account 权威投影,不写 `TeamMemberStorageRecord`;Go / Python / UE 三栈同构,昵称为空时优先显示编号,0 才兼容回退 `player_id` | 🟡 源码与回归测试落码;协议生成、全量编译、运行态与玩家 E2E 以本次验证记录为准 |

### 3.6 客服/运营按编号反查玩家(2026-08-10 用户提出,**待落地**)

**场景**:玩家报工单只会说自己的编号(界面上就显示这个),客服要据此查该玩家的背包流水、
交易订单、战报、邮件。这是编号「给玩家看」之后必然跟来的用法,也是 §3.3 红线明确
**允许**的一次性翻译。

**链路形状**(三跳,全部是索引点查):

```
玩家报编号 → ① accounts WHERE player_no=?  → player_id   (uk_player_no,pandora_account)
           → ② 各业务库 WHERE player_id=?     → 流水/订单/战报/邮件
           → ③ 结果只回显给客服,player_id 不落进任何新表
```

第 ② 跳已全通:玩家相关表**全部**以 `player_id` 为键且有索引(逐库核过:
`inventory_ledger` uk(player_id,idem)、`player_items` uk(player_id,item_config_id)、
`battle_player_stats` idx(player_id,match_id)、`player_mail` idx(player_id,status)、
`player_mail_claim` PK(player_id,mail_id)、`friendships` uk(player_id,friend_id)、
`blocks` uk(player_id,blocked_id) 等),分布在 inventory / trade / battle / social 各库;
各服务也已有按 player_id 的查询 RPC(如 `ListPlayerHistory` / `ListMyOrders` /
player `GetProfile`)。

**今天的缺口(两个,都不大)**:

1. **反查方法不存在**:`AccountRepo` 只有 `GetPlayerNo(player_id) → player_no`
   (正向,给登录下发用);没有 `FindByPlayerNo(player_no) → player_id`。
   索引 `uk_player_no` 已经建好,补一个方法即可,**不需要任何 schema 改动**。
2. **没有「查询类」运营接口**——但**运维通道本身已存在**,别重复造:
   - `GmService`(`proto/pandora/gm/v1/gm.proto`,2026-07-07,与 ds_allocator 同进程):
     已有的 GM / 运维指令通道,**内部接口不经 Envoy 暴露给玩家客户端**。但它是**写向**的
     (SendCommand 下发指令 → DS 队列 → PollCommands 拉取 → AckCommand 回报),
     且定位靠 `match_id` + `player_id`,**只覆盖战斗内玩家**,不是玩家数据查询接口。
   - `ConfigTableAdminService`、`gm_command` 配置表:GM 指令的配置侧,同样非查询。
   - login.proto 文件头既有约定:「**HTTP endpoint 给运营后台 / 第三方 webhook 用,
     玩家客户端不直连**」——即运营后台走各业务服 HTTP 面的路子**早有定调**,只是还没有
     「按编号/按玩家查数据」这一类接口。
   - 客户端面隔离的现成模式:owner 服务那样在 Envoy 客户端入口
     `direct_response: 403` 挡掉(deploy/envoy/envoy.yaml「内部系统接口,不对客户端开放」)。

   即缺的不是「通道」而是「查询接口 + 前端」;`FindByPlayerNo` 应挂 login(账号库权威
   在此),按上述既有约定以内部/运营面暴露,不要塞进 `GmService`(那是战斗内写指令通道,
   职责与生命周期都不同)。

**落地建议**(等运营后台立项时一并做,§15.3 不提前建):`FindByPlayerNo` 放 login
(账号库权威在此),作内部 RPC 暴露,鉴权 + 脱敏 + 不经 Envoy 对客户端开放(CLAUDE.md §5.11
对「运维/调试 RPC」的既有要求)。**注意查不到的两种情形要分开回话**:编号不存在(打错了)
vs 编号存在但玩家刚注册还没补号——后者客服看到的是玩家界面显示「生成中」,不是查询故障。

### 3.6.1 一账号多角色 + 卖角色:编号跟**角色**走(2026-08-10 用户拍板)

> ⚠️ 本节初稿(同日早些时候)结论写反了——曾写「必须跟 account_id 走,不得跟 player_id」,
> 理由是「注册是账号事件」。**该结论已被用户推翻并作废**:本项目**角色可交易(卖角色,
> 按过户设计)**,前提就不成立。保留此处纠错记录,防止后来者从旧结论重新推导。

**拍板语义**:一账号建 N 个角色 = N 个编号。编号是**角色**的固有属性,不是账号的。

**理由(卖角色业务决定的)**:
- 角色是**可交易资产**,编号是其身份与资历凭证——「第 3 号角色」是定价要素(编号小=老角色);
- 过户时编号必须**跟着角色走**。若编号挂账号,角色卖给新账号后编号即变,角色资历凭空蒸发;
- 推论:编号**不能**是账号的派生属性,必须是角色行上的独立列(现实现已满足)。

**现状即正确形态,代码零改动**:今天 `player_no` 挂 `accounts` 表,而该表 PK 是
`player_id`——`player_id` 承担的正是角色身份(`players`/`player_roles` PK 同为它)。
改造时 `player_id` 下沉为角色实体 ID,`player_no` 随之落到角色表,天然「一角色一编号」。

改造时的落点(两处,均小):
① `player_no` + `uk_player_no` 随 `player_id` 落到**角色表**;`accounts` 分裂出的纯账号表
   (PK=`account_id`)**不带**该列;
② 补号任务扫描目标改为角色表的 `player_no IS NULL`;`GetPlayerNo` RPC 仍按 `player_id`
   (=角色 ID)查,**无需改签名**——它本来就是「查当前角色的编号」。

**由此新生的待拍板项(§6 已登记)**:
- **删角色是否回收编号**:若允许真删角色,「严格连续无空洞」只在分配时刻成立,删除后必有洞。
  建议不回收(编号是历史事实,回收会让两个角色先后拥有同一编号,交易场景下是欺诈风险);
  但这意味着「最大编号 = 累计创建角色数」而非「当前存活角色数」,策划看数须知此口径。
- **账号是否也要一个编号**:若策划将来要「这个**账号**是第几个注册的」,那是**另一个字段**
  (账号表上的独立计数器),不能复用本列。本方案不预留(§15.3 无真实需求不做)。

### 3.6.2 删角色不回收编号(2026-08-10 用户拍板)

**决定**:角色被删除后,其 `player_no` **永不回收**、永不再分配给任何其它角色。

**理由**:编号是**历史事实**(「全服第 N 个被创建的角色」)。回收会让两个不同角色在不同
时间持有同一编号——在**卖角色**业务里这是直接的欺诈载体(买家按编号认老号,回收后
新角色可冒充已注销的老号资历),且交易纠纷追溯将失去唯一锚点。

**机制保证(现实现已天然满足,无需新增代码)**:
- 计数器 `player_no_counter.next_no` **只增不减**——补号事务只做 `next_no = next_no + N`,
  无任何回退路径;发号序列因此单调,**物理上不可能重发已发过的号**;
- 补号任务只处理 `player_no IS NULL` 的行,已编号行永不重编;删除的行不会被重新扫到;
- 故「不回收」不依赖任何删除逻辑的配合:**无论角色行是物理删还是软删,都不会重号**。

**代价(须让策划知道口径)**:
- 「最大编号」= **累计创建**角色数,**不等于**当前存活角色数,两者差值 = 历史删除量。
  策划若拿最大号当「全服角色规模」看,在有删角色的版本里会系统性偏大;
- 编号在存活集合上**有洞**(§3.1 ① 的准确表述:发号序列连续,存活集合可有洞)。

**运维红线**:任何「重置计数器」「按存活角色回填编号」的操作都会打破上述保证并制造重号,
一律禁止。计数器只允许由补号事务推进;`player_no_counter` 已登记 §9.24 豁免(不清理)。

**衍生问题(与本拍板独立,留给多角色改造立项时定)**:删角色若采用**物理删除**,该编号将
「查无此人」——客服/交易纠纷拿编号追溯时查不到任何记录(编号既不回收也不指向任何行)。
若希望编号永久可追溯,需在改造时选择**软删除**(保留角色行 + 删除标记)。本文不预判,
因为它取决于角色删除的产品形态(是否可恢复、是否有冷却期),不属编号方案职责。

### 3.6.3 字段改名 register_no → player_no(2026-08-10 用户拍板)

**决定**:列 / 表 / 索引 / RPC / proto 字段一律由 `register_no` 改为 `player_no`。

**理由**:编号已拍板绑定角色实体(§3.6.1),而「register(注册)」在通行语境里是**账号**动作
——角色是**创建**出来的。留 `register_no` 会持续把人往「账号级编号」引:注释能防一时,
名字防一世。改后与既有命名体系配对成立:

| 标识 | 含义 | 给谁看 |
|---|---|---|
| `player_id` | 角色实体 ID(snowflake) | 系统 |
| `player_no` | 角色展示序号(本方案) | 策划 / 玩家 / 客服 |
| `role_id` | 职业配置 ID(CfgRole.Id) | 配置表 |

**刻意不叫 `role_no`**:`role_id` 已被职业配置占用,再来一个 `role_*` 会让「职业」与
「角色序号」两个概念在命名上纠缠,比改名前更糟。

**为什么此刻改**:生产**零注册路径**(§2.1)无存量数据,dev 库可重建,客户端尚未正式消费
该字段——现在是改名成本的最低点,每拖一天要碰的地方就多一处。

**改名清单**(全部已落码):列 `player_no`、唯一索引 `uk_player_no`、计数器表
`player_no_counter`、Go(`SweepPlayerNo` / `EnsurePlayerNoCounter` / `GetPlayerNo` /
`PlayerNoBatchSize` / `PlayerNoWatermarkLag` / 配置 `player_no_start`)、
proto(`LoginResponse.player_no`、`GetPlayerNoRequest/Response`、rpc `GetPlayerNo`、
HTTP `/v1/player-no/get`)、envoy jwt_authn rules path、日志 msg(`player_no_assigned` 等)、
文件名(`data/player_no.go`、本文档)。中文措辞统一为「角色编号」。

**迁移 `000005_rename_player_no` + `000006_reconcile_player_no`**:
- `000004` / `000005` 都保持原样(README 明定迁移一旦执行即 immutable);`000005` 负责普通
  old-only 库的 RENAME COLUMN / INDEX / TABLE 与列注释同步;
- 不能再声称「fresh init 下 000005 四步全跳过」:本机 `dev_migrate.ps1` 会先重放
  mysql-init(target),再在无 `schema_migrations` 的库上执行 000004/000005。000004 会补出 legacy
  三件套,000005 因 target 已存在而跳过 rename,从而留下双列/双索引/双计数器;
- `000006` 是唯一收敛门:old-only 原地改名,target-only 幂等保留,双对象先 fail-closed 检查
  同行值冲突和跨角色重号,再把不冲突 legacy 值补入 target;双计数器同 id 取
  `GREATEST(next_no)` 后删 legacy,最后统一列/计数器列/计数器表三处 COMMENT;
- 任何写入/改名之前先校验两套编号列必须是 `BIGINT UNSIGNED NULL`、计数器 `next_no`
  必须是 `BIGINT UNSIGNED NOT NULL`、新旧唯一索引必须各自是预期列上的单列唯一索引;
  形态漂移直接 guard 失败,不允许 COMMENT 的 `MODIFY COLUMN` 静默顺手改类型;
- `000006.down.sql` 有意 no-op:合并后无法安全判断并重建原始双对象状态;
- 真 MySQL 8.4 / TiDB 8.5.1 临时库矩阵均验证 old-only、target-only、双对象空 legacy、
  双对象兼容值补入、冲突 fail-closed,成功路径重复执行仍幂等;
- 用 `RENAME COLUMN` 而非 `CHANGE old new <type>`:后者需完整重复列定义,漏写
  `UNSIGNED`/`NULL` 会**静默**改变列语义;RENAME 不碰类型,是本场景唯一无脑安全的写法;
- old-only 路径只改元数据;双对象路径只补 target 空值并合并计数器水位,冲突时在任何写入/删除
  之前以 `__pandora_player_no_reconcile_data_conflict__` 语义化 guard 报错停止。

⚠️ **down.sql 里的 `@old_comment` 必须与 000004 的 COMMENT 逐字一致**(含旧词「注册编号」
与旧文档名),它是回滚的匹配目标而非本次改名的产物——批量改词时若把它一起换掉,条件判断
将永远不成立,每次回滚白发一次 DDL。该处已加注释警示(本次改名中真的误伤过一次)。

### 3.6.4 改名违反滚动升级,000007 expand 回补(2026-08-12,推翻 §3.6.3 的落地方式)

**§3.6.3 的改名决定本身不变**(编号绑角色实体,`player_no` 是最终名),被推翻的是**它的落地
方式**:`000006` 用 `RENAME COLUMN` / `RENAME INDEX` / `RENAME TABLE` + `DROP` 一次性把
legacy 三件套换掉,这是 **contract**,不是 expand。CLAUDE.md §9.21 要求「已有 RPC / 字段 /
语义不得原地禁用或硬切;删除能力必须走 expand → migrate → contract」——迁移一执行,还没排空的
旧 login 副本(Stable)读写的 `register_no` / `uk_register_no` / `register_no_counter` **当场消失**,
补号事务直接报错。改名成本最低点的判断(§3.6.3「生产零注册路径、无存量数据」)只说明**数据**
没风险,不能推出**二进制共存**没风险。事故档案:[INC-20260812-001](../incidents/2026-08-12-p0-published-migration-rolling-upgrade-break.md)。

**`000006` 已发布即 immutable,只能向前修**。`000007_player_no_expand_compat` 是纯加法:

| 对象 | expand 后状态 |
|---|---|
| `accounts.player_no` | 新名,新副本读写 |
| `accounts.register_no` | 旧名**重新建回**,与新名双写同值 |
| `uk_player_no` / `uk_register_no` | 两个单列唯一索引并存 |
| `player_no_counter` / `register_no_counter` | 两张计数器表并存,同一 `next_no` |

**双写与锁序(唯一正确写法)**:新版 `SweepPlayerNo` 在**同一个事务**里固定先
`SELECT ... FOR UPDATE` 锁 `player_no_counter`、再锁 `register_no_counter`,取
`next = MAX(两张表的 next_no)` 起算,一次 UPDATE 同时写两列,收尾把两张表推到同一 `newNext`。
旧 Stable 只锁 `register_no_counter`(单锁),因此两代二进制在这张表上天然串行化,**不存在
反向持锁路径,不会死锁**;`MAX` 水位保证任何一代已发出的号都不会被另一代重发。
新版还会把旧版只写了 `register_no` 的行**回补** `player_no`(同值,不消耗新号),所以
`SweepPlayerNo` 的返回值语义已从「本批新分配数」变成「本批处理行数(新分配 + 回补)」。

**读路径**:`GetPlayerNo` 同时取两列,优先 `player_no`,缺失回落 `register_no`;两列都有值
且**不相等**时返回 `ErrInternal`(不猜权威值)。登录主链对该错误 fail-soft(置 0,客户端显示
「生成中」),只有 §3.7 的补拉 RPC 会把它翻成非 OK。

**启动探针**:`EnsurePlayerNoCounter` 现在探 `SELECT player_no, register_no`,并在同一事务里
幂等初始化 + 对齐两张计数器水位。**部署顺序硬约束:必须先跑 000007 迁移,再滚 login 新版**;
顺序反了新版探针失败 → 补号任务停用(fail-soft,不拦启动),且**不会自愈**,必须重启进程。

**contract 的退出条件(尚未满足,不得提前做)**:
1. 所有旧 login 二进制排空(按 §9.21 确认无旧 Pod、无旧 release track);
2. 经过一个独立观测窗;
3. 另立一个更高版本迁移执行 `DROP`,`000007.down.sql` 有意 no-op —— 回滚服务版本**不等于**
   旧副本已排空,回滚时删兼容面会立刻打死仍在跑的旧副本。

**登记面**:`register_no_counter` 已登记 CLAUDE.md §9.24 豁免清单 + `dbcheck` registry
(恒 1 行,发号权威闸,不清理)。

### 3.7 首登必见「生成中」缺陷与补拉 RPC(2026-08-10 实测暴露,已修)

**现象**:dev 实测(账号 `test123`)客户端角色界面恒显示「角色编号 生成中」,而库里
`player_no=1` 早已落好。

**根因**(不是 bug 在补号任务,任务完全按设计工作):

| 时刻 | 事件 |
|---|---|
| 12:08:57 | 首登=注册,`accounts` 落行;此刻 `player_no` 尚为 NULL → `LoginResponse.player_no=0` |
| 12:09:12 | 补号任务编号成功(`player_no_assigned rows=1`),恰好 15s = 5s 周期 + 10s 水位滞后 |
| 之后 | 客户端**无处再取**:编号只在 Login 响应里下发一次,界面「刷新」按钮刷的是角色数据 |

**这不是边缘情况,是 100% 必现**:本项目「首登即注册」(§2.1),注册与登录是同一个请求,
**任何新玩家的首次会话里 `player_no` 必然是 0**。原设计注释写的「0=未分配,下次登录
即有值」低估了它——等于「每个新玩家第一次进游戏都看不到自己的编号」,产品上不可接受。

**修法(已落码)**:加 `LoginService.GetPlayerNo` 补拉 RPC——异步生成 + 客户端补拉是
标准配套,而不是把发号塞回登录路径(那正是 §3.2 否决的方案 A)。要点:
- **入参为空**:`player_id` 只从 JWT sub 取(Envoy 注入 `x-pandora-player-id`,同 SelectRole
  纪律),玩家只能查自己;该 path **必须**列进 `envoy.yaml` 的 `jwt_authn rules`
  ——未列到的 path 默认放行不验签,上游拿不到 player_id 会一律 `ErrUnauthorized`(已加)。
- **0 是 code=OK 的正常态**,不是错误:客户端据此继续显示「生成中」并稍后重试;
  拿到非 0 后编号永不再变,停止轮询。查询失败才返回错误码,**不得伪装成「编号 0」**
  (§9.22 不得冒充默认态),否则客户端会一直空等。
- **刻意不做会话现行性(sjti)复核**:只读、只能读自己、零副作用、不发凭据;顶号后旧
  设备读到自己的展示编号无安全含义。对比 SelectRole 必须复核——它签发 hub 票据。

**测试**:`internal/service/login_player_no_rpc_test.go` 四条(正常补拉 / 0 是 OK 非错误 /
无 player_id 硬拒 / repo 故障透传);`login_player_no_test.go`(Codex)覆盖 Login 响应带出。

**客户端实现(已落码)**:
- 官方 `GenClientProto.ps1 -UpdateLock` 已把协议锁推进到服务端 `11320853`,随后
  `-VerifyOnly` 通过;实际只改 login `.pb.h`、login `.pb.cc`、`ClientProto.lock.json` 三个生成文件。
- RPC 使用空请求和 `bWithAuth=true`。登录拿到 0 后立即查询,随后由 CoreTicker 每 3s + 最多
  0.75s jitter 单飞补拉;`OK+0` 保持“生成中”,非 0 写回即停。真实错误显式停轮询并显示失败,
  玩家可点角色界面刷新按钮重试,不得以 0 掩盖。
- 回调以 attempt、`SessionGeneration`、`PlayerId` 三重围栏防止登出/切号后的迟到写回;
  专用写回入口不推进会话世代。UI 分别展示实际编号、“角色编号 生成中”和
  “角色编号 获取失败，点击刷新”。
- 新增三类 Automation:codec 契约、当前会话写回围栏、RPC/source 契约。

**当前交付边界**:服务端源码链与 Go 生成物已进入稳定提交 `11320853`,但该提交标题遗漏了
仓库规范要求的 `[proto]` 标记;推送前须由人决定 amend 或明确记录例外,Codex 不擅自改历史。
客户端本轮未 SVN commit、未 push。本任务修改的 C++ 源码已编译、
`UnrealEditor-PandoraTests.dll` 已链接成功;完整 `PandoraEditor` 最终链接被无关并行改动
`MyMainView.cpp/.h` 阻断:已声明并调用但未实现 `EnsureClientPerfWidget()` /
`RefreshClientPerf(float)`,触发 LNK2019/LNK1120。Editor 随后能加载旧 `Pandora.dll` /
`PandoraProto`,却无法加载新 `PandoraTests.dll`(`GetLastError=127`,game module could not be
loaded):新测试 DLL 引用了本轮新增导出,旧主 DLL 不具备。因此三组 Automation 均未实际执行,
不是测试断言失败。
仍须在该无关阻断清除后跑通完整 UE 目标,并完成“首登生成中 → 无需重登变为非 0”的
真实 Envoy/JWT E2E;生成成功或测试 DLL 链接成功均不能代替这两道门禁。

**待复核边界**:当前 `AccountRepo.GetPlayerNo` 把 `sql.ErrNoRows` 与 `player_no IS NULL`
都映射为 `0,nil`。在“有效 JWT 的账号行必然存在”不变量下,正常补号窗口语义成立;若未来允许
账号行删除或出现数据损坏,缺行也会表现为 `OK+0`。届时必须把缺行改成非 OK,不能让客户端永久空等。

---

## 4. 为什么「注册洪峰」=「登录洪峰」

本项目注册是**首登即注**(现状 dev 懒注册如此;未来正式注册大概率也是首登创建)。
开服那一刻每次登录就是一次注册:**注册峰值 ≈ 登录峰值,不存在「注册 QPS 很低」的前提**。
03-account-tidb.sql:6 早已点名「开服首日是写入尖峰」。这正是 §3.2 否决同步取号、
以及 §5 必须把洪峰防线补齐的原因。方案 C 对此的回答:编号完全退出关键路径,
洪峰整形(§5.2)之后剩下的账单,与编号无关。

---

## 5. 开服登录/注册洪峰:分层对策

### 5.0 账单的硬底在哪

十万级登录 QPS 下(scale-cellular-20m.md §1):
- **bcrypt CPU**:cost=4 单次 ~1ms 量级 → 十万 QPS ≈ 百核级,可扛;
  **若按安全要求升到 cost=10(≈×64)→ 单次 ~60ms → 需数千核**,bcrypt 立刻变成登录
  路径最大 CPU 项。数字为估算口径,待实测复核(§16.10.③)。
- **`player_session_generations` upsert**:与登录 QPS 1:1 的 fail-closed 事务写。
  行按 player 打散无单行热点,天花板 = TiKV 集群写容量,靠加节点水平扩——但必须有
  容量规划数字,目前没有实测(§5.4)。

结论:**bcrypt cost 决策(拍板项 B)与登录排队(拍板项 C)必须一起定**——cost 升档
把 CPU 账单放大 64 倍,没有排队整形就是拿开服赌估算。

### 5.1 第 0 层:Envoy 边缘速率闸

anti-abuse-scene-entry.md §4.1 已有完整设计(`local_ratelimit` 对未鉴权 path 按 IP
令牌桶,验收口径「登录洪峰下 429、后端 QPS 削平」),是该文档落地序 5 的待办。
**本文不重造,只把它的优先级从「防外挂」提为「开服生存必需」**。它是唯一能在
bcrypt/DB 之前挡住流量的层。

### 5.2 第 1 层:登录排队/开服放量(待立项)

现状不存在(§2.6)。设计要点(立项时展开成独立设计文档):
- **语义对齐既有契约**:排队响应复用 §9.23 统一入口的 `WAIT`(含 `retry_after`)状态,
  与 anti-abuse 落地序 3(DS 容量 WAIT 化)同一形状;客户端排队 UI 按 §9.19/20 有界驱动
  (watchdog 到期重查,不新建状态机)。
- **准入口径**:全局令牌桶,容量数字来自下游最薄弱环节的实测(bcrypt 核数、session upsert
  TPS、hub 分配速率),不拍脑袋。
- **开服放量** = 准入闸初始值从小往上调的运营操作,不是新机制。
- **前置条件**:先跑 §5.4 的洪峰压测拿到真实天花板,再定排队系统规模(§15.3:
  不为想象的数字提前建设;2000 万 DAU 目标已拍板,立项本身有据)。

### 5.3 第 2 层:服务与依赖

- BBR 已 prod 机械强制(门禁-A),保底「过载丢请求不雪崩」——但 BBR 是**尾部防线**,
  丢的是已到达服务的请求,体验上等于登录失败重试风暴,不能替代 §5.1/5.2。
- 【必修-1】登录 5s deadline 内串行扇出:洪峰下任一下游变慢即大面积超时,
  该项在压测前审核已立案,此处只标注它是洪峰放大器,修复归原审核清单。
- bcrypt cost:拍板项 B。若维持 cost=4 需在文档明示接受口令强度弱点;若升 10,
  CPU 账单进 §5.2 的容量口径。存量 cost=4 哈希可在登录成功时懒升级(verify 通过后
  重 hash 落库),不停服(§9.16)。

### 5.4 第 3 层:存储与压测验证

- schema 打散已就绪(§2.3);TiDB 容量靠加 TiKV 节点,但**没有登录洪峰下的实测写容量数字**。
- 压测缺口:新增**注册/登录洪峰专项场景**(瞬时 connection storm + 全新账号建号,
  robot/stress 现有 devAutoRegister 链路即真实注册路径),产出:①bcrypt 两档 cost 的
  CPU 曲线;②session upsert TPS 天花板;③补号任务积压/追平曲线(顺带验证 §3.3)。
  按 §8 压测纪律执行(prev-summary、dbcheck 基线/对比)。

---

## 6. 拍板清单

| # | 待拍板 | 责任方 | 依赖 |
|---|---|---|---|
| A | 策划三问:①严格连续 ②给谁看 ③起始号;④存储形态 | 策划/用户 | **全部已定**(2026-08-10):①严格连续+④加列已落码;②用户拍板=客户端玩家可见,服务端与 UE 本地链路已落码(完整编译/E2E 边界见 §3.5 第 6/7/8 项);③= login 配置 `player_no_start`(默认 1,计数器初始化前可改) |
| B | bcrypt cost:维持 4(明示接受弱点)或升 10(账单进容量口径,存量懒升级) | 用户 | 与 C 联动 |
| C | 登录排队/放量立项 + Envoy local_ratelimit 优先级提级 | 用户 | §5.4 洪峰压测数据 |
| D | 洪峰压测专项排期 | 用户 | robot/stress 现有能力即可开跑 |
| ~~E~~ | **删角色不回收编号** | 用户 | ✅ **已拍板(2026-08-10)**,详见 §3.6.2 |
| F | **账号是否也要独立编号**(与角色编号并存):若策划要「这个**账号**是第几个注册的」,须在账号表另起一个计数器与列,**不得复用** `player_no`;现不预留(§15.3) | 策划/用户 | 同上 |

## 7. 关联

- `deploy/tidb-init/03-account-tidb.sql` —— 账号库单点定性、AUTO_INCREMENT 否决、打散手段
- `docs/design/anti-abuse-scene-entry.md` —— §4.1 Envoy local_ratelimit 设计、落地序 3/5
- `docs/design/scale-cellular-20m.md` —— 容量基线、region/cell 语义、login 全局薄层
- `docs/reviews/压测前审核-20260724.md` —— 必修-1(串行扇出)、门禁-A(BBR)、bcrypt cost 弱点
- `tools/migrate/migrations/pandora_social/000002_guild_counter_tables.up.sql` —— 计数器表先例
- `docs/design/stress-single-cell-client.md` —— 现有压测 ramp 口径(刻意平滑,无洪峰场景)
