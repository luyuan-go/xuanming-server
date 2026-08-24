# [INC-20260812-001][P0] 两个已发布迁移做 contract 而非 expand,滚动升级期新旧副本无法共存

> **状态**：已止血（未关闭）
> **类型**：`availability` / `near-miss`
> **环境**：本机 k8s / dev（生产未部署，见 §1）
> **首次发生时间（UTC）**：不适用（未在线上发生；缺陷随 `4e78155c` / `b7178d0c` 合入）
> **首次发现时间（UTC）**：2026-08-12 07:00 前后（本轮改动审查）
> **负责人**：待指定
> **受影响服务/版本**：`services/account/login`、`services/account/player`、`tools/migrate`；迁移 `pandora_account/000006`、`pandora_player/000007`
> **最后更新**：2026-08-24

## 0. 一句话结论

`pandora_account/000006`(角色编号改名)与 `pandora_player/000007`(段位分池)都用 `RENAME`+`DROP`
一次性删掉了旧列/旧索引/旧表，这是 **contract** 而不是 expand：迁移一旦执行，尚未排空的旧
Go 副本(Stable)读写的对象当场消失，直接违反 CLAUDE.md §9.16 / §9.21「零停机滚动更新」与
「删除能力必须走 expand → migrate → contract」。已由 `000007_player_no_expand_compat` 与
`000008_rating_pool_expand_compat` 两个纯加法迁移向前回补兼容面并双写，**线上零影响**
(见 §1)；contract 尚未执行，兼容面必须长期保留。

## 1. 影响与范围

- 玩家影响：**无**。两条链路的缺陷版本都没有真正上线。
- 服务影响：
  - `pandora_player/000007` 在 MySQL 8.4 上必然失败——`players.mmr` 上挂着 `idx_mmr`，
    `DROP COLUMN mmr, ALGORITHM=INSTANT` 报 **1845**，留下 `schema_migrations` **v7 dirty**；
    `deploy/k8s/migrate/job.yaml` 是硬门禁(`backoffLimit: 0`，Job 成功才允许滚业务 Deployment)，
    所以 v7 版 player 镜像从未滚动上线。
  - `pandora_account/000006` 若在旧 login 副本仍在跑时执行，`register_no` 三件套消失 →
    补号事务报错、编号展示功能中断（登录主链 fail-soft，不掉线）。
- 数据与安全影响：无数据丢失。段位存量按 §3.6.3 / PROGRESS 2026-08-11 的既定口径**本就重置**。
- 开始/结束时间：不适用（未在线上发生）。
- 是否仍可复发：**是**——见 §10 A-1，未来的 contract 迁移若原样重放 `DROP players.mmr,
  ALGORITHM=INSTANT` 会复现同一个 1845 dirty。
- 严重级别判定理由：按 §1「上线前发现但若上线会造成 P0 后果」建 `near-miss`。若 000006 在
  多副本 login 滚动窗口内执行，属「关键服务功能中断」；若 000007 在已上线环境执行，会把
  `schema_migrations` 打成 dirty 并**阻断此后全部发布**。

## 2. 第一现场与证据

### 2.1 症状

- 服务端症状：`pandora_player` 迁移 Job 失败退出，`schema_migrations` = `version=7, dirty=1`；
  此后每次发布被 `rejectDirtyOrNewer` fail-closed 挡住。
- 静态症状：`000006`/`000007` up.sql 中出现 `RENAME COLUMN` / `RENAME INDEX` /
  `RENAME TABLE` / `DROP COLUMN` / `DROP TABLE`，而对应的旧 Go 副本仍在读写这些对象。

### 2.2 原始证据

```text
tools/migrate/migrations/pandora_account/000006_reconcile_player_no.up.sql
  ALTER TABLE `accounts` RENAME COLUMN `register_no` TO `player_no`
  ALTER TABLE `accounts` RENAME INDEX  `uk_register_no` TO `uk_player_no`
  ALTER TABLE `accounts` DROP INDEX  `uk_register_no`
  ALTER TABLE `accounts` DROP COLUMN `register_no`
  RENAME TABLE `register_no_counter` TO `player_no_counter`
  DROP TABLE `register_no_counter`

tools/migrate/migrations/pandora_player/000007_rating_pool_partition.up.sql
  ALTER TABLE `players` DROP COLUMN `mmr`, ALGORITHM=INSTANT   ← MySQL 8.4 报 1845(mmr 上有 idx_mmr)

tools/migrate/main.go:573-580   已发布 000007 在 MySQL 8.4 报 1845 的取证与 quarantine 说明
```

### 2.3 已排除的噪声

- **「新代码把旧副本刚落的 default 段位分覆盖回退」不成立**：能与新副本共存的 Stable 是
  **pre-000007** 版(`8feb325a`)，它读写的正是 `players.mmr`，与新代码的「default 以
  `players.mmr` 为兼容权威 + 双写」**双向兼容**。所谓「000007 版 Stable」在物理上不存在——
  那份代码的 `EnsureProfile` 仍 `INSERT INTO players(..., mmr, ...)`，而同 commit 的建表脚本
  已无 `mmr` 列，上线即 1054；且其自带的 CI 真库门禁会先把它判红。
- **「000007 双计数器合并 UPDATE 会被 login 补号事务锁死」不成立**：`SweepPlayerNo` 每批必
  提交(实测一批 500 行 ≈ 277ms)，持锁窗口百毫秒级；真库压测 59 次探针 0 次 1205，且等待
  不随副本数无界增长(6→16 副本，最大等待 5.2s→6.5s)。

## 3. 时间线

| UTC 时间 | 组件 | 事件 | 证据 |
|---|---|---|---|
| 2026-08-10 | 设计 | 拍板 `register_no` → `player_no` 改名 | `docs/design/player-no-and-login-surge.md` §3.6.3 |
| 2026-08-11 | 设计 | 拍板段位按 `rating_pool` 分池、存量清空 | `PROGRESS.md` 2026-08-11（续） |
| 2026-08-11~12 | migrate | `000006` / `000007` 以 contract 形态合入 | `4e78155c`、`b7178d0c` |
| 2026-08-12 | migrate | MySQL 8.4 上 `000007` 报 1845，留 v7 dirty | `tools/migrate/main.go:573-580` |
| 2026-08-12 | 审查 | 本轮改动审查确认两处均违反 §9.21 | 本文档 |
| 2026-08-12 | migrate | `000007`(account) / `000008`(player) expand 回补落地 | `0fdb15f1` |

## 4. 调用链与关键变量

```text
migrate Job (backoffLimit=0)
  → golang-migrate m.Up()
  → 000006 / 000007 执行 RENAME / DROP
  → 旧 Stable 副本的 SQL 目标对象消失
  → login: SweepPlayerNo / EnsureRegisterNoCounter 报错(补号中断)
     player: EnsureProfile / ApplyMMRChange 报 1054(建档与结算中断)
```

| 变量/对象 | 创建位置 | 所有者与生命周期 | 是否共享/可变 | 事故中的作用 |
|---|---|---|---|---|
| `accounts.register_no` 三件套 | `000004` | 旧 login 副本读写 | 共享 | 被 `000006` 删除 → 旧副本补号失败 |
| `players.mmr` + `idx_mmr` | `000001_baseline` | 旧 player 副本读写 | 共享 | 被 `000007` DROP（且因 idx_mmr 触发 1845） |
| `schema_migrations` | golang-migrate | 全库单行 | 共享 | 1845 后留 `v7 dirty`，阻断后续全部发布 |

## 5. 根因

### 5.1 直接根因

改名/重构被当成一次性 DDL 完成，而不是「加新→双写→排空→删旧」三阶段。§3.6.3 用「生产零
注册路径、无存量数据」论证了改名成本最低点——该论证只覆盖**数据**风险，不覆盖**二进制共存**
风险，而 §9.21 约束的恰恰是后者。

### 5.2 触发条件

- 迁移执行时刻仍有旧版本 Go 副本在跑（滚动升级窗口内必然成立）；或
- `players.mmr` 上存在 `idx_mmr`（`000001_baseline:43` 起一直存在）→ `DROP ... ALGORITHM=INSTANT` 报 1845。

### 5.3 故障放大因素

- `deploy/k8s/migrate/job.yaml` `backoffLimit: 0` + `rejectDirtyOrNewer` fail-closed：一次
  dirty 就把**此后所有发布**卡死，不只卡本次。
- 迁移文件「一旦对 origin 暴露即 immutable」，不能就地改错，只能再加一个版本向前修。

### 5.4 为什么现有保护没有挡住

- 迁移契约测试只断言**片段存在**与 fresh-init 一致性，没有任何一条断言「up.sql 不得出现
  `DROP COLUMN` / `DROP TABLE` / `RENAME` / `CHANGE COLUMN`，除非本版被显式标注为 contract」。
  （`CHANGE COLUMN` 是 2026-08-24 补入的形态，与 `RENAME COLUMN` 兼容性等价；本条与 §6 的
  搜索模式表原先都漏了它，见 §6.1。同日收口轮又发现 `DROP` / `RENAME` 的关键字判据漏掉
  `DROP <列名>` / `DROP PRIMARY KEY` / `DROP FOREIGN KEY` / `RENAME TO`，见 §6.2；
  **当前权威形态表在 §7.3**，别照抄本条这句话。）
- `ALGORITHM=INSTANT` 的可行性没有在真 MySQL 8.4 上验证过：PROGRESS 2026-08-11 把「真实
  MySQL/TiDB 上跑 000007」明确列为**未验证/交接**项，缺陷就落在这个缺口里。
- CI 此前从不设 `PANDORA_TEST_MYSQL_DSN`，真库用例全 Skip 而 `go test` 打 `ok`（与
  INC-20260811-002 同一个遮蔽机制）。本批 `392ae6e1` 已把真库回归转成门禁。

## 6. 全仓同类问题扫描

- 扫描基线 commit：`0fdb15f1`
- 扫描目录：`tools/migrate/migrations/**`
- 搜索模式：`DROP COLUMN` / `DROP TABLE` / `DROP INDEX` / `RENAME COLUMN` / `RENAME INDEX` / `RENAME TABLE`
- Confirmed 同型命中：`pandora_account/000006`、`pandora_player/000007`（本事故两条）
- 结构性隐患：**迁移评审缺少「expand-only」机械门禁**，见 §10 A-2
- ~~未覆盖边界：本次未逐条复核 `pandora_social` / `pandora_auction` / `pandora_battle` 等其余
  migration set 是否存在同型 contract（审查扇出中断，见 §10 A-5）~~
  **（已由 §6.1 的 2026-08-24 补扫覆盖：这三套已由探测器本体逐条走过，零命中。）**

### 6.1 2026-08-24 补扫：原搜索模式漏了 `CHANGE COLUMN`

**上面那张搜索模式表本身就是缺陷证据**：它没有 `CHANGE`。`ALTER TABLE ... CHANGE [COLUMN]
old new <type>` 与 `RENAME COLUMN` 在兼容性上**完全等价** —— 旧列名当场消失，仍在跑的旧副本
查旧列名一律报错。2026-08-12 的这次全仓扫描因此从来没扫过这一形态，A-2 落码的门禁也照抄了
这张表，于是 2026-08-22 的 `pandora_trade/000005` 用 `CHANGE COLUMN frozen_gold frozen_amount`
做硬切，门禁一声没吭（作者自己在迁移头注释里写明了「不支持混跑」，机器却看不见）。

- 补扫日期：2026-08-24
- 探测器：`tools/migrate/expand_only_contract_test.go` 的 `destructiveDDL`，已补入
  `CHANGE COLUMN`（四种拼法：`COLUMN` 关键字可省、标识符可不带反引号，全部命中）
- 扫描方式：不是 grep，是拿探测器本体遍历全部嵌入 `*.up.sql`（剥行注释后判）
- **`CHANGE` 形态全仓命中：仅 `pandora_trade/000005_multi_currency_wallet.up.sql` 一处**
  （已按 §9.21 在文件头补 `-- CONTRACT:` 与「旧副本排空判据」，不入 grandfathered 清单）
- 其余命中仍是原有 5 条 grandfathered（`pandora_account/000005`、`pandora_account/000006`、
  `pandora_player/000007`、`pandora_leaderboard/000003`、`pandora_trade/000004`），
  形态与登记理由不变，无新增
- ~~结论：**§6 原扫描的漏洞只影响 `CHANGE` 一种形态，且只漏了 trade/000005 这一条**~~
  **← 这条结论是错的，2026-08-24 收口轮实测推翻，见 §6.2。留着不删是因为它本身就是证据：
  同一条"关键字判据不够、要看语法形状"的理由，当时只在 `CHANGE` 那一行落实了。**
- 上面「未覆盖边界」列的 social / auction / battle 三套本轮已由探测器逐条走过，零命中

教训写在这里给下一个人：**扫描模式表与门禁探测器必须是同一份**。这次是两份各写一遍，
门禁补了形态、文档没补，等于书面留下一张"我们扫过了"的假证明。

### 6.2 2026-08-24 收口轮：`CHANGE` 不是唯一的漏，`DROP` / `RENAME` 漏得一模一样

**§6.1 当时的结论「原扫描的漏洞只影响 `CHANGE` 一种形态」是错的，已在上面划掉。**
§6.1 只把 `CHANGE` 改成语法形状判据，同一条理由**逐字**适用于 `DROP` 与 `RENAME`，
却只在 `CHANGE` 那一行落实了。第三轮复核**拿探测器本体喂样本**，下面六条当时全返回空：

| 样本 | 改前探测结果 | 为什么漏 |
|---|---|---|
| ``ALTER TABLE `t` DROP `col`;`` | `[]` | MySQL 的 `COLUMN` 关键字可省，与 `CHANGE` 完全同理 |
| `ALTER TABLE t DROP col;` | `[]` | 同上，且标识符裸写 |
| ``ALTER TABLE `t` DROP PRIMARY KEY;`` | `[]` | 旧正则是 `\bDROP\s+(INDEX\|KEY)\b`，`DROP` 后面是 `PRIMARY` 不是 `KEY` |
| ``ALTER TABLE `t` DROP FOREIGN KEY `fk`;`` | `[]` | 同上 |
| ``ALTER TABLE `old` RENAME TO `new`;`` | `[]` | ALTER 内改表名的正规写法；旧正则只认独立语句 `RENAME TABLE a TO b` |
| ``ALTER TABLE `old` RENAME `new`;`` | `[]` | 同上，`TO` 还能省 |

六条全是「旧副本的目标对象当场消失」，与 `CHANGE` **完全同级**。

- 处置：`DROP` / `RENAME` 一并改成语法形状判据（`tools/migrate/expand_only_contract_test.go`
  的 `destructiveRule` + `dropObjectHeads` / `renameObjectHeads`）。Go 的 regexp 是 RE2、
  没有 negative lookahead，「`DROP` 后面是对象名而不是开启另一种形态的关键字」只能在正则
  命中后二次过滤，所以多了 `rejectHeads` 这一层。
- 六条样本已逐条落成 `TestDestructiveDDLDetector` 用例；**变异验证**：把 `DROP` / `RENAME`
  两条正则换回旧的关键字写法，这 7 个子用例（含防重复计数那条）立即转红，换回后转绿。
- **全量重扫零新增误判**：拿新旧两版探测器同时遍历全部 48 份嵌入 `*.up.sql`，
  **命中集合逐份完全一致（差异 0 份）**。仍然只有 6 份命中，与 §6.1 一字不差：
  5 条 grandfathered + `pandora_trade/000005`（走 `-- CONTRACT:`）。形状化没有把流水线卡死。
- 顺带修掉一个会让整道门禁瞎掉的剥注释缺陷，见 §7.3 末尾的 `stripLineComments` 条。

教训（补在 §6.1 那条后面）：**"同一份"不只指同步更新，还指同一套判据**。
§6.1 把两份同步了，同步的却是一份仍然漏 `DROP col` / `DROP PRIMARY KEY` / `RENAME TO` 的表，
"假证明"因此升了一级 —— 从"文档没跟上门禁"变成"文档把一份有盲区的清单抬成了权威"。

## 7. 处置与永久修复

### 7.1 临时止血

| 动作 | 状态 | 证据 | 风险/回滚 |
|---|---|---|---|
| `tools/migrate` 加 `repairPandoraPlayerV7Dirty` 精确 quarantine（校验 000007 正文 SHA-256 + 中间 schema 形态后标 clean，再跑 000008） | 已落码 | `tools/migrate/main.go:586-706` | 仅认 `version==7 && dirty`，任一前置不符 fail-closed；不是通用 force |

### 7.2 永久修复

| 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|
| `pandora_account/000007` expand：加回 `register_no` / `uk_register_no` / `register_no_counter` | 已落码 | `000007_player_no_expand_compat.up.sql` | `TestPandoraAccountV7PlayerNoExpandCompatibilityContract` |
| login 双锁双写（先锁 `player_no_counter` 再锁 `register_no_counter`，取 MAX 水位，双列同写） | 已落码 | `services/account/login/internal/data/player_no.go` | `TestPlayerNo_MySQLAndTiDB_StableCanaryShareOneAllocator` |
| `pandora_player/000008` expand：加回 `players.mmr` + `idx_mmr`，从 `player_mmr` 回填 default | 已落码 | `000008_rating_pool_expand_compat.up.sql` | `TestPandoraPlayerV8RestoresRollingCompatibility` + 真库 7 场景矩阵 |
| player 服务 default 池以 `players.mmr` 为兼容权威并双写 | 已落码 | `services/account/player/internal/data/mmr_repo.go` | `TestApplyMMRChangeRatingPoolExpandCompatibility_MySQL` |
| **quarantine 增加 `player_mmr` 行数守卫**：列/索引形态区分不了「000007 半途 1845」与「已到 v8 的库」（000008 恰好把 000007 想删的列和索引原样加回来，两者 schema 逐列逐索引相同）。唯一判据是数据——000007 只 CREATE 不回填，真中间态恒 0 行。非空即 fail-closed | 已落码 | `tools/migrate/main.go`（`validatePandoraPlayerV7DirtyShape` 末段） | `v7_dirty_but_pool_data_exists` 场景，**真 MySQL 8.4 先红后绿**：摘掉守卫时 `migrateTarget` 返回 `err=<nil>`（静默洗白并覆写 `players.mmr`），加回后 fail-closed |
| `register_no_counter` 登记 §9.24 + dbcheck registry | 已落码 | `CLAUDE.md` §9.24、`tools/migrate/cmd/dbcheck/main.go` | `go test ./tools/migrate/...` 转绿（修前 `TestFreshInitTablesAreRegistered` / `TestMigrationTablesAreRegistered` 双红） |
| CI 强制 MySQL 8.4 + TiDB 8.5.1 真库回归 | 已落码 | `392ae6e1`（`ci_db.ps1` / `docker-compose.ci-db.yml` / `Jenkinsfile`） | `ci_db_contract_test.ps1` |

### 7.3 防复发规则

- `CLAUDE.md` §9.21：已有条款即本事故判据，本次未新增条款，改为在设计文档落地说明——
  见 `docs/design/player-no-and-login-surge.md` **§3.6.4**（改名的正确落地方式 + contract 退出条件）。
  （此处引 §3.6.4 **是对的**：那一节讲的正是 000007 expand 回补与 contract 退出条件。
  只有引「`CHANGE old new <type>` 这个拼法」时才该引 §3.6.3，见本节末尾。别把两处一起改。）
- `CLAUDE.md` §9.24：新增 `register_no_counter` 登记。
- **expand-only 机械门禁**(A-2，已落码)：`tools/migrate/expand_only_contract_test.go`。
  遍历全部 `*.up.sql`(剥行注释后判)，命中下列**九种**形态即失败，除非二选一：
  ①文件头写 `-- CONTRACT:` **且**写明「旧副本排空判据」；②登记在
  `grandfatheredContractMigrations`(仅限本门禁上线前已对 origin 暴露、按
  `tools/migrate/README` 不可再修改的历史迁移，**只减不增**)。

  下表是**从 `destructiveDDL` 逐条誊下来的事实**，不是另写一份规格。改探测器必须同步改这张表
  ——§6.1 / §6.2 两次翻车都出在"两份各写一遍"。

  | 形态（探测器条目名） | 判据 | 备注 |
  |---|---|---|
  | `DROP COLUMN` | 语法形状：`DROP [COLUMN] <名>` | **2026-08-24 收口轮改为形状判据**。`COLUMN` 关键字可省、标识符可不带反引号，省了/裸写照样命中 |
  | `DROP TABLE` | 关键字 | |
  | `DROP INDEX` | 关键字 `DROP INDEX` / `DROP KEY` | |
  | `DROP PRIMARY KEY` | 关键字 | **2026-08-24 收口轮补入**。旧正则 `\bDROP\s+(INDEX\|KEY)\b` 抓不到它（`DROP` 后面是 `PRIMARY`） |
  | `DROP FOREIGN KEY` | 关键字 | **2026-08-24 收口轮补入**，同上 |
  | `RENAME COLUMN` | 关键字 | |
  | `RENAME INDEX` | 关键字 `RENAME INDEX` / `RENAME KEY` | `RENAME KEY` 是 2026-08-24 补的同义拼法（与 `DROP INDEX\|KEY` 对称） |
  | `RENAME TABLE` | 语法形状：`RENAME [TABLE\|TO\|AS] <名>` | **2026-08-24 收口轮改为形状判据**。旧正则只认独立语句 `RENAME TABLE a TO b`，`ALTER TABLE old RENAME [TO\|AS] new` 三种拼法全漏 |
  | `CHANGE COLUMN` | 语法形状：`CHANGE [COLUMN] <旧名> <新名> <类型>` | 2026-08-24 补入；与 `RENAME COLUMN` 兼容性等价。四种拼法全认 |

  **本表之外仍然存在的盲区（刻意保留，别当成"扫过了"）**：

  - `DROP PARTITION` / `DROP CONSTRAINT` / `DROP CHECK` —— 探测器的 `dropObjectHeads`
    把这三个关键字从「省了 `COLUMN` 的列名」里放过，而它们**没有各自的条目**，
    因而本门禁完全不管。理由：本仓零分区表、零 `CHECK` 约束，且 `DROP CONSTRAINT` /
    `DROP CHECK` 是放松约束、不是让旧副本的目标对象消失。**一旦本仓开始用分区表，
    必须先给 `DROP PARTITION` 补一条**，不得指望这张表挡住它。
  - `#` 与 `/* */` 注释不剥（只剥 `--`）。本仓迁移一律用 `--`，改了反而多一套状态机要维护。
  - 字符串字面量里的英文散文会**误报**（见下条取舍）。误报是保守方向，不是漏报。

  `DROP` / `RENAME` / `CHANGE` 三条的判据是**语法形状**（例如 CHANGE 后跟得出两个标识符再跟一个类型词），
  不是"CHANGE 后面紧跟 COLUMN 或反引号"。后者是 2026-08-24 第一版的写法，
  变异实测会漏 `CHANGE frozen_gold frozen_amount BIGINT`（裸标识符 + 省 COLUMN，合法 MySQL，
  `docs/design/player-no-and-login-surge.md` **§3.6.3**（449-499 行）第 491 行讨论改名时用的正是
  这个拼法；那句原文是「用 `RENAME COLUMN` 而非 `CHANGE old new <type>`」。**不是 §3.6.4** ——
  §3.6.4 从 500 行才开始，讲的是 000007 expand 回补，本文档一度把这两节引串了）。
  取舍：字符串字面量里的散文 `COMMENT 'change this column now'` 仍会误报，**刻意接受** ——
  误报的代价是改一句措辞，漏报的代价是生产上打死旧副本。取舍本身由
  `TestDestructiveDDLDetector` 的同名用例钉住。

  配套 `TestGrandfatheredContractListIsExact` 反向断言清单里每条都**确实还是**破坏性迁移
  且真实存在，防止这张表退化成永久豁免后门。
  已收录的 5 条历史违规：`pandora_account/000005`、`pandora_account/000006`、
  `pandora_player/000007`(本事故两条)、`pandora_leaderboard/000003`、
  `pandora_trade/000004`(后两条是 json→pb 表示法切换，同样未经 expand 窗口)。
  `pandora_trade/000005` **不在**清单里：它走 ①，文件头有 `-- CONTRACT:` 与排空判据。
  **变异验证**：摘掉任一条 grandfathered 登记后两条门禁立即转红(`... 含破坏性 DDL DROP COLUMN`
  / `... 在嵌入迁移里不存在`)，还原后转绿 —— 门禁不是空转的。
- **迁移契约测试的正向断言一律查剥注释后的正文**(2026-08-24 补)：
  `strings.Contains(原文, "必须有的守卫")` 会被一行同文注释满足。变异实测：删掉
  `000005` 里真正的 `AND table_name = 'player_currency'`、在上面补一条 `--` 同文注释，
  查原文的断言由红转绿。反向断言（"不得出现 X"）相反，查含注释的原文更严，保留原样。
- **剥行注释必须跳过字符串字面量**(2026-08-24 收口轮补)：主门禁判的是
  `stripLineComments` **之后**的正文。上一版是"整行从第一个 `--` 起截断"，不认字面量；
  而本仓的条件迁移把整条 DDL 装在单引号字面量里（000005 的 `PREPARE` 写法），
  字面量里出现 `--` 是完全合法的正文。复核实测：

  ```
  SET @s := 'ALTER TABLE `t` COMMENT = ''a--b'', DROP COLUMN `x`';
  destructiveHits(原文)                    -> [DROP COLUMN]
  destructiveHits(stripLineComments(原文)) -> []
  ```

  也就是说**字面量里带 `--` 的破坏性 DDL 对门禁完全隐形**，而那个 `--` 不必是人写的注释——
  `COMMENT` 文案里一个破折号、一段 CSV 样例就能触发。已改成按单引号 / 反引号状态跳过
  （`''` 是转义不是结束），由 `TestStripLineCommentsSkipsStringLiterals` 钉住；
  **变异验证**：换回旧的按行截断实现，该用例前两条立即转红。
  刻意没做：单引号状态**不在换行处复位**（MySQL 字符串本就允许跨行，复位是错的）；
  代价是全文件若有落单单引号，其后的注释会被当正文留下——那是**保守**方向（注释里的散文
  去撞破坏性 DDL 正则会红给你看），不是漏报方向。
- **单条迁移的"破坏性 DDL 只许 N 条"必须复用探测器**(2026-08-24 补)：
  自己 `strings.Count(upper, "CHANGE COLUMN")` 只守 7 种形态里的 1 种。
  用 `destructiveOccurrences` 并断言**命中集合**恰好等于预期那几条 ——
  只断言条数不行，删一条 CHANGE 换一条 DROP TABLE 同样是 1 条。

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| dbcheck 登记契约 | FAIL ×2 | PASS | `go test ./tools/migrate/...` | 见 §7.2 |
| 编译 + vet（login / player / migrate） | — | PASS | `go build` / `go vet`（按 go.work use 列表） | 本轮实跑 |
| 单测（login / player / migrate 全模块） | — | PASS | `go test ./services/account/... ./tools/migrate/...` | 本轮实跑 |
| 真库迁移矩阵（fresh / v4 / v6 / v7-clean / v7-dirty-exact / v7-dirty-mismatch） | — | **未在本轮执行** | 需 `PANDORA_TEST_MYSQL_DSN` / `PANDORA_TEST_TIDB_DSN` | 用例已备（`player_migration_test.go`），本轮无 DSN → Skip |
| `go test -race` | — | **未执行** | 需 CGO Linux | — |
| 玩家 E2E（滚动窗口内新旧副本共存） | — | **未执行** | — | — |

## 9. 部署、回滚与观察

- 修复 commit：`0fdb15f1`（**注意**：该提交把本次 expand 修复、`392ae6e1`/`6fef1cb6` 两个
  在途提交、以及 4 份无关的 Agones Fleet 版本 yaml 一起推上了 `origin/main`，commit message
  也不符合 CLAUDE.md §4 的 `<type>(<scope>): <subject>` 格式）
- 构建产物/镜像 digest：未构建
- 部署时间与目标环境：**未部署**
- 回滚条件和步骤：`000007`/`000008` 的 `down.sql` 均**有意 no-op**——回滚服务版本不等于旧副本
  已排空，回滚时删兼容面会立刻打死仍在跑的旧副本
- 观察窗口、指标与结果：未开始

## 10. 剩余风险与行动项

| ID | 严重级别 | 行动项 | 负责人 | 状态 | 目标/关联 Incident |
|---|---|---|---|---|---|
| A-1 | P1 | **contract 迁移不得原样重放 `DROP players.mmr, ALGORITHM=INSTANT`**：`idx_mmr` 在列上，必先删索引或改算法，否则复现同一个 1845 dirty | 待指定 | 未开始 | 本 Incident |
| A-2 | P1 | 加 **expand-only 机械门禁**：迁移契约测试断言 up.sql 不得出现 `DROP COLUMN`/`DROP TABLE`/`DROP INDEX\|KEY`/`RENAME *`/`CHANGE [COLUMN] 旧 新 类型`，除非文件头显式标注 `-- CONTRACT:` 并写明旧副本排空判据 | — | **已落码**（`CHANGE` 形态 2026-08-24 补入见 §6.1；同日收口轮把 `DROP` / `RENAME` 一并改成语法形状判据并补 `DROP PRIMARY KEY` / `DROP FOREIGN KEY`，全仓重扫零新增命中，见 §6.2。权威形态表在 §7.3） | `tools/migrate/expand_only_contract_test.go`；见 §7.3 |
| A-3 | P1 | **contract 时必须反向回填 `player_mmr ← players.mmr`**：旧副本结算只写 `players.mmr` 不写 `player_mmr`，删列瞬间玩家 default 段位会回退到最后一次新副本写入的值。`000008` 注释只写了「以后删」，没写这一步 | 待指定 | 未开始 | 本 Incident |
| A-4 | P2 | `000008` 的兼容回填是一条不分批的多表 UPDATE，会对**每个已有 default 记录的玩家**的 `players` 行加记录锁并持到语句提交（真库实测 15 万行 ≈ 18s / 150001 把锁），期间这些玩家的 `ApplyMMRChange` `SELECT ... FOR UPDATE` 会等到 `innodb_lock_wait_timeout`(targets 配 15s) 后批量报 1205。**在受支持发布路径上该语句恒 0 行**（000007 必 1845 失败 → v7 代码从未上线），风险只存在于「按当前 `04-player-tables.sql` 全新初始化且 v7 代码跑过」的库。修法只能是按主键游标分批 + 每批独立提交，或在文件头写明该取舍（加 `WHERE p.mmr <> pm.mmr` **无效**：RR 下锁在判谓词之前就加） | 待指定 | 未开始 | 本 Incident |
| A-8 | P2 | **expand 窗口内老副本会把显式池结算吞进 `players.mmr`（=default 投影）**：pre-000007 副本不认识 `rating_pool`，对任何池的 `player.update` 都只 `UPDATE players SET mmr=?` 并写一条 `rating_pool` 由列 DEFAULT 补成 `'default'` 的 `mmr_history`。现网 4 个 ELO 关卡**全部**配非 default 池，所以 player 单独回滚到 Stable 期间为 **100% 排位局**记错池，而 `mmr_history` 幂等键不含池 → 重投也补不回来。**可恢复**（`mmr_history JOIN battles → map_id → 关卡表段位池` 可确定性判出全部错池行）。欠账：①写明修复口径；②`mmr_repo_mysql_test.go` 自称钉住「显式池不串分」，实际只模拟了老副本写 default 一场，旧→新方向的显式池组合从未覆盖，不满足 §9.21「验证 Stable↔Canary 组合」 | 待指定 | 未开始 | 本 Incident（与 A-3 并列） |
| A-9 | P2 | **quarantine 目标库白名单与 `validMigrationDatabaseMapping` 冲突**：前者只认 `database == "pandora_player"` 或 `pandora_player_mig_it_` 前缀，后者（`main.go:385`）与 `tools/migrate/README` 明面允许 `<migration_set>_<后缀>` 分片库名。若将来给 player 引入分片/额外物理库，MySQL 8.4 上全新建库会卡在 v7 dirty 且 quarantine 拒绝介入，自动化发布链路永久阻断（需 DBA 手工 `UPDATE schema_migrations SET dirty=0`，非不可恢复）。**当前不可触发**：全仓 player 库名恒为精确 `pandora_player`（infra.md 只批准 auction 分片）。正确修法是在 `loadTargets` 阶段就对 `pandora_player` 拒绝前缀库名，把矛盾提前到清单校验 | 待指定 | 未开始 | 本 Incident |
| A-5 | P1 | **审查两轮都没跑完**：`migrate-quarantine` 第二轮补回（产出本文档 §7.2 那条 P1 与 A-9），但 **`ci-and-proto` 与 `cross-cutting` 两轮均因连接中断未返回**，复核 agent 两轮各挂 4 条 / 4 条。累计 27 条发现，仅 9 条进入复核、3 条成立、2 条推翻，**18 条既未确认也未证伪**（清单见 §10 附注）。**尚未有任何一次完整的跨切面扫描**：其余 migration set 的同型 contract、proto 新字段是否真有读写方与 fail-closed 分支、cpp pb 与 UE 仓库的同步断裂、CI 容器版本与 skip 白名单，全部零覆盖 | 待指定 | 未开始 | 本 Incident |
| A-6 | P2 | `0fdb15f1` 把 4 份 Agones Fleet 版本 yaml（battle / battle-canary / hub / hub-canary，`r1971→r1977`）与本次 expand 修复混在同一提交推上 `origin/main`，未单独验证版本一致性 | 待指定 | 未开始 | 本 Incident |
| A-7 | P3 | `pandora_social` / `pandora_auction` / `pandora_battle` 等其余 migration set 未做同型 contract 扫描 | — | **已完成**（2026-08-24 由探测器本体逐条走过全部 48 份 `*.up.sql`，零命中；见 §6.1 / §6.2） | 本 Incident |

**A-5 附注:18 条未裁决发现**(按初判严重度)。它们既未被确认也未被证伪，**不得**当成"已审查通过"：

| 初判 | 发现 | 位置 |
|---|---|---|
| P1 | quarantine 让 MySQL 与 TiDB 在同一 v8 版本上产出不同玩家段位数据，MySQL 侧没执行 000007 声明的"存量重置"口径 | `tools/migrate/main.go` |
| P2 | `EnsurePlayerNoCounter` 改成持双行 `FOR UPDATE` 的同步启动事务且用无超时 ctx，一次锁等待失败就让该副本**终生**停用补号并拖慢 Pod 就绪 | `login/internal/data/player_no.go` |
| P2 | 000007 的数据冲突 guard 排在所有 DDL 之后，主升级路径上恒等于 0（形同虚设）；真正会炸的 `ADD UNIQUE KEY` 反而排在它前面 | `pandora_account/000007...up.sql` |
| P2 | 回填方向(`player_mmr→players.mmr`)与运行期权威方向相反，重跑 000008 up 会清掉老副本写入的分 | `pandora_player/000008...up.sql` |
| P2 | `EXISTS(mmr_history rating_pool='default')` 判据推翻 000007 的存量重置口径，并会被保留期清理反转 | `player/internal/data/mmr_repo.go` |
| P2 | 该 `EXISTS` 在结算热路径上做无可用索引的全历史扫描 | 同上 |
| P2 | §9.22 唯一权威口径三处自相矛盾；contract 无编号、无排空判据 | `deploy/mysql-init/04-player-tables.sql` |
| P2 | 形态白名单漏掉 `player_mmr.updated_at`，残缺表被判为"精确 1845 形态"并标 clean | `tools/migrate/main.go` |
| P2 | quarantine 后第二次 `m.Up()` 覆盖 `applyErr`，原始 1845 证据丢失且库落到 quarantine 自己救不了的 v8 dirty | 同上 |
| P2 | `idx_mmr` 无任何现役查询使用，且它正是 000007 触发 1845 的成因 | `pandora_player/000008...up.sql` |
| P3 | `SweepPlayerNo` 返回值改成 `len(pending)` 后，`player_no_assigned` 的 `rows` 不再是"本批新发号数" | `login/internal/data/player_no.go` |
| P3 | `GetPlayerNo` 末尾两条分支是死代码 | `login/internal/data/account.go` |
| P3 | fresh-init 的 `player_no` 列注释与迁移链终态不一致，而守护这一致性的测试断言在本次被删掉 | `deploy/mysql-init/02-account-tables.sql` |
| P3 | `change.Baseline` 在 default 池成为死参数，`base_mmr` 配置与迁移硬编码的 1500 会静默分叉 | `player/internal/data/mmr_repo.go` |
| P3 | `fakeRepo` 未复刻 default 池新语义，biz 层测试对本次改动的核心风险完全不敏感 | `player/internal/biz/player_test.go` |
| P3 | quarantine 成功路径会被 defer 里的 Unlock/Close 错误染成失败，而 `SetVersion` 已经提交 | `tools/migrate/main.go` |
| P3 | 测试库前缀 `pandora_player_mig_it_` 是编译进生产二进制的自动 force 后门，没有任何 environment 门 | 同上 |
| — | 「000008 回填是不分批 JOIN UPDATE」两轮复核**结论相反**（第一轮成立降 P2 并附真库实测，第二轮判推翻）。已按第一轮的实测口径记为 A-4，判定待复跑 | `pandora_player/000008...up.sql` |

## 11. 关闭审核

- [x] 直接根因和放大因素均有证据
- [x] 修复前失败、修复后通过的回归存在（dbcheck 登记契约）
- [ ] race/集成/故障注入达到本事故风险要求
- [ ] 同类代码扫描完成（仅覆盖 account/player 两个 set，见 A-7）
- [ ] 目标环境已加载可追溯的新产物（未部署）
- [ ] 玩家路径、恢复和补偿路径验证通过
- [ ] 观察窗口无复发
- [ ] 剩余风险已解决或另建 Incident/任务（A-1..A-7 全部未开始）
- [x] 文档已脱敏且时间线时区明确

**关闭结论与审批人**：未关闭。expand 兼容面已落地并有回归，但 contract 退出条件、A-1/A-2/A-3
三条防复发项与 A-5 的审查补跑均未完成；真库迁移矩阵与玩家 E2E 零执行。
