-- 单币种 gold → 多币种钱包(2026-08-22,CLAUDE.md §5.12 非负金额用无符号 + 通用货币系统)。
--
-- # 改了什么
--
--   ① 新建 `player_wallet`:PK (player_id, currency_kind),amount BIGINT UNSIGNED。
--      从此"加一种货币" = 多写一个 currency_kind 值,不加列、不加表、不改接口。
--   ② `inventory_ledger` 加 `result_currencies` VARBINARY(256):幂等重放的**多币种**余额快照
--      (pb CurrencyBalancesStorageRecord)。旧的 `result_gold` 保留只读,见下方"为什么不删"。
--   ③ `auction_escrow.frozen_gold` 更名 `frozen_amount`,并加 `currency_kind` 列:
--      买单托管从"只能冻金币"变成"冻某一种货币"。
--
-- # 为什么新建 player_wallet 而不是原地改 player_currency
--
-- 旧表是 `PRIMARY KEY (player_id)`,多币种需要 `PRIMARY KEY (player_id, currency_kind)`。
-- **TiDB 不支持 DROP 聚簇主键**(整数单列 PK 默认聚簇),`ALTER TABLE ... DROP PRIMARY KEY`
-- 会直接报 Unsupported;绕过它要走"建新表 + 搬数据 + RENAME"的换名舞,而 RENAME 序列在
-- 中途被杀会留下半迁移状态,重跑时很难自愈。
-- 直接新建一张终态表、把旧表留成只读存量,是唯一在 MySQL 8 与 TiDB 上都幂等可重跑的路径,
-- 也正好是 expand → migrate → contract 的 expand 阶段(§9.16)。
-- contract(DROP TABLE player_currency)留给后续迁移,等确认没有任何副本再读旧表。
--
-- # 为什么 result_gold 不删
--
-- `result_gold` 存的是首次执行后的金币快照,重放时原样返回(§9.7 幂等)。
-- 新格式是 pb 二进制,**无法用 SQL 把整数转成 pb 字节**(varint 编码要在应用层做),
-- 所以存量行没法就地转换。代码侧按"result_currencies 非空则用它,否则把 result_gold
-- 当成金币余额"读——存量行的重放结果因此保持不变。
-- 等 `ledger_retention_days`(默认 90 天)把存量行清完,再走 contract 删列。
--
-- # 金额上限与列类型
--
-- 应用层 `data.MaxCurrencyAmount = 2^62`,远小于 BIGINT UNSIGNED 上限;
-- 加钱越界返回 ERR_INVENTORY_CURRENCY_OVERFLOW 而不是让列回绕。
-- 扣钱一律"先 FOR UPDATE 锁行读出、在 Go 里比较、再写绝对值",SQL 里不出现 `amount - ?`
-- ——UNSIGNED 列上的负结果在严格模式抛 1690、非严格模式**静默截断成 0**,
-- 后者等于把"扣款失败"变成"余额清零",必须从写法上排除。
--
-- # ⚠️ 发布顺序:本迁移与新 inventory 二进制必须一起上,不支持混跑
--
-- `auction_escrow.frozen_gold` 更名 `frozen_amount` 是**硬切**:迁移跑完之后,
-- 仍在运行的旧 inventory 副本会因为查不到 `frozen_gold` 列而让拍卖冻结 / 退还全部报错。
-- `player_wallet` 与两个 ledger 新列是纯新增(旧副本读不到但也不会崩),只有这一处不是。
--
-- 按 `§3.1`(首次生产上线日期未填写)当前不要求兼容旧构建,所以选了更干净的更名而不是
-- "加新列 + 双写 + 以后再删"。日期一旦填写,同类改动必须改走 expand → migrate → contract。
--
-- 实操顺序:停 inventory → 跑迁移 → 起新版 inventory。
-- 若必须零停机,先把 auction 的挂单 / 撤单入口临时关掉,让 escrow 写路径静默再迁。
--
-- 幂等:全部语句先查 information_schema 再决定是否执行,可重复跑;fresh 库由 baseline +
-- 本迁移得到同一终态。每条 ALTER 只带一个子句(TiDB multi-schema change 拿每个子句
-- 与语句执行前的原表结构比对,合并写法会出现假冲突;详见 000004 顶部注释)。

-- ---------------------------------------------------------------------------
-- ① player_wallet:多币种余额终态表
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `player_wallet` (
    `player_id`     BIGINT UNSIGNED  NOT NULL COMMENT '玩家 ID(snowflake,§11)',
    `currency_kind` INT              NOT NULL COMMENT '货币类型(pandora.common.v1.CurrencyKind:1=金币 2=钻石 3=荣誉;0/未知值应用层拒)',
    `amount`        BIGINT UNSIGNED  NOT NULL DEFAULT 0 COMMENT '余额(>=0 由列类型保证;上限 2^62 由应用层 MaxCurrencyAmount 保证)',
    `updated_at`    DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`player_id`, `currency_kind`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家多币种钱包(一玩家一币种一行;无行 = 该币种余额 0)';

-- 存量金币搬进钱包(kind=1)。只搬 gold > 0 的行:0 余额与"没有行"语义等价。
-- GREATEST(gold, 0) 兜底理论上不该存在的负数(旧列是有符号 BIGINT),
-- 防止负值写进 UNSIGNED 列时在非严格 sql_mode 下被静默截断。
-- INSERT IGNORE:重跑时已搬过的行原样保留,**不覆盖**——覆盖会把迁移后产生的新余额抹回旧值。
SET @pandora_has_legacy_currency := (
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
      AND table_name = 'player_currency'
);
SET @pandora_sql := IF(
    @pandora_has_legacy_currency = 1,
    'INSERT IGNORE INTO `player_wallet` (`player_id`, `currency_kind`, `amount`) SELECT `player_id`, 1, GREATEST(`gold`, 0) FROM `player_currency` WHERE `gold` > 0',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;

-- ---------------------------------------------------------------------------
-- ② inventory_ledger.result_currencies:多币种幂等结果快照
-- ---------------------------------------------------------------------------
SET @pandora_col_missing := (
    SELECT COUNT(*) = 0
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'inventory_ledger'
      AND column_name = 'result_currencies'
);
SET @pandora_sql := IF(
    @pandora_col_missing = 1,
    'ALTER TABLE `inventory_ledger` ADD COLUMN `result_currencies` VARBINARY(256) NULL COMMENT ''首次执行后多币种余额快照(pb CurrencyBalancesStorageRecord);NULL = 老行,读时回退 result_gold'' AFTER `result_gold`',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;

-- 本次货币变动额(收入或支出的绝对值,方向由 op 决定)。
--
-- 为什么余额快照之外还要记变动额:出售 / 购买的响应要回"本次获得 X / 花费 Y",
-- 而**幂等重放必须返回与首次执行相同的值**(§9.7)。只存余额快照的话,
-- 重放时算不出当初那一笔是多少(中间可能已有别的收支),只能去 parse detail 文本 ——
-- detail 是人读审计字段,不该承担业务语义。
-- 记账本来就该记"变动 + 结果",这一列把 ledger 补成真正的流水。
SET @pandora_col_missing := (
    SELECT COUNT(*) = 0
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'inventory_ledger'
      AND column_name = 'result_currency_delta'
);
SET @pandora_sql := IF(
    @pandora_col_missing = 1,
    'ALTER TABLE `inventory_ledger` ADD COLUMN `result_currency_delta` VARBINARY(256) NULL COMMENT ''本次操作的货币变动额绝对值(pb CurrencyBalancesStorageRecord;方向由 op 决定:sell/grant=收入,purchase=支出);NULL = 老行或纯道具操作'' AFTER `result_currencies`',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;

-- ---------------------------------------------------------------------------
-- ③ auction_escrow:托管金额更名 + 币种列
-- ---------------------------------------------------------------------------
-- 纯改名(类型不变)在 TiDB 的一条 ALTER 子句里是支持的;
-- 类型保持有符号 BIGINT:应用层 2^62 上限已保证不越界,改 UNSIGNED 属无收益的高风险 DDL。
SET @pandora_needs_rename := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'auction_escrow'
      AND column_name = 'frozen_gold'
);
SET @pandora_sql := IF(
    @pandora_needs_rename = 1,
    'ALTER TABLE `auction_escrow` CHANGE COLUMN `frozen_gold` `frozen_amount` BIGINT NOT NULL DEFAULT 0 COMMENT ''kind=2 剩余冻结货币量(币种见 currency_kind;成交消费 / 退还递减)''',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;

-- currency_kind 默认 1(金币):存量买单托管全部是金币,默认值即正确回填。
SET @pandora_col_missing := (
    SELECT COUNT(*) = 0
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'auction_escrow'
      AND column_name = 'currency_kind'
);
SET @pandora_sql := IF(
    @pandora_col_missing = 1,
    'ALTER TABLE `auction_escrow` ADD COLUMN `currency_kind` INT NOT NULL DEFAULT 1 COMMENT ''kind=2 时冻结的货币类型(CurrencyKind;存量买单均为 1=金币)'' AFTER `frozen_amount`',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;
