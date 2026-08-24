-- 回滚多币种钱包(⚠️ 仅 dev)。
--
-- **回滚会丢数据,且丢法是不对称的**:
--   - 金币(kind=1)能写回 `player_currency.gold`,可以还原;
--   - 钻石 / 荣誉等其它币种在旧结构里**没有容身之处**,回滚即永久丢失。
-- 因此生产严禁执行;dev 回滚前请自行确认没有非金币余额。
--
-- `result_currencies` 列直接删除:它是 pb 二进制,旧代码读不懂,留着也没用;
-- 旧代码继续读 `result_gold`,存量行的重放结果不变。
--
-- 幂等 + 每条 ALTER 单子句,理由同 .up.sql。

-- ---------------------------------------------------------------------------
-- ③ auction_escrow 还原
-- ---------------------------------------------------------------------------
SET @pandora_col_exists := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'auction_escrow'
      AND column_name = 'currency_kind'
);
SET @pandora_sql := IF(
    @pandora_col_exists = 1,
    'ALTER TABLE `auction_escrow` DROP COLUMN `currency_kind`',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;

SET @pandora_needs_rename := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'auction_escrow'
      AND column_name = 'frozen_amount'
);
SET @pandora_sql := IF(
    @pandora_needs_rename = 1,
    'ALTER TABLE `auction_escrow` CHANGE COLUMN `frozen_amount` `frozen_gold` BIGINT NOT NULL DEFAULT 0 COMMENT ''kind=2 剩余冻结金币(成交消费 / 退还递减)''',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;

-- ---------------------------------------------------------------------------
-- ② inventory_ledger.result_currencies 删除
-- ---------------------------------------------------------------------------
SET @pandora_col_exists := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'inventory_ledger'
      AND column_name = 'result_currency_delta'
);
SET @pandora_sql := IF(
    @pandora_col_exists = 1,
    'ALTER TABLE `inventory_ledger` DROP COLUMN `result_currency_delta`',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;

SET @pandora_col_exists := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'inventory_ledger'
      AND column_name = 'result_currencies'
);
SET @pandora_sql := IF(
    @pandora_col_exists = 1,
    'ALTER TABLE `inventory_ledger` DROP COLUMN `result_currencies`',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;

-- ---------------------------------------------------------------------------
-- ① 金币回写 player_currency,然后删钱包表
-- ---------------------------------------------------------------------------
-- 只回写金币;其它币种无处可去(见顶部说明)。
SET @pandora_has_wallet := (
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
      AND table_name = 'player_wallet'
);
SET @pandora_has_legacy_currency := (
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
      AND table_name = 'player_currency'
);
SET @pandora_sql := IF(
    @pandora_has_wallet = 1 AND @pandora_has_legacy_currency = 1,
    'INSERT INTO `player_currency` (`player_id`, `gold`) SELECT `player_id`, LEAST(`amount`, 9223372036854775807) FROM `player_wallet` WHERE `currency_kind` = 1 ON DUPLICATE KEY UPDATE `gold` = VALUES(`gold`)',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;

DROP TABLE IF EXISTS `player_wallet`;
