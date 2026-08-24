-- 回滚战后金币入钱包(⚠️ 仅 dev)。
--
-- 删列会丢掉**尚未投递**的出箱行里的金币数:回滚后这些对局的金币永久发不出去
-- (旧代码不认识这一列)。已投递的不受影响(钱已在玩家钱包里)。
--
-- 幂等 + 单子句,理由同 .up.sql。

SET @pandora_col_exists := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'battle_drop_outbox'
      AND column_name = 'currency_amount'
);
SET @pandora_sql := IF(
    @pandora_col_exists = 1,
    'ALTER TABLE `battle_drop_outbox` DROP COLUMN `currency_amount`',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;
