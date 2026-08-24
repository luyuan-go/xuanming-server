-- 战后金币收益入钱包(2026-08-22)。
--
-- # 修的是什么
--
-- `battle_player_stats.gold` 一直只是**战绩展示字段**:全仓没有任何一处把它送进 inventory,
-- 玩家结算界面看到的"获得 N 金币"从来没有真的进过钱包。
--
-- 修法是让金币搭既有的战后发放出箱(`battle_drop_outbox`),而不是另起一条"金币发放链":
-- 出箱表已经有幂等键、失败重试、投递成功才删行、容量预算与保留期登记,
-- 复用它等于免费拿到全套正确性保证(§15.2 最少复杂度优先)。
--
-- 因此本迁移只加一列:
--   `currency_amount` —— 首次入箱时**已经过服务端上限闸**的金币数(DS 不可信,§9.6)。
--
-- # 为什么冻结在出箱行里而不是重试时现算
--
-- 与同表的 `stack_item_config_ids` / `instance_item_config_ids` 同一纪律:
-- 出箱行装的是**已裁决的事实**,不是待裁决的输入。重试时若回头再读 DS 上报值或热配置上限,
-- 上限刚好被改动就会导致"第一次发了 100 万、重试发了 50 万"这类不可解释的双额。
--
-- # 列类型
--
-- BIGINT UNSIGNED:金币语义非负(§5.12)。上限由应用层 `MaxBattleGoldPerPlayer`(默认 100 万)
-- 保证,远小于列容量;真触顶只可能是 DS 异常,那种情况在入箱前就被截断并 Warn 了。
--
-- 幂等:先查 information_schema 再决定是否执行,可重复跑。
-- 每条 ALTER 只带一个子句(TiDB multi-schema change 逐子句比对原表结构,合并写法会假冲突)。

SET @pandora_col_missing := (
    SELECT COUNT(*) = 0
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'battle_drop_outbox'
      AND column_name = 'currency_amount'
);
SET @pandora_sql := IF(
    @pandora_col_missing = 1,
    'ALTER TABLE `battle_drop_outbox` ADD COLUMN `currency_amount` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT ''本局该玩家金币收益(首次入箱时已过服务端上限闸冻结;0=无收益);发放走 inventory.GrantItems 与堆叠道具同一幂等键'' AFTER `instance_item_config_ids`',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;

-- 存量出箱行(升级前写入、尚未投递的)保持 currency_amount=0:
-- 它们对应的对局在旧版本下本来就不发金币,补发会让同一场战斗的收益口径前后不一致。
-- 需要补发请走运营邮件,不要改这里。
