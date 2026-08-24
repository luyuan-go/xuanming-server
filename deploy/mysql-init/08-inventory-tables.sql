-- Pandora 背包 / 经济库 W5 ③ 表结构(2026-06-18)
--
-- 装载方式:容器 entrypoint 自动扫 /docker-entrypoint-initdb.d/*.sql 顺序执行
-- (01-create-databases.sql 先建库 + grant,本文件接着建表)。
--
-- 表清单(对齐 docs/design/infra.md pandora_trade):
--   player_wallet     玩家多币种钱包(PK player_id+currency_kind)
--   player_items      背包道具堆叠(uk player_id+item_config_id)
--   inventory_ledger  发放 / 使用 / 出售幂等流水(uk player_id+idempotency_key,不变量 §9.7)
--
-- 约定:
--   - player_id 由 login 用 snowflake 生成(BIGINT UNSIGNED),inventory 不生成
--   - item_config_id 是配置表道具 ID(uint32,§12)
--   - 货币余额用 BIGINT UNSIGNED(§5.12 非负金额无符号);道具数量仍用 BIGINT
--   - GrantItems / UseItem / SellItem 幂等:inventory_ledger uk 命中即视为已处理(不变量 §9.7)
--   - 背包是大厅态持久化;战斗内即时道具走 UE GAS,不落本库(ds-arch §0.1)
--
-- ⚠️ 本文件是**新库首装**路径,tools/migrate/migrations/pandora_trade/ 是**存量库演进**路径,
--    两者必须同改、殊途同归到同一终态;只改一边会让新装库与升级库结构漂移。

USE `pandora_trade`;

-- 多币种钱包(2026-08-22 由单列 gold 的 player_currency 演进而来)。
--
-- 一玩家一币种一行;**没有行 = 该币种余额 0**,不预建行。
-- "加一种货币" = currency_kind 多写一个枚举值,不加列、不加表、不改接口。
--
-- amount 用 BIGINT UNSIGNED(§5.12);应用层另有 MaxCurrencyAmount=2^62 上限,
-- 加钱越界返回 ERR_INVENTORY_CURRENCY_OVERFLOW 而不是让列回绕。
-- 扣钱一律"FOR UPDATE 锁行 → Go 里比较 → 写绝对值",SQL 里不出现 `amount - ?`:
-- UNSIGNED 列上的负结果在非严格 sql_mode 会被**静默截断成 0**(= 余额清零),必须从写法上排除。
--
-- 存量库另有 legacy `player_currency`(单列 gold,只读待 contract 删除),
-- 由 000005_multi_currency_wallet 迁移搬数据;新装库不再创建该表。
CREATE TABLE IF NOT EXISTS `player_wallet` (
    `player_id`     BIGINT UNSIGNED  NOT NULL COMMENT '玩家 ID(snowflake,§11)',
    `currency_kind` INT              NOT NULL COMMENT '货币类型(pandora.common.v1.CurrencyKind:1=金币 2=钻石 3=荣誉;0/未知值应用层拒)',
    `amount`        BIGINT UNSIGNED  NOT NULL DEFAULT 0 COMMENT '余额(>=0 由列类型保证;上限 2^62 由应用层保证)',
    `updated_at`    DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`player_id`, `currency_kind`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家多币种钱包(一玩家一币种一行;无行 = 该币种余额 0)';

CREATE TABLE IF NOT EXISTS `player_items` (
    `id`             BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    `player_id`      BIGINT UNSIGNED  NOT NULL,
    `item_config_id` INT UNSIGNED     NOT NULL COMMENT '配置表道具 ID(uint32)',
    `count`          BIGINT           NOT NULL DEFAULT 0 COMMENT '持有数量(>=0;0 行可保留也可清理)',
    `updated_at`     DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_item` (`player_id`, `item_config_id`),
    KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家背包道具堆叠';

CREATE TABLE IF NOT EXISTS `inventory_ledger` (
    `id`                  BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    `player_id`           BIGINT UNSIGNED  NOT NULL,
    `idempotency_key`     VARCHAR(64)      NOT NULL COMMENT '防重复入账/扣减(如 drop:<match_id> / use:<uuid>)',
    `op`                  VARCHAR(16)      NOT NULL COMMENT 'grant | use | sell | auction_sell | auction_buy | trade_sell | trade_buy',
    `request_fingerprint` CHAR(64)         NOT NULL DEFAULT '' COMMENT '请求指纹 sha256(op+item+count+currencies);同 key 不同指纹判冲突',
    `result_remaining`    BIGINT           NOT NULL DEFAULT 0 COMMENT '首次执行后剩余数量快照(use/sell 用,回放返回)',
    `result_gold`         BIGINT           NOT NULL DEFAULT 0 COMMENT 'legacy 单币种金币快照:仅供读取 000005 之前写下的存量行,新行一律写 result_currencies',
    `result_currencies`   VARBINARY(256)   NULL COMMENT '首次执行后多币种余额快照(pb CurrencyBalancesStorageRecord);NULL = 老行,读时回退 result_gold',
    `result_currency_delta` VARBINARY(256) NULL COMMENT '本次操作的货币变动额绝对值(pb;方向由 op 决定:sell/grant=收入,purchase=支出);幂等重放靠它回放"本次获得/花费"',
    `detail`              VARCHAR(255)     NOT NULL DEFAULT '' COMMENT '人读摘要(审计用,非业务字段)',
    `created_at`          DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_idem` (`player_id`, `idempotency_key`),
    KEY `idx_player_created` (`player_id`, `created_at`),
    KEY `idx_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 背包发放/使用/出售幂等流水(不变量 §9.7;指纹防 key 复用,快照可回放;保留期 90 天由 inventory sweep 清理,§9.24)';
-- idx_created 服务保留期清理(DELETE WHERE created_at < cutoff LIMIT n,biz/sweep.go)。
-- 既有库(已建 volume 不重放 init SQL)需手动补:
--   ALTER TABLE inventory_ledger ADD KEY idx_created (created_at);

CREATE TABLE IF NOT EXISTS `auction_escrow` (
    `id`             BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    `player_id`      BIGINT UNSIGNED  NOT NULL COMMENT '挂单玩家(冻结资产的所有者)',
    `order_id`       BIGINT UNSIGNED  NOT NULL COMMENT '挂单 order_id(escrow 键 + 冻结幂等键)',
    `kind`           TINYINT          NOT NULL COMMENT '1=item(卖单冻道具) 2=currency(买单冻货币)',
    `item_config_id` INT UNSIGNED     NOT NULL DEFAULT 0 COMMENT 'kind=1 时冻结的道具配置 ID',
    `frozen_qty`     BIGINT           NOT NULL DEFAULT 0 COMMENT 'kind=1 剩余冻结道具数(成交消费 / 退还递减)',
    `frozen_amount`  BIGINT           NOT NULL DEFAULT 0 COMMENT 'kind=2 剩余冻结货币量(币种见 currency_kind;成交消费 / 退还递减)',
    `currency_kind`  INT              NOT NULL DEFAULT 1 COMMENT 'kind=2 时冻结的货币类型(CurrencyKind;存量买单均为 1=金币)',
    `status`         TINYINT          NOT NULL DEFAULT 1 COMMENT '1=active 2=closed(退还/完结)',
    `created_at`     DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`     DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_order` (`player_id`, `order_id`),
    KEY `idx_player` (`player_id`),
    KEY `idx_status_updated` (`status`, `updated_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 拍卖挂单托管(escrow:挂单冻结/成交消费/撤单过期退还;closed 行保留期 90 天由 inventory sweep 清理,active 永不清,§9.24)';
-- idx_status_updated 服务保留期清理(DELETE WHERE status=closed AND updated_at < cutoff LIMIT n)。
-- 既有库需手动补:
--   ALTER TABLE auction_escrow ADD KEY idx_status_updated (status, updated_at);

-- W5 ④ 装备实例背包(2026-07-08)
--
--   player_item_instance  装备类道具唯一实例(每件独立 + 鉴定后随机属性),不可堆叠。
--   与 player_items(可堆叠消耗品计数)并存:消耗品走计数,装备走实例(ds-arch §0.5 大厅态持久化)。
--   instance_id 由 inventory 服务 snowflake 生成(BIGINT UNSIGNED,§11)。
--   attributes 存鉴定后 roll 的随机属性(pb ItemInstanceAttributesStorageRecord 二进制);未鉴定为 NULL。
--   非基础类型一律 proto 二进制,不用 JSON(字段编号语义 + unknown fields 保留,§5.8/§9.17)。
--   幂等发放沿用 inventory_ledger(op=grant_inst,记 instance_id 列表指纹);
--   鉴定天然幂等(identified=1 后不再 roll,回放已落定属性)。
--   slot_index 未分配格 = NULL(MySQL 唯一键允许多个 NULL,故多件未分配格不冲突);
--   分配到具体格后为 [0,capacity),(player_id,slot_index) 唯一,防两件叠同格。
CREATE TABLE IF NOT EXISTS `player_item_instance` (
    `instance_id`    BIGINT UNSIGNED  NOT NULL COMMENT '装备实例唯一 ID(snowflake,§11)',
    `player_id`      BIGINT UNSIGNED  NOT NULL COMMENT '持有玩家',
    `item_config_id` INT UNSIGNED     NOT NULL COMMENT '配置表道具 ID(uint32,§12)',
    `identified`     TINYINT          NOT NULL DEFAULT 0 COMMENT '0=未鉴定 1=已鉴定(鉴定后 attributes 落定)',
    `attributes`     VARBINARY(1024)  NULL COMMENT '鉴定后随机属性(pb ItemInstanceAttributesStorageRecord);未鉴定为 NULL',
    `slot_index`     INT              NULL     DEFAULT NULL COMMENT '背包格子索引([0,capacity);NULL=未分配格)',
    `bound`          TINYINT          NOT NULL DEFAULT 0 COMMENT '0=未绑定 1=绑定(绑定后不可交易/出售)',
    `created_at`     DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`     DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`instance_id`),
    KEY `idx_player` (`player_id`),
    UNIQUE KEY `uk_player_slot` (`player_id`, `slot_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家装备类道具唯一实例(实例化背包;每件独立 + 鉴定后随机属性)';

-- 邮件 transfer 附件实例托管(2026-07-22,bag-domain.md §7.1 三不变量落地)
--
--   既存实例"只改归属"的在途托管行:EscrowOutInstances 从 player_item_instance 同事务
--   DELETE + INSERT 本表(托管期间实例不存在于任何玩家背包);ClaimTransferInstances /
--   ReleaseTransferEscrow 同事务 DELETE 本表 + INSERT 回实例表。instance_id 双表各自 PK +
--   事务性搬移 = "同一 instance 全局唯一"不变量。
--   增长有界(§9.24 豁免登记):行只存在于托管在途期,领取/释放即删;过期未领个人邮件
--   由 mail sweep 归档(player_mail_archive)留补偿凭据,运营凭归档重发/释放对应托管行。
CREATE TABLE IF NOT EXISTS `mail_transfer_escrow` (
    `instance_id`      BIGINT UNSIGNED  NOT NULL COMMENT '托管中的实例唯一 ID(与 player_item_instance 互斥存在)',
    `item_config_id`   INT UNSIGNED     NOT NULL COMMENT '配置表道具 ID(uint32,§12;领取侧交叉核对)',
    `identified`       TINYINT          NOT NULL DEFAULT 0 COMMENT '扣出时鉴定态原样保留',
    `attributes`       VARBINARY(1024)  NULL COMMENT '扣出时词条原样保留(pb ItemInstanceAttributesStorageRecord);未鉴定为 NULL',
    `bound`            TINYINT          NOT NULL DEFAULT 0 COMMENT '扣出时绑定态原样保留(扣出侧已拒绑定实例,防御性冗余)',
    `source_player_id` BIGINT UNSIGNED  NOT NULL COMMENT '扣出来源玩家(释放归还目标;审计)',
    `to_player_id`     BIGINT UNSIGNED  NOT NULL COMMENT '预期领取人(领取校验,防其它邮件冒领)',
    `escrow_key`       VARCHAR(64)      NOT NULL COMMENT '托管操作键(审计;幂等由 inventory_ledger 承担)',
    `created_at`       DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`instance_id`),
    KEY `idx_to_player` (`to_player_id`),
    KEY `idx_source_player` (`source_player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 邮件 transfer 附件实例托管(在途行,领取/释放即删;§9.24 豁免登记)';
