-- Pandora 战斗结算 player.update 事务出箱表 W4 ⑨(2026-06-06)
--
-- 装载方式:容器 entrypoint 自动扫 /docker-entrypoint-initdb.d/*.sql 顺序执行
-- (01 建库,03 建结算表,本文件接着建出箱表)。
--
-- 背景(不变量 §4 + HANDOFF §3 Step 2「可靠补偿收口」):
--   W4 ③ battle_result 落库后直接发 pandora.player.update(best-effort 弱依赖),
--   Kafka 不可用时事件直接丢 → 玩家段位永不更新,补偿不可靠。
--   W4 ⑨ 引入「事务出箱」(transactional outbox):落 battles + battle_player_stats
--   的同一事务里再写一行 player.update 出箱记录,二者原子提交;后台发布器轮询出箱
--   表逐条投递 Kafka,投递成功才删行。配合 player 服务幂等消费(W4 ④ mmr_history
--   uk),整条段位写链是 at-least-once 可靠闭环,可穿越 Kafka 临时不可用。
--
-- 约定:
--   - match_id / player_id 是 snowflake uint64(BIGINT UNSIGNED,不变量 §11)
--   - payload 是 player.v1.PlayerUpdateEvent 的 proto 序列化字节
--   - uk_match_player:同一对局同一玩家只入一行(防重复入箱;落库本身按 match_id 幂等,
--     正常路径不会重入,uk 是防御性兜底)
--   - 投递成功即 DELETE 该行,出箱表只保留待发布事件,不会无限增长

USE `pandora_battle`;

CREATE TABLE IF NOT EXISTS `player_update_outbox` (
    `id`            BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    `match_id`      BIGINT UNSIGNED  NOT NULL,
    `player_id`     BIGINT UNSIGNED  NOT NULL,
    `payload`       VARBINARY(512)   NOT NULL COMMENT 'player.v1.PlayerUpdateEvent proto bytes',
    `created_at_ms` BIGINT           NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_match_player` (`match_id`, `player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora player.update 事务出箱(at-least-once 可靠补偿,不变量 §4)';

-- ── 战斗道具掉落事务出箱 W5 ④(2026-07-08)────────────────────────────────────
--
-- 背景:DS 上报的战斗道具掉落(BattleResult.PlayerStats.dropped_item_config_ids)
--   必须可靠、幂等地落进 inventory(可堆叠物/装备实例,不可丢也不可重复)。沿用 player.update
--   同款「事务出箱」:落 battles + battle_player_stats 的同一事务里,对每个有掉落的玩家
--   再写一行 drop 出箱记录(原子提交);后台 RunDropPublisher 轮询本表,逐行调
--   inventory.GrantItems / GrantInstances(各自稳定幂等子键),成功才删行。
--
-- 与 player.update 出箱的差异:
--   - 掉落无跨玩家保序需求 → 发布器按行独立重试(单行失败不阻塞其他玩家)。
--   - 首次入箱按真实 item.type 分流并把两条路由 CSV 固化；发布重试不再读热配置。
--   - DS 不可信:写入本表前 battle_result 已按同一份 configtable drop×item 过滤,
--     不在真实掉落表或不存在于 item 表的 ID 不入表;
--     且每玩家按 max_drop_per_player 截断(默认 32,硬上限 46 = VARCHAR(512) 可容纳的
--     最大条数:46 个 10 位 uint32 + 45 逗号 = 505 字符),防超长 CSV 打失败整场结算。
--
-- 约定:
--   - uk_match_player:同对局同玩家只入一行(落库按 match_id 幂等,uk 为防御性兜底)。
--   - 投递成功即 DELETE,本表只保留待发放掉落,不会无限增长(容量满除外,见服务文档)。

CREATE TABLE IF NOT EXISTS `battle_drop_outbox` (
    `id`              BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    `match_id`        BIGINT UNSIGNED  NOT NULL,
    `player_id`       BIGINT UNSIGNED  NOT NULL,
    `item_config_ids` VARCHAR(512)     NOT NULL COMMENT 'CSV of dropped item_config_id, e.g. 5001,5002',
    `stack_item_config_ids`    VARCHAR(512) NOT NULL DEFAULT '' COMMENT '首次入箱时冻结的可堆叠路由;发布重试不得按热配置重算',
    `instance_item_config_ids` VARCHAR(512) NOT NULL DEFAULT '' COMMENT '首次入箱时冻结的装备实例路由;发布重试不得按热配置重算',
    `currency_amount`          BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '本局该玩家金币收益(首次入箱时已过服务端上限闸冻结;0=无收益);与堆叠道具共用同一次 GrantItems 与同一幂等键',
    `created_at_ms`   BIGINT           NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_match_player` (`match_id`, `player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 战斗道具掉落事务出箱(at-least-once 幂等发放 stack/instance,W5 ④)';

-- ── Battle 结算终态回收事务出箱(Model-B,2026-07-13)──────────────────────────
--
-- 正常结算请求通过 callback Guard + Redis active 校验后，battle_result 把服务端
-- authorized_at_ms 与完整凭据身份、稳定 GameServer 身份写入本表；本行与 battles /
-- battle_player_stats 同事务提交。后台 relay 在宽限窗后依次执行：Redis 终态 CAS +
-- receipt → Kubernetes UID precondition 回收 → MySQL CAS released_at_ms；第二阶段再给
-- Redis 永久墓碑设有限 TTL并 DELETE 本行。外部回收成功但 DB ACK 长期失败时，Redis
-- 绝不先过期，避免 outbox 尚在却永远失去 proof 的跨存储崩溃窗。
--
-- auth_gen/jti 记录“当时通过校验的凭据证明”，不是回收时必须仍为 current 的代际；
-- 回收只允许 stable identity(match/allocation/pod/uid/epoch/writer fence)仍一致。
CREATE TABLE IF NOT EXISTS `terminal_release_outbox` (
    `id`                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `match_id`          BIGINT UNSIGNED NOT NULL,
    `allocation_id`     CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    `ds_pod_name`       VARCHAR(253) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    `gameserver_uid`    VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    `instance_epoch`    INT UNSIGNED NOT NULL,
    `auth_gen`          BIGINT UNSIGNED NOT NULL,
    `auth_jti`          VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    `auth_exp_ms`       BIGINT NOT NULL,
    `auth_kid`          VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    `auth_token_sha256` CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    `auth_writer_epoch` INT UNSIGNED NOT NULL,
    `authorized_at_ms`  BIGINT NOT NULL,
    `release_after_ms`  BIGINT NOT NULL,
    `released_at_ms`    BIGINT NOT NULL DEFAULT 0,
    `created_at_ms`     BIGINT NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_terminal_release_match` (`match_id`),
    KEY `idx_terminal_release_due` (`release_after_ms`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Battle Model-B 正常结算终态回收事务出箱';

-- ── battle_result→matchmaker 撮合状态释放事务出箱(2026-07-15)──────────────
CREATE TABLE IF NOT EXISTS `match_release_outbox` (
    `id`                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `match_id`           BIGINT UNSIGNED NOT NULL,
    `payload`            VARBINARY(1024) NOT NULL COMMENT 'match.v1.MatchReleaseStorageRecord proto bytes',
    `next_attempt_at_ms` BIGINT NOT NULL DEFAULT 0,
    `attempt_count`      INT UNSIGNED NOT NULL DEFAULT 0,
    `created_at_ms`      BIGINT NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_match_release_match` (`match_id`),
    KEY `idx_match_release_due` (`next_attempt_at_ms`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Battle 结算后可靠释放 match/ticket/player claim';

-- ── 战斗中实时进度通道(实时成长,2026-07-20 realtime-progression.md)──────────
--
--   battle_progress_stream 每场进度水位:last_applied_seq 只增(乐观 CAS 推进,与出箱同事务);
--     settled_at_ms>0 = 对局已结算(正常 / ABANDONED 都打标记),此后 ReportProgress 一律拒
--     (僵尸 / 分区恢复 DS fencing);结算时水位>0 → 结算路径掉落只对账不发放(单一权威路径)。
--     已结算行保留 90 天后由 battle_result 保留期清理回收(§9.24;final_seq 在保留期内
--     留存对账证据);未结算行永不自动清理(陈年未结算 = 补偿链 bug 证据,每轮告警)。
--   battle_progress_outbox 进度发放事务出箱:exp → player.AddExperience;
--     装备/堆叠拾取 → GrantInstances/GrantItems;局内消费/丢弃 → ConsumeBattleItem/
--     DiscardBattleItem。幂等键 progress:{match_id}:{seq}:{player_id}:{kind};
--     发放成功即 DELETE,只保留待发放行。

CREATE TABLE IF NOT EXISTS `battle_progress_stream` (
    `match_id`         BIGINT UNSIGNED NOT NULL,
    `last_applied_seq` BIGINT UNSIGNED NOT NULL DEFAULT 0,
    `total_exp`        BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '本场累计入账经验(单场上限封顶依据)',
    `total_items`      INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT '本场累计入账掉落件数(单场上限封顶依据)',
    `final_seq`        BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'DS 结算上报的对账水位(0=未走实时通道)',
    `settled_at_ms`    BIGINT          NOT NULL DEFAULT 0 COMMENT '>0 = 对局已结算,进度一律拒',
    `stopped_at_ms`    BIGINT          NOT NULL DEFAULT 0 COMMENT '>0 = 实时通道已停流(未知事实/违纪混版),后续进度一律拒',
    `updated_at_ms`    BIGINT          NOT NULL DEFAULT 0,
    PRIMARY KEY (`match_id`),
    KEY `idx_settled` (`settled_at_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 战斗实时进度水位(去重 + 结算 fencing + 单场累计上限,实时成长;已结算行保留 90 天由 battle_result 保留期清理回收,§9.24)';
-- idx_settled 服务保留期清理(SELECT ... WHERE settled_at_ms>0 AND settled_at_ms<cutoff)。
-- 存量库由 tools/migrate pandora_battle 000007_battle_retention_indexes 条件补齐。

CREATE TABLE IF NOT EXISTS `battle_progress_outbox` (
    `id`                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `match_id`           BIGINT UNSIGNED NOT NULL,
    `seq`                BIGINT UNSIGNED NOT NULL COMMENT 'exp 行=批末 seq;item 行=拾取事实自身 seq(幂等键组成)',
    `player_id`          BIGINT UNSIGNED NOT NULL,
    `kind`               TINYINT UNSIGNED NOT NULL COMMENT '1=exp 2=grant_instance 3=grant_stack 4=consume_stack 5=discard_stack',
    `exp_delta`          BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'kind=1:本行经验(批内聚合)',
    `item_config_ids`    VARCHAR(512)    NOT NULL DEFAULT '' COMMENT 'kind=2/3:CSV;kind=4/5:单个 item_config_id',
    `item_count`         INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT 'kind=4/5:紧凑 action count;0=兼容旧重复CSV',
    `next_attempt_at_ms` BIGINT          NOT NULL DEFAULT 0 COMMENT '发放失败指数退避后的下次尝试时点(防队首阻塞)',
    `attempt_count`      INT UNSIGNED    NOT NULL DEFAULT 0,
    `created_at_ms`      BIGINT          NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_match_seq_player_kind` (`match_id`, `seq`, `player_id`, `kind`),
    KEY `idx_progress_due` (`next_attempt_at_ms`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 战斗实时进度发放事务出箱(at-least-once + 下游幂等 + 失败退避,实时成长)';

-- phase0 战斗堆叠道具支出额度。只把本场已接受的同玩家/同 item stack pickup 计入
-- picked_count；consume/discard 在 ApplyProgress 同一事务条件累加 spent_count，失陷 DS
-- 不能用合法 match 凭证删除玩家进入本场前已有的主库存。
CREATE TABLE IF NOT EXISTS `battle_progress_item_balance` (
    `match_id`       BIGINT UNSIGNED NOT NULL,
    `player_id`      BIGINT UNSIGNED NOT NULL,
    `item_config_id` INT UNSIGNED    NOT NULL,
    `picked_count`   BIGINT UNSIGNED NOT NULL DEFAULT 0,
    `spent_count`    BIGINT UNSIGNED NOT NULL DEFAULT 0,
    `updated_at_ms`  BIGINT          NOT NULL DEFAULT 0,
    PRIMARY KEY (`match_id`, `player_id`, `item_config_id`),
    CONSTRAINT `chk_battle_progress_item_balance` CHECK (`spent_count` <= `picked_count`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora phase0 每场玩家同配置 stack pickup/支出权威余额';

-- consume/discard 的 durable completion outcome。status:0=pending 1=succeeded 2=failed。
-- ReportProgress 响应丢失后按本表稳定回放；成功/终态失败与对应 outbox 删除同事务，
-- 绝不从“outbox 行不存在”猜测结果。
CREATE TABLE IF NOT EXISTS `battle_progress_action` (
    `match_id`       BIGINT UNSIGNED NOT NULL,
    `seq`            BIGINT UNSIGNED NOT NULL,
    `player_id`      BIGINT UNSIGNED NOT NULL,
    `kind`           TINYINT UNSIGNED NOT NULL COMMENT '4=consume_stack 5=discard_stack',
    `item_config_id` INT UNSIGNED    NOT NULL,
    `count`          INT UNSIGNED    NOT NULL,
    `status`         TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '0=pending 1=succeeded 2=failed',
    `result_code`    INT             NOT NULL DEFAULT 0 COMMENT 'failed 时稳定回放的 pandora ErrCode',
    `created_at_ms`  BIGINT          NOT NULL DEFAULT 0,
    `updated_at_ms`  BIGINT          NOT NULL DEFAULT 0,
    PRIMARY KEY (`match_id`, `seq`, `player_id`, `kind`),
    KEY `idx_progress_action_match_player` (`match_id`, `player_id`, `seq`),
    CONSTRAINT `chk_battle_progress_action_status` CHECK (`status` IN (0,1,2))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 战斗消费/丢弃同步完成结果(响应丢失可重放)';

-- 每玩家 Battle→Hub 终态证明。初始行与 battles/stats 同事务提交；worker
-- 从 player_locator 读取精确 stable BATTLE version 后把 UUIDv4/HMAC proof CAS
-- 写回 payload，再无 TTL 地 relay 到 Redis。成功才删，未知结果重试同一 payload。
CREATE TABLE IF NOT EXISTS `battle_exit_proof_outbox` (
    `id`                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `match_id`           BIGINT UNSIGNED NOT NULL,
    `player_id`          BIGINT UNSIGNED NOT NULL,
    `payload`            VARBINARY(2048) NOT NULL COMMENT 'battle_result internal immutable proof JSON',
    `prepared`           TINYINT UNSIGNED NOT NULL DEFAULT 0,
    `next_attempt_at_ms` BIGINT NOT NULL DEFAULT 0,
    `attempt_count`      INT UNSIGNED NOT NULL DEFAULT 0,
    `superseded_at_ms`   BIGINT NOT NULL DEFAULT 0,
    `created_at_ms`      BIGINT NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_battle_exit_match_player` (`match_id`, `player_id`),
    KEY `idx_battle_exit_due` (`superseded_at_ms`, `next_attempt_at_ms`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Battle terminal/leave placement proof durable relay';

CREATE TABLE IF NOT EXISTS `battle_progress_player` (
    `match_id`      BIGINT UNSIGNED NOT NULL,
    `player_id`     BIGINT UNSIGNED NOT NULL,
    `total_exp`     BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '本场该玩家累计入账经验(单玩家上限封顶依据)',
    `total_items`   INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT '本场该玩家累计入账掉落件数',
    `total_kills`   INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT '本场该玩家累计击杀数(反作弊上限依据)',
    `updated_at_ms` BIGINT          NOT NULL DEFAULT 0,
    PRIMARY KEY (`match_id`, `player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 战斗实时进度单玩家累计(单场单玩家 经验/掉落/击杀 上限,实时成长)';

-- 任务事实转发出箱(docs/design/mission.md §5.1;迁移同构 pandora_battle/000010)。
-- 独立于 battle_progress_outbox 的理由是**故障域隔离**:任务行混入进度出箱会让 mission
-- 不可用卡住队首、连带阻塞该玩家的经验/掉落投递。两张表各自都是每玩家严格 FIFO ——
-- 任务链前后两环的条件类别通常不同,后环在前环完成时才被自动接取,提前到达的后环事实
-- 匹配不上任何活跃任务会被收据吸收后静默丢弃且永不重放(详见迁移 000010 文件头)。
-- pending_action=1 是 USE_ITEM 的「扣除未落定」闸:占队首但不投递,由局内消费结果
-- 同事务置 0(扣成功)或删行(扣失败),防「上报没有的消耗刷使用类任务」。
CREATE TABLE IF NOT EXISTS `battle_mission_outbox` (
    `id`                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `match_id`           BIGINT UNSIGNED NOT NULL,
    `seq`                BIGINT UNSIGNED NOT NULL COMMENT '产生本事实的事件 seq(幂等键组成 + 每玩家 FIFO 排序键)',
    `player_id`          BIGINT UNSIGNED NOT NULL,
    `category`           INT UNSIGNED    NOT NULL COMMENT 'MissionConditionCategory:1 杀怪 / 4 使用道具 / 9 拾取',
    `slot_value`         INT UNSIGNED    NOT NULL COMMENT '槽位1值(怪物配置 ID / 道具配置 ID)',
    `amount`             INT UNSIGNED    NOT NULL COMMENT '进度增量(击杀数 / 拾取数 / 使用数)',
    `pending_action`     TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '1=等局内消费扣除落定(USE_ITEM);占队首但不投递',
    `next_attempt_at_ms` BIGINT          NOT NULL DEFAULT 0 COMMENT '投递失败指数退避后的下次尝试时点',
    `attempt_count`      INT UNSIGNED    NOT NULL DEFAULT 0,
    `created_at_ms`      BIGINT          NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_match_seq_player` (`match_id`, `seq`, `player_id`),
    KEY `idx_mission_player_fifo` (`player_id`, `id`),
    KEY `idx_mission_due` (`next_attempt_at_ms`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 战斗任务事实转发出箱(每玩家 FIFO + at-least-once + mission 侧收据幂等 + 失败退避)';
