"""player 业务逻辑层 —— 对应 Go 侧 internal/biz/{player,experience,reward,skill_card}.go。

职责:玩家档案 / 分池段位 MMR / 英雄池 / 出战养成(加点·装备预设·天赋·技能卡)/
玩家等级经验(实时成长)/ 领奖记录。

关键不变量(每条都是"写错了不报错"的):

  ① **UpdateMMR 幂等**:同一 idempotency_key 只算一次(mmr_history uk 兜底,§2)。
  ② **档案懒创建**:每条写路径前先 ensure_profile,保证后续行存在。
  ③ **fail-closed 而不是"没校验就放行"**:配置表未加载 / inventory 未接线时,
     SetEquipment / SetTalents / 技能卡写路径一律**拒绝**。GetLoadout 会把预设转成
     Battle DS 的初始 GameplayEffect —— 校验链不完整时放行 = 客户端可给自己配任意装备。
  ④ **经验事件走独立 topic**:pandora.player.update 是**单事件类型 topic**(§21),
     旧副本不看 event_type 直接按 PlayerUpdateEvent 解码。往它上面加新 event_type 会
     静默污染 MMR —— 所以经验推送另开 pandora.player.experience。
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as _dt
import time

from pandora.inventory.v1 import inventory_pb2 as inv_pb
from pandora.player.v1 import player_pb2 as ppb

from pandorapy import cellroute
from pandorapy import dbguard
from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import rating as prating
from pandorapy import rewardclaim
from pandorapy import safego
from pandorapy.configtable import ConfigTableError
from pandorapy.services.player import models as m
from pandorapy.services.player.experience import advance_experience  # noqa: F401 —— 供测试从 biz 引用
from pandorapy.services.player.tables import Store

# GetPlayerNames 单次可查的角色数上限(§9.18 读取侧上限)。
#
# 200 的依据:Hub 单实例 500 人,DS 按批拉名字时 3 批以内覆盖满场;再大会让单次 IN 查询
# 与响应体都失控。刻意**不做隐式分页** —— 超出直接截断并让调用方自己分批,服务端悄悄
# 少返回会让调用方以为"这些人就是没有档案"。
MAX_PLAYER_NAMES_PER_QUERY = 200

# 卡槽数量,对应战斗内的 Q/W/E/R 四个技能位。
#
# 刻意做成常量而不是配置项:槽位数同时被客户端 UI 布局、DS 给技能的循环和本校验读,
# 三处必须一致。做成可配会让"改了服务端配置但客户端还是 4 个格子"变成一类可能的事故。
SKILL_SLOT_COUNT = 4

# 领奖写入遇乐观锁冲突的最大重试次数(并发领取同一玩家时兜底)。
MAX_REWARD_CLAIM_RETRY = 3

# 两个 janitor 共用的节拍与批大小。复用同一对常量,避免"两个 janitor 各拍一个数"后
# 再也说不清哪个才是本服口径。
RETENTION_SWEEP_INTERVAL_SEC = 3600.0
RETENTION_SWEEP_BATCH = 1000

# 装备词条信任边界(Player → Battle DS)。
#
# 词条虽来自 inventory 内网权威接口,仍可能受脏库、旧副本或解析漂移影响;player 在组装
# GetLoadout 前只放行 DS **已实现且数值单位明确**的三类属性。Attack/Defense 是平坦整数,
# MoveSpeedRate 是基点(10000 = 100%)。任何未知 ID、非正数或超限值都 fail-closed。
EQUIPMENT_ATTR_ATTACK_ID = 3
EQUIPMENT_ATTR_MOVE_SPEED_RATE_ID = 7
EQUIPMENT_ATTR_DEFENSE_ID = 9
MAX_EQUIPMENT_FLAT_ATTRIBUTE_VALUE = 1_000_000
MAX_EQUIPMENT_RATE_BASIS_POINTS = 10_000


def _equipment_attribute_max_value(attr_id: int) -> int | None:
    if attr_id in (EQUIPMENT_ATTR_ATTACK_ID, EQUIPMENT_ATTR_DEFENSE_ID):
        return MAX_EQUIPMENT_FLAT_ATTRIBUTE_VALUE
    if attr_id == EQUIPMENT_ATTR_MOVE_SPEED_RATE_ID:
        return MAX_EQUIPMENT_RATE_BASIS_POINTS
    return None


def _battle_flags(reason: str) -> tuple[bool, bool]:
    """按 reason 决定是否计对局 / 计胜。

      win            → 计一场 + 计一胜
      lose / draw    → 计一场,不计胜
      abandon        → 对局作废,不计场不计胜(delta 应为 0)
      rollback / 其它 → 纯 MMR 修正,不计场不计胜
    """
    if reason == "win":
        return True, True
    if reason in ("lose", "draw"):
        return True, False
    return False, False


def profile_shard_key(player_id: int) -> str:
    """玩家档案存储的分片键口径(canonical)—— 对应 Go 的 ProfileShardKey。

    = player_id 十进制串(玩家 owner cell 决定者,scale-cellular §4.2 line 142)。
    同一玩家的档案 / 背包 / 段位 / 好友必落同一 owner cell;**不取 nickname / hero_id /
    任何配置 ID**(与落点无关)。
    """
    return str(player_id)


class PlayerUsecase:
    """player 业务逻辑核心。

    可选依赖一律 setter 注入且 **None-safe**,与 Go 的 SetXxx 同风格:
      - store          配置表快照(启动主链必装;None 时写路径 fail-closed)
      - ownership      inventory 实例归属校验器(None → SetEquipment/GetLoadout fail-closed)
      - exp_pusher     经验出箱 kafka producer(None → 出箱积压不丢,producer 可用后重启补发)
      - push_lease     发布器领导权来源(None → 不选举,本副本无条件发布)
    """

    def __init__(self, repo, cfg) -> None:  # noqa: ANN001 —— repo.MySQLPlayerRepo / conf.PlayerConf
        self._repo = repo
        self._cfg = cfg
        self._store: Store | None = None
        self._ownership = None
        self._exp_pusher = None
        self._push_lease = None
        # 分片部署时由 main 经 set_cell_router 注入;单 Cell 时恒 None(不打落点日志)。
        self._router = None
        # 上一轮领导权状态,只由发布器单协程读写(跃迁日志去重用)。
        self._push_lease_held = False

    # ── 装配 ──────────────────────────────────────────────────────────────

    def set_config_tables(self, store: Store | None) -> None:
        """注入启动时已成功加载并通过整批校验的配置表容器。

        Store 热更以整批不可变快照原子切换;单次事务先取一份快照,不会跨版本混算。
        """
        self._store = store

    def set_cell_router(self, router) -> None:  # noqa: ANN001 —— cellroute.Router | None
        """注入确定性 region/cell 路由器(对应 Go 的 `SetCellRouter`)。

        只用于**档案落点观测**(§4.2「同一 player_id 的 owner 数据必落同一 cell」的
        上线核对信号)。不注入 = 单 Cell,写路径一字不改。
        """
        self._router = router

    def _log_profile_placement(self, player_id: int, op: str) -> None:
        """一次档案写之后,把这名玩家的 owner 落点打成观测日志。

        router 未注入 / player_id=0 / 路由失败 → 整条不执行(单 Cell 语义不变)。
        ★ 刻意不 fail:落点观测失败不该让一次已经提交的档案写变成错误应答。
        """
        if self._router is None or player_id == 0:
            return
        try:
            loc = self._router.route(player_id)
        except cellroute.CellRouteError:
            return
        plog.get().debug(
            "profile_placement",
            player_id=player_id,
            op=op,
            region=loc.region_id,
            cell=loc.cell_id,
            shard_key=profile_shard_key(player_id),
        )

    def set_instance_ownership_checker(self, checker) -> None:  # noqa: ANN001
        """注入精确实例归属查询实现。

        传 None 等于**关闭** SetEquipment(fail-closed),不会退化成「不校验就放行」。
        """
        self._ownership = checker

    def set_experience_pusher(self, pusher) -> None:  # noqa: ANN001
        self._exp_pusher = pusher

    def set_push_writer_lease(self, lease) -> None:  # noqa: ANN001
        """注入发布器领导权来源;None = 不选举(单进程 / 单副本 Recreate)。

        只允许在启动装配期调用(run_push_outbox_publisher 之前)。
        """
        self._push_lease = lease

    # ── 内部小工具 ────────────────────────────────────────────────────────

    def _default_nickname(self, player_id: int) -> str:
        """给新玩家生成唯一默认昵称(prefix + player_id,保证 uk_nickname 不冲突)。"""
        return f"{self._cfg.default_nickname_prefix}{player_id}"

    async def _ensure(self, player_id: int) -> bool:
        return await self._repo.ensure_profile(
            player_id, self._default_nickname(player_id), self._cfg.base_mmr
        )

    def _tables(self):  # noqa: ANN202
        return self._store.tables if self._store is not None else None

    # ── 档案 ──────────────────────────────────────────────────────────────

    async def get_profile(self, player_id: int) -> ppb.PlayerProfile:
        """读玩家档案(懒创建:首次访问自动建默认档案)。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        await self._ensure(player_id)
        profile = await self._repo.get_profile(player_id)
        if profile is None:
            raise errcode.PandoraError(
                errcode.ErrPlayerNotFound, "player not found: %d", player_id
            )
        # 经验派生字段装饰:满级 → is_max_level + 级内经验按 0 展示。
        # 曲线未配置(功能关闭)→ 不标满级,行为与历史一致。
        exp_in_level, is_max = self.decorate_experience(profile.level, profile.exp_in_level)
        profile.exp_in_level = exp_in_level
        profile.is_max_level = is_max

        # 分池段位:新客户端读 ratings;deprecated mmr #4 已由 get_profile 从 players.mmr
        # (default 兼容投影)填好,供旧客户端继续显示。ratings 只含该玩家**已有记录**的池
        # —— 没打过的池不占位,客户端据此区分"未定级"与"已定级但分是基线"。
        ratings = await self._repo.list_ratings(player_id)
        if ratings:
            del profile.ratings[:]
            for r in ratings:
                profile.ratings.append(
                    ppb.PlayerRating(rating_pool=r.rating_pool, mmr=r.mmr)
                )
                # 两次仓储读取之间可能恰逢结算提交;同一响应内若已有 default rating,
                # 就以它覆盖 deprecated 字段,避免新旧客户端看到两份瞬时值。
                if r.rating_pool == prating.DEFAULT_POOL:
                    profile.mmr = r.mmr
        return profile

    async def update_nickname(self, player_id: int, nickname: str) -> None:
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        nickname = nickname.strip()
        if not nickname:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "nickname must not be empty")
        # 按**字符数**判长,与 Go 的 len([]rune(...)) 同口径(按字节判会让中文名少一半)。
        if len(nickname) > self._cfg.max_nickname_len:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "nickname too long (max %d)", self._cfg.max_nickname_len
            )
        await self._ensure(player_id)
        await self._repo.update_nickname(player_id, nickname)

    async def ensure_profile_named(self, player_id: int, nickname: str) -> m.EnsureProfileResult:
        """建档并播种昵称(账号 / 角色分离,唯一调用方是 login)。

        ★ **INSERT IGNORE 语义,不是 UPSERT**:档案已存在就原样返回既有值,一个字都不改。
          这条纪律是「角色名 = 账号名」能安全落地的前提 —— login 每次登录都会尽力播种
          (补上之前因 player 不可达而漏掉的),若改成覆盖式,玩家将来自己改的名字会在
          下一次登录被账号名冲掉。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        seed = (nickname or "").strip()
        if not seed:
            seed = self._default_nickname(player_id)
        elif len(seed) > self._cfg.max_nickname_len:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "nickname too long (max %d)", self._cfg.max_nickname_len
            )

        created = await self._repo.ensure_profile(player_id, seed, self._cfg.base_mmr)

        # 无论建没建成都回读一次:
        #   created=True  → 回读拿等级等派生值;
        #   created=False → 可能是「早就有档案」,也可能是**昵称撞了 uk_nickname 被
        #     INSERT IGNORE 静默跳过**。这两种必须靠回读区分,不能默认成前者 —— 后者下
        #     该玩家至此仍然没有档案,谎报成功会让 login 以为播种完成、再也不重试。
        profile = await self._repo.get_profile(player_id)
        if profile is None:
            raise errcode.PandoraError(
                errcode.ErrPlayerNicknameTaken,
                "ensure profile player=%d skipped: nickname %r already taken",
                player_id,
                seed,
            )
        level = profile.level if profile.level > 0 else 0
        return m.EnsureProfileResult(created=created, nickname=profile.nickname, level=level)

    async def get_player_names(self, player_ids: list[int]) -> list[m.PlayerName]:
        """批量反查角色显示名(Hub DS 头顶铭牌用)。

        只返回查到的角色:请求里有、结果里没有 = 该角色无档案,调用方应保留旧值 / 走兜底。
        用空串占位会让 DS 把一个真名字覆盖成空,铭牌反而退化。
        """
        if not player_ids:
            return []
        # 去重 + 去 0 + 截断。去重是因为调用方(DS 进场批)很容易把同一个人带两次,
        # 重复项会白白撑大 IN 列表并挤掉截断额度。
        seen: set[int] = set()
        unique: list[int] = []
        for pid in player_ids:
            if pid == 0 or pid in seen:
                continue
            seen.add(pid)
            unique.append(pid)
            if len(unique) >= MAX_PLAYER_NAMES_PER_QUERY:
                break
        if not unique:
            return []
        found = await self._repo.list_nicknames(unique)
        # 按请求顺序回填,让同一批请求的响应顺序稳定(调用方可直接顺序消费)。
        return [
            m.PlayerName(player_id=pid, nickname=found[pid]) for pid in unique if pid in found
        ]

    # ── 英雄 ──────────────────────────────────────────────────────────────

    async def list_heroes(self, player_id: int) -> list[int]:
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        return await self._repo.list_heroes(player_id)

    async def unlock_hero(self, player_id: int, hero_id: int, source: str) -> None:
        """解锁英雄(幂等:已拥有 → ErrPlayerHeroAlreadyOwn)。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if hero_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "hero_id required")
        await self._ensure(player_id)
        already = await self._repo.unlock_hero(player_id, hero_id, source)
        if already:
            raise errcode.PandoraError(
                errcode.ErrPlayerHeroAlreadyOwn,
                "hero already owned: player=%d hero=%d",
                player_id,
                hero_id,
            )

    # ── MMR ───────────────────────────────────────────────────────────────

    async def get_mmr(self, player_id: int, rating_pool: str) -> tuple[int, bool]:
        """读某池的分。该池无记录 → (base_mmr, False),**不创建行**。

        第二个返回值是 found:False = 该池没打过。调用方必须能区分"没定级"与"分刚好
        等于基线",否则未定级玩家会被当成已定级展示。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        pool = prating.normalize(rating_pool)
        mmr, found = await self._repo.get_mmr(player_id, pool)
        if not found:
            return self._cfg.base_mmr, False
        return mmr, True

    async def list_ratings(self, player_id: int) -> list[m.PlayerRating]:
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        return await self._repo.list_ratings(player_id)

    async def update_mmr(
        self, player_id: int, delta: int, reason: str, idempotency_key: str, rating_pool: str
    ) -> tuple[int, bool]:
        """幂等改 MMR + 战绩计数。返回 (新 MMR, 是否幂等命中)。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if not idempotency_key:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "idempotency_key required")

        await self._ensure(player_id)
        inc_battle, inc_win = _battle_flags(reason)
        # 段位池归一化在**写入侧**做一次,与读侧 get_mmr 同一函数,杜绝写 default / 读 "" 的分裂。
        pool = prating.normalize(rating_pool)
        new_mmr, already = await self._repo.apply_mmr_change(
            m.MMRChange(
                player_id=player_id,
                idempotency_key=idempotency_key,
                rating_pool=pool,
                baseline=self._cfg.base_mmr,
                delta=delta,
                reason=reason,
                floor=self._cfg.mmr_floor,
                inc_battle=inc_battle,
                inc_win=inc_win,
            )
        )
        if already:
            # INFO 而非 DEBUG:幂等命中罕见(重投/重放的唯一证据),与 inventory 同口径。
            plog.get().info(
                "update_mmr_idempotent_hit",
                player_id=player_id,
                idempotency_key=idempotency_key,
                new_mmr=new_mmr,
                rating_pool=pool,
            )
            return new_mmr, True
        # INFO:段位入账是每玩家每局一次的资产变更台账(「打完段位没变」必须能按 player_id
        # 正查;上游 battle_result 只有出箱台账,入账终点在这里)。
        plog.get().info(
            "update_mmr_applied",
            player_id=player_id,
            delta=delta,
            reason=reason,
            new_mmr=new_mmr,
            rating_pool=pool,
        )
        # 档案落点观测(router 未注入时整条不执行)。位置与 Go 一致:写成功之后。
        self._log_profile_placement(player_id, "update_mmr")
        return new_mmr, False

    # ── 出战养成 ──────────────────────────────────────────────────────────

    async def select_hero(self, player_id: int, hero_id: int) -> None:
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if hero_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "hero_id required")
        if not self._cfg.hero_selection_enabled:
            raise errcode.PandoraError(
                errcode.ErrPlayerFeatureDisabled, "hero selection disabled"
            )
        await self._ensure(player_id)
        if not await self._repo.is_hero_owned(player_id, hero_id):
            raise errcode.PandoraError(
                errcode.ErrPlayerHeroLocked,
                "hero not owned: player=%d hero=%d",
                player_id,
                hero_id,
            )
        await self._repo.set_active_hero(player_id, hero_id)
        plog.get().debug("select_hero", player_id=player_id, hero_id=hero_id)

    async def get_active_hero(self, player_id: int) -> int:
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        return await self._repo.get_active_hero(player_id)

    async def grant_attribute_points(
        self, player_id: int, points: int, idempotency_key: str
    ) -> int:
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if points <= 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "points must be positive")
        if not idempotency_key:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "idempotency_key required")
        await self._ensure(player_id)
        unspent, already = await self._repo.grant_attribute_points(
            player_id, points, idempotency_key
        )
        if already:
            plog.get().debug(
                "grant_attr_idempotent_hit",
                player_id=player_id,
                idempotency_key=idempotency_key,
                unspent=unspent,
            )
        return unspent

    async def allocate_attribute_points(
        self, player_id: int, allocs: list[m.AttrAllocation]
    ) -> int:
        """分配属性点(点数不足 → ErrPlayerInsufficientPoints)。

        这里只堵「请求级」越界(总和 / 单键增量);列「当前值 + 增量」越界由 repo 在事务内
        权威兜底 —— 两层都要,前者能在不开事务的情况下拒掉明显非法请求。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if not allocs:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "allocations required")
        per_key: dict[str, int] = {}
        total = 0
        for a in allocs:
            if not a.key:
                raise errcode.PandoraError(errcode.ErrInvalidArg, "attr_key must not be empty")
            if a.points <= 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "points must be positive: %s", a.key
                )
            per_key[a.key] = per_key.get(a.key, 0) + a.points
            if per_key[a.key] > 2**31 - 1:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "attr %s allocation out of range", a.key
                )
            total += a.points
            if total > 2**31 - 1:
                raise errcode.PandoraError(
                    errcode.ErrPlayerInsufficientPoints, "total allocation out of range"
                )
        await self._ensure(player_id)
        return await self._repo.allocate_attribute_points(player_id, allocs)

    async def reset_attributes(self, player_id: int) -> int:
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        await self._ensure(player_id)
        return await self._repo.reset_attributes(player_id)

    async def get_attributes(self, player_id: int) -> tuple[list[m.AttrPoint], int]:
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        return await self._repo.get_attributes(player_id)

    # ── 出战装备预设 ──────────────────────────────────────────────────────

    async def set_equipment(self, player_id: int, slots: list[m.EquipmentSlot]) -> None:
        """全量替换出战装备预设。

        权威校验三项,任一依赖缺失一律 fail-closed:
          ① isEquip(item)        —— 道具表 equip_slot > 0 才是装备;
          ② slotMatch(item,slot) —— 道具表 equip_slot 必须与提交的槽位号完全一致;
          ③ ownEquipmentInstance —— 经 inventory.CheckInstancesOwned 精确确认该
             instance_id 当前归属玩家且配置 ID 一致。
        前两项读配置表内存快照(零 RPC),第三项跨服务查 inventory。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if not self._cfg.loadout_customize_enabled:
            raise errcode.PandoraError(
                errcode.ErrPlayerFeatureDisabled, "loadout customize disabled"
            )
        seen_slots: set[int] = set()
        seen_instances: set[int] = set()
        for s in slots:
            if s.item_config_id == 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "item_config_id required for slot %d", s.slot
                )
            if s.instance_id == 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "instance_id required for slot %d", s.slot
                )
            # 槽位号与道具表「装备部位」列同一编号空间,该列约定 0 = 不可穿戴,
            # 因此预设里的 slot 必须 >= 1:slot 0 永远匹配不到任何装备,只会变成一条恒失败的记录。
            if s.slot == 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "slot must be positive (0 means not equippable in item table)",
                )
            if s.slot in seen_slots:
                raise errcode.PandoraError(errcode.ErrInvalidArg, "duplicate slot %d", s.slot)
            seen_slots.add(s.slot)
            if s.instance_id in seen_instances:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "duplicate instance_id %d", s.instance_id
                )
            seen_instances.add(s.instance_id)

        self._validate_equipment_against_config(slots)
        await self._validate_equipment_ownership(player_id, slots)
        await self._ensure(player_id)
        await self._repo.set_equipment(player_id, slots)
        plog.get().debug("set_equipment", player_id=player_id, slots=len(slots))

    def _validate_equipment_against_config(self, slots: list[m.EquipmentSlot]) -> None:
        """用道具表判定「是不是装备 + 部位对不对」。表未加载时 fail-closed。

        不区分「不存在 / 不是装备 / 部位不符」三种原因:对外统一是一条非法预设,细分只会
        给探测配置表提供信号。日志侧仍可按 item/slot 定位。
        """
        if not slots:
            return
        tables = self._tables()
        if tables is None:
            raise errcode.PandoraError(errcode.ErrInternal, "item config table unavailable")
        for s in slots:
            if not tables.matches_slot(s.item_config_id, s.slot):
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "item %d cannot be equipped in slot %d",
                    s.item_config_id,
                    s.slot,
                )

    async def _validate_equipment_ownership(
        self, player_id: int, slots: list[m.EquipmentSlot]
    ) -> m.InstanceOwnershipResult:
        """经 inventory 系统 RPC 确认玩家持有全部待装备唯一实例。

        查询失败(依赖不可用 / 超时)按 §9.22 **fail-closed**:抛错让客户端重试,绝不把
        「查不到」当成「持有」。
        """
        if not slots:
            return m.InstanceOwnershipResult()
        checker = self._ownership
        if checker is None:
            raise errcode.PandoraError(
                errcode.ErrInternal, "instance ownership checker unavailable"
            )
        owned = await checker.check_instances_owned(player_id, slots)

        want = {s.instance_id: s.item_config_id for s in slots}
        owned_set: set[int] = set()
        for inst_id in owned.owned_instance_ids:
            if inst_id == 0 or want.get(inst_id, 0) == 0:
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "inventory returned unexpected owned instance id=%d player=%d",
                    inst_id,
                    player_id,
                )
            if inst_id in owned_set:
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "inventory returned duplicate owned instance id=%d player=%d",
                    inst_id,
                    player_id,
                )
            owned_set.add(inst_id)
        for s in slots:
            if s.instance_id not in owned_set:
                raise errcode.PandoraError(
                    errcode.ErrPermissionDeny,
                    "equipment instance %d (item %d) not owned by player %d",
                    s.instance_id,
                    s.item_config_id,
                    player_id,
                )
        return owned

    async def get_equipment(self, player_id: int) -> list[m.EquipmentSlot]:
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        return await self._repo.get_equipment(player_id)

    async def _validate_equipment_for_battle_snapshot(
        self, player_id: int, slots: list[m.EquipmentSlot]
    ) -> dict[int, m.OwnedEquipmentInstance]:
        """在 GetLoadout 交付战斗初始效果前**重新**核验持久化预设。

        SetEquipment 的 inventory 查询与 player 落库跨两个服务,不可能共享 MySQL 事务;
        实例可能在两步之间或预设保存后被其它权威路径转移。开战前再批量查一次 exact pair,
        确保陈旧预设不会继续给 Battle DS 产生效果。

        ★ 000006 前只按 item_config_id 保存的旧行无法证明是哪一件实例:允许 GetEquipment
          展示给玩家重选,但这里必须 fail-closed。
        ★ 滚动升级期旧 inventory 仍只回 owned_instance_ids。那足以保护 SetEquipment 新写,
          但不足以生成含鉴定词条的战斗快照 —— 详情缺失时同样 fail-closed。
        """
        seen_slots: set[int] = set()
        seen_instances: set[int] = set()
        for s in slots:
            if s.instance_id == 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "legacy equipment preset requires exact instance selection "
                    "player=%d slot=%d item=%d",
                    player_id,
                    s.slot,
                    s.item_config_id,
                )
            if s.slot in seen_slots:
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "stored equipment has duplicate slot player=%d slot=%d",
                    player_id,
                    s.slot,
                )
            seen_slots.add(s.slot)
            if s.instance_id in seen_instances:
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "stored equipment has duplicate instance player=%d instance=%d",
                    player_id,
                    s.instance_id,
                )
            seen_instances.add(s.instance_id)

        self._validate_equipment_against_config(slots)
        owned = await self._validate_equipment_ownership(player_id, slots)
        if not slots:
            return {}

        want = {s.instance_id: s.item_config_id for s in slots}
        details: dict[int, m.OwnedEquipmentInstance] = {}
        for inst in owned.owned_instances:
            requested = want.get(inst.instance_id)
            if inst.instance_id == 0 or requested is None or requested != inst.item_config_id:
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "inventory returned non-exact instance detail player=%d instance=%d item=%d",
                    player_id,
                    inst.instance_id,
                    inst.item_config_id,
                )
            if inst.instance_id in details:
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "inventory returned duplicate instance detail player=%d instance=%d",
                    player_id,
                    inst.instance_id,
                )
            if not inst.identified and inst.attributes:
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "unidentified instance returned attributes player=%d instance=%d",
                    player_id,
                    inst.instance_id,
                )
            if inst.identified and not inst.attributes:
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "identified instance missing attributes player=%d instance=%d",
                    player_id,
                    inst.instance_id,
                )
            seen_attrs: set[int] = set()
            for attr in inst.attributes:
                if attr.attr_id in seen_attrs:
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "instance detail has duplicate attr player=%d instance=%d attr=%d",
                        player_id,
                        inst.instance_id,
                        attr.attr_id,
                    )
                seen_attrs.add(attr.attr_id)
                max_value = _equipment_attribute_max_value(attr.attr_id)
                if max_value is None:
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "instance detail has unsupported attr player=%d instance=%d attr=%d",
                        player_id,
                        inst.instance_id,
                        attr.attr_id,
                    )
                if attr.value <= 0 or attr.value > max_value:
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "instance detail attr value out of range player=%d instance=%d "
                        "attr=%d value=%d max=%d",
                        player_id,
                        inst.instance_id,
                        attr.attr_id,
                        attr.value,
                        max_value,
                    )
            details[inst.instance_id] = inst
        for s in slots:
            if s.instance_id not in details:
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "inventory instance detail unavailable; rollout incomplete "
                    "player=%d instance=%d",
                    player_id,
                    s.instance_id,
                )
        return details

    # ── 天赋 ──────────────────────────────────────────────────────────────

    async def grant_talent_points(
        self, player_id: int, points: int, idempotency_key: str
    ) -> int:
        """幂等授予天赋点(系统驱动,**不受** loadout_customize_enabled 影响)。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if points <= 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "points must be positive")
        if not idempotency_key:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "idempotency_key required")
        await self._ensure(player_id)
        unspent, already = await self._repo.grant_talent_points(
            player_id, points, idempotency_key
        )
        if already:
            plog.get().debug(
                "grant_talent_idempotent_hit",
                player_id=player_id,
                idempotency_key=idempotency_key,
                unspent=unspent,
            )
        return unspent

    async def set_talents(self, player_id: int, talents: list[m.TalentLevel]) -> int:
        """全量重置天赋分配。

        权威校验按专精表:节点存在、等级不超 max_level、前置节点在**本次方案内**达标、
        总消耗按 Σ 等级 × cost_per_level 计算后交 repo 与可用点比对。表未加载一律
        fail-closed —— 此前只校验「id 非 0 + level > 0 + 不重复」,客户端填 level=999
        能直接写进库。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if not self._cfg.loadout_customize_enabled:
            raise errcode.PandoraError(
                errcode.ErrPlayerFeatureDisabled, "loadout customize disabled"
            )
        levels: dict[int, int] = {}
        for t in talents:
            if t.talent_id == 0:
                raise errcode.PandoraError(errcode.ErrInvalidArg, "talent_id required")
            if t.level <= 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "level must be positive: talent=%d", t.talent_id
                )
            if t.talent_id in levels:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "duplicate talent_id %d", t.talent_id
                )
            levels[t.talent_id] = t.level

        costs = self._talent_allocation_cost(levels)
        # 逐节点消耗随分配一起落库:读取侧直接 SUM 这一列,不再按 Σ 等级 反推 ——
        # 反推在 cost_per_level≠1 时会算少已花点数,玩家看到的可点数比实际多。
        priced = [
            m.TalentLevel(
                talent_id=t.talent_id, level=t.level, spent_points=costs.get(t.talent_id, 0)
            )
            for t in talents
        ]
        await self._ensure(player_id)
        return await self._repo.set_talents(player_id, priced)

    def _talent_allocation_cost(self, levels: dict[int, int]) -> dict[int, int]:
        """用专精表校验整份分配并算出逐节点消耗。

        表未加载时 fail-closed(ErrInternal);表判定非法时统一归为 ErrInvalidArg
        (具体原因带在消息里)—— 与 Go 的两级错误码分工逐字一致。
        """
        if not levels:
            return {}
        tables = self._tables()
        if tables is None:
            raise errcode.PandoraError(errcode.ErrInternal, "talent config table unavailable")
        try:
            costs, _total = tables.validate_talent_allocation(levels)
        except ConfigTableError as exc:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "invalid talent allocation: %s", exc
            ) from exc
        return costs

    async def reset_talents(self, player_id: int) -> int:
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if not self._cfg.loadout_customize_enabled:
            raise errcode.PandoraError(
                errcode.ErrPlayerFeatureDisabled, "loadout customize disabled"
            )
        await self._ensure(player_id)
        return await self._repo.reset_talents(player_id)

    async def get_talents(self, player_id: int) -> tuple[list[m.TalentLevel], int]:
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        return await self._repo.get_talents(player_id)

    # ── 技能卡 ────────────────────────────────────────────────────────────

    async def grant_skill_cards(
        self, player_id: int, grants: list[m.SkillCardGrant], idempotency_key: str
    ) -> tuple[list[m.SkillCard], bool]:
        """幂等发放技能卡 / 碎片(系统 RPC)。不受 loadout_customize_enabled 约束 ——
        开关管的是玩家自助改配装,发放是系统行为。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if not grants:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "grants required")
        if not idempotency_key:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "idempotency_key required")

        seen: set[int] = set()
        tables = self._tables()
        for g in grants:
            if g.card_id == 0:
                raise errcode.PandoraError(errcode.ErrInvalidArg, "card_id required")
            if g.card_id in seen:
                # 同一批里同一张卡出现两次,两条 ON DUPLICATE KEY UPDATE 会各加一次碎片,
                # 结果对但很难对账。要求调用方自己合并,发放意图才始终是一目了然的。
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "duplicate card_id %d in one grant", g.card_id
                )
            seen.add(g.card_id)
            # 发放的卡必须在配置表里:发一张表里没有的卡,玩家背包里会出现一张永远打不开、
            # 升不了、装不上的幽灵卡,且没有任何报错。
            if tables is None:
                raise errcode.PandoraError(
                    errcode.ErrInternal, "skill card config table unavailable"
                )
            if not tables.card_exists(g.card_id):
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "unknown skill card %d", g.card_id
                )

        await self._ensure(player_id)
        cards, already = await self._repo.grant_skill_cards(player_id, grants, idempotency_key)
        if already:
            plog.get().debug(
                "grant_skill_cards_idempotent_hit",
                player_id=player_id,
                idempotency_key=idempotency_key,
            )
        return cards, already

    async def upgrade_skill_card(self, player_id: int, card_id: int) -> tuple[m.SkillCard, int]:
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if card_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "card_id required")
        if not self._cfg.loadout_customize_enabled:
            raise errcode.PandoraError(
                errcode.ErrPlayerFeatureDisabled, "loadout customize disabled"
            )
        tables = self._tables()
        if tables is None:
            raise errcode.PandoraError(errcode.ErrInternal, "skill card config table unavailable")
        # 整条曲线交给 repo 在事务内按锁到的等级查价:先读等级再算价会让并发两次升级
        # 都按同一级的价钱扣(§16.1 TOCTOU)。
        try:
            curve, max_level = tables.upgrade_curve(card_id)
        except ConfigTableError as exc:
            # 卡不存在 → ErrInvalidArg;曲线断档 → ErrInternal(配置事故,不是玩家的错)。
            code = (
                errcode.ErrInvalidArg
                if str(exc).startswith("unknown skill card")
                else errcode.ErrInternal
            )
            raise errcode.PandoraError(code, "%s", exc) from exc
        return await self._repo.upgrade_skill_card(player_id, card_id, curve, max_level)

    async def set_skill_slots(self, player_id: int, slots: list[m.SkillSlot]) -> list[m.SkillSlot]:
        """全量替换卡槽装配。card_id=0 表示显式清空该槽 —— 清空不落行,直接剔除。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if not self._cfg.loadout_customize_enabled:
            raise errcode.PandoraError(
                errcode.ErrPlayerFeatureDisabled, "loadout customize disabled"
            )
        tables = self._tables()
        if tables is None:
            raise errcode.PandoraError(errcode.ErrInternal, "skill card config table unavailable")

        filled: list[m.SkillSlot] = []
        seen_slot: set[int] = set()
        seen_card: set[int] = set()
        for s in slots:
            if s.slot >= SKILL_SLOT_COUNT:
                raise errcode.PandoraError(
                    errcode.ErrSkillCardSlotInvalid,
                    "slot %d out of range [0,%d)",
                    s.slot,
                    SKILL_SLOT_COUNT,
                )
            if s.slot in seen_slot:
                raise errcode.PandoraError(
                    errcode.ErrSkillCardSlotInvalid, "duplicate slot %d", s.slot
                )
            seen_slot.add(s.slot)

            if s.card_id == 0:
                continue  # 显式清空:不落行。
            if s.card_id in seen_card:
                # 同一张卡占两个槽。库上有 uk_player_card_once 兜底,但那会返回一条面向
                # 并发的错误;在这里判能给出准确的"是哪张卡重复了"。
                raise errcode.PandoraError(
                    errcode.ErrSkillCardSlotInvalid,
                    "skill card %d assigned to more than one slot",
                    s.card_id,
                )
            seen_card.add(s.card_id)
            if not tables.card_exists(s.card_id):
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "unknown skill card %d", s.card_id
                )
            filled.append(s)

        # 持有校验在 repo 的事务内做(与删旧插新同一把锁),这里不预查:
        # 预查会引入"查到持有 → 期间卡被消耗 → 装上了没有的卡"的窗口。
        await self._repo.set_skill_slots(player_id, filled)
        return filled

    async def get_skill_cards(
        self, player_id: int
    ) -> tuple[list[m.SkillCard], list[m.SkillSlot]]:
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        cards = await self._repo.get_skill_cards(player_id)
        slots = await self._repo.get_skill_slots(player_id)
        return cards, slots

    # ── 出战快照 ──────────────────────────────────────────────────────────

    async def get_loadout(self, player_id: int) -> ppb.PlayerLoadout:
        """组装开战前快照(出战英雄 + 属性点 + 装备预设 + 天赋 + 技能卡槽)。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        hero_id = await self._repo.get_active_hero(player_id)
        attrs, unspent = await self._repo.get_attributes(player_id)
        equip = await self._repo.get_equipment(player_id)
        instance_details = await self._validate_equipment_for_battle_snapshot(player_id, equip)

        equipment_pb: list[ppb.LoadoutEquipment] = []
        for s in equip:
            detail = instance_details.get(s.instance_id)
            attributes = (
                [
                    inv_pb.ItemAttribute(attr_id=a.attr_id, value=a.value)
                    for a in detail.attributes
                ]
                if detail is not None
                else []
            )
            equipment_pb.append(
                ppb.LoadoutEquipment(
                    slot=s.slot,
                    item_config_id=s.item_config_id,
                    instance_id=s.instance_id,
                    identified=bool(detail.identified) if detail is not None else False,
                    attributes=attributes,
                )
            )

        talents, talent_unspent = await self._repo.get_talents(player_id)
        cards, slots = await self.get_skill_cards(player_id)
        # 卡等级随槽位一起带出:DS 只关心"这个槽是哪张卡、几级",让它拿着 card_id 再查
        # 一次持有表等于把一次读拆成两次(且中间可能被改)。
        level_of = {c.card_id: c.level for c in cards}
        return ppb.PlayerLoadout(
            player_id=player_id,
            active_hero_id=hero_id,
            attributes=[
                ppb.AttributeAllocation(attr_key=a.key, points=a.points) for a in attrs
            ],
            unspent_attr_points=unspent,
            equipment=equipment_pb,
            talents=[ppb.TalentNode(talent_id=t.talent_id, level=t.level) for t in talents],
            unspent_talent_points=talent_unspent,
            skill_cards=[
                ppb.LoadoutSkillCard(
                    slot=s.slot, card_id=s.card_id, level=level_of.get(s.card_id, 0)
                )
                for s in slots
            ],
        )

    # ── 玩家等级经验 ──────────────────────────────────────────────────────

    def experience_curve(self) -> list[int]:
        """从当前原子快照提取本次调用使用的曲线副本。"""
        tables = self._tables()
        return tables.experience_curve() if tables is not None else []

    def decorate_experience(self, level: int, exp_in_level: int) -> tuple[int, bool]:
        """用等级曲线给档案补经验派生字段(GetProfile 出参装饰)。

        满级 → is_max_level=True 且级内经验按 0 展示(权威列已保证满级恒 0,此处防御性夹紧)。
        曲线未配置 / 功能关闭 → 不标满级,exp 原样,行为与历史一致。
        """
        if not self._cfg.experience_enabled:
            return exp_in_level, False
        curve = self.experience_curve()
        if not curve:
            return exp_in_level, False
        max_level = len(curve) + 1
        if level >= max_level:
            return 0, True
        return exp_in_level, False

    async def add_experience(
        self, player_id: int, delta: int, reason: str, idempotency_key: str
    ) -> tuple[m.ExpState, bool]:
        """幂等入账经验并结算等级(实时成长唯一入口,系统调用)。

        校验顺序与 Go 逐条相同 —— 顺序本身有意义:参数校验在功能开关之前,这样"功能关了"
        不会掩盖掉"调用方传了非法参数"。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if not idempotency_key:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "idempotency_key required")
        if delta == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "exp_delta must be positive")
        max_grant = self._cfg.max_exp_per_grant_effective()
        if delta > max_grant:
            # 防异常 / 越权调用方一次灌满等级(battle_result 已按怪物表换算,这里是
            # player 侧最后一道兜底)。
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "exp_delta %d exceeds max_exp_per_grant %d",
                delta,
                max_grant,
            )
        if not self._cfg.experience_enabled:
            raise errcode.PandoraError(errcode.ErrPlayerFeatureDisabled, "experience disabled")
        curve = self.experience_curve()
        if not curve:
            raise errcode.PandoraError(
                errcode.ErrPlayerFeatureDisabled,
                "experience disabled (player level table unavailable)",
            )
        await self._ensure(player_id)

        state, already = await self._repo.apply_experience(
            m.ExpApply(
                player_id=player_id,
                delta=delta,
                reason=reason,
                idempotency_key=idempotency_key,
                curve=tuple(curve),
            )
        )
        if already:
            plog.get().debug(
                "add_experience_idempotent_hit",
                player_id=player_id,
                idempotency_key=idempotency_key,
                level=state.level,
                exp_in_level=state.exp_in_level,
            )
            return state, True
        plog.get().debug(
            "add_experience_applied",
            player_id=player_id,
            delta=delta,
            reason=reason,
            level=state.level,
            exp_in_level=state.exp_in_level,
            levels_gained=state.levels_gained,
            is_max=state.is_max_level,
        )
        return state, False

    # ── 领奖 ──────────────────────────────────────────────────────────────

    async def _load_reward_record(
        self, player_id: int
    ) -> tuple[rewardclaim.Record, ppb.RewardClaimStorageRecord, int]:
        """读取并反序列化领奖记录,返回 (内存 Record, 底层存储 message, 乐观锁版本)。

        ★ stored 必须原样保留到 _save_reward_record 回写:金丝雀 / 滚动共存窗口内,新副本
          可能已写入本副本不认识的新字段(unknown fields),丢弃 stored 重建 message 等效
          DiscardUnknown,会**静默清掉新字段**(§17 / zero-downtime §2.3、§7.3)。
        """
        raw, version = await self._repo.load_reward_claims(player_id)
        stored = ppb.RewardClaimStorageRecord()
        if raw:
            try:
                stored.ParseFromString(raw)
            except Exception as exc:  # noqa: BLE001 —— proto 解码失败
                raise errcode.PandoraError(
                    errcode.ErrInternal, "decode reward record player=%d: %s", player_id, exc
                ) from exc
        rec = rewardclaim.load(dict(stored.permanent), dict(stored.activity))
        return rec, stored, version

    async def _save_reward_record(
        self,
        player_id: int,
        rec: rewardclaim.Record,
        stored: ppb.RewardClaimStorageRecord,
        expect_version: int,
    ) -> None:
        """把内存 Record 写回读取时的 stored message 后乐观锁落库。

        只原地覆盖 permanent / activity 两个字段,stored 携带的 unknown fields 随序列化
        原样带回 —— **禁止重建 message**。
        """
        permanent, activity = rec.snapshot()
        stored.ClearField("permanent")
        stored.ClearField("activity")
        for src, bits in permanent.items():
            stored.permanent[src] = bits
        for inst, bits in activity.items():
            stored.activity[inst] = bits
        await self._repo.save_reward_claims(
            player_id, stored.SerializeToString(), expect_version
        )

    async def claim_reward(
        self,
        player_id: int,
        source_type: int,
        source: str,
        activity_instance_id: int,
        reward_id: int,
    ) -> None:
        """领取一档奖励(客户端权威领取,幂等)。

        已领取 → ErrRewardAlreadyClaimed;reward_id 超 bit 上界 → ErrRewardUnknownID。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if reward_id >= rewardclaim.MAX_BIT_INDEX:
            raise errcode.PandoraError(
                errcode.ErrRewardUnknownID, "reward_id out of range: %d", reward_id
            )
        if source_type == ppb.REWARD_SOURCE_TYPE_PERMANENT:
            if not source:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "source required for permanent reward"
                )
        elif source_type == ppb.REWARD_SOURCE_TYPE_ACTIVITY:
            if activity_instance_id == 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "activity_instance_id required for activity reward"
                )
        else:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "unknown reward source_type: %d", source_type
            )

        last_err: BaseException | None = None
        for _attempt in range(MAX_REWARD_CLAIM_RETRY):
            rec, stored, version = await self._load_reward_record(player_id)
            try:
                if source_type == ppb.REWARD_SOURCE_TYPE_PERMANENT:
                    rec.claim_permanent(source, reward_id)
                else:
                    rec.claim_activity(activity_instance_id, reward_id)
            except rewardclaim.AlreadyClaimedError as exc:
                raise errcode.PandoraError(
                    errcode.ErrRewardAlreadyClaimed,
                    "reward already claimed: player=%d id=%d",
                    player_id,
                    reward_id,
                ) from exc
            except rewardclaim.IndexTooLargeError as exc:
                raise errcode.PandoraError(
                    errcode.ErrRewardUnknownID, "reward_id out of range: %d", reward_id
                ) from exc
            except (rewardclaim.TooManyEntriesError, rewardclaim.SourceNameTooLongError) as exc:
                # 位图条目数 / 来源名触顶(§9.18 在 blob 内部的对应物):record 落进 LONGBLOB,
                # DB 层不设防,只能在此 fail-closed。正常玩家永远碰不到(来源与活动都来自
                # 配置表);触发即意味着有人在刷任意 source / activity_instance_id,
                # 或运营配置失控 —— 两种都要人查,故 ERROR 留证。
                plog.get().error(
                    "reward_claim_entry_limit_exceeded",
                    player_id=player_id,
                    source_type=source_type,
                    source=source,
                    activity_instance_id=activity_instance_id,
                    reward_id=reward_id,
                    err=str(exc),
                    hint=(
                        "领奖记录位图条目数达上限:排查是否有客户端刷任意 "
                        "source/activity_instance_id,或活动实例未经回收"
                    ),
                )
                # 客户端只拿到 ErrRewardUnknownID,不外泄内部上限。
                raise errcode.PandoraError(
                    errcode.ErrRewardUnknownID, "reward source rejected: player=%d", player_id
                ) from exc

            try:
                await self._save_reward_record(player_id, rec, stored, version)
            except errcode.PandoraError as exc:
                if exc.code == errcode.ErrPlayerVersionMismatch:
                    last_err = exc
                    continue  # 并发冲突,重读重试
                raise
            return

        # 乐观锁重试耗尽(§16 CAS/写竞争耗尽盲点):热点玩家并发领取同一档。经 in-band 码
        # 返回会被 access log 记成 rpc_ok,竞争风暴无法与偶发冲突区分 → WARN 留证。
        plog.get().warning(
            "reward_claim_version_conflicts_exhausted",
            player_id=player_id,
            reward_id=reward_id,
            attempts=MAX_REWARD_CLAIM_RETRY,
            last_err=str(last_err),
        )
        raise errcode.PandoraError(
            errcode.ErrPlayerVersionMismatch,
            "claim reward player=%d id=%d: too many version conflicts: %s",
            player_id,
            reward_id,
            last_err,
        )

    async def get_reward_claims(
        self, player_id: int, source_type: int, source: str, activity_instance_id: int
    ) -> list[int]:
        """查询某来源已领取的奖励配置 ID 列表(客户端可见最小视图,不外露位图)。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        rec, _stored, _version = await self._load_reward_record(player_id)
        if source_type == ppb.REWARD_SOURCE_TYPE_PERMANENT:
            if not source:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "source required for permanent reward"
                )
            return rec.permanent_claimed_indices(source)
        if source_type == ppb.REWARD_SOURCE_TYPE_ACTIVITY:
            if activity_instance_id == 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "activity_instance_id required for activity reward"
                )
            return rec.activity_claimed_indices(activity_instance_id)
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "unknown reward source_type: %d", source_type
        )

    # ── 后台循环:经验推送出箱发布器 ───────────────────────────────────────

    def _push_is_leader(self) -> bool:
        """判定本轮是否由本副本发布,并把领导权跃迁打成日志(未注入恒为 True)。"""
        lease = self._push_lease
        if lease is None:
            return True
        held = bool(lease.current()[0])
        if held != self._push_lease_held:
            self._push_lease_held = held
            plog.get().info(
                "push_outbox_leadership_changed",
                held=held,
                hint="held=false 时本副本热备不发布;出箱由当选副本排空",
            )
        return held

    async def run_push_outbox_publisher(self) -> None:
        """后台玩家推送出箱发布循环(FIFO、失败中断本轮保序、成功才删行)。

        **边界**:选举只保证「同一时刻一个副本发布」以取得**保序**,不是防脑裂,故出箱的
        写不需要把 fencing token 带进事务 —— 发布器不携带跨轮次权威意图(每轮从 MySQL
        重读,动作只是投 kafka + 删行,权威态 level/exp 全程不被触碰)。迟到的旧 leader
        做的是与新 leader 逐字相同的重放,唯一危害是投递顺序交错,而那正是本选举要消除的。
        """
        if self._exp_pusher is None:
            plog.get().info(
                "push_outbox_publisher_disabled",
                hint="kafka producer 未注入 → 经验推送出箱积压不丢,producer 可用后重启补发",
            )
            return
        interval = self._cfg.push_outbox_interval_sec()
        batch = self._cfg.push_outbox_batch_effective()
        plog.get().info("push_outbox_publisher_started", interval=interval, batch=batch)

        async def _round() -> None:
            # 领导权逐轮判定(§9.21 同一未分区权威的单写者循环)。失主后本副本立刻停止
            # 发布,**不"补完"在飞的行** —— 那正是交错的来源。
            if not self._push_is_leader():
                return
            # 排空循环:满批说明还有积压,立即继续下一批,不等下个 tick(否则吞吐被钉死在
            # batch/interval,持续流量下积压只增不减)。
            while True:
                try:
                    published = await self._publish_push_outbox_batch()
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    plog.get().warning("push_outbox_publish_batch_failed", err=str(exc))
                    break
                if published < batch:
                    break  # 未满批 = 已清空

        # safego.loop 每轮独立兜底:单轮异常只丢本轮,出箱行下轮重试。
        await safego.loop("player_push_outbox_publisher", interval, _round)

    async def _publish_push_outbox_batch(self) -> int:
        """取一批出箱记录投递,返回本轮成功投递并删除的条数。

        投递失败**立即中断本轮**(保留出箱行下轮重试),保证同玩家事件按 id 顺序投递(§9)。
        """
        if self._exp_pusher is None:
            return 0
        records = await self._repo.fetch_push_outbox(self._cfg.push_outbox_batch_effective())
        published = 0
        for r in records:
            await self._exp_pusher.push_player_event(r.player_id, r.event_type, r.payload)
            await self._repo.delete_push_outbox(r.id)
            published += 1
        if published > 0:
            plog.get().debug("push_outbox_published", count=published)
        return published

    # ── 后台循环:保留期 janitor ───────────────────────────────────────────

    def _exp_history_sweeps(self) -> list[tuple[str, object]]:
        """exp_history 组的登记清单。新增只增表时必须在这里接入。"""
        return [("exp_history", self._repo.sweep_exp_history)]

    def _history_sweeps(self) -> list[tuple[str, object]]:
        return [
            ("mmr_history", self._repo.sweep_mmr_history),
            ("attr_point_grants", self._repo.sweep_attr_point_grants),
            ("talent_point_grants", self._repo.sweep_talent_point_grants),
            ("skill_card_grants", self._repo.sweep_skill_card_grants),
        ]

    async def run_exp_history_janitor(self) -> None:
        """exp_history 幂等收据的保留期循环。

        **默认只报告不删**(§9.24):janitor 照常跑,每轮统计"有多少行超过留存期"并打
        WARN + pending gauge,一行都不删。真删要两道闸同时开:`retention_mode: delete`
        + `exp_history_cleanup_enabled: true`。

        后者为什么单独存在:battle_result progress 出箱是永久重试链(退避上限 5min,无总
        重试期限)。入账成功但响应丢失 / 删行持续失败超过留存期时,清掉收据 = 同一事件
        **再次入账(双发)**。幂等正确性优先于表增长。

        ★ 早先的实现是前置条件没确认就**整个不跑** —— 那等于既不删也不报,§9.24 要的待
          清理量彻底不可见,库在无人知晓的情况下涨。现在降级为 report_only 而不是不跑。
        """
        await self._run_retention_janitor(
            "player_exp_history_janitor",
            lambda: self._cfg.exp_history_retention_effective(),
            self._cfg.exp_history_retention_mode,
            self._exp_history_sweeps(),
        )

    async def run_history_janitor(self) -> None:
        """mmr_history / attr_point_grants / talent_point_grants / skill_card_grants
        幂等历史的保留期循环(§9 不变量 24:只增表必须有界)。

        默认同样只报告不删。前置条件与 exp_history 同源但不同上游:kafka player.update
        消费与授予补扫是 at-least-once,清掉幂等行后同一事件重放 = 双发(重复加段位分 /
        加点 / 重复发卡)。
        """
        await self._run_retention_janitor(
            "player_history_janitor",
            lambda: self._cfg.history_retention_effective(),
            self._cfg.history_retention_mode,
            self._history_sweeps(),
        )

    async def _run_retention_janitor(self, name, retention_of, mode_of, sweeps) -> None:  # noqa: ANN001
        """两个 janitor 的共同循环:按节拍取一次 mode 与 cutoff,逐表 sweep。

        ★ mode **每轮现取**(而不是启动时定死):这样改配置重启副本即可生效,也不会出现
          "半个循环用旧模式"的错位。
        """

        async def _round() -> None:
            mode = mode_of()
            cutoff = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None) - retention_of()
            for table, sweep in sweeps:
                await self._drain_retention(mode, cutoff, table, sweep)

        await safego.loop(name, RETENTION_SWEEP_INTERVAL_SEC, _round)

    async def _drain_retention(self, mode, cutoff, table, sweep) -> None:  # noqa: ANN001
        """处理一张表:delete 模式下小批量循环删到追平,report_only 只跑一轮。

        ★ report_only 下**不循环**:那一轮的 COUNT 已经给出全量待清理规模(不受 batch
          截断),再循环只是把同一条 COUNT 重复执行 —— 循环的意义是"追平积压",而只报告时
          积压永远追不平,循环会变成每轮固定跑满的空转。
        """
        deleted = 0
        while True:
            try:
                out = await sweep(mode, cutoff, RETENTION_SWEEP_BATCH)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "retention_sweep_failed",
                    table=table,
                    mode=mode.value,
                    deleted_before_fail=deleted,
                    err=str(exc),
                )
                return
            if mode is not dbguard.Mode.DELETE:
                # 待清理量的 WARN + gauge 已由 dbguard.sweep_table 统一打,这里不重复。
                return
            deleted += out.deleted
            # ★ 判据只能是 truncated,不能从 matched/deleted 推。
            #
            # 这里曾写成 `out.deleted == 0 or out.matched <= out.deleted`,配一句
            # "matched > deleted 说明本轮被 batch 截断" —— 那句话描述的是 REPORT_ONLY
            # 档的形状,而上面第一道 `mode is not DELETE` 已经把那一档 return 掉了。
            # DELETE 档里 `matched == deleted` 恒成立(dbguard.sweep_table 与 Go 的
            # `out.Matched, out.Deleted = n, n` 同形),于是判据恒真:每轮只删一批
            # (RETENTION_SWEEP_BATCH=1000)就退出,五张只增表在 retention_mode=delete
            # 下每表每小时最多删 1000 行,积压永远追不平(§9.24 容量守护形同虚设),
            # 而同一轮的 `db_retention_deleted` 日志正打着 truncated=True。
            # Go 侧(experience.go:303)用的就是 `if !out.Truncated { break }`。
            if not out.truncated:
                break
        if deleted > 0:
            plog.get().info("retention_swept", table=table, deleted=deleted)

    # ── 容量巡检 ──────────────────────────────────────────────────────────

    async def close(self) -> None:
        """释放可选依赖(inventory 客户端)。进程退出路径调用。"""
        checker = self._ownership
        if checker is not None and hasattr(checker, "close"):
            with contextlib.suppress(Exception):
                await checker.close()


def now_ms() -> int:
    return int(time.time() * 1000)
