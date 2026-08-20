"""matchmaker 业务逻辑核心 —— 对应 Go 侧 internal/biz/match.go。

撮合流水线(docs/design/go-services.md §2.8):

    StartMatch(team) → 落一条 durable start operation(RPC 的唯一提交点)
       后台 worker:ACCEPTED → TICKET_READY → CLAIMING → CLAIMS_READY → QUEUED
       后台 RunMatchLoop:matchOnce 按 MMR 窗口贪心装箱凑齐 side_count×team_size
       → 建 match → 进确认期
       ConfirmMatch:全员 accept → ALLOCATING → 拉 DS → READY;
       任一 reject / 超时 → FAILED + 其余票据退回队列

协议铁律(docs/design/protocol-ordering-rules.md):
  - 6 个 RPC 全"已受理型"(原则 3):客户端 UI 状态机由 pandora.match.progress push 驱动
  - ★ **原则 3 的例外**:match 进度 push 发给**所有人(含发起方)**,caller_player_id=0。
    写成"排除发起方"会让组队非队长成员唯一的 READY 通道永久静默丢失 ——
    他们没有 match_id、不能轮询 GetMatchProgress,推送是他们得知成局的唯一渠道。
  - kafka key=player_id(不变量 §9)由 push_to_players 保证

关键不变量:
  - 同一玩家只能在一个 match 队列(claim SETNX,**无 TTL**)
  - 确认期内有人拒绝 → 其他人退回队列(保留 enqueued_at_ms 排队时长)

★ 本类的两张面刻意分文件:
  - 本文件 = **RPC 面**(每个副本都跑,无状态读写权威记录)
  - matchloop.py = **后台写者面**(未分区权威,多副本时必须过 leader election,§9.21)
  分开是为了让"哪些代码只有 leader 能跑"在文件边界上看得见。
"""

from __future__ import annotations

import asyncio
import time

from pandora.config.v1 import level_pb2 as levelpb
from pandora.match.v1 import match_pb2 as matchpb

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy.services.matchmaker import helpers as h
from pandorapy.services.matchmaker import presence_gate
from pandorapy.services.matchmaker.clients import (
    ROSTER_LOCK_LEASE_MS,
    new_operation_id,
    roster_lock_operation_id,
)
from pandorapy.services.matchmaker.matchloop import MatchLoopMixin
from pandorapy.services.matchmaker.rpcs import MatchRpcMixin

# saga 租约:只防 leader 交接窗口内并行推进,不承载业务真相。
START_OPERATION_LEASE_SEC = 15.0
# 全库扫描(索引重建)的节流间隔。
CANONICAL_RECONCILE_EVERY_SEC = 5.0
# 队列在线扫除 / 排队票离线回收的共用节流间隔。
LIVENESS_SWEEP_INTERVAL_SEC = 10.0
# 成局时最老票据排队时长的告警阈值。
STALE_TICKET_WARN_AGE_MS = 10 * 60 * 1000


def now_ms() -> int:
    return int(time.time() * 1000)


class MatchUsecase(MatchRpcMixin, MatchLoopMixin):
    """matchmaker 业务逻辑核心。

    ★ 两个 mixin **都必须挂上**,而且漏挂是静默的:
      MatchRpcMixin  = CancelMatch / ConfirmMatch / ReleaseMatch / GetMatchProgress /
                       ResolvePlayerMatchContext 五个 RPC 的主体(rpcs.py)
      MatchLoopMixin = 后台撮合写者面(matchloop.py)
    只挂后者的话,类照常能构造、进程照常起来、单测照常全绿(没有哪条测试碰得到
    那五个方法),而线上表现是**除 StartMatch 外每个 RPC 都回 ERR_UNKNOWN** ——
    因为 service 层的宽 except 会把 AttributeError 压成 in-band 错误码,
    gRPC status 还是 OK。2026-08-19 起真进程 e2e 打过一遍这五个方法才发现。

    可选依赖一律 nil-safe(不注入 = 整道功能跳过,与该功能落地前逐字节等价):
      reader    team 未起时跳过队伍校验(必须配 allow_missing_team 才允许)
      locator   本机不起 player_locator 时不上报位置、不查战斗态
      presence  在线闸与排队票离线回收的权威读入口
      tables    配置表未启用时跳过 map_id 校验(历史行为)
      limiter   dev 无 Redis 联调时不限流
      pusher    dev 纯轮询模式(只有队长能看到 READY)
    """

    def __init__(
        self,
        repo,  # noqa: ANN001
        reader,  # noqa: ANN001
        pusher,  # noqa: ANN001
        allocator,  # noqa: ANN001
        id_gen,  # noqa: ANN001
        locator,  # noqa: ANN001
        cfg,  # noqa: ANN001 —— conf.MatchConf
    ) -> None:
        self.repo = repo
        self.reader = reader
        self.pusher = pusher
        self.allocator = allocator
        self.id_gen = id_gen
        self.locator = locator
        self.cfg = cfg

        self.presence = None
        self.tables = None
        self.entry_limiter = None

        # 撮合循环内部节流游标(只在单 goroutine/单 task 里读写,无需加锁)。
        self._last_liveness_sweep = 0.0
        self._last_start_reconcile = 0.0
        self._last_match_reconcile = 0.0

        workers = cfg.allocation_workers
        if workers <= 0:
            workers = 1  # 兜底:未配 / 非法时退化为串行(等价历史行为)
        # 分配含最长 ~60s 的 RPC,不得在 listing 循环里串行内联(READY 补推会被头阻塞)。
        self._alloc_sem = asyncio.Semaphore(workers)

    # ── 可选依赖注入 ────────────────────────────────────────────────────────

    def set_presence_reader(self, presence) -> None:  # noqa: ANN001
        self.presence = presence

    def set_config_tables(self, store) -> None:  # noqa: ANN001
        self.tables = store

    def set_entry_limiter(self, limiter) -> None:  # noqa: ANN001
        self.entry_limiter = limiter

    # ── 配置表派生的开局形状 ────────────────────────────────────────────────

    def _effective_map_id(self, map_id: int) -> int:
        """map_id==0 表示「用本实例默认副本」。

        全仓只有这一处兜底口径,所有读表的地方都必须先过它 —— 否则
        「老客户端不发 map_id」与「新客户端显式发默认 map_id」会解析到两套人数。
        """
        return map_id if map_id != 0 else self.cfg.map_id

    def _level_row(self, map_id: int):  # noqa: ANN201
        if self.tables is None:
            return None
        tb = self.tables.tables
        if tb is None:
            return None
        return tb.by_id(self._effective_map_id(map_id))

    def validate_map_id(self, map_id: int) -> None:
        """StartMatch 入口的 map_id 关卡表准入门(配置表未启用时放行,历史行为)。

        不设这道闸会怎样:客户端上送的任意 map_id 会一路透传成 DS 的
        PANDORA_MAP_ID,拉起一台去加载不存在关卡的 DS —— 失败点跑到 DS 启动里。
        """
        if self.tables is None:
            return
        tb = self.tables.tables
        if tb is None:
            # 启用了配置表却无生效批次:main 的启动强依赖保证不会出现;真出现只能 fail-closed。
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "config tables enabled but not loaded"
            )
        effective = self._effective_map_id(map_id)
        if not tb.is_battle_level(effective):
            raise errcode.PandoraError(
                errcode.ErrMatchInvalidMap,
                "map_id %d not a battle level in level table (version %d)",
                effective,
                tb.version,
            )
        # 玩法模式交叉校验(§17.1「差异进表」的服务端一侧)。
        # 关卡表 game_mode 是「这张图属于哪个撮合池」的唯一事实源;本实例的
        # cfg.game_mode 是「本部署承接哪个池」。两者不等说明这次请求根本不该落到本实例。
        # ★ **留空只跳过校验,绝不据此拒绝**(§9.21 双向兼容):关卡表热更与二进制
        # 发布相互独立,"新二进制 + 旧批次表(无本列)"在滚动升级窗口内必然出现;
        # 把"读不到列"当成"配置错误"会让该窗口内所有匹配全失败。
        # 语义同 DS 侧关卡门:**Mismatch(读到且不同)才是证据,Unknown 只是无法判定**。
        row = tb.by_id(effective)
        if row is not None and row.game_mode and row.game_mode != self.cfg.game_mode:
            raise errcode.PandoraError(
                errcode.ErrMatchInvalidMap,
                "map_id %d belongs to game_mode %r but this matchmaker serves %r",
                effective,
                row.game_mode,
                self.cfg.game_mode,
            )

    def map_reject_reason(self, map_id: int) -> str:
        """把 validate_map_id 的拒绝原因归一成固定枚举串(仅供日志,只读无副作用)。

        单看 errcode 分不出「不是战斗关卡」与「玩法模式不属本实例」—— 两者同为
        ErrMatchInvalidMap,而排障时这两件事的处置完全不同(改表 vs 改路由头/热更)。
        """
        if self.tables is None:
            return "map_gate_unexpected"
        tb = self.tables.tables
        if tb is None:
            return "config_table_not_loaded"
        effective = self._effective_map_id(map_id)
        if not tb.is_battle_level(effective):
            return "map_not_battle_level"
        row = tb.by_id(effective)
        if row is not None and row.game_mode and row.game_mode != self.cfg.game_mode:
            return "map_game_mode_mismatch"
        return "map_gate_unexpected"

    def team_size_for_map(self, map_id: int) -> int:
        """一方人数:关卡表 team_size>0 时按表,否则回退服务端全局 cfg.team_size。

        回退是契约明确的合法路径,不是错误(表内 0 = 沿用全局兜底),不告警。
        全局值已在 conf.apply_defaults 里钳到 [1,50];这里再钳表值是防"手改 dist /
        绕过生成器"把 team_size 填成天文数字 —— 那会让 need 变成天文数字,
        撮合每 tick 都在构造巨大列表(§16.5)。
        """
        fallback = _clamp_team_size(self.cfg.team_size)
        row = self._level_row(map_id)
        if row is None:
            return fallback
        if row.team_size > 0:
            return _clamp_team_size(int(row.team_size))
        return fallback

    def side_count_for_map(self, map_id: int) -> int:
        """对局方数:关卡表 side_count>0 时按表,否则回退 2(历史 need=2×team_size)。

        PVE 合作副本填 1,多队混战填 N。
        """
        fallback = 2
        row = self._level_row(map_id)
        if row is None or row.side_count <= 0:
            return fallback
        return min(int(row.side_count), _MAX_TEAM_SIZE)

    def min_team_size_for_map(self, map_id: int) -> int:
        """直进人数下限(关卡表 min_team_size;0 / 表未启用 / 行不存在 = 无下限)。

        上界钳制刻意用 team_size_for_map 而非常量:加载期已校验 min ≤ team_size,
        这里再钳一次是防"手改 dist"把下限填得比上限还大 —— 那会让该图**任何人数
        都进不去**,是个静默的拒服务(§16.5 不能只靠加载期一道门)。
        """
        row = self._level_row(map_id)
        if row is None:
            return 0
        minimum = int(row.min_team_size)
        if minimum <= 0:
            return 0
        maximum = self.team_size_for_map(map_id)
        return min(minimum, maximum)

    def allowed_entry_modes(self, map_id: int) -> int:
        """这张图**允许哪些进法**:关卡表 entry_mode 为唯一事实源。

        未配置(UNSPECIFIED / 表未启用 / 行不存在)时沿用本部署的 walk_in 开关。
        返回值恒是三者之一:MATCHMAKE / WALK_IN / BOTH。

        为什么下沉到表:部署级开关只能表达「整个池要么全直进、要么全撮合」,而
        「多人撮合进副本」要求同一个 pve 池里有的副本直进、有的副本撮合(§17.1);
        BOTH 再进一步,要求**同一张图**两个入口共存 —— 那已经不是图的属性而是
        玩家的选择。
        """
        fallback = (
            levelpb.LEVEL_ENTRY_MODE_WALK_IN
            if self.cfg.walk_in
            else levelpb.LEVEL_ENTRY_MODE_MATCHMAKE
        )
        row = self._level_row(map_id)
        if row is None:
            return fallback
        mode = row.entry_mode
        if mode in (
            levelpb.LEVEL_ENTRY_MODE_WALK_IN,
            levelpb.LEVEL_ENTRY_MODE_MATCHMAKE,
            levelpb.LEVEL_ENTRY_MODE_BOTH,
        ):
            return mode
        return fallback

    def ready_mode_for_map(self, map_id: int) -> int:
        """准备模式:关卡表 ready_mode 为唯一事实源,折算成二者之一。

            PRE_READY     组队面板全员点准备才放行开局,撮合成功直接进场(不进确认期)
            POST_CONFIRM  开局无门槛,撮合成功后进确认期,全员点接受才拉 DS

        表未启用 / 行不存在 / 列留空一律折算成 POST_CONFIRM —— 与本列上线前的行为
        逐字节一致,所以新二进制 + 旧批次表不会改变任何图的行为(§9.21)。
        ★ **不设部署级 fallback 开关**:准备模式是图的产品属性,不是部署属性;
        按部署兜底会让同一张图在 stable / canary 上表现不同,那正是本列要消灭的东西。
        """
        row = self._level_row(map_id)
        if row is not None and row.ready_mode == levelpb.LEVEL_READY_MODE_PRE_READY:
            return levelpb.LEVEL_READY_MODE_PRE_READY
        return levelpb.LEVEL_READY_MODE_POST_CONFIRM

    def requires_pre_match_ready(self, map_id: int) -> bool:
        """ready_mode 的布尔投影,供 BeginTeamMatch 入参与确认期决策**共用同一判定**。

        两处分别判会出现「既要先准备、又要再接受」或「两道都没有」的错配。
        """
        return self.ready_mode_for_map(map_id) == levelpb.LEVEL_READY_MODE_PRE_READY

    def is_walk_in_map(self, map_id: int) -> bool:
        """某副本**在没有玩家选择时**是否走直进。只用于兜底:滚动升级期旧 matchmaker
        写下的、不带 entry_mode 的存量票据。

        本函数的目标不是"做个合理判断",而是**逐字节复刻旧二进制对同一张票会做的
        决定**:撮合循环虽是单写者,leader 仍会在新旧副本间交棒 —— 两边对同一张
        存量票的分流必须一致,否则票的命运取决于当时谁是 leader(§9.21)。
        所以这里对 BOTH 也必须回退 cfg.walk_in,不能自作聪明。
        """
        allowed = self.allowed_entry_modes(map_id)
        if allowed == levelpb.LEVEL_ENTRY_MODE_WALK_IN:
            return True
        if allowed == levelpb.LEVEL_ENTRY_MODE_MATCHMAKE:
            return False
        return self.cfg.walk_in  # BOTH:旧二进制眼里的未知值

    def is_walk_in_ticket(self, t: matchpb.MatchTicketStorageRecord) -> bool:
        """票上落定的 entry_mode 是权威;为空只可能是滚动升级期的存量票。

        存量票排空后本兜底不再有活路径,但**不删** —— 旧票据在 Redis 里无 TTL
        (非终态持久),删兜底等于让升级期的票永远分流不出去。
        """
        if t.entry_mode == levelpb.LEVEL_ENTRY_MODE_WALK_IN:
            return True
        if t.entry_mode == levelpb.LEVEL_ENTRY_MODE_MATCHMAKE:
            return False
        return self.is_walk_in_map(t.map_id)

    def resolve_entry_mode(self, map_id: int, choice: int) -> int:
        """把「关卡表允许什么」× 「玩家选什么」求交,落定本次进法。

        fail-closed 规则(§17.3 准入条件只有服务端一份权威判定):
          - 该图只允许一种进法:请求留空(老客户端)按那一种放行;显式选了别的即拒
          - 该图 BOTH:必须明确选一种,留空即拒 —— **不替玩家猜入口**。
            猜错的代价是玩家以为在排队实则已经单刷进本(或反之),而副本进去就消耗
            了次数 / CD,不是一个能靠重试挽回的错误
          - 请求填 BOTH 一律拒:它不是一种进法
        """
        allowed = self.allowed_entry_modes(map_id)
        if allowed == levelpb.LEVEL_ENTRY_MODE_BOTH:
            if choice in (
                levelpb.LEVEL_ENTRY_MODE_MATCHMAKE,
                levelpb.LEVEL_ENTRY_MODE_WALK_IN,
            ):
                return choice
            raise errcode.PandoraError(
                errcode.ErrMatchEntryModeDenied,
                "map %d allows both matchmake and walk-in, request must pick one (got %d)",
                map_id,
                choice,
            )
        if choice == levelpb.LEVEL_ENTRY_MODE_UNSPECIFIED or choice == allowed:
            return allowed
        raise errcode.PandoraError(
            errcode.ErrMatchEntryModeDenied,
            "map %d only allows entry mode %d, request asked for %d",
            map_id,
            allowed,
            choice,
        )

    # ── 通用小工具 ──────────────────────────────────────────────────────────

    def ticket_ttl_sec(self) -> float:
        return self.cfg.ticket_ttl_td().total_seconds()

    def match_ttl_sec(self) -> float:
        return self.cfg.match_ttl_td().total_seconds()

    def require_local_game_mode(self, stored: str) -> None:
        """防止被路由到默认 PVP 实例的冷客户端去改一张 canonical PVE 票据。

        空值**只**为滚动升级期的旧记录放行(那时还没有 game_mode 字段);
        每个新写者都会持久化 canonical 命名空间。
        不校验的后果:PVE 票据被 PVP 实例改写,写进错误的队列 / 活跃索引 ——
        玩家排在一个池里却被另一个池撮合,而且**不报错**。
        """
        presence_gate.require_local_game_mode(stored, self.cfg.game_mode)

    async def _remove_active(self, match_id: int) -> None:
        try:
            await self.repo.remove_active(match_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning("remove_active_failed", match_id=match_id, err=str(exc))

    # ── 前置闸 ──────────────────────────────────────────────────────────────

    async def ensure_none_in_battle(self, members) -> None:  # noqa: ANN001
        """拦截"战斗中还点匹配":任一成员正处于 BATTLE 则拒绝整队入队。

        权威来源是 player_locator(不变量 §1)。处理规则:
          - locator 未注入 → 跳过(本机不起 player_locator 的骨架联调路径)
          - 明确查到 state==BATTLE → ErrMatchInBattle
          - 查询失败 → 默认 **fail-closed**:拒绝入队并返回 ErrUnavailable 让客户端
            重试。避免 locator 短暂抖动叠加旧 claim 释放时,把战斗中玩家二次塞进队列。
            仅当显式 battle_gate_fail_open=true(dev)才降级为 Warn 后放行。
        """
        if self.locator is None:
            return
        for m in members:
            try:
                in_battle = await self.locator.is_in_battle(m.player_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                if self.cfg.battle_gate_fail_open:
                    plog.get().warning(
                        "locator_is_in_battle_failed_fail_open",
                        reason="battle_gate_locator_unavailable",
                        fail_open=True,
                        player_id=m.player_id,
                        err=str(exc),
                    )
                    continue
                plog.get().error(
                    "locator_is_in_battle_failed_fail_closed",
                    reason="battle_gate_locator_unavailable",
                    fail_open=False,
                    player_id=m.player_id,
                    err=str(exc),
                )
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "locator unavailable, cannot verify battle state for player %d: %s",
                    m.player_id,
                    exc,
                ) from exc
            if in_battle:
                plog.get().warning(
                    "match_start_member_in_battle",
                    reason="member_in_battle",
                    player_id=m.player_id,
                    team_id=m.team_id,
                )
                raise errcode.PandoraError(
                    errcode.ErrMatchInBattle, "player %d in battle", m.player_id
                )

    async def absent_beyond(
        self, ids: list[int], window_ms: int
    ) -> tuple[list[int], int, str]:
        """按「离开了多久」找出这批玩家里已离场超过 window 的人。

        返回 (缺席名单, 最长缺席 ms, 失败的那一跳)。查询失败**抛异常**,
        由调用方按各自的依赖故障策略收口(入队闸 fail-closed / 回收路径弱依赖跳过)。
        稳态下全员在线,第二跳 batch_last_seen 完全省掉。
        """
        try:
            online = await self.presence.batch_online(ids)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise _PresenceProbeFailed("batch_online", exc) from exc
        absent = [pid for pid in ids if not online.get(pid)]
        if not absent:
            return [], 0, ""
        try:
            last_seen = await self.presence.batch_last_seen(absent)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise _PresenceProbeFailed("batch_last_seen", exc) from exc
        offline, longest = presence_gate.absent_beyond(absent, last_seen, window_ms, now_ms())
        return offline, longest, ""

    async def ensure_all_present(self, members) -> None:  # noqa: ANN001
        """在线闸:拦截「把已经离开大厅的队友冻进对局票据」(INC-20260813-001)。

        判据刻意**不是**「locator 里查不查得到」:位置投影在正常路径上就会短暂缺席
        (撮合失败后停在 MATCHING 而 MATCHING 无保活;切线换 Hub 的换手窗口)。
        INC-20260724-001 的成局最终门正是按缺席判死,结果是结构上 100% 假阳性。
        本闸因此按「离开了多久」判,UNKNOWN 一律放行(§9.22)。

        位置刻意夹在 resolve_members **之后**、claim 预检**之前**:
          - 之后 —— 要查的是 BeginTeamMatch **冻结后**的那份名单,与最终写进票据的
            是同一批人;放在冻结之前查等于查了一份可能已经变了的名单
          - 之前 —— claim 是有副作用的写(SETNX 占坑),能在无副作用的读闸上拒掉的,
            就不要先去占坑再回滚
        """
        grace_ms = int(self.cfg.start_presence_grace_td().total_seconds() * 1000)
        if self.presence is None or grace_ms <= 0 or not members:
            return
        ids = [m.player_id for m in members if m.player_id != 0]
        if not ids:
            return
        try:
            offline, longest_ms, _ = await self.absent_beyond(ids, grace_ms)
        except _PresenceProbeFailed as probe:
            presence_gate.presence_gate_unavailable(
                probe.op, probe.cause, fail_open=self.cfg.battle_gate_fail_open
            )
            return
        if not offline:
            return
        plog.get().warning(
            "match_start_member_offline",
            reason="member_absent_beyond_grace",
            offline_players=offline,
            members=len(ids),
            grace_ms=grace_ms,
            longest_absent_ms=longest_ms,
        )
        # 缺席名单走**结构化通道**过线(StartMatchResponse.absent_player_ids):
        # 光靠 error 文本客户端点不了名,队长只能看见一句「有队员不在大厅」
        # 却不知道该等谁。
        raise presence_gate.MemberOfflineError(offline, grace_ms / 1000)

    async def check_no_show_penalty(self, members) -> None:  # noqa: ANN001
        """逐成员读 no-show 退避窗;任一成员在罚 → ErrRateLimited(带可见的剩余秒数)。

        读失败整批 fail-open:惩罚是背压不是不变量,读不到就别拦人(但要留证)。
        """
        if self.entry_limiter is None:
            return
        for m in members:
            remain, err = await self.entry_limiter.no_show_penalty_remaining(m.player_id)
            if err is not None:
                plog.get().warning(
                    "noshow_penalty_check_failed",
                    reason="noshow_probe_failed",
                    fail_open=True,
                    player_id=m.player_id,
                    err=str(err),
                )
                return
            if remain > 0:
                retry_sec = int(remain) + (1 if remain % 1 else 0)
                plog.get().warning(
                    "noshow_penalty_rejected",
                    reason="no_show_backoff_active",
                    player_id=m.player_id,
                    retry_after_sec=retry_sec,
                )
                raise errcode.PandoraError(
                    errcode.ErrRateLimited,
                    "player %d no-show cooldown, retry after %ds",
                    m.player_id,
                    retry_sec,
                )

    async def try_start_cooldown(self, captain_id: int, team_id: int) -> bool:
        """占用 StartMatch 冷却窗。limiter 未注入 / 窗口 <=0 / Redis 故障均放行。"""
        if self.entry_limiter is None:
            return True
        window = self.cfg.start_match_cooldown_td().total_seconds()
        if window <= 0:
            return True
        ok, err = await self.entry_limiter.try_start_cooldown(captain_id, team_id, window)
        if err is not None:
            plog.get().warning(
                "start_cooldown_check_failed",
                reason="cooldown_probe_failed",
                fail_open=True,
                captain_id=captain_id,
                team_id=team_id,
                err=str(err),
            )
            return True
        if not ok:
            # 拒绝走日志不走 metrics 定位到玩家(§4.4:player_id 绝不能做 label)。
            plog.get().warning(
                "start_cooldown_rejected",
                reason="start_cooldown_window",
                captain_id=captain_id,
                team_id=team_id,
                window_sec=window,
            )
        return ok

    async def release_start_cooldown(self, captain_id: int, team_id: int) -> None:
        """业务失败路径释放冷却。best-effort:最坏后果是玩家多等一个冷却窗(秒级)。"""
        if self.entry_limiter is None or self.cfg.start_match_cooldown_td().total_seconds() <= 0:
            return
        try:
            await self.entry_limiter.clear_start_cooldown(captain_id, team_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "start_cooldown_release_failed",
                captain_id=captain_id,
                team_id=team_id,
                err=str(exc),
            )

    async def preflight_start_claims(self, members) -> None:  # noqa: ANN001
        """提前拒绝明确的 live claim,并 CAS 清掉明确不存在票据的僵尸 claim。

        这只是**友好错误的快照检查**;真正的一人一票线性化点仍是 durable worker
        的 SETNX。所以这里的任何"读失败"都必须如实上抛(不能 fail-open):
        读不到就放行会让两批玩家同时通过预检,把冲突推到 worker 的补偿路径上,
        白白多一次成员被 claim 又回滚的抖动。
        """
        for member in members:
            pid = member.player_id
            start_ticket_id, start_found = await self.repo.get_start_player_operation(pid)
            if start_found:
                op, found = await self.repo.get_start_operation(start_ticket_id)
                if found and not h.start_operation_terminal(op.phase):
                    plog.get().warning(
                        "match_start_claim_conflict",
                        reason="start_operation_in_flight",
                        player_id=pid,
                        ticket_id=start_ticket_id,
                        phase=int(op.phase),
                    )
                    raise errcode.PandoraError(
                        errcode.ErrMatchAlreadyMatching,
                        "player %d already has start operation %d",
                        pid,
                        start_ticket_id,
                    )
                await self.repo.delete_start_player_if_matches(pid, start_ticket_id)

            ticket_id, found = await self.repo.get_player_ticket(pid)
            if not found:
                continue
            _, ticket_found = await self.repo.get_ticket(ticket_id)
            if ticket_found:
                plog.get().warning(
                    "match_start_claim_conflict",
                    reason="player_already_matching",
                    player_id=pid,
                    ticket_id=ticket_id,
                )
                raise errcode.PandoraError(
                    errcode.ErrMatchAlreadyMatching, "player %d already matching", pid
                )
            # claim 指向一张已不存在的票据 = 僵尸。CAS 清(不是无条件 DEL):
            # 读到旧 claim 与删之间玩家可能已经开了新一局。
            await self.repo.delete_player_index_if_matches(pid, ticket_id)

    # ── RPC 1:StartMatch ───────────────────────────────────────────────────

    async def start_match(
        self, ticket_id: int, team_id: int, captain_id: int, map_id: int, entry_choice: int
    ) -> int:
        """把 team(或单人)作为一张票据受理入队。ticket_id 由 service 层 snowflake 生成。

        返回的 ticket_id 同时作为客户端 QUEUEING 阶段的句柄。
        """
        # 关卡表准入门(不变量 §9.15 接线)。
        try:
            self.validate_map_id(map_id)
        except errcode.PandoraError as exc:
            plog.get().warning(
                "match_start_rejected",
                gate="validate_map",
                reason=self.map_reject_reason(map_id),
                code=errcode.as_code(exc),
                ticket_id=ticket_id,
                team_id=team_id,
                captain_id=captain_id,
                map_id=map_id,
                game_mode=self.cfg.game_mode,
                err=str(exc),
            )
            raise

        # 队列准入上限(§9.18 精神):撮合循环每 tick 全量处理 queue,无准入时突发
        # 入队会把 tick 拖过 match_interval 形成正反馈雪崩。软上限即可(并发窗口内
        # 少量超入无害,这是背压不是不变量);长度查询失败放行 —— 权威写也在同一
        # Redis,真故障会在 create_start_operation 如实失败,不需要在这里预判。
        if self.cfg.max_queue_tickets > 0:
            try:
                qlen = await self.repo.queue_len()
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "match_queue_len_check_failed",
                    reason="queue_len_probe_failed",
                    team_id=team_id,
                    captain_id=captain_id,
                    err=str(exc),
                )
            else:
                if qlen >= self.cfg.max_queue_tickets:
                    plog.get().warning(
                        "match_queue_admission_rejected",
                        reason="queue_full",
                        queue_len=qlen,
                        max=self.cfg.max_queue_tickets,
                        team_id=team_id,
                        captain_id=captain_id,
                        map_id=map_id,
                        game_mode=self.cfg.game_mode,
                    )
                    raise errcode.PandoraError(
                        errcode.ErrRateLimited, "match queue is full (%d), retry later", qlen
                    )

        # per-队长冷却(§6 第 2 项):挡外挂以 RPC 极限速率反复 StartMatch↔Cancel。
        # 占窗在一切副作用之前;之后任何失败都释放冷却,玩家可立即重试。
        if not await self.try_start_cooldown(captain_id, team_id):
            raise errcode.PandoraError(
                errcode.ErrRateLimited,
                "start match cooldown, retry in %ss",
                self.cfg.start_match_cooldown_td().total_seconds(),
            )
        try:
            return await self._start_match_admitted(
                ticket_id, team_id, captain_id, map_id, entry_choice
            )
        except BaseException:
            # 先占坑 → 干活 → 失败释放(§9.20):冷却只约束成功受理的频率,
            # 业务失败不得让玩家白等一个冷却窗。
            await self.release_start_cooldown(captain_id, team_id)
            raise

    async def _start_match_admitted(
        self, ticket_id: int, team_id: int, captain_id: int, map_id: int, entry_choice: int
    ) -> int:
        """过冷却门之后的主体。每一道门的拒绝都必须留证。

        此前 Go 侧所有失败分支都是裸 return,一行日志都不打 —— 服务端因此完全看不见
        「谁在什么时候被哪道门拒了」,INC-20260813-001 排查时只能靠 envoy 访问日志的
        响应体字节数反推(成功 49B / 只回 code 43B)。
        """

        def reject(gate: str, reason: str, exc: BaseException) -> None:
            plog.get().warning(
                "match_start_rejected",
                gate=gate,
                reason=reason,
                code=errcode.as_code(exc),
                ticket_id=ticket_id,
                team_id=team_id,
                captain_id=captain_id,
                map_id=map_id,
                game_mode=self.cfg.game_mode,
                entry_choice=entry_choice,
                err=str(exc),
            )

        # 进法先落定:后续的下限判定、票据落库、撮合分流全用这一个结果,不各自再解析一遍。
        try:
            entry_mode = self.resolve_entry_mode(map_id, entry_choice)
        except errcode.PandoraError as exc:
            reason = (
                "entry_mode_choice_required"
                if self.allowed_entry_modes(map_id) == levelpb.LEVEL_ENTRY_MODE_BOTH
                else "entry_mode_not_allowed"
            )
            reject("entry_mode", reason, exc)
            raise

        try:
            members, avg_mmr = await self.resolve_members(team_id, captain_id, map_id)
        except errcode.PandoraError as exc:
            reject("resolve_members", "roster_unavailable", exc)
            raise

        # 直进人数下限(关卡表 min_team_size)。刻意用 len(members) 判而不是写在
        # resolve_members 的组队分支后面:teamID==0(单人入口)直接返回单人名单、
        # 不走任何人数校验,下限若写在那个分支后面,"不组队直接点进"就能整条绕过去。
        # 撮合入口不判本闸:它的目标恒是凑满 team_size(≥ min,加载期已校验)。
        if entry_mode == levelpb.LEVEL_ENTRY_MODE_WALK_IN:
            minimum = self.min_team_size_for_map(map_id)
            if len(members) < minimum:
                exc = errcode.PandoraError(
                    errcode.ErrMatchTeamTooSmall,
                    "map %d requires at least %d players to walk in, got %d",
                    map_id,
                    minimum,
                    len(members),
                )
                reject("min_team_size", "walk_in_below_min_team_size", exc)
                raise exc

        try:
            await self.check_no_show_penalty(members)
        except errcode.PandoraError as exc:
            reject("no_show_penalty", "no_show_backoff_active", exc)
            raise

        # 战斗中玩家不得入队。claim 预检只拦"已在撮合链路里"的玩家;若上一局已
        # ReleaseMatch 但玩家仍在 DS 内(或 GM 拉入),唯一能拦住的是 locator BATTLE 门。
        try:
            await self.ensure_none_in_battle(members)
        except errcode.PandoraError as exc:
            reason = (
                "battle_gate_locator_unavailable"
                if errcode.as_code(exc) == errcode.ErrUnavailable
                else "member_in_battle"
            )
            reject("in_battle", reason, exc)
            raise

        try:
            await self.ensure_all_present(members)
        except errcode.PandoraError as exc:
            reason = (
                "presence_gate_locator_unavailable"
                if errcode.as_code(exc) == errcode.ErrUnavailable
                else "member_absent_beyond_grace"
            )
            reject("member_offline", reason, exc)
            raise

        try:
            await self.preflight_start_claims(members)
        except errcode.PandoraError as exc:
            reason = (
                "player_already_matching"
                if errcode.as_code(exc) == errcode.ErrMatchAlreadyMatching
                else "claim_probe_failed"
            )
            reject("start_claim", reason, exc)
            raise

        created = now_ms()
        op = matchpb.MatchStartOperationStorageRecord(
            operation_id=new_operation_id(),
            ticket_id=ticket_id,
            team_id=team_id,
            captain_id=captain_id,
            members=members,
            avg_mmr=avg_mmr,
            map_id=map_id,
            phase=matchpb.MATCH_START_PHASE_ACCEPTED,
            next_attempt_at_ms=created,
            created_at_ms=created,
            game_mode=self.cfg.game_mode,
            entry_mode=entry_mode,
        )
        # RPC 的**唯一提交点**是 durable operation。票据主体 → 成员 compare-claim →
        # queue ZADD 由服务生命周期 worker 推进;玩家断线、RPC 取消或进程重启都不会
        # 中断 saga(§9.19/§9.23)。
        try:
            await self.repo.create_start_operation(op)
        except errcode.PandoraError as exc:
            reject("create_start_operation", "commit_failed", exc)
            raise

        # StartMatch 的线性化点(durable operation 已落库 = 已受理)。
        # 这条必须是 INFO:线上默认 info 级下若是 Debug,"玩家到底有没有入队成功"无从证明。
        plog.get().info(
            "match_start_accepted",
            ticket_id=ticket_id,
            operation_id=op.operation_id,
            team_id=team_id,
            captain_id=captain_id,
            members=len(members),
            member_ids=h.member_player_ids(members),
            avg_mmr=avg_mmr,
            map_id=map_id,
            game_mode=self.cfg.game_mode,
            entry_mode=int(entry_mode),
        )
        return ticket_id

    async def resolve_members(self, team_id: int, captain_id: int, map_id: int):  # noqa: ANN201
        """根据 team 快照构造成员列表 + 平均 MMR。

        team_id==0 = **单人入口**(单排撮合 / 单人直进副本):名单就是调用者本人,
        不查 team 服务。「单人」与「单人组队」在协议层是同一件事 —— 都是一张票据带
        1 个成员 —— 所以不该强迫玩家先去组一个 1 人队(那只是用组队机制模拟单人,
        多一次 RPC 和一个失败点)。captain_id 取自 JWT sub,客户端无法伪造他人身份。

        reader 为 None 时同样退化为单人票据(本机不起 team 的骨架联调路径;
        这条路必须由 allow_missing_team 显式声明,见 conf.validate_conf)。
        """
        if team_id == 0 or self.reader is None:
            members = [
                matchpb.MatchMemberStorageRecord(
                    player_id=captain_id, team_id=team_id, confirm=h.CONFIRM_PENDING
                )
            ]
            return members, 0

        # 在 team 的乐观锁内冻结名单(见 clients.GrpcTeamReader.begin_team_match)。
        # 队长 / 存在性校验都在那把锁里 —— 在这里再查一遍只会重新打开刚消灭的窗口。
        require_ready = self.requires_pre_match_ready(map_id)
        try:
            team, ready_gen = await self.reader.begin_team_match(
                team_id,
                captain_id,
                roster_lock_operation_id(team_id, captain_id),
                ROSTER_LOCK_LEASE_MS,
                require_ready,
            )
        except errcode.PandoraError as exc:
            plog.get().warning(
                "match_roster_freeze_failed",
                reason="team_begin_match_rejected",
                code=errcode.as_code(exc),
                team_id=team_id,
                captain_id=captain_id,
                map_id=map_id,
                require_ready=require_ready,
                err=str(exc),
            )
            raise

        team_size = self.team_size_for_map(map_id)
        if len(team.members) == 0 or len(team.members) > team_size:
            plog.get().warning(
                "match_roster_size_rejected",
                reason="team_size_out_of_range",
                team_id=team_id,
                captain_id=captain_id,
                members=len(team.members),
                map_id=map_id,
                team_size=team_size,
            )
            raise errcode.PandoraError(
                errcode.ErrMatchTeamNotReady,
                "team %d invalid size %d (map %d team_size %d)",
                team_id,
                len(team.members),
                map_id,
                team_size,
            )

        members = [
            matchpb.MatchMemberStorageRecord(
                player_id=tm.player_id,
                team_id=team_id,
                mmr=tm.mmr,
                hero_id=tm.hero_id,
                confirm=h.CONFIRM_PENDING,
                # 冻结名单那一刻的 team ready 代际;ReleaseMatch 回传给 EndTeamMatch
                # 做跨代 CAS(INC-20260813-001 ①)。
                team_ready_generation=ready_gen,
            )
            for tm in team.members
        ]
        avg = sum(tm.mmr for tm in team.members) // len(members)
        return members, avg

    # ── push 辅助 ───────────────────────────────────────────────────────────

    async def push_progress(
        self, match_id: int, stage: int, members, ds_addr: str, map_id: int
    ) -> None:  # noqa: ANN001
        """给 members 全体推同一阶段进度。

        ★ 原则 3 例外:caller_player_id 恒传 0 = 发给所有人**含发起方**。
        """
        if self.pusher is None or not members:
            return
        stamp = now_ms()
        for m in members:
            prog = h.build_progress(match_id, stage, list(members), ds_addr, "", map_id)
            await self._push_one_progress(m.player_id, prog, stamp)

    async def push_queueing_wait(
        self, ticket: matchpb.MatchTicketStorageRecord, wait_sec: int
    ) -> None:
        """推带 estimated_wait_seconds 的 QUEUEING(容量耗尽静默窗,§6 第 3 项)。

        句柄仍是 ticket_id;复用 MatchProgress 既有字段,不加 proto。
        """
        if self.pusher is None or not ticket.members:
            return
        stamp = now_ms()
        for m in ticket.members:
            prog = h.build_progress(
                ticket.ticket_id, h.STAGE_QUEUEING, list(ticket.members), "", "", ticket.map_id
            )
            prog.estimated_wait_seconds = wait_sec
            await self._push_one_progress(m.player_id, prog, stamp)

    async def _push_one_progress(
        self, player_id: int, prog: matchpb.MatchProgress, stamp_ms: int
    ) -> None:
        event = matchpb.MatchProgressEvent(
            progress=prog, to_player_id=player_id, ts_ms=stamp_ms
        )
        try:
            await self.pusher.push_match_progress(0, [player_id], event.SerializeToString())
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning("match_push_failed", to_player_id=player_id, err=str(exc))

    async def push_ready_strict(
        self, m: matchpb.MatchStorageRecord, ds_addr: str, tickets: dict[int, str]
    ) -> None:
        """给全体成员各推一条带其专属 battle_ticket 的 READY 进度,失败**抛出**。

        与 push_progress(fire-and-forget)不同:READY 是非队长成员进入 Battle 的
        关键通知,交付失败必须反馈给调用方以保留重试驱动(match 留在 active ZSET),
        不能静默丢弃。部分成功也算失败:下轮对全员重推,已收到的客户端按契约幂等
        忽略重复(§9.19)。pusher 未配置(dev 纯轮询模式)视为无需交付。
        """
        if self.pusher is None:
            return
        stamp = now_ms()
        failures: list[str] = []
        for member in m.members:
            prog = h.build_progress(
                m.match_id,
                m.stage,
                list(m.members),
                ds_addr,
                tickets.get(member.player_id, ""),
                m.map_id,
            )
            event = matchpb.MatchProgressEvent(
                progress=prog, to_player_id=member.player_id, ts_ms=stamp
            )
            try:
                await self.pusher.push_match_progress(
                    0, [member.player_id], event.SerializeToString()
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                failures.append(f"player {member.player_id}: {exc}")
        if failures:
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "push ready progress failed for match %d: %s",
                m.match_id,
                "; ".join(failures),
            )

    async def rollback_claims(self, ticket_id: int, player_ids: list[int]) -> None:
        """释放一批玩家的队列归属。CAS 删:仅当 claim 仍指向本票据才删,
        防在「旧 claim 被清 → 同一玩家新一局 claim 写入」窗口误删新 claim。"""
        for pid in player_ids:
            try:
                await self.repo.delete_player_index_if_matches(pid, ticket_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "rollback_claim_failed", player_id=pid, ticket_id=ticket_id, err=str(exc)
                )


class _PresenceProbeFailed(Exception):
    """presence 查询本身失败(区别于「查通了但这个人不在线」)。

    单独一个异常类型是为了让调用方能把**依赖故障**与**判定结果**分开处理:
    在线闸对前者按配置 fail-open/closed,对后者一律拒;离线回收对前者整轮跳过。
    合成一种的话,locator 抖一下就会被当成"所有人都离线"而清空整个队列。
    """

    def __init__(self, op: str, cause: BaseException) -> None:
        self.op = op
        self.cause = cause
        super().__init__(f"{op}: {cause}")


_MAX_TEAM_SIZE = 50


def _clamp_team_size(ts: int) -> int:
    """把一方人数钳到 [1, 50]。撮合按 need=side_count×team_size 组队,
    负值会让每张票都凑不满(队列永远不成局),巨值会 OOM。"""
    if ts < 1:
        return 1
    return min(ts, _MAX_TEAM_SIZE)
