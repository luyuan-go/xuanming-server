"""「玩家离线满 N 秒后做点什么」的通用消费骨架 —— 对应 Go 侧 pkg/offlinewatch。

# 为什么需要它

player_locator 能回答「此刻在不在线」,也能回答「最后一次被观测到离开是什么时候」,
但它不知道也不该知道各业务的阈值和动作(组队 180s 后退队)。于是每个想按离线时长
做决策的服务都要重复同一段:订事件 → 排到期 → 到点回查 → 动作。本模块把那段抽出来,
业务只实现 handler。

# 唯一权威在 locator,本模块只有可重建的调度状态

「玩家什么时候离开的」这个权威事实仍只有 locator 一份(§9.22 不重复影子状态)。
本模块只存可重建的复查状态:
  - due ZSET 是「下次该复查谁」;evidence HASH 只保存事件 / locator last-seen;
    **locator key miss 不能证明持续离线,不得用本地时钟补 evidence**;
  - 独立 key(`pandora:offlinewatch:{ns}:due|evidence`),不寄生在业务索引上(§16.10);
  - 丢了 / 清空了只会 fail-closed 延迟动作。

# 三段链路

    locator.ReportDisconnect ──kafka: pandora.player.presence──▶ Watcher.enqueue
                                                                      │ evidence + due 原子排期
                                                                      ▼
                                                Watcher.sweep(ticker,只取到期项,预算封顶)
                                                                      │ 回查 locator 权威
                                                                      ▼
                                  online→条件清理 / waiting→推迟 / offline→handler / error→退避

# 事件是加速器,不是唯一触发源

kafka 会丢,所以业务**必须**另有一条兜底:在自己本来就要读该实体的路径上调 observe。
Hub DS 整台挂掉时可能既没有事件也没有 last-seen;单纯 key miss 无法排除期间发生过
未观测重连,本模块会保持 UNKNOWN(不动作)。

★ 五段 Lua 与 Go 侧**逐字节相同**:两栈并存期同一个 due/evidence key 会被 Go 副本和
  Python 副本同时读写,脚本语义差一点点(比如少一次 ZSCORE 复核)就会出现
  「两边都以为自己 claim 到了」→ 同一玩家被摘两次 / 被重复推送。
"""

from __future__ import annotations

import asyncio

import dataclasses
import enum
import time
from typing import Protocol

from pandora.common.v1 import errcode_pb2
from pandora.locator.v1 import locator_pb2, locator_pb2_grpc

from pandorapy import kafka_topics, kafkax
from pandorapy import log as plog
from pandorapy import redisx


class DeferredError(Exception):
    """业务前置条件当前不满足,但任务**不能视为完成**。对应 Go 的 ErrDeferred。

    例:玩家所在队伍正被一场对局占住 —— 本轮不能摘人,对局结束后仍要继续复查。
    与普通 error 分开是为了不让「正常竞争」刷成故障告警。
    """


class Handler(Protocol):
    """业务侧必须实现的唯一东西。

    契约:
      - **必须幂等**(事件 at-least-once、多副本各扫一遍、兜底复查也会重复触发);
      - 返回正常 = 处理完成(该玩家出调度队列);
      - 抛异常 = 本次没处理成,骨架按退避重排;已知业务暂缓抛 DeferredError;
      - **业务判定「不需要处理」时应正常返回而不是抛异常**(例:玩家根本不在任何队伍里),
        否则会一直重试到保留期结束。
    """

    async def on_player_offline(self, player_id: int, offline_since_ms: int) -> None: ...


class PresenceLostHandler(Protocol):
    """Handler 的**可选**扩展:此刻查不到位置、且已有权威离开基线,但还没满阈值。

    分两档的理由:阈值那一档管**破坏性动作**(摘人),必须给足重连余量;
    这一档管**软化状态**(取消准备),玩家自己一点就能恢复。
    必须幂等且**自己判有没有变化** —— 本回调在离线期间每轮 observe 都会来一次。
    """

    async def on_player_presence_lost(self, player_id: int, since_ms: int) -> None: ...


class PresenceReader(Protocol):
    """骨架对 locator 的只读依赖。

    两个方法都必须严格区分「查到了」「没查到」「查不通」三态:
    整批失败一律抛异常,**绝不允许把查不通压成空 dict** —— 那等价于宣布全体离线。
    """

    async def batch_online(self, player_ids: list[int]) -> dict[int, bool]: ...
    async def batch_last_seen(self, player_ids: list[int]) -> dict[int, int]: ...


class RosterSource(Protocol):
    """**可选**的兜底候选来源:每轮 sweep 顺带拉一小批「本服务当前关心的玩家」。

    为什么必须有它(2026-08-12 实测发现的缺口):另外两条触发链在
    「整支队伍一起掉线」时**同时失效** —— Hub DS 整机崩溃不会发离场事件,
    也没有活人打开面板触发读路径兜底。
    """

    async def next_batch(self, limit: int) -> list[int]: ...


class Verdict(enum.IntEnum):
    """「这个玩家现在该不该按离线处理」的三态判定(外加不确定态)。

    ★ 零值刻意是 UNKNOWN 而不是 OFFLINE:任何忘记赋值 / 取不到 key 的路径,
    落到的都必须是「不动作」那一档。把「不确定」压成「离线」会让依赖不可用时
    全服玩家被批量判离线(§9.22 明令禁止不确定冒充默认状态)。
    """

    UNKNOWN = 0
    ONLINE = 1
    WAITING = 2
    OFFLINE = 3


def classify(
    now_ms: int,
    player_id: int,
    online: dict[int, bool],
    last_seen: dict[int, int],
    hint_ms: int,
    threshold_ms: int,
) -> tuple[Verdict, int]:
    """本模块的全部判定逻辑,刻意做成**无 I/O 的纯函数**。

    离线判定一旦写错就是「把在线玩家踢出队伍」这类不可逆后果,必须能被单测穷举覆盖,
    不能埋在网络调用中间。

    取 max(last_seen, hint) 而不是任选其一:两者都是「离开时刻」的观测,取**更晚**的
    意味着更晚才判超时 —— 偏保守。反过来取更早的,会让一次「离开→回来→再离开」里的
    旧时刻把新一轮在线期一笔勾销,直接判超时踢人。
    """
    if online.get(player_id):
        return Verdict.ONLINE, 0
    since_ms = hint_ms
    ms = last_seen.get(player_id)
    if ms is not None and ms > since_ms:
        since_ms = ms
    if since_ms <= 0:
        # 查不到位置,也拿不到任何「什么时候离开的」基线:Hub DS 整台挂掉 /
        # 时刻已超保留期 / 这个玩家本来就没上过线。一律 UNKNOWN,调用方 fail-closed。
        return Verdict.UNKNOWN, 0
    # 未来时刻(时钟回拨)按「刚离开」处理:elapsed 为负,自然落进 WAITING。
    if now_ms - since_ms >= threshold_ms:
        return Verdict.OFFLINE, since_ms
    return Verdict.WAITING, since_ms


# ── Lua(与 Go 侧逐字节相同,见模块头)────────────────────────────────────────

_UPSERT_EVIDENCE = redisx.LuaScript(
    name="offlinewatch_upsert_evidence",
    body="""
local member = ARGV[1]
local candidate = tonumber(ARGV[2])
local threshold = tonumber(ARGV[3])
local current = tonumber(redis.call('HGET', KEYS[2], member))
if (not current) or candidate > current then
  redis.call('HSET', KEYS[2], member, ARGV[2])
  redis.call('ZADD', KEYS[1], candidate + threshold, member)
  return candidate
end
if not redis.call('ZSCORE', KEYS[1], member) then
  redis.call('ZADD', KEYS[1], current + threshold, member)
end
return current
""",
)

_CLAIM = redisx.LuaScript(
    name="offlinewatch_claim",
    body="""
local member = ARGV[1]
local expected_due = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local claim_until = tonumber(ARGV[4])
local current_due = tonumber(redis.call('ZSCORE', KEYS[1], member))
if (not current_due) or current_due ~= expected_due or current_due > now then
  return {0, 0}
end
local evidence = tonumber(redis.call('HGET', KEYS[2], member))
if not evidence then
  redis.call('ZREM', KEYS[1], member)
  return {2, 0}
end
redis.call('ZADD', KEYS[1], claim_until, member)
return {1, evidence}
""",
)

_FINISH_IF_EVIDENCE = redisx.LuaScript(
    name="offlinewatch_finish_if_evidence",
    body="""
local member = ARGV[1]
local expected = ARGV[2]
local expected_due = ARGV[3]
local current = redis.call('HGET', KEYS[2], member)
if expected == '' then
  if current then return 0 end
else
  if (not current) or tonumber(current) ~= tonumber(expected) then return 0 end
end
if expected_due ~= '' then
  local current_due = tonumber(redis.call('ZSCORE', KEYS[1], member))
  if (not current_due) or current_due ~= tonumber(expected_due) then return 0 end
end
if current then redis.call('HDEL', KEYS[2], member) end
redis.call('ZREM', KEYS[1], member)
return 1
""",
)

_RETRY_IF_EVIDENCE = redisx.LuaScript(
    name="offlinewatch_retry_if_evidence",
    body="""
local member = ARGV[1]
local expected = tonumber(ARGV[2])
local expected_due = tonumber(ARGV[3])
local retry_at = tonumber(ARGV[4])
local current = tonumber(redis.call('HGET', KEYS[2], member))
if (not current) or current ~= expected then return 0 end
local current_due = tonumber(redis.call('ZSCORE', KEYS[1], member))
if (not current_due) or current_due ~= expected_due then return 0 end
redis.call('ZADD', KEYS[1], retry_at, member)
return 1
""",
)

_REMOVE_ALL = redisx.LuaScript(
    name="offlinewatch_remove_all",
    body="""
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
""",
)

# 离线时长超过 threshold 的该倍数仍被 handler 推迟 = 病理性推迟,升 WARN 点名。
# 正常推迟主因是「在对局中」,对局时长量级几十分钟,180s×10=30min 足以盖住正常局。
DEFERRED_STUCK_MULTIPLE = 10


@dataclasses.dataclass
class Options:
    """Watcher 配置。默认值与 Go 的 normalize() 逐条同(判据都是 `<= 0`)。"""

    namespace: str = ""
    # 必填(<=0 报错):这个值没有安全的默认,猜一个等于替业务决定什么时候踢人。
    threshold_sec: float = 0.0
    interval_sec: float = 0.0
    budget: int = 0
    batch_size: int = 0
    retry_backoff_sec: float = 0.0
    roster_scan_batch: int = 0
    attempt_timeout_sec: float = 0.0

    def normalize(self) -> None:
        if not self.namespace:
            raise ValueError("offlinewatch: namespace required")
        if self.threshold_sec <= 0:
            raise ValueError(
                f"offlinewatch: threshold must be > 0 (ns={self.namespace})"
            )
        if self.interval_sec <= 0:
            self.interval_sec = 15.0
        if self.budget <= 0:
            self.budget = 200
        if self.batch_size <= 0:
            self.batch_size = 500
        if self.retry_backoff_sec <= 0:
            self.retry_backoff_sec = self.interval_sec
        if self.attempt_timeout_sec <= 0:
            self.attempt_timeout_sec = 5.0
        if self.roster_scan_batch <= 0:
            self.roster_scan_batch = 2000


def _now_ms() -> int:
    return int(time.time() * 1000)


class Watcher:
    """骨架本体。构造不启动任何后台循环(`sweep` 交给 safego.loop 驱动)。"""

    __slots__ = (
        "_rdb",
        "_reader",
        "_handler",
        "_lost",
        "_opts",
        "_due_key",
        "_evidence_key",
        "_roster",
        "_now_ms",
    )

    def __init__(
        self,
        rdb,  # noqa: ANN001
        reader: PresenceReader,
        handler: Handler,
        opts: Options,
        *,
        now_ms=_now_ms,
    ) -> None:
        if rdb is None:
            raise ValueError("offlinewatch: redis client required")
        if reader is None:
            raise ValueError("offlinewatch: PresenceReader required")
        if handler is None:
            raise ValueError("offlinewatch: Handler required")
        opts.normalize()
        self._rdb = rdb
        self._reader = reader
        self._handler = handler
        # 业务没实现软化档 → 整条路径不存在,行为与该接口落地前一致。
        self._lost = handler if hasattr(handler, "on_player_presence_lost") else None
        self._opts = opts
        # hash tag 括住 namespace:整个队列固定落一个 slot,ZRANGEBYSCORE 在 Cluster 下
        # 才能工作;due + evidence 共用同一 tag,Lua 才能原子 claim / finish / retry。
        self._due_key = f"pandora:offlinewatch:{{{opts.namespace}}}:due"
        self._evidence_key = f"pandora:offlinewatch:{{{opts.namespace}}}:evidence"
        self._roster: RosterSource | None = None
        self._now_ms = now_ms

    @property
    def options(self) -> Options:
        return self._opts

    def set_roster_source(self, src: RosterSource | None) -> None:
        """注入兜底候选源。None = 不启用。

        刻意复用 sweep 已有的节拍而不是新起一个循环(§16.10:不得为此新建第二套
        timer 状态机);扫描量由 roster_scan_batch 封顶,与到期项共享同一轮预算。
        """
        self._roster = src

    # ── 排期 ────────────────────────────────────────────────────────────────

    async def enqueue(self, player_id: int, left_at_ms: int) -> None:
        """记录离场事件并排期(kafka 消费路径)。

        较旧 / 重复事件不得倒退 evidence,也不得覆盖正在处理的新一轮任务;
        这两个条件由同 slot 的一段 Lua 一次完成。
        """
        if player_id == 0:
            raise ValueError("offlinewatch: player_id must > 0")
        if left_at_ms <= 0:
            raise ValueError(
                f"offlinewatch: left_at_ms must > 0 (player={player_id})"
            )
        await self._upsert_evidence(player_id, left_at_ms)

    async def observe(self, player_ids: list[int]) -> None:
        """观测并排期一批玩家(**兜底路径**,不依赖 kafka 事件)。

        「判定 + 排期」完全封装在本方法内,调用方拿不到中间判定,因此新增业务只能走
        同一条安全路径。实际业务动作仍只在 sweep。

        Hub DS 整机挂掉时没有 ReportDisconnect,locator 也可能没有 last-seen。此时
        key miss 不能证明期间没发生过未被观测的重连,必须按 UNKNOWN fail-closed,
        不排破坏性任务。
        """
        ids = _dedupe(player_ids)
        if not ids:
            return
        for chunk in _chunks(ids, self._opts.batch_size):
            # 必须在读 locator **之前**抓 evidence 版本:若读取期间发生新离场事件,
            # 在线分支只能条件清旧版本,不能无条件删掉刚排上的新任务。
            snapshot = await self._read_evidence(chunk)
            online, last_seen = await self._read_presence(chunk)
            for pid in chunk:
                if online.get(pid):
                    expected = snapshot.get(pid)
                    await self._finish_observed_evidence(pid, expected)
                    continue
                since_ms = 0
                ms = last_seen.get(pid, 0)
                if ms > 0:
                    since_ms = ms
                    await self._upsert_evidence(pid, ms)
                elif pid in snapshot:
                    # 已有 evidence 只能来自先前的权威 last-seen / 离场事件。复用 upsert
                    # 顺带修复「evidence 在但 due 缺失」的调度索引漂移。
                    since_ms = snapshot[pid]
                    await self._upsert_evidence(pid, since_ms)
                else:
                    continue
                # 软化档落在这里而不是 sweep 的 waiting 分支:upsert 把 due 排在
                # 「离开时刻 + threshold」,一次正常离场在满阈值**之前根本不会被 sweep
                # 扫到** —— 挂在那里等于挂了个永不触发的钩子。
                await self._notify_presence_lost(pid, since_ms)

    async def _notify_presence_lost(self, player_id: int, since_ms: int) -> None:
        """best-effort:失败只记日志,绝不影响本轮其余玩家的观测与排期。

        破坏性那一档(on_player_offline)有自己的 due/claim/retry 闭环,不能让一个
        软化动作的失败把它拖住。
        """
        if self._lost is None or since_ms <= 0:
            return
        try:
            await self._lost.on_player_presence_lost(player_id, since_ms)
        except asyncio.CancelledError:
            raise
        except DeferredError as exc:
            plog.get().debug(
                "offlinewatch_presence_lost_deferred",
                namespace=self._opts.namespace,
                player_id=player_id,
                err=str(exc),
            )
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "offlinewatch_presence_lost_failed",
                namespace=self._opts.namespace,
                player_id=player_id,
                err=str(exc),
            )

    # ── 到期复查 ────────────────────────────────────────────────────────────

    async def sweep(self) -> None:  # noqa: C901 —— 与 Go 同为一条线性判定链
        """跑一轮到期复查。交给 `safego.loop` 按 interval 驱动;单测可直接调。"""
        logger = plog.get()
        now_ms = self._now_ms()
        opts = self._opts

        try:
            due = await self._rdb.zrangebyscore(
                self._due_key, "-inf", now_ms, start=0, num=opts.budget, withscores=True
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.warning(
                "offlinewatch_due_scan_failed", namespace=opts.namespace, err=str(exc)
            )
            return

        if not due:
            # **队列空恰恰是缺口场景**:整队一起掉线时既没有 kafka 事件、也没人打开面板,
            # 到期项自然为空。兜底提名必须在这条分支也跑,否则那些玩家永远排不进来。
            await self._sweep_roster()
            return

        acted = online0 = waiting = deferred = failed = claimed = 0
        stuck_deferred: list[int] = []
        stuck_oldest_ms = 0

        for member, score in due:
            member_s = member.decode() if isinstance(member, bytes) else str(member)
            try:
                pid = int(member_s)
            except ValueError:
                pid = 0
            if pid == 0:
                await self._remove_all(member_s)
                continue

            expected_due = int(score)
            claim_now = self._now_ms()
            claim_until = claim_now + int(
                (opts.attempt_timeout_sec + opts.retry_backoff_sec) * 1000
            )
            try:
                status, evidence = await self._claim(
                    pid, expected_due, claim_now, claim_until
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.warning(
                    "offlinewatch_claim_failed",
                    namespace=opts.namespace,
                    player_id=pid,
                    err=str(exc),
                )
                continue
            # status=0:旧扫描结果,已被新事件 / 其他副本推进。
            # status=2:升级前旧 due 无 evidence,已 fail-closed 摘掉无依据提示。
            if status != 1:
                continue
            claimed += 1

            try:
                online, last_seen = await asyncio.wait_for(
                    self._read_presence([pid]), timeout=opts.attempt_timeout_sec
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                failed += 1
                await self._retry(
                    pid,
                    evidence,
                    claim_until,
                    self._now_ms() + int(opts.retry_backoff_sec * 1000),
                )
                logger.warning(
                    "offlinewatch_presence_unavailable",
                    namespace=opts.namespace,
                    player_id=pid,
                    err=str(exc),
                )
                continue

            # handler 前再读一次 locator,只负责挡住已经完成的重连并缩小窗口。它**不是**
            # 跨服务事务或线性化点;破坏性 handler 仍须在业务权威写路径自行 CAS/fence。
            if online.get(pid):
                online0 += 1
                await self._finish_claim(pid, evidence, claim_until)
                continue

            latest = last_seen.get(pid, 0)
            if latest > evidence:
                # locator 看到了更晚一轮离场。推进 evidence 交给下一轮,
                # 旧任务不能沿旧阈值动作。
                try:
                    await self._upsert_evidence(pid, latest)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    failed += 1
                    logger.warning(
                        "offlinewatch_evidence_advance_failed",
                        namespace=opts.namespace,
                        player_id=pid,
                        err=str(exc),
                    )
                continue

            verdict, since_ms = classify(
                self._now_ms(),
                pid,
                online,
                last_seen,
                evidence,
                int(opts.threshold_sec * 1000),
            )
            if verdict is Verdict.WAITING:
                waiting += 1
                await self._retry(
                    pid, evidence, claim_until, since_ms + int(opts.threshold_sec * 1000)
                )
            elif verdict is Verdict.OFFLINE:
                try:
                    await asyncio.wait_for(
                        self._handler.on_player_offline(pid, since_ms),
                        timeout=opts.attempt_timeout_sec,
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    await self._retry(
                        pid,
                        evidence,
                        claim_until,
                        self._now_ms() + int(opts.retry_backoff_sec * 1000),
                    )
                    if isinstance(exc, DeferredError):
                        deferred += 1
                        offline_ms = self._now_ms() - since_ms
                        if offline_ms >= DEFERRED_STUCK_MULTIPLE * int(
                            opts.threshold_sec * 1000
                        ):
                            stuck_deferred.append(pid)
                            stuck_oldest_ms = max(stuck_oldest_ms, offline_ms)
                        logger.debug(
                            "offlinewatch_handler_deferred",
                            namespace=opts.namespace,
                            player_id=pid,
                            err=str(exc),
                        )
                        continue
                    failed += 1
                    logger.warning(
                        "offlinewatch_handler_failed",
                        namespace=opts.namespace,
                        player_id=pid,
                        err=str(exc),
                    )
                    continue
                acted += 1
                await self._finish_claim(pid, evidence, claim_until)
            else:
                failed += 1
                # claim 保证 evidence>0,正常不应到这里。保留任务而不是把不确定压成完成。
                await self._retry(
                    pid,
                    evidence,
                    claim_until,
                    self._now_ms() + int(opts.retry_backoff_sec * 1000),
                )

        if stuck_deferred:
            logger.warning(
                "offlinewatch_deferred_stuck",
                namespace=opts.namespace,
                player_ids=stuck_deferred,
                oldest_offline_ms=stuck_oldest_ms,
                threshold_ms=int(opts.threshold_sec * 1000),
                hint="离线时长已超阈值多倍仍被 handler 推迟:查业务侧推迟原因"
                "(如 matchmaker 残留 claim)",
            )
        logger.info(
            "offlinewatch_swept",
            namespace=opts.namespace,
            scanned=len(due),
            claimed=claimed,
            acted=acted,
            online=online0,
            waiting=waiting,
            deferred=deferred,
            failed=failed,
            # 扫到预算上限说明还有积压,下轮继续;持续打满要么调大 budget 要么查下游。
            budget_saturated=len(due) >= opts.budget,
        )
        await self._sweep_roster()

    async def _sweep_roster(self) -> None:
        """每轮 sweep 末尾的兜底提名。全程 best-effort。"""
        if self._roster is None:
            return
        logger = plog.get()
        try:
            ids = await self._roster.next_batch(self._opts.roster_scan_batch)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.warning(
                "offlinewatch_roster_scan_failed",
                namespace=self._opts.namespace,
                err=str(exc),
            )
            return
        if not ids:
            return
        try:
            await self.observe(ids)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.warning(
                "offlinewatch_roster_observe_failed",
                namespace=self._opts.namespace,
                count=len(ids),
                err=str(exc),
            )
            return
        logger.debug(
            "offlinewatch_roster_observed",
            namespace=self._opts.namespace,
            count=len(ids),
        )

    # ── Redis 原语 ──────────────────────────────────────────────────────────

    async def _read_presence(
        self, ids: list[int]
    ) -> tuple[dict[int, bool], dict[int, int]]:
        """分批读 locator 的两份事实。任一批失败即整体失败(不返回半份结果)。"""
        online: dict[int, bool] = {}
        last_seen: dict[int, int] = {}
        for chunk in _chunks(ids, self._opts.batch_size):
            online.update(await self._reader.batch_online(chunk))
            last_seen.update(await self._reader.batch_last_seen(chunk))
        return online, last_seen

    async def _upsert_evidence(self, player_id: int, candidate_ms: int) -> int:
        if player_id == 0 or candidate_ms <= 0:
            raise ValueError(
                f"offlinewatch: invalid evidence player={player_id} since={candidate_ms}"
            )
        got = await _UPSERT_EVIDENCE(
            self._rdb,
            keys=[self._due_key, self._evidence_key],
            args=[str(player_id), candidate_ms, int(self._opts.threshold_sec * 1000)],
        )
        return _to_int(got)

    async def _read_evidence(self, player_ids: list[int]) -> dict[int, int]:
        """返回读取时存在且合法的 evidence。

        坏值按错误抛出,**不能把损坏静默压成「没有基线」** —— 那会让一个本该被复查的
        玩家永远排不进来,而日志里什么都看不到。
        """
        out: dict[int, int] = {}
        if not player_ids:
            return out
        fields = [str(pid) for pid in player_ids]
        values = await self._rdb.hmget(self._evidence_key, fields)
        for pid, raw in zip(player_ids, values, strict=True):
            if raw is None:
                continue
            ms = _to_int(raw)
            if ms <= 0:
                raise ValueError(
                    f"offlinewatch: bad evidence player={pid} value={raw!r}"
                )
            out[pid] = ms
        return out

    async def _claim(
        self, player_id: int, expected_due: int, now_ms: int, claim_until_ms: int
    ) -> tuple[int, int]:
        got = await _CLAIM(
            self._rdb,
            keys=[self._due_key, self._evidence_key],
            args=[str(player_id), expected_due, now_ms, claim_until_ms],
        )
        if not isinstance(got, (list, tuple)) or len(got) != 2:
            raise ValueError(f"offlinewatch: bad claim result {got!r}")
        return _to_int(got[0]), _to_int(got[1])

    async def _finish_observed_evidence(
        self, player_id: int, expected: int | None
    ) -> None:
        await _FINISH_IF_EVIDENCE(
            self._rdb,
            keys=[self._due_key, self._evidence_key],
            args=[str(player_id), "" if expected is None else str(expected), ""],
        )

    async def _finish_claim(
        self, player_id: int, expected_evidence: int, expected_claim_until: int
    ) -> None:
        try:
            await _FINISH_IF_EVIDENCE(
                self._rdb,
                keys=[self._due_key, self._evidence_key],
                args=[str(player_id), str(expected_evidence), str(expected_claim_until)],
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "offlinewatch_finish_failed",
                namespace=self._opts.namespace,
                player_id=player_id,
                err=str(exc),
            )

    async def _retry(
        self,
        player_id: int,
        expected_evidence: int,
        expected_claim_until: int,
        retry_at_ms: int,
    ) -> None:
        try:
            await _RETRY_IF_EVIDENCE(
                self._rdb,
                keys=[self._due_key, self._evidence_key],
                args=[
                    str(player_id),
                    expected_evidence,
                    expected_claim_until,
                    retry_at_ms,
                ],
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "offlinewatch_retry_schedule_failed",
                namespace=self._opts.namespace,
                player_id=player_id,
                err=str(exc),
            )

    async def _remove_all(self, member: str) -> None:
        try:
            await _REMOVE_ALL(
                self._rdb, keys=[self._due_key, self._evidence_key], args=[member]
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "offlinewatch_remove_failed",
                namespace=self._opts.namespace,
                member=member,
                err=str(exc),
            )

    # ── kafka 消费者 ────────────────────────────────────────────────────────

    def new_consumer(
        self, brokers: list[str], partition_count: int
    ) -> kafkax.KeyOrderedConsumer:
        """订阅 pandora.player.presence,把每条离场事件转成 enqueue。

        consumer group 用 namespace 隔离:每个业务各自消费全量事件,互不抢分区。
        解码失败按毒丸处理(格式坏了的消息重试多少次都还是坏的);enqueue 失败
        (Redis 抖动)按普通错误重试,彻底失败也只是丢一个加速信号,observe 兜底仍在。
        """

        async def _handle(msg) -> None:  # noqa: ANN001
            evt = locator_pb2.PlayerLeftHubEvent()
            try:
                evt.ParseFromString(bytes(msg.value))
            except Exception as exc:  # noqa: BLE001
                raise kafkax.poison(exc) from exc
            if evt.player_id == 0:
                raise kafkax.poison("offlinewatch: event without player_id")
            if evt.left_at_ms <= 0:
                # 没有权威离场时刻时不能用消费端 now 猜基线;这是永久坏消息。
                raise kafkax.poison(
                    f"offlinewatch: event without left_at_ms player={evt.player_id}"
                )
            await self.enqueue(evt.player_id, evt.left_at_ms)

        return kafkax.KeyOrderedConsumer(
            kafkax.ConsumerConf(
                brokers=tuple(brokers),
                topic=kafka_topics.TOPIC_PLAYER_PRESENCE,
                group_id="offlinewatch-" + self._opts.namespace,
                # enqueue 的 Redis 抖动是瞬时错误,不能像零值策略那样首次失败就丢事件。
                # 三次有限重试耗尽后由 observe 兜底,不能无限阻塞 Kafka 分区。
                retry=kafkax.RetryPolicy(max_retries=3, backoff_sec=0.2),
            ),
            _handle,
        )


class GrpcPresenceReader:
    """用 player_locator gRPC client 实现 PresenceReader。

    与 friend 服务那份 locator 客户端的关键区别:**这里绝不降级**。
    friend 查在线态是为了渲染面板,查不通全按离线显示最坏是列表灰一片;
    本模块的判定会导致「把玩家踢出队伍」,查不通必须原样抛错让上层 fail-closed
    (§9.22:不确定不得冒充 OFFLINE)。
    """

    __slots__ = ("_channel", "_stub", "_timeout_sec")

    def __init__(self, locator_addr: str, *, timeout_sec: float = 5.0) -> None:
        import grpc

        self._channel = grpc.aio.insecure_channel(locator_addr)
        self._stub = locator_pb2_grpc.PlayerLocatorServiceStub(self._channel)
        self._timeout_sec = timeout_sec

    async def close(self) -> None:
        await self._channel.close()

    async def batch_online(self, player_ids: list[int]) -> dict[int, bool]:
        """此刻在 locator 有位置记录的玩家集合。

        「在场即在线」:HUB / MATCHING / BATTLE / LOGIN_PENDING 都算。尤其是
        MATCHING / BATTLE —— 玩家 travel 去打比赛时位置正是这两个状态,那恰恰是
        **最不该**按离线处理的时刻。
        """
        out: dict[int, bool] = {}
        if not player_ids:
            return out
        resp = await self._stub.BatchGetLocation(
            locator_pb2.BatchGetLocationRequest(player_ids=player_ids),
            timeout=self._timeout_sec,
        )
        if resp.code != errcode_pb2.OK:
            raise RuntimeError(
                f"offlinewatch: locator BatchGetLocation code={int(resp.code)}"
            )
        for pid, loc in resp.locations.items():
            if loc.state not in (
                locator_pb2.LOCATION_STATE_OFFLINE,
                locator_pb2.LOCATION_STATE_UNSPECIFIED,
            ):
                out[pid] = True
        return out

    async def batch_last_seen(self, player_ids: list[int]) -> dict[int, int]:
        """「最后一次被观测到离开 Hub 的时刻」;缺席 = UNKNOWN(不是 0)。"""
        out: dict[int, int] = {}
        if not player_ids:
            return out
        resp = await self._stub.BatchGetLastSeen(
            locator_pb2.BatchGetLastSeenRequest(player_ids=player_ids),
            timeout=self._timeout_sec,
        )
        if resp.code != errcode_pb2.OK:
            raise RuntimeError(
                f"offlinewatch: locator BatchGetLastSeen code={int(resp.code)}"
            )
        for pid, ms in resp.last_seen_ms.items():
            out[pid] = ms
        return out


def _dedupe(ids) -> list[int]:  # noqa: ANN001
    """去重并剔除 0(保持首次出现顺序,便于测试断言稳定)。"""
    out: list[int] = []
    seen: set[int] = set()
    for i in ids:
        if i == 0 or i in seen:
            continue
        seen.add(i)
        out.append(i)
    return out


def _chunks(ids: list[int], size: int) -> list[list[int]]:
    if size <= 0 or len(ids) <= size:
        return [ids]
    return [ids[i : i + size] for i in range(0, len(ids), size)]


def _to_int(v) -> int:  # noqa: ANN001
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, bytes):
        return int(v.decode())
    if isinstance(v, str):
        return int(v)
    raise ValueError(f"offlinewatch: unexpected redis integer {v!r}")


__all__ = [
    "DeferredError",
    "GrpcPresenceReader",
    "Handler",
    "Options",
    "PresenceLostHandler",
    "PresenceReader",
    "RosterSource",
    "Verdict",
    "Watcher",
    "classify",
]
