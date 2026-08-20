"""push 业务层 —— 对应 Go 侧 internal/biz/push.go(539 行)。

这是整个服务里唯一「必须手写会话门」的地方,原因写在最前面:

★ `Subscribe` 是 **server stream**。grpcio 的 unary 拦截器(含
  `pandorapy.sessiongate.SessionCurrentInterceptor`)对它**一律不生效** ——
  拦截器返回的是 `grpc.unary_unary_rpc_method_handler`,流式 RPC 根本不走那条路。
  去 sessiongate 找中间件是找不到的(那个模块的头注释也写了这一条)。
  所以「建流门 + 流内看门狗 + 逐帧投递 fence」三处必须在本文件手写。
  漏掉的后果是 INC-20260722-004 的形状:顶号后旧设备的长连仍在收私有推送,
  且**每一帧看起来都是一次正常投递**。

三道会话闸,方向各不相同,不能合并:

  ① 建流门 `authorize_and_register`
     校验 jti == login 会话权威当前一代,**与注册在同玩家条带锁内原子完成**。
     分离执行存在 TOCTOU:旧会话校验通过后暂停 → 新会话注册 → 旧会话恢复再注册,
     反过来把新设备顶掉。
  ② 流内看门狗 `_session_watchdog`(30s)
     **独立协程**,不受写者阻塞影响。写者可能长时间卡在 `write`(慢客户端流控),
     把复查放进写者循环 = 阻塞期间无人裁决,「30s 内关旧流」不成立。
     连续 SESSION_FAIL_CLOSE 次权威不可达后 fail-closed 关流(短抖动不误杀)。
  ③ 逐帧投递 fence `_session_fence_delivery`
     ①②都只在**本 Pod** 内成立(条带锁是进程内的)。跨 Pod / 多副本时,
     Pod A 可能读到旧 jti 后暂停、B 在 Pod B 登录轮换并建流、A 恢复后继续投递。
     收口依据是 Redis 单 key 串行:轮换 = 对 `pandora:sess:<pid>` 的一次写,
     轮换后产生的帧其入缓冲写必然在轮换之后,能读到它的 Range 必然完成于之后,
     本 fence 在该帧 write 之前发起 → 必然读到新 jti 而拒绝。
     **诚实上界**:检查与 write 之间无法跨存储原子,在途暴露 ≤1 帧;
     "轮换瞬间起零帧"不可达,不作此宣称(与 Go 的 R6 复审措辞一致)。

裁决顺序有一处**必须先代际后到期**(Go R5 复审 P0-2):
  「已过期且已被顶」若先判到期只得到 ErrUnauthorized(→UNAUTHENTICATED),
  UE 会当自然过期用缓存凭据自动完整 Login,轮换 jti **反顶**新设备形成互踢循环。
  代际不匹配必须无条件优先返回 ErrSessionSuperseded(→ABORTED)。
"""

from __future__ import annotations

import asyncio
import time

from pandora.push.v1 import push_pb2

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import safego

# ── 与 Go 逐值对齐的常量(改任何一个都会改变对外承诺的窗口)────────────────
#
# 兜底轮询周期:只兜"写入落在其他 Pod"(滚动重叠 / 多副本)。本 Pod 写入走
# 唤醒信号零等待。40 万 CCU × 1/30s ≈ 1.3 万空读/s,容量可承受;
# 改成 1s 会变成 40 万空读/s —— Redis 直接被自己的兜底打垮(Go 审计 P1 原话)。
POLL_FALLBACK_SEC = 30.0
# 流内会话复查周期 = 顶号/登出/过期后「停止投递 + 发起关流」的最大暴露窗。
SESSION_RECHECK_SEC = 30.0
# 会话权威连续查询失败多少次后 fail-closed 关流。
# 1 会让 Redis 打个嗝就踢光全服长连;不设(无限重试)则权威长期故障时全服裸奔。
SESSION_FAIL_CLOSE = 3
# 拉取失败的最大退避。
DRAIN_BACKOFF_MAX_SEC = 60.0
# 「会话校验 + 注册」同玩家串行化条带锁数量。
AUTH_REG_STRIPES = 64

# 合成 resync 信号帧的 topic(R4 P1-3 gap 闭环)。**不是** kafka topic,
# 只存在于 Subscribe 下行:写者把缓冲拉空后发现客户端游标之后已有帧被修剪
# (LostSince > cursor)时发一条空 payload 帧(ts_ms=0,不推进客户端游标),
# 告知客户端「增量推送已有确定丢失,须回源拉取权威态」。
# 同一段丢失只信号一次;契约同步在 proto/pandora/push/v1/push.proto。
RESYNC_TOPIC = "pandora.push.resync"


def now_ms() -> int:
    return int(time.time() * 1000)


class SessionInfo:
    """建流时从 Envoy 验签 payload 头提取的会话身份(service 层注入)。"""

    __slots__ = ("jti", "exp_ms")

    def __init__(self, jti: str = "", exp_ms: int = 0) -> None:
        self.jti = jti          # 会话代际;"" = 未经网关(dev 直连)
        self.exp_ms = exp_ms    # 会话 JWT 到期毫秒;0 = 未携带


def drain_backoff_sec(streak: int) -> float:
    """按连败次数给退避时长(1s,2s,4s...封顶 60s)。与 Go 的 drainBackoff 同。"""
    shift = streak - 1
    if shift > 6:
        shift = 6
    if shift < 0:
        shift = 0
    return min(float(1 << shift), DRAIN_BACKOFF_MAX_SEC)


def is_session_fence_close(exc: BaseException | None) -> bool:
    """判定 drain 返回的错误是否为「会话已失效」类(不可恢复,必须关流)。

    与「拉取 / 权威瞬时失败」类(退避重试)区分 —— 方向写反的后果:
    把顶号当瞬时失败去退避重试 = 旧流保留了后续投递机会。
    """
    return isinstance(exc, errcode.PandoraError) and exc.code in (
        errcode.ErrSessionSuperseded,
        errcode.ErrUnauthorized,
    )


class PushUsecase:
    """push 用例。对应 Go 的 biz.PushUsecase。"""

    __slots__ = (
        "_conns",
        "_offline",
        "_session_gate",
        "_require_session",
        "_stripes",
        "_recheck_every",
    )

    def __init__(self, conns, offline) -> None:  # noqa: ANN001
        self._conns = conns
        self._offline = offline
        self._session_gate = None     # None = 未装配(dev 裸跑)
        self._require_session = False  # True = 生产档:无 jti / 无权威一律拒
        # 同玩家「校验 + 注册」串行化条带锁。用 64 条带而不是全局单锁:
        # 建流路径本就低频,但登录洪峰时全局锁会把所有玩家的建流串成一条队。
        self._stripes = [asyncio.Lock() for _ in range(AUTH_REG_STRIPES)]
        self._recheck_every = SESSION_RECHECK_SEC

    # ── 装配 ────────────────────────────────────────────────────────────
    def set_session_gate(self, gate, require: bool) -> None:  # noqa: ANN001
        self._session_gate = gate
        self._require_session = require

    def set_recheck_interval(self, sec: float) -> None:
        """仅供测试注入短周期(Go 的 sessionRecheckEvery 同)。"""
        self._recheck_every = sec

    @property
    def conns(self):
        return self._conns

    # ── 闸① 建流门(与注册原子)──────────────────────────────────────────
    async def authorize_and_register(self, player_id: int, sess: SessionInfo, write):  # noqa: ANN001
        """在同玩家条带锁内串行执行「会话门校验 → 注册连接」。

        锁保证同一玩家的校验与注册不可交错:
          - 旧 token 的校验若排在新会话注册**之后**,必然读到已轮换的 jti 而被拒;
          - 若排在**之前**,旧流短暂注册,但新会话随后注册时按顶号语义关掉它 ——
            新 token 的签发(login 轮换 jti)先于新连接建流,故新连接必过必成。
        两种交错都收敛到「新会话持有连接槽」。
        """
        lock = self._stripes[player_id % AUTH_REG_STRIPES]
        async with lock:
            await self.authorize_subscribe(player_id, sess)
            return self._conns.register(player_id, write)

    async def authorize_subscribe(self, player_id: int, sess: SessionInfo) -> None:
        """建流会话门(P0)。单独暴露仅供测试,生产路径必须走 authorize_and_register。

        JWT 验签只证明"曾经登录过";旧 / 被顶号 token 在 exp 前仍能过 Envoy
        jwt_authn,现行性只能问会话权威(§9.23)。
        """
        if player_id == 0:
            if self._require_session:
                raise errcode.PandoraError(
                    errcode.ErrUnauthorized, "subscribe requires authenticated player"
                )
            return  # dev 匿名直连(生产必经 Envoy jwt_authn,player_id 恒非 0)
        if self._session_gate is None:
            if self._require_session:
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "session authority not wired; subscribe rejected (fail-closed)",
                )
            return
        if not sess.jti:
            if self._require_session:
                # 生产必经 :8443 jwt_authn,payload 头必然存在;缺失 = 绕网关。
                raise errcode.PandoraError(errcode.ErrUnauthorized, "session payload required")
            return  # dev 直连内网端口联调
        cur, found = await self._session_gate.current_jti(player_id)  # 不可达 → 抛 ErrUnavailable
        if not found:
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "session expired or logged out; login again"
            )
        if cur != sess.jti:
            # 顶号用专属码(→ gRPC ABORTED):与自然过期/登出的 ErrUnauthorized 可判别,
            # 被顶设备不得自动完整 Login 反顶新设备(互踢循环)。
            plog.get().warning("push_subscribe_superseded_rejected", player_id=player_id)
            raise errcode.PandoraError(
                errcode.ErrSessionSuperseded, "session superseded by a newer login"
            )

    # ── 闸② 流内复查 ────────────────────────────────────────────────────
    async def recheck_session(
        self, player_id: int, sess: SessionInfo
    ) -> tuple[bool, BaseException | None]:
        """返回 (retryable, err)。err=None 表示会话仍现行。

        ★ 裁决顺序:**先判会话代际,后判到期**(见模块头注释的互踢循环)。
        """
        if self._session_gate is not None and player_id != 0 and sess.jti:
            try:
                cur, found = await self._session_gate.current_jti(player_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 权威不可达按可重试计连败
                return True, exc
            if not found:
                return False, errcode.PandoraError(
                    errcode.ErrUnauthorized, "session logged out; stream closed"
                )
            if cur != sess.jti:
                return False, errcode.PandoraError(
                    errcode.ErrSessionSuperseded, "session superseded; stream closed"
                )
        # 至此 jti 仍是当前一代(或 dev 无 gate / 无 jti,建流时已按 require 档裁决):
        # 到期按普通未授权关流,客户端自动换新会话不构成反顶。
        if sess.exp_ms > 0 and now_ms() >= sess.exp_ms:
            return False, errcode.PandoraError(
                errcode.ErrUnauthorized, "session token expired; stream closed"
            )
        return False, None

    # ── 闸③ 逐帧投递 fence ──────────────────────────────────────────────
    async def _session_fence_delivery(self, player_id: int, sess: SessionInfo) -> None:
        """每帧 write 之前复核会话现行性。抛异常 = 本帧不得投递。

        错误语义与 recheck_session 对齐:顶号 ErrSessionSuperseded / 登出
        ErrUnauthorized(不可恢复,调用方关流);权威不可达原样抛 ErrUnavailable
        (fail-closed 不投递,调用方按拉取失败退避,游标不动不漏帧)。
        """
        if self._session_gate is None or player_id == 0 or not sess.jti:
            return  # dev 裸跑 / 无 jti:建流时已按 require 档裁决
        cur, found = await self._session_gate.current_jti(player_id)  # 不可达 → 抛
        if not found:
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "session logged out; delivery fenced"
            )
        if cur != sess.jti:
            plog.get().warning("push_delivery_fenced_superseded", player_id=player_id)
            raise errcode.PandoraError(
                errcode.ErrSessionSuperseded, "session superseded; delivery fenced"
            )

    # ── 投递缓冲拉取 ────────────────────────────────────────────────────
    async def drain_buffer(
        self, slot, player_id: int, cursor: int, sess: SessionInfo
    ) -> tuple[int, BaseException | None]:
        """把缓冲中游标 > cursor 的帧全部投递,随后做 gap 终检。

        返回 (推进后的游标, 错误)。任何失败都**不推进游标**:下次重试不漏。

        gap 检查分两层(Go R7 复审 P1-1 收口):
          - **每页发送前预检**:resync 信号必须先于"越过缺口的帧"到达客户端。
            只在拉空后终检的话,多页补推期间客户端游标已被幸存帧推过缺口;
            若 resync 发出前断流重连,新流 last_seen_ms 已越过缺口,而服务端本地
            游标不持久化、重连后按客户端游标重建基线 —— fl 哨兵证据还在,却再也
            不会触发针对该缺口的信号 = permanent miss。
          - **拉空后终检**:兜住「最后一页预检后 ~ 拉空」间隙内被修剪而未投递的帧。

        检测失败必须返回错误,**不得当「无丢失」继续** —— 游标一旦越过缺口,
        resync 就永远无法触发。
        """
        entry = cursor
        # baseline 随已信号的丢失上界推进(同一段丢失只信号一次);
        # lostBound 记录本轮已信号丢失的最大上界,拉空后一次性推进游标。
        baseline = entry
        lost_bound = 0
        while True:
            if slot.closed.is_set():
                return cursor, None
            try:
                frames = await self._offline.range_after(player_id, cursor, now_ms())
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                return cursor, exc
            if not frames:
                break

            if baseline > 0:
                lost, err = await self._lost_since(player_id, baseline)
                if err is not None:
                    # 预检失败按拉取失败语义:fail-closed,游标不动,不越过潜在缺口。
                    return cursor, err
                if lost > baseline:
                    plog.get().warning(
                        "push_gap_resync_signaled",
                        player_id=player_id, baseline=baseline,
                        cursor=cursor, lost_up_to=lost,
                    )
                    serr = await self._send(slot, push_pb2.PushFrame(topic=RESYNC_TOPIC))
                    if serr is not None:
                        return cursor, serr
                    baseline = lost
                    lost_bound = max(lost_bound, lost)

            for item in frames:
                if slot.closed.is_set():
                    return cursor, None
                # 逐帧 fence:轮换发生在批内任意点,后续帧一律不发;已发帧均产生于
                # 各自 fence 通过之前。失败时游标停在最后一条已交付帧,不漏不重。
                try:
                    await self._session_fence_delivery(player_id, sess)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    return cursor, exc
                serr = await self._send(slot, item.frame)
                if serr is not None:
                    return cursor, serr
                cursor = item.cursor

            if baseline <= 0:
                # 首连:无增量历史,契约从首页交付上界开始,后续页起才做预检。
                baseline = cursor

        # 终检。基线用随信号推进的 baseline 而非拉空后的游标:丢失帧与幸存帧交错时
        # (丢 1001、幸存 1002),拉空后游标已被推到 1002,fl=1001 不再大于游标 ——
        # 丢失会被幸存帧"掩护"成永久漏报。
        # cursor=0 且缓冲无帧(baseline 仍 0)连检测都不做:新客户端无增量历史,
        # 交付契约从当下开始。该跳过依赖客户端时序契约(先订阅 push、后拉快照)。
        if baseline <= 0 or slot.closed.is_set():
            return cursor, None
        lost, err = await self._lost_since(player_id, baseline)
        if err is not None:
            return cursor, err
        if lost > baseline:
            plog.get().warning(
                "push_gap_resync_signaled",
                player_id=player_id, baseline=baseline, cursor=cursor, lost_up_to=lost,
            )
            serr = await self._send(slot, push_pb2.PushFrame(topic=RESYNC_TOPIC))
            if serr is not None:
                return cursor, serr
            lost_bound = max(lost_bound, lost)
        # 游标跳到已信号丢失上界:防止下一轮把同一段丢失再次当缺口信号。
        if lost_bound > cursor:
            cursor = lost_bound
        return cursor, None

    async def _lost_since(self, player_id: int, baseline: int) -> tuple[int, BaseException | None]:
        try:
            return await self._offline.lost_since(player_id, baseline, now_ms()), None
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return 0, exc

    @staticmethod
    async def _send(slot, frame) -> BaseException | None:  # noqa: ANN001
        """写一帧。返回异常而不是抛 —— 与 Go 的 `err := stream.Send(...)` 同形。"""
        try:
            await slot.write(frame)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return exc
        return None

    # ── stream 生命周期 ─────────────────────────────────────────────────
    async def run_subscribe_stream(
        self, slot, player_id: int, after_cursor: int, sess: SessionInfo
    ) -> BaseException | None:
        """跑一个 Subscribe stream 的生命周期(本协程 = 唯一写者)。

        返回关流原因(None = 正常结束);service 层负责映射成 gRPC status。
        after_cursor = 客户端 last_seen_ms(0 = 首连,从缓冲现存帧开始拉)。
        """
        logger = plog.get()
        started_at = time.monotonic()
        cursor = after_cursor

        watchdog = asyncio.create_task(
            self._session_watchdog(slot, player_id, sess), name="push_session_watchdog"
        )
        safego.supervise("push_session_watchdog", watchdog)

        def exit_(err: BaseException | None) -> BaseException | None:
            """关流日志的唯一收口 —— 与 push_stream_open 成对(每连接一条)。

            没有它的话流生命周期只有 open 单边可见,「玩家没收到通知」无法在 info
            级确认断流时刻,只能拿下一条 open 反推;Subscribe 不经 unary 中间件,
            access log 也兜不住。
            """
            reason = "client_disconnect"
            if slot.close_reason is not None:
                err = slot.close_reason
                reason = "session_closed"  # 顶号/登出/到期/权威 fail-closed
            elif err is not None:
                reason = "send_or_replay_failed"
            logger.info(
                "push_stream_closed",
                player_id=player_id, reason=reason,
                lived_ms=int((time.monotonic() - started_at) * 1000),
                cursor=cursor, err=str(err) if err is not None else "",
            )
            return err

        notify_task: asyncio.Task | None = None
        bcast_task: asyncio.Task | None = None
        closed_task = asyncio.create_task(slot.closed.wait())
        try:
            # 首轮拉取(重连补推 / 首连拉缓冲现存帧)。
            if player_id > 0:
                nxt, err = await self.drain_buffer(slot, player_id, cursor, sess)
                if err is not None:
                    logger.warning(
                        "push_replay_failed_stream_closed",
                        player_id=player_id, cursor=cursor, err=str(err),
                    )
                    return exit_(err)  # 首轮失败断流:客户端重连重试(游标没动,不漏)
                if nxt > cursor:
                    # INFO:补投是每次重连一次的低频事件,「重连丢通知」需要能**证实**
                    # 补投推进到了游标 X —— 只打失败的话这一步只能证伪。
                    logger.info(
                        "push_replayed",
                        player_id=player_id, after_cursor=after_cursor, cursor=nxt,
                    )
                cursor = nxt

            drain_fail_streak = 0
            drain_retry_at = 0.0
            while True:
                if notify_task is None:
                    notify_task = asyncio.create_task(slot.notify.wait())
                if bcast_task is None:
                    bcast_task = asyncio.create_task(slot.bcast.get())
                done, _ = await asyncio.wait(
                    {notify_task, bcast_task, closed_task},
                    timeout=POLL_FALLBACK_SEC,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if closed_task in done:
                    return exit_(None)

                pull = not done  # 空 done = 轮询周期到(兜底拉取)
                if notify_task in done:
                    slot.notify.clear()
                    notify_task = None
                    pull = True
                if bcast_task in done:
                    frame = bcast_task.result()
                    bcast_task = None
                    if slot.closed.is_set():
                        return exit_(None)  # 会话已失效/流已关:不得再投任何帧
                    serr = await self._send(slot, frame)
                    if serr is not None:
                        logger.warning(
                            "push_broadcast_send_failed", player_id=player_id, err=str(serr)
                        )
                        return exit_(serr)

                if pull and player_id > 0 and time.monotonic() > drain_retry_at:
                    nxt, err = await self.drain_buffer(slot, player_id, cursor, sess)
                    if err is not None:
                        if is_session_fence_close(err):
                            # 会话已失效(顶号/登出,闸③检出):立即关流,**不得退避重试**
                            # —— 旧流不允许保留任何后续投递机会。
                            logger.warning(
                                "push_stream_closed_by_delivery_fence",
                                player_id=player_id, err=str(err),
                            )
                            return exit_(err)
                        # 拉取 / gap 终检 / 权威瞬时失败不断流:游标未动,退避后重试
                        # (实时降级为轮询迟延)。只记首错与每 10 次,防 Redis 故障日志风暴。
                        drain_fail_streak += 1
                        drain_retry_at = time.monotonic() + drain_backoff_sec(drain_fail_streak)
                        if drain_fail_streak == 1 or drain_fail_streak % 10 == 0:
                            logger.warning(
                                "push_drain_failed_backoff",
                                player_id=player_id, cursor=cursor,
                                streak=drain_fail_streak, err=str(err),
                            )
                        continue
                    drain_fail_streak = 0
                    drain_retry_at = 0.0
                    cursor = nxt
        except asyncio.CancelledError:
            # 客户端断开 / 服务端停机:grpc.aio 用**取消**终止在途 handler。
            # 先补上关流日志(与 open 成对)再让取消穿透 —— 吞掉取消会让停机时
            # 的排空永远等不到这条流结束。
            exit_(None)
            raise
        finally:
            slot.cancel()  # 让看门狗退出
            for task in (notify_task, bcast_task, closed_task, watchdog):
                if task is not None:
                    task.cancel()
            # 等看门狗真正结束:不等的话它可能在请求生命周期之外还活着一小段,
            # 拿着已关闭的 slot 继续查会话权威(§16.7 协程不得逃逸出请求生命周期)。
            try:  # noqa: SIM105
                await watchdog
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass

    async def _session_watchdog(self, slot, player_id: int, sess: SessionInfo) -> None:
        """会话复查看门狗(闸②)。独立于写者协程 —— 理由见模块头注释。

        只读会话权威并置关流开关,**不碰 write**(单写者不变量保持)。
        """
        logger = plog.get()
        fails = 0
        try:
            while not slot.closed.is_set():
                try:
                    await asyncio.wait_for(slot.closed.wait(), timeout=self._recheck_every)
                    return  # 流已关,看门狗随之退出
                except asyncio.TimeoutError:
                    pass
                retryable, err = await self.recheck_session(player_id, sess)
                if err is None:
                    fails = 0
                    continue
                if retryable:
                    fails += 1
                    if fails < SESSION_FAIL_CLOSE:
                        continue
                    # 权威持续不可达:fail-closed 关流(重连时建流门同样 fail-closed,
                    # 客户端退避)。不允许在无法证明现行性的情况下长期裸奔。
                    logger.error(
                        "push_stream_session_authority_down_fail_closed",
                        player_id=player_id, fails=fails, err=str(err),
                    )
                else:
                    logger.warning(
                        "push_stream_session_closed", player_id=player_id, err=str(err)
                    )
                slot.close_reason = err
                slot.cancel()
                return
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 看门狗自身出错:Go 侧 recover 后 **主动断本流** —— fail-closed,
            # 不留"无人裁决会话"的流。客户端重连会重建看门狗。
            safego.recovered("push_session_watchdog", exc)
            slot.cancel()
