"""单写者租约 —— 对应 Go 侧 pkg/dsauthfence/writerlease。

与 `etcdleader` 的区别(两者都在选主,但保护的东西不同):

    etcdleader   保护「同一逻辑任务只跑一份」(撮合循环)
                 失去领导权 → 停任务,进程继续服务 RPC
    writerlease  保护「同一权威只有一个写者」(fence 水位推进)
                 失去持有 → **立刻停止对外宣告可写**,已在途的写必须被 fencing 拦下

★ 本模块有三处比普通选主更严的地方,每一处都对应一个静默故障:

════ ① 本地安全截止时间必须**早于**服务端 lease 过期 ════

    etcd 侧续租是周期性的,本地不可能精确知道服务端何时判定过期。
    宁可自己先停手,也不要在服务端已经把任期交给别人之后还认为自己持有。
    余量 3s 覆盖一次续租往返 + 时钟抖动;TTL 15s 时本地窗口 = 12s。

════ ② 激活钩子必须有**独立总期限** ════

    激活期间本副本**已当选并持有 etcd leader key**,却还没对外宣告持有
    (current() 仍返回不持有)。钩子若永久阻塞而不是返回错误:
        ① 本副本永远不可写;
        ② 它同时占着 leader key 不让位,其它副本的竞选全部排在后面
           → **整个集群进入无写者状态**;
        ③ 而失败计数器一次都不会 +1(计数只在 err 分支),degraded 恒为 false
           → **长期无主完全静默**。
    加期限后阻塞转成超时错误,走「让位 → 退避 → 重新竞选」,计数器才能动。

════ ③ 健康度必须可观测 ════

    两组计数分别定位「选不上」与「选上了但激活不过」—— 合成一个的话,
    运维看到 degraded 也不知道该查 etcd 还是查激活钩子。
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import time
from collections.abc import Awaitable, Callable

import aetcd

from pandorapy import log as plog
from pandorapy.etcdlease import LeaseGoneError, refresh_or_raise

DEFAULT_PREFIX = "/pandora/writerlease/"
DEFAULT_LEASE_TTL_SEC = 15
# 本地安全截止时间相对服务端 lease TTL 的提前量。见模块头 ①。
HOLD_SAFETY_MARGIN_SEC = 3
# 激活钩子的独立总期限 = 2× lease TTL。见模块头 ②。
DEFAULT_ACTIVATION_TIMEOUT_SEC = 30
# 失主 / 出错后重新竞选的退避。
RECAMPAIGN_BACKOFF_SEC = 2
# 连续失败达此次数后日志从 WARN 升 ERROR(无限重试不能 fail-silent)。
CAMPAIGN_ESCALATE_AFTER = 15


@dataclasses.dataclass(slots=True)
class HealthSnapshot:
    """竞选 / 激活健康度快照。★ 两组计数**分开**,见模块头 ③。"""

    held: bool = False
    token: int = 0
    consecutive_campaign_errs: int = 0
    last_campaign_err: str = ""
    consecutive_activation_errs: int = 0
    last_activation_err: str = ""
    escalate_after: int = CAMPAIGN_ESCALATE_AFTER

    def degraded(self) -> bool:
        """任一组计数达到阈值即降级 —— 长期无主必须可告警。"""
        return (
            self.consecutive_campaign_errs >= self.escalate_after
            or self.consecutive_activation_errs >= self.escalate_after
        )


# 向服务端要一次 TTL 证明的超时上限。对应 Go 的 ttlProofTimeout:
# 取续约间隔与 2s 的较小者 —— 证明本身不能比它要证明的窗口还慢。
TTL_PROOF_TIMEOUT_SEC = 2.0


@dataclasses.dataclass(slots=True)
class _HoldState:
    """一次**已激活任期**的持有态快照。对应 Go 的 holdState。

    ★ `valid_until` 只由 etcd 服务端的成功 TTL 应答推进,不由"配置里的 TTL"推进。
    ★ `self_fenced` 是本届任期的**单调终态**:任一处观察到本地安全截止已过,
      同一个 token 就永久不可再持有 —— 迟到的 TTL 证明只能促使让位,不能续活。
    """

    token: int
    valid_until: float = 0.0  # monotonic 秒
    self_fenced: bool = False


async def _remaining_ttl(client: aetcd.Client, lease_id: int) -> float:
    """向服务端查询本 lease 的剩余 TTL(秒)。对应 Go 的 Term.RemainingTTL。

    lease 已不存在时 etcd 回 TTL=-1(而不是报错),这里原样返回负数 ——
    调用方的 _apply_ttl_proof 会因窗口 <= 0 而拒绝开窗。
    """
    info = await client.get_lease_info(lease_id, keys=False)
    return float(getattr(info, "TTL", -1) if info is not None else -1)


def _apply_ttl_proof(hold: _HoldState, remaining_sec: float, proof_started_at: float) -> bool:
    """把一次服务端 TTL 证据应用到持有态。逐条对应 Go 的 applyTTLProof。

    返回 False = 本届必须让位(且已置单调终态)。四道判定缺一不可:

      ① 已自 fencing → 直接拒(迟到的证据不能续活旧任期)
      ② 剩余 - 安全余量 <= 0 → 没有足够证据继续写
      ③ 算出的截止线已经过去(证明本身太慢 / 进程被暂停过)→ 同样不算数
      ④ 写完之后**再查一次** —— 与 current() 的"观察越线即置位"是并发的,
         二次检查保证调用方看到 False 并立即让位,同一个 state 永不复活
    """
    if hold.self_fenced:
        return False
    window = remaining_sec - HOLD_SAFETY_MARGIN_SEC
    if window <= 0:
        hold.self_fenced = True
        return False
    deadline = proof_started_at + window
    if time.monotonic() >= deadline:
        hold.self_fenced = True
        return False
    hold.valid_until = deadline
    if hold.self_fenced or time.monotonic() >= deadline:
        hold.self_fenced = True
        return False
    return True


class WriterLease:
    """单写者租约。"""

    __slots__ = (
        "_endpoints",
        "_key_prefix",
        "_name",
        "_ttl",
        "_activation_timeout",
        "_health",
        "_hold",
        "_term",
    )

    def __init__(
        self,
        endpoints: list[str],
        name: str,
        *,
        prefix: str = DEFAULT_PREFIX,
        lease_ttl_sec: int = DEFAULT_LEASE_TTL_SEC,
        activation_timeout_sec: int = DEFAULT_ACTIVATION_TIMEOUT_SEC,
    ) -> None:
        if not endpoints:
            raise ValueError("writerlease: endpoints 不能为空")
        if not name:
            raise ValueError("writerlease: name 不能为空")
        self._endpoints = endpoints
        self._name = name
        self._key_prefix = f"{prefix}{name}/"
        self._ttl = lease_ttl_sec
        self._activation_timeout = activation_timeout_sec
        self._health = HealthSnapshot()
        # 本届任期的持有态。未持有时为 None。见 _HoldState。
        self._hold: _HoldState | None = None
        self._term = 0

    # ── 对外查询 ─────────────────────────────────────────────────────────

    def current(self) -> tuple[bool, int]:
        """当前是否持有写权、以及任期 token。

        ★ 自带**时间过期**:即使续约循环还没来得及标记失主,
        只要越过本地安全截止线,这里就返回不持有 ——
        调用方拿到的永远是"此刻可证明的"而不是"上次成功时的"。

        ★ 而且是**单调终态**:一旦这里观察到越线,本届任期就地自 fencing,
        之后哪怕来了一次成功的续约也不再复活(见 _HoldState.self_fenced)。
        """
        hold = self._hold
        if hold is None or hold.self_fenced:
            return False, 0
        if time.monotonic() >= hold.valid_until:
            # ★ 观察到越线即置位 —— 不能只是"这次返回 False,下次可能又 True"。
            hold.self_fenced = True
            return False, 0
        return True, hold.token

    def health(self) -> HealthSnapshot:
        snap = dataclasses.replace(self._health)
        snap.held, snap.token = self.current()
        return snap

    def local_hold_window_sec(self) -> float:
        """本地安全窗 = lease TTL - 余量。必须为正。"""
        return self._ttl - HOLD_SAFETY_MARGIN_SEC

    # ── 主循环 ───────────────────────────────────────────────────────────

    async def run(self, on_elected: Callable[[int], Awaitable[None]]) -> None:
        """竞选 → 激活 → 持有。阻塞直到被取消。

        on_elected 是激活钩子(hub_allocator 用它把全部已知 pod 的 fence 水位推一遍)。
        ★ 它有独立总期限,超时按失败处理 —— 见模块头 ②。
        """
        try:
            while True:
                try:
                    await self._one_term(on_elected)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    self._record_campaign_err(exc)
                await asyncio.sleep(RECAMPAIGN_BACKOFF_SEC)
        except asyncio.CancelledError:
            self._hold = None
            raise

    async def _one_term(self, on_elected) -> None:  # noqa: ANN001
        host, _, port = self._endpoints[0].rpartition(":")
        async with aetcd.Client(host=host or "127.0.0.1", port=int(port or 2379)) as client:
            lease = await client.lease(self._ttl)
            my_key = f"{self._key_prefix}{lease.id:x}".encode()
            try:
                succeeded, _ = await client.transaction(
                    compare=[client.transactions.version(my_key) == 0],
                    success=[client.transactions.put(my_key, b"w", lease=lease.id)],
                    failure=[],
                )
                if not succeeded:
                    return
                term = await self._first_revision(client, my_key)
                if term <= 0:
                    # 0 有两种来源:排队中(不是队首),或 etcd 给了个非法 revision。
                    # 两种都必须放弃本届 —— 0 是 source_revision 的 legacy 哨兵,
                    # 绝不能拿它冒充持有(那会让「不带版本」的写法绕过整道门)。
                    return

                # ★ 任期 token = **本届 leader key 的 CreateRevision**,不是 lease.id。
                #
                # 两者都"看起来像个唯一数",但只有前者满足 token 必须具备的两条性质:
                #   ① 历届严格递增(etcd revision 全局单调不回退)—— 这是 Chubby
                #      sequencer 语义,也是 pandorapy/source_revision.py 的全序前提;
                #      lease.id 高位含发号 member id,跨任期**不保证递增**。
                #   ② 位宽可编码 —— source_revision 用高 40 位装任期号,而实测
                #      lease.id 是 57 bit(如 112484531514171683),
                #      Minter().next(lease.id) 直接抛「超出编码上限」:一个号都铸不出来。
                # Go 侧取的是 election.Rev()(同样是 leader key 的 CreateRevision)。
                self._term = term

                # ★ 续约必须**从当选就开始跑**,而不是等激活完成(与 Go 的
                # concurrency.NewSession 自动续约对齐,2026-08-18 对照实验发现的差异)。
                #
                # 为什么不能"激活期间不续约、让 lease 自然过期兜底":
                #   hub_allocator 的激活是「把全部已知 pod 的 fence 水位推一遍」——
                #   大集群上本来就慢。中途丢租约会让这份工作**白做**,而且另一个副本
                #   立刻开始同样的激活 → 反复抢占churn,谁也完不成。
                #
                # 代价是:激活阻塞时 key 会被一直占着 —— 所以**激活超时是唯一的逃生口**,
                # 它不再是"更快的路径"而是"仅有的路径"(见下面的 wait_for)。
                keeper = asyncio.create_task(
                    self._keepalive(lease), name=f"writerlease-ka:{self._name}"
                )
                try:
                    # ★ 激活钩子:有界。超时 → 让位 → 退避重选(而不是永久占着 key)。
                    try:
                        await asyncio.wait_for(
                            on_elected(self._term), timeout=self._activation_timeout
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:  # noqa: BLE001
                        self._record_activation_err(exc)
                        plog.get().error(
                            "writerlease_activation_failed",
                            name=self._name,
                            term=self._term,
                            err=type(exc).__name__ + (f": {exc}" if str(exc) else ""),
                            hint="激活未在期限内完成 → 让位重选,防止占着 leader key 让全集群无写者",
                        )
                        return

                    # ★ 激活成功后**不能**直接拿"配置里的 TTL"开窗。
                    #
                    # 激活可能已经吃掉了大半个租期(甚至比一个续约间隔还短、
                    # 一次 refresh 都还没发生过),此刻最新的服务端证据可能还停在
                    # grant 那一刻。用 `now + (TTL - margin)` 开窗会让本地截止线
                    # **晚于**服务端 lease 真正过期 —— 与模块头 ① 的不等式正好相反。
                    #
                    # 所以先向服务端要一次 RemainingTTL,并以**请求发出前**的单调
                    # 时刻为锚点(响应回来后进程可能已被长暂停,用"现在"会把陈旧的
                    # TTL 凭空平移到未来,等于把旧任期重新打开)。
                    hold = _HoldState(token=self._term)
                    proof_started = time.monotonic()
                    try:
                        remaining = await asyncio.wait_for(
                            _remaining_ttl(client, lease.id), timeout=TTL_PROOF_TIMEOUT_SEC
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:  # noqa: BLE001
                        self._record_activation_err(exc)
                        plog.get().warning(
                            "writerlease_ttl_proof_failed",
                            name=self._name,
                            term=self._term,
                            err=str(exc),
                            hint="拿不到服务端 TTL 证据就不能宣告持有 —— 让位重选",
                        )
                        return
                    if not _apply_ttl_proof(hold, remaining, proof_started):
                        plog.get().warning(
                            "writerlease_ttl_proof_insufficient",
                            name=self._name,
                            term=self._term,
                            remaining_sec=remaining,
                            margin_sec=HOLD_SAFETY_MARGIN_SEC,
                            hint="剩余租期已不足安全余量,本届不宣告持有(激活吃掉了租期)",
                        )
                        return

                    self._health.consecutive_campaign_errs = 0
                    self._health.consecutive_activation_errs = 0
                    self._hold = hold
                    plog.get().info(
                        "writerlease_held",
                        name=self._name,
                        term=self._term,
                        ttl_sec=self._ttl,
                        remaining_sec=remaining,
                    )
                    await keeper
                finally:
                    keeper.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await keeper
            finally:
                self._hold = None
                with contextlib.suppress(Exception):
                    await client.revoke_lease(lease.id)

    async def _first_revision(self, client: aetcd.Client, my_key: bytes) -> int:
        """自己是队首则返回**本届 leader key 的 CreateRevision**,否则返回 0。

        ★ 返回值就是任期 token,不是布尔 —— 这个数必须带出来。见 _one_term 的说明。
        """
        entries = await client.get_prefix(self._key_prefix.encode())
        best = None
        mine = None
        for kv in entries:
            if kv.key == my_key:
                mine = kv.create_revision
            if best is None or kv.create_revision < best[0]:
                best = (kv.create_revision, kv.key)
        if mine is None or best is None or best[1] != my_key:
            return 0
        return int(mine)

    async def _keepalive(self, lease: aetcd.Lease) -> None:
        """续约循环 —— **从当选就开始跑**,贯穿激活期与持有期。

        两个阶段的区别只在 `self._hold`:
          激活期:续约照跑,但 `_hold` 仍为 None → current() 返回不持有
          持有期:`_hold` 已设,每次续约用**服务端回的新 TTL** 推进安全截止线

        ★ 推进截止线的唯一依据是**服务端应答里的 TTL**,不是"配置里的 TTL"。
        续约应答本身就是最新的服务端证据(refresh_or_raise 返回它),按**发起时刻**
        为锚点算(保守方向):服务端可能在请求刚到达时就重置了 TTL,按较早的时刻
        算不会高估剩余时间;而且响应回来前进程可能被长暂停,用"现在"会把陈旧的
        TTL 凭空平移到未来。
        """
        interval = self._ttl / 3
        # ★ 激活期也要有自己的安全窗,而不是"一次续约失败就作废整届"。
        #
        # 原先激活期的 except 分支直接 return —— 一次 etcd 瞬时抖动就把整届丢掉。
        # 那恰好推翻了本模块 ★ 注释的立论:hub_allocator 的激活是"把全部已知 pod 的
        # fence 水位推一遍",大集群上本来就慢;为了不让这份工作白做才让续约从当选就跑,
        # 结果又因为一次抖动把它整个作废,churn 一点没少。
        #
        # 与持有期同一条判据:**只有越过安全线才放弃**,窗内的失败继续重试。
        # 起点用 lease 刚 grant 的时刻(最保守),此后每次成功续约向后推。
        activation_valid_until = time.monotonic() + self.local_hold_window_sec()
        while True:
            # 激活期没有持有态,按固定节奏续;持有期不超过剩余安全窗。
            hold = self._hold
            if hold is None:
                sleep_for = interval
            else:
                sleep_for = min(interval, max(0.05, hold.valid_until - time.monotonic()))
            await asyncio.sleep(sleep_for)

            hold = self._hold
            started = time.monotonic()
            try:
                remaining = await refresh_or_raise(lease, timeout=interval)
            except asyncio.CancelledError:
                raise
            except LeaseGoneError as exc:
                # ★ 服务端明确说 lease 没了 —— 这是**失主的证据**,不是"这次没连上"。
                # 不能走下面的安全窗重试分支:那会在已经确定失主之后继续对外宣告可写,
                # 而 leader key 早已被别人拿走 → 两个写者并存。必须当场停手。
                plog.get().warning(
                    "writerlease_lost",
                    name=self._name,
                    term=self._term,
                    err=str(exc),
                    hint="etcd 回复 lease 不存在,立即停止宣告可写(不等本地安全截止线)",
                )
                if hold is not None:
                    hold.self_fenced = True
                self._hold = None
                return
            except Exception as exc:  # noqa: BLE001
                # 激活期续约失败:窗内继续重试,越线才放弃(让 lease 过期,后继接管)。
                if hold is None:
                    if time.monotonic() < activation_valid_until:
                        continue  # etcd 短抖动很常见,别把一整届激活白扔掉
                    plog.get().warning(
                        "writerlease_lost_during_activation",
                        name=self._name,
                        term=self._term,
                        err=str(exc),
                        hint="激活期续约越过本地安全截止线,放弃本届(租约多半已经没了)",
                    )
                    return
                if time.monotonic() >= hold.valid_until:
                    hold.self_fenced = True  # ★ 越线即单调终态
                    plog.get().warning(
                        "writerlease_lost",
                        name=self._name,
                        term=self._term,
                        err=str(exc),
                        hint="越过本地安全截止线,停止对外宣告可写(防两个写者)",
                    )
                    return
                continue  # 还在安全窗内,etcd 短抖动很常见,继续试

            if hold is None:
                # 还在激活期:只保活不开窗,但要把激活期的安全线往后推 ——
                # 用**发起时刻**为锚点(与持有期同一保守方向)。
                if remaining - HOLD_SAFETY_MARGIN_SEC > 0:
                    activation_valid_until = started + (remaining - HOLD_SAFETY_MARGIN_SEC)
                continue

            # ★ 成功响应也**不是无条件**推进:_apply_ttl_proof 会先看本届是否已自
            # fencing、以及这份证据算出来的窗口是不是已经过期。一次迟到的续约不能
            # 把已经停手的旧任期"续活"——那会让上层看到「持有→不持有→又持有」
            # 这种自相矛盾的序列,而 token 还是同一个。
            if not _apply_ttl_proof(hold, remaining, started):
                plog.get().warning(
                    "writerlease_lost",
                    name=self._name,
                    term=self._term,
                    remaining_sec=remaining,
                    hint="本届已自 fencing 或剩余租期不足安全余量,让位重选(同一任期不复活)",
                )
                self._hold = None
                return

    # ── 健康度 ───────────────────────────────────────────────────────────

    def _record_campaign_err(self, exc: BaseException) -> None:
        self._health.consecutive_campaign_errs += 1
        self._health.last_campaign_err = str(exc)
        level = (
            plog.get().error
            if self._health.consecutive_campaign_errs >= CAMPAIGN_ESCALATE_AFTER
            else plog.get().warning
        )
        level(
            "writerlease_campaign_failed",
            name=self._name,
            consecutive=self._health.consecutive_campaign_errs,
            err=str(exc),
        )

    def _record_activation_err(self, exc: BaseException) -> None:
        self._health.consecutive_activation_errs += 1
        self._health.last_activation_err = str(exc)
