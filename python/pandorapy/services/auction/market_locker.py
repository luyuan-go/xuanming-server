"""跨实例 per-market 单写者锁 —— 对应 Go 侧 internal/data/market_locker.go
(底座是 pkg/redislock)。

进程内条带锁只在单实例内串行;多实例部署时同一 market 可能落到不同实例并发撮合,
订单簿与权威库会被并发改。本锁保证正常运行时同一 market 全局只有一个实例在撮合。

★ 它**不是 fencing token**:权威正确性仍必须由 MySQL 行锁、条件状态迁移和唯一键兜底。
  把它当成"拿到锁就一定安全"是分布式锁最经典的误用 —— 锁过期、进程暂停、网络分区
  都会让"我以为我还持有"变成假的。

★ key 前缀 `pandora:auction:market:` 必须与 Go **逐字一致**:
  迁移期两栈并存,同一把 market 锁会被 Go 副本和 Python 副本分别去拿。前缀不一致 =
  两边落在两个不同的 key 上,互斥当场失效,而**两边日志都显示加锁成功**。

★ 续租失败 = 无法再证明自己是唯一写者 → **fail-stop 退出进程**(与 Go 的 os.Exit(1) 同)。
  不退出的后果:本实例继续在一个已经被别人接管的 market 上撮合,双写窗口一直开着。
  这是"停机"优于"静默双写"的少数场景之一。
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
import uuid
from collections.abc import Callable

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import redisx

# key 前缀:Go 侧 NewRedisLocker(rdb, "pandora:auction:market:") 注入的同一个值。
KEY_PREFIX = "pandora:auction:market:"

MAX_TTL_SEC = 30.0
# pkg/redislock 的 Extend 走秒级 EXPIRE;小于 1 秒会被截成 0 并立即删锁。
MIN_TTL_SEC = 1.0
MAX_REDIS_COMMAND_TIMEOUT_SEC = 2.0

# 与 Go 的 redislock Release / Extend 脚本逐字相同(只去掉 Go 里的缩进制表符)。
_RELEASE_SCRIPT = redisx.LuaScript(
    name="auction_market_unlock",
    body="""
local currentValue = redis.call('GET', KEYS[1])
if currentValue == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end""",
)

_EXTEND_SCRIPT = redisx.LuaScript(
    name="auction_market_extend",
    body="""
local currentValue = redis.call('GET', KEYS[1])
if currentValue == ARGV[1] then
    return redis.call('EXPIRE', KEYS[1], ARGV[2])
else
    return 0
end""",
)


def _exit_process(_market_id: int, _cause: BaseException | str) -> None:
    """生产用 fail-stop:立刻退出,交给 k8s 重新拉起。

    ★ 用 `os._exit` 而不是 `sys.exit`:此刻正身处一个后台 task 里,
    `sys.exit` 只会让那个 task 结束,进程照跑 —— 双写窗口不会关。
    """
    os._exit(1)


def market_key(market_id: int) -> str:
    """仅用数字 market_id,避免高基数(前缀已在类里拼)。"""
    return f"{KEY_PREFIX}{market_id}"


def _command_timeout(ttl_sec: float) -> float:
    timeout = ttl_sec / 3
    return MAX_REDIS_COMMAND_TIMEOUT_SEC if timeout > MAX_REDIS_COMMAND_TIMEOUT_SEC else timeout


class MarketLockLease:
    """一次成功持锁的生命周期(续租 + 释放)。

    ★ release 必须"先阻止新续租、等续租协程退出、再删 token":
    否则 Extend 与 Release 会并发读写同一份状态,出现"刚续上又被删"或
    "删完又被自己续回来"——后者会让锁在无人持有的情况下继续存在到 TTL,
    整个 market 卡住一个 TTL 而没有任何错误。
    """

    __slots__ = ("_rdb", "_key", "_token", "_market_id", "_ttl", "_fail_stop", "_task", "_state")

    def __init__(
        self,
        rdb,  # noqa: ANN001
        key: str,
        token: str,
        market_id: int,
        ttl_sec: float,
        fail_stop: Callable[[int, BaseException | str], None],
    ) -> None:
        self._rdb = rdb
        self._key = key
        self._token = token
        self._market_id = market_id
        self._ttl = ttl_sec
        self._fail_stop = fail_stop
        self._task: asyncio.Task | None = None
        # "releasing" / "failed" 两个标记合用一个字段,保证判定在同一处发生
        # (Go 用一把 mutex 保护 releasing+failed;asyncio 单线程,赋值即临界区)。
        self._state = ""

    def start(self) -> None:
        self._task = asyncio.create_task(
            self._renew_loop(), name=f"auction_market_lock_renew_{self._market_id}"
        )

    async def _renew_loop(self) -> None:
        renew_every = self._ttl / 3
        try:
            while True:
                await asyncio.sleep(renew_every)
                if self._state == "releasing":
                    return
                cause: BaseException | str | None = None
                try:
                    extended = await asyncio.wait_for(
                        _EXTEND_SCRIPT(
                            self._rdb, keys=[self._key], args=[self._token, int(self._ttl)]
                        ),
                        timeout=_command_timeout(self._ttl),
                    )
                    if int(extended) == 1:
                        continue
                    cause = "market lock token expired or ownership changed"
                except asyncio.CancelledError:
                    # ★ 取消必须穿透:正常停机会 cancel 这个 task,
                    # 吞掉会把"进程在停机"翻译成"续租失败 → 退出进程"。
                    raise
                except BaseException as exc:  # noqa: BLE001
                    cause = exc
                if self._state in ("releasing", "failed"):
                    return
                self._state = "failed"
                # 生产 hook 是退出进程:先 fail-stop,避免同步日志输出卡住而延长双写窗口。
                self._fail_stop(self._market_id, cause)
                plog.get().error(
                    "auction_market_lock_renew_failed_fail_stop",
                    market_id=self._market_id,
                    err=str(cause),
                )
                return
        except asyncio.CancelledError:
            raise

    async def release(self) -> None:
        if self._state == "released":
            return
        self._state = "releasing"
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        try:
            await asyncio.wait_for(
                _RELEASE_SCRIPT(self._rdb, keys=[self._key], args=[self._token]),
                timeout=_command_timeout(self._ttl),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— 释放失败只是等 TTL,不是错误路径
            plog.get().warning(
                "auction_market_unlock_failed", market_id=self._market_id, err=str(exc)
            )
        self._state = "released"


class RedisMarketLocker:
    """跨实例 per-market 单写者锁。ttl/max_wait/retry_every <= 0 时取安全默认。"""

    __slots__ = ("_rdb", "_ttl", "_max_wait", "_retry_every", "_fail_stop")

    def __init__(
        self,
        rdb,  # noqa: ANN001
        ttl_sec: float,
        max_wait_sec: float,
        retry_every_sec: float = 0.0,
        fail_stop: Callable[[int, BaseException | str], None] | None = None,
    ) -> None:
        if ttl_sec <= 0 or ttl_sec > MAX_TTL_SEC:
            ttl_sec = MAX_TTL_SEC  # 不变量 §10:Redis lock TTL ≤ 30s
        elif ttl_sec < MIN_TTL_SEC:
            ttl_sec = MIN_TTL_SEC
        self._rdb = rdb
        self._ttl = ttl_sec
        self._max_wait = max_wait_sec if max_wait_sec > 0 else 3.0
        self._retry_every = retry_every_sec if retry_every_sec > 0 else 0.02
        self._fail_stop = fail_stop or _exit_process

    async def lock(self, market_id: int) -> MarketLockLease:
        """阻塞式抢 market 写锁(带退避重试)。max_wait 内抢不到 → ErrAuctionMarketBusy。"""
        key = market_key(market_id)
        deadline = time.monotonic() + self._max_wait
        while True:
            token = uuid.uuid4().hex
            try:
                acquired = await self._rdb.set(key, token, nx=True, ex=int(self._ttl))
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                if time.monotonic() >= deadline:
                    raise _busy(market_id) from exc
                raise errcode.PandoraError(
                    errcode.ErrInternal, "market lock %d: %s", market_id, exc
                ) from exc
            if acquired:
                lease = MarketLockLease(
                    self._rdb, key, token, market_id, self._ttl, self._fail_stop
                )
                lease.start()
                return lease
            if time.monotonic() >= deadline:
                raise _busy(market_id)
            await asyncio.sleep(self._retry_every)


def _busy(market_id: int) -> errcode.PandoraError:
    return errcode.PandoraError(
        errcode.ErrAuctionMarketBusy, "market %d busy, retry later", market_id
    )
