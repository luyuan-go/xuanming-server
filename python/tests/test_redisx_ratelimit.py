"""限流两原语 —— 对应 Go 侧 pkg/redisx/ratelimit.go。

★ 这组件的定位是**背压,不是权威门**。所以它的正确性有两个方向,漏掉任一个都出事:

    压得住   —— 窗口内超过 limit 必须拒(否则等于没限)
    压不死   —— 判定失败必须**放行**(否则限流器自己成了卡玩家的源头,§9.20 反向红线)

第二个方向最容易写反:直觉上"查不到就拒"更安全,而在这里它恰好相反。

用 fakeredis(真 Lua)而不是 mock —— 配额的原子性全在那段 INCR+PEXPIRE 的 Lua 里,
mock 掉就等于什么都没测。
"""

from __future__ import annotations

import asyncio
import pathlib
import re

import pytest
from structlog.testing import capture_logs

from pandorapy import redisx


@pytest.fixture
async def rdb():
    fakeredis = pytest.importorskip("fakeredis")
    pytest.importorskip("lupa", reason="Lua 脚本需要 fakeredis[lua]")
    server = fakeredis.FakeServer()
    client = fakeredis.aioredis.FakeRedis(server=server, decode_responses=False)
    try:
        yield client
    finally:
        await client.aclose()


class _BrokenRedis:
    """任何命令都炸的 client —— 用来验 fail-open 方向。

    ★ 替身的**方法名必须是生产真正会调的那几个**,否则测的是"这个对象缺方法"
    而不是"Redis 挂了"。

    原版给了 `evalsha` / `eval` / `script_load` 三个方法,而 `redisx.LuaScript`
    走的是 redis-py 的 `client.register_script(body)`(EVALSHA→EVAL 回退由
    redis-py 在连接池层做,不由本仓调)。于是配额那两条 fail-open 用例实际拿到的是
    `AttributeError: '_BrokenRedis' object has no attribute 'register_script'`,
    被 `incr_window` 的 `except Exception` 接住 → 返回 `(True, exc)` → 断言全过。
    实测判据:换成一个**什么方法都没有**的空对象,那两条用例照样绿 ——
    也就是说它们对"Redis 故障"这件事零覆盖,连 Lua 脚本体都没跑到。

    ★ 光把方法名改对还不够,用例侧必须**钉住异常类型**(下面统一断言
    `isinstance(exc, ConnectionError)`)。只写 `exc is not None` 的话,替身哪天
    又漂成"缺方法"形状,抛出来的 AttributeError 同样满足断言,这个洞会**原样长回来**
    且没有任何信号。异常类型是"故障真的来自模拟的 Redis 传输层"的唯一凭据。

    Go 侧结构上没有这个洞,所以那边不需要这两层讲究:`brokenClient(t)` 是
    **真的 go-redis 客户端**打向一个已 Close 的 miniredis(pkg/redisx/ratelimit_test.go:23),
    故障来自 transport;而且 redis.UniversalClient 是编译期接口,
    "手搓一个缺方法的替身"在 Go 里根本编译不过。Python 这边只能靠上面两条约束自律。
    """

    def register_script(self, body):  # noqa: ANN001, ANN201
        """与 redis-py 同形:**同步**返回一个可 await 的 Script(不是 coroutine)。

        写成 `async def` 会让生产侧 `client.register_script(...)` 拿到 coroutine,
        接着 `await script(keys=..., args=...)` 抛 TypeError —— 又是一个
        "异常来自替身自己"的假覆盖。
        """
        del body

        async def _run(keys=None, args=None):  # noqa: ANN001, ANN202, ARG001
            raise ConnectionError("redis down")

        return _run

    async def set(self, *a, **kw):  # noqa: ANN002, ANN003, ANN201
        raise ConnectionError("redis down")

    async def pttl(self, *a, **kw):  # noqa: ANN002, ANN003, ANN201
        raise ConnectionError("redis down")


# ── ★ key 规范与 Go 逐字一致 ────────────────────────────────────────────────


def test_rl_key_format_matches_go_source(repo_root: pathlib.Path) -> None:
    """★ key 前缀与拼法必须与 Go 的 RLKey **逐字一致**,判据取自 Go 源码。

    两栈并存时同一个限流点会被 Go 副本和 Python 副本分别记数。key 不一致 =
    两边各记各的,实际放行量是配置的**两倍**,而两边日志都显示"限流生效中"。
    """
    src = (repo_root / "pkg" / "redisx" / "ratelimit.go").read_text(encoding="utf-8")
    m = re.search(r'func RLKey\([^)]*\)\s*string\s*\{\s*return fmt\.Sprintf\("([^"]+)"', src)
    assert m, "没在 Go 源码里找到 RLKey 的格式串"
    go_fmt = m.group(1)  # 形如 pandora:rl:%s:%s:%d
    expected = go_fmt.replace("%s", "{}", 2).replace("%d", "{}").format("match", "start", 1001)
    assert redisx.rl_key("match", "start", 1001) == expected


def test_rl_key_string_variant() -> None:
    assert redisx.rl_key_string("login", "fail", "ip1") == "pandora:rl:login:fail:ip1"


# ── ★ 配额:压得住 ──────────────────────────────────────────────────────────


async def test_quota_allows_up_to_limit_then_rejects(rdb) -> None:
    key = redisx.rl_key("friend", "request", 2001)
    for i in range(3):
        ok, exc = await redisx.quota(rdb, key, 3, 60)
        assert exc is None
        assert ok, f"第 {i + 1} 次就被拒了,配额是 3"
    ok, exc = await redisx.quota(rdb, key, 3, 60)
    assert exc is None
    assert not ok, "超过配额仍放行 —— 等于没限流"


async def test_quota_window_expires_and_resets(rdb) -> None:
    """窗口从**首次计数**起算,过期后重新开始。"""
    key = redisx.rl_key("friend", "request", 2002)
    assert (await redisx.quota(rdb, key, 1, 0.2))[0]
    assert not (await redisx.quota(rdb, key, 1, 0.2))[0]
    await asyncio.sleep(0.35)
    assert (await redisx.quota(rdb, key, 1, 0.2))[0], "窗口过期后没有重置"


@pytest.mark.parametrize(("limit", "window"), [(0, 60), (-1, 60), (3, 0), (3, -1)])
async def test_non_positive_limit_or_window_means_unlimited(rdb, limit, window) -> None:
    """limit / window <= 0 = 不限流(与 Go 同,也是 dev 默认关掉限流的方式)。"""
    key = redisx.rl_key("x", "y", 1)
    for _ in range(10):
        ok, exc = await redisx.quota(rdb, key, limit, window)
        assert ok and exc is None


async def test_counter_always_carries_a_ttl(rdb) -> None:
    """★ 计数键必须**自带 TTL**,不能留下永久键。

    INCR 与 PEXPIRE 分成两条命令的话,INCR 成功后进程死掉会留下一个没有 TTL 的
    计数键 —— 那个主体从此**永久超配额**,而且没有任何东西会清理它。
    (本模块无后台清理任务,内存有界完全依赖这条。)
    """
    key = redisx.rl_key("friend", "request", 2003)
    await redisx.quota(rdb, key, 5, 60)
    ttl = await rdb.pttl(key)
    assert ttl is not None and ttl > 0, "计数键没有 TTL —— 会永久占住并锁死该主体"


# ── ★ 配额:压不死(fail-open 方向)────────────────────────────────────────


async def test_quota_fails_open_when_redis_is_down() -> None:
    """★ Redis 故障必须**放行**并把异常交回调用方留证。

    这是本组件最容易写反的一条:直觉上"查不到就拒"更安全,而限流是背压门 ——
    拒了等于 Redis 一抖动全服玩家就动不了(§9.20:限流不得成为卡玩家的源头)。
    正确性由各自的权威门兜底,不由限流兜底。

    第二句断言写成 `isinstance(...)` 而不是 `is not None`,理由见 _BrokenRedis 的
    docstring:后者连"替身自己缺方法抛的 AttributeError"都收,等于没测。
    """
    ok, exc = await redisx.quota(_BrokenRedis(), "pandora:rl:x:y:1", 1, 60)
    assert ok, "Redis 故障时拒绝了请求 —— 限流器自己成了卡玩家的源头"
    assert isinstance(exc, ConnectionError), f"故障没有原样交回调用方:{exc!r}"


async def test_cooldown_fails_open_when_redis_is_down() -> None:
    ok, exc = await redisx.cooldown(_BrokenRedis(), "pandora:rl:x:y:1", 60)
    assert ok
    assert isinstance(exc, ConnectionError), f"故障没有原样交回调用方:{exc!r}"


async def test_action_quota_fails_open_and_returns_the_fault() -> None:
    """★ ActionQuota fail-open **放行**,并把故障**交回调用方**。

    原实现在这里自己打一条通用的 `rate_quota_unavailable` 就完事,返回裸 bool。
    后果是五个服务里 `<svc>_rate_quota_check_failed` 那段 except 全成了
    不可达死代码 —— 那几个 Loki 键在代码里存在、永远不会触发,
    真正打出来的只有一条不带 player_id 的通用 warn。

    fail-open 的**方向**不变(§9.20:限流是背压门不是权威门),
    变的是"谁来记这件事":调用方知道自己是哪个服务、哪个玩家。
    """
    q = redisx.ActionQuota(_BrokenRedis(), "trade", 1, 60)
    ok, exc = await q.allow("create_order", 1001)
    assert ok is True, "限流判定失败必须放行"
    assert isinstance(exc, ConnectionError), f"故障必须原样交回调用方,不能就地吞掉:{exc!r}"


# ── ★ 冷却窗 ────────────────────────────────────────────────────────────────


async def test_cooldown_is_first_come_first_served(rdb) -> None:
    key = redisx.rl_key("hub", "transfer", 3001)
    assert (await redisx.cooldown(rdb, key, 60))[0]
    assert not (await redisx.cooldown(rdb, key, 60))[0]


async def test_clear_cooldown_lets_a_failed_attempt_retry(rdb) -> None:
    """★ 「先占冷却 → 干活 → 失败释放」:业务失败必须立即放开。

    冷却只该约束**成功路径**的频率。不释放的话,一次失败会把玩家锁在门外
    整整一个窗口 —— 玩家视角是"点了没反应还得等",而日志里一切正常。
    """
    key = redisx.rl_key("hub", "transfer", 3002)
    assert (await redisx.cooldown(rdb, key, 60))[0]
    await redisx.clear_cooldown(rdb, key)
    assert (await redisx.cooldown(rdb, key, 60))[0], "失败释放后仍然进不来"


# ── ★ 惩罚窗 ────────────────────────────────────────────────────────────────


async def test_arm_penalty_overwrites_remaining_window(rdb) -> None:
    """★ 惩罚窗是**无条件覆盖**,不是"先到先占"。

    与 cooldown 的区别正在这里:cooldown 是准入判定,arm_penalty 是"事实发生后记罚"。
    用 NX 语义的话,第二次违规会因为"窗口里已经有一个"而被忽略 —— 越违规越安全。
    """
    key = redisx.rl_key("match", "noshow", 4001)
    assert await redisx.arm_penalty(rdb, key, 0.3) is None
    assert await redisx.arm_penalty(rdb, key, 30) is None
    remaining, exc = await redisx.penalty_remaining(rdb, key)
    assert exc is None
    assert remaining > 1.0, "新惩罚没有顶替旧惩罚"


async def test_penalty_remaining_is_zero_when_absent(rdb) -> None:
    assert await redisx.penalty_remaining(rdb, "pandora:rl:none:none:1") == (0.0, None)


async def test_penalty_remaining_fails_open_but_reports_the_error() -> None:
    """★ 读侧 fail-open 是对的(罚查不到别拦人),但**必须留证**。

    吞掉异常的话,"惩罚窗长期失效"没有任何信号 —— 而它的表现是
    "该被退避的玩家一直没被退避",没人会往 Redis 上想。
    """
    remaining, exc = await redisx.penalty_remaining(_BrokenRedis(), "k")
    assert remaining == 0.0
    assert isinstance(exc, ConnectionError), f"故障没有原样交回调用方:{exc!r}"


async def test_arm_penalty_reports_write_failure() -> None:
    """★ 罚**没记上**必须交回调用方。

    写侧不像读侧有 fail-open 兜底 —— 写失败就是真的漏了一次罚。
    """
    exc = await redisx.arm_penalty(_BrokenRedis(), "k", 30)
    assert isinstance(exc, ConnectionError), f"故障没有原样交回调用方:{exc!r}"


# ── ★ Lua 出入口(配额/锁/游标全走这一层)──────────────────────────────────
#
# 上面那几条 fail-open 用例是**隔着** incr_window 的 `except Exception` 看这一层的:
# 只要有异常从这里冒出来,是什么、有没有留痕,它们都不管。下面两条直接盯住
# LuaScript.__call__ 本身 —— 它是全仓 Lua 脚本的唯一出入口,被配额之外的大量
# 原子操作共用,改动概率不低,而它的两条分支(失败留痕再抛 / NOSCRIPT 重跑)
# 在此之前一次都没被跑到过。
#
# Go 侧没有对应用例,因为 Go 根本没有这一层:那边是
# `quotaScript.Run(ctx, rdb, ...).Int64()` 直接把 error 返给调用方
# (pkg/redisx/ratelimit.go:78),没有可供"吞掉异常"的包装。


async def test_lua_script_reraises_the_fault_and_leaves_a_trace() -> None:
    """★ 脚本失败必须「记一笔 **再抛**」—— 吞掉的话故障会静默消失。

    这是本模块最隐蔽的一种改法:把 __call__ 结尾的 `raise` 换成 `return 0`,
    quota 会拿到 (0, None) → `0 <= limit` → 返回 (True, None)。
    **放行方向没变、异常却没了**,于是五个服务里那句
    `<svc>_rate_quota_check_failed` 的 except 永不触发,Loki 上一片安静 ——
    "限流长期在 fail-open 空转"这件事从此没有任何信号。上面的 fail-open 用例
    只断言"放行",拦不住这种改法,所以要有这一条。

    同时钉住 warn 的事件名与字段:redis_script_failed / script / exc_type 是
    Loki 上筛这类故障的唯一入口,名字漂了查询就空了(见 pandorapy/log.py 头注释)。
    """
    with capture_logs() as logs:
        with pytest.raises(ConnectionError):
            await redisx.LuaScript("t_probe", "return 1")(_BrokenRedis(), keys=["k"], args=[1])
    warn = [e for e in logs if e["event"] == "redis_script_failed"]
    assert len(warn) == 1, "脚本失败没有留下 redis_script_failed —— 故障在日志上不可见"
    assert warn[0]["script"] == "t_probe"
    assert warn[0]["exc_type"] == "ConnectionError"


async def test_lua_script_retries_once_on_noscript() -> None:
    """★ NOSCRIPT 必须**当场重跑一次**,不能往上抛。

    Redis 节点重启 / 故障切换后脚本缓存会丢,EVALSHA 报 NOSCRIPT。redis-py 的
    Script 会自动改用 EVAL,所以原地重调一次就能成 —— 抛上去的话,一次 Redis
    重启就会让所有配额判定一起 fail-open(全服限流短暂失效),而这恰恰是最需要
    限流的时刻。

    判据用调用次数而不是返回值:只看返回值的话,把重试改成"吞掉异常返回 0"
    也是"没抛",分不出来。
    """

    class _NoScriptOnce:
        """第一次抛 NOSCRIPT、第二次成功 —— 模拟脚本缓存丢失后 redis-py 的 EVAL 兜底。"""

        def __init__(self) -> None:
            self.calls = 0

        def register_script(self, body):  # noqa: ANN001, ANN201
            del body

            async def _run(keys=None, args=None):  # noqa: ANN001, ANN202, ARG001
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("NOSCRIPT No matching script. Please use EVAL.")
                return 7

            return _run

    client = _NoScriptOnce()
    got = await redisx.LuaScript("t_noscript", "return 7")(client, keys=["k"])
    assert got == 7, "NOSCRIPT 后没有重跑 —— 一次 Redis 重启就会让配额判定整片失效"
    assert client.calls == 2, f"重试次数不对:{client.calls}"
