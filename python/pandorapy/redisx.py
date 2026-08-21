"""Redis 封装 —— 对应 Go 侧 pkg/redisx + pkg/redislock。

全仓有 **190 处** Redis 原子操作(Lua / 事务),它们承载的是限额校验、名额预留、
会话 fencing 这类"错了就是数据损坏"的逻辑。迁移策略:

    **Lua 脚本原样搬,一个字都不改。**

    Lua 在 Redis 服务端执行,与调用方语言无关。把已经在生产跑过的脚本原样搬过来,
    等于把这 190 处的正确性风险降到接近零 —— 需要重新验证的只剩"参数传对没有"。
    反过来,如果借迁移之机"顺手用 Python 重写成几条命令",就等于把 190 个原子操作
    重新实现一遍,每一个都是新的竞态入口。

redis-py 的 async 客户端(`redis.asyncio`)与 grpc.aio 同一个 event loop,
不会像同步客户端那样阻塞整个循环。
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import redis.asyncio as aioredis
from redis.asyncio.client import Redis
from redis.commands.core import AsyncScript

from pandorapy import log as plog


def _contextlib_suppress():
    """关闭半开客户端时的静默兜底 —— 关不掉不该盖住真正要抛的异常。"""
    return contextlib.suppress(Exception)

# 与 Go 侧 pkg/redislock 一致:锁 TTL 上限 30s(不变量 §9.10),业务跑完主动释放。
MAX_LOCK_TTL_SEC = 30


class LockTTLTooLongError(ValueError):
    """锁 TTL 超过 30s。不变量 §9.10 的机械闸。"""


def new_client(
    addr: str,
    *,
    db: int = 0,
    password: str = "",
    dial_timeout_sec: float = 2.0,
) -> Redis:
    """建一个 async Redis 客户端。参数名对齐 yaml 的 node.redis_client 段。

    decode_responses=False:全仓大量存 protobuf bytes,自动解码会炸。
    与 Go 侧 go-redis 的默认行为(返回 []byte)一致。
    """
    host, _, port = addr.rpartition(":")
    return aioredis.Redis(
        host=host or "127.0.0.1",
        port=int(port or 6379),
        db=db,
        password=password or None,
        socket_connect_timeout=dial_timeout_sec,
        socket_timeout=dial_timeout_sec,
        decode_responses=False,
        # health_check_interval:连接空闲后被中间设备静默断开时,下次使用前先 PING。
        # 不设的话会在长空闲后收到一次莫名其妙的 ConnectionError。
        health_check_interval=30,
    )


class LuaScript:
    """一段 Lua 脚本 —— 直接承载从 Go 侧原样搬来的脚本文本。

    用法:

        CLAIM_SLOT = LuaScript(name="claim_slot", body='''
            local n = redis.call('SCARD', KEYS[1])
            if n >= tonumber(ARGV[1]) then return 0 end
            redis.call('SADD', KEYS[1], ARGV[2])
            return 1
        ''')
        ok = await CLAIM_SLOT(client, keys=[key], args=[limit, member])

    为什么包一层而不是直接用 redis-py 的 register_script:
      - 强制给脚本起名字,失败日志里能看出是哪段脚本(190 段脚本靠 sha 排查是灾难)
      - 统一 NOSCRIPT 后的重新加载(集群故障切换后脚本缓存会丢)
    """

    __slots__ = ("name", "body")

    def __init__(self, name: str, body: str) -> None:
        self.name = name
        self.body = body

    async def __call__(
        self, client: Redis, keys: list[Any] | None = None, args: list[Any] | None = None
    ) -> Any:
        """执行脚本。

        ⚠️ **不缓存 Script 对象**(2026-08-18 实测踩到的缺陷)。
        最初写成 `if self._script is None: self._script = client.register_script(...)`,
        于是模块级的 LuaScript 实例把 Script 连同**第一个传进来的 client** 缓存了下来;
        之后换了 client 再调用,脚本仍然打到旧 client 上。

        生产里通常只有一个 client,所以这个 bug 会被掩盖 —— 直到:
          - 连接池被替换 / 故障切换后重建 client
          - 或者像测试里那样每个用例一个独立 client(4 个用例因此变红,
            而且单独跑全过、全量跑才挂,是最难查的形态)

        `client.register_script` 本身只是包一层并本地算 sha1(很便宜),
        真正的 EVALSHA→EVAL 回退由 redis-py 在连接池层处理,所以每次新建没有性能问题。
        """
        script: AsyncScript = client.register_script(self.body)
        try:
            return await script(keys=keys or [], args=args or [])
        except Exception as exc:  # noqa: BLE001
            # NOSCRIPT:节点重启 / 故障切换后脚本缓存丢失。redis-py 的 Script 会自动用
            # EVAL 兜底,这里只记一笔 —— 频繁出现说明 Redis 在反复重启。
            if "NOSCRIPT" in str(exc):
                plog.get().warning("redis_script_cache_miss", script=self.name)
                return await script(keys=keys or [], args=args or [])
            plog.get().warning(
                "redis_script_failed", script=self.name, err=str(exc), exc_type=type(exc).__name__
            )
            raise


# ── 分布式锁 ─────────────────────────────────────────────────────────────────

# 锁 key 前缀。★ 必须与 Go 侧 pkg/redislock 的 DefaultPrefix **逐字一致**。
#
# 迁移期两栈并存,同一把业务锁会被 Go 副本和 Python 副本分别去拿。前缀不一致 =
# 两边落在**两个不同的 key** 上 —— 双方都能"拿到锁",互斥当场失效,而且
# 两边日志都显示加锁成功,没有任何运行期信号。
LOCK_KEY_PREFIX = "pandora:lock:"


def lock_key(name: str) -> str:
    """把业务名拼成完整锁 key。已带前缀的原样返回(允许调用方传全名)。"""
    return name if name.startswith(LOCK_KEY_PREFIX) else f"{LOCK_KEY_PREFIX}{name}"


# 释放锁必须校验持有者 —— 否则会释放掉**别人**的锁:
#   A 拿锁 → A 卡住超过 TTL → 锁自动过期 → B 拿到锁 → A 恢复并 DEL → B 的锁没了
# 这是 Redis 分布式锁最经典的错误。用 Lua 保证"比对 + 删除"原子。
_UNLOCK_SCRIPT = LuaScript(
    name="unlock_if_owner",
    body="""
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
""",
)


@contextlib.asynccontextmanager
async def lock(client: Redis, key: str, token: str, ttl_sec: int):
    """分布式锁。对应 Go 侧 pkg/redislock。

    ttl_sec 超过 30s 直接拒绝(不变量 §9.10)。token 必须是本次持有的唯一标识
    (调用方通常用 snowflake 或 uuid),释放时用它校验持有者。

    key 会自动补上 `pandora:lock:` 前缀(与 Go 侧同一个 key 空间,见 LOCK_KEY_PREFIX);
    传入已带前缀的全名则原样使用。

    ⚠️ 这把锁只降低冲突概率,**不能**作为最终正确性的唯一保证 —— §16.1 要求
    共享写的正确性由数据库条件更新 / 唯一键 / CAS / Lua 保证。锁过期、进程暂停、
    网络分区都会让"我以为我还持有"变成假的。
    """
    if ttl_sec > MAX_LOCK_TTL_SEC:
        raise LockTTLTooLongError(
            f"redislock: TTL {ttl_sec}s 超过上限 {MAX_LOCK_TTL_SEC}s(不变量 §9.10);"
            f"长任务应当分段并在段间续租,而不是把锁 TTL 拉长"
        )
    full_key = lock_key(key)
    acquired = await client.set(full_key, token, nx=True, ex=ttl_sec)
    if not acquired:
        raise TimeoutError(f"redislock: 获取锁失败 key={full_key}")
    try:
        yield
    finally:
        with contextlib.suppress(Exception):
            await _UNLOCK_SCRIPT(client, keys=[full_key], args=[token])


async def ping(client: Redis) -> bool:
    """探活。启动期强依赖检查用,失败必须 fail-fast 而不是降级 ——
    Redis 不通时 hub_allocator 拉不起大厅 Hub DS,玩家会卡在连大厅。"""
    try:
        return bool(await client.ping())
    except Exception:  # noqa: BLE001
        return False


# ── 限流两原语 —— 对应 Go 侧 pkg/redisx/ratelimit.go ─────────────────────────
#
# 定位:**背压,不是权威门**(anti-abuse-scene-entry.md §2 铁律)。正确性由各自的
# 权威门兜底(ensureNoneInBattle / owner lease / Admission CAS…),限流只压成本。因此:
#
#   - 所有判定 error 时一律 **fail-open**(返回 allow=True 并把异常交给调用方 Warn
#     留证)—— 限流器故障绝不能成为卡玩家的源头(§9.20 反向红线);
#   - key 一律自带 PX 过期,无后台清理任务,内存有界 = 窗口内活跃主体数;
#   - 拒绝时调用方返回 ErrRateLimited,客户端按可重试处理。
#
# 刻意不做滑动窗口 / 令牌桶:固定窗口的边界双倍对「防外挂刷量」无实质影响
# (2 倍仍远小于外挂想要的 100 倍),而滑动窗口要存有序集合、要清理,属 §15.3
# 预设性复杂化。
#
# key 规范(docs/design/infra.md §3.2「RateLimit」):
#
#     pandora:rl:<域>:<动作>:<主体id>     例 pandora:rl:match:start:1234567


def rl_key(domain: str, action: str, subject: int) -> str:
    """限流 key(主体是 uint64 业务 ID 的常规形态)。与 Go 的 RLKey 逐字一致。"""
    return f"pandora:rl:{domain}:{action}:{subject}"


def rl_key_string(domain: str, action: str, subject: str) -> str:
    """主体不是数字 ID 时(账号名哈希 / IP)的变体。

    ⚠️ 调用方负责保证 subject 已消毒(定长哈希 / IP 字面量),
    **不得**直接拼客户端原文 —— 那会让 key 空间被玩家控制。
    """
    return f"pandora:rl:{domain}:{action}:{subject}"


async def cooldown(client: Redis, key: str, window_sec: float) -> tuple[bool, Exception | None]:
    """占用一次冷却窗:SET key 1 NX PX window。单命令原子、跨副本一致。

    返回 (是否占窗成功, 故障)。window <= 0 视为不限流。
    Redis 故障 **fail-open**:返回 (True, exc),调用方必须 Warn 留证后放行。
    """
    if window_sec <= 0:
        return True, None
    try:
        ok = await client.set(key, 1, nx=True, px=int(window_sec * 1000))
    except Exception as exc:  # noqa: BLE001 —— 背压门故障必须放行
        return True, exc
    return bool(ok), None


async def clear_cooldown(client: Redis, key: str) -> None:
    """释放冷却窗(DEL,幂等)。

    用于「先占冷却 → 干活 → 失败释放」模板:业务失败时立即释放让玩家可重试,
    冷却只约束**成功路径**的频率 —— 否则一次失败会把玩家锁在门外一整个窗口。
    """
    with contextlib.suppress(Exception):
        await client.delete(key)


# INCR + 仅首次设 PEXPIRE。★ 必须写在**一个** Lua 里:
# 分成两条命令的话,INCR 成功后进程死掉会留下一个**没有 TTL 的永久计数键** ——
# 那个主体从此永久超配额,而且没有任何东西会清理它。
_QUOTA_SCRIPT = LuaScript(
    name="rl_quota",
    body="""
local n = redis.call('INCR', KEYS[1])
if n == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
return n
""",
)


async def incr_window(client: Redis, key: str, window_sec: float) -> tuple[int, Exception | None]:
    """固定窗口计数:返回本次递增后的计数值。窗口从**首次计数**起算,PX 自过期。"""
    if window_sec <= 0:
        return 0, None
    try:
        n = await _QUOTA_SCRIPT(client, keys=[key], args=[int(window_sec * 1000)])
    except Exception as exc:  # noqa: BLE001
        return 0, exc
    return int(n), None


async def quota(
    client: Redis, key: str, limit: int, window_sec: float
) -> tuple[bool, Exception | None]:
    """固定窗口配额:窗口内放行 limit 次,超出拒绝。

    limit <= 0 或 window <= 0 视为不限流。Redis 故障 fail-open 返回 (True, exc)。
    """
    if limit <= 0 or window_sec <= 0:
        return True, None
    n, exc = await incr_window(client, key, window_sec)
    if exc is not None:
        return True, exc
    return n <= limit, None


class ActionQuota:
    """per-player 动作频率配额(anti-abuse §6 第 6 项)。对应 Go 的 redisx.ActionQuota。

    申请 / 邀请 / 下单 / 撤单这类**有总量闸但无频率闸**的写入面统一用它:
    总量闸只限「同时挂多少」,挡不住「下单-撤单-再下单」的循环(每轮都产生
    托管写 + 流水行)。

    各服务 biz 定义自己的小 Protocol,main 用本类一行装配 ——
    不再逐服务手写一份 data 适配(那正是 trade 的 rate_quota 至今没有实现的原因)。
    """

    __slots__ = ("_client", "_domain", "_limit", "_window_sec")

    def __init__(self, client: Redis, domain: str, limit: int, window_sec: float) -> None:
        self._client = client
        self._domain = domain
        self._limit = limit
        self._window_sec = window_sec

    async def allow(self, action: str, subject: int) -> tuple[bool, Exception | None]:
        """记一次动作并判定是否在配额内。返回 `(是否放行, 故障)` —— 对齐 Go 的 `(bool, error)`。

        ★ 故障**交回调用方**,不在这里就地吞掉。

        原实现返回裸 `bool`,自己打一条通用的 `rate_quota_unavailable` 就完事。
        后果是五个服务里那段:

            try:
                ok = await quota.allow(action, player_id)
            except BaseException as exc:
                log.warning("auction_rate_quota_check_failed", ..., err=str(exc))

        **整个 except 分支是不可达死代码** —— `allow()` 从不抛。于是
        `auction_rate_quota_check_failed` / `team_...` / `guild_...` / `friend_...`
        这四五个 Loki 告警键**在代码里存在、永远不会触发**;真正打出来的只有一条
        不带 player_id 的通用 warn。查"某个玩家被限流了还是 Redis 挂了"时,
        既定位不到服务也定位不到人。

        fail-open 的**方向**不变(§9.20:限流是背压门不是权威门,判定失败一律放行),
        变的是"谁来记这件事"。
        """
        return await quota(
            self._client, rl_key(self._domain, action, subject), self._limit, self._window_sec
        )


async def arm_penalty(client: Redis, key: str, window_sec: float) -> Exception | None:
    """布设惩罚窗(SET PX,**无条件覆盖** = 新惩罚顶替旧惩罚的剩余时长)。

    与 cooldown 的区别:cooldown 是「先到先占」的准入判定,arm_penalty 是
    「事实发生后记罚」的写入(如 no-show 判弃后的进入侧退避)。

    ★ 故障**交回调用方**(返回异常)而不是就地吞掉 —— 与 Go 的
    `ArmPenalty(...) error` 同形。吞掉的话"罚没记上"这件事**没有任何人知道**:
    惩罚窗是写入侧,写失败不像读失败那样有 fail-open 兜底,它就是真的漏了一次罚。
    """
    if window_sec <= 0:
        return None
    try:
        await client.set(key, 1, px=int(window_sec * 1000))
    except Exception as exc:  # noqa: BLE001
        return exc
    return None


async def penalty_remaining(client: Redis, key: str) -> tuple[float, Exception | None]:
    """读惩罚窗剩余秒数;key 不存在返回 0。故障返回 (0, exc) —— 调用方 Warn 后按无惩罚放行。

    与 Go 的 `PenaltyRemaining(...) (time.Duration, error)` 同形:读侧 fail-open
    是对的(罚查不到就别拦人),但**必须留证**,否则"惩罚长期失效"没有任何信号。
    """
    try:
        ms = await client.pttl(key)
    except Exception as exc:  # noqa: BLE001
        return 0.0, exc
    # PTTL 契约:-2 = key 不存在,-1 = 无过期(本模块永不产生,防御性归零)。
    return (0.0 if ms is None or ms < 0 else ms / 1000.0), None


# ─────────────────── 按配置选型的客户端 + 启动期 Ping 闸 ──────────────────────
#
# 对应 Go 的 `pkg/redisx.NewUniversalClient` + 各服务 main.go 的 redis_ping_failed 闸
# (21 个服务里 **18 个**有这道闸)。
#
# ⚠️ 这里补的是一个**实测过的静默分叉**:在此之前 `new_client(addr)` 只认单实例,
# `addrs` / `master_name` 在 Python 侧没有任何代码路径,而 Sentinel 部署常见的
# "只填 addrs、host 留空"喂给 Python 的结果是 —— 连上了 **127.0.0.1:6379**,
# 进程照常起来。Go 在同一配置下是 panic。


class RedisEndpointMissingError(ValueError):
    """既没有 host 也没有 addrs。对应 Go `svc.MustNewBaseContext` 的 panic 分支。"""


def _split_host_port(addr: str, default_port: int = 6379) -> tuple[str, int]:
    """拆 `host:port`。兼容 IPv6 字面量 `[::1]:6379`。"""
    a = addr.strip()
    if a.startswith("["):
        host, _, rest = a.partition("]")
        return host[1:], int(rest.lstrip(":") or default_port)
    host, sep, port = a.rpartition(":")
    if not sep:
        return a, default_port
    return host, int(port or default_port)


def new_universal_client(conf, *, context_timeout: bool = False):  # noqa: ANN001
    """按 conf 选型建客户端。选型规则与 go-redis 的 UniversalClient **逐条一致**:

        master_name 非空          → Sentinel 故障转移(FailoverClient)
        master_name 空且 addrs > 1 → Cluster
        其余                       → 单实例

    注意"addrs 只有 1 个仍是 standalone"这一格:go-redis 按**原始地址数量**选型,
    照抄这条是因为运维会用单元素 addrs 表示"单实例但写成列表",判成 Cluster 的话
    客户端会去发 CLUSTER SLOTS,对普通 Redis 直接报错 —— 那是响的;
    反过来把多节点判成单实例才是静默的(只连第一个节点,其余分片的 key 全部 MOVED)。

    `context_timeout` 对应 Go 的 `NewDeadlineUniversalClient`:让调用方的超时真正
    作用到 socket I/O。只给分布式锁这类"业务声明的硬等待上限必须生效"的路径用,
    不改默认行为(一次安全修复不该暗改所有服务的超时语义)。
    """
    from redis import asyncio as aioredis

    endpoints = conf.endpoints()
    if not endpoints:
        raise RedisEndpointMissingError(
            "redis endpoint required: 设 node.redis_client.host(单实例)"
            "或 node.redis_client.addrs(sentinel / cluster)。"
            "两者皆空时**不得**回落到 127.0.0.1 —— 那会让服务连上一个无关的本机 Redis "
            "并正常启动,而 Go 侧在同一配置下拒启。"
        )

    common: dict = {
        "username": conf.username or None,
        "password": conf.password or None,
        "decode_responses": False,   # 全仓大量存 protobuf bytes,自动解码会炸
    }
    for key, td in (
        ("socket_connect_timeout", conf.dial_timeout_td()),
        ("socket_timeout", conf.read_timeout_td()),
    ):
        if td.total_seconds() > 0:
            common[key] = td.total_seconds()
    if conf.pool_size > 0:
        common["max_connections"] = conf.pool_size

    if conf.master_name:
        # Sentinel:endpoints 是**哨兵**地址,不是主库地址。
        sentinel = aioredis.Sentinel(
            [_split_host_port(a, 26379) for a in endpoints],
            socket_connect_timeout=common.get("socket_connect_timeout"),
        )
        return sentinel.master_for(conf.master_name, db=conf.db, **common)

    if len(endpoints) > 1:
        from redis.asyncio.cluster import ClusterNode

        # Cluster 不支持 SELECT:非零 db 在集群模式下是配置错误,静默忽略会让
        # 所有 key 落在 db0 而运维以为做了隔离。
        if conf.db:
            raise ValueError(
                f"redis cluster 模式不支持 db={conf.db}(Redis Cluster 只有 db0);"
                "要隔离请用 key 前缀或独立集群"
            )
        return aioredis.RedisCluster(
            startup_nodes=[ClusterNode(*_split_host_port(a)) for a in endpoints],
            **common,
        )

    host, port = _split_host_port(endpoints[0])
    return aioredis.Redis(
        host=host,
        port=port,
        db=conf.db,
        # 连接空闲后被中间设备静默断开时,下次使用前先 PING;
        # 不设的话会在长空闲后收到一次莫名其妙的 ConnectionError。
        health_check_interval=30,
        **common,
    )


def new_universal_client_with_credentials(conf, username: str, password: str):  # noqa: ANN001
    """拓扑同 `new_universal_client`,但**替换**(而非回落到)conf 里的凭据。

    对应 Go 的 `pkg/redisx.NewUniversalClientWithCredentials`。

    ★ "替换而非回落"是这个函数存在的全部理由:一次性安全工具(ds_allocator 的
      pod_uid release preflight)拿的是**从写者服务配置裁出来的 endpoint-only 配置**,
      必须用专用只读 ACL 身份连接。如果实现成"username 空时回落到 conf.password",
      某天 conf 里混进写者口令就会以写权限跑审计 —— 而审计本身完全不会报错。
    ★ 用 `model_copy(update=...)` 而不是给 `new_universal_client` 加分支:选型规则
      (Sentinel / Cluster / 单实例)只有一份,不因凭据来源分叉。
    """
    return new_universal_client(conf.model_copy(update={"username": username, "password": password}))


async def must_connect(conf, *, ping_timeout_sec: float = 3.0):  # noqa: ANN001
    """建客户端并做一次**启动期 Ping**,连不上就抛。

    对应 Go 各服务 main.go 的 `redis_ping_failed` 闸(21 个服务里 18 个有)。

    为什么必须在启动期探一次:不探的话服务会带着一个死 Redis 正常 Ready,
    k8s 把流量切过来,**第一条业务命令**才暴露 —— 那时错误已经落在玩家请求上了。
    启动期失败是刺眼的、可回滚的;运行期第一条命令失败是事故。
    """
    client = new_universal_client(conf)
    try:
        await asyncio.wait_for(client.ping(), timeout=ping_timeout_sec)
    except asyncio.CancelledError:
        # ★ 取消必须穿透。它是 BaseException，会被下面那条宽 except 吞掉并翻译成
        # ConnectionError —— 于是启动期被 Ctrl-C / 被上层取消时，日志上是一条
        # **假的** `redis_ping_failed`：Redis 好好的，却告警说 ping 失败。
        # 本函数是全仓 18 个服务的强依赖入口，一条假事件名会污染所有服务的告警面。
        with _contextlib_suppress():
            await client.aclose()
        raise
    except BaseException as exc:  # noqa: BLE001
        with contextlib.suppress(Exception):
            await client.aclose()
        raise ConnectionError(
            f"redis ping failed host={conf.host!r} addrs={list(conf.addrs)!r}: {exc}"
        ) from exc
    return client
