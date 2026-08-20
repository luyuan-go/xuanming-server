"""auction MySQL 分片拓扑的持久启动门禁 —— 对应 Go 侧 internal/data/shard_topology.go。

market_id / owner_id 都按 `id % N` 路由。N、逻辑下标或 DSN 顺序一旦漂移,历史数据就会
被路由到别的库 —— 表现是**订单凭空消失**(查得到 registry、查不到订单行),
而且不报错。所以每个物理 shard 上都写一行 marker,启动时逐片 exact-match 比对。

★ 三条最容易写歪的地方:

  1. identity 的**材料与顺序**是契约:`network \\x00 lower(addr) \\x00 dbname` 再 sha256。
     Go 已经把这个值写进了生产库,Python 算出别的值 = 每次启动都判"漂移"而拒启;
     更糟的是 bootstrap 开着时会把 marker 覆盖成 Go 再也认不出来的值。
  2. `allow_bootstrap` **只授权"所有 marker 均不存在"的首次登记**。
     已有 marker 不一致时它也绝不允许覆盖 —— 那正是它要防的事。
  3. 部分 marker 存在时可以安全补齐缺片:已有的 topology_hash 已经锁定了完整有序身份,
     补齐等于把并发启动 / 上次跨库登记中途退出的残局收敛掉。

marker 表由版本化迁移创建;本模块**不偷偷建表**(偷偷建表 = 把"迁移没跑"这个事实
藏起来,下一次真正的 schema 变更会撞上一张手工建的表)。
"""

from __future__ import annotations

import dataclasses
import hashlib

from pandorapy import mysqlx

SINGLETON_ID = 1

_TOPOLOGY_PREFIX = "auction-shard-topology-v1"
_TOKEN_EXTRA_CHARS = frozenset("-_.")


class ShardTopologyError(RuntimeError):
    """拓扑校验失败。启动期 fail-fast,绝不降级继续跑。"""


@dataclasses.dataclass(frozen=True, slots=True)
class ExpectedTopology:
    generation: str
    topology_hash: str
    count: int
    identities: tuple[str, ...]


def _is_token_char(c: str) -> bool:
    return c.isascii() and (c.isalnum() or c in _TOKEN_EXTRA_CHARS)


def shard_identity(dsn: str) -> str:
    """单个 DSN 的逻辑库身份哈希。对应 Go 里那段 identityMaterial + sha256。"""
    conn = mysqlx.parse_go_dsn(dsn.strip())
    if not conn["db"]:
        raise ShardTopologyError("auction shard DSN must select a database")
    # Go 的 mysql.ParseDSN 在 Net 为空时归一成 "tcp",并给 Addr 补默认端口;
    # parse_go_dsn 已经补过端口(默认 3306),这里只需拼回同样的形状。
    #
    # ★ network 必须取**真实值**,不能写死 "tcp"。身份哈希是分片拓扑的去重与
    # 防漂移判据(两个 DSN 撞同一 identity 会被拒批)。写死之后,同一份配置在
    # Go 与 Python 两栈算出**不同的哈希** —— 拓扑代际校验会在切换时无缘无故
    # 失败,而两边配置一个字节都没差。unix 档的 Addr 是 socket 路径本身,
    # 不带端口(Go 侧同形)。
    net = conn.get("net") or "tcp"
    addr = (conn["host"] if net == "unix" else f"{conn['host']}:{conn['port']}").lower()
    material = net + "\x00" + addr + "\x00" + conn["db"]
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def build_expected(generation: str, dsns: list[str]) -> ExpectedTopology:
    """按 generation + 有序 DSN 列表算出期望拓扑。对应 Go 的 buildExpectedShardTopology。"""
    generation = (generation or "").strip()
    if not generation or len(generation) > 64:
        raise ShardTopologyError("auction shard topology generation must be 1..64 characters")
    for c in generation:
        if not _is_token_char(c):
            raise ShardTopologyError(
                f"auction shard topology generation contains unsupported character {c!r}"
            )
    if not dsns:
        raise ShardTopologyError("auction shard topology requires at least one DSN")

    identities: list[str] = []
    seen: dict[str, int] = {}
    for index, dsn in enumerate(dsns):
        try:
            identity = shard_identity(dsn)
        except ShardTopologyError as exc:
            raise ShardTopologyError(f"auction shard DSN index={index}: {exc}") from exc
        if identity in seen:
            raise ShardTopologyError(
                f"auction shard DSNs {seen[identity]} and {index} resolve to the same "
                f"logical database identity"
            )
        seen[identity] = index
        identities.append(identity)

    material = _TOPOLOGY_PREFIX + "\x00" + generation + "\x00" + "\x00".join(identities)
    return ExpectedTopology(
        generation=generation,
        topology_hash=hashlib.sha256(material.encode("utf-8")).hexdigest(),
        count=len(identities),
        identities=tuple(identities),
    )


async def _read_stored(pool) -> tuple | None:  # noqa: ANN001
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT topology_generation, topology_hash, shard_count, shard_index, "
                "shard_identity_hash FROM auction_shard_topology WHERE singleton_id = %s LIMIT 1",
                (SINGLETON_ID,),
            )
            return await cur.fetchone()


async def _insert_stored(pool, expected: ExpectedTopology, index: int) -> None:  # noqa: ANN001
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT IGNORE INTO auction_shard_topology "
                "(singleton_id, topology_generation, topology_hash, shard_count, shard_index, "
                "shard_identity_hash) VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    SINGLETON_ID,
                    expected.generation,
                    expected.topology_hash,
                    expected.count,
                    index,
                    expected.identities[index],
                ),
            )


def _validate_stored(stored: tuple, expected: ExpectedTopology, index: int) -> None:
    generation = _as_text(stored[0])
    topology_hash = _as_text(stored[1])
    count = int(stored[2])
    stored_index = int(stored[3])
    identity = _as_text(stored[4])
    if (
        generation != expected.generation
        or topology_hash != expected.topology_hash
        or count != expected.count
        or stored_index != index
        or identity != expected.identities[index]
    ):
        raise ShardTopologyError(
            f"auction shard topology mismatch index={index}: "
            f"stored generation={generation!r} hash={topology_hash} count={count} "
            f"index={stored_index} identity={identity}; "
            f"expected generation={expected.generation!r} hash={expected.topology_hash} "
            f"count={expected.count} index={index} identity={expected.identities[index]}"
        )


def _as_text(value: object) -> str:
    return value if isinstance(value, str) else bytes(value).decode()  # type: ignore[arg-type]


async def validate_shard_topology(
    pools: list, generation: str, dsns: list[str], allow_bootstrap: bool
) -> None:
    """校验并在允许时首次登记有序物理分片拓扑。对应 Go 的 ValidateShardTopology。

    pools 的顺序**必须**与 dsns 一致(main 里由同一个列表派生),否则 shard_index
    会与实际库对不上 —— 那正是本门禁要抓的漂移,自己制造一份就毫无意义了。
    """
    expected = build_expected(generation, dsns)
    if len(pools) != expected.count:
        raise ShardTopologyError(
            f"auction shard topology router count={len(pools)}, dsn count={expected.count}"
        )

    present = [False] * expected.count
    for index, pool in enumerate(pools):
        try:
            stored = await _read_stored(pool)
        except ShardTopologyError:
            raise
        except Exception as exc:  # noqa: BLE001 —— 读失败要带上分片下标才排得动
            raise ShardTopologyError(f"read auction shard topology index={index}: {exc}") from exc
        if stored is None:
            continue
        _validate_stored(stored, expected, index)
        present[index] = True

    if all(present):
        return
    if not any(present) and expected.count > 1 and not allow_bootstrap:
        raise ShardTopologyError(
            f"auction shard topology is uninitialized for {expected.count} shards; "
            f"set allow_shard_topology_bootstrap=true for the reviewed first start only"
        )

    # 单库首次升级可自动登记;双分片要求显式首次授权(上面那道闸)。
    for index, pool in enumerate(pools):
        if present[index]:
            continue
        try:
            await _insert_stored(pool, expected, index)
        except Exception as exc:  # noqa: BLE001
            raise ShardTopologyError(
                f"initialize auction shard topology index={index}: {exc}"
            ) from exc
        stored = await _read_stored(pool)
        if stored is None:
            raise ShardTopologyError(
                f"verify auction shard topology index={index} found=False"
            )
        _validate_stored(stored, expected, index)
