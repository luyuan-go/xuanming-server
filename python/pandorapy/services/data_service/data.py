"""data_service 数据层 —— 对应 Go 侧 internal/data(store.go + cache.go)。

MySQL 是**事实源**,Redis 只是旁路缓存(弱一致)。

schema 唯一来源是 PlayerData proto:表名 / 主键写在 proto option 里,每个标量字段即一列。
Go 侧靠 `proto2mysql` 库(你们自己的库)做这件事;Python 侧用 pandorapy/protosql
从描述符推导 —— 见那个模块的头注释解释为什么不照抄整个库。

乐观锁语义(与 Go 逐条对齐):
    version == 0 → 新建,INSERT 起始版本 1(冲突即已存在 → ErrDataVersionMismatch)
    version  > 0 → UPDATE ... WHERE player_id=? AND version=?
                   受影响行 0(版本不匹配 / 不存在)→ ErrDataVersionMismatch

★ update_mask 为什么在更新时**必须非空**(不变量 §9.17):
    空掩码 = 全量覆盖。滚动升级期间旧副本不认得新加的列,一次全量写会把新列**清零**。
    这是零停机更新的硬约束,不是风格问题。所以空掩码更新直接拒绝。
"""

from __future__ import annotations

import datetime as _dt
from typing import Protocol

from pandora.data_service.v1 import data_service_pb2
from redis.asyncio.client import Redis

from pandorapy import errcode, mysqlx, protosql
from pandorapy import log as plog

# 表结构从 proto 描述符推导。显式传表名/主键(显式 > 隐式),与 proto option 一致。
PLAYER_DATA_SCHEMA = protosql.schema_of(
    data_service_pb2.PlayerData, table_name="player_data", primary_key="player_id"
)

PK_FIELD = "player_id"
VERSION_FIELD = "version"

# 可经 update_mask 更新的业务列 —— **从描述符动态推导**,新增 proto 字段自动纳入。
# 手工维护列表漏一个字段,那个字段就永远写不进 MySQL,而且不报错(Go 侧注释点名了这点)。
UPDATABLE_FIELDS: tuple[str, ...] = tuple(PLAYER_DATA_SCHEMA.updatable_fields(VERSION_FIELD))
_UPDATABLE_SET = frozenset(UPDATABLE_FIELDS)


def is_updatable_field(name: str) -> bool:
    """判断字段名是否是可经 update_mask 更新的业务列(非主键、非 version)。"""
    return name in _UPDATABLE_SET


def cache_key(player_id: int) -> str:
    """与 Go 侧一致:pandora:data:player:<id>。"""
    return f"pandora:data:player:{player_id}"


# ── 缓存值格式(与 Go 侧 internal/data/cache.go 逐字节一致)────────────────────
#
# 布局:魔数 4B('P','D','C',0x02)+ 位图长度 4B(big-endian uint32)+ 字段号位图 + PlayerData pb
#
# 为什么不是裸 pb —— §9 不变量 16/17 的缓存投毒防护:
#     滚动升级期新旧副本共用同一个 Redis key。旧副本读 MySQL 时只读得进它 proto 描述符里
#     认得的列,新副本刚加的新列它读不到,手上是一份「缺新列的残缺 PlayerData」。这份残缺
#     数据一旦写进共享缓存,新副本读到就等于新列被抹掉,零停机升级被破坏。所以缓存值带上
#     「写入方字段号位图」,读方只信任「写入方字段集 ⊇ 自己字段集」的条目,否则当未命中回落
#     MySQL —— 自己读库能拿到自己认得的全部列。
#
# 为什么不用「最大字段编号」当版本:编号空洞里加字段(如 {1,2,5} 加 3)最大值不变;
#     reserved 删掉最高编号字段则最大值下降,版本非单调。两类合法演进都会被漏判。
#
# 魔数的作用:把「无头的旧裸 pb / 脏字节 / 旧 0x01 版本格式」与本格式区分开。没有魔数时,
#     裸 pb 的头几个字节会被当成头部解释,剩余字节又常能被 protobuf 宽松反序列化成功,
#     结果是命中一份错误数据。魔数不符一律当未命中。
CACHE_MAGIC = b"PDC\x02"
CACHE_MAGIC_LEN = 4
CACHE_MASK_LEN_LEN = 4
CACHE_HEADER_MIN_LEN = CACHE_MAGIC_LEN + CACHE_MASK_LEN_LEN

# 魔数是 Go 侧的私有 wire 常量,pb2 生成物里没有对应符号,只能写字面量。长度写错会让整个
# 头部偏移而不会报错,就地钉死,免得两处漂开。
assert len(CACHE_MAGIC) == CACHE_MAGIC_LEN


def _compute_cache_schema_mask() -> bytes:
    """本副本 PlayerData 描述符所有字段号的位图(bit n 置位 = 字段号 n 存在)。

    从描述符推导而不是手写常量:手写的位图不会随 proto 演进,加字段后它仍是旧值,
    投毒防护就成了摆设,而且不会有任何报错。
    """
    numbers = [f.number for f in data_service_pb2.PlayerData.DESCRIPTOR.fields]
    max_num = max(numbers, default=0)
    mask = bytearray(max_num // 8 + 1)
    for n in numbers:
        mask[n // 8] |= 1 << (n % 8)
    return bytes(mask)


CACHE_SCHEMA_MASK = _compute_cache_schema_mask()


def writer_has_all_reader_fields(writer_mask: bytes, reader_mask: bytes) -> bool:
    """写入方字段集是否 ⊇ 读取方字段集(按集合包含关系逐位判断)。

    只要读取方有某个字段号写入方没有,写入方那条缓存就可能缺这一列 → 判为不可信。
    """
    for i, r in enumerate(reader_mask):
        w = writer_mask[i] if i < len(writer_mask) else 0
        if r & ~w & 0xFF:  # reader 有置位而 writer 缺
            return False
    return True


class PlayerStore(Protocol):
    async def read(self, player_id: int): ...
    async def write(self, pd, update_fields: list[str]) -> int: ...


class PlayerCache(Protocol):
    async def get(self, player_id: int) -> tuple[object | None, bool]: ...
    async def set(self, pd, ttl: _dt.timedelta) -> None: ...
    async def delete(self, player_id: int) -> None: ...


class MySQLPlayerStore:
    """基于 asyncmy / aiomysql 的 PlayerStore。"""

    __slots__ = ("_pool",)

    def __init__(self, pool) -> None:  # noqa: ANN001
        self._pool = pool

    async def ensure_schema(self) -> None:
        """按 pb 建表。用户已确认「没上线、库可清空」,故只建不做增量同步(§15.3)。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(PLAYER_DATA_SCHEMA.create_table_sql())
            await conn.commit()

    async def read(self, player_id: int):
        """读玩家数据。不存在返回 None(对应 Go 的 (nil, false, nil))。"""
        cols = ", ".join(f"`{c}`" for c in PLAYER_DATA_SCHEMA.column_names())
        sql = f"SELECT {cols} FROM `{PLAYER_DATA_SCHEMA.table_name}` WHERE `{PK_FIELD}` = %s"  # noqa: S608
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(sql, (player_id,))
                row = await cur.fetchone()
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "read player_data %d: %s", player_id, exc
            ) from exc
        if row is None:
            return None
        pd = data_service_pb2.PlayerData()
        for name, value in zip(PLAYER_DATA_SCHEMA.column_names(), row, strict=True):
            if value is not None:
                setattr(pd, name, value)
        return pd

    async def write(self, pd, update_fields: list[str]) -> int:
        """乐观锁写。返回写入后的新版本号(= 期望版本 + 1)。**不修改入参 pd**。"""
        expect = pd.version
        if expect == 0:
            return await self._insert(pd)
        return await self._update(pd, update_fields)

    async def _insert(self, pd) -> int:
        """新建:整条 INSERT,起始版本 1。主键冲突 → ErrDataVersionMismatch(已存在)。"""
        names = PLAYER_DATA_SCHEMA.column_names()
        placeholders = ", ".join(["%s"] * len(names))
        cols = ", ".join(f"`{n}`" for n in names)
        sql = f"INSERT INTO `{PLAYER_DATA_SCHEMA.table_name}` ({cols}) VALUES ({placeholders})"  # noqa: S608
        values = [1 if n == VERSION_FIELD else getattr(pd, n) for n in names]
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(sql, values)
                await conn.commit()
        except Exception as exc:  # noqa: BLE001
            if mysqlx.is_duplicate_entry(exc):
                # 已存在 —— 对调用方就是"版本不匹配"(它以为是新建,实际不是)。
                raise errcode.PandoraError(
                    errcode.ErrDataVersionMismatch,
                    "player_data %d already exists",
                    pd.player_id,
                ) from exc
            raise errcode.PandoraError(
                errcode.ErrInternal, "insert player_data %d: %s", pd.player_id, exc
            ) from exc
        return 1

    async def _update(self, pd, update_fields: list[str]) -> int:
        """CAS 更新:只 SET 掩码内的列,version 单独 +1。

        调用方(biz)须已校验 update_fields 合法(非空、不含主键/version/未知字段)。
        这里再挡一道 —— 数据层不信任调用方是防御性编程的正当用法,
        因为拼进 SQL 的列名如果没校验就是注入面。
        """
        safe = [f for f in update_fields if is_updatable_field(f)]
        if not safe or len(safe) != len(update_fields):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "invalid update_mask for player_data %d",
                pd.player_id,
            )
        assignments = ", ".join(f"`{f}` = %s" for f in safe)
        sql = (
            f"UPDATE `{PLAYER_DATA_SCHEMA.table_name}` "  # noqa: S608
            f"SET {assignments}, `{VERSION_FIELD}` = `{VERSION_FIELD}` + 1 "
            f"WHERE `{PK_FIELD}` = %s AND `{VERSION_FIELD}` = %s"
        )
        values = [getattr(pd, f) for f in safe] + [pd.player_id, pd.version]
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(sql, values)
                affected = cur.rowcount
                await conn.commit()
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "update player_data %d: %s", pd.player_id, exc
            ) from exc
        if not affected:
            # 版本不匹配或行不存在 —— 两者对调用方是同一种处置(重读再试)。
            raise errcode.PandoraError(
                errcode.ErrDataVersionMismatch,
                "player_data %d version mismatch (expect %d)",
                pd.player_id,
                pd.version,
            )
        return pd.version + 1


class RedisPlayerCache:
    """Redis 旁路缓存,存「魔数 + 字段号位图 + protobuf bytes」。对应 Go 侧 cache.go。

    字节格式必须与 Go 逐字节一致:两边写的是**同一个** Redis key,格式一旦不同,
    Go 读 Python 写的条目判 miss、Python 读 Go 写的条目解不开,双向命中率塌成 0 且零信号。
    """

    __slots__ = ("_rdb",)

    def __init__(self, rdb: Redis) -> None:
        self._rdb = rdb

    async def get(self, player_id: int) -> tuple[object | None, bool]:
        """返回 (数据, 是否命中)。格式不符 / 字段集不够 / 坏档一律当 miss 回落 MySQL。"""
        raw = await self._rdb.get(cache_key(player_id))
        if raw is None:
            return None, False
        # 头部不足或魔数不符 = 旧裸 pb / 脏字节,不是本格式 → 当未命中,且**不打日志**:
        # 滚动升级期新旧格式交叉读是预期内的,只会多打几次 MySQL,刷日志反而淹掉真信号。
        if len(raw) < CACHE_HEADER_MIN_LEN or raw[:CACHE_MAGIC_LEN] != CACHE_MAGIC:
            return None, False
        mask_len = int.from_bytes(raw[CACHE_MAGIC_LEN:CACHE_HEADER_MIN_LEN], "big")
        header_len = CACHE_HEADER_MIN_LEN + mask_len
        if len(raw) < header_len:
            # 位图长度越界 → 脏数据。
            return None, False
        writer_mask = raw[CACHE_HEADER_MIN_LEN:header_len]
        # 写入方字段集必须 ⊇ 本副本字段集,否则这条可能缺本副本认得的列(投毒防护)。
        if not writer_has_all_reader_fields(writer_mask, CACHE_SCHEMA_MASK):
            return None, False
        pd = data_service_pb2.PlayerData()
        try:
            pd.ParseFromString(raw[header_len:])
        except Exception as exc:  # noqa: BLE001
            # 魔数与位图都过了还解不开 = 真坏档,不是滚动升级期的旧格式。行为仍是 miss,
            # 但必须留痕:静默吞掉只表现为命中率下降,排障时零线索。
            plog.get().warning(
                "player_cache_corrupt_entry",
                player_id=player_id,
                reason="unmarshal_failed",
                err=str(exc),
                bytes=len(raw),
            )
            return None, False
        if pd.player_id != player_id:
            # 串号是缓存投毒 / 键错配的强信号,比坏档更严重,必须点名。行为仍是 miss。
            plog.get().warning(
                "player_cache_corrupt_entry",
                player_id=player_id,
                reason="player_id_mismatch",
                cached_player_id=pd.player_id,
            )
            return None, False
        return pd, True

    async def set(self, pd, ttl: _dt.timedelta) -> None:
        # 头部写入本副本的字段号位图,供读方判断写入方字段集是否 ⊇ 自己。
        buf = b"".join(
            (
                CACHE_MAGIC,
                len(CACHE_SCHEMA_MASK).to_bytes(CACHE_MASK_LEN_LEN, "big"),
                CACHE_SCHEMA_MASK,
                pd.SerializeToString(),
            )
        )
        ms = int(ttl.total_seconds() * 1000)
        await self._rdb.set(cache_key(pd.player_id), buf, px=max(ms, 1))

    async def delete(self, player_id: int) -> None:
        await self._rdb.delete(cache_key(player_id))
