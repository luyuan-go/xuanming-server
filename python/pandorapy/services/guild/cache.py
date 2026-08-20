"""guild 的 Redis 读缓存(cache-aside)—— 对应 Go 侧 internal/data/cache.go。

公会是「全服共享」数据:同一公会的资料(名字 / 会长 / 人数)被全体成员反复读,
单 MySQL 上量后热行 / 热索引会被打爆。

两类 key(hashtag 括业务 ID,兼容 Redis Cluster / 单元化):

    pandora:guild:info:{guild_id}    → guildv1.Guild proto 快照
    pandora:guild:member:{player_id} → 玩家所属 guild_id 反查

一致性:
  - MySQL 是唯一事实源;缓存 miss / 反序列化失败 → 未命中,回落 MySQL,**不报错给上层**。
  - 写路径先写 MySQL 事务、后删缓存;删失败仅告警,靠短 TTL 兜底。
  - 只读缓存,不做 write-behind 脏写回(与不停服排空冲突,§9.16)。
  - member 反查**只缓存正向映射**(已在某公会),不做负缓存 ——
    否则入会后旧的「不在公会」负结果会在 TTL 内继续骗人。

★ 滚动升级缓存投毒防护(§9.16/§9.17,与 Go 逐字节同格式):

    guilds 若在某版本加列(列 + GuildRow + guildv1.Guild + SELECT 四处同改),
    滚动窗口里新旧副本同时在线 —— 旧副本读库时 SELECT 没有新列,写出的缓存快照
    **缺新字段**,新副本读到就丢字段(且是静默的:proto 缺字段解出零值)。
    所以 info 缓存值带一个「写入方字段号位图」,读取时只信任
    「写入方字段集 ⊇ 本副本字段集」的条目,否则当未命中回落 MySQL。

    用位图而不是"最大字段编号":后者漏掉两种情形 —— 在编号空洞里新增字段、
    以及 reserved 删掉最高号。两种都会让位图敏感、最大编号不敏感。

    值布局:魔数(4B) + 位图长度(big-endian uint32,4B) + 位图 + Guild pb
    member 反查值只是个标量 guild_id(语义不演进),只有魔数 + 定长校验。
"""

from __future__ import annotations

import struct

from pandora.guild.v1 import guild_pb2

from pandorapy.services.guild.rows import GuildRow

# 魔数区分「带位图头的新格式」与旧裸 pb / 脏数据。末字节 0x01 = 格式版本。
_GUILD_INFO_MAGIC = b"PGC\x01"
_GUILD_MEMBER_MAGIC = b"PGM\x01"

_MAGIC_LEN = 4
_MASK_LEN_LEN = 4
_HEADER_MIN_LEN = _MAGIC_LEN + _MASK_LEN_LEN
_MEMBER_VALUE_LEN = _MAGIC_LEN + 8  # 魔数 + 8B big-endian guild_id


def _compute_schema_mask() -> bytes:
    """本副本 guildv1.Guild 描述符所有字段号的位图(bit n = 字段号 n 存在)。"""
    numbers = [f.number for f in guild_pb2.Guild.DESCRIPTOR.fields]
    max_num = max(numbers) if numbers else 0
    mask = bytearray(max_num // 8 + 1)
    for n in numbers:
        mask[n // 8] |= 1 << (n % 8)
    return bytes(mask)


GUILD_CACHE_SCHEMA_MASK = _compute_schema_mask()


def writer_has_all_reader_fields(writer_mask: bytes, reader_mask: bytes) -> bool:
    """写入方位图是否 ⊇ 读取方位图。只要读取方有某位而写入方没有 → 不可信。"""
    for i in range(len(reader_mask)):
        w = writer_mask[i] if i < len(writer_mask) else 0
        if reader_mask[i] & ~w:
            return False
    return True


def guild_info_key(guild_id: int) -> str:
    return f"pandora:guild:info:{{{guild_id}}}"


def guild_member_key(player_id: int) -> str:
    return f"pandora:guild:member:{{{player_id}}}"


def _row_to_proto(g: GuildRow) -> guild_pb2.Guild:
    return guild_pb2.Guild(
        guild_id=g.guild_id,
        name=g.name,
        leader_id=g.leader_id,
        member_count=g.member_count,
        max_members=g.max_members,
        created_ms=g.created_ms,
    )


def _proto_to_row(p: guild_pb2.Guild) -> GuildRow:
    return GuildRow(
        guild_id=p.guild_id,
        name=p.name,
        leader_id=p.leader_id,
        member_count=p.member_count,
        max_members=p.max_members,
        created_ms=p.created_ms,
    )


class RedisGuildCache:
    """基于 redis-py 的 GuildCache 实现。对应 Go 的 `RedisGuildCache`。

    方法只在**Redis 本身出错**时抛异常(由 biz 记录后降级直连 MySQL);
    脏数据 / 格式不符一律当未命中返回,不抛 —— 缓存里有坏数据不该变成业务错误。
    """

    __slots__ = ("_rdb",)

    def __init__(self, rdb) -> None:  # noqa: ANN001
        self._rdb = rdb

    async def get_guild(self, guild_id: int) -> GuildRow | None:
        b = await self._rdb.get(guild_info_key(guild_id))
        if b is None:
            return None
        if isinstance(b, str):
            b = b.encode("utf-8", "surrogateescape")
        if len(b) < _HEADER_MIN_LEN or b[:_MAGIC_LEN] != _GUILD_INFO_MAGIC:
            return None  # 旧裸 pb / 脏数据 → 当未命中
        mask_len = struct.unpack(">I", b[_MAGIC_LEN:_HEADER_MIN_LEN])[0]
        header_len = _HEADER_MIN_LEN + mask_len
        if len(b) < header_len:
            return None
        if not writer_has_all_reader_fields(b[_HEADER_MIN_LEN:header_len], GUILD_CACHE_SCHEMA_MASK):
            # 写入方字段集缺本副本认得的列 → 条目可能残缺(缓存投毒防护),当未命中。
            return None
        p = guild_pb2.Guild()
        try:
            p.ParseFromString(bytes(b[header_len:]))
        except Exception:  # noqa: BLE001 —— 反序列化失败当未命中,不报错给上层
            return None
        # 防御纵深:解出的 guild_id 必须与 key 一致,否则视为脏 / 串号数据。
        if p.guild_id != guild_id:
            return None
        return _proto_to_row(p)

    async def set_guild(self, g: GuildRow, ttl_sec: float) -> None:
        body = _row_to_proto(g).SerializeToString()
        buf = (
            _GUILD_INFO_MAGIC
            + struct.pack(">I", len(GUILD_CACHE_SCHEMA_MASK))
            + GUILD_CACHE_SCHEMA_MASK
            + body
        )
        # ★ 必须带 TTL:写后删失败时,短 TTL 是唯一的兜底。
        await self._rdb.set(guild_info_key(g.guild_id), buf, ex=max(1, int(ttl_sec)))

    async def del_guild(self, guild_id: int) -> None:
        await self._rdb.delete(guild_info_key(guild_id))

    async def get_member_guild_id(self, player_id: int) -> int:
        """未命中返回 0。"""
        b = await self._rdb.get(guild_member_key(player_id))
        if b is None:
            return 0
        if isinstance(b, str):
            b = b.encode("utf-8", "surrogateescape")
        if len(b) != _MEMBER_VALUE_LEN or b[:_MAGIC_LEN] != _GUILD_MEMBER_MAGIC:
            return 0
        guild_id = struct.unpack(">Q", b[_MAGIC_LEN:])[0]
        # 0 非法(不做负缓存),当未命中。
        return int(guild_id)

    async def set_member_guild_id(self, player_id: int, guild_id: int, ttl_sec: float) -> None:
        if guild_id == 0:
            return  # 不缓存「不在公会」负结果
        buf = _GUILD_MEMBER_MAGIC + struct.pack(">Q", guild_id)
        await self._rdb.set(guild_member_key(player_id), buf, ex=max(1, int(ttl_sec)))

    async def del_member(self, player_id: int) -> None:
        await self._rdb.delete(guild_member_key(player_id))
