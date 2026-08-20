"""data → biz 的内部行结构 —— 对应 Go 侧 internal/data 里的 *Row 结构体。

单独一个模块是为了打断 cache ↔ repo 的循环 import(Go 侧它们同 package,
Python 侧分文件后 cache 要用 GuildRow、repo 也要用,谁 import 谁都成环)。

★ 这些是**存储行**,不是客户端可见结构(§9.14)。RPC 只回 guildv1.Guild /
  GuildMember / GuildJoinRequest,由 biz 从这里的行组装最小视图 ——
  存储行直接外发会把内部字段(如未来的审计列)一起送出去。
"""

from __future__ import annotations

import dataclasses

# 公会职位(与 proto GuildRole 数值一致)。
# ★ 从生成物取,不抄字面量:role 列的值会原样出现在 ListMembers 应答里,
#   而且权限判定(leader/officer 才能审批)全靠这三个数 —— 抄错一位
#   要么客户端看到 UNSPECIFIED,要么普通成员拿到审批权。
from pandora.guild.v1 import guild_pb2 as _guild_pb2

GUILD_ROLE_LEADER = _guild_pb2.GUILD_ROLE_LEADER
GUILD_ROLE_OFFICER = _guild_pb2.GUILD_ROLE_OFFICER
GUILD_ROLE_MEMBER = _guild_pb2.GUILD_ROLE_MEMBER

# 加入申请状态(与 proto GuildJoinStatus 数值一致)。
JOIN_STATUS_PENDING = _guild_pb2.GUILD_JOIN_STATUS_PENDING
JOIN_STATUS_APPROVED = _guild_pb2.GUILD_JOIN_STATUS_APPROVED
JOIN_STATUS_REJECTED = _guild_pb2.GUILD_JOIN_STATUS_REJECTED


@dataclasses.dataclass(slots=True)
class GuildRow:
    guild_id: int
    name: str
    leader_id: int
    member_count: int
    max_members: int
    created_ms: int


@dataclasses.dataclass(slots=True)
class GuildMemberRow:
    player_id: int
    guild_id: int
    role: int
    joined_ms: int


@dataclasses.dataclass(slots=True)
class GuildJoinRequestRow:
    request_id: int
    guild_id: int
    player_id: int
    status: int
    created_ms: int


@dataclasses.dataclass(slots=True)
class GroupRow:
    group_id: int
    name: str
    owner_id: int
    member_count: int
    max_members: int
    created_ms: int


@dataclasses.dataclass(slots=True)
class GroupMemberRow:
    group_id: int
    player_id: int
    role: int
    joined_ms: int
