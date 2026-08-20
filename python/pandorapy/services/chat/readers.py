"""队伍 / 公会 / 群成员解析的 gRPC 客户端 —— 对应 Go 侧 internal/data 的
team_reader.go / guild_reader.go / group_reader.go。

三者形状完全相同(拨一个内网地址、调一个 List/Get、把成员摊成 player_id 列表),
差别只有"拨哪个地址、调哪个 stub、读哪个字段",所以合成一个基类 + 三个薄壳。

★ 返回契约必须与 Go 逐条对齐 —— biz 对这三种结果的处理完全不同:

    (ids, True)   正常。发送者必须在 ids 里才能在该频道说话。
    ([], False)   服务答了但 code != OK / 队伍不存在 → biz 报 ErrChatChannelInvalid。
    **抛异常**     服务不可达(拨不通 / 超时)→ biz 报 ErrUnavailable 让客户端重试。

  把第三种压成 ([], False) 是最容易犯的错:那会让"公会服务挂了"表现成
  "你不在这个公会",玩家看到的是一句莫名其妙的拒绝而不是"稍后重试",
  而运维侧没有任何 chat 侧的不可达信号。

★ GroupService 与 GuildService **同进程**,共用 cfg.chat.guild_addr —— 但仍然各自
  拨一条连接(与 Go 一致:两个 reader 各建一个 ClientConn)。合成一条连接不会更快
  (gRPC 本来就多路复用),却会让"关掉一个 reader 顺手把另一个也关了"成为可能。

⚠️ 公会 ListMembers 在协议上是**分页**的(cursor/limit,服务端默认 50、上限 100),
   而 Go 侧 GetGuildMembers 只取第一页、不翻页。这里**照抄 Go 的行为**:
   补上翻页会让 Python 副本给大公会扇出更多人,同一条消息在两个副本上收件人不同。
   要改必须两栈一起改(见交付说明 honest_gaps)。
"""

from __future__ import annotations

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.group.v1 import group_pb2, group_pb2_grpc
from pandora.guild.v1 import guild_pb2, guild_pb2_grpc
from pandora.team.v1 import team_pb2, team_pb2_grpc

# 成员解析的单次 RPC 超时。
#
# ★ 必须给超时:成员解析在 SendMessage 的同步路径上,对端卡住而不断连时,
# 没有超时的话这条 RPC 会一直挂着 —— 表现是玩家点了发送之后界面卡死,
# 而 chat 侧看不到任何错误(它确实还在"等")。有超时才会翻成 ErrUnavailable。
MEMBER_RPC_TIMEOUT_SEC = 5.0


class _GrpcMemberReader:
    """三个 reader 的共同骨架。子类只提供 stub 构造与一次调用。"""

    __slots__ = ("_channel",)

    def __init__(self, addr: str) -> None:
        # insecure:内网直连,与 Go 的 grpcclient.MustDialInsecure 一致。
        self._channel = grpc.aio.insecure_channel(addr)

    async def close(self) -> None:
        await self._channel.close()

    async def members(self, container_id: int) -> tuple[list[int], bool]:
        raise NotImplementedError


class GrpcTeamReader(_GrpcMemberReader):
    """队伍成员解析。对应 Go 的 data.GrpcTeamReader。"""

    __slots__ = ("_stub",)

    def __init__(self, addr: str) -> None:
        super().__init__(addr)
        self._stub = team_pb2_grpc.TeamServiceStub(self._channel)

    async def members(self, container_id: int) -> tuple[list[int], bool]:
        resp = await self._stub.GetTeam(
            team_pb2.GetTeamRequest(team_id=container_id), timeout=MEMBER_RPC_TIMEOUT_SEC
        )
        # ★ 判据是 `code != OK` **或** team 字段缺席(与 Go 的 resp.GetTeam() == nil 同)。
        # 只判 code 的话,一个"OK 但没带 team"的应答会被当成"队伍存在且零成员" ——
        # 于是发送者被判为非成员,报的却是"你不在这个队伍"。
        if resp.code != errcode_pb2.OK or not resp.HasField("team"):
            return [], False
        return [m.player_id for m in resp.team.members], True


class GrpcGuildReader(_GrpcMemberReader):
    """公会成员解析。对应 Go 的 data.GrpcGuildReader。"""

    __slots__ = ("_stub",)

    def __init__(self, addr: str) -> None:
        super().__init__(addr)
        self._stub = guild_pb2_grpc.GuildServiceStub(self._channel)

    async def members(self, container_id: int) -> tuple[list[int], bool]:
        resp = await self._stub.ListMembers(
            guild_pb2.ListMembersRequest(guild_id=container_id),
            timeout=MEMBER_RPC_TIMEOUT_SEC,
        )
        if resp.code != errcode_pb2.OK:
            return [], False
        return [m.player_id for m in resp.members], True


class GrpcGroupReader(_GrpcMemberReader):
    """临时群成员解析。对应 Go 的 data.GrpcGroupReader(拨的仍是 guild_addr)。"""

    __slots__ = ("_stub",)

    def __init__(self, addr: str) -> None:
        super().__init__(addr)
        self._stub = group_pb2_grpc.GroupServiceStub(self._channel)

    async def members(self, container_id: int) -> tuple[list[int], bool]:
        resp = await self._stub.ListGroupMembers(
            group_pb2.ListGroupMembersRequest(group_id=container_id),
            timeout=MEMBER_RPC_TIMEOUT_SEC,
        )
        if resp.code != errcode_pb2.OK:
            return [], False
        return [m.player_id for m in resp.members], True
