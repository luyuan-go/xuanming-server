"""mail 数据层(MySQL)—— 对应 Go 侧 internal/data/mail_repo.go。

库表(deploy/mysql-init/12-mail-tables.sql,pandora_social 库):

    sys_mail             系统邮件一份(PK mail_id snowflake,channel 内递增)
    guild_mail           公会邮件一份(PK mail_id;idx guild_id)
    player_mail          个人收件箱(PK mail_id;idx player_id+status,写扩散)
    player_mail_cursor   系统/公会拉取游标(PK player_id)
    player_mail_claim    附件领取幂等 + DS 领取意图(PK player_id+mail_id)
    player_mail_archive  过期未领附件归档

系统/公会邮件 = channel + watermark 拉取(零写扩散,僵尸号不登录即零成本);
个人邮件 = 写扩散(离线可达)。正文 + 附件序列化成 MailContentStorageRecord
存 payload blob(§5.8),data 层只搬字节不解包。

★ 返回值约定:Go 用 `(value, found, err)`,Python 用 `value | None` + 抛异常。
  凡是"没这行"的地方一律返回 None,**绝不返回空 bytes** —— payload 允许是 b"",
  用真值判断会把"空邮件"和"没这封邮件"混成一个,领取侧就会把不存在的邮件
  当成"没有附件"。所有调用点必须写 `is None`。
"""

from __future__ import annotations

import asyncio

import contextlib
import dataclasses

from pandora.mail.v1 import mail_pb2

from pandorapy import errcode

# 个人邮件状态。★ 直接引 proto 枚举,不手抄数值 —— DB 的 player_mail.status
# 与 proto MailStatus 是**同一套数值**,抄一遍就多一个会漂移的真相。
STATUS_UNREAD: int = mail_pb2.MAIL_STATUS_UNREAD
STATUS_READ: int = mail_pb2.MAIL_STATUS_READ
STATUS_CLAIMED: int = mail_pb2.MAIL_STATUS_CLAIMED


@dataclasses.dataclass(slots=True)
class MailRow:
    """一行邮件(任意 channel;data → biz 的内部结构,不外露客户端)。"""

    mail_id: int
    payload: bytes
    status: int = 0  # 仅个人邮件有意义;系统/公会由 biz 按游标 / claim 推导
    claimed: bool = False
    created_ms: int = 0
    expire_ms: int = 0  # 个人邮件
    start_ms: int = 0  # 系统/公会邮件
    end_ms: int = 0  # 系统/公会邮件


@dataclasses.dataclass(slots=True)
class ExpiredPersonalRow:
    """sweep 捞出的过期个人邮件(含收件人;biz 解 payload 决定归档还是直删)。"""

    mail_id: int
    player_id: int
    status: int
    expire_ms: int
    created_ms: int
    payload: bytes


class MySQLMailRepo:
    """基于 asyncmy / aiomysql 连接池的邮件仓储。对应 Go 的 MySQLMailRepo。

    ★ 连接池必须以 **autocommit=True** 建(见 main.py),这不是随手选的:

      Go 侧 database/sql 的默认就是 autocommit,显式事务只在 BeginTx 里。
      Python 这边若把池建成 autocommit=False,MySQL 会在**每条 SELECT** 上隐式开启
      一个事务并一直挂着(没人 COMMIT/ROLLBACK)。连接归池后被下一个请求复用时,
      REPEATABLE READ 的快照仍是上次那一刻的 —— **读到任意陈旧的数据,且零报错**。
      表现是"刚发出去的邮件在列表里看不到,重试几次又出现了"。

      所以:单语句写入直接 execute(由 autocommit 落盘,不需要也不该再发 COMMIT);
      需要原子性的地方(收件箱上限、归档+删除)显式 conn.begin() 包起来。
    """

    __slots__ = ("_pool",)

    def __init__(self, pool) -> None:  # noqa: ANN001 —— asyncmy / aiomysql 池
        self._pool = pool

    # ── 读:列表与游标 ────────────────────────────────────────────────────

    async def get_cursor(self, player_id: int) -> tuple[int, int]:
        """读系统 / 公会 watermark 游标。无行 = (0, 0)(新玩家从头拉)。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(_SQL_GET_CURSOR, (player_id,))
                row = await cur.fetchone()
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "get cursor %d: %s", player_id, exc
                ) from exc
        if row is None:
            return 0, 0
        return int(row[0]), int(row[1])

    async def get_player_guild(self, player_id: int) -> int | None:
        """读玩家当前所属公会(guild_members 由 guild 服务写)。无行 = None。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(_SQL_GET_PLAYER_GUILD, (player_id,))
                row = await cur.fetchone()
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "get player guild %d: %s", player_id, exc
                ) from exc
        return int(row[0]) if row is not None else None

    async def list_personal(
        self, player_id: int, now_ms: int, before_id: int, limit: int
    ) -> list[MailRow]:
        """倒序拉个人邮件。before_id=0 取首页,>0 取 mail_id < before_id。

        过期行(expire_ms 已过)在这里就被 SQL 滤掉 —— 它们要等 sweep 的缓冲期
        才物理删除,期间必须对玩家不可见,否则玩家能看到一封点开就报错的邮件。
        """
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(
                    _SQL_LIST_PERSONAL, (player_id, now_ms, before_id, before_id, limit)
                )
                rows = await cur.fetchall()
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "list personal %d: %s", player_id, exc
                ) from exc
        return [
            MailRow(
                mail_id=int(r[0]),
                status=int(r[1]),
                claimed=int(r[2]) != 0,
                expire_ms=int(r[3] or 0),
                created_ms=int(r[4] or 0),
                payload=_as_bytes(r[5]),
            )
            for r in rows
        ]

    async def list_sys_since(self, last_sys: int, now_ms: int) -> list[MailRow]:
        """拉 mail_id > last_sys 且当前生效的系统邮件(watermark 增量)。"""
        return await self._list_channel(
            _SQL_LIST_SYS, (last_sys, now_ms, now_ms), "list sys mail"
        )

    async def list_guild_since(self, guild_id: int, last_guild: int, now_ms: int) -> list[MailRow]:
        """拉某公会 mail_id > last_guild 且当前生效的公会邮件。"""
        return await self._list_channel(
            _SQL_LIST_GUILD, (guild_id, last_guild, now_ms, now_ms), "list guild mail"
        )

    async def _list_channel(self, sql: str, args: tuple, op: str) -> list[MailRow]:
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(sql, args)
                rows = await cur.fetchall()
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(errcode.ErrInternal, "%s: %s", op, exc) from exc
        return [
            MailRow(
                mail_id=int(r[0]),
                start_ms=int(r[1] or 0),
                end_ms=int(r[2] or 0),
                created_ms=int(r[3] or 0),
                payload=_as_bytes(r[4]),
            )
            for r in rows
        ]

    async def advance_cursor(self, player_id: int, sys_max: int, guild_max: int) -> None:
        """推进 watermark。

        ★ 必须是 GREATEST 而不是直接赋值:同一玩家两个并发 ListMail(多端登录 /
        重试)会各自算出自己那批的最大值,后到的若比先到的小就会把水位**推回去**,
        玩家于是重复收到同一批系统邮件。GREATEST 让水位单调不回退。
        """
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(
                    _SQL_ADVANCE_CURSOR, (player_id, sys_max, guild_max, sys_max, guild_max)
                )
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "advance cursor %d: %s", player_id, exc
                ) from exc

    # ── 写:状态 / 删除 ──────────────────────────────────────────────────

    async def set_personal_status(self, player_id: int, mail_id: int, status: int) -> None:
        """置个人邮件状态。claimed 列随 status=CLAIMED 同步置 1(**只置不清**)。

        只置不清是因为 claimed 是"领过奖"的既成事实,不该被后续的状态变更抹掉;
        清掉会让驱逐 / 清理逻辑把一封已领的邮件当成未领而去归档。
        """
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(
                    _SQL_SET_STATUS, (status, status, STATUS_CLAIMED, mail_id, player_id)
                )
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "set status %d: %s", mail_id, exc
                ) from exc

    async def delete_personal(self, player_id: int, mail_id: int) -> None:
        """删个人邮件。带 player_id 条件 = 越权删除天然不可能(不是靠上层校验)。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(_SQL_DELETE_PERSONAL, (mail_id, player_id))
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "delete mail %d: %s", mail_id, exc
                ) from exc

    # ── 领取 ────────────────────────────────────────────────────────────

    async def get_claimable_payload(
        self, player_id: int, mail_id: int, now_ms: int
    ) -> bytes | None:
        """取邮件正文用于领取,并按 channel 校验领取人**有权**访问 + 生效区间。

        ★ 权限判定就在这三条 SQL 里,不在上层:
            个人邮件 → 必须是收件人本人(WHERE player_id = ?)
            系统邮件 → 任意玩家可领
            公会邮件 → JOIN guild_members,必须**当前**仍是该公会成员
        越权 / 未生效 / 已过期 / 不存在一律返回 None(对外表现为 NotFound),
        刻意不区分 —— 区分了就等于给探测者一个"这封邮件存在但你不能领"的信号。
        """
        for sql, args, op in (
            (_SQL_CLAIMABLE_PERSONAL, (mail_id, player_id, now_ms), "get personal payload"),
            (_SQL_CLAIMABLE_SYS, (mail_id, now_ms, now_ms), "get sys payload"),
            (_SQL_CLAIMABLE_GUILD, (mail_id, player_id, now_ms, now_ms), "get guild payload"),
        ):
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                try:
                    await cur.execute(sql, args)
                    row = await cur.fetchone()
                except asyncio.CancelledError:
                    # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                    # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                    #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                    #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                    # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                    raise
                except BaseException as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrInternal, "%s %d: %s", op, mail_id, exc
                    ) from exc
            if row is not None:
                return _as_bytes(row[0])
        return None

    async def has_claimed(self, player_id: int, mail_id: int) -> bool:
        """是否已**终态**领取。

        ★ 只认 claimed=1。DS 三段式的意图行(claimed=0)不算已领 —— 它由
        ClaimMail 的互斥检查(get_claim_state 的 intent_open → 9607)单独处理。
        把意图行也算成已领,会让列表里那封邮件立刻显示"已领"而实际还没入包。
        """
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(_SQL_HAS_CLAIMED, (player_id, mail_id))
                row = await cur.fetchone()
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(errcode.ErrInternal, "has claimed: %s", exc) from exc
        return row is not None

    async def get_claim_state(self, player_id: int, mail_id: int) -> tuple[bool, bool]:
        """返回 (claimed 终态, intent_open 意图进行中)。无行 = (False, False)。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(_SQL_GET_CLAIM_STATE, (player_id, mail_id))
                row = await cur.fetchone()
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "get claim state: %s", exc
                ) from exc
        if row is None:
            return False, False
        if int(row[0]) != 0:
            return True, False
        return False, True

    async def get_claim_intent(self, player_id: int, mail_id: int) -> bytes | None:
        """读意图行 payload(仅 claimed=0 的行;终态或无行 → None)。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(_SQL_GET_CLAIM_INTENT, (player_id, mail_id))
                row = await cur.fetchone()
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "get claim intent: %s", exc
                ) from exc
        return _as_bytes(row[0]) if row is not None else None

    async def create_claim_intent(self, player_id: int, mail_id: int, payload: bytes) -> bool:
        """建意图行(INSERT IGNORE)。已有任何行 → False,由调用方重读状态决策。

        ★ 必须是 IGNORE 而不是 REPLACE / ON DUPLICATE UPDATE:覆盖既有意图会换掉
        里面已铸好的 instance_id,而 bag journal 靠"意图内容逐字节一致"做指纹去重 ——
        换了 ID 就是同一封邮件被发两次。
        """
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(_SQL_CREATE_CLAIM_INTENT, (player_id, mail_id, payload))
                created = (cur.rowcount or 0) > 0
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "create claim intent: %s", exc
                ) from exc
        return created

    async def mark_claimed(self, player_id: int, mail_id: int) -> bool:
        """意图行置终态(幂等:已终态 no-op)。返回"行存在"。

        rowcount=0 有两种可能:行不存在,或已经是 claimed=1(MySQL 不计未变更行)。
        两者语义完全相反,所以必须补一次存在性判定 —— 直接把 0 当"没这行"会让
        重复 Mark 报错,而重复 Mark 正是崩溃重放的正常路径。
        """
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(_SQL_MARK_CLAIMED, (player_id, mail_id))
                affected = cur.rowcount or 0
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(errcode.ErrInternal, "mark claimed: %s", exc) from exc
        if affected > 0:
            return True
        claimed, intent_open = await self.get_claim_state(player_id, mail_id)
        return claimed or intent_open

    async def record_claim(self, player_id: int, mail_id: int) -> bool:
        """记直连链领取(claimed 默认 1 由建表定义)。返回"本次是首次"。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(_SQL_RECORD_CLAIM, (player_id, mail_id))
                first = (cur.rowcount or 0) > 0
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(errcode.ErrInternal, "record claim: %s", exc) from exc
        return first

    # ── 写:发送 ────────────────────────────────────────────────────────

    async def insert_sys_mail(
        self, mail_id: int, start_ms: int, end_ms: int, payload: bytes
    ) -> None:
        await self._exec_commit(
            _SQL_INSERT_SYS, (mail_id, start_ms, end_ms, payload), "insert sys mail"
        )

    async def insert_guild_mail(
        self, mail_id: int, guild_id: int, start_ms: int, end_ms: int, payload: bytes
    ) -> None:
        await self._exec_commit(
            _SQL_INSERT_GUILD, (mail_id, guild_id, start_ms, end_ms, payload), "insert guild mail"
        )

    async def insert_personal_mail(
        self, mail_id: int, player_id: int, expire_ms: int, payload: bytes, max_inbox: int
    ) -> None:
        """写收件箱,**事务内**原子校验单玩家行数上限(§9 不变量 18)。

        ★ 上限判定与插入必须在同一个事务里,而且 COUNT 要 FOR UPDATE:
        先查后写会被并发写入穿过(TOCTOU)—— 两个 battle_result 同时给同一玩家
        转邮件,各自读到 199/200 都判定"没满",结果收件箱变成 201 行。
        FOR UPDATE 锁住该玩家的索引范围,同玩家的并发写入被串行化。

        满时先驱逐最旧的**已领**邮件(附件已落袋,删掉无损);仍满则回滚返回
        ErrMailBoxFull —— 调用方靠补扫重试,等旧邮件过期被 sweep 清掉后自然成功。
        过期未清但**未领**的行仍占名额:写路径刻意不解 payload 判附件
        (那是一次反序列化 + 一次判定,放在最热的写路径上不划算),留给 sweep 归档后释放。
        """
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    if max_inbox > 0:
                        await cur.execute(_SQL_COUNT_INBOX_FOR_UPDATE, (player_id,))
                        row = await cur.fetchone()
                        cnt = int(row[0]) if row is not None else 0
                        if cnt >= max_inbox:
                            await cur.execute(
                                _SQL_EVICT_CLAIMED,
                                (player_id, STATUS_CLAIMED, cnt - max_inbox + 1),
                            )
                            evicted = cur.rowcount or 0
                            if cnt - evicted >= max_inbox:
                                raise errcode.PandoraError(
                                    errcode.ErrMailBoxFull,
                                    "player %d inbox full (%d/%d)",
                                    player_id,
                                    cnt,
                                    max_inbox,
                                )
                    await cur.execute(
                        _SQL_INSERT_PERSONAL, (mail_id, player_id, STATUS_UNREAD, expire_ms, payload)
                    )
                await conn.commit()
            except errcode.PandoraError:
                # 业务码(收件箱满)原样上抛,不包成 ErrInternal —— 调用方要按
                # ERR_MAIL_BOX_FULL 决定"稍后补扫重试"还是"当失败处理"。
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise errcode.PandoraError(
                    errcode.ErrInternal, "insert personal mail: %s", exc
                ) from exc

    # ── sweep 清理(全部幂等,多副本并发安全,单批 limit 有界)──────────

    async def list_expired_personal(
        self, expire_before_ms: int, limit: int
    ) -> list[ExpiredPersonalRow]:
        """捞过期个人邮件(expire_ms ∈ (0, expire_before_ms]),按过期时间升序。

        expire_ms > 0 这一半不能少:0 = 永不过期,漏掉它会把永久邮件全清了。
        """
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(_SQL_LIST_EXPIRED_PERSONAL, (expire_before_ms, limit))
                rows = await cur.fetchall()
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "list expired personal: %s", exc
                ) from exc
        return [
            ExpiredPersonalRow(
                mail_id=int(r[0]),
                player_id=int(r[1]),
                status=int(r[2]),
                expire_ms=int(r[3] or 0),
                created_ms=int(r[4] or 0),
                payload=_as_bytes(r[5]),
            )
            for r in rows
        ]

    async def archive_and_delete_personal(
        self, archive: list[ExpiredPersonalRow], delete_ids: list[int]
    ) -> None:
        """同事务:archive 行移入归档表(INSERT IGNORE 幂等),delete_ids 从收件箱删。

        ★ 必须同事务。分成两步的话,"归档成功 + 删除失败"会在下一轮重复归档(无害),
        但"删除成功 + 归档失败"就是**带未领附件的邮件被无声销毁** ——
        mail.md §7.4 要求"过期附件清理后必有补偿或归档,不静默丢失"。
        """
        if not delete_ids:
            return
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    if archive:
                        values = ",".join(["(%s, %s, %s, %s, %s, %s)"] * len(archive))
                        args: list[object] = []
                        for m in archive:
                            args += [
                                m.mail_id,
                                m.player_id,
                                m.status,
                                m.expire_ms,
                                m.created_ms,
                                m.payload,
                            ]
                        await cur.execute(_SQL_ARCHIVE_PREFIX + values, tuple(args))
                    placeholders = ",".join(["%s"] * len(delete_ids))
                    await cur.execute(
                        f"DELETE FROM player_mail WHERE mail_id IN ({placeholders})",  # noqa: S608
                        tuple(delete_ids),
                    )
                await conn.commit()
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise errcode.PandoraError(
                    errcode.ErrInternal, "archive personal: %s", exc
                ) from exc

    async def delete_sys_mail_ended_before(self, end_before_ms: int, limit: int) -> int:
        return await self._exec_affected(
            _SQL_DELETE_SYS_ENDED, (end_before_ms, limit), "delete sys mail"
        )

    async def delete_guild_mail_ended_before(self, end_before_ms: int, limit: int) -> int:
        return await self._exec_affected(
            _SQL_DELETE_GUILD_ENDED, (end_before_ms, limit), "delete guild mail"
        )

    async def delete_claims_before(self, max_mail_id: int, limit: int) -> int:
        """删 mail_id < max_mail_id 的领取记录。

        雪花 mail_id 的时间段单调,所以"创建早于 cutoff"等价于"mail_id 小于
        min_id_at(cutoff)" —— 把时间条件翻译成主键范围条件,走索引不全表扫。
        """
        return await self._exec_affected(
            _SQL_DELETE_CLAIMS_BEFORE, (max_mail_id, limit), "delete claims"
        )

    async def purge_archive_before(self, retention_days: int, limit: int) -> int:
        return await self._exec_affected(
            _SQL_PURGE_ARCHIVE, (retention_days, limit), "purge archive"
        )

    # ── 内部 ────────────────────────────────────────────────────────────

    async def _exec_commit(self, sql: str, args: tuple, op: str) -> None:
        """单语句写(autocommit 落盘)。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(sql, args)
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(errcode.ErrInternal, "%s: %s", op, exc) from exc

    async def _exec_affected(self, sql: str, args: tuple, op: str) -> int:
        """单语句写并回报影响行数(sweep 用它打"这一轮删了几行")。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(sql, args)
                affected = cur.rowcount or 0
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(errcode.ErrInternal, "%s: %s", op, exc) from exc
        return int(affected)


def _as_bytes(value) -> bytes:  # noqa: ANN001
    """把驱动返回的 BLOB 归一成 bytes(不同驱动可能给 bytearray / memoryview)。"""
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):
        # 极少数驱动配置会把 BLOB 当文本返回;latin-1 是唯一字节保真的编码。
        return value.encode("latin-1")
    return bytes(value)


# ── SQL(与 Go 侧逐句同形;占位符换成 %s)─────────────────────────────────

_SQL_GET_CURSOR = (
    "SELECT last_sys_mail_id, last_guild_mail_id FROM player_mail_cursor WHERE player_id = %s"
)

_SQL_GET_PLAYER_GUILD = "SELECT guild_id FROM guild_members WHERE player_id = %s"

_SQL_LIST_PERSONAL = """SELECT mail_id, status, claimed, expire_ms,
       CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED), payload
FROM player_mail
WHERE player_id = %s AND (expire_ms = 0 OR expire_ms > %s)
      AND (%s = 0 OR mail_id < %s)
ORDER BY mail_id DESC
LIMIT %s"""

_SQL_LIST_SYS = """SELECT mail_id, start_ms, end_ms,
       CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED), payload
FROM sys_mail
WHERE mail_id > %s AND (start_ms = 0 OR start_ms <= %s) AND (end_ms = 0 OR end_ms > %s)
ORDER BY mail_id"""

_SQL_LIST_GUILD = """SELECT mail_id, start_ms, end_ms,
       CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED), payload
FROM guild_mail
WHERE guild_id = %s AND mail_id > %s AND (start_ms = 0 OR start_ms <= %s)
      AND (end_ms = 0 OR end_ms > %s)
ORDER BY mail_id"""

# ★ 刻意**不用** `VALUES(col)` 引用待插入值(Go 侧那句用的是它)。
# MySQL 8.4 起 `VALUES()` 在 ON DUPLICATE KEY UPDATE 里已废弃,每执行一次就回一条
# deprecation warning;Go 的 driver 默默丢掉,而 asyncmy 会把它抛进 Python 日志 ——
# ListMail 首页是最热的路径之一,实测每次调用刷两条 WARN,足以把 access log 淹掉
# (而且是"看着像出问题、其实没问题"的那种噪音,最消耗值班注意力)。
# 改成把值再传一遍:语义逐字相同(仍是 GREATEST 单调推进),且 5.7 / 8.x / TiDB 通吃。
_SQL_ADVANCE_CURSOR = """INSERT INTO player_mail_cursor
    (player_id, last_sys_mail_id, last_guild_mail_id)
VALUES (%s, %s, %s)
ON DUPLICATE KEY UPDATE
  last_sys_mail_id = GREATEST(last_sys_mail_id, %s),
  last_guild_mail_id = GREATEST(last_guild_mail_id, %s)"""

_SQL_SET_STATUS = (
    "UPDATE player_mail SET status = %s, claimed = IF(%s = %s, 1, claimed) "
    "WHERE mail_id = %s AND player_id = %s"
)

_SQL_DELETE_PERSONAL = "DELETE FROM player_mail WHERE mail_id = %s AND player_id = %s"

_SQL_CLAIMABLE_PERSONAL = (
    "SELECT payload FROM player_mail "
    "WHERE mail_id = %s AND player_id = %s AND (expire_ms = 0 OR expire_ms > %s)"
)

_SQL_CLAIMABLE_SYS = (
    "SELECT payload FROM sys_mail "
    "WHERE mail_id = %s AND (start_ms = 0 OR start_ms <= %s) AND (end_ms = 0 OR end_ms > %s)"
)

_SQL_CLAIMABLE_GUILD = """SELECT gm.payload FROM guild_mail gm
JOIN guild_members m ON m.guild_id = gm.guild_id
WHERE gm.mail_id = %s AND m.player_id = %s
  AND (gm.start_ms = 0 OR gm.start_ms <= %s) AND (gm.end_ms = 0 OR gm.end_ms > %s)"""

_SQL_HAS_CLAIMED = (
    "SELECT 1 FROM player_mail_claim WHERE player_id = %s AND mail_id = %s AND claimed = 1"
)

_SQL_GET_CLAIM_STATE = (
    "SELECT claimed FROM player_mail_claim WHERE player_id = %s AND mail_id = %s"
)

_SQL_GET_CLAIM_INTENT = (
    "SELECT intent_payload FROM player_mail_claim "
    "WHERE player_id = %s AND mail_id = %s AND claimed = 0"
)

_SQL_CREATE_CLAIM_INTENT = (
    "INSERT IGNORE INTO player_mail_claim (player_id, mail_id, claimed, intent_payload) "
    "VALUES (%s, %s, 0, %s)"
)

_SQL_MARK_CLAIMED = (
    "UPDATE player_mail_claim SET claimed = 1 WHERE player_id = %s AND mail_id = %s"
)

_SQL_RECORD_CLAIM = "INSERT IGNORE INTO player_mail_claim (player_id, mail_id) VALUES (%s, %s)"

_SQL_INSERT_SYS = "INSERT INTO sys_mail (mail_id, start_ms, end_ms, payload) VALUES (%s, %s, %s, %s)"

_SQL_INSERT_GUILD = (
    "INSERT INTO guild_mail (mail_id, guild_id, start_ms, end_ms, payload) "
    "VALUES (%s, %s, %s, %s, %s)"
)

_SQL_COUNT_INBOX_FOR_UPDATE = "SELECT COUNT(*) FROM player_mail WHERE player_id = %s FOR UPDATE"

_SQL_EVICT_CLAIMED = (
    "DELETE FROM player_mail WHERE player_id = %s AND status = %s ORDER BY mail_id LIMIT %s"
)

_SQL_INSERT_PERSONAL = (
    "INSERT INTO player_mail (mail_id, player_id, status, expire_ms, payload) "
    "VALUES (%s, %s, %s, %s, %s)"
)

_SQL_LIST_EXPIRED_PERSONAL = """SELECT mail_id, player_id, status, expire_ms,
       CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED), payload
FROM player_mail
WHERE expire_ms > 0 AND expire_ms <= %s
ORDER BY expire_ms
LIMIT %s"""

_SQL_ARCHIVE_PREFIX = (
    "INSERT IGNORE INTO player_mail_archive "
    "(mail_id, player_id, status, expire_ms, created_ms, payload) VALUES "
)

_SQL_DELETE_SYS_ENDED = "DELETE FROM sys_mail WHERE end_ms > 0 AND end_ms <= %s LIMIT %s"

_SQL_DELETE_GUILD_ENDED = "DELETE FROM guild_mail WHERE end_ms > 0 AND end_ms <= %s LIMIT %s"

_SQL_DELETE_CLAIMS_BEFORE = "DELETE FROM player_mail_claim WHERE mail_id < %s LIMIT %s"

_SQL_PURGE_ARCHIVE = (
    "DELETE FROM player_mail_archive "
    "WHERE archived_at < DATE_SUB(NOW(), INTERVAL %s DAY) LIMIT %s"
)
