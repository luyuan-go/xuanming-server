"""临时群数据层 —— 对应 Go 侧 internal/data/group_repo.go。

★ 名额占用用的是一个**两把锁 + 对账**的模式,三条都不能改:

════ ① 固定锁顺序:计数行 → 明细范围 ════

    reserve 先锁 `player_group_counts` 的单行,再锁 `chat_group_members` 的明细范围。
    调用方涉及多个玩家时**必须按 player_id 升序**依次取 —— 顺序不固定就是环。

    两把锁各有分工:
      - 计数行:TiDB 没有 gap 锁,`COUNT ... FOR UPDATE` 在零行时一把锁都不加,
        所以必须有一行**确定存在**的行可锁,它才是新版之间的串行化点。
      - 明细范围:复用 MySQL 旧版 `COUNT ... FOR UPDATE` 的索引锁,
        让旧/新混跑时新版必看到旧写提交后的明细。

════ ② 明细 COUNT 是权威,计数行只是串行化点 + 读优化 ════

    判定用的是 `SELECT COUNT(*) FROM chat_group_members`,不是计数行的值。
    计数行写错 / 被旧 Pod 留脏都不会让上限判错。

════ ③ 对账用**绝对值回写**,不是 ±1 ════

    `UPDATE ... SET group_count = <实际值>` 而不是 `group_count + 1`。
    增量写在计数已经脏掉时会把错误永久累积;绝对值回写让脏计数**下次操作自愈**。
    release 同理:重算而不是 -1(可能被旧 Pod 留脏)。
"""

from __future__ import annotations

import contextlib

from pandora.group.v1 import group_pb2

from pandorapy import errcode, mysqlx
from pandorapy import log as plog
from pandorapy.services.guild.rows import GroupMemberRow, GroupRow

# 事务死锁重试次数。兜底二级索引间隙锁等偶发 1213/1205;fn 必须可安全重放。
TX_MAX_RETRIES = 3

# 群组职位。★ 直接取 proto 生成物,不抄字面量 —— role 列的值会原样出现在
# GetGroupMembers 应答里,手抄错一位客户端就看到 UNSPECIFIED;而且 Go 侧列表
# 走 `ORDER BY role ASC`,owner(1) 必须排在 member(2) 前面,写 0 会把普通成员
# 顶到群主上面。数值口径见 proto/pandora/group/v1/group.proto GroupRole。
GROUP_ROLE_OWNER = group_pb2.GROUP_ROLE_OWNER
GROUP_ROLE_MEMBER = group_pb2.GROUP_ROLE_MEMBER

# 群成员 / 我所在的群列表单次返回的防御性 SQL 上限(§9.18 读取侧「单次返回上限」兜底)。
# 群成员受 max_group_members(默认 50)兜住,我所在的群受 max_groups_per_player(默认 50)
# 兜住;这里取更宽松的硬上限,仅防历史脏数据下的无界返回。与 Go 的 groupListReadHardLimit 同值。
GROUP_LIST_READ_HARD_LIMIT = 500

_GROUP_COLS = """group_id, name, owner_id, member_count, max_members,
       CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED)"""


def _group_row(row) -> GroupRow:  # noqa: ANN001
    return GroupRow(
        group_id=int(row[0]),
        name=str(row[1]),
        owner_id=int(row[2]),
        member_count=int(row[3]),
        max_members=int(row[4]),
        created_ms=int(row[5] or 0),
    )


class MySQLGroupRepo:
    """临时群数据层。"""

    __slots__ = ("_pool",)

    def __init__(self, pool) -> None:  # noqa: ANN001
        self._pool = pool

    # ── 事务封装(带死锁重试)───────────────────────────────────────────────

    @contextlib.asynccontextmanager
    async def _tx_once(self):
        async with self._pool.acquire() as conn:
            await conn.begin()
            try:
                async with conn.cursor() as cur:
                    yield cur
                await conn.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise

    async def _run_tx(self, fn):  # noqa: ANN001
        """跑一个事务,遇可重试错误(死锁 / 锁等待超时)重试。

        ⚠️ fn **必须可安全重放** —— 它会被整体重跑,不能有事务外的副作用
        (发 kafka、调下游 RPC)。
        """
        last: BaseException | None = None
        for _ in range(TX_MAX_RETRIES + 1):
            try:
                async with self._tx_once() as cur:
                    return await fn(cur)
            except Exception as exc:  # noqa: BLE001
                if not _is_retryable(exc):
                    raise
                last = exc
                # 锁顺序修好之后 1213 应当是罕见事件;留在 debug 等于默认不上报,
                # 线上就再也看不出"又出现了一条反向取锁的新路径",只会表现为偶发变慢。
                plog.get().warning("group_tx_retry", err=str(exc))
        assert last is not None
        raise last

    # ── ★ 名额占用 ─────────────────────────────────────────────────────────

    @staticmethod
    async def _reconcile_player_group_count(cur, player_id: int) -> int:  # noqa: ANN001
        """取计数行锁 → 用明细锁定读算出**实际**群数 → 绝对值回写。返回实际值。

        惰性建行:`INSERT ... ON DUPLICATE KEY UPDATE player_id = player_id` 是
        "存在就锁、不存在就建并锁"的标准写法 —— 保证已有行和首次创建都成为
        本版本的**唯一串行化点**(普通 SELECT FOR UPDATE 在行不存在时,TiDB 一把锁都不加)。
        """
        await cur.execute(
            "INSERT INTO player_group_counts (player_id, group_count) VALUES (%s, 0) "
            "ON DUPLICATE KEY UPDATE player_id = player_id",
            (player_id,),
        )
        # 第一把锁:计数行(TiDB 新/新写的串行化点)
        await cur.execute(
            "SELECT group_count FROM player_group_counts WHERE player_id = %s FOR UPDATE",
            (player_id,),
        )
        row = await cur.fetchone()
        stored = int(row[0]) if row else 0

        # 第二把锁:明细范围(与 MySQL 旧版的 COUNT..FOR UPDATE 互斥)
        # ★ 这个 COUNT 才是**权威**,计数行只是串行化点 + 读优化值。
        await cur.execute(
            "SELECT COUNT(*) FROM chat_group_members WHERE player_id = %s FOR UPDATE",
            (player_id,),
        )
        row = await cur.fetchone()
        actual = int(row[0]) if row else 0

        if stored != actual:
            # ★ **绝对值回写**,不是 ±1 —— 增量写会把已经脏掉的计数永久累积。
            await cur.execute(
                "UPDATE player_group_counts SET group_count = %s WHERE player_id = %s",
                (actual, player_id),
            )
            plog.get().debug(
                "group_count_reconciled",
                player_id=player_id,
                stored=stored,
                actual=actual,
            )
        return actual

    async def _reserve_player_group_slot(
        self, cur, player_id: int, max_groups: int
    ) -> None:  # noqa: ANN001
        """为玩家占用一个「所在群」名额(§9.18 写入侧上限)。"""
        count = await self._reconcile_player_group_count(cur, player_id)
        if max_groups > 0 and count >= max_groups:
            raise errcode.PandoraError(
                errcode.ErrGroupJoinLimit,
                "group join limit reached for %d (max %d)",
                player_id,
                max_groups,
            )
        await cur.execute(
            "UPDATE player_group_counts SET group_count = %s WHERE player_id = %s",
            (count + 1, player_id),
        )

    async def _release_player_group_slot(self, cur, player_id: int) -> None:  # noqa: ANN001
        """释放名额。

        ★ **重算而不是 -1** —— 计数可能被旧 Pod 留脏,-1 会把脏值继续带下去。
        调用方须已按 count-row → detail-range 顺序锁定并完成明细 DELETE。
        """
        await self._reconcile_player_group_count(cur, player_id)

    # ── 建群 / 加人 / 退群 ─────────────────────────────────────────────────

    async def create_group(
        self,
        group_id: int,
        owner_id: int,
        member_ids: list[int],
        *,
        name: str = "",
        max_members: int,
        max_groups_per_player: int,
    ) -> None:
        """建群。owner 也算成员。

        ★ 多玩家取锁**必须按 player_id 升序** —— 两个并发建群若顺序相反就是环。

        `name` 是关键字参数且有默认值,只因为本方法先于 name 列存在
        (本仓已有一批只关心名额占用的用例按旧签名调用);业务路径上 biz 一定会传 ——
        空名在 `GroupUsecase.CreateGroup` 就被 ErrInvalidArg 拦掉了。
        """
        members = sorted({owner_id, *member_ids})
        if max_members > 0 and len(members) > max_members:
            raise errcode.PandoraError(
                errcode.ErrGroupFull,
                "group %d would have %d members (max %d)",
                group_id,
                len(members),
                max_members,
            )

        async def body(cur):  # noqa: ANN001
            # ★ 先按 player_id 升序取名额锁,**再**插群行 —— 与 Go
            # group_repo.go:108-120 同序,也与本文件 add_member / remove_member 的
            # 「计数行 → 明细」方向一致。
            #
            # 诚实说明:新 group_id 的行没有并发争用者,所以今天**没有**能证实的死锁路径;
            # 对齐是因为"同一套表的所有写路径用同一个取锁顺序"本身就是本文件开头那条
            # 不变量,留一条方向相反的路径,下一个加写路径的人会照着它抄。
            for pid in members:
                await self._reserve_player_group_slot(cur, pid, max_groups_per_player)
            await cur.execute(
                "INSERT INTO chat_groups (group_id, name, owner_id, member_count, max_members) "
                "VALUES (%s, %s, %s, %s, %s)",
                (group_id, name, owner_id, len(members), max_members),
            )
            for pid in members:
                await cur.execute(
                    "INSERT INTO chat_group_members (group_id, player_id, role) "
                    "VALUES (%s, %s, %s)",
                    (
                        group_id,
                        pid,
                        GROUP_ROLE_OWNER if pid == owner_id else GROUP_ROLE_MEMBER,
                    ),
                )
            # 明细写完后重算一遍,让计数行与明细一致
            for pid in members:
                await self._reconcile_player_group_count(cur, pid)

        await self._run_tx(body)

    async def add_member(
        self,
        group_id: int,
        player_id: int,
        *,
        operator_id: int = 0,
        max_members: int,
        max_groups_per_player: int,
    ) -> bool:
        """加人。群成员上限与玩家所在群上限**都要过**。返回 True = 已在群(幂等命中)。

        `operator_id` 非 0 时在事务内**持群行锁复核邀请者仍在群内**:
        消除「biz 的 GetGroupMember 通过后邀请者已退群 / 被踢仍能拉人」的 TOCTOU。
        为 0 时跳过(内部无邀请者场景;与 Go 的 `if operatorID != 0` 同判据)。
        """

        async def body(cur):  # noqa: ANN001
            already_in = False
            # 群行是每群操作的第一把锁(串行化点)
            await cur.execute(
                "SELECT member_count FROM chat_groups WHERE group_id = %s FOR UPDATE",
                (group_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrGroupNotFound, "group %d not found", group_id
                )
            member_count = int(row[0])

            # ★ 持群行锁复核邀请者仍在群内(Go group_repo.go:225-236)。
            if operator_id != 0:
                await cur.execute(
                    "SELECT 1 FROM chat_group_members WHERE group_id = %s AND player_id = %s LIMIT 1",
                    (group_id, operator_id),
                )
                if await cur.fetchone() is None:
                    raise errcode.PandoraError(
                        errcode.ErrGroupNotMember,
                        "operator %d not in group %d",
                        operator_id,
                        group_id,
                    )

            # ★ 「已在群」必须查在**满员判定之前**(Go group_repo.go:238-251)。
            # 放在后面的话,把一个已经在群里的人再拉一次、恰好群满 → 返回 ErrGroupFull,
            # 而正确答案是幂等成功:客户端重发一次邀请就变成一个假失败。
            await cur.execute(
                "SELECT 1 FROM chat_group_members WHERE group_id = %s AND player_id = %s LIMIT 1",
                (group_id, player_id),
            )
            if await cur.fetchone() is not None:
                # 幂等命中也顺手自愈旧 Pod 留下的计数漂移。
                await self._reconcile_player_group_count(cur, player_id)
                already_in = True
                return already_in

            if max_members > 0 and member_count >= max_members:
                raise errcode.PandoraError(
                    errcode.ErrGroupFull,
                    "group %d is full (max %d)",
                    group_id,
                    max_members,
                )

            await self._reserve_player_group_slot(cur, player_id, max_groups_per_player)
            try:
                await cur.execute(
                    "INSERT INTO chat_group_members (group_id, player_id, role) "
                    "VALUES (%s, %s, %s)",
                    (group_id, player_id, GROUP_ROLE_MEMBER),
                )
            except Exception as exc:  # noqa: BLE001
                if mysqlx.is_duplicate_entry(exc):
                    # 上面的存在性检查与 INSERT 之间理论上已被群行锁串行,这条是兜底:
                    # 已在群里 —— 幂等 no-op,但要把刚占的名额还回去(重算即可)
                    await self._reconcile_player_group_count(cur, player_id)
                    return True
                raise
            await cur.execute(
                "UPDATE chat_groups SET member_count = member_count + 1 WHERE group_id = %s",
                (group_id,),
            )
            await self._reconcile_player_group_count(cur, player_id)
            return already_in

        return bool(await self._run_tx(body))

    async def remove_member(self, group_id: int, player_id: int) -> None:
        async def body(cur):  # noqa: ANN001
            await cur.execute(
                "SELECT owner_id FROM chat_groups WHERE group_id = %s FOR UPDATE",
                (group_id,),
            )
            row = await cur.fetchone()
            if row is None:
                # 群已解散:退群 / 踢人**幂等成功**(Go group_repo.go:281 `return nil`)。
                # 抛 NotFound 会让"解散与退群并发"这一常见交错变成客户端可见的错误,
                # 而那两件事本来就该都成功。
                return
            # ★ 禁止移除现任群主(Go :285-288,三审 P1-9 TOCTOU)。
            # 不拦的话,"退群 / 踢人"与"转让群主"交错时会删掉**刚晋升的新群主**,
            # 群里留下一个指向已不在群的 owner_id —— 之后谁都转让不了、也解散不掉。
            # 群主要走必须先转让或解散。
            if int(row[0]) == player_id:
                raise errcode.PandoraError(
                    errcode.ErrGroupNotOwner,
                    "owner %d must transfer or disband before leaving group %d",
                    player_id,
                    group_id,
                )
            # ★ 固定 count-row → detail-range 顺序:先把计数行和该玩家的成员明细
            # 范围锁到手,再执行 DELETE。DELETE 自己就会锁明细,若放在前面,本路径
            # 就成了"明细 → 计数行",与 add_member/create_group 的"计数行 → 明细"
            # 反向,两条并发路径互等即死锁(1213),靠重试掩盖而已。
            await self._reconcile_player_group_count(cur, player_id)
            await cur.execute(
                "DELETE FROM chat_group_members WHERE group_id = %s AND player_id = %s",
                (group_id, player_id),
            )
            if cur.rowcount:
                await cur.execute(
                    "UPDATE chat_groups SET member_count = member_count - 1 "
                    "WHERE group_id = %s AND member_count > 0",
                    (group_id,),
                )
            # 无论是否真删都重算(幂等)
            await self._release_player_group_slot(cur, player_id)

        await self._run_tx(body)

    async def list_my_groups(self, player_id: int, limit: int = 200) -> list[int]:
        """§9.18 读取侧:SQL LIMIT 兜底(写入侧上限已把规模钉在几十)。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT group_id FROM chat_group_members WHERE player_id = %s "
                "ORDER BY group_id LIMIT %s",
                (player_id, limit),
            )
            return [int(r[0]) for r in await cur.fetchall()]

    async def player_group_count(self, player_id: int) -> int:
        """读计数行(只读路径,不加锁)。仅供展示 —— 权威始终是明细 COUNT。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT group_count FROM player_group_counts WHERE player_id = %s",
                (player_id,),
            )
            row = await cur.fetchone()
            return int(row[0]) if row else 0

    # ── 踢人 / 解散 / 转让(全部先锁群行,与 remove_member 共用同一串行化点)──────

    async def kick_member(self, group_id: int, operator_id: int, target_id: int) -> None:
        """踢人。仅现任群主;不能踢群主自己。

        ★ 权限在**事务内持群行锁复核**,不信 biz 那次读:
        「biz 检查通过后群主被并发转让 / target 被转成群主仍被踢」是真实可复现的交错。
        """

        async def body(cur):  # noqa: ANN001
            await cur.execute(
                "SELECT owner_id FROM chat_groups WHERE group_id = %s FOR UPDATE", (group_id,)
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrGroupNotFound, "group %d not found", group_id
                )
            cur_owner = int(row[0])
            if cur_owner != operator_id:
                raise errcode.PandoraError(
                    errcode.ErrGroupNotOwner,
                    "player %d is not current owner of group %d (concurrent transfer?)",
                    operator_id,
                    group_id,
                )
            if target_id == cur_owner:
                raise errcode.PandoraError(errcode.ErrGroupNotOwner, "cannot kick the owner")
            # 固定 count-row → detail-range 顺序,再 DELETE(同 remove_member)。
            await self._reconcile_player_group_count(cur, target_id)
            await cur.execute(
                "DELETE FROM chat_group_members WHERE group_id = %s AND player_id = %s",
                (group_id, target_id),
            )
            if not cur.rowcount:
                raise errcode.PandoraError(
                    errcode.ErrGroupNotMember, "target %d not in group %d", target_id, group_id
                )
            await cur.execute(
                "UPDATE chat_groups SET member_count = member_count - 1 "
                "WHERE group_id = %s AND member_count > 0",
                (group_id,),
            )
            await self._release_player_group_slot(cur, target_id)

        await self._run_tx(body)

    async def disband_group(self, group_id: int, operator_id: int) -> None:
        """解散群。仅现任群主。

        ★ 成员先读出来**按 player_id 升序**再逐个 reconcile —— 与 create_group 的
        升序 reserve 同序。乱序会和逐玩家 reserve 形成 count(B) ↔ detail(A) 的 ABBA。
        也**不能**"先锁完所有 count row 再碰明细"(那是另一种反序)。
        """

        async def body(cur):  # noqa: ANN001
            await cur.execute(
                "SELECT owner_id FROM chat_groups WHERE group_id = %s FOR UPDATE", (group_id,)
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrGroupNotFound, "group %d not found", group_id
                )
            if int(row[0]) != operator_id:
                raise errcode.PandoraError(
                    errcode.ErrGroupNotOwner,
                    "player %d is not current owner of group %d (concurrent transfer?)",
                    operator_id,
                    group_id,
                )
            await cur.execute(
                "SELECT player_id FROM chat_group_members WHERE group_id = %s", (group_id,)
            )
            member_ids = sorted(int(r[0]) for r in await cur.fetchall())
            for pid in member_ids:
                await self._reconcile_player_group_count(cur, pid)
            await cur.execute(
                "DELETE FROM chat_group_members WHERE group_id = %s", (group_id,)
            )
            await cur.execute("DELETE FROM chat_groups WHERE group_id = %s", (group_id,))
            for pid in member_ids:
                await self._release_player_group_slot(cur, pid)

        await self._run_tx(body)

    async def transfer_owner(self, group_id: int, old_owner_id: int, new_owner_id: int) -> None:
        """转让群主。

        ★ 必须确认旧群主**仍是现任**:少这一步,并发两次转让会各自降旧群主、
        升不同目标 → 双 OWNER,而 owner_id 只留最后一个。
        """

        async def body(cur):  # noqa: ANN001
            await cur.execute(
                "SELECT owner_id FROM chat_groups WHERE group_id = %s FOR UPDATE", (group_id,)
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrGroupNotFound, "group %d not found", group_id
                )
            if int(row[0]) != old_owner_id:
                raise errcode.PandoraError(
                    errcode.ErrGroupNotOwner,
                    "player %d is not current owner of group %d (concurrent transfer?)",
                    old_owner_id,
                    group_id,
                )
            await cur.execute(
                "SELECT role FROM chat_group_members WHERE group_id = %s AND player_id = %s FOR UPDATE",
                (group_id, new_owner_id),
            )
            if await cur.fetchone() is None:
                raise errcode.PandoraError(
                    errcode.ErrGroupNotMember, "target %d not in group %d", new_owner_id, group_id
                )
            await cur.execute(
                "UPDATE chat_group_members SET role = %s WHERE group_id = %s AND player_id = %s",
                (GROUP_ROLE_MEMBER, group_id, old_owner_id),
            )
            await cur.execute(
                "UPDATE chat_group_members SET role = %s WHERE group_id = %s AND player_id = %s",
                (GROUP_ROLE_OWNER, group_id, new_owner_id),
            )
            await cur.execute(
                "UPDATE chat_groups SET owner_id = %s WHERE group_id = %s",
                (new_owner_id, group_id),
            )

        await self._run_tx(body)

    # ── 只读查询 ───────────────────────────────────────────────────────────

    async def get_group(self, group_id: int) -> GroupRow | None:
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                f"SELECT {_GROUP_COLS} FROM chat_groups WHERE group_id = %s",  # noqa: S608
                (group_id,),
            )
            row = await cur.fetchone()
            return _group_row(row) if row else None

    async def get_group_member(self, group_id: int, player_id: int) -> GroupMemberRow | None:
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT group_id, player_id, role, "
                "CAST(UNIX_TIMESTAMP(joined_at) * 1000 AS SIGNED) "
                "FROM chat_group_members WHERE group_id = %s AND player_id = %s",
                (group_id, player_id),
            )
            row = await cur.fetchone()
            if row is None:
                return None
            return GroupMemberRow(
                group_id=int(row[0]),
                player_id=int(row[1]),
                role=int(row[2]),
                joined_ms=int(row[3] or 0),
            )

    async def list_group_members(self, group_id: int) -> list[GroupMemberRow]:
        """列群成员(owner 在前)。§9.18 读取侧:硬 LIMIT 兜底。

        `ORDER BY role ASC` 依赖 owner(1) < member(2) —— role 落成 0 会把普通成员
        顶到群主上面(这正是 GROUP_ROLE_* 取 proto 常量而不手抄的原因)。
        """
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT group_id, player_id, role, "
                "CAST(UNIX_TIMESTAMP(joined_at) * 1000 AS SIGNED) "
                "FROM chat_group_members WHERE group_id = %s "
                "ORDER BY role ASC, joined_at ASC LIMIT %s",
                (group_id, GROUP_LIST_READ_HARD_LIMIT),
            )
            return [
                GroupMemberRow(
                    group_id=int(r[0]),
                    player_id=int(r[1]),
                    role=int(r[2]),
                    joined_ms=int(r[3] or 0),
                )
                for r in await cur.fetchall()
            ]

    async def list_my_group_rows(self, player_id: int) -> list[GroupRow]:
        """列玩家所在的群(**整行**)。对应 Go 的 `ListMyGroups`。

        与上面的 `list_my_groups`(只回 group_id)刻意并存:后者是名额占用测试
        与内部计数核对用的轻量读,前者才是 RPC `ListMyGroups` 的数据源。
        合成一个的话,轻量路径会为了几个 id 去 JOIN chat_groups。
        """
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT g.group_id, g.name, g.owner_id, g.member_count, g.max_members, "
                "CAST(UNIX_TIMESTAMP(g.created_at) * 1000 AS SIGNED) "
                "FROM chat_groups g JOIN chat_group_members m ON m.group_id = g.group_id "
                "WHERE m.player_id = %s ORDER BY g.created_at DESC LIMIT %s",
                (player_id, GROUP_LIST_READ_HARD_LIMIT),
            )
            return [_group_row(r) for r in await cur.fetchall()]


def _is_retryable(exc: BaseException) -> bool:
    """1213 死锁 / 1205 锁等待超时 → 可重试。"""
    args = getattr(exc, "args", ())
    return bool(args) and args[0] in (mysqlx.ER_LOCK_DEADLOCK, 1205)
