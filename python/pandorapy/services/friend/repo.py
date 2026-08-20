"""friend 数据层 —— 对应 Go 侧 internal/data/friend_repo.go。

★ 这个文件有两条**正确性要求(不是调优)**,都是真实事故的修复,移植时一个都不能丢:

════ ① 写事务必须显式 READ COMMITTED ════

    本域所有权威判定读都是 `SELECT ... FOR UPDATE`,而这些探针**绝大多数查不到行**
    (首次申请 / 首次拉黑)。RR 下未命中的锁定读锁的不是"某一行"而是**该键所在的间隙**:

        TRX A 持 uk_requester_target 某间隙的 X 锁,等在同一间隙插入;
        TRX B 持同一间隙的 X 锁,也等在同一间隙插入;   → 1213 死锁

    间隙锁彼此相容,所以 N 个事务都能拿到;冲突发生在随后的 insert intention。
    真 MySQL 8.4 实测:16 个并发申请(**互不相同的 requester 与 target**,没有任何
    共享行)必炸。**这不是锁序问题**,重排守卫顺序解决不了 —— 它们根本没有共享的守卫行。

    降到 RC 安全的理由:本域的并发正确性从设计之初就**不依赖 gap 锁** ——
    限额权威来自守卫行 + 守卫锁内的锁定读,唯一性来自唯一键,两者在 RC 下都成立。
    RR 的 gap 锁在 MySQL 侧是纯多余的副作用,只贡献死锁。

════ ② 锁序:pair 守卫 → player 守卫 → 探针 ════

    player 守卫必须在**任何锁定读之前**取。原实现把它放在限额校验里(即三条 FOR UPDATE
    探针之后),依据是"本事务持有的行锁只属于本 pair" —— **这条前提不成立**:
    未命中的 FOR UPDATE 锁的是间隙,N 个不同 requester 指向同一 target 时全部落在
    同一个 supremum 间隙里。InnoDB 死锁日志逐字印证了这个环:

        TRX A 持 friend_requests 间隙的 X 锁,等 guards 主键行;
        TRX B 持 guards 主键行,等 friend_requests 同一间隙的 insert intention。

    修法:把守卫提到探针之前,所有间隙锁都在守卫的串行化**内部**取得。

    ⚠️ 守卫**无条件取**,不受限额开关门控 —— 间隙锁的暴露与限额是否开启无关,
    关掉限额不该把锁序纪律一起关掉。

    ⚠️ 这个死锁**只在 MySQL 上炸**(TiDB 无 gap 锁),所以只跑 TiDB 会一直是绿的。
"""

from __future__ import annotations

import contextlib
import random

from pandora.friend.v1 import friend_pb2

from pandorapy import dbguard, errcode, mysqlx

# 好友 / 申请 / 黑名单列表单次返回的防御性 SQL 上限(§9.18 读取侧兜底)。
# 写入侧上限默认 200,正常列表远低于此;这里取更宽松的硬上限,仅防历史脏数据
# / 极端场景下的无界扫描。
LIST_READ_HARD_LIMIT = 1000

# ★ 状态值一律从生成的 pb 取,不手抄数字:friend_requests.status 与
# proto FriendRequestStatus 是**同一个枚举**(DDL 注释逐条对齐)。手抄的常量
# 在 proto 改动后不会报错,只会让"库里存的 3"和"客户端理解的 3"悄悄分家。
REQUEST_STATUS_PENDING = int(friend_pb2.FRIEND_REQUEST_STATUS_PENDING)
REQUEST_STATUS_ACCEPTED = int(friend_pb2.FRIEND_REQUEST_STATUS_ACCEPTED)
REQUEST_STATUS_REJECTED = int(friend_pb2.FRIEND_REQUEST_STATUS_REJECTED)

# ★ 见文件头 ①。前置:binlog_format=ROW(MySQL 8.4 默认)。
WRITE_TX_ISOLATION = "READ COMMITTED"


class MySQLFriendRepo:
    """基于 asyncmy / aiomysql 的 friend 数据层。"""

    __slots__ = ("_pool", "_schema")

    def __init__(self, pool, schema: str = "") -> None:  # noqa: ANN001
        self._pool = pool
        # schema 只被保留期清理用到(dbguard.sweep_table 要写全限定表名)。
        # 留空 → 首次清理时用 `SELECT DATABASE()` 解析并缓存:main 传的是 **DSN 里
        # 实际连上的库**,而不是写死 pandora_social —— TiDB 档 / 测试库连的都不是它,
        # 写死会让 sweep 去删一个根本没连的库(或直接报表不存在)。
        self._schema = schema

    @contextlib.asynccontextmanager
    async def _write_tx(self):
        """写事务:**显式 RC**。见文件头 ① —— 这是正确性要求不是调优。"""
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"SET TRANSACTION ISOLATION LEVEL {WRITE_TX_ISOLATION}")
            await conn.begin()
            try:
                async with conn.cursor() as cur:
                    yield cur
                await conn.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise

    # ── 守卫行 ───────────────────────────────────────────────────────────────

    @staticmethod
    async def _acquire_pair_guard(cur, a: int, b: int) -> None:  # noqa: ANN001
        """取"这一对玩家"的守卫行锁 —— 与同对的 Block/Accept 串行化。

        `INSERT ... ON DUPLICATE KEY UPDATE lo_id = lo_id` 是"存在就锁、不存在就建并锁"
        的标准写法:它在两种情况下都取得该主键行的排他锁,而普通 `SELECT FOR UPDATE`
        在行不存在时(TiDB)一把锁都不加。

        ★ lo/hi 归一化:同一对玩家无论谁发起,必须落到**同一行**,否则守卫形同虚设。
        """
        lo, hi = (a, b) if a <= b else (b, a)
        await cur.execute(
            "INSERT INTO friend_pair_guards (lo_id, hi_id) VALUES (%s, %s) "
            "ON DUPLICATE KEY UPDATE lo_id = lo_id",
            (lo, hi),
        )

    @staticmethod
    async def _acquire_player_guard(cur, player_id: int) -> None:  # noqa: ANN001
        """取单玩家守卫行锁(该玩家限额域的写串行化)。"""
        await cur.execute(
            "INSERT INTO friend_player_guards (player_id) VALUES (%s) "
            "ON DUPLICATE KEY UPDATE player_id = player_id",
            (player_id,),
        )

    # ── CreateRequest ────────────────────────────────────────────────────────

    async def create_request(
        self, request_id: int, requester_id: int, target_id: int, max_incoming: int
    ) -> tuple[int, bool]:
        """创建好友申请。返回 (request_id, 是否新建)。

        ★ 顺序是契约(见文件头 ②),**不要重排**:
            1. pair 守卫    ← 与同对的 Block/Accept 串行化
            2. player 守卫  ← 必须在任何锁定读之前(死锁根因)
            3. block 探针   ← 以下三条都是锁定读
            4. friendship 探针
            5. 既有请求行
            6. 限额校验 + INSERT
        """
        async with self._write_tx() as cur:
            # 1 & 2:两把守卫,无条件取。
            await self._acquire_pair_guard(cur, requester_id, target_id)
            await self._acquire_player_guard(cur, target_id)

            # 3. 双向拉黑探针。任一方向拉黑都不允许申请。
            await cur.execute(
                "SELECT 1 FROM blocks "
                "WHERE (player_id = %s AND blocked_id = %s) "
                "   OR (player_id = %s AND blocked_id = %s) LIMIT 1 FOR UPDATE",
                (requester_id, target_id, target_id, requester_id),
            )
            if await cur.fetchone() is not None:
                raise errcode.PandoraError(
                    errcode.ErrFriendBlocked,
                    "blocked between %d and %d",
                    requester_id,
                    target_id,
                )

            # 4. 已是好友探针。
            await cur.execute(
                "SELECT 1 FROM friendships WHERE player_id = %s AND friend_id = %s "
                "LIMIT 1 FOR UPDATE",
                (requester_id, target_id),
            )
            if await cur.fetchone() is not None:
                raise errcode.PandoraError(
                    errcode.ErrFriendAlreadyAdded,
                    "already friends: %d-%d",
                    requester_id,
                    target_id,
                )

            # 5. 既有请求行(锁定读)。
            await cur.execute(
                "SELECT request_id, status FROM friend_requests "
                "WHERE requester_id = %s AND target_id = %s FOR UPDATE",
                (requester_id, target_id),
            )
            existing = await cur.fetchone()

            if existing is None:
                # 6. 新增前先校验 target 收件箱未满(§9.18)。
                await self._check_incoming_limit(cur, target_id, max_incoming)
                await cur.execute(
                    "INSERT INTO friend_requests (request_id, requester_id, target_id, status) "
                    "VALUES (%s, %s, %s, %s)",
                    (request_id, requester_id, target_id, REQUEST_STATUS_PENDING),
                )
                return request_id, True

            existing_id, status = int(existing[0]), int(existing[1])
            if status == REQUEST_STATUS_PENDING:
                # 重复申请同一目标:幂等返回既有 pending,**不占新名额**。
                return existing_id, False

            # 历史请求已被拒/已接受 → 复用旧行复活成 pending,同样要过限额。
            await self._check_incoming_limit(cur, target_id, max_incoming)
            # ★ 复用的是**行**,不是 ID:request_id 必须轮换成本次的新 ID,created_at 一并刷新。
            #   推送是 at-least-once,客户端按 (request_id, reason) 判重 —— 沿用旧 ID 会让
            #   「申请→拒绝→再次申请」的新推送被当成重投丢弃(玩家永远收不到第二次申请);
            #   列表按 created_at 排序,沿用首次申请时间会把这次申请排到陈旧位置。
            #   旧 ID 随之失效,迟到的旧 Accept 自然查无此请求 —— 这正是想要的。
            await cur.execute(
                "UPDATE friend_requests SET request_id = %s, status = %s, "
                "created_at = NOW(), updated_at = NOW() WHERE request_id = %s",
                (request_id, REQUEST_STATUS_PENDING, existing_id),
            )
            return request_id, True

    @staticmethod
    async def _check_incoming_limit(cur, target_id: int, max_incoming: int) -> None:  # noqa: ANN001
        """校验 target 的「收到的待处理申请」上限(§9.18)。

        前置条件:调用方**已在任何锁定读之前**取得 target 的 player 守卫。
        这里刻意不再自取 —— 2026-08-11 的 1213 死锁根因正是"守卫在此处才取",
        那时三条 FOR UPDATE 探针的间隙锁已经拿在手里,取得再早也来不及。

        COUNT 用锁定读拿当前读:普通 COUNT 在 RR 陈旧快照下会漏计守卫等待期间提交的
        pending(R9 复审 P1)。
        """
        if max_incoming <= 0:
            return
        await cur.execute(
            "SELECT COUNT(*) FROM friend_requests WHERE target_id = %s AND status = %s "
            "FOR UPDATE",
            (target_id, REQUEST_STATUS_PENDING),
        )
        row = await cur.fetchone()
        count = int(row[0]) if row else 0
        if count >= max_incoming:
            raise errcode.PandoraError(
                errcode.ErrFriendRequestLimit,
                "incoming friend request limit reached for %d (max %d)",
                target_id,
                max_incoming,
            )

    # ── AcceptRequest ────────────────────────────────────────────────────────

    async def accept_request(
        self, request_id: int, actor_id: int, max_friends: int
    ) -> tuple[int, int]:
        """接受好友申请。返回 (requester_id, target_id)。

        ★ 锁序必须与 create_request / block 完全一致(守卫行 → 业务行),见文件头 ②。
        ★ 双方的好友数上限**都要校验** —— 只校验一方会让另一方越界。
        """
        async with self._write_tx() as cur:
            # 0. 预读请求行**不加锁** —— 只为拿到 pair 身份(守卫行的 key 需要它)。
            #    这里若像其它路径那样直接 FOR UPDATE,锁序就成了「业务行 → 守卫行」,
            #    与 create_request/block 的「守卫行 → 业务行」反向:同一 pair 上
            #    「重新申请」与「接受」并发即构成 ABBA 环(A 持守卫等请求行,
            #    B 持请求行等守卫)。_write_tx **没有** 1213/1205 重试兜底,
            #    环一成就是一个错误直接抛给玩家。行内容随后在守卫锁内重读复核。
            await cur.execute(
                "SELECT requester_id, target_id FROM friend_requests WHERE request_id = %s",
                (request_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrFriendNotFound, "request %d not found", request_id
                )
            requester_id, target_id = int(row[0]), int(row[1])

            if actor_id != target_id:
                # ★ 与 Go 侧一致地返回 ErrFriendNotFound 而**不是** ErrUnauthorized:
                # 后者等于告诉调用方「这条申请确实存在」,是信息泄露 ——
                # 非 target 无从区分"没这条申请"和"有但不是给你的"。
                raise errcode.PandoraError(
                    errcode.ErrFriendNotFound,
                    "request %d not for %d",
                    request_id,
                    actor_id,
                )

            # 1 & 2:两把守卫。两个 player 守卫按 **ID 升序**取 —— 固定全局顺序
            # 才不会与另一个反向 Accept 形成环。
            await self._acquire_pair_guard(cur, requester_id, target_id)
            for pid in sorted((requester_id, target_id)):
                await self._acquire_player_guard(cur, pid)

            # 3. 守卫锁内锁请求行并复核:预读与取得守卫之间行可能已被并发改写
            #    (拉黑置 rejected / 另一次 accept / 重新申请轮换了 request_id → 查无此行)。
            #    预读的值一律作废,以下判定全部基于这次锁定读的结果。
            await cur.execute(
                "SELECT requester_id, target_id, status FROM friend_requests "
                "WHERE request_id = %s FOR UPDATE",
                (request_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrFriendNotFound, "request %d not found", request_id
                )
            requester_id, target_id, status = int(row[0]), int(row[1]), int(row[2])
            if actor_id != target_id:
                raise errcode.PandoraError(
                    errcode.ErrFriendNotFound,
                    "request %d not for %d",
                    request_id,
                    actor_id,
                )
            if status != REQUEST_STATUS_PENDING:
                raise errcode.PandoraError(
                    errcode.ErrFriendNotFound,
                    "request %d is not pending (status=%d)",
                    request_id,
                    status,
                )

            # 4. block 权威校验(Go friend_repo.go 步骤 4)。
            #
            # 看起来多余 —— Block 已经把两个方向的 pending 置成 rejected,上面
            # `status != PENDING` 似乎就挡住了。但那是**已提交**的 Block;真正要挡的是
            # 与本事务交错的那一笔:Block 先删好友边、其"置 rejected"还卡在请求行锁上
            # 等本事务,而本事务此时插好友边 —— 两笔都提交后就是「既好友又拉黑」。
            # pair 守卫把两者全序化,但全序不等于本事务能看见对方的结果:必须自己再查一次。
            # ★ 必须是**锁定读**:步骤 0 的普通预读已经把快照定在取守卫之前,
            #   普通 SELECT 看不到守卫等待期间提交的 Block。
            await cur.execute(
                "SELECT 1 FROM blocks "
                "WHERE (player_id = %s AND blocked_id = %s) "
                "   OR (player_id = %s AND blocked_id = %s) LIMIT 1 FOR UPDATE",
                (actor_id, requester_id, requester_id, actor_id),
            )
            if await cur.fetchone() is not None:
                raise errcode.PandoraError(
                    errcode.ErrFriendBlocked,
                    "blocked between %d and %d",
                    actor_id,
                    requester_id,
                )

            # 5. 好友上限权威校验(双方都要校,只校一方会让另一方越界)。
            for pid in sorted((requester_id, target_id)):
                await self._check_friend_limit(cur, pid, max_friends)

            await cur.execute(
                "UPDATE friend_requests SET status = %s WHERE request_id = %s",
                (REQUEST_STATUS_ACCEPTED, request_id),
            )
            # 好友关系双向各一行(查询侧只需单向索引)。
            await cur.execute(
                "INSERT IGNORE INTO friendships (player_id, friend_id) VALUES (%s, %s), (%s, %s)",
                (requester_id, target_id, target_id, requester_id),
            )
            # 6. 反向 pending 一并终结(Go friend_repo.go:465-470,R5 复审 P2-8)。
            #
            # A→B 与 B→A 可以各自 pending。本次接受已经让双方成为好友,反向那条申请的
            # 结果同样是"好友已建立",按 accepted 收敛;必须在**同一 pair 守卫内**做,
            # 否则它会一直挂在对方收件箱里,被接受时对已是好友的两人重复走一遍建边流程
            # (INSERT IGNORE 不报错,于是又推一次"XX 接受了你的好友申请")。
            await cur.execute(
                "UPDATE friend_requests SET status = %s "
                "WHERE requester_id = %s AND target_id = %s AND status = %s",
                (REQUEST_STATUS_ACCEPTED, target_id, requester_id, REQUEST_STATUS_PENDING),
            )
            return requester_id, target_id

    @staticmethod
    async def _check_friend_limit(cur, player_id: int, max_friends: int) -> None:  # noqa: ANN001
        if max_friends <= 0:
            return
        await cur.execute(
            "SELECT COUNT(*) FROM friendships WHERE player_id = %s FOR UPDATE", (player_id,)
        )
        row = await cur.fetchone()
        if row and int(row[0]) >= max_friends:
            raise errcode.PandoraError(
                errcode.ErrFriendLimit,
                "friend limit reached for %d (max %d)",
                player_id,
                max_friends,
            )

    # ── RejectRequest ────────────────────────────────────────────────────────

    async def reject_request(self, request_id: int, actor_id: int) -> tuple[int, int]:
        async with self._write_tx() as cur:
            await cur.execute(
                "SELECT requester_id, target_id, status FROM friend_requests "
                "WHERE request_id = %s FOR UPDATE",
                (request_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrFriendNotFound, "request %d not found", request_id
                )
            requester_id, target_id, status = int(row[0]), int(row[1]), int(row[2])
            if actor_id != target_id:
                # ★ 与 Go 侧一致地返回 ErrFriendNotFound 而**不是** ErrUnauthorized:
                # 后者等于告诉调用方「这条申请确实存在」,是信息泄露 ——
                # 非 target 无从区分"没这条申请"和"有但不是给你的"。
                raise errcode.PandoraError(
                    errcode.ErrFriendNotFound,
                    "request %d not for %d",
                    request_id,
                    actor_id,
                )
            if status != REQUEST_STATUS_PENDING:
                raise errcode.PandoraError(
                    errcode.ErrFriendNotFound,
                    "request %d is not pending",
                    request_id,
                )
            await cur.execute(
                "UPDATE friend_requests SET status = %s WHERE request_id = %s",
                (REQUEST_STATUS_REJECTED, request_id),
            )
            return requester_id, target_id

    # ── Block ────────────────────────────────────────────────────────────────

    async def block(self, player_id: int, blocked_id: int, max_blocks: int) -> None:
        """拉黑。★ 同时**删除既有好友关系与 pending 申请** —— 拉黑必须是彻底的。

        锁序与 create_request 一致(pair → player)。
        """
        async with self._write_tx() as cur:
            await self._acquire_pair_guard(cur, player_id, blocked_id)
            await self._acquire_player_guard(cur, player_id)

            if max_blocks > 0:
                # ★ 顺序是契约:**先查是否已拉黑,只有新拉黑才查配额**(对齐 Go
                # friend_repo.go:589-606)。
                #
                # 少了这一步,已拉黑的目标在名单满时会被**误拒**:玩家再点一次
                # "拉黑"(客户端重试、双击、或换设备重放)拿到 ERR_FRIEND_BLOCK_LIMIT,
                # 而那个人**明明已经在黑名单里**。拉黑本该是幂等操作,配额限的是
                # "名单能有多长",不是"能点几次"。
                await cur.execute(
                    "SELECT 1 FROM blocks WHERE player_id = %s AND blocked_id = %s "
                    "LIMIT 1 FOR UPDATE",
                    (player_id, blocked_id),
                )
                already = await cur.fetchone()
                if already is None:
                    await cur.execute(
                        "SELECT COUNT(*) FROM blocks WHERE player_id = %s FOR UPDATE",
                        (player_id,),
                    )
                    row = await cur.fetchone()
                    if row and int(row[0]) >= max_blocks:
                        raise errcode.PandoraError(
                            errcode.ErrFriendBlockLimit,
                            "block limit reached for %d (max %d)",
                            player_id,
                            max_blocks,
                        )

            await cur.execute(
                "INSERT IGNORE INTO blocks (player_id, blocked_id) VALUES (%s, %s)",
                (player_id, blocked_id),
            )
            # 拉黑即解除关系:双向删好友 + 作废两个方向的 pending 申请。
            await cur.execute(
                "DELETE FROM friendships WHERE (player_id = %s AND friend_id = %s) "
                "OR (player_id = %s AND friend_id = %s)",
                (player_id, blocked_id, blocked_id, player_id),
            )
            await cur.execute(
                "UPDATE friend_requests SET status = %s "
                "WHERE status = %s AND ((requester_id = %s AND target_id = %s) "
                "OR (requester_id = %s AND target_id = %s))",
                (
                    REQUEST_STATUS_REJECTED,
                    REQUEST_STATUS_PENDING,
                    player_id,
                    blocked_id,
                    blocked_id,
                    player_id,
                ),
            )

    async def unblock(self, player_id: int, blocked_id: int) -> None:
        async with self._write_tx() as cur:
            await cur.execute(
                "DELETE FROM blocks WHERE player_id = %s AND blocked_id = %s",
                (player_id, blocked_id),
            )

    # ── 读 ───────────────────────────────────────────────────────────────────

    async def list_friends(self, player_id: int, limit: int = 0) -> list[tuple[int, int]]:
        """好友列表 → [(friend_id, since_ms)]。★ SQL LIMIT 兜底(§9.18 读取侧上限)。

        ★ 排序 `created_at DESC` 与 Go 的 ListFriends 逐字一致(不是按 id)。
        两个实现喂的是同一个客户端面板:一个按"最近加的在前"、另一个按 id 升序,
        玩家会看到"换个副本好友顺序就变了",而两边都不报错。
        """
        capped = min(limit, LIST_READ_HARD_LIMIT) if limit > 0 else LIST_READ_HARD_LIMIT
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT friend_id, UNIX_TIMESTAMP(created_at)*1000 FROM friendships "
                "WHERE player_id = %s ORDER BY created_at DESC LIMIT %s",
                (player_id, capped),
            )
            return [(int(r[0]), int(r[1] or 0)) for r in await cur.fetchall()]

    async def list_incoming_requests(
        self, player_id: int, limit: int = 0
    ) -> list[tuple[int, int, int]]:
        """发给本人且仍 pending 的申请 → [(request_id, requester_id, created_ms)]。"""
        capped = min(limit, LIST_READ_HARD_LIMIT) if limit > 0 else LIST_READ_HARD_LIMIT
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT request_id, requester_id, UNIX_TIMESTAMP(created_at)*1000 "
                "FROM friend_requests WHERE target_id = %s AND status = %s "
                "ORDER BY created_at DESC LIMIT %s",
                (player_id, REQUEST_STATUS_PENDING, capped),
            )
            return [(int(r[0]), int(r[1]), int(r[2] or 0)) for r in await cur.fetchall()]

    async def list_blocks(self, player_id: int, limit: int = 0) -> list[tuple[int, int]]:
        """黑名单 → [(blocked_id, since_ms)]。"""
        capped = min(limit, LIST_READ_HARD_LIMIT) if limit > 0 else LIST_READ_HARD_LIMIT
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT blocked_id, UNIX_TIMESTAMP(created_at)*1000 FROM blocks "
                "WHERE player_id = %s ORDER BY created_at DESC LIMIT %s",
                (player_id, capped),
            )
            return [(int(r[0]), int(r[1] or 0)) for r in await cur.fetchall()]

    async def get_request(self, request_id: int) -> tuple[int, int, int, int] | None:
        """读一行好友申请 → (request_id, requester_id, target_id, status);无行返回 None。

        biz 的 fail-fast 预检用(非权威)。**不加锁**:权威判定一律在
        accept_request / reject_request 的守卫锁内重做。
        """
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT request_id, requester_id, target_id, status FROM friend_requests "
                "WHERE request_id = %s LIMIT 1",
                (request_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return int(row[0]), int(row[1]), int(row[2]), int(row[3])

    async def are_friends(self, a: int, b: int) -> bool:
        """单向查一行即可 —— 好友边双向落库,存在性等价。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM friendships WHERE player_id = %s AND friend_id = %s LIMIT 1",
                (a, b),
            )
            return await cur.fetchone() is not None

    async def is_blocked(self, a: int, b: int) -> bool:
        """**任一方向**拉黑都算 —— 只查单向会让"被对方拉黑的人还能发申请"。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM blocks "
                "WHERE (player_id = %s AND blocked_id = %s) "
                "   OR (player_id = %s AND blocked_id = %s) LIMIT 1",
                (a, b, b, a),
            )
            return await cur.fetchone() is not None

    async def count_friends(self, player_id: int) -> int:
        """好友数(AddFriend 提前失败用,**非权威**:权威在 accept 事务的守卫锁内)。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*) FROM friendships WHERE player_id = %s", (player_id,)
            )
            row = await cur.fetchone()
        return int(row[0]) if row else 0

    async def remove_friend(self, player_id: int, target_id: int) -> None:
        """删双向好友边(幂等:删不到行不报错)。不动黑名单 / 申请。

        单条 DELETE 覆盖两个方向 —— 拆成两条会出现"只删掉一边"的半边关系
        (A 的列表里没有 B,B 的列表里还有 A),而两条语句都返回成功。
        """
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM friendships WHERE (player_id = %s AND friend_id = %s) "
                "OR (player_id = %s AND friend_id = %s)",
                (player_id, target_id, target_id, player_id),
            )

    # ── 推荐好友 ─────────────────────────────────────────────────────────────

    @staticmethod
    def _exclude_clause(col: str, exclude) -> tuple[str, list]:  # noqa: ANN001
        """把 exclude 列表拼成 `AND col NOT IN (%s,%s,...)`。空列表 → 空串。

        占位符按元素个数生成,值仍走参数绑定 —— 绝不把 id 拼进 SQL 文本。
        """
        if not exclude:
            return "", []
        placeholders = ",".join(["%s"] * len(exclude))
        return f" AND {col} NOT IN ({placeholders})", [int(x) for x in exclude]

    async def recommend_by_mutual(
        self, player_id: int, exclude, limit: int
    ) -> list[tuple[int, int]]:  # noqa: ANN001
        """熟人策略:好友的好友(FOF),按共同好友数降序、同数随机。

        排除:自己 / 已是我好友 / 任一方向拉黑 / 任一方向 pending 申请 / exclude。
        少任何一条排除,推荐面板就会把"已经是好友的人"或"刚拉黑的人"推回来。
        """
        ex_clause, ex_args = self._exclude_clause("f2.friend_id", exclude)
        query = (
            "SELECT f2.friend_id, COUNT(*) AS mutual "
            "FROM friendships f1 "
            "JOIN friendships f2 ON f1.friend_id = f2.player_id "
            "WHERE f1.player_id = %s "
            "  AND f2.friend_id <> %s "
            "  AND f2.friend_id NOT IN (SELECT friend_id FROM friendships WHERE player_id = %s) "
            "  AND NOT EXISTS (SELECT 1 FROM blocks b "
            "        WHERE (b.player_id = %s AND b.blocked_id = f2.friend_id) "
            "           OR (b.player_id = f2.friend_id AND b.blocked_id = %s)) "
            "  AND NOT EXISTS (SELECT 1 FROM friend_requests r "
            "        WHERE r.status = " + str(REQUEST_STATUS_PENDING) + " "
            "          AND ((r.requester_id = %s AND r.target_id = f2.friend_id) "
            "            OR (r.requester_id = f2.friend_id AND r.target_id = %s)))"
            + ex_clause
            + " GROUP BY f2.friend_id ORDER BY mutual DESC, RAND() LIMIT %s"
        )
        args = [player_id] * 7 + ex_args + [limit]
        return await self._scan_recommend(query, args)

    async def recommend_random(
        self, player_id: int, exclude, limit: int
    ) -> list[tuple[int, int]]:  # noqa: ANN001
        """兜底策略:好友图里随机锚点正向扫,mutual 恒 0。

        ★ 绝不全表扫,且 pivot 必须落进真实 id 区间:player_id 是雪花 ID(集中在当前
        高位窗口),直接取随机 int64 几乎必然落在现有最大 id 之后 → 一个候选都扫不到。
        先用索引 MIN/MAX 取真实区间(O(1)),pivot ∈ [min,max] 保证 >=pivot 必有行;
        尾部不满返回偏少可接受(由下一个策略或下次刷新补)。
        """
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute("SELECT MIN(player_id), MAX(player_id) FROM friendships")
            row = await cur.fetchone()
        if not row or row[1] is None or int(row[1]) == 0:
            return []  # 空表,无兜底候选
        lo, hi = int(row[0]), int(row[1])
        pivot = lo if hi <= lo else lo + random.randint(0, hi - lo)

        ex_clause, ex_args = self._exclude_clause("player_id", exclude)
        query = (
            "SELECT player_id, 0 AS mutual FROM friendships "
            "WHERE player_id >= %s "
            "  AND player_id <> %s "
            "  AND player_id NOT IN (SELECT friend_id FROM friendships WHERE player_id = %s) "
            "  AND NOT EXISTS (SELECT 1 FROM blocks b "
            "        WHERE (b.player_id = %s AND b.blocked_id = friendships.player_id) "
            "           OR (b.player_id = friendships.player_id AND b.blocked_id = %s)) "
            "  AND NOT EXISTS (SELECT 1 FROM friend_requests r "
            "        WHERE r.status = " + str(REQUEST_STATUS_PENDING) + " "
            "          AND ((r.requester_id = %s AND r.target_id = friendships.player_id) "
            "            OR (r.requester_id = friendships.player_id AND r.target_id = %s)))"
            + ex_clause
            + " GROUP BY player_id ORDER BY player_id LIMIT %s"
        )
        args = [pivot] + [player_id] * 6 + ex_args + [limit]
        return await self._scan_recommend(query, args)

    async def _scan_recommend(self, query: str, args: list) -> list[tuple[int, int]]:
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(query, tuple(args))
            return [(int(r[0]), int(r[1])) for r in await cur.fetchall()]

    # ── 保留期清理(§9.24)────────────────────────────────────────────────────

    async def _schema_name(self, cur) -> str:  # noqa: ANN001
        """清理语句要写全限定表名;库名以**实际连上的库**为准并缓存一次。"""
        if not self._schema:
            await cur.execute("SELECT DATABASE()")
            row = await cur.fetchone()
            self._schema = str(row[0]) if row and row[0] else ""
        return self._schema

    async def sweep_terminal_requests_before(
        self, mode: dbguard.Mode, retention_days: int, limit: int
    ) -> dbguard.Outcome:
        """终态(≠pending)且 updated_at 超保留期的申请行。

        ★ pending 无论如何都不在处理范围:它是玩家的待办,不是历史垃圾。
        真删语义:删后再次发起 = 全新 INSERT pending,行为等价(好友关系权威在
        friendships,请求行无资产语义)。
        mode 默认 report_only —— 只统计 + WARN,一行都不删(§9.24 用户指令)。
        """
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                schema = await self._schema_name(cur)
            # where 只写一遍并交给 dbguard —— Count 与 Delete 共用同一份条件,
            # 从机制上排除"报告说 0 行、实际删了 10 万行"的条件漂移(§9.24)。
            return await dbguard.sweep_table(
                conn,
                mode,
                schema,
                "friend_requests",
                "status <> %s AND updated_at < DATE_SUB(NOW(), INTERVAL %s DAY)",
                limit,
                REQUEST_STATUS_PENDING,
                retention_days,
            )

    async def delete_pair_guards_before(self, retention_days: int, limit: int) -> int:
        """删超保留期的关系对守卫行(R9 复审 P1)。

        守卫行仅是锁载体:正被事务持有的行 DELETE 会阻塞到它提交,下次 acquire
        重新 INSERT —— 所以任意时刻删除都安全,保留期只为限制表规模。
        """
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM friend_pair_guards "
                "WHERE created_at < DATE_SUB(NOW(), INTERVAL %s DAY) LIMIT %s",
                (retention_days, limit),
            )
            return int(cur.rowcount or 0)


__all__ = ["MySQLFriendRepo", "LIST_READ_HARD_LIMIT", "WRITE_TX_ISOLATION", "mysqlx"]
