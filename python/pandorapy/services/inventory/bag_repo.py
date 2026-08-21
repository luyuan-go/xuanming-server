"""背包域数据层(pandora_bag 库)—— 对应 Go 侧 internal/data/bag_repo.go +
bag_capacity.go(bag-domain.md §4)。

库表(deploy/mysql-init/14-bag-tables.sql):

    bag_meta        每玩家 fencing 锚点(owner_epoch 单调 CAS + last_journal_seq 水位)
    bag_checkpoint  随身组快照(pb BagStorageRecord blob)
    bag_section     后端驻留段本体(仓库 / 活动段;pb BagSection blob,与 journal 同事务变更)
    bag_journal     背包流水(uk player+seq / uk player+idem 双去重;fingerprint 防 key 复用)
    bag_generation  活动段代际权威
    bag_capacity    每玩家每段已购容量增量(§5.3)

★ 一致性(五要件,CLAUDE.md §9.6)

  每个写事务**先** `SELECT ... FOR UPDATE` 锁 bag_meta 行:owner_epoch 单调 CAS
  (旧 epoch 拒,新 epoch 推进),同时天然串行化同一玩家的全部背包写(§9.22 禁"先查再存")。
  活动段写校验 bag_generation.current_generation,不符 fail-closed 整批拒。
  journal 前缀确认:批内 seq 升序,<= 水位的条目视为重放跳过,应用后推进水位。
  涉及后端驻留段的 op 在同一事务里改 bag_section(转移 / 领取 / 使用零撕裂)。

★ read-modify-write 路径**禁止丢弃 unknown fields**(§9 不变量 17)

  bag_section / bag_checkpoint 是读出来改完再写回的 pb blob。滚动更新期新副本写的
  新字段若被旧副本回写时丢掉,玩家数据会**静默少一块**且两边都不报错。
  protobuf-python 的 `ParseFromString` 默认保留 unknown fields —— 这里不调用
  任何 `DiscardUnknownFields`,也不许后来人"顺手清理"。

★ 尾部连续性校验(INC-20260722-003)

  恢复 = 快照 + (covered, last] 连续重放,journal_seq 每玩家单调连续。任何缺口
  (误删 / 损坏 / 越权清理)都意味着加载会**静默少资产** —— 必须拒绝加载并把缺口
  暴露给告警排查,绝不静默继续。
"""

from __future__ import annotations

import hashlib
import hmac

from pandora.bag.v1 import bag_pb2

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import mysqlx
from pandorapy.services.inventory import bag_apply as bapply
from pandorapy.services.inventory import bag_migration as bmig
from pandorapy.services.inventory import repo_sql as rsql

# 一小时滑窗额度用的 SQL 片段与 Go 逐字相同(NOW() 由服务端时钟裁决,不信调用方)。
_COUNT_RECENT_JOURNAL = (
    "SELECT COUNT(*) FROM bag_journal "
    "WHERE player_id = %s AND created_at > (NOW() - INTERVAL 1 HOUR)"
)

_INSERT_JOURNAL = (
    "INSERT INTO bag_journal "
    "(player_id, journal_seq, owner_epoch, op_type, bag_type, generation, payload, "
    "idempotency_key, fingerprint) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
)

_UPSERT_SECTION = (
    "INSERT INTO bag_section (player_id, bag_type, generation, section) VALUES (%s, %s, %s, %s) "
    "ON DUPLICATE KEY UPDATE generation = VALUES(generation), section = VALUES(section)"
)

_UPSERT_CHECKPOINT = (
    "INSERT INTO bag_checkpoint (player_id, snapshot, covered_journal_seq) VALUES (%s, %s, %s) "
    "ON DUPLICATE KEY UPDATE snapshot = VALUES(snapshot), "
    "covered_journal_seq = VALUES(covered_journal_seq)"
)

# 删除资格 = 超保留期 **且** 已被该玩家 checkpoint 覆盖(INC-20260722-003):
# 恢复 = 快照 + (covered, last] 尾部重放,未覆盖尾部是唯一恢复数据,时间到期也绝不删;
# 时间阈值只是附加条件,覆盖水位才是删除资格。无 checkpoint 行的玩家 INNER JOIN
# 不命中,任何流水都不删。covered_journal_seq 只单调前进(save_checkpoint 拒回退),
# 子查询一致性读读到旧值只会少删(安全方向),与 save_checkpoint 并发无需额外锁。
# 多表 DELETE 不支持 LIMIT → 派生表选主键再删(短事务小批量,§9.24)。
_SWEEP_JOURNAL = """
DELETE FROM bag_journal WHERE id IN (
  SELECT id FROM (
    SELECT j.id
    FROM bag_journal j
    JOIN bag_checkpoint c ON c.player_id = j.player_id
    WHERE j.created_at < (NOW() - INTERVAL %s SECOND)
      AND j.journal_seq <= c.covered_journal_seq
    ORDER BY j.id
    LIMIT %s
  ) pick)"""


def bag_entry_fingerprint(payload: bytes) -> str:
    """流水内容指纹(payload 原文 sha256 hex;同 key 不同内容 → 幂等冲突)。

    与 Go 的 bagEntryFingerprint 同口径:对**序列化后的字节**求哈希,不是对字段拼串。
    """
    return hashlib.sha256(payload).hexdigest()


async def _lock_bag_meta_tx(cur, player_id: int, req_epoch: int) -> int:  # noqa: ANN001
    """在事务里确保并锁定 bag_meta 行,执行 owner_epoch 单调 CAS,返回当前水位。

      - req_epoch < 存量 → ErrBagEpochFenced(失租旧写)
      - req_epoch > 存量 → 推进(新 owner checkout / 首写)

    锁行同时串行化同一玩家的全部背包写。
    """
    try:
        await cur.execute(
            "INSERT IGNORE INTO bag_meta (player_id, owner_epoch, last_journal_seq) "
            "VALUES (%s, 0, 0)",
            (player_id,),
        )
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal, "ensure bag_meta player=%d: %s", player_id, exc
        ) from exc
    try:
        await cur.execute(
            "SELECT owner_epoch, last_journal_seq FROM bag_meta WHERE player_id = %s FOR UPDATE",
            (player_id,),
        )
        row = await cur.fetchone()
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal, "lock bag_meta player=%d: %s", player_id, exc
        ) from exc
    if row is None:
        # INSERT IGNORE 之后行必然存在;消失只可能是并发 DELETE(越权清理)。
        raise errcode.PandoraError(
            errcode.ErrInternal, "lock bag_meta player=%d: row vanished", player_id
        )
    stored_epoch, last_seq = int(row[0]), int(row[1])
    if req_epoch < stored_epoch:
        # 失租旧 DS 的迟到写在存储侧 owner_epoch 单调 CAS 被拒(§9.22 fencing / 脑裂防线)。
        # ErrBagEpochFenced 是业务码,不被 access log 中间件当故障 → 在此显式 WARN 留证。
        plog.get().warning(
            "bag_owner_epoch_fenced",
            player_id=player_id,
            req_epoch=req_epoch,
            current_epoch=stored_epoch,
        )
        raise errcode.PandoraError(
            errcode.ErrBagEpochFenced,
            "stale owner epoch player=%d req=%d current=%d",
            player_id, req_epoch, stored_epoch,
        )
    if req_epoch > stored_epoch:
        try:
            await cur.execute(
                "UPDATE bag_meta SET owner_epoch = %s WHERE player_id = %s",
                (req_epoch, player_id),
            )
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "advance bag epoch player=%d: %s", player_id, exc
            ) from exc
    return last_seq


async def _read_capacity_extra_tx(cur, player_id: int, bag_type: int) -> int:  # noqa: ANN001
    """事务内读某段已购增量(append_journal 有效容量判定用;无行 = 0)。"""
    try:
        await cur.execute(
            "SELECT extra FROM bag_capacity WHERE player_id = %s AND bag_type = %s",
            (player_id, bag_type),
        )
        row = await cur.fetchone()
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "read capacity extra player=%d bag=%d: %s",
            player_id, bag_type, exc,
        ) from exc
    return int(row[0]) if row else 0


def collect_backend_bag_types(entries) -> list[int]:  # noqa: ANN001
    """一批 entries 触及的后端驻留段类型(主段 + 转移目标段 + 使用产出段)。"""
    seen: set[int] = set()

    def _add(bag_type: int) -> None:
        if bapply.is_backend_resident_bag_type(bag_type):
            seen.add(bag_type)

    for entry in entries:
        _add(entry.bag_type)
        which = entry.WhichOneof("op")
        if which == "transfer":
            _add(entry.transfer.to_bag_type)
        elif which == "consume":
            _add(entry.consume.produce_bag_type)
    return sorted(seen)


def effective_capacity_fn(base: bapply.CapacityFn, extras: dict[int, int]) -> bapply.CapacityFn:
    """把 base 容量回调包装成"base + 已购增量"的有效容量回调。

    base 为 0(未配置段)保持 0,fail-closed 语义不变;和溢出钳到 uint32 上限。
    """

    def _eff(bag_type: int) -> int:
        b = base(bag_type)
        if b == 0:
            return 0
        return min(b + extras.get(bag_type, 0), bapply.UINT32_MAX)

    return _eff


class MySQLBagRepo(bmig.BagSeederMixin):
    """基于 asyncmy 连接池的背包域仓储。对应 Go 的 MySQLBagRepo。

    ★ 池必须以 **autocommit=True** 建(见 main.py):与 Go 的 database/sql 默认语义一致;
      所有写路径显式 `rsql.transaction()` 包起来。
    """

    __slots__ = ("_pool", "_db")

    def __init__(self, pool, db: str) -> None:  # noqa: ANN001
        self._pool = pool
        # db 只被容量巡检 / 排障用到;SQL 里走连接当前库(与 Go 的裸表名一致)。
        self._db = db

    @property
    def db(self) -> str:
        return self._db

    # ── LoadBag ───────────────────────────────────────────────────────────

    async def load_bag(
        self, player_id: int, owner_epoch: int
    ) -> tuple[bytes, list[tuple[int, bytes]], int]:
        """加载随身组:epoch CAS(checkout 推进)+ checkpoint 快照 + covered 之后的
        journal 尾部 + 权威水位。新玩家返回空快照、空尾部、水位 0。

        返回 (snapshot_bytes, [(journal_seq, payload_bytes), ...], last_seq)。
        """
        async with rsql.transaction(self._pool) as cur:
            last_seq = await _lock_bag_meta_tx(cur, player_id, owner_epoch)

            try:
                await cur.execute(
                    "SELECT snapshot, covered_journal_seq FROM bag_checkpoint "
                    "WHERE player_id = %s",
                    (player_id,),
                )
                cp_row = await cur.fetchone()
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "read checkpoint player=%d: %s", player_id, exc
                ) from exc
            snapshot: bytes = bytes(cp_row[0] or b"") if cp_row else b""
            covered_seq = int(cp_row[1]) if cp_row else 0

            try:
                await cur.execute(
                    "SELECT journal_seq, payload FROM bag_journal "
                    "WHERE player_id = %s AND journal_seq > %s ORDER BY journal_seq",
                    (player_id, covered_seq),
                )
                rows = await cur.fetchall()
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "query journal tail player=%d: %s", player_id, exc
                ) from exc
            tail = [(int(r[0]), bytes(r[1] or b"")) for r in rows or ()]

            # 尾部连续性校验(INC-20260722-003 fail-closed),理由见模块头注释。
            expect = covered_seq
            for seq, _payload in tail:
                expect += 1
                if seq != expect:
                    plog.get().error(
                        "bag_journal_gap",
                        player_id=player_id,
                        expect_seq=expect,
                        got_seq=seq,
                        covered_seq=covered_seq,
                        last_seq=last_seq,
                    )
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "bag journal tail gap player=%d: expect seq %d got %d "
                        "(covered=%d last=%d), refusing lossy load",
                        player_id, expect, seq, covered_seq, last_seq,
                    )
            if expect != last_seq:
                plog.get().error(
                    "bag_journal_truncated",
                    player_id=player_id,
                    tail_end_seq=expect,
                    watermark_seq=last_seq,
                    covered_seq=covered_seq,
                )
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "bag journal tail truncated player=%d: tail ends at %d but watermark %d "
                    "(covered=%d), refusing lossy load",
                    player_id, expect, last_seq, covered_seq,
                )
        return snapshot, tail, last_seq

    # ── AppendJournal ─────────────────────────────────────────────────────

    async def append_journal(  # noqa: C901, PLR0912, PLR0915 —— 与 Go 同为一条线性事务
        self,
        player_id: int,
        owner_epoch: int,
        entries,  # noqa: ANN001 —— Sequence[BagJournalEntry]
        capacity: bapply.CapacityFn,
        max_stack: bapply.MaxStackFn,
        hourly_quota: int,
    ) -> int:
        """追加一批流水(单事务):epoch CAS + generation 校验 + 幂等去重 +
        后端驻留段同事务变更 + 水位推进。返回已应用水位(含本批;纯重放返回当前水位)。
        """
        async with rsql.transaction(self._pool) as cur:
            last_seq = await _lock_bag_meta_tx(cur, player_id, owner_epoch)

            # 有效容量(§5.3):事务内预取本批触及后端段的已购增量,base + extra 作判定容量
            # (判定与权威同址;bag_capacity 写路径同样先锁 bag_meta 行,天然串行无脏读)。
            extras: dict[int, int] = {}
            for bag_type in collect_backend_bag_types(entries):
                extra = await _read_capacity_extra_tx(cur, player_id, bag_type)
                if extra > 0:
                    extras[bag_type] = extra
            capacity = effective_capacity_fn(capacity, extras)

            # 额度(五要件④):单玩家滑窗流水条数封顶,压缩单实例被攻破 / 出 bug 的爆炸半径。
            if hourly_quota > 0:
                try:
                    await cur.execute(_COUNT_RECENT_JOURNAL, (player_id,))
                    qrow = await cur.fetchone()
                except Exception as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "count journal quota player=%d: %s",
                        player_id, exc,
                    ) from exc
                recent = int(qrow[0]) if qrow else 0
                if recent + len(entries) > hourly_quota:
                    # 额度封顶触发(五要件④限流):某玩家 / DS 在猛刷背包写路径,应能主动发现。
                    plog.get().warning(
                        "bag_journal_quota_exceeded",
                        player_id=player_id,
                        recent=recent,
                        batch=len(entries),
                        quota=hourly_quota,
                    )
                    raise errcode.PandoraError(
                        errcode.ErrBagQuotaExceeded,
                        "journal hourly quota exceeded player=%d recent=%d batch=%d quota=%d",
                        player_id, recent, len(entries), hourly_quota,
                    )

            # 代际权威一次性读取(事务内一致视图);仅活动段需要。
            generations: dict[int, int] = {}

            async def _load_generation(bag_type: int) -> int:
                if bag_type in generations:
                    return generations[bag_type]
                try:
                    await cur.execute(
                        "SELECT current_generation FROM bag_generation WHERE bag_type = %s",
                        (bag_type,),
                    )
                    grow = await cur.fetchone()
                except Exception as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrInternal, "read generation bag_type=%d: %s", bag_type, exc
                    ) from exc
                # 未登记的活动段:代际 0(活动开启前运营必须先写 bag_generation)。
                current = int(grow[0]) if grow else 0
                generations[bag_type] = current
                return current

            # 后端驻留段在事务内的工作副本(同一批多条 op 触同段时读改写复用,提交前统一落库)。
            sections: dict[int, bag_pb2.BagSection] = {}
            dirty: dict[int, bool] = {}

            async def _prepare_section(bag_type: int) -> None:
                """把某段的工作副本读进 sections(SQL 只在这里发生)。

                ★ 为什么要与 apply 阶段分开:bag_apply 的段加载回调是**同步**的
                  (它是纯内存变换,不该带 await),而读段必须走 await。所以先在这里
                  按 op 声明的段全部预载,apply 阶段只从 dict 里取。
                """
                if bag_type in sections:
                    return
                gen = await _load_generation(bag_type)
                sec = bag_pb2.BagSection(bag_type=bag_type, generation=gen)
                try:
                    await cur.execute(
                        "SELECT generation, section FROM bag_section "
                        "WHERE player_id = %s AND bag_type = %s FOR UPDATE",
                        (player_id, bag_type),
                    )
                    srow = await cur.fetchone()
                except Exception as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "lock section player=%d bag=%d: %s",
                        player_id, bag_type, exc,
                    ) from exc
                # 旧代行按"已逻辑清空"处理:从空段开始(物理回收交给后台 sweep,读写都不认旧代)。
                if srow is not None and (
                    not bapply.is_activity_bag_type(bag_type) or int(srow[0]) == gen
                ):
                    try:
                        # ★ 不 DiscardUnknown:read-modify-write 路径丢弃 unknown fields
                        #   会让旧副本回写时静默抹掉新副本写的字段(§9 不变量 17)。
                        sec.ParseFromString(bytes(srow[1] or b""))
                    except Exception as exc:  # noqa: BLE001
                        raise errcode.PandoraError(
                            errcode.ErrInternal,
                            "decode section player=%d bag=%d: %s",
                            player_id, bag_type, exc,
                        ) from exc
                    sec.bag_type = bag_type
                    sec.generation = gen
                sections[bag_type] = sec

            def _load_section(bag_type: int) -> bag_pb2.BagSection:
                sec = sections.get(bag_type)
                if sec is None:
                    # 预载遗漏 = 代码 bug,不是数据问题:fail-closed 而不是临时补读
                    # (补读会绕过 FOR UPDATE 的锁序,把死锁风险埋进业务路径)。
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "section not preloaded player=%d bag=%d",
                        player_id, bag_type,
                    )
                return sec

            for bag_type in collect_backend_bag_types(entries):
                await _prepare_section(bag_type)

            new_seq = last_seq
            prev_seq = 0
            applied = 0
            for entry in entries:
                seq = entry.journal_seq
                if seq == 0 or seq <= prev_seq:
                    raise errcode.PandoraError(
                        errcode.ErrBagSeqConflict,
                        "journal seq must be ascending player=%d seq=%d prev=%d",
                        player_id, seq, prev_seq,
                    )
                prev_seq = seq
                if seq <= last_seq:
                    continue  # 旧条目重放(at-least-once),已应用,跳过。

                payload = entry.SerializeToString()
                fp_hex = bag_entry_fingerprint(payload)

                # 活动段代际校验(fail-closed):切代后迟到写整批拒,旧物品不可能漏进新代。
                if bapply.is_activity_bag_type(entry.bag_type):
                    current = await _load_generation(entry.bag_type)
                    if entry.generation != current:
                        raise errcode.PandoraError(
                            errcode.ErrBagGenerationMismatch,
                            "generation mismatch player=%d bag=%d entry_gen=%d current=%d",
                            player_id, entry.bag_type, entry.generation, current,
                        )

                op_type = bapply.apply_bag_op(entry, _load_section, dirty, capacity, max_stack)

                try:
                    await cur.execute(
                        _INSERT_JOURNAL,
                        (
                            player_id, seq, owner_epoch, op_type, entry.bag_type,
                            entry.generation, payload, entry.idempotency_key, fp_hex,
                        ),
                    )
                except Exception as exc:  # noqa: BLE001
                    if not mysqlx.is_duplicate_entry(exc):
                        raise errcode.PandoraError(
                            errcode.ErrInternal,
                            "insert journal player=%d seq=%d: %s",
                            player_id, seq, exc,
                        ) from exc
                    # uk 命中:seq 冲突在 meta 行锁下不可能来自并发,只可能是 idem key 复用。
                    try:
                        await cur.execute(
                            "SELECT fingerprint FROM bag_journal "
                            "WHERE player_id = %s AND idempotency_key = %s",
                            (player_id, entry.idempotency_key),
                        )
                        frow = await cur.fetchone()
                    except Exception as ferr:  # noqa: BLE001
                        raise errcode.PandoraError(
                            errcode.ErrInternal,
                            "read journal idem player=%d key=%s: %s",
                            player_id, entry.idempotency_key, ferr,
                        ) from ferr
                    if frow is None:
                        raise errcode.PandoraError(
                            errcode.ErrInternal,
                            "read journal idem player=%d key=%s: row vanished after duplicate",
                            player_id, entry.idempotency_key,
                        ) from exc
                    stored_fp = str(frow[0] or "")
                    if not hmac.compare_digest(stored_fp, fp_hex):
                        # 同一幂等键被用于不同内容:客户端 bug / 重放攻击 / key 冲突。
                        # fail-closed 拒绝,但必须留证 —— 这是完整性信号,能发现异常写入方。
                        plog.get().warning(
                            "bag_idempotency_conflict",
                            player_id=player_id,
                            idempotency_key=entry.idempotency_key,
                        )
                        raise errcode.PandoraError(
                            errcode.ErrBagIdempotencyConflict,
                            "idempotency_key reused for different content player=%d key=%s",
                            player_id, entry.idempotency_key,
                        ) from exc
                    raise errcode.PandoraError(
                        errcode.ErrBagSeqConflict,
                        "idempotency_key already applied under another seq player=%d key=%s seq=%d",
                        player_id, entry.idempotency_key, seq,
                    ) from exc
                new_seq = seq
                applied += 1

            if applied == 0:
                # 整批旧条目重放:返回当前水位,安全可清(语义同 ReportProgress)。
                return last_seq

            # 落脏段(与 journal 同事务:转移 / 领取 / 使用零撕裂)。
            for bag_type, is_dirty in dirty.items():
                if not is_dirty:
                    continue
                sec = sections[bag_type]
                blob = sec.SerializeToString()
                try:
                    await cur.execute(
                        _UPSERT_SECTION, (player_id, bag_type, sec.generation, blob)
                    )
                except Exception as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "upsert section player=%d bag=%d: %s",
                        player_id, bag_type, exc,
                    ) from exc

            try:
                await cur.execute(
                    "UPDATE bag_meta SET last_journal_seq = %s WHERE player_id = %s",
                    (new_seq, player_id),
                )
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "advance watermark player=%d: %s", player_id, exc
                ) from exc
        return new_seq

    # ── SaveCheckpoint ────────────────────────────────────────────────────

    async def save_checkpoint(
        self, player_id: int, owner_epoch: int, snapshot: bytes, covered_seq: int
    ) -> None:
        """保存随身组快照:epoch CAS;covered_seq 不得回退、不得超已确认水位。"""
        async with rsql.transaction(self._pool) as cur:
            last_seq = await _lock_bag_meta_tx(cur, player_id, owner_epoch)
            if covered_seq > last_seq:
                raise errcode.PandoraError(
                    errcode.ErrBagCheckpointStale,
                    "covered_seq beyond watermark player=%d covered=%d last=%d",
                    player_id, covered_seq, last_seq,
                )
            try:
                await cur.execute(
                    "SELECT covered_journal_seq FROM bag_checkpoint WHERE player_id = %s "
                    "FOR UPDATE",
                    (player_id,),
                )
                row = await cur.fetchone()
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "read checkpoint covered player=%d: %s",
                    player_id, exc,
                ) from exc
            if row is not None and covered_seq < int(row[0]):
                raise errcode.PandoraError(
                    errcode.ErrBagCheckpointStale,
                    "covered_seq regressed player=%d covered=%d existing=%d",
                    player_id, covered_seq, int(row[0]),
                )
            try:
                await cur.execute(_UPSERT_CHECKPOINT, (player_id, snapshot, covered_seq))
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "upsert checkpoint player=%d: %s", player_id, exc
                ) from exc

    # ── GetSections ───────────────────────────────────────────────────────

    async def get_sections(
        self, player_id: int, bag_types, capacity: bapply.CapacityFn  # noqa: ANN001
    ) -> list[bag_pb2.BagSection]:
        """读后端驻留段(活动段按 current generation 过滤;无行返回空段)。

        返回的 capacity 为**有效容量**(base + 已购增量,§5.3)。
        """
        out: list[bag_pb2.BagSection] = []
        for bag_type in bag_types:
            current = 0
            if bapply.is_activity_bag_type(bag_type):
                async with self._pool.acquire() as conn, conn.cursor() as cur:
                    try:
                        await cur.execute(
                            "SELECT current_generation FROM bag_generation WHERE bag_type = %s",
                            (bag_type,),
                        )
                        grow = await cur.fetchone()
                    except Exception as exc:  # noqa: BLE001
                        raise errcode.PandoraError(
                            errcode.ErrInternal,
                            "read generation bag_type=%d: %s",
                            bag_type, exc,
                        ) from exc
                    current = int(grow[0]) if grow else 0

            # 有效容量(§5.3):base + 已购增量。
            extra, _purchases = await self.get_capacity_state(player_id, bag_type)
            eff_cap = effective_capacity_fn(capacity, {bag_type: extra})
            sec = bag_pb2.BagSection(
                bag_type=bag_type, generation=current, capacity=eff_cap(bag_type)
            )
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT generation, section FROM bag_section "
                        "WHERE player_id = %s AND bag_type = %s",
                        (player_id, bag_type),
                    )
                    srow = await cur.fetchone()
                except Exception as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "read section player=%d bag=%d: %s",
                        player_id, bag_type, exc,
                    ) from exc
            # 读过滤:活动段只认 current generation(切代即逻辑清空,旧代行返回空段)。
            if srow is not None and (
                not bapply.is_activity_bag_type(bag_type) or int(srow[0]) == current
            ):
                try:
                    sec.ParseFromString(bytes(srow[1] or b""))
                except Exception as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "decode section player=%d bag=%d: %s",
                        player_id, bag_type, exc,
                    ) from exc
                sec.bag_type = bag_type
                sec.generation = current
                sec.capacity = eff_cap(bag_type)
            out.append(sec)
        return out

    # ── 保留期清理(§9.24)─────────────────────────────────────────────────

    async def sweep_journal(self, retention_seconds: int, batch: int) -> int:
        """删除超保留期**且已被 checkpoint 覆盖**的流水(有界批量;返回删除行数)。

        ⚠️ 与 inventory 主库的清理不同,本路径**不走 dbguard.sweep_table 的
        retention_mode 分档** —— Go 侧 SweepJournal 就是直接 DELETE。两栈行为必须一致,
        不能在 Python 这边"顺手加一道 report_only",否则同一份配置下 Go 在删、Python 不删,
        库增长曲线在两个实现上完全不同而**都不报错**。
        删除资格由 JOIN bag_checkpoint 的覆盖水位谓词兜底,见模块头 _SWEEP_JOURNAL 注释。
        """
        if batch <= 0 or retention_seconds <= 0:
            return 0
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(_SWEEP_JOURNAL, (retention_seconds, batch))
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "sweep bag_journal: %s", exc
                ) from exc
            return cur.rowcount or 0

    # ── 容量购买(§5.3 两步 saga 第②步)───────────────────────────────────

    async def apply_capacity_purchase(
        self, player_id: int, bag_type: int, tier: int, slots: int, max_extra: int
    ) -> tuple[int, int, bool]:
        """容量落位(bag 库单事务):档数 CAS 幂等。

        返回 (extra, purchases, applied);applied=False 表示本次未新增(幂等回放)。
        """
        async with rsql.transaction(self._pool) as cur:
            # 锁 bag_meta 行:与 journal / 迁移落位共用同一每玩家串行化锚点(**不 CAS epoch**
            # —— 购买不是 owner 写者,越权防线在五要件② owner 授权)。
            try:
                await cur.execute(
                    "INSERT IGNORE INTO bag_meta (player_id, owner_epoch, last_journal_seq) "
                    "VALUES (%s, 0, 0)",
                    (player_id,),
                )
                await cur.execute(
                    "SELECT owner_epoch FROM bag_meta WHERE player_id = %s FOR UPDATE",
                    (player_id,),
                )
                meta_row = await cur.fetchone()
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "lock bag_meta player=%d: %s", player_id, exc
                ) from exc
            if meta_row is None:
                raise errcode.PandoraError(
                    errcode.ErrInternal, "lock bag_meta player=%d: row vanished", player_id
                )

            try:
                await cur.execute(
                    "INSERT IGNORE INTO bag_capacity (player_id, bag_type, extra, purchases) "
                    "VALUES (%s, %s, 0, 0)",
                    (player_id, bag_type),
                )
                await cur.execute(
                    "SELECT extra, purchases FROM bag_capacity "
                    "WHERE player_id = %s AND bag_type = %s FOR UPDATE",
                    (player_id, bag_type),
                )
                cap_row = await cur.fetchone()
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "lock bag_capacity player=%d bag=%d: %s",
                    player_id, bag_type, exc,
                ) from exc
            if cap_row is None:
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "lock bag_capacity player=%d bag=%d: row vanished",
                    player_id, bag_type,
                )
            extra, purchases = int(cap_row[0]), int(cap_row[1])

            if purchases >= tier:
                # 本档已应用(同档重试 / 双击并发的第二腿):幂等回放当前值。
                return extra, purchases, False
            if purchases != tier - 1:
                # tier 恒 = 服务端读到的 purchases+1,乱序只可能来自异常调用方,fail-closed。
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "capacity tier out of order player=%d bag=%d tier=%d purchases=%d",
                    player_id, bag_type, tier, purchases,
                )
            new_extra = extra + slots
            if new_extra > max_extra:
                # 已扣费但配置收缩到装不下(运营中途改配置):fail-closed 报警,
                # 凭 ledger 行排障;正常路径在扣费前已按同一配置预检,不会走到这里。
                raise errcode.PandoraError(
                    errcode.ErrBagCapacityMaxed,
                    "extra %d+%d exceeds max_extra %d player=%d bag=%d (config shrank after charge?)",
                    extra, slots, max_extra, player_id, bag_type,
                )
            try:
                await cur.execute(
                    "UPDATE bag_capacity SET extra = %s, purchases = %s "
                    "WHERE player_id = %s AND bag_type = %s",
                    (new_extra, tier, player_id, bag_type),
                )
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "apply capacity player=%d bag=%d: %s",
                    player_id, bag_type, exc,
                ) from exc
        return new_extra, tier, True

    async def get_capacity_state(self, player_id: int, bag_type: int) -> tuple[int, int]:
        """读某段已购状态(无行 = 0/0;购买用例定档 + LoadBag / GetSections 展示)。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(
                    "SELECT extra, purchases FROM bag_capacity "
                    "WHERE player_id = %s AND bag_type = %s",
                    (player_id, bag_type),
                )
                row = await cur.fetchone()
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "read bag_capacity player=%d bag=%d: %s",
                    player_id, bag_type, exc,
                ) from exc
        if row is None:
            return 0, 0
        return int(row[0]), int(row[1])
