"""装备实例背包数据层 —— 对应 Go 侧 internal/data/inventory_instance.go。

库表:player_item_instance(deploy/mysql-init/08-inventory-tables.sql)。
与 player_items(可堆叠消耗品计数)**并存**:消耗品走计数,装备类走实例
(每件唯一 + 鉴定后随机属性)。

一致性(不变量 §9.7):
  - GrantInstances 幂等复用 inventory_ledger(op=grant_inst,detail 记 instance_id CSV
    供回放);发放前锁玩家实例行校验容量,给每件分配最低空闲格
    (SELECT ... FOR UPDATE 防并发超发 / 占同格)。
  - IdentifyInstance 天然幂等:FOR UPDATE 后 identified=1 就不再 roll(回放已落定属性)。
  - MoveInstance 目标格唯一(uk_player_slot),被占 → ErrInventorySlotOccupied。

★ 词条列编码:`attributes` 是 VARBINARY,存 pb `ItemInstanceAttributesStorageRecord`
  二进制 —— MySQL 里的非基础类型一律 proto 二进制序列化,不用 JSON
  (§5.8/§9.17:字段编号语义 + unknown fields 保留)。
  用 JSON 的话,新增词条字段会在旧版本进程手里被**丢弃再写回**,数据静默变瘦。
"""

from __future__ import annotations

from pandora.inventory.v1 import inventory_pb2

from pandorapy import dbguard
from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import mysqlx
from pandorapy.services.inventory import currency as ccy
from pandorapy.services.inventory import fingerprint as fp
from pandorapy.services.inventory import repo_sql as rsql
from pandorapy.services.inventory.models import ItemAttribute, ItemInstance, SaleOutcome

# 词条 pb 写入侧字节上限,取列容量 VARBINARY(1024)。
# 鉴定词条数由池的 attr_count 约束(个位数),单条 pb 编码 ≤ 16 字节,
# 即使配成 32 条也不到 600 字节 —— 达到 1024 说明配置失控或有人直写。
INSTANCE_ATTRS_MAX_BYTES = 1024

INSTANCE_COLS = "instance_id, item_config_id, identified, attributes, slot_index, bound"


def encode_instance_attrs(attrs: list[ItemAttribute]) -> bytes | None:
    """词条 → pb 二进制(空词条 → None,列写 NULL)。

    落库前过写入侧字节闸(§9.24):超限 fail-closed 拒写,逼近上限 WARN。
    不设这道闸会怎样:非严格 sql_mode 下超长写入被**静默截断**,
    截断后的 pb 解不出来 —— 那件装备的词条永久损坏且无错可查。
    """
    if not attrs:
        return None
    rec = inventory_pb2.ItemInstanceAttributesStorageRecord(
        attributes=[
            inventory_pb2.ItemAttribute(attr_id=a.attr_id, value=a.value) for a in attrs
        ]
    )
    raw = rec.SerializeToString()
    try:
        dbguard.check_payload(
            "pandora_trade.player_item_instance.attributes", raw, INSTANCE_ATTRS_MAX_BYTES
        )
    except dbguard.PayloadTooLargeError as exc:
        raise errcode.PandoraError(
            errcode.ErrInternal, "instance attrs payload too large: %s", exc
        ) from exc
    return raw


def decode_instance_attrs(raw: bytes | None) -> list[ItemAttribute]:
    """pb 二进制 → 词条(NULL / 空 → 空列表)。"""
    if not raw:
        return []
    rec = inventory_pb2.ItemInstanceAttributesStorageRecord()
    rec.ParseFromString(bytes(raw))
    return [ItemAttribute(attr_id=a.attr_id, value=a.value) for a in rec.attributes]


def scan_instance(row) -> ItemInstance:  # noqa: ANN001
    """一行 SELECT 结果 → ItemInstance(列序必须与 INSTANCE_COLS 一致)。"""
    instance_id, item_config_id, identified, attrs_raw, slot, bound = row
    try:
        attrs = decode_instance_attrs(attrs_raw)
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal, "decode instance attrs id=%d: %s", int(instance_id), exc
        ) from exc
    return ItemInstance(
        instance_id=int(instance_id),
        item_config_id=int(item_config_id),
        identified=bool(identified),
        attributes=attrs,
        # slot NULL → -1(未分配格)。见 models.ItemInstance 的注释。
        slot_index=-1 if slot is None else int(slot),
        bound=bool(bound),
    )


async def lock_player_instances(cur, player_id: int) -> tuple[set[int], int]:  # noqa: ANN001
    """锁玩家全部实例行,返回 (已占用格集合, 实例总数)。

    ★ 必须锁全部行而不是只数一个 COUNT:容量校验和"分配最低空闲格"两件事
      都要在同一把锁下做,否则两笔并发发放会算出同一个空闲格 → 撞唯一键,
      或者各自算出"还有余量"→ 超发。
    """
    try:
        await cur.execute(
            "SELECT slot_index FROM player_item_instance WHERE player_id = %s FOR UPDATE",
            (player_id,),
        )
        rows = await cur.fetchall()
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal, "lock instances player=%d: %s", player_id, exc
        ) from exc
    occupied: set[int] = set()
    total = 0
    for (slot,) in rows or ():
        total += 1
        if slot is not None:
            occupied.add(int(slot))
    return occupied, total


def lowest_free_slot(occupied: set[int], capacity: int) -> int:
    """[0,capacity) 内最小的未占用格;满 → -1。"""
    for s in range(capacity):
        if s not in occupied:
            return s
    return -1


async def select_instance_for_update(cur, player_id: int, instance_id: int) -> ItemInstance:  # noqa: ANN001
    """锁读单个实例。非本人 / 不存在 → ErrInventoryItemNotFound。

    ★ 条件里带 player_id 是**鉴权的一部分**:只按 instance_id 锁会让任何人
      拿到别人的实例行(service 层的 caller 校验只保证"请求里的 player 是我",
      不保证"这个 instance 是我的")。
    """
    try:
        await cur.execute(
            f"SELECT {INSTANCE_COLS} FROM player_item_instance "
            "WHERE instance_id = %s AND player_id = %s LIMIT 1 FOR UPDATE",
            (instance_id, player_id),
        )
        row = await cur.fetchone()
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "read instance player=%d id=%d: %s",
            player_id,
            instance_id,
            exc,
        ) from exc
    if row is None:
        raise errcode.PandoraError(
            errcode.ErrInventoryItemNotFound,
            "instance not found player=%d id=%d",
            player_id,
            instance_id,
        )
    return scan_instance(row)


async def select_instances_by_ids_tx(cur, player_id: int, ids: list[int]) -> list[ItemInstance]:  # noqa: ANN001
    """按 id 列表读实例(幂等回放用;按 instance_id 升序)。"""
    if not ids:
        return []
    placeholders = ",".join(["%s"] * len(ids))
    try:
        await cur.execute(
            f"SELECT {INSTANCE_COLS} FROM player_item_instance "
            f"WHERE player_id = %s AND instance_id IN ({placeholders}) ORDER BY instance_id ASC",
            (player_id, *ids),
        )
        rows = await cur.fetchall()
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal, "select instances by ids player=%d: %s", player_id, exc
        ) from exc
    return [scan_instance(r) for r in rows or ()]


async def grant_instances_in_tx_mixed(  # noqa: ANN001
    cur,
    player_id: int,
    instance_ids: list[int],
    item_config_ids: list[int],
    capacity: int,
) -> list[ItemInstance]:
    """在**调用方已开好的事务**里锁玩家实例行、校验容量、逐件分配最低空闲格并插入。

    对应 Go 的 grantInstancesInTxMixed。抽出来是为了让"购买装备"能把扣钱与发货放进
    同一个事务(见 repo.purchase_shop_item):若购买复用外层的 grant_instances,
    扣钱和发货就会落在两个事务里,中间崩溃 = 钱扣了货没到。

    ★ 本函数**不提交也不回滚**,事务生命周期完全归调用方。
    """
    if len(instance_ids) != len(item_config_ids):
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "instanceIDs/itemConfigIDs length mismatch"
        )
    occupied, total = await lock_player_instances(cur, player_id)
    if capacity <= 0:
        raise errcode.PandoraError(
            errcode.ErrInventoryCapacityFull,
            "instance inventory disabled (capacity<=0) player=%d",
            player_id,
        )
    if total + len(instance_ids) > capacity:
        raise errcode.PandoraError(
            errcode.ErrInventoryCapacityFull,
            "capacity full player=%d have=%d grant=%d cap=%d",
            player_id,
            total,
            len(instance_ids),
            capacity,
        )
    out: list[ItemInstance] = []
    for inst_id, config_id in zip(instance_ids, item_config_ids, strict=True):
        slot = lowest_free_slot(occupied, capacity)
        if slot < 0:
            raise errcode.PandoraError(
                errcode.ErrInventoryCapacityFull,
                "no free slot player=%d cap=%d",
                player_id,
                capacity,
            )
        occupied.add(slot)
        try:
            await cur.execute(
                "INSERT INTO player_item_instance "
                "(instance_id, player_id, item_config_id, identified, attributes, "
                "slot_index, bound) VALUES (%s, %s, %s, 0, NULL, %s, 0)",
                (inst_id, player_id, config_id, slot),
            )
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "insert instance player=%d id=%d: %s",
                player_id,
                inst_id,
                exc,
            ) from exc
        out.append(ItemInstance(instance_id=inst_id, item_config_id=config_id, slot_index=slot))
    return out


class InstanceRepoMixin:
    """MySQLInventoryRepo 的装备实例部分。`self._pool` 由主类提供。"""

    # ── 只读 ──────────────────────────────────────────────────────────────

    async def list_instances(self, player_id: int) -> list[ItemInstance]:
        """读玩家全部装备实例(按 instance_id 升序;未建档 → 空)。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(
                    f"SELECT {INSTANCE_COLS} FROM player_item_instance "
                    "WHERE player_id = %s ORDER BY instance_id ASC",
                    (player_id,),
                )
                rows = await cur.fetchall()
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "list instances player=%d: %s", player_id, exc
                ) from exc
        return [scan_instance(r) for r in rows or ()]

    async def check_instances_owned(self, player_id: int, queries) -> list[ItemInstance]:  # noqa: ANN001
        """精确返回 instance_id + item_config_id **都**与玩家当前实例行一致的快照。

        返回详情而非仅 ID,使 player.GetLoadout 能把 identified/attributes 保真带到 DS,
        避免战斗链二次跨域查询。
        """
        queries = list(queries)
        if not queries:
            return []
        want = {q.instance_id: q.item_config_id for q in queries}
        placeholders = ",".join(["%s"] * len(queries))
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(
                    f"SELECT {INSTANCE_COLS} FROM player_item_instance "
                    f"WHERE player_id = %s AND instance_id IN ({placeholders}) "
                    "ORDER BY instance_id ASC",
                    (player_id, *want.keys()),
                )
                rows = await cur.fetchall()
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "check instance ownership player=%d: %s", player_id, exc
                ) from exc
        owned: list[ItemInstance] = []
        for r in rows or ():
            inst = scan_instance(r)
            # config 对不上 = 调用方拿的是漂移/伪造的组合,不算持有(静默剔除,与 Go 一致)。
            if want.get(inst.instance_id) == inst.item_config_id:
                owned.append(inst)
        return owned

    # ── 写 ────────────────────────────────────────────────────────────────

    async def grant_instances(
        self,
        player_id: int,
        instance_ids: list[int],
        item_config_ids: list[int],
        capacity: int,
        idempotency_key: str,
    ) -> tuple[list[ItemInstance], bool]:
        """幂等发放装备实例。返回 (实例列表, already)。

        detail 里编入本次 instance_id,供命中 uk 时按 id 回放 —— 这条流水**不是**
        普通的 claim_ledger:回放要还原"发了哪几件",而结果快照那两列存不下 id 列表。
        """
        if len(instance_ids) != len(item_config_ids):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "instanceIDs/itemConfigIDs length mismatch"
            )
        fingerprint = fp.grant_instances_fingerprint(item_config_ids)
        ledger_detail = fp.encode_instance_ids(instance_ids)

        # detail 列宽是**全服唯一一道**闸,按实际编码长度判(理由见 fingerprint.ledger_detail_fits)。
        #
        # 闸必须排在幂等回放**之后**:这里的"声明"与"判重"本是同一条 INSERT(撞唯一键即已处理),
        # 而超长时那条 INSERT 根本执行不了 —— MySQL 先报 Error 1406,压根轮不到唯一键。
        # 所以超长时必须显式探一次 (player_id, idempotency_key) 的旧流水:
        # 探到 = 这批货早就发过了,照常回放(下游 battle_result / mail / mission 都是永不放弃的
        # 重试者,拒一次就是永久卡住的行:货已发、行清不掉);探不到才是真·新的超长请求。
        fits = fp.ledger_detail_fits(ledger_detail)

        async with rsql.transaction(self._pool) as cur:
            claimed = False
            if fits:
                try:
                    await cur.execute(
                        "INSERT INTO inventory_ledger "
                        "(player_id, idempotency_key, op, request_fingerprint, detail) "
                        "VALUES (%s, %s, 'grant_inst', %s, %s)",
                        (player_id, idempotency_key, fingerprint, ledger_detail),
                    )
                except Exception as exc:  # noqa: BLE001
                    if not mysqlx.is_duplicate_entry(exc):
                        raise errcode.PandoraError(
                            errcode.ErrInternal,
                            "insert ledger player=%d key=%s: %s",
                            player_id,
                            idempotency_key,
                            exc,
                        ) from exc
                else:
                    claimed = True

            if not claimed:
                # 幂等命中(或超长时的显式探测):比对指纹,按已存 detail 里的 id 回放实例。
                await cur.execute(
                    "SELECT request_fingerprint, detail FROM inventory_ledger "
                    "WHERE player_id = %s AND idempotency_key = %s LIMIT 1",
                    (player_id, idempotency_key),
                )
                row = await cur.fetchone()
                if row is None and not fits:
                    # 没有旧流水 + 这批 id 编码后装不下 = 确实是一笔新的超长发放。
                    # 报 ErrInvalidArg(调用方拆批即可),不是"背包满" —— 报容量满会把运维引到扩容上。
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg,
                        "grant_inst detail exceeds ledger column "
                        "player=%d count=%d len=%d max=%d (split into multiple idempotency keys)",
                        player_id,
                        len(instance_ids),
                        len(ledger_detail),
                        fp.LEDGER_DETAIL_MAX_CHARS,
                    )
                if row is None:
                    # fits 且撞了唯一键却读不到行,只可能是并发事务未提交:fail-closed 让调用方重试。
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "read ledger player=%d key=%s: row vanished after duplicate",
                        player_id,
                        idempotency_key,
                    )
                stored_fp, stored_detail = str(row[0]), str(row[1] or "")
                if stored_fp != fingerprint:
                    plog.get().warning(
                        "inventory_idempotency_conflict",
                        player_id=player_id,
                        idempotency_key=idempotency_key,
                        op="instance",
                    )
                    raise errcode.PandoraError(
                        errcode.ErrInventoryIdempotencyConflict,
                        "idempotency_key reused for different request player=%d key=%s",
                        player_id,
                        idempotency_key,
                    )
                insts = await select_instances_by_ids_tx(
                    cur, player_id, fp.decode_instance_ids(stored_detail)
                )
                return insts, True

            # 首次:容量校验 + 分配空闲格 + 插入(与商店购买共用同一段逻辑)。
            out = await grant_instances_in_tx_mixed(
                cur, player_id, instance_ids, item_config_ids, capacity
            )
            return out, False

    async def identify_instance(
        self, player_id: int, instance_id: int, attrs: list[ItemAttribute]
    ) -> tuple[ItemInstance, bool]:
        """鉴定一件装备实例。返回 (最终实例, already)。

        幂等由**资源状态**承担:FOR UPDATE 后 identified=1 就直接回放已落定属性,
        不重新 roll —— 重 roll 会让同一件装备在重试时变出不同词条。
        """
        async with rsql.transaction(self._pool) as cur:
            inst = await select_instance_for_update(cur, player_id, instance_id)
            if inst.identified:
                return inst, True
            payload = encode_instance_attrs(attrs)
            try:
                await cur.execute(
                    "UPDATE player_item_instance SET identified = 1, attributes = %s "
                    "WHERE instance_id = %s AND player_id = %s",
                    (payload, instance_id, player_id),
                )
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "identify instance player=%d id=%d: %s",
                    player_id,
                    instance_id,
                    exc,
                ) from exc
            inst.identified = True
            inst.attributes = list(attrs)
            return inst, False

    async def move_instance(
        self, player_id: int, instance_id: int, to_slot: int, capacity: int
    ) -> ItemInstance:
        """移动实例到新格子。目标格越界 / 被别的实例占用 → ErrInventorySlotOccupied。"""
        if to_slot < 0 or to_slot >= capacity:
            raise errcode.PandoraError(
                errcode.ErrInventorySlotOccupied,
                "slot out of range player=%d slot=%d cap=%d",
                player_id,
                to_slot,
                capacity,
            )
        async with rsql.transaction(self._pool) as cur:
            inst = await select_instance_for_update(cur, player_id, instance_id)
            if inst.slot_index == to_slot:
                return inst
            try:
                await cur.execute(
                    "SELECT instance_id FROM player_item_instance "
                    "WHERE player_id = %s AND slot_index = %s LIMIT 1 FOR UPDATE",
                    (player_id, to_slot),
                )
                row = await cur.fetchone()
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "probe slot player=%d slot=%d: %s",
                    player_id,
                    to_slot,
                    exc,
                ) from exc
            occupant = int(row[0]) if row else 0
            if occupant not in (0, instance_id):
                raise errcode.PandoraError(
                    errcode.ErrInventorySlotOccupied,
                    "slot occupied player=%d slot=%d by=%d",
                    player_id,
                    to_slot,
                    occupant,
                )
            try:
                await cur.execute(
                    "UPDATE player_item_instance SET slot_index = %s "
                    "WHERE instance_id = %s AND player_id = %s",
                    (to_slot, instance_id, player_id),
                )
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "move instance player=%d id=%d: %s",
                    player_id,
                    instance_id,
                    exc,
                ) from exc
            inst.slot_index = to_slot
            return inst

    async def discard_instance(self, player_id: int, instance_id: int) -> None:
        """丢弃实例。幂等:不存在(已丢弃)→ OK no-op;bound 实例拒。"""
        async with rsql.transaction(self._pool) as cur:
            try:
                inst = await select_instance_for_update(cur, player_id, instance_id)
            except errcode.PandoraError as exc:
                if exc.code == errcode.ErrInventoryItemNotFound:
                    # 资源状态本身承担幂等:已丢弃重放 no-op。
                    return
                raise
            if inst.bound:
                raise errcode.PandoraError(
                    errcode.ErrInventoryInstanceBound,
                    "bound instance cannot be discarded player=%d id=%d",
                    player_id,
                    instance_id,
                )
            try:
                await cur.execute(
                    "DELETE FROM player_item_instance WHERE instance_id = %s AND player_id = %s",
                    (instance_id, player_id),
                )
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "discard instance player=%d id=%d: %s",
                    player_id,
                    instance_id,
                    exc,
                ) from exc

    async def sell_instance(
        self,
        player_id: int,
        instance_id: int,
        item_config_id: int,
        kind: int,
        amount: int,
        idempotency_key: str,
        detail: str,
    ) -> tuple[SaleOutcome, bool]:
        """原子出售唯一实例:锁实例、拒绝 bound、删除实例、货币入账与 ledger 同事务。

        返回 (SaleOutcome, already)。

        ★ amount == 0 的首次请求在 claim 之后才拒(ErrInventoryNotSellable):
          整笔事务回滚,连 claim 行一起没有 —— 不会留下脏流水把这个 key 永久占死。
          判据是 `== 0` 而不是 `<= 0`,理由见 repo.sell_item 同处注释。
        """
        async with rsql.transaction(self._pool) as cur:
            already, snap = await rsql.claim_sale_ledger(
                cur,
                player_id,
                idempotency_key,
                fp.sell_instance_fingerprint(instance_id, item_config_id),
                detail,
                op="sell_inst",
                item_config_id=item_config_id,
                instance_id=instance_id,
            )
            if already:
                return (
                    SaleOutcome(
                        remaining=0,
                        balances=snap.balances,
                        earned=ccy.balances_get(snap.delta, kind),
                        kind=kind,
                    ),
                    True,
                )
            if amount == 0:
                raise errcode.PandoraError(
                    errcode.ErrInventoryNotSellable,
                    "instance item not sellable player=%d instance=%d item=%d",
                    player_id,
                    instance_id,
                    item_config_id,
                )
            inst = await select_instance_for_update(cur, player_id, instance_id)
            if inst.bound:
                raise errcode.PandoraError(
                    errcode.ErrInventoryInstanceBound,
                    "bound instance cannot be sold player=%d id=%d",
                    player_id,
                    instance_id,
                )
            if inst.item_config_id != item_config_id:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "instance config mismatch player=%d id=%d got=%d want=%d",
                    player_id,
                    instance_id,
                    inst.item_config_id,
                    item_config_id,
                )
            try:
                await cur.execute(
                    "DELETE FROM player_item_instance WHERE instance_id = %s AND player_id = %s",
                    (instance_id, player_id),
                )
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "sell instance delete player=%d id=%d: %s",
                    player_id,
                    instance_id,
                    exc,
                ) from exc
            await ccy.add_currency_tx(cur, player_id, kind, amount)
            new_balances = await ccy.read_balances_tx(cur, player_id)
            await rsql.update_ledger_result(
                cur, player_id, idempotency_key, 0, new_balances, {kind: amount}
            )
            return (
                SaleOutcome(remaining=0, balances=new_balances, earned=amount, kind=kind),
                False,
            )
