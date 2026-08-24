"""邮件 transfer 附件实例托管数据层 —— 对应 Go 侧 internal/data/inventory_transfer.go。

库表:mail_transfer_escrow(pandora_trade)。

★ 不变量:"同一 instance 全局唯一"
  player_item_instance 与 mail_transfer_escrow 各自以 instance_id 为 PK,
  行只能经**同一个 MySQL 事务**在两表间 `INSERT ... SELECT` + `DELETE` 搬移,
  任一时刻实例恰存在于一处。

★ 为什么必须用 `INSERT ... SELECT` 而不是"读出来再写回去"
  搬移要求鉴定态 / 词条 / 绑定标记**逐字节原样**(零重铸零重 roll)。
  经 Go/Python 解码再编码的话,任何一次 pb 版本差异都会把 unknown fields 丢掉 ——
  玩家的装备在过一次邮件之后悄悄少了几条词条,而全程没有任何错误。

幂等(§9.7):
  - EscrowOutInstances / ClaimTransferInstances 复用 inventory_ledger(指纹防 key 复用);
  - ReleaseTransferEscrow / ConsumeTransferEscrow 的幂等由**托管行存在性**承担:
    行已被领取/已释放 → no-op(行只能被删一次,且每条删除路径在同事务里恰入账
    一个玩家,资产只落一处)。
"""

from __future__ import annotations

from pandorapy import errcode
from pandorapy.services.inventory import fingerprint as fp
from pandorapy.services.inventory import repo_sql as rsql
from pandorapy.services.inventory.models import EscrowedInstance
from pandorapy.services.inventory.repo_instance import (
    decode_instance_attrs,
    lock_player_instances,
    lowest_free_slot,
    select_instance_for_update,
)

ESCROW_COLS = "instance_id, item_config_id, identified, attributes, source_player_id, to_player_id"


def _sorted_ids(ids) -> list[int]:  # noqa: ANN001
    """升序副本 —— **统一加锁顺序**,防两笔并发操作交叉锁行成环(1213 死锁)。

    这不是"整理一下好看":两笔操作若各按调用方给的顺序锁行,
    {A,B} 与 {B,A} 就会互相等待。排序把全局锁序钉死。
    """
    return sorted(ids)


def _scan_escrow_row(row) -> EscrowedInstance:  # noqa: ANN001
    instance_id, item_config_id, identified, attrs_raw, source_player_id, to_player_id = row
    try:
        attrs = decode_instance_attrs(attrs_raw)
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal, "decode escrow attrs id=%d: %s", int(instance_id), exc
        ) from exc
    return EscrowedInstance(
        instance_id=int(instance_id),
        item_config_id=int(item_config_id),
        identified=bool(identified),
        attributes=attrs,
        source_player_id=int(source_player_id),
        to_player_id=int(to_player_id),
    )


async def _select_escrow_rows_tx(cur, ids: list[int], for_update: bool) -> list[EscrowedInstance]:  # noqa: ANN001
    """按 id 列表读托管行(缺行不报错,由调用方按语义处理)。"""
    if not ids:
        return []
    placeholders = ",".join(["%s"] * len(ids))
    q = (
        f"SELECT {ESCROW_COLS} FROM mail_transfer_escrow "
        f"WHERE instance_id IN ({placeholders}) ORDER BY instance_id ASC"
    )
    if for_update:
        q += " FOR UPDATE"
    try:
        await cur.execute(q, tuple(ids))
        rows = await cur.fetchall()
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(errcode.ErrInternal, "select escrow rows: %s", exc) from exc
    return [_scan_escrow_row(r) for r in rows or ()]


class TransferRepoMixin:
    """MySQLInventoryRepo 的托管转移部分。`self._pool` 由主类提供。"""

    async def escrow_out_instances(
        self,
        source_player_id: int,
        to_player_id: int,
        instance_ids: list[int],
        escrow_key: str,
        detail: str,
    ) -> tuple[list[EscrowedInstance], bool]:
        """从源玩家原子扣出实例并托管(bound 实例拒)。返回 (托管快照, already)。

        ★ 幂等回放时**缺行就报错**,不当成功:
          托管行只能被"已发出邮件的领取"或"调用方补偿释放"消灭,两者都以调用方
          已观察到本调用成功为前提;重试只发生在响应丢失、邮件尚未发出的窗口。
          此时缺行 = 调用方违反了 saga 顺序 —— 当成功会让邮件带着一批不存在的
          附件发出去,玩家永远领不到。
        """
        fingerprint = fp.escrow_out_fingerprint(to_player_id, instance_ids)
        async with rsql.transaction(self._pool) as cur:
            already, _snap = await rsql.claim_ledger(
                cur, source_player_id, escrow_key, "escrow_out", fingerprint, detail
            )
            if already:
                rows = await _select_escrow_rows_tx(cur, _sorted_ids(instance_ids), False)
                if len(rows) != len(instance_ids):
                    raise errcode.PandoraError(
                        errcode.ErrInventoryItemNotFound,
                        "escrow_out replay: %d/%d rows missing key=%s (saga order violated?)",
                        len(instance_ids) - len(rows),
                        len(instance_ids),
                        escrow_key,
                    )
                return rows, True

            out: list[EscrowedInstance] = []
            for instance_id in _sorted_ids(instance_ids):
                inst = await select_instance_for_update(cur, source_player_id, instance_id)
                if inst.bound:
                    raise errcode.PandoraError(
                        errcode.ErrInventoryInstanceBound,
                        "bound instance not transferable player=%d id=%d",
                        source_player_id,
                        instance_id,
                    )
                try:
                    await cur.execute(
                        "INSERT INTO mail_transfer_escrow "
                        "(instance_id, item_config_id, identified, attributes, bound, "
                        "source_player_id, to_player_id, escrow_key) "
                        "SELECT instance_id, item_config_id, identified, attributes, bound, "
                        "%s, %s, %s FROM player_item_instance "
                        "WHERE instance_id = %s AND player_id = %s",
                        (source_player_id, to_player_id, escrow_key, instance_id, source_player_id),
                    )
                except Exception as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrInternal, "escrow out insert id=%d: %s", instance_id, exc
                    ) from exc
                try:
                    await cur.execute(
                        "DELETE FROM player_item_instance "
                        "WHERE instance_id = %s AND player_id = %s",
                        (instance_id, source_player_id),
                    )
                except Exception as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrInternal, "escrow out delete id=%d: %s", instance_id, exc
                    ) from exc
                out.append(
                    EscrowedInstance(
                        instance_id=inst.instance_id,
                        item_config_id=inst.item_config_id,
                        identified=inst.identified,
                        attributes=inst.attributes,
                        source_player_id=source_player_id,
                        to_player_id=to_player_id,
                    )
                )
            return out, False

    async def claim_transfer_instances(
        self,
        to_player_id: int,
        items,  # noqa: ANN001 —— list[TransferClaimItem]
        capacity: int,
        idempotency_key: str,
        detail: str,
    ) -> bool:
        """把托管行原样搬进领取人实例表(mail ClaimMail 专用)。返回 already。

        领取只认托管行(escrow 权威):行缺失 / to_player 不符 / config 漂移
        → ErrInventoryItemNotFound **整批拒**(邮件保持未领取,可重试);
        容量满 → ErrInventoryCapacityFull(玩家清背包后重领)。
        """
        items = list(items)
        fingerprint = fp.transfer_claim_fingerprint(
            [(it.instance_id, it.item_config_id) for it in items]
        )
        async with rsql.transaction(self._pool) as cur:
            already, _snap = await rsql.claim_ledger(
                cur, to_player_id, idempotency_key, "transfer_claim", fingerprint, detail
            )
            if already:
                # 首次提交已完成搬移;回放直接成功(mail 侧 crash-after-claim 重试恰好一次)。
                return True

            want_by_id = {it.instance_id: it.item_config_id for it in items}
            rows = await _select_escrow_rows_tx(cur, _sorted_ids(want_by_id.keys()), True)
            if len(rows) != len(items):
                raise errcode.PandoraError(
                    errcode.ErrInventoryItemNotFound,
                    "transfer claim: %d/%d escrow rows missing player=%d",
                    len(items) - len(rows),
                    len(items),
                    to_player_id,
                )
            for row in rows:
                if row.to_player_id != to_player_id:
                    raise errcode.PandoraError(
                        errcode.ErrInventoryItemNotFound,
                        "transfer claim: escrow id=%d not destined to player=%d",
                        row.instance_id,
                        to_player_id,
                    )
                if want_by_id.get(row.instance_id) != row.item_config_id:
                    raise errcode.PandoraError(
                        errcode.ErrInventoryItemNotFound,
                        "transfer claim: config drift id=%d want=%d escrow=%d",
                        row.instance_id,
                        want_by_id.get(row.instance_id, 0),
                        row.item_config_id,
                    )

            occupied, total = await lock_player_instances(cur, to_player_id)
            if capacity <= 0:
                raise errcode.PandoraError(
                    errcode.ErrInventoryCapacityFull,
                    "instance inventory disabled (capacity<=0) player=%d",
                    to_player_id,
                )
            if total + len(rows) > capacity:
                raise errcode.PandoraError(
                    errcode.ErrInventoryCapacityFull,
                    "capacity full player=%d have=%d claim=%d cap=%d",
                    to_player_id,
                    total,
                    len(rows),
                    capacity,
                )
            for row in rows:
                slot = lowest_free_slot(occupied, capacity)
                if slot < 0:
                    raise errcode.PandoraError(
                        errcode.ErrInventoryCapacityFull,
                        "no free slot player=%d cap=%d",
                        to_player_id,
                        capacity,
                    )
                occupied.add(slot)
                try:
                    await cur.execute(
                        "INSERT INTO player_item_instance "
                        "(instance_id, player_id, item_config_id, identified, attributes, "
                        "slot_index, bound) "
                        "SELECT instance_id, %s, item_config_id, identified, attributes, %s, bound "
                        "FROM mail_transfer_escrow WHERE instance_id = %s",
                        (to_player_id, slot, row.instance_id),
                    )
                except Exception as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "transfer claim insert id=%d: %s",
                        row.instance_id,
                        exc,
                    ) from exc
                try:
                    await cur.execute(
                        "DELETE FROM mail_transfer_escrow WHERE instance_id = %s",
                        (row.instance_id,),
                    )
                except Exception as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "transfer claim delete escrow id=%d: %s",
                        row.instance_id,
                        exc,
                    ) from exc
            return False

    async def release_transfer_escrow(self, instance_ids: list[int]) -> int:
        """托管释放回各行的 source 玩家(发信 saga 失败补偿)。返回实际释放行数。

        ★ 释放**不设容量闸**(资产归还优先):一律以未分配格(slot NULL,uk 允许多 NULL)
          入包,玩家整理后自然回落。设了容量闸的话,背包满的玩家会让补偿永久失败,
          资产卡在托管表里谁也拿不到。
        """
        async with rsql.transaction(self._pool) as cur:
            rows = await _select_escrow_rows_tx(cur, _sorted_ids(instance_ids), True)
            for row in rows:
                try:
                    await cur.execute(
                        "INSERT INTO player_item_instance "
                        "(instance_id, player_id, item_config_id, identified, attributes, "
                        "slot_index, bound) "
                        "SELECT instance_id, source_player_id, item_config_id, identified, "
                        "attributes, NULL, bound FROM mail_transfer_escrow WHERE instance_id = %s",
                        (row.instance_id,),
                    )
                except Exception as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "release escrow insert id=%d: %s",
                        row.instance_id,
                        exc,
                    ) from exc
                try:
                    await cur.execute(
                        "DELETE FROM mail_transfer_escrow WHERE instance_id = %s",
                        (row.instance_id,),
                    )
                except Exception as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "release escrow delete id=%d: %s",
                        row.instance_id,
                        exc,
                    ) from exc
            return len(rows)

    async def consume_transfer_escrow(self, to_player_id: int, instance_ids: list[int]) -> int:
        """消托管行**不物化**(bag phase 2 DS 领取链)。返回实际消费行数。

        资产已经由 bag journal 原样入包,托管行只删 —— 防"bag 域已入 + 托管行残留"双持。
        存在的行必须 destined to 该玩家(不符 = 调用方错乱,整批拒);
        行缺失 = 已消费,no-op 幂等。
        """
        async with rsql.transaction(self._pool) as cur:
            rows = await _select_escrow_rows_tx(cur, _sorted_ids(instance_ids), True)
            for row in rows:
                if row.to_player_id != to_player_id:
                    raise errcode.PandoraError(
                        errcode.ErrInventoryItemNotFound,
                        "consume escrow: id=%d not destined to player=%d",
                        row.instance_id,
                        to_player_id,
                    )
                try:
                    await cur.execute(
                        "DELETE FROM mail_transfer_escrow WHERE instance_id = %s",
                        (row.instance_id,),
                    )
                except Exception as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "consume escrow delete id=%d: %s",
                        row.instance_id,
                        exc,
                    ) from exc
            return len(rows)
