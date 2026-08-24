"""幂等指纹族 —— 对应 Go 侧 internal/data/inventory_repo.go / inventory_instance.go /
inventory_transfer.go 里的 *Fingerprint 函数。

★ 指纹是什么、少了会怎样

  同一个 idempotency_key 被复用到**不同客户端意图**(op / item / count / instance 不同)
  时,只靠唯一键去重会把第二次请求静默当成 no-op —— 那是反作弊隐患:
  一个 key 先"卖 1 个 A",再拿同 key 请求"卖 1000 个 B",服务端返回成功却什么都没做,
  或者更糟,回放出与本次请求无关的结果快照。

  所以每条写路径把 key **绑定到请求内容**:
    首次执行  记录指纹 + 结果快照
    重复请求  指纹不一致 → ErrInventoryIdempotencyConflict;一致 → 回放首次结果快照

★ 字符串格式是**跨实现契约**,一个字符都不能改

  指纹落在 inventory_ledger.request_fingerprint 列里。Go 版写过的行,Python 版必须
  算出同一个 hex,否则迁移期两栈并存时:同一个 key 的重试会被判成"内容不同" →
  ErrInventoryIdempotencyConflict,表现为玩家的正常重试全部失败。
  反过来若某条指纹算得比 Go 松(丢了一个维度),就会把两笔不同的请求判成同一笔 →
  **重复入账**。两个方向都不会报错,只会表现成业务上的怪事。

★ 售价**刻意不进**新的 sell 指纹

  sell / sell_inst 的指纹只绑定"客户端选了什么"(item + count / instance + item),
  不含 gold。售价是服务端热配置:若把它编进指纹,首次响应丢失后恰逢配置表热更改价,
  同 key 的重试就会被误判为冲突,严重时诱发二次出售。
  升级前已提交的旧行(指纹里含 gold)由 repo 的 legacy 识别路径单独处理,见 repo.py。

★ 多币种改造(2026-08-22)刻意保留**旧格式兼容**,不是偷懒

  grant / auction_settle / trade_settle / bagcap 四条路径在**只涉及金币**时
  继续用改造前的字符串(`|gold=<n>` / `|price=<n>`),只有真的出现非金币币种
  才启用新格式(`|cur=...`)。
  理由:多币种上线前写下的存量流水行,其指纹就是按旧格式算的。若无条件换成新格式,
  存量行遇到同 key 重试会被判成 ErrInventoryIdempotencyConflict —— 那是
  "同键不同请求"的反作弊信号,用它来报"我升级了协议"会把真正的串账淹没在噪声里。
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Sequence

from pandorapy.services.inventory import currency as cur


def hash_hex(s: str) -> str:
    """sha256 → 小写 hex。与 Go 的 hashHex 同口径(对 UTF-8 字节求哈希)。"""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def is_gold_only(currencies) -> bool:  # noqa: ANN001
    """这笔货币是否"只涉及金币"(含完全没有货币的情形)。对应 Go 的 isGoldOnly。

    ★ 这是**旧指纹格式的适用条件**,不是业务判断,别拿它当"是不是金币交易"用。
      零额项忽略:{GOLD:0, DIAMOND:0} 与 {} 对指纹是同一件事。
    """
    for kind, amount in (currencies or {}).items():
        if int(amount) == 0:
            continue
        if int(kind) != cur.CURRENCY_GOLD:
            return False
    return True


def grant_fingerprint(items: Iterable[tuple[int, int]], currencies) -> str:  # noqa: ANN001
    """发放指纹。items 是 (item_config_id, count) 序列,currencies 是 {kind: amount}。

    ★ 必须先按 item_config_id 升序规范化:同一批发放在不同调用点可能顺序不同,
      不排序会让"同一笔发放"算出两个指纹 → 重试被判冲突。
    ★ 纯金币发放沿用旧格式 `|gold=<n>`(见模块头注释的"旧格式兼容")。
    """
    parts = ["grant"]
    for item_config_id, count in sorted(items, key=lambda it: it[0]):
        parts.append(f"|{item_config_id}:{count}")
    if is_gold_only(currencies):
        parts.append(f"|gold={cur.balances_get(currencies, cur.CURRENCY_GOLD)}")
    else:
        parts.append(f"|cur={cur.describe_balances(currencies)}")
    return hash_hex("".join(parts))


def use_fingerprint(item_config_id: int, count: int) -> str:
    return hash_hex(f"use|{item_config_id}:{count}")


def discard_fingerprint(item_config_id: int, count: int) -> str:
    return hash_hex(f"discard|{item_config_id}:{count}")


def battle_consume_fingerprint(item_config_id: int, count: int) -> str:
    return hash_hex(f"battle_consume|{item_config_id}:{count}")


def battle_discard_fingerprint(item_config_id: int, count: int) -> str:
    return hash_hex(f"battle_discard|{item_config_id}:{count}")


def sell_fingerprint(item_config_id: int, count: int) -> str:
    """只绑定客户端出售意图 —— 售价不进指纹,理由见模块头注释。"""
    return hash_hex(f"sell|{item_config_id}:{count}")


def sell_instance_fingerprint(instance_id: int, item_config_id: int) -> str:
    """只绑定客户端选中的唯一实例和配置一致性字段。"""
    return hash_hex(f"sell_inst|{instance_id}|item={item_config_id}")


def legacy_sell_fingerprint(item_config_id: int, count: int, gold: int) -> str:
    """升级前已提交的 sell 行的指纹口径(含首次成交价)。

    只用于**安全识别**旧行:不能拿当前热更价格来算,必须从旧行 detail 里恢复首次价格。
    """
    return hash_hex(f"sell|{item_config_id}:{count}|gold={gold}")


def legacy_sell_instance_fingerprint(instance_id: int, item_config_id: int, gold: int) -> str:
    return hash_hex(f"sell_inst|{instance_id}|item={item_config_id}|gold={gold}")


def auction_settle_fingerprint(
    seller_id: int,
    buyer_id: int,
    item_config_id: int,
    quantity: int,
    kind: int,
    total_amount: int,
) -> str:
    """拍卖结算指纹(双方 + 道具 + 数量 + 总价 + 币种)。

    同一 idempotency_key 复用到不同成交内容 → 指纹不一致判冲突,防 key 复用串改账。
    ★ 金币成交沿用旧字符串格式,理由同 grant_fingerprint:不能让协议升级冒充串账信号。
    """
    if int(kind) == cur.CURRENCY_GOLD:
        return hash_hex(
            f"auction_settle|seller={seller_id}|buyer={buyer_id}|item={item_config_id}"
            f"|qty={quantity}|gold={total_amount}"
        )
    return hash_hex(
        f"auction_settle|seller={seller_id}|buyer={buyer_id}|item={item_config_id}"
        f"|qty={quantity}|cur={int(kind)}:{total_amount}"
    )


def player_trade_settle_fingerprint(
    seller_id: int,
    buyer_id: int,
    seller_items: Iterable[tuple[int, int]],
    buyer_items: Iterable[tuple[int, int]],
    kind: int,
    price: int,
) -> str:
    """玩家间交易结算指纹(双方 + 双向道具 + 货币)。

    ★ `price == 0` 也走旧格式:纯物物交换在多币种之前写下的行就是 `|price=0`,
      此时币种没有任何语义(一分钱没动),把 kind 编进去会让存量行重试判冲突。
    """

    def _write(tag: str, items: Iterable[tuple[int, int]]) -> str:
        out = [tag]
        for item_config_id, count in sorted(items, key=lambda it: it[0]):
            out.append(f"|{item_config_id}:{count}")
        return "".join(out)

    body = (
        f"trade_settle|seller={seller_id}|buyer={buyer_id}|"
        + _write("sell", seller_items)
        + _write("|buy", buyer_items)
        + f"|price={price}"
    )
    if int(kind) != cur.CURRENCY_GOLD and price != 0:
        body += f"|cur={int(kind)}"
    return hash_hex(body)


def purchase_fingerprint(shop_id: int, item_config_id: int, unit_count: int) -> str:
    """NPC 商店购买指纹。对应 Go 的 PurchaseFingerprint。

    ★ **刻意不含价格**:价格是服务端热更配置。若把价格编进指纹,首次响应丢失后
      策划改了价,客户端拿同一个 key 重试就会被判 ErrInventoryIdempotencyConflict ——
      那是"同键不同请求"的反作弊信号,用它来报"价格变了"会让真正的串账淹没在噪声里。
      玩家意图 = (哪个商店, 买哪个, 买几份),这三项一致就是同一次购买;
      首次执行的实际扣费从 ledger 的 delta 列回放,所以改价也不会让重放金额漂移。
    """
    return hash_hex(f"shop_buy|shop={shop_id}|item={item_config_id}|units={unit_count}")


def bag_capacity_charge_fingerprint(
    player_id: int, bag_type: int, tier: int, slots: int, price: int, kind: int
) -> str:
    """背包容量购买扣费指纹。对应 Go ChargeBagCapacity 里内联的那两行。

    ★ 金币沿用旧格式:格容购买在多币种之前只可能是金币,换格式会让存量流水
      在同 key 重试时被误判成 ErrInventoryIdempotencyConflict。
    """
    body = f"bagcap|{player_id}|{bag_type}|{tier}|{slots}|{price}"
    if int(kind) != cur.CURRENCY_GOLD:
        body += f"|cur={int(kind)}"
    return hash_hex(body)


def grant_instances_fingerprint(item_config_ids: Sequence[int]) -> str:
    """发放实例指纹(item_config_ids 排序规范化)。"""
    parts = ["grant_inst"]
    for cid in sorted(item_config_ids):
        parts.append(f"|{cid}")
    return hash_hex("".join(parts))


def escrow_out_fingerprint(to_player_id: int, instance_ids: Sequence[int]) -> str:
    """托管扣出指纹(to_player + instance_ids 排序规范化)。"""
    parts = [f"escrow_out|to={to_player_id}"]
    for iid in sorted(instance_ids):
        parts.append(f"|{iid}")
    return hash_hex("".join(parts))


def transfer_claim_fingerprint(items: Iterable[tuple[int, int]]) -> str:
    """托管领取指纹。items 是 (instance_id, item_config_id) 序列,按 instance_id 排序。"""
    parts = ["transfer_claim"]
    for instance_id, item_config_id in sorted(items, key=lambda it: it[0]):
        parts.append(f"|{instance_id}:{item_config_id}")
    return hash_hex("".join(parts))


# ── ledger.detail 编解码(grant_inst 的幂等回放依赖它)────────────────────

# LEDGER_DETAIL_MAX_CHARS 是 inventory_ledger.detail 的列容量。
# 口径来自 deploy/mysql-init/08-inventory-tables.sql 与
# tools/migrate/migrations/pandora_trade/000001_baseline.up.sql 的 `VARCHAR(255)`。
# detail 全是 ASCII,所以字符数 == 字节数 —— Go 侧 `len(detail)` 数的是字节,
# 这里 `len(str)` 数的是字符,两边因此得到同一个数(对应 Go 的 ledgerDetailMaxChars)。
LEDGER_DETAIL_MAX_CHARS = 255


def ledger_detail_fits(detail: str) -> bool:
    """报告一条编码好的 detail 是否装得进列。对应 Go 的 ledgerDetailFits。

    ★ 为什么必须有这道闸

      detail 不是"人读审计摘要"那么轻:grant_inst / shop_buy 用它承载 instance_id 列表,
      它是幂等回放**唯一**的事实源("当初到底发了什么")。超长写入的两种下场都很糟:
        严格 sql_mode  → Error 1406,被包成 ErrInternal 抛给玩家,错误码里什么也看不出;
        非严格 sql_mode → **静默截断**,回放时按截断后的 id 列表算,等于算错了发货事实。
      后者更致命,而且不报任何错。

    ★ 为什么按**实际编码长度**判,而不是按 uint64 最坏 20 位反推件数

      与 Go 侧 2026-08-24 的改判逐条对齐:最坏位数闸(grant 11 件 / 购买 9 份)已被判为
      P0 回退删除 —— 现网雪花只有 17 位,实际装得下 grant 13 件 / 购买 11 份,那道闸把
      今天 100% 能成的 12、13 件直接改判为拒;更要命的是它挡在幂等回放**之前**,
      已提交成功的旧批次再也回放不了(下游掉落出箱 / 邮件领取 / 任务补扫全是永不放弃的
      重试者,拒一次就是永久卡住的行:货已发、行清不掉)。
      实际长度闸天然对回放安全:同一批 id 原来写得进去,重算长度还是同一个长度。
      唯一残留风险是"雪花跨位数增长后按新 id 重算变长",由 repo 层"超长时先探旧流水"兜住
      (见 repo_instance.grant_instances / repo._claim_purchase_ledger)。

    ★ 为什么不是"加宽列"

      加宽要动迁移(版本钉子是三处 lockstep 的高危项),而且只是把同一个洞往后推 ——
      一次发多少件本来就该有业务上限。加宽属另开一轮的 expand 迁移。
    """
    return len(detail) <= LEDGER_DETAIL_MAX_CHARS


def grant_instances_detail_fits(instance_ids: Sequence[int]) -> bool:
    """这批 instance_id 编码后是否装得进 ledger.detail 列。对应 Go 的 GrantInstancesDetailFits。

    公开出来是给测试复刻同一道闸用的:测试若自己另算一套长度公式,就会与生产漂移
    (算两套必漂移 —— 与 Go 侧 inventory_test.go 的纪律一致)。
    """
    return ledger_detail_fits(encode_instance_ids(instance_ids))


def purchase_detail_fits(
    shop_id: int, item_config_id: int, unit_count: int, total_items: int, instance_ids: Sequence[int]
) -> bool:
    """本次购买编码后的 detail 是否装得进 ledger.detail 列。对应 Go 的 PurchaseDetailFits。

    参数与 purchase_detail 逐个对齐:判定必须落在**将要真正写库的那条字符串**上,
    少传一个字段就会比生产松(比如漏掉 count= 那段,边界处会多放一份进去)。
    """
    return ledger_detail_fits(
        purchase_detail(shop_id, item_config_id, unit_count, total_items, instance_ids)
    )


def encode_instance_ids(ids: Sequence[int]) -> str:
    """把发放的 instance_id 编进 ledger.detail(格式 "grant_inst ids=123,456")。

    幂等回放时按这些 id 重新 SELECT 实例 —— 所以格式必须与 Go 逐字一致,
    否则 Go 写的行在 Python 侧解不出 id,回放会返回空列表(调用方以为一件都没发)。
    """
    return "grant_inst ids=" + ",".join(str(i) for i in ids)


def decode_instance_ids(detail: str) -> list[int]:
    marker = "ids="
    i = detail.find(marker)
    if i < 0:
        return []
    csv = detail[i + len(marker) :]
    if not csv:
        return []
    out: list[int] = []
    for part in csv.split(","):
        try:
            value = int(part.strip())
        except ValueError:
            continue
        if value != 0:
            out.append(value)
    return out


def purchase_detail(
    shop_id: int, item_config_id: int, unit_count: int, total_items: int, instance_ids: Sequence[int]
) -> str:
    """编码一次购买的可回放事实。对应 Go 的 purchaseDetail。

    detail 在本仓一般是"人读审计摘要",但 grant_inst 早已用它承载 instance_id 列表。
    购买同样需要回放"到底发了什么",且这些事实**无法从请求重算**(每份数量是热更配置,
    改表后重算会得到与首次执行不同的数量)。因此沿用同一惯例,并保持格式可读:

        shop_buy shop=1 item=10001 units=3 count=30 inst=
        shop_buy shop=1 item=10156 units=1 count=0 inst=123,124
    """
    ids = ",".join(str(i) for i in instance_ids)
    return (
        f"shop_buy shop={shop_id} item={item_config_id} units={unit_count} "
        f"count={total_items} inst={ids}"
    )


def parse_purchase_detail(detail: str) -> tuple[int, list[int]] | None:
    """从流水 detail 还原首次执行发放的事实。对应 Go 的 parsePurchaseDetail。

    解析失败返回 None;调用方据此 fail-closed —— 宁可报内部错,
    **也不能凭空编造发货结果**(那会让 UI 显示玩家其实没拿到的东西)。
    """
    prefix = "shop_buy shop="
    if not detail.startswith(prefix):
        return None
    idx = detail.find(" inst=")
    if idx < 0:
        return None
    head, inst_part = detail[:idx], detail[idx + len(" inst=") :]
    parsed = _parse_head(head)
    if parsed is None:
        return None
    _shop_id, _item_id, _units, count = parsed
    ids: list[int] = []
    if inst_part:
        for token in inst_part.split(","):
            try:
                value = int(token)
            except ValueError:
                return None
            if value == 0:
                return None
            ids.append(value)
    return count, ids


def _parse_head(head: str) -> tuple[int, int, int, int] | None:
    """解析 "shop_buy shop=%d item=%d units=%d count=%d"(等价于 Go 的 Sscanf 四个 %d)。"""
    fields = ("shop_buy shop=", " item=", " units=", " count=")
    rest = head
    out: list[int] = []
    for i, sep in enumerate(fields):
        if not rest.startswith(sep):
            return None
        rest = rest[len(sep) :]
        nxt = fields[i + 1] if i + 1 < len(fields) else None
        if nxt is None:
            token, rest = rest, ""
        else:
            j = rest.find(nxt)
            if j < 0:
                return None
            token, rest = rest[:j], rest[j:]
        try:
            out.append(int(token))
        except ValueError:
            return None
    if rest:
        return None
    return out[0], out[1], out[2], out[3]
