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
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Sequence


def hash_hex(s: str) -> str:
    """sha256 → 小写 hex。与 Go 的 hashHex 同口径(对 UTF-8 字节求哈希)。"""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def grant_fingerprint(items: Iterable[tuple[int, int]], gold: int) -> str:
    """发放指纹。items 是 (item_config_id, count) 序列。

    ★ 必须先按 item_config_id 升序规范化:同一批发放在不同调用点可能顺序不同,
      不排序会让"同一笔发放"算出两个指纹 → 重试被判冲突。
    """
    parts = ["grant"]
    for item_config_id, count in sorted(items, key=lambda it: it[0]):
        parts.append(f"|{item_config_id}:{count}")
    parts.append(f"|gold={gold}")
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
    seller_id: int, buyer_id: int, item_config_id: int, quantity: int, total_gold: int
) -> str:
    """拍卖结算指纹(双方 + 道具 + 数量 + 总价)。

    同一 idempotency_key 复用到不同成交内容 → 指纹不一致判冲突,防 key 复用串改账。
    """
    return hash_hex(
        f"auction_settle|seller={seller_id}|buyer={buyer_id}|item={item_config_id}"
        f"|qty={quantity}|gold={total_gold}"
    )


def player_trade_settle_fingerprint(
    seller_id: int,
    buyer_id: int,
    seller_items: Iterable[tuple[int, int]],
    buyer_items: Iterable[tuple[int, int]],
    price: int,
) -> str:
    """玩家间交易结算指纹(双方 + 双向道具 + 金币)。"""

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
