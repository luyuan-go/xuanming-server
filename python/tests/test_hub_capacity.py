"""Hub 容量账本测试。

三条不变量,每条都是"违反了不报错、只在容量上悄悄错":
  1. ★ player_count 只由账本派生 —— 心跳上报绝不覆盖
  2. ★ connected ownership 没有时间 TTL —— 不能靠"过期"假装玩家离开
  3. ★ successor 按 assignment_id 去重 —— 一个玩家不能占两个座位
"""

from __future__ import annotations

import pathlib
import re

import pytest

from pandorapy import auth, errcode
from pandorapy.services.hub_allocator import capacity as cap

# ★ WRITER 不是"随便挑一个数"。record_matches_instance 里的门是**恰等于**
# DS_AUTH_WRITER_EPOCH_V2,写任何别的值这些用例都该整批不匹配。
# 此前这里写的是 7 —— 实现漏了那道门,于是测试与实现一起错得自洽。
POD, UID, EPOCH, WRITER = "hub-1", "uid-a", 3, auth.DS_AUTH_WRITER_EPOCH_V2
NOW = 1_760_000_000_000


def _rec(assignment_id: str, *, expires_at_ms: int = 0, **kw) -> cap.LedgerRecord:
    base = dict(
        hub_pod_name=POD, hub_instance_uid=UID, auth_epoch=EPOCH, auth_writer_epoch=WRITER
    )
    base.update(kw)
    return cap.LedgerRecord(assignment_id=assignment_id, expires_at_ms=expires_at_ms, **base)


def _ledger(reservations=(), sessions=(), successors=()) -> cap.CapacityLedger:
    return cap.CapacityLedger(
        reservations={r.assignment_id: r for r in reservations},
        sessions={s.assignment_id: s for s in sessions},
        successors={f"cap-{i}": s for i, s in enumerate(successors)},
    )


# ── ★ ① 心跳不覆盖容量 ────────────────────────────────────────────────────


def test_heartbeat_never_changes_capacity() -> None:
    """★ 心跳上报只写审计,**绝不覆盖** player_count。

    心跳是 DS 报的,而 DS 可能漏报、可能在网络分区里报旧数据。
    拿它覆盖账本 = 让一台失联的 DS 决定服务端认为它上面有几个人:
      少报 → 服务端超额分配座位
      多报 → Hub 永远满员
    """
    shard = cap.ShardProjection(capacity=100)
    ledger = _ledger(sessions=[_rec("a"), _rec("b")])
    cap.sync_shard_projection(shard, ledger)
    assert shard.player_count == 2

    # DS 上报了一个完全不同的数字
    drift = cap.apply_heartbeat_audit(shard, reported_count=57)
    assert shard.player_count == 2, "心跳覆盖了派生的 player_count"
    assert shard.connected_ownership_count == 2
    assert drift == 55, "审计差值应当被返回供告警"


def test_heartbeat_drift_is_reported_for_alerting() -> None:
    """差值持续非零 = DS 与服务端认知分叉,值得查但不改容量。"""
    shard = cap.ShardProjection(capacity=100)
    cap.sync_shard_projection(shard, _ledger(sessions=[_rec("a")]))
    assert cap.apply_heartbeat_audit(shard, reported_count=1) == 0
    assert cap.apply_heartbeat_audit(shard, reported_count=0) == -1


# ── ★ ② connected ownership 没有时间 TTL ─────────────────────────────────


def test_connected_session_never_expires_by_time() -> None:
    """★ 新格式 expires_at_ms=0 → **永不因时间过期**。

    只由 exact Departure 或已确认的 UID teardown 删除。
    靠时间过期就是假装玩家已经离开 —— 而他可能还在那台 DS 上打。
    """
    ledger = _ledger(sessions=[_rec("a", expires_at_ms=0)])
    cap.prune(ledger, pod=POD, uid=UID, epoch=EPOCH, writer=WRITER, now_ms=NOW + 10**12)
    assert "a" in ledger.sessions, "已连接玩家被时间过期删掉了"


def test_zero_expiry_guard_is_not_omitted() -> None:
    """★ 这条盯的是一个具体的写法错误。

    prune 里若写成 `rec.expires_at_ms <= now_ms`(省掉 `> 0 and` 前置),
    则 0 <= now_ms 恒真 → **每次 prune 把所有已连接玩家全删掉** →
    服务端以为 Hub 空了,继续往里塞人。
    """
    ledger = _ledger(sessions=[_rec(str(i), expires_at_ms=0) for i in range(50)])
    cap.prune(ledger, pod=POD, uid=UID, epoch=EPOCH, writer=WRITER, now_ms=NOW)
    assert len(ledger.sessions) == 50


def test_legacy_positive_expiry_session_is_still_cleaned() -> None:
    """旧 / 未来格式里 expires_at_ms > 0 且已到期的仍要清(兼容路径)。"""
    ledger = _ledger(sessions=[_rec("old", expires_at_ms=NOW - 1)])
    cap.prune(ledger, pod=POD, uid=UID, epoch=EPOCH, writer=WRITER, now_ms=NOW)
    assert not ledger.sessions


def test_reservations_do_expire_by_time() -> None:
    """对比:预留**是**有时限的(它只是占位,没人真在里面)。"""
    ledger = _ledger(reservations=[_rec("r", expires_at_ms=NOW - 1)])
    cap.prune(ledger, pod=POD, uid=UID, epoch=EPOCH, writer=WRITER, now_ms=NOW)
    assert not ledger.reservations


# ── ★ ③ successor 去重 ────────────────────────────────────────────────────


def test_successor_does_not_double_count_while_session_alive() -> None:
    """★ 旧 session 还在时 successor **不计容** —— 玩家已经占着 connected 那一格。

    不去重的话一个玩家占两个座位,重连高峰时 Hub 会**假性满员**。
    """
    ledger = _ledger(sessions=[_rec("a")], successors=[_rec("a")])
    reserved, connected = cap.counts(ledger, capacity=100)
    assert (reserved, connected) == (0, 1), "同一玩家被算了两个座位"


def test_successor_counts_after_old_owner_departed() -> None:
    """exact Departure 删掉旧 owner 后,successor 立即作为 reserved seat 计容。

    这段窗口必须计容,否则别人会抢走这个座位而新 Admission 无处落座。
    """
    ledger = _ledger(successors=[_rec("a", expires_at_ms=NOW + 10_000)])
    reserved, connected = cap.counts(ledger, capacity=100)
    assert (reserved, connected) == (1, 0)


def test_multiple_successors_for_one_assignment_are_rejected() -> None:
    """★ 同一 assignment 出现多条 successor = 账本损坏,**整条拒**。

    这条盯的是一个"算出来是对的、账本却是烂的"的洞:派生容量按
    assignment_id 归并,多出来的那几条 successor 会被归并**悄悄吸收**,
    reserved 照样等于 1,一切看起来正常 —— 于是残留记录没人发现,
    一直挡到它绝对到期为止。Go 在解码阶段就 ErrInvalidState 拒掉。
    """
    ledger = cap.CapacityLedger(
        successors={
            "cap-1": _rec("same-assignment"),
            "cap-2": _rec("same-assignment"),
            "cap-3": _rec("same-assignment"),
        }
    )
    with pytest.raises(errcode.PandoraError, match="multiple hub successors"):
        cap.counts(ledger, capacity=100)


def test_successor_merges_with_reservation_of_same_assignment() -> None:
    """对比:跨表归并**仍然要做** —— 同一 assignment 既有 reservation 又有
    successor 时是同一个玩家的同一个座位,只能算一格。

    (把多 successor 改成拒之后,很容易顺手把归并也删了。)
    """
    ledger = cap.CapacityLedger(
        reservations={"a": _rec("a", expires_at_ms=NOW + 10_000)},
        successors={"cap-1": _rec("a", expires_at_ms=NOW + 10_000)},
    )
    reserved, connected = cap.counts(ledger, capacity=100)
    assert (reserved, connected) == (1, 0)


# ── ★ HASH field ↔ 记录内 assignment_id 的身份互校 ────────────────────────


def test_reservation_field_identity_mismatch_is_rejected() -> None:
    """★ field(HASH key)与记录里的 assignment_id 必须一致。

    不等 = 有人按错 field 写了记录。只信 field 的话,后续按记录内 id 去做的
    每一次匹配都会落空:座位删不掉、Departure 找不到对应 owner,漏一个座位。
    """
    ledger = cap.CapacityLedger(reservations={"field-a": _rec("record-b")})
    with pytest.raises(errcode.PandoraError, match="reservation field identity"):
        cap.counts(ledger, capacity=100)


def test_session_field_identity_mismatch_is_rejected() -> None:
    """已连接归属同理 —— 而且更严重:它没有时间 TTL,错了就永久占座。"""
    ledger = cap.CapacityLedger(sessions={"field-a": _rec("record-b")})
    with pytest.raises(errcode.PandoraError, match="session field identity"):
        cap.counts(ledger, capacity=100)


# ── ★ 账本自洽性 ──────────────────────────────────────────────────────────


def test_same_assignment_in_both_ledgers_is_rejected() -> None:
    """★ 同一 assignment 不能同时是"预留"和"已连接" —— 状态机漏了一次转移。

    继续用这种账本算容量只会把错误传下去。
    """
    ledger = _ledger(reservations=[_rec("a")], sessions=[_rec("a")])
    with pytest.raises(errcode.PandoraError, match="reservation and session"):
        cap.counts(ledger, capacity=100)


def test_overflow_is_rejected() -> None:
    """总数超容量 = 某处分配绕过了容量闸,必须显性失败。"""
    ledger = _ledger(sessions=[_rec(str(i)) for i in range(5)])
    with pytest.raises(errcode.PandoraError, match="overflow"):
        cap.counts(ledger, capacity=4)


def test_zero_capacity_is_rejected() -> None:
    with pytest.raises(errcode.PandoraError, match="must be positive"):
        cap.counts(cap.CapacityLedger(), capacity=0)


# ── ★ exact 实例绑定 ──────────────────────────────────────────────────────


def test_records_from_other_instance_are_pruned() -> None:
    """★ 同名 Pod 重建后 uid / epoch 会变。

    不比对的话旧实例的账本会被当成新实例的,容量凭空多出一批**幽灵占座**。
    """
    ledger = _ledger(
        sessions=[_rec("mine"), _rec("stale", hub_instance_uid="uid-OLD")]
    )
    cap.prune(ledger, pod=POD, uid=UID, epoch=EPOCH, writer=WRITER, now_ms=NOW)
    assert list(ledger.sessions) == ["mine"]


@pytest.mark.parametrize(
    "drift", [{"hub_pod_name": "other"}, {"auth_epoch": 99}, {"auth_writer_epoch": 99}]
)
def test_any_identity_field_drift_prunes(drift: dict) -> None:
    """四元组任意一项对不上都要清 —— 部分匹配不算匹配。"""
    ledger = _ledger(sessions=[_rec("x", **drift)])
    cap.prune(ledger, pod=POD, uid=UID, epoch=EPOCH, writer=WRITER, now_ms=NOW)
    assert not ledger.sessions


# ── ★ 三条前置硬约束(约束的是**入参**,不是记录)─────────────────────────


def test_blank_uid_matches_nothing() -> None:
    """★ uid 为空串时**谁都不匹配**。

    这条盯的正是"只写 rec.uid == uid"的写法:调用方还没解析出 GameServer UID
    时传的是空串,而旧格式记录里的 uid 也是空串 —— 两个空串**互相匹配上**,
    任意 Pod 的残留记录都会被认成本实例的,幽灵占座永远清不掉。
    """
    ledger = _ledger(sessions=[_rec("blank", hub_instance_uid="")])
    cap.prune(ledger, pod=POD, uid="", epoch=EPOCH, writer=WRITER, now_ms=NOW)
    assert not ledger.sessions


def test_zero_epoch_matches_nothing() -> None:
    """★ epoch=0 是"未知实例轮次"的哨兵,不是一个真轮次 —— 同样不能自匹配。"""
    ledger = _ledger(sessions=[_rec("zero", auth_epoch=0)])
    cap.prune(ledger, pod=POD, uid=UID, epoch=0, writer=WRITER, now_ms=NOW)
    assert not ledger.sessions


@pytest.mark.parametrize("writer", [0, 1, auth.DS_AUTH_WRITER_EPOCH_V2 + 1])
def test_writer_epoch_gate_is_exact_equality(writer: int) -> None:
    """★ writer 必须**恰等于** DS_AUTH_WRITER_EPOCH_V2,不是 >=。

    记录与入参**完全自洽**(两边都是同一个非 V2 代际),只写四元组比对的实现
    会判"匹配"并把座位留下 —— 这正是 Model B 代际门被拆掉的样子:
    一个不该再有写权的 DS 继续占着座位,没有任何运行期信号。
    """
    ledger = _ledger(sessions=[_rec("x", auth_writer_epoch=writer)])
    cap.prune(ledger, pod=POD, uid=UID, epoch=EPOCH, writer=writer, now_ms=NOW)
    assert not ledger.sessions, "非 V2 代际的自洽记录被留下了"


def test_writer_epoch_constant_matches_go_source() -> None:
    """★ 把 Python 的字面量 2 钉死在 Go 的 DSAuthWriterEpochV2 上。

    proto 里 writer_epoch 是裸 uint32、没有 enum,生成物里没有对应符号,
    这个值只能手抄 —— 手抄的跨语言常量必须有一道对拍,否则 Go 改了值之后
    Python 侧全绿地放行错代际。
    """
    jwt_go = pathlib.Path(__file__).resolve().parents[2] / "pkg" / "auth" / "jwt.go"
    assert jwt_go.is_file(), f"找不到 Go 侧真值出处:{jwt_go}"
    m = re.search(
        r"DSAuthWriterEpochV2\s+uint32\s*=\s*(\d+)", jwt_go.read_text(encoding="utf-8")
    )
    # ★ 匹配不上要 fail 而不是 skip:那说明 Go 侧改了名字/写法,
    # 正是最该响的时刻,skip 会让这道门恰好哑掉。
    assert m is not None, "pkg/auth/jwt.go 里找不到 DSAuthWriterEpochV2 —— Go 侧改了写法?"
    assert auth.DS_AUTH_WRITER_EPOCH_V2 == int(m.group(1))


# ── 派生投影 ────────────────────────────────────────────────────────────────


def test_player_count_is_sum_of_reserved_and_connected() -> None:
    shard = cap.ShardProjection(capacity=100)
    ledger = _ledger(
        reservations=[_rec("r1", expires_at_ms=NOW + 10_000)],
        sessions=[_rec("s1"), _rec("s2")],
    )
    cap.sync_shard_projection(shard, ledger)
    assert (shard.reserved_count, shard.connected_ownership_count) == (1, 2)
    assert shard.player_count == 3


def test_empty_ledger_yields_zero() -> None:
    shard = cap.ShardProjection(capacity=100)
    cap.sync_shard_projection(shard, cap.CapacityLedger())
    assert shard.player_count == 0
