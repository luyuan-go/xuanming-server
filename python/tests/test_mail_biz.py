"""mail 领取测试 —— 三种附件形态的分流与 fail-closed。

重点全部是**资产会静默消失**的那几条:
  1. ★ 未识别形态 → 整封 fail-closed(不能"跳过不认识的、发认识的")
  2. ★ transfer 无空领豁免(空领 = 资产滞留 escrow)
  3. ★ claim 记录在发放**之后**
  4. ★ bag journal 意图开启时旧直连链必须互斥拒(否则双发)
"""

from __future__ import annotations

import pytest
from pandora.mail.v1 import mail_pb2

from pandorapy import errcode
from pandorapy.services.mail import biz as mbiz
from pandorapy.services.mail import conf as mconf


class FakeRepo:
    def __init__(self, payload: bytes | None = None) -> None:
        self.payload = payload
        self.claimed = False
        self.intent_open = False
        self.record_claim_calls: list[tuple[int, int]] = []
        self.status_calls: list[tuple] = []
        self.order: list[str] = []

    async def get_claimable_payload(self, player_id, mail_id, now_ms):  # noqa: ANN001
        return self.payload

    async def get_claim_state(self, player_id, mail_id):  # noqa: ANN001
        return self.claimed, self.intent_open

    async def record_claim(self, player_id, mail_id):  # noqa: ANN001
        self.order.append("record_claim")
        self.record_claim_calls.append((player_id, mail_id))
        return True

    async def set_personal_status(self, player_id, mail_id, status):  # noqa: ANN001
        self.status_calls.append((player_id, mail_id, status))

    async def delete_personal(self, player_id, mail_id):  # noqa: ANN001
        return None


class RecordingGranter:
    def __init__(self, order: list[str], tag: str, fail: bool = False) -> None:
        self.order = order
        self.tag = tag
        self.fail = fail
        self.calls: list[tuple] = []

    async def _record(self, *args) -> None:
        self.order.append(self.tag)
        self.calls.append(args)
        if self.fail:
            raise errcode.PandoraError(errcode.ErrInternal, f"{self.tag} down")

    async def grant(self, player_id, atts, key):  # noqa: ANN001
        await self._record(player_id, tuple(a.stack.item_config_id for a in atts), key)

    async def grant_instances(self, player_id, config_ids, key):  # noqa: ANN001
        await self._record(player_id, tuple(config_ids), key)

    async def claim_transfers(self, player_id, atts, key):  # noqa: ANN001
        await self._record(player_id, len(atts), key)


def Cfg(allow_noop_grant: bool = False):  # noqa: N802 —— 沿用既有调用点写法
    """用**真实**的 MailConf 而不是手搭的桩。

    ★ 手搭桩只带 allow_noop_grant 一个字段时,biz 里任何新读到的配置项都会
    退回默认值而测试照绿 —— 上限类字段(max_instances_per_mail 等)一旦被读错,
    这份测试完全看不见。用真类型能让"字段名写错"当场 AttributeError。
    """
    return mconf.MailConf(allow_noop_grant=allow_noop_grant)


def _payload(*atts) -> bytes:
    rec = mail_pb2.MailContentStorageRecord()
    rec.attachments.extend(atts)
    return rec.SerializeToString()


def _stack(config_id: int, count: int = 1):
    return mail_pb2.MailAttachment(
        stack=mail_pb2.StackAttachment(item_config_id=config_id, count=count)
    )


def _instance(config_id: int, count: int = 1):
    return mail_pb2.MailAttachment(
        instance=mail_pb2.InstanceAttachment(item_config_id=config_id, count=count)
    )


def _transfer(instance_id: int, config_id: int = 5001):
    """transfer 附件 = 既存实例的托管转移凭证。

    item 是实例快照(仅供核对),真正的资产在 escrow 托管行里 ——
    这正是"空领 = 资产滞留"的原因:领取只认托管行。
    """
    att = mail_pb2.MailAttachment()
    att.transfer.source_player_id = 999
    att.transfer.item.instance_id = instance_id
    att.transfer.item.item_config_id = config_id
    return att


def _uc(repo, *, cfg=None, granter=True, inst=True, xfer=True):
    order = repo.order
    return mbiz.MailUsecase(
        repo,
        RecordingGranter(order, "stack") if granter else None,
        RecordingGranter(order, "instance") if inst else None,
        RecordingGranter(order, "transfer") if xfer else None,
        cfg or Cfg(),
    )


# ── ★ ① 未识别形态 → 整封 fail-closed ──────────────────────────────────────


async def test_unknown_attachment_kind_fails_whole_mail(monkeypatch) -> None:
    """★ 未识别形态必须**整封拒发**,不能"跳过不认识的、发认识的"。

    §9.21 滚动升级版本偏斜:新版本写入了旧 reader 不认识的形态。
    若跳过,新形态附件被静默吞掉而邮件被标已领 —— 资产永久消失。
    """
    # 构造一个 body 未设置的附件(模拟旧 reader 读到新形态)
    unknown_att = mail_pb2.MailAttachment()
    repo = FakeRepo(_payload(_stack(1001, 5), unknown_att))
    uc = _uc(repo)
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.claim_mail(1, 100, 0)
    assert exc.value.code == errcode.ErrMailAttachmentUnsupported
    assert "stack" not in repo.order, "未识别形态下仍发放了认识的那部分"
    assert not repo.record_claim_calls, "整封失败却记了 claim"


def test_partition_counts_unknown_not_silently_dropped() -> None:
    """分组函数必须**统计**未识别,而不是丢弃。"""
    stack, inst, xfer, unknown = mbiz.partition_attachments(
        [_stack(1), _instance(2), _transfer(3), mail_pb2.MailAttachment()]
    )
    assert (len(stack), len(inst), len(xfer), unknown) == (1, 1, 1, 1)


# ── ★ ② transfer 无空领豁免 ─────────────────────────────────────────────────


async def test_transfer_without_claimer_always_fails(monkeypatch) -> None:
    """★ transfer 形态在没有 claimer 时**必须报错**,`allow_noop_grant` 也不放行。

    空领 = 邮件标已领而托管行原地不动 → 实例资产静默滞留 escrow。
    宁可领取报错保持可重领。
    """
    for allow_noop in (False, True):
        repo = FakeRepo(_payload(_transfer(777)))
        uc = _uc(repo, cfg=Cfg(allow_noop_grant=allow_noop), xfer=False)
        with pytest.raises(errcode.PandoraError) as exc:
            await uc.claim_mail(1, 100, 0)
        assert exc.value.code == errcode.ErrInternal
        assert "transfer claimer unavailable" in exc.value.msg
        assert not repo.record_claim_calls, f"allow_noop={allow_noop} 时空领了 transfer"


async def test_stack_and_instance_do_allow_noop(monkeypatch) -> None:
    """对比:stack / instance **允许**空领(它们是"铸造",没发出去就是没发)。

    与 transfer 的区别在于 transfer 是"搬运" —— 托管行里的资产已经从发送方扣走了。
    """
    repo = FakeRepo(_payload(_stack(1001), _instance(2002)))
    uc = _uc(repo, cfg=Cfg(allow_noop_grant=True), granter=False, inst=False)
    atts = await uc.claim_mail(1, 100, 0)
    assert len(atts) == 2
    assert repo.record_claim_calls == [(1, 100)]


async def test_stack_without_granter_fails_when_noop_disabled() -> None:
    repo = FakeRepo(_payload(_stack(1001)))
    uc = _uc(repo, cfg=Cfg(allow_noop_grant=False), granter=False)
    with pytest.raises(errcode.PandoraError, match="inventory granter unavailable"):
        await uc.claim_mail(1, 100, 0)


# ── ★ ③ claim 记在发放之后 ─────────────────────────────────────────────────


async def test_claim_recorded_after_all_grants() -> None:
    """★ 顺序必须是 stack → instance → transfer → record_claim。

    反过来会让"记了 claim 但发放失败"变成永久丢失(下次重领被 claim 表挡住)。
    """
    repo = FakeRepo(_payload(_stack(1001), _instance(2002), _transfer(3003)))
    uc = _uc(repo)
    await uc.claim_mail(1, 100, 0)
    assert repo.order == ["stack", "instance", "transfer", "record_claim"]


async def test_grant_failure_does_not_record_claim() -> None:
    """★ 任一发放失败 → 不记 claim,保持可重领。"""
    repo = FakeRepo(_payload(_stack(1001), _transfer(3003)))
    order = repo.order
    uc = mbiz.MailUsecase(
        repo,
        RecordingGranter(order, "stack"),
        RecordingGranter(order, "instance"),
        RecordingGranter(order, "transfer", fail=True),  # transfer 失败
        Cfg(),
    )
    with pytest.raises(errcode.PandoraError):
        await uc.claim_mail(1, 100, 0)
    assert not repo.record_claim_calls, "发放失败却记了 claim —— 资产永久丢失"


async def test_record_claim_failure_still_raises() -> None:
    """记 claim 失败必须抛出 —— 不能吞掉,否则客户端以为领成功了但下次还能再领。

    (重领会被 inventory 幂等键去重,所以不会重发;但状态不一致必须暴露。)
    """

    class FailingRepo(FakeRepo):
        async def record_claim(self, player_id, mail_id):  # noqa: ANN001
            raise errcode.PandoraError(errcode.ErrInternal, "db down")

    repo = FailingRepo(_payload(_stack(1001)))
    uc = _uc(repo)
    with pytest.raises(errcode.PandoraError):
        await uc.claim_mail(1, 100, 0)


# ── ★ ④ bag journal 互斥 ────────────────────────────────────────────────────


async def test_open_intent_blocks_direct_claim() -> None:
    """★ DS 三段式领取意图已创建时,旧直连链必须**互斥拒**。

    否则会与已/将落库的 bag journal 双发 —— 玩家拿到两份。
    """
    repo = FakeRepo(_payload(_stack(1001)))
    repo.intent_open = True
    uc = _uc(repo)
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.claim_mail(1, 100, 0)
    assert exc.value.code == errcode.ErrMailClaimInProgress
    assert "stack" not in repo.order


async def test_already_claimed_returns_attachments_with_error() -> None:
    """已领:带上附件视图 + 明确错误码(客户端显示"你领过这些",不是失败)。"""
    repo = FakeRepo(_payload(_stack(1001)))
    repo.claimed = True
    uc = _uc(repo)
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.claim_mail(1, 100, 0)
    assert exc.value.code == errcode.ErrMailAlreadyClaimed
    assert len(getattr(exc.value, "attachments", [])) == 1


# ── 实例逐件展开 ────────────────────────────────────────────────────────────


def test_instance_expands_per_count() -> None:
    """★ 实例型按 count **逐件展开** —— 每件都是独立实例,不能合并成 count=N。"""
    assert mbiz.expand_instance_config_ids([_instance(5001, 3)]) == [5001, 5001, 5001]
    assert mbiz.expand_instance_config_ids(
        [_instance(5001, 2), _instance(6001, 1)]
    ) == [5001, 5001, 6001]


def test_instance_zero_count_defends_as_one() -> None:
    """count=0 防御性视为 1 件(发送侧已校验 >=1,这里是兜底)。"""
    assert mbiz.expand_instance_config_ids([_instance(5001, 0)]) == [5001]


# ── 幂等键 ──────────────────────────────────────────────────────────────────


async def test_each_form_uses_distinct_idempotency_key() -> None:
    """★ 三种形态各用**独立**幂等键 —— 共用一个键会让第二种形态被误判成已发。"""
    repo = FakeRepo(_payload(_stack(1001), _instance(2002), _transfer(3003)))
    order = repo.order
    stack_g = RecordingGranter(order, "stack")
    inst_g = RecordingGranter(order, "instance")
    xfer_g = RecordingGranter(order, "transfer")
    uc = mbiz.MailUsecase(repo, stack_g, inst_g, xfer_g, Cfg())
    await uc.claim_mail(7, 42, 0)
    keys = {stack_g.calls[0][-1], inst_g.calls[0][-1], xfer_g.calls[0][-1]}
    assert len(keys) == 3, f"幂等键重复:{keys}"
    assert all(str(42) in k and str(7) in k for k in keys), "幂等键必须含 mail+player"


async def test_instance_grant_key_from_record_is_preferred() -> None:
    """发送侧写入的 instance_grant_key 优先 —— 跨重发稳定。"""
    rec = mail_pb2.MailContentStorageRecord()
    rec.attachments.append(_instance(2002))
    rec.instance_grant_key = "stable-key-from-sender"
    repo = FakeRepo(rec.SerializeToString())
    order = repo.order
    inst_g = RecordingGranter(order, "instance")
    uc = mbiz.MailUsecase(repo, None, inst_g, None, Cfg(allow_noop_grant=True))
    await uc.claim_mail(1, 100, 0)
    assert inst_g.calls[0][-1] == "stable-key-from-sender"


# ── 基础校验 ────────────────────────────────────────────────────────────────


async def test_missing_mail_rejected() -> None:
    uc = _uc(FakeRepo(None))
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.claim_mail(1, 100, 0)
    assert exc.value.code == errcode.ErrMailNotFound


async def test_no_attachment_rejected() -> None:
    uc = _uc(FakeRepo(_payload()))
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.claim_mail(1, 100, 0)
    assert exc.value.code == errcode.ErrMailNoAttachment
