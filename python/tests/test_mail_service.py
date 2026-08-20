"""mail 服务:配置默认值对齐、鉴权边界、列表/发送/DS 三段式、sweep。

这份用例挑的全是**静默出错**的点位 —— 不会崩、日志正常、但语义已经错了:

  1. 配置默认值与 Go 分叉 → 同一份 yaml 两个实现行为不同,**两边都不报错**
  2. 系统 RPC 的守卫方向反了 → 玩家能自助给自己发带附件的邮件
  3. 玩家 RPC 用请求体里的 player_id → 越权读/删他人邮件
  4. watermark 水位回退 → 玩家重复收到同一批系统邮件
  5. end_ms 没被钳到 claim 保留期内 → claim 行先于邮件消失 = 可重复领奖
  6. DS 意图被覆盖重铸 ID → 同一封邮件的物品发两次
  7. sweep 一表失败就整轮 return → 后面的表**永远不被清**
"""

from __future__ import annotations

import pathlib
import re

import pytest
from pandora.bag.v1 import bag_pb2
from pandora.common.v1 import errcode_pb2
from pandora.mail.v1 import mail_pb2

from pandorapy import errcode, interceptors
from pandorapy import snowflake as psnowflake
from pandorapy.services.mail import biz as mbiz
from pandorapy.services.mail import conf as mconf
from pandorapy.services.mail import data as mdata
from pandorapy.services.mail import service as msvc

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
GO_CONF = REPO_ROOT / "services" / "social" / "mail" / "internal" / "conf" / "conf.go"


# ── 1. 配置默认值必须与 Go 逐个相同 ───────────────────────────────────────────


def _camel_to_snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _go_defaults() -> dict[str, int]:
    """从 Go 的 Defaults() 里解析出 mail 段的每个默认值。

    ★ 解析 Go 源码而不是把数字抄进测试:抄一遍就多一个会漂移的真相,
    而"测试抄了同一个错值所以永不红"是本仓刚修完 13 处的那类缺陷。
    """
    src = GO_CONF.read_text(encoding="utf-8")
    consts = {
        m.group(1): int(m.group(2).replace("_", ""))
        for m in re.finditer(r"^const (Default\w+) = ([\d_]+)$", src, re.M)
    }
    out: dict[str, int] = {}
    for field, raw in re.findall(r"^\t\tc\.Mail\.(\w+) = (.+)$", src, re.M):
        raw = raw.strip()
        if raw in consts:
            out[_camel_to_snake(field)] = consts[raw]
        elif raw.isdigit():
            out[_camel_to_snake(field)] = int(raw)
        elif "time.Minute" in raw:
            minutes = int(re.search(r"(\d+)\s*\*\s*time\.Minute", raw).group(1))
            out[_camel_to_snake(field)] = minutes * 60
    return out


def test_mail_conf_defaults_match_go() -> None:
    """逐字段比对 Go 的 Defaults()。分叉的后果是两边都不报错地跑出不同行为。"""
    cfg = mconf.Config()
    cfg.apply_defaults()
    go = _go_defaults()
    assert go, "没能从 Go conf.go 解析出任何默认值 —— 解析规则该跟着 Go 改"
    for field, expected in go.items():
        if field == "sweep_interval":
            actual = int(cfg.mail.sweep_interval_td().total_seconds())
        else:
            actual = getattr(cfg.mail, field)
        assert actual == expected, f"mail.{field} 默认值与 Go 不一致"


def test_mail_conf_covers_every_go_default_field() -> None:
    """Go 设了默认值的字段,Python 侧必须都建模 —— 漏一个就是"配了却不生效"。"""
    for field in _go_defaults():
        assert field in mconf.MailConf.model_fields, f"MailConf 缺字段 {field}"


def test_mail_conf_ports_match_go() -> None:
    """端口是契约:Envoy cluster / run_services.ps1 端口检查都钉在 20009/21009。"""
    src = GO_CONF.read_text(encoding="utf-8")
    assert f'c.Server.Grpc.Addr = "{mconf.DEFAULT_GRPC_ADDR}"' in src
    assert f'c.Server.Http.Addr = "{mconf.DEFAULT_HTTP_ADDR}"' in src


def test_mail_conf_zero_is_not_disable() -> None:
    """★ 上限的零值必须退回默认值,**不是**"禁止一切"。

    零值等于封禁功能的话,任何漏配 / 直接构造配置的路径都会静默把正常业务打死。
    """
    cfg = mconf.MailConf()  # 全零
    norm = mbiz._normalized_cfg(cfg)
    assert norm.max_instances_per_mail == mconf.DEFAULT_MAX_INSTANCES_PER_MAIL
    assert norm.max_stack_count_per_attachment == mconf.DEFAULT_MAX_STACK_COUNT_PER_ATTACHMENT
    assert norm.max_inbox_size == mconf.DEFAULT_MAX_INBOX_SIZE
    assert norm.claim_retention_days == mconf.DEFAULT_CLAIM_RETENTION_DAYS


# ── 夹具 ─────────────────────────────────────────────────────────────────────


class FakeContext:
    """最小 ServicerContext:只需要 invocation_metadata()。"""

    def __init__(self, player_id: int = 0) -> None:
        self._md = (
            ((interceptors.METADATA_KEY_PLAYER_ID, str(player_id)),) if player_id else ()
        )

    def invocation_metadata(self):  # noqa: ANN201
        return self._md


class FakeSnowflake:
    """确定性发号器 —— 意图展开必须"重放逐字节一致",随机 ID 测不出这一点。"""

    def __init__(self, start: int = 1000) -> None:
        self._next = start

    def generate(self) -> int:
        self._next += 1
        return self._next

    def generate_into(self, dst: list[int]) -> None:
        for i in range(len(dst)):
            dst[i] = self.generate()


class FakeRepo:
    """内存 repo。字段命名与 MySQLMailRepo 的方法一一对应。"""

    def __init__(self) -> None:
        self.cursor: tuple[int, int] = (0, 0)
        self.guild_id: int | None = None
        self.personal: list[mdata.MailRow] = []
        self.sys: list[mdata.MailRow] = []
        self.guild: list[mdata.MailRow] = []
        self.advanced: list[tuple[int, int, int]] = []
        self.claimed_set: set[tuple[int, int]] = set()
        self.claim_state: tuple[bool, bool] = (False, False)
        self.intent: bytes | None = None
        self.created_intents: list[bytes] = []
        self.create_returns = True
        self.marked: list[tuple[int, int]] = []
        self.status_calls: list[tuple] = []
        self.payload: bytes | None = None
        self.inserted: list[tuple] = []

    async def get_cursor(self, player_id):  # noqa: ANN001
        return self.cursor

    async def get_player_guild(self, player_id):  # noqa: ANN001
        return self.guild_id

    async def list_personal(self, player_id, now_ms, before_id, limit):  # noqa: ANN001
        rows = [r for r in self.personal if before_id == 0 or r.mail_id < before_id]
        return rows[:limit]

    async def list_sys_since(self, last_sys, now_ms):  # noqa: ANN001
        return [r for r in self.sys if r.mail_id > last_sys]

    async def list_guild_since(self, guild_id, last_guild, now_ms):  # noqa: ANN001
        return [r for r in self.guild if r.mail_id > last_guild]

    async def advance_cursor(self, player_id, sys_max, guild_max):  # noqa: ANN001
        self.advanced.append((player_id, sys_max, guild_max))

    async def has_claimed(self, player_id, mail_id):  # noqa: ANN001
        return (player_id, mail_id) in self.claimed_set

    async def set_personal_status(self, player_id, mail_id, status):  # noqa: ANN001
        self.status_calls.append((player_id, mail_id, status))

    async def delete_personal(self, player_id, mail_id):  # noqa: ANN001
        return None

    async def get_claimable_payload(self, player_id, mail_id, now_ms):  # noqa: ANN001
        return self.payload

    async def get_claim_state(self, player_id, mail_id):  # noqa: ANN001
        return self.claim_state

    async def get_claim_intent(self, player_id, mail_id):  # noqa: ANN001
        return self.intent

    async def create_claim_intent(self, player_id, mail_id, payload):  # noqa: ANN001
        self.created_intents.append(payload)
        return self.create_returns

    async def mark_claimed(self, player_id, mail_id):  # noqa: ANN001
        self.marked.append((player_id, mail_id))
        return True

    async def record_claim(self, player_id, mail_id):  # noqa: ANN001
        return True

    async def insert_sys_mail(self, mail_id, start_ms, end_ms, payload):  # noqa: ANN001
        self.inserted.append(("sys", mail_id, start_ms, end_ms, payload))

    async def insert_guild_mail(self, mail_id, guild_id, start_ms, end_ms, payload):  # noqa: ANN001
        self.inserted.append(("guild", mail_id, guild_id, start_ms, end_ms, payload))

    async def insert_personal_mail(self, mail_id, player_id, expire_ms, payload, max_inbox):  # noqa: ANN001
        self.inserted.append(("personal", mail_id, player_id, expire_ms, payload, max_inbox))


def _row(mail_id: int, **kw) -> mdata.MailRow:
    return mdata.MailRow(mail_id=mail_id, payload=kw.pop("payload", b""), **kw)


def _uc(repo, cfg=None, *, id_gen=None, escrow=None):  # noqa: ANN001
    uc = mbiz.MailUsecase(repo, None, None, None, cfg or mconf.MailConf())
    if id_gen is not None:
        uc.set_instance_id_gen(id_gen)
    if escrow is not None:
        uc.set_transfer_escrow_consumer(escrow)
    return uc


def _stack(config_id: int = 101, count: int = 3):
    return mail_pb2.MailAttachment(
        stack=mail_pb2.StackAttachment(item_config_id=config_id, count=count)
    )


def _instance(config_id: int = 5001, count: int = 2):
    return mail_pb2.MailAttachment(
        instance=mail_pb2.InstanceAttachment(item_config_id=config_id, count=count)
    )


def _transfer(instance_id: int, config_id: int = 5001):
    att = mail_pb2.MailAttachment()
    att.transfer.item.instance_id = instance_id
    att.transfer.item.item_config_id = config_id
    att.transfer.item.count = 1
    return att


# ── 2. 鉴权边界 ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "request_msg"),
    [
        ("ListMail", mail_pb2.ListMailRequest()),
        ("ReadMail", mail_pb2.ReadMailRequest(mail_id=1)),
        ("ClaimMail", mail_pb2.ClaimMailRequest(mail_id=1)),
        ("DeleteMail", mail_pb2.DeleteMailRequest(mail_id=1)),
    ],
)
async def test_player_rpcs_reject_anonymous(method, request_msg) -> None:
    """玩家 RPC 无鉴权身份 → ERR_UNAUTHORIZED(fail-closed,不是当成 player_id=0 去查)。"""
    svc = msvc.MailService(_uc(FakeRepo()), FakeSnowflake())
    resp = await getattr(svc, method)(request_msg, FakeContext(0))
    assert resp.code == errcode_pb2.ERR_UNAUTHORIZED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "request_msg"),
    [
        ("SendSystemMail", mail_pb2.SendSystemMailRequest(title="t")),
        ("SendGuildMail", mail_pb2.SendGuildMailRequest(guild_id=1, title="t")),
        ("SendPersonalMail", mail_pb2.SendPersonalMailRequest(to_player_id=1, title="t")),
        ("GetClaimableAttachments", mail_pb2.GetClaimableAttachmentsRequest(player_id=1, mail_id=2)),
        ("MarkMailClaimed", mail_pb2.MarkMailClaimedRequest(player_id=1, mail_id=2)),
    ],
)
async def test_system_rpcs_reject_client_caller(method, request_msg) -> None:
    """★ 系统 RPC 的守卫方向与玩家 RPC **相反**:带玩家身份的调用一律拒。

    方向弄反(改成"没身份就拒")的后果是玩家能自助给自己发带附件的邮件,
    而所有内网调用方(运营工具 / battle_result / owner DS)全被挡在门外。
    """
    svc = msvc.MailService(_uc(FakeRepo()), FakeSnowflake())
    resp = await getattr(svc, method)(request_msg, FakeContext(777))
    assert resp.code == errcode_pb2.ERR_PERMISSION_DENY


@pytest.mark.asyncio
async def test_player_id_comes_from_auth_not_request() -> None:
    """玩家 RPC 的 player_id 只认鉴权上下文(proto 里那些字段号已 reserved)。"""
    repo = FakeRepo()
    repo.personal = [_row(9, status=mdata.STATUS_UNREAD)]
    svc = msvc.MailService(_uc(repo), FakeSnowflake())
    seen: list[int] = []

    original = repo.list_personal

    async def _spy(player_id, *a, **kw):  # noqa: ANN001
        seen.append(player_id)
        return await original(player_id, *a, **kw)

    repo.list_personal = _spy
    resp = await svc.ListMail(mail_pb2.ListMailRequest(), FakeContext(42))
    assert resp.code == errcode_pb2.OK
    assert seen == [42]


@pytest.mark.asyncio
async def test_claim_mail_returns_attachments_on_already_claimed() -> None:
    """★ 已领过时**失败响应里也要带附件** —— 丢了它客户端只剩一个错误码。"""
    repo = FakeRepo()
    rec = mail_pb2.MailContentStorageRecord()
    rec.attachments.append(_stack(101, 3))
    repo.payload = rec.SerializeToString()
    repo.claim_state = (True, False)
    svc = msvc.MailService(_uc(repo), FakeSnowflake())
    resp = await svc.ClaimMail(mail_pb2.ClaimMailRequest(mail_id=7), FakeContext(42))
    assert resp.code == errcode.ErrMailAlreadyClaimed
    assert [a.stack.item_config_id for a in resp.attachments] == [101]


# ── 3. ListMail ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_mail_advances_watermark_once() -> None:
    """拉完系统/公会邮件后推进水位到本批最大 mail_id。"""
    repo = FakeRepo()
    repo.sys = [_row(11), _row(12)]
    repo.guild_id = 7
    repo.guild = [_row(21)]
    mails, next_cursor = await _uc(repo).list_mail(42, 0, 0, 0)
    assert next_cursor == 0
    assert {m.channel for m in mails} == {
        mail_pb2.MAIL_CHANNEL_SYSTEM,
        mail_pb2.MAIL_CHANNEL_GUILD,
    }
    assert repo.advanced == [(42, 12, 21)]


@pytest.mark.asyncio
async def test_list_mail_skips_channels_when_paging() -> None:
    """★ 翻页(cursor != 0)只走个人邮件。

    每页都拼系统/公会 = 同一批邮件在每一页重复出现,而水位每页都被推一次。
    """
    repo = FakeRepo()
    repo.personal = [_row(5, status=mdata.STATUS_UNREAD)]
    repo.sys = [_row(11)]
    mails, _ = await _uc(repo).list_mail(42, 0, 9, 0)
    assert [m.channel for m in mails] == [mail_pb2.MAIL_CHANNEL_PERSONAL]
    assert repo.advanced == []


@pytest.mark.asyncio
async def test_list_mail_next_cursor_only_when_page_full() -> None:
    """满页才给 next_cursor;不满页给 0,否则客户端会多拉一页空的。"""
    repo = FakeRepo()
    repo.personal = [_row(i) for i in range(100, 90, -1)]
    _, next_cursor = await _uc(repo).list_mail(42, 0, 0, 3)
    assert next_cursor == 98
    repo.personal = [_row(100)]
    _, next_cursor = await _uc(repo).list_mail(42, 0, 0, 3)
    assert next_cursor == 0


def test_clamp_limit() -> None:
    """0 归默认、超上限收敛 —— 不收敛的话一个 limit=10^9 就能拉整张表进内存。"""
    assert mbiz.clamp_limit(0) == mbiz.DEFAULT_PAGE_LIMIT
    assert mbiz.clamp_limit(-5) == mbiz.DEFAULT_PAGE_LIMIT
    assert mbiz.clamp_limit(10) == 10
    assert mbiz.clamp_limit(10**9) == mbiz.MAX_PAGE_LIMIT


# ── 4. 发送侧校验 ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_send_clamps_end_to_claim_retention() -> None:
    """★ end_ms 被钳到「创建时刻 + claim_retention_days」以内。

    不钳的后果:claim 行先于邮件被 sweep 清掉,而 inventory 的幂等流水只留 90 天
    (§9.24)——那封邮件从此可以**反复领奖**。
    """
    repo = FakeRepo()
    cfg = mconf.MailConf()
    uc = _uc(repo, cfg)
    now = 1_000_000_000_000
    far = now + 10_000 * mbiz.DAY_MS
    await uc.send_system_mail(1, "t", "b", [], 0, far, now)
    _, _, _start, end, _payload = repo.inserted[0]
    assert end == now + mconf.DEFAULT_CLAIM_RETENTION_DAYS * mbiz.DAY_MS


@pytest.mark.asyncio
async def test_send_rejects_window_invalid_after_clamp() -> None:
    """钳制后窗口无效 = 这封邮件永远不可领 → fail-fast,不落死信。"""
    repo = FakeRepo()
    now = 1_000_000_000_000
    start = now + 10_000 * mbiz.DAY_MS  # start 比"创建 + 保留期"还晚
    with pytest.raises(errcode.PandoraError) as ei:
        await _uc(repo).send_system_mail(1, "t", "b", [], start, 0, now)
    assert ei.value.code == errcode.ErrInvalidArg
    assert repo.inserted == []


@pytest.mark.asyncio
async def test_send_personal_fills_default_ttl() -> None:
    """expire_ms=0 补默认 TTL —— 没有它,永不过期的邮件让 player_mail 只增不减。"""
    repo = FakeRepo()
    now = 1_000_000_000_000
    await _uc(repo).send_personal_mail(1, 42, "t", "b", [], 0, now, "")
    _, _, _, expire, _, _ = repo.inserted[0]
    assert expire == now + mconf.DEFAULT_PERSONAL_TTL_DAYS * mbiz.DAY_MS


@pytest.mark.asyncio
async def test_send_system_rejects_transfer_attachment() -> None:
    """★ transfer 仅个人邮件可携带:系统邮件多人可领,与"单实例只改归属"矛盾。"""
    repo = FakeRepo()
    with pytest.raises(errcode.PandoraError) as ei:
        await _uc(repo).send_system_mail(1, "t", "b", [_transfer(900)], 0, 0, 0)
    assert ei.value.code == errcode.ErrMailAttachmentUnsupported


@pytest.mark.asyncio
async def test_send_personal_rejects_duplicate_transfer_instance() -> None:
    """同一实例出现两次 = 领取时对同一托管行搬两次,第二次必失败而整封 fail-closed。"""
    repo = FakeRepo()
    with pytest.raises(errcode.PandoraError) as ei:
        await _uc(repo).send_personal_mail(
            1, 42, "t", "b", [_transfer(900), _transfer(900)], 0, 0, ""
        )
    assert ei.value.code == errcode.ErrInvalidArg


@pytest.mark.asyncio
async def test_send_rejects_instance_count_over_per_mail_limit() -> None:
    """★ instance 的 count 是**循环次数**:不设累计上限,一封邮件就能吃光内存。"""
    repo = FakeRepo()
    atts = [_instance(5001, mconf.DEFAULT_MAX_INSTANCES_PER_MAIL), _instance(5002, 1)]
    with pytest.raises(errcode.PandoraError) as ei:
        await _uc(repo).send_personal_mail(1, 42, "t", "b", atts, 0, 0, "")
    assert ei.value.code == errcode.ErrInvalidArg
    assert repo.inserted == []


@pytest.mark.asyncio
async def test_send_rejects_absurd_stack_count() -> None:
    """stack 不是 DoS 面,但"一封邮件发 42 亿个道具"是坏数据,不该进权威存储。"""
    repo = FakeRepo()
    over = mconf.DEFAULT_MAX_STACK_COUNT_PER_ATTACHMENT + 1
    with pytest.raises(errcode.PandoraError) as ei:
        await _uc(repo).send_personal_mail(1, 42, "t", "b", [_stack(101, over)], 0, 0, "")
    assert ei.value.code == errcode.ErrInvalidArg


@pytest.mark.asyncio
async def test_send_rejects_empty_body_attachment() -> None:
    """空 body 附件必须在发送侧拒:落库之后领取侧只能 fail-closed,那封邮件永远领不了。"""
    repo = FakeRepo()
    with pytest.raises(errcode.PandoraError) as ei:
        await _uc(repo).send_personal_mail(
            1, 42, "t", "b", [mail_pb2.MailAttachment()], 0, 0, ""
        )
    assert ei.value.code == errcode.ErrMailAttachmentUnsupported


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("title", "body"),
    [("", "b"), ("x" * 65, "b"), ("t", "y" * 2049)],
)
async def test_send_rejects_over_long_text(title, body) -> None:
    """标题/正文长度按 **rune** 算(默认 64 / 2048),与 Go 的 utf8.RuneCount 同口径。"""
    repo = FakeRepo()
    with pytest.raises(errcode.PandoraError) as ei:
        await _uc(repo).send_personal_mail(1, 42, title, body, [], 0, 0, "")
    assert ei.value.code == errcode.ErrInvalidArg


# ── 5. DS 三段式领取 ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_claimable_builds_stable_intent() -> None:
    """意图展开:stack 一条、instance 按 count 逐件铸 ID、transfer 原样 + 记录托管 ID。"""
    repo = FakeRepo()
    rec = mail_pb2.MailContentStorageRecord()
    rec.attachments.extend([_stack(101, 3), _instance(5001, 2), _transfer(900)])
    repo.payload = rec.SerializeToString()
    uc = _uc(repo, id_gen=FakeSnowflake(1000))

    items, claim_key, already = await uc.get_claimable_attachments(42, 7, 0)
    assert already is False
    assert claim_key == "mail_claim:7:42"
    assert [(i.item_config_id, i.count, i.instance_id) for i in items] == [
        (101, 3, 0),
        (5001, 1, 1001),
        (5001, 1, 1002),
        (5001, 1, 900),  # transfer:原样透传快照,instance_id 保持既有实例的 ID
    ]
    intent = mail_pb2.MailClaimIntentStorageRecord()
    intent.ParseFromString(repo.created_intents[0])
    assert list(intent.transfer_instance_ids) == [900]


@pytest.mark.asyncio
async def test_get_claimable_never_overwrites_existing_intent() -> None:
    """★ 并发/重放时行已存在 → 重读既有意图,**绝不覆盖**。

    覆盖会换掉已铸的 instance_id,而 bag journal 靠内容指纹去重 ——
    换了 ID 就是同一封邮件的物品被发两次。
    """
    repo = FakeRepo()
    rec = mail_pb2.MailContentStorageRecord()
    rec.attachments.append(_instance(5001, 1))
    repo.payload = rec.SerializeToString()
    stored = mail_pb2.MailClaimIntentStorageRecord()
    stored.items.append(bag_pb2.BagItem(item_config_id=5001, count=1, instance_id=777))
    repo.intent = stored.SerializeToString()
    repo.create_returns = False  # INSERT IGNORE 撞上既有行

    items, _, already = await _uc(repo, id_gen=FakeSnowflake(1000)).get_claimable_attachments(
        42, 7, 0
    )
    assert already is False
    assert [i.instance_id for i in items] == [777]


@pytest.mark.asyncio
async def test_get_claimable_terminal_returns_already_claimed() -> None:
    repo = FakeRepo()
    rec = mail_pb2.MailContentStorageRecord()
    rec.attachments.append(_stack())
    repo.payload = rec.SerializeToString()
    repo.claim_state = (True, False)
    items, claim_key, already = await _uc(repo).get_claimable_attachments(42, 7, 0)
    assert (items, claim_key, already) == ([], "mail_claim:7:42", True)


@pytest.mark.asyncio
async def test_get_claimable_intent_vanished_treated_as_claimed() -> None:
    """读状态与读意图之间被 Mark 终结(并发重放窗口)→ 按已领返回,幂等安全。

    这里若报错,DS 的重放路径会卡在一个本该成功的窗口上。
    """
    repo = FakeRepo()
    rec = mail_pb2.MailContentStorageRecord()
    rec.attachments.append(_stack())
    repo.payload = rec.SerializeToString()
    repo.claim_state = (False, True)
    repo.intent = None
    _, _, already = await _uc(repo).get_claimable_attachments(42, 7, 0)
    assert already is True


@pytest.mark.asyncio
async def test_get_claimable_without_id_gen_fails_closed() -> None:
    """没有发号器时含 instance 的意图**拒创建** —— 不误发成堆叠。"""
    repo = FakeRepo()
    rec = mail_pb2.MailContentStorageRecord()
    rec.attachments.append(_instance(5001, 1))
    repo.payload = rec.SerializeToString()
    with pytest.raises(errcode.PandoraError) as ei:
        await _uc(repo).get_claimable_attachments(42, 7, 0)
    assert ei.value.code == errcode.ErrInternal
    assert repo.created_intents == []


@pytest.mark.asyncio
async def test_mark_without_intent_is_rejected() -> None:
    """★ journal 之前不得 Mark:那等于物品还没入包就把邮件销账。"""
    repo = FakeRepo()
    repo.claim_state = (False, False)
    with pytest.raises(errcode.PandoraError) as ei:
        await _uc(repo).mark_mail_claimed(42, 7)
    assert ei.value.code == errcode.ErrInvalidArg
    assert repo.marked == []


@pytest.mark.asyncio
async def test_mark_is_idempotent_when_terminal() -> None:
    repo = FakeRepo()
    repo.claim_state = (True, False)
    await _uc(repo).mark_mail_claimed(42, 7)
    assert repo.marked == []


@pytest.mark.asyncio
async def test_mark_consumes_escrow_before_terminal() -> None:
    """★ 先消托管行、再置终态。

    反过来的话,置完终态崩溃就再也不会重 Mark,托管行永久残留 = 实例双持。
    """
    order: list[str] = []

    class Escrow:
        async def consume_transfer_escrow(self, player_id, ids):  # noqa: ANN001
            order.append(f"escrow:{list(ids)}")

    repo = FakeRepo()
    repo.claim_state = (False, True)
    intent = mail_pb2.MailClaimIntentStorageRecord()
    intent.transfer_instance_ids.append(900)
    repo.intent = intent.SerializeToString()
    original_mark = repo.mark_claimed

    async def _mark(player_id, mail_id):  # noqa: ANN001
        order.append("mark")
        return await original_mark(player_id, mail_id)

    repo.mark_claimed = _mark
    await _uc(repo, escrow=Escrow()).mark_mail_claimed(42, 7)
    assert order == ["escrow:[900]", "mark"]


@pytest.mark.asyncio
async def test_mark_without_escrow_consumer_fails_closed() -> None:
    """含 transfer 的意图在没有托管消费器时**拒终结**,防托管行残留双持。"""
    repo = FakeRepo()
    repo.claim_state = (False, True)
    intent = mail_pb2.MailClaimIntentStorageRecord()
    intent.transfer_instance_ids.append(900)
    repo.intent = intent.SerializeToString()
    with pytest.raises(errcode.PandoraError) as ei:
        await _uc(repo).mark_mail_claimed(42, 7)
    assert ei.value.code == errcode.ErrInternal
    assert repo.marked == []


# ── 6. sweep ─────────────────────────────────────────────────────────────────


def test_partition_expired_archives_unclaimed_with_attachments() -> None:
    """带未领附件的进归档,已领 / 无附件的直删;delete_ids **含归档行**。"""
    rec = mail_pb2.MailContentStorageRecord()
    rec.attachments.append(_stack())
    with_att = rec.SerializeToString()

    rows = [
        mdata.ExpiredPersonalRow(1, 42, mdata.STATUS_UNREAD, 0, 0, with_att),
        mdata.ExpiredPersonalRow(2, 42, mdata.STATUS_CLAIMED, 0, 0, with_att),
        mdata.ExpiredPersonalRow(3, 42, mdata.STATUS_UNREAD, 0, 0, b""),
    ]
    archive, delete_ids = mbiz.partition_expired(rows)
    assert [m.mail_id for m in archive] == [1]
    assert delete_ids == [1, 2, 3]


def test_partition_expired_archives_undecodable_payload() -> None:
    """解不开 = 内容未知 → 保守归档。误删的是玩家还没领的东西。"""
    rows = [mdata.ExpiredPersonalRow(1, 42, mdata.STATUS_UNREAD, 0, 0, b"\xff\xff\xff\xff")]
    archive, delete_ids = mbiz.partition_expired(rows)
    assert [m.mail_id for m in archive] == [1]
    assert delete_ids == [1]


class SweepRepo(FakeRepo):
    def __init__(self, fail: str = "") -> None:
        super().__init__()
        self.fail = fail
        self.calls: list[str] = []
        self.claims_cutoff = 0

    async def list_expired_personal(self, expire_before_ms, limit):  # noqa: ANN001
        self.calls.append("list_expired")
        if self.fail == "list_expired":
            raise RuntimeError("boom")
        return []

    async def archive_and_delete_personal(self, archive, delete_ids):  # noqa: ANN001
        self.calls.append("archive")

    async def delete_sys_mail_ended_before(self, end_before_ms, limit):  # noqa: ANN001
        self.calls.append("sys")
        if self.fail == "sys":
            raise RuntimeError("boom")
        return 1

    async def delete_guild_mail_ended_before(self, end_before_ms, limit):  # noqa: ANN001
        self.calls.append("guild")
        return 0

    async def delete_claims_before(self, max_mail_id, limit):  # noqa: ANN001
        self.calls.append("claims")
        self.claims_cutoff = max_mail_id
        return 0

    async def purge_archive_before(self, retention_days, limit):  # noqa: ANN001
        self.calls.append("archive_purge")
        return 0


@pytest.mark.asyncio
async def test_sweep_continues_after_one_table_fails() -> None:
    """★ 一表失败不能让后面的表**永远不被清**(整轮 return 就是这个后果)。"""
    repo = SweepRepo(fail="sys")
    now = (psnowflake.EPOCH + 400 * 86400) * 1000
    await _uc(repo).sweep_expired(now)
    assert repo.calls == ["list_expired", "sys", "guild", "claims", "archive_purge"]


@pytest.mark.asyncio
async def test_sweep_claims_cutoff_uses_snowflake_min_id() -> None:
    """按 min_id_at(cutoff) 把"创建时间早于 X"翻成主键范围条件。"""
    repo = SweepRepo()
    now = (psnowflake.EPOCH + 400 * 86400) * 1000
    await _uc(repo).sweep_expired(now)
    cutoff_sec = (now - mconf.DEFAULT_CLAIM_RETENTION_DAYS * mbiz.DAY_MS) // 1000
    assert repo.claims_cutoff == psnowflake.min_id_at(cutoff_sec)
    assert repo.claims_cutoff > 0


@pytest.mark.asyncio
async def test_sweep_skips_claims_when_cutoff_before_epoch() -> None:
    """cutoff 早于 snowflake epoch → 没有任何 ID 早于它,跳过而不是拿 0 去删。"""
    repo = SweepRepo()
    await _uc(repo).sweep_expired(psnowflake.EPOCH * 1000)
    assert "claims" not in repo.calls


def test_min_id_at_matches_go_layout() -> None:
    """min_id_at 必须与 Go 的 MinIDAt 同布局:早于 epoch 返回 0,否则时间段独占高位。"""
    assert psnowflake.min_id_at(psnowflake.EPOCH - 1) == 0
    assert psnowflake.min_id_at(psnowflake.EPOCH) == 0
    one_sec = psnowflake.min_id_at(psnowflake.EPOCH + 1)
    assert psnowflake.node_of(one_sec) == 0
    assert psnowflake.step_of(one_sec) == 0
    assert psnowflake.timestamp_of(one_sec) == psnowflake.EPOCH + 1


def test_generate_into_is_strictly_increasing_and_unique() -> None:
    """批量铸号:严格递增 + 唯一(不保证连续)。重号在背包域表现为领取被 fail-closed。"""
    node = psnowflake.Node(3)
    dst = [0] * 64
    node.generate_into(dst)
    assert len(set(dst)) == len(dst)
    assert all(b > a for a, b in zip(dst, dst[1:], strict=False))
    assert all(psnowflake.node_of(x) == 3 for x in dst)
    # 与 generate() 混用仍单调 —— 两条铸号路径共用同一状态。
    assert node.generate() > dst[-1]


# ── 7. 状态常量必须来自 proto ────────────────────────────────────────────────


def test_status_constants_come_from_proto() -> None:
    """DB 的 player_mail.status 与 proto MailStatus 是同一套数值,不许手抄。"""
    assert mdata.STATUS_UNREAD == mail_pb2.MAIL_STATUS_UNREAD
    assert mdata.STATUS_READ == mail_pb2.MAIL_STATUS_READ
    assert mdata.STATUS_CLAIMED == mail_pb2.MAIL_STATUS_CLAIMED
