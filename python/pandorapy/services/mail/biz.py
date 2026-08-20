"""mail 业务逻辑层 —— 对应 Go 侧 internal/biz/mail.go + internal/biz/sweep.go。

职责(docs/design/mail.md):
  - ListMail:个人邮件(写扩散)+ 系统/公会邮件(channel + watermark 拉取)合并视图,
    拉完推进游标,实现"看过的不重复拉、过期的不拉"
  - ReadMail / ClaimMail / DeleteMail:个人邮件状态与附件领取(player_mail_claim 幂等)
  - SendSystemMail / SendGuildMail:只插一行(零写扩散,僵尸号不登录即零成本)
  - SendPersonalMail:写收件人收件箱(离线可达)
  - DS 三段式领取(bag phase 2):GetClaimableAttachments → journal → MarkMailClaimed
  - SweepExpired:周期清理,保证各表增长有界

客户端只拿 Mail / MailAttachment 视图(§14):正文 + 附件存 payload blob,
服务端解包成最小视图返回。

附件有三种形态,发放路径完全不同,**混了就是资产事故**:

    stack     无唯一 ID 的可堆叠物品     → inventory.Grant(按 config_id + count 入包)
    instance  有唯一 ID 的铸造凭证       → inventory.GrantInstances(按 count **逐件铸造**)
    transfer  既存实例的托管转移         → inventory.ClaimTransfers(托管行**只改归属**)

★ 三条最容易在移植中丢掉的不变量:

1. **未识别形态 → 整封 fail-closed**(§9.21 滚动升级版本偏斜)
   新版本写入了旧 reader 不认识的附件形态时,**整封拒发、保持未领**,
   绝不"跳过不认识的、发认识的" —— 那样新形态附件被静默吞掉,
   而邮件被标成已领,资产永久消失。

2. **transfer 没有空领豁免**(`allow_noop_grant` 对它**不放行**)
   空领 = 邮件标已领而托管行原地不动 → 实例资产静默滞留 escrow。
   宁可领取报错保持可重领。stack/instance 允许空领是因为它们是"铸造",
   没发出去就是没发;transfer 是"搬运",托管行里的资产已经从发送方扣走了。

3. **claim 记录必须在发放**之后**
   反过来会让"记了 claim 但发放失败"变成永久丢失(下次重领被 claim 表挡住)。
   顺序对了之后,"发放成功但记 claim 失败"由 inventory 的幂等键兜底,不会重发。
"""

from __future__ import annotations

from pandora.bag.v1 import bag_pb2
from pandora.mail.v1 import mail_pb2

from pandorapy import dbguard
from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import snowflake as psnowflake
from pandorapy.services.mail import conf as mconf
from pandorapy.services.mail import data as mdata

DAY_MS = 86_400_000

# 分页上限(决策:docs/design/decision-revisit-list-pagination.md)。
DEFAULT_PAGE_LIMIT = 50
MAX_PAGE_LIMIT = 100

# 邮件 payload 序列化后的字节上限(§9.24 写入侧闸)。
#
# 取值按**设计期望**而非列类型上限(BLOB=65535,写 65535 等于没设):
# 标题 ≤64 rune × 4B(utf8mb4 最坏)= 256B,正文 ≤2048 rune × 4B = 8KB,
# 附件 ≤16 条 × 约 64B = 1KB,加 proto framing 与 instance_grant_key ≈ 10KB。
# 取 16KB 留 1.6 倍余量:正常业务永远碰不到,碰到即说明某个发送方绕过了逐项上限。
MAIL_PAYLOAD_MAX_BYTES = 16 * 1024

# payload 上限的日志标识(dbguard.check_payload 用它打超限/逼近告警)。
_PAYLOAD_NAME = "pandora_social.player_mail.payload"


def clamp_limit(limit: int) -> int:
    """0 归默认、超上限收敛。对应 Go 的 clampLimit。

    不收敛的话客户端一个 limit=10^9 就能让服务端一次拉整张表进内存。
    """
    if limit <= 0:
        return DEFAULT_PAGE_LIMIT
    if limit > MAX_PAGE_LIMIT:
        return MAX_PAGE_LIMIT
    return limit


def partition_attachments(atts) -> tuple[list, list, list, int]:
    """按 oneof 形态分三类,并统计**未识别**的个数。

    ★ 未识别不是"忽略",是必须上报的信号 —— 见模块头 ①。
    用 WhichOneof 而不是逐个 HasField:新增形态时这里自然落进 unknown,
    而逐个 HasField 的写法容易被"顺手加一个分支"改成静默跳过。
    """
    stack, inst, transfer, unknown = [], [], [], 0
    for att in atts:
        kind = att.WhichOneof("body")
        if kind == "stack":
            stack.append(att)
        elif kind == "instance":
            inst.append(att)
        elif kind == "transfer":
            transfer.append(att)
        else:
            unknown += 1
    return stack, inst, transfer, unknown


def expand_instance_config_ids(atts) -> list[int]:
    """把实例型附件按 count **逐件展开**(count 份 → count 个元素)。

    count=0 防御性视为 1 件(发送侧已校验 >=1)。
    逐件展开是因为 instance 语义 = 每件都是独立实例,不能合并成一条 count=N。
    """
    out: list[int] = []
    for att in atts:
        if att.WhichOneof("body") != "instance":
            continue  # 调用方已分组,正常不会到这
        n = att.instance.count or 1
        out.extend([att.instance.item_config_id] * n)
    return out


def mail_claim_key(mail_id: int, player_id: int) -> str:
    """DS 领取链的 journal 幂等键(与意图行同生命周期)。对应 Go 的 mailClaimKey。"""
    return f"mail_claim:{mail_id}:{player_id}"


def decode_payload(payload: bytes) -> mail_pb2.Mail:
    """把存储 blob 解成客户端视图。

    解码失败刻意**不抛**(与 Go 的 `_ = proto.Unmarshal` 一致):列表接口不能因为
    一封坏邮件就整页失败,坏行会退化成空标题空正文,由 sweep / 客诉去处理。
    """
    rec = mail_pb2.MailContentStorageRecord()
    try:
        rec.ParseFromString(payload)
    except Exception:  # noqa: BLE001 —— 见上:列表侧不为单行坏数据整页失败
        return mail_pb2.Mail()
    return mail_pb2.Mail(title=rec.title, body=rec.body, attachments=rec.attachments)


class MailUsecase:
    """mail 业务逻辑核心。对应 Go 的 biz.MailUsecase。"""

    __slots__ = (
        "_repo",
        "_granter",
        "_inst_granter",
        "_xfer_claimer",
        "_escrow_consumer",
        "_id_gen",
        "_cfg",
    )

    def __init__(self, repo, granter, inst_granter, xfer_claimer, cfg) -> None:  # noqa: ANN001
        self._repo = repo
        self._granter = granter  # stack 形态,可为 None
        self._inst_granter = inst_granter  # instance 形态,可为 None
        self._xfer_claimer = xfer_claimer  # transfer 形态,可为 None(但领取时必须有)
        # DS 三段式两件(setter 注入,保持构造签名与 Go 一致):
        self._escrow_consumer = None  # None = 含 transfer 的意图拒终结,防托管行残留双持
        self._id_gen = None  # None = 含 instance 的意图拒创建,不误发
        self._cfg = _normalized_cfg(cfg)

    def set_transfer_escrow_consumer(self, consumer) -> None:  # noqa: ANN001
        """注入托管消费器(DS 三段式 Mark 用)。对应 Go 的 SetTransferEscrowConsumer。"""
        self._escrow_consumer = consumer

    def set_instance_id_gen(self, gen) -> None:  # noqa: ANN001
        """注入实例 ID 生成器(意图展开铸 instance_id 用)。对应 SetInstanceIDGen。"""
        self._id_gen = gen

    # ── 读 ───────────────────────────────────────────────────────────────

    async def list_mail(
        self, player_id: int, now_ms: int, cursor: int, limit: int
    ) -> tuple[list, int]:
        """合并三类邮件。返回 (mails, next_cursor);next_cursor=0 表示个人邮件无更多。

        ★ 系统/公会邮件**只在首页拼接**,翻页只走个人邮件。它们靠 watermark 天然有界
        (每次只拉游标之后的),每页都拼一遍等于同一批邮件在每页重复出现。
        """
        limit = clamp_limit(limit)
        last_sys, last_guild = await self._repo.get_cursor(player_id)

        out: list = []
        max_sys, max_guild = last_sys, last_guild

        personal = await self._repo.list_personal(player_id, now_ms, cursor, limit)
        next_cursor = 0
        if limit > 0 and len(personal) == limit:
            next_cursor = personal[-1].mail_id
        for m in personal:
            out.append(
                _to_mail(m, mail_pb2.MAIL_CHANNEL_PERSONAL, m.status, m.claimed)
            )

        if cursor != 0:
            return out, next_cursor

        for m in await self._repo.list_sys_since(last_sys, now_ms):
            out.append(await self._to_channel_mail(player_id, m, mail_pb2.MAIL_CHANNEL_SYSTEM))
            max_sys = max(max_sys, m.mail_id)

        guild_id = await self._repo.get_player_guild(player_id)
        if guild_id:
            for m in await self._repo.list_guild_since(guild_id, last_guild, now_ms):
                out.append(await self._to_channel_mail(player_id, m, mail_pb2.MAIL_CHANNEL_GUILD))
                max_guild = max(max_guild, m.mail_id)

        if max_sys > last_sys or max_guild > last_guild:
            await self._repo.advance_cursor(player_id, max_sys, max_guild)
        return out, next_cursor

    async def read_mail(self, player_id: int, mail_id: int) -> None:
        """个人邮件置已读。系统/公会邮件靠游标(list_mail 已推进),这里幂等 no-op。"""
        await self._repo.set_personal_status(player_id, mail_id, mdata.STATUS_READ)

    # ── 直连领取链 ───────────────────────────────────────────────────────

    async def claim_mail(self, player_id: int, mail_id: int, now_ms: int) -> list:
        """领取附件。幂等键保证重复领取 / 重试不重发(资产不变量 §9.7)。"""
        payload = await self._repo.get_claimable_payload(player_id, mail_id, now_ms)
        if payload is None:
            raise errcode.PandoraError(
                errcode.ErrMailNotFound, "mail %d not found or not claimable", mail_id
            )

        rec = mail_pb2.MailContentStorageRecord()
        try:
            rec.ParseFromString(payload)
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "decode mail %d: %s", mail_id, exc
            ) from exc

        if not rec.attachments:
            raise errcode.PandoraError(
                errcode.ErrMailNoAttachment, "mail %d has no attachment", mail_id
            )

        claimed, intent_open = await self._repo.get_claim_state(player_id, mail_id)
        if claimed:
            # 已领:返回附件视图 + 明确错误码(客户端据此显示"已领过",不是失败)。
            raise _already_claimed(mail_id, list(rec.attachments))
        if intent_open:
            # DS 三段式领取意图已创建(bag phase 2):本邮件只能经 bag journal 链终结。
            # 旧直连链在此**互斥拒** —— 若继续走 inventory 发放,与已/将落库的 journal 双发。
            raise errcode.PandoraError(
                errcode.ErrMailClaimInProgress,
                "mail %d claim in progress via bag journal",
                mail_id,
            )

        stack_atts, inst_atts, xfer_atts, unknown = partition_attachments(rec.attachments)

        # ★ ① 滚动版本偏斜的 fail-closed 分支(§9.21)。
        # 这是**必须被运维发现**的信号(说明有旧副本在读新数据),但只返回业务码时
        # access log 只有泛化失败,定位不到是哪封 / 几个未知形态 → 必须留 WARN。
        if unknown > 0:
            plog.get().warning(
                "mail_claim_unknown_attachment",
                player_id=player_id,
                mail_id=mail_id,
                unknown=unknown,
                hint="疑似滚动升级版本偏斜:旧副本读到新增附件形态,整封 fail-closed 保持未领",
            )
            raise errcode.PandoraError(
                errcode.ErrMailAttachmentUnsupported,
                "mail %d has %d unrecognized attachment kind(s)",
                mail_id,
                unknown,
            )

        # 发放顺序 stack → instance → transfer,**各用独立幂等键**。
        # 任一步失败,下次重领靠各自的幂等键去重,已发的不重发。
        if stack_atts:
            key = f"mail:{mail_id}:{player_id}"
            if self._granter is not None:
                await self._granter.grant(player_id, stack_atts, key)
            elif not self._cfg.allow_noop_grant:
                raise errcode.PandoraError(errcode.ErrInternal, "inventory granter unavailable")

        if inst_atts:
            # 幂等键优先用发送侧写入的(跨重发稳定);没有才现造。
            key = rec.instance_grant_key or f"mail_inst:{mail_id}:{player_id}"
            if self._inst_granter is not None:
                await self._inst_granter.grant_instances(
                    player_id, expand_instance_config_ids(inst_atts), key
                )
            elif not self._cfg.allow_noop_grant:
                raise errcode.PandoraError(errcode.ErrInternal, "instance granter unavailable")

        if xfer_atts:
            # ★ ② transfer **无空领豁免**(allow_noop_grant 不放行)。
            # 空领 = 邮件标已领而托管行原地滞留,实例资产静默丢失;
            # 宁可领取报错保持可重领。
            if self._xfer_claimer is None:
                raise errcode.PandoraError(errcode.ErrInternal, "transfer claimer unavailable")
            key = f"mail_xfer:{mail_id}:{player_id}"
            await self._xfer_claimer.claim_transfers(player_id, xfer_atts, key)

        # ★ ③ 入库成功后**再**记 claim。
        # 此处即便失败,下次重领被 inventory 的幂等键去重,不会重发。
        await self._repo.record_claim(player_id, mail_id)

        await self._set_personal_claimed(player_id, mail_id)
        return list(rec.attachments)

    async def delete_mail(self, player_id: int, mail_id: int) -> None:
        await self._repo.delete_personal(player_id, mail_id)

    # ── DS 三段式领取(bag phase 2;bag-domain.md §7)──────────────────────
    #
    # 时序(owner DS 驱动):get_claimable_attachments(意图落库,稳定展开)→ DS 预留容量
    # + bag.AppendJournal(op=mail_claim,幂等键=claim_key,单条批)→ mark_mail_claimed。
    # 恰好一次:意图内容持久化(重放逐字节一致 → journal 指纹去重命中即已入包);
    # Mark 前任意点崩溃 → 重走 Get(返回同内容)→ journal 重放去重 → Mark 幂等。

    async def get_claimable_attachments(
        self, player_id: int, mail_id: int, now_ms: int
    ) -> tuple[list, str, bool]:
        """取(或幂等重取)领取意图。返回 (items, claim_key, already_claimed)。"""
        claim_key = mail_claim_key(mail_id, player_id)
        payload = await self._repo.get_claimable_payload(player_id, mail_id, now_ms)
        if payload is None:
            raise errcode.PandoraError(
                errcode.ErrMailNotFound, "mail %d not found or not claimable", mail_id
            )
        claimed, intent_open = await self._repo.get_claim_state(player_id, mail_id)
        if claimed:
            return [], claim_key, True
        if intent_open:
            return await self._load_intent_items(player_id, mail_id, claim_key)

        rec = mail_pb2.MailContentStorageRecord()
        try:
            rec.ParseFromString(payload)
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "decode mail %d: %s", mail_id, exc
            ) from exc
        if not rec.attachments:
            raise errcode.PandoraError(
                errcode.ErrMailNoAttachment, "mail %d has no attachment", mail_id
            )

        intent = self._build_claim_intent(rec)
        created = await self._repo.create_claim_intent(
            player_id, mail_id, intent.SerializeToString()
        )
        if not created:
            # 并发/重放:行已存在(意图或终态),重读为准 —— **绝不覆盖既有展开**。
            # 覆盖会换掉已铸的 instance_id,而 journal 靠内容指纹去重:
            # 换了 ID = 同一封邮件的物品被发两次。
            claimed, _ = await self._repo.get_claim_state(player_id, mail_id)
            if claimed:
                return [], claim_key, True
            return await self._load_intent_items(player_id, mail_id, claim_key)
        return list(intent.items), claim_key, False

    async def _load_intent_items(
        self, player_id: int, mail_id: int, claim_key: str
    ) -> tuple[list, str, bool]:
        """读既有意图行并解包。

        行缺失 = 在"读状态"与"读意图"之间被 Mark 终结(并发重放窗口)→ 按已领返回,
        幂等安全。这里若报错,DS 的重放路径会在一个本该成功的窗口上卡住。
        """
        blob = await self._repo.get_claim_intent(player_id, mail_id)
        if blob is None:
            return [], claim_key, True
        intent = mail_pb2.MailClaimIntentStorageRecord()
        try:
            intent.ParseFromString(blob)
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "decode intent mail %d: %s", mail_id, exc
            ) from exc
        return list(intent.items), claim_key, False

    def _build_claim_intent(self, rec) -> mail_pb2.MailClaimIntentStorageRecord:  # noqa: ANN001
        """把附件展开为稳定 BagItem 列表(instance 形态在此**一次性**铸 ID)。

        任一附件未识别 → 整封 fail-closed(与直连链同语义,不静默跳过)。
        """
        intent = mail_pb2.MailClaimIntentStorageRecord()
        instance_total = 0  # instance 形态跨全部附件的累计件数
        for i, a in enumerate(rec.attachments):
            kind = a.WhichOneof("body")
            if kind == "stack":
                intent.items.append(
                    bag_pb2.BagItem(item_config_id=a.stack.item_config_id, count=a.stack.count)
                )
            elif kind == "instance":
                if self._id_gen is None:
                    raise errcode.PandoraError(
                        errcode.ErrInternal, "instance id generator unavailable"
                    )
                n = a.instance.count or 1
                # ★ 存量行兜底:同口径上限在发送侧已拦截,但**改这道闸之前**入库的
                # 邮件可能带着无界 count。必须在分配与铸号**之前**判定 —— 宁可整封
                # fail-closed 让运营改数据,也不能在这里把内存吃光(§16.5 容量边界)。
                instance_total += n
                if instance_total > self._cfg.max_instances_per_mail:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg,
                        "attachment[%d] instance count exceeds per-mail limit: total=%d max=%d",
                        i,
                        instance_total,
                        self._cfg.max_instances_per_mail,
                    )
                ids = [0] * n
                self._id_gen.generate_into(ids)
                for j in range(n):
                    intent.items.append(
                        bag_pb2.BagItem(
                            item_config_id=a.instance.item_config_id, count=1, instance_id=ids[j]
                        )
                    )
            elif kind == "transfer":
                item = a.transfer.item
                intent.items.append(item)
                intent.transfer_instance_ids.append(item.instance_id)
            else:
                raise errcode.PandoraError(
                    errcode.ErrMailAttachmentUnsupported, "attachment[%d] body required", i
                )
        return intent

    async def mark_mail_claimed(self, player_id: int, mail_id: int) -> None:
        """终结 DS 领取(journal 已 ACK 后调):消 transfer 托管行 → 置终态。

        幂等:已终态 no-op;**无意图且未领取 → ErrInvalidArg**(时序违规:
        journal 之前不得 Mark,否则等于在物品还没入包时就把邮件销账)。
        """
        claimed, intent_open = await self._repo.get_claim_state(player_id, mail_id)
        if claimed:
            return
        if not intent_open:
            # 调用方 bug 或迟到 / 乱序回调。只返回业务码的话现场只有一条泛化失败,
            # 查不出是哪封邮件、哪个玩家 → 必须留 WARN。
            plog.get().warning("mail_mark_without_intent", player_id=player_id, mail_id=mail_id)
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "mail %d has no claim intent to mark", mail_id
            )

        blob = await self._repo.get_claim_intent(player_id, mail_id)
        if blob is not None:
            intent = mail_pb2.MailClaimIntentStorageRecord()
            try:
                intent.ParseFromString(blob)
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "decode intent mail %d: %s", mail_id, exc
                ) from exc
            ids = list(intent.transfer_instance_ids)
            if ids:
                # transfer 附件已经 journal 原样入包:**先**消经济域托管行(幂等,
                # 缺行 no-op),**再**置终态。反过来的话,置完终态崩溃就再也不会重 Mark,
                # 托管行永久残留 = 实例双持。中间崩溃 → 意图仍开 → 重 Mark 重消(恰好一次)。
                if self._escrow_consumer is None:
                    raise errcode.PandoraError(
                        errcode.ErrInternal, "transfer escrow consumer unavailable"
                    )
                await self._escrow_consumer.consume_transfer_escrow(player_id, ids)

        await self._repo.mark_claimed(player_id, mail_id)
        await self._set_personal_claimed(player_id, mail_id)

    # ── 发送 ────────────────────────────────────────────────────────────

    async def send_system_mail(
        self,
        mail_id: int,
        title: str,
        body: str,
        atts,  # noqa: ANN001
        start_ms: int,
        end_ms: int,
        now_ms: int,
    ) -> int:
        """插一行系统邮件。transfer 附件拒收(多人可领与单实例矛盾)。"""
        payload = self._build_payload(title, body, atts, "", allow_transfer=False)
        end_ms = self._default_end(start_ms, end_ms, now_ms)
        self._assert_window(start_ms, end_ms)
        await self._repo.insert_sys_mail(mail_id, start_ms, end_ms, payload)
        return mail_id

    async def send_guild_mail(
        self,
        mail_id: int,
        guild_id: int,
        title: str,
        body: str,
        atts,  # noqa: ANN001
        start_ms: int,
        end_ms: int,
        now_ms: int,
    ) -> int:
        """插一行公会邮件。transfer 附件拒收(同上)。"""
        if guild_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "guild_id required")
        payload = self._build_payload(title, body, atts, "", allow_transfer=False)
        end_ms = self._default_end(start_ms, end_ms, now_ms)
        self._assert_window(start_ms, end_ms)
        await self._repo.insert_guild_mail(mail_id, guild_id, start_ms, end_ms, payload)
        return mail_id

    async def send_personal_mail(
        self,
        mail_id: int,
        to_player_id: int,
        title: str,
        body: str,
        atts,  # noqa: ANN001
        expire_ms: int,
        now_ms: int,
        instance_grant_key: str,
    ) -> int:
        """写收件人收件箱(离线可达)。

        transfer 附件**仅个人邮件**可携带:收件人唯一,与托管行 to_player 一一对应。
        调用方须先 EscrowOutInstances 托管再发信(saga),失败补偿 ReleaseTransferEscrow。

        expire_ms=0 时补默认 TTL —— "一切邮件生命有限"是 sweep 能清理的前提,
        少了这一步,永不过期的邮件会让 player_mail 只增不减。
        """
        if to_player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "to_player_id required")
        payload = self._build_payload(
            title, body, atts, instance_grant_key, allow_transfer=True
        )
        if expire_ms == 0:
            expire_ms = now_ms + self._cfg.default_personal_ttl_days * DAY_MS
        await self._repo.insert_personal_mail(
            mail_id, to_player_id, expire_ms, payload, self._cfg.max_inbox_size
        )
        return mail_id

    # ── sweep 清理(对应 Go 的 internal/biz/sweep.go)─────────────────────
    #
    # 邮件表只增不减会无界增长(写扩散的 player_mail 与 player_mail_claim 尤甚)。
    # 前提是一切邮件生命有限(发送侧已补默认 TTL),这里周期批量回收:
    #
    #   player_mail          过期 + 缓冲期后:已领/无附件直删;带未领附件的先归档再删
    #   sys_mail/guild_mail  失效 + 缓冲期后直删(领取幂等由 claim 行兜底)
    #   player_mail_claim    按雪花 mail_id cutoff 范围删
    #   player_mail_archive  超归档保留期后删(归档表自身有界)
    #
    # 多副本各自跑、无锁(对齐 leaderboard 发奖补扫模式):删除 / INSERT IGNORE 幂等,
    # 并发只多花几次空批,不破坏正确性。每轮每表单批 limit 有界,积压跨轮摊平。

    async def sweep_expired(self, now_ms: int) -> None:
        """跑一轮清理,每表至多一批。

        ★ 任一步失败只记日志继续后面的表:各表清理彼此独立且幂等,下一轮自然重试。
        一处失败就整轮 return 的话,排在前面的表一旦持续报错,后面的表**永远不会被清**,
        而日志只会显示一条与它们无关的错误。
        """
        log = plog.get()
        cfg = self._cfg

        # 个人邮件:过期 + 缓冲期后,按 payload 是否有未领附件分流(归档 or 直删)
        expire_before = now_ms - cfg.expired_retention_days * DAY_MS
        try:
            rows = await self._repo.list_expired_personal(expire_before, cfg.sweep_batch)
        except Exception as exc:  # noqa: BLE001
            log.warning("mail_sweep_list_expired_failed", err=str(exc))
            rows = []
        if rows:
            archive, delete_ids = partition_expired(rows)
            try:
                await self._repo.archive_and_delete_personal(archive, delete_ids)
            except Exception as exc:  # noqa: BLE001
                log.warning("mail_sweep_personal_failed", err=str(exc))
            else:
                log.info(
                    "mail_sweep_personal", deleted=len(delete_ids), archived=len(archive)
                )

        # 系统/公会邮件:失效 + 缓冲期后直删(游标玩家侧天然跳过,不参与拉取)
        end_before = now_ms - cfg.expired_retention_days * DAY_MS
        await self._sweep_step(
            self._repo.delete_sys_mail_ended_before(end_before, cfg.sweep_batch),
            "mail_sweep_sys",
            "deleted",
        )
        await self._sweep_step(
            self._repo.delete_guild_mail_ended_before(end_before, cfg.sweep_batch),
            "mail_sweep_guild",
            "deleted",
        )

        # 领取记录:雪花 mail_id 时间段单调,mail_id < min_id_at(cutoff)
        # ⇔ 邮件创建早于 cutoff。claim_retention_days ≥ 一切邮件可领窗口由发送侧强制
        # (_default_end 把 end_ms 钳到「创建时刻 + claim_retention_days」内)→
        # 被删 claim 的邮件本体必已失效,重复领取不可能发生。
        # ★ 不再依赖 inventory 幂等键永久兜底:inventory_ledger 自身只留 90 天(§9.24),
        #   超期后同 key 重放不会被唯一键拦住。
        claim_cutoff_sec = (now_ms - cfg.claim_retention_days * DAY_MS) // 1000
        max_id = psnowflake.min_id_at(claim_cutoff_sec)
        if max_id > 0:
            await self._sweep_step(
                self._repo.delete_claims_before(max_id, cfg.sweep_batch),
                "mail_sweep_claims",
                "deleted",
            )

        # 归档表:超保留期后清除,归档表自身有界
        await self._sweep_step(
            self._repo.purge_archive_before(cfg.archive_retention_days, cfg.sweep_batch),
            "mail_sweep_archive",
            "purged",
        )

    async def _sweep_step(self, coro, event: str, count_field: str) -> None:
        """跑一条清理语句;失败只 WARN 不中断本轮(事件名 = event + "_failed")。"""
        try:
            n = await coro
        except Exception as exc:  # noqa: BLE001
            plog.get().warning(f"{event}_failed", err=str(exc))
            return
        if n:
            plog.get().info(event, **{count_field: n})

    # ── 内部辅助 ────────────────────────────────────────────────────────

    def _assert_window(self, start_ms: int, end_ms: int) -> None:
        """钳制后窗口无效 = 这封邮件永远不可领 → fail-fast 提醒运营改期,不落死信。"""
        if end_ms <= start_ms:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "mail window invalid: start=%d end=%d (lifetime capped at claim_retention_days=%d)",
                start_ms,
                end_ms,
                self._cfg.claim_retention_days,
            )

    def _default_end(self, start_ms: int, end_ms: int, now_ms: int) -> int:
        """补默认有效期,并把 end_ms 钳到「创建时刻 + claim_retention_days」以内。

        ★ 这条钳制是"重复领取不可能发生"的前提:领取记录按邮件创建时刻 +
        claim_retention_days 清理(sweep_expired);若邮件可领窗口比它长,
        claim 行会**先于**邮件消失,而 inventory 的幂等流水自身只保留 90 天(§9.24),
        兜不住 —— 超长邮件将可以重复领奖。钳制后「claim 行存活 ≥ 邮件可领窗口」恒成立。

        钳制基准用 now_ms(≈ mail_id 生成时刻)而非 start_ms:定时邮件(start 在未来)
        的 claim 清理 cutoff 仍按创建时刻算。
        """
        base = start_ms or now_ms
        if end_ms == 0:
            end_ms = base + self._cfg.default_sys_ttl_days * DAY_MS
        max_end = now_ms + self._cfg.claim_retention_days * DAY_MS
        return min(end_ms, max_end)

    def _build_payload(
        self,
        title: str,
        body: str,
        atts,  # noqa: ANN001
        instance_grant_key: str,
        *,
        allow_transfer: bool,
    ) -> bytes:
        """组装并序列化邮件内容,同时做全部写入侧校验。"""
        title = title.strip()
        if not title:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "title required")
        # 长度按 **rune(码点)** 算而不是字节:上限是给策划/运营看的语义上限,
        # 按字节算会让同样 20 个汉字的标题在不同语言下时通时不通。
        if len(title) > self._cfg.max_title_len:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "title too long")
        if len(body) > self._cfg.max_body_len:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "body too long")
        if len(atts) > self._cfg.max_attachments:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "too many attachments")

        seen_instances: set[int] = set()
        instance_total = 0
        for i, a in enumerate(atts):
            kind = a.WhichOneof("body")
            if kind == "transfer":
                # transfer 仅个人邮件可携带:系统/公会邮件多人可领,与"单实例只改归属"
                # 矛盾(第一个领走后其余人整封领取失败)。
                if not allow_transfer:
                    raise errcode.PandoraError(
                        errcode.ErrMailAttachmentUnsupported,
                        "attachment[%d] transfer form only allowed in personal mail",
                        i,
                    )
                item = a.transfer.item
                if item.instance_id == 0 or item.item_config_id == 0 or item.count != 1:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg,
                        "attachment[%d] transfer requires instance_id/config and count=1",
                        i,
                    )
                # 同一实例在一封邮件里出现两次 = 领取时对同一托管行搬两次,
                # 第二次必然失败而整封 fail-closed —— 发送侧拒掉,不让坏数据入库。
                if item.instance_id in seen_instances:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg,
                        "attachment[%d] duplicate transfer instance %d",
                        i,
                        item.instance_id,
                    )
                seen_instances.add(item.instance_id)
                continue
            if kind == "stack":
                cfg_id, cnt = a.stack.item_config_id, a.stack.count
            elif kind == "instance":
                cfg_id, cnt = a.instance.item_config_id, a.instance.count
            else:
                # 拒绝空 body / 未识别形态入库:落库之后领取侧只能 fail-closed,
                # 那封邮件就**永远领不了**,还得运营去改数据。
                raise errcode.PandoraError(
                    errcode.ErrMailAttachmentUnsupported, "attachment[%d] body required", i
                )
            if cfg_id == 0 or cnt == 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "attachment[%d] item_config_id/count required", i
                )
            if kind == "instance":
                # instance 的 count = 独立实例件数,领取时按它循环铸 instance_id。
                # 必须在**入库前**卡住累计上限:count 是 uint32,没有上界时一封邮件
                # 就能让 _build_claim_intent 的循环跑到内存耗尽(§9.18 / §16.5)。
                # 发送侧拒掉 = 坏数据永不入库;领取侧另有一道同口径的闸兜住存量行。
                instance_total += cnt
                if instance_total > self._cfg.max_instances_per_mail:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg,
                        "attachment[%d] instance count exceeds per-mail limit: total=%d max=%d",
                        i,
                        instance_total,
                        self._cfg.max_instances_per_mail,
                    )
            elif cnt > self._cfg.max_stack_count_per_attachment:
                # stack 的 count 是数量不是循环次数,不构成 DoS(inventory 按容量拒),
                # 但不设限时一封邮件可声明发放 42 亿个道具、坏数据直到领取才暴露。
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "attachment[%d] stack count %d exceeds limit %d",
                    i,
                    cnt,
                    self._cfg.max_stack_count_per_attachment,
                )

        rec = mail_pb2.MailContentStorageRecord(
            title=title, body=body, attachments=atts, instance_grant_key=instance_grant_key
        )
        payload = rec.SerializeToString()
        # 序列化后字节兜底(§9.24 写入侧闸 ③):上面逐项的 rune / 条数上限都是**语义**
        # 上限,不保证序列化后装得进 BLOB(utf8mb4 单 rune 最多 4 字节,2048 rune 正文
        # 最坏 8KB;附件条数上限变化、proto 加字段都会推高实际字节)。
        # 超限 fail-closed 拒发 —— 落库被截断的话玩家附件会无声消失。
        try:
            dbguard.check_payload(_PAYLOAD_NAME, payload, MAIL_PAYLOAD_MAX_BYTES)
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "mail payload too large: %s", exc
            ) from exc
        return payload

    async def _to_channel_mail(self, player_id: int, m, channel: int):  # noqa: ANN001
        """系统/公会邮件 → 客户端视图。拉取即视为已读,领过则显示已领。"""
        claimed = False
        try:
            claimed = await self._repo.has_claimed(player_id, m.mail_id)
        except Exception as exc:  # noqa: BLE001
            # 与 Go 侧一致:查不到领取状态不让整页失败(退化成"未领",玩家再点一次
            # 领取会被 claim 行幂等挡住,不会重发)。Go 侧这里连日志都没有,
            # 补一条 DEBUG:持续失败时否则完全不可观测。
            plog.get().debug(
                "mail_has_claimed_failed", player_id=player_id, mail_id=m.mail_id, err=str(exc)
            )
        mail = decode_payload(m.payload)
        mail.mail_id = m.mail_id
        mail.channel = channel
        mail.status = (
            mail_pb2.MAIL_STATUS_CLAIMED if claimed else mail_pb2.MAIL_STATUS_READ
        )
        mail.claimed = claimed
        mail.created_ms = m.created_ms
        mail.expire_ms = m.end_ms
        return mail

    async def _set_personal_claimed(self, player_id: int, mail_id: int) -> None:
        """个人邮件置 claimed(系统/公会靠 player_mail_claim 表,没有这一行)。

        失败**不影响领取结果**:权威是 claim 表,列表侧幂等纠正。但持续失败会让
        player_mail.status 与权威长期漂移(客户端显示未领、实际已领),
        零可观测就无从排查 → 留 DEBUG。
        """
        try:
            await self._repo.set_personal_status(player_id, mail_id, mdata.STATUS_CLAIMED)
        except Exception as exc:  # noqa: BLE001
            plog.get().debug(
                "mail_set_personal_status_failed",
                player_id=player_id,
                mail_id=mail_id,
                target_status="claimed",
                err=str(exc),
            )


def partition_expired(rows) -> tuple[list, list]:  # noqa: ANN001
    """过期个人邮件分流:带未领附件的进归档(留补偿凭据),已领 / 无附件的直删。

    delete_ids **含归档行**(归档之后本体也要删)。
    payload 解码失败的行保守归档:内容未知,宁多存不误删 —— 误删的是玩家没领的东西。
    """
    archive: list = []
    delete_ids: list[int] = []
    for m in rows:
        delete_ids.append(m.mail_id)
        if m.status == mdata.STATUS_CLAIMED:
            continue
        rec = mail_pb2.MailContentStorageRecord()
        try:
            rec.ParseFromString(m.payload)
        except Exception:  # noqa: BLE001 —— 解不开 = 内容未知 → 归档
            archive.append(m)
            continue
        if len(rec.attachments) > 0:
            archive.append(m)
    return archive, delete_ids


def _already_claimed(mail_id: int, attachments: list) -> errcode.PandoraError:
    """已领取 —— 带上附件视图,客户端可以显示"你领过这些"。

    ★ Go 侧这里是 `(atts, err)` 双返回、失败路径上 atts 仍有意义。Python 用异常
    传播就必须把它挂在异常上,否则客户端只看到一个错误码,"领过什么"静默丢失。
    """
    err = errcode.PandoraError(
        errcode.ErrMailAlreadyClaimed, "mail %d already claimed", mail_id
    )
    err.attachments = attachments  # type: ignore[attr-defined]
    return err


def _to_mail(m, channel: int, status: int, claimed: bool):  # noqa: ANN001
    mail = decode_payload(m.payload)
    mail.mail_id = m.mail_id
    mail.channel = channel
    mail.status = status
    mail.claimed = claimed
    mail.created_ms = m.created_ms
    mail.expire_ms = m.expire_ms
    return mail


class _NormalizedCfg:
    """把上限字段归一化后的配置视图。

    ★ 归一化在这里再做一次(conf.apply_defaults 已覆盖 yaml 路径),是为了兜住
    **直接构造配置对象**的调用方(测试、内嵌装配)。对齐 Go 的 NewMailUsecase:
    一个上限的零值若等于"拒绝一切",任何漏配都会静默把正常业务打死(§14.2)。
    """

    __slots__ = (
        "allow_noop_grant",
        "archive_retention_days",
        "claim_retention_days",
        "default_personal_ttl_days",
        "default_sys_ttl_days",
        "expired_retention_days",
        "max_attachments",
        "max_body_len",
        "max_inbox_size",
        "max_instances_per_mail",
        "max_stack_count_per_attachment",
        "max_title_len",
        "sweep_batch",
    )


def _normalized_cfg(cfg) -> _NormalizedCfg:  # noqa: ANN001
    """按 Go 的 Defaults() 口径把 <=0 的字段退回默认值。

    刻意用 getattr 取值:测试与内嵌装配会传只带几个字段的轻量配置对象,
    缺字段等价于零值 —— 与 Go 侧「直接构造 MailConf 结构体」的形状一致。
    """
    out = _NormalizedCfg()
    out.allow_noop_grant = bool(getattr(cfg, "allow_noop_grant", False))
    for field, default in (
        ("default_sys_ttl_days", mconf.DEFAULT_SYS_TTL_DAYS),
        ("default_personal_ttl_days", mconf.DEFAULT_PERSONAL_TTL_DAYS),
        ("max_inbox_size", mconf.DEFAULT_MAX_INBOX_SIZE),
        ("sweep_batch", mconf.DEFAULT_SWEEP_BATCH),
        ("expired_retention_days", mconf.DEFAULT_EXPIRED_RETENTION_DAYS),
        ("archive_retention_days", mconf.DEFAULT_ARCHIVE_RETENTION_DAYS),
        ("claim_retention_days", mconf.DEFAULT_CLAIM_RETENTION_DAYS),
        ("max_title_len", mconf.DEFAULT_MAX_TITLE_LEN),
        ("max_body_len", mconf.DEFAULT_MAX_BODY_LEN),
        ("max_attachments", mconf.DEFAULT_MAX_ATTACHMENTS),
        ("max_instances_per_mail", mconf.DEFAULT_MAX_INSTANCES_PER_MAIL),
        ("max_stack_count_per_attachment", mconf.DEFAULT_MAX_STACK_COUNT_PER_ATTACHMENT),
    ):
        value = int(getattr(cfg, field, 0) or 0)
        setattr(out, field, value if value > 0 else default)
    return out
