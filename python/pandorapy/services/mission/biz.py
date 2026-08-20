"""任务域领域逻辑 —— 对应 Go 侧 internal/biz/{mission,reward,sweep}.go。

语义移植自 luyuan/mmorpg C++ MissionSystem/MissionsComp:
  · 接取校验:配置存在 / 未接取 / 未完成 / (type,sub_type) 类型互斥 / 活跃数上限;
  · 条件事实驱动进度:类别命中 + 槽位过滤 + 累加 clamp(判定件在 catalog.py);
  · 全条件满足即完成,完成扇出(同一事务):发奖或标记可领、自动接后续链、
    COMPLETE_MISSION 条件再入(有界迭代);
  · GM 批量完成与正常完成是两条**刻意分离**的路径(D 版 todo.md #225,不得合并)。

分层:引擎(engine.py)是纯函数(状态 + 配置进,突变出),IO 全在 repo ——
事务内 FOR UPDATE 载入状态 → 引擎回调 → 突变与推送出箱同事务持久化。

★ 发放路由(任一类失败整条不 GRANTED,下轮全量重放,下游幂等键各自去重):
    堆叠/货币 → inventory.GrantItems      键 mission:<p>:<m>:stack
    装备实例  → inventory.GrantInstances  键 mission:<p>:<m>:inst
      满包    → mail.SendOverflowMail     同 inst 键作 instance_grant_key(至多一次)
    经验      → player.AddExperience      键 quest:<p>:<m>(reason="quest")
  三下游各用独立幂等键:inventory_ledger 的 uk 是 (player_id, idempotency_key),
  GrantItems 与 GrantInstances 同键会撞收据指纹冲突(fail-closed),必须分键。
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import time

from pandora.mission.v1 import mission_pb2

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import safego
from pandorapy.services.mission import catalog as mcat
from pandorapy.services.mission import engine as eng
from pandorapy.services.mission import granter as mgranter
from pandorapy.services.mission import repo as mrepo

# 单条推送出箱 payload 软上限(列 VARBINARY(2048),留余量给分片再拆)。
PUSH_PAYLOAD_SOFT_LIMIT = 1800
# 单条 MissionUpdateEvent 携带的任务条数上限(分片粒度)。
PUSH_CHUNK_MISSIONS = 6


def now_ms() -> int:
    return int(time.time() * 1000)


class MissionUsecase:
    """任务域业务核心。对应 Go 的 MissionUsecase。

    关于 cellroute:任务域**刻意不接** region/cell 路由器(§14.1 不留"以后再接"的
    钩子)。全仓先例是「接了就真读」或「用不上就完全不接」,没有"存了不读"的中间态。
    """

    __slots__ = (
        "repo",
        "catalogs",
        "items",
        "exp",
        "mail",
        "pusher",
        "cfg",
        "log",
        "now_ms",
        "_push_lease",
        "_push_lease_held",
    )

    def __init__(
        self,
        repo: mrepo.MySQLMissionRepo,
        catalogs: mcat.CatalogSource,
        items,  # noqa: ANN001 —— GrpcItemGranter | NoopItemGranter
        exp,  # noqa: ANN001
        mail=None,  # noqa: ANN001 —— 可 None:满包溢出转邮件不可用,发放失败留补扫
        pusher=None,  # noqa: ANN001 —— 可 None:推送禁用(kafka 未配)
        cfg=None,  # noqa: ANN001 —— conf.MissionConf
        now_fn=None,  # noqa: ANN001 —— 测试注入确定性时钟
    ) -> None:
        self.repo = repo
        self.catalogs = catalogs
        self.items = items
        self.exp = exp
        self.mail = mail
        self.pusher = pusher
        self.cfg = cfg
        self.log = plog.get()
        self.now_ms = now_fn or now_ms
        # 发布器领导权来源;None = 不选举(本副本无条件发布)。
        self._push_lease = None
        # 上一轮领导权状态,只由发布器单协程读写(跃迁日志去重用)。
        self._push_lease_held = False
        # 配置目录要能构造发奖快照 —— 它需要 catalog(拿奖励行/装备位)也需要 biz
        # (铸幂等键、序列化 pb),所以由 biz 在装配期注入回调。
        catalogs.set_build_reward_log(self._build_reward_log)

    # ── 查询 ────────────────────────────────────────────────────────────────

    async def list_missions(self, player_id: int):  # noqa: ANN201
        """权威快照(活跃 + 已完成;也是 push resync 的回源接口)。"""
        cat = self.catalogs.snapshot()
        st = await self.repo.load_player(player_id)
        active = [
            self._to_proto_active(cat, st.active[mid]) for mid in sorted(st.active)
        ]
        completed = [_to_proto_completed(st.done[mid]) for mid in sorted(st.done)]
        return active, completed

    # ── 接取 / 放弃 / GM ────────────────────────────────────────────────────

    async def accept(self, player_id: int, mission_id: int):  # noqa: ANN201
        """玩家接取任务。"""
        cat = self.catalogs.snapshot()
        box: dict[str, eng.ActiveMission] = {}

        def _fn(st: eng.PlayerState):  # noqa: ANN202
            am = self._accept_into(cat, st, mission_id)
            box["am"] = am
            return eng.Mutation(upsert_active=[am])

        await self.repo.mutate_player(player_id, _fn)
        return self._to_proto_active(cat, box["am"])

    async def abandon(self, player_id: int, mission_id: int) -> None:
        """玩家放弃任务。

        语义:已完成不可弃(D 版一致);不在活跃列表返回 ERR_MISSION_NOT_ACCEPTED
        (**刻意差异**:D 版对不存在的任务静默成功,这里显式错误码暴露客户端状态漂移)。
        """

        def _fn(st: eng.PlayerState):  # noqa: ANN202
            if mission_id in st.done:
                raise errcode.PandoraError(
                    errcode.ErrMissionAlreadyCompleted,
                    "abandon completed mission=%d player=%d",
                    mission_id,
                    player_id,
                )
            if mission_id not in st.active:
                raise errcode.PandoraError(
                    errcode.ErrMissionNotAccepted,
                    "abandon non-active mission=%d player=%d",
                    mission_id,
                    player_id,
                )
            del st.active[mission_id]
            return eng.Mutation(delete_active=[mission_id])

        await self.repo.mutate_player(player_id, _fn)

    async def complete_all(self, player_id: int) -> int:
        """GM 批量完成:全部活跃任务置完成 + 清空活跃列表。

        ★ 刻意不发奖、不标记可领、不自动接后续链、不触发 COMPLETE_MISSION 再入 ——
        与正常完成扇出是两条**不可合并**的路径(D 版 CompleteAllMissions)。

        ★ **但仍产出推送**:不推等于让客户端留着一份与权威不一致的活跃列表,直到
        下次 ListMissions 才对齐。推送不承担正确性,推它不会把两条路径合并,漏推却
        会制造一个"只有 GM 知道"的状态漂移。
        """
        counter = {"n": 0}

        def _fn(st: eng.PlayerState):  # noqa: ANN202
            mut = eng.Mutation()
            now = self.now_ms()
            completed_protos = []
            for mid in sorted(st.active):
                del st.active[mid]
                dm = eng.DoneMission(
                    mission_config_id=mid,
                    reward_state=eng.REWARD_STATE_NONE,
                    completed_at_ms=now,
                )
                st.done[mid] = dm
                mut.delete_active.append(mid)
                mut.insert_done.append(dm)
                completed_protos.append(_to_proto_completed(dm))
                counter["n"] += 1
            if counter["n"] == 0:
                return mut
            mut.push_payloads = marshal_event_chunks(
                now, progressed=[], completed=completed_protos, auto_accepted=[]
            )
            return mut

        await self.repo.mutate_player(player_id, _fn)
        return counter["n"]

    # ── 领奖 ────────────────────────────────────────────────────────────────

    async def claim(self, player_id: int, mission_id: int) -> None:
        """领取任务奖励。

        CLAIMABLE→CLAIMED CAS 与 reward_log(PENDING) 同事务 = 「已领」立即权威生效;
        内容发放 at-least-once(提交后立即尝试一次,失败留补扫)。
        """
        cat = self.catalogs.snapshot()
        box: dict[str, list] = {"logs": []}

        def _fn(st: eng.PlayerState):  # noqa: ANN202
            dm = st.done.get(mission_id)
            if dm is None or dm.reward_state != eng.REWARD_STATE_CLAIMABLE:
                raise errcode.PandoraError(
                    errcode.ErrMissionNotClaimable,
                    "claim mission=%d player=%d state=%s",
                    mission_id,
                    player_id,
                    "none" if dm is None else dm.reward_state,
                )
            row = cat.mission_by_id(mission_id)
            if row is None:
                # 完成后任务行被策划删掉:配置错,fail-closed 不发未知内容。
                raise errcode.PandoraError(
                    errcode.ErrMissionConfigNotFound,
                    "claim mission=%d config missing",
                    mission_id,
                )
            entry = self._build_reward_log(cat, player_id, row)
            dm.reward_state = eng.REWARD_STATE_CLAIMED
            mut = eng.Mutation(claim_done=[mission_id])
            if entry is not None:
                mut.reward_logs = [entry]
            box["logs"] = mut.reward_logs
            return mut

        await self.repo.mutate_player(player_id, _fn)
        # 提交后同步尝试发放一次(失败不回滚领取,补扫兜底)。
        await self._grant_entries_best_effort(cat, player_id, box["logs"])

    # ── 条件事实入账(唯一进度写入通道)──────────────────────────────────────

    async def report_facts(
        self, player_id: int, facts: list[eng.Fact], idem_key: str
    ) -> bool:
        """事实入账:收据幂等 → 引擎推进 → 完成扇出,全部同一事务。返回 already。"""
        if not facts or not idem_key or len(idem_key) > 128:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "facts=%d key_len=%d",
                len(facts),
                len(idem_key),
            )
        if len(facts) > self.cfg.max_facts_per_report:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "facts %d > max %d",
                len(facts),
                self.cfg.max_facts_per_report,
            )
        cat = self.catalogs.snapshot()
        box: dict[str, list] = {"logs": []}

        def _fn(st: eng.PlayerState):  # noqa: ANN202
            mut = eng.apply_facts(
                cat,
                st,
                facts,
                self.now_ms(),
                # ★ 自动接链走**完整**接取校验(上限 + 类型互斥),校验不过跳过该条
                # 不阻断整批。不传这个回调 = 完成扇出绕过全部接取校验。
                accept_fn=lambda state, nid: self._chain_accept(cat, state, nid),
            )
            box["logs"] = mut.reward_logs
            if mut.fanout_truncated:
                self.log.error(
                    "mission_fanout_truncated",
                    player_id=player_id,
                    key=idem_key,
                    hint="完成扇出触顶断链;链环应已在配置加载期拒绝,检查跨表校验器",
                )
            mut.push_payloads = self._build_push_payloads(cat, st, mut)
            return mut

        already = await self.repo.apply_facts_tx(
            player_id, idem_key, facts_fingerprint(player_id, facts), _fn
        )
        if already:
            return True
        # 自动发奖:提交后同步尝试一次,失败留补扫。
        await self._grant_entries_best_effort(cat, player_id, box["logs"])
        return False

    # ── 引擎周边(纯函数,只读 catalog)──────────────────────────────────────

    def _accept_into(
        self, cat: mcat.Catalog, st: eng.PlayerState, mission_id: int
    ) -> eng.ActiveMission:
        """接取校验 + 建活跃行。失败抛 PandoraError(D 版 CheckMissionAcceptance)。

        类型互斥从活跃行**现算**(D 版 typeFilter 是派生态,不落库);
        sub_type=0 不参与互斥。
        """
        row = cat.mission_by_id(mission_id)
        if row is None:
            raise errcode.PandoraError(
                errcode.ErrMissionConfigNotFound, "mission=%d", mission_id
            )
        if mission_id in st.active:
            raise errcode.PandoraError(
                errcode.ErrMissionAlreadyAccepted, "mission=%d", mission_id
            )
        if mission_id in st.done:
            raise errcode.PandoraError(
                errcode.ErrMissionAlreadyCompleted, "mission=%d", mission_id
            )
        if len(st.active) >= self.cfg.max_active_missions:
            raise errcode.PandoraError(
                errcode.ErrMissionActiveLimit,
                "active=%d max=%d",
                len(st.active),
                self.cfg.max_active_missions,
            )
        if row.mission_sub_type > 0:
            for other in st.active.values():
                orow = cat.mission_by_id(other.mission_config_id)
                if orow is None:
                    continue
                if (
                    orow.mission_type == row.mission_type
                    and orow.mission_sub_type == row.mission_sub_type
                ):
                    raise errcode.PandoraError(
                        errcode.ErrMissionTypeConflict,
                        "mission=%d conflicts with active=%d type=%d sub=%d",
                        mission_id,
                        other.mission_config_id,
                        row.mission_type,
                        row.mission_sub_type,
                    )
        am = eng.ActiveMission(
            mission_config_id=mission_id,
            progress=[0] * len(mcat.mission_condition_ids(row)),
            accepted_at_ms=self.now_ms(),
        )
        st.active[mission_id] = am
        return am

    def _chain_accept(
        self, cat: mcat.Catalog, st: eng.PlayerState, mission_id: int
    ) -> eng.ActiveMission | None:
        """完成扇出的自动接链:校验不过**跳过该条不阻断整批**,并留下 WARN。

        不留日志会怎样:链上后续任务没接上,而服务端零信号 —— 玩家只看到"做完前置
        任务后什么都没发生",排查时无从判断是配置没配还是校验挡了。
        """
        try:
            return self._accept_into(cat, st, mission_id)
        except errcode.PandoraError as exc:
            self.log.warning(
                "mission_chain_accept_skipped",
                player_id=st.player_id,
                next=mission_id,
                err=str(exc),
            )
            return None

    def _build_reward_log(self, cat: mcat.Catalog, player_id: int, row):  # noqa: ANN001, ANN201
        """按奖励表拼发放内容快照(reward_id=0 返回 None)。"""
        reward_id = row.reward_id
        if reward_id == 0:
            return None
        rrow = cat.reward_by_id(reward_id)
        if rrow is None:
            # 加载期 fk 校验已挡;热更竞态兜底:fail-closed 拒绝而不是发空奖。
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "reward=%d config missing (mission=%d)",
                reward_id,
                row.id,
            )
        record = mission_pb2.MissionRewardStorageRecord(exp=rrow.exp)
        for item_id, count in mcat.reward_items(rrow):
            # ★ 发放形态在**落快照这一刻**冻结:装备走 GrantInstances(`:inst` 键)、
            # 堆叠走 GrantItems(`:stack` 键),两个键在 inventory 台账里互不相识。
            # 若发放时才回读道具表,形态在两次投递之间被热更改掉(或滚动升级期新旧副本
            # 加载着不同配置批次),同一条奖励会先后用两个不同幂等键各发一次 ——
            # 幂等键防不住,因为不是同一个键。
            record.items.append(
                mission_pb2.MissionRewardItem(
                    item_config_id=item_id,
                    count=count,
                    equipment=cat.is_equipment(item_id),
                )
            )
        return mrepo.RewardLogEntry(
            mission_config_id=row.id,
            key=f"mission:{player_id}:{row.id}",
            reward_pb=record.SerializeToString(),
        )

    def _build_push_payloads(
        self, cat: mcat.Catalog, st: eng.PlayerState, mut: eng.Mutation
    ) -> list[bytes]:
        """把引擎产出的三类变化拼成 MissionUpdateEvent 分片。

        progressed / completed 读**最终状态**(与 Go 一致:Go 也是在末尾用最终的
        ActiveMission 指针构造);auto_accepted 读引擎记下的**接取时刻快照**
        —— 见 engine.Mutation.auto_accepted_progress 的说明。
        """
        if not (mut.progressed or mut.completed or mut.auto_accepted):
            return []
        progressed = []
        for mid in mut.progressed:
            am = st.active.get(mid)
            if am is not None:
                progressed.append(self._to_proto_active(cat, am))
        completed = [
            _to_proto_completed(st.done[mid]) for mid in mut.completed if mid in st.done
        ]
        auto_accepted = []
        for mid in mut.auto_accepted:
            snap = mut.auto_accepted_progress.get(mid, ())
            am = st.active.get(mid)
            auto_accepted.append(
                self._to_proto_active(
                    cat,
                    eng.ActiveMission(
                        mission_config_id=mid,
                        progress=list(snap),
                        accepted_at_ms=am.accepted_at_ms if am is not None else 0,
                    ),
                )
            )
        return marshal_event_chunks(
            self.now_ms(),
            progressed=progressed,
            completed=completed,
            auto_accepted=auto_accepted,
        )

    def _to_proto_active(self, cat: mcat.Catalog, am: eng.ActiveMission):  # noqa: ANN201
        """活跃任务 → 客户端可见结构(targets 服务端算好下发,客户端不重算)。"""
        out = mission_pb2.ActiveMission(
            mission_config_id=am.mission_config_id,
            progress=list(am.progress),
            accepted_at_ms=am.accepted_at_ms,
        )
        row = cat.mission_by_id(am.mission_config_id)
        if row is not None:
            cond_ids = mcat.mission_condition_ids(row)
            # 热更加条件后的旧行 progress 比 condition_ids 短:下发前补零,否则
            # progress 与 targets 长度不等,客户端逐槽渲染会错位/越界。
            while len(out.progress) < len(cond_ids):
                out.progress.append(0)
            for i, cid in enumerate(cond_ids):
                target = mcat.mission_slot_target(row, i)
                cond = cat.condition_by_id(cid)
                if cond is not None:
                    target = mcat.condition_effective_target(cond, target)
                out.targets.append(target)
        return out

    # ── 发放链 ──────────────────────────────────────────────────────────────

    async def _grant_entries_best_effort(
        self, cat: mcat.Catalog, player_id: int, logs: list
    ) -> None:
        """事务提交后的立即发放尝试(失败只记日志,补扫兜底)。"""
        for entry in logs or []:
            if entry is None or entry.id == 0:
                continue
            row = mrepo.RewardLogRow(
                id=entry.id,
                player_id=player_id,
                mission_config_id=entry.mission_config_id,
                key=entry.key,
                reward_pb=entry.reward_pb,
            )
            try:
                await self.grant_one(cat, row)
            except asyncio.CancelledError:
                # ★ 取消必须穿透:停机时在途发放要能被中断,吞掉会让 §9.16 的
                # 「先摘流量 → 再排空在途」变成"永远排不空"。
                raise
            except BaseException as exc:  # noqa: BLE001
                self.log.warning(
                    "mission_reward_grant_deferred",
                    player_id=player_id,
                    mission=entry.mission_config_id,
                    key=entry.key,
                    err=str(exc),
                    hint="留 PENDING/FAILED 给补扫,不影响任务状态",
                )

    async def grant_one(self, cat: mcat.Catalog, row: mrepo.RewardLogRow) -> None:
        """发放一条流水并落终态标记。

        成功 → GRANTED;失败 → FAILED(补扫按 status<>GRANTED 继续捞,FAILED 只是
        审计区分)。发放成功但标记失败时:行滞留 → 补扫重放 → 下游幂等键吸收,
        「至多一次」不被破坏。
        """
        gerr: BaseException | None = None
        try:
            await self._deliver(cat, row)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            gerr = exc
        now = self.now_ms()
        try:
            await self.repo.mark_reward(row.id, gerr is None, now)
        except asyncio.CancelledError:
            raise
        except BaseException as merr:  # noqa: BLE001
            self.log.error(
                "mission_reward_mark_failed",
                id=row.id,
                granted=gerr is None,
                err=str(merr),
            )
            if gerr is None:
                raise
        if gerr is not None:
            raise gerr

    async def _deliver(self, cat: mcat.Catalog, row: mrepo.RewardLogRow) -> None:
        """按 reward_pb 快照逐类发放(**不回读配置表**:热更不影响在途发放)。"""
        record = mission_pb2.MissionRewardStorageRecord()
        try:
            record.ParseFromString(row.reward_pb)
        except Exception as exc:  # noqa: BLE001
            # 坏行:永远发不出去,显式暴露而不是静默重试到天荒地老。
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "reward_pb 解码失败(坏行需人工处置) id=%d",
                row.id,
                cause=exc,
            ) from exc

        stacks: list[mgranter.RewardItem] = []
        instances: list[int] = []
        for it in record.items:
            if it.count == 0:
                continue
            # 路由形态优先用快照里的**冻结位**:补扫重放永远走当初那条路由,配置热更 /
            # 滚动升级期的批次差异都不会让同一条奖励换到另一个幂等键上重发一次。
            # 缺省 = 冻结位上线前写的旧行,回退读配置表(旧行为,§9.17 双向兼容)。
            if it.HasField("equipment"):
                equipment = it.equipment
            else:
                equipment = cat.is_equipment(it.item_config_id)
                self.log.warning(
                    "mission_reward_route_unfrozen",
                    id=row.id,
                    player_id=row.player_id,
                    mission=row.mission_config_id,
                    item=it.item_config_id,
                    equipment=equipment,
                    hint="冻结位上线前的历史快照,按当前道具表路由",
                )
            if equipment:
                # 装备按件展开:数量**就是**列表长度。加载期已按同一上限拒批次,
                # 这里仍必须自带闸 —— reward_pb 是历史快照,可能来自早于该上限的批次,
                # 也可能是道具在热更里从堆叠改成了装备。没有这道闸,一条坏快照会让
                # 补扫每轮都按数量分配内存,一路 OOM 拖垮进程(§16.5 容量边界)。
                if (
                    it.count > mcat.MAX_REWARD_EQUIPMENT_INSTANCES
                    or len(instances) + it.count > mcat.MAX_REWARD_EQUIPMENT_INSTANCES
                ):
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "装备发放数量越界(坏快照需人工处置) id=%d item=%d count=%d already=%d max=%d",
                        row.id,
                        it.item_config_id,
                        it.count,
                        len(instances),
                        mcat.MAX_REWARD_EQUIPMENT_INSTANCES,
                    )
                instances.extend([it.item_config_id] * it.count)
            else:
                stacks.append(
                    mgranter.RewardItem(item_config_id=it.item_config_id, count=it.count)
                )

        if stacks:
            await self.items.grant_items(row.player_id, row.key + ":stack", stacks)
        if instances:
            key = row.key + ":inst"
            capacity_full = await self.items.grant_instances(
                row.player_id, key, instances
            )
            if capacity_full:
                if self.mail is None:
                    raise errcode.PandoraError(
                        errcode.ErrUnavailable,
                        "背包满且 mail_addr 未配,装备无法投递(留补扫) player=%d",
                        row.player_id,
                    )
                # 满包溢出转邮件:同键传 instance_grant_key,直发链与邮件领取链共享
                # 幂等键 → 至多一次。
                await self.mail.send_overflow_mail(row.player_id, instances, key)
        if record.exp > 0:
            # 幂等键 quest:<player>:<mission> 是 player.proto 注释的预留口径。
            await self.exp.add_experience(
                row.player_id,
                record.exp,
                f"quest:{row.player_id}:{row.mission_config_id}",
            )

    # ── 后台循环 ①:发奖补扫 ────────────────────────────────────────────────

    async def run_reward_retry(self) -> None:
        """发奖补扫 worker(周期扫、grace 挡在途、单轮限量;**多副本并发安全**)。

        刻意不选举:正确性由下游三个幂等键保证(重复重放被吸收)。§9.21 明确
        「可并行 worker 不得为金丝雀强行全局串行化」。
        """
        await safego.loop(
            "mission_reward_retry",
            self.cfg.reward_retry_interval_td().total_seconds(),
            self._retry_ungranted_once,
        )

    async def _retry_ungranted_once(self) -> None:
        # 每轮取一次批次快照:本轮所有重放共用同一配置批次。
        cat = self.catalogs.snapshot()
        older_than = self.now_ms() - int(
            self.cfg.reward_retry_grace_td().total_seconds() * 1000
        )
        try:
            rows = await self.repo.list_ungranted_rewards(
                older_than, self.cfg.reward_retry_batch
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # ★ 事件名对齐 Go(reward.go:165)。两点都要紧:
            #   ① 名字是 Loki 告警的键 —— 换个名字等于静默失去这条告警覆盖;
            #   ② Go 在这里是 **return**(本轮结束,下一 tick 再来)。让异常冒到
            #      safego 的话,一次 DB 抖动会被记成后台协程 panic 并推高
            #      pandora_safego_panic_recovered_total —— 把"可自愈的瞬时故障"
            #      报成"协程死了",告警语义反了。
            self.log.error("mission_reward_retry_list_failed", err=str(exc))
            return
        if not rows:
            return
        granted = 0
        failed = 0
        for row in rows:
            try:
                await self.grant_one(cat, row)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                failed += 1
                self.log.warning(
                    "mission_reward_retry_failed",
                    id=row.id,
                    player_id=row.player_id,
                    mission=row.mission_config_id,
                    err=str(exc),
                )
                continue
            granted += 1
        self.log.info(
            "mission_reward_retry_round",
            scanned=len(rows),
            granted=granted,
            failed=failed,
        )

    # ── 后台循环 ②:推送出箱发布 ────────────────────────────────────────────

    def set_push_writer_lease(self, lease) -> None:  # noqa: ANN001
        """注入发布器领导权来源;None = 不选举(本副本无条件发布)。

        只允许在装配期调用(run_push_publisher 之前)。
        """
        self._push_lease = lease

    def _push_is_leader(self) -> bool:
        """判定本轮是否由本副本发布,并把领导权跃迁打成日志(未注入恒为 True)。"""
        if self._push_lease is None:
            return True
        held, _token = self._push_lease.current()
        if held != self._push_lease_held:
            self._push_lease_held = held
            self.log.info(
                "mission_push_leadership_changed",
                held=held,
                hint="held=false 时本副本热备不发布;出箱由当选副本排空",
            )
        return held

    async def run_push_publisher(self) -> None:
        """推送出箱发布 worker(FIFO 按 id 序;成功即删行,失败中断本轮保序)。

        **单写者**:出箱是全局未分区表、按 id 序整表 FIFO,属 §9.21 点名要串行化的
        「同一未分区权威的单写者循环」,由 push_writer_lease 选举保证。

        **已知代价,刻意不改**:这里是**全局** FIFO,不是每玩家 FIFO。某个 kafka
        分区 leader 不可用时,队首行恰好哈希到该分区就会卡住整轮,期间其它分区健康的
        玩家推送同样延迟。分区恢复后按原序自动排空,无丢失无重复;窗口内客户端仍可靠
        ListMissions / push.resync 拿到正确态,故不是正确性问题。
        """
        if self.pusher is None:
            self.log.warning(
                "mission_push_publisher_disabled",
                hint="kafka 未配置;mission_push_outbox 将堆积,客户端只能靠 ListMissions 兜底",
            )
            return
        await safego.loop(
            "mission_push_publish",
            self.cfg.push_publish_interval_td().total_seconds(),
            self._push_round,
        )

    async def _push_round(self) -> None:
        # 领导权逐轮判定:失主后本副本立刻停止发布(下一拍即生效),
        # 不需要也不允许把在飞的行"补完" —— 那正是交错的来源。
        if not self._push_is_leader():
            return
        await self._publish_push_batch()

    async def _publish_push_batch(self) -> None:
        batch = self.cfg.push_publish_batch
        while True:
            try:
                rows = await self.repo.fetch_push_outbox(batch)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                # 同上(Go reward.go:260):本轮结束,不把瞬时故障升格成协程 panic。
                self.log.error("mission_push_fetch_failed", err=str(exc))
                return
            if not rows:
                return
            for row in rows:
                try:
                    await self.pusher.push_mission_update(row.player_id, row.payload)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    # 失败中断本轮:同玩家事件必须保序(kafka key=player_id,分区内
                    # 有序,但先跳过失败行再发后续行就乱序了)。
                    self.log.warning(
                        "mission_push_publish_failed", id=row.id, err=str(exc)
                    )
                    return
                try:
                    await self.repo.delete_push_outbox(row.id)
                except asyncio.CancelledError:
                    raise
                except mrepo.PushOutboxRacedError as exc:
                    self.log.warning(
                        "mission_push_publish_raced",
                        id=row.id,
                        err=str(exc),
                        hint="多副本同时跑发布器;单写者未生效或正处滚动升级共存窗口",
                    )
                    return
                except BaseException as exc:  # noqa: BLE001
                    self.log.warning(
                        "mission_push_publish_failed", id=row.id, err=str(exc)
                    )
                    return
            if len(rows) < batch:
                return  # 没满批 = 已排空

    # ── 后台循环 ③:保留期清理 ──────────────────────────────────────────────

    async def run_retention_sweep(self) -> None:
        """周期保留期清理(多副本各自跑,DELETE 幂等无需锁)。"""
        await safego.loop(
            "mission_retention_sweep",
            self.cfg.sweep_interval_td().total_seconds(),
            self._sweep_once,
        )

    async def _sweep_once(self) -> None:
        """单表失败只记日志继续下一表(一张表的问题不该把另一张表也停掉)。"""
        mode = self.cfg.retention_mode_parsed()
        try:
            await self.repo.sweep_reward_log(
                mode, self.cfg.reward_log_retention_days, self.cfg.sweep_batch
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            self.log.error("mission_sweep_reward_log_failed", err=str(exc))
        if not self.cfg.receipt_cleanup_enabled:
            # ★ 组级闸默认关:收据**连报告都不跑**,防误读"待清理量"当成可清。
            # 上游 battle_progress_outbox 的重试没有总期限,删收据后迟到重放会把同一批
            # 事实**双计**进任务进度(同 player exp_history 之因)。
            return
        try:
            await self.repo.sweep_receipts(
                mode, self.cfg.receipt_retention_days, self.cfg.sweep_batch
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            self.log.error("mission_sweep_receipts_failed", err=str(exc))


# ── 模块级辅助 ───────────────────────────────────────────────────────────────


def _to_proto_completed(dm: eng.DoneMission):  # noqa: ANN201
    return mission_pb2.CompletedMission(
        mission_config_id=dm.mission_config_id,
        reward_state=dm.reward_state,
        completed_at_ms=dm.completed_at_ms,
    )


def marshal_event_chunks(
    ts_ms: int, *, progressed: list, completed: list, auto_accepted: list
) -> list[bytes]:
    """把推送事件按任务条数分片序列化,单片超软上限再对半拆。

    这是出箱列 VARBINARY(2048) 的写入侧上限(§9.24 深度闸)。不分片的后果:
    一次大扇出(一批事实完成 20 个任务)拼出的单条事件超 2048 字节,写出箱时被
    dbguard 拒 → **整个事务回滚** → 玩家的进度一并丢失。分片让"推送太大"退化成
    "多发几条",而不是"进度写不进去"。
    """
    items: list[tuple[str, object]] = []
    items.extend(("progressed", p) for p in progressed)
    items.extend(("completed", c) for c in completed)
    items.extend(("auto_accepted", a) for a in auto_accepted)
    if not items:
        return []

    out: list[bytes] = []

    def _build(part: list[tuple[str, object]]) -> None:
        chunk = mission_pb2.MissionUpdateEvent(ts_ms=ts_ms)
        for kind, obj in part:
            if kind == "progressed":
                chunk.progressed.append(obj)
            elif kind == "completed":
                chunk.completed.append(obj)
            else:
                chunk.auto_accepted.append(obj)
        pb = chunk.SerializeToString()
        if len(pb) > PUSH_PAYLOAD_SOFT_LIMIT and len(part) > 1:
            mid = len(part) // 2
            _build(part[:mid])
            _build(part[mid:])
            return
        out.append(pb)

    for i in range(0, len(items), PUSH_CHUNK_MISSIONS):
        _build(items[i : i + PUSH_CHUNK_MISSIONS])
    return out


def facts_fingerprint(player_id: int, facts: list[eng.Fact]) -> bytes:
    """事实批的规范化指纹(收据表同键防串改账)。

    ★ 与 proto 编码**无关**、字段顺序固定、跨版本稳定 —— 直接哈希序列化后的 pb 会
    让 proto 字段增删改变指纹,老收据全部变成"同键不同内容"被 fail-closed 拒,
    上游的正常重放会突然全部报错。格式与 Go 的 factsFingerprint 逐字节相同
    (`p=%d` 后逐条 `|c=%d;a=%d;s=` 再逐槽 `%d,`)。
    """
    h = hashlib.sha256()
    h.update(f"p={player_id}".encode())
    for f in facts:
        h.update(f"|c={f.category};a={f.amount};s=".encode())
        for v in f.slot_values:
            h.update(f"{v},".encode())
    return h.digest()


async def close_quietly(obj) -> None:  # noqa: ANN001
    """关闭一个可能为 None / 可能没有 close 的下游客户端。"""
    if obj is None:
        return
    close = getattr(obj, "close", None)
    if close is None:
        return
    with contextlib.suppress(Exception):
        await close()
