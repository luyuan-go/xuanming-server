"""hub_allocator 业务层**后台扫描 / 伸缩 / 整合迁移** —— 对应 Go 侧
`services/battle/hub_allocator/internal/biz/hub.go` 的两段:

  - 第 1984–2137 行「后台心跳超时扫描」:`RunHeartbeatSweep` / `heartbeatSweepTick` /
    `reconcileOwnerCleanups` / `sweepOnce`
  - 第 3440–3979 行「伸缩 / 整合 / 迁移」:`autoScaleEnabled` /
    `tryScaleOutOnNoCapacity` / `reconcileFleetReplicas` / `consolidationEnabled` /
    `consolidateOnce` / `drainEmptyShards` / `drainAndMigrate` / `migratePlayer` /
    `pushMigrate` / `reclaimDrainedShards` / `addShardMember` / `removeShardMember` /
    `sumPlayers`

本模块提供 `SweepMixin`(带 `(u *HubUsecase)` receiver 的那批)与模块级纯函数
(`sum_players`)。最终组装:

    class HubUsecase(SweepMixin, RpcMixin, ShardMixin, HubUsecaseBase): ...

字段与工具全部来自 `HubUsecaseBase` / `ShardMixin`,本文件**一个都不重复实现**。
`add_shard_member` / `remove_shard_member` 反过来由**本批**提供,`biz_shard.py` /
`biz_rpc.py` 只调用不实现(见那两个文件的模块头约定)。

═══════════════════════════════════════════════════════════════════════════════
这批的失败形状:**全是静默停摆**,不是报错
═══════════════════════════════════════════════════════════════════════════════

RPC 出错客户端立刻看得见;后台循环死掉**没有任何人看得见** —— 进程还在、端口还
在、health 还答 SERVING,只是:

  - 心跳超时不再把死分片标 draining(§9 不变量 4 的补偿链断了),玩家继续被分配
    到一台已经不在的 Hub;
  - `reconcile_owner_cleanups` 不再驱动 transfer/release 的旧 owner 驱逐,于是
    同一玩家在两台 DS 上各有一份 owner(§9.22 脑裂);
  - fence 水位不再推扫,失主的前任写者仍能在没被触碰过的 {pod} slot 上写;
  - 强制整合停摆,缩容后留下永远回收不掉的 stale 镜像。

所以本模块对 `safego` 的依赖不是"顺手":**每一轮 tick 必须独立兜底**,单轮异常只
终止本轮,循环整体存活(`CLAUDE.md` 移植要求 12 的直接适用对象)。

═══════════════════════════════════════════════════════════════════════════════
§16.10「禁止用定时器掩盖时序问题」在本批的判别
═══════════════════════════════════════════════════════════════════════════════

本批全是"到点做点什么",但**没有一处**是掩盖时序:

  - `sweep_once` 到期后**重查权威**(`range_stale_shards` 是 Redis ZSET 的实时
    心跳水位),不是"等一会儿就假设 DS 死了";标 draining 只停止**新**分配,
    在场玩家由 §9.22 的 owner lease + 再入屏障负责,不靠这个 tick 判生死。
  - `reclaim_drained_shards` 恰恰是反例的**反面教材**:逻辑 grace 到期后 Go 刻意
    **不删**镜像(返回 0),因为"计时器到了"不是物理拆机证明。这段代码存在的
    全部意义就是拒绝用定时器代替证明。
  - `drain_and_migrate` 的每一步失败都"回加源 member 索引,下个 tick 重扫",
    即到期后重查权威再试,不是假设成功。

═══════════════════════════════════════════════════════════════════════════════
与 Go 的形变(逐条都有理由)
═══════════════════════════════════════════════════════════════════════════════

  1. `ticker + for/select` → `safego.loop(name, interval, fn)`。语义逐条对齐:
     首轮在第一个 tick **之后**执行、慢轮丢 tick 不堆积、单轮异常不终止循环。
     `wasWriter` / `sweptToken` 这两个跨 tick 状态在 Go 是 `*bool` / `*uint64`
     出参,这里收进 `_SweepState` —— 它们**不是**权威状态,只是降噪与幂等提示,
     进程重启即清空(与 Go 同)。

  2. `(值, error)` → 抛异常。Go 里 `reconcileOwnerCleanups` / `sweepOnce` /
     `reconcileFleetReplicas` 返回 error 供 tick 记日志,这里改成抛出、由 tick
     的 try/except 记同名日志。用 bool 表达**控制流**的那些
     (`consolidate_once` / `drain_empty_shards` / `drain_and_migrate` /
     `migrate_player` / `push_migrate`)照抄二元语义。

  3. `u.writerFence.Current()` 在 Go 返回 `(token, held)`,Python 的
     `writer_fence.current()` 返回 **`(held, token)`**(顺序相反,见
     `biz_base.mint_source_revision` 的告诫)。写反了不报错,只会让全服的 fence
     水位永远停在 1。

  4. `proto.Clone` → `repo.clone`(`CopyFrom`,带走 unknown fields)。
     read-modify-write 路径禁止 `DiscardUnknown`(§9 不变量 17)—— Go 测试
     `TestReconcile_ConsolidationMigratesPlayers` 专门注入了一个 2046 号未知字段
     并断言 drain 迁移后仍在。

  5. `proto.Marshal` → `SerializeToString()`,失败分支保留(Python 会在字段越界
     时抛,与 Go 的 marshal error 同位)。

  6. **负的 `min_replicas` / `need`**:Go 会在 `ready[keep:]` / `ready[:len-need]`
     上**切片越界 panic**(被 tick 的 recover 吞掉 = 每轮都白跑)。Python 的负索引
     不会报错,而是从尾部反向取 —— 那等于把"配置写错了"变成"把整个 fleet 排空"。
     这里显式挡住:负值只记 WARN 并本轮不排空(fail-closed)。conf.py 刻意保留
     负值(不兜回默认)正是为了让配置错误暴露出来,而不是悄悄修正。
"""

from __future__ import annotations

import asyncio
import dataclasses
import time
import uuid
from typing import Any

from pandora.hub.v1 import allocator_pb2 as hubpb

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import safego
from pandorapy.services.hub_allocator import ledger as L
from pandorapy.services.hub_allocator import owner_authority as OA
from pandorapy.services.hub_allocator import repo as R
from pandorapy.services.hub_allocator.biz_base import (
    MIGRATE_REASON_CONSOLIDATION,
    REASON_SHARD_HEARTBEAT_TIMEOUT,
    STATE_DRAINING,
    STATE_READY,
)
from pandorapy.services.hub_allocator.biz_shard import (
    assignment_binding_v2_complete,
    assignment_same_instance,
    authoritative_shard,
    bind_assignment_auth,
    least_loaded,
    sticky_release_track,
    transfer_cleanup_ref,
)

# `safego` 的点位名。**必须与 Go 的 `safego.Recover(ctx, "hub_heartbeat_sweep")`
# 逐字节一致**:它是 `pandora_safego_panic_recovered_total{name}` 的 label 值,
# 两栈并存期改一个字母,告警规则就只覆盖到一半的副本(而"少了一半"没有任何提示)。
SWEEP_TASK_NAME = "hub_heartbeat_sweep"

# 单个分片单 tick 的默认迁移批量(Go: `batch <= 0` 时的兜底 50)。
# ★ 判据是 `<= 0` 而不是 `== 0`:这是 Go 在**业务代码里**写死的兜底,与 conf.py
#   那条"默认值一律 `== 0`"的纪律不冲突 —— 后者说的是"不要偷偷修正配置",
#   而这里是算法自身不能除以 / 比较一个非正批量。照抄 Go。
DEFAULT_CONSOLIDATION_BATCH = 50

# 同上:`players_per_hub <= 0` 时的算式兜底(Go 硬编码 500)。
DEFAULT_PLAYERS_PER_HUB = 500


@dataclasses.dataclass(slots=True)
class _SweepState:
    """跨 tick 的**降噪 / 幂等提示**,不是权威状态。对应 Go 的 `wasWriter` / `sweptToken`。

    - `was_writer` 初值 `True`:与 Go 一致。它只决定"失去写者身份时打不打那条
      Warn",初值给 True 是为了第一次失主一定留痕;给 False 会让进程起来就不是
      写者的那批副本静默,而那恰恰是最需要看见的情况。
    - `swept_token` 是**本届已完成 fence 水位推扫**的 token。它只是"这一届别重复
      推"的提示:推扫本身幂等,丢了最多多跑一次(Go 注释亦然)。
    """

    was_writer: bool = True
    swept_token: int = 0


def sum_players(shards: list) -> int:
    """汇总分片在册人数(负数视为 0)。对应 Go 的 `sumPlayers`。

    ★ 负数按 0 计而不是原样累加:`player_count` 理论上不该为负,但真出现负值时
      原样累加会把总量算小,进而让 `need` 偏低、触发不该发生的排空。丢弃负值是
      唯一不会造成"少算导致误排空"的处置。

    ★ Go 的 `total` 是 int64,理论上会回绕;Python 不会。差异不可达(要溢出需要
      ~40 亿个满载分片),且 Python 侧"不回绕"的方向是安全的 —— 巨大的 total 只会
      让 `need` 被钳到 `max_replicas`,而 Go 回绕成负数会让它掉回 `min_replicas`
      并触发排空。
    """
    total = 0
    for s in shards:
        if s.player_count > 0:
            total += s.player_count
    return total


class SweepMixin:
    """`HubUsecase` 的后台扫描 / 伸缩 / 整合迁移。对应 Go hub.go 1984–2137 + 3440–3979。

    ★ 刻意**不**加 `__slots__`(与 `HubUsecaseBase` / `ShardMixin` / `RpcMixin`
      同因):多 mixin 叠加时 slots 布局要在每层重复声明,漏一层就静默退回
      `__dict__`,制造"以为有约束其实没有"的假象。
    """

    # 下面这些由 HubUsecaseBase / ShardMixin 提供,这里只声明给读者看:
    #   repo / fleet / scaler / auth_repo / migrate / owner_auth / owner_admitted
    #   writer_fence / cfg
    #   retry() / shard_ttl_sec() / assignment_saga_ttl_sec() / heartbeat_max_age_ms()
    #   mint_source_revision() / migrate_resign_session_jti()
    #   reconcile_shard_topology() / routable_shard_views() / assignment_routable()
    #   ensure_existing_assignment_seat() / reserve_routable_seat()
    #   compensate_reserved_seat() / release_assignment_seat()
    #   register_transfer_cleanup() / remove_transfer_cleanup_ref()
    #   resume_assignment_cleanup() / prepare_hub_ticket() / sign_hub_ticket()
    #   bind_owner_for_published_hub_assignment()

    # ── 后台心跳超时扫描 ─────────────────────────────────────────────────

    async def run_heartbeat_sweep(self) -> None:
        """启动后台心跳超时扫描,直到被取消(§9 不变量 4)。对应 Go 的 `RunHeartbeatSweep`。

        ★ 必须走 `safego.loop` 而不是自建 `while True` + `create_task`:
          裸 task 的异常会被存进 Task 对象等人来取,没人取就**零日志**地躺着 ——
          进程照跑、端口照服务、health 照答 SERVING,而这条补偿链已经死了
          (`pandorapy/safego.py` 模块头有实测)。

        `interval <= 0` 由 `safego.loop` 自身挡住(打 `safego_loop_invalid_interval`
        后立即返回,防误配自旋),这里不重复判。
        """
        interval = self.cfg.sweep_interval_td().total_seconds()
        # 事件名 + 字段与 Go 逐字节一致(Loki 面板跨栈共用)。interval / timeout
        # 直接取配置串:`apply_defaults` 写回的就是 Go duration 形式("5s"/"30s"),
        # 与 Go 的 `.String()` 同形。
        plog.get().info(
            "hub_heartbeat_sweep_started",
            interval=self.cfg.sweep_interval,
            timeout=self.cfg.heartbeat_timeout,
        )
        state = _SweepState()

        async def _tick() -> None:
            await self.heartbeat_sweep_tick(state)

        try:
            await safego.loop(SWEEP_TASK_NAME, interval, _tick)
        except asyncio.CancelledError:
            # Go 在 `<-ctx.Done()` 分支里打这条后 return;Python 必须**再抛出去**,
            # 否则停机时 `server.run()` 的 gather 等不到这个任务结束。
            plog.get().info("hub_heartbeat_sweep_stopped")
            raise

    async def heartbeat_sweep_tick(self, state: _SweepState) -> None:
        """单个清扫 tick。对应 Go 的 `heartbeatSweepTick`。

        Go 在函数头 `defer safego.Recover(ctx, "hub_heartbeat_sweep")`,使 recover
        作用域恰为一轮;Python 侧这层由 `safego.loop` 的 `run_once` 提供(同名点位),
        本函数**不再自己兜底** —— 兜两层会让第二层永远收不到异常,变成假的防线。

        四个步骤的错误各自吞掉并记**不同**事件名:任何一步失败都不该阻断后面的步骤,
        它们之间没有依赖,而合并成一条日志会让运维分不清是哪条链停了。
        """
        # 复审 P1-5:census 准入缓存按 last-touch TTL 清死实例项。本地内存卫生,
        # 不经存储、不依赖 writer 身份,故置于 writer 门控**之前**每 tick 无条件执行
        # —— 否则热备副本自己处理过的心跳留下的缓存项永不回收(§9.18 有界要求)。
        # 活实例项每心跳续期,只有已销毁实例项会被清。
        OA.sweep_stale_owner_admitted(
            self.owner_admitted, time.monotonic() - OA.OWNER_ADMITTED_STALE_TTL_SEC
        )
        # R9 P0-7:非写者副本跳 tick,避免 RollingUpdate 重叠窗口内双写者并发
        # reconcile/sweep(存储级 fence 是最终防线,这里是快路径 + 降噪)。
        if self.writer_fence is not None:
            # ★ Python 是 `(held, token)`,与 Go 的 `(token, held)` **相反**。
            held, token = self.writer_fence.current()
            if not held:
                if state.was_writer:
                    plog.get().warning("hub_heartbeat_sweep_paused_not_writer")
                    state.was_writer = False
                return
            if not state.was_writer:
                plog.get().info("hub_heartbeat_sweep_resumed_writer")
                state.was_writer = True
            # 继任者水位推扫的**再断言**:推扫已前移为接流前硬门(当选后宣告持有前
            # 必须跑成功),故正常情况下这里恒是零写入 no-op。保留它是给"fence 已注入
            # 但未走激活钩子"的装配(dev/测试/warmup 误配)兜底;失败下个 tick 重试,
            # 不阻塞扫描。
            if token != state.swept_token:
                try:
                    await self.repo.advance_writer_fences()
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001 —— 下个 tick 重试
                    plog.get().warning(
                        "hub_writer_fence_sweep_failed", token=token, err=str(exc)
                    )
                else:
                    state.swept_token = token
                    plog.get().info("hub_writer_fence_swept", token=token)
        for step, event in (
            (self.reconcile_owner_cleanups, "hub_owner_cleanup_reconcile_failed"),
            (self.reconcile_shard_topology, "hub_reconcile_topology_failed"),
            (self.sweep_once, "hub_heartbeat_sweep_failed"),
            (self.reconcile_fleet_replicas, "hub_reconcile_replicas_failed"),
        ):
            try:
                await step()
            except asyncio.CancelledError:
                # ★ 必须先于宽 except 放行:CancelledError 继承 BaseException,
                #   被吞掉会让停机时这条循环退不出去。
                raise
            except BaseException as exc:  # noqa: BLE001 —— 每步独立降级,不阻断后续
                plog.get().warning(event, err=str(exc))

    async def reconcile_owner_cleanups(self) -> None:
        """index-first transfer/release saga 的**重启恢复**。对应 Go 的 `reconcileOwnerCleanups`。

        全局 pod 索引是持久 superset:陈旧 ref 只按 exact `(player, target-assignment)`
        身份移除,**机械上不可能**删掉并发 winner 的 ref。

        ★ 判据是"当前 assignment 是否仍指名这个 source pod",不是"这条 ref 有多老"。
          按时间清理会在源 DS 恢复慢时把还没驱逐完的旧 owner 从待清理集合里抹掉 ——
          玩家从此在两台 DS 各有一份 owner,而且再没有人回来清。

        Go 返回 firstErr 供 tick 记日志;这里改成把**第一个**异常抛出去(其余已被
        逐条处理),语义等价。
        """
        pods = await self.repo.list_transfer_cleanup_pods()
        first_err: BaseException | None = None
        for source_pod in pods:
            try:
                refs = await self.repo.list_transfer_cleanups(source_pod)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                if first_err is None:
                    first_err = exc
                continue
            for ref in refs:
                try:
                    assignment = await self.repo.get_assignment(ref.player_id)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    # 读权威失败 ≠ ref 陈旧。跳过本条,绝不据此摘索引
                    # (摘掉就再没人回来清这个旧 owner)。
                    if first_err is None:
                        first_err = exc
                    continue
                expected_source = ""
                if assignment is not None and assignment.assignment_id == ref.target_assignment_id:
                    if assignment.transfer_cleanup_pending:
                        expected_source = assignment.transfer_source_hub_pod_name
                    elif assignment.release_cleanup_pending:
                        expected_source = assignment.hub_pod_name
                if expected_source == "" or expected_source != source_pod:
                    await self.remove_transfer_cleanup_ref(source_pod, ref)
                    continue
                try:
                    await self.resume_assignment_cleanup(ref.player_id, ref.target_assignment_id)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    plog.get().warning(
                        "hub_owner_cleanup_retry_failed",
                        source_pod=source_pod,
                        player_id=ref.player_id,
                        assignment_id=ref.target_assignment_id,
                        err=str(exc),
                    )
                    if first_err is None:
                        first_err = exc
        if first_err is not None:
            raise first_err

    async def sweep_once(self) -> None:
        """扫一次:心跳早于阈值的分片 → 标 draining + 移出 active(停止分配)。

        对应 Go 的 `sweepOnce`(§9 不变量 4)。

        ★ 从未心跳过的种子分片(active ZSET 里 score=0)被 `range_stale_shards`
          排除,不会被误标 draining —— 否则一次冷启动就把全部新分片扫死。

        ★ 标 draining 只停止**新**分配,不代表判定 DS 已死:在场玩家的归属由
          §9.22 的 owner lease + 再入屏障负责。这也是本函数没有违反 §16.10 的原因
          —— 到期动作是"重查权威水位后停止分配",不是"假设它死了并放行别处"。
        """
        threshold = L.now_ms() - self.heartbeat_max_age_ms()
        stale = await self.repo.range_stale_shards(threshold)
        for pod in stale:

            def _mark_draining(s: Any) -> None:
                if s.state == STATE_READY:
                    s.state = STATE_DRAINING  # 心跳超时:停止向其分配新玩家

            try:
                await self.repo.update_shard_with_lock(
                    pod, self.retry(), _mark_draining, self.shard_ttl_sec()
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                # 分片镜像已经没了(ErrHubNoAvailable)不是错误:本来就要停掉它。
                if errcode.as_code(exc) != errcode.ErrHubNoAvailable:
                    plog.get().warning("sweep_mark_draining_failed", pod=pod, err=str(exc))
            try:
                await self.repo.remove_active(pod)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning("sweep_remove_active_failed", pod=pod, err=str(exc))
            plog.get().warning(
                "hub_shard_heartbeat_timeout",
                pod=pod,
                ds_pod=pod,
                reason=REASON_SHARD_HEARTBEAT_TIMEOUT,
                threshold_ms=threshold,
                timeout=self.cfg.heartbeat_timeout,
                stale_total=len(stale),
            )

    # ── 伸缩 ─────────────────────────────────────────────────────────────

    def auto_scale_enabled(self) -> bool:
        """自动扩缩容是否真正可用。对应 Go 的 `autoScaleEnabled`。

        ★ 两个条件缺一不可:开关开了但 fleet provider 不实现 `HubFleetScaler`
          (Mock / Local 刻意不实现)时,整套 reconcile 会每轮空跑并对假分片跑
          consolidation。`self.scaler` 在基座里由 `isinstance` 断言得出。
        """
        return self.cfg.autoscale_enabled and self.scaler is not None

    async def try_scale_out_on_no_capacity(self, region: str) -> None:
        """当前 region 无可用分片时的兜底扩容(+1)。对应 Go 的 `tryScaleOutOnNoCapacity`。

        触发后调用方**仍然**返回 `ErrHubNoAvailable`,由上游退避重试进新副本 ——
        这里不等新 pod 起来(那才是 §16.10 说的"用等待掩盖时序")。

        全程 best-effort:扩容失败只告警,绝不把一次"没容量"升级成"扩容也炸了"的
        另一种错误码,客户端的处置是同一个(退避重试)。
        """
        if not self.auto_scale_enabled():
            return
        try:
            current = await self.scaler.get_fleet_replicas()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "hub_scaleout_get_replicas_failed", region=region, err=str(exc)
            )
            return
        desired = current + 1
        if desired < self.cfg.min_replicas:
            desired = self.cfg.min_replicas
        if desired > self.cfg.max_replicas:
            desired = self.cfg.max_replicas
        if desired == current:
            return
        try:
            await self.scaler.set_fleet_replicas(desired)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "hub_scaleout_set_replicas_failed",
                region=region,
                current=current,
                desired=desired,
                err=str(exc),
            )
            return
        plog.get().info(
            "hub_scaleout_triggered", region=region, **{"from": current}, to=desired
        )

    async def reconcile_fleet_replicas(self) -> None:  # noqa: C901,PLR0912 —— 与 Go 同形状
        """周期性副本治理(每个 sweep tick 一次)。对应 Go 的 `reconcileFleetReplicas`。

          ① 扩容(立即,**仅向上**):`ceil(total/players_per_hub) > current` 即扩;
          ② 排空多余分片:总在线>0 且开了强制整合 → 搬迁最空的多余分片;
             总在线=0 → 把超出 `min_replicas` 的空 ready 分片标 draining 并**盖戳**;
          ③ 回收 + 缩容:见 `reclaim_drained_shards`(当前恒 0,故缩容段不会执行)。

        ★ ② 里"总在线=0 也必须盖戳走回收路径"是刻意的:直接把 Fleet 缩到 min 后,
          Agones 删掉的 pod 只会被心跳超时扫成一个**没有** `draining_since_ms` 的
          draining 分片,而回收逻辑跳过这种分片、sweep 又每 tick 续 TTL ——
          镜像就成了永久残留的 stale shard,并把过期 `hub_ds_addr` 发给重登玩家。
        """
        if not self.auto_scale_enabled():
            return
        shards = await self.repo.list_shards()
        total_players = sum_players(shards)
        current = await self.scaler.get_fleet_replicas()

        min_replicas = self.cfg.min_replicas
        max_replicas = self.cfg.max_replicas
        players_per_hub = self.cfg.players_per_hub
        if players_per_hub <= 0:
            players_per_hub = DEFAULT_PLAYERS_PER_HUB

        # 负载所需 ready 分片数(总在线=0 → min)。
        need = min_replicas
        if total_players > 0:
            # 先夹到 max 再用:Go 在 int64 内夹紧后才转 int32,防止 total 极大时
            # 除法结果超 int32 回绕成负数。Python 不回绕,但**顺序照抄** ——
            # 顺序换了就不是同一个 need(先转再夹 vs 先夹再转在边界上不同)。
            needed = (total_players + players_per_hub - 1) // players_per_hub
            if needed > max_replicas:
                needed = max_replicas
            need = needed
            if need < min_replicas:
                need = min_replicas

        # ① 扩容(立即,仅向上)。扩容当 tick 不再缩容,等新 pod ready 后下个 tick 治理。
        if need > current:
            await self.scaler.set_fleet_replicas(need)
            plog.get().info(
                "hub_fleet_scaled_out",
                current=current,
                desired=need,
                players=total_players,
                players_per_hub=players_per_hub,
                min=min_replicas,
                max=max_replicas,
            )
            return

        # ② 排空多余分片。
        drained = False
        if total_players > 0 and self.consolidation_enabled():
            drained = await self.consolidate_once(shards, need)
        elif total_players == 0:
            drained = await self.drain_empty_shards(shards, min_replicas)
        if drained:
            try:
                shards = await self.repo.list_shards()  # 重读快照供回收判断
            except asyncio.CancelledError:
                raise
            except BaseException:  # noqa: BLE001 —— 对齐 Go 的 `if ferr == nil`
                pass

        # ③ 回收 + 缩容。
        reclaimed = await self.reclaim_drained_shards(shards)
        if reclaimed == 0:
            # 绝不从"Redis 镜像列表缺失 / 变短"推导缩容授权。只有升级成 exact
            # 逐实例拆机 saga 之后才可能返回正数(见 reclaim_drained_shards)。
            return
        live = len(shards) - reclaimed
        desired = current
        target = live
        if target < need:
            target = need
        if target < min_replicas:
            target = min_replicas
        if target > max_replicas:
            target = max_replicas
        if target < current:
            desired = target  # 只在此处缩容
        if desired != current:
            await self.scaler.set_fleet_replicas(desired)
            plog.get().info(
                "hub_fleet_scaled_in",
                current=current,
                desired=desired,
                players=total_players,
                reclaimed=reclaimed,
                min=min_replicas,
                max=max_replicas,
            )

    # ── 强制整合 ─────────────────────────────────────────────────────────

    def consolidation_enabled(self) -> bool:
        """强制整合开关(需自动扩缩容已开)。对应 Go 的 `consolidationEnabled`。

        ★ 刻意**不**要求 migrate pusher:即便没接 Kafka,服务端权威搬迁 +
          Hub DS drain 心跳仍能让玩家重连到新分片。把 pusher 写成硬前置会让
          一个纯观测通道的缺失挡掉整条整合链。
        """
        return self.auto_scale_enabled() and self.cfg.consolidation_enabled

    async def consolidate_once(self, shards: list, need: int) -> bool:
        """ready 分片多于 `need` 时,把最空的多余分片标 draining 并搬迁其玩家。

        对应 Go 的 `consolidateOnce`。返回是否有分片被排空(供调用方决定是否重读快照)。

        ★ 排序键是 `(player_count, shard_id)`:并列时必须用 `shard_id` 而不是遍历
          顺序 —— `list_shards` 的顺序来自 Redis SET,两次调用可能不同,不定序会让
          每个 tick 排空不同的分片,谁也排不空。
        """
        ready = [s for s in shards if s.state == STATE_READY]
        if len(ready) <= need:
            return False  # 没有多余 ready 分片
        if need < 0:
            # 见模块头形变 ⑥:Go 在这里会切片越界 panic。负 `need` 只可能来自负的
            # `min_replicas`(conf 刻意保留负值以暴露配置错误),此时"排空全部 ready
            # 分片"是灾难性的误动作,fail-closed 本轮不排空。
            plog.get().warning("hub_consolidate_skipped_negative_need", need=need)
            return False
        # 升序前段 = 最空的多余分片(保留最满的 need 个承接玩家)。
        ready.sort(key=lambda s: (s.player_count, s.shard_id))
        surplus = ready[: len(ready) - need]
        drained = False
        for s in surplus:
            if await self.drain_and_migrate(s):
                drained = True
        return drained

    async def drain_empty_shards(self, shards: list, keep: int) -> bool:
        """大厅没人时,把超出 `keep` 的空 ready 分片标 draining + 盖戳。

        对应 Go 的 `drainEmptyShards`。返回是否有分片被排空。

        盖戳的理由见 `reconcile_fleet_replicas` ② 的说明(不盖戳 = 不可回收的
        stale 镜像永久残留)。
        """
        ready = [s for s in shards if s.state == STATE_READY]
        if len(ready) <= keep:
            return False  # 不超过保底,无需排空
        if keep < 0:
            # 同 `consolidate_once`:Go 会 panic,Python 的负索引会静默把**全部**空
            # 分片排空。fail-closed。
            plog.get().warning("hub_drain_empty_skipped_negative_keep", keep=keep)
            return False
        # 保留 shard_id 最小的 keep 个(全空,排序仅取确定性)。
        ready.sort(key=lambda s: s.shard_id)
        surplus = ready[keep:]
        drained = False
        for s in surplus:
            if await self.drain_and_migrate(s):
                drained = True
        return drained

    async def drain_and_migrate(self, shard: Any) -> bool:  # noqa: C901,PLR0912
        """标 draining(盖时间戳)并服务端权威搬迁其在册玩家。对应 Go 的 `drainAndMigrate`。

        单 tick 每分片最多搬 `consolidation_batch` 人(防抢占),剩余留下个 tick 续搬。

        ★ 成员反向索引是 **best-effort 优化**(只在 AssignHub/TransferHub 维护):
          部署前已在线、索引里没有的老玩家不会被这里搬迁,而是靠 Hub DS 的 drain
          心跳兜底 —— 客户端收到 drain 指令后重连 AssignHub,幂等路径发现旧分片非
          ready 即释放旧位重分,旧分片计数随之递减,最终仍可回收。索引数明显少于
          在册人数时告警,便于观测首次整合的降级范围。
        """
        now = L.now_ms()

        def _mark(s: Any) -> None:
            if s.state == STATE_READY:
                s.state = STATE_DRAINING
                s.draining_since_ms = now

        try:
            await self.repo.update_shard_with_lock(
                shard.hub_pod_name, self.retry(), _mark, self.shard_ttl_sec()
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            if errcode.as_code(exc) != errcode.ErrHubNoAvailable:
                plog.get().warning("drain_mark_failed", pod=shard.hub_pod_name, err=str(exc))

        members: list[int] = []
        try:
            members = await self.repo.list_shard_members(shard.hub_pod_name)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— 对齐 Go:members 保持空,继续往下
            plog.get().warning(
                "drain_list_members_failed", pod=shard.hub_pod_name, err=str(exc)
            )
        if shard.player_count > 0 and len(members) < shard.player_count:
            plog.get().warning(
                "drain_members_index_incomplete",
                pod=shard.hub_pod_name,
                indexed=len(members),
                player_count=shard.player_count,
            )

        try:
            fresh = await self.repo.list_shards()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "drain_list_shards_failed", pod=shard.hub_pod_name, err=str(exc)
            )
            return True  # 已标 draining,搬迁留下个 tick
        try:
            fresh = await self.routable_shard_views(fresh)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "drain_authoritative_routes_failed", pod=shard.hub_pod_name, err=str(exc)
            )
            return True

        batch = self.cfg.consolidation_batch
        if batch <= 0:
            batch = DEFAULT_CONSOLIDATION_BATCH
        moved = 0
        for pid in members:
            if moved >= batch:
                break
            try:
                track = sticky_release_track(shard.release_track)
            except errcode.PandoraError as exc:
                # 脏轨道无法确定目标轨,继续搬只会把玩家甩到别的轨(§9.21)。
                plog.get().warning(
                    "drain_invalid_release_track", pod=shard.hub_pod_name, err=str(exc)
                )
                break
            target = least_loaded(fresh, shard.region, track, shard.hub_pod_name)
            if target is None:
                plog.get().warning(
                    "drain_no_target", pod=shard.hub_pod_name, region=shard.region
                )
                break  # 无空闲目标分片,留下个 tick
            if await self.migrate_player(pid, shard, target):
                moved += 1
                target.player_count += 1  # 本地快照计数同步,均衡后续选择
        plog.get().info(
            "hub_shard_draining",
            pod=shard.hub_pod_name,
            region=shard.region,
            members=len(members),
            moved=moved,
        )
        return True

    async def migrate_player(  # noqa: C901,PLR0912,PLR0915 —— 与 Go 同形状
        self, player_id: int, from_shard: Any, target: Any
    ) -> bool:
        """把单个玩家服务端权威搬迁到 `target`。对应 Go 的 `migratePlayer`。

        镜像 `TransferHub` 的「占新位 → 切归属 → 退旧位」顺序,重签票并推送
        `HubMigrateEvent`(best-effort)。返回是否搬迁成功。

        ⚠️ 这是 `replace_assignment_saga` 的**变体而非拷贝**(那边的 docstring 也这么
        写):签票在 `register_transfer_cleanup` **之后**(与 saga 相反)、CAS 输者不
        重试、resume 失败走"回加源 member 索引"而非上抛、且无写者复核。合并前须先补
        drain 侧故障注入测试。

        ★ 贯穿全函数的一条铁律:**通知未送达之前,玩家必须留在源 member 索引里**。
          源索引是 drain 扫描的唯一来源;任何失败路径若忘了把他加回去,"下个 tick
          重试"就永远不会发生 = 唯一的迁移通知永久丢失。
        """
        # 复核玩家仍在 from 分片(避免与玩家自身 Release/Transfer 竞争)。
        try:
            assign = await self.repo.get_assignment(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 读权威失败 ≠ 玩家已离开:此时删索引会让该玩家永远退出 drain 扫描
            # (§9.22 UNKNOWN 不得当 OFFLINE)。保留索引,下个 tick 重试。
            plog.get().warning(
                "drain_assignment_read_failed", player_id=player_id, err=str(exc)
            )
            return False
        if assign is None:
            await self.remove_shard_member(from_shard.hub_pod_name, player_id)
            return False
        if assign.transfer_cleanup_pending or assign.release_cleanup_pending:
            try:
                assign, still_found = await self.resume_assignment_cleanup(
                    player_id, assign.assignment_id
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "drain_owner_cleanup_resume_failed", player_id=player_id, err=str(exc)
                )
                return False
            if not still_found:
                await self.remove_shard_member(from_shard.hub_pod_name, player_id)
                return False

        if assign.hub_pod_name != from_shard.hub_pod_name:
            if target is not None and assign.hub_pod_name == target.hub_pod_name:
                # 崩溃 / 物理离场等待可能在后一个 drain tick 才把持久 cleanup 做完。
                # 此时应当**补发**那个已经选定的目标,而不是把源 member 当成陈旧索引
                # 删掉、从而丢掉唯一一次迁移通知。先刷新可能已过期的目标 reservation。
                #
                # 进入条件刻意**不**依赖"本 tick 恰好恢复了 cleanup"(上一次失败尝试
                # 可能已把 cleanup 做完、pending 位已清):凡「仍在源索引 + 归属已在
                # drain 目标」都按补发处理。对玩家自迁到同一目标的罕见崩溃残留,重复
                # 推送指向其当前精确归属,客户端契约容忍重复,索引随之收敛。
                async def _keep_scanned() -> bool:
                    await self.add_shard_member(from_shard.hub_pod_name, player_id)
                    return False

                try:
                    current, reusable = await self.assignment_routable(player_id, assign)
                except asyncio.CancelledError:
                    raise
                except BaseException:  # noqa: BLE001 —— 对齐 Go 的 `routeErr != nil`
                    return await _keep_scanned()
                if not reusable and not assignment_same_instance(assign, current):
                    return await _keep_scanned()
                try:
                    ensured = await self.ensure_existing_assignment_seat(
                        player_id, assign, current
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException:  # noqa: BLE001
                    return await _keep_scanned()
                next_rec = R.clone(assign)
                next_rec.hub_addr = ensured.hub_addr
                next_rec.shard_id = ensured.shard_id
                next_rec.region = ensured.region
                next_rec.release_track = ensured.release_track
                bind_assignment_auth(next_rec, ensured)
                try:
                    swapped = await self.repo.compare_and_swap_assignment(
                        player_id, assign, next_rec, self.assignment_saga_ttl_sec()
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException:  # noqa: BLE001
                    return await _keep_scanned()
                if not swapped:
                    return await _keep_scanned()
                sess_jti, jok = await self.migrate_resign_session_jti(player_id)
                if not jok:
                    # 会话权威不可达:回源索引,下个 tick 重扫补发(迁移已落地)。
                    return await _keep_scanned()
                try:
                    token, _exp = await self.sign_hub_ticket(
                        player_id, next_rec.role_id, next_rec, 0, sess_jti
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException:  # noqa: BLE001
                    return await _keep_scanned()  # 同上:回源索引,重扫重试
                if not await self.push_migrate(
                    player_id, from_shard, authoritative_shard(target, ensured), token
                ):
                    # 真实发布失败:回源索引,下个 tick 重签补发。
                    return await _keep_scanned()
                # 通知路径已走完(发布已确认或功能关闭),此时才清源索引,退出 drain 扫描。
                await self.remove_shard_member(from_shard.hub_pod_name, player_id)
                return True
            # 归属在其它分片(玩家自身 Release/Transfer 已带走),纯陈旧索引,清理。
            await self.remove_shard_member(from_shard.hub_pod_name, player_id)
            return False

        if self.auth_repo is not None and not assignment_binding_v2_complete(assign, player_id):
            plog.get().warning(
                "drain_migration_rejected_invalid_assignment", player_id=player_id
            )
            return False
        new_assignment_id = str(uuid.uuid4())
        try:
            seat = await self.reserve_routable_seat(
                target.hub_pod_name, player_id, new_assignment_id
            )
        except asyncio.CancelledError:
            raise
        except BaseException:  # noqa: BLE001
            return False  # 目标没位置 / 非 ready,留下个 tick 重试
        target = authoritative_shard(target, seat)
        now = L.now_ms()
        new_assign = R.clone(assign)
        new_assign.player_id = player_id
        new_assign.hub_pod_name = target.hub_pod_name
        new_assign.hub_addr = target.hub_addr
        new_assign.shard_id = target.shard_id
        new_assign.region = target.region
        new_assign.team_id = assign.team_id
        new_assign.assigned_at_ms = now
        new_assign.role_id = assign.role_id  # 选角镜像随强制整合搬迁
        new_assign.assignment_id = new_assignment_id
        new_assign.release_track = target.release_track
        bind_assignment_auth(new_assign, seat)
        if self.auth_repo is not None and not assignment_binding_v2_complete(
            new_assign, player_id
        ):
            plog.get().error(
                "migrate_assignment_missing_writer_v2_binding", player_id=player_id
            )
            await self.compensate_reserved_seat(
                target.hub_pod_name, player_id, new_assignment_id, seat
            )
            return False
        # 领来源版本(INC-20260818-003)。drain 迁移与 `replace_assignment_saga` 一样是
        # 一次**真实的归属置换**,同样必须领号 —— 漏在这里的后果是:drain 出来的
        # assignment 带 0,而该玩家水位已非零,owner 会把它按 legacy 拒掉,玩家卡在
        # 旧 Pod 上排不走。与那边的差异只有失败处置:本函数的既定风格是
        # 记日志 + 补偿 + 下个 tick 重试(不上抛)。
        try:
            migrate_revision = self.mint_source_revision()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "drain_source_revision_mint_failed",
                player_id=player_id,
                err=str(exc),
                hint="写者租约已失或号段耗尽;本轮 drain 放弃,下个 tick 重试",
            )
            await self.compensate_reserved_seat(
                target.hub_pod_name, player_id, new_assignment_id, seat
            )
            return False
        new_assign.source_revision = migrate_revision

        cleanup_registered = False
        if self.auth_repo is not None:
            try:
                await self.register_transfer_cleanup(new_assign, assign)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "drain_owner_cleanup_register_failed", player_id=player_id, err=str(exc)
                )
                await self.compensate_reserved_seat(
                    target.hub_pod_name, player_id, new_assignment_id, seat
                )
                return False
            cleanup_registered = True
        sess_jti, jok = await self.migrate_resign_session_jti(player_id)
        if not jok:
            await self.compensate_reserved_seat(
                target.hub_pod_name, player_id, new_assignment_id, seat
            )
            return False  # 会话权威不可达:fail-closed,下个 tick 重试
        try:
            prepared = await self.prepare_hub_ticket(
                player_id, assign.role_id, new_assign, 0, sess_jti
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "migrate_sign_ticket_failed", player_id=player_id, err=str(exc)
            )
            await self.compensate_reserved_seat(
                target.hub_pod_name, player_id, new_assignment_id, seat
            )
            return False
        try:
            swapped = await self.repo.compare_and_swap_assignment(
                player_id, assign, new_assign, self.assignment_saga_ttl_sec()
            )
        except asyncio.CancelledError:
            raise
        except BaseException:  # noqa: BLE001
            # CAS 结果**可能未知**。已登记 cleanup 时保留 index-first ref 与 exact
            # reservation 交重启对账 —— 盲目补偿会退掉一个可能已经生效的新 owner 的座位。
            if not cleanup_registered:
                await self.compensate_reserved_seat(
                    target.hub_pod_name, player_id, new_assignment_id, seat
                )
            return False
        if not swapped:
            if cleanup_registered:
                await self.remove_transfer_cleanup_ref(
                    assign.hub_pod_name, transfer_cleanup_ref(new_assign)
                )
            await self.compensate_reserved_seat(
                target.hub_pod_name, player_id, new_assignment_id, seat
            )
            return False
        await self.add_shard_member(target.hub_pod_name, player_id)
        if cleanup_registered:
            resume_err: BaseException | None = None
            still_found = False
            try:
                _, still_found = await self.resume_assignment_cleanup(
                    player_id, new_assignment_id
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                resume_err = exc
            if resume_err is not None or not still_found:
                plog.get().warning(
                    "drain_migration_owner_cleanup_failed",
                    player_id=player_id,
                    **{"from": from_shard.hub_pod_name},
                    to=target.hub_pod_name,
                    err=str(resume_err) if resume_err is not None else "",
                )
                # CAS 已落地但 cleanup/通知未完成;cleanup 可能已部分清掉源 member
                # 索引,重新加回保证下个 tick 仍能扫到该玩家补发通知(幂等 best-effort)。
                await self.add_shard_member(from_shard.hub_pod_name, player_id)
                return False
        else:
            await self.release_assignment_seat(assign)
            await self.remove_shard_member(from_shard.hub_pod_name, player_id)
        try:
            await self.bind_owner_for_published_hub_assignment(
                player_id, new_assign, prepared
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "migrate_owner_bind_failed", player_id=player_id, err=str(exc)
            )
            # assignment 已提交且旧 owner cleanup 已完成,不能释放 target;回加源索引让
            # 下个 tick 进入「归属已在 drain 目标」分支重签 / 重绑并补发通知。
            await self.add_shard_member(from_shard.hub_pod_name, player_id)
            return False

        # 通知仍是异步交付;发布失败就把玩家加回源 member 索引:迁移已落地,下个 tick
        # 「归属已在 drain 目标」分支会重签票据并补发通知。Login 从 durable assignment
        # 重签恢复仍是最终兜底,但不再把「发布失败」静默当作已送达。
        if not await self.push_migrate(player_id, from_shard, target, prepared.token):
            await self.add_shard_member(from_shard.hub_pod_name, player_id)
            return False
        return True

    async def push_migrate(
        self, player_id: int, from_shard: Any, target: Any, token: str
    ) -> bool:
        """推送 `HubMigrateEvent` 给被迁移玩家。对应 Go 的 `pushMigrate`。

        返回是否可视为「通知路径已走完」:

          - pusher 未装配 → `True`(功能关闭,由 drain 心跳 + Login 重签兜底);
          - **真实发布失败 → `False`**,调用方必须把玩家留在 / 加回源 member 索引,
            下个 tick 重签重发。把失败当成功会静默丢掉唯一一次迁移通知。
        """
        if self.migrate is None:
            return True
        ev = hubpb.HubMigrateEvent(
            player_id=player_id,
            from_hub_pod=from_shard.hub_pod_name,
            to_hub_ds_addr=target.hub_addr,
            to_hub_ticket=token,
            to_hub_pod_name=target.hub_pod_name,
            to_shard_id=target.shard_id,
            grace_seconds=self.cfg.migrate_grace_seconds,
            reason=MIGRATE_REASON_CONSOLIDATION,
            ts_ms=L.now_ms(),
        )
        try:
            payload = ev.SerializeToString()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— 对齐 Go 的 marshal error 分支
            plog.get().warning("migrate_marshal_failed", player_id=player_id, err=str(exc))
            return False
        try:
            await self.migrate.push_migrate(player_id, payload)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning("migrate_push_failed", player_id=player_id, err=str(exc))
            return False
        return True

    async def reclaim_drained_shards(self, shards: list) -> int:
        """**刻意不删镜像**,只在过 grace 后留痕。对应 Go 的 `reclaimDrainedShards`。

        为什么"逻辑 drain/grace 计时器到了"不构成删除授权(这正是 §16.10 的正面
        示范 —— 到期后不假设成功):

          - Fleet 缩容**不指定**删哪个 GameServer,被删的很可能不是排空的那台;
          - Kubernetes 接受 DELETE ≠ 进程已经拆机。

        在缩容升级为 exact 逐实例拆机 saga 之前,正确性优先于资源回收:物理 owner
        栅栏保持持久,本函数恒返回 0(于是调用方的缩容段永不执行)。
        """
        grace_ms = self.cfg.migrate_grace_seconds * 1000
        now = L.now_ms()
        for s in shards:
            if s.state != STATE_DRAINING or s.player_count > 0 or s.draining_since_ms <= 0:
                continue
            if now - s.draining_since_ms < grace_ms:
                continue  # 未过 grace,保持 pod 存活让在场玩家完成倒计时切换
            plog.get().warning(
                "hub_scalein_waiting_exact_instance_teardown",
                pod=s.hub_pod_name,
                region=s.region,
                gameserver_uid=s.gameserver_uid,
            )
        return 0

    # ── 成员反向索引(best-effort)────────────────────────────────────────

    async def add_shard_member(self, pod: str, player_id: int) -> None:
        """把玩家记入分片成员反向索引。对应 Go 的 `addShardMember`。

        失败仅 Warn 不阻断主流程:索引漂移只影响"强制整合能不能枚举到他",
        而 drain 心跳双通道兜底;把它写成硬失败会让一次 Redis 抖动回滚掉一笔
        **已经提交**的归属置换。
        """
        try:
            await self.repo.add_shard_member(pod, player_id, self.assignment_saga_ttl_sec())
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "add_shard_member_failed", pod=pod, player_id=player_id, err=str(exc)
            )

    async def remove_shard_member(self, pod: str, player_id: int) -> None:
        """把玩家移出分片成员反向索引。对应 Go 的 `removeShardMember`。"""
        try:
            await self.repo.remove_shard_member(pod, player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "remove_shard_member_failed", pod=pod, player_id=player_id, err=str(exc)
            )
