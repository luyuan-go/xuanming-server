// progress.go — 战斗中实时进度通道业务逻辑(实时成长,docs/design/realtime-progression.md)。
//
// 链路:DS 异步批量 ReportProgress(事实事件,seq 幂等)
//
//	→ 本文件:校验(roster / 上限 / 升序)→ 换算(怪物经验表 / 掉落白名单,DS 不可信)
//	→ repo.ApplyProgress(水位 CAS + 进度出箱同事务)
//	→ RunProgressPublisher:出箱 worker 幂等调 player.AddExperience / inventory.GrantInstances。
//
// 错误语义(DS 侧行为契约,battle.proto ReportProgress 注释):
//   - ErrUnavailable(水位竞争 / DB 瞬时)→ DS 原批重试;
//   - ErrInvalidArg / ErrUnauthorized(坏批 / 越权)→ DS 丢批告警,继续后续批;
//   - ErrInvalidState(对局已结算 / 通道关闭 / 未知事实类型)→ DS 停流,不得无限重试
//     (未知事实 = 新 DS 对旧 Go,能力不匹配是整场性质的,丢批语义会造成逐批永久丢失)。
//     停流后果:已 ACK 部分保持有效;水位>0 时结算掉落发放保持抑制(单一权威路径),
//     停流之后的拾取 / 经验不结算兜底,本场剩余实时奖励永久丢失,错误日志告警留证
//     (该场景只该出现在违反 Go 先行发布纪律时,不为违纪场景做兜底)。
package biz

import (
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/safego"
	battlev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/battle/v1"
	missionv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/mission/v1"

	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/data"
)

// 任务条件类别(取 mission proto 枚举,改编号即编译期暴露;docs/design/mission.md §5.2)。
const (
	missionCategoryKillMonster = uint32(missionv1.MissionConditionCategory_MISSION_CONDITION_CATEGORY_KILL_MONSTER)
	missionCategoryUseItem     = uint32(missionv1.MissionConditionCategory_MISSION_CONDITION_CATEGORY_USE_ITEM)
	missionCategoryPickupItem  = uint32(missionv1.MissionConditionCategory_MISSION_CONDITION_CATEGORY_PICKUP_ITEM)
)

// ExperienceGranter 把击杀经验幂等入账到 player(AddExperience,系统 RPC)。
//
// 由 RunProgressPublisher 调用:失败 → 返回 error → 进度出箱行保留下轮重试
// (at-least-once,配合 player exp_history uk 去重)。实现可为 nil:player_addr 未配
// → 经验出箱行积压不丢(地址配好重启后补发,与掉落 granter 同语义)。
type ExperienceGranter interface {
	AddExperience(ctx context.Context, playerID uint64, expDelta uint64, reason, idempotencyKey string) error
}

// SetExperienceGranter 注入 player 经验入账器(nil-safe,风格同 SetInstanceGranter)。
func (u *BattleResultUsecase) SetExperienceGranter(g ExperienceGranter) {
	u.expGranter = g
}

// MissionReporter 把战斗事实转发给 mission 服务推进任务进度
// (ReportMissionFacts 系统 RPC,docs/design/mission.md §5.1)。
//
// **弱依赖**:未注入(mission_addr 未配)时 ReportProgress 根本不产生任务出箱行 ——
// 产生了却投不出去只会让出箱表无界堆积。因此"是否转发"由本字段是否为 nil 决定,
// 发布顺序按 §9.21 Go 先行:先上 mission 服务,再给 battle_result 配地址开转发。
//
// 幂等键 progress:{match_id}:{seq}:{player_id}:mission 由调用方给定,mission 侧收据表
// (uk + 指纹)吸收 at-least-once 重放。
type MissionReporter interface {
	ReportMissionFact(ctx context.Context, playerID uint64, category, slotValue, amount uint32, idempotencyKey string) error
}

// SetMissionReporter 注入任务事实转发器(nil = 转发关闭,不产生出箱行)。
func (u *BattleResultUsecase) SetMissionReporter(r MissionReporter) {
	u.missionReporter = r
}

// MonsterExpTable 按 (怪物角色 ID, 怪物等级) 查击杀经验。
//
// 唯一实现是 configtable 的 role_level 表(源表 角色/j_角色等级.xlsx 的「击杀经验」列)。
// 抽成接口只为让 biz 不直接依赖 pkg/configtable(该包会随注册表增长牵动整个进程的启动依赖),
// 以及让换算逻辑可以脱离真实配置表跑单测。
//
// 返回 ok=false 表示**该 (角色, 等级) 在表里没有行**;调用方必须把它与"有行但经验为 0"
// 区分开:前者是配置漏项(告警),后者是策划有意不给经验(如玩家英雄行显式填 0)。
type MonsterExpTable interface {
	KillExpOf(roleID, level uint32) (uint64, bool)
}

// SetMonsterExpTable 注入怪物击杀经验表。
//
// **不是 nil-safe 的可选依赖**:未注入时 ReportProgress 对每条击杀事实一律返回可重试错误,
// 而不是"查不到 → 跳过 → 水位照常推进"。理由是后者会让整批击杀经验**永久丢失**且只留一条
// Warn —— seq 一旦推进,DS 重发会被 `seq <= lastSeq` 跳过,没有任何补救路径。
// 启动接线见 cmd/battle_result/main.go(配置表是 battle_result 的启动强依赖)。
func (u *BattleResultUsecase) SetMonsterExpTable(t MonsterExpTable) {
	u.monsterExp = t
}

// expSharePermilleFull 是「一整份经验」对应的归属权重(千分比满值)。
const expSharePermilleFull = 1000

// maxBattleItemActionCountHard 是独立于 CSV/配置错误的最终护栏。正常上限来自
// item.max_stack；硬上限只防热配置异常把单次同步事务放大到不可控规模。
const maxBattleItemActionCountHard uint32 = 1000

type progressActionRequest struct {
	Seq          uint64
	PlayerID     uint64
	Kind         data.ProgressGrantKind
	ItemConfigID uint32
	Count        uint32
}

// battleItemRejectReason 把局内 consume / discard 的「不是合法战斗道具」判定拆成枚举
// reason(§11.3 R2:一个 if 收敛的 N 个条件必须拆成 N 个 reason)。
// consumable=true 走消费口径(要求 BattleUsable),false 走丢弃口径(要求 Droppable)。
// 只作日志判据,不参与控制流。
func battleItemRejectReason(itemID uint32, configured bool, def BattleItemDefinition, consumable bool) string {
	switch {
	case itemID == 0:
		return "zero_item_config_id"
	case !configured:
		return "not_configured"
	case def.Equipment:
		return "equipment"
	case consumable && !def.BattleUsable:
		return "not_battle_usable"
	case !consumable && !def.Droppable:
		return "not_droppable"
	default:
		return "unknown"
	}
}

// progressKindName 把出箱类型渲染成稳定的 snake_case 名字(日志字段用,不参与业务判定)。
// 日志里只打数字时,排障要回来翻常量表才知道 4 是消费还是丢弃。
func progressKindName(kind data.ProgressGrantKind) string {
	switch kind {
	case data.ProgressGrantExp:
		return "grant_exp"
	case data.ProgressGrantInstance:
		return "grant_instance"
	case data.ProgressGrantStack:
		return "grant_stack"
	case data.ProgressConsumeStack:
		return "consume_stack"
	case data.ProgressDiscardStack:
		return "discard_stack"
	default:
		return "unknown"
	}
}

// isolatedProgressAction 强制 consume/discard 独占 ReportProgress 批。这样 action 的
// 业务失败不会让同一批已接受的 pickup claim 被 UE 整批释放，也不存在一个 action 被
// 拆成多个下游事务后部分成功的问题。
//
// 返回值 (action, reason, err):reason 只供调用方打日志(§11.3 R2 —— 两个拒绝条件
// 「批型不合法」与「count 越硬上限」历史上都零日志,DS 侧只看到整批 InvalidArg);
// err 与拆分前逐字节一致,控制流不变。err 非 nil 时调用方必定立即返回,不读 action。
func isolatedProgressAction(events []*battlev1.BattleProgressEvent) (*progressActionRequest, string, error) {
	var action *progressActionRequest
	for _, e := range events {
		var candidate *progressActionRequest
		switch fact := e.GetFact().(type) {
		case *battlev1.BattleProgressEvent_ItemConsume:
			candidate = &progressActionRequest{
				Seq: e.GetSeq(), PlayerID: e.GetPlayerId(), Kind: data.ProgressConsumeStack,
				ItemConfigID: fact.ItemConsume.GetItemConfigId(), Count: fact.ItemConsume.GetCount(),
			}
		case *battlev1.BattleProgressEvent_ItemDiscard:
			candidate = &progressActionRequest{
				Seq: e.GetSeq(), PlayerID: e.GetPlayerId(), Kind: data.ProgressDiscardStack,
				ItemConfigID: fact.ItemDiscard.GetItemConfigId(), Count: fact.ItemDiscard.GetCount(),
			}
		}
		if candidate != nil {
			if action != nil || len(events) != 1 {
				return candidate, "action_not_isolated", errcode.New(errcode.ErrInvalidArg,
					"consume/discard must be exactly one fact in an isolated ReportProgress batch")
			}
			action = candidate
		}
	}
	if action != nil && (action.Count == 0 || action.Count > maxBattleItemActionCountHard) {
		return action, "action_count_out_of_hard_range", errcode.New(errcode.ErrInvalidArg,
			"battle item action count %d out of hard range (max %d)", action.Count, maxBattleItemActionCountHard)
	}
	return action, "", nil
}

// applyExpShare 按千分比权重切一份经验,向下取整(拆成商余两段算,等价 total*share/1000
// 但不会在 total 很大时溢出;余项乘积上界 999*1000 远小于 uint64)。
// 向下取整意味着小额经验 × 小权重可能归零 —— 归零的份额不产出箱行(0 额度会被 player 拒收),
// 这是「均分」档刻意接受的舍入损失,不是丢账。
func applyExpShare(total, sharePermille uint64) uint64 {
	if sharePermille >= expSharePermilleFull {
		return total
	}
	return total/expSharePermilleFull*sharePermille +
		total%expSharePermilleFull*sharePermille/expSharePermilleFull
}

// ReportProgress 处理 DS 的一批进度事实事件,返回已应用水位 acked_seq。
//
// roster 是凭据检查器从权威 BattleStorageRecord 取的本场玩家名单(service 层注入);
// nil = dev / guard off 模式,跳过成员校验(生产 authority_mode=redis 恒非 nil)。
func (u *BattleResultUsecase) ReportProgress(ctx context.Context, matchID uint64, roster []uint64, events []*battlev1.BattleProgressEvent) (uint64, error) {
	if matchID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "match_id required")
	}
	if len(events) == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "events required")
	}
	if maxBatch := u.cfg.MaxProgressBatchOrDefault(); len(events) > maxBatch {
		// 批量硬上限拒收 = DS 违反上报契约 / 失陷 DS 刷量,与其它 cap 同一监控面。
		plog.With(ctx).Errorw("msg", "progress_cap_rejected",
			"match_id", matchID, "kind", "batch_size", "value", len(events), "cap", maxBatch)
		return 0, errcode.New(errcode.ErrInvalidArg, "batch size %d exceeds max %d", len(events), maxBatch)
	}
	var rosterSet map[uint64]struct{}
	if roster != nil {
		rosterSet = make(map[uint64]struct{}, len(roster))
		for _, pid := range roster {
			rosterSet[pid] = struct{}{}
		}
	}
	action, actionReason, err := isolatedProgressAction(events)
	if err != nil {
		// 整批明确拒绝(DS 据契约丢批)。历史上这两个条件零日志:UE 侧只看到 InvalidArg,
		// 后端查不到「是批型不合法还是 count 越硬上限」。
		kv := []any{"msg", "progress_batch_rejected",
			"match_id", matchID, "reason", actionReason, "events", len(events)}
		if action != nil {
			kv = append(kv, "seq", action.Seq, "player_id", action.PlayerID,
				"kind", progressKindName(action.Kind),
				"item_config_id", action.ItemConfigID, "count", action.Count,
				"hard_max", maxBattleItemActionCountHard)
		}
		kv = append(kv, "code", int32(errcode.As(err)), "err", err,
			"hint", "consume/discard 必须独占整批且 count 在硬上限内,否则整批拒收")
		plog.With(ctx).Warnw(kv...)
		return 0, err
	}
	if action != nil {
		if action.Seq == 0 || action.PlayerID == 0 {
			plog.With(ctx).Warnw("msg", "progress_action_rejected",
				"match_id", matchID, "reason", "missing_seq_or_player_id",
				"seq", action.Seq, "player_id", action.PlayerID,
				"kind", progressKindName(action.Kind), "path", "isolated_action")
			return 0, errcode.New(errcode.ErrInvalidArg, "isolated action requires seq and player_id")
		}
		if rosterSet != nil {
			if _, ok := rosterSet[action.PlayerID]; !ok {
				// 事件循环里的同一判定已经打了 progress_roster_reject,这条前置的 isolated
				// action 分支历史上没打 —— 失陷 DS 只要把越权请求做成 consume/discard 独占批
				// 就能绕开这条安全告警(§9.6 owner 授权 / roster 门)。
				plog.With(ctx).Warnw("msg", "progress_roster_reject",
					"match_id", matchID, "player_id", action.PlayerID, "seq", action.Seq,
					"kind", progressKindName(action.Kind), "path", "isolated_action")
				return 0, errcode.New(errcode.ErrUnauthorized,
					"player %d not in match %d roster", action.PlayerID, matchID)
			}
		}
	}

	wm, err := u.repo.GetProgressWatermark(ctx, matchID)
	if err != nil {
		// 读不到水位 = 本批既不能接受也不能拒绝(DS 会原批重试)。不留证时,
		// 「DS 一直重试上报进度」在本服只表现为零日志。
		plog.With(ctx).Warnw("msg", "progress_watermark_read_failed",
			"match_id", matchID, "events", len(events),
			"code", int32(errcode.As(err)), "err", err,
			"hint", "水位读失败,本批未入账;DS 原批重试")
		return 0, err
	}
	// 已接受 action 的终态回放优先于 settled/stopped 门。否则 inventory 已完成但
	// ReportProgress 回包丢失、随后对局结算时，UE 重试会只收到 InvalidState，永远
	// 无法确定是否应该扣本地/施放效果。未曾接受的 action 查不到 outcome，仍会拒绝。
	if action != nil && action.Seq <= wm.LastAppliedSeq {
		return u.completeProgressAction(ctx, matchID, *action)
	}
	if wm.Settled {
		// 结算后仍收到进度 = 僵尸 / 分区恢复 DS 迟到上报(水位表终局标记挡住,§9.22 / §9.4 fencing);
		// ErrInvalidState 是业务码不被 access log 当故障 → 显式 WARN 留证。
		plog.With(ctx).Warnw("msg", "progress_rejected_settled", "match_id", matchID, "events", len(events))
		return 0, errcode.New(errcode.ErrInvalidState, "match %d already settled, progress rejected", matchID)
	}
	if wm.Stopped {
		// 停流标记持久化(审计 P1):未知事实停流后,违纪 DS 再发只含已知事实的批
		// 也一律拒,禁止重新开流(契约:整场停流、剩余实时奖励永久丢失)。
		plog.With(ctx).Warnw("msg", "progress_rejected_stream_stopped", "match_id", matchID, "events", len(events))
		return 0, errcode.New(errcode.ErrInvalidState,
			"match %d progress stream permanently stopped, rejected", matchID)
	}
	// 每场模式以水位行存在性固化(§22 单一权威):行已存在 = 本场发放权已归实时通道,
	// killswitch 中途关闭不影响进行中对局(否则"部分实时 + 结算掉落被整体抑制"会丢奖);
	// 行不存在时由开关决定能否开流(默认关,混版发布纪律见 conf.ProgressEnabled)。
	if !wm.Existed && !u.cfg.ProgressEnabled {
		// 固化"本场 legacy 结算模式"权威事实(审计 P1:只返回错误不落标记时,对局
		// 中途重新开启配置会让同一对局**晚开流**,与已按 legacy 进行的前半场混用)。
		// 停流标记行 lastSeq=0 → 结算路径正常发放全部产出(零实时入账,无双发面)。
		// 认领必须是"无行才创建"(审计 R4 #11):滚动混版下开启副本可能在本副本读
		// 水位之后、认领之前已创建行开流,upsert 会把那条**合法已开的流**停掉——
		// 认领失败(claimed=false)= 输给并发写者,重读水位按现行状态继续。
		// 认领错误保持可重试(与未知事实路径同纪律)。
		claimed, merr := u.repo.ClaimProgressLegacy(ctx, matchID)
		if merr != nil {
			plog.With(ctx).Errorw("msg", "progress_claim_legacy_failed_retryable", "match_id", matchID, "err", merr)
			return 0, errcode.New(errcode.ErrUnavailable,
				"progress channel disabled and legacy claim persist failed, retry: %v", merr)
		}
		if claimed {
			// 一次**不可逆的每局模式定格**(写下停流标记行,本局此后永远走结算发放,
			// 中途开 killswitch 也不生效)。失败路径有 Errorw,成功路径历史上什么都不打,
			// 事后无法回答「这局为什么没走实时通道」。§11.3 R1:不可逆状态推进打 INFO。
			plog.With(ctx).Infow("msg", "progress_legacy_claimed",
				"match_id", matchID, "events", len(events), "first_seq", events[0].GetSeq(),
				"reason", "progress_disabled",
				"hint", "本场固化为 legacy 结算发放模式,实时通道对本局永久关闭")
			return 0, errcode.New(errcode.ErrInvalidState, "realtime progress channel disabled")
		}
		// 竞态输给已存在的行:可能是开启副本已开流(继续在途流程入账)、已认领
		// legacy、或已结算——重读后按与首读相同的裁决顺序处理。
		wm, err = u.repo.GetProgressWatermark(ctx, matchID)
		if err != nil {
			return 0, err
		}
		if wm.Settled {
			// 首读路径对同一裁决打了 Warn,这条**竞态重读**路径历史上没打:僵尸 DS 恰好在
			// legacy 认领竞态窗口内到达时,fencing 生效却不留证。
			plog.With(ctx).Warnw("msg", "progress_rejected_settled",
				"match_id", matchID, "events", len(events),
				"last_seq", wm.LastAppliedSeq, "path", "legacy_claim_race")
			return 0, errcode.New(errcode.ErrInvalidState, "match %d already settled, progress rejected", matchID)
		}
		if wm.Stopped {
			plog.With(ctx).Warnw("msg", "progress_rejected_stream_stopped",
				"match_id", matchID, "events", len(events),
				"last_seq", wm.LastAppliedSeq, "path", "legacy_claim_race")
			return 0, errcode.New(errcode.ErrInvalidState,
				"match %d progress stream permanently stopped, rejected", matchID)
		}
		if !wm.Existed {
			// INSERT IGNORE 没生效行却不存在(并发删除/复制异常):按瞬时态重试收敛。
			plog.With(ctx).Errorw("msg", "progress_legacy_claim_raced",
				"match_id", matchID, "events", len(events),
				"hint", "INSERT IGNORE 未生效且水位行不存在(并发删除/复制异常),按瞬时态让 DS 重试")
			return 0, errcode.New(errcode.ErrUnavailable, "progress legacy claim raced, retry")
		}
		plog.With(ctx).Warnw("msg", "progress_disabled_replica_joins_open_stream",
			"match_id", matchID, "last_seq", wm.LastAppliedSeq,
			"hint", "通道关闭副本遇到已开流对局(滚动混版/killswitch 中途关闭),按已开流继续入账")
	}
	lastSeq := wm.LastAppliedSeq

	// 事实换算(DS 不可信):怪物经验查配置表,拾取过白名单;未知怪 / 非白名单物品跳过并告警
	// (只丢该事实的发放,水位照常推进 —— 坏配置不能把整条流卡死)。
	maxSeqCap := u.cfg.MaxProgressSeqPerMatchOrDefault()
	maxKill := u.cfg.MaxKillCountPerFactOrDefault()
	maxPickup := u.cfg.MaxPickupCountPerFactOrDefault()
	expByPlayer := make(map[uint64]uint64)
	itemsByPlayer := make(map[uint64]uint32)
	killsByPlayer := make(map[uint64]uint32)
	var (
		itemRows       []data.ProgressOutboxRecord
		missionRows    []data.MissionFactRecord
		prevSeq        uint64
		newSeq         = lastSeq
		prevAppliedSeq = lastSeq
		skippedFact    int
		batchExp       uint64
		batchItems     uint32
	)
	// 任务事实转发开关:reporter 未注入(mission_addr 未配)时一行不产,避免出箱无界堆积。
	forwardMission := u.missionReporter != nil
	// pendingAction=true 的事实落库即不可投递,等局内消费扣除结果在
	// ResolveProgressAction 同事务里放行或删行(见 data.MissionFactRecord.PendingAction)。
	addMissionFact := func(seq, playerID uint64, category, slotValue, amount uint32, pendingAction bool) {
		if !forwardMission || slotValue == 0 || amount == 0 {
			return
		}
		missionRows = append(missionRows, data.MissionFactRecord{
			MatchID: matchID, Seq: seq, PlayerID: playerID,
			Category: category, SlotValue: slotValue, Amount: amount,
			PendingAction: pendingAction,
		})
	}
	// 模式 C:漏配 / 非白名单事实原先在事件循环内**逐条**打 Warn——一张漏配的怪物经验表
	// 会让「该怪每次击杀 × 每个玩家 × 每场对局」都刷一行。改为按 config_id 去重收集
	// (distinct 计数 + 样例 ID),批末汇总一条:既能定位是哪个 ID 漏配,又不随击杀数刷屏。
	unconfiguredMonsters := map[uint32]struct{}{}
	notWhitelistedItems := map[uint32]struct{}{}
	var sampleMonsterID, sampleItemID uint32
	// 受影响玩家同样要去重收集:DS 回调面没有玩家 JWT,plog.With(ctx) 不会自动带 player_id,
	// 少了它「同一局里只有我没加经验」这类投诉定位不到人 —— 而漏配是按 (角色, 等级) 行
	// 发生的,完全可能只影响打了某种怪的那一两个玩家。
	skippedPlayers := map[uint64]struct{}{}
	var sampleSkippedPlayerID uint64
	noteSkippedPlayer := func(playerID uint64) {
		if _, seen := skippedPlayers[playerID]; seen {
			return
		}
		skippedPlayers[playerID] = struct{}{}
		if sampleSkippedPlayerID == 0 {
			sampleSkippedPlayerID = playerID
		}
	}
	for _, e := range events {
		seq := e.GetSeq()
		if seq == 0 || seq <= prevSeq {
			// 整批明确拒绝(DS 据契约丢批)。不留证时,DS 侧「批被拒」与后端「为什么拒」
			// 对不上,只能从客户端反推。
			plog.With(ctx).Warnw("msg", "progress_batch_rejected",
				"match_id", matchID, "reason", "seq_not_ascending",
				"seq", seq, "prev_seq", prevSeq, "events", len(events))
			return 0, errcode.New(errcode.ErrInvalidArg, "event seq must be ascending (seq=%d prev=%d)", seq, prevSeq)
		}
		prevSeq = seq
		if seq > maxSeqCap {
			// seq 硬上限拒收也要告警(契约:拒收并告警;与累计 cap 的 progress_cap_rejected
			// 同名同监控面——失陷 DS 刷 seq 的第一现场信号)。
			plog.With(ctx).Errorw("msg", "progress_cap_rejected",
				"match_id", matchID, "kind", "seq", "seq", seq, "cap", maxSeqCap)
			return 0, errcode.New(errcode.ErrInvalidArg, "event seq %d exceeds per-match cap %d", seq, maxSeqCap)
		}
		if seq <= lastSeq {
			continue // 旧事件重放(at-least-once),已入账,跳过
		}
		if seq > prevAppliedSeq+1 {
			// seq 跳号合法(DS 有界缓冲满载丢最老事件,realtime-progression.md §3/§9),
			// 但必须留痕(批首与批内统一检测,批内 [1,3] 同样告警):跳过的 seq 永不再来,
			// 结算对账 gap 告警的先导信号。
			plog.With(ctx).Warnw("msg", "progress_seq_gap",
				"match_id", matchID, "prev_applied", prevAppliedSeq, "next", seq)
		}
		prevAppliedSeq = seq
		newSeq = seq
		playerID := e.GetPlayerId()
		if playerID == 0 {
			plog.With(ctx).Warnw("msg", "progress_batch_rejected",
				"match_id", matchID, "reason", "missing_player_id",
				"seq", seq, "events", len(events))
			return 0, errcode.New(errcode.ErrInvalidArg, "event %d missing player_id", seq)
		}
		if rosterSet != nil {
			if _, ok := rosterSet[playerID]; !ok {
				// DS 为非本场玩家上报进度事实 = 越权 / 失陷 DS 的直接信号(§9.6 owner 授权 / roster 门)。
				plog.With(ctx).Warnw("msg", "progress_roster_reject",
					"match_id", matchID, "player_id", playerID, "seq", seq, "path", "event")
				return 0, errcode.New(errcode.ErrUnauthorized, "player %d not in match %d roster", playerID, matchID)
			}
		}
		switch fact := e.GetFact().(type) {
		case *battlev1.BattleProgressEvent_MonsterKill:
			cnt := fact.MonsterKill.GetCount()
			if cnt == 0 || cnt > maxKill {
				// 兄弟闸门(seq 硬上限 / 累计上限)都以 progress_cap_rejected 打 Error,
				// 单事实计数上限历史上静默 —— 失陷 DS 刷单条巨量击杀是最直接的作弊形态。
				plog.With(ctx).Errorw("msg", "progress_cap_rejected",
					"match_id", matchID, "player_id", playerID, "seq", seq,
					"kind", "kill_count", "value", cnt, "cap", maxKill,
					"monster_config_id", fact.MonsterKill.GetMonsterConfigId())
				return 0, errcode.New(errcode.ErrInvalidArg, "kill count %d out of range (max %d)", cnt, maxKill)
			}
			// 击杀计数在经验换算前累计:未配置经验的怪也计入单玩家击杀上限,
			// 失陷 DS 不能靠刷未知怪 ID 绕过反作弊额度。
			killsByPlayer[playerID] += cnt
			// 任务事实同样在经验换算前收集:「这只怪被杀了」与「这只怪配没配经验」是两件事,
			// role_level 漏配不该让杀怪类任务跟着不计数。未被任何任务条件引用的怪物 ID
			// 到 mission 侧自然匹配不上,不构成放行面。
			addMissionFact(seq, playerID, missionCategoryKillMonster,
				fact.MonsterKill.GetMonsterConfigId(), cnt, false)
			// 归属权重(千分比):0 / 未设置 = 整份。旧 DS 不带本字段,「最后一击」「全队共享」
			// 两档也恒 1000,只有「伤害占比均分」会给出小于 1000 的值(battle.proto 同款注释)。
			// >1000 = 坏批(单条事实拿到超过一整份),按 ErrInvalidArg 让 DS 丢批告警。
			share := uint64(fact.MonsterKill.GetSharePermille())
			if share == 0 {
				share = expSharePermilleFull
			}
			if share > expSharePermilleFull {
				plog.With(ctx).Errorw("msg", "progress_cap_rejected",
					"match_id", matchID, "player_id", playerID, "seq", seq,
					"kind", "share_permille", "value", share, "cap", expSharePermilleFull,
					"hint", "单条事实拿到超过一整份归属权重 = 坏批 / 失陷 DS")
				return 0, errcode.New(errcode.ErrInvalidArg,
					"share_permille %d exceeds %d (seq=%d)", share, expSharePermilleFull, seq)
			}
			// 经验查表:(怪物角色 ID, 怪物等级) → 整份经验。等级 0 = 1 级(见 battle.proto)。
			// 表未注入时**不能**走"跳过并推进水位"那条路 —— 水位一推进,这批击杀的经验
			// 就永久没有补救路径了(DS 重发会被 seq<=lastSeq 跳过)。按可重试错误整批退回。
			if u.monsterExp == nil {
				plog.With(ctx).Errorw("msg", "monster_exp_table_unavailable",
					"match_id", matchID, "seq", seq,
					"hint", "configtable role_level 未注入;经验无法换算,整批退回等待重试(绝不推进水位)")
				return 0, errcode.New(errcode.ErrUnavailable,
					"monster exp table unavailable, retry batch (seq=%d)", seq)
			}
			monsterID := fact.MonsterKill.GetMonsterConfigId()
			expPer, ok := u.monsterExp.KillExpOf(monsterID, fact.MonsterKill.GetMonsterLevel())
			if !ok {
				// 表里没有这一 (角色, 等级) 行 = 配置漏项。只丢该事实的发放,水位照常推进:
				// 坏配置不能把整条流卡死(与非白名单拾取同一纪律)。
				skippedFact++
				noteSkippedPlayer(playerID)
				if _, seen := unconfiguredMonsters[monsterID]; !seen {
					unconfiguredMonsters[monsterID] = struct{}{}
					if sampleMonsterID == 0 {
						sampleMonsterID = monsterID
					}
				}
				continue
			}
			gained := applyExpShare(expPer*uint64(cnt), share)
			expByPlayer[playerID] += gained
			batchExp += gained
		case *battlev1.BattleProgressEvent_ItemPickup:
			cnt := fact.ItemPickup.GetCount()
			if cnt == 0 || cnt > maxPickup {
				plog.With(ctx).Errorw("msg", "progress_cap_rejected",
					"match_id", matchID, "player_id", playerID, "seq", seq,
					"kind", "pickup_count", "value", cnt, "cap", maxPickup,
					"item_config_id", fact.ItemPickup.GetItemConfigId())
				return 0, errcode.New(errcode.ErrInvalidArg, "pickup count %d out of range (max %d)", cnt, maxPickup)
			}
			itemID := fact.ItemPickup.GetItemConfigId()
			def, configured := u.battleItemDefinition(itemID)
			if itemID == 0 || !configured || !def.Droppable {
				skippedFact++
				noteSkippedPlayer(playerID)
				if _, seen := notWhitelistedItems[itemID]; !seen {
					notWhitelistedItems[itemID] = struct{}{}
					if sampleItemID == 0 {
						sampleItemID = itemID
					}
				}
				continue
			}
			// 每拾取事实一行出箱(Seq=事实自身 seq,uk 天然唯一):单事实 count 已被
			// 夹紧到 CSV 列宽内,合法掉落永不截断(审计 P1;拾取低频,行数有界)。
			items := make([]uint32, cnt)
			for i := range items {
				items[i] = itemID
			}
			kind := data.ProgressGrantStack
			if def.Equipment {
				kind = data.ProgressGrantInstance
			}
			itemRows = append(itemRows, data.ProgressOutboxRecord{
				MatchID: matchID, Seq: seq, PlayerID: playerID,
				Kind: kind, ItemConfigIDs: items,
			})
			batchItems += cnt
			itemsByPlayer[playerID] += cnt
			// 拾取类任务事实只收白名单内的:非白名单拾取本就被判为可疑事实(上面已 skip
			// 掉发放),放它推进任务进度等于另开一条绕过白名单的计数通道。
			// 不挂 pending 闸:拾取没有 battle_progress_action 结果行可等(只有
			// consume/discard 有),且"捡到了"本身就是 DS 记录的事实,发放失败属投递问题
			// (满包转邮件 / 退避重试),不是"这件事没发生"。
			addMissionFact(seq, playerID, missionCategoryPickupItem, itemID, cnt, false)
		case *battlev1.BattleProgressEvent_ItemConsume:
			cnt := fact.ItemConsume.GetCount()
			itemID := fact.ItemConsume.GetItemConfigId()
			def, configured := u.battleItemDefinition(itemID)
			if itemID == 0 || !configured || def.Equipment || !def.BattleUsable {
				// 不能跳过并 ACK：DS 会据 ACK 最终扣本地并应用 GAS，后端若未留下
				// 消费出箱会让资产重登复活。坏事实必须整批明确拒绝。
				//
				// 这是**整批明确拒绝**(不是跳过):DS 据契约丢批,本场该玩家后续道具行为
				// 全部作废。漏配 battle_usable 列会让某个道具全服不可用,零日志时只能
				// 从客户端「用不了」反推。reason 必须拆开(§11.3 R2)。
				plog.With(ctx).Warnw("msg", "progress_item_action_rejected",
					"match_id", matchID, "player_id", playerID, "seq", seq, "kind", "consume",
					"item_config_id", itemID, "count", cnt,
					"reason", battleItemRejectReason(itemID, configured, def, true))
				return 0, errcode.New(errcode.ErrInvalidArg,
					"item %d is not configured battle consumable", itemID)
			}
			if cnt == 0 || def.MaxStack == 0 || cnt > def.MaxStack || cnt > maxBattleItemActionCountHard {
				plog.With(ctx).Warnw("msg", "progress_item_action_rejected",
					"match_id", matchID, "player_id", playerID, "seq", seq, "kind", "consume",
					"item_config_id", itemID, "count", cnt, "max_stack", def.MaxStack,
					"hard_max", maxBattleItemActionCountHard, "reason", "count_out_of_range")
				return 0, errcode.New(errcode.ErrInvalidArg,
					"consume count %d out of range for item %d (max_stack %d, hard max %d)",
					cnt, itemID, def.MaxStack, maxBattleItemActionCountHard)
			}
			itemRows = append(itemRows, data.ProgressOutboxRecord{
				MatchID: matchID, Seq: seq, PlayerID: playerID,
				Kind: data.ProgressConsumeStack, ItemConfigIDs: []uint32{itemID}, ItemCount: cnt,
			})
			// 「使用道具」类任务事实。丢弃(ItemDiscard)刻意不转发:扔掉不是用掉,
			// 否则「使用 N 个 X」型任务能靠捡了再扔刷完。
			// pendingAction=true:这一行**在扣除落定前不可投递**。局内消费可能以业务失败
			// 终态收场(道具不足等),此时 inventory 一件没扣、UE 也保留本地物品 ——
			// 事实照发就等于让"上报根本没发生的消耗"刷完「使用 N 个 X」(§9.6 不信 DS)。
			// 放行 / 删行由 ResolveProgressAction 在扣除结果同一事务里完成。
			addMissionFact(seq, playerID, missionCategoryUseItem, itemID, cnt, true)
		case *battlev1.BattleProgressEvent_ItemDiscard:
			cnt := fact.ItemDiscard.GetCount()
			itemID := fact.ItemDiscard.GetItemConfigId()
			def, configured := u.battleItemDefinition(itemID)
			if itemID == 0 || !configured || def.Equipment || !def.Droppable {
				plog.With(ctx).Warnw("msg", "progress_item_action_rejected",
					"match_id", matchID, "player_id", playerID, "seq", seq, "kind", "discard",
					"item_config_id", itemID, "count", cnt,
					"reason", battleItemRejectReason(itemID, configured, def, false))
				return 0, errcode.New(errcode.ErrInvalidArg,
					"battle discard only supports configured droppable stackable item: %d", itemID)
			}
			if cnt == 0 || def.MaxStack == 0 || cnt > def.MaxStack || cnt > maxBattleItemActionCountHard {
				plog.With(ctx).Warnw("msg", "progress_item_action_rejected",
					"match_id", matchID, "player_id", playerID, "seq", seq, "kind", "discard",
					"item_config_id", itemID, "count", cnt, "max_stack", def.MaxStack,
					"hard_max", maxBattleItemActionCountHard, "reason", "count_out_of_range")
				return 0, errcode.New(errcode.ErrInvalidArg,
					"discard count %d out of range for item %d (max_stack %d, hard max %d)",
					cnt, itemID, def.MaxStack, maxBattleItemActionCountHard)
			}
			itemRows = append(itemRows, data.ProgressOutboxRecord{
				MatchID: matchID, Seq: seq, PlayerID: playerID,
				Kind: data.ProgressDiscardStack, ItemConfigIDs: []uint32{itemID}, ItemCount: cnt,
			})
		default:
			// 未知事实类型 = 能力不匹配(新 DS 对旧 Go),是整场性质而非单批坏数据:
			// ErrInvalidArg 的"丢批继续"语义会让 DS 逐批丢弃所有含新事实的批(永久丢失,
			// 审计 P1)。改用 ErrInvalidState → DS 停流。停流后果(审计明示,proto 同步):
			// 水位>0 时结算掉落发放保持抑制,停流之后的拾取 / 经验不结算兜底,本场剩余
			// 实时奖励永久丢失——该场景只该出现在违反发布纪律时,不为违纪场景做兜底。
			// 新 DS 携带新事实类型必须先升级 battle_result 全 fleet 再放量
			// (与 conf.ProgressEnabled 混版纪律同向:Go 先行,DS 后行)。
			// 停流标记持久化:防违纪 DS 用后续"只含已知事实"的批重新开流(审计 P1)。
			// 标记失败必须**保持可重试**(审计 P1:吞掉失败直接返回终态 InvalidState 会让
			// DS 永久停流而库里没有标记,后续已知批仍可能被接受)——返回 ErrUnavailable,
			// DS 原批重试 → 再次命中未知事实 → 重试落标记,收敛后才返回停流终态。
			// "已停流"日志只在标记成功后打(审计 R4 P2:先打日志再落标记,标记失败时
			// 日志与库状态矛盾,排障会按已停流处理)。
			if merr := u.repo.MarkProgressStopped(ctx, matchID); merr != nil {
				plog.With(ctx).Errorw("msg", "progress_mark_stopped_failed_retryable", "match_id", matchID, "err", merr)
				return 0, errcode.New(errcode.ErrUnavailable,
					"unknown progress fact seq=%d and stop marker persist failed, retry batch: %v", seq, merr)
			}
			plog.With(ctx).Errorw("msg", "progress_unknown_fact_stream_stopped",
				"match_id", matchID, "seq", seq,
				"hint", "新 DS 事实类型早于 battle_result 升级放量(违反 Go 先行纪律),本场已停流,停流后实时奖励永久丢失")
			return 0, errcode.New(errcode.ErrInvalidState, "unknown progress fact seq=%d: upgrade battle_result fleet before enabling new DS fact types (stream stopped; remaining realtime rewards for this match are permanently lost)", seq)
		}
	}

	// 漏配 / 非白名单汇总(模式 C):按 config_id 去重后每批至多一条。distinct 计数暴露
	// 「漏了几个 ID」,样例 ID 给出排查落点;非白名单还可能是失陷 DS 在刷拾取,故保持 Warn。
	if len(unconfiguredMonsters) > 0 || len(notWhitelistedItems) > 0 {
		plog.With(ctx).Warnw("msg", "progress_facts_skipped",
			"match_id", matchID, "skipped_facts", skippedFact,
			"affected_player_ids", len(skippedPlayers), "sample_player_id", sampleSkippedPlayerID,
			"unconfigured_monster_ids", len(unconfiguredMonsters), "sample_monster_config_id", sampleMonsterID,
			"not_whitelisted_item_ids", len(notWhitelistedItems), "sample_item_config_id", sampleItemID,
			"hint", "怪物经验表漏配 / 掉落白名单漏项,或失陷 DS 上报未授权事实(该玩家这几笔经验/掉落永久不发)")
	}

	if newSeq == lastSeq {
		// 普通旧事件是纯重放；action 必须从 durable outcome 稳定回放，不能从
		// “outbox 已不存在”猜成功。
		if action != nil {
			return u.completeProgressAction(ctx, matchID, *action)
		}
		return lastSeq, nil
	}

	// 单场 / 单玩家累计上限统一在 ApplyProgress 事务内的一致快照上判定(审计 P1:
	// 此处若按事务外读到的 wm / player totals 先判,与水位 CAS 分属不同快照——重试
	// 请求可能读到旧水位 + 首请求已提交的新累计,把同批 delta 重复计入后永久误拒,
	// DS 据契约丢批并释放拾取认领,而首请求出箱已提交 → 重新拾取可重复发放)。
	// 这里只聚合本批 delta:expByPlayer 键集 ⊆ killsByPlayer 键集(经验只源自击杀),
	// 触达玩家 = 击杀 ∪ 拾取。
	touched := make(map[uint64]struct{}, len(killsByPlayer)+len(itemsByPlayer))
	for pid := range killsByPlayer {
		touched[pid] = struct{}{}
	}
	for pid := range itemsByPlayer {
		touched[pid] = struct{}{}
	}
	playerDeltas := make([]data.ProgressPlayerDelta, 0, len(touched))
	for pid := range touched {
		playerDeltas = append(playerDeltas, data.ProgressPlayerDelta{
			PlayerID: pid, Exp: expByPlayer[pid], Items: itemsByPlayer[pid], Kills: killsByPlayer[pid],
		})
	}
	sort.Slice(playerDeltas, func(i, j int) bool { return playerDeltas[i].PlayerID < playerDeltas[j].PlayerID })
	caps := data.ProgressCaps{
		MatchExp:    u.cfg.MaxProgressExpPerMatchOrDefault(),
		MatchItems:  u.cfg.MaxProgressItemsPerMatchOrDefault(),
		PlayerExp:   u.cfg.MaxProgressExpPerPlayerOrDefault(),
		PlayerItems: u.cfg.MaxProgressItemsPerPlayerOrDefault(),
		PlayerKills: u.cfg.MaxProgressKillsPerPlayerOrDefault(),
	}

	rows := make([]data.ProgressOutboxRecord, 0, len(expByPlayer)+len(itemRows))
	for playerID, exp := range expByPlayer {
		if exp == 0 {
			continue // monster_exp 显式配 0(无经验怪):不产生 0 额度出箱行(player 拒收会永久重试)
		}
		rows = append(rows, data.ProgressOutboxRecord{
			MatchID: matchID, Seq: newSeq, PlayerID: playerID,
			Kind: data.ProgressGrantExp, ExpDelta: exp,
		})
	}
	rows = append(rows, itemRows...)

	if err := u.repo.ApplyProgress(ctx, matchID, lastSeq, newSeq, batchExp, batchItems, playerDeltas, rows, missionRows, caps); err != nil {
		if errcode.As(err) == errcode.ErrInvalidArg {
			// 累计上限拒收告警(契约:拒收**并告警**,审计 P2):这是失陷 DS 刷产出的
			// 第一现场信号,不能只静默返回业务错误。biz 层 InvalidArg 前置校验都在
			// ApplyProgress 之前,此处 InvalidArg 只来自事务内单场/单玩家累计上限。
			plog.With(ctx).Errorw("msg", "progress_cap_rejected",
				"match_id", matchID, "batch_exp", batchExp, "batch_items", batchItems,
				"players", len(playerDeltas), "err", err)
		} else {
			// 水位 CAS 竞争 / 事务瞬时失败(ErrUnavailable 等)。DS 会原批重试,
			// 反复失败时本服历史上一条日志都没有 —— 「进度一直不入账」查不到原因。
			plog.With(ctx).Warnw("msg", "progress_apply_failed",
				"match_id", matchID, "expected_seq", lastSeq, "new_seq", newSeq,
				"events", len(events), "grant_rows", len(rows), "mission_rows", len(missionRows),
				"batch_exp", batchExp, "batch_items", batchItems, "players", len(playerDeltas),
				"code", int32(errcode.As(err)), "err", err,
				"hint", "水位 CAS 失败 / 已结算 / DB 瞬时错误,本批整体回滚;DS 原批重试")
		}
		return 0, err
	}
	plog.With(ctx).Debugw("msg", "battle_progress_applied",
		"match_id", matchID, "acked_seq", newSeq, "events", len(events),
		"grant_rows", len(rows), "skipped_facts", skippedFact,
		"batch_exp", batchExp, "batch_items", batchItems)
	if action != nil {
		return u.completeProgressAction(ctx, matchID, *action)
	}
	return newSeq, nil
}

// ReconcileProgress 结算后对账:DS 上报的 final_progress_seq vs 服务端已应用水位。
// 缺口只告警不自动补(尾窗丢失,realtime-progression.md §9 明示的残余风险)。
func reconcileProgress(ctx context.Context, matchID, finalSeq uint64, info data.ProgressSettleInfo) {
	switch {
	case finalSeq == 0 && !info.StreamExisted:
		return // 本场未走实时通道(旧 DS / 通道关),无需对账
	case finalSeq > 0 && !info.StreamExisted:
		plog.With(ctx).Warnw("msg", "progress_reconcile_stream_missing",
			"match_id", matchID, "final_seq", finalSeq,
			"hint", "DS 声称走了实时通道但服务端无水位(全部批次丢失或伪造 final_seq)")
	case finalSeq != info.LastAppliedSeq:
		plog.With(ctx).Warnw("msg", "progress_reconcile_gap",
			"match_id", matchID, "final_seq", finalSeq, "applied_seq", info.LastAppliedSeq,
			"hint", "尾窗事件丢失(DS 崩溃/网络),只告警不自动补(realtime-progression.md §9)")
	default:
		plog.With(ctx).Debugw("msg", "progress_reconcile_ok",
			"match_id", matchID, "applied_seq", info.LastAppliedSeq)
	}
}

// completeProgressAction 同步驱动该玩家到 action seq 为止的出箱，并只从
// battle_progress_action 回放终态。普通 pickup/exp 仍是异步 ACK；只有 isolated
// consume/discard 会走这里等待 inventory 事务明确完成。
func (u *BattleResultUsecase) completeProgressAction(ctx context.Context, matchID uint64, req progressActionRequest) (uint64, error) {
	for {
		action, found, err := u.repo.GetProgressAction(ctx, matchID, req.Seq, req.PlayerID, req.Kind)
		if err != nil {
			// 读不到 durable outcome:UE 拿不到确定结果,会同 seq 同 payload 重试。
			plog.With(ctx).Warnw("msg", "progress_action_lookup_failed",
				"match_id", matchID, "seq", req.Seq, "player_id", req.PlayerID,
				"kind", progressKindName(req.Kind), "path", "outcome_read",
				"code", int32(errcode.As(err)), "err", err)
			return 0, err
		}
		if !found {
			plog.With(ctx).Warnw("msg", "progress_action_rejected",
				"match_id", matchID, "seq", req.Seq, "player_id", req.PlayerID,
				"kind", progressKindName(req.Kind), "item_config_id", req.ItemConfigID,
				"count", req.Count, "reason", "outcome_row_missing",
				"hint", "该 seq 从未被接受过(重放/伪造),或 battle_progress_action 行被清理")
			return 0, errcode.New(errcode.ErrInvalidState,
				"progress action outcome missing match=%d seq=%d player=%d kind=%d",
				matchID, req.Seq, req.PlayerID, req.Kind)
		}
		if action.ItemConfigID != req.ItemConfigID || action.Count != req.Count {
			plog.With(ctx).Warnw("msg", "progress_action_rejected",
				"match_id", matchID, "seq", req.Seq, "player_id", req.PlayerID,
				"kind", progressKindName(req.Kind), "reason", "seq_payload_mismatch",
				"stored_item_config_id", action.ItemConfigID, "stored_count", action.Count,
				"item_config_id", req.ItemConfigID, "count", req.Count,
				"hint", "同一 seq 被换了载荷重放 = DS 违约 / 失陷信号")
			return 0, errcode.New(errcode.ErrInvalidArg,
				"progress action seq reused with different payload match=%d seq=%d stored=%d:%d request=%d:%d",
				matchID, req.Seq, action.ItemConfigID, action.Count, req.ItemConfigID, req.Count)
		}
		switch action.Status {
		case data.ProgressActionSucceeded:
			// 局内消费/丢弃的**不可逆终态**(inventory 已实扣)。completeProgressAction
			// 整个函数历史上一条日志都没有,「战斗里用了药到底扣没扣」只能查
			// battle_progress_action 表。每玩家每 seq 至多一条,不是高频路径。
			plog.With(ctx).Infow("msg", "progress_action_completed",
				"match_id", matchID, "seq", req.Seq, "player_id", req.PlayerID,
				"kind", progressKindName(req.Kind), "item_config_id", req.ItemConfigID,
				"count", req.Count, "status", "succeeded")
			return req.Seq, nil
		case data.ProgressActionFailed:
			// UE 的 mutation claim 只需区分“确定拒绝”与“可重试”。内部保留
			// inventory 原始 result_code 供审计；线协议统一映射 InvalidArg，避免
			// 客户端把陌生的 701x 业务码误当瞬时失败无限重试。
			//
			// inventory 的确定拒绝码只存在于返回给 DS 的错误串里,日志侧被 access log
			// 记成 rpc_ok(DEBUG) 丢弃 —— 不在这里打就永远查不到「为什么用药没生效」。
			plog.With(ctx).Warnw("msg", "progress_action_terminal_failed",
				"match_id", matchID, "seq", req.Seq, "player_id", req.PlayerID,
				"kind", progressKindName(req.Kind), "item_config_id", req.ItemConfigID,
				"count", req.Count, "inventory_code", int32(action.ResultCode),
				"hint", "inventory 明确拒绝(道具不足等),一件没扣;UE 保留本地物品并释放 claim")
			return req.Seq, errcode.New(errcode.ErrInvalidArg,
				"battle item action terminally failed match=%d seq=%d player=%d item=%d count=%d inventory_code=%d",
				matchID, req.Seq, req.PlayerID, req.ItemConfigID, req.Count, action.ResultCode)
		}

		row, ok, err := u.repo.FetchProgressOutboxForPlayer(ctx, matchID, req.PlayerID, req.Seq)
		if err != nil {
			plog.With(ctx).Warnw("msg", "progress_action_lookup_failed",
				"match_id", matchID, "seq", req.Seq, "player_id", req.PlayerID,
				"kind", progressKindName(req.Kind), "path", "outbox_read",
				"code", int32(errcode.As(err)), "err", err)
			return 0, err
		}
		if !ok {
			// Missing outbox is never evidence of success. A concurrent worker may have
			// just resolved it, so re-read once on the next loop; if still pending this
			// invariant violation remains retryable and is never ACKed.
			action, found, err = u.repo.GetProgressAction(ctx, matchID, req.Seq, req.PlayerID, req.Kind)
			if err != nil {
				plog.With(ctx).Warnw("msg", "progress_action_lookup_failed",
					"match_id", matchID, "seq", req.Seq, "player_id", req.PlayerID,
					"kind", progressKindName(req.Kind), "path", "outcome_reread",
					"code", int32(errcode.As(err)), "err", err)
				return 0, err
			}
			if found && action.Status != data.ProgressActionPending {
				continue
			}
			plog.With(ctx).Warnw("msg", "progress_action_rejected",
				"match_id", matchID, "seq", req.Seq, "player_id", req.PlayerID,
				"kind", progressKindName(req.Kind), "reason", "pending_without_outbox",
				"hint", "pending action 找不到出箱行(不变量破坏),按可重试返回,绝不 ACK")
			return 0, errcode.New(errcode.ErrUnavailable,
				"pending progress action has no outbox match=%d seq=%d; retry", matchID, req.Seq)
		}
		if _, err := u.processProgressRecord(ctx, row); err != nil {
			u.deferProgressRow(ctx, row.ID)
			return 0, err
		}
	}
}

// ── 进度发放事务出箱发布器(at-least-once + 下游幂等)──────────────────────────

// RunProgressPublisher 启动后台进度出箱发放循环,直到 ctx 取消。
//
// 纪律同 RunDropPublisher:单行失败仅记录并 continue(保留出箱行下轮重试),不阻塞其他行;
// 对应下游客户端未注入(player_addr / inventory_addr 未配)的行原样跳过积压不丢。
// 背包满 + 已配 mail → 溢出转个人邮件(同键去重,直发链与邮件链至多一次)。
func (u *BattleResultUsecase) RunProgressPublisher(ctx context.Context) {
	if u.expGranter == nil && u.granter == nil {
		plog.With(ctx).Infow("msg", "progress_publisher_disabled",
			"hint", "player_addr / inventory_addr 均未配置 → 进度出箱积压不丢,配置后重启补发")
		return
	}
	interval := u.cfg.ProgressPublishIntervalOrDefault()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	plog.With(ctx).Infow("msg", "progress_publisher_started",
		"interval", interval.String(), "batch", u.cfg.ProgressBatchSizeOrDefault())
	for {
		select {
		case <-ctx.Done():
			plog.With(ctx).Infow("msg", "progress_publisher_stopped")
			return
		case <-ticker.C:
			safego.Run(ctx, "battle_progress_publisher", func() {
				if n, err := u.publishProgressBatch(ctx); err != nil {
					plog.With(ctx).Warnw("msg", "progress_publish_batch_failed", "resolved", n, "err", err)
				}
			})
		}
	}
}

// publishProgressBatch 取一批进度出箱行处理,返回本轮成功发放或落定 action 终态的条数。
// 单行失败 → deferRow 指数退避推迟(行不丢,坏行不会长期占满首批饿死后续正常行)。
func (u *BattleResultUsecase) publishProgressBatch(ctx context.Context) (int, error) {
	recs, err := u.repo.FetchProgressOutbox(ctx, u.cfg.ProgressBatchSizeOrDefault())
	if err != nil {
		return 0, err
	}
	processed := 0
	for _, r := range recs {
		rowCtx := withOutboxTrace(ctx)
		rowStartedAt := time.Now()
		if _, err := u.processProgressRecord(rowCtx, r); err != nil {
			plog.With(rowCtx).Warnw("msg", "progress_outbox_delivery_failed",
				"id", r.ID, "match_id", r.MatchID, "seq", r.Seq, "player_id", r.PlayerID,
				"kind", progressKindName(r.Kind), "exp_delta", r.ExpDelta,
				"items", len(r.ItemConfigIDs), "item_count", r.ItemCount,
				"elapsed_ms", time.Since(rowStartedAt).Milliseconds(),
				"code", int32(errcode.As(err)), "err", err,
				"hint", "出箱行退避后下轮重试(at-least-once);持续失败查 player / inventory 侧同 trace_id")
			u.deferProgressRow(rowCtx, r.ID)
			continue
		}
		processed++
	}
	if processed > 0 {
		plog.With(ctx).Debugw("msg", "progress_outbox_resolved", "count", processed)
	}
	return processed, nil
}

// processProgressRecord 是后台 publisher 与同步 action 路径共用的唯一投递语义。
// action 的终态（成功或业务失败）由 repo 原子写 outcome + 删除 outbox；普通行成功后
// 直接删 outbox。返回的 action 非 nil 表示已持久进入终态。
func (u *BattleResultUsecase) processProgressRecord(ctx context.Context, r data.ProgressOutboxRecord) (*data.ProgressAction, error) {
	// idempotencyKey 只多留一个变量供成功台账打印:经验/物品是资产变更,
	// 失败有逐行日志而成功只有一个聚合 count 时,「玩家 X 在对局 Y 到底拿没拿到」
	// 即使把 LOG_LEVEL 调到 debug 也回答不了,只能反查 player / inventory 两个服务。
	var idempotencyKey string
	switch r.Kind {
	case data.ProgressGrantExp:
		if u.expGranter == nil {
			return nil, errcode.New(errcode.ErrUnavailable, "player_addr not configured")
		}
		key := progressIdempotencyKey(r.MatchID, r.Seq, r.PlayerID, "exp")
		idempotencyKey = key
		if err := u.expGranter.AddExperience(ctx, r.PlayerID, r.ExpDelta, "monster_kill", key); err != nil {
			return nil, err
		}
	case data.ProgressGrantInstance:
		if u.granter == nil {
			return nil, errcode.New(errcode.ErrUnavailable, "inventory_addr not configured")
		}
		key := progressIdempotencyKey(r.MatchID, r.Seq, r.PlayerID, "item")
		idempotencyKey = key
		if err := u.granter.GrantInstances(ctx, r.PlayerID, r.ItemConfigIDs, key); err != nil {
			if u.mailSender == nil || errcode.As(err) != errcode.ErrInventoryCapacityFull {
				return nil, err
			}
			if err := u.mailSender.SendOverflowMail(ctx, r.PlayerID, r.ItemConfigIDs, key); err != nil {
				return nil, err
			}
			plog.With(ctx).Infow("msg", "progress_grant_overflow_mailed",
				"match_id", r.MatchID, "seq", r.Seq, "player_id", r.PlayerID, "outbox_id", r.ID,
				"items", len(r.ItemConfigIDs), "idempotency_key", key,
				"hint", "背包满 → 装备掉落转个人邮件(同幂等键去重,直发链与邮件链至多一次)")
		}
	case data.ProgressGrantStack:
		if u.granter == nil {
			return nil, errcode.New(errcode.ErrUnavailable, "inventory_addr not configured")
		}
		stacks, err := aggregateStackGrants(r.ItemConfigIDs)
		if err != nil {
			return nil, err
		}
		key := progressIdempotencyKey(r.MatchID, r.Seq, r.PlayerID, "stack")
		idempotencyKey = key
		// 实时进度通道只发道具,金币走结算出箱(战斗中不即时到账)。
		if err := u.granter.GrantItems(ctx, r.PlayerID, stacks, 0, key); err != nil {
			return nil, err
		}
	case data.ProgressConsumeStack, data.ProgressDiscardStack:
		if u.granter == nil {
			return nil, errcode.New(errcode.ErrUnavailable, "inventory_addr not configured")
		}
		itemID, count, err := singleStackFact(r)
		if err != nil {
			return nil, err
		}
		kindName := "consume"
		if r.Kind == data.ProgressDiscardStack {
			kindName = "discard"
		}
		key := progressIdempotencyKey(r.MatchID, r.Seq, r.PlayerID, kindName)
		if r.Kind == data.ProgressConsumeStack {
			err = u.granter.ConsumeBattleItem(ctx, r.PlayerID, itemID, count, key)
		} else {
			err = u.granter.DiscardBattleItem(ctx, r.PlayerID, itemID, count, key)
		}
		if err != nil && isRetryableProgressActionError(errcode.As(err)) {
			return nil, err
		}
		resultCode := errcode.OK
		if err != nil {
			resultCode = errcode.As(err)
		}
		resolved, err := u.repo.ResolveProgressAction(ctx, r, resultCode)
		if err != nil {
			return nil, err
		}
		// 局内扣减的持久终态已落定(出箱行同事务删除)。这是资产变更的逐玩家台账,
		// 出箱行随即消失,不留痕就只能反查 inventory。
		plog.With(ctx).Infow("msg", "progress_action_resolved",
			"match_id", r.MatchID, "seq", r.Seq, "player_id", r.PlayerID, "outbox_id", r.ID,
			"kind", progressKindName(r.Kind), "item_config_id", itemID, "count", count,
			"status", progressActionStatusName(resolved.Status),
			"inventory_code", int32(resultCode), "idempotency_key", key)
		return &resolved, nil
	default:
		plog.With(ctx).Errorw("msg", "progress_outbox_unknown_kind",
			"outbox_id", r.ID, "match_id", r.MatchID, "seq", r.Seq, "player_id", r.PlayerID,
			"kind", uint8(r.Kind),
			"hint", "出箱行类型本进程不认识(旧副本读到新版本写的行),行保留退避重试")
		return nil, errcode.New(errcode.ErrInvalidState,
			"unknown progress outbox kind id=%d kind=%d", r.ID, r.Kind)
	}
	if err := u.repo.DeleteProgressOutbox(ctx, r.ID); err != nil {
		return nil, err
	}
	// 落在 DELETE 之后:出箱行删掉才算这笔发放彻底闭环(先打日志再删,删失败会让
	// 日志与库状态矛盾)。每玩家每 seq 至多一条。
	plog.With(ctx).Infow("msg", "progress_grant_delivered",
		"match_id", r.MatchID, "seq", r.Seq, "player_id", r.PlayerID, "outbox_id", r.ID,
		"kind", progressKindName(r.Kind), "exp_delta", r.ExpDelta,
		"items", len(r.ItemConfigIDs), "idempotency_key", idempotencyKey)
	return nil, nil
}

// progressActionStatusName 把 durable action 终态渲染成稳定名字(日志字段用)。
func progressActionStatusName(status data.ProgressActionStatus) string {
	switch status {
	case data.ProgressActionPending:
		return "pending"
	case data.ProgressActionSucceeded:
		return "succeeded"
	case data.ProgressActionFailed:
		return "failed"
	default:
		return "unknown"
	}
}

func isRetryableProgressActionError(code errcode.Code) bool {
	switch code {
	case errcode.ErrUnknown, errcode.ErrInternal, errcode.ErrTimeout, errcode.ErrUnavailable,
		errcode.ErrRateLimited, errcode.ErrCanceled, errcode.ErrServiceDisabled,
		errcode.ErrInventoryLockFailed:
		return true
	default:
		return false
	}
}

func aggregateStackGrants(itemConfigIDs []uint32) ([]data.StackGrant, error) {
	if len(itemConfigIDs) == 0 {
		return nil, errcode.New(errcode.ErrInvalidState, "empty stack grant row")
	}
	counts := make(map[uint32]int64)
	for _, id := range itemConfigIDs {
		if id == 0 {
			return nil, errcode.New(errcode.ErrInvalidState, "zero item_config_id in stack row")
		}
		counts[id]++
	}
	ids := make([]uint32, 0, len(counts))
	for id := range counts {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]data.StackGrant, 0, len(ids))
	for _, id := range ids {
		out = append(out, data.StackGrant{ItemConfigID: id, Count: counts[id]})
	}
	return out, nil
}

func singleStackFact(row data.ProgressOutboxRecord) (uint32, int64, error) {
	itemConfigIDs := row.ItemConfigIDs
	if len(itemConfigIDs) == 0 || itemConfigIDs[0] == 0 {
		return 0, 0, errcode.New(errcode.ErrInvalidState, "empty consume row")
	}
	if row.ItemCount > 0 {
		if len(itemConfigIDs) != 1 {
			return 0, 0, errcode.New(errcode.ErrInvalidState,
				"compact consume row must contain exactly one item_config_id")
		}
		return itemConfigIDs[0], int64(row.ItemCount), nil
	}
	id := itemConfigIDs[0]
	for _, got := range itemConfigIDs[1:] {
		if got != id {
			return 0, 0, errcode.New(errcode.ErrInvalidState,
				"consume row mixes item_config_ids %d and %d", id, got)
		}
	}
	return id, int64(len(itemConfigIDs)), nil
}

// deferProgressRow 推迟一条发放失败的出箱行(失败本身只告警:推迟失败下轮 Fetch 仍会
// 取到该行重试,不影响 at-least-once)。
func (u *BattleResultUsecase) deferProgressRow(ctx context.Context, id int64) {
	if err := u.repo.DeferProgressOutbox(ctx, id); err != nil {
		plog.With(ctx).Warnw("msg", "progress_outbox_defer_failed", "id", id, "err", err)
	}
}

// progressIdempotencyKey 组装进度发放幂等键:progress:{match_id}:{seq}:{player_id}:{kind}。
// 与 realtime-progression.md §3 / player.proto AddExperienceRequest 注释保持同一口径。
func progressIdempotencyKey(matchID, seq, playerID uint64, kind string) string {
	return "progress:" + strconv.FormatUint(matchID, 10) +
		":" + strconv.FormatUint(seq, 10) +
		":" + strconv.FormatUint(playerID, 10) +
		":" + kind
}
