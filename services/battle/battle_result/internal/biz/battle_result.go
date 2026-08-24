// Package biz 是 battle_result 服务的业务逻辑层(W4 ③,2026-06-06)。
//
// 职责(docs/design/go-services.md §2.13):
//   - Model-B 同步 ReportResult / legacy battle.result → 幂等落库(不变量 §2,unique match_id)
//   - MMR 在此算(Elo,DS 不可信,不变量 §6),落 battle_player_stats.mmr_delta
//   - 消费 pandora.ds.lifecycle 的 ABANDONED → 写 abandoned 补偿记录(不变量 §4)
//   - 落库同事务写 player.update 出箱 → 后台发布器可靠投递(不变量 §4)
//   - 提供 GetMatchResult / ListPlayerHistory 查询 RPC
//
// 关键不变量:
//   - 幂等键 = match_id(SaveResult 命中唯一键 → alreadyRecorded,不重复写)
//   - MMR 覆盖 DS 上报值(只信对局胜负 winner_team,不信 DS 给的 mmr_delta)
//
// W4 ⑨ 可靠补偿(事务出箱,HANDOFF §3 Step 2):
//
//	W4 ③ 落库后直接发 player.update 是 best-effort 弱依赖,Kafka 不可用时事件直接丢
//	→ 玩家段位永不更新。W4 ⑨ 改为:落 battles + stats 的同一事务里再写 player.update
//	出箱行(原子提交);后台 RunOutboxPublisher 轮询出箱逐条投递 Kafka,成功才删行。
//	配合 player 服务幂等消费(W4 ④ mmr_history uk),整条段位写链是 at-least-once
//	可靠闭环,可穿越 Kafka 临时不可用。
package biz

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/rating"
	"github.com/luyuancpp/pandora/pkg/safego"
	battlev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/battle/v1"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/data"
)

// MMRReader 读玩家在**某段位池**下的当前 MMR(算 Elo 期望胜率用)。
//
// ratingPool 是本局定格的段位池:算期望胜率必须用**同一份**段位的分,拿另一池的分
// 当输入会让 Elo 完全失真(3v3 高分玩家打 5v5 会被当成高手压分)。
// W4 ③ player 服务未上线 → StaticMMRReader 全返 BaseMMR;player 上线后换 gRPC 实现。
type MMRReader interface {
	GetMMR(ctx context.Context, playerID uint64, ratingPool string) (int, error)
}

// PlayerUpdatePusher 发 pandora.player.update 事件(kafka key=player_id,不变量 §9)。
//
// W4 ⑨ 起由后台 RunOutboxPublisher 调用:投递失败 → 返回 error → 出箱行保留下轮重试
// (不再是 best-effort 静默丢)。
type PlayerUpdatePusher interface {
	PushPlayerUpdate(ctx context.Context, playerID uint64, payload []byte) error
}

// MatchReleaser 通知 matchmaker 释放一场已结算/废弃对局的撮合状态(内部 RPC)。
//
// 修复:对局走完 READY → 进战斗 → 结算后,matchmaker 故意保留的 player→ticket 归属
// (SETNX claim)+ 票据 + match 镜像本只能等 30min TTL 自然过期;期间玩家回 Hub 再次
// StartMatch 会撞上残留 claim 报 ErrMatchAlreadyMatching(4002)。battle_result 落库后
// 主动调此接口让 matchmaker 彻底释放,玩家回 Hub 即可立刻再次匹配。
//
// 调用由 MySQL match_release_outbox worker 执行；失败/未知保留行重试，明确成功才 ACK。
type MatchReleaser interface {
	ReleaseMatch(ctx context.Context, matchID uint64, playerIDs []uint64) error
}

// TerminalReleaseRelay 把 MySQL 持久证明交给 ds_allocator 的两阶段控制面：
// ReleaseTerminal 只做永久 Redis terminal/receipt CAS + UID precondition Release；
// FinalizeTerminal 只在 MySQL 已 durable 标记 released 后给同 proof 墓碑恢复 TTL。
// 任一超时/未知结果都必须返回 error 让 outbox 保留重试。
type TerminalReleaseRelay interface {
	ReleaseTerminal(context.Context, data.TerminalReleaseRecord) error
	FinalizeTerminal(context.Context, data.TerminalReleaseRecord) error
}

// InstanceGranter 把战斗掉落按真实类型幂等写入 inventory。历史名称保留以减少接线漂移；
// 可堆叠物品走 GrantItems，装备走 GrantInstances，局内消费走系统 ConsumeBattleItem。
//
// 由后台 RunDropPublisher 调用:发放失败 → 返回 error → drop 出箱行保留下轮重试
// (at-least-once,配合 GrantInstances 幂等键去重)。实现可为 nil:inventory_addr 未配
// → 不启动掉落发布器,掉落出箱积压不丢(等 inventory 地址配好重启后补发)。
type InstanceGranter interface {
	// GrantItems 同时发放可堆叠道具与金币(goldAmount=0 表示纯道具)。
	// 两者合并成一次调用是刻意的:共用同一幂等键与同一 MySQL 事务,
	// 不会出现"道具到了钱没到"的半成功。
	GrantItems(ctx context.Context, playerID uint64, items []data.StackGrant, goldAmount uint64, idempotencyKey string) error
	GrantInstances(ctx context.Context, playerID uint64, itemConfigIDs []uint32, idempotencyKey string) error
	ConsumeBattleItem(ctx context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey string) error
	DiscardBattleItem(ctx context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey string) error
}

// BattleItemDefinition 是 battle_result 对同源 item/drop 表需要的最小投影。
type BattleItemDefinition struct {
	Equipment    bool
	BattleUsable bool
	Droppable    bool
	MaxStack     uint32
}

// BattleItemCatalog 每次从热更 Store 查当前批次；未知 ID 必须 fail-closed。
type BattleItemCatalog interface {
	Lookup(itemConfigID uint32) (BattleItemDefinition, bool)
}

// MailSender 把背包满溢出的战斗装备掉落转个人邮件(mail.SendPersonalMail,幂等键防重发)。
//
// 由 RunDropPublisher 调用:GrantInstances 返回 ErrInventoryCapacityFull(背包满)时,
// 改调此接口把溢出装备转邮件,成功后删出箱行(不再无休止重试)。实现可为 nil:mail_addr 未配
// → 背包满掉落留在出箱轮询重试(退化为历史行为,at-least-once 不丢)。
type MailSender interface {
	SendOverflowMail(ctx context.Context, playerID uint64, itemConfigIDs []uint32, idempotencyKey string) error
}

// StaticMMRReader 是固定返回 base 的 MMRReader(player 服务未上线时兜底)。
type StaticMMRReader struct {
	base int
}

// NewStaticMMRReader 构造。
func NewStaticMMRReader(base int) *StaticMMRReader { return &StaticMMRReader{base: base} }

// GetMMR 恒返 base。
func (s *StaticMMRReader) GetMMR(_ context.Context, _ uint64, _ string) (int, error) {
	return s.base, nil
}

// BattleResultUsecase 是 battle_result 业务逻辑核心。
type BattleResultUsecase struct {
	repo          data.BattleRepo
	mmr           MMRReader
	pusher        PlayerUpdatePusher
	releaser      MatchReleaser
	cfg           conf.BattleConf
	terminalRelay TerminalReleaseRelay

	// granter 把战斗装备掉落幂等发放到 inventory(W5 ④,nil-safe)。
	// nil = inventory_addr 未配 → 不启动 RunDropPublisher,掉落出箱积压不丢。
	// 用 setter 注入(SetInstanceGranter),避免构造签名被迫改(与 SetCellRouter 一致)。
	granter InstanceGranter

	// mailSender 把背包满溢出的战斗装备掉落转个人邮件(W5 ④+,nil-safe)。
	// nil = mail_addr 未配 → 背包满掉落留在出箱轮询重试(退化为历史行为,不丢)。
	mailSender MailSender

	// expGranter 把实时进度通道的击杀经验幂等入账到 player(实时成长,nil-safe)。
	// nil = player_addr 未配 → RunProgressPublisher 跳过经验行,出箱积压不丢。
	expGranter ExperienceGranter

	// monsterExp 怪物击杀经验查表(configtable role_level;数值权威见 §9.6)。
	// **非 nil-safe**:nil 时击杀事实按可重试错误拒收,绝不"跳过并推进水位"(会永久丢经验)。
	monsterExp MonsterExpTable

	// missionReporter 把战斗事实(击杀/拾取/局内使用)转发给 mission 服务推进任务进度。
	// nil = mission_addr 未配 → **不产生任务出箱行**(产生却投不出去只会让出箱无界堆积);
	// 发布顺序 Go 先行:先上 mission 服务再配地址开转发(§9.21)。
	missionReporter MissionReporter

	// itemCatalog 同时裁决掉落白名单、堆叠/实例路由与局内可消费语义。
	// 生产必须注入；nil 仅保留旧单测按 cfg.DropWhitelist 运行。
	itemCatalog BattleItemCatalog

	// router 是确定性 region/cell 路由器(scale-cellular-20m.md §4.2)。
	// 可为 nil:单 Cell / dev / 阶段 1~2 不分区,结算回流落点观测退化为不打日志(行为不变)。
	// 多 Region 部署(阶段 3)由 main 经 SetCellRouter 注入,ReportResult 落库后额外打一条
	// 跨 region 回流落点观测(overflow 对局 region_count>1 → 需多 region 回流)。nil-safe。
	router *cellroute.Router
}

// NewBattleResultUsecase 构造。pusher 可为 nil:player.update 已写事务出箱,
// pusher/producer 不可用时出箱积压不丢,等 producer 可用后由发布器补发(当前需重启/重配)。
// releaser 可为 nil:matchmaker 地址未配时 outbox 持久积压，配置恢复并重启后继续发送。
func NewBattleResultUsecase(repo data.BattleRepo, mmr MMRReader, pusher PlayerUpdatePusher, releaser MatchReleaser, cfg conf.BattleConf) *BattleResultUsecase {
	if mmr == nil {
		mmr = NewStaticMMRReader(cfg.BaseMMR)
	}
	return &BattleResultUsecase{repo: repo, mmr: mmr, pusher: pusher, releaser: releaser, cfg: cfg}
}

// SetCellRouter 注入确定性 region/cell 路由器(scale-cellular-20m.md §4.2 两级架构)。
//
// nil-safe:不调用 / 传 nil 时(单 Cell / dev / 阶段 1~2),ReportResult 不做结算回流落点观测,
// 行为与历史一致。用 setter 而非构造参数,避免单 Cell 阶段调用点被迫改签名(与 matchmaker /
// auction 一致)。Router 内部读路径无锁,并发安全。
func (u *BattleResultUsecase) SetCellRouter(r *cellroute.Router) {
	u.router = r
}

// SetInstanceGranter 注入 inventory 装备掉落发放器(W5 ④,nil-safe)。
//
// 用 setter 而非构造参数,保持 NewBattleResultUsecase 签名不变(与 SetCellRouter 一致)。
// nil / 不调用 = inventory_addr 未配 → RunDropPublisher 不启动,掉落出箱积压不丢。
func (u *BattleResultUsecase) SetInstanceGranter(g InstanceGranter) {
	u.granter = g
}

// SetBattleItemCatalog 注入与 UE 同源的 item/drop 热更视图。
func (u *BattleResultUsecase) SetBattleItemCatalog(c BattleItemCatalog) {
	u.itemCatalog = c
}

func (u *BattleResultUsecase) battleItemDefinition(itemConfigID uint32) (BattleItemDefinition, bool) {
	if u.itemCatalog != nil {
		return u.itemCatalog.Lookup(itemConfigID)
	}
	// 兼容旧单测：历史 drop_whitelist 仅装装备，因此 fallback 只声明 equipment。
	if u.cfg.IsDroppable(itemConfigID) {
		return BattleItemDefinition{Equipment: true, Droppable: true, MaxStack: 1}, true
	}
	return BattleItemDefinition{}, false
}

// SetMailSender 注入背包满溢出转邮件发送器(W5 ④+,nil-safe)。
//
// nil / 不调用 = mail_addr 未配 → 背包满掉落留在出箱轮询重试(退化为历史行为,不丢)。
// 用 setter 而非构造参数,保持 NewBattleResultUsecase 签名不变(与 SetInstanceGranter 一致)。
func (u *BattleResultUsecase) SetMailSender(m MailSender) {
	u.mailSender = m
}

// SetTerminalReleaseRelay 注入 Model-B 正常结算终态回收 relay。
// authority_mode=redis 的 main 在 schema probe 成功后必须注入；nil 时 worker 不启动，
// 但已存在 outbox 行绝不会被删除。
func (u *BattleResultUsecase) SetTerminalReleaseRelay(relay TerminalReleaseRelay) {
	u.terminalRelay = relay
}

// ── ReportResult:幂等落库 + MMR ─────────────────────────────────────────────

// ReportResult 落一场对局结算(消费 battle.result / 同步 RPC 共用)。
// 返回 alreadyRecorded:true 表示幂等命中,本次跳过(不算错误)。
// finalProgressSeq 是 DS 上报的实时进度对账水位(0 = 未走实时通道;legacy kafka 路径恒 0)。
func (u *BattleResultUsecase) ReportResult(ctx context.Context, result *battlev1.BattleResult, finalProgressSeq uint64) (bool, error) {
	return u.reportResult(ctx, result, nil, finalProgressSeq)
}

// ReportAuthorizedResult 是 Redis-authority 同步入口。terminalRelease 必须来自 service
// 已完成 Guard + active 校验的服务端快照；它与战绩同事务提交，不从 BattleResult 请求体补值。
// roster 精确校验通过后,reportResult 会先用 terminalRelease 携带的 canonical
// game_mode/map_id 覆盖请求体,再做 MMR 决策与落库(DS 伪报 game_mode/map_id 无效)。
func (u *BattleResultUsecase) ReportAuthorizedResult(
	ctx context.Context,
	result *battlev1.BattleResult,
	terminalRelease data.TerminalReleaseRecord,
	finalProgressSeq uint64,
) (bool, error) {
	if reason, samplePlayerID, err := validateAuthorizedResultRoster(result, terminalRelease.PlayerIDs); err != nil {
		// 整场结算被判 ErrUnauthorized 的闸门。ErrUnauthorized 不属 errcode.IsServerFault,
		// access log 走 rpc_ok(DEBUG)、service 层 logDSAuthReject 也不覆盖 biz 返回的错误
		// → 不在这里留证就是全链零日志。中途掉线导致 DS 少报一个 stat、或多报一个观战/替补 ID,
		// 玩家表现为「打完什么都没有」,运维必须能区分它与「DS 根本没上报」。
		plog.With(ctx).Warnw("msg", "battle_result_roster_rejected",
			"match_id", terminalRelease.MatchID, "ds_pod_name", terminalRelease.DSPodName,
			"reason", reason,
			"reported_players", len(result.GetStats()),
			"authority_players", len(terminalRelease.PlayerIDs),
			"sample_player_id", samplePlayerID,
			"code", int32(errcode.As(err)), "err", err,
			"hint", "DS 上报 stats 名单 ≠ canonical roster,本场结算整体拒绝(不落库、不发段位、不发掉落)")
		return false, err
	}
	return u.reportResult(ctx, result, &terminalRelease, finalProgressSeq)
}

// roster 校验拒绝原因枚举(§11.3 R2:一个 if 收敛的 N 个条件必须拆成 N 个 reason)。
// 只作日志判据,不参与控制流:每个 reason 对应的返回错误与拆分前逐字节一致。
const (
	rosterRejectNilResult       = "nil_result"
	rosterRejectAuthorityEmpty  = "authority_roster_empty"
	rosterRejectCountMismatch   = "count_mismatch"
	rosterRejectAuthorityZeroID = "authority_zero_player_id"
	rosterRejectAuthorityDup    = "authority_duplicate_player"
	rosterRejectReportedZeroID  = "reported_zero_player_id"
	rosterRejectReportedDup     = "reported_duplicate_player"
	rosterRejectOutsider        = "reported_outsider"
)

// validateAuthorizedResultRoster binds every settlement side effect to the
// canonical BattleStorageRecord roster captured by the credential checker.
// Equality is set-based because stat order is not an authority signal, while
// duplicate stats, omissions and outsiders are all rejected before MMR/DB work.
//
// 返回值 (reason, samplePlayerID, err):reason / samplePlayerID 只供调用方打日志
// (哪一项对不上、哪个 player_id 是第一现场),err 与拆分前完全一致。
func validateAuthorizedResultRoster(result *battlev1.BattleResult, authoritative []uint64) (string, uint64, error) {
	rosterMismatch := func(reason string, samplePlayerID uint64) (string, uint64, error) {
		return reason, samplePlayerID, errcode.New(errcode.ErrUnauthorized, "battle result roster does not match authority")
	}
	if result == nil {
		return rosterMismatch(rosterRejectNilResult, 0)
	}
	if len(authoritative) == 0 {
		return rosterMismatch(rosterRejectAuthorityEmpty, 0)
	}
	if len(result.GetStats()) != len(authoritative) {
		return rosterMismatch(rosterRejectCountMismatch, 0)
	}
	want := make(map[uint64]struct{}, len(authoritative))
	for _, playerID := range authoritative {
		if playerID == 0 {
			return rosterRejectAuthorityZeroID, 0, errcode.New(errcode.ErrUnauthorized, "battle authority roster is invalid")
		}
		if _, duplicate := want[playerID]; duplicate {
			return rosterRejectAuthorityDup, playerID, errcode.New(errcode.ErrUnauthorized, "battle authority roster is invalid")
		}
		want[playerID] = struct{}{}
	}
	seen := make(map[uint64]struct{}, len(result.GetStats()))
	for _, stat := range result.GetStats() {
		playerID := stat.GetPlayerId()
		if playerID == 0 {
			return rosterRejectReportedZeroID, 0, errcode.New(errcode.ErrUnauthorized, "battle result roster contains an invalid player")
		}
		if _, duplicate := seen[playerID]; duplicate {
			return rosterRejectReportedDup, playerID, errcode.New(errcode.ErrUnauthorized, "battle result roster contains a duplicate player")
		}
		if _, member := want[playerID]; !member {
			return rosterRejectOutsider, playerID, errcode.New(errcode.ErrUnauthorized, "battle result roster contains an unauthorized player")
		}
		seen[playerID] = struct{}{}
	}
	return "", 0, nil
}

// canonicalGameModePVECoop 是 PVE walk-in 部署的 canonical game_mode
// (decision-dungeon-entry-modes.md §4:matchmaker-pve game_mode: "pve_coop")。
// 只允许与 TerminalReleaseRecord.GameMode(canonical BattleStorageRecord 来源)比较,
// 绝不与 DS 请求体 result.game_mode 比较(§9.6 DS 请求字段不可作 MMR 安全依据)。
//
// ⚠️ 2026-08-11 起本常量**只作旧局兜底**,不再是计分判据。
// 原因:它把「算不算段位」挂在**撮合池标识**上,并且是排除法(`!= "pve_coop"` 即算 Elo)。
// game_mode 是会新增取值的部署标识(ds/v1 AllocateBattleRequest 注释里就写着未来的
// "casual_5v5" / "custom"),任何新池在旧口径下都会**静默按排位改玩家段位**——改段位
// 不可逆,是最坏的失败方向。现在的权威判据是关卡表 rating_mode 列定格进 canonical
// BattleStorageRecord 的 TerminalReleaseRecord.RatingMode(§17.1 差异进表)。
const canonicalGameModePVECoop = "pve_coop"

// settlementRunsElo 判定本局正常结算要不要算 Elo,并给出判据来源(用于日志观测)。
//
// 优先级:①canonical rating_mode(权威,成局定格) → ②旧口径兜底(canonical game_mode)
// → ③无 canonical 可用的 legacy/内部路径(保持历史行为)。
// 三条都只读**权威**字段,绝不读 DS 请求体(§9.6)。
func settlementRunsElo(terminalRelease *data.TerminalReleaseRecord) (bool, string) {
	if terminalRelease == nil {
		// legacy kafka / 内部直调:本就没有 canonical 快照可用,保持历史行为照算 Elo。
		// 该路径不受 canonical 保护,行为与本列上线前逐字节一致。
		return true, "legacy_no_canonical"
	}
	switch terminalRelease.RatingMode {
	case configpb.LevelRatingMode_LEVEL_RATING_MODE_NONE:
		return false, "rating_mode_none"
	case configpb.LevelRatingMode_LEVEL_RATING_MODE_ELO:
		return true, "rating_mode_elo"
	}
	// rating_mode 未定格:滚动升级期的旧 matchmaker / 旧批次表(§9.21)。保守回落旧口径,
	// 既不因缺字段就跳过计分(排位局白打),也不因缺字段就强开计分。
	if terminalRelease.GameMode == canonicalGameModePVECoop {
		return false, "legacy_canonical_pve_coop"
	}
	return true, "legacy_canonical_game_mode"
}

// settlementRatingPool 取本局的段位池(结算入账的分区键)。
//
// 只读 canonical 定格值,绝不读 DS 请求体、也不在结算那一刻重查关卡表 —— 与
// settlementRunsElo 同一口径(热更改表不得改写正在打的那一局的规则)。
// 空值(旧 matchmaker / 本列上线前的对局 / legacy 内部路径)归一到默认池:
// 段位必须有确定落点,不能因为缺一个字段就把玩家这一局的分丢掉(§9.22)。
func settlementRatingPool(terminalRelease *data.TerminalReleaseRecord) string {
	if terminalRelease == nil {
		return rating.DefaultPool
	}
	return rating.Normalize(terminalRelease.RatingPool)
}

func (u *BattleResultUsecase) reportResult(ctx context.Context, result *battlev1.BattleResult, terminalRelease *data.TerminalReleaseRecord, finalProgressSeq uint64) (bool, error) {
	startedAt := time.Now()
	if result == nil || result.GetMatchId() == 0 {
		// ErrInvalidArg 不属 errcode.IsServerFault,access log 记 rpc_ok(DEBUG)。
		// legacy kafka / 内部直调路径没有 service 层那条 ds_report_result_rejected,
		// 不在这里留证就是「这局根本没进结算链」零日志。
		plog.With(ctx).Warnw("msg", "battle_result_rejected",
			"reason", "missing_match_id", "authorized", terminalRelease != nil,
			"hint", "result 为空或 match_id=0,未做任何落库 / 出箱副作用")
		return false, errcode.New(errcode.ErrInvalidArg, "match_id required")
	}
	if len(result.GetStats()) == 0 {
		// 「打完什么都没有」的一种真因:DS 报了空 stats。之前整条链零日志,
		// 只能从 DS 侧看到拿着 InvalidArg 回来。
		plog.With(ctx).Warnw("msg", "battle_result_rejected",
			"match_id", result.GetMatchId(), "ds_pod_name", result.GetDsPodName(),
			"reason", "missing_stats", "authorized", terminalRelease != nil,
			"outcome", result.GetOutcome().String(),
			"hint", "DS 上报 stats 为空,本场不落库(玩家无战绩 / 无段位 / 无掉落)")
		return false, errcode.New(errcode.ErrInvalidArg, "stats required for match %d", result.GetMatchId())
	}

	// 权威字段覆盖(§9.6 数值不信 DS):授权同步路径(terminalRelease 非空)下,
	// game_mode/map_id 一律以 canonical BattleStorageRecord(经 AuthorizeResult 随
	// terminalRelease 带入)为准,在任何 MMR/DB/outbox 副作用之前覆盖 DS 请求体。
	// canonical game_mode 为空(滚动升级前旧局)也照覆盖为空:宁可少存元数据,
	// 也不把不可信请求字段伪装成权威事实。legacy/内部路径(terminalRelease==nil)
	// 无权威可用,保持请求原值(该路径本就不受 canonical 保护,行为不变)。
	if terminalRelease != nil {
		result.GameMode = terminalRelease.GameMode
		result.MapId = terminalRelease.MapID
	}

	// 正常结算:outcome 缺省补 NORMAL
	if result.GetOutcome() == battlev1.BattleOutcome_BATTLE_OUTCOME_UNSPECIFIED {
		result.Outcome = battlev1.BattleOutcome_BATTLE_OUTCOME_NORMAL
	}

	// 本局段位池(结算入账的分区键):算 Elo 读同一池的分,出箱按同一池入账。
	ratingPool := settlementRatingPool(terminalRelease)

	// MMR 仅对正常结算计算(不变量 §6,覆盖 DS 上报的 mmr_delta)。
	// ABANDONED 是补偿语义:权威路径是 ds.lifecycle → HandleAbandoned(delta 全 0,不掉段)。
	// 此处兜底:若 battle.result 误报 / 伪造 Outcome=ABANDONED,强制 delta 全 0,
	// 防止 DS 不可信地通过 abandoned 改玩家段位(不变量 §4/§6)。
	//
	// 正常结算是否算 Elo 由 settlementRunsElo 按**权威**判定:关卡表 rating_mode 成局定格
	// 优先,旧局回落旧口径。不计分的局一律 mmr_delta=0 且**完全不触碰 MMR reader**。
	switch {
	case result.GetOutcome() == battlev1.BattleOutcome_BATTLE_OUTCOME_ABANDONED:
		for _, s := range result.GetStats() {
			s.MmrDelta = 0
		}
	default:
		runElo, basis := settlementRunsElo(terminalRelease)
		if !runElo {
			for _, s := range result.GetStats() {
				s.MmrDelta = 0
			}
		} else {
			u.assignMMR(ctx, result, ratingPool)
		}
		// 判据可观测:一局到底按什么算的分,事后必须能查(尤其是回落旧口径的局)。
		if basis == "legacy_canonical_pve_coop" || basis == "legacy_canonical_game_mode" {
			plog.With(ctx).Warnw("msg", "battle_rating_basis_legacy_fallback",
				"match_id", result.GetMatchId(), "map_id", result.GetMapId(),
				"game_mode", result.GetGameMode(), "rating_pool", ratingPool,
				"run_elo", runElo, "basis", basis,
				"hint", "本局 canonical rating_mode 未定格(旧 matchmaker / 旧批次表),按旧口径结算")
		} else {
			plog.With(ctx).Debugw("msg", "battle_rating_basis",
				"match_id", result.GetMatchId(), "rating_pool", ratingPool,
				"run_elo", runElo, "basis", basis)
		}
	}

	// MMR 算完才组装出箱(携带最终 mmr_delta);与落库同事务原子提交(不变量 §4)。
	abandoned := result.GetOutcome() == battlev1.BattleOutcome_BATTLE_OUTCOME_ABANDONED
	if abandoned && terminalRelease != nil {
		// 走了完整鉴权的同步结算却报 ABANDONED = DS 想借补偿语义绕过 MMR / 掉落规则
		// (§9.6 不信 DS)。整场拒绝,必须留安全信号。
		plog.With(ctx).Warnw("msg", "battle_result_rejected",
			"match_id", result.GetMatchId(), "ds_pod_name", result.GetDsPodName(),
			"credential_pod", terminalRelease.DSPodName,
			"reason", "abandoned_with_terminal_proof", "players", len(result.GetStats()),
			"hint", "授权同步路径不接受 ABANDONED(权威补偿路径是 ds.lifecycle → HandleAbandoned)")
		return false, errcode.New(errcode.ErrInvalidArg, "completed terminal release proof cannot settle abandoned match %d", result.GetMatchId())
	}
	if terminalRelease != nil {
		if reason, err := prepareTerminalRelease(result, terminalRelease, u.cfg.TerminalReleaseGrace.Std()); err != nil {
			// 这里返回的 ErrUnauthorized / ErrInvalidState 都不属 errcode.IsServerFault,
			// access log 走 rpc_ok(DEBUG)。其中 grace_out_of_range 是**纯配置错误**:
			// 配错一次会让**每一场**正常结算失败且监控面全绿,必须 ERROR 留证。
			plog.With(ctx).Errorw("msg", "terminal_release_proof_rejected",
				"match_id", result.GetMatchId(), "allocation_id", terminalRelease.AllocationID,
				"ds_pod_name", terminalRelease.DSPodName, "reported_pod", result.GetDsPodName(),
				"reason", reason, "grace_ms", u.cfg.TerminalReleaseGrace.Std().Milliseconds(),
				"code", int32(errcode.As(err)), "err", err,
				"hint", "终态回收证明校验失败,本场结算整体拒绝;reason=grace_out_of_range 时是配置错误(全服失败)")
			return false, err
		}
	}
	outbox, err := u.buildOutbox(result, abandoned, ratingPool)
	if err != nil {
		// proto.Marshal 失败 = 本进程 pb 定义与数据不自洽,整场段位事件一条都发不出去。
		// ErrInternal 虽在 IsServerFault 白名单里(access log 会记 rpc_failed),但那条
		// 不带 match_id / 玩家数,接不回具体一局。
		plog.With(ctx).Errorw("msg", "battle_result_outbox_build_failed",
			"match_id", result.GetMatchId(), "players", len(result.GetStats()),
			"rating_pool", ratingPool, "abandoned", abandoned,
			"code", int32(errcode.As(err)), "err", err,
			"hint", "player.update 出箱组装失败,本场未落库;DS 会按同一 match_id 重试")
		return false, err
	}

	// DS 上报的金币先就地钳到服务端上限,**再**落战绩与出箱(§9.6 数值不信 DS)。
	//
	// 必须在这里钳、且钳完写回 result:battle_player_stats 与钱包发放读的是同一份 stats,
	// 若只在出箱侧钳,战绩表会记着"本局 999 亿金币"而钱包只加了 100 万 —— 玩家看战报会
	// 认为系统吞了收益,客服无从解释。另外 gold 已是 uint64,高位置 1 的值直接进
	// database/sql 会被 driver 拒(uint64 out of range),钳一次同时解决这个问题。
	u.clampReportedGold(ctx, result)

	// 战斗装备掉落出箱(W5 ④):正常结算才发放;ABANDONED(DS 崩溃补偿)不产出掉落。
	// DS 上报的 dropped_item_config_ids 按 drop 白名单过滤(DS 不可信),与落库同事务提交。
	var dropOutbox []data.DropOutboxRecord
	if !abandoned {
		dropOutbox = u.buildDropOutbox(ctx, result)
	}

	already, settleInfo, err := u.repo.SaveResult(ctx, result, outbox, dropOutbox, terminalRelease, finalProgressSeq)
	if err != nil {
		// SaveResult 的每一个失败点都返回 ErrBattleResultDBWrite(6003),而 6003 **不在**
		// errcode.IsServerFault 白名单里 → access log 走 rpc_ok(DEBUG)。不在这里打 ERROR,
		// 线上 info 级下 MySQL 抖动 / 死锁 / 字段截断 / terminal_release_outbox 写失败导致的
		// **整场结算失败在本服零日志**,只能从 DS 侧看到拿着 6003 无限重试。
		plog.With(ctx).Errorw("msg", "battle_result_persist_failed",
			"match_id", result.GetMatchId(), "ds_pod_name", result.GetDsPodName(),
			"outcome", result.GetOutcome().String(), "players", len(result.GetStats()),
			// built_drop_rows 而不是 drop_rows:这是失败路径,一行都没写进库,
			// 能说的只有"本来打算写几行"。与成功路径的 drop_rows(真正入库行数)
			// 刻意不同名 —— 同名不同义正是下面 battle_result_recorded 刚踩过的坑。
			"outbox_rows", len(outbox), "built_drop_rows", len(dropOutbox),
			"has_terminal_release", terminalRelease != nil,
			"final_progress_seq", finalProgressSeq,
			"duration_ms", time.Since(startedAt).Milliseconds(),
			"code", int32(errcode.As(err)), "err", err,
			"hint", "本场结算未落库,DS 会按同一 match_id 重试;查 MySQL 侧错误(死锁/超长/schema 漂移)")
		return false, err
	}
	if already {
		// 幂等命中本身是良性的,但「同 pod 重试」与「换了 pod 的僵尸 DS 重放」性质完全不同
		// (后者是 fencing 事件),必须能从这一条日志上区分。
		credentialPod := ""
		if terminalRelease != nil {
			credentialPod = terminalRelease.DSPodName
		}
		plog.With(ctx).Infow("msg", "battle_result_idempotent_hit",
			"match_id", result.GetMatchId(), "ds_pod_name", result.GetDsPodName(),
			"credential_pod", credentialPod, "players", len(result.GetStats()),
			"outcome", result.GetOutcome().String(), "final_progress_seq", finalProgressSeq,
			"authorized", terminalRelease != nil)
		return true, nil
	}

	// 整条结算链**唯一**一条「这一局已落库」的状态推进日志(§11.3 R1 不可逆推进打 INFO)。
	// 历史上是 Debugw,而 ABANDONED 补偿路径的 battle_abandoned_recorded 是 Infow ——
	// 正常结算反而线上不可见,「这局到底结没结算」只能查库不能查日志。
	plog.With(ctx).Infow("msg", "battle_result_recorded",
		"match_id", result.GetMatchId(), "ds_pod_name", result.GetDsPodName(),
		"winner_team", result.GetWinnerTeam(),
		"outcome", result.GetOutcome().String(), "players", len(result.GetStats()),
		"rating_pool", ratingPool, "map_id", result.GetMapId(), "game_mode", result.GetGameMode(),
		// drop_rows 必须是**真正写进 battle_drop_outbox 的行数**,不是 len(dropOutbox)。
		// 2026-08-24 收口:原来打的是入箱前的构建行数 —— 抑制局里纯道具行会被
		// SettleDropOutboxRow 整行丢弃,于是"日志说写了 5 行、库里只有 2 行",
		// 对账的人会把差额当成写丢了去查 MySQL。
		"drop_rows", persistedDropRows(dropOutbox, settleInfo.DropsSuppressed),
		"drops_suppressed", settleInfo.DropsSuppressed,
		"final_progress_seq", finalProgressSeq, "applied_seq", settleInfo.LastAppliedSeq,
		"duration_ms", time.Since(startedAt).Milliseconds())

	// 实时进度通道对账 + 单一权威路径观测(realtime-progression.md §5):
	// 掉落发放权已归实时通道时,结算上报的 dropped_item_config_ids 只作审计不再发放。
	reconcileProgress(ctx, result.GetMatchId(), finalProgressSeq, settleInfo)
	logDropSuppression(ctx, result.GetMatchId(), dropOutbox, settleInfo.DropsSuppressed)

	// 多 region:观测本局结算回流落点分布(overflow 对局 region_count>1 → 需回流多 region)。
	// router 为 nil(单 Cell)→ 不打,行为不变;跨 region 桥 / 多 region topic 回流路径属 infra(§11.1)。
	u.logSettlementRouting(ctx, result)

	// 不在同步响应路径回收 DS。Model-B 的 terminal_release_outbox 会等待宽限窗，让 DS
	// 收到 OK 并通知客户端；响应丢失/令牌过期也会在宽限后由服务端可靠回收。
	return false, nil
}

// prepareTerminalRelease 校验并定格终态回收证明。
//
// 返回值 (reason, err):reason 只供调用方打日志(§11.3 R2 —— 原实现把 19 个子条件塌成
// 一句话,pod / jti / exp / writer_epoch / 配置 grace 哪一项对不上完全查不出来);
// err 与拆分前逐字节一致,控制流不变。
func prepareTerminalRelease(result *battlev1.BattleResult, rec *data.TerminalReleaseRecord, grace time.Duration) (string, error) {
	incomplete := func(reason string) (string, error) {
		return reason, errcode.New(errcode.ErrUnauthorized, "terminal release proof is incomplete or not bound to result")
	}
	switch {
	case result == nil:
		return incomplete("nil_result")
	case rec == nil:
		return incomplete("nil_proof")
	case rec.MatchID == 0:
		return incomplete("missing_match_id")
	case rec.MatchID != result.GetMatchId():
		return incomplete("match_id_mismatch")
	case rec.AllocationID == "":
		return incomplete("missing_allocation_id")
	case rec.DSPodName == "":
		return incomplete("missing_pod")
	case rec.DSPodName != result.GetDsPodName():
		return incomplete("pod_mismatch")
	case rec.GameserverUID == "":
		return incomplete("missing_gameserver_uid")
	case rec.InstanceEpoch == 0:
		return incomplete("missing_instance_epoch")
	case rec.AuthGen == 0:
		return incomplete("missing_auth_gen")
	case rec.AuthJTI == "":
		return incomplete("missing_jti")
	case rec.AuthExpMs <= 0:
		return incomplete("invalid_exp")
	case rec.AuthKid == "":
		return incomplete("missing_kid")
	case rec.AuthTokenSHA256 == "":
		return incomplete("missing_token_sha")
	case rec.AuthWriterEpoch != auth.DSAuthWriterEpochV2:
		return incomplete("writer_epoch_mismatch")
	case rec.AuthorizedAtMs <= 0:
		return incomplete("invalid_authorized_at")
	case rec.AuthorizedAtMs >= rec.AuthExpMs:
		return incomplete("authorized_at_not_before_exp")
	case rec.ReleasedAtMs != 0:
		return incomplete("already_released")
	case len(rec.PlayerIDs) == 0:
		return incomplete("empty_roster")
	}
	if grace < 5*time.Second || grace > 2*time.Minute {
		return "grace_out_of_range", errcode.New(errcode.ErrInvalidState, "terminal release grace is outside [5s,2m]")
	}
	nowMs := time.Now().UnixMilli()
	if rec.AuthorizedAtMs > nowMs {
		return "authorized_in_future", errcode.New(errcode.ErrUnauthorized, "terminal release authorization time is in the future")
	}
	rec.ReleaseAfterMs = nowMs + grace.Milliseconds()
	rec.ReleasedAtMs = 0 // 只允许 phase1 worker 经 MySQL CAS 推进，调用方不能伪造。
	rec.CreatedAtMs = 0  // MySQL writer owns created_at_ms; caller不能伪造。
	return "", nil
}

// ── HandleAbandoned:DS 崩溃补偿 ───────────────────────────────────────────────

// HandleAbandoned 处理 ds_allocator 发来的 ABANDONED 事件(不变量 §4)。
// 写一条 outcome=ABANDONED、mmr_delta 全 0 的补偿记录(幂等),并通知 player 段位回滚。
func (u *BattleResultUsecase) HandleAbandoned(ctx context.Context, matchID uint64, playerIDs []uint64, mapID uint32, gameMode string, tsMs int64) error {
	if matchID == 0 {
		// kafka 消费面没有 access log 兜底:不打就是「ds.lifecycle 收到了但什么都没发生」。
		plog.With(ctx).Warnw("msg", "battle_abandoned_rejected",
			"reason", "missing_match_id", "players", len(playerIDs),
			"map_id", mapID, "game_mode", gameMode,
			"hint", "ABANDONED 事件缺 match_id,无法写补偿记录(上游 ds_allocator 发事件时漏填)")
		return errcode.New(errcode.ErrInvalidArg, "match_id required")
	}
	if tsMs <= 0 {
		tsMs = time.Now().UnixMilli()
	}

	stats := make([]*battlev1.PlayerStats, 0, len(playerIDs))
	for _, pid := range playerIDs {
		stats = append(stats, &battlev1.PlayerStats{PlayerId: pid, MmrDelta: 0})
	}
	result := &battlev1.BattleResult{
		MatchId:    matchID,
		EndedAtMs:  tsMs,
		WinnerTeam: winnerTeamDraw,
		Outcome:    battlev1.BattleOutcome_BATTLE_OUTCOME_ABANDONED,
		GameMode:   gameMode,
		MapId:      mapID,
		Stats:      stats,
	}

	// 出箱携 delta=0(不掉段)+ reason=abandon;与补偿记录同事务提交。
	// ABANDONED 补偿:delta 恒 0,段位池取默认值即可(0 分入账不改任何池的分,
	// 但事件仍需一个确定的池字段,消费侧才不会因空值走不同分支)。
	outbox, err := u.buildOutbox(result, true, rating.DefaultPool)
	if err != nil {
		plog.With(ctx).Errorw("msg", "battle_abandoned_outbox_build_failed",
			"match_id", matchID, "players", len(playerIDs),
			"map_id", mapID, "game_mode", gameMode,
			"code", int32(errcode.As(err)), "err", err,
			"hint", "补偿出箱组装失败,本场未写补偿记录;ds.lifecycle 消费会重试")
		return err
	}

	// ABANDONED 同样收口实时进度水位(finalProgressSeq=0):打终局标记后,分区恢复的
	// 僵尸 DS 再上报进度一律拒;崩溃前已入账的经验 / 掉落按需求保留不回滚。
	already, _, err := u.repo.SaveResult(ctx, result, outbox, nil, nil, 0)
	if err != nil {
		// 与正常结算同因:ErrBattleResultDBWrite(6003) 不是 server fault,不打就零日志。
		// 补偿记录写不进去 = DS 崩溃后玩家段位既不回滚也无战报,必须 ERROR。
		plog.With(ctx).Errorw("msg", "battle_abandoned_persist_failed",
			"match_id", matchID, "players", len(playerIDs), "map_id", mapID, "game_mode", gameMode,
			"outbox_rows", len(outbox), "code", int32(errcode.As(err)), "err", err,
			"hint", "ABANDONED 补偿未落库,ds.lifecycle 消费会重试;查 MySQL 侧错误")
		return err
	}
	if already {
		// 已有正常结算或已补偿过 → 不重复(不变量 §2)
		plog.With(ctx).Infow("msg", "abandoned_idempotent_hit", "match_id", matchID)
		return nil
	}
	plog.With(ctx).Infow("msg", "battle_abandoned_recorded", "match_id", matchID, "players", len(playerIDs))

	// matchmaker 释放由同事务 match_release_outbox 异步可靠推进。这里不调 releaseDS：
	// ABANDONED 事件来自 ds_allocator sweep，它已经回收 pod 并保留诊断镜像。
	return nil
}

// ── 查询 RPC ──────────────────────────────────────────────────────────────────

// GetMatchResult 读一场对局结算。
func (u *BattleResultUsecase) GetMatchResult(ctx context.Context, matchID uint64) (*battlev1.BattleResult, bool, error) {
	if matchID == 0 {
		plog.With(ctx).Warnw("msg", "battle_result_query_rejected", "reason", "missing_match_id")
		return nil, false, errcode.New(errcode.ErrInvalidArg, "match_id required")
	}
	res, found, err := u.repo.GetResult(ctx, matchID)
	if err != nil {
		// ErrBattleResultDBWrite / 读错误都不是 server fault,access log 记 rpc_ok(DEBUG)。
		plog.With(ctx).Warnw("msg", "battle_result_query_failed",
			"match_id", matchID, "code", int32(errcode.As(err)), "err", err)
	}
	return res, found, err
}

// ListPlayerHistory 倒序列出玩家战绩历史。
func (u *BattleResultUsecase) ListPlayerHistory(ctx context.Context, playerID uint64, limit int, beforeMs int64) ([]*battlev1.BattleResult, error) {
	if playerID == 0 {
		plog.With(ctx).Warnw("msg", "battle_history_query_rejected", "reason", "missing_player_id")
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	results, err := u.repo.ListPlayerHistory(ctx, playerID, limit, beforeMs)
	if err != nil {
		plog.With(ctx).Warnw("msg", "battle_history_query_failed",
			"player_id", playerID, "limit", limit, "before_ms", beforeMs,
			"code", int32(errcode.As(err)), "err", err)
	}
	return results, err
}

// ── 辅助 ──────────────────────────────────────────────────────────────────────

// assignMMR 按两队当前 MMR 均值算 Elo delta,写回每个 stat.MmrDelta(不变量 §6)。
func (u *BattleResultUsecase) assignMMR(ctx context.Context, result *battlev1.BattleResult, ratingPool string) {
	var sum0, n0, sum1, n1 int
	for _, s := range result.GetStats() {
		m, err := u.mmr.GetMMR(ctx, s.GetPlayerId(), ratingPool)
		if err != nil {
			m = u.cfg.BaseMMR
			// match_id 必须手写:DS 回调面没有玩家 JWT,plog.With(ctx) 不会自动带 player_id,
			// match_id 也要到 service 层 WithMatchID 才有;缺了这条就接不回具体一局。
			plog.With(ctx).Warnw("msg", "mmr_read_failed_fallback_base",
				"match_id", result.GetMatchId(),
				"player_id", s.GetPlayerId(), "rating_pool", ratingPool,
				"base_mmr", u.cfg.BaseMMR, "err", err)
		}
		if s.GetTeam() == winnerTeamA {
			sum0 += m
			n0++
		} else {
			sum1 += m
			n1++
		}
	}
	avgA := u.cfg.BaseMMR
	if n0 > 0 {
		avgA = sum0 / n0
	}
	avgB := u.cfg.BaseMMR
	if n1 > 0 {
		avgB = sum1 / n1
	}
	deltaA, deltaB := eloDeltas(avgA, avgB, u.cfg.EloKFactor, result.GetWinnerTeam())
	for _, s := range result.GetStats() {
		if s.GetTeam() == winnerTeamA {
			s.MmrDelta = int32(deltaA)
		} else {
			s.MmrDelta = int32(deltaB)
		}
	}
}

// buildOutbox 把一场结算的每个玩家组装成 player.update 出箱记录(待发布,与落库同事务)。
//
//	abandoned=true → reason 全 "abandon"(delta 已置 0,不掉段)
//	abandoned=false → 按胜负 win/lose/draw
func (u *BattleResultUsecase) buildOutbox(result *battlev1.BattleResult, abandoned bool, ratingPool string) ([]data.OutboxRecord, error) {
	recs := make([]data.OutboxRecord, 0, len(result.GetStats()))
	for _, s := range result.GetStats() {
		reason := "abandon"
		if !abandoned {
			reason = reasonForTeam(s.GetTeam(), result.GetWinnerTeam())
		}
		evt := &playerv1.PlayerUpdateEvent{
			PlayerId: s.GetPlayerId(),
			MatchId:  result.GetMatchId(),
			MmrDelta: s.GetMmrDelta(),
			Reason:   reason,
			TsMs:     result.GetEndedAtMs(),
			// 段位池随事件带给 player 服务:消费侧按本值分区入账,不再自己猜。
			RatingPool: ratingPool,
		}
		payload, err := proto.Marshal(evt)
		if err != nil {
			return nil, errcode.New(errcode.ErrInternal, "marshal player.update player=%d: %v", s.GetPlayerId(), err)
		}
		recs = append(recs, data.OutboxRecord{PlayerID: s.GetPlayerId(), Payload: payload})
	}
	return recs, nil
}

// buildDropOutbox 把一场结算里每个玩家的战斗装备掉落组装成 drop 出箱记录(与落库同事务,W5 ④)。
//
// DS 不可信:逐条按同源 drop 表过滤 DS 上报的 dropped_item_config_ids；item/drop 缺失
// 一律 fail-closed。生产不再维护手抄 drop_whitelist。
// 每玩家最多保留 cfg.MaxDropsPerPlayer() 条(超限截断记 Warn):防异常/恶意 DS 重复上报
// 海量白名单 ID 撑爆 battle_drop_outbox.item_config_ids VARCHAR(512) 导致整场结算回滚。
// 无任何白名单内掉落的玩家不产出出箱行。
// ctx 必须是**请求 ctx**(不是 context.Background):本函数的两条告警是「异常/恶意 DS 上报」
// 信号,必须能按 trace_id 关联回具体 ReportResult 调用链(不变量 §9.8 所有写都要带 trace_id)。
// clampReportedGold 把 DS 上报的每玩家金币就地钳到服务端上限(§9.6 数值不信 DS)。
//
// **就地改写 result** 是刻意的:战绩落库与钱包发放读的是同一份 stats,钳一次让两者永远一致。
// 超限只截断不拒整场 —— 战绩落库失败会连带段位、任务、掉落一起丢,代价远大于少发点钱;
// 但每次截断都留 Warn,异常 DS 照样可发现。
func (u *BattleResultUsecase) clampReportedGold(ctx context.Context, result *battlev1.BattleResult) {
	maxGold := u.cfg.MaxBattleGoldPerPlayer()
	for _, s := range result.GetStats() {
		if s == nil || s.GetGold() <= maxGold {
			continue
		}
		plog.With(ctx).Warnw("msg", "battle_gold_truncated",
			"match_id", result.GetMatchId(), "player_id", s.GetPlayerId(),
			"reported", s.GetGold(), "kept", maxGold,
			"hint", "DS 上报金币超服务端上限(配置错误或越权上报)")
		s.Gold = maxGold
	}
}

func (u *BattleResultUsecase) buildDropOutbox(ctx context.Context, result *battlev1.BattleResult) []data.DropOutboxRecord {
	maxDrops := u.cfg.MaxDropsPerPlayer()
	recs := make([]data.DropOutboxRecord, 0, len(result.GetStats()))
	for _, s := range result.GetStats() {
		// 本局金币收益(2026-08-22 补齐)。
		//
		// 此前 PlayerStats.gold 只被写进 battle_player_stats 战绩表,**从来没有发放到玩家钱包** ——
		// 全仓没有任何一处把它送进 inventory,战后的"获得 N 金币"是纯展示。
		// 现在让它搭既有的战后发放出箱:同一套幂等键、同一套重试、同一套审计,
		// 不另起一条"金币专用发放链"(§15.2 最少复杂度)。
		//
		// 值已由 clampReportedGold 钳过上限,这里直接用。
		goldGranted := s.GetGold()

		reported := s.GetDroppedItemConfigIds()
		if len(reported) == 0 {
			// 没掉落但有金币:仍要出一条只带货币的出箱行,否则金币照旧发不出去。
			if goldGranted > 0 {
				recs = append(recs, data.DropOutboxRecord{
					PlayerID: s.GetPlayerId(), CurrencyAmount: goldGranted,
				})
			}
			continue
		}
		capHint := len(reported)
		if capHint > maxDrops {
			capHint = maxDrops
		}
		allowed := make([]uint32, 0, capHint)
		stacks := make([]uint32, 0, capHint)
		instances := make([]uint32, 0, capHint)
		truncated := false
		// 被过滤掉的 ID 去重收集:「打完没掉落」最常见的真因就是 item/drop 表漏配某个 ID,
		// 只报「报了 N 个全被过滤」而不给 ID,排障没有落点(实时通道那条
		// progress_facts_skipped 反而带了 sample_item_config_id,两边口径必须一致)。
		filteredIDs := map[uint32]struct{}{}
		var sampleFilteredID uint32
		for _, id := range reported {
			def, ok := u.battleItemDefinition(id)
			if id != 0 && ok && def.Droppable {
				if len(allowed) >= maxDrops {
					truncated = true
					break
				}
				allowed = append(allowed, id)
				if def.Equipment {
					instances = append(instances, id)
				} else {
					stacks = append(stacks, id)
				}
				continue
			}
			if _, seen := filteredIDs[id]; !seen {
				filteredIDs[id] = struct{}{}
				if sampleFilteredID == 0 {
					sampleFilteredID = id
				}
			}
		}
		if truncated {
			// 超过每玩家上限 → 截断丢弃并 Warn(大概率是异常/恶意 DS,不能让它打失败整场结算)。
			plog.With(ctx).Warnw("msg", "battle_drop_truncated",
				"match_id", result.GetMatchId(), "player_id", s.GetPlayerId(),
				"reported", len(reported), "kept", len(allowed), "max", maxDrops)
		}
		if len(allowed) == 0 {
			// DS 上报了掉落但全不在白名单 → 记一条 Warn(可能是配置漏项或 DS 越权尝试)。
			plog.With(ctx).Warnw("msg", "battle_drop_all_filtered",
				"match_id", result.GetMatchId(), "player_id", s.GetPlayerId(),
				"reported", len(reported),
				"distinct_item_ids", len(filteredIDs), "sample_item_config_id", sampleFilteredID,
				"hint", "item/drop 表漏配该 ID(改表)或 DS 上报未授权掉落(安全信号)")
			// 掉落全被过滤不代表金币也该丢:两者是独立的收益来源。
			if goldGranted > 0 {
				recs = append(recs, data.DropOutboxRecord{
					PlayerID: s.GetPlayerId(), CurrencyAmount: goldGranted,
				})
			}
			continue
		}
		recs = append(recs, data.DropOutboxRecord{
			PlayerID: s.GetPlayerId(), ItemConfigIDs: allowed,
			StackItemConfigIDs: stacks, InstanceItemConfigIDs: instances,
			CurrencyAmount: goldGranted,
		})
	}
	return recs
}

// persistedDropRows 数出这一局**真正写进 battle_drop_outbox 的行数**。
//
// 判据不在这里复刻,直接调生产的 data.SettleDropOutboxRow(纯函数,与 SaveResult 事务里
// 逐行调的是同一个)。抄一份长度公式或分支条件必然漂移 —— 假仓抄生产分支导致
// "金币被静默吞掉"全绿,就是本轮刚付过的学费。
func persistedDropRows(built []data.DropOutboxRecord, dropsSuppressed bool) int {
	n := 0
	for _, d := range built {
		if _, keep := data.SettleDropOutboxRow(d, dropsSuppressed); keep {
			n++
		}
	}
	return n
}

// logDropSuppression 记录「实时进度通道已接管道具发放」这一事实。
//
// built 是 buildDropOutbox 的**入箱前**列表(SaveResult 内部才按 data.SettleDropOutboxRow
// 逐行裁剪),所以这里能算出「本来要发什么、实际被掐掉了哪一部分」。
//
// 2026-08-24 修日志与代码事实不符:这条日志原文是「本场掉落已经实时通道逐事件发放,
// 结算掉落字段仅审计」,并把 audit_rows 打成 len(built)。P0-2(货币不受抑制)修完后
// 这句话对**金币行不成立** —— 复核实测 match_id=622 先打了这条「仅审计」,紧接着同一行
// 就 drop_grant_delivered 把 250 金币发了出去。排障的人按原文会认定「抑制局什么都没入箱」,
// 正是 SettleDropOutboxRow 注释里警告的「把排障引到错方向」。
// 教训:抑制的粒度从「整行」收窄到「三列道具」之后,描述整行的日志必须跟着一起收窄,
// 否则修复本身就成了下一次误判的源头。
//
// 只有**真的掐掉了道具**才打:全是纯金币行时一件道具都没被抑制,打 *_suppressed_*
// 又会是一次同样的名不副实。
func logDropSuppression(ctx context.Context, matchID uint64, built []data.DropOutboxRecord, dropsSuppressed bool) {
	if !dropsSuppressed || len(built) == 0 {
		return
	}
	suppressedRows, suppressedItems, currencyRows := 0, 0, 0
	var currencyTotal uint64
	for _, d := range built {
		if n := len(d.ItemConfigIDs); n > 0 {
			suppressedRows++
			suppressedItems += n
		}
		if d.CurrencyAmount > 0 {
			currencyRows++
			currencyTotal += d.CurrencyAmount
		}
	}
	if suppressedRows == 0 {
		return
	}
	// hint 分两句写:承诺"随后会有 drop_grant_delivered"只有在**真有金币行**时才成立。
	// 2026-08-24 收口:上一版只判 suppressedRows==0 就把带 delivered 承诺的 hint 无条件打出去,
	// 而"有掉落、这一局没金币"是常见形态(currencyRows==0 → 一行都不入箱)——排障的人
	// 会照着 hint 去等一条**永远不会出现**的 drop_grant_delivered,再一次被日志引到错方向。
	hint := "只有**道具**被抑制(已由实时进度通道逐事件发放,结算字段仅审计);" +
		"本局无金币收益 → 本 match_id 不会再有任何出箱行,也不会有 drop_grant_delivered"
	if currencyRows > 0 {
		hint = "只有**道具**被抑制(已由实时进度通道逐事件发放,结算字段仅审计);" +
			"金币不走实时通道,granted_currency_rows 行照常入箱,随后同 match_id 会有 drop_grant_delivered"
	}
	plog.With(ctx).Infow("msg", "battle_drop_suppressed_by_progress",
		"match_id", matchID, "built_rows", len(built),
		"suppressed_item_rows", suppressedRows, "suppressed_items", suppressedItems,
		"granted_currency_rows", currencyRows, "granted_currency_total", currencyTotal,
		"hint", hint)
}

// withOutboxTrace 给后台出箱 worker 的**单行单次投递**现铸一个 trace_id(不变量 §9.8,
// §11.3 R3 join key)。
//
// 出箱行落库时没有记下原始请求的 trace_id(要给出箱表加列,属 schema 改动),而后台
// publisher 的根 ctx 是进程级的、本就不带 trace_id —— 结果是 worker 侧「发放失败 /
// 发放成功」日志 trace_id 恒空,与 player / inventory / mission / matchmaker 侧的 access log
// 没有任何共同字段,只能靠幂等键人肉拼。pkg/middleware.Trace 的 client 分支**优先取 ctx 里的
// trace_id**(取不到才自己现生成、且不回写调用方),所以在这里先铸一个,本服日志与下游
// access log 就落在同一个 trace_id 上。同一行重试会拿到新的 trace_id(每次投递一条链路),
// 跨重试的关联键是日志里同时打出的 outbox_id + 幂等键。
func withOutboxTrace(ctx context.Context) context.Context {
	return plog.WithTraceID(ctx, uuid.NewString())
}

// ── player.update 事务出箱发布器(W4 ⑨,不变量 §4)─────────────────────────────

// RunOutboxPublisher 启动后台 player.update 出箱发布循环,直到 ctx 取消。
//
// 每轮取一批待发布出箱行(FIFO 按 id),逐条投递 Kafka;投递成功才删行。投递失败 →
// 本批中断、保留出箱行,下一轮重试(同玩家 key 有序,不变量 §9)。配合 player 服务幂等
// 消费(W4 ④ mmr_history uk),整条段位写链是 at-least-once 可靠闭环,可穿越 Kafka 临时不可用。
func (u *BattleResultUsecase) RunOutboxPublisher(ctx context.Context) {
	interval := u.cfg.OutboxPublishInterval.Std()
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	plog.With(ctx).Infow("msg", "outbox_publisher_started", "interval", interval.String(), "batch", u.outboxBatchSize())
	for {
		select {
		case <-ctx.Done():
			plog.With(ctx).Infow("msg", "outbox_publisher_stopped")
			return
		case <-ticker.C:
			// panic 兜底(压测审核【必修-6】同类点位):单轮 panic 只丢本轮,出箱行保留下轮重试。
			safego.Run(ctx, "battle_outbox_publisher", func() {
				if n, err := u.publishOutboxBatch(ctx); err != nil {
					plog.With(ctx).Warnw("msg", "outbox_publish_batch_failed", "published", n, "err", err)
				}
			})
		}
	}
}

// publishOutboxBatch 取一批出箱记录投递,返回本轮成功投递并删除的条数。
// 投递失败立即中断本轮(保留出箱行下轮重试),保证同玩家事件按 id 顺序投递。
// outboxNoPusherLog 限流「无 producer 但出箱有积压」告警(首错+每 60s 一条)。
var outboxNoPusherLog plog.Window

func (u *BattleResultUsecase) publishOutboxBatch(ctx context.Context) (int, error) {
	if u.pusher == nil {
		// kafka 未配置:出箱无法投递。出箱行已落库不丢,等 producer 可用后重启再发。
		// 积压必须可见:启动仅有一条 kafka_brokers_empty WARN,运行期若段位事件持续
		// 堆积而零信号,「打完段位没变」会被当成结算 bug 排查(实为部署缺 kafka)。
		if recs, ferr := u.repo.FetchOutbox(ctx, 1); ferr == nil && len(recs) > 0 {
			if ok, streak := outboxNoPusherLog.Admit(time.Now().UnixMilli(), 60_000); ok {
				plog.With(ctx).Warnw("msg", "outbox_pending_without_pusher",
					"streak_ticks", streak,
					"hint", "kafka producer 未配置但段位出箱有积压:配置 kafka.brokers 后重启")
			}
		}
		return 0, nil
	}
	recs, err := u.repo.FetchOutbox(ctx, u.outboxBatchSize())
	if err != nil {
		return 0, err
	}
	published := 0
	for _, r := range recs {
		rowCtx := withOutboxTrace(ctx)
		rowStartedAt := time.Now()
		if perr := u.pusher.PushPlayerUpdate(rowCtx, r.PlayerID, r.Payload); perr != nil {
			// 批级 outbox_publish_batch_failed 说不出卡在谁身上;段位事件按 player_id 保序,
			// 卡住的这一行会挡住该玩家后续所有 player.update。
			plog.With(rowCtx).Warnw("msg", "outbox_publish_failed",
				"player_id", r.PlayerID, "outbox_id", r.ID, "published_before", published,
				"elapsed_ms", time.Since(rowStartedAt).Milliseconds(),
				"code", int32(errcode.As(perr)), "err", perr,
				"hint", "本轮中断保留出箱行下轮重试(同玩家保序);持续失败查 kafka producer")
			return published, perr // 本轮中断,保留出箱行下轮重试
		}
		if derr := u.repo.DeleteOutbox(rowCtx, r.ID); derr != nil {
			// 已投递但未 ACK:下轮会重投同一事件(player 侧 mmr_history uk 幂等吸收)。
			// 不留证时,「同一段位事件反复重投」在本服零日志,只能从 player 侧发现。
			plog.With(rowCtx).Warnw("msg", "outbox_delete_failed",
				"player_id", r.PlayerID, "outbox_id", r.ID, "published_before", published,
				"elapsed_ms", time.Since(rowStartedAt).Milliseconds(),
				"code", int32(errcode.As(derr)), "err", derr,
				"hint", "kafka 已投递但出箱行未删除 → 下轮重投同一事件(下游幂等吸收)")
			return published, derr
		}
		// 逐玩家成功台账(资产变更口径,与 drop_grant_delivered / mission_fact_delivered 对齐):
		// 段位是玩家资产,「该玩家的段位事件已投出」必须能按 player_id 在 info 级正查,
		// 不能只靠「没有 outbox_publish_failed」反证。每玩家每局一条,量级有界。
		plog.With(rowCtx).Infow("msg", "player_update_delivered",
			"player_id", r.PlayerID, "outbox_id", r.ID,
			"elapsed_ms", time.Since(rowStartedAt).Milliseconds())
		published++
	}
	if published > 0 {
		plog.With(ctx).Debugw("msg", "outbox_published", "count", published)
	}
	return published, nil
}

// outboxBatchSize 返回每轮发布批大小(配置缺省 128)。
func (u *BattleResultUsecase) outboxBatchSize() int {
	if u.cfg.OutboxBatchSize > 0 {
		return u.cfg.OutboxBatchSize
	}
	return 128
}

// ── matchmaker release 事务出箱 ──────────────────────────────────────────────

func matchReleaseRetryDelay(attempt uint32) time.Duration {
	shift := attempt
	if shift > 6 {
		shift = 6
	}
	d := time.Second * time.Duration(1<<shift)
	if d > time.Minute {
		return time.Minute
	}
	return d
}

// RunMatchReleasePublisher 可靠释放 matchmaker 的 ticket/claim/match 状态。SaveResult
// 与 outbox 同事务；RPC 失败或响应未知只延期，明确成功后才删除行。
func (u *BattleResultUsecase) RunMatchReleasePublisher(ctx context.Context) {
	interval := u.cfg.OutboxPublishInterval.Std()
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	plog.With(ctx).Infow("msg", "match_release_publisher_started", "interval", interval.String(), "batch", u.outboxBatchSize())
	for {
		select {
		case <-ctx.Done():
			plog.With(ctx).Infow("msg", "match_release_publisher_stopped")
			return
		case <-ticker.C:
			safego.Run(ctx, "battle_match_release_publisher", func() {
				if n, err := u.publishMatchReleaseBatch(ctx); err != nil {
					plog.With(ctx).Warnw("msg", "match_release_batch_failed", "released", n, "err", err)
				}
			})
		}
	}
}

func (u *BattleResultUsecase) publishMatchReleaseBatch(ctx context.Context) (int, error) {
	if u.releaser == nil {
		return 0, nil
	}
	nowMs := time.Now().UnixMilli()
	recs, err := u.repo.FetchMatchReleaseOutbox(ctx, u.outboxBatchSize(), nowMs)
	if err != nil {
		return 0, err
	}
	released := 0
	var joined error
	for _, rec := range recs {
		rowCtx := withOutboxTrace(ctx)
		rowStartedAt := time.Now()
		callCtx, cancel := context.WithTimeout(rowCtx, 10*time.Second)
		rerr := u.releaser.ReleaseMatch(callCtx, rec.MatchID, rec.PlayerIDs)
		cancel()
		if rerr != nil {
			// 逐行留证:批级聚合的 match_release_batch_failed 说不出「是哪一局没释放」,
			// 而没释放的直接后果是玩家回 Hub 再次 StartMatch 撞 4002(残留 claim)。
			nextMs := time.Now().Add(matchReleaseRetryDelay(rec.AttemptCount)).UnixMilli()
			plog.With(rowCtx).Warnw("msg", "match_release_failed",
				"match_id", rec.MatchID, "outbox_id", rec.ID, "players", len(rec.PlayerIDs),
				"attempt", rec.AttemptCount, "next_attempt_at_ms", nextMs,
				"elapsed_ms", time.Since(rowStartedAt).Milliseconds(),
				"code", int32(errcode.As(rerr)), "err", rerr,
				"hint", "matchmaker 撮合状态未释放,玩家回 Hub 再匹配可能撞 ErrMatchAlreadyMatching(4002)")
			if derr := u.repo.DeferMatchReleaseOutbox(ctx, rec.ID, nextMs); derr != nil {
				plog.With(rowCtx).Warnw("msg", "match_release_defer_failed",
					"match_id", rec.MatchID, "outbox_id", rec.ID, "next_attempt_at_ms", nextMs,
					"code", int32(errcode.As(derr)), "err", derr,
					"hint", "退避写入失败:下轮 Fetch 仍会取到该行重试(不影响 at-least-once)")
				joined = errors.Join(joined, rerr, derr)
			} else {
				joined = errors.Join(joined, rerr)
			}
			continue
		}
		if derr := u.repo.DeleteMatchReleaseOutbox(ctx, rec.ID); derr != nil {
			plog.With(rowCtx).Warnw("msg", "match_release_delete_failed",
				"match_id", rec.MatchID, "outbox_id", rec.ID, "players", len(rec.PlayerIDs),
				"elapsed_ms", time.Since(rowStartedAt).Milliseconds(),
				"code", int32(errcode.As(derr)), "err", derr,
				"hint", "matchmaker 已释放但出箱行未删 → 下轮重调 ReleaseMatch(matchmaker 侧幂等)")
			joined = errors.Join(joined, derr)
			continue
		}
		plog.With(rowCtx).Infow("msg", "match_release_published",
			"match_id", rec.MatchID, "outbox_id", rec.ID, "players", len(rec.PlayerIDs),
			"attempt", rec.AttemptCount,
			"elapsed_ms", time.Since(rowStartedAt).Milliseconds())
		released++
	}
	return released, joined
}

// ── Battle terminal-release 事务出箱(Model-B)────────────────────────────────

// RunTerminalReleasePublisher 启动正常结算资源回收 worker。它只能在 MySQL schema
// probe、relay 构造和 dsauth capability 获取全部成功后启动。单行失败保留重试，不阻塞
// 同批其它对局；UID precondition 与 ds_allocator Redis CAS 保证多副本/响应丢失幂等。
func (u *BattleResultUsecase) RunTerminalReleasePublisher(ctx context.Context) {
	if u.terminalRelay == nil {
		plog.With(ctx).Infow("msg", "terminal_release_publisher_disabled")
		return
	}
	interval := u.cfg.TerminalReleaseInterval.Std()
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	plog.With(ctx).Infow("msg", "terminal_release_publisher_started",
		"interval", interval.String(), "batch", u.terminalReleaseBatchSize())
	for {
		select {
		case <-ctx.Done():
			plog.With(ctx).Infow("msg", "terminal_release_publisher_stopped")
			return
		case <-ticker.C:
			safego.Run(ctx, "battle_terminal_release_publisher", func() {
				if n, err := u.publishTerminalReleaseBatch(ctx); err != nil {
					plog.With(ctx).Warnw("msg", "terminal_release_batch_failed", "finalized", n, "err", err)
				}
			})
		}
	}
}

func (u *BattleResultUsecase) publishTerminalReleaseBatch(ctx context.Context) (int, error) {
	if u.terminalRelay == nil {
		return 0, nil
	}
	recs, err := u.repo.FetchTerminalReleaseOutbox(ctx, u.terminalReleaseBatchSize(), time.Now().UnixMilli())
	if err != nil {
		return 0, err
	}
	finalized := 0
	for _, rec := range recs {
		if rec.ReleasedAtMs < 0 {
			// 库里出现不可能的值 = schema 漂移 / 写入者 bug,本轮整批停止且会反复发生。
			plog.With(ctx).Errorw("msg", "terminal_release_row_invalid",
				"match_id", rec.MatchID, "allocation_id", rec.AllocationID,
				"pod", rec.DSPodName, "outbox_id", rec.ID, "released_at_ms", rec.ReleasedAtMs,
				"reason", "negative_released_at_ms",
				"hint", "terminal_release_outbox 行非法,本轮中断;DS pod 不会被回收,需人工排查")
			return finalized, errcode.New(errcode.ErrInvalidState,
				"terminal release outbox id=%d has invalid released_at_ms", rec.ID)
		}
		rowCtx := withOutboxTrace(ctx)
		rowStartedAt := time.Now()
		callCtx, cancel := context.WithTimeout(rowCtx, 10*time.Second)
		if rec.ReleasedAtMs == 0 {
			err := u.terminalRelay.ReleaseTerminal(callCtx, rec)
			cancel()
			if err != nil {
				// Redis/K8s unknown 绝不能推进 DB phase；永久墓碑/原始行保留重试。
				plog.With(rowCtx).Warnw("msg", "terminal_release_phase1_failed",
					"match_id", rec.MatchID, "allocation_id", rec.AllocationID,
					"pod", rec.DSPodName, "outbox_id", rec.ID,
					"elapsed_ms", time.Since(rowStartedAt).Milliseconds(),
					"code", int32(errcode.As(err)), "err", err)
				continue
			}
			marked, err := u.repo.MarkTerminalReleaseReleased(rowCtx, rec.ID, time.Now().UnixMilli())
			if err != nil {
				// UID delete 已成功但 durable ACK 未知：phase1 绝不 expire Redis。
				// 下轮按 DB 真实状态重读；0 则重放 UID delete，>0 则进入 finalize。
				plog.With(rowCtx).Warnw("msg", "terminal_release_mark_failed",
					"match_id", rec.MatchID, "allocation_id", rec.AllocationID,
					"pod", rec.DSPodName, "outbox_id", rec.ID,
					"elapsed_ms", time.Since(rowStartedAt).Milliseconds(),
					"code", int32(errcode.As(err)), "err", err,
					"hint", "Redis terminal CAS 已成功但 MySQL durable ACK 未知,本轮中断等下轮按库真实状态重跑")
				return finalized, err
			}
			if !marked {
				plog.With(rowCtx).Debugw("msg", "terminal_release_phase1_already_advanced",
					"match_id", rec.MatchID, "outbox_id", rec.ID)
			} else {
				// 不可逆推进(Redis terminal/receipt CAS 已完成 + MySQL 已 durable 标记)。
				plog.With(rowCtx).Infow("msg", "terminal_release_phase1_done",
					"match_id", rec.MatchID, "allocation_id", rec.AllocationID,
					"pod", rec.DSPodName, "outbox_id", rec.ID,
					"elapsed_ms", time.Since(rowStartedAt).Milliseconds())
			}
			continue
		}

		err := u.terminalRelay.FinalizeTerminal(callCtx, rec)
		cancel()
		if err != nil {
			// finalize 响应未知绝不能 DELETE released 行；重试只校验/Expire 同 proof，
			// 绝不再次删除 K8s。若 TTL 已自然清空全部墓碑，服务端按幂等成功返回。
			plog.With(rowCtx).Warnw("msg", "terminal_release_finalize_failed",
				"match_id", rec.MatchID, "allocation_id", rec.AllocationID,
				"pod", rec.DSPodName, "outbox_id", rec.ID,
				"elapsed_ms", time.Since(rowStartedAt).Milliseconds(),
				"code", int32(errcode.As(err)), "err", err)
			continue
		}
		if err := u.repo.DeleteTerminalReleaseOutbox(rowCtx, rec.ID); err != nil {
			// finalize 已成功但 DB delete 失败：released 行保留。下一轮只重放
			// finalize；即使墓碑 TTL 已过、三键都不存在，也会幂等重认成功。
			plog.With(rowCtx).Warnw("msg", "terminal_release_delete_failed",
				"match_id", rec.MatchID, "allocation_id", rec.AllocationID,
				"pod", rec.DSPodName, "outbox_id", rec.ID,
				"elapsed_ms", time.Since(rowStartedAt).Milliseconds(),
				"code", int32(errcode.As(err)), "err", err,
				"hint", "回收已完成但出箱行未删 → 下轮重放 finalize(幂等)")
			return finalized, err
		}
		// 落在 DELETE 之后:先打日志再落库时,库操作失败会让日志与库状态互相矛盾,
		// 排障会按"已完成"处理(同 progress_unknown_fact_stream_stopped 的纪律)。
		plog.With(rowCtx).Infow("msg", "terminal_release_finalized",
			"match_id", rec.MatchID, "allocation_id", rec.AllocationID,
			"pod", rec.DSPodName, "outbox_id", rec.ID,
			"released_at_ms", rec.ReleasedAtMs,
			"elapsed_ms", time.Since(rowStartedAt).Milliseconds())
		finalized++
	}
	if finalized > 0 {
		plog.With(ctx).Debugw("msg", "terminal_release_outbox_finalized", "count", finalized)
	}
	return finalized, nil
}

func (u *BattleResultUsecase) terminalReleaseBatchSize() int {
	if u.cfg.TerminalReleaseBatchSize > 0 {
		return u.cfg.TerminalReleaseBatchSize
	}
	return 128
}

// ── 战斗装备掉落事务出箱发布器(W5 ④,at-least-once + GrantInstances 幂等)──────────

// RunDropPublisher 启动后台战斗装备掉落出箱发放循环,直到 ctx 取消。
//
// 每轮取一批 drop 出箱行,逐行调 inventory.GrantInstances(幂等键 battle_drop:{match_id}:{player_id}),
// 成功才删行。与 player.update 出箱不同:掉落无跨玩家保序需求 → 单行失败不中断本轮(continue),
// 只把失败行留到下轮重试(避免某玩家背包满时阻塞其他玩家)。配合 GrantInstances 幂等,
// 整条掉落写链是 at-least-once 可靠闭环,可穿越 inventory 临时不可用。
//
// granter==nil(inventory_addr 未配)→ 直接返回不启动;drop 出箱积压不丢,等地址配好重启补发。
func (u *BattleResultUsecase) RunDropPublisher(ctx context.Context) {
	if u.granter == nil {
		plog.With(ctx).Infow("msg", "drop_publisher_disabled", "hint", "inventory_addr 未配置 → 战斗装备掉落不发放(出箱积压不丢)")
		return
	}
	interval := u.cfg.DropPublishInterval.Std()
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	plog.With(ctx).Infow("msg", "drop_publisher_started", "interval", interval.String(), "batch", u.dropBatchSize())
	for {
		select {
		case <-ctx.Done():
			plog.With(ctx).Infow("msg", "drop_publisher_stopped")
			return
		case <-ticker.C:
			safego.Run(ctx, "battle_drop_publisher", func() {
				if n, err := u.publishDropBatch(ctx); err != nil {
					plog.With(ctx).Warnw("msg", "drop_publish_batch_failed", "granted", n, "err", err)
				}
			})
		}
	}
}

// publishDropBatch 取一批掉落出箱行发放,返回本轮成功发放并删除的条数。
// 单行发放失败仅记录并 continue(保留出箱行下轮重试),不阻塞其他玩家掉落。
func (u *BattleResultUsecase) publishDropBatch(ctx context.Context) (int, error) {
	if u.granter == nil {
		return 0, nil
	}
	recs, err := u.repo.FetchDropOutbox(ctx, u.dropBatchSize())
	if err != nil {
		return 0, err
	}
	granted := 0
	for _, r := range recs {
		rowCtx := withOutboxTrace(ctx)
		rowStartedAt := time.Now()
		idempotencyKey := dropIdempotencyKey(r.MatchID, r.PlayerID)
		if gerr := u.deliverDropRecord(rowCtx, r); gerr != nil {
			// 任一路由失败都保留整行；已成功的另一路由靠独立幂等键回放，不会重复入账。
			// match_id / outbox_id / 幂等键必须打全:没有它们时「某玩家某局掉落没进背包」
			// 只能靠 player_id 在 inventory 侧大海捞针。
			plog.With(rowCtx).Warnw("msg", "drop_grant_failed",
				"match_id", r.MatchID, "player_id", r.PlayerID, "outbox_id", r.ID,
				"items", len(r.ItemConfigIDs), "stack_items", len(r.StackItemConfigIDs),
				"instance_items", len(r.InstanceItemConfigIDs),
				"idempotency_key", idempotencyKey,
				"elapsed_ms", time.Since(rowStartedAt).Milliseconds(),
				"code", int32(errcode.As(gerr)), "err", gerr,
				"hint", "出箱行保留下轮重试(at-least-once);持续失败查 inventory / mail 侧同 trace_id")
			continue
		}
		if derr := u.repo.DeleteDropOutbox(rowCtx, r.ID); derr != nil {
			plog.With(rowCtx).Warnw("msg", "drop_outbox_delete_failed",
				"match_id", r.MatchID, "player_id", r.PlayerID, "outbox_id", r.ID,
				"idempotency_key", idempotencyKey,
				"elapsed_ms", time.Since(rowStartedAt).Milliseconds(),
				"code", int32(errcode.As(derr)), "err", derr,
				"hint", "掉落已发放但出箱行未删 → 下轮重发(inventory 同幂等键去重)")
			return granted, derr
		}
		// 资产变更必须有逐玩家台账:失败有行、成功没行时,「掉落到底发没发」在出箱行被删后
		// 只能反查 inventory。这是每场对局每玩家至多一条,不是高频路径(§11.3 R1/R4)。
		plog.With(rowCtx).Infow("msg", "drop_grant_delivered",
			"match_id", r.MatchID, "player_id", r.PlayerID, "outbox_id", r.ID,
			"stack_items", len(r.StackItemConfigIDs), "instance_items", len(r.InstanceItemConfigIDs),
			"idempotency_key", idempotencyKey,
			"elapsed_ms", time.Since(rowStartedAt).Milliseconds())
		granted++
	}
	if granted > 0 {
		plog.With(ctx).Debugw("msg", "drop_outbox_granted", "count", granted)
	}
	return granted, nil
}

func (u *BattleResultUsecase) deliverDropRecord(ctx context.Context, r data.DropOutboxRecord) error {
	// 路由在 SaveResult 首次入箱时已经冻结。重试绝不读取 item/drop 热配置，否则
	// stack 已成功、instance 失败期间的类型热更会换 method/key，造成已成功部分双发。
	var stacks []data.StackGrant
	if len(r.StackItemConfigIDs) > 0 {
		var err error
		stacks, err = aggregateStackGrants(r.StackItemConfigIDs)
		if err != nil {
			return err
		}
	}
	instances := r.InstanceItemConfigIDs
	if len(stacks) == 0 && len(instances) == 0 && r.CurrencyAmount == 0 {
		return errcode.New(errcode.ErrInvalidState, "drop outbox row has no frozen route id=%d", r.ID)
	}
	// 两个 bool 是本函数**唯一**的路由事实:分叉子键与两处实际调用都只读它们。
	//
	// 2026-08-24 修 P0:分叉条件曾是 `len(stacks)>0 && len(instances)>0`,而发放条件在
	// 08-22 加金币时变成了 `len(stacks)>0 || CurrencyAmount>0` —— 两份条件从此各走各的。
	// 于是「爆装备 + 有金币、但没爆可堆叠道具」(最常见的战果形态之一)会不分叉:
	// GrantItems 先用 baseKey 把金币发了,GrantInstances 紧接着拿同一个 baseKey 必被判幂等冲突
	// → 整行返错 → 出箱行不删 → 下轮金币幂等命中、装备再次冲突,装备永远发不到玩家手上,
	// 而且日志打成 inventory_idempotency_conflict(那是反作弊/串账信号,直接把排障引到错方向)。
	// 教训:同一件事绝不写两份条件 —— 复制的那一份迟早只改一处。
	//
	// ⚠ 上线迁移窗口(2026-08-24,一次性):本次修复把「货币 + 装备、无堆叠道具」这一形态的
	// stack 侧幂等键从 baseKey 改成了 baseKey+":stack"。库里**已经卡住**的这类存量出箱行
	// (旧代码下 GrantItems 已按 baseKey 把金币成功入账、GrantInstances 随即被判 7015
	// 幂等冲突 → 整行返错 → 行永不删)在新代码上线后重试时,会拿 baseKey+":stack" 这把
	// **全新的键**再发一次金币 —— inventory 那边没有这把键的流水,于是幂等去重不生效,
	// 金币双发。一个"卡死不发"的缺陷被换成了"多发钱"。
	//
	// 影响面精确到一种形态:currency_amount > 0 且 instance 列非空 且 stack 列为空。
	// 其余形态的键逐字节没变(不分叉时仍是 baseKey;stack 非空时旧代码本来就分叉)。
	//
	// 上线前必须做的排查与清账步骤(含可直接执行的 SQL)在
	// **docs/ops/release-checklist.md §2.7「战斗掉落出箱幂等键分叉(本次必须人工门禁)」**。
	// 刻意只在那里留一份:发布的人读的是发布清单,不会来读 biz 层某个函数的注释;
	// 两处各写一份步骤,迟早只改一处。这里只保留"为什么会有这个窗口"。
	grantStackOrCurrency := len(stacks) > 0 || r.CurrencyAmount > 0
	grantInstances := len(instances) > 0
	baseKey := dropIdempotencyKey(r.MatchID, r.PlayerID)
	stackKey, instanceKey := baseKey, baseKey
	if grantStackOrCurrency && grantInstances {
		stackKey, instanceKey = baseKey+":stack", baseKey+":instance"
	}
	// 金币与可堆叠道具走同一次 GrantItems:两者共用一个幂等键、一个事务,
	// 不会出现"道具到了钱没到"。纯金币行(无掉落)也走这条路径。
	if grantStackOrCurrency {
		if err := u.granter.GrantItems(ctx, r.PlayerID, stacks, r.CurrencyAmount, stackKey); err != nil {
			return err
		}
	}
	if !grantInstances {
		return nil
	}
	// ⚠ GrantInstances 这一整行只调**一次**,别再拆批(2026-08-24 回退,已实测复现装备双发)。
	//
	// 上一轮加过 grantInstancesBatched/grantInstanceChunk,把 instances 按固定条数切成
	// instanceKey:0 / :1 … 多次调用。它引入 P0:**拆批把"转邮件"和"重试"这两条互斥路径
	// 变成了可以叠加的**。
	//   - 拆批前:GrantInstances 只调一次 → 背包满就 SendOverflowMail → return nil →
	//     调用方把出箱行当轮删掉。"转邮件"即终态,这一行此后不存在重试。
	//   - 拆批后:批 0 背包满 → 转邮件成功 → 只 return nil、**循环继续**;只要后面任一批
	//     失败(最典型的是 mail 响应丢包,本包 drop_overflow_ack_loss_test.go 就是为这个
	//     场景写的),整行返错保留 → 下一轮 publishDropBatch 把**已经邮寄出去的批 0**
	//     再走一次 GrantInstances。inventory 侧那把键从没落过流水(grant 失败不留幂等记录),
	//     背包一腾出空位就真发。实测 8 件装备变 16 件。
	// 教训:**拆批必须与"转邮件即终态"这条不变量一起设计**,单独加拆批就是把互斥路径
	// 变成可叠加。要重做必须先把"这一行已部分转邮件"变成可持久化、可续跑的状态
	// (出箱行加进度列),那不是发布器侧一个 for 循环能覆盖的改动。
	//
	// 另外拆批还把任何 >N 件装备的行的键从 instanceKey 改成 instanceKey:0/:1…,
	// 而这类行在旧代码下本来就能一次发成功(inventory 按**实际编码长度**上闸,见下),
	// 于是白白制造了一个"存量行换新键再发一遍"的迁移窗口。
	//
	// 遗留边界(**本轮明确不修**,先于本次改造就存在):inventory 把本次发放的 instance_id
	// 列表编进 inventory_ledger.detail(`grant_inst ids=1,2,…`,VARCHAR(255))作为幂等回放的
	// 唯一事实源,判据是 data.GrantInstancesDetailFits(services/economy/inventory/internal/data/
	// inventory_instance.go)——按实际编码长度判,17 位雪花下一次约能发 13 件。单行装备数
	// 超过它时 GrantInstances 返 errcode.ErrInvalidArg(不是 capacity-full,**不会**转邮件),
	// 出箱行每轮重试每轮失败 = 永久卡死行。每玩家掉落上限 conf.MaxDropsPerPlayer()
	// 默认 32、硬上限 46,所以形态可达。
	// 正解是 expand 迁移把 detail 列加宽、或把 instance_id 挪进独立子表,让"一次发放"
	// 仍是一次原子记账;**不是**在发布器侧拆批(拆批的代价见上)。
	if err := u.granter.GrantInstances(ctx, r.PlayerID, instances, instanceKey); err != nil {
		// 只有实例背包满才允许转邮件；堆叠物品已走计数模型，不应进入装备邮件链。
		if u.mailSender != nil && errcode.As(err) == errcode.ErrInventoryCapacityFull {
			if merr := u.mailSender.SendOverflowMail(ctx, r.PlayerID, instances, instanceKey); merr != nil {
				plog.With(ctx).Warnw("msg", "drop_overflow_mail_failed",
					"match_id", r.MatchID, "player_id", r.PlayerID, "outbox_id", r.ID,
					"items", len(instances), "idempotency_key", instanceKey,
					"code", int32(errcode.As(merr)), "err", merr)
				return merr
			}
			plog.With(ctx).Infow("msg", "drop_overflow_mailed",
				"match_id", r.MatchID, "player_id", r.PlayerID, "outbox_id", r.ID,
				"items", len(instances), "idempotency_key", instanceKey)
			return nil
		}
		return err
	}
	return nil
}

// dropBatchSize 返回每轮掉落发放批大小(配置缺省 128)。
func (u *BattleResultUsecase) dropBatchSize() int {
	if u.cfg.DropBatchSize > 0 {
		return u.cfg.DropBatchSize
	}
	return 128
}

// dropIdempotencyKey 组装战斗装备掉落幂等键:battle_drop:{match_id}:{player_id}。
// 同对局同玩家的掉落只入账一次(GrantInstances 幂等去重,资产不变量)。
func dropIdempotencyKey(matchID, playerID uint64) string {
	return "battle_drop:" + strconv.FormatUint(matchID, 10) + ":" + strconv.FormatUint(playerID, 10)
}
