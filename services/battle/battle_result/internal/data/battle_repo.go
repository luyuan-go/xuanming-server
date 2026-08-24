// Package data 是 battle_result 服务的数据层(MySQL 战斗结算落库)。
//
// 库表(deploy/mysql-init/03-battle-tables.sql):
//
//	pandora_battle.battles              对局结算头(PK match_id,幂等键,不变量 §2)
//	pandora_battle.battle_player_stats  玩家战绩 + MMR 变化(uk match_id+player_id)
//
// 幂等:SaveResult 在一个事务里 INSERT battles;命中 1062 唯一键冲突 → 视为已落库
// (alreadyRecorded=true),回滚不重复写 stats。MMR 在 biz 层算好后由本层落库。
package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/errcode"
	battlev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/battle/v1"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
)

// OutboxRecord 是一条待发布的 player.update 事务出箱记录(W4 ⑨,不变量 §4)。
//
// 落 battles + battle_player_stats 的同一事务里写入,二者原子提交;后台发布器轮询
// 出箱表逐条投递 Kafka,投递成功才删行。ID 仅 FetchOutbox 返回时填充。
type OutboxRecord struct {
	ID       int64  // 出箱行主键(SaveResult 入参时忽略,FetchOutbox 返回时填充)
	PlayerID uint64 // kafka key(不变量 §9 同玩家事件保序)
	Payload  []byte // player.v1.PlayerUpdateEvent proto bytes
}

// DropOutboxRecord 是一条待发放的战斗道具掉落事务出箱记录(W5 ④)。
//
// 与 battles + battle_player_stats 同一事务写入(原子提交);后台 RunDropPublisher
// 轮询本表,按真实 item.type 分流到 inventory.GrantItems / GrantInstances，
// 两路使用稳定子幂等键，全部成功才删行。ItemConfigIDs 已由 biz 层按同源
// configtable drop×item 过滤(DS 不可信)。
// ID 仅 FetchDropOutbox 返回时填充。
type DropOutboxRecord struct {
	ID                    int64    // 出箱行主键(SaveResult 入参时忽略,FetchDropOutbox 返回时填充)
	MatchID               uint64   // 对局 ID(SaveResult 入参时忽略,取 result.MatchId;FetchDropOutbox 返回时填充,组幂等键用)
	PlayerID              uint64   // 受益玩家
	ItemConfigIDs         []uint32 // 首次过滤后的完整掉落列表(审计/历史兼容)
	StackItemConfigIDs    []uint32 // 首次入箱时冻结的可堆叠路由；重试不查热配置
	InstanceItemConfigIDs []uint32 // 首次入箱时冻结的装备路由；重试不查热配置

	// CurrencyAmount 本局该玩家的金币收益(2026-08-22 补;0 = 无收益)。
	// 首次入箱时已过服务端上限闸冻结,重试不重新读 DS 上报值也不重新读热配置 ——
	// 与 Stack/InstanceItemConfigIDs 同一纪律:出箱行是**已裁决的事实**,不是待裁决的输入。
	CurrencyAmount uint64
}

// SettleDropOutboxRow 判定结算路径下这一行掉落出箱**实际该写什么**,返回裁剪后的行与是否要写。
//
// dropsSuppressed 的语义只有一条:**掉落道具**已被实时进度通道逐事件发放过,结算路径不得重复发。
// 它**不覆盖货币** —— 实时通道刻意不发金币(biz/progress.go:GrantItems 第三实参硬编码 0,
// 注释「实时进度通道只发道具,金币走结算出箱」),金币的唯一发放路径就是这里。
//
// 2026-08-24 修 P0:此前抑制包住的是整个入箱循环,连"纯货币、零道具"的行一起跳过 ——
// 于是走过实时通道的对局(即拾取/击杀走第三通道的正常局)战后金币两边都不发,
// 永久丢失且零错误日志,把 08-22「战后金币此前从未进过钱包」的修复在主路径上原地废掉。
//
// 抽成导出函数是刻意的:此前 biz 层假仓逐字复刻了同一段分支,假仓与生产同构 →
// 这类"静默漏发"在单测里根本不可见。假仓改调本函数后,biz 层对出箱内容的断言
// 观察到的就是生产判据本身,复制体不会再各自漂移。
//
// # 成行判据为什么看**三列并集**而不只看 ItemConfigIDs
//
// 判据是「四项里任意一项非空就成行」:ItemConfigIDs / StackItemConfigIDs /
// InstanceItemConfigIDs / CurrencyAmount。
//
// 今天全仓唯一的生产者 biz.buildDropOutbox 维持着这条不变量:
//
//	ItemConfigIDs ⊇ StackItemConfigIDs ∪ InstanceItemConfigIDs
//
// (它对每个通过白名单的 ID 先 append 进 allowed → ItemConfigIDs,再按 def.Equipment
// 分到 instances / stacks,三列在同一个 append 里一次性写进同一条记录。)
// 在这条不变量下,并集判据与旧的"只看 ItemConfigIDs"**零行为差异**:凡两条路由列非空的
// 行,ItemConfigIDs 必定也非空。
//
// 2026-08-24 收口:那就把它改掉,而不是把陷阱写进注释留给下一个人踩。
// 本函数是**导出**的(biz 层假仓也在调),将来任何新增生产者 —— 例如只填 Stack/Instance
// 两列、把 ItemConfigIDs 当"历史兼容字段"留空的 —— 在旧判据下会被整行丢弃:
// 出箱表里没有行、结算日志里没有错误、玩家掉落凭空消失(零日志静默丢弃)。
// 改成并集之后这个失败模式**从机制上不存在**,代价是一行代码。
// "记录陷阱"和"消除陷阱"能选后者时,永远选后者。
//
// 不变量本身仍由 data/drop_outbox_settle_test.go 钉住(buildDropOutbox 改坏了要变红):
// 并集判据保证了"不丢行",但 ItemConfigIDs 仍是下游对账用的全量视图,不能任其漂移。
func SettleDropOutboxRow(d DropOutboxRecord, dropsSuppressed bool) (DropOutboxRecord, bool) {
	row := d
	if dropsSuppressed {
		// 只掐掉道具三列,货币原样保留;整行只剩货币时下面的判据自会决定是否成行。
		row.ItemConfigIDs, row.StackItemConfigIDs, row.InstanceItemConfigIDs = nil, nil, nil
	}
	// 只带金币、不带掉落的行也要写:金币是独立收益来源,
	// 旧判据 `len(ItemConfigIDs) == 0 → continue` 会把它整条丢掉。
	if len(row.ItemConfigIDs) == 0 && len(row.StackItemConfigIDs) == 0 &&
		len(row.InstanceItemConfigIDs) == 0 && row.CurrencyAmount == 0 {
		return DropOutboxRecord{}, false
	}
	return row, true
}

// TerminalReleaseRecord 是正常结算的持久终态回收证明。
//
// 本记录只能由 ReportResult 完成 callback Guard + Redis active 校验后构造，并与
// battles / battle_player_stats 同一 MySQL 事务提交。Auth* 是“服务端校验时刻”的
// 完整凭据证明；relay 时允许该 token 已过期或 gen/jti 已轮换，但 stable identity
// (match/allocation/pod/UID/epoch/writer fence)必须仍精确一致。
type TerminalReleaseRecord struct {
	ID              uint64
	MatchID         uint64
	AllocationID    string
	DSPodName       string
	GameserverUID   string
	InstanceEpoch   uint32
	AuthGen         uint64
	AuthJTI         string
	AuthExpMs       int64
	AuthKid         string
	AuthTokenSHA256 string
	AuthWriterEpoch uint32
	AuthorizedAtMs  int64
	ReleaseAfterMs  int64
	// PlayerIDs is the immutable authoritative roster copied from the canonical
	// BattleStorageRecord at authorization time. It is deliberately not a DS
	// payload field and is not persisted in terminal_release_outbox; SaveResult
	// uses it in the same transaction to build the match-release outbox. This
	// prevents a compromised DS from omitting a player or adding an outsider to
	// recovery side effects.
	PlayerIDs []uint64
	// GameMode / MapID are the canonical battle metadata copied from the same
	// canonical BattleStorageRecord snapshot as PlayerIDs at authorization time.
	// They are NOT DS BattleResult request fields: reportResult overwrites the
	// untrusted request game_mode/map_id with these values before any MMR/DB/
	// outbox side effect, and the MMR policy (canonical "pve_coop" never runs
	// Elo) keys off GameMode here, never off the request. GameMode may be empty
	// for BattleStorageRecords written before the field existed (rolling
	// upgrade); empty means "canonical mode unknown" and must keep the legacy
	// conservative behavior (NORMAL still runs Elo), never trust the request to
	// fill the gap. Like PlayerIDs, both fields are only used inside this
	// authorized settlement and are not persisted in terminal_release_outbox
	// (no DB migration).
	GameMode string
	MapID    uint32
	// RatingMode is the canonical "does this battle change rank" fact, frozen by
	// matchmaker from the level table at match formation and copied here from the
	// same canonical BattleStorageRecord snapshot as PlayerIDs/GameMode/MapID.
	// It is the authoritative MMR policy input: LEVEL_RATING_MODE_NONE never runs
	// Elo, LEVEL_RATING_MODE_ELO does. UNSPECIFIED means "not frozen" (old
	// matchmaker or a config batch predating the column) and falls back to the
	// legacy GameMode=="pve_coop" heuristic — never to the DS request. Like
	// PlayerIDs/GameMode/MapID it lives only inside this authorized settlement and
	// is not persisted in terminal_release_outbox (no DB migration).
	RatingMode configpb.LevelRatingMode
	// RatingPool is the canonical rating partition key frozen alongside RatingMode.
	// Empty means "not frozen" (old matchmaker / pre-column battle) and is normalized
	// to rating.DefaultPool at settlement so the delta always has a definite home.
	RatingPool string
	// ReleasedAtMs>0 是阶段1“永久 Redis terminal + UID delete 已明确成功”的
	// MySQL durable ACK。只有该状态才允许阶段2给墓碑设 TTL并删除本行。
	ReleasedAtMs int64
	CreatedAtMs  int64
}

// MatchReleaseRecord 是 battle_result→matchmaker 的持久事务 outbox 行。
// payload 是 MatchReleaseStorageRecord；调用明确成功才 ACK 删除，未知/失败只延期。
type MatchReleaseRecord struct {
	ID              uint64
	OperationID     string
	MatchID         uint64
	PlayerIDs       []uint64
	AttemptCount    uint32
	NextAttemptAtMs int64
	CreatedAtMs     int64
}

// BattleRepo 是 battle_result 数据层抽象。biz 层只依赖此接口,不依赖 *sql.DB。
type BattleRepo interface {
	// SaveResult 事务写 battles + battle_player_stats + player_update_outbox +
	// battle_drop_outbox + 可选 terminal_release_outbox,并在同一事务内收口实时进度通道
	// (battle_progress_stream 打终局标记;水位>0 时抑制 dropOutbox 发放,单一权威路径,
	// realtime-progression.md §5)。六者原子提交(不变量 §4:落库、业务出箱、终态资源回收不会半成功)。
	// 幂等:match_id 已存在 → 返回 (true, zero, nil),不重复写(两路出箱也不写)。
	// dropOutbox 可为空(无掉落 / ABANDONED);finalProgressSeq 是 DS 上报的对账水位(0=未走实时通道)。
	// terminalRelease 仅正常的、已完成 Model-B 鉴权的同步 ReportResult 非空；ABANDONED
	// 已由 ds_allocator 先回收，不得写 completed 终态行。
	SaveResult(ctx context.Context, result *battlev1.BattleResult, outbox []OutboxRecord, dropOutbox []DropOutboxRecord, terminalRelease *TerminalReleaseRecord, finalProgressSeq uint64) (alreadyRecorded bool, progress ProgressSettleInfo, err error)
	// GetResult 读一场对局结算(含全部玩家战绩)。not found → (nil, false, nil)。
	GetResult(ctx context.Context, matchID uint64) (*battlev1.BattleResult, bool, error)
	// ListPlayerHistory 倒序列出玩家参与的对局(ended_at_ms < beforeMs,最多 limit 条)。
	// beforeMs<=0 表示从最新开始。
	ListPlayerHistory(ctx context.Context, playerID uint64, limit int, beforeMs int64) ([]*battlev1.BattleResult, error)
	// FetchOutbox 按 id 升序取最多 limit 条待发布出箱记录(FIFO 保序)。
	FetchOutbox(ctx context.Context, limit int) ([]OutboxRecord, error)
	// DeleteOutbox 删除已成功投递的出箱行。
	DeleteOutbox(ctx context.Context, id int64) error
	// FetchDropOutbox 按 id 升序取最多 limit 条待发放掉落出箱记录(W5 ④)。
	FetchDropOutbox(ctx context.Context, limit int) ([]DropOutboxRecord, error)
	// DeleteDropOutbox 删除已成功发放的掉落出箱行。
	DeleteDropOutbox(ctx context.Context, id int64) error
	// FetchTerminalReleaseOutbox 只取已经到达客户端通知宽限窗的终态行。
	FetchTerminalReleaseOutbox(ctx context.Context, limit int, nowMs int64) ([]TerminalReleaseRecord, error)
	// MarkTerminalReleaseReleased 是 UID 条件回收明确成功后的 durable phase-1 ACK。
	// CAS 只允许 0→releasedAtMs；false 表示已由并发 worker 推进或行已不存在。
	MarkTerminalReleaseReleased(ctx context.Context, id uint64, releasedAtMs int64) (bool, error)
	// DeleteTerminalReleaseOutbox 只在 released 行的 finalize-only RPC 明确成功后 ACK。
	DeleteTerminalReleaseOutbox(ctx context.Context, id uint64) error
	FetchMatchReleaseOutbox(ctx context.Context, limit int, nowMs int64) ([]MatchReleaseRecord, error)
	DeferMatchReleaseOutbox(ctx context.Context, id uint64, nextAttemptAtMs int64) error
	DeleteMatchReleaseOutbox(ctx context.Context, id uint64) error

	// ── 实时进度通道(实时成长,realtime-progression.md §3)────────────────────
	// GetProgressWatermark 读进度水位快照(seq / 累计入账 / 结算态 / 停流态 / 行存在性)。
	GetProgressWatermark(ctx context.Context, matchID uint64) (ProgressWatermark, error)
	// MarkProgressStopped 持久化本场停流标记(未知事实场景;幂等,只记首次)。
	// 停流后 ReportProgress 一律拒,已知事实批也不得重新开流(审计 P1)。
	// ⚠️ 本方法是 upsert(行存在也写停流位),只能用于"流内确定要停"的场景;
	// 通道关闭副本固化 legacy 模式必须走 ClaimProgressLegacy(审计 R4 #11:
	// 滚动混版下 upsert 会把开启副本并发刚开的流停掉,整场实时奖励丢失)。
	MarkProgressStopped(ctx context.Context, matchID uint64) error
	// ClaimProgressLegacy 仅当水位行**不存在**时原子创建停流标记行(INSERT IGNORE
	// 语义,claimed=true);行已存在(开启副本已开流/已认领/已结算)时不修改任何
	// 内容返回 claimed=false,调用方必须重读水位按现行状态继续。
	ClaimProgressLegacy(ctx context.Context, matchID uint64) (claimed bool, err error)
	// ApplyProgress 原子推进水位(乐观 CAS)+ 累计本批经验/件数(场 + 单玩家)+
	// **事务内一致快照上判定 caps 累计上限** + 写进度出箱。stack pickup 同事务
	// 增加本场同 item 余额；consume/discard 条件预留支出并创建 durable action outcome。
	// CAS 失败 / 已结算 → ErrUnavailable(调用方重读水位收敛);超限 → ErrInvalidArg
	// 整体回滚。上限判定必须留在事务内:事务外读-判与 CAS 分属不同快照,重试请求
	// 会把同批 delta 重复计入后永久误拒(审计 P1,§16.1 TOCTOU)。
	ApplyProgress(ctx context.Context, matchID, expectedSeq, newSeq uint64, addExp uint64, addItems uint32, playerDeltas []ProgressPlayerDelta, rows []ProgressOutboxRecord, missionRows []MissionFactRecord, caps ProgressCaps) error

	// ── 任务事实转发出箱(docs/design/mission.md §5.1;独立表,故障域与进度出箱隔离)──
	FetchMissionOutbox(ctx context.Context, limit int) ([]MissionFactRecord, error)
	DeleteMissionOutbox(ctx context.Context, id int64) error
	DeferMissionOutbox(ctx context.Context, id int64) error
	// FetchProgressOutbox 按 id 升序取最多 limit 条已到重试时点的待发放进度出箱记录。
	FetchProgressOutbox(ctx context.Context, limit int) ([]ProgressOutboxRecord, error)
	// FetchProgressOutboxForPlayer 忽略退避时间，取该玩家不超过 maxSeq 的最早行。
	// ReportProgress 的同步 action 路径用它驱动前序 Grant，确保 Grant 先于 Consume/Discard。
	FetchProgressOutboxForPlayer(ctx context.Context, matchID, playerID, maxSeq uint64) (ProgressOutboxRecord, bool, error)
	// GetProgressAction 读取独立 action 批的 durable completion outcome。
	GetProgressAction(ctx context.Context, matchID, seq, playerID uint64, kind ProgressGrantKind) (ProgressAction, bool, error)
	// ResolveProgressAction 把 inventory 明确结果持久为 success/failure，并与删除对应
	// outbox 行放在同一 MySQL 事务；并发执行时返回先落库的权威 outcome。
	ResolveProgressAction(ctx context.Context, row ProgressOutboxRecord, resultCode errcode.Code) (ProgressAction, error)
	// DeleteProgressOutbox 删除已成功发放的进度出箱行。
	DeleteProgressOutbox(ctx context.Context, id int64) error
	// DeferProgressOutbox 发放失败后指数退避推迟该行(行不丢,防队首阻塞)。
	DeferProgressOutbox(ctx context.Context, id int64) error

	// ── 保留期清理(CLAUDE.md §9 不变量 24:只增表必须有界)──────────────────────
	// SweepExpiredBattles 处理服务端落库时间 created_at 早于 cutoffMs 的对局
	// (battles + battle_player_stats 成组,单批最多 batch 场)。
	//
	// **mode 默认 ModeReportOnly:只统计待清理对局数并 WARN 告警,一行都不删**(用户指令:
	// 不允许"因为数据大了"自动删数据);只有配置显式 retention_mode=delete 才真删。
	//
	// 清理依据必须是服务端时间(§9.6 数值不信 DS):ended_at_ms 是 DS 上报,失陷 DS 报过去
	// 时间可让新战绩立即可删(毁审计证据)、报未来时间可让行永不可删。
	// 真删语义:battles.match_id 是结算幂等键,删除后同 match 重放已不可能到达
	// (权威 BattleStorageRecord / active match / Guard 早已释放,ReportResult 在凭据层就被拒)。
	SweepExpiredBattles(ctx context.Context, mode dbguard.Mode, cutoffMs int64, batch int) (dbguard.Outcome, error)
	// SweepSettledProgress 处理已结算(settled_at_ms>0,服务端结算打标)且早于 cutoffMs
	// 的进度水位(stream + player + item balance + action + 残留 progress outbox 成组)。
	// **mode 默认 ModeReportOnly(只报告不删)**;未结算行无论如何都不在处理范围
	// (陈年未结算 = 补偿链 bug,由 CountStaleUnsettledProgress 持续告警,不静默删证据)。
	SweepSettledProgress(ctx context.Context, mode dbguard.Mode, cutoffMs int64, batch int) (dbguard.Outcome, error)
	// CountStaleUnsettledProgress 数超保留期仍未结算的水位行(结算补偿链 bug 证据,
	// 永不自动清理;sweep 每轮告警暴露)。updated_at_ms 为服务端写入时间。
	CountStaleUnsettledProgress(ctx context.Context, cutoffMs int64) (int64, error)
}

// MySQLBattleRepo 是基于 database/sql 的 BattleRepo 实现。
type MySQLBattleRepo struct {
	db *sql.DB
}

// NewMySQLBattleRepo 构造。db 由 pkg/mysqlx.MustNewClient 提供(连 pandora_battle 库)。
func NewMySQLBattleRepo(db *sql.DB) *MySQLBattleRepo {
	return &MySQLBattleRepo{db: db}
}

// ValidateRecoveryOutboxSchema fails at startup instead of waiting for the
// first settlement to discover a missing additive migration.
func (r *MySQLBattleRepo) ValidateRecoveryOutboxSchema(ctx context.Context) error {
	checks := []string{
		`SELECT id, match_id, payload, next_attempt_at_ms, attempt_count, created_at_ms FROM match_release_outbox LIMIT 0`,
		`SELECT id, match_id, player_id, item_config_ids, stack_item_config_ids, instance_item_config_ids, created_at_ms FROM battle_drop_outbox LIMIT 0`,
	}
	for _, query := range checks {
		rows, err := r.db.QueryContext(ctx, query)
		if err != nil {
			return errcode.New(errcode.ErrInternal, "battle recovery outbox schema invalid: %v", err)
		}
		if err := rows.Close(); err != nil {
			return errcode.New(errcode.ErrInternal, "close battle recovery schema probe: %v", err)
		}
	}
	return nil
}

func (r *MySQLBattleRepo) SaveResult(ctx context.Context, result *battlev1.BattleResult, outbox []OutboxRecord, dropOutbox []DropOutboxRecord, terminalRelease *TerminalReleaseRecord, finalProgressSeq uint64) (bool, ProgressSettleInfo, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, ProgressSettleInfo{}, errcode.New(errcode.ErrBattleResultDBWrite, "begin tx: %v", err)
	}
	// 任何提前 return 前回滚;Commit 成功后 Rollback 返回 ErrTxDone 可忽略。
	defer func() { _ = tx.Rollback() }()

	const insBattle = `INSERT INTO battles
(match_id, started_at_ms, ended_at_ms, winner_team, outcome, ds_pod_name, game_mode, map_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	_, err = tx.ExecContext(ctx, insBattle,
		result.GetMatchId(),
		result.GetStartedAtMs(),
		result.GetEndedAtMs(),
		result.GetWinnerTeam(),
		int32(result.GetOutcome()),
		result.GetDsPodName(),
		result.GetGameMode(),
		result.GetMapId(),
	)
	if err != nil {
		if isDupErr(err) {
			// 幂等重放仍须恢复可能缺失的 recovery outbox。Redis-authority 路径只信
			// checker 从 canonical BattleStorageRecord 带回的 roster；legacy/ABANDONED
			// 才回读首笔已落库 stats，绝不信本次重复 payload。
			playerIDs := authoritativeRecoveryPlayerIDs(terminalRelease)
			if len(playerIDs) == 0 {
				var qerr error
				playerIDs, qerr = loadMatchPlayerIDsTx(ctx, tx, result.GetMatchId())
				if qerr != nil {
					return false, ProgressSettleInfo{}, errcode.New(errcode.ErrBattleResultDBWrite,
						"load idempotent match release players match=%d: %v", result.GetMatchId(), qerr)
				}
			}
			if ierr := insertMatchReleaseOutboxTx(ctx, tx, result.GetMatchId(), playerIDs, time.Now().UnixMilli()); ierr != nil {
				return false, ProgressSettleInfo{}, errcode.New(errcode.ErrBattleResultDBWrite,
					"restore match release outbox match=%d: %v", result.GetMatchId(), ierr)
			}
			// 幂等重放分支同样必须收口实时进度水位(审计 P0):首笔落库若由不含进度逻辑的
			// 旧副本完成,水位行会一直未打终局标记,僵尸 / 分区恢复 DS 的迟到进度仍会被
			// 接受并发放。settleProgressStreamTx 幂等,已收口的行原样返回不改写首次标记。
			if _, serr := settleProgressStreamTx(ctx, tx, result.GetMatchId(), finalProgressSeq, time.Now().UnixMilli()); serr != nil {
				return false, ProgressSettleInfo{}, errcode.New(errcode.ErrBattleResultDBWrite,
					"settle progress stream on replay match=%d: %v", result.GetMatchId(), serr)
			}
			if cerr := tx.Commit(); cerr != nil {
				return false, ProgressSettleInfo{}, errcode.New(errcode.ErrBattleResultDBWrite,
					"commit idempotent match release match=%d: %v", result.GetMatchId(), cerr)
			}
			return true, ProgressSettleInfo{}, nil
		}
		return false, ProgressSettleInfo{}, errcode.New(errcode.ErrBattleResultDBWrite, "insert battles match=%d: %v", result.GetMatchId(), err)
	}

	const insStat = `INSERT INTO battle_player_stats
(match_id, player_id, hero_id, team, kills, deaths, assists, damage_dealt, damage_taken, healing, gold, mmr_delta)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	for _, s := range result.GetStats() {
		if _, serr := tx.ExecContext(ctx, insStat,
			result.GetMatchId(),
			s.GetPlayerId(),
			s.GetHeroId(),
			s.GetTeam(),
			s.GetKills(),
			s.GetDeaths(),
			s.GetAssists(),
			s.GetDamageDealt(),
			s.GetDamageTaken(),
			s.GetHealing(),
			s.GetGold(),
			s.GetMmrDelta(),
		); serr != nil {
			return false, ProgressSettleInfo{}, errcode.New(errcode.ErrBattleResultDBWrite, "insert stats match=%d player=%d: %v",
				result.GetMatchId(), s.GetPlayerId(), serr)
		}
	}

	// 同事务写 player.update 出箱(不变量 §4:落库与待发布段位事件原子提交)。
	const insOutbox = `INSERT INTO player_update_outbox
(match_id, player_id, payload, created_at_ms)
VALUES (?, ?, ?, ?)`
	nowMs := time.Now().UnixMilli()
	for _, o := range outbox {
		if _, oerr := tx.ExecContext(ctx, insOutbox,
			result.GetMatchId(), o.PlayerID, o.Payload, nowMs,
		); oerr != nil {
			return false, ProgressSettleInfo{}, errcode.New(errcode.ErrBattleResultDBWrite, "insert outbox match=%d player=%d: %v",
				result.GetMatchId(), o.PlayerID, oerr)
		}
	}

	// 实时进度通道结算收口(与落库同事务,realtime-progression.md §5):
	// 打终局标记封死迟到进度(僵尸 / 分区恢复 DS fencing);水位>0 = 本场发放权已归实时通道,
	// 结算路径掉落只作对账不再入 drop 出箱(单一权威路径,防双发;判定依据是服务端水位表,不信 DS)。
	settleInfo, serr := settleProgressStreamTx(ctx, tx, result.GetMatchId(), finalProgressSeq, nowMs)
	if serr != nil {
		return false, ProgressSettleInfo{}, errcode.New(errcode.ErrBattleResultDBWrite,
			"settle progress stream match=%d: %v", result.GetMatchId(), serr)
	}

	// 同事务写战斗装备掉落出箱(W5 ④):落库与待发放装备掉落原子提交(不变量 §4)。
	const insDropOutbox = `INSERT INTO battle_drop_outbox
(match_id, player_id, item_config_ids, stack_item_config_ids, instance_item_config_ids, currency_amount, created_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?)`
	for _, d := range dropOutbox {
		row, keep := SettleDropOutboxRow(d, settleInfo.DropsSuppressed)
		if !keep {
			continue
		}
		if _, derr := tx.ExecContext(ctx, insDropOutbox,
			result.GetMatchId(), row.PlayerID, encodeConfigIDs(row.ItemConfigIDs),
			encodeConfigIDs(row.StackItemConfigIDs), encodeConfigIDs(row.InstanceItemConfigIDs),
			row.CurrencyAmount, nowMs,
		); derr != nil {
			return false, ProgressSettleInfo{}, errcode.New(errcode.ErrBattleResultDBWrite, "insert drop outbox match=%d player=%d: %v",
				result.GetMatchId(), row.PlayerID, derr)
		}
	}

	// 正常结算的终态回收证明必须与战绩原子提交。它不是 DS 可填写的业务字段，
	// 调用方已从 Guard + Redis active 的同一服务端快照构造并验证完整性。
	if terminalRelease != nil {
		const insTerminalRelease = `INSERT INTO terminal_release_outbox
(match_id, allocation_id, ds_pod_name, gameserver_uid, instance_epoch,
 auth_gen, auth_jti, auth_exp_ms, auth_kid, auth_token_sha256, auth_writer_epoch,
 authorized_at_ms, release_after_ms, created_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
		if _, terr := tx.ExecContext(ctx, insTerminalRelease,
			result.GetMatchId(), terminalRelease.AllocationID, terminalRelease.DSPodName,
			terminalRelease.GameserverUID, terminalRelease.InstanceEpoch,
			terminalRelease.AuthGen, terminalRelease.AuthJTI, terminalRelease.AuthExpMs,
			terminalRelease.AuthKid, terminalRelease.AuthTokenSHA256, terminalRelease.AuthWriterEpoch,
			terminalRelease.AuthorizedAtMs, terminalRelease.ReleaseAfterMs, nowMs,
		); terr != nil {
			return false, ProgressSettleInfo{}, errcode.New(errcode.ErrBattleResultDBWrite,
				"insert terminal release outbox match=%d allocation=%s: %v",
				result.GetMatchId(), terminalRelease.AllocationID, terr)
		}
	}

	playerIDs := authoritativeRecoveryPlayerIDs(terminalRelease)
	if len(playerIDs) == 0 {
		playerIDs = make([]uint64, 0, len(result.GetStats()))
		for _, stat := range result.GetStats() {
			if stat.GetPlayerId() != 0 {
				playerIDs = append(playerIDs, stat.GetPlayerId())
			}
		}
	}
	if err := insertMatchReleaseOutboxTx(ctx, tx, result.GetMatchId(), playerIDs, nowMs); err != nil {
		return false, ProgressSettleInfo{}, errcode.New(errcode.ErrBattleResultDBWrite,
			"insert match release outbox match=%d: %v", result.GetMatchId(), err)
	}

	if cerr := tx.Commit(); cerr != nil {
		return false, ProgressSettleInfo{}, errcode.New(errcode.ErrBattleResultDBWrite, "commit match=%d: %v", result.GetMatchId(), cerr)
	}
	return false, settleInfo, nil
}

func authoritativeRecoveryPlayerIDs(rec *TerminalReleaseRecord) []uint64 {
	if rec == nil || len(rec.PlayerIDs) == 0 {
		return nil
	}
	return append([]uint64(nil), rec.PlayerIDs...)
}

func loadMatchPlayerIDsTx(ctx context.Context, tx *sql.Tx, matchID uint64) ([]uint64, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT player_id FROM battle_player_stats WHERE match_id = ? ORDER BY player_id ASC`, matchID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var playerIDs []uint64
	for rows.Next() {
		var playerID uint64
		if err := rows.Scan(&playerID); err != nil {
			return nil, err
		}
		playerIDs = append(playerIDs, playerID)
	}
	return playerIDs, rows.Err()
}

func insertMatchReleaseOutboxTx(ctx context.Context, tx *sql.Tx, matchID uint64, playerIDs []uint64, nowMs int64) error {
	record := &matchv1.MatchReleaseStorageRecord{
		OperationId: uuid.NewString(),
		MatchId:     matchID,
		PlayerIds:   playerIDs,
		CreatedAtMs: nowMs,
	}
	payload, err := proto.Marshal(record)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO match_release_outbox
(match_id, payload, next_attempt_at_ms, attempt_count, created_at_ms)
VALUES (?, ?, 0, 0, ?)
ON DUPLICATE KEY UPDATE
  match_id = VALUES(match_id),
  next_attempt_at_ms = LEAST(next_attempt_at_ms, VALUES(next_attempt_at_ms))`, matchID, payload, nowMs)
	return err
}

// FetchOutbox 按 id 升序取最多 limit 条待发布出箱记录(FIFO 保序)。
func (r *MySQLBattleRepo) FetchOutbox(ctx context.Context, limit int) ([]OutboxRecord, error) {
	if limit <= 0 {
		limit = 128
	}
	const q = `SELECT id, player_id, payload FROM player_update_outbox ORDER BY id ASC LIMIT ?`
	rows, err := r.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "query outbox: %v", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]OutboxRecord, 0, limit)
	for rows.Next() {
		var rec OutboxRecord
		if serr := rows.Scan(&rec.ID, &rec.PlayerID, &rec.Payload); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan outbox: %v", serr)
		}
		out = append(out, rec)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate outbox: %v", rerr)
	}
	return out, nil
}

// DeleteOutbox 删除已成功投递的出箱行。
func (r *MySQLBattleRepo) DeleteOutbox(ctx context.Context, id int64) error {
	if _, err := r.db.ExecContext(ctx, `DELETE FROM player_update_outbox WHERE id = ?`, id); err != nil {
		return errcode.New(errcode.ErrInternal, "delete outbox id=%d: %v", id, err)
	}
	return nil
}

// FetchDropOutbox 按 id 升序取最多 limit 条待发放装备掉落出箱记录(W5 ④)。
func (r *MySQLBattleRepo) FetchDropOutbox(ctx context.Context, limit int) ([]DropOutboxRecord, error) {
	if limit <= 0 {
		limit = 128
	}
	const q = `SELECT id, match_id, player_id, item_config_ids, stack_item_config_ids, instance_item_config_ids, currency_amount
FROM battle_drop_outbox ORDER BY id ASC LIMIT ?`
	rows, err := r.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "query drop outbox: %v", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]DropOutboxRecord, 0, limit)
	for rows.Next() {
		var (
			rec         DropOutboxRecord
			allCSV      string
			stackCSV    string
			instanceCSV string
		)
		if serr := rows.Scan(&rec.ID, &rec.MatchID, &rec.PlayerID, &allCSV, &stackCSV, &instanceCSV, &rec.CurrencyAmount); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan drop outbox: %v", serr)
		}
		rec.ItemConfigIDs = decodeConfigIDs(allCSV)
		rec.StackItemConfigIDs = decodeConfigIDs(stackCSV)
		rec.InstanceItemConfigIDs = decodeConfigIDs(instanceCSV)
		out = append(out, rec)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate drop outbox: %v", rerr)
	}
	return out, nil
}

// DeleteDropOutbox 删除已成功发放的掉落出箱行。
func (r *MySQLBattleRepo) DeleteDropOutbox(ctx context.Context, id int64) error {
	if _, err := r.db.ExecContext(ctx, `DELETE FROM battle_drop_outbox WHERE id = ?`, id); err != nil {
		return errcode.New(errcode.ErrInternal, "delete drop outbox id=%d: %v", id, err)
	}
	return nil
}

// FetchTerminalReleaseOutbox 按到期时间/id 取一批待终态回收行。
func (r *MySQLBattleRepo) FetchTerminalReleaseOutbox(ctx context.Context, limit int, nowMs int64) ([]TerminalReleaseRecord, error) {
	if limit <= 0 {
		limit = 128
	}
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}
	const q = `SELECT id, match_id, allocation_id, ds_pod_name, gameserver_uid, instance_epoch,
auth_gen, auth_jti, auth_exp_ms, auth_kid, auth_token_sha256, auth_writer_epoch,
authorized_at_ms, release_after_ms, released_at_ms, created_at_ms
FROM terminal_release_outbox
WHERE release_after_ms <= ?
ORDER BY release_after_ms ASC, id ASC
LIMIT ?`
	rows, err := r.db.QueryContext(ctx, q, nowMs, limit)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "query terminal release outbox: %v", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]TerminalReleaseRecord, 0, limit)
	for rows.Next() {
		var rec TerminalReleaseRecord
		if err := rows.Scan(
			&rec.ID, &rec.MatchID, &rec.AllocationID, &rec.DSPodName, &rec.GameserverUID,
			&rec.InstanceEpoch, &rec.AuthGen, &rec.AuthJTI, &rec.AuthExpMs, &rec.AuthKid,
			&rec.AuthTokenSHA256, &rec.AuthWriterEpoch, &rec.AuthorizedAtMs,
			&rec.ReleaseAfterMs, &rec.ReleasedAtMs, &rec.CreatedAtMs,
		); err != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan terminal release outbox: %v", err)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate terminal release outbox: %v", err)
	}
	return out, nil
}

// MarkTerminalReleaseReleased 持久化阶段1 ACK。它必须发生在 Redis 永久 terminal
// 与 UID-precondition delete 明确成功之后、任何 Redis TTL 恢复之前。
func (r *MySQLBattleRepo) MarkTerminalReleaseReleased(ctx context.Context, id uint64, releasedAtMs int64) (bool, error) {
	if id == 0 || releasedAtMs <= 0 {
		return false, errcode.New(errcode.ErrInvalidArg, "terminal release mark requires id/time")
	}
	res, err := r.db.ExecContext(ctx,
		`UPDATE terminal_release_outbox SET released_at_ms=? WHERE id=? AND released_at_ms=0`,
		releasedAtMs, id)
	if err != nil {
		return false, fmt.Errorf("mark terminal release released: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("mark terminal release rows affected: %w", err)
	}
	return affected == 1, nil
}

// DeleteTerminalReleaseOutbox 是阶段2 finalize 的 ACK。外部回收或 finalize 结果未知时绝不能调用。
func (r *MySQLBattleRepo) DeleteTerminalReleaseOutbox(ctx context.Context, id uint64) error {
	if id == 0 {
		return errcode.New(errcode.ErrInvalidArg, "terminal release outbox id required")
	}
	res, err := r.db.ExecContext(ctx, deleteTerminalReleaseOutboxSQL, id)
	if err != nil {
		return errcode.New(errcode.ErrInternal, "delete terminal release outbox id=%d: %v", id, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return errcode.New(errcode.ErrInternal, "delete terminal release rows affected id=%d: %v", id, err)
	}
	// 0 = 已被并发 worker 删除，或仍是 pending（WHERE 前置条件保护它不被误删）；
	// 两者都按幂等 no-op。PK 保证 >1 是结构/驱动异常，必须 fail-closed。
	if affected > 1 {
		return errcode.New(errcode.ErrInternal, "delete terminal release id=%d affected=%d", id, affected)
	}
	return nil
}

func (r *MySQLBattleRepo) FetchMatchReleaseOutbox(ctx context.Context, limit int, nowMs int64) ([]MatchReleaseRecord, error) {
	if limit <= 0 {
		limit = 128
	}
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}
	rows, err := r.db.QueryContext(ctx, `SELECT id, payload, attempt_count, next_attempt_at_ms, created_at_ms
FROM match_release_outbox
WHERE next_attempt_at_ms <= ?
ORDER BY next_attempt_at_ms ASC, id ASC
LIMIT ?`, nowMs, limit)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "query match release outbox: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := make([]MatchReleaseRecord, 0, limit)
	for rows.Next() {
		var (
			rec     MatchReleaseRecord
			payload []byte
		)
		if err := rows.Scan(&rec.ID, &payload, &rec.AttemptCount, &rec.NextAttemptAtMs, &rec.CreatedAtMs); err != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan match release outbox: %v", err)
		}
		storage := &matchv1.MatchReleaseStorageRecord{}
		if err := proto.Unmarshal(payload, storage); err != nil {
			return nil, errcode.New(errcode.ErrInternal, "decode match release outbox id=%d: %v", rec.ID, err)
		}
		if storage.GetMatchId() == 0 || storage.GetOperationId() == "" {
			return nil, errcode.New(errcode.ErrInternal, "invalid match release outbox id=%d", rec.ID)
		}
		rec.OperationID = storage.GetOperationId()
		rec.MatchID = storage.GetMatchId()
		rec.PlayerIDs = append([]uint64(nil), storage.GetPlayerIds()...)
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate match release outbox: %v", err)
	}
	return out, nil
}

func (r *MySQLBattleRepo) DeferMatchReleaseOutbox(ctx context.Context, id uint64, nextAttemptAtMs int64) error {
	if id == 0 || nextAttemptAtMs <= 0 {
		return errcode.New(errcode.ErrInvalidArg, "match release defer requires id/time")
	}
	res, err := r.db.ExecContext(ctx, `UPDATE match_release_outbox
SET attempt_count = attempt_count + 1, next_attempt_at_ms = ?
WHERE id = ?`, nextAttemptAtMs, id)
	if err != nil {
		return errcode.New(errcode.ErrInternal, "defer match release outbox id=%d: %v", id, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return errcode.New(errcode.ErrInternal, "defer match release rows affected id=%d: %v", id, err)
	}
	if affected > 1 {
		return errcode.New(errcode.ErrInternal, "defer match release id=%d affected=%d", id, affected)
	}
	return nil
}

func (r *MySQLBattleRepo) DeleteMatchReleaseOutbox(ctx context.Context, id uint64) error {
	if id == 0 {
		return errcode.New(errcode.ErrInvalidArg, "match release outbox id required")
	}
	res, err := r.db.ExecContext(ctx, `DELETE FROM match_release_outbox WHERE id = ?`, id)
	if err != nil {
		return errcode.New(errcode.ErrInternal, "delete match release outbox id=%d: %v", id, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return errcode.New(errcode.ErrInternal, "delete match release rows affected id=%d: %v", id, err)
	}
	if affected > 1 {
		return errcode.New(errcode.ErrInternal, "delete match release id=%d affected=%d", id, affected)
	}
	return nil
}

const deleteTerminalReleaseOutboxSQL = `DELETE FROM terminal_release_outbox WHERE id = ? AND released_at_ms > 0`

// encodeConfigIDs 把 item_config_id 列表编码成 CSV(如 "5001,5002"),存 drop 出箱。
func encodeConfigIDs(ids []uint32) string {
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, strconv.FormatUint(uint64(id), 10))
	}
	return strings.Join(parts, ",")
}

// decodeConfigIDs 解析 CSV item_config_id(非法/空段跳过,防御性)。
func decodeConfigIDs(csv string) []uint32 {
	if csv == "" {
		return nil
	}
	parts := strings.Split(csv, ",")
	ids := make([]uint32, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if v, err := strconv.ParseUint(p, 10, 32); err == nil && v != 0 {
			ids = append(ids, uint32(v))
		}
	}
	return ids
}

func (r *MySQLBattleRepo) GetResult(ctx context.Context, matchID uint64) (*battlev1.BattleResult, bool, error) {
	const q = `SELECT started_at_ms, ended_at_ms, winner_team, outcome, ds_pod_name, game_mode, map_id
FROM battles WHERE match_id = ? LIMIT 1`
	var (
		startedAt, endedAt  int64
		winnerTeam, outcome int32
		dsPod, gameMode     string
		mapID               uint32
	)
	err := r.db.QueryRowContext(ctx, q, matchID).Scan(
		&startedAt, &endedAt, &winnerTeam, &outcome, &dsPod, &gameMode, &mapID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errcode.New(errcode.ErrInternal, "query battle match=%d: %v", matchID, err)
	}

	stats, err := r.loadStats(ctx, matchID)
	if err != nil {
		return nil, false, err
	}
	return &battlev1.BattleResult{
		MatchId:     matchID,
		StartedAtMs: startedAt,
		EndedAtMs:   endedAt,
		WinnerTeam:  winnerTeam,
		Outcome:     battlev1.BattleOutcome(outcome),
		DsPodName:   dsPod,
		GameMode:    gameMode,
		MapId:       mapID,
		Stats:       stats,
	}, true, nil
}

func (r *MySQLBattleRepo) ListPlayerHistory(ctx context.Context, playerID uint64, limit int, beforeMs int64) ([]*battlev1.BattleResult, error) {
	if limit <= 0 {
		limit = 20
	}
	// 先取玩家参与的 match_id(按结束时间倒序,游标分页),再逐场 load 完整结算。
	const q = `SELECT b.match_id FROM battle_player_stats s
JOIN battles b ON b.match_id = s.match_id
WHERE s.player_id = ? AND (? <= 0 OR b.ended_at_ms < ?)
ORDER BY b.ended_at_ms DESC
LIMIT ?`
	rows, err := r.db.QueryContext(ctx, q, playerID, beforeMs, beforeMs, limit)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "query history player=%d: %v", playerID, err)
	}
	defer func() { _ = rows.Close() }()

	matchIDs := make([]uint64, 0, limit)
	for rows.Next() {
		var mid uint64
		if serr := rows.Scan(&mid); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan history player=%d: %v", playerID, serr)
		}
		matchIDs = append(matchIDs, mid)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate history player=%d: %v", playerID, rerr)
	}

	out := make([]*battlev1.BattleResult, 0, len(matchIDs))
	for _, mid := range matchIDs {
		res, found, gerr := r.GetResult(ctx, mid)
		if gerr != nil {
			return nil, gerr
		}
		if found {
			out = append(out, res)
		}
	}
	return out, nil
}

// loadStats 读一场对局的全部玩家战绩。
func (r *MySQLBattleRepo) loadStats(ctx context.Context, matchID uint64) ([]*battlev1.PlayerStats, error) {
	const q = `SELECT player_id, hero_id, team, kills, deaths, assists,
damage_dealt, damage_taken, healing, gold, mmr_delta
FROM battle_player_stats WHERE match_id = ? ORDER BY team, player_id`
	rows, err := r.db.QueryContext(ctx, q, matchID)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "query stats match=%d: %v", matchID, err)
	}
	defer func() { _ = rows.Close() }()

	var stats []*battlev1.PlayerStats
	for rows.Next() {
		s := &battlev1.PlayerStats{}
		if serr := rows.Scan(
			&s.PlayerId, &s.HeroId, &s.Team, &s.Kills, &s.Deaths, &s.Assists,
			&s.DamageDealt, &s.DamageTaken, &s.Healing, &s.Gold, &s.MmrDelta,
		); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan stats match=%d: %v", matchID, serr)
		}
		stats = append(stats, s)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate stats match=%d: %v", matchID, rerr)
	}
	return stats, nil
}

// isDupErr 判断是否 MySQL 唯一键冲突(1062 ER_DUP_ENTRY)。
// 字符串匹配避免强依赖 driver 错误类型(对齐 login data/account.go)。
func isDupErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "1062") || strings.Contains(msg, "Duplicate entry")
}

// ── 保留期清理(CLAUDE.md §9 不变量 24)────────────────────────────────────────
//
// 多副本各自跑,无锁:SELECT 候选 → 同事务批删,并发副本重复选中只多花空删,幂等。
// 单批 batch 场有界,积压跨轮摊平,不长事务锁表。

// selectMatchIDs 按查询取一批 match_id(保留期清理候选)。
func (r *MySQLBattleRepo) selectMatchIDs(ctx context.Context, q string, args ...any) ([]uint64, error) {
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "select purge candidates: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []uint64
	for rows.Next() {
		var id uint64
		if serr := rows.Scan(&id); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan purge candidate: %v", serr)
		}
		ids = append(ids, id)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate purge candidates: %v", rerr)
	}
	return ids, nil
}

// deleteByMatchIDsTx 在一个事务里按 match_id 列表依次清多张表(子表在前,主表最后)。
func (r *MySQLBattleRepo) deleteByMatchIDsTx(ctx context.Context, tables []string, ids []uint64) error {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return errcode.New(errcode.ErrInternal, "begin purge tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, table := range tables {
		if _, derr := tx.ExecContext(ctx,
			`DELETE FROM `+table+` WHERE match_id IN (`+placeholders+`)`, args...); derr != nil {
			return errcode.New(errcode.ErrInternal, "purge %s: %v", table, derr)
		}
	}
	if cerr := tx.Commit(); cerr != nil {
		return errcode.New(errcode.ErrInternal, "commit purge tx: %v", cerr)
	}
	return nil
}

// 清理条件常量:**Count(report-only)与 SELECT 候选(delete)共用同一 where**,
// 条件只写一遍,杜绝"报告的条件"与"实删的条件"漂移(见 pkg/dbguard/sweep.go 头注释)。
const (
	// expiredBattlesWhere 依据服务端落库时间 created_at(§9.6 数值不信 DS;走 idx_created)。
	// 行龄 = 结算落库距今,比 ended_at 晚一个对局时长,相对 180 天保留期误差可忽略,
	// 且偏保守方向(只会晚删,不会早删)。
	expiredBattlesWhere = "created_at < FROM_UNIXTIME(? / 1000)"
	// settledProgressWhere 依据服务端结算事务打的 settled_at_ms(settleProgressStreamTx);走 idx_settled。
	settledProgressWhere = "settled_at_ms > 0 AND settled_at_ms < ?"
)

// sweepByMatchID 是多表按 match_id 成组清理的统一实现(report-only 只数不删)。
//
// 多表事务清理用不了 dbguard.SweepTable(那是单条 DELETE),但告警口径必须一致,
// 所以 report-only 走 dbguard.ReportPending、delete 走 dbguard.ReportDeleted。
// unit 是 "matches":这里删的是"一组行"(一场对局的主表+子表),不是"一行"。
func (r *MySQLBattleRepo) sweepByMatchID(
	ctx context.Context, mode dbguard.Mode,
	anchorTable, where string, deleteTables []string,
	cutoffMs int64, batch int,
) (dbguard.Outcome, error) {
	out := dbguard.Outcome{Mode: mode}

	if mode != dbguard.ModeDelete {
		// report-only:只数满足条件的对局数(不受 batch 截断,给真实积压规模),不动数据。
		var n int64
		if err := r.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM `"+anchorTable+"` WHERE "+where, cutoffMs).Scan(&n); err != nil {
			return out, errcode.New(errcode.ErrInternal, "count purgeable %s: %v", anchorTable, err)
		}
		out.Matched = n
		dbguard.ReportPending(ctx, "pandora_battle", anchorTable, "matches", n)
		return out, nil
	}

	ids, err := r.selectMatchIDs(ctx,
		"SELECT match_id FROM `"+anchorTable+"` WHERE "+where+" LIMIT ?", cutoffMs, batch)
	if err != nil || len(ids) == 0 {
		return out, err
	}
	if derr := r.deleteByMatchIDsTx(ctx, deleteTables, ids); derr != nil {
		return out, derr
	}
	out.Matched, out.Deleted = int64(len(ids)), int64(len(ids))
	out.Truncated = batch > 0 && len(ids) >= batch
	dbguard.ReportDeleted(ctx, "pandora_battle", anchorTable, "matches", out.Deleted)
	return out, nil
}

// SweepExpiredBattles 处理超保留期的对局(battles + battle_player_stats 同事务成组)。
// mode 由 conf 决定:battle_result 默认 ModeDelete(战报只留六个月,2026-08-03 用户指令);
// 显式配 report_only 时只统计待清理对局数并 WARN 告警,一行都不删。
func (r *MySQLBattleRepo) SweepExpiredBattles(ctx context.Context, mode dbguard.Mode, cutoffMs int64, batch int) (dbguard.Outcome, error) {
	return r.sweepByMatchID(ctx, mode, "battles", expiredBattlesWhere,
		[]string{"battle_player_stats", "battles"}, cutoffMs, batch)
}

// SweepSettledProgress 处理已结算且超保留期的进度水位(stream + player 同事务成组)。
// mode 同上(默认真删);未结算行无论如何都不在处理范围
// (陈年未结算 = 补偿链 bug 证据,另有 CountStaleUnsettledProgress 持续告警)。
func (r *MySQLBattleRepo) SweepSettledProgress(ctx context.Context, mode dbguard.Mode, cutoffMs int64, batch int) (dbguard.Outcome, error) {
	return r.sweepByMatchID(ctx, mode, "battle_progress_stream", settledProgressWhere,
		[]string{"battle_progress_outbox", "battle_progress_action", "battle_progress_item_balance", "battle_progress_player", "battle_progress_stream"}, cutoffMs, batch)
}

func (r *MySQLBattleRepo) CountStaleUnsettledProgress(ctx context.Context, cutoffMs int64) (int64, error) {
	// settled_at_ms=0 走 idx_settled 前缀,未结算行本就该是极少数(结算链正常时趋零)。
	var n int64
	if err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM battle_progress_stream WHERE settled_at_ms = 0 AND updated_at_ms < ?`,
		cutoffMs).Scan(&n); err != nil {
		return 0, errcode.New(errcode.ErrInternal, "count stale unsettled progress: %v", err)
	}
	return n, nil
}
