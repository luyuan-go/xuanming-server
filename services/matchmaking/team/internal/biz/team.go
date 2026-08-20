// Package biz 是 team 服务的业务逻辑层(W3 ⑦ Phase 3,2026-06-05)。
//
// 设计原则(协议铁律 4 原则):
//  1. 立即完成型:7 个 RPC 在 biz 内完成状态机迁移 + redis 写 + kafka push 后立即返回
//  2. push 不发 caller:PushTeamUpdate callerPlayerID != 0 时不发给发起者自身
//  3. kafka key = player_id(不变量 §9):PushToPlayers 已保证
//  4. WATCH/MULTI/EXEC 乐观锁:所有写路径走 UpdateWithLock,冲突重试 OptimisticRetry 次
//
// 状态机合法迁移(见 proto/pandora/team/v1/team.proto):
//
//	FORMING  → READY(全员 ready)
//	READY    → FORMING(任一成员 leave/kick)
//	DISBANDED → 任何写操作都拒绝(ErrTeamWrongState)
package biz

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"

	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"

	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/conf"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/data"
)

// TeamEventPusher 是 kafka push 的抽象接口。
// 实现由 main 装配时注入(kafkax.KeyOrderedProducer.PushToPlayers 包装)。
type TeamEventPusher interface {
	// PushTeamUpdate 向 toPlayerIDs 广播队伍变更事件字节(不发给 callerPlayerID)。
	// payload 是 proto.Marshal(teamv1.TeamUpdateEvent) 的结果,event_type 缺省 0(旧事件)。
	PushTeamUpdate(ctx context.Context, callerPlayerID uint64, toPlayerIDs []uint64, payload []byte) (sent int, err error)

	// PushTeamEvent 与 PushTeamUpdate 相同,额外指定 push 域内事件类型判别键 event_type
	// (填入 PushFrame.event_type),供客户端按 (topic, event_type) 定位反序列化哪个 message。
	// eventType=0 时等价 PushTeamUpdate。用于邀请等「payload 是各自独立 proto」的专属事件。
	PushTeamEvent(ctx context.Context, callerPlayerID uint64, toPlayerIDs []uint64, payload []byte, eventType uint32) (sent int, err error)
}

// MatchCanceler 是“离队/踢人 → 撤销 matchmaker 匹配票据”联动的抽象接口。
// 实现由 main 装配时注入(data.GrpcMatchClient,直连 matchmaker 内网 gRPC)。
// 可为 nil:本机不起 matchmaker 的骨架联调 / 未配 matchmaker_addr 时跳过联动。
type MatchCanceler interface {
	// CancelMatch 撤销 playerID 当前所在的匹配票据(整张票据,含全体队友)。
	// 玩家未在排队时返回 ErrMatchNotFound(4004),调用方按常态忽略。
	CancelMatch(ctx context.Context, playerID uint64) error
}

// MatchCommitmentReader 是“入队 / 列表复核前确认这支队伍没有被一场对局占住”的抽象接口。
// 实现由 main 装配时注入(data.GrpcMatchClient,与 MatchCanceler 同一条 matchmaker 连接)。
//
// 为什么不看 team.State:`TEAM_STATE_MATCHING` / `TEAM_STATE_IN_BATTLE` 在本服务
// **没有任何写入点**(全仓只有 stateMatching / stateInBattle 两行常量定义),队伍在排队、
// 确认期、拉 DS、整场战斗期间 State 都停在 READY —— 光看 State 分不出“全员点了准备”
// 和“已经在打了”。唯一权威是 matchmaker 的 player→ticket claim(无 TTL SETNX,只有显式
// 取消 / 失败 / battle_result 调 ReleaseMatch 才删),ResolvePlayerMatchContext 读的就是它。
type MatchCommitmentReader interface {
	// IsPlayerCommittedToMatch 返回 playerID 当前是否已被一场对局占住
	// (STARTING / 排队 / 确认期 / 拉 DS / 战斗中,直到本局释放为止)。
	// 读取不确定(RPC 失败 / 权威返回 UNKNOWN)时返回 error,调用方必须 fail-closed。
	IsPlayerCommittedToMatch(ctx context.Context, playerID uint64) (bool, error)
}

// PlayerNoResolver 是 team 组装客户端可见快照时读取 login/account 权威角色编号的窄端口。
// 返回值以 player_id 为键；player_no 只用于展示，绝不能参与队伍身份或成员关系判断。
// 实现必须把整支队伍一次批量解析，禁止按成员 N+1 调用。
type PlayerNoResolver interface {
	ResolvePlayerNos(ctx context.Context, playerIDs []uint64) (map[uint64]uint64, error)
}

// PlayerNameResolver 是 team 组装客户端可见快照时读取 player 权威角色名的窄端口。
// 返回值以 player_id 为键；实现必须整队单批读取，禁止按成员 N+1 调用。
type PlayerNameResolver interface {
	ResolvePlayerNames(ctx context.Context, playerIDs []uint64) (map[uint64]string, error)
}

// ── 常量 ─────────────────────────────────────────────────────────────────────

const (
	stateForming   = teamv1.TeamState_TEAM_STATE_FORMING
	stateReady     = teamv1.TeamState_TEAM_STATE_READY
	stateMatching  = teamv1.TeamState_TEAM_STATE_MATCHING
	stateInBattle  = teamv1.TeamState_TEAM_STATE_IN_BATTLE
	stateDisbanded = teamv1.TeamState_TEAM_STATE_DISBANDED
)

// ── TeamUsecase ───────────────────────────────────────────────────────────────

// TeamUsecase 是 team 业务逻辑的核心。
type TeamUsecase struct {
	repo   data.TeamRepo
	pusher TeamEventPusher
	cfg    conf.TeamConf

	// router 是确定性 region/cell 路由器(scale-cellular-20m.md §4.2)。
	// 可为 nil:单 Cell / dev / 阶段 1~2 不分片,队伍 region 分布观测退化为不打日志(行为不变)。
	// 分片部署时由 main 经 SetCellRouter 注入,成员变更(建队 / 入队)后额外打一条队伍
	// 跨 region 组队观测(供撮合 / battle 放置评估跨 region 组队占比)。nil-safe。
	router *cellroute.Router

	// matchCanceler 是“离队/踢人 → 撤销 matchmaker 票据”联动。可为 nil(未配
	// matchmaker_addr / 骨架联调)→ 不联动,行为与历史一致。nil-safe。
	matchCanceler MatchCanceler

	// rateQuota 申请/邀请频率配额(anti-abuse §6 第 6 项)。可为 nil(不限)。
	rateQuota ActionRateQuota

	// matchCommitment 是“入队闸门”:队伍已提交对局时拒绝新人进来。可为 nil(未配
	// matchmaker_addr / 骨架联调)→ 跳过闸门;那种部署下根本没有匹配链路,不存在
	// 被对局占住的队伍,与 matchCanceler 的弱依赖口径一致。nil-safe。
	matchCommitment MatchCommitmentReader

	// playerNos 是客户端展示投影的弱依赖。nil / 查询失败时 player_no 保持 0，
	// 队伍 Redis 权威读写与 RPC 主流程不受影响。
	playerNos PlayerNoResolver

	// playerNames 是客户端展示投影的弱依赖。nil / 查询失败 / 缺行时 nickname 保持空；
	// 绝不回退 TeamMemberStorageRecord.nickname，避免把历史影子或伪造输入重新暴露给客户端。
	playerNames PlayerNameResolver

	// presence 是「离线成员自动退队」的读路径兜底入口(见 offline_leave.go)。
	// 可为 nil(功能关 / 未配 locator_addr)→ 读路径不做任何额外查询,行为与历史一致。nil-safe。
	presence PresenceInspector

	// lastTouch 记录每个玩家上次 GetMyTeam 续期队伍 TTL 的时刻(节流,避免每次
	// 轮询都敲 Redis EXPIRE)。key=playerID(uint64) value=time.Time。
	// 多实例部署下各实例独立节流,最坏情况多几次 EXPIRE,无正确性影响。
	// 内存上限:maybeSweepLastTouch 每 touchInterval 清一次过期条目(见下),
	// 常驻规模 ≈ 最近 2×touchInterval 内轮询过 GetMyTeam 的活跃玩家数,不随 DAU 永久增长。
	lastTouch sync.Map

	// lastTouchSweepAtNs 是上次清扫 lastTouch 的时刻(UnixNano)。CAS 抢占保证
	// 同一时刻至多一个 goroutine 执行清扫,其余直接返回。
	lastTouchSweepAtNs atomic.Int64
}

// NewTeamUsecase 构造 TeamUsecase。
func NewTeamUsecase(repo data.TeamRepo, pusher TeamEventPusher, cfg conf.TeamConf) *TeamUsecase {
	return &TeamUsecase{repo: repo, pusher: pusher, cfg: cfg}
}

// SetCellRouter 注入确定性 region/cell 路由器(scale-cellular-20m.md §4.2 两级架构)。
//
// nil-safe:不调用 / 传 nil 时(单 Cell / dev / 阶段 1~2),不做队伍 region 分布观测,行为与历史
// 一致。用 setter 而非构造参数,避免单 Cell 阶段调用点被迫改签名(与 matchmaker / auction /
// battle_result / friend / chat / trade / dialogue / inventory / locator / push 一致)。Router 内部读路径无锁,并发安全。
func (u *TeamUsecase) SetCellRouter(r *cellroute.Router) {
	u.router = r
}

// ActionRateQuota 申请/邀请的 per-player 频率配额(anti-abuse §6 第 6 项;
// 实现 pkg/redisx.ActionQuota)。总量闸(§9.18 pending 上限)只限「同时挂多少」,
// 挡不住「加满 → 全撤 → 再加满」的写放大循环,频率配额补这一维。
// 背压非权威门:判定 error 由调用方 fail-open 放行。
type ActionRateQuota interface {
	Allow(ctx context.Context, action string, subject uint64) (bool, error)
}

// SetRateQuota 注入频率配额(可选;不注入 = 不限,dev 联调兼容)。
func (u *TeamUsecase) SetRateQuota(q ActionRateQuota) {
	u.rateQuota = q
}

// allowAction 频率配额门:窗内超额返回 ErrRateLimited(先于一切副作用);
// quota 未注入 / 判定失败(Redis 抖动)一律放行(fail-open,anti-abuse §2 铁律)。
func (u *TeamUsecase) allowAction(ctx context.Context, action string, playerID uint64) error {
	if u.rateQuota == nil {
		return nil
	}
	ok, err := u.rateQuota.Allow(ctx, action, playerID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "team_rate_quota_check_failed",
			"reason", reasonRateQuotaUnknown,
			"action", action, "player_id", playerID, "err", err,
			"fail_open", true)
		return nil
	}
	if !ok {
		plog.With(ctx).Warnw("msg", "team_rate_quota_rejected",
			"reason", reasonRateLimited,
			"action", action, "player_id", playerID)
		return errcode.New(errcode.ErrRateLimited, "team %s rate limited, retry later", action)
	}
	return nil
}

// SetMatchCanceler 注入“离队/踢人 → 撤销 matchmaker 匹配票据”联动。
//
// nil-safe:不调用 / 传 nil 时(未配 matchmaker_addr / 骨架联调),离队不联动撤票,
// 行为与历史一致。用 setter 而非构造参数,避免现有调用点/测试被迫改签名(与
// SetCellRouter 一致)。
func (u *TeamUsecase) SetMatchCanceler(c MatchCanceler) {
	u.matchCanceler = c
}

// SetMatchCommitmentReader 注入“入队闸门”所需的 matchmaker 权威读取。
//
// nil-safe:不调用 / 传 nil 时(未配 matchmaker_addr / 骨架联调)不做闸门校验,
// 行为与历史一致。用 setter 而非构造参数,与 SetMatchCanceler 一致。
func (u *TeamUsecase) SetMatchCommitmentReader(r MatchCommitmentReader) {
	u.matchCommitment = r
}

// SetPlayerNoResolver 注入 login/account 权威角色编号批量读取端口。
// nil-safe：未配置时客户端快照中的 player_no 为 0，不阻断组队核心操作。
func (u *TeamUsecase) SetPlayerNoResolver(r PlayerNoResolver) {
	u.playerNos = r
}

// SetPlayerNameResolver 注入 player 权威角色名批量读取端口。
// nil-safe：未配置时客户端快照中的 nickname 为空，不阻断组队核心操作。
func (u *TeamUsecase) SetPlayerNameResolver(r PlayerNameResolver) {
	u.playerNames = r
}

// InviteTTLMs 返回邀请令牌 TTL 的毫秒数,供 service 层计算 expires_at_ms。
func (u *TeamUsecase) InviteTTLMs() int64 {
	return u.cfg.InviteTTL.Std().Milliseconds()
}

// activeTTL 返回活跃队伍 Redis key 的生命周期。
func (u *TeamUsecase) activeTTL() time.Duration {
	return u.cfg.ActiveTTL.Std()
}

// touchInterval 是 GetMyTeam 在线续期的节流间隔:同一玩家至多每 15 分钟续一次。
// 客户端轮询周期(秒级)远小于它,active_ttl(60 分钟)远大于它,续期不会断流。
const touchInterval = 15 * time.Minute

// maybeTouchTeam 在线心跳保活:玩家仍在轮询自己的队伍 → 续期队伍与索引 TTL,
// 避免在线队伍被 active_ttl 误回收;停止轮询后 TTL 自然到期,僵尸队伍 GC 仍在。
// 15 分钟节流 + best-effort:失败只告警,不影响读返回。
//
// 同时刷新开放队伍索引的 score:索引成员的 score 是"索引项最晚存活时刻",与队伍 key 的
// TTL 同源。只续队伍 key 而不续索引,会让一支持续在线、持续招募的队伍在 active_ttl 后
// 被 ZREMRANGEBYSCORE 当成过期项清掉,从"获取队伍"列表里静默消失(队伍还活着但没人找得到)。
// 节流间隔(15min)远小于 active_ttl(60min),续期不会断流。
func (u *TeamUsecase) maybeTouchTeam(ctx context.Context, team *teamv1.TeamStorageRecord, playerID uint64) {
	now := time.Now()
	if v, ok := u.lastTouch.Load(playerID); ok {
		if last, ok2 := v.(time.Time); ok2 && now.Sub(last) < touchInterval {
			return
		}
	}
	u.lastTouch.Store(playerID, now)
	u.maybeSweepLastTouch(now)
	if err := u.repo.TouchTeam(ctx, team.GetTeamId(), playerID, u.activeTTL()); err != nil {
		plog.With(ctx).Warnw("msg", "team_touch_failed",
			"reason", reasonStoreWriteFailed,
			"player_id", playerID, "team_id", team.GetTeamId(), "err", err,
			"hint", "队伍与开放索引未续期,持续失败会让在线队伍被 active_ttl 误回收")
	}
	u.syncOpenIndex(ctx, team, team.GetMapId())
}

// maybeSweepLastTouch 惰性清扫 lastTouch 里已过节流窗口的条目,防止长跑进程内存
// 随历史活跃玩家数无界增长。清扫间隔 = touchInterval;删除“距上次续期 ≥ touchInterval”
// 的条目与直接不存在等价(下次 Load 反正会放行续期),行为不变。CAS 抢占单 goroutine
// 执行,Range 全量扫描 O(活跃玩家数),每 15 分钟一次可忽略。
func (u *TeamUsecase) maybeSweepLastTouch(now time.Time) {
	last := u.lastTouchSweepAtNs.Load()
	if now.UnixNano()-last < int64(touchInterval) {
		return
	}
	if !u.lastTouchSweepAtNs.CompareAndSwap(last, now.UnixNano()) {
		return // 其他 goroutine 已在清扫
	}
	u.lastTouch.Range(func(k, v any) bool {
		if t, ok := v.(time.Time); !ok || now.Sub(t) >= touchInterval {
			u.lastTouch.Delete(k)
		}
		return true
	})
}

// ── 9 RPC ──────────────────────────────────────────────────────────────────

// CreateTeam 创建队伍,playerID 为队长。
// 前置条件:playerID 不在任何队伍中。
//
// 写序铁律:**先写队伍主体,后 ClaimPlayer 声明归属**。主体先落地时索引尚未指向它
// (teamID 是 Snowflake 新发,返回前无人可见),故不存在「索引已指向、主体还没写」的
// in-flight 窗口 —— 这是 claimPlayerHealingOrphan 把「主体不存在」判为真孤儿的安全前提。
// 若倒过来先 claim 后写主体,并发的 heal 会把 in-flight claim 误判孤儿并 CAS 删掉,
// 同一玩家可能同时出现在两支队伍(违反不变量 §1)。
// claim 失败 → 回滚删掉自己刚写的主体;中途崩溃残留的无主主体(无索引指向)由 TTL 自然回收。
func (u *TeamUsecase) CreateTeam(ctx context.Context, teamID, playerID uint64) (*teamv1.TeamStorageRecord, error) {
	ttl := u.activeTTL()

	now := time.Now().UnixMilli()
	team := &teamv1.TeamStorageRecord{
		TeamId:      teamID,
		CaptainId:   playerID,
		State:       stateForming,
		Members:     []*teamv1.TeamMemberStorageRecord{{PlayerId: playerID}},
		CreatedAtMs: now,
		UpdatedAtMs: now,
		MaxSize:     int32(u.cfg.MaxMembers),
	}

	// 1. 先写队伍主体(此时索引不指向它,对全世界不可见)。
	if err := u.repo.Create(ctx, team, ttl); err != nil {
		plog.With(ctx).Warnw("msg", "team_create_rejected",
			"reason", reasonStoreWriteFailed,
			"team_id", teamID, "captain_id", playerID, "err", err)
		return nil, err
	}

	// 2. 原子声明玩家归属(SETNX),保证不变量 §1:一人只能在一个队。
	//    孤儿索引(索引指向的队伍主体已过期/解散)会自愈,不误拦成 3004。
	if err := u.claimPlayerHealingOrphan(ctx, playerID, teamID, ttl); err != nil {
		// 声明失败(玩家真在其他队) → 回滚删掉自己刚写的主体,避免残留无主队伍。
		_ = u.repo.DeleteTeam(ctx, teamID)
		// claimPlayerHealingOrphan 已打了带具体 reason 的拒绝日志(冲突 / 存储失败),
		// 这里只补一条「建队因此整体失败且已回滚」的收尾,避免只看到一个孤零零的
		// team_claim_conflict 却不知道主体有没有残留。
		plog.With(ctx).Warnw("msg", "team_create_rejected",
			"reason", reasonAlreadyInTeam,
			"team_id", teamID, "captain_id", playerID,
			"rolled_back", true, "err", err)
		return nil, err
	}

	// 3. push 给队长自己(创建者收到快照确认)
	u.pushUpdate(ctx, 0, []uint64{playerID}, team,
		teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_JOINED, 0)

	// 新建队伍只有队长一人、状态 FORMING → 立即进入"开放招募"索引,让别人能找到它。
	// prevMapID 与 MapId 同为 0(新建队尚未选图),不产生换桶操作。
	u.syncOpenIndex(ctx, team, team.MapId)

	// R1:建队是链路第一个不可逆推进,线上默认 info 级必须能看到(原为 Debug =
	// 线上一条都不出,"玩家说他建不了队"只能靠猜)。
	plog.With(ctx).Infow("msg", "team_created", "team_id", teamID, "captain_id", playerID,
		"state", team.State, "max_size", team.MaxSize)
	logTeamStateChanged(ctx, teamID, teamv1.TeamState_TEAM_STATE_UNSPECIFIED, team.State,
		"create", team.GetReadyGeneration(), len(team.Members))
	// 分片:队伍锁定队长 owner cell(TeamShardKey=captain_id);新建队仅队长一人,region 分布
	// 为单一,但统一打点便于后续成员加入后对比。router 为 nil(单 Cell)→ 不打。
	u.logTeamComposition(ctx, team)
	return team, nil
}

// Invite 邀请目标玩家加入队伍。inviterID 必须在该队伍中。
func (u *TeamUsecase) Invite(ctx context.Context, inviteID, teamID, inviterID, targetPlayerID uint64) (*teamv1.TeamStorageRecord, error) {
	// 频率配额(anti-abuse §6 第 6 项):按发起方计,先于一切读写。
	if err := u.allowAction(ctx, "invite", inviterID); err != nil {
		return nil, err
	}
	team, found, err := u.repo.Get(ctx, teamID)
	if err != nil {
		u.logInviteRejected(ctx, reasonStoreReadFailed, teamID, inviterID, targetPlayerID, inviteID, err)
		return nil, err
	}
	if !found {
		u.logInviteRejected(ctx, reasonTeamNotFound, teamID, inviterID, targetPlayerID, inviteID, nil)
		return nil, errcode.New(errcode.ErrTeamNotFound, "team %d not found", teamID)
	}
	if team.State == stateDisbanded {
		u.logInviteRejected(ctx, reasonTeamDisbanded, teamID, inviterID, targetPlayerID, inviteID, nil)
		return nil, errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
	}
	if !hasMember(team, inviterID) {
		u.logInviteRejected(ctx, reasonInviterNotMember, teamID, inviterID, targetPlayerID, inviteID, nil)
		return nil, errcode.New(errcode.ErrTeamNotFound, "player %d not in team %d", inviterID, teamID)
	}
	if len(team.Members) >= int(team.MaxSize) {
		u.logInviteRejected(ctx, reasonTeamFull, teamID, inviterID, targetPlayerID, inviteID, nil)
		return nil, errcode.New(errcode.ErrTeamFull, "team %d is full (%d/%d)", teamID, len(team.Members), team.MaxSize)
	}

	// 存储邀请令牌。同一被邀请人 pending 邀请数超 MaxPendingInvites 时返
	// ErrTeamInvitePendingLimit(3008)——不变量 §9-18 写入侧上限,限流+占位在
	// data 层 Lua 内原子完成。
	if err := u.repo.SetInvite(ctx, inviteID, teamID, inviterID, targetPlayerID, u.cfg.InviteTTL.Std(), u.cfg.MaxPendingInvites); err != nil {
		// 这里同时收敛「被邀请人 pending 上限已满(3008)」与「Redis 写失败」两种,
		// 用错误码把它们区分开:上限是产品语义的拒绝,写失败是故障。
		reason := reasonInviteStoreFailed
		if errcode.As(err) == errcode.ErrTeamInvitePendingLimit {
			reason = reasonInvitePendingLimit
		}
		u.logInviteRejected(ctx, reason, teamID, inviterID, targetPlayerID, inviteID, err)
		return nil, err
	}

	// push 邀请给 target(不发给 inviter — 原则 2)。
	// 邀请是「payload 各自独立 proto」的专属事件。老客户端只认 TeamUpdateEvent(reason=INVITE_SENT),
	// 新客户端只认独立 TeamInviteEvent(已不再从 TeamUpdateEvent 读邀请)。灰度共存期靠"双发"喂饱两代:
	//   - dual(默认):两条都发。老客户端 legacy 弹框、把独立事件误解成 TeamUpdateEvent→护栏(InviteId/
	//     TeamId>0)不过→只多一次无害快照不误弹;新客户端忽略 legacy、只在独立事件弹框。各弹一次不双弹。
	//   - dedicated:只发独立事件(全量铺完新客户端后用)。
	//   - legacy:只发旧事件(回退用)。
	switch u.cfg.InvitePushMode {
	case "legacy":
		// 回退模式只发旧 TeamUpdateEvent,继续服务尚未识别 event_type 的客户端。
		u.pushUpdate(ctx, inviterID, []uint64{targetPlayerID}, team,
			teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_INVITE_SENT, inviteID)
	case "dedicated":
		// 新客户端全量铺开后只发精简的独立邀请事件,停止冗余旧快照。
		u.pushInvite(ctx, inviterID, targetPlayerID, teamID, inviteID)
	default: // "dual"(含空串):共存期双发,金丝雀安全
		// 金丝雀共存期同时照顾新旧客户端;两代各认一种 payload,不会重复弹邀请框。
		u.pushInvite(ctx, inviterID, targetPlayerID, teamID, inviteID)
		u.pushUpdate(ctx, inviterID, []uint64{targetPlayerID}, team,
			teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_INVITE_SENT, inviteID)
	}

	// R1:邀请令牌已落 Redis(不可逆),且它是"入队"链路的起点。玩家操作级频率,
	// 升 INFO 才能在"我明明发了邀请他没收到"时区分「没发出去」与「推送丢帧」。
	plog.With(ctx).Infow("msg", "team_invite_sent",
		"team_id", teamID, "inviter_id", inviterID,
		"target_player_id", targetPlayerID, "invite_id", inviteID,
		"push_mode", u.cfg.InvitePushMode, "members", len(team.Members))
	return team, nil
}

// logInviteRejected 收口 Invite 的全部拒绝分支(§11.3 R2)。
//
// 单独一个助手而不是逐处写 Warnw:邀请有 6 个互不相同的拒绝原因,分散写迟早出现
// 字段名不一致(team/team_id、target/target_player_id),看板就聚合不起来。
func (u *TeamUsecase) logInviteRejected(
	ctx context.Context, reason string,
	teamID, inviterID, targetPlayerID, inviteID uint64, err error,
) {
	plog.With(ctx).Warnw("msg", "team_invite_rejected",
		"reason", reason,
		"team_id", teamID, "inviter_id", inviterID,
		"target_player_id", targetPlayerID, "invite_id", inviteID, "err", err)
}

// logAcceptInviteRejected 收口 AcceptInvite 令牌校验的拒绝分支(§11.3 R2)。
//
// 这四种情况在协议上是同一个错误码(3007 邀请已失效),客户端提示也一样;
// 只有 reason 能分出"令牌真过期了"/"点了发给别人的邀请"/"令牌与队伍对不上"/"Redis 读失败"。
func (u *TeamUsecase) logAcceptInviteRejected(
	ctx context.Context, reason string, teamID, playerID, inviteID uint64, err error,
) {
	plog.With(ctx).Warnw("msg", "team_accept_invite_rejected",
		"reason", reason,
		"team_id", teamID, "player_id", playerID, "invite_id", inviteID, "err", err)
}

// AcceptInvite 目标玩家接受邀请加入队伍。
func (u *TeamUsecase) AcceptInvite(ctx context.Context, inviteID, teamID, playerID uint64) (*teamv1.TeamStorageRecord, error) {
	// 1. 若提供 inviteID,校验令牌
	if inviteID != 0 {
		inv, found, err := u.repo.GetInvite(ctx, inviteID)
		if err != nil {
			u.logAcceptInviteRejected(ctx, reasonInviteLookupFailed, teamID, playerID, inviteID, err)
			return nil, err
		}
		// 三个分支都回同一个 3007/ErrTeamInviteExpired,客户端只看到"邀请已失效" ——
		// 拆成三个 reason 才分得清「令牌真的过期了」「点了发给别人的邀请」「令牌与队伍对不上」。
		if !found {
			u.logAcceptInviteRejected(ctx, reasonInviteNotFound, teamID, playerID, inviteID, nil)
			return nil, errcode.New(errcode.ErrTeamInviteExpired, "invite %d expired or not found", inviteID)
		}
		if inv.TargetPlayerID != playerID {
			u.logAcceptInviteRejected(ctx, reasonInviteTargetMismatch, teamID, playerID, inviteID, nil)
			return nil, errcode.New(errcode.ErrTeamInviteExpired, "invite %d target mismatch", inviteID)
		}
		if inv.TeamID != teamID {
			u.logAcceptInviteRejected(ctx, reasonInviteTeamMismatch, teamID, playerID, inviteID, nil)
			return nil, errcode.New(errcode.ErrTeamInviteExpired, "invite %d team mismatch", inviteID)
		}
	}

	// 2. 走共用入队事务(内部先 ClaimPlayer 保不变量 §1,再改成员表)。
	//    joinTeam 内部已按分支打了带 reason 的拒绝日志,这里不重复。
	result, err := u.joinTeam(ctx, teamID, playerID)
	if err != nil {
		return nil, err
	}

	// 删 invite 令牌(同时释放被邀请人 pending 索引配额)
	if inviteID != 0 {
		_ = u.repo.DeleteInvite(ctx, inviteID, playerID)
	}

	// push MEMBER_JOINED 给所有成员(不发给 playerID — 原则 2)
	u.pushUpdate(ctx, playerID, memberIDs(result), result,
		teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_JOINED, 0)

	// 人数变了 → 可能从"招募中"变成"已满",同步开放队伍索引(非权威投影)。
	u.syncOpenIndex(ctx, result, result.MapId)

	// R1:入队是不可逆推进(占了名额、改了队伍状态),必须 INFO。
	// 带上 new_state:接受邀请会把一支已 READY 的队伍打回 FORMING(新人默认没准备),
	// 那正是"队长以为能直接开局却点不动"的成因。
	plog.With(ctx).Infow("msg", "team_accept_invite",
		"team_id", teamID, "player_id", playerID, "invite_id", inviteID,
		"new_state", result.State, "members", len(result.Members),
		"ready_generation", result.GetReadyGeneration())
	// 分片:成员加入后队伍 region 分布可能变跨 region(影响 §4.4 battle DS 放置)。router 为 nil → 不打。
	u.logTeamComposition(ctx, result)
	return result, nil
}

// joinTeam 是"把 playerID 加进 teamID"的唯一入队事务,由三条路径共用:
// AcceptInvite(接受邀请)、ApplyToTeam(open 策略直接入队)、HandleTeamApplication(队长同意)。
//
// 为什么必须共用一份:入队是不变量 §1(一人只能在一个可操作队伍)的关键写路径,
// 顺序铁律是**先 ClaimPlayer 原子声明归属,后改成员表**——两个并发入队路径若各写一份,
// 迟早有一条忘了先声明,同一玩家就会同时出现在两支队伍。共用后新增入队入口不可能绕过它。
//
// 失败时用 CAS 回滚 claim(仅当索引仍指向本队才删),防误删并发路径刚写入的新归属;
// 回滚失败会把玩家锁在"claim 指向一支没真进的队"的状态(靠 claimPlayerHealingOrphan
// 下次自愈),因此必须留 Warn 可观测。
func (u *TeamUsecase) joinTeam(ctx context.Context, teamID, playerID uint64) (*teamv1.TeamStorageRecord, error) {
	ttl := u.activeTTL()

	// 入队闸门:队伍一旦提交过对局,成员名单已被 matchmaker.resolveMembers 冻结进票据,
	// 此刻放人进来 = 他不在票据里、也没有 match claim,永远不会被拉进这一局,只能占着
	// 队伍名额干等(§9.20 不准把玩家卡住)。必须在 ClaimPlayer 之前判,避免拒绝后还要回滚归属。
	if err := u.ensureTeamNotCommittedToMatch(ctx, teamID); err != nil {
		return nil, err
	}

	if err := u.claimPlayerHealingOrphan(ctx, playerID, teamID, ttl); err != nil {
		return nil, err
	}

	var result *teamv1.TeamStorageRecord
	// prevState / rejectReason 只用于日志:锁内看到的最后一次取值即本次提交的依据
	// (乐观锁重试时会被覆盖成最后一轮的值,正是我们要记的那一轮)。
	var prevState teamv1.TeamState
	var rejectReason string

	if err := u.updateTeam(ctx, teamID, func(team *teamv1.TeamStorageRecord) error {
		prevState = team.State
		if team.State == stateDisbanded {
			rejectReason = reasonTeamDisbanded
			return errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
		}
		if len(team.Members) >= int(team.MaxSize) {
			rejectReason = reasonTeamFull
			return errcode.New(errcode.ErrTeamFull, "team %d full", teamID)
		}
		if hasMember(team, playerID) {
			rejectReason = reasonAlreadyInTeam
			return errcode.New(errcode.ErrTeamAlreadyInTeam, "player %d already in team %d", playerID, teamID)
		}
		rejectReason = ""

		team.Members = append(team.Members, &teamv1.TeamMemberStorageRecord{PlayerId: playerID})
		team.UpdatedAtMs = time.Now().UnixMilli()

		// 入队后**重算**就绪态,而不是只处理 FORMING→READY 一个方向:新成员默认
		// ready=false,加入一支已经 READY 的队伍(邀请路径可达)必须把队伍打回 FORMING。
		// 否则会留下“State=READY 但有人没准备”的脏态,而 matchmaker.resolveMembers 只校验
		// State==READY、不逐人校验 ready,会带着没准备的人开局。
		// 到这里 State 只可能是 FORMING / READY(disbanded 已在上面拒掉;MATCHING / IN_BATTLE
		// 本服务无写入点,见 MatchCommitmentReader 注释),所以两分支覆盖完备。
		if allReady(team.Members) {
			team.State = stateReady
		} else {
			team.State = stateForming
		}
		result = cloneTeam(team)
		return nil
	}, ttl); err != nil {
		if derr := u.repo.DeletePlayerIndexIfMatches(ctx, playerID, teamID); derr != nil {
			plog.With(ctx).Warnw("msg", "team_join_rollback_index_delete_failed",
				"reason", reasonStoreWriteFailed,
				"player_id", playerID, "team_id", teamID, "err", derr)
		}
		// 锁内没给出 reason 的只剩「乐观锁重试耗尽 / Redis 故障」两类,按错误码区分。
		if rejectReason == "" {
			rejectReason = reasonStoreWriteFailed
			if errcode.As(err) == errcode.ErrTeamConcurrent {
				rejectReason = reasonOptimisticRetryExhausted
			}
		}
		plog.With(ctx).Warnw("msg", "team_join_rejected",
			"reason", rejectReason,
			"team_id", teamID, "player_id", playerID,
			"team_state", prevState, "err", err)
		return nil, err
	}

	// R1:入队成功 = 成员表已改(不可逆)。三条入队路径(接受邀请 / 直接入队 /
	// 队长同意申请)都收敛到这里,查"这个人到底进没进队"只需一个 msg。
	plog.With(ctx).Infow("msg", "team_member_joined",
		"team_id", teamID, "player_id", playerID,
		"prev_state", prevState, "new_state", result.State,
		"members", len(result.Members), "max_size", result.MaxSize,
		"ready_generation", result.GetReadyGeneration())
	logTeamStateChanged(ctx, teamID, prevState, result.State, "join",
		result.GetReadyGeneration(), len(result.Members))

	// player index 已由 ClaimPlayer 在锁前原子写入,此处无需再写。
	return result, nil
}

// ensureTeamNotCommittedToMatch 是入队闸门:队伍已被一场对局占住时拒绝新人进来。
//
// 判据取 matchmaker 的权威 claim 而不是 team.State,原因见 MatchCommitmentReader 注释
// (MATCHING / IN_BATTLE 在本服务没有写入点,排队和战斗期间 State 都是 READY)。
// 查队长一人即可代表整队:StartMatch 由队长发起,队长的 start 索引先于成员 claim 写入,
// 成员 claim 与队长同属一张票据,ReleaseMatch 也整票释放。
//
// **fail-closed**:matchmaker 不可达或返回 UNKNOWN 一律拒绝入队。放进一支已开打的队伍
// 会让玩家既进不去这一局又占着队伍名额(§9.20),比让他重试一次严重得多;§9.22 也要求
// 状态不确定时返回 UNKNOWN 并 fail-closed,不得冒充“空闲”。
//
// 残留窗口:本检查与随后写成员表之间存在毫秒级 TOCTOU(跨服务无法原子)。后果有界——
// 挤进来的人没有 match claim,绝不会被拉进那一局;他只是留在大厅的队伍里,随时可离队。
func (u *TeamUsecase) ensureTeamNotCommittedToMatch(ctx context.Context, teamID uint64) error {
	if u.matchCommitment == nil {
		return nil
	}
	team, found, err := u.repo.Get(ctx, teamID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "team_join_gate_rejected",
			"reason", reasonStoreReadFailed, "team_id", teamID, "err", err)
		return err
	}
	if !found {
		plog.With(ctx).Warnw("msg", "team_join_gate_rejected",
			"reason", reasonTeamNotFound, "team_id", teamID)
		return errcode.New(errcode.ErrTeamNotFound, "team %d not found", teamID)
	}

	committed, err := u.isTeamCommittedToMatch(ctx, team)
	if err != nil {
		// fail-closed 的降级必须带 reason:线上这条码是 ErrUnavailable,access log 里
		// 只会看到一次失败调用,分不出"matchmaker 挂了"与"队伍真在打"。
		plog.With(ctx).Warnw("msg", "team_join_gate_rejected",
			"reason", reasonMatchCommitmentUnknown,
			"team_id", teamID, "captain_id", team.CaptainId,
			"fail_closed", true, "err", err)
		return errcode.NewCause(errcode.ErrUnavailable, err,
			"team %d match commitment unknown, join rejected", teamID)
	}
	if committed {
		// 原为 Debug:线上默认 info 级下这条一行都不出,而"这支队伍正在打所以你进不去"
		// 恰恰是玩家最常问的拒绝原因。
		plog.With(ctx).Warnw("msg", "team_join_gate_rejected",
			"reason", reasonMatchCommitted,
			"team_id", teamID, "captain_id", team.CaptainId,
			"team_state", team.State)
		return errcode.New(errcode.ErrTeamWrongState,
			"team %d already committed to a match", teamID)
	}
	return nil
}

// isTeamCommittedToMatch 读“这支队伍是否已被一场对局占住”,复用调用方已取到的队伍记录。
// 未注入 reader(骨架联调 / 未配 matchmaker_addr)时恒为 false,与历史行为一致。
// 判定口径与失败语义见 ensureTeamNotCommittedToMatch 与 MatchCommitmentReader 注释。
func (u *TeamUsecase) isTeamCommittedToMatch(ctx context.Context, team *teamv1.TeamStorageRecord) (bool, error) {
	if u.matchCommitment == nil || team == nil || team.CaptainId == 0 {
		return false, nil
	}
	return u.matchCommitment.IsPlayerCommittedToMatch(ctx, team.CaptainId)
}

// LeaveTeam 玩家主动离队。
//
// 匹配联动:若该成员正在排队/确认期(matchmaker 持有其 claim),离队后 best-effort
// 撤销整张匹配票据(队伍人数已变,票据快照不再成立):排队中 → 全队退出队列;
// 确认期 → 等价该玩家拒绝确认(match 失败,其余票据退回队列)。见 cancelMatchmaking。
func (u *TeamUsecase) LeaveTeam(ctx context.Context, teamID, playerID uint64) (*teamv1.TeamStorageRecord, error) {
	ttl := u.activeTTL()
	disbandedTTL := u.cfg.DisbandedRetention.Std()
	var result *teamv1.TeamStorageRecord
	var prevState teamv1.TeamState
	var prevCaptainID uint64
	var rejectReason string

	if err := u.updateTeam(ctx, teamID, func(team *teamv1.TeamStorageRecord) error {
		prevState = team.State
		prevCaptainID = team.CaptainId
		if team.State == stateDisbanded {
			rejectReason = reasonTeamDisbanded
			return errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
		}
		if !hasMember(team, playerID) {
			rejectReason = reasonNotMember
			return errcode.New(errcode.ErrTeamNotFound, "player %d not in team %d", playerID, teamID)
		}
		rejectReason = ""

		team.Members = removeMember(team.Members, playerID)
		team.UpdatedAtMs = time.Now().UnixMilli()

		if len(team.Members) == 0 {
			// 队伍空 → 解散
			team.State = stateDisbanded
		} else {
			// 队长离队 → 转移给第一个成员
			if team.CaptainId == playerID {
				team.CaptainId = team.Members[0].PlayerId
			}
			// READY 状态下有人离开 → 回 FORMING
			if team.State == stateReady {
				team.State = stateForming
			}
		}
		result = cloneTeam(team)
		return nil
	}, ttl); err != nil {
		if rejectReason == "" {
			rejectReason = reasonStoreWriteFailed
			if errcode.As(err) == errcode.ErrTeamConcurrent {
				rejectReason = reasonOptimisticRetryExhausted
			}
		}
		plog.With(ctx).Warnw("msg", "team_leave_rejected",
			"reason", rejectReason,
			"team_id", teamID, "player_id", playerID,
			"team_state", prevState, "err", err)
		return nil, err
	}

	// 删 player index。CAS:仅当索引仍指向本队才删,防误删玩家并发加入新队的归属。
	if err := u.repo.DeletePlayerIndexIfMatches(ctx, playerID, teamID); err != nil {
		plog.With(ctx).Warnw("msg", "team_leave_delete_player_index_failed",
			"reason", reasonStoreWriteFailed,
			"player_id", playerID, "team_id", teamID, "err", err)
	}

	// 匹配联动:离队成员若正在排队/确认期 → 撤销整张票据(best-effort,不阻断离队)
	u.cancelMatchmaking(ctx, teamID, playerID)

	// 人数/状态变了 → 同步开放队伍索引(解散或满员时会被摘掉)。
	u.syncOpenIndex(ctx, result, result.MapId)

	// 解散时用短 TTL 刷新 key
	if result.State == stateDisbanded {
		u.refreshDisbandedTTL(ctx, teamID, disbandedTTL)
		// 队伍没了,残留的入队申请再无人能处理 → 顺手清掉,不留到 TTL(best-effort)。
		if err := u.repo.DeleteApplications(ctx, teamID); err != nil {
			plog.With(ctx).Warnw("msg", "team_disband_delete_applications_failed",
				"reason", reasonStoreWriteFailed,
				"team_id", teamID, "err", err)
		}
		u.pushUpdate(ctx, playerID, memberIDs(result), result,
			teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_DISBANDED, 0)
	} else {
		u.pushUpdate(ctx, playerID, memberIDs(result), result,
			teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_LEFT, 0)
	}

	// R1:离队是不可逆推进。captain_transferred 显式打出来 —— 队长离队会把权限转给
	// 第一个成员,而"我怎么突然成队长了 / 谁把图改了"没有这条就无从解释。
	plog.With(ctx).Infow("msg", "team_leave", "team_id", teamID, "player_id", playerID,
		"prev_state", prevState, "new_state", result.State,
		"remaining", len(result.Members),
		"captain_transferred", prevCaptainID == playerID && result.State != stateDisbanded,
		"new_captain_id", result.CaptainId,
		"ready_generation", result.GetReadyGeneration())
	logTeamStateChanged(ctx, teamID, prevState, result.State, "leave",
		result.GetReadyGeneration(), len(result.Members))
	return result, nil
}

// Kick 队长踢人。
//
// 匹配联动:同 LeaveTeam——被踢成员若正在排队/确认期,踢人后 best-effort 撤销其所在
// 的整张匹配票据。见 cancelMatchmaking。
func (u *TeamUsecase) Kick(ctx context.Context, teamID, captainID, targetPlayerID uint64) (*teamv1.TeamStorageRecord, error) {
	ttl := u.activeTTL()
	var result *teamv1.TeamStorageRecord
	var prevState teamv1.TeamState
	var rejectReason string

	if err := u.updateTeam(ctx, teamID, func(team *teamv1.TeamStorageRecord) error {
		prevState = team.State
		if team.State == stateDisbanded {
			rejectReason = reasonTeamDisbanded
			return errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
		}
		if team.CaptainId != captainID {
			rejectReason = reasonNotCaptain
			return errcode.New(errcode.ErrTeamNotCaptain, "player %d is not captain of team %d", captainID, teamID)
		}
		if captainID == targetPlayerID {
			rejectReason = reasonCaptainSelfKick
			return errcode.New(errcode.ErrInvalidArg, "captain cannot kick themselves")
		}
		if !hasMember(team, targetPlayerID) {
			rejectReason = reasonNotMember
			return errcode.New(errcode.ErrTeamNotFound, "player %d not in team %d", targetPlayerID, teamID)
		}
		rejectReason = ""

		team.Members = removeMember(team.Members, targetPlayerID)
		team.UpdatedAtMs = time.Now().UnixMilli()

		// READY 状态下踢人 → 回 FORMING
		if team.State == stateReady {
			team.State = stateForming
		}
		result = cloneTeam(team)
		return nil
	}, ttl); err != nil {
		if rejectReason == "" {
			rejectReason = reasonStoreWriteFailed
			if errcode.As(err) == errcode.ErrTeamConcurrent {
				rejectReason = reasonOptimisticRetryExhausted
			}
		}
		plog.With(ctx).Warnw("msg", "team_kick_rejected",
			"reason", rejectReason,
			"team_id", teamID, "captain_id", captainID,
			"target_player_id", targetPlayerID,
			"team_state", prevState, "err", err)
		return nil, err
	}

	// 删 target player index。CAS:仅当索引仍指向本队才删,防误删被踢者并发加入新队的归属。
	if err := u.repo.DeletePlayerIndexIfMatches(ctx, targetPlayerID, teamID); err != nil {
		plog.With(ctx).Warnw("msg", "team_kick_delete_player_index_failed",
			"reason", reasonStoreWriteFailed,
			"player_id", targetPlayerID, "team_id", teamID, "err", err)
	}

	// 匹配联动:被踢成员若正在排队/确认期 → 撤销整张票据(best-effort,不阻断踢人)
	u.cancelMatchmaking(ctx, teamID, targetPlayerID)

	// 人数变了(满员队踢掉一人后重新开放招募)→ 同步开放队伍索引。
	u.syncOpenIndex(ctx, result, result.MapId)

	// push 给剩余成员 + 被踢者(不发给 captain — 原则 2)
	recipients := append(memberIDs(result), targetPlayerID)
	u.pushUpdate(ctx, captainID, recipients, result,
		teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_KICKED, 0)

	// R1:踢人是不可逆推进,且会把 READY 打回 FORMING(队长会发现开不了局)。
	plog.With(ctx).Infow("msg", "team_kick", "team_id", teamID, "captain_id", captainID,
		"target_player_id", targetPlayerID,
		"prev_state", prevState, "new_state", result.State,
		"remaining", len(result.Members),
		"ready_generation", result.GetReadyGeneration())
	logTeamStateChanged(ctx, teamID, prevState, result.State, "kick",
		result.GetReadyGeneration(), len(result.Members))
	return result, nil
}

// SetReady 设置玩家 ready 状态,并可选更换英雄。
func (u *TeamUsecase) SetReady(ctx context.Context, teamID, playerID uint64, ready bool, heroID uint32) (*teamv1.TeamStorageRecord, error) {
	ttl := u.activeTTL()
	var result *teamv1.TeamStorageRecord
	var prevState teamv1.TeamState
	var prevReady bool
	var rejectReason string

	if err := u.updateTeam(ctx, teamID, func(team *teamv1.TeamStorageRecord) error {
		prevState = team.State
		if team.State == stateDisbanded {
			rejectReason = reasonTeamDisbanded
			return errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
		}
		if team.State != stateForming && team.State != stateReady {
			rejectReason = reasonStateNotAllowed
			return errcode.New(errcode.ErrTeamWrongState, "team %d state %d not allows set_ready", teamID, team.State)
		}

		idx := memberIndex(team.Members, playerID)
		if idx < 0 {
			rejectReason = reasonNotMember
			return errcode.New(errcode.ErrTeamNotFound, "player %d not in team %d", playerID, teamID)
		}
		rejectReason = ""
		prevReady = team.Members[idx].Ready

		team.Members[idx].Ready = ready
		if heroID > 0 {
			team.Members[idx].HeroId = heroID
		}
		team.UpdatedAtMs = time.Now().UnixMilli()

		// 全员 ready → 切 READY
		if ready && allReady(team.Members) {
			team.State = stateReady
		} else if !ready && team.State == stateReady {
			// 任一成员取消 ready → 回 FORMING
			team.State = stateForming
		}

		result = cloneTeam(team)
		return nil
	}, ttl); err != nil {
		if rejectReason == "" {
			rejectReason = reasonStoreWriteFailed
			if errcode.As(err) == errcode.ErrTeamConcurrent {
				rejectReason = reasonOptimisticRetryExhausted
			}
		}
		plog.With(ctx).Warnw("msg", "team_set_ready_rejected",
			"reason", rejectReason,
			"team_id", teamID, "player_id", playerID,
			"want_ready", ready, "team_state", prevState, "err", err)
		return nil, err
	}

	reason := teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_READY
	if heroID > 0 {
		reason = teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_HERO_CHANGED
	}
	// push 给其他成员(不发给自己 — 原则 2)
	u.pushUpdate(ctx, playerID, memberIDs(result), result, reason, 0)

	// FORMING ↔ READY 会改变"是否还在招募"(只有 FORMING 才进列表)→ 同步索引。
	u.syncOpenIndex(ctx, result, result.MapId)

	// R1:ready 置位/复位是本链路最关键的一次状态推进 —— INC-20260813-001 里"打完一局
	// 还挂着 ready"整条线索就断在这里没有 INFO。ready_count/members 让"谁还没准备"
	// 不必再去翻推送内容;ready_generation 是 EndTeamMatch 复位 CAS 的凭据,必须同框。
	plog.With(ctx).Infow("msg", "team_set_ready", "team_id", teamID, "player_id", playerID,
		"prev_ready", prevReady, "ready", ready, "hero_id", heroID,
		"prev_state", prevState, "new_state", result.State,
		"ready_count", readyCount(result.Members), "members", len(result.Members),
		"ready_generation", result.GetReadyGeneration())
	logTeamStateChanged(ctx, teamID, prevState, result.State, "set_ready",
		result.GetReadyGeneration(), len(result.Members))
	return result, nil
}

// GetTeam 读取队伍快照(只读,不走 WATCH)。
func (u *TeamUsecase) GetTeam(ctx context.Context, teamID uint64) (*teamv1.TeamStorageRecord, error) {
	team, found, err := u.repo.Get(ctx, teamID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "team_get_rejected",
			"reason", reasonStoreReadFailed, "team_id", teamID, "err", err)
		return nil, err
	}
	if !found {
		plog.With(ctx).Warnw("msg", "team_get_rejected",
			"reason", reasonTeamNotFound, "team_id", teamID)
		return nil, errcode.New(errcode.ErrTeamNotFound, "team %d not found", teamID)
	}
	return team, nil
}

// GetPlayerTeamID 只反查玩家当前队伍编号(内部只读,DS 出生编制专用)。
//
// 刻意不复用 GetMyTeam:那条路径带三个「玩家本人正在看队伍」的副作用 —— 续 TTL、清脏索引、
// 离线成员观察。DS 每有一名玩家进场就会调一次,把它当成玩家心跳会让已被抛弃的队伍被反复
// 续命,正是 GetMyTeam 注释里点名要防的「旁人反复读把队伍永久续命」。
//
// 仍要读一次队伍记录:索引可能指向已解散/已过期的队伍,把陈旧 team_id 发给 DS 会让两个
// 早已不同队的人在场上被判成队友。这里只判定不清理,自愈仍留给 GetMyTeam。
//
// 判定以**成员表**为准,不以索引为准。索引是派生投影:退队 / 被踢 / 离线清扫都会走
// DeletePlayerIndexIfMatches 去删它,但那一步删失败时只打一条 warn(best-effort,不回滚
// 已经生效的退队)。于是存在这样一个残留窗口 —— 索引还指向队伍 T,T 也确实存在且未解散,
// 只是这名玩家早已不在 T 的成员表里。只校验「T 存在且未解散」放不掉这种残留,
// 会把一个已经退队的人当成 T 的队友发给 DS,而 DS 只在进场时查这一次、整场不再纠正。
// 成员表是队伍归属的权威(§9.22 状态优先查唯一权威,不信派生投影),故以它为准。
func (u *TeamUsecase) GetPlayerTeamID(ctx context.Context, playerID uint64) (uint64, bool, error) {
	teamID, found, err := u.repo.GetPlayerTeamID(ctx, playerID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "team_player_team_lookup_failed",
			"reason", reasonStoreReadFailed, "player_id", playerID, "err", err)
		return 0, false, err
	}
	if !found {
		// R1:这是 DS 出生编制的**权威判定结果**,一名玩家一次进场只有这一条。
		// 「大厅里队友不显示成队友 / 队伍人数对不上」的第一现场就在这里 ——
		// 缺了它,DS 侧只知道自己拿到了空 team_id,查不出后端为什么给空。
		plog.With(ctx).Infow("msg", "team_player_team_resolved",
			"player_id", playerID, "has_team", false, "reason", reasonNoTeam)
		return 0, false, nil
	}

	team, found, err := u.repo.Get(ctx, teamID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "team_player_team_lookup_failed",
			"reason", reasonStoreReadFailed, "player_id", playerID, "team_id", teamID, "err", err)
		return 0, false, err
	}
	if !found || team.State == stateDisbanded {
		reason := reasonTeamNotFound
		if found {
			reason = reasonTeamDisbanded
		}
		plog.With(ctx).Infow("msg", "team_player_team_resolved",
			"player_id", playerID, "has_team", false,
			"stale_team_id", teamID, "reason", reason)
		return 0, false, nil
	}
	if !hasMember(team, playerID) {
		// 索引残留(删索引那一步失败过)。按无队伍处理:错的归属比缺失的归属更糟 ——
		// 前者让路人整场显示成队友,后者只是逐档回落到阵营矩阵。
		// 与本函数其余分支同样「只判定不清理」,自愈仍留给 GetMyTeam 的 CAS 删索引。
		plog.With(ctx).Warnw("msg", "team_player_index_points_to_non_member",
			"reason", reasonIndexNonMember,
			"player_id", playerID, "team_id", teamID,
			"team_state", team.State, "members", len(team.Members))
		plog.With(ctx).Infow("msg", "team_player_team_resolved",
			"player_id", playerID, "has_team", false,
			"stale_team_id", teamID, "reason", reasonIndexNonMember)
		return 0, false, nil
	}
	plog.With(ctx).Infow("msg", "team_player_team_resolved",
		"player_id", playerID, "has_team", true, "team_id", teamID,
		"team_state", team.State, "members", len(team.Members),
		"captain_id", team.CaptainId)
	return teamID, true, nil
}

// GetMyTeam 查询玩家当前所在队伍(只读,登录后进大厅时调用)。
// 返回 (record, hasTeam, err):没队伍是正常态,hasTeam=false 且 err=nil。
// 索引命中但队伍记录已过期/已解散时,顺手清掉脏索引(否则玩家会被
// ClaimPlayer SETNX 挡住无法再建队,不变量 §1 的残留侧漏洞)。
func (u *TeamUsecase) GetMyTeam(ctx context.Context, playerID uint64) (*teamv1.TeamStorageRecord, bool, error) {
	teamID, found, err := u.repo.GetPlayerTeamID(ctx, playerID)
	if err != nil {
		// R2:这条路径是「登录后进大厅第一件事」。读失败会让客户端看到「你没有队伍」
		// 之外的错误码却查不出原因 —— 它返回的是 ErrInternal 一类,access log 虽会记
		// rpc_failed,但没有 player_id / 阶段名,串不到这名玩家的组队链路上。
		plog.With(ctx).Warnw("msg", "team_my_team_rejected",
			"reason", reasonStoreReadFailed, "player_id", playerID,
			"stage", "player_index", "err", err)
		return nil, false, err
	}
	if !found {
		// R4:没队伍是最常见的正常态,且本接口是轮询路径 —— 只能 Debug。
		plog.With(ctx).Debugw("msg", "team_my_team_resolved",
			"player_id", playerID, "has_team", false, "reason", reasonNoTeam)
		return nil, false, nil
	}

	team, found, err := u.repo.Get(ctx, teamID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "team_my_team_rejected",
			"reason", reasonStoreReadFailed, "player_id", playerID, "team_id", teamID,
			"stage", "team_record", "err", err)
		return nil, false, err
	}
	if !found || team.State == stateDisbanded {
		// TTL 竞态残留:索引还在但队伍已没/已解散 → 按无队伍处理并清索引。
		// CAS:仅当索引仍指向该孤儿 teamID 才删,防误删玩家并发建队/入队刚写入的新归属。
		if err := u.repo.DeletePlayerIndexIfMatches(ctx, playerID, teamID); err != nil {
			plog.With(ctx).Warnw("msg", "team_stale_player_index_cleanup_failed",
				"reason", reasonStoreWriteFailed,
				"player_id", playerID, "team_id", teamID, "err", err)
		} else {
			// R1:清索引是不可逆推进(玩家从此可以重新建队)。GetMyTeam 是轮询路径,
			// 但这条只在真的清掉脏索引那一次触发,不是每轮都出,不违反 R4。
			// "玩家反复被 3004 挡住建不了队"就是靠它证明自愈到底有没有发生。
			plog.With(ctx).Infow("msg", "team_stale_player_index_healed",
				"player_id", playerID, "stale_team_id", teamID,
				"reason", reasonTeamNotFound)
		}
		return nil, false, nil
	}
	// 在线心跳:玩家仍在轮询自己的队伍 → 续期(15s 节流,best-effort)。
	// 只在 GetMyTeam(本人+索引校验过)续,GetTeam(任意 teamID)绝不续,
	// 防旁人反复读把已抛弃队伍永久续命;disbanded 分支已在上方 return,不续。
	u.maybeTouchTeam(ctx, team, playerID)
	// 离线成员兜底观察(功能关闭时为 no-op):kafka 事件会丢、Hub DS 整台挂掉时压根不发
	// 事件,只靠事件会留下永远清不掉的残留成员。玩家打开组队面板时把完整成员列表
	// 一次交给 offlinewatch；分类与排期均在其内部完成，失败不影响本次读返回。
	u.inspectTeamPresence(ctx, team)
	// 兜底:滚动升级期旧 matchmaker 副本不调 EndTeamMatch,那些局释放后队伍会永远停在 READY。
	// 队长要开下一局必然先看队伍面板,所以这条读路径覆盖了真实风险路径(INC-20260813-001 ②)。
	// R4:轮询路径,成功侧只能 Debug;它回答的是"这一刻服务端认为他在哪支队、什么状态"。
	plog.With(ctx).Debugw("msg", "team_my_team_resolved",
		"player_id", playerID, "has_team", true, "team_id", teamID,
		"team_state", team.State, "members", len(team.Members),
		"captain_id", team.CaptainId, "ready_count", readyCount(team.Members),
		"ready_generation", team.GetReadyGeneration())
	return team, true, nil
}

// ListPendingInvites 查询发给 playerID 的未过期 pending 邀请(拉取兜底,只读)。
//
// 为什么存在:邀请令牌的唯一权威在 Redis,kafka→push 推送只是投影(不变量
// §9-22)。此前 invite_id 只能从推送获得,推送链路任一环丢帧(producer 故障窗口/
// push 副本崩溃/客户端断线)邀请就静默失效到 TTL 过期。客户端在登录、回前台、
// 打开组队 UI 时调本接口兜底,推送从「唯一通道」降级为「加速器」。
// 读取侧上限 = MaxPendingInvites(写入侧硬上限已兜住总量,单次全量返回即达标)。
func (u *TeamUsecase) ListPendingInvites(ctx context.Context, playerID uint64) ([]*data.InviteRecord, error) {
	invites, err := u.repo.ListPendingInvites(ctx, playerID, u.cfg.MaxPendingInvites)
	if err != nil {
		// R2:这是「我没收到邀请」的兜底查询。它挂了等于邀请链路彻底断掉,
		// 而返回的错误码在 access log 里没有 player_id,串不到人。
		plog.With(ctx).Warnw("msg", "team_list_invites_rejected",
			"reason", reasonStoreReadFailed, "player_id", playerID,
			"limit", u.cfg.MaxPendingInvites, "err", err)
		return nil, err
	}
	// R4:登录 / 回前台 / 打开组队 UI 都会调,成功侧只能 Debug。
	plog.With(ctx).Debugw("msg", "team_list_invites",
		"player_id", playerID, "invites", len(invites), "limit", u.cfg.MaxPendingInvites)
	return invites, nil
}

// ── 找队伍:列表 / 申请 / 审批 ─────────────────────────────────────────────────
//
// 玩家没有队伍时的入口:ListOpenTeams 拉一批正在招募的队伍(全部 / 指定 map_id,上限 10),
// ApplyToTeam 申请其中一支。ApplyToTeam 按服务端配置 join_policy 走两条路径之一,
// 客户端不分叉、也不自己判定能不能进(§17.3 准入条件只有服务端一份权威判定)。

const (
	// openCandidateFactor 是候选超取倍数。索引是非权威投影,候选里可能混着已满 / 已开打 /
	// 已解散的队伍,复核会刷掉一部分;只取 limit 条会导致"明明有队伍却返回不足 limit 条"。
	openCandidateFactor = 3
	// openCandidateMax 是单次候选读取的硬上限,保证单次 Redis 读与复核开销有界
	// (limit 已被 max_open_teams_per_query 钳住,这里是第二道闸)。
	openCandidateMax = 64
)

// joinPolicy 返回当前生效的入队策略。
//
// 启动时 ValidateJoinPolicy 已 fail-fast,正常永远解析成功;万一运行期配置被改坏,
// 一律退回最保守的 approval——绝不因为解析失败就把全服队伍对陌生人敞开(权限放大是
// 不可接受的失败模式,同 conf.ParseJoinPolicy 的口径)。
func (u *TeamUsecase) joinPolicy() string {
	policy, err := conf.ParseJoinPolicy(u.cfg.JoinPolicy)
	if err != nil {
		return conf.JoinPolicyApproval
	}
	return policy
}

// joinPolicyProto 把生效策略映射成客户端可见枚举(填进每份 Team 快照)。
func (u *TeamUsecase) joinPolicyProto() teamv1.TeamJoinPolicy {
	if u.joinPolicy() == conf.JoinPolicyOpen {
		return teamv1.TeamJoinPolicy_TEAM_JOIN_POLICY_OPEN
	}
	return teamv1.TeamJoinPolicy_TEAM_JOIN_POLICY_APPROVAL
}

// maxOpenTeams 返回单次列表的返回上限(读取侧上限,不变量 §9-18)。
func (u *TeamUsecase) maxOpenTeams() int {
	if u.cfg.MaxOpenTeamsPerQuery > 0 {
		return u.cfg.MaxOpenTeamsPerQuery
	}
	return 10
}

// maxApplications 返回单队 pending 申请上限(写入侧 + 读取侧共用,不变量 §9-18)。
func (u *TeamUsecase) maxApplications() int {
	if u.cfg.MaxApplicationsPerTeam > 0 {
		return u.cfg.MaxApplicationsPerTeam
	}
	return 10
}

// isOpenForRecruit 判定队伍是否"正在招募"(会出现在 ListOpenTeams 结果里)。
//
// 口径:**只要没满员且没解散就还在招募**,FORMING 与 READY 一视同仁。
//
// 为什么 READY 也算(2026-08-05 修订,原先只认 FORMING):READY 只表示“当前成员都点了
// 准备”,不表示“已经开打”。而 StartMatch 要求队伍必须是 READY 才能开局,于是原口径下
// “想被人搜到就不能准备、想开局就必须准备”互斥——单人队长点了准备就从招募列表消失,
// 谁也搜不到他(实测:23:58 建队 → 23:58 点准备 → 另一个号连点三次获取队伍全是空列表)。
//
// 那“已经在排队 / 在打的队伍别被打扰”由谁保证?**不由本函数**:MATCHING / IN_BATTLE 在
// 本服务没有写入点,队伍从提交票据到打完全程停在 READY,靠 State 根本分不出来。真正的
// 闸门是入队路径的 ensureTeamNotCommittedToMatch(查 matchmaker 权威 claim),以及
// ListOpenTeams 复核时的同一道闸门 —— 权威判定只有那一份,本函数只管“满没满 / 散没散”。
//
// 这是**唯一**的满员/解散判定口径:写索引和读复核都调它,避免"写进去的和读出来的不是
// 同一套标准"。
func isOpenForRecruit(team *teamv1.TeamStorageRecord) bool {
	if team == nil {
		return false
	}
	if team.State != stateForming && team.State != stateReady {
		return false
	}
	count := len(team.Members)
	return count > 0 && count < int(team.MaxSize)
}

// syncOpenIndex 把队伍在开放招募索引里的存在性同步为当前权威状态。
//
// 索引是非权威投影(不变量 §9.22):写失败只告警,**绝不回滚已提交的队伍状态机迁移**——
// 为了一个用来"找候选"的加速结构而回退已经落地的入队/离队,才是真的破坏正确性。
// score 取"现在 + active_ttl",与队伍 key 的 TTL 同源:队伍 key 到期消失时索引成员恰好
// 也过期,ZREMRANGEBYSCORE 一扫即净,不留悬挂。
func (u *TeamUsecase) syncOpenIndex(ctx context.Context, team *teamv1.TeamStorageRecord, prevMapID uint32) {
	if team == nil || team.TeamId == 0 {
		return
	}
	ttl := u.activeTTL()
	expiresAtMs := time.Now().Add(ttl).UnixMilli()
	if err := u.repo.SyncOpenTeam(ctx, team.TeamId, team.MapId, prevMapID, isOpenForRecruit(team), expiresAtMs, ttl); err != nil {
		plog.With(ctx).Warnw("msg", "team_open_index_sync_failed",
			"reason", reasonStoreWriteFailed,
			"team_id", team.TeamId, "map_id", team.MapId, "prev_map_id", prevMapID,
			"open", isOpenForRecruit(team), "err", err)
	}
}

// SetTeamMap 队长设置本队目标关卡(招募展示 + ListOpenTeams 的 map_id 筛选依据)。
//
// 不校验 map_id 是否在关卡表内:本字段只是招募标签,真正的准入判定在进入链
// (MatchService.StartMatch 已有 ERR_MATCH_INVALID_MAP),在这里再判一次就是第二份判定
// (§17.3 明确禁止)。填了非法值的后果仅限于"这支队伍出现在一个没人筛的分桶里",
// 且换图会把它从旧分桶摘掉,分桶数因此被在线队伍数有界(空 ZSET 被 Redis 自动删除)。
func (u *TeamUsecase) SetTeamMap(ctx context.Context, teamID, captainID uint64, mapID uint32) (*teamv1.TeamStorageRecord, error) {
	ttl := u.activeTTL()
	var result *teamv1.TeamStorageRecord
	var prevMapID uint32
	var prevState teamv1.TeamState
	var rejectReason string

	if err := u.updateTeam(ctx, teamID, func(team *teamv1.TeamStorageRecord) error {
		prevState = team.State
		if team.State == stateDisbanded {
			rejectReason = reasonTeamDisbanded
			return errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
		}
		if team.CaptainId != captainID {
			rejectReason = reasonNotCaptain
			return errcode.New(errcode.ErrTeamNotCaptain, "player %d is not captain of team %d", captainID, teamID)
		}
		// 已进撮合/战斗的队伍改目标关卡没有意义(本次对局的图早已定死),拒绝以免误导队员。
		if team.State != stateForming && team.State != stateReady {
			rejectReason = reasonStateNotAllowed
			return errcode.New(errcode.ErrTeamWrongState, "team %d state %d not allows set_map", teamID, team.State)
		}
		rejectReason = ""

		prevMapID = team.MapId
		team.MapId = mapID
		team.UpdatedAtMs = time.Now().UnixMilli()
		result = cloneTeam(team)
		return nil
	}, ttl); err != nil {
		if rejectReason == "" {
			rejectReason = reasonStoreWriteFailed
			if errcode.As(err) == errcode.ErrTeamConcurrent {
				rejectReason = reasonOptimisticRetryExhausted
			}
		}
		plog.With(ctx).Warnw("msg", "team_set_map_rejected",
			"reason", rejectReason,
			"team_id", teamID, "captain_id", captainID, "map_id", mapID,
			"team_state", prevState, "err", err)
		return nil, err
	}

	// 换桶:先摘旧 map 分桶再写新分桶(由 repo.SyncOpenTeam 内部按 prevMapID 处理)。
	u.syncOpenIndex(ctx, result, prevMapID)

	u.pushUpdate(ctx, captainID, memberIDs(result), result,
		teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MAP_CHANGED, 0)

	plog.With(ctx).Infow("msg", "team_set_map", "team_id", teamID, "captain_id", captainID,
		"prev_map_id", prevMapID, "map_id", mapID,
		"team_state", result.State, "members", len(result.Members),
		"open_for_recruit", isOpenForRecruit(result))
	return result, nil
}

// ListOpenTeams 列出正在招募的队伍(只读)。
//
//	mapID = 0 → 全部;mapID > 0 → 只要目标关卡等于该值的。
//	limit ≤ 0 或超过 max_open_teams_per_query → 钳到 max_open_teams_per_query(默认 10)。
//
// 两段式:先从非权威索引取候选,再逐条回权威队伍记录复核。复核不通过(已满 / 已开打 /
// 已解散 / 记录已没 / map 已改)的候选顺手从索引剔除(best-effort 自愈)。
// 因此索引脏不会让玩家看到一支实际进不去的队伍,最坏只是这一次少返几条。
//
// 单条 Get 失败(网络抖 / 该条记录 proto 损坏)只跳过该候选并计数告警,不整单失败:
// 这里返回的是候选展示列表,跳过一条不授权任何东西;真正的准入判定在 ApplyToTeam,
// 那条路径是 fail-closed 的。索引本身读失败则照常返回错误,由客户端退避重试。
func (u *TeamUsecase) ListOpenTeams(ctx context.Context, mapID uint32, limit int) ([]*teamv1.OpenTeamBrief, error) {
	maxTeams := u.maxOpenTeams()
	if limit <= 0 || limit > maxTeams {
		limit = maxTeams
	}

	candidateLimit := limit * openCandidateFactor
	if candidateLimit > openCandidateMax {
		candidateLimit = openCandidateMax
	}
	candidates, err := u.repo.ListOpenTeamIDs(ctx, mapID, candidateLimit)
	if err != nil {
		plog.With(ctx).Warnw("msg", "team_open_list_rejected",
			"reason", reasonStoreReadFailed, "map_id", mapID, "limit", limit, "err", err)
		return nil, err
	}

	policy := u.joinPolicyProto()
	out := make([]*teamv1.OpenTeamBrief, 0, limit)
	skipped := 0
	// R4 就地聚合:候选逐条打日志会在"找队伍"高频轮询下把同文件的 WARN 冲走。
	// 四个计数器分别对应四类刷掉原因,循环后随那一条汇总日志一起出。
	var (
		skippedReadErr    int
		skippedNotOpen    int
		skippedCommitted  int
		skippedCommitUnkn int
	)

	for _, teamID := range candidates {
		if len(out) >= limit {
			break
		}
		team, found, gerr := u.repo.Get(ctx, teamID)
		if gerr != nil {
			skipped++
			skippedReadErr++
			continue
		}
		if !found || !isOpenForRecruit(team) || (mapID > 0 && team.MapId != mapID) {
			skippedNotOpen++
			// 索引脏了。剔除时用权威记录里的 map_id(记录已没就用查询用的 mapID),
			// 保证摘的是它真正挂着的那个分桶。
			bucket := mapID
			if found {
				bucket = team.MapId
			}
			if rerr := u.repo.RemoveOpenTeamCandidate(ctx, teamID, bucket); rerr != nil {
				plog.With(ctx).Warnw("msg", "team_open_index_prune_failed",
					"reason", reasonStoreWriteFailed,
					"team_id", teamID, "map_id", bucket, "err", rerr)
			}
			continue
		}

		// 已提交对局的队伍不该出现在招募列表里。isOpenForRecruit 分不出这一档(排队/战斗
		// 期间 State 都是 READY),必须查 matchmaker 权威 claim —— 与入队闸门同一份判定,
		// 避免"列表说能进、点了却被拒"。这里只对**已通过前面全部复核、即将返回**的候选查,
		// 不是对全部 candidates 查:返回数被 limit(默认 10)钳住,RPC 次数有界。
		//
		// 与入队闸门的关键差异是**失败方向相反**:这里查不到只跳过本条(少列一支队伍),
		// 不整体报错——列表是加速器,不是准入判定,让一次 matchmaker 抖动把整个"找队伍"
		// 打成失败没有必要;真正的准入 fail-closed 在 ensureTeamNotCommittedToMatch。
		if committed, cerr := u.isTeamCommittedToMatch(ctx, team); cerr != nil || committed {
			if cerr != nil {
				skippedCommitUnkn++
				plog.With(ctx).Warnw("msg", "team_open_list_commitment_unknown",
					"reason", reasonMatchCommitmentUnknown,
					"team_id", teamID, "captain_id", team.CaptainId, "err", cerr)
			} else if rerr := u.repo.RemoveOpenTeamCandidate(ctx, teamID, team.MapId); rerr != nil {
				// 已确认在对局中 → 顺手摘索引自愈,下次浏览就不用再查它。
				skippedCommitted++
				plog.With(ctx).Warnw("msg", "team_open_index_prune_failed",
					"reason", reasonStoreWriteFailed,
					"team_id", teamID, "map_id", team.MapId, "err", rerr)
			} else {
				skippedCommitted++
			}
			skipped++
			continue
		}

		out = append(out, &teamv1.OpenTeamBrief{
			TeamId:      team.TeamId,
			CaptainId:   team.CaptainId,
			MemberCount: uint32(len(team.Members)),
			MaxSize:     uint32(team.MaxSize),
			MapId:       team.MapId,
			CreatedAtMs: team.CreatedAtMs,
			JoinPolicy:  policy,
		})
	}

	if skipped > 0 {
		// 就地聚合的四个分项:一条日志就能回答"明明有队伍却搜不到"是被哪一类刷掉的,
		// 不用逐条去翻候选。
		plog.With(ctx).Warnw("msg", "team_open_list_candidates_skipped",
			"map_id", mapID, "skipped", skipped, "returned", len(out),
			"candidates", len(candidates),
			"skipped_read_err", skippedReadErr,
			"skipped_not_open", skippedNotOpen,
			"skipped_committed", skippedCommitted,
			"skipped_commitment_unknown", skippedCommitUnkn,
			"hint", "候选被复核刷掉:读队伍失败 / 已满或已解散 / 已在对局中 / 匹配状态不确定")
	} else if skippedNotOpen > 0 {
		// 索引指向已满 / 已解散的队伍是常态自愈(不是故障),留 Debug 即可:
		// 真要查"搜不到队伍"时 LOG_LEVEL=debug 单 pod 打开就能看到被剔了多少条。
		plog.With(ctx).Debugw("msg", "team_open_list_candidates_pruned",
			"map_id", mapID, "returned", len(out), "candidates", len(candidates),
			"skipped_not_open", skippedNotOpen)
	}

	// 只富化最终会返回的队长，不对候选循环逐条调用，避免 N+1 和无效查询。
	captainIDs := make([]uint64, 0, len(out))
	seenCaptains := make(map[uint64]struct{}, len(out))
	for _, brief := range out {
		if brief == nil || brief.GetCaptainId() == 0 {
			continue
		}
		if _, ok := seenCaptains[brief.GetCaptainId()]; ok {
			continue
		}
		seenCaptains[brief.GetCaptainId()] = struct{}{}
		captainIDs = append(captainIDs, brief.GetCaptainId())
	}
	display, err := u.resolvePlayerDisplays(ctx, 0, "open_team_captains", captainIDs)
	if err != nil {
		return nil, err
	}
	for _, brief := range out {
		if brief == nil {
			continue
		}
		brief.CaptainNickname = display.names[brief.GetCaptainId()]
		brief.CaptainPlayerNo = display.nos[brief.GetCaptainId()]
	}
	return out, nil
}

// ApplyToTeam 申请加入队伍。返回 (joined, team, expiresAtMs, err):
//   - joined=true  → open 策略下已当场入队,team 为入队后完整快照;
//   - joined=false → approval 策略下已写入申请令牌,expiresAtMs 为其过期时刻。
//
// 前置校验读的是权威队伍记录(不是索引):队伍存在、未解散、正在招募、申请人不在队内。
// open 路径复用 joinTeam(与接受邀请同一入队事务,保不变量 §1)。
// approval 路径的上限校验与占位在 data 层 Lua 内原子完成,无 TOCTOU;重复申请同一队伍
// 幂等(只刷新自己那条的过期时间,不再占新名额)。
func (u *TeamUsecase) ApplyToTeam(ctx context.Context, teamID, applicantID uint64) (bool, *teamv1.TeamStorageRecord, int64, error) {
	// 频率配额(anti-abuse §6 第 6 项):按申请人计,先于一切读写。
	if err := u.allowAction(ctx, "apply", applicantID); err != nil {
		return false, nil, 0, err
	}
	team, found, err := u.repo.Get(ctx, teamID)
	if err != nil {
		u.logApplyRejected(ctx, reasonStoreReadFailed, teamID, applicantID, 0, err)
		return false, nil, 0, err
	}
	if !found {
		u.logApplyRejected(ctx, reasonTeamNotFound, teamID, applicantID, 0, nil)
		return false, nil, 0, errcode.New(errcode.ErrTeamNotFound, "team %d not found", teamID)
	}
	if team.State == stateDisbanded {
		u.logApplyRejected(ctx, reasonTeamDisbanded, teamID, applicantID, team.State, nil)
		return false, nil, 0, errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
	}
	if hasMember(team, applicantID) {
		u.logApplyRejected(ctx, reasonAlreadyInTeam, teamID, applicantID, team.State, nil)
		return false, nil, 0, errcode.New(errcode.ErrTeamAlreadyInTeam, "player %d already in team %d", applicantID, teamID)
	}
	if !isOpenForRecruit(team) {
		// 满员与状态不对分开报,客户端才能给出正确提示(而不是笼统的"进不去")。
		if len(team.Members) >= int(team.MaxSize) {
			u.logApplyRejected(ctx, reasonTeamFull, teamID, applicantID, team.State, nil)
			return false, nil, 0, errcode.New(errcode.ErrTeamFull,
				"team %d is full (%d/%d)", teamID, len(team.Members), team.MaxSize)
		}
		u.logApplyRejected(ctx, reasonTeamNotRecruiting, teamID, applicantID, team.State, nil)
		return false, nil, 0, errcode.New(errcode.ErrTeamWrongState,
			"team %d state %d not recruiting", teamID, team.State)
	}

	// open 策略:当场入队。上面的读只是快速失败,真正的满员/重复入队判定在 joinTeam 的
	// WATCH/MULTI/EXEC 事务内重做一遍(读到写之间队伍可能已被别人填满)。
	if u.joinPolicy() == conf.JoinPolicyOpen {
		result, jerr := u.joinTeam(ctx, teamID, applicantID)
		if jerr != nil {
			return false, nil, 0, jerr
		}
		u.pushUpdate(ctx, applicantID, memberIDs(result), result,
			teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_JOINED, 0)
		u.syncOpenIndex(ctx, result, result.MapId)
		// R1:open 策略下这是"当场入队"的路由判定结果(与 approval 分叉的那一支)。
		plog.With(ctx).Infow("msg", "team_apply_joined_open",
			"team_id", teamID, "player_id", applicantID,
			"join_policy", conf.JoinPolicyOpen,
			"new_state", result.State, "members", len(result.Members))
		u.logTeamComposition(ctx, result)
		return true, result, 0, nil
	}

	// approval 策略:写申请令牌,等队长审批。
	expiresAtMs, err := u.repo.ClaimApplication(ctx, teamID, applicantID, u.cfg.ApplyTTL.Std(), u.maxApplications())
	if err != nil {
		// 这里同时收敛「本队 pending 申请已达上限」与「Redis 写失败」,用错误码分开。
		reason := reasonApplicationStoreFailed
		if errcode.As(err) == errcode.ErrTeamApplyPendingLimit {
			reason = reasonApplicationPendingLimit
		}
		u.logApplyRejected(ctx, reason, teamID, applicantID, team.State, err)
		return false, nil, 0, err
	}

	// 推送只是"去重查申请列表"的提示(§9.22 权威是 ListTeamApplications),丢帧最多延迟
	// 队长看到申请,不丢申请。只发队长,不打扰其他队员。
	u.pushUpdate(ctx, applicantID, []uint64{team.CaptainId}, team,
		teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_APPLICATION_RECEIVED, 0)

	// R1:申请令牌已落 Redis(占了名额、有 TTL),是"申请"链路的不可逆推进。
	plog.With(ctx).Infow("msg", "team_apply_pending",
		"team_id", teamID, "player_id", applicantID,
		"captain_id", team.CaptainId, "join_policy", conf.JoinPolicyApproval,
		"expires_at_ms", expiresAtMs)
	return false, nil, expiresAtMs, nil
}

// logApplyRejected 收口 ApplyToTeam 的全部拒绝分支(§11.3 R2)。
// team_state 一并打出来:满员与"不在招募"回的是不同错误码,但玩家看到的都是"进不去"。
func (u *TeamUsecase) logApplyRejected(
	ctx context.Context, reason string, teamID, applicantID uint64,
	teamState teamv1.TeamState, err error,
) {
	plog.With(ctx).Warnw("msg", "team_apply_rejected",
		"reason", reason,
		"team_id", teamID, "player_id", applicantID,
		"team_state", teamState, "err", err)
}

// ListTeamApplications 队长查本队待处理入队申请(只读,拉取兜底)。
// 申请人名单不对普通成员开放:非队长返回 ErrTeamNotCaptain(3003)。
func (u *TeamUsecase) ListTeamApplications(ctx context.Context, teamID, captainID uint64) ([]*data.ApplicationRecord, error) {
	team, found, err := u.repo.Get(ctx, teamID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "team_list_applications_rejected",
			"reason", reasonStoreReadFailed, "team_id", teamID, "captain_id", captainID, "err", err)
		return nil, err
	}
	if !found {
		plog.With(ctx).Warnw("msg", "team_list_applications_rejected",
			"reason", reasonTeamNotFound, "team_id", teamID, "captain_id", captainID)
		return nil, errcode.New(errcode.ErrTeamNotFound, "team %d not found", teamID)
	}
	if team.CaptainId != captainID {
		plog.With(ctx).Warnw("msg", "team_list_applications_rejected",
			"reason", reasonNotCaptain, "team_id", teamID, "captain_id", captainID,
			"actual_captain_id", team.CaptainId)
		return nil, errcode.New(errcode.ErrTeamNotCaptain, "player %d is not captain of team %d", captainID, teamID)
	}
	return u.repo.ListApplications(ctx, teamID, u.maxApplications())
}

// HandleTeamApplication 队长同意 / 拒绝一份入队申请。
//
// 定序:先用 TakeApplication 原子取走令牌,再决定做什么。这保证同一份申请只被处理一次
// (队长连点两次"同意"→ 第二次拿不到令牌,返回 3010,不会重复入队);也保证"同意"与
// "拒绝"竞争时只有一方生效。
//
// 已知取舍:accept 路径若在取走令牌后入队失败(期间队伍被填满 / 申请人已加入别队),
// 令牌不会被放回。理由是"放回一份队长已经处理过的申请"会让队长再看到一次幽灵申请,
// 比让申请人重新申请更糟(§3 宁可 fail-closed 拒一次,也不写出不自洽的状态)。
// 队长收到明确错误码,申请人的本地待处理态到期后按钮自动恢复可点。
func (u *TeamUsecase) HandleTeamApplication(ctx context.Context, teamID, captainID, applicantID uint64, accept bool) (*teamv1.TeamStorageRecord, error) {
	team, found, err := u.repo.Get(ctx, teamID)
	if err != nil {
		u.logHandleApplicationRejected(ctx, reasonStoreReadFailed, teamID, captainID, applicantID, accept, err)
		return nil, err
	}
	if !found {
		u.logHandleApplicationRejected(ctx, reasonTeamNotFound, teamID, captainID, applicantID, accept, nil)
		return nil, errcode.New(errcode.ErrTeamNotFound, "team %d not found", teamID)
	}
	if team.State == stateDisbanded {
		u.logHandleApplicationRejected(ctx, reasonTeamDisbanded, teamID, captainID, applicantID, accept, nil)
		return nil, errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
	}
	if team.CaptainId != captainID {
		u.logHandleApplicationRejected(ctx, reasonNotCaptain, teamID, captainID, applicantID, accept, nil)
		return nil, errcode.New(errcode.ErrTeamNotCaptain, "player %d is not captain of team %d", captainID, teamID)
	}

	taken, err := u.repo.TakeApplication(ctx, teamID, applicantID)
	if err != nil {
		u.logHandleApplicationRejected(ctx, reasonStoreWriteFailed, teamID, captainID, applicantID, accept, err)
		return nil, err
	}
	if !taken {
		// 队长连点两次"同意"的第二次会走到这里(令牌已被第一次取走)。没有这条日志时,
		// 3010 在线上只落 access log 的 DEBUG,"我明明点了同意他却没进来"完全查不出。
		u.logHandleApplicationRejected(ctx, reasonApplicationNotFound, teamID, captainID, applicantID, accept, nil)
		return nil, errcode.New(errcode.ErrTeamApplyNotFound,
			"application of player %d to team %d not found or expired", applicantID, teamID)
	}

	if !accept {
		// 拒绝:令牌已消耗、配额已释放,队伍状态不变。不给申请人发推送——
		// 申请人的等待本来就是有界的(令牌 TTL),到期即恢复可申请,不需要为"被拒"
		// 单开一条推送通道(§15.3 不为可能的将来预留机制)。
		// R1:令牌已被消费(不可逆),申请人从此可以重新申请。
		// msg 保持历史名不改(§11.3 msg 稳定不变);注意它表达的是"队长拒了这份申请"
		// 这个**业务结果**,与本方法入参/权限被拒的 team_handle_application_rejected 不是一回事。
		plog.With(ctx).Infow("msg", "team_application_rejected",
			"team_id", teamID, "captain_id", captainID, "applicant_id", applicantID,
			"accept", false)
		return team, nil
	}

	// joinTeam 内部已按分支打了带 reason 的拒绝日志;这里补一条"令牌已消费但没进成"的
	// 收尾 —— 那是本方法唯一的已知取舍(令牌不放回),不打就成了静默丢申请。
	result, err := u.joinTeam(ctx, teamID, applicantID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "team_application_accept_failed",
			"reason", reasonJoinAfterApplicationConsumed,
			"team_id", teamID, "captain_id", captainID, "applicant_id", applicantID,
			"hint", "令牌已消费且不放回(避免幽灵申请),申请人需重新申请", "err", err)
		return nil, err
	}

	// push 给全体成员(含刚入队的申请人;不发给队长自己 — 原则 2)。
	u.pushUpdate(ctx, captainID, memberIDs(result), result,
		teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_JOINED, 0)
	u.syncOpenIndex(ctx, result, result.MapId)

	// R1:队长同意 = 申请人已入队(不可逆),且可能把 READY 打回 FORMING。
	plog.With(ctx).Infow("msg", "team_application_accepted",
		"team_id", teamID, "captain_id", captainID, "applicant_id", applicantID,
		"new_state", result.State, "members", len(result.Members))
	u.logTeamComposition(ctx, result)
	return result, nil
}

// logHandleApplicationRejected 收口 HandleTeamApplication 的拒绝分支(§11.3 R2)。
// accept 一起打:同一个 3010 在"同意"与"拒绝"两条路径上的含义完全不同。
func (u *TeamUsecase) logHandleApplicationRejected(
	ctx context.Context, reason string, teamID, captainID, applicantID uint64, accept bool, err error,
) {
	plog.With(ctx).Warnw("msg", "team_handle_application_rejected",
		"reason", reason,
		"team_id", teamID, "captain_id", captainID,
		"applicant_id", applicantID, "accept", accept, "err", err)
}

// claimPlayerHealingOrphan 原子声明 player→teamID 归属(SETNX,不变量 §1),并对
// 孤儿索引自愈:索引虽在但其指向的队伍主体已过期/解散(TTL 竞态 / 解散后删索引
// 失败的悬挂残留)时,不该把玩家永久锁在不存在的队伍里,而应清掉脏索引后重新声明。
//
// 判孤儿安全前提(缺一不可):**所有写路径都先写队伍主体、后写/改索引**——
//   - CreateTeam:先 Create 主体再 claim(本函数),因此「索引指向 X 但 X 主体不在」
//     永远不会是另一个 CreateTeam 的 in-flight 中间态;
//   - AcceptInvite:claim 时目标队伍主体必已存在(邀请的前提)。
//
// 若有人改成「先 claim 后写主体」,本函数会把 in-flight claim 误判孤儿并删掉,
// 造成同一玩家进两支队伍 —— 违反不变量 §1,绝对禁止。
//
// 并发安全:
//   - SETNX 成功 → 直接返回(常态)。
//   - 声明失败且现有队伍真实存在且未解散 → 真冲突,返回 3004。
//   - 声明失败但现有队伍主体已没/已解散 → 孤儿:用 DeletePlayerIndexIfMatches(CAS)
//     仅当索引仍指向该孤儿 teamID 时才删(防误删其他请求刚写入的新 claim),再重试一次
//     SETNX;若重试仍撞占用(他人抢先真建队)→ 诚实返回 3004。
func (u *TeamUsecase) claimPlayerHealingOrphan(ctx context.Context, playerID, teamID uint64, ttl time.Duration) error {
	existTeamID, claimed, err := u.repo.ClaimPlayer(ctx, playerID, teamID, ttl)
	if err != nil {
		return err
	}
	if claimed {
		return nil
	}

	// 声明失败:核对现有队伍是否真实存在。存在且未解散 = 真冲突。
	existTeam, found, err := u.repo.Get(ctx, existTeamID)
	if err != nil {
		return err
	}
	if found && existTeam.State != stateDisbanded {
		// 真冲突(玩家确在他队):排查「为什么玩家进不去队」时需要知道他当前卡在哪支队,
		// 而 access log 只有错误码 3004 —— 且 3004 不是 IsServerFault,线上落 rpc_ok=DEBUG,
		// 完全不可见。原为 Debugw = 线上一条不出,故按 R2 升 WARN 并带枚举 reason。
		plog.With(ctx).Warnw("msg", "team_claim_conflict",
			"reason", reasonAlreadyInTeam,
			"player_id", playerID, "want_team_id", teamID,
			"existing_team_id", existTeamID, "existing_state", existTeam.State)
		return errcode.New(errcode.ErrTeamAlreadyInTeam, "player %d already in team %d", playerID, existTeamID)
	}

	// 孤儿索引:队伍主体已没/已解散。CAS 清掉脏索引(仅当仍指向该 teamID)后重试一次声明。
	if err := u.repo.DeletePlayerIndexIfMatches(ctx, playerID, existTeamID); err != nil {
		plog.With(ctx).Warnw("msg", "team_orphan_player_index_cleanup_failed",
			"reason", reasonStoreWriteFailed,
			"player_id", playerID, "team_id", existTeamID, "err", err)
		return err
	}
	plog.With(ctx).Infow("msg", "team_orphan_player_index_healed",
		"player_id", playerID, "stale_team_id", existTeamID, "want_team_id", teamID)

	retryTeamID, claimed, err := u.repo.ClaimPlayer(ctx, playerID, teamID, ttl)
	if err != nil {
		return err
	}
	if !claimed {
		// 清理与重试之间有人抢先真建队 → 诚实报冲突(自愈后仍撞上,属并发竞争)。
		// 同 team_claim_conflict:3004 在线上不可见,必须自己打 WARN。
		plog.With(ctx).Warnw("msg", "team_claim_conflict_after_heal",
			"reason", reasonAlreadyInTeam,
			"player_id", playerID, "want_team_id", teamID, "existing_team_id", retryTeamID)
		return errcode.New(errcode.ErrTeamAlreadyInTeam, "player %d already in team %d", playerID, retryTeamID)
	}
	return nil
}

// ── 匹配联动辅助 ──────────────────────────────────────────────────────────────

// cancelMatchmaking 成员离开队伍(主动离队 / 被踢)后,best-effort 撤销其所在的
// matchmaker 匹配票据。修复原 TODO"排队中离队不取消票据"的跨服务不一致:
// 不撤销时票据里仍含已离队成员,成局会把他拉进战斗;其残留 claim 也会阻塞他加入的
// 新队伍 StartMatch(4002)。
//
// 弱依赖语义:
//   - matchCanceler 为 nil(未配 matchmaker_addr / 骨架联调)→ 跳过,行为与历史一致;
//   - ErrMatchNotFound(4004)= 该成员本就没在排队,常态,静默;
//   - 其余错误仅 Warn 不阻断离队(残留票据由确认期超时 / TTL 兜底回收)。
func (u *TeamUsecase) cancelMatchmaking(ctx context.Context, teamID, playerID uint64) {
	if u.matchCanceler == nil {
		return
	}
	if err := u.matchCanceler.CancelMatch(ctx, playerID); err != nil {
		if errcode.As(err) == errcode.ErrMatchNotFound {
			return // 未在排队,常态
		}
		plog.With(ctx).Warnw("msg", "team_cancel_matchmaking_failed",
			"reason", reasonMatchmakerCancelFailed,
			"team_id", teamID, "player_id", playerID, "err", err,
			"hint", "票据仍含已离队成员,靠确认期超时 / TTL 兜底回收")
		return
	}
	// R1:撤票是跨服务的不可逆推进(整张票据连同队友一起退回队列)。原为 Debug =
	// 线上看不到,于是"我离个队全队被踢出匹配"在日志里没有任何痕迹。
	plog.With(ctx).Infow("msg", "team_matchmaking_cancelled_on_leave",
		"team_id", teamID, "player_id", playerID)
}

// ── push 辅助 ─────────────────────────────────────────────────────────────────

// pushUpdate 把 TeamUpdateEvent marshal 后调 pusher.PushTeamUpdate。
// pusher 为 nil 时(Phase 2 骨架阶段)直接跳过。
//
// 每个接收方单独序列化一条 TeamUpdateEvent,使 to_player_id 字段精确标识接收方。
// kafka key = player_id(不变量 §9)由 PushToPlayers 内部保证;
// PushToPlayers 内部同时排除 callerPlayerID(原则 2)。
func (u *TeamUsecase) pushUpdate(
	ctx context.Context,
	callerPlayerID uint64,
	toPlayerIDs []uint64,
	team *teamv1.TeamStorageRecord,
	reason teamv1.TeamUpdateReason,
	inviteID uint64,
) {
	if u.pusher == nil || len(toPlayerIDs) == 0 {
		return
	}

	now := time.Now().UnixMilli()
	protoTeam, err := u.teamToProto(ctx, team)
	if err != nil {
		return
	}

	for _, pid := range toPlayerIDs {
		event := &teamv1.TeamUpdateEvent{
			Team:       protoTeam,
			ByPlayerId: callerPlayerID,
			ToPlayerId: pid, // 每条消息精确标识接收方,客户端可直接读取
			TsMs:       now,
			Reason:     reason,
			InviteId:   inviteID,
		}
		payload, err := proto.Marshal(event)
		if err != nil {
			plog.With(ctx).Warnw("msg", "team_push_marshal_failed",
				"reason", reasonPushMarshalFailed,
				"team_id", team.GetTeamId(), "to_player_id", pid,
				"update_reason", reason.String(), "err", err)
			continue
		}
		// PushToPlayers 内部跳过 callerPlayerID == pid 的情况(原则 2)
		if _, err := u.pusher.PushTeamUpdate(ctx, callerPlayerID, []uint64{pid}, payload); err != nil {
			if inviteID != 0 {
				// legacy 路径承载邀请的推送丢了 → 计入邀请推送失败指标(可告警,
				// 不再只靠 Warn 日志;被邀请人靠 ListMyPendingInvites 拉取兜底)。
				InvitePushFailed.WithLabelValues("legacy").Inc()
			}
			// ⚠️ 这两条历史上把 TeamUpdateReason 放在 "reason" 字段里,与 §11.3 R2
			// 规定的「reason = 枚举化拒绝原因」撞名。改挂到 update_reason,
			// 让 reason 在全服务范围内只有一个含义(否则按 reason 聚合会把两类混在一起)。
			plog.With(ctx).Warnw("msg", "team_push_failed",
				"reason", reasonPushProduceFailed,
				"team_id", team.GetTeamId(), "to_player_id", pid,
				"update_reason", reason.String(), "err", err,
				"hint", "推送是加速器不是权威;客户端靠 GetMyTeam / ListMyPendingInvites 拉取兜底")
		}
	}
}

// pushInvite 构造独立的 TeamInviteEvent 并以 event_type=INVITE(=1)推送给被邀请人。
// 由 Invite 按 InvitePushMode 调用(dual/dedicated 会调,legacy 不调)。
func (u *TeamUsecase) pushInvite(ctx context.Context, inviterID, targetPlayerID, teamID, inviteID uint64) {
	// 未装配推送器或接收方无效时无法投递,直接保持邀请主流程成功。
	if u.pusher == nil || targetPlayerID == 0 {
		return
	}
	// now 统一作为事件产生时间和过期时间的计算基准,避免两次取时产生偏差。
	now := time.Now().UnixMilli()
	// event 只携带邀请弹框所需的最小字段,不再附带完整队伍快照。
	event := &teamv1.TeamInviteEvent{
		TeamId:      teamID,
		InviteId:    inviteID,
		InviterId:   inviterID,
		ToPlayerId:  targetPlayerID,
		TsMs:        now,
		ExpiresAtMs: now + u.cfg.InviteTTL.Std().Milliseconds(),
	}
	// payload 是写入 Kafka 的 TeamInviteEvent protobuf 字节;err 表示序列化失败。
	payload, err := proto.Marshal(event)
	if err != nil {
		// 序列化失败只记录告警;邀请令牌已落库,不能把推送弱依赖反向变成业务失败。
		InvitePushFailed.WithLabelValues("dedicated").Inc()
		plog.With(ctx).Warnw("msg", "team_invite_marshal_failed",
			"reason", reasonPushMarshalFailed,
			"team_id", teamID, "to_player_id", targetPlayerID, "invite_id", inviteID, "err", err)
		return
	}
	// eventTypeInvite 是客户端选择 TeamInviteEvent 反序列化器的域内判别值。
	const eventTypeInvite = uint32(teamv1.TeamPushEventType_TEAM_PUSH_EVENT_TYPE_INVITE)
	// 推送失败沿用弱依赖策略只记告警 + 计数(可告警),不改变已经落库的邀请主流程结果;
	// 丢帧由被邀请人 ListMyPendingInvites 拉取兜底。
	if _, err := u.pusher.PushTeamEvent(ctx, inviterID, []uint64{targetPlayerID}, payload, eventTypeInvite); err != nil {
		InvitePushFailed.WithLabelValues("dedicated").Inc()
		plog.With(ctx).Warnw("msg", "team_invite_push_failed",
			"reason", reasonPushProduceFailed,
			"team_id", teamID, "to_player_id", targetPlayerID, "invite_id", inviteID, "err", err,
			"hint", "被邀请人靠 ListMyPendingInvites 拉取兜底")
	}
}

// refreshDisbandedTTL 用短 TTL 刷新已解散队伍的 key。
// 单条 EXPIRE 即可,无需再走一轮 WATCH/MULTI/EXEC 空写。
func (u *TeamUsecase) refreshDisbandedTTL(ctx context.Context, teamID uint64, ttl time.Duration) {
	if err := u.repo.ExpireTeam(ctx, teamID, ttl); err != nil {
		plog.With(ctx).Warnw("msg", "team_refresh_disbanded_ttl_failed",
			"reason", reasonStoreWriteFailed, "team_id", teamID, "err", err)
	}
}

// ── 类型转换 ──────────────────────────────────────────────────────────────────

type playerDisplayProjection struct {
	names map[uint64]string
	nos   map[uint64]uint64
}

// resolvePlayerDisplays 对一份客户端视图中的全部玩家做两次互不依赖的权威批量读取。
// 两端都是展示弱依赖：任一端失败只让对应字段留空，不得阻断组队主流程或污染身份字段。
func (u *TeamUsecase) resolvePlayerDisplays(
	ctx context.Context,
	teamID uint64,
	projection string,
	playerIDs []uint64,
) (playerDisplayProjection, error) {
	result := playerDisplayProjection{
		names: make(map[uint64]string, len(playerIDs)),
		nos:   make(map[uint64]uint64, len(playerIDs)),
	}
	if len(playerIDs) == 0 {
		return result, nil
	}

	var displayWG sync.WaitGroup
	if u.playerNos != nil {
		displayWG.Add(1)
		go func() {
			defer displayWG.Done()
			resolved, err := u.playerNos.ResolvePlayerNos(ctx, playerIDs)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				plog.With(ctx).Warnw("msg", "team_player_no_resolve_failed",
					"reason", "display_dependency_unavailable", "team_id", teamID,
					"projection", projection, "player_count", len(playerIDs), "err", err, "fail_soft", true)
				return
			}
			result.nos = resolved
		}()
	}
	if u.playerNames != nil {
		displayWG.Add(1)
		go func() {
			defer displayWG.Done()
			resolved, err := u.playerNames.ResolvePlayerNames(ctx, playerIDs)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				plog.With(ctx).Warnw("msg", "team_player_name_resolve_failed",
					"reason", "display_dependency_unavailable", "team_id", teamID,
					"projection", projection, "player_count", len(playerIDs), "err", err, "fail_soft", true)
				return
			}
			result.names = resolved
		}()
	}
	displayWG.Wait()
	if err := ctx.Err(); err != nil {
		return playerDisplayProjection{}, err
	}
	return result, nil
}

// teamToProto 把存储快照 TeamStorageRecord 转成客户端可见结构 Team(不变量 §9.14)。
//
// join_policy 不是存储字段,而是**每次组装时从服务端配置派生**(§9.11 派生字段服务端重算 /
// §9.22 不重复影子状态):改配置即时对全服生效,也不存在"队伍里存的策略"与配置漂移的问题。
// 因此本转换必须是 TeamUsecase 的方法而不是自由函数——离开 usecase 就拿不到权威配置,
// 只能填 UNSPECIFIED,客户端就会一直按保守策略渲染。
func (u *TeamUsecase) teamToProto(ctx context.Context, r *teamv1.TeamStorageRecord) (*teamv1.Team, error) {
	if r == nil {
		return nil, nil
	}
	playerIDs := make([]uint64, 0, len(r.Members))
	seen := make(map[uint64]struct{}, len(r.Members))
	for _, member := range r.Members {
		if member == nil || member.GetPlayerId() == 0 {
			continue
		}
		if _, ok := seen[member.GetPlayerId()]; ok {
			continue
		}
		seen[member.GetPlayerId()] = struct{}{}
		playerIDs = append(playerIDs, member.GetPlayerId())
	}
	display, err := u.resolvePlayerDisplays(ctx, r.GetTeamId(), "team_members", playerIDs)
	if err != nil {
		return nil, err
	}
	members := make([]*teamv1.TeamMember, 0, len(r.Members))
	for _, m := range r.Members {
		if m == nil {
			continue
		}
		members = append(members, &teamv1.TeamMember{
			PlayerId: m.PlayerId,
			Nickname: display.names[m.PlayerId],
			Mmr:      m.Mmr,
			Ready:    m.Ready,
			HeroId:   m.HeroId,
			PlayerNo: display.nos[m.PlayerId],
		})
	}
	return &teamv1.Team{
		TeamId:      r.TeamId,
		CaptainId:   r.CaptainId,
		Members:     members,
		State:       r.State,
		CreatedAtMs: r.CreatedAtMs,
		MaxSize:     r.MaxSize,
		MapId:       r.MapId,
		JoinPolicy:  u.joinPolicyProto(),
	}, nil
}

// TeamToProto 导出供 service 层使用。
func (u *TeamUsecase) TeamToProto(ctx context.Context, r *teamv1.TeamStorageRecord) (*teamv1.Team, error) {
	return u.teamToProto(ctx, r)
}

// TeamApplicationsToProto 把申请 Redis 记录投影为队长可见查询视图。
// 名字和编号只在此处临时富化，绝不写回 ApplicationRecord；审批仍只使用 player_id。
func (u *TeamUsecase) TeamApplicationsToProto(
	ctx context.Context,
	teamID uint64,
	records []*data.ApplicationRecord,
) ([]*teamv1.TeamApplication, error) {
	playerIDs := make([]uint64, 0, len(records))
	seen := make(map[uint64]struct{}, len(records))
	for _, record := range records {
		if record == nil || record.PlayerID == 0 {
			continue
		}
		if _, ok := seen[record.PlayerID]; ok {
			continue
		}
		seen[record.PlayerID] = struct{}{}
		playerIDs = append(playerIDs, record.PlayerID)
	}
	display, err := u.resolvePlayerDisplays(ctx, teamID, "team_applications", playerIDs)
	if err != nil {
		return nil, err
	}

	applications := make([]*teamv1.TeamApplication, 0, len(records))
	for _, record := range records {
		if record == nil {
			continue
		}
		applications = append(applications, &teamv1.TeamApplication{
			PlayerId:    record.PlayerID,
			ExpiresAtMs: record.ExpiresAtMs,
			Nickname:    display.names[record.PlayerID],
			PlayerNo:    display.nos[record.PlayerID],
		})
	}
	return applications, nil
}

// ── 成员辅助函数 ──────────────────────────────────────────────────────────────

func hasMember(team *teamv1.TeamStorageRecord, playerID uint64) bool {
	for _, m := range team.Members {
		if m.PlayerId == playerID {
			return true
		}
	}
	return false
}

func memberIndex(members []*teamv1.TeamMemberStorageRecord, playerID uint64) int {
	for i, m := range members {
		if m.PlayerId == playerID {
			return i
		}
	}
	return -1
}

func removeMember(members []*teamv1.TeamMemberStorageRecord, playerID uint64) []*teamv1.TeamMemberStorageRecord {
	out := make([]*teamv1.TeamMemberStorageRecord, 0, len(members))
	for _, m := range members {
		if m.PlayerId != playerID {
			out = append(out, m)
		}
	}
	return out
}

func allReady(members []*teamv1.TeamMemberStorageRecord) bool {
	if len(members) == 0 {
		return false
	}
	for _, m := range members {
		if !m.Ready {
			return false
		}
	}
	return true
}

func memberIDs(team *teamv1.TeamStorageRecord) []uint64 {
	ids := make([]uint64, 0, len(team.Members))
	for _, m := range team.Members {
		ids = append(ids, m.PlayerId)
	}
	return ids
}

func cloneTeam(team *teamv1.TeamStorageRecord) *teamv1.TeamStorageRecord {
	return proto.Clone(team).(*teamv1.TeamStorageRecord)
}

// cloneMembers 深拷贝成员列表。
//
// BeginTeamMatch 的收据要存一份消费前名单,而**同一份**名单还要返回给 matchmaker 建票。
// 两者共享底层切片会让「存进队伍记录的快照」与「调用方手里的快照」互为别名 ——
// 那种别名不会立刻出错,只会在将来某次看似无关的改动里静默串味。
func cloneMembers(members []*teamv1.TeamMemberStorageRecord) []*teamv1.TeamMemberStorageRecord {
	if members == nil {
		return nil
	}
	out := make([]*teamv1.TeamMemberStorageRecord, 0, len(members))
	for _, m := range members {
		out = append(out, proto.Clone(m).(*teamv1.TeamMemberStorageRecord))
	}
	return out
}
