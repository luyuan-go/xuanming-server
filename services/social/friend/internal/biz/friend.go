// Package biz 是 friend 服务的业务逻辑层(2026-06-15)。
//
// 职责(docs/design/go-services.md §2.4):
//   - 好友请求 / 接受 / 列表 / 拉黑
//   - 好友图落 pandora_social(MySQL 强依赖,data.FriendRepo)
//   - 好友请求 / 接受经 kafka pandora.friend.event → push 推送给接收方(弱依赖)
//   - ListFriends 经 player_locator 填在线状态(弱依赖,查不到按离线)
//
// 关键规则:
//   - 不能加自己;互相拉黑则不能加好友(ErrFriendBlocked)
//   - 已是好友再加 → ErrFriendAlreadyAdded
//   - AcceptFriend 只有请求的 target 本人可接受(R5 player_id 来自 JWT ctx)
//   - 推送原则 2:好友请求发给 target,接受通知发给 requester,均不发给操作者自己
package biz

import (
	"context"
	"time"

	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/playerdisplay"
	friendv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/friend/v1"

	"github.com/luyuancpp/pandora/services/social/friend/internal/conf"
	"github.com/luyuancpp/pandora/services/social/friend/internal/data"
)

// FriendEventPusher 把好友事件发到 kafka(main.go 注入 kafkax 适配器;弱依赖,nil 时静默跳过)。
// toPlayerID 是接收方(= evt.to_player_id),kafka key 用它(不变量 §9)。
type FriendEventPusher interface {
	PushFriendEvent(ctx context.Context, toPlayerID uint64, evt *friendv1.FriendEvent) error
}

// PlayerNameResolver / PlayerNoResolver 只暴露客户端展示所需的公开投影；
// 好友审批与持久化始终继续使用原始 player_id。
type PlayerNameResolver interface {
	ResolvePlayerNames(ctx context.Context, playerIDs []uint64) (map[uint64]string, error)
}

type PlayerNoResolver interface {
	ResolvePlayerNos(ctx context.Context, playerIDs []uint64) (map[uint64]uint64, error)
}

// FriendUsecase 是 friend 服务业务逻辑核心。
type FriendUsecase struct {
	repo   data.FriendRepo
	pusher FriendEventPusher       // 弱依赖,可为 nil
	online data.OnlineStatusReader // 弱依赖,可为 nil
	cfg    conf.FriendConf

	// strategies 是推荐好友策略链(按序召回直到凑够 limit);conf recommend_strategies 决定顺序。
	strategies []RecommendStrategy

	// router 是确定性 region/cell 路由器(scale-cellular-20m.md §4.2)。
	// 可为 nil:单 Cell / dev / 阶段 1~2 不分片,好友边分片落点观测退化为不打日志(行为不变)。
	// 分片部署时由 main 经 SetCellRouter 注入,AcceptFriend 成功后额外打一条好友边分片
	// 落点观测(跨 region 好友 → 走 §4.4 最小跨 region 通道)。nil-safe。
	router *cellroute.Router

	// rateQuota 好友申请频率配额(anti-abuse §6 第 6 项)。可为 nil(不限)。
	rateQuota ActionRateQuota

	playerNameResolver PlayerNameResolver
	playerNoResolver   PlayerNoResolver
}

// ActionRateQuota 申请类写入的 per-player 频率配额(实现 pkg/redisx.ActionQuota)。
// 总量闸(max_incoming_requests 200)只限「同时挂多少」,挡不住「加满 → 全撤 → 再加满」
// 的写放大循环,频率配额补这一维。背压非权威门:判定 error 由调用方 fail-open 放行。
type ActionRateQuota interface {
	Allow(ctx context.Context, action string, subject uint64) (bool, error)
}

// SetRateQuota 注入频率配额(可选;不注入 = 不限,dev 无 Redis 联调兼容)。
func (u *FriendUsecase) SetRateQuota(q ActionRateQuota) {
	u.rateQuota = q
}

func (u *FriendUsecase) SetPlayerNameResolver(r PlayerNameResolver) {
	u.playerNameResolver = r
}

func (u *FriendUsecase) SetPlayerNoResolver(r PlayerNoResolver) {
	u.playerNoResolver = r
}

// allowAction 频率配额门:窗内超额返回 ErrRateLimited(先于一切副作用);fail-open。
func (u *FriendUsecase) allowAction(ctx context.Context, action string, playerID uint64) error {
	if u.rateQuota == nil {
		return nil
	}
	ok, err := u.rateQuota.Allow(ctx, action, playerID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "friend_rate_quota_check_failed",
			"action", action, "player_id", playerID, "err", err)
		return nil
	}
	if !ok {
		plog.With(ctx).Warnw("msg", "friend_rate_quota_rejected",
			"action", action, "player_id", playerID)
		return errcode.New(errcode.ErrRateLimited, "friend %s rate limited, retry later", action)
	}
	return nil
}

// NewFriendUsecase 构造。pusher / online 允许为 nil(弱依赖未配置时降级)。
func NewFriendUsecase(repo data.FriendRepo, pusher FriendEventPusher, online data.OnlineStatusReader, cfg conf.FriendConf) *FriendUsecase {
	if cfg.MaxFriends <= 0 {
		cfg.MaxFriends = 200
	}
	if cfg.MaxIncomingRequests <= 0 {
		cfg.MaxIncomingRequests = 200
	}
	if cfg.MaxBlocks <= 0 {
		cfg.MaxBlocks = 200
	}
	if cfg.RecommendLimit <= 0 {
		cfg.RecommendLimit = 10
	}
	if cfg.RecommendLimit > recommendMaxLimit {
		cfg.RecommendLimit = recommendMaxLimit
	}
	return &FriendUsecase{
		repo:       repo,
		pusher:     pusher,
		online:     online,
		cfg:        cfg,
		strategies: buildStrategies(repo, cfg.RecommendStrategies),
	}
}

// SetCellRouter 注入确定性 region/cell 路由器(scale-cellular-20m.md §4.2 两级架构)。
//
// nil-safe:不调用 / 传 nil 时(单 Cell / dev / 阶段 1~2),AcceptFriend 不做好友边分片落点观测,
// 行为与历史一致。用 setter 而非构造参数,避免单 Cell 阶段调用点被迫改签名(与 matchmaker /
// auction / battle_result 一致)。Router 内部读路径无锁,并发安全。
func (u *FriendUsecase) SetCellRouter(r *cellroute.Router) {
	u.router = r
}

// AddFriend 发起好友请求。requester / target 由 service 从 JWT ctx + 请求体得到。
// newRequestID 是 service 用 snowflake 预生成的请求 ID(复用历史请求时会被丢弃)。
func (u *FriendUsecase) AddFriend(ctx context.Context, requesterID, targetID, newRequestID uint64) (uint64, error) {
	if requesterID == 0 || targetID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "requester / target required")
	}
	if requesterID == targetID {
		return 0, errcode.New(errcode.ErrInvalidArg, "cannot add self as friend")
	}

	// 频率配额(anti-abuse §6 第 6 项):按发起方计,先于一切读写。
	if err := u.allowAction(ctx, "request", requesterID); err != nil {
		return 0, err
	}

	// 互相拉黑则不能加好友(fail-fast 预检,非权威:权威复核在 CreateRequest 事务内的
	// pair 守卫锁下再做一次,R5 复审 P1-3——预检后并发 Block/Accept 落库的交错由守卫拦截)
	blocked, err := u.repo.IsBlocked(ctx, requesterID, targetID)
	if err != nil {
		return 0, err
	}
	if blocked {
		return 0, errcode.New(errcode.ErrFriendBlocked, "blocked between %d and %d", requesterID, targetID)
	}

	// 已是好友(同上:fail-fast 预检,权威复核在 CreateRequest 事务内)
	already, err := u.repo.AreFriends(ctx, requesterID, targetID)
	if err != nil {
		return 0, err
	}
	if already {
		return 0, errcode.New(errcode.ErrFriendAlreadyAdded, "already friends: %d-%d", requesterID, targetID)
	}

	// 提前失败:requester 好友已满就不用发请求(非权威校验,真正的原子上限
	// 在 AcceptRequest 事务内对双方再校一次)。
	if u.cfg.MaxFriends > 0 {
		cnt, cerr := u.repo.CountFriends(ctx, requesterID)
		if cerr != nil {
			return 0, cerr
		}
		if cnt >= u.cfg.MaxFriends {
			return 0, errcode.New(errcode.ErrFriendLimit,
				"friend limit reached: %d (max %d)", requesterID, u.cfg.MaxFriends)
		}
	}

	requestID, _, err := u.repo.CreateRequest(ctx, newRequestID, requesterID, targetID, u.cfg.MaxIncomingRequests)
	if err != nil {
		return 0, err
	}

	// 推送原则 2:好友请求通知发给接收方 target
	u.pushEvent(ctx, targetID, &friendv1.FriendEvent{
		ByPlayerId: requesterID,
		ToPlayerId: targetID,
		RequestId:  requestID,
		Reason:     friendv1.FriendEventReason_FRIEND_EVENT_REASON_REQUEST_RECEIVED,
		TsMs:       nowMs(),
	})
	return requestID, nil
}

// AcceptFriend 接受好友请求。player 必须是请求的 target 本人(R5)。
func (u *FriendUsecase) AcceptFriend(ctx context.Context, playerID, requestID uint64) error {
	if playerID == 0 || requestID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player / request_id required")
	}

	req, found, err := u.repo.GetRequest(ctx, requestID)
	if err != nil {
		return err
	}
	// 预检(fail-fast,非权威):请求不存在 / 不是发给本人 / 已非 pending → 直接报找不到,
	// 避免无谓开事务。真正的权威校验(target / block / 上限 / 状态)在 AcceptRequest 事务内做。
	if !found || req.TargetID != playerID || req.Status != requestStatusPending {
		return errcode.New(errcode.ErrFriendNotFound, "no acceptable request: %d", requestID)
	}

	// 权威:target 校验 + block 校验 + 上限校验 + 状态更新 + 建边,全在一个事务里(原子)。
	// accepted=false 表示预检后到事务取锁前,请求已被 Block / 另一次 accept 并发处理 →
	// 本次未真正完成 pending→accepted,绝不能推送 accepted(否则假成功,P1)。
	accepted, err := u.repo.AcceptRequest(ctx, requestID, playerID, u.cfg.MaxFriends)
	if err != nil {
		return err
	}
	if !accepted {
		// CAS/TOCTOU 失败方:预检后到取锁前请求被 Block / 另一次 accept 并发处理(request_id 轮换
		// 或 status≠pending)。翻成 NotFound 给客户端,DEBUG 留证以区分"真不存在"与"并发竞争丢工作"。
		plog.With(ctx).Debugw("msg", "friend_accept_lost", "request_id", requestID, "player_id", playerID)
		return errcode.New(errcode.ErrFriendNotFound, "no acceptable request: %d", requestID)
	}

	// 分片:观测本条好友边的分片落点(跨分片 → 双向建边拆两个 owner 分片;跨 region → 走最小
	// 跨 region 通道)。router 为 nil(单 Cell)→ 不打,行为不变;分片 MySQL / Kafka 双向建边属 infra(§11.1)。
	u.logFriendshipSharding(ctx, requestID, req.RequesterID, playerID)

	// 推送原则 2:接受通知发给发起方 requester
	u.pushEvent(ctx, req.RequesterID, &friendv1.FriendEvent{
		ByPlayerId: playerID,
		ToPlayerId: req.RequesterID,
		RequestId:  requestID,
		Reason:     friendv1.FriendEventReason_FRIEND_EVENT_REASON_REQUEST_ACCEPTED,
		TsMs:       nowMs(),
	})
	return nil
}

// RejectFriend 拒绝好友请求。player 必须是请求的 target 本人(R5)。
// 不推送给 requester(避免"被拒绝"的尴尬,业界惯例);pending→rejected 后该 requester 仍可再次发起。
func (u *FriendUsecase) RejectFriend(ctx context.Context, playerID, requestID uint64) error {
	if playerID == 0 || requestID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player / request_id required")
	}

	req, found, err := u.repo.GetRequest(ctx, requestID)
	if err != nil {
		return err
	}
	// 预检(fail-fast,非权威):不存在 / 不是发给本人 / 已非 pending → 找不到。
	if !found || req.TargetID != playerID || req.Status != requestStatusPending {
		return errcode.New(errcode.ErrFriendNotFound, "no rejectable request: %d", requestID)
	}

	// 权威:事务内锁行 + target 校验 + 状态校验 + 置 rejected。
	rejected, err := u.repo.RejectRequest(ctx, requestID, playerID)
	if err != nil {
		return err
	}
	if !rejected {
		return errcode.New(errcode.ErrFriendNotFound, "no rejectable request: %d", requestID)
	}
	return nil
}

// ListFriendRequests 列出"发给本人且仍 pending"的好友请求(客户端可见结构 FriendRequestInfo)。
//
// 离线玩家错过 kafka push 后,靠本接口补拉待处理请求。from_nickname / from_player_no
// 在最终结果页按 player/login 权威批量投影；失败只降级为空/0，原始 from_player_id 不变。
func (u *FriendUsecase) ListFriendRequests(ctx context.Context, playerID uint64) ([]*friendv1.FriendRequestInfo, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	rows, err := u.repo.ListIncomingRequests(ctx, playerID)
	if err != nil {
		return nil, err
	}
	playerIDs := make([]uint64, 0, len(rows))
	for _, r := range rows {
		if r.RequesterID != 0 {
			playerIDs = append(playerIDs, r.RequesterID)
		}
	}
	projection, err := playerdisplay.Resolve(ctx, playerIDs, u.playerNameResolver, u.playerNoResolver)
	if err != nil {
		return nil, err
	}
	for _, failure := range projection.Failures {
		plog.With(ctx).Warnw("msg", "friend_request_display_resolve_failed",
			"dependency", failure.Dependency,
			"batch_size", len(failure.PlayerIDs),
			"err", failure.Err)
	}
	infos := make([]*friendv1.FriendRequestInfo, 0, len(rows))
	for _, r := range rows {
		infos = append(infos, &friendv1.FriendRequestInfo{
			RequestId:    r.RequestID,
			FromPlayerId: r.RequesterID,
			FromNickname: projection.Names[r.RequesterID],
			CreatedMs:    r.CreatedMs,
			FromPlayerNo: projection.Numbers[r.RequesterID],
		})
	}
	return infos, nil
}

// ListFriends 列好友(客户端可见结构 FriendInfo)。
//
// nickname 留空:由客户端按 player_id 向 player 服务批量解析展示名(CLAUDE.md §5.8
// 最小数据单位,friend 服务不持有昵称真源,避免跨库 join)。
// is_online / last_seen_ms 经 locator 填充,弱依赖查不到按离线。
func (u *FriendUsecase) ListFriends(ctx context.Context, playerID uint64) ([]*friendv1.FriendInfo, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}

	rows, err := u.repo.ListFriends(ctx, playerID)
	if err != nil {
		return nil, err
	}

	ids := make([]uint64, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.FriendID)
	}

	var online map[uint64]data.OnlineStatus
	if u.online != nil && len(ids) > 0 {
		online = u.online.BatchOnline(ctx, ids)
	}

	infos := make([]*friendv1.FriendInfo, 0, len(rows))
	for _, r := range rows {
		fi := &friendv1.FriendInfo{
			PlayerId: r.FriendID,
			SinceMs:  r.SinceMs,
		}
		if st, ok := online[r.FriendID]; ok {
			fi.IsOnline = st.Online
			fi.LastSeenMs = st.LastSeenMs
		}
		infos = append(infos, fi)
	}
	return infos, nil
}

// Block 拉黑 target(同时删好友关系 + 取消两人之间 pending 请求,见 data.Block)。
func (u *FriendUsecase) Block(ctx context.Context, playerID, targetID uint64) error {
	if playerID == 0 || targetID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player / target required")
	}
	if playerID == targetID {
		return errcode.New(errcode.ErrInvalidArg, "cannot block self")
	}
	return u.repo.Block(ctx, playerID, targetID, u.cfg.MaxBlocks)
}

// RemoveFriend 删好友(双向边,幂等)。不动黑名单。
func (u *FriendUsecase) RemoveFriend(ctx context.Context, playerID, targetID uint64) error {
	if playerID == 0 || targetID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player / target required")
	}
	if playerID == targetID {
		return errcode.New(errcode.ErrInvalidArg, "cannot remove self")
	}
	return u.repo.RemoveFriend(ctx, playerID, targetID)
}

// Unblock 取消拉黑 target(幂等)。不自动恢复好友关系,玩家需重新加好友。
func (u *FriendUsecase) Unblock(ctx context.Context, playerID, targetID uint64) error {
	if playerID == 0 || targetID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player / target required")
	}
	if playerID == targetID {
		return errcode.New(errcode.ErrInvalidArg, "cannot unblock self")
	}
	return u.repo.Unblock(ctx, playerID, targetID)
}

// ListBlocks 列出本人拉黑的人(客户端可见结构 BlockInfo)。
// nickname 留空,由客户端按 player_id 向 player 服务解析(CLAUDE.md §5.8)。
func (u *FriendUsecase) ListBlocks(ctx context.Context, playerID uint64) ([]*friendv1.BlockInfo, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	rows, err := u.repo.ListBlocks(ctx, playerID)
	if err != nil {
		return nil, err
	}
	infos := make([]*friendv1.BlockInfo, 0, len(rows))
	for _, r := range rows {
		infos = append(infos, &friendv1.BlockInfo{
			PlayerId: r.BlockedID,
			SinceMs:  r.SinceMs,
		})
	}
	return infos, nil
}

// recommendMaxLimit 硬上限:无论 conf / 请求填多少,推荐数绝不超过 20(防爆量)。
const recommendMaxLimit = 20

// RecommendFriends 推荐好友(客户端可见结构 RecommendedFriendInfo,默认 conf.RecommendLimit 个,可刷新)。
//
// 按策略链(conf recommend_strategies,默认 mutual→random)依次召回,凑够 limit 即止;
// 每选中一批就追加到 exclude,后续策略不重复。
// 排除自己 / 已是好友 / 双向拉黑 / 双向 pending 请求 / exclude(客户端回传已看过的)。
// 服务端无状态,刷新靠客户端把已展示 id 放进 exclude;nickname 留空由客户端解析(§5.8)。
// limit<=0 用 conf 默认;超 recommendMaxLimit(20)收敛到 20。
func (u *FriendUsecase) RecommendFriends(ctx context.Context, playerID uint64, limit int, exclude []uint64) ([]*friendv1.RecommendedFriendInfo, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if limit <= 0 {
		limit = u.cfg.RecommendLimit
	}
	if limit <= 0 {
		limit = recommendMaxLimit
	}
	if limit > recommendMaxLimit {
		limit = recommendMaxLimit
	}

	// exclude 永远带上自己,防止把自己推荐给自己;每选中一批就追加,避免跨策略重复。
	ex := make([]uint64, 0, len(exclude)+1+limit)
	ex = append(ex, exclude...)
	ex = append(ex, playerID)

	var rows []data.RecommendRow
	for _, st := range u.strategies {
		if len(rows) >= limit {
			break
		}
		picked, err := st.Candidates(ctx, playerID, ex, limit-len(rows))
		if err != nil {
			return nil, err
		}
		for _, r := range picked {
			rows = append(rows, r)
			ex = append(ex, r.CandidateID)
		}
	}

	ids := make([]uint64, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.CandidateID)
	}

	var online map[uint64]data.OnlineStatus
	if u.online != nil && len(ids) > 0 {
		online = u.online.BatchOnline(ctx, ids)
	}

	infos := make([]*friendv1.RecommendedFriendInfo, 0, len(rows))
	for _, r := range rows {
		ri := &friendv1.RecommendedFriendInfo{
			PlayerId:          r.CandidateID,
			MutualFriendCount: uint32(r.Mutual),
		}
		if st, ok := online[r.CandidateID]; ok {
			ri.IsOnline = st.Online
			ri.LastSeenMs = st.LastSeenMs
		}
		infos = append(infos, ri)
	}
	return infos, nil
}

// pushEvent 弱依赖推送:pusher 为 nil 或发送失败只 warn,不影响主流程成功。
func (u *FriendUsecase) pushEvent(ctx context.Context, toPlayerID uint64, evt *friendv1.FriendEvent) {
	if u.pusher == nil {
		return
	}
	if err := u.pusher.PushFriendEvent(ctx, toPlayerID, evt); err != nil {
		plog.With(ctx).Warnw("msg", "friend_event_push_failed",
			"to_player_id", toPlayerID, "reason", evt.GetReason().String(), "err", err)
	}
}

// nowMs 返回当前毫秒时间戳。
func nowMs() int64 {
	return time.Now().UnixMilli()
}

// requestStatusPending 在 data 包定义为常量;biz 这里复用其语义值(1=pending)。
// 为避免跨包导出 data 内部常量,这里就地声明同值常量(与 proto FriendRequestStatus 对齐)。
const requestStatusPending = 1
