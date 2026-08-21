// Package biz 是 guild 服务的业务逻辑层(2026-06-27)。
//
// 职责(docs/design/decision-revisit-chat-group.md):
//   - 公会成员管理:创建 / 申请 / 审批 / 退会 / 踢人 / 解散 / 转让会长 / 任命官员 / 查询
//   - 权限:LEADER 解散 / 转让 / 任命 / 审批 / 踢任意成员;OFFICER 审批 / 踢普通成员;MEMBER 仅聊天 / 退会
//   - 单归属:玩家只能属于一个公会(DB guild_members.player_id 唯一 + 事务校验)
//   - 成员变更经 kafka pandora.guild.event → push 推送给接收方(弱依赖,nil 静默跳过)
//
// 关键规则:
//   - LEADER 不能直接退会 / 被踢:必须先 TransferLeader 或 DisbandGuild(否则公会无主)
//   - 推送原则 2:通知不回发操作者本人(申请通知发给会长 / 官员;审批结果发给申请人)
//   - 入会申请 nickname/player_no 由 player/login 权威批量投影；审批仍只认 player_id
//   - 客户端只拿可见结构(CLAUDE.md §14):RPC 只回 Guild / GuildMember / GuildJoinRequest
package biz

import (
	"context"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/playerdisplay"
	guildv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/guild/v1"

	"github.com/luyuancpp/pandora/services/social/guild/internal/conf"
	"github.com/luyuancpp/pandora/services/social/guild/internal/data"
)

// GuildEventPusher 把公会成员变更事件发到 kafka(main 注入;弱依赖,nil 时静默跳过)。
// kafka key = to_player_id(同接收方有序;push 服务按 key 路由到该玩家 stream)。
type GuildEventPusher interface {
	PushGuildEvent(ctx context.Context, toPlayerID uint64, evt *guildv1.GuildEvent) error
}

// PlayerNameResolver / PlayerNoResolver 只提供客户端展示投影；审批权限与
// request identity 始终由原始 player_id 决定。
type PlayerNameResolver interface {
	ResolvePlayerNames(ctx context.Context, playerIDs []uint64) (map[uint64]string, error)
}

type PlayerNoResolver interface {
	ResolvePlayerNos(ctx context.Context, playerIDs []uint64) (map[uint64]uint64, error)
}

// GuildUsecase 是 guild 服务公会业务逻辑核心。
type GuildUsecase struct {
	repo     data.GuildRepo
	cache    data.GuildCache  // 弱依赖读缓存,可为 nil(降级直连 MySQL)
	pusher   GuildEventPusher // 弱依赖,可为 nil
	cfg      conf.GuildConf
	cacheTTL time.Duration

	// rateQuota 入会申请频率配额(anti-abuse §6 第 6 项)。可为 nil(不限)。
	rateQuota ActionRateQuota

	playerNameResolver PlayerNameResolver
	playerNoResolver   PlayerNoResolver

	// 缓存降级日志限流(模式 C):GetGuild / GetMyGuild 是「全服共享热 key」的高 QPS 只读
	// 入口,Redis 一抖就按请求量刷屏,且一次请求可能叠 get + 回填 set 两条。缓存是显式弱
	// 依赖(失败只掉命中率,权威读走 MySQL),但仍是 Redis 健康度的信号 → 不删、限流。
	// 读 / 写两侧分开计数便于区分故障面;无 key ⇒ 常量内存(guild_id / player_id 高基数,
	// 按 key 分桶会随实体数无界增长)。
	cacheReadLog  plog.Window
	cacheWriteLog plog.Window
}

// guildCacheLogWindowMs 是缓存降级日志的最小重打间隔(毫秒)。
const guildCacheLogWindowMs = 5000

// NewGuildUsecase 构造。cache / pusher 均允许为 nil(弱依赖未配置时降级)。
func NewGuildUsecase(repo data.GuildRepo, cache data.GuildCache, pusher GuildEventPusher, cfg conf.GuildConf) *GuildUsecase {
	return &GuildUsecase{repo: repo, cache: cache, pusher: pusher, cfg: cfg, cacheTTL: cfg.CacheTTL.Std()}
}

// ActionRateQuota 申请类写入的 per-player 频率配额(anti-abuse §6 第 6 项;
// 实现 pkg/redisx.ActionQuota)。总量闸(每公会 pending 200)只限「同时挂多少」,
// 频率配额补「刷多快」。背压非权威门:判定 error 由调用方 fail-open 放行。
type ActionRateQuota interface {
	Allow(ctx context.Context, action string, subject uint64) (bool, error)
}

// SetRateQuota 注入频率配额(可选;不注入 = 不限,dev 无 Redis 联调兼容)。
func (u *GuildUsecase) SetRateQuota(q ActionRateQuota) {
	u.rateQuota = q
}

func (u *GuildUsecase) SetPlayerNameResolver(r PlayerNameResolver) {
	u.playerNameResolver = r
}

func (u *GuildUsecase) SetPlayerNoResolver(r PlayerNoResolver) {
	u.playerNoResolver = r
}

// allowAction 频率配额门:窗内超额返回 ErrRateLimited(先于一切副作用);fail-open。
func (u *GuildUsecase) allowAction(ctx context.Context, action string, playerID uint64) error {
	if u.rateQuota == nil {
		return nil
	}
	ok, err := u.rateQuota.Allow(ctx, action, playerID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "guild_rate_quota_check_failed",
			"action", action, "player_id", playerID, "err", err)
		return nil
	}
	if !ok {
		plog.With(ctx).Warnw("msg", "guild_rate_quota_rejected",
			"action", action, "player_id", playerID)
		return errcode.New(errcode.ErrRateLimited, "guild %s rate limited, retry later", action)
	}
	return nil
}

// CreateGuild 创建公会,创建者成为会长。newGuildID 由 service 用 snowflake 预生成。
func (u *GuildUsecase) CreateGuild(ctx context.Context, playerID uint64, name string, newGuildID uint64) (uint64, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return 0, errcode.New(errcode.ErrInvalidArg, "guild name required")
	}
	if utf8.RuneCountInString(name) > u.cfg.MaxNameLen {
		return 0, errcode.New(errcode.ErrInvalidArg, "guild name too long")
	}
	if err := u.repo.CreateGuild(ctx, newGuildID, playerID, name, u.cfg.MaxGuildMembers); err != nil {
		return 0, err
	}
	// 写后删:创建者入会成为会长 → 失效其 member 反查缓存。
	// 若不删,旧「所属某公会」反查(退会/踢出后删失败或并发迟到回填的残留)会让
	// GetMyGuild 在 TTL 内继续返回旧公会,最长 cacheTTL(默认 60s)。
	u.invalidateMember(ctx, playerID)
	return newGuildID, nil
}

// ApplyJoin 申请加入公会。newRequestID 由 service 预生成。
func (u *GuildUsecase) ApplyJoin(ctx context.Context, playerID, guildID, newRequestID uint64) (uint64, error) {
	if guildID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "guild_id required")
	}
	// 频率配额(anti-abuse §6 第 6 项):按申请人计,先于一切读写。
	if err := u.allowAction(ctx, "apply", playerID); err != nil {
		return 0, err
	}
	// 申请人不能已在任何公会(单归属)。
	if m, ok, err := u.repo.GetMember(ctx, playerID); err != nil {
		return 0, err
	} else if ok {
		return 0, errcode.New(errcode.ErrGuildAlreadyInGuild, "player %d already in guild %d", playerID, m.GuildID)
	}
	// 目标公会须存在。
	g, ok, err := u.repo.GetGuild(ctx, guildID)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, errcode.New(errcode.ErrGuildNotFound, "guild %d not found", guildID)
	}

	requestID, _, err := u.repo.CreateJoinRequest(ctx, newRequestID, guildID, playerID, u.cfg.MaxPendingRequestsPerGuild)
	if err != nil {
		return 0, err
	}

	// 推送:通知会长 / 官员有人申请(原则 2:不发申请人本人)。
	u.fanoutToManagers(ctx, guildID, &guildv1.GuildEvent{
		Type:      guildv1.GuildEventType_GUILD_EVENT_TYPE_JOIN_APPLIED,
		GuildId:   guildID,
		ActorId:   playerID,
		GuildName: g.Name,
	})
	return requestID, nil
}

// ApproveJoin 审批通过加入申请。approverID 须为该公会 LEADER / OFFICER(权威校验在事务内)。
func (u *GuildUsecase) ApproveJoin(ctx context.Context, approverID, requestID uint64) error {
	if requestID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "request_id required")
	}
	rq, ok, err := u.repo.GetRequest(ctx, requestID)
	if err != nil {
		return err
	}
	if !ok {
		return errcode.New(errcode.ErrGuildRequestInvalid, "request %d not found", requestID)
	}
	approved, err := u.repo.ApproveJoin(ctx, requestID, approverID, u.cfg.MaxGuildMembers)
	if err != nil {
		return err
	}
	if !approved {
		return errcode.New(errcode.ErrGuildRequestInvalid, "request %d not pending", requestID)
	}
	// 写后删:成员数++ → 失效公会资料;申请人新入会 → 失效其 member 反查。
	u.invalidateGuild(ctx, rq.GuildID)
	u.invalidateMember(ctx, rq.PlayerID)
	g, _, _ := u.repo.GetGuild(ctx, rq.GuildID)
	guildName := ""
	if g != nil {
		guildName = g.Name
	}
	// 通知申请人:通过(发给申请人本人)。
	u.push(ctx, rq.PlayerID, &guildv1.GuildEvent{
		Type:      guildv1.GuildEventType_GUILD_EVENT_TYPE_JOIN_APPROVED,
		GuildId:   rq.GuildID,
		ActorId:   approverID,
		GuildName: guildName,
	})
	return nil
}

// RejectJoin 拒绝加入申请。approverID 须为该公会 LEADER / OFFICER。
func (u *GuildUsecase) RejectJoin(ctx context.Context, approverID, requestID uint64) error {
	if requestID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "request_id required")
	}
	rq, ok, err := u.repo.GetRequest(ctx, requestID)
	if err != nil {
		return err
	}
	if !ok {
		return errcode.New(errcode.ErrGuildRequestInvalid, "request %d not found", requestID)
	}
	rejected, err := u.repo.RejectJoin(ctx, requestID, approverID)
	if err != nil {
		return err
	}
	if !rejected {
		return errcode.New(errcode.ErrGuildRequestInvalid, "request %d not pending", requestID)
	}
	u.push(ctx, rq.PlayerID, &guildv1.GuildEvent{
		Type:    guildv1.GuildEventType_GUILD_EVENT_TYPE_JOIN_REJECTED,
		GuildId: rq.GuildID,
		ActorId: approverID,
	})
	return nil
}

// LeaveGuild 退会。LEADER 不能直接退会(须先转让或解散)。
func (u *GuildUsecase) LeaveGuild(ctx context.Context, playerID uint64) error {
	m, ok, err := u.repo.GetMember(ctx, playerID)
	if err != nil {
		return err
	}
	if !ok {
		return errcode.New(errcode.ErrGuildNotMember, "player %d not in any guild", playerID)
	}
	if m.Role == data.GuildRoleLeader {
		return errcode.New(errcode.ErrGuildNotLeader, "leader must transfer or disband before leaving")
	}
	if err := u.repo.RemoveMember(ctx, m.GuildID, playerID); err != nil {
		return err
	}
	// 写后删:成员数-- → 失效公会资料;玩家退会 → 失效其 member 反查。
	u.invalidateGuild(ctx, m.GuildID)
	u.invalidateMember(ctx, playerID)
	return nil
}

// KickMember 踢出成员。LEADER 可踢任意非会长成员;OFFICER 只能踢普通成员。不能踢自己 / 踢会长。
func (u *GuildUsecase) KickMember(ctx context.Context, operatorID, targetID uint64) error {
	if operatorID == targetID {
		return errcode.New(errcode.ErrInvalidArg, "cannot kick self")
	}
	op, ok, err := u.repo.GetMember(ctx, operatorID)
	if err != nil {
		return err
	}
	if !ok {
		return errcode.New(errcode.ErrGuildNotMember, "operator %d not in any guild", operatorID)
	}
	target, ok, err := u.repo.GetMember(ctx, targetID)
	if err != nil {
		return err
	}
	if !ok || target.GuildID != op.GuildID {
		return errcode.New(errcode.ErrGuildNotMember, "target %d not in operator's guild", targetID)
	}
	if target.Role == data.GuildRoleLeader {
		return errcode.New(errcode.ErrGuildNoPermission, "cannot kick the leader")
	}
	switch op.Role {
	case data.GuildRoleLeader:
		// 可踢 officer / member
	case data.GuildRoleOfficer:
		if target.Role != data.GuildRoleMember {
			return errcode.New(errcode.ErrGuildNoPermission, "officer can only kick members")
		}
	default:
		return errcode.New(errcode.ErrGuildNoPermission, "member cannot kick")
	}
	if err := u.repo.KickMember(ctx, op.GuildID, operatorID, targetID); err != nil {
		return err
	}
	// 写后删:成员数-- → 失效公会资料;被踢玩家 → 失效其 member 反查。
	u.invalidateGuild(ctx, op.GuildID)
	u.invalidateMember(ctx, targetID)
	g, _, _ := u.repo.GetGuild(ctx, op.GuildID)
	guildName := ""
	if g != nil {
		guildName = g.Name
	}
	u.push(ctx, targetID, &guildv1.GuildEvent{
		Type:      guildv1.GuildEventType_GUILD_EVENT_TYPE_KICKED,
		GuildId:   op.GuildID,
		ActorId:   operatorID,
		GuildName: guildName,
	})
	return nil
}

// DisbandGuild 解散公会。仅 LEADER。删除的成员集合由解散事务在锁住公会父行后原子返回。
func (u *GuildUsecase) DisbandGuild(ctx context.Context, leaderID uint64) error {
	m, ok, err := u.repo.GetMember(ctx, leaderID)
	if err != nil {
		return err
	}
	if !ok {
		return errcode.New(errcode.ErrGuildNotMember, "player %d not in any guild", leaderID)
	}
	if m.Role != data.GuildRoleLeader {
		return errcode.New(errcode.ErrGuildNotLeader, "only leader can disband")
	}
	g, _, _ := u.repo.GetGuild(ctx, m.GuildID)
	guildName := ""
	if g != nil {
		guildName = g.Name
	}
	// 成员集合由解散事务在持公会父行 FOR UPDATE 锁时读取并原子返回:与删除同事务,不会漏掉
	// 「快照后并发批准的新成员」(那类成员会被删却拿不到,导致缓存失效 / 通知遗漏)。事务失败
	// 则整体回滚、什么都没删,直接返回错误让上层重试。
	deletedMembers, err := u.repo.DisbandGuild(ctx, m.GuildID, leaderID)
	if err != nil {
		return err
	}
	// 写后删:公会已删 → 失效公会资料 + 全体被删成员 member 反查。
	u.invalidateGuild(ctx, m.GuildID)
	for _, pid := range deletedMembers {
		u.invalidateMember(ctx, pid)
	}
	// 通知全体成员(含会长本人,解散是全员事件,例外于原则 2)。
	for _, pid := range deletedMembers {
		u.push(ctx, pid, &guildv1.GuildEvent{
			Type:      guildv1.GuildEventType_GUILD_EVENT_TYPE_DISBANDED,
			GuildId:   m.GuildID,
			ActorId:   leaderID,
			GuildName: guildName,
		})
	}
	return nil
}

// TransferLeader 转让会长。仅现任 LEADER;目标须为本公会成员且非自己。
func (u *GuildUsecase) TransferLeader(ctx context.Context, leaderID, targetID uint64) error {
	if leaderID == targetID {
		return errcode.New(errcode.ErrInvalidArg, "cannot transfer to self")
	}
	m, ok, err := u.repo.GetMember(ctx, leaderID)
	if err != nil {
		return err
	}
	if !ok || m.Role != data.GuildRoleLeader {
		return errcode.New(errcode.ErrGuildNotLeader, "only leader can transfer")
	}
	if err := u.repo.TransferLeader(ctx, m.GuildID, leaderID, targetID); err != nil {
		return err
	}
	// 写后删:leader_id 变更 → 失效公会资料(成员集合不变,member 反查无需动)。
	u.invalidateGuild(ctx, m.GuildID)
	g, _, _ := u.repo.GetGuild(ctx, m.GuildID)
	guildName := ""
	if g != nil {
		guildName = g.Name
	}
	members, _ := u.repo.ListMembers(ctx, m.GuildID, 0, 0)
	for _, mem := range members {
		u.push(ctx, mem.PlayerID, &guildv1.GuildEvent{
			Type:      guildv1.GuildEventType_GUILD_EVENT_TYPE_LEADER_CHANGED,
			GuildId:   m.GuildID,
			ActorId:   targetID,
			GuildName: guildName,
		})
	}
	return nil
}

// SetOfficer 任命 / 撤销官员。仅 LEADER;目标须为本公会成员且非自己。
func (u *GuildUsecase) SetOfficer(ctx context.Context, leaderID, targetID uint64, isOfficer bool) error {
	if leaderID == targetID {
		return errcode.New(errcode.ErrInvalidArg, "cannot set officer on self")
	}
	m, ok, err := u.repo.GetMember(ctx, leaderID)
	if err != nil {
		return err
	}
	if !ok || m.Role != data.GuildRoleLeader {
		return errcode.New(errcode.ErrGuildNotLeader, "only leader can set officer")
	}
	target, ok, err := u.repo.GetMember(ctx, targetID)
	if err != nil {
		return err
	}
	if !ok || target.GuildID != m.GuildID {
		return errcode.New(errcode.ErrGuildNotMember, "target %d not in guild", targetID)
	}
	if target.Role == data.GuildRoleLeader {
		return errcode.New(errcode.ErrGuildNoPermission, "target is leader")
	}
	role := int32(data.GuildRoleMember)
	if isOfficer {
		role = data.GuildRoleOfficer
	}
	return u.repo.SetRole(ctx, m.GuildID, leaderID, targetID, role)
}

// GetGuild 查公会。不存在 → ErrGuildNotFound。cache-aside:先读缓存,miss 回落 MySQL 并回填。
func (u *GuildUsecase) GetGuild(ctx context.Context, guildID uint64) (*guildv1.Guild, error) {
	if u.cache != nil {
		if g, ok, err := u.cache.GetGuild(ctx, guildID); err != nil {
			// Redis 读故障:记录后回落 MySQL(缓存弱依赖,不报错给上层)。限流见结构体注释。
			if adm, streak := u.cacheReadLog.Admit(time.Now().UnixMilli(), guildCacheLogWindowMs); adm {
				plog.With(ctx).Warnw("msg", "guild_cache_get_failed",
					"guild_id", guildID, "streak", streak, "err", err)
			}
		} else {
			if n, _ := u.cacheReadLog.Recovered(); n > 0 {
				plog.With(ctx).Infow("msg", "guild_cache_read_recovered", "failed_total", n)
			}
			if ok {
				return toGuildView(g), nil
			}
		}
	}
	g, ok, err := u.repo.GetGuild(ctx, guildID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errcode.New(errcode.ErrGuildNotFound, "guild %d not found", guildID)
	}
	u.fillGuildCache(ctx, g)
	return toGuildView(g), nil
}

// GetMyGuild 查玩家当前公会;不在任何公会返回 (nil, nil)(service 回 OK + 空)。
// cache-aside:member 反查缓存解析所属 guild_id → info 缓存取公会资料;任一 miss 回落 MySQL。
func (u *GuildUsecase) GetMyGuild(ctx context.Context, playerID uint64) (*guildv1.Guild, error) {
	if u.cache != nil {
		if guildID, ok, err := u.cache.GetMemberGuildID(ctx, playerID); err != nil {
			if adm, streak := u.cacheReadLog.Admit(time.Now().UnixMilli(), guildCacheLogWindowMs); adm {
				plog.With(ctx).Warnw("msg", "guild_member_cache_get_failed",
					"player_id", playerID, "streak", streak, "err", err)
			}
		} else if ok {
			if g, ok2, err2 := u.cache.GetGuild(ctx, guildID); err2 != nil {
				if adm, streak := u.cacheReadLog.Admit(time.Now().UnixMilli(), guildCacheLogWindowMs); adm {
					plog.With(ctx).Warnw("msg", "guild_cache_get_failed",
						"guild_id", guildID, "streak", streak, "err", err2)
				}
			} else if ok2 {
				return toGuildView(g), nil
			}
			// member 命中但 info miss / 读故障 → 落到下方权威读取并重建缓存。
		}
	}
	g, ok, err := u.repo.GetMyGuild(ctx, playerID)
	if err != nil {
		return nil, err
	}
	if !ok {
		// 权威:不在任何公会 → 清掉可能残留的陈旧 member 反查缓存(写路径删失败时的自愈)。
		u.invalidateMember(ctx, playerID)
		return nil, nil
	}
	u.fillGuildCache(ctx, g)
	u.fillMemberCache(ctx, playerID, g.GuildID)
	return toGuildView(g), nil
}

// GetPlayerGuildID 反查玩家当前公会编号(内部只读,DS 出生编制专用)。
//
// 刻意不复用 GetMyGuild:那条是给玩家自己看公会面板的 cache-aside 读,member→guild 反查
// 缓存命中即返回、不回源权威。同一次陈旧读在两条路径上的代价差一个量级 ——
// 玩家面板读到旧值只是晚一拍,下次拉取自愈;而 DS 只在进场时查这一次,陈旧值一旦写到实体上
// 就会复制给全场并**整场不再纠正**(会友被显示成路人,或反过来)。写路径删缓存失败时仅告警、
// 靠 TTL 兜底,所以"删缓存基本都成功"不足以让 DS 这一侧也吃缓存。
//
// 与 team 侧 GetPlayerTeamID 同构:只回编号不回快照(§9.14 最小视图)。权威读到的结果顺手
// 回填缓存,玩家面板那条路径照常受益。
func (u *GuildUsecase) GetPlayerGuildID(ctx context.Context, playerID uint64) (uint64, bool, error) {
	g, ok, err := u.repo.GetMyGuild(ctx, playerID)
	if err != nil {
		return 0, false, err
	}
	if !ok {
		// 权威:不在任何公会 → 清掉可能残留的陈旧 member 反查缓存(与 GetMyGuild 同口径)。
		u.invalidateMember(ctx, playerID)
		return 0, false, nil
	}
	u.fillGuildCache(ctx, g)
	u.fillMemberCache(ctx, playerID, g.GuildID)
	return g.GuildID, true, nil
}

// 分页上限(决策:docs/design/decision-revisit-list-pagination.md)。
const (
	defaultPageLimit = 50
	maxPageLimit     = 100
)

func clampLimit(limit int) int {
	if limit <= 0 {
		return defaultPageLimit
	}
	if limit > maxPageLimit {
		return maxPageLimit
	}
	return limit
}

// ListMembers 列公会成员(客户端可见结构),按 player_id 升序游标分页。
func (u *GuildUsecase) ListMembers(ctx context.Context, guildID, cursor uint64, limit int) ([]*guildv1.GuildMember, uint64, error) {
	limit = clampLimit(limit)
	rows, err := u.repo.ListMembers(ctx, guildID, cursor, limit)
	if err != nil {
		return nil, 0, err
	}
	out := make([]*guildv1.GuildMember, 0, len(rows))
	for _, m := range rows {
		out = append(out, &guildv1.GuildMember{
			PlayerId: m.PlayerID,
			Role:     guildv1.GuildRole(m.Role),
			JoinedMs: m.JoinedMs,
		})
	}
	var next uint64
	if len(rows) == limit {
		next = rows[len(rows)-1].PlayerID
	}
	return out, next, nil
}

// ListJoinRequests 列公会挂起申请。requesterID 须为该公会 LEADER / OFFICER。按 request_id 升序游标分页。
func (u *GuildUsecase) ListJoinRequests(ctx context.Context, requesterID, cursor uint64, limit int) ([]*guildv1.GuildJoinRequest, uint64, error) {
	limit = clampLimit(limit)
	m, ok, err := u.repo.GetMember(ctx, requesterID)
	if err != nil {
		return nil, 0, err
	}
	if !ok {
		return nil, 0, errcode.New(errcode.ErrGuildNotMember, "player %d not in any guild", requesterID)
	}
	if m.Role != data.GuildRoleLeader && m.Role != data.GuildRoleOfficer {
		return nil, 0, errcode.New(errcode.ErrGuildNoPermission, "only leader/officer can list requests")
	}
	rows, err := u.repo.ListPendingRequests(ctx, m.GuildID, cursor, limit)
	if err != nil {
		return nil, 0, err
	}
	playerIDs := make([]uint64, 0, len(rows))
	for _, request := range rows {
		if request.PlayerID != 0 {
			playerIDs = append(playerIDs, request.PlayerID)
		}
	}
	projection, err := playerdisplay.Resolve(ctx, playerIDs, u.playerNameResolver, u.playerNoResolver)
	if err != nil {
		return nil, 0, err
	}
	for _, failure := range projection.Failures {
		plog.With(ctx).Warnw("msg", "guild_join_request_display_resolve_failed",
			"dependency", failure.Dependency,
			"batch_size", len(failure.PlayerIDs),
			"err", failure.Err)
	}
	out := make([]*guildv1.GuildJoinRequest, 0, len(rows))
	for _, rq := range rows {
		out = append(out, &guildv1.GuildJoinRequest{
			RequestId:    rq.RequestID,
			GuildId:      rq.GuildID,
			FromPlayerId: rq.PlayerID,
			FromNickname: projection.Names[rq.PlayerID],
			CreatedMs:    rq.CreatedMs,
			FromPlayerNo: projection.Numbers[rq.PlayerID],
		})
	}
	var next uint64
	if len(rows) == limit {
		next = rows[len(rows)-1].RequestID
	}
	return out, next, nil
}

// fanoutToManagers 把事件推给公会的会长 + 官员(申请通知用),排除 evt.ActorId 本人。
func (u *GuildUsecase) fanoutToManagers(ctx context.Context, guildID uint64, evt *guildv1.GuildEvent) {
	if u.pusher == nil {
		return
	}
	members, err := u.repo.ListMembers(ctx, guildID, 0, 0)
	if err != nil {
		plog.With(ctx).Warnw("msg", "guild_fanout_managers_failed", "guild_id", guildID, "err", err)
		return
	}
	for _, m := range members {
		if m.Role != data.GuildRoleLeader && m.Role != data.GuildRoleOfficer {
			continue
		}
		if m.PlayerID == evt.GetActorId() {
			continue
		}
		u.push(ctx, m.PlayerID, evt)
	}
}

// push 发一条公会事件给接收方(弱依赖,nil / 失败只 warn)。
func (u *GuildUsecase) push(ctx context.Context, toPlayerID uint64, evt *guildv1.GuildEvent) {
	if u.pusher == nil || toPlayerID == 0 {
		return
	}
	e := &guildv1.GuildEvent{
		Type:       evt.GetType(),
		GuildId:    evt.GetGuildId(),
		ToPlayerId: toPlayerID,
		ActorId:    evt.GetActorId(),
		GuildName:  evt.GetGuildName(),
	}
	if err := u.pusher.PushGuildEvent(ctx, toPlayerID, e); err != nil {
		plog.With(ctx).Warnw("msg", "guild_push_failed",
			"to_player_id", toPlayerID, "type", evt.GetType(), "err", err)
	}
}

// invalidateGuild 删公会资料缓存(写后删,弱依赖;删失败仅 warn,靠短 TTL 兜底)。
func (u *GuildUsecase) invalidateGuild(ctx context.Context, guildID uint64) {
	if u.cache == nil || guildID == 0 {
		return
	}
	if err := u.cache.DelGuild(ctx, guildID); err != nil {
		plog.With(ctx).Warnw("msg", "guild_cache_del_failed", "guild_id", guildID, "err", err)
	}
}

// invalidateMember 删玩家→guild_id 反查缓存(入会 / 退会 / 踢人 / 解散后)。
func (u *GuildUsecase) invalidateMember(ctx context.Context, playerID uint64) {
	if u.cache == nil || playerID == 0 {
		return
	}
	if err := u.cache.DelMember(ctx, playerID); err != nil {
		if adm, streak := u.cacheWriteLog.Admit(time.Now().UnixMilli(), guildCacheLogWindowMs); adm {
			plog.With(ctx).Warnw("msg", "guild_member_cache_del_failed",
				"player_id", playerID, "streak", streak, "err", err)
		}
	}
}

// fillGuildCache 回填公会资料缓存(读 miss 后;失败仅少命中,不报错给上层)。
func (u *GuildUsecase) fillGuildCache(ctx context.Context, g *data.GuildRow) {
	if u.cache == nil || g == nil {
		return
	}
	if err := u.cache.SetGuild(ctx, g, u.cacheTTL); err != nil {
		// 读 miss 回填在热读路径上:Redis 宕机时 get 失败紧跟 set 也失败,同一请求叠两条 → 限流。
		if adm, streak := u.cacheWriteLog.Admit(time.Now().UnixMilli(), guildCacheLogWindowMs); adm {
			plog.With(ctx).Warnw("msg", "guild_cache_set_failed",
				"guild_id", g.GuildID, "streak", streak, "err", err)
		}
	} else if n, _ := u.cacheWriteLog.Recovered(); n > 0 {
		plog.With(ctx).Infow("msg", "guild_cache_write_recovered", "failed_total", n)
	}
}

// fillMemberCache 回填玩家→guild_id 反查缓存(guildID=0 时为不在公会,SetMemberGuildID 自身跳过)。
func (u *GuildUsecase) fillMemberCache(ctx context.Context, playerID, guildID uint64) {
	if u.cache == nil {
		return
	}
	if err := u.cache.SetMemberGuildID(ctx, playerID, guildID, u.cacheTTL); err != nil {
		if adm, streak := u.cacheWriteLog.Admit(time.Now().UnixMilli(), guildCacheLogWindowMs); adm {
			plog.With(ctx).Warnw("msg", "guild_member_cache_set_failed",
				"player_id", playerID, "streak", streak, "err", err)
		}
	}
}

// toGuildView 把存储行映射成客户端可见 Guild(CLAUDE.md §14)。
func toGuildView(g *data.GuildRow) *guildv1.Guild {
	return &guildv1.Guild{
		GuildId:     g.GuildID,
		Name:        g.Name,
		LeaderId:    g.LeaderID,
		MemberCount: g.MemberCount,
		MaxMembers:  g.MaxMembers,
		CreatedMs:   g.CreatedMs,
	}
}
