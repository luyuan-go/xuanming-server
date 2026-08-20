// Package service 是 team 服务的 gRPC service 层(W3 ⑦ Phase 4,2026-06-05)。
//
// 职责:
//   - 实现 teamv1.TeamServiceServer 接口
//   - 从 ctx 取 JWT player_id(R5:override request 字段,防伪造他人身份)
//   - proto Request/Response ↔ biz 入参/出参互转(R1:Response 包含完整 Team 快照)
//   - errcode.Code → commonv1.ErrCode 1:1 映射
//
// 协议原则(R5):所有写 RPC 强制用 ctx 中的 player_id 覆盖 request 的对应字段。
// player_id=0 时返回 ERR_UNAUTHORIZED(Envoy jwt_authn 已在路由层 require JWT)。
package service

import (
	"context"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/middleware"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"

	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/biz"
)

// TeamService 实现 teamv1.TeamServiceServer。
type TeamService struct {
	teamv1.UnimplementedTeamServiceServer
	uc *biz.TeamUsecase

	// team_id 与 invite_id 是两个互不相干的 ID 空间(各自独立的 Redis key 前缀),
	// 各持一个独立发号器,各走各的 step 池。
	//
	// ⚠️ 两者共用同一 nodeID,发出的 ID 会逐位相同(见 etcdnode.ProvideSnowflakeN)。
	// team_id 与 invite_id 必须各自留在自己的 key 空间里,禁止混进同一个 map / 唯一键比较。
	teamSF   snowflakeGen
	inviteSF snowflakeGen

	// dsGuard DS 回调令牌守卫;nil = 未启用(mode=off),行为与接线前一致。
	//
	// systemOnly 只能证明「本次调用不带玩家 JWT」,证明不了「调用方真的是 DS」——:8444
	// 没有 jwt_authn,任何能连到网关或直连本服务端口的东西都满足 callerID==0。GetPlayerTeam
	// 是「查任意 player_id 在哪支队」的接口,只靠 systemOnly 等于把它对整个集群内网敞开。
	// 与 login / battle_result / ds_allocator / hub_allocator / inventory / player_locator
	// 同一套守卫,同一份 ds_auth 配置。
	dsGuard *middleware.DSCallbackGuard

	// matchCallAuth 校验 BeginTeamMatch / EndTeamMatch 确实来自 matchmaker(见 match_call_auth.go)。
	// nil = 未配密钥,整道跳过;matchCallRequire=false 时验不过只 WARN 放行(观察期)。
	matchCallAuth    internalRequestVerifier
	matchCallRequire bool
}

// SetDSCallbackGuard 由 main.go 在构造后注入;不调用即 mode=off。
func (s *TeamService) SetDSCallbackGuard(g *middleware.DSCallbackGuard) { s.dsGuard = g }

// snowflakeGen 是 snowflake.Node 的最小接口,避免 service 直接依赖 snowflake 包。
type snowflakeGen interface {
	Generate() uint64
}

// NewTeamService 构造 TeamService。
//
// teamSF / inviteSF 由 main.go 经 etcdnode.MustProvideSnowflakeN 取得。测试可传同一实例;
// 生产接线必须分开,否则失去拆分带来的容量隔离。
func NewTeamService(uc *biz.TeamUsecase, teamSF, inviteSF snowflakeGen) *TeamService {
	return &TeamService{uc: uc, teamSF: teamSF, inviteSF: inviteSF}
}

// ── 14 RPC(9 个原有 + 5 个"找队伍") ──────────────────────────────────────────

// CreateTeam 创建队伍。player_id 以 JWT ctx 为准(R5)。
func (s *TeamService) CreateTeam(ctx context.Context, _ *teamv1.CreateTeamRequest) (*teamv1.CreateTeamResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		logRPCRejected(ctx, "CreateTeam", reasonUnauthenticated)
		return &teamv1.CreateTeamResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}

	teamID := s.teamSF.Generate()
	// R3:team_id 是本链路的 join key,刚发号就写进 ctx —— biz / data 层后续每条日志
	// 由 plog.With(ctx) 自动带上,不必逐处手写(WithTeamID 历史上全仓零调用)。
	ctx = plog.WithTeamID(ctx, teamID)
	rec, err := s.uc.CreateTeam(ctx, teamID, playerID)
	if err != nil {
		return &teamv1.CreateTeamResponse{Code: toProtoCode(err)}, nil
	}
	teamView, err := s.uc.TeamToProto(ctx, rec)
	if err != nil {
		return nil, err
	}
	return &teamv1.CreateTeamResponse{
		Code:   commonv1.ErrCode_OK,
		TeamId: rec.TeamId,
		Team:   teamView,
	}, nil
}

// Invite 邀请玩家。inviter_id 以 JWT ctx 为准(R5)。
func (s *TeamService) Invite(ctx context.Context, req *teamv1.InviteRequest) (*teamv1.InviteResponse, error) {
	inviterID := callerID(ctx)
	if inviterID == 0 {
		logRPCRejected(ctx, "Invite", reasonUnauthenticated)
		return &teamv1.InviteResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetTeamId() == 0 || req.GetTargetPlayerId() == 0 {
		// 一个 if 两个条件 → 两个 reason(R2):缺 team_id 是客户端队伍态丢了,
		// 缺 target 是选人 UI 没传上来,排查方向完全不同。
		reason := reasonMissingTeamID
		if req.GetTeamId() != 0 {
			reason = reasonMissingTargetPlayerID
		}
		logRPCRejected(ctx, "Invite", reason,
			"team_id", req.GetTeamId(), "target_player_id", req.GetTargetPlayerId())
		return &teamv1.InviteResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	ctx = plog.WithTeamID(ctx, req.GetTeamId())

	inviteID := s.inviteSF.Generate()
	rec, err := s.uc.Invite(ctx, inviteID, req.GetTeamId(), inviterID, req.GetTargetPlayerId())
	if err != nil {
		return &teamv1.InviteResponse{Code: toProtoCode(err)}, nil
	}
	// expires_at_ms 以"现在"为锚点,与 biz.SetInvite 写 redis 的 TTL 起算点一致;
	// 不能用 rec.UpdatedAtMs(那是队伍上次变更时间,Invite 不改队伍,会偏早过期)。
	expiresAtMs := time.Now().UnixMilli() + s.uc.InviteTTLMs()
	teamView, err := s.uc.TeamToProto(ctx, rec)
	if err != nil {
		return nil, err
	}
	return &teamv1.InviteResponse{
		Code:        commonv1.ErrCode_OK,
		Team:        teamView,
		InviteId:    inviteID,
		ExpiresAtMs: expiresAtMs,
	}, nil
}

// AcceptInvite 接受邀请。player_id 以 JWT ctx 为准(R5)。
func (s *TeamService) AcceptInvite(ctx context.Context, req *teamv1.AcceptInviteRequest) (*teamv1.AcceptInviteResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		logRPCRejected(ctx, "AcceptInvite", reasonUnauthenticated)
		return &teamv1.AcceptInviteResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetTeamId() == 0 {
		logRPCRejected(ctx, "AcceptInvite", reasonMissingTeamID, "invite_id", req.GetInviteId())
		return &teamv1.AcceptInviteResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	ctx = plog.WithTeamID(ctx, req.GetTeamId())

	rec, err := s.uc.AcceptInvite(ctx, req.GetInviteId(), req.GetTeamId(), playerID)
	if err != nil {
		return &teamv1.AcceptInviteResponse{Code: toProtoCode(err)}, nil
	}
	teamView, err := s.uc.TeamToProto(ctx, rec)
	if err != nil {
		return nil, err
	}
	return &teamv1.AcceptInviteResponse{
		Code: commonv1.ErrCode_OK,
		Team: teamView,
	}, nil
}

// LeaveTeam 离队。player_id 以 JWT ctx 为准(R5)。
func (s *TeamService) LeaveTeam(ctx context.Context, req *teamv1.LeaveTeamRequest) (*teamv1.LeaveTeamResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		logRPCRejected(ctx, "LeaveTeam", reasonUnauthenticated)
		return &teamv1.LeaveTeamResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetTeamId() == 0 {
		logRPCRejected(ctx, "LeaveTeam", reasonMissingTeamID)
		return &teamv1.LeaveTeamResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	ctx = plog.WithTeamID(ctx, req.GetTeamId())

	rec, err := s.uc.LeaveTeam(ctx, req.GetTeamId(), playerID)
	if err != nil {
		return &teamv1.LeaveTeamResponse{Code: toProtoCode(err)}, nil
	}
	teamView, err := s.uc.TeamToProto(ctx, rec)
	if err != nil {
		return nil, err
	}
	return &teamv1.LeaveTeamResponse{
		Code: commonv1.ErrCode_OK,
		Team: teamView,
	}, nil
}

// Kick 踢人。captain_id 以 JWT ctx 为准(R5)。
func (s *TeamService) Kick(ctx context.Context, req *teamv1.KickRequest) (*teamv1.KickResponse, error) {
	captainID := callerID(ctx)
	if captainID == 0 {
		logRPCRejected(ctx, "Kick", reasonUnauthenticated)
		return &teamv1.KickResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetTeamId() == 0 || req.GetTargetPlayerId() == 0 {
		reason := reasonMissingTeamID
		if req.GetTeamId() != 0 {
			reason = reasonMissingTargetPlayerID
		}
		logRPCRejected(ctx, "Kick", reason,
			"team_id", req.GetTeamId(), "target_player_id", req.GetTargetPlayerId())
		return &teamv1.KickResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	ctx = plog.WithTeamID(ctx, req.GetTeamId())

	rec, err := s.uc.Kick(ctx, req.GetTeamId(), captainID, req.GetTargetPlayerId())
	if err != nil {
		return &teamv1.KickResponse{Code: toProtoCode(err)}, nil
	}
	teamView, err := s.uc.TeamToProto(ctx, rec)
	if err != nil {
		return nil, err
	}
	return &teamv1.KickResponse{
		Code: commonv1.ErrCode_OK,
		Team: teamView,
	}, nil
}

// SetReady 设置准备状态。player_id 以 JWT ctx 为准(R5)。
func (s *TeamService) SetReady(ctx context.Context, req *teamv1.SetReadyRequest) (*teamv1.SetReadyResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		logRPCRejected(ctx, "SetReady", reasonUnauthenticated, "want_ready", req.GetReady())
		return &teamv1.SetReadyResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetTeamId() == 0 {
		logRPCRejected(ctx, "SetReady", reasonMissingTeamID, "want_ready", req.GetReady())
		return &teamv1.SetReadyResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	ctx = plog.WithTeamID(ctx, req.GetTeamId())

	rec, err := s.uc.SetReady(ctx, req.GetTeamId(), playerID, req.GetReady(), req.GetHeroId())
	if err != nil {
		return &teamv1.SetReadyResponse{Code: toProtoCode(err)}, nil
	}
	teamView, err := s.uc.TeamToProto(ctx, rec)
	if err != nil {
		return nil, err
	}
	return &teamv1.SetReadyResponse{
		Code: commonv1.ErrCode_OK,
		Team: teamView,
	}, nil
}

// GetTeam 查询队伍(只读,无鉴权要求,team_id 即授权)。
func (s *TeamService) GetTeam(ctx context.Context, req *teamv1.GetTeamRequest) (*teamv1.GetTeamResponse, error) {
	if req.GetTeamId() == 0 {
		logRPCRejected(ctx, "GetTeam", reasonMissingTeamID)
		return &teamv1.GetTeamResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	ctx = plog.WithTeamID(ctx, req.GetTeamId())

	rec, err := s.uc.GetTeam(ctx, req.GetTeamId())
	if err != nil {
		return &teamv1.GetTeamResponse{Code: toProtoCode(err)}, nil
	}
	teamView, err := s.uc.TeamToProto(ctx, rec)
	if err != nil {
		return nil, err
	}
	return &teamv1.GetTeamResponse{
		Code: commonv1.ErrCode_OK,
		Team: teamView,
	}, nil
}

// GetMyTeam 查询自己当前所在队伍的完整快照(队伍主界面直接渲染)。player_id 以 JWT ctx 为准(R5)。
// 没队伍是正常态:返 OK + has_team_msg=false,不用 errcode 表达。
func (s *TeamService) GetMyTeam(ctx context.Context, _ *teamv1.GetMyTeamRequest) (*teamv1.GetMyTeamResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		logRPCRejected(ctx, "GetMyTeam", reasonUnauthenticated)
		return &teamv1.GetMyTeamResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}

	rec, hasTeam, err := s.uc.GetMyTeam(ctx, playerID)
	if err != nil {
		return &teamv1.GetMyTeamResponse{Code: toProtoCode(err)}, nil
	}
	if !hasTeam {
		return &teamv1.GetMyTeamResponse{Code: commonv1.ErrCode_OK, HasTeamMsg: false}, nil
	}
	teamView, err := s.uc.TeamToProto(ctx, rec)
	if err != nil {
		return nil, err
	}
	return &teamv1.GetMyTeamResponse{
		Code:       commonv1.ErrCode_OK,
		HasTeamMsg: true,
		Team:       teamView,
	}, nil
}

// ListMyPendingInvites 查询"发给我的待处理邀请"(只读,拉取兜底)。player_id 以 JWT ctx 为准(R5)。
// 推送(TeamInviteEvent)只是投影/加速器,这里才是邀请令牌的权威查询(不变量 §9-22):
// 客户端在登录、回前台、打开组队 UI 时调用,推送丢帧最多延迟弹窗,不丢邀请。
// 没有邀请是正常态:返 OK + 空列表。
func (s *TeamService) ListMyPendingInvites(ctx context.Context, _ *teamv1.ListMyPendingInvitesRequest) (*teamv1.ListMyPendingInvitesResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		logRPCRejected(ctx, "ListMyPendingInvites", reasonUnauthenticated)
		return &teamv1.ListMyPendingInvitesResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}

	recs, err := s.uc.ListPendingInvites(ctx, playerID)
	if err != nil {
		return &teamv1.ListMyPendingInvitesResponse{Code: toProtoCode(err)}, nil
	}
	invites := make([]*teamv1.PendingInvite, 0, len(recs))
	for _, r := range recs {
		invites = append(invites, &teamv1.PendingInvite{
			TeamId:      r.TeamID,
			InviteId:    r.InviteID,
			InviterId:   r.InviterID,
			ExpiresAtMs: r.ExpiresAtMs,
		})
	}
	return &teamv1.ListMyPendingInvitesResponse{
		Code:    commonv1.ErrCode_OK,
		Invites: invites,
	}, nil
}

// ── 找队伍:列表 / 申请 / 审批 ─────────────────────────────────────────────────

// SetTeamMap 队长设置本队目标关卡。captain_id 以 JWT ctx 为准(R5)。
// map_id=0 合法(清空/未指定),因此不做非零校验。
func (s *TeamService) SetTeamMap(ctx context.Context, req *teamv1.SetTeamMapRequest) (*teamv1.SetTeamMapResponse, error) {
	captainID := callerID(ctx)
	if captainID == 0 {
		logRPCRejected(ctx, "SetTeamMap", reasonUnauthenticated, "map_id", req.GetMapId())
		return &teamv1.SetTeamMapResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetTeamId() == 0 {
		logRPCRejected(ctx, "SetTeamMap", reasonMissingTeamID, "map_id", req.GetMapId())
		return &teamv1.SetTeamMapResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	ctx = plog.WithTeamID(ctx, req.GetTeamId())

	rec, err := s.uc.SetTeamMap(ctx, req.GetTeamId(), captainID, req.GetMapId())
	if err != nil {
		return &teamv1.SetTeamMapResponse{Code: toProtoCode(err)}, nil
	}
	teamView, err := s.uc.TeamToProto(ctx, rec)
	if err != nil {
		return nil, err
	}
	return &teamv1.SetTeamMapResponse{
		Code: commonv1.ErrCode_OK,
		Team: teamView,
	}, nil
}

// ListOpenTeams 列正在招募的队伍(只读)。player_id 以 JWT ctx 为准(R5)。
//
// 要求登录:这是面向客户端的枚举接口(GetTeam 那种"知道 team_id 即授权"的口径不适用),
// 未登录一律拒。map_id=0 表示不限关卡;limit 由 biz 钳到 max_open_teams_per_query。
// 没有开放队伍是正常态:返 OK + 空列表,不用 errcode 表达。
func (s *TeamService) ListOpenTeams(ctx context.Context, req *teamv1.ListOpenTeamsRequest) (*teamv1.ListOpenTeamsResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		logRPCRejected(ctx, "ListOpenTeams", reasonUnauthenticated, "map_id", req.GetMapId())
		return &teamv1.ListOpenTeamsResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}

	teams, err := s.uc.ListOpenTeams(ctx, req.GetMapId(), int(req.GetLimit()))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return &teamv1.ListOpenTeamsResponse{Code: toProtoCode(err)}, nil
	}
	return &teamv1.ListOpenTeamsResponse{
		Code:  commonv1.ErrCode_OK,
		Teams: teams,
	}, nil
}

// ApplyToTeam 申请加入队伍。player_id 以 JWT ctx 为准(R5)。
// 走"申请待审批"还是"直接入队"由服务端配置决定,客户端不选也不能选。
func (s *TeamService) ApplyToTeam(ctx context.Context, req *teamv1.ApplyToTeamRequest) (*teamv1.ApplyToTeamResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		logRPCRejected(ctx, "ApplyToTeam", reasonUnauthenticated, "team_id", req.GetTeamId())
		return &teamv1.ApplyToTeamResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetTeamId() == 0 {
		logRPCRejected(ctx, "ApplyToTeam", reasonMissingTeamID)
		return &teamv1.ApplyToTeamResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	ctx = plog.WithTeamID(ctx, req.GetTeamId())

	joined, rec, expiresAtMs, err := s.uc.ApplyToTeam(ctx, req.GetTeamId(), playerID)
	if err != nil {
		return &teamv1.ApplyToTeamResponse{Code: toProtoCode(err)}, nil
	}
	resp := &teamv1.ApplyToTeamResponse{
		Code:        commonv1.ErrCode_OK,
		Joined:      joined,
		ExpiresAtMs: expiresAtMs,
	}
	if joined {
		resp.Team, err = s.uc.TeamToProto(ctx, rec)
		if err != nil {
			return nil, err
		}
	}
	return resp, nil
}

// ListTeamApplications 队长查本队待处理入队申请(只读)。captain_id 以 JWT ctx 为准(R5)。
// 没有申请是正常态:返 OK + 空列表。非队长返 ERR_TEAM_NOT_CAPTAIN。
func (s *TeamService) ListTeamApplications(ctx context.Context, req *teamv1.ListTeamApplicationsRequest) (*teamv1.ListTeamApplicationsResponse, error) {
	captainID := callerID(ctx)
	if captainID == 0 {
		logRPCRejected(ctx, "ListTeamApplications", reasonUnauthenticated, "team_id", req.GetTeamId())
		return &teamv1.ListTeamApplicationsResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetTeamId() == 0 {
		logRPCRejected(ctx, "ListTeamApplications", reasonMissingTeamID)
		return &teamv1.ListTeamApplicationsResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	ctx = plog.WithTeamID(ctx, req.GetTeamId())

	recs, err := s.uc.ListTeamApplications(ctx, req.GetTeamId(), captainID)
	if err != nil {
		return &teamv1.ListTeamApplicationsResponse{Code: toProtoCode(err)}, nil
	}
	applications, err := s.uc.TeamApplicationsToProto(ctx, req.GetTeamId(), recs)
	if err != nil {
		return nil, err
	}
	return &teamv1.ListTeamApplicationsResponse{
		Code:         commonv1.ErrCode_OK,
		Applications: applications,
	}, nil
}

// HandleTeamApplication 队长同意 / 拒绝一份入队申请。captain_id 以 JWT ctx 为准(R5)。
func (s *TeamService) HandleTeamApplication(ctx context.Context, req *teamv1.HandleTeamApplicationRequest) (*teamv1.HandleTeamApplicationResponse, error) {
	captainID := callerID(ctx)
	if captainID == 0 {
		logRPCRejected(ctx, "HandleTeamApplication", reasonUnauthenticated,
			"team_id", req.GetTeamId(), "applicant_id", req.GetApplicantId())
		return &teamv1.HandleTeamApplicationResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetTeamId() == 0 || req.GetApplicantId() == 0 {
		reason := reasonMissingTeamID
		if req.GetTeamId() != 0 {
			reason = reasonMissingApplicantID
		}
		logRPCRejected(ctx, "HandleTeamApplication", reason,
			"team_id", req.GetTeamId(), "applicant_id", req.GetApplicantId(), "accept", req.GetAccept())
		return &teamv1.HandleTeamApplicationResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	ctx = plog.WithTeamID(ctx, req.GetTeamId())

	rec, err := s.uc.HandleTeamApplication(ctx, req.GetTeamId(), captainID, req.GetApplicantId(), req.GetAccept())
	if err != nil {
		return &teamv1.HandleTeamApplicationResponse{Code: toProtoCode(err)}, nil
	}
	teamView, err := s.uc.TeamToProto(ctx, rec)
	if err != nil {
		return nil, err
	}
	return &teamv1.HandleTeamApplicationResponse{
		Code: commonv1.ErrCode_OK,
		Team: teamView,
	}, nil
}

// ── 辅助 ──────────────────────────────────────────────────────────────────────

// callerID 从 ctx 取 JWT 注入的 player_id。
func callerID(ctx context.Context) uint64 {
	id, _ := ctx.Value(plog.CtxKeyPlayerID).(uint64)
	return id
}

// systemOnly 内部东西向接口鉴权:带玩家 JWT 的调用一律拒。
//
// Envoy 是按 /pandora.team.v1.TeamService/ **整前缀**路由到本服务的,没有按方法的白名单,
// 因此"内部方法"在客户端面同样可达。少了这道门,GetPlayerTeam 就是一个"查任意玩家在哪支队"
// 的 IDOR 口子。拒绝必须留痕:这类分支以 response Code + nil error 返回,统一 access log
// 会记成 rpc_ok,不打日志则线上对越权尝试零可见性(与 player 服务 logAuthzDeny 同口径)。
func systemOnly(ctx context.Context) commonv1.ErrCode {
	if id := callerID(ctx); id != 0 {
		plog.With(ctx).Warnw("msg", "team_authz_denied",
			"reason", reasonSystemRPCByClient, "caller_id", id)
		return commonv1.ErrCode_ERR_PERMISSION_DENY
	}
	return commonv1.ErrCode_OK
}

// toProtoCode 把 pkg/errcode 1:1 映射成 proto enum(数值相同)。
func toProtoCode(err error) commonv1.ErrCode {
	return commonv1.ErrCode(errcode.As(err))
}

// BeginTeamMatch 组票前的 roster fence(matchmaker 专用,内部东西向)。
//
// 与其它 RPC 的区别:它不是客户端面接口,身份不取 JWT —— 调用方是 matchmaker,
// captain_id 由它从已校验的 JWT sub 透传过来,team 侧仍会复核那确实是本队队长
// (§9.6 派生判定服务端重算,不因为「内部调用」就免检)。
func (s *TeamService) BeginTeamMatch(ctx context.Context, req *teamv1.BeginTeamMatchRequest) (*teamv1.BeginTeamMatchResponse, error) {
	// 本方法此前**一道守卫都没有**(2026-08-13 补)。Envoy 按 `/pandora.team.v1.TeamService/`
	// 整前缀路由,带玩家 JWT 的客户端同样打得到它 —— 而它能给**任意**队伍上一把 roster 租约,
	// 反复调用即可让那支队伍始终处于「被别人的组票占住」状态,队长自己反而开不了局。
	// 这与 GetPlayerTeam 是同一类东西向接口,用同一道门。
	if code := systemOnly(ctx); code != commonv1.ErrCode_OK {
		return &teamv1.BeginTeamMatchResponse{Code: code}, nil
	}
	// systemOnly 只挡住「带玩家 JWT 的客户端」;这一道才校验调用方**确实是 matchmaker**。
	if code := s.verifyMatchCall(ctx, teamv1.TeamService_BeginTeamMatch_FullMethodName, req.GetTeamId()); code != commonv1.ErrCode_OK {
		return &teamv1.BeginTeamMatchResponse{Code: code}, nil
	}
	// R3:内部东西向面既没有玩家 JWT(player_id 不会自动注入)、也没人写过 team_id。
	// 两个 key 必须在这里手写进 ctx,否则 matchmaker→team 这一段在日志里与队长本人
	// 完全串不起来 —— "队长点了开始匹配之后发生了什么"就断在服务边界上。
	// ⚠️ 必须放在 systemOnly / verifyMatchCall **之后**:systemOnly 判的就是
	// ctx 里有没有 player_id,提前写进去会把这道门直接打穿。
	ctx = plog.WithTeamID(ctx, req.GetTeamId())
	ctx = plog.WithPlayerID(ctx, req.GetCaptainId())
	// require_ready 由 matchmaker 按本次 map_id 的关卡表 ready_mode 解析后传入;
	// 旧 matchmaker 不发 → false → 无门槛(与该字段上线前一致,§9.21 共存安全)。
	team, expiresAtMs, err := s.uc.BeginTeamMatch(ctx,
		req.GetTeamId(), req.GetCaptainId(), req.GetOperationId(), req.GetLeaseMs(), req.GetRequireReady())
	if err != nil {
		return &teamv1.BeginTeamMatchResponse{Code: toProtoCode(err)}, nil
	}
	teamView, err := s.uc.TeamToProto(ctx, team)
	if err != nil {
		return nil, err
	}
	return &teamv1.BeginTeamMatchResponse{
		Code:             commonv1.ErrCode_OK,
		Team:             teamView,
		LeaseExpiresAtMs: expiresAtMs,
		// 冻结这份名单那一刻的 ready 代际。matchmaker 必须原样带进 match 记录,
		// 并在 EndTeamMatch 回传 —— 它是「这次复位对应的正是这一局」的唯一凭据。
		ReadyGeneration: team.GetReadyGeneration(),
	}, nil
}

// EndTeamMatch 对局结束后复位队伍准备状态(matchmaker 专用,内部东西向)。
//
// **必须 systemOnly**,与 BeginTeamMatch 不同:Begin 拿队长身份当授权(只有本队队长
// 能给自己队上一把 5s 自净的租约,越权收益近乎为零);而本方法能把任意队伍打回 FORMING,
// 客户端可达就等于一个「让任何队伍开不了局」的骚扰口子。Envoy 是按
// /pandora.team.v1.TeamService/ 整前缀放行的,不显式拒就是对客户端开放。
func (s *TeamService) EndTeamMatch(ctx context.Context, req *teamv1.EndTeamMatchRequest) (*teamv1.EndTeamMatchResponse, error) {
	if code := systemOnly(ctx); code != commonv1.ErrCode_OK {
		return &teamv1.EndTeamMatchResponse{Code: code}, nil
	}
	if code := s.verifyMatchCall(ctx, teamv1.TeamService_EndTeamMatch_FullMethodName, req.GetTeamId()); code != commonv1.ErrCode_OK {
		return &teamv1.EndTeamMatchResponse{Code: code}, nil
	}
	// R3:同 BeginTeamMatch —— 内部面不会自动带 team_id。放在两道门之后。
	ctx = plog.WithTeamID(ctx, req.GetTeamId())
	if err := s.uc.EndTeamMatch(ctx, req.GetTeamId(), req.GetPlayerIds(), req.GetExpectedReadyGeneration()); err != nil {
		return &teamv1.EndTeamMatchResponse{Code: toProtoCode(err)}, nil
	}
	return &teamv1.EndTeamMatchResponse{Code: commonv1.ErrCode_OK}, nil
}

// GetPlayerTeam 按 player_id 反查队伍编号(内部东西向,DS 出生编制专用)。
//
// DS 在玩家进场时把队伍编号写到实体上并复制给全场客户端,「谁和谁是一伙的」才有权威来源;
// 否则大厅里所有玩家共用玩家阵营,队友与路人无从区分。
// 只回编号不回名单:DS 只判定「同队与否」,发整份 Team 快照是无谓的扩面(§9.14)。
func (s *TeamService) GetPlayerTeam(ctx context.Context, req *teamv1.GetPlayerTeamRequest) (*teamv1.GetPlayerTeamResponse, error) {
	if code := systemOnly(ctx); code != commonv1.ErrCode_OK {
		return &teamv1.GetPlayerTeamResponse{Code: code}, nil
	}
	// 令牌门。RequireToken:本方法只可能来自 DS —— 全仓没有任何内部 Go 服务调它
	// (mail 的同名 repo 方法查的是自己的 MySQL,与本 RPC 无关),故直连无令牌也一律拒,
	// 堵住「被攻破的业务 Pod 绕过 Envoy 直连 20010、无标记无令牌被当东西向内部信任」。
	// 不绑 Type:大厅与对局都可能查(公会两种玩法都要,队伍虽只有大厅查,但绑死 hub 会让
	// 将来任何新玩法的合法查询变成鉴权失败,而这里的范围收益是零 —— 令牌本身已证明是 DS)。
	// 不绑 Pod / MatchID:反查与哪台 DS、哪一局无关,填了只会把合法调用拒掉。
	if err := s.dsGuard.Check(ctx, middleware.DSScope{RequireToken: true}); err != nil {
		return &teamv1.GetPlayerTeamResponse{Code: toProtoCode(err)}, nil
	}
	playerID := req.GetPlayerId()
	if playerID == 0 {
		logRPCRejected(ctx, "GetPlayerTeam", reasonMissingPlayerID)
		return &teamv1.GetPlayerTeamResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	// R3:DS 回调面走 AuthOptional 且 DS 不带 x-pandora-player-id —— player_id 必须手写,
	// 否则"某个玩家进场后队友颜色不对"在 team 侧完全定位不到人。
	// 放在 systemOnly / dsGuard **之后**(systemOnly 判的就是 ctx 里有没有 player_id)。
	ctx = plog.WithPlayerID(ctx, playerID)

	teamID, hasTeam, err := s.uc.GetPlayerTeamID(ctx, playerID)
	if err != nil {
		return &teamv1.GetPlayerTeamResponse{Code: toProtoCode(err)}, nil
	}
	if !hasTeam {
		// 没队伍是正常态,不是错误:DS 据此把实体的队伍编号留在「未知」。
		return &teamv1.GetPlayerTeamResponse{Code: commonv1.ErrCode_OK, HasTeam: false}, nil
	}
	return &teamv1.GetPlayerTeamResponse{
		Code:    commonv1.ErrCode_OK,
		HasTeam: true,
		TeamId:  teamID,
	}, nil
}
