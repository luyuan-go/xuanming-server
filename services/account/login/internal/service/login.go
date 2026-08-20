// Package service 是 login 服务的 RPC 入口层。
//
// 职责:
//   - 实现 loginv1.LoginServiceServer 接口
//   - proto Request/Response 与 biz 入参/出参互转
//   - errcode.*Error 翻译成 proto.LoginResponse.code(不抛 grpc error,客户端永远看 code 字段)
//
// 不变量(docs/design/protocol-ordering-rules.md 原则 1):
//   - "立即完成型 RPC" 的 response 必须包含完整业务数据,客户端不等任何后续 push
package service

import (
	"context"
	"strings"

	"github.com/go-kratos/kratos/v2/transport"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/middleware"

	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	loginv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/login/v1"

	"github.com/luyuancpp/pandora/services/account/login/internal/biz"
	"github.com/luyuancpp/pandora/services/account/login/internal/data"
)

// LoginService 实现 loginv1.LoginServiceServer。
//
// 内嵌 UnimplementedLoginServiceServer 以满足 grpc 向前兼容约束。
//
// W3 ①(2026-06-05):IssueDSTicket / VerifyDSTicket 接 pkg/auth 真实化。
// Login() 返回的 session_token / hub_ticket 也都是 HS256 JWT(由 LoginUsecase 内部签)。
type LoginService struct {
	loginv1.UnimplementedLoginServiceServer

	loginUC    *biz.LoginUsecase
	ticketUC   *biz.TicketUsecase
	playerNoUC *biz.PlayerNoResolveUsecase

	// redisDSAdmission 仅由 authority_mode=redis + mode=enforce 的 main 开启。
	// guard/checker 任一缺失都 fail-closed，绝不回退 legacy Verify。
	redisDSAdmission bool
	dsGuard          *middleware.DSCallbackGuard
	admissionChecker data.DSAdmissionChecker
}

// NewLoginService 注入 LoginUsecase + TicketUsecase。
func NewLoginService(loginUC *biz.LoginUsecase, ticketUC *biz.TicketUsecase) *LoginService {
	return &LoginService{loginUC: loginUC, ticketUC: ticketUC}
}

// clientIPFromHeader 读 Envoy 注入的受信客户端 IP 头(x-pandora-client-ip)。
// 与 pkg/middleware/auth.go 读 x-pandora-player-id 同模式;入站同名头由 Envoy
// header_mutation 无条件剥离,后端看到的值只可能出自 Envoy 自己(不可伪造)。
// 未经 Envoy 的直连(本机 dev / 测试)返回空,登录失败 Quota 的 IP 维度自动关闭。
func clientIPFromHeader(ctx context.Context) string {
	tr, ok := transport.FromServerContext(ctx)
	if !ok {
		return ""
	}
	return strings.TrimSpace(tr.RequestHeader().Get("x-pandora-client-ip"))
}

// SetRedisDSAdmissionAuthority 启用 VerifyDSTicket 的 DS 在线 active 权威门。
func (s *LoginService) SetRedisDSAdmissionAuthority(guard *middleware.DSCallbackGuard, checker data.DSAdmissionChecker) {
	s.redisDSAdmission = true
	s.dsGuard = guard
	s.admissionChecker = checker
}

// SetPlayerNoResolveUsecase 注入 LoginService 与内部 Team RPC 共用的账号编号批量权威读取。
func (s *LoginService) SetPlayerNoResolveUsecase(uc *biz.PlayerNoResolveUsecase) {
	s.playerNoUC = uc
}

// Login 立即完成型(参考 proto/pandora/login/v1/login.proto 注释)。
func (s *LoginService) Login(ctx context.Context, req *loginv1.LoginRequest) (*loginv1.LoginResponse, error) {
	// 受信客户端 IP(Envoy 注入 x-pandora-client-ip,入站同名头已被剥离防伪造)挂到
	// 请求 ctx,供登录失败 Quota 的 IP 维度使用;未经 Envoy(dev 直连)为空 = 该维度关闭。
	ctx = biz.WithClientIP(ctx, clientIPFromHeader(ctx))
	res, err := s.loginUC.Login(ctx, req.GetAccount(), req.GetPasswordHash(), req.GetDeviceId(),
		req.GetDeferRoleEntry())
	if err != nil {
		return &loginv1.LoginResponse{
			Code: toProtoCode(err),
		}, nil
	}
	return &loginv1.LoginResponse{
		Code:         commonv1.ErrCode_OK,
		PlayerId:     res.PlayerID,
		SessionToken: res.SessionToken,
		HubDsAddr:    res.HubDSAddr,
		HubTicket:    res.HubTicket,
		RegionId:     res.RegionID,
		CellId:       res.CellID,
		// 账号层(两步登录 2026-08-18):defer_role_entry=true 时只有这几项非空。
		AccountToken:      res.AccountToken,
		AccountId:         res.AccountID,
		AccountTokenExpMs: uint64(res.AccountTokenExpMs),
		Roles:             accountRolesToProto(res.Roles),
		// 断线重连(docs/design/battle-reconnect.md):命中时非空,客户端直连 battle DS 重连;
		// 未命中时为空(零值),客户端走 hub_ds_addr / hub_ticket 进大厅。
		BattleDsAddr: res.BattleDSAddr,
		BattleTicket: res.BattleTicket,
		MatchId:      res.MatchID,
		// 选角权威化(2026-07-08):玩家当前已选角色(0=从未选过),客户端选角界面预选中用。
		SelectedRoleId: res.SelectedRoleID,
		ResumeContext:  resumeContextToProto(res.Resume),
		// 角色编号(展示专用,player-no-and-login-surge.md §3):0=补号中,客户端显示「生成中」。
		// #13 兼容旧客户端/JSON，#14 给新客户端；旧调用方排空前必须双写同值。
		RegisterNo: res.PlayerNo,
		PlayerNo:   res.PlayerNo,
	}, nil
}

// accountRolesToProto 把 biz 的选角视图翻成 proto(客户端可见结构,§5 第 11 条:
// 由服务端按最小数据单位填充,不把存储行原样外抛)。
func accountRolesToProto(in []biz.AccountRoleView) []*loginv1.AccountRole {
	if len(in) == 0 {
		return nil
	}
	out := make([]*loginv1.AccountRole, 0, len(in))
	for _, r := range in {
		out = append(out, &loginv1.AccountRole{
			PlayerId:      r.PlayerID,
			RoleName:      r.RoleName,
			RoleId:        r.RoleID,
			PlayerNo:      r.PlayerNo,
			Level:         r.Level,
			LastLoginAtMs: r.LastLoginAtMs,
			Slot:          r.Slot,
		})
	}
	return out
}

// ListAccountRoles 立即完成型:列出本账号名下的角色(两步登录,2026-08-18)。
//
// account_id 从 ctx 读(Envoy 账号态 provider 验签后注入 x-pandora-account-id),
// 请求体为空、不信任自报身份 —— 否则任何人都能列别人账号下的角色。
// ⚠️ 该 path 必须挂在 envoy.yaml 的**账号态** provider 上:挂错成玩家态 provider 的话
// 账号 token 过不了验签,挂空则完全不验签,两种都是错的。
func (s *LoginService) ListAccountRoles(ctx context.Context, _ *loginv1.ListAccountRolesRequest) (*loginv1.ListAccountRolesResponse, error) {
	accountID := middleware.AccountIDFromContext(ctx)
	roles, err := s.loginUC.ListAccountRoles(ctx, accountID)
	if err != nil {
		return &loginv1.ListAccountRolesResponse{Code: toProtoCode(err)}, nil
	}
	return &loginv1.ListAccountRolesResponse{
		Code:  commonv1.ErrCode_OK,
		Roles: accountRolesToProto(roles),
	}, nil
}

// EnterRole 立即完成型:选定角色进入游戏(两步登录第二步,2026-08-18)。
//
// account_id 从 ctx 读(账号态 JWT sub);请求体里的 player_id 是「选哪个」而非「我是谁」,
// biz 层会按 account_roles 台账回查归属,不属于本账号一律拒。
func (s *LoginService) EnterRole(ctx context.Context, req *loginv1.EnterRoleRequest) (*loginv1.EnterRoleResponse, error) {
	ctx = biz.WithClientIP(ctx, clientIPFromHeader(ctx))
	accountID := middleware.AccountIDFromContext(ctx)
	res, err := s.loginUC.EnterRole(ctx, accountID, req.GetPlayerId(), req.GetDeviceId())
	if err != nil {
		return &loginv1.EnterRoleResponse{Code: toProtoCode(err)}, nil
	}
	return &loginv1.EnterRoleResponse{
		Code:         commonv1.ErrCode_OK,
		PlayerId:     res.PlayerID,
		SessionToken: res.SessionToken,
		HubDsAddr:    res.HubDSAddr,
		HubTicket:    res.HubTicket,
		RegionId:     res.RegionID,
		CellId:       res.CellID,
		// 断线重连:选中的角色正在战斗中时非空,客户端直连原对局而不是回大厅。
		BattleDsAddr:   res.BattleDSAddr,
		BattleTicket:   res.BattleTicket,
		MatchId:        res.MatchID,
		SelectedRoleId: res.SelectedRoleID,
		ResumeContext:  resumeContextToProto(res.Resume),
		PlayerNo:       res.PlayerNo,
	}, nil
}

func (s *LoginService) GetResumeContext(ctx context.Context, req *loginv1.GetResumeContextRequest) (*loginv1.GetResumeContextResponse, error) {
	out, err := s.loginUC.GetResumeContext(ctx, req.GetSessionToken())
	if err != nil {
		return &loginv1.GetResumeContextResponse{Code: toProtoCode(err)}, nil
	}
	return &loginv1.GetResumeContextResponse{Code: commonv1.ErrCode_OK, Context: resumeContextToProto(out)}, nil
}

func resumeContextToProto(in biz.ResumeContextResult) *loginv1.ResumeContext {
	return &loginv1.ResumeContext{Route: in.Route, MatchId: in.MatchID,
		MatchStage: in.MatchStage, GameMode: in.GameMode, MapId: in.MapID,
		// §9.23 query-first owner placement(未启用 owner_query_first 时全为零值,与旧行为一致)。
		PlacementState:  in.PlacementState,
		OperationId:     in.OperationID,
		DsPodName:       in.DSPodName,
		DsInstanceUid:   in.DSInstanceUID,
		DsInstanceEpoch: in.DSInstanceEpoch,
		HubAssignmentId: in.HubAssignmentID,
		AllocationId:    in.AllocationID,
		ReleaseTrack:    in.ReleaseTrack,
		// R11 复审 架构 P0:这三项此前 data 层取回后被 biz 静默丢弃,客户端拿不到
		// §9.23 要求的 (state, exact target, owner_epoch) 三元组,幂等 no-op 无从判定。
		OwnerEpoch:   in.OwnerEpoch,
		RetryAfterMs: in.RetryAfterMs,
		EntryState:   in.EntryState,
		WaitReason:   in.WaitReason,
	}
}

// GetPlayerNo 立即完成型:查本人角色编号(展示专用,player-no-and-login-surge.md §3)。
//
// player_id 从 ctx 读(Envoy jwt_authn 验 session 后注入 x-pandora-player-id,
// 与 SelectRole 同纪律),请求体为空、不信任自报身份——玩家只能查自己的编号。
// ⚠️ 该 path 必须列在 envoy.yaml 的 jwt_authn rules 里:未列到的 path 默认放行不验签,
// 上游拿不到 x-pandora-player-id,这里会一律 ErrUnauthorized。
//
// code=OK 且 player_no=0 表示「仍在补号窗口内」(约 15s),不是错误:客户端继续显示
// 「生成中」并稍后重试;拿到非 0 后编号永不再变,应停止轮询。
func (s *LoginService) GetPlayerNo(ctx context.Context, _ *loginv1.GetPlayerNoRequest) (*loginv1.GetPlayerNoResponse, error) {
	playerID, _ := ctx.Value(plog.CtxKeyPlayerID).(uint64)
	if playerID == 0 {
		plog.With(ctx).Warnw("msg", "get_player_no_no_player_id")
		return &loginv1.GetPlayerNoResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	no, err := s.loginUC.GetPlayerNo(ctx, playerID)
	if err != nil {
		return &loginv1.GetPlayerNoResponse{Code: toProtoCode(err)}, nil
	}
	return &loginv1.GetPlayerNoResponse{Code: commonv1.ErrCode_OK, PlayerNo: no}, nil
}

// GetRegisterNo 是已发布客户端的兼容入口；新调用方必须使用 GetPlayerNo。
// 删除前须先证明旧客户端已排空，不能在滚动升级窗口内原地收缩 RPC。
func (s *LoginService) GetRegisterNo(ctx context.Context, _ *loginv1.GetRegisterNoRequest) (*loginv1.GetRegisterNoResponse, error) {
	res, err := s.GetPlayerNo(ctx, &loginv1.GetPlayerNoRequest{})
	if err != nil {
		return nil, err
	}
	return &loginv1.GetRegisterNoResponse{Code: res.GetCode(), RegisterNo: res.GetPlayerNo()}, nil
}

// SelectRole 立即完成型(选角权威化 2026-07-08,见 login.proto SelectRole 注释)。
//
// player_id 从 ctx 读(Envoy jwt_authn 验 session 后注入 x-pandora-player-id,
// middleware/auth 提进 ctx,与 IssueDSTicket 同纪律),请求体不信任自报 player_id。
func (s *LoginService) SelectRole(ctx context.Context, req *loginv1.SelectRoleRequest) (*loginv1.SelectRoleResponse, error) {
	playerID, _ := ctx.Value(plog.CtxKeyPlayerID).(uint64)
	if playerID == 0 {
		plog.With(ctx).Warnw("msg", "select_role_no_player_id")
		return &loginv1.SelectRoleResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	// 会话现行性门(2026-07-18,封"顶号后旧设备仍可 SelectRole 拿 hub 票"缺口):
	// jti 取自 Envoy 验签后重写的 x-pandora-jwt-payload(入站剥离,客户端无法伪造),
	// 与 IssueDSTicket 的请求体 token 走同一 Redis session 判定。
	callerJTI := middleware.SessionJTIFromContext(ctx)
	if err := s.loginUC.RequireCurrentSessionJTI(ctx, playerID, callerJTI); err != nil {
		// 权威缺失、过期或读取失败都在 SelectRole 业务写前返回，保证 fail-closed 且零签票副作用。
		return &loginv1.SelectRoleResponse{Code: toProtoCode(err)}, nil
	}
	// addr 与 ticket 只会在会话现行性已确认后生成，旧设备不能越过 fencing 取得 Hub 入口。
	// callerJTI 下传(R6 复审 P0-3):角色落库 precommit fencing + hub 票 sjti 绑定。
	addr, ticket, _, err := s.loginUC.SelectRole(ctx, playerID, req.GetRoleId(), callerJTI)
	if err != nil {
		return &loginv1.SelectRoleResponse{Code: toProtoCode(err)}, nil
	}
	// R5 复审 P0-5 终检:预检通过后、角色落库+签票期间会话可能已被新登录轮换
	// (检查与副作用之间的 TOCTOU)。交付前复核,失败则扣留票据(票据从未离开服务端
	// = 未取得)。诚实边界:角色行已落库(跨 Redis/MySQL 无法原子回卷),新设备下次
	// 读角色即以库内权威为准,残余仅为一次可被覆盖的选角写,不构成进场能力。
	if err := s.loginUC.RequireCurrentSessionJTI(ctx, playerID, middleware.SessionJTIFromContext(ctx)); err != nil {
		plog.With(ctx).Warnw("msg", "select_role_delivery_fenced", "player_id", playerID)
		return &loginv1.SelectRoleResponse{Code: toProtoCode(err)}, nil
	}
	return &loginv1.SelectRoleResponse{
		Code:      commonv1.ErrCode_OK,
		HubDsAddr: addr,
		HubTicket: ticket,
	}, nil
}

// Logout 立即完成型。
func (s *LoginService) Logout(ctx context.Context, req *loginv1.LogoutRequest) (*loginv1.LogoutResponse, error) {
	if err := s.loginUC.Logout(ctx, req.GetSessionToken()); err != nil {
		return &loginv1.LogoutResponse{Code: toProtoCode(err)}, nil
	}
	return &loginv1.LogoutResponse{Code: commonv1.ErrCode_OK}, nil
}

// IssueDSTicket 立即完成型,W3 ① 真实化:
//   - 校验 req.SessionToken(委托给 TicketUsecase 内部走 verifier;此处直接信任 Envoy 已校验)
//   - 用 Signer 签 ds 票据,exp 默认 5min
//
// W2 阶段调用方传 session_token,W3 ① 暂不二次解 session(Envoy jwt_authn 已校验过),
// player_id 直接从 ctx 的 player_id(由 middleware/auth 从 x-pandora-player-id 头注入)读。
//
// W3 ②:加 jti SETNX EX 5min 防重放,加 session 在线检查。
func (s *LoginService) IssueDSTicket(ctx context.Context, req *loginv1.IssueDSTicketRequest) (*loginv1.IssueDSTicketResponse, error) {
	playerID, _ := ctx.Value(plog.CtxKeyPlayerID).(uint64)
	if playerID == 0 {
		plog.With(ctx).Warnw("msg", "ds_ticket_issue_no_player_id")
		return &loginv1.IssueDSTicketResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	// P0 修复(2026-07-15,codex P0-10):session 现行性门。JWT 验签只证明"曾登录过",
	// 顶号后旧 token 在 exp 前仍验得过;这里用 Redis session jti 确认 token 是当前一代,
	// 防止旧设备继续给自己签 hub/battle 票造成双在场。
	if err := s.loginUC.RequireCurrentSessionToken(ctx, playerID, req.GetSessionToken()); err != nil {
		return &loginv1.IssueDSTicketResponse{Code: toProtoCode(err)}, nil
	}

	// ds_type=hub:复用登录的 hub 分配链路(hub_allocator.AssignHub),返回"当前有效"的大厅地址
	// + 全新一次性票据。结算返回大厅必须走这条路,以应对 Hub DS 被 Agones 重建/换端口/换分片
	// (客户端登录时缓存的旧地址会失效)。battle 票据仍由 ticketUC 仅签发(地址来自 matchmaker)。
	// R5 复审 P0-5 终检(闭包供三条分支复用):预检通过后、分配/签票期间会话可能已被
	// 新登录轮换(检查与副作用之间的 TOCTOU)。交付前复核当前 token 仍是当前一代,
	// 失败则扣留票据——票据已签但从未离开服务端 = 旧在途请求未取得可用票据。
	// R6 复审 P0-3 兜底:即便终检与响应写出之间被轮换,票内 sjti(callerJTI)绑定使
	// 已交付旧票在 VerifyDSTicket 兑换点被拒。
	fenceTicketDelivery := func() error {
		return s.loginUC.RequireCurrentSessionToken(ctx, playerID, req.GetSessionToken())
	}
	callerJTI := middleware.SessionJTIFromContext(ctx)

	if req.GetDsType() == "hub" {
		// target_id 历史上携带来源 match;现在仅作日志参考,路由权威是
		// locator 租约 + match 三态门(biz.ResolveHubEndpointFromMatch)。
		addr, ticket, _, err := s.loginUC.ResolveHubEndpointFromMatch(ctx, playerID, req.GetTargetId(), callerJTI)
		if err != nil {
			return &loginv1.IssueDSTicketResponse{Code: toProtoCode(err)}, nil
		}
		if err := fenceTicketDelivery(); err != nil {
			plog.With(ctx).Warnw("msg", "ds_ticket_delivery_fenced", "player_id", playerID, "ds_type", "hub")
			return &loginv1.IssueDSTicketResponse{Code: toProtoCode(err)}, nil
		}
		return &loginv1.IssueDSTicketResponse{
			Code:      commonv1.ErrCode_OK,
			Ticket:    ticket,
			HubDsAddr: addr,
		}, nil
	}

	if req.GetDsType() == "battle" {
		_, ticket, _, err := s.loginUC.ResolveBattleEndpoint(ctx, playerID, req.GetTargetId(), callerJTI)
		if err != nil {
			return &loginv1.IssueDSTicketResponse{Code: toProtoCode(err)}, nil
		}
		if err := fenceTicketDelivery(); err != nil {
			plog.With(ctx).Warnw("msg", "ds_ticket_delivery_fenced", "player_id", playerID, "ds_type", "battle")
			return &loginv1.IssueDSTicketResponse{Code: toProtoCode(err)}, nil
		}
		return &loginv1.IssueDSTicketResponse{Code: commonv1.ErrCode_OK, Ticket: ticket}, nil
	}

	res, err := s.ticketUC.IssueDSTicket(ctx, playerID, req.GetDsType(), req.GetTargetId(), callerJTI)
	if err != nil {
		return &loginv1.IssueDSTicketResponse{Code: toProtoCode(err)}, nil
	}
	if err := fenceTicketDelivery(); err != nil {
		plog.With(ctx).Warnw("msg", "ds_ticket_delivery_fenced", "player_id", playerID, "ds_type", req.GetDsType())
		return &loginv1.IssueDSTicketResponse{Code: toProtoCode(err)}, nil
	}
	return &loginv1.IssueDSTicketResponse{
		Code:   commonv1.ErrCode_OK,
		Ticket: res.Ticket,
	}, nil
}

// VerifyDSTicket 立即完成型,W3 ① 真实化(验签 + exp + iss + aud)。
//
// Envoy 客户端面 :8443 对本 path 精确 403；唯一网关入口是 :8444 exact route。
// Redis authority 下还必须通过 DS Bearer + active/projection，网络位置本身不构成身份。
// 不变量 §3:本方法返回的 claims.exp 必须严格短时效。
func (s *LoginService) VerifyDSTicket(ctx context.Context, req *loginv1.VerifyDSTicketRequest) (*loginv1.VerifyDSTicketResponse, error) {
	var (
		claims *biz.DSTicketClaims
		err    error
	)
	if s.redisDSAdmission {
		// 前置门拒绝必须显式落盘(§11.3 R2):这些是 DS 调用方级拒绝,一台 DS 的 callback
		// credential 与 active 权威漂移会让它上面**所有玩家**的重连/进场核销整体失败,
		// 而 ErrUnauthorized/ErrInvalidArg 非 IsServerFault → access log 只记 rpc_ok(DEBUG),
		// toProtoCode 又只回 code 不回 message——不在这里打,拒绝原因在任何级别都不存在。
		// 此时票还没解析,拿不到 player_id;join key 是 ds_pod + trace_id。
		// ds_pod_name 是 Guard 的范围输入；空值不能退化成“不校验 pod”。
		if req.GetDsPodName() == "" {
			plog.With(ctx).Warnw("msg", "verify_ds_ticket_rejected",
				"reason", "ds_pod_name_empty")
			return &loginv1.VerifyDSTicketResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
		}
		if s.dsGuard == nil || s.admissionChecker == nil {
			plog.With(ctx).Errorw("msg", "verify_ds_ticket_rejected",
				"reason", "ds_admission_guard_not_wired", "ds_pod", req.GetDsPodName())
			return &loginv1.VerifyDSTicketResponse{Code: commonv1.ErrCode_ERR_UNAVAILABLE}, nil
		}
		// 固定线性顺序：① Bearer 验签+请求 pod scope；② Redis active；
		// ③ TicketUsecase 比对玩家票 binding/assignment；④ 原子 MarkUsedByAdmission。
		_, credential, guardErr := s.dsGuard.CheckCredential(ctx, middleware.DSScope{
			Pod: req.GetDsPodName(), RequireToken: true,
		})
		if guardErr != nil {
			plog.With(ctx).Warnw("msg", "verify_ds_ticket_rejected",
				"reason", "ds_credential_rejected", "ds_pod", req.GetDsPodName(),
				"code", int32(errcode.As(guardErr)), "err", guardErr,
				"hint", "DS callback credential 验签/scope 失败:该 DS 上所有玩家的核销都会同型失败,查凭据轮换")
			return &loginv1.VerifyDSTicketResponse{Code: toProtoCode(guardErr)}, nil
		}
		if credential == nil {
			plog.With(ctx).Warnw("msg", "verify_ds_ticket_rejected",
				"reason", "ds_credential_missing", "ds_pod", req.GetDsPodName())
			return &loginv1.VerifyDSTicketResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
		}
		admission, activeErr := s.admissionChecker.CheckActive(ctx, req.GetDsPodName(), credential)
		if activeErr != nil {
			plog.With(ctx).Warnw("msg", "verify_ds_ticket_rejected",
				"reason", "ds_admission_not_active", "ds_pod", req.GetDsPodName(),
				"admission_id", req.GetAdmissionId(),
				"code", int32(errcode.As(activeErr)), "err", activeErr,
				"hint", "DS credential 与 Redis active 权威漂移(轮换半途/心跳超时/投影翻转):该 DS 上所有玩家 travel 会被拒")
			return &loginv1.VerifyDSTicketResponse{Code: toProtoCode(activeErr)}, nil
		}
		claims, err = s.ticketUC.VerifyDSTicketForAdmission(
			ctx, req.GetTicket(), req.GetDsPodName(), req.GetAdmissionId(), admission)
	} else {
		// off/legacy 完整保留既有内部 Verify 语义与单次 JTI SETNX。
		claims, err = s.ticketUC.VerifyDSTicket(ctx, req.GetTicket(), req.GetDsPodName())
	}
	if err != nil {
		return &loginv1.VerifyDSTicketResponse{Code: toProtoCode(err)}, nil
	}
	// 兑换点会话复核(R6 复审 P0-3,§9.23):票内 sjti 非空时必须仍是该玩家会话权威的
	// 当前一代——签发与响应写出之间被新登录轮换的旧票,即使已交付到旧设备,在此作废。
	// sjti 空 = 兼容窗(matchmaker 批签/Transfer 重签/滚动升级旧票),不做判定。
	if serr := s.loginUC.RequireTicketSessionCurrent(ctx, claims.PlayerID, claims.SessJTI); serr != nil {
		plog.With(ctx).Warnw("msg", "ds_ticket_session_stale_rejected",
			"player_id", claims.PlayerID, "ds_type", claims.DSType)
		return &loginv1.VerifyDSTicketResponse{Code: toProtoCode(serr)}, nil
	}
	return &loginv1.VerifyDSTicketResponse{
		Code: commonv1.ErrCode_OK,
		Claims: &loginv1.DSTicket{
			PlayerId:        claims.PlayerID,
			MatchId:         claims.MatchID,
			IssuedAtMs:      claims.IssuedAtMs,
			ExpiresAtMs:     claims.ExpiresAtMs,
			DsType:          claims.DSType,
			Jti:             claims.JTI,
			RegionId:        claims.RegionID,
			CellId:          claims.CellID,
			RoleId:          claims.RoleID,
			DsPodName:       claims.DSPodName,
			DsInstanceUid:   claims.DSInstanceUID,
			DsProtocolEpoch: claims.DSProtocolEpoch,
			DsCredentialGen: claims.DSCredentialGen,
			DsCredentialJti: claims.DSCredentialJTI,
			HubAssignmentId: claims.HubAssignmentID,
			DsWriterEpoch:   claims.DSWriterEpoch,
			DstVer:          uint32(claims.Version),
			DsInstanceEpoch: claims.DSInstanceEpoch,
			AllocationId:    claims.AllocationID,
			ReleaseTrack:    claims.ReleaseTrack,
		},
	}, nil
}

// ResolvePlayerNosForDS 是 DS listener 独占的展示读取入口。
// legacy/off 保留本地开发直连语义；redis admission 才强制 Bearer DS credential + active 权威。
func (s *LoginService) ResolvePlayerNosForDS(
	ctx context.Context,
	req *loginv1.ResolvePlayerNosRequest,
) (*loginv1.ResolvePlayerNosResponse, error) {
	playerIDs, ok := normalizePlayerNoIDs(req.GetPlayerIds())
	if !ok {
		return &loginv1.ResolvePlayerNosResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if s == nil {
		return &loginv1.ResolvePlayerNosResponse{Code: commonv1.ErrCode_ERR_UNAVAILABLE}, nil
	}
	if s.redisDSAdmission {
		if s.dsGuard == nil || s.admissionChecker == nil || s.dsGuard.Mode() != middleware.DSAuthEnforce {
			plog.With(ctx).Errorw("msg", "resolve_player_nos_for_ds_rejected", "reason", "ds_admission_guard_not_enforced")
			return &loginv1.ResolvePlayerNosResponse{Code: commonv1.ErrCode_ERR_UNAVAILABLE}, nil
		}
		_, credential, guardErr := s.dsGuard.CheckCredential(ctx, middleware.DSScope{RequireToken: true})
		if guardErr != nil {
			plog.With(ctx).Warnw("msg", "resolve_player_nos_for_ds_rejected", "reason", "ds_credential_rejected",
				"code", int32(errcode.As(guardErr)), "err", guardErr)
			return &loginv1.ResolvePlayerNosResponse{Code: toProtoCode(guardErr)}, nil
		}
		if credential == nil {
			plog.With(ctx).Warnw("msg", "resolve_player_nos_for_ds_rejected", "reason", "ds_credential_missing")
			return &loginv1.ResolvePlayerNosResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
		}
		admission, activeErr := s.admissionChecker.CheckActive(ctx, credential.Pod, credential)
		if activeErr != nil {
			plog.With(ctx).Warnw("msg", "resolve_player_nos_for_ds_rejected", "reason", "ds_admission_not_active",
				"ds_pod", credential.Pod, "code", int32(errcode.As(activeErr)), "err", activeErr)
			return &loginv1.ResolvePlayerNosResponse{Code: toProtoCode(activeErr)}, nil
		}
		if credential.DSType == auth.DSTypeBattle && !playerIDsBelongToAdmission(playerIDs, admission.PlayerIDs) {
			plog.With(ctx).Warnw("msg", "resolve_player_nos_for_ds_rejected", "reason", "player_outside_battle_admission",
				"ds_pod", credential.Pod, "count", len(playerIDs))
			return &loginv1.ResolvePlayerNosResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
		}
	}
	return resolvePlayerNosFromAuthority(ctx, s.playerNoUC, playerIDs), nil
}

func playerIDsBelongToAdmission(requested, admitted []uint64) bool {
	allowed := make(map[uint64]struct{}, len(admitted))
	for _, playerID := range admitted {
		allowed[playerID] = struct{}{}
	}
	for _, playerID := range requested {
		if _, ok := allowed[playerID]; !ok {
			return false
		}
	}
	return true
}

// toProtoCode 把 pkg/errcode 转成 proto enum。
//
// pkg/errcode.Code 是 int32,proto enum 数值跟它 1:1 对齐
// (见 proto/pandora/common/v1/errcode.proto 上的"errcode 双向同步纪律"注释)。
func toProtoCode(err error) commonv1.ErrCode {
	c := errcode.As(err)
	return commonv1.ErrCode(c)
}
