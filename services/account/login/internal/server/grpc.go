// Package server 负责把 LoginService 实现挂到 gRPC server 上。
package server

import (
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"

	"github.com/luyuancpp/pandora/pkg/grpcserver"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
	loginv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/login/v1"

	"github.com/luyuancpp/pandora/services/account/login/internal/conf"
	"github.com/luyuancpp/pandora/services/account/login/internal/service"
)

// NewGRPCServer 构造 gRPC server 并把 LoginService 注册上去。
//
// 端口由 cfg.Server.Grpc.Addr 决定(默认 :20001,见 conf.Defaults)。
//
// W3 ①(2026-06-05):加 pmw.AuthOptional(),把 Envoy jwt_authn 注入的
// x-pandora-player-id 头解析进 ctx,供 login.IssueDSTicket / login.Logout 使用。
// Optional 而非 Required 原因:
//   - login.Login 本身就没有 token(还没签出来),Required 会让 Login 全部 401
//   - Envoy 已经按 path 强制 IssueDSTicket / Logout 必须带合法 JWT,业务层只需读 player_id
//   - 直连 :20001 调试(绕过 Envoy)时,Login 仍可联调通过
func NewGRPCServer(cfg *conf.Config, svc *service.LoginService, internal ...*service.LoginInternalService) *kgrpc.Server {
	srv := grpcserver.MustNewServer(cfg.Server, pmw.AuthOptional())
	loginv1.RegisterLoginServiceServer(srv, svc)
	if len(internal) != 0 && internal[0] != nil {
		loginv1.RegisterLoginInternalServiceServer(srv, internal[0])
	}
	return srv
}
