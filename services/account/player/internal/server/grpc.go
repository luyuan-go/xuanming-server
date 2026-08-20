// Package server — gRPC server 注册。
package server

import (
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"

	"github.com/luyuancpp/pandora/pkg/grpcserver"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
	"github.com/luyuancpp/pandora/pkg/sessiongate"
	configv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"

	"github.com/luyuancpp/pandora/services/account/player/internal/conf"
	"github.com/luyuancpp/pandora/services/account/player/internal/service"
)

// NewGRPCServer 构造 gRPC server，并注册公开 PlayerService 与可选的 PlayerInternalService。
//
// 端口由 cfg.Server.Grpc.Addr 决定(默认 :20002)。
// 调用方为后端内部(battle_result)/ 经 Envoy 的客户端,pmw.AuthOptional() 与其它服一致。
// pmw.SessionCurrent 校验客户端面请求 jti == login 会话权威当前一代(R5 复审 P0-1:
// 顶号后旧 JWT 在 exp 前不得继续按 player_id 定向操作,INC-20260722-004;
// 内部直连不带 x-pandora-jwt-payload,天然放行)。
func NewGRPCServer(cfg *conf.Config, svc *service.PlayerService, internalSvc playerv1.PlayerInternalServiceServer,
	ctAdmin configv1.ConfigTableAdminServiceServer, sessGate sessiongate.Gate,
) *kgrpc.Server {
	srv := grpcserver.MustNewServer(cfg.Server, pmw.AuthOptional(),
		pmw.SessionCurrent(sessGate, cfg.SessionGate.Require))
	playerv1.RegisterPlayerServiceServer(srv, svc)
	if internalSvc != nil {
		playerv1.RegisterPlayerInternalServiceServer(srv, internalSvc)
	}
	if ctAdmin != nil {
		configv1.RegisterConfigTableAdminServiceServer(srv, ctAdmin)
	}
	return srv
}
