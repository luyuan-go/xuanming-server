package main

import (
	"os"
	"strings"
	"testing"
)

func TestMainWiresPlayerInternalNameAuthorityIndependentlyFromDSAuth(t *testing.T) {
	raw, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	for _, required := range []string{
		"cfg.ValidatePlayerNameResolver()",
		"internalrpcauth.NewRedisReplayStore",
		"pandora:player:name-resolve:nonce:",
		"internalrpcauth.NewVerifier(credential.secret, credential.caller",
		"cfg.Player.FriendPlayerNameResolveAuthSecret",
		"cfg.Player.GuildPlayerNameResolveAuthSecret",
		"internalrpcauth.NewMultiCallerVerifier",
		"service.NewPlayerInternalService(uc, playerNameVerifier)",
		"server.NewGRPCServer(&cfg, svc, internalSvc, ctAdmin, sessGate)",
	} {
		if !strings.Contains(src, required) {
			t.Fatalf("main.go missing player-name authority wiring %q", required)
		}
	}
}
