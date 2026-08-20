package main

import (
	"os"
	"strings"
	"testing"
)

func TestMainWiresPlayerNameResolverIndependentlyFromPlayerNo(t *testing.T) {
	raw, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	for _, required := range []string{
		"cfg.ValidatePlayerNameResolver()",
		"internalrpcauth.NewSigner(cfg.Team.PlayerNameResolverAuthSecret",
		"data.NewGrpcPlayerNameResolver(cfg.Team.PlayerNameResolverAddr, playerNameSigner)",
		"uc.SetPlayerNameResolver(playerNameResolver)",
	} {
		if !strings.Contains(src, required) {
			t.Fatalf("main.go missing player-name resolver wiring %q", required)
		}
	}
}
