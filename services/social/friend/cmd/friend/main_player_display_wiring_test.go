package main

import (
	"os"
	"strings"
	"testing"
)

func TestMainWiresIndependentFriendDisplayResolvers(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, want := range []string{
		"cfg.ValidatePlayerDisplayResolvers()",
		"internalrpcauth.NewSigner(cfg.Friend.PlayerNameResolverAuthSecret,",
		`"friend", cfg.Friend.PlayerNameResolverAuthAudience`,
		"playerdisplay.NewGrpcPlayerNameResolver(cfg.Friend.PlayerNameResolverAddr, playerNameSigner)",
		"uc.SetPlayerNameResolver(playerNameResolver)",
		"internalrpcauth.NewSigner(cfg.Friend.PlayerNoResolverAuthSecret,",
		`"friend", cfg.Friend.PlayerNoResolverAuthAudience`,
		"playerdisplay.NewGrpcPlayerNoResolver(cfg.Friend.PlayerNoResolverAddr, playerNoSigner)",
		"uc.SetPlayerNoResolver(playerNoResolver)",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("main.go missing friend display wiring %q", want)
		}
	}
}
