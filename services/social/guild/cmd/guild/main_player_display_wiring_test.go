package main

import (
	"os"
	"strings"
	"testing"
)

func TestMainWiresIndependentGuildDisplayResolvers(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, want := range []string{
		"cfg.ValidatePlayerDisplayResolvers()",
		"internalrpcauth.NewSigner(cfg.Guild.PlayerNameResolverAuthSecret,",
		`"guild", cfg.Guild.PlayerNameResolverAuthAudience`,
		"playerdisplay.NewGrpcPlayerNameResolver(cfg.Guild.PlayerNameResolverAddr, playerNameSigner)",
		"guildUC.SetPlayerNameResolver(playerNameResolver)",
		"internalrpcauth.NewSigner(cfg.Guild.PlayerNoResolverAuthSecret,",
		`"guild", cfg.Guild.PlayerNoResolverAuthAudience`,
		"playerdisplay.NewGrpcPlayerNoResolver(cfg.Guild.PlayerNoResolverAddr, playerNoSigner)",
		"guildUC.SetPlayerNoResolver(playerNoResolver)",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("main.go missing guild display wiring %q", want)
		}
	}
}
