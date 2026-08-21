package conf

import "testing"

func TestValidatePlayerDisplayResolvers(t *testing.T) {
	cfg := Config{Guild: GuildConf{
		PlayerNameResolverAddr:         "player:20002",
		PlayerNameResolverAuthSecret:   "guild-player-name-key-0123456789abcdef0",
		PlayerNameResolverAuthAudience: "player:name",
		PlayerNoResolverAddr:           "login:20001",
		PlayerNoResolverAuthSecret:     "guild-player-no-key-0123456789abcdef012",
		PlayerNoResolverAuthAudience:   "login:player-no",
	}}
	if err := cfg.ValidatePlayerDisplayResolvers(); err != nil {
		t.Fatalf("valid display resolver config: %v", err)
	}

	cfg.Guild.PlayerNoResolverAuthSecret = cfg.Guild.PlayerNameResolverAuthSecret
	if err := cfg.ValidatePlayerDisplayResolvers(); err == nil {
		t.Fatal("reusing one HMAC key across name and player_no authorities must fail")
	}

	cfg.Guild.PlayerNoResolverAuthSecret = ""
	if err := cfg.ValidatePlayerDisplayResolvers(); err == nil {
		t.Fatal("configured resolver address without secret must fail")
	}
}
