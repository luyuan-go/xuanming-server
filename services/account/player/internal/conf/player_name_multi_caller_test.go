package conf

import "testing"

func TestValidatePlayerNameResolverSupportsIndependentCallers(t *testing.T) {
	cfg := Config{}
	cfg.Node.RedisClient.Host = "127.0.0.1:6379"
	cfg.Player.PlayerNameResolveAuthSecret = "team-player-name-key-0123456789abcdef"
	cfg.Player.FriendPlayerNameResolveAuthSecret = "friend-player-name-key-0123456789abcdef"
	cfg.Player.GuildPlayerNameResolveAuthSecret = "guild-player-name-key-0123456789abcdef0"
	cfg.Defaults()
	if err := cfg.ValidatePlayerNameResolver(); err != nil {
		t.Fatalf("valid multi-caller config: %v", err)
	}
	if cfg.Player.FriendPlayerNameResolveAuthAudience != "player:name" ||
		cfg.Player.GuildPlayerNameResolveAuthAudience != "player:name" {
		t.Fatalf("default audiences = (%q,%q)", cfg.Player.FriendPlayerNameResolveAuthAudience,
			cfg.Player.GuildPlayerNameResolveAuthAudience)
	}

	cfg.Player.FriendPlayerNameResolveAuthSecret = cfg.Player.PlayerNameResolveAuthSecret
	if err := cfg.ValidatePlayerNameResolver(); err == nil {
		t.Fatal("friend must not reuse team player-name HMAC key")
	}
}
