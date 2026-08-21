package conf

import "testing"

func TestValidatePlayerNoResolverSupportsIndependentCallers(t *testing.T) {
	cfg := Config{}
	cfg.Node.RedisClient.Host = "127.0.0.1:6379"
	cfg.Login.PlayerNoResolveAuthSecret = "team-player-no-key-0123456789abcdef00"
	cfg.Login.FriendPlayerNoResolveAuthSecret = "friend-player-no-key-0123456789abcdef0"
	cfg.Login.GuildPlayerNoResolveAuthSecret = "guild-player-no-key-0123456789abcdef01"
	cfg.Defaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid multi-caller config: %v", err)
	}
	if cfg.Login.FriendPlayerNoResolveAuthAudience != "login:player-no" ||
		cfg.Login.GuildPlayerNoResolveAuthAudience != "login:player-no" {
		t.Fatalf("default audiences = (%q,%q)", cfg.Login.FriendPlayerNoResolveAuthAudience,
			cfg.Login.GuildPlayerNoResolveAuthAudience)
	}

	cfg.Login.GuildPlayerNoResolveAuthSecret = cfg.Login.PlayerNoResolveAuthSecret
	if err := cfg.Validate(); err == nil {
		t.Fatal("guild must not reuse team player-no HMAC key")
	}
}
