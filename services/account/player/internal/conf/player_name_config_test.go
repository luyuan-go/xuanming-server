package conf

import "testing"

func TestPlayerNameResolveAuthConfigIsComplete(t *testing.T) {
	var cfg Config
	cfg.Defaults()
	if err := cfg.ValidatePlayerNameResolver(); err != nil {
		t.Fatalf("disabled resolver should remain valid: %v", err)
	}

	cfg.Player.PlayerNameResolveAuthAudience = "player:name"
	if err := cfg.ValidatePlayerNameResolver(); err == nil {
		t.Fatal("audience without secret must fail fast")
	}

	cfg.Player.PlayerNameResolveAuthSecret = "player-name-resolve-test-secret-0123456789"
	if err := cfg.ValidatePlayerNameResolver(); err == nil {
		t.Fatal("enabled resolver without Redis replay authority must fail fast")
	}
	cfg.Node.RedisClient.Host = "redis:6379"
	if err := cfg.ValidatePlayerNameResolver(); err != nil {
		t.Fatalf("complete resolver config: %v", err)
	}
}

func TestPlayerNameResolveAuthDefaultsAudienceWhenEnabled(t *testing.T) {
	var cfg Config
	cfg.Player.PlayerNameResolveAuthSecret = "player-name-resolve-test-secret-0123456789"
	cfg.Node.RedisClient.Host = "redis:6379"
	cfg.Defaults()
	if cfg.Player.PlayerNameResolveAuthAudience != "player:name" {
		t.Fatalf("audience=%q", cfg.Player.PlayerNameResolveAuthAudience)
	}
	if err := cfg.ValidatePlayerNameResolver(); err != nil {
		t.Fatal(err)
	}
}
