package conf_test

import (
	"strings"
	"testing"
)

func TestPlayerDeploymentConfigsCarryPlayerNameResolverVerifier(t *testing.T) {
	cfg := loadConfig(t, devTemplate)
	if cfg.Player.PlayerNameResolveAuthSecret == "" {
		t.Fatalf("%s missing player_name_resolve_auth_secret", devTemplate)
	}
	if cfg.Player.FriendPlayerNameResolveAuthSecret == "" || cfg.Player.GuildPlayerNameResolveAuthSecret == "" {
		t.Fatalf("%s missing friend/guild player-name verifier keys", devTemplate)
	}
	if err := cfg.ValidatePlayerNameResolver(); err != nil {
		t.Fatalf("%s player_name verifier config: %v", devTemplate, err)
	}
	prod := readRepoFile(t, prodExample)
	for _, key := range []string{
		"player_name_resolve_auth_secret:", "player_name_resolve_auth_audience:",
		"friend_player_name_resolve_auth_secret:", "friend_player_name_resolve_auth_audience:",
		"guild_player_name_resolve_auth_secret:", "guild_player_name_resolve_auth_audience:",
	} {
		if !strings.Contains(prod, key) {
			t.Fatalf("%s missing %s", prodExample, key)
		}
	}
}
