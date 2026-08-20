package conf_test

import (
	"testing"

	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/conf"
)

func TestPlayerNameResolverConfigIsComplete(t *testing.T) {
	var cfg conf.Config
	cfg.Defaults()
	if err := cfg.ValidatePlayerNameResolver(); err != nil {
		t.Fatalf("disabled resolver should remain valid: %v", err)
	}
	cfg.Team.PlayerNameResolverAuthAudience = "player:name"
	if err := cfg.ValidatePlayerNameResolver(); err == nil {
		t.Fatal("audience without resolver address must fail fast")
	}
	cfg.Team.PlayerNameResolverAuthAudience = ""

	cfg.Team.PlayerNameResolverAddr = "player:20002"
	cfg.Defaults()
	if err := cfg.ValidatePlayerNameResolver(); err == nil {
		t.Fatal("resolver address without secret must fail fast")
	}
	cfg.Team.PlayerNameResolverAuthSecret = "team-player-name-resolver-test-key-0123456789"
	if err := cfg.ValidatePlayerNameResolver(); err != nil {
		t.Fatalf("complete resolver config: %v", err)
	}
	cfg.Team.MaxMembers = 33
	if err := cfg.ValidatePlayerNameResolver(); err == nil {
		t.Fatal("max_members above batch contract must fail fast")
	}
	cfg.Team.MaxMembers = 5
	cfg.Team.MaxApplicationsPerTeam = 33
	if err := cfg.ValidatePlayerNameResolver(); err == nil {
		t.Fatal("max_applications_per_team above batch contract must fail fast")
	}
	cfg.Team.MaxApplicationsPerTeam = 10
	cfg.Team.MaxOpenTeamsPerQuery = 33
	if err := cfg.ValidatePlayerNameResolver(); err == nil {
		t.Fatal("max_open_teams_per_query above batch contract must fail fast")
	}
}

func TestTeamDeploymentConfigsCarryPlayerNameResolver(t *testing.T) {
	for _, rel := range []string{"etc/team-dev.yaml", "etc/team-prod.yaml.example"} {
		t.Run(rel, func(t *testing.T) {
			cfg := loadConfig(t, rel)
			if err := cfg.ValidatePlayerNameResolver(); err != nil {
				t.Fatalf("%s player_name resolver config: %v", rel, err)
			}
			if cfg.Team.PlayerNameResolverAddr == "" {
				t.Fatalf("%s must enable player_name resolver", rel)
			}
		})
	}
}
