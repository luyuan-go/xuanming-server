package main

import "testing"

const testWorkspaceID = "01arz3ndektsv4rrffq69g5fav"

func TestValidMigrationDatabaseMappingAcceptsExactWorkspaceDatabase(t *testing.T) {
	if !validMigrationDatabaseMapping(
		"pandora_account",
		"pandora_account_w_"+testWorkspaceID,
	) {
		t.Fatal("strict workspace database mapping should be accepted")
	}
}

func TestValidMigrationDatabaseMappingReservesWorkspaceNamespace(t *testing.T) {
	invalid := []string{
		"pandora_account_w_short",
		"pandora_account_w_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"pandora_account_w_01arz3ndektsv4rrffq69g5fai",
		"pandora_account_w_" + testWorkspaceID + "_extra",
	}
	for _, database := range invalid {
		if validMigrationDatabaseMapping("pandora_account", database) {
			t.Errorf("reserved workspace database %q should be rejected", database)
		}
	}
}

func TestValidMigrationDatabaseMappingKeepsCanonicalAndLegacyShardCompatibility(t *testing.T) {
	for _, database := range []string{"pandora_auction", "pandora_auction_00"} {
		if !validMigrationDatabaseMapping("pandora_auction", database) {
			t.Errorf("existing database mapping %q should remain valid", database)
		}
	}
}

func TestPandoraPlayerV7QuarantineTargetUsesExactDatabaseAllowlist(t *testing.T) {
	accepted := []string{
		"pandora_player",
		"pandora_player_w_" + testWorkspaceID,
		"pandora_player_mig_it_1755687600123456789",
	}
	for _, database := range accepted {
		target := migrationTarget{MigrationSet: "pandora_player", Database: database}
		if !validPandoraPlayerV7QuarantineTarget(target) {
			t.Errorf("quarantine target %q should be accepted", database)
		}
	}

	rejected := []migrationTarget{
		{MigrationSet: "pandora_account", Database: "pandora_player"},
		{MigrationSet: "pandora_player", Database: "pandora_player_00"},
		{MigrationSet: "pandora_player", Database: "pandora_player_w_short"},
		{MigrationSet: "pandora_player", Database: "pandora_player_w_" + testWorkspaceID + "_extra"},
		{MigrationSet: "pandora_player", Database: "pandora_player_mig_it_"},
		{MigrationSet: "pandora_player", Database: "pandora_player_mig_it_notdigits"},
		{MigrationSet: "pandora_player", Database: "pandora_player_mig_it_17556876001234567890"},
	}
	for _, target := range rejected {
		if validPandoraPlayerV7QuarantineTarget(target) {
			t.Errorf("quarantine target %+v should be rejected", target)
		}
	}
}

func TestValidateWorkspaceTargetsPreventsCrossWorkspaceAndMixedTargets(t *testing.T) {
	matching := []migrationTarget{
		{MigrationSet: "pandora_account", Database: "pandora_account_w_" + testWorkspaceID},
		{MigrationSet: "pandora_player", Database: "pandora_player_w_" + testWorkspaceID},
	}
	if err := validateWorkspaceTargets(matching, testWorkspaceID); err != nil {
		t.Fatalf("matching workspace targets error = %v", err)
	}
	if err := validateWorkspaceTargets(
		[]migrationTarget{{MigrationSet: "pandora_account", Database: "pandora_account"}},
		"",
	); err != nil {
		t.Fatalf("canonical target without workspace guard error = %v", err)
	}
	if err := validateWorkspaceTargets(
		[]migrationTarget{{MigrationSet: "pandora_auction", Database: "pandora_auction_00"}},
		"",
	); err != nil {
		t.Fatalf("legacy shard without workspace guard error = %v", err)
	}

	tests := []struct {
		name        string
		targets     []migrationTarget
		workspaceID string
	}{
		{name: "missing_guard", targets: matching},
		{
			name: "malformed_reserved_workspace_without_guard",
			targets: []migrationTarget{{
				MigrationSet: "pandora_account",
				Database:     "pandora_account_w_short",
			}},
		},
		{
			name: "cross_workspace",
			targets: []migrationTarget{{
				MigrationSet: "pandora_account",
				Database:     "pandora_account_w_01arz3ndektsv4rrffq69g5fb0",
			}},
			workspaceID: testWorkspaceID,
		},
		{
			name: "mixed_canonical",
			targets: []migrationTarget{
				{MigrationSet: "pandora_account", Database: "pandora_account_w_" + testWorkspaceID},
				{MigrationSet: "pandora_player", Database: "pandora_player"},
			},
			workspaceID: testWorkspaceID,
		},
		{
			name:        "legacy_shard_under_workspace_guard",
			targets:     []migrationTarget{{MigrationSet: "pandora_auction", Database: "pandora_auction_00"}},
			workspaceID: testWorkspaceID,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateWorkspaceTargets(tt.targets, tt.workspaceID); err == nil {
				t.Fatal("validateWorkspaceTargets() should fail")
			}
		})
	}
}
