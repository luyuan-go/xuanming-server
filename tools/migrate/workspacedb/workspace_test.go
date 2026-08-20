package workspacedb

import (
	"strings"
	"testing"
)

const validWorkspaceID = "01arz3ndektsv4rrffq69g5fav"

func TestPhysicalDatabaseNameBuildsCanonicalWorkspaceName(t *testing.T) {
	got, err := PhysicalDatabaseName("pandora_account", validWorkspaceID)
	if err != nil {
		t.Fatalf("PhysicalDatabaseName() error = %v", err)
	}
	if want := "pandora_account_w_" + validWorkspaceID; got != want {
		t.Fatalf("PhysicalDatabaseName() = %q, want %q", got, want)
	}
}

func TestValidateWorkspaceIDRejectsNonCanonicalValues(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "empty"},
		{name: "short", value: validWorkspaceID[:25]},
		{name: "long", value: validWorkspaceID + "0"},
		{name: "uppercase", value: strings.ToUpper(validWorkspaceID)},
		{name: "first_character_8", value: "81arz3ndektsv4rrffq69g5fav"},
		{name: "first_character_z", value: "z1arz3ndektsv4rrffq69g5fav"},
		{name: "forbidden_i", value: "01arz3ndektsv4rrffq69g5fai"},
		{name: "forbidden_l", value: "01arz3ndektsv4rrffq69g5fal"},
		{name: "forbidden_o", value: "01arz3ndektsv4rrffq69g5fao"},
		{name: "forbidden_u", value: "01arz3ndektsv4rrffq69g5fau"},
		{name: "hyphen", value: "01arz3ndektsv4rrffq69g5fa-"},
		{name: "leading_space", value: " " + validWorkspaceID},
		{name: "sql_fragment", value: "01arz3ndektsv4rrffq69g5f'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateWorkspaceID(tt.value); err == nil {
				t.Fatalf("ValidateWorkspaceID(%q) should fail", tt.value)
			}
		})
	}
}

func TestPhysicalDatabaseNameRejectsInvalidMigrationSetAndMySQLOverflow(t *testing.T) {
	maximumSet := strings.Repeat("a", 35)
	maximumName, err := PhysicalDatabaseName(maximumSet, validWorkspaceID)
	if err != nil {
		t.Fatalf("64-byte physical database should be accepted: %v", err)
	}
	if len(maximumName) != 64 {
		t.Fatalf("maximum physical database length = %d, want 64", len(maximumName))
	}

	tests := []struct {
		name         string
		migrationSet string
	}{
		{name: "uppercase", migrationSet: "Pandora_account"},
		{name: "sql_fragment", migrationSet: "pandora_account`"},
		{name: "too_long_after_suffix", migrationSet: strings.Repeat("a", 36)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := PhysicalDatabaseName(tt.migrationSet, validWorkspaceID); err == nil {
				t.Fatalf("PhysicalDatabaseName(%q, ...) should fail", tt.migrationSet)
			}
		})
	}
}

func TestParsePhysicalDatabaseNameRoundTripsOnlyExactWorkspaceNames(t *testing.T) {
	database, err := PhysicalDatabaseName("pandora_player", validWorkspaceID)
	if err != nil {
		t.Fatalf("PhysicalDatabaseName() error = %v", err)
	}
	if got, ok := ParsePhysicalDatabaseName("pandora_player", database); !ok || got != validWorkspaceID {
		t.Fatalf("ParsePhysicalDatabaseName() = %q, %v", got, ok)
	}

	invalid := []string{
		"pandora_player",
		"pandora_player_" + validWorkspaceID,
		"pandora_player_w_" + validWorkspaceID[:25],
		"pandora_player_w_" + strings.ToUpper(validWorkspaceID),
		"pandora_player_w_" + validWorkspaceID + "_extra",
		"pandora_account_w_" + validWorkspaceID,
	}
	for _, database := range invalid {
		if got, ok := ParsePhysicalDatabaseName("pandora_player", database); ok {
			t.Errorf("ParsePhysicalDatabaseName(..., %q) = %q, true; want false", database, got)
		}
	}
}
