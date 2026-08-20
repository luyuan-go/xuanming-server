package workspacedb

import "testing"

func TestCanonicalMigrationSetsReturnsUnmodifiableCopy(t *testing.T) {
	first := CanonicalMigrationSets()
	if len(first) != 10 {
		t.Fatalf("migration sets=%d, want=10", len(first))
	}
	first[0] = "tampered"
	second := CanonicalMigrationSets()
	if second[0] != "pandora_account" {
		t.Fatalf("caller mutated canonical migration sets: %v", second)
	}
}
