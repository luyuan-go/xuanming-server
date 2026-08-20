package plannerdb

import "testing"

func TestAuditBlockingFindingsExcludeInactiveOnly(t *testing.T) {
	if (AuditReport{Counts: AuditCounts{InactiveWorkspaces: 10}}).HasBlockingFindings() {
		t.Fatal("inactive workspace must remain report-only")
	}
	for _, counts := range []AuditCounts{
		{StaleInProgress: 1},
		{MigrationFailed: 1},
		{ExpiredUnusedTokenHashes: 1},
		{ExpiredConsumedHashes: 1},
	} {
		if !(AuditReport{Counts: counts}).HasBlockingFindings() {
			t.Fatalf("blocking counts not detected: %+v", counts)
		}
	}
}
