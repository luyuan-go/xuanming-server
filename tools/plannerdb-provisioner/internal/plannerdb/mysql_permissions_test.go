package plannerdb

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/luyuancpp/pandora/tools/migrate/workspacedb"
)

func TestGeneratedAccountNamesMatchClientContract(t *testing.T) {
	workspaceID := "01arz3ndektsv4rrffq69g5fav"
	for _, username := range []string{"p_app_" + workspaceID, "p_mig_" + workspaceID} {
		if len(username) != 32 || !credentialUsernamePattern.MatchString(username) {
			t.Fatalf("username=%q length=%d", username, len(username))
		}
	}
	for _, oldName := range []string{"pda_" + workspaceID, "pdm_" + workspaceID} {
		if credentialUsernamePattern.MatchString(oldName) {
			t.Fatalf("legacy username unexpectedly accepted: %s", oldName)
		}
	}
}

func TestAccountHostBoundaryRejectsAmbiguousMySQLRows(t *testing.T) {
	if err := validateAccountHosts([]string{"%"}); err != nil {
		t.Fatalf("canonical account rejected: %v", err)
	}
	for _, hosts := range [][]string{nil, {"localhost"}, {"%", "10.%"}, {"%", "localhost"}} {
		if err := validateAccountHosts(hosts); err == nil {
			t.Fatalf("ambiguous hosts accepted: %v", hosts)
		}
	}
}

func TestProvisionerRegistryRequiresAuditScanIndexes(t *testing.T) {
	want := map[string]string{
		"planner_workspaces.idx_planner_workspaces_state_updated":        "state,updated_at",
		"planner_workspaces.idx_planner_workspaces_last_seen":            "last_seen_at",
		"planner_enrollment_tokens.idx_planner_enrollment_tokens_expiry": "consumed_at,expires_at",
	}
	for _, index := range requiredRegistryIndexes() {
		key := index.table + "." + index.index
		columns, tracked := want[key]
		if !tracked {
			continue
		}
		if index.columns != columns || index.nonUnique != 1 {
			t.Fatalf("audit index %s shape=%s/%d", key, index.columns, index.nonUnique)
		}
		delete(want, key)
	}
	if len(want) != 0 {
		t.Fatalf("verifyRegistryShape missing audit indexes: %v", want)
	}
}

func TestMigrationPrivilegesCoverEveryEmbeddedUpOperation(t *testing.T) {
	workspaceID := "01arz3ndektsv4rrffq69g5fav"
	databases := make(map[string]string)
	for _, migrationSet := range workspacedb.CanonicalMigrationSets() {
		database, err := workspacedb.PhysicalDatabaseName(migrationSet, workspaceID)
		if err != nil {
			t.Fatal(err)
		}
		databases[migrationSet] = database
	}
	migrationPrivileges := expectedMigrationPrivileges(databases)
	runtimePrivileges := expectedRuntimePrivileges(databases)

	required := map[string]bool{}
	foundRenameTable := false
	for _, migrationSet := range workspacedb.CanonicalMigrationSets() {
		entries, err := os.ReadDir(filepath.Join(migrationsDirectory(t), migrationSet))
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".up.sql") {
				continue
			}
			raw, err := os.ReadFile(filepath.Join(migrationsDirectory(t), migrationSet, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			sqlText := stripSQLLineComments(string(raw))
			for privilege, pattern := range map[string]*regexp.Regexp{
				"SELECT": regexp.MustCompile(`(?i)\bSELECT\b`),
				"INSERT": regexp.MustCompile(`(?i)\bINSERT\b`),
				"UPDATE": regexp.MustCompile(`(?i)\bUPDATE\b`),
				"DELETE": regexp.MustCompile(`(?i)\bDELETE\b`),
				"CREATE": regexp.MustCompile(`(?i)\bCREATE\s+(?:TABLE|INDEX)\b`),
				"ALTER":  regexp.MustCompile(`(?i)\bALTER\s+TABLE\b`),
				"DROP":   regexp.MustCompile(`(?i)\bDROP\s+TABLE\b`),
				"INDEX":  regexp.MustCompile(`(?i)\b(?:CREATE|DROP)\s+INDEX\b|\bALTER\s+TABLE\b[^;]*(?:ADD|DROP|RENAME)\s+(?:UNIQUE\s+)?(?:KEY|INDEX)\b`),
			} {
				if pattern.MatchString(sqlText) {
					required[privilege] = true
				}
			}
			if regexp.MustCompile(`(?i)\bRENAME\s+TABLE\b`).MatchString(sqlText) {
				foundRenameTable = true
				for _, privilege := range []string{"ALTER", "DROP", "CREATE", "INSERT"} {
					required[privilege] = true
				}
			}
		}
	}
	if !foundRenameTable || !required["DROP"] {
		t.Fatal("migration scan did not observe the RENAME/DROP contract")
	}
	for database, privileges := range migrationPrivileges {
		actual := make(map[string]bool, len(privileges))
		for _, privilege := range privileges {
			actual[privilege] = true
		}
		for privilege := range required {
			if !actual[privilege] {
				t.Fatalf("migration account on %s lacks %s required by embedded up SQL", database, privilege)
			}
		}
	}
	for database, privileges := range runtimePrivileges {
		for _, privilege := range privileges {
			if privilege == "DROP" {
				t.Fatalf("runtime account on %s unexpectedly has DROP", database)
			}
		}
	}
}

func migrationsDirectory(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "..", "..", "migrate", "migrations"))
}

func stripSQLLineComments(value string) string {
	lines := strings.Split(value, "\n")
	for index, line := range lines {
		if comment := strings.Index(line, "--"); comment >= 0 {
			lines[index] = line[:comment]
		}
	}
	return strings.Join(lines, "\n")
}
