package plannerdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/luyuancpp/pandora/tools/migrate/workspacedb"
)

func TestMigrationRunnerUsesExactWorkspaceManifestAndNoSecretsInArguments(t *testing.T) {
	binaryPath := filepath.Join(t.TempDir(), "pandora-migrate.bin")
	binaryBytes := []byte("pinned-migrate-binary")
	if err := os.WriteFile(binaryPath, binaryBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	binaryHash := sha256.Sum256(binaryBytes)
	access := testWorkspaceAccess(t)
	var manifestPath string
	executor := func(_ context.Context, executable string, arguments, environment []string) ([]byte, error) {
		if strings.Contains(strings.Join(arguments, " "), access.AppPassword) || strings.Contains(strings.Join(arguments, " "), access.MigrationPassword) {
			return nil, errors.New("secret leaked in arguments")
		}
		if strings.Contains(strings.Join(environment, "\n"), "PANDORA_TEST_SECRET_SENTINEL") {
			return nil, errors.New("parent secret env leaked")
		}
		copied, err := os.ReadFile(executable)
		if err != nil || string(copied) != string(binaryBytes) {
			return nil, errors.New("migrate binary snapshot mismatch")
		}
		for i := 0; i+1 < len(arguments); i++ {
			if arguments[i] == "-targets-file" {
				manifestPath = arguments[i+1]
			}
		}
		if manifestPath == "" {
			return nil, errors.New("missing targets file")
		}
		data, err := os.ReadFile(manifestPath)
		if err != nil {
			return nil, err
		}
		var manifest struct {
			Targets []struct {
				Name         string `json:"name"`
				MigrationSet string `json:"migration_set"`
				Database     string `json:"database"`
				DSNFile      string `json:"dsn_file"`
				TLSCAFile    string `json:"tls_ca_file"`
			} `json:"targets"`
		}
		if err := json.Unmarshal(data, &manifest); err != nil {
			return nil, err
		}
		if len(manifest.Targets) != 10 {
			return nil, errors.New("target count mismatch")
		}
		for _, target := range manifest.Targets {
			want, err := workspacedb.PhysicalDatabaseName(target.MigrationSet, access.WorkspaceID)
			if err != nil || target.Database != want || !filepath.IsAbs(target.TLSCAFile) {
				return nil, errors.New("workspace mapping mismatch")
			}
			dsnBytes, err := os.ReadFile(filepath.Join(filepath.Dir(manifestPath), target.DSNFile))
			if err != nil {
				return nil, err
			}
			config, err := mysql.ParseDSN(strings.TrimSpace(string(dsnBytes)))
			if err != nil || config.User != access.MigrationUsername || config.Passwd != access.MigrationPassword || config.DBName != want || config.TLSConfig != "true" {
				return nil, errors.New("migration DSN mismatch")
			}
		}
		return []byte("migration complete"), nil
	}
	t.Setenv("PANDORA_TEST_SECRET_SENTINEL", "must-not-leak")
	runner, err := newExecMigrationRunner(execMigrationConfig{
		BinaryPath:     binaryPath,
		ExpectedSHA256: hex.EncodeToString(binaryHash[:]),
		MySQLCAPEM:     []byte("test-ca"),
		Timeout:        time.Minute,
		Executor:       executor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.MigrateWorkspace(context.Background(), access); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(manifestPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("secret migration bundle was not removed: %v", err)
	}
}

func testWorkspaceAccess(t *testing.T) WorkspaceAccess {
	t.Helper()
	workspaceID := "01arz3ndektsv4rrffq69g5fav"
	migrationSets := workspacedb.CanonicalMigrationSets()
	databases := make(map[string]string, len(migrationSets))
	for _, migrationSet := range migrationSets {
		database, err := workspacedb.PhysicalDatabaseName(migrationSet, workspaceID)
		if err != nil {
			t.Fatal(err)
		}
		databases[migrationSet] = database
	}
	return WorkspaceAccess{
		WorkspaceID: workspaceID,
		AppUsername: "p_app_" + workspaceID, AppPassword: strings.Repeat("A", 43),
		MigrationUsername: "p_mig_" + workspaceID, MigrationPassword: strings.Repeat("B", 43),
		CredentialVersion: 1,
		Endpoint:          Endpoint{Host: "pandora-dev-db.intra", Port: 3306, TLSServerName: "pandora-dev-db.intra"},
		Databases:         databases,
	}
}
