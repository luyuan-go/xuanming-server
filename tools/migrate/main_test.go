package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type stubVersionReader struct {
	version uint
	dirty   bool
	err     error
}

func (s stubVersionReader) Version() (uint, bool, error) {
	return s.version, s.dirty, s.err
}

func TestParseCommandConfigBootstrapDefaultsFalse(t *testing.T) {
	t.Setenv("MIGRATE_BOOTSTRAP_DB", "")
	t.Setenv("MIGRATE_TARGETS_FILE", "")
	t.Setenv("MIGRATE_EXPECTED_TARGETS", "")
	t.Setenv("MIGRATE_VERIFY_ONLY", "")
	t.Setenv("PANDORA_ENV", "")
	path := filepath.Join(t.TempDir(), "targets.json")
	cfg, err := parseCommandConfig([]string{
		"-targets-file", path,
		"-expected-targets", "auction-a:pandora_auction:pandora_auction",
	})
	if err != nil {
		t.Fatalf("parseCommandConfig() error = %v", err)
	}
	if cfg.Bootstrap {
		t.Fatal("bootstrap 默认值必须为 false")
	}
	if cfg.VerifyOnly {
		t.Fatal("verify-only 默认值必须为 false")
	}
	if cfg.Environment != "production" {
		t.Fatalf("environment = %q, want production", cfg.Environment)
	}
}

func TestParseCommandConfigRequiresIndependentExpectedTargets(t *testing.T) {
	t.Setenv("MIGRATE_BOOTSTRAP_DB", "")
	t.Setenv("MIGRATE_EXPECTED_TARGETS", "")
	_, err := parseCommandConfig([]string{"-targets-file", "targets.json"})
	if err == nil || !strings.Contains(err.Error(), "expected-targets") {
		t.Fatalf("error = %v, want expected-targets requirement", err)
	}
}

func TestParseCommandConfigRejectsInvalidBootstrapEnv(t *testing.T) {
	t.Setenv("MIGRATE_BOOTSTRAP_DB", "yes-please")
	_, err := parseCommandConfig([]string{
		"-targets-file", "targets.json",
		"-expected-targets", "auction-a:pandora_auction:pandora_auction",
	})
	if err == nil || !strings.Contains(err.Error(), "必须是 true/false") {
		t.Fatalf("error = %v, want strict bool error", err)
	}
}

func TestParseCommandConfigValidatesWorkspaceGuard(t *testing.T) {
	t.Setenv("MIGRATE_BOOTSTRAP_DB", "")
	t.Setenv("MIGRATE_WORKSPACE_ID", "")
	base := []string{
		"-targets-file", "targets.json",
		"-expected-targets", "account:pandora_account:pandora_account_w_" + testWorkspaceID,
	}

	cfg, err := parseCommandConfig(append(base, "-workspace-id", testWorkspaceID))
	if err != nil {
		t.Fatalf("valid workspace guard error = %v", err)
	}
	if cfg.WorkspaceID != testWorkspaceID {
		t.Fatalf("workspace id = %q, want %q", cfg.WorkspaceID, testWorkspaceID)
	}

	if _, err := parseCommandConfig(append(base, "-workspace-id", "short")); err == nil ||
		!strings.Contains(err.Error(), "workspace_id") {
		t.Fatalf("invalid workspace id error = %v", err)
	}

	if _, err := parseCommandConfig(append(base,
		"-workspace-id", testWorkspaceID,
		"-bootstrap=true",
		"-environment=dev",
	)); err == nil || !strings.Contains(err.Error(), "bootstrap") {
		t.Fatalf("workspace bootstrap error = %v", err)
	}
}

func TestParseCommandConfigVerifyOnlyCLIOverridesEnvAndRejectsBootstrap(t *testing.T) {
	t.Setenv("MIGRATE_BOOTSTRAP_DB", "")
	t.Setenv("MIGRATE_WORKSPACE_ID", "")
	t.Setenv("MIGRATE_VERIFY_ONLY", "true")
	base := []string{
		"-targets-file", "targets.json",
		"-expected-targets", "account:pandora_account:pandora_account",
	}

	cfg, err := parseCommandConfig(append(append([]string{}, base...), "-verify-only=false"))
	if err != nil {
		t.Fatalf("CLI false override error = %v", err)
	}
	if cfg.VerifyOnly {
		t.Fatal("explicit -verify-only=false must override MIGRATE_VERIFY_ONLY=true")
	}

	cfg, err = parseCommandConfig(base)
	if err != nil {
		t.Fatalf("env verify-only error = %v", err)
	}
	if !cfg.VerifyOnly {
		t.Fatal("MIGRATE_VERIFY_ONLY=true should enable verify-only by default")
	}

	_, err = parseCommandConfig(append(append([]string{}, base...),
		"-verify-only=true", "-bootstrap=true", "-environment=dev"))
	if err == nil || !strings.Contains(err.Error(), "verify-only") || !strings.Contains(err.Error(), "bootstrap") {
		t.Fatalf("verify-only/bootstrap conflict error = %v", err)
	}

	t.Setenv("MIGRATE_VERIFY_ONLY", "not-a-bool")
	if _, err := parseCommandConfig(base); err == nil || !strings.Contains(err.Error(), "MIGRATE_VERIFY_ONLY") {
		t.Fatalf("invalid verify-only env error = %v", err)
	}
	cfg, err = parseCommandConfig(append(append([]string{}, base...), "-verify-only=false"))
	if err != nil || cfg.VerifyOnly {
		t.Fatalf("explicit CLI must override even invalid verify env: cfg=%+v err=%v", cfg, err)
	}
}

func TestLoadManifestSupportsMultipleAuctionShards(t *testing.T) {
	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "auction-00.dsn"),
		"migrator@tcp(mysql-a:3306)/pandora_auction_00")
	writeTestFile(t, filepath.Join(dir, "auction-01.dsn"),
		"migrator@tcp(mysql-a:3306)/pandora_auction_01")
	manifestPath := filepath.Join(dir, "targets.json")
	writeTestFile(t, manifestPath, `{
  "targets": [
    {
      "name": "auction-shard-00",
      "migration_set": "pandora_auction",
      "database": "pandora_auction_00",
      "dsn_file": "auction-00.dsn"
    },
    {
      "name": "auction-shard-01",
      "migration_set": "pandora_auction",
      "database": "pandora_auction_01",
      "dsn_file": "auction-01.dsn",
      "timeout_seconds": 1200,
      "lock_wait_timeout_seconds": 20
    }
  ]
}`)

	manifest, err := loadTargetManifest(manifestPath)
	if err != nil {
		t.Fatalf("loadTargetManifest() error = %v", err)
	}
	if got := len(manifest.Targets); got != 2 {
		t.Fatalf("targets len = %d, want 2", got)
	}
	if got := manifest.Targets[0].expectedMigrationVersion; got == 0 {
		t.Fatal("auction expected version must be derived from embedded migrations")
	}
	if got := manifest.Targets[0].TimeoutSeconds; got != defaultTargetTimeoutSeconds {
		t.Fatalf("default timeout = %d", got)
	}
	if got := manifest.Targets[0].LockWaitTimeoutSeconds; got != defaultLockWaitTimeoutSeconds {
		t.Fatalf("default lock timeout = %d", got)
	}
	if err := preflightTargets(manifest.Targets, false, false); err != nil {
		t.Fatalf("preflightTargets() error = %v", err)
	}
}

func TestValidateExpectedTargetsRequiresExactInventory(t *testing.T) {
	targets := []migrationTarget{
		{Name: "account-primary", MigrationSet: "pandora_account", Database: "pandora_account"},
		{Name: "auction-shard-00", MigrationSet: "pandora_auction", Database: "pandora_auction_00"},
	}
	expected, err := parseExpectedTargetInventory(
		"auction-shard-00:pandora_auction:pandora_auction_00," +
			"account-primary:pandora_account:pandora_account",
	)
	if err != nil {
		t.Fatalf("parseExpectedTargetInventory() error = %v", err)
	}
	if err := validateExpectedTargets(targets, expected); err != nil {
		t.Fatalf("exact inventory error = %v", err)
	}

	missing, err := parseExpectedTargetInventory(
		"account-primary:pandora_account:pandora_account," +
			"auction-shard-00:pandora_auction:pandora_auction_00," +
			"auction-shard-01:pandora_auction:pandora_auction_01",
	)
	if err != nil {
		t.Fatalf("parse missing inventory: %v", err)
	}
	if err := validateExpectedTargets(targets, missing); err == nil || !strings.Contains(err.Error(), "auction-shard-01") {
		t.Fatalf("missing target error = %v", err)
	}

	partial, err := parseExpectedTargetInventory("account-primary:pandora_account:pandora_account")
	if err != nil {
		t.Fatalf("parse partial inventory: %v", err)
	}
	if err := validateExpectedTargets(targets, partial); err == nil || !strings.Contains(err.Error(), "unexpected") {
		t.Fatalf("unexpected target error = %v", err)
	}

	swapped, err := parseExpectedTargetInventory(
		"account-primary:pandora_account:pandora_account," +
			"auction-shard-00:pandora_auction:pandora_auction_01",
	)
	if err != nil {
		t.Fatalf("parse swapped inventory: %v", err)
	}
	if err := validateExpectedTargets(targets, swapped); err == nil ||
		!strings.Contains(err.Error(), "pandora_auction_00") ||
		!strings.Contains(err.Error(), "pandora_auction_01") {
		t.Fatalf("swapped descriptor error = %v", err)
	}
}

func TestPreflightRejectsDuplicatePhysicalDatabase(t *testing.T) {
	dir := t.TempDir()
	dsnPath := filepath.Join(dir, "same.dsn")
	writeTestFile(t, dsnPath, "migrator@tcp(mysql-a:3306)/pandora_auction")
	targets := []migrationTarget{
		testTarget("auction-a", "pandora_auction", dsnPath),
		testTarget("auction-b", "pandora_auction", dsnPath),
	}
	err := preflightTargets(targets, false, false)
	if err == nil || !strings.Contains(err.Error(), "同一物理库") {
		t.Fatalf("error = %v, want duplicate physical database error", err)
	}
}

func TestPreflightBootstrapRequiresSeparateAdminDSN(t *testing.T) {
	dir := t.TempDir()
	dsnPath := filepath.Join(dir, "target.dsn")
	writeTestFile(t, dsnPath, "migrator@tcp(mysql-a:3306)/pandora_auction")
	err := preflightTargets([]migrationTarget{
		testTarget("auction-a", "pandora_auction", dsnPath),
	}, true, false)
	if err == nil || !strings.Contains(err.Error(), "bootstrap_admin_dsn_file") {
		t.Fatalf("error = %v, want separate admin DSN requirement", err)
	}
}

func TestReadAndHardenDSNRejectsDatabaseMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "target.dsn")
	writeTestFile(t, path, "migrator@tcp(mysql-a:3306)/wrong_database")
	target := testTarget("auction-a", "pandora_auction", path)
	_, err := readAndHardenDSN(path, target, false)
	if err == nil || !strings.Contains(err.Error(), "不一致") {
		t.Fatalf("error = %v, want database mismatch", err)
	}
}

func TestReadAndHardenDSNForcesFiniteTimeouts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "target.dsn")
	writeTestFile(t, path,
		"migrator@tcp(mysql-a:3306)/pandora_auction?timeout=2h&readTimeout=2h&writeTimeout=2h")
	target := testTarget("auction-a", "pandora_auction", path)
	target.TimeoutSeconds = 120
	target.LockWaitTimeoutSeconds = 7
	cfg, err := readAndHardenDSN(path, target, false)
	if err != nil {
		t.Fatalf("readAndHardenDSN() error = %v", err)
	}
	if cfg.Timeout != connectionTimeout {
		t.Fatalf("connect timeout = %s, want %s", cfg.Timeout, connectionTimeout)
	}
	if cfg.ReadTimeout != 115*time.Second || cfg.WriteTimeout != 115*time.Second {
		t.Fatalf("read/write timeout = %s/%s", cfg.ReadTimeout, cfg.WriteTimeout)
	}
	if got := cfg.Params["lock_wait_timeout"]; got != "7" {
		t.Fatalf("lock_wait_timeout = %q, want 7", got)
	}
	if got := cfg.Params["innodb_lock_wait_timeout"]; got != "7" {
		t.Fatalf("innodb_lock_wait_timeout = %q, want 7", got)
	}
	if !cfg.MultiStatements || !cfg.ParseTime {
		t.Fatal("runner 必须强制 multiStatements/parseTime")
	}
}

func TestReadAndHardenDSNProductionRequiresVerifiedTLS(t *testing.T) {
	tests := []struct {
		name   string
		params string
		wantOK bool
	}{
		{name: "verified", params: "tls=true", wantOK: true},
		{name: "missing"},
		{name: "false", params: "tls=false"},
		{name: "preferred", params: "tls=preferred"},
		{name: "skip_verify", params: "tls=skip-verify"},
		{name: "fallback", params: "tls=true&allowFallbackToPlaintext=true"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "target.dsn")
			dsn := "migrator@tcp(mysql-a:3306)/pandora_auction"
			if tt.params != "" {
				dsn += "?" + tt.params
			}
			writeTestFile(t, path, dsn)
			target := testTarget("auction-a", "pandora_auction", path)
			_, err := readAndHardenDSN(path, target, true)
			if tt.wantOK && err != nil {
				t.Fatalf("verified TLS error = %v", err)
			}
			if !tt.wantOK && (err == nil || !strings.Contains(err.Error(), "tls=true")) {
				t.Fatalf("error = %v, want fail-closed TLS error", err)
			}
		})
	}
}

func TestLoadManifestRejectsUnknownField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "targets.json")
	writeTestFile(t, path, `{
  "targets": [{
    "name": "auction-a",
    "migration_set": "pandora_auction",
    "database": "pandora_auction",
    "dsn": "forbidden-inline-value"
  }]
}`)
	_, err := loadTargetManifest(path)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("error = %v, want unknown field error", err)
	}
}

func TestLoadManifestRejectsCredentialPathOutsideBundle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "targets.json")
	writeTestFile(t, path, `{
  "targets": [{
    "name": "auction-a",
    "migration_set": "pandora_auction",
    "database": "pandora_auction",
    "dsn_file": "../outside.dsn"
  }]
}`)
	_, err := loadTargetManifest(path)
	if err == nil || !strings.Contains(err.Error(), "必须位于目标清单所在目录") {
		t.Fatalf("error = %v, want credential bundle traversal rejection", err)
	}
}

func TestLoadManifestRejectsWrongMigrationDatabaseMapping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "targets.json")
	writeTestFile(t, path, `{
  "targets": [{
    "name": "account-primary",
    "migration_set": "pandora_auction",
    "database": "pandora_account",
    "dsn_file": "account.dsn"
  }]
}`)
	_, err := loadTargetManifest(path)
	if err == nil || !strings.Contains(err.Error(), "必须等于 migration_set") {
		t.Fatalf("error = %v, want migration/database mapping rejection", err)
	}
}

func TestBootstrapEnvironmentAllowlist(t *testing.T) {
	for _, environment := range []string{"local", "dev", "development", "DEV"} {
		if !isDevelopmentEnvironment(environment) {
			t.Errorf("%q should allow bootstrap", environment)
		}
	}
	for _, environment := range []string{"", "prod", "production", "test", "staging"} {
		if isDevelopmentEnvironment(environment) {
			t.Errorf("%q must reject bootstrap", environment)
		}
	}
}

func TestWorkspaceTargetsAlwaysRequireVerifiedTLS(t *testing.T) {
	for _, environment := range []string{"local", "dev", "development", "production"} {
		if !requiresVerifiedTLS(environment, testWorkspaceID) {
			t.Errorf("workspace environment %q must require verified TLS", environment)
		}
	}
	if requiresVerifiedTLS("dev", "") {
		t.Fatal("legacy local dev without workspace should keep plaintext compatibility")
	}
	if !requiresVerifiedTLS("production", "") {
		t.Fatal("production must require verified TLS")
	}
}

func TestRejectDirtyOrNewer(t *testing.T) {
	if err := rejectDirtyOrNewer(stubVersionReader{version: 2, dirty: true}, 2); err == nil || !strings.Contains(err.Error(), "dirty=true") {
		t.Fatalf("dirty error = %v", err)
	}
	if err := rejectDirtyOrNewer(stubVersionReader{version: 3}, 2); err == nil || !strings.Contains(err.Error(), "高于") {
		t.Fatalf("newer error = %v", err)
	}
	if err := rejectDirtyOrNewer(stubVersionReader{err: errors.New("read failed")}, 2); err == nil || !strings.Contains(err.Error(), "读取") {
		t.Fatalf("read error = %v", err)
	}
	if err := rejectDirtyOrNewer(stubVersionReader{version: 2}, 2); err != nil {
		t.Fatalf("clean current version error = %v", err)
	}
}

func TestValidateFinalVersion(t *testing.T) {
	if err := validateFinalVersion(2, false, nil, 2); err != nil {
		t.Fatalf("valid final version error = %v", err)
	}
	for name, err := range map[string]error{
		"dirty":    validateFinalVersion(2, true, nil, 2),
		"old":      validateFinalVersion(1, false, nil, 2),
		"read_err": validateFinalVersion(0, false, errors.New("read failed"), 2),
	} {
		if err == nil {
			t.Errorf("%s should fail", name)
		}
	}
}

func TestAdvisoryLockWrapperOutlivesDriverWait(t *testing.T) {
	if got := advisoryLockTimeout(migrationTarget{LockWaitTimeoutSeconds: 1}); got != 11*time.Second {
		t.Fatalf("minimum advisory wrapper timeout = %s, want 11s", got)
	}
	if got := advisoryLockTimeout(migrationTarget{LockWaitTimeoutSeconds: 20}); got != 20*time.Second {
		t.Fatalf("configured advisory wrapper timeout = %s, want 20s", got)
	}
}

func TestWorkerCommandArgsAuthoritativelyPropagateVerifyOnly(t *testing.T) {
	t.Setenv("MIGRATE_BOOTSTRAP_DB", "")
	t.Setenv("MIGRATE_WORKSPACE_ID", "")
	t.Setenv("MIGRATE_VERIFY_ONLY", "true")
	expected := []targetDescriptor{{
		Name: "account-primary", MigrationSet: "pandora_account", Database: "pandora_account",
	}}
	target := migrationTarget{Name: "account-primary"}

	args := workerCommandArgs("targets.json", "dev", "", false, expected, target)
	cfg, err := parseCommandConfig(args)
	if err != nil {
		t.Fatalf("parse worker args: %v", err)
	}
	if cfg.VerifyOnly {
		t.Fatal("worker args must carry explicit -verify-only=false over inherited true env")
	}
	if cfg.Bootstrap {
		t.Fatal("worker must keep bootstrap disabled")
	}
	if cfg.WorkerTarget != target.Name {
		t.Fatalf("worker target = %q, want %q", cfg.WorkerTarget, target.Name)
	}

	args = workerCommandArgs("targets.json", "production", testWorkspaceID, true, expected, target)
	cfg, err = parseCommandConfig(args)
	if err != nil {
		t.Fatalf("parse verify worker args: %v", err)
	}
	if !cfg.VerifyOnly || cfg.WorkspaceID != testWorkspaceID {
		t.Fatalf("worker verify/workspace = %v/%q", cfg.VerifyOnly, cfg.WorkspaceID)
	}
}

func testTarget(name, database, dsnFile string) migrationTarget {
	return migrationTarget{
		Name:                   name,
		MigrationSet:           "pandora_auction",
		Database:               database,
		DSNFile:                dsnFile,
		TimeoutSeconds:         defaultTargetTimeoutSeconds,
		LockWaitTimeoutSeconds: defaultLockWaitTimeoutSeconds,
	}
}

func writeTestFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
