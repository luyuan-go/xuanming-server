package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestReadVerifyOnlyDSNRemovesSessionSETAndMultiStatements(t *testing.T) {
	dir := t.TempDir()
	dsnPath := filepath.Join(dir, "verify.dsn")
	if err := os.WriteFile(dsnPath, []byte(
		"migrator@tcp(mysql.internal:3306)/pandora_account?tls=true&charset=utf8mb4&lock_wait_timeout=1",
	), 0o600); err != nil {
		t.Fatalf("write DSN: %v", err)
	}
	target := migrationTarget{
		Name: "account", MigrationSet: "pandora_account", Database: "pandora_account",
		DSNFile: dsnPath, TimeoutSeconds: 60, LockWaitTimeoutSeconds: 15,
	}
	cfg, err := readVerifyOnlyDSN(dsnPath, target, true)
	if err != nil {
		t.Fatalf("readVerifyOnlyDSN() error = %v", err)
	}
	if cfg.MultiStatements {
		t.Fatal("verify-only must disable multiStatements")
	}
	if cfg.DBName != target.Database || cfg.TLSConfig != "true" {
		t.Fatalf("verify-only must preserve target/TLS identity, db=%q tls=%q", cfg.DBName, cfg.TLSConfig)
	}
	if len(cfg.Params) != 0 {
		t.Fatalf("verify-only DSN params = %#v; any system variable would emit session SET", cfg.Params)
	}
}

func TestExecuteWorkerTargetModeVerifyNeverCallsMigration(t *testing.T) {
	verifyCalls := 0
	migrateCalls := 0
	err := executeWorkerTargetMode(true,
		func() error {
			verifyCalls++
			return nil
		},
		func() error {
			migrateCalls++
			return errors.New("migration seam must not be called")
		},
	)
	if err != nil {
		t.Fatalf("verify mode error = %v", err)
	}
	if verifyCalls != 1 || migrateCalls != 0 {
		t.Fatalf("verify/migrate calls = %d/%d, want 1/0", verifyCalls, migrateCalls)
	}

	verifyCalls = 0
	migrateCalls = 0
	err = executeWorkerTargetMode(false,
		func() error {
			verifyCalls++
			return errors.New("verify seam must not be called")
		},
		func() error {
			migrateCalls++
			return nil
		},
	)
	if err != nil {
		t.Fatalf("migrate mode error = %v", err)
	}
	if verifyCalls != 0 || migrateCalls != 1 {
		t.Fatalf("migrate mode verify/migrate calls = %d/%d, want 0/1", verifyCalls, migrateCalls)
	}
}

func TestVerifyTargetConnectionUsesOnlyReadOnlyQueries(t *testing.T) {
	target := migrationTarget{Database: "pandora_account", expectedMigrationVersion: 8}
	db, state := openVerifySpyDB(t, verifySpyScenario{
		database:    target.Database,
		currentUser: "migrator@localhost",
		cipher:      "TLS_AES_128_GCM_SHA256",
		version:     8,
	})
	if err := verifyTargetConnection(context.Background(), db, target, "migrator"); err != nil {
		t.Fatalf("verifyTargetConnection() error = %v", err)
	}
	wantQueries := []string{
		"SELECT DATABASE()",
		"SELECT CURRENT_USER()",
		"SHOW SESSION STATUS LIKE 'Ssl_cipher'",
		"SELECT `version`, `dirty` FROM `schema_migrations`",
	}
	if !reflect.DeepEqual(state.queries, wantQueries) {
		t.Fatalf("queries = %#v, want %#v", state.queries, wantQueries)
	}
	if state.pings != 1 || state.execs != 0 || state.begins != 0 || state.prepares != 0 {
		t.Fatalf("ping/query-only counters pings=%d execs=%d begins=%d prepares=%d",
			state.pings, state.execs, state.begins, state.prepares)
	}
}

func TestVerifyTargetConnectionRejectsUnsafeOrStaleState(t *testing.T) {
	target := migrationTarget{Database: "pandora_account", expectedMigrationVersion: 8}
	tests := []struct {
		name      string
		scenario  verifySpyScenario
		wantError string
	}{
		{
			name: "tls_handshake_failure",
			scenario: verifySpyScenario{
				database: target.Database, cipher: "TLS_AES_128_GCM_SHA256", version: 8,
				pingErr: errors.New("tls: failed to verify certificate"),
			},
			wantError: "Ping",
		},
		{
			name:      "wrong_database",
			scenario:  verifySpyScenario{database: "pandora_player", cipher: "TLS_AES_128_GCM_SHA256", version: 8},
			wantError: "DATABASE()",
		},
		{
			name: "overprivileged_or_wrong_account",
			scenario: verifySpyScenario{
				database: target.Database, currentUser: "root@localhost",
				cipher: "TLS_AES_128_GCM_SHA256", version: 8,
			},
			wantError: "CURRENT_USER()",
		},
		{
			name:      "plaintext_session",
			scenario:  verifySpyScenario{database: target.Database, version: 8},
			wantError: "TLS cipher",
		},
		{
			name: "missing_schema_migrations",
			scenario: verifySpyScenario{
				database: target.Database, cipher: "TLS_AES_128_GCM_SHA256", version: 8,
				migrationErr: errors.New("Error 1146: table schema_migrations doesn't exist"),
			},
			wantError: "schema_migrations",
		},
		{
			name:      "old_version",
			scenario:  verifySpyScenario{database: target.Database, cipher: "TLS_AES_128_GCM_SHA256", version: 7},
			wantError: "version=7",
		},
		{
			name:      "newer_version",
			scenario:  verifySpyScenario{database: target.Database, cipher: "TLS_AES_128_GCM_SHA256", version: 9},
			wantError: "version=9",
		},
		{
			name:      "dirty",
			scenario:  verifySpyScenario{database: target.Database, cipher: "TLS_AES_128_GCM_SHA256", version: 8, dirty: true},
			wantError: "dirty=true",
		},
		{
			name: "multiple_version_rows",
			scenario: verifySpyScenario{
				database: target.Database, cipher: "TLS_AES_128_GCM_SHA256", version: 8,
				extraMigrationRow: true,
			},
			wantError: "多行",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, state := openVerifySpyDB(t, tt.scenario)
			err := verifyTargetConnection(context.Background(), db, target, "migrator")
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("verifyTargetConnection() error = %v, want %q", err, tt.wantError)
			}
			if state.execs != 0 || state.begins != 0 || state.prepares != 0 {
				t.Fatalf("failure path performed write-capable operation execs=%d begins=%d prepares=%d",
					state.execs, state.begins, state.prepares)
			}
		})
	}
}

type verifySpyScenario struct {
	database          string
	currentUser       string
	cipher            string
	version           int64
	dirty             bool
	pingErr           error
	migrationErr      error
	extraMigrationRow bool
}

type verifySpyState struct {
	scenario verifySpyScenario
	queries  []string
	pings    int
	execs    int
	begins   int
	prepares int
}

var verifySpyDriverSequence atomic.Uint64

func openVerifySpyDB(t *testing.T, scenario verifySpyScenario) (*sql.DB, *verifySpyState) {
	t.Helper()
	if scenario.currentUser == "" {
		scenario.currentUser = "migrator@%"
	}
	state := &verifySpyState{scenario: scenario}
	name := fmt.Sprintf("pandora_verify_spy_%d", verifySpyDriverSequence.Add(1))
	sql.Register(name, verifySpyDriver{state: state})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open verify spy DB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, state
}

type verifySpyDriver struct{ state *verifySpyState }

func (d verifySpyDriver) Open(string) (driver.Conn, error) {
	return &verifySpyConn{state: d.state}, nil
}

type verifySpyConn struct{ state *verifySpyState }

func (c *verifySpyConn) Prepare(string) (driver.Stmt, error) {
	c.state.prepares++
	return nil, errors.New("verify-only 不得 Prepare")
}

func (*verifySpyConn) Close() error { return nil }

func (c *verifySpyConn) Begin() (driver.Tx, error) {
	c.state.begins++
	return nil, errors.New("verify-only 不得 Begin")
}

func (c *verifySpyConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	c.state.begins++
	return nil, errors.New("verify-only 不得 BeginTx")
}

func (c *verifySpyConn) Ping(context.Context) error {
	c.state.pings++
	return c.state.scenario.pingErr
}

func (c *verifySpyConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	c.state.execs++
	return nil, errors.New("verify-only 不得 Exec")
}

func (c *verifySpyConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.state.queries = append(c.state.queries, query)
	switch query {
	case "SELECT DATABASE()":
		return &verifySpyRows{columns: []string{"DATABASE()"}, values: [][]driver.Value{{c.state.scenario.database}}}, nil
	case "SELECT CURRENT_USER()":
		return &verifySpyRows{columns: []string{"CURRENT_USER()"}, values: [][]driver.Value{{c.state.scenario.currentUser}}}, nil
	case "SHOW SESSION STATUS LIKE 'Ssl_cipher'":
		return &verifySpyRows{
			columns: []string{"Variable_name", "Value"},
			values:  [][]driver.Value{{"Ssl_cipher", c.state.scenario.cipher}},
		}, nil
	case "SELECT `version`, `dirty` FROM `schema_migrations`":
		if c.state.scenario.migrationErr != nil {
			return nil, c.state.scenario.migrationErr
		}
		values := [][]driver.Value{{c.state.scenario.version, c.state.scenario.dirty}}
		if c.state.scenario.extraMigrationRow {
			values = append(values, []driver.Value{c.state.scenario.version, c.state.scenario.dirty})
		}
		return &verifySpyRows{columns: []string{"version", "dirty"}, values: values}, nil
	default:
		return nil, fmt.Errorf("verify-only 非白名单 query: %s", query)
	}
}

type verifySpyRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *verifySpyRows) Columns() []string { return r.columns }
func (*verifySpyRows) Close() error        { return nil }

func (r *verifySpyRows) Next(destination []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(destination, r.values[r.index])
	r.index++
	return nil
}
