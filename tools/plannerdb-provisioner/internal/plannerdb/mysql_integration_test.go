package plannerdb

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/luyuancpp/pandora/tools/migrate/workspacedb"
)

const destructiveMySQLIntegrationAck = "I_UNDERSTAND_THIS_DROPS_ONLY_RANDOM_TEST_WORKSPACES"

// TestMySQLIntegrationFreshMigrationAndIsolation 只在显式提供隔离 MySQL 8 + 确认值时运行。
// 它用真实 pandora-migrate 验证 fresh 十库、DROP/RENAME 权限、跨 workspace 拒绝、
// recovery token 绑定与 MIGRATION_FAILED 显式 CAS。
func TestMySQLIntegrationFreshMigrationAndIsolation(t *testing.T) {
	adminDSN := os.Getenv("PANDORA_PLANNERDB_TEST_ADMIN_DSN")
	caPath := os.Getenv("PANDORA_PLANNERDB_TEST_CA_FILE")
	migrateBinary := os.Getenv("PANDORA_PLANNERDB_TEST_MIGRATE_BINARY")
	if adminDSN == "" || caPath == "" || migrateBinary == "" {
		t.Skip("set PANDORA_PLANNERDB_TEST_ADMIN_DSN/CA_FILE/MIGRATE_BINARY to run isolated MySQL integration")
	}
	if os.Getenv("PANDORA_PLANNERDB_TEST_DESTRUCTIVE_ACK") != destructiveMySQLIntegrationAck {
		t.Fatal("isolated MySQL integration requires explicit destructive ack")
	}
	host := os.Getenv("PANDORA_PLANNERDB_TEST_HOST")
	if host == "" {
		host = "localhost"
	}
	portValue, err := strconv.ParseUint(os.Getenv("PANDORA_PLANNERDB_TEST_PORT"), 10, 16)
	if err != nil || portValue == 0 {
		t.Fatal("PANDORA_PLANNERDB_TEST_PORT must be 1..65535")
	}
	endpoint := Endpoint{Host: host, Port: uint16(portValue), TLSServerName: host}
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	binaryImage, err := os.ReadFile(migrateBinary)
	if err != nil {
		t.Fatal(err)
	}
	binaryHash := sha256.Sum256(binaryImage)
	registryRandom := make([]byte, 8)
	if _, err := rand.Read(registryRandom); err != nil {
		t.Fatal(err)
	}
	registrySuffix := hex.EncodeToString(registryRandom)
	backendConfig := BackendConfig{
		AdminDSN: adminDSN, MySQLCAPEM: caPEM, Endpoint: endpoint,
		RegistryDatabase:       "pandora_planner_registry_it_" + registrySuffix,
		RegistryInstanceMarker: "it-" + registrySuffix,
		LockMonitorInterval:    50 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	preflightBackend, err := openAdminBackend(ctx, backendConfig)
	if err != nil {
		t.Fatal(err)
	}
	var registryExists int
	if err := preflightBackend.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=?`, backendConfig.RegistryDatabase).Scan(&registryExists); err != nil {
		t.Fatal(err)
	}
	if registryExists != 0 {
		t.Fatalf("random registry unexpectedly exists before test: %s", backendConfig.RegistryDatabase)
	}
	if opened, err := OpenMySQLBackend(ctx, backendConfig); err == nil {
		_ = opened.Close()
		t.Fatal("open-existing unexpectedly initialized missing registry")
	}
	if err := preflightBackend.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=?`, backendConfig.RegistryDatabase).Scan(&registryExists); err != nil || registryExists != 0 {
		t.Fatalf("open-existing executed DDL: exists=%d err=%v", registryExists, err)
	}
	if err := preflightBackend.Close(); err != nil {
		t.Fatal(err)
	}
	backend, err := InitMySQLRegistry(ctx, backendConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	assertKilledWorkspaceLockCancelsCallback(t, ctx, backend, backendConfig)
	// Mutant 门禁：audit 扫描索引被误删时启动 shape verifier 必须拒绝。
	if _, err := backend.db.ExecContext(ctx, "DROP INDEX idx_planner_workspaces_state_updated ON "+backend.registryTable("planner_workspaces")); err != nil {
		t.Fatal(err)
	}
	if err := backend.verifyRegistryShape(ctx); err == nil {
		t.Fatal("registry verifier accepted missing audit index")
	}
	if _, err := backend.db.ExecContext(ctx, "CREATE INDEX idx_planner_workspaces_state_updated ON "+backend.registryTable("planner_workspaces")+" (state, updated_at)"); err != nil {
		t.Fatal(err)
	}
	if err := backend.verifyRegistryShape(ctx); err != nil {
		t.Fatalf("registry verifier did not recover after index restoration: %v", err)
	}
	runner, err := newExecMigrationRunner(execMigrationConfig{
		BinaryPath: migrateBinary, ExpectedSHA256: hex.EncodeToString(binaryHash[:]),
		MySQLCAPEM: caPEM, Timeout: 3 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := newService(serviceConfig{
		Registry: backend, Resources: backend, Migrator: runner, Endpoint: endpoint,
		MasterKey: make([]byte, 32), Random: rand.Reader, ProvisionTimeout: 4 * time.Minute,
		WorkspaceLockWait: 5 * time.Second, MaxConcurrentProvisions: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	var workspaces []WorkspaceAccess
	defer func() {
		_ = service.close(10 * time.Second)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		for _, access := range workspaces {
			_, _ = backend.db.ExecContext(cleanupCtx, "DROP USER IF EXISTS "+mysqlAccount(access.AppUsername))
			_, _ = backend.db.ExecContext(cleanupCtx, "DROP USER IF EXISTS "+mysqlAccount(access.MigrationUsername))
			_, _ = backend.db.ExecContext(cleanupCtx, "DROP USER IF EXISTS '"+access.AppUsername+"'@'localhost'")
			for _, database := range access.Databases {
				_, _ = backend.db.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS `"+database+"`")
			}
		}
		var marker string
		if err := backend.db.QueryRowContext(cleanupCtx, `SELECT instance_marker FROM `+backend.registryTable("planner_registry_meta")+` WHERE singleton=1`).Scan(&marker); err == nil && marker == backendConfig.RegistryInstanceMarker {
			_, _ = backend.db.ExecContext(cleanupCtx, "DROP DATABASE `"+backendConfig.RegistryDatabase+"`")
		}
	}()

	register := func(label string) (enrollRequest, enrollResponse, WorkspaceAccess) {
		t.Helper()
		token, err := backend.IssueToken(ctx, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		device := sha256.Sum256([]byte(label))
		request := enrollRequest{
			SchemaVersion: 1, EnrollmentToken: token,
			DeviceDigest: fmt.Sprintf("sha256:%x", device), DisplayName: label + "\\planner",
		}
		firstResponse, err := service.enroll(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		record, err := backend.GetWorkspace(ctx, firstResponse.WorkspaceID)
		if err != nil {
			t.Fatal(err)
		}
		access, err := service.workspaceAccess(record)
		if err != nil {
			t.Fatal(err)
		}
		workspaces = append(workspaces, access)
		response := firstResponse
		if response.State != StateReady {
			response = pollServiceReady(t, ctx, service, request)
		}
		return request, response, access
	}

	requestA, responseA, accessA := register("MYSQL-E2E-A")
	_, _, accessB := register("MYSQL-E2E-B")
	if len(responseA.Databases) != len(workspacedb.CanonicalMigrationSets()) || responseA.Credential == nil {
		t.Fatalf("fresh migration response=%+v", responseA)
	}
	assertRuntimeCanUseOwnDatabase(t, ctx, endpoint, caPEM, accessA,
		accessA.Databases["pandora_account"])
	assertRuntimeCannotUseOtherWorkspace(t, ctx, endpoint, caPEM, accessA,
		accessB.Databases["pandora_account"])
	assertMigrationDropIsWorkspaceScoped(t, ctx, backend.db, endpoint, caPEM, accessA,
		accessB.Databases["pandora_account"])

	// inactive 只报告不打红；过期 token hash 是可见门禁，下次签发会显式清理。
	if _, err := backend.db.ExecContext(ctx, "UPDATE "+backend.registryTable("planner_workspaces")+" SET last_seen_at=UTC_TIMESTAMP(6)-INTERVAL 40 DAY WHERE workspace_id=?", accessB.WorkspaceID); err != nil {
		t.Fatal(err)
	}
	report, err := backend.Audit(ctx, AuditOptions{StuckAfter: time.Minute, InactiveAfter: 30 * 24 * time.Hour, EntryLimit: 100})
	if err != nil || report.Counts.InactiveWorkspaces == 0 || report.HasBlockingFindings() {
		t.Fatalf("inactive-only audit=%+v err=%v", report.Counts, err)
	}
	if _, err := backend.db.ExecContext(ctx, "UPDATE "+backend.registryTable("planner_workspaces")+" SET last_seen_at=UTC_TIMESTAMP(6) WHERE workspace_id=?", accessB.WorkspaceID); err != nil {
		t.Fatal(err)
	}
	expiredHash := sha256.Sum256([]byte("isolated-expired-token"))
	if _, err := backend.db.ExecContext(ctx, `INSERT INTO `+backend.registryTable("planner_enrollment_tokens")+`
(token_hash,purpose,target_workspace_id,consumed_device_digest,expires_at,consumed_at,created_at)
VALUES (?,'ENROLL',NULL,NULL,UTC_TIMESTAMP(6)-INTERVAL 1 HOUR,NULL,UTC_TIMESTAMP(6)-INTERVAL 2 HOUR)`, expiredHash[:]); err != nil {
		t.Fatal(err)
	}
	report, err = backend.Audit(ctx, AuditOptions{StuckAfter: time.Minute, InactiveAfter: 30 * 24 * time.Hour, EntryLimit: 100})
	if err != nil || report.Counts.ExpiredUnusedTokenHashes == 0 || !report.HasBlockingFindings() {
		t.Fatalf("expired-token audit=%+v err=%v", report.Counts, err)
	}
	_, err = backend.IssueToken(ctx, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	report, err = backend.Audit(ctx, AuditOptions{StuckAfter: time.Minute, InactiveAfter: 30 * 24 * time.Hour, EntryLimit: 100})
	if err != nil || report.Counts.ExpiredUnusedTokenHashes != 0 {
		t.Fatalf("expired token cleanup audit=%+v err=%v", report.Counts, err)
	}

	// 更具体 Host 行会改变 MySQL 账号匹配优先级；发现时必须 fail-closed，不自动删。
	if _, err := backend.db.ExecContext(ctx, "CREATE USER '"+accessA.AppUsername+"'@'localhost' IDENTIFIED BY 'AmbiguousTestPassword_123' REQUIRE SSL"); err != nil {
		t.Fatal(err)
	}
	if err := backend.verifyUserPrivileges(ctx, accessA.AppUsername, expectedRuntimePrivileges(accessA.Databases)); err == nil {
		t.Fatal("ambiguous localhost account was not rejected")
	}
	if _, err := backend.db.ExecContext(ctx, "DROP USER '"+accessA.AppUsername+"'@'localhost'"); err != nil {
		t.Fatal(err)
	}

	// 新普通 token 不能认领旧 digest，且拒绝后 token 仍未消费。
	conflictToken, err := backend.IssueToken(ctx, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	conflictHash := sha256.Sum256([]byte(conflictToken))
	conflictRequest := requestA
	conflictRequest.EnrollmentToken = conflictToken
	if _, err := service.enroll(ctx, conflictRequest); !errors.Is(err, ErrDeviceAlreadyEnrolled) {
		t.Fatalf("ordinary token identity conflict error=%v", err)
	}
	var consumed []byte
	if err := backend.db.QueryRowContext(ctx, "SELECT consumed_device_digest FROM "+backend.registryTable("planner_enrollment_tokens")+" WHERE token_hash=?", conflictHash[:]).Scan(&consumed); err != nil {
		t.Fatal(err)
	}
	if len(consumed) != 0 {
		t.Fatalf("rejected ordinary token consumed=%x", consumed)
	}

	// recovery token 同时绑定 target workspace + 旧 device digest + 客户端 expected ID。
	recoveryToken, err := backend.IssueRecoveryToken(ctx, accessA.WorkspaceID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	recoveryRequest := requestA
	recoveryRequest.EnrollmentToken = recoveryToken
	recoveryRequest.ExpectedWorkspaceID = "02arz3ndektsv4rrffq69g5fav"
	if _, err := service.enroll(ctx, recoveryRequest); !errors.Is(err, ErrInvalidEnrollmentToken) {
		t.Fatalf("wrong expected workspace error=%v", err)
	}
	recoveryRequest.ExpectedWorkspaceID = accessA.WorkspaceID
	wrongDevice := sha256.Sum256([]byte("wrong-recovery-device"))
	recoveryRequest.DeviceDigest = fmt.Sprintf("sha256:%x", wrongDevice)
	if _, err := service.enroll(ctx, recoveryRequest); !errors.Is(err, ErrInvalidEnrollmentToken) {
		t.Fatalf("wrong recovery digest error=%v", err)
	}
	recoveryRequest.DeviceDigest = requestA.DeviceDigest
	recovered, err := service.enroll(ctx, recoveryRequest)
	if err != nil || recovered.WorkspaceID != accessA.WorkspaceID || recovered.Credential == nil ||
		recovered.Credential.Password != responseA.Credential.Password {
		t.Fatalf("recovery response=%+v err=%v", recovered, err)
	}

	// MIGRATION_FAILED 本身不会自动重试；管理 CAS 并发时恰好一个成功。
	if err := backend.TransitionState(ctx, accessA.WorkspaceID, []State{StateReady}, StateMigrationFailed, "INTEGRATION_TEST"); err != nil {
		t.Fatal(err)
	}
	terminal, err := service.enroll(ctx, requestA)
	if err != nil || terminal.State != StateMigrationFailed || terminal.RetryAfterMS != 0 || terminal.Credential != nil {
		t.Fatalf("terminal response=%+v err=%v", terminal, err)
	}
	time.Sleep(100 * time.Millisecond)
	stillFailed, err := backend.GetWorkspace(ctx, accessA.WorkspaceID)
	if err != nil || stillFailed.State != StateMigrationFailed {
		t.Fatalf("MIGRATION_FAILED auto-retried: state=%s err=%v", stillFailed.State, err)
	}
	var retrySuccess atomic.Int32
	var retryWait sync.WaitGroup
	for range 16 {
		retryWait.Add(1)
		go func() {
			defer retryWait.Done()
			_, retryErr := backend.RetryWorkspace(ctx, accessA.WorkspaceID)
			if retryErr == nil {
				retrySuccess.Add(1)
				return
			}
			if !errors.Is(retryErr, ErrWorkspaceNotFailed) {
				t.Errorf("retry CAS error=%v", retryErr)
			}
		}()
	}
	retryWait.Wait()
	if retrySuccess.Load() != 1 {
		t.Fatalf("retry CAS successes=%d", retrySuccess.Load())
	}
	if _, err := backend.RetryWorkspace(ctx, accessA.WorkspaceID); !errors.Is(err, ErrWorkspaceNotFailed) {
		t.Fatalf("non-failed retry error=%v", err)
	}
	readyAgain := pollServiceReady(t, ctx, service, requestA)
	if readyAgain.WorkspaceID != accessA.WorkspaceID || readyAgain.Credential == nil ||
		readyAgain.Credential.Password != responseA.Credential.Password {
		t.Fatalf("retry changed workspace credential: %+v", readyAgain)
	}
}

func assertKilledWorkspaceLockCancelsCallback(t *testing.T, ctx context.Context, owner *MySQLBackend, backendConfig BackendConfig) {
	t.Helper()
	killer, err := OpenMySQLBackend(ctx, backendConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer killer.Close()
	workspaceID := "01arz3ndektsv4rrffq69g5fav"
	lockName := "pandora_planner:" + workspaceID
	callbackStarted := make(chan struct{})
	callbackCanceled := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, lockErr := owner.WithWorkspaceLock(ctx, workspaceID, time.Second, func(lockCtx context.Context) error {
			close(callbackStarted)
			<-lockCtx.Done()
			close(callbackCanceled)
			return lockCtx.Err()
		})
		result <- lockErr
	}()
	select {
	case <-callbackStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("lock callback did not start")
	}
	var connectionID sql.NullInt64
	if err := killer.db.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", lockName).Scan(&connectionID); err != nil || !connectionID.Valid {
		t.Fatalf("lock owner connection id=%v err=%v", connectionID, err)
	}
	if _, err := killer.db.ExecContext(ctx, "KILL CONNECTION "+strconv.FormatInt(connectionID.Int64, 10)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-callbackCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("killed lock connection did not cancel callback")
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrWorkspaceLockLost) {
			t.Fatalf("killed lock error=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("killed lock owner did not return")
	}
	locked, err := killer.WithWorkspaceLock(ctx, workspaceID, time.Second, func(context.Context) error { return nil })
	if err != nil || !locked {
		t.Fatalf("second backend could not acquire released lock: locked=%v err=%v", locked, err)
	}
}

func pollServiceReady(t *testing.T, ctx context.Context, service *service, request enrollRequest) enrollResponse {
	t.Helper()
	deadline := time.Now().Add(4 * time.Minute)
	for time.Now().Before(deadline) {
		response, err := service.enroll(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		switch response.State {
		case StateReady:
			return response
		case StateMigrationFailed:
			t.Fatalf("fresh migration failed: workspace=%s", response.WorkspaceID)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("workspace did not become READY")
	return enrollResponse{}
}

func openRuntimeDatabase(t *testing.T, endpoint Endpoint, caPEM []byte, access WorkspaceAccess, database string) *sql.DB {
	t.Helper()
	config := mysql.NewConfig()
	config.User = access.AppUsername
	config.Passwd = access.AppPassword
	config.Net = "tcp"
	config.Addr = fmt.Sprintf("%s:%d", endpoint.Host, endpoint.Port)
	config.DBName = database
	config.TLSConfig = "true"
	config.ParseTime = true
	if err := registerAdminTLS(config, endpoint, caPEM); err != nil {
		t.Fatal(err)
	}
	databaseHandle, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	return databaseHandle
}

func assertRuntimeCanUseOwnDatabase(t *testing.T, ctx context.Context, endpoint Endpoint, caPEM []byte, access WorkspaceAccess, database string) {
	t.Helper()
	handle := openRuntimeDatabase(t, endpoint, caPEM, access, database)
	defer handle.Close()
	var selected string
	if err := handle.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&selected); err != nil {
		t.Fatal(err)
	}
	var statusName, cipher string
	if err := handle.QueryRowContext(ctx, "SHOW SESSION STATUS LIKE 'Ssl_cipher'").Scan(&statusName, &cipher); err != nil {
		t.Fatal(err)
	}
	if selected != database || cipher == "" {
		t.Fatalf("runtime own DB/TLS=%q/%q", selected, cipher)
	}
}

func assertRuntimeCannotUseOtherWorkspace(t *testing.T, ctx context.Context, endpoint Endpoint, caPEM []byte, access WorkspaceAccess, otherDatabase string) {
	t.Helper()
	handle := openRuntimeDatabase(t, endpoint, caPEM, access, otherDatabase)
	defer handle.Close()
	if err := handle.PingContext(ctx); err == nil {
		t.Fatalf("runtime account %s connected to other workspace DB %s", access.AppUsername, otherDatabase)
	}
}

func assertMigrationDropIsWorkspaceScoped(t *testing.T, ctx context.Context, admin *sql.DB, endpoint Endpoint, caPEM []byte, access WorkspaceAccess, otherDatabase string) {
	t.Helper()
	rows, err := admin.QueryContext(ctx, `SELECT TABLE_SCHEMA FROM information_schema.SCHEMA_PRIVILEGES
WHERE GRANTEE=? AND PRIVILEGE_TYPE='DROP' ORDER BY TABLE_SCHEMA`, mysqlAccount(access.MigrationUsername))
	if err != nil {
		t.Fatal(err)
	}
	granted := make(map[string]bool)
	for rows.Next() {
		var database string
		if err := rows.Scan(&database); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		granted[database] = true
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	_ = rows.Close()
	if len(granted) != len(access.Databases) {
		t.Fatalf("migration DROP database grants=%v", granted)
	}
	for _, database := range access.Databases {
		if !granted[database] {
			t.Fatalf("migration account lacks database-level DROP on own DB %s", database)
		}
	}
	if granted[otherDatabase] {
		t.Fatalf("migration account has DROP on other workspace DB %s", otherDatabase)
	}

	config := mysql.NewConfig()
	config.User = access.MigrationUsername
	config.Passwd = access.MigrationPassword
	config.Net = "tcp"
	config.Addr = fmt.Sprintf("%s:%d", endpoint.Host, endpoint.Port)
	config.TLSConfig = "true"
	if err := registerAdminTLS(config, endpoint, caPEM); err != nil {
		t.Fatal(err)
	}
	handle, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	if err := handle.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.ExecContext(ctx, "DROP DATABASE `"+otherDatabase+"`"); err == nil {
		t.Fatalf("migration account dropped other workspace DB %s", otherDatabase)
	}
	var stillExists int
	if err := admin.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=?`, otherDatabase).Scan(&stillExists); err != nil || stillExists != 1 {
		t.Fatalf("other workspace DB existence=%d err=%v", stillExists, err)
	}
}
