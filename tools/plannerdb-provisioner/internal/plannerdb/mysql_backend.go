package plannerdb

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/luyuancpp/pandora/tools/migrate/workspacedb"
)

var (
	credentialUsernamePattern = regexp.MustCompile(`^p_(?:app|mig)_[0-9a-hjkmnp-tv-z]{26}$`)
	databasePasswordPattern   = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)
	failureCodePattern        = regexp.MustCompile(`^[A-Z0-9_]{0,64}$`)
)

type MySQLBackend struct {
	db                  *sql.DB
	endpoint            Endpoint
	registryDatabase    string
	registryMarker      string
	lockMonitorInterval time.Duration
}

type registryIndexExpectation struct {
	table, index, columns string
	nonUnique             int64
}

func requiredRegistryIndexes() []registryIndexExpectation {
	return []registryIndexExpectation{
		{"planner_registry_meta", "PRIMARY", "singleton", 0},
		{"planner_workspaces", "PRIMARY", "workspace_id", 0},
		{"planner_workspaces", "uk_planner_workspaces_device", "device_digest", 0},
		{"planner_workspaces", "uk_planner_workspaces_app_user", "app_username", 0},
		{"planner_workspaces", "uk_planner_workspaces_migration_user", "migration_username", 0},
		{"planner_workspaces", "idx_planner_workspaces_state_updated", "state,updated_at", 1},
		{"planner_workspaces", "idx_planner_workspaces_last_seen", "last_seen_at", 1},
		{"planner_enrollment_tokens", "PRIMARY", "token_hash", 0},
		{"planner_enrollment_tokens", "idx_planner_enrollment_tokens_expiry", "consumed_at,expires_at", 1},
		{"planner_enrollment_tokens", "idx_planner_enrollment_tokens_target", "target_workspace_id", 1},
	}
}

// OpenMySQLBackend 只打开并严格验收已存在的 registry，绝不执行 DDL。
func OpenMySQLBackend(ctx context.Context, backendConfig BackendConfig) (*MySQLBackend, error) {
	backend, err := openAdminBackend(ctx, backendConfig)
	if err != nil {
		return nil, err
	}
	if err := backend.verifyRegistryMarker(ctx); err != nil {
		_ = backend.Close()
		return nil, err
	}
	if err := backend.verifyRegistryShape(ctx); err != nil {
		_ = backend.Close()
		return nil, err
	}
	return backend, nil
}

// InitMySQLRegistry 是首次部署唯一允许创建 registry 的显式管理入口。
func InitMySQLRegistry(ctx context.Context, backendConfig BackendConfig) (*MySQLBackend, error) {
	backend, err := openAdminBackend(ctx, backendConfig)
	if err != nil {
		return nil, err
	}
	if err := backend.initializeRegistry(ctx); err != nil {
		_ = backend.Close()
		return nil, err
	}
	return backend, nil
}

func openAdminBackend(ctx context.Context, backendConfig BackendConfig) (*MySQLBackend, error) {
	if err := validateRegistryIdentity(backendConfig); err != nil {
		return nil, err
	}
	config, err := parseAdminDSN(backendConfig.AdminDSN, backendConfig.Endpoint)
	if err != nil {
		return nil, err
	}
	if config.Params == nil {
		config.Params = make(map[string]string)
	}
	config.Params["time_zone"] = "'+00:00'"
	config.Loc = time.UTC
	if err := registerAdminTLS(config, backendConfig.Endpoint, backendConfig.MySQLCAPEM); err != nil {
		return nil, err
	}
	database, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil, errors.New("打开 admin MySQL 连接失败")
	}
	database.SetMaxOpenConns(8)
	database.SetMaxIdleConns(4)
	database.SetConnMaxLifetime(10 * time.Minute)
	monitorInterval := backendConfig.LockMonitorInterval
	if monitorInterval <= 0 {
		monitorInterval = time.Second
	}
	if monitorInterval < 10*time.Millisecond || monitorInterval > 30*time.Second {
		_ = database.Close()
		return nil, errors.New("workspace lock monitor interval 必须在 10ms..30s")
	}
	backend := &MySQLBackend{
		db: database, endpoint: backendConfig.Endpoint,
		registryDatabase: backendConfig.RegistryDatabase, registryMarker: backendConfig.RegistryInstanceMarker,
		lockMonitorInterval: monitorInterval,
	}
	if err := backend.preflight(ctx); err != nil {
		_ = database.Close()
		return nil, err
	}
	return backend, nil
}

func (b *MySQLBackend) registryTable(table string) string {
	// registryDatabase 来自严格正则；table 仅由模块内常量调用。
	return "`" + b.registryDatabase + "`.`" + table + "`"
}

func (b *MySQLBackend) Close() error {
	if b == nil || b.db == nil {
		return nil
	}
	return b.db.Close()
}

func (b *MySQLBackend) preflight(ctx context.Context) error {
	checkCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := b.db.PingContext(checkCtx); err != nil {
		return errors.New("admin MySQL TLS 连接失败")
	}
	var statusName, cipherName string
	if err := b.db.QueryRowContext(checkCtx, "SHOW SESSION STATUS LIKE 'Ssl_cipher'").Scan(&statusName, &cipherName); err != nil {
		return errors.New("读取 admin MySQL TLS 状态失败")
	}
	if !strings.EqualFold(statusName, "Ssl_cipher") || strings.TrimSpace(cipherName) == "" {
		return errors.New("admin MySQL session 未使用 TLS cipher")
	}
	var sqlMode, version string
	if err := b.db.QueryRowContext(checkCtx, "SELECT @@SESSION.sql_mode, VERSION()").Scan(&sqlMode, &version); err != nil {
		return errors.New("读取 admin MySQL 运行参数失败")
	}
	if !hasStrictSQLMode(sqlMode) {
		return errors.New("admin MySQL session 未启用 STRICT_TRANS_TABLES/STRICT_ALL_TABLES")
	}
	lowerVersion := strings.ToLower(version)
	majorText := strings.SplitN(version, ".", 2)[0]
	major, parseErr := strconv.Atoi(majorText)
	if parseErr != nil || major < 8 || strings.Contains(lowerVersion, "mariadb") || strings.Contains(lowerVersion, "tidb") {
		return fmt.Errorf("中心 provisioner 只支持 Oracle MySQL 8+，当前版本标识=%q", version)
	}
	return nil
}

func hasStrictSQLMode(sqlMode string) bool {
	for _, value := range strings.Split(sqlMode, ",") {
		switch strings.ToUpper(strings.TrimSpace(value)) {
		case "STRICT_TRANS_TABLES", "STRICT_ALL_TABLES":
			return true
		}
	}
	return false
}

func (b *MySQLBackend) initializeRegistry(ctx context.Context) error {
	statements := []string{
		"CREATE DATABASE IF NOT EXISTS `" + b.registryDatabase + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci",
		`CREATE TABLE IF NOT EXISTS ` + b.registryTable("planner_registry_meta") + ` (
  singleton TINYINT UNSIGNED NOT NULL,
  instance_marker VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  schema_version INT UNSIGNED NOT NULL,
  created_at DATETIME(6) NOT NULL,
  PRIMARY KEY (singleton),
  CONSTRAINT chk_planner_registry_meta_singleton CHECK (singleton = 1),
  CONSTRAINT chk_planner_registry_meta_schema_version CHECK (schema_version = 1)
) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS ` + b.registryTable("planner_workspaces") + ` (
  workspace_id CHAR(26) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  device_digest BINARY(32) NOT NULL,
  display_name VARCHAR(255) NOT NULL,
  state ENUM('PROVISIONING','MIGRATING','READY','MIGRATION_FAILED') NOT NULL,
  app_username VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  app_secret_nonce BINARY(12) NOT NULL,
  app_secret_cipher VARBINARY(128) NOT NULL,
  migration_username VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  migration_secret_nonce BINARY(12) NOT NULL,
  migration_secret_cipher VARBINARY(128) NOT NULL,
  credential_version BIGINT UNSIGNED NOT NULL,
  failure_code VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
  operation_epoch BIGINT UNSIGNED NOT NULL DEFAULT 0,
  attempt_status ENUM('IDLE','RUNNING','SUCCEEDED','FAILED') NOT NULL DEFAULT 'IDLE',
  last_seen_at DATETIME(6) NOT NULL,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  PRIMARY KEY (workspace_id),
  UNIQUE KEY uk_planner_workspaces_device (device_digest),
  UNIQUE KEY uk_planner_workspaces_app_user (app_username),
  UNIQUE KEY uk_planner_workspaces_migration_user (migration_username),
  KEY idx_planner_workspaces_state_updated (state, updated_at),
  KEY idx_planner_workspaces_last_seen (last_seen_at),
  CONSTRAINT chk_planner_workspaces_credential_version CHECK (credential_version > 0),
  CONSTRAINT chk_planner_workspaces_attempt_state CHECK (
    (state='PROVISIONING' AND attempt_status IN ('IDLE','RUNNING')) OR
    (state='MIGRATING' AND attempt_status='RUNNING') OR
    (state='READY' AND attempt_status='SUCCEEDED') OR
    (state='MIGRATION_FAILED' AND attempt_status='FAILED'))
) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS ` + b.registryTable("planner_enrollment_tokens") + ` (
  token_hash BINARY(32) NOT NULL,
  purpose ENUM('ENROLL','RECOVERY') NOT NULL,
  target_workspace_id CHAR(26) CHARACTER SET ascii COLLATE ascii_bin NULL,
  consumed_device_digest BINARY(32) NULL,
  expires_at DATETIME(6) NOT NULL,
  consumed_at DATETIME(6) NULL,
  created_at DATETIME(6) NOT NULL,
  PRIMARY KEY (token_hash),
  KEY idx_planner_enrollment_tokens_expiry (consumed_at, expires_at),
  KEY idx_planner_enrollment_tokens_target (target_workspace_id),
  CONSTRAINT fk_planner_enrollment_tokens_workspace FOREIGN KEY (target_workspace_id)
    REFERENCES ` + b.registryTable("planner_workspaces") + ` (workspace_id),
  CONSTRAINT chk_planner_enrollment_tokens_purpose_target CHECK (
    (purpose='ENROLL' AND target_workspace_id IS NULL) OR
    (purpose='RECOVERY' AND target_workspace_id IS NOT NULL))
) ENGINE=InnoDB`,
	}
	for _, statement := range statements {
		execCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, err := b.db.ExecContext(execCtx, statement)
		cancel()
		if err != nil {
			return errors.New("初始化 planner registry schema 失败")
		}
	}
	if _, err := b.db.ExecContext(ctx, `INSERT IGNORE INTO `+b.registryTable("planner_registry_meta")+`
(singleton, instance_marker, schema_version, created_at) VALUES (1, ?, 1, UTC_TIMESTAMP(6))`, b.registryMarker); err != nil {
		return errors.New("写入 planner registry instance marker 失败")
	}
	if err := b.verifyRegistryMarker(ctx); err != nil {
		return err
	}
	return b.verifyRegistryShape(ctx)
}

func (b *MySQLBackend) verifyRegistryMarker(ctx context.Context) error {
	var marker string
	var version uint64
	err := b.db.QueryRowContext(ctx, `SELECT instance_marker, schema_version FROM `+
		b.registryTable("planner_registry_meta")+` WHERE singleton=1`).Scan(&marker, &version)
	if err != nil || marker != b.registryMarker || version != schemaVersion {
		return errors.New("planner registry 不存在、instance marker 不匹配或 schema version 非法；首次部署必须显式 init-registry")
	}
	return nil
}

func (b *MySQLBackend) verifyRegistryShape(ctx context.Context) error {
	type columnShape struct {
		columnType string
		nullable   string
	}
	expectedColumns := map[string]map[string]columnShape{
		"planner_registry_meta": {
			"singleton":       {"tinyint unsigned", "NO"},
			"instance_marker": {"varchar(128)", "NO"},
			"schema_version":  {"int unsigned", "NO"},
			"created_at":      {"datetime(6)", "NO"},
		},
		"planner_workspaces": {
			"workspace_id": {"char(26)", "NO"}, "device_digest": {"binary(32)", "NO"},
			"display_name": {"varchar(255)", "NO"},
			"state":        {"enum('PROVISIONING','MIGRATING','READY','MIGRATION_FAILED')", "NO"},
			"app_username": {"varchar(32)", "NO"}, "app_secret_nonce": {"binary(12)", "NO"},
			"app_secret_cipher": {"varbinary(128)", "NO"}, "migration_username": {"varchar(32)", "NO"},
			"migration_secret_nonce": {"binary(12)", "NO"}, "migration_secret_cipher": {"varbinary(128)", "NO"},
			"credential_version": {"bigint unsigned", "NO"}, "failure_code": {"varchar(64)", "NO"},
			"operation_epoch": {"bigint unsigned", "NO"}, "attempt_status": {"enum('IDLE','RUNNING','SUCCEEDED','FAILED')", "NO"},
			"last_seen_at": {"datetime(6)", "NO"}, "created_at": {"datetime(6)", "NO"},
			"updated_at": {"datetime(6)", "NO"},
		},
		"planner_enrollment_tokens": {
			"token_hash": {"binary(32)", "NO"}, "purpose": {"enum('ENROLL','RECOVERY')", "NO"},
			"target_workspace_id": {"char(26)", "YES"}, "consumed_device_digest": {"binary(32)", "YES"},
			"expires_at": {"datetime(6)", "NO"}, "consumed_at": {"datetime(6)", "YES"},
			"created_at": {"datetime(6)", "NO"},
		},
	}
	for table, expected := range expectedColumns {
		var engine string
		if err := b.db.QueryRowContext(ctx, `SELECT ENGINE FROM information_schema.TABLES
WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`, b.registryDatabase, table).Scan(&engine); err != nil || !strings.EqualFold(engine, "InnoDB") {
			return fmt.Errorf("planner registry 表 %s 不是 InnoDB", table)
		}
		rows, err := b.db.QueryContext(ctx, `SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE
FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`, b.registryDatabase, table)
		if err != nil {
			return fmt.Errorf("读取 planner registry 表 %s 形态失败", table)
		}
		actual := make(map[string]columnShape)
		for rows.Next() {
			var name string
			var shape columnShape
			if err := rows.Scan(&name, &shape.columnType, &shape.nullable); err != nil {
				_ = rows.Close()
				return fmt.Errorf("解析 planner registry 表 %s 形态失败", table)
			}
			actual[name] = shape
		}
		rowsErr := rows.Err()
		_ = rows.Close()
		if rowsErr != nil || len(actual) != len(expected) {
			return fmt.Errorf("planner registry 表 %s 列集合异常", table)
		}
		for name, want := range expected {
			got, ok := actual[name]
			if !ok || !strings.EqualFold(got.columnType, want.columnType) || got.nullable != want.nullable {
				return fmt.Errorf("planner registry 表 %s 列 %s 形态异常", table, name)
			}
		}
	}
	for _, want := range requiredRegistryIndexes() {
		var columns sql.NullString
		var nonUnique sql.NullInt64
		err := b.db.QueryRowContext(ctx, `SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX), MAX(NON_UNIQUE)
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME=?`, b.registryDatabase, want.table, want.index).Scan(&columns, &nonUnique)
		if err != nil || !columns.Valid || columns.String != want.columns || !nonUnique.Valid || nonUnique.Int64 != want.nonUnique {
			return fmt.Errorf("planner registry 索引 %s.%s 形态异常", want.table, want.index)
		}
	}
	for _, constraint := range []struct {
		table, name, constraintType string
	}{
		{"planner_registry_meta", "chk_planner_registry_meta_singleton", "CHECK"},
		{"planner_registry_meta", "chk_planner_registry_meta_schema_version", "CHECK"},
		{"planner_workspaces", "chk_planner_workspaces_credential_version", "CHECK"},
		{"planner_workspaces", "chk_planner_workspaces_attempt_state", "CHECK"},
		{"planner_enrollment_tokens", "chk_planner_enrollment_tokens_purpose_target", "CHECK"},
		{"planner_enrollment_tokens", "fk_planner_enrollment_tokens_workspace", "FOREIGN KEY"},
	} {
		var constraintType, enforced string
		err := b.db.QueryRowContext(ctx, `SELECT CONSTRAINT_TYPE, ENFORCED
FROM information_schema.TABLE_CONSTRAINTS
WHERE CONSTRAINT_SCHEMA=? AND TABLE_NAME=? AND CONSTRAINT_NAME=?`,
			b.registryDatabase, constraint.table, constraint.name).Scan(&constraintType, &enforced)
		if err != nil || constraintType != constraint.constraintType || enforced != "YES" {
			return fmt.Errorf("planner registry 约束 %s.%s 形态异常", constraint.table, constraint.name)
		}
	}
	var referencedSchema, referencedTable, referencedColumn string
	if err := b.db.QueryRowContext(ctx, `SELECT REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
FROM information_schema.KEY_COLUMN_USAGE
WHERE CONSTRAINT_SCHEMA=? AND TABLE_NAME='planner_enrollment_tokens'
  AND CONSTRAINT_NAME='fk_planner_enrollment_tokens_workspace' AND COLUMN_NAME='target_workspace_id'`,
		b.registryDatabase).Scan(&referencedSchema, &referencedTable, &referencedColumn); err != nil ||
		referencedSchema != b.registryDatabase || referencedTable != "planner_workspaces" || referencedColumn != "workspace_id" {
		return errors.New("planner registry recovery token workspace 外键形态异常")
	}
	return nil
}

func (b *MySQLBackend) IssueTokenGrant(ctx context.Context, grant TokenGrant) error {
	if !grant.ExpiresAt.After(time.Now().UTC()) {
		return errors.New("enrollment token 过期时间必须晚于当前时间")
	}
	var target any
	switch grant.Purpose {
	case TokenPurposeEnroll:
		if grant.TargetWorkspaceID != "" {
			return errors.New("ENROLL token 禁止绑定 target workspace")
		}
		target = nil
	case TokenPurposeRecovery:
		if err := workspacedb.ValidateWorkspaceID(grant.TargetWorkspaceID); err != nil {
			return errors.New("RECOVERY token target workspace 非法")
		}
		target = grant.TargetWorkspaceID
	default:
		return errors.New("enrollment token purpose 非法")
	}
	tx, err := b.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return errors.New("开始 token 签发事务失败")
	}
	defer func() { _ = tx.Rollback() }()
	// token 是业务性短期凭据：未消费过期 token 立即物理清理；已消费 hash 保留 30 天，
	// 保障响应丢失重试窗口后再清理。这里复用管理命令调用，不新建后台 timer。
	if _, err := tx.ExecContext(ctx, `DELETE FROM `+b.registryTable("planner_enrollment_tokens")+`
WHERE (consumed_at IS NULL AND expires_at < UTC_TIMESTAMP(6))
	   OR (consumed_at IS NOT NULL AND consumed_at < UTC_TIMESTAMP(6) - INTERVAL 30 DAY)`); err != nil {
		return errors.New("清理过期 enrollment token hash 失败")
	}
	if grant.Purpose == TokenPurposeRecovery {
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM `+b.registryTable("planner_workspaces")+`
WHERE workspace_id=? FOR SHARE`, grant.TargetWorkspaceID).Scan(&exists); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return errors.New("RECOVERY token target workspace 不存在")
			}
			return errors.New("验证 RECOVERY token target workspace 失败")
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO `+b.registryTable("planner_enrollment_tokens")+`
(token_hash, purpose, target_workspace_id, consumed_device_digest, expires_at, consumed_at, created_at)
VALUES (?, ?, ?, NULL, ?, NULL, UTC_TIMESTAMP(6))`,
		grant.TokenHash[:], grant.Purpose, target, grant.ExpiresAt.UTC())
	if err != nil {
		return errors.New("写入 enrollment token hash 失败")
	}
	if err := tx.Commit(); err != nil {
		return errors.New("提交 token 签发事务失败")
	}
	return nil
}

func (b *MySQLBackend) ClaimEnrollment(ctx context.Context, claim EnrollmentClaim) (WorkspaceRecord, error) {
	if err := validateCandidate(claim); err != nil {
		return WorkspaceRecord{}, err
	}
	tx, err := b.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return WorkspaceRecord{}, errors.New("开始 enrollment 事务失败")
	}
	defer func() { _ = tx.Rollback() }()

	var consumedDigest []byte
	var unexpired bool
	var purpose TokenPurpose
	var targetWorkspace sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT consumed_device_digest, expires_at > UTC_TIMESTAMP(6), purpose, target_workspace_id
FROM `+b.registryTable("planner_enrollment_tokens")+`
WHERE token_hash=? FOR UPDATE`, claim.TokenHash[:]).Scan(&consumedDigest, &unexpired, &purpose, &targetWorkspace)
	if errors.Is(err, sql.ErrNoRows) {
		return WorkspaceRecord{}, ErrInvalidEnrollmentToken
	}
	if err != nil {
		return WorkspaceRecord{}, errors.New("锁定 enrollment token 失败")
	}
	if len(consumedDigest) != 0 && !bytes.Equal(consumedDigest, claim.DeviceDigest[:]) {
		return WorkspaceRecord{}, ErrInvalidEnrollmentToken
	}
	if len(consumedDigest) == 0 && !unexpired {
		return WorkspaceRecord{}, ErrInvalidEnrollmentToken
	}

	var record WorkspaceRecord
	switch purpose {
	case TokenPurposeRecovery:
		if !targetWorkspace.Valid || claim.ExpectedWorkspaceID == "" || claim.ExpectedWorkspaceID != targetWorkspace.String {
			return WorkspaceRecord{}, ErrInvalidEnrollmentToken
		}
		record, err = scanWorkspace(tx.QueryRowContext(ctx,
			b.workspaceSelect()+" WHERE workspace_id=? FOR UPDATE", targetWorkspace.String))
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceRecord{}, ErrInvalidEnrollmentToken
		}
		if err != nil {
			return WorkspaceRecord{}, errors.New("读取 RECOVERY token target workspace 失败")
		}
		if record.DeviceDigest != claim.DeviceDigest {
			return WorkspaceRecord{}, ErrInvalidEnrollmentToken
		}
	case TokenPurposeEnroll:
		if targetWorkspace.Valid {
			return WorkspaceRecord{}, errors.New("ENROLL token 含非法 target workspace")
		}
		if len(consumedDigest) == 0 && claim.ExpectedWorkspaceID != "" {
			return WorkspaceRecord{}, ErrInvalidEnrollmentToken
		}
		if len(consumedDigest) == 0 {
			candidate := claim.Candidate
			_, err = tx.ExecContext(ctx, `INSERT INTO `+b.registryTable("planner_workspaces")+`
(workspace_id, device_digest, display_name, state,
 app_username, app_secret_nonce, app_secret_cipher,
 migration_username, migration_secret_nonce, migration_secret_cipher,
 credential_version, failure_code, operation_epoch, attempt_status, last_seen_at, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', 0, 'IDLE', UTC_TIMESTAMP(6), UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
`,
				candidate.WorkspaceID, candidate.DeviceDigest[:], claim.DisplayName, candidate.State,
				candidate.AppUsername, candidate.AppSecret.Nonce, candidate.AppSecret.Ciphertext,
				candidate.MigrationUsername, candidate.MigrationSecret.Nonce, candidate.MigrationSecret.Ciphertext,
				candidate.CredentialVersion)
			if err != nil {
				var mysqlErr *mysql.MySQLError
				if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1062 {
					return WorkspaceRecord{}, errors.New("登记 workspace 失败")
				}
				_, existingErr := scanWorkspace(tx.QueryRowContext(ctx, b.workspaceSelect()+" WHERE device_digest=? FOR UPDATE", claim.DeviceDigest[:]))
				if existingErr == nil {
					// 新 token 不能在 identity/DPAPI 丢失或机器克隆后自动领取旧库凭据。
					// 整个事务回滚，token 保持未消费，显式 recovery 由管理员另行处理。
					return WorkspaceRecord{}, ErrDeviceAlreadyEnrolled
				}
				if !errors.Is(existingErr, sql.ErrNoRows) {
					return WorkspaceRecord{}, errors.New("判定 workspace 唯一键冲突失败")
				}
				// workspace_id 及由它派生的两个 username 冲突：上层换新的 CSPRNG ID 重试。
				return WorkspaceRecord{}, ErrWorkspaceCollision
			}
		}

		record, err = scanWorkspace(tx.QueryRowContext(ctx, b.workspaceSelect()+" WHERE device_digest=? FOR UPDATE", claim.DeviceDigest[:]))
		if errors.Is(err, sql.ErrNoRows) {
			if len(consumedDigest) != 0 {
				return WorkspaceRecord{}, errors.New("已消费 enrollment token 对应的 workspace 丢失")
			}
			return WorkspaceRecord{}, ErrWorkspaceCollision
		}
		if err != nil {
			return WorkspaceRecord{}, errors.New("读取 workspace 登记失败")
		}
		if claim.ExpectedWorkspaceID != "" && claim.ExpectedWorkspaceID != record.WorkspaceID {
			return WorkspaceRecord{}, ErrInvalidEnrollmentToken
		}
	default:
		return WorkspaceRecord{}, errors.New("enrollment token purpose 非法")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE `+b.registryTable("planner_enrollment_tokens")+`
SET consumed_device_digest=COALESCE(consumed_device_digest, ?),
    consumed_at=COALESCE(consumed_at, UTC_TIMESTAMP(6))
WHERE token_hash=? AND (consumed_device_digest IS NULL OR consumed_device_digest=?)`,
		claim.DeviceDigest[:], claim.TokenHash[:], claim.DeviceDigest[:]); err != nil {
		return WorkspaceRecord{}, errors.New("消费 enrollment token 失败")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE `+b.registryTable("planner_workspaces")+`
SET display_name=?, last_seen_at=UTC_TIMESTAMP(6), updated_at=updated_at
WHERE workspace_id=?`, claim.DisplayName, record.WorkspaceID); err != nil {
		return WorkspaceRecord{}, errors.New("更新 workspace 诊断标签失败")
	}
	if err := tx.Commit(); err != nil {
		return WorkspaceRecord{}, errors.New("提交 enrollment 事务失败")
	}
	record.DisplayName = claim.DisplayName
	return record, nil
}

const workspaceSelectColumns = `SELECT workspace_id, device_digest, display_name, state,
app_username, app_secret_nonce, app_secret_cipher,
migration_username, migration_secret_nonce, migration_secret_cipher,
credential_version, failure_code, operation_epoch, attempt_status FROM `

func (b *MySQLBackend) workspaceSelect() string {
	return workspaceSelectColumns + b.registryTable("planner_workspaces")
}

type rowScanner interface {
	Scan(...any) error
}

func scanWorkspace(row rowScanner) (WorkspaceRecord, error) {
	var record WorkspaceRecord
	var deviceDigest []byte
	err := row.Scan(
		&record.WorkspaceID, &deviceDigest, &record.DisplayName, &record.State,
		&record.AppUsername, &record.AppSecret.Nonce, &record.AppSecret.Ciphertext,
		&record.MigrationUsername, &record.MigrationSecret.Nonce, &record.MigrationSecret.Ciphertext,
		&record.CredentialVersion, &record.FailureCode, &record.OperationEpoch, &record.AttemptStatus,
	)
	if err != nil {
		return WorkspaceRecord{}, err
	}
	if len(deviceDigest) != 32 {
		return WorkspaceRecord{}, errors.New("registry device_digest 长度异常")
	}
	copy(record.DeviceDigest[:], deviceDigest)
	if !validAttemptStatus(record.AttemptStatus) || record.OperationEpoch == 0 && record.AttemptStatus != AttemptStatusIdle {
		return WorkspaceRecord{}, errors.New("registry workspace attempt fence 形态异常")
	}
	return record, nil
}

func (b *MySQLBackend) GetWorkspace(ctx context.Context, workspaceID string) (WorkspaceRecord, error) {
	if err := workspacedb.ValidateWorkspaceID(workspaceID); err != nil {
		return WorkspaceRecord{}, err
	}
	record, err := scanWorkspace(b.db.QueryRowContext(ctx, b.workspaceSelect()+" WHERE workspace_id=?", workspaceID))
	if err != nil {
		return WorkspaceRecord{}, errors.New("读取 workspace 状态失败")
	}
	return record, nil
}

// RetryWorkspace 是 MIGRATION_FAILED 唯一自动化恢复入口。它只做单条 CAS，
// 不更改 workspace/账号/密文，也不在管理命令进程内启动 DDL。
func (b *MySQLBackend) RetryWorkspace(ctx context.Context, workspaceID string) (WorkspaceRecord, error) {
	if err := workspacedb.ValidateWorkspaceID(workspaceID); err != nil {
		return WorkspaceRecord{}, err
	}
	result, err := b.db.ExecContext(ctx, `UPDATE `+b.registryTable("planner_workspaces")+`
SET state='PROVISIONING', failure_code='', attempt_status='IDLE', updated_at=UTC_TIMESTAMP(6)
WHERE workspace_id=? AND state='MIGRATION_FAILED'`, workspaceID)
	if err != nil {
		return WorkspaceRecord{}, errors.New("重置 MIGRATION_FAILED workspace 失败")
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return WorkspaceRecord{}, errors.New("确认 workspace retry CAS 结果失败")
	}
	if affected != 1 {
		if _, loadErr := b.GetWorkspace(ctx, workspaceID); loadErr != nil {
			return WorkspaceRecord{}, loadErr
		}
		return WorkspaceRecord{}, ErrWorkspaceNotFailed
	}
	return b.GetWorkspace(ctx, workspaceID)
}

func (b *MySQLBackend) BeginProvisionAttempt(ctx context.Context, workspaceID string) (WorkspaceRecord, error) {
	if err := workspacedb.ValidateWorkspaceID(workspaceID); err != nil {
		return WorkspaceRecord{}, err
	}
	result, err := b.db.ExecContext(ctx, `UPDATE `+b.registryTable("planner_workspaces")+`
SET operation_epoch=operation_epoch+1, attempt_status='RUNNING', failure_code='', updated_at=UTC_TIMESTAMP(6)
WHERE workspace_id=? AND state='PROVISIONING' AND attempt_status='IDLE'`, workspaceID)
	if err != nil {
		return WorkspaceRecord{}, errors.New("开始 fenced workspace attempt 失败")
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return WorkspaceRecord{}, errors.New("确认 fenced workspace attempt 失败")
	}
	record, loadErr := b.GetWorkspace(ctx, workspaceID)
	if loadErr != nil {
		return WorkspaceRecord{}, loadErr
	}
	if affected == 1 && record.State == StateProvisioning && record.AttemptStatus == AttemptStatusRunning && record.OperationEpoch != 0 {
		return record, nil
	}
	if record.AttemptStatus == AttemptStatusRunning &&
		(record.State == StateProvisioning || record.State == StateMigrating) {
		return record, ErrAttemptOutcomeUnknown
	}
	return record, errors.New("workspace 不可开始新的 fenced attempt")
}

func (b *MySQLBackend) TransitionAttemptState(
	ctx context.Context,
	workspaceID string,
	operationEpoch uint64,
	from []State,
	to State,
	failureCode string,
	attemptStatus AttemptStatus,
) error {
	if operationEpoch == 0 || len(from) == 0 || !validState(to) || !validAttemptStatus(attemptStatus) ||
		!failureCodePattern.MatchString(failureCode) {
		return errors.New("fenced workspace 状态迁移参数非法")
	}
	placeholders := make([]string, len(from))
	arguments := []any{to, failureCode, attemptStatus, workspaceID, operationEpoch}
	for index, state := range from {
		if !validState(state) {
			return errors.New("fenced workspace 起始状态非法")
		}
		placeholders[index] = "?"
		arguments = append(arguments, state)
	}
	result, err := b.db.ExecContext(ctx, `UPDATE `+b.registryTable("planner_workspaces")+`
SET state=?, failure_code=?, attempt_status=?, updated_at=UTC_TIMESTAMP(6)
WHERE workspace_id=? AND operation_epoch=? AND state IN (`+strings.Join(placeholders, ",")+")", arguments...)
	if err != nil {
		return errors.New("更新 fenced workspace 状态失败")
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return errors.New("确认 fenced workspace 状态失败")
	}
	if affected == 1 {
		return nil
	}
	current, loadErr := b.GetWorkspace(ctx, workspaceID)
	if loadErr == nil && current.OperationEpoch == operationEpoch && current.State == to &&
		current.FailureCode == failureCode && current.AttemptStatus == attemptStatus {
		return nil
	}
	return errors.New("fenced workspace 状态发生并发变化")
}

func (b *MySQLBackend) WithWorkspaceLock(ctx context.Context, workspaceID string, wait time.Duration, fn func(context.Context) error) (bool, error) {
	if err := workspacedb.ValidateWorkspaceID(workspaceID); err != nil {
		return false, err
	}
	if fn == nil {
		return false, errors.New("workspace lock 回调不能为空")
	}
	waitSeconds := int(wait.Round(time.Second) / time.Second)
	if waitSeconds < 1 {
		waitSeconds = 1
	}
	if waitSeconds > 30 {
		waitSeconds = 30
	}
	connection, err := b.db.Conn(ctx)
	if err != nil {
		return false, errors.New("获取 workspace lock 连接失败")
	}
	defer connection.Close()
	lockName := "pandora_planner:" + workspaceID
	var acquired sql.NullInt64
	if err := connection.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", lockName, waitSeconds).Scan(&acquired); err != nil {
		return false, errors.New("获取 workspace advisory lock 失败")
	}
	if !acquired.Valid || acquired.Int64 != 1 {
		return false, nil
	}
	lockCtx, cancelLock := context.WithCancelCause(ctx)
	monitorDone := make(chan error, 1)
	go func() {
		monitorDone <- b.monitorWorkspaceLock(lockCtx, connection, lockName, cancelLock)
	}()
	callbackErr := fn(lockCtx)
	cancelLock(context.Canceled)
	monitorErr := <-monitorDone
	lockCause := context.Cause(lockCtx)
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var released sql.NullInt64
		_ = connection.QueryRowContext(releaseCtx, "SELECT RELEASE_LOCK(?)", lockName).Scan(&released)
	}()
	if errors.Is(lockCause, ErrWorkspaceLockLost) {
		return true, errors.Join(ErrWorkspaceLockLost, callbackErr, monitorErr)
	}
	return true, errors.Join(callbackErr, monitorErr)
}

func (b *MySQLBackend) monitorWorkspaceLock(
	ctx context.Context,
	connection *sql.Conn,
	lockName string,
	cancel context.CancelCauseFunc,
) error {
	ticker := time.NewTicker(b.lockMonitorInterval)
	defer ticker.Stop()
	for {
		checkCtx, checkCancel := context.WithTimeout(ctx, b.lockMonitorInterval)
		var ownerID, currentID sql.NullInt64
		err := connection.QueryRowContext(checkCtx,
			"SELECT IS_USED_LOCK(?), CONNECTION_ID()", lockName).Scan(&ownerID, &currentID)
		checkCancel()
		if err != nil || !ownerID.Valid || !currentID.Valid || ownerID.Int64 != currentID.Int64 {
			if ctx.Err() != nil {
				return nil
			}
			cancel(ErrWorkspaceLockLost)
			return ErrWorkspaceLockLost
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (b *MySQLBackend) TransitionState(ctx context.Context, workspaceID string, from []State, to State, failureCode string) error {
	if err := workspacedb.ValidateWorkspaceID(workspaceID); err != nil {
		return err
	}
	if len(from) == 0 || !validState(to) || !failureCodePattern.MatchString(failureCode) {
		return errors.New("workspace 状态迁移参数非法")
	}
	placeholders := make([]string, len(from))
	attemptStatus := terminalAttemptStatus(to)
	arguments := []any{to, failureCode, attemptStatus, workspaceID}
	for index, state := range from {
		if !validState(state) {
			return errors.New("workspace 起始状态非法")
		}
		placeholders[index] = "?"
		arguments = append(arguments, state)
	}
	result, err := b.db.ExecContext(ctx, `UPDATE `+b.registryTable("planner_workspaces")+`
SET state=?, failure_code=?, attempt_status=?, updated_at=UTC_TIMESTAMP(6)
WHERE workspace_id=? AND state IN (`+strings.Join(placeholders, ",")+")", arguments...)
	if err != nil {
		return errors.New("更新 workspace 状态失败")
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return errors.New("确认 workspace 状态更新结果失败")
	}
	if affected == 1 {
		return nil
	}
	current, loadErr := b.GetWorkspace(ctx, workspaceID)
	if loadErr == nil && current.State == to && current.FailureCode == failureCode && current.AttemptStatus == attemptStatus {
		return nil
	}
	return errors.New("workspace 状态发生并发变化")
}

func terminalAttemptStatus(state State) AttemptStatus {
	switch state {
	case StateProvisioning:
		return AttemptStatusIdle
	case StateMigrating:
		return AttemptStatusRunning
	case StateReady:
		return AttemptStatusSucceeded
	case StateMigrationFailed:
		return AttemptStatusFailed
	default:
		return ""
	}
}

func validState(state State) bool {
	switch state {
	case StateProvisioning, StateMigrating, StateReady, StateMigrationFailed:
		return true
	default:
		return false
	}
}

func validAttemptStatus(status AttemptStatus) bool {
	switch status {
	case AttemptStatusIdle, AttemptStatusRunning, AttemptStatusSucceeded, AttemptStatusFailed:
		return true
	default:
		return false
	}
}

func validateCandidate(claim EnrollmentClaim) error {
	candidate := claim.Candidate
	if err := workspacedb.ValidateWorkspaceID(candidate.WorkspaceID); err != nil {
		return err
	}
	if candidate.DeviceDigest != claim.DeviceDigest || candidate.DisplayName != claim.DisplayName || candidate.State != StateProvisioning ||
		candidate.OperationEpoch != 0 || candidate.AttemptStatus != AttemptStatusIdle {
		return errors.New("workspace candidate 身份或初态非法")
	}
	if candidate.AppUsername != "p_app_"+candidate.WorkspaceID || candidate.MigrationUsername != "p_mig_"+candidate.WorkspaceID ||
		!credentialUsernamePattern.MatchString(candidate.AppUsername) || !credentialUsernamePattern.MatchString(candidate.MigrationUsername) {
		return errors.New("workspace 数据库用户名非法")
	}
	if candidate.CredentialVersion == 0 || len(candidate.AppSecret.Nonce) != 12 || len(candidate.MigrationSecret.Nonce) != 12 ||
		len(candidate.AppSecret.Ciphertext) == 0 || len(candidate.AppSecret.Ciphertext) > 128 ||
		len(candidate.MigrationSecret.Ciphertext) == 0 || len(candidate.MigrationSecret.Ciphertext) > 128 {
		return errors.New("workspace 加密凭据形态非法")
	}
	return nil
}

func (b *MySQLBackend) EnsureWorkspace(ctx context.Context, access WorkspaceAccess) error {
	if err := validateWorkspaceAccess(access, b.endpoint); err != nil {
		return err
	}
	for _, migrationSet := range workspacedb.CanonicalMigrationSets() {
		database := access.Databases[migrationSet]
		statement := "CREATE DATABASE IF NOT EXISTS `" + database + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
		if _, err := b.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("创建 workspace 数据库 %s 失败", migrationSet)
		}
	}
	if err := b.ensureMySQLUser(ctx, access.AppUsername, access.AppPassword); err != nil {
		return err
	}
	if err := b.ensureMySQLUser(ctx, access.MigrationUsername, access.MigrationPassword); err != nil {
		return err
	}
	for _, migrationSet := range workspacedb.CanonicalMigrationSets() {
		database := access.Databases[migrationSet]
		runtimePrivileges := "SELECT, INSERT, UPDATE, DELETE"
		if migrationSet == "pandora_player" {
			runtimePrivileges += ", CREATE, ALTER, INDEX, REFERENCES"
		}
		if _, err := b.db.ExecContext(ctx, "GRANT "+runtimePrivileges+" ON `"+database+"`.* TO "+mysqlAccount(access.AppUsername)); err != nil {
			return fmt.Errorf("授予 runtime 账号 %s 权限失败", migrationSet)
		}
		if _, err := b.db.ExecContext(ctx, "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, INDEX, REFERENCES ON `"+database+"`.* TO "+mysqlAccount(access.MigrationUsername)); err != nil {
			return fmt.Errorf("授予 migration 账号 %s 权限失败", migrationSet)
		}
	}
	if err := b.verifyUserPrivileges(ctx, access.AppUsername, expectedRuntimePrivileges(access.Databases)); err != nil {
		return err
	}
	if err := b.verifyUserPrivileges(ctx, access.MigrationUsername, expectedMigrationPrivileges(access.Databases)); err != nil {
		return err
	}
	return nil
}

func validateWorkspaceAccess(access WorkspaceAccess, endpoint Endpoint) error {
	if err := validateEndpoint(access.Endpoint); err != nil {
		return err
	}
	if err := workspacedb.ValidateWorkspaceID(access.WorkspaceID); err != nil {
		return err
	}
	if access.Endpoint != endpoint || access.CredentialVersion == 0 ||
		access.AppUsername != "p_app_"+access.WorkspaceID || access.MigrationUsername != "p_mig_"+access.WorkspaceID ||
		!databasePasswordPattern.MatchString(access.AppPassword) || !databasePasswordPattern.MatchString(access.MigrationPassword) {
		return errors.New("workspace access 凭据或 endpoint 非法")
	}
	migrationSets := workspacedb.CanonicalMigrationSets()
	if len(access.Databases) != len(migrationSets) {
		return errors.New("workspace 必须精确映射 10 个 migration set")
	}
	for _, migrationSet := range migrationSets {
		want, err := workspacedb.PhysicalDatabaseName(migrationSet, access.WorkspaceID)
		if err != nil || access.Databases[migrationSet] != want {
			return fmt.Errorf("workspace 数据库映射 %s 非法", migrationSet)
		}
	}
	return nil
}

func mysqlAccount(username string) string {
	// username 已被严格限定为小写 Crockford；不接受调用方任意标识符。
	return "'" + username + "'@'%'"
}

func (b *MySQLBackend) ensureMySQLUser(ctx context.Context, username, password string) error {
	if !credentialUsernamePattern.MatchString(username) || !databasePasswordPattern.MatchString(password) {
		return errors.New("数据库账号材料非法")
	}
	account := mysqlAccount(username)
	statements := []string{
		"CREATE USER IF NOT EXISTS " + account + " IDENTIFIED BY '" + password + "' REQUIRE SSL",
		"ALTER USER " + account + " IDENTIFIED BY '" + password + "' REQUIRE SSL",
		"REVOKE ALL PRIVILEGES, GRANT OPTION FROM " + account,
		"SET DEFAULT ROLE NONE TO " + account,
	}
	for _, statement := range statements {
		if _, err := b.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("初始化数据库账号 %s 失败", username)
		}
	}
	var roles int
	if err := b.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mysql.role_edges WHERE TO_USER=? AND TO_HOST='%'`, username).Scan(&roles); err != nil {
		return fmt.Errorf("验证数据库账号 %s role 失败", username)
	}
	if roles != 0 {
		return fmt.Errorf("数据库账号 %s 仍绑定 %d 个 role，拒绝继续", username, roles)
	}
	return nil
}

func expectedRuntimePrivileges(databases map[string]string) map[string][]string {
	result := make(map[string][]string, len(databases))
	for migrationSet, database := range databases {
		privileges := []string{"DELETE", "INSERT", "SELECT", "UPDATE"}
		if migrationSet == "pandora_player" {
			privileges = append(privileges, "ALTER", "CREATE", "INDEX", "REFERENCES")
		}
		sort.Strings(privileges)
		result[database] = privileges
	}
	return result
}

func expectedMigrationPrivileges(databases map[string]string) map[string][]string {
	result := make(map[string][]string, len(databases))
	for _, database := range databases {
		result[database] = []string{"ALTER", "CREATE", "DELETE", "DROP", "INDEX", "INSERT", "REFERENCES", "SELECT", "UPDATE"}
	}
	return result
}

func (b *MySQLBackend) verifyUserPrivileges(ctx context.Context, username string, expected map[string][]string) error {
	grantee := mysqlAccount(username)
	rows, err := b.db.QueryContext(ctx, `SELECT Host FROM mysql.user WHERE User=? ORDER BY Host`, username)
	if err != nil {
		return fmt.Errorf("读取数据库账号 %s host 集合失败", username)
	}
	var hosts []string
	for rows.Next() {
		var host string
		if err := rows.Scan(&host); err != nil {
			_ = rows.Close()
			return fmt.Errorf("解析数据库账号 %s host 集合失败", username)
		}
		hosts = append(hosts, host)
	}
	rowsErr := rows.Err()
	_ = rows.Close()
	if rowsErr != nil {
		return fmt.Errorf("遍历数据库账号 %s host 集合失败", username)
	}
	if err := validateAccountHosts(hosts); err != nil {
		return fmt.Errorf("数据库账号 %s host 边界异常: %w", username, err)
	}
	var sslType string
	if err := b.db.QueryRowContext(ctx, `SELECT ssl_type FROM mysql.user WHERE User=? AND Host='%'`, username).Scan(&sslType); err != nil || sslType != "ANY" {
		return fmt.Errorf("数据库账号 %s 未强制 REQUIRE SSL", username)
	}
	var globalPrivileges int
	if err := b.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.USER_PRIVILEGES
WHERE GRANTEE=? AND PRIVILEGE_TYPE <> 'USAGE'`, grantee).Scan(&globalPrivileges); err != nil || globalPrivileges != 0 {
		return fmt.Errorf("数据库账号 %s 存在全局权限", username)
	}
	type privilegeSet map[string]bool
	actual := make(map[string]privilegeSet)
	rows, err = b.db.QueryContext(ctx, `SELECT TABLE_SCHEMA, PRIVILEGE_TYPE, IS_GRANTABLE
FROM information_schema.SCHEMA_PRIVILEGES WHERE GRANTEE=?`, grantee)
	if err != nil {
		return fmt.Errorf("读取数据库账号 %s schema 权限失败", username)
	}
	defer rows.Close()
	for rows.Next() {
		var database, privilege, grantable string
		if err := rows.Scan(&database, &privilege, &grantable); err != nil {
			return fmt.Errorf("解析数据库账号 %s schema 权限失败", username)
		}
		if grantable != "NO" {
			return fmt.Errorf("数据库账号 %s 含 GRANT OPTION", username)
		}
		if _, ok := expected[database]; !ok {
			return fmt.Errorf("数据库账号 %s 越权访问非本 workspace 库 %s", username, database)
		}
		if actual[database] == nil {
			actual[database] = make(privilegeSet)
		}
		actual[database][strings.ToUpper(privilege)] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("遍历数据库账号 %s schema 权限失败", username)
	}
	for database, privileges := range expected {
		if len(actual[database]) != len(privileges) {
			return fmt.Errorf("数据库账号 %s 对 %s 权限集合不完整", username, database)
		}
		for _, privilege := range privileges {
			if !actual[database][privilege] {
				return fmt.Errorf("数据库账号 %s 对 %s 缺少 %s", username, database, privilege)
			}
		}
	}
	for _, scope := range []string{"TABLE_PRIVILEGES", "COLUMN_PRIVILEGES"} {
		var scoped int
		query := "SELECT COUNT(*) FROM information_schema." + scope + " WHERE GRANTEE=?"
		if err := b.db.QueryRowContext(ctx, query, grantee).Scan(&scoped); err != nil || scoped != 0 {
			return fmt.Errorf("数据库账号 %s 存在未审核的 %s", username, scope)
		}
	}
	var routinePrivileges int
	if err := b.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mysql.procs_priv WHERE User=?`,
		username).Scan(&routinePrivileges); err != nil || routinePrivileges != 0 {
		return fmt.Errorf("数据库账号 %s 存在未审核的 routine privileges", username)
	}
	var proxyPrivileges int
	if err := b.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mysql.proxies_priv
WHERE User=? OR Proxied_user=?`,
		username, username).Scan(&proxyPrivileges); err != nil || proxyPrivileges != 0 {
		return fmt.Errorf("数据库账号 %s 存在未审核的 proxy privileges", username)
	}
	return nil
}

func validateAccountHosts(hosts []string) error {
	if len(hosts) != 1 || hosts[0] != "%" {
		return errors.New("必须恰好只有 Host='%' 一条账号，发现更具体 Host 时拒绝自动删除")
	}
	return nil
}

var _ Registry = (*MySQLBackend)(nil)
var _ ResourceManager = (*MySQLBackend)(nil)
