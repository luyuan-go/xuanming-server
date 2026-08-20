package plannerdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/luyuancpp/pandora/tools/migrate/workspacedb"
)

const maximumMigrationOutputBytes = 1 << 20

type migrationExecutor func(context.Context, string, []string, []string) ([]byte, error)

type execMigrationConfig struct {
	BinaryPath     string
	ExpectedSHA256 string
	MySQLCAPEM     []byte
	Timeout        time.Duration
	Executor       migrationExecutor
}

type execMigrationRunner struct {
	binaryImage []byte
	binaryName  string
	mysqlCAPEM  []byte
	timeout     time.Duration
	executor    migrationExecutor
}

type migrationManifest struct {
	Targets []migrationManifestTarget `json:"targets"`
}

type migrationManifestTarget struct {
	Name                   string `json:"name"`
	MigrationSet           string `json:"migration_set"`
	Database               string `json:"database"`
	DSNFile                string `json:"dsn_file"`
	TLSCAFile              string `json:"tls_ca_file"`
	TimeoutSeconds         int    `json:"timeout_seconds"`
	LockWaitTimeoutSeconds int    `json:"lock_wait_timeout_seconds"`
}

func newExecMigrationRunner(config execMigrationConfig) (*execMigrationRunner, error) {
	if !filepath.IsAbs(config.BinaryPath) || strings.TrimSpace(config.ExpectedSHA256) == "" {
		return nil, errors.New("migrate binary 必须使用绝对路径并提供 release SHA256")
	}
	info, err := os.Lstat(config.BinaryPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > 64<<20 {
		return nil, errors.New("migrate binary 必须是 1..64MiB 的非符号链接普通文件")
	}
	expectedHash, err := hex.DecodeString(config.ExpectedSHA256)
	if err != nil || len(expectedHash) != sha256.Size || config.ExpectedSHA256 != strings.ToLower(config.ExpectedSHA256) {
		return nil, errors.New("migrate binary SHA256 必须是 64 位小写十六进制")
	}
	image, err := os.ReadFile(config.BinaryPath)
	if err != nil || int64(len(image)) != info.Size() {
		return nil, errors.New("读取 migrate binary snapshot 失败")
	}
	actualHash := sha256.Sum256(image)
	if !bytes.Equal(expectedHash, actualHash[:]) {
		return nil, errors.New("migrate binary SHA256 与同版本 release manifest 不一致")
	}
	if len(config.MySQLCAPEM) == 0 || len(config.MySQLCAPEM) > 1<<20 {
		return nil, errors.New("MySQL CA PEM 必须在 1..1MiB")
	}
	if config.Timeout < time.Minute || config.Timeout > 4*time.Hour {
		return nil, errors.New("migrate 总 timeout 必须在 1 分钟到 4 小时之间")
	}
	if config.Executor == nil {
		config.Executor = executeMigrationCommand
	}
	binaryName := "pandora-migrate"
	if strings.EqualFold(filepath.Ext(config.BinaryPath), ".exe") {
		binaryName += ".exe"
	}
	return &execMigrationRunner{
		binaryImage: append([]byte(nil), image...), binaryName: binaryName,
		mysqlCAPEM: append([]byte(nil), config.MySQLCAPEM...), timeout: config.Timeout, executor: config.Executor,
	}, nil
}

func (r *execMigrationRunner) MigrateWorkspace(ctx context.Context, access WorkspaceAccess) (returnErr error) {
	if err := validateWorkspaceAccess(access, access.Endpoint); err != nil {
		return err
	}
	bundleDirectory, err := newSecureTempDirectory("pandora-plannerdb-migrate-")
	if err != nil {
		return errors.New("创建 migration secret bundle 失败")
	}
	defer func() {
		cleanupErr := os.RemoveAll(bundleDirectory)
		if cleanupErr == nil {
			if _, statErr := os.Stat(bundleDirectory); !errors.Is(statErr, os.ErrNotExist) {
				cleanupErr = errors.New("migration secret bundle 删除后仍存在")
			}
		}
		if cleanupErr != nil {
			if returnErr == nil {
				returnErr = errors.New("migration 已结束但 secret bundle 无法删除，拒绝进入 READY")
			} else {
				returnErr = fmt.Errorf("%w；且 secret bundle 无法删除", returnErr)
			}
		}
	}()

	binaryPath := filepath.Join(bundleDirectory, r.binaryName)
	if err := writePrivateFile(binaryPath, r.binaryImage, 0o700); err != nil {
		return errors.New("写入 migrate binary snapshot 失败")
	}
	caPath := filepath.Join(bundleDirectory, "mysql-ca.pem")
	if err := writePrivateFile(caPath, r.mysqlCAPEM, 0o600); err != nil {
		return errors.New("写入 migration CA bundle 失败")
	}

	migrationSets := workspacedb.CanonicalMigrationSets()
	manifest := migrationManifest{Targets: make([]migrationManifestTarget, 0, len(migrationSets))}
	expectedTargets := make([]string, 0, len(migrationSets))
	for index, migrationSet := range migrationSets {
		database := access.Databases[migrationSet]
		targetName := "planner-" + strings.ReplaceAll(strings.TrimPrefix(migrationSet, "pandora_"), "_", "-")
		dsnName := fmt.Sprintf("%02d-%s.dsn", index+1, migrationSet)
		dsnConfig := mysql.NewConfig()
		dsnConfig.User = access.MigrationUsername
		dsnConfig.Passwd = access.MigrationPassword
		dsnConfig.Net = "tcp"
		dsnConfig.Addr = fmt.Sprintf("%s:%d", access.Endpoint.Host, access.Endpoint.Port)
		dsnConfig.DBName = database
		dsnConfig.TLSConfig = "true"
		if err := writePrivateFile(filepath.Join(bundleDirectory, dsnName), []byte(dsnConfig.FormatDSN()+"\n"), 0o600); err != nil {
			return errors.New("写入 migration DSN bundle 失败")
		}
		manifest.Targets = append(manifest.Targets, migrationManifestTarget{
			Name: targetName, MigrationSet: migrationSet, Database: database, DSNFile: dsnName,
			TLSCAFile: caPath, TimeoutSeconds: 900, LockWaitTimeoutSeconds: 15,
		})
		expectedTargets = append(expectedTargets, targetName+":"+migrationSet+":"+database)
	}
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return errors.New("编码 migration targets 失败")
	}
	manifestBytes = append(manifestBytes, '\n')
	manifestPath := filepath.Join(bundleDirectory, "targets.json")
	if err := writePrivateFile(manifestPath, manifestBytes, 0o600); err != nil {
		return errors.New("写入 migration targets 失败")
	}

	runCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	arguments := []string{
		"-targets-file", manifestPath,
		"-expected-targets", strings.Join(expectedTargets, ","),
		"-environment", "production",
		"-workspace-id", access.WorkspaceID,
	}
	output, runErr := r.executor(runCtx, binaryPath, arguments, safeMigrationEnvironment())
	if runErr != nil {
		safeOutput := redactMigrationOutput(output, access)
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("migration runner 超过 %s 硬时限；输出=%s", r.timeout, safeOutput)
		}
		return fmt.Errorf("migration runner 非零退出；输出=%s", safeOutput)
	}
	return nil
}

func redactMigrationOutput(output []byte, access WorkspaceAccess) string {
	value := string(output)
	for _, secret := range []string{access.AppPassword, access.MigrationPassword} {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, "[REDACTED]")
		}
	}
	if len(value) > 4096 {
		value = value[len(value)-4096:]
	}
	return strings.TrimSpace(value)
}

func safeMigrationEnvironment() []string {
	allowed := []string{"SystemRoot", "WINDIR", "PATH", "PATHEXT", "COMSPEC", "TEMP", "TMP", "TZ"}
	environment := make([]string, 0, len(allowed))
	for _, name := range allowed {
		if value, ok := os.LookupEnv(name); ok {
			environment = append(environment, name+"="+value)
		}
	}
	return environment
}

var _ MigrationRunner = (*execMigrationRunner)(nil)
