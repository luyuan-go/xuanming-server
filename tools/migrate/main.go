// Pandora 数据库迁移器。
//
// 迁移目标与凭据由外部 Secret 文件显式提供；二进制不会从业务配置猜测数据库，也不会
// 把 DSN 放进参数或日志。每个物理库独立执行并受硬超时约束，任一目标失败都会让整个
// 进程以非零状态退出，从而阻断后续业务发布。
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"embed"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/golang-migrate/migrate/v4"
	migratemysql "github.com/golang-migrate/migrate/v4/database/mysql"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/luyuancpp/pandora/tools/migrate/workspacedb"
)

const (
	defaultTargetTimeoutSeconds   = 900
	defaultLockWaitTimeoutSeconds = 15
	minimumTargetTimeoutSeconds   = 30
	maximumTargetTimeoutSeconds   = 3600
	maximumLockWaitTimeoutSeconds = 60
	connectionTimeout             = 10 * time.Second
	workerShutdownGracePeriod     = 5 * time.Second
	mysqlAdvisoryLockWait         = 10 * time.Second
	advisoryLockWrapperGrace      = time.Second
	maximumSecretFileBytes        = 64 << 10
	schemaMigrationsTable         = "schema_migrations"
	// pandoraPlayerV7SHA256 固定已发布 000007 的 LF 规范化正文。quarantine 只有在
	// 镜像内仍是这一份精确 SQL 时才可运行，防止“同版本不同内容”被自动洗成 clean。
	pandoraPlayerV7SHA256 = "576b73ea3eee0e3dedd3ce22d5c6e7f7a4f278aa8e787cb9e74e292eea4f2e4f"
)

var (
	targetNamePattern    = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	databaseNamePattern  = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	migrationFilePattern = regexp.MustCompile(
		`^([0-9]+)_[a-z0-9_]+\.up\.sql$`,
	)
	mysqlAccountPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,32}$`)
	// 真实 quarantine 只认 canonical/workspace 库；额外这一项仅供本文件的真库集成测试，
	// 且钉死 time.Now().UnixNano() 当前生成的 19 位十进制后缀，不能退回宽泛前缀。
	pandoraPlayerMigrationIntegrationDatabasePattern = regexp.MustCompile(
		`^pandora_player_mig_it_[0-9]{19}$`,
	)
)

// migrationsFS 烘焙进 scratch 镜像，无需运行期 ConfigMap 或可写卷。
//
//go:embed all:migrations
var migrationsFS embed.FS

type targetManifest struct {
	Targets []migrationTarget `json:"targets"`
}

// migrationTarget 描述一个物理数据库。多个拍卖分片可以复用同一个 MigrationSet，
// 但必须分别给出唯一 Name、Database 与 DSNFile。
type migrationTarget struct {
	Name                     string `json:"name"`
	MigrationSet             string `json:"migration_set"`
	Database                 string `json:"database"`
	DSNFile                  string `json:"dsn_file"`
	TLSCAFile                string `json:"tls_ca_file,omitempty"`
	BootstrapAdminDSNFile    string `json:"bootstrap_admin_dsn_file,omitempty"`
	TimeoutSeconds           int    `json:"timeout_seconds,omitempty"`
	LockWaitTimeoutSeconds   int    `json:"lock_wait_timeout_seconds,omitempty"`
	expectedMigrationVersion uint
}

type targetDescriptor struct {
	Name         string
	MigrationSet string
	Database     string
}

type commandConfig struct {
	TargetsFile     string
	ExpectedTargets string
	Environment     string
	Bootstrap       bool
	WorkspaceID     string
	VerifyOnly      bool
	WorkerTarget    string
}

type migrationVersionReader interface {
	Version() (version uint, dirty bool, err error)
}

type migrationRepairDriver interface {
	Version() (version int, dirty bool, err error)
	Lock() error
	Unlock() error
	SetVersion(version int, dirty bool) error
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.LUTC)
	if err := run(os.Args[1:]); err != nil {
		log.Printf("[migrate] 失败: %v", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := parseCommandConfig(args)
	if err != nil {
		return err
	}
	manifest, err := loadTargetManifest(cfg.TargetsFile)
	if err != nil {
		return err
	}
	expectedTargets, err := parseExpectedTargetInventory(cfg.ExpectedTargets)
	if err != nil {
		return err
	}
	if err := validateExpectedTargets(manifest.Targets, expectedTargets); err != nil {
		return err
	}
	if err := validateWorkspaceTargets(manifest.Targets, cfg.WorkspaceID); err != nil {
		return fmt.Errorf("workspace 目标校验失败: %w", err)
	}
	requireTLS := cfg.VerifyOnly || requiresVerifiedTLS(cfg.Environment, cfg.WorkspaceID)
	if err := preflightTargets(manifest.Targets, cfg.Bootstrap, requireTLS); err != nil {
		return fmt.Errorf("目标预检失败: %w", err)
	}

	// worker 只处理父进程指定的单目标。父进程用操作系统级进程超时兜底，避免迁移 DDL、
	// TLS 握手或只读验收查询永久卡住。
	if cfg.WorkerTarget != "" {
		target, ok := findTarget(manifest.Targets, cfg.WorkerTarget)
		if !ok {
			return fmt.Errorf("worker 目标 %q 不在清单中", cfg.WorkerTarget)
		}
		return executeWorkerTargetMode(
			cfg.VerifyOnly,
			func() error { return verifyTarget(target, requireTLS) },
			func() error { return migrateTarget(target, requireTLS) },
		)
	}

	if cfg.Bootstrap {
		if !isDevelopmentEnvironment(cfg.Environment) {
			return fmt.Errorf("bootstrap 仅允许 local/dev/development，当前环境为 %q", cfg.Environment)
		}
		if err := bootstrapTargets(manifest.Targets); err != nil {
			return fmt.Errorf("开发环境 bootstrap 失败: %w", err)
		}
	}

	for _, target := range manifest.Targets {
		if err := runTargetWithDeadline(
			cfg.TargetsFile, cfg.Environment, cfg.WorkspaceID, cfg.VerifyOnly, expectedTargets, target,
		); err != nil {
			return err
		}
	}
	if cfg.VerifyOnly {
		log.Printf("[migrate] 全部 %d 个物理目标只读预检完成", len(manifest.Targets))
	} else {
		log.Printf("[migrate] 全部 %d 个物理目标迁移并验收完成", len(manifest.Targets))
	}
	return nil
}

func executeWorkerTargetMode(verifyOnly bool, verify, migrate func() error) error {
	if verifyOnly {
		return verify()
	}
	return migrate()
}

func parseCommandConfig(args []string) (commandConfig, error) {
	bootstrapDefault, err := strictEnvBool("MIGRATE_BOOTSTRAP_DB", false)
	if err != nil {
		return commandConfig{}, err
	}
	verifyOnlyDefault := false
	if !hasExplicitBoolFlag(args, "verify-only") {
		verifyOnlyDefault, err = strictEnvBool("MIGRATE_VERIFY_ONLY", false)
		if err != nil {
			return commandConfig{}, err
		}
	}

	set := flag.NewFlagSet("pandora-migrate", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	cfg := commandConfig{}
	set.StringVar(&cfg.TargetsFile, "targets-file", os.Getenv("MIGRATE_TARGETS_FILE"),
		"迁移目标清单文件（必须由外部 Secret 提供）")
	set.StringVar(&cfg.ExpectedTargets, "expected-targets", os.Getenv("MIGRATE_EXPECTED_TARGETS"),
		"发布侧独立审核的 name:migration_set:database 精确目标集合（逗号分隔）")
	set.StringVar(&cfg.Environment, "environment", envOr("PANDORA_ENV", "production"),
		"环境名；bootstrap 只允许 local/dev/development")
	set.BoolVar(&cfg.Bootstrap, "bootstrap", bootstrapDefault,
		"仅开发环境显式建库并给迁移账号授权，默认 false")
	set.StringVar(&cfg.WorkspaceID, "workspace-id", os.Getenv("MIGRATE_WORKSPACE_ID"),
		"中心开发库的稳定 workspace ID；workspace 目标必须显式提供")
	set.BoolVar(&cfg.VerifyOnly, "verify-only", verifyOnlyDefault,
		"只读校验连接、TLS 与 schema 版本，不执行迁移或建库")
	set.StringVar(&cfg.WorkerTarget, "worker-target", "", "内部参数：只执行一个迁移目标")
	if err := set.Parse(args); err != nil {
		return commandConfig{}, fmt.Errorf("解析参数: %w", err)
	}
	if set.NArg() != 0 {
		return commandConfig{}, fmt.Errorf("存在未识别参数: %s", strings.Join(set.Args(), " "))
	}
	cfg.TargetsFile = strings.TrimSpace(cfg.TargetsFile)
	if cfg.TargetsFile == "" {
		return commandConfig{}, errors.New("必须通过 -targets-file 或 MIGRATE_TARGETS_FILE 显式指定目标清单")
	}
	abs, err := filepath.Abs(cfg.TargetsFile)
	if err != nil {
		return commandConfig{}, fmt.Errorf("解析目标清单绝对路径: %w", err)
	}
	cfg.TargetsFile = filepath.Clean(abs)
	cfg.ExpectedTargets = strings.TrimSpace(cfg.ExpectedTargets)
	if cfg.ExpectedTargets == "" {
		return commandConfig{}, errors.New("必须通过 -expected-targets 或 MIGRATE_EXPECTED_TARGETS 提供独立目标清单")
	}
	cfg.Environment = strings.ToLower(strings.TrimSpace(cfg.Environment))
	if cfg.Environment == "" {
		cfg.Environment = "production"
	}
	cfg.WorkspaceID = strings.TrimSpace(cfg.WorkspaceID)
	if cfg.WorkspaceID != "" {
		if err := workspacedb.ValidateWorkspaceID(cfg.WorkspaceID); err != nil {
			return commandConfig{}, err
		}
		if cfg.Bootstrap {
			return commandConfig{}, errors.New("workspace 目标禁止使用客户端 bootstrap/admin DSN；必须由中心 provisioner 建库授权")
		}
	}
	if cfg.VerifyOnly && cfg.Bootstrap {
		return commandConfig{}, errors.New("verify-only 与 bootstrap 互斥，只读预检不得建库或授权")
	}
	return cfg, nil
}

func hasExplicitBoolFlag(args []string, name string) bool {
	short := "-" + name
	long := "--" + name
	for _, arg := range args {
		if arg == "--" {
			return false
		}
		if arg == short || arg == long || strings.HasPrefix(arg, short+"=") || strings.HasPrefix(arg, long+"=") {
			return true
		}
	}
	return false
}

func loadTargetManifest(path string) (targetManifest, error) {
	data, err := readBoundedFile(path)
	if err != nil {
		return targetManifest{}, fmt.Errorf("读取目标清单: %w", err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var manifest targetManifest
	if err := decoder.Decode(&manifest); err != nil {
		return targetManifest{}, fmt.Errorf("解析目标清单 JSON: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return targetManifest{}, err
	}
	if len(manifest.Targets) == 0 {
		return targetManifest{}, errors.New("目标清单 targets 不能为空")
	}

	manifestDir := filepath.Dir(path)
	seenNames := make(map[string]struct{}, len(manifest.Targets))
	for i := range manifest.Targets {
		target := &manifest.Targets[i]
		target.Name = strings.TrimSpace(target.Name)
		target.MigrationSet = strings.TrimSpace(target.MigrationSet)
		target.Database = strings.TrimSpace(target.Database)
		if !targetNamePattern.MatchString(target.Name) {
			return targetManifest{}, fmt.Errorf("targets[%d].name=%q 非法", i, target.Name)
		}
		if _, exists := seenNames[target.Name]; exists {
			return targetManifest{}, fmt.Errorf("目标名 %q 重复", target.Name)
		}
		seenNames[target.Name] = struct{}{}
		if !databaseNamePattern.MatchString(target.MigrationSet) {
			return targetManifest{}, fmt.Errorf("目标 %s 的 migration_set=%q 非法", target.Name, target.MigrationSet)
		}
		if !databaseNamePattern.MatchString(target.Database) {
			return targetManifest{}, fmt.Errorf("目标 %s 的 database=%q 非法", target.Name, target.Database)
		}
		if !validMigrationDatabaseMapping(target.MigrationSet, target.Database) {
			return targetManifest{}, fmt.Errorf(
				"目标 %s 的 database=%q 必须等于 migration_set=%q、使用合法 workspace 名或其下划线分片前缀",
				target.Name, target.Database, target.MigrationSet,
			)
		}
		target.TLSCAFile = strings.TrimSpace(target.TLSCAFile)
		_, workspaceTarget := workspacedb.ParsePhysicalDatabaseName(target.MigrationSet, target.Database)
		if workspaceTarget {
			if target.TLSCAFile == "" {
				return targetManifest{}, fmt.Errorf("目标 %s 的 workspace 库必须提供 tls_ca_file", target.Name)
			}
			if !filepath.IsAbs(target.TLSCAFile) {
				return targetManifest{}, fmt.Errorf("目标 %s 的 tls_ca_file 必须是绝对路径", target.Name)
			}
			target.TLSCAFile = filepath.Clean(target.TLSCAFile)
			info, err := os.Stat(target.TLSCAFile)
			if err != nil {
				return targetManifest{}, fmt.Errorf("目标 %s 的 tls_ca_file 不可读: %w", target.Name, err)
			}
			if !info.Mode().IsRegular() {
				return targetManifest{}, fmt.Errorf("目标 %s 的 tls_ca_file 必须是普通文件", target.Name)
			}
		} else if target.TLSCAFile != "" {
			return targetManifest{}, fmt.Errorf("目标 %s 不是 workspace 库，不得设置 tls_ca_file", target.Name)
		}
		if strings.TrimSpace(target.DSNFile) == "" {
			return targetManifest{}, fmt.Errorf("目标 %s 缺少 dsn_file", target.Name)
		}
		resolvedDSN, err := resolveSecretPath(manifestDir, target.DSNFile)
		if err != nil {
			return targetManifest{}, fmt.Errorf("目标 %s 的 dsn_file: %w", target.Name, err)
		}
		target.DSNFile = resolvedDSN
		if target.BootstrapAdminDSNFile != "" {
			resolvedAdminDSN, err := resolveSecretPath(manifestDir, target.BootstrapAdminDSNFile)
			if err != nil {
				return targetManifest{}, fmt.Errorf("目标 %s 的 bootstrap_admin_dsn_file: %w", target.Name, err)
			}
			target.BootstrapAdminDSNFile = resolvedAdminDSN
		}
		if target.TimeoutSeconds == 0 {
			target.TimeoutSeconds = defaultTargetTimeoutSeconds
		}
		if target.TimeoutSeconds < minimumTargetTimeoutSeconds || target.TimeoutSeconds > maximumTargetTimeoutSeconds {
			return targetManifest{}, fmt.Errorf("目标 %s 的 timeout_seconds 必须在 %d..%d", target.Name, minimumTargetTimeoutSeconds, maximumTargetTimeoutSeconds)
		}
		if target.LockWaitTimeoutSeconds == 0 {
			target.LockWaitTimeoutSeconds = defaultLockWaitTimeoutSeconds
		}
		if target.LockWaitTimeoutSeconds < 1 || target.LockWaitTimeoutSeconds > maximumLockWaitTimeoutSeconds {
			return targetManifest{}, fmt.Errorf("目标 %s 的 lock_wait_timeout_seconds 必须在 1..%d", target.Name, maximumLockWaitTimeoutSeconds)
		}
		if target.LockWaitTimeoutSeconds >= target.TimeoutSeconds {
			return targetManifest{}, fmt.Errorf("目标 %s 的锁等待超时必须小于目标总超时", target.Name)
		}
		latest, err := latestMigrationVersion(target.MigrationSet)
		if err != nil {
			return targetManifest{}, fmt.Errorf("目标 %s: %w", target.Name, err)
		}
		target.expectedMigrationVersion = latest
	}
	return manifest, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("目标清单只能包含一个 JSON 对象")
		}
		return fmt.Errorf("目标清单尾部存在非法内容: %w", err)
	}
	return nil
}

func parseExpectedTargetInventory(raw string) ([]targetDescriptor, error) {
	parts := strings.Split(raw, ",")
	seenNames := make(map[string]struct{}, len(parts))
	descriptors := make([]targetDescriptor, 0, len(parts))
	for _, part := range parts {
		fields := strings.Split(strings.TrimSpace(part), ":")
		if len(fields) != 3 {
			return nil, fmt.Errorf("expected-targets 项 %q 必须是 name:migration_set:database", strings.TrimSpace(part))
		}
		descriptor := targetDescriptor{
			Name:         strings.TrimSpace(fields[0]),
			MigrationSet: strings.TrimSpace(fields[1]),
			Database:     strings.TrimSpace(fields[2]),
		}
		if !targetNamePattern.MatchString(descriptor.Name) {
			return nil, fmt.Errorf("expected-targets 中的目标名 %q 非法", descriptor.Name)
		}
		if !databaseNamePattern.MatchString(descriptor.MigrationSet) || !databaseNamePattern.MatchString(descriptor.Database) {
			return nil, fmt.Errorf("expected-targets 中目标 %s 的 migration_set/database 非法", descriptor.Name)
		}
		if !validMigrationDatabaseMapping(descriptor.MigrationSet, descriptor.Database) {
			return nil, fmt.Errorf("expected-targets 中目标 %s 的 database 与 migration_set 映射非法", descriptor.Name)
		}
		if _, exists := seenNames[descriptor.Name]; exists {
			return nil, fmt.Errorf("expected-targets 中的目标名 %q 重复", descriptor.Name)
		}
		seenNames[descriptor.Name] = struct{}{}
		descriptors = append(descriptors, descriptor)
	}
	if len(descriptors) == 0 {
		return nil, errors.New("expected-targets 不能为空")
	}
	sort.Slice(descriptors, func(i, j int) bool {
		return targetDescriptorKey(descriptors[i]) < targetDescriptorKey(descriptors[j])
	})
	return descriptors, nil
}

func validateExpectedTargets(targets []migrationTarget, expected []targetDescriptor) error {
	actualSet := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		actualSet[targetDescriptorKey(targetDescriptor{
			Name: target.Name, MigrationSet: target.MigrationSet, Database: target.Database,
		})] = struct{}{}
	}
	expectedSet := make(map[string]struct{}, len(expected))
	for _, descriptor := range expected {
		expectedSet[targetDescriptorKey(descriptor)] = struct{}{}
	}
	missing := make([]string, 0)
	for _, descriptor := range expected {
		key := targetDescriptorKey(descriptor)
		if _, exists := actualSet[key]; !exists {
			missing = append(missing, key)
		}
	}
	unexpected := make([]string, 0)
	for name := range actualSet {
		if _, exists := expectedSet[name]; !exists {
			unexpected = append(unexpected, name)
		}
	}
	sort.Strings(unexpected)
	if len(missing) != 0 || len(unexpected) != 0 {
		return fmt.Errorf("目标清单与发布 inventory 不一致: missing=%v unexpected=%v", missing, unexpected)
	}
	return nil
}

func targetDescriptorKey(descriptor targetDescriptor) string {
	return descriptor.Name + ":" + descriptor.MigrationSet + ":" + descriptor.Database
}

func formatExpectedTargetInventory(descriptors []targetDescriptor) string {
	parts := make([]string, 0, len(descriptors))
	for _, descriptor := range descriptors {
		parts = append(parts, targetDescriptorKey(descriptor))
	}
	return strings.Join(parts, ",")
}

func validMigrationDatabaseMapping(migrationSet, database string) bool {
	if database == migrationSet {
		return true
	}
	if strings.HasPrefix(database, migrationSet+"_w_") {
		_, ok := workspacedb.ParsePhysicalDatabaseName(migrationSet, database)
		return ok
	}
	return strings.HasPrefix(database, migrationSet+"_")
}

// validateWorkspaceTargets 把 workspace ID 当作独立的 fail-closed guard，而不是从
// database 字符串反推授权。workspace 清单缺 guard、混入 canonical/shard 库或串到另一个
// workspace 都会在读取 DSN 和连库前失败。
func validateWorkspaceTargets(targets []migrationTarget, workspaceID string) error {
	if workspaceID != "" {
		if err := workspacedb.ValidateWorkspaceID(workspaceID); err != nil {
			return err
		}
	}
	for _, target := range targets {
		actualID, isWorkspace := workspacedb.ParsePhysicalDatabaseName(target.MigrationSet, target.Database)
		if strings.HasPrefix(target.Database, target.MigrationSet+"_w_") && !isWorkspace {
			return fmt.Errorf("目标 %s 的 database=%q 使用保留 _w_ 命名空间但不是合法 workspace 库", target.Name, target.Database)
		}
		if workspaceID == "" {
			if isWorkspace {
				return fmt.Errorf("目标 %s 指向 workspace 库 %q，但缺少 -workspace-id guard", target.Name, target.Database)
			}
			continue
		}
		if !isWorkspace {
			return fmt.Errorf("目标 %s 的 database=%q 不是 workspace %s 的精确物理库", target.Name, target.Database, workspaceID)
		}
		if actualID != workspaceID {
			return fmt.Errorf("目标 %s 的 database 属于 workspace %s，不是本次 guard=%s", target.Name, actualID, workspaceID)
		}
	}
	return nil
}

func latestMigrationVersion(migrationSet string) (uint, error) {
	entries, err := fs.ReadDir(migrationsFS, "migrations/"+migrationSet)
	if err != nil {
		return 0, fmt.Errorf("迁移集 migrations/%s 不存在: %w", migrationSet, err)
	}
	seen := make(map[uint]string)
	var latest uint
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		match := migrationFilePattern.FindStringSubmatch(entry.Name())
		if match == nil {
			continue
		}
		value, err := strconv.ParseUint(match[1], 10, 64)
		if err != nil || value == 0 {
			return 0, fmt.Errorf("迁移文件版本非法: %s", entry.Name())
		}
		version := uint(value)
		if previous, exists := seen[version]; exists {
			return 0, fmt.Errorf("迁移版本 %d 重复: %s / %s", version, previous, entry.Name())
		}
		seen[version] = entry.Name()
		if version > latest {
			latest = version
		}
	}
	if latest == 0 {
		return 0, fmt.Errorf("迁移集 migrations/%s 没有 .up.sql", migrationSet)
	}
	return latest, nil
}

func preflightTargets(targets []migrationTarget, bootstrap, requireTLS bool) error {
	physicalTargets := make(map[string]string, len(targets))
	for _, target := range targets {
		cfg, err := readAndHardenDSN(target.DSNFile, target, requireTLS)
		if err != nil {
			return fmt.Errorf("目标 %s: %w", target.Name, err)
		}
		identity := cfg.Net + "|" + cfg.Addr + "|" + cfg.DBName
		if previous, exists := physicalTargets[identity]; exists {
			return fmt.Errorf("目标 %s 与 %s 指向同一物理库；每库只能声明一次", target.Name, previous)
		}
		physicalTargets[identity] = target.Name
		if bootstrap && strings.TrimSpace(target.BootstrapAdminDSNFile) == "" {
			return fmt.Errorf("目标 %s 开启 bootstrap 时必须提供 bootstrap_admin_dsn_file", target.Name)
		}
	}
	return nil
}

func readAndHardenDSN(path string, target migrationTarget, requireTLS bool) (*mysql.Config, error) {
	data, err := readBoundedFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取 dsn_file: %w", err)
	}
	dsn := strings.TrimSpace(string(data))
	if dsn == "" {
		return nil, errors.New("dsn_file 为空")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("DSN 格式错误: %w", err)
	}
	if cfg.User == "" {
		return nil, errors.New("DSN 必须包含独立迁移账号")
	}
	if cfg.DBName != target.Database {
		return nil, fmt.Errorf("DSN database=%q 与清单 database=%q 不一致", cfg.DBName, target.Database)
	}
	_, workspaceTarget := workspacedb.ParsePhysicalDatabaseName(target.MigrationSet, target.Database)
	if requireTLS || workspaceTarget {
		if cfg.TLSConfig != "true" || cfg.TLS == nil || cfg.TLS.InsecureSkipVerify || cfg.AllowFallbackToPlaintext {
			return nil, errors.New("production DSN 必须显式 tls=true，拒绝明文、preferred、skip-verify 与 plaintext fallback")
		}
	}
	if workspaceTarget {
		if err := registerWorkspaceTLSConfig(cfg, target); err != nil {
			return nil, err
		}
	}
	cfg.MultiStatements = true
	cfg.ParseTime = true
	cfg.Timeout = connectionTimeout
	cfg.ReadTimeout = statementTimeout(target)
	cfg.WriteTimeout = statementTimeout(target)
	if cfg.Params == nil {
		cfg.Params = make(map[string]string)
	}
	// 通过 DSN 系统变量让连接池创建的每条连接都带有限 metadata/行锁等待，
	// 不能只在某条随机 session 上 SET。
	cfg.Params["lock_wait_timeout"] = strconv.Itoa(target.LockWaitTimeoutSeconds)
	cfg.Params["innodb_lock_wait_timeout"] = strconv.Itoa(target.LockWaitTimeoutSeconds)
	return cfg, nil
}

func registerWorkspaceTLSConfig(cfg *mysql.Config, target migrationTarget) error {
	if target.TLSCAFile == "" {
		return errors.New("workspace 目标缺少 tls_ca_file")
	}
	if !filepath.IsAbs(target.TLSCAFile) {
		return errors.New("workspace tls_ca_file 必须是绝对路径")
	}
	caPath := filepath.Clean(target.TLSCAFile)
	info, err := os.Stat(caPath)
	if err != nil {
		return fmt.Errorf("读取 workspace tls_ca_file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("workspace tls_ca_file 必须是普通文件")
	}
	if cfg.Net != "tcp" {
		return fmt.Errorf("workspace MySQL 只允许 tcp，当前 network=%q", cfg.Net)
	}
	host, _, err := net.SplitHostPort(cfg.Addr)
	if err != nil || strings.TrimSpace(host) == "" {
		return fmt.Errorf("workspace MySQL DSN 地址 %q 无法提取 TLS 主机名", cfg.Addr)
	}
	caPEM, err := readBoundedFile(caPath)
	if err != nil {
		return fmt.Errorf("读取 workspace CA PEM: %w", err)
	}
	// workspace bundle CA 是唯一信任锚；不能混入系统根，否则同名但非 bundle
	// 签发的证书也可能通过，破坏策划中心 MySQL 的私有 PKI 边界。
	roots := x509.NewCertPool()
	if err := appendStrictCACertificates(roots, caPEM); err != nil {
		return err
	}

	keyMaterial := append(append([]byte(host), 0), caPEM...)
	keyHash := sha256.Sum256(keyMaterial)
	tlsConfigName := fmt.Sprintf("pandora-workspace-%x", keyHash)
	if err := mysql.RegisterTLSConfig(tlsConfigName, &tls.Config{
		RootCAs:    roots,
		ServerName: host,
		MinVersion: tls.VersionTLS12,
	}); err != nil {
		return fmt.Errorf("注册 workspace MySQL TLS 配置: %w", err)
	}
	// FormatDSN 只能序列化注册名，不能携带 *tls.Config。清掉 ParseDSN 为 tls=true
	// 临时生成的 config，后续 sql.Open 重新解析 DSN 时会按唯一注册名取严格配置。
	cfg.TLS = nil
	cfg.TLSConfig = tlsConfigName
	cfg.AllowFallbackToPlaintext = false
	return nil
}

func appendStrictCACertificates(roots *x509.CertPool, raw []byte) error {
	remaining := bytes.TrimSpace(raw)
	if len(remaining) == 0 {
		return errors.New("workspace CA PEM 为空")
	}
	certificates := 0
	for len(remaining) > 0 {
		if !bytes.HasPrefix(remaining, []byte("-----BEGIN CERTIFICATE-----")) {
			return errors.New("workspace CA PEM 含非证书内容")
		}
		block, rest := pem.Decode(remaining)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return errors.New("workspace CA PEM 格式非法")
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return fmt.Errorf("解析 workspace CA PEM: %w", err)
		}
		if !certificate.IsCA || !certificate.BasicConstraintsValid {
			return errors.New("workspace CA PEM 必须只包含有效 CA 证书")
		}
		certificates++
		remaining = bytes.TrimSpace(rest)
	}
	if certificates == 0 || !roots.AppendCertsFromPEM(raw) {
		return errors.New("workspace CA PEM 未包含可用 CA 证书")
	}
	return nil
}

func verifyTarget(target migrationTarget, requireTLS bool) (err error) {
	log.Printf("[migrate] 目标=%s database=%s verify-only 开始", target.Name, target.Database)
	cfg, err := readVerifyOnlyDSN(target.DSNFile, target, requireTLS)
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("打开 verify-only 数据库连接: %w", err)
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), statementTimeout(target))
	defer cancel()
	if err := verifyTargetConnection(ctx, db, target, cfg.User); err != nil {
		return err
	}
	log.Printf("[migrate] 目标=%s verify-only 通过 version=%d dirty=false TLS=verified",
		target.Name, target.expectedMigrationVersion)
	return nil
}

func readVerifyOnlyDSN(path string, target migrationTarget, requireTLS bool) (*mysql.Config, error) {
	cfg, err := readAndHardenDSN(path, target, requireTLS)
	if err != nil {
		return nil, err
	}
	// go-sql-driver/mysql 会把 Params 编成连接初始化 SET。迁移模式需要有限锁等待参数，
	// verify-only 则必须只握手、Ping 和执行固定读查询，因此清掉全部 session variables。
	cfg.Params = nil
	cfg.MultiStatements = false
	return cfg, nil
}

func verifyTargetConnection(ctx context.Context, db *sql.DB, target migrationTarget, expectedUser string) error {
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("verify-only Ping 失败: %w", err)
	}

	var currentDatabase string
	if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&currentDatabase); err != nil {
		return fmt.Errorf("verify-only SELECT DATABASE(): %w", err)
	}
	if currentDatabase != target.Database {
		return fmt.Errorf("verify-only DATABASE()=%q，与目标 database=%q 不一致", currentDatabase, target.Database)
	}

	var currentUser string
	if err := db.QueryRowContext(ctx, "SELECT CURRENT_USER()").Scan(&currentUser); err != nil {
		return fmt.Errorf("verify-only SELECT CURRENT_USER(): %w", err)
	}
	separator := strings.LastIndex(currentUser, "@")
	if separator <= 0 {
		return fmt.Errorf("verify-only CURRENT_USER()=%q 格式非法", currentUser)
	}
	authenticatedUser := currentUser[:separator]
	if authenticatedUser != expectedUser {
		return fmt.Errorf("verify-only CURRENT_USER() 认证账号=%q，与 DSN user=%q 不一致", authenticatedUser, expectedUser)
	}

	var tlsVariable, tlsCipher string
	if err := db.QueryRowContext(ctx, "SHOW SESSION STATUS LIKE 'Ssl_cipher'").Scan(&tlsVariable, &tlsCipher); err != nil {
		return fmt.Errorf("verify-only 读取 TLS cipher: %w", err)
	}
	if !strings.EqualFold(tlsVariable, "Ssl_cipher") || strings.TrimSpace(tlsCipher) == "" {
		return fmt.Errorf("verify-only TLS cipher 为空或状态项异常: name=%q value=%q", tlsVariable, tlsCipher)
	}

	rows, err := db.QueryContext(ctx, "SELECT `version`, `dirty` FROM `schema_migrations`")
	if err != nil {
		return fmt.Errorf("verify-only 读取 schema_migrations: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fmt.Errorf("verify-only 读取 schema_migrations: %w", err)
		}
		return errors.New("verify-only schema_migrations 缺少版本行")
	}
	var version uint64
	var dirty bool
	if err := rows.Scan(&version, &dirty); err != nil {
		return fmt.Errorf("verify-only 解析 schema_migrations: %w", err)
	}
	if rows.Next() {
		return errors.New("verify-only schema_migrations 出现多行，拒绝猜测当前版本")
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("verify-only 读取 schema_migrations: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("verify-only 关闭 schema_migrations 结果集: %w", err)
	}
	if dirty {
		return fmt.Errorf("verify-only schema_migrations version=%d dirty=true", version)
	}
	if version != uint64(target.expectedMigrationVersion) {
		return fmt.Errorf("verify-only schema_migrations version=%d，内嵌 latest=%d",
			version, target.expectedMigrationVersion)
	}
	return nil
}

func migrateTarget(target migrationTarget, requireTLS bool) error {
	log.Printf("[migrate] 目标=%s migration_set=%s database=%s expected_version=%d 开始",
		target.Name, target.MigrationSet, target.Database, target.expectedMigrationVersion)
	cfg, err := readAndHardenDSN(target.DSNFile, target, requireTLS)
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("打开数据库连接: %w", err)
	}
	// golang-migrate 使用单连接即可；限制连接数也避免 session 级安全参数漂移。
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	pingCtx, cancel := context.WithTimeout(context.Background(), connectionTimeout)
	err = db.PingContext(pingCtx)
	cancel()
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("数据库连接预检: %w", err)
	}

	driver, err := migratemysql.WithInstance(db, &migratemysql.Config{
		MigrationsTable:  schemaMigrationsTable,
		DatabaseName:     target.Database,
		StatementTimeout: statementTimeout(target),
	})
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("构造 MySQL migration driver: %w", err)
	}
	source, err := iofs.New(migrationsFS, "migrations/"+target.MigrationSet)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("加载迁移集: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", source, target.Name, driver)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("构造迁移器: %w", err)
	}
	// golang-migrate 的 MySQL driver 内部 GET_LOCK 固定等待 10 秒。wrapper 必须比它长，
	// 否则 wrapper 先返回时底层 goroutine 仍占同一连接，后续 Version 查询会并发撞连接。
	m.LockTimeout = advisoryLockTimeout(target)

	if _, err := repairPandoraPlayerV7Dirty(target, driver, cfg); err != nil {
		closeMigration(m)
		return err
	}
	if err := rejectDirtyOrNewer(m, target.expectedMigrationVersion); err != nil {
		closeMigration(m)
		return err
	}
	applyErr := m.Up()
	if errors.Is(applyErr, migrate.ErrLockTimeout) {
		// 此时底层 Lock goroutine 可能仍在 I/O；不要再对同一连接 Version/Close。
		// migrateTarget 只在专用 worker 进程中运行，返回后进程立即退出并由 OS 回收连接。
		return fmt.Errorf("等待 migration advisory lock 超时，worker 停止且发布阻断: %w", applyErr)
	}
	// 首次从 nil/v4/v6 升级时，已发布 000007 可能在本次 Up 内才触发 MySQL 1845。
	// 只允许同一个精确 quarantine 验证并收敛该 dirty 中间态，再重试正常 Up；其他
	// 任意迁移失败仍原样阻断，绝不借此变成通用自动 force。
	if applyErr != nil && !errors.Is(applyErr, migrate.ErrNoChange) {
		repaired, repairErr := repairPandoraPlayerV7Dirty(target, driver, cfg)
		if repairErr != nil {
			closeMigration(m)
			return fmt.Errorf("执行 up 失败且 quarantine 拒绝（原始错误=%v）: %w", applyErr, repairErr)
		}
		if repaired {
			applyErr = m.Up()
			if errors.Is(applyErr, migrate.ErrLockTimeout) {
				return fmt.Errorf("quarantine 后等待 migration advisory lock 超时，worker 停止且发布阻断: %w", applyErr)
			}
		}
	}
	version, dirty, versionErr := m.Version()
	closeErr := closeMigration(m)
	if applyErr != nil && !errors.Is(applyErr, migrate.ErrNoChange) {
		if versionErr == nil {
			return fmt.Errorf("执行 up 失败（当前 version=%d dirty=%v）: %w", version, dirty, applyErr)
		}
		return fmt.Errorf("执行 up 失败且无法读取版本（version_err=%v）: %w", versionErr, applyErr)
	}
	if err := validateFinalVersion(version, dirty, versionErr, target.expectedMigrationVersion); err != nil {
		return err
	}
	if closeErr != nil {
		return fmt.Errorf("关闭迁移连接: %w", closeErr)
	}
	log.Printf("[migrate] 目标=%s 验收通过 version=%d dirty=false", target.Name, version)
	return nil
}

// repairPandoraPlayerV7Dirty 是一次性、精确形态的 quarantine，不是通用 force。
//
// 已发布 000007 在 MySQL 8.4 上会先成功创建 player_mmr、给 mmr_history 加池列，
// 随后因 `DROP players.mmr, ALGORITHM=INSTANT` 不受支持而报 1845。golang-migrate
// 此时留下 version=7 dirty=true；000008 无法越过 dirty 状态。本函数仅在以下条件全部
// 成立时把它修成 expand 终态并标 v7 clean，再由普通 Up 跑 000008：
//   - migration_set / database 精确是 pandora_player；
//   - schema_migrations 精确为 7 dirty；
//   - 镜像内 000007 正文 SHA-256 与已发布值一致；
//   - 中间 schema 精确命中“新表/新列已成功，旧列/旧索引仍在”的 1845 后置形态。
//
// 任一偏差都 fail-closed，禁止任意 skip / 任意 force。修复全程持同一 migration
// advisory lock，避免另一个 Job 在校验与标 clean 之间并发介入。
func repairPandoraPlayerV7Dirty(target migrationTarget, driver migrationRepairDriver, cfg *mysql.Config) (repaired bool, err error) {
	version, dirty, verr := driver.Version()
	if verr != nil {
		return false, fmt.Errorf("pandora_player v7 quarantine 读取版本: %w", verr)
	}
	if !dirty || version != 7 {
		return false, nil
	}
	if !validPandoraPlayerV7QuarantineTarget(target) {
		return false, fmt.Errorf("迁移前验收失败: version=7 dirty=true，quarantine 仅允许 pandora_player 精确目标，禁止自动 force")
	}
	if target.expectedMigrationVersion < 8 {
		return false, fmt.Errorf("pandora_player v7 quarantine 要求镜像至少含 000008，当前 expected_version=%d", target.expectedMigrationVersion)
	}
	if err := verifyEmbeddedPandoraPlayerV7(); err != nil {
		return false, err
	}
	if err := driver.Lock(); err != nil {
		return false, fmt.Errorf("pandora_player v7 quarantine 获取 migration lock: %w", err)
	}
	defer func() {
		if unlockErr := driver.Unlock(); unlockErr != nil {
			err = errors.Join(err, fmt.Errorf("pandora_player v7 quarantine 释放 migration lock: %w", unlockErr))
		}
	}()

	// 拿锁后重新读版本，锁外观察只能用于快路径，不能作为修复授权依据。
	version, dirty, verr = driver.Version()
	if verr != nil {
		return false, fmt.Errorf("pandora_player v7 quarantine 锁内读取版本: %w", verr)
	}
	if version != 7 || !dirty {
		return false, fmt.Errorf("pandora_player v7 quarantine 锁内版本漂移: version=%d dirty=%v", version, dirty)
	}
	// migratemysql driver 独占主 *sql.DB 的单连接；另开同 DSN 的只读校验连接，
	// advisory lock 已阻止其它迁移 Job 介入，且所有 session 安全参数仍由同一 cfg 带入。
	checkDB, openErr := sql.Open("mysql", cfg.FormatDSN())
	if openErr != nil {
		return false, fmt.Errorf("pandora_player v7 quarantine 打开校验连接: %w", openErr)
	}
	defer func() { err = errors.Join(err, checkDB.Close()) }()
	if err := validatePandoraPlayerV7DirtyShape(checkDB); err != nil {
		return false, fmt.Errorf("pandora_player v7 quarantine 形态拒绝: %w", err)
	}
	if err := driver.SetVersion(7, false); err != nil {
		return false, fmt.Errorf("pandora_player v7 quarantine 标记 clean: %w", err)
	}
	log.Printf("[migrate] 目标=%s 命中 pandora_player 000007/MySQL-1845 精确 quarantine，已验证中间 schema 并标记 version=7 dirty=false；继续 000008 expand", target.Name)
	return true, nil
}

func validPandoraPlayerV7QuarantineTarget(target migrationTarget) bool {
	if target.MigrationSet != "pandora_player" {
		return false
	}
	if target.Database == "pandora_player" {
		return true
	}
	if _, ok := workspacedb.ParsePhysicalDatabaseName("pandora_player", target.Database); ok {
		return true
	}
	return pandoraPlayerMigrationIntegrationDatabasePattern.MatchString(target.Database)
}

func verifyEmbeddedPandoraPlayerV7() error {
	raw, err := fs.ReadFile(migrationsFS,
		"migrations/pandora_player/000007_rating_pool_partition.up.sql")
	if err != nil {
		return fmt.Errorf("pandora_player v7 quarantine 读取已发布迁移: %w", err)
	}
	normalized := strings.ReplaceAll(string(raw), "\r\n", "\n")
	got := fmt.Sprintf("%x", sha256.Sum256([]byte(normalized)))
	if got != pandoraPlayerV7SHA256 {
		return fmt.Errorf("pandora_player v7 quarantine 校验和不匹配: got=%s want=%s，禁止自动修复", got, pandoraPlayerV7SHA256)
	}
	return nil
}

func validatePandoraPlayerV7DirtyShape(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), connectionTimeout)
	defer cancel()

	wantColumns := []struct {
		table, column, dataType, columnType, nullable, columnDefault string
	}{
		{"players", "mmr", "int", "int", "NO", "1500"},
		{"mmr_history", "rating_pool", "varchar", "varchar(32)", "NO", "default"},
		{"player_mmr", "player_id", "bigint", "bigint unsigned", "NO", ""},
		{"player_mmr", "rating_pool", "varchar", "varchar(32)", "NO", ""},
		{"player_mmr", "mmr", "int", "int", "NO", "1500"},
	}
	for _, want := range wantColumns {
		var dataType, columnType, nullable string
		var columnDefault sql.NullString
		err := db.QueryRowContext(ctx, `SELECT DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`, want.table, want.column).
			Scan(&dataType, &columnType, &nullable, &columnDefault)
		if err != nil {
			return fmt.Errorf("缺列 %s.%s: %w", want.table, want.column, err)
		}
		gotDefault := ""
		if columnDefault.Valid {
			gotDefault = columnDefault.String
		}
		if !strings.EqualFold(dataType, want.dataType) ||
			!strings.EqualFold(columnType, want.columnType) || nullable != want.nullable || gotDefault != want.columnDefault {
			return fmt.Errorf("列 %s.%s 形态=%s/%s nullable=%s default=%q，期望=%s/%s/%s/%q",
				want.table, want.column, dataType, columnType, nullable, gotDefault,
				want.dataType, want.columnType, want.nullable, want.columnDefault)
		}
	}

	wantIndexes := []struct {
		table, index, columns string
	}{
		{"players", "idx_mmr", "mmr"},
		{"player_mmr", "PRIMARY", "player_id,rating_pool"},
		{"player_mmr", "idx_pool_mmr", "rating_pool,mmr"},
	}
	for _, want := range wantIndexes {
		var columns sql.NullString
		if err := db.QueryRowContext(ctx, `SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',')
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?`, want.table, want.index).
			Scan(&columns); err != nil {
			return fmt.Errorf("读取索引 %s.%s: %w", want.table, want.index, err)
		}
		if !columns.Valid || !strings.EqualFold(columns.String, want.columns) {
			return fmt.Errorf("索引 %s.%s 列=%q，期望=%q", want.table, want.index, columns.String, want.columns)
		}
	}

	// 列与索引形态**不足以**认定这是 1845 中间态:000008 的终态 schema 与它逐列逐索引完全
	// 相同(000008 恰好把 000007 想删的列和索引原样加了回来)。唯一能把两者分开的事实是数据——
	// 000007 只 CREATE player_mmr 而**不回填**(存量段位重置口径,见 PROGRESS 2026-08-11),
	// 所以真正的 1845 中间态下 player_mmr 恒为 0 行。
	//
	// 少了这一条,任何"schema 已是 v8、schema_migrations 却被弄回 (7,dirty)"的库(备份恢复、
	// 跨环境拷库时连 schema_migrations 一起拷、人工 `migrate force 7`)都会被判成中间态并标
	// clean,随后 000008 的 `UPDATE players p JOIN player_mmr pm ... SET p.mmr = pm.mmr`
	// 不再是 0 行,而是把**滞后的影子值**盖回 expand 期指定给旧 Stable 副本读写的兼容权威
	// players.mmr —— 即提前触发 INC-20260812-001 A-3 的段位回退,且是静默的。
	//
	// player_mmr 非空时一律 fail-closed:那正是"无法确定该库处于哪个状态"的情形,
	// 交人处置远好过自动改玩家段位。
	var poolRows int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `player_mmr`").Scan(&poolRows); err != nil {
		return fmt.Errorf("读取 player_mmr 行数: %w", err)
	}
	if poolRows != 0 {
		return fmt.Errorf("player_mmr 已有 %d 行，不是 000007/1845 中间态"+
			"(该迁移不回填,中间态恒 0 行);此库更可能是 schema 已到 v8 而 schema_migrations 被弄回 (7,dirty)。"+
			"继续 quarantine 会让 000008 的兼容回填用滞后的 player_mmr 值覆写 players.mmr(段位静默回退),"+
			"故 fail-closed 交人处置", poolRows)
	}
	return nil
}

func rejectDirtyOrNewer(reader migrationVersionReader, expected uint) error {
	version, dirty, err := reader.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("迁移前读取 schema_migrations: %w", err)
	}
	if dirty {
		return fmt.Errorf("迁移前验收失败: version=%d dirty=true，禁止自动 force", version)
	}
	if version > expected {
		return fmt.Errorf("数据库 version=%d 高于迁移镜像 version=%d，禁止旧镜像继续发布", version, expected)
	}
	return nil
}

func validateFinalVersion(version uint, dirty bool, versionErr error, expected uint) error {
	if versionErr != nil {
		return fmt.Errorf("迁移后读取 schema_migrations: %w", versionErr)
	}
	if dirty {
		return fmt.Errorf("迁移后验收失败: version=%d dirty=true", version)
	}
	if version != expected {
		return fmt.Errorf("迁移后验收失败: version=%d，镜像期望=%d", version, expected)
	}
	return nil
}

func closeMigration(m *migrate.Migrate) error {
	sourceErr, databaseErr := m.Close()
	return errors.Join(sourceErr, databaseErr)
}

func runTargetWithDeadline(
	targetsFile, environment, workspaceID string,
	verifyOnly bool,
	expectedTargets []targetDescriptor,
	target migrationTarget,
) error {
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("定位迁移器可执行文件: %w", err)
	}
	timeout := time.Duration(target.TimeoutSeconds) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, workerCommandArgs(
		targetsFile, environment, workspaceID, verifyOnly, expectedTargets, target,
	)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("目标 %s 超过硬时限 %s，worker 已终止，发布必须阻断", target.Name, timeout)
		}
		return fmt.Errorf("目标 %s worker 失败: %w", target.Name, err)
	}
	return nil
}

func workerCommandArgs(
	targetsFile, environment, workspaceID string,
	verifyOnly bool,
	expectedTargets []targetDescriptor,
	target migrationTarget,
) []string {
	workerArgs := []string{
		"-targets-file", targetsFile,
		"-expected-targets", formatExpectedTargetInventory(expectedTargets),
		"-environment", environment,
		"-bootstrap=false",
		"-verify-only=" + strconv.FormatBool(verifyOnly),
		"-worker-target", target.Name,
	}
	if workspaceID != "" {
		workerArgs = append(workerArgs, "-workspace-id", workspaceID)
	}
	return workerArgs
}

func statementTimeout(target migrationTarget) time.Duration {
	timeout := time.Duration(target.TimeoutSeconds) * time.Second
	// 给驱动取消、写入 dirty/version 状态和关闭连接留出确定窗口，避免 SQL 超时与父进程
	// 硬杀发生在同一瞬间。
	if timeout > workerShutdownGracePeriod {
		return timeout - workerShutdownGracePeriod
	}
	return time.Second
}

func advisoryLockTimeout(target migrationTarget) time.Duration {
	configured := time.Duration(target.LockWaitTimeoutSeconds) * time.Second
	minimum := mysqlAdvisoryLockWait + advisoryLockWrapperGrace
	if configured < minimum {
		return minimum
	}
	return configured
}

// bootstrapTargets 只供显式 local/dev 使用。管理员 DSN 与迁移 DSN 分文件，且管理员
// DSN 必须指向同一实例并且不能带 database，防止误连其它生产实例。
func bootstrapTargets(targets []migrationTarget) error {
	for _, target := range targets {
		migrationCfg, err := readAndHardenDSN(target.DSNFile, target, false)
		if err != nil {
			return fmt.Errorf("目标 %s 读取迁移 DSN: %w", target.Name, err)
		}
		if !mysqlAccountPattern.MatchString(migrationCfg.User) {
			return fmt.Errorf("目标 %s 的迁移账号名 %q 不满足 bootstrap 安全字符约束", target.Name, migrationCfg.User)
		}
		adminCfg, err := readAdminDSN(target.BootstrapAdminDSNFile)
		if err != nil {
			return fmt.Errorf("目标 %s 读取管理员 DSN: %w", target.Name, err)
		}
		if adminCfg.Net != migrationCfg.Net || adminCfg.Addr != migrationCfg.Addr {
			return fmt.Errorf("目标 %s 的管理员 DSN 与迁移 DSN 不在同一实例", target.Name)
		}
		if err := bootstrapTarget(adminCfg, target.Database, migrationCfg.User); err != nil {
			return fmt.Errorf("目标 %s: %w", target.Name, err)
		}
		log.Printf("[migrate] dev bootstrap 目标=%s database=%s 完成", target.Name, target.Database)
	}
	return nil
}

func readAdminDSN(path string) (*mysql.Config, error) {
	data, err := readBoundedFile(path)
	if err != nil {
		return nil, err
	}
	cfg, err := mysql.ParseDSN(strings.TrimSpace(string(data)))
	if err != nil {
		return nil, fmt.Errorf("管理员 DSN 格式错误: %w", err)
	}
	if cfg.User == "" {
		return nil, errors.New("管理员 DSN 缺少账号")
	}
	if cfg.DBName != "" {
		return nil, errors.New("管理员 DSN 不得指定 database")
	}
	cfg.MultiStatements = false
	cfg.Timeout = connectionTimeout
	cfg.ReadTimeout = 30 * time.Second
	cfg.WriteTimeout = 30 * time.Second
	return cfg, nil
}

func bootstrapTarget(adminCfg *mysql.Config, database, migrationUser string) error {
	db, err := sql.Open("mysql", adminCfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("打开管理员连接: %w", err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("管理员连接预检: %w", err)
	}
	createStatement := fmt.Sprintf(
		"CREATE DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_0900_ai_ci",
		database,
	)
	if _, err := db.ExecContext(ctx, createStatement); err != nil {
		return fmt.Errorf("CREATE DATABASE: %w", err)
	}
	// 不创建账号、不设置密码，只给 dev 中已存在的迁移账号授予 Up 所需权限。
	grantStatement := fmt.Sprintf(
		"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, REFERENCES ON `%s`.* TO '%s'@'%%'",
		database, migrationUser,
	)
	if _, err := db.ExecContext(ctx, grantStatement); err != nil {
		return fmt.Errorf("GRANT 迁移最小权限: %w", err)
	}
	return nil
}

func findTarget(targets []migrationTarget, name string) (migrationTarget, bool) {
	for _, target := range targets {
		if target.Name == name {
			return target, true
		}
	}
	return migrationTarget{}, false
}

func resolveSecretPath(base, path string) (string, error) {
	path = strings.TrimSpace(path)
	resolved := path
	if !filepath.IsAbs(resolved) {
		resolved = filepath.Join(base, resolved)
	}
	resolved = filepath.Clean(resolved)
	relative, err := filepath.Rel(filepath.Clean(base), resolved)
	if err != nil {
		return "", fmt.Errorf("解析路径: %w", err)
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return "", errors.New("凭据文件必须位于目标清单所在目录或其子目录")
	}
	return resolved, nil
}

func readBoundedFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maximumSecretFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maximumSecretFileBytes {
		return nil, fmt.Errorf("文件超过 %d 字节上限", maximumSecretFileBytes)
	}
	return data, nil
}

func isDevelopmentEnvironment(environment string) bool {
	switch strings.ToLower(strings.TrimSpace(environment)) {
	case "local", "dev", "development":
		return true
	default:
		return false
	}
}

func requiresVerifiedTLS(environment, workspaceID string) bool {
	return workspaceID != "" || !isDevelopmentEnvironment(environment)
}

func strictEnvBool(key string, fallback bool) (bool, error) {
	value, exists := os.LookupEnv(key)
	if !exists || strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return false, fmt.Errorf("环境变量 %s 必须是 true/false: %w", key, err)
	}
	return parsed, nil
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
