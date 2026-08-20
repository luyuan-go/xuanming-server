package main

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/luyuancpp/pandora/tools/migrate/workspacedb"
	"github.com/luyuancpp/pandora/tools/plannerdb-provisioner/internal/plannerdb"
)

const (
	defaultAdminDSNRef  = "env:PANDORA_PLANNERDB_ADMIN_DSN"
	defaultMySQLCARef   = "env:PANDORA_PLANNERDB_MYSQL_CA_PEM"
	defaultMasterRef    = "env:PANDORA_PLANNERDB_MASTER_KEY"
	defaultHTTPSCertRef = "env:PANDORA_PLANNERDB_HTTPS_CERT_PEM"
	defaultHTTPSKeyRef  = "env:PANDORA_PLANNERDB_HTTPS_KEY_PEM"
)

var errTokenOutputFailed = errors.New("一次性 code 输出失败")

type commonFlags struct {
	adminDSNRef      string
	mysqlCARef       string
	databaseHost     string
	databasePort     uint
	tlsServerName    string
	registryDatabase string
	registryMarker   string
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(arguments []string, stdout, stderr io.Writer) int {
	if len(arguments) == 0 {
		fmt.Fprintln(stderr, "用法: pandora-plannerdb-provisioner <init-registry|serve|issue-token|issue-recovery-token|retry-workspace|audit> [flags]")
		return 2
	}
	switch arguments[0] {
	case "init-registry":
		return runInitRegistry(arguments[1:], stdout, stderr)
	case "serve":
		return runServe(arguments[1:], stderr)
	case "issue-token":
		return runIssueToken(arguments[1:], stdout, stderr)
	case "issue-recovery-token":
		return runIssueRecoveryToken(arguments[1:], stdout, stderr)
	case "retry-workspace":
		return runRetryWorkspace(arguments[1:], stdout, stderr)
	case "audit":
		return runAudit(arguments[1:], stdout, stderr)
	default:
		fmt.Fprintln(stderr, "未知子命令")
		return 2
	}
}

func runInitRegistry(arguments []string, stdout, stderr io.Writer) int {
	set := flag.NewFlagSet("init-registry", flag.ContinueOnError)
	set.SetOutput(stderr)
	var common commonFlags
	bindCommonFlags(set, &common)
	if err := set.Parse(arguments); err != nil || set.NArg() != 0 {
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	backend, ca, err := loadBackend(ctx, common, true)
	if err != nil {
		fmt.Fprintln(stderr, "[ERR] init-registry 失败")
		return 1
	}
	defer plannerdb.ZeroBytes(ca)
	if err := backend.Close(); err != nil {
		fmt.Fprintln(stderr, "[ERR] init-registry 关闭连接失败")
		return 1
	}
	response := struct {
		SchemaVersion  int    `json:"schema_version"`
		Database       string `json:"database"`
		InstanceMarker string `json:"instance_marker"`
	}{SchemaVersion: 1, Database: common.registryDatabase, InstanceMarker: common.registryMarker}
	if err := json.NewEncoder(stdout).Encode(response); err != nil {
		fmt.Fprintln(stderr, "[ERR] init-registry JSON 输出失败")
		return 1
	}
	return 0
}

func runRetryWorkspace(arguments []string, stdout, stderr io.Writer) int {
	set := flag.NewFlagSet("retry-workspace", flag.ContinueOnError)
	set.SetOutput(stderr)
	var common commonFlags
	bindCommonFlags(set, &common)
	workspaceID := set.String("workspace-id", "", "要从 MIGRATION_FAILED 恢复的 26 位 workspace ID")
	if err := set.Parse(arguments); err != nil || set.NArg() != 0 || strings.TrimSpace(*workspaceID) == "" {
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	backend, ca, err := loadBackend(ctx, common, false)
	if err != nil {
		fmt.Fprintf(stderr, "[ERR] retry-workspace 初始化失败: %v\n", err)
		return 1
	}
	defer plannerdb.ZeroBytes(ca)
	defer backend.Close()
	record, err := backend.RetryWorkspace(ctx, strings.TrimSpace(*workspaceID))
	if err != nil {
		fmt.Fprintf(stderr, "[ERR] retry-workspace 拒绝: %v\n", err)
		return 1
	}
	// 管理命令只输出非秘密 identity/state，不解密 credential。
	response := struct {
		SchemaVersion int    `json:"schema_version"`
		WorkspaceID   string `json:"workspace_id"`
		State         string `json:"state"`
	}{SchemaVersion: 1, WorkspaceID: record.WorkspaceID, State: string(record.State)}
	if err := json.NewEncoder(stdout).Encode(response); err != nil {
		fmt.Fprintln(stderr, "[ERR] retry-workspace JSON 输出失败")
		return 1
	}
	return 0
}

func runIssueRecoveryToken(arguments []string, stdout, stderr io.Writer) int {
	set := flag.NewFlagSet("issue-recovery-token", flag.ContinueOnError)
	set.SetOutput(stderr)
	var common commonFlags
	bindCommonFlags(set, &common)
	workspaceID := set.String("workspace-id", "", "管理员明确授权恢复的 26 位 workspace ID")
	ttl := set.Duration("ttl", 15*time.Minute, "一次性 recovery token 有效期（1m..24h）")
	if err := set.Parse(arguments); err != nil || set.NArg() != 0 || strings.TrimSpace(*workspaceID) == "" {
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	backend, ca, err := loadBackend(ctx, common, false)
	if err != nil {
		fmt.Fprintf(stderr, "[ERR] issue-recovery-token 初始化失败: %v\n", err)
		return 1
	}
	defer plannerdb.ZeroBytes(ca)
	defer backend.Close()
	token, err := backend.IssueRecoveryToken(ctx, strings.TrimSpace(*workspaceID), *ttl)
	if err != nil {
		fmt.Fprintf(stderr, "[ERR] issue-recovery-token 失败: %v\n", err)
		return 1
	}
	// 管理员把整段 recovery code 原样交给策划；客户端只在内存中拆成
	// expected_workspace_id 与 raw token。registry 仍只 hash raw token。
	if err := writeRecoveryCode(stdout, strings.TrimSpace(*workspaceID), token); err != nil {
		fmt.Fprintln(stderr, "[ERR] issue-recovery-token 输出失败")
		return 1
	}
	return 0
}

func runAudit(arguments []string, stdout, stderr io.Writer) int {
	set := flag.NewFlagSet("audit", flag.ContinueOnError)
	set.SetOutput(stderr)
	var common commonFlags
	bindCommonFlags(set, &common)
	stuckAfter := set.Duration("stuck-after", 30*time.Minute, "PROVISIONING/MIGRATING 卡住告警阈值")
	inactiveAfter := set.Duration("inactive-after", 30*24*time.Hour, "workspace 未见告警阈值（只报告不删除）")
	entryLimit := set.Int("limit", 200, "详细 workspace 最大返回数（1..10000）")
	if err := set.Parse(arguments); err != nil || set.NArg() != 0 {
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	backend, ca, err := loadBackend(ctx, common, false)
	if err != nil {
		fmt.Fprintf(stderr, "[ERR] audit 初始化失败: %v\n", err)
		return 1
	}
	defer plannerdb.ZeroBytes(ca)
	defer backend.Close()
	report, err := backend.Audit(ctx, plannerdb.AuditOptions{
		StuckAfter: *stuckAfter, InactiveAfter: *inactiveAfter, EntryLimit: *entryLimit,
	})
	if err != nil {
		fmt.Fprintf(stderr, "[ERR] audit 失败: %v\n", err)
		return 1
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		fmt.Fprintln(stderr, "[ERR] audit JSON 输出失败")
		return 1
	}
	if report.HasBlockingFindings() {
		return 3
	}
	return 0
}

func bindCommonFlags(set *flag.FlagSet, values *commonFlags) {
	set.StringVar(&values.adminDSNRef, "admin-dsn-ref", defaultAdminDSNRef, "admin DSN 的 env:/file: secret 引用")
	set.StringVar(&values.mysqlCARef, "mysql-ca-ref", defaultMySQLCARef, "MySQL CA PEM 的 env:/file: 引用")
	set.StringVar(&values.databaseHost, "db-host", "", "中心 MySQL 小写 DNS 名（禁止 IP）")
	set.UintVar(&values.databasePort, "db-port", 3306, "中心 MySQL 端口")
	set.StringVar(&values.tlsServerName, "db-tls-server-name", "", "MySQL 证书 SAN；必须与 db-host 相同")
	set.StringVar(&values.registryDatabase, "registry-database", plannerdb.DefaultRegistryDatabase, "planner registry 数据库名")
	set.StringVar(&values.registryMarker, "registry-marker", "", "显式 registry instance marker（必填）")
}

func (values commonFlags) endpoint() (plannerdb.Endpoint, error) {
	if values.databasePort == 0 || values.databasePort > 65535 {
		return plannerdb.Endpoint{}, errors.New("db-port 必须是 1..65535")
	}
	return plannerdb.Endpoint{
		Host: strings.TrimSpace(values.databaseHost), Port: uint16(values.databasePort),
		TLSServerName: strings.TrimSpace(values.tlsServerName),
	}, nil
}

func loadBackend(ctx context.Context, common commonFlags, initialize bool) (*plannerdb.MySQLBackend, []byte, error) {
	endpoint, err := common.endpoint()
	if err != nil {
		return nil, nil, err
	}
	adminDSNBytes, err := plannerdb.LoadSecretRef(common.adminDSNRef, 8<<10)
	if err != nil {
		return nil, nil, err
	}
	defer plannerdb.ZeroBytes(adminDSNBytes)
	mysqlCAPEM, err := plannerdb.LoadSecretRef(common.mysqlCARef, 1<<20)
	if err != nil {
		return nil, nil, err
	}
	backendConfig := plannerdb.BackendConfig{
		AdminDSN: strings.TrimSpace(string(adminDSNBytes)), MySQLCAPEM: mysqlCAPEM, Endpoint: endpoint,
		RegistryDatabase: strings.TrimSpace(common.registryDatabase), RegistryInstanceMarker: strings.TrimSpace(common.registryMarker),
	}
	var backend *plannerdb.MySQLBackend
	if initialize {
		backend, err = plannerdb.InitMySQLRegistry(ctx, backendConfig)
	} else {
		backend, err = plannerdb.OpenMySQLBackend(ctx, backendConfig)
	}
	if err != nil {
		plannerdb.ZeroBytes(mysqlCAPEM)
		return nil, nil, err
	}
	return backend, mysqlCAPEM, nil
}

func runIssueToken(arguments []string, stdout, stderr io.Writer) int {
	set := flag.NewFlagSet("issue-token", flag.ContinueOnError)
	set.SetOutput(stderr)
	var common commonFlags
	bindCommonFlags(set, &common)
	ttl := set.Duration("ttl", 15*time.Minute, "一次性 token 有效期（1m..24h）")
	if err := set.Parse(arguments); err != nil || set.NArg() != 0 {
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	backend, ca, err := loadBackend(ctx, common, false)
	if err != nil {
		fmt.Fprintf(stderr, "[ERR] issue-token 初始化失败: %v\n", err)
		return 1
	}
	defer plannerdb.ZeroBytes(ca)
	defer backend.Close()
	token, err := backend.IssueToken(ctx, *ttl)
	if err != nil {
		fmt.Fprintf(stderr, "[ERR] issue-token 失败: %v\n", err)
		return 1
	}
	// token 只在显式管理命令的 stdout 出现一次；日志和数据库只保存 SHA-256。
	if err := writeEnrollmentToken(stdout, token); err != nil {
		fmt.Fprintln(stderr, "[ERR] issue-token 输出失败")
		return 1
	}
	return 0
}

func writeEnrollmentToken(stdout io.Writer, rawToken string) error {
	if !isCanonicalRawToken(rawToken) {
		return errors.New("内部签发的 enrollment token 格式非法")
	}
	if _, err := fmt.Fprintln(stdout, rawToken); err != nil {
		// writer adapter 的错误文本不可信，可能夹带宿主路径或其它 secret。
		return errTokenOutputFailed
	}
	return nil
}

func writeRecoveryCode(stdout io.Writer, workspaceID, rawToken string) error {
	if workspacedb.ValidateWorkspaceID(workspaceID) != nil || !isCanonicalRawToken(rawToken) {
		return errors.New("内部签发的 recovery code 格式非法")
	}
	if _, err := fmt.Fprintf(stdout, "%s.%s\n", workspaceID, rawToken); err != nil {
		return errTokenOutputFailed
	}
	return nil
}

func isCanonicalRawToken(rawToken string) bool {
	raw, err := base64.RawURLEncoding.DecodeString(rawToken)
	return err == nil && len(raw) == 32 && base64.RawURLEncoding.EncodeToString(raw) == rawToken
}

func runServe(arguments []string, stderr io.Writer) int {
	set := flag.NewFlagSet("serve", flag.ContinueOnError)
	set.SetOutput(stderr)
	var common commonFlags
	bindCommonFlags(set, &common)
	masterRef := set.String("master-key-ref", defaultMasterRef, "32-byte master key 的 env:/file: 引用")
	httpsCertRef := set.String("https-cert-ref", defaultHTTPSCertRef, "HTTPS cert PEM 的 env:/file: 引用")
	httpsKeyRef := set.String("https-key-ref", defaultHTTPSKeyRef, "HTTPS private key PEM 的 env:/file: 引用")
	migrateBinary := set.String("migrate-binary", "", "同 release 的 pandora-migrate 绝对路径")
	migrateSHA256 := set.String("migrate-sha256", os.Getenv("PANDORA_MIGRATE_SHA256"), "同 release manifest 中的 migrate SHA256")
	listenAddress := set.String("listen", ":9443", "HTTPS 监听地址")
	migrationTimeout := set.Duration("migration-timeout", 3*time.Hour, "pandora-migrate 总硬时限")
	provisionTimeout := set.Duration("provision-timeout", 3*time.Hour+5*time.Minute, "单 workspace provisioning 总硬时限")
	maxConcurrentProvisions := set.Int("max-concurrent-provisions", 2, "并发 workspace provisioning 上限（1..32）")
	if err := set.Parse(arguments); err != nil || set.NArg() != 0 {
		return 2
	}
	if err := validateServeDurations(*migrationTimeout, *provisionTimeout); err != nil {
		fmt.Fprintf(stderr, "[ERR] 超时配置非法: %v\n", err)
		return 1
	}
	endpoint, err := common.endpoint()
	if err != nil {
		fmt.Fprintf(stderr, "[ERR] 配置非法: %v\n", err)
		return 1
	}
	adminDSNBytes, err := plannerdb.LoadSecretRef(common.adminDSNRef, 8<<10)
	if err != nil {
		fmt.Fprintf(stderr, "[ERR] admin DSN 引用不可用: %v\n", err)
		return 1
	}
	defer plannerdb.ZeroBytes(adminDSNBytes)
	mysqlCAPEM, err := plannerdb.LoadSecretRef(common.mysqlCARef, 1<<20)
	if err != nil {
		fmt.Fprintf(stderr, "[ERR] MySQL CA 引用不可用: %v\n", err)
		return 1
	}
	defer plannerdb.ZeroBytes(mysqlCAPEM)
	masterEncoded, err := plannerdb.LoadSecretRef(*masterRef, 1024)
	if err != nil {
		fmt.Fprintf(stderr, "[ERR] master key 引用不可用: %v\n", err)
		return 1
	}
	defer plannerdb.ZeroBytes(masterEncoded)
	masterKey, err := plannerdb.DecodeMasterKey(masterEncoded)
	if err != nil {
		fmt.Fprintf(stderr, "[ERR] master key 非法: %v\n", err)
		return 1
	}
	defer plannerdb.ZeroBytes(masterKey)
	certificatePEM, err := plannerdb.LoadSecretRef(*httpsCertRef, 1<<20)
	if err != nil {
		fmt.Fprintf(stderr, "[ERR] HTTPS cert 引用不可用: %v\n", err)
		return 1
	}
	defer plannerdb.ZeroBytes(certificatePEM)
	keyPEM, err := plannerdb.LoadSecretRef(*httpsKeyRef, 1<<20)
	if err != nil {
		fmt.Fprintf(stderr, "[ERR] HTTPS key 引用不可用: %v\n", err)
		return 1
	}
	defer plannerdb.ZeroBytes(keyPEM)
	tlsConfig, err := plannerdb.NewServerTLSConfig(certificatePEM, keyPEM)
	if err != nil {
		fmt.Fprintf(stderr, "[ERR] HTTPS TLS 配置非法: %v\n", err)
		return 1
	}
	if !filepathIsAbsClean(*migrateBinary) || len(*migrateSHA256) != 64 {
		fmt.Fprintln(stderr, "[ERR] migrate-binary 必须是绝对路径，migrate-sha256 必须来自同 release manifest")
		return 1
	}
	logger := log.New(stderr, "", log.Ldate|log.Ltime|log.LUTC)
	startupCtx, startupCancel := context.WithTimeout(context.Background(), time.Minute)
	defer startupCancel()
	app, err := plannerdb.NewApp(startupCtx, plannerdb.AppConfig{
		AdminDSN: strings.TrimSpace(string(adminDSNBytes)), MySQLCAPEM: mysqlCAPEM, MasterKey: masterKey,
		Endpoint: endpoint, RegistryDatabase: strings.TrimSpace(common.registryDatabase),
		RegistryInstanceMarker: strings.TrimSpace(common.registryMarker),
		MigrateBinaryPath:      *migrateBinary, MigrateBinarySHA256: strings.TrimSpace(*migrateSHA256),
		MigrationTimeout: *migrationTimeout, ProvisionTimeout: *provisionTimeout, WorkspaceLockWait: 5 * time.Second,
		Logger: logger, MaxConcurrentProvisions: *maxConcurrentProvisions,
	})
	if err != nil {
		fmt.Fprintf(stderr, "[ERR] provisioner 初始化失败: %v\n", err)
		return 1
	}
	defer app.Close()

	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		fmt.Fprintf(stderr, "[ERR] HTTPS 监听失败: %v\n", err)
		return 1
	}
	server := &http.Server{
		Handler: app.Handler(), TLSConfig: tlsConfig,
		ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 15 * time.Second,
		// enrollment 始终快速返回 202/200；数小时 provisioning 只在后台 worker 中运行。
		WriteTimeout: 30 * time.Second, IdleTimeout: time.Minute, MaxHeaderBytes: 16 << 10,
	}
	shutdownSignals := make(chan os.Signal, 1)
	signal.Notify(shutdownSignals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(shutdownSignals)
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(tls.NewListener(listener, tlsConfig)) }()
	logger.Printf("[plannerdb-provisioner] HTTPS ready listen=%s db=%s:%s", *listenAddress, endpoint.Host, strconv.Itoa(int(endpoint.Port)))
	select {
	case serveErr := <-serveErrors:
		if !errors.Is(serveErr, http.ErrServerClosed) {
			fmt.Fprintf(stderr, "[ERR] HTTPS 服务退出: %v\n", serveErr)
			return 1
		}
	case <-shutdownSignals:
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			fmt.Fprintf(stderr, "[ERR] HTTPS 优雅停止失败: %v\n", err)
			return 1
		}
	}
	return 0
}

func validateServeDurations(migrationTimeout, provisionTimeout time.Duration) error {
	if migrationTimeout < time.Minute || migrationTimeout > 4*time.Hour {
		return errors.New("migration-timeout 必须在 1 分钟到 4 小时之间")
	}
	if provisionTimeout < migrationTimeout+time.Minute || provisionTimeout > 5*time.Hour {
		return errors.New("provision-timeout 必须至少比 migration-timeout 长 1 分钟且不超过 5 小时")
	}
	return nil
}

func filepathIsAbsClean(path string) bool {
	if path == "" {
		return false
	}
	absolute, err := filepath.Abs(path)
	return err == nil && absolute == filepath.Clean(path)
}
