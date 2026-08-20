// Package mysqlx 提供 Pandora 服务的 MySQL 客户端工厂。
//
// 设计:
//   - 用标准 database/sql + github.com/go-sql-driver/mysql,**不引 ORM**(W2/W3 轻量)
//   - 业务 data 层自己写 SQL,通过 *sql.DB 调 QueryContext / ExecContext
//   - DSN 在业务 yaml 配置;连接池参数有 Pandora 默认值
//
// 用法:
//
//	db := mysqlx.MustNewClient(cfg.Node.MySQLClient)
//	defer db.Close()
//	row := db.QueryRowContext(ctx, "SELECT player_id FROM accounts WHERE account = ?", account)
//
// 故意不在本包导出 Repo 抽象,各业务自己定义 Repo 接口,只把 *sql.DB 当依赖。
package mysqlx

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/luyuancpp/pandora/pkg/config"
)

// 默认连接池参数(开发环境用,prod 应在 yaml 调整)。
const (
	defaultMaxOpenConns    = 32
	defaultMaxIdleConns    = 8
	defaultConnMaxLifetime = 30 * time.Minute
	defaultPingTimeout     = 3 * time.Second
	maxTLSCAFileBytes      = 1 << 20
)

var tlsConfigSequence atomic.Uint64

// MustNewClient 用 config.MySQLConf 构造 *sql.DB,失败 panic。
//
// 启动期会做一次 PingContext 验证连通性(超时 3s)。
// DSN 示例:`pandora:pandora_dev_pwd@tcp(127.0.0.1:3307)/pandora_account?parseTime=true&loc=UTC&charset=utf8mb4&collation=utf8mb4_0900_ai_ci`
func MustNewClient(c config.MySQLConf) *sql.DB {
	db, err := NewClient(c)
	if err != nil {
		panic(fmt.Sprintf("mysqlx.MustNewClient: %v", err))
	}
	return db
}

// NewClient 构造 *sql.DB 并 Ping 验证。
func NewClient(c config.MySQLConf) (*sql.DB, error) {
	if c.DSN == "" {
		return nil, fmt.Errorf("mysql DSN is empty")
	}
	tlsCAFile := strings.TrimSpace(c.TLSCAFile)
	tlsServerName := strings.TrimSpace(c.TLSServerName)
	if (tlsCAFile == "") != (tlsServerName == "") {
		return nil, fmt.Errorf("mysql tls_ca_file and tls_server_name must be configured together")
	}
	driverCfg, err := mysql.ParseDSN(c.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse mysql DSN: %w", err)
	}
	if mode := strings.ToLower(driverCfg.TLSConfig); mode == "skip-verify" || mode == "preferred" ||
		driverCfg.AllowFallbackToPlaintext || unsafeTLSConfig(driverCfg.TLS) {
		return nil, fmt.Errorf("mysql DSN contains unsafe tls mode %q", driverCfg.TLSConfig)
	}
	if tlsCAFile != "" {
		if driverCfg.Net != "tcp" {
			return nil, fmt.Errorf("mysql TLS requires tcp DSN, got network %q", driverCfg.Net)
		}
		host, _, splitErr := net.SplitHostPort(driverCfg.Addr)
		if splitErr != nil {
			return nil, fmt.Errorf("parse mysql tcp address %q: %w", driverCfg.Addr, splitErr)
		}
		if !strings.EqualFold(host, tlsServerName) {
			return nil, fmt.Errorf("mysql DSN host %q must equal tls_server_name %q", host, tlsServerName)
		}
	}

	var db *sql.DB
	if tlsCAFile == "" {
		db, err = sql.Open("mysql", c.DSN)
		if err != nil {
			return nil, fmt.Errorf("sql.Open: %w", err)
		}
	} else {
		if driverCfg.TLSConfig != "" || driverCfg.TLS != nil {
			return nil, fmt.Errorf("mysql DSN must not contain tls parameter when tls_ca_file/tls_server_name are configured")
		}
		rootCAs, rootErr := loadStrictRootCAs(tlsCAFile)
		if rootErr != nil {
			return nil, fmt.Errorf("mysql tls_ca_file %q: %w", tlsCAFile, rootErr)
		}
		configKey := fmt.Sprintf("pandora-strict-%d", tlsConfigSequence.Add(1))
		if registerErr := mysql.RegisterTLSConfig(configKey, &tls.Config{
			MinVersion:         tls.VersionTLS12,
			RootCAs:            rootCAs,
			ServerName:         tlsServerName,
			InsecureSkipVerify: false,
		}); registerErr != nil {
			return nil, fmt.Errorf("register strict mysql TLS config: %w", registerErr)
		}
		driverCfg.TLSConfig = configKey
		connector, connectorErr := mysql.NewConnector(driverCfg)
		mysql.DeregisterTLSConfig(configKey)
		if connectorErr != nil {
			return nil, fmt.Errorf("create mysql TLS connector: %w", connectorErr)
		}
		db = sql.OpenDB(connector)
	}

	maxOpen := c.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = defaultMaxOpenConns
	}
	maxIdle := c.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = defaultMaxIdleConns
	}
	maxLife := c.ConnMaxLifetime.Std()
	if maxLife <= 0 {
		maxLife = defaultConnMaxLifetime
	}

	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(maxLife)
	// 门禁-B:opt-in 空闲连接上限,留空(0)沿用 database/sql 默认(不因空闲回收)。
	if maxIdleTime := c.ConnMaxIdleTime.Std(); maxIdleTime > 0 {
		db.SetConnMaxIdleTime(maxIdleTime)
	}

	pingTimeout := c.PingTimeout.Std()
	if pingTimeout <= 0 {
		pingTimeout = defaultPingTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return db, nil
}

func unsafeTLSConfig(cfg *tls.Config) bool {
	if cfg == nil {
		return false
	}
	return cfg.InsecureSkipVerify ||
		(cfg.MinVersion != 0 && cfg.MinVersion < tls.VersionTLS12) ||
		(cfg.MaxVersion != 0 && cfg.MaxVersion < tls.VersionTLS12)
}

// loadStrictRootCAs 只信中心 MySQL bundle 的私有 CA。不能从 SystemCertPool 起步：
// 否则同名证书只要被任意系统根签发也会通过，bundle 就不再是唯一信任锚。
// 文件中任何非证书、损坏证书或尾随垃圾都拒绝，避免
// AppendCertsFromPEM“至少成功一个就返回 true”掩盖坏包。
func loadStrictRootCAs(path string) (*x509.CertPool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("must be a regular file")
	}
	if info.Size() > maxTLSCAFileBytes {
		return nil, fmt.Errorf("size %d exceeds %d-byte limit", info.Size(), maxTLSCAFileBytes)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	pemBytes, err := io.ReadAll(io.LimitReader(file, maxTLSCAFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(pemBytes) > maxTLSCAFileBytes {
		return nil, fmt.Errorf("content exceeds %d-byte limit", maxTLSCAFileBytes)
	}
	rest := pemBytes
	certCount := 0
	for len(bytes.TrimSpace(rest)) > 0 {
		block, next := pem.Decode(rest)
		if block == nil {
			return nil, fmt.Errorf("contains data that is not a PEM certificate")
		}
		if block.Type != "CERTIFICATE" {
			return nil, fmt.Errorf("contains PEM block %q, want CERTIFICATE", block.Type)
		}
		if _, parseErr := x509.ParseCertificate(block.Bytes); parseErr != nil {
			return nil, fmt.Errorf("parse certificate: %w", parseErr)
		}
		certCount++
		rest = next
	}
	if certCount == 0 {
		return nil, fmt.Errorf("contains no certificate")
	}

	pool := x509.NewCertPool()
	if ok := pool.AppendCertsFromPEM(pemBytes); !ok {
		return nil, fmt.Errorf("append certificate to bundle-only pool")
	}
	return pool, nil
}
