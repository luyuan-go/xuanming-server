package plannerdb

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
)

var mysqlTLSRegistrationSequence atomic.Uint64

const DefaultRegistryDatabase = "pandora_planner_registry"

var (
	registryDatabasePattern = regexp.MustCompile(`^pandora_planner_registry(?:_[a-z0-9]{1,32})?$`)
	registryMarkerPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

type BackendConfig struct {
	AdminDSN               string
	MySQLCAPEM             []byte
	Endpoint               Endpoint
	RegistryDatabase       string
	RegistryInstanceMarker string
	LockMonitorInterval    time.Duration
}

func validateRegistryIdentity(config BackendConfig) error {
	if !registryDatabasePattern.MatchString(config.RegistryDatabase) {
		return errors.New("registry database 必须是 pandora_planner_registry 或其安全命名的隔离后缀")
	}
	if !registryMarkerPattern.MatchString(config.RegistryInstanceMarker) {
		return errors.New("registry instance marker 必须显式提供 1..128 位安全标识")
	}
	return nil
}

func parseAdminDSN(dsn string, endpoint Endpoint) (*mysql.Config, error) {
	if err := validateEndpoint(endpoint); err != nil {
		return nil, err
	}
	config, err := mysql.ParseDSN(strings.TrimSpace(dsn))
	if err != nil {
		return nil, errors.New("admin DSN 格式非法")
	}
	if config.User == "" || config.Passwd == "" {
		return nil, errors.New("admin DSN 必须包含专用账号和密码")
	}
	if strings.EqualFold(config.User, "root") {
		return nil, errors.New("admin DSN 禁止使用 root，必须使用专用 provisioner 账号")
	}
	if config.Net != "tcp" || config.Addr != net.JoinHostPort(endpoint.Host, fmt.Sprint(endpoint.Port)) {
		return nil, errors.New("admin DSN 必须使用配置中的 exact DNS endpoint")
	}
	if config.DBName != "" {
		return nil, errors.New("admin DSN 不得预选业务数据库")
	}
	if config.TLSConfig != "true" || config.TLS == nil || config.TLS.InsecureSkipVerify || config.AllowFallbackToPlaintext {
		return nil, errors.New("admin DSN 必须使用 tls=true，禁止明文、skip-verify 与 fallback")
	}
	config.Timeout = 10 * time.Second
	config.ReadTimeout = 30 * time.Second
	config.WriteTimeout = 30 * time.Second
	config.ParseTime = true
	config.MultiStatements = false
	return config, nil
}

func registerAdminTLS(config *mysql.Config, endpoint Endpoint, caPEM []byte) error {
	if config == nil {
		return errors.New("admin MySQL config 为空")
	}
	roots := x509.NewCertPool()
	remaining := bytes.TrimSpace(caPEM)
	certificates := 0
	for len(remaining) > 0 {
		if !bytes.HasPrefix(remaining, []byte("-----BEGIN CERTIFICATE-----")) {
			return errors.New("MySQL CA PEM 含非证书内容")
		}
		block, rest := pem.Decode(remaining)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return errors.New("MySQL CA PEM 格式非法")
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !certificate.IsCA || !certificate.BasicConstraintsValid {
			return errors.New("MySQL CA PEM 必须只含有效 CA 证书")
		}
		roots.AddCert(certificate)
		certificates++
		remaining = bytes.TrimSpace(rest)
	}
	if certificates == 0 {
		return errors.New("MySQL CA PEM 为空")
	}
	hash := sha256.Sum256(append(append([]byte(endpoint.Host), 0), caPEM...))
	name := fmt.Sprintf("pandora-plannerdb-%x-%d", hash[:8], mysqlTLSRegistrationSequence.Add(1))
	if err := mysql.RegisterTLSConfig(name, &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: endpoint.TLSServerName,
		RootCAs:    roots,
	}); err != nil {
		return errors.New("注册 admin MySQL TLS 配置失败")
	}
	config.TLS = nil
	config.TLSConfig = name
	config.AllowFallbackToPlaintext = false
	return nil
}
