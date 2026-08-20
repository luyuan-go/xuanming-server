package plannerdb

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

type AppConfig struct {
	AdminDSN                string
	MySQLCAPEM              []byte
	MasterKey               []byte
	Endpoint                Endpoint
	RegistryDatabase        string
	RegistryInstanceMarker  string
	MigrateBinaryPath       string
	MigrateBinarySHA256     string
	MigrationTimeout        time.Duration
	ProvisionTimeout        time.Duration
	WorkspaceLockWait       time.Duration
	Logger                  safeLogger
	MaxConcurrentProvisions int
}

type App struct {
	backend *MySQLBackend
	service *service
	handler http.Handler
}

func NewApp(ctx context.Context, config AppConfig) (*App, error) {
	backend, err := OpenMySQLBackend(ctx, BackendConfig{
		AdminDSN: config.AdminDSN, MySQLCAPEM: config.MySQLCAPEM, Endpoint: config.Endpoint,
		RegistryDatabase: config.RegistryDatabase, RegistryInstanceMarker: config.RegistryInstanceMarker,
	})
	if err != nil {
		return nil, err
	}
	runner, err := newExecMigrationRunner(execMigrationConfig{
		BinaryPath: config.MigrateBinaryPath, ExpectedSHA256: config.MigrateBinarySHA256,
		MySQLCAPEM: config.MySQLCAPEM, Timeout: config.MigrationTimeout,
	})
	if err != nil {
		_ = backend.Close()
		return nil, err
	}
	service, err := newService(serviceConfig{
		Registry: backend, Resources: backend, Migrator: runner, Endpoint: config.Endpoint,
		MasterKey: config.MasterKey, Random: rand.Reader, ProvisionTimeout: config.ProvisionTimeout,
		WorkspaceLockWait: config.WorkspaceLockWait, Logger: config.Logger,
		MaxConcurrentProvisions: config.MaxConcurrentProvisions,
	})
	if err != nil {
		_ = backend.Close()
		return nil, err
	}
	return &App{backend: backend, service: service, handler: newHTTPHandler(service)}, nil
}

func (a *App) Handler() http.Handler {
	return a.handler
}

func (a *App) Close() error {
	if a == nil {
		return nil
	}
	workerErr := a.service.close(30 * time.Second)
	backendErr := a.backend.Close()
	return errors.Join(workerErr, backendErr)
}

func (b *MySQLBackend) IssueToken(ctx context.Context, ttl time.Duration) (string, error) {
	return issueEnrollmentToken(ctx, b, rand.Reader, time.Now().UTC(), ttl)
}

func (b *MySQLBackend) IssueRecoveryToken(ctx context.Context, workspaceID string, ttl time.Duration) (string, error) {
	return issueRecoveryToken(ctx, b, rand.Reader, time.Now().UTC(), ttl, workspaceID)
}

func LoadSecretRef(reference string, maximumBytes int64) ([]byte, error) {
	return loadSecretRef(reference, maximumBytes)
}

func DecodeMasterKey(encoded []byte) ([]byte, error) {
	value := strings.TrimSpace(string(encoded))
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(decoded) != 32 || base64.RawURLEncoding.EncodeToString(decoded) != value {
		return nil, errors.New("master key 必须是 32 字节的 canonical base64url（43 字符、无 padding）")
	}
	return decoded, nil
}

func NewServerTLSConfig(certificatePEM, keyPEM []byte) (*tls.Config, error) {
	certificate, err := tls.X509KeyPair(certificatePEM, keyPEM)
	if err != nil {
		return nil, errors.New("HTTPS certificate/key 不匹配或 PEM 非法")
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
		NextProtos:   []string{"h2", "http/1.1"},
	}, nil
}

func ZeroBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

var _ io.Closer = (*App)(nil)
