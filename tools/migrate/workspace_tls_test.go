package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
)

func TestLoadManifestWorkspaceRequiresAbsoluteRegularTLSCAFile(t *testing.T) {
	dir := t.TempDir()
	dsnFile := filepath.Join(dir, "account.dsn")
	writeTestFile(t, dsnFile, "migrator@tcp(mysql-planner.internal:3306)/pandora_account_w_"+testWorkspaceID+"?tls=true")
	regularCA := filepath.Join(dir, "planner-ca.pem")
	writeTestFile(t, regularCA, "placeholder")

	tests := []struct {
		name      string
		caFile    *string
		wantError string
	}{
		{name: "missing", wantError: "tls_ca_file"},
		{name: "relative", caFile: stringPointer("planner-ca.pem"), wantError: "绝对路径"},
		{name: "not_found", caFile: stringPointer(filepath.Join(dir, "missing.pem")), wantError: "tls_ca_file"},
		{name: "directory", caFile: stringPointer(dir), wantError: "普通文件"},
		{name: "regular_file", caFile: &regularCA},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manifestPath := filepath.Join(dir, tt.name+".json")
			target := map[string]any{
				"name":          "account-workspace",
				"migration_set": "pandora_account",
				"database":      "pandora_account_w_" + testWorkspaceID,
				"dsn_file":      filepath.Base(dsnFile),
			}
			if tt.caFile != nil {
				target["tls_ca_file"] = *tt.caFile
			}
			raw, err := json.Marshal(map[string]any{"targets": []any{target}})
			if err != nil {
				t.Fatalf("marshal manifest: %v", err)
			}
			if err := os.WriteFile(manifestPath, raw, 0o600); err != nil {
				t.Fatalf("write manifest: %v", err)
			}

			manifest, err := loadTargetManifest(manifestPath)
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("loadTargetManifest() error = %v, want %q", err, tt.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadTargetManifest() error = %v", err)
			}
			if got := manifest.Targets[0].TLSCAFile; got != filepath.Clean(regularCA) {
				t.Fatalf("tls ca file = %q, want %q", got, filepath.Clean(regularCA))
			}
		})
	}
}

func stringPointer(value string) *string { return &value }

func TestReadAndHardenDSNWorkspaceRegistersVerifiedCustomTLS(t *testing.T) {
	const serverName = "mysql-planner.internal"
	caPEM, serverCertificate := issueWorkspaceTestCertificate(t, serverName)
	dir := t.TempDir()
	caPath := filepath.Join(dir, "planner-ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatalf("write CA: %v", err)
	}
	target, dsnPath := workspaceTLSTestTarget(t, dir, serverName, caPath, "tls=true")

	cfg, err := readAndHardenDSN(dsnPath, target, true)
	if err != nil {
		t.Fatalf("readAndHardenDSN() error = %v", err)
	}
	if !strings.HasPrefix(cfg.TLSConfig, "pandora-workspace-") {
		t.Fatalf("TLS config name = %q", cfg.TLSConfig)
	}
	parsed, err := mysql.ParseDSN(cfg.FormatDSN())
	if err != nil {
		t.Fatalf("round-trip custom TLS DSN: %v", err)
	}
	if parsed.TLS == nil {
		t.Fatal("custom TLS config was not registered")
	}
	if parsed.TLS.InsecureSkipVerify {
		t.Fatal("workspace TLS must verify certificates")
	}
	if parsed.TLS.MinVersion != tls.VersionTLS12 {
		t.Fatalf("TLS MinVersion = %x, want TLS 1.2", parsed.TLS.MinVersion)
	}
	if parsed.TLS.ServerName != serverName {
		t.Fatalf("TLS ServerName = %q, want %q", parsed.TLS.ServerName, serverName)
	}
	if parsed.TLS.RootCAs == nil {
		t.Fatal("workspace TLS RootCAs must contain the configured bundle CA")
	}
	bundleOnlyRoots := x509.NewCertPool()
	if !bundleOnlyRoots.AppendCertsFromPEM(caPEM) {
		t.Fatal("append fixture bundle CA")
	}
	if !parsed.TLS.RootCAs.Equal(bundleOnlyRoots) {
		t.Fatal("workspace TLS RootCAs 仍带系统信任语义，期望只信 bundle CA")
	}
	clientErr, serverErr := handshakeWorkspaceTLS(t, parsed.TLS, serverCertificate)
	if clientErr != nil || serverErr != nil {
		t.Fatalf("valid CA/SAN handshake client=%v server=%v", clientErr, serverErr)
	}

	wrongTarget, wrongDSN := workspaceTLSTestTarget(t, dir, "other.internal", caPath, "tls=true")
	wrongCfg, err := readAndHardenDSN(wrongDSN, wrongTarget, true)
	if err != nil {
		t.Fatalf("build mismatched hostname TLS config: %v", err)
	}
	wrongParsed, err := mysql.ParseDSN(wrongCfg.FormatDSN())
	if err != nil {
		t.Fatalf("round-trip mismatched hostname config: %v", err)
	}
	clientErr, _ = handshakeWorkspaceTLS(t, wrongParsed.TLS, serverCertificate)
	if clientErr == nil {
		t.Fatal("host not present in certificate SAN must fail the TLS handshake")
	}
}

func TestReadAndHardenDSNWorkspaceRejectsMissingBadCAAndUnsafeTLS(t *testing.T) {
	const serverName = "mysql-planner.internal"
	caPEM, _ := issueWorkspaceTestCertificate(t, serverName)
	dir := t.TempDir()
	validCAPath := filepath.Join(dir, "valid-ca.pem")
	badCAPath := filepath.Join(dir, "bad-ca.pem")
	if err := os.WriteFile(validCAPath, caPEM, 0o600); err != nil {
		t.Fatalf("write valid CA: %v", err)
	}
	if err := os.WriteFile(badCAPath, []byte("not a PEM certificate"), 0o600); err != nil {
		t.Fatalf("write bad CA: %v", err)
	}

	tests := []struct {
		name      string
		caPath    string
		tlsQuery  string
		wantError string
	}{
		{name: "missing_ca", tlsQuery: "tls=true", wantError: "tls_ca_file"},
		{name: "bad_pem", caPath: badCAPath, tlsQuery: "tls=true", wantError: "PEM"},
		{name: "plaintext", caPath: validCAPath, wantError: "tls=true"},
		{name: "skip_verify", caPath: validCAPath, tlsQuery: "tls=skip-verify", wantError: "tls=true"},
		{name: "custom_query", caPath: validCAPath, tlsQuery: "tls=planner-ca", wantError: "DSN 格式错误"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, dsnPath := workspaceTLSTestTarget(t, dir, serverName, tt.caPath, tt.tlsQuery)
			_, err := readAndHardenDSN(dsnPath, target, true)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("readAndHardenDSN() error = %v, want %q", err, tt.wantError)
			}
		})
	}
}

func workspaceTLSTestTarget(
	t *testing.T,
	dir, host, caPath, tlsQuery string,
) (migrationTarget, string) {
	t.Helper()
	database := "pandora_account_w_" + testWorkspaceID
	dsn := "migrator@tcp(" + host + ":3306)/" + database
	if tlsQuery != "" {
		dsn += "?" + tlsQuery
	}
	dsnPath := filepath.Join(dir, strings.ReplaceAll(host, ".", "-")+"-"+strings.ReplaceAll(tlsQuery, "=", "-")+".dsn")
	if err := os.WriteFile(dsnPath, []byte(dsn), 0o600); err != nil {
		t.Fatalf("write DSN: %v", err)
	}
	return migrationTarget{
		Name:                   "account-workspace",
		MigrationSet:           "pandora_account",
		Database:               database,
		DSNFile:                dsnPath,
		TLSCAFile:              caPath,
		TimeoutSeconds:         defaultTargetTimeoutSeconds,
		LockWaitTimeoutSeconds: defaultLockWaitTimeoutSeconds,
	}, dsnPath
}

func issueWorkspaceTestCertificate(t *testing.T, dnsName string) ([]byte, tls.Certificate) {
	t.Helper()
	now := time.Now()
	caPublic, caPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Pandora workspace test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caPublic, caPrivate)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}

	serverPublic, serverPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: dnsName},
		DNSNames:     []string{dnsName},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caTemplate, serverPublic, caPrivate)
	if err != nil {
		t.Fatalf("create server certificate: %v", err)
	}
	serverLeaf, err := x509.ParseCertificate(serverDER)
	if err != nil {
		t.Fatalf("parse server certificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), tls.Certificate{
		Certificate: [][]byte{serverDER, caDER},
		PrivateKey:  serverPrivate,
		Leaf:        serverLeaf,
	}
}

func handshakeWorkspaceTLS(
	t *testing.T,
	clientConfig *tls.Config,
	serverCertificate tls.Certificate,
) (clientErr, serverErr error) {
	t.Helper()
	clientRaw, serverRaw := net.Pipe()
	deadline := time.Now().Add(3 * time.Second)
	if err := clientRaw.SetDeadline(deadline); err != nil {
		t.Fatalf("set client deadline: %v", err)
	}
	if err := serverRaw.SetDeadline(deadline); err != nil {
		t.Fatalf("set server deadline: %v", err)
	}
	serverDone := make(chan error, 1)
	go func() {
		server := tls.Server(serverRaw, &tls.Config{
			Certificates: []tls.Certificate{serverCertificate},
			MinVersion:   tls.VersionTLS12,
		})
		serverDone <- server.HandshakeContext(context.Background())
		_ = serverRaw.Close()
	}()
	client := tls.Client(clientRaw, clientConfig.Clone())
	clientErr = client.HandshakeContext(context.Background())
	_ = clientRaw.Close()
	select {
	case serverErr = <-serverDone:
	case <-time.After(4 * time.Second):
		t.Fatal("TLS server handshake timed out")
	}
	return clientErr, serverErr
}
