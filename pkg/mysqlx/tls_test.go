package mysqlx_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/mysqlx"
)

func writeTestCA(t *testing.T) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	now := time.Now()
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Pandora MySQL test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}, &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Pandora MySQL test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	path := filepath.Join(t.TempDir(), "planner-ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

type mysqlTLSTestMaterial struct {
	caFile     string
	serverCert tls.Certificate
}

func newMySQLTLSTestMaterial(t *testing.T, certificateIP net.IP) mysqlTLSTestMaterial {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey(CA): %v", err)
	}
	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(11),
		Subject:               pkix.Name{CommonName: "Pandora MySQL handshake CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("CreateCertificate(CA): %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("ParseCertificate(CA): %v", err)
	}

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey(server): %v", err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(12),
		Subject:      pkix.Name{CommonName: "Pandora MySQL TLS test server"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{certificateIP},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("CreateCertificate(server): %v", err)
	}
	serverKeyDER, err := x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		t.Fatalf("MarshalPKCS8PrivateKey: %v", err)
	}
	serverCert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: serverKeyDER}),
	)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	caFile := filepath.Join(t.TempDir(), "handshake-ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		t.Fatalf("WriteFile(CA): %v", err)
	}
	return mysqlTLSTestMaterial{caFile: caFile, serverCert: serverCert}
}

func writeMySQLPacket(w io.Writer, sequence byte, payload []byte) error {
	header := []byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16), sequence}
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func readMySQLPacket(r io.Reader) (byte, []byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}
	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return header[3], payload, nil
}

func mysqlHandshakePayload() []byte {
	const capabilities = uint32(1<<0 | 1<<2 | 1<<3 | 1<<9 | 1<<11 | 1<<13 | 1<<15 | 1<<17 | 1<<19 | 1<<20)
	payload := []byte{0x0a}
	payload = append(payload, []byte("8.4.0-pandora-test")...)
	payload = append(payload, 0)
	payload = binary.LittleEndian.AppendUint32(payload, 1)
	payload = append(payload, []byte("12345678")...)
	payload = append(payload, 0)
	payload = binary.LittleEndian.AppendUint16(payload, uint16(capabilities&0xffff))
	payload = append(payload, 0x2d)
	payload = binary.LittleEndian.AppendUint16(payload, 0x0002)
	payload = binary.LittleEndian.AppendUint16(payload, uint16(capabilities>>16))
	payload = append(payload, 21)
	payload = append(payload, make([]byte, 10)...)
	payload = append(payload, []byte("abcdefghijkl")...)
	payload = append(payload, 0)
	payload = append(payload, []byte("mysql_native_password")...)
	payload = append(payload, 0)
	return payload
}

func serveOneMySQLTLSConnection(listener net.Listener, tlsConfig *tls.Config) error {
	conn, err := listener.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if err := writeMySQLPacket(conn, 0, mysqlHandshakePayload()); err != nil {
		return fmt.Errorf("write handshake: %w", err)
	}
	_, sslRequest, err := readMySQLPacket(conn)
	if err != nil {
		return fmt.Errorf("read SSLRequest: %w", err)
	}
	if len(sslRequest) != 32 || binary.LittleEndian.Uint32(sslRequest[:4])&(1<<11) == 0 {
		return fmt.Errorf("client did not send a MySQL SSLRequest")
	}
	tlsConn := tls.Server(conn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return fmt.Errorf("TLS handshake: %w", err)
	}
	if _, _, err := readMySQLPacket(tlsConn); err != nil {
		return fmt.Errorf("read authenticated handshake: %w", err)
	}
	ok := []byte{0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00}
	if err := writeMySQLPacket(tlsConn, 3, ok); err != nil {
		return fmt.Errorf("write auth OK: %w", err)
	}
	_, command, err := readMySQLPacket(tlsConn)
	if err != nil {
		return fmt.Errorf("read command: %w", err)
	}
	if len(command) != 1 || command[0] != 0x0e {
		return fmt.Errorf("command = %x, want COM_PING", command)
	}
	if err := writeMySQLPacket(tlsConn, 1, ok); err != nil {
		return fmt.Errorf("write ping OK: %w", err)
	}
	return nil
}

func startMySQLTLSTestServer(t *testing.T, material mysqlTLSTestMaterial, minVersion, maxVersion uint16) (string, <-chan error) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	errCh := make(chan error, 1)
	go func() {
		errCh <- serveOneMySQLTLSConnection(listener, &tls.Config{
			Certificates: []tls.Certificate{material.serverCert},
			MinVersion:   minVersion,
			MaxVersion:   maxVersion,
		})
	}()
	return listener.Addr().String(), errCh
}

func TestNewClientRejectsPartialTLSProfileBeforeDial(t *testing.T) {
	t.Parallel()

	_, err := mysqlx.NewClient(config.MySQLConf{
		DSN:       "planner@tcp(127.0.0.1:1)/pandora_account?timeout=20ms",
		TLSCAFile: `C:\missing\planner-ca.pem`,
	})
	if err == nil {
		t.Fatal("NewClient() = nil error, 期望拒绝只有 tls_ca_file 的半配置")
	}
	if !strings.Contains(err.Error(), "tls_ca_file") || !strings.Contains(err.Error(), "tls_server_name") {
		t.Fatalf("NewClient() error = %q, 期望同时点名 tls_ca_file/tls_server_name", err)
	}
}

func TestNewClientRejectsTLSIdentityDifferentFromDSNHost(t *testing.T) {
	t.Parallel()

	_, err := mysqlx.NewClient(config.MySQLConf{
		DSN:           "planner@tcp(db-a.intra:3306)/pandora_account?timeout=20ms",
		TLSCAFile:     writeTestCA(t),
		TLSServerName: "db-b.intra",
	})
	if err == nil {
		t.Fatal("NewClient() = nil error, 期望拒绝 DSN host 与 tls_server_name 不一致")
	}
	if !strings.Contains(err.Error(), "db-a.intra") || !strings.Contains(err.Error(), "db-b.intra") {
		t.Fatalf("NewClient() error = %q, 期望同时点名 DSN host 与 TLS 身份", err)
	}
}

func TestNewClientRejectsMissingOrMalformedCA(t *testing.T) {
	t.Parallel()

	badPEM := filepath.Join(t.TempDir(), "bad-ca.pem")
	if err := os.WriteFile(badPEM, []byte("not a PEM certificate\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	mixedPEM := filepath.Join(t.TempDir(), "mixed-ca.pem")
	validPEM, err := os.ReadFile(writeTestCA(t))
	if err != nil {
		t.Fatalf("ReadFile(valid CA): %v", err)
	}
	if err := os.WriteFile(mixedPEM, append(validPEM, []byte("trailing garbage\n")...), 0o600); err != nil {
		t.Fatalf("WriteFile(mixed CA): %v", err)
	}

	for _, tc := range []struct {
		name string
		path string
	}{
		{name: "文件不存在", path: filepath.Join(t.TempDir(), "missing-ca.pem")},
		{name: "内容不是证书", path: badPEM},
		{name: "有效证书后夹垃圾", path: mixedPEM},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := mysqlx.NewClient(config.MySQLConf{
				DSN:           "planner@tcp(db.intra:3306)/pandora_account?timeout=20ms",
				TLSCAFile:     tc.path,
				TLSServerName: "db.intra",
			})
			if err == nil {
				t.Fatal("NewClient() = nil error, 期望在拨号前拒绝无效 CA")
			}
			if !strings.Contains(err.Error(), "tls_ca_file") {
				t.Fatalf("NewClient() error = %q, 期望点名 tls_ca_file", err)
			}
		})
	}
}

func TestNewClientRejectsNonRegularOrOversizedCAFile(t *testing.T) {
	t.Parallel()

	oversized := filepath.Join(t.TempDir(), "oversized-ca.pem")
	if err := os.WriteFile(oversized, []byte(strings.Repeat("x", (1<<20)+1)), 0o600); err != nil {
		t.Fatalf("WriteFile(oversized CA): %v", err)
	}
	for _, tc := range []struct {
		name string
		path string
		want string
	}{
		{name: "目录不是普通文件", path: t.TempDir(), want: "regular file"},
		{name: "超过一MiB", path: oversized, want: "exceeds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := mysqlx.NewClient(config.MySQLConf{
				DSN:           "planner@tcp(db.intra:3306)/pandora_account?timeout=20ms",
				TLSCAFile:     tc.path,
				TLSServerName: "db.intra",
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("NewClient(CA=%q) error = %v, 期望包含 %q", tc.path, err, tc.want)
			}
		})
	}
}

func TestNewClientRejectsUnsafeOrAmbiguousDSNTLSModes(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"skip-verify", "preferred"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			_, err := mysqlx.NewClient(config.MySQLConf{
				DSN: "planner@tcp(127.0.0.1:1)/pandora_account?timeout=20ms&tls=" + mode,
			})
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "unsafe") {
				t.Fatalf("NewClient(tls=%s) error = %v, 期望在拨号前 fail-closed", mode, err)
			}
		})
	}

	weakConfigName := fmt.Sprintf("pandora-test-tls10-%d", time.Now().UnixNano())
	if err := drivermysql.RegisterTLSConfig(weakConfigName, &tls.Config{MinVersion: tls.VersionTLS10}); err != nil {
		t.Fatalf("RegisterTLSConfig: %v", err)
	}
	t.Cleanup(func() { drivermysql.DeregisterTLSConfig(weakConfigName) })
	_, err := mysqlx.NewClient(config.MySQLConf{
		DSN: "planner@tcp(127.0.0.1:1)/pandora_account?timeout=20ms&tls=" + weakConfigName,
	})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "unsafe") {
		t.Fatalf("NewClient(custom TLS 1.0) error = %v, 期望在拨号前拒绝低于 TLS 1.2", err)
	}

	caFile := writeTestCA(t)
	for _, mode := range []string{"true", "false"} {
		mode := mode
		t.Run("中心配置拒绝DSN内tls="+mode, func(t *testing.T) {
			t.Parallel()
			_, err := mysqlx.NewClient(config.MySQLConf{
				DSN:           "planner@tcp(db.intra:3306)/pandora_account?timeout=20ms&tls=" + mode,
				TLSCAFile:     caFile,
				TLSServerName: "db.intra",
			})
			if err == nil || !strings.Contains(err.Error(), "must not contain tls") {
				t.Fatalf("NewClient(tls=%s + strict profile) error = %v, 期望拒绝歧义配置", mode, err)
			}
		})
	}
}

func TestNewClientUsesVerifiedTLS12ForMySQLHandshake(t *testing.T) {
	t.Parallel()

	material := newMySQLTLSTestMaterial(t, net.ParseIP("127.0.0.1"))
	addr, serverErr := startMySQLTLSTestServer(t, material, tls.VersionTLS12, tls.VersionTLS12)
	db, err := mysqlx.NewClient(config.MySQLConf{
		DSN:           "planner@tcp(" + addr + ")/pandora_account?timeout=1s&readTimeout=1s&writeTimeout=1s",
		TLSCAFile:     material.caFile,
		TLSServerName: "127.0.0.1",
		PingTimeout:   config.Duration(time.Second),
	})
	if err != nil {
		t.Fatalf("NewClient() TLS 1.2 handshake: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("fake MySQL server: %v", err)
	}
}

func TestNewClientRejectsTrustedCertificateForDifferentHost(t *testing.T) {
	t.Parallel()

	material := newMySQLTLSTestMaterial(t, net.ParseIP("127.0.0.2"))
	addr, serverErr := startMySQLTLSTestServer(t, material, tls.VersionTLS12, tls.VersionTLS12)
	_, err := mysqlx.NewClient(config.MySQLConf{
		DSN:           "planner@tcp(" + addr + ")/pandora_account?timeout=1s&readTimeout=1s&writeTimeout=1s",
		TLSCAFile:     material.caFile,
		TLSServerName: "127.0.0.1",
		PingTimeout:   config.Duration(time.Second),
	})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "certificate") {
		t.Fatalf("NewClient() error = %v, 期望严格主机名校验拒绝受信但 SAN 不匹配的证书", err)
	}
	if err := <-serverErr; err == nil || !strings.Contains(strings.ToLower(err.Error()), "handshake") {
		t.Fatalf("fake MySQL server error = %v, 期望握手因客户端拒证书而中止", err)
	}
}

func TestNewClientRejectsTLS11Server(t *testing.T) {
	t.Parallel()

	material := newMySQLTLSTestMaterial(t, net.ParseIP("127.0.0.1"))
	addr, serverErr := startMySQLTLSTestServer(t, material, tls.VersionTLS10, tls.VersionTLS11)
	_, err := mysqlx.NewClient(config.MySQLConf{
		DSN:           "planner@tcp(" + addr + ")/pandora_account?timeout=1s&readTimeout=1s&writeTimeout=1s",
		TLSCAFile:     material.caFile,
		TLSServerName: "127.0.0.1",
		PingTimeout:   config.Duration(time.Second),
	})
	if err == nil {
		t.Fatal("NewClient() = nil error, 期望 TLS 1.2 最低版本拒绝 TLS 1.1 服务端")
	}
	if err := <-serverErr; err == nil || !strings.Contains(strings.ToLower(err.Error()), "handshake") {
		t.Fatalf("fake MySQL server error = %v, 期望 TLS 版本协商失败", err)
	}
}

func TestNewClientWithoutTLSFieldsKeepsLocalDialPath(t *testing.T) {
	t.Parallel()

	_, err := mysqlx.NewClient(config.MySQLConf{
		DSN:         "planner@tcp(127.0.0.1:1)/pandora_account?timeout=20ms",
		PingTimeout: config.Duration(20 * time.Millisecond),
	})
	if err == nil || !strings.Contains(err.Error(), "ping mysql") {
		t.Fatalf("NewClient(local) error = %v, 期望保持原有明文拨号/探活路径", err)
	}
	if strings.Contains(strings.ToLower(err.Error()), "tls") {
		t.Fatalf("NewClient(local) error = %v, 未配置 TLS 时不应进入 TLS 分支", err)
	}
}

func TestNewShardSetAppliesStrictTLSContractToEveryShard(t *testing.T) {
	t.Parallel()

	_, err := mysqlx.NewShardSet(config.MySQLConf{
		Shards:        []string{"planner@tcp(shard-a.intra:3306)/pandora_auction_0?timeout=20ms"},
		TLSCAFile:     writeTestCA(t),
		TLSServerName: "shard-b.intra",
	})
	if err == nil {
		t.Fatal("NewShardSet() = nil error, 期望分片复用严格 TLS 身份契约")
	}
	for _, want := range []string{"shard 0", "shard-a.intra", "shard-b.intra"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("NewShardSet() error = %q, 缺少 %q", err, want)
		}
	}
}
