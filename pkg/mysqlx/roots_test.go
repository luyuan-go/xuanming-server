package mysqlx

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadStrictRootCAsTrustsOnlyBundleSubjects(t *testing.T) {
	t.Parallel()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(991),
		Subject:               pkix.Name{CommonName: "Pandora bundle-only root"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}
	caPath := filepath.Join(t.TempDir(), "planner-ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	roots, err := loadStrictRootCAs(caPath)
	if err != nil {
		t.Fatalf("loadStrictRootCAs: %v", err)
	}
	subjects := roots.Subjects()
	if len(subjects) != 1 || !bytes.Equal(subjects[0], certificate.RawSubject) {
		t.Fatalf("root subjects = %d，期望只含 bundle CA subject", len(subjects))
	}
	expected := x509.NewCertPool()
	expected.AddCert(certificate)
	if !roots.Equal(expected) {
		t.Fatal("root pool 仍带系统信任语义，期望与只含 bundle CA 的新池完全相等")
	}
}
