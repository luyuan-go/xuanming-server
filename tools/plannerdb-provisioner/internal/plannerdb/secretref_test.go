package plannerdb

import (
	"bytes"
	"testing"
)

func TestSecretRefAcceptsNamedEnvironmentAndRejectsInlineValue(t *testing.T) {
	t.Setenv("PANDORA_TEST_PROVISIONER_SECRET", "top-secret-value")
	got, err := loadSecretRef("env:PANDORA_TEST_PROVISIONER_SECRET", 1024)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("top-secret-value")) {
		t.Fatalf("secret mismatch")
	}
	for _, ref := range []string{"top-secret-value", "env:", "env:Path", "file:relative.key", "inline:top-secret-value"} {
		if _, err := loadSecretRef(ref, 1024); err == nil {
			t.Fatalf("ref %q should fail", ref)
		}
	}
}
