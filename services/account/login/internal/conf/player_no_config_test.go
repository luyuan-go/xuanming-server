package conf_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"

	"github.com/luyuancpp/pandora/services/account/login/internal/conf"
)

func loadPlayerNoConfig(t *testing.T, rel string) conf.Config {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "..", rel))
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Ext(path) == ".example" {
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatalf("read %s: %v", rel, readErr)
		}
		staged := strings.ReplaceAll(string(raw), "[__ROLE_ID_1__, __ROLE_ID_2__]", "[1, 2]")
		path = filepath.Join(t.TempDir(), "config.yaml")
		if writeErr := os.WriteFile(path, []byte(staged), 0o600); writeErr != nil {
			t.Fatalf("stage %s: %v", rel, writeErr)
		}
	}
	c := kconfig.New(kconfig.WithSource(file.NewSource(path)))
	defer c.Close()
	if err := c.Load(); err != nil {
		t.Fatalf("load %s: %v", rel, err)
	}
	var cfg conf.Config
	if err := c.Scan(&cfg); err != nil {
		t.Fatalf("scan %s: %v", rel, err)
	}
	cfg.Defaults()
	return cfg
}

func TestLoginDeploymentConfigsCarryPlayerNoResolveVerifier(t *testing.T) {
	for _, rel := range []string{
		"etc/login-dev.yaml",
		"etc/login-dev-tidb.yaml",
		"etc/login-prod.yaml.example",
	} {
		t.Run(rel, func(t *testing.T) {
			cfg := loadPlayerNoConfig(t, rel)
			if cfg.Login.PlayerNoResolveAuthSecret == "" {
				t.Fatalf("%s missing player_no_resolve_auth_secret", rel)
			}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("%s player_no verifier config: %v", rel, err)
			}
		})
	}
}
