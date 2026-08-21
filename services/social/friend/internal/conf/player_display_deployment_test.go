package conf

import (
	"os"
	"path/filepath"
	"testing"

	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
)

func TestFriendDeploymentConfigsCarryPlayerDisplayResolvers(t *testing.T) {
	for _, name := range []string{"friend-dev.yaml", "friend-dev-tidb.yaml", "friend-prod.yaml.example"} {
		t.Run(name, func(t *testing.T) {
			path, err := filepath.Abs(filepath.Join("..", "..", "etc", name))
			if err != nil {
				t.Fatal(err)
			}
			if filepath.Ext(path) == ".example" {
				raw, readErr := os.ReadFile(path)
				if readErr != nil {
					t.Fatal(readErr)
				}
				path = filepath.Join(t.TempDir(), "config.yaml")
				if writeErr := os.WriteFile(path, raw, 0o600); writeErr != nil {
					t.Fatal(writeErr)
				}
			}
			source := kconfig.New(kconfig.WithSource(file.NewSource(path)))
			defer source.Close()
			if err := source.Load(); err != nil {
				t.Fatalf("load %s: %v", name, err)
			}
			var cfg Config
			if err := source.Scan(&cfg); err != nil {
				t.Fatalf("scan %s: %v", name, err)
			}
			cfg.Defaults()
			if err := cfg.ValidatePlayerDisplayResolvers(); err != nil {
				t.Fatalf("validate %s: %v", name, err)
			}
			if cfg.Friend.PlayerNameResolverAddr == "" || cfg.Friend.PlayerNoResolverAddr == "" {
				t.Fatalf("%s missing display resolver address", name)
			}
		})
	}
}
