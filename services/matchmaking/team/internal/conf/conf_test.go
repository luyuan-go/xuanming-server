// conf_test.go — 校验 etc/ 下 team 部署配置能被 main.go 同款加载路径解析,且鉴权字段齐备。
// 这里盯的是一类**无声故障**:team→matchmaker 的 ResolvePlayerMatchContext 强制验签,
// 少了 secret / audience 拼错,两端进程照常启动、日志一片正常,只表现为
// 「招募列表恒空 + 申请入队恒失败」。YAML 手滑必须在测试里就红。
package conf_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"

	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/conf"
)

func TestValidateOfflineLeaveDisabled(t *testing.T) {
	var cfg conf.Config
	cfg.Defaults()
	if err := cfg.ValidateOfflineLeave(); err != nil {
		t.Fatalf("offline_leave 默认关闭时不应拒绝启动: %v", err)
	}
}

// 该用例原本断言「没有共用 roster fence 就必须拒启动」。
// 2026-08-06 起 matchmaker 组票改走 TeamService.BeginTeamMatch，在 team 自己的乐观锁内
// 冻结名单、摘人在同一把锁内看到租约即推迟 —— 共同线性化点已经存在，前提不再成立。
// 因此改为断言「依赖配齐即可启动」，并保留下面两条依赖缺失的拒启用例。
func TestValidateOfflineLeaveEnabledPassesWithSharedRosterFence(t *testing.T) {
	var cfg conf.Config
	cfg.Defaults()
	cfg.Team.OfflineLeave.Enabled = true
	cfg.Team.LocatorAddr = "player-locator:20006"
	cfg.Team.MatchmakerAddr = "matchmaker:20011"

	if err := cfg.ValidateOfflineLeave(); err != nil {
		t.Fatalf("依赖配齐且 roster fence 已就位时应允许启动: %v", err)
	}
}

// 依赖缺失必须仍然拒启:少了 locator 判不了离线(功能静默失效),
// 少了 matchmaker 就失去「整场对局占用」的闸门 —— 那把秒级 roster 租约只覆盖组票瞬间，
// 覆盖不了整场战斗，两者不能互相顶替。
func TestValidateOfflineLeaveEnabledFailsWithoutDeps(t *testing.T) {
	for _, tc := range []struct {
		name      string
		locator   string
		matchmake string
		want      string
	}{
		{"缺 locator", "", "matchmaker:20011", "locator_addr"},
		{"缺 matchmaker", "player-locator:20006", "", "matchmaker_addr"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg conf.Config
			cfg.Defaults()
			cfg.Team.OfflineLeave.Enabled = true
			cfg.Team.LocatorAddr = tc.locator
			cfg.Team.MatchmakerAddr = tc.matchmake

			err := cfg.ValidateOfflineLeave()
			if err == nil {
				t.Fatal("依赖缺失时必须拒绝启动,不能静默降级")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("拒启原因必须点名缺失项 %q,实际: %v", tc.want, err)
			}
		})
	}
}

func loadConfig(t *testing.T, rel string) conf.Config {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "..", rel))
	if err != nil {
		t.Fatalf("abs %s: %v", rel, err)
	}
	if filepath.Ext(path) == ".example" {
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatalf("read %s: %v", rel, readErr)
		}
		path = filepath.Join(t.TempDir(), "config.yaml")
		if writeErr := os.WriteFile(path, raw, 0o600); writeErr != nil {
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

// 配了 matchmaker_addr 就等于启用了入队闸门与招募列表复核,这两条路径都必须签名。
func TestDevConfigCarriesMatchResumeAuthCredential(t *testing.T) {
	cfg := loadConfig(t, "etc/team-dev.yaml")
	if cfg.Team.MatchmakerAddr == "" {
		t.Skip("dev 模板未接 matchmaker,鉴权凭据无从校验")
	}
	if cfg.Team.MatchResumeAuthSecret == "" {
		t.Fatal("team-dev.yaml 缺 team.match_resume_auth_secret;matchmaker 会拒掉每一次调用,表现为招募列表恒空 + 入队恒失败")
	}
	if cfg.Team.MatchResumeAuthAudience == "" {
		t.Fatal("team-dev.yaml 缺 team.match_resume_auth_audience;audience 不匹配同样是静默被拒")
	}
	// main.go 用同一组参数构造 signer,构造失败即进程退出。让 YAML 里的弱 key 在这里就红。
	if _, err := internalrpcauth.NewSigner(cfg.Team.MatchResumeAuthSecret, "team",
		cfg.Team.MatchResumeAuthAudience); err != nil {
		t.Fatalf("dev 配置无法构造 team resume signer: %v", err)
	}
}

func TestPlayerNoResolverConfigIsComplete(t *testing.T) {
	var cfg conf.Config
	cfg.Defaults()
	if err := cfg.ValidatePlayerNoResolver(); err != nil {
		t.Fatalf("disabled resolver should remain valid: %v", err)
	}
	cfg.Team.PlayerNoResolverAuthAudience = "login:player-no"
	if err := cfg.ValidatePlayerNoResolver(); err == nil {
		t.Fatal("audience without resolver address must fail fast")
	}
	cfg.Team.PlayerNoResolverAuthAudience = ""

	cfg.Team.PlayerNoResolverAddr = "login:20001"
	cfg.Defaults()
	if err := cfg.ValidatePlayerNoResolver(); err == nil {
		t.Fatal("resolver address without secret must fail fast")
	}

	cfg.Team.PlayerNoResolverAuthSecret = "team-player-no-resolver-test-key-0123456789"
	if err := cfg.ValidatePlayerNoResolver(); err != nil {
		t.Fatalf("complete resolver config: %v", err)
	}
	cfg.Team.MaxMembers = 33
	if err := cfg.ValidatePlayerNoResolver(); err == nil {
		t.Fatal("max_members above batch contract must fail fast")
	}
	cfg.Team.MaxMembers = 5
	cfg.Team.MaxApplicationsPerTeam = 33
	if err := cfg.ValidatePlayerNoResolver(); err == nil {
		t.Fatal("max_applications_per_team above batch contract must fail fast")
	}
	cfg.Team.MaxApplicationsPerTeam = 10
	cfg.Team.MaxOpenTeamsPerQuery = 33
	if err := cfg.ValidatePlayerNoResolver(); err == nil {
		t.Fatal("max_open_teams_per_query above batch contract must fail fast")
	}
}

func TestTeamDeploymentConfigsCarryPlayerNoResolver(t *testing.T) {
	for _, rel := range []string{"etc/team-dev.yaml", "etc/team-prod.yaml.example"} {
		t.Run(rel, func(t *testing.T) {
			cfg := loadConfig(t, rel)
			if err := cfg.ValidatePlayerNoResolver(); err != nil {
				t.Fatalf("%s player_no resolver config: %v", rel, err)
			}
			if cfg.Team.PlayerNoResolverAddr == "" {
				t.Fatalf("%s must enable player_no resolver", rel)
			}
		})
	}
}
