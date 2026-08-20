package biz

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/conf"
)

// newLocalFleetForTest 造一个可构造的 LocalHubFleetProvider:executable_path 指向临时文件
// (满足 os.Stat 存在校验),token 签发器故意返回错误以复现 enforce 下的 fail-closed 分支。
// buildEnv 在拿 token 前失败 → start() 在 exec 前返回错误 → 不会真正拉起任何进程。
func newLocalFleetForTest(t *testing.T, required bool) *LocalHubFleetProvider {
	t.Helper()
	dir := t.TempDir()
	exe := filepath.Join(dir, "PandoraServer.exe")
	if err := os.WriteFile(exe, []byte("stub"), 0o644); err != nil {
		t.Fatalf("write stub exe: %v", err)
	}
	p, err := NewLocalHubFleetProvider(conf.LocalHubConf{
		ExecutablePath: exe,
		AdvertiseHost:  "127.0.0.1",
		Port:           7777,
		Region:         "cn-1",
		Capacity:       500,
	})
	if err != nil {
		t.Fatalf("NewLocalHubFleetProvider: %v", err)
	}
	// 令牌签发器恒失败:enforce 下应导致 start 失败并 fail-closed。
	p.SetDSTokenIssuer(func(string, string, uint32) (string, LocalHubCredential, error) {
		return "", LocalHubCredential{}, errors.New("boom: ds token sign failed")
	}, required)
	return p
}

// TestLocalFleet_EnforceFailClosed:enforce(required=true)下令牌签发失败 → ListShards
// 不返回候选(返回 ErrHubNoAvailable),避免把客户端路由到一个回调必被守卫全拒的 Hub。
func TestLocalFleet_EnforceFailClosed(t *testing.T) {
	p := newLocalFleetForTest(t, true)
	cands, err := p.ListShards(context.Background(), "cn-1")
	if err == nil {
		t.Fatal("enforce 下启动失败应返回错误,却成功了")
	}
	if errcode.As(err) != errcode.ErrHubNoAvailable {
		t.Fatalf("errcode: got %d want ErrHubNoAvailable(%d)", errcode.As(err), errcode.ErrHubNoAvailable)
	}
	if len(cands) != 0 {
		t.Fatalf("enforce fail-closed 不应返回候选,却返回了 %d 个", len(cands))
	}
}

// TestLocalFleet_PermissiveStillReturnsCandidate:off/permissive(required=false)下
// 即使 token 签发失败,也照旧返回候选(便于排查 DS 启动问题,保持原语义不变)。
func TestLocalFleet_PermissiveStillReturnsCandidate(t *testing.T) {
	p := newLocalFleetForTest(t, false)
	cands, err := p.ListShards(context.Background(), "cn-1")
	if err != nil {
		t.Fatalf("permissive 下不应因启动失败而返回错误: %v", err)
	}
	if len(cands) != 1 {
		t.Fatalf("permissive 应返回 1 个候选,却返回了 %d 个", len(cands))
	}
}

func TestLocalFleetBuildEnvCarriesModelBIdentityAndLocalProfile(t *testing.T) {
	p := newLocalFleetForTest(t, false)
	var gotPod, gotUID string
	var gotEpoch uint32
	p.SetDSTokenIssuer(func(pod, uid string, epoch uint32) (string, LocalHubCredential, error) {
		gotPod, gotUID, gotEpoch = pod, uid, epoch
		return "model-b-token", LocalHubCredential{
			InstanceUID: uid, ProtocolEpoch: epoch, Gen: 7, JTI: "jti-7", WriterEpoch: 2, ExpiresAtMs: 123,
		}, nil
	}, true)

	env, err := p.buildEnv()
	if err != nil {
		t.Fatalf("buildEnv: %v", err)
	}
	if gotPod != p.podName || gotUID != p.instanceUID || gotEpoch != 1 {
		t.Fatalf("issuer identity=(%q,%q,%d), want (%q,%q,1)", gotPod, gotUID, gotEpoch, p.podName, p.instanceUID)
	}
	if _, err := uuid.Parse(gotUID); err != nil {
		t.Fatalf("instance uid 不是 UUID: %q: %v", gotUID, err)
	}
	if value := lastEnvValue(env, "PANDORA_DS_TOKEN"); value != "model-b-token" {
		t.Fatalf("PANDORA_DS_TOKEN=%q", value)
	}
	if value := lastEnvValue(env, "PANDORA_DS_LOCAL_PROFILE"); value != "local-off-v1" {
		t.Fatalf("PANDORA_DS_LOCAL_PROFILE=%q", value)
	}
	if p.cred.Gen != 7 {
		t.Fatalf("cred.Gen=%d, want 7", p.cred.Gen)
	}
	// 心跳 ACK 回显的五元组必须与 env 下发的同一份凭据同源,否则 UE 判 mismatched。
	ack, ok := p.LocalCredentialACK(p.podName)
	if !ok {
		t.Fatal("LocalCredentialACK 应在签发成功后可用")
	}
	if ack.InstanceUID != p.instanceUID || ack.ProtocolEpoch != 1 || ack.Gen != 7 ||
		ack.JTI != "jti-7" || ack.WriterEpoch != 2 {
		t.Fatalf("ACK tuple=%+v 与下发凭据不同源", ack)
	}
	if _, ok := p.LocalCredentialACK("pandora-hub-local-other"); ok {
		t.Fatal("别的 pod 不得拿到本机 ACK")
	}
}

func TestLocalFleetExtraEnvCannotOverrideLocalProfile(t *testing.T) {
	p := newLocalFleetForTest(t, false)
	p.dsTokenIssuer = nil
	p.cfg.ExtraEnv = map[string]string{"pandora_ds_local_profile": "evil"}
	env, err := p.buildEnv()
	if err != nil {
		t.Fatalf("buildEnv: %v", err)
	}
	if value := lastEnvValue(env, "PANDORA_DS_LOCAL_PROFILE"); value != "local-off-v1" {
		t.Fatalf("profile 被 extra_env 覆盖: %q", value)
	}
}

// newHubFleetWithLauncher 造一个指定 launcher 的 provider(桩 exe / 桩 .uproject 都落在 TempDir)。
func newHubFleetWithLauncher(t *testing.T, launcher string, withProject bool) *LocalHubFleetProvider {
	t.Helper()
	dir := t.TempDir()
	exe := filepath.Join(dir, "stub.exe")
	if err := os.WriteFile(exe, []byte("stub"), 0o644); err != nil {
		t.Fatalf("write stub exe: %v", err)
	}
	cfg := conf.LocalHubConf{
		Launcher:       launcher,
		ExecutablePath: exe,
		MapName:        "/Game/Hub?game=/Script/Pandora.PandoraHubGameMode",
		AdvertiseHost:  "127.0.0.1",
		Port:           7777,
		Region:         "cn-1",
		Capacity:       500,
	}
	if withProject {
		proj := filepath.Join(dir, "Pandora.uproject")
		if err := os.WriteFile(proj, []byte("{}"), 0o644); err != nil {
			t.Fatalf("write stub uproject: %v", err)
		}
		cfg.ProjectPath = proj
	}
	p, err := NewLocalHubFleetProvider(cfg)
	if err != nil {
		t.Fatalf("NewLocalHubFleetProvider: %v", err)
	}
	return p
}

// packaged(含缺省零值)的命令行必须与改造前一致:关卡打头,不带 .uproject。
func TestHubBuildArgs_PackagedLauncherUnchanged(t *testing.T) {
	want := []string{
		"/Game/Hub?game=/Script/Pandora.PandoraHubGameMode?MaxPlayers=500",
		"-server", "-log", "-port=7777",
	}
	for _, launcher := range []string{"", conf.LauncherPackaged} {
		p := newHubFleetWithLauncher(t, launcher, false)
		if got := p.buildArgs(); !reflect.DeepEqual(got, want) {
			t.Fatalf("launcher=%q args changed:\n got=%v\nwant=%v", launcher, got, want)
		}
	}
	// packaged 下即便配了 project_path 也必须忽略。
	p := newHubFleetWithLauncher(t, conf.LauncherPackaged, true)
	if got := p.buildArgs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("packaged 不应把 project_path 混入命令行: %v", got)
	}
}

// editor 形态:.uproject 必须排在关卡 URL 之前(UE 只认第一个非 '-' token 作工程),
// 且 -server 仍在(NetMode=NM_DedicatedServer,后端对接与打包 DS 完全一致);
// 末尾必须带 MissingLevelPackage 的 CVar 覆盖(理由见 conf.EditorLauncherCVarArg)。
func TestHubBuildArgs_EditorLauncherPutsProjectFirst(t *testing.T) {
	p := newHubFleetWithLauncher(t, conf.LauncherEditor, true)
	got := p.buildArgs()
	if len(got) != 6 || !strings.HasSuffix(got[0], "Pandora.uproject") {
		t.Fatalf("editor 形态 .uproject 应为第一个参数: %v", got)
	}
	want := []string{
		got[0],
		"/Game/Hub?game=/Script/Pandora.PandoraHubGameMode?MaxPlayers=500",
		"-server", "-log", "-port=7777",
		conf.EditorLauncherCVarArg,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("editor args:\n got=%v\nwant=%v", got, want)
	}
}

// 锁死 MissingLevelPackage 开关的形态与作用域(2026-08-18 秒级无限重连事故)。
//
// 三条都是修复本体,任缺一条这次事故就会原样复发或波及生产:
//   - editor 形态必须带,否则未 cook 的 DS 与 PIE 客户端的 WP cell 命名对不上就踢人;
//   - packaged 绝不能带,那里的 MissingLevelPackage 是真的内容不一致,要保留踢人保护;
//   - 参数**不得含空格**:含空格的 -ExecCmds= 经 exec.Command 传到 Windows 会被引号
//     转义打散,引擎解析不到,表现是"加了等于没加"且不报错。
func TestHubBuildArgs_EditorSkipsMissingLevelDisconnectOnly(t *testing.T) {
	if strings.ContainsAny(conf.EditorLauncherCVarArg, " \t\"") {
		t.Fatalf("CVar 覆盖参数不得含空格/引号(会被 Windows 参数转义打散): %q",
			conf.EditorLauncherCVarArg)
	}
	if !strings.Contains(conf.EditorLauncherCVarArg, "net.SkipMissingLevelDisconnect=1") {
		t.Fatalf("CVar 覆盖参数必须关掉缺 level package 的踢人: %q", conf.EditorLauncherCVarArg)
	}
	for _, launcher := range []string{"", conf.LauncherPackaged} {
		p := newHubFleetWithLauncher(t, launcher, false)
		for _, a := range p.buildArgs() {
			if strings.Contains(a, "SkipMissingLevelDisconnect") {
				t.Fatalf("launcher=%q(cook 过的内容)不得关掉踢人保护: %v", launcher, p.buildArgs())
			}
		}
	}
}

// editor 缺 project_path / 路径不存在 → 构造期 fail-fast(而不是 DS 秒退后客户端干等大厅)。
func TestNewLocalHubFleetProvider_EditorRequiresProjectPath(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "UnrealEditor.exe")
	if err := os.WriteFile(exe, []byte("stub"), 0o644); err != nil {
		t.Fatalf("write stub exe: %v", err)
	}
	base := conf.LocalHubConf{
		Launcher:       conf.LauncherEditor,
		ExecutablePath: exe,
		MapName:        "/Game/Hub",
		AdvertiseHost:  "127.0.0.1",
		Port:           7777,
		Capacity:       500,
	}
	if _, err := NewLocalHubFleetProvider(base); err == nil {
		t.Fatal("launcher=editor 缺 project_path 应构造失败")
	}
	missing := base
	missing.ProjectPath = filepath.Join(dir, "nope", "Pandora.uproject")
	if _, err := NewLocalHubFleetProvider(missing); err == nil {
		t.Fatal("launcher=editor 的 project_path 不存在应构造失败")
	}
	// packaged 不要求 project_path(老配置零改动)。
	packaged := base
	packaged.Launcher = conf.LauncherPackaged
	if _, err := NewLocalHubFleetProvider(packaged); err != nil {
		t.Fatalf("packaged 不应要求 project_path: %v", err)
	}
}

func TestHubMapURLWithMaxPlayers(t *testing.T) {
	tests := []struct {
		name, input, want string
		capacity          int32
		wantErr           bool
	}{
		{"append", "/Game/Hub?game=/Script/Pandora.PandoraHubGameMode", "/Game/Hub?game=/Script/Pandora.PandoraHubGameMode?MaxPlayers=500", 500, false},
		{"exact-existing", "/Game/Hub?MaxPlayers=500?game=HubGM", "/Game/Hub?MaxPlayers=500?game=HubGM", 500, false},
		{"mismatch", "/Game/Hub?MaxPlayers=16", "", 500, true},
		{"duplicate", "/Game/Hub?MaxPlayers=500?maxplayers=500", "", 500, true},
		{"noncanonical-leading-zero", "/Game/Hub?MaxPlayers=0500", "", 500, true},
		{"noncanonical-plus", "/Game/Hub?MaxPlayers=+500", "", 500, true},
		{"invalid-capacity", "/Game/Hub", "", 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := hubMapURLWithMaxPlayers(tc.input, tc.capacity)
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Fatalf("got=%q err=%v, want=%q wantErr=%v", got, err, tc.want, tc.wantErr)
			}
		})
	}
}

func lastEnvValue(env []string, key string) string {
	for i := len(env) - 1; i >= 0; i-- {
		parts := strings.SplitN(env[i], "=", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], key) {
			return parts[1]
		}
	}
	return ""
}
