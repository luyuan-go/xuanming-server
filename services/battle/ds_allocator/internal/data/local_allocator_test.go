// local_allocator_test.go — LocalGameServerAllocator 单测。
//
// 用注入的假进程(fakeProc)绕过真 exec UE Windows DS:fakeProc.Wait() 阻塞直到 Kill(),
// 模拟一个长驻进程,从而能确定性地测端口分配、台账、幂等、释放、Close 全杀。
package data

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/conf"
)

// fakeProc 模拟一个运行中的 DS 进程:Wait 阻塞到 Kill 才返回。
type fakeProc struct {
	killed chan struct{}
	once   sync.Once
	mu     sync.Mutex
	killN  int
}

func newFakeProc() *fakeProc { return &fakeProc{killed: make(chan struct{})} }

func (f *fakeProc) Kill() error {
	f.mu.Lock()
	f.killN++
	f.mu.Unlock()
	f.once.Do(func() { close(f.killed) })
	return nil
}

func (f *fakeProc) Wait() error {
	<-f.killed
	return nil
}

func (f *fakeProc) killCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.killN
}

// newLocalTestAllocator 构造一个注入假进程的 allocator(不校验 executable 文件存在)。
func newLocalTestAllocator(t *testing.T, cfg conf.LocalDSConf) (*LocalGameServerAllocator, *[]*fakeProc) {
	t.Helper()
	if cfg.AdvertiseHost == "" {
		cfg.AdvertiseHost = "127.0.0.1"
	}
	l := &LocalGameServerAllocator{
		cfg:       cfg,
		procs:     make(map[string]*launchedProc),
		usedPorts: make(map[int]struct{}),
	}
	// 关卡表解析器在生产由 main 按 configtable 注入;与关卡无关的用例(端口/幂等/回收)
	// 给一个恒成功的桩即可,否则 Allocate 会按"关卡无法解析"fail-closed。
	if cfg.LoaderMap == "" {
		l.mapURLResolver = func(mapID uint32) (string, error) {
			return fmt.Sprintf("/Game/Test/Map%d", mapID), nil
		}
	}
	var mu sync.Mutex
	created := make([]*fakeProc, 0)
	l.startProc = func(_ string, _ int, _ uint64, _ uint32, _, _, _ string) (dsProcess, error) {
		fp := newFakeProc()
		mu.Lock()
		created = append(created, fp)
		mu.Unlock()
		return fp, nil
	}
	return l, &created
}

func TestNewLocalGameServerAllocator_RequiresExecutable(t *testing.T) {
	if _, err := NewLocalGameServerAllocator(conf.LocalDSConf{}); err == nil {
		t.Fatal("expected error when executable_path empty")
	}
}

func TestNewLocalGameServerAllocator_RejectsBadPortRange(t *testing.T) {
	// 用本测试文件自身当一个"存在的可执行路径"绕过 Stat 校验,只验 port_range。
	if _, err := NewLocalGameServerAllocator(conf.LocalDSConf{
		ExecutablePath: "local_allocator_test.go",
		PortRange:      0,
	}); err == nil {
		t.Fatal("expected error when port_range <= 0")
	}
}

func TestAllocate_ReturnsTrackedAddr(t *testing.T) {
	l, created := newLocalTestAllocator(t, conf.LocalDSConf{PortBase: 7777, PortRange: 10})
	pod, addr, _, err := l.Allocate(context.Background(), 42, 1, "moba5v5", "stable")
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if pod != "pandora-battle-local-42" {
		t.Fatalf("pod=%q", pod)
	}
	if addr != "127.0.0.1:7777" {
		t.Fatalf("addr=%q", addr)
	}
	if len(*created) != 1 {
		t.Fatalf("expected 1 process started, got %d", len(*created))
	}
}

func TestAllocate_Idempotent(t *testing.T) {
	l, created := newLocalTestAllocator(t, conf.LocalDSConf{PortBase: 7777, PortRange: 10})
	_, addr1, _, err := l.Allocate(context.Background(), 7, 1, "", "stable")
	if err != nil {
		t.Fatalf("Allocate#1: %v", err)
	}
	_, addr2, _, err := l.Allocate(context.Background(), 7, 1, "", "stable")
	if err != nil {
		t.Fatalf("Allocate#2: %v", err)
	}
	if addr1 != addr2 {
		t.Fatalf("idempotent addr mismatch: %q vs %q", addr1, addr2)
	}
	if len(*created) != 1 {
		t.Fatalf("expected only 1 process for repeated allocate, got %d", len(*created))
	}
}

func TestAllocatePassesUniqueModelBIdentityAndTokenToProcess(t *testing.T) {
	l, _ := newLocalTestAllocator(t, conf.LocalDSConf{PortBase: 7777, PortRange: 10})
	var gotMatch uint64
	var gotPod, gotUID, gotToken string
	var gotEpoch uint32
	l.SetDSTokenIssuer(func(matchID uint64, podName, instanceUID string, instanceEpoch uint32) (string, BattleCredentialIdentity, error) {
		gotMatch, gotPod, gotUID, gotEpoch = matchID, podName, instanceUID, instanceEpoch
		return "model-b-token", testLocalBattleCredential(podName, instanceUID, instanceEpoch), nil
	}, true)
	l.startProc = func(_ string, _ int, _ uint64, _ uint32, _, _, token string) (dsProcess, error) {
		gotToken = token
		return newFakeProc(), nil
	}

	pod, _, _, err := l.Allocate(context.Background(), 88, 2, "moba", "stable")
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if gotMatch != 88 || gotPod != pod || gotEpoch != 1 || gotToken != "model-b-token" {
		t.Fatalf("issuer/process=(match=%d pod=%q epoch=%d token=%q)", gotMatch, gotPod, gotEpoch, gotToken)
	}
	if _, err := uuid.Parse(gotUID); err != nil {
		t.Fatalf("instance uid 不是 UUID: %q: %v", gotUID, err)
	}
}

// testLocalBattleCredential 造一份与 (pod,uid,epoch) 自洽的完整 tuple,模拟 main 的签发器。
func testLocalBattleCredential(pod, uid string, epoch uint32) BattleCredentialIdentity {
	return BattleCredentialIdentity{
		PodName: pod, InstanceUID: uid, InstanceEpoch: epoch,
		Gen: 1, JTI: "jti-" + uid, WriterEpoch: BattleDSWriterEpochV2,
	}
}

// 心跳应答要回显的 ACK 必须是"下发给该 pod 的那一份",且严格 fail-closed。
// 回不出 ACK 时 UE 会丢掉整个 Command 与驱逐单(实测 DS 每 5s 一条 ACK missing)。
func TestLocalCredentialACKEchoesIssuedTupleAndFailsClosed(t *testing.T) {
	l, _ := newLocalTestAllocator(t, conf.LocalDSConf{PortBase: 7777, PortRange: 10})
	l.SetDSTokenIssuer(func(_ uint64, podName, instanceUID string, instanceEpoch uint32) (string, BattleCredentialIdentity, error) {
		return "model-b-token", testLocalBattleCredential(podName, instanceUID, instanceEpoch), nil
	}, true)

	pod, _, _, err := l.Allocate(context.Background(), 88, 2, "moba", "stable")
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	uid, epoch, ok := l.LocalInstanceIdentity(pod)
	if !ok {
		t.Fatal("LocalInstanceIdentity 应在分配后可用")
	}
	ack, ok := l.LocalCredentialACK(pod)
	if !ok {
		t.Fatal("LocalCredentialACK 应在签发成功后可用")
	}
	// ACK 与实例身份、与签发进 token 的那份 tuple 必须逐字段同源,不能是另造一份。
	if ack.InstanceUID != uid || ack.InstanceEpoch != epoch || ack.Gen != 1 ||
		ack.JTI != "jti-"+uid || ack.WriterEpoch != BattleDSWriterEpochV2 || ack.PodName != pod {
		t.Fatalf("ACK tuple 与下发凭据不一致: %+v (uid=%s epoch=%d)", ack, uid, epoch)
	}
	if _, ok := l.LocalCredentialACK("pandora-battle-local-999"); ok {
		t.Fatal("不在台账的 pod 必须拒绝回显 ACK")
	}
	if _, ok := l.LocalCredentialACK(""); ok {
		t.Fatal("空 pod 必须拒绝回显 ACK")
	}
}

// 签发"成功"但 tuple 不全 = 心跳 ACK 注定回显不出去。required 下必须在拉起前就失败,
// 不能拉起一个注定收不到 allocator 指令的 DS(§14 接线完整性)。
func TestAllocateRejectsIncompleteCredentialTupleWhenRequired(t *testing.T) {
	l, _ := newLocalTestAllocator(t, conf.LocalDSConf{PortBase: 7777, PortRange: 10})
	started := false
	l.startProc = func(_ string, _ int, _ uint64, _ uint32, _, _, _ string) (dsProcess, error) {
		started = true
		return newFakeProc(), nil
	}
	l.SetDSTokenIssuer(func(_ uint64, podName, instanceUID string, instanceEpoch uint32) (string, BattleCredentialIdentity, error) {
		cred := testLocalBattleCredential(podName, instanceUID, instanceEpoch)
		cred.JTI = "" // 少一项即整份不可用
		return "model-b-token", cred, nil
	}, true)

	if _, _, _, err := l.Allocate(context.Background(), 88, 2, "moba", "stable"); err == nil {
		t.Fatal("tuple 不全时 required 下必须 fail-closed")
	}
	if started {
		t.Fatal("fail-closed 时不得拉起 DS 进程")
	}
}

func TestAllocate_DistinctPorts(t *testing.T) {
	l, _ := newLocalTestAllocator(t, conf.LocalDSConf{PortBase: 7777, PortRange: 10})
	_, addr1, _, _ := l.Allocate(context.Background(), 1, 1, "", "stable")
	_, addr2, _, _ := l.Allocate(context.Background(), 2, 1, "", "stable")
	if addr1 == addr2 {
		t.Fatalf("expected distinct ports, both %q", addr1)
	}
}

func TestAllocate_PortExhaustion(t *testing.T) {
	l, _ := newLocalTestAllocator(t, conf.LocalDSConf{PortBase: 7777, PortRange: 2})
	if _, _, _, err := l.Allocate(context.Background(), 1, 1, "", "stable"); err != nil {
		t.Fatalf("Allocate#1: %v", err)
	}
	if _, _, _, err := l.Allocate(context.Background(), 2, 1, "", "stable"); err != nil {
		t.Fatalf("Allocate#2: %v", err)
	}
	_, _, _, err := l.Allocate(context.Background(), 3, 1, "", "stable")
	if err == nil {
		t.Fatal("expected ErrDSNoAvailable on port exhaustion")
	}
	if errcode.As(err) != errcode.ErrDSNoAvailable {
		t.Fatalf("expected ErrDSNoAvailable, got %v", err)
	}
}

func TestRelease_KillsAndFreesPort(t *testing.T) {
	l, created := newLocalTestAllocator(t, conf.LocalDSConf{PortBase: 7777, PortRange: 1})
	pod, _, _, err := l.Allocate(context.Background(), 1, 1, "", "stable")
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if err := l.Release(context.Background(), pod); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if (*created)[0].killCount() == 0 {
		t.Fatal("expected process killed on Release")
	}
	// 端口已释放 → 再分配应成功(池子只有 1 个端口)。
	if _, _, _, err := l.Allocate(context.Background(), 2, 1, "", "stable"); err != nil {
		t.Fatalf("re-Allocate after release: %v", err)
	}
}

func TestRelease_IdempotentOnMissing(t *testing.T) {
	l, _ := newLocalTestAllocator(t, conf.LocalDSConf{PortBase: 7777, PortRange: 10})
	if err := l.Release(context.Background(), "pandora-battle-local-999"); err != nil {
		t.Fatalf("Release missing should be nil, got %v", err)
	}
	if err := l.Release(context.Background(), ""); err != nil {
		t.Fatalf("Release empty should be nil, got %v", err)
	}
}

func TestClose_KillsAll(t *testing.T) {
	l, created := newLocalTestAllocator(t, conf.LocalDSConf{PortBase: 7777, PortRange: 10})
	_, _, _, _ = l.Allocate(context.Background(), 1, 1, "", "stable")
	_, _, _, _ = l.Allocate(context.Background(), 2, 1, "", "stable")
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for i, fp := range *created {
		if fp.killCount() == 0 {
			t.Fatalf("process %d not killed on Close", i)
		}
	}
}

// TestAllocate_ProbeSkipsBusyPort:portProbe 报告某端口被占(幽灵 DS)时,分配跳过它取下一个,
// 保证返回给 UE DS 的端口就是它能真正绑上的端口(否则 UE 静默 fallback,客户端连错 DS)。
func TestAllocate_ProbeSkipsBusyPort(t *testing.T) {
	l, _ := newLocalTestAllocator(t, conf.LocalDSConf{PortBase: 7800, PortRange: 10})
	l.portProbe = func(port int) bool { return port != 7800 } // 7800 被幽灵进程占用
	_, addr, _, err := l.Allocate(context.Background(), 1, 1, "", "stable")
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if addr != "127.0.0.1:7801" {
		t.Fatalf("addr=%q, want 127.0.0.1:7801 (7800 probed busy)", addr)
	}
}

// TestAllocate_ProbeAllBusy:全部端口都被探测报占用 → ErrDSNoAvailable(不静默发出被占端口)。
func TestAllocate_ProbeAllBusy(t *testing.T) {
	l, _ := newLocalTestAllocator(t, conf.LocalDSConf{PortBase: 7800, PortRange: 3})
	l.portProbe = func(int) bool { return false }
	_, _, _, err := l.Allocate(context.Background(), 1, 1, "", "stable")
	if err == nil {
		t.Fatal("expected ErrDSNoAvailable when all ports probed busy")
	}
	if errcode.As(err) != errcode.ErrDSNoAvailable {
		t.Fatalf("expected ErrDSNoAvailable, got %v", err)
	}
}

// 关卡由注入的解析器(生产 = 现查 g_关卡.xlsx)决定,allocator 不再有第二份 map_id → URL 映射。
func TestResolveStartupMap_UsesInjectedResolver(t *testing.T) {
	l, _ := newLocalTestAllocator(t, conf.LocalDSConf{PortBase: 7777, PortRange: 10})
	l.SetMapURLResolver(func(mapID uint32) (string, error) {
		if mapID != 7 {
			return "", fmt.Errorf("关卡表没有 map_id=%d 的行", mapID)
		}
		return "/Game/Test/Level/SonglinTown?game=/Script/Pandora.PandoraPveGameMode", nil
	})

	got, err := l.resolveStartupMap(7)
	if err != nil || got != "/Game/Test/Level/SonglinTown?game=/Script/Pandora.PandoraPveGameMode" {
		t.Fatalf("resolveStartupMap(7)=(%q,%v)", got, err)
	}
	if _, err := l.resolveStartupMap(99); err == nil {
		t.Fatal("关卡表查不到的 map_id 必须报错,绝不回退兜底图")
	}
}

// 关卡表查不到 map_id 时 Allocate 必须整体失败:这是 2026-08-04 事故的护栏 ——
// 旧实现会静默起兜底图,DS 侧关卡门随后判 Mismatch 自杀,分配卡到超时,玩家只看到"排队中"。
func TestAllocate_FailsClosedWhenMapUnresolvable(t *testing.T) {
	l, created := newLocalTestAllocator(t, conf.LocalDSConf{PortBase: 7777, PortRange: 10})
	l.SetMapURLResolver(func(mapID uint32) (string, error) {
		return "", fmt.Errorf("关卡表(g_关卡.xlsx)没有 map_id=%d 的行", mapID)
	})

	_, _, _, err := l.Allocate(context.Background(), 42, 11, "pve_coop", "stable")
	if err == nil {
		t.Fatal("关卡解析失败必须让 Allocate 失败")
	}
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("expected ErrInvalidArg, got %v", err)
	}
	if len(*created) != 0 {
		t.Fatalf("解析失败不得拉起任何 DS 进程,got %d", len(*created))
	}
	// 端口也不能被占用(失败发生在取端口之前)。
	if len(l.usedPorts) != 0 {
		t.Fatalf("失败路径不得占用端口,used=%v", l.usedPorts)
	}
}

// 没有解析器又没配 loader_map = 无处得知 map_id 对应哪张图,只能 fail-closed
// (启动期 conf.ValidateLocalMapSourceConfig 已挡住,这里是最后一道)。
func TestResolveStartupMap_NoSourceFailsClosed(t *testing.T) {
	l, _ := newLocalTestAllocator(t, conf.LocalDSConf{PortBase: 7777, PortRange: 10})
	l.mapURLResolver = nil
	if _, err := l.resolveStartupMap(7); err == nil {
		t.Fatal("既无 config_table 也无 loader_map 时必须报错")
	}
}

// LoaderMap 一旦配置,DS 一律启到加载/分发关卡(不再由 allocator 查表拼 URL),目标副本改由 UE Loader
// 读 PANDORA_MAP_ID → 查同一张 g_关卡.xlsx → ServerTravel 决定。map_id 仍经 env 注入,不影响本参数。
func TestResolveStartupMap_LoaderMapWins(t *testing.T) {
	l, _ := newLocalTestAllocator(t, conf.LocalDSConf{
		LoaderMap: "/Game/Maps/Loader?game=/Script/Pandora.PandoraDSLoaderGameMode",
		PortBase:  7777,
		PortRange: 10,
	})
	l.SetMapURLResolver(func(uint32) (string, error) { return "/Game/Maps/PVE", nil })

	got, err := l.resolveStartupMap(2)
	if err != nil || got != "/Game/Maps/Loader?game=/Script/Pandora.PandoraDSLoaderGameMode" {
		t.Fatalf("loader_map should win, got=(%q,%v)", got, err)
	}
}

// launcher=packaged(默认/缺省)必须与改造前逐字节一致:关卡打头,不带 .uproject。
// 这是「加 editor 模式不影响原有 packaged 路径」的回归护栏。
func TestBuildArgs_PackagedLauncherUnchanged(t *testing.T) {
	base := conf.LocalDSConf{
		PortBase:  7777,
		PortRange: 10,
	}
	want := []string{"/Game/Maps/Default", "-server", "-log", "-port=7788"}

	for _, tc := range []struct {
		name string
		cfg  conf.LocalDSConf
	}{
		// 缺省(零值):老配置不写 launcher 时的行为。
		{"zero value", base},
		// 显式 packaged。
		{"explicit packaged", func() conf.LocalDSConf {
			c := base
			c.Launcher = conf.LauncherPackaged
			return c
		}()},
		// packaged 下即便误配了 project_path 也必须忽略,绝不混入命令行。
		{"packaged ignores project_path", func() conf.LocalDSConf {
			c := base
			c.Launcher = conf.LauncherPackaged
			c.ProjectPath = `C:\x\Pandora.uproject`
			return c
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l, _ := newLocalTestAllocator(t, tc.cfg)
			got := l.buildArgs(7788, "/Game/Maps/Default")
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("packaged args changed:\n got=%v\nwant=%v", got, want)
			}
		})
	}
}

// launcher=editor 时 .uproject 必须是命令行**第一个**参数(排在关卡 URL 之前):
// UE 的 LaunchSetGameName 只把首个不以 '-' 开头的 token 当工程,顺序错了引擎会把关卡路径
// 当工程名解析失败 → DS 秒退。-server 仍在,NetMode 依旧是 NM_DedicatedServer。
func TestBuildArgs_EditorLauncherPutsProjectFirst(t *testing.T) {
	l, _ := newLocalTestAllocator(t, conf.LocalDSConf{
		Launcher:    conf.LauncherEditor,
		ProjectPath: `F:\work\Pandora-Client-SVN\Pandora\Pandora.uproject`,
		PortBase:    7777,
		PortRange:   10,
	})

	got := l.buildArgs(7788, "/Game/Maps/PVE")
	want := []string{
		`F:\work\Pandora-Client-SVN\Pandora\Pandora.uproject`,
		"/Game/Maps/PVE",
		"-server", "-log", "-port=7788",
		conf.EditorLauncherCVarArg,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("editor args:\n got=%v\nwant=%v", got, want)
	}
}

// 锁死 MissingLevelPackage 开关的形态与作用域(2026-08-18 秒级无限重连事故)。
//
// 三条都是修复本体,任缺一条这次事故就会原样复发或波及生产:
//   - editor 形态必须带,否则未 cook 的 DS 与 PIE 客户端的 WP cell 命名对不上就踢人,
//     而客户端拿不到 close reason,只会当瞬态掉线一遍遍连回同一个 DS;
//   - packaged 绝不能带,那里的 MissingLevelPackage 是真的内容不一致,要保留踢人保护;
//   - 参数**不得含空格**:含空格的 -ExecCmds= 经 exec.Command 传到 Windows 会被引号
//     转义打散,引擎解析不到,表现是"加了等于没加"且不报错。
func TestBuildArgs_EditorSkipsMissingLevelDisconnectOnly(t *testing.T) {
	if strings.ContainsAny(conf.EditorLauncherCVarArg, " \t\"") {
		t.Fatalf("CVar 覆盖参数不得含空格/引号(会被 Windows 参数转义打散): %q",
			conf.EditorLauncherCVarArg)
	}
	if !strings.Contains(conf.EditorLauncherCVarArg, "net.SkipMissingLevelDisconnect=1") {
		t.Fatalf("CVar 覆盖参数必须关掉缺 level package 的踢人: %q", conf.EditorLauncherCVarArg)
	}

	l, _ := newLocalTestAllocator(t, conf.LocalDSConf{
		Launcher:  conf.LauncherPackaged,
		PortBase:  7777,
		PortRange: 10,
	})
	for _, a := range l.buildArgs(7788, "/Game/Maps/PVE") {
		if strings.Contains(a, "SkipMissingLevelDisconnect") {
			t.Fatalf("packaged(cook 过的内容)不得关掉踢人保护: %v",
				l.buildArgs(7788, "/Game/Maps/PVE"))
		}
	}
}

// editor 形态仍必须带 -server:它决定 NetMode=NM_DedicatedServer,
// 进而决定 IsRunningDedicatedServer()=true → DS 子系统/心跳/在线准入全链路照常工作。
// 丢了 -server 就退化成 listen server,后端对接全断,故单列一条护栏。
func TestBuildArgs_EditorKeepsServerFlag(t *testing.T) {
	l, _ := newLocalTestAllocator(t, conf.LocalDSConf{
		Launcher:    conf.LauncherEditor,
		ProjectPath: `C:\p\Pandora.uproject`,
		PortBase:    7777,
		PortRange:   10,
	})
	var hasServer bool
	for _, a := range l.buildArgs(7788, "/Game/Maps/Default") {
		if a == "-server" {
			hasServer = true
		}
	}
	if !hasServer {
		t.Fatal("editor 形态必须保留 -server,否则退化成 listen server,后端链路全断")
	}
}

func TestNewLocalGameServerAllocator_EditorRequiresProjectPath(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "UnrealEditor.exe")
	if err := os.WriteFile(exe, []byte("stub"), 0o644); err != nil {
		t.Fatalf("write stub exe: %v", err)
	}

	// project_path 缺失 → 构造失败(fail-fast,而不是让 DS 秒退后干等 ready 超时)。
	if _, err := NewLocalGameServerAllocator(conf.LocalDSConf{
		Launcher:       conf.LauncherEditor,
		ExecutablePath: exe,
		PortRange:      10,
	}); err == nil {
		t.Fatal("launcher=editor 缺 project_path 应构造失败")
	}

	// project_path 指向不存在的文件 → 同样构造失败。
	if _, err := NewLocalGameServerAllocator(conf.LocalDSConf{
		Launcher:       conf.LauncherEditor,
		ExecutablePath: exe,
		ProjectPath:    filepath.Join(dir, "missing", "Pandora.uproject"),
		PortRange:      10,
	}); err == nil {
		t.Fatal("launcher=editor 的 project_path 不存在应构造失败")
	}

	// 齐活 → 构造成功。
	proj := filepath.Join(dir, "Pandora.uproject")
	if err := os.WriteFile(proj, []byte("{}"), 0o644); err != nil {
		t.Fatalf("write stub uproject: %v", err)
	}
	if _, err := NewLocalGameServerAllocator(conf.LocalDSConf{
		Launcher:       conf.LauncherEditor,
		ExecutablePath: exe,
		ProjectPath:    proj,
		PortRange:      10,
	}); err != nil {
		t.Fatalf("齐活时应构造成功: %v", err)
	}

	// packaged 不要求 project_path(旧配置零改动)。
	if _, err := NewLocalGameServerAllocator(conf.LocalDSConf{
		ExecutablePath: exe,
		PortRange:      10,
	}); err != nil {
		t.Fatalf("packaged 不应要求 project_path: %v", err)
	}
}

func TestBuildEnvCarriesLocalProfileAndRejectsOverride(t *testing.T) {
	l, _ := newLocalTestAllocator(t, conf.LocalDSConf{
		PortBase: 7777, PortRange: 10,
		ExtraEnv: map[string]string{"pandora_ds_local_profile": "evil"},
	})
	env := l.buildEnv("pandora-battle-local-9", 9, 1, "moba", "model-b-token")
	if value := lastLocalEnvValue(env, "PANDORA_DS_LOCAL_PROFILE"); value != "local-off-v1" {
		t.Fatalf("profile=%q", value)
	}
	if value := lastLocalEnvValue(env, "PANDORA_DS_TOKEN"); value != "model-b-token" {
		t.Fatalf("token=%q", value)
	}
}

func lastLocalEnvValue(env []string, key string) string {
	for i := len(env) - 1; i >= 0; i-- {
		parts := strings.SplitN(env[i], "=", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], key) {
			return parts[1]
		}
	}
	return ""
}
