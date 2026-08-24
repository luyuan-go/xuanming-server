// local_allocator.go — 本机拉起 Windows Dedicated Server 进程的调试用 GameServerAllocator。
//
// 这是与 AgonesGameServerAllocator(Linux 生产,见 agones_allocator.go)并列的第二种
// DS 启动方式,专供本机联调:匹配成局后 ds_allocator 直接 exec 打包好的 UE Windows DS,
// 分配一个本机端口,返回真实地址(host:port)给客户端 NetDriver;Release / 心跳超时
// abandoned 时 Kill 进程。三种方式共用 biz.GameServerAllocator 接口,biz 逻辑零改。
//
// 设计要点:
//   - 进程台账(podName → 进程 + 端口)在内存维护,带互斥锁;ds_allocator 退出时 Close 全杀。
//   - 每个 DS 进程一个 reaper goroutine Wait(),进程自行退出(崩溃)时清理台账释放端口
//     (镜像仍靠心跳超时 sweep 标 abandoned,与 Agones pod 崩溃同语义)。
//   - Allocate 幂等:同 podName(由 matchID 派生)已在台账则直接返回原地址,不重复拉进程。
//   - 启动函数 startProc 抽成字段,单测可注入假进程,避免真的 exec UE。
package data

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/dsmetadata"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/conf"
)

// dsProcess 抽象一个已拉起的 DS 进程,便于单测注入假实现。
type dsProcess interface {
	// Kill 终止进程(已退出则应为 no-op 不报致命错)。
	Kill() error
	// Wait 阻塞直到进程退出。
	Wait() error
}

// execProcess 是 dsProcess 的真实现,包一个 *exec.Cmd。
type execProcess struct {
	cmd  *exec.Cmd
	logF *os.File // 日志文件句柄,进程退出后关闭
}

func (e *execProcess) Kill() error {
	if e.cmd.Process == nil {
		return nil
	}
	// Windows:UE DS(PandoraServer.exe)可能派生子进程,只 kill 父进程会留下仍占监听端口的
	// 子进程(幽灵 DS),导致后续对局撞端口。taskkill /T 杀整棵进程树,/F 强制,确保端口真正释放。
	if runtime.GOOS == "windows" {
		kc := exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(e.cmd.Process.Pid)) //nolint:gosec // pid 来自本进程派生的 DS
		if err := kc.Run(); err == nil {
			return nil
		}
		// taskkill 不可用/失败 → 回退直接 kill 父进程(至少不泄漏父进程)。
	}
	return e.cmd.Process.Kill()
}

func (e *execProcess) Wait() error {
	err := e.cmd.Wait()
	if e.logF != nil {
		_ = e.logF.Close()
	}
	return err
}

// launchedProc 是台账里的一条记录。
type launchedProc struct {
	proc dsProcess
	port int
	addr string
	// instanceUID / instanceEpoch 是本机 DS 的 exact 实例身份,与下发给该进程的
	// DS 回调令牌**同源**(Allocate 处一次生成,既签进令牌又留台账)。留存的意义在于
	// 幂等重分配必须返回同一身份:身份漂移会让票据绑到一个不存在的实例。
	instanceUID   string
	instanceEpoch uint32
	// cred 是经 env 下发给该进程的**完整凭据身份**(与 instanceUID/instanceEpoch 同一次签发)。
	// 必须整组留存而不只留 uid/epoch:UE 的 SendBattleHeartbeat 用 IsBoundToRequest 把心跳应答
	// 里的 CredentialAck 与 DS 自持凭据逐字段比对(uid/epoch/gen/jti/writer_epoch),缺一即判
	// "heartbeat response credential ACK missing" —— 见 LocalCredentialACK 注释。
	// local 不续期 → 全程不变。
	cred BattleCredentialIdentity
}

// completeForACK 判定五元组是否齐备,可用于回显心跳 ACK。缺任一项都不得回显半截 ACK:
// UE 侧 IsComplete 会拒,回半截只会把"缺字段"伪装成"不匹配",更难排查。
func (c BattleCredentialIdentity) completeForACK() bool {
	return c.InstanceUID != "" && c.InstanceEpoch != 0 && c.Gen != 0 && c.JTI != "" && c.WriterEpoch != 0
}

// LocalGameServerAllocator 在本机 exec UE Windows Dedicated Server 进程。
type LocalGameServerAllocator struct {
	cfg conf.LocalDSConf

	mu        sync.Mutex
	procs     map[string]*launchedProc // podName → 进程记录
	usedPorts map[int]struct{}
	// pendingRosters:matchID → 待随 DS env 下发的权威准入元数据(见 SetPendingBattleRoster)。
	// 由 Allocate 在 buildEnv 时消费并删除;受 mu 保护。
	pendingRosters map[uint64]localBattleRoster

	// startProc 拉起一个 DS 进程;单测注入假实现绕过真 exec。
	// token 是本对局的 DS 回调令牌(Allocate 处一次性签发,避免二次签发失败只告警的空窗)。
	// mapURL 是本次已解析定型的启动关卡(Allocate 处一次性解析,见 resolveStartupMap)。
	startProc func(podName string, port int, matchID uint64, mapID uint32, mapURL, gameMode, token string) (dsProcess, error)

	// portProbe 探测端口在本机是否真的空闲(可绑定)。nil=不探测(单测默认放行)。
	// 用于挡住「台账已释放但进程未退出(幽灵 DS)」或外部程序占用的端口:否则 allocator 把
	// -port=X 传给 UE DS,X 被占时 UE 会静默 fallback 到 X+1,导致 allocator 记录/返回的端口(X)
	// 与 DS 实际监听端口(X+1)不一致,新对局客户端拿新 ticket 却连到 X 上的旧 DS,被 PreLogin 拒。
	portProbe func(port int) bool

	// dsTokenIssuer 签发 DS 回调服务令牌(审核 P1 #1;main 在 ds_auth.secret 已配时注入)。
	// 非 nil 时 defaultStart 把令牌注入 DS 进程 env PANDORA_DS_TOKEN,DS 回调时带 Bearer 头。
	// 签发失败只告警不阻断拉起(guard 默认 off/permissive,先保对局可开)。
	//
	// 除 token 外还必须回吐**完整凭据身份**:心跳应答要逐字段回显它作为 ACK(见
	// LocalCredentialACK),只拿到 token 串是回显不出 gen/jti 的。
	dsTokenIssuer func(matchID uint64, podName, instanceUID string, instanceEpoch uint32) (token string, cred BattleCredentialIdentity, err error)
	// dsTokenRequired 为 guard=enforce 时 true:签发失败则 fail-closed 不拉起 DS(否则该 DS
	// 回调会被 enforce 守卫全拒)。off/permissive 下 false,签发失败只告警照拉。
	dsTokenRequired bool

	// mapURLResolver 把 map_id 解析成 DS 要加载的关卡 URL,由 main 在配置表就绪时注入
	// (与 SetDSTokenIssuer 同范式,不动 GameServerAllocator 接口)。唯一实现是"现查关卡表"
	// —— 关卡数据的唯一权威源是 g_关卡.xlsx(configtable),allocator 不再留第二份手抄映射。
	// 每次 Allocate 现查:配置表热更(ConfigTableAdminService.ReloadConfigTable)后新增的副本
	// 无需重启本服务即可开局。
	//
	// nil 表示未注入:只有配了 loader_map(DS 侧自己查表决定目标图)时才允许,否则 Allocate
	// fail-closed —— 见 resolveStartupMap。conf.ValidateLocalMapSourceConfig 在启动期就挡住。
	mapURLResolver func(mapID uint32) (string, error)
}

// SetMapURLResolver 注入「map_id → 关卡 URL」解析器(main 在 config_table.dir 配置时调用)。
func (l *LocalGameServerAllocator) SetMapURLResolver(f func(mapID uint32) (string, error)) {
	l.mapURLResolver = f
}

// SetDSTokenIssuer 注入 DS 回调令牌签发器(可选依赖,main 在 ds_auth.secret 已配时调用)。
// required=true(guard=enforce / local-off-v1)时签发失败会让 Allocate 返回错误(fail-closed)。
func (l *LocalGameServerAllocator) SetDSTokenIssuer(f func(matchID uint64, podName, instanceUID string, instanceEpoch uint32) (string, BattleCredentialIdentity, error), required bool) {
	l.dsTokenIssuer = f
	l.dsTokenRequired = required
}

// NewLocalGameServerAllocator 构造本机 DS 拉起器。
//
// 失败场景(返 error,main 据此 fatal):
//   - ExecutablePath 空
//   - ExecutablePath 指向的文件不存在
//   - launcher=editor 但 ProjectPath 空 / 指向的 .uproject 不存在
//   - PortRange <= 0
func NewLocalGameServerAllocator(cfg conf.LocalDSConf) (*LocalGameServerAllocator, error) {
	if cfg.ExecutablePath == "" {
		return nil, fmt.Errorf("local_ds: executable_path required when enabled")
	}
	if _, err := os.Stat(cfg.ExecutablePath); err != nil {
		return nil, fmt.Errorf("local_ds: executable_path %q not found: %w", cfg.ExecutablePath, err)
	}
	// editor 形态必须能定位 .uproject:否则 UnrealEditor.exe 会把关卡路径当工程名解析失败,
	// 表现为 DS 秒退 + ready 等待超时,排查成本高 —— 在启动时 fail-fast。
	if cfg.Launcher == conf.LauncherEditor {
		if cfg.ProjectPath == "" {
			return nil, fmt.Errorf("local_ds: project_path required when launcher=%s", conf.LauncherEditor)
		}
		if _, err := os.Stat(cfg.ProjectPath); err != nil {
			return nil, fmt.Errorf("local_ds: project_path %q not found: %w", cfg.ProjectPath, err)
		}
	}
	if cfg.PortRange <= 0 {
		return nil, fmt.Errorf("local_ds: port_range must be > 0")
	}
	l := &LocalGameServerAllocator{
		cfg:       cfg,
		procs:     make(map[string]*launchedProc),
		usedPorts: make(map[int]struct{}),
	}
	l.startProc = l.defaultStart
	l.portProbe = defaultPortProbe
	return l, nil
}

// Allocate 拉起一个本机 DS 进程,返回 (podName, host:port)。
func (l *LocalGameServerAllocator) Allocate(_ context.Context, matchID uint64, mapID uint32, gameMode, releaseTrack string) (string, string, string, error) {
	podName := fmt.Sprintf("pandora-battle-local-%d", matchID)

	l.mu.Lock()
	defer l.mu.Unlock()

	// 幂等:同对局已拉起 → 直接返回原地址。
	if p, ok := l.procs[podName]; ok {
		return podName, p.addr, releaseTrack, nil
	}

	// 关卡在拉起进程前就解析定型:解析失败必须让整次分配失败,**绝不回退兜底图**。
	// 起错图的 DS 会被 DS 侧关卡门判 Mismatch 后自杀,分配卡在 warming 直到 ready_wait 超时,
	// 玩家侧只看到"一直排队中",排查成本极高(2026-08-04 map_id=11 实测)。这里失败则
	// matchmaker 立刻拿到一条写明原因的错误。
	mapURL, mapErr := l.resolveStartupMap(mapID)
	if mapErr != nil {
		plog.With(context.Background()).Errorw("msg", "local_ds_map_resolve_failed",
			"match_id", matchID, "map_id", mapID, "err", mapErr,
			"hint", "关卡由 g_关卡.xlsx(configtable/dist/level.json)权威决定;新增战斗关卡改表重导即可,不改 yaml")
		return "", "", "", errcode.New(errcode.ErrInvalidArg,
			"local_ds: resolve startup map for map_id %d: %v", mapID, mapErr)
	}

	port, ok := l.pickPortLocked()
	if !ok {
		return "", "", "", errcode.New(errcode.ErrDSNoAvailable,
			"local_ds: no free port in [%d,%d) for match %d",
			l.cfg.PortBase, l.cfg.PortBase+l.cfg.PortRange, matchID)
	}

	// DS 回调令牌一次性签发(审核 P1):在此签一次并透传给进程 env,避免“预签验证 + 启动再签”
	// 的二次签发失败只告警的空窗(enforce 下二次失败会拉起一个无令牌、回调必被拒的 DS)。
	//   enforce(dsTokenRequired):签发失败 fail-closed,不拉起。
	//   off/permissive:签发失败只告警,token 置空(DS 无令牌照常运行,守卫放行)。
	var dsToken string
	var dsCred BattleCredentialIdentity
	instanceUID := uuid.NewString()
	const instanceEpoch uint32 = 1
	if l.dsTokenIssuer != nil {
		tok, cred, terr := l.dsTokenIssuer(matchID, podName, instanceUID, instanceEpoch)
		switch {
		case terr != nil:
			if l.dsTokenRequired {
				return "", "", "", errcode.New(errcode.ErrDSAllocationFailed,
					"ds_callback_token sign failed under enforce for match %d: %v", matchID, terr)
			}
			plog.With(context.Background()).Warnw("msg", "ds_callback_token_sign_failed", "match_id", matchID, "err", terr)
		// 签发"成功"但 tuple 不全 = 心跳 ACK 一定回显不出去(DS 会永远拿不到准入租约、
		// 收不到 stop/驱逐指令)。required 下这属于半成品接线,必须在拉起前就失败,
		// 而不是拉起一个注定每 5s 打一条 ACK missing 的 DS(§14 接线完整性)。
		case !cred.completeForACK() || cred.InstanceUID != instanceUID || cred.InstanceEpoch != instanceEpoch:
			if l.dsTokenRequired {
				return "", "", "", errcode.New(errcode.ErrDSAllocationFailed,
					"ds_callback_token for match %d signed with incomplete/mismatched credential tuple", matchID)
			}
			plog.With(context.Background()).Warnw("msg", "ds_callback_credential_incomplete",
				"match_id", matchID, "pod", podName,
				"hint", "心跳应答无法回显绑定式 ACK,DS 将拒收 allocator 指令")
			dsToken = tok
		default:
			dsToken = tok
			dsCred = cred
			dsCred.PodName = podName
		}
	}

	proc, err := l.startProc(podName, port, matchID, mapID, mapURL, gameMode, dsToken)
	if err != nil {
		return "", "", "", errcode.New(errcode.ErrDSAllocationFailed,
			"local_ds: launch match %d on port %d: %v", matchID, port, err)
	}

	addr := fmt.Sprintf("%s:%d", l.cfg.AdvertiseHost, port)
	lp := &launchedProc{proc: proc, port: port, addr: addr,
		instanceUID: instanceUID, instanceEpoch: instanceEpoch, cred: dsCred}
	l.procs[podName] = lp
	l.usedPorts[port] = struct{}{}

	go l.reap(podName, lp)

	return podName, addr, releaseTrack, nil
}

// localBattleRoster 是一局待拉起 DS 的权威准入元数据(mode=local 版的 Agones annotation)。
type localBattleRoster struct {
	playerIDs             []uint64
	combatFactionByPlayer map[uint64]uint32
	allocationID          string
	releaseTrack          string
}

// SetPendingBattleRoster 在拉起 DS **之前**登记本局的权威准入元数据。
//
// 为什么必须有:UE 的 APandoraBattleGameMode::ApplyAgonesAdmissionMetadata 只从 Agones
// GameServer annotation 装载 ExpectedPlayers 与权威阵营;mode=local 没有 Agones,两者恒空:
//   - 花名册空 → APandoraPveGameMode::EvaluateSoloDungeonLeaveRequest 因 roster_count==0
//     返回 AuthorityNotReady,玩家能正常战斗但点「退出副本/失败结算」永远被拒
//     (2026-08-04 实测,DS 日志:result=4 canonical_pve=1 roster_count=0);
//   - 阵营空 → APandoraBattleGameMode::ResolveCampForSpawn 返回 RejectSpawn 且**禁止回退**
//     默认 Pawn,玩家进图后根本没有角色,表现为"进副本就卡死"
//     (2026-08-13 实测,DS 日志:拒绝出生 present=0 valid=0 mapping_count=0)。
//
// 本地没有 annotation 通道,故改用 env 投递同一份事实(与 PANDORA_DS_TOKEN 同机制)。
//
// 必须在 Allocate 之前调用:DS 进程在 Allocate 内 exec,env 那一刻就已定型。
// 幂等:同 matchID 重复登记以最后一次为准;Allocate 消费后即删除,避免台账无界增长。
// 入参一律深拷贝:调用方(biz)的 slice / map 在本次 RPC 返回前仍可能被复用或修改。
func (l *LocalGameServerAllocator) SetPendingBattleRoster(matchID uint64, playerIDs []uint64,
	combatFactionByPlayer map[uint64]uint32, allocationID, releaseTrack string) {
	if matchID == 0 {
		return
	}
	ids := append([]uint64(nil), playerIDs...)
	var factions map[uint64]uint32
	if len(combatFactionByPlayer) > 0 {
		factions = make(map[uint64]uint32, len(combatFactionByPlayer))
		for playerID, factionID := range combatFactionByPlayer {
			factions[playerID] = factionID
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.pendingRosters == nil {
		l.pendingRosters = make(map[uint64]localBattleRoster)
	}
	l.pendingRosters[matchID] = localBattleRoster{
		playerIDs: ids, combatFactionByPlayer: factions,
		allocationID: allocationID, releaseTrack: releaseTrack,
	}
}

// canonicalRosterText 把玩家 ID 编成 UE 侧 ParseCanonicalBattleRoster 认的 canonical 文本:
// 十进制、逗号分隔、**严格升序去重**、非零、上限 128 人。
// 任一条不满足 UE 会整份判非法并保持空名单(fail-closed),所以这里宁可返回空串不投递,
// 也不投一份会被对面拒掉的半成品 —— 那只会把「没投」伪装成「投了但格式错」,更难查。
func canonicalRosterText(playerIDs []uint64) string {
	const maxBattleRosterPlayers = 128
	uniq := make([]uint64, 0, len(playerIDs))
	seen := make(map[uint64]struct{}, len(playerIDs))
	for _, id := range playerIDs {
		if id == 0 {
			return ""
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		uniq = append(uniq, id)
	}
	if len(uniq) == 0 || len(uniq) > maxBattleRosterPlayers {
		return ""
	}
	slices.Sort(uniq)
	parts := make([]string, 0, len(uniq))
	for _, id := range uniq {
		parts = append(parts, strconv.FormatUint(id, 10))
	}
	return strings.Join(parts, ",")
}

// LocalInstanceIdentity 返回本机 DS 进程的 exact 实例身份(供 legacy 面回填战斗记录)。
//
// 为什么必须有:legacy(非 Model B)分配路径只回 (pod, addr, track),BattleStorageRecord
// 的 gameserver_uid / instance_epoch 恒为零值,matchmaker 据此判「ds_allocator 未回填完整
// DS 目标」拒签 v2 战斗票,对局直接判 FAILED —— 玩家永远进不去副本
// (2026-08-04 mode=local 实测)。这里给出的身份**就是**签进该 DS 回调令牌的那一组
// (Allocate 处同一次生成),所以与 DS 自报身份逐字段相等,不是另造一份。
//
// pod 不在台账(已回收 / 名字不符)时返回 false —— 调用方必须据此**不回填**,
// 绝不能拿别的进程或半截身份糊一个实例身份出来。
func (l *LocalGameServerAllocator) LocalInstanceIdentity(podName string) (string, uint32, bool) {
	if podName == "" {
		return "", 0, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	lp, ok := l.procs[podName]
	if !ok || lp.instanceUID == "" || lp.instanceEpoch == 0 {
		return "", 0, false
	}
	return lp.instanceUID, lp.instanceEpoch, true
}

// LocalCredentialACK 返回下发给指定 pod 的完整凭据身份,供 legacy 心跳应答回显 ACK。
//
// 为什么必须有:UE 的 SendBattleHeartbeat 无条件用 IsBoundToRequest 校验应答里的
// CredentialAck(uid/instance_epoch/gen/jti/writer_epoch 五项须与 DS 自持凭据逐字段相等)。
// 不通过时它会**清空 Command 与 BattleEvictionOrders 并置错**,于是:
//   - allocator 的 stop / drain 指令永远送不到 DS(local 面靠 killStrandedDS 兜底,
//     Agones+legacy 灰度面则真的没人收);
//   - 精确驱逐单被整份丢弃,PendingBattleDepartureAcks 永不消费,每跳重发;
//   - NotifyAuthorizedActiveHeartbeat 不触发,本地准入租约永不打开
//     (local-off-v1 战斗票走 HS256 档不撞这道门,验票档会直接拒收玩家)。
//
// 2026-08-05 实测 DS 日志每 5s 一条 "heartbeat response credential ACK missing"。
//
// 为什么这不是"伪造回显":ACK 的值直接取自本进程签发、并经 env 下发给该 DS 的**同一份**
// 凭据(Allocate 处一次生成),确是"服务端仍授权本实例"的真实证据。且严格 fail-closed:
//   - pod 不在台账(已回收 / 名字不符)→ 不回显;
//   - 凭据五元组不全 → 不回显(绝不糊半截 ACK);
//   - 只有 legacy 心跳入口会走到这里,Model B 心跳有自己的 ActivateHeartbeat 线性化点。
func (l *LocalGameServerAllocator) LocalCredentialACK(podName string) (BattleCredentialIdentity, bool) {
	if podName == "" {
		return BattleCredentialIdentity{}, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	lp, ok := l.procs[podName]
	if !ok || !lp.cred.completeForACK() {
		return BattleCredentialIdentity{}, false
	}
	return lp.cred, true
}

// Release 终止指定 DS 进程;台账无此记录视作已释放(幂等)。
func (l *LocalGameServerAllocator) Release(_ context.Context, podName string) error {
	if podName == "" {
		return nil
	}
	l.mu.Lock()
	lp, ok := l.procs[podName]
	if ok {
		delete(l.procs, podName)
		delete(l.usedPorts, lp.port)
	}
	l.mu.Unlock()

	if !ok {
		return nil
	}
	if err := lp.proc.Kill(); err != nil {
		return errcode.New(errcode.ErrDSAllocationFailed, "local_ds: kill %s: %v", podName, err)
	}
	return nil
}

// Close 终止全部在管 DS 进程(ds_allocator 退出时调用,避免遗留孤儿 DS)。
func (l *LocalGameServerAllocator) Close() error {
	l.mu.Lock()
	procs := l.procs
	l.procs = make(map[string]*launchedProc)
	l.usedPorts = make(map[int]struct{})
	l.mu.Unlock()

	for _, lp := range procs {
		_ = lp.proc.Kill()
	}
	return nil
}

// reap 等待进程退出后清理台账释放端口(仅当台账里仍是同一条记录,避免与 Release/重拉竞态)。
func (l *LocalGameServerAllocator) reap(podName string, lp *launchedProc) {
	_ = lp.proc.Wait()
	l.mu.Lock()
	defer l.mu.Unlock()
	if cur, ok := l.procs[podName]; ok && cur == lp {
		delete(l.procs, podName)
		delete(l.usedPorts, lp.port)
	}
}

// pickPortLocked 在端口池里取一个空闲端口(调用方须持锁)。
// 除排除本 allocator 已分配的端口(usedPorts)外,还用 portProbe 实际探测端口在本机可绑定,
// 跳过被幽灵 DS / 外部程序占用的端口,保证发给 UE DS 的 -port 就是它能真正绑上的端口。
func (l *LocalGameServerAllocator) pickPortLocked() (int, bool) {
	for p := l.cfg.PortBase; p < l.cfg.PortBase+l.cfg.PortRange; p++ {
		if _, used := l.usedPorts[p]; used {
			continue
		}
		if l.portProbe != nil && !l.portProbe(p) {
			continue // 端口被占(幽灵 DS / 外部程序)→ 跳过,避免 UE 静默换端口
		}
		return p, true
	}
	return 0, false
}

// defaultPortProbe 探测端口在所有网卡上 UDP+TCP 是否可绑定(UE DS NetDriver 用 UDP,保守起见
// TCP 也探,兼容 TCP/WebSocket 传输)。绑定成功立即释放再交给 UE 启动;探测与 UE 真正绑定之间
// 有极短 TOCTOU 窗口,但幽灵 DS 是持续占用能被稳定挡住,usedPorts 又已防 allocator 自身重复分配。
func defaultPortProbe(port int) bool {
	uc, err := net.ListenUDP("udp", &net.UDPAddr{Port: port})
	if err != nil {
		return false
	}
	_ = uc.Close()
	tl, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return false
	}
	_ = tl.Close()
	return true
}

// defaultStart 是 startProc 的真实现:exec UE Windows DS 并把 stdout/stderr 落盘。
func (l *LocalGameServerAllocator) defaultStart(podName string, port int, matchID uint64, mapID uint32, mapURL, gameMode, token string) (dsProcess, error) {
	cmd := exec.Command(l.cfg.ExecutablePath, l.buildArgs(port, mapURL)...) //nolint:gosec // 路径来自受信本机配置
	if l.cfg.WorkingDir != "" {
		cmd.Dir = l.cfg.WorkingDir
	}
	cmd.Env = l.buildEnv(podName, matchID, mapID, gameMode, token)

	var logF *os.File
	if l.cfg.LogDir != "" {
		if err := os.MkdirAll(l.cfg.LogDir, 0o755); err == nil {
			if f, ferr := os.Create(filepath.Join(l.cfg.LogDir, podName+".log")); ferr == nil {
				logF = f
				cmd.Stdout = f
				cmd.Stderr = f
			}
		}
	}

	if err := cmd.Start(); err != nil {
		if logF != nil {
			_ = logF.Close()
		}
		return nil, err
	}
	return &execProcess{cmd: cmd, logF: logF}, nil
}

// resolveStartupMap 返回 DS 进程「首个加载」的关卡 URL(命令行位置参数),两条权威路径:
//   - LoaderMap 非空 → 统一启到加载 / 分发关卡;目标副本由 UE 侧 Loader GameMode 读 PANDORA_MAP_ID
//     查 g_关卡.xlsx 后 ServerTravel 决定(生产 Agones 走的就是这条)。
//   - 否则 → 由注入的 mapURLResolver 现查关卡表拼出目标图 URL(本机 local 默认路径)。
//
// 两条都以 g_关卡.xlsx 为唯一事实源,区别只是"谁来查表"。没有第三条:解析器缺失或查表失败一律报错,
// 不存在"未命中回退默认图"——那正是 2026-08-04 事故的直接成因(见 Allocate 处注释)。
//
// 无论走哪条,PANDORA_MAP_ID env 都已注入(见 buildEnv),故切换只影响「首个加载哪张图」。
func (l *LocalGameServerAllocator) resolveStartupMap(mapID uint32) (string, error) {
	if l.cfg.LoaderMap != "" {
		return l.cfg.LoaderMap, nil
	}
	if l.mapURLResolver == nil {
		return "", fmt.Errorf("未接入关卡表:请配 config_table.dir(现查 g_关卡.xlsx)或 local_ds.loader_map")
	}
	mapURL, err := l.mapURLResolver(mapID)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(mapURL) == "" {
		return "", fmt.Errorf("map_id=%d 解析出的关卡 URL 为空", mapID)
	}
	return mapURL, nil
}

// buildArgs 拼 UE DS 命令行:[.uproject] + 关卡 + -server -log -port=<port> + 额外参数。
// mapURL 由 Allocate 经 resolveStartupMap 解析定型后传入(同一次分配只解析一次,
// 避免"校验用 A、启动用 B"在配置表热更瞬间劈叉)。
//
// launcher=editor 时在最前面插 .uproject:UE 的 LaunchSetGameName 只把命令行里**第一个**不以 '-'
// 开头的 token 当工程/关卡,所以 .uproject 必须排在关卡 URL 之前,否则引擎会把关卡路径当工程名解析失败。
// -server 两种形态都带 —— NetMode 恒为 NM_DedicatedServer,IsRunningDedicatedServer() 为 true,
// DS 子系统/心跳/在线准入全链路与打包 DS 完全一致,后端对此无感。
func (l *LocalGameServerAllocator) buildArgs(port int, mapURL string) []string {
	args := make([]string, 0, 5+len(l.cfg.ExtraArgs))
	if l.cfg.Launcher == conf.LauncherEditor && l.cfg.ProjectPath != "" {
		args = append(args, l.cfg.ProjectPath)
	}
	if mapURL != "" {
		args = append(args, mapURL)
	}
	// ★ 用 -stdout 而**不是** -log,原因与 hub_allocator/internal/biz/local_fleet.go 同一条:
	// `-log` 开的 Windows 控制台在快速编辑模式下被点一下,WriteConsole 阻塞 → 游戏线程冻死
	// (cdb 抓栈实证:FWindowsConsoleOutputDevice::Serialize ← UIpNetDriver::TrackAndLogNewIP
	// ← TickDispatch ← FEngineLoop::Tick)。战斗 DS 冻死比大厅更糟:一局人全卡在里面且不结算。
	// stdout 已由本类重定向到 log_dir;-FullStdOutLogOutput 保证全量。Python 侧同改。
	args = append(args, "-server", "-stdout", "-FullStdOutLogOutput", fmt.Sprintf("-port=%d", port))
	// editor 形态额外关掉「缺 streaming level package 就踢人」:未 cook 的 editor DS 与 PIE
	// 客户端对 World Partition runtime cell 的命名天然对不上,不关掉就是秒级无限重连
	// (完整成因、引擎出处与作用域理由见 conf.EditorLauncherCVarArg 声明处)。
	// 必须排在 ExtraArgs 之前,运维仍可用 extra_args 覆盖回去。
	if l.cfg.Launcher == conf.LauncherEditor {
		args = append(args, conf.EditorLauncherCVarArg)
	}
	args = append(args, l.cfg.ExtraArgs...)
	return args
}

// buildEnv 在当前进程环境基础上注入 DS 身份变量(对齐 UE DS 侧 PandoraAgonesProvider 读取的 env)。
// token 是 Allocate 一次性签好的 DS 回调令牌(空=未启用/off 下签发失败,DS 无令牌运行)。
//
// **调用约定:调用方必须已持有 l.mu**(唯一链路 Allocate → startProc → defaultStart → buildEnv,
// Allocate 全程持锁)。本函数内部因此直接读写 pendingRosters,绝不可再取锁(不可重入 → 死锁)。
func (l *LocalGameServerAllocator) buildEnv(podName string, matchID uint64, mapID uint32, gameMode, token string) []string {
	env := os.Environ()
	env = append(env,
		"AGONES_GAMESERVER_NAME="+podName,
		"PANDORA_MATCH_ID="+strconv.FormatUint(matchID, 10),
		"PANDORA_MAP_ID="+strconv.FormatUint(uint64(mapID), 10),
		"PANDORA_GAME_MODE="+gameMode,
		// 仅 Windows 本机 allocator 注入；UE 还会校验 local pod 身份与非 Agones 运行态。
		auth.DSLocalProfileEnv+"="+auth.DSLocalProfileOffV1,
	)
	// DS 回调服务令牌(审核 P1 #1):local 模式经 env 下发(agones 模式走 annotation)。
	if token != "" {
		env = append(env, "PANDORA_DS_TOKEN="+token)
	}
	// 权威准入元数据(mode=local 版的 battle admission annotation 四件套)。
	// UE 侧要求 roster / allocation-id / release-track **同时**齐备才认(缺一即整份判非法),
	// 而 combat-factions 缺失会让 ResolveCampForSpawn 直接 RejectSpawn(玩家进图没角色),
	// 故这里四者同投同不投,绝不投半份。roster 编码见 canonicalRosterText,
	// 阵营编码复用与 annotation 路径同一个权威规范化器 dsmetadata.CanonicalCombatFactions。
	// ⚠️ 这里**不能**再取 l.mu:唯一生产调用链是 Allocate → startProc → defaultStart → buildEnv,
	// 而 Allocate 全程持有 l.mu(defer Unlock),Go 互斥锁不可重入,再取即自锁死
	// —— 表现为 DS 进程永远不被拉起、AllocateBattle 无限挂起、对局最终按空 pod 判弃
	// (2026-08-04 实测,pod="" 的 battle_abandoned_heartbeat_timeout 就是它)。
	// 故本函数按「调用方已持 l.mu」的前提直接读写 pendingRosters。
	roster, hasRoster := l.pendingRosters[matchID]
	if hasRoster {
		delete(l.pendingRosters, matchID) // 消费即删,台账不随对局数无界增长
	}
	if hasRoster {
		rosterText := canonicalRosterText(roster.playerIDs)
		// CanonicalCombatFactions 同时校验"精确覆盖 roster、升序去重、非零 player_id、
		// faction_id 可安全映射为 UE Camp",并按 roster 顺序输出 `pid=faction,...`。
		// 不自己再拼一遍:UE 的 ParseCanonicalCombatFactions 是逐项对齐 roster 的强校验,
		// 两边规则一旦漂移,投出去也会被整份判非法(把"没投"伪装成"格式错",更难查)。
		_, factionsText, factionErr := dsmetadata.CanonicalCombatFactions(
			roster.playerIDs, roster.combatFactionByPlayer)
		if rosterText != "" && roster.allocationID != "" && roster.releaseTrack != "" && factionErr == nil {
			env = append(env,
				"PANDORA_BATTLE_ROSTER="+rosterText,
				"PANDORA_ALLOCATION_ID="+roster.allocationID,
				"PANDORA_RELEASE_TRACK="+roster.releaseTrack,
				"PANDORA_BATTLE_COMBAT_FACTIONS="+factionsText,
			)
		} else {
			factionErrText := ""
			if factionErr != nil {
				factionErrText = factionErr.Error()
			}
			plog.With(context.Background()).Warnw("msg", "local_battle_roster_not_canonical",
				"match_id", matchID, "players", len(roster.playerIDs),
				"factions", len(roster.combatFactionByPlayer),
				"allocation_id", roster.allocationID, "release_track", roster.releaseTrack,
				"faction_err", factionErrText,
				"hint", "四件套不全或 roster/阵营非 canonical,不投递;UE 侧将保持空名单与空阵营,"+
					"主动退出会被判 AuthorityNotReady、玩家出生会被 RejectSpawn(进图无角色)")
		}
	}
	// extra_env 追加,但严禁覆盖内置身份/令牌变量(审核 P1:extra_env 覆盖 PANDORA_DS_TOKEN
	// 会用静态/伪造令牌替换真签发令牌,绕过范围绑定)。保留字命中即跳过并告警。
	for k, v := range l.cfg.ExtraEnv {
		if isReservedDSEnvKey(k) {
			plog.With(context.Background()).Warnw("msg", "extra_env_reserved_key_ignored", "key", k,
				"hint", "extra_env 不得覆盖 PANDORA_DS_TOKEN / PANDORA_MATCH_ID / AGONES_GAMESERVER_NAME 等内置变量")
			continue
		}
		env = append(env, k+"="+v)
	}
	return env
}

// isReservedDSEnvKey 判断 env key 是否为 allocator 内置注入的身份/令牌变量,extra_env 不得覆盖。
// 大小写不敏感:Windows(local 模式 DS 宿主)环境变量名大小写不敏感,`pandora_ds_token` 与
// `PANDORA_DS_TOKEN` 指向同一变量;若只按精确大写比对,小写别名仍能覆盖真令牌(审核 P1 补漏)。
func isReservedDSEnvKey(k string) bool {
	switch strings.ToUpper(strings.TrimSpace(k)) {
	case "PANDORA_DS_TOKEN", auth.DSLocalProfileEnv, "AGONES_GAMESERVER_NAME", "PANDORA_MATCH_ID",
		"PANDORA_MAP_ID", "PANDORA_GAME_MODE", "PANDORA_DS_TYPE", "PANDORA_REGION":
		return true
	default:
		return false
	}
}
