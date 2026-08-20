// Package conf 是 hub_allocator 服务的私有配置结构。
package conf

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/placement"
)

// Hub DS 分片发现/启动模式(标准两模式开关 + 离线兜底,与 ds_allocator.mode 对齐)。
//
//	ModeLocal  本机 exec 一个常驻 Windows Hub DS 进程(LocalHubConf),Windows 单机自测
//	ModeAgones k8s Agones Fleet 发现 Hub DS 分片(AgonesConf),Linux 线上
//	ModeMock   确定性假分片(无真实 Hub DS),离线联调兜底
const (
	ModeLocal  = "local"
	ModeAgones = "agones"
	ModeMock   = "mock"
)

// 本机 Hub DS 的两种进程形态(LocalHubConf.Launcher 的合法取值,与 ds_allocator 对齐)。
const (
	// LauncherPackaged 跑打包好的 PandoraServer.exe(默认,现状行为)。
	LauncherPackaged = "packaged"
	// LauncherEditor 跑 UnrealEditor.exe + .uproject -server,读未 cook 的工程内容,免出包。
	LauncherEditor = "editor"
)

// EditorLauncherCVarArg 是 launcher=editor 专用的引擎 CVar 覆盖参数(与 ds_allocator 逐字对齐)。
//
// 为什么必须加(2026-08-18 实测:PIE 客户端连未 cook 的 editor DS 后秒断秒重连):
// 未 cook 的 editor DS 把 World Partition runtime cell 注册成 <WorldPackage>/<Cell>,
// 而 PIE 客户端把同一个 cell 以 /Memory/UEDPIE_<n>_<World>_<Cell> 上报可见性。服务端
// 在 UNetConnection.cpp:1968 找不到该 package,默认行为是**直接关连接**
// (ENetCloseResult::MissingLevelPackage)。客户端拿不到这个原因(UE 5.8 在客户端侧把
// NMT_CloseReason 直接丢弃),只会当成瞬态掉线去重查权威 → 权威照旧把玩家送回同一个
// DS → 再被踢,形成秒级无限重连循环。大厅是常驻世界、WP cell 更多,更容易撞上。
//
// net.SkipMissingLevelDisconnect 是引擎自带的开关(NetConnection.cpp:119,Epic 就是为
// 同类问题加的):打开后服务端只打一条 Warning 忽略该 package,不再踢人。代价是那批 cell
// 里的 Actor 不会复制给该客户端,这对"免出包看资源改动"的策划回路显然优于被踢回登录。
//
// 刻意只作用于 launcher=editor:packaged 本机 DS 与 k8s Linux DS 跑的都是 cook 过的内容,
// 那里的 MissingLevelPackage 是**真的**内容不一致,必须保留引擎的踢人保护。
// 形态选 -DPCVars= 而非 -ExecCmds=:前者是 Key=Value 逗号列表、**不含空格**,经
// exec.Command 传给 Windows 时不会被引号转义打散;引擎在 PreInit 阶段
// (LaunchEngineLoop.cpp:2857 InitializeCVarsForActiveDeviceProfile)就应用,
// 远早于任何客户端连入,且该调用对 DS 同样生效(无 !UE_SERVER 守卫)。
// 放在 ExtraArgs **之前**:同名 tag 引擎按出现顺序逐条应用、后写的赢,运维仍可覆盖。
const EditorLauncherCVarArg = "-DPCVars=net.SkipMissingLevelDisconnect=1"

// Config 是 hub_allocator 服务的完整配置。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	// Mode 选择 Hub DS 分片来源,与 ds_allocator.mode 对齐的「标准两模式开关」:
	//   "local"  → 本机 exec 一个常驻 Windows Hub DS(LocalHub,Windows 单机自测)
	//   "agones" → k8s Agones Fleet 发现分片(Agones,Linux 线上)
	//   "mock"   → 确定性假分片(无真实 Hub DS,离线联调)
	// 留空时按 legacy 的 agones.enabled 推导(向后兼容旧配置)。
	Mode string `yaml:"mode,omitempty" json:"mode,omitempty"`

	Hub HubConf `yaml:"hub" json:"hub"`

	// JWT 用于给玩家签发 hub DSTicket(AssignHub / TransferHub 返回 hub_ticket)。
	// Issuer / Audience / Secret 必须与 login / Envoy jwt_authn provider 完全一致。
	JWT JWTConf `yaml:"jwt,omitempty" json:"jwt,omitempty"`

	// DSTicket 是玩家 DSTicket v2(RS256 非对称,方案 B)签发配置。private_key_file 非空
	// 即启用:hub 票据改由 auth.DSTicketSigner 签发并绑死到唯一 Hub DS 实例
	// (绑定不完整时 fail-closed 拒签,不再允许签无绑定票)。
	// 留空 = 沿用 legacy HS256 DSTicket(dev/local-off 行为不变)。
	DSTicket config.DSTicketConf `yaml:"ds_ticket,omitempty" json:"ds_ticket,omitempty"`

	// Agones 真 Hub DS Fleet 发现配置(W4 ⑬)。mode=agones 时生效。
	Agones AgonesConf `yaml:"agones" json:"agones"`

	// LocalHub 本机 exec 常驻 Windows Hub DS 配置(mode=local 时生效)。
	LocalHub LocalHubConf `yaml:"local_hub" json:"local_hub"`

	// DSAuth DS 回调服务令牌(审核 P1 #1:DS→后端回调认证)。本服务两个角色都用它:
	//   - 签发:ListShards 发现 ready Hub DS 时签 hub 令牌(绑 pod),经 GameServer
	//     annotation(agones,剩余寿命 < TTL/3 时续期)/ PANDORA_DS_TOKEN env(local)下发。
	//   - 校验:Heartbeat 按 mode(off/permissive/enforce)验证令牌 + pod 范围绑定。
	// 详见 pkg/config.DSAuthConf、docs/design/decision-revisit-ds-callback-auth.md。
	DSAuth config.DSAuthConf `yaml:"ds_auth,omitempty" json:"ds_auth,omitempty"`
}

// LocalHubConf 是「本机拉起一个常驻 Windows Hub Dedicated Server 进程」的调试后端配置(mode=local)。
//
// 与 ds_allocator.LocalDSConf 对称:这是 Windows 单机自测时大厅 DS 的来源。hub_allocator 在
// 首次 AssignHub 时懒拉起一个常驻 Hub DS 进程(加载 hub 关卡 / PandoraHubGameMode),把它作为
// 唯一分片返回给 login;进程随 hub_allocator 退出而 Kill。常驻不按对局回收(与战斗 DS 不同)。
type LocalHubConf struct {
	// Launcher 选择本机 Hub DS 的「进程形态」,与 ds_allocator.LocalDSConf.Launcher 对称;
	// 两种模式并存、互不影响,只改这一个字段即可切换:
	//   - LauncherPackaged("packaged",默认)= 现状:跑打包好的 PandoraServer.exe。
	//   - LauncherEditor("editor")= 跑 UnrealEditor.exe + .uproject,仍带 -server
	//     (NetMode 依旧是 NM_DedicatedServer,登录/大厅/匹配全链路一字不改),
	//     但直接读工程里未 cook 的 Content/,策划存盘后重进大厅即生效,免出包。
	Launcher string `yaml:"launcher,omitempty" json:"launcher,omitempty"`

	// ExecutablePath 本机要 exec 的可执行文件绝对路径,含义随 Launcher 变化:
	//   - packaged:打包好的 UE Windows Dedicated Server
	//     (与战斗 DS 同一个 PandoraServer.exe,靠 map_name 区分大厅/战斗关卡)。
	//   - editor:  引擎的 UnrealEditor.exe。
	// mode=local 时必填且必须存在。
	ExecutablePath string `yaml:"executable_path,omitempty" json:"executable_path,omitempty"`

	// ProjectPath 工程 .uproject 绝对路径,仅 Launcher=editor 时必填且必须存在(packaged 忽略)。
	// UE 的 LaunchSetGameName 只认命令行里第一个不以 '-' 开头的 token,所以它必须排在关卡 URL 之前;
	// buildArgs 已保证该顺序。
	ProjectPath string `yaml:"project_path,omitempty" json:"project_path,omitempty"`

	// MapName 启动时加载的大厅关卡(DS 命令行首个位置参数,例如 /Game/Maps/HubMap)。
	// 留空则不带关卡参数,由 DS 自身默认关卡决定。
	MapName string `yaml:"map_name,omitempty" json:"map_name,omitempty"`

	// AdvertiseHost 返回给客户端的可连接 host(默认 127.0.0.1,本机联调)。
	AdvertiseHost string `yaml:"advertise_host,omitempty" json:"advertise_host,omitempty"`

	// Port 常驻 Hub DS 监听端口(默认 7777)。
	Port int `yaml:"port,omitempty" json:"port,omitempty"`

	// Region 该本机 Hub 分片归属的 region(默认取 hub.default_region)。
	Region string `yaml:"region,omitempty" json:"region,omitempty"`

	// Capacity 该本机 Hub 分片人数上限(默认取 hub.default_capacity)。
	Capacity int32 `yaml:"capacity,omitempty" json:"capacity,omitempty"`

	// WorkingDir DS 进程工作目录(留空用 hub_allocator 当前目录)。
	WorkingDir string `yaml:"working_dir,omitempty" json:"working_dir,omitempty"`

	// LogDir DS 进程 stdout/stderr 落盘目录(默认 run/dev/logs/ds);文件名 <pod>.log。
	LogDir string `yaml:"log_dir,omitempty" json:"log_dir,omitempty"`

	// ExtraArgs 追加到 DS 命令行末尾的额外参数。
	ExtraArgs []string `yaml:"extra_args,omitempty" json:"extra_args,omitempty"`

	// ExtraEnv 注入 DS 进程的额外环境变量(在内置 PANDORA_* 变量之后追加)。
	ExtraEnv map[string]string `yaml:"extra_env,omitempty" json:"extra_env,omitempty"`
}

// AgonesConf 是真 Agones Hub DS Fleet 发现配置(W4 ⑬,镜像 ds_allocator.AgonesConf)。
//
// Enabled=false(默认)→ 用 MockHubFleetProvider;Enabled=true → 用
// AgonesHubFleetProvider(经 k8s apiserver REST 查 agones.dev/v1 GameServer 列表,
// 按 agones.dev/fleet=<FleetName> + pandora.dev/region=<region> 标签过滤)。
//
// 集群内运行时 token_path / ca_path / api_server / namespace 留空即用 in-cluster 默认;
// 集群外联调(本机进程 → minikube)可显式指定 api_server + token_path(或 kubectl proxy 不带 token)。
type AgonesConf struct {
	// Enabled 打开真 Agones 分片发现(默认 false → Mock)。
	Enabled bool `yaml:"enabled,omitempty" json:"enabled,omitempty"`

	// APIServer k8s apiserver 地址(默认 https://kubernetes.default.svc,in-cluster)。
	APIServer string `yaml:"api_server,omitempty" json:"api_server,omitempty"`

	// Namespace GameServer 所在命名空间(默认 default)。
	Namespace string `yaml:"namespace,omitempty" json:"namespace,omitempty"`

	// FleetName 选择 Hub DS GameServer 的 Fleet 名(selector agones.dev/fleet=<FleetName>)。
	// Enabled=true 时必填,否则构造失败。
	FleetName string `yaml:"fleet_name,omitempty" json:"fleet_name,omitempty"`

	// CanaryFleetName 是 canary Hub Fleet。CanaryPercent>0 时必填；stable 玩家永不
	// 进入此 Fleet，canary 玩家无容量时可以回退 stable 并持久化实际命中轨。
	CanaryFleetName string `yaml:"canary_fleet_name,omitempty" json:"canary_fleet_name,omitempty"`

	// CanaryPercent/CanarySeed 以 player_id 做确定性 cohort；已有 assignment 的
	// release_track 是粘性权威，不随百分比调整漂移。
	CanaryPercent uint32 `yaml:"canary_percent,omitempty" json:"canary_percent,omitempty"`
	CanarySeed    string `yaml:"canary_seed,omitempty" json:"canary_seed,omitempty"`

	// AdvertiseHost 覆盖返回给客户端连接的 host;留空则使用 Agones status.address。
	// 本机 minikube docker-driver 联调时常设为 127.0.0.1,配合 UDP relay。
	AdvertiseHost string `yaml:"advertise_host,omitempty" json:"advertise_host,omitempty"`

	// TokenPath ServiceAccount bearer token 文件路径
	// (默认 /var/run/secrets/kubernetes.io/serviceaccount/token;留 "-" 显式禁用 token)。
	TokenPath string `yaml:"token_path,omitempty" json:"token_path,omitempty"`

	// CAPath apiserver CA 证书路径
	// (默认 /var/run/secrets/kubernetes.io/serviceaccount/ca.crt)。
	CAPath string `yaml:"ca_path,omitempty" json:"ca_path,omitempty"`

	// InsecureSkipTLSVerify 跳过 apiserver TLS 校验(仅 dev,生产禁用)。
	InsecureSkipTLSVerify bool `yaml:"insecure_skip_tls_verify,omitempty" json:"insecure_skip_tls_verify,omitempty"`

	// ListTimeout 单次 LIST GameServer REST 调用超时(默认 5s)。
	ListTimeout config.Duration `yaml:"list_timeout,omitempty" json:"list_timeout,omitempty"`
}

// JWTConf 是签发 hub DSTicket 的 JWT 参数(镜像 login.JWTConf / matchmaker.JWTConf)。
type JWTConf struct {
	Issuer   string `yaml:"issuer,omitempty" json:"issuer,omitempty"`
	Audience string `yaml:"audience,omitempty" json:"audience,omitempty"`
	Secret   string `yaml:"secret,omitempty" json:"secret,omitempty"`
	// AdditionalSecrets 是**仅用于校验**的额外可接受密钥(不用于签发),支持玩家面
	// JWT 不停服密钥轮换(三段式,同 ds_auth.additional_secrets)。默认空。
	AdditionalSecrets []string        `yaml:"additional_secrets,omitempty" json:"additional_secrets,omitempty"`
	SessionTTL        config.Duration `yaml:"session_ttl,omitempty" json:"session_ttl,omitempty"`
	DSTicketTTL       config.Duration `yaml:"ds_ticket_ttl,omitempty" json:"ds_ticket_ttl,omitempty"`
}

// HubConf 是 hub_allocator 服务私有配置。
type HubConf struct {
	// HeartbeatTimeout Hub DS 心跳超时阈值(默认 30s,不变量 §4)。
	// 超过此时长没收到 Heartbeat → 分片标记 draining 并移出可分配集。
	HeartbeatTimeout config.Duration `yaml:"heartbeat_timeout,omitempty" json:"heartbeat_timeout,omitempty"`

	// SweepInterval 心跳超时扫描间隔(默认 5s)。
	SweepInterval config.Duration `yaml:"sweep_interval,omitempty" json:"sweep_interval,omitempty"`

	// ShardTTL 分片镜像 Redis key TTL(默认 30min,每次 Assign/Heartbeat 刷新)。
	ShardTTL config.Duration `yaml:"shard_ttl,omitempty" json:"shard_ttl,omitempty"`

	// AssignmentTTL 玩家→分片归属 Redis key TTL(默认 30min,每次 Assign/Transfer 刷新)。
	AssignmentTTL config.Duration `yaml:"assignment_ttl,omitempty" json:"assignment_ttl,omitempty"`

	// ReservationTTL 从 allocator 签票前占位到 Hub DS Admission ACK 的绝对 lease。
	// Model B 启动时机械校验 >= DSTicket 有效窗(+DS 侧 15s leeway)且 <= AssignmentTTL。
	ReservationTTL config.Duration `yaml:"reservation_ttl,omitempty" json:"reservation_ttl,omitempty"`

	// DefaultRegion AssignHub 未指定 region 时的兜底分区(默认 "global")。
	DefaultRegion string `yaml:"default_region,omitempty" json:"default_region,omitempty"`

	// DefaultCapacity 单分片人数上限(默认 500,大厅 500 人/实例)。
	DefaultCapacity int32 `yaml:"default_capacity,omitempty" json:"default_capacity,omitempty"`

	// OptimisticRetry WATCH/MULTI/EXEC 乐观锁冲突最大重试次数,耗尽返 ErrHubNoAvailable。
	OptimisticRetry int `yaml:"optimistic_retry,omitempty" json:"optimistic_retry,omitempty"`

	// MockShardCount W4 ⑤ MockHubFleetProvider 每 region 种的假分片数(默认 3)。
	// 真 Agones Fleet 接入后此字段废弃,分片拓扑由 Fleet 查询返回。
	MockShardCount int `yaml:"mock_shard_count,omitempty" json:"mock_shard_count,omitempty"`

	// MockHubAddrHost W4 ⑤ Mock 分片返回的假 Hub DS host(默认 127.0.0.1)。
	MockHubAddrHost string `yaml:"mock_hub_addr_host,omitempty" json:"mock_hub_addr_host,omitempty"`

	// MockHubPortBase W4 ⑤ Mock 分片端口基址(默认 7777;分片 port = base + shard_id)。
	MockHubPortBase int `yaml:"mock_hub_port_base,omitempty" json:"mock_hub_port_base,omitempty"`

	// AutoScaleEnabled 是否开启 Hub Fleet 自动扩缩容(默认 false)。
	// 开启条件:建议配合 agones.enabled=true(真 Fleet Provider),否则仅记录日志不生效。
	AutoScaleEnabled bool `yaml:"autoscale_enabled,omitempty" json:"autoscale_enabled,omitempty"`

	// PlayersPerHub 自动扩容阈值:单 Hub 目标承载人数(默认 500)。
	// 例:总在线 501 → 期望副本 ceil(501/500)=2。
	PlayersPerHub int32 `yaml:"players_per_hub,omitempty" json:"players_per_hub,omitempty"`

	// MinReplicas 开服默认保底大厅副本数(默认 1)。
	MinReplicas int32 `yaml:"min_replicas,omitempty" json:"min_replicas,omitempty"`

	// MaxReplicas 大厅副本上限(默认 20)。
	MaxReplicas int32 `yaml:"max_replicas,omitempty" json:"max_replicas,omitempty"`

	// ConsolidationEnabled 是否开启强制整合(低负载时把人换到该去的分片,排空分片后缩容,默认 false)。
	// 依赖 autoscale_enabled=true + kafka.brokers 非空(推迁迁移通知);任一缺失只记日志不生效。
	ConsolidationEnabled bool `yaml:"consolidation_enabled,omitempty" json:"consolidation_enabled,omitempty"`

	// OwnerAddr owner 权威服务地址(owner-authority.md migrate ⑥)。
	// 空 = 不双写实例租约(未启用,现网行为不变,安全默认)。
	OwnerAddr string `yaml:"owner_addr,omitempty" json:"owner_addr,omitempty"`

	// OwnerLeaseRequired 实例租约双写失败是否令授权心跳失败。
	// 默认 false = migrate 弱依赖(失败只告警,旧 last_heartbeat_ms 再入门双门并行兜底);
	// contract 阶段全链验证后置 true 转强依赖(续租失败心跳必须失败 → DS 自我 fencing)。
	OwnerLeaseRequired bool `yaml:"owner_lease_required,omitempty" json:"owner_lease_required,omitempty"`

	// MigrateGraceSeconds 迁移优雅倒计时(秒,默认 30)。
	// 下发给客户端/Hub DS 的提示倒计时;也是排空分片可被缩容回收的最短等待(避免提前杀 pod)。
	MigrateGraceSeconds int32 `yaml:"migrate_grace_seconds,omitempty" json:"migrate_grace_seconds,omitempty"`

	// ConsolidationBatch 单次 reconcile 每个排空分片最多迁移的玩家数(默认 50,防撑死)。
	// 超过部分留给下一个 sweep 周期继续排。
	ConsolidationBatch int `yaml:"consolidation_batch,omitempty" json:"consolidation_batch,omitempty"`

	// TransferCooldown 玩家主动切线冷却(默认 10s,防刷)。
	// 冷却窗口内再次 TransferToLine 返回 ErrHubTransferCooldown;<=0 视为不限流。
	TransferCooldown config.Duration `yaml:"transfer_cooldown,omitempty" json:"transfer_cooldown,omitempty"`

	// LocatorAddr player_locator gRPC 地址(玩家切线护栏:战斗/匹配中禁切)。
	// 弱依赖:留空则跳过位置检查(locator 抖动不硬阻断低危的大厅切线)。
	LocatorAddr string `yaml:"locator_addr,omitempty" json:"locator_addr,omitempty"`

	// WriterLeaseMode 写者继任租约的接线档位(session-generation-rollout.md §5.4
	// 首次引导升级,R10 复审 P0-5)。留空 = enforce(保持现网行为,§14 默认不变)。
	//
	//	enforce:竞选 + 接流前 fence 推扫硬门 + 注入 biz/repo(当前稳态形态);
	//	warmup :**只竞选、只观测**——不注入任何 fence,不写 fence 键,不改写路径。
	//	        用于"从不含 writerlease 的旧镜像首次升级"的第一跳:新旧副本并存时新副本
	//	        行为与旧副本完全一致(都按旧路径写),同时可在日志里确认继任链健康
	//	        (elected/token 单调、失主重选)。确认后再滚动到 enforce 的第二跳。
	//	off    :完全不启动租约(仅限单副本 Recreate 的历史部署;RollingUpdate 下禁用)。
	//
	// 取值非法时启动 fail-fast(安全开关不允许静默变形,见 ValidateWriterLeaseMode)。
	WriterLeaseMode string `yaml:"writer_lease_mode,omitempty" json:"writer_lease_mode,omitempty"`
}

// 写者继任租约档位取值(WriterLeaseMode)。
const (
	WriterLeaseEnforce = "enforce"
	WriterLeaseWarmup  = "warmup"
	WriterLeaseOff     = "off"
)

// ResolveWriterLeaseMode 归一化并校验 writer_lease_mode(空 → enforce)。
// 非法值返回 error,由 main fail-fast:安全档位配错必须炸,不能静默退化。
func (c HubConf) ResolveWriterLeaseMode() (string, error) {
	switch c.WriterLeaseMode {
	case "", WriterLeaseEnforce:
		return WriterLeaseEnforce, nil
	case WriterLeaseWarmup:
		return WriterLeaseWarmup, nil
	case WriterLeaseOff:
		return WriterLeaseOff, nil
	default:
		return "", fmt.Errorf("hub.writer_lease_mode %q invalid (want enforce|warmup|off)", c.WriterLeaseMode)
	}
}

// Defaults 填默认值。
func (c *Config) Defaults() {
	// Mode 归一化:显式 mode 优先;留空时按 legacy 的 agones.enabled 推导(向后兼容)。
	c.Mode = strings.ToLower(strings.TrimSpace(c.Mode))
	if c.Mode == "" {
		if c.Agones.Enabled {
			c.Mode = ModeAgones
		} else {
			c.Mode = ModeMock
		}
	}
	if c.Hub.HeartbeatTimeout == 0 {
		c.Hub.HeartbeatTimeout = config.Duration(30 * time.Second)
	}
	// 脑裂再入屏障机械下限(pkg/placement 契约,2026-07-16):AssignHub 只有在分片心跳
	// 超过 HeartbeatTimeout 后才会把玩家改派到新分片;该窗口必须 ≥ DS 授权租约上限 +
	// 偏差余量(27s),保证分区的旧 Hub 已对存量玩家完成自我 fencing。这是正确性下限
	// 而非调优参数:配置调低会重新打开「一人两 Hub」窗口,机械抬到下限。
	// 比较和回填都复用 placement 包的单一常量，避免 Hub 与 Battle 再入屏障发生配置漂移。
	if c.Hub.HeartbeatTimeout.Std() < placement.DSFenceReentryBarrier {
		c.Hub.HeartbeatTimeout = config.Duration(placement.DSFenceReentryBarrier)
	}
	if c.Hub.SweepInterval == 0 {
		c.Hub.SweepInterval = config.Duration(5 * time.Second)
	}
	if c.Hub.ShardTTL == 0 {
		c.Hub.ShardTTL = config.Duration(30 * time.Minute)
	}
	if c.Hub.AssignmentTTL == 0 {
		c.Hub.AssignmentTTL = config.Duration(30 * time.Minute)
	}
	if c.Hub.ReservationTTL == 0 {
		c.Hub.ReservationTTL = config.Duration(auth.DSTicketMaxTTL + 15*time.Second)
	}
	if c.Hub.DefaultRegion == "" {
		c.Hub.DefaultRegion = "global"
	}
	if c.Hub.DefaultCapacity == 0 {
		c.Hub.DefaultCapacity = 500
	}
	if c.Hub.OptimisticRetry == 0 {
		c.Hub.OptimisticRetry = 3
	}
	if c.Hub.MockShardCount == 0 {
		c.Hub.MockShardCount = 3
	}
	if c.Hub.MockHubAddrHost == "" {
		c.Hub.MockHubAddrHost = "127.0.0.1"
	}
	if c.Hub.MockHubPortBase == 0 {
		c.Hub.MockHubPortBase = 7777
	}
	if c.Hub.PlayersPerHub == 0 {
		c.Hub.PlayersPerHub = 500
	}
	if c.Hub.MinReplicas == 0 {
		c.Hub.MinReplicas = 1
	}
	if c.Hub.MaxReplicas == 0 {
		c.Hub.MaxReplicas = 20
	}
	if c.Hub.MaxReplicas < c.Hub.MinReplicas {
		c.Hub.MaxReplicas = c.Hub.MinReplicas
	}
	if c.Hub.MigrateGraceSeconds == 0 {
		c.Hub.MigrateGraceSeconds = 30
	}
	if c.Hub.ConsolidationBatch == 0 {
		c.Hub.ConsolidationBatch = 50
	}
	if c.Hub.TransferCooldown == 0 {
		c.Hub.TransferCooldown = config.Duration(10 * time.Second)
	}
	if c.Agones.APIServer == "" {
		c.Agones.APIServer = "https://kubernetes.default.svc"
	}
	if c.Agones.Namespace == "" {
		c.Agones.Namespace = "default"
	}
	if c.Agones.TokenPath == "" {
		c.Agones.TokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	}
	if c.Agones.CAPath == "" {
		c.Agones.CAPath = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	}
	if c.Agones.ListTimeout == 0 {
		c.Agones.ListTimeout = config.Duration(5 * time.Second)
	}
	c.DSAuth.Defaults()
	// LocalHub 默认值(mode=local 时生效)。
	// 路径字段支持环境变量展开 + 跨机器兜底,便于策划机移植(Client 目录可能不在配置写死的盘符):
	//  1. 先做 ${VAR}/$VAR 展开(绝对路径不含 $,dev 配置原样保留);
	//  2. filepath.FromSlash 归一化分隔符:策划在 yaml 里写正斜杠 / (无需 \\ 转义)也能在 Windows 正常工作;
	//  3. 展开后的路径在本机不存在时,回退到启动脚本按平级 Client 目录探测注入的
	//     PANDORA_DS_EXE / PANDORA_DS_DIR(play.ps1 自动填充);dev 上 F:\ 路径存在则不覆盖。
	c.LocalHub.ExecutablePath = filepath.FromSlash(os.ExpandEnv(c.LocalHub.ExecutablePath))
	c.LocalHub.WorkingDir = filepath.FromSlash(os.ExpandEnv(c.LocalHub.WorkingDir))
	if envExe := os.Getenv("PANDORA_DS_EXE"); envExe != "" {
		if _, err := os.Stat(c.LocalHub.ExecutablePath); c.LocalHub.ExecutablePath == "" || err != nil {
			c.LocalHub.ExecutablePath = filepath.FromSlash(envExe)
			if envDir := os.Getenv("PANDORA_DS_DIR"); envDir != "" {
				c.LocalHub.WorkingDir = filepath.FromSlash(envDir)
			}
		}
	}
	// Launcher 两模式开关:缺省/非法值一律归一到 packaged(现状行为),旧配置零改动。
	// PANDORA_DS_LAUNCHER / PANDORA_DS_UPROJECT 让一键脚本免改 yaml 就能切换,
	// 与 ds_allocator 同名变量同语义(一次注入同时切大厅和战斗 DS)。
	if envLauncher := strings.TrimSpace(os.Getenv("PANDORA_DS_LAUNCHER")); envLauncher != "" {
		c.LocalHub.Launcher = envLauncher
	}
	if c.LocalHub.Launcher = strings.ToLower(strings.TrimSpace(c.LocalHub.Launcher)); c.LocalHub.Launcher != LauncherEditor {
		c.LocalHub.Launcher = LauncherPackaged
	}
	c.LocalHub.ProjectPath = filepath.FromSlash(os.ExpandEnv(c.LocalHub.ProjectPath))
	if envProj := strings.TrimSpace(os.Getenv("PANDORA_DS_UPROJECT")); envProj != "" {
		if _, err := os.Stat(c.LocalHub.ProjectPath); c.LocalHub.ProjectPath == "" || err != nil {
			c.LocalHub.ProjectPath = filepath.FromSlash(envProj)
		}
	}
	// AdvertiseHost 是「返回给客户端连接的 Hub DS host」,属每台机器各异的运行期值:内网测试服要用
	// 局域网 IP(远程策划客户端才连得到大厅 DS),本机自测用 127.0.0.1。启动脚本(play.ps1 -Battle
	// -Intranet)自动探测本机内网 IPv4 并经 PANDORA_DS_ADVERTISE_HOST 注入,优先级高于 yaml 写死值,
	// 无需改仓库配置。留空时回退 yaml 值 / 127.0.0.1。
	if envHost := strings.TrimSpace(os.Getenv("PANDORA_DS_ADVERTISE_HOST")); envHost != "" {
		c.LocalHub.AdvertiseHost = envHost
	}
	if c.LocalHub.AdvertiseHost == "" {
		c.LocalHub.AdvertiseHost = "127.0.0.1"
	}
	if c.LocalHub.Port == 0 {
		c.LocalHub.Port = 7777
	}
	if c.LocalHub.Region == "" {
		c.LocalHub.Region = c.Hub.DefaultRegion
	}
	if c.LocalHub.Capacity == 0 {
		c.LocalHub.Capacity = c.Hub.DefaultCapacity
	}
	if c.LocalHub.LogDir == "" {
		c.LocalHub.LogDir = "run/dev/logs/ds"
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":20021"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":21021"
	}
}
