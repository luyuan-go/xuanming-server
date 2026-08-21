// Package conf 是 login 服务的私有配置结构。
//
// 内嵌 pkg/config.Base 拿公共字段,再加 login 自有字段。
//
// 加载方式(见 cmd/login/main.go):
//
//	c := kconfig.New(kconfig.WithSource(file.NewSource("./etc/login-dev.yaml")))
//	c.Load()
//	var cfg conf.Config
//	c.Scan(&cfg)
package conf

import (
	"fmt"
	"slices"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
)

// Config 是 login 服务的完整配置。
type Config struct {
	// Base 公共字段(Server/Node/Snowflake/Locker/Registry/Timeouts/Kafka)。
	config.Base `yaml:",inline" mapstructure:",squash"`

	// Login 业务字段。
	Login LoginConf `yaml:"login" json:"login"`

	// DSAuth 是 UE DS 经 :8444 调 VerifyDSTicket 时的服务身份与 Redis active 权威配置。
	// 默认 off/legacy，保持既有内部 Verify 行为；仅 redis+enforce 启用在线入场门。
	DSAuth config.DSAuthConf `yaml:"ds_auth,omitempty" json:"ds_auth,omitempty"`
}

// LoginConf 是 login 服务私有配置。
type LoginConf struct {
	// SessionTokenTTL session_token 的有效期(写到 Redis,也用作 JWT exp)。
	SessionTokenTTL config.Duration `yaml:"session_token_ttl,omitempty" json:"session_token_ttl,omitempty"`

	// DSTicketTTL DS 票据有效期(JWT exp - issued_at)。
	// 不变量 §3:DS 票据短时效。默认 5 分钟。
	DSTicketTTL config.Duration `yaml:"ds_ticket_ttl,omitempty" json:"ds_ticket_ttl,omitempty"`

	// OwnerAddr owner 权威服务地址(owner-authority.md migrate ⑤)。
	// 空 = 不接 owner(未启用,现网行为不变,安全默认);非空时 Logout 成功后弱调用
	// Query+Release(compare-delete 自己,失败仅告警,不影响登出)。
	OwnerAddr string `yaml:"owner_addr,omitempty" json:"owner_addr,omitempty"`

	// OwnerQueryFirst 已于 2026-07-29 删除:§9.23 query-first 成为唯一进场路由路径,
	// locator-first 旧基线一并退役(它按 presence 推路由,是 §9.22 禁止的 fail-open,
	// 且永远填不出五态)。保留一个只会置 true 的开关违反 §15.3,故直接删。
	// reserved: owner_query_first

	// RequireTiDB 是账号库后端强校验门(全服单点扩容,2026-07-27;与 owner.require_tidb 同语义、
	// 按服务独立激活)。dev 默认 false = 连单机 MySQL 联调不校验;-Prod 产物由
	// gen_cluster_config.ps1 机械翻转 true,启动期强制两条,任一不符 fail-fast 拒启:
	//   ① VERSION() 含 -TiDB-:生成器只能校验 DSN 字符串,证不了对端真是 TiDB(双层防线);
	//   ② accounts.account 的排序规则**实际行为**仍是大小写不敏感 + NO PAD。
	// ②是 TiDB 特有的静默陷阱:_ci 排序规则只在集群以 new_collations_enabled_on_first_bootstrap
	// =true 初始化时才真正大小写不敏感,否则只在语法上被接受、语义上按 binary 比较且不报错;
	// 该参数首次 bootstrap 后不可更改。而 Go 侧对账号串零归一化,唯一性完全由列 collation 决定,
	// 漂移即「老玩家登不进 + 同名抢注」。故必须行为探针,不能只看版本号(§16 隐蔽 bug)。
	RequireTiDB bool `yaml:"require_tidb,omitempty" json:"require_tidb,omitempty"`

	// RetentionModeRaw 保留期清理模式:留空 / "report_only" = 默认只报告不删;"delete" = 真删。
	RetentionModeRaw string `yaml:"retention_mode,omitempty" json:"retention_mode,omitempty"`

	// DeviceRetentionDays account_devices 设备绑定行保留天数(默认 90,§9.24)。
	// device_id 由客户端上报,单账号可无限堆新行;按 last_login_at 超期批删兜底有界,
	// 被删设备下次登录 TouchDevice upsert 自然重建。account_bans 不清理(运营合规审计,
	// 量级 = 运营操作数,§9.24 登记豁免)。
	DeviceRetentionDays int `yaml:"device_retention_days,omitempty" json:"device_retention_days,omitempty"`

	// PlayerNoStart 角色编号起始号(player-no-and-login-surge.md 拍板项 A③,默认 1)。
	// 只在 player_no_counter 首次初始化(INSERT IGNORE)时生效;计数器行已存在后改本值
	// 无效——编号严格连续,不允许事后改起点。补号任务本身无开关:批大小/水位滞后是
	// data 层常量(PlayerNoBatchSize / PlayerNoWatermarkLag),迁移未跑时启动探针
	// fail-soft 停用补号(ERROR 日志),不影响登录主链。
	PlayerNoStart uint64 `yaml:"player_no_start,omitempty" json:"player_no_start,omitempty"`

	// PlayerNoResolveAuthSecret/Audience 校验 team→login 内部 ResolvePlayerNos 的
	// request-bound HMAC。该接口只接受 player_id 批量查询，且不经客户端 Envoy 暴露。
	PlayerNoResolveAuthSecret         string `yaml:"player_no_resolve_auth_secret,omitempty" json:"player_no_resolve_auth_secret,omitempty"`
	PlayerNoResolveAuthAudience       string `yaml:"player_no_resolve_auth_audience,omitempty" json:"player_no_resolve_auth_audience,omitempty"`
	FriendPlayerNoResolveAuthSecret   string `yaml:"friend_player_no_resolve_auth_secret,omitempty" json:"friend_player_no_resolve_auth_secret,omitempty"`
	FriendPlayerNoResolveAuthAudience string `yaml:"friend_player_no_resolve_auth_audience,omitempty" json:"friend_player_no_resolve_auth_audience,omitempty"`
	GuildPlayerNoResolveAuthSecret    string `yaml:"guild_player_no_resolve_auth_secret,omitempty" json:"guild_player_no_resolve_auth_secret,omitempty"`
	GuildPlayerNoResolveAuthAudience  string `yaml:"guild_player_no_resolve_auth_audience,omitempty" json:"guild_player_no_resolve_auth_audience,omitempty"`

	// SessionGenerationEnforce 是 SetRole 会话代际强制门(R7 收口,滚动发布分阶段激活)。
	// false(默认):Login 照常把单调代际写进 MySQL(emit/双写),SetRole 只做 Redis
	// precommit 复核;true:SetRole 同事务 FOR UPDATE 复核 MySQL 代际,确定性挡旧会话。
	// 激活前提(顺序硬约束,提前开会误拒合法会话):
	//   ① pandora_account 已跑 000003 迁移(player_session_generations 含 generation 列);
	//   ② 全 fleet Login 已升级到会写代际的版本,旧版本 Pod 已排空;
	//   ③ 观察 session_generation_persist_failed 为零后再置 true 滚动重启。
	SessionGenerationEnforce bool `yaml:"session_generation_enforce,omitempty" json:"session_generation_enforce,omitempty"`

	// RequireTicketSJTI 是 VerifyDSTicket 兑换点空 sjti 强制门(R8 收口,P0-5 滚动兼容;
	// 与 hub_allocator 的 session_gate.require_ticket_sjti 同语义、按服务独立激活)。
	// false(默认):不带 sjti 的票据告警放行(混版兼容窗:旧 matchmaker/旧 hub_allocator
	// 仍持续签空 sjti 票);非空 sjti 始终强制复核会话现行性,不受本门影响。
	// true:空 sjti 硬拒。激活前提(顺序硬约束,提前开会硬拒旧签发面的存量合法票):
	//   ① 全 fleet matchmaker / hub_allocator 已升级为签票必带 sjti,旧版本 Pod 已排空;
	//   ② 再等满一个票据最大 TTL(v2 RS256 上限 180s;legacy HS256 为 ds_ticket_ttl,默认 5min);
	//   ③ 观察 ticket_missing_session_binding_compat_allow 日志为零后再置 true 滚动重启。
	// 详见 docs/design/session-generation-rollout.md。
	RequireTicketSJTI bool `yaml:"require_ticket_sjti,omitempty" json:"require_ticket_sjti,omitempty"`

	// RequireHubAssignmentBinding 是 Hub DSTicket 归属绑定的机械激活栅栏。
	// false(默认):滚动兼容旧的无绑定 hub 票，但带绑定票仍会严格查 Redis 当前归属。
	// true:拒绝所有无绑定 hub 票，并禁止 login 在 hub_allocator 缺失/失败时自签回退。
	// 开启前必须同时配置 Redis 与 hub_allocator，否则启动失败。
	RequireHubAssignmentBinding bool `yaml:"require_hub_assignment_binding,omitempty" json:"require_hub_assignment_binding,omitempty"`

	// HubAssignmentFence 与 DS Redis authority 共用全局 required writer/capability lease。
	// binding=true 时必填，确保旧 login writer 激活后不能回滚接流量。
	HubAssignmentFence config.DSAuthFenceConf `yaml:"hub_assignment_fence,omitempty" json:"hub_assignment_fence,omitempty"`

	// MockHubDSAddr 是 hub_allocator 不可用时的本地回退 hub DS 地址。
	MockHubDSAddr string `yaml:"mock_hub_ds_addr,omitempty" json:"mock_hub_ds_addr,omitempty"`

	// DevSkipPassword 开发期免密登录开关(默认 false)。
	//
	// 为 true 时(仅供本机 / 联调,⚠️ 严禁上生产):
	//   - 跳过 bcrypt 密码校验,任意 password_hash 都放行
	//   - 账号不存在时自动懒注册一条 accounts 记录(snowflake 分配 player_id)
	//     → 同一 account 名每次登录拿到稳定 player_id(持久化在 MySQL,靠 uk_account 唯一)
	// 这样客户端随便填一个账号名即可进入,无需独立注册流程。
	DevSkipPassword bool `yaml:"dev_skip_password,omitempty" json:"dev_skip_password,omitempty"`

	// DevAutoRegister 开发期“假注册”开关(默认 false)。
	//
	// 为 true 时(仅供本机 / 联调,⚠️ 严禁上生产):账号不存在时首次登录
	// 自动注册一条 accounts 记录(snowflake 分配 player_id,存入本次客户端所发密码的 bcrypt 哈希)。
	//
	// 与 DevSkipPassword 正交:
	//   - 仅 DevAutoRegister:首登即注册,后续用同密码走正常 bcrypt 校验(真实“首登即注”语义)
	//   - 仅 DevSkipPassword:跳过密码校验(未知账号也会被懒注册,保持原行为)
	//   - 两者都开:任意账号名 + 任意密码都能进(最宽松 dev 模式)
	DevAutoRegister bool `yaml:"dev_auto_register,omitempty" json:"dev_auto_register,omitempty"`

	// ── 登录失败 Quota(账号 + IP,anti-abuse §4.2/§6 第 4 项)────────────────────
	// 只对**凭据失败**计数(密码错 / 自动注册关闭下的账号不存在):成功登录不计数、
	// 封禁与 DB 故障不计数,正常重连玩家零感知。达限后布锁,锁窗内 Login 直接拒
	// (ErrRateLimited + 可见剩余秒数),锁 PX 自过期无需人工解。
	// IP 维度依赖 Envoy 注入的受信 x-pandora-client-ip 头(入站同名头被剥离防伪造);
	// 未经 Envoy 的直连(本机 dev)拿不到 IP → 自动退化为仅账号维度(fail-open 方向)。
	// 背压非权威门:Redis 故障一律放行只 Warn(§2 铁律)。

	// LoginFailLimit 失败窗口内允许的凭据失败次数(默认 5;**负值**关闭整个失败配额,0=用默认)。
	LoginFailLimit int `yaml:"login_fail_limit,omitempty" json:"login_fail_limit,omitempty"`
	// LoginFailWindow 失败计数窗口(默认 15min)。
	LoginFailWindow config.Duration `yaml:"login_fail_window,omitempty" json:"login_fail_window,omitempty"`
	// LoginFailLock 达限后的锁定时长(默认 5min)。
	LoginFailLock config.Duration `yaml:"login_fail_lock,omitempty" json:"login_fail_lock,omitempty"`

	// JWT 设置(W3 ①,2026-06-05)。
	// dev/prod 都走 HS256,secret 要跟 deploy/envoy/envoy.yaml 的 jwt_authn provider 保持一致。
	JWT JWTConf `yaml:"jwt,omitempty" json:"jwt,omitempty"`

	// DSTicket 是玩家 DSTicket v2(RS256 非对称,方案 B)签发配置。private_key_file 非空
	// 即启用:login 重连/公共签发的 battle 票改走 v2 实例绑定签发(roster 权威门同一
	// Redis 快照必须能提供 pod/uid/epoch/allocation,缺失 fail-closed 拒签);hub 票一律
	// 拒签(v2 hub 票只能由 hub_allocator 签)。留空 = 沿用 legacy HS256(dev 行为不变)。
	DSTicket config.DSTicketConf `yaml:"ds_ticket,omitempty" json:"ds_ticket,omitempty"`

	// Locator W3 ⑤ 联动:登录成功后调 PlayerLocatorService.SetLocation(state=LOGIN_PENDING)。
	// addr 为空仅允许 local/off；RequireHubAssignmentBinding=true 时它是 Hub 分配前的
	// 权威门，必须配置且查询/LOGIN_PENDING 写入失败均 fail-closed。
	Locator LocatorClientConf `yaml:"locator,omitempty" json:"locator,omitempty"`

	// Hub W4 ⑥ 联动:登录成功后调 HubAllocatorService.AssignHub 拿真实 hub_ds_addr + hub_ticket。
	// addr 为空 → 不调,回退自签 hub 票据 + MockHubDSAddr(便于本机不起 hub_allocator 也能跑通 login)。
	Hub HubClientConf `yaml:"hub,omitempty" json:"hub,omitempty"`

	// Match(P0 修复 2026-07-15):matchmaker 只读权威兜底。locator presence 未命中 BATTLE
	// 时,login 再查 matchmaker ResolvePlayerMatchContext——player claim + match 记录是
	// "玩家是否属于活跃对局"的耐久事实(不靠 30s TTL)。READY 局即使 locator 投影缺失
	// 也能把玩家路由回原 battle,防 Hub/Battle 双在场。
	// addr 为空 → 不查,presence-only(dev/local 兼容);B1 生产建议必配。
	Match MatchClientConf `yaml:"matchmaker,omitempty" json:"matchmaker,omitempty"`

	// Player(账号 / 角色分离 2026-08-18):把账号名播种成**全服显示名**。
	//
	// 创建角色功能上线前角色名固定 = 账号名,而全服显示名权威是 player 服务的
	// players.nickname(头顶铭牌 / 队伍面板 / 聊天 / 好友 / 公会 / 排行榜都读它)。
	// login 在登录路径上尽力调 player.EnsureProfile 播种(INSERT IGNORE,不覆盖玩家改过的名字)。
	//
	// addr 为空 → 不播种,角色名回落 player 自己的默认前缀名 Player_<player_id>。
	// **弱依赖**:player 不可达 / 版本还没这个 RPC,一律降级,绝不阻断登录(§9.21)。
	Player PlayerClientConf `yaml:"player,omitempty" json:"player,omitempty"`

	// AllowedRoleIDs 是选角白名单(选角权威化 2026-07-08,SelectRole RPC 服务端校验)。
	// 对齐客户端 CfgMisc.DefaultRoleIDs(选角界面可选列表)。
	// 非空 = 严格白名单;空 = fail-closed,SelectRole 一律拒绝(防改包客户端签任意 role_id
	// 进 hub 票据)。dev 宽松(空白名单只校非 0)需显式开 DevAllowAnyRole。
	AllowedRoleIDs []uint32 `yaml:"allowed_role_ids,omitempty" json:"allowed_role_ids,omitempty"`

	// DevAllowAnyRole 开发期选角宽松开关(默认 false,⚠️ 严禁上生产)。
	// 为 true 且 AllowedRoleIDs 为空时,SelectRole 只校验 role_id 非 0(配合客户端配置表快速迭代)。
	DevAllowAnyRole bool `yaml:"dev_allow_any_role,omitempty" json:"dev_allow_any_role,omitempty"`
}

// LocatorClientConf 是 login 调 player_locator 的客户端参数。
type LocatorClientConf struct {
	// Addr player_locator gRPC 端口(默认 127.0.0.1:20006)。
	// 留空仅允许 local/off；Hub assignment binding 激活时 Validate 会拒绝启动。
	Addr string `yaml:"addr,omitempty" json:"addr,omitempty"`
}

// PlayerClientConf 是 login 调 player 的客户端参数(账号 / 角色分离 2026-08-18)。
type PlayerClientConf struct {
	// Addr player gRPC 端口(默认 127.0.0.1:20002)。
	// 留空 → 不播种角色名(角色名回落 player 默认前缀名),其余功能不受影响。
	Addr string `yaml:"addr,omitempty" json:"addr,omitempty"`
}

// MatchClientConf 是 login 调 matchmaker 的客户端参数(P0 修复 2026-07-15)。
type MatchClientConf struct {
	// Addr matchmaker gRPC 端口(默认 127.0.0.1:20011)。
	// 留空 → 不查 matchmaker 耐久权威,仅凭 locator presence 判断在局(dev/local)。
	Addr string `yaml:"addr,omitempty" json:"addr,omitempty"`

	// AuthSecret 是 login→matchmaker ResolvePlayerMatchContext 的内部服务鉴权密钥
	// (pkg/internalrpcauth,HMAC + Redis nonce 防重放)。必须与所连 matchmaker 的
	// match.match_resume_auth_secret 一致,且是独立随机密钥(≥32 字节,不得复用玩家 JWT)。
	// 留空 → 不签名(仅容忍 matchmaker 未启用 resume auth 的裸 dev;启用环境会被拒)。
	AuthSecret string `yaml:"auth_secret,omitempty" json:"auth_secret,omitempty"`

	// AuthAudience 是鉴权受众,必须与 matchmaker 的 match_resume_auth_audience 一致
	// (如 matchmaker:5v5_ranked)。读路径是 canonical 的:PVE 玩家的 pve_coop 记录
	// 同样能从这一实例解析出来(共享 Redis),无需每个 game_mode 各拨一路。
	AuthAudience string `yaml:"auth_audience,omitempty" json:"auth_audience,omitempty"`
}

// HubClientConf 是 login 调 hub_allocator 的客户端参数(W4 ⑥)。
type HubClientConf struct {
	// Addr hub_allocator gRPC 端口(默认 127.0.0.1:20021)。
	// 留空 → 不调 hub_allocator,Login 回退自签 hub 票据 + MockHubDSAddr。
	Addr string `yaml:"addr,omitempty" json:"addr,omitempty"`

	// Region 传给 AssignHub 的大厅区服(空 = 让 hub_allocator 选最空分片)。
	Region string `yaml:"region,omitempty" json:"region,omitempty"`
}

// JWTConf 是 login 签发 SessionToken / DSTicket 的 JWT 参数。
//
// 与 Envoy jwt_authn 的 provider 配套:
//   - Issuer / Audience 必须跟 envoy.yaml 一致(否则 Envoy 会拒)
//   - Secret base64某种 / 明文 都可以,但 envoy.yaml 里是 base64url(secret) 填进 JWKS 的 k 字段
//   - SessionTTL 默认 24h;DSTicketTTL 默认 5min(不变量 §3)
type JWTConf struct {
	Issuer   string `yaml:"issuer,omitempty" json:"issuer,omitempty"`
	Audience string `yaml:"audience,omitempty" json:"audience,omitempty"`
	Secret   string `yaml:"secret,omitempty" json:"secret,omitempty"`
	// AdditionalSecrets 是**仅用于校验**的额外可接受密钥(不用于签发),支持玩家面
	// JWT 不停服密钥轮换(三段式,同 ds_auth.additional_secrets;注意 Envoy JWKS 也要
	// 同步包含全部 key,gen_cluster_config.ps1 -SecretAdditional 一道注入)。默认空。
	AdditionalSecrets []string        `yaml:"additional_secrets,omitempty" json:"additional_secrets,omitempty"`
	SessionTTL        config.Duration `yaml:"session_ttl,omitempty" json:"session_ttl,omitempty"`
	DSTicketTTL       config.Duration `yaml:"ds_ticket_ttl,omitempty" json:"ds_ticket_ttl,omitempty"`
}

// Defaults 把零值填成 Pandora 标准默认值。
func (c *Config) Defaults() {
	if c.Login.SessionTokenTTL == 0 {
		c.Login.SessionTokenTTL = config.Duration(24 * time.Hour)
	}
	if c.Login.DSTicketTTL == 0 {
		c.Login.DSTicketTTL = config.Duration(5 * time.Minute)
	}
	if c.Login.DeviceRetentionDays <= 0 {
		c.Login.DeviceRetentionDays = 90
	}
	if c.Login.PlayerNoStart == 0 {
		c.Login.PlayerNoStart = 1
	}
	if c.Login.MockHubDSAddr == "" {
		c.Login.MockHubDSAddr = "127.0.0.1:7777"
	}
	if c.Login.LoginFailLimit == 0 {
		c.Login.LoginFailLimit = 5
	}
	if c.Login.LoginFailWindow == 0 {
		c.Login.LoginFailWindow = config.Duration(15 * time.Minute)
	}
	if c.Login.LoginFailLock == 0 {
		c.Login.LoginFailLock = config.Duration(5 * time.Minute)
	}
	// JWT(W3 ① 默认)
	if c.Login.JWT.Issuer == "" {
		c.Login.JWT.Issuer = "pandora-login"
	}
	if c.Login.JWT.Audience == "" {
		c.Login.JWT.Audience = "pandora-client"
	}
	if c.Login.JWT.Secret == "" {
		// ❗ dev 默认 secret,不要上生产。envoy.yaml 里同步这个值的 base64url。
		c.Login.JWT.Secret = "pandora-dev-jwt-secret-change-me-32!"
	}
	if c.Login.JWT.SessionTTL == 0 {
		c.Login.JWT.SessionTTL = c.Login.SessionTokenTTL // 默认跟 SessionTokenTTL一致
	}
	if c.Login.JWT.DSTicketTTL == 0 {
		c.Login.JWT.DSTicketTTL = c.Login.DSTicketTTL
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":20001"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":21001"
	}
	if c.Login.PlayerNoResolveAuthSecret != "" && c.Login.PlayerNoResolveAuthAudience == "" {
		c.Login.PlayerNoResolveAuthAudience = "login:player-no"
	}
	if c.Login.FriendPlayerNoResolveAuthSecret != "" && c.Login.FriendPlayerNoResolveAuthAudience == "" {
		c.Login.FriendPlayerNoResolveAuthAudience = "login:player-no"
	}
	if c.Login.GuildPlayerNoResolveAuthSecret != "" && c.Login.GuildPlayerNoResolveAuthAudience == "" {
		c.Login.GuildPlayerNoResolveAuthAudience = "login:player-no"
	}
	c.DSAuth.Defaults()
}

// Validate 校验不能靠运行期降级修复的配置冲突。
func (c *Config) Validate() error {
	type callerCredential struct {
		caller   string
		field    string
		secret   string
		audience string
	}
	credentials := []callerCredential{
		{"team", "player_no_resolve_auth", c.Login.PlayerNoResolveAuthSecret, c.Login.PlayerNoResolveAuthAudience},
		{"friend", "friend_player_no_resolve_auth", c.Login.FriendPlayerNoResolveAuthSecret, c.Login.FriendPlayerNoResolveAuthAudience},
		{"guild", "guild_player_no_resolve_auth", c.Login.GuildPlayerNoResolveAuthSecret, c.Login.GuildPlayerNoResolveAuthAudience},
	}
	seenSecrets := make(map[string]string, len(credentials))
	hasPlayerNoVerifier := false
	for _, credential := range credentials {
		if credential.secret == "" {
			if credential.audience != "" {
				return fmt.Errorf("login.%s_audience requires %s_secret", credential.field, credential.field)
			}
			continue
		}
		hasPlayerNoVerifier = true
		if err := internalrpcauth.ValidateSecret(credential.secret); err != nil {
			return fmt.Errorf("login.%s_secret: %w", credential.field, err)
		}
		if err := internalrpcauth.ValidateIdentity(credential.audience); err != nil {
			return fmt.Errorf("login.%s_audience: %w", credential.field, err)
		}
		if priorCaller, reused := seenSecrets[credential.secret]; reused {
			return fmt.Errorf("login %s player-no HMAC key must differ from %s caller key",
				credential.caller, priorCaller)
		}
		seenSecrets[credential.secret] = credential.caller
	}
	if hasPlayerNoVerifier && c.Node.RedisClient.Host == "" && len(c.Node.RedisClient.Addrs) == 0 {
		return fmt.Errorf("login player-no resolver auth requires node.redis_client replay authority")
	}
	if c.Login.DSTicket.SignerEnabled() && c.Login.DSTicket.ActiveKid == "" {
		return fmt.Errorf("login.ds_ticket signer requires explicit active_kid")
	}
	if c.Login.DSTicket.VerifierEnabled() &&
		(c.Login.DSTicket.ActiveKid == "" || c.Login.DSTicket.KeysetRevision == "") {
		return fmt.Errorf("login.ds_ticket verifier requires explicit active_kid and keyset_revision")
	}
	switch c.DSAuth.AuthorityMode {
	case "", "legacy", "redis":
	default:
		return fmt.Errorf("ds_auth.authority_mode invalid: %q (want legacy|redis)", c.DSAuth.AuthorityMode)
	}
	if err := c.DSAuth.ValidateRedisFence(); err != nil {
		return err
	}
	if c.DSAuth.AuthorityModeRedis() {
		if !c.Login.RequireHubAssignmentBinding {
			return fmt.Errorf("ds_auth.authority_mode=redis requires login.require_hub_assignment_binding=true")
		}
		if !sameFence(c.DSAuth.Fence, c.Login.HubAssignmentFence) {
			return fmt.Errorf("login ds_auth.fence and hub_assignment_fence must be identical (single capability lease)")
		}
	}
	if c.Login.RequireHubAssignmentBinding {
		if c.Node.RedisClient.Host == "" && len(c.Node.RedisClient.Addrs) == 0 {
			return fmt.Errorf("login.require_hub_assignment_binding=true requires node.redis_client")
		}
		if c.Login.Hub.Addr == "" {
			return fmt.Errorf("login.require_hub_assignment_binding=true requires login.hub.addr")
		}
		if c.Login.Locator.Addr == "" {
			return fmt.Errorf("login.require_hub_assignment_binding=true requires login.locator.addr")
		}
		if len(c.Login.HubAssignmentFence.EtcdEndpoints) == 0 || c.Login.HubAssignmentFence.KeysetRevision == "" {
			return fmt.Errorf("login.require_hub_assignment_binding=true requires login.hub_assignment_fence etcd endpoints/keyset revision")
		}
	}
	// 保留期清理模式必须能被识别(§9.24 fail-fast):拼错的值会静默回落 report_only,
	// 运维以为开了 account_devices 清理、实际一行没删,库继续增长且启动期毫无痕迹。
	if err := c.Login.ValidateRetentionMode(); err != nil {
		return err
	}
	return nil
}

// CapabilityFence 返回 login 唯一应注册的 capability 配置。Redis admission 与既有
// Hub assignment fence 同时开启时 Validate 已要求二者完全一致，因此 main 只 Acquire 一次。
func (c *Config) CapabilityFence() (config.DSAuthFenceConf, bool) {
	if c.DSAuth.AuthorityModeRedis() {
		return c.DSAuth.Fence, true
	}
	if c.Login.RequireHubAssignmentBinding {
		return c.Login.HubAssignmentFence, true
	}
	return config.DSAuthFenceConf{}, false
}

func sameFence(a, b config.DSAuthFenceConf) bool {
	return slices.Equal(a.EtcdEndpoints, b.EtcdEndpoints) && a.EtcdPrefix == b.EtcdPrefix &&
		a.EtcdLeaseTTLSec == b.EtcdLeaseTTLSec && a.EtcdDialTimeout == b.EtcdDialTimeout &&
		a.KeysetRevision == b.KeysetRevision
}

// RetentionMode 返回生效的保留期清理模式(默认 ModeReportOnly = 只报告不删,§9.24 +
// 2026-07-22 用户指令:不能因为数据大了就删数据)。
func (l *LoginConf) RetentionMode() dbguard.Mode {
	m, err := dbguard.ParseMode(l.RetentionModeRaw)
	if err != nil {
		return dbguard.ModeReportOnly
	}
	return m
}

// ValidateRetentionMode 供启动 fail-fast(写了无法识别的模式必须拒启)。
func (l *LoginConf) ValidateRetentionMode() error {
	_, err := dbguard.ParseMode(l.RetentionModeRaw)
	return err
}
