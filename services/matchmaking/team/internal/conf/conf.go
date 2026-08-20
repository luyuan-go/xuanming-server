// Package conf 是 team 服务的私有配置结构。
package conf

import (
	"fmt"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	"github.com/luyuancpp/pandora/pkg/playername"
	"github.com/luyuancpp/pandora/pkg/playerno"
)

// Config 是 team 服务的完整配置。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	Team TeamConf `yaml:"team" json:"team"`

	// DSAuth DS 回调服务令牌校验(verify-only)。GetPlayerTeam 是 DS 内部东西向反查,
	// systemOnly 只证明「不带玩家 JWT」,证明不了「调用方是 DS」;本配置提供后者。
	// mode 默认 off(不校验),与接线前行为一致。
	//
	// 2026-08-17 入列:team 已进两处权威清单(gen_cluster_config.ps1 的 DsSecretServiceNames
	// + online_manifest_contract.ps1 的 PandoraDsCallbackHmacServices),etc 模板带 ds_auth
	// 节点(permissive 灰度档)。生成器的双向断言(节点存在 ⟺ 在清单里)自此反向成立:
	// **删节点或删清单必须同批**,否则真实配置生成 [FATAL]。此前未入列时守卫为 nil、
	// Check 恒放行 —— GetPlayerTeam 的 RequireToken 曾是纸面门,入列正是为了封它。
	DSAuth config.DSAuthConf `yaml:"ds_auth,omitempty" json:"ds_auth,omitempty"`
}

// TeamConf 是 team 服务私有配置。
type TeamConf struct {
	// InviteTTL 邀请令牌 Redis key 的 TTL,客户端须在此时间内 AcceptInvite。
	InviteTTL config.Duration `yaml:"invite_ttl,omitempty" json:"invite_ttl,omitempty"`

	// DisbandedRetention 队伍解散后 Redis key 的保留时长,供客户端查询最终状态。
	DisbandedRetention config.Duration `yaml:"disbanded_retention,omitempty" json:"disbanded_retention,omitempty"`

	// ActiveTTL 活跃队伍(未解散)Redis key 的生命周期。
	// 队伍在此时间内无任何写操作则整体过期消失,防止僵尸队伍长期占用 Redis。
	ActiveTTL config.Duration `yaml:"active_ttl,omitempty" json:"active_ttl,omitempty"`

	// MaxMembers MOBA 5v5,一队最多允许多少成员。
	MaxMembers int `yaml:"max_members,omitempty" json:"max_members,omitempty"`

	// OptimisticRetry WATCH/MULTI/EXEC 乐观锁冲突时最大重试次数。
	// 耗尽后返回 ErrTeamConcurrent(3007)。
	OptimisticRetry int `yaml:"optimistic_retry,omitempty" json:"optimistic_retry,omitempty"`

	// MatchmakerAddr matchmaker 服务 gRPC 直连地址(host:port,内网 insecure)。
	// 成员离队/被踢时联动撤销其所在的匹配票据(弱依赖):留空 → 不联动,
	// 行为与历史一致(本机不起 matchmaker 的骨架联调路径)。
	MatchmakerAddr string `yaml:"matchmaker_addr,omitempty" json:"matchmaker_addr,omitempty"`

	// PlayerNoResolverAddr 是 login 账号权威的内部 gRPC 地址。team 每次组装客户端 Team
	// 快照时对整支队伍做一次批量读取；留空时 player_no 全部降级为 0。
	PlayerNoResolverAddr string `yaml:"player_no_resolver_addr,omitempty" json:"player_no_resolver_addr,omitempty"`
	// PlayerNoResolverAuthSecret/Audience 是 team→login ResolvePlayerNos 的独立
	// request-bound internalrpcauth 凭据，不得复用玩家 JWT 或其它服务间密钥。
	PlayerNoResolverAuthSecret   string `yaml:"player_no_resolver_auth_secret,omitempty" json:"player_no_resolver_auth_secret,omitempty"`
	PlayerNoResolverAuthAudience string `yaml:"player_no_resolver_auth_audience,omitempty" json:"player_no_resolver_auth_audience,omitempty"`

	// PlayerNameResolver* 是 team→player ResolvePlayerNames 的独立内部批量读取配置。
	// 留空时 nickname 全部降级为空；不得回退 Redis 中的历史 nickname 影子。
	PlayerNameResolverAddr         string `yaml:"player_name_resolver_addr,omitempty" json:"player_name_resolver_addr,omitempty"`
	PlayerNameResolverAuthSecret   string `yaml:"player_name_resolver_auth_secret,omitempty" json:"player_name_resolver_auth_secret,omitempty"`
	PlayerNameResolverAuthAudience string `yaml:"player_name_resolver_auth_audience,omitempty" json:"player_name_resolver_auth_audience,omitempty"`

	// MatchResumeAuthSecret / MatchResumeAuthAudience 是 team→matchmaker 调
	// ResolvePlayerMatchContext 的东西向服务鉴权凭据(pkg/internalrpcauth),caller 固定
	// 签成 "team"。matchmaker 侧对该方法强制验签,**不签名一律 ERR_PERMISSION_DENY(7)**。
	//
	// 这把密钥必须与 login 那把 match_resume_auth_secret **不同**(matchmaker 侧按 caller
	// 分别持有独立密钥,共用会把两个服务的信任域合并),对应 matchmaker 的
	// match.team_resume_auth_secret。audience 填 matchmaker 的 match_resume_auth_audience
	// (audience 标识被调方服务池,不区分调用方,故与 login 相同)。
	//
	// 留空 → 不签名:入队闸门与招募列表复核都会拿不到判定,前者 fail-closed 拒绝入队、
	// 后者返回空列表。因此 matchmaker_addr 已配时留空会在启动打 WARN。
	MatchResumeAuthSecret   string `yaml:"match_resume_auth_secret,omitempty" json:"match_resume_auth_secret,omitempty"`
	MatchResumeAuthAudience string `yaml:"match_resume_auth_audience,omitempty" json:"match_resume_auth_audience,omitempty"`

	// ── 反方向:matchmaker → team 的组票 / 复位调用鉴权(INC-20260813-001 A-13)───────
	//
	// BeginTeamMatch / EndTeamMatch 是 matchmaker 专用的东西向接口,但 `systemOnly` 只能
	// 证明「本次调用不带玩家 JWT」——集群内网里任何 Pod 都满足。而这两个方法一个能给**任意**
	// 队伍上 roster 租约(反复调 = 让那支队伍永远开不了局),一个能把**任意**队伍打回 FORMING。
	// 因此必须像 team→matchmaker 那样验签,caller 固定 "matchmaker"。
	//
	// **这把密钥必须与 MatchResumeAuthSecret 不同**:那把是 team 拿去签**出站**调用的,
	// 两边共用等于让任一方能冒充另一方(与该字段注释里 login/team 分域同理)。
	//
	// 三档,刻意做成可降级(§9.21 共存窗口不得靠发布顺序):
	//   - 留空          → 完全不验(现状,滚动升级期先滚 team 不会打断任何调用);
	//   - 配了 + require=false(默认)→ **观察期**:验不过只 WARN 后放行,用于确认
	//     matchmaker 已全量滚上带签名的版本(看日志里 rejected 归零);
	//   - 配了 + require=true         → 强制:验不过一律 ERR_PERMISSION_DENY。
	//
	// 正确上线顺序因此是「两边都配密钥 → 观察 → 翻 require」,每一步单独都安全,
	// 不存在「谁必须先上线」的隐含约束。
	MatchCallAuthSecret   string `yaml:"match_call_auth_secret,omitempty" json:"match_call_auth_secret,omitempty"`
	MatchCallAuthAudience string `yaml:"match_call_auth_audience,omitempty" json:"match_call_auth_audience,omitempty"`
	MatchCallAuthRequire  bool   `yaml:"match_call_auth_require,omitempty" json:"match_call_auth_require,omitempty"`

	// InvitePushMode 邀请推送模式(金丝雀灰度用)。老客户端只认 TeamUpdateEvent(reason=INVITE_SENT),
	// 新客户端只认独立的 TeamInviteEvent(event_type=INVITE=1、已不再从 TeamUpdateEvent 读邀请)。
	// 两代客户端各认各的 payload,单一模式无法同时喂饱两代 → 灰度共存期必须"双发"。
	//   - "dual"(默认):两条都发。老客户端用 legacy 弹框、把独立事件当 TeamUpdateEvent 误解→
	//     InviteId/TeamId=0→护栏不过→仅多一次无害快照,不误弹;新客户端忽略 legacy(纯化后不读)、
	//     只在独立事件弹框。**各弹一次、不双弹**(客户端已纯化,故双发安全,金丝雀期首选)。
	//   - "dedicated":只发独立事件。全量铺完新客户端后切到此值,停发冗余 legacy。老客户端会漏弹。
	//   - "legacy":只发旧 TeamUpdateEvent。回退用;新客户端会漏弹邀请框。
	// 空串按 "dual" 处理(见 Defaults)。
	InvitePushMode string `yaml:"invite_push_mode,omitempty" json:"invite_push_mode,omitempty"`

	// MaxPendingInvites 同一被邀请人的未过期 pending 邀请数上限(不变量 §9-18:
	// 组队邀请是客户端可写入、可堆积、会被拉取展示的列表,必须有写入侧上限)。
	// 超限 Invite 返 ErrTeamInvitePendingLimit(3008);ListMyPendingInvites 单次返回
	// 也按此值截断(写入侧硬上限已兜住总量,无需分页)。默认 10。
	MaxPendingInvites int `yaml:"max_pending_invites,omitempty" json:"max_pending_invites,omitempty"`

	// ── 找队伍(ListOpenTeams / ApplyToTeam / HandleTeamApplication) ──────────

	// JoinPolicy 决定 ApplyToTeam 的语义,全服一份配置(不给每支队伍存,§9.22 不重复影子状态):
	//   - "approval"(默认):申请进入队伍待处理列表,队长同意才入队;
	//   - "open"          :队伍未满即当场入队,不需要队长操作。
	// 空串按 "approval" 处理(见 Defaults):默认保守——不经队长同意就把陌生人塞进队伍
	// 属于"多做了玩家没授权的事",反过来只是多一次点击。
	// ParseJoinPolicy 对无法识别的值**报错而非猜**(拼错一个字母就变成任何人都能进队,
	// 是不可接受的失败模式),启动 ValidateJoinPolicy fail-fast。
	JoinPolicy string `yaml:"join_policy,omitempty" json:"join_policy,omitempty"`

	// MaxOpenTeamsPerQuery 单次 ListOpenTeams 返回的队伍数上限(读取侧上限,不变量 §9-18)。
	// 请求里的 limit=0 取本值,>本值一律钳到本值。默认 10。
	MaxOpenTeamsPerQuery int `yaml:"max_open_teams_per_query,omitempty" json:"max_open_teams_per_query,omitempty"`

	// MaxApplicationsPerTeam 同一队伍未过期 pending 入队申请数上限(写入侧上限,不变量 §9-18:
	// 入队申请是客户端可写入、可堆积、会被队长拉取展示的列表)。
	// 超限 ApplyToTeam 返 ErrTeamApplyPendingLimit(3009);ListTeamApplications 单次返回
	// 也按此值截断(写入侧硬上限已兜住总量,无需分页)。默认 10。
	MaxApplicationsPerTeam int `yaml:"max_applications_per_team,omitempty" json:"max_applications_per_team,omitempty"`

	// RateQuotaPerMin 申请/邀请的 per-player 每分钟频率配额(anti-abuse §6 第 6 项;
	// 默认 12,即人均 5s 一次,远高于人类点按、远低于外挂速率;负值 = 关闭)。
	// 与 §9.18 的 pending 总量上限正交:总量限「同时挂多少」,本值限「刷多快」,
	// 专挡「加满 → 全撤 → 再加满」的 D 类写放大循环。窗口固定 1 分钟(少一个可配错的旋钮)。
	RateQuotaPerMin int `yaml:"rate_quota_per_min,omitempty" json:"rate_quota_per_min,omitempty"`

	// LocatorAddr player_locator 服务 gRPC 直连地址(host:port,内网 insecure)。
	// 当前仅保留配置结构；离线自动退队在获得与 StartMatch 共用的 roster fence 前禁止开启，
	// 见 ValidateOfflineLeave。
	LocatorAddr string `yaml:"locator_addr,omitempty" json:"locator_addr,omitempty"`

	// OfflineLeave 离线成员自动退队。
	OfflineLeave OfflineLeaveConf `yaml:"offline_leave,omitempty" json:"offline_leave,omitempty"`

	// ApplyTTL 入队申请令牌的存活时长。取值依据:队长要能在一次组队界面停留内看到并处理
	// (推送到达 + 人眼反应 + 点击),同时申请人不该被无限期挂着——到期后客户端按钮自动
	// 恢复可点,是"有界等待 + 可见出口"(验收底线第 1 条)。默认 120s(邀请是 60s,
	// 申请方向多一次队长注意力成本,故取两倍)。**待实测复核**:若线上出现大量
	// "队长还没看到就过期",优先调大本值而不是取消上限。
	ApplyTTL config.Duration `yaml:"apply_ttl,omitempty" json:"apply_ttl,omitempty"`
}

// OfflineLeaveConf 是「队员离线超时自动移出队伍」的配置。
//
// **当前禁止开启**:team 的成员 WATCH 与 matchmaker 的 StartMatch durable operation
// 不在同一个事务域，也没有共用 roster fence。仅靠先后读取 commitment 存在 TOCTOU：
// StartMatch 可以先读旧成员快照、再在摘人提交后受理旧名单。ValidateOfflineLeave 因此
// 对 enabled=true 无条件 fail-fast；关闭时 team 不查 locator、不起消费者与复查循环。
type OfflineLeaveConf struct {
	// Enabled 是否启用。当前只能为 false；true 会在启动时 fail-fast。
	Enabled bool `yaml:"enabled,omitempty" json:"enabled,omitempty"`

	// Threshold 离线多久才移出队伍。默认 180s。
	//
	// 取值依据(**不要随手调小**):
	//  ① 下界由「一个会回来的玩家最快多久能回来」决定 —— 客户端 watchdog 发现断线 →
	//     重新走登录恢复 → 旧 Controller 清退(Hub DS 上最长挂满 UE ConnectionTimeout=60s)
	//     → Travel + Hub 地图加载 → Admission ACK → PostLogin 重写位置,保守 30~60s。
	//     低于这个数,一个只断了几秒网、立刻开始重连的玩家也会被踢,与网络好坏无关,是纯误伤。
	//  ② 上界由「弱网玩家的连续失联时长」决定 —— 地铁 / 隧道 / 电梯断 10~30s 是常态,
	//     要容忍一段 ~2 分钟的无信号区就得再加 120s。
	//  ③ 另外还要叠加**检测本身的延迟**:静默掉线要等 UE ConnectionTimeout(60s)+
	//     locator grace(10s)才被观测到,所以玩家实际被容忍的总时长 ≈ 阈值 + 60~90s。
	// 180s 是 ①②③ 下的保守取值。**待实测复核**:线上若出现「地铁回来队伍没了」的反馈,
	// 优先调大本值,而不是去压 UE 的 ConnectionTimeout(那会让全服弱网玩家更容易掉线)。
	Threshold config.Duration `yaml:"threshold,omitempty" json:"threshold,omitempty"`

	// CheckInterval 到期复查轮询周期。默认 15s。
	// 不必更细:上游检测本身就有 60~90s 的模糊,压到秒级只是成倍花钱买不到精度。
	CheckInterval config.Duration `yaml:"check_interval,omitempty" json:"check_interval,omitempty"`

	// Budget 单轮最多处理多少个到期玩家。默认 200(防积压一次性打爆 locator / matchmaker)。
	Budget int `yaml:"budget,omitempty" json:"budget,omitempty"`

	// KafkaPartitions 离场事件 topic 的分区数(消费者按分区起 worker)。默认 3。
	KafkaPartitions int32 `yaml:"kafka_partitions,omitempty" json:"kafka_partitions,omitempty"`
}

// ValidateOfflineLeave 供 main 启动时 fail-fast。
//
// 当前即使 locator / matchmaker 都已配置也不允许开启：两次跨服务 commitment 读取无法
// 与 Team 的 WATCH/MULTI/EXEC、StartMatch 的 durable operation 构成共同线性化点。
// 必须先让两条写路径共享按 team_id + operation_id fencing 的 roster 预留/CAS；在此之前
// 静默运行会把“可能拆掉刚开始匹配的队伍”伪装成已受保护，故启动时明确拒绝。
func (c *Config) ValidateOfflineLeave() error {
	if !c.Team.OfflineLeave.Enabled {
		return nil
	}
	// 曾经因为「与 StartMatch 缺少共同线性化点」在这里 fail-fast。该前提已不成立:
	// matchmaker 组票改走 TeamService.BeginTeamMatch,在 team 自己的乐观锁内冻结名单,
	// 摘人在同一把锁内看到租约即推迟 —— TOCTOU 窗口已消除,不再只是后果收敛。
	//
	// 剩下的依赖校验照旧:少任一个都会让功能静默失效或失去对局闸门。
	if c.Team.LocatorAddr == "" {
		return fmt.Errorf("team: offline_leave.enabled=true requires team.locator_addr")
	}
	if c.Team.MatchmakerAddr == "" {
		// 没有 matchmaker 就没法判「这支队伍是不是正被一场对局占住」(整场战斗期间的占用
		// 权威是 player→ticket claim,不是那把秒级 roster 租约),自动退队会有把在打的
		// 队伍拆掉的风险,宁可不启动。
		return fmt.Errorf("team: offline_leave.enabled=true requires team.matchmaker_addr (match commitment gate)")
	}
	return nil
}

// ValidatePlayerNoResolver 防止“地址已配但签名缺失”静默把所有展示编号降级为 0。
func (c *Config) ValidatePlayerNoResolver() error {
	if c.Team.PlayerNoResolverAddr == "" {
		if c.Team.PlayerNoResolverAuthSecret != "" {
			return fmt.Errorf("team.player_no_resolver_auth_secret requires team.player_no_resolver_addr")
		}
		if c.Team.PlayerNoResolverAuthAudience != "" {
			return fmt.Errorf("team.player_no_resolver_auth_audience requires team.player_no_resolver_addr")
		}
		return nil
	}
	if err := internalrpcauth.ValidateSecret(c.Team.PlayerNoResolverAuthSecret); err != nil {
		return fmt.Errorf("team.player_no_resolver_auth_secret: %w", err)
	}
	if err := internalrpcauth.ValidateIdentity(c.Team.PlayerNoResolverAuthAudience); err != nil {
		return fmt.Errorf("team.player_no_resolver_auth_audience: %w", err)
	}
	if c.Team.MaxMembers < 1 || c.Team.MaxMembers > playerno.ResolveBatchLimit {
		return fmt.Errorf("team.max_members must be within [1,%d] when player_no resolver is enabled",
			playerno.ResolveBatchLimit)
	}
	if c.Team.MaxApplicationsPerTeam < 1 || c.Team.MaxApplicationsPerTeam > playerno.ResolveBatchLimit {
		return fmt.Errorf("team.max_applications_per_team must be within [1,%d] when player_no resolver is enabled",
			playerno.ResolveBatchLimit)
	}
	if c.Team.MaxOpenTeamsPerQuery < 1 || c.Team.MaxOpenTeamsPerQuery > playerno.ResolveBatchLimit {
		return fmt.Errorf("team.max_open_teams_per_query must be within [1,%d] when player_no resolver is enabled",
			playerno.ResolveBatchLimit)
	}
	return nil
}

// ValidatePlayerNameResolver 防止“地址已配但签名缺失”静默让所有成员名字降级为空。
func (c *Config) ValidatePlayerNameResolver() error {
	if c.Team.PlayerNameResolverAddr == "" {
		if c.Team.PlayerNameResolverAuthSecret != "" {
			return fmt.Errorf("team.player_name_resolver_auth_secret requires team.player_name_resolver_addr")
		}
		if c.Team.PlayerNameResolverAuthAudience != "" {
			return fmt.Errorf("team.player_name_resolver_auth_audience requires team.player_name_resolver_addr")
		}
		return nil
	}
	if err := internalrpcauth.ValidateSecret(c.Team.PlayerNameResolverAuthSecret); err != nil {
		return fmt.Errorf("team.player_name_resolver_auth_secret: %w", err)
	}
	if err := internalrpcauth.ValidateIdentity(c.Team.PlayerNameResolverAuthAudience); err != nil {
		return fmt.Errorf("team.player_name_resolver_auth_audience: %w", err)
	}
	if c.Team.MaxMembers < 1 || c.Team.MaxMembers > playername.ResolveBatchLimit {
		return fmt.Errorf("team.max_members must be within [1,%d] when player_name resolver is enabled",
			playername.ResolveBatchLimit)
	}
	if c.Team.MaxApplicationsPerTeam < 1 || c.Team.MaxApplicationsPerTeam > playername.ResolveBatchLimit {
		return fmt.Errorf("team.max_applications_per_team must be within [1,%d] when player_name resolver is enabled",
			playername.ResolveBatchLimit)
	}
	if c.Team.MaxOpenTeamsPerQuery < 1 || c.Team.MaxOpenTeamsPerQuery > playername.ResolveBatchLimit {
		return fmt.Errorf("team.max_open_teams_per_query must be within [1,%d] when player_name resolver is enabled",
			playername.ResolveBatchLimit)
	}
	return nil
}

// 入队策略取值(JoinPolicy)。
const (
	// JoinPolicyApproval 申请 → 队长审批 → 入队。
	JoinPolicyApproval = "approval"
	// JoinPolicyOpen 未满即直接入队。
	JoinPolicyOpen = "open"
)

// ParseJoinPolicy 把配置字符串解析成合法策略值。
//
// 空串 = 默认 approval(保守)。**无法识别的值报错,绝不猜**:把 "aproval" 猜成 open
// 会让全服队伍对陌生人敞开,属于静默的权限放大,失败模式不可接受(同 dbguard.ParseMode
// 对 retention_mode 的处理口径)。
func ParseJoinPolicy(s string) (string, error) {
	switch s {
	case "":
		return JoinPolicyApproval, nil
	case JoinPolicyApproval, JoinPolicyOpen:
		return s, nil
	default:
		return "", fmt.Errorf("team: unknown join_policy %q (want %q or %q)", s, JoinPolicyApproval, JoinPolicyOpen)
	}
}

// ValidateJoinPolicy 供 main 启动时 fail-fast:配置写错不该等到第一个玩家点"申请"才暴露。
func (c *Config) ValidateJoinPolicy() error {
	_, err := ParseJoinPolicy(c.Team.JoinPolicy)
	return err
}

// Defaults 填默认值,防止 yaml 缺字段时零值引发 panic。
func (c *Config) Defaults() {
	c.DSAuth.Defaults()
	if c.Team.InviteTTL == 0 {
		c.Team.InviteTTL = config.Duration(60 * time.Second)
	}
	if c.Team.DisbandedRetention == 0 {
		c.Team.DisbandedRetention = config.Duration(5 * time.Minute)
	}
	if c.Team.ActiveTTL == 0 {
		c.Team.ActiveTTL = config.Duration(60 * time.Minute)
	}
	if c.Team.MaxMembers == 0 {
		c.Team.MaxMembers = 5
	}
	if c.Team.OptimisticRetry == 0 {
		c.Team.OptimisticRetry = 3
	}
	if c.Team.PlayerNoResolverAddr != "" && c.Team.PlayerNoResolverAuthAudience == "" {
		c.Team.PlayerNoResolverAuthAudience = "login:player-no"
	}
	if c.Team.PlayerNameResolverAddr != "" && c.Team.PlayerNameResolverAuthAudience == "" {
		c.Team.PlayerNameResolverAuthAudience = "player:name"
	}
	if c.Team.InvitePushMode == "" {
		// 金丝雀期默认双发:老/新客户端各认各的 payload,互不干扰,各弹一次。
		c.Team.InvitePushMode = "dual"
	}
	if c.Team.MaxPendingInvites == 0 {
		c.Team.MaxPendingInvites = 10
	}
	if c.Team.JoinPolicy == "" {
		// 保守默认:需要队长同意。改成 open 必须是显式配置动作。
		c.Team.JoinPolicy = JoinPolicyApproval
	}
	if c.Team.MaxOpenTeamsPerQuery == 0 {
		c.Team.MaxOpenTeamsPerQuery = 10
	}
	if c.Team.MaxApplicationsPerTeam == 0 {
		c.Team.MaxApplicationsPerTeam = 10
	}
	if c.Team.RateQuotaPerMin == 0 {
		c.Team.RateQuotaPerMin = 12
	}
	if c.Team.ApplyTTL == 0 {
		c.Team.ApplyTTL = config.Duration(120 * time.Second)
	}
	if c.Team.OfflineLeave.Threshold == 0 {
		c.Team.OfflineLeave.Threshold = config.Duration(180 * time.Second)
	}
	if c.Team.OfflineLeave.CheckInterval == 0 {
		c.Team.OfflineLeave.CheckInterval = config.Duration(15 * time.Second)
	}
	if c.Team.OfflineLeave.Budget == 0 {
		c.Team.OfflineLeave.Budget = 200
	}
	if c.Team.OfflineLeave.KafkaPartitions == 0 {
		c.Team.OfflineLeave.KafkaPartitions = 3
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":20010"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":21010"
	}
}
