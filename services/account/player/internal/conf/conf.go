// Package conf 是 player 服务的私有配置结构(W4 ④,2026-06-06)。
package conf

import (
	"fmt"
	"strings"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	"github.com/luyuancpp/pandora/pkg/kafkax"
)

// Config 是 player 服务的完整配置。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	Player PlayerConf `yaml:"player" json:"player"`

	// DSAuth DS 回调服务令牌校验(verify-only)。GetLoadout 自 2026-08-13 起挂上了 Envoy
	// DS 面(:8444)精确路由,而该监听器没有 jwt_authn —— callerID 恒为 0,原先「callerID==0
	// 即后端内部可信」的双模读语义在该方法上不再成立(任何能连 :8444 或直连 20002 的进程
	// 都能查任意玩家出战快照)。本配置提供「调用方确实是 DS」的那半证明。
	// player 2026-08-13 已入权威清单(gen_cluster_config.ps1 的 DsSecretServiceNames 与
	// online_manifest_contract.ps1 的 PandoraDsCallbackHmacServices),因此 etc/ 模板里
	// **必须**带 ds_auth 节点 —— 生成器对每个服务做双向断言(节点存在 ⟺ 在权威清单里),
	// 少了就是 `[FATAL] player 的 ds_auth 节点与权威服务清单不一致`,且只在真实配置生成时才炸。
	// 三者(dev 模板 / 两处清单)必须同增同减,ds_auth_conf_test.go 把这条钉在 go test 里。
	//
	// player 只**验签**不签发:签发方是 ds_allocator / hub_allocator,secret 三方同值。
	// 档位:dev=permissive(验签路径全跑、失败只 warn,不改变现有行为),prod=enforce。
	DSAuth config.DSAuthConf `yaml:"ds_auth,omitempty" json:"ds_auth,omitempty"`
}

// PlayerConf 是 player 服务私有配置。
type PlayerConf struct {
	// BaseMMR 新玩家缺省 MMR(EnsureProfile / GetMMR 未建档兜底,默认 1500,与 battle_result 对齐)。
	BaseMMR int `yaml:"base_mmr,omitempty" json:"base_mmr,omitempty"`

	// MMRFloor MMR 下限(UpdateMMR 后 clamp,默认 0)。
	MMRFloor int `yaml:"mmr_floor,omitempty" json:"mmr_floor,omitempty"`

	// DefaultNicknamePrefix 默认昵称前缀(EnsureProfile 建档时 nickname=prefix+player_id,保证 uk 唯一,默认 "Player_")。
	DefaultNicknamePrefix string `yaml:"default_nickname_prefix,omitempty" json:"default_nickname_prefix,omitempty"`

	// MaxNicknameLen 昵称最大长度(UpdateNickname 校验,默认 32)。
	MaxNicknameLen int `yaml:"max_nickname_len,omitempty" json:"max_nickname_len,omitempty"`

	// PlayerNameResolveAuthSecret/Audience 校验 team→player ResolvePlayerNames 的独立
	// request-bound internalrpcauth 身份，不得复用 DS callback secret 或玩家 JWT。
	PlayerNameResolveAuthSecret         string `yaml:"player_name_resolve_auth_secret,omitempty" json:"player_name_resolve_auth_secret,omitempty"`
	PlayerNameResolveAuthAudience       string `yaml:"player_name_resolve_auth_audience,omitempty" json:"player_name_resolve_auth_audience,omitempty"`
	FriendPlayerNameResolveAuthSecret   string `yaml:"friend_player_name_resolve_auth_secret,omitempty" json:"friend_player_name_resolve_auth_secret,omitempty"`
	FriendPlayerNameResolveAuthAudience string `yaml:"friend_player_name_resolve_auth_audience,omitempty" json:"friend_player_name_resolve_auth_audience,omitempty"`
	GuildPlayerNameResolveAuthSecret    string `yaml:"guild_player_name_resolve_auth_secret,omitempty" json:"guild_player_name_resolve_auth_secret,omitempty"`
	GuildPlayerNameResolveAuthAudience  string `yaml:"guild_player_name_resolve_auth_audience,omitempty" json:"guild_player_name_resolve_auth_audience,omitempty"`

	// HeroSelectionEnabled 出战英雄选择功能开关(默认 false,demo 阶段跳过选英雄,
	// 与 login demo-skip 风格一致;关闭时 SelectHero 返回 ERR_PLAYER_FEATURE_DISABLED)。
	HeroSelectionEnabled bool `yaml:"hero_selection_enabled,omitempty" json:"hero_selection_enabled,omitempty"`

	// LoadoutCustomizeEnabled 出战装备预设 / 天赋树自定义功能开关(默认 false;
	// 关闭时 SetEquipment / SetTalents / ResetTalents 返回 ERR_PLAYER_FEATURE_DISABLED;
	// 授予类 GrantTalentPoints 由系统驱动不受此开关影响)。
	//
	// 2026-07-25:2026-06-17 审查提出的三项校验已补齐,本开关不再是唯一防线——
	//   - isEquip / slotMatch:读 configtable 道具表(d_道具.xlsx「装备部位」列),零 RPC;
	//   - ownEquipment:经 inventory.CheckInstancesOwned 系统 RPC 精确确认实例归属(见 InventoryAddr);
	//   - SetTalents:读 configtable 专精表校验等级上限 / 前置 / 总消耗。
	// 三条依赖任一缺失(表未加载 / InventoryAddr 未配)一律 fail-closed 拒绝,不会退化成放行。
	// 因此打开本开关的前提是 **config_table.dir 已就绪 + inventory_addr 已配置**。
	LoadoutCustomizeEnabled bool `yaml:"loadout_customize_enabled,omitempty" json:"loadout_customize_enabled,omitempty"`

	// InventoryAddr inventory 服务 gRPC 端点(host:port,内网直连无 JWT)。
	// SetEquipment/GetLoadout 的精确实例归属校验依赖它;留空 = 校验器不接线,写入与战斗快照
	// fail-closed 返回内部错误(不会静默跳过校验)。LoadoutCustomizeEnabled=true 时必配。
	InventoryAddr string `yaml:"inventory_addr,omitempty" json:"inventory_addr,omitempty"`

	// ConsumeTopics 本服订阅的 kafka topic(默认 [player.update])。
	ConsumeTopics []string `yaml:"consume_topics,omitempty" json:"consume_topics,omitempty"`

	// ExperienceEnabled 玩家经验入账开关。曲线始终来自 configtable；本开关只控制功能放行，
	// 不再兼任数值载体。策划正式数值确认前生产保持 false。
	ExperienceEnabled bool `yaml:"experience_enabled,omitempty" json:"experience_enabled,omitempty"`

	// MaxExpPerGrant 单次 AddExperience 入账上限(默认 1000000)。
	// 防异常 / 越权调用方一次灌满等级(DS 不可信纵深:battle_result 已按怪物表换算,
	// 这里是 player 侧最后一道兜底)。
	MaxExpPerGrant uint64 `yaml:"max_exp_per_grant,omitempty" json:"max_exp_per_grant,omitempty"`

	// PushOutboxInterval 经验推送出箱发布轮询间隔(默认 1s;经验条刷新体感由它决定上界)。
	PushOutboxInterval config.Duration `yaml:"push_outbox_interval,omitempty" json:"push_outbox_interval,omitempty"`

	// PushOutboxBatch 每轮发布取多少条推送出箱记录(默认 128)。
	PushOutboxBatch int `yaml:"push_outbox_batch,omitempty" json:"push_outbox_batch,omitempty"`

	// PushWriterLease 推送出箱发布器的单写者选举(与 mission 同款,推导见该结构注释)。
	PushWriterLease PushWriterLeaseConf `yaml:"push_writer_lease,omitempty" json:"push_writer_lease,omitempty"`

	// RetentionModeRaw 保留期清理模式(§9.24 全服标准口径):留空 / "report_only" / "report"
	// = **只统计待清理量并 WARN 告警,一行都不删**(默认);"delete" = 真删。
	// 无法识别的值由 ValidateRetentionMode 在启动期 fail-fast 拒启 —— 拼错一个字母就开始
	// 删生产数据是不可接受的失败模式,绝不能猜成 delete。
	//
	// 与下面两个 *_cleanup_enabled 的分工(两道闸,都开才删):
	//   retention_mode        = 本服的删除总闸(运维口径,§9.24 默认只报告)
	//   *_cleanup_enabled     = 每组表的**前置条件确认**(技术口径:上游重放窗口已经小于
	//                           留存期了吗?)。默认 false,即便总闸开着也只报告不删。
	// 两个都是"默认不删",分开是因为它们回答的是不同的问题,且前置条件对 exp_history
	// (battle_result progress 出箱无总重试期限)与 mmr/点数/发卡组(kafka 重放 + 授予补扫)
	// 并不同时成立 —— 合成一个开关会让"这组已确认有界、那组还没有"无法表达。
	RetentionModeRaw string `yaml:"retention_mode,omitempty" json:"retention_mode,omitempty"`

	// ExpHistoryCleanupEnabled 经验幂等收据(exp_history)**删除**前置条件确认开关
	// (**默认 false=只报告不删**,审计 P1:battle_result progress 出箱只有退避上限(5min)
	// 没有总重试期限——入账成功但响应/删行持续失败超过留存期后,同一事件会被再次入账(双发)。
	// 开启前置条件:上游出箱必须先有小于留存期的有界重试/隔离期限,否则收据表宁可增长
	// 也不能破坏幂等,§9.2)。关闭时 janitor 仍会跑,只是模式降为 report_only。
	ExpHistoryCleanupEnabled bool `yaml:"exp_history_cleanup_enabled,omitempty" json:"exp_history_cleanup_enabled,omitempty"`

	// ExpHistoryRetention 经验幂等收据(exp_history)留存期(默认 7 天,下限 7 天:
	// 必须严格覆盖 battle_result progress 出箱最长重试窗)。report_only 下它决定
	// "多少行算待清理"(报告口径),delete 下它决定删哪些行 —— 两边同一个值。
	ExpHistoryRetention config.Duration `yaml:"exp_history_retention,omitempty" json:"exp_history_retention,omitempty"`

	// HistoryCleanupEnabled mmr_history / attr_point_grants / talent_point_grants /
	// skill_card_grants 幂等历史行**删除**前置条件确认开关(**默认 false=只报告不删**,
	// 与 exp_history 同理由:上游 kafka player.update 消费与授予补扫是 at-least-once,
	// 清掉幂等行后同一事件重放 = 双发(重复加段位分 / 重复加点 / 重复发卡)。开启前置条件:
	// 上游重放期限(kafka retention / 补扫窗口)必须小于留存期,由运维确认后配置。
	// CLAUDE.md §9 不变量 24)。关闭时 janitor 仍会跑,只是模式降为 report_only。
	HistoryCleanupEnabled bool `yaml:"history_cleanup_enabled,omitempty" json:"history_cleanup_enabled,omitempty"`

	// HistoryRetentionDays mmr_history / 点数授予 / 技能卡发放幂等表留存天数
	// (默认 90,下限 30,上限 90:必须远大于 kafka retention 与一切授予补扫窗口)。
	// 同 ExpHistoryRetention:report_only 与 delete 共用这一个值。
	HistoryRetentionDays int `yaml:"history_retention_days,omitempty" json:"history_retention_days,omitempty"`
}

// Defaults 填默认值。
func (c *Config) Defaults() {
	c.DSAuth.Defaults()
	if c.Player.BaseMMR <= 0 {
		c.Player.BaseMMR = 1500
	}
	if c.Player.MMRFloor < 0 {
		c.Player.MMRFloor = 0
	}
	if c.Player.DefaultNicknamePrefix == "" {
		c.Player.DefaultNicknamePrefix = "Player_"
	}
	if c.Player.MaxNicknameLen <= 0 {
		c.Player.MaxNicknameLen = 32
	}
	if c.Player.PlayerNameResolveAuthSecret != "" && c.Player.PlayerNameResolveAuthAudience == "" {
		c.Player.PlayerNameResolveAuthAudience = "player:name"
	}
	if c.Player.FriendPlayerNameResolveAuthSecret != "" && c.Player.FriendPlayerNameResolveAuthAudience == "" {
		c.Player.FriendPlayerNameResolveAuthAudience = "player:name"
	}
	if c.Player.GuildPlayerNameResolveAuthSecret != "" && c.Player.GuildPlayerNameResolveAuthAudience == "" {
		c.Player.GuildPlayerNameResolveAuthAudience = "player:name"
	}
	if len(c.Player.ConsumeTopics) == 0 {
		c.Player.ConsumeTopics = []string{kafkax.TopicPlayerUpdate}
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":20002"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":21002"
	}
}

// ValidatePlayerNameResolver 防止内部名称解析只配半套凭据后静默拒绝申请展示。
// team/friend/guild 各持独立 HMAC key；旧 team 字段保留以支持滚动升级。
func (c *Config) ValidatePlayerNameResolver() error {
	type callerCredential struct {
		caller   string
		field    string
		secret   string
		audience string
	}
	credentials := []callerCredential{
		{"team", "player_name_resolve_auth", c.Player.PlayerNameResolveAuthSecret, c.Player.PlayerNameResolveAuthAudience},
		{"friend", "friend_player_name_resolve_auth", c.Player.FriendPlayerNameResolveAuthSecret, c.Player.FriendPlayerNameResolveAuthAudience},
		{"guild", "guild_player_name_resolve_auth", c.Player.GuildPlayerNameResolveAuthSecret, c.Player.GuildPlayerNameResolveAuthAudience},
	}
	seenSecrets := make(map[string]string, len(credentials))
	hasVerifier := false
	for _, credential := range credentials {
		if credential.secret == "" {
			if credential.audience != "" {
				return fmt.Errorf("player.%s_audience requires %s_secret", credential.field, credential.field)
			}
			continue
		}
		hasVerifier = true
		if err := internalrpcauth.ValidateSecret(credential.secret); err != nil {
			return fmt.Errorf("player.%s_secret: %w", credential.field, err)
		}
		if err := internalrpcauth.ValidateIdentity(credential.audience); err != nil {
			return fmt.Errorf("player.%s_audience: %w", credential.field, err)
		}
		if priorCaller, reused := seenSecrets[credential.secret]; reused {
			return fmt.Errorf("player %s player-name HMAC key must differ from %s caller key",
				credential.caller, priorCaller)
		}
		seenSecrets[credential.secret] = credential.caller
	}
	if hasVerifier && c.Node.RedisClient.Host == "" && len(c.Node.RedisClient.Addrs) == 0 {
		return fmt.Errorf("player.player_name_resolve_auth_secret requires node.redis_client replay authority")
	}
	return nil
}

// MaxExpPerGrantOrDefault 返回生效的单次入账上限(未配置 → 1000000)。
func (p *PlayerConf) MaxExpPerGrantOrDefault() uint64 {
	if p.MaxExpPerGrant > 0 {
		return p.MaxExpPerGrant
	}
	return 1_000_000
}

// 推送出箱发布器写者租约档位(与 mission.conf 同名常量语义一致)。
const (
	// PushWriterLeaseOff 不选举:本副本无条件跑发布器(只在能保证单发布者时合法)。
	PushWriterLeaseOff = "off"
	// PushWriterLeaseEnforce 只有当选副本跑发布器,其余副本热备。
	PushWriterLeaseEnforce = "enforce"
)

// PushWriterLeaseConf 是 player_push_outbox 发布器的单写者选举配置。
//
// 为什么 player 也必须单写者(与 mission 同因):player_push_outbox 是**全局未分区**表,
// 发布器按 id 升序整表 FIFO 取行(experience_repo.go FetchPushOutbox),属 §9.21
// 「作用于同一未分区权威的单写者循环」。而 PlayerExperienceEvent 携带的是**绝对值快照**
// (level / exp_in_level / is_max_level,不是增量),两个副本交错投递会让旧快照后到并
// 覆盖新的 —— 玩家看到等级/经验条**倒退**。事件里没有 revision;ts_ms 是各副本墙钟,
// protocol-ordering-rules §5-B 明令不得只靠它判重。
//
// 与 battle_result 的 player_update_outbox 的**有据差异**:那条链下游是 player 服务、
// 靠 mmr_history 唯一键幂等去重,乱序与重复天然无害,且没有客户端可见的覆盖语义,
// 故**刻意不接选举**(§15.3 不为形式统一付复杂度)。三条同类链路之间是有据的差异,
// 不是漂移。
type PushWriterLeaseConf struct {
	// Mode:"off"(留空即 off)| "enforce"。默认 off 保证 dev 单进程无 etcd 也起得来;
	// 生产的安全网是 main.go 的机械门禁(RollingUpdate × 非 enforce → fail-closed 退出)。
	Mode string `yaml:"mode,omitempty" json:"mode,omitempty"`
	// EtcdEndpoints etcd 地址(mode=enforce 必填)。
	EtcdEndpoints []string `yaml:"etcd_endpoints,omitempty" json:"etcd_endpoints,omitempty"`
	// LeaseTTLSec etcd session lease TTL(秒);留空用 writerlease 默认值。
	LeaseTTLSec int `yaml:"lease_ttl_sec,omitempty" json:"lease_ttl_sec,omitempty"`
	// DialTimeout etcd 连接超时;留空用 writerlease 默认值。
	DialTimeout config.Duration `yaml:"dial_timeout,omitempty" json:"dial_timeout,omitempty"`
}

// ResolveMode 归一化档位,取值不认识**报错而非猜**(拼错一个字母就静默退回无保护并发
// 发布是不可接受的失败模式,同 dbguard.ParseMode 的纪律)。
func (c PushWriterLeaseConf) ResolveMode() (string, error) {
	switch strings.ToLower(strings.TrimSpace(c.Mode)) {
	case "", PushWriterLeaseOff:
		return PushWriterLeaseOff, nil
	case PushWriterLeaseEnforce:
		return PushWriterLeaseEnforce, nil
	default:
		return "", fmt.Errorf("player.push_writer_lease.mode=%q 不认识(只允许 %q / %q,留空=%q)",
			c.Mode, PushWriterLeaseOff, PushWriterLeaseEnforce, PushWriterLeaseOff)
	}
}

// PushOutboxIntervalOrDefault 返回生效的推送出箱轮询间隔(未配置 → 1s)。
func (p *PlayerConf) PushOutboxIntervalOrDefault() time.Duration {
	if d := p.PushOutboxInterval.Std(); d > 0 {
		return d
	}
	return time.Second
}

// PushOutboxBatchOrDefault 返回生效的推送出箱批大小(未配置 → 128)。
func (p *PlayerConf) PushOutboxBatchOrDefault() int {
	if p.PushOutboxBatch > 0 {
		return p.PushOutboxBatch
	}
	return 128
}

// ExpHistoryRetentionOrDefault 返回生效的 exp_history 留存期(未配置 → 7 天;
// 配置低于 7 天按 7 天,防手滑把幂等窗清穿)。
func (p *PlayerConf) ExpHistoryRetentionOrDefault() time.Duration {
	const (
		min = 7 * 24 * time.Hour
		cap = 90 * 24 * time.Hour // §9.24 硬上限:失效数据最多保留 90 天(审计 P1:上限必须钳制,不能只信配置)
	)
	d := p.ExpHistoryRetention.Std()
	if d < min {
		return min
	}
	if d > cap {
		return cap
	}
	return d
}

// HistoryRetentionOrDefault 返回生效的 mmr/点数授予幂等历史留存期(未配置 → 90 天;
// 低于 30 天按 30 天,防手滑把幂等窗清穿;高于 90 天按 90 天,§9.24 硬上限)。
// 先钳**天数整数**再乘 Duration(审计 P1:先乘后判时,极大天数乘 24h 溢出为负,
// 会误落 floor 分支返回 30 天,清理开启时提前删幂等收据)。
func (p *PlayerConf) HistoryRetentionOrDefault() time.Duration {
	days := p.HistoryRetentionDays
	if days <= 0 {
		days = 90
	}
	if days < 30 {
		days = 30
	}
	if days > 90 {
		days = 90
	}
	return time.Duration(days) * 24 * time.Hour
}

// RetentionMode 返回本服生效的保留期清理模式(§9.24 全局默认:留空 = report_only,只报告不删)。
//
// 无法识别的值回落 ModeReportOnly(不删更安全),但启动期 ValidateRetentionMode 已经
// fail-fast 拒启,正常路径到不了这里。
func (p *PlayerConf) RetentionMode() dbguard.Mode {
	mode, err := dbguard.ParseMode(p.RetentionModeRaw)
	if err != nil {
		return dbguard.ModeReportOnly
	}
	return mode
}

// ExpHistoryRetentionMode 返回 exp_history 这一组生效的模式:总闸(retention_mode)与本组
// 前置条件确认(exp_history_cleanup_enabled)**都开**才删,任一没开都只报告。
func (p *PlayerConf) ExpHistoryRetentionMode() dbguard.Mode {
	return gateDelete(p.RetentionMode(), p.ExpHistoryCleanupEnabled)
}

// HistoryRetentionMode 返回 mmr_history / attr_point_grants / talent_point_grants /
// skill_card_grants 这一组生效的模式(同上,两道闸都开才删)。
func (p *PlayerConf) HistoryRetentionMode() dbguard.Mode {
	return gateDelete(p.RetentionMode(), p.HistoryCleanupEnabled)
}

// gateDelete 把"前置条件没确认"表达成降级到 report_only,而不是干脆不跑 janitor ——
// 不跑就等于既不删也不报,§9.24 要的待清理量(WARN + pending gauge)会整个消失,
// 库在无人知晓的情况下继续涨。
func gateDelete(mode dbguard.Mode, precondition bool) dbguard.Mode {
	if !precondition {
		return dbguard.ModeReportOnly
	}
	return mode
}

// ValidateRetentionMode 供启动 fail-fast(写了无法识别的模式必须拒启)。
// 只回落默认是不够的:运维以为配了 delete、实际一行没删,库继续增长且启动期毫无痕迹。
func (p *PlayerConf) ValidateRetentionMode() error {
	_, err := dbguard.ParseMode(p.RetentionModeRaw)
	return err
}
