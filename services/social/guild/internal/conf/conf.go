// Package conf 是 guild 服务的私有配置结构(2026-06-27)。
package conf

import (
	"fmt"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
)

// Config 是 guild 服务的完整配置(公会 + 临时群同进程共用)。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	Guild GuildConf `yaml:"guild" json:"guild"`

	// DSAuth DS 回调服务令牌校验(verify-only)。GetPlayerGuild 是 DS 内部东西向反查,
	// systemOnly 只证明「不带玩家 JWT」,证明不了「调用方是 DS」;本配置提供后者。
	// mode 默认 off(不校验),与接线前行为一致。
	//
	// ⚠️ etc/ 模板里**不得**出现 ds_auth 节点:生成器对每个服务做双向断言(节点存在 ⟺ 在权威
	// 清单里),而 guild 不在清单里,加了就是 `[FATAL] guild 的 ds_auth 节点与权威服务清单不一致`,
	// 且只在真实配置生成时才炸。真要启用须三处同批改:模板加节点 +
	// gen_cluster_config.ps1 的 DsSecretServiceNames + online_manifest_contract.ps1 的
	// PandoraDsCallbackHmacServices —— 那等于扩大 DS 回调密钥的分发面,是安全决策。
	// 未启用期间守卫为 nil,Check 直接放行(同 inventory 的做法)。
	DSAuth config.DSAuthConf `yaml:"ds_auth,omitempty" json:"ds_auth,omitempty"`
}

// GuildConf 是 guild 服务私有配置(公会 + 群上限)。
type GuildConf struct {
	// MaxGuildMembers 单公会成员上限(默认 100)。
	MaxGuildMembers int `yaml:"max_guild_members,omitempty" json:"max_guild_members,omitempty"`

	// MaxGroupMembers 单临时群成员上限(默认 50)。
	MaxGroupMembers int `yaml:"max_group_members,omitempty" json:"max_group_members,omitempty"`

	// MaxPendingRequestsPerGuild 单公会挂起(pending)加入申请上限(默认 200,不变量 §9.18)。
	// ApplyJoin 时在 CreateJoinRequest 事务内校验该公会 pending 申请数,超限回 ErrGuildRequestLimit,
	// 防公会申请列表被刷爆(客户端可写入的累积列表须有写入侧总量上限)。
	MaxPendingRequestsPerGuild int `yaml:"max_pending_requests_per_guild,omitempty" json:"max_pending_requests_per_guild,omitempty"`

	// RateQuotaPerMin 入会申请的 per-player 每分钟频率配额(anti-abuse §6 第 6 项;
	// 默认 10;负值 = 关闭)。与 pending 总量闸正交,挡「申满 → 撤 → 再申」写放大循环。
	// 窗口固定 1 分钟。
	RateQuotaPerMin int `yaml:"rate_quota_per_min,omitempty" json:"rate_quota_per_min,omitempty"`

	// MaxGroupsPerPlayer 单玩家可同时加入的临时群数量上限(默认 50,不变量 §9.18)。
	// 建群 / AddMember 时在事务内校验目标玩家所在群数,超限回 ErrGroupJoinLimit,
	// 防「我所在的群」列表无界堆积。
	MaxGroupsPerPlayer int `yaml:"max_groups_per_player,omitempty" json:"max_groups_per_player,omitempty"`

	// MaxNameLen 公会 / 群名最大长度(utf8 rune,默认 24)。
	MaxNameLen int `yaml:"max_name_len,omitempty" json:"max_name_len,omitempty"`

	// CacheTTL 公会读缓存(Redis cache-aside)条目存活时长(默认 60s)。
	// 读 miss 回填按此 TTL;写路径写库后主动删缓存,删失败靠 TTL 兜底(read-cache-strategy.md §3/§4)。
	// Redis 弱依赖:node.redis_client 未配 / Ping 失败则降级直连 MySQL(cache 关闭)。
	CacheTTL config.Duration `yaml:"cache_ttl,omitempty" json:"cache_ttl,omitempty"`

	// PlayerNameResolver* / PlayerNoResolver* 分别是 guild→player 与
	// guild→login 的公开展示投影。两个 authority 使用独立 HMAC key。
	PlayerNameResolverAddr         string `yaml:"player_name_resolver_addr,omitempty" json:"player_name_resolver_addr,omitempty"`
	PlayerNameResolverAuthSecret   string `yaml:"player_name_resolver_auth_secret,omitempty" json:"player_name_resolver_auth_secret,omitempty"`
	PlayerNameResolverAuthAudience string `yaml:"player_name_resolver_auth_audience,omitempty" json:"player_name_resolver_auth_audience,omitempty"`
	PlayerNoResolverAddr           string `yaml:"player_no_resolver_addr,omitempty" json:"player_no_resolver_addr,omitempty"`
	PlayerNoResolverAuthSecret     string `yaml:"player_no_resolver_auth_secret,omitempty" json:"player_no_resolver_auth_secret,omitempty"`
	PlayerNoResolverAuthAudience   string `yaml:"player_no_resolver_auth_audience,omitempty" json:"player_no_resolver_auth_audience,omitempty"`

	// ── 保留期清理(CLAUDE.md §9 不变量 24:只增表必须有界)──

	// RequestRetentionDays 终态入会申请(approved/rejected)保留天数(默认 90)。
	// guild_join_requests 每对 (guild,player) 至多一行,终态行随申请对数累积;
	// 删后再次申请 = 重新 INSERT pending,行为等价(成员权威在 guild_members)。pending 永不清。
	RequestRetentionDays int `yaml:"request_retention_days,omitempty" json:"request_retention_days,omitempty"`

	// SweepInterval 保留期清理轮询间隔(默认 5m)。多副本各自跑,DELETE 幂等无需锁。
	SweepInterval config.Duration `yaml:"sweep_interval,omitempty" json:"sweep_interval,omitempty"`

	// SweepBatch 每轮清理行数上限(默认 500)。
	SweepBatch int `yaml:"sweep_batch,omitempty" json:"sweep_batch,omitempty"`

	// RetentionModeRaw 保留期清理模式:留空 / "report_only" = 默认只报告不删;"delete" = 真删。
	// 「因为数据大了就自动删」不可接受(§9.24 + 2026-07-22 用户指令);业务性删除(解散公会等)不受此约束。
	RetentionModeRaw string `yaml:"retention_mode,omitempty" json:"retention_mode,omitempty"`
}

// Defaults 填默认值,防止 yaml 缺字段时零值引发非预期行为。
func (c *Config) Defaults() {
	c.DSAuth.Defaults()
	if c.Guild.MaxGuildMembers <= 0 {
		c.Guild.MaxGuildMembers = 100
	}
	if c.Guild.MaxGroupMembers <= 0 {
		c.Guild.MaxGroupMembers = 50
	}
	if c.Guild.RateQuotaPerMin == 0 {
		c.Guild.RateQuotaPerMin = 10
	}
	if c.Guild.MaxPendingRequestsPerGuild <= 0 {
		c.Guild.MaxPendingRequestsPerGuild = 200
	}
	if c.Guild.MaxGroupsPerPlayer <= 0 {
		c.Guild.MaxGroupsPerPlayer = 50
	}
	if c.Guild.MaxNameLen <= 0 {
		c.Guild.MaxNameLen = 24
	}
	if c.Guild.CacheTTL <= 0 {
		c.Guild.CacheTTL = config.Duration(60 * time.Second)
	}
	if c.Guild.RequestRetentionDays <= 0 {
		c.Guild.RequestRetentionDays = 90
	}
	if c.Guild.SweepInterval <= 0 {
		c.Guild.SweepInterval = config.Duration(5 * time.Minute)
	}
	if c.Guild.SweepBatch <= 0 {
		c.Guild.SweepBatch = 500
	}
	if c.Guild.PlayerNameResolverAddr != "" && c.Guild.PlayerNameResolverAuthAudience == "" {
		c.Guild.PlayerNameResolverAuthAudience = "player:name"
	}
	if c.Guild.PlayerNoResolverAddr != "" && c.Guild.PlayerNoResolverAuthAudience == "" {
		c.Guild.PlayerNoResolverAuthAudience = "login:player-no"
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":20008"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":21008"
	}
}

// ValidatePlayerDisplayResolvers 防止弱依赖配置不完整，导致运行时所有申请行静默降级。
func (c *Config) ValidatePlayerDisplayResolvers() error {
	if err := validateResolver("guild.player_name_resolver", c.Guild.PlayerNameResolverAddr,
		c.Guild.PlayerNameResolverAuthSecret, c.Guild.PlayerNameResolverAuthAudience); err != nil {
		return err
	}
	if err := validateResolver("guild.player_no_resolver", c.Guild.PlayerNoResolverAddr,
		c.Guild.PlayerNoResolverAuthSecret, c.Guild.PlayerNoResolverAuthAudience); err != nil {
		return err
	}
	if c.Guild.PlayerNameResolverAddr != "" && c.Guild.PlayerNoResolverAddr != "" &&
		c.Guild.PlayerNameResolverAuthSecret == c.Guild.PlayerNoResolverAuthSecret {
		return fmt.Errorf("guild.player_name_resolver_auth_secret must differ from guild.player_no_resolver_auth_secret")
	}
	return nil
}

func validateResolver(prefix, addr, secret, audience string) error {
	if addr == "" {
		if secret != "" {
			return fmt.Errorf("%s_auth_secret requires %s_addr", prefix, prefix)
		}
		if audience != "" {
			return fmt.Errorf("%s_auth_audience requires %s_addr", prefix, prefix)
		}
		return nil
	}
	if err := internalrpcauth.ValidateSecret(secret); err != nil {
		return fmt.Errorf("%s_auth_secret: %w", prefix, err)
	}
	if err := internalrpcauth.ValidateIdentity(audience); err != nil {
		return fmt.Errorf("%s_auth_audience: %w", prefix, err)
	}
	return nil
}

// RetentionMode 返回生效的保留期清理模式(默认 ModeReportOnly = 只报告不删)。
func (c *GuildConf) RetentionMode() dbguard.Mode {
	m, err := dbguard.ParseMode(c.RetentionModeRaw)
	if err != nil {
		return dbguard.ModeReportOnly
	}
	return m
}

// ValidateRetentionMode 供 main 启动 fail-fast(写了无法识别的模式必须拒启)。
func (c *GuildConf) ValidateRetentionMode() error {
	_, err := dbguard.ParseMode(c.RetentionModeRaw)
	return err
}
