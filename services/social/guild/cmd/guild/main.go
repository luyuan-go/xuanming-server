// Pandora guild 服务入口(2026-06-27)。
//
// 职责:公会(GuildService)+ 临时群(GroupService)同进程;成员关系落 pandora_social(MySQL 强依赖); 公会成员变更经 kafka pandora.guild.event → push 推送给接收方(弱依赖);
// 临时群 MVP 不单独推送成员变更(客户端拉 ListMyGroups 兜底)。
//
// 启动顺序(对齐 friend / team):
//  1. 解析 -conf 路径,加载 yaml
//  2. conf.Defaults 填默认值
//  3. log.Setup → 全局 zap logger
//  4. MySQL client + Ping(强依赖:公会 / 群关系落库不可降级)
//  5. pandora_social schema gate(计数列 / 计数表缺失时在接流量前失败)
//  6. Snowflake Node(guild_id / group_id / request_id 生成,node_id 来自 yaml)
//  7. Redis client + Ping(弱依赖:公会资料读缓存 cache-aside,失败降级直连 MySQL)
//  8. kafka producer(topic=pandora.guild.event)→ guildEventPusher(弱依赖)
//  9. 装配 GuildUsecase + GroupUsecase → GuildService + GroupService → gRPC/HTTP server
//  10. kratos.New(...).Run() 阻塞
package main

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-kratos/kratos/v2"
	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	"github.com/luyuancpp/pandora/pkg/kafkax"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/middleware"
	"github.com/luyuancpp/pandora/pkg/mysqlx"
	"github.com/luyuancpp/pandora/pkg/playerdisplay"
	"github.com/luyuancpp/pandora/pkg/redisx"
	"github.com/luyuancpp/pandora/pkg/safego"
	"github.com/luyuancpp/pandora/pkg/sessiongate"
	"github.com/luyuancpp/pandora/pkg/snowflake/etcdnode"
	guildv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/guild/v1"
	"github.com/redis/go-redis/v9"

	"github.com/luyuancpp/pandora/services/social/guild/internal/biz"
	"github.com/luyuancpp/pandora/services/social/guild/internal/conf"
	"github.com/luyuancpp/pandora/services/social/guild/internal/data"
	"github.com/luyuancpp/pandora/services/social/guild/internal/server"
	"github.com/luyuancpp/pandora/services/social/guild/internal/service"
)

const serviceName = "guild"

var flagConf string

func init() {
	flag.StringVar(&flagConf, "conf", "etc/guild-dev.yaml", "config file path")
}

func main() {
	flag.Parse()

	// 1. Logger
	logger := plog.Setup(serviceName)
	helper := plog.NewHelper(logger)
	helper.Infow("msg", "service_starting", "conf", flagConf)

	// 2. 加载 yaml
	cfgPath, err := filepath.Abs(flagConf)
	if err != nil {
		helper.Errorw("msg", "abs_conf_path_failed", "err", err)
		os.Exit(1)
	}
	c := kconfig.New(kconfig.WithSource(file.NewSource(cfgPath)))
	defer func() { _ = c.Close() }()

	if err := c.Load(); err != nil {
		helper.Errorw("msg", "config_load_failed", "err", err, "path", cfgPath)
		os.Exit(1)
	}

	var cfg conf.Config
	if err := c.Scan(&cfg); err != nil {
		helper.Errorw("msg", "config_scan_failed", "err", err)
		os.Exit(1)
	}
	cfg.Defaults()
	if err := cfg.ValidatePlayerDisplayResolvers(); err != nil {
		helper.Errorw("msg", "player_display_resolver_config_invalid", "err", err)
		os.Exit(1)
	}
	// 保留期清理模式必须能被识别(§9.24 fail-fast):拼错的值会静默回落 report_only,
	// 运维以为开了清理、实际一行没删,库继续无界增长且启动期毫无痕迹。
	if err := cfg.Guild.ValidateRetentionMode(); err != nil {
		helper.Errorw("msg", "guild_retention_mode_invalid", "err", err,
			"hint", "guild.retention_mode 只接受 \"report_only\"(默认,不删) 或 \"delete\"")
		os.Exit(1)
	}

	// 3. MySQL(强依赖:公会 / 群关系落库不可降级)
	if cfg.Node.MySQLClient.DSN == "" {
		helper.Errorw("msg", "mysql_dsn_required", "hint", "node.mysql_client.dsn required (pandora_social)")
		os.Exit(1)
	}
	db := mysqlx.MustNewClient(cfg.Node.MySQLClient)
	defer func() { _ = db.Close() }()

	// 严格模式断言(§9.24):非严格 sql_mode 下超长写入会被 MySQL **静默截断**
	// (err=nil 但数据被砍断),等于无声的数据损坏,故 fail-fast 而不是继续产生坏数据。
	if serr := dbguard.AssertStrictModeStartup(db); serr != nil {
		helper.Errorw("msg", "mysql_strict_mode_required", "err", serr)
		os.Exit(1)
	}

	schemaCtx, schemaCancel := context.WithTimeout(context.Background(), 5*time.Second)
	schemaErr := data.ValidateRequiredSchema(schemaCtx, db)
	schemaCancel()
	if schemaErr != nil {
		helper.Errorw(
			"msg", "guild_schema_incompatible",
			"err", schemaErr,
			"required_migration_version", data.RequiredSchemaVersion,
			"hint", "先运行 tools/migrate 将 pandora_social 升至 version 2，再滚动 guild",
		)
		os.Exit(1)
	}
	helper.Infow("msg", "mysql_connected", "dsn", maskDSN(cfg.Node.MySQLClient.DSN))

	// 容量巡检(§9.24):启动即跑一轮拿基线,之后每小时一轮;超预算只告警不阻断。
	capacityCtx, capacityCancel := context.WithCancel(context.Background())
	defer capacityCancel()
	go runCapacityGuard(capacityCtx, dbguard.New(db, "pandora_social", data.Budgets(), nil))

	// 4. Snowflake(guild_id / group_id / request_id 生成；node_id_source=static 静态，=etcd 走 etcd 自动抢占，失租自动退出)
	//
	// guild_id / request_id / group_id 是三个独立 ID 空间，各取一个发号器
	// (共用同一 nodeID / lease)。⚠️ 共用 nodeID ⇒ 三个空间会发出逐位相同的 ID，
	// 禁止跨空间放进同一容器比较(见 etcdnode.ProvideSnowflakeN)。
	sfs, sfCloser := etcdnode.MustProvideSnowflakeN(serviceName, cfg.Node.NodeId, cfg.Snowflake, 3)
	defer func() { _ = sfCloser.Close() }()
	guildSF, requestSF, groupSF := sfs[0], sfs[1], sfs[2]

	// 5. Redis(弱依赖:公会资料读缓存 cache-aside;Ping 失败降级直连 MySQL,cache=nil)。
	//    单实例填 host,Redis Cluster / Sentinel 只填 addrs,两者皆空才算未配置。
	var guildCache data.GuildCache
	var quotaRdb redis.UniversalClient // 申请频率配额与读缓存共用同一 Redis(健康时才启用)
	if rc := cfg.Node.RedisClient; rc.Host != "" || len(rc.Addrs) > 0 {
		rdb := redisx.NewUniversalClient(rc)
		pingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if perr := rdb.Ping(pingCtx).Err(); perr != nil {
			cancel()
			_ = rdb.Close()
			helper.Warnw("msg", "redis_ping_failed", "err", perr, "addr", rc.Host, "addrs", rc.Addrs,
				"hint", "degrade to direct MySQL (no guild cache)")
		} else {
			cancel()
			defer func() { _ = rdb.Close() }()
			guildCache = data.NewRedisGuildCache(rdb)
			quotaRdb = rdb
			helper.Infow("msg", "redis_connected", "addr", rc.Host, "addrs", rc.Addrs, "cache_ttl", cfg.Guild.CacheTTL.String())
		}
	} else {
		helper.Warnw("msg", "redis_endpoint_empty", "hint", "guild cache disabled (direct MySQL)")
	}

	// 6. kafka producer → guildEventPusher(弱依赖:broker 不通则 warn 并继续,推送静默 fail)
	var pusher biz.GuildEventPusher
	if len(cfg.Kafka.Brokers) > 0 {
		producer, perr := kafkax.NewKeyOrderedProducer(cfg.Kafka, kafkax.TopicGuildEvent)
		if perr != nil {
			helper.Warnw("msg", "kafka_producer_init_failed", "err", perr,
				"hint", "guild push silently dropped until kafka is available")
		} else {
			defer func() { _ = producer.Close() }()
			pusher = &guildEventPusher{p: producer}
			helper.Infow("msg", "kafka_producer_ready", "topic", kafkax.TopicGuildEvent)
		}
	} else {
		helper.Warnw("msg", "kafka_brokers_empty", "hint", "guild push disabled")
	}

	// 7. 装配链(公会 + 临时群同进程)
	guildRepo := data.NewMySQLGuildRepo(db)
	groupRepo := data.NewMySQLGroupRepo(db)
	guildUC := biz.NewGuildUsecase(guildRepo, guildCache, pusher, cfg.Guild)
	if cfg.Guild.PlayerNameResolverAddr != "" {
		playerNameSigner, signErr := internalrpcauth.NewSigner(cfg.Guild.PlayerNameResolverAuthSecret,
			"guild", cfg.Guild.PlayerNameResolverAuthAudience)
		if signErr != nil {
			helper.Errorw("msg", "player_display_resolver_config_invalid", "dependency", "player_name", "err", signErr)
			os.Exit(1)
		}
		playerNameResolver := playerdisplay.NewGrpcPlayerNameResolver(cfg.Guild.PlayerNameResolverAddr, playerNameSigner)
		defer func() { _ = playerNameResolver.Close() }()
		guildUC.SetPlayerNameResolver(playerNameResolver)
		helper.Infow("msg", "player_name_resolver_ready", "addr", cfg.Guild.PlayerNameResolverAddr,
			"caller", "guild", "audience", cfg.Guild.PlayerNameResolverAuthAudience)
	} else {
		helper.Warnw("msg", "player_name_resolver_disabled", "hint", "guild join request nickname projection disabled")
	}
	if cfg.Guild.PlayerNoResolverAddr != "" {
		playerNoSigner, signErr := internalrpcauth.NewSigner(cfg.Guild.PlayerNoResolverAuthSecret,
			"guild", cfg.Guild.PlayerNoResolverAuthAudience)
		if signErr != nil {
			helper.Errorw("msg", "player_display_resolver_config_invalid", "dependency", "player_no", "err", signErr)
			os.Exit(1)
		}
		playerNoResolver := playerdisplay.NewGrpcPlayerNoResolver(cfg.Guild.PlayerNoResolverAddr, playerNoSigner)
		defer func() { _ = playerNoResolver.Close() }()
		guildUC.SetPlayerNoResolver(playerNoResolver)
		helper.Infow("msg", "player_no_resolver_ready", "addr", cfg.Guild.PlayerNoResolverAddr,
			"caller", "guild", "audience", cfg.Guild.PlayerNoResolverAuthAudience)
	} else {
		helper.Warnw("msg", "player_no_resolver_disabled", "hint", "guild join request player_no projection disabled")
	}
	// 入会申请频率配额(anti-abuse §6 第 6 项):Redis 健康时启用,否则不限(fail-open 边界)。
	if quotaRdb != nil {
		guildUC.SetRateQuota(&redisx.ActionQuota{
			RDB: quotaRdb, Domain: "guild",
			Limit: int64(cfg.Guild.RateQuotaPerMin), Window: time.Minute,
		})
		helper.Infow("msg", "guild_rate_quota_ready", "per_min", cfg.Guild.RateQuotaPerMin)
	} else {
		helper.Warnw("msg", "guild_rate_quota_disabled", "reason", "redis not available")
	}
	groupUC := biz.NewGroupUsecase(groupRepo, cfg.Guild)
	guildSvc := service.NewGuildService(guildUC, guildSF, requestSF)

	// DS 回调令牌守卫:GetPlayerGuild 经 :8444 进来的调用须带 DS 服务令牌。
	// mode=off(默认)→ dsGuard 为 nil,Check 直接放行,与接线前完全一致。
	dsGuard, derr := middleware.NewDSCallbackGuardFromConf(cfg.DSAuth)
	if derr != nil {
		helper.Errorw("msg", "ds_auth_guard_init_failed", "err", derr)
		os.Exit(1)
	}
	guildSvc.SetDSCallbackGuard(dsGuard)
	if dsGuard != nil {
		helper.Infow("msg", "ds_callback_guard_ready", "mode", dsGuard.Mode().String())
	}
	groupSvc := service.NewGroupService(groupUC, groupSF)

	// 终态入会申请保留期清理:只增终态行 90 天后批删,增长有界(§9.24,biz/sweep.go)。
	sweepCtx, sweepCancel := context.WithCancel(context.Background())
	defer sweepCancel()
	go guildUC.RunRequestSweep(sweepCtx)

	// 会话现行性门(R5 复审 P0-1,INC-20260722-004):客户端面请求 jti 必须是 login
	// 会话权威(pandora:sess,node.redis_client 指向的共享 Redis)当前一代;
	// prod 生成器机械置 session_gate.require=true(漏配端点拒启)。
	sessGate, sgClose := sessiongate.MustBuild(cfg.Node.RedisClient, cfg.SessionGate.Require)
	defer sgClose()

	grpcSrv := server.NewGRPCServer(&cfg, guildSvc, groupSvc, sessGate)
	httpSrv := server.NewHTTPServer(&cfg)

	helper.Infow(
		"msg", "service_ready",
		"grpc", cfg.Server.Grpc.Addr,
		"http", cfg.Server.Http.Addr,
		"kafka_brokers", cfg.Kafka.Brokers,
		"cache_enabled", guildCache != nil,
		"max_guild_members", cfg.Guild.MaxGuildMembers,
		"max_group_members", cfg.Guild.MaxGroupMembers,
	)

	// 8. Kratos App
	app := kratos.New(
		kratos.Name(serviceName),
		kratos.Logger(logger),
		kratos.Server(grpcSrv, httpSrv),
	)
	if err := app.Run(); err != nil {
		helper.Errorw("msg", "app_run_failed", "err", err)
		os.Exit(1)
	}
}

// guildEventPusher 把 biz.GuildEventPusher 接口适配到 kafkax.KeyOrderedProducer。
// kafka key = to_player_id(不变量 §9:同接收方事件保序;push 服务按 key 路由到该玩家 stream)。
type guildEventPusher struct {
	p *kafkax.KeyOrderedProducer
}

func (k *guildEventPusher) PushGuildEvent(ctx context.Context, toPlayerID uint64, evt *guildv1.GuildEvent) error {
	return k.p.Send(ctx, strconv.FormatUint(toPlayerID, 10), evt)
}

// runCapacityGuard 启动即跑一轮容量巡检拿基线,之后每小时一轮(§9.24)。
// 走 information_schema 估算(毫秒级、不锁表);超预算只告警不阻断。
func runCapacityGuard(ctx context.Context, g *dbguard.Guard) {
	safego.Run(ctx, "db_capacity_guard_initial", func() { g.Check(ctx) })
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			safego.Run(ctx, "db_capacity_guard", func() { g.Check(ctx) })
		}
	}
}

// maskDSN 脱敏 DSN 里的密码(对齐 friend / player main.go)。
func maskDSN(dsn string) string {
	at := -1
	colon := -1
	for i := 0; i < len(dsn); i++ {
		if dsn[i] == ':' && colon == -1 {
			colon = i
		}
		if dsn[i] == '@' {
			at = i
			break
		}
	}
	if colon != -1 && at != -1 && at > colon {
		return dsn[:colon+1] + "***" + dsn[at:]
	}
	return dsn
}
