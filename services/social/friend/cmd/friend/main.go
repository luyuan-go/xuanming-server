// Pandora friend 服务入口(2026-06-15)。
//
// 职责:好友请求 / 接受 / 列表 / 拉黑;好友图落 pandora_social(MySQL 强依赖);
// 好友请求 / 接受经 kafka pandora.friend.event → push 推送给接收方(弱依赖);
// ListFriends 经 player_locator 填在线状态(弱依赖)。
//
// 启动顺序(对齐 team / player):
//  1. 解析 -conf 路径,加载 yaml
//  2. conf.Defaults 填默认值
//  3. log.Setup → 全局 zap logger
//  4. MySQL client + Ping(强依赖:好友图落库不可降级)
//  5. Snowflake Node(request_id 生成,node_id 来自 yaml)
//  6. kafka producer(topic=pandora.friend.event)→ friendEventPusher(弱依赖)
//  7. player_locator gRPC client → OnlineStatusReader(弱依赖,addr 空则离线)
//  8. 装配 FriendUsecase → FriendService → gRPC/HTTP server
//  9. kratos.New(...).Run() 阻塞
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

	"github.com/luyuancpp/pandora/pkg/cellroute/etcdtable"
	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/grpcclient"
	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	"github.com/luyuancpp/pandora/pkg/kafkax"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/mysqlx"
	"github.com/luyuancpp/pandora/pkg/playerdisplay"
	"github.com/luyuancpp/pandora/pkg/redisx"
	"github.com/luyuancpp/pandora/pkg/safego"
	"github.com/luyuancpp/pandora/pkg/sessiongate"
	"github.com/luyuancpp/pandora/pkg/snowflake/etcdnode"
	friendv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/friend/v1"

	"github.com/luyuancpp/pandora/services/social/friend/internal/biz"
	"github.com/luyuancpp/pandora/services/social/friend/internal/conf"
	"github.com/luyuancpp/pandora/services/social/friend/internal/data"
	"github.com/luyuancpp/pandora/services/social/friend/internal/server"
	"github.com/luyuancpp/pandora/services/social/friend/internal/service"
)

const serviceName = "friend"

var flagConf string

func init() {
	flag.StringVar(&flagConf, "conf", "etc/friend-dev.yaml", "config file path")
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
	if err := cfg.Friend.ValidateRetentionMode(); err != nil {
		helper.Errorw("msg", "friend_retention_mode_invalid", "err", err,
			"hint", "friend.retention_mode 只接受 \"report_only\"(默认,不删) 或 \"delete\"")
		os.Exit(1)
	}

	// 3. MySQL(强依赖:好友图落库不可降级)
	if cfg.Node.MySQLClient.DSN == "" {
		helper.Errorw("msg", "mysql_dsn_required", "hint", "node.mysql_client.dsn required (pandora_social)")
		os.Exit(1)
	}
	db := mysqlx.MustNewClient(cfg.Node.MySQLClient)
	defer func() { _ = db.Close() }()
	helper.Infow("msg", "mysql_connected", "dsn", maskDSN(cfg.Node.MySQLClient.DSN))

	// 严格模式断言(§9.24):非严格 sql_mode 下超长写入会被 MySQL **静默截断**
	// (err=nil 但数据被砍断),等于无声的数据损坏,故 fail-fast 而不是继续产生坏数据。
	if serr := dbguard.AssertStrictModeStartup(db); serr != nil {
		helper.Errorw("msg", "mysql_strict_mode_required", "err", serr)
		os.Exit(1)
	}

	// 容量巡检(§9.24):启动即跑一轮拿基线(上线时就已超限当场可见),之后每小时一轮。
	// 走 information_schema 估算(毫秒级不锁表);超预算只打 ERROR 日志 + metric,不阻止启动。
	capacityCtx, capacityCancel := context.WithCancel(context.Background())
	defer capacityCancel()
	go runCapacityGuard(capacityCtx, dbguard.New(db, "pandora_social", data.Budgets(), nil))

	// 启动期 schema 检查(R8 收口):守卫行表是后补的(R5 复审 P1-2/3/4),既有库不会
	// 自动重放 init SQL;缺表时 acquirePairGuard/acquirePlayerGuard 首条 INSERT 即炸,
	// 好友操作全量内部错误。fail-fast 并指向迁移 SQL。
	{
		schemaCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if serr := mysqlx.CheckTables(schemaCtx, db,
			"tools/migrate/migrations/pandora_social/000006_friend_guard_tables.up.sql(或重放 deploy/mysql-init/06-social-tables.sql)",
			"friendships", "friend_requests", "blocks",
			"friend_player_guards", "friend_pair_guards"); serr != nil {
			helper.Errorw("msg", "mysql_schema_check_failed", "err", serr)
			os.Exit(1)
		}
	}

	// 4. Snowflake(request_id 生成；node_id_source=static 静态，=etcd 走 etcd 自动抢占，失租自动退出)
	sf, sfCloser := etcdnode.MustProvideSnowflake(serviceName, cfg.Node.NodeId, cfg.Snowflake)
	defer func() { _ = sfCloser.Close() }()

	// 5. kafka producer → friendEventPusher(弱依赖:broker 不通则 warn 并继续,推送静默 fail)
	var pusher biz.FriendEventPusher
	if len(cfg.Kafka.Brokers) > 0 {
		producer, perr := kafkax.NewKeyOrderedProducer(cfg.Kafka, kafkax.TopicFriendEvent)
		if perr != nil {
			helper.Warnw("msg", "kafka_producer_init_failed", "err", perr,
				"hint", "friend push silently dropped until kafka is available")
		} else {
			defer func() { _ = producer.Close() }()
			pusher = &friendEventPusher{p: producer}
			helper.Infow("msg", "kafka_producer_ready", "topic", kafkax.TopicFriendEvent)
		}
	} else {
		helper.Warnw("msg", "kafka_brokers_empty", "hint", "friend push disabled")
	}

	// 6. player_locator gRPC client → OnlineStatusReader(弱依赖:addr 空则在线状态全离线)
	var online data.OnlineStatusReader
	if cfg.Friend.LocatorAddr != "" {
		conn := grpcclient.MustDialInsecure(cfg.Friend.LocatorAddr)
		defer func() { _ = conn.Close() }()
		online = data.NewGrpcOnlineStatusReader(conn)
		helper.Infow("msg", "locator_client_ready", "locator_addr", cfg.Friend.LocatorAddr)
	} else {
		helper.Warnw("msg", "locator_addr_empty", "hint", "friend online status disabled (all offline)")
	}

	// 7. 装配链
	repo := data.NewMySQLFriendRepo(db)
	uc := biz.NewFriendUsecase(repo, pusher, online, cfg.Friend)
	if cfg.Friend.PlayerNameResolverAddr != "" {
		playerNameSigner, signErr := internalrpcauth.NewSigner(cfg.Friend.PlayerNameResolverAuthSecret,
			"friend", cfg.Friend.PlayerNameResolverAuthAudience)
		if signErr != nil {
			helper.Errorw("msg", "player_display_resolver_config_invalid", "dependency", "player_name", "err", signErr)
			os.Exit(1)
		}
		playerNameResolver := playerdisplay.NewGrpcPlayerNameResolver(cfg.Friend.PlayerNameResolverAddr, playerNameSigner)
		defer func() { _ = playerNameResolver.Close() }()
		uc.SetPlayerNameResolver(playerNameResolver)
		helper.Infow("msg", "player_name_resolver_ready", "addr", cfg.Friend.PlayerNameResolverAddr,
			"caller", "friend", "audience", cfg.Friend.PlayerNameResolverAuthAudience)
	} else {
		helper.Warnw("msg", "player_name_resolver_disabled", "hint", "friend request nickname projection disabled")
	}
	if cfg.Friend.PlayerNoResolverAddr != "" {
		playerNoSigner, signErr := internalrpcauth.NewSigner(cfg.Friend.PlayerNoResolverAuthSecret,
			"friend", cfg.Friend.PlayerNoResolverAuthAudience)
		if signErr != nil {
			helper.Errorw("msg", "player_display_resolver_config_invalid", "dependency", "player_no", "err", signErr)
			os.Exit(1)
		}
		playerNoResolver := playerdisplay.NewGrpcPlayerNoResolver(cfg.Friend.PlayerNoResolverAddr, playerNoSigner)
		defer func() { _ = playerNoResolver.Close() }()
		uc.SetPlayerNoResolver(playerNoResolver)
		helper.Infow("msg", "player_no_resolver_ready", "addr", cfg.Friend.PlayerNoResolverAddr,
			"caller", "friend", "audience", cfg.Friend.PlayerNoResolverAuthAudience)
	} else {
		helper.Warnw("msg", "player_no_resolver_disabled", "hint", "friend request player_no projection disabled")
	}
	// 好友申请频率配额(anti-abuse §6 第 6 项):复用 node.redis_client。
	// 未配 Redis(纯 MySQL 骨架联调)不限流,与 chat 的弱依赖边界一致。
	if rc := cfg.Node.RedisClient; rc.Host != "" || len(rc.Addrs) > 0 {
		quotaRdb := redisx.NewUniversalClient(rc)
		defer func() { _ = quotaRdb.Close() }()
		uc.SetRateQuota(&redisx.ActionQuota{
			RDB: quotaRdb, Domain: "friend",
			Limit: int64(cfg.Friend.RateQuotaPerMin), Window: time.Minute,
		})
		helper.Infow("msg", "friend_rate_quota_ready", "per_min", cfg.Friend.RateQuotaPerMin)
	} else {
		helper.Warnw("msg", "friend_rate_quota_disabled", "reason", "redis not configured")
	}
	if closeCell, e := etcdtable.WireRouter(context.Background(), cfg.CellRoute, uc.SetCellRouter); e != nil {
		helper.Errorw("msg", "cellroute_init_failed", "err", e)
		os.Exit(1)
	} else if closeCell != nil {
		defer func() { _ = closeCell() }()
	}
	svc := service.NewFriendService(uc, sf)

	// 终态好友请求保留期清理:只增终态行 90 天后批删,增长有界(§9.24,biz/sweep.go)。
	sweepCtx, sweepCancel := context.WithCancel(context.Background())
	defer sweepCancel()
	go uc.RunRequestSweep(sweepCtx)

	// 会话现行性门(R5 复审 P0-1,INC-20260722-004):客户端面请求 jti 必须是 login
	// 会话权威(pandora:sess,node.redis_client 指向的共享 Redis)当前一代;
	// prod 生成器机械置 session_gate.require=true(漏配端点拒启)。
	sessGate, sgClose := sessiongate.MustBuild(cfg.Node.RedisClient, cfg.SessionGate.Require)
	defer sgClose()

	grpcSrv := server.NewGRPCServer(&cfg, svc, sessGate)
	httpSrv := server.NewHTTPServer(&cfg)

	helper.Infow(
		"msg", "service_ready",
		"grpc", cfg.Server.Grpc.Addr,
		"http", cfg.Server.Http.Addr,
		"kafka_brokers", cfg.Kafka.Brokers,
		"locator_addr", cfg.Friend.LocatorAddr,
		"max_friends", cfg.Friend.MaxFriends,
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

// friendEventPusher 把 biz.FriendEventPusher 接口适配到 kafkax.KeyOrderedProducer。
// kafka key = to_player_id(不变量 §9:同接收方事件保序;push 服务按 key 路由到该玩家 stream)。
type friendEventPusher struct {
	p *kafkax.KeyOrderedProducer
}

func (k *friendEventPusher) PushFriendEvent(ctx context.Context, toPlayerID uint64, evt *friendv1.FriendEvent) error {
	return k.p.Send(ctx, strconv.FormatUint(toPlayerID, 10), evt)
}

// runCapacityGuard 启动即跑一轮容量巡检拿基线,之后每小时一轮(§9.24)。
// 走 information_schema 估算(毫秒级、不锁表、不扫数据),放启动路径安全;
// 绝不用 COUNT(*)(千万行表几十秒,会拖垮滚动更新)。超预算只告警不阻断。
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

// maskDSN 脱敏 DSN 里的密码(对齐 player / battle_result main.go)。
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
