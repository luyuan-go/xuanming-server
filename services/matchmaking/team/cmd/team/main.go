// Pandora team 服务入口(W3 ⑦ Phase 4,2026-06-05)。
//
// 启动顺序:
//  1. 解析 -conf 路径,加载 yaml
//  2. conf.Defaults 填默认值
//  3. log.Setup → 全局 zap logger
//  4. Redis client 连通性 Ping(强依赖)
//  5. Snowflake Node(node_id 来自 yaml)
//  6. kafkax.KeyOrderedProducer(topic=pandora.team.update) → kafkaPusher
//  7. 装配链:RedisTeamRepo → TeamUsecase → TeamService → gRPC/HTTP server
//  8. kratos.New(...).Run() 阻塞
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2"
	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"

	"github.com/luyuancpp/pandora/pkg/cellroute/etcdtable"
	pconfig "github.com/luyuancpp/pandora/pkg/config"
	plog "github.com/luyuancpp/pandora/pkg/log"

	"github.com/luyuancpp/pandora/pkg/grpcclient"
	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	"github.com/luyuancpp/pandora/pkg/kafkax"
	"github.com/luyuancpp/pandora/pkg/middleware"
	"github.com/luyuancpp/pandora/pkg/offlinewatch"
	"github.com/luyuancpp/pandora/pkg/playername"
	"github.com/luyuancpp/pandora/pkg/playerno"
	"github.com/luyuancpp/pandora/pkg/redisx"
	"github.com/luyuancpp/pandora/pkg/sessiongate"
	"github.com/luyuancpp/pandora/pkg/snowflake/etcdnode"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/biz"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/conf"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/data"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/server"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/service"
)

const serviceName = "team"

var flagConf string

func init() {
	flag.StringVar(&flagConf, "conf", "etc/team-dev.yaml", "config file path")
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

	// join_policy fail-fast:拼错一个字母(如 "aproval")若被猜成 open,会让全服队伍
	// 对任何人敞开——这是静默的权限放大,必须启动就拒,不能等第一个玩家点"申请"才暴露。
	if err := cfg.ValidateJoinPolicy(); err != nil {
		helper.Errorw("msg", "team_join_policy_invalid", "err", err, "value", cfg.Team.JoinPolicy)
		os.Exit(1)
	}

	// 离线自动退队的依赖必须齐:开了却缺 locator / matchmaker 地址,功能会静默不生效
	// (或更糟:缺了对局闸门就有拆掉正在打的队伍的风险)。这类「配了却不生效」的失败
	// 模式必须启动就暴露,不能等第一个玩家掉线才发现。
	if err := cfg.ValidateOfflineLeave(); err != nil {
		helper.Errorw("msg", "team_offline_leave_config_invalid", "err", err)
		os.Exit(1)
	}
	if err := cfg.ValidatePlayerNoResolver(); err != nil {
		helper.Errorw("msg", "player_no_resolver_init_failed", "err", err)
		os.Exit(1)
	}
	if err := cfg.ValidatePlayerNameResolver(); err != nil {
		helper.Errorw("msg", "player_name_resolver_init_failed", "err", err)
		os.Exit(1)
	}

	// 3. Redis(强依赖)
	// 单实例填 host,Redis Cluster / Sentinel 只填 addrs,两者皆空才算未配置。
	rc := cfg.Node.RedisClient
	if rc.Host == "" && len(rc.Addrs) == 0 {
		helper.Errorw("msg", "redis_endpoint_required",
			"hint", "set node.redis_client.host (single) or node.redis_client.addrs (cluster)")
		os.Exit(1)
	}
	rdb := redisx.NewUniversalClient(rc)
	defer func() { _ = rdb.Close() }()

	pingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		cancel()
		helper.Errorw("msg", "redis_ping_failed", "err", err, "addr", rc.Host, "addrs", rc.Addrs)
		os.Exit(1)
	}
	cancel()
	helper.Infow("msg", "redis_connected", "addr", rc.Host, "addrs", rc.Addrs)

	// 4. Snowflake(node_id_source=static 静态，=etcd 走 etcd 自动抢占，失租自动退出)
	//
	// team_id 与 invite_id 是两个独立 ID 空间，各取一个发号器(共用同一 nodeID / lease)。
	// ⚠️ 共用 nodeID ⇒ 两个空间会发出逐位相同的 ID，禁止跨空间放进同一容器比较
	// (见 etcdnode.ProvideSnowflakeN)。
	sfs, sfCloser := etcdnode.MustProvideSnowflakeN(serviceName, cfg.Node.NodeId, cfg.Snowflake, 2)
	defer func() { _ = sfCloser.Close() }()
	teamSF, inviteSF := sfs[0], sfs[1]

	// 5. Kafka producer → kafkaPusher。
	// kafka.brokers 非空表示启用队伍推送；此时 producer 是启动强依赖。初始化失败必须在
	// gRPC server 对外 Ready 前退出，让 Kubernetes 保留旧 Pod 并重试新 Pod，不能再以
	// pusher=nil 接受 Invite 后静默丢掉被邀请方唯一能看到的通知。
	var pusher biz.TeamEventPusher
	publication, err := initializeTeamPublication(cfg.Kafka, func(kcfg pconfig.KafkaConfig, topic string) (rawTeamProducer, error) {
		return kafkax.NewKeyOrderedProducer(kcfg, topic)
	})
	if err != nil {
		helper.Errorw("msg", "kafka_producer_required_but_unavailable", "err", err,
			"hint", "team service exits before Ready so the orchestrator can retry after Kafka recovers")
		os.Exit(1)
	}
	if publication.producer != nil {
		defer func() { _ = publication.producer.Close() }()
		pusher = publication.pusher
		helper.Infow("msg", "kafka_producer_ready", "topic", kafkax.TopicTeamUpdate, "required", true)
	} else {
		helper.Warnw("msg", "kafka_producer_disabled_dev_only", "reason", publication.disabledReason,
			"hint", "Invite only stores tokens in this explicit no-Kafka development mode; configure kafka.brokers for player-visible invitations")
	}

	// 6. 装配链
	repo := data.NewRedisTeamRepo(rdb)
	uc := biz.NewTeamUsecase(repo, pusher, cfg.Team)
	// 申请/邀请频率配额(anti-abuse §6 第 6 项):复用共享 rdb,窗口固定 1 分钟。
	uc.SetRateQuota(&redisx.ActionQuota{
		RDB: rdb, Domain: "team",
		Limit: int64(cfg.Team.RateQuotaPerMin), Window: time.Minute,
	})
	helper.Infow("msg", "team_rate_quota_ready", "per_min", cfg.Team.RateQuotaPerMin)
	if cfg.Team.PlayerNoResolverAddr != "" {
		playerNoSigner, signErr := internalrpcauth.NewSigner(cfg.Team.PlayerNoResolverAuthSecret,
			"team", cfg.Team.PlayerNoResolverAuthAudience)
		if signErr != nil {
			helper.Errorw("msg", "player_no_resolver_init_failed", "err", signErr)
			os.Exit(1)
		}
		playerNoResolver := data.NewGrpcPlayerNoResolver(cfg.Team.PlayerNoResolverAddr, playerNoSigner)
		defer func() { _ = playerNoResolver.Close() }()
		uc.SetPlayerNoResolver(playerNoResolver)
		helper.Infow("msg", "player_no_resolver_ready", "addr", cfg.Team.PlayerNoResolverAddr,
			"timeout", "250ms", "max_batch", playerno.ResolveBatchLimit)
	} else {
		helper.Warnw("msg", "player_no_resolver_disabled",
			"hint", "TeamMember.player_no will remain 0 until team.player_no_resolver_addr is configured")
	}
	if cfg.Team.PlayerNameResolverAddr != "" {
		playerNameSigner, signErr := internalrpcauth.NewSigner(cfg.Team.PlayerNameResolverAuthSecret,
			"team", cfg.Team.PlayerNameResolverAuthAudience)
		if signErr != nil {
			helper.Errorw("msg", "player_name_resolver_init_failed", "err", signErr)
			os.Exit(1)
		}
		playerNameResolver := data.NewGrpcPlayerNameResolver(cfg.Team.PlayerNameResolverAddr, playerNameSigner)
		defer func() { _ = playerNameResolver.Close() }()
		uc.SetPlayerNameResolver(playerNameResolver)
		helper.Infow("msg", "player_name_resolver_ready", "addr", cfg.Team.PlayerNameResolverAddr,
			"timeout", "250ms", "max_batch", playername.ResolveBatchLimit)
	} else {
		helper.Warnw("msg", "player_name_resolver_disabled",
			"hint", "TeamMember.nickname will remain empty until team.player_name_resolver_addr is configured")
	}
	// matchmaker 联动(弱依赖:matchmaker_addr 留空 → 离队/踢人不撤匹配票据,
	// 且入队闸门跳过 —— 没有匹配链路的部署本就不存在"被对局占住的队伍")
	if cfg.Team.MatchmakerAddr != "" {
		// ResolvePlayerMatchContext 在 matchmaker 侧强制验签(caller="team",独立密钥)。
		// 密钥缺失不阻断启动:CancelMatch 那半边不需要签名,仍然可用;但入队闸门会
		// fail-closed、招募列表会恒空,所以必须打 WARN 而不是静默降级。
		var matchSigner *internalrpcauth.Signer
		if cfg.Team.MatchResumeAuthSecret != "" {
			s, signErr := internalrpcauth.NewSigner(cfg.Team.MatchResumeAuthSecret,
				"team", cfg.Team.MatchResumeAuthAudience)
			if signErr != nil {
				helper.Errorw("msg", "match_resume_signer_init_failed", "err", signErr)
				os.Exit(1)
			}
			matchSigner = s
		} else {
			helper.Warnw("msg", "match_resume_signer_missing",
				"hint", "team.match_resume_auth_secret is empty: join gate will reject with ERR_UNAVAILABLE and ListOpenTeams will return empty")
		}
		matchCli := data.NewGrpcMatchClient(cfg.Team.MatchmakerAddr, matchSigner)
		defer func() { _ = matchCli.Close() }()
		uc.SetMatchCanceler(matchCli)
		uc.SetMatchCommitmentReader(matchCli)
		helper.Infow("msg", "match_client_ready", "matchmaker_addr", cfg.Team.MatchmakerAddr,
			"resume_auth_signed", matchSigner != nil)
	} else {
		helper.Warnw("msg", "matchmaker_addr_empty",
			"hint", "leave/kick will not cancel matchmaking tickets; team join match-gate disabled")
	}

	// 6.1 离线成员自动退队(默认关,见 conf.OfflineLeaveConf)。
	//
	// 装配三件:locator 只读客户端(判定依据)、offlinewatch 复查骨架(排期 + 到期回查)、
	// kafka 消费者(离场事件 → 排期)。三者任一装不起来都 fail-fast:半截接线会让功能
	// 看起来在跑却永不触发,排查成本远高于起不来。
	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()
	if cfg.Team.OfflineLeave.Enabled {
		locatorConn := grpcclient.MustDialInsecure(cfg.Team.LocatorAddr)
		defer func() { _ = locatorConn.Close() }()

		watcher, werr := offlinewatch.New(rdb, offlinewatch.NewGrpcPresenceReader(locatorConn), uc,
			offlinewatch.Options{
				Namespace:    serviceName,
				Threshold:    cfg.Team.OfflineLeave.Threshold.Std(),
				Interval:     cfg.Team.OfflineLeave.CheckInterval.Std(),
				Budget:       cfg.Team.OfflineLeave.Budget,
				RetryBackoff: cfg.Team.OfflineLeave.CheckInterval.Std(),
			})
		if werr != nil {
			helper.Errorw("msg", "offline_watch_init_failed", "err", werr)
			os.Exit(1)
		}
		// 读路径兜底(GetMyTeam 顺手复查),与下面的事件链是两条独立触发源:
		// 事件丢了靠它补,没人看队伍时靠事件。少接一条就会有清不掉的残留。
		uc.SetPresenceInspector(watcher)
		// 兜底候选源:整支队伍一起掉线时,事件链(Hub 崩溃没有 Logout)与读路径
		// (没人打开面板)会同时失效,只有主动提名能发现残留(见 biz.teamRosterSource)。
		watcher.SetRosterSource(biz.NewTeamRosterSource(repo))

		if len(cfg.Kafka.Brokers) == 0 {
			// 允许:没有 kafka 时退化成「纯兜底」——玩家打开面板才复查。
			// 这不是半成品,是明确的降级档;但必须让运维看见,否则会误以为有秒级时效。
			helper.Warnw("msg", "offline_leave_without_kafka",
				"hint", "no kafka.brokers: offline members are only reaped when someone reads the team; configure kafka + locator.departure_event.enabled for timely removal")
		} else {
			consumer, cerr := watcher.NewConsumer(cfg.Kafka, cfg.Team.OfflineLeave.KafkaPartitions)
			if cerr != nil {
				helper.Errorw("msg", "offline_watch_consumer_init_failed", "err", cerr)
				os.Exit(1)
			}
			consumer.Start()
			defer func() { _ = consumer.Close() }()
		}

		watcher.Start(watchCtx)
		helper.Infow("msg", "offline_leave_enabled",
			"locator_addr", cfg.Team.LocatorAddr,
			"threshold", cfg.Team.OfflineLeave.Threshold.String(),
			"check_interval", cfg.Team.OfflineLeave.CheckInterval.String(),
			"budget", cfg.Team.OfflineLeave.Budget)
	} else {
		// 关闭态也要留启动痕迹(与 offline_leave_without_kafka 同精神):否则线上查
		// 「离线队员为什么不被摘」时,日志里没有任何「本链路未启用」的证据,
		// 排查会在 locator/offlinewatch 侧空转。
		helper.Infow("msg", "offline_leave_disabled",
			"hint", "team.offline_leave.enabled=false: 离线成员不会被自动摘出队伍")
	}

	if closeCell, e := etcdtable.WireRouter(context.Background(), cfg.CellRoute, uc.SetCellRouter); e != nil {
		helper.Errorw("msg", "cellroute_init_failed", "err", e)
		os.Exit(1)
	} else if closeCell != nil {
		defer func() { _ = closeCell() }()
	}
	svc := service.NewTeamService(uc, teamSF, inviteSF)

	// DS 回调令牌守卫:GetPlayerTeam 经 :8444 进来的调用须带 DS 服务令牌。
	// mode=off(默认)→ dsGuard 为 nil,Check 直接放行,与接线前完全一致。
	dsGuard, derr := middleware.NewDSCallbackGuardFromConf(cfg.DSAuth)
	if derr != nil {
		helper.Errorw("msg", "ds_auth_guard_init_failed", "err", derr)
		os.Exit(1)
	}
	svc.SetDSCallbackGuard(dsGuard)

	// matchmaker → team 的组票 / 复位调用验签(A-13)。三档可降级,见 conf.MatchCallAuthSecret:
	// 留空=不验;配了+require=false 观察期(验不过只 WARN);require=true 强制。
	if cfg.Team.MatchCallAuthSecret != "" {
		replay, rerr := internalrpcauth.NewRedisReplayStore(rdb, "pandora:team:match-call:nonce:")
		if rerr != nil {
			helper.Errorw("msg", "match_call_replay_store_init_failed", "err", rerr)
			os.Exit(1)
		}
		v, verr := internalrpcauth.NewVerifier(cfg.Team.MatchCallAuthSecret, "matchmaker",
			cfg.Team.MatchCallAuthAudience, 30*time.Second, replay)
		if verr != nil {
			helper.Errorw("msg", "match_call_verifier_init_failed", "err", verr)
			os.Exit(1)
		}
		svc.SetMatchCallAuth(v, cfg.Team.MatchCallAuthRequire)
		helper.Infow("msg", "match_call_verifier_ready", "require", cfg.Team.MatchCallAuthRequire)
	} else {
		helper.Warnw("msg", "match_call_verifier_disabled",
			"hint", "team.match_call_auth_secret 留空:BeginTeamMatch/EndTeamMatch 只有 systemOnly,集群内任何 Pod 都可调")
	}
	if dsGuard != nil {
		helper.Infow("msg", "ds_callback_guard_ready", "mode", dsGuard.Mode().String())
	}

	// 7. gRPC + HTTP
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
		"redis_addr", rc.Host,
		"invite_ttl", cfg.Team.InviteTTL.String(),
		"max_members", cfg.Team.MaxMembers,
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

// rawTeamProducer 是启动门禁测试可替换的 Kafka 最小能力面。
type rawTeamProducer interface {
	PushToPlayers(context.Context, uint64, []uint64, []byte) (int, error)
	PushToPlayersWithEventType(context.Context, uint64, []uint64, []byte, uint32) (int, error)
	Close() error
}

type teamPublicationInit struct {
	pusher         biz.TeamEventPusher
	producer       rawTeamProducer
	disabledReason string
}

// initializeTeamPublication 集中约束队伍推送的启动语义：
//   - kafka.brokers 显式为空时保留纯 RPC 本地调试模式；
//   - 只要配置了 broker，producer 就是强依赖，初始化失败或返回 nil 都拒绝启动。
//
// 该门禁必须发生在 gRPC server 构造与 app.Run 之前，确保 readiness 永远不会把
// “Invite 返回成功但推送永久关闭”的进程加入 Service Endpoints。
func initializeTeamPublication(
	cfg pconfig.KafkaConfig,
	factory func(pconfig.KafkaConfig, string) (rawTeamProducer, error),
) (teamPublicationInit, error) {
	configured := false
	for _, broker := range cfg.Brokers {
		if strings.TrimSpace(broker) != "" {
			configured = true
			break
		}
	}
	if !configured {
		return teamPublicationInit{disabledReason: "kafka.brokers is empty"}, nil
	}

	producer, err := factory(cfg, kafkax.TopicTeamUpdate)
	if err != nil {
		return teamPublicationInit{}, fmt.Errorf("initialize required %s producer: %w", kafkax.TopicTeamUpdate, err)
	}
	if producer == nil {
		return teamPublicationInit{}, fmt.Errorf("initialize required %s producer: factory returned nil", kafkax.TopicTeamUpdate)
	}
	return teamPublicationInit{
		pusher:   &kafkaPusher{p: producer},
		producer: producer,
	}, nil
}

// kafkaPusher 把 biz.TeamEventPusher 接口适配到 Kafka producer。
type kafkaPusher struct {
	p rawTeamProducer
}

func (k *kafkaPusher) PushTeamUpdate(ctx context.Context, callerPlayerID uint64, toPlayerIDs []uint64, payload []byte) (int, error) {
	return k.p.PushToPlayers(ctx, callerPlayerID, toPlayerIDs, payload)
}

// PushTeamEvent 带 push 域内事件类型判别键(event_type)推送(邀请等专属事件)。
func (k *kafkaPusher) PushTeamEvent(ctx context.Context, callerPlayerID uint64, toPlayerIDs []uint64, payload []byte, eventType uint32) (int, error) {
	// 适配层不解释 payload,只把业务层给出的事件类型原样交给统一 Kafka producer。
	return k.p.PushToPlayersWithEventType(ctx, callerPlayerID, toPlayerIDs, payload, eventType)
}

// kratosHelper 是 *klog.Helper 的简化接口。
type kratosHelper interface {
	Infow(keyvals ...any)
	Warnw(keyvals ...any)
	Errorw(keyvals ...any)
}
