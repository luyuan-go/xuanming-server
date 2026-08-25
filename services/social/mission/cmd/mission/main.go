// Pandora mission 服务入口(2026-08-11)。
//
// 职责(docs/design/mission.md):通用任务域——接取 / 放弃 / 条件事实驱动进度 /
// 完成扇出(发奖或标记可领、自动接后续链)/ 领奖;语义移植自 luyuan/mmorpg C++
// mission/condition/reward 模块。
//
// 启动顺序(骨架对齐 dialogue,存储/门禁接线对齐 friend / inventory):
//  1. 解析 -conf 路径,加载 yaml;Defaults + ValidateRetentionMode + Validate fail-fast
//  2. 配置表 Store fail-closed 加载(强依赖:校验/判定/发奖全读表)+ 跨表校验器
//  3. MySQL(pandora_mission,强依赖)+ 严格模式断言 + schema 检查 + 容量巡检
//  4. kafka producer(pandora.mission.update,弱依赖:未配则推送禁用,出箱堆积可见)
//  5. 发奖下游 granter(inventory / player 强依赖 fail-fast;mail 弱依赖;
//     allow_noop_reward=true 仅 dev 骨架联调)
//  6. 装配 MissionUsecase → MissionService → gRPC/HTTP server(sessiongate 会话门)
//  7. workers:发奖补扫 / 推送出箱发布 / 保留期清理
//  8. kratos.New(...).Run() 阻塞
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2"
	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"

	"github.com/luyuancpp/pandora/pkg/configtable"
	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/dsauthfence/writerlease"
	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/kafkax"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/mysqlx"
	"github.com/luyuancpp/pandora/pkg/safego"
	"github.com/luyuancpp/pandora/pkg/sessiongate"

	"github.com/luyuancpp/pandora/services/social/mission/internal/biz"
	"github.com/luyuancpp/pandora/services/social/mission/internal/conf"
	"github.com/luyuancpp/pandora/services/social/mission/internal/data"
	"github.com/luyuancpp/pandora/services/social/mission/internal/server"
	"github.com/luyuancpp/pandora/services/social/mission/internal/service"
)

const serviceName = "mission"

var flagConf string

func init() {
	flag.StringVar(&flagConf, "conf", "etc/mission-dev.yaml", "config file path")
}

func main() {
	flag.Parse()

	// 1. Logger + 配置
	logger := plog.Setup(serviceName)
	helper := plog.NewHelper(logger)
	helper.Infow("msg", "service_starting", "conf", flagConf)

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
	// 保留期清理模式必须能被识别(§9.24 fail-fast:拼错静默回落 report_only 不可接受)。
	if err := cfg.Mission.ValidateRetentionMode(); err != nil {
		helper.Errorw("msg", "mission_retention_mode_invalid", "err", err,
			"hint", "mission.retention_mode 只接受 \"report_only\"(默认,不删) 或 \"delete\"")
		os.Exit(1)
	}
	if err := cfg.Validate(); err != nil {
		helper.Errorw("msg", "config_validation_failed", "err", err)
		os.Exit(1)
	}

	// 2. 配置表(不变量 §9.15):启动强依赖 fail-closed;跨表校验器在首载与每次热
	// reload 走同一门禁,失败整批不切换保留旧表(对齐 matchmaker)。
	ctStore := configtable.NewStore()
	ctStore.AddValidator(validateMissionTables)
	if res, lerr := ctStore.Load(cfg.ConfigTable.Dir, 0); lerr != nil {
		helper.Errorw("msg", "configtable_load_failed", "dir", cfg.ConfigTable.Dir, "err", lerr)
		os.Exit(1)
	} else {
		for _, w := range res.Warnings {
			helper.Warnw("msg", "configtable_load_warning", "warning", w)
		}
		helper.Infow("msg", "configtable_loaded", "dir", cfg.ConfigTable.Dir, "version", res.Version,
			"missions", ctStore.Tables().Mission.Count(),
			"conditions", ctStore.Tables().Condition.Count(),
			"rewards", ctStore.Tables().Reward.Count())
	}

	// 3. MySQL(强依赖:任务状态唯一权威)
	if cfg.Node.MySQLClient.DSN == "" {
		helper.Errorw("msg", "mysql_dsn_required", "hint", "node.mysql_client.dsn required (pandora_mission)")
		os.Exit(1)
	}
	db := mysqlx.MustNewClient(cfg.Node.MySQLClient)
	defer func() { _ = db.Close() }()

	// 严格模式断言(§9.24):非严格 sql_mode 下超长写入被静默截断 = 无声数据损坏,fail-fast。
	if serr := dbguard.AssertStrictModeStartup(db); serr != nil {
		helper.Errorw("msg", "mysql_strict_mode_required", "err", serr)
		os.Exit(1)
	}

	// 容量巡检(§9.24):启动即跑一轮拿基线,之后每小时一轮;超预算只告警不阻断。
	capacityCtx, capacityCancel := context.WithCancel(context.Background())
	defer capacityCancel()
	go runCapacityGuard(capacityCtx, dbguard.New(db, "pandora_mission", data.Budgets(), data.BigFields()))

	// 启动期 schema 检查:缺表 fail-fast 并指向迁移 SQL(对齐 friend / inventory)。
	{
		schemaCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if serr := mysqlx.CheckTables(schemaCtx, db,
			"tools/migrate/migrations/pandora_mission/000001_baseline.up.sql(或重放 deploy/mysql-init/16-mission-tables.sql)",
			"player_mission_active", "player_mission_done",
			"mission_reward_log", "mission_fact_receipts", "mission_push_outbox",
			"mission_player_guards"); serr != nil {
			helper.Errorw("msg", "mysql_schema_check_failed", "err", serr)
			os.Exit(1)
		}
	}

	// 4. kafka producer → 推送(弱依赖:未配则推送禁用,出箱堆积由 dbcheck outbox 检查揭示)
	var pusher biz.Pusher
	if len(cfg.Kafka.Brokers) > 0 {
		// 惰性 producer(2026-08-24):装配期不拨号,首次投递才连。
		//
		// 旧实现在这里一次性 NewKeyOrderedProducer,而它构造期就拨号 —— 启动时 Kafka 恰好
		// 不可用就再也没有第二次机会:只打一条 Warn、pusher 保持 nil、RunPushPublisher
		// 直接 return **连 goroutine 都不起**,Kafka 后来恢复也不补发,必须重启进程才排空。
		// 而 mission_push_outbox 堆积此前没有任何告警,这一档在生产上完全静默。
		// 改惰性后:投递失败 → 发布器中断本轮 → 下一拍重试 → Kafka 恢复即自动排空。
		producer := kafkax.NewLazyProducer(cfg.Kafka, kafkax.TopicMissionUpdate)
		defer func() { _ = producer.Close() }()
		pusher = &missionUpdatePusher{p: producer}
		helper.Infow("msg", "kafka_producer_ready", "topic", kafkax.TopicMissionUpdate,
			"mode", "lazy",
			"hint", "惰性连接:broker 此刻不可达也不阻断启动,发布器按拍重试直到接通")
	} else {
		helper.Warnw("msg", "kafka_brokers_empty", "hint", "mission push disabled")
	}

	// 5. 发奖下游(inventory / player 强依赖 fail-fast;mail 弱依赖溢出转邮件;
	// allow_noop_reward=true 仅 dev 骨架联调——noop 返回错误让流水滞留 PENDING,
	// 绝不假装发放成功,§8 不假装成功)。
	var items biz.ItemGranter
	var exp biz.ExpGranter
	var mail biz.OverflowMailSender
	if cfg.Mission.InventoryAddr != "" {
		g := data.NewGrpcItemGranter(cfg.Mission.InventoryAddr)
		defer func() { _ = g.Close() }()
		items = g
		helper.Infow("msg", "inventory_granter_ready", "addr", cfg.Mission.InventoryAddr)
	} else {
		items = noopItemGranter{}
		helper.Warnw("msg", "inventory_granter_noop", "hint", "allow_noop_reward=true;道具发放将滞留 PENDING")
	}
	if cfg.Mission.PlayerAddr != "" {
		g := data.NewGrpcExpGranter(cfg.Mission.PlayerAddr)
		defer func() { _ = g.Close() }()
		exp = g
		helper.Infow("msg", "exp_granter_ready", "addr", cfg.Mission.PlayerAddr)
	} else {
		exp = noopExpGranter{}
		helper.Warnw("msg", "exp_granter_noop", "hint", "allow_noop_reward=true;经验发放将滞留 PENDING")
	}
	if cfg.Mission.MailAddr != "" {
		m := data.NewGrpcOverflowMailSender(cfg.Mission.MailAddr)
		defer func() { _ = m.Close() }()
		mail = m
		helper.Infow("msg", "overflow_mail_ready", "addr", cfg.Mission.MailAddr)
	} else {
		helper.Warnw("msg", "overflow_mail_disabled", "hint", "背包满时装备发放失败留补扫(mail_addr 未配)")
	}

	// 6. 装配链
	repo := data.NewMySQLMissionRepo(db)
	// cellroute 刻意不接:见 biz/mission.go 里 MissionUsecase 结构体后的说明
	// (全仓先例是「接了就真读」或「用不上就完全不接」,mail / guild / leaderboard 同)。
	uc := biz.NewMissionUsecase(repo, storeCatalogSource{store: ctStore}, items, exp, mail, pusher, cfg.Mission, logger)
	svc := service.NewMissionService(uc)

	// 会话现行性门(INC-20260722-004:顶号后旧 JWT 不得继续操作;对齐 friend)。
	sessGate, sgClose := sessiongate.MustBuild(cfg.Node.RedisClient, cfg.SessionGate.Require)
	defer sgClose()

	grpcSrv := server.NewGRPCServer(&cfg, svc, sessGate)
	httpSrv := server.NewHTTPServer(&cfg)

	// 6.1 推送发布器的写者继任租约(单写者选举;推导见 conf.PushWriterLeaseConf 注释)。
	//
	// 只包 RunPushPublisher:RunRewardRetry 靠下游三个幂等键、RunRetentionSweep 靠
	// DELETE 幂等,两者多副本并跑本就安全,按 §9.21「可并行 worker 不得为金丝雀强行
	// 全局串行化」不得一起包进来。
	pushLeaseMode, plErr := cfg.Mission.PushWriterLease.ResolveMode()
	if plErr != nil {
		helper.Errorw("msg", "mission_push_writer_lease_mode_invalid", "err", plErr)
		os.Exit(1)
	}
	// 机械门禁(与 ds_allocator / hub_allocator 同款):mode != enforce 时"单发布者"只由
	// 部署形态保证。进程看不到 spec.strategy,故由 Deployment 把策略作为 annotation 注入
	// env,再由 push_writer_lease_manifest_test.go 钉住 annotation 与真实 strategy 一致。
	//   · 受管 k8s 内 + RollingUpdate + 非 enforce → fail-closed 退出(不留无保护并发发布);
	//   · 受管 k8s 内 + env 缺失 → fail-closed 退出(清单回归必须炸,不能靠人看日志);
	//   · 非 k8s(本机裸跑 / dev)+ env 缺失 → 只告警(阻断会把开发环境一起打死)。
	deployStrategy := strings.TrimSpace(os.Getenv("PANDORA_DEPLOY_STRATEGY"))
	inManagedK8s := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST")) != ""
	switch {
	case deployStrategy != "":
		if strings.EqualFold(deployStrategy, "RollingUpdate") && pushLeaseMode != conf.PushWriterLeaseEnforce {
			helper.Errorw("msg", "mission_push_writer_lease_rollingupdate_without_enforce",
				"strategy", deployStrategy, "mode", pushLeaseMode,
				"hint", "RollingUpdate × push_writer_lease.mode!=enforce = 滚动重叠期两个发布器并发,"+
					"同玩家旧进度快照会后到并覆盖新的(progressed 是全量快照,无 revision 可判旧);"+
					"要么把 mission.push_writer_lease.mode 改 enforce,要么把 Deployment 改回单副本 Recreate")
			os.Exit(1)
		}
		helper.Infow("msg", "mission_push_writer_lease_strategy_checked",
			"strategy", deployStrategy, "mode", pushLeaseMode)
	case inManagedK8s:
		helper.Errorw("msg", "mission_push_writer_lease_strategy_annotation_missing", "mode", pushLeaseMode,
			"hint", "受管 k8s 内必须注入 PANDORA_DEPLOY_STRATEGY(取自 Deployment 的 "+
				"pandora.dev/deploy-strategy annotation);缺失则无法机械校验 RollingUpdate×非 enforce 组合,"+
				"fail-closed 退出。见 deploy/k8s/services/services.yaml")
		os.Exit(1)
	default:
		helper.Warnw("msg", "mission_push_writer_lease_strategy_unknown", "mode", pushLeaseMode,
			"hint", "非 k8s 环境(本机裸跑/dev):跳过部署策略机械校验")
	}
	if pushLeaseMode == conf.PushWriterLeaseEnforce {
		if len(cfg.Mission.PushWriterLease.EtcdEndpoints) == 0 {
			helper.Errorw("msg", "mission_push_writer_lease_endpoints_missing",
				"hint", "push_writer_lease.mode=enforce 必须配 etcd_endpoints,否则选举无从谈起")
			os.Exit(1)
		}
		hostname, _ := os.Hostname()
		pushLease, wlErr := writerlease.Start(context.Background(), writerlease.Config{
			Endpoints:   cfg.Mission.PushWriterLease.EtcdEndpoints,
			Election:    "mission/push_publisher",
			Identity:    fmt.Sprintf("%s/%d", hostname, os.Getpid()),
			LeaseTTLSec: cfg.Mission.PushWriterLease.LeaseTTLSec,
			DialTimeout: cfg.Mission.PushWriterLease.DialTimeout.Std(),
			// 无 OnElected:接任不需要推进任何 fence 水位(发布器不携带跨轮次权威意图,
			// 见 biz.PushWriterLease 的边界说明)。
		})
		if wlErr != nil {
			helper.Errorw("msg", "mission_push_writer_lease_start_failed", "err", wlErr)
			os.Exit(1)
		}
		defer func() { _ = pushLease.Close() }()
		uc.SetPushWriterLease(pushLease)
		helper.Infow("msg", "mission_push_writer_lease_started",
			"election", "mission/push_publisher", "mode", pushLeaseMode,
			"hint", "enforce:只有当选副本跑推送发布器,热备副本照常服务 RPC 与补扫/清理")
	} else {
		helper.Warnw("msg", "mission_push_writer_lease_disabled",
			"hint", "mode=off:单发布者只由部署形态保证,只允许单进程 / 单副本 Recreate")
	}

	// 7. workers(随进程退出而停;各自 safego 兜 panic,不新建第二套 timer 状态机)
	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()
	go uc.RunRewardRetry(workerCtx)
	go uc.RunPushPublisher(workerCtx)
	go uc.RunRetentionSweep(workerCtx)

	helper.Infow(
		"msg", "service_ready",
		"grpc", cfg.Server.Grpc.Addr,
		"http", cfg.Server.Http.Addr,
		"configtable_dir", cfg.ConfigTable.Dir,
		"max_active_missions", cfg.Mission.MaxActiveMissions,
		"inventory_addr", cfg.Mission.InventoryAddr,
		"player_addr", cfg.Mission.PlayerAddr,
		"mail_addr", cfg.Mission.MailAddr,
	)

	// 8. Kratos App(SIGTERM 先摘流量→排空→退出,§9.16)
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

// missionUpdatePusher 把 biz.Pusher 适配到 kafkax.KeyOrderedProducer。
// kafka key = player_id(不变量 §9:同玩家事件保序;push 服务按 key 路由到该玩家 stream);
// payload 是事务出箱里已序列化的 MissionUpdateEvent,直接 SendRaw 透传。
type missionUpdatePusher struct {
	p *kafkax.LazyProducer
}

func (k *missionUpdatePusher) PushMissionUpdate(ctx context.Context, playerID uint64, payload []byte) error {
	return k.p.SendRaw(ctx, strconv.FormatUint(playerID, 10), payload)
}

// noopItemGranter / noopExpGranter — allow_noop_reward=true 的 dev 骨架占位。
// 刻意返回错误而不是假装成功:发奖流水滞留 PENDING 可见可补,绝不静默丢失(§8)。
type noopItemGranter struct{}

func (noopItemGranter) GrantItems(context.Context, uint64, string, []biz.RewardItem) error {
	return errcode.New(errcode.ErrUnavailable, "noop item granter(inventory_addr 未配)")
}

func (noopItemGranter) GrantInstances(context.Context, uint64, string, []uint32) (bool, error) {
	return false, errcode.New(errcode.ErrUnavailable, "noop item granter(inventory_addr 未配)")
}

type noopExpGranter struct{}

func (noopExpGranter) AddExperience(context.Context, uint64, uint64, string) error {
	return errcode.New(errcode.ErrUnavailable, "noop exp granter(player_addr 未配)")
}

// runCapacityGuard 启动即跑一轮拿基线,之后每小时一轮(§9.24;对齐 friend)。
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
