// Pandora login 服务入口。
//
// 启动顺序:
//  1. 解析 -conf 路径,加载 yaml(Kratos config + file source)
//  2. 填默认值(conf.Defaults)
//  3. log.Setup → 全局 zap logger
//  4. data layer + biz usecase + service 三层构造
//  5. gRPC + HTTP server 注册
//  6. kratos.New(...).Run() 阻塞
//
// 信号处理:Kratos App 默认监听 SIGINT/SIGTERM,优雅 stop server。
//
// W3 ②(2026-06-05):
//   - cfg.Node.MySQLClient.DSN 接 MySQL(NewMySQLAccountRepo)；DSN 为空致命退出
//   - cfg.Node.RedisClient.Host 非空时,接 Redis(NewRedisSessionRepo + NewRedisTicketJTIRepo)
//   - dev 免密 / 首登自动注册由 dev_skip_password / dev_auto_register 开关控制
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2"
	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	"github.com/redis/go-redis/v9"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/cellroute/etcdtable"
	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/dsauthfence"
	"github.com/luyuancpp/pandora/pkg/grpcclient"
	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/middleware"
	"github.com/luyuancpp/pandora/pkg/mysqlx"
	"github.com/luyuancpp/pandora/pkg/playerno"
	"github.com/luyuancpp/pandora/pkg/redisx"
	"github.com/luyuancpp/pandora/pkg/safego"
	"github.com/luyuancpp/pandora/pkg/snowflake/etcdnode"

	"github.com/luyuancpp/pandora/services/account/login/internal/biz"
	"github.com/luyuancpp/pandora/services/account/login/internal/conf"
	"github.com/luyuancpp/pandora/services/account/login/internal/data"
	"github.com/luyuancpp/pandora/services/account/login/internal/server"
	"github.com/luyuancpp/pandora/services/account/login/internal/service"
)

const serviceName = "login"

var flagConf string

func init() {
	flag.StringVar(&flagConf, "conf", "etc/login-dev.yaml", "config file path")
}

func main() {
	flag.Parse()

	// 1. Logger 先起
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
	if err := cfg.Validate(); err != nil {
		helper.Errorw("msg", "config_validation_failed", "err", err)
		os.Exit(1)
	}

	// 3. snowflake（node_id_source=static 静态本地发号；=etcd 走 etcd 自动抢占独占 nodeID，失租自动退出）
	sf, sfCloser := etcdnode.MustProvideSnowflake(serviceName, cfg.Node.NodeId, cfg.Snowflake)
	defer func() { _ = sfCloser.Close() }()

	// 4. JWT signer / verifier
	authCfg := auth.Config{
		Issuer:            cfg.Login.JWT.Issuer,
		Audience:          cfg.Login.JWT.Audience,
		Secret:            []byte(cfg.Login.JWT.Secret),
		AdditionalSecrets: auth.AdditionalSecretsBytes(cfg.Login.JWT.AdditionalSecrets),
		SessionTTL:        cfg.Login.JWT.SessionTTL.Std(),
		DSTicketTTL:       cfg.Login.JWT.DSTicketTTL.Std(),
	}
	signer, err := auth.NewSigner(authCfg)
	if err != nil {
		helper.Errorw("msg", "auth_signer_init_failed", "err", err)
		os.Exit(1)
	}
	verifier, err := auth.NewVerifier(authCfg)
	if err != nil {
		helper.Errorw("msg", "auth_verifier_init_failed", "err", err)
		os.Exit(1)
	}

	// 5. data 层装配
	accountRepo, roleRepo, roleLedger, sessionGenRepo, mode, db := mustBuildAccountRepo(&cfg, helper)
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()

	// account_devices 保留期清理(§9.24):不活跃设备行超期批删,客户端可刷的只增行兜底有界。
	// 多副本各自跑,DELETE 幂等无需锁;mock 模式(非 MySQL 库)不跑。
	if sdb, ok := db.(*sql.DB); ok && sdb != nil {
		deviceSweepCtx, deviceSweepCancel := context.WithCancel(context.Background())
		defer deviceSweepCancel()
		go func(sdb *sql.DB, retentionDays int) {
			ticker := time.NewTicker(time.Hour)
			defer ticker.Stop()
			for {
				select {
				case <-deviceSweepCtx.Done():
					return
				case <-ticker.C:
					// panic 兜底(压测审核【必修-6】同类点位):单轮 panic 只丢本轮,下轮继续。
					safego.Run(deviceSweepCtx, "login_device_sweep", func() {
						// mode 默认 report_only:待清理量由 dbguard 统一 WARN 告警,这里只在真删时补 INFO。
						if out, err := data.SweepStaleDevices(deviceSweepCtx, sdb, cfg.Login.RetentionMode(), retentionDays, 500); err != nil {
							helper.Warnw("msg", "device_sweep_failed", "err", err)
						} else if out.Cleaned() {
							helper.Infow("msg", "stale_devices_purged", "rows", out.Deleted, "retention_days", retentionDays)
						}
					})
				}
			}
		}(sdb, cfg.Login.DeviceRetentionDays)
	}

	// 角色编号补号任务(docs/design/player-no-and-login-surge.md §3.3):异步批量,
	// 注册/登录关键路径零参与;事务先锁 player_no_counter 单行即全局互斥,多副本
	// 各自跑安全,无需 leader election。mock 模式(非 MySQL 库)不跑。
	if sdb, ok := db.(*sql.DB); ok && sdb != nil {
		playerNoSweepCtx, playerNoSweepCancel := context.WithCancel(context.Background())
		defer playerNoSweepCancel()
		// 启动探针 + 计数器幂等初始化。失败(典型:存量库尚未收敛到 000006)只停用补号:
		// 编号是展示功能,fail-soft 不拦 login 启动;ERROR 让缺迁移在部署当天可见。
		if err := data.EnsurePlayerNoCounter(playerNoSweepCtx, sdb, cfg.Login.PlayerNoStart); err != nil {
			helper.Errorw("msg", "player_no_sweeper_disabled", "err", err)
		} else {
			go func(sdb *sql.DB) {
				// 5s 周期:编号可见延迟 ≈ 周期 + 10s 水位滞后,展示场景无感。
				ticker := time.NewTicker(5 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-playerNoSweepCtx.Done():
						return
					case <-ticker.C:
						// panic 兜底(同 device sweep 点位):单轮 panic 只丢本轮,下轮继续。
						safego.Run(playerNoSweepCtx, "login_player_no_sweep", func() {
							// 单轮 drain 上限 20 批(1 万行):存量追平期不长期霸占,下轮继续
							// (§16.10:复用同一 ticker,不新建第二套状态机)。
							for i := 0; i < 20; i++ {
								n, err := data.SweepPlayerNo(playerNoSweepCtx, sdb, data.PlayerNoBatchSize)
								if err != nil {
									helper.Warnw("msg", "player_no_sweep_failed", "err", err)
									return
								}
								if n > 0 {
									helper.Infow("msg", "player_no_assigned", "rows", n)
								}
								if n < data.PlayerNoBatchSize {
									return
								}
							}
						})
					}
				}
			}(sdb)
		}
	}

	sessionRepo, jtiRepo, rdb := mustBuildRedisRepos(&cfg, helper)
	defer func() {
		if rdb != nil {
			_ = rdb.Close()
		}
	}()

	// locator 客户端(W3 ⑤):addr 为空 → 跳过,Login 仅 Warn 日志
	locatorNotifier, locatorConn, locatorMode := mustBuildLocatorNotifier(&cfg, helper)
	defer func() {
		if locatorConn != nil {
			_ = locatorConn.Close()
		}
	}()

	// hub_allocator 客户端(W4 ⑥):addr 为空 → 跳过,Login 回退自签 hub 票据
	hubAssigner, hubConn, hubMode := mustBuildHubAssigner(&cfg, helper)
	defer func() {
		if hubConn != nil {
			_ = hubConn.Close()
		}
	}()

	// matchmaker 只读权威客户端(P0 修复 2026-07-15):addr 为空 → presence-only
	matchResolver, matchConn, matchMode := mustBuildMatchResolver(&cfg, helper)
	defer func() {
		if matchConn != nil {
			_ = matchConn.Close()
		}
	}()

	// player 客户端(账号 / 角色分离 2026-08-18):把账号名播种成全服显示名。
	// addr 为空 → 不播种,角色名回落 Player_<player_id>;弱依赖,不阻断登录。
	profileSeeder, playerConn, playerMode := mustBuildProfileSeeder(&cfg, helper)
	defer func() {
		if playerConn != nil {
			_ = playerConn.Close()
		}
	}()

	// Hub allocator 的 v2 票与 Session/legacy HS256 是独立信任域。Login 主登录链和
	// VerifyDSTicket 诊断链共用同一份完整 overlap JWKS verifier，但分别显式注入各自 usecase。
	var v2Verifier *auth.DSTicketVerifier
	if cfg.Login.DSTicket.VerifierEnabled() {
		loaded, verr := auth.NewDSTicketVerifierFromConf(cfg.Login.DSTicket)
		if verr != nil {
			helper.Errorw("msg", "ds_ticket_v2_verifier_init_failed", "err", verr,
				"hint", "check login.ds_ticket.jwks_file / keyset_revision / active_kid")
			os.Exit(1)
		}
		v2Verifier = loaded
		helper.Infow("msg", "ds_ticket_v2_verifier_ready",
			"active_kid", cfg.Login.DSTicket.ActiveKid,
			"keyset_revision", cfg.Login.DSTicket.KeysetRevision)
	}
	if cfg.Login.DSTicket.SignerEnabled() && v2Verifier == nil {
		helper.Errorw("msg", "ds_ticket_v2_signer_requires_verifier",
			"hint", "Login 需要校验 Hub allocator 返回的 RS256 票据，请配置完整的重叠期 JWKS")
		os.Exit(1)
	}

	// 6. biz + service 装配
	loginUC := biz.NewLoginUsecase(accountRepo, sessionRepo, locatorNotifier, hubAssigner, roleRepo, sf, cfg.Login.MockHubDSAddr, cfg.Login.Hub.Region, signer, verifier, v2Verifier, cfg.Login.DevSkipPassword, cfg.Login.DevAutoRegister, cfg.Login.AllowedRoleIDs, cfg.Login.DevAllowAnyRole)
	// R7 复审 P0-4 + 收口:会话代际 MySQL 落库(fail-closed 定序权威),与 player_roles
	// 同库同连接池;Login 先 MySQL 分配单调代际再条件写 Redis,并发登录确定性定序。
	// SetRole 的 MySQL 代际强制复核由 session_generation_enforce 分阶段激活(默认只 emit)。
	loginUC.SetSessionGenerationRepo(sessionGenRepo)
	loginUC.SetSessionGenerationEnforce(cfg.Login.SessionGenerationEnforce)
	// 登录失败 Quota(anti-abuse §6 第 4 项):账号 + IP 双维度,只对凭据失败计数。
	// Redis 未配(dev 无会话权威)时不启用——与会话仓储同一个弱依赖边界。
	if rdb != nil {
		loginUC.SetLoginRateLimiter(data.NewRedisLoginRateLimiter(
			rdb, cfg.Login.LoginFailLimit, cfg.Login.LoginFailWindow.Std(), cfg.Login.LoginFailLock.Std()))
		helper.Infow("msg", "login_fail_quota_ready",
			"limit", cfg.Login.LoginFailLimit,
			"window", cfg.Login.LoginFailWindow.Std().String(),
			"lock", cfg.Login.LoginFailLock.Std().String())
	} else {
		helper.Warnw("msg", "login_fail_quota_disabled", "reason", "redis not configured")
	}
	// R9 复审 P1(开关依赖门禁):两个强制门都以「Redis 会话权威存在」为前提——
	// enforce 的 SetRole 复核对象与 sjti 强制门的现行性判定都来自会话仓储。缺 Redis
	// 时开关只会静默变形为"永不强制",安全开关必须 fail-fast 而不是装饰性存在。
	if (cfg.Login.SessionGenerationEnforce || cfg.Login.RequireTicketSJTI) && sessionRepo == nil {
		helper.Errorw("msg", "session_enforce_requires_redis_sessions",
			"session_generation_enforce", cfg.Login.SessionGenerationEnforce,
			"require_ticket_sjti", cfg.Login.RequireTicketSJTI,
			"hint", "配置 node.redis_client(会话权威)或按 rollout 文档显式关闭强制门")
		os.Exit(1)
	}
	if cfg.Login.SessionGenerationEnforce {
		helper.Infow("msg", "session_generation_enforce_active",
			"note", "SetRole 同事务复核 MySQL 会话代际;前提=全 fleet emit 且旧版本已排空")
	} else {
		helper.Infow("msg", "session_generation_emit_only",
			"note", "Login 双写会话代际但 SetRole 不强制;滚动排空后开 login.session_generation_enforce")
	}
	// R8 收口(P0-5):票据兑换点空 sjti 强制门,与 hub_allocator 的 require_ticket_sjti
	// 同语义。首次上线必须 false(兼容档),全 fleet 签发面必带 sjti + 旧版本排空 +
	// 等满票据最大 TTL 后再置 true(顺序见 docs/design/session-generation-rollout.md)。
	loginUC.SetRequireTicketSJTI(cfg.Login.RequireTicketSJTI)
	if cfg.Login.RequireTicketSJTI {
		helper.Infow("msg", "ticket_sjti_binding_enforced",
			"note", "VerifyDSTicket 硬拒空 sjti 票;前提=全 fleet 签发面必带 sjti 且旧票已过期")
	} else {
		helper.Infow("msg", "ticket_sjti_binding_compat",
			"note", "空 sjti 告警放行(混版兼容窗);签发面排空后开 login.require_ticket_sjti")
	}

	// owner 权威接线(owner-authority.md;登出释放 ⑤ + §9.23 query-first 路由)。
	// owner_addr 空 = owner 服务未部署:进场路径按 WAIT 处理(不冒充"无归属"再分配一台 DS),
	// 故这里显式告警——它已不是"少一个可选增强",而是进场链缺了权威。
	if cfg.Login.OwnerAddr != "" {
		ownerReleaser := data.NewGrpcOwnerReleaser(cfg.Login.OwnerAddr)
		defer func() { _ = ownerReleaser.Close() }()
		loginUC.SetOwnerReleaser(ownerReleaser)
		// 同一 owner 客户端兼任 placement 查询器(§9.23 唯一路由权威,开关已删除)。
		loginUC.SetOwnerPlacementQuerier(ownerReleaser)
		helper.Infow("msg", "owner_authority_enabled", "owner_addr", cfg.Login.OwnerAddr)
	} else {
		helper.Warnw("msg", "owner_addr_missing",
			"warn", "owner 是 §9.23 归属唯一权威;未配置时进场一律 WAIT,玩家进不去场景")
	}
	loginUC.SetRequireHubAssignmentBinding(cfg.Login.RequireHubAssignmentBinding)
	loginUC.SetMatchContextResolver(matchResolver)
	// 账号 / 角色分离(2026-08-18):角色归属台账 + 角色名播种。
	// 两者都是弱依赖 —— 台账 nil 时登录降级为「一账号一角色」(改造前的行为),
	// seeder nil 时角色名回落 player 默认前缀名。任何一个缺失都不会让玩家登不进去。
	loginUC.SetRoleLedger(roleLedger)
	if profileSeeder != nil {
		loginUC.SetProfileSeeder(profileSeeder)
	}
	if cfg.Login.DevSkipPassword {
		helper.Warnw("msg", "DEV_SKIP_PASSWORD_ENABLED",
			"warn", "password verification disabled + unknown accounts auto-provisioned; NEVER enable in prod")
	}
	if cfg.Login.DevAutoRegister {
		helper.Warnw("msg", "DEV_AUTO_REGISTER_ENABLED",
			"warn", "unknown accounts auto-registered on first login; NEVER enable in prod")
	}
	if cfg.Login.DevAllowAnyRole {
		helper.Warnw("msg", "DEV_ALLOW_ANY_ROLE_ENABLED",
			"warn", "SelectRole accepts any non-zero role_id when allowed_role_ids empty; NEVER enable in prod")
	} else if len(cfg.Login.AllowedRoleIDs) == 0 {
		helper.Warnw("msg", "select_role_fail_closed",
			"warn", "login.allowed_role_ids empty and dev_allow_any_role false: SelectRole will reject all requests")
	}
	ticketUC := biz.NewTicketUsecase(signer, verifier, jtiRepo)
	// R7 复审 P2-1:票据兑换点会话现行性门前置到 replay marker 之前(LoginUsecase 实现)。
	ticketUC.SetTicketSessionGate(loginUC)
	// DSTicket v2(RS256,方案 B):配置了私钥即启用;启用后 login 侧 battle 票全部走 v2
	// 实例绑定签发,hub 票拒签(只能由 hub_allocator 签)。加载失败直接拒绝启动(fail-closed)。
	if cfg.Login.DSTicket.SignerEnabled() {
		v2, verr := auth.NewDSTicketSignerFromConf(cfg.Login.DSTicket)
		if verr != nil {
			helper.Errorw("msg", "ds_ticket_v2_signer_init_failed", "err", verr,
				"hint", "check login.ds_ticket.private_key_file / active_kid / ttl")
			os.Exit(1)
		}
		if hubMode != "grpc" {
			// v2 下 login 回退自签的 HS256 hub 票会被 v2 DS 全拒,这属于半完成配置,直接拒启。
			helper.Errorw("msg", "ds_ticket_v2_requires_hub_allocator",
				"hub_assigner", hubMode,
				"hint", "ds_ticket v2 启用时必须配置 login.hub.addr(hub 票只能由 hub_allocator 签)")
			os.Exit(1)
		}
		ticketUC.SetDSTicketV2Signer(v2)
		helper.Infow("msg", "ds_ticket_v2_signer_ready", "kid", v2.Kid(), "ttl", v2.TTL().String())
	}
	if cfg.DSAuth.AuthorityModeRedis() && !cfg.Login.DSTicket.SignerEnabled() {
		helper.Errorw("msg", "model_b_requires_ds_ticket_v2_signer",
			"hint", "B1 k8s Login 只允许 RS256 battle 票；配置 login.ds_ticket.private_key_file + active_kid")
		os.Exit(1)
	}
	if v2Verifier != nil {
		ticketUC.SetDSTicketV2Verifier(v2Verifier)
	}
	var hubAssignmentChecker data.HubAssignmentChecker
	if rdb != nil {
		hubAssignmentChecker = data.NewRedisHubAssignmentChecker(rdb)
	}
	ticketUC.SetHubAssignmentBindingPolicy(cfg.Login.RequireHubAssignmentBinding, hubAssignmentChecker)
	if rdb != nil {
		ticketUC.SetBattleTicketAuthorizer(data.NewRedisBattleTicketAuthorizer(
			rdb, cfg.DSAuth.AuthorityModeRedis(), cfg.DSAuth.ActiveHeartbeatMaxAge.Std()))
	}
	loginUC.SetBattleTicketIssuer(ticketUC)
	if closeCell, e := etcdtable.WireRouter(context.Background(), cfg.CellRoute, func(r *cellroute.Router) {
		loginUC.SetCellRouter(r)
		ticketUC.SetCellRouter(r)
	}); e != nil {
		helper.Errorw("msg", "cellroute_init_failed", "err", e)
		os.Exit(1)
	} else if closeCell != nil {
		defer func() { _ = closeCell() }()
	}
	svc := service.NewLoginService(loginUC, ticketUC)
	// DS-only 与 Team internal 两个入口共用同一账号库批量权威；权威 reader 的构造不能
	// 绑在 Team HMAC 开关上，否则本地 legacy/off DS 会因未配置 Team secret 永远拿不到编号。
	playerNoUC, playerNoErr := newPlayerNoResolveUsecase(accountRepo)
	if playerNoErr != nil {
		helper.Errorw("msg", "player_no_authority_init_failed", "err", playerNoErr)
		os.Exit(1)
	}
	svc.SetPlayerNoResolveUsecase(playerNoUC)
	var playerNoVerifier *internalrpcauth.MultiCallerVerifier
	playerNoCredentials := []struct {
		caller   string
		secret   string
		audience string
	}{
		{"team", cfg.Login.PlayerNoResolveAuthSecret, cfg.Login.PlayerNoResolveAuthAudience},
		{"friend", cfg.Login.FriendPlayerNoResolveAuthSecret, cfg.Login.FriendPlayerNoResolveAuthAudience},
		{"guild", cfg.Login.GuildPlayerNoResolveAuthSecret, cfg.Login.GuildPlayerNoResolveAuthAudience},
	}
	playerNoCallerCount := 0
	for _, credential := range playerNoCredentials {
		if credential.secret != "" {
			playerNoCallerCount++
		}
	}
	if playerNoCallerCount != 0 {
		if rdb == nil {
			helper.Errorw("msg", "player_no_resolve_auth_requires_redis",
				"hint", "shared Redis nonce authority is required for LoginInternalService.ResolvePlayerNos")
			os.Exit(1)
		}
		replay, replayErr := internalrpcauth.NewRedisReplayStore(rdb,
			"pandora:login:player-no-resolve:nonce:")
		if replayErr != nil {
			helper.Errorw("msg", "player_no_resolve_verifier_init_failed", "err", replayErr)
			os.Exit(1)
		}
		verifiers := make([]*internalrpcauth.Verifier, 0, playerNoCallerCount)
		for _, credential := range playerNoCredentials {
			if credential.secret == "" {
				continue
			}
			verifier, verifyErr := internalrpcauth.NewVerifier(credential.secret, credential.caller,
				credential.audience, 30*time.Second, replay)
			if verifyErr != nil {
				helper.Errorw("msg", "player_no_resolve_verifier_init_failed", "caller", credential.caller, "err", verifyErr)
				os.Exit(1)
			}
			verifiers = append(verifiers, verifier)
			helper.Infow("msg", "player_no_resolve_verifier_ready", "caller", credential.caller,
				"audience", credential.audience, "max_batch", playerno.ResolveBatchLimit)
		}
		playerNoVerifier, replayErr = internalrpcauth.NewMultiCallerVerifier(verifiers...)
		if replayErr != nil {
			helper.Errorw("msg", "player_no_resolve_verifier_init_failed", "err", replayErr)
			os.Exit(1)
		}
	}
	internalSvc := service.NewLoginInternalService(playerNoUC, playerNoVerifier)
	// UE DS 在线 VerifyDSTicket 入场权威：默认 off/legacy 完全不改变旧内部调用；
	// redis+enforce 才装配 Guard + 同一 Redis 的 Hub/Battle active checker，任一缺失启动失败。
	dsGuard, derr := middleware.NewDSCallbackGuardFromConf(cfg.DSAuth)
	if derr != nil {
		helper.Errorw("msg", "ds_auth_guard_init_failed", "err", derr)
		os.Exit(1)
	}
	if cfg.DSAuth.AuthorityModeRedis() {
		if dsGuard == nil || dsGuard.Mode() != middleware.DSAuthEnforce || rdb == nil {
			helper.Errorw("msg", "ds_admission_authority_incomplete",
				"hint", "redis authority requires enforce guard and Redis")
			os.Exit(1)
		}
		svc.SetRedisDSAdmissionAuthority(dsGuard,
			data.NewRedisDSAdmissionChecker(rdb, cfg.DSAuth.ActiveHeartbeatMaxAge.Std()))
		helper.Infow("msg", "ds_admission_authority_ready", "mode", dsGuard.Mode().String(),
			"authority_mode", cfg.DSAuth.AuthorityMode,
			"active_heartbeat_max_age", cfg.DSAuth.ActiveHeartbeatMaxAge.String())
	}

	// 7. gRPC + HTTP server
	grpcSrv := server.NewGRPCServer(&cfg, svc, internalSvc)
	httpSrv := server.NewHTTPServer(&cfg, svc)

	helper.Infow(
		"msg", "service_ready",
		"grpc", cfg.Server.Grpc.Addr,
		"http", cfg.Server.Http.Addr,
		"account_repo", mode,
		"session_repo", repoEnabled(sessionRepo != nil),
		"jti_repo", repoEnabled(jtiRepo != nil),
		"locator_notifier", locatorMode,
		"hub_assigner", hubMode,
		"match_resolver", matchMode,
		// 账号 / 角色分离(2026-08-18):这两项直接决定「两步登录能不能用」与
		// 「角色名是不是账号名」,起服日志里必须一眼可见,否则线上排查只能靠猜。
		"role_ledger", repoEnabled(roleLedger != nil),
		"role_name_seeder", playerMode,
		"require_hub_assignment_binding", cfg.Login.RequireHubAssignmentBinding,
		"ds_auth_mode", cfg.DSAuth.Mode,
		"ds_auth_authority_mode", cfg.DSAuth.AuthorityMode,
		"dev_skip_password", cfg.Login.DevSkipPassword,
		"dev_auto_register", cfg.Login.DevAutoRegister,
		"jwt_issuer", cfg.Login.JWT.Issuer,
		"jwt_audience", cfg.Login.JWT.Audience,
		"jwt_session_ttl", cfg.Login.JWT.SessionTTL.String(),
		"jwt_ds_ticket_ttl", cfg.Login.JWT.DSTicketTTL.String(),
	)
	if fenceCfg, fenceEnabled := cfg.CapabilityFence(); fenceEnabled {
		fence, err := dsauthfence.AcquireRuntime(context.Background(), dsauthfence.RuntimeConfig{
			Endpoints: fenceCfg.EtcdEndpoints, Prefix: fenceCfg.EtcdPrefix,
			Service: serviceName, KeysetRevision: fenceCfg.KeysetRevision,
			WriterEpoch: dsauthfence.ProtocolEpochV2,
			LeaseTTLSec: fenceCfg.EtcdLeaseTTLSec, DialTimeout: fenceCfg.EtcdDialTimeout.Std(),
		})
		if err != nil {
			helper.Errorw("msg", "login_ds_auth_fence_acquire_failed", "err", err)
			os.Exit(1)
		}
		defer func() { _ = fence.Close() }()
		go func() {
			<-fence.Lost()
			helper.Errorw("msg", "login_ds_auth_fence_lost", "reason", fence.LostReason(),
				"hint", "立即退出，禁止失租 login writer 消费 DS 入场票")
			os.Exit(1)
		}()
		helper.Infow("msg", "login_ds_auth_fence_ready", "required_writer_epoch", fence.RequiredEpoch(), "reclaimed_stale_capability", fence.Reclaimed())
	}

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

func newPlayerNoResolveUsecase(repo any) (*biz.PlayerNoResolveUsecase, error) {
	reader, ok := repo.(biz.PlayerNoBatchReader)
	if !ok {
		return nil, fmt.Errorf("account repository does not implement batch player_no authority")
	}
	return biz.NewPlayerNoResolveUsecase(reader), nil
}

// mustBuildAccountRepo 连 MySQL 构造账号仓储,失败致命退出。
// 返回 (accountRepo, roleRepo, sessionGenRepo, "mysql", *sql.DB)。dev 免密 / 首登自动注册由 biz 层的
// dev_skip_password / dev_auto_register 负责,不再种子固定 mock 账号。
// roleRepo(选角权威化 2026-07-08):player_roles 表仓储,与账号表共库共连接池。
// sessionGenRepo(R7 复审 P0-4):player_session_generations 会话代际仓储,同库同连接池。
func mustBuildAccountRepo(cfg *conf.Config, h kratosHelper) (data.AccountRepo, data.PlayerRoleRepo, data.AccountRoleRepo, data.SessionGenerationRepo, string, sqlDBLike) {
	if cfg.Node.MySQLClient.DSN == "" {
		h.Errorw("msg", "mysql_dsn_required", "hint", "set node.mysql_client.dsn to pandora_account DSN")
		os.Exit(1)
	}

	db, err := mysqlx.NewClient(cfg.Node.MySQLClient)
	if err != nil {
		h.Errorw("msg", "mysql_init_failed", "err", err, "dsn_masked", maskDSN(cfg.Node.MySQLClient.DSN))
		os.Exit(1)
	}

	h.Infow("msg", "account_repo_mysql", "dsn_masked", maskDSN(cfg.Node.MySQLClient.DSN))

	// 严格模式断言(§9.24):非严格 sql_mode 下超长写入会被 MySQL **静默截断**
	// (err=nil 但数据被砍断),等于无声的数据损坏,故 fail-fast 而不是继续产生坏数据。
	if serr := dbguard.AssertStrictModeStartup(db); serr != nil {
		h.Errorw("msg", "mysql_strict_mode_required", "err", serr)
		os.Exit(1)
	}

	// 容量巡检(§9.24):启动即跑一轮拿基线,之后每小时一轮;超预算只告警不阻断。
	// 本函数在 main 装配路径内被调用一次,goroutine 随进程生命周期(main 的 defer 不覆盖此处)。
	go runCapacityGuard(context.Background(), dbguard.New(db, "pandora_account", data.Budgets(), nil))

	// 启动期 schema 检查(2026-07-08):player_roles 是后补的表,既有 MySQL volume / PVC 不会
	// 自动重放 init SQL;缺表时 SelectRole 落库必炸、Login 读已选角持续告警。fail-fast 并指向迁移 SQL。
	schemaCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// 六张表全查(2026-07-27 起;2026-08-18 加 account_roles):此前只查 player_roles /
	// player_session_generations 两张,accounts / account_devices / account_bans 缺表时
	// login 照常启动、首次登录才炸。
	// account_roles 是 000008 新增的角色归属台账,缺表时两步登录的 EnterRole 会
	// fail-closed 拒绝一切角色(「这个角色属不属于这个账号」查不出来就不能放行),
	// 等于全服登不进去 —— 必须启动期就炸,不能等第一个玩家来试。
	if serr := mysqlx.CheckTables(schemaCtx, db, "deploy/mysql-init/02-account-tables.sql",
		"accounts", "account_devices", "account_bans",
		"account_roles", "player_roles", "player_session_generations"); serr != nil {
		h.Errorw("msg", "mysql_schema_check_failed", "err", serr)
		os.Exit(1)
	}
	// accounts.account_id 是 000008 新增的账号身份列。只查表名的话,跑过 000001 基线
	// 但漏跑 000008 的库会照常启动,直到第一次注册/补铸账号身份时才炸在 SQL 上。
	// 刻意只校验 DATA_TYPE 与可空性:本列在 expand 窗口内必须保持 NULL-able
	// (旧二进制注册的行留 NULL,由新 login 下次登录补铸),写成 NOT NULL 会让旧二进制
	// 的 INSERT 直接失败,反而打穿滚动升级。
	if serr := mysqlx.CheckColumnSpecs(schemaCtx, db,
		"tools/migrate/migrations/pandora_account/000008_account_roles.up.sql",
		"accounts",
		mysqlx.ColumnSpec{Name: "account_id", DataType: "bigint", Nullable: "YES"}); serr != nil {
		h.Errorw("msg", "mysql_schema_check_failed", "err", serr)
		os.Exit(1)
	}
	// 列级检查(R8 收口;R9 复审 P2 升级为形状校验):player_session_generations 的
	// generation 列是 000003 迁移新增,早期只建过旧版表的库表名检查会通过、运行期首条
	// 含 generation 的 SQL 才炸;只查列名又识别不出「列在但类型/可空性错」的手工库。
	// 对照迁移产物校验 DATA_TYPE / IS_NULLABLE / COLUMN_KEY,不符 fail-fast。
	if serr := mysqlx.CheckColumnSpecs(schemaCtx, db,
		"tools/migrate/migrations/pandora_account/000003_session_generations.up.sql",
		"player_session_generations",
		mysqlx.ColumnSpec{Name: "player_id", DataType: "bigint", Nullable: "NO", Key: "PRI"},
		mysqlx.ColumnSpec{Name: "sess_jti", DataType: "varchar", Nullable: "NO"},
		mysqlx.ColumnSpec{Name: "generation", DataType: "bigint", Nullable: "NO"}); serr != nil {
		h.Errorw("msg", "mysql_schema_check_failed", "err", serr)
		os.Exit(1)
	}

	// 账号库后端强校验(全服单点扩容,2026-07-27):-Prod 产物由生成器机械注入 require_tidb: true。
	// 与生成器侧 DSN 字符串校验构成双层防线 —— DSN 长什么样证不了对端真是 TiDB,也证不了
	// 排序规则在这个集群上**语义**成立(见 pkg/mysqlx/backend_check.go 里 TiDB 静默回退的说明)。
	// v7.4 是 utf8mb4_0900_ai_ci 的支持下限;accounts.account 必须仍是大小写不敏感 + NO PAD,
	// 否则唯一键语义与单机 MySQL 不一致(Go 侧对账号串零归一化,唯一性全靠 collation)。
	if cfg.Login.RequireTiDB {
		if verr := mysqlx.AssertTiDBVersionAtLeast(schemaCtx, db, 7, 4); verr != nil {
			h.Errorw("msg", "account_backend_not_tidb", "err", verr)
			os.Exit(1)
		}
		if verr := mysqlx.AssertColumnCollationSemantics(schemaCtx, db, "accounts", "account", true, true); verr != nil {
			h.Errorw("msg", "account_collation_semantics_mismatch", "err", verr)
			os.Exit(1)
		}
		h.Infow("msg", "account_backend_tidb_verified")
	}
	return data.NewMySQLAccountRepo(db), data.NewMySQLPlayerRoleRepo(db), data.NewMySQLAccountRoleRepo(db),
		data.NewMySQLSessionGenerationRepo(db), "mysql", db
}

// mustBuildRedisRepos 按 cfg 决定是否启 Redis Session / JTI repo。
// host 与 addrs 同时为空时跳过(测试 / mock 模式)。redis 初始化失败 → panic。
func mustBuildRedisRepos(cfg *conf.Config, h kratosHelper) (data.SessionRepo, data.TicketJTIRepo, redis.UniversalClient) {
	rc := cfg.Node.RedisClient
	// 单实例填 host,Redis Cluster / Sentinel 只填 addrs,两者皆空才算关闭。
	if rc.Host == "" && len(rc.Addrs) == 0 {
		h.Warnw("msg", "redis_disabled_in_config")
		return nil, nil, nil
	}
	rdb := redisx.NewUniversalClient(rc)
	// 启动期 ping 一次,确保 redis 可达;失败致命(login 不可降级)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	redisAddr := rc.Host
	if redisAddr == "" {
		redisAddr = strings.Join(rc.Addrs, ",")
	}
	if err := rdb.Ping(ctx).Err(); err != nil {
		h.Errorw("msg", "redis_ping_failed", "err", err, "addr", redisAddr)
		os.Exit(1)
	}
	h.Infow("msg", "redis_connected", "addr", redisAddr, "db", rc.DB)
	return data.NewRedisSessionRepo(rdb), data.NewRedisTicketJTIRepo(rdb), rdb
}

// mustBuildLocatorNotifier 按 cfg.Login.Locator.Addr 决定是否拨号到 player_locator。
// addr 空 → 返回 nil notifier(Login 仅 Warn,不阻断);
// 拨号失败 → panic(注意:grpcclient.MustDialInsecure 内部 panic,这里语义一致)。
func mustBuildLocatorNotifier(cfg *conf.Config, h kratosHelper) (data.LocationNotifier, locatorConnLike, string) {
	addr := cfg.Login.Locator.Addr
	if addr == "" {
		h.Warnw("msg", "locator_disabled_in_config",
			"hint", "set login.locator.addr to 127.0.0.1:20006 to enable LOGIN_PENDING upsert")
		return nil, nil, "disabled"
	}
	conn := grpcclient.MustDialInsecure(addr)
	h.Infow("msg", "locator_dial_ok", "addr", addr)
	return data.NewGrpcLocationNotifier(conn), conn, "grpc"
}

// mustBuildHubAssigner 按 cfg.Login.Hub.Addr 决定是否拨号到 hub_allocator(W4 ⑥)。
// addr 空 → 返回 nil assigner(Login 回退自签 hub 票据 + MockHubDSAddr);
// 拨号失败 → panic(grpcclient.MustDialInsecure 内部 panic,与 locator 语义一致)。
func mustBuildHubAssigner(cfg *conf.Config, h kratosHelper) (data.HubAssigner, locatorConnLike, string) {
	addr := cfg.Login.Hub.Addr
	if addr == "" {
		h.Warnw("msg", "hub_allocator_disabled_in_config",
			"hint", "set login.hub.addr to 127.0.0.1:20021 to assign real hub shard + ticket")
		return nil, nil, "disabled"
	}
	// P0#5:hub_allocator 是单写者,普通 ClusterIP 直连被 L4 钉在某一 Pod,落到非-writer 副本
	// 就永远拿 ErrWriterSuperseded(可重试)。改用 round_robin 客户端 LB——生产 addr 配成
	// dns:///hub-allocator-headless.<ns>.svc.cluster.local:<port>(headless Service,DNS 返回全部
	// Pod IP)时,AssignHub 的就地重试每次 RPC 轮到不同副本,数次内命中当前 writer;dev 单静态
	// addr(无 dns:/// 前缀,passthrough 单后端)下 round_robin 退化为单后端,行为不变。
	conn := grpcclient.MustDialInsecureRoundRobin(addr)
	h.Infow("msg", "hub_allocator_dial_ok", "addr", addr, "region", cfg.Login.Hub.Region,
		"lb", "round_robin", "hint", "生产 addr 用 dns:/// headless 才有多后端轮询效果")
	return data.NewGrpcHubAssigner(conn), conn, "grpc"
}

// mustBuildMatchResolver 按 cfg.Login.Match.Addr 决定是否拨号到 matchmaker(P0 修复 2026-07-15)。
// addr 空 → 返回 nil resolver(biz 走 presence-only,dev/local 兼容);拨号失败 → panic。
// auth_secret 配置后每次调用携带 internalrpcauth 服务身份签名(matchmaker 强制校验);
// secret 非法(太短等) → panic(配置错误 fail-fast);addr 已配但 secret 缺失 → 告警
// (启用 resume auth 的 matchmaker 会拒绝裸调,权威兑底等于失效)。
func mustBuildMatchResolver(cfg *conf.Config, h kratosHelper) (data.MatchContextResolver, locatorConnLike, string) {
	addr := cfg.Login.Match.Addr
	if addr == "" {
		h.Warnw("msg", "matchmaker_authority_disabled_in_config",
			"hint", "set login.matchmaker.addr to enable durable battle-authority fallback (P0-2/P0-3)")
		return nil, nil, "disabled"
	}
	var signer *internalrpcauth.Signer
	if cfg.Login.Match.AuthSecret != "" {
		s, serr := internalrpcauth.NewSigner(cfg.Login.Match.AuthSecret, "login", cfg.Login.Match.AuthAudience)
		if serr != nil {
			panic(fmt.Sprintf("login.matchmaker.auth_secret/auth_audience invalid: %v", serr))
		}
		signer = s
	} else {
		h.Errorw("msg", "matchmaker_resume_auth_secret_missing",
			"hint", "set login.matchmaker.auth_secret/auth_audience to match matchmaker match_resume_auth_*; unsigned calls are rejected")
	}
	conn := grpcclient.MustDialInsecure(addr)
	h.Infow("msg", "matchmaker_dial_ok", "addr", addr, "resume_auth", signer != nil)
	return data.NewGrpcMatchContextResolver(conn, signer), conn, "grpc"
}

// mustBuildProfileSeeder 按 cfg.Login.Player.Addr 决定是否拨号到 player 服务
// (账号 / 角色分离 2026-08-18:把账号名播种成全服显示名)。
//
// addr 空 → 返回 nil(不播种,角色名回落 player 默认前缀名 Player_<player_id>);
// 拨号失败 → panic(配置写了地址却连不上属于配置错误,fail-fast 比静默不播种好排查)。
//
// 注意这里返回 nil 时**不打 Error**:不播种是一个完全可用的部署形态(dev 只起 login),
// 只是角色名不好看。用 Warn 说明后果即可,不该把它渲染成故障。
func mustBuildProfileSeeder(cfg *conf.Config, h kratosHelper) (biz.ProfileSeeder, locatorConnLike, string) {
	addr := cfg.Login.Player.Addr
	if addr == "" {
		h.Warnw("msg", "role_name_seeding_disabled_in_config",
			"hint", "set login.player.addr to seed 角色名=账号名; 未配时角色名回落 Player_<player_id>")
		return nil, nil, "disabled"
	}
	conn := grpcclient.MustDialInsecure(addr)
	h.Infow("msg", "player_dial_ok", "addr", addr, "purpose", "role_name_seeding")
	return data.NewGrpcProfileSeeder(conn), conn, "grpc"
}

// kratosHelper 是 *klog.Helper 的简化接口,避免 main.go 导出泛型。
type kratosHelper interface {
	Infow(keyvals ...interface{})
	Warnw(keyvals ...interface{})
	Errorw(keyvals ...interface{})
}

// sqlDBLike 给 mustBuildAccountRepo 返回 *sql.DB(可能为 nil)的占位,Close() 由 defer 统一。
type sqlDBLike = interface {
	Close() error
}

// locatorConnLike 给 mustBuildLocatorNotifier 返回 *grpc.ClientConn(可能为 nil)的占位,Close() 由 defer 统一。
type locatorConnLike = interface {
	Close() error
}

func repoEnabled(b bool) string {
	if b {
		return "redis"
	}
	return "disabled"
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

// maskDSN 把 user:password 段脱敏,只保留 host 信息便于日志诊断。
func maskDSN(dsn string) string {
	// 形如:user:password@tcp(host:port)/db?...
	// 简易处理:截到 '@' 替换前缀为 ***
	for i := 0; i < len(dsn); i++ {
		if dsn[i] == '@' {
			return "***@" + dsn[i+1:]
		}
	}
	return dsn
}
