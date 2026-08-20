"""Pandora login 服务入口(Python 版)—— 对应 Go 侧
services/account/login/cmd/login/main.go。

login 是**全服唯一的身份签发点**:SessionToken(玩家态)、AccountToken(账号态)、
DSTicket(进场票)全部由它签。它带病上线的后果不是"某个功能不好用",而是
**任何人都能变成任何人**,或者**全服登不进去**。所以启动闸一条都不能省,
而且顺序必须与 Go 相同 —— 顺序不同会让同一份坏配置在两栈上报**不同的第一个错误**,
值班的人照着 Loki 上的事件名去查,查到的是另一件事。

★ 启动闸(逐条对应 Go 的 os.Exit / panic / Must* / Errorw,**事件名逐字相同**):

    ①  abs_conf_path_failed                   fail-fast  配置路径解析
    ②  config_load_failed                     fail-fast  读不到 yaml / 解析不了
    ③  config_scan_failed                     fail-fast  结构对不上
    ③' cellroute_init_failed                  fail-fast  cell_route.mode 非空(Python 单 Cell)
    ④  config_validation_failed               fail-fast  cfg.Validate() 的跨字段冲突
    ⑤  passwd_backend_required                fail-fast  ★Python 独有,见下
    ⑥  snowflake_init_failed /                fail-fast  static 号段闸 / etcd 抢号
        snowflake_nodeid_acquire_failed
    ⑦  auth_signer_init_failed                fail-fast  JWT 签发器(密钥长度 / 两受众相同)
    ⑧  auth_verifier_init_failed              fail-fast  JWT 校验器
    ⑨  mysql_dsn_required                     fail-fast  账号库是强依赖,不可降级
    ⑩  mysql_init_failed                      fail-fast  连不上库绝不带着起来
    ⑪  mysql_strict_mode_required             fail-fast  非严格 sql_mode = 静默截断
    ⑫  mysql_schema_check_failed(六张表)     fail-fast  缺表 = 首个玩家来了才炸
    ⑬  mysql_schema_check_failed(列形状 ×2)  fail-fast  列在但类型/可空性错
    ⑭  account_backend_not_tidb               fail-fast  仅 require_tidb
    ⑮  account_collation_semantics_mismatch   fail-fast  仅 require_tidb;**行为探针**
    ⑯  player_no_sweeper_disabled             ERROR      fail-soft,只停补号不拦启动
    ⑰  redis_disabled_in_config               WARN       会话权威缺失(dev)
    ⑱  redis_ping_failed                      fail-fast  配了就必须连得上
    ⑲  locator_disabled_in_config             WARN
    ⑳  hub_allocator_disabled_in_config       WARN
    ㉑  matchmaker_authority_disabled_in_config WARN
        matchmaker_resume_auth_secret_missing  ERROR      配了 addr 却没 secret
        match_resume_auth_secret_invalid       fail-fast  Go 侧是 panic
    ㉒  role_name_seeding_disabled_in_config   WARN
    ㉓  ds_ticket_v2_verifier_init_failed      fail-fast
    ㉔  ds_ticket_v2_signer_requires_verifier  fail-fast
    ㉕  session_enforce_requires_redis_sessions fail-fast ★安全开关不能静默变形
    ㉖  ds_ticket_v2_signer_init_failed        fail-fast
    ㉗  ds_ticket_v2_requires_hub_allocator    fail-fast
    ㉘  model_b_requires_ds_ticket_v2_signer   fail-fast
    ㉙  ds_auth_guard_init_failed              fail-fast  mode 拼错
    ㉚  ds_admission_authority_incomplete      fail-fast  Model B 权威件不全
    ㉛  login_ds_auth_fence_acquire_failed     fail-fast  capability 租约
    ㉜  app_run_failed                         exit 1

  ⑤ 是 Python 独有的一道闸:Go 的 bcrypt 编译进二进制,不可能缺;Python 侧
  bcrypt 是外部包。**唯一"能跑"的降级形态是跳过密码校验**,也就是任何密码都能
  登任何账号 —— 所以缺包必须在装配期拒启,而不是等第一个玩家登录时 ImportError
  (那时进程已 Ready、流量已切过来)。

★ 后台循环五条,全部走 safego(单轮异常只丢本轮,不静默弄死循环):
    login_device_sweep       account_devices 保留期清理(1h)
    login_player_no_sweep    角色编号异步补号(5s,单轮 drain ≤ 20 批)
    db_capacity_guard_initial + db_capacity_guard   容量巡检(启动一轮 + 1h)
    (etcd 档的 snowflake 续约在 provide_node 内部拉起,算第五条)

★ 诚实边界(见 honest_gaps,每一条都是 fail-closed 而不是静默降级):
    - DSTicket v2(RS256):未实现 → 配了就拒启(㉓㉖),绝不静默签 HS256 顶替
      (DS 只认 RS256,静默降级 = 全服进不去场景且启动日志全绿)。
    - Model B(ds_auth.authority_mode=redis)的在线入场权威:未实现 → 拒启(㉚)。
    - capability fence(dsauthfence 运行时):未实现 → 需要时拒启(㉛)。
    - matchmaker 只读权威兜底:未接线 → 只保留配置面的告警,断线重连按 WAIT 处理
      (biz 层的 §9.22 fail-closed 分支)。

运行:
    cd python
    .venv/Scripts/python.exe -m pandorapy.services.login.main \\
        -conf ../services/account/login/etc/login-dev.yaml
"""

from __future__ import annotations

# ★ 必须最先 import:Windows 控制台默认 cp1252,日志里的中文会抛 UnicodeEncodeError
# 并把真正的启动错误顶掉。见 pandorapy/_utf8.py。
from pandorapy import _utf8  # noqa: F401  isort:skip

import argparse
import asyncio
import contextlib
import pathlib
import sys

import asyncmy
from pandora.login.v1 import login_pb2_grpc as logingrpc

from pandorapy import auth as pauth
from pandorapy import config as pconfig
from pandorapy import dbguard
from pandorapy import dsauth
from pandorapy import godur
from pandorapy import internalrpcauth
from pandorapy import log as plog
from pandorapy import mysqlx
from pandorapy import redisx
from pandorapy import safego
from pandorapy import server as pserver
from pandorapy import snowflake_etcd as psnowflake_etcd
from pandorapy.services.login import biz as lbiz
from pandorapy.services.login import budgets as lbudgets
from pandorapy.services.login import clients as lclients
from pandorapy.services.login import conf as lconf
from pandorapy.services.login import data as ldata
from pandorapy.services.login import dsadmission as ldsadmission
from pandorapy.services.login import dsticket as ldsticket
from pandorapy.services.login import hubbinding as lhubbinding
from pandorapy.services.login import passwd as lpasswd
from pandorapy.services.login import rest as lrest
from pandorapy.services.login import schema_probe as lschema
from pandorapy.services.login import service as lsvc

SERVICE_NAME = "login"
HTTP_DEFAULT_PORT = 21001

# 账号库。与 Go 的 mustBuildAccountRepo / dbguard.New(db, "pandora_account", ...) 同名。
ACCOUNT_DB = "pandora_account"

PLAYER_NO_RESOLVE_MAX_CLOCK_SKEW_SEC = 30.0
PLAYER_NO_RESOLVE_NONCE_PREFIX = "pandora:login:player-no-resolve:nonce:"

# 缺表提示直接指向 init SQL / 迁移产物,省得值班的人翻仓库(与 Go 逐字同)。
SCHEMA_HINT = "deploy/mysql-init/02-account-tables.sql"
MIGRATION_HINT_ACCOUNT_ROLES = "tools/migrate/migrations/pandora_account/000008_account_roles.up.sql"
MIGRATION_HINT_SESSION_GEN = (
    "tools/migrate/migrations/pandora_account/000003_session_generations.up.sql"
)

# 六张表全查(2026-07-27 起;2026-08-18 加 account_roles)。顺序与 Go 逐字相同。
REQUIRED_TABLES = (
    "accounts",
    "account_devices",
    "account_bans",
    "account_roles",
    "player_roles",
    "player_session_generations",
)

# 启动期 DB 调用的超时,对齐 Go 的 context.WithTimeout(5s)。
SCHEMA_PROBE_TIMEOUT_SEC = 5.0
# Redis 启动期 Ping 超时,对齐 Go 的 context.WithTimeout(3s)。
REDIS_PING_TIMEOUT_SEC = 3.0

# 后台循环周期(与 Go 的 ticker 同值)。
DEVICE_SWEEP_INTERVAL_SEC = 3600.0
PLAYER_NO_SWEEP_INTERVAL_SEC = 5.0
CAPACITY_GUARD_INTERVAL_SEC = 3600.0
# 单轮补号 drain 上限(Go: `for i := 0; i < 20; i++`)。存量追平期不长期霸占,下轮继续。
PLAYER_NO_DRAIN_BATCHES = 20
# 设备清理单批上限(Go: data.SweepStaleDevices(..., 500))。
DEVICE_SWEEP_BATCH = 500


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行。

    刻意用 `-conf`(单横线)而不是 argparse 惯常的 `--conf`:Go 侧 flag 包用单横线,
    而 run_services.ps1 / start.ps1 / K8s manifest 里全是 `-conf xxx.yaml`。
    Python 版必须能被**同一条命令行**拉起,否则那些脚本都要改。
    """
    ap = argparse.ArgumentParser(prog=f"pandora-{SERVICE_NAME}", add_help=True)
    ap.add_argument("-conf", dest="conf", default="etc/login-dev.yaml")
    return ap.parse_args(argv)


async def _run_capacity_guard(pool, schema: str = ACCOUNT_DB) -> None:  # noqa: ANN001
    """容量巡检(§9.24):**只告警不阻断**。

    容量超限是"要去查的问题",不是"服务不能跑的理由" —— 拒绝启动会把容量问题
    升级成可用性事故。走 information_schema 估算(毫秒级、不锁表、不扫数据),
    绝不用 COUNT(*)。启动即一轮拿基线(上线时就已超限当场可见),再进周期循环
    (与 Go 的 runCapacityGuard 同序)。
    """
    budgets = lbudgets.budgets()

    async def _once() -> None:
        async with pool.acquire() as conn:
            result = await dbguard.check_budgets(conn, schema, budgets)
        dbguard.log_violations(result, db=schema)

    await safego.run_once("db_capacity_guard_initial", _once)
    await safego.loop("db_capacity_guard", CAPACITY_GUARD_INTERVAL_SEC, _once)


async def _run_device_sweep(  # noqa: ANN001
    pool,
    mode: dbguard.Mode,
    retention_days: int,
    *,
    schema: str = ACCOUNT_DB,
) -> None:
    """account_devices 保留期清理(§9.24)。

    device_id 由客户端上报,单账号可无限堆新设备行 —— 不清理就是一张只增不减的表。
    多副本各自跑,DELETE 幂等无需锁。mode 默认 report_only:待清理量由 dbguard 统一
    WARN,这里只在**真删**时补一条 INFO(与 Go 的 out.Cleaned() 分支同)。
    """
    log = plog.get()

    async def _once() -> None:
        try:
            out = await ldata.sweep_stale_devices(
                pool,
                mode,
                retention_days,
                DEVICE_SWEEP_BATCH,
                schema=schema,
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透:吞掉之后取消不再传播,该停的停不下来(§9.16
            # 「先摘流量 → 再排空在途」失效),而这里还会把它渲染成一次 sweep 失败。
            raise
        except BaseException as exc:  # noqa: BLE001
            log.warning("device_sweep_failed", err=str(exc))
            return
        # Go: `out.Cleaned()` == `Deleted > 0`(report_only 档 Deleted 恒 0,
        # 待清理量由 dbguard 自己 WARN,不在这里重复报)。
        if out.deleted > 0:
            log.info(
                "stale_devices_purged", rows=out.deleted, retention_days=retention_days
            )

    await safego.loop("login_device_sweep", DEVICE_SWEEP_INTERVAL_SEC, _once)


async def _run_player_no_sweep(pool) -> None:  # noqa: ANN001
    """角色编号异步补号(docs/design/player-no-and-login-surge.md §3.3)。

    注册 / 登录关键路径**零参与**:编号是展示功能,拿它挡注册就是把开服洪峰
    卡在一个串行发号器上。事务先锁 player_no_counter 单行即全局互斥,多副本各自跑
    安全,无需 leader election。
    周期 5s:编号可见延迟 ≈ 周期 + 10s 水位滞后,展示场景无感。
    """
    log = plog.get()

    async def _once() -> None:
        # 单轮 drain 上限 20 批:存量追平期不长期霸占,下轮继续
        # (§16.10:复用同一个循环,不新建第二套状态机)。
        for _ in range(PLAYER_NO_DRAIN_BATCHES):
            try:
                n = await ldata.sweep_player_no(pool, ldata.PLAYER_NO_BATCH_SIZE)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                log.warning("player_no_sweep_failed", err=str(exc))
                return
            if n > 0:
                log.info("player_no_assigned", rows=n)
            if n < ldata.PLAYER_NO_BATCH_SIZE:
                return

    await safego.loop("login_player_no_sweep", PLAYER_NO_SWEEP_INTERVAL_SEC, _once)


def ds_admission_incomplete(ds_guard, rdb) -> bool:  # noqa: ANN001
    """Model B 在线入场权威的三件套是否残缺(对应 Go 的 ds_admission_authority_incomplete)。

    抽成模块级函数**只为可测**:这一支只在 `ds_auth.authority_mode=redis` 才走到,
    dev 起不到、集成测试也起不到 —— 内联写在 main 里的话,一个
    `ds_guard.mode()`(`mode` 其实是 property)这样的低级错误要到**生产切 Model B
    的那一刻**才炸。本轮就真写出过这个形状,是被一次手工调用抓到的。
    """
    return ds_guard is None or ds_guard.mode is not dsauth.Mode.ENFORCE or rdb is None


def _repo_enabled(present: bool) -> str:
    """对应 Go 的 repoEnabled():service_ready 日志里 redis/disabled 两个字面量。"""
    return "redis" if present else "disabled"


async def _main_async(args: argparse.Namespace) -> int:  # noqa: C901, PLR0911, PLR0912, PLR0915
    # 与 Go 同为线性启动闸:每道闸一个 return 1。拆成子函数会让"顺序即契约"
    # 这件事散掉,而顺序恰恰是本文件最重要的性质。
    logger = plog.setup(SERVICE_NAME)
    logger.info("service_starting", conf=args.conf)

    # ── ① 配置路径 ───────────────────────────────────────────────────────
    try:
        conf_path = pathlib.Path(args.conf).resolve()
    except OSError as exc:
        logger.error("abs_conf_path_failed", err=str(exc))
        return 1

    # ── ②③ 加载 + 结构校验 ───────────────────────────────────────────────
    # Go 的 c.Load() 覆盖"读文件 + 解析"两步,两者都归 config_load_failed;
    # 结构对不上(c.Scan)才是 config_scan_failed。分错的后果是 Loki 上按事件名
    # 建的告警对不上 —— 静默失去覆盖。
    try:
        cfg = lconf.Config.load(str(conf_path))
    except pconfig.UnsupportedSectionError as exc:
        # ③' cell_route.mode 非空。事件名由异常自带(cellroute_init_failed),
        # 不在这里猜 —— 猜错的事件名比没有事件名更难查。
        logger.error(exc.event, section=exc.section, err=str(exc))
        return 1
    except pconfig.ConfigLoadError as exc:
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except Exception as exc:  # noqa: BLE001
        logger.error("config_scan_failed", err=str(exc), path=str(conf_path))
        return 1

    # ── ④ 跨字段校验 ─────────────────────────────────────────────────────
    try:
        cfg.validate_conf()
    except Exception as exc:  # noqa: BLE001
        logger.error("config_validation_failed", err=str(exc))
        return 1

    # ── ⑤ 密码后端(Python 独有)─────────────────────────────────────────
    # 不设这道闸会怎样:唯一"能跑"的降级形态是跳过密码校验 = 任何密码登任何账号。
    # 而它不会在启动期有任何信号,要到第一个真实玩家登录时才 ImportError,
    # 那时进程已 Ready、流量已经切过来。
    try:
        lpasswd.require_backend()
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "passwd_backend_required",
            err=str(exc),
            hint="pip/uv install bcrypt>=4.1(已在 python/pyproject.toml 依赖里)",
        )
        return 1

    # ── ⑥ Snowflake(player_id / account_id 的发号器)────────────────────
    node_holder = None
    try:
        snowflake_node, node_holder = await psnowflake_etcd.provide_node(
            list(cfg.snowflake.etcd_endpoints),
            cfg.snowflake.etcd_service_name or SERVICE_NAME,
            cfg.node.node_id,
            cfg.snowflake.node_id_source,
            # 失主 = 独占权不可证明 = 继续发号就是重号,没有安全的降级 → 退出进程。
            on_lost=psnowflake_etcd.exit_process_on_lost,
            **({"prefix": cfg.snowflake.etcd_prefix} if cfg.snowflake.etcd_prefix else {}),
            **(
                {"lease_ttl_sec": cfg.snowflake.etcd_lease_ttl_sec}
                if cfg.snowflake.etcd_lease_ttl_sec > 0
                else {}
            ),
        )
    except ValueError as exc:
        # static 档的号段闸(拒 node_id=0 / 超 NODE_MASK)在 provide_node 内部。
        # node_id=0 是 UE DS 本地发号器的保留号:用它铸出来的 player_id 会与 DS
        # 本地铸的 ID 逐位相同,撞进同一玩家的键空间 —— 只在数据层面表现为重号。
        logger.error(
            "snowflake_init_failed",
            err=str(exc),
            node_id=cfg.node.node_id,
            node_id_source=cfg.snowflake.node_id_source,
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "snowflake_nodeid_acquire_failed",
            err=str(exc),
            node_id_source=cfg.snowflake.node_id_source,
            hint="etcd 档抢不到 nodeID 时**不得**退回 static —— 那正好会与别的副本重号",
        )
        return 1
    if node_holder is not None:
        logger.info("snowflake_nodeid_acquired", node_id=node_holder.node_id, source="etcd")

    try:
        return await _run_with_snowflake(cfg, snowflake_node, logger)
    finally:
        if node_holder is not None:
            # 正常退出:停续约并断开 etcd。**刻意不 revoke** —— 立刻释放会让新副本
            # 在同一日历秒抢到同号并从 step 0 重数,逐位重号。
            with contextlib.suppress(Exception):
                await node_holder.close()


async def _run_with_snowflake(  # noqa: C901, PLR0911, PLR0912, PLR0915
    cfg: lconf.Config, snowflake_node, logger  # noqa: ANN001
) -> int:
    lg = cfg.login

    # ── ⑦⑧ JWT 签发 / 校验 ──────────────────────────────────────────────
    # 两个受众必须不同(账号态 vs 玩家态):相同 = 账号 token 能冒充玩家 token,
    # 而 sub 里装的是 account_id —— 越权且运行期毫无信号。SignerConfig.validate()
    # 连同 HS256 密钥长度一起拦下。
    signer_cfg = pauth.SignerConfig(
        secret=lg.jwt.secret.encode("utf-8"),
        issuer=lg.jwt.issuer,
        audience=lg.jwt.audience,
        account_audience=lconf.DEFAULT_JWT_ACCOUNT_AUDIENCE,
        session_ttl=lg.jwt.session_ttl_td(),
        account_ttl=lconf.DEFAULT_JWT_ACCOUNT_TTL,
        additional_secrets=tuple(s.encode("utf-8") for s in lg.jwt.additional_secrets),
    )
    try:
        signer = pauth.Signer(signer_cfg)
    except Exception as exc:  # noqa: BLE001
        logger.error("auth_signer_init_failed", err=str(exc))
        return 1
    # Go 侧 signer / verifier 是两个对象、两道闸。Python 的 Signer 同时兼任校验
    # (verify()),但**校验器的配置校验必须单独跑一遍**并保留独立事件名:
    # 两道闸合并后,Loki 上按 auth_verifier_init_failed 建的告警会静默失去覆盖。
    try:
        pauth.Signer(signer_cfg)
    except Exception as exc:  # noqa: BLE001
        logger.error("auth_verifier_init_failed", err=str(exc))
        return 1
    verifier = signer

    ds_signer = ldsticket.DSTicketSigner(
        secret=lg.jwt.secret,
        issuer=lg.jwt.issuer,
        audience=lg.jwt.audience,
        ttl=lg.jwt.ds_ticket_ttl_td(),
        additional_secrets=tuple(lg.jwt.additional_secrets),
    )

    # ── ⑨ MySQL 强依赖 ───────────────────────────────────────────────────
    raw_dsn = cfg.node.mysql_client.dsn
    if not raw_dsn:
        logger.error(
            "mysql_dsn_required", hint="set node.mysql_client.dsn to pandora_account DSN"
        )
        return 1

    conn_cfg = mysqlx.parse_go_dsn(raw_dsn, default_db=ACCOUNT_DB)
    try:
        # ★ 池参数走 mysqlx.pool_kwargs 统一翻译 —— max_open/max_idle/
        # conn_max_lifetime/ping_timeout 四个字段 yaml 里都写了、Go 侧都读,
        # 手写 create_pool 只传连接身份的话它们**配了不生效且不报错**。
        #
        # ★ autocommit=True:与 Go 的 database/sql 默认语义一致,也是 data.py 的
        # 明写前提。建成 False 会让每条只读 SELECT 也开一个事务并长期持有快照 ——
        # 而 data.py 里需要原子性的地方全部显式 conn.begin()。
        mysqlx.assert_pool_conf_supported(cfg.node.mysql_client)
        pool = await asyncmy.create_pool(
            **mysqlx.pool_kwargs(cfg.node.mysql_client, conn_cfg, autocommit=True)
        )
    except asyncio.CancelledError:
        # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
        # 吞掉之后取消**不再传播** —— 启动路径上 Ctrl-C / 上层取消会被翻译成某道闸
        # 的失败,报出假的失败原因。
        raise
    except BaseException as exc:  # noqa: BLE001
        logger.error("mysql_init_failed", err=str(exc), dsn_masked=mysqlx.mask_dsn(raw_dsn))
        return 1
    logger.info("account_repo_mysql", dsn_masked=mysqlx.mask_dsn(raw_dsn))

    try:
        rc = await _run_with_pool(
            cfg,
            snowflake_node,
            signer,
            verifier,
            ds_signer,
            pool,
            conn_cfg["db"],
            logger,
        )
    finally:
        pool.close()
        with contextlib.suppress(Exception):
            await pool.wait_closed()
    return rc


async def _probe_schema(pool, cfg: lconf.Config, logger) -> int | None:  # noqa: ANN001, C901
    """⑪–⑮:严格模式 + 六张表 + 两处列形状 + TiDB 双探针。

    全部包 `asyncio.wait_for(..., 5s)`,对齐 Go 的 context.WithTimeout(5s):
    库半死不活(TCP 通但不回包)时没有超时会让进程**挂着不退** —— k8s 只看到
    「未 Ready」,排障的人根本不知道卡在哪道闸上。
    返回 None = 全过;返回 1 = 调用方直接 return 1。
    """
    async with pool.acquire() as conn:
        # ⑪ 严格模式:非严格 sql_mode 下超长写入被 MySQL **静默截断**(err=nil 但
        # 数据被砍断)。账号名被截断 = 两个账号塌成一个,是无声的身份事故。
        try:
            await asyncio.wait_for(dbguard.assert_strict_mode(conn), timeout=SCHEMA_PROBE_TIMEOUT_SEC)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("mysql_strict_mode_required", err=str(exc))
            return 1

        # ⑫ 六张表。account_roles 是 000008 新增的角色归属台账,缺表时两步登录的
        # EnterRole 会 fail-closed 拒绝一切角色(「这个角色属不属于这个账号」查不出来
        # 就不能放行)= 全服登不进去 —— 必须启动期就炸,不能等第一个玩家来试。
        try:
            await asyncio.wait_for(
                mysqlx.check_tables(conn, SCHEMA_HINT, *REQUIRED_TABLES),
                timeout=SCHEMA_PROBE_TIMEOUT_SEC,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("mysql_schema_check_failed", err=str(exc))
            return 1

        # ⑬-a accounts.account_id:只查表名的话,跑过 000001 基线但漏跑 000008 的库
        # 会照常启动,直到第一次注册/补铸账号身份时才炸在 SQL 上。
        # ★ 刻意只校 DATA_TYPE 与可空性,而且要求 **Nullable=YES**:
        # 本列在 expand 窗口内必须保持可空(旧二进制注册的行留 NULL,由新 login
        # 下次登录补铸)。写成 NOT NULL 会让旧二进制的 INSERT 直接失败 ——
        # 一道本意是"检查迁移跑没跑"的闸,反而**打穿滚动升级**。
        try:
            await asyncio.wait_for(
                lschema.check_column_specs(
                    conn,
                    MIGRATION_HINT_ACCOUNT_ROLES,
                    "accounts",
                    lschema.ColumnSpec(name="account_id", data_type="bigint", nullable="YES"),
                ),
                timeout=SCHEMA_PROBE_TIMEOUT_SEC,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("mysql_schema_check_failed", err=str(exc))
            return 1

        # ⑬-b player_session_generations 的 generation 列是 000003 迁移新增:早期只
        # 建过旧版表的库表名检查会通过、运行期首条含 generation 的 SQL 才炸;只查列名
        # 又识别不出「列在但类型/可空性错」的手工库。对照迁移产物校验形状。
        try:
            await asyncio.wait_for(
                lschema.check_column_specs(
                    conn,
                    MIGRATION_HINT_SESSION_GEN,
                    "player_session_generations",
                    lschema.ColumnSpec(
                        name="player_id", data_type="bigint", nullable="NO", key="PRI"
                    ),
                    lschema.ColumnSpec(name="sess_jti", data_type="varchar", nullable="NO"),
                    lschema.ColumnSpec(name="generation", data_type="bigint", nullable="NO"),
                ),
                timeout=SCHEMA_PROBE_TIMEOUT_SEC,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("mysql_schema_check_failed", err=str(exc))
            return 1

        # ⑭⑮ 账号库后端强校验(全服单点扩容,2026-07-27)。
        # -Prod 产物由生成器机械注入 require_tidb: true;与生成器侧 DSN 字符串校验
        # 构成双层防线 —— DSN 长什么样证不了对端真是 TiDB,更证不了排序规则在这个
        # 集群上**语义**成立。
        if not cfg.login.require_tidb:
            return None
        try:
            await asyncio.wait_for(
                mysqlx.assert_tidb(conn, min_major=7, min_minor=4),
                timeout=SCHEMA_PROBE_TIMEOUT_SEC,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("account_backend_not_tidb", err=str(exc))
            return 1
        try:
            # ★ 这是**行为探针**,不是"查列名存在"。TiDB 的 _ci 排序规则只在集群以
            # new_collations_enabled_on_first_bootstrap=true 初始化时才真正大小写不
            # 敏感,否则语义上按 binary 比较且**不报错**(该参数首次 bootstrap 后
            # 不可更改)。漂移的后果:老玩家换个大小写登不进去(dev_auto_register
            # 若误开还会静默开个新号),以及大小写变体抢注同名账号。
            await asyncio.wait_for(
                lschema.assert_column_collation_semantics(conn, "accounts", "account", True, True),
                timeout=SCHEMA_PROBE_TIMEOUT_SEC,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("account_collation_semantics_mismatch", err=str(exc))
            return 1
        logger.info("account_backend_tidb_verified")
    return None


async def _run_with_pool(  # noqa: C901, PLR0911, PLR0912, PLR0915
    cfg: lconf.Config,
    snowflake_node,  # noqa: ANN001
    signer: pauth.Signer,
    verifier: pauth.Signer,
    ds_signer: ldsticket.DSTicketSigner,
    pool,  # noqa: ANN001
    db_schema: str,
    logger,  # noqa: ANN001
) -> int:
    lg = cfg.login
    del verifier  # Python 的 Signer 兼任校验;保留形参与 Go 装配同形

    # ── ⑪–⑮ schema / 后端语义探针 ───────────────────────────────────────
    rc = await _probe_schema(pool, cfg, logger)
    if rc is not None:
        return rc

    # ── data 层 ──────────────────────────────────────────────────────────
    account_repo = ldata.MySQLAccountRepo(pool)
    role_repo = ldata.MySQLPlayerRoleRepo(pool)
    role_ledger = ldata.MySQLAccountRoleRepo(pool)
    session_gen_repo = ldata.MySQLSessionGenerationRepo(pool)

    # ── ⑯ 补号计数器初始化(fail-soft)───────────────────────────────────
    # 失败(典型:存量库尚未跑 000007 expand)**只停用补号**:编号是展示功能,
    # 不拦 login 启动;ERROR 让"缺迁移"在部署当天可见而不是等运维发现编号一直是 0。
    player_no_sweep_enabled = True
    try:
        await asyncio.wait_for(
            ldata.ensure_player_no_counter(pool, lg.player_no_start),
            timeout=SCHEMA_PROBE_TIMEOUT_SEC,
        )
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001
        player_no_sweep_enabled = False
        logger.error("player_no_sweeper_disabled", err=str(exc))

    # ── ⑰⑱ Redis 会话权威 ───────────────────────────────────────────────
    rdb = None
    if not cfg.node.redis_client.endpoints():
        # 单实例填 host,Cluster/Sentinel 只填 addrs,两者皆空才算关闭。
        logger.warning("redis_disabled_in_config")
    else:
        try:
            rdb = await redisx.must_connect(
                cfg.node.redis_client, ping_timeout_sec=REDIS_PING_TIMEOUT_SEC
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 配了就必须连得上:login 的会话权威不可降级 —— 静默无会话权威时
            # 「顶号」这件事整个失效(旧设备的 token 永远有效)。
            logger.error("redis_ping_failed", err=str(exc))
            return 1
        logger.info("redis_connected", db=cfg.node.redis_client.db)

    session_repo = ldata.RedisSessionRepo(rdb) if rdb is not None else None
    jti_repo = ldata.RedisTicketJTIRepo(rdb) if rdb is not None else None

    # team → login 内部 player_no 批量解析验签。handler 始终注册；未配置时它
    # fail-closed。配置了 secret 就必须有共享 Redis replay authority，不能退化成
    # 进程内 nonce 表（多副本下可跨 Pod 重放）。
    player_no_verifier: internalrpcauth.Verifier | None = None
    if lg.player_no_resolve_auth_secret:
        if rdb is None:
            logger.error(
                "player_no_resolve_auth_requires_redis",
                hint="configured internal RPC auth requires shared Redis replay authority",
            )
            return 1
        try:
            player_no_verifier = internalrpcauth.Verifier(
                lg.player_no_resolve_auth_secret,
                "team",
                lg.player_no_resolve_auth_audience,
                PLAYER_NO_RESOLVE_MAX_CLOCK_SKEW_SEC,
                internalrpcauth.RedisReplayStore(
                    rdb, PLAYER_NO_RESOLVE_NONCE_PREFIX
                ),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("player_no_resolve_verifier_init_failed", err=str(exc))
            return 1
        logger.info(
            "player_no_resolve_verifier_ready",
            caller="team",
            audience=lg.player_no_resolve_auth_audience,
        )
    else:
        logger.warning(
            "player_no_resolve_verifier_disabled",
            hint="LoginInternalService.ResolvePlayerNos will fail closed",
        )

    # ── ⑲–㉒ 四个 gRPC 客户端(全部弱依赖,addr 空即跳过)────────────────
    notifier = None
    locator_mode = "disabled"
    if not lg.locator.addr:
        logger.warning(
            "locator_disabled_in_config",
            hint="set login.locator.addr to 127.0.0.1:20006 to enable LOGIN_PENDING upsert",
        )
    else:
        notifier = lclients.GrpcLocationNotifier(lg.locator.addr)
        locator_mode = "grpc"
        logger.info("locator_dial_ok", addr=lg.locator.addr)

    hub_assigner = None
    hub_mode = "disabled"
    if not lg.hub.addr:
        logger.warning(
            "hub_allocator_disabled_in_config",
            hint="set login.hub.addr to 127.0.0.1:20021 to assign real hub shard + ticket",
        )
    else:
        hub_assigner = lclients.GrpcHubAssigner(lg.hub.addr)
        hub_mode = "grpc"
        logger.info(
            "hub_allocator_dial_ok",
            addr=lg.hub.addr,
            region=lg.hub.region,
            lb="round_robin",
            hint="生产 addr 用 dns:/// headless 才有多后端轮询效果",
        )

    # matchmaker:Python 侧**未接线**(biz 的断线重连三态门未移植)。
    # 配置面的三条判定仍然逐条保留 —— 它们说的是"这份 yaml 对不对",与实现无关;
    # 少打一条会让运维以为配置没问题,而 resume 权威其实从来没生效过。
    match_mode = "disabled"
    if not lg.matchmaker.addr:
        logger.warning(
            "matchmaker_authority_disabled_in_config",
            hint="set login.matchmaker.addr to enable durable battle-authority fallback",
        )
    else:
        match_mode = "not_implemented"
        if lg.matchmaker.auth_secret:
            try:
                # Go 侧 secret 非法直接 panic(配置错误 fail-fast)。这里只校验形状,
                # 不建 signer(没有调用点)—— 但校验必须照跑:一份非法 secret 在
                # Go 版拒启、Python 版放行,等于两栈对同一份 yaml 结论不同。
                internalrpcauth.validate_secret(lg.matchmaker.auth_secret)
            except Exception as exc:  # noqa: BLE001
                logger.error("match_resume_auth_secret_invalid", err=str(exc))
                return 1
        else:
            logger.error(
                "matchmaker_resume_auth_secret_missing",
                hint=(
                    "set login.matchmaker.auth_secret/auth_audience to match matchmaker "
                    "match_resume_auth_*; unsigned calls are rejected"
                ),
            )
        logger.warning(
            "matchmaker_resolver_not_implemented",
            addr=lg.matchmaker.addr,
            hint=(
                "Python 版未移植 ResolvePlayerMatchContext 兜底:locator 报 BATTLE 时"
                "按 §9.22 返回 WAIT(不冒充默认 Hub),玩家表现为退避重查"
            ),
        )

    profile_seeder = None
    player_mode = "disabled"
    if not lg.player.addr:
        # 刻意用 WARN 不用 ERROR:不播种是一个完全可用的部署形态(dev 只起 login),
        # 只是角色名回落 Player_<player_id>。把它渲染成故障会稀释真正的故障。
        logger.warning(
            "role_name_seeding_disabled_in_config",
            hint="set login.player.addr to seed 角色名=账号名; 未配时角色名回落 Player_<player_id>",
        )
    else:
        profile_seeder = lclients.GrpcProfileSeeder(lg.player.addr)
        player_mode = "grpc"
        logger.info("player_dial_ok", addr=lg.player.addr, purpose="role_name_seeding")

    # ── ㉓㉔ DSTicket v2(RS256)—— Python 侧未实现,配了就拒启 ────────────
    # 静默用 HS256 顶替 RS256 的后果:DS 侧只认 RS256,票签出来了但一律被拒,
    # 表现为"全服进不去场景"且**启动日志全绿**。所以只有拒启一条路。
    if lg.ds_ticket.verifier_enabled():
        logger.error(
            "ds_ticket_v2_verifier_init_failed",
            err="DSTicket v2 (RS256/JWKS) not implemented in the Python build",
            hint="check login.ds_ticket.jwks_file / keyset_revision / active_kid;需要 v2 请用 Go 版跑 login",
        )
        return 1
    if lg.ds_ticket.signer_enabled():
        # Go 的顺序:先 verifier 闸,再 "signer 必须配 verifier" 闸。这里 verifier
        # 段已经拒启,能走到这说明 jwks_file 为空 —— 正好命中 Go 的第二道闸。
        logger.error(
            "ds_ticket_v2_signer_requires_verifier",
            hint="Login 需要校验 Hub allocator 返回的 RS256 票据，请配置完整的重叠期 JWKS",
        )
        return 1

    # ── biz 装配 ─────────────────────────────────────────────────────────
    login_uc = lbiz.LoginUsecase(
        repo=account_repo,
        sessions=session_repo,
        notifier=notifier,
        hub_assigner=hub_assigner,
        role_repo=role_repo,
        snowflake_node=snowflake_node,
        signer=signer,
        ds_ticket_signer=ds_signer,
        mock_hub_ds_addr=lg.mock_hub_ds_addr,
        hub_region=lg.hub.region,
        dev_skip_password=lg.dev_skip_password,
        dev_auto_register=lg.dev_auto_register,
        allowed_role_ids=set(lg.allowed_role_ids),
        dev_allow_any_role=lg.dev_allow_any_role,
        session_ttl_sec=lg.session_token_ttl_td().total_seconds(),
    )
    login_uc.set_session_generation_repo(session_gen_repo)
    login_uc.set_session_generation_enforce(lg.session_generation_enforce)

    # 登录失败 Quota(anti-abuse §6 第 4 项):账号 + IP 双维度,只对凭据失败计数。
    # Redis 未配时不启用 —— 与会话仓储同一个弱依赖边界。
    if rdb is not None:
        login_uc.set_login_rate_limiter(
            ldata.RedisLoginRateLimiter(
                rdb,
                lg.login_fail_limit,
                lg.login_fail_window_td().total_seconds(),
                lg.login_fail_lock_td().total_seconds(),
            )
        )
        logger.info(
            "login_fail_quota_ready",
            limit=lg.login_fail_limit,
            window=godur.duration_string(lg.login_fail_window_td()),
            lock=godur.duration_string(lg.login_fail_lock_td()),
        )
    else:
        logger.warning("login_fail_quota_disabled", reason="redis not configured")

    # ── ㉕ 安全开关不能静默变形 ──────────────────────────────────────────
    # 两个强制门都以「Redis 会话权威存在」为前提:enforce 的 SetRole 复核对象与
    # sjti 强制门的现行性判定都来自会话仓储。缺 Redis 时开关只会静默变形为
    # **"永不强制"** —— 安全开关必须 fail-fast,而不是装饰性地存在于 yaml 里。
    if (lg.session_generation_enforce or lg.require_ticket_sjti) and session_repo is None:
        logger.error(
            "session_enforce_requires_redis_sessions",
            session_generation_enforce=lg.session_generation_enforce,
            require_ticket_sjti=lg.require_ticket_sjti,
            hint="配置 node.redis_client(会话权威)或按 rollout 文档显式关闭强制门",
        )
        return 1
    if lg.session_generation_enforce:
        logger.info(
            "session_generation_enforce_active",
            note="SetRole 同事务复核 MySQL 会话代际;前提=全 fleet emit 且旧版本已排空",
        )
    else:
        logger.info(
            "session_generation_emit_only",
            note="Login 双写会话代际但 SetRole 不强制;滚动排空后开 login.session_generation_enforce",
        )

    login_uc.set_require_ticket_sjti(lg.require_ticket_sjti)
    if lg.require_ticket_sjti:
        logger.info(
            "ticket_sjti_binding_enforced",
            note="VerifyDSTicket 硬拒空 sjti 票;前提=全 fleet 签发面必带 sjti 且旧票已过期",
        )
    else:
        logger.info(
            "ticket_sjti_binding_compat",
            note="空 sjti 告警放行(混版兼容窗);签发面排空后开 login.require_ticket_sjti",
        )

    owner_client = None
    if lg.owner_addr:
        owner_client = lclients.GrpcOwnerClient(lg.owner_addr)
        login_uc.set_owner_client(owner_client)
        logger.info("owner_authority_enabled", owner_addr=lg.owner_addr)
    else:
        # owner_addr 空 ≠ "少一个可选增强":进场路径按 WAIT 处理(不冒充"无归属"
        # 再分配一台 DS),也就是**进场链缺了权威**,玩家进不去场景。
        logger.warning(
            "owner_addr_missing",
            warn="owner 是 §9.23 归属唯一权威;未配置时进场一律 WAIT,玩家进不去场景",
        )

    login_uc.set_require_hub_assignment_binding(lg.require_hub_assignment_binding)
    login_uc.set_role_ledger(role_ledger)
    if profile_seeder is not None:
        login_uc.set_profile_seeder(profile_seeder)

    if lg.dev_skip_password:
        logger.warning(
            "DEV_SKIP_PASSWORD_ENABLED",
            warn="password verification disabled + unknown accounts auto-provisioned; NEVER enable in prod",
        )
    if lg.dev_auto_register:
        logger.warning(
            "DEV_AUTO_REGISTER_ENABLED",
            warn="unknown accounts auto-registered on first login; NEVER enable in prod",
        )
    if lg.dev_allow_any_role:
        logger.warning(
            "DEV_ALLOW_ANY_ROLE_ENABLED",
            warn="SelectRole accepts any non-zero role_id when allowed_role_ids empty; NEVER enable in prod",
        )
    elif not lg.allowed_role_ids:
        logger.warning(
            "select_role_fail_closed",
            warn="login.allowed_role_ids empty and dev_allow_any_role false: SelectRole will reject all requests",
        )

    ticket_uc = lbiz.TicketUsecase(ds_signer, jti_repo)
    # 票据兑换点的会话现行性门**前置到 replay marker 之前**(R7 复审 P2-1)。
    ticket_uc.set_ticket_session_gate(login_uc)

    # ── Hub 票据归属绑定校验器(§9.3 五要件的兑换点核对) ──────────────────
    # 没有 Redis 就没有归属权威。此时**不能**装一个"放行"的假校验器 —— 归属
    # 不可判定必须 fail-closed(TicketUsecase._check_hub_binding 里 checker 为
    # None 且票据带完整绑定时抛 ErrUnavailable)。
    #
    # `require_binding` 只控制**无绑定旧票**的处置(滚动窗口兼容闸);带绑定的票
    # 无论开关如何都必须核对 —— 这个开关不是"要不要校验"的总闸。
    if rdb is not None:
        ticket_uc.set_hub_assignment_checker(
            lhubbinding.RedisHubAssignmentChecker(rdb),
            require_binding=lg.require_hub_assignment_binding,
        )
        logger.info(
            "hub_assignment_checker_ready",
            require_binding=lg.require_hub_assignment_binding,
        )
    else:
        # conf 校验已保证 require_hub_assignment_binding=true 时必配 redis,
        # 所以走到这里必然是 require=false 的 dev 档。
        ticket_uc.set_hub_assignment_checker(None, require_binding=False)
        logger.warning(
            "hub_assignment_checker_disabled",
            hint="未配 node.redis_client:带完整归属绑定的 Hub 票将因权威不可判定被拒(fail-closed)",
        )

    # ── ㉖㉗ v2 signer(未实现,已在 ㉓㉔ 拒启)/ ㉘ Model B ────────────────
    # ㉖㉗ 的两个条件(signer_enabled)在上面已经 return 1,走到这里必然未启用 v2。
    if cfg.ds_auth.authority_mode_redis():
        # Go 的 model_b_requires_ds_ticket_v2_signer:B1 k8s Login 只允许 RS256
        # battle 票。Python 没有 v2 签发能力 → 这个组合永远不成立,拒启。
        logger.error(
            "model_b_requires_ds_ticket_v2_signer",
            hint="B1 k8s Login 只允许 RS256 battle 票；Python 版未实现 DSTicket v2,请用 Go 版跑 login",
        )
        return 1

    # ── ㉙ DS 回调守卫 ───────────────────────────────────────────────────
    # mode 拼错必须启动即失败,不能静默回落 off —— 那等于把一道安全门悄悄关掉,
    # 而 yaml 上还写着 "enforce"。
    try:
        ds_guard = dsauth.guard_from_conf(cfg.ds_auth)
    except Exception as exc:  # noqa: BLE001
        logger.error("ds_auth_guard_init_failed", err=str(exc))
        return 1

    svc = lsvc.LoginService(login_uc, ticket_uc)
    internal_svc = lsvc.LoginInternalService(login_uc, player_no_verifier)

    # ── ㉚ Model B 在线入场权威 ─────────────────────────────────────────
    # authority_mode=redis 在 ㉘ 已经拒启;这条是**结构性**兜底:哪天 v2 做完了,
    # 权威门的另外两件(enforce guard + Redis admission checker)仍然可能缺。
    # Go 在缺任一件时打同名事件退出,这里保持同名同方向。
    if cfg.ds_auth.authority_mode_redis() and ds_admission_incomplete(ds_guard, rdb):
        logger.error(
            "ds_admission_authority_incomplete",
            hint="redis authority requires enforce guard and Redis",
        )
        return 1
    if cfg.ds_auth.authority_mode_redis():
        svc.set_redis_ds_admission_authority(
            ds_guard,
            ldsadmission.RedisDSAdmissionChecker(
                rdb,
                max_active_heartbeat_age_sec=(
                    cfg.ds_auth.active_heartbeat_max_age_td().total_seconds()
                ),
            ),
        )

    # ── gRPC / HTTP ──────────────────────────────────────────────────────
    # auth_required=False 对应 Go 的 pmw.AuthOptional():Login 本身没有 token
    # (还没签出来),Required 会让 Login 全部 401。需要身份的 RPC 在 service 层
    # 各自 fail-closed(player_id/account_id == 0 → ERR_UNAUTHORIZED)。
    grpc_server = pserver.build_grpc_server(cfg.server.grpc, auth_required=False)
    logingrpc.add_LoginServiceServicer_to_server(svc, grpc_server)
    logingrpc.add_LoginInternalServiceServicer_to_server(internal_svc, grpc_server)
    if cfg.server.grpc.enable_reflection:
        pserver.enable_reflection(
            grpc_server,
            [
                lsvc.GRPC_SERVICE_FULL_NAME,
                lsvc.INTERNAL_GRPC_SERVICE_FULL_NAME,
            ],
        )

    # login 是全仓唯一带 google.api.http 注解的服务:同一个 :21001 上既有
    # /metrics + /healthz,也有 10 个 REST 端点(与 Go 的 http.go 同形)。
    http_app = pserver.build_http_app(SERVICE_NAME)
    lrest.register(http_app, svc)

    # ★ 每条后台循环都带**显式点位名**((name, factory) 二元组)。
    # 传裸 lambda 的话 `pandora_safego_panic_recovered_total{name}` 全是 bg_anonymous:
    # 告警只能告诉你"有个后台协程死了",不能告诉你死的是补号、清理还是容量巡检。
    background = [
        (
            "login_device_sweep",
            lambda: _run_device_sweep(
                pool,
                lg.retention_mode_parsed(),
                lg.device_retention_days,
                schema=db_schema,
            ),
        ),
        ("db_capacity_guard", lambda: _run_capacity_guard(pool, db_schema)),
    ]
    if player_no_sweep_enabled:
        background.append(("login_player_no_sweep", lambda: _run_player_no_sweep(pool)))

    def _on_ready() -> None:
        logger.info(
            "service_ready",
            grpc=cfg.server.grpc.addr,
            http=cfg.server.http.addr,
            account_repo="mysql",
            session_repo=_repo_enabled(session_repo is not None),
            jti_repo=_repo_enabled(jti_repo is not None),
            locator_notifier=locator_mode,
            hub_assigner=hub_mode,
            match_resolver=match_mode,
            # 账号 / 角色分离(2026-08-18):这两项直接决定「两步登录能不能用」与
            # 「角色名是不是账号名」,起服日志里必须一眼可见,否则线上排查只能靠猜。
            role_ledger=_repo_enabled(role_ledger is not None),
            role_name_seeder=player_mode,
            require_hub_assignment_binding=lg.require_hub_assignment_binding,
            ds_auth_mode=cfg.ds_auth.mode,
            ds_auth_authority_mode=cfg.ds_auth.authority_mode,
            dev_skip_password=lg.dev_skip_password,
            dev_auto_register=lg.dev_auto_register,
            jwt_issuer=lg.jwt.issuer,
            jwt_audience=lg.jwt.audience,
            jwt_session_ttl=godur.duration_string(lg.jwt.session_ttl_td()),
            jwt_ds_ticket_ttl=godur.duration_string(lg.jwt.ds_ticket_ttl_td()),
            runtime="python",  # 灰度期用它在 Grafana 里区分两个实现
        )

    # ── ㉛ capability fence ──────────────────────────────────────────────
    # Go 在 service_ready 之后才 Acquire,这里保持同序。
    # Python 未实现 dsauthfence 运行时(capability 注册 + 陈旧回收 + writer epoch),
    # 需要它的档位一律拒启:没有 fence 的 login 会在失租后继续消费 DS 入场票,
    # 而"失租"这件事本身没有任何本地信号。
    fence_cfg, fence_enabled = cfg.capability_fence()
    if fence_enabled:
        logger.error(
            "login_ds_auth_fence_acquire_failed",
            err="dsauthfence runtime not implemented in the Python build",
            etcd_prefix=fence_cfg.etcd_prefix,
            keyset_revision=fence_cfg.keyset_revision,
            hint="需要 capability fence(ds_auth.authority_mode=redis 或 require_hub_assignment_binding)请用 Go 版跑 login",
        )
        return 1

    try:
        await pserver.run(
            service_name=SERVICE_NAME,
            grpc_server=grpc_server,
            grpc_addr=cfg.server.grpc.addr,
            http_app=http_app,
            http_addr=cfg.server.http.addr,
            http_default_port=HTTP_DEFAULT_PORT,
            on_ready=_on_ready,
            background=background,
        )
    finally:
        # 客户端 channel 逐个关掉。Go 侧是 main 的一串 defer conn.Close();
        # 漏关的表现是进程退出时 grpc.aio 打一堆 "channel not closed" 噪音,
        # 把真正的退出原因埋掉。
        for client in (notifier, hub_assigner, profile_seeder, owner_client):
            if client is None:
                continue
            with contextlib.suppress(Exception):
                await client.close()
        if rdb is not None:
            with contextlib.suppress(Exception):
                await rdb.aclose()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        return asyncio.run(_main_async(args))
    except KeyboardInterrupt:
        return 0
    except Exception:  # noqa: BLE001
        # 对应 Go 侧 app_run_failed。
        plog.get().exception("app_run_failed")
        return 1


if __name__ == "__main__":
    with contextlib.suppress(BrokenPipeError):
        sys.exit(main())
