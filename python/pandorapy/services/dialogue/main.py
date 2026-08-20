"""Pandora dialogue 服务入口(Python 版)—— 对应 Go 侧 cmd/dialogue/main.go。

职责:NPC 对话树运行时;StartDialogue / ChooseOption / EndDialogue 三个 unary RPC。
  - 对话树从配置表加载(对话/d_对话.xlsx → configtable/dist/dialogue.json,与 UE 同源)
  - 会话状态(dialogue_id)由服务端持有,当前为单实例内存会话

阶段限制(与 Go 版同一阶段):内存会话不跨实例、进程重启即丢。多实例部署需把
SessionStore 换 Redis 版(biz / service 不动)。当前对话选项无副作用。

启动顺序**逐步对齐 Go 侧**(顺序本身是契约:日志事件名和失败点位都被运维手册引用):
 1. log.setup → 全局 logger
 2. 解析 -conf 路径,加载 yaml
 3. apply_defaults 填默认值
 4. Snowflake Node(dialogue_id 生成,node_id 来自 yaml)
 5. 配置表加载(缺目录 / 坏批次 fail-closed)→ 对话树 provider;内存会话
 6. 装配 DialogueUsecase → DialogueService → gRPC/HTTP server
 7. 启动会话过期清理任务
 8. 阻塞运行

运行:
    cd python
    .venv/Scripts/python.exe -m pandorapy.services.dialogue.main \
        -conf ../services/social/dialogue/etc/dialogue-dev.yaml
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import pathlib
import sys

# 必须最先 import:Windows 下 stdout 默认 cp1252,日志里的中文会抛 UnicodeEncodeError
# 并把整条日志丢掉(实测)。见 pandorapy/_utf8.py。
from pandorapy import _utf8  # noqa: F401
from pandorapy import config as pconfig
from pandorapy import configtable as pct
from pandorapy import godur
from pandorapy import log as plog
from pandorapy import server as pserver
from pandorapy import snowflake_etcd as psnowflake_etcd
from pandorapy.services.dialogue import biz as pbiz
from pandorapy.services.dialogue import conf as pconf
from pandorapy.services.dialogue import data as pdata
from pandorapy.services.dialogue import service as psvc

SERVICE_NAME = "dialogue"

# 会话过期清理任务的扫描周期。对应 Go 的 sessionSweepInterval。
SESSION_SWEEP_INTERVAL_SEC = 60.0

# gRPC service 全名,开 reflection 时要用(grpcurl list 靠它)。
GRPC_SERVICE_FULL_NAME = "pandora.dialogue.v1.DialogueService"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行。

    刻意用 `-conf`(单横线)而不是 argparse 惯常的 `--conf`:Go 侧 flag 包用的是
    单横线,而 run_services.ps1 / start.ps1 / K8s manifest 里全是 `-conf xxx.yaml`。
    Python 版必须能被**同一条命令行**拉起,否则那些脚本都要改。
    argparse 支持单横线长选项,所以这里能对齐。
    """
    parser = argparse.ArgumentParser(prog=f"pandora-{SERVICE_NAME}", add_help=True)
    parser.add_argument(
        "-conf",
        dest="conf",
        default="etc/dialogue-dev.yaml",
        help="config file path(与 Go 版同名同默认值)",
    )
    return parser.parse_args(argv)


async def _run_session_sweep(
    store: pdata.MemorySessionStore, interval_sec: float = SESSION_SWEEP_INTERVAL_SEC
) -> None:
    """周期清理过期会话,防止被遗弃的会话堆积。对应 Go 的 runSessionSweep。

    异常兜底对应 Go 侧的 safego.Run:单轮失败只丢本轮,下轮继续。
    没有这层兜底,一次意外异常会让清理任务**静默死掉**,而服务看起来完全正常,
    直到内存被遗弃会话吃满 —— 这正是 Go 侧压测审核【必修-6】要覆盖的点位。
    """
    logger = plog.get()
    while True:
        try:
            await asyncio.sleep(interval_sec)
            swept = store.sweep_expired(pdata.now_ms())
            if swept > 0:
                logger.info("dialogue_sessions_swept", count=swept)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 —— 兜底就是要抓全部
            logger.exception("dialogue_session_sweep_failed")


async def _main_async(args: argparse.Namespace) -> int:
    # 1. Logger
    logger = plog.setup(SERVICE_NAME)
    logger.info("service_starting", conf=args.conf)

    # 2. 加载 yaml
    try:
        conf_path = pathlib.Path(args.conf).resolve()
    except OSError as exc:
        logger.error("abs_conf_path_failed", err=str(exc))
        return 1

    try:
        cfg = pconf.Config.load(conf_path)
    except pconfig.ConfigLoadError as exc:
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except Exception as exc:  # noqa: BLE001
        # Go 侧把"读不到文件"和"结构对不上"分成 config_load_failed / config_scan_failed
        # 两个事件名。这里保持同样区分:能读到文件但解析/校验失败 = scan。
        logger.error("config_scan_failed", err=str(exc), path=str(conf_path))
        return 1

    # 注:「配了但 Python 侧没实现的功能段拒启」这道闸**不在这里** ——
    # 它挂在 pandorapy.config.BaseConf 的 pydantic 校验器上,加载配置时就抛了
    # (上面的 config_scan_failed 分支会接住)。放在这里的版本是死代码:
    # 永远轮不到它执行,而它的存在会让人以为"每个 main 都得记得调一次"。

    # 3. Snowflake(dialogue_id 生成)
    #
    # 对应 Go 的 etcdnode.MustProvideSnowflake:由 snowflake.node_id_source 二选一
    #   ""/"static" → 用 yaml 的 node.node_id(单副本 / dev 默认,不碰 etcd)
    #   "etcd"      → etcd 自动抢占 nodeID + 失租退出
    # 两条路径都是完整实现(CLAUDE.md §14.2)。
    #
    # ★ 失租**必须退出进程**,不能降级继续发号:此刻另一个副本可能已经抢到同一个
    # nodeID,继续发就是重号(§9 不变量 11),而重号在背包域表现为 DuplicateGuid
    # fail-closed 卡住玩家领取 —— 现场看到的是"玩家领不了奖",查不到这里。
    node_holder = None
    try:
        snowflake_node, node_holder = await psnowflake_etcd.provide_node(
            list(cfg.snowflake.etcd_endpoints),
            cfg.snowflake.etcd_service_name or SERVICE_NAME,
            cfg.node.node_id,
            cfg.snowflake.node_id_source,
            # 失主 = 独占权不可证明 = 继续发号就是重号,没有安全的降级 → 退出进程。
            on_lost=psnowflake_etcd.exit_process_on_lost,
            **(
                {"prefix": cfg.snowflake.etcd_prefix}
                if cfg.snowflake.etcd_prefix
                else {}
            ),
            **(
                {"lease_ttl_sec": cfg.snowflake.etcd_lease_ttl_sec}
                if cfg.snowflake.etcd_lease_ttl_sec > 0
                else {}
            ),
        )
    except ValueError as exc:
        logger.error(
            "snowflake_init_failed",
            err=str(exc),
            node_id=cfg.node.node_id,
            node_id_source=cfg.snowflake.node_id_source,
        )
        return 1
    except Exception as exc:  # noqa: BLE001 —— 抢号失败必须拒启,不能退回 static
        logger.error(
            "snowflake_nodeid_acquire_failed",
            err=str(exc),
            node_id_source=cfg.snowflake.node_id_source,
            hint="etcd 档抢不到 nodeID 时**不得**退回 static —— 那正好会与别的副本重号",
        )
        return 1

    if node_holder is not None:
        # 续约由 provide_node 内部拉起(失主默认退出进程)—— 这里不再各写一份,
        # 免得两处失主处置漂移。见 snowflake_etcd.provide_node 的说明。
        logger.info(
            "snowflake_nodeid_acquired",
            node_id=node_holder.node_id,
            source="etcd",
        )

    # 4. 对话树:与 UE 同源的 configtable dialogue 表是唯一权威。
    #    缺目录、缺表、checksum 异常、起始节点缺失 / 重复、后继节点悬空一律拒启
    #    (不退回 YAML 内联树)。
    if not cfg.config_table.dir:
        logger.error(
            "configtable_dir_required",
            hint="config_table.dir required; dialogue trees read 对话/d_对话.xlsx only",
        )
        return 1

    # yaml 里的 config_table.dir 是**相对进程工作目录**的,不是相对配置文件。
    #
    # 这是 Go 版的既有契约:run_services.ps1 用 `-WorkingDirectory $svcDir` 启动进程
    # (services/social/dialogue),所以 "../../../configtable/dist" 从服务目录往上三级
    # 才是仓库根。Python 版必须用**同一个**契约,否则同一份 yaml 两个实现解出不同路径。
    #
    # 刻意不做"找不到就换个基准再试"的兜底:那样会在某些机器上碰巧成功、某些机器上
    # 加载到错误批次,而配置表是策划数值的唯一权威,加载错批次比启动失败严重得多。
    ct_dir = pathlib.Path(cfg.config_table.dir)
    if not ct_dir.is_absolute():
        ct_dir = (pathlib.Path.cwd() / ct_dir).resolve()

    try:
        load_result = pct.load_dialogue(ct_dir)
    except pct.ConfigTableError as exc:
        logger.error(
            "configtable_load_failed",
            dir=str(ct_dir),
            err=str(exc),
            # 把 cwd 一起打出来:这个失败几乎总是"启动时工作目录不对"造成的,
            # 只报解析后的路径会让人以为是配置写错了。
            cwd=str(pathlib.Path.cwd()),
            hint="config_table.dir 相对进程工作目录;须在服务目录下启动(与 Go 版一致)",
        )
        return 1
    for warning in load_result.warnings:
        logger.warning("configtable_load_warning", warning=warning)

    assert load_result.dialogue is not None  # load_dialogue 成功时必然非 None
    tree_provider = pdata.ConfigTreeProvider(load_result.dialogue)

    # 5. 内存会话存储
    sessions = pdata.MemorySessionStore()

    # 6. 装配链
    usecase = pbiz.DialogueUsecase(
        tree_provider, sessions, cfg.dialogue.session_ttl_td()
    )
    # cellroute 未在 Python 侧实现(依赖 etcd,是本次迁移唯一的高风险项)。
    # 不注入 = 单 Cell 行为,与 Go 侧 router 为 nil 时完全一致。
    service = psvc.DialogueService(usecase, snowflake_node)

    grpc_server = pserver.build_grpc_server(cfg.server.grpc, auth_required=False)
    psvc.dialogue_pb2_grpc.add_DialogueServiceServicer_to_server(service, grpc_server)
    if cfg.server.grpc.enable_reflection:
        pserver.enable_reflection(grpc_server, [GRPC_SERVICE_FULL_NAME])

    http_app = pserver.build_http_app(SERVICE_NAME)

    def _on_ready() -> None:
        logger.info(
            "service_ready",
            grpc=cfg.server.grpc.addr,
            http=cfg.server.http.addr,
            # Go 打的是 time.Duration.String(),5 分钟是 "5m0s" 而不是 yaml 里的 "5m"。
            session_ttl=godur.duration_string(cfg.dialogue.session_ttl_td()),
            # Go 打的是 yaml 原值(相对路径),这里保持一致;解析后的绝对路径另开一个
            # 字段给排障用 —— 加字段不影响既有查询,改既有字段的值会。
            configtable_dir=cfg.config_table.dir,
            configtable_dir_resolved=str(ct_dir),
            configtable_version=load_result.version,
            dialogue_nodes=load_result.dialogue.count(),
            tree_source="configtable/dialogue",
            runtime="python",  # Go 版没有这个字段:灰度期用它在 Grafana 里区分两个实现
        )

    # 7 + 8. 双 server + 会话清理任务,阻塞到收到停止信号
    await pserver.run(
        service_name=SERVICE_NAME,
        grpc_server=grpc_server,
        grpc_addr=cfg.server.grpc.addr,
        http_app=http_app,
        http_addr=cfg.server.http.addr,
        http_default_port=21013,
        on_ready=_on_ready,
        background=[("session_sweep", lambda: _run_session_sweep(sessions))],
    )
    if node_holder is not None:
        # 正常退出:停续约并断开 etcd。**刻意不 revoke**(见 Holder.close 注释) ——
        # 立刻释放会让新副本在同一日历秒抢到同号并从 step 0 重数,逐位重号。
        await node_holder.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        return asyncio.run(_main_async(args))
    except KeyboardInterrupt:
        return 0
    except Exception:  # noqa: BLE001
        # 对应 Go 侧 app_run_failed。用 logging 而不是 structlog:走到这里 logger
        # 可能还没建好(setup 之前就抛了),得保证这条一定能落盘。
        plog.get().exception("app_run_failed")
        return 1


if __name__ == "__main__":
    with contextlib.suppress(BrokenPipeError):
        sys.exit(main())
