"""结构化日志 —— 与 Go 侧 pkg/log 的 zap 输出**逐字段等价**。

为什么必须逐字段对齐(这是本模块存在的唯一理由):
    整条观测链 Alloy → Loki → Grafana 是语言无关的,它只认 JSON 里的字段名。
    仓库里有 1449 个不同的 `msg` 事件名、一本 295 行的 docs/ops/player-journey-log-map.md,
    以及所有基于它们的 LogQL 查询与面板。字段名一旦漂移:
        - Alloy 的 stage.json 照样解析成功
        - Loki 照样收下
        - **不会有任何报错**
        - 但 Grafana 面板全空,LogQL 查不到东西
    这是静默失效,要等到下一次线上排障才会被发现。所以本模块的字段口径是硬契约,
    不是风格选择。改这里之前先读 pkg/log/log.go 的 EncoderConfig。

Go 侧口径(pkg/log/log.go:46 zapcore.EncoderConfig):
    TimeKey        = "ts"      ISO8601,毫秒 3 位,时区偏移无冒号(2006-01-02T15:04:05.000Z0700)
    LevelKey       = "level"   小写(LowercaseLevelEncoder)
    NameKey        = "logger"
    CallerKey      = "caller"  短路径(ShortCallerEncoder,形如 biz/dialogue.go:88)
    MessageKey     = "msg"     ← structlog 默认叫 event,必须改名,见 EventRenamer
    StacktraceKey  = "stack"

Windows 编码坑(实测 2026-08-18):
    CPython 在 Windows 上 stdout 默认走 cp1252/gbk,日志里只要出现中文(NPC 说话人
    "商店老板"、配表名"对话/d_对话.xlsx")就抛 UnicodeEncodeError 并丢掉整条日志。
    Go 的 zap 直接写 UTF-8 字节,没有这个问题。因此这里在 setup 时强制 reconfigure。
"""

from __future__ import annotations

import contextvars
import datetime as _dt
import logging
import os
import sys
from typing import Any

import structlog

# ── ctx 字段(对应 Go 侧 pkg/log 的 ContextKey)─────────────────────────────────
#
# Go 用 context.Context 携带这些值并在 plog.With(ctx) 时展开。Python 没有 ctx 传参
# 惯例,用 contextvars —— 它同样是 per-task 隔离的,asyncio 下每个请求一份,不会串。
_trace_id: contextvars.ContextVar[str] = contextvars.ContextVar("trace_id", default="")
_player_id: contextvars.ContextVar[int] = contextvars.ContextVar("player_id", default=0)
_match_id: contextvars.ContextVar[int] = contextvars.ContextVar("match_id", default=0)
_team_id: contextvars.ContextVar[int] = contextvars.ContextVar("team_id", default=0)

_CTX_VARS = (
    ("trace_id", _trace_id),
    ("player_id", _player_id),
    ("match_id", _match_id),
    ("team_id", _team_id),
)


def bind_player_id(player_id: int) -> contextvars.Token:
    """把 player_id 绑到当前上下文,之后本请求的每条日志都会带上它。

    对应 Go 的 plog.WithPlayerID(ctx, id)。返回 Token 供调用方 reset(拦截器会做)。
    """
    return _player_id.set(player_id)


def bind_trace_id(trace_id: str) -> contextvars.Token:
    """把 trace_id 绑到当前上下文。对应 Go 的 plog.WithTraceID。"""
    return _trace_id.set(trace_id)


def current_player_id() -> int:
    """读当前上下文的 player_id;未绑定返回 0(与 Go 侧 callerID 语义一致)。"""
    return _player_id.get()


# ── processors ────────────────────────────────────────────────────────────────


# 进程级服务名。由 setup() 写入,之后每条日志(含 stdlib / uvicorn 的行)都带上。
#
# 为什么必须是进程级而不是绑在某个 logger 实例上(实测缺陷,2026-08-18):
#     最初写成 `setup()` 末尾 `return get_logger().bind(service=name)` —— 于是只有
#     main.py 里用那个返回值打的行才有 service,而 biz.py / service.py 用 plog.get()
#     打的行**全都没有**:
#         {"level":"info","service":"dialogue","msg":"service_starting"}      ← 有
#         {"level":"warn","caller":"dialogue/biz.py:162","msg":"..._access"}  ← 没有
#     Go 侧不会这样:Kratos 把 "service" 绑进全局 logger,每一行都带。
#     后果同样是静默的 —— 按 service 分组/过滤的面板只能看到启动那几行。
_service_name: str = ""


def _add_ctx_fields(_logger: Any, _name: str, event_dict: dict) -> dict:
    """把服务名与 contextvars 里的链路字段展开到日志体。

    对应 Go 的 plog.With(ctx) —— 它遍历 CtxKeyTraceID/PlayerID/MatchID/TeamID 四个 key,
    外加 Kratos 全局绑定的 service。
    零值不输出,和 Go 侧一致(Go 那边 ctx.Value 取不到就不加字段)。
    """
    if _service_name:
        event_dict.setdefault("service", _service_name)
    for key, var in _CTX_VARS:
        value = var.get()
        if value:
            event_dict.setdefault(key, value)
    return event_dict


# Python logging 的 level 名 → zap 的 level 字面量。
#
# 为什么必须映射(实测 2026-08-18,这是一个真实缺陷):
#     zapcore.LowercaseLevelEncoder 输出的是 zapcore.Level.String(),枚举是
#         debug / info / warn / error / dpanic / panic / fatal
#     Python 的 logging 用的是
#         debug / info / warning / error / critical
#     两个词表在 warning 和 critical 上不一致。
#
#     而 deploy/alloy/config.alloy 的 stage.labels 把 level **直接提成 Loki label**:
#         stage.json  { expressions = { level = "level" } }
#         stage.labels{ values      = { level = ""      } }
#     于是 level="warn" 和 level="warning" 在 Loki 里是两个不同的 label 值 ——
#     所有按 {level="warn"} 过滤的面板和告警规则会**静默漏掉 Python 侧的全部警告**,
#     而且白白增加一份重复语义的 label 基数(config.alloy 头注释明确要求 label 低基数)。
_ZAP_LEVEL_NAMES = {
    "warning": "warn",
    "critical": "fatal",
}


def _zap_level(_logger: Any, _name: str, event_dict: dict) -> dict:
    """把 level 值改写成 zap 的字面量。必须排在 add_log_level 之后。"""
    level = event_dict.get("level")
    if level in _ZAP_LEVEL_NAMES:
        event_dict["level"] = _ZAP_LEVEL_NAMES[level]
    return event_dict


def _timestamper(_logger: Any, _name: str, event_dict: dict) -> dict:
    """ts 字段 —— 复刻 zapcore.ISO8601TimeEncoder 的**精确**格式。

    Go: 2006-01-02T15:04:05.000Z0700   例:2026-08-18T12:34:56.789+0800
    Python datetime.isoformat() 给的是 ...56.789000+08:00 —— 微秒 6 位、偏移带冒号,
    两处都不一样。Loki 侧按字符串存无所谓,但 Grafana 面板若按 ts 做正则/解析就会错位,
    所以这里手工拼,不用 isoformat。
    """
    now = _dt.datetime.now().astimezone()
    millis = f"{now.microsecond // 1000:03d}"
    offset = now.strftime("%z")  # 形如 +0800,已经是无冒号格式
    event_dict["ts"] = f"{now.strftime('%Y-%m-%dT%H:%M:%S')}.{millis}{offset}"
    return event_dict


def _caller(_logger: Any, _name: str, event_dict: dict) -> dict:
    """caller 字段 —— 复刻 zapcore.ShortCallerEncoder(只保留最后两段路径)。

    Go 输出形如 `biz/dialogue.go:88`。structlog 的 CallsiteParameterAdder 给的是
    完整 pathname,这里裁成同样的两段式,免得 Grafana 里一列全是 F:\\work\\... 的绝对路径。
    """
    pathname = event_dict.pop("pathname", None)
    lineno = event_dict.pop("lineno", None)
    if pathname and lineno:
        parts = str(pathname).replace("\\", "/").split("/")
        short = "/".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
        event_dict["caller"] = f"{short}:{lineno}"
    return event_dict


def _order_fields(_logger: Any, _name: str, event_dict: dict) -> dict:
    """把固定字段按 Go 侧顺序前置,业务字段跟在后面。

    纯可读性:Loki 查询与字段名无关顺序,但人直接 tail 日志时,ts/level/msg 在前
    才跟现在的 Go 日志观感一致。
    """
    head = ("ts", "level", "caller", "service", "msg")
    ordered = {k: event_dict.pop(k) for k in head if k in event_dict}
    ordered.update(event_dict)
    return ordered


def setup(service_name: str, level: str | None = None) -> structlog.BoundLogger:
    """初始化全局 logger 并返回。对应 Go 的 plog.Setup(serviceName)。

    level 缺省读环境变量 **LOG_LEVEL** —— 与 Go 侧 pkg/log 的 levelFromEnv 同名同语义。

    ★ 这个名字是运维契约,不是内部实现细节:全仓有 7 处业务注释把「对单个 pod 临时
    设 LOG_LEVEL=debug」写成标准排障手法。此前 Python 侧读的是 PANDORA_LOG_LEVEL,
    同一条手法对 Python 副本**纹丝不动**,而按 debug 过滤时只看到一半流量 ——
    灰度期两栈并存,这会直接把人引向错误结论。
    PANDORA_LOG_LEVEL 保留为兼容别名;两者都设时以 LOG_LEVEL 为准。
    """
    global _service_name
    _service_name = service_name

    # Windows cp1252 会让任何含中文的日志整条丢失,必须强制 UTF-8。
    # errors="backslashreplace" 是兜底:万一遇到不可编码字符也只是转义,不是丢日志。
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="backslashreplace")

    lvl = (
        level or os.getenv("LOG_LEVEL") or os.getenv("PANDORA_LOG_LEVEL") or "info"
    ).lower()
    numeric_level = getattr(logging, lvl.upper(), logging.INFO)

    # ── 把 stdlib logging 也接进同一条渲染链 ─────────────────────────────────
    #
    # 必须做这一步,否则第三方库的日志会以**纯文本**混进 stdout。实测 uvicorn 会打:
    #     Started server process [14248]
    #     Application startup complete.
    #     Uvicorn running on http://...
    # 这些行不是 JSON,Alloy 的 stage.json 解析不了 → 进 Loki 后没有 level/msg 标签,
    # 既污染日志流,也让"按 level 过滤"的面板漏掉这些行。
    # 同类来源还有 grpcio、以及后续要接的 kafka-python / redis-py / SQLAlchemy。
    #
    # Go 侧不存在这个问题:Kratos 把 klog 注入到各组件,全链路只有一个 logger。
    _shared_processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        _zap_level,  # warning→warn / critical→fatal,见 _ZAP_LEVEL_NAMES
        _timestamper,
        _add_ctx_fields,
    ]
    _render_processors: list = [
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.EventRenamer("msg"),
        _order_fields,
        structlog.processors.JSONRenderer(ensure_ascii=False),
    ]

    stdlib_handler = logging.StreamHandler(sys.stdout)
    stdlib_handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=_shared_processors,
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                *_render_processors,
            ],
        )
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(stdlib_handler)
    root.setLevel(numeric_level)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,          # level,小写
            _zap_level,                                  # → zap 字面量(warn 而非 warning)
            _timestamper,                                # ts
            _add_ctx_fields,                             # trace_id / player_id / ...
            structlog.processors.CallsiteParameterAdder(
                {
                    structlog.processors.CallsiteParameter.PATHNAME,
                    structlog.processors.CallsiteParameter.LINENO,
                }
            ),
            _caller,                                     # caller(短路径)
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            # ★ 关键一行:structlog 默认把消息放在 `event`,Go 侧是 `msg`。
            #   不改名 = 1449 个事件名对应的 LogQL 查询全部静默失效。
            structlog.processors.EventRenamer("msg"),
            _order_fields,
            structlog.processors.JSONRenderer(ensure_ascii=False),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, lvl.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )
    # ★ 服务名字段叫 service,**不是** zap 的 NameKey("logger")。
    #
    # 实测对比 Go 版真实输出(2026-08-18):
    #     {"level":"info","ts":"...","caller":"dialogue/main.go:63","msg":"",
    #      "service":"dialogue","msg":"service_starting","conf":"..."}
    # Go 侧 zap 的 NameKey 虽然配成 "logger",但从没被使用 —— 服务名是
    # pkg/log/log.go:70 用 `"service", serviceName` 以**普通字段**加进去的。
    # 所以线上日志里只有 service,没有 logger。
    #
    # 用错名字的后果:Grafana 里所有 `| json | service="dialogue"` 的面板和按服务
    # 分组的查询都会漏掉 Python 侧 —— 又一个不报错的静默失效。
    #
    # 另注:上面那行 Go 输出里有**两个 msg 键**(先一个空的,再一个真事件名)。
    # 那是 zap MessageKey 写空消息 + Infow("msg", ...) 又加一个同名字段造成的。
    # JSON 重复键的解析结果取决于解析器(Alloy/Loki 取最后一个,所以线上恰好正常)。
    # Python 侧不复制这个缺陷 —— 只输出一个 msg,等价于 Go 的有效值。
    return structlog.get_logger()


def get(**initial: Any) -> structlog.BoundLogger:
    """取一个 logger。对应 Go 的 plog.With(ctx) —— ctx 字段由 processor 自动展开。"""
    return structlog.get_logger().bind(**initial) if initial else structlog.get_logger()
