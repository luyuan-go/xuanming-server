"""日志字段口径校验 —— 防"Grafana 面板静默变空"。

为什么单独一个文件:
    这是整条迁移里**最容易漏、后果最隐蔽**的一项。structlog 默认把消息放在 `event`
    字段,而 Go 侧 zap 用的是 `msg`。用默认配置的话:
        - Alloy 的 stage.json 照样解析成功
        - Loki 照样收下
        - 不会有任何报错
        - 但 Grafana 面板全空,`| json | msg="xxx"` 全部落空
    要等到下一次线上排障、想查 armed_source_disconnect 之类判据时才发现判据没了。

所以字段名在这里被当作硬契约测试,而不是靠 code review。
"""

from __future__ import annotations

import datetime as _dt
import io
import json
import pathlib
import re

import pytest
import structlog

from pandorapy import log as plog

# Go 侧 pkg/log/log.go:46 的 EncoderConfig 键名。
GO_ENCODER_KEYS = {
    "TimeKey": "ts",
    "LevelKey": "level",
    "NameKey": "logger",
    "CallerKey": "caller",
    "MessageKey": "msg",
    "StacktraceKey": "stack",
}


@pytest.fixture
def captured() -> io.StringIO:
    """把 structlog 输出接到内存,便于逐字段断言。"""
    buf = io.StringIO()
    plog.setup("test-service")
    structlog.configure(
        processors=structlog.get_config()["processors"],
        wrapper_class=structlog.get_config()["wrapper_class"],
        logger_factory=structlog.PrintLoggerFactory(file=buf),
        cache_logger_on_first_use=False,
    )
    return buf


def _last_line(buf: io.StringIO) -> dict:
    lines = [ln for ln in buf.getvalue().splitlines() if ln.strip()]
    assert lines, "没有捕获到任何日志输出"
    return json.loads(lines[-1])


def test_go_encoder_keys_unchanged(repo_root: pathlib.Path) -> None:
    """先确认 Go 侧的键名没变 —— 本文件其余断言都以它为基准。

    Go 侧改了字段名而 Python 侧没跟,这条会红,提醒同步。
    """
    go_src = (repo_root / "pkg" / "log" / "log.go").read_text(encoding="utf-8")
    for go_field, expected in GO_ENCODER_KEYS.items():
        m = re.search(rf'{go_field}:\s*"(?P<v>[a-z]+)"', go_src)
        assert m, f"未从 pkg/log/log.go 解析到 {go_field}"
        assert m.group("v") == expected, (
            f"Go 侧 {go_field} 已改成 {m.group('v')!r}(本测试基准是 {expected!r})"
            f" —— 请同步 pandorapy/log.py 并更新本测试"
        )


def test_message_key_is_msg_not_event(captured: io.StringIO) -> None:
    """★ 最关键的一条:消息字段必须叫 msg,不能是 structlog 默认的 event。"""
    plog.get().info("dialogue_sessions_swept", count=3)
    record = _last_line(captured)
    assert "msg" in record, (
        "日志里没有 msg 字段 —— structlog 的 EventRenamer('msg') 没生效。"
        "后果:1449 个事件名对应的 LogQL 查询全部静默失效,Grafana 面板变空。"
    )
    assert "event" not in record, "仍然输出了 structlog 默认的 event 字段(应已改名为 msg)"
    assert record["msg"] == "dialogue_sessions_swept"
    assert record["count"] == 3


def test_required_fields_present(captured: io.StringIO) -> None:
    """ts / level / caller 必须都在。"""
    plog.get().warning("dialogue_id_conflict", dialogue_id=1)
    record = _last_line(captured)
    for key in ("ts", "level", "msg", "caller"):
        assert key in record, f"缺少字段 {key}"


# zapcore.Level.String() 的完整词表 —— level 值只能取这里面的。
# Python logging 用 warning / critical,与 zap 的 warn / fatal 不一致,必须映射。
ZAP_LEVEL_VOCABULARY = {"debug", "info", "warn", "error", "dpanic", "panic", "fatal"}


@pytest.mark.parametrize(
    ("log_method", "expected_level"),
    [
        ("debug", "debug"),
        ("info", "info"),
        ("warning", "warn"),  # ★ 不是 "warning"
        ("error", "error"),
        ("critical", "fatal"),  # ★ 不是 "critical"
    ],
)
def test_level_values_use_zap_vocabulary(
    captured: io.StringIO, log_method: str, expected_level: str
) -> None:
    """★ level 值必须用 zap 的词表,不是 Python logging 的词表。

    这条曾经是松的(`== "warning" or == "warn"`),所以没抓到真实缺陷 ——
    实测 Python 侧输出了 level="warning" 而 Go 侧是 "warn"。

    为什么这个差异是硬故障而不是风格问题:
        deploy/alloy/config.alloy 的 stage.labels 把 level **直接提成 Loki label**:
            stage.json  { expressions = { level = "level" } }
            stage.labels{ values      = { level = ""      } }
        于是 {level="warn"} 和 {level="warning"} 在 Loki 里是两个不同的 label 值。
        所有按 level="warn" 过滤的面板和告警会**静默漏掉 Python 侧的全部警告**,
        同时白白多一份重复语义的 label 基数(config.alloy 头注释要求 label 低基数)。
    """
    plog.setup("test-service", level="debug")
    buf = io.StringIO()
    structlog.configure(
        processors=structlog.get_config()["processors"],
        wrapper_class=structlog.make_filtering_bound_logger(10),  # DEBUG
        logger_factory=structlog.PrintLoggerFactory(file=buf),
        cache_logger_on_first_use=False,
    )
    getattr(structlog.get_logger(), log_method)("probe_event")
    record = _last_line(buf)
    assert record["level"] == expected_level, (
        f"{log_method}() 输出 level={record['level']!r},应为 zap 的 {expected_level!r}"
    )
    assert record["level"] in ZAP_LEVEL_VOCABULARY


def test_service_name_field_is_service_not_logger(
    captured: io.StringIO, repo_root: pathlib.Path
) -> None:
    """★ 服务名字段必须叫 service,不是 zap 的 NameKey("logger")。

    实测 Go 版真实输出(2026-08-18):
        {"level":"info", ..., "service":"dialogue", "msg":"service_starting", ...}
    Go 侧 zap 虽然把 NameKey 配成 "logger",但那个键从没被用过 —— 服务名是
    pkg/log/log.go:70 用 `"service", serviceName` 以普通字段加进去的。

    用错名字的后果:所有 `| json | service="dialogue"` 的面板和按服务分组的查询
    都会漏掉 Python 副本,且不会报错。
    """
    go_src = (repo_root / "pkg" / "log" / "log.go").read_text(encoding="utf-8")
    assert '"service", serviceName' in go_src, (
        "Go 侧不再用 service 字段承载服务名?请复核本测试与 pandorapy/log.py"
    )
    plog.get().info("service_starting")
    record = _last_line(captured)
    assert record.get("service") == "test-service", (
        f"服务名字段不是 service:{record!r}"
    )
    assert "logger" not in record, "输出了 Go 侧实际不存在的 logger 字段"


def test_duration_fields_match_go_format() -> None:
    """日志里的时长字段必须用 Go 的 time.Duration.String() 格式。

    Go 版 service_ready 打的是 "5m0s"(Duration.String()),不是 yaml 里的 "5m"。
    灰度期两个实现的日志要放在一起比对,格式不同会让"配置是否一致"需要人脑换算。
    """
    from pandorapy import godur

    cases = {
        _dt.timedelta(minutes=5): "5m0s",
        _dt.timedelta(minutes=15): "15m0s",
        _dt.timedelta(hours=1, minutes=30): "1h30m0s",
        _dt.timedelta(seconds=15): "15s",
        _dt.timedelta(0): "0s",
        _dt.timedelta(milliseconds=500): "500ms",
    }
    for td, want in cases.items():
        assert godur.duration_string(td) == want, f"{td} → {godur.duration_string(td)!r},应为 {want!r}"


def test_alloy_extracts_level_as_label(repo_root: pathlib.Path) -> None:
    """确认 Alloy 真的把 level 提成 label —— 上一条的后果依据。

    如果哪天 Alloy 改成不提 level,上面那条的严格性就可以放宽;
    在此之前它是硬约束。这条测试把"为什么严格"钉在配置事实上,而不是注释里。
    """
    alloy = (repo_root / "deploy" / "alloy" / "config.alloy").read_text(encoding="utf-8")

    # ★ 断言必须落在 **stage.labels 块内部**,不能只查两个词各自出现过。
    # 原来的写法是 `"stage.labels" in alloy and "level" in alloy` —— 而 "level"
    # 在这份配置里到处都是(注释、其它 stage、字段名),所以那条断言接近恒真:
    # 就算哪天真的把 level 从 label 里拿掉,它照样绿,而它存在的唯一意义
    # 就是回答"上面那条严格性还有没有必要"。
    block = re.search(r"stage\.labels\s*\{(.*?)\}", alloy, re.S)
    assert block, "deploy/alloy/config.alloy 里已经没有 stage.labels 块了"
    labels = set(re.findall(r"(\w+)\s*=", block.group(1)))
    assert "level" in labels, (
        f"level 已不在 Alloy 的 label 集合里(当前 {sorted(labels)})—— "
        f"请复核 test_level_values_use_zap_vocabulary 的严格性是否仍必要"
    )


def test_timestamp_format_matches_zap_iso8601(captured: io.StringIO) -> None:
    """ts 必须是 zap 的 ISO8601 格式:毫秒 3 位、时区偏移无冒号。

    Go: 2026-08-18T12:34:56.789+0800
    Python isoformat() 默认给 ...56.789000+08:00 —— 微秒 6 位、偏移带冒号,两处都不同。
    """
    plog.get().info("service_ready")
    record = _last_line(captured)
    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}[+-]\d{4}", record["ts"]
    ), (
        f"ts 格式不符 zapcore.ISO8601TimeEncoder: {record['ts']!r}"
        f"(应形如 2026-08-18T12:34:56.789+0800)"
    )


def test_caller_is_short_path(captured: io.StringIO) -> None:
    """caller 必须是短路径(最后两段),不能是 Windows 绝对路径。

    zapcore.ShortCallerEncoder 给的是 `biz/dialogue.go:88`。若这里漏出
    `F:\\work\\XuanMing-Server\\python\\...`,Grafana 那一列会完全不可读,
    而且泄露构建机路径。
    """
    plog.get().info("service_starting")
    record = _last_line(captured)
    caller = record["caller"]
    assert ":" in caller, f"caller 应含行号: {caller!r}"
    assert "\\" not in caller, f"caller 含反斜杠(未转成短路径): {caller!r}"
    assert caller.count("/") <= 1, f"caller 应最多两段路径: {caller!r}"
    assert not re.match(r"^[A-Za-z]:", caller), f"caller 是绝对路径: {caller!r}"


def test_ctx_player_id_flows_into_logs(captured: io.StringIO) -> None:
    """绑过 player_id 后,本上下文的每条日志都要带上它。

    对应 Go 的 plog.With(ctx) 展开 CtxKeyPlayerID。运维排障靠
    `| json | player_id="1001"` 过滤单个玩家的全链路,漏了这个字段就查不了。
    """
    token = plog.bind_player_id(1001)
    try:
        plog.get().info("dialogue_cross_player_access", dialogue_id=7)
        record = _last_line(captured)
        assert record.get("player_id") == 1001
    finally:
        token.var.reset(token)

    # reset 之后不应再带上 —— 否则会串到别的请求上,把 A 的日志标成 B 的。
    plog.get().info("service_ready")
    assert "player_id" not in _last_line(captured)


def test_chinese_does_not_break_output(captured: io.StringIO) -> None:
    """中文字段值不能让日志丢失(Windows cp1252 坑)。

    实测:未强制 UTF-8 时 print 含中文会抛 UnicodeEncodeError,整条日志被丢掉,
    而且如果发生在 except 分支里会把真正的故障盖掉。
    """
    plog.get().info("dialogue_tree_loaded", speaker="商店老板", table="对话/d_对话.xlsx")
    record = _last_line(captured)
    assert record["speaker"] == "商店老板"
    assert record["table"] == "对话/d_对话.xlsx"


def test_json_is_not_ascii_escaped(captured: io.StringIO) -> None:
    """中文必须原样输出,不能被转成 \\uXXXX。

    Go 的 zap JSON 编码器输出原始 UTF-8。若 Python 侧 ensure_ascii=True,
    Loki 里存的是 \\u5546\\u5e97... ,人工排障时完全不可读,
    且按中文关键字做 LogQL 行过滤(`|= "商店"`)会匹配不到。
    """
    plog.get().info("dialogue_tree_loaded", speaker="商店老板")
    raw = [ln for ln in captured.getvalue().splitlines() if ln.strip()][-1]
    assert "商店老板" in raw, f"中文被 ASCII 转义了: {raw}"
    assert "\\u" not in raw
