"""Kill-Switch —— 对应 Go 侧 pkg/killswitch。

作用:运行期临时关停某个 RPC(维护中,稍后可重试),不用发版。
关停的 RPC 返回 ErrServiceDisabled(13),客户端据此提示"维护中"而不是当成故障重试。

读路径必须无锁:
    Manager.disabled() 在**每个请求**上被调用。Go 侧用 atomic.Value 存整张规则快照,
    变更时整体替换(Replace),读路径零锁。Python 侧同理 —— 用不可变 dict 的原子引用替换,
    读的时候只做一次字典查找。
    不能用"读的时候加锁"的写法:那会让每个 RPC 都过一次锁,把并发打回串行。

fail-open 还是 fail-closed:
    ⚠️ 这里刻意 **fail-open**(规则源不可用时放行)。与 §9.22 的 fail-closed 原则相反,
    是有意的:killswitch 是"临时关停"工具,它的默认状态是"不关停"。若规则源挂了就
    把所有 RPC 关掉,等于把一个运维工具变成全服故障开关。
    这与 owner 查询、准入判定那些"不确定就必须拒绝"的场景性质不同。
"""

from __future__ import annotations

import contextlib
import json
import pathlib
from types import MappingProxyType
from typing import Mapping

import yaml

from pandorapy import errcode
from pandorapy import log as plog

DEFAULT_ETCD_PREFIX = "/pandora/killswitch/"

# feature/ 前缀的键表示"功能开关"而非单个 RPC(与 Go 侧 CutPrefix("feature/") 一致)。
_FEATURE_PREFIX = "feature/"

_DEFAULT_REASON = "维护中,稍后重试"


class Manager:
    """规则快照持有者。读路径无锁。对应 Go 的 killswitch.Manager。"""

    __slots__ = ("_rules",)

    def __init__(self) -> None:
        # MappingProxyType 让快照不可变;替换整个引用是原子操作(GIL 保证单次赋值原子)。
        self._rules: Mapping[str, str] = MappingProxyType({})

    def replace(self, rules: dict[str, str]) -> None:
        """原子换上新规则快照。对应 Go 的 Replace。

        整体替换而非增量修改:增量改会让读路径看到"改了一半"的中间态
        (一个 RPC 被关了但它依赖的另一个还开着)。
        """
        normalized = {key.lstrip("/"): value for key, value in rules.items()}
        self._rules = MappingProxyType(normalized)

    def disabled(self, operation: str) -> tuple[bool, str]:
        """判断某 operation 是否被关停。返回 (是否关停, 原因)。

        operation 形如 `/pandora.login.v1.LoginService/Login`(Kratos transport.Operation()
        / gRPC 的 full method)。比对时去掉前导 "/",与 Go 侧口径一致。

        ★ 匹配优先级与 Go 的 Manager.Disabled **逐级一致**:

            ① 全局 "*"                  全服维护(慎用)
            ② 精确 method               单个 RPC
            ③ 整服 "<service>/*"        某个服务的全部 RPC
            ④ feature 组 "feature/<名>"  一组跨服务的 RPC(需先 register_feature)

        早先这里**只做第 ② 级**。后果不是"少一点能力":运维照手册写下
        `pandora.trade.v1.TradeService/*` 想关掉整个交易服,规则**加进去了、
        也生效返回了成功**,而 Python 副本一条都没挡住 —— 关停这种"出事时才用"
        的能力,失效恰好只在出事时才被发现。
        """
        op = operation.lstrip("/")
        rules = self._rules  # 只读一次引用,后续不受并发替换影响

        # ① 全局维护开关
        reason = rules.get("*")
        if reason is not None:
            return True, reason or "全服维护中,稍后重试"

        # ② 精确 method
        reason = rules.get(op)
        if reason is not None:
            return True, reason or _DEFAULT_REASON

        # ③ 整服通配 "<service>/*"
        idx = op.rfind("/")
        if idx > 0:
            svc = op[:idx]
            reason = rules.get(f"{svc}/*")
            if reason is not None:
                return True, reason or f"服务维护中: {svc}"

        # ④ feature 组:被关的键形如 "feature/<name>",展开成注册进该组的 operation 列表
        for key, why in rules.items():
            if not key.startswith(_FEATURE_PREFIX):
                continue
            name = key[len(_FEATURE_PREFIX) :]
            if feature_contains(name, op):
                return True, why or f"功能维护中: {name}"

        return False, ""

    def feature_disabled(self, feature: str) -> tuple[bool, str]:
        """功能开关查询。键形如 `feature/<name>`。"""
        reason = self._rules.get(f"{_FEATURE_PREFIX}{feature}")
        if reason is not None:
            return True, reason or _DEFAULT_REASON
        return False, ""

    def rule_count(self) -> int:
        return len(self._rules)


# ── feature 组注册表(对应 Go 的 RegisterFeature / featureContains)───────────
#
# feature 是"一组跨服务的 RPC"的别名,让运维能一键关掉"交易玩法"而不必列出
# 它涉及的十几个 method。组的成员由**代码**注册(哪些 RPC 属于哪个玩法是业务事实),
# 规则文件里只写 `feature/<名>`。
_features: dict[str, frozenset[str]] = {}


def register_feature(name: str, operations: list[str]) -> None:
    """把一组 operation 归入某 feature。各服务在 main 装配时调用。

    重复注册同名 feature 会**合并**而不是覆盖 —— 一个玩法的 RPC 天然分散在多个服务,
    覆盖语义会让"后注册的那个服务把前面几个挤掉",而且不报错。
    """
    normalized = {op.lstrip("/") for op in operations if op}
    existing = _features.get(name, frozenset())
    _features[name] = frozenset(existing | normalized)


def feature_contains(name: str, operation: str) -> bool:
    """该 operation 是否属于这个 feature 组。"""
    ops = _features.get(name)
    return bool(ops) and operation.lstrip("/") in ops


def clear_features() -> None:
    """清空注册表(测试用)。"""
    _features.clear()


# 包级默认 Manager,拦截器用它。为 None 时 fail-open 放行(见模块头注释)。
_default: Manager | None = None


def set_default(manager: Manager | None) -> None:
    global _default
    _default = manager


def default() -> Manager | None:
    return _default


def disabled(operation: str) -> tuple[bool, str]:
    """包级便捷查询。默认 Manager 为 None 时 **fail-open 放行**。"""
    mgr = _default
    if mgr is None:
        return False, ""
    return mgr.disabled(operation)


# ── 文件规则源 ────────────────────────────────────────────────────────────────


def parse_rules(data: bytes) -> dict[str, str]:
    """解析规则文件。支持 yaml 与 json(yaml 是 json 的超集,一个解析器覆盖两种)。

    形状:
        rules:
          "pandora.trade.v1.TradeService/CreateOrder": "交易维护中"
          "feature/auction": ""            # 空原因用默认文案
    """
    if not data.strip():
        return {}
    try:
        parsed = yaml.safe_load(data.decode("utf-8"))
    except (yaml.YAMLError, UnicodeDecodeError) as exc:
        raise ValueError(f"killswitch: 规则解析失败: {exc}") from exc
    if parsed is None:
        return {}
    if not isinstance(parsed, dict):
        raise ValueError("killswitch: 规则文件根节点必须是 mapping")
    raw = parsed.get("rules", parsed)
    if not isinstance(raw, dict):
        raise ValueError("killswitch: rules 必须是 mapping")
    out: dict[str, str] = {}
    for key, value in raw.items():
        # 规范化:去前导 "/",与 Manager.disabled 的比对口径一致。
        out[str(key).lstrip("/")] = "" if value is None else str(value)
    return out


class FileSource:
    """从文件读规则。对应 Go 的 killswitch fileSource。

    没有实现 watch:Go 侧用 fsnotify 监听文件变更。Python 侧要接 `watchdog`,
    但 dialogue 等第一批服务都没用 killswitch,等真正需要热更时再接
    (§15.3 不为"以后可能要"提前搭)。当前语义 = 启动时读一次。
    """

    __slots__ = ("_path", "manager")

    def __init__(self, path: str | pathlib.Path) -> None:
        self._path = pathlib.Path(path)
        self.manager = Manager()

    def load(self) -> int:
        """读一次规则。文件不存在视为"无规则"(fail-open),不是错误。

        缺文件 fail-open 是有意的:killswitch.yaml 是可选兜底文件,镜像里烘焙了一份,
        但本地联调时可能没有。缺它就把全部 RPC 关掉显然不对。
        """
        if not self._path.is_file():
            plog.get().info("killswitch_rules_absent", path=str(self._path))
            self.manager.replace({})
            return 0
        try:
            rules = parse_rules(self._path.read_bytes())
        except ValueError as exc:
            # 解析失败**保留旧快照**,不清空 —— 清空等于把所有关停规则突然放开,
            # 可能让正在维护的 RPC 重新接流量。与配置表热更的"失败保留旧配置"同理。
            plog.get().error(
                "killswitch_rules_parse_failed",
                path=str(self._path),
                err=str(exc),
                hint="保留上一份规则快照,不清空",
            )
            return self.manager.rule_count()
        self.manager.replace(rules)
        plog.get().info(
            "killswitch_rules_loaded", path=str(self._path), rule_count=len(rules)
        )
        return len(rules)


def disabled_error(reason: str) -> errcode.PandoraError:
    """构造关停错误。code = ErrServiceDisabled(13),客户端据此提示维护中。"""
    return errcode.PandoraError(errcode.ErrServiceDisabled, reason or _DEFAULT_REASON)


# ── 供 json 规则源复用(etcd 值通常是 json)──────────────────────────────────


def parse_rules_json(data: bytes) -> dict[str, str]:
    """解析 json 形式的规则(etcd 值)。与 parse_rules 的规范化口径一致。"""
    if not data.strip():
        return {}
    with contextlib.suppress(json.JSONDecodeError):
        parsed = json.loads(data.decode("utf-8"))
        if isinstance(parsed, dict):
            return {str(k).lstrip("/"): ("" if v is None else str(v)) for k, v in parsed.items()}
    raise ValueError("killswitch: json 规则解析失败")
