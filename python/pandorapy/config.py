"""配置模型 —— 与 Go 侧 pkg/config 的 yaml 口径一致,直接读现有的 etc/*.yaml。

设计约束(为什么不重新设计配置格式):
    迁移期间 Go 版和 Python 版会长期并存,同一个服务的两种实现必须能读**同一份**
    etc/xxx-dev.yaml。原因:
      - 21 份 yaml 里有大量运维已经熟悉的字段和注释,重新设计等于让运维背两套
      - deploy/ 下 55 个 K8s manifest 用 ConfigMap 覆盖主配置(gen_cluster_config.ps1
        生成的"集群版"),那套生成器只认现有字段名
      - Envoy / 端口约定(gRPC 2000x / HTTP 2100x)也钉在这些字段上
    所以这里是"照着 Go 结构体抄一份 pydantic 模型",不是设计新格式。

Duration 字段:
    Go 侧 pkg/config.Duration 支持 yaml 里直接写 "60s" / "15m" / "24h"。
    Python 侧用 _parse_duration 解析成 datetime.timedelta,语义对齐。
"""

from __future__ import annotations

import datetime as _dt
import pathlib
import re
from typing import Any

import yaml
from pydantic import BaseModel, Field, model_validator

# Go 的 time.ParseDuration 支持 ns/us/ms/s/m/h。yaml 里实际只用到 s/m/h,
# 这里把 ms 也收进来(有配置写过 "500ms"),其余按 Go 的单位表补齐。
_DURATION_RE = re.compile(r"(?P<value>\d+(?:\.\d+)?)(?P<unit>ns|us|ms|s|m|h)")
_UNIT_SECONDS = {
    "ns": 1e-9,
    "us": 1e-6,
    "ms": 1e-3,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
}


def parse_duration(raw: Any) -> _dt.timedelta:
    """把 yaml 里的 "15m" / "1h30m" / 数字秒 解析成 timedelta。

    对应 Go 侧 pkg/config.Duration 的 UnmarshalYAML。空值 → 0,由各服务
    Defaults() 兜底(和 Go 侧一样:零值不代表"无限",代表"用默认")。
    """
    if raw is None or raw == "":
        return _dt.timedelta(0)
    if isinstance(raw, _dt.timedelta):
        return raw
    if isinstance(raw, (int, float)):
        # 裸数字:Go 的 time.Duration 零值语义是纳秒,但 yaml 里从没这么写过;
        # 按秒解释更符合实际配置意图,且这里只在 Python 侧发生。
        return _dt.timedelta(seconds=float(raw))
    text = str(raw).strip()
    # ★ 负号:Go 的 time.ParseDuration("-1s") 合法,而且多个服务用**负值表示"显式关闭"**
    # (Defaults() 里判 `== 0` 才套默认值,负值原样保留)。这里不认负号的后果是
    # 同一份 yaml **Go 起得来、Python 启动即崩** —— 而那份 yaml 一个字都没错。
    # 符号在前缀上处理,不塞进正则:塞进去会让 "1h-30m" 这种畸形串也被接受。
    sign = 1
    if text.startswith(("-", "+")):
        sign = -1 if text[0] == "-" else 1
        text = text[1:].strip()
    matches = list(_DURATION_RE.finditer(text))
    if not matches:
        raise ValueError(f"无法解析 duration: {raw!r}(期望形如 15m / 30s / 1h30m / -1s)")
    # 校验整串都被吃掉,避免 "15x" 这种被静默解析成 15 而不报错。
    consumed = "".join(m.group(0) for m in matches)
    if consumed != text:
        raise ValueError(f"duration {raw!r} 含无法识别的部分(已解析 {consumed!r})")
    total = sign * sum(
        float(m.group("value")) * _UNIT_SECONDS[m.group("unit")] for m in matches
    )
    return _dt.timedelta(seconds=total)


class UnsupportedSectionError(NotImplementedError):
    """yaml 里配了 Python 侧尚未实现的功能段。

    ★ 带 `section` 与 `event` 两个字段：各服务 `main.py` 直接
    `logger.error(exc.event, section=exc.section, err=str(exc))` 即可，
    **不必自己猜是哪一段、也不必自己挑事件名**。

    为什么这件事值得一个专门的异常类：第一版只抛裸 `NotImplementedError`，
    player_locator 于是把它无条件映射成 `cellroute_init_failed`。
    等哪天加了第二个不支持的段，它会以 **cellroute 的名义**报出去 ——
    排障的人照着事件名去查 cell 路由，而真凶在别处。事件名错比没有事件名更难查。

    仍然继承 `NotImplementedError`，既有的 `except NotImplementedError` 照常接住。
    """

    __slots__ = ("section", "event")

    def __init__(self, section: str, event: str, message: str) -> None:
        self.section = section
        self.event = event
        super().__init__(message)


class GrpcConf(BaseModel):
    """对应 Go 的 pkg/config.Grpc。"""

    network: str = "tcp"
    addr: str = ""
    timeout: str = ""
    # enable_reflection:dev 开(grpcurl 联调),prod 零值 false = 关,少一个攻击面。
    # 与 Go 侧 pkg/grpcserver.MustNewServer 的行为对齐。
    enable_reflection: bool = False
    # max_conn_age:达龄 GOAWAY 让客户端重拨,滚动更新时流量能滚到新副本
    # (zero-downtime §6.2)。grpcio 侧映射到 grpc.max_connection_age_ms。
    max_conn_age: str = ""
    # max_conn_age_grace:达龄后给在途请求的收尾宽限。ds_allocator 实配 **360s**,
    # 盖过 **330s** 的 AllocateBattle 在途调用(见 services/battle/ds_allocator/etc/*.yaml)
    # —— 没有它,GOAWAY 会**砍断在途分配**。
    # 配了 max_conn_age 却留空本项时,按 Go 的行为兜底 30s(见 server.build_grpc_server)。
    # grpcio 侧映射到 grpc.max_connection_age_grace_ms。
    #
    # ★ 这两个字段此前**没有建模**。pydantic 默认 extra="ignore"(本仓刻意设成
    # allow,理由见 BaseConf),于是 yaml 里配了、Python 侧当没看见 —— 静默丢弃。
    max_conn_age_grace: str = ""
    # enable_rate_limit:Go 侧是 Kratos 的 BBR 自适应限流(过载保护)。
    #
    # ★ Python 侧**没有实现**。所以这里显式建模成字段并在装配时 fail-fast,
    # 而不是继续静默忽略:yaml 里写着 true、运维以为有过载保护、实际一点都没有,
    # 是比"没这功能"糟糕得多的状态(§14:开关打开后的分支必须是真实实现)。
    enable_rate_limit: bool = False

    def timeout_td(self) -> _dt.timedelta:
        return parse_duration(self.timeout)

    def max_conn_age_td(self) -> _dt.timedelta:
        return parse_duration(self.max_conn_age)

    def max_conn_age_grace_td(self) -> _dt.timedelta:
        return parse_duration(self.max_conn_age_grace)


class HttpConf(BaseModel):
    """对应 Go 的 pkg/config.Http。20 个服务里它只承载 /metrics。"""

    network: str = "tcp"
    addr: str = ""


class ServerConf(BaseModel):
    grpc: GrpcConf = Field(default_factory=GrpcConf)
    http: HttpConf = Field(default_factory=HttpConf)


class MySQLConf(BaseModel):
    """对应 Go 的 pkg/config.MySQLConf。

    dsn 是 go-sql-driver 格式 `user:pass@tcp(host:port)/db?params` —— 同一份 yaml
    要同时喂给 Go 版和 Python 版,所以这里**不改格式**,由 mysqlx.parse_go_dsn 解析。
    """

    dsn: str = ""
    # 必须成对配置；留空保持本地开发明文行为，中心档由 mysqlx 构造严格 SSLContext。
    tls_ca_file: str = ""
    tls_server_name: str = ""
    max_open_conns: int = 0
    max_idle_conns: int = 0
    conn_max_lifetime: str = ""
    conn_max_idle_time: str = ""
    ping_timeout: str = ""
    # 分库 DSN 列表。留空 = 单库(用 dsn)。
    shards: list[str] = Field(default_factory=list)

    def conn_max_lifetime_td(self) -> _dt.timedelta:
        return parse_duration(self.conn_max_lifetime)

    def ping_timeout_td(self) -> _dt.timedelta:
        return parse_duration(self.ping_timeout)


class SnowflakeConf(BaseModel):
    """对应 Go 的 pkg/config.SnowflakeConf —— nodeID 来源开关。

    ★ 这一段此前**没有建模**,于是 yaml 里配了 `node_id_source: etcd` 也会被
    静默丢弃(BaseConf 是 extra="allow"),服务照样按 static 起来 ——
    多副本部署时两个副本用同一个 nodeID 发号,**重号**且零信号(§9 不变量 11)。
    """

    # ""/"static" = 用 node.node_id(单副本 / dev 默认);"etcd" = 自动抢占 + 失租退出。
    node_id_source: str = ""
    etcd_endpoints: list[str] = Field(default_factory=list)
    etcd_prefix: str = ""
    # 留空用服务名。★ 共铸同一种 ID 的服务(如 inventory / mail 共铸 instance_id)
    # 必须显式共用同一个值,否则各自的空间会分到相同 nodeID。
    etcd_service_name: str = ""
    etcd_lease_ttl_sec: int = 0


class RedisConf(BaseModel):
    """对应 Go 的 pkg/config.RedisConf。

    ★ 建模之前的实测行为(2026-08-19):这一段整个落进 NodeConf 的 extra,
    `redisx.new_client(addr)` 只认单实例,而 `addrs` / `master_name` **在 Python 侧
    没有任何代码路径**。于是 Sentinel 部署常见的"只填 addrs、host 留空"喂给 Python:

        host='' → new_client('') → 实际连 127.0.0.1:6379   ← Go 侧此处 panic

    连的是本机一个根本不存在(或存在但无关)的 Redis,而进程照常起来。
    选型规则与 go-redis 的 UniversalClient 逐条一致,见 redisx.new_universal_client。
    """

    host: str = ""
    addrs: list[str] = Field(default_factory=list)
    master_name: str = ""
    username: str = ""
    password: str = ""
    db: int = 0
    # Go 的 RedisConf.DefaultTTL。Python 侧当前没有读它的地方,建模只为不落进 extra ——
    # 落进 extra 的字段"配了却不生效"且零信号,正是这一族缺陷的形状。
    default_ttl: str = ""
    dial_timeout: str = ""
    read_timeout: str = ""
    write_timeout: str = ""
    # 连接池参数:全部留空(0)= 沿用客户端库默认,opt-in(与 Go 的门禁-C 同口径)。
    pool_size: int = 0
    min_idle_conns: int = 0
    pool_timeout: str = ""
    # go-redis 专有的云厂商维护通知探测开关。redis-py 没有对应能力,
    # 建模只是为了让它不落进 extra;Python 侧读到非空值时不做任何事(见 redisx)。
    maint_notifications: str = ""

    def dial_timeout_td(self) -> _dt.timedelta:
        return parse_duration(self.dial_timeout)

    def read_timeout_td(self) -> _dt.timedelta:
        return parse_duration(self.read_timeout)

    def write_timeout_td(self) -> _dt.timedelta:
        return parse_duration(self.write_timeout)

    def pool_timeout_td(self) -> _dt.timedelta:
        return parse_duration(self.pool_timeout)

    def endpoints(self) -> list[str]:
        """解析出实际要连的地址列表。对应 Go `newUniversalClient` 的头两行。

        Go: `addrs := c.Addrs; if len(addrs) == 0 { addrs = []string{c.Host} }`
        —— 注意 Go 在两者皆空时会得到 `[""]`,由 `svc`/各服务 main 的显式校验拦下;
        这里直接返回空列表,由 `redisx.new_universal_client` 拒绝。
        """
        if self.addrs:
            return list(self.addrs)
        return [self.host] if self.host else []


class NodeConf(BaseModel):
    """对应 Go 的 pkg/config.NodeConfig(只取 Python 侧当前用得到的字段)。

    node_id 是 snowflake 的 node 段,**不是玩家选区**。同一服务的多副本必须各自唯一,
    否则发重号(CLAUDE.md §9 不变量 11)。dev 单副本填 1。

    ★ extra="allow" 与 BaseConf 同因,而且**更容易漏**:
    pydantic 默认丢弃未建模字段且**不报错**。node 段下还有 redis_client / kafka /
    lease_ttl 等一堆 Python 侧尚未建模的子段,不开 allow 就会被静默吃掉 ——
    表现是"配置明明写了却不生效",没有任何日志。
    owner 的 node.mysql_client 曾正是这样被丢掉(读到空 DSN → 直接拒启)。
    """

    node_id: int = 0
    session_expire_min: int = 0
    mysql_client: MySQLConf = Field(default_factory=MySQLConf)
    redis_client: RedisConf = Field(default_factory=RedisConf)

    model_config = {"extra": "allow"}


class ConfigTableConf(BaseModel):
    """对应 Go 的 pkg/config.ConfigTableConf。dir 指向 active 批次目录。"""

    dir: str = ""


class BaseConf(BaseModel):
    """对应 Go 的 pkg/config.Base —— 各服务私有配置继承它。"""

    server: ServerConf = Field(default_factory=ServerConf)
    node: NodeConf = Field(default_factory=NodeConf)
    snowflake: SnowflakeConf = Field(default_factory=SnowflakeConf)
    config_table: ConfigTableConf = Field(default_factory=ConfigTableConf)

    # 未在 Python 侧建模的段(locker / registry / timeouts ...)会落到这里而不是被拒绝。
    # 刻意这样:同一份 yaml 要同时喂给 Go 和 Python,Python 侧还没迁到的功能段必须能
    # 原样存在,否则 Go 版一改字段 Python 版就起不来。
    #
    # ★ 但"能原样存在"不等于"可以假装支持"。凡是**配了就会改变正确性**的段,
    # 必须显式建模并在装配时 fail-fast(见 assert_unsupported_sections):
    # cell_route 配了却按单 Cell 跑,玩家会被路由到错的 cell 且不报错。
    model_config = {"extra": "allow"}

    @model_validator(mode="after")
    def _reject_unsupported_sections(self) -> "BaseConf":
        """★ 校验挂在**模型上**,不是让每个 main 各自记得调。

        原先是暴露一个 assert_unsupported_sections() 方法由 main 调用 ——
        而实测只有 dialogue 调了,owner/main.py 没调(它是后写的)。
        "每个新 main 都记得调一行"是靠不住的:漏掉不会报错,只会静默按单 Cell 跑。
        放进 pydantic 的 after 校验器之后,**任何**服务只要加载配置就必然过这道闸。
        """
        self.assert_unsupported_sections()
        return self

    def assert_unsupported_sections(self) -> None:
        """配了 Python 侧尚未实现的功能段就**拒绝启动**。

        ★ 为什么不是"忽略 + 打个 WARN":这些段配上去是为了改变行为的。
        忽略掉之后系统行为与配置意图**不一致而且不报错** —— 运维看着 yaml
        以为 cell 路由已经生效,实际所有玩家都落在单 Cell 上。
        起不来是刺眼的,静默跑错是致命的(CLAUDE.md §14)。
        """
        extra = self.model_extra or {}
        cell_route = extra.get("cell_route") or {}
        # ★ 判据是 **mode 非空**,不是"这一段存不存在"。
        #
        # Go 的关闭态就是 `mode` 为空(pkg/config.go:63「mode 空=单 Cell 不路由」),
        # 而不是不写这一段。按"段存在"判会把 `cell_route: {mode: ""}` 这种
        # **合法的单 Cell 配置**也拒掉 —— 一个防止静默出错的闸,自己变成了
        # 让服务起不来的原因,方向就反了。
        mode = ""
        if isinstance(cell_route, dict):
            mode = str(cell_route.get("mode") or "").strip()
        if mode:
            raise UnsupportedSectionError(
                section="cell_route",
                # 与 Go 侧同名:各服务 main.go 在 cellroute 装配失败时打的就是它。
                event="cellroute_init_failed",
                message=(
                    f"配置要求 cell_route.mode={mode!r},但 Python 侧只实现了单 Cell"
                    "(pandorapy/cellroute.py "
                    "有静态表与路由算法,缺 BuildRouter 装配 / keyspace 分片 / 表热更)。"
                    "继续启动会让所有玩家静默落在单 Cell 上,与配置意图不符 —— "
                    "要么用 Go 版跑这个服务,要么先把 cellroute 装配做完。"
                ),
            )


class ConfigLoadError(Exception):
    """yaml **读取或解析**阶段的失败 —— 对应 Go 的 `c.Load()`。

    ★ 存在的理由是把两个阶段分开,而不是让 main 各自猜:

      | 阶段 | Go | 事件名 |
      |---|---|---|
      | 读文件 + 解析 yaml | `c.Load()` | `config_load_failed` |
      | 结构映射到模型 | `c.Scan(&cfg)` | `config_scan_failed` |

    移植初版 19 个 main 都写成 `except FileNotFoundError → load_failed` +
    `except Exception → scan_failed`,于是 **yaml 语法错 / 权限拒 / 编码坏 /
    根节点不是 mapping 全被报成 `config_scan_failed`**。

    这不是洁癖:事件名是 Loki 告警和运维手册的入口。运维看到 `config_scan_failed`
    会去查"哪个字段填错了",而真实原因是文件根本没读成 —— 排查方向从第一步就错。

    同一个缺口在 chat / battle_result / mission 三个服务里同时出现 = 它是**跟着
    模板复制**的,所以修在这里(共享件)而不是修三处。
    """


def load_yaml(path: str | pathlib.Path) -> dict[str, Any]:
    """读一份 yaml。读取 / 解析 / 根节点类型失败一律抛 `ConfigLoadError`。

    对应 Go 侧 `kconfig.New(file.NewSource(...)).Load()`;后续的 `model_validate`
    才对应 `Scan()`,由调用方 main 归到 `config_scan_failed`。
    """
    p = pathlib.Path(path).resolve()
    try:
        with p.open(encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
    except OSError as exc:
        # 缺文件 / 权限拒 / 是个目录 —— 都是"没读成",不是"结构不对"。
        raise ConfigLoadError(f"读取配置文件失败: {p}: {exc}") from exc
    except UnicodeDecodeError as exc:
        raise ConfigLoadError(f"配置文件不是合法 UTF-8: {p}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ConfigLoadError(f"配置文件 yaml 语法错误: {p}: {exc}") from exc
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ConfigLoadError(
            f"配置文件根节点必须是 mapping,实际是 {type(data).__name__}: {p}"
        )
    return data
