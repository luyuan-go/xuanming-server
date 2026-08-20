"""ds_allocator 的 **Pod UID 发布预检 / Redis 只读安全体检** —— 对应 Go 侧
`services/battle/ds_allocator/internal/poduidpreflight/` 的四个文件:

    redis_security.go   ACL 只读身份证明 + 集群拓扑取证 + 目标身份摘要(1123 行)
    scan.go             全 master SCAN 审计与发现汇总(301 行)
    redis_config.go     Redis 目标配置的规范化身份 + 无凭据 YAML 严格解析(231 行)
    audit.go            单条 BattleStorageRecord 的保守分级(145 行)

## 这道闸是干什么的

严格 Model-B 授权(§9.22 的 exact owner / ABA-safe release)上线前,必须先证明
**Redis 里每一条已经写下 exact GameServer 身份的对局镜像都带着 pod_uid**。
没有 pod_uid,回收只能按 Pod 名比对 —— 同名 Pod 重建后就是一个教科书式 ABA:
"我删掉的那台"和"现在正在服务玩家的那台"名字一样,而权威分不出来。

本包**只读**。它不写 Redis 一个字节:精确 ACL 状态来自 `ACL GETUSER`,语义探针
用 `ACL DRYRUN` 而不是真的去执行那些命令。

## 为什么"只读"还需要这么长的一套证明

因为审计结论要被当作发布依据,所以每一条"我看到的"都必须先证明"我看的是对的地方、
用的是对的身份、而且期间没人换过地方":

  ① **身份**:连上来的 ACL 用户必须**逐字段等于**专用只读契约(标志位 / 口令条数 /
     命令白名单 / key 规则 / 频道规则 / selector 数)。多一条命令、key 规则少一个
     `%R~` 前缀,这份审计就不是只读审计了。
  ② **口令强制**:同一个地址用**空凭据**必须连不上。连得上说明这台 Redis 根本没开
     认证 —— 那么"我用专用只读身份连的"这句话不成立。
  ③ **拓扑**:standalone 必须证明 `cluster_enabled=0` 且 `role=master`;cluster 必须
     两次观测完全一致,且 `CLUSTER INFO` / `NODES` / `SLOTS` 三方对 16384 个 slot 的
     归属**逐个 slot** 一致,不能有 importing/migrating。
  ④ **期间未漂移**:每个 master 在 SCAN 前后各取一次运行时身份,变了就整轮作废。

★ 头注释里 Go 写了一句必须照搬的告诫:**这些观测只能发现"看得见的漂移",不能排除
  Sentinel A→B→A 或 Redis 8.4 的原子 slot 迁移。** 激活协议必须另外持有一把
  外部强制的 failover / reshard / migration 锁,从 prepare 一直握到 rollout CAS 成功;
  本包产出的证据摘要**不是**那把锁。移植不会让这条变松。

## 与 Go 的调用关系

Go 侧本包是 library:`cmd/pod_uid_acl_cleanup` 用它的 `CanonicalReadOnlyUsername` /
`ParseReadOnlyRedisConfigYAML` / `IdentifyRedisConfig`,发布 Job 用
`ProveReadOnlyAndIdentify` + `AuditRedis`。**Go 侧本包没有任何日志输出**(全部靠
返回 error),所以 Python 侧同样不打日志、不新造 event 名 —— 新造事件名 = 两栈日志
对不上,运维按 Go 的事件名建的 Loki 查询在 Python 侧查不到任何东西。调用方
(未来的 ds_allocator main.py / 发布 Job)自己决定怎么记。

## Python 侧必须显式处理、Go 由类型系统免费给的东西

  - **uint64 / int64 边界**:Go 的 `strconv.ParseUint(...,10,64)` 越界即错;Python 的
    int 无限精度,不显式判就会把一个 2^70 的 match_id 当成合法值传下去 —— 而那个
    match 在 Go 侧根本不可能存在。
  - **`\\A...\\Z` 而不是 `^...$`**:Python 的 `$` 也匹配**末尾换行**,
    `"0123...4567\\n"` 会被判成合法的 40 位运行时身份;Go 的 `^...$`(无 `(?m)`)不会。
    这些字符串全部来自 Redis 回包,是可被写入方影响的字节。
  - **redis-py 的响应回调**:`execute_command("INFO", "server")` 会命中 `INFO` 的
    `parse_info` 回调并返回**已解析的 dict**,于是 Go 那套"逐行判断空白规范性 /
    重复字段"的闸全部失效。所以本模块把多词命令写成**单个参数**
    (`"INFO server"`)—— 回调表的键是 `"INFO"`,`"INFO server"` 命中不了,拿到的是
    原始 bulk string;而 redis-py 的 `pack_command` 会把它按空格拆回两个参数发出去
    (这正是 redis-py 自己实现 `ACL GETUSER` 等多词命令的方式)。
    `_as_text()` 对非文本回包一律 fail-closed,万一将来 redis-py 加了
    `"INFO server"` 回调,表现是**报错**而不是静默降级。

## 已知的 Go/Python 语义分叉(逐条在实现处标注,汇总见交付报告)

  - Go 的 `ForEachMaster` **并发**访问各 master,Python 侧**顺序**遍历
    (`get_primaries()`)。结论相同(发现列表最后统一排序),但没有并发,
    因此 `AuditSummary` 不需要 Go 那把 `sync.Mutex`。
  - `strings.Fields` / `strings.TrimSpace` 用的是 `unicode.IsSpace`,它**不**认
    U+001C..U+001F;Python 的 `str.split()` / `str.strip()` 认。差异会让同一个
    带 U+001C 的 ACL commands 串在 Go 侧是"一个含控制字符的 token"(拒),在
    Python 侧变成"两个干净 token"(**放行**)—— 方向是"该拒的没拒"。所以本模块
    自带 `_go_fields` / `_go_trim_space`,不用 Python 的默认切分。
  - `netip.ParseAddr` 与 Python `ipaddress` 对 IPv4-mapped IPv6 / zone 的规范形态
    不同(见 `canonical_redis_endpoint`),分叉方向是 Python **更严**(拒)。
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import ipaddress
import json
import re
import unicodedata
from typing import Any

import yaml
from pandora.ds.v1 import allocator_pb2 as dspb

from pandorapy import config as pconfig
from pandorapy import errcode
from pandorapy.services.ds_allocator import repo as dsrepo

# ── 常量:一个字符都不能改 ───────────────────────────────────────────────────

# 专用只读 ACL 身份。★ 这个串同时出现在 deploy/docker-compose.ci-db.yml 的 ACL
# 配置、tools/scripts/activate_ds_auth.ps1 与 Go 侧;改一个字母 = 审计连不上,
# 或者更糟:连上了但用的是另一个权限更大的身份。
CANONICAL_READ_ONLY_USERNAME = "pandora-pod-uid-release-preflight-ro"

# SCAN 的命名空间模式。**刻意宽**(不是 `pandora:ds:battle:{*}`):它既覆盖全部规范
# key,也让畸形的历史 key 变成**可见的 finding**,而不是被模式悄悄漏掉 ——
# 发布证明里"没扫到"和"扫到了但形状不对"是两回事。
BATTLE_SCAN_PATTERN = "pandora:ds:battle:*"

# 分级(audit.go 的四个 Category 常量,逐字节相同)。
CATEGORY_EXACT_IDENTITY = "exact_identity"
CATEGORY_ALLOCATION_UNCERTAIN = "allocation_uncertain"
CATEGORY_NO_PHYSICAL_IDENTITY = "no_physical_identity"
CATEGORY_UNSAFE = "unsafe"

# 专用只读身份的**精确**命令白名单。顺序无所谓(比较前会排序去重),内容必须逐字相同。
# ★ 没有 `select`:全局要求 db=0,于是这份白名单与拓扑无关,可以被机械 review。
# ★ 没有 `+@read` 这类**类目**授权:类目的成员随 Redis 版本变化,今天只读的类目
#   明天可能多出一条写命令,而 ACL 文本一个字没改。只允许逐条命令。
CANONICAL_READ_ONLY_ACL_COMMANDS = (
    "-@all",
    "+ping",
    "+get",
    "+scan",
    "+info",
    "+acl|whoami",
    "+acl|dryrun",
    "+acl|getuser",
    "+cluster|myid",
    "+cluster|shards",
    "+cluster|slots",
    "+cluster|info",
    "+cluster|nodes",
)

# Redis Cluster 的 slot 总数。Go 侧写死 16384(协议常量,不是配置)。
CLUSTER_SLOT_COUNT = 16384

# ── 正则:一律 `\A...\Z`(见模块头)───────────────────────────────────────────

# Redis 运行时身份:CLUSTER MYID 的 node id / INFO server 的 run_id,都是 40 位小写 hex。
_REDIS_RUNTIME_ID_RE = re.compile(r"\A[0-9a-f]{40}\Z")
# 对外发布的目标身份摘要。
_TARGET_IDENTITY_RE = re.compile(r"\Asha256:[0-9a-f]{64}\Z")
# ACL GETUSER 返回的口令哈希(SHA-256 hex)。
_REDIS_PASSWORD_HASH_RE = re.compile(r"\A[0-9a-f]{64}\Z")
# 规范 battle key。`{...}` 是 Cluster hashtag 的**字面**花括号,不是占位符。
_BATTLE_RECORD_KEY_RE = re.compile(r"\Apandora:ds:battle:\{([0-9]+)\}\Z")
# 十进制整数(等价于 Go `strconv.ParseUint(s, 10, N)` 对**字符集**的要求:
# 不接受正负号、空白、下划线、0x 前缀)。
_DECIMAL_RE = re.compile(r"\A[0-9]+\Z")

# uint64 / int64 / uint32 边界(Go 的类型系统免费提供,Python 必须显式判)。
UINT64_MAX = dsrepo.UINT64_MAX
INT64_MAX = dsrepo.INT64_MAX
UINT32_MAX = (1 << 32) - 1


class PreflightError(errcode.PandoraError):
    """对应 Go 侧本包里的裸 `fmt.Errorf(...)`。

    ★ 码是 `ErrUnknown`(=1),理由与 `repo.BattleDataError` 完全相同:Go 的
      `fmt.Errorf` 不带 errcode,`errcode.As(err)` 找不到 `*errcode.Error` 时回落
      `ErrUnknown`。在这里挑一个"看起来更贴切"的码 = 同一次预检失败在两栈上呈现
      成两种语义,而两边都不报错。

    ★ `__slots__ = ()`:本包**没有** "(value, err) 双返回且失败时 value 仍有意义"
      的地方 —— Go 的每个失败分支返回的都是零值结构体,`AuditSummary` 则是**入参**
      (证据不会随异常丢失)。所以没有需要随异常带回的证据字段。将来若要带,
      必须像 `repo.BattleActiveIndexError` 那样**声明进 `__slots__`**,
      绝不用 `setattr`:后者能写进去(Exception 自带 `__dict__`),但拼错一个字母
      不会报错,读的那侧永远拿到默认值。
    """

    __slots__ = ()

    def __init__(self, msg: str = "", *args: object, cause: BaseException | None = None) -> None:
        super().__init__(errcode.ErrUnknown, msg, *args, cause=cause)


# ── Go 语义的文本原语 ────────────────────────────────────────────────────────

# Go `unicode.IsSpace` 的**完整**定义域:ASCII 六个 + U+0085(NEL) + U+00A0(NBSP)
# + Unicode Z 类(Zs/Zl/Zp)。刻意不用 Python 的 `str.isspace()` —— 它多认
# U+001C..U+001F(文件/组/记录/单元分隔符),见模块头的分叉说明。
_GO_ASCII_SPACE = "\t\n\v\f\r \x85\xa0"


def _go_is_space(ch: str) -> bool:
    return ch in _GO_ASCII_SPACE or unicodedata.category(ch) in ("Zs", "Zl", "Zp")


def _go_is_control(ch: str) -> bool:
    """Go `unicode.IsControl` = Unicode Cc 类。

    ★ 不能写成 `category[0] == "C"`:那会把 Cf(如 U+00AD 软连字符)也算进去,
      而 Go 不算 —— 于是 Go 写得进的值 Python 读出来判非法。
    """
    return unicodedata.category(ch) == "Cc"


def _go_trim_space(text: str) -> str:
    """`strings.TrimSpace` 的等价实现(按 Go 的空白定义域两端裁剪)。"""
    start = 0
    end = len(text)
    while start < end and _go_is_space(text[start]):
        start += 1
    while end > start and _go_is_space(text[end - 1]):
        end -= 1
    return text[start:end]


def _go_fields(text: str) -> list[str]:
    """`strings.Fields` 的等价实现(按 Go 的空白定义域切分,丢弃空片段)。"""
    out: list[str] = []
    current: list[str] = []
    for ch in text:
        if _go_is_space(ch):
            if current:
                out.append("".join(current))
                current = []
        else:
            current.append(ch)
    if current:
        out.append("".join(current))
    return out


def _go_contains_space_or_control(text: str) -> bool:
    """Go 里反复出现的 `strings.ContainsFunc(s, IsControl || IsSpace)`。"""
    return any(_go_is_control(ch) or _go_is_space(ch) for ch in text)


def _quote_go(value: str) -> str:
    """近似 Go 的 `%q`(双引号 + 转义)。

    与 `repo._quote_go` 同形(`json.dumps` 而不是 Python 的 `repr`,后者是**单**
    引号)。刻意复制而不是 import 私有符号:一行的等价实现,比跨模块摸私有更稳。
    """
    return json.dumps(value, ensure_ascii=False)


def _err_text(exc: BaseException) -> str:
    """取错误的**消息本体**,用于拼进 Go 的 `%w` 包装位。

    ★ 必须用 `PandoraError.msg` 而不是 `str(exc)`:后者是
      `f"errcode={code} {msg}"`,直接拼进去会让两栈的错误文本(以及 finding 的
      reason 字段)差一段 `errcode=1 ` 前缀 —— 而 finding.reason 是发布证据的一部分。
    """
    if isinstance(exc, errcode.PandoraError):
        return exc.msg
    return str(exc)


def _as_text(raw: Any) -> str:
    """把 redis 回包规范成 str —— 对应 go-redis `*redis.Cmd.Text()`。

    ★ 非 bytes/str 一律 fail-closed。这条不是防御性冗余:redis-py 会对 `INFO` /
      `CLUSTER INFO` 等命令**自动套响应回调**返回 dict,而本模块所有逐行规范性
      检查都建立在"拿到的是原始文本"之上。真拿到 dict 时必须报错,不能降级放行。
    """
    if isinstance(raw, str):
        return raw
    if isinstance(raw, (bytes, bytearray, memoryview)):
        try:
            return bytes(raw).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise PreflightError("Redis returned a non-UTF-8 reply", cause=exc) from exc
    raise PreflightError("Redis returned a non-text reply")


async def _do_text(node: Any, *args: Any) -> str:
    """执行一条命令并取原始文本。args[0] 若是多词命令必须写成**单个**参数
    (如 `"INFO server"`),否则会命中 redis-py 的响应回调(见模块头)。"""
    return _as_text(await node.execute_command(*args))


async def _try_text(node: Any, *args: Any) -> tuple[str, BaseException | None]:
    """Go 的 `(string, error)` 双返回在 Python 里的等价形态。

    ★ `CancelledError` 必须**穿透**(紧邻 `except BaseException` 之前):吞掉取消
      会让关停时的任务永远不退,而 asyncio 的取消是通过异常传递的。
    """
    try:
        return await _do_text(node, *args), None
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001 —— 与 Go 的 err 分支一一对应
        return "", exc


# ── 摘要原语(跨语言必须逐字节一致)────────────────────────────────────────


def length_prefixed_sha256(parts: list[str]) -> bytes:
    """长度前缀 SHA-256。对应 Go 的 `lengthPrefixedSHA256`。

    ★ 每段先写 8 字节**大端的字节长度**再写内容。这不是装饰:不加长度前缀时
      `["ab","c"]` 与 `["a","bc"]` 的摘要相同 —— 于是"两个 endpoint"和"一个更长的
      endpoint"可以撞出同一个目标身份,而目标身份正是发布证据的锚点。

    ★ 长度取的是 **UTF-8 字节数**(Go 的 `len(string)`),不是 Python 的字符数。
    """
    h = hashlib.sha256()
    for part in parts:
        encoded = part.encode("utf-8")
        h.update(len(encoded).to_bytes(8, "big"))
        h.update(encoded)
    return h.digest()


def digest_strings(prefix: str, values: list[str]) -> str:
    """`sha256:` + 十六进制。对应 Go 的 `digestStrings`(prefix 也进摘要,做域分隔)。"""
    return "sha256:" + length_prefixed_sha256([prefix, *values]).hex()


def runtime_master_set_digest(ids: list[str]) -> str:
    """master 运行时身份集合的摘要。对应 Go 的 `runtimeMasterSetDigest`。

    空集合 / 非规范 / 重复一律报错 —— 这三种情况下"集合摘要"没有意义,而调用方
    会把它当作"我确实看全了这些 master"的证据。
    """
    if not ids:
        raise PreflightError("Redis master identity set is empty")
    canonical = sorted(ids)
    for i, node_id in enumerate(canonical):
        if not _REDIS_RUNTIME_ID_RE.fullmatch(node_id):
            raise PreflightError("Redis master identity set is non-canonical")
        if i > 0 and canonical[i - 1] == node_id:
            raise PreflightError("Redis master identity set contains a duplicate")
    return digest_strings("pod-uid-preflight-master-set-v1", canonical)


def valid_target_identity(value: str) -> bool:
    """对应 Go 的 `ValidTargetIdentity`。"""
    return isinstance(value, str) and _TARGET_IDENTITY_RE.fullmatch(value) is not None


# ══════════════════════════════════════════════════════════════════════════
# redis_config.go —— Redis 目标配置的规范化身份
# ══════════════════════════════════════════════════════════════════════════


@dataclasses.dataclass(frozen=True)
class RedisConfigIdentity:
    """只绑定**连接路由**,不含凭据、不含明文内网地址。

    于是激活流程可以拿"写者快照"与"另行投递的只读预检快照"对摘要,而**不必**把
    写者口令交给审计 Job。
    """

    digest: str
    topology: str


def configured_redis_topology(rc: Any) -> str:
    """对应 Go 的 `configuredRedisTopology`。

    ★ 判 cluster 用的是**原始 `addrs` 的长度**,不是去重规范化后的结果 ——
      必须与 `redis.NewUniversalClient` 的选型逐条一致。写成"去重后 > 1"会让
      `addrs: [a, a]` 在选型上被当 standalone、而 Go 侧建的是 ClusterClient,
      两栈从此连的是不同形态的客户端。
    """
    if rc.master_name != "":
        return "sentinel"
    if len(rc.addrs) > 1:
        return "cluster"
    return "standalone"


def _split_host_port(hostport: str) -> tuple[str, str]:
    """`net.SplitHostPort` 的逐条等价实现(含 IPv6 方括号规则)。

    Python 的 `str.rsplit(":", 1)` **不是**等价物:它会把 `::1:6379` 拆成
    `("::1", "6379")` 而 Go 报 "too many colons"(未加方括号的 IPv6 是歧义的)。
    这里失败一律抛 ValueError,由调用方翻成 Go 的那句统一错误。
    """
    i = hostport.rfind(":")
    if i < 0:
        raise ValueError("missing port")
    j = 0
    k = 0
    if hostport[0] == "[":
        end = hostport.find("]")
        if end < 0:
            raise ValueError("missing ']'")
        if end + 1 == len(hostport):
            raise ValueError("missing port")
        if end + 1 != i:
            if hostport[end + 1] == ":":
                raise ValueError("too many colons")
            raise ValueError("missing port")
        host = hostport[1:end]
        j, k = 1, end + 1
    else:
        host = hostport[:i]
        if ":" in host:
            raise ValueError("too many colons")
    if "[" in hostport[j:]:
        raise ValueError("unexpected '['")
    if "]" in hostport[k:]:
        raise ValueError("unexpected ']'")
    return host, hostport[i + 1 :]


def _join_host_port(host: str, port: str) -> str:
    """`net.JoinHostPort`:host 含 `:` 时补方括号。"""
    if ":" in host:
        return "[" + host + "]:" + port
    return host + ":" + port


def canonical_redis_endpoint(endpoint: str) -> str:
    """把 endpoint 规范化;调用方再与原串比对,不等即判非规范。

    对应 Go 的 `canonicalRedisEndpoint`。三段判据:
      ① `host:port` 结构必须成立;
      ② 端口是 1..65535 的**规范十进制**(`06379` 因回写不等被拒);
      ③ host 要么是**规范形态**的 IP,要么是全小写 DNS 名;
         全数字的 DNS 名(如 `2130706433`)一律拒 —— 那是 IP 的非规范简写,
         两栈 / 不同解析器对它的结论可能不同。

    ★ 与 Go 的窄边分叉(方向都是 Python **更严**,即拒掉 Go 会接受的写法):
      - IPv4-mapped IPv6:`netip` 打印 `::ffff:1.2.3.4`,Python 打印 `::ffff:102:304`,
        于是回写比对不等 → Python 拒。
      这类地址不该出现在生产 Redis 配置里;真出现时表现是**启动被拒**(可见),
      不是静默放行。
    """
    try:
        host, port_text = _split_host_port(endpoint)
    except ValueError as exc:
        raise PreflightError("endpoint must be canonical host:port", cause=exc) from exc
    if host == "" or port_text == "":
        raise PreflightError("endpoint must be canonical host:port")
    if not _DECIMAL_RE.fullmatch(port_text):
        raise PreflightError("endpoint has invalid port")
    port = int(port_text)
    # Go: ParseUint(portText, 10, 16) 越界即错 —— Python 必须显式判上界。
    if port == 0 or port > 0xFFFF or str(port) != port_text:
        raise PreflightError("endpoint has invalid port")

    try:
        address: Any = ipaddress.ip_address(host)
    except ValueError:
        address = None
    if address is not None:
        host = str(address)
    else:
        # Go 的 `len(host) > 253` 是**字节**长度。
        if len(host.encode("utf-8")) > 253:
            raise PreflightError("endpoint has invalid DNS host")
        all_numeric = True
        for label in host.split("."):
            if label == "" or len(label) > 63 or label[0] == "-" or label[-1] == "-":
                raise PreflightError("endpoint has invalid DNS host")
            for ch in label:
                if not ("0" <= ch <= "9"):
                    all_numeric = False
            for ch in label:
                if not ("a" <= ch <= "z") and not ("0" <= ch <= "9") and ch != "-":
                    raise PreflightError("endpoint has invalid DNS host")
        if all_numeric:
            raise PreflightError("endpoint contains a non-canonical IP address")
    return _join_host_port(host, port_text)


def normalized_effective_endpoints(rc: Any) -> list[str]:
    """规范化、去重校验并排序后的实际连接地址。对应 Go 的 `normalizedEffectiveEndpoints`。

    ★ **不要**改用 `config.RedisConf.endpoints()`:那个方法在 host / addrs 皆空时
      返回 `[]`,而 Go 在这里得到的是 `[""]` —— 于是 Go 走进"空 endpoint"分支报错,
      Python 若返回空列表就会掉进另一条分支("no endpoints"),更糟的是若哪天
      调用方把空列表当"无需校验"就直接放行了。这里逐字照抄 Go 的取值。
    """
    raw = list(rc.addrs) if rc.addrs else [rc.host]
    unique: set[str] = set()
    result: list[str] = []
    for endpoint in raw:
        if (
            not isinstance(endpoint, str)
            or endpoint == ""
            or endpoint != _go_trim_space(endpoint)
            or endpoint != endpoint.lower()
            or _go_contains_space_or_control(endpoint)
        ):
            raise PreflightError("Redis target contains an empty or non-canonical endpoint")
        try:
            canonical = canonical_redis_endpoint(endpoint)
        except asyncio.CancelledError:
            raise
        except BaseException:  # noqa: BLE001 —— Go 把 err 与"不等"折进同一句
            raise PreflightError(
                "Redis target contains an empty or non-canonical endpoint"
            ) from None
        if canonical != endpoint:
            raise PreflightError("Redis target contains an empty or non-canonical endpoint")
        if endpoint in unique:
            raise PreflightError("Redis target contains a duplicate endpoint")
        unique.add(endpoint)
        result.append(endpoint)
    if not unique:
        raise PreflightError("Redis target contains no endpoints")
    result.sort()
    return result


def identify_redis_config(rc: Any) -> RedisConfigIdentity:
    """对应 Go 的 `IdentifyRedisConfig`。

    ★ 强制 `db=0` 的两个理由(照抄 Go 注释):Redis Cluster 只有 db 0;统一要求 db 0
      还能把 `SELECT` 挡在专用 ACL 白名单之外,于是发布身份有**一份与拓扑无关、
      可机械 review 的**命令清单。
    """
    db = rc.db
    if not isinstance(db, int) or isinstance(db, bool):
        raise PreflightError("pod_uid preflight Redis target must use db=0")
    # Go 的 rc.DB 是 uint32,负数 / 越界在类型上不可能;Python 必须显式判 ——
    # 不判的话 `db=-0` 之类的写法会先通过 `!= 0` 再被格式化进摘要。
    if db < 0 or db > UINT32_MAX:
        raise PreflightError("pod_uid preflight Redis target must use db=0")
    if db != 0:
        raise PreflightError("pod_uid preflight Redis target must use db=0")
    endpoints = normalized_effective_endpoints(rc)
    master_name = rc.master_name
    if (
        not isinstance(master_name, str)
        or master_name != _go_trim_space(master_name)
        or _go_contains_space_or_control(master_name)
    ):
        raise PreflightError("Redis sentinel master_name is non-canonical")
    topology = configured_redis_topology(rc)
    parts = [
        "pod-uid-release-preflight-config-v1",
        topology,
        master_name,
        str(db),
        str(len(endpoints)),
        *endpoints,
    ]
    return RedisConfigIdentity(
        digest="sha256:" + length_prefixed_sha256(parts).hex(), topology=topology
    )


# 只读快照允许出现的字段。**刻意不含 username / password** —— Go 用
# `decoder.KnownFields(true)` 让这两个字段"即使值为空串也被拒",凭据只能走两个
# 专用的 Secret 环境变量。少写一个字段名 = 那个字段被拒(fail-closed);
# 多写 username/password = 凭据可以从 YAML 里流进审计 Job(权限洞)。
_READ_ONLY_REDIS_FIELDS = (
    "host",
    "db",
    "default_ttl",
    "dial_timeout",
    "read_timeout",
    "write_timeout",
    "addrs",
    "master_name",
    "maint_notifications",
)

# 写者快照按**宽松**规则解析(Go 用 `yaml.Unmarshal`,未知字段直接忽略),
# 但已知字段的类型仍然要对 —— 只取这几个进 RedisConf。
_WRITER_REDIS_STRING_FIELDS = ("host", "master_name", "username", "password", "maint_notifications")


def _yaml_documents(body: bytes) -> list[Any]:
    try:
        return list(yaml.safe_load_all(body))
    except yaml.YAMLError as exc:
        raise PreflightError("YAML is malformed", cause=exc) from exc


def _require_str(value: Any, message: str) -> str:
    """YAML 标量 → str。`null` 按 Go 的零值语义当空串。"""
    if value is None:
        return ""
    if not isinstance(value, str):
        raise PreflightError(message)
    return value


def _require_uint32(value: Any, message: str) -> int:
    if value is None:
        return 0
    if not isinstance(value, int) or isinstance(value, bool):
        raise PreflightError(message)
    if value < 0 or value > UINT32_MAX:
        raise PreflightError(message)
    return value


def _require_duration_text(value: Any, message: str) -> str:
    """严格 duration 标量。对应 Go 的 `strictConfigDuration.UnmarshalYAML`。

    Go 只接受 `!!str` / `!!int` / `!!null` 三种标量,其余(map/list/bool)直接拒;
    字符串还要能被 `time.ParseDuration` 解析。这里用 `config.parse_duration` 做
    同一件事,解析不了就拒 —— 拒的方向与 Go 一致。

    ★ 已知窄边分叉:裸整数在 Go 是**纳秒**、在 `config.parse_duration` 是**秒**。
      本模块只用它做**合法性校验**(超时值不进任何摘要、不影响审计结论),
      所以分叉在这里是惰性的;真要读这些超时值的地方必须自己再确认口径。
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        raise PreflightError(message)
    if isinstance(value, int):
        return str(value)
    if not isinstance(value, str):
        raise PreflightError(message)
    if value != "":
        try:
            pconfig.parse_duration(value)
        except (ValueError, TypeError) as exc:
            raise PreflightError(message, cause=exc) from exc
    return value


def parse_read_only_redis_config_yaml(body: bytes) -> pconfig.RedisConf:
    """无凭据的严格 YAML 解析器。对应 Go 的 `ParseReadOnlyRedisConfigYAML`。

    四道闸,每道都是 fail-closed:
      ① 空文档拒;
      ② **恰好一个** YAML 文档(尾随 `---` 也算第二个);
      ③ 未知字段一律拒(含 `username` / `password`,**即使值是空串**);
      ④ `maint_notifications` 必须**恰好**是 `disabled`,且目标本身通过
         `identify_redis_config` 的全部规范性校验。
    """
    if not body:
        raise PreflightError("empty YAML")
    documents = _yaml_documents(body)
    if len(documents) == 0:
        raise PreflightError("empty YAML")
    if len(documents) > 1:
        raise PreflightError("multiple YAML documents")
    document = documents[0]
    if document is None:
        raise PreflightError("empty YAML")
    if not isinstance(document, dict):
        raise PreflightError("YAML root must be a mapping")
    for key in document:
        if key != "node":
            raise PreflightError("YAML contains an unknown field")
    node = document.get("node")
    if node is None:
        node = {}
    if not isinstance(node, dict):
        raise PreflightError("node must be a mapping")
    for key in node:
        if key != "redis_client":
            raise PreflightError("YAML contains an unknown field")
    raw = node.get("redis_client")
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise PreflightError("redis_client must be a mapping")
    for key in raw:
        if key not in _READ_ONLY_REDIS_FIELDS:
            raise PreflightError("YAML contains an unknown field")

    addrs_raw = raw.get("addrs")
    if addrs_raw is None:
        addrs: list[str] = []
    elif isinstance(addrs_raw, list):
        addrs = [_require_str(item, "addrs must be a list of strings") for item in addrs_raw]
    else:
        raise PreflightError("addrs must be a list of strings")

    rc = pconfig.RedisConf(
        host=_require_str(raw.get("host"), "host must be a string"),
        addrs=addrs,
        master_name=_require_str(raw.get("master_name"), "master_name must be a string"),
        db=_require_uint32(raw.get("db"), "db must be a non-negative integer"),
        default_ttl=_require_duration_text(raw.get("default_ttl"), "default_ttl is invalid"),
        dial_timeout=_require_duration_text(raw.get("dial_timeout"), "dial_timeout is invalid"),
        read_timeout=_require_duration_text(raw.get("read_timeout"), "read_timeout is invalid"),
        write_timeout=_require_duration_text(raw.get("write_timeout"), "write_timeout is invalid"),
        maint_notifications=_require_str(
            raw.get("maint_notifications"), "maint_notifications must be a string"
        ),
    )
    if rc.maint_notifications != "disabled":
        raise PreflightError("maint_notifications must be disabled")
    identify_redis_config(rc)
    return rc


def _decode_redis_config_yaml(body: bytes) -> pconfig.RedisConf:
    """写者快照的**宽松**解析。对应 Go 的 `decodeRedisConfigYAML`。

    未知字段忽略(写者配置里本来就有一堆本包不关心的段),但已知字段类型必须对;
    只取 host / addrs / master_name / db 进 RedisConf —— 与 Go 的
    `redisConfigYAML.redisConf()` 完全一致,凭据字段读到就丢,**不往下传**。
    """
    if not body:
        raise PreflightError("empty YAML")
    documents = _yaml_documents(body)
    if len(documents) != 1:
        # Go 的 yaml.Unmarshal 对多文档同样报错。
        raise PreflightError("writer YAML must contain exactly one document")
    document = documents[0]
    if document is None:
        return pconfig.RedisConf()
    if not isinstance(document, dict):
        raise PreflightError("YAML root must be a mapping")
    node = document.get("node")
    if node is None:
        return pconfig.RedisConf()
    if not isinstance(node, dict):
        raise PreflightError("node must be a mapping")
    raw = node.get("redis_client")
    if raw is None:
        return pconfig.RedisConf()
    if not isinstance(raw, dict):
        raise PreflightError("redis_client must be a mapping")
    for field in _WRITER_REDIS_STRING_FIELDS:
        _require_str(raw.get(field), "writer field must be a string")
    addrs_raw = raw.get("addrs")
    if addrs_raw is None:
        addrs: list[str] = []
    elif isinstance(addrs_raw, list):
        addrs = [_require_str(item, "addrs must be a list of strings") for item in addrs_raw]
    else:
        raise PreflightError("addrs must be a list of strings")
    return pconfig.RedisConf(
        host=_require_str(raw.get("host"), "host must be a string"),
        addrs=addrs,
        master_name=_require_str(raw.get("master_name"), "master_name must be a string"),
        db=_require_uint32(raw.get("db"), "db must be a non-negative integer"),
    )


def compare_redis_config_yaml(writer_yaml: bytes, read_only_yaml: bytes) -> RedisConfigIdentity:
    """跨快照机械闸。对应 Go 的 `CompareRedisConfigYAML`。

    ★ 五条错误消息**刻意不带原因**(`from None` 抑制异常链):Go 在这里丢弃了内层
      error,原因是输入文档里含写者口令和内网地址 —— 一旦把内层错误拼进消息,
      调用方一记日志就把口令写进了 Loki。Go 有专门的测试断言错误串里不含口令,
      Python 侧的 `from None` 同时切断了 traceback 里的 `__cause__` 链。
      **调用方绝不能记录输入文档本身。**
    """
    try:
        writer = _decode_redis_config_yaml(writer_yaml)
    except asyncio.CancelledError:
        raise
    except BaseException:  # noqa: BLE001
        raise PreflightError("writer Redis snapshot is invalid") from None
    try:
        read_only = parse_read_only_redis_config_yaml(read_only_yaml)
    except asyncio.CancelledError:
        raise
    except BaseException:  # noqa: BLE001
        raise PreflightError("read-only Redis snapshot is invalid") from None
    try:
        writer_identity = identify_redis_config(writer)
    except asyncio.CancelledError:
        raise
    except BaseException:  # noqa: BLE001
        raise PreflightError("writer Redis target is invalid") from None
    try:
        read_only_identity = identify_redis_config(read_only)
    except asyncio.CancelledError:
        raise
    except BaseException:  # noqa: BLE001
        raise PreflightError("read-only Redis target is invalid") from None
    if writer_identity != read_only_identity:
        raise PreflightError(
            "writer and read-only Redis snapshots target different normalized identities"
        )
    return writer_identity


# ══════════════════════════════════════════════════════════════════════════
# redis_security.go —— ACL 只读身份证明 + 拓扑取证
# ══════════════════════════════════════════════════════════════════════════


@dataclasses.dataclass(frozen=True)
class RedisTargetIdentity:
    """可以安全写进激活日志的目标身份:绑定"规范化配置目标 + 运行时身份",
    但不含任何凭据、不含明文内网地址。"""

    digest: str
    topology: str
    nodes: int
    master_set_digest: str
    topology_digest: str


@dataclasses.dataclass(frozen=True)
class ACLUserSnapshot:
    """`ACL GETUSER` 的规范化快照。

    ★ 只保留口令**条数**,绝不保留哈希本身 —— Go 有专门测试断言快照里搜不到哈希。
      这条不是洁癖:`ACL GETUSER` 能读出这台 Redis 上**每一个**用户的口令哈希,
      快照一旦被日志打出去就等于把哈希发布了。
    """

    flags: tuple[str, ...]
    password_count: int
    commands: tuple[str, ...]
    keys: str
    channels: str
    selector_count: int


@dataclasses.dataclass(frozen=True)
class _ClusterTopologySnapshot:
    digest: str
    master_set_digest: str
    master_count: int


@dataclasses.dataclass(frozen=True)
class _ClusterInfoFence:
    current_epoch: int
    known_nodes: int
    cluster_size: int
    digest: str


@dataclasses.dataclass(frozen=True)
class _ClusterOwnershipView:
    owners: tuple[str, ...]
    master_ids: tuple[str, ...]
    node_count: int
    self_id: str
    digest: str


@dataclasses.dataclass(frozen=True)
class _ClusterSlotRange:
    """`CLUSTER SLOTS` 的一段区间。

    Go 侧由 go-redis 解析成 `[]redis.ClusterSlot`;redis-py 不给解析,所以本模块
    自己把原始嵌套数组拆成这个结构(`_parse_cluster_slots_reply`)。多出来的这层
    解析是 Python 独有的,形状不对一律 fail-closed。
    """

    start: int
    end: int
    node_ids: tuple[str, ...]


def _acl_dry_run_args(username: str, command: list[str]) -> list[str]:
    """对应 Go 的 `aclDryRunArgs`。"""
    return ["ACL", "DRYRUN", username, *command]


async def require_acl_dry_run_allowed(node: Any, username: str, command: list[str]) -> None:
    """DRYRUN 必须**明确返回 OK**。对应 Go 的 `requireACLDryRunAllowed`。

    方向:命令不可用 / 返回任何非 OK 的东西 → **报错**。也就是"证明不了允许"
    等价于"失败" —— 因为审计要靠 GET/SCAN 看全数据,看不全的审计结论没有意义。
    """
    result, err = await _try_text(node, *_acl_dry_run_args(username, command))
    if err is not None:
        raise PreflightError(
            "ACL DRYRUN %s must be allowed: %s", command[0], _err_text(err), cause=err
        )
    if result != "OK":
        raise PreflightError(
            "ACL DRYRUN %s returned %s, want OK", command[0], _quote_go(result)
        )


async def require_acl_dry_run_denied(node: Any, username: str, command: list[str]) -> None:
    """DRYRUN 必须返回**明确点名该命令**的权限拒绝。对应 `requireACLDryRunDenied`。

    方向(逐条照抄 Go,顺序不能改):
      ① 回包/错误里同时含("no permissions" 或 "noperm")与**命令名** → 通过;
      ② 否则若无错且回包是 "OK" → "unexpectedly allowed a forbidden command";
      ③ 其余(超时、连接错、含糊的错误串)→ "did not return an explicit ... denial"。
    ③ 是关键:**拿不到明确拒绝也算失败**,不能因为"反正没说允许"就放行。
    """
    result, err = await _try_text(node, *_acl_dry_run_args(username, command))
    message = result.lower()
    if err is not None:
        message = _err_text(err).lower()
    name = command[0].lower()
    permission_denied = "no permissions" in message or "noperm" in message
    if permission_denied and name in message:
        return
    if err is None and result == "OK":
        raise PreflightError(
            "ACL DRYRUN %s unexpectedly allowed a forbidden command", command[0]
        )
    raise PreflightError(
        "ACL DRYRUN %s did not return an explicit command permission denial", command[0]
    )


async def require_acl_dry_run_key_denied(node: Any, username: str, command: list[str]) -> None:
    """DRYRUN 必须返回明确的 **key** 权限拒绝。对应 `requireACLDryRunKeyDenied`。

    与上一条的差别只有一个字:这里要求拒绝理由里出现 `key` —— 证明的是"命名空间
    之外的 key 读不到",而不是"这条命令被禁了"。混用会让"GET 整个被禁"冒充
    "GET 只能读本命名空间",而前者会让审计一条记录都读不到。
    """
    result, err = await _try_text(node, *_acl_dry_run_args(username, command))
    message = result.lower()
    if err is not None:
        message = _err_text(err).lower()
    if ("no permissions" in message or "noperm" in message) and "key" in message:
        return
    if err is None and result == "OK":
        raise PreflightError("ACL DRYRUN GET unexpectedly allowed an out-of-namespace key")
    raise PreflightError("ACL DRYRUN GET did not return an explicit key permission denial")


def canonical_acl_token_set(tokens: list[str]) -> list[str]:
    """规范 token 集合(非空、全小写、无空白 / 控制字符、无重复,排序返回)。

    对应 Go 的 `canonicalACLTokenSet`。排序是为了让"集合相等"与书写顺序无关;
    去重是因为 `+get +get` 与 `+get` 在语义上相同但字符串不同。
    """
    if not tokens:
        raise PreflightError("empty token set")
    seen: set[str] = set()
    for token in tokens:
        if token == "" or token != token.lower() or _go_contains_space_or_control(token):
            raise PreflightError("non-canonical token")
        if token in seen:
            raise PreflightError("duplicate token")
        seen.add(token)
    return sorted(tokens)


def _acl_interface_list(value: Any) -> list[Any]:
    """对应 Go 的 `aclInterfaceList`(只认列表)。"""
    if isinstance(value, (list, tuple)):
        return list(value)
    raise PreflightError("not a list")


def _acl_string_list(value: Any) -> list[str]:
    """对应 Go 的 `aclStringList`。

    ★ redis-py 默认返回 bytes,Go 侧拿到的是 string —— 这里把 bytes 解成 str
      再套 Go 的"非空字符串"判据。解不出 UTF-8 一律拒(fail-closed)。
    """
    items = _acl_interface_list(value)
    result: list[str] = []
    for item in items:
        if isinstance(item, (bytes, bytearray, memoryview)):
            try:
                item = bytes(item).decode("utf-8")
            except UnicodeDecodeError as exc:
                raise PreflightError("non-string list member", cause=exc) from exc
        if not isinstance(item, str) or item == "":
            raise PreflightError("non-string list member")
        result.append(item)
    return result


def _acl_user_fields(value: Any) -> dict[str, Any]:
    """把 RESP2(交替数组)/ RESP3(map)两种回包折成同一个字典。

    对应 Go 的 `aclUserFields`。字段名必须非空且全小写,重复字段拒;六个必需字段
    缺一即拒 —— 缺字段时"没检查到"和"检查通过"在结构上不可区分,只能拒。
    """
    result: dict[str, Any] = {}

    def add(raw_key: Any, raw_value: Any) -> None:
        key = raw_key
        if isinstance(key, (bytes, bytearray, memoryview)):
            try:
                key = bytes(key).decode("utf-8")
            except UnicodeDecodeError as exc:
                raise PreflightError("non-canonical field name", cause=exc) from exc
        if not isinstance(key, str) or key == "" or key.lower() != key:
            raise PreflightError("non-canonical field name")
        if key in result:
            raise PreflightError("duplicate field")
        result[key] = raw_value

    if isinstance(value, (list, tuple)):
        if len(value) % 2 != 0:
            raise PreflightError("odd field array")
        for i in range(0, len(value), 2):
            add(value[i], value[i + 1])
    elif isinstance(value, dict):
        for key, field in value.items():
            add(key, field)
    else:
        raise PreflightError("unexpected response shape")
    for required in ("flags", "passwords", "commands", "keys", "channels", "selectors"):
        if required not in result:
            raise PreflightError("missing %s field", required)
    return result


def parse_acl_get_user(value: Any) -> ACLUserSnapshot:
    """解析 `ACL GETUSER` 回包。对应 Go 的 `parseACLGetUser`。

    ★ 安全边界(照搬 Go 的 SECURITY BOUNDARY 注释,一个字都不能弱化):
      `ACL GETUSER` 是这几种机制里**唯一**能证明完整
      flags / 口令条数 / 命令 / key / 频道 / selector 状态的命令;它同时能泄露这台
      Redis 上**所有**用户的口令哈希,而 `SCAN` 会泄露 key 名。因此这个身份**只能**
      在激活期存在,只能对着专用或同信任域的 Redis 使用,且那台 Redis 的每一个口令
      都必须是高熵的。激活控制器必须发放不可变的带版本凭据,并在 rollout CAS 成功后
      **立即**禁用 / 删除该用户。**本进程不做那步清理,也不能被当成生命周期控制器。**
    """
    fields = _acl_user_fields(value)
    if len(fields) != 6:
        raise PreflightError("unexpected field set")
    try:
        flags = _acl_string_list(fields["flags"])
    except asyncio.CancelledError:
        raise
    except BaseException:  # noqa: BLE001
        raise PreflightError("flags are malformed") from None
    try:
        passwords = _acl_string_list(fields["passwords"])
    except asyncio.CancelledError:
        raise
    except BaseException:  # noqa: BLE001
        raise PreflightError("password metadata is malformed") from None
    for password_hash in passwords:
        if not _REDIS_PASSWORD_HASH_RE.fullmatch(password_hash):
            raise PreflightError("password metadata is non-canonical")
    commands = fields["commands"]
    if isinstance(commands, (bytes, bytearray, memoryview)):
        try:
            commands = bytes(commands).decode("utf-8")
        except UnicodeDecodeError:
            raise PreflightError("commands are malformed") from None
    if (
        not isinstance(commands, str)
        or commands == ""
        or _go_trim_space(commands) != commands
        or " ".join(_go_fields(commands)) != commands
    ):
        raise PreflightError("commands are malformed")
    try:
        command_tokens = canonical_acl_token_set(_go_fields(commands))
    except asyncio.CancelledError:
        raise
    except BaseException:  # noqa: BLE001
        raise PreflightError("commands are malformed") from None
    keys = fields["keys"]
    if isinstance(keys, (bytes, bytearray, memoryview)):
        try:
            keys = bytes(keys).decode("utf-8")
        except UnicodeDecodeError:
            raise PreflightError("key rules are malformed") from None
    if not isinstance(keys, str):
        raise PreflightError("key rules are malformed")
    channels = fields["channels"]
    if isinstance(channels, (bytes, bytearray, memoryview)):
        try:
            channels = bytes(channels).decode("utf-8")
        except UnicodeDecodeError:
            raise PreflightError("channel rules are malformed") from None
    if not isinstance(channels, str):
        raise PreflightError("channel rules are malformed")
    try:
        selectors = _acl_interface_list(fields["selectors"])
    except asyncio.CancelledError:
        raise
    except BaseException:  # noqa: BLE001
        raise PreflightError("selectors are malformed") from None
    return ACLUserSnapshot(
        flags=tuple(flags),
        password_count=len(passwords),
        commands=tuple(command_tokens),
        keys=keys,
        channels=channels,
        selector_count=len(selectors),
    )


def validate_canonical_read_only_acl(user: ACLUserSnapshot) -> None:
    """六条**精确**契约。对应 Go 的 `validateCanonicalReadOnlyACL`。

    每一条都是"等于",不是"包含":
      flags == {on, sanitize-payload}          多一个 flag 就可能改变解析行为;
      口令条数 == 1                            多一条 = 多一把能登进来的钥匙;
      commands == 白名单(排序去重后逐个相等)  多一条命令 = 这不再是只读身份;
      keys == "%R~pandora:ds:battle:*"         `%R~` 是**只读**前缀,写成 `~` 就是读写;
      channels == ""                           发布订阅一律不给;
      selector 数 == 0                         selector 是"附加权限集",必须为空。
    """
    want_flags = canonical_acl_token_set(["on", "sanitize-payload"])
    want_commands = canonical_acl_token_set(list(CANONICAL_READ_ONLY_ACL_COMMANDS))
    try:
        got_flags = canonical_acl_token_set(list(user.flags))
    except asyncio.CancelledError:
        raise
    except BaseException:  # noqa: BLE001
        raise PreflightError(
            "dedicated Redis ACL flags differ from the exact activation-only contract"
        ) from None
    if got_flags != want_flags:
        raise PreflightError(
            "dedicated Redis ACL flags differ from the exact activation-only contract"
        )
    if user.password_count != 1:
        raise PreflightError("dedicated Redis ACL must contain exactly one password hash")
    if list(user.commands) != want_commands:
        raise PreflightError(
            "dedicated Redis ACL commands differ from the exact read-only allowlist"
        )
    if user.keys != "%R~" + BATTLE_SCAN_PATTERN:
        raise PreflightError(
            "dedicated Redis ACL key rules differ from the exact read-only namespace"
        )
    if user.channels != "":
        raise PreflightError("dedicated Redis ACL channel rules must be empty")
    if user.selector_count != 0:
        raise PreflightError("dedicated Redis ACL selectors must be empty")


async def prove_read_only_acl(node: Any, username: str) -> None:
    """证明当前连接的 ACL 身份就是那个专用只读身份。对应 Go 的 `proveReadOnlyACL`。

    顺序即契约:
      ① `ACL WHOAMI` 必须**等于**期望用户名(不是"包含",不是"以...开头");
      ② `ACL GETUSER <自己>` 解析 + 六条精确契约 —— **完整证明在这一步就完成了**;
      ③ 之后的 DRYRUN 只是**纵深防御**的语义抽查。Go 的注释写得很清楚:
         错误的元数比对和一份**有限**的禁止清单,永远不可能证明"不存在另一条授权"。
         所以不能把 ② 换成"多跑几条 DRYRUN"——那是把完整证明换成抽样。

    ★ 只查**自己**的 GETUSER。查别人 = 顺手把别人的口令哈希读进本进程内存,
      Go 有测试专门断言"没有对别的用户调用 GETUSER"。
    """
    whoami, err = await _try_text(node, "ACL", "WHOAMI")
    if err is not None:
        raise PreflightError("ACL WHOAMI is unavailable: %s", _err_text(err), cause=err)
    if whoami != username:
        raise PreflightError(
            "ACL WHOAMI=%s, want dedicated read-only identity %s",
            _quote_go(whoami),
            _quote_go(username),
        )
    try:
        value = await node.execute_command("ACL", "GETUSER", username)
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001
        raise PreflightError(
            "ACL GETUSER is unavailable for the dedicated identity: %s",
            _err_text(exc),
            cause=exc,
        ) from exc
    try:
        user = parse_acl_get_user(value)
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001
        raise PreflightError(
            "ACL GETUSER returned a non-canonical dedicated identity: %s",
            _err_text(exc),
            cause=exc,
        ) from exc
    validate_canonical_read_only_acl(user)

    # 以下探针的命令与参数**逐字符照抄 Go**:`{1}` 是 Cluster hashtag,
    # `pandora:outside-preflight-trust-domain` 刻意落在命名空间之外。
    for command in (
        ["GET", "pandora:ds:battle:{1}"],
        ["SCAN", "0", "MATCH", BATTLE_SCAN_PATTERN, "COUNT", "1"],
    ):
        await require_acl_dry_run_allowed(node, username, command)
    await require_acl_dry_run_key_denied(
        node, username, ["GET", "pandora:outside-preflight-trust-domain"]
    )
    for command in (
        ["SET", "pandora:ds:battle:{1}", "forbidden"],
        ["DEL", "pandora:ds:battle:{1}"],
        ["EVAL", "return 1", "0"],
        ["CONFIG", "GET", "requirepass"],
    ):
        await require_acl_dry_run_denied(node, username, command)


async def cluster_master_id(node: Any) -> str:
    """`CLUSTER MYID` → 40 位小写 hex。对应 Go 的 `clusterMasterID`。"""
    node_id, err = await _try_text(node, "CLUSTER", "MYID")
    if err is not None:
        raise PreflightError("CLUSTER MYID is unavailable: %s", _err_text(err), cause=err)
    node_id = _go_trim_space(node_id).lower()
    if not _REDIS_RUNTIME_ID_RE.fullmatch(node_id):
        raise PreflightError("CLUSTER MYID returned a non-canonical node identity")
    return node_id


def _unauthenticated_clone(node: Any) -> Any:
    """从已认证客户端派生一个**空凭据**客户端。对应 Go 里就地复制 Options 的那段。

    Go 拷贝的是 `*redis.Options` 结构体并清掉 Username/Password/各种
    CredentialsProvider/OnConnect;Python 侧没有这个结构体,只能从
    `connection_pool.connection_kwargs` 复制,并且**按 Redis.__init__ 的形参白名单
    过滤** —— 连接池里塞着一堆构造函数不接受的内部键(parser_class /
    connection_class 等),照单全收会直接 TypeError,而那会被上层当成"证明失败",
    把一次正常启动变成拒启。

    ★ 测试通过 monkeypatch 本函数注入替身(Go 侧对应的是 miniredis)。
    """
    import inspect

    import redis.asyncio as aioredis

    pool = getattr(node, "connection_pool", None)
    if pool is None or not isinstance(getattr(pool, "connection_kwargs", None), dict):
        raise PreflightError("Redis password-required proof has no concrete client")
    kwargs = dict(pool.connection_kwargs)
    for credential in ("username", "password", "credential_provider"):
        kwargs.pop(credential, None)
    allowed = set(inspect.signature(aioredis.Redis.__init__).parameters)
    kwargs = {key: value for key, value in kwargs.items() if key in allowed}
    if not kwargs.get("host") and not kwargs.get("path"):
        raise PreflightError("Redis password-required proof has no concrete client")
    return aioredis.Redis(**kwargs)


async def prove_password_required(node: Any) -> None:
    """证明这台 Redis **强制**认证。对应 Go 的 `provePasswordRequired`。

    方向:空凭据 PING **成功** → 报错(这台 Redis 根本没开认证,那么"我用的是专用
    只读身份"这句话不成立);失败但错误不是规范的认证拒绝 → 也报错(连不上不等于
    要口令,可能只是网络不通,而"网络不通"证明不了任何事)。
    """
    if node is None:
        raise PreflightError("Redis password-required proof has no concrete client")
    unauthenticated = _unauthenticated_clone(node)
    try:
        try:
            await unauthenticated.ping()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            message = str(exc).lower()
            if "noauth" not in message and "authentication required" not in message:
                raise PreflightError(
                    "Redis unauthenticated PING did not return canonical authentication denial",
                    cause=exc,
                ) from exc
            return
        raise PreflightError(
            "Redis accepted unauthenticated PING; dedicated password is not mandatory"
        )
    finally:
        close = getattr(unauthenticated, "aclose", None) or getattr(
            unauthenticated, "close", None
        )
        if close is not None:
            try:
                await close()
            except asyncio.CancelledError:
                raise
            except BaseException:  # noqa: BLE001 —— 关闭失败不改变证明结论
                pass


def parse_redis_info_fields(body: str) -> dict[str, str]:
    """`INFO` / `CLUSTER INFO` 的逐行严格解析。对应 Go 的 `parseRedisInfoFields`。

    ★ 三条"非规范即拒"的判据不是洁癖:本函数的输出直接决定"这台是不是主、集群是不是
      健康",而回包是可被写入方影响的字节。带前后空白的行、名字带空白、重复字段,
      在宽松解析下会让**后一条覆盖前一条** —— 于是注入一行 `role:master` 就能把
      一台从库伪装成主库。
    """
    fields: dict[str, str] = {}
    for line in body.replace("\r\n", "\n").split("\n"):
        if line == "" or line.startswith("#"):
            continue
        if _go_trim_space(line) != line:
            raise PreflightError("non-canonical line whitespace")
        name, sep, value = line.partition(":")
        if (
            not sep
            or name == ""
            or _go_trim_space(name) != name
            or _go_trim_space(value) != value
        ):
            raise PreflightError("non-canonical field")
        if name in fields:
            raise PreflightError("duplicate field")
        fields[name] = value
    return fields


def parse_strict_uint_field(fields: dict[str, str], name: str) -> int:
    """严格 uint64 字段。对应 Go 的 `parseStrictUintField`。

    ★ `str(parsed) != value` 这条回写比对必须留着:它挡的是 `007` / `+7` /
      `7\\n` 这类"能解析出 7 但不是规范十进制"的写法。
    ★ Go 的 `ParseUint(...,10,64)` 越界即错;Python 必须显式判 UINT64_MAX。
    """
    value = fields.get(name)
    if value is None or value == "":
        raise PreflightError("missing %s", name)
    if not _DECIMAL_RE.fullmatch(value):
        raise PreflightError("invalid %s", name)
    parsed = int(value)
    if parsed > UINT64_MAX or str(parsed) != value:
        raise PreflightError("invalid %s", name)
    return parsed


def parse_strict_positive_int_field(fields: dict[str, str], name: str) -> int:
    """严格正 int 字段。对应 Go 的 `parseStrictPositiveIntField`。

    Go 的上界是平台 `int`(64 位机上 = INT64_MAX);Python 照此显式判。
    """
    try:
        value = parse_strict_uint_field(fields, name)
    except asyncio.CancelledError:
        raise
    except BaseException:  # noqa: BLE001 —— Go 把 err 与越界折进同一句
        raise PreflightError("CLUSTER INFO %s is invalid", name) from None
    if value == 0 or value > INT64_MAX:
        raise PreflightError("CLUSTER INFO %s is invalid", name)
    return value


def parse_cluster_info_fence(body: str) -> _ClusterInfoFence:
    """`CLUSTER INFO` 的健康栅栏。对应 Go 的 `parseClusterInfoFence`。

    必须**全部 16384 个 slot 已分配且 ok、pfail/fail 均为 0**、状态为 ok。
    有一个 slot 处于 pfail 就说明观测期间集群正在变动,那么"我看全了"不成立。
    """
    try:
        fields = parse_redis_info_fields(body)
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001
        raise PreflightError(
            "CLUSTER INFO is malformed: %s", _err_text(exc), cause=exc
        ) from exc
    if fields.get("cluster_state") != "ok":
        raise PreflightError("CLUSTER INFO state is not ok")
    required = {
        "cluster_slots_assigned": CLUSTER_SLOT_COUNT,
        "cluster_slots_ok": CLUSTER_SLOT_COUNT,
        "cluster_slots_pfail": 0,
        "cluster_slots_fail": 0,
    }
    for name, want in required.items():
        try:
            got = parse_strict_uint_field(fields, name)
        except asyncio.CancelledError:
            raise
        except BaseException:  # noqa: BLE001
            raise PreflightError("CLUSTER INFO %s is not canonical", name) from None
        if got != want:
            raise PreflightError("CLUSTER INFO %s is not canonical", name)
    try:
        current_epoch = parse_strict_uint_field(fields, "cluster_current_epoch")
    except asyncio.CancelledError:
        raise
    except BaseException:  # noqa: BLE001
        raise PreflightError("CLUSTER INFO current_epoch is invalid") from None
    known_nodes = parse_strict_positive_int_field(fields, "cluster_known_nodes")
    cluster_size = parse_strict_positive_int_field(fields, "cluster_size")
    if known_nodes < cluster_size:
        raise PreflightError("CLUSTER INFO known_nodes is smaller than cluster_size")
    digest = digest_strings(
        "pod-uid-preflight-cluster-info-v1",
        [str(current_epoch), str(known_nodes), str(cluster_size)],
    )
    return _ClusterInfoFence(
        current_epoch=current_epoch,
        known_nodes=known_nodes,
        cluster_size=cluster_size,
        digest=digest,
    )


def parse_cluster_slot_token(token: str) -> tuple[int, int]:
    """`CLUSTER NODES` 行尾的 slot token(`123` 或 `123-456`)。对应 `parseClusterSlotToken`。"""
    parts = token.split("-")
    if len(parts) > 2 or len(parts) == 0 or parts[0] == "":
        raise PreflightError("CLUSTER NODES contains an invalid slot token")
    start_text = parts[0]
    if not _DECIMAL_RE.fullmatch(start_text):
        raise PreflightError("CLUSTER NODES contains an invalid slot token")
    start = int(start_text)
    if str(start) != start_text or start < 0 or start >= CLUSTER_SLOT_COUNT:
        raise PreflightError("CLUSTER NODES contains an invalid slot token")
    end = start
    if len(parts) == 2:
        end_text = parts[1]
        if not _DECIMAL_RE.fullmatch(end_text):
            raise PreflightError("CLUSTER NODES contains an invalid slot range")
        end = int(end_text)
        if str(end) != end_text or end < start or end >= CLUSTER_SLOT_COUNT:
            raise PreflightError("CLUSTER NODES contains an invalid slot range")
    return start, end


def compress_slot_owners(owners: list[str]) -> list[str]:
    """把 16384 个 slot 的归属压成 `[start, end, owner]` 三元组序列。

    对应 Go 的 `compressSlotOwners`。压缩只为让摘要输入短,**不丢信息**:
    同一个归属表压出来的序列唯一。
    """
    if not owners:
        return []
    result: list[str] = []
    start = 0
    total = len(owners)
    while start < total:
        end = start
        while end + 1 < total and owners[end + 1] == owners[start]:
            end += 1
        result.extend([str(start), str(end), owners[start]])
        start = end + 1
    return result


def parse_cluster_nodes_ownership(body: str) -> _ClusterOwnershipView:
    """`CLUSTER NODES` 的精确归属视图。对应 Go 的 `parseClusterNodesOwnership`。

    每一条判据都对应一种"看起来正常但结论不可信"的状态:
      未知 flag              → 这个副本不认识对面的状态,不能替它下结论;
      fail/fail?/handshake/noaddr → 节点状态不稳,观测无意义;
      非 connected           → 同上;
      角色歧义(既主既从/皆非) → 不知道该不该扫它;
      importing/migrating slot → **正在搬 key**,SCAN 必然看不全;
      replica 持有 slot      → 归属表自相矛盾;
      零 slot 的 master      → 这台不是 slot 拥有者,却被当成 master 计数;
      slot 被分配两次 / 有未分配的 slot → 归属表不是一个函数,覆盖不全。
    """
    owners = [""] * CLUSTER_SLOT_COUNT
    master_ids: list[str] = []
    node_parts: list[str] = []
    seen_nodes: set[str] = set()
    replica_parents: list[str] = []
    self_id = ""
    node_count = 0
    for raw_line in body.replace("\r\n", "\n").split("\n"):
        line = _go_trim_space(raw_line)
        if line == "":
            continue
        fields = _go_fields(line)
        if len(fields) < 8:
            raise PreflightError("CLUSTER NODES contains a short record")
        node_id = fields[0].lower()
        if fields[0] != node_id or not _REDIS_RUNTIME_ID_RE.fullmatch(node_id):
            raise PreflightError("CLUSTER NODES contains a non-canonical node ID")
        if node_id in seen_nodes:
            raise PreflightError("CLUSTER NODES contains a duplicate node ID")
        seen_nodes.add(node_id)
        node_count += 1
        flags: dict[str, bool] = {}
        for flag in fields[2].split(","):
            if flag == "" or flag != flag.lower():
                raise PreflightError("CLUSTER NODES contains a non-canonical flag")
            if flag in flags:
                raise PreflightError("CLUSTER NODES contains a duplicate flag")
            if flag not in (
                "myself",
                "master",
                "slave",
                "replica",
                "nofailover",
                "fail",
                "fail?",
                "handshake",
                "noaddr",
                "noflags",
            ):
                raise PreflightError("CLUSTER NODES contains an unknown node flag")
            flags[flag] = True
        for unsafe in ("fail", "fail?", "handshake", "noaddr"):
            if flags.get(unsafe):
                raise PreflightError("CLUSTER NODES contains an unsafe node flag")
        if fields[7] != "connected":
            raise PreflightError("CLUSTER NODES contains a disconnected node")
        if flags.get("myself"):
            if self_id != "":
                raise PreflightError("CLUSTER NODES contains multiple myself nodes")
            self_id = node_id
        master = bool(flags.get("master"))
        replica = bool(flags.get("slave")) or bool(flags.get("replica"))
        if master == replica:
            raise PreflightError("CLUSTER NODES contains an ambiguous node role")
        if not _DECIMAL_RE.fullmatch(fields[6]):
            raise PreflightError("CLUSTER NODES contains an invalid config epoch")
        config_epoch = int(fields[6])
        if config_epoch > UINT64_MAX or str(config_epoch) != fields[6]:
            raise PreflightError("CLUSTER NODES contains an invalid config epoch")
        parent = fields[3]
        role = "master"
        if replica:
            role = "replica"
            parent = parent.lower()
            if not _REDIS_RUNTIME_ID_RE.fullmatch(parent):
                raise PreflightError("CLUSTER NODES replica has invalid master identity")
            replica_parents.append(parent)
        elif parent != "-":
            raise PreflightError("CLUSTER NODES master has a parent identity")
        node_parts.append(
            digest_strings(
                "pod-uid-preflight-cluster-node-v1",
                [node_id, role, parent, str(config_epoch)],
            )
        )
        slot_count = 0
        for token in fields[8:]:
            if token.startswith("[") or "->-" in token or "-<-" in token:
                raise PreflightError("CLUSTER NODES reports an importing or migrating slot")
            if replica:
                raise PreflightError("CLUSTER NODES replica unexpectedly owns slots")
            start, end = parse_cluster_slot_token(token)
            for slot in range(start, end + 1):
                if owners[slot] != "":
                    raise PreflightError("CLUSTER NODES assigns a slot more than once")
                owners[slot] = node_id
                slot_count += 1
        if master:
            if slot_count == 0:
                raise PreflightError("CLUSTER NODES contains a zero-slot master")
            master_ids.append(node_id)
    if node_count == 0 or not master_ids:
        raise PreflightError("CLUSTER NODES contains no usable masters")
    if self_id == "":
        raise PreflightError("CLUSTER NODES omitted the myself node")
    master_set = set(master_ids)
    for parent in replica_parents:
        if parent not in master_set:
            raise PreflightError("CLUSTER NODES replica references an unknown master")
    if self_id not in master_set:
        raise PreflightError("CLUSTER NODES myself node is not a slot-owning master")
    for slot, owner in enumerate(owners):
        if owner == "":
            raise PreflightError("CLUSTER NODES leaves slot %d unassigned", slot)
    master_ids.sort()
    node_parts.sort()
    digest_parts = ["nodes", str(node_count), *node_parts, *compress_slot_owners(owners)]
    return _ClusterOwnershipView(
        owners=tuple(owners),
        master_ids=tuple(master_ids),
        node_count=node_count,
        self_id=self_id,
        digest=digest_strings("pod-uid-preflight-cluster-nodes-v1", digest_parts),
    )


def _parse_cluster_slots_reply(raw: Any) -> list[_ClusterSlotRange]:
    """把 `CLUSTER SLOTS` 的原始嵌套数组拆成区间列表。

    ★ **Python 独有的一层**:Go 侧 go-redis 已经解析成 `[]redis.ClusterSlot`。
      形状不符一律 fail-closed —— 解析不出来时"没有可信的归属表",不能当成空表放行。
    """
    if not isinstance(raw, (list, tuple)):
        raise PreflightError("CLUSTER SLOTS returned a malformed reply")
    result: list[_ClusterSlotRange] = []
    for entry in raw:
        if not isinstance(entry, (list, tuple)) or len(entry) < 3:
            raise PreflightError("CLUSTER SLOTS returned a malformed reply")
        start, end = entry[0], entry[1]
        if (
            not isinstance(start, int)
            or isinstance(start, bool)
            or not isinstance(end, int)
            or isinstance(end, bool)
        ):
            raise PreflightError("CLUSTER SLOTS returned a malformed reply")
        node_ids: list[str] = []
        for node in entry[2:]:
            if not isinstance(node, (list, tuple)) or len(node) < 3:
                raise PreflightError("CLUSTER SLOTS returned a malformed reply")
            node_id = node[2]
            if isinstance(node_id, (bytes, bytearray, memoryview)):
                try:
                    node_id = bytes(node_id).decode("utf-8")
                except UnicodeDecodeError:
                    raise PreflightError(
                        "CLUSTER SLOTS contains a non-canonical master ID"
                    ) from None
            if not isinstance(node_id, str):
                raise PreflightError("CLUSTER SLOTS contains a non-canonical master ID")
            node_ids.append(node_id)
        result.append(_ClusterSlotRange(start=start, end=end, node_ids=tuple(node_ids)))
    return result


def parse_cluster_slots_ownership(slots: list[_ClusterSlotRange]) -> _ClusterOwnershipView:
    """`CLUSTER SLOTS` 必须是 0..16383 的**精确连续**覆盖。对应 `parseClusterSlotsOwnership`。

    "连续"是关键:按 Start 排序后每段必须**紧接**上一段的下一格。留一个洞或有重叠时,
    这份归属表就不能证明"我访问的这些 master 覆盖了全部 key 空间"。
    """
    if not slots:
        raise PreflightError("CLUSTER SLOTS returned no ownership ranges")
    ordered = sorted(slots, key=lambda item: item.start)
    owners = [""] * CLUSTER_SLOT_COUNT
    masters: set[str] = set()
    next_slot = 0
    for slot_range in ordered:
        if (
            slot_range.start != next_slot
            or slot_range.end < slot_range.start
            or slot_range.end >= len(owners)
            or not slot_range.node_ids
        ):
            raise PreflightError("CLUSTER SLOTS is not an exact contiguous 0..16383 map")
        node_id = slot_range.node_ids[0].lower()
        if slot_range.node_ids[0] != node_id or not _REDIS_RUNTIME_ID_RE.fullmatch(node_id):
            raise PreflightError("CLUSTER SLOTS contains a non-canonical master ID")
        masters.add(node_id)
        for slot in range(slot_range.start, slot_range.end + 1):
            owners[slot] = node_id
        next_slot = slot_range.end + 1
    if next_slot != len(owners):
        raise PreflightError("CLUSTER SLOTS does not cover all 16384 slots")
    return _ClusterOwnershipView(
        owners=tuple(owners),
        master_ids=tuple(sorted(masters)),
        node_count=0,
        self_id="",
        digest=digest_strings("pod-uid-preflight-cluster-slots-v1", compress_slot_owners(owners)),
    )


async def observe_cluster_topology(node: Any, expected_self_id: str) -> _ClusterTopologySnapshot:
    """一次拓扑观测:INFO / NODES / SLOTS 三方必须**逐个 slot** 一致。

    对应 Go 的 `observeClusterTopology`。三方交叉比对而不是信任其中一个:
    `CLUSTER INFO` 只给计数,`NODES` 是本节点的视图,`SLOTS` 是客户端路由表 ——
    它们不一致就说明观测窗口里集群在变。
    """
    info_body, err = await _try_text(node, "CLUSTER", "INFO")
    if err is not None:
        raise PreflightError("CLUSTER INFO is unavailable: %s", _err_text(err), cause=err)
    info = parse_cluster_info_fence(info_body)
    nodes_body, err = await _try_text(node, "CLUSTER", "NODES")
    if err is not None:
        raise PreflightError("CLUSTER NODES is unavailable: %s", _err_text(err), cause=err)
    nodes = parse_cluster_nodes_ownership(nodes_body)
    try:
        slots_raw = await node.execute_command("CLUSTER", "SLOTS")
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001
        raise PreflightError(
            "CLUSTER SLOTS is unavailable: %s", _err_text(exc), cause=exc
        ) from exc
    slots = parse_cluster_slots_ownership(_parse_cluster_slots_reply(slots_raw))
    if info.cluster_size != len(nodes.master_ids) or info.known_nodes != nodes.node_count:
        raise PreflightError("CLUSTER INFO size/count does not match CLUSTER NODES")
    if nodes.self_id != expected_self_id:
        raise PreflightError("CLUSTER MYID and CLUSTER NODES disagree on the connected master")
    if nodes.master_ids != slots.master_ids or nodes.owners != slots.owners:
        raise PreflightError("CLUSTER NODES and CLUSTER SLOTS disagree on exact slot ownership")
    master_set_digest = runtime_master_set_digest(list(nodes.master_ids))
    digest = digest_strings(
        "pod-uid-preflight-cluster-topology-v1",
        [info.digest, nodes.digest, slots.digest, master_set_digest],
    )
    return _ClusterTopologySnapshot(
        digest=digest,
        master_set_digest=master_set_digest,
        master_count=len(nodes.master_ids),
    )


async def observe_stable_cluster_topology(
    node: Any, expected_self_id: str
) -> _ClusterTopologySnapshot:
    """要求**两次完全一致**的本地观测。对应 Go 的 `observeStableClusterTopology`。

    ★ Go 的头注释必须照搬:激活协议还必须持有一把**外部**的 Redis 拓扑变更锁。
      Redis 8.4 的原子迁移可以在**归属表完全不变**的情况下让 SCAN 看不到 key,
      而它的 STATUS 子命令无法在不同时授予破坏性 CLUSTER MIGRATION 操作的前提下
      单独授权 —— 也就是说,这两次观测**证明不了**没有迁移。移植不放宽这一条。
    """
    first = await observe_cluster_topology(node, expected_self_id)
    second = await observe_cluster_topology(node, expected_self_id)
    if first != second:
        raise PreflightError("Redis cluster topology changed during observation")
    return first


def parse_server_cluster_disabled(body: str) -> None:
    """`INFO cluster` 必须证明 `cluster_enabled=0`。对应 `parseServerClusterDisabled`。"""
    try:
        fields = parse_redis_info_fields(body)
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001
        raise PreflightError(
            "INFO cluster is malformed: %s", _err_text(exc), cause=exc
        ) from exc
    if fields.get("cluster_enabled") != "0":
        raise PreflightError("INFO cluster must prove cluster_enabled=0")


async def prove_server_cluster_disabled(node: Any) -> None:
    """对应 Go 的 `proveServerClusterDisabled`。"""
    body, err = await _try_text(node, "INFO cluster")
    if err is not None:
        raise PreflightError("INFO cluster is unavailable: %s", _err_text(err), cause=err)
    parse_server_cluster_disabled(body)


def parse_server_primary(body: str) -> None:
    """`INFO replication` 必须证明 `role=master`。对应 `parseServerPrimary`。

    方向:字段缺失也算失败 —— "读不到 role"不能当成"是主"。
    """
    try:
        fields = parse_redis_info_fields(body)
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001
        raise PreflightError(
            "INFO replication is malformed: %s", _err_text(exc), cause=exc
        ) from exc
    if fields.get("role") != "master":
        raise PreflightError("INFO replication must prove role=master")


async def prove_server_primary(node: Any) -> None:
    """对应 Go 的 `proveServerPrimary`。"""
    body, err = await _try_text(node, "INFO replication")
    if err is not None:
        raise PreflightError("INFO replication is unavailable: %s", _err_text(err), cause=err)
    parse_server_primary(body)


async def standalone_runtime_id(node: Any) -> str:
    """`INFO server` 的 `run_id`。对应 Go 的 `standaloneRuntimeID`。

    run_id 每次进程重启都会变,所以它能证明"SCAN 前后是同一个进程" ——
    这正是 Sentinel 故障切换 / 容器重建的检测手段(但见头注释:检测不到 A→B→A)。
    """
    info, err = await _try_text(node, "INFO server")
    if err is not None:
        raise PreflightError("INFO server is unavailable: %s", _err_text(err), cause=err)
    for line in info.replace("\r\n", "\n").split("\n"):
        key, sep, value = line.partition(":")
        if sep and key == "run_id":
            value = _go_trim_space(value).lower()
            if not _REDIS_RUNTIME_ID_RE.fullmatch(value):
                raise PreflightError("INFO server returned a non-canonical run_id")
            return value
    raise PreflightError("INFO server omitted run_id")


def _node_client(node: Any) -> Any:
    """RedisCluster 的节点对象 → 可执行命令的客户端。

    redis-py 的 `get_primaries()` 返回 `ClusterNode`,真正的连接在
    `.redis_connection`(与 `repo.reconcile_battle_active_index` 同一写法)。
    """
    connection = getattr(node, "redis_connection", None)
    if connection is not None:
        return connection
    return node


def _node_source_endpoint(node: Any) -> str:
    """取节点地址,只用于生成**已摘要**的 source 标签(绝不外泄明文)。"""
    name = getattr(node, "name", None)
    if isinstance(name, str) and name != "":
        return name
    host = getattr(node, "host", None)
    port = getattr(node, "port", None)
    if isinstance(host, str) and host != "" and port is not None:
        return f"{host}:{port}"
    return ""


async def prove_read_only_and_identify(
    rdb: Any, rc: Any, expected_username: str
) -> RedisTargetIdentity:
    """整套只读证明 + 目标身份摘要。对应 Go 的 `ProveReadOnlyAndIdentify`。

    ★ 客户端形态与配置形态必须**互相印证**:客户端是 cluster 而配置算出来不是
      cluster(或反过来)一律拒。两者不一致说明连的地方和以为连的地方不是一回事,
      而目标身份摘要正是要绑定"我连的就是配置里那个目标"。

    ★ Python 侧用 `get_primaries` 是否存在来判 ClusterClient(Go 是类型断言)。
      顺序遍历各 master,不像 Go 的 `ForEachMaster` 并发 —— 结论相同。
    """
    if rdb is None:
        raise PreflightError("Redis ACL proof requires context and client")
    if expected_username != CANONICAL_READ_ONLY_USERNAME:
        raise PreflightError(
            "Redis ACL proof requires canonical read-only username %s",
            _quote_go(CANONICAL_READ_ONLY_USERNAME),
        )
    endpoints = normalized_effective_endpoints(rc)
    config_identity = identify_redis_config(rc)
    topology = config_identity.topology
    runtime_ids: list[str] = []
    snapshots: list[_ClusterTopologySnapshot] = []
    topology_digest = ""

    primaries = getattr(rdb, "get_primaries", None)
    if callable(primaries):
        if topology != "cluster":
            raise PreflightError(
                "Redis client topology is cluster but normalized config topology is %s",
                topology,
            )
        try:
            for node in primaries():
                client = _node_client(node)
                await prove_password_required(client)
                await prove_read_only_acl(client, expected_username)
                node_id = await cluster_master_id(client)
                snapshot = await observe_stable_cluster_topology(client, node_id)
                runtime_ids.append(node_id)
                snapshots.append(snapshot)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise PreflightError(
                "Redis cluster ACL/identity proof failed: %s", _err_text(exc), cause=exc
            ) from exc
        if not snapshots:
            raise PreflightError("Redis cluster topology proof visited zero slot-owning masters")
        baseline = snapshots[0]
        for snapshot in snapshots[1:]:
            if snapshot != baseline:
                raise PreflightError("Redis masters disagree on cluster topology")
        topology_digest = baseline.digest
    else:
        if topology == "cluster":
            raise PreflightError(
                "normalized Redis config requires cluster but client is not a cluster client"
            )
        if not callable(getattr(rdb, "execute_command", None)):
            raise PreflightError(
                "standalone/sentinel Redis client has unsupported type %s", type(rdb).__name__
            )
        await prove_password_required(rdb)
        try:
            await prove_read_only_acl(rdb, expected_username)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise PreflightError(
                "Redis ACL proof failed: %s", _err_text(exc), cause=exc
            ) from exc
        try:
            await prove_server_cluster_disabled(rdb)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise PreflightError(
                "Redis non-cluster topology proof failed: %s", _err_text(exc), cause=exc
            ) from exc
        try:
            await prove_server_primary(rdb)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise PreflightError(
                "Redis primary role proof failed: %s", _err_text(exc), cause=exc
            ) from exc
        try:
            node_id = await standalone_runtime_id(rdb)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise PreflightError(
                "Redis runtime identity proof failed: %s", _err_text(exc), cause=exc
            ) from exc
        runtime_ids.append(node_id)
        topology_digest = digest_strings(
            "pod-uid-preflight-standalone-topology-v1", [node_id]
        )

    if not runtime_ids:
        raise PreflightError("Redis target identity visited zero masters")
    runtime_ids.sort()
    for i, node_id in enumerate(runtime_ids):
        if not _REDIS_RUNTIME_ID_RE.fullmatch(node_id):
            raise PreflightError("Redis returned a non-canonical runtime identity")
        if i > 0 and runtime_ids[i - 1] == node_id:
            raise PreflightError("Redis returned a duplicate master identity")
    master_set_digest = runtime_master_set_digest(runtime_ids)
    if topology == "cluster":
        # 每个 master 自己看到的 master 集合,必须**恰好**等于我们实际回调到的集合。
        # 少一个 = 有分片没被 SCAN 到,而审计结论会被当成"全库都查过了"。
        for snapshot in snapshots:
            if (
                snapshot.master_set_digest != master_set_digest
                or snapshot.master_count != len(runtime_ids)
            ):
                raise PreflightError(
                    "Redis slot-owner callbacks do not exactly cover the topology master set"
                )

    parts = [
        "pod-uid-release-preflight-target-v1",
        config_identity.digest,
        topology,
        rc.master_name,
        str(rc.db),
        str(len(endpoints)),
        *endpoints,
        str(len(runtime_ids)),
        *runtime_ids,
        master_set_digest,
        topology_digest,
    ]
    return RedisTargetIdentity(
        digest="sha256:" + length_prefixed_sha256(parts).hex(),
        topology=topology,
        nodes=len(runtime_ids),
        master_set_digest=master_set_digest,
        topology_digest=topology_digest,
    )


# ══════════════════════════════════════════════════════════════════════════
# audit.go —— 单条记录的保守分级
# ══════════════════════════════════════════════════════════════════════════


@dataclasses.dataclass
class Classification:
    """刻意是**纯函数**的结果:发布工具与测试用同一套状态 / 身份判定,不联系 Kubernetes。

    `reasons` 为空 = 这条记录对严格 Model-B 激活是安全的。
    """

    category: str
    reasons: list[str] = dataclasses.field(default_factory=list)


def classify_battle(key_match_id: int, rec: Any) -> Classification:
    """对应 Go 的 `ClassifyBattle`。

    ★ `allocation_uncertain` **刻意不算 exact identity**:对账之前它只有
      allocation_id,可能还没有 GameServer。那个状态归"durable uncertain 对账器"管。
      但只要出现**任何**物理身份字段,四元组就必须完整并且含 pod_uid。

    ★ unknown protobuf 字段 = 不安全。Go 的解码器刻意保留本二进制不认识的字段
      (§9 不变量 17),但把这种记录当"安全"等于对本闸看不懂的状态盖章放行 ——
      更新的写者必须先发布更新的闸。

    ★ 最后一句 `if reasons: category = unsafe` 不能省:前面按状态给出的分类是
      "这条记录属于哪一类",出现任何 reason 后它必须降级成 unsafe,
      否则调用方按 category 统计时会把有问题的记录数进安全桶。
    """
    result = Classification(category=CATEGORY_UNSAFE, reasons=[])
    if rec is None:
        result.reasons = ["record is nil"]
        return result
    if dsrepo.has_unknown_fields(rec):
        result.reasons.append(
            "battle record contains unknown protobuf fields that this release gate cannot audit"
        )
    if key_match_id == 0:
        result.reasons.append("battle key contains match_id=0")
    if rec.match_id != key_match_id:
        result.reasons.append(
            f"record match_id={rec.match_id} does not match key match_id={key_match_id}"
        )
    if not dsrepo.canonical_battle_allocation_id(rec.allocation_id):
        result.reasons.append("battle record has non-canonical UUIDv4 allocation_id")

    state = rec.state
    if state == "allocating":
        result.category = CATEGORY_NO_PHYSICAL_IDENTITY
        result.reasons.extend(_require_no_physical_identity(rec, "allocating"))
    elif state == dsrepo.BATTLE_STATE_ALLOCATION_UNCERTAIN:
        result.category = CATEGORY_ALLOCATION_UNCERTAIN
        result.reasons.extend(
            _require_no_physical_identity(rec, dsrepo.BATTLE_STATE_ALLOCATION_UNCERTAIN)
        )
    elif state == dsrepo.BATTLE_STATE_ALLOCATION_RECONCILE_EMPTY_TOMBSTONE:
        result.category = CATEGORY_NO_PHYSICAL_IDENTITY
        result.reasons.extend(
            _require_no_physical_identity(
                rec, dsrepo.BATTLE_STATE_ALLOCATION_RECONCILE_EMPTY_TOMBSTONE
            )
        )
    elif state == "abandoned":
        if dsrepo.battle_physical_identity_empty(rec):
            # 权威的 allocation-id 对账可能证明"根本没有 GameServer",
            # 于是留下一个空的永久终态栅栏 —— 那是合法形状,不是缺字段。
            result.category = CATEGORY_NO_PHYSICAL_IDENTITY
        else:
            result.category = CATEGORY_EXACT_IDENTITY
            result.reasons.extend(_require_exact_physical_identity(rec, "abandoned"))
    elif state in (
        "warming",
        "ready",
        "running",
        "ended",
        dsrepo.BATTLE_STATE_ALLOCATION_RECONCILE_RELEASE_PENDING,
        dsrepo.BATTLE_STATE_PREACTIVE_RELEASE_PENDING,
        dsrepo.BATTLE_STATE_ALLOCATION_ABORT_PENDING,
    ):
        result.category = CATEGORY_EXACT_IDENTITY
        result.reasons.extend(_require_exact_physical_identity(rec, state))
    else:
        result.reasons.append(f"unknown canonical battle state {_quote_go(state)}")
    if result.reasons:
        result.category = CATEGORY_UNSAFE
    return result


def _require_no_physical_identity(rec: Any, state: str) -> list[str]:
    """对应 Go 的 `requireNoPhysicalIdentity`。"""
    if dsrepo.battle_physical_identity_empty(rec):
        return []
    return [f"{state} carries a partial or unexpected physical GameServer identity"]


def _require_exact_physical_identity(rec: Any, state: str) -> list[str]:
    """对应 Go 的 `requireExactPhysicalIdentity`。

    ★ 四条**全部**收集(不是遇到第一条就返回):发布报告要一次列全缺什么,
      否则运维要跑四轮才知道全貌。
    ★ `release_track` 只认 stable / canary —— 复用 `releasetrack` 的判定同一处,
      不在这里抄两个字面量。
    """
    reasons: list[str] = []
    if not dsrepo.canonical_battle_identity_value(rec.ds_pod_name):
        reasons.append(f"{state} exact identity has empty ds_pod_name")
    if not dsrepo.canonical_battle_identity_value(rec.gameserver_uid):
        reasons.append(f"{state} exact identity has empty gameserver_uid")
    if rec.release_track != "stable" and rec.release_track != "canary":
        reasons.append(f"{state} exact identity has invalid release_track")
    if not dsrepo.canonical_battle_identity_value(rec.pod_uid):
        reasons.append(f"{state} exact allocation identity is missing pod_uid")
    return reasons


def valid_allocation_id(value: str) -> bool:
    """对应 Go 的 `validAllocationID`。

    与 `repo.canonical_battle_allocation_id` 是**同一套**四条判据(Go 侧同样是两份
    等价实现:`validAllocationID` 与 `canonicalBattleAllocationID`)。这里直接复用
    已移植的那份,避免两份副本各自漂移。
    """
    return dsrepo.canonical_battle_allocation_id(value)


# ══════════════════════════════════════════════════════════════════════════
# scan.go —— 全 master SCAN 审计
# ══════════════════════════════════════════════════════════════════════════


@dataclasses.dataclass(frozen=True)
class Finding:
    """一条审计发现。`match_id=0` 表示 key 本身就没解析出 match_id。"""

    source: str
    key: str
    match_id: int = 0
    reason: str = ""


class AuditSummary:
    """审计汇总。对应 Go 的 `AuditSummary`。

    ★ Go 用 `sync.Mutex` 是因为 `ForEachMaster` 并发回调;Python 侧顺序遍历各
      master(见 `audit_redis`),所以**没有**锁。若将来改成 `asyncio.gather` 并发,
      必须同时把这里改成带锁 —— 现在加锁属于对不存在的并发预设复杂度(§15.3)。
    """

    __slots__ = (
        "masters_visited",
        "keys_visited",
        "records_decoded",
        "allocation_uncertain",
        "findings",
        "_runtime_master_ids",
        "_seen_keys",
        "_record_digests",
    )

    def __init__(self) -> None:
        self.masters_visited = 0
        self.keys_visited = 0
        self.records_decoded = 0
        self.allocation_uncertain = 0
        self.findings: list[Finding] = []
        self._runtime_master_ids: set[str] = set()
        self._seen_keys: dict[str, str] = {}
        self._record_digests: dict[str, bytes] = {}

    def register_runtime_master(self, node_id: str) -> None:
        """登记一个**实际执行过 SCAN** 的 master 身份。对应 `registerRuntimeMaster`。"""
        if not _REDIS_RUNTIME_ID_RE.fullmatch(node_id):
            raise PreflightError("scan observed a non-canonical Redis master identity")
        if node_id in self._runtime_master_ids:
            raise PreflightError("scan observed a duplicate Redis master identity")
        self._runtime_master_ids.add(node_id)

    def runtime_master_set_digest(self) -> str:
        """把"确实执行了 SCAN 的运行时身份集合"绑成一个可安全公开的摘要。

        对应 Go 的 `RuntimeMasterSetDigest`。★ 数量必须与 `masters_visited` 相等:
        不等说明有 master 开始扫了却没登记身份(或反之),那么这份审计覆盖不完整。
        """
        if (
            not self._runtime_master_ids
            or len(self._runtime_master_ids) != self.masters_visited
        ):
            raise PreflightError("scan master identity coverage does not match visited master count")
        return runtime_master_set_digest(list(self._runtime_master_ids))

    def master_started(self) -> None:
        self.masters_visited += 1

    def register_scanned_key(self, key: str, owner_runtime_id: str) -> bool:
        """登记一个扫到的 key,返回"是否首次访问"。对应 `registerScannedKey`。

        ★ SCAN **允许**同一个 key 在一次遍历里出现多次,所以重复不是错误;
          但同一个 key 出现在**两个不同的 master** 上是错误 —— 那意味着 slot 正在
          搬迁或归属表不可信,而这正是"看不全"的形态。
        """
        if not _REDIS_RUNTIME_ID_RE.fullmatch(owner_runtime_id):
            raise PreflightError("scan key owner has a non-canonical Redis runtime identity")
        first_owner = self._seen_keys.get(key)
        if first_owner is not None:
            if first_owner != owner_runtime_id:
                raise PreflightError(
                    "durable battle key %s appeared on multiple Redis masters", _quote_go(key)
                )
            return False
        self._seen_keys[key] = owner_runtime_id
        self.keys_visited += 1
        return True

    def register_record_body(self, key: str, body: bytes) -> bool:
        """登记记录内容,返回"是否首次"。对应 `registerRecordBody`。

        ★ 这条让 SCAN 的重复 key 行为变得安全:第一份内容审计一次;完全相同的重复
          忽略;**同一次遍历里内容变了就 fail-closed**(必须重跑)——
          因为那说明审计期间有人在写,而"审计时的快照"不再成立。
        """
        digest = hashlib.sha256(body).digest()
        previous = self._record_digests.get(key)
        if previous is None:
            self._record_digests[key] = digest
            return True
        if previous != digest:
            raise PreflightError("durable battle key %s changed during audit", _quote_go(key))
        return False

    def record_decoded(self, category: str) -> None:
        self.records_decoded += 1
        if category == CATEGORY_ALLOCATION_UNCERTAIN:
            self.allocation_uncertain += 1

    def add_finding(self, finding: Finding) -> None:
        self.findings.append(finding)

    def sort_findings(self) -> None:
        """对应 Go 的 `SortFindings`(key → reason → source 三级排序)。"""
        self.findings.sort(key=lambda f: (f.key, f.reason, f.source))


def parse_battle_record_key(key: str) -> int:
    """从 key 反解 match_id。对应 Go 的 `ParseBattleRecordKey`。

    ★ 三条判据缺一不可:形状、**规范十进制**(`007` 拒)、非零。
      `match_id=0` 是保留值(§9 不变量:0 表示"没有 match"),让它通过会让
      分级函数拿一个不存在的对局去比对记录里的 match_id。
    ★ uint64 上界必须显式判(Go 由 `ParseUint(...,10,64)` 免费提供)。
    """
    match = _BATTLE_RECORD_KEY_RE.fullmatch(key)
    if match is None:
        raise PreflightError("unexpected key shape under battle namespace")
    text = match.group(1)
    match_id = int(text)
    if match_id > UINT64_MAX:
        raise PreflightError("invalid battle key match_id: value out of range")
    if str(match_id) != text:
        raise PreflightError("invalid battle key match_id: non-canonical decimal")
    if match_id == 0:
        raise PreflightError("invalid battle key match_id: zero is reserved")
    return match_id


def safe_redis_source(kind: str, endpoint: str) -> str:
    """把节点地址摘要成稳定标签。对应 Go 的 `safeRedisSource`。

    发现列表要能按"来自同一个 master"聚合,但发布日志里**不能**出现内网地址 ——
    所以取 `sha256(lower(trim(addr)))` 的前 6 字节(12 位 hex)。
    """
    endpoint = _go_trim_space(endpoint)
    if endpoint == "":
        return kind
    digest = hashlib.sha256(endpoint.lower().encode("utf-8")).digest()
    return f"{kind}-{digest[:6].hex()}"


async def scan_redis_node(
    node: Any,
    owner_runtime_id: str,
    source: str,
    scan_count: int,
    summary: AuditSummary,
) -> None:
    """在**单个** master 上完成 SCAN + GET + 分级。对应 Go 的 `scanRedisNode`。

    ★ 必须用 `SCAN` 游标,**绝不能**用 `KEYS`:`KEYS` 在大库上会把整个 Redis 阻塞
      到扫完为止 —— 一次"只读的发布体检"直接变成线上事故。
    ★ 任何一步出错都**整轮中止**(不"跳过继续"):半份审计结果会被当成"全库都查过了"。
      只有"key 形状不对"和"proto 解不开"两种情况记成 finding 后继续 ——
      它们本身就是审计要报告的内容。
    """
    summary.master_started()
    cursor = 0
    while True:
        try:
            cursor, keys = await node.scan(
                cursor=cursor, match=BATTLE_SCAN_PATTERN, count=scan_count
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise PreflightError(
                "SCAN %s cursor=%d: %s",
                _quote_go(BATTLE_SCAN_PATTERN),
                cursor,
                _err_text(exc),
                cause=exc,
            ) from exc
        for raw_key in keys:
            key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else str(raw_key)
            first_key_visit = summary.register_scanned_key(key, owner_runtime_id)
            try:
                match_id = parse_battle_record_key(key)
            except asyncio.CancelledError:
                raise
            except PreflightError as exc:
                if first_key_visit:
                    summary.add_finding(Finding(source=source, key=key, reason=exc.msg))
                continue
            body = await node.get(key)
            if body is None:
                raise PreflightError(
                    "durable battle key %s disappeared during audit", _quote_go(key)
                )
            if not summary.register_record_body(key, body):
                continue
            rec = dspb.BattleStorageRecord()
            try:
                rec.ParseFromString(body)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                summary.add_finding(
                    Finding(
                        source=source,
                        key=key,
                        match_id=match_id,
                        reason="protobuf decode failed: " + str(exc),
                    )
                )
                continue
            classification = classify_battle(match_id, rec)
            summary.record_decoded(classification.category)
            for reason in classification.reasons:
                summary.add_finding(
                    Finding(source=source, key=key, match_id=match_id, reason=reason)
                )
        if cursor == 0:
            return


async def audit_redis(rdb: Any, scan_count: int, summary: AuditSummary) -> None:
    """扫描**每一个** Redis Cluster master。对应 Go 的 `AuditRedis`。

    ★ 直接对 UniversalClient / RedisCluster 调 SCAN 只会问到**一个**分片,
      其余 hash slot 上的对局镜像会被完全漏掉 —— 而这份结果是要当发布依据的。
    ★ 每个 master 在 SCAN 前后各取一次运行时身份,变了就整轮失败:
      期间发生过 failover / 重建时,"扫过的那台"和"现在这台"不是同一个进程。
    """
    if rdb is None or summary is None:
        raise PreflightError("pod_uid preflight requires Redis, positive scan count and summary")
    if not isinstance(scan_count, int) or isinstance(scan_count, bool):
        raise PreflightError("pod_uid preflight requires Redis, positive scan count and summary")
    # Go 的 scanCount 是 int64;Python 必须显式判上界,否则一个 2^70 的 COUNT
    # 会被原样拼进命令,由 Redis 报一个与两栈无关的协议错。
    if scan_count <= 0 or scan_count > INT64_MAX:
        raise PreflightError("pod_uid preflight requires Redis, positive scan count and summary")

    primaries = getattr(rdb, "get_primaries", None)
    if callable(primaries):
        for node in primaries():
            client = _node_client(node)
            source = safe_redis_source("redis-cluster-master", _node_source_endpoint(node))
            try:
                before_id = await cluster_master_id(client)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise PreflightError(
                    "%s identity before scan: %s", source, _err_text(exc), cause=exc
                ) from exc
            try:
                await scan_redis_node(client, before_id, source, scan_count, summary)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise PreflightError(
                    "%s: %s", source, _err_text(exc), cause=exc
                ) from exc
            try:
                after_id = await cluster_master_id(client)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise PreflightError(
                    "%s identity after scan: %s", source, _err_text(exc), cause=exc
                ) from exc
            if after_id != before_id:
                raise PreflightError("%s runtime identity changed during scan", source)
            summary.register_runtime_master(before_id)
        return

    try:
        before_id = await standalone_runtime_id(rdb)
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001
        raise PreflightError(
            "Redis primary identity before scan: %s", _err_text(exc), cause=exc
        ) from exc
    await scan_redis_node(rdb, before_id, "redis-primary", scan_count, summary)
    try:
        after_id = await standalone_runtime_id(rdb)
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001
        raise PreflightError(
            "Redis primary identity after scan: %s", _err_text(exc), cause=exc
        ) from exc
    if after_id != before_id:
        raise PreflightError("Redis primary runtime identity changed during scan")
    summary.register_runtime_master(before_id)
