"""账号密码哈希 —— 对应 Go 侧 `pkg/passwd`(golang.org/x/crypto/bcrypt)。

契约(与 Go 逐条同):
  - 客户端上行的是**密码摘要**(SHA-256),不是明文;服务端对摘要再做 bcrypt 落库。
  - 落库串就是 bcrypt 的 `$2a$` / `$2b$` 格式,两栈互认:
    Go 的 x/crypto/bcrypt 解版本时只校 major('2')、minor 任意,所以 Python 侧
    产出的 `$2b$` 在 Go 上验得过;反向同理(Python 的 bcrypt 接受 `$2a$`)。
    这条互认是**灰度期能两栈并存**的前提 —— 若两边格式不通,同一个账号在
    Go 副本上能登、Python 副本上登不上,而两边日志都只报"密码错误"。
  - cost 固定 DEV_COST=4,与 Go 侧 `passwd.Hash(passwordHash, passwd.DevCost)`
    的**实际调用**一致(不是 ProdCost;2026-08-09 已查证过 login 只用 DevCost)。
    这里刻意不"顺手调高":cost 是写死在两栈代码里的,Python 单方面改成 10 会让
    Python 副本写出的哈希在 Go 上依然验得过(cost 存在串里),但两栈注册的账号
    登录耗时差 64 倍,压测/容量结论直接不可比。要改就两栈一起改。

★ 为什么必须是 bcrypt、不能用 hashlib 里的东西替代:
  库里既有的 `accounts.password_hash` 全是 bcrypt 串。换算法 = 全服老账号一个都
  验不过,而表现只是"密码错误" —— 没有任何一条日志会说"算法换了"。

★ 为什么缺包时是 fail-fast 而不是降级:
  唯一"能跑"的降级形态是跳过校验,那等于任何密码都能登任何账号。
  所以本模块在缺包时**不提供任何可用路径**,由 main.py 的启动闸
  `passwd_backend_required` 在装配期拒启。
"""

from __future__ import annotations

from pandorapy import errcode

try:  # pragma: no cover —— 分支取决于环境是否装了 bcrypt
    import bcrypt as _bcrypt

    AVAILABLE = True
    IMPORT_ERROR = ""
except ImportError as _exc:  # pragma: no cover
    _bcrypt = None  # type: ignore[assignment]
    AVAILABLE = False
    IMPORT_ERROR = str(_exc)

# 与 Go 的 passwd.DevCost / ProdCost 同值。
DEV_COST = 4
PROD_COST = 10

# bcrypt 的输入上限是 72 字节(算法固有,不是实现限制)。Go 的 x/crypto/bcrypt
# 对超长输入返回错误,Python 的 bcrypt 5.x 直接抛 ValueError —— 两边都不静默截断。
# 客户端送的是 SHA-256 hex(64 字节),正常永远不会撞到这条。
MAX_DIGEST_BYTES = 72


class PasswdBackendMissingError(RuntimeError):
    """bcrypt 包缺失。只在装配期抛,业务路径上永远看不到它。"""


def require_backend() -> None:
    """启动闸用:缺包就抛。放在 main.py 的密码相关装配之前。

    不设这道闸会怎样:第一次有真实玩家登录时才 ImportError,
    而那时进程已经 Ready、流量已经切过来。
    """
    if not AVAILABLE:
        raise PasswdBackendMissingError(
            f"login 需要 bcrypt 做密码校验,但导入失败:{IMPORT_ERROR};"
            "装 `bcrypt>=4.1`(已在 pyproject 依赖里)。绝不允许跳过密码校验降级运行"
        )


def hash_password(client_digest: str, cost: int = DEV_COST) -> str:
    """对客户端摘要做 bcrypt。对应 Go 的 `passwd.Hash`。

    cost 越界时回落 DEV_COST —— 与 Go 的 `cost < MinCost || cost > MaxCost` 同判据。
    """
    require_backend()
    if cost < 4 or cost > 31:
        cost = DEV_COST
    raw = client_digest.encode("utf-8")
    if len(raw) > MAX_DIGEST_BYTES:
        # 与 Go 同为显式错误。截断在这里是最危险的选择:两个不同密码会哈希成同一串。
        raise errcode.PandoraError(
            errcode.ErrInvalidArg,
            "passwd: digest too long (%d bytes, bcrypt max %d)",
            len(raw),
            MAX_DIGEST_BYTES,
        )
    return _bcrypt.hashpw(raw, _bcrypt.gensalt(rounds=cost)).decode("ascii")


def verify(stored: str, client_digest: str) -> bool:
    """比对。匹配返回 True,不匹配返回 False。对应 Go 的 `passwd.Verify`。

    ★ 哈希串**格式坏**(不是 bcrypt)时同样返回 False,而不是抛异常:
    Go 侧那种情况会把原始错误透传,调用方(biz.login)一律翻成"凭据错误"。
    在这里收口成 False 是为了让上层只有一条失败路径 —— 否则"库里存了脏数据"
    这种运维事故会被表达成一个 500,玩家看到的是"服务器错误"而不是"密码错误",
    而两者的排查方向完全不同。真要发现脏数据靠的是注册路径的写入约束,不是登录路径。
    """
    require_backend()
    try:
        return bool(
            _bcrypt.checkpw(client_digest.encode("utf-8"), stored.encode("utf-8"))
        )
    except (ValueError, TypeError):
        # ValueError: 哈希串不是合法 bcrypt / 摘要超过 72 字节。
        # 两者都归"这次校验没通过",不升级成服务端故障。
        return False
