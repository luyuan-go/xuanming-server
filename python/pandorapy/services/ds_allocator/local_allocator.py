"""本机拉起 Windows Dedicated Server 进程的调试用 `GameServerAllocator` —— 对应 Go 侧
`services/battle/ds_allocator/internal/data/local_allocator.go`。

这是与 `agones_allocator.AgonesGameServerAllocator`(Linux 生产)并列的第二种 DS 启动
方式,专供本机联调:匹配成局后 ds_allocator 直接 exec 打包好的 UE Windows DS,分配一个
本机端口,返回真实地址(host:port)给客户端 NetDriver;Release / 心跳超时 abandoned 时
Kill 进程。三种方式共用同一套 allocator 契约,biz 逻辑零改。

设计要点(照抄 Go 头注释):
  - 进程台账(pod_name → 进程 + 端口)在内存维护,带互斥锁;退出时 `close()` 全杀。
  - 每个 DS 进程一个 reaper 协程 `wait()`,进程自行退出(崩溃)时清理台账释放端口
    (镜像仍靠心跳超时 sweep 标 abandoned,与 Agones pod 崩溃同语义)。
  - `allocate` 幂等:同 pod_name(由 match_id 派生)已在台账则直接返回原地址,不重复拉进程。
  - 启动函数 `start_proc` 抽成字段,单测可注入假进程,避免真的 exec UE。

── Python 与 Go 的并发模型差异(必须知道,否则会照抄出错误的锁)───────────────
Go 用 `sync.Mutex`,且 `buildEnv` 明写「调用方已持锁,内部绝不可再取锁(不可重入 →
死锁)」。Python 侧用 `asyncio.Lock`,同一条约定原样保留:

  - `allocate` / `set_pending_battle_roster` / `release` / `close` / `_reap` 走
    `async with self._lock`;
  - `_default_start` → `_build_env` 在 `allocate` 的锁内被调用,**直接**读写
    `_pending_rosters`,不再取锁(asyncio.Lock 同样不可重入,再取即永久挂起 ——
    表现为 DS 进程永远不被拉起、AllocateBattle 无限挂起、对局最终按空 pod 判弃)。
  - `local_instance_identity` / `local_credential_ack` 是**纯同步读**,刻意不取锁:
    它们内部没有 `await`,协程不会被抢占,而台账的每一次写都发生在某个同步片段里,
    读不到撕裂状态。加锁会把它们变成 async,污染 legacy 心跳应答那条同步路径。
"""

from __future__ import annotations

import asyncio
import dataclasses
import functools
import os
import pathlib
import socket
import sys
import uuid as _uuid
from typing import Awaitable, Callable, Protocol

from pandorapy import dsmetadata, errcode, safego
from pandorapy import log as plog
from pandorapy.services.ds_allocator import conf as dconf

# ── allocator → UE DS 的 env 契约 ───────────────────────────────────────────
#
# ★ 逐字符照抄。DS 侧 `PandoraAgonesProvider` 按这些名字读;改一个字母就是
#   「DS 起来了但拿不到身份」,而两边都不报错。

GAMESERVER_NAME_ENV = "AGONES_GAMESERVER_NAME"
MATCH_ID_ENV = "PANDORA_MATCH_ID"
MAP_ID_ENV = "PANDORA_MAP_ID"
GAME_MODE_ENV = "PANDORA_GAME_MODE"
DS_TOKEN_ENV = "PANDORA_DS_TOKEN"
DS_TYPE_ENV = "PANDORA_DS_TYPE"
REGION_ENV = "PANDORA_REGION"
BATTLE_ROSTER_ENV = "PANDORA_BATTLE_ROSTER"
ALLOCATION_ID_ENV = "PANDORA_ALLOCATION_ID"
RELEASE_TRACK_ENV = "PANDORA_RELEASE_TRACK"
BATTLE_COMBAT_FACTIONS_ENV = "PANDORA_BATTLE_COMBAT_FACTIONS"

#: 本机运行契约标记 —— 对应 Go 的 `pkg/auth.DSLocalProfileEnv` /
#: `auth.DSLocalProfileOffV1`。它**不是**授权凭据;UE 还会同时校验本地 pod 身份与
#: 非 Agones 运行态。Python 侧 `pandorapy.auth` 尚未搬这两个常量(hub_allocator 也是
#: 在自己的 conf 里定义的),故在此声明并注明出处,不散抄字面量。
DS_LOCAL_PROFILE_ENV = "PANDORA_DS_LOCAL_PROFILE"
DS_LOCAL_PROFILE_OFF_V1 = "local-off-v1"

#: extra_env 不得覆盖的内置身份 / 令牌变量(Go: `isReservedDSEnvKey` 的 switch 分支)。
RESERVED_DS_ENV_KEYS = frozenset(
    {
        DS_TOKEN_ENV,
        DS_LOCAL_PROFILE_ENV,
        GAMESERVER_NAME_ENV,
        MATCH_ID_ENV,
        MAP_ID_ENV,
        GAME_MODE_ENV,
        DS_TYPE_ENV,
        REGION_ENV,
    }
)

#: `canonical_roster_text` 的人数上限。刻意**不**复用 `dsmetadata.MAX_BATTLE_ROSTER_PLAYERS`
#: 之外的任何值 —— 两者必须同值,这里直接引用而不是抄一个 128。
MAX_BATTLE_ROSTER_PLAYERS = dsmetadata.MAX_BATTLE_ROSTER_PLAYERS

_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1


def _require_uint64(name: str, value: int) -> int:
    """Go 的形参是 uint64,负数 / 超界在那边编译不出来;Python 必须显式挡。

    不挡的后果:`pandora-battle-local--1` 这样的 pod 名会进台账,而 `PANDORA_MATCH_ID`
    env 会写成 "-1",DS 侧解析成一个不存在的对局,全链零报错。
    """
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= _UINT64_MAX:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "local_ds: %s %r out of uint64 range", name, value
        )
    return value


def _require_uint32(name: str, value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= _UINT32_MAX:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "local_ds: %s %r out of uint32 range", name, value
        )
    return value


# ── 凭据身份 ────────────────────────────────────────────────────────────────


@dataclasses.dataclass(slots=True)
class BattleCredentialIdentity:
    """中间件验签后交给权威仓的**完整**凭据身份。对应 Go 的
    `internal/data/battle_auth.go` 的同名结构。

    ★ `exp_ms` / `kid` / `token_sha256` 也属于身份,禁止退化成只比较 gen/jti。
    """

    pod_name: str = ""
    instance_uid: str = ""
    instance_epoch: int = 0  # Go: uint32
    gen: int = 0  # Go: uint64
    jti: str = ""
    exp_ms: int = 0  # Go: uint64
    kid: str = ""
    token_sha256: str = ""
    writer_epoch: int = 0  # Go: uint32

    def complete_for_ack(self) -> bool:
        """五元组是否齐备,可用于回显心跳 ACK。对应 Go 的 `completeForACK`。

        缺任一项都不得回显半截 ACK:UE 侧 `IsComplete` 会拒,回半截只会把"缺字段"
        伪装成"不匹配",更难排查。
        """
        return (
            self.instance_uid != ""
            and self.instance_epoch != 0
            and self.gen != 0
            and self.jti != ""
            and self.writer_epoch != 0
        )


#: 签发器签名:(match_id, pod_name, instance_uid, instance_epoch) -> (token, cred);失败抛异常。
#: 对应 Go 的 `dsTokenIssuer func(uint64, string, string, uint32) (string, BattleCredentialIdentity, error)`。
DSTokenIssuer = Callable[[int, str, str, int], Awaitable[tuple[str, BattleCredentialIdentity]]]

#: map_id → DS 要加载的关卡 URL。**同步**函数(Go 侧就是现查内存配置表),失败抛异常。
MapURLResolver = Callable[[int], str]


# ── 进程抽象 ────────────────────────────────────────────────────────────────


class DSProcess(Protocol):
    """已拉起的 DS 进程,便于单测注入假实现。对应 Go 的 `dsProcess` 接口。"""

    async def kill(self) -> None:
        """终止进程(已退出应为 no-op 不报致命错)。"""
        ...

    async def wait(self) -> None:
        """阻塞直到进程退出。"""
        ...


class ExecProcess:
    """`DSProcess` 的真实现,包一个 `asyncio.subprocess.Process`。对应 Go 的 `execProcess`。"""

    __slots__ = ("_proc", "_log_f")

    def __init__(self, proc: asyncio.subprocess.Process, log_f: object | None) -> None:
        self._proc = proc
        self._log_f = log_f

    @property
    def pid(self) -> int:
        return self._proc.pid

    async def kill(self) -> None:
        """终止进程树。

        ★ Windows:UE DS(PandoraServer.exe)可能派生子进程,只 kill 父进程会留下仍占
          监听端口的子进程(幽灵 DS),导致后续对局撞端口。`taskkill /T` 杀整棵进程树,
          `/F` 强制,确保端口真正释放。taskkill 不可用 / 失败 → 回退直接 kill 父进程
          (至少不泄漏父进程)。
        """
        if self._proc.returncode is not None:
            return
        if sys.platform == "win32":
            try:
                killer = await asyncio.create_subprocess_exec(
                    "taskkill",
                    "/T",
                    "/F",
                    "/PID",
                    str(self._proc.pid),
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                if await killer.wait() == 0:
                    return
            except asyncio.CancelledError:
                raise
            except OSError:
                pass  # taskkill 不在 PATH:回退父进程 kill
        try:
            self._proc.kill()
        except ProcessLookupError:
            # 进程已退出。Go 那边 `os.Process.Kill()` 在此会返回 error,但其接口注释
            # 明写「已退出则应为 no-op 不报致命错」;照注释语义收敛,避免一次正常的
            # 崩溃 + Release 竞态被报成 ErrDSAllocationFailed。
            return

    async def wait(self) -> None:
        try:
            await self._proc.wait()
        finally:
            if self._log_f is not None:
                self._log_f.close()  # type: ignore[attr-defined]


#: 启动函数签名:(pod_name, port, match_id, map_id, map_url, game_mode, token) -> DSProcess。
StartProc = Callable[[str, int, int, int, str, str, str], Awaitable[DSProcess]]

#: 端口探测:port -> 是否可绑定。`None` = 不探测(单测默认放行)。
PortProbe = Callable[[int], bool]


@dataclasses.dataclass(slots=True)
class LaunchedProc:
    """台账里的一条记录。对应 Go 的 `launchedProc`。"""

    proc: DSProcess
    port: int
    addr: str
    #: 本机 DS 的 exact 实例身份,与下发给该进程的 DS 回调令牌**同源**(`allocate` 处
    #: 一次生成,既签进令牌又留台账)。留存的意义在于幂等重分配必须返回同一身份:
    #: 身份漂移会让票据绑到一个不存在的实例。
    instance_uid: str = ""
    instance_epoch: int = 0
    #: 经 env 下发给该进程的**完整凭据身份**(与 instance_uid/instance_epoch 同一次签发)。
    #: 必须整组留存而不只留 uid/epoch:UE 的 SendBattleHeartbeat 用 IsBoundToRequest 把
    #: 心跳应答里的 CredentialAck 与 DS 自持凭据逐字段比对(uid/epoch/gen/jti/writer_epoch),
    #: 缺一即判 "heartbeat response credential ACK missing"。local 不续期 → 全程不变。
    cred: BattleCredentialIdentity = dataclasses.field(default_factory=BattleCredentialIdentity)


@dataclasses.dataclass(slots=True)
class LocalBattleRoster:
    """一局待拉起 DS 的权威准入元数据(mode=local 版的 Agones annotation)。

    对应 Go 的 `localBattleRoster`。
    """

    player_ids: list[int] = dataclasses.field(default_factory=list)
    combat_faction_by_player: dict[int, int] | None = None
    allocation_id: str = ""
    release_track: str = ""


# ── 模块级小工具 ────────────────────────────────────────────────────────────


def canonical_roster_text(player_ids: list[int]) -> str:
    """把玩家 ID 编成 UE 侧 `ParseCanonicalBattleRoster` 认的 canonical 文本:
    十进制、逗号分隔、**严格升序去重**、非零、上限 128 人。对应 Go 的 `canonicalRosterText`。

    ★ 任一条不满足 UE 会整份判非法并保持空名单(fail-closed),所以这里宁可返回空串
      不投递,也不投一份会被对面拒掉的半成品 —— 那只会把「没投」伪装成「投了但格式错」,
      更难查。

    ★ 刻意**不**复用 `dsmetadata.canonical_roster`:后者对非法输入是 raise(annotation
      路径要 fail-closed 到分配失败),而本函数的契约是「返回空串,由调用方降级为不投递
      并打 `local_battle_roster_not_canonical`」。两者方向不同,合并会让一次 roster
      手抖把整局分配打掉,而 Go 侧那条路径只是不投 env。
    """
    uniq: list[int] = []
    seen: set[int] = set()
    for pid in player_ids:
        if not isinstance(pid, int) or isinstance(pid, bool) or not 0 <= pid <= _UINT64_MAX:
            return ""
        if pid == 0:
            return ""
        if pid in seen:
            continue
        seen.add(pid)
        uniq.append(pid)
    if not uniq or len(uniq) > MAX_BATTLE_ROSTER_PLAYERS:
        return ""
    uniq.sort()
    return ",".join(str(pid) for pid in uniq)


def is_reserved_ds_env_key(key: str) -> bool:
    """env key 是否为 allocator 内置注入的身份 / 令牌变量,extra_env 不得覆盖。
    对应 Go 的 `isReservedDSEnvKey`。

    ★ **大小写不敏感**,且要先 strip。Windows(local 模式 DS 宿主)环境变量名大小写
      不敏感,`pandora_ds_token` 与 `PANDORA_DS_TOKEN` 指向同一变量;若只按精确大写
      比对,小写别名仍能覆盖真令牌(Go 审核 P1 补漏)。
    """
    return key.strip().upper() in RESERVED_DS_ENV_KEYS


def default_port_probe(port: int) -> bool:
    """探测端口在所有网卡上 UDP+TCP 是否可绑定。对应 Go 的 `defaultPortProbe`。

    UE DS NetDriver 用 UDP,保守起见 TCP 也探(兼容 TCP/WebSocket 传输)。绑定成功
    立即释放再交给 UE 启动;探测与 UE 真正绑定之间有极短 TOCTOU 窗口,但幽灵 DS 是
    持续占用能被稳定挡住,`_used_ports` 又已防 allocator 自身重复分配。

    ★ 不设 `SO_REUSEADDR`:那正好会让"已被占用"探测成功(Linux 上对 TIME_WAIT、
      Windows 上配合 SO_EXCLUSIVEADDRUSE 语义更复杂),把幽灵 DS 放过去。
    """
    try:
        uc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except OSError:
        return False
    try:
        uc.bind(("", port))
    except OSError:
        return False
    finally:
        uc.close()
    try:
        tl = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except OSError:
        return False
    try:
        tl.bind(("", port))
        tl.listen(1)
    except OSError:
        return False
    finally:
        tl.close()
    return True


# ── 分配器 ──────────────────────────────────────────────────────────────────


class LocalGameServerAllocator:
    """在本机 exec UE Windows Dedicated Server 进程。对应 Go 的 `LocalGameServerAllocator`。

    构造失败场景(Go 返 error,这里抛 `ValueError`,main 据此 fatal):
      - `executable_path` 空
      - `executable_path` 指向的文件不存在
      - `launcher=editor` 但 `project_path` 空 / 指向的 .uproject 不存在
      - `port_range <= 0`
    """

    def __init__(self, cfg: dconf.LocalDSConf) -> None:
        if cfg.executable_path == "":
            raise ValueError("local_ds: executable_path required when enabled")
        if not os.path.exists(cfg.executable_path):
            raise ValueError(f"local_ds: executable_path {cfg.executable_path!r} not found")
        # editor 形态必须能定位 .uproject:否则 UnrealEditor.exe 会把关卡路径当工程名
        # 解析失败,表现为 DS 秒退 + ready 等待超时,排查成本高 —— 在启动时 fail-fast。
        if cfg.launcher == dconf.LAUNCHER_EDITOR:
            if cfg.project_path == "":
                raise ValueError(
                    f"local_ds: project_path required when launcher={dconf.LAUNCHER_EDITOR}"
                )
            if not os.path.exists(cfg.project_path):
                raise ValueError(f"local_ds: project_path {cfg.project_path!r} not found")
        if cfg.port_range <= 0:
            raise ValueError("local_ds: port_range must be > 0")

        self._cfg = cfg
        self._lock = asyncio.Lock()
        self._procs: dict[str, LaunchedProc] = {}
        self._used_ports: set[int] = set()
        #: match_id → 待随 DS env 下发的权威准入元数据(见 `set_pending_battle_roster`)。
        #: 由 `allocate` 在 `_build_env` 时消费并删除;受 `_lock` 保护。
        self._pending_rosters: dict[int, LocalBattleRoster] = {}

        #: 拉起一个 DS 进程;单测注入假实现绕过真 exec。
        self.start_proc: StartProc = self._default_start
        #: 探测端口在本机是否真的空闲(可绑定)。`None` = 不探测(单测默认放行)。
        #: 用于挡住「台账已释放但进程未退出(幽灵 DS)」或外部程序占用的端口:否则
        #: allocator 把 `-port=X` 传给 UE DS,X 被占时 UE 会静默 fallback 到 X+1,
        #: 导致 allocator 记录 / 返回的端口(X)与 DS 实际监听端口(X+1)不一致,
        #: 新对局客户端拿新 ticket 却连到 X 上的旧 DS,被 PreLogin 拒。
        self.port_probe: PortProbe | None = default_port_probe

        self._ds_token_issuer: DSTokenIssuer | None = None
        self._ds_token_required = False
        #: 把 map_id 解析成 DS 要加载的关卡 URL,由 main 在配置表就绪时注入。唯一实现是
        #: "现查关卡表" —— 关卡数据的唯一权威源是 g_关卡.xlsx(configtable),allocator
        #: 不再留第二份手抄映射。每次 allocate 现查:配置表热更后新增的副本无需重启本
        #: 服务即可开局。`None` 表示未注入:只有配了 loader_map(DS 侧自己查表决定目标图)
        #: 时才允许,否则 `allocate` fail-closed —— 见 `_resolve_startup_map`。
        self._map_url_resolver: MapURLResolver | None = None

    # ── 依赖注入 ──────────────────────────────────────────────────────────

    def set_map_url_resolver(self, resolver: MapURLResolver | None) -> None:
        """注入「map_id → 关卡 URL」解析器。对应 Go 的 `SetMapURLResolver`。"""
        self._map_url_resolver = resolver

    def set_ds_token_issuer(self, issuer: DSTokenIssuer | None, required: bool) -> None:
        """注入 DS 回调令牌签发器(可选依赖)。对应 Go 的 `SetDSTokenIssuer`。

        `required=True`(guard=enforce / local-off-v1)时签发失败会让 `allocate` 抛错
        (fail-closed)。
        """
        self._ds_token_issuer = issuer
        self._ds_token_required = required

    # ── 分配 ─────────────────────────────────────────────────────────────

    async def allocate(
        self, match_id: int, map_id: int, game_mode: str, release_track: str
    ) -> tuple[str, str, str]:
        """拉起一个本机 DS 进程,返回 `(pod_name, host:port, release_track)`。

        对应 Go 的 `Allocate`。`release_track` **原样透传**(本机没有 Stable/Canary
        双 Fleet,轨道由上游决定并由 biz 侧复核 —— 见交付报告"语义分叉"一节)。
        """
        match_id = _require_uint64("match_id", match_id)
        map_id = _require_uint32("map_id", map_id)
        pod_name = f"pandora-battle-local-{match_id}"

        async with self._lock:
            # 幂等:同对局已拉起 → 直接返回原地址。
            existing = self._procs.get(pod_name)
            if existing is not None:
                return pod_name, existing.addr, release_track

            # 关卡在拉起进程前就解析定型:解析失败必须让整次分配失败,**绝不回退兜底图**。
            # 起错图的 DS 会被 DS 侧关卡门判 Mismatch 后自杀,分配卡在 warming 直到
            # ready_wait 超时,玩家侧只看到"一直排队中",排查成本极高(2026-08-04
            # map_id=11 实测)。这里失败则 matchmaker 立刻拿到一条写明原因的错误。
            try:
                map_url = self._resolve_startup_map(map_id)
            except Exception as map_err:  # noqa: BLE001 — 与 Go 的 `mapErr != nil` 同宽
                plog.get().error(
                    "local_ds_map_resolve_failed",
                    match_id=match_id,
                    map_id=map_id,
                    err=repr(map_err),
                    hint="关卡由 g_关卡.xlsx(configtable/dist/level.json)权威决定;"
                    "新增战斗关卡改表重导即可,不改 yaml",
                )
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "local_ds: resolve startup map for map_id %d: %s",
                    map_id,
                    map_err,
                    cause=map_err,
                ) from map_err

            port = self._pick_port_locked()
            if port is None:
                raise errcode.PandoraError(
                    errcode.ErrDSNoAvailable,
                    "local_ds: no free port in [%d,%d) for match %d",
                    self._cfg.port_base,
                    self._cfg.port_base + self._cfg.port_range,
                    match_id,
                )

            # DS 回调令牌一次性签发:在此签一次并透传给进程 env,避免"预签验证 + 启动
            # 再签"的二次签发失败只告警的空窗(enforce 下二次失败会拉起一个无令牌、
            # 回调必被拒的 DS)。
            #   enforce(_ds_token_required):签发失败 fail-closed,不拉起。
            #   off/permissive:签发失败只告警,token 置空(DS 无令牌照常运行,守卫放行)。
            ds_token = ""
            ds_cred = BattleCredentialIdentity()
            instance_uid = str(_uuid.uuid4())
            instance_epoch = 1
            if self._ds_token_issuer is not None:
                try:
                    tok, cred = await self._ds_token_issuer(
                        match_id, pod_name, instance_uid, instance_epoch
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as terr:  # noqa: BLE001 — 与 Go 的 `terr != nil` 同宽
                    if self._ds_token_required:
                        raise errcode.PandoraError(
                            errcode.ErrDSAllocationFailed,
                            "ds_callback_token sign failed under enforce for match %d: %s",
                            match_id,
                            terr,
                            cause=terr,
                        ) from terr
                    plog.get().warning(
                        "ds_callback_token_sign_failed", match_id=match_id, err=repr(terr)
                    )
                else:
                    # 签发"成功"但 tuple 不全 = 心跳 ACK 一定回显不出去(DS 会永远拿不到
                    # 准入租约、收不到 stop/驱逐指令)。required 下这属于半成品接线,
                    # 必须在拉起前就失败,而不是拉起一个注定每 5s 打一条 ACK missing 的
                    # DS(§14 接线完整性)。
                    if (
                        not cred.complete_for_ack()
                        or cred.instance_uid != instance_uid
                        or cred.instance_epoch != instance_epoch
                    ):
                        if self._ds_token_required:
                            raise errcode.PandoraError(
                                errcode.ErrDSAllocationFailed,
                                "ds_callback_token for match %d signed with "
                                "incomplete/mismatched credential tuple",
                                match_id,
                            )
                        plog.get().warning(
                            "ds_callback_credential_incomplete",
                            match_id=match_id,
                            pod=pod_name,
                            hint="心跳应答无法回显绑定式 ACK,DS 将拒收 allocator 指令",
                        )
                        ds_token = tok
                    else:
                        ds_token = tok
                        ds_cred = dataclasses.replace(cred, pod_name=pod_name)

            try:
                proc = await self.start_proc(
                    pod_name, port, match_id, map_id, map_url, game_mode, ds_token
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — 与 Go 的 `err != nil` 同宽
                raise errcode.PandoraError(
                    errcode.ErrDSAllocationFailed,
                    "local_ds: launch match %d on port %d: %s",
                    match_id,
                    port,
                    exc,
                    cause=exc,
                ) from exc

            addr = f"{self._cfg.advertise_host}:{port}"
            lp = LaunchedProc(
                proc=proc,
                port=port,
                addr=addr,
                instance_uid=instance_uid,
                instance_epoch=instance_epoch,
                cred=ds_cred,
            )
            self._procs[pod_name] = lp
            self._used_ports.add(port)

        # reaper 在锁外起:`_reap` 自己要拿锁,在锁内 spawn 只是把死锁风险提前。
        # ★ safego 的 metric label 是有界枚举,**不能**把 pod_name 拼进去(§12 高基数)。
        safego.spawn("local_ds_reap", functools.partial(self._reap, pod_name, lp))
        return pod_name, addr, release_track

    async def set_pending_battle_roster(
        self,
        match_id: int,
        player_ids: list[int],
        combat_faction_by_player: dict[int, int] | None,
        allocation_id: str,
        release_track: str,
    ) -> None:
        """在拉起 DS **之前**登记本局的权威准入元数据。对应 Go 的 `SetPendingBattleRoster`。

        为什么必须有:UE 的 `ApplyAgonesAdmissionMetadata` 只从 Agones GameServer
        annotation 装载 ExpectedPlayers 与权威阵营;mode=local 没有 Agones,两者恒空:

          - 花名册空 → `EvaluateSoloDungeonLeaveRequest` 因 roster_count==0 返回
            AuthorityNotReady,玩家能正常战斗但点「退出副本 / 失败结算」永远被拒;
          - 阵营空 → `ResolveCampForSpawn` 返回 RejectSpawn 且**禁止回退**默认 Pawn,
            玩家进图后根本没有角色,表现为"进副本就卡死"。

        本地没有 annotation 通道,故改用 env 投递同一份事实(与 `PANDORA_DS_TOKEN` 同机制)。

        必须在 `allocate` 之前调用:DS 进程在 `allocate` 内 exec,env 那一刻就已定型。
        幂等:同 match_id 重复登记以最后一次为准;`allocate` 消费后即删除,避免台账无界增长。
        入参一律深拷贝:调用方(biz)的 list / dict 在本次 RPC 返回前仍可能被复用或修改。

        ★ Go 是同步方法(拿 `sync.Mutex`),这里是 `async`(拿 `asyncio.Lock`)——
          否则它能在 `allocate` 的某个 await 点插进去改台账,而 Go 那边被互斥锁挡住。
        """
        if match_id == 0:
            return
        match_id = _require_uint64("match_id", match_id)
        ids = list(player_ids)
        factions: dict[int, int] | None = None
        if combat_faction_by_player:
            factions = dict(combat_faction_by_player)
        async with self._lock:
            self._pending_rosters[match_id] = LocalBattleRoster(
                player_ids=ids,
                combat_faction_by_player=factions,
                allocation_id=allocation_id,
                release_track=release_track,
            )

    # ── legacy 面的身份回填 ───────────────────────────────────────────────

    def local_instance_identity(self, pod_name: str) -> tuple[str, int] | None:
        """本机 DS 进程的 exact 实例身份(供 legacy 面回填战斗记录)。
        对应 Go 的 `LocalInstanceIdentity`(Go 返 `(uid, epoch, ok)`,这里 `None` = not-ok)。

        为什么必须有:legacy(非 Model B)分配路径只回 (pod, addr, track),
        BattleStorageRecord 的 gameserver_uid / instance_epoch 恒为零值,matchmaker 据此
        判「ds_allocator 未回填完整 DS 目标」拒签 v2 战斗票,对局直接判 FAILED ——
        玩家永远进不去副本(2026-08-04 mode=local 实测)。这里给出的身份**就是**签进该
        DS 回调令牌的那一组(`allocate` 处同一次生成),所以与 DS 自报身份逐字段相等。

        pod 不在台账(已回收 / 名字不符)时返回 `None` —— 调用方必须据此**不回填**,
        绝不能拿别的进程或半截身份糊一个实例身份出来。
        """
        if pod_name == "":
            return None
        lp = self._procs.get(pod_name)
        if lp is None or lp.instance_uid == "" or lp.instance_epoch == 0:
            return None
        return lp.instance_uid, lp.instance_epoch

    def local_credential_ack(self, pod_name: str) -> BattleCredentialIdentity | None:
        """下发给指定 pod 的完整凭据身份,供 legacy 心跳应答回显 ACK。
        对应 Go 的 `LocalCredentialACK`。

        为什么必须有:UE 的 SendBattleHeartbeat 无条件用 IsBoundToRequest 校验应答里的
        CredentialAck(uid/instance_epoch/gen/jti/writer_epoch 五项须与 DS 自持凭据逐
        字段相等)。不通过时它会**清空 Command 与 BattleEvictionOrders 并置错**,于是
        allocator 的 stop / drain 指令永远送不到 DS、精确驱逐单被整份丢弃、本地准入
        租约永不打开。

        为什么这不是"伪造回显":ACK 的值直接取自本进程签发、并经 env 下发给该 DS 的
        **同一份**凭据(`allocate` 处一次生成),确是"服务端仍授权本实例"的真实证据。
        且严格 fail-closed:pod 不在台账 → 不回显;凭据五元组不全 → 不回显。
        """
        if pod_name == "":
            return None
        lp = self._procs.get(pod_name)
        if lp is None or not lp.cred.complete_for_ack():
            return None
        return lp.cred

    # ── 回收 ─────────────────────────────────────────────────────────────

    async def release(self, pod_name: str) -> None:
        """终止指定 DS 进程;台账无此记录视作已释放(幂等)。对应 Go 的 `Release`。"""
        if pod_name == "":
            return
        async with self._lock:
            lp = self._procs.pop(pod_name, None)
            if lp is not None:
                self._used_ports.discard(lp.port)
        if lp is None:
            return
        try:
            await lp.proc.kill()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "local_ds: kill %s: %s",
                pod_name,
                exc,
                cause=exc,
            ) from exc

    async def close(self) -> None:
        """终止全部在管 DS 进程(ds_allocator 退出时调用,避免遗留孤儿 DS)。
        对应 Go 的 `Close`。
        """
        async with self._lock:
            procs = self._procs
            self._procs = {}
            self._used_ports = set()
        for lp in procs.values():
            try:
                await lp.proc.kill()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — Go: `_ = lp.proc.Kill()`
                pass

    async def _reap(self, pod_name: str, lp: LaunchedProc) -> None:
        """等待进程退出后清理台账释放端口。对应 Go 的 `reap`。

        ★ 只在台账里仍是**同一条记录**时才清理(`cur is lp`),避免与 release / 重拉
          竞态:否则一次"崩溃后立刻重分配同 match"会让迟到的 reaper 把新记录连同它
          正在用的端口一起删掉。
        """
        await lp.proc.wait()
        async with self._lock:
            cur = self._procs.get(pod_name)
            if cur is lp:
                del self._procs[pod_name]
                self._used_ports.discard(lp.port)

    # ── 端口 / 进程 ───────────────────────────────────────────────────────

    def _pick_port_locked(self) -> int | None:
        """在端口池里取一个空闲端口(调用方须持锁)。对应 Go 的 `pickPortLocked`。"""
        for p in range(self._cfg.port_base, self._cfg.port_base + self._cfg.port_range):
            if p in self._used_ports:
                continue
            if self.port_probe is not None and not self.port_probe(p):
                continue  # 端口被占(幽灵 DS / 外部程序)→ 跳过,避免 UE 静默换端口
            return p
        return None

    async def _default_start(
        self,
        pod_name: str,
        port: int,
        match_id: int,
        map_id: int,
        map_url: str,
        game_mode: str,
        token: str,
    ) -> DSProcess:
        """`start_proc` 的真实现:exec UE Windows DS 并把 stdout/stderr 落盘。
        对应 Go 的 `defaultStart`。
        """
        args = self._build_args(port, map_url)
        env = self._build_env(pod_name, match_id, map_id, game_mode, token)

        log_f = None
        if self._cfg.log_dir != "":
            try:
                pathlib.Path(self._cfg.log_dir).mkdir(parents=True, exist_ok=True)
                log_f = open(  # noqa: SIM115 — 生命周期与进程绑定,由 ExecProcess.wait 关
                    os.path.join(self._cfg.log_dir, pod_name + ".log"), "wb"
                )
            except OSError:
                # Go 侧同样把建目录 / 建文件失败**静默降级**为「不落盘,照常拉起」:
                # 日志落不了盘是排查不便,不是启动失败。
                log_f = None

        try:
            proc = await asyncio.create_subprocess_exec(
                self._cfg.executable_path,
                *args,
                cwd=self._cfg.working_dir or None,
                env=env,
                stdout=log_f,
                stderr=log_f,
            )
        except BaseException:
            # 这里必须用 BaseException:CancelledError 也要把刚开的日志文件关掉,
            # 否则一次停机就漏一个 fd。无条件 re-raise,取消照常穿透。
            if log_f is not None:
                log_f.close()
            raise
        return ExecProcess(proc, log_f)

    def _resolve_startup_map(self, map_id: int) -> str:
        """DS 进程「首个加载」的关卡 URL(命令行位置参数)。对应 Go 的 `resolveStartupMap`。

        两条权威路径:
          - `loader_map` 非空 → 统一启到加载 / 分发关卡;目标副本由 UE 侧 Loader
            GameMode 读 `PANDORA_MAP_ID` 查 g_关卡.xlsx 后 ServerTravel 决定
            (生产 Agones 走的就是这条)。
          - 否则 → 由注入的 `map_url_resolver` 现查关卡表拼出目标图 URL(本机默认路径)。

        两条都以 g_关卡.xlsx 为唯一事实源,区别只是"谁来查表"。**没有第三条**:解析器
        缺失或查表失败一律报错,不存在"未命中回退默认图" —— 那正是 2026-08-04 事故的
        直接成因。

        无论走哪条,`PANDORA_MAP_ID` env 都已注入,故切换只影响「首个加载哪张图」。
        """
        if self._cfg.loader_map != "":
            return self._cfg.loader_map
        if self._map_url_resolver is None:
            raise ValueError(
                "未接入关卡表:请配 config_table.dir(现查 g_关卡.xlsx)或 local_ds.loader_map"
            )
        map_url = self._map_url_resolver(map_id)
        if map_url.strip() == "":
            raise ValueError(f"map_id={map_id} 解析出的关卡 URL 为空")
        return map_url

    def _build_args(self, port: int, map_url: str) -> list[str]:
        """拼 UE DS 命令行。对应 Go 的 `buildArgs`,顺序逐字一致:

            [.uproject] + 关卡 + -server -log -port=<port> [+ CVar 覆盖] + extra_args

        ★ launcher=editor 时 `.uproject` 必须在**最前面**:UE 的 LaunchSetGameName 只把
          命令行里**第一个**不以 '-' 开头的 token 当工程 / 关卡,排在关卡 URL 之后就会
          被当成关卡名解析失败。
        ★ `-server` 两种形态都带 —— NetMode 恒为 NM_DedicatedServer,DS 子系统 / 心跳 /
          在线准入全链路与打包 DS 完全一致,后端对此无感。
        ★ editor 形态的 CVar 覆盖(关掉「缺 streaming level package 就踢人」)必须排在
          `extra_args` **之前**,运维仍可用 extra_args 覆盖回去。参数值引用
          `conf.EDITOR_LAUNCHER_CVAR_ARG`,不在这里重写字面量。
        """
        args: list[str] = []
        if self._cfg.launcher == dconf.LAUNCHER_EDITOR and self._cfg.project_path != "":
            args.append(self._cfg.project_path)
        if map_url != "":
            args.append(map_url)
        # ★ 用 -stdout 而**不是** -log,原因与 hub_allocator/local_fleet.py 同一条:
        #   `-log` 会开一个 Windows 控制台窗口,快速编辑模式下被点一下 WriteConsole 就阻塞,
        #   整个游戏线程冻死(cdb 抓栈实证:FWindowsConsoleOutputDevice::Serialize ←
        #   UIpNetDriver::TrackAndLogNewIP ← TickDispatch ← FEngineLoop::Tick)。
        #   战斗 DS 冻死比大厅更糟:一局人全卡在里面且不会结算。
        #   stdout 已由本类重定向到 log_dir,-FullStdOutLogOutput 保证全量。
        args.extend(["-server", "-stdout", "-FullStdOutLogOutput", f"-port={port}"])
        if self._cfg.launcher == dconf.LAUNCHER_EDITOR:
            args.append(dconf.EDITOR_LAUNCHER_CVAR_ARG)
        args.extend(self._cfg.extra_args)
        return args

    def _build_env(
        self, pod_name: str, match_id: int, map_id: int, game_mode: str, token: str
    ) -> dict[str, str]:
        """在当前进程环境基础上注入 DS 身份变量。对应 Go 的 `buildEnv`。

        **调用约定:调用方必须已持有 `self._lock`**(唯一链路
        `allocate → start_proc → _default_start → _build_env`,`allocate` 全程持锁)。
        本函数内部因此直接读写 `_pending_rosters`,**绝不可再取锁**(asyncio.Lock
        同样不可重入 → 永久挂起)。

        Go 用 `os.Environ()` 切片追加(后者覆盖前者);Python 用 dict 复制 + 赋值,
        语义等价且不会出现同名重复项。
        """
        env = dict(os.environ)
        env[GAMESERVER_NAME_ENV] = pod_name
        env[MATCH_ID_ENV] = str(match_id)
        env[MAP_ID_ENV] = str(map_id)
        env[GAME_MODE_ENV] = game_mode
        # 仅 Windows 本机 allocator 注入;UE 还会校验 local pod 身份与非 Agones 运行态。
        env[DS_LOCAL_PROFILE_ENV] = DS_LOCAL_PROFILE_OFF_V1
        # DS 回调服务令牌:local 模式经 env 下发(agones 模式走 annotation)。
        if token != "":
            env[DS_TOKEN_ENV] = token

        # 权威准入元数据(mode=local 版的 battle admission annotation 四件套)。
        # UE 侧要求 roster / allocation-id / release-track **同时**齐备才认(缺一即整份
        # 判非法),而 combat-factions 缺失会让 ResolveCampForSpawn 直接 RejectSpawn
        # (玩家进图没角色),故这里四者同投同不投,绝不投半份。
        roster = self._pending_rosters.pop(match_id, None)  # 消费即删,台账不随对局数无界增长
        if roster is not None:
            roster_text = canonical_roster_text(roster.player_ids)
            # `canonical_combat_factions` 同时校验"精确覆盖 roster、升序去重、非零
            # player_id、faction_id 可安全映射为 UE Camp",并按 roster 顺序输出
            # `pid=faction,...`。不自己再拼一遍:UE 的 ParseCanonicalCombatFactions 是
            # 逐项对齐 roster 的强校验,两边规则一旦漂移,投出去也会被整份判非法
            # (把"没投"伪装成"格式错",更难查)。
            factions_text = ""
            faction_err_text = ""
            try:
                _, factions_text = dsmetadata.canonical_combat_factions(
                    roster.player_ids, roster.combat_faction_by_player or {}
                )
            except ValueError as faction_err:
                faction_err_text = str(faction_err)
            if (
                roster_text != ""
                and roster.allocation_id != ""
                and roster.release_track != ""
                and faction_err_text == ""
            ):
                env[BATTLE_ROSTER_ENV] = roster_text
                env[ALLOCATION_ID_ENV] = roster.allocation_id
                env[RELEASE_TRACK_ENV] = roster.release_track
                env[BATTLE_COMBAT_FACTIONS_ENV] = factions_text
            else:
                plog.get().warning(
                    "local_battle_roster_not_canonical",
                    match_id=match_id,
                    players=len(roster.player_ids),
                    factions=len(roster.combat_faction_by_player or {}),
                    allocation_id=roster.allocation_id,
                    release_track=roster.release_track,
                    faction_err=faction_err_text,
                    hint="四件套不全或 roster/阵营非 canonical,不投递;UE 侧将保持空名单与空阵营,"
                    "主动退出会被判 AuthorityNotReady、玩家出生会被 RejectSpawn(进图无角色)",
                )

        # extra_env 追加,但严禁覆盖内置身份 / 令牌变量(审核 P1:extra_env 覆盖
        # PANDORA_DS_TOKEN 会用静态 / 伪造令牌替换真签发令牌,绕过范围绑定)。
        # 保留字命中即跳过并告警。
        for k, v in self._cfg.extra_env.items():
            if is_reserved_ds_env_key(k):
                plog.get().warning(
                    "extra_env_reserved_key_ignored",
                    key=k,
                    hint="extra_env 不得覆盖 PANDORA_DS_TOKEN / PANDORA_MATCH_ID / "
                    "AGONES_GAMESERVER_NAME 等内置变量",
                )
                continue
            env[k] = v
        return env
