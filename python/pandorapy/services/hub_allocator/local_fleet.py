"""本机 exec 常驻 Windows Hub DS 的 `HubFleetProvider`(mode=local)—— 对应 Go 侧
`services/battle/hub_allocator/internal/biz/local_fleet.go`。

与 ds_allocator 的本机分配器对称,是 Windows 单机自测时大厅 DS 的来源:首次
`list_shards` 时懒拉起「一个」常驻 Hub DS 进程(加载 hub 关卡 / PandoraHubGameMode),
把它作为该 region 唯一的 `ShardCandidate` 返回;进程随 hub_allocator 退出由 `close()` Kill。

与战斗 DS 的差异:Hub DS 是常驻分片(不按对局回收),所以这里只起一个进程并长期持有,
不做端口池 / 多实例 / reaper 回收。topology-only:**不实现** `HubFleetScaler`,
故本机模式下 autoscale / consolidation 不会运行(与 Mock 同语义)。

── Python 与 Go 的并发模型差异(必须知道,否则会照抄出多余的锁)──────────────
Go 用 `sync.Once` + `sync.Mutex`,因为心跳来自**另一个 goroutine**,`once.Do` 的
happens-before 覆盖不到它。Python 这里全部在**单个事件循环**内:

  - 懒拉起用 `asyncio.Lock` + `_started` 标志复刻 `sync.Once` —— 不能只用标志,
    因为 `_start()` 里有 `await`,两个并发的 `list_shards` 会在同一个 await 点交错,
    双双看到 `_started is False` 然后**各拉起一个 DS 进程**(端口冲突、后一个秒退)。
  - `_cred` / `_start_err` 是普通属性:读写之间没有 await,协程不会被抢占,
    所以不需要额外的锁。这里刻意不加 `asyncio.Lock` —— 加了既不增加安全性,
    又会诱导后来者以为"这个字段可能被并行修改",反过来掩盖真正的时序问题。
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
import pathlib
import uuid
from typing import Awaitable, Callable

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import releasetrack
from pandorapy.services.hub_allocator import conf as hconf
from pandorapy.services.hub_allocator.fleet import ShardCandidate

# DS 回调服务令牌的下发变量名。local 模式经 env **一次性**下发(进程常驻无法改 env,
# 不支持续期);agones 模式走 annotation,可续期。
DS_TOKEN_ENV = "PANDORA_DS_TOKEN"
GAMESERVER_NAME_ENV = "AGONES_GAMESERVER_NAME"
DS_TYPE_ENV = "PANDORA_DS_TYPE"
REGION_ENV = "PANDORA_REGION"

# extra_env 不得覆盖的内置身份 / 令牌变量(Go: isReservedHubDSEnvKey 的 switch 分支)。
# `DS_LOCAL_PROFILE_ENV` 从 conf 引用,不手抄字面量。
RESERVED_HUB_DS_ENV_KEYS = frozenset(
    {
        DS_TOKEN_ENV,
        hconf.DS_LOCAL_PROFILE_ENV,
        GAMESERVER_NAME_ENV,
        DS_TYPE_ENV,
        REGION_ENV,
    }
)


@dataclasses.dataclass(slots=True)
class LocalHubCredential:
    """mode=local 一次性下发给本机 Hub DS 的**完整凭据身份**。对应 Go 的同名结构。

    必须整组留存,不能只留 gen:UE 的 SendHubHeartbeat 会用 IsBoundToRequest 把心跳
    应答里的 CredentialAck 与 DS 自持凭据逐字段比对(InstanceUID / InstanceEpoch /
    Gen / JTI / WriterEpoch),任一缺失即判 "heartbeat response credential ACK
    missing" —— 而 `IsAcceptingNewPlayers()` 对 local-off-v1 **没有豁免**
    (豁免只在验票档 IsOnlineVerificationRequired),拿不到绑定式 ACK 就等于准入
    租约永不打开,玩家连上大厅也进不去。
    """

    instance_uid: str = ""
    protocol_epoch: int = 0  # Go: uint32
    gen: int = 0  # Go: uint64
    jti: str = ""
    writer_epoch: int = 0  # Go: uint32
    expires_at_ms: int = 0  # Go: int64

    def complete(self) -> bool:
        """五元组是否齐备,可用于回显 ACK。对应 Go 的 `Complete`。

        缺任一项都不得回显半截 ACK:UE 侧 IsComplete 会拒,回半截只会把"缺字段"
        伪装成"不匹配",更难排查。
        """
        return (
            self.instance_uid != ""
            and self.protocol_epoch != 0
            and self.gen != 0
            and self.jti != ""
            and self.writer_epoch != 0
        )


# 签发器签名:(pod, instance_uid, protocol_epoch) -> (token, cred);失败抛异常。
DSTokenIssuer = Callable[[str, str, int], Awaitable[tuple[str, LocalHubCredential]]]


def hub_map_url_with_max_players(map_name: str, capacity: int) -> str:
    """把 `?MaxPlayers=<capacity>` 拼进关卡 URL,已存在则要求**逐字等于** capacity。

    对应 Go 的 `hubMapURLWithMaxPlayers`,判据逐条同序、同严格度。

    为什么不能"已经写了就照用":UE 的 MaxPlayers 决定 DS 实际接多少人,Redis 容量
    账本按 `capacity` 记账。两者不等的后果是**没有任何一侧报错**:账本说还有座,
    DS 直接拒连,玩家反复"进大厅失败"而全链日志绿的。

    为什么连 `"08"` / `"+8"` 这种也拒(`key_value[1] != canonical`):它们解析出来
    确实等于 8,但说明 yaml 是人手改的、且与 capacity 字段是两处独立维护 ——
    这次碰巧一致,下次改 capacity 时必然漏改一处。宁可启动就 fail-fast。
    """
    if capacity <= 0:
        raise ValueError("capacity must be positive")
    parts = map_name.split("?")
    found = False
    for option in parts[1:]:
        key_value = option.split("=", 1)
        # Go: strings.EqualFold(strings.TrimSpace(keyValue[0]), "MaxPlayers")
        if key_value[0].strip().lower() != "maxplayers":
            continue
        if found or len(key_value) != 2:
            raise ValueError("map_name contains duplicate/invalid MaxPlayers option")
        canonical = str(capacity)
        try:
            parsed = int(key_value[1])
        except ValueError:
            parsed = None
        if parsed != capacity or key_value[1] != canonical:
            raise ValueError(f"map_name MaxPlayers must exactly equal capacity {capacity}")
        found = True
    if found:
        return map_name
    return f"{map_name}?MaxPlayers={capacity}"


def is_reserved_hub_ds_env_key(key: str) -> bool:
    """env key 是否为 fleet 内置注入的身份 / 令牌变量,extra_env 不得覆盖。

    对应 Go 的 `isReservedHubDSEnvKey`。

    ★ **大小写不敏感**,且要先 strip。Windows(local 模式 Hub DS 的宿主)环境变量名
      大小写不敏感,小写别名 `pandora_ds_token` 与内置大写名指向同一个变量。
      精确比对会放行小写覆盖,等于让 extra_env 用一个静态 / 伪造令牌顶掉真签发令牌,
      绕过令牌的范围绑定(Go 审核 P1 补漏)。
    """
    return key.strip().upper() in RESERVED_HUB_DS_ENV_KEYS


class LocalHubFleetProvider:
    """在本机 exec 一个常驻 UE Windows Hub Dedicated Server 进程。

    对应 Go 的 `LocalHubFleetProvider`;构造失败场景与 Go 的
    `NewLocalHubFleetProvider` 一致(Go 返 error,这里抛 `ValueError`,main 据此 fatal):

      - `executable_path` 为空
      - `executable_path` 指向的文件不存在
      - `launcher=editor` 但 `project_path` 为空 / 指向的 .uproject 不存在
      - `map_name` 的 MaxPlayers 与 capacity 不一致(见 `hub_map_url_with_max_players`)
    """

    def __init__(self, cfg: hconf.LocalHubConf) -> None:
        if cfg.executable_path == "":
            raise ValueError("local_hub: executable_path required when mode=local")
        if not os.path.exists(cfg.executable_path):
            raise ValueError(f"local_hub: executable_path {cfg.executable_path!r} not found")
        # editor 形态必须能定位 .uproject:否则 UnrealEditor.exe 会把关卡路径当工程名
        # 解析失败,表现为 Hub DS 秒退、客户端登录后连不上大厅 —— 在启动时 fail-fast。
        if cfg.launcher == hconf.LAUNCHER_EDITOR:
            if cfg.project_path == "":
                raise ValueError(
                    f"local_hub: project_path required when launcher={hconf.LAUNCHER_EDITOR}"
                )
            if not os.path.exists(cfg.project_path):
                raise ValueError(f"local_hub: project_path {cfg.project_path!r} not found")
        try:
            map_url = hub_map_url_with_max_players(cfg.map_name, cfg.capacity)
        except ValueError as exc:
            raise ValueError(f"local_hub: {exc}") from exc

        self._cfg = cfg
        # 每次进程启动生成唯一实例名(对齐线上 Agones「GameServer 名每次唯一」语义):
        # 旧进程被杀后残留的 Redis 分片记录会因名字不再匹配而成为「不在 Fleet live 集」
        # 的孤儿,被拓扑对账清理;新进程用新名建一条全新 ready 记录,不复用旧的 draining。
        # 与心跳存活复活是双保险:UUID 治「身份复用」,复活治「活 pod 被误判超时」。
        self._pod_name = "pandora-hub-local-" + str(uuid.uuid4())[:8]
        self._instance_uid = str(uuid.uuid4())
        self._protocol_epoch = 1
        self._addr = f"{cfg.advertise_host}:{cfg.port}"
        self._map_url = map_url

        self._ds_token_issuer: DSTokenIssuer | None = None
        self._ds_token_required = False
        self._cred = LocalHubCredential()

        self._start_lock = asyncio.Lock()
        self._started = False
        self._start_err: BaseException | None = None
        self._proc: asyncio.subprocess.Process | None = None
        self._log_f = None

    @property
    def pod_name(self) -> str:
        return self._pod_name

    def set_ds_token_issuer(self, issuer: DSTokenIssuer | None, required: bool) -> None:
        """注入 DS 回调令牌签发器(可选依赖;须在首次 `list_shards` 前调用)。

        对应 Go 的 `SetDSTokenIssuer`。required=True 时签发失败则懒拉起失败
        (fail-closed)。local-off-v1 虽不做服务端 Guard,UE 仍强制完整凭据,
        故 main 也必须把它设为 True,防止启动"能连但所有回调都发不出"的半成品。
        """
        self._ds_token_issuer = issuer
        self._ds_token_required = required

    def local_credential_ack(self, pod: str) -> LocalHubCredential | None:
        """返回下发给指定 pod 的完整凭据身份,供心跳应答回显 ACK。

        对应 Go 的 `LocalCredentialACK`(Go 返 `(cred, ok)`,这里用 `None` 表示 not-ok)。

        pod 不匹配或凭据不全时返回 None —— 调用方必须据此**不回显**,绝不能拿别的
        pod 或半截身份糊一个 ACK 出去。
        """
        if pod == "" or pod != self._pod_name:
            return None
        cred = self._cred
        if not cred.complete():
            return None
        return cred

    async def list_shards(self, region: str) -> list[ShardCandidate]:
        """返回本机唯一的 Hub 分片(并在首次调用时懒拉起常驻 Hub DS 进程)。

        对应 Go 的 `ListShards`。
        """
        await self._ensure_started()
        # required(ds_token_required):启动失败(含 DS 回调令牌签发失败)则不返回候选
        # 分片,否则 ensure_shards 会据此在 Redis 种一条 ready 记录、把客户端路由到一个
        # 未拉起 / 回调必被 enforce 守卫全拒的 Hub(fail-closed,对齐 agones 路径的
        # 「签发失败跳过候选」)。
        if self._ds_token_required and self._start_err is not None:
            raise errcode.PandoraError(
                errcode.ErrHubNoAvailable,
                "local hub ds not started because required credential/start failed: %s",
                self._start_err,
            )
        if region == "":
            region = self._cfg.region
        # 分片镜像里的 gen 必须与 env 下发给 DS 的凭据同源;签发被跳过(off/permissive
        # 且非 required)时 cred 为零值,token_gen 回落 0 与旧行为一致。
        cred = self._cred
        return [
            ShardCandidate(
                pod_name=self._pod_name,
                addr=self._addr,
                region=region,
                shard_id=1,
                capacity=self._cfg.capacity,
                release_track=releasetrack.STABLE,
                token_ready=True,
                token_gen=cred.gen,
                # exact 实例身份:与 _build_env 下发给 Hub DS 的凭据同源(同一
                # instance_uid / protocol_epoch),因此分片镜像里的身份与 DS 自报身份
                # 天然相等,不存在两处各写一份的漂移。
                instance_uid=self._instance_uid,
                protocol_epoch=self._protocol_epoch,
            )
        ]

    async def _ensure_started(self) -> None:
        """懒拉起常驻 Hub DS 进程(仅一次)。对应 Go 的 `ensureStarted` + `sync.Once`。

        off / permissive 下拉起失败只记日志:客户端仍会拿到分片地址,连接失败时由其
        自身重试 / 报错,便于排查 DS 启动问题。enforce 下把错误记入 `_start_err`,
        `list_shards` 据此 fail-closed 不返回候选。

        ★ 失败**不重试**(Go 的 once 语义):重试会在 UE 可执行文件路径配错这种
          必然失败的场景下,每轮 reconcile 拉起一次进程 —— 几分钟内攒出成百个僵尸
          UE 进程,把开发机拖死。失败一次就停,让人去看日志改配置。
        """
        if self._started:
            return
        async with self._start_lock:
            if self._started:
                return
            self._started = True
            try:
                await self._start()
            except asyncio.CancelledError:
                # 取消不是启动失败:把 _started 退回去,让下次调用重新尝试。
                # 吞成 start_err 会让一次优雅停机把本机 DS 永久标记为不可用。
                self._started = False
                raise
            except Exception as exc:  # noqa: BLE001 — 与 Go 的 `if err := l.start()` 同宽
                self._start_err = exc
                plog.get().error(
                    "local_hub_ds_start_failed",
                    err=repr(exc),
                    executable=self._cfg.executable_path,
                    addr=self._addr,
                    enforce=self._ds_token_required,
                    hint="检查 local_hub.executable_path / map_name;enforce 下将 fail-closed 不返回候选",
                )
                return
            plog.get().info(
                "local_hub_ds_started",
                pod=self._pod_name,
                addr=self._addr,
                map=self._cfg.map_name,
            )

    async def _start(self) -> None:
        """真正 exec UE Windows Hub DS 并把 stdout/stderr 落盘。对应 Go 的 `start`。"""
        args = self._build_args()
        env = await self._build_env()  # 签发失败在这里抛(enforce)

        log_f = None
        if self._cfg.log_dir != "":
            try:
                pathlib.Path(self._cfg.log_dir).mkdir(parents=True, exist_ok=True)
                log_f = open(  # noqa: SIM115 — 生命周期与进程绑定,close() 里关
                    os.path.join(self._cfg.log_dir, self._pod_name + ".log"), "wb"
                )
            except OSError:
                # Go 侧同样把建目录 / 建文件失败**静默降级**为「不落盘,照常拉起」:
                # 日志落不了盘是排查不便,不是启动失败;为它 fail-closed 会让一个
                # 只读的 log_dir 配置把整个本机自测环境卡死。
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

        self._proc = proc
        self._log_f = log_f

    def _build_args(self) -> list[str]:
        """拼 UE DS 命令行。对应 Go 的 `buildArgs`,顺序逐字一致。

            [.uproject] + 大厅关卡URL + -server -log -port=<port> [+ CVar覆盖] + extra_args

        ★ launcher=editor 时 `.uproject` 必须在**最前面**:UE 的 LaunchSetGameName
          只把命令行里**第一个**不以 '-' 开头的 token 当工程 / 关卡,排在关卡 URL
          之后就会被当成关卡名解析失败。
        ★ `-server` 两种形态都带 —— NetMode 恒为 NM_DedicatedServer,大厅心跳 /
          SetLocationHub / 在线准入与打包 DS 完全一致。
        ★ editor 形态的 CVar 覆盖(关掉「缺 streaming level package 就踢人」)必须排在
          `extra_args` **之前**,运维仍可用 extra_args 覆盖回去。
          参数值引用 `conf.EDITOR_LAUNCHER_CVAR_ARG`,不在这里重写字面量 ——
          完整成因、引擎出处与作用域理由都写在该常量的声明处。
        """
        args: list[str] = []
        if self._cfg.launcher == hconf.LAUNCHER_EDITOR and self._cfg.project_path != "":
            args.append(self._cfg.project_path)
        if self._map_url != "":
            args.append(self._map_url)
        # ★ 用 -stdout 而**不是** -log:`-log` 会给 DS 开一个真的 Windows 控制台窗口,
        #   而 Windows 控制台默认开着「快速编辑」—— 只要有人在那个黑窗口里点一下或选中
        #   文字,WriteConsole 就会一直阻塞,把**整个游戏线程**冻住。
        #
        #   2026-08-23 实测(cdb 非侵入抓栈,游戏线程):
        #       FWindowsConsoleOutputDevice::Serialize   ← 阻塞在这
        #         ← UE::Logging::Private::BasicLog
        #         ← UIpNetDriver::TrackAndLogNewIP       ← 第一个客户端连入时打的那行
        #         ← UIpNetDriver::TickDispatch ← UWorld::Tick ← FEngineLoop::Tick
        #   现象:DS accept 了连接后 CPU 归零、心跳停止、日志一行不出、进程仍 Responding;
        #   客户端 20 秒收不到任何回包后 ConnectionTimeout 退回登录界面。
        #   也就是说:**一次误点就能让整个大厅永久不可进**(§9.20 不得让玩家进不去场景)。
        #
        #   -stdout 让日志直接走标准输出(本类已经把 stdout 重定向到 log_dir 下的文件),
        #   不再创建控制台窗口,从机制上消灭这个死锁。-FullStdOutLogOutput 保证是全量而
        #   非精简输出。实测:88KB 完整日志照常落盘,DS 正常启动。
        args.extend(["-server", "-stdout", "-FullStdOutLogOutput", f"-port={self._cfg.port}"])
        if self._cfg.launcher == hconf.LAUNCHER_EDITOR:
            args.append(hconf.EDITOR_LAUNCHER_CVAR_ARG)
        args.extend(self._cfg.extra_args)
        return args

    async def _build_env(self) -> dict[str, str]:
        """注入 Hub DS 身份变量(对齐 UE DS 侧读取的 env)。对应 Go 的 `buildEnv`。

        Go 用 `os.Environ()` 切片追加(后者覆盖前者);Python 用 dict 复制 +
        赋值,语义等价且不会出现同名重复项。
        """
        env = dict(os.environ)
        env[GAMESERVER_NAME_ENV] = self._pod_name
        env[DS_TYPE_ENV] = "hub"
        env[REGION_ENV] = self._cfg.region
        # local-off-v1 是 Windows 本机联调专用的机械隔离契约。UE 还会同时校验本地 pod
        # 前缀与非 Agones 运行态,Linux / Agones 不会因误注入单一变量而降级。
        env[hconf.DS_LOCAL_PROFILE_ENV] = hconf.DS_LOCAL_PROFILE_OFF_V1

        # DS 回调服务令牌:local 模式经 env 一次性下发(agones 模式走 annotation 可续期)。
        # required(ds_token_required):签发失败 fail-closed 不拉起;否则失败只告警照拉。
        if self._ds_token_issuer is not None:
            try:
                token, cred = await self._ds_token_issuer(
                    self._pod_name, self._instance_uid, self._protocol_epoch
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — 与 Go 的 `if ..., err != nil` 同宽
                if self._ds_token_required:
                    raise RuntimeError(
                        f"required hub_ds_token sign failed for pod {self._pod_name}: {exc}"
                    ) from exc
                plog.get().warning(
                    "hub_ds_token_sign_failed", pod=self._pod_name, err=repr(exc)
                )
            else:
                # 签发失败以外的所有路径都必须让 cred 与 env 里的 token 严格同源:
                # 心跳 ACK 就是拿它逐字段回显的,漂移一处 UE 即判 mismatched。
                if not cred.complete() and self._ds_token_required:
                    raise RuntimeError(
                        f"required hub_ds_token for pod {self._pod_name} "
                        "is missing ACK identity fields"
                    )
                self._cred = cred
                env[DS_TOKEN_ENV] = token

        # extra_env 追加,但严禁覆盖内置身份 / 令牌变量(审核 P1:extra_env 覆盖
        # PANDORA_DS_TOKEN 会用静态 / 伪造令牌替换真签发令牌,绕过范围绑定)。
        # 保留字命中即跳过并告警。
        for k, v in self._cfg.extra_env.items():
            if is_reserved_hub_ds_env_key(k):
                plog.get().warning(
                    "extra_env_reserved_key_ignored",
                    key=k,
                    hint=(
                        "extra_env 不得覆盖 PANDORA_DS_TOKEN / AGONES_GAMESERVER_NAME / "
                        "PANDORA_DS_TYPE 等内置变量"
                    ),
                )
                continue
            env[k] = v
        return env

    async def close(self) -> None:
        """终止常驻 Hub DS 进程(hub_allocator 退出时调用,避免遗留孤儿)。

        对应 Go 的 `Close`。
        """
        proc, log_f = self._proc, self._log_f
        self._proc, self._log_f = None, None
        if proc is not None and proc.returncode is None:
            try:
                proc.kill()
            except ProcessLookupError:
                # 进程已自行退出。Go 侧 `_ = cmd.Process.Kill()` 同样忽略这个错误。
                pass
        if log_f is not None:
            log_f.close()
