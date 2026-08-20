"""配置表加载 —— 与 Go 侧 pkg/configtable 的口径一致(manifest + sha256 + 整批 fail-closed)。

为什么必须严格对齐:
    配表是**与 UE 客户端同源**的策划数值,唯一真源是 xlsx → configtable/dist/*.json。
    dist 目录的 checksum 口径是"含尾换行的 LF 全字节 sha256"(仓库里已有一次事故:
    手改 dist 导致 checksum 不匹配)。Python 侧若算法或字节口径不同,会出现:
      - Go 版能加载、Python 版拒启(或反过来)
      - 更糟:Python 版放过了一个被篡改的批次
    所以这里逐条照抄 Go 侧规则,不做"优化"。

Go 侧规则(pkg/configtable/store.go:56 Load):
    1. 读 manifest.json,拿 version + 每张表的 file/proto/checksum/rows
    2. expectVersion != 0 时要求 manifest.version 恰等于它(发布脚本确认批次已生效)
    3. 逐表:读原始字节 → VerifyChecksum(sha256 全字节) → protojson 解析 → 行数比对
       → 表内校验;**任一失败 → 整批不切换**
    4. manifest 未列出的 *.json 视为脏数据,只告警不拒载
    5. 缺本进程必需的表 → 整批拒绝

JSON 解析用 protobuf 的 json_format 而不是裸 dict:
    这样字段名、类型、默认值语义与 Go 侧 protojson 完全一致(比如 uint32 的 0 值、
    bool 的省略),不会因为 Python dict 的宽松处理放过表里的类型错误。
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import pathlib
from typing import Any

from google.protobuf import json_format

from pandora.config.v1 import dialogue_pb2 as _cfg_dialogue_pb2

MANIFEST_FILE_NAME = "manifest.json"

# 单个对话节点的选项上限 = 源表成对列的组数。
# 对应 Go 的 configtable.DialogueMaxOptions;加选项 = 加表列 + 加 proto 字段 + 改本常量。
DIALOGUE_MAX_OPTIONS = 3


class ConfigTableError(RuntimeError):
    """配置表加载失败 —— 调用方应打日志后退出进程(fail-closed,不降级)。"""


@dataclasses.dataclass(frozen=True, slots=True)
class ManifestTable:
    name: str
    file: str
    proto: str
    checksum: str
    rows: int


@dataclasses.dataclass(frozen=True, slots=True)
class Manifest:
    version: int
    generated_at_ms: int
    generator: str
    source_rev: str
    tables: dict[str, ManifestTable]


@dataclasses.dataclass(frozen=True, slots=True)
class DialogueOptionView:
    """某节点上一个**已填写**的选项。

    index 是选项在源表里的序号(1 基),同时充当协议层 option_id —— 它随表稳定,
    不受行序 / 文案改动影响。对应 Go 的 configtable.DialogueOptionView。
    """

    index: int
    text: str
    # next_node_id 选完跳转的节点 id;0 = 选完即结束对话。
    next_node_id: int


def read_manifest(active_dir: str | pathlib.Path) -> Manifest:
    """读 manifest.json。对应 Go 的 configtable.ReadManifest。"""
    path = pathlib.Path(active_dir) / MANIFEST_FILE_NAME
    if not path.is_file():
        raise ConfigTableError(f"配置表 active 目录缺少 {MANIFEST_FILE_NAME}: {path}")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigTableError(f"{path} 解析失败: {exc}") from exc

    # ★ 三道结构闸,逐条对应 Go 的 ReadManifest。少一道都不会当场报错,
    # 只会让**坏批次被当成好批次加载**:
    version = int(raw.get("version", 0))
    if version <= 0:
        # 版本单调是热更流水线的防回退依据(§9.15)。version=0 的批次一旦被接受,
        # 后续任何批次都"更新",防回退直接失效。
        raise ConfigTableError(f"{path} 的 version 必须 > 0(实为 {version})")

    tables: dict[str, ManifestTable] = {}
    for i, entry in enumerate(raw.get("tables", [])):
        mt = ManifestTable(
            name=str(entry.get("name", "")),
            file=str(entry.get("file", "")),
            proto=str(entry.get("proto", "")),
            checksum=str(entry.get("checksum", "")),
            rows=int(entry.get("rows", 0)),
        )
        if not mt.name:
            raise ConfigTableError(f"{path} 的 tables[{i}] name 为空")
        if mt.name in tables:
            raise ConfigTableError(f"manifest 中表名重复: {mt.name}")
        # ★ 文件名钉死为 <name>.json —— 这一条同时消灭两件事:
        #   ① 文件名与表名漂移(改了文件名却没改表名,加载的是另一张表的内容);
        #   ② **路径逃逸**:file 写成 "../../etc/passwd" 或绝对路径时,
        #      下面的 `dir / mt.file` 会**跳出 active 目录**(pathlib 对绝对路径
        #      是整个替换基路径,不是拼接)。manifest 是发布产物,但发布链上任何一环
        #      被写坏都不该让加载器去读目录外的文件。
        if mt.file != f"{mt.name}.json":
            raise ConfigTableError(
                f"表 {mt.name!r} 的 file 必须是 {mt.name + '.json'!r},实为 {mt.file!r}"
            )
        if not mt.checksum.startswith("sha256:"):
            raise ConfigTableError(f"表 {mt.name!r} 的 checksum 缺少 sha256: 前缀")
        tables[mt.name] = mt
    if not tables:
        raise ConfigTableError(f"{path} 未列出任何表")
    return Manifest(
        version=version,
        generated_at_ms=int(raw.get("generated_at_ms", 0)),
        generator=str(raw.get("generator", "")),
        source_rev=str(raw.get("source_rev", "")),
        tables=tables,
    )


def verify_checksum(raw: bytes, expect: str) -> None:
    """校验 sha256。对应 Go 的 configtable.VerifyChecksum(manifest.go:82)。

    口径:对**文件原始全字节**求 sha256(含尾换行)。不做任何规范化 —— 不 strip、
    不转行尾。仓库里已经因为手改 dist 触发过 checksum 不匹配,那正是这道闸的作用。
    """
    if not expect.startswith("sha256:"):
        raise ConfigTableError(f"checksum 格式非法(期望 sha256:<hex>): {expect!r}")
    want = expect[len("sha256:") :].lower()
    got = hashlib.sha256(raw).hexdigest()
    if got != want:
        raise ConfigTableError(
            f"checksum 不匹配: 期望 {want} 实际 {got}"
            f"(dist 目录被手改过?dist 是生成产物,改源表重新导出,勿手改)"
        )


class DialogueTable:
    """对话表 —— 一行 = 一个对话节点,同 npc_id 的全部行组成一棵树。

    对应 Go 的 configtable.DialogueTable。索引在构造期一次建好,之后纯读(并发安全)。
    """

    __slots__ = ("_rows", "_by_id", "_by_npc", "_start_of")

    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows
        self._by_id: dict[int, Any] = {}
        self._by_npc: dict[int, list[Any]] = {}
        for i, row in enumerate(rows):
            # ★ 主键为 0 即拒批(与 Go 的 newDialogueTable 同一道闸)。
            #
            # 0 是 protobuf 的默认值:一行"什么都没填"的空行解析出来 id 就是 0。
            # 放过它的后果是**一整行垃圾进了索引** —— 而且 `_by_id[0]` 还会被
            # 后来的空行覆盖,表现为"某个对话节点查出来是另一个"。
            # 更糟的是 is_start 为真的空行会成为该 NPC 的起始节点(npc_id 也是 0)。
            if row.id == 0:
                raise ConfigTableError(f"dialogue 第 {i + 1} 行主键为 0")
            if row.id in self._by_id:
                raise ConfigTableError(f"对话表节点 id 重复: {row.id}")
            self._by_id[row.id] = row
            self._by_npc.setdefault(row.npc_id, []).append(row)
        # 起始节点索引在这里建;唯一性由 validate_dialogue_table 保证。
        self._start_of: dict[int, Any] = {}
        for row in rows:
            if row.is_start:
                self._start_of.setdefault(row.npc_id, row)

    def count(self) -> int:
        return len(self._rows)

    def all(self) -> list[Any]:
        return list(self._rows)

    def by_id(self, node_id: int) -> Any | None:
        return self._by_id.get(node_id)

    def list_by_npc_id(self, npc_id: int) -> list[Any]:
        return self._by_npc.get(npc_id, [])

    def start_node_of(self, npc_id: int) -> Any | None:
        """返回该 NPC 的起始节点;不存在返回 None,调用方 fail-closed。"""
        return self._start_of.get(npc_id)


def dialogue_options(row: Any) -> list[DialogueOptionView]:
    """返回该节点**已填写**的选项(按表内顺序)。空列表 = 终止节点。

    对应 Go 的 configtable.DialogueOptions:遇到第一个空文本即到尾
    (选项从 1 起连续由 validate_dialogue_row 保证)。
    """
    cols = (
        (row.option1_text, row.option1_next),
        (row.option2_text, row.option2_next),
        (row.option3_text, row.option3_next),
    )
    out: list[DialogueOptionView] = []
    for idx, (text, nxt) in enumerate(cols, start=1):
        if not text:
            break
        out.append(DialogueOptionView(index=idx, text=text, next_node_id=nxt))
    return out


def validate_dialogue_row(row: Any) -> None:
    """逐行形状校验 —— 对应 Go 的 validateDialogueRow(dialogue.go:74)。

    两条约束都是为了让"空选项"只有一种表达,否则策划漏填一格会得到一个点不动的死选项,
    而表面上表是合法的:
      - 选项必须从 1 起连续填(不得空 1 填 2)
      - 选项文本为空时后继必须为 0(有后继没文案 = 玩家永远看不到也点不到的分支)
    """
    cols = (
        (row.option1_text, row.option1_next),
        (row.option2_text, row.option2_next),
        (row.option3_text, row.option3_next),
    )
    ended = False
    for idx, (text, nxt) in enumerate(cols, start=1):
        if not text:
            ended = True
            if nxt != 0:
                raise ConfigTableError(
                    f"节点 {row.id}:选项{idx} 文本为空但填了后继 {nxt}(空选项的后继必须为 0)"
                )
            continue
        if ended:
            raise ConfigTableError(
                f"节点 {row.id}:选项{idx} 有文本但前面的选项是空的(选项须从 1 起连续填)"
            )


def validate_dialogue_table(table: DialogueTable | None) -> None:
    """批次级校验 —— 对应 Go 的 ValidateDialogueTable(dialogue.go:96)。

    补齐 (excel_fk) 因禁止自引用而覆盖不到的两件事:
      - 每个 npc_id 恰好一个起始节点(0 个 → StartDialogue 永远进不去;多个 → 入口不确定)
      - 每个非 0 后继必须存在,且与来源节点同属一个 NPC(跨 NPC 跳转会让会话的 npc_id
        与实际节点分叉,展示的说话人和内容对不上)
    """
    if table is None:
        raise ConfigTableError("缺少 dialogue 配置表")

    starts: dict[int, int] = {}  # npc_id → 已见起始节点 id
    for row in table.all():
        if not row.is_start:
            continue
        if row.npc_id in starts:
            raise ConfigTableError(
                f"NPC {row.npc_id} 有多个起始节点({starts[row.npc_id]} 与 {row.id});"
                f"每个 NPC 必须恰好一个"
            )
        starts[row.npc_id] = row.id

    for row in table.all():
        if row.npc_id not in starts:
            raise ConfigTableError(
                f"NPC {row.npc_id} 没有起始节点(需要有一行「起始节点」填 1)"
            )
        for opt in dialogue_options(row):
            if opt.next_node_id == 0:
                continue
            nxt = table.by_id(opt.next_node_id)
            if nxt is None:
                raise ConfigTableError(
                    f"节点 {row.id}:选项{opt.index} 的后继 {opt.next_node_id} 不存在于对话表"
                )
            if nxt.npc_id != row.npc_id:
                raise ConfigTableError(
                    f"节点 {row.id}(NPC {row.npc_id}):选项{opt.index} 的后继 "
                    f"{opt.next_node_id} 属于 NPC {nxt.npc_id}(不允许跨 NPC 跳转)"
                )


@dataclasses.dataclass(slots=True)
class LoadResult:
    version: int
    source_rev: str
    warnings: list[str]
    dialogue: DialogueTable | None = None


def load_dialogue(
    active_dir: str | pathlib.Path, expect_version: int = 0
) -> LoadResult:
    """加载 dialogue 表(dialogue 服务当前只需要这一张)。

    整批 fail-closed:manifest 缺表 / checksum 不符 / 解析失败 / 行数不符 /
    表内校验不过 —— 任一条都抛异常,不返回半个批次。对应 Go 的 Store.Load 语义。
    """
    active = pathlib.Path(active_dir)
    if not active.is_dir():
        raise ConfigTableError(f"配置表目录不存在: {active}")

    manifest = read_manifest(active)
    if expect_version and manifest.version != expect_version:
        raise ConfigTableError(
            f"manifest 版本不符: 期望 {expect_version} 实际 {manifest.version}"
        )

    mt = manifest.tables.get("dialogue")
    if mt is None:
        raise ConfigTableError("manifest 缺少本进程必需的表 'dialogue',整批拒绝")

    expect_proto = "pandora.config.v1.DialogueTableData"
    if mt.proto != expect_proto:
        raise ConfigTableError(
            f"dialogue 表 proto 不符: 期望 {expect_proto} 实际 {mt.proto}(接错文件?)"
        )

    path = active / mt.file
    if not path.is_file():
        raise ConfigTableError(f"manifest 列出的表文件不存在: {path}")
    raw = path.read_bytes()
    verify_checksum(raw, mt.checksum)

    container = _cfg_dialogue_pb2.DialogueTableData()
    try:
        # ★ ignore_unknown_fields=True —— 与 Go 侧 pkg/configtable/store.go 的
        # unmarshalTable(protojson + DiscardUnknown)一致,**不是**放松校验。
        #
        # 严格校验属于**生成阶段**(导表器负责);运行期必须容忍新增列,否则:
        # 标准发布序是「先发配置、再滚二进制」,新 dist 一加列,尚未滚上的旧进程
        # 就会在热加载时整批拒载 —— 整个共存窗口(§9.21)被打穿。
        # Go 侧有 store_test.go 的 TestLoadTolerateUnknownField 钉住这条。
        json_format.Parse(raw.decode("utf-8"), container, ignore_unknown_fields=True)
    except json_format.ParseError as exc:
        raise ConfigTableError(f"{path} protojson 解析失败: {exc}") from exc

    rows = list(container.rows)
    # ★ 判据是 `len(rows) != mt.rows`,**不是** `if mt.rows and ...`。
    #
    # 加了 `if mt.rows` 之后,manifest 声明 rows=0 的表会**整条跳过**行数校验 ——
    # 而这道校验防的正是"发布拷贝被截断":一份被截成 0 行的表配上一份声明 0 行的
    # manifest,两边"自洽",服务照常启动,对话树整个是空的。
    # rows 是发布器写的,它写 0 本身就该被当成异常而不是"不校验"。
    if len(rows) != mt.rows:
        raise ConfigTableError(
            f"dialogue 表行数不符: manifest 声明 {mt.rows} 实际 {len(rows)}"
        )
    for row in rows:
        validate_dialogue_row(row)

    table = DialogueTable(rows)
    validate_dialogue_table(table)

    # manifest 未列出的 *.json 是脏数据 —— 告警不拒载(hotreload doc §5:
    # 服务端只加载 manifest 列出的表)。
    listed = {MANIFEST_FILE_NAME} | {t.file for t in manifest.tables.values()}
    warnings = [
        f"active 目录存在 manifest 未列出的文件 {p.name!r}(脏数据)"
        for p in sorted(active.glob("*.json"))
        if p.name not in listed
    ]

    return LoadResult(
        version=manifest.version,
        source_rev=manifest.source_rev,
        warnings=warnings,
        dialogue=table,
    )


# ── 热更互斥 ────────────────────────────────────────────────────────────

class ReloadMutex:
    """`ReloadConfigTable` 的批次切换互斥。对齐 Go `pkg/configtable/store.go` 的 `s.mu`。

    ★ 没有它的后果**不是**"多热更一次",是**版本静默回退**。

    Python 侧原先的形状是:

        current = store.tables.version          # ← 读在 await 之前
        result  = await asyncio.to_thread(load_tables, ...)   # ← 让出事件循环
        if result.version < current: reject
        store.replace(result.tables)

    两个并发 reload(运维双击、发布脚本重试、canary 与 stable 同时触发)会这样交错:

        A 读 current=4 → A 让出
        B 读 current=4 → B 加载到 v6 → B 判 6>4 通过 → B 切到 v6
        A 加载到 v5    → A 判 5>4 通过(**用的是过期的 4**) → A 切回 v5

    结果:内存里生效 v5,而两次热更都返回成功、运维视图上是 v6。此后所有人对
    "现在生效的是哪批"的判断都是错的 —— 这正是 §9.15 要求 version 单调递增
    想防的事,单调闸本身却因为读了过期基准而被绕过。

    Go 里 `Store.Load` 整段持锁,load + 单调检查 + 切换是一个原子段。这里用
    `asyncio.Lock` 复现同一个边界:**`current_version` 必须在锁内读**。

    用法:

        async with store.reload_mutex:
            current = store.tables.version      # ← 锁内读,基准才是新鲜的
            result = await asyncio.to_thread(load_tables, store.active_dir, expect)
            ...单调检查...
            store.replace(result.tables)

    读侧照旧无锁:`replace()` 是单次属性赋值,读者要么看到旧的一整批、要么看到
    新的一整批,不需要参与互斥。
    """

    __slots__ = ("_lock",)

    def __init__(self) -> None:
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> "ReloadMutex":
        await self._lock.acquire()
        return self

    async def __aexit__(self, *exc) -> None:  # noqa: ANN002
        self._lock.release()

    @property
    def locked(self) -> bool:
        return self._lock.locked()
