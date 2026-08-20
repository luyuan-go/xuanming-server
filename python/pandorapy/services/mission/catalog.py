"""任务域配置表 —— 对应 Go 侧 pkg/configtable/{mission,condition,reward}.go
+ services/social/mission/cmd/mission/configtable.go。

★ 为什么 mission 必须强依赖配置表(main.py 缺 config_table.dir 直接拒启):
  接取校验(类型互斥)、进度判定(条件比较符 / 槽位过滤 / clamp)、发奖内容
  (奖励表 + 装备/堆叠路由)**全部**读表。没有表的 mission 不是"功能少一点",
  是一个会把每次接取都判成"配置不存在"的空壳。

★ 加载语义逐条照抄 Go 的 Store.Load,任一失败**整批不切换**:
    manifest 缺表 / proto 名不符 / 文件缺失 / checksum 不符 / protojson 解析失败 /
    行数与 manifest 声明不符 / 逐行校验不过 / 跨表校验器不过。
  manifest 未列出的 *.json 视为脏数据,**只告警不拒载**(hotreload doc §5)。

★ 复用 pandorapy.configtable 的 read_manifest / verify_checksum,不再抄一份:
  checksum 的字节口径(含尾换行的 LF 全字节 sha256)已经踩过一次事故,
  两份实现必然漂移,而漂移的表现是"Go 版拒载的批次 Python 版放行"。

★ **批次快照粒度 = 一次领域操作**,不是一次方法调用(对齐 Go 的 configtable.go 头注释)。
  一次 ApplyFactsTx 事务回调里 mission_by_id / condition_by_id / reward_by_id /
  is_equipment 会被调用几十次(每活跃任务 × 每条件槽 × 每事实),中间只要发生一次
  reload 原子切换,同一个事务就会读到**两个批次的混合数据**。最坏的一条:
  build_reward_log 用批次 A 拿奖励条目、用批次 B 决定装备/堆叠冻结位 —— 冻结位与
  奖励内容出身不同批次,而冻结位的整个存在意义就是"路由必须与快照同源"。
  所以 CatalogSource.snapshot() 在操作入口取一次并钉住,整个回调复用同一份。
"""

from __future__ import annotations

import dataclasses
import pathlib

from google.protobuf import json_format

from pandora.config.v1 import item_pb2 as _item_pb2
from pandora.config.v1 import mission_pb2 as _cfg_mission_pb2
from pandora.mission.v1 import mission_pb2 as _mission_pb2

from pandorapy.configtable import (
    MANIFEST_FILE_NAME,
    ConfigTableError,
    read_manifest,
    verify_checksum,
)

# ── 上限常量(与 Go 逐个同值;放宽任何一条都等于放行一类配置事故)────────────────

# 单任务条件槽上限。与存储直接挂钩:player_mission_active.progress 是
# MissionProgressStorageRecord pb(VARBINARY(256)),槽数上限是该列「集合条目上限」闸。
MAX_MISSION_CONDITION_SLOTS = 8

# 任务表行数上限。这不是"手滑护栏",是 §9.18 读取侧上限的**前提**:
# player_mission_done 每玩家每任务至多一行,完成集规模 = 任务表行数;而完成集同时在
# **写路径**上(每次事务 FOR UPDATE 全量载入),行锁数与事务时长随它线性增长。
MAX_MISSION_ROWS = 2000

# 单任务后续链条数上限(完成扇出逐条 accept,每条失败一条 WARN)。
MAX_MISSION_NEXT_IDS = 16

# 单个槽位过滤集合的取值条数上限(§9.24 深度②)。槽位匹配是线性扫描:
# max_facts_per_report × max_active_missions × 8 槽 × 本上限次比较,填大了纯烧 CPU。
MAX_CONDITION_SLOT_VALUES = 32

# 单条奖励里**装备**类道具的数量上限。装备没有堆叠,发放前按件展开成 instance 列表,
# 数量**直接等于切片长度** —— 数量列手滑成 1e8 时分配的是一亿元素的列表,发放侧当场
# OOM,且快照落库后每轮补扫再炸一次(§16.5)。堆叠/货币不受此限。
MAX_REWARD_EQUIPMENT_INSTANCES = 64

# 单条奖励的道具条目数上限(§9.24 深度②)。reward_pb 列是 VARBINARY(2048),光靠
# 字节闸要到 ~200 条才拦得住 —— §9.24 明令"按列类型上限设限"不算数。
MAX_REWARD_ITEM_ENTRIES = 32

# ── 条件类别 / 比较符 ────────────────────────────────────────────────────────

# ★ 类别号从 proto 生成物取,**不手抄数字**:它是跨语言共享的线上契约
# (DS / Go / 配置表三方按它匹配条件行),抄错了不会报错,只表现为链上后环任务
# 永远收不到"前环已完成"事实(进度恒 0)。本仓刚修完 13 处手抄错位。
CONDITION_CATEGORY_COMPLETE_MISSION = int(
    _mission_pb2.MISSION_CONDITION_CATEGORY_COMPLETE_MISSION
)
CONDITION_CATEGORY_MAX = int(_mission_pb2.MISSION_CONDITION_CATEGORY_PICKUP_ITEM)

# 比较符(「比较符」列取值;继承 D 版 kComparisonFunctions 的下标语义)。
# 这一组**没有** proto 枚举,只在配置表列语义里定义,故只能是常量。
CONDITION_COMPARE_GE = 0  # 进度 >= 目标
CONDITION_COMPARE_GT = 1  # 进度 >  目标
CONDITION_COMPARE_LE = 2  # 进度 <= 目标
CONDITION_COMPARE_LT = 3  # 进度 <  目标
CONDITION_COMPARE_EQ = 4  # 进度 == 目标
CONDITION_COMPARE_MAX = 4

MAX_UINT32 = 2**32 - 1

# 装备判定:equip_slot > 0(与 Go 的 ItemTable.IsEquipment 同款)。
# 不用 item_type 是刻意的 —— Go 判的就是 equip_slot,改判据会让同一批表在两端路由
# 到不同的幂等键(:inst vs :stack),同一条奖励被发两次。


# ── CSV 列解析(对应 Go 的 pkg/configtable/csvcol.go)─────────────────────────


def parse_uint32_csv(text: str) -> list[int]:
    """解析逗号分隔的 uint32 数组。空串 = 空数组(合法)。

    空元素("1,,2" / 尾逗号)、非十进制数字、超出 uint32 一律报错 —— 这道闸在
    加载边界,放过之后业务代码拿到的是一个"看着正常"的短数组:条件槽少一格,
    任务判定按更少的条件算,直接白送完成。
    """
    s = (text or "").strip()
    if not s:
        return []
    out: list[int] = []
    for i, part in enumerate(s.split(",")):
        p = part.strip()
        if not p:
            raise ConfigTableError(f"第 {i + 1} 个元素为空(连续/尾部逗号)")
        if not p.isdigit():
            # 刻意不接受 "+1" / "-1" / "0x10":Go 用 strconv.ParseUint(p, 10, 32),
            # Python 的 int() 会吃下 "+1" 与前后空白,口径必须窄到与 Go 相同。
            raise ConfigTableError(f"第 {i + 1} 个元素 {p!r} 不是合法 uint32")
        v = int(p)
        if v > MAX_UINT32:
            raise ConfigTableError(f"第 {i + 1} 个元素 {p!r} 超出 uint32")
        out.append(v)
    return out


def must_uint32_csv(text: str) -> list[int]:
    """取已过加载期校验的数组列。防御:拿到未校验文本时返回空而不是脏数据。"""
    try:
        return parse_uint32_csv(text)
    except ConfigTableError:
        return []


# ── 条件判定件(对应 Go 的 pkg/configtable/condition.go)───────────────────────


def condition_slot_filters(cond) -> list[list[int]]:  # noqa: ANN001
    """四个槽位过滤集合(空槽 = 空列表 = 不过滤)。"""
    return [
        must_uint32_csv(cond.slot1),
        must_uint32_csv(cond.slot2),
        must_uint32_csv(cond.slot3),
        must_uint32_csv(cond.slot4),
    ]


def condition_effective_target(cond, target_override: int) -> int:  # noqa: ANN001
    """有效目标值:任务行「条件目标」>0 优先,否则条件行「目标值」。"""
    if target_override > 0:
        return target_override
    return cond.target_count


def condition_is_fulfilled(cond, progress: int, target_override: int) -> bool:  # noqa: ANN001
    """进度是否满足条件。非法比较符返回 False(加载期已挡,这里是防御)。"""
    target = condition_effective_target(cond, target_override)
    op = cond.comparison_op
    if op == CONDITION_COMPARE_GE:
        return progress >= target
    if op == CONDITION_COMPARE_GT:
        return progress > target
    if op == CONDITION_COMPARE_LE:
        return progress <= target
    if op == CONDITION_COMPARE_LT:
        return progress < target
    if op == CONDITION_COMPARE_EQ:
        return progress == target
    return False


def condition_min_fulfilling_progress(comparison_op: int, target: int) -> tuple[int, bool]:
    """「刚好达标」的最小进度,以及**该比较符能否用在单调累加计数器上**。

    任务进度是单调不减的累加器(饱和加 + 达标槽不再累加),所以比较符的达标集合
    必须**向上闭合**:一旦为真,更大的进度也必须为真。
        GE 向上闭合 ✓ 最小达标值 = target
        GT 向上闭合 ✓ 最小达标值 = target+1(target=MaxUint32 时无解)
        LE / LT     ✗ 进度=0 就为真,越推进越假 → 白送完成
        EQ          ✗ 单点集合,amount>1 一步跨过后永不再等 → 任务永久完不成
    """
    if comparison_op == CONDITION_COMPARE_GE:
        return target, True
    if comparison_op == CONDITION_COMPARE_GT:
        if target == MAX_UINT32:
            return MAX_UINT32, False
        return target + 1, True
    return 0, False


def condition_clamp_if_fulfilled(cond, progress: int, target_override: int) -> int:  # noqa: ANN001
    """达标后把进度 clamp 到**最小达标值**,未达标原样返回。

    必须 clamp 到最小达标值而不是 target 本身 —— 否则 GT 会被 clamp 打回未达标:
    target=5 的 GT 条件,进度 6 达标 → clamp 到 5 → 再判 `5 > 5` 为假 → 任务回到
    未完成而进度写死 5;下一条事实推到 6 又被 clamp 回 5,**永久活锁**。
    clamp 的不变量是「clamp 不得改变达标与否」。
    """
    if not condition_is_fulfilled(cond, progress, target_override):
        return progress
    target = condition_effective_target(cond, target_override)
    floor, ok = condition_min_fulfilling_progress(cond.comparison_op, target)
    if not ok:
        return progress
    return floor if progress > floor else progress


def condition_matches_event_slots(cond, event_slot_values) -> bool:  # noqa: ANN001
    """事实槽位值是否命中本条件的槽位过滤。

    - 只有配置了取值集合的槽才参与判定(空槽跳过);
    - 事实第 N 槽的值必须落在条件第 N 槽集合内;
    - 全部非空槽命中才算匹配;一个非空槽都没有 = 匹配任意同类事实;
    - 事实槽位数不足时缺位的非空槽判**不命中**(fail-closed,与 D 版一致)。
    """
    filters = condition_slot_filters(cond)
    configured = 0
    matched = 0
    for i, flt in enumerate(filters):
        if not flt:
            continue
        configured += 1
        if i >= len(event_slot_values):
            continue
        if event_slot_values[i] in flt:
            matched += 1
    return configured == 0 or matched == configured


# ── 逐行校验(对应 Go 的 validate*Row,由 newXxxTable 调用)────────────────────


def validate_mission_row(row) -> None:  # noqa: ANN001
    cond_ids = parse_uint32_csv(row.condition_ids)
    if not cond_ids:
        raise ConfigTableError(f"mission {row.id}:条件ID为空;无条件的任务永远无法完成")
    if len(cond_ids) > MAX_MISSION_CONDITION_SLOTS:
        raise ConfigTableError(
            f"mission {row.id}:条件数 {len(cond_ids)} 超上限 "
            f"{MAX_MISSION_CONDITION_SLOTS}(进度列存储闸,疑似手滑)"
        )
    for i, cid in enumerate(cond_ids):
        if cid == 0:
            raise ConfigTableError(f"mission {row.id}:条件ID第 {i + 1} 个元素为 0")

    targets = parse_uint32_csv(row.target_counts)
    if targets and len(targets) != len(cond_ids):
        raise ConfigTableError(
            f"mission {row.id}:条件目标数组长度 {len(targets)} 与条件ID数组长度 "
            f"{len(cond_ids)} 不等(空 = 全用条件行目标;非空必须等长)"
        )

    next_ids = parse_uint32_csv(row.next_mission_ids)
    if len(next_ids) > MAX_MISSION_NEXT_IDS:
        raise ConfigTableError(
            f"mission {row.id}:后续任务数 {len(next_ids)} 超上限 {MAX_MISSION_NEXT_IDS}"
        )
    for i, nid in enumerate(next_ids):
        if nid == 0:
            raise ConfigTableError(f"mission {row.id}:后续任务第 {i + 1} 个元素为 0")
        if nid == row.id:
            raise ConfigTableError(f"mission {row.id}:后续任务包含自身,直接自环")

    if row.auto_reward > 0 and row.reward_id == 0:
        raise ConfigTableError(
            f"mission {row.id}:自动发奖开启但奖励ID为 0;自动发放无内容必是配置错"
        )


def validate_condition_row(row) -> None:  # noqa: ANN001
    cat = row.condition_category
    if cat == 0 or cat > CONDITION_CATEGORY_MAX:
        raise ConfigTableError(
            f"condition {row.id}:条件类别 {cat} 不在 [1,{CONDITION_CATEGORY_MAX}];"
            "类别不认识的条件永远不会被任何事实命中"
        )
    if row.comparison_op > CONDITION_COMPARE_MAX:
        raise ConfigTableError(
            f"condition {row.id}:比较符 {row.comparison_op} 不在 [0,{CONDITION_COMPARE_MAX}]"
        )
    for i, slot in enumerate((row.slot1, row.slot2, row.slot3, row.slot4)):
        vals = parse_uint32_csv(slot)
        if len(vals) > MAX_CONDITION_SLOT_VALUES:
            raise ConfigTableError(
                f"condition {row.id}:槽位{i + 1} 取值条数 {len(vals)} 超上限 "
                f"{MAX_CONDITION_SLOT_VALUES}(槽位匹配是线性扫描,填大了纯烧 CPU)"
            )
        for v in vals:
            if v == 0:
                raise ConfigTableError(
                    f"condition {row.id}:槽位{i + 1} 含元素 0;槽位过滤值是配置 ID/量值,0 必是手滑"
                )


def validate_reward_row(row) -> None:  # noqa: ANN001
    ids = parse_uint32_csv(row.item_ids)
    counts = parse_uint32_csv(row.item_counts)
    if len(ids) != len(counts):
        raise ConfigTableError(
            f"reward {row.id}:道具ID数组长度 {len(ids)} 与道具数量数组长度 {len(counts)} 不等"
        )
    if len(ids) > MAX_REWARD_ITEM_ENTRIES:
        raise ConfigTableError(
            f"reward {row.id}:道具条目数 {len(ids)} 超上限 {MAX_REWARD_ITEM_ENTRIES}"
            "(reward_pb 落库列 2048 字节,按设计期望而非列容量设限)"
        )
    seen: set[int] = set()
    for i, iid in enumerate(ids):
        if iid == 0:
            raise ConfigTableError(f"reward {row.id}:道具ID第 {i + 1} 个元素为 0")
        if iid in seen:
            raise ConfigTableError(
                f"reward {row.id}:道具ID {iid} 重复;同一奖励里重复道具应合并数量"
            )
        seen.add(iid)
        if counts[i] == 0:
            raise ConfigTableError(f"reward {row.id}:道具 {iid} 的数量为 0;发 0 个必是手滑")
    if not ids and row.exp == 0:
        raise ConfigTableError(
            f"reward {row.id}:空奖励行(无道具且经验为 0);"
            "任务表想表达无奖励应把奖励ID填 0,而不是指向空行"
        )


# ── 批次容器 ─────────────────────────────────────────────────────────────────


@dataclasses.dataclass(slots=True)
class Tables:
    """一个已校验的原子批次。热更时整份替换,读侧永远看到自洽的一批。"""

    version: int
    source_rev: str
    missions: dict[int, object]
    conditions: dict[int, object]
    rewards: dict[int, object]
    # equip_slot>0 的道具集合。只存判定结果而不是整张道具表:mission 对道具表的
    # 唯一需求就是「这件是不是装备」,存整表白占内存也让人误以为可以随手多读几列。
    equipment_items: frozenset[int]
    item_ids: frozenset[int]

    def mission_count(self) -> int:
        return len(self.missions)

    def condition_count(self) -> int:
        return len(self.conditions)

    def reward_count(self) -> int:
        return len(self.rewards)


@dataclasses.dataclass(slots=True)
class LoadResult:
    version: int
    source_rev: str
    warnings: list[str]
    tables: Tables


_TABLE_PROTOS: dict[str, tuple[str, type]] = {
    "mission": ("pandora.config.v1.MissionTableData", _cfg_mission_pb2.MissionTableData),
    "condition": (
        "pandora.config.v1.ConditionTableData",
        _cfg_mission_pb2.ConditionTableData,
    ),
    "reward": ("pandora.config.v1.RewardTableData", _cfg_mission_pb2.RewardTableData),
    "item": ("pandora.config.v1.ItemTableData", _item_pb2.ItemTableData),
}


def _load_one(active: pathlib.Path, manifest, name: str) -> list:  # noqa: ANN001
    """加载并校验单张表,返回行列表。任一步失败抛 ConfigTableError(整批不切换)。"""
    mt = manifest.tables.get(name)
    if mt is None:
        raise ConfigTableError(f"manifest 缺少本进程必需的表 {name!r},整批拒绝")
    expect_proto, container_cls = _TABLE_PROTOS[name]
    if mt.proto != expect_proto:
        raise ConfigTableError(
            f"{name} 表 proto 不符: 期望 {expect_proto} 实际 {mt.proto}(接错文件?)"
        )
    path = active / mt.file
    if not path.is_file():
        raise ConfigTableError(f"manifest 列出的表文件不存在: {path}")
    raw = path.read_bytes()
    verify_checksum(raw, mt.checksum)

    container = container_cls()
    try:
        # ★ ignore_unknown_fields=True 与 Go 的 protojson + DiscardUnknown 一致,
        # **不是**放松校验:严格校验属于生成阶段。标准发布序是「先发配置、再滚二进制」,
        # 新 dist 一加列,尚未滚上的旧进程就会在热加载时整批拒载 —— 共存窗口(§9.21)
        # 被打穿。
        json_format.Parse(raw.decode("utf-8"), container, ignore_unknown_fields=True)
    except json_format.ParseError as exc:
        raise ConfigTableError(f"{path} protojson 解析失败: {exc}") from exc

    rows = list(container.rows)
    # ★ 判据是 `!=`,**不是** `if mt.rows and ...`:manifest 声明 rows=0 的表会整条
    # 跳过行数校验,而这道校验防的正是"发布拷贝被截断" —— 一份被截成 0 行的表配一份
    # 声明 0 行的 manifest,两边"自洽",服务照常启动,任务表整个是空的。
    if len(rows) != mt.rows:
        raise ConfigTableError(
            f"{name} 表行数不符: manifest 声明 {mt.rows} 实际 {len(rows)}"
        )
    return rows


def _index_by_id(name: str, rows: list) -> dict[int, object]:
    """按主键建索引。主键为 0 / 重复一律拒批次(对应 Go 生成代码里的同款校验)。"""
    out: dict[int, object] = {}
    for row in rows:
        if row.id == 0:
            raise ConfigTableError(f"{name} 表存在主键为 0 的行")
        if row.id in out:
            raise ConfigTableError(f"{name} 表主键重复: {row.id}")
        out[row.id] = row
    return out


def load_tables(active_dir: str | pathlib.Path, expect_version: int = 0) -> LoadResult:
    """加载任务域四张表(mission / condition / reward / item)。整批 fail-closed。"""
    active = pathlib.Path(active_dir)
    if not active.is_dir():
        raise ConfigTableError(f"配置表目录不存在: {active}")

    manifest = read_manifest(active)
    if expect_version and manifest.version != expect_version:
        raise ConfigTableError(
            f"manifest 版本不符: 期望 {expect_version} 实际 {manifest.version}"
        )

    mission_rows = _load_one(active, manifest, "mission")
    condition_rows = _load_one(active, manifest, "condition")
    reward_rows = _load_one(active, manifest, "reward")
    item_rows = _load_one(active, manifest, "item")

    for row in mission_rows:
        validate_mission_row(row)
    for row in condition_rows:
        validate_condition_row(row)
    for row in reward_rows:
        validate_reward_row(row)

    tables = Tables(
        version=manifest.version,
        source_rev=manifest.source_rev,
        missions=_index_by_id("mission", mission_rows),
        conditions=_index_by_id("condition", condition_rows),
        rewards=_index_by_id("reward", reward_rows),
        equipment_items=frozenset(r.id for r in item_rows if r.equip_slot > 0),
        item_ids=frozenset(r.id for r in item_rows),
    )
    validate_mission_cross_tables(tables)

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
        tables=tables,
    )


# ── 跨表校验(对应 Go 的 ValidateMissionCrossTables)──────────────────────────


def validate_mission_cross_tables(tb: Tables) -> None:
    """批次级门禁:数组列跨表存在性 + next_mission_ids 链环 + 比较符可用性 + 装备累计件数。

    fk 注解只支持单值 uint32 列(reward_id 已用),数组列的引用完整性只能在这里兜。
    链环必须**加载期**拒绝:运行期完成扇出的 16 轮迭代上限只是纵深兜底,不是许可。
    """
    if len(tb.missions) > MAX_MISSION_ROWS:
        raise ConfigTableError(
            f"任务表行数 {len(tb.missions)} 超上限 {MAX_MISSION_ROWS};"
            "完成集 player_mission_done 每玩家每任务一行,§9.18「读取侧上限」靠的正是"
            "这条写入侧硬上限,没有它 ListMissions 与事务内 load_state 都会无界增长"
        )

    for mid in sorted(tb.missions):
        row = tb.missions[mid]
        for i, cid in enumerate(must_uint32_csv(row.condition_ids)):
            cond = tb.conditions.get(cid)
            if cond is None:
                raise ConfigTableError(f"mission {mid} 引用不存在的条件 {cid}")
            target = condition_effective_target(cond, mission_slot_target(row, i))
            _, ok = condition_min_fulfilling_progress(cond.comparison_op, target)
            if not ok:
                # 为什么这条闸在任务域而不是 validate_condition_row:条件件是跨系统
                # 通用判定件,「达标集合必须向上闭合」是**任务进度是单调累加计数器**
                # 才有的要求。将来若有快照型消费者(如「等级 <= 10」),LE/LT 在那里合法。
                raise ConfigTableError(
                    f"mission {mid} 第 {i + 1} 个条件 {cid} 的比较符="
                    f"{cond.comparison_op} 不能用作任务条件(目标={target}):"
                    "任务进度是单调不减的累加器且达标槽不再累加,达标集合必须向上闭合。"
                    "LE/LT 在进度=0 时即为真 → 该槽永不累加、恒定达标(白送完成);"
                    "EQ 是单点集合,单次事实 amount>1 会一步跨过目标后永远不再相等"
                    "(任务永久完不成);GT 目标=MaxUint32 无可达值。"
                    f"当前只有 GE(={CONDITION_COMPARE_GE})与 GT(={CONDITION_COMPARE_GT})可用"
                )
        for nid in must_uint32_csv(row.next_mission_ids):
            if nid not in tb.missions:
                raise ConfigTableError(f"mission {mid} 的后续任务 {nid} 不存在")

    _detect_mission_cycle(tb)

    for rid in sorted(tb.rewards):
        row = tb.rewards[rid]
        # ★ 累计而不是逐条判:发放侧闸的是**整条奖励展开出的 instance 列表总长**。
        # 加载期若只判单条,「10 个不同装备各 64 件」= 640 件可以整批过审、落进
        # reward_pb 快照、任务同事务置 CLAIMED,然后在发放的累计闸上**永远发不出去**
        # (快照是发放唯一入参不回读配置表,改表也救不回在途行)→ 玩家永久损失该任务
        # 全部奖励,且补扫每轮重试一次、FAILED 行不被保留期清理。两侧必须同口径。
        equip_total = 0
        for item_id, count in reward_items(row):
            if item_id not in tb.item_ids:
                raise ConfigTableError(f"reward {rid} 引用不存在的道具 {item_id}")
            if item_id not in tb.equipment_items:
                continue
            equip_total = min(equip_total + count, MAX_UINT32)
            if equip_total > MAX_REWARD_EQUIPMENT_INSTANCES:
                raise ConfigTableError(
                    f"reward {rid} 的装备累计件数 {equip_total} 超上限 "
                    f"{MAX_REWARD_EQUIPMENT_INSTANCES}(累计到道具 {item_id} 时越界;"
                    "装备按件展开成实例,总件数即列表长度,大数会打爆发放侧内存)"
                )


def _detect_mission_cycle(tb: Tables) -> None:
    """next_mission_ids 链环检测(三色 DFS,含间接环)。

    ★ 显式栈而不是递归:Python 默认递归深度 1000,而任务表上限 2000 行 ——
    一条 1500 长的合法直链会在**校验器自己**里抛 RecursionError,把"配置没问题"
    误报成加载失败。Go 的递归没有这个限制,照抄形状会引入一个 Python 独有的 bug。
    """
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[int, int] = {}
    for start in sorted(tb.missions):
        if color.get(start, WHITE) != WHITE:
            continue
        # 栈元素:(mission_id, 待访问的后续任务迭代器)
        path: list[int] = []
        stack: list[tuple[int, object]] = []
        color[start] = GRAY
        path.append(start)
        stack.append((start, iter(must_uint32_csv(tb.missions[start].next_mission_ids))))
        while stack:
            node, it = stack[-1]
            nxt = next(it, None)
            if nxt is None:
                color[node] = BLACK
                stack.pop()
                path.pop()
                continue
            c = color.get(nxt, WHITE)
            if c == GRAY:
                raise ConfigTableError(f"后续任务链成环: {path} → {nxt}")
            if c == BLACK:
                continue
            color[nxt] = GRAY
            path.append(nxt)
            stack.append((nxt, iter(must_uint32_csv(tb.missions[nxt].next_mission_ids))))


# ── 行访问助手(对应 Go 的 MissionConditionIDs / MissionSlotTarget / RewardItems)──


def mission_condition_ids(row) -> list[int]:  # noqa: ANN001
    return must_uint32_csv(row.condition_ids)


def mission_target_counts(row) -> list[int]:  # noqa: ANN001
    return must_uint32_csv(row.target_counts)


def mission_next_ids(row) -> list[int]:  # noqa: ANN001
    return must_uint32_csv(row.next_mission_ids)


def mission_slot_target(row, i: int) -> int:  # noqa: ANN001
    """第 i 个条件槽的目标覆盖值(越界/未覆盖返回 0 = 用条件行目标)。"""
    targets = mission_target_counts(row)
    if i < 0 or i >= len(targets):
        return 0
    return targets[i]


def reward_items(row) -> list[tuple[int, int]]:  # noqa: ANN001
    """道具奖励 (item_config_id, count) 列表(可能为空 = 纯经验奖励)。"""
    ids = must_uint32_csv(row.item_ids)
    counts = must_uint32_csv(row.item_counts)
    if len(ids) != len(counts):
        return []  # 防御:加载期已保证等长
    return list(zip(ids, counts, strict=True))


# ── Catalog / CatalogSource ──────────────────────────────────────────────────


class Catalog:
    """**单一批次**上的只读视图,构造后不再回读 Store。

    方法名同时满足两套调用方:
      · engine.apply_facts 的鸭子协议(mission_by_id / condition_category / ...);
      · biz 层的接取校验与发奖快照(mission_type / reward_by_id / is_equipment)。
    """

    __slots__ = ("tables", "_build_reward_log")

    def __init__(self, tables: Tables, build_reward_log=None) -> None:  # noqa: ANN001
        self.tables = tables
        # build_reward_log 由 biz 注入(它要铸幂等键、序列化 pb,属业务而非配置);
        # engine 只知道"有这么一个回调"。
        self._build_reward_log = build_reward_log

    # ── 行查询(未加载完成时一律返回 None,fail-closed)──
    def mission_by_id(self, mid: int):  # noqa: ANN201
        return self.tables.missions.get(mid)

    def condition_by_id(self, cid: int):  # noqa: ANN201
        return self.tables.conditions.get(cid)

    def reward_by_id(self, rid: int):  # noqa: ANN201
        return self.tables.rewards.get(rid)

    def is_equipment(self, item_config_id: int) -> bool:
        """道具是否装备(发奖路由:装备走 GrantInstances,其余走 GrantItems)。

        行不存在返回 False(fail-closed,发放时由 inventory 白名单再兜)。
        """
        return item_config_id in self.tables.equipment_items

    # ── engine 鸭子协议 ──
    def mission_condition_ids(self, row) -> list[int]:  # noqa: ANN001
        return mission_condition_ids(row)

    def mission_next_ids(self, row) -> list[int]:  # noqa: ANN001
        return mission_next_ids(row)

    def mission_slot_target(self, row, i: int) -> int:  # noqa: ANN001
        return mission_slot_target(row, i)

    def mission_reward_id(self, row) -> int:  # noqa: ANN001
        return row.reward_id

    def mission_auto_reward(self, row) -> int:  # noqa: ANN001
        return row.auto_reward

    def build_reward_log(self, player_id: int, row):  # noqa: ANN001, ANN201
        if self._build_reward_log is None:
            return None
        return self._build_reward_log(self, player_id, row)

    def condition_category(self, cond) -> int:  # noqa: ANN001
        return cond.condition_category

    def condition_matches_slots(self, cond, slot_values) -> bool:  # noqa: ANN001
        return condition_matches_event_slots(cond, slot_values)

    def condition_is_fulfilled(self, cond, value: int, override: int) -> bool:  # noqa: ANN001
        return condition_is_fulfilled(cond, value, override)

    def condition_clamp(self, cond, value: int, override: int) -> int:  # noqa: ANN001
        return condition_clamp_if_fulfilled(cond, value, override)

    def condition_effective_target(self, cond, override: int) -> int:  # noqa: ANN001
        return condition_effective_target(cond, override)


class CatalogSource:
    """产出「一次领域操作期间恒定」的配置批次快照。见模块头注释。"""

    __slots__ = ("_tables", "_build_reward_log")

    def __init__(self, tables: Tables, build_reward_log=None) -> None:  # noqa: ANN001
        self._tables = tables
        self._build_reward_log = build_reward_log

    def set_build_reward_log(self, fn) -> None:  # noqa: ANN001
        """装配期注入 biz 的发奖快照构造器(usecase 构造晚于 CatalogSource)。"""
        self._build_reward_log = fn

    def replace(self, tables: Tables) -> None:
        """热更原子切换。**整份替换**,不做逐表增量 —— 半个批次比旧批次更危险。"""
        self._tables = tables

    def snapshot(self) -> Catalog:
        return Catalog(self._tables, self._build_reward_log)
