"""共享基础件测试 —— cellroute / killswitch / dbguard / mysqlx。

重点覆盖"写错了不报错"的不变量:
  - cellroute:region/cell 错配必须在**建表期**就被拒(否则玩家 owner 数据分散两个 region)
  - cellroute:路由失败必须抛异常,不能返回默认落点(§9.22 禁止冒充默认值)
  - killswitch:规则解析失败必须保留旧快照,不能清空(清空 = 维护中的 RPC 突然接流量)
  - killswitch:规则源缺失 fail-open(它是运维工具,不是全服故障开关)
  - dbguard:payload 三档告警
"""

from __future__ import annotations

import pathlib
import re

import pytest

from pandorapy import cellroute, configtable, dbguard, killswitch, mysqlx


# ── cellroute ────────────────────────────────────────────────────────────────


def test_logical_cell_count_is_4096() -> None:
    """LogicalCellCount 是永久契约,必须与 Go 侧同值。

    改这个数 = 全体玩家重新分片 = 所有 owner 数据错位。迁移期两个实现同时在线,
    不同值会让同一玩家被路由到两个 cell,而且不报错。
    """
    assert cellroute.LOGICAL_CELL_COUNT == 4096


def test_logical_cell_is_deterministic_modulo() -> None:
    """logical_cell = player_id % 4096,纯函数。"""
    assert cellroute.logical_cell_of(0) == 0
    assert cellroute.logical_cell_of(4096) == 0
    assert cellroute.logical_cell_of(4097) == 1
    assert cellroute.logical_cell_of(25380000000000000) == 25380000000000000 % 4096


def test_router_routes_same_player_to_same_cell() -> None:
    """★ 同一 player_id 必须恒落同一 (region, cell) —— owner 不变量的基础。"""
    entries, region_of_cell = cellroute.build_balanced_entries(
        [cellroute.CellSpec(region_id=1, cell_id=c) for c in range(1, 5)]
        + [cellroute.CellSpec(region_id=2, cell_id=c) for c in range(5, 9)]
    )
    router = cellroute.Router(cellroute.StaticTable(entries, region_of_cell))
    for player_id in (1001, 25380000000000000, 999999999):
        first = router.route(player_id)
        for _ in range(10):
            assert router.route(player_id) == first


def test_static_table_rejects_region_cell_mismatch() -> None:
    """★ region/cell 错配必须在建表期被拒。

    放过一个错配,那批玩家的 owner 数据会分散在两个 region(档案在 A、背包在 B),
    读不回来,且要等玩家实际访问才发现。
    """
    entries = [cellroute.Entry(region_id=1, cell_id=7)] * cellroute.LOGICAL_CELL_COUNT
    # 拓扑里 cell 7 属于 region 2,与 entry 声明的 region 1 冲突
    with pytest.raises(cellroute.CellRouteError, match="region 不匹配"):
        cellroute.StaticTable(entries, {7: 2})


def test_static_table_rejects_unregistered_cell() -> None:
    """entry 引用了拓扑里没登记的 cell → 拒绝建表。"""
    entries = [cellroute.Entry(region_id=1, cell_id=99)] * cellroute.LOGICAL_CELL_COUNT
    with pytest.raises(cellroute.CellRouteError, match="未在 region_of_cell 中登记"):
        cellroute.StaticTable(entries, {1: 1})


def test_static_table_rejects_wrong_length() -> None:
    """表长度必须恰好是 LOGICAL_CELL_COUNT —— 短了会让部分玩家路由不到。"""
    with pytest.raises(cellroute.CellRouteError, match="LogicalCellCount"):
        cellroute.StaticTable([cellroute.Entry(1, 1)], {1: 1})


def test_build_balanced_entries_covers_all_shards_evenly() -> None:
    """铺表必须覆盖全部 4096 个分片,且尽量均匀(余数摊到前几个 Cell)。"""
    cells = [cellroute.CellSpec(region_id=1, cell_id=c) for c in range(1, 8)]  # 7 个
    entries, region_of_cell = cellroute.build_balanced_entries(cells)
    assert len(entries) == cellroute.LOGICAL_CELL_COUNT
    assert len(region_of_cell) == 7
    counts: dict[int, int] = {}
    for e in entries:
        counts[e.cell_id] = counts.get(e.cell_id, 0) + 1
    # 4096 / 7 = 585 余 1 → 前 1 个 Cell 拿 586,其余 585
    assert sorted(counts.values()) == [585] * 6 + [586]


def test_build_balanced_entries_rejects_cell_in_two_regions() -> None:
    """同一 cell 被声明在两个 region → 拒绝(错配的源头)。"""
    with pytest.raises(cellroute.CellRouteError, match="两个 region"):
        cellroute.build_balanced_entries(
            [cellroute.CellSpec(1, 5), cellroute.CellSpec(2, 5)]
        )


def test_route_failure_raises_not_default() -> None:
    """★ 路由失败必须抛异常,不能返回默认落点(§9.22 禁止冒充默认值)。"""

    class EmptyTable(cellroute.StaticTable):
        def __init__(self) -> None:  # 绕过父类校验,构造一个查不到的表
            pass

        def lookup(self, logical_cell: int):  # noqa: ANN201
            return None

    router = cellroute.Router(EmptyTable())
    with pytest.raises(cellroute.CellRouteError, match="未映射"):
        router.route(1001)


# ── killswitch ───────────────────────────────────────────────────────────────


def test_disabled_normalizes_leading_slash() -> None:
    """gRPC full method 带前导 "/",规则文件里通常不带 —— 两种写法必须都命中。"""
    mgr = killswitch.Manager()
    mgr.replace({"pandora.trade.v1.TradeService/CreateOrder": "交易维护中"})
    for op in (
        "/pandora.trade.v1.TradeService/CreateOrder",
        "pandora.trade.v1.TradeService/CreateOrder",
    ):
        blocked, reason = mgr.disabled(op)
        assert blocked and reason == "交易维护中", f"{op} 没命中规则"


def test_empty_reason_falls_back_to_default_text() -> None:
    """规则值为空串时用默认文案 —— 客户端不能收到空的维护提示。"""
    mgr = killswitch.Manager()
    mgr.replace({"a.b.C/D": ""})
    blocked, reason = mgr.disabled("/a.b.C/D")
    assert blocked and reason


def test_unlisted_operation_is_allowed() -> None:
    mgr = killswitch.Manager()
    mgr.replace({"a.b.C/D": "x"})
    assert mgr.disabled("/a.b.C/Other") == (False, "")


def test_package_level_disabled_is_fail_open_without_manager() -> None:
    """★ 没设默认 Manager 时必须 fail-open 放行。

    killswitch 是"临时关停"工具,默认状态是不关停。若规则源不可用就把所有 RPC 关掉,
    等于把运维工具变成全服故障开关 —— 这是刻意与 §9.22 的 fail-closed 相反的一处。
    """
    killswitch.set_default(None)
    assert killswitch.disabled("/anything/At") == (False, "")


def test_parse_rules_accepts_yaml_and_json() -> None:
    yaml_rules = killswitch.parse_rules(b'rules:\n  "a.b.C/D": "\xe7\xbb\xb4\xe6\x8a\xa4"\n')
    assert yaml_rules == {"a.b.C/D": "维护"}
    json_rules = killswitch.parse_rules(b'{"rules": {"a.b.C/D": "x"}}')
    assert json_rules == {"a.b.C/D": "x"}
    assert killswitch.parse_rules(b"") == {}


def test_file_source_missing_file_is_fail_open(tmp_path: pathlib.Path) -> None:
    """规则文件不存在 = 无规则,不是错误(本地联调常态)。"""
    src = killswitch.FileSource(tmp_path / "nope.yaml")
    assert src.load() == 0
    assert src.manager.disabled("/a/B") == (False, "")


def test_file_source_parse_failure_keeps_old_snapshot(tmp_path: pathlib.Path) -> None:
    """★ 解析失败必须保留旧快照,不能清空。

    清空 = 所有关停规则突然放开 = 正在维护的 RPC 重新接流量。
    与配置表热更的"加载失败保留旧配置"同理。
    """
    path = tmp_path / "ks.yaml"
    path.write_text('rules:\n  "a.b.C/D": "维护中"\n', encoding="utf-8")
    src = killswitch.FileSource(path)
    assert src.load() == 1
    assert src.manager.disabled("/a.b.C/D")[0]

    # 写入坏内容后重载
    path.write_text("rules: [this is a list not a mapping]\n", encoding="utf-8")
    src.load()
    still_blocked, reason = src.manager.disabled("/a.b.C/D")
    assert still_blocked, "解析失败后规则被清空了 —— 维护中的 RPC 会突然接流量"
    assert reason == "维护中"


def test_disabled_error_uses_service_disabled_code() -> None:
    """关停错误码必须是 ErrServiceDisabled(13),客户端据此提示维护而不是重试。"""
    from pandorapy import errcode

    err = killswitch.disabled_error("维护中")
    assert err.code == errcode.ErrServiceDisabled == 13


# ── dbguard ──────────────────────────────────────────────────────────────────


def test_check_payload_rejects_oversize() -> None:
    """超上限必须抛异常拒写 —— 这是"数据被静默截断"的唯一防线。"""
    with pytest.raises(dbguard.PayloadTooLargeError, match="超过上限"):
        dbguard.check_payload("bag_items", b"x" * 101, max_bytes=100)


def test_check_payload_allows_under_limit() -> None:
    dbguard.check_payload("bag_items", b"x" * 50, max_bytes=100)  # 不应抛


def test_check_payload_warns_at_80_percent() -> None:
    """达 80% 放行但告警 —— 留出排查窗口。"""
    assert dbguard.WARN_RATIO == 0.8
    dbguard.check_payload("bag_items", b"x" * 80, max_bytes=100)  # 放行,只 WARN


# ── dbguard.sweep_table:Outcome 的形状契约 ───────────────────────────────────
#
# 这一组钉的是"调用方能从 Outcome 里读出什么"。曾经出过的事故:Outcome 少了
# truncated 这一位,调用方(player 保留期清理循环)只好从 matched/deleted 推,
# 而 DELETE 档下这两个字段携带的是同一个数 —— 推出来的判据恒真,循环每轮只删
# 一批就退出,只增表积压永远追不平。用手搓 Outcome 的假件测不出这一条:假件想
# 编什么形状就编什么形状,而真 sweep_table 产不出那种形状。


class _FakeCursor:
    """够 dbguard.sweep_table 用的最小 cursor:execute / rowcount / fetchone。"""

    def __init__(self, rowcount: int = 0, count: int = 0) -> None:
        self.rowcount = rowcount
        self._count = count
        self.executed: list[tuple[str, object]] = []

    async def __aenter__(self) -> "_FakeCursor":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def execute(self, sql: str, params: object = None) -> None:
        self.executed.append((sql, params))

    async def fetchone(self) -> tuple[int]:
        return (self._count,)


class _FakeConn:
    def __init__(self, cur: _FakeCursor) -> None:
        self._cur = cur

    def cursor(self) -> _FakeCursor:
        return self._cur


async def test_sweep_rejects_schema_injection_before_sql() -> None:
    cur = _FakeCursor(rowcount=1)
    with pytest.raises(ValueError, match="schema"):
        await dbguard.sweep_table(
            _FakeConn(cur),
            dbguard.Mode.DELETE,
            "physical_db`.`canonical_db",
            "exp_history",
            "created_at < %s",
            1000,
            "t",
        )
    assert cur.executed == []


async def test_capacity_check_rejects_invalid_schema_before_query() -> None:
    cur = _FakeCursor()
    with pytest.raises(ValueError, match="schema"):
        await dbguard.check_budgets(
            _FakeConn(cur),
            "physical-db",
            [dbguard.TableBudget(table="exp_history", max_rows=1)],
        )
    assert cur.executed == []


async def test_sweep_delete_full_batch_is_truncated() -> None:
    """DELETE 打满 limit → truncated=True(还有积压,调用方必须继续下一批)。"""
    cur = _FakeCursor(rowcount=1000)
    out = await dbguard.sweep_table(
        _FakeConn(cur), dbguard.Mode.DELETE, "db", "exp_history", "created_at < %s", 1000, "t"
    )
    assert out.deleted == 1000
    assert out.truncated is True


async def test_sweep_delete_short_batch_is_not_truncated() -> None:
    """DELETE 没打满 limit → truncated=False(追平了,可以收工)。"""
    cur = _FakeCursor(rowcount=500)
    out = await dbguard.sweep_table(
        _FakeConn(cur), dbguard.Mode.DELETE, "db", "exp_history", "created_at < %s", 1000, "t"
    )
    assert out.deleted == 500
    assert out.truncated is False


# ── dbguard.check_columns:列级字节巡检 ───────────────────────────────────────
#
# 这一组钉的是"列级与表级是两条独立的信号"。表级 avg_row_bytes 只能告诉你"行普遍
# 变胖了",定位不到是哪一列;而列级 MAX(LENGTH) 是全表扫描,不能挂周期 ticker。
# 混淆两者的后果分别是"查不出来"和"把生产库扫死"。


class _ColumnCursor:
    """够 check_columns / top_large_rows 用的最小 cursor。

    `rows` 是按 execute 顺序弹出的返回值;`fail_on` 里的子串命中就抛 —— 用来验
    "单列扫描失败只 WARN 并继续下一列",而不是整轮哑掉。
    """

    def __init__(self, rows: list[object], fail_on: str = "") -> None:
        self._rows = list(rows)
        self._fail_on = fail_on
        self.executed: list[tuple[str, object]] = []

    async def __aenter__(self) -> "_ColumnCursor":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def execute(self, sql: str, params: object = None) -> None:
        self.executed.append((sql, params))
        if self._fail_on and self._fail_on in sql:
            raise RuntimeError("boom")

    async def fetchone(self) -> object:
        return self._rows.pop(0)

    async def fetchall(self) -> object:
        return self._rows.pop(0)


async def test_check_columns_rejects_identifier_injection_before_sql() -> None:
    """列名要拼进 SQL(标识符位置用不了占位符),所以必须先过白名单。"""
    cur = _ColumnCursor([])
    with pytest.raises(ValueError, match="column"):
        await dbguard.check_columns(
            _FakeConn(cur),
            "db",
            [dbguard.ColumnBudget(table="t", column="payload`),(1,1)--", max_bytes=1)],
        )
    assert cur.executed == []


async def test_check_columns_flags_only_over_budget() -> None:
    """超预算才进 violations;等于预算不算超(与 Go 的 `maxLen > MaxBytes` 同为严格大于)。"""
    cur = _ColumnCursor([(769, 100), (1536, 200)])
    out = await dbguard.check_columns(
        _FakeConn(cur),
        "pandora_battle",
        [
            dbguard.ColumnBudget(table="match_release_outbox", column="payload", max_bytes=768),
            dbguard.ColumnBudget(
                table="battle_exit_proof_outbox", column="payload", max_bytes=1536
            ),
        ],
    )
    assert out.checked == 2
    assert [(v.table, v.column, v.kind, v.actual, v.budget) for v in out.violations] == [
        ("match_release_outbox", "payload", "column_bytes", 769, 768)
    ]


async def test_check_columns_zero_budget_never_flags() -> None:
    """max_bytes<=0 = 只采指标不设阈值(与 Go 同)。否则默认值 0 会让每一列都告警。"""
    cur = _ColumnCursor([(999_999, 1)])
    out = await dbguard.check_columns(
        _FakeConn(cur), "db", [dbguard.ColumnBudget(table="t", column="payload")]
    )
    assert out.checked == 1
    assert out.violations == []


async def test_check_columns_one_failing_column_does_not_abort_the_round() -> None:
    """★ 一列扫不动不该让整轮哑掉 —— 否则第一张大表就吃掉了后面所有列的可见性。"""
    cur = _ColumnCursor([(2000, 1)], fail_on="`bad`")
    out = await dbguard.check_columns(
        _FakeConn(cur),
        "db",
        [
            dbguard.ColumnBudget(table="t", column="bad", max_bytes=1),
            dbguard.ColumnBudget(table="t", column="payload", max_bytes=1),
        ],
    )
    # 失败那列不计入 checked,但后一列照常扫出并告警。
    assert out.checked == 1
    assert [v.column for v in out.violations] == ["payload"]


async def test_top_large_rows_clamps_limit() -> None:
    """limit 越界回落 20(与 Go 同):一次拉太多会把排查工具自己拖垮。"""
    cur = _ColumnCursor([[(7, 900), (8, 800)]])
    out = await dbguard.top_large_rows(_FakeConn(cur), "t", "id", "payload", limit=10_000)
    assert cur.executed[0][1] == (20,)
    assert [(r.pk, r.size_bytes) for r in out] == [("7", 900), ("8", 800)]


@pytest.mark.parametrize("rowcount", [0, 1, 500, 999, 1000])
async def test_sweep_delete_matched_always_equals_deleted(rowcount: int) -> None:
    """★ DELETE 档 matched == deleted **恒成立**,所以 truncated 无法从两者推回来。

    这正是 Go 必须单独留 `Truncated bool` 的原因(sweep.go:181-182 是
    `out.Matched, out.Deleted = n, n` 紧跟 `out.Truncated = limit > 0 && n >= limit`)。
    谁想省掉这一位、改用 `matched > deleted` 之类的替代判据,得到的必然是个恒假的式子。
    """
    cur = _FakeCursor(rowcount=rowcount)
    out = await dbguard.sweep_table(
        _FakeConn(cur), dbguard.Mode.DELETE, "db", "exp_history", "created_at < %s", 1000, "t"
    )
    assert out.matched == out.deleted == rowcount
    assert (out.matched > out.deleted) is False


async def test_sweep_report_only_is_never_truncated() -> None:
    """report_only 的 COUNT 不受 limit 截断,truncated 恒 False(对齐 Go 的零值)。

    注意 report_only 档 matched(=待清理总量)可以远大于 deleted(=0)—— 一个只在
    这一档为真、而这一档根本不需要循环的形状。把它当"被截断"用就会把语义搬错档位。
    """
    cur = _FakeCursor(count=10_000)
    out = await dbguard.sweep_table(
        _FakeConn(cur), dbguard.Mode.REPORT_ONLY, "db", "exp_history", "created_at < %s", 1000, "t"
    )
    assert (out.matched, out.deleted, out.truncated) == (10_000, 0, False)


def test_truncated_formula_matches_go(repo_root: pathlib.Path) -> None:
    """公式取自 Go 源码:Go 改了口径这条必须红,而不是等生产上积压追不平才发现。"""
    src = (repo_root / "pkg" / "dbguard" / "sweep.go").read_text(encoding="utf-8")
    assert "out.Truncated = limit > 0 && n >= int64(limit)" in src


# ── mysqlx ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("8.0.11-TiDB-v8.5.0", (8, 5, 0)),
        ("5.7.25-TiDB-v6.1.0", (6, 1, 0)),
        ("8.4.0", None),  # 普通 MySQL
        ("8.0.36-MariaDB", None),
        ("", None),
    ],
)
def test_parse_tidb_version(version: str, expected: tuple[int, int, int] | None) -> None:
    """TiDB 版本解析必须与 Go 侧 tidbVersionRe 同结果。

    误把普通 MySQL 认成 TiDB 的后果:依赖 TiDB 语义的逻辑(无 gap 锁前提下的
    守卫行写法)会悄悄跑偏,不报错。
    """
    assert mysqlx.parse_tidb_version(version) == expected


def test_error_code_classification() -> None:
    """1062/1406/1213 必须能正确判别 —— 幂等与重试都依赖它。"""

    class FakeDBError(Exception):
        pass

    assert mysqlx.is_duplicate_entry(FakeDBError(1062, "dup"))
    assert mysqlx.is_data_too_long(FakeDBError(1406, "too long"))
    assert mysqlx.is_deadlock(FakeDBError(1213, "deadlock"))
    assert not mysqlx.is_deadlock(FakeDBError(1062, "dup"))
    assert not mysqlx.is_duplicate_entry(ValueError("not a db error"))


def test_map_db_error_does_not_swallow_deadlock() -> None:
    """死锁不该被映射成业务码 —— 它应当在数据层重试。

    走到 map_db_error 的死锁说明重试已耗尽,那是真的内部错误。
    """
    from pandorapy import errcode

    class FakeDBError(Exception):
        pass

    assert mysqlx.map_db_error(FakeDBError(1062, "dup")) == errcode.ErrAlreadyExists
    assert mysqlx.map_db_error(FakeDBError(1406, "long")) == errcode.ErrInvalidArg
    assert mysqlx.map_db_error(FakeDBError(1213, "deadlock")) == errcode.ErrInternal


# ── redisx:分布式锁的 key 空间必须与 Go 侧同一个 ─────────────────────────────


def test_lock_prefix_matches_go_source(repo_root: pathlib.Path) -> None:
    """★ 锁 key 前缀必须与 Go 的 pkg/redislock.DefaultPrefix **逐字一致**。

    判据直接从 **Go 源码**取,不是抄一份字面量到测试里 —— 抄一份的话
    Go 改了前缀这条测试照样绿,而那恰恰是要防的事。

    为什么这条重要:迁移期两栈并存,同一把业务锁会被 Go 副本和 Python 副本分别去拿。
    前缀不一致 = 两边落在**两个不同的 key** 上,双方都能"拿到锁",互斥当场失效,
    而且两边日志都显示加锁成功 —— 没有任何运行期信号。
    """
    from pandorapy import redisx

    src = (repo_root / "pkg" / "redislock" / "redislock.go").read_text(encoding="utf-8")
    m = re.search(r'const\s+DefaultPrefix\s*=\s*"([^"]+)"', src)
    assert m, "没在 pkg/redislock/redislock.go 里找到 DefaultPrefix —— Go 侧改名了?"
    assert redisx.LOCK_KEY_PREFIX == m.group(1), (
        f"锁前缀与 Go 不一致:Python={redisx.LOCK_KEY_PREFIX!r} Go={m.group(1)!r}"
    )


def test_lock_key_is_prefixed_and_idempotent() -> None:
    """业务名自动补前缀;已经是全名的原样返回(不叠加)。"""
    from pandorapy import redisx

    assert redisx.lock_key("team:1001") == "pandora:lock:team:1001"
    assert redisx.lock_key("pandora:lock:team:1001") == "pandora:lock:team:1001"


def test_check_payload_boundary_matches_go(repo_root: pathlib.Path) -> None:
    """★ 拒写边界必须与 Go 一致:`size >= max` 而不是 `>`,判据取自 Go 源码。

    差这一格的后果是"恰好等于上限"的 payload 两栈判定**相反**:Go 拒、Python 放行。
    而列宽本身就是上限 —— 放行的那条要么被 MySQL 报错,要么在非严格模式下被
    **静默截断**(§9.24 点名的那种数据损坏)。
    """
    src = (repo_root / "pkg" / "dbguard" / "payload.go").read_text(encoding="utf-8")
    assert "size >= limit.Max" in src, "Go 的拒写边界变了?请同步 Python 侧"

    with pytest.raises(dbguard.PayloadTooLargeError):
        dbguard.check_payload("bag_items", b"x" * 100, max_bytes=100)  # 恰好等于
    dbguard.check_payload("bag_items", b"x" * 99, max_bytes=100)  # 差一个字节:放行


def test_check_payload_skips_when_no_budget_configured(repo_root: pathlib.Path) -> None:
    """★ 未设预算(max<=0)= **不校验**,不是"拒掉一切"。

    Go 侧是 `if limit.Max <= 0 { return nil }`。少这一分支的话,任何还没定阈值的列
    都会因为 max_bytes=0 而拒掉所有写入 —— 一个容量守护把正常业务全挡了。
    """
    src = (repo_root / "pkg" / "dbguard" / "payload.go").read_text(encoding="utf-8")
    assert "limit.Max <= 0" in src, "Go 不再有「未设预算不校验」分支?"

    dbguard.check_payload("unbudgeted", b"x" * 10_000, max_bytes=0)
    dbguard.check_payload("unbudgeted", b"x" * 10_000, max_bytes=-1)


# ── ★ configtable manifest 的三道结构闸 ────────────────────────────────────


def _manifest_dir(tmp_path: pathlib.Path, **overrides) -> pathlib.Path:
    """造一份最小可读 manifest;overrides 用来逐条破坏某一格。"""
    import json

    table = {
        "name": "level",
        "file": "level.json",
        "proto": "pandora.config.v1.LevelTableData",
        "checksum": "sha256:" + "0" * 64,
        "rows": 1,
    }
    table.update(overrides.pop("table", {}))
    doc = {"version": 1, "tables": [table]}
    doc.update(overrides)
    (tmp_path / "manifest.json").write_text(json.dumps(doc), encoding="utf-8")
    return tmp_path


def test_manifest_rejects_file_name_drift(tmp_path: pathlib.Path) -> None:
    """★ file 必须恰为 `<name>.json` —— 名字漂移会**加载到另一张表的内容**。"""
    d = _manifest_dir(tmp_path, table={"file": "level_v2.json"})
    with pytest.raises(configtable.ConfigTableError, match="file 必须是"):
        configtable.read_manifest(d)


@pytest.mark.parametrize(
    "evil",
    ["../../../etc/passwd", "/etc/passwd", "C:/Windows/win.ini", "sub/level.json"],
)
def test_manifest_rejects_path_traversal(tmp_path: pathlib.Path, evil: str) -> None:
    """★ 同一道闸也是**路径逃逸**的防线。

    `pathlib` 对绝对路径是**整个替换**基路径而不是拼接(`Path("a") / "/etc/passwd"`
    得到 `/etc/passwd`),所以 file 一旦能写任意值,加载器就会去读 active 目录之外
    的文件。manifest 是发布产物,但发布链上任何一环被写坏都不该有这个能力。
    """
    d = _manifest_dir(tmp_path, table={"file": evil})
    with pytest.raises(configtable.ConfigTableError):
        configtable.read_manifest(d)


def test_manifest_rejects_zero_version(tmp_path: pathlib.Path) -> None:
    """★ version 必须 > 0 —— 它是热更防回退的唯一依据(§9.15)。

    version=0 的批次一旦被接受,之后**任何**批次都"更新",防回退直接失效。
    """
    d = _manifest_dir(tmp_path, version=0)
    with pytest.raises(configtable.ConfigTableError, match="version"):
        configtable.read_manifest(d)


def test_manifest_rejects_checksum_without_prefix(tmp_path: pathlib.Path) -> None:
    """checksum 必须带 sha256: 前缀 —— 缺前缀说明发布器换了算法或写坏了。"""
    d = _manifest_dir(tmp_path, table={"checksum": "0" * 64})
    with pytest.raises(configtable.ConfigTableError, match="sha256"):
        configtable.read_manifest(d)


def test_manifest_rejects_empty_table_name(tmp_path: pathlib.Path) -> None:
    d = _manifest_dir(tmp_path, table={"name": ""})
    with pytest.raises(configtable.ConfigTableError, match="name 为空"):
        configtable.read_manifest(d)


@pytest.mark.parametrize("alias", ["", "report", "report-only", "report_only", "REPORT_ONLY", " Report "])
def test_parse_mode_accepts_every_alias_go_accepts(alias: str, repo_root: pathlib.Path) -> None:
    """★ retention_mode 的词表必须与 Go 的 ParseMode 一致。

    少认一个别名的后果不是"配置不生效",而是**启动直接失败**
    (ValidateRetentionMode 是 fail-fast)—— 一份在 Go 上跑得好好的 yaml,
    换成 Python 副本就起不来,而错误只说"无法识别",看不出是两栈词表不同。
    """
    src = (repo_root / "pkg" / "dbguard" / "sweep.go").read_text(encoding="utf-8")
    assert '"report-only"' in src and '"report"' in src, "Go 的别名集合变了?"
    assert dbguard.parse_mode(alias) is dbguard.Mode.REPORT_ONLY


def test_parse_mode_still_rejects_typos() -> None:
    """拼错仍必须报错 —— 拼错一个字母就开始删生产数据是不可接受的失败模式。"""
    with pytest.raises(ValueError):
        dbguard.parse_mode("delet")
    with pytest.raises(ValueError):
        dbguard.parse_mode("report_onlyy")


# ── ★ errcode → gRPC 状态码映射与 Go 一致 ──────────────────────────────────


def test_grpc_status_mapping_matches_go(repo_root: pathlib.Path) -> None:
    """★ 映射表必须与 Go 的 GRPCCode **逐条一致**,判据取自 Go 源码。

    这张表的每一格都是客户端的分支依据。最要命的是顶号那格:
    Go 刻意把 ErrSessionSuperseded 映射成 ABORTED 而**不是** UNAUTHENTICATED ——
    因为网关对自然过期 token 也产 UNAUTHENTICATED,客户端分不出"可以自动换新"
    和"另一台设备登录了"。分不出的后果是被顶设备自动重登、反顶新设备,
    形成**互踢循环**(INC-20260722-004)。抄错这一格不会报错,只会让两台手机打架。

    解析刻意用逐行扫描而不是正则:这段 Go 是 `case A, B:` 多行块,
    正则写复杂了容易在"没匹配到"时静默变成空表,而空表会让本用例恒真。
    """
    from pandorapy import errcode, errcode_grpc

    src = (repo_root / "pkg" / "errcode" / "grpc.go").read_text(encoding="utf-8")
    body_text = src[src.index("func GRPCCode") : src.index("// ToGRPCError")]

    pairs: dict[str, str] = {}
    pending: list[str] = []
    for line in body_text.splitlines():
        s = line.strip()
        if s.startswith("case ") and s.endswith(":"):
            pending = [n.strip() for n in s[len("case ") : -1].split(",")]
        elif s.startswith("return codes.") and pending:
            code = s[len("return codes.") :].strip()
            for name in pending:
                pairs[name] = code
            pending = []
    assert len(pairs) >= 8, f"Go 映射表只解析出 {len(pairs)} 项,解析多半坏了"

    # ★ 两个库对同一个状态的**拼写**不同,不是语义不同,必须显式登记:
    #   Go   codes.Canceled   (一个 l)
    #   grpc StatusCode.CANCELLED (两个 l)
    # 不登记的话本用例会红在一个纯拼写差异上,然后被人"顺手"改成忽略大小写/模糊比对
    # —— 那会把真正的映射错误一起放过去。
    _SPELLING = {"Canceled": "CANCELLED"}

    def to_py_name(go_code: str) -> str:
        """Go 的 CamelCase 码名 → grpcio 的 UPPER_SNAKE(OK / DeadlineExceeded 两种形态)。"""
        if go_code in _SPELLING:
            return _SPELLING[go_code]
        if go_code.isupper():
            return go_code  # OK
        out = []
        for i, ch in enumerate(go_code):
            if ch.isupper() and i:
                out.append("_")
            out.append(ch.upper())
        return "".join(out)

    for go_name, go_code in pairs.items():
        code = getattr(errcode, go_name, None)
        if code is None:
            continue  # Go 里的 OK 之类不在 errcode 常量表
        want = to_py_name(go_code)
        got = errcode_grpc.grpc_code(code).name
        assert got == want, f"{go_name}: Python={got} Go={want}"

    # 顶号那一格单独再钉一次 —— 它是整张表里唯一"反直觉"的映射。
    assert errcode_grpc.grpc_code(errcode.ErrSessionSuperseded).name == "ABORTED"


def test_unmapped_business_codes_fall_back_to_unknown() -> None:
    """业务段(>=1000)刻意不逐一映射 —— 逐一映射会让每加一个码就要同步两处。"""
    import grpc

    from pandorapy import errcode, errcode_grpc

    assert errcode_grpc.grpc_code(errcode.ErrTradeInsufficient) is grpc.StatusCode.UNKNOWN


def test_dialogue_table_rejects_zero_primary_key() -> None:
    """★ 主键为 0 即拒批 —— 与 Go 的 newDialogueTable 同一道闸。

    0 是 protobuf 的默认值:一行"什么都没填"的空行解析出来 id 就是 0。
    放过它的后果不只是多了一行垃圾 —— `_by_id[0]` 会被后来的空行**覆盖**,
    表现为"查某个对话节点查出来是另一个";而 is_start 为真的空行还会成为
    该 NPC(npc_id 同样是 0)的起始节点。
    """

    class _Row:
        def __init__(self, rid: int, npc: int = 1, start: bool = False) -> None:
            self.id = rid
            self.npc_id = npc
            self.is_start = start

    with pytest.raises(configtable.ConfigTableError, match="主键为 0"):
        configtable.DialogueTable([_Row(1), _Row(0)])

    # 正常行仍然建得起来
    assert configtable.DialogueTable([_Row(1), _Row(2)]).count() == 2


def test_oversize_payload_rejection_is_observable() -> None:
    """★ 拒写必须留下**可观测信号**,不能只抛异常。

    异常会被调用方按业务错误处理掉,于是"某个玩家的数据一直写不进去"
    在运维视角上**完全不可见**:没有日志、没有指标,只有客服工单。
    Go 侧这里有 ERROR 日志 + 拒写计数器。
    """
    from structlog.testing import capture_logs

    with capture_logs() as logs:
        with pytest.raises(dbguard.PayloadTooLargeError):
            dbguard.check_payload("bag_items", b"x" * 200, max_bytes=100)
    assert "db_payload_too_large_rejected" in [e["event"] for e in logs], (
        "拒写没有任何日志 —— 运维看不见这件事在发生"
    )
