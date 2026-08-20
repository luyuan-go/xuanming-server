"""data_service 测试 —— cache-aside 编排 + update_mask 不变量 + 日志限流窗口。

重点覆盖:
  1. ★ update_mask 更新时必须非空(§9.17 零停机滚动升级的硬约束)
  2. ★ 缓存是**旁路**:缓存挂了读写都必须照常成功
  3. ★ MySQL 是**事实源**:它挂了必须报错,不能拿缓存假装成功
  4. ★ 降级日志限流:首错必打、窗口内一条、恢复时一条
  5. proto → SQL schema 推导(替代 proto2mysql)
"""

from __future__ import annotations

import datetime as _dt

import pytest
from pandora.data_service.v1 import data_service_pb2

from pandorapy import errcode, logwindow, protosql
from pandorapy.services.data_service import biz as dbiz
from pandorapy.services.data_service import data as ddata


class FakeStore:
    def __init__(self) -> None:
        self.rows: dict[int, object] = {}
        self.fail_read = False
        self.fail_write = False
        self.write_calls: list[tuple[int, list[str]]] = []

    async def read(self, player_id: int):
        if self.fail_read:
            raise errcode.PandoraError(errcode.ErrInternal, "mysql down")
        return self.rows.get(player_id)

    async def write(self, pd, update_fields: list[str]) -> int:
        if self.fail_write:
            raise errcode.PandoraError(errcode.ErrInternal, "mysql down")
        self.write_calls.append((pd.player_id, list(update_fields)))
        if pd.version == 0:
            if pd.player_id in self.rows:
                raise errcode.PandoraError(errcode.ErrDataVersionMismatch, "exists")
            stored = data_service_pb2.PlayerData()
            stored.CopyFrom(pd)
            stored.version = 1
            self.rows[pd.player_id] = stored
            return 1
        current = self.rows.get(pd.player_id)
        if current is None or current.version != pd.version:
            raise errcode.PandoraError(errcode.ErrDataVersionMismatch, "mismatch")
        for f in update_fields:
            setattr(current, f, getattr(pd, f))
        current.version += 1
        return current.version


class FakeCache:
    def __init__(self) -> None:
        self.store: dict[int, object] = {}
        self.fail = False
        self.deleted: list[int] = []

    async def get(self, player_id: int):
        if self.fail:
            raise RuntimeError("redis down")
        pd = self.store.get(player_id)
        return (pd, True) if pd is not None else (None, False)

    async def set(self, pd, ttl) -> None:  # noqa: ANN001
        if self.fail:
            raise RuntimeError("redis down")
        self.store[pd.player_id] = pd

    async def delete(self, player_id: int) -> None:
        if self.fail:
            raise RuntimeError("redis down")
        self.deleted.append(player_id)
        self.store.pop(player_id, None)


class Cfg:
    def cache_ttl_td(self) -> _dt.timedelta:
        return _dt.timedelta(minutes=5)


def _pd(player_id: int, version: int = 0, **fields) -> object:
    return data_service_pb2.PlayerData(player_id=player_id, version=version, **fields)


# ── ★ update_mask 不变量(§9.17)────────────────────────────────────────────


async def test_update_with_empty_mask_is_rejected() -> None:
    """★ 更新时 update_mask 必须非空。

    空掩码 = 全量覆盖。滚动升级期间旧副本不认得新加的列,一次全量写会把新列**清零**。
    这是零停机更新的硬约束,不是风格问题。
    """
    store, cache = FakeStore(), FakeCache()
    uc = dbiz.DataUsecase(store, cache, Cfg())
    await uc.write_player(_pd(1001, 0, nickname="a"), [])  # 新建可以空掩码
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.write_player(_pd(1001, 1, nickname="b"), [])
    assert exc.value.code == errcode.ErrInvalidArg
    assert "update_mask required" in exc.value.msg


@pytest.mark.parametrize("bad_field", ["player_id", "version", "no_such_column", "1=1"])
async def test_update_mask_rejects_pk_version_and_unknown(bad_field: str) -> None:
    """★ 掩码不能含主键 / version / 未知字段。

    未知字段那条同时是**注入防线** —— 列名会被拼进 SQL。
    """
    store, cache = FakeStore(), FakeCache()
    uc = dbiz.DataUsecase(store, cache, Cfg())
    await uc.write_player(_pd(1001, 0, nickname="a"), [])
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.write_player(_pd(1001, 1, nickname="b"), [bad_field])
    assert exc.value.code == errcode.ErrInvalidArg
    assert "invalid update_mask" in exc.value.msg


async def test_new_record_ignores_mask() -> None:
    """新建(version==0)整条 INSERT,掩码被忽略。"""
    store, cache = FakeStore(), FakeCache()
    uc = dbiz.DataUsecase(store, cache, Cfg())
    assert await uc.write_player(_pd(1001, 0, nickname="x", level=3), []) == 1


async def test_version_mismatch_surfaces() -> None:
    """乐观锁冲突返回 ErrDataVersionMismatch(良性竞争,调用方重读再试)。"""
    store, cache = FakeStore(), FakeCache()
    uc = dbiz.DataUsecase(store, cache, Cfg())
    await uc.write_player(_pd(1001, 0, nickname="a"), [])
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.write_player(_pd(1001, 99, nickname="b"), ["nickname"])
    assert exc.value.code == errcode.ErrDataVersionMismatch


# ── ★ 缓存是旁路 ─────────────────────────────────────────────────────────────


async def test_read_falls_back_to_mysql_when_cache_down() -> None:
    """★ 缓存挂了读必须照常成功(回落 MySQL)。"""
    store, cache = FakeStore(), FakeCache()
    store.rows[1001] = _pd(1001, 1, nickname="alice")
    cache.fail = True
    uc = dbiz.DataUsecase(store, cache, Cfg())
    pd = await uc.read_player(1001)
    assert pd is not None and pd.nickname == "alice"


async def test_write_succeeds_when_cache_del_fails() -> None:
    """★ 写后删缓存失败只告警,**不回滚** —— 缓存最终随 TTL 失效。"""
    store, cache = FakeStore(), FakeCache()
    uc = dbiz.DataUsecase(store, cache, Cfg())
    await uc.write_player(_pd(1001, 0, nickname="a"), [])
    cache.fail = True
    assert await uc.write_player(_pd(1001, 1, nickname="b"), ["nickname"]) == 2


async def test_cache_hit_skips_mysql() -> None:
    """命中缓存直返,不读库。"""
    store, cache = FakeStore(), FakeCache()
    cache.store[1001] = _pd(1001, 1, nickname="cached")
    store.fail_read = True  # 库挂了也不影响,因为不该读它
    uc = dbiz.DataUsecase(store, cache, Cfg())
    pd = await uc.read_player(1001)
    assert pd.nickname == "cached"


async def test_read_backfills_cache_on_miss() -> None:
    store, cache = FakeStore(), FakeCache()
    store.rows[1001] = _pd(1001, 1, nickname="alice")
    uc = dbiz.DataUsecase(store, cache, Cfg())
    await uc.read_player(1001)
    assert 1001 in cache.store


async def test_write_deletes_cache() -> None:
    """写后删缓存,避免读到旧版本。"""
    store, cache = FakeStore(), FakeCache()
    uc = dbiz.DataUsecase(store, cache, Cfg())
    await uc.write_player(_pd(1001, 0, nickname="a"), [])
    assert 1001 in cache.deleted


async def test_no_cache_configured_works() -> None:
    """cache 为 None(未配置)时退化为直连 MySQL。"""
    store = FakeStore()
    uc = dbiz.DataUsecase(store, None, Cfg())
    await uc.write_player(_pd(1001, 0, nickname="a"), [])
    assert (await uc.read_player(1001)).nickname == "a"


# ── ★ MySQL 是事实源 ────────────────────────────────────────────────────────


async def test_mysql_read_failure_raises_not_silent() -> None:
    """★ 库读失败必须抛出,不能静默返回 None。

    返回 None 会被 service 层转成 ErrNotFound —— 调用方会以为"这个玩家不存在",
    可能据此创建重复数据。§16 禁止静默吞错。
    """
    store, cache = FakeStore(), FakeCache()
    store.fail_read = True
    uc = dbiz.DataUsecase(store, cache, Cfg())
    with pytest.raises(errcode.PandoraError):
        await uc.read_player(1001)


async def test_read_zero_player_id_returns_none() -> None:
    """player_id=0 直接返回 None(不查库)。"""
    uc = dbiz.DataUsecase(FakeStore(), FakeCache(), Cfg())
    assert await uc.read_player(0) is None


async def test_write_zero_player_id_rejected() -> None:
    uc = dbiz.DataUsecase(FakeStore(), FakeCache(), Cfg())
    with pytest.raises(errcode.PandoraError, match="player_id required"):
        await uc.write_player(_pd(0, 0), [])


# ── ★ 降级日志限流窗口 ───────────────────────────────────────────────────────


def test_window_first_error_always_logs() -> None:
    """首错必打 —— 降级开始的时刻必须精确。"""
    w = logwindow.Window()
    should, streak = w.admit(1000, 5000)
    assert should and streak == 1


def test_window_suppresses_within_window() -> None:
    """窗口内只打一条,但累计数继续增长。"""
    w = logwindow.Window()
    w.admit(1000, 5000)
    for i in range(1, 10):
        should, streak = w.admit(1000 + i, 5000)
        assert not should
        assert streak == i + 1


def test_window_logs_again_after_window() -> None:
    w = logwindow.Window()
    w.admit(1000, 5000)
    w.admit(2000, 5000)
    should, streak = w.admit(6000, 5000)
    assert should and streak == 3


def test_window_recovered_reports_and_resets() -> None:
    """恢复时返回累计失败数并归零 —— 用于给降级区间画右边界。"""
    w = logwindow.Window()
    for i in range(5):
        w.admit(1000 + i, 5000)
    failed, _extra = w.recovered()
    assert failed == 5
    assert w.recovered() == (0, 0)  # 再次调用不重复报告


def test_window_zero_interval_logs_every_time() -> None:
    """window_ms <= 0 退化为不限流。"""
    w = logwindow.Window()
    for _ in range(5):
        should, _ = w.admit(1000, 0)
        assert should


# ── proto → SQL schema(替代 proto2mysql)────────────────────────────────────


def test_schema_derived_from_proto_descriptor() -> None:
    """表结构从 proto 描述符推导,不手写 SQL。"""
    s = ddata.PLAYER_DATA_SCHEMA
    assert s.table_name == "player_data"
    assert s.primary_key == ("player_id",)
    assert "player_id" in s.column_names()
    assert "version" in s.column_names()


def test_unsigned_semantics_carried_into_column_types() -> None:
    """★ uint64/uint32 必须建成 UNSIGNED 列。

    建成有符号列时,超过 2^31 的 player_id 会溢出 —— 严格模式下报错、
    非严格模式下**静默截断**,两种都不可接受(§5.12 + §9.24)。
    """
    sql = ddata.PLAYER_DATA_SCHEMA.create_table_sql()
    assert "`player_id` BIGINT UNSIGNED" in sql
    assert "`version` INT UNSIGNED" in sql


def test_updatable_fields_exclude_pk_and_version() -> None:
    """可更新列 = 全部列 - 主键 - version,**动态推导**。

    手工维护列表漏一个字段,那个字段就永远写不进 MySQL 且不报错。
    """
    fields = set(ddata.UPDATABLE_FIELDS)
    assert "player_id" not in fields
    assert "version" not in fields
    assert "nickname" in fields
    # 与描述符里的字段总数对得上
    all_fields = {f.name for f in data_service_pb2.PlayerData.DESCRIPTOR.fields}
    assert fields == all_fields - {"player_id", "version"}


def test_is_updatable_field_guards_injection() -> None:
    """未知列名必须被拒 —— 它会被拼进 SQL。"""
    assert ddata.is_updatable_field("nickname")
    assert not ddata.is_updatable_field("player_id")
    assert not ddata.is_updatable_field("version")
    assert not ddata.is_updatable_field("nickname`; DROP TABLE x; --")


def test_schema_rejects_repeated_and_message_fields() -> None:
    """repeated / 嵌套 message 无法映射成标量列 → 建表期 fail-fast。"""
    from pandora.trade.v1 import trade_pb2

    with pytest.raises(protosql.SchemaError):
        protosql.schema_of(trade_pb2.Order, table_name="t", primary_key="order_id")


def test_cache_key_matches_go_format() -> None:
    """缓存 key 与 Go 侧一致。"""
    assert ddata.cache_key(1001) == "pandora:data:player:1001"


# ── ★ 缓存值字节格式与 Go 互通(§9 不变量 16/17)──────────────────────────────
#
# 两个实现写的是**同一个** Redis key。格式只要差一个字节,Go 读 Python 写的条目判 miss、
# Python 读 Go 写的条目解不开被当 miss,双向命中率塌成 0 —— 而且全程不报错,零信号。
# 所以下面的用例手工拼「Go 侧会写出的字节」,不借 Python 自己的编码函数自证。


class FakeRedis:
    """只实现 RedisPlayerCache 用到的三个方法,存原始 bytes(decode_responses=False)。"""

    def __init__(self) -> None:
        self.kv: dict[str, bytes] = {}

    async def get(self, key: str):
        return self.kv.get(key)

    async def set(self, key: str, value: bytes, px: int | None = None) -> None:
        self.kv[key] = value

    async def delete(self, key: str) -> None:
        self.kv.pop(key, None)


# 手工拼出的一条「Go 侧写入」的缓存值,逐段对应 cache.go 的 Set:
#   50 44 43 02   魔数 'P','D','C',0x02(版本字节 0x02 = 位图格式)
#   00 00 00 02   位图长度(big-endian uint32)
#   fe 07         字段号 1..10 的位图(bit n = 字段号 n 存在)
#   protobuf      player_id=1001, version=3, nickname="alice"
_GO_MAGIC = bytes.fromhex("50444302")
_GO_MASK = bytes.fromhex("fe07")
_GO_BODY = bytes.fromhex("08e90710031a05") + b"alice"
_GO_ENTRY = _GO_MAGIC + bytes.fromhex("00000002") + _GO_MASK + _GO_BODY


def test_cache_schema_mask_matches_proto_descriptor() -> None:
    """位图必须由 pb2 描述符推导 —— 手写常量不会随 proto 演进,而且不会报错。

    这条同时钉住上面那串手拼的 Go 字节:proto 加字段后位图会变,它一起红,
    提醒你 Go 侧那条样本也要跟着更新,而不是让互通静默失效。
    """
    numbers = [f.number for f in data_service_pb2.PlayerData.DESCRIPTOR.fields]
    expected = bytearray(max(numbers) // 8 + 1)
    for n in numbers:
        expected[n // 8] |= 1 << (n % 8)
    assert ddata.CACHE_SCHEMA_MASK == bytes(expected)
    assert ddata.CACHE_SCHEMA_MASK == _GO_MASK


def test_cache_magic_matches_go_literal() -> None:
    """魔数逐字节等于 Go 的 'P','D','C',0x02(pb2 里没有这个符号,只能字面量对拍)。"""
    assert ddata.CACHE_MAGIC == _GO_MAGIC
    assert ddata.CACHE_HEADER_MIN_LEN == 8


async def test_python_reads_go_written_entry() -> None:
    """★ Python 必须能读出 Go 写的条目(手拼字节,不借 Python 自己的编码器)。"""
    rdb = FakeRedis()
    rdb.kv[ddata.cache_key(1001)] = _GO_ENTRY
    pd, hit = await ddata.RedisPlayerCache(rdb).get(1001)
    assert hit
    assert pd.player_id == 1001
    assert pd.version == 3
    assert pd.nickname == "alice"


async def test_python_writes_go_compatible_bytes() -> None:
    """★ Python 写出的字节必须是 Go 认得的格式:魔数 + 位图长度 + 位图 + pb。"""
    rdb = FakeRedis()
    await ddata.RedisPlayerCache(rdb).set(_pd(1001, 3, nickname="alice"), _dt.timedelta(minutes=5))
    raw = rdb.kv[ddata.cache_key(1001)]
    assert raw.startswith(_GO_MAGIC)
    assert raw[4:8] == len(ddata.CACHE_SCHEMA_MASK).to_bytes(4, "big")
    assert raw[8 : 8 + len(_GO_MASK)] == _GO_MASK
    # 尾部是纯 PlayerData pb —— Go 侧就是从 headerLen 之后直接 Unmarshal。
    body = data_service_pb2.PlayerData()
    body.ParseFromString(raw[8 + len(_GO_MASK) :])
    assert body.player_id == 1001
    assert body.nickname == "alice"


async def test_bare_pb_entry_is_miss() -> None:
    """无头的旧裸 pb(以及任何魔数不符的脏字节)必须当未命中,不能宽松解出错数据。"""
    rdb = FakeRedis()
    rdb.kv[ddata.cache_key(1001)] = _GO_BODY  # 没有魔数头
    assert await ddata.RedisPlayerCache(rdb).get(1001) == (None, False)


async def test_writer_missing_field_is_miss() -> None:
    """★ 写入方字段集缺本副本认得的字段 → 当未命中回落 MySQL(缓存投毒防护)。

    模拟旧副本:它的位图里没有本副本的最高字段号,它写的 pb 就可能缺那一列。
    信了这条 = 新列被抹掉,零停机升级破功。
    """
    numbers = [f.number for f in data_service_pb2.PlayerData.DESCRIPTOR.fields]
    top = max(numbers)
    stale = bytearray(ddata.CACHE_SCHEMA_MASK)
    stale[top // 8] &= ~(1 << (top % 8)) & 0xFF
    rdb = FakeRedis()
    rdb.kv[ddata.cache_key(1001)] = (
        _GO_MAGIC + len(stale).to_bytes(4, "big") + bytes(stale) + _GO_BODY
    )
    assert await ddata.RedisPlayerCache(rdb).get(1001) == (None, False)


def test_writer_superset_rule() -> None:
    """超集判定:writer ⊇ reader 才可信;writer 多出字段无妨,少一位就不行。"""
    reader = bytes.fromhex("fe07")
    assert ddata.writer_has_all_reader_fields(bytes.fromhex("ffff"), reader)
    assert ddata.writer_has_all_reader_fields(reader, reader)
    # writer 位图更短 = 它根本没有高位那些字段号
    assert not ddata.writer_has_all_reader_fields(bytes.fromhex("fe"), reader)
    assert not ddata.writer_has_all_reader_fields(bytes.fromhex("fe03"), reader)


async def test_player_id_mismatch_is_miss() -> None:
    """★ 串号(key 里的 id 与 pb 里的对不上)必须当未命中 —— 缓存投毒/键错配的强信号。"""
    rdb = FakeRedis()
    rdb.kv[ddata.cache_key(2002)] = _GO_ENTRY  # 条目里装的是 1001
    assert await ddata.RedisPlayerCache(rdb).get(2002) == (None, False)


async def test_truncated_header_is_miss() -> None:
    """位图长度声明得比实际字节长 → 当未命中,不能越界读。"""
    rdb = FakeRedis()
    rdb.kv[ddata.cache_key(1001)] = _GO_MAGIC + bytes.fromhex("00000040") + _GO_MASK
    assert await ddata.RedisPlayerCache(rdb).get(1001) == (None, False)


# ── conf 默认值:与 Go 侧 Defaults() 逐个相同 ────────────────────────────────
#
# 默认值分叉不会报错,只会让**同一份 yaml 喂两个实现跑出不同行为**。
# 所以下面不是"测代码能跑",而是把 Go 那三行 Defaults() 钉成可执行断言。

import asyncio  # noqa: E402

from google.protobuf import field_mask_pb2  # noqa: E402
from pandora.common.v1 import errcode_pb2  # noqa: E402
from pandora.data_service.v1 import data_service_pb2 as dpb  # noqa: E402

from pandorapy.services.data_service import budgets as dbudgets  # noqa: E402
from pandorapy.services.data_service import conf as dconf  # noqa: E402
from pandorapy.services.data_service import main as dmain  # noqa: E402
from pandorapy.services.data_service import service as dsvc  # noqa: E402


def _load_conf(tmp_path, body: str):
    p = tmp_path / "data_service.yaml"
    p.write_text(body, encoding="utf-8")
    return dconf.Config.load(p)


def test_conf_defaults_match_go(tmp_path) -> None:
    """空 yaml → :20003 / :21003 / 5m,与 Go 的 Defaults() 逐个相同。

    端口尤其要命:Envoy 的 cluster 和 run_services.ps1 的端口占用检查都钉在 20003/21003,
    默认值漂了就是"服务起来了但没人调得到"。
    """
    cfg = _load_conf(tmp_path, "{}\n")
    assert cfg.server.grpc.addr == ":20003"
    assert cfg.server.http.addr == ":21003"
    assert cfg.data.cache_ttl_td() == _dt.timedelta(minutes=5)


def test_conf_zero_cache_ttl_is_normalized(tmp_path) -> None:
    """★ 判据是 `<= 0` 不是 `== ""` —— 显式写 0 也要被纠回 5m(与 Go 同)。

    放行 0 的后果:回填缓存的 TTL 为 0,写进去就立刻过期,命中率恒 0 且零错误日志。
    """
    cfg = _load_conf(tmp_path, 'data:\n  cache_ttl: "0s"\n')
    assert cfg.data.cache_ttl_td() == _dt.timedelta(minutes=5)


def test_conf_explicit_values_win(tmp_path) -> None:
    """yaml 显式配了就不能被默认值盖掉(否则配置形同虚设)。"""
    cfg = _load_conf(
        tmp_path,
        'server:\n  grpc:\n    addr: ":30003"\n  http:\n    addr: ":31003"\n'
        'data:\n  cache_ttl: "90s"\n',
    )
    assert cfg.server.grpc.addr == ":30003"
    assert cfg.server.http.addr == ":31003"
    assert cfg.data.cache_ttl_td() == _dt.timedelta(seconds=90)


def test_conf_loads_the_real_go_yaml(repo_root) -> None:
    """★ 读 Go 版**同一份** etc/data_service-dev.yaml,不另建配置文件。

    这条测的是迁移前提本身:运维只维护一份配置。哪天 Go 侧加了个新段而 Python 侧的
    pydantic 模型把它判成非法,这里会红 —— 而不是等到部署时才发现起不来。
    """
    cfg = dconf.Config.load(
        repo_root / "services" / "data" / "data_service" / "etc" / "data_service-dev.yaml"
    )
    assert cfg.server.grpc.addr == ":20003"
    assert cfg.data.cache_ttl_td() == _dt.timedelta(minutes=5)
    # 只填 host 的单实例形态必须被 endpoints() 认出来,否则缓存会被静默关掉
    # (表现只是 MySQL QPS 变高,没有任何错误)。
    assert cfg.node.redis_client.endpoints() == ["127.0.0.1:6380"]
    assert cfg.node.mysql_client.dsn


def test_conf_rejects_cell_route_mode(tmp_path) -> None:
    """配了 cell_route.mode 必须拒启(Python 侧只实现了单 Cell)。

    静默按单 Cell 跑的后果是玩家被路由到错的 cell 且不报错 —— 起不来是刺眼的,
    静默跑错是致命的。main.py 把这条还原成 Go 的 cellroute_init_failed 事件名。
    """
    with pytest.raises(NotImplementedError) as ei:
        _load_conf(tmp_path, "cell_route:\n  mode: static\n")
    assert "cell_route" in str(ei.value)


def test_conf_empty_cell_route_mode_is_allowed(tmp_path) -> None:
    """mode 为空是**合法的单 Cell 配置**,不能被拒 —— 与 Go 的关闭态一致。"""
    cfg = _load_conf(tmp_path, 'cell_route:\n  mode: ""\n')
    assert cfg.server.grpc.addr == ":20003"


# ── 容量预算:数值与 Go 侧 Budgets() 逐个相同 ──────────────────────────────


def test_budgets_match_go() -> None:
    """阈值两边必须同值:同一套面板会同时看到 Go 和 Python 打的 db_capacity_budget_exceeded,
    阈值不同会让"到底超没超"这个问题按副本随机作答。"""
    (b,) = dbudgets.budgets()
    assert b.table == "player_data"
    assert b.max_rows == 300_000
    assert b.max_avg_row_bytes == 4096
    assert "dbcheck" in b.note  # note 要指出往哪查,不能只说一句"超预算"


# ── service 层:in-band code + 身份来源 ────────────────────────────────────


def _svc(store=None, cache=None):
    return dsvc.DataService(dbiz.DataUsecase(store or FakeStore(), cache, Cfg()))


async def test_service_read_not_found_is_not_internal() -> None:
    """★ "没这行"与"读失败"必须分开:合并成 ERR_INTERNAL 会让调用方
    把"新玩家还没建档"当成源库故障去重试。"""
    resp = await _svc().ReadPlayer(dpb.ReadPlayerRequest(player_id=1001), None)
    assert resp.code == errcode_pb2.ERR_NOT_FOUND


async def test_service_read_uses_request_player_id_not_auth_ctx() -> None:
    """★ data_service 是内网网关:player_id 取**请求体**,不从 JWT override。

    调用方(player / inventory)代表别的玩家读写,自己不持有那个玩家的 JWT。
    若照搬客户端面服务的 extract_player_id,拿到的恒为 0 → 每个 RPC 都 401,
    整条玩家数据链直接断。context 传 None 就是在证明这条路径根本不碰鉴权上下文 ——
    真去读 metadata 的话这里会 AttributeError。
    """
    store = FakeStore()
    svc = _svc(store)
    await svc.WritePlayer(dpb.WritePlayerRequest(data=_pd(1001, 0, nickname="a")), None)
    resp = await svc.ReadPlayer(dpb.ReadPlayerRequest(player_id=1001), None)
    assert resp.code == errcode_pb2.OK
    assert resp.data.player_id == 1001
    assert resp.data.nickname == "a"


async def test_service_write_empty_mask_returns_invalid_arg_in_band() -> None:
    """★ 业务失败走 body 里的 code,gRPC status 仍是 OK(不 abort)。

    改成 context.abort() 的话调用方会走到完全不同的错误分支 —— 迁移中最易悄悄改掉的语义。
    """
    svc = _svc()
    await svc.WritePlayer(dpb.WritePlayerRequest(data=_pd(1001, 0, nickname="a")), None)
    resp = await svc.WritePlayer(
        dpb.WritePlayerRequest(data=_pd(1001, 1, nickname="b")), None
    )
    assert resp.code == errcode_pb2.ERR_INVALID_ARG


async def test_service_write_returns_new_version() -> None:
    svc = _svc()
    await svc.WritePlayer(dpb.WritePlayerRequest(data=_pd(1001, 0, nickname="a")), None)
    resp = await svc.WritePlayer(
        dpb.WritePlayerRequest(
            data=_pd(1001, 1, nickname="b"),
            update_mask=field_mask_pb2.FieldMask(paths=["nickname"]),
        ),
        None,
    )
    assert resp.code == errcode_pb2.OK
    assert resp.new_version == 2


async def test_service_missing_data_is_invalid_arg() -> None:
    """没传 data 的请求 → ERR_INVALID_ARG(对应 Go 的 GetData()==nil 分支)。"""
    resp = await _svc().WritePlayer(dpb.WritePlayerRequest(), None)
    assert resp.code == errcode_pb2.ERR_INVALID_ARG


async def test_service_zero_player_id_rejected() -> None:
    assert (
        await _svc().ReadPlayer(dpb.ReadPlayerRequest(), None)
    ).code == errcode_pb2.ERR_INVALID_ARG
    assert (
        await _svc().InvalidateCache(dpb.InvalidateCacheRequest(), None)
    ).code == errcode_pb2.ERR_INVALID_ARG


async def test_service_invalidate_cache_ok_without_cache() -> None:
    """没配缓存时 InvalidateCache 是 no-op 成功 —— 不能因为"没缓存"报错,
    否则降级运行期间上游每次主动失效都会拿到一个假故障。"""
    resp = await _svc().InvalidateCache(dpb.InvalidateCacheRequest(player_id=1001), None)
    assert resp.code == errcode_pb2.OK


async def test_service_mysql_failure_surfaces_as_code_not_ok() -> None:
    """★ 源库读故障必须变成非 OK 的 code —— 不能被当成"没这行"吞掉。"""
    store = FakeStore()
    store.fail_read = True
    resp = await _svc(store).ReadPlayer(dpb.ReadPlayerRequest(player_id=1001), None)
    assert resp.code not in (errcode_pb2.OK, errcode_pb2.ERR_NOT_FOUND)


# ── 容量巡检循环:启动即一轮 + 单轮异常不杀循环 ────────────────────────────


async def test_capacity_guard_runs_a_round_at_startup(monkeypatch) -> None:
    """★ "启动即一轮"不是可省的优化:没有它,上线时就已超预算的表要等一小时
    才有第一条告警,而那一小时正是刚发版、最需要基线的窗口。"""
    rounds: list[object] = []

    async def _fake_round(pool, schema: str) -> None:
        rounds.append((pool, schema))

    monkeypatch.setattr(dmain, "_capacity_round", _fake_round)
    task = asyncio.create_task(dmain._run_capacity_guard("POOL", interval_sec=3600.0))
    for _ in range(4):
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert rounds == [("POOL", dmain.DEFAULT_DB)]


async def test_capacity_guard_survives_a_failing_round(monkeypatch) -> None:
    """单轮异常只丢本轮。没有这层兜底,一次意外异常会让循环**静默死掉**而服务看起来正常 ——
    从此再没有容量告警,且没有任何信号说明为什么。"""
    calls: list[int] = []

    async def _boom(pool, schema: str) -> None:
        assert schema == dmain.DEFAULT_DB
        calls.append(1)
        raise RuntimeError("information_schema unavailable")

    monkeypatch.setattr(dmain, "_capacity_round", _boom)
    task = asyncio.create_task(dmain._run_capacity_guard("POOL", interval_sec=0.01))
    await asyncio.sleep(0.08)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # 首轮炸了之后周期轮仍在跑 —— 这才是"单轮异常不杀循环"。
    assert len(calls) >= 2
