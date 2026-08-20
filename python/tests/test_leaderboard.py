"""leaderboard 服务测试 —— 存储层用**真 Lua**(fakeredis),结算逻辑用假仓库,
gRPC 语义用**真 grpc.aio server** 对打,MySQL 数据层打**真库**(没库则整体 skip)。

为什么不用 mock 做存储层:被测的正是那两段从 Go 逐字搬来的 Lua ——
"按 mode 算新分 + 打包 + 截断 + 直方图增减" 全在服务端一次执行完。
用 mock 等于把被测对象换成"我对 Lua 的想象",而搬 Lua 的全部价值就在于不必想象。
"""

from __future__ import annotations

import pathlib
import re

import grpc
import pytest

from mysqlfixture import MYSQL_DSN as DSN
from mysqlfixture import skip_only_if_mysql_is_down
from pandora.common.v1 import errcode_pb2
from pandora.leaderboard.v1 import leaderboard_pb2, leaderboard_pb2_grpc

from pandorapy import config as pconfig
from pandorapy import dbguard, errcode
from pandorapy import interceptors as pintercept
from pandorapy import server as pserver
from pandorapy.services.leaderboard import biz as lbbiz
from pandorapy.services.leaderboard import board_store as bs
from pandorapy.services.leaderboard import conf as lbconf
from pandorapy.services.leaderboard import repo as lbrepo
from pandorapy.services.leaderboard import reward_client as lbreward
from pandorapy.services.leaderboard import service as lbsvc

PLAYER_HEADER = pintercept.METADATA_KEY_PLAYER_ID

GLOBAL_BOARD = bs.BoardKey(board_type=1, scope=bs.SCOPE_GLOBAL, scope_id=0, period="S1")


# ══════════════════════════════════════════════════════════════════════════════
# 1. conf 默认值必须与 Go 的 Defaults() 逐个相同
# ══════════════════════════════════════════════════════════════════════════════
#
# 分叉的后果是:同一份 yaml 喂两个实现,行为不同而**两边都不报错**。
# 所以这里不写死期望值,而是**从 Go 源码里把数字抠出来**对拍 —— 写死的话
# Go 改了默认值,Python 这边照样绿。


def _go_defaults(repo_root: pathlib.Path) -> dict[str, int]:
    """从 Go 的 conf.go 的 Defaults() 里抠出 `c.Leaderboard.X = N` 形式的赋值。"""
    src = (
        repo_root / "services" / "runtime" / "leaderboard" / "internal" / "conf" / "conf.go"
    ).read_text(encoding="utf-8")
    body = src.split("func (c *Config) Defaults()", 1)[1]
    out: dict[str, int] = {}
    for name, value in re.findall(r"c\.Leaderboard\.(\w+) = (\d+)", body):
        out[name] = int(value)
    for field, value in re.findall(r'c\.Server\.(\w+)\.Addr = "([^"]+)"', body):
        out[field] = value  # type: ignore[assignment]
    return out


def test_conf_defaults_match_go(repo_root: pathlib.Path) -> None:
    go = _go_defaults(repo_root)
    assert go, "没从 Go 的 Defaults() 里抠出任何默认值 —— 正则或函数签名变了"

    cfg = lbconf.Config()
    cfg.apply_defaults()
    lb = cfg.leaderboard

    assert lb.default_list_limit == go["DefaultListLimit"]
    assert lb.max_list_limit == go["MaxListLimit"]
    assert lb.default_around_radius == go["DefaultAroundRadius"]
    assert lb.default_settle_top_n == go["DefaultSettleTopN"]
    assert lb.default_estimate_bucket_width == go["DefaultEstimateBucketWidth"]
    assert lb.retention_days == go["RetentionDays"]
    assert lb.retention_sweep_batch == go["RetentionSweepBatch"]
    assert cfg.server.grpc.addr == go["Grpc"]
    assert cfg.server.http.addr == go["Http"]


def test_conf_defaults_use_le_zero_not_eq_zero() -> None:
    """判据符号必须是 `<= 0`。Go 用的就是 `<= 0`,写成 `== 0` 会让负数原样透传。"""
    cfg = lbconf.Config()
    cfg.leaderboard.max_list_limit = -1
    cfg.leaderboard.retention_days = -7
    cfg.apply_defaults()
    assert cfg.leaderboard.max_list_limit == lbconf.DEFAULT_MAX_LIST_LIMIT
    assert cfg.leaderboard.retention_days == lbconf.DEFAULT_RETENTION_DAYS


def test_allow_noop_reward_defaults_false() -> None:
    """默认 False 是**安全默认**:生产漏配 inventory_addr 必须拒启,而不是静默不发奖。"""
    assert lbconf.LeaderboardConf().allow_noop_reward is False


def test_retention_mode_validate_rejects_typo() -> None:
    """拼错的模式必须启动期拒启;运行期取值则回落 report_only(绝不猜成 delete)。"""
    lb = lbconf.LeaderboardConf(retention_mode="delet")
    with pytest.raises(ValueError):
        lb.validate_retention_mode()
    assert lb.retention_mode_parsed() is dbguard.Mode.REPORT_ONLY

    ok = lbconf.LeaderboardConf(retention_mode="delete")
    ok.validate_retention_mode()
    assert ok.retention_mode_parsed() is dbguard.Mode.DELETE


def test_real_dev_yaml_loads(repo_root: pathlib.Path) -> None:
    """真实 etc/leaderboard-dev.yaml 必须能被 Python 版原样吃下(同一份配置两个实现)。"""
    path = repo_root / "services" / "runtime" / "leaderboard" / "etc" / "leaderboard-dev.yaml"
    cfg = lbconf.Config.load(path)
    assert cfg.server.grpc.addr == ":20007"
    assert cfg.server.http.addr == ":21007"
    assert cfg.leaderboard.inventory_addr == "127.0.0.1:20015"
    assert cfg.leaderboard.allow_noop_reward is False
    # kafka / session_gate 必须被**建模字段**接住,不能掉进 model_extra ——
    # 掉进去的话 Python 副本会永远不发结算事件,而 yaml 明明配了 broker。
    assert cfg.kafka.brokers == ["127.0.0.1:9093"]
    assert cfg.session_gate.require is False


def test_proto_enum_values_not_hand_copied() -> None:
    """scope / mode 常量必须来自 proto 生成物。手抄会让测试抄同一个错值,永远不红。"""
    assert bs.SCOPE_GLOBAL == leaderboard_pb2.LEADERBOARD_SCOPE_GLOBAL
    assert bs.SCOPE_GUILD == leaderboard_pb2.LEADERBOARD_SCOPE_GUILD
    assert bs.SCOPE_CUSTOM == leaderboard_pb2.LEADERBOARD_SCOPE_CUSTOM
    assert bs.MODE_INCREMENT == leaderboard_pb2.SUBMIT_MODE_INCREMENT


# ══════════════════════════════════════════════════════════════════════════════
# 2. Redis ZSET 存储层(真 Lua)
# ══════════════════════════════════════════════════════════════════════════════


@pytest.fixture
async def store():
    fakeredis = pytest.importorskip("fakeredis")
    pytest.importorskip("lupa", reason="Lua 脚本需要 fakeredis[lua]")
    server = fakeredis.FakeServer()
    client = fakeredis.aioredis.FakeRedis(server=server, decode_responses=False)
    try:
        yield bs.RedisBoardStore(client)
    finally:
        await client.aclose()


TS = 1_800_000_000_000  # 固定时间戳,避免测试依赖 wall clock


async def test_key_format_matches_go(store: bs.RedisBoardStore) -> None:
    """key 前缀 / board 串是**跨语言硬契约**:迁移期两栈读写同一批榜。"""
    b = bs.BoardKey(board_type=7, scope=bs.SCOPE_GUILD, scope_id=42, period="")
    assert b.board_str() == "7:2:42:-"  # period 空用 "-" 占位
    assert b.z_key() == "pandora:lb:{7:2:42:-}:z"
    assert b.all_keys() == [
        "pandora:lb:{7:2:42:-}:z",
        "pandora:lb:{7:2:42:-}:t",
        "pandora:lb:{7:2:42:-}:m",
        "pandora:lb:{7:2:42:-}:s",
        "pandora:lb:{7:2:42:-}:h",
    ]


async def test_submit_set_if_higher_does_not_lower(store: bs.RedisBoardStore) -> None:
    opt = bs.Options(estimate_bucket_width=25)
    score, rank = await store.submit(GLOBAL_BOARD, 1001, 500, bs.MODE_SET_IF_HIGHER, opt, TS)
    assert (score, rank) == (500, 1)
    # 更低的分不得覆盖 —— 覆盖了的话玩家打了一局差的就掉榜,这是最经典的排行榜 bug。
    score, _ = await store.submit(GLOBAL_BOARD, 1001, 300, bs.MODE_SET_IF_HIGHER, opt, TS + 1)
    assert score == 500
    score, _ = await store.submit(GLOBAL_BOARD, 1001, 900, bs.MODE_SET_IF_HIGHER, opt, TS + 2)
    assert score == 900


async def test_submit_increment_accumulates(store: bs.RedisBoardStore) -> None:
    opt = bs.Options(estimate_bucket_width=25)
    await store.submit(GLOBAL_BOARD, 1001, 10, bs.MODE_INCREMENT, opt, TS)
    score, _ = await store.submit(GLOBAL_BOARD, 1001, 15, bs.MODE_INCREMENT, opt, TS + 1)
    assert score == 25


async def test_descending_order_and_range(store: bs.RedisBoardStore) -> None:
    opt = bs.Options(estimate_bucket_width=25)
    for entity, score in ((1, 100), (2, 300), (3, 200)):
        await store.submit(GLOBAL_BOARD, entity, score, bs.MODE_SET, opt, TS)
    entries = await store.range(GLOBAL_BOARD, 0, 10, ascending=False)
    assert [(e.entity_id, e.score, e.rank) for e in entries] == [
        (2, 300, 1),
        (3, 200, 2),
        (1, 100, 3),
    ]
    assert await store.total(GLOBAL_BOARD) == 3


async def test_ascending_board_orders_low_first(store: bs.RedisBoardStore) -> None:
    """升序榜(竞速用时):分低的排前面。meta 在首次上报时定死。"""
    board = bs.BoardKey(board_type=2, scope=bs.SCOPE_GLOBAL, period="R1")
    opt = bs.Options(ascending=True, estimate_bucket_width=25)
    for entity, score in ((1, 100), (2, 300), (3, 200)):
        await store.submit(board, entity, score, bs.MODE_SET, opt, TS)
    asc, tie, exists = await store.get_meta(board)
    assert (asc, tie, exists) == (True, False, True)
    entries = await store.range(board, 0, 10, ascending=asc)
    assert [e.entity_id for e in entries] == [1, 3, 2]


async def test_get_meta_absent_board(store: bs.RedisBoardStore) -> None:
    """榜不存在时 exists=False —— SettleBoard 靠它拦住"在空榜上占掉 settle uk"。"""
    asc, tie, exists = await store.get_meta(bs.BoardKey(board_type=99, scope=bs.SCOPE_GLOBAL))
    assert (asc, tie, exists) == (False, False, False)


async def test_tie_break_by_time_prefers_earlier(store: bs.RedisBoardStore) -> None:
    """同分先达者名次高。packed = real - normTs*1e-13(降序榜)。"""
    board = bs.BoardKey(board_type=3, scope=bs.SCOPE_GLOBAL, period="T")
    opt = bs.Options(tie_break_by_time=True, estimate_bucket_width=25)
    await store.submit(board, 11, 500, bs.MODE_SET, opt, TS)           # 先达
    await store.submit(board, 22, 500, bs.MODE_SET, opt, TS + 60_000)  # 后达
    entries = await store.range(board, 0, 10, ascending=False)
    assert [e.entity_id for e in entries] == [11, 22]
    # 真实分必须能从 packed 还原回整数 500(时间项 < 0.5,不影响取整)。
    assert [e.score for e in entries] == [500, 500]


async def test_max_size_truncation_and_estimate(store: bs.RedisBoardStore) -> None:
    """截断出榜的玩家仍要能查到"约第几名",且**估算名次不得落进精确区**。"""
    board = bs.BoardKey(board_type=4, scope=bs.SCOPE_GLOBAL, period="M")
    opt = bs.Options(max_size=3, estimate_bucket_width=25)
    for entity in range(1, 8):  # 分数 100,200,...,700
        await store.submit(board, entity, entity * 100, bs.MODE_SET, opt, TS)

    assert await store.total(board) == 3  # 精确榜只留 Top-3
    entries = await store.range(board, 0, 10, ascending=False)
    assert [e.entity_id for e in entries] == [7, 6, 5]

    # 被截断的 entity=1(最低分)不在精确榜
    _entry, found = await store.rank(board, 1, ascending=False)
    assert found is False

    est, total, est_found = await store.estimate(board, 1, ascending=False)
    assert est_found is True
    assert est.score == 100
    assert total == 7  # 直方图是全员口径,不随截断收缩
    assert est.rank >= 4  # 钳到 ZCARD+1 之后:绝不能报出"第 3 名"与精确榜打架
    assert est.updated_at_ms == 0  # 估算不带时间


async def test_estimate_absent_entity(store: bs.RedisBoardStore) -> None:
    """从未上报过 → found=False(不是"估算成最后一名")。"""
    opt = bs.Options(estimate_bucket_width=25)
    await store.submit(GLOBAL_BOARD, 1, 100, bs.MODE_SET, opt, TS)
    _e, _t, found = await store.estimate(GLOBAL_BOARD, 999, ascending=False)
    assert found is False


async def test_remove_refunds_histogram(store: bs.RedisBoardStore) -> None:
    """移除必须**同步回扣直方图** —— 不回扣的话封号玩家永远占着一格,
    所有人的估算名次系统性偏低一名,且没有任何运行期信号。"""
    board = bs.BoardKey(board_type=5, scope=bs.SCOPE_GLOBAL, period="R")
    opt = bs.Options(max_size=2, estimate_bucket_width=25)
    for entity in range(1, 6):
        await store.submit(board, entity, entity * 100, bs.MODE_SET, opt, TS)
    _e, total_before, _f = await store.estimate(board, 1, ascending=False)
    assert total_before == 5

    await store.remove(board, 3)
    _e, total_after, _f = await store.estimate(board, 1, ascending=False)
    assert total_after == 4
    # 被移除者自己也查不到了
    _e, _t, found = await store.estimate(board, 3, ascending=False)
    assert found is False


async def test_clear_keeps_meta_delete_drops_it(store: bs.RedisBoardStore) -> None:
    """clear 与 delete 的唯一区别就是 meta —— 清了 meta,下周期首次上报会用新参数
    重新建榜(桶宽 / 排序方向都可能翻),历史口径就对不上了。"""
    board = bs.BoardKey(board_type=6, scope=bs.SCOPE_GLOBAL, period="C")
    opt = bs.Options(ascending=True, estimate_bucket_width=50)
    await store.submit(board, 1, 100, bs.MODE_SET, opt, TS)

    await store.clear(board)
    assert await store.total(board) == 0
    asc, _tie, exists = await store.get_meta(board)
    assert (asc, exists) == (True, True)  # meta 保留

    await store.delete(board)
    _asc, _tie, exists = await store.get_meta(board)
    assert exists is False


async def test_around_includes_self(store: bs.RedisBoardStore) -> None:
    opt = bs.Options(estimate_bucket_width=25)
    for entity in range(1, 8):
        await store.submit(GLOBAL_BOARD, entity, entity * 10, bs.MODE_SET, opt, TS)
    entries, found = await store.around(GLOBAL_BOARD, 4, radius=1, ascending=False)
    assert found is True
    assert [e.entity_id for e in entries] == [5, 4, 3]
    assert [e.rank for e in entries] == [3, 4, 5]

    _entries, found = await store.around(GLOBAL_BOARD, 999, radius=1, ascending=False)
    assert found is False


async def test_range_rejects_bad_args(store: bs.RedisBoardStore) -> None:
    """limit<=0 / offset<0 → 空列表(与 Go 同,不报错)。"""
    assert await store.range(GLOBAL_BOARD, 0, 0, ascending=False) == []
    assert await store.range(GLOBAL_BOARD, -1, 10, ascending=False) == []


def test_unpack_real_is_floor_half_not_bankers() -> None:
    """Go 是 floor(p+0.5);Python 内置 round() 是银行家舍入,在 x.5 上差 1 分。"""
    assert bs.unpack_real(0.5) == 1  # round(0.5) 会给 0
    assert bs.unpack_real(1.5) == 2
    assert bs.unpack_real(2.5) == 3  # round(2.5) 会给 2
    assert bs.unpack_real(499.9999999) == 500


# ══════════════════════════════════════════════════════════════════════════════
# 3. biz(结算 / 发奖 / 补扫)—— 假仓库 + 假发奖方
# ══════════════════════════════════════════════════════════════════════════════


class FakeSnowflake:
    def __init__(self, start: int = 900_001) -> None:
        self._next = start

    def generate(self) -> int:
        value = self._next
        self._next += 1
        return value


class FakeRepo:
    """内存版结算归档仓库。行为逐条对齐 MySQL 的唯一键语义。"""

    def __init__(self) -> None:
        self.settlements: dict[str, lbrepo.SettlementRecord] = {}
        self.snapshots: dict[int, list[lbrepo.SnapshotRow]] = {}
        self.rewards: dict[str, lbrepo.RewardLogRecord] = {}
        self.mark_calls: list[tuple[str, int]] = []

    async def claim_settlement(self, rec):  # noqa: ANN001
        existing = self.settlements.get(rec.settle_idem_key)
        if existing is not None:
            return existing, True
        self.settlements[rec.settle_idem_key] = rec
        return rec, False

    async def save_snapshot(self, settlement_id: int, rows) -> None:  # noqa: ANN001
        self.snapshots.setdefault(settlement_id, []).extend(rows)

    async def load_snapshot(self, settlement_id: int):  # noqa: ANN001
        return sorted(self.snapshots.get(settlement_id, []), key=lambda r: r.rank)

    async def claim_reward(self, rec) -> bool:  # noqa: ANN001
        if rec.grant_idem_key in self.rewards:
            return True
        self.rewards[rec.grant_idem_key] = rec
        return False

    async def mark_reward(self, key: str, status: int, updated_at_ms: int) -> None:
        self.mark_calls.append((key, status))
        rec = self.rewards.get(key)
        if rec is None:
            return
        # 与 build_mark_reward_sql 同语义:失败标记不得覆盖 GRANTED。
        if status != lbrepo.REWARD_GRANTED and rec.status == lbrepo.REWARD_GRANTED:
            return
        rec.status = status
        rec.updated_at_ms = updated_at_ms

    async def list_ungranted_rewards(self, older_than_ms: int, limit: int):
        rows = [
            r
            for r in self.rewards.values()
            if r.status != lbrepo.REWARD_GRANTED and r.updated_at_ms < older_than_ms
        ]
        return sorted(rows, key=lambda r: r.updated_at_ms)[:limit]


class FakeGranter:
    def __init__(self, fail_for: set[int] | None = None) -> None:
        self.calls: list[tuple[int, str, int]] = []
        self._fail_for = fail_for or set()

    async def grant(self, player_id: int, idem_key: str, items) -> None:  # noqa: ANN001
        self.calls.append((player_id, idem_key, len(items)))
        if player_id in self._fail_for:
            raise errcode.PandoraError(
                errcode.ErrLeaderboardRewardFailed, "inventory down"
            )


class FakeEvents:
    def __init__(self, fail: bool = False) -> None:
        self.pushed: list[int] = []
        self._fail = fail

    async def push_settle(self, settlement_id: int, board, winners) -> None:  # noqa: ANN001
        if self._fail:
            raise RuntimeError("kafka down")
        self.pushed.append(settlement_id)


def _reward_table() -> leaderboard_pb2.RewardTable:
    return leaderboard_pb2.RewardTable(
        tiers=[
            leaderboard_pb2.RewardTier(
                rank_from=1,
                rank_to=3,
                items=[leaderboard_pb2.RewardItem(item_config_id=101, count=10)],
            )
        ]
    )


async def _seeded_usecase(store: bs.RedisBoardStore, **kw):
    repo = kw.pop("repo", None) or FakeRepo()
    granter = kw.pop("granter", None) or FakeGranter()
    events = kw.pop("events", None)
    cfg = lbconf.LeaderboardConf()
    uc = lbbiz.LeaderboardUsecase(repo, store, granter, events, FakeSnowflake(), cfg)
    return uc, repo, granter


async def test_settle_board_not_found(store: bs.RedisBoardStore) -> None:
    """榜不存在必须在占掉 settle uk **之前**拦住。"""
    uc, repo, _g = await _seeded_usecase(store)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.settle_board(GLOBAL_BOARD, 10, None, False, "")
    assert ei.value.code == errcode.ErrLeaderboardBoardNotFound
    assert repo.settlements == {}  # 一个批次都没建


async def test_settle_grants_and_is_idempotent(store: bs.RedisBoardStore) -> None:
    opt = bs.Options(estimate_bucket_width=25)
    for entity in (1, 2, 3, 4):
        await store.submit(GLOBAL_BOARD, entity, entity * 100, bs.MODE_SET, opt, TS)

    uc, repo, granter = await _seeded_usecase(store)
    res = await uc.settle_board(GLOBAL_BOARD, 3, _reward_table(), False, "")
    assert res.already_settled is False
    assert res.settled_count == 3
    assert [w.entity_id for w in res.winners] == [4, 3, 2]
    assert len(granter.calls) == 3
    assert all(r.status == lbrepo.REWARD_GRANTED for r in repo.rewards.values())

    # 幂等重放:不重复发奖,winners 从**快照**回放
    replay = await uc.settle_board(GLOBAL_BOARD, 3, _reward_table(), False, "")
    assert replay.already_settled is True
    assert replay.settlement_id == res.settlement_id
    assert [w.entity_id for w in replay.winners] == [4, 3, 2]
    assert len(granter.calls) == 3  # 一次都没多发


async def test_settle_reset_after_clears_board(store: bs.RedisBoardStore) -> None:
    opt = bs.Options(estimate_bucket_width=25)
    await store.submit(GLOBAL_BOARD, 1, 100, bs.MODE_SET, opt, TS)
    uc, _repo, _g = await _seeded_usecase(store)
    await uc.settle_board(GLOBAL_BOARD, 10, None, True, "")
    assert await store.total(GLOBAL_BOARD) == 0
    # meta 保留 → 下周期还是同一个榜
    _asc, _tie, exists = await store.get_meta(GLOBAL_BOARD)
    assert exists is True


async def test_settle_guild_board_skips_player_grants(store: bs.RedisBoardStore) -> None:
    """工会榜 entity=guild_id,**不能**直接发到玩家背包 —— 发了就是给一个不存在的
    玩家 ID 打钱,inventory 那边会建出一份幽灵背包。"""
    board = bs.BoardKey(board_type=9, scope=bs.SCOPE_GUILD, scope_id=7, period="S1")
    opt = bs.Options(estimate_bucket_width=25)
    await store.submit(board, 555, 100, bs.MODE_SET, opt, TS)
    uc, repo, granter = await _seeded_usecase(store)
    res = await uc.settle_board(board, 10, _reward_table(), False, "")
    assert res.settled_count == 1
    assert granter.calls == []
    assert repo.rewards == {}
    assert repo.snapshots  # 快照照落


async def test_settle_grant_failure_marks_failed_and_continues(
    store: bs.RedisBoardStore,
) -> None:
    """一个人发失败不得中断整批 —— 中断的话后面的名次全收不到奖且无补偿路径。"""
    opt = bs.Options(estimate_bucket_width=25)
    for entity in (1, 2, 3):
        await store.submit(GLOBAL_BOARD, entity, entity * 100, bs.MODE_SET, opt, TS)
    granter = FakeGranter(fail_for={3})
    uc, repo, _g = await _seeded_usecase(store, granter=granter)
    await uc.settle_board(GLOBAL_BOARD, 3, _reward_table(), False, "")

    statuses = {r.entity_id: r.status for r in repo.rewards.values()}
    assert statuses[3] == lbrepo.REWARD_FAILED
    assert statuses[2] == lbrepo.REWARD_GRANTED
    assert statuses[1] == lbrepo.REWARD_GRANTED


async def test_kafka_push_failure_does_not_fail_settle(store: bs.RedisBoardStore) -> None:
    """结算事件是弱依赖:kafka 挂了,结算本身仍然算数(已落库)。"""
    opt = bs.Options(estimate_bucket_width=25)
    await store.submit(GLOBAL_BOARD, 1, 100, bs.MODE_SET, opt, TS)
    uc, _repo, _g = await _seeded_usecase(store, events=FakeEvents(fail=True))
    res = await uc.settle_board(GLOBAL_BOARD, 10, None, False, "")
    assert res.settled_count == 1


async def test_retry_ungranted_rewards(store: bs.RedisBoardStore) -> None:
    """补扫覆盖 FAILED + PENDING(崩残),且 older_than 把新记录挡在外面。"""
    opt = bs.Options(estimate_bucket_width=25)
    for entity in (1, 2):
        await store.submit(GLOBAL_BOARD, entity, entity * 100, bs.MODE_SET, opt, TS)
    granter = FakeGranter(fail_for={2})
    uc, repo, _g = await _seeded_usecase(store, granter=granter)
    await uc.settle_board(GLOBAL_BOARD, 2, _reward_table(), False, "")
    failed_keys = [k for k, r in repo.rewards.items() if r.status == lbrepo.REWARD_FAILED]
    assert len(failed_keys) == 1

    # 把记录时间推老,让补扫能看见它;granter 这次不再失败。
    for rec in repo.rewards.values():
        rec.updated_at_ms -= 10 * 60 * 1000
    uc._granter = FakeGranter()  # noqa: SLF001 —— 换掉下游模拟"inventory 恢复了"
    granted, failed = await uc.retry_ungranted_rewards(older_than_sec=120.0, limit=100)
    assert (granted, failed) == (1, 0)
    assert all(r.status == lbrepo.REWARD_GRANTED for r in repo.rewards.values())


async def test_retry_bad_payload_marks_failed_not_infinite_loop(
    store: bs.RedisBoardStore,
) -> None:
    """解不开的 payload 必须标 FAILED 收口,否则补扫会永远重扫同一条。"""
    repo = FakeRepo()
    repo.rewards["lb:1:9"] = lbrepo.RewardLogRecord(
        settlement_id=1,
        entity_id=9,
        rank=1,
        grant_idem_key="lb:1:9",
        status=lbrepo.REWARD_PENDING,
        reward_payload=b"",  # 空 payload 解出 0 件 → 脏数据
        created_at_ms=0,
        updated_at_ms=0,
    )
    uc, _repo, _g = await _seeded_usecase(store, repo=repo)
    granted, failed = await uc.retry_ungranted_rewards(older_than_sec=0.0, limit=10)
    assert (granted, failed) == (0, 1)
    assert repo.rewards["lb:1:9"].status == lbrepo.REWARD_FAILED


def test_rewards_for_rank_takes_first_matching_tier() -> None:
    table = leaderboard_pb2.RewardTable(
        tiers=[
            leaderboard_pb2.RewardTier(
                rank_from=1, rank_to=10, items=[leaderboard_pb2.RewardItem(item_config_id=1, count=1)]
            ),
            leaderboard_pb2.RewardTier(
                rank_from=1, rank_to=3, items=[leaderboard_pb2.RewardItem(item_config_id=2, count=2)]
            ),
        ]
    )
    got = lbbiz.rewards_for_rank(table, 2)
    assert [g.item_config_id for g in got] == [1]  # 只取第一个匹配区间,不合并
    assert lbbiz.rewards_for_rank(table, 99) == []
    # count<=0 的项被剔掉(与 Go 同):发 0 个道具是配置噪声,不该产生一次 RPC
    zero = leaderboard_pb2.RewardTable(
        tiers=[
            leaderboard_pb2.RewardTier(
                rank_from=1, rank_to=1, items=[leaderboard_pb2.RewardItem(item_config_id=3, count=0)]
            )
        ]
    )
    assert lbbiz.rewards_for_rank(zero, 1) == []


def test_reward_payload_roundtrip_and_size_gate() -> None:
    items = [lbreward.RewardGrant(item_config_id=101, count=10)]
    raw = lbbiz.encode_reward_grants(items)
    assert lbbiz.decode_reward_grants(raw) == items
    # 写入侧字节闸:超上限必须**拒写**,不能让它进库被静默截断。
    huge = [lbreward.RewardGrant(item_config_id=i, count=i) for i in range(1, 4000)]
    with pytest.raises(dbguard.PayloadTooLargeError):
        lbbiz.encode_reward_grants(huge)


async def test_get_rank_falls_back_to_estimate(store: bs.RedisBoardStore) -> None:
    board = bs.BoardKey(board_type=11, scope=bs.SCOPE_GLOBAL, period="E")
    opt = bs.Options(max_size=2, estimate_bucket_width=25)
    for entity in range(1, 6):
        await store.submit(board, entity, entity * 100, bs.MODE_SET, opt, TS)
    uc, _repo, _g = await _seeded_usecase(store)

    exact = await uc.get_rank(board, 5)
    assert exact.found and exact.estimated is False and exact.entry.rank == 1

    est = await uc.get_rank(board, 1)
    assert est.found and est.estimated is True and est.total_submitters == 5

    missing = await uc.get_rank(board, 999)
    assert missing.found is False


async def test_get_range_clamps_limit(store: bs.RedisBoardStore) -> None:
    """limit 必须被钳到 max_list_limit —— 不钳的话一次 GetRange 能把整个榜拖出来。"""
    opt = bs.Options(estimate_bucket_width=25)
    for entity in range(1, 6):
        await store.submit(GLOBAL_BOARD, entity, entity, bs.MODE_SET, opt, TS)
    cfg = lbconf.LeaderboardConf(max_list_limit=2, default_list_limit=2)
    uc = lbbiz.LeaderboardUsecase(FakeRepo(), store, FakeGranter(), None, FakeSnowflake(), cfg)
    entries, total = await uc.get_range(GLOBAL_BOARD, 0, 1000)
    assert len(entries) == 2
    assert total == 5


async def test_submit_rejects_invalid_board_and_entity(store: bs.RedisBoardStore) -> None:
    uc, _repo, _g = await _seeded_usecase(store)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.submit_score(
            bs.BoardKey(board_type=0, scope=bs.SCOPE_GLOBAL), 1, 1, bs.MODE_SET, bs.Options()
        )
    assert ei.value.code == errcode.ErrLeaderboardInvalidBoard

    with pytest.raises(errcode.PandoraError) as ei:
        await uc.submit_score(bs.BoardKey(board_type=1, scope=99), 1, 1, bs.MODE_SET, bs.Options())
    assert ei.value.code == errcode.ErrLeaderboardInvalidBoard

    with pytest.raises(errcode.PandoraError) as ei:
        await uc.submit_score(GLOBAL_BOARD, 0, 1, bs.MODE_SET, bs.Options())
    assert ei.value.code == errcode.ErrInvalidArg


# ══════════════════════════════════════════════════════════════════════════════
# 4. gRPC service 语义(真 server 对打)
# ══════════════════════════════════════════════════════════════════════════════


@pytest.fixture
async def lb_channel(store: bs.RedisBoardStore):
    uc = lbbiz.LeaderboardUsecase(
        FakeRepo(), store, FakeGranter(), None, FakeSnowflake(), lbconf.LeaderboardConf()
    )
    server = pserver.build_grpc_server(
        pconfig.GrpcConf(addr="127.0.0.1:0"), auth_required=False
    )
    leaderboard_pb2_grpc.add_LeaderboardServiceServicer_to_server(
        lbsvc.LeaderboardService(uc), server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    try:
        yield channel
    finally:
        await channel.close()
        await server.stop(None)


def _pb_board() -> leaderboard_pb2.BoardKey:
    return leaderboard_pb2.BoardKey(
        board_type=1, scope=leaderboard_pb2.LEADERBOARD_SCOPE_GLOBAL, period="S1"
    )


async def test_system_rpc_rejects_player_jwt(lb_channel) -> None:  # noqa: ANN001
    """★ 方向别写反:**带**玩家身份才拒。写反了玩家就能自助刷榜 / 自助发奖。"""
    stub = leaderboard_pb2_grpc.LeaderboardServiceStub(lb_channel)
    for call, req in (
        (stub.SubmitScore, leaderboard_pb2.SubmitScoreRequest(board=_pb_board(), entity_id=1, score=1)),
        (stub.RemoveEntry, leaderboard_pb2.RemoveEntryRequest(board=_pb_board(), entity_id=1)),
        (stub.SettleBoard, leaderboard_pb2.SettleBoardRequest(board=_pb_board())),
        (stub.DeleteBoard, leaderboard_pb2.DeleteBoardRequest(board=_pb_board())),
    ):
        resp = await call(req, metadata=((PLAYER_HEADER, "12345"),))
        assert resp.code == errcode_pb2.ERR_PERMISSION_DENY


async def test_system_rpc_allows_internal_call(lb_channel) -> None:  # noqa: ANN001
    stub = leaderboard_pb2_grpc.LeaderboardServiceStub(lb_channel)
    resp = await stub.SubmitScore(
        leaderboard_pb2.SubmitScoreRequest(
            board=_pb_board(),
            entity_id=1,
            score=500,
            mode=leaderboard_pb2.SUBMIT_MODE_SET,
        )
    )
    assert resp.code == errcode_pb2.OK
    assert (resp.new_score, resp.rank) == (500, 1)


async def test_read_rpc_allows_player_jwt(lb_channel) -> None:  # noqa: ANN001
    """读接口经 Envoy 由客户端直接调,带 JWT 是常态。"""
    stub = leaderboard_pb2_grpc.LeaderboardServiceStub(lb_channel)
    await stub.SubmitScore(
        leaderboard_pb2.SubmitScoreRequest(
            board=_pb_board(), entity_id=1, score=500, mode=leaderboard_pb2.SUBMIT_MODE_SET
        )
    )
    resp = await stub.GetRank(
        leaderboard_pb2.GetRankRequest(board=_pb_board(), entity_id=1),
        metadata=((PLAYER_HEADER, "1"),),
    )
    assert resp.code == errcode_pb2.OK
    assert resp.found is True
    assert resp.entry.rank == 1


async def test_business_failure_is_in_band_not_grpc_error(lb_channel) -> None:  # noqa: ANN001
    """★ 业务失败必须是 gRPC status=OK + body.code=ErrXxx。
    改成 abort 的话 UE 客户端会走到完全不同的错误分支。"""
    stub = leaderboard_pb2_grpc.LeaderboardServiceStub(lb_channel)
    call = stub.GetRange(
        leaderboard_pb2.GetRangeRequest(
            board=leaderboard_pb2.BoardKey(board_type=0), limit=10
        )
    )
    resp = await call
    assert await call.code() == grpc.StatusCode.OK
    assert resp.code == errcode_pb2.ERR_LEADERBOARD_INVALID_BOARD


async def test_settle_board_not_found_returns_code(lb_channel) -> None:  # noqa: ANN001
    stub = leaderboard_pb2_grpc.LeaderboardServiceStub(lb_channel)
    resp = await stub.SettleBoard(
        leaderboard_pb2.SettleBoardRequest(board=_pb_board(), top_n=10)
    )
    assert resp.code == errcode_pb2.ERR_LEADERBOARD_BOARD_NOT_FOUND


# ══════════════════════════════════════════════════════════════════════════════
# 5. MySQL 数据层(**打真库**;没库整体 skip,不假装通过)
# ══════════════════════════════════════════════════════════════════════════════
#
#   docker run -d --name pandora-mysql-verify -p 13306:3306 \
#     -e MYSQL_ROOT_PASSWORD=pandora_dev_root \
#     mysql:8.4 --sql-mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"


# 与 deploy/mysql-init/10-leaderboard-tables.sql 同构。
_DDL = [
    """CREATE TABLE IF NOT EXISTS leaderboard_settlement (
        settlement_id BIGINT UNSIGNED NOT NULL,
        board_type INT UNSIGNED NOT NULL,
        scope TINYINT NOT NULL,
        scope_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
        period VARCHAR(32) NOT NULL DEFAULT '',
        top_n INT NOT NULL,
        settled_count INT NOT NULL DEFAULT 0,
        settle_idempotency_key VARCHAR(96) NOT NULL,
        reset_after TINYINT NOT NULL DEFAULT 0,
        created_at_ms BIGINT NOT NULL,
        PRIMARY KEY (settlement_id),
        UNIQUE KEY uk_settle_idem (settle_idempotency_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS leaderboard_snapshot (
        settlement_id BIGINT UNSIGNED NOT NULL,
        `rank` BIGINT NOT NULL,
        entity_id BIGINT UNSIGNED NOT NULL,
        score BIGINT NOT NULL,
        created_at_ms BIGINT NOT NULL,
        PRIMARY KEY (settlement_id, `rank`),
        KEY idx_created (created_at_ms)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS leaderboard_reward_log (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        settlement_id BIGINT UNSIGNED NOT NULL,
        entity_id BIGINT UNSIGNED NOT NULL,
        `rank` BIGINT NOT NULL,
        grant_idempotency_key VARCHAR(96) NOT NULL,
        status TINYINT NOT NULL DEFAULT 0,
        reward_pb VARBINARY(2048) NOT NULL DEFAULT '',
        created_at_ms BIGINT NOT NULL,
        updated_at_ms BIGINT NOT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uk_grant_idem (grant_idempotency_key),
        KEY idx_status_updated (status, updated_at_ms)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
]

_TABLES = ("leaderboard_settlement", "leaderboard_snapshot", "leaderboard_reward_log")


@pytest.fixture
async def mysql_repo():
    asyncmy = pytest.importorskip("asyncmy")
    from mysqlfixture import ensure_database, parse_go_dsn

    cfg = parse_go_dsn(DSN, default_db="pandora_leaderboard")
    # * 只有"MySQL 没起来"才允许跳过 —— 夹具自己的代码(ensure_database / cfg 取键 /
    #   create_pool 形参)抛的异常必须原样冒红。判据、放行集与 CI 文案契约
    #   见 tests/mysqlfixture.py 的 skip_only_if_mysql_is_down。
    with skip_only_if_mysql_is_down(cfg, "leaderboard 数据层测试"):
        await ensure_database(asyncmy, cfg)
        pool = await asyncmy.create_pool(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["user"],
            password=cfg["password"],
            db=cfg["db"],
            autocommit=False,
            minsize=1,
            maxsize=4,
        )

    async with pool.acquire() as conn, conn.cursor() as cur:
        for ddl in _DDL:
            await cur.execute(ddl)
        for table in _TABLES:
            await cur.execute(f"TRUNCATE TABLE {table}")
        await conn.commit()
    try:
        # 库名传的是本进程的独占测试库 —— 与 main.py 一样从 DSN 取,而不是写死
        # pandora_leaderboard(写死的话保留期清理会对着一个不存在的 schema 报错)。
        yield lbrepo.MySQLLeaderboardRepo(pool, cfg["db"]), cfg["db"]
    finally:
        pool.close()
        await pool.wait_closed()


def _settlement(idem_key: str, settlement_id: int = 111) -> lbrepo.SettlementRecord:
    return lbrepo.SettlementRecord(
        settlement_id=settlement_id,
        board_type=1,
        scope=int(bs.SCOPE_GLOBAL),
        scope_id=0,
        period="S1",
        top_n=10,
        settled_count=3,
        settle_idem_key=idem_key,
        reset_after=True,
        created_at_ms=TS,
    )


async def test_mysql_claim_settlement_is_idempotent(mysql_repo) -> None:  # noqa: ANN001
    """幂等靠**唯一键冲突**,不是"先查再插" —— 后者在两副本同时结算时会双双发奖。"""
    repo, _db = mysql_repo
    rec, already = await repo.claim_settlement(_settlement("lb:1:1:0:S1"))
    assert already is False and rec.settlement_id == 111

    again, already = await repo.claim_settlement(_settlement("lb:1:1:0:S1", settlement_id=222))
    assert already is True
    assert again.settlement_id == 111  # 回放的是**已存**批次,不是新号
    assert again.reset_after is True
    assert again.period == "S1"


async def test_mysql_snapshot_roundtrip_and_replay(mysql_repo) -> None:  # noqa: ANN001
    repo, _db = mysql_repo
    await repo.claim_settlement(_settlement("k-snap"))
    rows = [
        lbrepo.SnapshotRow(rank=2, entity_id=20, score=200, created_at_ms=TS),
        lbrepo.SnapshotRow(rank=1, entity_id=10, score=300, created_at_ms=TS),
    ]
    await repo.save_snapshot(111, rows)
    await repo.save_snapshot(111, rows)  # 幂等回放:INSERT IGNORE 不该报错也不该翻倍

    got = await repo.load_snapshot(111)
    assert [(r.rank, r.entity_id, r.score) for r in got] == [(1, 10, 300), (2, 20, 200)]


async def test_mysql_reward_claim_and_mark_guard(mysql_repo) -> None:  # noqa: ANN001
    """★ 失败标记必须带 `status <> GRANTED` 守卫(INC-20260811-001 同型缺陷)。

    不带的话:A 副本发成功写 GRANTED 的同时,B 副本因下游瞬时不可用把同一行打回
    FAILED —— 已发的奖重回补发工作集,过保留期后重放就是**真重复发放**。
    """
    repo, _db = mysql_repo
    rec = lbrepo.RewardLogRecord(
        settlement_id=111,
        entity_id=10,
        rank=1,
        grant_idem_key="lb:111:10",
        status=lbrepo.REWARD_PENDING,
        reward_payload=lbbiz.encode_reward_grants(
            [lbreward.RewardGrant(item_config_id=101, count=5)]
        ),
        created_at_ms=TS,
        updated_at_ms=TS,
    )
    assert await repo.claim_reward(rec) is False
    assert await repo.claim_reward(rec) is True  # uk 命中 = 本名次已发过

    await repo.mark_reward("lb:111:10", lbrepo.REWARD_GRANTED, TS + 1)
    # 已 GRANTED 的行不得被打回 FAILED
    await repo.mark_reward("lb:111:10", lbrepo.REWARD_FAILED, TS + 2)
    rows = await repo.list_ungranted_rewards(TS + 10_000, 100)
    assert rows == []  # 仍是 GRANTED,没回到补发工作集


async def test_mysql_list_ungranted_respects_grace(mysql_repo) -> None:  # noqa: ANN001
    """older_than 把"刚结算还在同步发"的批次挡在扫描外。"""
    repo, _db = mysql_repo
    payload = lbbiz.encode_reward_grants([lbreward.RewardGrant(item_config_id=1, count=1)])
    for i, updated in ((1, TS - 10_000), (2, TS + 10_000)):
        await repo.claim_reward(
            lbrepo.RewardLogRecord(
                settlement_id=111,
                entity_id=i,
                rank=i,
                grant_idem_key=f"lb:111:{i}",
                status=lbrepo.REWARD_FAILED,
                reward_payload=payload,
                created_at_ms=TS,
                updated_at_ms=updated,
            )
        )
    rows = await repo.list_ungranted_rewards(TS, 100)
    assert [r.entity_id for r in rows] == [1]
    # payload 必须原样回来 —— 它是补发重放的**权威入参**,坏一个字节这条奖就永远发不出去
    assert lbbiz.decode_reward_grants(rows[0].reward_payload) == [
        lbreward.RewardGrant(item_config_id=1, count=1)
    ]


async def test_mysql_retention_sweep_report_only_deletes_nothing(mysql_repo) -> None:  # noqa: ANN001
    """默认 report_only:统计待清理量但**一行都不删**(用户 2026-07-22 指令)。"""
    repo, db = mysql_repo
    await repo.claim_settlement(_settlement("k-sweep"))
    await repo.save_snapshot(
        111, [lbrepo.SnapshotRow(rank=1, entity_id=10, score=1, created_at_ms=1)]
    )
    out = await repo.sweep_snapshots_before(dbguard.Mode.REPORT_ONLY, TS, 100)
    assert out.matched == 1
    assert out.deleted == 0
    assert len(await repo.load_snapshot(111)) == 1

    out = await repo.sweep_snapshots_before(dbguard.Mode.DELETE, TS, 100)
    assert out.deleted == 1
    assert await repo.load_snapshot(111) == []


async def test_mysql_reward_sweep_never_touches_pending(mysql_repo) -> None:  # noqa: ANN001
    """PENDING / FAILED 是补发工作集,清掉等于把"还没发出去的奖"连证据一起删了。"""
    repo, _db = mysql_repo
    payload = lbbiz.encode_reward_grants([lbreward.RewardGrant(item_config_id=1, count=1)])
    for i, status in ((1, lbrepo.REWARD_GRANTED), (2, lbrepo.REWARD_PENDING)):
        await repo.claim_reward(
            lbrepo.RewardLogRecord(
                settlement_id=111,
                entity_id=i,
                rank=i,
                grant_idem_key=f"lb:111:{i}",
                status=status,
                reward_payload=payload,
                created_at_ms=1,
                updated_at_ms=1,
            )
        )
    out = await repo.sweep_granted_rewards_before(dbguard.Mode.DELETE, TS, 100)
    assert out.deleted == 1
    remaining = await repo.list_ungranted_rewards(TS, 100)
    assert [r.entity_id for r in remaining] == [2]
