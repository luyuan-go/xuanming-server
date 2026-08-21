"""battle_result Model-B 授权链 —— 凭据判据、roster 规范化、终态回收出箱两阶段。

这些用例守的都是"错了不报错"的不变量:

  · 拒绝原因串对不上 Go → 两栈日志无法并排比对,灰度期出问题查不出是哪一栈判的;
  · roster 规范化漏了去重 / 0 校验 → 伪造的 player_ids 能把不在场玩家写进战绩;
  · phase1 RPC 失败被当成成功推进 DB → DS pod 永不回收,且行被标 released 后再无重试;
  · phase1 mark 失败被 `continue` 掉 → 带着"已回收"的错误认知继续跑同批剩余行;
  · phase2 delete 之前就打 finalized 日志 → 库操作失败时日志与库互相矛盾,排障按"已完成"处理。
"""

from __future__ import annotations

import asyncio

import pytest
from pandora.ds.v1 import allocator_pb2 as dspb
from structlog.testing import capture_logs

from pandorapy import dsauth, errcode
from pandorapy.auth import DS_AUTH_WRITER_EPOCH_V2, DS_TYPE_BATTLE
from pandorapy.services.battle_result import biz as bbiz
from pandorapy.services.battle_result import conf as bconf
from pandorapy.services.battle_result import credential as bcred
from pandorapy.services.battle_result import repo as brepo

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


# ── canonical_battle_roster ─────────────────────────────────────────────────


def test_canonical_roster_sorts_ascending() -> None:
    """规范化必须给出确定序:两栈、两副本对同一场算出的 roster 必须逐字节相同。"""
    assert bcred.canonical_battle_roster([30, 10, 20]) == [10, 20, 30]


def test_canonical_roster_rejects_empty() -> None:
    """空 roster 不是"没人",是权威记录缺失 —— fail-closed,不能当成允许结算。"""
    with pytest.raises(errcode.PandoraError) as ei:
        bcred.canonical_battle_roster([])
    assert errcode.as_code(ei.value) == errcode.ErrUnauthorized


@pytest.mark.parametrize("raw", [[1, 0, 2], [1, 2, 2]])
def test_canonical_roster_rejects_zero_and_duplicate(raw: list[int]) -> None:
    """0 和重复都说明权威记录本身已损坏,继续用它结算 = 用坏数据发段位 / 掉落。"""
    with pytest.raises(errcode.PandoraError) as ei:
        bcred.canonical_battle_roster(raw)
    assert errcode.as_code(ei.value) == errcode.ErrUnauthorized


# ── 拒绝原因枚举 ────────────────────────────────────────────────────────────


def _cred(**kw) -> dsauth.VerifiedCredential:
    base = dict(
        ds_type=DS_TYPE_BATTLE,
        match_id=1001,
        pod="battle-pod-1",
        instance_uid="uid-1",
        protocol_epoch=3,
        gen=7,
        jti="jti-1",
        exp_ms=9_000,
        kid="kid-1",
        token_sha256="a" * 64,
        writer_epoch=DS_AUTH_WRITER_EPOCH_V2,
    )
    base.update(kw)
    return dsauth.VerifiedCredential(**base)


def test_scope_reason_accepts_full_credential() -> None:
    assert bcred.credential_scope_reason(1001, _cred()) == ""


@pytest.mark.parametrize(
    ("match_id", "kw", "want"),
    [
        (0, {}, "missing_match_id"),
        (1001, {"ds_type": "hub"}, "ds_type_not_battle"),
        (1001, {"match_id": 1002}, "token_match_id_mismatch"),
        (1001, {"pod": ""}, "token_missing_pod"),
        (1001, {"instance_uid": ""}, "token_missing_instance_uid"),
        (1001, {"protocol_epoch": 0}, "token_missing_instance_epoch"),
        (1001, {"gen": 0}, "token_missing_gen"),
        (1001, {"jti": ""}, "token_missing_jti"),
        (1001, {"exp_ms": 0}, "token_missing_exp"),
        (1001, {"token_sha256": ""}, "token_missing_token_sha"),
        (1001, {"kid": ""}, "token_missing_kid"),
        (1001, {"writer_epoch": 1}, "token_writer_epoch_unsupported"),
    ],
)
def test_scope_reason_strings_match_go(match_id: int, kw: dict, want: str) -> None:
    """原因串是**跨栈契约**:灰度期两栈日志要能并排比对,拼写差一个字母就对不上。"""
    assert bcred.credential_scope_reason(match_id, _cred(**kw)) == want


def test_scope_reason_missing_credential() -> None:
    assert bcred.credential_scope_reason(1001, None) == "missing_credential"


# ── 终态回收出箱两阶段 ──────────────────────────────────────────────────────


class _Relay:
    """记录调用的 relay 替身;可分别让 phase1 / phase2 抛错。"""

    def __init__(self, *, release_error=None, finalize_error=None):
        self.released: list[int] = []
        self.finalized: list[int] = []
        self.release_error = release_error
        self.finalize_error = finalize_error

    async def release_terminal(self, rec) -> None:
        if self.release_error is not None:
            raise self.release_error
        self.released.append(rec.id)

    async def finalize_terminal(self, rec) -> None:
        if self.finalize_error is not None:
            raise self.finalize_error
        self.finalized.append(rec.id)


class _Repo:
    """只服务终态出箱三方法的仓储替身。真 SQL 语义由 test_battle_result_repo.py 打真库验。"""

    def __init__(self, rows: list[brepo.TerminalReleaseRecord]):
        self.rows = rows
        self.marked: list[int] = []
        self.deleted: list[int] = []
        self.mark_result = True
        self.mark_error: BaseException | None = None
        self.delete_error: BaseException | None = None

    async def fetch_terminal_release_outbox(self, limit: int, now_ms: int):
        return self.rows[:limit]

    async def mark_terminal_release_released(self, outbox_id: int, released_at_ms: int) -> bool:
        if self.mark_error is not None:
            raise self.mark_error
        self.marked.append(outbox_id)
        return self.mark_result

    async def delete_terminal_release_outbox(self, outbox_id: int) -> None:
        if self.delete_error is not None:
            raise self.delete_error
        self.deleted.append(outbox_id)


def _row(outbox_id: int, released_at_ms: int = 0) -> brepo.TerminalReleaseRecord:
    return brepo.TerminalReleaseRecord(
        match_id=1000 + outbox_id,
        allocation_id=f"alloc-{outbox_id}",
        ds_pod_name=f"battle-pod-{outbox_id}",
        released_at_ms=released_at_ms,
        id=outbox_id,
    )


def _cfg() -> bconf.BattleConf:
    cfg = bconf.Config(battle=bconf.BattleConf())
    cfg.apply_defaults()
    return cfg.battle


def _uc(repo, relay) -> bbiz.BattleResultUsecase:
    uc = bbiz.BattleResultUsecase(repo, None, None, None, _cfg())
    if relay is not None:
        uc.set_terminal_release_relay(relay)
    return uc


async def test_phase1_marks_and_does_not_finalize_same_round() -> None:
    """released_at_ms=0 的行本轮只做 phase1 + mark,绝不在同一轮直接 finalize。

    合并两阶段会让"UID 已 delete、durable 标记未落"的窗口不可观测,崩在中间时下一轮
    读到的是原始行,会把已经删掉的 UID 再删一次。
    """
    repo, relay = _Repo([_row(1)]), _Relay()
    assert await _uc(repo, relay).publish_terminal_release_batch() == 0
    assert relay.released == [1]
    assert repo.marked == [1]
    assert relay.finalized == [] and repo.deleted == []


async def test_phase2_finalizes_and_deletes_released_row() -> None:
    repo, relay = _Repo([_row(2, released_at_ms=123)]), _Relay()
    assert await _uc(repo, relay).publish_terminal_release_batch() == 1
    assert relay.finalized == [2] and repo.deleted == [2]
    assert relay.released == [] and repo.marked == []


async def test_phase1_rpc_failure_keeps_row_and_continues_batch() -> None:
    """RPC unknown 绝不能推进 DB:失败行保留重试,同批后续行照常处理。"""
    repo = _Repo([_row(1), _row(2, released_at_ms=123)])
    relay = _Relay(release_error=RuntimeError("redis unknown"))
    with capture_logs() as logs:
        assert await _uc(repo, relay).publish_terminal_release_batch() == 1
    assert repo.marked == []  # 失败行没有被标 released
    assert repo.deleted == [2]  # 同批后续行没有被拖累
    assert any(e["event"] == "terminal_release_phase1_failed" for e in logs)


async def test_phase1_mark_failure_aborts_whole_batch() -> None:
    """durable ACK 未知时必须整批中断:带着错误认知继续跑剩余行会放大不一致。"""
    repo = _Repo([_row(1), _row(2, released_at_ms=123)])
    repo.mark_error = RuntimeError("mysql gone")
    relay = _Relay()
    with capture_logs() as logs, pytest.raises(RuntimeError):
        await _uc(repo, relay).publish_terminal_release_batch()
    assert repo.deleted == []  # 第二行根本没轮到
    assert any(e["event"] == "terminal_release_mark_failed" for e in logs)


async def test_phase1_already_advanced_when_mark_returns_false() -> None:
    """并发副本已抢先标 released → 不是错误,只降级成 debug 且不重复推进。"""
    repo, relay = _Repo([_row(1)]), _Relay()
    repo.mark_result = False
    with capture_logs() as logs:
        assert await _uc(repo, relay).publish_terminal_release_batch() == 0
    events = [e["event"] for e in logs]
    assert "terminal_release_phase1_already_advanced" in events
    assert "terminal_release_phase1_done" not in events


async def test_phase2_finalize_failure_keeps_row() -> None:
    repo = _Repo([_row(2, released_at_ms=123)])
    relay = _Relay(finalize_error=RuntimeError("k8s unknown"))
    with capture_logs() as logs:
        assert await _uc(repo, relay).publish_terminal_release_batch() == 0
    assert repo.deleted == []
    assert any(e["event"] == "terminal_release_finalize_failed" for e in logs)


async def test_phase2_delete_failure_does_not_log_finalized() -> None:
    """delete 失败时绝不能出现 terminal_release_finalized —— 那会让排障按"已完成"处理。"""
    repo = _Repo([_row(2, released_at_ms=123)])
    repo.delete_error = RuntimeError("mysql gone")
    relay = _Relay()
    with capture_logs() as logs, pytest.raises(RuntimeError):
        await _uc(repo, relay).publish_terminal_release_batch()
    events = [e["event"] for e in logs]
    assert "terminal_release_delete_failed" in events
    assert "terminal_release_finalized" not in events


async def test_negative_released_at_ms_aborts_batch() -> None:
    """库里不可能出现的值 = schema 漂移 / 写入者 bug,必须当场 fail-closed 而不是跳过。"""
    repo, relay = _Repo([_row(1, released_at_ms=-1)]), _Relay()
    with capture_logs() as logs, pytest.raises(errcode.PandoraError) as ei:
        await _uc(repo, relay).publish_terminal_release_batch()
    assert errcode.as_code(ei.value) == errcode.ErrInvalidState
    assert relay.released == [] and relay.finalized == []
    assert any(e["event"] == "terminal_release_row_invalid" for e in logs)


async def test_publisher_disabled_without_relay() -> None:
    """legacy(authority_mode≠redis)下发布器不启动,且必须留下可检索的证据。"""
    repo = _Repo([_row(1)])
    uc = _uc(repo, None)
    assert await uc.publish_terminal_release_batch() == 0
    with capture_logs() as logs:
        await asyncio.wait_for(uc.run_terminal_release_publisher(), timeout=5.0)
    assert [e["event"] for e in logs] == ["terminal_release_publisher_disabled"]


# ── phase 门(authorize_result 的相位判据)─────────────────────────────────


def test_playable_battle_states_is_exactly_two() -> None:
    """可结算态多一个就等于给未就绪 / 已终态的 DS 开了结算权。"""
    assert bcred.PLAYABLE_BATTLE_STATES == ("ready", "running")


def test_battle_auth_keys_share_hashtag_slot() -> None:
    """两键必须落同一 Cluster slot —— MGET / WATCH 跨 slot 会直接报错。"""
    assert bcred.battle_auth_key(1001) == "pandora:ds:auth:{1001}"
    assert bcred.battle_key(1001) == "pandora:ds:battle:{1001}"


def test_phase_enum_names_exist() -> None:
    """相位常量从生成物取,不手抄 —— 手抄的值改了 proto 也不会报错。"""
    assert dspb.BATTLE_AUTH_PHASE_ACTIVE != dspb.BATTLE_AUTH_PHASE_ROTATING
    assert dspb.BATTLE_AUTH_PHASE_UNSPECIFIED == 0
