"""`pandorapy.dsauthfence` 的移植验收 —— 对拍 `pkg/dsauthfence/{fence,etcd,security}.go`。

★ 本文件守的是什么

    这是**两栈共读同一份 etcd** 的控制面栅栏。它的失效形状不是"报错",而是:

      - key 前缀差一个字符 → Go 与 Python 各写各的 → 两个进程都认为自己是唯一 writer;
      - required 值差一个字节 → CAS 永远不成立(或更糟:成立在旧策略上);
      - 续租"超时"被当成成功 → 本副本永远认为自己还持有,而 capability 早被接管。

    三种都**不会有任何运行期信号**。所以下面的断言里,凡是标 ★ 的都直接对着
    Go 源文件逐字核对,而不是对着"我记得应该是这样"。

★ 变异验证

    每条用例的 docstring 标了 `★ 变异:...→ 本条红`,并已**实际做过一遍**:
    把产品代码改成那个样子、确认本条转红、再恢复。假件必须真的把代码推进目标失败态 ——
    "造不出失败条件"的假件等于没验(见 tests/test_no_mutation_probe_residue.py 的教训)。
"""

from __future__ import annotations

import asyncio
import ast
import contextlib
import json
import pathlib
import re
import time
from collections.abc import AsyncIterator

import pytest

from pandorapy import dsauthfence as fence
from pandorapy import errcode

from tests.srcprobe import module_code_text

_GOOD_DIGEST = "sha256:" + "a" * 64


# ══════════════════════════════════════════════════════════════════════════
# Go 源码解析(★ 对拍的**唯一**事实来源:不抄常量,直接读 Go 文件)
# ══════════════════════════════════════════════════════════════════════════


@pytest.fixture(scope="module")
def fence_go(repo_root: pathlib.Path) -> str:
    return (repo_root / "pkg" / "dsauthfence" / "fence.go").read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def security_go(repo_root: pathlib.Path) -> str:
    return (repo_root / "pkg" / "dsauthfence" / "security.go").read_text(encoding="utf-8")


def _go_const_str(src: str, name: str) -> str:
    """抠出 `Name = "value"` 里的字面量。抠不到直接 fail —— Go 改名必须显式同步。"""
    m = re.search(rf"\b{name}\s*=\s*\"((?:[^\"\\]|\\.)*)\"", src)
    assert m, f"未能从 Go 源解析常量 {name}(Go 侧改名了?请同步本测试与移植件)"
    return m.group(1)


def _go_func_literal(src: str, func: str) -> str:
    """抠出 `func xxx(prefix string) string { return cleanPrefix(prefix) + "lit" }`。

    ★ `[^}\n]` 而不是 `[^}]`：后者能跨行,而贪婪回溯会抓到**下一个函数**的引号,
    把两个声明拼成一个“字面量”—— 对拍器自己解析错了还会说得很像真的。
    """
    m = re.search(rf"func {func}\(prefix string\) string\s*\{{[^}}\n]*?\"([^\"\n]+)\"", src)
    assert m, f"未能从 Go 源解析 {func} 的 key 后缀"
    return m.group(1)


def _go_feature_map(src: str, var: str) -> dict[str, tuple[str, ...]]:
    """解析 `var requiredPolicyVxFeatures = map[string][]string{...}`。"""
    start = src.index(f"var {var} = map[string][]string{{")
    end = src.index("\n}\n", start)
    body = src[start:end]
    out: dict[str, tuple[str, ...]] = {}
    for service, inner in re.findall(r"\"(\w+)\":\s*\{([^}]*)\}", body):
        out[service] = tuple(re.findall(r"\"([^\"]+)\"", inner))
    assert out, f"未能从 Go 源解析 {var}"
    return out


def _go_capability_json_tags(src: str) -> list[tuple[str, bool]]:
    """解析 Capability struct 的 json tag,返回 [(字段名, 是否 omitempty)] 的**有序**列表。"""
    start = src.index("type Capability struct {")
    end = src.index("\n}", start)
    tags: list[tuple[str, bool]] = []
    for raw in re.findall(r"json:\"([^\"]+)\"", src[start:end]):
        parts = raw.split(",")
        tags.append((parts[0], "omitempty" in parts[1:]))
    assert tags, "未能从 Go 源解析 Capability 的 json tag"
    return tags


# ══════════════════════════════════════════════════════════════════════════
# ① key 模板 / 策略值 —— 两栈脑裂的根因都在这里
# ══════════════════════════════════════════════════════════════════════════


def test_key_templates_match_go(fence_go: str) -> None:
    """★ etcd key 模板逐字符对拍 Go。

    前缀写错 = 两栈各写各的 capability = 双 writer 脑裂,而且**零信号**。

    ★ 变异:`DEFAULT_PREFIX = "/pandora/ds-auth/"` 改成 `"/pandora/dsauth/"` → 本条红。
    ★ 变异:`_REQUIRED_KEY_SUFFIX` 改成 `"required-epoch"` → 本条红。
    """
    assert fence.DEFAULT_PREFIX == _go_const_str(fence_go, "DefaultPrefix")
    prefix = fence.DEFAULT_PREFIX
    assert fence.required_key(prefix) == prefix + _go_func_literal(fence_go, "requiredKey")
    assert fence.capability_prefix(prefix) == prefix + _go_func_literal(
        fence_go, "capabilityPrefix"
    )
    assert fence.activation_lock_key(prefix) == prefix + _go_func_literal(
        fence_go, "activationLockKey"
    )
    # capabilityKey(prefix, service, uid) = capabilityPrefix + service + "/" + uid
    assert (
        fence.capability_key(prefix, "login", "pod-uid-1")
        == "/pandora/ds-auth/capabilities/login/pod-uid-1"
    )


def test_clean_prefix_matches_go_trimsuffix() -> None:
    """★ `cleanPrefix` = TrimSuffix("/") + "/" —— 只去**一个**尾斜杠。

    ★ 变异:`prefix.removesuffix("/")` 改成 `prefix.rstrip("/")` → "a//" 那条红。
    """
    assert fence.clean_prefix("/a") == "/a/"
    assert fence.clean_prefix("/a/") == "/a/"
    # Go 的 TrimSuffix 只剥一个;rstrip 会把两个都剥掉 → 前缀不同 → 键空间分叉。
    assert fence.clean_prefix("/a//") == "/a//"


def test_required_values_match_go(fence_go: str) -> None:
    """★ required 原始值逐字节对拍 —— 它是 capability 注册 CAS 的比较对象。

    ★ 变异:`REQUIRED_POLICY_V3` 末尾 `-v1` 改成 `-v2` → 本条红。
    """
    assert fence.REQUIRED_POLICY_V2 == _go_const_str(fence_go, "RequiredPolicyV2")
    assert fence.REQUIRED_POLICY_V3 == _go_const_str(fence_go, "RequiredPolicyV3")
    assert fence.REQUIRED_VALUE_V2 == "2@" + fence.REQUIRED_POLICY_V2
    assert fence.REQUIRED_VALUE_V3 == "2@" + fence.REQUIRED_POLICY_V3
    assert fence.PROTOCOL_EPOCH_V2 == 2


def test_lost_reason_constants_match_go(fence_go: str) -> None:
    """★ 失效原因常量对拍 —— 它们进日志与审计,漂移即 LogQL 查询静默落空。

    ★ 变异:`LOST_REASON_REQUIRED_DELETED = "required_deleted"` → 本条红。
    """
    pairs = {
        "LostReasonLeaseKeepaliveEnded": fence.LOST_REASON_LEASE_KEEPALIVE_ENDED,
        "LostReasonRequiredWatchError": fence.LOST_REASON_REQUIRED_WATCH_ERROR,
        "LostReasonRequiredDeleted": fence.LOST_REASON_REQUIRED_DELETED,
        "LostReasonRequiredRegressed": fence.LOST_REASON_REQUIRED_REGRESSED,
        "LostReasonRequiredAdvanced": fence.LOST_REASON_REQUIRED_ADVANCED,
        "LostReasonRequiredWatchClosed": fence.LOST_REASON_REQUIRED_WATCH_CLOSED,
    }
    for go_name, py_value in pairs.items():
        assert py_value == _go_const_str(fence_go, go_name), go_name
    # 六个分支必须互不相同 —— 合并任意两个就退回 2026-07-29 那次"分不清失租还是 watch 断"。
    assert len(set(pairs.values())) == len(pairs)


def test_feature_policy_maps_match_go(fence_go: str) -> None:
    """★ 生产 writer 策略表逐条对拍。

    多一个 / 少一个 feature 都会让本副本以**别的策略**入场:V2 与 V3 共享数据面
    epoch 2,单看 epoch 分不出来,只有 feature 集合能分。

    ★ 变异:从 `REQUIRED_POLICY_V3_FEATURES["hub_allocator"]` 删掉
      `"hub-successor-lease-v1"` → 本条红。
    """
    assert {k: tuple(v) for k, v in fence.REQUIRED_POLICY_V2_FEATURES.items()} == (
        _go_feature_map(fence_go, "requiredPolicyV2Features")
    )
    assert {k: tuple(v) for k, v in fence.REQUIRED_POLICY_V3_FEATURES.items()} == (
        _go_feature_map(fence_go, "requiredPolicyV3Features")
    )


def test_topology_lock_error_text_matches_go(fence_go: str) -> None:
    """★ 发布阻断文案对拍 —— 它是运维判"该重试还是该停止发布"的唯一依据。

    ★ 变异:文案里 "is not wired" 改成 "unavailable" → 本条红。
    """
    m = re.search(r"ErrTopologyChangeLockProviderUnavailable = errors\.New\(\s*\"([^\"]+)\"", fence_go)
    assert m, "未能从 Go 源解析 ErrTopologyChangeLockProviderUnavailable"
    assert fence.ERR_TOPOLOGY_CHANGE_LOCK_PROVIDER_UNAVAILABLE == m.group(1)
    err = fence.TopologyChangeLockProviderUnavailableError()
    assert err.msg == m.group(1)
    # 发布阻断不是"稍后重试"——错误码不能是 ErrUnavailable。
    assert err.code == errcode.ErrInvalidState


# ══════════════════════════════════════════════════════════════════════════
# ② 值解析:回滚栅栏与规范形
# ══════════════════════════════════════════════════════════════════════════


def test_parse_required_state_rejects_naked_two() -> None:
    """★ **刻意不接受裸 "2"** —— 这条字节级不兼容正是旧 epoch-2 二进制的回滚栅栏。

    接受了它,旧二进制就能在 V2/V3 策略下重新拿到 writer capability。

    ★ 变异:在 `parse_required_state` 里加一条 `if s == "2": return RequiredState(epoch=2,...)`
      → 本条红。
    """
    with pytest.raises(fence.FenceError):
        fence.parse_required_state(b"2")
    with pytest.raises(fence.FenceError):
        fence.parse_required_state(b"")
    with pytest.raises(fence.FenceError):
        fence.parse_required_state(b"3@" + fence.REQUIRED_POLICY_V3.encode())


def test_parse_required_state_known_values() -> None:
    """三个受支持值解析出的四元组必须完全确定。

    ★ 变异:V3 分支的 `policy_generation` 写成 `REQUIRED_POLICY_GENERATION_V2` → 本条红。
    """
    v1 = fence.parse_required_state(b"1")
    assert (v1.epoch, v1.policy_generation, v1.policy_id, v1.raw_value) == (1, 1, "", "1")
    v2 = fence.parse_required_state(fence.REQUIRED_VALUE_V2.encode())
    assert (v2.epoch, v2.policy_generation, v2.policy_id) == (2, 2, fence.REQUIRED_POLICY_V2)
    v3 = fence.parse_required_state(fence.REQUIRED_VALUE_V3.encode())
    assert (v3.epoch, v3.policy_generation, v3.policy_id) == (2, 3, fence.REQUIRED_POLICY_V3)


@pytest.mark.parametrize("raw", [b"", b"0", b"02", b" 2", b"2 ", b"+2", b"2\n", b"-1", b"x"])
def test_parse_epoch_rejects_non_canonical(raw: bytes) -> None:
    """★ 只接受规范十进制正整数 —— "02" 与 "2" 是同一个数但**不同字节**,会破坏 CAS。

    Python 的 `int()` 接受 "+2" / " 2 " / "2\\n",Go 的 ParseUint 不接受,
    所以这些分支必须显式判,不能靠 `int()` 兜。

    ★ 变异:`parse_epoch` 改成 `return int(s)` → 本条(除 ""/"0"/"x" 外)红。
    """
    with pytest.raises(fence.FenceError):
        fence.parse_epoch(raw)


def test_parse_epoch_rejects_uint32_overflow() -> None:
    """★ Go 的 uint32 会**拒绝**超界;Python 整数不回绕会一路带着天文数字跑。

    ★ 变异:去掉 `value > _MAX_UINT32` 这一判 → 本条红。
    """
    assert fence.parse_epoch(b"4294967295") == 0xFFFF_FFFF
    with pytest.raises(fence.FenceError):
        fence.parse_epoch(b"4294967296")


def test_parse_epoch_rejects_fullwidth_digits() -> None:
    """★ `"２"`(全角)`str.isdigit()` 为真且 `int()` 认,Go 不认 —— 两栈分叉。

    ★ 变异:去掉 `s.isascii()` 这一判 → 本条红。
    """
    with pytest.raises(fence.FenceError):
        fence.parse_epoch("２")


def test_required_value_lookups_round_trip() -> None:
    """代 ↔ 值 ↔ writer epoch 的三张表必须自洽。

    ★ 变异:`required_writer_epoch_for_policy_generation(V3)` 返回 3 → 本条红。
    """
    for generation in (1, 2, 3):
        value = fence.required_value_for_policy_generation(generation)
        state = fence.parse_required_state(value)
        assert state.policy_generation == generation
        assert state.policy_id == fence.required_policy_id_for_generation(generation)
        assert state.epoch == fence.required_writer_epoch_for_policy_generation(generation)
    with pytest.raises(fence.FenceError):
        fence.required_value_for_policy_generation(4)
    # RequiredValueForEpoch 只覆盖 epoch(不是代),V3 不在其中。
    assert fence.required_value_for_epoch(2) == fence.REQUIRED_VALUE_V2
    with pytest.raises(fence.FenceError):
        fence.required_value_for_epoch(3)


# ══════════════════════════════════════════════════════════════════════════
# ③ feature 校验与正则形状
# ══════════════════════════════════════════════════════════════════════════


def test_feature_pattern_matches_go(fence_go: str) -> None:
    """★ feature 正则对拍 Go(把 Go 的 `^...$` 折算成 Python 的 `\\A...\\Z`)。

    ★ 变异:`{2,63}` 改成 `{2,64}` → 本条红。
    """
    m = re.search(r"capabilityFeaturePattern = regexp\.MustCompile\(`([^`]+)`\)", fence_go)
    assert m, "未能从 Go 源解析 capabilityFeaturePattern"
    go_body = m.group(1)
    assert go_body.startswith("^") and go_body.endswith("$")
    assert fence.CAPABILITY_FEATURE_PATTERN.pattern == r"\A" + go_body[1:-1] + r"\Z"


def test_feature_pattern_rejects_trailing_newline() -> None:
    """★ 正则必须用 `\\A...\\Z`:Python 的 `$` 会匹配**结尾换行**,Go 的不会。

    环境变量 / 配置文件带尾换行是常态,`^...$` 会让 Python 侧放行、Go 侧拒绝 ——
    校验器最不该有的分叉。

    ★ 变异:`CAPABILITY_FEATURE_PATTERN` 改回 `re.compile(r"^[a-z][a-z0-9-]{2,63}$")`
      → 本条红(strip 检查也会被同一条 `\\n` 拦住,所以这里直接测正则本身)。
    """
    assert fence.CAPABILITY_FEATURE_PATTERN.match("hub-successor-lease-v1")
    assert not fence.CAPABILITY_FEATURE_PATTERN.match("hub-successor-lease-v1\n")
    assert not fence.DIGEST_PATTERN.match(_GOOD_DIGEST + "\n")
    assert not fence.ETCD_IDENTITY_REVISION_PATTERN.match("r1\n")


def test_validate_features_rejects_duplicates_and_junk() -> None:
    """重复 feature 必须拒 —— 否则"精确集合"退化成"多重集",子集判定会误放行。

    ★ 变异:去掉 `if feature in seen` 分支 → 重复那条红。
    """
    fence.validate_features(["hub-owner-cleanup-v1", "hub-reservation-ledger-v1"])
    with pytest.raises(fence.FenceError):
        fence.validate_features(["a-b", "a-b"])  # 重复(且太短,双重非法)
    with pytest.raises(fence.FenceError):
        fence.validate_features(["Hub-Owner"])  # 大写
    with pytest.raises(fence.FenceError):
        fence.validate_features([" hub-owner-cleanup-v1"])  # 首空白


def test_equal_feature_set_is_exact_not_subset() -> None:
    """★ 精确相等,不是子集 / 超集。

    超集放行 = 一个多带了实验 feature 的二进制能以生产策略入场。

    ★ 变异:`equal_feature_set` 去掉 `len(actual) != len(expected)` 判定 → 本条红。
    """
    expected = fence.REQUIRED_POLICY_V3_FEATURES["hub_allocator"]
    assert fence.equal_feature_set(list(expected), list(expected))
    assert not fence.equal_feature_set(list(expected)[:-1], list(expected))
    assert not fence.equal_feature_set([*expected, "extra-feature-v1"], list(expected))
    # 顺序无关(Go 用 map 判定)。
    assert fence.equal_feature_set(list(reversed(expected)), list(expected))


# ══════════════════════════════════════════════════════════════════════════
# ④ required 策略 × capability 的合法性(fail-closed 矩阵)
# ══════════════════════════════════════════════════════════════════════════


def _state(generation: int) -> fence.RequiredState:
    return fence.parse_required_state(fence.required_value_for_policy_generation(generation))


def test_policy_v2_requires_exact_v2_features() -> None:
    """V2 下必须精确广播 V2 feature 集。

    ★ 变异:V2 分支改成 `return`(无条件放行)→ 本条红。
    """
    fence.validate_required_policy_for_capability(_state(2), "login", 2, ())
    with pytest.raises(fence.FenceError):
        fence.validate_required_policy_for_capability(_state(2), "login", 2, ("extra-v1",))
    with pytest.raises(fence.FenceError):
        # writer_epoch 必须恰是 2
        fence.validate_required_policy_for_capability(_state(2), "login", 1, ())


def test_policy_v2_staging_is_hub_allocator_only() -> None:
    """★ V2 下预置 V3 feature **只对 hub_allocator** 放行,且必须精确。

    这是唯一一条"不可变的下一策略"的过渡口子:它让候选 hub writer 能以 V2 注册,
    好让 V2→V3 激活审计到它。放宽成"谁都能带未来 feature"就等于策略形同虚设。

    ★ 变异:把 `service == "hub_allocator"` 这个条件删掉 → ds_allocator 那条红。
    """
    hub_v3 = fence.REQUIRED_POLICY_V3_FEATURES["hub_allocator"]
    fence.validate_required_policy_for_capability(_state(2), "hub_allocator", 2, hub_v3)
    with pytest.raises(fence.FenceError):
        fence.validate_required_policy_for_capability(
            _state(2), "ds_allocator", 2, [*fence.REQUIRED_POLICY_V3_FEATURES["ds_allocator"], "x-v1"]
        )
    with pytest.raises(fence.FenceError):
        # 精确:不能是“V3 减一个”。★ 这里删的必须是**V2 里也有**的那一个 ——
        # V3 = V2 + hub-successor-lease-v1,所以 `hub_v3[:-1]` 恰好等于 V2 集合,
        # 用它做假件根本造不出失败条件(本轮实际撑红过一次)。
        fence.validate_required_policy_for_capability(
            _state(2), "hub_allocator", 2, hub_v3[1:]
        )


def test_policy_v3_requires_exact_v3_features() -> None:
    """V3 下必须精确广播 V3 feature 集(没有"预置下一代"的口子)。

    ★ 变异:V3 分支的 `equal_feature_set` 换成子集判定 → 本条红。
    """
    hub_v3 = fence.REQUIRED_POLICY_V3_FEATURES["hub_allocator"]
    fence.validate_required_policy_for_capability(_state(3), "hub_allocator", 2, hub_v3)
    with pytest.raises(fence.FenceError):
        fence.validate_required_policy_for_capability(
            _state(3), "hub_allocator", 2, fence.REQUIRED_POLICY_V2_FEATURES["hub_allocator"]
        )


def test_policy_unknown_service_fails_closed() -> None:
    """不在生产 writer 策略里的服务一律拒 —— 不认识 ≠ 放行。

    ★ 变异:`REQUIRED_POLICY_V2_FEATURES.get(service)` 的 None 分支改成 `v2_features = ()`
      → 本条红。
    """
    with pytest.raises(fence.FenceError):
        fence.validate_required_policy_for_capability(_state(2), "matchmaker", 2, ())


def test_policy_unsupported_generation_fails_closed() -> None:
    """未知策略代必须 fail-closed(不是"当成 V1 放行")。

    ★ 变异:default 分支改成 `return` → 本条红。
    """
    bogus = fence.RequiredState(epoch=2, policy_generation=9, policy_id="x", raw_value="x")
    with pytest.raises(fence.FenceError):
        fence.validate_required_policy_for_capability(bogus, "login", 2, ())


# ══════════════════════════════════════════════════════════════════════════
# ⑤ Capability JSON —— Go 要 Unmarshal 它
# ══════════════════════════════════════════════════════════════════════════


def _full_capability() -> fence.Capability:
    return fence.Capability(
        service="hub_allocator",
        instance_uid="pod-uid-1",
        writer_epoch=2,
        supported_policy_generation=3,
        supported_policy_id=fence.REQUIRED_POLICY_V3,
        acquired_policy_generation=2,
        acquired_policy_id=fence.REQUIRED_POLICY_V2,
        image_digest=_GOOD_DIGEST,
        keyset_revision="ks-7",
        etcd_identity_revision="r3",
        started_at_ms=1_700_000_000_000,
        features=fence.REQUIRED_POLICY_V3_FEATURES["hub_allocator"],
    )


def test_capability_json_field_names_and_order_match_go(fence_go: str) -> None:
    """★ JSON 字段名 + **顺序** 对拍 Go struct tag。

    名字错 = Go 的激活审计读到零值(不报错);顺序不影响 Unmarshal,但顺序漂了通常
    意味着有人在两边各改了一半,所以一起钉住。

    ★ 变异:把 `image_digest` 写成 `"imageDigest"` → 本条红。
    """
    tags = _go_capability_json_tags(fence_go)
    emitted = list(json.loads(_full_capability().to_json_bytes()).keys())
    assert emitted == [name for name, _ in tags]


def test_capability_json_omitempty_matches_go(fence_go: str) -> None:
    """★ `omitempty` 语义对拍:该省的省、不该省的必须出现(哪怕是零值)。

    Go 的 `started_at_ms` **没有** omitempty —— 漏掉它等于漏掉审计时间戳,
    而 Unmarshal 侧只会看到 0,分不清"没写"和"写了 0"。

    ★ 变异:给 `started_at_ms` 也加上 `if self.started_at_ms:` 条件 → 本条红。
    """
    tags = dict(_go_capability_json_tags(fence_go))
    empty = fence.Capability(service="login", instance_uid="u", image_digest=_GOOD_DIGEST)
    keys = set(json.loads(empty.to_json_bytes()).keys())
    for name, omitempty in tags.items():
        if omitempty:
            assert name not in keys, f"{name} 标了 omitempty 却在零值时被写出"
        else:
            assert name in keys, f"{name} 没标 omitempty 却在零值时被省略"


def test_capability_json_round_trip() -> None:
    """写出去再读回来必须逐字段相等(features 用元组,避免可变默认值共享)。

    ★ 变异:`from_json` 里把 `features` 恒返回 `()` → 本条红。
    """
    original = _full_capability()
    assert fence.Capability.from_json(original.to_json_bytes()) == original


def test_capability_json_no_ascii_escaping_of_payload() -> None:
    """输出必须是紧凑无空白的(与 Go `json.Marshal` 同形),便于逐字节比对与审计。

    ★ 变异:`separators` 去掉(默认带空格)→ 本条红。
    """
    raw = _full_capability().to_json_bytes()
    assert b", " not in raw and b'": ' not in raw


@pytest.mark.parametrize(
    "payload",
    [
        b"not json",
        b"[]",
        b'{"writer_epoch":"2"}',
        b'{"writer_epoch":true}',
        b'{"writer_epoch":-1}',
        b'{"writer_epoch":4294967296}',
        b'{"features":"a"}',
        b'{"features":[1]}',
        b'{"service":123}',
    ],
)
def test_capability_from_json_rejects_wrong_types(payload: bytes) -> None:
    """★ 类型不符必须**报错**,不能悄悄取零值。

    取零值会让"记录被污染"长得像"字段没写",而同 Pod 接管判定正是靠这些字段逐条相等 ——
    污染成零值 + 配置也是零值 = 接管通过 = 双 writer。

    ★ 变异:`_json_uint32` 的类型判定改成 `return int(value or 0)` → 多条红。
    ★ 变异:去掉 `isinstance(value, bool)` 排除 → `true` 那条红(bool 是 int 子类)。
    """
    with pytest.raises(fence.FenceError):
        fence.Capability.from_json(payload)


def test_capability_from_json_ignores_unknown_fields() -> None:
    """未知字段忽略(对齐 `json.Unmarshal`)—— 否则滚更期新版写的新字段会让旧版拒读。

    ★ 变异:在 `from_json` 里加"未知键即报错" → 本条红。
    """
    cap = fence.Capability.from_json(b'{"service":"login","future_field":42}')
    assert cap.service == "login"


# ══════════════════════════════════════════════════════════════════════════
# ⑥ 同 Pod 安全接管 —— 放宽任何一条都会造出第二个 writer
# ══════════════════════════════════════════════════════════════════════════


def _cfg(**over: object) -> fence.Config:
    base: dict[str, object] = {
        "endpoints": ["127.0.0.1:2379"],
        "prefix": fence.DEFAULT_PREFIX,
        "service": "login",
        "instance_uid": "pod-uid-1",
        "image_digest": _GOOD_DIGEST,
        "keyset_revision": "ks-1",
        "writer_epoch": 2,
        "lease_ttl_sec": 15,
        "dial_timeout_sec": 0.5,
        "features": (),
    }
    base.update(over)
    return fence.Config(**base)  # type: ignore[arg-type]


def _prev_capability(**over: object) -> bytes:
    cap = fence.Capability(
        service="login",
        instance_uid="pod-uid-1",
        writer_epoch=2,
        image_digest=_GOOD_DIGEST,
        keyset_revision="ks-1",
    )
    for key, value in over.items():
        setattr(cap, key, value)
    return cap.to_json_bytes()


def test_takeover_accepts_identical_identity() -> None:
    """同 Pod、同镜像、同 epoch、同 keyset → 允许接管(消除等旧租约 TTL 的空窗)。

    ★ 变异:`validate_same_pod_takeover` 无条件 raise → 本条红。
    """
    fence.validate_same_pod_takeover(_prev_capability(), _cfg())


@pytest.mark.parametrize(
    "over",
    [
        {"service": "player_locator"},
        {"instance_uid": "pod-uid-2"},
        {"image_digest": "sha256:" + "b" * 64},
        {"writer_epoch": 1},
        {"keyset_revision": "ks-2"},
    ],
)
def test_takeover_refuses_on_any_identity_drift(over: dict[str, object]) -> None:
    """★ 任一字段不一致一律拒绝接管并 fail-closed(等旧租约自然过期或人工介入)。

    异 PodUID 接管 = 直接造出第二个 writer;镜像 digest 变了 = 不是"同一个 Pod 的上一个
    进程",不能推定旧进程已退出。

    ★ 变异:去掉 image_digest 那条判定 → 对应参数化用例红。
    ★ 变异:`_same_secret` 恒返回 True → 前三条红。
    """
    with pytest.raises(fence.FenceError):
        fence.validate_same_pod_takeover(_prev_capability(**over), _cfg())


def test_takeover_refuses_unparsable_stale_record() -> None:
    """残留记录读不懂 → 拒绝接管(不是"读不懂就当没有")。

    ★ 变异:`from_json` 解析失败时返回空 `Capability()` → 本条红。
    """
    with pytest.raises(fence.FenceError):
        fence.validate_same_pod_takeover(b"{oops", _cfg())


# ══════════════════════════════════════════════════════════════════════════
# ⑦ 激活策略 / 摘要(供 dsauthfence_activate 复用的公开面)
# ══════════════════════════════════════════════════════════════════════════


def _activation_inputs(generation: int) -> tuple[dict[str, int], dict[str, set[str]]]:
    policy = fence.required_features_for_policy_generation(generation)
    return ({name: 1 for name in policy}, {name: set(feats) for name, feats in policy.items()})


def test_validate_activation_policy_generation_exact() -> None:
    """激活工具必须与运行期用**同一份**固定策略。

    ★ 变异:`len(services) != len(expected_policy)` 判定删掉 → "多一个服务"那条红。
    """
    for generation in (2, 3):
        services, features = _activation_inputs(generation)
        fence.validate_activation_policy_generation(generation, services, features)

    services, features = _activation_inputs(2)
    services["matchmaker"] = 1
    with pytest.raises(fence.FenceError):
        fence.validate_activation_policy_generation(2, services, features)


def test_validate_activation_policy_v3_requires_single_hub_writer() -> None:
    """★ V3 要求**恰好一个** hub_allocator writer(继任租约的前提)。

    ★ 变异:`!= 1` 改成 `< 1` → 本条红。
    """
    services, features = _activation_inputs(3)
    services["hub_allocator"] = 2
    with pytest.raises(fence.FenceError):
        fence.validate_activation_policy_generation(3, services, features)


def test_validate_activation_policy_rejects_unknown_epoch() -> None:
    """`validate_activation_policy` 只接受 epoch 2,且转发到 V2 代。

    ★ 变异:`epoch != PROTOCOL_EPOCH_V2` 判定删掉 → 本条红。
    """
    services, features = _activation_inputs(2)
    fence.validate_activation_policy(2, services, features)
    with pytest.raises(fence.FenceError):
        fence.validate_activation_policy(3, services, features)


def test_expected_services_hash_format_is_pinned() -> None:
    """★ 摘要格式钉死(排序 + "k=v\\n" 拼接 + sha256 hex)。

    这条摘要会进 activation record 供审计,两栈算出不同值 = 审计对不上。
    期望值是独立算出的字面量,不是"用同一段代码再算一遍"。

    ★ 变异:拼接符 `"\\n"` 改成 `";"`,或 `sorted` 去掉 → 本条红。
    """
    services = {
        "login": 3,
        "player_locator": 1,
        "ds_allocator": 2,
        "hub_allocator": 1,
        "battle_result": 1,
    }
    assert fence.expected_services_hash(services) == (
        "f9672991ac15c96eda513272c35d08c3c2311f9d6d8da15139b648da6b9ccb8f"
    )


def test_expected_services_hash_rejects_non_ascii_service() -> None:
    """非 ASCII 服务名会让 Go 的 `sort.Strings`(字节序)与 Python 的码点序分叉 → 直接拒。

    ★ 变异:去掉 `isascii()` 判定 → 本条红。
    """
    with pytest.raises(fence.FenceError):
        fence.expected_services_hash({"登录": 1})


# ══════════════════════════════════════════════════════════════════════════
# ⑧ Holder:注册 / watch / 失效 —— 用确定性 fake backend 覆盖每条 fail-closed 分支
# ══════════════════════════════════════════════════════════════════════════


class _FakeLease:
    """确定性租约。`drop()` 模拟续租循环判定失租。"""

    def __init__(self) -> None:
        self._lost = asyncio.Event()
        self._holding = True
        self.closed = False

    @property
    def lost(self) -> asyncio.Event:
        return self._lost

    def holding(self) -> bool:
        return self._holding and not self._lost.is_set()

    async def close(self) -> None:
        self.closed = True

    def drop(self) -> None:
        self._holding = False
        self._lost.set()


class _FakeBackend:
    """确定性 backend。watch 事件用完后按 `watch_ends` 决定是"结束"还是"挂住"。"""

    def __init__(
        self,
        *,
        required: fence.RequiredRead | None = None,
        required_exc: BaseException | None = None,
        required_hangs: bool = False,
        capability: fence.CapabilityRead | None = None,
        events: list[fence.RequiredEvent] | None = None,
        watch_ends: bool = False,
        acquire_exc: BaseException | None = None,
    ) -> None:
        self._required = required or fence.RequiredRead(_state(2), 100, 90, True)
        self._required_exc = required_exc
        self._required_hangs = required_hangs
        self._capability = capability or fence.CapabilityRead(b"", 0, 0, False)
        self._events = list(events or [])
        self._watch_ends = watch_ends
        self._acquire_exc = acquire_exc
        self.lease = _FakeLease()
        self.acquire_calls: list[tuple] = []
        self.watch_calls: list[tuple[str, int]] = []
        self.closed = False

    async def get_required(self, key: str) -> fence.RequiredRead:
        if self._required_hangs:
            await asyncio.Event().wait()
        if self._required_exc is not None:
            raise self._required_exc
        return self._required

    async def get_capability(self, key: str) -> fence.CapabilityRead:
        return self._capability

    async def acquire_capability(self, *args: object) -> fence.Lease:
        self.acquire_calls.append(args)
        if self._acquire_exc is not None:
            raise self._acquire_exc
        return self.lease

    async def watch_required(self, key: str, revision: int) -> AsyncIterator[fence.RequiredEvent]:
        self.watch_calls.append((key, revision))
        for event in self._events:
            yield event
        if not self._watch_ends:
            await asyncio.Event().wait()

    async def close(self) -> None:
        self.closed = True


async def _lost_reason(holder: fence.Holder, timeout: float = 2.0) -> str:
    await asyncio.wait_for(holder.lost.wait(), timeout=timeout)
    return holder.lost_reason()


async def test_start_registers_capability_with_required_cas_binding() -> None:
    """★ capability 注册必须与**本次线性读到的** required 值 + modRevision 绑定同一事务。

    绑定断了,「读 required → 注册」之间的激活 / 回退就成了 TOCTOU:本进程会以
    已经作废的旧策略拿到 capability。

    ★ 变异:`_acquire_with_same_pod_takeover` 传 `0` 而不是 `required_mod_revision`
      → 本条红。
    ★ 变异:传 `REQUIRED_VALUE_V2` 字面量而不是读到的 `raw_value` → V3 场景红。
    """
    read = fence.RequiredRead(_state(3), 100, 90, True)
    backend = _FakeBackend(required=read)
    cfg = _cfg(service="hub_allocator", features=fence.REQUIRED_POLICY_V3_FEATURES["hub_allocator"])
    holder = await fence.start(backend, cfg)
    try:
        (key, lock_key, req_key, raw_value, mod_rev, payload, ttl, prev_mod, prev_lease) = (
            backend.acquire_calls[0]
        )
        assert key == fence.capability_key(cfg.prefix, "hub_allocator", "pod-uid-1")
        assert lock_key == fence.activation_lock_key(cfg.prefix)
        assert req_key == fence.required_key(cfg.prefix)
        assert raw_value == fence.REQUIRED_VALUE_V3
        assert mod_rev == 90
        assert ttl == 15
        assert (prev_mod, prev_lease) == (0, 0)
        cap = fence.Capability.from_json(payload)
        assert cap.acquired_policy_generation == 3
        assert cap.supported_policy_generation == fence.REQUIRED_POLICY_GENERATION_V3
        assert holder.required_epoch() == 2
        assert holder.required_policy_generation() == 3
        assert holder.reclaimed is False
        assert holder.holding() is True
    finally:
        await holder.close()


async def test_start_watches_from_read_revision_plus_one() -> None:
    """★ watch 必须从 `读到的 revision + 1` 起 —— 不 +1 会重放已处理的那条,
    被当成"revision 未推进"而误判回退;起点更靠后则会**漏掉**真正的推进事件。

    ★ 变异:`read.watch_revision + 1` 改成 `read.watch_revision` → 本条红。
    """
    backend = _FakeBackend(required=fence.RequiredRead(_state(2), 100, 90, True))
    holder = await fence.start(backend, _cfg())
    try:
        # watch 是异步生成器:函数体到**首次迭代**才跑(真实现同理,etcd 的 watch 流
        # 也是监视任务开始迭代时才建)。所以这里等一个调度点,而不是断言“返回即已建流”。
        for _ in range(50):
            if backend.watch_calls:
                break
            await asyncio.sleep(0.01)
        assert backend.watch_calls == [(fence.required_key(fence.DEFAULT_PREFIX), 101)]
    finally:
        await holder.close()


async def test_start_fails_closed_when_required_missing() -> None:
    """required 键不存在 = 权威策略不可证明 → 拒绝启动(**不是**当成 epoch 1)。

    ★ 变异:`not read.found` 分支改成 `read = RequiredRead(_state(1), 0, 0, True)`
      → 本条红。
    """
    backend = _FakeBackend(required=fence.RequiredRead(fence.RequiredState(), 100, 0, False))
    with pytest.raises(fence.FenceError, match="explicit bootstrap"):
        await fence.start(backend, _cfg())
    assert backend.closed, "启动失败必须关掉 backend,不能泄漏连接"


async def test_start_fails_closed_when_required_exceeds_supported() -> None:
    """required epoch 高于本二进制支持的 epoch → 拒绝启动(这正是滚更的前向栅栏)。

    ★ 变异:`read.state.epoch > cfg.writer_epoch` 改成 `>=` 或删掉 → 本条红。
    """
    backend = _FakeBackend(required=fence.RequiredRead(_state(2), 100, 90, True))
    with pytest.raises(fence.FenceError, match="exceeds supported"):
        await fence.start(backend, _cfg(writer_epoch=1))


async def test_start_fails_closed_on_policy_mismatch() -> None:
    """feature 集合与 required 策略不符 → 拒绝启动。

    ★ 变异:`start` 里去掉 `validate_required_policy_for_capability` 调用 → 本条红。
    """
    backend = _FakeBackend(required=fence.RequiredRead(_state(3), 100, 90, True))
    with pytest.raises(fence.FenceError):
        await fence.start(backend, _cfg(service="hub_allocator", features=()))


async def test_start_read_has_bounded_timeout() -> None:
    """★ etcd 读必须有**有界超时**(§9 不变量 19/20):卡住不能变成启动期永久挂起。

    ★ 变异:去掉 `asyncio.wait_for(backend.get_required(...))` 的包裹 →
      本条超时(3s)后红,且失败形状是 TimeoutError 而不是 FenceError。
    """
    backend = _FakeBackend(required_hangs=True)
    with pytest.raises(fence.FenceError):
        await asyncio.wait_for(fence.start(backend, _cfg(dial_timeout_sec=0.2)), timeout=3.0)


async def test_lease_loss_signals_keepalive_ended_and_blocks_writes() -> None:
    """★ 失租 → `lost` 置位 + `holding()` 转假 + `require_holding()` 抛。

    这是"续租失败必须立刻自 fencing"的调用点入口:只置位不提供断言入口,
    调用方就得靠纪律记得检查,而漏检**没有任何信号**。

    ★ 变异:`Holder.holding` 改成 `return not self._lost.is_set()` → 不红(等价);
      改成 `return True` → 本条红。
    ★ 变异:`_monitor_lease` 里去掉 `_signal_lost` → 本条红(超时)。
    """
    backend = _FakeBackend()
    holder = await fence.start(backend, _cfg())
    try:
        assert holder.holding() is True
        holder.require_holding()
        backend.lease.drop()
        assert await _lost_reason(holder) == fence.LOST_REASON_LEASE_KEEPALIVE_ENDED
        assert holder.holding() is False
        with pytest.raises(fence.CapabilityLostError) as caught:
            holder.require_holding()
        # 证据必须挂在**声明式 slots** 字段上(不是 setattr),否则拼错不报错、读侧恒空。
        assert caught.value.lost_reason == fence.LOST_REASON_LEASE_KEEPALIVE_ENDED
        assert caught.value.code == errcode.ErrInvalidState
    finally:
        await holder.close()


async def test_holder_holding_follows_lease_local_safety_window() -> None:
    """★ `holding()` 必须同时看**租约本地安全窗**,不能只看 lost 事件。

    只看事件会漏掉"续租已连续失败、循环还没来得及置位"的那一段 ——
    而那正是双写者最可能发生的窗口。

    ★ 变异:`Holder.holding` 去掉 `and self._lease.holding()` → 本条红。
    """
    backend = _FakeBackend()
    holder = await fence.start(backend, _cfg())
    try:
        backend.lease._holding = False  # 安全窗越线,但尚未置位 lost
        assert holder.lost.is_set() is False
        assert holder.holding() is False
    finally:
        await holder.close()


async def test_required_advance_signals_restart() -> None:
    """★ required 正常推进也必须 `lost`(按契约强制重启,以新 raw value 重新 CAS 注册)。

    沿用旧租约继续跑会让审计含糊:capability 声称的 acquired policy 与现行 required 不符。

    ★ 变异:推进分支改成 `continue` → 本条红(超时)。
    """
    backend = _FakeBackend(events=[fence.RequiredEvent(state=_state(3), revision=200)])
    holder = await fence.start(backend, _cfg())
    try:
        assert await _lost_reason(holder) == fence.LOST_REASON_REQUIRED_ADVANCED
        # 高水位只增不减:推进后必须已记录新值(供退出日志取证)。
        assert holder.required_policy_generation() == 3
    finally:
        await holder.close()


async def test_required_delete_signals_deleted() -> None:
    """required 被删 / 值为空 → 权威策略不可证明 → 立即失效。

    ★ 变异:`event.deleted` 分支改成 `continue` → 本条红。
    """
    backend = _FakeBackend(events=[fence.RequiredEvent(revision=200, deleted=True)])
    holder = await fence.start(backend, _cfg())
    try:
        assert await _lost_reason(holder) == fence.LOST_REASON_REQUIRED_DELETED
    finally:
        await holder.close()


@pytest.mark.parametrize(
    ("label", "event"),
    [
        # revision 未推进(watch 重放 / 乱序)
        ("stale_revision", fence.RequiredEvent(state=_state(3), revision=90)),
        # 策略代回退 V2→V2(不严格递增)
        ("same_generation", fence.RequiredEvent(state=_state(2), revision=200)),
    ],
)
async def test_required_regression_signals_regressed(
    label: str, event: fence.RequiredEvent
) -> None:
    """★ revision / epoch / 策略代任一不严格前进 → 判回退,立即失效。

    回退是最危险的一种:它意味着有人把 required 改回了旧策略,而旧策略下可能存在
    本进程不该共存的 writer。

    ★ 变异:`event.state.policy_generation <= seen_policy_generation` 改成 `<`
      → same_generation 那条红。
    ★ 变异:`event.revision <= seen_revision` 改成 `< `→ stale_revision 那条红。
    """
    backend = _FakeBackend(events=[event])
    holder = await fence.start(backend, _cfg())
    try:
        assert await _lost_reason(holder) == fence.LOST_REASON_REQUIRED_REGRESSED, label
    finally:
        await holder.close()


async def test_watch_error_signals_watch_error() -> None:
    """★ watch 报错(含 compact revision)必须独立成一格,不能与"通道结束"混同。

    2026-07-29 的取证卡点正是"五个分支同形";合并任意两格,下次事故又分不出来。

    ★ 变异:`event.err is not None` 分支改成 `signal(LOST_REASON_REQUIRED_WATCH_CLOSED)`
      → 本条红。
    """
    backend = _FakeBackend(events=[fence.RequiredEvent(err=RuntimeError("compacted"))])
    holder = await fence.start(backend, _cfg())
    try:
        assert await _lost_reason(holder) == fence.LOST_REASON_REQUIRED_WATCH_ERROR
    finally:
        await holder.close()


async def test_watch_silent_close_signals_watch_closed() -> None:
    """★ watch 静默结束也不能继续写 —— 不以重连 / 旧缓存冒充授权。

    ★ 变异:`_monitor_required` 循环结束后的 `_signal_lost` 删掉 → 本条红(超时)。
    """
    backend = _FakeBackend(events=[], watch_ends=True)
    holder = await fence.start(backend, _cfg())
    try:
        assert await _lost_reason(holder) == fence.LOST_REASON_REQUIRED_WATCH_CLOSED
    finally:
        await holder.close()


async def test_intentional_close_does_not_signal_lost() -> None:
    """主动关闭是优雅下线,不是失租 —— 置位 lost 会让每次正常滚更都产生假告警。

    ★ 变异:`Holder.close` 里去掉 `self._intentional = True` → 本条红
      (watch 生成器被取消 / 结束后会走到 watch_closed 分支)。
    """
    backend = _FakeBackend(events=[], watch_ends=True)
    holder = await fence.start(backend, _cfg())
    await holder.close()
    await asyncio.sleep(0.05)
    assert holder.lost.is_set() is False
    assert holder.lost_reason() == ""
    assert backend.lease.closed and backend.closed
    await holder.close()  # 幂等


async def test_same_pod_takeover_passes_prev_mod_revision_and_lease() -> None:
    """★ 残留同身份 capability → 以 **ModRevision 精确 CAS** 接管,并带上旧 leaseID 去 revoke。

    传 0 会退化成"要求 key 不存在" → 永远注册不上(CrashLoop);
    不传旧 leaseID 则旧进程的续租不会被终结 → 理论上仍存活的旧进程不会立刻退出。

    ★ 变异:接管分支恒传 `(0, 0)` → 本条红。
    """
    backend = _FakeBackend(capability=fence.CapabilityRead(_prev_capability(), 77, 4242, True))
    holder = await fence.start(backend, _cfg())
    try:
        *_, prev_mod, prev_lease = backend.acquire_calls[0]
        assert (prev_mod, prev_lease) == (77, 4242)
        assert holder.reclaimed is True
    finally:
        await holder.close()


async def test_takeover_identity_mismatch_aborts_without_retry() -> None:
    """★ 身份不符必须**立即**失败,不进第二次尝试:重试解决不了身份冲突。

    ★ 变异:把 `validate_same_pod_takeover` 的异常吞掉改成 `prev_mod=0` → 本条红。
    """
    stale = _prev_capability(instance_uid="pod-uid-OTHER")
    backend = _FakeBackend(capability=fence.CapabilityRead(stale, 77, 4242, True))
    with pytest.raises(fence.FenceError, match="refuse takeover"):
        await fence.start(backend, _cfg())
    assert backend.acquire_calls == [], "身份不符时不得尝试注册"


async def test_acquire_retries_once_on_registration_race() -> None:
    """注册失败重试**恰好一次**(覆盖"预检与 Txn 之间残留租约刚好过期"的窄竞态)。

    次数不能更多:那会把一次容器级退避拖成长时间无声重试。

    ★ 变异:`range(2)` 改成 `range(1)` → 第一条断言红;改成 `range(5)` → 第二条红。
    """
    backend = _FakeBackend(acquire_exc=fence.CapabilityFencedError("k"))
    with pytest.raises(fence.FenceError):
        await fence.start(backend, _cfg())
    assert len(backend.acquire_calls) == 2


async def test_capability_fenced_error_carries_key() -> None:
    """fencing 失败必须带上 capability key(五条 fail-closed 路径在日志里同形)。

    ★ 变异:`CapabilityFencedError` 的 `capability_key` 改成 `setattr` 写法 → 不红,
      但把字段名拼错成 `capabilty_key` → 本条红(而 setattr 版本拼错不会报错)。
    """
    err = fence.CapabilityFencedError("/pandora/ds-auth/capabilities/login/u")
    assert err.capability_key == "/pandora/ds-auth/capabilities/login/u"
    assert err.code == errcode.ErrInvalidState


# ══════════════════════════════════════════════════════════════════════════
# ⑨ 续租循环 —— aetcd 没有自动 KeepAlive,这里是整条移植最容易脑裂的一处
# ══════════════════════════════════════════════════════════════════════════


class _FakeEtcdLeaseHandle:
    """`refresh()` 的可编程假件。`ttl` 为 0 模拟 etcd 说"这个 lease 不存在"。"""

    def __init__(self, *, ttl: int = 15, exc: BaseException | None = None, delay: float = 0.0) -> None:
        self.id = 0x1234
        self._ttl = ttl
        self._exc = exc
        self._delay = delay
        self.calls = 0
        self.successes = 0

    async def refresh(self) -> object:
        self.calls += 1
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._exc is not None:
            raise self._exc
        self.successes += 1
        return type("Resp", (), {"TTL": self._ttl})()


class _FakeEtcdClient:
    def __init__(self) -> None:
        self.revoked: list[int] = []

    async def revoke_lease(self, lease_id: int) -> None:
        self.revoked.append(lease_id)


async def test_keepalive_lease_gone_is_evidence_not_a_blip() -> None:
    """★ 服务端回 TTL<=0 = **已确定失主**,必须立刻放弃,不得等本地安全窗走完。

    这是 `pandorapy/etcdlease.py` 记录的实测坑:etcd 对已不存在的 lease 的 keepalive
    **不报错**,只把 TTL 置 0。把它当"没抛异常 = 续上了"会让本副本永远认为自己还持有。
    本条把安全窗设到 30s 之后,若实现是"等窗口过期才放弃",30s 内不会置位 → 红。

    ★ 变异:`_keepalive_loop` 里把 `except LeaseGoneError` 分支并进通用失败分支
      (走安全窗重试)→ 本条红(2s 超时)。
    """
    handle = _FakeEtcdLeaseHandle(ttl=0)
    lease = fence._EtcdLease(_FakeEtcdClient(), handle, 1)
    lease._safe_deadline = time.monotonic() + 30.0  # 安全窗远未到期
    lease.start_keepalive()
    try:
        await asyncio.wait_for(lease.lost.wait(), timeout=2.0)
        assert lease.holding() is False
        assert handle.calls == 1
    finally:
        await lease.close()


async def test_keepalive_uncertain_refresh_is_treated_as_failure() -> None:
    """★ 续租"不确定"(超时 / UNKNOWN / 连接断)必须按**失败**处理,禁止乐观当成功。

    安全窗内可以重试(etcd 短抖动很常见),越线必须放弃 —— 此时无法证明 lease 仍有效。
    本条同时钉住两侧:窗口内仍持有(不过早让位),越线后必失(不无限乐观)。

    ★ 变异:`except BaseException` 分支改成 `continue`(永不放弃)→ 第二段红。
    ★ 变异:该分支改成立刻 `_declare_lost`(不给重试)→ 第一段红。
    """
    handle = _FakeEtcdLeaseHandle(exc=TimeoutError("etcd unreachable"))
    lease = fence._EtcdLease(_FakeEtcdClient(), handle, 1)  # 窗口 = 1 - 1/3 ≈ 0.67s
    lease.start_keepalive()
    try:
        await asyncio.sleep(0.4)
        assert lease.holding() is True, "安全窗内的一次续租失败不该立刻让位"
        await asyncio.wait_for(lease.lost.wait(), timeout=3.0)
        assert lease.holding() is False
        assert handle.calls >= 1, "越线前必须真的尝试过续租"
    finally:
        await lease.close()


async def test_late_success_cannot_revive_self_fenced_lease() -> None:
    """★ 自 fencing 是**单调终态**:迟到的成功续租不得复活已让位的租约。

    复活 = 此刻可能已有接管者在写,本副本又开始写 = 第二个 writer。
    本条构造的正是那条时序:续租**成功**返回,但返回时本地安全窗已被观察到越线。

    ★ 变异:删掉续租成功后的 `if self._self_fenced:` 检查(直接推进 `_safe_deadline`)
      → 本条红(lost 永不置位)。
    """
    handle = _FakeEtcdLeaseHandle(ttl=15, delay=0.3)
    lease = fence._EtcdLease(_FakeEtcdClient(), handle, 3)  # interval=1.0,不会 wait_for 超时
    lease._safe_deadline = time.monotonic() + 0.1  # 首个 sleep 只睡到安全线
    lease.start_keepalive()
    try:
        await asyncio.sleep(0.25)
        assert lease.holding() is False  # 观察到越线 → 就地自 fencing
        await asyncio.wait_for(lease.lost.wait(), timeout=2.0)
        assert handle.successes == 1, "本条必须走「续租成功」那条路径,否则没测到目标分支"
    finally:
        await lease.close()


async def test_intentional_lease_close_revokes_and_stays_silent() -> None:
    """主动关闭:停续租 + revoke,且**不**置位 lost(优雅下线不是事故)。

    ★ 变异:`_declare_lost` 去掉 `if self._intentional: return` → 概率性红;
      `close()` 去掉 `revoke_lease` → 第二条断言红。
    """
    client = _FakeEtcdClient()
    handle = _FakeEtcdLeaseHandle()
    lease = fence._EtcdLease(client, handle, 15)
    lease.start_keepalive()
    await lease.close()
    await lease.close()  # 幂等
    assert lease.lost.is_set() is False
    assert client.revoked == [handle.id]


def test_local_safety_deadline_uses_monotonic_clock() -> None:
    """★ 本地安全截止必须走**单调钟**;墙钟回拨会让安全窗静默失效。

    失效的方向恰好是最危险的那个:"本地以为还持有"。
    第一段用源码扫描钉住 `time.time()` 只出现在审计字段那一处;
    第二段验证墙钟被拨动时 `holding()` 不受影响。

    ★ 变异:`_EtcdLease.__init__` 的 `time.monotonic()` 改成 `time.time()` → 第一段红。
    """
    source = pathlib.Path(fence.__file__).read_text(encoding="utf-8")
    # ★ 用 AST 而不是字串扫描:模块文档里就写着“禁止 `time.time()`”,正则/包含判定
    # 会把那句话当成违规 —— 误报的检查最终会被整条删掉,连它本来能抓的真缺陷一起没了。
    calls = [
        node.lineno
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "time"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "time"
    ]
    lines = source.splitlines()
    assert len(calls) == 1, f"time.time() 只允许出现一处(审计字段);实际行号:{calls}"
    assert "started_at_ms" in lines[calls[0] - 1], (
        f"time.time() 只允许用在 capability 审计字段;实际:{lines[calls[0] - 1].strip()}"
    )
    assert "time.monotonic()" in source

    lease = fence._EtcdLease(_FakeEtcdClient(), _FakeEtcdLeaseHandle(), 15)
    assert lease.holding() is True
    lease._safe_deadline = time.monotonic() - 0.001
    assert lease.holding() is False
    # 单调终态:把截止线推回未来也不能复活。
    lease._safe_deadline = time.monotonic() + 100.0
    assert lease.holding() is False


def test_keepalive_and_margin_divisors_match_repo_convention() -> None:
    """续租节奏 TTL/3、安全余量 TTL/3(与 etcdleader / snowflake_etcd 同一口径)。

    余量太小 = 本地窗口逼近服务端到期 → 兑现不了 §9.22 的
    `旧持有者最晚停止 < 新持有者最早开始`。

    ★ 变异:`_SAFETY_MARGIN_DIVISOR` 改成 30(余量 0.5s)→ 本条红。
    """
    assert fence._KEEPALIVE_DIVISOR == 3
    assert fence._SAFETY_MARGIN_DIVISOR == 3
    lease = fence._EtcdLease(_FakeEtcdClient(), _FakeEtcdLeaseHandle(), 15)
    window = lease._safe_deadline - time.monotonic()
    assert 9.0 < window <= 10.0, "TTL=15s 时本地可持有窗口应为 10s"


# ══════════════════════════════════════════════════════════════════════════
# ⑩ 安全姿态(security.go)—— 不允许"以为安全"的静默明文
# ══════════════════════════════════════════════════════════════════════════


def test_env_constant_names_match_go(security_go: str) -> None:
    """★ 环境变量名对拍 —— 名字漂了 = 生产注入的安全配置被静默忽略。

    ★ 变异:`ENV_ETCD_REQUIRE_MTLS` 改成 `..._REQUIRE_TLS` → 本条红。
    """
    pairs = {
        "EnvEtcdRequireMTLS": fence.ENV_ETCD_REQUIRE_MTLS,
        "EnvEtcdCAFile": fence.ENV_ETCD_CA_FILE,
        "EnvEtcdCertFile": fence.ENV_ETCD_CERT_FILE,
        "EnvEtcdKeyFile": fence.ENV_ETCD_KEY_FILE,
        "EnvEtcdServerName": fence.ENV_ETCD_SERVER_NAME,
        "EnvEtcdClientIdentity": fence.ENV_ETCD_CLIENT_IDENTITY,
        "EnvEtcdIdentityRevision": fence.ENV_ETCD_IDENTITY_REVISION,
        "EnvEtcdUsernameFile": fence.ENV_ETCD_USERNAME_FILE,
        "EnvEtcdPasswordFile": fence.ENV_ETCD_PASSWORD_FILE,
        "EnvEtcdRequireAuth": fence.ENV_ETCD_REQUIRE_AUTH,
        "EnvEtcdForbiddenReadPrefix": fence.ENV_ETCD_FORBIDDEN_READ_PREFIX,
    }
    for go_name, py_value in pairs.items():
        assert py_value == _go_const_str(security_go, go_name), go_name
    # Downward API 注入的两个不可伪造身份。
    assert fence.ENV_POD_UID == "PANDORA_POD_UID"
    assert fence.ENV_IMAGE_DIGEST == "PANDORA_IMAGE_DIGEST"


@pytest.mark.parametrize("value", ["true", "TRUE", "yes", "on", "2", " 1 x"])
def test_strict_bool_env_rejects_anything_but_0_1(monkeypatch, value: str) -> None:
    """★ 只接受 "" / "0" / "1"。

    `bool(os.getenv(...))` 会把 "false" 当真、把 "0" 当真 —— 一个把安全开关拧反、
    一个把它默默打开,两个方向都错。宁可启动期报错。

    ★ 变异:`_strict_bool_env` 改成 `return bool(value)` → 本条红。
    """
    monkeypatch.setenv(fence.ENV_ETCD_REQUIRE_MTLS, value)
    with pytest.raises(fence.FenceError):
        fence.client_security_from_env()


def test_client_security_from_env_reads_paths_only(monkeypatch) -> None:
    """只读路径 / 开关,不读凭据内容;未设时是"未启用"的开发路径。

    ★ 变异:`client_security_from_env` 里去掉 `.strip()` → 带空格那条红。
    """
    for name in (
        fence.ENV_ETCD_REQUIRE_MTLS,
        fence.ENV_ETCD_REQUIRE_AUTH,
        fence.ENV_ETCD_CA_FILE,
        fence.ENV_ETCD_CERT_FILE,
        fence.ENV_ETCD_KEY_FILE,
        fence.ENV_ETCD_SERVER_NAME,
        fence.ENV_ETCD_CLIENT_IDENTITY,
        fence.ENV_ETCD_IDENTITY_REVISION,
        fence.ENV_ETCD_USERNAME_FILE,
        fence.ENV_ETCD_PASSWORD_FILE,
        fence.ENV_ETCD_FORBIDDEN_READ_PREFIX,
    ):
        monkeypatch.delenv(name, raising=False)
    assert fence.client_security_from_env().enabled() is False

    monkeypatch.setenv(fence.ENV_ETCD_CA_FILE, "  /etc/ca.pem  ")
    security = fence.client_security_from_env()
    assert security.ca_file == "/etc/ca.pem"
    assert security.enabled() is True


def _secure(**over: object) -> fence.ClientSecurity:
    base: dict[str, object] = {
        "require_mtls": True,
        "ca_file": "/etc/ca.pem",
        "cert_file": "/etc/tls.crt",
        "key_file": "/etc/tls.key",
        "server_name": "etcd.pandora.svc",
        "client_identity": "ds-auth-writer",
        "identity_revision": "r3",
    }
    base.update(over)
    return fence.ClientSecurity(**base)  # type: ignore[arg-type]


def test_secure_config_requires_mtls_and_every_field() -> None:
    """安全档缺任一必填项一律拒(不允许"半安全")。

    ★ 变异:`if not security.require_mtls` 判定删掉 → 第一条红。
    ★ 变异:必填项循环里少查 `server_name` → 对应那条红。
    """
    endpoints = ["https://etcd.pandora.svc:2379"]
    fence.validate_client_security(endpoints, fence.DEFAULT_PREFIX, _secure())
    with pytest.raises(fence.FenceError, match="requires mTLS"):
        fence.validate_client_security(endpoints, fence.DEFAULT_PREFIX, _secure(require_mtls=False))
    for missing in ("ca_file", "cert_file", "key_file", "server_name", "client_identity"):
        with pytest.raises(fence.FenceError, match="missing"):
            fence.validate_client_security(
                endpoints, fence.DEFAULT_PREFIX, _secure(**{missing: ""})
            )


def test_identity_revision_must_be_canonical() -> None:
    """身份 revision 必须是规范 `rN`(N 不以 0 开头)—— 它是审计里定位"用的哪份证书"的键。

    ★ 变异:正则改成 `\\Ar[0-9]+\\Z` → "r0" / "r01" 那条红。
    """
    endpoints = ["https://etcd.pandora.svc:2379"]
    for bad in ("3", "R3", "r0", "r01", "r3 ", ""):
        with pytest.raises(fence.FenceError):
            fence.validate_client_security(
                endpoints, fence.DEFAULT_PREFIX, _secure(identity_revision=bad)
            )


def test_credentials_must_be_configured_in_pairs() -> None:
    """用户名 / 口令文件必须成对 —— 只配一个会让认证以"匿名"悄悄降级。

    ★ 变异:`(username_file == "") != (password_file == "")` 改成 `and` → 本条红。
    """
    endpoints = ["https://etcd.pandora.svc:2379"]
    with pytest.raises(fence.FenceError, match="together"):
        fence.validate_client_security(
            endpoints, fence.DEFAULT_PREFIX, _secure(username_file="/etc/u")
        )
    fence.validate_client_security(
        endpoints,
        fence.DEFAULT_PREFIX,
        _secure(username_file="/etc/u", password_file="/etc/p"),
    )


def test_forbidden_read_prefix_must_not_overlap_allowed_prefix() -> None:
    """★ ACL 反向探针的前缀不能与本服务被允许的前缀重叠 —— 重叠了探针必然"失败",
    于是它证明的不是"越权被拒",而是"我自己配错了"。

    ★ 变异:去掉 `forbidden.startswith(allowed)` 那一判 → 第三条红。
    """
    endpoints = ["https://etcd.pandora.svc:2379"]
    fence.validate_client_security(
        endpoints, fence.DEFAULT_PREFIX, _secure(forbidden_read_prefix="/pandora/secrets/")
    )
    for overlapping in ("/pandora/ds-auth/", "/pandora/", "/pandora/ds-auth/capabilities/"):
        with pytest.raises(fence.FenceError, match="overlaps"):
            fence.validate_client_security(
                endpoints, fence.DEFAULT_PREFIX, _secure(forbidden_read_prefix=overlapping)
            )


def test_require_auth_needs_a_negative_probe_prefix() -> None:
    """要求 auth 却没有反向探针前缀 = 没有任何"最小权限"证据,只有一句声明。

    ★ 变异:该判定删掉 → 本条红。
    """
    with pytest.raises(fence.FenceError, match="forbidden read prefix"):
        fence.validate_client_security(
            ["https://etcd.pandora.svc:2379"], fence.DEFAULT_PREFIX, _secure(require_auth=True)
        )


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://etcd.pandora.svc:2379",  # 明文
        "https://etcd.pandora.svc",  # 没端口
        "https://etcd.pandora.svc:2379/v3",  # 带路径
        "https://etcd.pandora.svc:2379?a=b",  # 带 query
        "https://etcd.pandora.svc:2379#f",  # 带 fragment
        "https://user:pw@etcd.pandora.svc:2379",  # 带凭据
        "https://etcd.pandora.svc:02379",  # 非规范端口(同一端口、不同字节)
        "https://etcd.pandora.svc:0",  # 0 端口
        "https://etcd.pandora.svc:70000",  # 越界
        "https://:2379",  # 无 host
    ],
)
def test_production_endpoint_must_be_canonical_https(endpoint: str) -> None:
    """★ 生产端点必须是规范 `https://host:port`。

    "02379" 与 "2379" 指同一端口但字节不同 —— 运维比对的是字节,放行它等于给
    "两份看起来不同其实相同的配置"开门。

    ★ 变异:`str(port) != raw_port` 判定删掉 → "02379" 那条红。
    ★ 变异:`parsed.path != ""` 判定删掉 → "/v3" 那条红。
    """
    with pytest.raises(fence.FenceError):
        fence.validate_client_security([endpoint], fence.DEFAULT_PREFIX, _secure())


async def test_new_etcd_client_fails_closed_instead_of_silent_plaintext(monkeypatch) -> None:
    """★ 配了安全档但 aetcd 建不了 mTLS → **明确报错**,绝不悄悄退回明文。

    静默明文会让一个自以为有 mTLS + 最小权限的部署在毫无信号的情况下裸奔 ——
    正是本模块要防的那类失效。

    ★ 变异:`new_etcd_client` 里把 `raise SecureEtcdUnsupportedError(...)` 换成
      "打个 warning 然后继续建明文客户端" → 本条红。
    """
    tmp_calls: list[str] = []
    monkeypatch.setattr(fence, "load_tls_material", lambda security: tmp_calls.append("tls"))
    with pytest.raises(fence.SecureEtcdUnsupportedError):
        await fence.new_etcd_client(
            ["https://etcd.pandora.svc:2379"], 5.0, fence.DEFAULT_PREFIX, _secure()
        )
    assert tmp_calls == ["tls"], "拒绝之前必须已做过全量配置 + 证书校验(配置错要在启动期红)"


async def test_new_etcd_client_rejects_malformed_dev_endpoint() -> None:
    """开发档端点写错 → 报错,不能退回 `127.0.0.1:2379` 默认值。

    (`tests/etcdfixture.py` 记的正是这个坑:漏写端口被洗成"环境不可用",44 条静默跳过。)

    ★ 变异:`int(port)` 前的 isdigit 判定删掉 → 本条变成 ValueError 而不是 FenceError → 红。
    """
    with pytest.raises(fence.FenceError):
        await fence.new_etcd_client(["127.0.0.1"], 5.0, fence.DEFAULT_PREFIX, fence.ClientSecurity())


def test_read_credential_file_rejects_non_canonical(tmp_path: pathlib.Path) -> None:
    """凭据文件不得为空 / 带首尾空白 / 含 NUL、CR、LF。

    带尾换行的口令是 `echo secret > file` 的默认产物,而 etcd 会拿它原样去认证 →
    认证失败,现象是"密码明明对的"。

    ★ 变异:去掉 `value.strip() != value` 判定 → 尾换行那条红。
    """
    good = tmp_path / "u"
    good.write_bytes(b"writer")
    assert fence.read_credential_file(str(good), "username") == "writer"
    for bad_bytes in (b"", b"writer\n", b" writer", b"wri\x00ter"):
        bad = tmp_path / "bad"
        bad.write_bytes(bad_bytes)
        with pytest.raises(fence.FenceError):
            fence.read_credential_file(str(bad), "username")
    with pytest.raises(fence.FenceError, match="read etcd"):
        fence.read_credential_file(str(tmp_path / "missing"), "password")


# ══════════════════════════════════════════════════════════════════════════
# ⑪ 配置归一化 / 校验
# ══════════════════════════════════════════════════════════════════════════


def test_normalize_fills_defaults() -> None:
    """空值归一到默认(与 Go 的 normalize 一致);非法负值也走默认而不是照单全收。

    ★ 变异:`if cfg.lease_ttl_sec <= 0` 改成 `== 0` → 负 TTL 那条红。
    """
    cfg = fence.Config(endpoints=["h:1"], service="login", lease_ttl_sec=-5, dial_timeout_sec=-1)
    fence.normalize(cfg)
    assert cfg.prefix == fence.DEFAULT_PREFIX
    assert cfg.lease_ttl_sec == fence.DEFAULT_LEASE_TTL_SEC
    assert cfg.dial_timeout_sec == fence.DEFAULT_DIAL_TIMEOUT_SEC


def test_default_lease_ttl_matches_go(fence_go: str) -> None:
    """★ 默认租约 TTL 对拍 Go —— 它同时是恢复空窗的时间预算(§16.8)。

    ★ 变异:`DEFAULT_LEASE_TTL_SEC = 30` → 本条红。
    """
    m = re.search(r"DefaultLeaseTTLSec int64 = (\d+)", fence_go)
    assert m, "未能从 Go 源解析 DefaultLeaseTTLSec"
    assert fence.DEFAULT_LEASE_TTL_SEC == int(m.group(1))


@pytest.mark.parametrize(
    ("over", "hint"),
    [
        ({"endpoints": []}, "empty endpoints"),
        ({"service": ""}, "invalid service"),
        ({"service": "a/b"}, "invalid service"),
        ({"service": "matchmaker"}, "production writer policy"),
        ({"instance_uid": ""}, "invalid instance uid"),
        ({"instance_uid": "a/b"}, "invalid instance uid"),
        ({"writer_epoch": 0}, "writer epoch is zero"),
        ({"image_digest": "sha256:ABC"}, "image digest"),
        ({"image_digest": "latest"}, "image digest"),
        ({"keyset_revision": ""}, "keyset revision"),
        ({"features": ("BAD",)}, "capability feature"),
    ],
)
def test_validate_rejects_unusable_config(over: dict[str, object], hint: str) -> None:
    """★ 身份 / 策略字段的启动期闸。每一条都对应一种"能起来但是错的"部署。

    尤其 `image_digest`:回退到 image tag(`latest`)就等于放弃了"同 Pod 接管"的
    唯一前提 —— tag 可变,digest 不可变。

    ★ 变异:`validate` 里去掉 `DIGEST_PATTERN` 判定 → 两条 digest 用例红。
    ★ 变异:去掉 `cfg.service not in REQUIRED_POLICY_V2_FEATURES` 判定 → matchmaker 那条红。
    """
    with pytest.raises(fence.FenceError, match=hint):
        fence.validate(_cfg(**over))


def test_validate_accepts_production_shape() -> None:
    """五个生产 writer 服务 + 各自精确 feature 集必须全部通过校验。

    ★ 变异:把 `REQUIRED_POLICY_V2_FEATURES` 里任一服务名拼错 → 本条红。
    """
    for service, features in fence.REQUIRED_POLICY_V2_FEATURES.items():
        fence.validate(_cfg(service=service, features=features))


async def test_acquire_runtime_reads_identity_from_downward_api(monkeypatch) -> None:
    """★ 身份只从 Downward API 环境读,**绝不回退** hostname / image tag。

    回退可伪造 / 可漂移,而 capability key 的唯一性正建立在 PodUID 上。
    这里让环境为空,断言它以"instance uid 非法"失败,而不是悄悄用主机名。

    ★ 变异:`acquire_runtime` 里 `os.getenv(ENV_POD_UID, "")` 改成
      `os.getenv(ENV_POD_UID) or socket.gethostname()` → 本条红。
    """
    monkeypatch.delenv(fence.ENV_POD_UID, raising=False)
    monkeypatch.delenv(fence.ENV_IMAGE_DIGEST, raising=False)
    for name in (fence.ENV_ETCD_REQUIRE_MTLS, fence.ENV_ETCD_REQUIRE_AUTH):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(fence.FenceError, match="invalid instance uid"):
        await fence.acquire_runtime(
            fence.RuntimeConfig(endpoints=["127.0.0.1:2379"], service="login", writer_epoch=2,
                                keyset_revision="ks-1")
        )


# ══════════════════════════════════════════════════════════════════════════
# ⑫ 结构性纪律:后台循环必须走 safego 且带**静态**名字
# ══════════════════════════════════════════════════════════════════════════


async def test_background_loops_go_through_safego_with_static_names() -> None:
    """★ 后台循环必须走 `safego.spawn` 且名字是静态字面量。

    - 裸 `create_task`:异常躺在 Task 里没人取 → 那条监视循环已经死了,
      而进程照常 SERVING、全程零日志(见 pandorapy/safego.py 的实测)。
    - 名字拼进 service / pod uid:safego 把 name 当 Prometheus label,
      拼动态值 = 高基数标签(§12)。

    ★ 变异:`start()` 里把 `safego.spawn(...)` 换成 `asyncio.create_task(...)` → 本条红。
    ★ 变异:`_TASK_MONITOR_LEASE` 改成 f-string 拼 service → 第二段红。
    """
    # 只看代码：上面那句“禁止裸 create_task”的说明写进源码注释后会把本条变成误报
    # （同理 docstring）。理由见 tests/srcprobe.py。
    source = module_code_text(fence)
    assert "asyncio.create_task(" not in source, "后台协程一律走 safego.spawn"
    for name in (
        fence._TASK_LEASE_KEEPALIVE,
        fence._TASK_MONITOR_LEASE,
        fence._TASK_MONITOR_REQUIRED,
    ):
        assert isinstance(name, str) and "{" not in name and name.islower()

    backend = _FakeBackend()
    holder = await fence.start(backend, _cfg())
    try:
        names = {task.get_name() for task in holder._tasks}
        assert names == {fence._TASK_MONITOR_LEASE, fence._TASK_MONITOR_REQUIRED}
    finally:
        await holder.close()


async def test_close_cancels_background_tasks() -> None:
    """关闭必须真的把后台任务收干净 —— 残留的 watch 循环会在下次 `lost` 时打假告警。

    ★ 变异:`Holder.close` 里去掉 `task.cancel()` → 本条红。
    """
    backend = _FakeBackend()
    holder = await fence.start(backend, _cfg())
    tasks = list(holder._tasks)
    await holder.close()
    for task in tasks:
        assert task.done()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.gather(*tasks, return_exceptions=True)
