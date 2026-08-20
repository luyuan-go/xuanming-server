"""DSTicket v2(RS256 / JWKS)—— 跨栈互操作与 fail-closed 回归。

★ 这个文件守的是什么:
    Python 签出的票必须能被 Go 的 `auth.DSTicketVerifier` 与 UE DS 侧验过,反之亦然。
    三个实现读同一份 JWKS、同一份 claim 契约。偏差不会报错,只会表现为
    "票签出来了、DS 一律拒" → 玩家全线进不去场景,而两栈日志全绿。

★ 关于"固定测试私钥":
    本文件**不内嵌任何私钥 PEM**(AGENTS.md §3:secret 不进 git 跟踪文件)。
    跨栈契约里真正需要钉死的是 **claim 名 / 单位 / header / 编码**,这些与具体
    密钥无关 —— 所以 `test_go_wire_contract_*` 用每次生成的密钥,但把
    **header 键集合、payload 键集合与每个值**逐个断言死。密钥相关的固定向量由
    `test_rfc7638_thumbprint_matches_published_vector` 承担:它用 RFC 7638 §3.1
    **公开发表的公钥**(不是秘密)对拍官方指纹 —— Go / Python / UE 三边算 kid
    的口径只要有一边漂,这条就红。

变异验证:每条测试的 docstring 里标了 `★ 变异:<把产品代码怎么改> → 本条红`,
全部实际跑过一遍(改坏→红→还原→绿)。
"""

from __future__ import annotations

import base64
import copy
import datetime as _dt
import json

import pytest
from cryptography.hazmat.primitives.asymmetric import rsa as _rsa

from pandorapy import dsticket, errcode

# ── 固定时钟(注入,不看系统时间)────────────────────────────────────────────

_NOW = _dt.datetime(2026, 7, 13, 12, 0, 0, tzinfo=_dt.UTC)


def _now_fn() -> _dt.datetime:
    return _NOW


@pytest.fixture(scope="module")
def keypair() -> tuple[bytes, _rsa.RSAPublicKey, str]:
    """一对测试密钥(module 级:RSA-2048 生成不便宜,但绝不缓存到磁盘)。"""
    return dsticket.generate_ds_ticket_key_pair()


@pytest.fixture(scope="module")
def signer(keypair) -> dsticket.DSTicketSigner:
    private_pem, _pub, kid = keypair
    return dsticket.DSTicketSigner.new(
        dsticket.DSTicketSignerConfig(
            private_key_pem=private_pem, active_kid=kid, now_fn=_now_fn
        )
    )


@pytest.fixture(scope="module")
def verifier(keypair) -> dsticket.DSTicketVerifier:
    _pem, pub, kid = keypair
    jwks = dsticket.marshal_ds_ticket_jwks(1, kid, pub)
    return dsticket.DSTicketVerifier.new(
        dsticket.DSTicketVerifierConfig(jwks=jwks, now_fn=_now_fn)
    )


def _hub_target() -> dsticket.DSTicketTarget:
    return dsticket.DSTicketTarget(
        ds_pod_name="pandora-hub-abc12",
        ds_instance_uid="uid-hub-1",
        ds_instance_epoch=7,
        release_track=dsticket.RELEASE_TRACK_STABLE,
        hub_assignment_id="assign-9",
    )


def _battle_target() -> dsticket.DSTicketTarget:
    return dsticket.DSTicketTarget(
        ds_pod_name="pandora-battle-x9",
        ds_instance_uid="uid-battle-1",
        ds_instance_epoch=3,
        release_track=dsticket.RELEASE_TRACK_CANARY,
        match_id=9001,
        allocation_id="alloc-77",
    )


def _segments(token: str) -> tuple[str, str, str]:
    header_b64, payload_b64, signature_b64 = token.split(".")
    return header_b64, payload_b64, signature_b64


def _decode_segment(text: str) -> dict:
    padded = text + "=" * ((4 - len(text) % 4) % 4)
    return json.loads(base64.urlsafe_b64decode(padded.encode("ascii")))


# ══ 跨栈线格式契约(最重要的一组)══════════════════════════════════════════


def test_go_wire_contract_hub_ticket_claims_and_header(signer, keypair) -> None:
    """hub 票的 header / payload **逐字段**等于 Go 侧 json tag 契约。

    这条是整个文件的核心:Go 侧任何人改了 `dsticket.go` 的 json tag、
    改了 aud 形态、把 iat 写成毫秒,这条立刻红。

    ★ 变异:把 `_sign` 里的 `"ds_pod"` 改成 `"ds_pod_name"` → 本条红
    ★ 变异:把 `"iat": _unix_seconds(now)` 改成 `_unix_milli(now)` → 本条红
    ★ 变异:把 `"aud": [self._audience]` 改成 `self._audience`(裸串)→ 本条红
    """
    _pem, _pub, kid = keypair
    token, expires_at_ms = signer.sign_hub_ticket(
        7777, 3, 17, 42, "jti-hub-1", _hub_target()
    )
    header_b64, payload_b64, signature_b64 = _segments(token)

    # ① header:恰好三个键,值逐个钉死(Go 是 typ/alg + 手工塞的 kid)。
    assert _decode_segment(header_b64) == {
        "alg": "RS256",
        "kid": kid,
        "typ": "JWT",
    }

    # ② payload:键集合与值逐个钉死。region/cell/role 非零 → 必须出现;
    #    match_id / allocation_id / source_match_id / sjti 为零值 → 必须**不出现**。
    iat = int((_NOW - _dt.datetime(1970, 1, 1, tzinfo=_dt.UTC)).total_seconds())
    assert _decode_segment(payload_b64) == {
        "iss": "pandora-dsticket",
        "sub": "7777",
        "aud": ["pandora-game-ds"],
        "iat": iat,
        "exp": iat + 120,
        "jti": "jti-hub-1",
        "dst_ver": 2,
        "ds_type": "hub",
        "ds_pod": "pandora-hub-abc12",
        "ds_uid": "uid-hub-1",
        "ds_instance_epoch": 7,
        "release_track": "stable",
        "region_id": 3,
        "cell_id": 17,
        "role_id": 42,
        "hub_assignment_id": "assign-9",
    }

    # ③ 单位:JWT 里是**秒**,返回值是**毫秒**。弄反了两栈永远互不认票。
    assert expires_at_ms == (iat + 120) * 1000

    # ④ base64url 三段一律**无 padding**(RFC 7515 §2)。
    for segment in (header_b64, payload_b64, signature_b64):
        assert "=" not in segment
        assert "+" not in segment and "/" not in segment


def test_go_wire_contract_battle_ticket_claims(signer) -> None:
    """battle 票:match_id / allocation_id 出现,hub 专属字段与 role_id 全部消失。

    ★ 变异:把 `sign_battle_ticket` 的 `0`(role_id)改成传入 role_id → 本条红
    ★ 变异:把 `_put_omitempty` 改成无条件 `claims[key] = value` → 本条红
      (payload 会多出 role_id/hub_assignment_id/source_match_id/sjti 四个零值键)
    """
    token, _ = signer.sign_battle_ticket(88, 0, 0, "jti-b", _battle_target())
    payload = _decode_segment(_segments(token)[1])
    assert payload["ds_type"] == "battle"
    assert payload["match_id"] == 9001
    assert payload["allocation_id"] == "alloc-77"
    assert payload["release_track"] == "canary"
    # 零值 omitempty 字段一个都不许出现。
    for absent in (
        "region_id",
        "cell_id",
        "role_id",
        "hub_assignment_id",
        "source_match_id",
        "sjti",
    ):
        assert absent not in payload
    # 非 omitempty 字段即使零值也必须在(Go 侧无 omitempty tag)。
    assert payload["ds_instance_epoch"] == 3


def test_optional_fences_are_carried_verbatim(signer, verifier) -> None:
    """§9 不变量 3 点名的两个 fence(sjti / source_match_id)必须原样带上并回读。

    ★ 变异:把 `_sign` 里的 `_put_omitempty(claims, "sjti", ...)` 整行删掉 → 本条红
    """
    target = dsticket.DSTicketTarget(
        ds_pod_name="pandora-hub-abc12",
        ds_instance_uid="uid-hub-1",
        ds_instance_epoch=7,
        release_track=dsticket.RELEASE_TRACK_STABLE,
        hub_assignment_id="assign-9",
        source_match_id=555,
        session_jti="sess-abc",
    )
    token, _ = signer.sign_hub_ticket(7777, 0, 0, 0, "jti-fence", target)
    payload = _decode_segment(_segments(token)[1])
    assert payload["source_match_id"] == 555
    assert payload["sjti"] == "sess-abc"

    claims = verifier.verify(token)
    assert claims.source_match_id == 555
    assert claims.sess_jti == "sess-abc"


def test_rfc7638_thumbprint_matches_published_vector() -> None:
    """RFC 7638 §3.1 的官方样例:kid 必须是 `NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs`。

    这是**跨语言固定向量**:Go 用 `fmt.Sprintf` 手拼 canonical JSON、Python 手拼、
    UE 侧也手拼。任意一边多个空格 / 排错成员顺序 / 给 n 留了前导 0,kid 就变了,
    而变了的表现是"票带的 kid 在对方 keyset 里找不到"。

    ★ 变异:把 `rsa_public_key_thumbprint` 的 canonical 串改成
      `{"kty":"RSA","e":"...","n":"..."}`(成员非字典序)→ 本条红
    ★ 变异:把 `_b64url_encode` 的 `.rstrip(b"=")` 去掉 → 本条红
    """
    n = (
        "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7apJ"
        "l3WkjcYWLwWpH0YMRvHHIrRAVJMcHIx7-Nlb2iOHiXHwUwjJRAqXf3XFsdE9YQmzcRJRvjnbz"
        "1ecFDdJK7NPxdCETcaXt7iXpDdWfGdCTIgo1AJUwSQ8-2WMFj3aE0GZY2BUQdCa9K6HRAZS0O"
        "L2Cp-y_HrIVwvFPZfLzq-4Ke-6PgrK2FTHUuLK4uAWdbCsBM2QGjxeFvKtT4Vq6QT2ZGkPTQ_"
        "SUsX8hxNLYCJ7ivTnO-yPYbtCkYCKDbBqBpFvBLmZ6BXqzOw"
    )
    # 上面这串是 RFC 原文样例的转写;真正的固定向量断言在下面用 RFC 原样 n/e。
    del n
    rfc_n = (
        "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4"
        "cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMs"
        "tn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2"
        "QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbI"
        "SD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqb"
        "w0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw"
    )
    rfc_e = "AQAB"
    pub = dsticket.rsa_public_key_from_jwk(rfc_n, rfc_e)
    assert (
        dsticket.rsa_public_key_thumbprint(pub)
        == "NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs"
    )


def test_round_trip_hub_and_battle(signer, verifier) -> None:
    """签→验闭环:所有绑定字段原样回读。

    ★ 变异:把 `_claims_from_payload` 里 `ds_instance_epoch` 的 key 写成
      `"ds_epoch"` → 本条红(回读成 0,随后 target.validate 也会拒)
    """
    token, _ = signer.sign_hub_ticket(7777, 3, 17, 42, "jti-hub-1", _hub_target())
    claims = verifier.verify(token)
    assert claims.player_id() == 7777
    assert claims.dst_ver == dsticket.DS_TICKET_VERSION_2
    assert claims.ds_type == dsticket.DS_TYPE_HUB
    assert claims.ds_pod_name == "pandora-hub-abc12"
    assert claims.ds_instance_uid == "uid-hub-1"
    assert claims.ds_instance_epoch == 7
    assert claims.release_track == dsticket.RELEASE_TRACK_STABLE
    assert claims.hub_assignment_id == "assign-9"
    assert claims.region_id == 3
    assert claims.cell_id == 17
    assert claims.role_id == 42
    assert claims.jti == "jti-hub-1"

    battle_token, _ = signer.sign_battle_ticket(88, 0, 0, "jti-b", _battle_target())
    battle = verifier.verify(battle_token)
    assert battle.match_id == 9001
    assert battle.allocation_id == "alloc-77"
    assert battle.release_track == dsticket.RELEASE_TRACK_CANARY
    assert battle.role_id == 0


# ══ TTL:双向强制(§9 不变量 3)═════════════════════════════════════════════


def test_signer_rejects_ttl_over_max(keypair) -> None:
    """签发侧:TTL > 180s **启动即拒**。

    ★ 变异:把 `DS_TICKET_MAX_TTL` 改成 `timedelta(minutes=30)` → 本条红
    ★ 变异:把 `ttl > DS_TICKET_MAX_TTL` 改成 `ttl > _dt.timedelta(days=1)` → 本条红
    """
    private_pem, _pub, kid = keypair
    with pytest.raises(dsticket.DSTicketConfigError) as caught:
        dsticket.DSTicketSigner.new(
            dsticket.DSTicketSignerConfig(
                private_key_pem=private_pem,
                active_kid=kid,
                ttl=_dt.timedelta(minutes=5),  # legacy HS256 档的值,v2 必须拒
            )
        )
    # 时长格式走 godur:Go 打的是 "3m0s",不是 "0:03:00"。
    assert "3m0s" in caught.value.msg


def test_signer_defaults_to_120s(signer) -> None:
    """签发侧:不配 TTL → 恰好 120s(不是 PyJWT/框架的某个默认值)。

    ★ 变异:把 `DS_TICKET_DEFAULT_TTL` 改成 `timedelta(minutes=1)` → 本条红
    """
    assert signer.ttl() == _dt.timedelta(seconds=120)
    token, _ = signer.sign_hub_ticket(1, 0, 0, 0, "j", _hub_target())
    payload = _decode_segment(_segments(token)[1])
    assert payload["exp"] - payload["iat"] == 120


def test_verifier_rejects_long_lived_ticket(keypair) -> None:
    """验签侧:`exp-iat > 180s` 一律拒 —— 这就是"双向"的另一半。

    只在签发侧限制的话,任何一个配错(或被攻破)的签发点都能签出长效 capability,
    而 B1 下 DS 不回后端查吊销 → 吊销手段直接失效。

    ★ 变异:把 verify 里的 `> DS_TICKET_MAX_TTL.total_seconds()` 判断整段删掉 → 本条红
    """
    import jwt as pyjwt  # noqa: PLC0415 —— 只有这条测试要手工签越权票

    private_pem, pub, kid = keypair
    key = dsticket.parse_rsa_private_key_pem(private_pem)
    iat = int(_NOW.timestamp())
    payload = {
        "iss": dsticket.DS_TICKET_ISSUER,
        "sub": "7777",
        "aud": [dsticket.DS_TICKET_AUDIENCE],
        "iat": iat,
        "exp": iat + 3600,  # 一小时:签名完全合法,只是寿命违约
        "jti": "jti-long",
        "dst_ver": 2,
        "ds_type": "hub",
        "ds_pod": "pandora-hub-abc12",
        "ds_uid": "uid-hub-1",
        "ds_instance_epoch": 7,
        "release_track": "stable",
        "hub_assignment_id": "assign-9",
    }
    token = pyjwt.encode(payload, key, algorithm="RS256", headers={"kid": kid})
    jwks = dsticket.marshal_ds_ticket_jwks(1, kid, pub)
    verifier = dsticket.DSTicketVerifier.new(
        dsticket.DSTicketVerifierConfig(jwks=jwks, now_fn=_now_fn)
    )
    with pytest.raises(dsticket.DSTicketInvalidError) as caught:
        verifier.verify(token)
    assert "max ttl" in caught.value.msg


def test_expired_ticket_maps_to_expired_code(signer, keypair) -> None:
    """过期 → `ErrLoginTicketExpired`(1010),**不是** 1011。

    两者客户端处置相反:过期该静默重取票,非法该停手告警。合成一个之后,
    密钥配错那天会变成全量客户端重试风暴。

    ★ 变异:把 verify 里的 `raise DSTicketExpiredError` 改成 `DSTicketInvalidError` → 本条红
    """
    _pem, pub, kid = keypair
    token, _ = signer.sign_hub_ticket(7777, 0, 0, 0, "j", _hub_target())
    jwks = dsticket.marshal_ds_ticket_jwks(1, kid, pub)
    late = dsticket.DSTicketVerifier.new(
        dsticket.DSTicketVerifierConfig(
            jwks=jwks, now_fn=lambda: _NOW + _dt.timedelta(seconds=121)
        )
    )
    with pytest.raises(dsticket.DSTicketExpiredError) as caught:
        late.verify(token)
    assert errcode.as_code(caught.value) == errcode.ErrLoginTicketExpired


# ══ fail-closed:算法混淆 / kid ═════════════════════════════════════════════


def test_alg_confusion_hs256_with_public_key_is_rejected(keypair, verifier) -> None:
    """把 alg 换成 HS256、拿**公钥字节**当 HMAC 密钥 —— 经典混淆攻击,必须拒。

    ★ 变异:把 verify 的 `algorithms=[DS_TICKET_ALGORITHM]` 改成
      `["RS256", "HS256"]` → 本条红
    """
    import hashlib  # noqa: PLC0415
    import hmac as _hmac  # noqa: PLC0415

    from cryptography.hazmat.primitives import serialization  # noqa: PLC0415

    _pem, pub, kid = keypair
    pub_bytes = pub.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    iat = int(_NOW.timestamp())
    payload = {
        "iss": dsticket.DS_TICKET_ISSUER,
        "sub": "7777",
        "aud": [dsticket.DS_TICKET_AUDIENCE],
        "iat": iat,
        "exp": iat + 60,
        "jti": "evil",
        "dst_ver": 2,
        "ds_type": "hub",
        "ds_pod": "p",
        "ds_uid": "u",
        "ds_instance_epoch": 1,
        "release_track": "stable",
        "hub_assignment_id": "a",
    }

    # PyJWT 自己就拦"拿非对称密钥当 HMAC 秘密",所以攻击票必须手工拼 ——
    # 攻击者用的是自己的 JWT 库,不会有这层善意保护。
    def _seg(obj: dict) -> str:
        raw = json.dumps(obj, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    signing_input = f"{_seg({'alg': 'HS256', 'typ': 'JWT', 'kid': kid})}.{_seg(payload)}"
    mac = _hmac.new(pub_bytes, signing_input.encode(), hashlib.sha256).digest()
    forged = f"{signing_input}.{base64.urlsafe_b64encode(mac).rstrip(b'=').decode()}"

    with pytest.raises(dsticket.DSTicketInvalidError):
        verifier.verify(forged)


def test_alg_none_is_rejected(verifier, keypair) -> None:
    """`alg: none`(无签名)必须拒 —— **用真 kid**,确保走到算法闸而不是被 kid 闸提前挡下。

    用假 kid 的话,`unknown kid` 会先拒掉,这条就变成了在测 kid 查找,
    算法白名单被删也照样绿。

    ★ 变异:把 verify 的 `algorithms=[DS_TICKET_ALGORITHM]` 改成
      `[DS_TICKET_ALGORITHM, "none"]` → 本条红
    """
    _pem, _pub, kid = keypair
    iat = int(_NOW.timestamp())
    header = base64.urlsafe_b64encode(
        json.dumps({"alg": "none", "typ": "JWT", "kid": kid}).encode()
    ).rstrip(b"=")
    payload = base64.urlsafe_b64encode(
        json.dumps(
            {
                "iss": dsticket.DS_TICKET_ISSUER,
                "sub": "1",
                "aud": [dsticket.DS_TICKET_AUDIENCE],
                "iat": iat,
                "exp": iat + 60,
                "jti": "j",
            }
        ).encode()
    ).rstrip(b"=")
    token = f"{header.decode()}.{payload.decode()}."
    with pytest.raises(dsticket.DSTicketInvalidError):
        verifier.verify(token)


def test_ds_ticket_algorithm_dispatch_whitelist() -> None:
    """`ds_ticket_algorithm` 只放行 HS256 / RS256,其余(含 none)在分发前就拒。

    ★ 变异:把 `alg not in ("HS256", DS_TICKET_ALGORITHM)` 改成
      `alg == "none"` (黑名单)→ 本条红(`ES256` 会被放行)
    """
    import jwt as pyjwt  # noqa: PLC0415

    hs = pyjwt.encode({"sub": "1"}, "x" * 32, algorithm="HS256")
    assert dsticket.ds_ticket_algorithm(hs) == "HS256"

    header = base64.urlsafe_b64encode(json.dumps({"alg": "ES256"}).encode()).rstrip(b"=")
    payload = base64.urlsafe_b64encode(json.dumps({"sub": "1"}).encode()).rstrip(b"=")
    with pytest.raises(dsticket.DSTicketInvalidError):
        dsticket.ds_ticket_algorithm(f"{header.decode()}.{payload.decode()}.sig")

    with pytest.raises(dsticket.DSTicketInvalidError):
        dsticket.ds_ticket_algorithm("")


def test_unknown_and_missing_kid_are_rejected(signer, keypair) -> None:
    """未知 kid / 缺 kid 一律拒(kid 只是选键提示,但**必须**存在且已知)。

    ★ 变异:把 `_lookup_key` 返回 None 时的 raise 改成"取 keyset 里第一把" → 本条红
    """
    _pem, _pub, _kid = keypair
    other_pem, other_pub, other_kid = dsticket.generate_ds_ticket_key_pair()
    foreign_signer = dsticket.DSTicketSigner.new(
        dsticket.DSTicketSignerConfig(
            private_key_pem=other_pem, active_kid=other_kid, now_fn=_now_fn
        )
    )
    foreign_token, _ = foreign_signer.sign_hub_ticket(1, 0, 0, 0, "j", _hub_target())

    # keyset 里只有 keypair 那把 → 外来 kid 必须拒。
    jwks = dsticket.marshal_ds_ticket_jwks(1, keypair[2], keypair[1])
    verifier = dsticket.DSTicketVerifier.new(
        dsticket.DSTicketVerifierConfig(jwks=jwks, now_fn=_now_fn)
    )
    with pytest.raises(dsticket.DSTicketInvalidError) as caught:
        verifier.verify(foreign_token)
    assert "unknown kid" in caught.value.msg
    del other_pub

    # 缺 kid:手工签一张不带 kid 头的票。
    import jwt as pyjwt  # noqa: PLC0415

    key = dsticket.parse_rsa_private_key_pem(keypair[0])
    iat = int(_NOW.timestamp())
    no_kid = pyjwt.encode(
        {
            "iss": dsticket.DS_TICKET_ISSUER,
            "sub": "1",
            "aud": [dsticket.DS_TICKET_AUDIENCE],
            "iat": iat,
            "exp": iat + 60,
            "jti": "j",
            "dst_ver": 2,
            "ds_type": "hub",
            "ds_pod": "p",
            "ds_uid": "u",
            "ds_instance_epoch": 1,
            "release_track": "stable",
            "hub_assignment_id": "a",
        },
        key,
        algorithm="RS256",
    )
    with pytest.raises(dsticket.DSTicketInvalidError) as caught:
        verifier.verify(no_kid)
    assert "requires kid header" in caught.value.msg


def test_cross_domain_issuer_audience_rejected(keypair) -> None:
    """换 iss 或 aud 的票在 DSTicket 域必然失败(信任域隔离)。

    两条**分别**只改一个维度 —— 否则一个维度的校验被删掉,另一个维度还会兜住,
    测试照样绿,等于这条没在守 iss。

    ★ 变异:把 verify 的 `issuer=self._issuer,` 改成 `issuer=None,` → 第一段红
    ★ 变异:把 verify 的 `audience=self._audience,` 改成 `audience=None,` → 第二段红
    """
    private_pem, pub, kid = keypair
    jwks = dsticket.marshal_ds_ticket_jwks(1, kid, pub)
    verifier = dsticket.DSTicketVerifier.new(
        dsticket.DSTicketVerifierConfig(jwks=jwks, now_fn=_now_fn)
    )

    # ① 只换 iss(aud 保持 pandora-game-ds)。
    wrong_issuer = dsticket.DSTicketSigner.new(
        dsticket.DSTicketSignerConfig(
            private_key_pem=private_pem,
            active_kid=kid,
            issuer="pandora-login",
            now_fn=_now_fn,
        )
    )
    token, _ = wrong_issuer.sign_hub_ticket(1, 0, 0, 0, "j", _hub_target())
    with pytest.raises(dsticket.DSTicketInvalidError):
        verifier.verify(token)

    # ② 只换 aud(iss 保持 pandora-dsticket)—— 拿玩家面 audience 冒充。
    wrong_audience = dsticket.DSTicketSigner.new(
        dsticket.DSTicketSignerConfig(
            private_key_pem=private_pem,
            active_kid=kid,
            audience="pandora-client",
            now_fn=_now_fn,
        )
    )
    token, _ = wrong_audience.sign_hub_ticket(1, 0, 0, 0, "j", _hub_target())
    with pytest.raises(dsticket.DSTicketInvalidError):
        verifier.verify(token)


# ══ 绑定完整性(§9 不变量 3:一个字段都不能漏)═══════════════════════════════


@pytest.mark.parametrize(
    ("kwargs", "fragment"),
    [
        ({"ds_pod_name": ""}, "pod/uid/instance_epoch"),
        ({"ds_instance_uid": ""}, "pod/uid/instance_epoch"),
        ({"ds_instance_epoch": 0}, "pod/uid/instance_epoch"),
        ({"release_track": ""}, "invalid release_track"),
        ({"release_track": "beta"}, "invalid release_track"),
        ({"hub_assignment_id": ""}, "requires hub_assignment_id"),
        ({"match_id": 5}, "must not carry match/allocation"),
    ],
)
def test_hub_target_validation(signer, kwargs, fragment) -> None:
    """hub 票的每个必填 / 互斥绑定都必须在签发期就拒。

    ★ 变异:把 `validate` 里 release_track 那条 `if` 删掉 → `release_track=""`
      与 `"beta"` 两个用例红(空轨道会让 §9.21 轨道粘滞静默失效)
    """
    target = dataclasses_replace(_hub_target(), **kwargs)
    with pytest.raises(dsticket.DSTicketConfigError) as caught:
        signer.sign_hub_ticket(1, 0, 0, 0, "j", target)
    assert fragment in caught.value.msg


@pytest.mark.parametrize(
    ("kwargs", "fragment"),
    [
        ({"match_id": 0}, "requires match_id and allocation_id"),
        ({"allocation_id": ""}, "requires match_id and allocation_id"),
        ({"hub_assignment_id": "a"}, "must not carry hub_assignment_id"),
        ({"source_match_id": 7}, "must not carry source_match_id"),
    ],
)
def test_battle_target_validation(signer, kwargs, fragment) -> None:
    """battle 票同理;`source_match_id` 只属 hub 票(battle 绑定走 match_id)。

    ★ 变异:把 `battle ticket must not carry source_match_id` 那条 `if` 删掉
      → 最后一个用例红
    """
    target = dataclasses_replace(_battle_target(), **kwargs)
    with pytest.raises(dsticket.DSTicketConfigError) as caught:
        signer.sign_battle_ticket(1, 0, 0, "j", target)
    assert fragment in caught.value.msg


def test_empty_jti_is_rejected(signer) -> None:
    """jti 为空必须拒:B1 纯本地验票下它是**唯一**的吊销抓手。

    ★ 变异:把 `if not jti: raise` 改成 `jti = jti or str(uuid.uuid4())` → 本条红
    """
    with pytest.raises(dsticket.DSTicketConfigError) as caught:
        signer.sign_hub_ticket(1, 0, 0, 0, "", _hub_target())
    assert "jti must be non-empty" in caught.value.msg


def test_uint_boundaries_are_enforced(signer) -> None:
    """uint32 / uint64 越界必须拒 —— Python 不回绕,Go 会。

    塞进一个 2**32 的 ds_instance_epoch,Python 侧毫无感觉,到 Go/UE 侧要么解析失败
    要么被截断成**另一个实例号** → 票被兑换到错误的 Pod。

    ★ 变异:把 `_require_uint` 的范围判断改成 `if value < 0:` → 本条红
    """
    over = dataclasses_replace(_hub_target(), ds_instance_epoch=1 << 32)
    with pytest.raises(dsticket.DSTicketConfigError) as caught:
        signer.sign_hub_ticket(1, 0, 0, 0, "j", over)
    assert "uint32" in caught.value.msg

    with pytest.raises(dsticket.DSTicketConfigError):
        signer.sign_hub_ticket(1, 1 << 32, 0, 0, "j", _hub_target())

    with pytest.raises(dsticket.DSTicketConfigError):
        signer.sign_hub_ticket(1 << 64, 0, 0, 0, "j", _hub_target())


def test_player_id_parsing_matches_go_parseuint() -> None:
    """`sub` 解析口径必须与 Go 的 `strconv.ParseUint` 一致。

    ★ 变异:把 `player_id()` 里的 `_DECIMAL_RE.fullmatch` 改成 `self.subject.isdigit()`
      → 本条红(Arabic-Indic 数字会被解成一个 Go 侧解不出的 player_id)
    """
    assert dsticket.DSTicketClaimsV2(subject="7777").player_id() == 7777
    assert dsticket.DSTicketClaimsV2(subject="").player_id() == 0
    assert dsticket.DSTicketClaimsV2(subject="-1").player_id() == 0
    assert dsticket.DSTicketClaimsV2(subject="+1").player_id() == 0
    assert dsticket.DSTicketClaimsV2(subject=" 1 ").player_id() == 0
    assert dsticket.DSTicketClaimsV2(subject="1_0").player_id() == 0
    assert dsticket.DSTicketClaimsV2(subject="١٢٣").player_id() == 0
    assert dsticket.DSTicketClaimsV2(subject="1\n2").player_id() == 0
    assert dsticket.DSTicketClaimsV2(subject=str(1 << 64)).player_id() == 0
    assert dsticket.DSTicketClaimsV2(subject=str((1 << 64) - 1)).player_id() == (1 << 64) - 1


# ══ 严格 JWKS ══════════════════════════════════════════════════════════════


def _good_jwks(keypair) -> dict:
    _pem, pub, kid = keypair
    return json.loads(dsticket.marshal_ds_ticket_jwks(1, kid, pub).decode())


def test_marshal_output_is_deterministic_and_parseable(keypair) -> None:
    """输出确定性(同组 key 恒得同一份文件)+ 自解析闭环 + 2 空格缩进(对 Go MarshalIndent)。

    ★ 变异:把 `entries.sort(...)` 删掉 → 本条红(两把 key 时顺序不定)
    ★ 变异:把 `json.dumps(..., indent=2)` 改成 `indent=4` → 本条红
    """
    _pem, pub, kid = keypair
    _pem2, pub2, _kid2 = dsticket.generate_ds_ticket_key_pair()
    first = dsticket.marshal_ds_ticket_jwks(2, kid, pub, pub2)
    second = dsticket.marshal_ds_ticket_jwks(2, kid, pub2, pub)  # 顺序反过来
    assert first == second
    assert b'\n  "revision": 2' in first
    keys = dsticket.parse_ds_ticket_jwks(first)
    assert set(keys) == {kid, dsticket.rsa_public_key_thumbprint(pub2)}
    assert dsticket.ds_ticket_jwks_metadata(first) == (2, kid)
    assert dsticket.ds_ticket_jwks_revision(first) == 2


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda s: s["keys"][0].update({"kty": "oct"}), id="kty_oct"),
        pytest.param(lambda s: s["keys"][0].update({"d": "AAAA"}), id="private_d"),
        pytest.param(lambda s: s["keys"][0].update({"d": None}), id="private_d_null"),
        pytest.param(lambda s: s["keys"][0].update({"k": ""}), id="private_k_empty"),
        pytest.param(lambda s: s["keys"][0].update({"use": "enc"}), id="use_enc"),
        pytest.param(lambda s: s["keys"][0].update({"alg": "RS512"}), id="alg_rs512"),
        pytest.param(lambda s: s["keys"][0].update({"kid": ""}), id="kid_empty"),
        pytest.param(lambda s: s["keys"][0].update({"kid": "bogus"}), id="kid_mismatch"),
        pytest.param(lambda s: s["keys"][0].update({"x5c": []}), id="unknown_member"),
        pytest.param(lambda s: s.update({"revision": 0}), id="revision_zero"),
        pytest.param(lambda s: s.update({"active_kid": ""}), id="active_kid_empty"),
        pytest.param(lambda s: s.update({"active_kid": "nope"}), id="active_kid_absent"),
        pytest.param(lambda s: s.update({"keys": []}), id="no_keys"),
        pytest.param(lambda s: s.update({"extra": 1}), id="unknown_top_field"),
        pytest.param(lambda s: s["keys"].append(copy.deepcopy(s["keys"][0])), id="dup_kid"),
    ],
)
def test_jwks_is_fail_closed(keypair, mutate) -> None:
    """任何一把 key 不合规 → **整份 keyset 拒**(放行其余几把 = 半可用 keyset)。

    `kty=oct` 与私钥成员这两条尤其是事故判据:它们意味着有人把对称密钥 / 私钥
    投递进了只该拿公钥的 DS Fleet。

    ★ 变异:把 `if any(member in entry for member in _JWK_PRIVATE_MEMBERS)` 改成
      `if entry.get("d")` → `private_d_null` / `private_k_empty` 两个用例红
    ★ 变异:把 unknown field 检查删掉 → `unknown_member` / `unknown_top_field` 红
    """
    document = _good_jwks(keypair)
    mutate(document)
    with pytest.raises(dsticket.DSTicketConfigError):
        dsticket.parse_ds_ticket_jwks(json.dumps(document).encode())


def test_jwks_size_and_count_limits(keypair) -> None:
    """空 / 超大 / 超 8 把 / 尾随数据一律拒。

    ★ 变异:把 `len(raw) > _DS_TICKET_JWKS_MAX_BYTES` 改成 `>= 1 << 30` → 超大用例红
    """
    with pytest.raises(dsticket.DSTicketConfigError):
        dsticket.parse_ds_ticket_jwks(b"")
    with pytest.raises(dsticket.DSTicketConfigError):
        dsticket.parse_ds_ticket_jwks(b"x" * (64 * 1024 + 1))

    document = _good_jwks(keypair)
    document["keys"] = [copy.deepcopy(document["keys"][0]) for _ in range(9)]
    with pytest.raises(dsticket.DSTicketConfigError):
        dsticket.parse_ds_ticket_jwks(json.dumps(document).encode())

    good = dsticket.marshal_ds_ticket_jwks(1, keypair[2], keypair[1])
    with pytest.raises(dsticket.DSTicketConfigError):
        dsticket.parse_ds_ticket_jwks(good + b"{}")


def test_jwks_rejects_weak_and_malformed_keys() -> None:
    """弱 RSA(<2048)、带前导 0 的 n、偶数 e 一律拒。

    ★ 变异:把 `pub.key_size < DS_TICKET_MIN_RSA_BITS` 改成 `< 512` → 弱钥用例红
    ★ 变异:把 `n_bytes[0] == 0` 判断删掉 → 前导 0 用例红(指纹会与 Go 不同)
    """
    weak = _rsa.generate_private_key(public_exponent=65537, key_size=1024).public_key()
    weak_jwks = _forge_jwks(weak)
    with pytest.raises(dsticket.DSTicketConfigError) as caught:
        dsticket.parse_ds_ticket_jwks(weak_jwks)
    assert "too weak" in caught.value.msg

    with pytest.raises(dsticket.DSTicketConfigError) as caught:
        dsticket.rsa_public_key_from_jwk(
            base64.urlsafe_b64encode(b"\x00\x01\x02").rstrip(b"=").decode(), "AQAB"
        )
    assert "minimal big-endian" in caught.value.msg

    with pytest.raises(dsticket.DSTicketConfigError) as caught:
        dsticket.rsa_public_key_from_jwk("AQAB", "AQAC")  # e 偶数
    assert "public exponent" in caught.value.msg


def test_base64url_padding_tolerance_and_strictness() -> None:
    """解码**补** padding(不然所有票都解不开),但非字母表字符一律拒。

    这是移植时最常踩的坑:Python 的 `urlsafe_b64decode` 在缺 padding 时直接抛。

    ★ 变异:把 `padded = text + "=" * ((4 - remainder) % 4)` 改成 `padded = text` → 本条红
    ★ 变异:把 `if not _B64URL_RE.fullmatch(text):` 改成 `if text is None:`
      → `"A+/B"`(标准 base64 字母表)用例红
    """
    # "AQAB" 是 65537 的标准无 padding 编码,长度恰好 4 的倍数。
    assert dsticket.rsa_public_key_from_jwk(
        base64.urlsafe_b64encode((1 << 2048 | 3).to_bytes(257, "big"))
        .rstrip(b"=")
        .decode(),
        "AQAB",
    )
    # 长度非 4 倍数(需要补 padding)也必须能解。
    assert dsticket._b64url_decode("AQ", what="e") == b"\x01"
    for bad in ("AQ=", "A+/B", "AQAB\n", "A"):
        with pytest.raises(dsticket.DSTicketConfigError):
            dsticket._b64url_decode(bad, what="e")


def _forge_jwks(pub: _rsa.RSAPublicKey) -> bytes:
    """手工拼一份 JWKS(绕过 marshal 的位数闸,用于构造弱钥用例)。"""
    numbers = pub.public_numbers()

    def b64(value: int) -> str:
        raw = value.to_bytes((value.bit_length() + 7) // 8, "big")
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    kid = dsticket.rsa_public_key_thumbprint(pub)
    return json.dumps(
        {
            "revision": 1,
            "active_kid": kid,
            "keys": [
                {
                    "kty": "RSA",
                    "use": "sig",
                    "alg": "RS256",
                    "kid": kid,
                    "n": b64(numbers.n),
                    "e": b64(numbers.e),
                }
            ],
        }
    ).encode()


# ══ 私钥 PEM / 配置接线 ════════════════════════════════════════════════════


def test_private_key_pem_block_type_whitelist(keypair, tmp_path) -> None:
    """只认 PKCS#8 `PRIVATE KEY` 与 PKCS#1 `RSA PRIVATE KEY`。

    `load_pem_private_key` 会连 EC / Ed25519 一起收下,而 Go 只认这两种 block 类型;
    放宽的表现不是报错,而是"启动成功但签出的票 DS 验不了"。

    ★ 变异:把 `if block_type not in (...)` 整段删掉 → 本条红(EC 私钥会被收下,
      随后在 isinstance 判定处才炸,错误信息完全不同)
    """
    private_pem, _pub, _kid = keypair
    assert isinstance(
        dsticket.parse_rsa_private_key_pem(private_pem), _rsa.RSAPrivateKey
    )
    with pytest.raises(dsticket.DSTicketConfigError) as caught:
        dsticket.parse_rsa_private_key_pem(b"")
    assert "empty" in caught.value.msg
    with pytest.raises(dsticket.DSTicketConfigError) as caught:
        dsticket.parse_rsa_private_key_pem(b"not a pem at all")
    assert "decode failed" in caught.value.msg
    with pytest.raises(dsticket.DSTicketConfigError) as caught:
        dsticket.parse_rsa_private_key_pem(
            b"-----BEGIN ENCRYPTED PRIVATE KEY-----\nAAAA\n-----END ENCRYPTED PRIVATE KEY-----\n"
        )
    assert "unsupported PEM block type" in caught.value.msg


def test_conf_wiring_rejects_half_finished_rollout(keypair, tmp_path) -> None:
    """revision / active_kid 双对账:"换了键没换文件"必须启动即失败。

    它在运行期的表现是随机一部分票验不过,而两边日志都正常。

    ★ 变异:把 `if revision != want: raise` 删掉 → 本条红
    """
    private_pem, pub, kid = keypair
    key_file = tmp_path / "ds_ticket.key"
    key_file.write_bytes(private_pem)
    jwks_file = tmp_path / "ds_ticket.jwks"
    jwks_file.write_bytes(dsticket.marshal_ds_ticket_jwks(3, kid, pub))

    conf = dsticket.DSTicketConf(
        private_key_file=str(key_file),
        active_kid=kid,
        jwks_file=str(jwks_file),
        keyset_revision="3",
    )
    assert conf.signer_enabled() and conf.verifier_enabled()
    assert dsticket.new_ds_ticket_signer_from_conf(conf).kid() == kid
    assert dsticket.new_ds_ticket_verifier_from_conf(conf) is not None

    stale = dsticket.DSTicketConf(
        active_kid=kid, jwks_file=str(jwks_file), keyset_revision="2"
    )
    with pytest.raises(dsticket.DSTicketConfigError) as caught:
        dsticket.new_ds_ticket_verifier_from_conf(stale)
    assert "revision 不匹配" in caught.value.msg

    wrong_kid = dsticket.DSTicketConf(
        active_kid="not-the-kid", jwks_file=str(jwks_file), keyset_revision="3"
    )
    with pytest.raises(dsticket.DSTicketConfigError) as caught:
        dsticket.new_ds_ticket_verifier_from_conf(wrong_kid)
    assert "active_kid 不匹配" in caught.value.msg

    disabled = dsticket.DSTicketConf()
    assert not disabled.signer_enabled()
    with pytest.raises(dsticket.DSTicketConfigError):
        dsticket.new_ds_ticket_signer_from_conf(disabled)


def test_signer_rejects_wrong_active_kid(keypair) -> None:
    """active_kid 与私钥指纹不符 → 启动即拒(挂错密钥文件的机械闸)。

    ★ 变异:把 `hmac.compare_digest(cfg.active_kid, kid)` 判断整段删掉 → 本条红
    """
    private_pem, _pub, _kid = keypair
    with pytest.raises(dsticket.DSTicketConfigError) as caught:
        dsticket.DSTicketSigner.new(
            dsticket.DSTicketSignerConfig(private_key_pem=private_pem, active_kid="wrong")
        )
    assert "does not match private key thumbprint" in caught.value.msg

    with pytest.raises(dsticket.DSTicketConfigError) as caught:
        dsticket.DSTicketSigner.new(
            dsticket.DSTicketSignerConfig(private_key_pem=private_pem)
        )
    assert "active_kid is required" in caught.value.msg


def test_local_profile_guard(keypair) -> None:
    """本机 DS 运行契约:生产 / 灰度姿态不得被误标成离线 profile。

    ★ 变异:把 `authority_mode != "legacy"` 条件删掉 → 本条红
    """
    dsticket.validate_ds_local_profile_off_v1("off", "legacy", True)
    with pytest.raises(dsticket.DSTicketConfigError):
        dsticket.validate_ds_local_profile_off_v1("enforce", "legacy", True)
    with pytest.raises(dsticket.DSTicketConfigError):
        dsticket.validate_ds_local_profile_off_v1("off", "redis", True)
    with pytest.raises(dsticket.DSTicketConfigError):
        dsticket.validate_ds_local_profile_off_v1("off", "legacy", False)

    dsticket.validate_ds_local_hub_profile_off_v1(
        "off", "legacy", True, _dt.timedelta(hours=12)
    )
    with pytest.raises(dsticket.DSTicketConfigError) as caught:
        dsticket.validate_ds_local_hub_profile_off_v1(
            "off", "legacy", True, _dt.timedelta(hours=11)
        )
    # 时长走 godur:Go 打 "12h0m0s"。
    assert "12h0m0s" in caught.value.msg


# ── 小工具 ───────────────────────────────────────────────────────────────────


def dataclasses_replace(target: dsticket.DSTicketTarget, **kwargs):
    """`dataclasses.replace` 的薄封装(frozen dataclass 不能就地改)。"""
    import dataclasses  # noqa: PLC0415

    return dataclasses.replace(target, **kwargs)
