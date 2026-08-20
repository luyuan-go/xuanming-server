"""legacy HS256 DSTicket 的签发与验签 —— 对应 Go 的 `pkg/auth` `signDSTicket` /
`VerifyDSTicket`(**v1 / HS256 那一支**)。

★ 为什么不用 `pandorapy.auth.Signer.sign_internal`:DSTicket 带一整套自定义 claim
  (ds_type / match_id / region_id / cell_id / role_id / ds_pod / hub_assignment_id …),
  `sign_internal` 只能签 RegisteredClaims。claim 名必须与 Go 的 json tag **逐字相同** ——
  UE DS 是照 Go 的字节解析的,少一个下划线就是"票签出来了、DS 一律拒",而两边日志全绿。

★ v2(RS256,`login.ds_ticket.private_key_file` 非空)**未实现**:本模块只覆盖
  legacy HS256。main.py 在 v2 启用时 fail-fast 拒启,不会走到这里 —— 这是刻意的:
  静默用 HS256 顶替 RS256 会让 DS 全拒票,表现为"全服进不去场景"且启动日志全绿。
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
import uuid

import jwt as pyjwt

from pandorapy import errcode

ALGORITHM = "HS256"

DS_TYPE_HUB = "hub"
DS_TYPE_BATTLE = "battle"


@dataclasses.dataclass(slots=True)
class DSTicketClaims:
    """已验签的 DSTicket claims(与 Go 的 biz.DSTicketClaims 同形,只保留 v1 能带的字段)。"""

    player_id: int = 0
    match_id: int = 0
    issued_at_ms: int = 0
    expires_at_ms: int = 0
    ds_type: str = ""
    jti: str = ""
    region_id: int = 0
    cell_id: int = 0
    role_id: int = 0
    ds_pod_name: str = ""
    ds_instance_uid: str = ""
    ds_protocol_epoch: int = 0
    ds_credential_gen: int = 0
    ds_credential_jti: str = ""
    hub_assignment_id: str = ""
    ds_writer_epoch: int = 0
    #: §9.21 灰度轨道粘滞。Go `pkg/auth/dsticket.go:81` 的 `release_track,omitempty`。
    #: 初版漏解这一列 —— 于是归属校验里所有 release_track 判据都因为恒为空串而
    #: 被跳过,stable 票能在 canary Pod 上兑换,反之亦然。
    release_track: str = ""
    source_match_id: int = 0
    # sjti 只存在于 v2(RS256)票;v1 票恒为空 —— 见 RequireTicketSessionCurrent 的兼容窗。
    sess_jti: str = ""
    version: int = 1


class DSTicketSigner:
    """HS256 DSTicket 签发器。密钥与 SessionToken 同一把(legacy 档的既定形态)。"""

    __slots__ = ("_secret", "_issuer", "_audience", "_ttl", "_additional")

    def __init__(
        self,
        secret: str,
        issuer: str,
        audience: str,
        ttl: _dt.timedelta,
        additional_secrets: tuple[str, ...] = (),
    ) -> None:
        self._secret = secret
        self._issuer = issuer
        self._audience = audience
        self._ttl = ttl
        self._additional = additional_secrets

    @property
    def ttl(self) -> _dt.timedelta:
        return self._ttl

    def sign(
        self,
        player_id: int,
        ds_type: str,
        *,
        match_id: int = 0,
        region_id: int = 0,
        cell_id: int = 0,
        role_id: int = 0,
        source_match_id: int = 0,
        jti: str = "",
    ) -> tuple[str, int]:
        """签一张 DSTicket,返回 (token, expires_at_ms)。

        入参校验逐条对 Go 的 `signDSTicket`:
          - battle 票必须带 match_id(没有 match 的 battle 票无从判定 roster);
          - battle 票**不得**带 source_match_id(那是 Battle→Hub 回流 fence,只属 hub 票);
          - jti 不得为空(它是 B1 纯本地验票下**唯一**的吊销手段)。
        """
        if player_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "auth.SignDSTicket: playerID must be > 0"
            )
        if ds_type not in (DS_TYPE_HUB, DS_TYPE_BATTLE):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "auth.SignDSTicket: invalid dsType %r", ds_type
            )
        if ds_type == DS_TYPE_BATTLE and match_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "auth.SignDSTicket: battle DSTicket requires matchID"
            )
        if ds_type == DS_TYPE_BATTLE and source_match_id != 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "auth.SignDSTicket: battle DSTicket must not carry source_match_id",
            )
        jti = jti or str(uuid.uuid4())
        now = _dt.datetime.now(_dt.UTC)
        exp = now + self._ttl
        claims: dict = {
            "iss": self._issuer,
            "sub": str(player_id),
            "aud": [self._audience],
            "iat": int(now.timestamp()),
            "exp": int(exp.timestamp()),
            "jti": jti,
            "ds_type": ds_type,
        }
        # omitempty:零值不序列化 —— 与 Go 的 json tag 一致。多写一个 "match_id": 0
        # 会让按字节比对票据的用例分叉,也会让 DS 侧把 0 当成"显式指定了 match 0"。
        if match_id:
            claims["match_id"] = match_id
        if region_id:
            claims["region_id"] = region_id
        if cell_id:
            claims["cell_id"] = cell_id
        if role_id:
            claims["role_id"] = role_id
        if source_match_id:
            claims["source_match_id"] = source_match_id
        token = pyjwt.encode(claims, self._secret, algorithm=ALGORITHM)
        return token, int(exp.timestamp() * 1000)

    def verify(self, token: str) -> DSTicketClaims:
        """验签 + 校验 iss / aud / exp,返回 claims。

        依次尝试 [主密钥, *additional_secrets] —— 备用密钥只用于校验,不用于签发,
        这是不停服密钥轮换的全部机制(缺了它,换密钥当天旧副本签的票被新副本拒)。
        """
        last: Exception | None = None
        for key in (self._secret, *self._additional):
            try:
                payload = pyjwt.decode(
                    token,
                    key,
                    algorithms=[ALGORITHM],
                    audience=self._audience,
                    issuer=self._issuer,
                    options={"require": ["exp", "iat", "iss", "sub", "aud", "jti"]},
                )
            except pyjwt.ExpiredSignatureError as exc:
                # 过期与"签名不对"必须分开:换密钥时不能把已过期的旧票拿去逐把重试。
                raise errcode.PandoraError(
                    errcode.ErrLoginTicketExpired, "ds ticket expired: %s", exc
                ) from exc
            except pyjwt.InvalidSignatureError as exc:
                last = exc
                continue  # 可能是别的密钥签的,继续试
            except pyjwt.PyJWTError as exc:
                raise errcode.PandoraError(
                    errcode.ErrLoginTicketInvalid, "ds ticket invalid: %s", exc
                ) from exc
            else:
                return _claims_from_payload(payload)
        raise errcode.PandoraError(
            errcode.ErrLoginTicketInvalid, "ds ticket signature invalid: %s", last
        )


def _claims_from_payload(payload: dict) -> DSTicketClaims:
    try:
        player_id = int(payload.get("sub") or 0)
    except (TypeError, ValueError):
        player_id = 0
    return DSTicketClaims(
        player_id=player_id,
        match_id=int(payload.get("match_id") or 0),
        issued_at_ms=int(payload.get("iat") or 0) * 1000,
        expires_at_ms=int(payload.get("exp") or 0) * 1000,
        ds_type=str(payload.get("ds_type") or ""),
        jti=str(payload.get("jti") or ""),
        region_id=int(payload.get("region_id") or 0),
        cell_id=int(payload.get("cell_id") or 0),
        role_id=int(payload.get("role_id") or 0),
        ds_pod_name=str(payload.get("ds_pod") or ""),
        ds_instance_uid=str(payload.get("ds_uid") or ""),
        ds_protocol_epoch=int(payload.get("ds_epoch") or 0),
        ds_credential_gen=int(payload.get("ds_gen") or 0),
        ds_credential_jti=str(payload.get("ds_credential_jti") or ""),
        hub_assignment_id=str(payload.get("hub_assignment_id") or ""),
        ds_writer_epoch=int(payload.get("ds_writer_epoch") or 0),
        release_track=str(payload.get("release_track") or ""),
        source_match_id=int(payload.get("source_match_id") or 0),
        sess_jti=str(payload.get("sjti") or ""),
        version=1,
    )
