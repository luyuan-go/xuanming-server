"""legacy HS256 DSTicket 的签发与验签 —— 对应 Go 的 `pkg/auth` `signDSTicket` /
`VerifyDSTicket`(**v1 / HS256 那一支**)。

★ 为什么不用 `pandorapy.auth.Signer.sign_internal`:DSTicket 带一整套自定义 claim
  (ds_type / match_id / region_id / cell_id / role_id / ds_pod / hub_assignment_id …),
  `sign_internal` 只能签 RegisteredClaims。claim 名必须与 Go 的 json tag **逐字相同** ——
  UE DS 是照 Go 的字节解析的,少一个下划线就是"票签出来了、DS 一律拒",而两边日志全绿。

★ 本模块**只签 / 只验 legacy HS256**。v2(RS256,`login.ds_ticket.private_key_file`
  非空)在公共层 `pandorapy.dsticket` 实现,由 `biz.TicketUsecase` 按**配置**二选一
  分派(`_verify_ds_ticket_signature` 按 JOSE header 的 alg 选严格 verifier)。
  两条路径互斥且由配置显式决定:装了任一 v2 组件后,legacy HS256 玩家票一律拒 ——
  静默用 HS256 顶替 RS256 会让 DS 全拒票,表现为"全服进不去场景"且启动日志全绿。

★ `DSTicketClaims` 是**两条路径共用**的已验签视图(对应 Go 的 `biz.verifiedDSTicket`):
  `version` 精确区分 1 / 2,绝不把 v2 缺失字段降级解释成 legacy。
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
    """已验签的 DSTicket claims —— 对应 Go 的 `biz.verifiedDSTicket` / `biz.DSTicketClaims`。

    legacy(v1 / HS256)与 v2(RS256)**共用**这一个结构,由 `version` 精确区分:
    v1 票的 `ds_instance_epoch` / `allocation_id` 恒零,v2 票的
    `ds_protocol_epoch` / `ds_credential_gen` / `ds_credential_jti` / `ds_writer_epoch`
    恒零(v2 有意不携带 callback credential,它只钉稳定实例 + assignment)。

    ★ 判据必须先看 `version` 再看字段是否为零。反过来("字段空就当 legacy")会把一张
      v2 票按 legacy 规则放行 —— §9.22 exact 实例绑定当场失效。
    """

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
    #: v2(RS256 / B1)稳定实例绑定 —— §9.22 exact 实例绑定的两项。legacy 票恒零值。
    #: ★ `ds_instance_epoch` 与 `ds_protocol_epoch` **不是同一个东西**:前者是 v2 的
    #:   `ds_instance_epoch` claim(同名 Pod 重建后递增),后者是 legacy 的 callback
    #:   credential 协议代际。共用一个字段会让 v2 票拿 legacy 规则过门。
    ds_instance_epoch: int = 0
    allocation_id: str = ""
    #: §9.21 灰度轨道粘滞。Go `pkg/auth/dsticket.go:81` 的 `release_track,omitempty`。
    #: 初版漏解这一列 —— 于是归属校验里所有 release_track 判据都因为恒为空串而
    #: 被跳过,stable 票能在 canary Pod 上兑换,反之亦然。
    release_track: str = ""
    source_match_id: int = 0
    # sjti 只存在于 v2(RS256)票;v1 票恒为空 —— 见 RequireTicketSessionCurrent 的兼容窗。
    sess_jti: str = ""
    version: int = 1


@dataclasses.dataclass(frozen=True, slots=True)
class DSTicketBinding:
    """hub DSTicket 的实例 / 归属绑定 —— 对应 Go 的 `auth.DSTicketBinding`。

    零值 = 旧兼容票(不带绑定);非零时**七项必须完整**,身份 =
    `hub_assignment_id + (pod, instance_uid, protocol_epoch, gen, credential_jti, writer_epoch)`。

    ★ 为什么不允许"填几项算几项":半绑定票在 DS 侧会被逐字段比对判空放行 ——
      看起来是新格式,实际能跨实例 / 跨归属兑换,§9.22 的 exact 实例绑定当场失效,
      而两边日志都正常。所以要么整组齐,要么一项都不带。
    """

    ds_pod_name: str = ""
    ds_instance_uid: str = ""
    protocol_epoch: int = 0  # Go: uint32
    credential_gen: int = 0  # Go: uint64
    credential_jti: str = ""
    hub_assignment_id: str = ""
    writer_epoch: int = 0  # Go: uint32

    def empty(self) -> bool:
        """对应 Go 的 `DSTicketBinding.empty()`(七项全零)。"""
        return (
            self.ds_pod_name == ""
            and self.ds_instance_uid == ""
            and self.protocol_epoch == 0
            and self.credential_gen == 0
            and self.credential_jti == ""
            and self.hub_assignment_id == ""
            and self.writer_epoch == 0
        )

    def complete(self) -> bool:
        """对应 Go 的 `DSTicketBinding.complete()`(七项全非零)。"""
        return (
            self.ds_pod_name != ""
            and self.ds_instance_uid != ""
            and self.protocol_epoch != 0
            and self.credential_gen != 0
            and self.credential_jti != ""
            and self.hub_assignment_id != ""
            and self.writer_epoch != 0
        )


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
        binding: DSTicketBinding | None = None,
    ) -> tuple[str, int]:
        """签一张 DSTicket,返回 (token, expires_at_ms)。

        入参校验逐条对 Go 的 `signDSTicket`:
          - battle 票必须带 match_id(没有 match 的 battle 票无从判定 roster);
          - battle 票**不得**带 source_match_id(那是 Battle→Hub 回流 fence,只属 hub 票);
          - jti 不得为空(它是 B1 纯本地验票下**唯一**的吊销手段);
          - `binding` 非空时必须是 hub 票且七项完整(对应 Go 的
            `SignBoundHubDSTicket`;半绑定票一律拒签,理由见 `DSTicketBinding`)。
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
        if binding is not None and not binding.empty():
            if ds_type != DS_TYPE_HUB or not binding.complete():
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "auth.SignDSTicket: hub binding must be complete and only used by hub tickets",
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
        # 绑定七项:claim 名逐字对应 Go `DSTicketClaims` 的 json tag,且照搬 omitempty
        # (零值不序列化)—— 多写一个 "ds_epoch": 0 会让"票是否带绑定"在字节层失真,
        # 而 DS 侧正是按 claim 存在性判别半绑定票的。
        if binding is not None and not binding.empty():
            claims["ds_pod"] = binding.ds_pod_name
            claims["ds_uid"] = binding.ds_instance_uid
            claims["ds_epoch"] = binding.protocol_epoch
            claims["ds_gen"] = binding.credential_gen
            claims["ds_credential_jti"] = binding.credential_jti
            claims["hub_assignment_id"] = binding.hub_assignment_id
            claims["ds_writer_epoch"] = binding.writer_epoch
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
