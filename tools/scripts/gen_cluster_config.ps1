# Pandora 集群版配置生成器
#
# 把各服务的 etc/<svc>-dev.yaml(地址都是 127.0.0.1)转换成「集群版」配置:
# mysql/redis/kafka/etcd 与同伴服务的地址改成容器/Service 短名,allocator 的
# mode: "local"(本机 exec DS)改成 "mock"(容器内无 PandoraServer.exe)。
#
# 同一份产物 docker 与 k8s 共用:
#   - docker-compose.services.yml 里服务名 = mysql/redis/kafka/etcd/login/...
#   - k8s 同 namespace 内 Service 短名 = mysql/redis/kafka/etcd/login/...
# 两边都能用短名解析,所以生成的 endpoint 一致。
#
# 用法:
#   pwsh tools/scripts/gen_cluster_config.ps1                                  # 生成到 run/cluster/etc(allocator=mock)
#   pwsh tools/scripts/gen_cluster_config.ps1 -OutDir <dir>                     # 自定义输出目录
#   pwsh tools/scripts/gen_cluster_config.ps1 -AllocatorMode agones            # 线上/Agones 链路:真 Linux DS
#   pwsh tools/scripts/gen_cluster_config.ps1 -AllocatorMode agones -AllocatorAdvertiseHost 127.0.0.1
#                                                                              # 本地 minikube(docker driver)+ udp-relay 回程
#   pwsh tools/scripts/gen_cluster_config.ps1 -HostAllocators                  # 混合模式:容器服务回连宿主 allocator
#   pwsh tools/scripts/gen_cluster_config.ps1 -AllocatorMode agones -Prod -Secret <玩家面密钥> -DsSecret <DS回调面密钥>
#                                                                              # 生产还必须注入五把 placement key + 独立内部 RPC service keys
#
# ⚠️ 安全(§5 审核):生产判定**只看 -Prod**(不再从 -AllocatorAdvertiseHost 推断,避免线上配了
#    advertise host 就被误判为 dev 而放行公开 dev 密钥)。-Prod 时必须提供**两把**真密钥:
#    玩家面 -Secret(login/hub/matchmaker jwt + envoy JWKS)与 DS 回调面 -DsSecret(ds_auth),
#    各自非空 / ≠dev / ≥32B、且**彼此不同**(P0 审核:同一密钥覆盖玩家 JWT 与 DS 回调 = 泄露即互通);
#    也可分别用环境变量 PANDORA_JWT_SECRET / PANDORA_DS_JWT_SECRET。注入后在 <OutDir>/envoy-jwks.json
#    产出匹配玩家面密钥的 Envoy JWKS + 校验 committed envoy.yaml。
#    placement 另需 account-bootstrap / match-start / battle-exit / hub-transfer /
#    battle-departure 五把独立 ≥32B key，
#    对应 PANDORA_PLACEMENT_*_SECRET；Login→Matchmaker 另用 PANDORA_MATCH_RESUME_AUTH_SECRET；
#    Team→Matchmaker 读同一个方法、但必须是另一把 PANDORA_TEAM_RESUME_AUTH_SECRET；
#    Team→Login 批量解析 player_no 另用 PANDORA_PLAYER_NO_RESOLVE_AUTH_SECRET；
#    Team→Player 批量解析 nickname 另用 PANDORA_PLAYER_NAME_RESOLVE_AUTH_SECRET；
#    Matchmaker→DS allocator 销毁未入场分配另用 PANDORA_ALLOCATION_ABORT_AUTH_SECRET；
#    生成器拒绝全部权限域之间的密钥复用。
#    owner 权威库另需 -OwnerStoreDsn / PANDORA_OWNER_TIDB_DSN(真 TiDB DSN,pandora_owner 库,
#    §9.22 确认写不回滚;拒绝 dev 凭据 / dev mysql 地址),并机械翻转 require_tidb: true +
#    全服务 enable_reflection: false(owner 启动强校验 VERSION() 含 -TiDB-,双层防线)。
#
# 三条链路与 allocator 模式的对应(由 start.ps1 驱动):
#   本地 windows (-Mode local)  → dev yaml 原样 mode=local,不过本生成器(宿主 exec Windows DS)
#   docker        (-Mode docker) → -AllocatorMode mock  (容器内无真 DS,假地址只测后端链路)
#   battle       (-Mode battle) → -AllocatorMode mock -HostAllocators(19 容器 + 2 宿主 allocator)
#   线上 k8s     (-Mode online) → -AllocatorMode agones(GameServer status.address 直连真 Linux DS)
#   本地 k8s     (-Mode k8s)    → -AllocatorMode agones -AllocatorAdvertiseHost 127.0.0.1 + udp_relay.ps1

[CmdletBinding()]
param(
    [string]$OutDir,
    [ValidateSet('mock', 'agones')]
    [string]$AllocatorMode = 'mock',
    [string]$AllocatorAdvertiseHost = '',
    # 混合(含战斗)模式:ds_allocator / hub_allocator 跑在宿主(要 exec Windows DS),
    # 不进容器。容器里的 matchmaker/login/battle_result 需经 host.docker.internal 回连宿主 allocator。
    [switch]$HostAllocators,
    # 玩家面 HS256 密钥(login jwt / hub_allocator jwt / matchmaker jwt / envoy JWKS 同一把,
    # 客户端 SessionToken / DSTicket 用它签验)。默认取环境变量,便于 CI/CD 注入真密钥而不落盘。
    [string]$Secret = $env:PANDORA_JWT_SECRET,
    # DS 回调面 HS256 密钥(ds_auth.secret:ds_allocator / hub_allocator / battle_result /
    # player_locator 校验 DS→后端回调令牌)。**必须与玩家面 -Secret 不同**——两把同值时,泄露玩家
    # 面密钥即可伪造 DS 回调令牌绕过范围绑定(审核 P0:生产不得用同一密钥覆盖玩家 JWT 与 DS 回调)。
    [string]$DsSecret = $env:PANDORA_DS_JWT_SECRET,
    # 版本化 placement 的五个写/物理离场权限域必须使用彼此独立的 HMAC key。生产从 Secret
    # manager 注入；生成产物只把每个 writer 所需的子集写进 pandora-config Secret。
    [string]$PlacementAccountBootstrapSecret = $env:PANDORA_PLACEMENT_ACCOUNT_BOOTSTRAP_SECRET,
    [string]$PlacementMatchStartSecret = $env:PANDORA_PLACEMENT_MATCH_START_SECRET,
    [string]$PlacementBattleExitSecret = $env:PANDORA_PLACEMENT_BATTLE_EXIT_SECRET,
    [string]$PlacementHubTransferSecret = $env:PANDORA_PLACEMENT_HUB_TRANSFER_SECRET,
    [string]$PlacementBattleDepartureSecret = $env:PANDORA_PLACEMENT_BATTLE_DEPARTURE_SECRET,
    # Login→Matchmaker ResolvePlayerMatchContext 的唯一服务身份 key。请求 HMAC 绑定
    # method/player/timestamp/nonce，Matchmaker 用共享 Redis SETNX 防跨副本重放。
    [string]$MatchResumeAuthSecret = $env:PANDORA_MATCH_RESUME_AUTH_SECRET,
    # Team→Matchmaker 读同一个 ResolvePlayerMatchContext 的独立服务身份 key(caller="team")。
    # 必须与 Login 那把不同:Matchmaker 侧按 caller 分别持有独立密钥，共用会把两个服务的信任域
    # 合并(任一方可冒充另一方)、且轮换变成全有全无。漏配 → team 入队闸门 fail-closed 拒绝入队、
    # 招募列表恒空，因此 -Prod 必填。
    [string]$TeamResumeAuthSecret = $env:PANDORA_TEAM_RESUME_AUTH_SECRET,
    # Team→Login ResolvePlayerNos 的独立服务身份 key。签名端 team 与验签端 login 必须同值，
    # 且不得复用 JWT、DS callback、placement、resume 或 allocation-abort 权限域。
    [string]$PlayerNoResolveAuthSecret = $env:PANDORA_PLAYER_NO_RESOLVE_AUTH_SECRET,
    # Team→Player ResolvePlayerNames 的独立服务身份 key。签名端 team 与验签端 player 必须同值，
    # 且不得复用 player-no、JWT、DS callback、placement、resume 或 allocation-abort 权限域。
    [string]$PlayerNameResolveAuthSecret = $env:PANDORA_PLAYER_NAME_RESOLVE_AUTH_SECRET,
    # Friend/Guild 申请列表使用各自 caller 与独立 key，不能冒充 team 或彼此。
    [string]$FriendPlayerNameResolveAuthSecret = $env:PANDORA_FRIEND_PLAYER_NAME_RESOLVE_AUTH_SECRET,
    [string]$FriendPlayerNoResolveAuthSecret = $env:PANDORA_FRIEND_PLAYER_NO_RESOLVE_AUTH_SECRET,
    [string]$GuildPlayerNameResolveAuthSecret = $env:PANDORA_GUILD_PLAYER_NAME_RESOLVE_AUTH_SECRET,
    [string]$GuildPlayerNoResolveAuthSecret = $env:PANDORA_GUILD_PLAYER_NO_RESOLVE_AUTH_SECRET,
    # Matchmaker(PVP/PVE)→DS allocator AbortPreactiveBattle 的独立 payload-bound HMAC。
    # 该权限可物理删除精确 GameServer，绝不能与 JWT、DS callback、placement 或 resume 复用。
    [string]$AllocationAbortAuthSecret = $env:PANDORA_ALLOCATION_ABORT_AUTH_SECRET,
    # 版本化 placement rollout：生产默认 enforce；shadow 只用于先服务端后客户端的短期灰度。
    [ValidateSet('', 'off', 'shadow', 'enforce')]
    [string]$PlacementMode = '',
    # 玩家面轮换兼容密钥(可选,仅非生产验证):写进 login/hub/matchmaker 的
    # jwt.additional_secrets(仅验签不签发)并进 Envoy JWKS 第二把 key。阶段①它是待启用的新 key，
    # 阶段②它是待清退的旧 key，不能固定理解成“旧密钥”。生产暂由下方待决策门拒绝。
    [string]$SecretAdditional = $env:PANDORA_JWT_SECRET_ADDITIONAL,
    # DS 回调面轮换兼容密钥(可选,仅非生产验证):写进 4 个 ds_auth 服务的 additional_secrets。
    [string]$DsSecretAdditional = $env:PANDORA_DS_JWT_SECRET_ADDITIONAL,
    # ds_auth.mode 改写(二审 A#2):-Prod 只允许 'enforce'(生产 DS 回调必须鉴权,否则
    # warming→ready 只是活性信号、任意进程可伪造心跳/战果回调)。灰度 permissive/off
    # 只能在非 Prod 环境显式测试；非 -Prod 不传则保持 dev 模板值不变。
    [ValidateSet('', 'off', 'permissive', 'enforce')]
    [string]$DsAuthMode = '',
    # Model B 授权权威。-Prod 只允许 redis；非生产默认保留模板 legacy，显式 redis 时也必须
    # 同时提供 fence etcd/keyset revision，生成器会原子改写四个回调服务 + login binding。
    [ValidateSet('', 'legacy', 'redis')]
    [string]$DsAuthorityMode = '',
    [string]$DsFenceEtcdEndpoints = $env:PANDORA_DS_AUTH_FENCE_ETCD_ENDPOINTS,
    [string]$DsFenceKeysetRevision = $env:PANDORA_DS_AUTH_KEYSET_REVISION,
    # DSTicket v2(方案 B,RS256 非对称,decision-revisit-player-jwt-key-rotation.md §7)签发接线:
    # agones 链路(真 Linux DS,DS 侧只认 v2 票)必填 active kid(RFC 7638 指纹,43 字符 base64url,
    # 取自 tools/dsticketkeys 生成的 jwks.json)。生成器把 ds_ticket 段注入 4 个签发方
    # (login/matchmaker/matchmaker-pve/hub-allocator);私钥经 revisioned K8s Secret
    # pandora-dsticket-signer-rN 挂载到稳定容器路径
    # /run/secrets/pandora-dsticket/private.pem(见 deploy/k8s/services/services.yaml)。
    [string]$DsTicketActiveKid = $env:PANDORA_DSTICKET_ACTIVE_KID,
    # 公钥 JWKS 不可变 ConfigMap 的 revision。Login 的 VerifyDSTicket 诊断/兼容路径与
    # DS 挂载同一份公开 keyset 内容，并同时校验 revision + explicit active_kid。
    [string]$DsTicketKeysetRevision = $env:PANDORA_DSTICKET_KEYSET_REVISION,
    # DSTicket v2 票据 TTL(默认 120s;机械上限 180s,UE 验票侧同样强制 exp-iat ≤ 180s)。
    [string]$DsTicketTTL = '120s',
    # owner 权威库 DSN(§9.22 确认写不回滚):-Prod 必须显式提供真 TiDB DSN(pandora_owner 库);
    # dev 模板连单机 MySQL(无复制天然线性一致)只服务本地联调,MySQL 异步复制主从切换会回滚
    # 已确认写,owner CAS 回滚即可能双 owner,生产禁用。非 -Prod 也可显式覆盖(本地连真 TiDB 测)。
    [string]$OwnerStoreDsn = $env:PANDORA_OWNER_TIDB_DSN,
    # login 账号库 DSN(全服单点扩容,2026-07-27):pandora_account 是**全国共享的单点**——
    # 每次登录都要在 player_session_generations 上跑一个定序事务,写 QPS = 全服登录 QPS,
    # login 无状态可水平扩但这个库摊不了;该库不可用 = 全国 100% 玩家进不去。
    # -Prod 必须显式提供真 TiDB DSN(pandora_account 库),校验规则与 owner 同构。
    [string]$AccountStoreDsn = $env:PANDORA_ACCOUNT_TIDB_DSN,
    # Stable/Canary DS 轨道：百分比按服务端确定性 cohort 分桶，seed 是发布配置而非密钥，
    # 但启用灰度后必须稳定不漂移；普通发布与两条 Fleet 共用同一 DSTicket keyset。
    [ValidateRange(0, 100)][int]$BattleCanaryPercent = 0,
    [ValidateRange(0, 100)][int]$HubCanaryPercent = 0,
    [string]$CanarySeed = $env:PANDORA_DS_CANARY_SEED,
    # -Prod:显式声明「这是生产/线上产物」。**唯一**的生产判定信号(不再从 advertise host 推断,
    # 避免线上设了 advertise host 就被误判为 dev 而放行公开 dev 密钥,§5 安全审核)。
    # 生产模式强制:必须提供两把真密钥(玩家面 -Secret + DS 回调面 -DsSecret,各自非空、≠dev、
    # ≥32B、且彼此不同),否则拒绝生成;并同步产出匹配的 Envoy JWKS。
    [switch]$Prod,
    # -AllowDevSecrets:显式声明「这是本地/开发链路,允许写入公开 dev 密钥」。agones 模式(真 Linux DS)
    # 不带 -Prod 时,必须显式加本开关才放行 dev 密钥(审核 P1:不再用「存在任意 advertise host」推断本地,
    # 生产 IP/DNS 也会配 advertise host,那样推断会让生产绕过 -Prod 写入 dev 密钥)。仅供本地 minikube 自测。
    [switch]$AllowDevSecrets
)

# 两个公开 dev 密钥只供本机 / docker-mock 链路,绝不能进生产产物。即使是 dev，
# 玩家 Session 与 DS callback 也必须保持不同 keyset，才能让 Model-B 的域隔离门禁真实生效。
$DevPublicSecret = 'pandora-dev-jwt-secret-change-me-32!'
$DevDsCallbackSecret = 'pandora-dev-ds-callback-secret-change-me-32!'
$DevPlacementSecrets = [ordered]@{
    AccountBootstrap = 'pandora-dev-placement-bootstrap-key-v1!'
    MatchStart       = 'pandora-dev-placement-match-start-key-v1!'
    BattleExit       = 'pandora-dev-placement-battle-exit-key-v1!'
    HubTransfer      = 'pandora-dev-placement-hub-transfer-key-v1!'
    BattleDeparture  = 'pandora-dev-placement-battle-departure-key-v1!'
}
$DevMatchResumeAuthSecret = 'pandora-dev-match-resume-auth-key-v1!'
$DevTeamResumeAuthSecret = 'pandora-dev-team-resume-auth-key-v1!'
$DevPlayerNoResolveAuthSecret = 'pandora-dev-team-player-no-auth-key-v1!'
$DevPlayerNameResolveAuthSecret = 'pandora-dev-team-player-name-auth-key-v1!'
$DevFriendPlayerNameResolveAuthSecret = 'pandora-dev-friend-player-name-auth-key-v1!'
$DevFriendPlayerNoResolveAuthSecret = 'pandora-dev-friend-player-no-auth-key-v1!'
$DevGuildPlayerNameResolveAuthSecret = 'pandora-dev-guild-player-name-auth-key-v1!'
$DevGuildPlayerNoResolveAuthSecret = 'pandora-dev-guild-player-no-auth-key-v1!'
$DevAllocationAbortAuthSecret = 'pandora-dev-allocation-abort-auth-key-v1!'


$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../..").Path
if (-not $OutDir) { $OutDir = Join-Path $ProjectRoot 'run/cluster/etc' }
$OutDir = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($OutDir)
$OutDir = [System.IO.Path]::TrimEndingDirectorySeparator($OutDir)

if (($BattleCanaryPercent -gt 0 -or $HubCanaryPercent -gt 0) -and
    ($CanarySeed -cnotmatch '^[A-Za-z0-9._-]{8,128}$')) {
    throw '[FATAL] 启用 DS Canary 时必须提供 8..128 字符稳定 -CanarySeed / PANDORA_DS_CANARY_SEED(仅字母数字._-)。'
}
if ($AllocatorMode -ne 'agones' -and ($BattleCanaryPercent -ne 0 -or $HubCanaryPercent -ne 0)) {
    throw '[FATAL] Stable/Canary Fleet 分流仅适用于 -AllocatorMode agones。'
}

# ===== 密钥策略(§5 + P0 安全审核:禁止把公开 dev 密钥发到生产;玩家面 / DS 回调面必须分离)=====
# 生产判定**只看 -Prod**(不再从 -AllocatorAdvertiseHost 推断:真线上完全可能配 advertise host,
# 那样推断会把生产误判为 dev 而放行 dev 密钥)。
#   -Prod → 必须注入两把独立真密钥:
#            玩家面 $PlayerSecretToInject(login/hub/matchmaker jwt + envoy JWKS)
#            DS 回调面 $DsSecretToInject(ds_auth)
#          两把各自非空 / ≠dev / ≥32B、且**彼此不同**(P0:同值时泄露玩家面即可伪造 DS 回调令牌)。
#   非 -Prod(dev/mock/minikube) → 默认沿用 dev 密钥;也可分别用 -Secret / -DsSecret 覆盖成真密钥(便于本地测)。
$PlayerSecretToInject = $null
$DsSecretToInject = $null

# 校验单把密钥的强度(生产:非空 + ≠dev + ≥32B)。返回校验后的密钥。
function Assert-ProdSecret([string]$val, [string]$flag, [string]$envName) {
    if ([string]::IsNullOrWhiteSpace($val)) {
        throw "[FATAL] -Prod 生产模式必须提供 $flag:传 $flag 或设环境变量 $envName。" +
              ' 拒绝把公开 dev 密钥写进生产配置(§5/P0 安全审核)。'
    }
    if ($val -eq $DevPublicSecret -or $val -eq $DevDsCallbackSecret) {
        throw "[FATAL] -Prod 生产模式的 $flag 不能等于公开 dev 密钥。请换成 CI/CD 注入的真密钥。"
    }
    if ([System.Text.Encoding]::UTF8.GetByteCount($val) -lt 32) {
        throw "[FATAL] $flag 至少需要 32 字节(HS256),当前长度不足。"
    }
    # 拒绝控制字符(审核 P1 #8):密钥会被注入双引号 YAML 字符串。换行 / 制表 / 其它控制字符
    # 多半是误带(如 $(cat secret) 的尾部换行),静默转义会掩盖错误 → 直接拒绝并要求清理。
    if ($val -match '[\x00-\x1F\x7F-\x9F]') {
        throw "[FATAL] $flag 含控制字符(换行 / 制表 / NUL 等),多为误带的尾部空白。请清理后再注入生产配置。"
    }
    return $val
}

if ($Prod) {
    $PlayerSecretToInject = Assert-ProdSecret $Secret '-Secret(玩家面)' 'PANDORA_JWT_SECRET'
    $DsSecretToInject = Assert-ProdSecret $DsSecret '-DsSecret(DS 回调面)' 'PANDORA_DS_JWT_SECRET'
    if ($PlayerSecretToInject -eq $DsSecretToInject) {
        throw "[FATAL] -Secret(玩家面)与 -DsSecret(DS 回调面)不得相同(P0 审核:同一密钥覆盖玩家 JWT" +
              " 与 DS 回调令牌 —— 泄露玩家面密钥即可伪造 DS 回调令牌绕过范围绑定)。请用两把独立真密钥。"
    }
} else {
    # 非生产也允许分别显式覆盖(便于本地测真密钥),但不强制;≥32B 仍要求。
    if (-not [string]::IsNullOrWhiteSpace($Secret) -and $Secret -ne $DevPublicSecret) {
        if ([System.Text.Encoding]::UTF8.GetByteCount($Secret) -lt 32) { throw "[FATAL] -Secret 至少需要 32 字节(HS256)。" }
        if ($Secret -match '[\x00-\x1F\x7F-\x9F]') { throw '[FATAL] -Secret 含 C0/C1 控制字符,不能安全写入 YAML。' }
        $PlayerSecretToInject = $Secret
    }
    if (-not [string]::IsNullOrWhiteSpace($DsSecret) -and $DsSecret -ne $DevDsCallbackSecret) {
        if ([System.Text.Encoding]::UTF8.GetByteCount($DsSecret) -lt 32) { throw "[FATAL] -DsSecret 至少需要 32 字节(HS256)。" }
        if ($DsSecret -match '[\x00-\x1F\x7F-\x9F]') { throw '[FATAL] -DsSecret 含 C0/C1 控制字符,不能安全写入 YAML。' }
        $DsSecretToInject = $DsSecret
    }
}

# ===== 轮换旧密钥(additional_secrets,可选)=====
# 只在对应主密钥已注入真密钥时才允许(dev 主密钥 + 真 additional 是配置事故);与主密钥同标准校验,
# 且四把密钥(玩家主/玩家旧/DS 主/DS 旧)两两不同 —— 跨面交叉 = 泄露一面即可伪造另一面(P0)。
$PlayerAdditionalToInject = $null
$DsAdditionalToInject = $null
function Assert-AdditionalSecret([string]$val, [string]$flag, [string]$primaryVal, [string]$primaryFlag) {
    if ($null -eq $primaryVal) {
        throw "[FATAL] 提供了 $flag 但对应主密钥 $primaryFlag 未注入真密钥;轮换旧密钥只能与真主密钥搭配。"
    }
    if ($val -eq $DevPublicSecret -or $val -eq $DevDsCallbackSecret) { throw "[FATAL] $flag 不能等于公开 dev 密钥。" }
    if ([System.Text.Encoding]::UTF8.GetByteCount($val) -lt 32) { throw "[FATAL] $flag 至少需要 32 字节(HS256)。" }
    if ($val -match '[\x00-\x1F\x7F-\x9F]') { throw "[FATAL] $flag 含 C0/C1 控制字符,多为误带的尾部空白,已拒绝。" }
    return $val
}
if (-not [string]::IsNullOrWhiteSpace($SecretAdditional)) {
    $PlayerAdditionalToInject = Assert-AdditionalSecret $SecretAdditional '-SecretAdditional(玩家面兼容密钥)' $PlayerSecretToInject '-Secret'
}
if (-not [string]::IsNullOrWhiteSpace($DsSecretAdditional)) {
    $DsAdditionalToInject = Assert-AdditionalSecret $DsSecretAdditional '-DsSecretAdditional(DS 回调面兼容密钥)' $DsSecretToInject '-DsSecret'
}
# 两份轮换决策仍为“待人拍板”。允许非生产验证现有部分接线，但生产生成必须 fail-closed，
# 不能在 Edge/阶段流程与权威 key-set gate 尚未批准时把 additional 带入线上产物。
if ($Prod -and ($null -ne $PlayerAdditionalToInject -or $null -ne $DsAdditionalToInject)) {
    throw '[FATAL] -Prod 暂不允许玩家面/DS 面 additional_secrets：轮换决策仍待人拍板，现有代码仅可非生产验证。' +
          '请先批准并更新 decision-revisit-player-jwt-key-rotation.md / decision-revisit-ds-key-rotation.md，再移除此生产门。'
}
$effectivePlayerPrimary = if ($null -ne $PlayerSecretToInject) { $PlayerSecretToInject } else { $DevPublicSecret }
$effectiveDsPrimary = if ($null -ne $DsSecretToInject) { $DsSecretToInject } else { $DevDsCallbackSecret }
$allEffective = @(
    @{ n = '玩家面 primary';                  v = $effectivePlayerPrimary },
    @{ n = '-SecretAdditional(玩家面兼容)';    v = $PlayerAdditionalToInject },
    @{ n = 'DS 回调面 primary';                v = $effectiveDsPrimary },
    @{ n = '-DsSecretAdditional(DS 回调面兼容)'; v = $DsAdditionalToInject }
) | Where-Object { $null -ne $_.v }
for ($i = 0; $i -lt $allEffective.Count; $i++) {
    for ($j = $i + 1; $j -lt $allEffective.Count; $j++) {
        if ($allEffective[$i].v -ceq $allEffective[$j].v) {
            throw "[FATAL] $($allEffective[$i].n) 与 $($allEffective[$j].n) 不得相同(P0:玩家面/DS 回调面/新旧轮换密钥必须各自独立)。"
        }
    }
}

# ===== placement proof key 分权 =====
# 五个权限域的 writer 只拿各自 key；locator 作为唯一 verifier 拿五把。生产不允许缺 key、公开 dev key、
# 跨域复用或与玩家/DS callback key 复用。非生产未提供时使用确定性的公开 dev key。
function Resolve-PlacementSecret {
    param(
        [string]$Value,
        [string]$DevValue,
        [string]$Flag,
        [string]$EnvName
    )
    if ([string]::IsNullOrWhiteSpace($Value)) {
        if ($Prod) {
            throw "[FATAL] -Prod 必须提供 $Flag 或环境变量 $EnvName；placement writer 不允许无 proof key 启动。"
        }
        return $DevValue
    }
    if ([System.Text.Encoding]::UTF8.GetByteCount($Value) -lt 32) {
        throw "[FATAL] $Flag 至少需要 32 字节。"
    }
    if ($Value -match '[\x00-\x1F\x7F-\x9F]') {
        throw "[FATAL] $Flag 含控制字符，拒绝写入 YAML。"
    }
    if ($Prod -and ($DevPlacementSecrets.Values -contains $Value -or
        $Value -ceq $DevPublicSecret -or $Value -ceq $DevDsCallbackSecret)) {
        throw "[FATAL] $Flag 不能使用仓库公开 dev key。"
    }
    return $Value
}

$EffectivePlacementSecrets = [ordered]@{
    AccountBootstrap = Resolve-PlacementSecret $PlacementAccountBootstrapSecret $DevPlacementSecrets.AccountBootstrap `
        '-PlacementAccountBootstrapSecret' 'PANDORA_PLACEMENT_ACCOUNT_BOOTSTRAP_SECRET'
    MatchStart = Resolve-PlacementSecret $PlacementMatchStartSecret $DevPlacementSecrets.MatchStart `
        '-PlacementMatchStartSecret' 'PANDORA_PLACEMENT_MATCH_START_SECRET'
    BattleExit = Resolve-PlacementSecret $PlacementBattleExitSecret $DevPlacementSecrets.BattleExit `
        '-PlacementBattleExitSecret' 'PANDORA_PLACEMENT_BATTLE_EXIT_SECRET'
    HubTransfer = Resolve-PlacementSecret $PlacementHubTransferSecret $DevPlacementSecrets.HubTransfer `
        '-PlacementHubTransferSecret' 'PANDORA_PLACEMENT_HUB_TRANSFER_SECRET'
    BattleDeparture = Resolve-PlacementSecret $PlacementBattleDepartureSecret $DevPlacementSecrets.BattleDeparture `
        '-PlacementBattleDepartureSecret' 'PANDORA_PLACEMENT_BATTLE_DEPARTURE_SECRET'
}
$EffectiveMatchResumeAuthSecret = if ([string]::IsNullOrWhiteSpace($MatchResumeAuthSecret)) {
    if ($Prod) {
        throw '[FATAL] -Prod 必须提供 -MatchResumeAuthSecret 或 PANDORA_MATCH_RESUME_AUTH_SECRET；内部 READY 凭据读取不得匿名开放。'
    }
    $DevMatchResumeAuthSecret
} else {
    if ([System.Text.Encoding]::UTF8.GetByteCount($MatchResumeAuthSecret) -lt 32) {
        throw '[FATAL] -MatchResumeAuthSecret 至少需要 32 字节。'
    }
    if ($MatchResumeAuthSecret -match '[\x00-\x1F\x7F-\x9F]') {
        throw '[FATAL] -MatchResumeAuthSecret 含控制字符，拒绝写入 YAML。'
    }
    if ($Prod -and $MatchResumeAuthSecret -ceq $DevMatchResumeAuthSecret) {
        throw '[FATAL] -Prod 的 Match resume service key 不能使用仓库公开 dev key。'
    }
    $MatchResumeAuthSecret
}
$EffectiveTeamResumeAuthSecret = if ([string]::IsNullOrWhiteSpace($TeamResumeAuthSecret)) {
    if ($Prod) {
        throw '[FATAL] -Prod 必须提供 -TeamResumeAuthSecret 或 PANDORA_TEAM_RESUME_AUTH_SECRET；漏配会让 team 入队闸门 fail-closed 、招募列表恒空。'
    }
    $DevTeamResumeAuthSecret
} else {
    if ([System.Text.Encoding]::UTF8.GetByteCount($TeamResumeAuthSecret) -lt 32) {
        throw '[FATAL] -TeamResumeAuthSecret 至少需要 32 字节。'
    }
    if ($TeamResumeAuthSecret -match '[\x00-\x1F\x7F-\x9F]') {
        throw '[FATAL] -TeamResumeAuthSecret 含控制字符，拒绝写入 YAML。'
    }
    if ($Prod -and $TeamResumeAuthSecret -ceq $DevTeamResumeAuthSecret) {
        throw '[FATAL] -Prod 的 Team resume service key 不能使用仓库公开 dev key。'
    }
    $TeamResumeAuthSecret
}
$EffectivePlayerNoResolveAuthSecret = if ([string]::IsNullOrWhiteSpace($PlayerNoResolveAuthSecret)) {
    if ($Prod) {
        throw '[FATAL] -Prod 必须提供 -PlayerNoResolveAuthSecret 或 PANDORA_PLAYER_NO_RESOLVE_AUTH_SECRET；Team→Login 玩家编号解析不得携带公开 dev key。'
    }
    $DevPlayerNoResolveAuthSecret
} else {
    if ([System.Text.Encoding]::UTF8.GetByteCount($PlayerNoResolveAuthSecret) -lt 32) {
        throw '[FATAL] -PlayerNoResolveAuthSecret 至少需要 32 字节。'
    }
    if ($PlayerNoResolveAuthSecret -match '[\x00-\x1F\x7F-\x9F]') {
        throw '[FATAL] -PlayerNoResolveAuthSecret 含控制字符，拒绝写入 YAML。'
    }
    if ($Prod -and $PlayerNoResolveAuthSecret -ceq $DevPlayerNoResolveAuthSecret) {
        throw '[FATAL] -Prod 的 player-no resolve service key 不能使用仓库公开 dev key。'
    }
    $PlayerNoResolveAuthSecret
}

function Resolve-DisplayProjectionSecret {
    param(
        [string]$Value,
        [string]$DevValue,
        [string]$Flag,
        [string]$EnvName,
        [string]$Description
    )
    if ([string]::IsNullOrWhiteSpace($Value)) {
        if ($Prod) {
            throw "[FATAL] -Prod 必须提供 $Flag 或 $EnvName；$Description 不得携带公开 dev key。"
        }
        return $DevValue
    }
    if ([System.Text.Encoding]::UTF8.GetByteCount($Value) -lt 32) {
        throw "[FATAL] $Flag 至少需要 32 字节。"
    }
    if ($Value -match '[\x00-\x1F\x7F-\x9F]') {
        throw "[FATAL] $Flag 含控制字符，拒绝写入 YAML。"
    }
    if ($Prod -and $Value -ceq $DevValue) {
        throw "[FATAL] -Prod 的 $Description service key 不能使用仓库公开 dev key。"
    }
    return $Value
}
$EffectivePlayerNameResolveAuthSecret = if ([string]::IsNullOrWhiteSpace($PlayerNameResolveAuthSecret)) {
    if ($Prod) {
        throw '[FATAL] -Prod 必须提供 -PlayerNameResolveAuthSecret 或 PANDORA_PLAYER_NAME_RESOLVE_AUTH_SECRET；Team→Player 玩家名字解析不得携带公开 dev key。'
    }
    $DevPlayerNameResolveAuthSecret
} else {
    if ([System.Text.Encoding]::UTF8.GetByteCount($PlayerNameResolveAuthSecret) -lt 32) {
        throw '[FATAL] -PlayerNameResolveAuthSecret 至少需要 32 字节。'
    }
    if ($PlayerNameResolveAuthSecret -match '[\x00-\x1F\x7F-\x9F]') {
        throw '[FATAL] -PlayerNameResolveAuthSecret 含控制字符，拒绝写入 YAML。'
    }
    if ($Prod -and $PlayerNameResolveAuthSecret -ceq $DevPlayerNameResolveAuthSecret) {
        throw '[FATAL] -Prod 的 player-name resolve service key 不能使用仓库公开 dev key。'
    }
    $PlayerNameResolveAuthSecret
}
$EffectiveFriendPlayerNameResolveAuthSecret = Resolve-DisplayProjectionSecret `
    $FriendPlayerNameResolveAuthSecret $DevFriendPlayerNameResolveAuthSecret `
    '-FriendPlayerNameResolveAuthSecret' 'PANDORA_FRIEND_PLAYER_NAME_RESOLVE_AUTH_SECRET' 'Friend→Player name resolve'
$EffectiveFriendPlayerNoResolveAuthSecret = Resolve-DisplayProjectionSecret `
    $FriendPlayerNoResolveAuthSecret $DevFriendPlayerNoResolveAuthSecret `
    '-FriendPlayerNoResolveAuthSecret' 'PANDORA_FRIEND_PLAYER_NO_RESOLVE_AUTH_SECRET' 'Friend→Login player-no resolve'
$EffectiveGuildPlayerNameResolveAuthSecret = Resolve-DisplayProjectionSecret `
    $GuildPlayerNameResolveAuthSecret $DevGuildPlayerNameResolveAuthSecret `
    '-GuildPlayerNameResolveAuthSecret' 'PANDORA_GUILD_PLAYER_NAME_RESOLVE_AUTH_SECRET' 'Guild→Player name resolve'
$EffectiveGuildPlayerNoResolveAuthSecret = Resolve-DisplayProjectionSecret `
    $GuildPlayerNoResolveAuthSecret $DevGuildPlayerNoResolveAuthSecret `
    '-GuildPlayerNoResolveAuthSecret' 'PANDORA_GUILD_PLAYER_NO_RESOLVE_AUTH_SECRET' 'Guild→Login player-no resolve'
$EffectiveAllocationAbortAuthSecret = if ([string]::IsNullOrWhiteSpace($AllocationAbortAuthSecret)) {
    if ($Prod) {
        throw '[FATAL] -Prod 必须提供 -AllocationAbortAuthSecret 或 PANDORA_ALLOCATION_ABORT_AUTH_SECRET；未入场 GameServer 销毁 RPC 不得使用公开 dev key。'
    }
    $DevAllocationAbortAuthSecret
} else {
    if ([System.Text.Encoding]::UTF8.GetByteCount($AllocationAbortAuthSecret) -lt 32) {
        throw '[FATAL] -AllocationAbortAuthSecret 至少需要 32 字节。'
    }
    if ($AllocationAbortAuthSecret -match '[\x00-\x1F\x7F-\x9F]') {
        throw '[FATAL] -AllocationAbortAuthSecret 含控制字符，拒绝写入 YAML。'
    }
    if ($Prod -and $AllocationAbortAuthSecret -ceq $DevAllocationAbortAuthSecret) {
        throw '[FATAL] -Prod 的 allocation abort service key 不能使用仓库公开 dev key。'
    }
    $AllocationAbortAuthSecret
}
$allAuthoritySecrets = @(
    @{ n = '玩家面 primary'; v = $effectivePlayerPrimary },
    @{ n = '玩家面 additional'; v = $PlayerAdditionalToInject },
    @{ n = 'DS 回调面 primary'; v = $effectiveDsPrimary },
    @{ n = 'DS 回调面 additional'; v = $DsAdditionalToInject },
    @{ n = 'placement account bootstrap'; v = $EffectivePlacementSecrets.AccountBootstrap },
    @{ n = 'placement match start'; v = $EffectivePlacementSecrets.MatchStart },
    @{ n = 'placement battle exit'; v = $EffectivePlacementSecrets.BattleExit },
    @{ n = 'placement hub transfer'; v = $EffectivePlacementSecrets.HubTransfer },
    @{ n = 'placement battle departure'; v = $EffectivePlacementSecrets.BattleDeparture },
    @{ n = 'Match resume service identity'; v = $EffectiveMatchResumeAuthSecret },
    @{ n = 'Team resume service identity'; v = $EffectiveTeamResumeAuthSecret },
    @{ n = 'Team to Login player-no resolve service identity'; v = $EffectivePlayerNoResolveAuthSecret },
    @{ n = 'Team to Player player-name resolve service identity'; v = $EffectivePlayerNameResolveAuthSecret },
    @{ n = 'Friend to Player player-name resolve service identity'; v = $EffectiveFriendPlayerNameResolveAuthSecret },
    @{ n = 'Friend to Login player-no resolve service identity'; v = $EffectiveFriendPlayerNoResolveAuthSecret },
    @{ n = 'Guild to Player player-name resolve service identity'; v = $EffectiveGuildPlayerNameResolveAuthSecret },
    @{ n = 'Guild to Login player-no resolve service identity'; v = $EffectiveGuildPlayerNoResolveAuthSecret },
    @{ n = 'allocation abort service identity'; v = $EffectiveAllocationAbortAuthSecret }
) | Where-Object { $null -ne $_.v }
for ($i = 0; $i -lt $allAuthoritySecrets.Count; $i++) {
    for ($j = $i + 1; $j -lt $allAuthoritySecrets.Count; $j++) {
        if ($allAuthoritySecrets[$i].v -ceq $allAuthoritySecrets[$j].v) {
            throw "[FATAL] $($allAuthoritySecrets[$i].n) 与 $($allAuthoritySecrets[$j].n) 不得复用同一密钥。"
        }
    }
}

# 生产默认严格门；shadow 只允许显式短期灰度，off 永不允许生产。真实 Agones 的本地
# 验证链默认同样 enforce；mock/local 模板保持 off，除非调用方显式要求。
$PlacementModeToInject = $null
if ($Prod) {
    if ($PlacementMode -eq 'off') {
        throw '[FATAL] -Prod 不允许 placement_mode=off。使用 shadow 做短期兼容观测，完成服务端发布后切 enforce。'
    }
    $PlacementModeToInject = if ([string]::IsNullOrWhiteSpace($PlacementMode)) { 'enforce' } else { $PlacementMode }
    if ($PlacementModeToInject -eq 'shadow') {
        Write-Host '[WARN] 生产 placement_mode=shadow：只记录漂移，不构成唯一 DS 最终门；仅限发布顺序中的短期灰度。' -ForegroundColor Yellow
    }
} elseif (-not [string]::IsNullOrWhiteSpace($PlacementMode)) {
    $PlacementModeToInject = $PlacementMode
} elseif ($AllocatorMode -eq 'agones') {
    $PlacementModeToInject = 'enforce'
}

# ===== ds_auth.mode 改写决策(二审 A#2 + 三审 P1-3)=====
# 目标姿态:生产 DS 回调必须 enforce(否则 warming→ready 只是活性信号,任意进程可伪造心跳/战果回调)。
# 但**当前 UE DS 尚未读取 pandora.dev/ds-token、不发 Bearer**;直接对线上硬制 enforce 会让七类
# DS→后端回调全部 401(三审 P1-3 阻塞)。故 -Prod 默认 enforce,但保留**显式灰度逃生门**:
#   -DsAuthMode enforce(或不传)→ enforce(目标姿态,要求 DS 已接令牌)。
#   -DsAuthMode permissive       → permissive(仅记录不拒绝)。**DS 未接令牌期间的过渡窗口**用它:
#                                  回调照常放行、同时观测「带令牌比例」,等 DS 全量发令牌再切 enforce。
#   -DsAuthMode off              → 生产**永远拒绝**(off = 完全不校验,连观测都没有,绝不允许上线)。
# 非 -Prod:不传保持模板值;显式传了则改写(本地 minikube 测链路用)。
$DsAuthModeToInject = $null
if ($Prod) {
    if ($DsAuthMode -eq 'off') {
        throw "[FATAL] -Prod 不允许 ds_auth.mode=off(完全不校验 DS 回调,任意进程可伪造心跳/战果)。" +
              " 目标用 enforce;DS 未接令牌的过渡窗口用 permissive(仍记录并观测),绝不用 off。"
    }
    if (-not [string]::IsNullOrWhiteSpace($DsAuthMode) -and $DsAuthMode -ne 'enforce') {
        # 走到这里只可能是 permissive(off 已拒、enforce 是目标)。
        Write-Host "[WARN] ⚠️ -Prod 但 ds_auth.mode=permissive:DS 回调**不强制**鉴权,仅记录。" -ForegroundColor Yellow
        Write-Host "       仅限『UE DS 尚未发送令牌』的过渡灰度窗口使用;DS 全量带令牌后必须去掉 -DsAuthMode 回 enforce。" -ForegroundColor Yellow
        Write-Host "       permissive 期间线上任意能连到服务的进程都可伪造 Hub 心跳/战果回调,务必尽快收敛。" -ForegroundColor Yellow
        $DsAuthModeToInject = $DsAuthMode
    } else {
        $DsAuthModeToInject = 'enforce'
    }
} elseif (-not [string]::IsNullOrWhiteSpace($DsAuthMode)) {
    $DsAuthModeToInject = $DsAuthMode
}

# ===== Redis 单一授权权威 + 机械 fence 原子配置门 =====
$DsAuthorityModeToInject = $null
if ($Prod) {
    if ($DsAuthorityMode -ne 'redis') {
        throw '[FATAL] -Prod 必须显式传 -DsAuthorityMode redis；生产不再生成 legacy/K8s-first 授权配置。'
    }
    if ($DsAuthModeToInject -ne 'enforce') {
        throw '[FATAL] authority_mode=redis 只允许 ds_auth.mode=enforce；permissive/off 不能作为授权闭环。'
    }
    $DsAuthorityModeToInject = 'redis'
} elseif (-not [string]::IsNullOrWhiteSpace($DsAuthorityMode)) {
    $DsAuthorityModeToInject = $DsAuthorityMode
}

$DsFenceEndpoints = @()
if (-not [string]::IsNullOrWhiteSpace($DsFenceEtcdEndpoints)) {
    $DsFenceEndpoints = @($DsFenceEtcdEndpoints.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' })
}
if ($DsAuthorityModeToInject -eq 'redis') {
    if ($AllocatorMode -ne 'agones') { throw '[FATAL] authority_mode=redis 只允许 allocator mode=agones。' }
    if ($DsAuthModeToInject -ne 'enforce') { throw '[FATAL] authority_mode=redis 必须同时显式使用 ds_auth.mode=enforce。' }
    if ($DsFenceEndpoints.Count -eq 0) { throw '[FATAL] authority_mode=redis 必须提供 -DsFenceEtcdEndpoints。' }
    foreach ($endpoint in $DsFenceEndpoints) {
        if ($Prod) {
            $uri = $null
            if (-not [Uri]::TryCreate($endpoint, [UriKind]::Absolute, [ref]$uri) -or $uri.Scheme -cne 'https' -or
                $endpoint -cnotmatch '^https://(?:\[[0-9A-Fa-f:]+\]|[A-Za-z0-9._-]+):[1-9][0-9]{0,4}$' -or
                -not [string]::IsNullOrEmpty($uri.UserInfo) -or $uri.AbsolutePath -cne '/' -or
                -not [string]::IsNullOrEmpty($uri.Query) -or -not [string]::IsNullOrEmpty($uri.Fragment)) {
                throw "[FATAL] Prod fence etcd endpoint 必须是 canonical https://host:port:$endpoint"
            }
        } elseif ($endpoint -cnotmatch '^[A-Za-z0-9._:\-\[\]]+$') {
            throw "[FATAL] 非法 fence etcd endpoint:$endpoint"
        }
    }
    if ([string]::IsNullOrWhiteSpace($DsFenceKeysetRevision) -or $DsFenceKeysetRevision -cnotmatch '^[A-Za-z0-9._-]+$') {
        throw '[FATAL] authority_mode=redis 必须提供不可变 -DsFenceKeysetRevision(仅字母数字._-)。'
    }
} elseif ($Prod) {
    throw '[FATAL] -Prod 未进入 redis authority，拒绝生成。'
}

# ===== owner 权威库 DSN(§9.22:线性一致 + 确认写不回滚;生产必须 TiDB)=====
# dev 模板 owner-dev.yaml 连单机 MySQL(pandora_dev_pwd 公开凭据)只服务本地联调;
# MySQL 异步复制主从切换会回滚已确认写,owner CAS 回滚即可能双 owner(脑裂),生产禁用。
# -Prod 必须注入真 TiDB DSN,并由 Set-ProdOwnerRequireTiDB 机械打开服务端启动强校验
# (owner 启动查 VERSION() 必须含 -TiDB-,不符 fail-fast),双层防线防 dev mysql 带上线。
if ($Prod) {
    if ([string]::IsNullOrWhiteSpace($OwnerStoreDsn)) {
        throw '[FATAL] -Prod 必须提供 -OwnerStoreDsn 或环境变量 PANDORA_OWNER_TIDB_DSN(owner 权威库真 TiDB DSN,pandora_owner 库)。' +
              ' owner CAS 依赖线性一致 + 确认写不回滚(§9.22),不允许 -Prod 产物继承 dev mysql 配置。'
    }
    if ($OwnerStoreDsn -match '[\x00-\x1F\x7F-\x9F]') {
        throw '[FATAL] owner DSN 含控制字符(换行/制表等),多为误带的尾部空白,请清理后再注入。'
    }
    if ($OwnerStoreDsn.Contains('pandora_dev_pwd')) {
        throw '[FATAL] -Prod 的 owner DSN 不能使用公开 dev 凭据(pandora_dev_pwd)。请换成 CI/CD 注入的真 TiDB 凭据。'
    }
    if ($OwnerStoreDsn.Contains('mysql:3306') -or $OwnerStoreDsn.Contains('127.0.0.1:3307')) {
        throw '[FATAL] -Prod 的 owner DSN 指向 dev MySQL(mysql:3306 / 127.0.0.1:3307)。生产必须连 TiDB(deploy/tidb-init/02-owner-tidb.sql,§9.22)。'
    }
    if ($OwnerStoreDsn -cnotmatch '/pandora_owner(?:[?]|$)') {
        throw '[FATAL] -Prod 的 owner DSN 必须指向 pandora_owner 库(形如 user:pwd@tcp(tidb-host:4000)/pandora_owner?parseTime=true&loc=UTC)。'
    }
} elseif (-not [string]::IsNullOrWhiteSpace($OwnerStoreDsn)) {
    # 非 -Prod 显式覆盖(本地连真 TiDB 测):只做注入安全校验,不强制 TiDB。
    if ($OwnerStoreDsn -match '[\x00-\x1F\x7F-\x9F]') {
        throw '[FATAL] -OwnerStoreDsn 含控制字符,不能安全写入 YAML。'
    }
}

# ===== login 账号库 DSN(全服单点扩容,2026-07-27;校验规则与上面 owner 块同构)=====
# ⚠️ 本块必须**排在 owner 块之后**:既有 -Prod 契约测试(b1 / owner / ratelimit / session_gate)
# 的负向用例断言的是「缺 X 必须拒绝」,若新校验插到它们前面,那些用例会改为因「缺 account DSN」
# 而失败 —— 仍然 PASS,却再也证明不了它自己声称的东西(静默空转)。新增校验一律往后追加。
#
# 为什么 -Prod 必须给:①pandora_account 是全服写压力汇聚点(每次登录一个定序事务),单 MySQL
# 无横向逃生口;②不给就会继承 dev 模板的**公开凭据** pandora_dev_pwd@mysql:3306(见下方
# $ProdDevCredentialWired 断言)。两条都属发布阻断级。
if ($Prod) {
    if ([string]::IsNullOrWhiteSpace($AccountStoreDsn)) {
        throw '[FATAL][ACCOUNT_DSN_REQUIRED] -Prod 必须提供 -AccountStoreDsn 或环境变量 PANDORA_ACCOUNT_TIDB_DSN(login 账号库真 TiDB DSN,pandora_account 库)。' +
              ' 该库承载全服登录定序事务且不可分片降级,不允许 -Prod 产物继承 dev mysql 配置(公开凭据 pandora_dev_pwd)。'
    }
    if ($AccountStoreDsn -match '[\x00-\x1F\x7F-\x9F]') {
        throw '[FATAL] account DSN 含控制字符(换行/制表等),多为误带的尾部空白,请清理后再注入。'
    }
    if ($AccountStoreDsn.Contains('pandora_dev_pwd')) {
        throw '[FATAL][ACCOUNT_DSN_DEV_CREDENTIAL] -Prod 的 account DSN 不能使用公开 dev 凭据(pandora_dev_pwd)。请换成 CI/CD 注入的真 TiDB 凭据。'
    }
    if ($AccountStoreDsn.Contains('mysql:3306') -or $AccountStoreDsn.Contains('127.0.0.1:3307')) {
        throw '[FATAL][ACCOUNT_DSN_DEV_MYSQL] -Prod 的 account DSN 指向 dev MySQL(mysql:3306 / 127.0.0.1:3307)。生产必须连 TiDB(deploy/tidb-init/03-account-tidb.sql)。'
    }
    if ($AccountStoreDsn -cnotmatch '/pandora_account(?:[?]|$)') {
        throw '[FATAL][ACCOUNT_DSN_DATABASE] -Prod 的 account DSN 必须指向 pandora_account 库(形如 user:pwd@tcp(tidb-host:4000)/pandora_account?parseTime=true&loc=UTC&charset=utf8mb4&collation=utf8mb4_0900_ai_ci)。'
    }
    # accounts.account 的唯一性语义由列 collation 决定(Go 侧对账号串零归一化)。dev 单机 MySQL
    # 用 utf8mb4_0900_ai_ci(大小写/口音不敏感 + NO PAD);DSN 若改成 utf8mb4_bin,连接级
    # collation 翻转会让「大小写不同的同名账号」被当成两个账号,老玩家登不进、且可被抢注。
    # 这里只挡住显式写错的情形;真正的把关是 login 启动期对**列**做行为探针(§16 隐蔽 bug)。
    if ($AccountStoreDsn -match 'collation=utf8mb4_bin') {
        throw '[FATAL][ACCOUNT_DSN_COLLATION] -Prod 的 account DSN 不能用 collation=utf8mb4_bin:accounts.account 是客户端上报的账号名且带唯一键,' +
              'Go 侧无归一化,大小写敏感化会让存量玩家登不进并开出同名抢注口子。请沿用 utf8mb4_0900_ai_ci(TiDB v7.4+ 原生支持)。'
    }
} elseif (-not [string]::IsNullOrWhiteSpace($AccountStoreDsn)) {
    # 非 -Prod 显式覆盖(本地连真 TiDB 测):只做注入安全校验,不强制 TiDB。
    if ($AccountStoreDsn -match '[\x00-\x1F\x7F-\x9F]') {
        throw '[FATAL] -AccountStoreDsn 含控制字符,不能安全写入 YAML。'
    }
}

# ===== agones 链路必须显式声明生产或本地(审核 P1:agones 不带 -Prod 会写入公开 dev 密钥)=====
# -AllocatorMode agones 指向**真 Linux DS**(线上 k8s 或本地 minikube),不是 docker-mock 假链路。
# 生产/本地必须**显式**二选一,不再从 advertise host 推断(真线上也会配 advertise host/DNS,推断会让
# 生产绕过 -Prod 写入公开 dev 密钥):
#   -Prod            → 注入两把真密钥(见上);
#   -AllowDevSecrets → 显式承认本地/dev 链路,允许沿用公开 dev 密钥(仅限本地 minikube 自测);
#   两者都没有        → 拒绝生成(deny-by-default,防 dev 密钥被静默带上真集群)。
if ($AllocatorMode -eq 'agones' -and -not $Prod) {
    if (-not $AllowDevSecrets) {
        throw "[FATAL] -AllocatorMode agones 指向真 Linux DS 链路,必须显式二选一:线上加 -Prod 并提供两把真密钥;" +
              ' 本地 minikube 自测加 -AllowDevSecrets 显式承认沿用两套独立的公开 dev 密钥。' +
              " 不再从 -AllocatorAdvertiseHost 推断本地(生产也会配 advertise host,推断会让生产绕过 -Prod 泄露 dev 密钥)。"
    }
    Write-Host "[WARN] agones + -AllowDevSecrets:沿用公开 dev 密钥,仅限本地 minikube 自测,勿部署到真集群。" -ForegroundColor Yellow
}

# ===== DSTicket v2(方案 B)机械门 =====
# agones = 真 Linux DS 链路:DS 侧终态只认 RS256 v2 票(2026-07-14 拍板),4 个签发方必须注入
# ds_ticket 段,缺 kid 直接拒绝生成(直接切换,不保留「agones 但仍只签 HS256 票」的半配置形态)。
# mock 链路无真 DS 验票,禁止注入(防半套配置漂移进 docker 链路)。
if ($AllocatorMode -eq 'agones') {
    if ([string]::IsNullOrWhiteSpace($DsTicketActiveKid)) {
        throw '[FATAL] -AllocatorMode agones 必须提供 -DsTicketActiveKid(或环境变量 PANDORA_DSTICKET_ACTIVE_KID):' +
              ' DSTicket v2(RS256)active kid = tools/dsticketkeys 生成的 jwks.json 里的 kid(RFC 7638 指纹)。' +
              ' 方案 B 已拍板为生产终态,agones 链路不再生成纯 HS256 玩家票配置。'
    }
    if ($DsTicketActiveKid -cnotmatch '^[A-Za-z0-9_-]{43}$') {
        throw "[FATAL] -DsTicketActiveKid 必须是 43 字符 base64url(RFC 7638 SHA-256 指纹),实际=$DsTicketActiveKid"
    }
    if ($DsTicketKeysetRevision -cnotmatch '^[1-9][0-9]*$' -or
        [int64]$DsTicketKeysetRevision -gt [int]::MaxValue) {
        throw '[FATAL] -AllocatorMode agones 必须提供正整数 -DsTicketKeysetRevision / PANDORA_DSTICKET_KEYSET_REVISION。'
    }
    $ttlMatch = [regex]::Match($DsTicketTTL, '^([0-9]+)s$')
    if (-not $ttlMatch.Success -or [int]$ttlMatch.Groups[1].Value -lt 30 -or [int]$ttlMatch.Groups[1].Value -gt 180) {
        throw "[FATAL] -DsTicketTTL 必须是 30s..180s 的整数秒(UE 验票机械上限 exp-iat ≤ 180s),实际=$DsTicketTTL"
    }
} elseif (-not [string]::IsNullOrWhiteSpace($DsTicketActiveKid) -or
          -not [string]::IsNullOrWhiteSpace($DsTicketKeysetRevision)) {
    throw '[FATAL] 非 agones 链路(mock)不注入 DSTicket v2 配置,请去掉 active kid / keyset revision。'
}

# base64url(secret):HS256 的 JWKS `k` 字段编码(与 pkg/auth.JWKSInlineHS256 / envoy local_jwks 一致)。
function ConvertTo-Base64Url([string]$s) {
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($s)
    return [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')
}

# 把要注入双引号 YAML 字符串的密钥做转义(审核 P1 #8):`\` → `\\`,`"` → `\"`。
# 控制字符已在 Assert-ProdSecret 阶段拒绝,这里只需处理 YAML 双引号里必须转义的两个字符,
# 保证含 `"` / `\` 的合法密钥注入后仍是有效 YAML 且字节不被 yaml.v3 改写。
function ConvertTo-YamlDoubleQuoted([string]$s) {
    return $s.Replace('\', '\\').Replace('"', '\"')
}

# 当前 dev 模板中两类密钥节点的权威服务集合。新增/移动节点必须显式更新本清单；发现未登记节点
# 直接失败，避免正则或目录扫描把玩家密钥误写进 DS 域。
$PlayerSecretServiceNames = @('login', 'matchmaker', 'matchmaker-pve', 'hub-allocator')
# player 2026-08-13 入列:GetLoadout 挂上了 Envoy DS 面(:8444),而该监听器没有 jwt_authn,
# 经它进来的 callerID 恒为 0 —— 服务端必须能验 DS 回调令牌,才拦得住「任意能连 :8444 或
# 直连 20002 的进程查任意玩家出战快照」。player 只做 verify(不签发),与 team/guild 不同:
# team/guild 2026-08-17 同因入列(GetPlayerTeam / 公会归属反查同构暴露在 DS 面):服务侧 dsGuard 早已接线,此前唯独缺密钥分发。三者都 verify-only。
$DsSecretServiceNames = @('login', 'ds-allocator', 'hub-allocator', 'battle-result', 'player-locator', 'player', 'team', 'guild')
# DSTicket v2(方案 B)签发方清单(与决策文档 §7.2 私钥暴露面一致):恰好等于玩家面 jwt 清单。
$DsTicketServiceNames = @('login', 'matchmaker', 'matchmaker-pve', 'hub-allocator')
$PlacementSecretBindings = @()
# login→matchmaker 的内部服务鉴权(internalrpcauth,HMAC + Redis nonce 防重放)是
# **成对**的:验签端是 matchmaker.match.match_resume_auth_secret,签名端是
# login.matchmaker.auth_secret。两端必须同值,只改一端 = login 查不动 matchmaker 权威。
#
# 2026-08-11 补 login 绑定(此前只绑两个 matchmaker):漏了它的后果是运维用
# `-MatchResumeAuth <生产密钥>` 生成产物时,matchmaker 拿到生产密钥而 **login 留着 dev 密钥**,
# 于是 ①login→matchmaker 的 ResolvePlayerMatchContext 全部被拒 —— 那是 2026-07-15 P0 修复
# 的兜底路径(locator presence 未命中 BATTLE 时改查耐久权威,READY 局投影蒸发也能路由回原局),
# 静默失效;②dev 密钥被带进生产。
# 这条漂移此前一直被 gen_cluster_b1 里那条恒红的 placement 断言遮蔽(它一抛,后面 14 条
# 断言包括本条全都不再执行),2026-08-11 隔离未实现能力后才暴露出来。
$MatchResumeAuthSecretBindings = @(
    @{ Service = 'matchmaker'; Section = 'match'; Child = 'match_resume_auth_secret' },
    @{ Service = 'matchmaker-pve'; Section = 'match'; Child = 'match_resume_auth_secret' },
    @{ Service = 'login'; Section = 'matchmaker'; Child = 'auth_secret' }
)
# Team→Matchmaker 那把 key 的两端必须成对改写:验签端(matchmaker.match.team_resume_auth_secret)
# 与签名端(team.team.match_resume_auth_secret)。只写一端 = 全部 team 调用被拒。
# matchmaker-pve 不在此列:team 只连 PVP matchmaker(team.matchmaker_addr),PVE 模板也没有该节点。
$TeamResumeAuthSecretBindings = @(
    @{ Service = 'matchmaker'; Section = 'match'; Child = 'team_resume_auth_secret' },
    @{ Service = 'team'; Section = 'team'; Child = 'match_resume_auth_secret' }
)
$PlayerNoResolveAuthSecretBindings = @(
    @{ Service = 'login'; Section = 'login'; Child = 'player_no_resolve_auth_secret' },
    @{ Service = 'team'; Section = 'team'; Child = 'player_no_resolver_auth_secret' }
)
$PlayerNoResolveAuthAudienceBindings = @(
    @{ Service = 'login'; Section = 'login'; Child = 'player_no_resolve_auth_audience' },
    @{ Service = 'team'; Section = 'team'; Child = 'player_no_resolver_auth_audience' }
)
$PlayerNoResolveAuthAudience = 'login:player-no'
$PlayerNoResolveAddressBindings = @(
    @{ Service = 'team'; Section = 'team'; Child = 'player_no_resolver_addr'; Value = 'login:20001' }
)
$PlayerNameResolveAuthSecretBindings = @(
    @{ Service = 'player'; Section = 'player'; Child = 'player_name_resolve_auth_secret' },
    @{ Service = 'team'; Section = 'team'; Child = 'player_name_resolver_auth_secret' }
)
$PlayerNameResolveAuthAudienceBindings = @(
    @{ Service = 'player'; Section = 'player'; Child = 'player_name_resolve_auth_audience' },
    @{ Service = 'team'; Section = 'team'; Child = 'player_name_resolver_auth_audience' }
)
$PlayerNameResolveAuthAudience = 'player:name'
$PlayerNameResolveAddressBindings = @(
    @{ Service = 'team'; Section = 'team'; Child = 'player_name_resolver_addr'; Value = 'player:20002' }
)
$ApplicationDisplayAuthGroups = @(
    @{
        Effective = $EffectiveFriendPlayerNameResolveAuthSecret
        Dev = $DevFriendPlayerNameResolveAuthSecret
        Audience = $PlayerNameResolveAuthAudience
        SecretBindings = @(
            @{ Service = 'player'; Section = 'player'; Child = 'friend_player_name_resolve_auth_secret' },
            @{ Service = 'friend'; Section = 'friend'; Child = 'player_name_resolver_auth_secret' }
        )
        AudienceBindings = @(
            @{ Service = 'player'; Section = 'player'; Child = 'friend_player_name_resolve_auth_audience' },
            @{ Service = 'friend'; Section = 'friend'; Child = 'player_name_resolver_auth_audience' }
        )
        AddressBindings = @(
            @{ Service = 'friend'; Section = 'friend'; Child = 'player_name_resolver_addr'; Value = 'player:20002' }
        )
    },
    @{
        Effective = $EffectiveFriendPlayerNoResolveAuthSecret
        Dev = $DevFriendPlayerNoResolveAuthSecret
        Audience = $PlayerNoResolveAuthAudience
        SecretBindings = @(
            @{ Service = 'login'; Section = 'login'; Child = 'friend_player_no_resolve_auth_secret' },
            @{ Service = 'friend'; Section = 'friend'; Child = 'player_no_resolver_auth_secret' }
        )
        AudienceBindings = @(
            @{ Service = 'login'; Section = 'login'; Child = 'friend_player_no_resolve_auth_audience' },
            @{ Service = 'friend'; Section = 'friend'; Child = 'player_no_resolver_auth_audience' }
        )
        AddressBindings = @(
            @{ Service = 'friend'; Section = 'friend'; Child = 'player_no_resolver_addr'; Value = 'login:20001' }
        )
    },
    @{
        Effective = $EffectiveGuildPlayerNameResolveAuthSecret
        Dev = $DevGuildPlayerNameResolveAuthSecret
        Audience = $PlayerNameResolveAuthAudience
        SecretBindings = @(
            @{ Service = 'player'; Section = 'player'; Child = 'guild_player_name_resolve_auth_secret' },
            @{ Service = 'guild'; Section = 'guild'; Child = 'player_name_resolver_auth_secret' }
        )
        AudienceBindings = @(
            @{ Service = 'player'; Section = 'player'; Child = 'guild_player_name_resolve_auth_audience' },
            @{ Service = 'guild'; Section = 'guild'; Child = 'player_name_resolver_auth_audience' }
        )
        AddressBindings = @(
            @{ Service = 'guild'; Section = 'guild'; Child = 'player_name_resolver_addr'; Value = 'player:20002' }
        )
    },
    @{
        Effective = $EffectiveGuildPlayerNoResolveAuthSecret
        Dev = $DevGuildPlayerNoResolveAuthSecret
        Audience = $PlayerNoResolveAuthAudience
        SecretBindings = @(
            @{ Service = 'login'; Section = 'login'; Child = 'guild_player_no_resolve_auth_secret' },
            @{ Service = 'guild'; Section = 'guild'; Child = 'player_no_resolver_auth_secret' }
        )
        AudienceBindings = @(
            @{ Service = 'login'; Section = 'login'; Child = 'guild_player_no_resolve_auth_audience' },
            @{ Service = 'guild'; Section = 'guild'; Child = 'player_no_resolver_auth_audience' }
        )
        AddressBindings = @(
            @{ Service = 'guild'; Section = 'guild'; Child = 'player_no_resolver_addr'; Value = 'login:20001' }
        )
    }
)
$AllocationAbortAuthSecretBindings = @(
    @{ Service = 'matchmaker'; Section = 'match'; Child = 'allocation_abort_auth_secret' },
    @{ Service = 'matchmaker-pve'; Section = 'match'; Child = 'allocation_abort_auth_secret' },
    @{ Service = 'ds-allocator'; Section = 'allocator'; Child = 'allocation_abort_auth_secret' }
)

# 精确定位 YAML 节点的直接子项(默认 `secret`,也用于 `mode`)。不使用跨段 `.*?`：那会在 jwt 缺 secret 时越过同级
# ds_auth 段，把玩家密钥写进 DS 域。这里只接受 dev 模板约定的双引号标量；格式漂移必须人工审查。
function Get-YamlSectionSecretLocation {
    param(
        [string]$ServiceName,
        [string]$Text,
        [string]$SectionName,
        [string]$ChildName = 'secret'
    )

    $newline = if ($Text.Contains("`r`n")) { "`r`n" } else { "`n" }
    [string[]]$lines = [regex]::Split($Text, '\r?\n')
    $sectionPattern = '^([ ]*)' + [regex]::Escape($SectionName) + ':[ \t]*(?:#.*)?$'
    $sectionIndexes = @(
        for ($i = 0; $i -lt $lines.Count; $i++) {
            if ($lines[$i] -match $sectionPattern) { $i }
        }
    )
    if ($sectionIndexes.Count -ne 1) {
        throw "[FATAL] $ServiceName 期望且只能有一个 $SectionName 节点,实际=$($sectionIndexes.Count)。"
    }

    $sectionIndex = [int]$sectionIndexes[0]
    $sectionMatch = [regex]::Match($lines[$sectionIndex], $sectionPattern)
    $sectionIndent = $sectionMatch.Groups[1].Value.Length
    $end = $lines.Count
    for ($i = $sectionIndex + 1; $i -lt $lines.Count; $i++) {
        if ($lines[$i] -match '^\s*$' -or $lines[$i] -match '^[ ]*#') { continue }
        $indentMatch = [regex]::Match($lines[$i], '^([ ]*)')
        if ($indentMatch.Groups[1].Value.Length -le $sectionIndent) { $end = $i; break }
    }

    $directIndent = [int]::MaxValue
    for ($i = $sectionIndex + 1; $i -lt $end; $i++) {
        if ($lines[$i] -match '^\s*$' -or $lines[$i] -match '^[ ]*#') { continue }
        $indentMatch = [regex]::Match($lines[$i], '^([ ]*)')
        $indent = $indentMatch.Groups[1].Value.Length
        if ($indent -gt $sectionIndent -and $indent -lt $directIndent) { $directIndent = $indent }
    }
    if ($directIndent -eq [int]::MaxValue) { throw "[FATAL] $ServiceName.$SectionName 是空节点。" }

    $childPattern = '^([ ]*)' + [regex]::Escape($ChildName) + '[ \t]*:'
    $secretIndexes = @(
        for ($i = $sectionIndex + 1; $i -lt $end; $i++) {
            $m = [regex]::Match($lines[$i], $childPattern)
            if ($m.Success -and $m.Groups[1].Value.Length -eq $directIndent) { $i }
        }
    )
    if ($secretIndexes.Count -ne 1) {
        throw "[FATAL] $ServiceName.$SectionName 期望且只能有一个直接子项 $ChildName,实际=$($secretIndexes.Count)。"
    }
    $secretIndex = [int]$secretIndexes[0]
    $valuePattern = '^([ ]*' + [regex]::Escape($ChildName) + '[ \t]*:[ \t]*)"((?:\\.|[^"])*)"([ \t]*(?:#.*)?)$'
    $valueMatch = [regex]::Match($lines[$secretIndex], $valuePattern)
    if (-not $valueMatch.Success) {
        throw "[FATAL] $ServiceName.$SectionName.$ChildName 必须是单行双引号标量,拒绝模糊替换:$($lines[$secretIndex])"
    }
    return [pscustomobject]@{
        Lines        = $lines
        Newline      = $newline
        SecretIndex  = $secretIndex
        Prefix       = $valueMatch.Groups[1].Value
        RawValue     = $valueMatch.Groups[2].Value
        Suffix       = $valueMatch.Groups[3].Value
        SectionIndex = $sectionIndex
        SectionEnd   = $end
        DirectIndent = $directIndent
    }
}

function Assert-YamlSectionSecret {
    param(
        [string]$ServiceName,
        [string]$Text,
        [string]$SectionName,
        [string]$ExpectedValue
    )
    $location = Get-YamlSectionSecretLocation $ServiceName $Text $SectionName
    $expectedRaw = ConvertTo-YamlDoubleQuoted $ExpectedValue
    if ($location.RawValue -cne $expectedRaw) {
        throw "[FATAL] $ServiceName.$SectionName.secret 与期望密钥域不一致。"
    }
}

function Set-YamlSectionSecret {
    param(
        [string]$ServiceName,
        [string]$Text,
        [string]$SectionName,
        [string]$NewValue
    )
    $location = Get-YamlSectionSecretLocation $ServiceName $Text $SectionName
    if ($location.RawValue -cne $DevPublicSecret) {
        throw "[FATAL] $ServiceName.$SectionName.secret 不等于权威 dev 模板值,拒绝把未知旧值静默覆盖。"
    }
    $location.Lines[$location.SecretIndex] = $location.Prefix + '"' + (ConvertTo-YamlDoubleQuoted $NewValue) + '"' + $location.Suffix
    return ($location.Lines -join $location.Newline)
}

function Set-YamlDirectString {
    param(
        [string]$ServiceName,
        [string]$Text,
        [string]$SectionName,
        [string]$ChildName,
        [string]$ExpectedTemplateValue,
        [string]$NewValue
    )
    $location = Get-YamlSectionSecretLocation $ServiceName $Text $SectionName $ChildName
    if ($location.RawValue -cne (ConvertTo-YamlDoubleQuoted $ExpectedTemplateValue)) {
        throw "[FATAL] $ServiceName.$SectionName.$ChildName 不等于权威 dev 模板值，拒绝覆盖未知配置。"
    }
    $location.Lines[$location.SecretIndex] = $location.Prefix + '"' + (ConvertTo-YamlDoubleQuoted $NewValue) + '"' + $location.Suffix
    return ($location.Lines -join $location.Newline)
}

function Assert-YamlDirectString {
    param(
        [string]$ServiceName,
        [string]$Text,
        [string]$SectionName,
        [string]$ChildName,
        [string]$ExpectedValue
    )
    $location = Get-YamlSectionSecretLocation $ServiceName $Text $SectionName $ChildName
    if ($location.RawValue -cne (ConvertTo-YamlDoubleQuoted $ExpectedValue)) {
        throw "[FATAL] $ServiceName.$SectionName.$ChildName 与期望 placement proof key 不一致。"
    }
}

function Set-YamlPlacementMode {
    param([string]$ServiceName, [string]$Text, [string]$SectionName, [string]$NewMode)
    $location = Get-YamlSectionSecretLocation $ServiceName $Text $SectionName 'placement_mode'
    if ($location.RawValue -cnotin @('off', 'shadow', 'enforce')) {
        throw "[FATAL] $ServiceName.$SectionName.placement_mode 旧值非法。"
    }
    $location.Lines[$location.SecretIndex] = $location.Prefix + '"' + $NewMode + '"' + $location.Suffix
    return ($location.Lines -join $location.Newline)
}

# 在节段 secret 行之后插入 `additional_secrets: ["<旧密钥>"]`(同缩进,flow 列表)。
# dev 模板不带此字段;若节段内已存在直接子项 additional_secrets 则拒绝(防重复注入/模板漂移)。
function Add-YamlSectionAdditionalSecrets {
    param(
        [string]$ServiceName,
        [string]$Text,
        [string]$SectionName,
        [string]$AdditionalValue
    )
    $location = Get-YamlSectionSecretLocation $ServiceName $Text $SectionName
    $addPattern = '^([ ]*)additional_secrets[ \t]*:'
    for ($i = $location.SectionIndex + 1; $i -lt $location.SectionEnd; $i++) {
        $m = [regex]::Match($location.Lines[$i], $addPattern)
        if ($m.Success -and $m.Groups[1].Value.Length -eq $location.DirectIndent) {
            throw "[FATAL] $ServiceName.$SectionName 已存在 additional_secrets,拒绝重复注入。"
        }
    }
    $indent = ' ' * $location.DirectIndent
    $newLine = $indent + 'additional_secrets: ["' + (ConvertTo-YamlDoubleQuoted $AdditionalValue) + '"]'
    $lines = [System.Collections.Generic.List[string]]::new($location.Lines)
    $lines.Insert($location.SecretIndex + 1, $newLine)
    return (@($lines) -join $location.Newline)
}

function Assert-YamlSectionAdditionalSecrets {
    param(
        [string]$ServiceName,
        [string]$Text,
        [string]$SectionName,
        [string]$ExpectedValue
    )
    $location = Get-YamlSectionSecretLocation $ServiceName $Text $SectionName
    $expectedLine = (' ' * $location.DirectIndent) + 'additional_secrets: ["' + (ConvertTo-YamlDoubleQuoted $ExpectedValue) + '"]'
    $found = $false
    for ($i = $location.SectionIndex + 1; $i -lt $location.SectionEnd; $i++) {
        if ($location.Lines[$i] -cmatch '^[ ]*additional_secrets[ \t]*:') {
            if ($location.Lines[$i] -cne $expectedLine) {
                throw "[FATAL] $ServiceName.$SectionName.additional_secrets 与期望轮换旧密钥不一致。"
            }
            $found = $true
        }
    }
    if (-not $found) { throw "[FATAL] $ServiceName.$SectionName 缺少期望的 additional_secrets。" }
}

# P0(三审 #1):断言节段内**不存在**任何 additional_secrets 直接子项。
# 生产/任意不注入 additional 的路径都必须调用它:dev 模板此刻虽不带该字段,但生成器绝不能
# 依赖模板“恰好没写”。若将来有人往 dev 模板预置 additional_secrets:["<dev 公钥>"],
# 而 -Prod 又(因轮换决策未拍板)跳过 additional 注入,旧逻辑会把该行原样带进线上产物 =
# dev 公钥泄漏进生产。这里一律 fail-closed:发现未经本次注入的 additional_secrets 直接拒绝生成。
function Assert-NoYamlSectionAdditionalSecrets {
    param(
        [string]$ServiceName,
        [string]$Text,
        [string]$SectionName
    )
    $location = Get-YamlSectionSecretLocation $ServiceName $Text $SectionName
    for ($i = $location.SectionIndex + 1; $i -lt $location.SectionEnd; $i++) {
        if ($location.Lines[$i] -cmatch '^[ ]*additional_secrets[ \t]*:') {
            throw "[FATAL] $ServiceName.$SectionName 存在未经本次注入的 additional_secrets(模板预置?),拒绝生成。" +
                  " 生产轮换密钥只能由 -SecretAdditional / -DsSecretAdditional 显式注入,不得由模板携带(防 dev 公钥泄漏进线上)。"
        }
    }
}

# 改写 ds_auth.mode(二审 A#2)。只接受已知合法旧值(off/permissive/enforce),防模板漂移静默覆盖。
function Set-YamlDsAuthMode {
    param(
        [string]$ServiceName,
        [string]$Text,
        [string]$NewMode
    )
    $location = Get-YamlSectionSecretLocation $ServiceName $Text 'ds_auth' 'mode'
    if ($location.RawValue -cnotin @('off', 'permissive', 'enforce')) {
        throw "[FATAL] $ServiceName.ds_auth.mode 旧值非法(=$($location.RawValue)),拒绝改写。"
    }
    $location.Lines[$location.SecretIndex] = $location.Prefix + '"' + $NewMode + '"' + $location.Suffix
    return ($location.Lines -join $location.Newline)
}

function Assert-YamlDsAuthMode {
    param(
        [string]$ServiceName,
        [string]$Text,
        [string]$ExpectedMode
    )
    $location = Get-YamlSectionSecretLocation $ServiceName $Text 'ds_auth' 'mode'
    if ($location.RawValue -cne $ExpectedMode) {
        throw "[FATAL] $ServiceName.ds_auth.mode=$($location.RawValue) 与期望值 $ExpectedMode 不一致。"
    }
}

function Set-YamlDsAuthorityMode {
    param([string]$ServiceName, [string]$Text, [string]$NewMode)
    $location = Get-YamlSectionSecretLocation $ServiceName $Text 'ds_auth' 'authority_mode'
    if ($location.RawValue -cnotin @('legacy', 'redis')) {
        throw "[FATAL] $ServiceName.ds_auth.authority_mode 旧值非法(=$($location.RawValue))。"
    }
    $location.Lines[$location.SecretIndex] = $location.Prefix + '"' + $NewMode + '"' + $location.Suffix
    return ($location.Lines -join $location.Newline)
}

function Add-YamlDsAuthFence {
    param([string]$ServiceName, [string]$Text)
    $location = Get-YamlSectionSecretLocation $ServiceName $Text 'ds_auth' 'secret'
    for ($i = $location.SectionIndex + 1; $i -lt $location.SectionEnd; $i++) {
        if ($location.Lines[$i] -cmatch ('^' + (' ' * $location.DirectIndent) + 'fence[ \t]*:')) {
            throw "[FATAL] $ServiceName.ds_auth 已有活动 fence 节点，拒绝重复/模糊覆盖。"
        }
    }
    $indent = ' ' * $location.DirectIndent
    $nested = ' ' * ($location.DirectIndent + 2)
    $endpointValues = @($DsFenceEndpoints | ForEach-Object { '"' + (ConvertTo-YamlDoubleQuoted $_) + '"' })
    $lines = [System.Collections.Generic.List[string]]::new($location.Lines)
    $insertAt = $location.SecretIndex + 1
    [string[]]$newFenceLines = @(
        ($indent + 'fence:')
        ($nested + 'etcd_endpoints: [' + ($endpointValues -join ', ') + ']')
        ($nested + 'etcd_prefix: "/pandora/ds-auth/"')
        ($nested + 'etcd_lease_ttl_sec: 15')
        ($nested + 'etcd_dial_timeout: "5s"')
        ($nested + 'keyset_revision: "' + (ConvertTo-YamlDoubleQuoted $DsFenceKeysetRevision) + '"')
    )
    foreach ($line in $newFenceLines) {
        $lines.Insert($insertAt, $line)
        $insertAt++
    }
    return (@($lines) -join $location.Newline)
}

# 精确定位 battle.consume_topics 的 block list。Redis authority 下无凭据的
# pandora.battle.result 会绕过 Guard/active/receipt，故生成器必须机械删掉，不能靠人工改 YAML。
function Get-BattleResultConsumeTopicsLocation {
    param([string]$Text)

    $newline = if ($Text.Contains("`r`n")) { "`r`n" } else { "`n" }
    [string[]]$lines = [regex]::Split($Text, '\r?\n')
    $sectionIndexes = @(
        for ($i = 0; $i -lt $lines.Count; $i++) {
            if ($lines[$i] -cmatch '^battle:[ \t]*(?:#.*)?$') { $i }
        }
    )
    if ($sectionIndexes.Count -ne 1) {
        throw "[FATAL] battle-result 期望且只能有一个顶级 battle 节点,实际=$($sectionIndexes.Count)。"
    }
    $sectionIndex = [int]$sectionIndexes[0]
    $sectionEnd = $lines.Count
    for ($i = $sectionIndex + 1; $i -lt $lines.Count; $i++) {
        if ($lines[$i] -cmatch '^\S') { $sectionEnd = $i; break }
    }
    $headerIndexes = @(
        for ($i = $sectionIndex + 1; $i -lt $sectionEnd; $i++) {
            if ($lines[$i] -cmatch '^[ ]{2}consume_topics:[ \t]*(?:#.*)?$') { $i }
        }
    )
    if ($headerIndexes.Count -ne 1) {
        throw "[FATAL] battle-result.battle.consume_topics 期望唯一 block list,实际=$($headerIndexes.Count)。"
    }
    $headerIndex = [int]$headerIndexes[0]
    $entryIndexes = [System.Collections.Generic.List[int]]::new()
    $values = [System.Collections.Generic.List[string]]::new()
    for ($i = $headerIndex + 1; $i -lt $sectionEnd; $i++) {
        $match = [regex]::Match($lines[$i], '^[ ]{4}-[ \t]*"([^"]+)"[ \t]*(?:#.*)?$')
        if (-not $match.Success) { break }
        $entryIndexes.Add($i)
        $values.Add($match.Groups[1].Value)
    }
    if ($entryIndexes.Count -eq 0) {
        throw '[FATAL] battle-result.battle.consume_topics 必须至少有一个双引号列表项。'
    }
    return [pscustomobject]@{
        Lines        = $lines
        Newline      = $newline
        HeaderIndex  = $headerIndex
        EntryIndexes = @($entryIndexes)
        Values       = @($values)
    }
}

function Set-BattleResultRedisAuthorityIngress {
    param([string]$Text)
    $location = Get-BattleResultConsumeTopicsLocation $Text
    $allowed = @('pandora.battle.result', 'pandora.ds.lifecycle')
    foreach ($value in $location.Values) {
        if ($value -cnotin $allowed) {
            throw "[FATAL] battle-result.consume_topics 含未知旧值 $value,拒绝模糊改写。"
        }
    }
    if (@($location.Values | Group-Object | Where-Object Count -gt 1).Count -ne 0) {
        throw '[FATAL] battle-result.consume_topics 存在重复值,拒绝改写。'
    }
    if ($location.Values -cnotcontains 'pandora.ds.lifecycle') {
        throw '[FATAL] battle-result.consume_topics 缺 pandora.ds.lifecycle,不能生成 Redis authority。'
    }
    $lines = [System.Collections.Generic.List[string]]::new($location.Lines)
    $first = [int]$location.EntryIndexes[0]
    $lines.RemoveRange($first, $location.EntryIndexes.Count)
    $lines.Insert($first, '    - "pandora.ds.lifecycle"')
    return (@($lines) -join $location.Newline)
}

function Assert-BattleResultConsumeTopics {
    param([string]$Text, [string[]]$Expected)
    $location = Get-BattleResultConsumeTopicsLocation $Text
    if ($location.Values.Count -ne $Expected.Count) {
        throw "[FATAL] battle-result.consume_topics 数量错误:actual=[$($location.Values -join ',')] expected=[$($Expected -join ',')]"
    }
    for ($i = 0; $i -lt $Expected.Count; $i++) {
        if ($location.Values[$i] -cne $Expected[$i]) {
            throw "[FATAL] battle-result.consume_topics 错误:actual=[$($location.Values -join ',')] expected=[$($Expected -join ',')]"
        }
    }
}

function Set-LoginHubAssignmentBinding {
    param([string]$Text, [bool]$Enabled)
    $location = Get-YamlSectionSecretLocation 'login' $Text 'login' 'mock_hub_ds_addr'
    $pattern = '^([ ]*)require_hub_assignment_binding[ \t]*:[ \t]*(true|false)([ \t]*(?:#.*)?)$'
    $hits = @()
    for ($i = $location.SectionIndex + 1; $i -lt $location.SectionEnd; $i++) {
        $match = [regex]::Match($location.Lines[$i], $pattern)
        if ($match.Success -and $match.Groups[1].Value.Length -eq $location.DirectIndent) { $hits += $i }
    }
    if ($hits.Count -ne 1) { throw "[FATAL] login.require_hub_assignment_binding 期望唯一，实际=$($hits.Count)。" }
    $index = [int]$hits[0]
    $match = [regex]::Match($location.Lines[$index], $pattern)
    $location.Lines[$index] = $match.Groups[1].Value + 'require_hub_assignment_binding: ' + $(if ($Enabled) { 'true' } else { 'false' }) + $match.Groups[3].Value
    $fencePattern = '^' + (' ' * $location.DirectIndent) + 'hub_assignment_fence[ \t]*:'
    $activeFence = @($location.Lines | Where-Object { $_ -cmatch $fencePattern })
    if ($activeFence.Count -ne 0) { throw '[FATAL] login 模板已含活动 hub_assignment_fence，拒绝模糊覆盖。' }
    if (-not $Enabled) { return ($location.Lines -join $location.Newline) }
    $nested = ' ' * ($location.DirectIndent + 2)
    $endpointValues = @($DsFenceEndpoints | ForEach-Object { '"' + (ConvertTo-YamlDoubleQuoted $_) + '"' })
    $lines = [System.Collections.Generic.List[string]]::new($location.Lines)
    $insertAt = $index + 1
    [string[]]$newFenceLines = @(
        ((' ' * $location.DirectIndent) + 'hub_assignment_fence:')
        ($nested + 'etcd_endpoints: [' + ($endpointValues -join ', ') + ']')
        ($nested + 'etcd_prefix: "/pandora/ds-auth/"')
        ($nested + 'etcd_lease_ttl_sec: 15')
        ($nested + 'etcd_dial_timeout: "5s"')
        ($nested + 'keyset_revision: "' + (ConvertTo-YamlDoubleQuoted $DsFenceKeysetRevision) + '"')
    )
    foreach ($line in $newFenceLines) {
        $lines.Insert($insertAt, $line)
        $insertAt++
    }
    return (@($lines) -join $location.Newline)
}

function Assert-YamlRedisAuthorityBundle {
    param([string]$ServiceName, [string]$Text)
    $authority = Get-YamlSectionSecretLocation $ServiceName $Text 'ds_auth' 'authority_mode'
    if ($authority.RawValue -cne 'redis') { throw "[FATAL] $ServiceName authority_mode 未原子切到 redis。" }
    foreach ($needle in @(
        'fence:', 'etcd_prefix: "/pandora/ds-auth/"', 'etcd_lease_ttl_sec: 15',
        'etcd_dial_timeout: "5s"', 'keyset_revision: "' + (ConvertTo-YamlDoubleQuoted $DsFenceKeysetRevision) + '"'
    )) {
        if (-not $Text.Contains($needle)) { throw "[FATAL] $ServiceName 缺少 fence 字段:$needle" }
    }
    foreach ($endpoint in $DsFenceEndpoints) {
        if (-not $Text.Contains('"' + (ConvertTo-YamlDoubleQuoted $endpoint) + '"')) {
            throw "[FATAL] $ServiceName fence 缺 endpoint:$endpoint"
        }
    }
    $direct = ' ' * $authority.DirectIndent
    $nested = ' ' * ($authority.DirectIndent + 2)
    # 只在 ds_auth section 内计数；login 同时有 login.hub_assignment_fence，若对整文件
    # 数缩进相同的 etcd_endpoints 会把合法双 fence 误判成重复。
    $sectionLines = $authority.Lines[$authority.SectionIndex..($authority.SectionEnd - 1)]
    $sectionText = $sectionLines -join $authority.Newline
    if (([regex]::Matches($sectionText, '(?m)^' + [regex]::Escape($direct) + 'fence:[ \t]*\r?$')).Count -ne 1 -or
        ([regex]::Matches($sectionText, '(?m)^' + [regex]::Escape($nested) + 'etcd_endpoints:[ \t]*\[.+\][ \t]*\r?$')).Count -ne 1) {
        throw "[FATAL] $ServiceName fence 必须是唯一的嵌套 YAML 节点，拒绝同一行/错误缩进。"
    }
}

function Assert-LoginHubAssignmentBinding {
    param([string]$Text, [bool]$Expected)
    $want = 'require_hub_assignment_binding: ' + $(if ($Expected) { 'true' } else { 'false' })
    if (([regex]::Matches($Text, '(?m)^[ ]*require_hub_assignment_binding[ \t]*:[ \t]*(?:true|false)[ \t]*(?:#.*)?$')).Count -ne 1 -or -not $Text.Contains($want)) {
        throw "[FATAL] login assignment binding 与 authority bundle 不一致，期望=$Expected。"
    }
    if ($Expected) {
        foreach ($needle in @('hub_assignment_fence:', 'etcd_prefix: "/pandora/ds-auth/"', 'keyset_revision: "' + (ConvertTo-YamlDoubleQuoted $DsFenceKeysetRevision) + '"')) {
            if (-not $Text.Contains($needle)) { throw "[FATAL] login 缺少 assignment fence 字段:$needle" }
        }
        if (([regex]::Matches($Text, '(?m)^[ ]{2}hub_assignment_fence:[ \t]*\r?$')).Count -ne 1 -or
            ([regex]::Matches($Text, '(?m)^[ ]{4}etcd_endpoints:[ \t]*\[.+\][ \t]*\r?$')).Count -lt 1) {
            throw '[FATAL] login hub_assignment_fence 必须是嵌套 YAML 节点。'
        }
    }
}

# ===== DSTicket v2(方案 B,RS256)签发段注入 =====
# 在唯一 jwt: 节段之后、同缩进插入 ds_ticket 段(matchmaker/hub-allocator 顶级;login 在 login: 段内,
# 与其 jwt 同级,与 Go 侧 conf 结构一致)。dev 模板不带 ds_ticket(本地/docker 链路沿用 legacy HS256
# + local-off);agones 链路由本生成器机械注入,不靠人工改 YAML。
function Add-YamlDsTicketSection {
    param([string]$ServiceName, [string]$Text)
    if ([regex]::IsMatch($Text, '(?m)^[ ]*ds_ticket:[ \t]*(?:#.*)?\r?$')) {
        throw "[FATAL] $ServiceName 模板已存在 ds_ticket 节点,拒绝重复/模糊注入。"
    }
    $location = Get-YamlSectionSecretLocation $ServiceName $Text 'jwt' 'secret'
    $sectionIndent = [regex]::Match($location.Lines[$location.SectionIndex], '^([ ]*)').Groups[1].Value
    $nested = $sectionIndent + '  '
    $lines = [System.Collections.Generic.List[string]]::new($location.Lines)
    $insertAt = [int]$location.SectionEnd
    [string[]]$newLines = @(
        ($sectionIndent + 'ds_ticket:')
        ($nested + 'private_key_file: "/run/secrets/pandora-dsticket/private.pem"')
        ($nested + 'active_kid: "' + $DsTicketActiveKid + '"')
        ($nested + 'ttl: "' + $DsTicketTTL + '"')
    )
    if ($ServiceName -ceq 'login') {
        # Login 仍是签票方，但 VerifyDSTicket 诊断/兼容路径只读公开 overlap JWKS。
        # ConfigMap 为 namespaced 对象：Login 在 pandora，Fleet/DS 在 default，bootstrap 会在
        # 两个 namespace 建立内容和 hash 完全相同的 immutable ConfigMap。
        $newLines += @(
            ($nested + 'jwks_file: "/run/config/pandora-dsticket/jwks.json"')
            ($nested + 'keyset_revision: "' + $DsTicketKeysetRevision + '"')
        )
    }
    foreach ($line in $newLines) { $lines.Insert($insertAt, $line); $insertAt++ }
    return (@($lines) -join $location.Newline)
}

function Assert-YamlDsTicketSection {
    param([string]$ServiceName, [string]$Text)
    if (([regex]::Matches($Text, '(?m)^[ ]*ds_ticket:[ \t]*\r?$')).Count -ne 1) {
        throw "[FATAL] $ServiceName 期望唯一 ds_ticket 节段。"
    }
    foreach ($needle in @(
        'private_key_file: "/run/secrets/pandora-dsticket/private.pem"',
        ('active_kid: "' + $DsTicketActiveKid + '"'),
        ('ttl: "' + $DsTicketTTL + '"')
    )) {
        if (-not $Text.Contains($needle)) { throw "[FATAL] $ServiceName ds_ticket 段缺字段:$needle" }
    }
    if ($ServiceName -ceq 'login') {
        foreach ($needle in @(
            'jwks_file: "/run/config/pandora-dsticket/jwks.json"',
            ('keyset_revision: "' + $DsTicketKeysetRevision + '"')
        )) {
            if (-not $Text.Contains($needle)) { throw "[FATAL] login ds_ticket verifier 缺字段:$needle" }
        }
    } elseif ($Text.Contains('jwks_file: "/run/config/pandora-dsticket/jwks.json"')) {
        throw "[FATAL] $ServiceName 不应启用 Login-only DSTicket verifier 挂载。"
    }
}

function Assert-NoYamlDsTicketSection {
    param([string]$ServiceName, [string]$Text)
    if ([regex]::IsMatch($Text, '(?m)^[ ]*ds_ticket:[ \t]*(?:#.*)?\r?$')) {
        throw "[FATAL] $ServiceName 非 agones 链路不得携带 ds_ticket 节点(模板漂移?),拒绝生成。"
    }
}

# ===== match.ds_local_profile 与 DSTicket v2 互斥收口 =====
# dev 模板给 Windows 本机联调档写死 `ds_local_profile: "local-off-v1"`(签/验都锁 HS256LocalOff)。
# agones 链路由 Add-YamlDsTicketSection 注入 RS256 私钥,两者**同时出现服务会拒启**
# (Go 侧 matchmaker/main.go 报 ds_ticket_profile_conflict 后 exit)。注入 v2 的同时必须把本档清空,
# 否则产物是"半配置"形态,而且生成期一切正常、要到 Pod CrashLoopBackOff 才看得出来。
# 2026-08-06 实测:matchmaker 与 matchmaker-pve 两个副本全部 CrashLoop,启动卡在 [7/8] rollout 超时。
function Clear-YamlDsLocalProfile {
    param([string]$ServiceName, [string]$Text)
    $pattern = '(?m)^([ \t]*)ds_local_profile:[ \t]*"[^"]+"[ \t]*(?:#.*)?$'
    $count = ([regex]::Matches($Text, $pattern)).Count
    if ($count -eq 0) { return $Text }   # 该服务模板没有本地档,无需处理
    if ($count -ne 1) {
        throw "[FATAL] $ServiceName 的 ds_local_profile 锚点异常(count=$count),拒绝静默改写。"
    }
    return [regex]::Replace($Text, $pattern, '${1}ds_local_profile: ""   # agones 链路由生成器清空(与 ds_ticket v2 互斥)', 1)
}

function Assert-NoYamlDsLocalProfile {
    param([string]$ServiceName, [string]$Text)
    if ([regex]::IsMatch($Text, '(?m)^[ \t]*ds_local_profile:[ \t]*"[^"]+"[ \t]*(?:#.*)?$')) {
        throw "[FATAL] $ServiceName 的 agones 产物仍带非空 ds_local_profile,与注入的 DSTicket v2 私钥互斥,服务会拒启。"
    }
}

function Convert-Secret([string]$ServiceName, [string]$Text) {
    $hasPlayerSection = [regex]::IsMatch($Text, '(?m)^[ ]*jwt:[ \t]*(?:#.*)?\r?$')
    $hasDsSection = [regex]::IsMatch($Text, '(?m)^[ ]*ds_auth:[ \t]*(?:#.*)?\r?$')
    $expectsPlayer = $PlayerSecretServiceNames -contains $ServiceName
    $expectsDs = $DsSecretServiceNames -contains $ServiceName
    if ($hasPlayerSection -ne $expectsPlayer) { throw "[FATAL] $ServiceName 的 jwt 节点与权威服务清单不一致。" }
    if ($hasDsSection -ne $expectsDs) { throw "[FATAL] $ServiceName 的 ds_auth 节点与权威服务清单不一致。" }

    if ($expectsPlayer) {
        if ($null -ne $PlayerSecretToInject) { $Text = Set-YamlSectionSecret $ServiceName $Text 'jwt' $PlayerSecretToInject }
        else { Assert-YamlSectionSecret $ServiceName $Text 'jwt' $DevPublicSecret }
        if ($null -ne $PlayerAdditionalToInject) { $Text = Add-YamlSectionAdditionalSecrets $ServiceName $Text 'jwt' $PlayerAdditionalToInject }
        else { Assert-NoYamlSectionAdditionalSecrets $ServiceName $Text 'jwt' }
    }
    if ($expectsDs) {
        if ($null -ne $DsSecretToInject) { $Text = Set-YamlSectionSecret $ServiceName $Text 'ds_auth' $DsSecretToInject }
        else {
            # committed dev 模板仍以历史共享值作为唯一可接受输入；生成产物必须把 DS callback
            # 域确定性改写为另一把公开 dev key，避免本地 Model-B 因跨域复用而 fail-closed。
            Assert-YamlSectionSecret $ServiceName $Text 'ds_auth' $DevPublicSecret
            $Text = Set-YamlSectionSecret $ServiceName $Text 'ds_auth' $DevDsCallbackSecret
        }
        if ($null -ne $DsAdditionalToInject) { $Text = Add-YamlSectionAdditionalSecrets $ServiceName $Text 'ds_auth' $DsAdditionalToInject }
        else { Assert-NoYamlSectionAdditionalSecrets $ServiceName $Text 'ds_auth' }
        if ($null -ne $DsAuthModeToInject) { $Text = Set-YamlDsAuthMode $ServiceName $Text $DsAuthModeToInject }
        if ($null -ne $DsAuthorityModeToInject) { $Text = Set-YamlDsAuthorityMode $ServiceName $Text $DsAuthorityModeToInject }
        if ($DsAuthorityModeToInject -eq 'redis') { $Text = Add-YamlDsAuthFence $ServiceName $Text }
    }
    if ($ServiceName -eq 'login' -and $null -ne $DsAuthorityModeToInject) {
        $Text = Set-LoginHubAssignmentBinding $Text ($DsAuthorityModeToInject -eq 'redis')
    }
    if ($DsTicketServiceNames -contains $ServiceName) {
        if ($AllocatorMode -eq 'agones') {
            $Text = Add-YamlDsTicketSection $ServiceName $Text
            # 注入 v2 私钥的同时清空互斥的本机联调档,否则服务启动即 ds_ticket_profile_conflict 退出。
            $Text = Clear-YamlDsLocalProfile $ServiceName $Text
        }
        else { Assert-NoYamlDsTicketSection $ServiceName $Text }
    }
    foreach ($binding in @($PlacementSecretBindings | Where-Object Service -CEQ $ServiceName)) {
        $devValue = [string]$DevPlacementSecrets[$binding.Kind]
        $effectiveValue = [string]$EffectivePlacementSecrets[$binding.Kind]
        if ($effectiveValue -ceq $devValue) {
            Assert-YamlDirectString $ServiceName $Text $binding.Section $binding.Child $devValue
        } else {
            $Text = Set-YamlDirectString $ServiceName $Text $binding.Section $binding.Child $devValue $effectiveValue
        }
    }
    foreach ($binding in @($MatchResumeAuthSecretBindings | Where-Object Service -CEQ $ServiceName)) {
        if ($EffectiveMatchResumeAuthSecret -ceq $DevMatchResumeAuthSecret) {
            Assert-YamlDirectString $ServiceName $Text $binding.Section $binding.Child $DevMatchResumeAuthSecret
        } else {
            $Text = Set-YamlDirectString $ServiceName $Text $binding.Section $binding.Child `
                $DevMatchResumeAuthSecret $EffectiveMatchResumeAuthSecret
        }
    }
    foreach ($binding in @($TeamResumeAuthSecretBindings | Where-Object Service -CEQ $ServiceName)) {
        if ($EffectiveTeamResumeAuthSecret -ceq $DevTeamResumeAuthSecret) {
            Assert-YamlDirectString $ServiceName $Text $binding.Section $binding.Child $DevTeamResumeAuthSecret
        } else {
            $Text = Set-YamlDirectString $ServiceName $Text $binding.Section $binding.Child `
                $DevTeamResumeAuthSecret $EffectiveTeamResumeAuthSecret
        }
    }
    foreach ($binding in @($PlayerNoResolveAuthSecretBindings | Where-Object Service -CEQ $ServiceName)) {
        if ($EffectivePlayerNoResolveAuthSecret -ceq $DevPlayerNoResolveAuthSecret) {
            Assert-YamlDirectString $ServiceName $Text $binding.Section $binding.Child $DevPlayerNoResolveAuthSecret
        } else {
            $Text = Set-YamlDirectString $ServiceName $Text $binding.Section $binding.Child `
                $DevPlayerNoResolveAuthSecret $EffectivePlayerNoResolveAuthSecret
        }
    }
    foreach ($binding in @($PlayerNoResolveAuthAudienceBindings | Where-Object Service -CEQ $ServiceName)) {
        Assert-YamlDirectString $ServiceName $Text $binding.Section $binding.Child $PlayerNoResolveAuthAudience
    }
    foreach ($binding in @($PlayerNoResolveAddressBindings | Where-Object Service -CEQ $ServiceName)) {
        Assert-YamlDirectString $ServiceName $Text $binding.Section $binding.Child $binding.Value
    }
    foreach ($binding in @($PlayerNameResolveAuthSecretBindings | Where-Object Service -CEQ $ServiceName)) {
        if ($EffectivePlayerNameResolveAuthSecret -ceq $DevPlayerNameResolveAuthSecret) {
            Assert-YamlDirectString $ServiceName $Text $binding.Section $binding.Child $DevPlayerNameResolveAuthSecret
        } else {
            $Text = Set-YamlDirectString $ServiceName $Text $binding.Section $binding.Child `
                $DevPlayerNameResolveAuthSecret $EffectivePlayerNameResolveAuthSecret
        }
    }
    foreach ($binding in @($PlayerNameResolveAuthAudienceBindings | Where-Object Service -CEQ $ServiceName)) {
        Assert-YamlDirectString $ServiceName $Text $binding.Section $binding.Child $PlayerNameResolveAuthAudience
    }
    foreach ($binding in @($PlayerNameResolveAddressBindings | Where-Object Service -CEQ $ServiceName)) {
        Assert-YamlDirectString $ServiceName $Text $binding.Section $binding.Child $binding.Value
    }
    foreach ($group in $ApplicationDisplayAuthGroups) {
        foreach ($binding in @($group.SecretBindings | Where-Object Service -CEQ $ServiceName)) {
            if ($group.Effective -ceq $group.Dev) {
                Assert-YamlDirectString $ServiceName $Text $binding.Section $binding.Child $group.Dev
            } else {
                $Text = Set-YamlDirectString $ServiceName $Text $binding.Section $binding.Child `
                    $group.Dev $group.Effective
            }
        }
        foreach ($binding in @($group.AudienceBindings | Where-Object Service -CEQ $ServiceName)) {
            Assert-YamlDirectString $ServiceName $Text $binding.Section $binding.Child $group.Audience
        }
        foreach ($binding in @($group.AddressBindings | Where-Object Service -CEQ $ServiceName)) {
            Assert-YamlDirectString $ServiceName $Text $binding.Section $binding.Child $binding.Value
        }
    }
    foreach ($binding in @($AllocationAbortAuthSecretBindings | Where-Object Service -CEQ $ServiceName)) {
        if ($EffectiveAllocationAbortAuthSecret -ceq $DevAllocationAbortAuthSecret) {
            Assert-YamlDirectString $ServiceName $Text $binding.Section $binding.Child $DevAllocationAbortAuthSecret
        } else {
            $Text = Set-YamlDirectString $ServiceName $Text $binding.Section $binding.Child `
                $DevAllocationAbortAuthSecret $EffectiveAllocationAbortAuthSecret
        }
    }
    return $Text
}
# Sync-EnvoyJwks:注入真密钥时,必须同步 Envoy 客户端面(:8443)的 local_jwks,否则 login 用新密钥
# 签的 SessionToken 会被 envoy(仍是 dev JWKS)全部拒掉。做两件事(§5 审核:生成器要同步/校验 JWKS):
#   1) 把匹配的 JWKS(base64url(secret))写进 <OutDir>/envoy-jwks.json(run/ 已 gitignore,不落库),
#      供运维把边缘 Envoy 的 inline_string 换成它 / 或挂载覆盖。
#   2) 校验仓库内 deploy/envoy/envoy.yaml:若它仍内联 dev JWKS 而我们注入了新密钥,大声告警
#      (不自动改committed 文件:严禁把真密钥写进 git 跟踪文件,AGENTS.md §3/§10)。
# 密钥指纹(与 pkg/auth.keyFingerprint 同源:hex(SHA256(secret)[:8]),16 个小写 hex)。
# 多 key JWKS 的 kid 用它:与 Go 侧 SignDSCallback 写入的 kid 一致,日志/审计可对应到具体密钥。
function Get-KeyFingerprint([string]$s) {
    $h = [System.Security.Cryptography.SHA256]::HashData([System.Text.Encoding]::UTF8.GetBytes($s))
    return ([System.BitConverter]::ToString($h[0..7]) -replace '-', '').ToLowerInvariant()
}

# 组装玩家面 JWKS 文本:单 key 保持历史格式(kid=pandora-dev)不变;带轮换旧密钥时双 key,
# kid 用各自密钥指纹(HS256 验签不依赖 kid 匹配,Envoy 会逐 key 尝试;kid 仅供审计)。
function Get-PlayerJwksText {
    $k = ConvertTo-Base64Url $PlayerSecretToInject
    if ($null -eq $PlayerAdditionalToInject) {
        return '{"keys":[{"kty":"oct","alg":"HS256","kid":"pandora-dev","k":"' + $k + '"}]}'
    }
    $kAdd = ConvertTo-Base64Url $PlayerAdditionalToInject
    return '{"keys":[' +
        '{"kty":"oct","alg":"HS256","kid":"' + (Get-KeyFingerprint $PlayerSecretToInject) + '","k":"' + $k + '"},' +
        '{"kty":"oct","alg":"HS256","kid":"' + (Get-KeyFingerprint $PlayerAdditionalToInject) + '","k":"' + $kAdd + '"}]}'
}

function Sync-EnvoyJwks([string]$TargetDir) {
    # Envoy :8443 客户端面校验的是**玩家面**令牌(SessionToken / DSTicket),故 JWKS 用玩家面密钥。
    $jwksPath = Join-Path $TargetDir 'envoy-jwks.json'
    if ($null -eq $PlayerSecretToInject) {
        # staging 中不生成 JWKS；发布事务会删除最终目录中由本生成器拥有的陈旧 JWKS。
        return
    }
    $jwks = Get-PlayerJwksText
    [System.IO.File]::WriteAllText($jwksPath, $jwks + "`n", (New-Object System.Text.UTF8Encoding($false)))
    Write-Host "[ OK ] staging 已生成匹配的 Envoy JWKS(keys=$(if ($null -ne $PlayerAdditionalToInject) { 2 } else { 1 }));事务发布成功后路径=$(Join-Path $OutDir 'envoy-jwks.json')" -ForegroundColor Green

    # 校验 committed envoy.yaml 是否仍是 dev JWKS。
    $envoyYaml = Join-Path $ProjectRoot 'deploy/envoy/envoy.yaml'
    if (Test-Path $envoyYaml) {
        $devK = ConvertTo-Base64Url $DevPublicSecret
        $k = ConvertTo-Base64Url $PlayerSecretToInject
        $ec = Get-Content $envoyYaml -Raw
        if ($ec.Contains($devK) -and -not $ec.Contains($k)) {
            Write-Host "[WARN] deploy/envoy/envoy.yaml 仍内联 dev JWKS(k=$devK)。生产必须改用上面的 envoy-jwks.json," -ForegroundColor Yellow
            Write-Host "       否则 login 用新密钥签的 SessionToken 会被 Envoy 全部拒绝。(不自动改 committed 文件:严禁把真密钥写进 git)" -ForegroundColor Yellow
        }
    }
}


# ===== 服务清单(name; 相对 dev 配置路径)=====
# port 用于把同伴服务的 127.0.0.1:<port> 换成 <svc>:<port>(端口不变,只换 host)。
# Name 用「连字符」形式:同时满足 docker-compose 服务名与 k8s Service 名(k8s 禁止下划线),
# docker / k8s 两边据此短名解析,所以同一份产物通用。
$Services = @(
    @{ Name = 'login';          Conf = 'services/account/login/etc/login-dev.yaml';                Port = 20001 }
    @{ Name = 'player';         Conf = 'services/account/player/etc/player-dev.yaml';              Port = 20002 }
    @{ Name = 'data-service';   Conf = 'services/data/data_service/etc/data_service-dev.yaml';     Port = 20003 }
    @{ Name = 'friend';         Conf = 'services/social/friend/etc/friend-dev.yaml';               Port = 20004 }
    @{ Name = 'chat';           Conf = 'services/social/chat/etc/chat-dev.yaml';                   Port = 20005 }
    @{ Name = 'player-locator'; Conf = 'services/runtime/player_locator/etc/locator-dev.yaml';     Port = 20006 }
    @{ Name = 'leaderboard';    Conf = 'services/runtime/leaderboard/etc/leaderboard-dev.yaml';    Port = 20007 }
    @{ Name = 'owner';          Conf = 'services/runtime/owner/etc/owner-dev.yaml';                Port = 20017 }
    @{ Name = 'guild';          Conf = 'services/social/guild/etc/guild-dev.yaml';                 Port = 20008 }
    @{ Name = 'mail';           Conf = 'services/social/mail/etc/mail-dev.yaml';                   Port = 20009 }
    @{ Name = 'team';           Conf = 'services/matchmaking/team/etc/team-dev.yaml';              Port = 20010 }
    @{ Name = 'matchmaker';     Conf = 'services/matchmaking/matchmaker/etc/matchmaker-dev.yaml';  Port = 20011 }
    # PVE 直进匹配实例:同 matchmaker 二进制、不同配置(game_mode=pve_coop + walk_in)。
    @{ Name = 'matchmaker-pve'; Conf = 'services/matchmaking/matchmaker/etc/matchmaker-pve.yaml';  Port = 20018 }
    @{ Name = 'trade';          Conf = 'services/economy/trade/etc/trade-dev.yaml';                Port = 20012 }
    @{ Name = 'dialogue';       Conf = 'services/social/dialogue/etc/dialogue-dev.yaml';           Port = 20013 }
    @{ Name = 'push';           Conf = 'services/runtime/push/etc/push-dev.yaml';                  Port = 20014 }
    @{ Name = 'inventory';      Conf = 'services/economy/inventory/etc/inventory-dev.yaml';        Port = 20015 }
    @{ Name = 'auction';        Conf = 'services/economy/auction/etc/auction-dev.yaml';            Port = 20016 }
    @{ Name = 'mission';        Conf = 'services/social/mission/etc/mission-dev.yaml';             Port = 20019 }
    @{ Name = 'ds-allocator';   Conf = 'services/battle/ds_allocator/etc/ds_allocator-dev.yaml';   Port = 20020 }
    @{ Name = 'hub-allocator';  Conf = 'services/battle/hub_allocator/etc/hub_allocator-dev.yaml'; Port = 20021 }
    @{ Name = 'battle-result';  Conf = 'services/battle/battle_result/etc/battle_result-dev.yaml'; Port = 20022 }
)

# 同伴服务 host 映射:127.0.0.1:<port> -> <svc>:<port>
$PortToHost = @{}
foreach ($s in $Services) { $PortToHost[[string]$s.Port] = $s.Name }

# 混合(含战斗)模式:ds/hub allocator 跑宿主而非容器,把它们的同伴地址从 docker 服务名
# (ds-allocator/hub-allocator)改指 host.docker.internal —— 容器内经该名回连宿主发布端口。
# 只影响调用方(matchmaker/battle_result→20020、login→20021)的地址改写,不改 allocator 自身。
if ($HostAllocators) {
    $PortToHost['20020'] = 'host.docker.internal'
    $PortToHost['20021'] = 'host.docker.internal'
}

function Convert-DevToCluster([string]$text) {
    # 1) 基础设施地址(host:port 都变)
    $text = $text.Replace('127.0.0.1:3307', 'mysql:3306')
    $text = $text.Replace('127.0.0.1:6380', 'redis:6379')
    $text = $text.Replace('127.0.0.1:9093', 'kafka:9092')
    $text = $text.Replace('localhost:9093', 'kafka:9092')
    $text = $text.Replace('127.0.0.1:2380', 'etcd:2379')

    # 2) 同伴服务地址:host 换成服务短名,端口不变(容器内仍监听同端口)
    foreach ($port in $PortToHost.Keys) {
        $svc = $PortToHost[$port]
        $text = $text.Replace("127.0.0.1:$port", "${svc}:$port")
        $text = $text.Replace("localhost:$port", "${svc}:$port")
    }

    return $text
}

# MultiReplicaSnowflakeServices 是「集群产物必须用 etcd 独占 snowflake nodeID」的服务清单。
#
# 判据只有一条:**该服务在集群里可能同时跑多于一个副本**,且它会发 snowflake ID。
# static 模式下所有副本共用配置里的同一个 node_id,两个副本的发号器会发出**逐位相同**的 ID
# (pkg/snowflake/etcdnode.ProvideSnowflakeN 契约,已有单测钉死),直接产出重号。
#
# 2026-07-28 起清单 = **全部发号服务**(调用 etcdnode.MustProvideSnowflake* 的 13 个部署):
# services.yaml 现已**全部**是 RollingUpdate(§9.16/§9.21 不停服硬要求,Recreate 被审计否决;
# 最后一个例外 ds-allocator 于 2026-07-29 随 INC-20260729-001 收口,它不发号故不在本清单),
# replicas=1 + maxSurge 也意味着**每次发版都有新旧两副本并存窗口**——
# static 同 node_id 在该窗口内双活发重号,不是「将来上金丝雀才会遇到」的问题。
# 非发号服务(player / data-service / player-locator / owner / push / ds-allocator /
# hub-allocator / battle-result)不在列:它们不 import etcdnode,无需为此引入 etcd 依赖。
#
# ⚠️ 进本清单 = "多副本不会发重号",**不等于**"该服务可以安全多副本"。已知例外:
#   - mail:ListMail 的系统/公会增量按 AdvanceCursor(max mail_id) 推水位,依赖"ID 顺序 =
#     提交顺序",而这只在单发号器内成立。mail 扩 >1 副本(含金丝雀)前须先改单写者或
#     换游标口径,详见 services/social/mail/cmd/mail/main.go 的说明。
#   - matchmaker:>1 副本前须开 match.leader.enabled(见 services.yaml 注释),否则每个
#     副本都跑撮合循环重复成局。
# 扩副本 / 上灰度前先核对该服务自身的单写者约束,不要只看本清单。
$script:MultiReplicaSnowflakeServices = @(
    'login', 'friend', 'chat', 'leaderboard', 'guild', 'mail', 'team',
    'matchmaker', 'matchmaker-pve', 'trade', 'dialogue', 'inventory', 'auction'
)

# SnowflakeEtcdNamespaceOverride:跨服务/跨部署**共铸同一种 ID** 的空间必须共用同一个
# etcd_service_name(infra.md §8.2②)。etcd 的 nodeID 空间默认按服务名隔离、跨服务刻意
# 复用 nodeID——共铸方若各占一个命名空间,会稳定同时分到 nodeID 0,发出逐位相同的号。
#
#   - instance-id:instance_id 由 inventory(GrantInstances)与 mail(buildClaimIntent)
#     两个服务共铸,汇进同一玩家背包段,重号会被 bag_apply 的 duplicate 检查 fail-closed
#     拒掉(玩家领不了邮件)。static 下靠 inventory=1 / mail=2 错开,etcd 下靠共用池。
#   - matchmaker:match_id(和 ticket_id)由 matchmaker 与 matchmaker-pve 两个部署
#     (同一二进制)共铸,流入同一 battle 结算链路(match_id 是战斗结果幂等键 §9.2)。
#     static 下靠 pvp=1 / pve=2 错开,etcd 下必须共用 "matchmaker" 命名空间。
#
# 未列出的服务默认用自己的部署名做命名空间。新增「两个服务铸同一种 ID」前先问:
# 能不能只让一个服务铸(§9.22 唯一权威);确须共铸,必须同步登记本表。
$script:SnowflakeEtcdNamespaceOverride = @{
    'inventory'      = 'instance-id'
    'mail'           = 'instance-id'
    'matchmaker-pve' = 'matchmaker'
}

# Set-ClusterSnowflakeEtcd 给集群产物追加 etcd 独占 nodeID 的 snowflake 配置块。
# dev 源配置**不含** snowflake 块(static 走零值 + node.node_id),避免本机只启一个服务时
# 额外依赖 etcd;位布局/Epoch 是 pkg/snowflake 编译期常量,本就不该出现在配置里。
function Set-ClusterSnowflakeEtcd([string]$serviceName, [string]$text) {
    if ([regex]::IsMatch($text, '(?m)^snowflake:')) {
        throw "[FATAL] $serviceName dev 配置含 snowflake: 块。集群 etcd 注入由生成器整块追加,dev 配置应保持无该块(static 由零值驱动);若确需 dev 显式配置,请人工确认改写规则后再生成。"
    }

    $ns = $script:SnowflakeEtcdNamespaceOverride[$serviceName]
    if (-not $ns) { $ns = $serviceName }

    if (-not $text.EndsWith("`n")) { $text += "`n" }
    return $text + "`nsnowflake:`n  node_id_source: etcd`n  etcd_endpoints: [`"etcd:2379`"]`n  etcd_prefix: `"/pandora/snowflake/node/`"`n  etcd_service_name: `"$ns`"`n  etcd_lease_ttl_sec: 15`n"
}

# auction 另需开启跨实例 market 锁(与 snowflake 无关,单独一条)。
function Set-AuctionCrossInstanceLock([string]$text) {
    $crossLockCount = [regex]::Matches($text, '(?m)^[ \t]{2}cross_instance_lock:[ \t]*(?:true|false)[ \t]*$').Count
    if ($crossLockCount -ne 1) {
        throw "[FATAL] auction cross_instance_lock 锚点异常:count=$crossLockCount"
    }
    return [regex]::Replace(
        $text,
        '(?m)^([ \t]{2}cross_instance_lock:)[ \t]*(?:true|false)[ \t]*$',
        '$1 true',
        1)
}

# -Prod 机械关断实时成长通道(审核 P0,2026-07-21):生成链以 dev 模板为唯一输入,
# dev 里 progress_enabled: true 只服务本地联调。线上产物若顺手继承,新旧 fleet 混跑
# 窗口会实时+终局双发掉落(§9.21)。玩家升级曲线已迁到 configtable 单一数据源。
# 生产启用是**独立显式动作**:battle_result 全 fleet 升级完成后按 realtime-progression.md
# 发布纪律另行开启(人工改产物或未来 configtable 门禁),不允许由本生成器默认放行。
function Set-ProdBattleResultProgressOff([string]$text) {
    $anchorCount = [regex]::Matches($text, '(?m)^[ \t]{2}progress_enabled:[ \t]*(?:true|false)[ \t]*(?:#.*)?$').Count
    if ($anchorCount -ne 1) {
        throw "[FATAL] battle-result 模板 progress_enabled 锚点异常(count=$anchorCount),拒绝生成 -Prod 产物。"
    }
    return [regex]::Replace($text,
        '(?m)^([ \t]{2})progress_enabled:[ \t]*(?:true|false)[ \t]*(?:#.*)?$',
        '${1}progress_enabled: false', 1)
}

function Set-ServiceClusterConfigTableDir([string]$serviceName, [string]$text) {
    $location = Get-YamlSectionSecretLocation $serviceName $text 'config_table' 'dir'
    if ($location.RawValue -cne '../../../configtable/dist') {
        throw "[FATAL] $serviceName.config_table.dir 不是宿主 dev 模板路径,拒绝静默覆盖未知配置。"
    }
    $location.Lines[$location.SecretIndex] = $location.Prefix + '"/app/configtable/active"' + $location.Suffix
    return ($location.Lines -join $location.Newline)
}

# -Prod 机械把 login 的 hub_allocator 地址改成 headless + dns:/// 方案(P0#5 收口,2026-07-25)。
# hub_allocator 是单写者:同一时刻只有当选副本能写,其余副本对写请求返回可重试 UNAVAILABLE。
# 走 ClusterIP(`hub-allocator:20021`)时 gRPC 用 passthrough 解析 + L4 长连接,会被**钉在**某一
# 个 Pod;钉到热备副本时 AssignHub 的就地重试全落同一 Pod = 白重试(audit-residual A2)。
# login 侧已改用 grpcclient.MustDialInsecureRoundRobin,但只有目标是 `dns:///<headless FQDN>`
# 时 round_robin 才拿得到全部 Ready Pod IP。此前只有 login-prod.yaml.example 手写了该地址,
# **标准生成链仍产出 ClusterIP 短名**——即生产实际配置绕过了整条修复链,故在此机械收口。
#
# 只在 -Prod 生效:同一份产物 docker-compose 与 k8s 共用(见文件头),headless FQDN 在
# compose 里无法解析;-Prod 的部署目标固定是 deploy/k8s(namespace=pandora,与
# services.yaml 的 hub-allocator-headless Service 一致)。
function Set-ProdLoginHubHeadlessAddr([string]$text) {
    $pattern = '(?m)^([ \t]{4})addr:[ \t]*"hub-allocator:20021"[ \t]*(?:#.*)?$'
    $anchorCount = [regex]::Matches($text, $pattern).Count
    if ($anchorCount -ne 1) {
        throw "[FATAL] login 模板 hub.addr 锚点异常(count=$anchorCount;-HostAllocators 与 -Prod 不可同用),拒绝生成 -Prod 产物。"
    }
    return [regex]::Replace($text, $pattern,
        '${1}addr: "dns:///hub-allocator-headless.pandora.svc.cluster.local:20021"', 1)
}

function Set-ProdPlayerExperienceOff([string]$text) {
    $pattern = '(?m)^([ \t]{2})experience_enabled:[ \t]*(?:true|false)[ \t]*(?:#.*)?$'
    $anchorCount = [regex]::Matches($text, $pattern).Count
    if ($anchorCount -ne 1) {
        throw "[FATAL] player 模板 experience_enabled 锚点异常(count=$anchorCount),拒绝生成 -Prod 产物。"
    }
    return [regex]::Replace($text, $pattern, '${1}experience_enabled: false', 1)
}

# -Prod 机械强制 push 会话现行性门(审核 P0,INC-20260722-004,2026-07-22):JWT 验签只证明
# "曾经登录过",旧/被顶号 token 在 exp 前仍能过 Envoy jwt_authn 重建 Subscribe 流收私有推送。
# dev 模板 require_session_gate: false 只服务直连内网端口联调;生产必须 true(建流校验
# jti == login 会话权威当前一代,权威不可达 fail-closed 拒),不允许由产物继承 dev 宽松档。
function Set-ProdPushSessionGateOn([string]$text) {
    $pattern = '(?m)^([ \t]{2})require_session_gate:[ \t]*(?:true|false)[ \t]*(?:#.*)?$'
    $anchorCount = [regex]::Matches($text, $pattern).Count
    if ($anchorCount -ne 1) {
        throw "[FATAL] push 模板 require_session_gate 锚点异常(count=$anchorCount),拒绝生成 -Prod 产物。"
    }
    return [regex]::Replace($text, $pattern, '${1}require_session_gate: true', 1)
}

# -Prod 机械强制全部客户端面服务的 unary 会话现行性门(R5 复审 P0-1,INC-20260722-004,
# 2026-07-22):push 建流门只封了长连接入口,顶号后旧 JWT 在 exp 前仍能对 friend/trade/
# inventory 等所有玩家 RPC 保留按 player_id 定向能力。pkg/middleware.SessionCurrent 已在
# 各客户端面服务 unary 链接线;dev 模板 session_gate.require: false 只服务无 Redis 直连
# 联调,生产必须 true(gate 漏配/权威不可达一律 fail-closed),不允许产物继承 dev 宽松档。
$UnarySessionGateServiceNames = @(
    'friend', 'chat', 'mail', 'guild', 'trade', 'team',
    'matchmaker', 'matchmaker-pve', 'player', 'inventory', 'leaderboard', 'hub-allocator',
    'mission' # 2026-08-11 任务域上线:客户端面 RPC(envoy 已按 JWT 路由 MissionService)
)
# PushWriterLeaseServiceNames 是「推送出箱发布器必须单写者选举」的服务清单。
#
# 判据只有一条:该服务有一个**全局未分区**的推送出箱表,发布器整表 FIFO 取行,且推送
# payload 是**客户端可见的全量/绝对值快照**(后到即覆盖)。满足这三条时,两个副本并发
# 发布会让旧帧后到覆盖新帧,而事件里没有 revision 可供客户端判旧。
#   · mission:mission_push_outbox → MissionUpdateEvent.progressed(逐任务全量进度快照)
#   · player :player_push_outbox  → PlayerExperienceEvent(level / exp_in_level 绝对值)
#
# ⚠️ battle_result 的 player_update_outbox **刻意不在列**:它的下游是 player 服务、靠
# mmr_history 唯一键幂等去重,乱序与重复天然无害,且没有客户端可见的覆盖语义。为形式
# 统一给它加选举是拿复杂度换整齐(§15.3),差异是有据的,不是漂移。
$script:PushWriterLeaseServiceNames = @('mission', 'player')

# 集群产物机械强制推送发布器单写者选举(2026-08-11)。
#
# 注意本条**不是 -Prod 专属**:凡集群产物都适用。两个服务的 Deployment 都是 RollingUpdate
# (§9.16/§9.21 不停服硬要求),replicas=1 + maxSurge 也意味着**每次发版都有新旧两副本
# 并存窗口**。
#
# dev 模板写 mode: "off" 是对的(单进程无 etcd 也要起得来),但产物**不允许继承**这个
# 宽松档 —— 与 session_gate.require 同款纪律。进程侧另有 fail-closed 门禁兜底
# (main.go 读 PANDORA_DEPLOY_STRATEGY:RollingUpdate × 非 enforce 直接退出),
# 两道闸缺一不可:这里保证产物是对的,那里保证产物被改坏时炸得出来。
function Set-ClusterPushWriterLeaseEnforce([string]$svcName, [string]$text) {
    $modePattern = '(?m)^([ \t]{2}push_writer_lease:[ \t]*\r?\n[ \t]{4})mode:[ \t]*"[^"]*"[ \t]*(?:#.*)?$'
    $modeCount = [regex]::Matches($text, $modePattern).Count
    if ($modeCount -ne 1) {
        throw "[FATAL] mission 模板 push_writer_lease.mode 锚点异常(count=$modeCount),拒绝生成集群产物。"
    }
    $text = [regex]::Replace($text, $modePattern, '${1}mode: "enforce"', 1)

    # enforce 必须带 etcd_endpoints,否则进程启动即 fail-closed。dev 模板里该行是注释掉的,
    # 这里整行替换成集群地址(Convert-DevToCluster 只改已存在的 host:port,不会凭空插入键)。
    $epPattern = '(?m)^([ \t]{4})#[ \t]*etcd_endpoints:.*$'
    $epCount = [regex]::Matches($text, $epPattern).Count
    if ($epCount -ne 1) {
        throw "[FATAL] mission 模板 push_writer_lease.etcd_endpoints 注释锚点异常(count=$epCount),拒绝生成集群产物。"
    }
    return [regex]::Replace($text, $epPattern, '${1}etcd_endpoints: ["etcd:2379"]', 1)
}

function Set-ProdUnarySessionGateOn([string]$svcName, [string]$text) {
    $pattern = '(?m)^(session_gate:[ \t]*\r?\n[ \t]{2})require:[ \t]*(?:true|false)[ \t]*(?:#.*)?$'
    $anchorCount = [regex]::Matches($text, $pattern).Count
    if ($anchorCount -ne 1) {
        throw "[FATAL] $svcName 模板 session_gate.require 锚点异常(count=$anchorCount),拒绝生成 -Prod 产物。"
    }
    return [regex]::Replace($text, $pattern, '${1}require: true', 1)
}

# -Prod 机械强制客户端面服务 BBR 自适应限流(压测前审核门禁-A,2026-07-26):
# dev 模板不写 enable_rate_limit(零值 false)只服务本地联调;生产客户端面服务过载时
# 必须有 server 侧丢负载(BBR 按 CPU/inflight/RT 自适应,无需阈值),否则登录/聊天洪峰
# 只能靠 Envoy 隐式熔断 + OOMKill 兜底(个别 Pod 延迟劣化/crash-loop)。
# 模板已写 enable_rate_limit → 强制 true(锚点必须唯一);未写 → 插到 server.grpc: 首行后。
$GrpcRateLimitServiceNames = $UnarySessionGateServiceNames + @('login', 'push')
function Set-ProdGrpcRateLimitOn([string]$svcName, [string]$text) {
    $explicit = '(?m)^([ \t]+)enable_rate_limit:[ \t]*(?:true|false)[ \t]*(?:#.*)?$'
    $explicitCount = [regex]::Matches($text, $explicit).Count
    if ($explicitCount -gt 1) {
        throw "[FATAL] $svcName 模板 enable_rate_limit 锚点异常(count=$explicitCount),拒绝生成 -Prod 产物。"
    }
    if ($explicitCount -eq 1) {
        return [regex]::Replace($text, $explicit, '${1}enable_rate_limit: true', 1)
    }
    $anchor = '(?m)^([ \t]{2}grpc:[ \t]*(\r?\n))'
    $anchorCount = [regex]::Matches($text, $anchor).Count
    if ($anchorCount -ne 1) {
        throw "[FATAL] $svcName 模板 server.grpc 锚点异常(count=$anchorCount),拒绝生成 -Prod 产物。"
    }
    return [regex]::Replace($text, $anchor, '${1}    enable_rate_limit: true${2}', 1)
}

# -Prod 机械关断幂等历史的**真删**(审核 P1,2026-07-21;2026-08-10 补 retention_mode):
# dev 写 retention_mode: "delete" + 两个 *_cleanup_enabled: true 只为覆盖真删代码路径
# (本地数据可弃)。上游 progress 出箱与 kafka 重放目前没有小于留存期的有界重试,生产删
# 收据后迟到重放会重复入账经验/MMR/点数/技能卡(不可逆)。生产开启是独立显式动作:
# 上游具备有界重试后按 §9.24 前置条件另行开启。
#
# 三个键都强制,不是只关前置开关:两道闸里任意一道都足以阻止真删,但产物里若残留
# retention_mode: "delete",运维读配置会误以为清理已生效;而且将来若有新表组直接读
# RetentionMode() 而没有自己的前置闸,生产就会静默开始删。
function Set-ProdPlayerRetentionOff([string]$text) {
    foreach ($key in @('exp_history_cleanup_enabled', 'history_cleanup_enabled')) {
        $pattern = '(?m)^([ \t]{2})' + $key + ':[ \t]*(?:true|false)[ \t]*(?:#.*)?$'
        $anchorCount = [regex]::Matches($text, $pattern).Count
        if ($anchorCount -ne 1) {
            throw "[FATAL] player 模板 $key 锚点异常(count=$anchorCount),拒绝生成 -Prod 产物。"
        }
        $text = [regex]::Replace($text, $pattern, ('${1}' + $key + ': false'))
    }
    $modePattern = '(?m)^([ \t]{2})retention_mode:[ \t]*"[^"]*"[ \t]*(?:#.*)?$'
    $modeCount = [regex]::Matches($text, $modePattern).Count
    if ($modeCount -ne 1) {
        throw "[FATAL] player 模板 retention_mode 锚点异常(count=$modeCount),拒绝生成 -Prod 产物。"
    }
    return [regex]::Replace($text, $modePattern, '${1}retention_mode: "report_only"')
}

# owner 权威库 DSN 注入(§9.22):整行替换 owner.yaml 的 node.mysql_client.dsn。
# 复用 Get-YamlSectionSecretLocation 的精确定位(节点唯一 + 直接子项 + 双引号标量),
# 旧值必须仍含 dev 凭据特征(pandora_dev_pwd),证明是权威 dev 模板值,拒绝静默覆盖未知配置。
function Set-OwnerStoreDsn([string]$Text, [string]$NewDsn) {
    $location = Get-YamlSectionSecretLocation 'owner' $Text 'mysql_client' 'dsn'
    if (-not $location.RawValue.Contains('pandora_dev_pwd')) {
        throw '[FATAL] owner.mysql_client.dsn 不是权威 dev 模板值(未见 dev 凭据特征),拒绝静默覆盖未知配置。'
    }
    $location.Lines[$location.SecretIndex] = $location.Prefix + '"' + (ConvertTo-YamlDoubleQuoted $NewDsn) + '"' + $location.Suffix
    return ($location.Lines -join $location.Newline)
}

# -Prod 机械打开 owner 服务端 TiDB 强校验(§9.22):dev 模板 require_tidb: false 只服务
# 本地单机 MySQL 联调;线上产物必须 true —— owner 启动查 VERSION() 必须含 -TiDB-,
# 不符 fail-fast 拒启。与生成器侧 DSN 校验构成双层防线(DSN 字符串证不了后端真是 TiDB)。
function Set-ProdOwnerRequireTiDB([string]$text) {
    $pattern = '(?m)^([ \t]{2})require_tidb:[ \t]*(?:true|false)[ \t]*(?:#.*)?$'
    $anchorCount = [regex]::Matches($text, $pattern).Count
    if ($anchorCount -ne 1) {
        throw "[FATAL] owner 模板 require_tidb 锚点异常(count=$anchorCount),拒绝生成 -Prod 产物。"
    }
    return [regex]::Replace($text, $pattern, '${1}require_tidb: true', 1)
}

# ===== -Prod 产物 dev 数据库凭据门禁(2026-07-27 审计发现)=====
# 现状(实测):-Prod 产物里除 owner 外的**全部**服务都原样继承 dev 模板的公开凭据
# `pandora:pandora_dev_pwd@tcp(mysql:3306)/...`。生成器对 JWT/DS/placement 各类密钥都有
# 严格注入与拒绝复用,唯独数据库凭据没有任何把关 —— 密钥防线再严,库口令是公开的等于没锁。
#
# 一次性给 10 个库都加注入参数属于「泛化成统一机制」,与 §15 冲突,也需要先定生产存储拓扑
# (几个 TiDB 集群、哪个库落哪),不是本次能替用户拍板的。故按 expand→migrate→contract:
#   · $ProdDbCredentialWired  = 已接线 DSN 注入的服务 → **硬断言**不得残留 dev 凭据;
#   · $ProdDbCredentialDebt   = 尚未接线的服务 → 显式登记为已知欠账,每接线一个删一行。
# 清单删空后把本函数改成对全部产物无条件断言,并删掉 $ProdDbCredentialDebt。
# 这样既 fail-closed 守住已完成的部分,又让剩余欠账无法被静默遗忘(不是注释里的 TODO,
# 而是每次 -Prod 生成都会打印的清单)。
$ProdDbCredentialWired = @('owner', 'login')
$ProdDbCredentialDebt = @(
    'player', 'data-service', 'friend', 'chat', 'guild', 'mail',
    'inventory', 'auction', 'leaderboard', 'battle-result',
    'mission' # 2026-08-11 任务域上线:pandora_mission 存玩家任务进度与发奖流水,DSN 注入未接线
)

function Assert-ProdDbCredentials([string]$StageDir) {
    if (-not $Prod) { return }
    $leaked = @()
    foreach ($name in $ProdDbCredentialWired) {
        $path = Join-Path $StageDir "$name.yaml"
        if (-not (Test-Path -LiteralPath $path)) {
            throw "[FATAL] -Prod 产物缺少 $name.yaml,无法校验数据库凭据。"
        }
        if ((Get-Content -LiteralPath $path -Raw).Contains('pandora_dev_pwd')) {
            $leaked += $name
        }
    }
    if ($leaked.Count -gt 0) {
        throw "[FATAL] -Prod 产物残留公开 dev 数据库凭据(pandora_dev_pwd):$($leaked -join ', ')。" +
              ' 这些服务已接线 DSN 注入,出现该串说明注入未生效或模板被改,拒绝发布。'
    }
    # 未接线服务:逐次生成都把欠账打出来,防止「没人记得还有 10 个库用公开口令」。
    $stillDebt = @()
    foreach ($name in $ProdDbCredentialDebt) {
        $path = Join-Path $StageDir "$name.yaml"
        if ((Test-Path -LiteralPath $path) -and (Get-Content -LiteralPath $path -Raw).Contains('pandora_dev_pwd')) {
            $stillDebt += $name
        }
    }
    if ($stillDebt.Count -gt 0) {
        Write-Host ("[WARN] -Prod 产物仍有 {0} 个服务使用公开 dev 数据库凭据(尚未接线 DSN 注入):{1}" -f `
            $stillDebt.Count, ($stillDebt -join ', ')) -ForegroundColor Yellow
        Write-Host '       上线前必须逐个接线(参照 -OwnerStoreDsn / -AccountStoreDsn),或确认这些库不在本次发布范围。' -ForegroundColor Yellow
    }
}

# login 账号库 DSN 注入(全服单点扩容):整行替换 login.yaml 的 node.mysql_client.dsn。
# 与 Set-OwnerStoreDsn 同构:旧值必须仍含 dev 凭据特征,证明是权威 dev 模板值,
# 拒绝静默覆盖未知配置(防重复生成/手工改过的产物被无声吃掉)。
function Set-AccountStoreDsn([string]$Text, [string]$NewDsn) {
    $location = Get-YamlSectionSecretLocation 'login' $Text 'mysql_client' 'dsn'
    if (-not $location.RawValue.Contains('pandora_dev_pwd')) {
        throw '[FATAL] login.mysql_client.dsn 不是权威 dev 模板值(未见 dev 凭据特征),拒绝静默覆盖未知配置。'
    }
    $location.Lines[$location.SecretIndex] = $location.Prefix + '"' + (ConvertTo-YamlDoubleQuoted $NewDsn) + '"' + $location.Suffix
    return ($location.Lines -join $location.Newline)
}

# -Prod 机械打开 login 服务端 TiDB 强校验:与 owner 同构的双层防线 —— 生成器只能校验 DSN
# 字符串,证不了对端真是 TiDB,故服务端启动再查一次 VERSION() 并对 accounts.account 做
# collation 行为探针,不符 fail-fast 拒启(见 pkg/mysqlx/backend_check.go)。
function Set-ProdLoginRequireTiDB([string]$text) {
    $pattern = '(?m)^([ \t]{2})require_tidb:[ \t]*(?:true|false)[ \t]*(?:#.*)?$'
    $anchorCount = [regex]::Matches($text, $pattern).Count
    if ($anchorCount -ne 1) {
        throw "[FATAL] login 模板 require_tidb 锚点异常(count=$anchorCount),拒绝生成 -Prod 产物。"
    }
    return [regex]::Replace($text, $pattern, '${1}require_tidb: true', 1)
}

# -Prod 机械关断 login 全部开发期后门(2026-07-27 审计发现:此前 -Prod 产物原样继承
# dev 模板的三个 true —— 生产**任意账号 + 任意密码都能登录并自动注册**)。
#   dev_skip_password  true = 跳过 bcrypt 校验,任何密码都放行
#   dev_auto_register  true = 账号不存在就自动建号(配合上一条 = 任意账号名即可开号)
#   dev_allow_any_role true = allowed_role_ids 为空时不再 fail-closed,任意 role_id 都收
# 三项都必须唯一锚点、机械置 false;违例拒绝生成。
# 注:tools/scripts/release_preflight.ps1 名义上查过 dev_skip_password,但它无任何调用方、
# 默认路径是失效的历史绝对路径、且 glob '*-prod.yaml' 在本仓匹配 0 个文件(实际是
# login-prod.yaml.example),更关键的是它扫的是 services/ 下的**模板**而不是发布产物。
# 因此把关必须做在生成器里。
function Set-ProdLoginDevSwitchesOff([string]$text) {
    foreach ($key in @('dev_skip_password', 'dev_auto_register', 'dev_allow_any_role')) {
        $pattern = '(?m)^([ \t]{2})' + $key + ':[ \t]*(?:true|false)[ \t]*(?:#.*)?$'
        $anchorCount = [regex]::Matches($text, $pattern).Count
        if ($anchorCount -ne 1) {
            throw "[FATAL] login 模板 $key 锚点异常(count=$anchorCount),拒绝生成 -Prod 产物。"
        }
        $text = [regex]::Replace($text, $pattern, ('${1}' + $key + ': false'))
    }
    return $text
}

# -Prod 全量机械关闭 gRPC reflection(审核 2026-07-22):dev 模板 enable_reflection: true
# 只供本地 grpcurl 联调;线上开 reflection 会把全部服务面/消息结构暴露给任何可达客户端,
# 便于探测攻击面。所有服务统一关,不许任何 -Prod 产物继承 dev 宽松档。
function Set-ProdReflectionOff([string]$svcName, [string]$text) {
    $pattern = '(?m)^([ \t]+)enable_reflection:[ \t]*(?:true|false)[ \t]*(?:#.*)?$'
    $anchorCount = [regex]::Matches($text, $pattern).Count
    if ($anchorCount -ne 1) {
        throw "[FATAL] $svcName 模板 enable_reflection 锚点异常(count=$anchorCount),拒绝生成 -Prod 产物。"
    }
    return [regex]::Replace($text, $pattern, '${1}enable_reflection: false', 1)
}

# allocator(ds-allocator / hub-allocator)专用改写:根据 -AllocatorMode 把 dev 的
# mode: "local"(宿主 exec Windows DS)改成集群里能跑的模式。
#   mock   → mode: "mock"(容器/集群内无 Windows PandoraServer.exe,返回确定性假地址)
#   agones → mode: "agones" + 把整个 agones: 段替换成 in-cluster 确定性模板(真 Linux DS)
function Rewrite-Allocator([string]$svcName, [string]$text) {
    if ($AllocatorMode -eq 'mock') {
        return ($text -replace '(?m)^(\s*mode:\s*)"local"', '$1"mock"')
    }

    # agones 模式:mode 改 agones
    $text = $text -replace '(?m)^(\s*mode:\s*)"local"', '$1"agones"'

    # 按服务选 fleet 与 timeout 键(ds=分配超时,hub=列表超时)
    if ($svcName -eq 'hub-allocator') {
        $fleet = 'pandora-hub-stable'
        $canaryFleet = 'pandora-hub-canary'
        $canaryPercent = $HubCanaryPercent
        $timeoutLine = '  list_timeout: "5s"'
    } else {
        $fleet = 'pandora-battle-stable'
        $canaryFleet = 'pandora-battle-canary'
        $canaryPercent = $BattleCanaryPercent
        $timeoutLine = '  allocate_timeout: "5s"'
    }

    # 组装 in-cluster agones 段(投影 token/ca 自动轮转,allocator 每次请求重读 token 文件)
    $lines = @(
        'agones:'
        '  enabled: true'
        '  api_server: "https://kubernetes.default.svc"'
        '  namespace: "default"'
        "  fleet_name: `"$fleet`""
        "  canary_fleet_name: `"$canaryFleet`""
        "  canary_percent: $canaryPercent"
        ('  canary_seed: "' + $CanarySeed + '"')
        '  token_path: "/var/run/secrets/kubernetes.io/serviceaccount/token"'
        '  ca_path: "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"'
        '  insecure_skip_tls_verify: false'
    )
    # advertise_host:本地 minikube(docker driver)用 127.0.0.1 + udp-relay 回程;线上留空用 status.address
    if (-not [string]::IsNullOrWhiteSpace($AllocatorAdvertiseHost)) {
        $lines += "  advertise_host: `"$AllocatorAdvertiseHost`""
    }
    $lines += $timeoutLine
    # ds-allocator 专属:Fleet 容量巡检(快到上限预警),hub-allocator 无此项
    if ($svcName -ne 'hub-allocator') {
        $lines += '  capacity_watch_interval: "30s"'
        $lines += '  capacity_warn_ratio: 0.8'
    }
    $agonesBlock = ($lines -join "`n") + "`n`n"

    # 把原 dev 的整个 agones: 段(直到下一个顶级注释块「# 本机拉起」)整块替换
    $text = [regex]::Replace($text, '(?ms)^agones:\r?\n.*?(?=^# 本机拉起)', $agonesBlock)
    return $text
}

function Assert-GeneratedSet {
    param(
        [string]$StageDir,
        [string[]]$ExpectedNames
    )

    $duplicates = @($ExpectedNames | Group-Object | Where-Object Count -gt 1)
    if ($duplicates.Count -ne 0) {
        throw "[FATAL] 生成器期望文件名重复:$($duplicates.Name -join ', ')"
    }

    $items = @(Get-ChildItem -LiteralPath $StageDir -Force)
    $directories = @($items | Where-Object { $_.PSIsContainer })
    if ($directories.Count -ne 0) {
        throw "[FATAL] staging 中出现非预期目录:$($directories.Name -join ', ')"
    }
    $actualNames = @($items | ForEach-Object Name | Sort-Object)
    $expectedSorted = @($ExpectedNames | Sort-Object)
    $setDiff = @(Compare-Object -ReferenceObject $expectedSorted -DifferenceObject $actualNames -CaseSensitive)
    if ($setDiff.Count -ne 0) {
        $missing = @($setDiff | Where-Object SideIndicator -eq '<=' | ForEach-Object InputObject)
        $extra = @($setDiff | Where-Object SideIndicator -eq '=>' | ForEach-Object InputObject)
        throw "[FATAL] staging 文件集不完整:缺少=[$($missing -join ', ')],多余=[$($extra -join ', ')]"
    }
    foreach ($name in $ExpectedNames) {
        $path = Join-Path $StageDir $name
        if ((Get-Item -LiteralPath $path).Length -le 0) { throw "[FATAL] 生成文件为空:$name" }
    }

    if ($ExpectedNames -contains 'envoy-jwks.json') {
        $jwksPath = Join-Path $StageDir 'envoy-jwks.json'
        try { $jwks = Get-Content -LiteralPath $jwksPath -Raw | ConvertFrom-Json -ErrorAction Stop }
        catch { throw "[FATAL] Envoy JWKS 不是合法 JSON:$($_.Exception.Message)" }
        $keys = @($jwks.keys)
        $expectedKs = @(ConvertTo-Base64Url $PlayerSecretToInject)
        if ($null -ne $PlayerAdditionalToInject) { $expectedKs += ConvertTo-Base64Url $PlayerAdditionalToInject }
        if ($keys.Count -ne $expectedKs.Count) { throw '[FATAL] Envoy JWKS key 数量与本次玩家面密钥集不一致。' }
        for ($i = 0; $i -lt $expectedKs.Count; $i++) {
            if ($keys[$i].kty -cne 'oct' -or $keys[$i].alg -cne 'HS256' -or $keys[$i].k -cne $expectedKs[$i]) {
                throw '[FATAL] Envoy JWKS 与本次玩家面密钥不一致。'
            }
        }
    }

    # 不只搜索 dev 文本：逐节点核对最终 jwt/ds_auth 值，防格式漂移、替换 0 次或跨段误写仍通过。
    $expectedPlayerValue = if ($null -ne $PlayerSecretToInject) { $PlayerSecretToInject } else { $DevPublicSecret }
    $expectedDsValue = if ($null -ne $DsSecretToInject) { $DsSecretToInject } else { $DevDsCallbackSecret }
    foreach ($svc in $Services) {
        $yaml = Get-Content -LiteralPath (Join-Path $StageDir "$($svc.Name).yaml") -Raw
        if ($PlayerSecretServiceNames -contains $svc.Name) {
            Assert-YamlSectionSecret $svc.Name $yaml 'jwt' $expectedPlayerValue
            if ($null -ne $PlayerAdditionalToInject) { Assert-YamlSectionAdditionalSecrets $svc.Name $yaml 'jwt' $PlayerAdditionalToInject }
            else { Assert-NoYamlSectionAdditionalSecrets $svc.Name $yaml 'jwt' }
        }
        if ($DsSecretServiceNames -contains $svc.Name) {
            Assert-YamlSectionSecret $svc.Name $yaml 'ds_auth' $expectedDsValue
            if ($null -ne $DsAdditionalToInject) { Assert-YamlSectionAdditionalSecrets $svc.Name $yaml 'ds_auth' $DsAdditionalToInject }
            else { Assert-NoYamlSectionAdditionalSecrets $svc.Name $yaml 'ds_auth' }
            if ($null -ne $DsAuthModeToInject) { Assert-YamlDsAuthMode $svc.Name $yaml $DsAuthModeToInject }
            if ($DsAuthorityModeToInject -eq 'redis') { Assert-YamlRedisAuthorityBundle $svc.Name $yaml }
        }
        if ($svc.Name -eq 'login' -and $null -ne $DsAuthorityModeToInject) {
            Assert-LoginHubAssignmentBinding $yaml ($DsAuthorityModeToInject -eq 'redis')
        }
        if ($DsTicketServiceNames -contains $svc.Name) {
            if ($AllocatorMode -eq 'agones') {
                Assert-YamlDsTicketSection $svc.Name $yaml
                Assert-NoYamlDsLocalProfile $svc.Name $yaml
            }
            else { Assert-NoYamlDsTicketSection $svc.Name $yaml }
        }
        foreach ($binding in @($PlacementSecretBindings | Where-Object Service -CEQ $svc.Name)) {
            Assert-YamlDirectString $svc.Name $yaml $binding.Section $binding.Child `
                ([string]$EffectivePlacementSecrets[$binding.Kind])
        }
        foreach ($binding in @($MatchResumeAuthSecretBindings | Where-Object Service -CEQ $svc.Name)) {
            Assert-YamlDirectString $svc.Name $yaml $binding.Section $binding.Child `
                $EffectiveMatchResumeAuthSecret
        }
        foreach ($binding in @($TeamResumeAuthSecretBindings | Where-Object Service -CEQ $svc.Name)) {
            Assert-YamlDirectString $svc.Name $yaml $binding.Section $binding.Child `
                $EffectiveTeamResumeAuthSecret
        }
        foreach ($binding in @($PlayerNoResolveAuthSecretBindings | Where-Object Service -CEQ $svc.Name)) {
            Assert-YamlDirectString $svc.Name $yaml $binding.Section $binding.Child `
                $EffectivePlayerNoResolveAuthSecret
        }
        foreach ($binding in @($PlayerNoResolveAuthAudienceBindings | Where-Object Service -CEQ $svc.Name)) {
            Assert-YamlDirectString $svc.Name $yaml $binding.Section $binding.Child $PlayerNoResolveAuthAudience
        }
        foreach ($binding in @($PlayerNoResolveAddressBindings | Where-Object Service -CEQ $svc.Name)) {
            Assert-YamlDirectString $svc.Name $yaml $binding.Section $binding.Child $binding.Value
        }
        foreach ($binding in @($PlayerNameResolveAuthSecretBindings | Where-Object Service -CEQ $svc.Name)) {
            Assert-YamlDirectString $svc.Name $yaml $binding.Section $binding.Child `
                $EffectivePlayerNameResolveAuthSecret
        }
        foreach ($binding in @($PlayerNameResolveAuthAudienceBindings | Where-Object Service -CEQ $svc.Name)) {
            Assert-YamlDirectString $svc.Name $yaml $binding.Section $binding.Child $PlayerNameResolveAuthAudience
        }
        foreach ($binding in @($PlayerNameResolveAddressBindings | Where-Object Service -CEQ $svc.Name)) {
            Assert-YamlDirectString $svc.Name $yaml $binding.Section $binding.Child $binding.Value
        }
        foreach ($group in $ApplicationDisplayAuthGroups) {
            foreach ($binding in @($group.SecretBindings | Where-Object Service -CEQ $svc.Name)) {
                Assert-YamlDirectString $svc.Name $yaml $binding.Section $binding.Child $group.Effective
            }
            foreach ($binding in @($group.AudienceBindings | Where-Object Service -CEQ $svc.Name)) {
                Assert-YamlDirectString $svc.Name $yaml $binding.Section $binding.Child $group.Audience
            }
            foreach ($binding in @($group.AddressBindings | Where-Object Service -CEQ $svc.Name)) {
                Assert-YamlDirectString $svc.Name $yaml $binding.Section $binding.Child $binding.Value
            }
        }
        foreach ($binding in @($AllocationAbortAuthSecretBindings | Where-Object Service -CEQ $svc.Name)) {
            Assert-YamlDirectString $svc.Name $yaml $binding.Section $binding.Child `
                $EffectiveAllocationAbortAuthSecret
        }
        if ($svc.Name -eq 'battle-result') {
            if ($DsAuthorityModeToInject -eq 'redis') {
                Assert-BattleResultConsumeTopics $yaml @('pandora.ds.lifecycle')
            } else {
                Assert-BattleResultConsumeTopics $yaml @('pandora.battle.result', 'pandora.ds.lifecycle')
            }
        }
        # -Prod 产物合约(R5 复审 P0-1):客户端面服务 unary 会话现行性门必须为强制档,
        # 任何模板漂移/替换 0 次都在发布前失败,不允许静默放行。
        if ($Prod -and ($UnarySessionGateServiceNames -contains $svc.Name)) {
            if (([regex]::Matches($yaml, '(?m)^session_gate:[ \t]*\r?\n[ \t]{2}require:[ \t]*true[ \t]*$')).Count -ne 1 -or
                [regex]::IsMatch($yaml, '(?m)^session_gate:[ \t]*\r?\n[ \t]{2}require:[ \t]*false')) {
                throw "[FATAL] -Prod 产物 $($svc.Name) session_gate.require 必须且只能为 true(旧 JWT 全服务吊销门,INC-20260722-004)。"
            }
        }
        # -Prod 产物合约(压测前审核门禁-A):客户端面服务 BBR 限流必须开启,
        # 任何模板漂移/插入 0 次都在发布前失败,不允许静默放行。
        if ($Prod -and ($GrpcRateLimitServiceNames -contains $svc.Name)) {
            if (([regex]::Matches($yaml, '(?m)^[ \t]+enable_rate_limit:[ \t]*true[ \t]*(?:#.*)?$')).Count -ne 1 -or
                [regex]::IsMatch($yaml, '(?m)^[ \t]+enable_rate_limit:[ \t]*false')) {
                throw "[FATAL] -Prod 产物 $($svc.Name) enable_rate_limit 必须且只能为 true(压测前审核门禁-A,BBR 过载丢负载)。"
            }
        }
        # -Prod 产物合约(审核 P0):实时成长通道必须被机械关断,
        # 任何模板漂移/替换 0 次都在发布前失败,不允许静默放行。
        if ($Prod -and $svc.Name -eq 'battle-result') {
            if (([regex]::Matches($yaml, '(?m)^[ \t]{2}progress_enabled:[ \t]*false[ \t]*$')).Count -ne 1 -or
                [regex]::IsMatch($yaml, '(?m)^[ \t]{2}progress_enabled:[ \t]*true')) {
                throw '[FATAL] -Prod 产物 battle-result progress_enabled 必须且只能为 false(实时通道启用是独立显式动作)。'
            }
        }
        if ($svc.Name -in @('matchmaker', 'matchmaker-pve', 'player', 'battle-result', 'inventory')) {
            # \r? 不能省:battle_result-dev.yaml 是 CRLF(其余模板 LF),生成器按源文件换行符原样
            # 回写。.NET 多行模式的 $ 只锚在 \n 前,行尾残留的 \r 会让 [ \t]*$ 匹配 0 次,
            # 于是"改写明明成功却断言失败"。
            if (([regex]::Matches($yaml, '(?m)^[ \t]{2}dir:[ \t]*"/app/configtable/active"[ \t]*\r?$')).Count -ne 1) {
                throw "[FATAL] $($svc.Name) 集群产物必须只读 /app/configtable/active。"
            }
        }
        if ($svc.Name -eq 'player') {
            if ([regex]::IsMatch($yaml, 'exp_curve:')) {
                throw '[FATAL] player 集群产物不得残留 exp_curve。'
            }
            if ($Prod -and ([regex]::Matches($yaml, '(?m)^[ \t]{2}experience_enabled:[ \t]*false[ \t]*$')).Count -ne 1) {
                throw '[FATAL] -Prod 产物 player experience_enabled 必须为 false(策划正式数值确认后独立启用)。'
            }
        }
    }
}

function Publish-GeneratedSet {
    param(
        [string]$StageDir,
        [string]$TargetDir,
        [string[]]$ExpectedNames,
        [string[]]$OwnedNames,
        [string]$BackupDir
    )

    $targetDirCreated = $false
    if (Test-Path -LiteralPath $TargetDir) {
        if (-not (Test-Path -LiteralPath $TargetDir -PathType Container)) {
            throw "[FATAL] 输出路径已存在但不是目录:$TargetDir"
        }
    } else {
        [System.IO.Directory]::CreateDirectory($TargetDir) | Out-Null
        $targetDirCreated = $true
    }

    foreach ($name in $OwnedNames) {
        $target = Join-Path $TargetDir $name
        if (Test-Path -LiteralPath $target) {
            $item = Get-Item -LiteralPath $target -Force
            if ($item.PSIsContainer -or (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0)) {
                throw "[FATAL] 生成器自有目标名不是普通文件:$target"
            }
        }
    }

    $originalNames = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    try {
        [System.IO.Directory]::CreateDirectory($BackupDir) | Out-Null
        foreach ($name in $OwnedNames) {
            $target = Join-Path $TargetDir $name
            if (Test-Path -LiteralPath $target -PathType Leaf) {
                Copy-Item -LiteralPath $target -Destination (Join-Path $BackupDir $name)
                $null = $originalNames.Add($name)
            }
        }
    } catch {
        $backupError = $_.Exception.Message
        if (Test-Path -LiteralPath $BackupDir -PathType Container) { Remove-Item -LiteralPath $BackupDir -Recurse -Force }
        if ($targetDirCreated -and @(Get-ChildItem -LiteralPath $TargetDir -Force).Count -eq 0) {
            [System.IO.Directory]::Delete($TargetDir)
        }
        throw "[FATAL] 发布前备份旧配置失败,最终目录尚未修改:$backupError"
    }

    $publishId = [guid]::NewGuid().ToString('N')
    $preparedTemps = [System.Collections.Generic.List[string]]::new()
    $rollbackFailed = $false
    $publishCommitted = $false
    try {
        # 先把本次完整集合复制到目标目录内的唯一临时文件；到这里失败时最终集合完全未动。
        foreach ($name in $ExpectedNames) {
            $temp = Join-Path $TargetDir ".$name.pandora-gen-$publishId.tmp"
            $preparedTemps.Add($temp)
            Copy-Item -LiteralPath (Join-Path $StageDir $name) -Destination $temp
        }

        foreach ($name in $ExpectedNames) {
            $temp = Join-Path $TargetDir ".$name.pandora-gen-$publishId.tmp"
            $target = Join-Path $TargetDir $name
            if (Test-Path -LiteralPath $target -PathType Leaf) {
                [System.IO.File]::Move($temp, $target, $true)
            } else {
                [System.IO.File]::Move($temp, $target)
            }
        }
        foreach ($name in @($OwnedNames | Where-Object { $ExpectedNames -notcontains $_ })) {
            $stale = Join-Path $TargetDir $name
            if (Test-Path -LiteralPath $stale -PathType Leaf) { Remove-Item -LiteralPath $stale -Force }
        }
        $publishCommitted = $true
    }
    catch {
        $publishError = $_.Exception.Message
        $rollbackErrors = [System.Collections.Generic.List[string]]::new()
        foreach ($name in $OwnedNames) {
            try {
                $target = Join-Path $TargetDir $name
                if ($originalNames.Contains($name)) {
                    $restoreTemp = Join-Path $TargetDir ".$name.pandora-rollback-$publishId.tmp"
                    $preparedTemps.Add($restoreTemp)
                    Copy-Item -LiteralPath (Join-Path $BackupDir $name) -Destination $restoreTemp
                    if (Test-Path -LiteralPath $target -PathType Leaf) {
                        [System.IO.File]::Move($restoreTemp, $target, $true)
                    } elseif (Test-Path -LiteralPath $target) {
                        throw '目标被外部改成了非文件。'
                    } else {
                        [System.IO.File]::Move($restoreTemp, $target)
                    }
                } elseif (Test-Path -LiteralPath $target -PathType Leaf) {
                    Remove-Item -LiteralPath $target -Force
                } elseif (Test-Path -LiteralPath $target) {
                    throw '目标被外部改成了非文件。'
                }
            } catch {
                $rollbackErrors.Add("$name=$($_.Exception.Message)")
            }
        }
        if ($rollbackErrors.Count -ne 0) {
            $rollbackFailed = $true
            throw "[FATAL] 发布失败:$publishError;回滚也失败:$($rollbackErrors -join '; ')。已保留含旧配置的备份:$BackupDir,需人工处理。"
        }
        if ($targetDirCreated -and @(Get-ChildItem -LiteralPath $TargetDir -Force).Count -eq 0) {
            [System.IO.Directory]::Delete($TargetDir)
        }
        throw "[FATAL] 发布失败:$publishError;最终目录已回滚到旧完整集合。"
    }
    finally {
        $cleanupErrors = [System.Collections.Generic.List[string]]::new()
        foreach ($temp in $preparedTemps) {
            if (Test-Path -LiteralPath $temp -PathType Leaf) {
                try { Remove-Item -LiteralPath $temp -Force }
                catch { $cleanupErrors.Add("临时文件 $temp=$($_.Exception.Message)") }
            }
        }
        if (-not $rollbackFailed -and (Test-Path -LiteralPath $BackupDir -PathType Container)) {
            try { Remove-Item -LiteralPath $BackupDir -Recurse -Force }
            catch { $cleanupErrors.Add("备份目录 $BackupDir=$($_.Exception.Message)") }
        }
        if ($cleanupErrors.Count -ne 0) {
            if ($publishCommitted) {
                throw "[FATAL] 新配置集合已成功发布到 $TargetDir,但含配置材料的临时项清理失败:$($cleanupErrors -join '; ')。最终目录可用,需人工清理上述路径。"
            }
            Write-Host "[WARN] 发布失败路径还有临时项未清理:$($cleanupErrors -join '; ')" -ForegroundColor Yellow
        }
    }
}

$yamlNames = @($Services | ForEach-Object { "$($_.Name).yaml" })
$ownedNames = @($yamlNames + 'envoy-jwks.json')
$expectedNames = @($yamlNames)
if ($null -ne $PlayerSecretToInject) { $expectedNames += 'envoy-jwks.json' }

# 缺任一源配置就拒绝生成；不能再用 WARN + continue 把半套目录交给 compose/k8s。
$missingSources = @(
    foreach ($s in $Services) {
        $src = Join-Path $ProjectRoot $s.Conf
        if (-not (Test-Path -LiteralPath $src -PathType Leaf)) { $s.Conf }
    }
)
if ($missingSources.Count -ne 0) {
    throw "[FATAL] 缺少必需 dev 配置,未发布任何产物:$($missingSources -join ', ')"
}

$outParent = Split-Path -Parent $OutDir
$outLeaf = Split-Path -Leaf $OutDir
if ([string]::IsNullOrWhiteSpace($outParent) -or [string]::IsNullOrWhiteSpace($outLeaf)) {
    throw "[FATAL] -OutDir 必须是普通目录路径,不能是文件系统根:$OutDir"
}
[System.IO.Directory]::CreateDirectory($outParent) | Out-Null
$pathHashBytes = [System.Security.Cryptography.SHA256]::HashData([System.Text.Encoding]::UTF8.GetBytes($OutDir.ToUpperInvariant()))
$pathHash = ([System.BitConverter]::ToString($pathHashBytes) -replace '-', '').Substring(0, 16)
$mutex = [System.Threading.Mutex]::new($false, "PandoraGenClusterConfig_$pathHash")
$lockTaken = $false
$fileLock = $null
$stageDir = Join-Path $outParent ".$outLeaf.pandora-stage-$([guid]::NewGuid().ToString('N'))"
$backupDir = Join-Path $outParent ".$outLeaf.pandora-backup-$([guid]::NewGuid().ToString('N'))"
$publicationSucceeded = $false
try {
    try { $lockTaken = $mutex.WaitOne(0) }
    catch [System.Threading.AbandonedMutexException] { $lockTaken = $true }
    if (-not $lockTaken) { throw "[FATAL] 已有另一个生成器正在发布到同一 OutDir:$OutDir" }

    # named mutex 只覆盖同名路径/当前会话；目标目录内的独占文件锁还能让 junction、8.3/大小写别名
    # 与不同登录 session 最终竞争同一个物理文件。锁文件永久保留为空文件，避免 Unix unlink 后新 inode 绕锁。
    if ((Test-Path -LiteralPath $OutDir) -and -not (Test-Path -LiteralPath $OutDir -PathType Container)) {
        throw "[FATAL] 输出路径已存在但不是目录:$OutDir"
    }
    [System.IO.Directory]::CreateDirectory($OutDir) | Out-Null
    $fileLockPath = Join-Path $OutDir '.pandora-gen.lock'
    try {
        $fileLock = [System.IO.File]::Open(
            $fileLockPath,
            [System.IO.FileMode]::OpenOrCreate,
            [System.IO.FileAccess]::ReadWrite,
            [System.IO.FileShare]::None)
    } catch {
        throw "[FATAL] 无法取得 OutDir 独占发布锁 $fileLockPath(可能有并发生成器):$($_.Exception.Message)"
    }

    [System.IO.Directory]::CreateDirectory($stageDir) | Out-Null
    foreach ($s in $Services) {
        $src = Join-Path $ProjectRoot $s.Conf
        $raw = Get-Content -LiteralPath $src -Raw
        $out = Convert-DevToCluster $raw
        # 凡 dev 模板里配了 config_table 的服务都必须在此列出:容器内没有 ../../../configtable/dist,
        # 漏掉一个 = 该服务带着宿主相对路径进集群 → 启动 fail-closed 退出 → CrashLoopBackOff。
        # (2026-08-05:ds-allocator 因 08-04 新增 config_table 时未同步登记,正是这样炸的。)
        if ($s.Name -in @('matchmaker', 'matchmaker-pve', 'player', 'battle-result', 'ds-allocator', 'inventory', 'mission', 'dialogue')) {
            $out = Set-ServiceClusterConfigTableDir $s.Name $out
        }
        if ($s.Name -in $script:MultiReplicaSnowflakeServices) {
            $out = Set-ClusterSnowflakeEtcd $s.Name $out
        }
        if ($s.Name -eq 'auction') { $out = Set-AuctionCrossInstanceLock $out }
        # 非 -Prod 也要强制:集群产物一律 RollingUpdate,滚动窗口就有并发发布器。
        if ($s.Name -in $script:PushWriterLeaseServiceNames) { $out = Set-ClusterPushWriterLeaseEnforce $s.Name $out }
        if ($Prod -and $s.Name -eq 'battle-result') { $out = Set-ProdBattleResultProgressOff $out }
        if ($Prod -and $s.Name -eq 'push') { $out = Set-ProdPushSessionGateOn $out }
        if ($Prod -and $s.Name -eq 'login') { $out = Set-ProdLoginHubHeadlessAddr $out }
        if ($Prod -and ($UnarySessionGateServiceNames -contains $s.Name)) {
            $out = Set-ProdUnarySessionGateOn $s.Name $out
        }
        if ($Prod -and ($GrpcRateLimitServiceNames -contains $s.Name)) {
            $out = Set-ProdGrpcRateLimitOn $s.Name $out
        }
        if ($Prod -and $s.Name -eq 'player') {
            $out = Set-ProdPlayerExperienceOff $out
            $out = Set-ProdPlayerRetentionOff $out
        }
        if ($Prod) { $out = Set-ProdReflectionOff $s.Name $out }
        if ($s.Name -eq 'owner' -and -not [string]::IsNullOrWhiteSpace($OwnerStoreDsn)) {
            # -Prod 时 OwnerStoreDsn 已强制非空(§9.22 校验块);非 -Prod 为显式本地覆盖。
            $out = Set-OwnerStoreDsn $out $OwnerStoreDsn
        }
        if ($Prod -and $s.Name -eq 'owner') { $out = Set-ProdOwnerRequireTiDB $out }
        if ($s.Name -eq 'login' -and -not [string]::IsNullOrWhiteSpace($AccountStoreDsn)) {
            # -Prod 时 AccountStoreDsn 已强制非空(全服单点校验块);非 -Prod 为显式本地覆盖。
            $out = Set-AccountStoreDsn $out $AccountStoreDsn
        }
        if ($Prod -and $s.Name -eq 'login') {
            $out = Set-ProdLoginRequireTiDB $out
            $out = Set-ProdLoginDevSwitchesOff $out
        }
        $out = Convert-Secret $s.Name $out
        if ($s.Name -eq 'battle-result' -and $DsAuthorityModeToInject -eq 'redis') {
            $out = Set-BattleResultRedisAuthorityIngress $out
        }
        if ($s.Name -in @('ds-allocator', 'hub-allocator')) { $out = Rewrite-Allocator $s.Name $out }
        # 机械闸:集群产物里绝不允许残留宿主相对路径。上面那份白名单是手工登记,漏登记时
        # 产物会带着 ../../../configtable/dist 进 Pod,而容器里根本没有这个目录 ——
        # 服务 fail-closed 退出、CrashLoopBackOff,但**生成这一步照样报成功**,故障要到
        # rollout 超时才暴露(2026-08-05 ds-allocator 实例)。这里在写盘前就把它变成生成期错误。
        if ($out -match '\.\./\.\./\.\./configtable/dist') {
            throw "[FATAL] $($s.Name).yaml 集群产物仍含宿主相对路径 ../../../configtable/dist。" +
                  "该服务的 dev 模板配了 config_table,但未登记进 Set-ServiceClusterConfigTableDir 的服务白名单;" +
                  "请把 '$($s.Name)' 加进该白名单,并确认 deploy/k8s/services/services.yaml 里它的 Deployment 已挂载 pandora-configtable。"
        }
        # 机械闸二:agones 产物里「非空 ds_local_profile」与「DSTicket v2 私钥」不得共存。
        # 上面的 Clear/Assert 只覆盖 $DsTicketServiceNames 名单内的服务;这条兜住名单外的服务
        # 将来也长出 ds_local_profile 的情况。同样在写盘前变成生成期错误,而不是 Pod CrashLoop。
        if ($AllocatorMode -eq 'agones' -and
            ($out -match '(?m)^[ \t]*ds_local_profile:[ \t]*"[^"]+"') -and
            $out.Contains('private_key_file: "/run/secrets/pandora-dsticket/private.pem"')) {
            throw "[FATAL] $($s.Name).yaml 同时带非空 match.ds_local_profile 与 ds_ticket.private_key_file,两者互斥,服务启动会 ds_ticket_profile_conflict 退出。" +
                  "若该服务确需签票,请把它登记进 `$DsTicketServiceNames(生成器会自动清空本地档);否则应从 dev 模板移除 ds_local_profile。"
        }
        $dst = Join-Path $stageDir "$($s.Name).yaml"
        [System.IO.File]::WriteAllText($dst, $out, (New-Object System.Text.UTF8Encoding($false)))
    }
    Sync-EnvoyJwks -TargetDir $stageDir
    Assert-ProdDbCredentials -StageDir $stageDir
    Assert-GeneratedSet -StageDir $stageDir -ExpectedNames $expectedNames
    Publish-GeneratedSet -StageDir $stageDir -TargetDir $OutDir -ExpectedNames $expectedNames -OwnedNames $ownedNames -BackupDir $backupDir
    $publicationSucceeded = $true
}
finally {
    $stageCleanupError = $null
    if (Test-Path -LiteralPath $stageDir -PathType Container) {
        try { Remove-Item -LiteralPath $stageDir -Recurse -Force }
        catch { $stageCleanupError = $_.Exception.Message }
    }
    if ($null -ne $fileLock) { $fileLock.Dispose() }
    if ($lockTaken) { $mutex.ReleaseMutex() }
    $mutex.Dispose()
    if ($null -ne $stageCleanupError) {
        if ($publicationSucceeded) {
            throw "[FATAL] 新配置集合已成功发布到 $OutDir,但 staging 清理失败:$stageDir;$stageCleanupError。最终目录可用,需人工清理 staging。"
        }
        throw "[FATAL] 生成失败且 staging 清理也失败:$stageDir;$stageCleanupError"
    }
}

Write-Host "[ OK ] 生成并事务发布 $($yamlNames.Count) 个集群版配置(allocator=$AllocatorMode, host_allocators=$HostAllocators, player_secret=$(if ($null -ne $PlayerSecretToInject) { '真密钥' } else { 'dev' }), ds_secret=$(if ($null -ne $DsSecretToInject) { '真密钥' } else { 'dev' }), match_resume_auth=$(if ($EffectiveMatchResumeAuthSecret -ceq $DevMatchResumeAuthSecret) { 'dev' } else { '真密钥' }), team_resume_auth=$(if ($EffectiveTeamResumeAuthSecret -ceq $DevTeamResumeAuthSecret) { 'dev' } else { '真密钥' }), player_no_resolve_auth=$(if ($EffectivePlayerNoResolveAuthSecret -ceq $DevPlayerNoResolveAuthSecret) { 'dev' } else { '真密钥' }), player_name_resolve_auth=$(if ($EffectivePlayerNameResolveAuthSecret -ceq $DevPlayerNameResolveAuthSecret) { 'dev' } else { '真密钥' }), allocation_abort_auth=$(if ($EffectiveAllocationAbortAuthSecret -ceq $DevAllocationAbortAuthSecret) { 'dev' } else { '真密钥' }), owner_store=$(if (-not [string]::IsNullOrWhiteSpace($OwnerStoreDsn)) { '注入DSN' } else { 'dev-mysql' })) -> $OutDir" -ForegroundColor Green
