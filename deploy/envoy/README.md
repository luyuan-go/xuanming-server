# Pandora Envoy 边缘网关(W2 ④,2026-06-05)

> 本目录:Envoy `v1.38-latest` 本地开发期配置 + 证书占位（当前验证镜像为 1.38.1）。
> 上层设计:[`docs/design/gateway-decision.md`](../../docs/design/gateway-decision.md) §5。

## 目录文件

| 文件 | 入库 | 说明 |
|---|---|---|
| `envoy.yaml` | ✅ | Envoy 配置(listener + filters + routes + clusters) |
| `.gitignore` | ✅ | 屏蔽 `*.pem` `*.key` `*.crt` 入库 |
| `README.md` | ✅ | 本文 |
| `cert.pem` | ❌ | mkcert 本机生成 |
| `key.pem`  | ❌ | mkcert 本机生成 |

## 端口

| 端口 | 用途 | 暴露 |
|---|---|---|
| **8443** | 客户端入口(HTTPS / gRPC-Web over HTTP/2 TLS) | 默认 `127.0.0.1`;局域网模式可显式开 `0.0.0.0` |
| **8444** | DS 面入口(UE Hub/Battle DS → 内部服务,gRPC-Web,agones-dev.md §5.1) | 默认 `127.0.0.1`;仅 `-ExposeDsFace` 显式开放 |
| **9901** | Envoy admin(`/ready` `/clusters` `/stats` `/config_dump`) | 恒定 `127.0.0.1`,绝不对外 |

## 上游 cluster(W2 ④)

客户端面(`:8443` `pandora_listener`,带 jwt_authn):

| cluster | 后端业务服 | 端口 | 协议 | timeout |
|---|---|---|---|---|
| `login_cluster`  | login | host.docker.internal:20001 | h2c | route 5s |
| `push_cluster`   | push  | host.docker.internal:20014 | h2c | route 0s(server stream) |
| `team_cluster`   | team  | host.docker.internal:20010 | h2c | route 15s |
| `match_cluster`  | matchmaker | host.docker.internal:20011 | h2c | route 15s |
| `friend_cluster` | friend | host.docker.internal:20004 | h2c | route 15s |
| `chat_cluster`   | chat | host.docker.internal:20005 | h2c | route 15s |
| `trade_cluster`  | trade | host.docker.internal:20012 | h2c | route 15s |
| `auction_cluster` | auction | host.docker.internal:20016 | h2c | route 15s |
| `leaderboard_cluster` | leaderboard | host.docker.internal:20007 | h2c | route 15s |
| `dialogue_cluster` | dialogue | host.docker.internal:20013 | h2c | route 15s | ⚠️ **本行是漂移**:`envoy.yaml` 里并不存在该 cluster / route,dialogue 当前未接入客户端面网关(2026-08-11 核实)。要放行需照 friend 补 route + jwt_authn 规则 + cluster 三件套。 |
| `mission_cluster` | mission | host.docker.internal:20019 | h2c | route 15s |

**mission 的系统 RPC 不对客户端开放**:`ReportMissionFacts`(任务进度唯一写入通道,由
battle_result 出箱内网直连)与 `CompleteAllMissions`(GM 批量完成)在 route 里按精确 path
`direct_response 403`,且排在 `prefix` 路由之前;服务层另有 `systemOnly`(callerID!=0 拒)兜底。

DS 面(`:8444` `pandora_ds_listener`,**不挂玩家面 jwt_authn**):DSTicket 只认证玩家→DS；
DS→后端由 exact method 白名单、服务层 DS Bearer Guard 与 Redis active/projection 共同授权。
NetworkPolicy 只是可达性收敛，生产仍须补 mTLS/ACL 信任根（见 agones-dev.md §5）。

> 当前 Fleet 尚不能安全完成玩家 DSTicket 的本地验签：不得把玩家 HS256 签名 secret 直接发给
> 不可信 DS。生产须先拍板 DSTicket 公钥 JWKS 或只走 online login authority；UE 对空/短/dev 占位
> secret 会 fail-closed。该项与 DS callback credential 是两套不同密钥边界，不能混用。
>
> `:8444` 当前本地/集群清单为明文 gRPC-Web。Bearer/玩家票/GM 命令因此没有传输机密性与服务端身份；
> NetworkPolicy 与应用 ACK 不能替代 mTLS。生产必须先完成 mesh STRICT mTLS 或等价 Envoy 双向 TLS、
> 身份/SAN 绑定和 revisioned CA 轮换。

| cluster | 内部服务 | 端口 | 协议 | timeout | DS 用途 |
|---|---|---|---|---|---|
| `hub_allocator_cluster`  | hub_allocator | host.docker.internal:20021 | h2c | route 15s | Hub DS 心跳 |
| `ds_allocator_cluster`   | ds_allocator  | host.docker.internal:20020 | h2c | route 15s | Battle DS 心跳 |
| `locator_cluster`        | player_locator | host.docker.internal:20006 | h2c | route 15s | Hub DS SetLocation(HUB) |
| `battle_result_cluster`  | battle_result | host.docker.internal:20022 | h2c | route 15s | Battle DS 同步结算上报 |
| `login_cluster` | login | host.docker.internal:20001 | h2c | route 15s | DS 在线 `VerifyDSTicket`；`:8443` 对同 path 精确 403 |

后续客户端业务服上线时可复制 cluster 块并按鉴权契约增加 route；DS 面新增方法必须逐个增加
**精确 `path`**，禁止用 service `prefix` 扩大未鉴权攻击面。

> **客户端鉴权 RPC 不是只加 route 就完成**:`jwt_authn.rules` 未命中的 path 默认放行且不验签,
> 因而也不会把 JWT `sub` 注入 `x-pandora-player-id`。凡上游从该头取玩家身份的新 RPC,必须把
> exact path 同步加入 `jwt_authn.rules`;漏配时应明确返回未授权,不得把缺身份伪装成业务默认态。

---

## 1. 证书生成(由 **ChatGPT / Codex** 执行,Claude 不动)

> 前置:已 `mkcert -install`(机器已加入本地 root CA)。

```powershell
cd e:\work\Pandora\deploy\envoy
mkcert -cert-file cert.pem -key-file key.pem `
  localhost 127.0.0.1 host.docker.internal ::1
```

### 验收

```powershell
Test-Path cert.pem        # True
Test-Path key.pem         # True

# 证书 SAN 必须含 localhost + 127.0.0.1(grpcurl :8443 用 localhost 校验)
mkcert -CAROOT             # 显示本机 CA 目录(信息)
```

⚠️ **不要** `mkcert localhost` 默认输出(会落到 `~/<cwd>` 而且文件名带域名),**必须**显式 `-cert-file -key-file`。

---

## 2. 启 Envoy(由 **ChatGPT / Codex** 执行)

```powershell
# 已合并进 deploy/docker-compose.dev.yml,跟基础设施一起起
cd e:\work\Pandora
pwsh tools/scripts/dev_up.ps1

# 单独操作 envoy:
docker compose -f deploy/docker-compose.dev.yml --env-file deploy/env/dev.env up -d envoy
docker compose -f deploy/docker-compose.dev.yml --env-file deploy/env/dev.env logs -f envoy
docker compose -f deploy/docker-compose.dev.yml --env-file deploy/env/dev.env restart envoy
docker compose -f deploy/docker-compose.dev.yml --env-file deploy/env/dev.env stop envoy
```

### Phase B 验收(Codex 启完 envoy 后,Claude 跑下面命令复查)

```powershell
# 1. envoy 启动日志(无 cert 找不到 / config error)
docker logs pandora-envoy --tail 50
# 期望:"starting main dispatch loop"

# 2. admin 健康
(Invoke-WebRequest http://127.0.0.1:9901/ready -UseBasicParsing).StatusCode
# 期望:200(body=LIVE)

# 3. cluster 健康(host_statuses 至少 1 个)
(Invoke-WebRequest http://127.0.0.1:9901/clusters?format=json -UseBasicParsing).Content `
  | ConvertFrom-Json `
  | ForEach-Object cluster_statuses `
  | Select-Object name, @{n='hosts'; e={$_.host_statuses.Count}}
# 期望:login_cluster / push_cluster 各 hosts >= 1
```

---

## 3. 端到端联调(Phase C,Claude 跑)

> 前置:envoy 已启 + login / push 业务服已 `go run` 起来(两个终端各起一个)。

### 3.1 直连 login(基线 — 确认服务本身 OK)

```powershell
grpcurl -plaintext -d '{\"account\":\"test\",\"password_hash\":\"abc\",\"device_id\":\"d1\"}' `
  127.0.0.1:20001 pandora.login.v1.LoginService/Login
```

期望:`{"code":"OK","playerId":"...","sessionToken":"<uuid>","hubDsAddr":"127.0.0.1:7777", ...}`

### 3.2 经 Envoy 测 login(W2 ⑥ 第一项)

```powershell
grpcurl -insecure -d '{\"account\":\"test\",\"password_hash\":\"abc\",\"device_id\":\"d1\"}' `
  127.0.0.1:8443 pandora.login.v1.LoginService/Login
```

期望:同 3.1。grpcurl `-insecure` 跳证书校验(mkcert root 已 install 也可 `-cacert "$(mkcert -CAROOT)\rootCA.pem"`)。

### 3.3 经 Envoy 测 push server stream(W2 ⑥ 第二项)

```powershell
grpcurl -insecure -max-time 12 -d '{\"session_token\":\"mock\",\"last_seen_ms\":0}' `
  127.0.0.1:8443 pandora.push.v1.PushService/Subscribe
```

期望:
- 立刻收到第一帧 `PushFrame { topic: "pandora.system.notify", payload: "aGVsbG8=" (base64 hello), tsMs, traceId }`
- 之后每 5s 一帧,12s 内累计 2~3 帧
- 12s 后 grpcurl 因 `-max-time` 退出(**不是错误**,验证流持续推送有效)

### 3.4 reflection 验证(可选)

```powershell
grpcurl -insecure 127.0.0.1:8443 list
# 期望(节选):
#   grpc.reflection.v1.ServerReflection
#   pandora.login.v1.LoginService
# (注意:list 只反映 envoy 路由命中的 cluster 上 reflection 注册的 services,
#  reflection 路由打到 login_cluster,所以会列出 login 服务 + reflection 本身;
#  push 的 service list 要直接连 :20014 或单独打 reflection 路由)

grpcurl -plaintext 127.0.0.1:20001 describe pandora.login.v1.LoginService
grpcurl -plaintext 127.0.0.1:20014 describe pandora.push.v1.PushService
```

---

## 4. 故障排查速查

| 现象 | 根因 | 修复 |
|---|---|---|
| `connection refused :8443` | envoy 没起 / 配置错 | `docker logs pandora-envoy --tail 100` |
| `no healthy upstream` | 业务服没 `go run` 起 | 在另一终端起 login / push |
| envoy 反回 415 | 上游 cluster 漏 `http2_protocol_options` | 已在 envoy.yaml 显式配,别删 |
| push stream 15s 后断 | route 漏 `timeout: 0s` | 已配,别改 |
| `x509: certificate signed by unknown authority` | mkcert root 没 install,或 grpcurl 没 -insecure | `mkcert -install`(由 Codex)或加 `-insecure` |
| 证书 SAN 不含 localhost | 生成命令漏 SAN | 重生:`mkcert -cert-file ... localhost 127.0.0.1 host.docker.internal ::1` |
| 配置改完不生效 | envoy 需重启或 hot reload | `docker compose ... restart envoy` |

---

## 5. 后续待办(本配置遗留)

- [ ] gRPC reflection 路由改 `direct_response: { status: 403 }`(生产闸门)
- [ ] mTLS 上行(`UpstreamTlsContext` + 业务服 server-side TLS)
- [ ] 加 `envoy.filters.http.ratelimit`(对接独立 ratelimit service)
- [ ] CORS `allow_origin_string_match` 收紧到具体域名(去掉 `.*`)
- [ ] 接 OpenTelemetry tracing collector(对齐 docs/design/infra.md)
- [ ] 当前静态配置已有 19 个 cluster；后续继续增长或进入多环境动态路由时，评估 Envoy CDS / xDS 下发
