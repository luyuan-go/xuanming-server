# Pandora 发布前清单（Release Checklist）

> 用途:把发布前**必须从 dev 切到生产**的所有开关、证书、配置列成一张可勾选的清单,
> 防止"开发环境好好的,打包发给玩家就连不上 / 不安全"。
> 配套一键预检脚本:[`tools/scripts/release_preflight.ps1`](../../tools/scripts/release_preflight.ps1)。

## 0. 为什么需要这份清单

很多 dev 开关的**默认值就是 dev 态**(尤其 UE 客户端),直接打包会把 dev hack 发给玩家。典型后果:

- `GatewayHost = 127.0.0.1`(UE 头文件默认)→ 打包后连本机,**玩家根本连不上**。
- `bAutoLoginForDev = true`(UE 头文件默认)→ 自动用 dev 账号登录,必须在生产 ini 显式关闭。
- 后端 `dev_skip_password: true` → **任意账号名可登录任意 player_id**。
- mkcert 自签证书 → 玩家设备不信任,握手失败(就是本机 UE 当前遇到的问题)。

**铁律:发布前必须跑预检脚本,FAIL 一项都不许打包。**

---

## 1. 一键预检（先跑这个）

```powershell
pwsh tools/scripts/release_preflight.ps1 `
  -UeGameIni <UE工程路径>\Config\DefaultGame.ini `
  -BackendConfigDir E:\work\Pandora\services `
  -ConfigGlob '*-prod.yaml' `
  -EnvoyCert <生产证书 cert.pem 路径> `
  -ExpectedGatewayHost gateway.yourgame.com
```

- 全 PASS → 退出码 0,可进入打包/部署。
- 任一 FAIL → 退出码 1,按下面分项修复。**建议把这条命令接进 CI / 打包脚本作为前置门禁。**

---

## 2. 分项清单（脚本拦不全的人工项也在这）

### 2.1 UE 客户端（隐患最大）

UE `PandoraBackendSubsystem` 的 dev 开关默认值在 C++ 头文件里,生产**必须**在打包用的 ini
(`Config/DefaultGame.ini` 或 Shipping/Platform 专用 ini)的 `[/Script/Pandora.PandoraBackendSubsystem]` 段**显式覆盖**:

- [ ] `GatewayHost=` **真实域名**(如 `gateway.yourgame.com`),不是 `127.0.0.1`
- [ ] `GatewayPort=443`(或你的生产网关端口)
- [ ] `bDevInsecureTls=False`  ← **关键安全项**,强制校验 TLS 证书
- [ ] `bAutoLoginForDev=False`  ← 关掉自动 test 账号登录
- [ ] `DevLoginAccount=` / `DevLoginPasswordHash=` 清空(不带 dev 口令进包)
- [ ] 登录密码在客户端**先 sha256** 再填 `password_hash`(对齐 [login.proto](../../proto/pandora/login/v1/login.proto) 契约;当前实现是直接透传,**发布前必须补 sha256**)
- [ ] 打 **Shipping** 配置(非 Development/DebugGame),`UE_BUILD_SHIPPING` 关掉调试日志/作弊指令

> 生产 ini 推荐写法(显式覆盖头文件默认):
> ```ini
> [/Script/Pandora.PandoraBackendSubsystem]
> GatewayHost=gateway.yourgame.com
> GatewayPort=443
> bDevInsecureTls=False
> bAutoLoginForDev=False
> DevLoginAccount=
> DevLoginPasswordHash=
> ```

### 2.2 后端服务

> 9 个现役服务已备好生产模板 `services/**/etc/<svc>-prod.yaml.example`(占位符版,入库安全)。
> 部署时 `cp <svc>-prod.yaml.example <svc>-prod.yaml`,把所有 `__占位符__` 换成真实值,
> 用 `-conf etc/<svc>-prod.yaml` 启动。⚠️ 真实 `*-prod.yaml` 已被 `services/.gitignore` 忽略,
> **绝不入库**(Kratos file source 无 env 替换,secret 直接写在 yaml,故真值文件不能进 git)。

- [ ] 每个服务用独立的 **`*-prod.yaml`**(从 `.example` 复制),不要直接拿 `*-dev.yaml` 上线
- [ ] 所有 `__占位符__` 已替换:`__REDIS_HOST__` / `__REDIS_PASSWORD__` / `__MYSQL_HOST__` /
      `__MYSQL_STRONG_PASSWORD__` / `__KAFKA_BROKER_*__` / `__JWT_SHARED_SECRET_32B_CHANGE_ME__`
- [ ] `login.dev_skip_password: false`(或删除该键)← 关掉免密登录
- [ ] 所有服务 `server.grpc.enable_reflection`: 不写 / false ← 关 reflection,防 schema 泄露
- [ ] DSN / Redis / Kafka 地址改为生产实例,**强密码**(不是 `pandora_dev_pwd`)
- [ ] JWT secret 在 login / matchmaker / hub_allocator / Envoy jwt_authn **四处完全一致**,≥32 字节强随机
- [ ] ds_allocator / hub_allocator `agones.enabled: true`(接真 Agones,不再用 Mock)
- [ ] `insecure_skip_tls_verify`(ds_allocator / hub_allocator 的 Agones 段)保持 false
- [ ] `configtable/dist/manifest.json` 的 `source_rev` 可追溯、逐表 checksum 通过;player Pod 已挂
      `pandora-configtable` 到 `/app/configtable/active`,启动日志的版本/等级数与 manifest 一致
- [ ] `player_level_exp` 正式数值已由策划确认并完成全 fleet 收敛门禁后才打开
      `player.experience_enabled`;当前占位曲线不得在生产启用
- [ ] 密码 / token / secret 不写进入库 yaml(真值文件已被 gitignore,只提交 `.example` 模板)
- [ ] Linux DS 符号文件已随版本归档,crash dump / minidump / UE crash report 能自动上传
- [ ] DS metrics 已接 Prometheus / Grafana,至少能看在线、tick p95/p99、CPU、内存、网络、心跳、崩溃次数
- [ ] 本版本关联的崩溃/P0 已登记 `docs/incidents/index.md`;未关闭 Incident 已明确阻断发布或有审批记录,已关闭项具备修复 commit、镜像 digest、race/故障路径和观察窗口证据

> Linux DS 崩溃和性能排障手册见
> [`docs/ops/linux-ds-observability.md`](linux-ds-observability.md)。

### 2.3 GameServer 公网 UDP 可达性（上线硬门）

> `-Mode online` 不使用本地 `udp-relay`。Battle / Hub allocator 默认把 Agones
> `status.address:status.ports[0].port` 原样返回给玩家；K8s Pod Ready、Agones GameServer Ready 和
> DS→allocator 业务心跳都只证明**集群内部**正常，不能证明玩家网络能打到该 UDP 地址。
>
> 当前 `start.ps1 -Mode online` 尚未回读云网络规则，也没有能力从集群外发 UDP；本节是由外部平台 /
> 运维提供证据的人工发布硬门，**不是发布脚本已自动执行的检查**。当前 online 的独立零写阻断仍须保留。

- [ ] online 生成的 DS/Hub allocator 配置均**没有** `agones.advertise_host`；若确需统一地址覆盖，已有独立架构审批和逐 GameServer 动态端口路由证明，不能仅凭“域名能解析”放行
- [ ] Stable / Canary 每个可接新玩家的 GameServer，其 `status.address` 都是玩家侧可路由地址，不是 Pod IP、Service ClusterIP、仅 VPC 可见的节点内网 IP
- [ ] `status.ports[0].port` 落在平台批准的 UDP 端口池；当前 Fleet 使用 `portPolicy: Dynamic`，不得把文档里的 Hub/Battle 目标分段误当成已生效配置
- [ ] 云安全组、节点防火墙及公网 NAT / LB 将该 UDP 端口精确路由到 GameServer 所在节点的 Agones hostPort；跨节点或端口重映射不会把流量送错实例
- [ ] 从**集群外、与真实玩家同网络路径**对当前每个可接新玩家的 GameServer 做真实 UDP 握手；至少覆盖每个 distinct 公网 address、Node / 节点池、放量 track 和实际动态 port，不能用 Hub/Battle 各抽一个替代
- [ ] 每个探针结果都已记录 address、port、GameServer name/UID、Node、track、时间与结果（不得记录 DSTicket 或密钥），并在 exact GameServer 观察到对应握手 / `PreLogin`，不是只看到 NAT/LB 收包
- [ ] 自动扩容产生的新 Node / 节点池或 GameServer 在接收玩家前重复等价外部验证；若平台暂不能把该验证接入准入门，则保持该容量不可分配
- [ ] 上述任一项为 UNKNOWN、超时或证据无法与 exact GameServer UID 对账时，发布保持阻断，不得用 Fleet Ready、心跳正常或集群内探针替代

> 具体 online 拓扑与操作口径见
> [`deploy/k8s/agones/README.md`](../../deploy/k8s/agones/README.md) 的“玩家 → GameServer 公网 UDP 硬门”。

### 2.4 边缘 TLS 证书（Envoy）

- [ ] 证书由**公网 CA** 签发(Let's Encrypt 免费 / 商业 CA),**不是 mkcert 自签**
- [ ] 证书 **SAN = 真实域名**,不含 IP(公网 CA 不给 IP 签)
- [ ] 证书自动续期已配置(Let's Encrypt 90 天,certbot/acme 定时续)
- [ ] Envoy listener 绑生产证书 + 真实域名,JWT secret 用生产值(不是 dev 共享 secret)

> 完整证书策略(dev vs 生产、为什么要域名、成本)见
> [`docs/design/gateway-decision.md`](../design/gateway-decision.md) §14。

### 2.5 数据 / 账号

- [ ] 清掉 dev 种子账号(`test` / `test1`~`test10000`,密码 `abc`)
- [ ] 生产 DB 不带任何 dev 测试数据
- [ ] 备份 / 回滚预案就绪；版本化迁移仅包含经审核的向后兼容 expand 变更
- [ ] 相对上一个已发布 tag，所有已发布 migration SQL 均未改写/删除/重编号；修正只新增更高版本（`schema_migrations` 无 SQL checksum，违反会产生伪 clean）
- [ ] 独立预存在的 `pandora-db-migrate` Secret 已由 DBA/Secret 管理器准备，使用逐库最小权限迁移账号（`SELECT/INSERT/UPDATE/DELETE/CREATE/ALTER/INDEX/REFERENCES`，不授 `TRIGGER/SUPER`），不含 root，也不复用业务 `pandora-config`
- [ ] Job 的独立 `-expected-targets` inventory 已从真实分片配置生成并人工复核，按 `name:migration_set:database` triple 精确列全普通库和全部 auction shards
- [ ] 按 [`tools/migrate`](../../tools/migrate/README.md) 先运行一次性 migration Job；生产 DSN 均为可验证的 `tls=true`，所有 expected targets 均验收 `version=N dirty=false`
- [ ] migration Job 失败/超时/dirty/TLS 不安全/inventory 不一致时发布已阻断，未自动 force、未继续滚动业务 Deployment

### 2.6 Auction 状态机升级（本次必须人工门禁）

- [ ] 生产 auction 为单库或恰好 2 个 MySQL shard；`N>2` 已由服务 fail-fast，未绕过。扩容前另做 owner registry 全量回填、冲突审计和持久完成标记
- [ ] `auction_shard_topology` 已在每片 exact-match generation/count/index/identity；禁止 data-bearing 集群改 `1↔2`、重排/重复 DSN。双分片首次登记只临时打开 bootstrap，成功后已恢复 false
- [ ] 所有 old auction 已启用 etcd Snowflake node ID 与同一 Redis 跨实例 market 锁；inventory 的 `EnsureAuctionEscrow` 已先行发布
- [ ] 每个 auction shard 的 expand migration 均完成；真实规模索引建造时间、MDL/复制延迟和 Claim p99 已验收
- [ ] 已统计历史 COMPLETED match 数量并验证 Kafka/消费者容量：迁移会把历史成交纳入有界 outbox 重放，消费者必须按 `match_id` 幂等，发布期间持续监控 backlog 与消费延迟
- [ ] R3 green 明确为 `passive_warmup=true`，不接写且 verifier/reconciler/expiry 均未运行；入口先暂停 `PlaceOrder/Bid`，old 在途写归零并等待至少一个旧 lock TTL 后才下线 old
- [ ] old 全部退出后，green 已以 `passive_warmup=false` 重启并确认补偿器 ready，随后才一次切流并恢复写入口
- [ ] 切流后 active unverified、PENDING/match/event/release 补偿队列持续下降并最终收敛；Kafka 消费者按 `match_id` 幂等
- [ ] 若不接受短暂写冻结，已先滚兼容 owner coordinator 版本；禁止 old/green 同时接拍卖写流量

### 2.7 战斗掉落出箱幂等键分叉（本次必须人工门禁，一次性）

**为什么有这一条**:2026-08-24 修了 `battle_result` 的 `deliverDropRecord`——它的**分叉条件**
(`len(stacks)>0 && len(instances)>0`)与**发放条件**(`len(stacks)>0 || currency>0`)长期是两份各自
演化的条件。于是「爆装备 + 有金币、但没爆可堆叠道具」这一形态不分叉:`GrantItems` 先用
`battle_drop:{match_id}:{player_id}` 把金币发了,`GrantInstances` 紧接着拿**同一把键**必被 inventory
判 7015 幂等冲突 → 整行返错 → 出箱行永不删 → 装备永远发不到玩家手上。

修复把这一形态的 **stack 侧幂等键改成了 `battle_drop:{match_id}:{player_id}:stack`**(装备侧为
`…:instance`)。**其余形态的键逐字节没变。** 但库里**已经卡住**的这类存量行在新代码上线后重试时,
会拿一把 inventory 从没见过的新键**再发一次金币** —— inventory 那边没有这把键的流水,幂等去重不生效。
一个「卡死不发」的缺陷会被换成「多发钱」。所以必须在部署前把存量清干净。

> ⚠️ 与「单行装备件数」无关:上一轮那版按件数拆批的改动**已整块回退**(拆批打破「转邮件即终态」
> 不变量,实测造成装备双发)。本条**只**针对幂等键分叉这一种形态,不要顺手按件数捞行。

顺序不能反:

- [ ] **① 先停 drop publisher**(或先不部署 `battle_result` 本次改动),再执行下面的排查 SQL。publisher
  在跑的时候捞出来的集合会边捞边变,拿到的清单不可用于对账。
- [ ] **② 执行排查 SQL,捞出存量卡住行**:

  ```sql
  -- 存量受影响行:有金币 + 有装备 + 无堆叠道具(= 旧代码下两路共用 baseKey 的唯一形态)
  SELECT id, match_id, player_id, currency_amount,
         instance_item_config_ids, created_at_ms,
         CONCAT('battle_drop:', match_id, ':', player_id) AS legacy_idempotency_key
  FROM `pandora_battle`.`battle_drop_outbox`
  WHERE currency_amount > 0
    AND instance_item_config_ids <> ''
    AND stack_item_config_ids = ''
  ORDER BY id;
  ```

- [ ] **③ 捞到 0 行 = 无迁移窗口**,直接部署,④⑤ 跳过。**捞出来确实是空集才可以跳过**——不要因为
  「应该没有」而免掉这次实测。
- [ ] **④ 捞到行**:逐行拿上面那列 `legacy_idempotency_key` 到 inventory 库核对金币**是否已入账**:

  ```sql
  -- 把 :key_list 换成 ② 查出来的 legacy_idempotency_key(op='grant' 即 GrantItems 那一笔)
  SELECT player_id, idempotency_key, op, created_at
  FROM `pandora_trade`.`inventory_ledger`
  WHERE op = 'grant'
    AND idempotency_key IN (:key_list);
  ```

  - **查到流水 = 金币已到玩家手上,只欠装备** → 把 `battle_drop_outbox` 对应行的
    `currency_amount` 清 0(`UPDATE ... SET currency_amount = 0 WHERE id = ?`,逐 id 改、不要按条件批量改),
    新代码上线后这一行只走装备一路,金币不会二次发放。
  - **查不到流水 = 金币压根没发出去** → **原样不动**。新键发放即首次入账,不存在双发。

- [ ] **⑤ 放开 publisher 后确认收敛**:② 捞到的那批 `id` 全部从 `battle_drop_outbox` 消失(行被删 =
  两路都成功),并且 `inventory_idempotency_conflict` 告警归零、这些 `match_id` 有对应的
  `drop_grant_delivered` 日志。

> 存量清干净并确认收敛之后,本节可以在下一个 tag 一起删除(它只对「跨过 2026-08-24 那次修复」的那次
> 发布有效)。删之前请先确认线上不再有旧版本 `battle_result` 副本在跑。

---

## 3. 开发机本地（不入包、不影响发布）

dev 联调要让本机 UE 用真证书认证连自签 Envoy。跑一次导入脚本即可(**不碰引擎、不进发行包**):

```powershell
pwsh tools/scripts/import_dev_ca.ps1
```

它把公开 dev CA 放进客户端工程 `Config/Certificates/`,并在 `DefaultEngine.ini` 设
`[SSL] DebuggingCertificatePath` 把 dev CA **叠加**到引擎公网 CA 包之上(不替换、仅非 Shipping 生效)。
之后 UE 用 `bDevInsecureTls=false` 也能过 TLS 校验。详见 [deploy/dev-ca/README.md](../../deploy/dev-ca/README.md)。

> ⚠️ 这只让**开发机 / 编辑器**信任本地 dev CA,证书在 `Config/`(不在 `Content`)→ 不打进发行包,与玩家无关。
> 生产靠公网 CA + 真实域名,玩家零配置。

---

## 4. 防呆机制（已落地）

已把 UE 危险开关改成**生产安全默认 + 编译期剔除**(`PandoraBackendSubsystem`):

- `bDevInsecureTls` 头文件默认翻为 **false**(生产安全意图)。dev 联调**推荐用证书认证**(见 §3 方案A 导入 mkcert CA),`bDevInsecureTls` 保持 false 也能正常 TLS 校验。注意:当前字段是意图标记,不直接控制 libcurl;实际信任来自 `[SSL] DebuggingCertificatePath`。
- `bAutoLoginForDev` / dev 账号 / dev 口令在 **Shipping 包编译期剔除**(`#if UE_BUILD_SHIPPING`):即使生产 ini 误设 `bAutoLoginForDev=true`,发行包也不会走自动登录。

> 这两项需 Codex/人 编译验证(UE Win64 + Linux DS,Shipping/Development 各编一遍,确认 `#if UE_BUILD_SHIPPING` 两分支均编过),按 [`AGENTS.md`](../../AGENTS.md) §11.1。

仍需人工保证的(脚本 + 清单已覆盖):`GatewayHost` 生产域名覆盖、后端 `*-prod.yaml`、公网 CA 证书。

> **dev 用不用证书认证?** 用,而且推荐用。dev 走证书认证的正确做法就是 §3 方案A(导入 mkcert CA),这样 `bDevInsecureTls=false` 也能过 TLS 校验,和生产同一条链路。

---

## 5. 发布流程顺序

1. 跑 `release_preflight.ps1` → 必须全 PASS
2. 构建与发布版本同源的 migration 镜像，运行一次性 migration Job；全部目标验收通过才继续
3. 完成下述 DS B1 / Stable-Canary 发布门禁；先预热 Canary（权重 0），验收后才逐级放量
4. 后端用 `*-prod.yaml` 滚动部署,确认 dev_skip_password/reflection 关
   - ⛔ **INC-20260813-001 v2 当前禁止发布**。复核证明“team 先于 matchmaker”只是必要条件,
     不是充分条件；详见
     [`decision-revisit-team-match-lifecycle-and-roster-rollout.md`](../design/decision-revisit-team-match-lifecycle-and-roster-rollout.md)。
     当前 `EndTeamMatch` 没有 match/member/ready generation，旧 outbox 重投可清掉玩家新一轮
     ready；claim 释放到 End 成功之间仍可复用旧 ready 开下一局。
   - 不得把 `Unimplemented` 直接 Warn 后当成功来“消除顺序”。这会让该局永久跳过复位，旧行为
     本身正是本 P0 的第一根因，不是安全降级。新旧 matchmaker RollingUpdate 共存时，
     battle_result 的长期 ClusterIP gRPC 连接还可能钉在旧 Pod；旧 Pod 会 ACK ReleaseMatch 并
     删除 outbox，却从不调 End。
   - 当前 `start.ps1` 会一次 apply 整个 overlay，并先向全部 Deployment 发 restart、之后才逐个
     wait；`Get-ServiceList` 的文本顺序不构成 rollout 屏障。在最终代际协议落地前，A-11 只能
     保持阻断；若最终仍保留 End，需用 blue-green/versioned endpoint 或 quiesce 阻止旧 caller
     ACK。回滚必须反向：先让 matchmaker/matchmaker-pve 全量停调，再退 team。
   - ds_allocator 的 45s roster deadline 同样不得直接随二副本 RollingUpdate 激活。必须先
     observe-only 全量采证，再只给全量新副本后创建的 policy generation 开 enforce；legacy /
     存量局默认不执行，否则可能把 rollout 时已局中掉线的正在进行对局误判弃。
   - 通用规则仍是 expand → migrate → contract；弱依赖降级只有在“跳过不会保留或制造错误
     状态”时才成立。新增跨服务 RPC 必须登记旧新 caller/callee 矩阵、live capability、失败停点
     和反向 rollback，并落为 fail-closed 机械门。
5. Envoy 换公网 CA 证书 + 真实域名
6. UE 用生产 ini 打 **Shipping** 包
7. 用真机 / 干净环境(没装过 mkcert CA)验证登录链路:登录 → 进大厅 → 匹配 → 战斗 → 结算
8. git push / tag / release(人手动执行,见 AGENTS §3)

### 5.1 DS B1 / Stable-Canary 硬门

- 目标集群不得残留旧单轨 `pandora-battle` / `pandora-hub` Fleet。Battle 只有在无 Allocated 后由运维
  显式移除；Hub 必须先用外部在线人数证据证明排空，普通发布器绝不自动删除。
- DSTicket K1 在独立受控步骤用 `dsticket_keyset.ps1` bootstrap。发布时只读对账 immutable
  `Secret/pandora-dsticket-signer-r<revision>` 与 `default`/`pandora` 两份同 hash、同 revision、同顶层 `active_kid` 的
  public JWKS。DS 只挂公钥；login / matchmaker / matchmaker-pve / hub-allocator 四个 signer 才挂私钥。
- 轮换只能分三次显式运行 `dsticket_rotate.ps1 -Phase stage|promote|retire`；不得塞进普通发布。
  `retire` 必须以 activation marker 的 apiserver `creationTimestamp` 满足 225 秒清退窗，且四 signer、
  Login、四 Fleet、GameServerSet/GameServer/Pod owner 链与存活残量全部通过门禁。脚本不删旧 key、
  不杀 Allocated DS；首次真实轮换还须补真 UE DS K1/K2 矩阵和集群审计证据。
- 普通发布与轮换共用 `pandora-dsticket-operation-lock`。遗留锁一律 fail-closed：先只读审计 holder、UID、
  marker 和现场对象，再由人决定后续；禁止按本机时间判断过期、自动抢锁或为赶发布直接删除。
- 玩家 DSTicket keyset 与 Model-B DS callback identity 是两套信任域。`-DsTicketKeysetRevision` 不能
  代替 `-DsFenceKeysetRevision`，callback HMAC 也不能放进玩家 JWKS/Fleet。
- `required_writer_epoch` 必须在发布前由独立激活流程显式建立并可线性读取；普通发布只读检查，
  missing 不得默认成 1。生产必须使用 etcd mTLS/custom CA/ACL 只读身份。
- Canary 镜像和 Stable 镜像都必须解析成 registry digest，显式 Canary digest 必须不同于 Stable。
  Fleet/GameServer/Pod 三层 release-track、Ready 池 imageID 与 annotation 全部对账成功才可加权。
- Canary 权重非 0 时禁止更换 `CanarySeed`。回滚先把 Battle/Hub 权重归零，再等旧 Allocated 会话排空；
  不杀在场 DS、不删 Hub Fleet、不轮换 DSTicket keyset。

当前 `start.ps1 -Mode online` 仍有三道**不可用 ACK 绕过**的生产硬阻断：DS auth etcd
mTLS/custom CA/ACL、真 UE DS 的 DSTicket v2 正负向 E2E、registry native immutable-tag/create-only
策略与发布锁。在相应证据由 Claude 审核、代码门禁被明确移除前，命令必须在 push/apply 前停止；
看到该停止信息是正确行为，不得注释 throw 强行上线。

---

## 6. 版本记录与可追溯(线上发布必看)

线上**不提交任何二进制**:git 只存源码 + tag,可部署的二进制以**容器镜像**存在 registry。
用**同一个版本号**把三处串起来,做到「线上某个 pod ↔ git 某次提交」可追溯。

### 6.1 版本 + commit + digest，焊成可追溯链

```
git tag v1.2.3                              ① 源码快照(人手动打,AGENTS §3)
  + commit b5a5a95
      └─► 镜像 pandora/<svc>:v1.2.3-b5a5a95 ② create-only 发布别名
            └─registry 回读─► repo@sha256:… ③ k8s 线上实际运行身份
```

二进制内部也烙了版本(编译期 `-ldflags -X` 注入 [`pkg/version`](../../pkg/version/version.go)):

- 每个服务**启动首行日志**就打印 `version=v1.2.3 commit=abc1234 built=... go=...`(由 `pkg/log.Setup` 统一输出)。
- 版本号来源:`tools/scripts/start.ps1` 的 `Get-VersionInfo` 跑 `git describe --tags` + `git rev-parse --short HEAD`,
  通过 Dockerfile build-arg(`VERSION`/`GIT_COMMIT`/`BUILD_TIME`)注入。
- 本地随手 `go run`(没注入)时回退 `version=dev commit=unknown`,不影响启动。

> 排障时先看启动首行的 version/commit，再看 Deployment/Pod 的 `repo@sha256:…`、运行时 imageID 与
> `pandora.dev/image-digest` annotation；四者应闭环。tag 只是发布别名，不能作为最终身份。

### 6.2 线上发布打版本(命令)

```powershell
# 1) 人手动打 git tag(冻结源码快照)
git tag v1.2.3
git push origin v1.2.3

# 2) 目标命令形态（仅在 §5.1 三道硬门有审核证据并明确解除后执行）
# Tag 必须含本次 git SHA；所有 DS 镜像最终由发布器回读并钉为 repo@sha256:digest。
pwsh tools/scripts/start.ps1 -Mode online -Env prod -ProdKubeContext pandora-prod `
  -Registry registry.yourcorp.com -Tag v1.2.3-b5a5a95 -BuildPush `
  -BattleDsImage registry.yourcorp.com/pandora/battle-ds@sha256:<stable-battle-digest> `
  -HubDsImage registry.yourcorp.com/pandora/hub-ds@sha256:<stable-hub-digest> `
  -CanaryBattleDsImage registry.yourcorp.com/pandora/battle-ds@sha256:<canary-battle-digest> `
  -CanaryHubDsImage registry.yourcorp.com/pandora/hub-ds@sha256:<canary-hub-digest> `
  -BattleCanaryPercent 0 -HubCanaryPercent 0 `
  -BattleCanaryReplicas 2 -HubCanaryReplicas 1 -CanarySeed ds-release-20260713 `
  -DsGatewayAddr pandora-envoy.pandora.svc:8444 -DsAuthMode enforce -DsAuthorityMode redis `
  -DsFenceEtcdEndpoints <mtls-etcd-endpoint> -DsFenceKeysetRevision <callback-revision> `
  -DsTicketKeysetRevision 1
```

- 上例是 Canary **预热**（权重 0）；确认 Ready/指标后，下一次运行保持相同 digest/seed，只把百分比
  调到小值。异常时先用同一参数把两项百分比归零。
- `-BuildPush` 在门禁解除后才会构建 20 个服务镜像(自动带 git 版本烙印)、以不可变 tag 推送，
  再解析 registry digest 并部署；tag 不是最终运行身份，运行身份必须是 digest。
- **git push / tag 由人手动执行**(AGENTS §3),脚本不替你推。

> dev/docker 模式的镜像 tag 固定 `:dev`(跟最新代码走,不需要版本号)；线上 tag 必须带 git SHA，
> 例如 `v1.2.3-b5a5a95`，且最终以 registry digest 为准。
