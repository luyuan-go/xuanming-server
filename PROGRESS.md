# Pandora 进度记录

> 2026-07-01 整理版。旧版里大量 RPC 表、Redis key、命令流水、重复验证清单已经压缩/删除。
> 本文件只保留新会话接棒必须知道的进度、重要决策、破坏性变更、风险和待办。
>
> 细节归档规则:
> - 架构和长期决策:看 `docs/design/pandora-arch.md`
> - 服务契约和端口:看 `docs/design/go-services.md` / `docs/design/infra.md`
> - proto 规则:看 `docs/design/proto-design.md` 与 `CLAUDE.md` §5
> - 协议 response/push 语义:看 `docs/design/protocol-ordering-rules.md`
> - 压测纪律和报告:看 `docs/design/stress-discipline.md` 与 `docs/design/stress-*.md`

## 当前状态(截至 2026-06-30)

- 后端主路线:Go + Kratos + Envoy + gRPC-Web over HTTP/2 TLS + 集中 push gRPC server stream。
- UE 侧命名统一为 Pandora,`Xuanming` / `Xm` 已废弃;proto source of truth 在后端仓库。
- UE Hub/Battle DS 骨架已完成,包含 `APandoraHubGameMode` / `APandoraBattleGameMode` 与 Agones SDK 接入。
- `services/` 已按业务域分组,当前 workspace 有 19 个 Go 服务:
  - account:`login`,`player`
  - data:`data_service`
  - social:`friend`,`chat`,`dialogue`,`guild`,`mail`
  - matchmaking:`team`,`matchmaker`
  - runtime:`player_locator`,`push`,`leaderboard`
  - economy:`trade`,`inventory`,`auction`
  - battle:`ds_allocator`,`hub_allocator`,`battle_result`
- 基础设施:MySQL / Redis / Kafka / etcd / Prometheus / Grafana 走本地 compose;Envoy 作为 edge gateway。
- 当前最新业务进度:mail 服务上线;player 领奖记录底座上线;cellroute 装配层已接进主要服务;配置表热更路线已拍板。

## 2026-07-01 追加

- 开发编排:含战斗混合模式第一版落地。`start.ps1 -Mode battle` / `play.ps1 -Battle` 现在走
  17 个业务服务容器 + 宿主 `ds_allocator`/`hub_allocator` 的形态,用于本机/内网真实 Windows DS
  联调;`local` 保留 19 个宿主 Go 进程断点调试口径。
- 已做项目内轻量验证:默认 cluster 配置仍指向容器服务名,`-HostAllocators` 才把 allocator RPC
  改为 `host.docker.internal:20020/20021`。真 Docker + Windows DS + UE 客户端端到端跑一局仍待人工验收。

## 重要决策索引

- 2026-06-04:后端框架从 go-zero 切到 Kratos;Edge Gateway 选 Envoy;客户端业务通道走 gRPC-Web over HTTP/2 TLS。
- 2026-06-04:客户端两条连接固定:UE NetDriver 只承载游戏内同步,FHttpModule 承载业务请求和推送。
- 2026-06-04:push 架构固定为集中 push 服务 + gRPC server stream,不做自研 WebSocket gateway。
- 2026-06-03 起:RPC response 与 push 乱序是协议语义问题,不是单连接能解决的问题;具体原则见 `protocol-ordering-rules.md`。
- 2026-06-06:客户端可见结构与服务端存储快照强隔离,不准直接把 StorageRecord / Redis value / DB row 推给客户端。
- 2026-06-11:Snowflake 继续本地发号;多副本 nodeID 走 etcd Lease 分配,拒绝 Redis INCR 发号。
- 2026-06-18:friend / chat 所在 `pandora_social` 拍板切 TiDB 路线;Go 业务尽量保持 MySQL 协议兼容。
- 2026-06-19:trade 不承载全服拍卖行;全服拍卖和撮合独立为 `auction` 服务。
- 2026-06-26:DAU 目标从 200 万上调到 2000 万,采用 Region -> Cell -> Cell 内分片三层化路线。
- 2026-06-27:采用轻量 DDD 思想,不把“微服务 + 事件”误认为 DDD。
- 2026-06-30:配置表热更走自研轻量流水线:版本号 + checksum + staging + reload + 原子切换 + 失败保留旧配置。
- 2026-07-01:确立不停服更新(零停机)为硬约束:go 服务无状态滚动更新 + Redis 二进制 pb 存储双向兼容演进(只加字段/懒迁移,禁改编号类型、禁 read-modify-write 丢 unknown fields)。见 `CLAUDE.md` §9 不变量 16/17、`docs/design/zero-downtime-update.md`。
- 2026-07-09:**data_service 未正式上线**，截至本日仅用于本地开发/minikube 验证，无外部调用方，也没有需要保留的旧协议、Redis 缓存或 `player_data` 表有效数据。因此本轮 PlayerData blob→强 schema 改造按开发期例外处理，可在停 data-service 后定向清 `pandora:data:player:*` 并 `DROP pandora_player.player_data`；该例外不适用于未来正式上线或产生有效数据后的 schema 演进。
- 2026-07-06:Battle DS 空场回收拍板「回收 + 宽限窗」双层方案(对齐业界 empty-server-timeout):DS 侧空场计时器自结算为主路径(UE 仓库待实现,建议 2~3min),后端 `ds_allocator` 按 `player_count==0` 持续超 `empty_battle_timeout`(默认 5m,须 > 断线重连窗口 ~30s)心跳内判 abandoned + 回收 + 段位回滚补偿兜底(已上线,复用心跳超时补偿链路)。**[proto]** `BattleStorageRecord` 新增 `empty_since_ms=11`(存储侧字段,加字段兼容演进,客户端无感知,无需 UE 同步)。契约见 `agones-dev.md` §3.2。
- 2026-07-06:matchmaker 两道 locator 离线判定门(成局最终门 findOfflineMembers + 队列在线扫除 livenessSweep)收进开关 `match.liveness_gate_enabled`,**默认关闭**:离线判定依赖 Hub DS 心跳捎带 `player_ids`(hub/v1 HeartbeatRequest)续期 locator HUB 位置,UE Hub DS 生产端尚未实现;先上线服务端会把在线玩家 30s 后误判离线、扫掉排队票据。**待 UE Hub DS 上报 player_ids 联发后才可开启**(开启路径已完整实现并有测试)。同批:hub_allocator `RefreshHubPresence` 改 goroutine + 独立 3s 超时(同 ds_allocator.refreshBattleLocations),locator 抖动不再拖慢 Hub DS 心跳响应。→ **2026-07-08 已开启**:两真实打包客户端实机联调验证保活链(UE Hub DS→hub_allocator→locator→Redis),K8s Redis 采样在线玩家 locator TTL 稳定 25~30s 回升不衰减;`matchmaker-dev.yaml`/`matchmaker-pve.yaml` 置 `liveness_gate_enabled: true`(cluster 配置由 gen_cluster_config.ps1 从此生成)。回归失败(在线玩家 TTL 单调掉 0)先关此开关再排查。**同日端到端验收通过**:①配置生效(configmap 重建 + matchmaker/matchmaker-pve 滚动重启,两 pod Ready);②离线扫死票——无 HUB 位置玩家入队,一轮 sweep 内 `liveness_sweep_reaped_ticket` 删票,Redis ticket/claim/queue 全清;③在线不误删——synthetic HUB 位置每 5s 续期挂队列 45s,9 次采样票据完好、无 reap 日志。液门(liveness gate)正式生效。残余(非阻塞):真机 UE 登录后挂队列的纯真机复验。
- 2026-07-07:根治「重启电脑/换模式后 500xx 端口被旧 compose 业务容器劫持」:`docker-compose.services.yml` 业务容器 `restart` 由 `unless-stopped` 改 `"no"`(dev 业务容器生命周期一律由一键脚本显式管理,不随 Docker Desktop 开机自启;dev.yml 基础设施保留 unless-stopped);`k8s_envoy_bridge.ps1` 三处加固——预检 `Stop-StaleComposeContainers` 自动 stop 发布 bridge 端口的 pandora-* 容器、端口检测扩到 0.0.0.0/::(docker 发布端口监听在 0.0.0.0,旧检测只查 127.0.0.1 看不到 → 双监听并存导致 Envoy 流量去向不确定)、占用者为 com.docker.backend 等 Docker 转发进程时拒绝 Stop-Process(会杀整个 Docker)。**与不停服滚动更新(不变量 16/17)无关**:灰度升级的载体是 k8s Deployment RollingUpdate,compose 只是本地 dev 环境且本身无滚动更新能力。流量切换时序、gRPC 长连接 L7 均衡、金丝雀灰度四阶段已补进 `zero-downtime-update.md` §6。终局方向(未做):envoy 部署进 k8s,消灭宿主 500xx 桥接层。
- 2026-07-08:滚动更新流量切换两项基础能力落地(zero-downtime-update.md §6.5 前两项):① `deploy/k8s/services/services.yaml` 20 个 Deployment 全部加原生 gRPC readinessProbe(打 grpc_health_v1,Kratos 默认注册、Stop 自动 NOT_SERVING;新 Pod 必须 SERVING 才进 Endpoints 接流量);② gRPC 连接轮换:`pkg/config.Grpc` 新增 `max_conn_age`/`max_conn_age_grace`,`pkg/grpcserver` 按配置挂 keepalive MaxConnectionAge(零值=关,行为不变),20 个服务 dev yaml 全量开 15m,ds_allocator 显式 grace 90s(盖过 AllocateBattle 同步等 DS ready 的 ~60s,防 GOAWAY 砍断在途分配)。验证:pkg + 18 个服务 module 全部 go build 通过、pkg/config 测试绿、kustomize 渲染 20 个 readinessProbe。剩余待补(扩多副本前):服务间 headless/L7 均衡、RollingUpdate 策略显式化,见 §6.5。
- 2026-07-08:**角色养成五件套(角色界面/装备更换/属性加点/天赋树/背包道具使用·出售)对客户端放行 + IDOR 加固**。核心结论:这五个都是**局外(meta)系统**,与 MOBA 战斗延迟零耦合(客户端走 Envoy→player/inventory 独立 gRPC 通道,DS 战斗内绝不同步回调 Go),后端 proto/表/biz/data/service 早已实现,真正缺口只是「安全地暴露给客户端」。改动:①`player`(:20002)/`inventory`(:20015)两 cluster 接进 Envoy edge(`deploy/envoy/envoy.yaml`,STRICT_DNS/V4_ONLY/http2,host.docker.internal,k8s 复用同文件经 bridge 转发);②两服务全 RPC 加 `jwt_authn` 需 `pandora_session`(R5 player_id 以 JWT sub 为准);③系统/内部方法双保险 403——Envoy 精确 path `direct_response 403`(player:UpdateMMR/GetMMR/UnlockHero/GrantAttributePoints/GrantTalentPoints;inventory:GrantItems/GrantInstances/FreezeForOrder/SettleAuctionMatch/SettlePlayerTrade/ReleaseEscrow)+ 服务层兜底。**player 服务 IDOR(OWASP A01)修复**:原先信任请求体 `player_id`,任意登录客户端可读写他人数据;仿 inventory 的 `callerPlayerID` 模式加三个纯鉴权辅助——`selfPlayerID`(客户端自助写:身份缺失→UNAUTHORIZED,body≠caller→PERMISSION_DENY,回落 caller)、`resolvePlayerID`(读,双模式:内网直连 callerID==0 信任 body,客户端强制自身)、`systemOnly`(callerID≠0→PERMISSION_DENY)。读/写/系统三类分流已套全 handler,`s.uc.*` 一律传解析后 `playerID` 不再用 `req.GetPlayerId()`。`GetProfile` 默认自查(安全默认;跨玩家看板将来另开 `ViewProfile`)。当前无 PlayerService 内网 gRPC 调用方(grep 无 NewPlayerServiceClient),改动不破坏既有链路(battle_result 的 GetMMR reader / matchmaker·DS 的 GetLoadout 快照注入均 callerID==0 走信任分支)。验证:`go build`/`go vet`/`go test ./...` 全绿(新增 `internal/service/auth_test.go` 覆盖三辅助分流),envoy.yaml yaml.v3 解析通过。**残余(UE/人工领域)**:UMG 面板调这些 RPC(需带 player SessionToken JWT,个人数据自查)属客户端侧,按 AGENTS.md §11 交 UE/Codex。**待确认**:「技能卡」若为独立于天赋(player_talents/SetTalents/GetTalents)的系统,可能是真实未来缺口。
- 2026-07-08:**延迟不变量固化**——局外系统放 Go 零战斗延迟,是架构决定不是调优结果:①客户端→Go 大厅连接(gRPC-Web/HTTP2)与客户端→DS 战斗连接(NetDriver/UDP)物理独立、不共享带宽与故障域;②DS 帧循环里没有对 Go 的同步调用(tick 全走 GAS/Replication,DS→Go 只剩 Heartbeat 5s/GetLoadout 开局一次/battle_result 局后一次,全独立 goroutine+5s 超时不阻塞主 tick);③唯一会真拖慢延迟的错误做法 = 让 DS 战斗中同步 RPC 大厅服务,守住「开局快照 + 局后上报」边界即永不发生。红线:任何"战斗内实时读写 player/inventory/economy"需求必须改造成开局快照或局后异步上报,否则推翻。落文档 `docs/design/ds-arch.md` §0.6(配套 §0.3/§0.5)。
- 2026-07-10:**Agones DS 回调拓扑本地/线上同构**——Fleet 的 5 处回调统一指向集群内
  `pandora-envoy.pandora.svc:8444` 且默认明文 `TLS=0`;minikube 自动部署/重载 in-cluster Envoy，
  online NetworkPolicy 仅允许带 Agones GameServer 标签的 Pod 访问 8444。宿主客户端面 8443 可按模式
  对局域网开放；未鉴权的宿主 DS 面 8444、admin 9901、基础设施与 20 个业务 gRPC 发布端口默认
  固定回环（特殊 Linux dev 环境须显式覆盖）。安全残留：方法白名单和
  NetworkPolicy 不等于 DS 身份认证，生产仍需 mTLS/ext_authz/短时效 DS token 并绑定 pod/match。
- 2026-07-10:**战斗 DS 并发容量监控 + 告警通知链路**落地。①`ds_allocator`(mode=agones)新增
  Fleet 容量巡检 `internal/biz/capacity.go`:定期 GET 通用 Fleet + 各 map_fleets 专属 Fleet 的
  status,暴露指标 `pandora_ds_allocator_fleet_{replicas,ready,allocated,usage_ratio}`(label=fleet),
  `allocated/replicas ≥ capacity_warn_ratio`(默认 0.8)打 Warn `ds_fleet_capacity_near_limit`、
  `ready==0` 打 Error `ds_fleet_capacity_exhausted`,状态变化才打 + 5m 重报降噪。配置
  `agones.capacity_watch_interval`(默认 30s,负值禁用)/`capacity_warn_ratio`;dev yaml +
  gen_cluster_config.ps1 in-cluster 模板同步;复用既有 RBAC `fleets: get`。②**告警出口唯一 =
  Grafana 统一告警**,业务服务只暴露指标/打日志,绝不直连通知端点(见 `docs/design/alerting.md`)。
  新增 `deploy/grafana/provisioning/alerting/`(contact-points/templates/notification-policies/rules):
  群通知走企微(原生 wecom)/飞书(需已验证 relay),个人推送走 ntfy(compose 内置
  `binwiederhier/ntfy` 服务,开 SQLite 磁盘缓存)。secret 保留 `$__env{}` 占位,本机 `dev.env`
  被 git 忽略,受跟踪的 `dev.env.example` 只放空占位;Grafana entrypoint 仅为非空群 env
  生成 receiver。有群时 warning→群、critical→群+ntfy;无群时均回退 ntfy。首批规则消费
  上述 DS 容量指标(warning near-limit / critical exhausted),非 Agones 模式 NoData 按 OK。
  验证:ds_allocator go build/vet/test 全绿(新增 capacity_test.go + ListFleetCapacities 测试);
  compose config、Grafana 11.3.1 空群/有群两种隔离 provisioning 均通过，本机 compose 已重建
  ntfy/Grafana 并经 API 确认联系点/规则/路由，ntfy 文案模板与 SQLite 跨重启回放实测通过。**待办**:飞书 relay、
  k8s Grafana 同步、ntfy 公网鉴权、Grafana 安全版升级后再开 DingDing、扩展更多规则(见 alerting.md §9)。

## 已完成里程碑

### 基础骨架

- 仓库文档、proto 工具链、公共 `pkg/` 框架、dev compose 和脚本已搭好。
- proto 已覆盖核心业务域,并经历过多轮规则收紧:业务 ID 用 `uint64`,配置表 ID 默认 `uint32`,枚举保持 enum/int32 语义。
- 服务目录已从根目录平铺改为 `services/<domain>/<service>`。
- `go.work` 多 module 模式为当前构建口径;根目录不加单根 `go.mod`。

### 协议与网关

- Envoy + gRPC-Web 架构已落文档,dev TLS / 生产 TLS 策略已明确。
- UE 5.8 FHttpModule 已确认支持 HTTP/2 TLS 与流式接收,客户端可自研 gRPC-Web 解析,不引入第三方 UE gRPC 插件。
- JWT session / DS ticket 已真实化,Envoy `jwt_authn` 已接入。
- push 服务已接 Kafka + Redis ZSET 离线 5min,订阅核心 push topics。

### 核心服务闭环

- `login`:MySQL / Redis 真实化,接 `hub_allocator.AssignHub`,支持 dev skip password / auto register。
- `player`:档案、MMR、出战养成、装备预设、天赋树、领奖记录底座已上线。
- `player_locator`:MATCHING / BATTLE 状态机守卫和 BATTLE fence 已补。
- `team`:组队服务上线,已补 `GetMyTeam`;客户端同步约定已记录到 `go-services.md`。
- `matchmaker`:5v5 撮合、auto-confirm 语义、两级跨 region 撮合基础已落地。
- `ds_allocator`:真实 Agones GameServerAllocation、abandoned 补偿链路已打通。
- `hub_allocator`:大厅分配、自动扩缩容、强制整合与玩家迁移通知已落地。
- `battle_result`:战斗结果幂等落库、MMR 更新、player.update 事务出箱可靠化已落地。
- 2026-06-09:真 Agones + Kafka + MySQL 两段补偿链验证跑通。

### 社交与运行时

- `friend`:好友、黑名单、请求闭环 RPC 已补;分布式好友图路线文档化,本地 TiDB 验收通过。
- `chat`:世界 / 队伍 / 私聊 / 公会 / 临时群五频道扩展已落地。
- `dialogue`:NPC 对话树运行时服务上线。
- `guild`:公会 + 临时群聊服务上线,chat 已接 guild/group fan-out。
- `mail`:系统/公会邮件 channel + watermark 拉取,个人邮件写扩散,附件领取幂等,claim 越权问题已修。
- `leaderboard`:通用排行榜服务上线,支持 Redis ZSET 实时榜 + MySQL 结算归档。

### 经济与资产

- `inventory`:大厅背包上线,覆盖货币、可堆叠道具、使用/出售/授予和 ledger 幂等。
- `trade`:玩家交易上线,后续接 inventory 真实 P2P 原子对转,替换 NoopResourceLedger。
- `auction`:全服拍卖行/跨玩家撮合引擎上线,含 escrow 冻结、per-market 单写者、过期清扫和 inventory 结算。
- `auction` 真依赖本机冒烟通过;buyer/seller 资产变更链路已验证。

### 扩容与平台能力

- `pkg/snowflake/etcdnode`:etcd Lease 分配 nodeID 底座已落。
- `redisx.NewUniversalClient` 与 `mysqlx.ShardSet` 已作为 Redis Cluster / MySQL 分片底座。
- `pkg/cellroute`:Region/Cell/Cell 内分片确定性路由、热更新和 etcdtable 子 module 已落。
- cellroute 装配层已接入主要服务 main;默认 `mode=off` 保持单 Cell 行为。
- 本地 k8s + Agones + 端到端 hello world 已完成;生产 k8s 形态另行定稿。
- UE DS D5-D6 骨架代码已完成;GAS/Iris 深度玩法联调继续按 UE 主线推进。
- Kill-Switch RPC 级临时关停与自动防护四层方案已落地。
- 配置表热更方案已形成文档:不接 Apollo/Nacos,先复用 etcd 做版本通知。

### 压测与工具

- `robot/stress` 机群和压测三脚本已落地。
- P0 本机 80 VU harness 已跑通,并完成多轮修复:
  - error 调用点归因
  - shutdown canceled 与真实 error 分流
  - auto-confirm 竞态修复
- 最新 P0 结论:80 VU 冒烟可跑通,真实 RPC error 已归零;但这不是单 Cell 40 万 CCU 验收。

## 重要破坏性变更 / 客户端需同步

- trade proto 已从实例道具 `item_uid` 语义切到可堆叠 `item_config_id + count` 模型,并支持 `buyer_items`。UE 客户端必须按新模型同步;若产品坚持实例道具交易,需要另起设计复议。
- player 领奖记录新增 `ClaimReward` / `GetRewardClaims` 与 `RewardClaimStorageRecord`,已生成 Go/C++ pb。
- mail proto 已上线,需确认 UE 侧 C++ pb 与 UI/红点逻辑同步情况。
- Region/Cell 字段曾随 DS ticket / login 路由接线发生 proto 变更,继续改 proto 时必须跑完整生成和启用 module 编译。

## 当前风险与待办

- player 领奖记录目前只记录“已领取状态”,还未把奖励发到 inventory;完整领奖链路需接 `inventory.GrantItems` 或货币变更。
- leaderboard 仍有一个业务问题待修:同一 `settle_idempotency_key` 复调不会重复发奖,但 `reset_after=true` 后响应未从 MySQL snapshot 回放 winners。
- trade -> inventory 的真实 P2P 原子对转已有代码和单测,但仍需真 MySQL / gRPC 端到端冒烟,并确认 UE 接受 trade item 模型变更。
- 蜂窝扩容代码地基已到 cellroute 装配层,但 24 Cell / 3 Region 物理部署、多 k8s、分库分表、跨 region Kafka 桥仍未落地。
- 单 Cell 满载压测未启动;目前只有本机 P0 80 VU 冒烟,不能声明性能达标或进入多 Cell。
- 本地 Windows + `ds_mode=stub` 下 `AllocateBattle` 慢路径属于假慢;接真 DS 后需重新测量。
- k8s 生产形态仍未最终定稿;本地以 minikube / Agones dev 验证为主。
- push 横扩、Agones 池化、TiDB/Kafka 集群化属于后续 infra 工作,按 AGENTS §11.1 由 Codex/人执行环境侧动作。

## 后续记录方式

以后往本文件追加时只写这几类:

- 已拍板且会影响后续实现的决策。
- 新服务/新能力是否真正上线,一句话说明边界。
- proto/API 破坏性变更和客户端同步要求。
- 真实验证结论,尤其是压测、端到端冒烟、生产风险。
- 当前 blocker、未修 bug、需要人拍板的问题。

不要再写:

- 单个 RPC 的逐项语义表。
- Redis key / SQL 字段 / 配置项流水账。
- 完整命令清单和每条命令输出。
- 与 `docs/design/` 已有内容重复的大段解释。
- 每个文件逐条列“新增/修改”清单,除非它是破坏性变更索引。

## 2026-07-10:DS 回调服务令牌认证(审核 P1 #1,已拍板并实现)

- 已拍板决策:DS→后端回调(:8444 七个白名单方法)补服务令牌认证——allocator 签发
  scope-bound HS256 JWT(battle 绑 match_id / hub 绑 pod),经 GameServer annotation
  `pandora.dev/ds-token` / env `PANDORA_DS_TOKEN` 下发,四服务 handler 用
  `middleware.DSCallbackGuard` 校验;Envoy :8444 盖 `x-pandora-ds-gateway` 标记头区分
  DS 面与内部直连。详见 `docs/design/decision-revisit-ds-callback-auth.md`。
- 边界:后端全部接线完成(pkg + ds_allocator/hub_allocator/player_locator/battle_result),
  `ds_auth.mode` 默认 **off**(行为与现状一致);proto 零改动。
- 客户端(UE DS 仓库)待同步:读 annotation/env 拿令牌,7 个回调方法带
  `authorization: Bearer`,Hub DS 监听 annotation 续期换新令牌;接完后先 permissive 再 enforce。
- 部署待办(Codex/人):重发 `deploy/k8s/agones/10-rbac-allocator.yaml`(gameservers 补
  patch 动词)、重滚两份 Envoy 配置(均已 `envoy --mode validate` OK)。
- 验证:5 模块 build/vet/test 全绿;enforce 端到端冒烟待 UE DS 接令牌后执行。

## 2026-07-11:部署/重置阻断项 C/D/E 修复，A/B 保持待决策

- 配置生成改为独立 staging 精确校验 20 份 YAML(+生产 JWKS)、自有文件事务发布/失败回滚、
  目标目录物理文件锁；Online 在 BuildPush 前生成本次独占快照，Secret 只取这 20 份配置，堵住
  共享 `run/cluster/etc` 被 dev/mock 并发覆盖后灌入生产的窗口。
- K8s 配置载体按 Secret→Deployment 全量 rollout→控制器/存活 Pod 无旧引用→删除旧 ConfigMap
  的顺序迁移；Resume 也逐个等待 20 个业务 Deployment。删除旧 ConfigMap 是明确回退边界，
  迁移前零副本 ReplicaSet revision 不再可直接 rollout undo。
- compose data_service reset 改为 checked 容器状态 + compose label 校验；stopped/absent 都需宿主停服
  确认，stop 后复查才允许清理，`-Restart` 不再因容器不存在而静默忽略；验证失败会确认停服后
  重新清缓存/删表，停服未知或保护清理失败均组合报错。
- 文档已纠正 DS 轮换 TTL 没有上限的事实，并新增玩家 JWT 轮换待决策记录。
- **仍阻塞、本轮 Codex 未改业务代码**:当前工作树已有 Hub token-exp 代际/玩家多 key 的部分接线，
  但尚未获决策批准；Hub 仍缺 Agones⇒enforce 的启动期硬耦合，且秒精度 exp 不是无碰撞 generation。
  玩家/DS 全集群 primary+additional 权威 key-set gate 与轮换算法/密钥权属也待人拍板。生产脚本暂时
  拒绝 additional_secrets，避免未批准的半链路投产。详见 `decision-revisit-ds-callback-auth.md` §6.4 与
  `decision-revisit-player-jwt-key-rotation.md`。

## 2026-07-12:DS callback auth Model B P1 收束；生产行为激活仍阻断

- 人已批准 Redis 唯一授权权威 + active/pending 两阶段方案。当前脏工作树已接 Hub/Battle 两类
  auth record、UID/epoch/gen/jti/kid/hash/writer 完整绑定、K8s UID+RV 条件投递、跨服务 active 门、
  login assignment fence，以及 UE active+staged token/Bearer/ACK 切换。这里的“已接”仅描述当前脏
  工作树，不代表生产行为已经获准激活或真集群验收通过。
- 独立反证后追加修复：Battle result receipt 先落库后允许 ended、GSA/PATCH 未知结果 fencing、
  `allocation_uncertain` 永久隔离与 UID 条件 Release 墓碑（**不能**用 sweep 猜测恢复）、Hub assignment
  future protobuf 字段保留、同实例轮换不重复占座、
  Hub/Battle 完整 tuple 紧急 quarantine + audit-only ops CLI、etcd required value+ModRevision 注册 CAS、
  后台 writer 在 capability 成功后才启动、Redis auth↔live projection 双向三周期审计、login 在线
  VerifyDSTicket 的 Guard→active/projection→assignment/roster→JTI 顺序、battle 票签发前
  player↔match roster 权威门，以及 Redis authority 下拒绝无凭据 `pandora.battle.result` consumer。
  locator 已明确 `InBattle` 后，Battle 签票权威失败会使 Login 返回 unavailable，不再错误继续分配
  Hub/占第二个 seat/写 LOGIN_PENDING；reconnect 地址取自同一次 Redis roster/active 快照，不再把
  locator 的旧 UID 地址与当前实例的授权证明拼接。
- 本机调试只允许 Windows `mode=local + off + legacy` 的精确 `local-off-v1` profile；仍签完整
  `(pod,随机 UID,epoch,gen,jti,exp,kid,writer=2)` 凭据。profile/pod 前缀/平台任一伪造均不能关闭在线
  admission。它是明确的非生产例外，不解决生产密钥投递或 Battle DS 的客户端侧空 roster 防御。
- UE 长驻 Hub 的本地 JTI 防重放已从永久无界 `TSet` 改为按票据 exp 回收的线程安全有界 map；
  满载不驱逐未过期值而是 fail-closed，`PreLoginAsync` 仅检查/清过期且不新增、真正接纳时才消费。
- UE 心跳响应现绑定“本请求实际 Bearer snapshot + 回调时仍为 current active”：Hub/Battle 的
  missing/wrong/stale ACK 都在 stop/drain/reload 前失败；activation 只有精确 staged ACK 成功提升后
  才处理命令，active=A/staged=B 时迟到 ACK(A) 不再误报成功。`grpc-status` 同时改为 canonical
  `0..16` 有界解析，整数溢出/前导零/重复/缺失均不能冒充 status 0。
- UE admission owner 不再直接信任平台 `FGuid` 的字段布局/宽松 parser；改由 vendored OpenSSL
  CSPRNG 生成并显式编码小写 RFC4122 UUIDv4，随机源失败在网络前拒绝，validator 逐字符检查
  36 长度/连字符/version/variant。Automation 首轮正是由 UUID 反例失败并触发此修复，不能把首轮
  10/11 误报为全绿。
- 本地最终验证：UUID 修复后的 PandoraEditor UBT 714/714 Succeeded（1164.39s）；headless
  `Pandora.Net` 11/11 + Battle terminal 1/1（合计 12/12）；PandoraServer UBT 824/824，
  `PandoraServer.exe` 链接成功（1132.36s）。服务端相关 Go test/vet、`buf lint`、两套 Kustomize render、
  Envoy config validate、PowerShell AST、生成物跨仓 SHA-256 与 `git diff --check` 也已通过。
  `-race` 因 `CGO_ENABLED=0` 未执行且未改环境；真 Redis Cluster/envtest/minikube、安全 `:8444`
  synthetic 与真 Hub/Battle 往返仍未验收。
- **禁止宣称可生产激活**：审计证实 required=1 尚未约束数据面，新 Model-B 与旧 legacy writer 会在
  普通滚动窗口混跑。需人拍板 blue/green prepare→quiesce→active（推荐）或逐路由 stage-only 门；
  同时仍缺 immutable image/Secret、etcd/Redis TLS+ACL、仓内固定 K8s :8444 synthetic，以及
  :8444 service-mesh mTLS/等价传输身份与机密性。当前 Fleet
  还未安全注入 `PANDORA_DS_TICKET_SECRET`：真实生产 signer 与 UE tracked dev 占位会导致全拒票，
  若生产沿用占位则直接失守，因此两种情况都不可上线。也不得把现有玩家 HS256 签名 secret 直接
  注入不可信 DS；须拍板 DSTicket 公钥验签或只走 online authority。
- 仍待人拍板：Hub active A→B 后旧 A 票据的 bounded `previous_admission` grace；Battle 结算与
  MySQL 同事务的 terminal-release outbox；Hub quarantine 后 assignment/UID 持久迁移；
  `allocation_uncertain` 权威 reconciler；locator 连续查询失败时选择“未知即拒绝”或版本化
  placement lease + Hub admission 最终门。现状只保证已明确 InBattle 后不会误分 Hub；locator 状态
  不可判定仍沿用既有弱依赖行为，因此也是生产 blocker。
  `tools/scripts/activate_ds_auth.ps1 -Apply` 已主动 fail-closed，只保留 audit。
- Claude Code 首轮独立只读审核仅覆盖 `hub_allocator` Model B 后端与 auth/middleware 抽检：未发现新的
  鉴权 P0/P1，确认过期方法名注释一处（已修）。其把心跳实报覆盖 reservation 定为 P2，但后续反证确认
  assignment 默认 30min、票据默认 5min、心跳 5s，且无逐 reservation ledger/到期退座；不同玩家可跨
  多轮累计有效票据，不能证明容量上限，改列 §7.16.4 P1 生产阻断。标准修复需人拍板独立 reservation
  lease/session 容量权威，当前未暗改。另修复等值 assignment 复用重签票却不刷新 Redis TTL 的缺陷，
  统一走完整 bytes CAS SET 并补剩 1s 回归。UE、Battle 全调用点、`-race`、真集群仍未被该轮 Claude
  审核覆盖。
- 经人于 2026-07-12 明确授权，由 Codex 仅为本次范围创建本地 Git 提交；未 push/tag，未碰生产，
  未写真实 secret。部署验证时误触发 Docker 自动拉取本机 `envoyproxy/envoy:v1.38.1`，发现后已停止
  后续 Docker 操作；该镜像保留或移除仍待人指示，不能再宣称“未改本机环境”。详见
  `docs/design/decision-revisit-ds-callback-auth.md` §7.14–§7.16。

## 2026-07-12:全代码审核后的 Codex 部署防线修复；业务阻断交 Claude Code

- online 发布代码已补不可变 SHA tag、首次 push 前的全 tag 不存在证明、push/registry digest 一致性、
  20 Deployment 与 2 Fleet digest pin、五 writer/Fleet 两层 annotation、rollout 后 spec/imageID 回读；
  旧 Allocated DS 只排空不强删。纯 helper/mutant 测试与 PowerShell AST 已通过，未访问远端。
- 新增 `dsauth-required` 只读线性预检及 uint32 参数防截断；test online 下 required key 缺失/非法/
  不可读会在远端写前停止。prod 因五 writer/预检尚未统一 custom CA+mTLS+ACL 最小权限身份，已在
  明文/无身份读取前硬阻断；没有调用现有 `BootstrapRequired`，其删 key 后回退风险仍待 Claude 修。
- 生产发布仍被 DSTicket B(公钥 keyset)或 C(online authority)未决硬门拦住；Fleet 禁止注入玩家
  HMAC/私钥。BuildPush 另被 registry native immutable-tag/create-only+发布锁未验证硬门拦住；
  clean tree/严格重建/provenance 与结构化 manifest mutant 已补。离线 tar 与 registry manifest digest
  等价性也未在真实 CRI 证明。
- Codex 按 `AGENTS.md §11.1` 未修改业务逻辑。player 属性点 `int32` 求和溢出与 auction 跨物品撮合
  仍是 P0;其余跨服务 P1 修复队列及验收反例见
  `docs/design/code-audit-blockers-claude-handoff-20260712.md`,交 Claude Code 实现并独立审核。

## 2026-07-12:拍卖缓存正确性与属性 MySQL 集成测试阻断修复（待 Claude Code 复审）

- 人明确授权 Codex 仅本次修复这组业务阻断。拍卖撮合候选改由 MySQL 按
  `market_id + item_config_id + side + active status` 权威选择，查询直接排除 incoming owner，
  按价格/`order_id` 时间优先；Redis ZSET 退为 best-effort 旧版本兼容缓存。删除临时移出/放回、
  Redis reconcile SET、10,000 截断重建和扫描上限路径，缓存 `ZADD/ZREM` 失败不再让已冻结且
  已持久化订单报失败或永久不可见。128 条异物品/自有前缀、Redis 全挂、缓存缺失、撤单降级、
  价格时间优先用例均通过。
- 新增 additive-only `idx_instrument_match` 与 `pandora_auction/000002` 迁移；真实 MySQL 8.4 已验证
  init 已含索引时幂等、老库无索引时 `LOCK=NONE` 新增均成功。安全发布要求先扩索引，再蓝绿
  一次切流并排空漏洞旧实例；详见 `decision-revisit-auction-match-authority.md`。
- 属性 repo MySQL 测试改为拒绝带库名 DSN、随机临时库、严格 cleanup；用触发器在属性 upsert 后
  强制最终扣点失败，验证完整事务回滚；用外部行锁与 InnoDB 锁等待视图确定两个 writer 同时阻塞
  在首个 `SELECT ... FOR UPDATE`，真实 MySQL 8.4 验证恰一成功一余额不足。无 DSN 仍明确 SKIP，
  不再把 SKIP 宣称为真实事务通过。
- 两服务 `go test -count=1 ./...` 与 `go vet ./...` 已通过；未 commit/push、未碰生产、未写真实 secret。

## 2026-07-12:Player / Inventory / Auction 业务阻断收束（待 Claude Code 最终复审）

- 人明确授权 Codex 修复本批业务阻断。player 属性扣点改为事务内宽类型总和/重复属性/列上界校验；
  inventory 新增严格快照校验且可并发幂等补冻的 `EnsureAuctionEscrow`。两者真实 MySQL 提交、回滚、
  溢出和并发用例均实际执行通过。
- auction 撮合与幂等权威收敛到 MySQL：owner coordinator registry 返回唯一 canonical order，跨片扫描/
  补行移出 coordinator 事务；持久 shard topology 门禁阻断片数、顺序和目标库漂移。当前明确仅支持
  单库或 2 片，`N>2` 重分片仍禁止。
- 成交改为 MySQL Reserve→inventory 幂等 Settle→持久 release/event outbox；资产与 Kafka worker
  分离，ready escrow 释放前置，事件另以双方 `release_pending` 为持久屏障；弱 audit 走有界非阻塞
  队列。`passive_warmup` 保证 green 与旧 matcher 共存时不写、不补偿。
- `000002` 以单次 `event_pending DEFAULT 1` 覆盖迁移中旧写并受控重放历史成交，无表锁/无界 DML；
  MySQL 8.4 fresh/old schema 全量一致，首跑/复跑均 `version=2 dirty=false`。
- player、inventory、auction 的完整 test/vet 与真实 MySQL 用例全绿。`CGO_ENABLED=0`，race、真 Kafka/
  inventory gRPC 故障注入、生产规模 MDL/吞吐及真实蓝绿仍属外部验收门。Claude 复审清单见
  `docs/design/auction-blockers-claude-review-20260712-final.md`；未 commit/push、未碰生产、未写真实 secret。

## 2026-07-13:DSTicket v2 与不停服轮换/普通发布防线收束（生产激活仍阻断）

- 人明确授权 Codex 修复本轮本地审核问题。Login 已严格签发/校验 RS256 `dst_ver=2` 票据，绑定
  DS UID/epoch、assignment、release track 等权威声明并限制最大 TTL 180s；四 signer 私钥改为
  revisioned immutable Secret、非 root UID/GID/fsGroup 10001 与 0440，只向 Login/DS 投递对应公钥 JWKS。
- 新增独立 `stage -> promote -> retire` 轮换流程：K1/K2 overlap、四 signer 逐个 rollout、全部 Fleet/
  GameServer/Pod 与历史 controller 引用闭包、apiserver activation 时间起算 225s 后退役 K1；旧 key/
  config 不自动删除，也不强杀 GameServer。普通 online 发布与轮换共用 create-only immutable 操作锁，
  以 UID/resourceVersion CAS 释放；崩溃残留锁只允许人工审计，不按本机时间抢锁。
- 轮换/发布门禁已覆盖 direct/projected/env/envFrom/init/ephemeral/CSI、畸形保留前缀、影子 config/
  signer/JWKS consumer、YAML 真实反序列化路径、fixed/phase Secret 全量 data 投影与 marker 历史配对；
  activation/terminal/fixed handoff 的远端写入前均紧邻复证精确运行态和锁身份。
- Social Guild 的 TiDB counter/schema migration 已保持新旧实例混跑兼容并补并发/自愈用例；Envoy 对
  七个 Inventory internal RPC 已精确 403。Login/Social/迁移工具及 `pkg/auth`/`dsticketkeys` 的
  test/vet/build、7 组 PowerShell 契约、38 脚本 AST、两套 Kustomize render 与 `git diff --check`
  均通过；离线业务镜像包已按 host 模式重建并复查为未过期、精确 20 tags。
- **仍禁止宣称可生产激活**：真集群/UE K1→K2→K2-only（含 K1 旧票耗尽）E2E 未完成；本轮
  post-redaction K1 尚未到达认证入口，K2 未执行，也未写真实 secret、apply 生产、commit/push。
  `pkg/auth`、`services/account/login`、`services/social/guild` 的离线 linux/amd64 `-race` 已通过；Guild
  `internal/data` 12 个真实 MySQL 集成/并发场景与 `tools/migrate` 2 个 Social v2 场景已复跑通过，临时库
  清零。Inventory 的 Istio 方案 A 已获人批准，但仅交付独立静态候选，不能冒充已完成服务间鉴权或生产激活。

## 2026-07-13:Battle Model-B 正常结算持久终态回收（方案 B）

- `battle_result` 将结算、玩家更新与完整 terminal-release proof 放入同一 MySQL 事务；DB commit
  失败不返回 OK，commit 后 Redis receipt 即时失败仍由 durable outbox 恢复。旧 credential 已提交后，
  新 active credential 的幂等重试不替换旧 proof、不增加第二行。
- worker 落地两阶段状态机：pending 先以 exact proof 建永久 Redis terminal/receipt 墓碑并执行
  Kubernetes UID precondition release，再 CAS `released_at_ms`；released 行只走
  `completed-finalize` 恢复墓碑 TTL，绝不再删 K8s，最后仅以 `released_at_ms>0` 删除 outbox。
  response loss、DB mark/delete failure、跨完整 TTL 与两个 worker 并发 CAS/finalize 均有回归测试。
- additive migration 与启动 schema gate 精确核验全部列/索引、`ENGINE=InnoDB`、
  `utf8mb4_0900_ai_ci`、`released_at_ms NOT NULL DEFAULT 0`；mutant 和隔离 MySQL 8.4 集成测试通过。
  Redis authority 生产配置拒绝无凭据 `pandora.battle.result`，match-id-only Model-B release 拒绝；
  online 内部 ReleaseBattle 另以 battle-result SPIFFE/mTLS exact method policy 收口，不暴露到 `:8444`。
- 本轮 battle_result/ds_allocator 全量 Go test/vet、并发用例 50 次、mesh/config 契约均通过；未
  commit/push、未 Apply 生产、未写真实 secret。上线前仍须先执行 migration、接入 online component，
  并通过 §7.15 blue/green/真实集群 synthetic 激活门。

## 2026-07-13:Inventory 服务身份方案 A 获批并交付独立静态候选（普通 online 未接线）

- 人已明确批准 `docs/design/decision-revisit-internal-service-auth.md` 方案 A：revision 化 Istio
  STRICT mTLS + SPIFFE + exact AuthorizationPolicy 为统一最终身份层，Inventory 是第一条落地链。
  本轮未安装 Istio、未执行 Kubernetes apply、未写真实 secret、未碰生产。
- 独立静态候选包含六个 ServiceAccount、纯 revision sidecar、Inventory `grpc/appProtocol`、
  9 个系统 allow / 26 个 system deny 补集、edge 6 个玩家 allow / 7 个 system deny 补集、STRICT 与
  observe 分层策略、Inventory 专用 L4 NetworkPolicy。内部六 workload 与外部 edge 均以
  Deployment→ReplicaSet→Pod VAP 锁 owner、受保护 SA、token/capability、sidecar 与流量截获；生产最低
  Kubernetes 1.30。candidate 安全对象由测试按审核 hash 精确比对。共享静态 component 唯一拥有
  battle-result SA；Inventory 与 DS-terminal 双候选组合只引入一次，DS-terminal 自行声明两端 revision
  patch，不依赖 Inventory identity component。
- 只读 helper/契约可覆盖完整 live Pod labels、protected SA 全量占用（含 terminating）、
  canonical green `battle-result-ds-auth-green`、edge owner/容器/image、创建时 RS labels、唯一 Istio
  injector、actual revision、MeshConfig canonical 表示、root namespace additive policy、VAP controller
  收敛、STRICT、Service 与 NetworkPolicy；但这些 helper 未接入 ordinary `start.ps1`。
- 默认 online kustomization 不引用 shared identity/Inventory/DS-terminal component，普通 online NetworkPolicy 未改，
  `start.ps1` 不加载 Inventory helper、不接收 Istio revision、不生成 runtime patch，也不执行 Inventory
  preflight。静态候选不能由普通发布误 apply；未来接线必须重新审核。
- 可伪造/重放的 `pandora.inventory-mesh-audit/v1` 已永久 hard-fail。真实 Kubernetes 尚未产生短时
  `observeEvidence` 与 active ALLOW 新 generation/RV 后的 `activeAllowEvidence`，也未逐 proxy 证明 xDS
  传播。首次激活必须完成 PERMISSIVE→identity→gate→observe→active ALLOW→STRICT 独立阶段，不能把
  单个 `workflows_ok` 布尔当验收。ordinary online 当前的全局零写门来自独立 DSTicket K1→K2→K2-only
  真实 Kubernetes/UE E2E 边界，不代表 Inventory 已接线。
- 本地通过 `inventory_mesh_contract_test.ps1`（含 wildcard、额外 policy、sidecar/token/旁加载、
  MeshConfig flow/缩进/merge、candidate broad-policy mutants）、`ds_terminal_mesh_contract_test.ps1`、
  `online_manifest_contract_test.ps1` 的 B 收口断言、Kustomize render 与 PowerShell AST；`pandora-agones`
  API server 对 7 个 VAP + 7 个 Binding 的 server dry-run/CEL 编译全部通过。`pkg/auth`、Login、Guild 的
  离线 linux/amd64 `-race`，以及 Guild 12 个真实 MySQL 集成/并发场景、Social v2 2 个迁移场景已通过；
  post-redaction K1 尚未到达认证入口，K2 未执行。
  真实 Kubernetes/外部 edge/metrics/probe/CNI/五条补偿链滚动验收尚未执行，不宣称生产可激活。

## 2026-07-14:battle 混合模式退役(Windows DS 只保留 local 断点调试)

- 决策(`docs/design/decision-revisit-retire-battle-mode.md`,推翻 2026-07-01
  `decision-revisit-battle-services-in-docker.md`):Windows DS 只在 `start.ps1 -Mode local`
  (断点调试)下由宿主 exec 启动;其他一切要真 DS 的场景一律 k8s + Agones(Linux DS,
  `-Mode k8s` / 内网服务器一键启动-k8s集群.cmd)。`docker`/`intranet` 维持 DS=mock 不变。
- 已删双击入口:`策划一键启动-含战斗.cmd`、`内网服务器一键启动-含战斗.cmd`。两个停止 .cmd
  (`策划一键停止.cmd`、`内网服务器一键停止.cmd`)保留,仅用于清理旧机器遗留的 battle 栈。
- `start.ps1 -Mode battle` 启动/Resume 一律拒绝并指路 k8s(exit 1);`-Down`/`-Status` 保留
  (清理/查看遗留环境),`-Reset` 只清理不再重启。`play.ps1 -Battle` 启动同样拒绝,仅
  `-Battle -Stop`/`-Battle -Status` 可用;play.ps1 删除 battle 专属函数
  (Resolve-LanIp 副本/Get-LocalDsExePath/Confirm-HubDsUp/Resolve-LocalDsExe/
  Ensure-GoInstalled/Test-BattlePrerequisites)。`gen_cluster_config.ps1 -HostAllocators`
  参数暂留(已无调用方,下次触碰该脚本时移除)。
- 文档同步:README、`docs/ops/planner-quickstart.md`、`deploy/offline-images/README.md`、
  `tools/scripts/README.md`、export/import_images.ps1 提示语均已改口径;旧决策文档头部已标
  「已被推翻(2026-07-14)」。
- 验证:两个 ps1 通过 AST 解析零错误;`start.ps1 -Mode battle` 与 `play.ps1 -Battle` 冒烟
  确认拒绝且 exit 1;`play.ps1 -Battle -Status` 透传正常。未 commit/push(Codex 收尾)。

## 2026-07-14:任意时点断线/切后台 DS 迁移复核(未闭环)

- 复核范围覆盖 Login/SelectRole/Hub travel+Admission、排队/确认/分配/READY、Battle 首次握手、局内
  断线、杀进程重登与结算回 Hub。已确认 Battle-aware Login、B1 locator fail-closed、Hub
  reservation/session ledger、matchmaker liveness/claim healing、allocator/outbox 和 locator fence 等
  服务端恢复骨架存在；这些机制不能等同于“任意时间点绝不卡死”。
- 发现安全阻断：`IssueDSTicket(hub)` 直接 `ResolveHubEndpoint → AssignHub`,绕过 Login 的
  locator/B1/LOGIN_PENDING 门。客户端掉线而 Battle DS 健康时,roster 会继续续租 BATTLE；当前客户端
  30s 超时却直接申请 Hub 票。Hub admission 又未核 placement,可出现玩家已进 Hub、locator 仍为 BATTLE
  的双归属/后续匹配异常。
- 发现客户端阻断：READY 在无 LocalPlayerController 时先写去重态并停轮询,后续同地址不再连接；
  杀进程后 Login 直连 Battle 未恢复 MatchModel 的 match_id/Hub 上下文,结算回 Hub 可直接失败；
  `ReturnToHubDs` 在请求前清匹配态,失败后重试会丢 fence。前后台/HTTP 黑洞、真实 UDP Admission、
  杀进程全矩阵仍无 UE 自动化 E2E。
- 发现匹配持久恢复阻断：StartMatch 三步写的失败回滚复用玩家请求 ctx,可留未入队 body+claim；最后一人
  Confirm 后的 DS 分配/READY/locator/push 仍同步绑在该玩家 RPC ctx；allocator error 未先 CAS FAILED,
  expire scanner 遇瞬态锁错误却移除 active。上述窗口会让 QUEUEING/ALLOCATING 状态脱离恢复索引直到
  30min TTL。结算后的 match claim 释放又是 best-effort,DB 已提交而释放失败时幂等重报不会重试清理,
  玩家回 Hub 后仍可能被 AlreadyMatching 挡住。
- B1 只覆盖 locator 查询报错的 fail-closed；若 DS→locator best-effort 刷新连续失败到 key 正常过期,
  Login 会把“未找到 BATTLE”当成可进 Hub,不会用 live roster 作第二证明。签 Hub 票与 Hub Admission 仍需
  统一的权威 placement/terminal 最终门。
- 验证：account login、player locator、matchmaker、hub allocator、ds allocator、battle result 六个相关
  Go module 的现有测试均通过；本轮另复跑相关 biz/data/service 目标包也全绿。现有测试未覆盖上述
  ctx-cancel、active-index、release-response-loss 与 Hub/Battle 双归属反例,所以不能据此改判为已闭环。
- 已纠正 `battle-reconnect.md` §6/§7 的旧绝对结论,补“不卡死”定义、阻断反例和必跑矩阵；同步更新
  `ds-arch.md` §9.2 与 `decision-revisit-ds-callback-auth.md` §7.16.3。当前结论明确为**代码尚未全部完成**；
  本轮仅做审计与文档收口,未修改服务端/客户端业务代码,未 commit/push。

## 2026-07-15：任意时点 DS 恢复代码收口（本地全绿，真环境仍阻断发布）

- 服务端已以版本化 placement + immutable source-departure proof 形成 Hub/Battle 唯一准入门：
  UNKNOWN 始终 retryable 且零 seat/ticket/spawn，Hub/Battle Admission 必须在 spawn/READY 前提交 exact
  version/operation/target/source 证明。Login `ResumeContext` 覆盖 Hub、排队、确认、分配、Ready 和对局。
- Matchmaker 已把 formation/allocation/release 改为可重放持久 saga；StartMatch 在任何副作用前与
  Agones 外调前都检查 STABLE_HUB。allocation 新增 exact `REQUESTING→ABORTING→FAILED`，
  payload-bound 独立 HMAC 与持久 abort journal；UNKNOWN/ACK loss 不抢先 FAILED/requeue。BattleResult
  release outbox、compare-delete claim/ticket 与 active index reconciler 已收口。
- Model-B 回收必须同时持有真实 GameServer UID + Pod UID；新分配在 usecase/finalize 双硬门
  拒绝空 PodUID。正常结算、empty/stale、abort、preactive 的旧记录都在 terminal fence 前做
  exact K8s 回填。pre-Prepare `instance_epoch=0` 改为专用 fenced release，unknown 保留永久 fence，
  成功才 purge，不伪造玩家 teardown proof。late abort 同时要求 teardown 与 Kafka ACK 后 lifecycle
  两份 full-target proof。
- 发布流程新增只读 legacy PodUID 三阶段证明：`prepare` 在排空前审计存量，`drained` 在 blue writer=0 +
  capability empty + drain marker 后审计零写窗口，`final` 在 green exact capability/strict writer 已启动但
  Service 尚未切换前再次审计。三份 Job 绑定同一 RunId、immutable image/config、Redis identity/topology；
  final PASS 前不切 Service、不 CAS epoch，epoch=2 审计禁止事后创建证据。临时只读 Redis ACL 身份在
  CAS 后必须由独立控制身份精确 `DELUSER` 并回读 absent，cleanup pending 不算发布完成。
- Redis `BattleStorageRecord` 的 23 个生产写点（含独立 quarantine 命令）统一进入不可逆 strict Model-B
  mutation gate：新/重写记录必须带完整 PodUID，legacy 只允许 exact PodUID backfill，未知 protobuf bytes
  保留且 PodUID 不可变。etcd target 值固定为 `2@ds-auth-v2-pod-uid-write-invariant-v1`，五个 writer 的
  exact feature policy 同时绑定初读、capability 注册事务、watch 与 activation record；旧 numeric epoch=2
  binary 和新 binary 遇裸 `2` 都 fail-closed，关闭 feature-only rollback 窗口。
- UE 已以 `UMyDsRecoveryCoordinator` 作唯一 DS `ClientTravel` writer，generation/request-seq/phase 拒绝
  迟到回调；30s 只改 UI/退避。Battle DS 的 Controller/Pawn/World/weak-ledger census 与 exact ABA eviction
  已落地，无法归因的物理对象 fail-closed。
- 最终本地验证：`go.work` 29/29 module test + vet；Proto lint 与两次生成 diff 确定；
  activation/cluster 合同、PowerShell AST、services/online kustomize、`git diff --check` 通过；
  PandoraEditor 725/725、PandoraServer 577/577；UE DsRecovery 5/5、DSAuth 11/11。race 因 CGO=0/无 gcc
  未跑；online manifest live API 因本机 kubeconfig `127.0.0.1:59751` 不可达而 BLOCKED-ENV。
- 仍不允许宣称生产已证明“任意时点绝不卡死”：仓库尚未接入能覆盖整个 preflight→CAS 窗口的真实
  Redis topology-change/failover/reshard lease provider 与信任根。这不是本机环境偶发失败，而是外部
  控制面能力尚未接线；fresh/retry Activate、Go CLI/core CAS 与普通 online release 当前都在任何
  create/patch/scale/build/push/apply 前 fail-closed。产线 placement/PodUID preflight 以及真 Redis
  Cluster/K8s/Agones/UDP/移动端前后台故障矩阵也尚未执行。如果旧空 PodUID record 的 K8s 不可变证据
  已丢失，代码只会保留明确 retryable fence，发布前需可审计迁移/清退，禁止猜 UID。

## 2026-07-15：Hub successor policy V3 发布交接

- successor lease 新 writer 由库自动写 `supported_policy_generation/id=V3`，并把实际初读 required 写入
  `acquired_policy_generation/id`；V3 activation 对五类 writer 的 compiled support、V2 staging acquire、
  exact features/Pod UID/digest 与 Hub count=1 全部 fail-closed。V2→V3 是同 writer epoch 的 policy-only
  CAS；fresh local 使用 missing→V3 zero-writer 单事务 genesis，Resume 必须验证同事务 immutable record。
- 平台必须通过强制 HTTPS+mTLS+auth 的 `prepare_hub_successor_policy.ps1` 完成 V2 staging：真实 apply 前
  server-side dry-run 并逐个验证五个合并后 Deployment 的 identity/template/image/selector/count，强制
  locator preflight 与 Hub `replicas=1 + Recreate`，再 create-only 写 exact immutable
  `pandora-ds-auth-policy-v3-evidence`。prepare/activate 都会在 Endpoint 0/1 门前验证 Hub Service 的 exact
  green selector/ClusterIP/20021，避免 selector 漂移伪造零 Endpoint；既有 marker 只能精确回读，不能覆盖。
- `activate_hub_successor_policy.ps1` 已支持崩溃续跑：required=V2 才执行 pre-CAS 门/CAS；required=V3
  从 record-only proof 继续。post-CAS 从固定五个 canonical Deployment/selector/owner chain 重新派生当前
  live UID（不把历史 marker UID 当永久 allowlist），要求唯一业务 container Running+imageID、所有 capability
  acquired=V3、Hub ready Endpoint 精确 1 个且 UID 唯一匹配；最后再次 capability audit 并写 immutable
  completion marker。required=V3 的只读 Audit 与普通 online release 也必须验证该 completion marker 并把其
  UID/resourceVersion 纳入窗口内不可漂移基线，缺失不得假绿。完整契约见 `docs/design/battle-reconnect.md` §7.8。

## 2026-07-16：no-freeze 需求固化 + 双仓库复核 + 前台恢复缺陷修复

- 需求固化：「进 Hub DS / 匹配进 Battle DS 任意阶段切后台或断网，回来不卡死且能正确回到唯一权威
  DS」升级为需求级不变量，写入 `CLAUDE.md §9.19` 与 `battle-reconnect.md §7.11`；违反直接拒 PR。
- 复核范围：服务端 2026-07-14 起全部提交（8ab6c59→1c21311）+ 未提交 §7.10 修复；UE 客户端
  r1090–r1130（luhailong）+ 未提交 Coordinator/OnlineSession/超时兜底工作树。服务端
  login / player_locator / ds_allocator / hub_allocator / matchmaker 与 `pkg/placement`、
  `pkg/battleabort` 全部 `go build + go test` 绿（§7.10 `exactSamePreparedBattlePending` 修复含
  正/负例）。客户端 unary 超时钳制 5–300s、完成回调恰好一次、Coordinator 各 phase 均有驱动源，
  OnlineSession exact-driver 清理复核通过。
- 修复一处 UE 前台恢复缺陷：前台事件原来无条件重启权威重查，从未登录（登录页/离线本地流程）或
  显式登出/放弃重连后会经 `RenewSessionForRecovery` 无凭据分支强制打开登录关卡，把玩家从当前界面
  踢走。新增 `ShouldRestartRecoveryOnForeground` 纯判定门（session/重连中/缓存凭据/ReturnHub fence
  全空则跳过），`ReturnToLogin`/`AbandonBattleReconnect` 显式清 session+缓存凭据；新增 Automation 用例
  `DsRecovery.ForegroundRestartRequiresRecoverableContext`。PandoraServer Win64 编译通过（Pandora
  runtime module 含全部修复）；PandoraEditor 目标因本机编辑器 Live Coding 占用未能本轮编译，
  PandoraTests 新用例待编辑器空闲后随 Editor 构建验证。真实移动端前后台/断网矩阵仍是发布前验收项。

## 2026-07-17：脑裂根治落地 — DS 授权租约 fencing + 服务端再入屏障

- 定义并落地「一人两 DS」的标准最简根治（lease-shorter-than-failure-detector fencing，
  完整契约见 `battle-reconnect.md §8`，常量集中 `pkg/placement`）：
  ① DS 短租约自我 fencing：连续 20s 拿不到绑定 active 凭据的权威心跳响应 → DS 除拒新玩家外，
  Kick 全部存量已准入玩家、销毁玩家 Pawn、拒 pending 准入（UE `PandoraDSBackendSubsystem`
  1s watchdog + `OnAuthorityLeaseLost`，`PandoraDSGameModeBase::FenceAllAdmittedPlayersForAuthorityLoss`，
  Hub/Battle 共用）；② 服务端再入屏障：`abandoned` 只有在 `last_heartbeat + 25s` 后才算 Terminal
  （login `InspectBattleRoute`，四个 Hub 再入门全部继承）；locator TTL 与 hub `heartbeat_timeout`
  机械下限 ≥ 25s。`ended`（DS 自报正常终局）不经屏障，正常结算零延迟。
- 迟到响应防护：DS 心跳 RPC 有界超时 4s（迟到响应不得刷新租约锚点）；心跳间隔钳 [1,5]s；
  UE Automation 硬断言不等式 `20(租约)+1(检测)+4(在途) ≤ 25(屏障)`。
- 测试：服务端 login/hub_allocator/player_locator/ds_allocator/matchmaker/pkg 全 `go test` 绿
  （屏障四态正负例、两处下限地板、locator TTL 地板修正既有用例）；UE 新增
  `Pandora.Auth.DSTicketV2.AuthorityFencePolicy`，编译与真机分区注入由用户执行。
- 边界如实记录：CLAUDE.md §9.22 的每玩家 `owner_epoch` 线性一致 owner authority 仍是文档化
  目标；当前由 session JTI 围栏 + DSTicket v2 exact 实例绑定 + locator match fence 提供迟到写
  防护。可达传输层的秒级转移重叠窗口依赖 eviction order 送达，列为已知边界与发布前验收项。

## 2026-07-18：全量双规则审计 + 三项已知边界闭环（面向上线加固，battle-reconnect.md §8.6）

- 全量 inline 审计两条需求级规则（任意时点不卡死+安全重连；严格单会话/根除脑裂）：四个再入门、
  两个机械地板、DS 侧 fencing/GameMode Kick、PostLogin 同机顶号、会话 JTI 单写者、matchmaker
  入队门（BattleGateFailOpen 默认 fail-closed）、ShrinkHubTTL Lua 守卫逐一读码核实；五服务 +
  全 pkg `go test -count=1` 全绿。多 agent workflow 因订阅 session limit 全灭，结论均来自 inline 取证。
- 边界 1 闭环——时钟漂移零预留：`DSFenceSkewMarginSeconds` 5→7（屏障 25s→27s），显式留出
  ≥2s 服务间时钟漂移预算；UE 镜像常量 `ServerFenceReentryBarrierSeconds=27` /
  `InterServiceClockSkewReserveSeconds=2`，Automation 断言升级为 `20+1+4+2 ≤ 27`；
  屏障边界测试改 27s 并新增「25s(旧屏障值)仍须 UNKNOWN」防回退负例。代价：abandoned 再入 +2s。
- 边界 2 闭环——SelectRole 会话现行性：免 proto 字段，从 Envoy jwt_authn 验签后重写的
  `x-pandora-jwt-payload`（入站无条件剥离，不可伪造）提取 jti，`RequireCurrentSessionJTI` 与
  IssueDSTicket 同门判定；jti 缺失时 B1 严格档 fail-closed。`pkg/middleware` 纯函数解析 +
  login biz 七态表驱动测试。顶号后旧设备四条拿票路径（Login/Resume/IssueDSTicket/SelectRole）
  现全部封死。
- 边界 3 闭环——挂起恢复首帧积压输入（§9.22）：`FWorldDelegates::OnWorldTickStart` 先于
  NetDriver TickDispatch（引擎 LevelTick.cpp 顺序），DS 订阅后每帧在分发积压包前复查租约；
  1s watchdog 保留兜底，共用 edge-trigger。
- 文档：battle-reconnect.md §8 全量改 27s，新增 §8.6 加固记录与 §8.5 分区注入验收执行清单
  （NetworkPolicy 步骤 + T0 时序断言 + 顶号矩阵）。go-services.md §2.6 同步。
- 交接：UE 编译 + `Pandora.Auth.DSTicketV2.AuthorityFencePolicy` 由用户执行（Live Coding 约束）；
  分区注入清单为发布前必跑验收，代码级不能替代。git/svn 提交由用户复核后执行。

## 2026-07-20：队伍邀请恢复（Kafka 启动门禁 + push/team 协议同版滚动）

- 现场根因闭环：旧 team 在 Kafka 尚未就绪时 producer 初始化失败后仍以 `pusher=nil` Ready，后续
  Invite RPC 虽返回 OK 但通知永久静默丢弃；同时运行中的 team/push 仍为 `4193897`，落后于已发布
  的 `event_type=1 + TeamInviteEvent` 客户端协议，单独重启旧 team 也无法恢复新客户端邀请 UI。
- 代码修复：配置了 `kafka.brokers` 时，team producer 改为启动强依赖，失败在 gRPC Ready 前退出，
  交给编排器重试；只有显式空 brokers 才进入有醒目标志的纯 RPC dev-only 模式。补齐空配置、构造
  失败、nil producer、成功装配/事件类型透传测试；team Deployment 显式 `maxUnavailable=0`、
  `maxSurge=1`，保证失败的新 Pod 不替换仍可服务的旧 Pod。push 补 header 缺失为 0、header=1 原样
  透传两条回归断言，协议与发布文档同步为严格 `push reader → team dual writer → 新客户端` 顺序。
- 验证：team、push、`pkg` 各自 `go test -count=1 ./...` 全绿，K8s manifest server dry-run 通过；
  不可达 Kafka 的隔离进程以 exit 1 退出，出现 `kafka_producer_required_but_unavailable`、无
  `service_ready`、全程 0 个 TCP listener。仅重建/替换 push 与 team，运行版本均为
  `6aff5dd-dirty`，两 Pod imageID 与 minikube 新镜像一致，其余 18 个业务 Pod UID 未变化。
- 真链路验收：被邀请方先建立 Push Subscribe，随后 CreateTeam/Invite 经 team → Kafka → push 实际
  收到同一 topic 的 `eventType=1` 专属帧和 `eventType=0` legacy 帧；AcceptInvite 与 GetTeam 均确认
  双成员，测试队伍随后解散且两玩家 `GetMyTeam.hasTeamMsg=false`。滚动导致失效的宿主 20010/20014
  port-forward 已替换并分别反射到新 `TeamInviteEvent` / `PushFrame.event_type`。
- 已知边界如实保留：本次根治的是“启动时 producer 失败后永久 nil”和协议版本错配；运行期间
  Kafka send 重试耗尽仍只有错误日志。若要把邀请承诺为端到端 durable delivery，仍需 outbox 或
  被邀请方可查询的权威邀请列表，不能用本次启动门禁代替。

## 2026-07-20:组队匹配 READY 通知闭环(matchmaker Kafka 启动门禁 + READY 补推 + UE 等待 watchdog)

- 现场根因闭环:matchmaker-pve 启动时 Kafka 未就绪,producer 一次性初始化失败后以 `pusher=nil`
  继续服务,整个 Pod 生命周期 `pandora.match.progress` 静默丢弃(现场 4 个 partition end offset
  均为 0)。组队匹配只有队长持有 StartMatch 返回的 match_id 可轮询兜底,非队长成员唯一通道就是
  该推送 → 队长进 Battle、队员永远停在 Hub(match 14537609598533632)。
- 修复一(启动门禁,与 2026-07-20 team 同口径):配置 `kafka.brokers` 时 matchmaker producer 为
  启动强依赖,初始化失败在对外 Ready 前 exit;显式空 brokers 保留 dev 纯轮询模式。新增
  `initializeMatchPublication` + cmd 层 4 条测试。
- 修复二(READY 推送 at-least-once):新机械不变量「READY ∈ active ZSET ⟺ READY 推送交付
  未确认」。READY CAS 后推送改 `pushReadyStrict`(聚合错误),全员成功才 RemoveActive;滞留
  READY 由撮合循环 `finalizeReadyMatch` 幂等补推(全员重签新 jti,与 refreshBattleTicket 同
  口径),覆盖「READY 提交后、推送前崩溃」与「推送时 Kafka 不可用」两个窗口,上限 match TTL。
  `expireOnce`(keepActive)与 `reconcileActiveOnce`(不清不建)同步改语义。回归:
  `ready_push_saga_test.go` 两条(推送失败保留 active + 重启后补推带个人票据;过期扫描不误清)。
  matchmaker 全包 `go test` 全绿。
- 修复三(UE 侧最后闭环,Pandora-Client-SVN):`UMyMatchModel` 新增组队匹配等待 watchdog——
  订阅 `UMyTeamModel::OnTeamSnapshotChanged`,本队 `TEAM_STATE_MATCHING` 且本地无匹配归属期间
  以 `TeamMatchStandbyCheckIntervalSeconds`(默认 5s)循环检查,到期仍无归属且 Coordinator 空闲
  (Phase==Idle,不抢流不换代)则触发 `RestartAuthoritativeRecovery(Resume)`——与「收到无归属
  推送」同一条权威恢复路径,由 ResumeContext 决定 QUEUED/CONFIRM/READY 或明确 Hub。World 切换
  后 OnWorldBeginPlay 幂等重挂 ticker(§9.19 有界驱动)。切后台场景本就由 Coordinator 前台
  `RestartAuthoritativeRecovery(Resume)` 覆盖,未改动。
- 场景矩阵:启动窗口 Kafka 未就绪 → 门禁拒 Ready(编排器重试);运行中 Kafka 宕机 → READY 滞留
  active 持续补推,恢复即达,UE watchdog 兜底前台等待;matchmaker 崩溃/换 leader → durable saga
  重放 + 补推;客户端切后台 → 前台恢复权威重查;推送链路全灭 → watchdog 周期 ResumeContext。
- 交接:UE 编译与真机验证由用户执行(Live Coding 约束);建议复验事故时序(先起 matchmaker 后起
  Kafka → CrashLoop 至就绪;双人组队 → 两端都收到 READY 并进 Battle;匹配中 kill matchmaker-pve
  Pod → 重启后队员仍能进场)。git/svn 提交由用户复核后执行。文档:go-services.md §2.8 新增
  READY at-least-once 不变量条目。
- 追加(同日,用户质询「Kafka 恢复后人已在别的副本,补推岂不是有问题」触发的审查):
  ①服务端侧确认无害——ReleaseMatch 前成员被 claim + locator BATTLE 门锁死进不了别局,
  ReleaseMatch 后 match 记录删除、补推循环即停,不存在"跨局迟到补推";②客户端侧查出真缺口:
  `UMyDsRecoveryCoordinator::TryDriveTravel` 缺 §23 要求的 Battle 幂等 no-op 守卫(Hub 有
  CanReuseCurrentHubAdmission,Battle 没有),战斗内收到重复/迟到 READY(补推使之常态化)会
  对同一 DS 重复 ClientTravel 把玩家拽出重载地图——先于本次改动即存在,补推放大。已补:
  Battle 目标且当前 live connection 端点精确一致时不再 Travel,转入与 World BeginPlay 后验
  同款的 post-travel 权威复核(bPostTravelAuthorityCheck + RetryBackoff + ScheduleAuthorityRetry),
  由 ResumeContext 按 route+match 确认 admission 收口;漂移则照常权威重查。验收补充:战斗内
  手动重发 READY 推送(或制造部分成员推送失败触发补推)→ 在局玩家无地图重载,日志出现
  "already connected to target battle endpoint"。

## 2026-07-20 ~ 07-21 实时成长入账通道(玩家经验 + 掉落即时到账,Claude)

- 需求拍板:击杀怪物/完成任务**即时**加经验(Lv15 封顶 MAX,连升多级),金品质+掉落同队广播;
  DS 崩溃**已入账部分保住**;§0.6 红线不删。设计:`docs/design/realtime-progression.md`(已拍板);
  契约修订已合入 `ds-arch.md` §0.5 ③ / §0.6;决策已登记 `pandora-arch.md` §11。
- proto(`[proto]`,已本地 buf lint + go/cpp 双生成,cpp pb 已拷入 UE 仓库):
  `battle.proto` 新增 `ReportProgress`(BattleProgressEvent oneof MonsterKill/ItemPickup,
  seq 幂等)+ `ReportResultRequest.final_progress_seq`;`player.proto` 新增 `AddExperience`
  (系统 RPC)、`PlayerPushEventType`(0=旧 MMR 事件,1=EXPERIENCE)、`PlayerExperienceEvent`、
  `PlayerProfile.exp_in_level/is_max_level`(取自 stats 预留段 12/13,reserved 收窄为 14-49)。
- player 服务:`players.exp` 列 + `exp_history`(uk player_id+idempotency_key)+
  `player_push_outbox`(与入账同事务);`AddExperience` 幂等入账 + 等级曲线结算
  (`AdvanceExperience` 纯函数:连升多级/升满清零/满级 no-op 不消费幂等键不出箱);
  推送出箱发布器 `RunPushOutboxPublisher`(SendRawWithEventType,kafka header 路由);
  `exp_curve` 配置与客户端 `j_玩家等级经验.xlsx` 同源(dev/prod 样例已填 1000..11400 占位曲线,
  空=功能关闭);MMR 消费者按 event_type header 跳过非 0 事件(防经验事件进 DLQ,有回归测试)。
- battle_result 服务:`ReportProgress`(复用 ReportResult 的 Guard+Redis active 鉴权,
  roster 越权拒)→ 校验/上限(单批 256/单场 seq 10 万/单事实 count 上限)→ 怪物经验表换算 +
  掉落白名单过滤(未知怪/非白名单跳过告警,水位照常推进,不卡流)→ `battle_progress_stream`
  水位乐观 CAS 与 `battle_progress_outbox` 同事务;出箱 worker 幂等调 player.AddExperience /
  inventory.GrantInstances(幂等键 progress:{match}:{seq}:{player}:{kind},背包满转邮件同 drop);
  SaveResult 事务内打终局标记(僵尸 DS fencing:结算后进度一律 ERR_INVALID_STATE)+
  水位>0 抑制结算路径掉落发放(单一权威路径防双发,不信 DS 声明)+ final_progress_seq 对账
  (缺口只告警,§9 尾窗残余风险);killswitch `progress_disabled`。ABANDONED 不回滚已入账。
- SQL:mysql-init 04/05 更新 + `tools/migrate` pandora_player 000002_experience、
  pandora_battle 000005_battle_progress(纯 additive,不停服)。Envoy:AddExperience 加入
  player.v1 403 精确拦截清单(与 UpdateMMR/Grant* 同双保险)。
- 验证:player/battle_result `go build + go test` 全绿(新增等级曲线 10 例、AddExperience 6 例、
  consumer 路由 2 例、progress 校验矩阵/聚合/重放/结算拒收/防双发/发布器 8 例);全 go.work
  模块构建通过(顺手修了 matchmaker 上会话遗留的 buildProgress 调用点漏传 m.MapId 编译错)。
  无新增依赖,无需 go mod tidy。
- UE 侧(Pandora-Client-SVN)由并行会话按同一设计实施中(已见:PandoraBattleProgressReporter、
  Loot 掉落模块/品质色阶(1白..5金6红)、battle/player wire+codec+ReportBattleProgress 传输层、
  GameMode 接线进行中)。**交接/待验收清单**:①怪物击杀事实(MonsterKillFact)上报接线与
  battle_result `monster_exp` 表(dev yaml 目前空表,需按怪物配置 ID 填值);②客户端经验适配
  (player.update event_type=1 → SetExperienceDisplay/PlayLevelUpPresentation、登录/重连
  GetProfile 刷新、player codec 的 GetProfile/PlayerExperienceEvent 解码)当前尚未见落地;
  ③`j_玩家等级经验.xlsx` 与怪物经验列进服务端导表管线(当前以服务 yaml 为权威配置);
  ④UE 编译/联调由用户执行;⑤验收矩阵见 realtime-progression.md §8(幂等/连升/封顶/故障/
  崩溃保住/防双发/断线补推/广播可见域/killswitch)。

## 2026-07-21:关卡内规则功能补齐(UE 侧,基于既有 Drop/实时进度系统扩展)

- 盘点结论(对照策划三图,除场景缩略图):主流程/大厅五件套/交易行/组队匹配/结算服务端均已上线;
  怪物掉落+拾取入包+实时入账(CfgDrop/AMyDropItemActor/PandoraBattleProgressReporter)已由前序
  工作树实现。本轮补齐其余关卡内规则,全部为 UE 仓库改动,后端零改动:
  ①拾取分配规则强化:AMyDropItemActor 新增专属归属 OwnerPlayerId(0=公共先到先得,>0 仅归属者
  可拾)、死亡玩家不可拾取、bPersistOnPickup(false=拾取只入战斗背包不入账)、品质查询与
  BP 外观钩子;ItemConfigId/Count 开放 EditAnywhere,关卡直摆=初始场景物品。
  ②玩家死亡掉落:APandoraBattleGameMode::SpawnPlayerDeathDrops,撤离制副本死亡时战斗背包
  (人物背包,装备可配)整包散布为 persist=false 公共掉落——原持有者拾取时已即时入账,再分配
  拾取只入战斗背包,防后端双发;PVP 保持原行为。开关在 UMyLootSetting(DefaultMyGameSetting.ini)。
  ③宝箱开锁拾取规则:AMyLootChestActor(站桩开锁 UnlockSeconds/离开重置/开锁权继承/掉落表
  独立 roll/可配开锁者专属/RespawnSeconds 重刷),状态+进度复制,蓝图挂外观。
  ④撤离点显示和撤离规则:AMyExtractionPointActor(bAlwaysRelevant 全程可见,可配延迟开放,
  进圈蓄力 ExtractSeconds、离圈/死亡重置,多人并行蓄力),蓄力满走
  APandoraBattleGameMode::HandlePlayerExtracted(可信身份+防重复+终局快照冻结前受理)。
  ⑤终局规则策略化:UPandoraBattleSettlementRule 新增 SupportsExtraction/EvaluateTerminal;
  PVP 行为不变(任一死亡立即结算);PVE 撤离制=全员死亡或撤离才结算,任一人撤离成功=通关(0),
  全灭=未通关(1);单人 PVE 语义与旧行为兼容。GameMode 维护 Dead/Extracted 终态集合。
  ⑥公屏信息广播规则(realtime-progression §7 掉落广播):拾取品质≥门槛(默认金=5,可配)时
  NotifyItemPickedUp 内经可信 ActivePlayers 解析拾取者,向同阵营在场玩家控制器逐个
  ClientReceivePublicBroadcast;客户端本地解析道具名/品质色后送 UMyMainView::EnqueuePublicBroadcast。
  ⑦副本场景道具使用规则(食物/水):FCfgItem 新增「使用回血量 UseHealHp」;
  UMyBagComponent::ServerUseBagItem 服务端权威校验(可使用+回血>0+存活)→四元组精确扣 1
  →ASC ApplyModToAttribute 回血(PreAttributeChange 钳制);抽光格子即时清空防幽灵条目。
  ⑧装备穿戴规则:FCfgItem 新增「装备部位 EquipSlot」;ServerEquipItem(背包→装备栏,同部位
  自动替换,替换后背包无位整体回滚,强制堆叠上限=1 保 guid/动态属性跟随)/ServerUnequipItem
  (背包满则失败保持穿戴)。装备栏全员复制,查看他人出装天然支持。
  ⑨入场随机选择出生点:PandoraDSGameModeBase 新增 bRandomizeSpawnPointSelection
  (Battle 构造置 true,Hub 保持 SlotIndex 确定性),同筛选条件空闲候选内随机,占用互斥不变。
  显式配置恒优先于随机(用户 2026-07-21 纠正):玩家绑定点(PlayerId>0)永不随机;候选集中
  任一点填了显式槽位(SlotIndex!=0,如 PVE 副本入口固定队形)即整组回退 SlotIndex 确定性顺序,
  只有全部候选未填槽位时才随机。
- 小修:UMyBagComponent::GetMaxStackSize 在 UCfgSystem 缺失(无表测试环境)时回退
  SetDefaultMaxStackSize 的默认上限(此前该字段写而不读);具体道具配置缺失仍 fail-closed 0。
- 明确未做(有既有拍板/另行立项):PVE 匹配补人与 AI 填充(decision-dungeon-entry-modes §5
  拍板当前不做);NPC 对话客户端 UI(dialogue 服务端已上线);可破坏物=「无行为树怪物实体+
  CfgDrop 配掉落」内容配置路径,无需新代码;鉴定/背包/交易行等大厅系统服务端已上线且
  inventory/player/battle_result `go test ./...` 本轮复跑全绿。
- 交接(用户/编辑器侧):UE 编译+PIE 验证(Live Coding 约束,AI 不代跑);掉落物/宝箱/撤离点
  蓝图子类外观与关卡摆放;CfgItem 表新增「使用回血量/装备部位」两列导表;DefaultMyGameSetting.ini
  可选配置 UMyLootSetting(默认值即可跑通)。git/svn 提交由用户复核后执行。

## 2026-07-21:配置表热更流水线落地(旧项目读表移植 + §9.15 标准化,Claude)

- 背景:把 D:\luyuan\mmorpg 的 Go 读表(go/shared/generated/table 单例 TableManager 模式)移植到本仓库,
  并按 config-table-hotreload.md §0 标准流水线加固。旧实现三处不达标,移植时全部修正:
  ①每表 `m.snap = snap` 普通指针赋值(热更下数据竞争)→ 全批快照 + `atomic.Pointer` 一次切换;
  ②`LoadTables` 失败 `log.Fatalf` 杀进程 → 返回 error,失败保留旧批次;
  ③逐表独立加载(批内跨表版本可能撕裂)→ manifest 驱动整批 all-or-nothing。
- 新增(全链 `go build`/`go test` 绿):
  - `proto/pandora/config/v1`:`level.proto`(LevelRow/LevelTableData,对齐 g_关卡.xlsx)+
    `configtable.proto`(ConfigTableAdminService.ReloadConfigTable,幂等/失败保留旧表);
    errcode 增 `ERR_MATCH_INVALID_MAP=4008`(proto 与 pkg/errcode 同步)。[proto]
  - `pkg/configtable`:manifest(version+sha256+rows)校验、protojson 加载(运行时 DiscardUnknown,
    滚动窗口容忍新字段)、version 单调防回退、expect_version、未知新表跳过告警、脏文件告警、
    LevelTable 只读视图(ByID/IsBattleLevel);测试覆盖 9 类失败路径「失败保留旧表」+ 并发读切换。
  - `tools/configtable-gen`(独立 module,已入 go.work):读 Pandora-Client-SVN/Table 源表
    (中文表头版式:1 列名/2-4 注释/5+ 数据),§7 严格校验(表头精确对齐/主键唯一/枚举越界/
    布尔 0-1/路径前缀),protojson 确定性序列化(Compact→Indent 消除 protojson 随机空白),
    生成回读严格校验;version 自动单调(YYYYMMDD*1000+seq),同内容幂等不写盘。
    xlsx 解析用 stdlib 自实现最小读取器(zip+xml,fail-closed),不引第三方、无需 Python。
  - `configtable/dist`:首批产物 v20260721001(level 7 行,git 跟踪)。
  - matchmaker 接线:`config_table.dir` 开关(空=不启用,现行为不变;非空=启动强依赖 fail-closed,
    并校验兜底 `match.map_id` 必须战斗类关卡);StartMatch 增关卡表准入门——客户端 map_id
    (含 0→默认兜底后)必须存在于关卡表且 category=战斗,否则 `ERR_MATCH_INVALID_MAP`
    (此前任意 map_id 可一路透传成 DS `PANDORA_MAP_ID`);同 gRPC 端口挂 ReloadConfigTable
    (内部接口,callerID!=0 一律拒;信任模型同 ReleaseMatch)。热更后新批次对后续 StartMatch 立即生效,
    已入队票据在 ticket TTL 内自然流完不回溯。`pkg/config.Base` 增 `config_table` 段(全服务可复用)。
  - `tools/scripts/configtable_publish.ps1`:dist→staging→sha256 校验→版本单调→旧 active 归档
    history/v<ver>→改名切 active(+可选 grpcurl 触发 reload);已实测发布/no-op/升级归档/回退拒绝。
- 决策记录:「匹配列表显示」列不做服务端准入(是客户端 UI 展示位,dev/GM 直进测试关卡需放行),
  服务端只卡「存在 + category=战斗」;生成器与 pkg/configtable 不互相 import,以 §5 manifest JSON
  为共同契约。产物 JSON 口径:proto 原名 snake_case + 枚举数字 + 零值省略。
- 剩余(待排期,不阻塞现网):etcd 版本键 watch 多机统一刷新(§6 方式 1,单机 reload RPC 已够);
  其余表(道具/技能/Buff…)按「proto 容器 + tablegen 表规格 + specByName 注册 + Tables 加字段」四步扩;
  本机无 gcc,`-race` 未跑(读路径 atomic.Pointer + 不可变快照,竞态面已设计消除)。
- 交接:①`go.work` 新增 `use ./tools/configtable-gen`(如需 `go work sync`/tidy 由 Codex 收尾;
  该 module 仅依赖 proto+protobuf,本机 build/test 已绿);②proto 有改动已重生 pb,UE 侧无需同步
  (纯服务端协议);③dev 启用方式见 matchmaker-dev.yaml `config_table` 注释段。

## 2026-07-21(续):Go 表访问代码改为生成(移植旧项目表代码模板,Claude)

- 补齐读表移植缺口:旧项目 mmorpg 的「每表一份生成 Go 代码」能力(go_config.go.j2 /
  go_all_table.go.j2)移入 `tools/configtable-gen/internal/gogen`(text/template + go/format)。
  旧生成物 48 个 `*_table.go` 不直接拷(表集与 pb 包都是旧项目的),生成能力对 Pandora 表规格重放。
- 结构:`pkg/configtable/<name>_table.gen.go`(视图 + All/ByID/Exists/Count/ByIDs/RandOne/Where/First,
  即旧 TableManager API 去单例化)+ `tables.gen.go`(Tables 快照结构 + specByName 注册,替代旧
  all_table.go 的 LoadTables);表私有校验/域方法留手写伴生文件(level.go:validateLevelRow +
  IsBattleLevel)。生成/手写边界:gen 文件头 DO NOT EDIT,钩子由生成代码显式调用。
- 表规格单一事实源收拢到 `internal/tablegen/registry.go` `Sources()`(数据产物与代码生成共用);
  main 增 `-go-out`(默认 pkg/configtable,空=跳过),内容不变不写盘。
- 守护:`gogen.TestGeneratedFilesUpToDate`(规格改了不重跑生成器 / 手改 gen 文件 → 测试红)、
  确定性测试、生成 API 与钩子接线测试。pkg/configtable 既有全部测试 + matchmaker 全量回归绿
  (API 兼容,消费方零改动)。
- 未移植并记录原因:comp(ECS 组件结构,Go 侧无消费方)、fk(外键 helper,现有表无外键列)、
  multi-key 复合主键(无此类表);出现真实需求再加(§15.3)。加新表五步见 hotreload doc §10。

## 2026-07-21(续2):生成器定稿为 protogen 式(proto 注解驱动,零手写登记,Claude)

- 按用户指认的旧项目工具 D:\luyuan\mmorpg\tools\proto_generator\protogen 重构 configtable-gen:
  描述符驱动 + 独立模板文件 + proto 自定义 option,替换上一版的手写表规格登记(registry.go
  Sources() 的 GoName/RowType/KeyGetter 等字段与 BuildLevelTable 手写列映射全部删除)。[proto]
- proto 即单一事实源:新增 `proto/pandora/config/v1/excel.proto` 自定义 option
  ((excel_file)/(excel_col)/(excel_required)/(excel_default)/(excel_prefix),扩展号 51501-51505
  内部保留区);level.proto 全字段打注解。生成器 import configpb 后经 protoregistry 遍历
  pandora.config.v1 自动发现表(discover.go:容器命名 <Name>TableData、rows 字段、id uint32 主键
  三项约定 fail-closed 校验),通用 protoreflect 行构建器(builder.go)按注解执行 §7 校验;
  与原手写构建器产出字节级一致(dist version 未变即证)。
- gogen 模板迁独立文件 internal/gogen/template/*.tmpl(go:embed;table/tables/companion 三模板,
  对应旧 go_config.go.j2 / go_all_table.go.j2 / protogen instance 模式);伴生文件 <name>.go
  缺失时生成一次空 validate 钩子桩,此后归人维护不覆盖。
- 测试:builder_test(发现 + 14 个校验用例含枚举拒 0)、gogen 三测试(形状/最新性守护/确定性,
  伴生文件存在性纳入守护)、manifest/pkg/matchmaker 全量回归绿;生成器幂等复跑零写盘。
- 加新表三步(hotreload doc §10 已更新):proto 注解 → proto_gen.ps1 → configtable-gen。

## 2026-07-21(续3):补齐 fk / multi-key / bitindex 三类生成能力(Claude)

- 按用户点名移植旧导表的最后三类列能力,全部走 excel.proto 注解(protogen 式):[proto]
  - `(excel_key)` 唯一二级键(旧 `key`)→ By<Field> 单行查询,生成+加载双阶段查重;
  - `(excel_multi_key)` 非唯一索引(旧 `multi`)→ ListBy<Field>;
  - `(excel_fk)`(旧 `fk:Table`,限 uint32 引用目标表 id)→ 生成阶段批内引用完整性
    (tablegen.ValidateFKs,失败整批不产出)+ 加载阶段生成 validateCrossTables(store.go
    整批切换前 fail-closed 兜底)+ 正查 Tables.<Src><Field>Row[ByID] + 反查 ListBy<Field>;
    必填外键 0 非法,非必填 0=无引用;fk:Table.column 与 gfk 无用例未移植;
  - `(excel_bit_index)`(容器注解,旧 bit_index)→ <name>_bitindex.gen.go 稳定 ID→位序映射;
    状态文件 configtable/bitindex_state/<name>.json 为权威(git 跟踪,新 ID 追加、删 ID 保位
    永不复用,丢失=已落库位图错位作废);关卡表已启用(ID 1-7→位 0-6),供解锁/进度位图。
- 端到端测试夹具:proto/pandora/configtest/v1(场景+副本表,对齐 dungeon-scene 分层决策形状;
  独立包生产 Discover 扫不到,角色=旧项目 Test/TestMultiKey.xlsx)。覆盖:注解发现/互斥校验、
  唯一键重复、FK 通过/悬空/必填 0/目标缺席、位序稳定性(删行保位/同集合零变更/回环)、
  夹具代码渲染断言;TestGeneratedFilesUpToDate 增 bitindex 产物与状态/dist 一致性守护。
- 全量回归绿(configtable-gen 三包 + pkg/configtable + matchmaker);生成器幂等复跑零写盘。

## 2026-07-21(续4):金丝雀共存窗口「旧副本回写丢新字段」审计(Claude)

- 问题:金丝雀期间同一玩家请求在新旧副本间跳,新副本写入的新字段可能被旧副本
  read-modify-write 回写静默清掉(用户提出,按存储类别全仓审计)。
- 结论:MySQL 结构化列(只 SET 认识的列,全仓无 REPLACE INTO)、data_service
  (update_mask 掩码写 + 缓存写入方字段位图超集判定)、Redis blob RMW(team/hub 默认
  Unmarshal + 原地改回写)均安全;pkg/configtable 的 DiscardUnknown 为只读路径合规。
- 发现 1 处潜在陷阱:reward.go saveRewardRecord「重建式回写」RewardClaimStorageRecord
  (当前仅 permanent/activity 两字段无实际丢失;给该 message 加新字段前必须先改为
  保留 stored message 原地改回写,否则金丝雀窗口丢新字段)。
- 规则升级:zero-downtime-update.md §2.3 增补「禁止重建式回写」硬规则,新增 §7
  审计记录(问题定义 / 分类结论表 / 陷阱处方 / 数据兼容≠路由粘性)。

## 2026-07-21(续5):修复领奖记录重建式回写 + 全仓落盘点排查(Claude)

- 修复 reward.go(§7.3 陷阱):loadRewardRecord 保留 stored message,saveRewardRecord 原地
  覆盖 permanent/activity 后 Marshal(stored),unknown fields 随回写原样带回;新增回归测试
  TestClaimReward_PreservesUnknownFields(存量记录挂 field 15 raw varint,领奖后断言未知
  字段保留 + 位图正确;修复前失败)。player 服务 go build + 全量 go test 绿。
- 全仓 proto.Marshal 落盘点分类排查:除 reward.go 外无第二处重建式回写。
  team/trade UpdateWithLock 原地改;ds_allocator/hub_allocator/matchmaker 全线
  proto.Clone+原地改;hub capacity ledger 整表重写 marshal 的是 load 反序列化原对象;
  mail 内容建一次不改;guild/data_service 缓存带字段位图投毒防护(PGC\x01/PDC\x02);
  friend/chat/dialogue/auction/leaderboard/player_locator/inventory 无 proto blob 落盘。
- zero-downtime-update.md §7.3 改为已修复(含测试说明),新增 §7.4 全仓排查结论表,
  原"数据兼容≠路由粘性"顺延为 §7.5。

## 2026-07-21(续6):mail 服务增长有界(默认 TTL + 收件箱上限 + sweep 清理)(Claude)

- 问题:邮件库只增不减——过期邮件仅读时过滤从不删;个人邮件 expire_ms=0 永不过期;
  player_mail_claim 只 INSERT 不 DELETE(领取状态写扩散,长期最大增长点);无收件箱上限。
- 三层修复(docs/design/mail.md §2.4):①个人邮件默认 TTL 30 天(default_personal_ttl_days);
  ②InsertPersonalMail 事务内 COUNT(*) FOR UPDATE 原子校验收件箱上限 200(§9 不变量 18,
  满时驱逐最旧已领邮件,仍满 ERR_MAIL_BOX_FULL=9605,battle_result 出箱补扫重试自愈);
  ③biz/sweep.go 周期清理 worker(5m/轮、每表单批 500,多副本无锁幂等对齐 leaderboard 补扫):
  过期个人邮件缓冲 7 天后带未领附件的先归档 player_mail_archive(保留 90 天)再删,
  已领/无附件直删;sys/guild 邮件失效后删;claim 表按雪花 mail_id cutoff 范围删(180 天,
  新增 snowflake.MinIDAt)。
- 表结构:player_mail+idx_expire、sys/guild_mail+idx_end、player_mail_claim+idx_mail、
  新增 player_mail_archive(12-mail-tables.sql 原地修订,存量库需手动 ALTER)。
- 顺带修复:SetPersonalStatus 现在同步置 claimed 列(原来永远 0,客户端视图 Claimed 恒 false)。
- 测试:TTL 默认/显式、归档分流(含坏 payload 保守归档)、各表 cutoff、MinIDAt 区间;
  mail + snowflake 全部 go build/test/vet 绿。errcode.proto 加 ERR_MAIL_BOX_FULL 待 Codex 重生 pb。

## 2026-07-21(续7):mail 表改动改走版本化迁移 000003(Claude)

- 修正续6"存量库需手动 ALTER":发现 tools/migrate 已管 pandora_social,新增
  000003_mail_growth_bounded(4 个清理索引 information_schema 守卫幂等 + player_mail_archive
  建表;down 按惯例 additive-only no-op,回滚保留归档数据)。
- social_migration_test 契约同步:latest version 2→3、baseline immutable 断言扩到邮件结构、
  v3 up 契约片段、down 断言抽 assertAdditiveOnlyDown(新增禁 DROP INDEX)、集成测试
  schema_migrations 版本改跟 latestMigrationVersion、fresh 形态预建 v3 结构验幂等、
  迁移后断言归档表 + 4 索引就位。workspace 模式 go test/vet 绿。
- 既有阻断(非本次引入):tools/migrate 在 GOWORK=off -mod=readonly 下编译失败,
  go.sum 缺 go.uber.org/atomic 条目;修复(go mod tidy)按 AGENTS.md §11.1 交 Codex,
  修好前 README 的 docker 镜像构建路径走不通。

## 2026-07-21(续8):MailAttachment 重构为 oneof 形态(Claude)

- 背景:附件旧结构 config_id+count+as_instance bool,bool 本质是伪装的类型判别器,
  再加附件种类(货币/已存在实例转移等)会组合爆炸;开发期是最后的免费重构窗口
  (上线后字段编号冻结、oneof 迁移不再 wire 兼容,邮件 blob 长期存活)。
- mail.proto:MailAttachment 改 oneof body{ stack=StackAttachment | instance=InstanceAttachment },
  两分支均 config_id+count。stack=无唯一 ID 可堆叠(GrantItems);instance=有唯一 ID 物品/装备,
  领取时逐件铸实例(GrantInstances,实例雪花 uint64 ID 领取时生成,铸出未鉴定、词条鉴定时掷)。
  将来新形态(货币、按 instance_id 托管转移已存在实例)加新分支新编号。
  顺带修正旧注释误导:GrantInstances 并不"含随机词条"。
- 未识别形态契约(§9.21 滚更共存):发送侧 buildPayload 校验 body 必设 + config/count 非零,
  拒空 body 入库;领取侧 partitionAttachments 计 unknown,>0 整封 fail-closed 报
  ERR_MAIL_ATTACHMENT_UNSUPPORTED(=9606,errcode.proto+pkg/errcode 同步新增),
  不发放任何附件、不记 claim,邮件保持可领,禁止静默跳过。
- 跟改:mail biz partition/expand 按 oneof、mail inventory_client Grant 只认 stack(混入报错)、
  battle_result mail_client 溢出附件拼 instance 形态;instance_grant_key 幂等语义不变。
- 测试:既有 5 个领取/源键用例迁到新构造;新增 fail-closed 领取(不发已识别部分、不记 claim)、
  发送侧拒空 body/零值、battle_result 分组纯函数(含"绝不拼成 stack")。
- ⚠️ 待办:mail pb(go/cpp)+ errcode pb 需 Codex 重生后才能编译验证(本次改动含
  生成代码新 API,重生前 go build 红);UE 侧仅生成 pb 无手写引用,同步后无需改客户端代码;
  dev 库 pandora_social 存量邮件 blob 旧编码不兼容,需清空
  sys_mail/guild_mail/player_mail/player_mail_cursor/player_mail_claim/player_mail_archive。

## 2026-07-21(续9):掉落零丢失——拾取 ACK 门控(MMO 化拍板,Claude)

- 背景:产品形态 MOBA→MMO,"局中已得掉落绝对不丢"升级为硬需求;原 realtime-progression.md
  §9"尾窗丢失明示接受"(DS 崩溃丢 ≤1s 未发缓冲)作废。DS 本地 WAL 方案评估后否决:
  Agones 临时 Pod 本地盘在 Pod 替换/节点宕机时消失,加 PV+回捞成本高且仍非零丢失。
- 方案(用户拍板"拾取 ACK 门控"):UE 侧把"捡到"反转为入包=已持久化——拾取权威点先认领
  锁定掉落物(他人不可拾、暂停 LifeSpan),事实上报 ReportProgress,服务端水位+出箱同事务
  提交回 acked_seq 覆盖整组后才入战斗背包并销毁;确定未应用(未发送即丢弃/整批拒/停流)
  释放认领回地面。新不变量:战斗背包 ⊆ 后端已入账,DS 任意时刻崩溃不丢已见入包物品。
- Go 侧:零功能改动(ReportProgress ACK 本就是持久化确认);仅修订 realtime-progression.md
  (拍板记录、§1 约束④措辞、§3 门控协议、§5 对账语义降级为审计、§9 残余风险改写)。
- UE 侧(Pandora-Client,待用户编译验证):
  - PandoraBattleProgressReporter:RecordItemPickup 加认领回执委托+返回末 seq、入队即发、
    认领组不拆批、按 acked_seq 前缀提交/整批拒收释放、停流释放全部认领、
    缓冲满丢最老未发认领组并释放;
  - AMyDropItemActor:门控路径(CanAddItem 预检→认领→回执入包/释放)+回退路径保持旧行为
    (通道停流/无 player_id/死亡再分配 persist=false);
  - APandoraBattleGameMode:IsPickupAckGatingActive/BeginGatedPickup/FinishGatedPickup;
  - UMyBagComponent::CanAddItem 只读空间预检(权威端专用,委托 FMyBag::CheckSpaceFor)。
- 防复制关键:已发送未确认的认领只能经回执终结(停流释放后重拾走回退路径不再产生持久化
  事实,与服务端水位抑制协同=恰好一次发放,任一侧成功归原认领玩家)。
- ⚠️ 残余边界(§9 已明示):seq/单场累计上限触顶停流后超限部分不入账(反作弊封顶,
  配置须远高于合法单场产出);认领玩家掉线/背包满时发放不回滚仅损局内表现。
- (续9 补充)经济模式开关:UPandoraBattleSettlementRule::AreDropsPersistent()(默认 true),
  MOBA 类玩法 override false ⇒ 掉落随局清零、拾取即入包零后端交互,门控/上报整体旁路;
  掉落物与战斗背包始终在 DS,门控只是"跨局持久"经济语义的实现,玩法回摆无需改拾取链。

## 2026-07-21(续10):背包预留制 + §9.6 信任模型改写(Claude)

- 预留制(用户拍板"预检→预留",消 TOCTOU):FMyBag 新增 ReserveSpaceFor/CommitReservation/
  ReleaseReservation + 活跃预留计入 CheckSpaceFor 与 AddItem 容量门(仅有预留时启用,
  无预留行为与原规划器等价);Commit 先摘除自身预留再入包(合并判定保证必成,失败仅可能
  来自配置热更破坏,错误如实传播);ClearAllItems 只清物品不清预留(死亡散布不动容量承诺)。
  UMyBagComponent 加权威端包装(Commit 同步复制快照);AMyDropItemActor 门控路径改
  预留→认领→回执转正/释放。回归测试 PandoraTests/MyBagReservationTest.cpp(挡直写/
  释放归还/恰好一次/堆叠共享池/ClearAllItems 保留预留)。全部待用户 UE 编译验证。
- CLAUDE.md §9.6 正式改写:"MMR 计算在 battle_result(DS 不可信)" → "派生数值一律服务端
  计算;DS 的写权限有范围、可验证、有额度"。数值仍不信 DS;DS 作受信写者须满足五要件:
  身份(DS JWT/writer epoch)+ owner 授权(按 owner 权威校验"该 DS 持有该玩家",禁只验
  合法 DS)+ fencing(owner_epoch 失租拒写)+ 额度(journal 层速率/单场上限)+ 审计(journal
  流水)。原则内核(限制爆炸半径)不变;为 MMO 化"背包权威跟随 owner DS"铺路。

## 2026-07-21(续11):数据库增长有界 —— inventory 保留期清理 + §9.24 新不变量(Claude)

- 背景:用户问"玩家已没有的道具在 MySQL 还存在吗,怕库越来越大"。审计结论:
  player_items 扣到 0 保留行但被 uk 有界(玩家数×配置数,故意不清,清了错误码语义漂移);
  player_item_instance 丢弃是硬 DELETE 无残留;真正无界的是 inventory_ledger(每笔操作
  1~2 行永不删)与 auction_escrow closed 行(每挂单 1 行永不删)。
- inventory 落地(对齐 mail sweep 模式,多副本无锁幂等,单批 LIMIT 防长事务):
  - data:DeleteLedgerBefore / DeleteClosedEscrowBefore(closed 且超期才删,active 永不清);
  - biz/sweep.go SweepRetention + main ticker 接线;conf 新增 sweep_interval(5m)/
    sweep_batch(500)/ledger_retention_days(90)/escrow_retention_days(90),yaml 同步;
  - schema:inventory_ledger 加 idx_created、auction_escrow 加 idx_status_updated
    (既有库需手动 ALTER,SQL 文件内附语句);EnsureAuctionEscrow 注释同步。
- 跨服务闭环(隐蔽 bug 修复):mail claim 180 天清理原依赖"inventory 幂等键永久兜底"防
  重复领奖;ledger 限 90 天后该兜底失效 → mail defaultEnd 把 sys/guild 邮件 end_ms 钳到
  「创建时刻 + claim_retention_days」内(钳后 end<=start 拒收),保证 claim 行存活 ≥ 可领
  窗口,防重不再依赖任何永久流水;sweep/conf 注释同步改写。
- 规范:CLAUDE.md §9 新增不变量 24(只增表必须有保留期+清理任务,失效数据默认且最多
  90 天,幂等行删后重放须 fail-closed,只增表登记清单,mail claim 180 天为登记例外);
  AGENTS.md §10 红线追加同条。
- 验证:build/vet/单测绿;集成测试(随机临时库夹具)在本机 TiDB 4000 全过
  (LedgerDeletesOnlyExpired / LedgerBatchLimitBounded / EscrowDeletesOnlyClosedExpired +
  既有 EnsureAuctionEscrow 套件);真 MySQL 8.4 环境未跑(本机无 MySQL 容器,剩余风险低:
  语句均为标准 DELETE..LIMIT/DATE_SUB)。

## 2026-07-21(续11):背包域设计文档定稿(Claude)

- 新增 docs/design/bag-domain.md:独立背包域(pandora.bag.v1)蓝图——三域定界(battle 事实/
  背包 journal/经济 escrow,货币留经济域)、权威跟随 owner(五要件+flush-before-fence)、
  journal/checkpoint 分层(判据=效果是否被本人之外观察到;恢复=快照+尾部重放)、存储模型
  (bag_meta/bag_checkpoint/bag_journal/bag_generation,pb blob+行式流水,90 天保留)、
  背包类型×策略矩阵(bag_type uint32,0-3 对齐 UE,100+ 活动段)、邮件中转层、拍卖在线扣+
  邮件到账、bag.v1 契约草案(LoadBag/AppendJournal/SaveCheckpoint,前缀确认语义同
  ReportProgress)、四阶段迁移路径、失败模式验证矩阵、复杂度举证。
- **活动背包代际设计(本次新需求:类型重用+活动结束清空)**:段身份=(player,bag_type,
  generation);切代=读过滤瞬时逻辑清空+generation fencing 拒迟到旧代写(fail-closed)+
  后台 sweep 物理回收;salvage_mode 可配 discard/mail 补发;类型重用天然安全。
- 决策已登记 pandora-arch §11。**契约草案暂不落 .proto 文件**(journal fencing 字段依赖
  owner authority 最终形态,§9.22 未建成;phase 1 开工时落文件+Codex proto_gen,避免二次
  返工)。phase 1 硬前置 = owner authority 落地。
- (续11 修正,用户拍板)驻留分层:只有随身组(身上背包/装备栏/临时格)checkout 进 owner DS
  内存权威;仓库与临时活动背包**后端驻留**(存储侧权威,DS 只发起操作+只读视图)。收益:
  checkout/flush 面收窄、活动切代不涉及 DS 状态(拒绝旧代写后 DS 仅刷视图)、仓库⇄身上
  转移=一条 journal 存储侧同事务改两侧(无跨服务 saga,随身侧走预留制)。bag-domain.md
  §0/§2/§3/§4/§5/§6/§9/§11 已同步修订(新增 bag_section 表 + GetSections RPC)。
- (续11 补充)后端驻留段物品的使用语义(bag-domain.md §5.1):①持久产出型=扣除+产出同一
  存储事务(零窗口,产出进身上背包走预留/或邮件);②局内瞬时效果型=先扣后生效,崩溃时
  效果与局内状态同命回档自洽,贵重道具做激活型归①;③高频消耗型=先 journal 装填到随身组
  再按 DS 速度消耗,后端驻留段保持 UI 频率定位。

## 2026-07-21(续12):背包域 phase 1 服务端落码(Claude)

- 契约:proto/pandora/bag/v1/bag.proto(LoadBag/AppendJournal/SaveCheckpoint/GetSections;
  BagStorageRecord/BagSection/BagItem;journal oneof = pickup_grant/mail_claim/transfer/consume;
  前缀确认 acked_seq 语义同 ReportProgress);errcode.proto 新增 bag 段 14001-14009
  (EPOCH_FENCED/GENERATION_MISMATCH/SEQ_CONFLICT/CAPACITY_FULL/QUOTA_EXCEEDED/
  IDEMPOTENCY_CONFLICT/ITEM_NOT_FOUND/CHECKPOINT_STALE/SECTION_NOT_ALLOWED)+ pkg/errcode 常量。
- 存储:01-create-databases.sql 加 pandora_bag 库;新 14-bag-tables.sql 五表
  (bag_meta fencing 锚点/bag_checkpoint/bag_section/bag_journal 双唯一键+指纹/bag_generation)。
- 实现(inventory 进程承载,bag.dsn 空=不启用):data/bag_repo.go(bag_meta FOR UPDATE 锁 =
  每玩家写串行化 + owner_epoch 单调 CAS;活动段代际 fail-closed;journal 前缀确认+纯重放安全+
  幂等指纹;后端驻留段与 journal 同事务读改写;滑窗额度;90 天 sweep)、data/bag_apply.go
  (op 应用纯函数:随身组只记账不落 section,仓库/活动段真实入扣,consume 扣+产同事务,
  未知 op 整批拒)、biz/bag.go(形状校验/段类型合法性/批量与单 op 上限)、service/bag.go
  (拒客户端 JWT)、server 注册、main 装配(schema gate fail-fast + sweep 协程)、
  conf.BagConf(默认 batch 64/items 64/时额度 2000/仓库容量 200)、inventory-dev.yaml bag 块、
  Envoy /pandora.bag.v1/ 前缀 403。
- 测试:data/bag_apply_test.go(随身组 no-op/仓库领取容量堆叠/转移/开箱扣产/代际/未配置段,
  失败场景独立 store 模拟事务回滚)、data/bag_repo_mysql_test.go(DSN 门控随机库:重放安全/
  整批回滚/epoch fence 读写/切代读过滤+迟到写拒+类型重用/幂等冲突/checkpoint 单调/额度)、
  biz/bag_test.go(校验层假仓)。
- ⚠️ 交接阻断项:
  1. **Codex proto_gen 重生 pb(bag.proto 新包 + errcode.proto),重生前 go build 红**
     (先例同 mail oneof 重构);cpp pb 同步 UE 仓库(errcode 增量 additive,UE 无 bag 引用);
  2. 既有 MySQL volume 需手动重放 01(建 pandora_bag)+ 14(建表),启动 schema gate 会
     fail-fast 提示;3. MySQL 集成测试需设 PANDORA_TEST_MYSQL_DSN 跑一轮;
  4. phase 2(DS 写路径接入)前置 = owner authority(§9.22),未落地前 BagService 仅供
     联调/工具调用,不接 DS 生产写。

## 2026-07-21(续12):全库只增表审计 + 保留期清理全量落地(§9.24 收口,Claude)

- 背景:续11 之后用户问"其他表呢"。全量审计 13 个 mysql-init 建表文件 + 各服务 data 层,
  逐表分类:出箱表投递即删(有界)、per-player/config 表有界、其余只增表逐个接清理。
- 新增清理(8 服务,全部对齐 mail sweep 模式:多副本无锁幂等、单批 LIMIT、失败不阻断):
  - battle_result:battles+battle_player_stats(ended_at_ms 超 90 天,同事务批删,异常
    ended=0 行按 created_at 兜底)、battle_progress_stream+player(仅 settled 行,未结算
    陈年行=补偿链 bug 证据永不清);match_id 幂等键删除安全性:重放在凭据层(Guard/
    active match/roster)就被拒。
  - player:RunHistoryJanitor 清 mmr_history / attr_point_grants / talent_point_grants
    (90 天,下限 30;**默认关**,与 exp_history 同理由:上游 kafka 重放/授予补扫须先有界,
    dev yaml 开启)。
  - chat:chat_private_messages 按雪花 message_id cutoff 主键范围删(90 天,无需新索引)。
  - friend/guild:终态申请行(status≠pending 且 updated_at 超 90 天)批删;pending 永不清;
    行按 (requester,target)/(guild,player) uk 复用,删后再发起=全新请求,行为等价。
  - auction:逐分片(DBRouter.All())清终态订单(且 release/match_pending=0)、已结算成交
    (settlement=COMPLETED 且 event_pending=0)、超期 idempotency_keys(与 orders 异分片
    无法 join,按 created_at_ms 独立清)。
  - leaderboard:snapshot + reward_log(仅 GRANTED;PENDING/FAILED 是补发工作集)90 天;
    **settlement 行故意不清**(settle uk 永久闸:删了会让超期重放当新结算重复发奖,保留则
    already+空快照回放,fail-safe)。
  - login:account_devices 按 last_login_at 90 天(client 可刷 device_id 的只增行兜底有界,
    下次登录 upsert 重建);account_bans 登记豁免(运营合规审计)。
- schema:7 张表新增清理索引(mysql-init 原地 + tidb-init 同步);存量库走 tools/migrate
  新增 7 库 *_retention_indexes 迁移(幂等条件建索引,ALGORITHM=INPLACE,down 条件删)。
- 规范:CLAUDE.md §9.24 登记表补全 17 类只增表 + 豁免清单(settlement/owner_guards/bans/
  出箱表/player_items 0 行)。
- 验证:8 服务 build/vet/单测全绿;集成测试(本机 TiDB 4000):battle retention(自建随机
  库重放 03+05 schema,批删+未结算保留断言)、inventory/guild 全套全过。已知非回归失败:
  auction ConcurrentGlobalIdempotency 在 TiDB 上原版 schema 同样失败(auction 设计只跑
  MySQL 分库,历史审核在真 MySQL 过);battle terminal_release 集成测须 DSN 直指已迁移库
  (测试前置,与本次无关)。真 MySQL 8.4 未跑(本机无容器),剩余风险低(标准 DELETE..LIMIT)。

## 2026-07-22(续):owner authority 权威本体落码(Claude)

- 设计:docs/design/owner-authority.md(§9.22 落地蓝图):宿主=新独立 owner 服务(runtime 域,
  20017/21017,infra.md 已登记);存储=生产 TiDB(线性一致+确认写不回滚)/dev 单机 MySQL,
  三表同库单事务域;租约分层=实例级租约(allocator 心跳代写)派生玩家 owner lease,
  续租 QPS 钉在实例粒度;fence 常量单一来源 pkg/placement(20/7/27s 不动)。
- 契约:proto/pandora/owner/v1/owner.proto(Query/BeginTransition/Admit/RenewInstanceLease/
  ReleaseOwner;OwnerRecord 含派生 lease_deadline;BARRIER_NOT_OPEN 带 retry_after_ms 对应
  §9.23 WAIT);errcode 15000-15005 + pkg/errcode 常量。
- 存储:mysql-init/01 加 pandora_owner 库;15-owner-tables.sql(owner_record/ds_instance_lease/
  owner_transition_log);tidb-init/02-owner-tidb.sql(NONCLUSTERED+SHARD_ROW_ID_BITS 打散
  雪花热点,AUTO_RANDOM 审计主键,utf8mb4_bin,悲观锁写法)。
- 实现:services/runtime/owner(独立 module,已入 go.work):data 层状态机(owner_record 行锁=
  每玩家串行化锚点;epoch 单调 CAS;admit_not_before=CAS 时点 FOR UPDATE 观察旧实例租约
  最晚截止+margin,后续续租不回写;Begin/Admit 幂等重放;Release 迟到 no-op;租约只前进+
  实例纪元守卫;审计 90 天 sweep)、biz(UUIDv4/身份完整性/lease 硬钳 ≤ 协议上限)、
  service(拒客户端 JWT)、grpc/http server、main(schema gate + sweep)、owner-dev.yaml;
  Envoy /pandora.owner.v1/ 前缀 403。
- 测试:**biz 单测已真跑绿**(margin 来自 placement 常量/校验不触数据层/lease 钳制);
  data MySQL 集成测试(DSN 门控)覆盖:首迁移无屏障+Begin/Admit 幂等重放、并发双迁移
  恰好一胜一冲突、屏障≥旧租约+余量、早到 Admit 拒(带 retry_after)、Begin 后旧实例续租
  不回写屏障、旧 epoch/换实例 UID Admit 拒、迟到 Release no-op、租约单调+纪元守卫。
- ⚠️ 交接:1. Codex proto_gen 重生 pb(owner.proto 新包 + errcode 增量;service/server/main
  在此之前编译红,data/biz 层已可编译测试)+ owner module go mod tidy;2. 部署接线
  (docker-compose/start.ps1/gen_cluster/离线镜像)按 §11.1 Codex/人;3. 生产 TiDB 需重放
  tidb-init/02;4. **集成属 migrate 阶段**(login query-first/allocator 双写租约/DS Admission/
  battle_result 终局/logout Release),旧 last_heartbeat_ms 再入门保留双门并行到 contract。

## 2026-07-22(续13):dbcheck 无界增长发布门禁 + 压测库增长断言(§9.24 收口,Claude)

- 用户拍板"上线前要检查所有库有没有无上限,压测也要测这个"→ 机械化落地,不靠人肉过表:
- **tools/migrate/cmd/dbcheck**(同 module 零新依赖,不需 tidy):内嵌与 §9.24 同步的全库
  登记清单(9 库 65 表,类别 bounded/swept/outbox/exempt),对真实库断言:①无未登记表
  ②swept 表清理索引齐备 ③outbox 无堆积;-snapshot/-compare 压测前后行数对比;
  -force-sweep -confirm=YES-DELETE 清理速率抽测(与服务同构批删,cutoff=now,只准压测库;
  player_mail/bag_journal/battles 组不重复实现,由服务 sweep 覆盖)。
- 规范接线:CLAUDE.md §9.24 增"机械化检查=发布门禁"段 + §8 压测核心句;AGENTS.md §10
  红线注明须同步 dbcheck 清单;stress-discipline.md §4.1.1(压前基线)/§4.3(压后三断言 +
  清理速率抽测)/完成清单加两项。
- 实测(本机 TiDB 重放全部 15 个 init SQL):工具当场抓到 pandora_owner 三张新表未登记
  (owner 线刚建,transition_log 正是只增流水——已登记:record/lease bounded,
  transition_log swept 且 owner 线已按 §9.24 预留 idx_created_at)+ 遗留旧 social 库缺
  5 个清理索引(ALTER 补齐后 PASS);造 3×20000 行流水 → -compare 增量精确 →
  -force-sweep 批删 14~18k rows/s 清空,全链路 PASS。
- 剩余:生产/CI 接线(发布 pipeline 里跑 dbcheck)由部署侧接;真 MySQL 8.4 未跑(同续12)。

## 2026-07-22(续2):邮件附件"实例托管转移"形态定契约(Claude)

- 背景(用户需求):领取附件里的**既存装备**必须"只改归属",实例身份与全部数据(鉴定态/
  词条等)不得改变;且机制要对未来一切实例类物品通用,不限装备。
- 现状确认:InstanceAttachment 是**铸造凭证**(config+count,领取时铸全新实例:新雪花 ID、
  未鉴定、词条鉴定时 roll)——只适用"发新物品",无法转移既存实例;误用会把玩家装备变成
  另一件东西。
- 落地:mail.proto 新增 oneof 分支 transfer = TransferAttachment{ bag.v1.BagItem item
  (快照,instance_id 必填,count 恒 1), source_player_id }(复用 bag 域 BagItem,§5.8 不造
  并行 struct);MailAttachment 头注释改三形态表 + instance 分支标注"仅发新物品"误用警示。
- **接线前两侧 fail-closed(已落码)**:发送侧 buildPayload 显式拒收 transfer(托管扣出机制
  未落地前放行 = 可伪造"声称托管但实例未扣出"的附件,§9.7);领取侧 partitionAttachments
  显式计 unknown → 整封 9606 保持未领取(不得落进 GrantInstances 铸造路径)。回归测试
  TestTransferAttachmentFailClosedUntilWired(发送拒 + 领取整封拒不发放不记 claim)。
- 三不变量入 bag-domain.md §7.1(全局唯一/归属变更快照原样/接线前 fail-closed);
  放开时机 = 拍卖成交走邮件(phase 3)或玩家转赠落地,必须与托管扣出 + bag 域领取链同一提交。
- ⚠️ pb 重生清单追加:mail.proto(新分支 + import bag/v1)——与既有 mail/bag/errcode/owner
  重生同一批交 Codex;mail biz 新测试在重生前编译红(同批闭合)。

## 2026-07-22(续3):owner migrate ⑥ 实例租约双写落码(Claude)

- hub_allocator / ds_allocator 两侧接线:Model B 授权心跳成功后、**响应返回前**经
  renewOwnerLeaseGate 调 owner.RenewInstanceLease(时序关键:DS 收到响应才延长本地租约,
  权威侧 lease 必须先覆盖该认知,否则 BeginTransition 屏障计算偏小)。
- 弱/强双模式完整实现(§14):owner_addr 空=不启用(默认,现网零变化);启用后默认弱依赖
  (失败仅告警,migrate 窗口由旧 last_heartbeat_ms 再入门双门并行兜底);contract 阶段置
  owner_lease_required=true 转强依赖(续租失败→心跳失败→DS 拿不到响应不延长本地租约→
  连续失败自我 fencing,权威侧租约滞后时 DS 必然停玩,屏障时序闭合)。
- hub 凭据无实例纪元:续租 epoch 传 0;owner 侧纪元守卫放宽为"双方都非零且不同才拒,
  存量 0 请求非零则补齐"(owner repo/biz 已改,身份完整性只要求 pod+uid)。
- 客户端:两服务各自 data/owner_lease_client.go(内网 insecure 直连,单调用 2s 超时,
  租约秒数=placement.DSFenceLeaseMaxSeconds,owner 侧再钳一次双保险)。
- 测试:renewOwnerLeaseGate 纯单测两侧**已真跑绿**(nil no-op/身份透传/弱依赖放行/
  强依赖失败,biz 包整体编译过=接线无编译错误);dev yaml 加注释配置块(owner 服务进
  编排后再开)。
- ⚠️ 交接:data 客户端依赖 owner pb(Codex proto_gen 后闭合);migrate ①-⑤(login/
  BeginTransition/Admit/battle_result/logout)未开工,依赖 pb 重生后按 owner-authority.md
  §4 顺序推进。

## 2026-07-22(续4):owner migrate ①-⑤ 全部接线(Claude;三服务编译测试全绿)

- pb 已由并行会话重生(owner/bag/mail/errcode),owner 服务整体 go build 通过,本轮全部
  改动可真实编译验证。
- **①/④ hub 归属 Begin(HUB)**:插在 hub_allocator signHubTicket(签票统一出口——分配/
  恢复/转移/Battle→Hub 回流全路径过此),弱 Query→decide→BeginTransition;hub 无独立
  实例纪元,以 ProtocolEpoch 充当(census Admit 同源,exact 等值自洽)。
- **② battle Begin(BATTLE)**:ds_allocator AllocateBattle READY 确认后、交付 matchmaker 前
  逐玩家弱 Begin(AllocateResult 全 Target);3s 批预算防 owner 卡顿拖慢分配。
- **③ census 代提交 Admit(migrate 近似)**:两 allocator 授权心跳 census 首见玩家
  Query→(记录指向本实例且 PENDING)→Admit(目标取记录字段,pod/uid 为调用方独立断言);
  屏障未开静默跳过下轮重试;进程内 sync.Map 已准入缓存(重启重查一轮收敛);
  contract 阶段移交 DS Admission 链原生提交后本近似退役。
- **⑤ logout Release**:login 登出成功(session compare-delete 命中)后弱 Query→Release
  (携带观察 epoch+operation,owner 侧幂等 no-op 防误删新 owner)。
- **§9.23 幂等规则落进 decideOwnerBegin**:记录已指向同一实例(类型+pod+uid 同)且
  PENDING/ADMITTED → 跳过不推进 epoch(同目标重连/重复交付零副作用)。
- 装配:三服务 owner_addr 空=整体不启用;allocator 复用 ⑥ 的同一连接(SetOwnerAuthority);
  login 新增 GrpcOwnerReleaser。全部弱依赖:任何 owner 故障只告警,路由决策不变,
  旧 last_heartbeat_ms 再入门双门并行(行为切换属 contract 阶段)。
- 验证:ds_allocator/hub_allocator/login 三服务全量 go test 通过(含既有心跳/分配/登录
  大 fixture 用例,零回归);owner 服务四包测试通过;gofmt 干净。
- ⚠️ contract 阶段待办(全链验证后):owner_lease_required=true 转强依赖;login 路由决策
  改 query-first 消费 owner 记录(§9.23 WAIT/TARGET 语义);Admit 移交 DS Admission 链;
  last_heartbeat_ms 旧门退役;CLAUDE.md §22"尚未实现"注记删除。

## 2026-07-22(续5):owner 服务部署编排 + dev 全链闭环(Claude)

- 五处登记:docker-compose.services.yml(owner 块,20017)、start.ps1 镜像构建清单、
  run_services.ps1 宿主运行清单、gen_cluster_config.ps1 服务清单、export_images.ps1
  业务镜像清单(20→21)。gen_cluster prod progress / B1 两个合约测试 PASS。
- dev 闭环:ds_allocator/hub_allocator/login 三处 owner_addr 置 127.0.0.1:20017
  (dev 一键启动含 owner;弱依赖,owner 掉线仅告警);owner_lease_required 保持缺省 false,
  contract 阶段才转强。
- ⚠️ 事故与修复(自查自纠):清理 yaml 尾部过时注释时误用 powershell 5(违反本仓
  "PowerShell 优先 7"),三个 dev yaml 被 cp1252 双重编码 + BOM 损坏;经确定性反向映射
  (UTF-8 读→cp1252 编码回原始字节)无损还原,xxd/乱码计数/中文/配置逐项验证恢复。
  教训:本仓一律 pwsh,或改文件只用 Edit 工具。
- 剩余非代码项不变:dev 库重放 01/14/15 SQL、UE 编译、DSN 集成测试、生产 TiDB 02、
  contract 阶段(强依赖/query-first/Admit 移交/旧门退役)。

## 2026-07-22(续6):堆叠扣空即删行 + 邮件 transfer 计数显式化(Claude)

- **堆叠道具用尽即删行(用户要求)**:inventory deductItemTx 扣到 0 时 DELETE player_items 行
  (原为 UPDATE count=0 留死行;读侧本就过滤 count>0,留行只会无界堆积)。再发放同 config
  走 GrantItems upsert 重建,行为不变;UseItem/SellItem/SettlePlayerTrade/FreezeForOrder
  全部经此函数统一生效。测试助手 queryItemCount 无行返回 0(语义=持有 0);新增回归
  TestUseItemEmptiedRowDeleted(用尽→行物理删除→幂等重放快照 0 不复活→重发放重建)。
  bag 域 sectionRemoveItems 本就扣空移格,无需改。
- **partitionAttachments transfer 单列(用户指出计数混同)**:transfer 从 unknown 拆出
  独立计数,ClaimMail 对两者给出不同错误消息(transfer="已识别但领取链未接线,
  bag-domain phase 2";unknown="未识别形态")——同为 9606 整封 fail-closed 保持未领取,
  但排查语义一眼可辨。既有 fail-closed 测试全部保持通过。
- mail/inventory 编译+测试全绿。

### 2026-07-22(勘误)retention_indexes 迁移 down 语义修订

此前条目写"新增 7 库 *_retention_indexes 迁移(…down 条件删)":down 已全部改为**有意
no-op**——清理索引属权威表定义(fresh-init 自带),回滚删索引会让"fresh 建表 + 回滚"的库
与权威定义不一致(2026-07-22 审计 P1)。migrate 测试锁死该语义(down 含 DROP KEY 即 FAIL)。

## 2026-07-22(续7):邮件 transfer 附件托管转移链接线完成(Claude)

- **transfer 三不变量落地(bag-domain.md §7.1,两侧同一提交放开)**:既存实例"只改归属"
  的托管转移链全通,经济域闭环(当前实例权威在 player_item_instance;phase 2 写权威切 DS
  后领取入包路径迁 bag journal,托管语义不变)。
- **inventory 三个系统 RPC**(内网直连,Envoy 精确 403 + 服务层 callerID==0 兜底):
  - EscrowOutInstances:同事务从 player_item_instance 扣出 + 写 mail_transfer_escrow
    (两表各以 instance_id 为 PK + 事务性搬移 = 实例全局唯一);bound 拒
    (新 errcode 7018 ERR_INVENTORY_INSTANCE_BOUND);幂等 ledger op=escrow_out。
  - ClaimTransferInstances:托管行 INSERT...SELECT 原样搬进领取人实例表(鉴定态/词条/绑定
    逐字节保留,零重铸零重 roll);**领取只认托管行**(缺行/收件人不符/config 漂移整批拒,
    伪造附件必 fail-closed);容量满可重试;幂等 ledger op=transfer_claim。
  - ReleaseTransferEscrow:saga 补偿归还源玩家;幂等由行存在性承担;不设容量闸
    (slot NULL 入包,资产归还优先)。
- **mail 侧**:个人邮件放开 transfer 发送(系统/公会邮件仍拒:多人可领与单实例矛盾;
  形状校验 instance_id/config 必填、count 恒 1、同封不重复);ClaimMail 接 TransferClaimer
  (幂等键 mail_xfer:{mail}:{player});transfer 无空领豁免(AllowNoopGrant 不放行,
  空领=托管行滞留资产静默丢失);过期未领沿用归档补偿链,托管行保持在途。
- **存储/登记**:mail_transfer_escrow 建表(mysql-init/08 + pandora_trade 000003 迁移,
  down=additive-only no-op:在途行是已扣出资产唯一持有处);dbcheck 登记 classBounded;
  CLAUDE.md §9.24 豁免表登记。
- **验证**:mail biz 4 例新回归(全链路由/系统邮件拒/形状校验/无 claimer 严格拒)+
  inventory biz transfer_test.go(fakeRepo 全链)+ data TestMailTransferEscrow_MySQL
  (真 MySQL 3307 绿:原子搬移/幂等回放/指纹冲突/越权/漂移/容量/释放);
  mail+inventory+migrate 编译测试全绿。go pb 已本地经官方 proto_gen.ps1 重生(lint 过)。
- **现状无生产发送方**:拍卖成交到账/玩家转赠/活动补发接入时走
  EscrowOut → SendPersonalMail → (失败)Release saga,零机制改动。
- 待办:cpp pb 同步 UE 仓(errcode/mail/inventory,Codex);dev 库重放 08 SQL 或跑
  pandora_trade 000003 迁移。

## 2026-07-22(续8):bag 域 phase 2 全链接线(门控默认关;Claude)

- **Go / BagService 五要件补全**:①身份 = DSCallbackGuard 验签 DS Bearer(inventory 新增
  ds_auth 配置,与 battle_result 同密钥体系);②owner 授权 = 逐写 QueryOwner,
  record.target 与调用方 pod/uid 全等 + ADMITTED + 租约在效,owner_epoch 由服务端解析代填
  (请求 0 = 代填;非 0 须相等,为票据携带 epoch 的 contract 阶段预留)。LoadBag 也过授权
  (伪造高 epoch 的加载会围栏真 owner)。bag.owner_addr 必填门(allow_unverified_owner
  仅 dev);授权门单测 3 例绿。
- **mail DS 三段式领取**:GetClaimableAttachments(意图落库 player_mail_claim 加
  claimed/intent_payload 列,pandora_social 000005 迁移 + TiDB init 同步;instance 形态
  一次性铸 ID,重放逐字节同内容)/ MarkMailClaimed(先消 transfer 托管行
  inventory.ConsumeTransferEscrow(新 RPC)再置终态);旧直连 ClaimMail 对意图行互斥拒
  (新 errcode 9607)。恰好一次:journal 幂等键 = mail_claim:{mail}:{player},
  崩溃任意点重驱动收敛。mail biz 新增 4 例回归全绿。
- **UE 侧(编译待用户)**:cpp pb 同步 bag/mail/inventory/common + 生成包装 0032;
  Codec_Bag / Codec_MailClaim;DS 子系统 5 个新 RPC(BagLoad/Append/Checkpoint +
  MailGetClaimable/MarkClaimed,bag 方法 hub/battle 双类型凭据域);
  **UMyBagPersistenceComponent**(挂 MyEntityPlayerState:SetPandoraPlayerId 即
  StartCheckout;单飞行单条批 journal 写者,EPOCH_FENCED 永久停写并释放认领;
  checkpoint 周期 8s + EndPlay 冲刷,实例鉴定态/词条 sidecar 保真;邮件领取驱动:
  预留→journal→Mark,实例项以权威 instance_id 入包);UMyBagComponent 加
  AddItemAuthoritative(权威 Guid 保留);BeginGatedPickup 按 checkout 状态选路
  (journal / 旧 ReportProgress,认领/回执/预留机制原样);PlayerState 加
  ServerClaimMailAttachments/ClientMailClaimResult 入口。
- **部署面**:Envoy DS 面加 bag 三写路径 + mail 两领取路径;客户端面 403
  GetClaimableAttachments/MarkMailClaimed/ConsumeTransferEscrow;UseItem/SellItem/
  ClaimMail cutover 403 以注释预留(DS 链全量启用并排空旧客户端后启用)。
  ds-arch §0.5 新增合法通道 ④(journal 直写五条硬约束)+ §0.6 红线措辞同步。
- **启用顺序**(全默认关,任一步可回退):迁移(social 000005)→ inventory 配
  bag.dsn+owner_addr+ds_auth=enforce → mail 配 inventory_addr(已有)→ Envoy 下发 →
  UE 设 PANDORA_BAG_JOURNAL_ENABLED=1(金丝雀 DS)→ 观察后全量 → cutover 403。
- 验证:mail/inventory 全测绿(真 MySQL 3307);全 14 服务模块编译零失败(待终验);
  UE 全量编译/联调交用户;phase 2 与并行落地的服务端拆堆(bag §5.2)改动已合并共存。
- 剩余(phase 3 前置):拾取切 journal 后 battle_result 掉落分支退役、仓库/活动段 UI 走
  GetSections、拍卖/交易在线扣、owner contract 阶段(票据带 epoch/强依赖)。

## 2026-07-22(续8):后端驻留段服务端建模 MaxStack 拆堆(用户拍板,Claude)

- 背景:架构评审确认 bag 域 sectionAddItems"同 config 无限合并单格"与 UE 格子语义矛盾
  (容量按条目数判定形同虚设 + uint32 Count 无检查累加存在回绕 + 客户端展示拆堆会画出
  超 capacity 格数);用户拍板"堆叠语义一段一权威,后端段由 Go 建模"。
- 实现(services/economy/inventory):
  - data/bag_apply.go sectionAddItems 重写:可堆叠按 MaxStack 拆堆——先规划(不改段)
    既有未满堆吸纳量,溢出按上限整格折算新格数,容量不足在任何写入前整体拒;应用阶段
    填堆+整格铺开。计数运算全程 uint64,回绕构造上不可能;超上限历史脏堆跳过不吸纳
    (防下溢)、资产原样保留。实例分支不变(每件一格)。
  - data/bag_repo.go 新增 BagMaxStack 回调类型;BagRepo.AppendJournal 接口与 MySQL 实现
    增参透传 applyBagOpTx→grantIntoBagType→sectionAddItems。
  - conf:BagConf 新增 default_max_stack(默认 99,与 UE MyBag::DefaultMaxStackSize 同值)
    + item_max_stacks 覆盖表 + BagConf.Validate(段容量/堆叠上限启动 fail-fast,main 接线);
    正式数据源为 §9.15 配置表管线道具表(与 CfgItem 同源),接入前由本配置承载,0 =
    未配置 fail-closed 拒写。
  - biz/bag.go AppendJournal 注入 cfg.ItemMaxStackOf。
- 测试:新增 TestSectionAddItemsMaxStackSplit 六例(拆堆 [5 5 2]/先填零头再开格/容量不足
  写前整体拒且零头不被部分填充/上限未配置 fail-closed/超满脏堆跳过/uint32 极值 4294967295
  入账不回绕);既有 bag 全部单测与边界测试按新签名平移,断言语义不变(合并计数均 <99)。
  inventory 全包 build/vet/test 绿;mail 构建绿(其对 bag 仅注释引用)。
- 文档:bag-domain.md 新增 §5.2(一段一权威堆叠语义表 + 无限合并三否决理由 + 数据源与
  禁令)。MySQL 集成测试仍门控于 PANDORA_TEST_MYSQL_DSN,本轮未跑真库(纯内存变换逻辑,
  事务边界未动)。
- 关联决策(评审已给、待用户拍板,未实施):重放容量语义(fail-closed→资产守恒+溢出临时
  格)、随身组 journal 组级寻址、UE FMyBag 幽灵格修复、存量 player_items 迁仓库、邮件领取
  默认目标段;拍板后先写 decision-revisit-bag-replay-semantics.md 再动码。
- 备注:biz/sweep_test.go 存在 gofmt 未格式化(并行会话文件,未动)。

## 2026-07-22(续9):bag 重放语义/存量迁移/幽灵格全量拍板落地(用户拍板"全部实现";Claude)

(注:上方存在两条「续8」,系并行会话编号撞车,内容各自独立有效。)

- **决策文档**:新增 docs/design/decision-revisit-bag-replay-semantics.md(D1-D7 全拍板):
  D1 重放容量语义 = 数据完整性 fail-closed 不变 + 容量冲突改"资产守恒 + 溢出临时格 +
  超容只出不进"(初版 fail-closed 拒载作废,违反不变量 20);D2 随身组 journal 组级寻址;
  D3 邮件领取默认进身上(评审建议进仓库,被并行落地的三段式链事实修正,机制均备可切);
  D4 checkout 失败 WAIT 降级;D5 存量迁仓库 + bag_migration 幂等闸 + contract 冻结时序 +
  transfer 托管链割接互锁;D6 FMyBag 扣空即删格;D7 整理后即时 checkpoint(优化项)。
  已否决:全量 op-log(TMap 迭代非确定)/写前强制 checkpoint/迁邮件(寿命钳丢资产)。
- **bag-domain.md 修订**:新增 §3.2(重放两类处置 + 组级寻址 + 守恒证明 + checkout WAIT);
  §7 默认领取形态定型进身上;§10 phase 3 行补存量迁移与托管链割接互锁;§11 矩阵加
  重放容量冲突/组级扣减/迁移双算三行。bag.proto BagJournalEntry 头注释补组级寻址契约
  (字段形状零变化;cpp pb 注释同步列 Codex 重生清单,非阻塞)。
- **Go 存量迁移作业(全落码,配置门 bag.legacy_migration_enabled 默认关)**:
  - 14-bag-tables.sql 新增 bag_migration(player_id PK + 对账三元组;一玩家一行永久幂等闸,
    §9.24 豁免登记 + dbcheck classExempt + CLAUDE.md 豁免清单 + main schema gate 补表);
  - data/bag_migration.go:ListLegacyBagPlayers(双表并集游标)/LoadLegacyBagStock(实例
    鉴定态词条 JSON 保真;**bound 实例 fail-closed 拒迁**——BagItem 尚无 bound 字段,
    phase 3 proto 批次补齐后放开,防绑定约束静默丢失)/SeedLegacyWarehouse(bag 库单事务:
    锁 bag_meta 行不 CAS epoch + 幂等闸 + sectionAddItems 容量豁免超容落位复用拆堆单源)/
    VerifyLegacyWarehouse(实例逐个在段 + 计数 ≥ + 记录三元组与 legacy 相等,冻结违反即暴露);
  - biz/bag_migrate.go:游标批量 runner,单玩家失败告警不阻断,重跑 no-op;main 接线
    (开启时告警提示 D5 时序纪律)。
- **测试**:biz 迁移 runner 纯单测 3 例(翻批/幂等 skip/失败继续/统计/取消);data 集成测试
  TestBagLegacyMigration_MySQL(DSN 门控:枚举游标/快照保真/bound 拒/幂等落位拆堆 6 格/
  对账过/超容段真实 journal 路径新格拒扣减照常/冻结漂移暴露);纯单测新增
  TestSectionOverCapacityDrainOnly(超容只出不进 + 低于容量恢复)。inventory + migrate
  全包 build/vet/test 绿,gofmt 干净。
- **UE 侧(D6,编译交用户)**:FMyBag DrainItemStacks/RemoveItemByPos 扣空即删格
  (Items+PosToGuid 同步清理,TMap 迭代后统一删);MyBagComponent ServerUseBagItem 的
  逐点幽灵格补偿删除(语义下沉后冗余);MyBagMergeTest 新增 DrainDestroysEmptyGrid
  回归(全量/部分/按格扣空 + 空位复用)。既有 SyncPrivateIncremental"清空后容器无残留"
  断言在旧实现下本应是红的,修复后语义对齐。历史 checkpoint 中已存在的 Size=0 条目
  经 AddItem 校验会被拒(dev 数据,bag 域未上生产,无存量风险)。
- **本轮未实施(有意,非遗漏)**:UE 重放溢出临时格/组级寻址重放/整理后即时 checkpoint
  属 UMyBagPersistenceComponent 恢复路径(续8 phase 2 刚落地待用户编译),按 §3.2 契约
  在其 LoadBag 重放器上实现,避免在未编译验证的新组件上叠改;邮件领取切仓库形态不做
  (D3 定型进身上)。
- ⚠️ 交接:1. Codex:bag.proto 注释重生(go/cpp,非阻塞)+ 本轮 Go/文档 commit;
  2. 用户:UE 全量编译(含续8 并行大改 + 本轮 4 文件)+ 跑 Pandora.Module.Bag.* 自动化
  测试;3. dev 库重放 14-bag-tables.sql(新增 bag_migration;schema gate 会 fail-fast 提示);
  4. phase 3 前置:BagItem 加 bound 字段后放开 bound 实例迁移。

## 2026-07-22(续10):背包容量购买链全量落码(用户"按建议"拍板;Claude)

- 需求:容量有配置初始值,玩家可花钱购买扩容(§5.3 契约 2026-07-22 已先行,本轮落码)。
- **proto(本地官方 proto_gen.ps1 重生 go+cpp,buf lint 绿)**:bag.proto 新增
  PurchaseCapacity RPC(幂等身份 = (player, bag_type, 第 N 档),价格/档位/封顶服务端
  权威)、BagEffectiveCapacity、LoadBagResponse.effective_capacities=5(随身段权威有效
  容量,checkpoint 内 capacity 仅回显);errcode 新增 ERR_BAG_CAPACITY_MAXED=14010。
- **存储**:14-bag-tables.sql 新增 bag_capacity(player+bag_type PK,extra 单调只增 +
  purchases 档数游标;dbcheck classBounded + main schema gate 登记)。
- **实现(inventory)**:
  - conf:BagCapacityPurchaseRule 阶梯价规则 + 默认档位(身上 0:10 档×10 格,第 N 档
    100N 金;仓库 1:15 档×20 格,第 N 档 200N 金;§5.3 拍板值,正式数值走导表管线覆盖);
    Validate 锁死仅 0/1 可买、档位合法、总格数 ≤ max_extra、可买段必须有 base;
    SectionCapacities 默认补身上 base 100。
  - data/bag_capacity.go:ChargeBagCapacity(trade 库;claimLedger 幂等
    key=bagcap:{bag}:{tier} + 指纹钳档参数 + deductGoldTx)/ ApplyCapacityPurchase
    (bag 库;锁 bag_meta 行不 CAS epoch + 档数 CAS:== tier-1 应用、>= tier 幂等回放、
    超 max_extra fail-closed)/ GetCapacityState;AppendJournal 事务内预取触及段 extra,
    判定容量 = base+extra(判定与权威同址);GetSections 返回有效容量。
  - biz:PurchaseCapacity(定档 → 预检 → 扣费 → 落位;两步间崩溃同档重试收敛,双击并发
    同幂等身份单扣单生效)+ CarryEffectiveCapacities(LoadBag 下发,只含配置 base 的段);
    CapacityCharger 注入(main 同进程直用 inventory repo)。
  - service:PurchaseCapacity handler + LoadBag effective_capacities 组装;
    Envoy DS 面两文件补 PurchaseCapacity 路由(客户端面整前缀 403 自动覆盖)。
- **测试全绿**(inventory/migrate/mail/battle_result/owner build+vet+test):biz 单测
  (顺序两档/购罄拒且不扣费/扣费后落位前崩溃重试收敛零重扣/金币不足不落位/不可买段/
  未装配 fail-closed/授权失败不扣费/LoadBag 有效容量视图);data 集成测试
  TestBagCapacityPurchase_MySQL(DSN 门控:扣费幂等/指纹冲突/余额不足/档数 CAS/乱序拒/
  超上限拒/有效容量进真实 journal 判定恰满边界/GetSections 有效容量/状态读取)。
- 文档:bag-domain.md §5.3 定稿(拍板值 + 已落码范围)。
- ⚠️ 交接:1. Codex:cpp pb 同步 UE 仓(bag/errcode)+ commit;2. UE 侧待 pb 同步后接:
  购买入口(UI → PlayerState Server RPC → DS 子系统 PurchaseCapacity Codec)、ACK 后
  ExpandCapacity、UMyBagPersistenceComponent checkout 用 LoadBag effective_capacities
  Init 随身段容量(不信 checkpoint capacity);3. dev 库重放 14-bag-tables.sql
  (新增 bag_capacity + bag_migration);4. gofmt 残留(并行会话文件,未动):
  services/economy/inventory/internal/biz/sweep_test.go、pkg/errcode/errcode_cause_test.go。

## 2026-07-22(续11):owner K8s manifests 补齐 + buf breaking 规则对齐(Claude)

- **owner K8s 编排缺口关闭**(续5 五处登记后唯一遗漏):deploy/k8s/services/services.yaml
  新增 owner Deployment+Service(20017,标准段:conf secret subPath owner.yaml + gRPC
  readiness probe;无状态 CAS 通道,标准滚更,不需 Recreate/POD_UID);文件头计数 20→21。
  overlays/online/kustomization.yaml images 补 pandora/owner 占位(20→21 条);
  netpol.yaml 注释计数 19/20→20/21(label 分层策略本身自动覆盖 owner,无需逐条加白)。
  验证:kubectl apply --dry-run=client 全过;kubectl kustomize online overlay 构建过,
  21 个 Deployment,owner 镜像钉扎生效。
- **buf breaking 58 项分诊完毕**(基线 = main HEAD a138ff2 = DS 旧协议锁):
  ①43 项 = R5 自报身份字段删除,8 proto 全部 number+name 双 reserved,属 §5.4/§9.17
  认可路径 → proto/buf.yaml breaking 规则改 FIELD_NO_DELETE_UNLESS_NUMBER_RESERVED
  + UNLESS_NAME_RESERVED(except FIELD_NO_DELETE),误删(未 reserved)仍拦;
  ②15 项 = mail MailAttachment 字段 1/2/3 原地改 oneof 三形态(开发期编号复用,proto
  注释有完整设计语义),不可也不应用规则放行 → 保留为"DS 必须重打包"的诚实证据,
  冻结提交落 main 后自然清零。改后 buf lint 过、breaking 余 15 项(全 mail)。
- ⚠️ 交接:1. run/cluster/etc 缺 owner.yaml(陈旧产物,下次 gen_cluster_config.ps1
  重跑自动生成,secret pandora-config 随之含 owner.yaml);2. 生成链无 TiDB DSN 处理:
  -Prod 产物 owner 仍指 mysql:3306,违反 §9.22 生产必须 TiDB——需运维提供 TiDB 端点
  后加 prod 改写规则或独立 owner prod 配置,当前仅 dev/minikube 可部署 owner;
  3. 本轮未部署未编译,集群未动。

## 2026-07-22(续12):独立复核回应 —— -Prod owner TiDB 门落地 + 部署计数硬门修复(Claude)

- 独立复核结论核实:buf lint=0 / breaking 15 项分诊准确;owner manifests kustomize 过
  (21 Deployment/21 Service);`git diff --check` 过;复核提出的两个硬问题均成立。
- **硬问题1(15 项 mail breaking)定性修正**:续11"冻结提交落 main 后自然清零"的表述
  不当——提交只移动 buf 比较基线,不构成兼容修复。真实关闭条件:
  ①开发期编号复用属 §5.4/§9.5 认可路径,前提 = 重生 go+cpp pb 并全量编译所有启用
  module(cpp pb 待 Codex 同步 UE 仓,UE 编译待用户);
  ②消费者穷举:mail 服务/battle_result 发送方(同仓重编)、UE 客户端+DS(同一 SVN 重
  打包);无线上老消费者(-Prod 路径尚不可用,不存在生产部署);
  ③存量数据是硬前提:player_mail/sys_mail/guild_mail/player_mail_archive 的 payload
  blob(MailContentStorageRecord)旧格式行在新 schema 下 wire type 不匹配 → 附件字段
  落 unknown,**附件静默丢失**;dev 邮件表清空(续10 已在案)必须在新版本部署前执行;
  ④上线后该路径永久关闭(§9.5 编号不复用/§9.17 兼容演进/§9.21 共存窗口双向兼容)。
- **硬问题2(-Prod owner 不安全)落地关闭**(续11 交接点2):
  - gen_cluster_config.ps1:新增 -OwnerStoreDsn / PANDORA_OWNER_TIDB_DSN;-Prod 强制
    非空 + 拒 dev 凭据(pandora_dev_pwd)+ 拒 dev MySQL 地址(mysql:3306/127.0.0.1:3307)
    + 必须 pandora_owner 库 + 拒控制字符;owner.yaml DSN 整行注入(复用既有 YAML 精确
    定位/转义,旧值必须含 dev 凭据特征防覆盖未知配置);require_tidb 机械翻转 true;
    **全部 21 服务 -Prod 统一 enable_reflection: false**(锚点 count==1,违例拒生成)。
  - owner 服务:新增 owner.require_tidb 配置(dev 默认 false)+ data.AssertTiDBBackend
    启动强校验(SELECT VERSION() 必须含 "-TiDB-",不符 fail-fast 拒启;DSN 字符串证不了
    后端真是 TiDB,与生成器校验构成双层防线)。
  - start.ps1:online -Prod 预检 PANDORA_OWNER_TIDB_DSN(BuildPush 推镜像前 fail-fast,
    dev 凭据/地址即拒,防"半推+未部署"脏状态)。
  - **修复部署必炸 bug**:Get-ServiceList 已含 owner(21 项)但 Apply-PandoraConfigSecret
    硬门仍 `-ne 20`——续5/续11 加 owner 后任何 k8s/online 部署在 Secret 组装即 throw。
    改 21,并同步 start.ps1/gen/docker-compose 全部 20→21 计数文案(battle 模式 18→19 容器)。
- 测试:owner 全 go test 绿(新增 isTiDBVersion 单测 + 真 MySQL 负向集成用例,后者
  PANDORA_TEST_MYSQL_DSN 门控);新增 gen_cluster_prod_owner_contract_test.ps1 PASS
  (缺失/dev 凭据/dev 地址/错库 4 负向 + 正向注入断言 + dev 行为不变);
  gen_cluster_prod_progress_contract_test.ps1 补 -OwnerStoreDsn 后 PASS。
- ⚠️ 交接:1. gen_cluster_b1_contract_test.ps1 在 **HEAD 基线即失败**(placement_mode
  enforce 断言 vs 已提交的 $PlacementSecretBindings=@() 清空,owner 迁移后测试未跟上;
  已用 HEAD 版生成器复现,与本轮改动无关,已建独立修复任务);2. push 副本 2→1 是
  07-22 push 审计的**有意**行为改动,拆分提交时应随 push 投递游标 v2 提交或单独提交,
  勿混入 owner manifests 提交;3. require_tidb 对真实 TiDB 的端到端放行未验(需真
  TiDB 实例,当前仅负向真 MySQL 拒 + 版本串单测);4. 本轮未提交未部署,集群未动。

## 2026-07-22(续13):玩家等级经验改为策划表单一数据源(Codex,用户授权)

- **删除 YAML 双曲线**:`player.exp_curve` 与 `PlayerConf.ExpCurve/ValidateExpCurve` 已移除;
  `experience_enabled` 只保留为功能开关。生产生成链机械保持 false,因为源 Excel 备注仍说明
  当前 Lv1-Lv15 数值是联调占位(仅 Lv8→Lv9=6600 已确认),不得把“已接单源”误报成
  “正式数值已确认”。
- **新增导表**:`player_level_exp.proto` 对应
  `Pandora-Client-SVN/Table/角色/j_玩家等级经验.xlsx`;该表数据从第 4 行开始,因此给生成器
  新增 `(excel_data_start_row)=4` 覆盖(未设置仍默认第 5 行),防漏 Lv1。当前真实产物
  `configtable/dist` = v20260722002 / svn-r1306,15 行;曲线为 1000..11400,Lv15
  UpgradeExp=0、累计 86800。重复生成确认内容不变、未写盘。
- **player 接线**:启动前强制加载配置表并校验 `ID==level`、等级 1 起连续、非末级经验>0、
  唯一末级经验=0、累计经验精确匹配;同时检查数据库 `players.level` 在表范围内。每次
  AddExperience 从单个不可变快照复制曲线,热更坏批次保留旧快照,且拒绝降低最高等级;
  同 gRPC 端口注册内部 `ConfigTableAdminService`。
- **运行载体**:宿主 dev 读仓库 `configtable/dist`;Compose 只读挂载;
  K8s/online/start-resume 由 `pandora-configtable` ConfigMap 整目录挂到
  `/app/configtable/active`。生成器把 player 集群配置固定改写到该路径,并断言不再出现
  `exp_curve`。ConfigMap 发布先冻结/校验候选,以 version 单调 + 同版本表内容精确一致 +
  `resourceVersion` CAS + UID 回读门禁前向切换;同版本只允许在表运行语义不变时同步 manifest 的
  `source_rev/generator/generated_at_ms` 溯源纠正。rollout 失败保留新批次供同版本重跑,
  不做与 Player Store 降版规则冲突的文件面回滚。
- **验证**:`buf lint`;configtable/configtable-gen/player 全量 Go 测试;matchmaker 受新增必需表
  影响的两组夹具回归;生成器真实源表幂等复跑;prod/dev 配置生成契约;start/gen PowerShell
  语法解析;ConfigMap create/no-op/溯源修正/同版本冲突/降版/写后未知结果重跑/UID 竞争模拟;
  K8s client dry-run;Docker Compose config 均通过。`go test -race` 未运行成功:
  本机没有 gcc(CGO 开启后 runtime/cgo 构建失败),未安装工具、未改系统环境。
- **边界**:`Store` 原子切换只保证单进程,当前单地址发布脚本不提供多副本原子切换;
  正式改曲线前仍须关闭经验入口并完成全 fleet 版本收敛。本轮未部署、未提交。

## 2026-07-22(续14):R4 复审 2×P0 + 8×P1 修复(push 会话门/gap 契约/好友判重/发布链)

- **P0① 建流 TOCTOU(INC-20260722-004 补修)**:`AuthorizeSubscribe` 与 `Register` 分离
  存在交错窗口(旧会话校验通过后暂停→新会话注册→旧会话再注册反顶新设备)。新增
  `AuthorizeAndRegister` 同玩家 64 条带锁内串行「校验+注册」,service 层改走该入口;
  可阻塞 gate 确定性复现原交错的回归 `TestAuthorizeAndRegister_StaleSessionCannotDisplaceNewer`。
- **P0② 会话复查看门狗**:复查从写者 select 拆出为独立 goroutine——写者阻塞在
  `stream.Send`(慢客户端流控)时会话失效仍 ≤30s 取消流上下文,写者每次 Send 前检查
  取消,不再投递任何新帧。诚实契约:30s 界定「停止投递+发起关流」,句柄物理回收由
  keepalive/max_conn_age/Envoy 1h 收敛。回归 `TestRunSubscribeStream_WatchdogClosesBlockedWriter`
  (阻塞写者期间顶号→零新帧+ErrUnauthorized 关流)。事故档案已补 4.1.1 节,保持未关闭。
- **P1 gap 契约重做(fail-closed)**:`GapSince(bool)` → `LostSince(丢失上界)`;检测移到
  **每轮拉空之后**(修剪只删 score 前缀,拉空时刻 fl>cursor ⟺ 有未投递即丢的帧),消除
  「建流时检一次、检查后/分页间隙修剪永不再报」漏报;检测失败按拉取失败退避、游标
  不动,删除告警放行路径及锁死该行为的 check-err 测试;检出丢失发一次 resync 并把游标
  跳到丢失上界(同段丢失只报一次)。push.proto 契约注释同步(**注释级改动,cpp/go pb
  重生待 Codex,不影响 wire**)。
- **P1 gRPC 状态映射**:新增 `pkg/errcode/grpc.go`(`GRPCCode`/`ToGRPCError`,显式转换
  助手,刻意不给 *Error 加 GRPCStatus() 防全服 unary 线上形态静默变化);push Subscribe
  全部返回路径接入:会话失效=UNAUTHENTICATED,权威不可达=UNAVAILABLE。
- **P1 好友重新申请判重误杀**:`friend_repo` 拒绝/过期后重新申请改为**换新 request_id**
  (客户端 (request_id,reason) 判重不再吞掉合法再次申请;旧 ID 失效,迟到 Accept 自然
  NotFound)。biz fake 对齐,回归 `TestRejectThenReapply_PushesNewRequestID`。
- **P1 noeviction 门与基线冲突**:dev compose/sentinel(含两副本)/minikube infra 三处
  基线 `allkeys-lru`→`noeviction`(该 Redis 承载会话/投递缓冲/租约权威态);push 启动门
  CONFIG GET 失败改**缺省 fail-closed**(新配置 `push.allow_unverified_eviction_policy`
  供托管 Redis 人工确认后放行);Cluster 模式改 `ForEachMaster` 逐 master 核验。
- **P1 configtable 发布脚本三修**:①残缺 active(有目录无 manifest)fail-fast,不再落入
  "active 缺失"分支生成 `active\staging` 嵌套且退出 0;Move-Item 前加不变量护栏。
  ②回滚精确恢复**服务端上报的 activeVersion**(内存 v7/磁盘 v9 场景不再恢复 v9 加剧
  劈叉;槽位缺失明示劈叉待人工收敛)。③同版本同批次门禁补 manifest 运行语义比对
  (proto/rows;active 路径与 history 槽位路径都比)。新增行为回归
  `tools/scripts/tests/configtable_publish_behavior_test.ps1`(PASS)。
- **configtable-gen 红灯**:`TestFilesShape` 断言硬编码 gofmt map 对齐空格,表数量一变即
  红;改为空白折叠后比较(`containsShape`),HEAD/工作区/后续加表均稳定。
- **UE 客户端(待用户编译)**:①`PandoraPushClient` 记录并暴露最近关闭的 gRPC 状态码
  (`GetLastCloseGrpcStatus`,Subscribe 复位);②`MyDsRecoveryCoordinator` 关流分类:
  UNAUTHENTICATED→`RenewSessionForRecovery`(不再每秒重放旧 token),其余指数退避
  1s..60s,收到任何帧/前台恢复清零;③resync 契约接线:Friend 重拉列表+申请、Team 重拉
  快照、Match 有活跃比赛时重拉进度、Coordinator 空闲态回源重查权威路由。
- **验证**:push/friend/errcode/configtable-gen 全量 Go 测试绿,go vet 绿,发布脚本行为
  测试 PASS。**未验**:真实 Envoy/Redis/多 Pod 并发验收(事故档案关闭条件)、真实
  MySQL/TiDB friend 容量测试(无 DSN 跳过)、`go test -race`(本机无 CGO)、UE 编译与
  resync 端到端、P1-7 回滚含 grpcurl 交互路径。R4 条件项(广播 group 旧位点/Redis 主切
  ACK 窗口/旧 member 格式/生成锁 fencing)未动,待拍板。本轮未提交未部署。

## 2026-07-22(R5 复审整改:5P0+10P1+10P2+3P3,session/push/friend/configtable 跨服务链)

审计基线 server d5b2d2b7c4 / client r1306(HEAD 8434fae9 仅多 start.ps1 WIP,不触审计链路)。

- **P0-1 旧 JTI 全服务吊销**:push RedisSessionGate 提升为 `pkg/sessiongate` + 新增
  `pkg/middleware.SessionCurrent`(payload jti 现行性门:顶号 ABORTED/14、登出过期
  UNAUTHENTICATED、权威不可达 fail-closed;无 payload 头=内部面放行),接线 12 个客户端
  面服务(friend/chat/mail/guild+group/trade/team/matchmaker×2/player/inventory/
  leaderboard/hub-allocator 玩家 method/push unary);friend/chat/mail/player/inventory
  dev 配置补 redis_client;全模板加 `session_gate.require`(dev false),gen -Prod 机械置
  true + 产物断言 + 新契约测试 `gen_cluster_session_gate_contract_test.ps1`(PASS)。
- **P0-2 过期旧设备反顶**:push recheckSession 先判会话代际后判到期,「已过期且已被顶」
  恒 ABORTED/14;回归 `TestRecheckSession_SupersededTakesPrecedenceOverExpiry`。
- **P0-4 跨 Pod 投递 fencing**:drainBuffer 每批 Range 后 Send 前复核 jti(Redis session
  key 单点串行 ⇒ 轮换后产生的帧旧流零交付,单/多 Pod/Cluster 均成立);会话失效关流不
  退避,权威不可达 fail-closed 不投递;跨 Pod 双 usecase 回归 3 例。事故档案 §4.1.1 补
  "条带锁仅单 Pod"边界修正。
- **P0-5 login 副作用终检**:`fenceLoginDelivery` 在 Login 交付前(含 battle 重连分支)
  复核本流程 jti 仍现行,失败扣留全部凭据;SelectRole/IssueDSTicket 三分支副作用后返回
  前二次过门(票据不出服务端=未取得);诚实边界:跨存储 ms 级残余窗口(角色写可被新会
  话覆盖)。回归 `login_delivery_fence_test.go` 4 例;测试假件 fakeSessionRepo 改为记住
  Set 的 jti。
- **P0-3 UE 会话代次绑定(待用户编译)**:PushClient 流快照 SessionGeneration + 关闭携带
  归属代次;登录成功在会话提交点无条件换代重订阅(撤四处 IsStreamActive 条件补订);
  Coordinator 关流处置前校验代次归属,登出态不安排重订阅;ReturnToLogin 显式关流;
  HandleSessionSupersededByOtherLogin 补幂等 guard。
- **P1**:①push gap 终检基线改进入时游标(丢 1001/幸存 1002 漏报修复+回归);②friend
  TiDB 并发:新增 friend_player_guards/friend_pair_guards 守卫行(TiDB 无 gap 锁,
  ODKU 点锁串行化;锁序 pair→player 升序→业务行),好友/黑名单/收件箱三上限进守卫临界
  区,AddFriend 拉黑/已好友复核进事务(P1-3),Accept/Block pair 全序(P1-4 既好友又拉黑
  消除);DDL 双库同步,§9.24+dbcheck 双登记,真实库并发回归 3 例(env-gated,本机无
  MySQL/TiDB 未跑);③configtable publisher:Assert-UInt64Field 严格整数(拒 1.5/字符串)、
  全部校验大小写敏感(-cne/-cnotmatch)、复制后快照边界比对(P1-10)、回滚槽位复验+原子
  补位(P2-9);行为测试新增 ⑤⑥ 用例 PASS;④UE:FriendClient/TeamClient 共用完成路径按
  会话代次+请求序号丢弃迟到回包(P1-5),Friend/Team 模型接 OnSessionChanged 切号清缓存
  (Team 补拉快照+邀请,登录补拉真正接通,P1-6;resync 补拉邀请),后台取消重订阅 ticker+
  回调自查暂停(P1-7),凭据被拒走完整 ReturnToLogin 清理(P1-8)。
- **P2**:offline.go 坏 member 记日志+折进 fl 哨兵触发 resync+Lua 自愈(P2-1);consumer
  拒 player 0(P2-2)、event_type 存在但非法毒丸不降级(P2-3);friend action 改每次调用
  专属回调(P2-4 单槽误配消除);push 重连退避加 ±25% jitter+稳定 30s 后才清零(P2-5);
  好友判重 64→256+切号清空+边界注明(P2-6);重新申请刷新 created_at(P2-7);Accept 收敛
  反向 pending(P2-8);跨 Pod 唤醒信号 pandora:push:wake pub/sub(写侧本地无连接才发,
  订阅端 SendTo,30s 轮询降级为兜底,P2-10)+infra.md 键位登记+miniredis 回归。
- **P3**:push.proto Subscribe/event_type 注释纠偏(⚠️ proto 注释变更,cpp/go pb 注释
  同步待 Codex 重生);Coordinator UNAUTHENTICATED 注释纠偏;INC-20260722-004 增补
  §4.1.3 + 验收矩阵 R5 五行 + 剩余风险(保持未关闭)。
- **验证**:pkg(middleware/sessiongate/config 等)+login+push+friend+chat+mail+guild+
  trade+inventory+team+matchmaker+player+leaderboard+hub_allocator 全量 Go 测试绿;
  发布脚本行为测试、session-gate/owner/progress 生成器契约测试 PASS(b1 契约测试为
  存量红,placement_mode,与本轮无关)。**未验**:真实 Envoy/共享 Redis/多 Pod/双设备/
  TiDB 并发/故障注入/race;UE 全量编译(交用户);friend 真实库回归需 DSN。本轮未提交。

## 2026-07-23(R6 复审:三条 P0 残余路径,只修 P0 + 交错测试,不宣称闭环)

R6 只读复审推翻上一条"P0 5/5 完成"的结论(上一条中"旧流零轮换后私有帧"的表述不成立,
特此纠偏;事故档案 §4.1.3/§4.1.4 已同步修正)。本轮按指示只修三条 P0:

- **P0-1(Envoy 层过期反顶)**:过期请求在 jwt_authn 即拒,应用层"先判代际"不可达;
  UE `RenewSessionForRecovery` 增加本地 exp 守卫(5min 时钟偏差余量,保守方向)——
  已过期/临近过期/不可解析 = 无法证明未被顶,走顶号同款清理链转交互登录,绝不自动
  重放;确定未过期才允许自动换新。行为变更:离线超 24h 回前台要求手动登录。待用户编译。
- **P0-2(fence 批内竞态)**:投递 fence 每批→**逐帧**;诚实上界 = 每条交付帧产生于该帧
  fence 通过之前,在途暴露 ≤1 帧(不宣称"轮换瞬间起零帧")。回归:批内轮换只交付
  fence 已过的 1 帧 + 批前轮换零交付,两例绿。
- **P0-3(角色写 fencing + 票据会话绑定)**:①`PlayerRoleRepo.SetRole` 事务内 precommit
  (UPSERT 后 COMMIT 前复核 jti,失败 ROLLBACK 不落地);②DSTicket v2 新增 sjti claim,
  签发链 login 三入口→resolveHub/battle v2→hub_allocator `AssignHubRequest.session_jti`
  (proto +1 字段,**go pb 已本地重生**,cpp pb 待 Codex);③`VerifyDSTicket`(redis
  authority 生产档 DS 在线核销)对非空 sjti 复核会话现行性,不匹配 ABORTED/14——响应
  窗口交付的旧票在兑换点作废。兼容窗(sjti 空,不判定):matchmaker READY 批签/allocator
  Transfer 重签/滚动旧票/dev/legacy;B1 纯本地验票模式由 ≤120s TTL 兜底(既有拍板)。
  回归:角色写 fencing 交错、兑换点五语义、sjti 签验往返,全绿。
- **验证**:pkg(auth 等)+login+hub_allocator+push+matchmaker 全量测试绿;测试假件/调用
  点已适配新签名(AssignHub/SelectRole/Resolve*/IssueDSTicket +sessJTI,SetRole +precommit)。
- **未处理(R6 已确认,按指示留待下轮)**:混版滚动旧副本无门(旧副本排空 = 安全生效门)/
  guild-prod 模板缺 session_gate/push-prod 模板缺 require_session_gate/friend 守卫内
  COUNT 需恢复 FOR UPDATE 当前读/guard 表存量迁移与 pair 表无界登记/旧本地 slot 抑制
  跨 Pod wake/空 event_type/Team 邀请无序号/parser error 迟到关流无代次校验/Friend 换号
  不自动重拉/AbandonRecovery 不清 push ticker/回滚槽位版本结构复验/进程内游标与判重
  持久化。**INC-20260722-004 保持未关闭;不宣称 P0 闭环**(真实 Envoy/多 Pod/双设备/
  race/UE 编译未验)。本轮未提交。

## 2026-07-23(发布线:构建产物退出版本库,四层发布线落地)

- **动机**:Packages 整包提交进客户端 SVN(源头 = Tool/Build/Jenkinsfile 的 Commit Packages 阶段)、
  pandora-images.tar(177MB)提交进 git,均属产物入库反模式。
- **落地**:①客户端 `svn rm --keep-local Packages` + 根 svn:ignore 增 Packages;git 解除 tar 跟踪,
  .gitignore 撤销 tar 白名单例外并兜底 artifacts/。②服务端钩子 `tools/vcs-hooks/`(SVN pre-commit
  路径黑名单 sh/bat/ps1 三版,git pre-receive 拒 *.tar+50MB;注意客户端仓 Pandora/Binaries 是有意纳管,
  不拉黑)。③制品目录层 `PANDORA_ARTIFACT_ROOT`(默认 F:\work\artifacts):artifacts_lib +
  publish_offline_images(git sha 版本戳/脏树拒绝/从 tar manifest 提镜像 ID/原子不可变发布+latest 指针)+
  fetch_offline_images(sha256 校验后落 deploy/offline-images,一键启动链不变)+ make_release(release
  manifest,引用版本永不清理)+ artifacts_retention(默认 dry-run)。④客户端 PublishPackages.ps1
  (svnversion 强校验+目录原样发布),Package.bat BUILD_INFO 增 Revision 戳,Jenkinsfile Commit Packages
  → Publish Packages(删 svn 提交凭据);后端新增仓根 Jenkinsfile + ci_backend.ps1(按 go.work 逐模块
  build+test,全绿才发布)。⑤文档:docs/design/release-pipeline.md 新增;offline-images README 重写
  (入库过渡方案退役);arch §11 决策行;AGENTS §4 tar 不入库;start.ps1 四处提示语换 fetch/publish。
- **验证**:8 个新 ps1 全部语法解析通过;retention dry-run OK;fetch 无制品 fail-closed;publish 脏树
  拒绝门生效。**未验证(诚实清单)**:publish 全流程真实出包(需重建 21 镜像)、PublishPackages 真实
  发布(svnversion 大工作副本耗时)、两条 Jenkins 流水线真实跑(Jenkins 服务与 agent 由人/Codex 装)、
  SVN 服务端钩子部署(需仓库管理员)。git 历史里的 177MB tar 未重写历史,瘦身需 filter-repo 单独拍板。
- 本轮未提交(SVN 与 git 改动均待用户审核提交)。

## 2026-07-23(续:INC-20260722-004 R7/R8 复审收口——会话代际定序 + sjti 分阶段收口,Claude)

- **R7 收口(此前未记录,本轮审计确认已在库)**:①UE 自动恢复登录废除,会话失效一律转
  交互登录(反顶窗口消除);②matchmaker READY 批签票签入当前 sjti(fail-closed);
  ③hub Transfer/迁移重签补 sjti + `AcknowledgeAdmission` 会话复核(proto session_jti=9,
  go/cpp pb 重生,UE 生成物同步);④Login 同步写 `player_session_generations`(MySQL,
  fail-closed),SetRole 同事务 FOR UPDATE 比对 fencing;⑤push 断层先 resync 信号后帧;
  ⑥UE code14 终态走顶号清理链;⑦VerifyDSTicket 会话门双检(marker 前+响应写出前)。
- **R8 复审判定 R7 仍有 5 条 P0**,本轮处置:P0-1 并发 Login 定序——000003 迁移加单调
  `generation` 列,MySQL-first 原子分配代际 + Redis「仅更高代际覆盖」条件写;P0-2 Hub
  ACK TOCTOU——耐久写后重读会话权威,不匹配 `AcknowledgeDeparture` 回滚再拒;P0-3 DS
  缓存 claims——准入缓存双重到期 + InitNewPlayer 匹配消费 + PostLogin 幂等重验;P0-4
  TransferToLine 前后双 `requireCallerSessionCurrent` 终检;P0-5 滚动发布——空 sjti 硬拒
  回退为三个分阶段门(`login.session_generation_enforce`/`login.require_ticket_sjti`(新增)/
  hub `session_gate.require_ticket_sjti`,默认全 false 兼容档),发布顺序权威文档
  **docs/design/session-generation-rollout.md** 新增(迁移→全 fleet emit→排空+等满票据
  最大 TTL(v2 180s/legacy 5min/混用 5min)→开 require;含 hub Recreate 单写者取舍记录)。
- **P1/P2**:pandora_account 000003 + pandora_social 000006(friend 守卫表)存量迁移;
  `mysqlx.CheckColumns` 新增,login 启动列级 dbcheck,friend 启动 CheckTables;push 坏
  member 折账失败扣发不漏报;match 重签失败 fail-closed 不回退旧票;hub 迁移通知失败
  保留源索引下 tick 补发;Logout MySQL 代际 CAS 墓碑;push.proto 注释对齐实现 + 客户端
  resync 契约(regen go/cpp,UE 两处同步)。
- **验证**:login(biz/service)/friend/pkg 构建+测试绿(其余服务全量测试见本轮末次跑批);
  UE 侧仅生成物与注释,待用户编译。**诚实边界**:签发器不结构性拒空 sjti(兑换点收口,
  migrate 对已登出玩家签空票合法);resync 无 ACK(重连补推再检出兜底);chat/guild 客户端
  推送消费未实现、mail 纯拉取;B1 模式仍短 TTL 兜底;真实并发/混版矩阵/故障注入未跑。
  **INC-20260722-004 保持未关闭**。本轮未提交(新文件 000006 SQL、rollout doc 未纳管)。

## 2026-07-23(续:INC-20260722-004 R9 复审处置——spawn 后复核 fail-closed + 混版窗口口径修正,Claude)

- **R9 复审在 HEAD 4b5f9adb 判定 7 条 P0 未闭**,本轮逐条处置:
  - P0-1 fencing 默认未启用:login/hub 部署模板 `session_generation_enforce` /
    `require_ticket_sjti` 置 true(生产口径硬拒),启动时对开关组合 fail-fast;
    rollout doc §1 记录「代码默认 false 仅为混版过渡,模板即生产默认」。
  - P0-2 MySQL-first 撕裂:login 代际分配回归 MySQL 单权威定序,Redis 仅作
    「更高代际才覆盖」的条件投影,消除双写撕裂窗口(r7_login_generation_test 扩展)。
  - P0-3 混版窗口漏算:rollout doc §2 拆成「票据 TTL 窗口(v2 180s/legacy 5min)」与
    「session 24h 生命周期窗口」两个独立等待面;阶段 D 前置改为「最后一个旧版 login Pod
    终止时刻 + 24h」或主动收敛;并修正原文错误——emit-only 档 SetRole 传空 sjti 不执行
    MySQL 代际比对,**没有**可观测 mismatch 告警,不能以「无告警」判定窗口已满。
  - P0-4 Hub gate 打开后终态竞态:UE PandoraHubGameMode 在 spawn gate 开放+locator 写回后
    以同 (admission_id, seq, sjti) 幂等重发一次 ACK,服务端 AlreadyAdmitted 路径重跑前置+
    后置会话复核;定性失效→FailAdmission 清退,未知→有界重试(共 3 次,ABA 门,可取消),
    耗尽仍未定性→fail-closed 清退。
  - P0-5 Battle spawn 后复核 fail-open:UE PandoraDSGameModeBase 复核改为在途状态机;
    结果未知/凭据缺失按未知处理,2s 间隔有界重试(共 3 次,同 ticket+admission_id 幂等),
    耗尽未确证→fail-closed Kick+销毁 Pawn;Logout/EndPlay 全量取消复核定时器。
  - P0-6 TransferToLine 路由副作用:终检失败不再遗留半程路由,失败路径补偿/回滚后再拒绝。
  - P0-7 hub-allocator Recreate 停服窗口:**未解决,保持 OPEN**;rollout doc §5 重写为冲突
    记录(dsauthfence V3 单写者 vs 不停服红线),附 succession-lease+单调 fencing token
    设计草案,明令禁止在未实现继任协议前单独改回 RollingUpdate。
- **P1/P2**:hub ACK postcheck 结果分型(未知不回退 owner,定性否定 exact 回退);friend 热
  路径读加 FOR UPDATE;friend_pair_guards 增 created_at+保留期清扫(000006 扩展);push
  resync 客户端脏标记+有界重试(Team/Friend;Match 靠既有进度轮询,注释说明豁免理由);
  cursor=0 首连跳过 LostSince 落为 push.proto 显式交付契约(依赖客户端「先订阅后拉快照」
  时序,MyAccountModel 唯一订阅点);mysqlx.CheckColumnSpecs 新增列类型/可空/键形状校验
  (login 接入);Kafka migrate 发布失败补偿;tools/migrate 测试修绿。
- **验证**:login/hub_allocator/friend/push/tools-migrate 测试绿(见本轮末次跑批);UE 改动
  (MyTeamModel/MyFriendModel/MyMatchModel/PandoraDSGameModeBase/PandoraHubGameMode)仅过
  静态诊断,**编译由用户执行,本轮无编译证据**。**诚实边界**:P0-7 未解决;真实并发/混版
  矩阵/故障注入未跑。**INC-20260722-004 保持未关闭,待 R10 复审**。

## 2026-07-23(续:INC-20260722-004 R9 P0-7 收口——hub-allocator 写者继任协议落地,Claude)

- **P0-7 关闭**:按 rollout doc §5.3 原草图实现写者继任协议,hub-allocator Deployment
  从 `Recreate` 改为 `RollingUpdate{maxSurge:1, maxUnavailable:0}`,发布无停机窗口,
  不停服红线(2026-07-01 硬约束)与单写者约束同时满足。三层构成:
  - **继任租约** `pkg/dsauthfence/writerlease`(dsauthfence 子包,零 go.mod 变更,
    复用其 etcd mTLS 安全姿态):etcd `concurrency.Election`(election=
    `hub_allocator/writer`),`election.Rev()`(leader key CreateRevision)即单调
    fencing token,届次严格递增;session 掉线立即降级+退避重竞选,退出时 Resign
    亚秒交接。仅 Model B(`AuthorityModeRedis`)启用,无新增配置面。
  - **业务闸门**:`biz.requireWriter()` 于 AssignHub/ReleaseHub/TransferHub/
    TransferToLineForPlayer/Heartbeat/AcknowledgeAdmission/AcknowledgeDeparture
    入口 fail-fast,非写者返回可重试 UNAVAILABLE;心跳清扫失租暂停、得租恢复。
  - **存储级 fencing(权威防线)**:持久化 fence 键 `pandora:hub:wfence:{pod}`
    (与 shard/auth/ledger 同 slot,进同一 WATCH/MULTI/EXEC);所有 hub 权威写
    事务 Watch 回调内 guard:水位 > 本届 token → 零写入拒绝(ErrWriterSuperseded,
    可重试),< 本届 → 写管线内原子推进;fence 键永不 TTL/删除。迟到旧写者即使
    绕过业务闸门也被存储层确定性拒绝(Chubby sequencer,与会话代际同构)。
- **守护测试互锁**:main_test.go `TestKubernetesDeploymentRollingUpdateRequiresWriterLease`
  同时断言 manifest RollingUpdate/maxUnavailable=0 与 main.go 装配 writerlease,
  缺一即红;新增 data 层 fence 测试(拒写零变更/推进水位/幂等/损坏值 fail-closed/
  nil fence legacy/teardown proof 受 fence)与 biz 闸门测试。writerlease 自带
  fake backend 单测(当选/失租降级/重竞选 token 递增/Close Resign/配置校验)。
- **验证**:pkg/dsauthfence(含 writerlease)与 hub_allocator 全模块 build/vet/test 绿。
- **诚实残余**(rollout doc §5.2 记录):每玩家 assignment 键(无 hashtag)不可入
  fence 事务,仅业务闸门+既有精确 CAS 保护;滚动重叠期非写者副本写请求收可重试
  UNAVAILABLE(重试即成功,非零感知);readiness 未与租约挂钩(保读流量)。
- **首次升级迁移仪式(必读,rollout doc §5.3)**:首次从不含 writerlease 的旧镜像
  升级时旧二进制不受协议约束,滚动重叠=最后一次无保护双写窗口;必须先
  `kubectl -n pandora scale deploy hub-allocator --replicas=0` 再 apply。此后升级全程无停机。

## 2026-07-23(续:静态复审逐条核实与修复——7 P0/10 P1/3 P2 判定处置,Claude)

- **外部静态复审逐条核实**,判定后按批修复(全部服务端测试绿):
  - **P0-4 assignment 键 fencing 盲区 → 修复**:①继任者 fence sweep
    `AdvanceWriterFences`(RunHeartbeatSweep 每届次一次,shard SET ∪ transfer 清理源
    pod 逐个 WATCH/MULTI 单调推进,消除 lazy-advance 盲区);②签票前
    `confirmWriterForTicket` 复核租约(AssignHub 新/旧路径 + TransferHub 两路径,
    失租扣票返回可重试 UNAVAILABLE)。§5.2 残余口径改写为四层覆盖
    (闸门/CAS/sweep/签票复核),残余窗口收窄至「失租通知与复核之间旧写者可能写一条
    合法 assignment(数据有效、票据不下发)」。
  - **P0-3 login Set 报错但已提交 → 修复**:sessions.Set 失败后回读 GetJTI,确认
    已提交则跳过 JTI restore 向前收敛(登录仍失败,但不再回滚已生效的新会话)。
  - **P0-6 writerlease 竞选静默失败 → 修复**:新增 `Health()` 可观测(连续失败数+
    最后错误);连续竞选失败 ≥15 次(约 30s)日志升级 Error 提示「可能全局无写者」。
  - **P0-5 两步迁移误判 → 驳回**(spec.strategy 不属 pod template,strategy-only
    apply 不重启 Pod,§5.3 两步法成立);**P0-1/P0-2(UE 预生成复核前移)为 R9 既定
    取舍,保持现状待用户裁决**(前移会给每次进场加一跳阻塞 RPC)。
  - **P1-2 owner census admitted 缓存吞掉重进 → 修复**:缓存按 census 轮剪枝,
    离场玩家条目删除,重进(代际推进)后重新查询+重新 Admit。
  - **P1-3 owner 记录漂移无自愈 → 修复**:census 发现记录不指向自己但本地
    assignment 指向自己时,以弱一致 BeginTransition 自愈(真迁移不受影响)。
  - **P1-4 login AssignHub 单发即败 → 修复**:有界重试(3 次/150ms 退避,仅
    UNAVAILABLE 类可重试,其余 fail-fast),覆盖写者交接窗口。
  - **P1-5 push 本地陈旧 slot 抑制跨 Pod 唤醒 → 修复**:消费后本地 SendTo 快路径 +
    **无条件** PublishWake(wake 幂等 size-1 去重,双唤醒零成本)。
  - **配置模板**:guild/push 生产 .example 补 session gate require:true 与
    node.redis_client;push topics 硬编码列表移除(回落 kafkax.PushTopics 单一权威)。
- **UE 客户端修复(静态改动,编译由用户执行)**:
  - P1-6 PandoraPushClient 解析错误路径排队的 CloseStream 补 generation 守卫,
    旧流迟到关流任务不再误杀新流;
  - P1-7 修复方式**修正复审建议**:不能在 AbandonRecovery 移除 push 重订阅 ticker
    (匹配失败路径玩家仍持会话留大厅,摘 ticker 会让已关闭推送流永不恢复);
    实际收口在 ResubscribePush 增加未登录守卫,登出后迟到 ticker 零副作用;
  - P1-10 Hub Admission/Departure ACK 补 10s 有界超时(对齐本文件 unary 约定,
    悬死不再卡住准入重试链);
  - P2-1 Friend/Team Deinitialize 补 ClearTimer(ResyncRepullRetryTimer);
  - P1-8 判定**无需改码**:两模型在每次新 resync 信号已重置重试预算=3,
    「预算耗尽永久停止」只持续到下一次 resync,为 R9 有界重试既定设计。
- **验证**:hub_allocator/login/push/pkg-dsauthfence 全模块 build/vet/test 绿
  (新增 owner census 剪枝+自愈、写者失租扣票、fence sweep、login 重试与
  回读收敛、writerlease Health、push 无条件唤醒等回归用例)。UE 五文件改动
  仅静态检查,**编译由用户执行**。**诚实边界**:P0-1/P0-2 保持现状待裁决;
  mail/leaderboard 无生产 .example 模板(超出本轮范围);
  **INC-20260722-004 保持未关闭**。新增文件未纳管,待用户提交。

## 2026-07-25 R10 复审整改:P0-1/2/3/4/5 落码 + UE 进场前复核门(P0-6/7)

- **P0-1 login 跨存储模糊回补(Go,已落码测试绿)**:`sessions.Set` 报错后旧实现用
  `GetJTI` 读回判断,**读回本身失败时无条件回补 MySQL 旧 jti**——若 Redis 实际已提交,
  就造出「Redis=新 jti / MySQL=旧 jti」跨存储撕裂(§9.22 禁止对不确定结果猜测)。
  新增 `reconcileFailedSessionWrite`:① 先用按 jti 的 CAS 删 `DeleteIfJTI` 把
  「Redis 到底提交没有」变成可判定——无错误返回即证明 Redis 不再持有本次 jti,
  才做条件回补(保住 R9 P0-2 目的);② CAS 删本身失败 = 不可证明 → 改走
  `TombstoneSessionJTI` 条件墓碑 fail-closed。回归:committed-but-errored 回滚后回补、
  不可证明走墓碑不回补、墓碑失败不掩盖原始错误。
- **P0-2 writerlease 长期无主可观测(Go,已落码)**:`Health()` 扩为 `HealthSnapshot`
  (held/token/竞选失败/激活失败/degraded),hub_allocator HTTP 暴露 `/healthz/writer`
  (degraded → 503)。**按 audit-residual A1 明确不接 K8s readiness**:失主副本是有意热备,
  门成"必须是 writer"会让滚动升级死锁、全体无法当选时把写降级放大成整服零端点;
  `main_test.go` 守护"不得把 /healthz/writer 接进 readinessProbe"。
- **P0-3 标准生成链 hub 地址(工具,已落码契约测试绿)**:此前只有 login-prod.yaml.example
  手写 headless 地址,**标准生成链仍产出 ClusterIP 短名**,生产实际配置绕过整条 round_robin
  修复。新增 `Set-ProdLoginHubHeadlessAddr`(仅 -Prod 机械改写为
  `dns:///hub-allocator-headless.pandora.svc.cluster.local:20021`);非 -Prod 保持短名
  (同一产物 compose 共用,FQDN 在 compose 内不可解析)。契约测试双向断言。
- **P0-4 继任 sweep 接流前硬门 + assignment 持久 fencing(Go,已落码测试绿)**:
  ① `writerlease.Config.OnElected` 激活钩子——当选后、**宣告持有领导权之前**必须跑成功
  `AdvanceWriterFencesForToken`,失败即让位重选,消除"已接写、继任推扫未完成"窗口;
  ② 归属记录内嵌 `HubAssignmentStorageRecord.writer_token`(allocator.proto 31,additive):
  同一 key 的 WATCH/MULTI/EXEC 天然原子,`current.writer_token > 本届 token` → 零写入拒绝,
  被继任的旧写者**既不能覆盖也不能删除**继任者写的归属;0 = 旧记录/未启用,双向兼容。
- **P0-5 首次 writerlease 引导升级(Go+文档,已落码;集群演练待跑)**:新增
  `hub.writer_lease_mode`(enforce 默认 / warmup 只竞选观测 / off),rollout §5.4 三跳发布合约。
  **照实说明边界**:旧二进制既不竞选也不读 fence 键,"零写暂停"与"零双写"原理上不可兼得;
  §5.4 选零双写,每跳窗口 = 一次 Pod 重启(等于迁移前基线),第三跳换 RollingUpdate 后进稳态。
- **P0-6/P0-7 UE 进场前会话复核门(静态改动,编译由用户执行)**:把既有复核从
  "spawn 之后补检"前移为"spawn 的前置条件"——**确证前不存在可操作 Pawn**。
  Hub:`TryOpenAdmissionSpawnGate` 改为先 `StartPreOpenAdmissionRecheck`,确证通过才
  `FinishOpenAdmissionSpawnGate`;Battle:新增 `SessionRecheckSpawnGate`,`PostLogin` 在
  `Super::PostLogin` 前入门,starting flow 与 spawn 工厂两道门拦截,确证后
  `OpenSessionRecheckSpawnGate` 补做唯一一次 starting flow。原 `PostSpawn*` 符号整体更名
  `Entry*`/`PreOpen*`(语义已变)。**§9.19/20 不卡死自证**:每条终局路径要么开门要么 Kick
  (含"非会话语义的定性拒绝也开门"、无挂账/无后端子系统直接开门),未知有界重试,耗尽 fail-closed。
- **P1 proto 生成物漂移**:`match.pb.go` 仍带旧"10 人"描述 → 跑 `proto_gen.ps1` 重生全量 go pb
  (含本轮新增 hub `writer_token`)。
- **验证**:go.work 全 31 个 module `go build` + `go test` 绿;
  `gen_cluster_prod_progress_contract_test.ps1` PASS。**未跑**:UE 编译(仓库规定由用户执行)、
  §23 验收矩阵与故障注入、集群冒烟(headless LB 分发 / writerlease 三跳演练)。
- **诚实边界**:P0-6/7 只是窄版屏障,§9.22 的 PENDING→ADMITTED 全量屏障仍属阶段 B/D;
  复核返回与开门之间仍有进程内 check-then-act 窗口(窗口内无可操作 Pawn)。
  未过 UE 编译与集群验收前,**不得声称这些 P0 已关闭**。

## 2026-07-26 R13 Codex P0 实施：Login 失败代次墓碑、Hub writer fencing 与 UE 选角门闩

- **授权边界**：用户明确授权 Codex 实现 P0（覆盖 `AGENTS.md §11.1` 分工限制）；未
  commit/push/tag，未运行 UE 编译。
- **Login 跨存储补偿纠偏（INC-20260726-002，仍未关闭）**：删除 R12“恢复即时前代”
  模型，避免 A 已交付、B/C 未交付时把 B 恢复成 current。Redis `Set` 同
  `(jti,generation)` lost-reply 幂等成功、同代不同 jti fail-closed；普通 Redis Set
  失败时，MySQL 精确条件墓碑与 Redis `<=failedGen` 无能力墓碑分别使用独立有界预算，
  更高赢家不受影响。MySQL COMMIT 模糊且读回失败时，只有 MySQL 条件墓碑明确命中才
  证明 generation 已持久占用并允许继续 fence Redis；no-op/error 不拿可复用的未证实
  generation 清 Redis。常驻交错覆盖 A→B→C 不恢复 B、B 未落地/C 复用同 generation
  后 B 迟到消歧不误杀 C、任一侧失败/上下文取消。补充 Session/Fence TTL 数据层边界，
  JWT 配置拒绝低于 1s 的 TTL。
- **Hub writer/assignment（INC-20260726-001，仍未关闭）**：writerlease 仅以 etcd
  `TimeToLive` 证据续本地任期，proof 失败自 fencing；激活有界单飞；assignment 写后
  使用本次 token/完整 intended/原 PTTL 精确补偿；删除墓碑复用既有 assignment_id 写
  UUID 防同任期 ABA；`CreateShard` 与 `{pod}` writer fence 同事务；持有期 proof 失败
  进入 Health。**结构性 P0 仍 OPEN**：受支持的 Redis Sentinel/Cluster 异步复制主切
  可能回滚已确认写，最终需 §9.22 线性一致 owner authority，不能用 WAIT/min-replicas
  冒充关闭。
- **标准生成链**：canonical green 强制单副本 + Recreate，并同步 strategy annotation、
  downward-API `PANDORA_DEPLOY_STRATEGY`；契约测试拒绝重复 env/漂移，PASS。基础模板仍
  RollingUpdate；真实三跳发布未演练。
- **UE 选角 P0（INC-20260726-003，待验证未关闭）**：外部并发提交已到 SVN r1502；
  在其上仅新增本地改动：当前权威 `ROLE_REQUIRED` 到达时释放旧 SelectRole 的
  `bSelectRoleInFlight`，避免旧 callback 因 token 失配退出后永久吞掉后续确认。未编译、
  未运行乱序/双端验收。
- **Owner Authority 全链复核**：Login/Hub/Battle/matchmaker/票据/UE 尚未形成同批
  Begin→PENDING→admit_not_before→Admit→PLAYABLE→Release 闭环；保持
  `owner_query_first` 与 allocator contract 开关关闭，禁止把局部 writer 修复声明成
  一人一 DS 架构闭环。
- **验证**：login 全模块 `go test ./... -count=1` + `go vet ./...` PASS；pkg/auth 定向
  test/vet PASS；pkg/dsauthfence 与 hub_allocator 全模块 test/vet PASS；
  `ds_auth_activation_contract_test.ps1` PASS；`git diff --check` 干净。真 MySQL/Redis HA/
  etcd 分区/SIGKILL/race/多副本部署/UE 编译与玩家路径均未执行，三份事故档案保持未关闭。

## 2026-07-27:全服单点存储审计 —— account 迁 TiDB 落地 + 两个 -Prod 安全 P0(Claude)

**起因**:用户问「全服的东西不能就一个 MySQL 库吧」。对 10 个库域做并行审计 + 对抗验证
(验证方直接连本机在跑的 `pingcap/tidb:v8.5.1` 做实测,而非查文档推断)。

### 一、审计推翻的结论(先说,避免后人照着错清单干活)

- **「TiDB 不支持 utf8mb4_0900_ai_ci / 带它的 DSN 连不上 / 建表即失败」全部不成立**。
  v8.5.1 上 `SHOW COLLATION` 显示 Compiled=Yes、NO PAD;`new_collation_enabled=True`;
  实测 `'a'='A'`=1、`'a '='a'`=0。该 TiDB 上九个 `pandora_*` 库早就用这个 collation 建好在跑。
  **不要**把 DSN / DDL 改成 `utf8mb4_bin` —— 那才会引入 PAD SPACE 与大小写语义翻转。
- **「TiDB 无 gap 锁,所以 `WHERE player_id=? FOR UPDATE` 挡不住并发 INSERT」只对范围条件成立**。
  实测:悲观事务对**不存在的行**做主键点条件 FOR UPDATE 照样阻塞并发 INSERT(~7s 直到提交)。
  `session_generation.go:161` 那种主键点查不受影响;mail 的 `COUNT(*) ... FOR UPDATE`(二级索引
  范围条件)才是真穿透。
- **`session_generation.go` 的 errcode 条不成立**:TiDB 实测回**兼容的 1213**,`isRetryableTxErr`
  照常生效;且 `biz/login.go:363` 把 `PersistSessionJTI` 的任何错误统一包成 `ErrUnavailable`
  (可重试),内层 `ErrInternal` 只是 cause,到不了客户端,不违反底线 1。
- 方法学教训:凡涉及 TiDB 的结论,先跑 `SELECT VERSION()` /
  `SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME='new_collation_enabled'` /
  `SHOW COLLATION`,并发语义必须写双 goroutine 探针实测,不能靠文档措辞推断。

### 二、本次修复的两个 -Prod 安全 P0(与 TiDB 无关,但优先级更高)

1. **`-Prod` 产物的 login.yaml 带着 `dev_skip_password: true` + `dev_auto_register: true` +
   `dev_allow_any_role: true`** —— 即**生产任意账号 + 任意密码都能登录并自动开号**。
   实跑生成器确证(不是读代码推断)。生成器 `Set-Prod*` 家族此前完全没覆盖这三项;名义检查器
   `release_preflight.ps1` 三重失效(无调用方 / 默认路径是失效的历史绝对路径 `E:\work\Pandora\services` /
   glob `*-prod.yaml` 在本仓匹配 0 个文件,且扫模板而非发布产物)。
   → 新增 `Set-ProdLoginDevSwitchesOff`(三项锚点 count≠1 即拒绝生成)。
2. **`-Prod` 产物除 owner 外全部 11 个服务带着公开 dev 库口令 `pandora_dev_pwd@mysql:3306`**。
   生成器对 JWT / DS / placement 各类密钥都有严格注入与拒绝复用,唯独数据库凭据零把关。
   → 新增 `Assert-ProdDbCredentials`:已接线注入的服务(owner / login)**硬断言**无 dev 凭据;
   其余 10 个登记在 `$ProdDbCredentialDebt` 里,每次 -Prod 生成打印欠账清单(expand→migrate→
   contract,清单删空后改成无条件断言)。一次性泛化成统一 `-DbDsn` 机制与 §15 冲突且需先定
   生产存储拓扑,不在本次替用户拍板。

### 三、owner 的 latent P0(生产首次部署即 100% 失败)

`owner_repo.go:185` 用 `sql.TxOptions{ReadOnly: true}` → go-sql-driver 发
`START TRANSACTION READ ONLY` → TiDB 在默认 `tidb_enable_noop_functions=OFF` 下返回
**`Error 1235: function READ ONLY has only noop implementation in tidb now`**。
owner 生产被 `-Prod` 机械注入 `require_tidb: true` 强制连 TiDB,而 `Query` 是 owner **唯一读路径**
→ 一上生产 100% 读失败;dev 走 3307 单机 MySQL 所以永远测不出来。
→ 改 `BeginTx(ctx, nil)`(普通事务按 start_ts 取快照,同样满足「两读同快照」)。
**不要**用 DSN 打开 `tidb_enable_noop_functions` 绕过。

### 四、account 迁 TiDB 落地(用户要求的主线)

- `deploy/tidb-init/03-account-tidb.sql`(新):雪花 PK 三表 NONCLUSTERED + SHARD_ROW_ID_BITS=4 +
  PRE_SPLIT_REGIONS=4;`account_devices`/`account_bans` 代理主键 AUTO_RANDOM(已核实 Go 侧不读
  LastInsertId、不依赖 id 顺序)。**collation 保持 utf8mb4_0900_ai_ci,刻意不跟随 01/02 用
  utf8mb4_bin** —— 本库有客户端上报的字符串唯一键(`accounts.account`、`account_devices.device_id`)
  且 Go 侧零归一化,唯一性语义由 collation 决定。
  **已在真实 TiDB v8.5.1 上装载验证**:五表属性符合预期(SHARD_BITS=4 / NONCLUSTERED /
  PK_AUTO_RANDOM_BITS=5),`accounts.account` collation 正确,行为探针 CI=true、PAD=false。
- `pkg/mysqlx/backend_check.go`(新):`AssertTiDBBackend` 从 owner 的 internal **下沉共享**
  (各服务独立 go module,login 无法 import owner internal;两份实现必然漂移,§15.5);
  新增 `AssertTiDBVersionAtLeast`(v7.4 下限,解析失败 fail-closed)与
  `AssertColumnCollationSemantics`(**行为探针**:大小写不敏感 + NO PAD 两个维度)。
  owner 侧保留同名薄封装,不改既有调用点与测试。
- `login`:新增 `login.require_tidb`(dev false / -Prod 机械 true),启动期跑上述两条断言;
  `CheckTables` 从 2 张表补齐到 5 张;新增 `login-dev-tidb.yaml`(login-dev.yaml 逐行副本,仅改端口)。
- 生成器:新增 `-AccountStoreDsn` / `PANDORA_ACCOUNT_TIDB_DSN`(-Prod 强制),校验规则与 owner 同构
  并**多一条**:拒绝 `collation=utf8mb4_bin`。校验块**排在 owner 块之后**——插到前面会让既有
  -Prod 负向用例改为因「缺 account DSN」而失败,仍 PASS 却证明不了自己声称的东西(静默空转)。
- `tidb_up.ps1`:此前**硬编码只装 `01-social-tidb.sql`**(`02-owner-tidb.sql` 从未被这条链路装载过),
  改为遍历目录逐个装载 + 循环内判 `$LASTEXITCODE` + 库名从 DDL 提取并与白名单对账 + 装载后回读断言。

### 五、顺带修掉的 social(已在 TiDB 上跑)的实际破损

`deploy/tidb-init/01-social-tidb.sql` **漏了整张 `player_mail_archive` 表**和四个 sweep 索引
(`sys_mail`/`guild_mail` 的 `idx_end`、`player_mail` 的 `idx_expire`、`player_mail_claim` 的 `idx_mail`)。
mail 已经在 TiDB 上跑(`run_services.ps1` 用 `mail-dev-tidb.yaml`),而 `mail_repo.go:479` 的
`ArchiveAndDeletePersonal` 在**同事务**里写归档表 → 1146 → 整批回滚 → 保留期 sweep 永远卡在同一批;
缺索引则让 §9.24 清理走全表扫描且 `dbcheck` 判红。已补齐并在真实 TiDB 上装载验证(18 表)。

### 六、验证

- `pkg` build/vet PASS;`pkg/mysqlx` 单测 PASS;**对真实 TiDB v8.5.1 的集成探针 PASS**
  (`PANDORA_TIDB_TEST_DSN=... go test ./mysqlx/... -run RealBackend`)。
- `owner` / `login` 全模块 `go build` + `go vet` + `go test` PASS。
- 五个 -Prod 契约测试全 PASS(含新增 `gen_cluster_prod_account_contract_test.ps1`,负向用例断言
  **错误文本**而非仅退出码);四个此前一直绿却没进门禁的脚本已补登记进 `ci_backend.ps1`
  的 `$contractTests`(README 表格是文档,那个数组才是 CI 实际执行的清单)。
- 两份 TiDB DDL 均在真实 v8.5.1 上装载 + 属性回读 + scratch 库清理验证。

### 七、未做 / 已知未关闭(不得当成已完成)

- **`gen_cluster_b1_contract_test` 仍红,且是存量红**:基线(stash 掉本次改动)同样失败于
  `缺 placement 分权 key`,失败的服务名在基线与改动后都**随机变化**(该断言遍历的是普通 `@{}`
  哈希表,枚举顺序不定)。本次未引入、也未修复,故未登记进 CI。
- **其余 10 个服务仍用公开 dev 库口令**(见 `$ProdDbCredentialDebt`),上线前必须逐个接线。
- **审计确认但本次未修的 TiDB 锁模型 blocker(实测确凿,均属「先 FOR UPDATE 拿锁、再用普通
  SELECT 读同事务内参与计算的值」这一个共性模式;那个普通 SELECT 必须也加 FOR UPDATE)**:
  `inventory_repo.go:289`(claimLedger 回读)、`inventory_instance.go:193/206`(返回残缺结果而非报错,
  最危险)、`bag_repo.go:158/165`、`player/attribute_repo.go:172`(SUM 非加锁读 → 属性点数算错)、
  `mail_repo.go:414`(`COUNT(*) ... FOR UPDATE` 范围条件 → 上限 TOCTOU 被穿透,应照 friend 的
  `friend_player_guards` 守卫行改造)。每条都需按 §16.6 补「修复前失败、修复后通过」的回归测试,
  未纳入本次范围。
- **审计指出的更高优先级非 TiDB 问题**:`inventory` 的保留期清理每轮**每表只删一批**
  (`sweep.go:31`,500 行/5min × replicas=1 ≈ 144k 行/天)结构性追不平 `inventory_ledger` 写入,
  违反 §8 压测断言;`battle` 已因同一问题改成排空循环,`inventory`/`chat`/`auction`/`mail` 未改。
  **迁存储不改清理速率,积压照样只增不减** —— 这条优先级高于任何「迁 TiDB」讨论。
- 未跑:真实数据迁移(Dumpling/Lightning)、`dbcheck`(其单 `-dsn` 假设与「owner 已在 TiDB、
  account 将在 TiDB」的拆库拓扑冲突,**今天就已经过不了**)、k8s/minikube 无 TiDB 故
  `require_tidb` 路径在本仓任何环境都不会被执行(owner 今天即如此)、`run_services.ps1` 的 login
  仍指向 `login-dev.yaml`(未默认切 TiDB,避免动到本地链路;需要时手动用 `login-dev-tidb.yaml`)。

## 2026-07-28 INC-20260727-001/-002 收尾轮:E2E 首验通过回填 + 清单漂移修复 + race 归档(Claude)

- **状态确认**:三个 P0 修复已全部入库并 live 生效(Go `8abf30a3` 已推送、UE r1570 用户提交、
  四轨 DS 镜像 `*-ds:r1502-dirty-v3-df2478e9c061`、battle 14Gi limits=requests、maxReplicas=2);
  当日 map8 真实客户端 E2E 首验通过(两拍 activation_pending→第三拍提升→battle_ready→进图→
  Battle Admission)。**验收门 A/B/C、pinger 硬门、观察窗口仍 OPEN,两事故均未关闭**。
- **⑯ 部署清单漂移修复(未提交)**:live 四轨镜像已是 v3,但树内 20/21-fleet-battle yaml 仍钉
  上一代 `r1553-dirty-20260727-133010`、30/31-fleet-hub yaml 仍是 `:dev` 可变 tag(P1-4 当时
  漏钉 hub)——apply 即回滚。四行 image 已对齐 live 实测 tag(imageID sha256:4363...) 并补注释;
  live 本就是目标态,无需 apply。
- **race 随 `8abf30a3` 重跑归档**:docker golang:1.26.5-bookworm,data/biz 全量 `-race` +
  `TestBattleAuthActivateConcurrentSinglePromotion -count=100` + `TestBattleAuthActivationStabilityGate
  -count=30` 全部 ok(exit 0)。
- **A6 证据回捞定案**:07-27/07-28 各局原始 allocator 日志确认永久缺失(events TTL 1h、容器多代
  重启、loki 19 天 CrashLoopBackOff 1127 次无聚合);loki 修复列为后续改进项。
- **A10 首批证据**:minikube 节点 PSI 实测 CPU some avg60=79.71(严重饥饿)、memory PSI=0、
  dmesg 无 OOM;当日 login 37 次/player_locator 56 次/ds-allocator 12 次重启、etcd/mysql/redis
  readiness 1s 探针集体超时→摘 endpoints→依赖方启动失败 exit 1 CrashLoop。方向=宿主机共载
  (UE 编译/docker 构建/race 容器与 minikube 同机)CPU 争用,非内存;采样存在 race 容器自污染,
  无负载对照样本待补,A10 保持 OPEN。
- **新增 `robot/stress/cmd/gatecheck`(未提交)**:验收门 A/B 合成驱动——stressbot 同款直连
  (login devAutoRegister→SetLocation(HUB)+20s 刷新→CreateTeam→SetReady→StartMatch(map_id=8,
  走 matchmaker-pve walk-in)→轮询到 READY→保活观测窗口,不连 DS UDP,恰好构成门 A「无客户端」
  场景。已编译通过;待集群从当前 CrashLoop 恢复后执行。
- **档案回填**:INC-001 增 §4.3(第三次部署验证=首验通过,证据缺口如实标注)、§7.2⑯、§8 E2E 行、
  §9 产物、A6/A10 状态、新增 A11(FogOfWar/GameState Ensure P1 候选);INC-002 状态更新为
  「14Gi 已生效,完整一局回归未跑」,"3 台并发"验收口径钳为 2 台(外层 40Gi 实测约束);index.md 同步。
- **续(同日下午,验收门实测)**:门 A **通过**(gatecheck 合成驱动,服务端实收 15 拍/78.1s/最大间隔
  10.6s,Redis ZSCORE 逐拍采样;顺带实证 120s 宽限击穿→有界放弃→自动重试不卡玩家);pinger 硬门
  **stable+canary 双轨通过**(加载阻塞窗口内层单调时钟 5.00s 零断流,60s 摘要 12/12/12/0;canary
  临时 1 副本验毕归零);门 B **3/3 通过**(kill→重试 36/36/45s、kill→READY 96/101/118s,waiter 零
  空转;取值修订:graceful delete 被 30s 终止宽限主导,目标应为 ~45s,18s 样本属 force-kill 场景);
  A3 冷加载实测数据入档(22/48/49~58/84/>120s 五档)。**仍 OPEN:门 C(真实 UE 客户端×3)、观察窗口、
  INC-002 完整一局 memory.peak、A10 定谳(节点 08:20Z 重启原因+无负载对照)、A11 FogOfWar Ensure**。

## 2026-07-28 本地 K8s 闭环批① + 宿主重启事故恢复(Claude)

- **宿主重启事故(当日)**:重启后 Docker Desktop 反复启动崩溃,根因=本机 360 过滤驱动拦死
  AF_UNIX socket 文件的删除/改名(Error 1920,连新建 socket 都删不掉)→ 每次退出残留的
  dockerInference/engine.sock 清不掉,下次启动必崩;连带 settings-store.json 的
  `CustomWslDistroDir` 被崩溃重置为默认 C: 路径,Docker 挂上全新空盘(daemon 零容器,
  像整个集群没了)。**处置**:改名 socket 父目录(%LOCALAPPDATA%\Docker\run 与
  docker-secrets-engine,墓地保留)+ 改回 F:\docker\wsl\DockerDesktopWSL(有 .bak 备份)。
  466GB 数据盘无损,minikube 43d 集群、MySQL 10 库、Agones 3 GS Ready 全部原样恢复,
  31 业务 Pod 收敛 Running。**根治需用户在 360 卸载/白名单 Docker,未做前每次异常退出
  可能须重复绕法**(教训:daemon 空≠数据丢,先查 CustomWslDistroDir 与两处 vhdx;
  绝不能在空 daemon 上跑 minikube start)。
- **P0 镜像/清单对齐(worktree-k8s-closure-20260728 分支 1a276cd)**:services.yaml 三行
  :dev 钉定为 live 实测 tag(matchmaker/matchmaker-pve=geed8ce2c6b5d,
  ds-allocator=geed8ce2-p03-…-062100,注释含 imageID)——re-apply 不再回滚
  INC-20260727-001 修复镜像(与 162bafc1 的 fleet yaml 钉定同一目的收口)。
- **P1 六项 live 问题**:①loki 1174 次 CrashLoop 终结(删 Pod 换新 emptyDir 清坏 WAL,
  1/1 Ready);②20001 断链=bridge 未跑,重启后重建 21 条 port-forward 全 LISTEN(含 login);
  ③redis-master allkeys-lru→noeviction 运行时止血(**容器 args 仍旧,重启回漂;根治=用
  F: 的 docker-compose.redis-sentinel.yml 收养重建,命名卷保数据,按 AGENTS §11.1 由
  用户/Codex 执行**);④孤儿 redis-cluster 栈(rc-node-1..6+rc-init)已按拍板拆除;
  ⑤镜像漂移=上条 P0;⑥监控进集群:新增 deploy/k8s/infra/monitoring.yaml(Prometheus
  kubernetes_sd pod 角色注解驱动 + Grafana 数据源 provisioning,dev-grade emptyDir,
  版本与 compose 一致),services.yaml 21 个 Deployment 全部加 prometheus.io/* 注解,
  start.ps1 接线(apply/Down/genesis 白名单)。live 已 apply:三 Pod Ready,hub-allocator
  抓取点自动发现且 up(其余 20 服务的注解随下次标准部署生效——**不可手动 raw-apply
  services.yaml**,会抹掉部署期注入的 pandora.dev/image-digest 注解)。告警 provisioning
  留 compose 侧(依赖 PANDORA_ALERT_NTFY_URL env,进集群需 Secret 注入,单列待办)。
- **P2 前置(containerd 化,同分支 +a91b28c)**:start.ps1/e2e_k8s.ps1 支持
  PANDORA_MINIKUBE_PROFILE 显式覆盖与 PANDORA_MINIKUBE_DRIVER/RUNTIME/CNI/K8S_VERSION
  拓扑参数(仅新建 profile 生效,既有集群空参续跑);节点内镜像 untag/拉取按 runtime 分流
  (docker CLI vs crictl);镜像存在性/digest 统一 `minikube image ls --format json` 口径
  (docker runtime live 实测与 docker inspect .Id 一致;裸 hex→sha256: 归一修复系 live
  验证抓获)。**containerd 分支待 P2 真集群重建(-Reset)首跑验证**;
  deploy/ds/build-image-minikube.ps1 默认路径依赖 docker-env,containerd 下 fail-fast,
  须 -BuildOnHost。
- **剩余(P2 主体,待用户口令/执行)**:containerd+Calico+钉版本集群重建(-Reset,DS auth
  etcd 权威随空盘重置=genesis 合法路径);边缘 Envoy/TiDB/Sentinel 进集群;退役个人路径
  port-forward 桥;sentinel 栈收养重建;告警 provisioning 进集群。

## 2026-07-28 P2 闭环:containerd+Calico 集群重建 + 边缘 Envoy/TiDB/Sentinel/监控全进集群 + 退役宿主桥(Claude)

- **集群重建(用户拍板"修复到闭环",本地数据可弃)**:minikube delete 后按贴近生产形态重建——
  **containerd 2.2.1 + Calico v3.31.3 + K8s 钉 v1.35.1** + 16C/40Gi(与旧集群实测外层限额一致)+
  阿里云 kicbase + `--ports=127.0.0.1:8443:31443`(节点端口发布,退役桥的关键)。墙内网络三卡点:
  containerd 变体 preload 缺失→逐镜像拉 registry.k8s.io 死路(手动从阿里云镜像源拉 7 核心镜像灌入);
  Calico 由节点自行从 quay 拉齐;Agones 五镜像 us-docker.pkg.dev 墙外(delete 前从旧节点导出,
  含 argocd/dex/storage-provisioner 共 9 tar 存 F:\work\image-export)。
- **全量部署 8 轮迭代,抓获并修复 6 个全新路径 bug**(全部提交 worktree 分支):
  ①分离进程控制台 GBK→kubectl JSON 回读乱码→configtable 写后校验假不一致(修=强制 UTF-8);
  ②fresh-genesis 门:白名单补 sentinel 三名+副本数声明式(replicas 2/3);③同门:Job 类别被拒→
  tidb-init 拆独立文件移门后 [3.6/8];④同门:控制面静态 Pod(etcd-pandora-agones 等)因 profile
  名含 pandora 撞身份正则→按 kube-system+owner=Node 精确豁免 mirror Pod;⑤Hub digest rollout
  断言过时(R11 前 Recreate 时代)→纳入 RollingUpdate+deploy-strategy 注解契约(writerlease 单写者);
  ⑥writer digest provenance 写死 :dev→改按 Deployment 实际引用镜像(ds-allocator 钉定 tag)。
  另:DS 构建防降级门正确拦截制品库旧包(r1553)覆盖 stage 新二进制,用 PANDORA_DS_LINUX_PKG
  指向 Packages 最新包(04:05 构建)通过——门本身工作正常。
- **验证全绿**:节点 Ready(containerd);kube-system 9/9;41+ Pod Running;三 GameServer Ready
  (battle×2/hub×1,canary 归零);**边缘 Envoy 链路实测**:127.0.0.1:8443→NodePort 31443→
  pandora-edge-envoy(TLS 握手回 mkcert dev 叶子,客户端地址不变);e2e 自动检测集群内边缘并
  **跳过宿主桥**(零 port-forward,兜底回落逻辑保他人路径);**Prometheus 22/22 targets 全 up**
  (21 服务注解全量生效,监控闭环);Sentinel master+2 replicas 全被哨兵识别;TiDB v8.5.1 三库
  (account/owner/social)schema 落库(tidb-init Job Complete);Argo CD v3.4.5 重装+app 注册。
- **TiDB 自引导鸡生蛋(live 抓获)**:pd 经 advertise(pd:2379)拨自己,Service 默认只发布 Ready
  endpoints→CrashLoop 死锁;修=pd/tikv/tidb 三 Service `publishNotReadyAddresses: true`
  (K8s 自引导标准解法);半状态残留(duplicated store)须三件套同批回收重引导。
- **退役与清退**:宿主 compose envoy+21 条 port-forward 桥退役(脚本保留,自动回落);孤儿 compose
  tidb/sentinel 栈容器移除(命名卷保留);udp-relay 重建指向新节点。
- **残余/交接**:告警 provisioning 需带 PANDORA_ALERT_NTFY_URL 重跑部署段(或手动创建 configmap);
  局域网多机需重建时用 0.0.0.0:8443:31443 端口发布或 -HostBridge;客户端(UE)真机验证=门 C 待用户;
  worktree 分支 worktree-k8s-closure-20260728 待合 main(主树有已同步的未提交副本)。
- **功能性终验(gatecheck 合成 E2E,新集群)**:login→CreateTeam→StartMatch(map8)→QUEUEING→
  ALLOCATING→**READY t+196.1s**(ds 192.168.2.28:7541)+30s 观测窗干净退出。首跑 4 分钟超时属
  全新节点零页缓存首冷载(首台分配被弃→授权重试链正常运转,不卡玩家链实证);二跑一次通过。
  TiDB tidb-init 二次 Job Complete(三库 schema);孤儿 compose tidb/sentinel 栈容器已清退。

## 2026-07-28 换机器可移植性审计整改(批③,合 main 前门禁)(Claude)

36 代理四维审计+逐条对抗验证(11 条驳回),确认缺口全部整改:
- **钉定镜像产出链**:services.yaml 三个 go 服务回归 :dev(修复已入 main,临时钉定动机消失;
  服务面 re-apply 不换 Pod 无回滚面);fleet 钉定 tag 改为「tag=制品版本」契约,宿主缺失时
  Build-DsImagesForMinikube 按 PANDORA_ARTIFACT_ROOT 定位同版本制品自动重建,制品缺失才
  fail-fast——新机器可复现,绝不静默退回 :dev。
- **证书链修真**:[7.5/8] 原以 -File 调 envoy_cert.ps1(纯函数库)是空操作;改 dot-source +
  Confirm-EnvoyDevCert 真实重签;k8s 前置工具补 mkcert 与 Go(原缺,分别在 [7.5/8]/[5/8]
  中途才炸)。
- **DS 包前置预检**:-ResolveOnly 增 exit 2 契约(彻底无源),Invoke-K8s 在起集群前预检并
  给三条可执行出路,不再让新机器等到 [4/8] 才失败。
- **bridge dev.env 自举**:缺失时自动从 example 初始化(dev 默认值),回落路径不再因未入库
  文件断头。
- **离线清单同步**:export_images 补 pingcap 三件套 + envoy v1.38.1。
- **边缘 Envoy 钉定 v1.38.1**(与 latest 当日同 imageID,防 latest 漂移;[7.5/8] 负责 load)。
- 文案纠偏:fleet :dev 陈述、16Gi 门机理(cgroup OOM 而非调度 Pending,审计实测容量 47Gi
  vs 限额 40Gi 的口径错位)。

## 2026-07-29 INC-20260729-001:ds_allocator 单副本重启打断全部在场对局

事故档案:[docs/incidents/2026-07-29-p0-ds-allocator-single-replica-restart-kills-battles.md](docs/incidents/2026-07-29-p0-ds-allocator-single-replica-restart-kills-battles.md)(未关闭)

- **定性**:节点落盘 I/O 卡顿(etcd WAL `fdatasync` 39.4s)只是触发条件;结构性根因是
  ds_allocator `replicas:1 + Recreate` + 整进程被 capability 门控失租即 `os.Exit(1)`,
  于是**任何重启(含例行换镜像)都让 Heartbeat 断流 160s**,远超 Battle DS 的 20s 授权租约
  (`pkg/placement.DSFenceLeaseMaxSeconds`)→ DS 自我 fencing 踢掉在场玩家。
  `§16.8` 重启预算不闭合、验收底线 7「升级不得打断对局」被破。**无数据丢失/回档/双写。**
- **修的是可用性,不是 fencing**:capability key 按 `(service, PodUID)` 唯一,异 Pod 副本
  各持异 key,多副本本就合法;唯一需串行的是心跳超时扫描。
  - `deploy/k8s/services/services.yaml`:ds-allocator → `replicas:2` +
    `RollingUpdate(maxSurge 1/maxUnavailable 0)` + `PodDisruptionBudget minAvailable:1` +
    跨节点 preferred 反亲和 + 补声明 `containerPort 21020`(此前 prometheus 注解指向它却无端口)。
  - `internal/biz/allocator.go`:新增 `SweepWriterLease` + `sweepIsLeader`,`RunHeartbeatSweep`
    与首扫都过领导权门;**明确写清为什么 sweep 不需要存储级 fencing token**(它不携带跨轮次
    权威意图,每轮从 Redis 重算,写是同事务 CAS;`sweepDeferUntil`/`ownerAdmitted` 已是非权威
    调度提示与可重建缓存)——这是 `§9.21`「单写者循环选举」与「可并行 worker 幂等 CAS」的分界。
  - `cmd/ds_allocator/main.go`:接 `pkg/dsauthfence/writerlease`(`election=ds_allocator/sweep`,
    无 `OnElected`)+ 机械门禁(`RollingUpdate × mode!=enforce` 与受管 k8s 内缺
    `PANDORA_DEPLOY_STRATEGY` 均 fail-closed)。
  - `internal/conf/conf.go`:`allocator.writer_lease_mode`(enforce/warmup/off,空=enforce,
    非法值 fail-fast),与 hub_allocator 同一档位语义。
  - `internal/server/http.go`:`/healthz/writer` + 6 个 `pandora_ds_allocator_writer_*` 指标;
    **不接 readiness**(热备副本必须继续服务 Heartbeat,否则等于没有多副本)。
  - `deploy/grafana/.../rules.yaml`:新增 critical `pandora-ds-allocator-no-sweep-writer`
    (`sum(...writer_held)==0` for 1m),覆盖「补偿链停摆但 RPC 全正常」这种外部无感故障。
- **观测缺口修复**:`pkg/dsauthfence` 的 `signalLost` 带 reason(6 个分支常量)+ `LostReason()`,
  5 个服务的 `ds_auth_fence_lost` 全部打印。此前五个分支在日志里完全同形,本次取证卡死在这。
- **UE 客户端(待用户编译)**:`UMyDsRecoveryCoordinator::AuthorityWaitDeadlineSeconds` 是绝对
  时刻且只在 `==0` 时武装,而清零只覆盖 4 个 `Operation` 复位点中的 2 个 —— 两处 admission
  确认终态漏配对,上一轮残留的过期 deadline 被下一轮断线继承,0ms 即误报「已等满 30s」
  (事故当天弹窗提前 35 秒)。已在两处补 `ResetAuthorityWaitState()` 并把不变量写进头文件。
- **测试**:新增 13 个用例(fence 原因 6 + 扫描领导权门 3 + 档位解析 1 + 清单契约 3),
  ds_allocator / dsauthfence 两模块 `go test ./...` 全绿,4 个受影响服务模块 build+vet 绿。
- **未做/阻断**:`go test -race`(本机无 gcc,须 Linux/CI 补跑)、滚动升级故障注入与玩家 E2E、
  UE 编译、未 commit 未构建未部署;宿主机 360 白名单与 vhdx 迁盘需用户操作。
  ⚠ 首次滚动必须两步走(先 Recreate 换镜像,再单独 apply strategy/replicas),否则旧二进制
  不参与选举 = 无保护并发扫描窗口。

## 2026-07-22(续14):库容量守护 + 大字段排查体系(§9.24 深度侧,Claude)

- 用户要求:①服务初始化时超上限打日志去查;②"某一格子数据过大就证明有问题",问标准做法。
- **核心结论(已写进 §9.24)**:增长有两个**独立**失控方向,只管一个必漏另一个 ——
  广度(行数变多,信号 TABLE_ROWS,闸=保留期清理)/ 深度(单行变胖,信号 AVG_ROW_LENGTH,
  闸=写入侧上限)。本仓两个方向各错一个:bag 管住 items 条数却没管单个 item 的 attrs
  (深度无闸);rewardclaim 管住单条位图大小却没管位图条目数(广度无闸)。
  故 blob/JSON/CSV/位图列必须同时具备三闸:单元素上限 + 集合条目上限 + 整体字节上限。
- **实测定性(真 MySQL 8.4)**:非严格 sql_mode 下往 VARBINARY(16) 写 100 字节
  err=nil 且只存 16 字节(静默截断=无声数据损坏);严格模式 Error 1406 拒写。
  全仓此前**零处**校验 sql_mode。
- 落码:
  - **pkg/dbguard** 新包:AssertStrictMode(启动 fail-fast,唯一允许拒启动的库检查)、
    Guard.Check(information_schema 估算,毫秒级不锁表,禁 COUNT(*);超预算 ERROR 日志 +
    metric,**不阻止启动**)、CheckColumns/TopLargeRows(定位)、CheckPayload(写入侧三档:
    超限拒写 / 达 80% WARN 留排查窗口 / 否则静默)+ 6 个 Prometheus 指标
    (强调 payload histogram 看 p99 而非 max:p99 正常+max 爆=个例,p99 齐涨=设计问题)。
  - **P1 修复**:pkg/rewardclaim 位图条目数无上限 —— ClaimReward 是客户端可直调
    (Envoy 不在 403 名单)、source/activity_instance_id 无配置表白名单,每换一个值就永久
    新增一条位图(单条最大 128KiB),落进 LONGBLOB(4GB)DB 层不设防;约 3.3 万次调用即可
    撑到 4GB,而每次领奖都全量 load+marshal,几 MB 后先打爆的是服务与带宽。
    修:MaxPermanentSources=64 / MaxActivityInstances=256 / MaxSourceNameLen=64,
    **只拦新增条目**(已有条目继续可领,存量超限只冻结不报错 —— 否则=回档);
    触顶 ERROR 留证 + 客户端只拿 ErrRewardUnknownID(不外泄内部上限);4 个子测试锁定。
    根治方向已注明:切 ClaimPermanentByID + BitIndexMap 配置表白名单。
  - 接线 3 服务:inventory(trade+bag 双库严格模式断言 + 容量巡检)、player(LONGBLOB 表)、
    mail(buildPayload 加序列化后字节兜底,签名加 ctx);budgets.go 声明各表预算
    (口径:有保留期表=峰值速率×保留期×3;MaxAvgRowBytes 按 blob **设计期望**定不按类型上限定)。
  - **dbcheck -size-check [-top-rows N]**:登记 9 个大字段,输出 rows/max/avg/预算/超限行数,
    按超限程度排序 + 给出"超了该查什么",超阈值自动列 Top-N 主键。
- 文档:**docs/design/db-capacity-guard.md** 排查 runbook(5 步:确认变胖还是变多 → 定位到列
  并判个例/普遍 → 定位到行 → 反序列化定位到字段 → 回写入路径补最里层缺的那个闸)+
  已知缺口表 + 新增表检查清单。CLAUDE.md §9.24 与 AGENTS.md §10 同步扩写。
- 审计:workflow 6 域 21 候选 → 7 确认(全 P2,验证者逐条给出降级论证)+ 14 驳回;
  **reward-claim agent 挂了,该域由我自己复核并查出上述 P1**(印证"挂掉的 agent 必须自查")。
- 验证:pkg + 7 服务 build/vet/test 全绿;sql_mode 截断行为已在真 MySQL 8.4 实测。
  **未跑**:-size-check 的真库验证(本机 Docker Desktop 挂了,TiDB/MySQL 容器均不可用),
  SQL 是标准 MAX/AVG/COUNT,风险低但如实记为待办。

## 2026-07-22(续15):dbguard 全服务接入补齐(12/12,Claude)

- 自检发现自相矛盾:上一轮往 AGENTS.md 写了红线"连 MySQL 的服务必须启动时断言 sql_mode",
  但自己只接了 3 个服务(且 mail 只接了写入侧 CheckPayload,没接断言)。本轮补齐到 12/12。
- **严格模式断言 12/12**:新增 pkg/dbguard.AssertStrictModeStartup(自带 5s 超时,免各 main
  重复写 context.WithTimeout 三行;不在包内 os.Exit——各服务退出约定不同,只统一"怎么探测")。
  接入 login / battle_result / data_service / leaderboard / chat / friend / mail / guild /
  owner / auction(前 6 个批量 perl 注入统一锚点,后 4 个结构特殊单独处理)。
  **auction 逐分片断言**:各分片可能连不同 MySQL 实例,配置漂移可能只出现在个别分片,只查一个会漏。
- **容量巡检 12/12**:新建 10 个 internal/data/budgets.go(每表都写了预算推导依据与"超了该查什么"),
  各 main 加 runCapacityGuard(启动跑一轮拿基线 + 每小时一轮,safego 兜 panic,超限只 ERROR 不阻断)。
  两处接法要点:①pandora_social 是 chat/friend/guild/mail 共用库,**各服务只声明自己负责的表**
  (否则同表被四服务重复告警、处置责任不清);②auction 逐分片各建 Guard,预算按单分片量级给。
- 文档同步:db-capacity-guard.md §2.1 补覆盖状态与两处接法要点。
- 验证:pkg + 12 服务 build/vet/test 全绿(35 测试包 ok,0 FAIL);tools/migrate 编译通过。
  未跑项同续14:dbcheck -size-check 的真库验证(本机 Docker Desktop 仍不可用)。

## 2026-07-22(续16):保留期清理改为默认只报告不删(用户指令,Claude)

- 用户指令:「你不能清理我的数据,只能打印日志」+ 澄清范围「原来像道具超时删除、过期删除
  那种要保留,不允许的是我这个会话里为了『数据大了』新增的删除代码」。
- **范围划定(关键)**:只改本会话续11~续15 新增的**运维语义**清理;**业务语义**删除一律不动
  ——mail 过期清理 / bag_journal / owner_transition_log / friend_pair_guards(均非本轮新增)、
  道具过期、挂单置 EXPIRED、玩家丢弃/解散公会、出箱投递成功即删,全部保持原样。
- **pkg/dbguard/sweep.go** 新机制:
  - Mode 零值 = ModeReportOnly(只 COUNT + WARN 告警 + pending gauge,一行不删);
  - ParseMode 对无法识别的值**报错而非猜成 delete**(拼错一字母就删生产数据不可接受),
    各服务 ValidateRetentionMode 供启动 fail-fast;
  - **SweepTable 强制 Count 与 Delete 共用同一 where**(条件只写一遍,从机制上排除
    "报告说 0 行、实际删 10 万行"的漂移);多表事务场景用 ReportPending/ReportDeleted 保持同口径。
- 8 处改造(全部默认 report_only):inventory(ledger+escrow)、battle_result(battles/stats +
  progress 组,report-only 只跑一轮不空转)、chat(私聊历史)、friend(终态请求)、
  guild(终态申请)、auction(逐分片三表)、leaderboard(snapshot+GRANTED)、login(设备行)。
  各服务 conf 加 retention_mode(留空=report_only)。
- **dbcheck 移除真删能力**:-force-sweep/-confirm/-batch 整块删掉,换 -pending(只 COUNT
  报告待清理量);**刻意不留同名 flag**——留着改语义会让按旧文档敲的命令静默变行为。
- 测试:pkg/dbguard 加 ParseMode 安全语义单测(默认/拼错/零值全覆盖);inventory 集成测试
  新增 ReportOnlyDeletesNothing(**保留期给 0 天极端条件下仍一行不少**);battle 加
  DefaultsToReportOnly + MistypedModeFallsBack;inventory biz 加同类。
- 规范:CLAUDE.md §9.24 开头改写(默认只报告 + 业务/运维语义删除对照表)、§8 压测句;
  AGENTS.md §10 红线;db-capacity-guard.md 加 §0.0;stress-discipline.md §4.3 去掉
  "批删速率抽测"改 -pending。
- 验证:pkg + 12 服务 build 全绿,36 个测试包 ok / 0 FAIL,tools/migrate 编译通过。

## 2026-08-03:战报保留期改为六个月且默认真删(用户指令,Claude)

用户口径:"战斗结算数据库不会无限大啊?玩家战报在 MySQL 最多只存最近六个月,超过六个月的
在 MySQL 里面就应该没有数据了"。

- **改前现状(确实会无界增长)**:清理任务早就有(`biz/retention.go`),但两处让它实际不删:
  ① `history_retention_days` 硬钳 ≤90;② `retention_mode` 留空 = `report_only`
  (2026-07-22 用户指令的全局默认)—— 所以线上只有 WARN + `pandora_db_retention_pending_rows`
  在报待清理量,一行没删。
- **改后**:`battle_result` 域按域覆盖全局默认 ——
  - `history_retention_days` 默认 180(六个月),钳 `[30,180]`
    (`conf.HistoryRetentionMaxDays/MinDays`;下限是真删域的手滑护栏);
  - `retention_mode` **留空即 `delete`**(其它域仍 report_only 不变);
  - `main.go` 补上 `ValidateRetentionMode` 启动 fail-fast —— 之前定义了却没人调,
    拼错 "delete" 会让六个月口径静默失效;
  - `budgets.go` 容量预算改为直接引用 `conf.HistoryRetentionMaxDays`(不再抄一份 90),
    保留期一改预算跟着改,杜绝告警恒响/形同虚设;
  - dev yaml + prod example 显式写出 180 / delete + 首次开删纪律(先 `dbcheck -pending`
    看积压,低峰期发布)。
- **口径提醒**:保留期同时是玩家战报可见窗口(`ListPlayerHistory` 只读 MySQL,无冷存归档);
  运营/客服要更久必须另做归档,不是加大这个数。
- 测试:`DefaultsToDelete`(留空必须真删并排空积压)、`ExplicitReportOnlyStopsDeleting`
  (刹车仍在)、`HistoryRetentionDaysClamped`(0/-1/365/180/90/1)、
  `RetentionModeDefaultsToDelete`(含拼错拒启)、prod 模板断言 180+delete。
  `go build`/`go vet` 全绿,conf + biz 测试包 ok。
- 规范同步:CLAUDE.md §9.24 开头加"按域覆盖"段 + 登记表 battles/progress 两行改 180 天例外;
  battle_result README 保留期章节与配置表。

## 2026-08-03(续1):ValidateRetentionMode 启动 fail-fast 补齐 7 服务(Claude)

§9.24 原文写着"启动 ValidateRetentionMode fail-fast",但全仓 grep 显示该方法**只有定义、
没有任何调用方** —— 拼错 `retention_mode`(如 "delet"/"true"/"1")时 `RetentionMode()`
静默回落 report_only,运维以为开了清理、实际一行没删,库继续增长且启动期毫无痕迹。
battle_result 更危险:它 2026-08-03 起默认 delete,拼错会静默关掉"战报只留六个月"。

- 接线(全部在 `cfg.Defaults()` 之后、连 MySQL 之前,Errorw + os.Exit(1)):
  chat / friend / guild / leaderboard / auction / inventory 各自 main.go;
  **login 挂在已有的 `Config.Validate()` 里**(main 本就调它,不新增第二个调用点)。
- 单测:6 个服务新增 `internal/conf/retention_mode_test.go`(留空=report_only 且过校验、
  delete 生效、"Delete " 大小写空白归一化接受、拼错必须 err≠nil 且绝不猜成 delete);
  login 在既有 conf_test.go 加同名用例,走 `Config.Validate()` 断言整体拒启。
- **真跑验证**(不是只看编译):7 个服务各自用 dev yaml 注入坏值启动,全部在触碰 MySQL 前
  exit 1 —— chat/friend/guild/leaderboard/auction/inventory 打 `*_retention_mode_invalid`,
  login 打 `config_validation_failed`,err 均为 dbguard 的"无法识别的 retention_mode"。
- go build / go vet / go test 七个模块全绿。gofmt 另报 6 个**本次未改**的文件不规范
  (guild data/cache*.go、login data/match_client*.go + service/login.go),疑为并发编辑者
  在改,未动。

## 2026-08-04(续):ds_allocator 关卡影子配置消除 —— 改读策划关卡表

- **起因**:`map_id=11` 玩家侧恒"排队中"。链条 = `ds_allocator-dev.yaml` 的 `local_ds.maps`
  只手抄了 6/7 两行 → 未命中回退默认图 MobaLevel → DS 侧关卡门判「已加载世界 ≠ 注入 map_id」
  → fail-closed 自杀 → 分配卡到 `ready_wait` 超时。关卡表里 `category=4` 实有 8 张(4/5/6/7/8/9/10/11)。
- **治标**(当天早些时候):8 张全补进 `maps` 并逐个校验 `.umap` 存在。
- **治本**(本次):`maps` / `map_name` 整块删除,`ds_allocator` 接 `pkg/configtable`,起本机战斗 DS 时
  按 `map_id` 现查 `g_关卡.xlsx`(`asset_path` + `game_mode_class`)拼关卡 URL,换算规则与 UE 侧
  `PandoraDSLoaderGameMode::BuildTravelURL` 逐字一致(剥 ObjectPath 对象名;`game_mode_class`
  为空则不拼 `?game=`)。**查不到 map_id → `Allocate` 直接失败且不占端口不拉进程,绝不回退兜底图。**
- `mode=local` 现在必须配 `config_table.dir` 或 `local_ds.loader_map`,两者皆空启动即拒
  (`conf.ValidateLocalMapSourceConfig`)。`mode=agones` 一字未动(那条路本就由 DS 侧 Loader 查表)。
- 批次级校验器:`category=BATTLE` 每一行都必须能拼出合法 URL,坏批次整批不切换保留旧表(§9.15);
  同时注册通用 `configtable.AdminService`,**新增副本改表重导后 reload 即可,无需重启 ds_allocator**。
- 验证:ds_allocator 全模块 + `pkg/configtable` build/vet/test 全绿;真实 dist 逐张比对 8 张战斗图
  URL(7 张与旧 yaml 手抄值逐字一致,`map_id=5` 因表里 GameMode类 留空而不再拼 `?game=`);
  实机重启后 `configtable_loaded version=20260804002 levels=11`、`map_source=config_table`;
  热更 RPC `ReloadConfigTable` 在 :20020 实测可用。
- 仍未验证:§2.6 验收标准的玩家侧那一半(加一行 → 重导 → 不重启 → 真人进图)。
  详见 `docs/reviews/交接-消除ds_allocator关卡影子配置-20260804.md` §9。

## 2026-08-04(续):策划日常循环加「只重启 DS」快速通道(-DsOnly)

- **起因**:策划一天里绝大多数改动只在客户端仓(改资源 / 重编编辑器 DLL),go 服务一行没动;
  但唯一的入口 `策划一键启动-改资源即时生效.cmd` 每次都要走「等基础设施 healthy → TiDB →
  数据库迁移 → 21 个 go 服务逐个 build/起/端口探活」,这些步骤与改的资源全无关系,纯白等。
- **落地**:`start.ps1` 新增 `-DsOnly`(仅 `-Mode local`)+ 根目录 `策划一键重启DS-读最新资源.cmd`。
  只做三件事:①重新解析 DS 形态(顺带 `Assert-PandoraEditorModulesMatch` 血统校验,刚重编过
  编辑器 DLL 最容易在这里翻车);②杀掉在跑的本机 DS(editor 形态在**进程启动时**读未 cook 的
  `Content/`,老进程内存里还是旧资源);③重启 `hub_allocator` / `ds_allocator`。
  **必须连 allocator 一起重启**:常驻 Hub DS 是 `local_fleet.go ensureStarted` 的 `sync.Once`
  懒拉起,一个进程内只拉一次,光杀 DS 没人再拉起来,表现成"登录后永远进不了大厅"。
- **刻意的取舍**:①排在 `Resolve-Prerequisites` 之前 —— 后端正跑着时那套前置要么白跑要么语义
  不对(8443 正被自己的 Envoy 占着);②后端没在跑时返回 `$false` **自动回落完整启动**,双击的人
  不需要分辨今天该点哪个图标;③`-Mode` 非 local 或与 `-Down/-Resume/-Reset/-BuildOnly` 同用直接
  throw,k8s/online 路径零改动(主流程只加一行且 `-and` 短路);④走 `-NoBuild`(go 没改是本入口
  前提),二进制缺失时 `run_services.ps1` 仍会自己补编;⑤`Initialize-LocalEdgeBinding` 必须重跑,
  否则重启后的 allocator 回落 yaml 写死值,局域网客户端拿到回环地址。
- **验证**:AST 解析通过;两条 guard 实测 throw+exit 1(`-Mode k8s -DsOnly`、`-DsOnly -Down`);
  真机跑通,~15s 完成,日志 `local_hub_fleet_provider_ready launcher=editor
  project=...Pandora.uproject executable=...UnrealEditor.exe advertise_host=192.168.2.28`
  → editor 形态与局域网 advertise 均正确继承,随后 `local_hub_ds_started` 拉起新 Hub DS。
- **已知代价**(文档已写明):本机正在进行的战斗会被中断(战斗 DS 是 `ds_allocator` 子进程);
  改了 go 代码 / 换了 `run/artifacts` 二进制必须回到完整启动。

## 2026-08-05:协议原则 5 落档——推送不承担正确性,权威态必须可查

- **起因**:用户问「刷新状态(比如匹配状态)是不是该客户端主动去拉?后端有些特殊操作可能
  通知不到客户端」。结论:标准做法不是二选一,而是分工——push 负责变更提示与低延迟,
  **pull(权威查询 RPC)才是真相源**;客户端不允许存在"只能靠 push 才知道结果"的状态。
- **落档**:`docs/design/protocol-ordering-rules.md` 补 **原则 5**(+ 5-A 两种 apply 模型、
  5-B at-least-once 判重)、**§5.4 五个刷新触发点**(界面进入 / push 重连 / 切前台 /
  `pandora.push.resync` / watchdog)、**§12 落地现状与缺口**(含 §12.0 A/B/C 三档适用判定表)。
  §7 加 6 条反模式禁令,§8.3 加 5 条 review 清单项。`pandora-arch.md` §11 登记决策行。
  **原则 1~4 治乱序、原则 5 治丢失,是两种失效模式,做完前者不代表推送可靠。**
- **顺带修正三处旧内容**:①§5.3 "按 `ts_ms` 去重"只挡旧帧、**挡不住重投**(同一业务事件重投会
  拿到更大的新游标),判重必须按业务 ID;②文档标题与 §9 的"4 个原则"改为 5 个;
  ③原则 5 的子编号与既有 §5.1/§5.2 撞号,改为 5-A / 5-B 并同步 4 处引用。
- **核查推翻了一条旧结论**:UE 侧 `pandora.push.resync` **已经接了**(Match / Team / Friend /
  Guild / DsRecoveryCoordinator),不是待办;匹配域已是完整参考实现(resync 回源 +
  `MatchProgressPollTimer` 有界轮询、拿到 `battle_ds_addr` 即停表 + `TeamMatchStandbyTimer`
  watchdog),新域照抄这三件即可。
- **剩余缺口(§12.4,均未动代码)**:①`MyPlayerProgressionModel` 只认
  `pandora.player.experience`、其余 topic 原样忽略 → **没接 resync**,漏帧后等级 / 经验条停在
  旧值直到下次登录拉快照;②五个触发点未逐域核对(只确证匹配域有有界轮询兜底);
  ③聊天域客户端尚无 push 消费者,接入时须同时接 resync + `PullHistory` 并按 `message_id` 判重;
  ④`presence.update` 无消费者、`system.notify` 无 proto 无 producer,接入前不算缺口。
  以上为对 HEAD 的**静态核查**,未经编译与真机验证(UE 编译归用户,`CLAUDE.md §11.6`)。
- **待确认**:`CLAUDE.md §11.7` 引用的 `Pandora-Client-SVN/CLAUDE.md`、§16.10 引用的
  `F:\work\CLAUDE.md` 在当前工作副本中**都不存在**(`Test-Path` 均 False)。客户端侧对应条款
  因此无处可加;按惯例规则要双仓同步,需要用户确认这两份文件是被移走还是未纳入工作副本。

## 2026-08-05:配置表通用投影查询 `pkg/configtable/query.go`(Claude)

**起因**:「`level_table.gen.go` 该不该加『取全部表 id』『取某主键列全部取值』的接口」。
结论是**不进生成器模板**——这两件事只依赖「每行都有 uint32 主键」这一条全表共有形状
(`configtable-gen` 的 `discover.go/buildDef` 强制,§5.6),泛型一次写完即覆盖全部 23 张表、
加新表零成本;生成逐表副本除了模板变长、每次改动都要全量重生 + 过 `TestGeneratedFilesUpToDate`
之外没有收益,而且会在第一个消费者出现之前先摊 23 份死代码(§15.2 / §15.3)。**生成器模板
一个字未改**,`tools/configtable-gen` 全部单测仍绿。

**落码**:新增手写文件 `pkg/configtable/query.go` + `query_test.go`,三个泛型函数——
`IDs(rows)` 全部主键(加载序)、`Values(rows, get)` 某列投影(加载序、不去重)、
`DistinctValues(rows, get)` 某列键集合(升序去重,`slices.Sort`+`slices.Compact`)。

**两个刻意的设计取舍(别按「更现代」改回去)**:
- **不返回 `iter.Seq`**。Go 1.23+ 标准库(`maps.Keys`)确实转向 iterator,但在本项目的快照
  语义下它更危险:`seq := IDs(tb.Level.All())` 惰性求值,存进字段后每次 range 都**看起来**
  新鲜,实际永久钉在旧快照上;切片存下来还长得像「一份旧快照」,iterator 长得像「一个查询」。
- **返回调用方独占的新切片,不返回预计算共享切片**。共享切片只能把只读约定压在注释上,
  调用方 `sort.Slice(t.IDs())` 就会原地打乱表内部顺序(行切片没人排序,ID 切片很容易)。
  代价是每次一次分配:最大表 `spawn_point` 402 行(manifest `v20260804002`),全表合计约 900 行,
  量级微秒,且这些 API 都不在每帧 / 每 tick 路径上——真热路径是 `ByID()` 的 O(1) map 查,本文件
  完全不碰。**判据:一旦某调用点进了逐帧路径,改成 `new<X>Table` 构建期预计算**,那时零分配
  才值钱,也才值得承担共享切片的所有权风险。

**热更(§9.15)**:helper 纯读传入的行切片,不碰 `Store`、不缓存。`Store.Load` 是整批新建 +
`atomic.Pointer` 换指针、旧批次永不原地改,所以结果是「取样那一刻的快照」——不会读到撕裂数据,
也不会自动跟随热更。调用方纪律不变:每次请求开头取一次 `store.Tables()`,请求内用同一个 `tb`。

**已验证**:`go build ./configtable/...`、`go vet`、`go test ./configtable/...` 全绿;
`tools/configtable-gen` 全部单测绿(含 `TestGeneratedFilesUpToDate`,证明生成产物未被牵动)。
**未验证**:`go test -race` 因本机无 gcc 未运行(§16.7 记为阻断项,不计入已验证)。

**同源同步**:mmorpg(`D:\luyuan\mmorpg`)按同结论落了 `go/shared/tablequery`,并顺带修了
导表器三处问题(bit_index 同目录双 package、TableManager 快照改 `atomic.Pointer`、deploy
陈旧产物检查),详见该仓 `PROGRESS.md` 2026-08-05 条目;那边的 Python / Jinja 改动本机跑不了
导表器,已列出 Codex 执行清单。

## 2026-08-05:`-DsOnly` 补「等 DS 真就绪」+ 改名(原名承诺了做不到的"即时")

- **改名**:`策划一键重启DS-改资源即时生效.cmd` → **`策划一键重启DS-读最新资源.cmd`**。
  原名自相矛盾:这条路恰恰是"必须重启 DS 才生效",叫"即时生效"会让人以为存盘就好。
  老的启动/停止两个脚本名不动(它们的"改资源即时生效"指的是**免出包**,成立)。
- **顺手澄清一个术语**:editor 形态的 DS **不是 listen server**。两个 allocator 的 `buildArgs`
  都无条件拼 `-server`(`local_fleet.go:257` / ds_allocator `local_allocator.go`),NetMode 恒为
  `NM_DedicatedServer`;策划是另开客户端走 Envoy→login→大厅连过来,两个进程。旁证:这形态
  **不编 shader**,而 listen server / PIE 必编。准确叫法是 **editor server / 未 cook 形态 DS**。
- **新增 `Wait-LocalHubDsReady`**:重启完 allocator 后等到 Hub DS 真能进才退出。
  - 判据 = DS 把 NetDriver 端口绑上(UE 专服在 `LoadMap` 走完后才 `Listen`,绑上 ⇒ 关卡已加载完)。
    端口**不写死也不读 yaml**,从 DS 自己命令行的 `-port=<N>` 上读(allocator 拼的就是它)。
  - DS 识别判据与 `run_services.ps1` 的 `Test-IsLocalDsProcess` 逐字同源(进程名 + `-server` +
    `?game=/Script/Pandora.`),绝不误伤策划手工开的编辑器。
  - 每轮确认 DS 进程还活着:血统不匹配时 UE 想弹"模块过期"对话框但 headless 弹不出来会直接退,
    这是本模式最难查的坑,不能干等到超时才说话。
  - 超时 300s,与 allocator 给 editor 形态放宽的 ready_wait 同源(不另拍数字);**到期不假装成功**,
    点名 DS 日志目录并让脚本退非零码(§16.10)。窗口绿了 = 能进大厅,这个契约不靠人读滚动输出。
- **实测(热机)**:allocator 回来 ~10s → DS 进程被拉起 ~18s → 监听 ~44s;二次运行端到端
  42.7s、rc=0。**关键发现:Hub DS 不需要有人登录就会自己起来** ——
  `reconcileShardTopology` 挂在 sweep ticker(dev 5s)上,每跳调 `fleet.ListShards`,
  正好触发 local provider 的 `sync.Once` 懒拉起。所以加载全程在后台并行,等它是划算的。
- **修正一条昨天的夸大**:`-DsOnly` 省的是 go 侧那 30~60s,DS 自身加载(热机 ~26s /
  首次进新图更久)省不掉。总时长改善约 1/3,不是"秒重启"。
- **仍未做(已记录,非本次范围)**:①DS 启动第一步在跑 UnrealBuildTool `-Mode=ValidatePlatforms`
  (拉 dotnet 做 SDK 平台校验),对 headless DS 无意义,`local_hub.extra_args` 是现成注入口,
  能不能跳掉**待实测**;②滚动预热双 DS(新 DS 后台加载完再切分片)可把等待压到 ~0,需改
  `local_fleet.go` 的单实例 `sync.Once`;③先量策划改动构成——若多数是数值,正解是搬进
  configtable 热更(`ReloadConfigTable` 已可用,不需重启任何东西),而不是继续优化重启速度。

## 2026-08-05:服务端口整段搬到 49152 以下(gRPC 20001-20022 / HTTP 21001-21022)

- **起因(真实事故)**:开机后 21 个 go 服务全部启动即退,日志 `app_run_failed ... bind: AccessDenied`。
  实测判据:`50001` 可绑、`51001`/`51022` 绑不上、`51100` 又可绑 —— 不是"端口被别的进程占",
  是**被系统整段保留了**。`netsh int ipv4 show excludedportrange protocol=tcp` 显示
  Hyper-V 本次开机动态占了 `50949-51048`,把 HTTP 段整个吞掉。
- **根因**:Windows 动态端口范围默认 **49152-65535**(本机实测 Start 49152 / 16384 ports)。
  原来的 `50001-50022` / `51001-51022` **全在这个范围里**,每次开机都可能被 Hyper-V/WSL 抢走一段,
  位置还每次不同。gRPC 段当时之所以没事,只是因为早先有人给 `50000-50059` 做过一条
  `netsh add excludedportrange ... store=persistent` 的管理员保留 —— 一直站在雷区里垫了块沙袋。
- **为什么不选"再加一条 netsh 保留"**:每台机器都要做一次、会被系统更新/重置网络冲掉、
  新策划机上线必忘,而且"保留了哪些段"是不在版本库里的隐性环境依赖(违反 §15)。
  搬到动态段以下之后**任何机器开箱即用,不需要任何 netsh 操作**。
- **落地**:`\b50(0[0-2]\d)\b→20$1`、`\b51(0[0-2]\d)\b→21$1`,**215 个文件 / 1081 处**。
  行级豁免 8 处假阳性(道具 ID `stackItem(50001,1)`、队伍 ID `uint64(51001)`、
  proto 扩展号值域注释 `[50000, 99999]`);`robot/logs/` 历史压测日志刻意不改(§8 既成记录不可篡改)。
  端口权威 `docs/design/infra.md §6.2` 已补写**为什么必须留在 49152 以下**,并明令
  「严禁为了看起来整齐把服务端口挪回 49152 以上」。
- **验证**:①`go.work` 全部 **31 个模块 build 通过**(先用错正则只跑了 3 个,已重跑);
  ②完整启动 rc=0 / 266s,**21/21 gRPC + 6/6 HTTP 端口全部监听**,28 个 `[ OK ]`、0 个 FAIL;
  ③**Envoy 17 个 endpoint 全部指向 200xx 段、旧 500xx 段 0 个、不健康 0 个**(bind-mount 配置
  已被容器吃到,无需手工重建);④导表 23 张表全过(源表 svn-r1774);⑤Hub DS 起来,UDP 7777 已绑。
- **遗留**:`dist/` 里的预编译 linux 二进制和 `deploy/ds/stage` 的 pak 仍含旧端口字面量,
  属构建产物,下次出包自然覆盖;本机那条 `50000-50059` 的 netsh 保留现在已无用,可留可删。

## 2026-08-06:队员离线自动退队(locator 记时刻 + pkg/offlinewatch 通用骨架 + team 首个接入)

- **需求**:没在战斗中的队员掉线一段时间后自动移出队伍,不要让队伍里挂着一个占坑的死人。
- **先量了现状**(全部代码/配置读数,非估算):唯一的"下线"判据是 locator key 过期;
  链路常数 = Hub DS 心跳 5s(代码钳 `[1,5]`)/ `location_ttl` 30s / 断线宽限 10s /
  UE `ConnectionTimeout` **60s**(项目 ini 未覆盖,吃引擎默认)。所以**正常退出约 10s 判离线,
  静默掉线(拔网线/断电)约 70s,最坏 90s**。
- **明确否决了压 `ConnectionTimeout`**:该值 client / DS 共用一份,压到 20s 会让
  ①地铁隧道断 10~30s 的玩家每过一个隧道断一次线(且重连要重走 login→Travel→Admission,
  一次 20s 隧道换 30~60s 不可玩,净亏);②DS 侧 hitch 超时就被客户端主动断开 ——
  本仓画像有 Chaos 加速结构重建**单帧 3.58s**、Artic01 加载期 GT 阻塞 **15.6~22.2s** 的实测。
  60s 不是没人调过的默认值,它在保护弱网玩家和 hitch。**结论:一行不改。**
- **架构要点**:「玩家什么时候离开的」这个事实只放 locator 一份(§9.22)。位置 key 整个带 TTL,
  过期时 `updated_at_ms` 跟着消失 → 权威侧答不了"离线多久",故新增**独立 key**
  `pandora:locator:lastseen:<id>`(`last_seen_retention` 默认 1h)+ `BatchGetLastSeen` RPC。
  刻意**不**给各业务抽"扫描全量实体"的公共库 —— 那只解决代码重复、不解决状态重复。
- **落地三块**:
  ① **locator**:`ReportDisconnect` 守卫通过后记 last-seen + 发离场事件
  (新 topic `pandora.player.presence`,proto `PlayerLeftHubEvent`)。
  **顺序铁律**:先守卫缩 TTL、成功才记时刻/发事件,两步刻意非原子 —— 中间挂掉最多"缩了 TTL 没时刻",
  消费方查到 UNKNOWN 一律不动作,失效方向安全;反过来会留下无守卫背书的时刻,
  让在线玩家在下次真离线时被算成已离线很久、提前踢人。
  ② **`pkg/offlinewatch`**:事件驱动 + 延迟复查骨架(kafka consumer → 到期 ZSET → ticker
  只取到期项 → 回查 locator 权威 → Handler)。业务只实现 `OnPlayerOffline`。
  ZSET 是**独立 key 的调度提示、非权威**,可重建、不寄生在任何有语义的索引 score 上(§16.10)。
  复用 `safego.Loop`,不新建 timer 状态机。
  ③ **team**:第一个使用者,`offline_leave.enabled` **默认关**(§14.2)。三道闸:
  此刻真不在线(骨架判)/ 整队没被对局占住 / 该玩家自己没被对局占住 —— 后两道读 matchmaker,
  **读不确定一律 fail-closed 返回 error 重试,绝不放行**(拆掉一支正在打的队伍会波及还在游戏的队友)。
- **阈值 180s 的推导**(写进 conf 注释,别随手调小):下界由"一个会回来的玩家最快多久能回来"定
  —— 旧 Controller 清退 + Travel + Hub 地图加载 + Admission,保守 30~60s;低于它踢的是本来
  能回来的人,与网络好坏无关。再叠容忍一段 ~2 分钟弱网失联 → 180s。玩家实际被容忍
  ≈ 阈值 + 60~90s 检测延迟 ≈ 4 分钟。
- **两条独立触发源缺一不可**:kafka 事件(秒级)+ `GetMyTeam` 读路径兜底(Inspect → 只排队不写队伍)。
  事件会丢、Hub DS 整台挂掉时压根不发事件,只接事件会留下永远清不掉的残留成员。
- **验证**:go.work **全部模块 build 通过**;`pkg/offlinewatch` / team biz / locator biz+service
  测试全绿;proto 重生前先验证过生成器**字节可复现**(改动前重生与仓库版 diff 为空),
  故 pb 的 diff 恰为本次改动。新增测试重点全在闸门上(守卫没过不留痕、对局中不动、
  读不到对局状态必须重试而非放行、拿不到时刻则出队不猜、locator 挂了不影响读返回)。
- **遗留**:①UE cpp pb 未同步(新 message 是服务端内部事件,客户端用不到;
  `TEAM_UPDATE_REASON_MEMBER_OFFLINE_LEFT` 客户端不认识时按默认分支刷新,队伍快照仍正确,
  只是少一句"XXX 掉线已离队"文案);②功能默认关,启用顺序 = 先 locator `departure_event.enabled`
  再 team `offline_leave.enabled`,只开后者会退化成纯兜底(已打 WARN)。
- **同日补修(自查「断线→秒重连」路径时发现的真 bug)**:`SetLocation(state=HUB)` 原本不清
  last-seen,于是一次「断线→秒重连」会在权威侧留下陈旧时刻。当下不出错(消费方永远先看
  「此刻是否在线」),但会在**下一次掉线恰好写不出新时刻**时爆 —— Hub DS 整台挂掉压根不会调
  `ReportDisconnect`,位置 key 自然过期判离线,而权威侧还留着半小时前的旧时刻 →
  算出「已离线半小时」→ 把刚掉线 10 秒的玩家立刻踢出队伍,**180s 宽限形同虚设**。
  修法 = 玩家回到 Hub 时清掉时刻,维持不变量「last-seen 存在 ⟺ 离开 Hub 后还没回来过」。
  只在 HUB 分支清(last-seen 只由「离开 HUB」写出,要再次离开必先回到 HUB,故已覆盖全部路径;
  无条件清会给 BATTLE 心跳链路每人每 5s 平白加一次 Redis 往返)。已补两条回归测试。

### 2026-08-06 复审修正：秒重连连接代围栏 + 离线观察状态机收口

- 先按用户要求把原改动原样冻结为基线 `a94e738a`，后续修复保持未提交，供 Claude Code 用
  `git diff a94e738a` 独立审核；未 push。
- 复审红测稳定复现一条 P0 near-miss：旧连接 A 与秒重连 B 落在同一 Hub Pod 时，A 的迟到
  `ReportDisconnect(player_id, hub_pod)` 无法区分连接代，会缩短 B 的 locator TTL 并重写
  last-seen。另有 `SetLocation(HUB)` 与 `ClearLastSeen` 分步导致的崩溃/失败窗口。事故登记为
  `INC-20260806-001`，功能默认关闭且未发现线上命中，仍按 P0 未关闭标准追踪。
- locator 修法复用 Hub Admission 已有的 `assignment_id + admission_id + admission_seq`，不改
  login / matchmaker / ds_allocator / hub_allocator。带 fence 的 HUB 上线先实时查询唯一 owner
  authority，只有 `HUB/ADMITTED + 有效 lease + exact target/assignment` 才把服务端返回的
  `owner_epoch + operation_id` 写入投影 fence；调用方不能自铸 owner 代际。连接代与离开时刻进入
  同一 per-player meta 状态机；带 fence 的旧代 Set/Disconnect 零副作用，legacy Set 仅作滚动升级
  兼容，legacy Disconnect 安全退化为不缩 TTL。位置 hash 与 meta 不同 Redis key，明确不伪装成
  跨 key 原子事务，写序与补偿只允许故障造成保守延后。
- `pkg/offlinewatch` 收口为同 slot 的 `due + evidence`：业务只调用 `Observe`，claim / finish / retry
  均条件提交，Handler 前最终重查 locator，`ErrDeferred` 保留任务。缺 last-seen 且没有离场事件时
  必须保持 UNKNOWN，不得用第一次 key miss 或本机 `now` 猜离线起点；因此 Hub 整机掉线且从未留下
  exact 离场证据的自动清理仍需另接 owner generation / Hub roster 等权威信号，不能由通用包猜测。
- 对 team 与 StartMatch 做并发时序证明后确认：严格不改 matchmaker 时没有共同 roster CAS，
  多次读取 commitment 也不能封住“StartMatch 先读旧名单、摘人先提交、StartMatch 后提交旧名单”。
  因此 `offline_leave.enabled=true` 现在启动 fail-fast；这是防止已知 P0 路径被误开启，不把
  locator/offlinewatch 通用能力包装成已可安全执行的自动退队。
- UE 官方 proto 同步必须等服务端协议提交且 proto 工作区干净后运行
  `Tool\Build\_GenerateClientProto.bat -UpdateLock`。本轮按用户要求保留修复 diff，故旧 Hub 先按
  legacy 安全降级；禁止手改生成代码绕过协议锁。服务端验证与剩余 UE/集成门见事故文档 §8。
- **同日三修(审核四条假设后落地)**:
  ① **P0 止血**:`ReportDisconnect` 的 legacy 降级从 Info 提到 **Error + 新指标
  `pandora_locator_hub_presence_legacy_degraded_total{op}`**。此前 UE 不带 fence →
  `fence.isZero()` → 整条 no-op(不缩 TTL / 不记 last-seen / 不发事件)→
  **离线自动退队一个人也踢不掉,而每一环都返回 OK**;测试全绿掩盖了它。
  ② **P1 根治**:UE 侧把本就存在的连接 identity(`FHubAdmissionState` 的
  AssignmentId / AdmissionId / AdmissionSeq)透传到 `SetLocationHub` / `ReportHubDisconnect`。
  含 cpp pb 同步(先验证过重生成**纯新增、零 message 丢失**)、wire 结构、codec、
  两处调用点。identity 在 `HubAdmissions.Remove` 之前快照,避免拿不到。
  ③ **P4 端口漂移**:`-prod.yaml.example` **整族 16 个文件 52 处** 500xx/510xx →
  200xx/210xx。此前与 `deploy/k8s/services/services.yaml`(20001-20022)、
  `netpol.yaml`(只放行该段)、`infra.md §6.2` 权威表全部不一致 —— 是 08-05 端口迁移
  漏掉的一族,不止新增的那一行。零假阳性(52 处全是端口)。
- **刻意没做**:locator 里的 `owner_epoch` 跨 assignment 全序层未砍。它超出了「组队离线退队」
  的需求范围(§9.22 的 owner authority 是独立工作线,且该条明写"尚未实现"),但砍掉是
  不可逆重构、需要先拍板要不要在这条线上开那个工作;连接三元组比较那一层是必要的,保留。
- **仍未验证**:全部为 build + 单测层面。未连真 Redis Cluster / 真 kafka,未端到端跑过
  「真掉线 → 真被移出队伍」。UE 侧改动待用户编译。
- **同日五修:H2(Hub DS 整台崩溃 → 离线成员永远清不掉)闭环**。
  症状:Hub 崩溃 / OOM kill / 网络分区时 locator 收不到任何 `ReportDisconnect`,写不出
  `left_at_ms` → `BatchGetLastSeen` 返回缺席(UNKNOWN)→ 消费方按 §9.22 一律不动作 →
  那一批玩家永远挂在队伍里(只能等 team `active_ttl` 60 分钟回收整支队伍)。
  **刻意没走「hub_allocator 判死时按名册补发离场事件」**:①判死点 `sweepOnce` 只拿得到 pod 名,
  名册要另存一份「pod → player_ids」,那是 §9.22 的重复影子状态(名册权威在 Hub DS 自己),
  且每 5s 每 pod 写 500 个 id;②hub_allocator 有多次 P0 事故史,为这个需求动它性价比不对。
  **实际改法(不碰 hub_allocator 一行)**:心跳续期 meta 时顺手把 `last_alive_ms` 推到当前时刻
  ——「最后一次被观测在线」就是崩溃场景下唯一能留下的时间线索,精度 ±一个心跳周期(5s),
  对 180s 级阈值绰绰有余。`BatchGetLastSeen` 改成两级来源:`left_at_ms`(显式离开,更精确)
  优先,缺失才回退 `last_alive_ms`。在线玩家的 `last_alive_ms` 一直在刷新、看起来像「刚离开 5 秒」,
  但不会误伤:`offlinewatch.classify` 永远先判「此刻是否在线」,只有确认查不到位置才用到时刻。
  **两个坑**:①心跳写 meta 必须 `EXISTS` 守卫 —— HSET 会凭空建 key,而「有内容但没有 mode 字段」
  的 meta 会被 `hubPresenceScript` 判为损坏并 fail-closed,等于给 legacy 玩家造出永远无法接受
  HUB 写的毒 key(已补回归测试);②pipeline 里必须用 `Script.Eval`(全文)而非 `Run` ——
  `Run` 只发 EVALSHA,脚本未被本连接加载过就整批 `NOSCRIPT` 失败,而 pipeline 内拿不到
  单命令的自动 fallback(实测踩到,测试先红后绿)。
- **同日六修:H3 后半(闸门检查 → 改队伍 的 TOCTOU)后果收敛**。
  先量化再决定:窗口命中 = 队长在闸门放行后、`UpdateWithLock` 提交前点了开始匹配,
  matchmaker 把含该离线成员的 roster 冻进票据 → **人在票据里却已不在队伍里**,
  被拉进一场自己不在场的对局(掉分),回来还发现没了队伍。不卡死、不破 §1、claim 也会
  正常释放,但可低成本修掉。
  **修法复用既有语义**:摘人成功后复核一次,发现票据确实在窗口内成立了,就走与
  `LeaveTeam` 完全相同的补偿 —— 撤销整张票据、全队退回队列重新匹配(理由也一样:
  队伍人数已变,票据里那份成员快照不再成立)。后果从「带着离线的人开局」降为「重匹一次」。
  不在锁内复核(Redis 事务里发不了 gRPC);不回滚摘人(票据已冻结,加回去只会制造
  第二种不一致)。新增 `pandora_team_offline_leave_race_total{outcome}` 让窗口真实频率
  由线上数据说话,而不是靠推理。
  **诚实边界:收敛 ≠ 消除。** 复核 RPC 自身失败时仍会留下「人在票据、不在队伍」且无从判定,
  只能靠 Error + `outcome="recheck_failed"` 人工兜。因此**没有**放开
  `conf.ValidateOfflineLeave` 的 fail-fast —— 那道闸是上一轮别人基于同一风险加的,
  放开的前置条件是 matchmaker 组票时写一下 team key、借 team 自己的乐观锁形成共同
  线性化点(顺带能修「MATCHING/IN_BATTLE 全仓无写入点」那个老问题),属独立设计变更,
  不由本开关决定。同步修正了文件头「刻意不联动 cancelMatchmaking」与新补偿路径的矛盾表述。
- **同日七修:H3 TOCTOU 真正消除(用户拍板开 matchmaker fence 线)**。
  matchmaker 组票不再用只读 `GetTeam`,改走新增的内部 RPC `TeamService.BeginTeamMatch`:
  在 **team 自己的乐观锁内**原子完成「存在/READY/队长 三项校验 + 冻结名单 + 上租约 + 返回快照」;
  `removeOfflineMember` 在**同一把锁内**看到未过期租约就返回 `ErrDeferred`。
  两个操作因此只能有一个赢 —— 从「后果收敛」升级为「窗口不存在」,
  于是 `conf.ValidateOfflineLeave` 那道 fail-fast 得以名正言顺撤掉(只保留依赖缺失拒启)。
  **锁的形态刻意是短租约而不是 `TeamState=MATCHING`**:写 State 需要有人负责改回来,
  matchmaker 中途崩溃就会把队伍永久卡在 MATCHING(违反 §20),这也正是 State 至今
  无写入点的原因;租约到期自净、无补偿路径,钳在 `[2s,15s]`,只需覆盖「组票 → ClaimPlayer 落地」,
  此后整场对局的占用判定仍由 player→ticket claim 负责。两者语义不同不可互相顶替,
  所以 matchmaker_addr 仍是启用前提。
  operation_id 取 `startmatch:<team>:<captain>`(**刻意不掺时间戳**):同一次组票的重试
  必须拿到同一个 id 才能幂等续租,掺了就会让自己的重试变成"另一次组票"而互相顶掉(§9.23)。
  校验从 matchmaker 挪进 team 的锁后,matchmaker 侧的非法状态矩阵测试同步在 fake reader 里
  复刻那三项校验,断言不丢。新增 4 条 fence 回归(上锁后摘人必须被拒 / 租约过期恢复 /
  同 operation 幂等续租 / 锁内仍复核 READY 与队长);上一轮那条「无 fence 必须拒启」的
  conf 测试前提已不成立,改为断言「依赖配齐即可启动」+ 两条依赖缺失拒启。
  `compensateIfCommittedDuringRemoval` 作为纵深防御保留(覆盖租约已过期而 claim 恰在此刻
  落地的极窄残留),正常路径不再触发。14 个包测试全绿。

## 2026-08-06:玩家昵称校验规范落档(仅文档,未改代码)

- 新增 [`docs/design/player-name-validation.md`](./docs/design/player-name-validation.md),
  并在 `go-services.md §2.2 player` 的关键不变量里加了指回链接。
- **动机**:现状只有三条校验(`TrimSpace` → 非空 → rune 数 ≤ `max_nickname_len` 默认 32),
  唯一性全靠 `uk_nickname`。归一化、字符白名单、保留前缀、同形字防仿冒、敏感词**全部缺失**,
  且玩家可自取默认昵称前缀 `Player_` 冒充他人。
- **文档要点**(七层,顺序不可颠倒):①NFKC 归一化必须在校验之前,且校验对象与入库对象是同一个串,
  否则全角可绕过白名单与敏感词;②长度要 rune 上限 + 字节上限两条,后者兜 `VARCHAR(64)` 列,
  非严格 `sql_mode` 下超长是**静默截断**(§9.24);③字符集用白名单不用黑名单,显式拒
  `\p{Cc}`/`\p{Cf}`(零宽、RTL override)/`\p{Co}`/Zalgo;④唯一性另存 `nickname_normalized` 列
  (casefold + confusables 折叠)防西里尔同形字,现有 `utf8mb4_0900_ai_ci` 挡不住;判定只能靠
  唯一键冲突,"先查再写"是 §9.22 点名的 TOCTOU;⑤保留前缀读同一份配置不硬编码;
  ⑥敏感词独立一层、走 §9.15 热更、匹配前去分隔符;⑦服务端唯一权威,客户端只做展示灰化(§17.3)。
- **状态:设计稿,一行代码未动。** 实现落点建议(`pkg/namecheck` + 迁移加 normalized 列 +
  错误码细分 + 改名 CD/审计)与验收矩阵已写在文档 §9/§10;实现前不得声称昵称校验已达标。

## 2026-08-06:外挂滥用(刷进出副本 + 各功能面被刷)防护盘点与设计落档(仅文档,未改代码)

- 新增 [`docs/design/anti-abuse-scene-entry.md`](./docs/design/anti-abuse-scene-entry.md)。
- **威胁模型**:外挂 = 持合法 session、按协议发包、把频率拉到人做不到的程度。四类危害:
  A 资源放大(刷进出副本 → 拉起 GameServer)、B 扇出放大(世界喊话/群发申请)、
  C 状态搅动(反复 owner 迁移)、D 存储膨胀(高频写只增表)。
- **一条铁律**:限流是**背压**不是权威门 —— 依赖故障时 fail-open(牺牲限流保可用),
  正确性另由 fail-closed 权威门(`ensureNoneInBattle`、owner lease fencing、Admission CAS)兜。
  两者不得互相冒充;限流器故障绝不能变成 §9.20 的卡玩家源头。
- **盘点结论(带证据)**:进出副本链上**权威门齐全、成本闸完全没有** ——
  `validateMapID`/`ensureNoneInBattle`/durable operation/全局 `MaxQueueTickets` 都在,
  但 `StartMatch` **无 per-player 频率闸**;成局级冷却 + 换 match_id 仍是
  `decision-revisit-allocating-bounded-terminal.md:48` 记着的未落地前置;abandon 无任何代价。
  Hub 切线的 `TryTransferCooldown`(SETNX,10s,失败即释放)是本仓唯一做对的进场侧防刷范例,
  被定为模板。其余:私聊/交易撤单/改名/登录失败全无频率闸;**Envoy 边缘零限流**
  (envoy.yaml 只有一行注释);**登录 `ensureAccount` 是自动注册**,是「无成本创建身份」入口,
  会让所有 per-player 配额被「换个号」绕过。
- **设计**:四层 —— ①Envoy `local_ratelimit` 挡未鉴权洪水(不上全局 RLS,§15.3);
  ②`pkg/redisx/ratelimit.go` 两原语 `Cooldown`(SETNX)/`Quota`(Lua INCR+PEXPIRE)统一现有两份
  散写 SETNX,**不做滑动窗口/令牌桶**;③A 类专属成本闸(在途分配唯一 + 成局级冷却 + abandon 计数);
  ④复用 Prometheus + killswitch 观测止血(player_id 绝不做 label,§12)。
  明确拒绝清单已写进 §5(行为分析、全局 RLS、客户端限流、给 CancelMatch 加闸等)。
- **验收底线**:零副作用拒绝(不得先干重活再判限流)、fail-open 已注入验证、被拒时不卡玩家、
  退出路径永不受限、压测给出前后对比表。
- **状态:设计稿,一行代码未动。** 三项待人拍板:abandon 惩罚是否做、各限流初值、
  自动注册是否保留(§7)。

### 同日补充:用户圈定真正的高危面 = Battle DS 占位耗尽(文档已加 §3.2.1,并重排优先级)

用户三次澄清把范围收窄到:**「玩家进 battle DS,DS 没回收就退出又进新的 battle DS,
耗尽 DS 资源导致进不去」**。Hub DS 按人数自动扩展,**明确不在射程**。

- **这不是限流问题,是持有时间问题**。核准的成本账(全部有出处):
  - Battle DS = 一局一 Pod,`requests = limits = 14Gi`(`20-fleet-battle.yaml:199,211`,
    由 INC-20260727-002 实测 `memory.peak≈10.43GiB` × 1.34 定档);
  - 同时最大局数 = `maxReplicas`(本地 2,线上按节点池覆写),是**硬上限**;
  - 空场回收 **5 分钟**(`empty_battle_timeout`,判定在心跳 CAS 内 `battle_auth.go:910`);
  - **DS 侧 2~3min 自结算计时器(主路径)UE 仍未实现**(`agones-dev.md:465` 写着「待实现」),
    ⇒ 当前 5min 后端兜底是**唯一**回收手段;
  - 冷启动 22s/48~58s/>120s 期间 Pod 已被占;
  - **⇒ 单次 StartMatch 放大比 = 14Gi × 约 6 分钟。**
- **押死 Fleet 的成本低到离谱**:`maxReplicas` 个小号 × 每 6 分钟点一次。线上若 20,
  就是 20 个号每号 6 分钟一次 —— **比正常玩家还慢**,BBR 不会触发(服务毫不繁忙),
  任何频率闸也拦不住。正常玩家此时拿 `ErrDSNoAvailable(5001)`,**这本身就是 §9.20 违规**。
- **最便宜的攻击面是单人 PVE**(`team_size=1`):1 账号 = 1 台独占 DS;5v5 反而贵 5 倍。
- **`ensureNoneInBattle` 拦不住但值得注意**:`refreshBattleLocations` 刷的是 roster 全员
  (`allocator.go:2382`),所以从未连入的玩家也被标 BATTLE、5min 内开不了新局 ——
  它把乘数从「频率」换成了「账号数」,而 `ensureAccount` 自动注册让账号免费。
  **副作用**:正常玩家强退后同样被锁最多 5 分钟,只看到 `ErrMatchInBattle(4007)`「正在战斗中」,
  这是实打实的 §9.20 卡玩家 —— **所以缩短空场回收既是防刷也是修 bug,一个修同时解决两件事**。
- **对策按杠杆排序(已写进 §4.3)**:
  ①**把「从未连入(no-show)」与「有人连过后全员掉线」拆成两个阈值** —— 前者只需覆盖
  「DS 报 ready 之后 travel+连接+Admission」(建议 60~90s,待实测),后者必须 > 重连窗(2~3min)。
  **读 CAS 核准的关键事实**:空场计时只在 `state ∈ {ready,running}` 才推进
  (`heartbeatLegacy` 守卫),**冷启动 warming 期不计入空场窗口**(那段归 `heartbeat_timeout` 管),
  所以 no-show 阈值不必给冷启动留余量;但总持有 = 冷启动 + 空场超时两段串行,6 分钟放大比不变。
  实现极廉价:`BattleStorageRecord` 加一个 `ever_had_players` 位(§9.17 加字段兼容),
  判定处按位选阈值,**不新增计时器/状态机**(§16.10:到期后重查权威属有界兜底,非掩盖时序)。
  ②**账号必须有成本**(否则①只是让攻击者多开几个号);③容量分级准入 + no-show 指数退避 +
  `ErrDSNoAvailable` 改带 `retry_after` 的 `WAIT` 并入 §9.23 同一恢复协调器;④原在途成本闸并入。
- **明确不做**:不靠调大 `maxReplicas`(把被押死换成被烧钱);**不缩 `heartbeat_timeout`**
  (INC-20260727-001 根因正是单阈值同时管启动与稳态,缩它会重新击穿冷加载中的 warming DS —— 
  空场回收与失联回收是两条独立时钟,不要合并)。
- **落地顺序已重排**:双阈值空场回收提为**第 0 项(最高优先级)**,排在限流原语之前。
- 待拍板升级为 5 项,**自动注册是否保留升到第 1 位**(它直接决定攻击成本上限)。

### 2026-08-07:落地第 0 项 —— Battle DS 双阈值空场回收(代码完成,待编译/实测)

用户拍板「先做第 0 项」。理由不变:它同时修掉「正常玩家强退后被锁 5 分钟」这个 §9.20 卡玩家 bug,
收益不依赖任何产品决策,改动也小。

**改了什么**(4 个文件,`ds_allocator` + proto):

| 文件 | 改动 |
|---|---|
| `proto/pandora/ds/v1/allocator.proto` | `BattleStorageRecord.ever_had_players = 21`(纯加字段,§9.17 双向兼容;`updateWithLock` 的 read-modify-write 不 `DiscardUnknown`,滚动升级期旧副本回写不会丢它) |
| `internal/conf/conf.go` | 新增 `no_show_battle_timeout`;常量 `DefaultNoShowBattleTimeout=150s`、`NoShowTimeoutFloor=60s`;新增 `ResolveNoShowTimeout()` 把四个分支收敛成一个返回值,调用方不再判分支 |
| `internal/biz/allocator.go` | `heartbeatLegacy` 按 `EverHadPlayers` 选阈值;判弃日志分 `reason=no_show` / `all_disconnected`(运维能一眼分清「刷子」和「真掉线」) |
| `internal/data/battle_auth.go` | `ActivateHeartbeat`(Model B 生产路径)同款双阈值;`BattleHeartbeatInput` 加 `NoShowTimeout` |

**两条路径都改了**:`heartbeatLegacy`(`!RedisAuthorityEnabled()` 时走)和 `ActivateHeartbeat`
(Model B 生产路径)。只改一条等于线上没生效。

**默认 150s 的推导(不是拍脑袋)**:DSTicket v2 生产档 TTL 120s(§9.3,签发与验签双向强制)
+ 30s 余量。票据过期后该客户端**已不可能**再进入这一局(DS 验签必拒),继续押 Pod 是纯浪费。
这条推导比「感觉 90s 够用」站得住,也天然满足「不误杀正在 travel 的正常玩家」——他们票还没过期。

**实现中踩到并修掉的自造 bug(写下来免得后人重犯)**:第一版把 `timeout = in.NoShowTimeout`
无条件赋值,于是 legacy 调用方 / 存量单测传的零值会让阈值变 0,而判定是 `timeout > 0 && ...` ——
**no-show 局将永不回收**,比不改还糟。已在两条路径都加 `> 0` 回退,并由
`TestResolveNoShowTimeoutNeverSilentlyZero` 锁住。fail-safe 方向统一为「宁可回收得晚,不可不回收」。

**护栏**:`no_show_battle_timeout` 配小于 60s 被钳到 60s —— 手滑把 `150s` 写成 `1.5s` 不能变成
「玩家进不去场景」(§9.20 红线)。配负值 = 显式禁用差异化,退回改动前单阈值行为(回滚开关)。

**新增单测**(共 6 个,均**未运行**):
- `TestHeartbeatNoShowUsesShortTimeout` / `TestHeartbeatEverHadPlayersKeepsLongTimeout` ——
  两条用**完全相同的空场时长**,只差 `EverHadPlayers`,一个必须判弃、一个必须活着。
  后者就是「防刷改动不许误伤真实掉线玩家」的护栏。
- `TestHeartbeatEverHadPlayersStickyAcrossDisconnect` —— 该位一经置位永不清零。
- `TestHeartbeatNoShowDisabledFallsBackToEmpty` —— 禁用差异化 ≠ 永不回收。
- `TestResolveNoShowTimeout`(表驱动 7 例)+ `TestResolveNoShowTimeoutNeverSilentlyZero`。
- **存量测试语义已核**:`allocateReady` 以 `player_count=len(playerIDs)>0` 上报,故存量空场测试
  的局都会置位 `EverHadPlayers` 走长阈值,而它们的 `backdateEmptySince` 回拨到 epoch,仍超时 ⇒ 不受影响。

**交接(§14.3)**:
1. ⚠️ **必须先由 Codex 跑 `pwsh tools/scripts/proto_gen.ps1`** —— 代码引用的
   `EverHadPlayers` / `GetEverHadPlayers()` 在生成的 pb 里**还不存在**,不跑必然 build 红。
2. 无需 `go mod tidy`(没引入新依赖)。
3. **默认行为变了**:no-show 局从 300s 提前到 150s 回收。要退回改动前行为:`no_show_battle_timeout: -1s`。
4. **仍未验证**:未编译、未跑测试(用户自行编译)、未做故障注入验证真实断线玩家在重连窗内不被误判、
   未实测「DS 报 ready → 客户端完成 Admission」的 P99 来复核 150s、未给出「单次进场占用 Pod·分钟」前后对比。
   在这些补齐前,§6 第 0 项不算验收完成。

---

## 2026-08-07 配置表表头漂移同步工具(configtable-sync)

**起因**:策划把 `技能/j_技能_方位类型_圆形.xlsx` 的 D 列「圆形集合」改名为「范围内圆形集合」
并新增 E 列「范围外圆形集合」,一键导表整批失败。手工修完之后暴露两个更值得修的问题:
① 这类「列改名 / 加列 → 手改 proto 注解」本来就该是工具干的;
② `configtable_gen.ps1` 优先用预编译 exe,而 exe 内嵌的是**编译时**的 proto 描述符——
改完 proto 只跑 proto_gen 而忘了重建 exe,导表会报一字不差的旧错,极难想到原因。

**用户指令**:「不应该手写,而是用工具去生成这些表的 Proto」(参照旧项目 data_table_exporter)。

**调查结论(实测,非推测)**:旧项目能生成 proto 的前提是它的 xlsx **自带 schema 元数据行**
(类型 / map 角色 / owner / 外键)。Pandora 的策划表**一行元数据都没有**,而唯一像 schema 的
客户端列登记是严格子集(`role_level` 登记 10 列而服务端要 12 列、缺的正是击杀经验唯一权威
`kill_exp`;`z_专精.xlsx` 客户端根本没登记)。加上字段编号必须跨版本稳定、`required` /
`prefix` / `fk` / enum / 上限全是服务端决策、proto 注释是这些决策的唯一存档 —— 四条阻塞。
详见 `docs/design/decision-revisit-configtable-proto-generation.md`。

**落地(方案 B,自主决策)**:proto 仍是单一事实源,新增 `configtable-sync` 只做机械那一半。
这是加法不是推翻旧决策,故未走 `AGENTS.md §7` 拍板门禁;全量生成 proto(方案 A)跨两仓、
跨策划/客户端/服务端三方,**仍待人拍板**。

- 新增 `tools/configtable-gen` 的 `-sync` / `-sync-write` / `-client-registry` / `-sync-col`;
  `internal/protosync`(diff / registry / apply)+ `internal/tablegen/view.go` 只读视图。
- 新增 `tools/scripts/configtable_sync.ps1`:报差异 →(`-Write`)改 proto → 重生 pb →
  重建 exe → 重跑导表,一条命令走完。
- `tools/scripts/configtable_gen.ps1`:exe 比源码 / pb 旧时自动重建;表头改名报错不再误导到
  「文件被 Excel 打开」,改为指向 configtable_sync.ps1。

**守住的红线**:改名只替换 `(excel_col)` 字面量且要求全文件恰好命中一次;挪位 / 删列 / 重名
一律只报告(改名与挪位从表头看不出区别,猜错会让整批数据错列);字段编号取「已用 + reserved
上界」之后,不回填空洞(§5.4);不自动写 required / prefix / fk / bit_index / enum。

**验证**:protosync 单测 11 组全绿;端到端把 `skill_circle.circles` 的注解改回旧列名制造真实
漂移,`-Write` 自动改回并跑完全链,23 张表全过且**批次号未变**(v20260807001,内容完全相同),
证明改写语义等价;`go test ./tools/configtable-gen/...` 与 `pkg/configtable` 全绿。

**剩余风险**:本次未遇到真实的「新增列」漂移,追加字段路径只有单测覆盖,下次策划真加列时
需人确认一次追加位置与注释格式。
- **同日八修:补上「只跑过 miniredis / 没跑过真依赖」这个空白(用户已编译 UE 后继续)**。
  拉起真 Redis + Kafka,把此前一直列为「未验证」的两类语义补了回归:
  - `location_realredis_test.go`(4 条):`PEXPIRE ... LT` 只缩不涨、hubPresence Lua 的同 assignment
    定序 / 跨 assignment 接受 / 同序 ABA、`RecordLastSeen` 幂等不后移、心跳 `last_alive_ms` 兜底
    且不给无 meta 玩家凭空建毒 key。
  - `pkg/offlinewatch/realredis_test.go`(4 条):`ZADD GT` 只后推不前拉、Sweep 判定与出队、
    依赖不可用零动作、出队后不复活。
  - `pkg/offlinewatch/realkafka_test.go`(1 条):producer→consumer→Enqueue 真往返。
  三份都用环境变量守卫(`PANDORA_TEST_REDIS_ADDR` / `PANDORA_TEST_KAFKA_BROKERS`),不设即 skip,
  不影响 CI 与离线开发。**理由**:miniredis 是 Go 仿真件,`PEXPIRE LT` / `ZADD GT|XX` 这类修饰符
  语义不保证与真 Redis 一致,而判错的后果是「把在线玩家踢出队伍」——仿真件绿灯不能当真 Redis 绿灯。
  跑测过程中的两处真实发现:①`ZADD GT` / Lua 在真 Redis 上行为与预期一致(**没有**分叉,
  但这是验出来的、不是假设的);②**`pandora.player.presence` 漏建 topic 会静默降级** ——
  producer 是 best-effort,发送失败只打 Warn,表现为「功能看起来在跑,但离线成员要等有人打开
  组队面板才被清掉」,无任何 Error。本地首次 Send 就撞了 `topic does not exist`,靠 broker
  auto-create 才在第二次通;已把该 topic 连同这条警告登记进 `docs/design/infra.md` topic 表,
  生产禁用 auto-create 的集群必须列进建表清单。
  同时把 dev 两个开关打开(`locator.departure_event.enabled` / `team.offline_leave.enabled`)并
  更新了 team-dev.yaml 里那段「必须保持 false、会 fail-fast」的过期注释;prod 模板仍默认关闭。
- **MySQL 里的复合类型全部改 proto 二进制,清掉最后 3 处 JSON 列(2026-08-07,用户指令)**。
  口径:`§5.8` 四类里「服务端存储快照」落 MySQL blob 列时必须是 pb 二进制,不用 JSON ——
  JSON 没有字段编号语义(改名/加字段靠手写 tag 对齐)、不保留 unknown fields,新旧副本
  read-modify-write 会静默丢新字段(违反 `§9.17` 不停服更新)。
  改的三列:`player_item_instance.attributes` / `mail_transfer_escrow.attributes`
  (JSON → `VARBINARY(1024)`,存 `ItemInstanceAttributesStorageRecord`)、
  `leaderboard_reward_log.reward_json` → `reward_pb`(`VARCHAR(2048)` → `VARBINARY(2048)`,
  存 `RewardGrantStorageRecord`)。后者不只是审计:`RetryUngrantedRewards` 补发要把它解回
  奖励列表重放,编码漂移会让整条发奖记录变成永远补不出来的脏数据。
  **存量数据:干净切换(人拍板,未上线)** —— 新迁移 `pandora_trade/000004`、
  `pandora_leaderboard/000003` 用 `information_schema` 守卫做 DROP + ADD,旧 JSON 值**不保留**
  (两种编码不同构,原地 MODIFY 会让 `proto.Unmarshal` 读到 JSON 文本字节)。leaderboard 那条
  迁移前须确认无 PENDING/FAILED 待补发行,否则补发入参会一起丢。
  顺手把两列的写入侧字节闸补齐(`dbguard.CheckPayload`,attributes Max=768、reward_pb Max=1536),
  之前 `grantRewards` 是 `json.Marshal` 忽略错误直接落库,现在编码/超限即拒 Claim,不留解不开的行。
  budgets.go ×2 与 `dbcheck/bigfield.go` 同步改名并新登记 `mail_transfer_escrow.attributes`。
  **验证**:`go build` / `go vet` / `go test -count=1` 全绿(inventory、leaderboard、tools/migrate);
  真 MySQL 集成测试 `inventory_transfer_mysql_test.go` 的种子改成走生产同一条编码路径。
  **剩余风险**:真 MySQL 迁移未在本机执行(需 dev 库重跑 migrate);dev 库里已有的鉴定词条
  与发奖明细会被清空。

## 2026-08-10:注册编号(register_no)全链落码 + 开服登录洪峰设计落档

- **需求**:策划要自增注册编号(对标梦幻西游)。讨论全程与拍板记录见
  `docs/design/register-no-and-login-surge.md`(新):架构定性(全区全服,账号命名空间
  全局一套;玩家库与账号库扩容路线刻意相反)、方案对比(同步取号/AUTO_INCREMENT/独立
  ID 服务+push 均否决,理由与反例齐)、开服洪峰四层对策与现状缺口(Envoy 零速率闸、
  无登录排队、bcrypt 实为 cost=4、压测无洪峰场景)。
- **方案**:`accounts.register_no` 列(NULL=待补)+ `register_no_counter` 单行计数器 +
  login 异步批量补号(5s ticker,单事务「锁计数器行→按 created_at+player_id 取批(带
  10s 水位安全滞后,封死迟可见错序)→逐行复核编号→推进计数器」;计数器行锁即全局
  互斥,多副本安全无需 leader;不锁账号行,避开 InnoDB 间隙锁挡注册 INSERT)。
  严格连续无空洞;编号全序=created_at 全序零反例。**红线:纯展示字段,禁作身份键/外键**。
- **落码**:mysql-init/02 + tidb-init/03 DDL、`pandora_account/000004_register_no`
  条件幂等迁移(含 down)、`login/internal/data/register_no.go`、main.go 接线(启动探针
  fail-soft:缺迁移只停用补号不拦启动)、conf `register_no_start`(起始号,仅首次初始化
  生效)、dbcheck registry + budgets.go + CLAUDE.md §9.24 豁免登记(`register_no_counter`
  恒 1 行权威闸)、login README 职责行。
- **测试**:`register_no_mysql_test.go` 真 MySQL 套件(PANDORA_TEST_MYSQL_DSN 门控,
  沿用 session_generation 临时库惯例):跨批全序连续、水位滞后、双 sweeper 并发无重号
  无空洞、起始号幂等、缺迁移探针失败。
- **验证**:`go build` / `go vet` / `go test -run RegisterNo_MySQL`(skip 档)全绿;
  dbcheck 构建绿。**剩余风险**:真 MySQL 套件未跑(本机 3306/3307 无监听,栈未起);
  存量 dev 库需跑 000004 迁移,否则启动 ERROR `register_no_sweeper_disabled`(login 照常)。
- **待拍板**(文档 §6):A②给谁看(阻塞查询接口/proto/UE 展示)、B bcrypt cost、
  C 登录排队立项 + Envoy local_ratelimit 提级、D 洪峰压测专项。

### 同日补充:A② 拍板「客户端玩家可见」,展示链路全链落码(Codex 已完成生成与编译)

- proto:`LoginResponse.register_no = 13`(uint64,0=补号中;注释含红线),go/cpp pb 已生成;
  服务端协议基线以 `[proto]` 提交 `bea78b83`,客户端使用官方 `GenClientProto.ps1 -UpdateLock`
  同步并以 `-VerifyOnly` 复验通过。锁后累计协议变化涉及 7 组 pb(14 个 `.h/.cc`),不是仅
  login 一对;`ClientProto.lock.json` 锁定实际引入协议的 `bea78b83`。
- 服务端:`AccountRepo.GetRegisterNo`(接口 + MySQL 实现;**fail-soft**:独立 250ms 查询预算,
  读失败/超时/存量库缺列只 Warn 置 0,不取消登录父 ctx——刻意不并进 FindByAccount)、
  biz `LoginResult.RegisterNo`(主路径 + battle 重连路径都带出)、service 组装;
  两个测试 fake(fakeAccountRepo/devFakeRepo)已补方法;新增主登录、慢查询降级、battle 重连、
  service proto 四类非零传播回归。
- UE:Wire decode→LoginResult→Backend 会话态→AccountModel→RoleInfo 已贯通;无会话时折叠,
  非零显示「注册编号 N」,登录态 0 显示「注册编号 生成中」。未改 `WBP_RoleInfo.uasset`,
  沿用界面现有的运行时补建控件机制;当前 0 不主动轮询,要等下一次完整登录响应刷新。
- **Codex 验证**:login `go build ./...`、`go vet ./...`、`go test ./... -count=1` 全绿;
  CodecGen Python 15/15、官方协议 `-VerifyOnly` 全绿;UE `Pandora Win64 Development`
  (659/659 actions)与 `PandoraEditor Win64 Development`(866/866 actions)均编译成功。
  未执行 PIE 视觉检查或大厅/战斗重连真实登录 E2E;本次双后端测试因 DSN 未设置均 SKIP,
  真 MySQL/TiDB 的先前实跑记录见下节。客户端 SVN 未 commit/push。

### 同日修复:真 TiDB 并发测试抓获补号事务快照错序(提交前阻断,已修)

- **症状**:`ConcurrentSweepersSerialize` 在真 TiDB(v8.5.1,悲观模式)必现
  `register_no assign affected=0(计数器锁外存在第二写者)`——两个并发 sweeper 互相
  把对方打成 fail-closed 整批回滚;真 MySQL 全绿。
- **根因**:TiDB 悲观事务在默认 RR 下用 `BEGIN` 时刻的 start_ts 快照服务**普通 SELECT**;
  后到的 sweeper 在计数器行锁上等前一批提交,拿锁后 `FOR UPDATE`(当前读)读到推进后的
  next_no,但取批扫描仍按旧快照重扫刚被编号的同一批行,复核 UPDATE(当前读)恒 affected=0
  → 正常并发被误判成"第二写者"。InnoDB RR 无症状是侥幸:read view 迟到首个一致性读
  (取批在拿锁后)才建。即**计数器行锁只串行化了写,没串行化取批读的可见性**。
- **修复**:补号事务改 `READ COMMITTED`(`BeginTx` 显式 Isolation;register_no.go)——
  两端 RC 都是逐语句新快照,取批发生在拿锁之后必然包含锁前驱批次的提交;affected=0 恢复
  「真第二写者」语义。刻意不用「取批加 FOR UPDATE」修法:InnoDB 下会在 uk_register_no
  的 NULL 范围产生间隙锁挡注册 INSERT(原设计规避点,RC 顺带免除)。设计文档 §3.3 新增
  「事务隔离必须 READ COMMITTED」要点,伪代码补 ⓪。
- **测试升级**:`register_no_mysql_test.go` 升为 MySQL/TiDB 双后端门控
  (`PANDORA_TEST_MYSQL_DSN`/`PANDORA_TEST_TIDB_DSN`,friend/guild 同款 forEach;
  用例更名 `RegisterNo_MySQLAndTiDB_*`),并发用例即该缺陷针对性回归(修复前 TiDB 必炸
  已复现,修复后两端全绿)。
- **验证**:真 TiDB(本地 4000)+ 真 MySQL(k8s port-forward 13306)双后端全套 ×1、
  并发用例 ×5 全绿;login 模块 build/vet/全包 test(含集成)全绿。
- **同类扫描**:friend/guild/mail 的锁内权威读全部已是 `FOR UPDATE` 当前读(friend README
  「读侧防陈旧快照」条款先例),register_no 是全仓唯一「锁后普通读」例外,已闭环。

## 2026-08-05(续):经验域接入 resync 回源 + 逐域覆盖矩阵核完

- **接着上一条的 §12.4 待办做**。逐域核对推送消费域 × 兜底手段后,`MyPlayerProgressionModel`
  是唯一三项(resync / 切前台 / 常驻兜底)全缺的域;Team / Friend / Guild 早已是
  「resync + 有限重试 + 前台恢复 + 会话切换」四件套,Match 靠有界轮询 + standby watchdog。
  Match 域**刻意**不接前台回调(轮询与 watchdog 前台后自然继续驱动,
  `UMyDsRecoveryCoordinator` 另有前台恢复覆盖进场链),不是缺口。
- **落码**(`Pandora-Client-SVN/.../Module/Player/Model/MyPlayerProgressionModel.{h,cpp}`):
  ①`HandlePushFrame` 增加 `pandora.push.resync` 分支 → `BeginAuthoritativeRepull()`;
  ②原本裸调 `RequestProfile()` 的另两条兜底路径(未知 `event_type`、payload 解析失败)
  一并收编到同一 helper —— 它们此前同样「回源失败就没了」;③`HandlePlayerProfile` 成功清脏、
  失败在预算内 `ScheduleResyncRepullRetry()`(3 次 / 2s,与好友域同口径);④新增前台恢复兜底
  (仅当仍脏**且**预算已耗尽才补满重来,预算 >0 说明已有在途请求/定时器在驱动);
  ⑤`DispatchRepull()` 兜住「RPC 客户端未就绪」窗口 —— 此时 `RequestProfile` 只打警告返回、
  **不会有结果回调**驱动重试,脏标记会挂着无人驱动(违反 §9.19),故自行接着排一次,
  预算递减保证收敛。定时器与前台委托均在 `Deinitialize` 成对解除。
- **修正上一条的一处不准确**:`ApplyExperienceSnapshot` **本来就是** pull 与 push 共用的同一个
  apply 函数且带 `(Level, ExpInLevel)` 单调守卫,已经是原则 5-A 模型 B 的标准形态,不需要改造;
  上一条把「走同一 apply 路径」写成待办是低估了现状。真缺口只有 resync 那一处。
- **文档**:`protocol-ordering-rules.md` §12.4 由「缺口清单」改写为**逐域覆盖矩阵** +
  本次修复说明 + 「仍未接入」三项(聊天域 / presence / system.notify,接入前不算缺口)。
- **未验证**:UE 未编译、未真机(§11.6 编译归用户)。验收建议:登录后挂后台超过 push 缓冲窗口
  (默认 5min / 512 帧)再回前台,期间用其它端刷经验,确认回前台后等级与经验条收敛到权威值。
- **仍待确认(与上一条同)**:`Pandora-Client-SVN/CLAUDE.md` 与 `F:\work\CLAUDE.md` 在当前工作副本
  中都不存在,客户端侧对应条款无处可加。

## 2026-08-10:防滥用清单 §6 全量落地(第 1–9 项,拍板 7/8/9 后一次收口)

- **拍板**(用户,2026-08-10):第 7 项「成局级冷却 + 换 match_id」现在做;第 8 项 no-show
  退避取**温和档**(10min 窗首次免罚,第 2 次起 30s→60s→…封顶 5min);第 9 项账号成本
  采「配置开关」——核查后发现 `dev_auto_register` 默认 false + `-Prod` 机械强制关 +
  契约测试三件套**早已存在**,第 9 项定谳为已覆盖,零代码。
- **第 1 项** `pkg/redisx/ratelimit.go`:Cooldown/ClearCooldown/Quota/IncrWindow/
  ArmPenalty/PenaltyRemaining 六原语 + 通用 ActionQuota + RLKey 统一构造;契约测试 13 个。
  fail-open 内建在原语里(error 时 allow=true + err 上抛),调用方想写错都难。
- **第 2 项** matchmaker StartMatch:per-队长 + per-队伍冷却(`start_match_cooldown` 3s),
  占窗在一切副作用前、业务失败即释放(hub transfer_cooldown 同模板)。
- **第 3 项** 容量耗尽:确定性 5001/5002 不再推 FAILED,改推 QUEUEING +
  `estimated_wait_seconds`(复用既有字段,**零 proto 改动**),票据退队 + 10s 静默窗,
  后端自动重试;客户端零改动可用(QUEUEING 是既有状态)。
- **第 4 项** login 失败 Quota:账号(sha256 键)+ IP 双维度,5 次/15min 窗,锁 5min;
  只对凭据失败计数(DB 故障/封禁不计),锁窗在 bcrypt 之前拒。IP 来自 Envoy 注入的
  受信 `x-pandora-client-ip`(入站同名头剥离防伪造;经 LB 多层代理部署时需重新评估)。
- **第 5 项** Envoy local_ratelimit:filter 默认关,仅 login.Login exact 路由启用
  token bucket(100 突发/50rps,初值待压测),映射 RESOURCE_EXHAUSTED;
  新契约测试 `envoy_edge_ratelimit_contract_test.ps1` 锁结构(含「退出路径不受波及」)。
- **第 6 项** 六个面:chat 非世界频道 500ms/频道独立;team 申请+邀请 12/min;
  friend 申请 10/min;guild 申请 10/min;trade 下单/撤单 20/min;auction 挂单·出价/撤单
  20/min。统一 ActionQuota,各服务单测同第 2 项口径。
- **第 7 项**(兼正确性修复):formSoloMatch 换新雪花 match_id(旧复用 ticket_id 会撞
  ds_allocator 侧保留 2h 的 abandoned claim,decision-revisit-allocating-bounded-terminal.md
  §2.3);CreateMatch 改 SETNX;成局级冷却 `match_form_cooldown` 5s 压 2s requeue 风暴。
  注意:该决策文档 §3 的「ALLOCATING 有界终态 A–D」**仍未拍板**,本轮只落了 §2.3 前置。
- **第 8 项**:ds_allocator 在 `finishEmptyAbandon` 的 reason=no_show 分支对 roster 记账
  (单一收口点,legacy 与 Model B 共用,CAS 保证每局至多一次);matchmaker StartMatch
  读罚窗执行拒绝(明确错误码 + 剩余秒数)。all_disconnected **不记**(网络差不受罚);
  CancelMatch 惩罚期内可用(测试锁定)。
- **key 登记**:infra.md §3.2 新增「RateLimit」小节(pandora:rl:* 全清单,含跨服务
  noshow 键的写者/读者标注);动作段豁免「不准用动词」已注明。
- **验证**:pkg/redisx + matchmaker + team + ds_allocator + login + chat + friend +
  guild + trade + auction 十模块 `go build` + `go test -count=1` 全绿(40 个包),
  新增测试 40+;两个 envoy 契约测试(新 + 存量)PASS;go vet 干净;gofmt 已归位
  (仅本轮文件,历史漂移文件未动)。
- **未验证 / 遗留**:①压测断言(§6 底线 5/6:限流前后 QPS·分配次数·Pod·分钟对比表)
  未跑;②Envoy 需 Codex `envoy --mode validate` + 重启生效;③故障注入(真实 Redis
  故障下的 fail-open)只有 miniredis 级验证;④初值(3s/5s/10s/各 per-min/桶值)全部
  待压测复核;⑤设备维度配额与 UE「容量排队」专属文案未做。

### 同日补充:首登必见「生成中」缺陷修复 —— GetRegisterNo 补拉 RPC

- **dev 实测暴露**(账号 test123):界面恒显示「生成中」,而库里 register_no=1 早已落好。
  根因非补号任务(它 12:08:57 建号 → 12:09:12 编号成功,恰好 15s=5s 周期+10s 水位滞后),
  而是**编号只在 Login 响应下发一次**:首登=注册同一请求,响应必然是 0,之后客户端无处再取。
  **100% 必现**——每个新玩家首次会话都看不到自己的编号,产品不可接受。
- **修法**:加 `LoginService.GetRegisterNo` 补拉 RPC(异步生成+客户端补拉的标准配套,
  而非把发号塞回登录路径)。入参为空,player_id 只从 JWT sub 取(同 SelectRole 纪律);
  **envoy.yaml jwt_authn rules 已加该 path**——未列到的 path 默认放行不验签,上游拿不到
  player_id 会一律 ErrUnauthorized。0=code OK 的正常态(补号窗口),查询失败才返回错误码,
  不得伪装成「编号 0」(§9.22)。刻意不做 sjti 复核(只读+只能读自己+零副作用)。
- **落码**:proto `GetRegisterNo` RPC + 两个 message(go pb 已重生成,lint OK)、
  biz `LoginUsecase.GetRegisterNo`、service 实现、envoy rules、
  测试 `internal/service/login_register_no_rpc_test.go` 四条。
- **顺手修**:`register_no_counter` 的 budget `MaxAvgRowBytes: 64` 每轮巡检刷一条 ERROR
  `db_capacity_budget_exceeded actual=16384`——单行表 avg_row_length 由 information_schema
  按 data_length/rows 估算,InnoDB 最小分配 16KB 页,恒报 16384 与真实行长(≈9B)无关,
  设任何按 schema 推算的值都必然误报。改为留 0(不检查),行数仍由 MaxRows=8 兜住。
- **验证**:login `go build` / `go vet` / `go test ./... -count=1` 全绿。
- **待 Codex**:① cpp pb 同步(新 RPC,标 [proto]);② 客户端拿到 0 时补拉(建议几秒一次、
  拿到非 0 即停;或挂在「刷新」按钮上),脱离「生成中」。

## 2026-08-10(续):防滥用落地的对抗性复审 + 9 项修复

- 6 路 agent 对抗性复审(证伪导向),**0 P0**;确认核心 fail-open / 零副作用 / 退出路径零波及
  纪律正确(大量 clean)。已修 9 项(2 P1 + 7 P2),全部补回归测试,10 模块重跑全绿:
  - [P1] `RequeueTicketIfOwned` 守卫退队封盲写复活竞态(先前就存在,onMatchNoCapacity 放大);
  - [P1] login hashAccount 归一化(大小写/空格绕过);
  - [P2] §9.6 DS 自报 abandoned 白名单(sanitizeReportedState);LockRemaining 双维度独立读;
    RecordFailure 布锁清计数(封续锁);StartMatch 冷却只按 captain_id(删 team_id 骚扰面);
    6 处配置注释「负值关闭/0 用默认」;Envoy :8444 剥离 client-ip;anti-abuse 文档键名更正。
- 7 项 P2 评估后判定可接受并文档化(见 HANDOFF §11.6):onMatchNoCapacity 重放闪 FAILED
  (需加 proto 字段,留待下批)、no-show 采样窄窗、IPv6/CGNAT 固有取舍、FirstAbandon 少罚
  (fail-open 安全)、roster 租约秒级自净、anyTicketInFormCooldown 满载成本、Envoy 桶值待压测。

## 2026-08-10(续2):技能卡库表治理收口 —— v5 迁移门禁 + player 落地 retention_mode

技能卡三张表(`player_skill_cards` / `player_skill_slots` / `skill_card_grants`)的持久化治理
收口,§9.24 从"登记了"补到"机械可验证 + 默认只报告不删"。

- **000005 清理索引兜底**:建表用 `CREATE TABLE IF NOT EXISTS`,对"表已存在但形态不同"是
  **静默 no-op** —— 缺 `idx_created` 的存量表会带着缺口进 v5,而 dbcheck 拿这条索引当发布
  门禁项,届时没有任何迁移能补回来。up 末尾补条件建索引(同 000003 写法,表本就带索引时退化
  成 `SELECT 1`)。unique key 刻意不补:有重复数据时加 UNIQUE 会失败,那种库属开发期手搓残留。
- **回滚风险显式化**:000005 的 down 是真删表(与 000002 同类,非 000003 那种 no-op),
  头注写清三条 —— ①数据不可恢复(碎片无第二份台账);②fresh-init 过的库回滚后会比权威定义
  少三张表,dbcheck 报「登记表缺失」,重新 up 恢复结构但数据不回来;③不停服顺序必须
  先回滚服务再回滚迁移,反过来会让在线副本对着不存在的表报错。
- **测试**:`tools/migrate/player_migration_test.go` 新增静态契约(清理索引守卫、fresh-init
  与迁移不漂移、down 只碰本次新增三张表)+ 真库四场景 × 各跑两遍(重复迁移必须仍 clean v5):
  `fresh_empty` / `fresh_init_schema`(先跑 mysql-init 再迁移)/ `legacy_v4`(停在 v4 且有业务
  数据,升级后逐行核对没被动过)/ `legacy_missing_index`(缺索引库必须被守卫补上);
  另有回滚用例(down 后既有表数据一行不少,再 up 结构完整回来)。
- **dbcheck 漂移测试**:`cmd/dbcheck/registry_test.go` 用 `deploy/mysql-init/*.sql` 与
  `tools/migrate/migrations/**` 里的 `CREATE TABLE` 反查内嵌 registry —— 建表脚本里有、
  registry 里没有即 FAIL。原先这类漏登记只有拿真库跑 dbcheck 才发现。另加:swept 表必须声明
  清理索引(豁免要写 allowlist 并说明理由)、`PendingWhere` 只能挂在 swept 上(挂别的类别是
  静默失效的死配置)、技能卡三表的类别与索引断言。
- **player 落地 `retention_mode`(§9.24 标准口径,本服此前没有)**:留空 = `report_only`,
  janitor **照常跑但一行不删**,只统计超期行数打 WARN + `pandora_db_retention_pending_rows`。
  改前是前置开关 false 就整个不跑 —— 既不删也不报,§9.24 要的待清理量彻底不可见。
  - 真删要**两道闸**:总闸 `retention_mode: delete` + 每组前置条件确认
    (`exp_history_cleanup_enabled` / `history_cleanup_enabled`)。分两道是因为前置条件对
    两组不同时成立(exp_history 卡 progress 出箱无总重试期限;mmr 组卡 kafka retention 与
    授予补扫窗口),合成一个开关就无法表达"这组已确认、那组还没有"。
  - repo 层四个 `Purge*` 改 `Sweep*(mode, cutoff, limit) → dbguard.Outcome`,走
    `dbguard.SweepTable`:COUNT 与 DELETE 强制共用同一 where,杜绝"报告 0 行、实际删 10 万"。
  - `main.go` 补 `ValidateRetentionMode` 启动 fail-fast(拼错绝不猜成 delete)。
  - report_only 下**不循环**(一轮 COUNT 即全量规模,循环只是空转);delete 下小批量删到追平。
  - `gen_cluster_config.ps1` 的 `-Prod` 硬化从两个开关扩到三个键:产物里残留
    `retention_mode: "delete"` 会让运维误判清理已生效,且将来新表组若直接读 `RetentionMode()`
    而没有自己的前置闸,生产会静默开始删。契约测试同步断言 prod=report_only / dev=delete。
- **验证**:go.work 全部模块 `go build` + `go test` 全绿(0 失败模块);一次性 mysql:8.4 容器
  上跑真实迁移四场景 + 回滚,全过;`dbcheck -exact -pending` 对同一库跑通,唯一 FAIL 是
  `pandora_player.player_data`(data_service 运行期 proto2mysql 自动建表,fresh-init 里本就
  没有,与本次改动无关);`gen_cluster_prod_progress_contract_test.ps1` PASS。
  **未跑**:TiDB 后端(pandora_player 在 MySQL;用例已按 `PANDORA_TEST_TIDB_DSN` 备好)。
- 2026-08-11:**任务域(mission)整体移植落地**。把 `D:\luyuan\mmorpg\cpp\libs\modules\{mission,condition,reward}` 三个 C++ 模块的语义搬进 Pandora 后端(不是搬代码:ECS 在 Go 侧无对应物,bitset→行存,倒排索引/类型互斥集降为事务内现算的派生态)。**新服务 `services/social/mission`**(20019/21019,库 `pandora_mission`,错误码 11000 段):客户端 RPC `ListMissions/AcceptMission/AbandonMission/ClaimMissionReward` + 系统 RPC `ReportMissionFacts`(进度唯一写入通道)/`CompleteAllMissions`(GM,与正常完成扇出**刻意分离不合并**,D 版 todo.md #225)。**配置表三张**(任务/条件/奖励,源表 `Table/任务/r_*.xlsx`)走既有 configtable 注解流水线,数组列按仓库惯例逗号分隔 string,行校验 + 跨表引用 + **任务链环 DFS** 在加载期拒批次;伴生件 `pkg/configtable/{mission,condition,reward,csvcol}.go`(condition.go 内含 D 版 condition_util 的五比较器/槽位过滤/clamp 移植)。**四表 + 出箱**:player_mission_active(bounded,进度 pb ≤256B)/player_mission_done(bounded)/mission_reward_log(swept,GRANTED 90 天清、PENDING/FAILED 永不清)/mission_fact_receipts(swept,**清理默认关**——上游重试无总期限,删收据会双计,同 exp_history)/mission_push_outbox;§9.24 清单 + dbcheck registry + budgets 三处同步登记。**发奖链照抄 leaderboard**(流水 + 提交后同步尝试 + 1min 补扫),道具/装备/经验三下游**分幂等键**(inventory ledger uk 是 (player,key),同键会撞指纹冲突),满包溢出转邮件传同键;经验键 `quest:{player}:{mission}` 用的是 player.proto 早已预留的口径。D 版发奖止于 `OnMissionAwardEvent` 事件,本移植接到真实入包,**比源头更完整**。**battle_result 挂接**:击杀/拾取/局内使用三类事实转发,新增**独立 `battle_mission_outbox` 表**而非复用 battle_progress_outbox —— 后者按每玩家严格 FIFO 取行(item balance 权威要求 pickup 与 consume/discard 有序),任务行混入会让 mission 故障卡住队首、连带阻塞该玩家的掉落/经验投递,把弱依赖变强依赖;任务事实顺序无关(累加+clamp 幂等),分表即隔离故障域。转发由 `mission_addr` 开关控制(空=一行不产,§9.21 Go 先行);纪律差异已固化进测试:**漏配经验的怪照样转发**(杀了就是杀了)、**非白名单拾取不转发**(否则绕过白名单另开计数通道)、**丢弃不转发**(扔掉不是用掉,否则「使用 N 个」能靠捡了再扔刷完)。登记面:kafka `pandora.mission.update`(+PushTopics)、compose/k8s/overlays/prometheus/envoy(route+jwt+cluster+系统 RPC 精确 403)/gen_cluster_config/run_services/start.ps1×2/k8s_envoy_bridge/export_images/两个契约测试、infra 端口表(**顺手补登记了漏登记的 matchmaker-pve 20018**)+ 库清单 + topic 清单、go-services §2.10a、arch §11。顺手核实并标注了 envoy README 声称存在实则没有的 `dialogue_cluster` 漂移。验证:proto lint/gen 绿、导表 v20260811001 绿、pkg/tools/mission/battle_result 全模块 build+test 绿(含新增 mission_forward 五项与 mission biz 语义矩阵)。**未验证(交接)**:MySQL 真实并发事务、`go test -race`(需 CGO Linux)、push→客户端全链、UE 侧任务 UI。详见 `docs/design/mission.md`
- 2026-08-11(附带发现,**非本次移植引入**):两个部署契约测试 `online_manifest_contract_test` / `services_dsticket_secret_contract_test` 在**干净 HEAD 上就是红的**(用 `git worktree --detach HEAD` 对照证实)。根因三条:①**owner 从未登记进两个测试的服务清单**(2026-07-21 owner 上线时漏登记,表现为 `Deployment 数=21,应为 20` 与 `online 最终 kustomization 仍含 newTag`)——本次已顺手补登记;②补 owner 后暴露 `Deployment/player-locator strategy=,placement writer 升级必须为 Recreate`(services.yaml 里 player-locator 无 strategy 字段);③同时暴露 `matchmaker 真实 DS 票据链不得静默构造 legacy HS256 signer`。②③在 HEAD + 仅补 owner 的基线上同样复现,与 mission 无关,且 matchmaker 当前有并发编辑者(入口模式 / min_team_size 在途),已另开任务交归属人处理,未在本次改动中触碰。同批另有两个模块因并发编辑者的在途改动而红:`tools/configtable-gen`(其 level.proto 新增「队伍人数下限」列,测试夹具表头未跟)与 `services/matchmaking/matchmaker`(match_test.go 新加 configpb 导入但 biz 未编译通过)——均非本次移植所致,`tools/configtable-gen` 目录本身零改动。
- 2026-08-11(续:Mission 批次硬阻断收口)。上一条交接里列的阻断项逐条闭环,并**推翻了本批次自己的一条设计论断**。
  - **推翻「任务事实顺序无关」**(§5.1 已入勘误):原设计据此让 `battle_mission_outbox` 按 id 平摊投递、失败行只退避自己。反例是任务链——前后两环条件类别通常不同(「杀 5 只狼」→「收集 3 张狼皮」),后环只在前环完成时才被自动接取;「狼皮」事实若先到,mission 侧扫遍活跃任务匹配不上任何类别,被收据吸收后**静默丢弃且永不重放**,玩家后续任务进度永久缺一块。乱序来源正是 `DeferMissionOutbox`(队首退避后同玩家后续行越过它先投)。改为与进度出箱同款 `NOT EXISTS` 前驱谓词 → **每玩家严格 FIFO、队首阻塞**,跨玩家互不影响;分表的意义回归为**故障域隔离**而非"可以乱序"。
  - **「使用道具」事实必须等扣除落定**:局内消费走 `battle_progress_action` 同步 action,可能以业务失败终态收场(道具不足),此时 inventory 一件没扣,而任务事实照发 = 「使用 N 个 X」能靠上报根本没发生的消耗刷完(§9.6 不信 DS)。新增 `pending_action` 列:USE_ITEM 行落库即不可投递但**仍占队首挡住后续行**,由 `ResolveProgressAction` 在**扣除结果同一事务**里置 0 或删行(分两次提交会留下"已失败但已可投递"的窗口)。拾取/击杀不挂闸(没有 action 结果行可等,且"捡到了"是 DS 记录的事实,发放失败属投递问题)。
  - **TiDB 无 gap 锁 → 接取上限与类型互斥被并发穿透**(P0,与 friend R5 P1-2 同因):`SELECT ... FROM player_mission_active WHERE player_id=? FOR UPDATE` 在该玩家零活跃行时**一把锁都不加**。新增 `mission_player_guards` 每玩家守卫行(§9.24 登记豁免,同 `friend_player_guards`),`MutatePlayer`/`ApplyFactsTx` 入口先 `INSERT ... ON DUPLICATE KEY UPDATE` 取点锁(恒为第一把锁,两条路径不成环)。**真 TiDB 实测钉死**:去掉守卫 → 上限 3 放过 12 条、互斥 8 条全活;加回 → 3/1,两端符合预期(`internal/data/mission_repo_mysql_test.go`,MySQL/TiDB 双后端门控)。
  - **奖励装备数量无上限 → 发放侧 OOM**:装备按件展开成 instance 列表,数量**就是切片长度**。加上 `configtable.MaxRewardEquipmentInstances`(=64)双闸——加载期 `ValidateMissionCrossTables` 拒批次(只对装备,堆叠/货币不受限),运行期 `deliver` 另有同值 fail-closed(reward_pb 是历史快照,可能早于该上限,或道具热更从堆叠改成装备;没有这道闸一条坏快照会让补扫每轮再炸一次)。
  - **配置面**:mission-dev.yaml 的 mysql/redis/kafka 写成了 vanilla 端口(3306/6379/9092),而 `Convert-DevToCluster` 只改写 Pandora dev 端口(3307/6380/9093)→ 集群产物**原样带着 127.0.0.1 进 Pod 且生成期不报错**;已改齐并把这条坑写进模板注释。`mail_addr` 20011 是 matchmaker 的端口,dev/prod 两处改回 20009。另发现 mission **没登记进 `$UnarySessionGateServiceNames`** → 生产会带着 `session_gate.require: false` 上线(INC-20260722-004 同类),已补登记(连带进 `$GrpcRateLimitServiceNames`),两个契约测试同步扩清单。
  - **push 漏订阅 `pandora.mission.update`**:push-dev.yaml 显式列 topics 是**覆盖而非追加**,漏一条 = 该事件本地栈完全不消费且全绿无报错(prod 模板正因同类事故改成不列)。补订阅 + 新增 `push_topics_dev_contract_test.go` 把 yaml 与 `kafkax.PushTopics` 的对齐钉死(漏项报出具体 topic 名)。
  - **脚本硬编码 21**:`start.ps1` 的 `pandora-config` Secret 份数校验写死 21,新增 mission 后一键启动直接抛错;改为从 `Get-ServiceList` 推导。`local_k8s_profile_contract_test` 的 bridge 断言同理,改为与 start.ps1 服务清单**逐名核对**(字面量会把"服务清单变了"伪装成"bridge 漏映射")。其余 30 余处 21 是注释/提示文案,一并更正。
  - **gofmt**:5 个文件已格式化(mission biz/data 四个 + `pkg/configtable/store_test.go`);`tools/migrate/cmd/dbcheck/main.go` 因本批次新增的 `pandora_mission` 块而变脏,一并格式化。全部 git 变更 go 文件现已 `gofmt` 干净。
  - **验证**:touched 模块 build/vet/test 全绿,含真 TiDB 并发回归(`PANDORA_TEST_TIDB_DSN` 指向本机 4000);`battle_mission_outbox` 的 FIFO 与 pending 闸两个用例在真实 SQL 引擎上**先红后绿**(退掉谓词 → 首轮取到 [1 2 3 4]、pending 行照取;加回 → [1 4]、pending 全挡)。PowerShell 契约测试 `local_k8s_profile` / `gen_cluster_session_gate` / `gen_cluster_prod_ratelimit` 由红转绿。
  - **仍红且非本批次所致**(与上一条交接的 worktree 对照结论一致,均在干净 HEAD 复现):`services_dsticket_secret`(matchmaker `legacySigner`)、`online_manifest`(**newTag 那条已修**,现停在 placement-preflight)、`gen_cluster_b1`(`$PlacementSecretBindings` 是空数组,产物里根本没有 placement 分权 key)、`ds_auth_activation`(断言字面量与 start.ps1 逐条比对两边一致,HEAD 同红)、`ds_entrypoint_log_redaction`(DS entrypoint 脚本本批次零改动)。**placement-preflight 已定位到根因**:`-placement-preflight` 系列 flag 于 2026-07-16 随 owner_epoch fencing 改造从 player_locator 二进制删除(commit `678d58d3`),manifest 同批清理(`4193897b`),但 `lib/online_manifest_contract.ps1`、`lib/ds_auth_activation_contract.ps1`、`start.ps1` 两处调用与两个测试**被落下**——它现在既断言一个已不存在的形态,又会在 `activate_ds_auth` 路径**构造**一个跑已删除 flag 的 initContainer(必 CrashLoop),且硬阻断每一次 `-Mode online` 发布。清理跨两个契约库 + 发布门禁,已超出本批次范围,**未动,待拍板**。
- 2026-08-11(关卡表「计分模式」列:把「算不算段位」从撮合池名字里拆出来)。起因是策划视角的一句质疑——关卡表 `5v5_ranked` 有歧义,因为"有时候可以 3v3"。查证后确认歧义是真的,而且已经埋了一个 fail-open。
  - **名字对每一行都不成立**:挂 `5v5_ranked` 的四行 `team_size` 是 1/1/1/**3**(id 9「PVP战斗」就是 3v3),没有一行是 5。人数事实的唯一权威是 `team_size × side_count`,撮合按 `need = side_count × team_size` 算;字符串里那个 5 没有任何消费者。**不同人数的图本来也不需要分池**:`partitionTicketsByMap` 已在同池内按 `map_id` 分组撮合,3v3 图与 1v1 图同池不同组、互不串局。
  - **真缺陷(fail-open)**:`battle_result` 用**排除法**从 canonical `game_mode` 推计分——`!= "pve_coop"` 即算 Elo。而 `game_mode` 是撮合池 / 部署标识,是会新增取值的:`ds/v1 AllocateBattleRequest.game_mode` 的注释里当时就写着未来取值 `"casual_5v5"` / `"custom"`,ds_allocator 单测也已经在用 `"custom"` 分配。任何新池都会**静默按排位改玩家段位**,而改段位不可逆——最坏的失败方向。
  - **修法(§17.1 差异进表)**:三个事实各归各位——池归属=`game_mode`(纯池 ID)、人数/方数=`team_size`×`side_count`、**计分=新增关卡表第 16 列「计分模式」`rating_mode`**(`LevelRatingMode`:0 未配置 / 1 不计分 / 2 按 Elo)。**判定值在成局那一刻定格**:matchmaker 发 `AllocateBattle` 时按 effective map_id 读表填进请求 → allocator 原样存进 canonical `BattleStorageRecord.rating_mode`(field 22)→ `AuthorizeResult` 与 roster/game_mode/map_id 同源复制进 `TerminalReleaseRecord` → 结算只认定格值。**刻意不在结算时重查表**:关卡表是热更的,结算查表会让改一列就改写"正在打的那一局"的计分规则(与 game_mode/map_id 同为本局元数据,同一口径定格)。
  - **兼容(§9.21)**:`rating_mode` 未定格(旧 matchmaker / 旧批次表)时 `settlementRunsElo` 回落旧口径(pve_coop 不计分、其余算 Elo),既不因缺字段白打排位局,也不因缺字段强开计分;拿不到表 / 表里没这行 / 这一列没填,一律 UNSPECIFIED + WARN,**绝不猜 ELO**(猜错=给合作副本玩家扣段位)。`sameBattleAllocationRequest` **刻意不比本字段**:共存窗口里同一 match 的 ACK-loss 重试会让该值从 0 变成显式值,纳入比对会把正常重试判成"快照冲突"直接分配失败(§9.20 玩家进不去场景);它不是身份字段(身份是 roster+map_id+game_mode,三者都比了),落定按 claim 赢家。
  - **加载期新校验**:`rating_mode=ELO` 且 `side_count=1` 整批拒绝——单方合作副本没有对手结构,Elo 算给谁都说不通,属配置错配,挡在加载边界而不是等打完一局给一群合作玩家互相扣分(`side_count=0`=沿用默认 2 方,与 ELO 相容)。
  - **存量表填值刻意与现行为逐局等价**:PVP 四行填 2=Elo,PVE 四行填 1=不计分,非战斗类留空 —— 本次只把隐含口径显式化,**不改任何一局的结算结果**;真正的行为变化只发生在"将来新增一个池"那一刻。
  - **顺手钉死 `game_mode` 职责**(level.proto / allocator.proto / 客户端 `CfgLevel.h` 三处注释):它**只**表达"这张图归哪个池",不表达人数与排位;`"5v5_ranked"` 是历史遗留标签;禁止按这个字符串解析人数或推断计分(§9.22 唯一权威——客户端此前已因同类串白名单被清过一次)。**未做:把取值改名成 `pvp`**——那要动 Redis key 空间(`pandora:match:<mode>:queue`,在途票据会变孤儿)、JWT audience(`matchmaker:<mode>`,过渡期须双 audience)与双仓四处配置,且"3v3 与 5v5 是否共用同一份段位分"是产品决策(共用→一个池改名;各自算→两个池两个部署,那时名字才不撒谎),**待拍板**。
  - **验证**:`buf lint` + go pb 重生绿;导表 v20260811001 → v20260811002 绿(11 行,前 15 列逐格未变,脚本内断言);`pkg/configtable` / `battle_result` / `ds_allocator` / `matchmaker` 四模块 build + test 全绿。新增用例:`settlementRunsElo` 六分支决策表、**`TestCanonicalRatingModeNoneStopsEloForNonPVEPool`(本次修复的回归判据,修复前必红:`casual_5v5` 池会得到 ±16 且读 MMR)**、`TestCanonicalRatingModeEloScoresRegardlessOfPoolName`(反向锁死,防止修成"只认识 5v5_ranked"的新 fail-closed)、matchmaker 侧定格与三种回落、加载期 ELO×side_count 矩阵、allocator 定格落库断言。
  - **交接(必须由人完成)**:①`Table/关卡/g_关卡.xlsx` 加了第 16 列,需 **SVN 提交**(AI 不提交);②客户端 `CfgLevel.h` / `MyLevelEnums.h` 已加字段与枚举,需**重新导表生成客户端 CSV → 重导 `CfgLevel.uasset` → UE 编译**(§11.6 UE 编译由用户执行);③服务端 `configtable/dist` 这批产物须**整批一起提交**。**未验证**:UE 侧编译与 uasset 重导、真实联机一局的端到端计分链。
- 2026-08-11(续 2:复核 7 项收口 + 发布门禁转绿)。b65d5cdb 之后的工作区修复,均未提交。
  - **docker 模式配置表漏挂**:mission 的 `config_table.dir` 是启动强依赖,集群产物指向 `/app/configtable/active`,而 compose 没把 `configtable/dist` 挂进去 → 容器起来即 fail-closed 退出。核查发现**漏挂的不止 mission**:`inventory` / `ds-allocator` / `battle-result` 同样缺(k8s 侧七个都齐,compose 只有三个)—— 典型的"两处手工维护、只改了想起来的那一处"。四个一并补齐,并新增 `configtable_mount_contract_test.ps1` 把**生成器白名单 / compose / k8s** 三处清单绑死(漂移即失败,已用去掉 mission 挂载的变异验证会红)。
  - **dbcheck 大字段漏登记**:mission 的 `progress` / `reward_pb` / `payload` 三列在 §9.24 表格、行数 registry、budgets 里都登记了,唯独 `bigFields` 漏了 → `-size-check` 对任务域完全不体检。补登记 + 新增 `bigfield_test.go` 双向漂移检查(登记的列必须存在 / DDL 里的大列必须登记或进 allowlist)。**该检查当场抓出一条存量 bug**:`bag_meta.snapshot` 登记错表(那张表根本没有 snapshot 列,实际在 `bag_checkpoint`),即随身组快照的大字段体检**从来没生效过**,已修正。另把 6 条 mail/player 域的存量欠账列进 allowlist 并写明"待推 MaxBytes 口径后登记",不再隐形。
  - **热更加条件会白送完成**(P0):`progressMission` / `allConditionsFulfilled` 两处都取 `min(len(condition_ids), len(progress))`。任务原本单条件 2/3,热更加第二个条件后,下一条属于条件 1 的事实把槽 0 推到 3 → 达标判定只看 min=1 个槽 → **直接判定全条件满足并发奖**,新条件一次都没被检查过。改为 `alignProgressSlots` 补零扩容(已有进度保留、新条件从 0 开始),达标判定按配置全部槽走、槽数不足一律 fail-closed;`toProtoActive` 下发前同样补零(否则 progress 与 targets 不等长,客户端逐槽渲染越界)。反向(热更删条件)刻意不截断,留着可在配置回滚后复原。
  - **发奖 GRANTED 可被打回 FAILED**:多副本补扫是刻意允许的(正确性靠下游幂等键,§15.3 不为此加 claim/lease),但 `MarkReward` 无条件覆盖 → A 副本发放成功写 GRANTED 的同时,B 副本因下游瞬时不可用写 FAILED,已发放的行被打回补发工作集,每轮重放、"陈年 FAILED = 发放链有 bug"的审计信号被淹没,且下游幂等记录过保留期(90 天)后再重放就是**真重复发放**。改为失败标记带 `status <> 1` 条件更新(成功标记仍无条件,终态推进幂等)。**同款缺陷在 `leaderboard_repo.go MarkReward` 也在**(同为无条件 UPDATE),属另一服务未动,已单列。
  - **发放形态未冻结 → 滚动升级双键重发**:装备走 `GrantInstances`(`:inst` 键)、堆叠走 `GrantItems`(`:stack` 键),**两个键在 inventory 台账里互不相识**。原实现在发放时才回读道具表,形态一旦在两次投递之间变化(热更改 `equip_slot`,或滚动升级期新旧副本加载不同配置批次),同一条奖励会先后用两个键各发一次,幂等键防不住。`MissionRewardItem` 新增 `optional bool equipment = 3`(显式 presence:缺省 = 冻结位上线前的旧行,回退读配置表并打 WARN,§9.17 双向兼容),快照落库那刻冻结,补扫重放永远走当初那条路由。proto 已重生(buf lint + generate 绿,只重写了 mission.pb.go)。
  - **任务出箱 FIFO 只按对局分组**:上一轮改成每玩家 FIFO 时,谓词仍带 `prev.match_id = cur.match_id`,于是 A 局卡住的队首挡不住 B 局的行 —— 任务链跨对局延续,玩家打完 A 再打 B 照样能让 B 的事实抢在 A 前面落地,同一个丢进度的洞只是要多打一局才踩到。改为**按 player_id 分组、按 id(插入序)排序**:seq 每对局自增跨局不可比;id 在对局内等价于 seq 序(一事务一批、批内按 seq 升序插),跨对局等价于对局发生序(§9.1 保证玩家同一时刻只在一个可操作 DS)。索引同步改 `(player_id, id)`,新增 `TestMissionOutbox_FIFOSpansMatches` 并用旧谓词验证会红。
  - **发布门禁**:`online_manifest_contract_test.ps1` 由红转绿。修的是**两条与实现脱节的死契约**——① placement-preflight:`-placement-preflight` 系列 flag 已于 2026-07-16 随 owner_epoch fencing 改造从 player_locator 二进制删除(`678d58d3`,manifest 同批 `4193897b` 清理),PowerShell 侧被落下,既断言一个不存在的形态硬阻断每次 online 发布,又会在 `activate_ds_auth` 路径**构造**一个跑已删除 flag 的 initContainer(部署即 CrashLoop);它守的性质现由 `dsauthfence.AcquireRuntime` 运行时租约保证,整段删除。② hub-allocator 单写者:R9 P0-7 起改由 writerlease 保证,services.yaml 早已是 `RollingUpdate{maxSurge:1,maxUnavailable:0}`,而门禁仍要求 `Recreate`。**没有直接删,改为断言当前真正的不变量**:replicas 恒 1 / 必须 RollingUpdate / `maxUnavailable=0` / `maxSurge≤1`,变异矩阵同步重写。`ds_auth_activation_contract_test.ps1` 也由红转绿:两条断言的字面量分别停在 2026-07-28 换掉的 `ssh -- docker image inspect`(containerd 节点无 docker CLI,已改走 `minikube image ls --format json`)和 R11 P0-5 前的 Hub `Recreate` 写法,按现实现对齐(守的性质不变)。
  - **验证**:PowerShell 契约测试 **20/23 PASS**(此前 18/22)。真实引擎回归全部实跑并逐条验过"先红后绿":TiDB 并发接取 12→3 / 互斥 8→1、出箱跨对局 FIFO、pending 扣除闸。touched 模块 build/vet/test 全绿,git 变更的 go 文件 gofmt 干净。mission README 补了"真实数据库用例"节(裸 `go test` 会 Skip 不等于通过,给出可直接跑的 DSN 命令)。
  - **仍红且非本批次所致**(均在干净 HEAD 复现,且都不在本次改动面内):`services_dsticket_secret`(matchmaker `legacySigner`,该文件当前有并发编辑者)、`gen_cluster_b1`(`$PlacementSecretBindings` 是空数组,产物里根本没有 placement 分权 key)、`ds_entrypoint_log_redaction`(DS entrypoint 脚本本批次零改动)。`ds_allocator` 两个 go 文件 gofmt 不干净,同属并发编辑者在途改动,未触碰。
- 2026-08-11(续):**任务域移植的对抗式复核 + 收口**。四维度复核(半成品接线 / 移植保真度 / 分布式正确性 / 登记面与发布)提出 13 条,每条派独立 agent 证伪后**确认 7 条、推翻 6 条**。三条真会出事:①**发放时回读道具表决定装备/堆叠路由** —— 首投走 `:stack` 键成功后若 MarkReward/经验段/进程崩任一失败使行留非 GRANTED,期间道具被热更改成装备,补扫重放就换成 `:inst` 键,inventory 查无此键 → **同一份奖励发两次**;部分翻转时反而撞 `claimLedger` 请求指纹冲突 fail-closed → **奖励永久发不出去**。修法是把路由**冻结进快照**(`MissionRewardItem.equipment`,`buildRewardLog` 落快照那刻写,`deliver` 只读快照)。②**完成判定取 `min(条件槽数, 进度长度)`** —— 给已上线任务热更加一条条件,存量玩家 progress 比 condition_ids 短,推进与判定两处都只看 min 个槽 → 打满旧条件即判全条件满足 → **白送完成并发奖**,新条件一次没查过。修法 = 推进前 `alignProgressSlots` 补零扩容(只扩不缩,配置回滚可复原)+ 判定改逐 condIDs 且槽不足 fail-closed。**注意这个 min 是从 C++ 原版 `mission.cpp:155` 忠实移植的** —— D 版那个 min 覆盖的是"条件行查不到就不加槽",而 Go 侧 accept 恒等长,唯一造成短槽的路径(配置增列)原版不存在;**忠实移植不改变它在新语境下是 bug 的事实**,这是"照搬即正确"的思维盲区,记录备忘。③compose 漏挂 configtable → docker 模式(策划一键入口默认路径)mission 启动即 fail-closed 退出。另补登记三处:`$ProdDbCredentialDebt`(**实害最大**:运维照该清单逐个接线生产 DSN,漏的恰好是存玩家任务进度与发奖流水的库,产物会带 `pandora_dev_pwd` 进生产且既不断言也不告警)、`.goreleaser.yaml` 少一个 mission 二进制、inventory-mesh enforce 策略未给 `sa/pandora-mission` 授权(mesh 启用后发奖全被 RBAC 拒)。**唯一刻意不修的一条**:push 出箱是全局 FIFO 而非每玩家 FIFO,单分区故障会放大成全服推送延迟——但 battle_result / player 两处同构,只改 mission 会在三条同类链路留两套纪律,已在 `RunPushPublisher` 注释归档已知代价与"要改就三处一起改 + 按 §15.4 举证"的口径。**闭环判据**:为①②补了三条回归测试(`TestDeliverUsesFrozenRouteAcrossCatalogFlip` / `TestDeliverFrozenRouteHoldsForEquipmentToStackFlip` / `TestHotAddedConditionIsNotSkipped`),并**实测把代码改回旧写法后三条全部变红、还原后全绿**(不是"写了个测试恰好过")。另自查删掉 `MissionUsecase` 里只写不读的 `router` 字段(§14.1 禁"以后再接"的钩子;全仓先例是接了就真读或完全不接,无中间态),并修正设计文档三处与代码漂移——其中 §5.1 描述的还是实现期被推翻的方案(复用出箱表 kind),已改成实际的独立表并把推翻理由入档。
- 2026-08-11:**后端 Jenkins 权威源码人工拍板从旧 SVN 切回 GitHub `main`**。根因是开发提交已推送到 `luyuan-go/XuanMing-Server`，而 `backend-dev` 仍轮询 `^/trunk/Server`，所以只报 `No changes`、不会产生新 Go 构建。持久配置改为公开 GitSCM + `H/5`，后端制品恢复 `g<sha>`；客户端/DS 保持 SVN `r<rev>`，`artifacts-sync` 本轮仍从 SVN 读取流水线定义。迁移与验收见 `docs/design/decision-revisit-backend-ci-source.md`。
- 2026-08-11(续 3:复核轴 FAIL 的收口 —— 单写者选举、条件比较符、读写侧上限、事故建档)。
  上一轮把「部分关键修复已落码并通过单测」误写成了接近完成的口径,复核给出 Standards 轴 FAIL +
  Spec 轴 7 类缺口。本条按缺口逐项收口,**并明确哪些物理上无法在本轮闭环**。
  - **条件比较符在单调累加计数器上不成立(P0 级配置面 fail-open)**:任务进度是单调不减的累加器
    且达标槽不再累加,所以比较符的达标集合**必须向上闭合**,只有 GE/GT 满足。而 ①`ConditionClampIfFulfilled`
    无脑钳到 `target`,GT 下「进度 6 达标 → 钳回 5 → 再判 5>5 为假」→ **进度永久钉死、任务 100% 完不成**;
    ②LE/LT 在 `progress=0` 即为真 → 该槽永不累加、恒定达标(白送);③EQ 是单点集合,`amount>1` 一步
    跨过后永远不再相等。加载期 `validateConditionRow` 却明确放行 0..4 全部取值。修法:钳位改落**最小
    达标值**(GE→target,GT→target+1,溢出保护),并在 `ValidateMissionCrossTables` 拒绝 LE/LT/EQ 用作
    任务条件(**放任务域而非条件表**:条件件声明为跨系统通用,快照型消费者用 LE/LT 是合法的)。
    现网 dist 三行全是 GE,**今天没坏**,风险面是"策划下次填一次比较符列"。先红后绿实测:退回旧钳位 →
    `进度钉死在 [5]`。
  - **装备件数加载期判单条、发放期判累计,两个口径**:「10 个不同装备各 64 件」= 640 件整批过审 →
    落进 `reward_pb` 快照 → 任务同事务置 CLAIMED → `deliver` 在累计闸上**永远发不出去**。快照是发放
    唯一入参不回读配置表,改表也救不回在途行:玩家永久损失该任务全部奖励(含经验,装备失败先 return
    走不到经验段);补扫每轮重试无上限;`SweepRewardLog` 只清 GRANTED,这些 FAILED 行**永不清理**并
    淹没"陈年 FAILED = 发放链有 bug"的审计信号。改为整条奖励累计判(带饱和加防回绕),与发放侧同口径。
    先红后绿实测:退回逐条判 → `装备累计 66 件超上限 64`。
  - **配置批次可在单次事务内撕裂**:`catalogFromStore` 每个方法各取一次 `Store.Tables()`,而一次
    `ApplyFactsTx` 回调里 catalog 被调几十次。最坏一条:`buildRewardLog` 用批次 A 的 `RewardByID` 拿
    奖励内容、用批次 B 的 `IsEquipment` 定装备/堆叠冻结位 —— **冻结位的全部意义就是"路由必须与快照
    同源"**,撕裂之后它守的东西恰好被绕过。改为 `CatalogSource.Snapshot()`:操作入口取一次 `*Tables`
    钉住,整个回调复用同一指针(inventory 的 `Lookup` 是单方法自足所以不需要,任务域不是)。顺带补上
    inventory 早有而 mission 缺的 nil 守卫(原写法启动竞态会 nil panic)。
  - **推送出箱多副本乱序(mission + player 两处同批改)**:出箱是**全局未分区**表、按 id 序整表 FIFO,
    属 §9.21「同一未分区权威的单写者循环」。两副本各持一份内存快照交错投递,而 `MissionUpdateEvent.
    progressed` 与 `PlayerExperienceEvent` 都是**全量/绝对值快照**(后到即覆盖),事件里没有任何
    revision 可判旧(`ts_ms` 是各副本墙钟,protocol-ordering-rules §5-B 明令不得只靠它)。后果:玩家
    进度条从 7/10 退回 3/10、等级经验条倒退。**注意 replicas:1 不等于单进程** —— Deployment 是
    RollingUpdate(§9.16 硬要求),maxSurge 让每次发版都有并存窗口。修法接既有 `writerlease`(零 schema、
    零额外往返、已在 ds/hub_allocator 生产跑着),**两道闸**:产物侧 `gen_cluster_config.ps1` 机械改写
    `mode: enforce` + etcd 端点;进程侧读 Deployment 注入的 `PANDORA_DEPLOY_STRATEGY`,
    RollingUpdate × 非 enforce **fail-closed 退出**。只包发布器 —— 补扫靠下游幂等键、清理靠 DELETE 幂等,
    按 §9.21「可并行 worker 不得强行全局串行化」不得一起包进来。**battle_result 的 `player_update_outbox`
    刻意不接**(下游 `mmr_history` uk 幂等、无客户端可见覆盖语义),三条链路之间是**有据的差异不是漂移**。
  - **完成集读写两侧都没有上限**:§9.18 逐字要求写入侧硬上限**与**读取侧上限**同时**存在,而
    「完成集被任务表行数有界」此前只是一句描述,没有任何代码拒过批次;`loadState` 的 done 查询一个
    LIMIT 都没有,且它**同时**服务 `FOR UPDATE` 事务路径(每次 ReportMissionFacts 都全量载入,行锁数与
    事务时长随之线性增长,是热路径事务放大而非展示问题)。修法:新增 `MaxMissionRows=2000` 加载期拒批次
    + **只在只读路径**加 `ORDER BY mission_config_id LIMIT 2000`;**事务路径刻意不加**(截断会让已完成
    任务被判成可重新接取 → 重复发奖,把展示问题升级成发奖 bug),该理由已写进代码注释与 CLAUDE.md §9.18
    新增的登记行。
  - **`mission_addr` 全环境未配 → 任务事实链从未通电**:与 `inventory_addr` 那种"未配也不丢"的弱依赖
    **语义不同**,任务事实是**未配即不产**(未配时一行出箱都不写),已发生的战斗事实**无法事后补齐**。
    dev + prod 模板补齐,产物侧确认改写为 `mission:20019`。开通前置(migrate 到 pandora_battle 000010、
    §9.21 Go 先行)与线上第二道闸(`-Prod` 把 `progress_enabled` 机械钉死 false)一并写进注释。
  - **顺带补上 `run/cluster/etc/mission.yaml` 根本不存在**:生成器自 mission 落地后一次没跑过,而
    compose 与 k8s 都要挂它 —— 一键起栈会直接卡在缺失的挂载源上。已重跑生成器,22 份产物齐。
  - **P7 三小项**:①proto reserved —— 查 git 历史确认 mission.proto **从未删过字段编号**,无需 reserved,
    该条不成立;②RPC 完成语义 —— `CompleteAllMissions` 的 proto 注释写着"不广播完成事件",而
    `biz.CompleteAll` 确实产出推送。**复核后判定实现是对的、注释是错的**(不推等于让客户端留一份与权威
    不一致的活跃列表,GM 改了状态却看不见;推送不承担正确性,推它不会把两条路径合并),已改注释并写明
    更正日期;③登记面 —— `mission_player_guards` 漏登记进 infra.md 表清单(CLAUDE.md §9.24 豁免里有),已补。
  - **事故建档(Standards 轴)**:按 §16.9 + incidents/index.md §1 第三条(上线前发现、若上线即 P0 → near-miss)
    建 `INC-20260811-001`。**刻意合并为一份而非五份**:这不是五次独立事故,而是同一个事件(批次上线前被审计),
    共享同一时间点、同一发现途径、同一影响面(零);为五个从未运行过的缺陷各建一份会把审计发现伪装成五起
    生产事故,违反 §1「不得伪装成线上事故」。五条根因在 §5 分节独立取证。同型扫描命中
    `leaderboard_repo.go MarkReward` 同款无条件 UPDATE(**另一服务未动**,已列行动项 A-1)。
  - **验证**:touched 模块 build/vet/test 全绿;git 变更 go 文件 gofmt 干净;`buf lint` 绿;真 TiDB
    集成用例实跑通过(本机 4000 容器,`PANDORA_TEST_TIDB_DSN` 显式设置);新增两组清单契约测试
    (Deployment strategy ↔ annotation ↔ env ↔ 集群产物)全绿;PowerShell 契约 `configtable_mount` /
    `local_k8s_profile` / `gen_cluster_session_gate` / `online_manifest` 四条 PASS。
  - **仍红且已证实非本批次所致**:`services_dsticket_secret` / `gen_cluster_b1` /
    `ds_entrypoint_log_redaction` 三条 —— 本轮用 `git worktree --detach HEAD` **实际复验**,干净 HEAD 上
    同样 FAIL。
  - **未闭环(不得当成已完成)**:①`go test -race` —— CI 唯一测试入口 `ci_backend.ps1:50` 根本没有 `-race`,
    且 Jenkins 跑 Windows(`bat`),本机 `CGO_ENABLED=0` 下直接报错,属**阻断项**;②真实 MySQL 后端未跑
    (本机只有 TiDB 容器,mysql 子测试 SKIP —— 而 SKIP 在 CI 报告里与 PASS 无法区分,已列 A-4);
    ③**未部署到任何环境**,现有 `pandora/mission:dev` 镜像早于本轮修复必须重建;④观察窗口为零,且
    `deploy/grafana` 5 条告警规则里 mission 相关 0 条;⑤**玩家 E2E 物理上不可执行** —— 客户端零 mission
    接线(无 cpp pb、无 `Module/Mission`、无任何 RPC 调用点,仅 7 张图标贴图),属跨仓 + 需用户编译 UE;
    ⑥`player.proto` 已加 `instance_id` 但 `player.pb.go` 未重生,**player 模块当前编译失败** —— 属并发
    编辑者在途改动,按 §5.1 proto 重生是 Codex 的活,未代跑。
  - **补测本轮新增的五条闸(自查发现零覆盖)**:`MaxMissionRows` / `MaxMissionNextIDs` /
    `MaxConditionSlotValues` / `MaxRewardItemEntries` / `doneReadLimit` 落码时都只有生产引用、
    没有任何测试引用 —— 已补齐(含边界值放行用例)。其中 `doneReadLimit` 的判据是**设计判断本身**
    而非"有没有 LIMIT":只读路径必须截断、事务路径**绝不能**截断(截断会让已完成任务被判成可重新
    接取 → 重复发奖)。真 TiDB 实测先红后绿:把 LIMIT 改成两条路径共用 →
    `事务路径必须看到全部 6 行完成任务,实为 2`。
  - **仍未执行(代码侧唯一自带缺口)**:`push_writer_lease.mode=enforce` 分支 ——
    `pushIsLeader()` 用 fake 租约验过、清单与产物契约验过,但 `writerlease.Start()` 的**真实选举
    路径一次都没跑过**(本机 2379/2380 均未监听 etcd)。§14.2 要求"开关打开后的分支必须是完整
    可用的真实实现",当前是代码完整但未执行,首次上线前必须起 etcd 冒烟一次。
- 2026-08-11(续 4):**修复 backend-dev #17 的 Windows OEM 编码假失败**。`#17` 已成功检出
  `e0a10786`,33 个 Go module 的 build/test 全绿,但 `gen_cluster_prod_account_contract_test.ps1`
  用中文片段区分五类 Account DSN 拒绝原因;Jenkins `cmd` 的 OEM code page 把子 `pwsh` stderr
  转成 `?`,生成器已经按预期拒绝公开 dev 凭据,测试却因匹配不到中文而误报失败,继而让
  `ci_backend.ps1` 返回 1、跳过 `Publish Offline Images`。修法是在五条生产 Account DSN FATAL
  中增加稳定 ASCII 标识(`[ACCOUNT_DSN_*]`),测试继续同时断言非零退出码与精确原因标识,没有
  降级安全门禁。验证在干净 `e0a10786` 临时副本中只叠加本修复:普通 `pwsh` 与
  `cmd/chcp 437/pwsh` 针对性测试均 PASS;OEM 437 下完整 `ci_backend.ps1` 33 module + 7 contract
  tests 全绿(exit 0,162.1s)。当前主工作树另有 dialogue/configtable 在途改动会更早触发独立生成器
  门禁,未纳入本修复;修复尚未提交/推送,因此 Jenkins 尚未产生下一次成功构建或新 Go 制品。
- 2026-08-11(续 4:代码侧剩余四处收口 + 一条被遮蔽的生产缺陷)。上一条自查承认"代码方面
  没有闭环",本条把那四处做完,并在过程中挖出一条**此前被恒红断言遮蔽**的真实生产缺陷。
  - **enforce 分支从"代码完整但未执行"变成实跑通过**:起本机 etcd 真选举,验三件事 ——
    `writerlease.Start()` 能连上并选出 leader、第二副本在持有期内拿不到领导权、
    `*writerlease.Lease` 直接满足 `PushWriterLease` 接口。**跑的过程当场抓到一个 nil-deref**:
    测试里 `&MissionUsecase{}` 直连构造漏了 log,`pushIsLeader()` 的跃迁日志炸了 —— 这正是
    fake 租约验不出、只有真跑才暴露的那类接线问题。用例按 DSN 同款纪律门控
    (`PANDORA_TEST_ETCD_ENDPOINTS` 未设即 Skip),README 补了可直接粘的复现命令
    (含 Git Bash 会改写容器内路径的坑)。
  - **A-1 leaderboard `MarkReward`**:同款无条件 UPDATE 已修(失败标记带 `status <> GRANTED`),
    抽出 `buildMarkRewardSQL` 以便断言(对齐本文件既有 `buildSaveSnapshotSQL` 写法),补两个
    子用例(失败标记必须带守卫 / 成功标记不得带守卫)。
  - **A-2 出箱发布器全仓扫描**:8 张出箱表逐张按三条判据(未分区整表 FIFO / 客户端可见全量
    快照 / 下游无幂等)判定,只有 mission + player 两张 push 出箱同时满足,均已修;其余六张
    的"不接"逐条写明理由。归档 `docs/reviews/2026-08-11-outbox-single-writer-audit.md`。
  - **三条契约测试全部转绿,但三条根因完全不同**:
    ①`ds_entrypoint_log_redaction` = **死契约**:断言写死 `exec "${SERVER_SH}"`,而 c37e8660
      (2026-07-30)为 stdbuf 行缓冲把启动命令重构成数组 `${FINAL_LAUNCH[@]}`,契约被落下。
      它要守的性质(强制日志参数必须排在 `${EXTRA_ARGS}` 之后,否则被用户参数覆盖回 Display 级、
      含 DSTicket 的 URL 重新进 stdout)一直成立。改为**不再匹配启动变量名**(那正是上次写死的
      地方)只匹配顺序,并做了变异验证:把强制参数挪到 EXTRA_ARGS 之前 → 必红。
    ②`services_dsticket_secret` = **死契约**:字面量禁用 `legacySigner` 写于 3ba27c3c(07-13),
      而 2f369c22(08-04)引入的 `ds_local_profile=local-off-v1` 是显式声明、与 v2 私钥互斥、
      构造时打 WARN 的**认可例外**。要守的从来不是"标识符不许出现"而是"不得静默回退",
      改为断言三件结构事实(只允许一处赋值 / 只能在显式本机档分支里构造 / 未声明必须走
      default fail-closed)。同处另一条 `..., nil, v2Signer` 字面量同因同批修正。
    ③`gen_cluster_b1` = **未实现能力的断言**,与本文件 R11 已清理的三条同类:生成器有参数、
      有解析、有注入循环,但 `$PlacementSecretBindings` 是空数组,且 **Go 侧零 placement 密钥
      conf 字段**,注入无处可注。它恒红,并且因为 Assert 抛出,**其后 14 条断言一条都没跑过**。
      按本文件既有结论(「正确修法是反方向:先落 conf 字段 + 生成器注入,再把断言加回来」)
      隔离为显式 TODO(A-10),断言逻辑原样保留在 if 内未作任何弱化。
  - **隔离后立刻暴露一条真实生产缺陷(A-11,已修)**:`login.matchmaker.auth_secret` 没登记进
    `$MatchResumeAuthSecretBindings`(此前只绑两个 matchmaker)。这把 key 与
    `matchmaker.match.match_resume_auth_secret` 必须**成对同值**(Go conf 注释与 dev yaml 注释
    都明写)。漏绑的后果:运维用 `-MatchResumeAuth <生产密钥>` 时 matchmaker 拿生产密钥而
    **login 留着 dev 密钥** → ①login→matchmaker `ResolvePlayerMatchContext` 全部被拒,而那是
    2026-07-15 P0 修复的兜底路径(locator presence 未命中 BATTLE 时改查耐久权威,READY 局投影
    蒸发也能路由回原局),会**静默失效**;②dev 密钥被带进生产。这条正是"恒红断言替真门禁挡枪"
    的实例。
  - **`inventory_mesh` 契约重锁**:e27ffc63 给 mesh 策略加了 mission 的 principal 却没更新审核锁,
    自那次提交起恒红(干净 HEAD 复验同红)。重锁前逐条核对新增授权确是最小权限:只给
    `sa/pandora-mission` 两个方法 `GrantItems`/`GrantInstances`,而 mission 侧实际调用面核实
    无第三个 inventory 方法(经验走 player、满包溢出走 mail),策略除该段外无其它改动。
  - **验证**:PowerShell 契约测试 **20/20 PASS**(此前 20 条里 4 条红);pkg / mission /
    battle_result / player / leaderboard 五个模块 build+vet+test 全绿;真 TiDB 与真 etcd 用例
    均实跑通过。

### 同日更正:编号跟**角色**走(卖角色业务;初稿结论作废)

- **初稿写反并已作废**:曾在 §3.6.1 写「多角色改造时 register_no 必须跟 account_id 走」,
  理由「注册是账号事件」。**用户推翻**:本项目**角色可交易(卖角色,按过户设计)**,
  编号是角色的固有属性与资历凭证(「第 3 号角色」是定价要素);编号若挂账号,
  角色过户后编号即变、资历蒸发。**一账号建 N 角色 = N 个编号**。
- **代码零改动,现状即正确形态**:`register_no` 挂 `accounts` 而该表 PK 是 `player_id`,
  今天 `player_id` 承担的正是角色身份 → 改造时它下沉为角色实体 ID,编号随之落到角色表,
  天然「一角色一编号」。改造落点两处(均小):编号列随 player_id 进角色表(纯账号表不带该列);
  补号任务扫描目标改角色表。`GetRegisterNo` RPC **签名无需改**(本就是查当前角色的编号)。
- **新生待拍板(§6 已登记 E/F)**:E=删角色是否回收编号(建议不回收:回收会让两角色先后
  持有同一编号,交易场景是欺诈风险;代价是最大编号=累计创建数而非存活数);
  F=账号是否也要独立编号(若要须另起计数器与列,不得复用 register_no,现不预留)。

### 同日续:编号语义全仓统一为「角色编号」(文档+注释完整对齐)

按用户拍板(编号绑定角色实体,卖角色过户随角色走),把**所有基于旧前提写下的表述**审改一遍
——不止 §3.6.1 那一节。改动全是文档与注释,**业务逻辑零改动**(现实现本就正确)。

- **DDL 三处**(mysql-init / tidb-init / 新增 000005 注释迁移):`register_no` 列 COMMENT 补
  「绑定角色实体——今 player_id 即角色身份,卖角色过户时随角色走、值不变,一账号建 N 角色
  = N 个编号」,并把禁用清单补全为「身份键/外键/幂等键」。
- **proto**(两处字段,go pb 已重生成、buf lint OK):`LoginResponse.register_no` 与
  `GetRegisterNoResponse.register_no` 均补角色语义 + UI 呈现口径(「该角色的编号」而非
  「账号编号」);前者顺带删掉「下次登录即有值」的陈旧说法(补拉 RPC 已取代)。
- **Go 注释**:biz/data 的 `GetRegisterNo` 从「查玩家注册编号」改为「查**当前角色**的编号」,
  并说明入参名 playerID 是历史口径、多角色改造后签名不变;`register_no.go` 头部补语义归属段
  + 澄清「不得作身份键」约束的是别的服务拿它当键,不妨碍它是角色的固有展示属性。
- **文档**:§0 新增第 0 条(读本文先读:归属主体=角色 + 旧结论作废);§1.1 声明归属主体并
  统一旧措辞;§1.2 补关键佐证——**梦幻的编号本来就是角色编号**(一账号多角色 + 藏宝阁角色
  交易,老号溢价),反向印证归属判定;§3.1 三问表升为四问并全部标已拍板(⓪归属主体);
  §3.6.1 整节重写(含纠错声明);§6 新增拍板项 E/F。
- **顺带修正一处残留论证**:§1.3「编号挂账号库的原因:注册是账号事件」用的正是被推翻的
  前提。结论(挂账号库)不变但理由换成**落点的分片宿命**(玩家库注定按 player_id 切开,
  全局连续计数+全局唯一索引需要「宿命是全局一份」的落点)。
- **由此发现并记录一条改造期硬约束**(此前无人写过):`register_no`+`uk_register_no` 随
  player_id 落到角色表后,**角色表落在哪个库决定唯一性怎么保**——若进按 player_id 分片的
  玩家库,`uk_register_no` 退化为分片内唯一,全局唯一性只剩补号事务保证,uk 从硬兜底降级
  为软兜底,**必须显式接受并记录**,不能默认 uk 还在保护全局唯一(§16.1)。
- **验证**:login `go build`/`go vet`/`go test ./... -count=1` 全绿;
  真 MySQL(k8s pf 13306)+ 真 TiDB(4000)双后端 `RegisterNo_MySQLAndTiDB_*` 全绿。

### 同日续:删角色不回收编号(拍板项 E 已定)

- **用户拍板**:角色删除后 `register_no` **永不回收**、永不再分配。
- **机制保证已存在,零新增代码**:保证来自 `next_no` **只增不减**(补号事务只做
  `next_no + N`,无回退路径)→ 物理上不可能重发已发过的号;**与删除逻辑无关**
  (角色行物理删还是软删都不会重号)。已把这条写进 `register_no.go` 头注释并标注
  「本文件是该保证的唯一承载处」+ 运维红线(禁止重置计数器/按存活角色回填,会直接制造重号)。
- **口径影响(策划须知)**:「最大编号」= **累计创建**角色数,≠ 当前存活角色数(差值=历史
  删除量);编号在存活集合上有洞。故 §3.1 ①「严格连续无空洞」的准确表述改为
  **发号序列连续、存活集合可有洞**;§0 速览同步修正。
- **文档**:新增 §3.6.2(决定/理由/机制保证/代价/运维红线/衍生问题);§6 拍板项 E 标记已定。
- **衍生问题留给多角色改造立项**(与本拍板独立,不预判):删角色若**物理删除**,该编号将
  「查无此人」——客服与交易纠纷拿编号追溯时查不到任何记录(编号既不回收也不指向任何行)。
  要编号永久可追溯需选**软删除**;取决于角色删除的产品形态(可否恢复/冷却期),不属编号方案职责。
- **验证**:login `go build`/`go vet`/`go test ./... -count=1` 全绿。

### 同日续:字段改名 register_no → player_no(一次性改完)

**理由**:编号已绑定角色实体(§3.6.1),而「register(注册)」在通行语境是**账号**动作,角色是
**创建**出来的——留旧名会持续把人往「账号级编号」引。改后与既有体系配对:
`player_id`(角色实体 ID)/ `player_no`(角色展示序号)/ `role_id`(职业配置 ID)。
**刻意不叫 `role_no`**:`role_id` 已被 CfgRole.Id 占用,再来一个 role_* 比不改更糊涂。
**此刻改成本最低**:生产零注册路径无存量数据、dev 库可重建、客户端尚未正式消费该字段。

**改名范围**(24 个文件,标识符零残留):列 `player_no`、唯一索引 `uk_player_no`、计数器表
`player_no_counter`、Go(SweepPlayerNo/EnsurePlayerNoCounter/GetPlayerNo/PlayerNoBatchSize/
PlayerNoWatermarkLag/配置 `player_no_start`)、proto(LoginResponse.player_no、
GetPlayerNoRequest/Response、rpc GetPlayerNo、HTTP `/v1/player-no/get`)、envoy jwt_authn
rules path、日志 msg、文件名(data/player_no.go、docs/design/player-no-and-login-surge.md)。
中文措辞统一「注册编号」→「角色编号」。**PROGRESS/HANDOFF 历史条目保持原样**(历史即历史)。

**迁移 `000005_rename_player_no`**:四步条件执行(RENAME COLUMN → RENAME INDEX →
RENAME TABLE → MODIFY COMMENT),全部幂等。**000004 保持原样**(README:迁移一旦执行即
immutable;本次曾误改过它的 COMMENT,已回滚)。两条路径最终一致:`deploy/*-init` 建的新库
直接是 player_no(四步全跳过),迁移建的库由本版改名。用 `RENAME COLUMN` 而非
`CHANGE old new <type>`——后者需完整重复列定义,漏写 UNSIGNED/NULL 会**静默**改变列语义。

**踩到并已加警示的坑**:批量改中文词时误伤了 down.sql 的 `@old_comment`——它必须与 000004
的 COMMENT **逐字一致**(含旧词与旧文档名),是回滚的匹配目标而非改名产物;被换掉后条件
判断永远不成立,每次回滚白发一次 DDL。已在该处加注释警示。

**验证**:①临时库跑通 000004→000005 全链,列/索引/表全改名且 `bigint unsigned`+唯一性
(NON_UNIQUE=0)不变;②重复跑 000005 幂等(输出 SELECT 1 跳过);③down 三样全还原;
④dev 活库应用后数据完好(编号 1/2/3 与计数器 next_no=6 原样,印证只动元数据);
⑤login/migrate/friend/mission `go build`+`go vet` 全绿;⑥login `go test ./...` 全绿;
⑦真 MySQL + 真 TiDB 双后端 `PlayerNo_MySQLAndTiDB_*` 5 组×2 后端全绿。

**当前环境状态(重要)**:dev 库已改名,但 **login pod 仍跑旧镜像**(找 register_no)→
补号任务每 5s 一条 `register_no_sweep_failed` Warn,**pod 1/1 Running 登录不受影响**
(fail-soft 承诺实测成立)。**待 Codex**:①构建部署新 login 镜像后 Warn 自动消失、编号恢复;
②cpp pb 同步(rpc 与字段均改名,标 [proto]);③UE 侧字段名与 UI 文案随之更新。
- 2026-08-11(**段位分区缺口**:用户拍板「3v3 与 5v5 不共用同一份段位」,查证结论=当前实现不支持,**未做,待拍板**)。上一条落地 `rating_mode` 后追问「3v3 与 5v5 是否共用段位分」,用户答**不共用**。这个答案否掉了上一条留的「改名成 `pvp` 单池」选项,同时暴露出一个比命名严重得多的缺口。
  - **查证:全链路零玩法维度**。`players.mmr` 是**单列全局值**(`04-player-tables.sql`:`mmr INT NOT NULL DEFAULT 1500`,附 `idx_mmr`);`MMRReader.GetMMR(ctx, player_id)`、`PlayerUsecase.UpdateMMR(ctx, player_id, delta, reason, idem_key)`、`mmr_history`(uk 是 `player_id+idempotency_key`,idem_key=match_id)、`PlayerUpdateEvent.mmr_delta`、matchmaker 票据 `avg_mmr`(`resolveMembers` 取的就是这一份)——**没有任何一处带玩法/池维度**。所以"各自段位"不是配置问题,是**存储模型缺一个分区维度**;`rating_mode` 只回答了"算不算",没回答"算哪份"(NONE 那半边完全成立,已修掉的 fail-open 不受影响;ELO 那半边目前恒指向同一份全局分)。已把这条**写进 `LevelRatingMode.LEVEL_RATING_MODE_ELO` 的 proto 注释**,防止下一个人以为填了列就已经分玩法计分(§14 不留假象)。
  - **岔口(必须先拍这个,两条路自洽但不可混)**:
    - **(a) 各自成池**:新建 `3v3_ranked` matchmaker 部署,id 9 改挂过去,段位分区键**复用 canonical game_mode**(已定格在 `BattleStorageRecord`,零新增列)。代价=每种人数一套部署 + audience + Redis 命名空间 + envoy 路由,加个 4v4 就再来一套;好处=`game_mode` 名字里的数字终于不撒谎。
    - **(b) 撮合池与段位池正交(推荐)**:撮合维持一个池不动(**撮合隔离已由 `partitionTicketsByMap` 按 map_id 分组解决**,3v3 图与 1v1 图本来就不会串局),另加关卡表列 `rating_pool` 表达"算哪份段位"(`rating_mode=NONE` 时留空)。代价=一列 + 分区存储;好处=不新建任何部署,且把本次教训推广了——**一个字符串不扛两个语义**。走 (b) 后 `game_mode` 就真的只剩"哪个撮合池"一个语义,那时改名成 `pvp` 反而重新成立。
  - **无论走哪条都要做的存储改造**:`players.mmr` 单列 → 分区存储(如 `player_mmr(player_id, rating_pool, mmr)`),`GetMMR` / `UpdateMMR` / `mmr_history` / `PlayerUpdateEvent` 全链路补分区键,leaderboard 竞技分榜按 `board_type` 分榜(**榜那边已有复合 BoardKey 维度,数据源改了即可**),客户端段位 UI 要能显示多份。
  - **拍板点(涉及玩家已有数据,不替用户定)**:①存量单值 `mmr` 怎么分裂(复制成两份 / 只留给 5v5 而 3v3 从基线起 / 全部重置);②新分区初始分=1500 基线还是继承;③要不要顺带做赛季——**若要,分区键现在就该是 `(rating_pool, season)`,事后再加比现在加贵得多**;④(a)/(b) 二选一。
  - **本次未写任何段位存储代码**:每一步都依赖上述拍板,且动的是玩家已有段位数据,做错要回滚不可逆(§9.24「不因数据大自动删」同源的谨慎)。已落地的 `rating_mode` 保持不变且仍然正确。
- 2026-08-11(续:段位按 rating_pool 分区落地 —— 用户拍板「3v3 与 5v5 不共用同一份段位」+「已有玩家数据清空」)。上一条把「算不算段位」从池名里拆成 `rating_mode` 列后,用户回答了那个待拍板问题,答案直接推翻了「把 game_mode 改名成 pvp」的方案,并暴露一个比命名严重得多的缺口。
  - **查证:per-mode 段位在旧实现里根本不存在**。`players.mmr` 是**单列全局值**,`MMRReader.GetMMR(ctx, player_id)` / `PlayerService.UpdateMMR(player_id, delta, reason, idem_key)` / `mmr_history` / `PlayerUpdateEvent.mmr_delta` **全链路零玩法维度**。所以「3v3 与 5v5 各自算分」不是配置问题,是**段位存储模型缺一个维度**;`rating_mode` 只回答了"算不算",没回答"算进哪一份"。
  - **方案选择(否决了新建撮合池)**:撮合隔离**早已由 `partitionTicketsByMap` 解决**(同池内按 map_id 分组,3v3 图与 1v1 图本就不会串局),所以「段位分开」**不需要**为 3v3 新建一整套 matchmaker 部署 / audience / Redis 命名空间(§15.2)。改为**新增关卡表第 17 列「段位池」`rating_pool`**,与 `game_mode` **正交**:前者=算哪一份段位,后者=去哪个撮合池。这也把本轮教训推广了一层——**一个字符串不许同时扛多个语义**。副作用:`game_mode` 从此只剩"撮合池"一个含义,将来真想改名成 `pvp` 反而变简单了(但仍未做,无必要)。
  - **修正了我自己上一条的一个论断**:上一条写「若要赛季,分区键现在就该是 (pool, season),事后加贵得多」。**这不成立**——赛季首次上线本身就是一次段位重置,与本次"清空数据"是同一个动作。故**刻意不预留 season**(§15.3),将来加的代价与今天相同。
  - **落地(§14 一次做完,不留半成品)**:①关卡表 `rating_pool` 列(第 17 列,proto+xlsx+dist 三处同步,dist 走生成器重生 v20260811004→v20260812001);②加载期**两向校验**:`ELO 必须填池` / `非 ELO 必须留空` / 超长拒(列宽 32,非严格 sql_mode 会静默截断成**另一份**段位);③新 `pkg/rating`(`DefaultPool="default"` + `Normalize`,**刻意不设池名白名单**——白名单等于每开一档玩法都要改代码发版,与 §17.1 相悖);④定格链 matchmaker(`ratingPoolForMap`,**刻意不在此归一化**,空值要能被识别成"未定格")→ `AllocateBattleRequest.rating_pool` → canonical `BattleStorageRecord.rating_pool`(field 23)→ `TerminalReleaseRecord` → 结算;⑤`player_mmr(player_id, rating_pool, mmr)` **新表 = 段位唯一权威**,`players.mmr` 退役,`mmr_history` 加 `rating_pool` 列;⑥`GetMMR` / `UpdateMMR` / `PlayerUpdateEvent` 全链补分区键,`assignMMR` 读**本局那一份**段位(读错池 → Elo 期望胜率失真);⑦`PlayerProfile.mmr`(field 4)退役为 `reserved`,改 `repeated PlayerRating ratings`(**只含已有记录的池**,没打过的池不占位,客户端据此区分"未定级"与"分刚好是基线");⑧登记面:dbcheck registry + CLAUDE.md §9.24 豁免清单 + `budgets.go` 容量预算 + `tools/migrate` 000007(条件 DDL 幂等 + `ALGORITHM=INSTANT`,被 `TestPandoraPlayerExperienceMigrationIsInitSafe` 的契约点出来);⑨客户端 `CfgLevel.RatingPool` + `EMyLevelRatingMode`。
  - **并发正确性(关键)**:`player_mmr` 行**首战才创建**,而 TiDB 无 gap 锁 —— 零行上的 `FOR UPDATE` 一把锁都不加(与 `friend_player_guards` / `mission_player_guards` 同一个坑)。**但这里不必新建守卫表**:`players` 行由 `EnsureProfile` 保证必然存在,且本事务**本来就要 UPDATE 它的战绩计数**,锁它是自然且免费的。代价是同一玩家跨池结算串行化(一个玩家同时结算多局本就极罕见),换来"两局并发都判自己是首战、各自 INSERT 互相覆盖"的彻底消除。加锁顺序固定 `players → player_mmr → mmr_history`,全仓无反向路径。
  - **幂等键刻意不含 rating_pool**:一场对局只属于一个池,把池并入 `(player_id, idempotency_key)` 会让同一 match 换个池名就能**重复入账**(§16.2)。
  - **存量表填值**:PVP 图按**玩法档位**分池而非当前测试人数 —— id 4/5/6 是 5v5 玩法的图(本地端到端把 team_size 临时填 1)→ `5v5_ranked`;id 9(team_size=3)→ `3v3_ranked`;PVE 四行留空。这样将来把 team_size 改回 5,池归属不用动。
  - **数据清空的直接后果**:迁移**不搬运存量段位分**(脚本注释已写明),所有人在各池从基线分重新起算。若将来要保留存量,必须先拍"单值怎么分裂成多份"(复制到每池 / 只给某一池 / 全部重置)——那是产品决策,不能由迁移脚本替它选。
  - **顺手发现的既存缺口(与本次正交,未修)**:**撮合的 MMR 窗口目前是空转的** —— `TeamMemberStorageRecord.Mmr` 全仓**没有任何写入点**(`team.go` 建队/入队都只填 `PlayerId`),单人入口更是显式 `return m, 0, nil`,所以所有票据 `avg_mmr` 恒为 0,`mmr_base_window` / `mmr_widen_per_sec` / `mmr_max_window` 三个配置形同虚设。本次没顺手接(撮合不读 player MMR,与段位分区正交,§15.3),但它意味着**「按段位撮合」这个功能其实从未生效**,需要单独立项。
  - **验证**:`buf lint` + go pb 重生绿;导表绿(11 行,dist 里 3v3/5v5 两池 + PVE 留空已逐行核对);`pkg/rating` / `pkg/configtable` / `player` / `battle_result` / `ds_allocator` / `matchmaker` / `tools/migrate` / `configtable-gen` 全模块 build+vet+test 绿。新增用例:`settlementRatingPool` 五分支、**`TestSettlementReadsAndReportsOwnRatingPool`(分区回归判据:读 MMR 逐人必须用本局池 + 出箱事件必须带同一池;分区上线前必红)**、`TestValidateRatingPool`(两向校验 + 3v3/5v5 双行共存 + 超长拒)、matchmaker 定格与三种回落、player fake 改为**池感知**(不感知池的 fake 会让分区 bug 全部漏过)。
  - **未验证 / 交接**:①真实 MySQL/TiDB 上跑 000007 迁移与并发首战入账(用例已备,需 `PANDORA_TEST_TIDB_DSN`);②`go test -race`(需 CGO Linux);③**UE 侧编译 + `CfgLevel.uasset` 重导**(§11.6 由用户执行);④xlsx 第 17 列需 **SVN 提交**,dist 这批产物需**整批一起提交**;⑤leaderboard 竞技分榜的数据源仍是旧口径,分池榜未接(`board_type` 维度本就存在,但取数要改成按 `rating_pool`),需单独立项。
- 2026-08-12(**已发布迁移做成 contract 导致滚动升级期新旧副本无法共存 —— expand 回补 + 登记面收口**;事故档案 [INC-20260812-001](docs/incidents/2026-08-12-p0-published-migration-rolling-upgrade-break.md))。上一条(08-11 段位分区)与 08-10 的编号改名各自留了一颗雷,本条把两颗一起拆掉。
  - **根因是同一个,不是两件事**:`pandora_account/000006` 用 `RENAME COLUMN`/`RENAME INDEX`/`RENAME TABLE`+`DROP` 换掉角色编号三件套,`pandora_player/000007` 直接 `DROP players.mmr` —— 这是 **contract**,不是 expand。迁移一执行,尚未排空的旧 Go 副本读写的对象**当场消失**,违反 §9.16/§9.21「已有 RPC/字段/语义不得原地禁用或硬切;删除能力必须走 expand → migrate → contract」。**§3.6.3 那句「生产零注册路径、无存量数据,现在是改名成本的最低点」是本次判断失误的源头**:它只论证了**数据**没风险,推不出**二进制共存**没风险,而 §9.21 约束的恰恰是后者。
  - **放大因素让它从"功能中断"升级成"卡死全部发布"**:`players.mmr` 上挂着 `idx_mmr`(`000001_baseline:43` 起一直有),`DROP COLUMN mmr, ALGORITHM=INSTANT` 在 MySQL 8.4 报 **1845**,golang-migrate 留下 `schema_migrations` v7 dirty;而 `deploy/k8s/migrate/job.yaml` 是 `backoffLimit:0` 硬门禁 + `rejectDirtyOrNewer` fail-closed → **一次 dirty 把此后每一次发布都挡住**,不只挡本次。反过来这也是**线上零影响**的原因:v7 版 player 镜像根本没滚上去。
  - **落地(两个纯加法迁移 + 双写,不碰任何已发布迁移)**:①`account/000007_player_no_expand_compat` 把 `register_no`/`uk_register_no`/`register_no_counter` **重新建回**并与新名并存;②login 侧 `SweepPlayerNo` 在**同一事务**固定先锁 `player_no_counter` 再锁 `register_no_counter`、取 `MAX(两张 next_no)` 起算、一次 UPDATE 双列同写、收尾把两张表推到同一水位——旧 Stable 只锁 register 一张(单锁),**无反向持锁路径故不会死锁**,MAX 水位保证两代都不会重发号;③`player/000008_rating_pool_expand_compat` 把 `players.mmr`+`idx_mmr` 加回来当 default 池兼容投影,并从 `player_mmr` 回填;④player 服务 default 池改以 `players.mmr` 为兼容权威并双写,显式池(3v3/5v5)只写 `player_mmr` 绝不污染旧列;⑤`tools/migrate` 加一次性**精确** quarantine 收敛 v7 dirty(校验已发布 000007 正文 SHA-256 + 中间 schema 形态 + 目标库名 + 镜像至少含 000008,任一不符 fail-closed;**不是通用 force**)。
  - **两个 down.sql 都有意 no-op**:回滚服务版本**不等于**旧副本已排空,回滚时删兼容面会立刻打死仍在跑的旧副本。contract 只能由未来独立的更高版本迁移在确认旧副本归零后执行。
  - **登记面收口(本次唯一的真红)**:`register_no_counter` 建了表却没登记 → `TestFreshInitTablesAreRegistered` / `TestMigrationTablesAreRegistered` **双红**(先红后绿实测)。已同步补 `CLAUDE.md` §9.24 豁免清单 + `tools/migrate/cmd/dbcheck` registry(恒 1 行,发号权威闸,不清理)。
  - **两条初审发现被真库实测推翻,记下来别再重报**:①「新代码把 000007 版 Stable 刚落的 default 分覆盖回退」——**「000007 版 Stable」物理上不存在**,那份代码的 `EnsureProfile` 仍 `INSERT INTO players(...,mmr,...)` 而同 commit 建表脚本已无该列,上线即 1054,且它自带的真库 CI 门禁会先判它红;真正共存的是 pre-000007 版,读写的正是 `players.mmr`,与新口径**双向兼容**。②「000007 双计数器合并 UPDATE 会被 login 补号事务锁死打成 dirty」——`SweepPlayerNo` 每批必提交(实测 500 行≈277ms),真库压测 59 次探针 0 次 1205,等待不随副本数无界增长(6→16 副本,最大等待 5.2s→6.5s)。
  - **验证**:`go build`/`go vet`/`go test`(login+player+migrate 全模块)绿;dbcheck 登记契约先红后绿。**未验证**:①真库迁移 7 场景矩阵(用例已备,需 `PANDORA_TEST_MYSQL_DSN`/`PANDORA_TEST_TIDB_DSN`);②`go test -race`;③滚动窗口内新旧副本共存的玩家 E2E。
  - **剩余风险(全部未开始,见事故档案 §10)**:A-1 未来 contract **不得原样重放** `DROP players.mmr, ALGORITHM=INSTANT`(否则复现同一个 1845);A-2 **已落码**(`tools/migrate/expand_only_contract_test.go`:遍历全部 up.sql,命中 DROP/RENAME 即红,除非文件头写 `-- CONTRACT:` 且写明「旧副本排空判据」,或登记在只减不增的 grandfathered 清单;配套反向断言防清单腐化;已收录 5 条历史违规;摘掉任一条即转红的变异验证已做);A-3 **contract 时必须反向回填 `player_mmr ← players.mmr`**——旧副本结算只写旧列不写 `player_mmr`,删列瞬间玩家 default 段位会回退到最后一次新副本写入的值,000008 注释只写了「以后删」漏了这一步;A-4 `000008` 回填是不分批多表 UPDATE(真库实测 15 万行≈18s/150001 把记录锁,期间这些玩家结算等锁超时报 1205),受支持路径上恒 0 行故降 P2,但加 `WHERE p.mmr <> pm.mmr` **无效**(RR 下锁在判谓词之前就加),只能主键游标分批;A-5 **本轮审查未跑完**(5 维度中 3 个 agent 连接中断,19 条发现仅 3 条完成裁决,16 条既未确认也未证伪);A-6 `0fdb15f1` 把 4 份 Agones Fleet 版本 yaml(r1971→r1977)与本次修复混在同一提交推上 origin/main 且 commit message 不合 §4 格式;A-7 其余 migration set 未做同型 contract 扫描。
- **同日九修:线上实测发现「整队一起掉线 → 永久不退队」的真缺口(用户报「离线很久没退队」)**。
  排障从头到尾查错过两次地方(先查了自己起的 docker-compose Redis、又把 kubectl port-forward
  当成服务进程),真环境是 k8s(pandora-agones)。**现场证据**:两名玩家仍挂在队伍
  23028424935505920 里,位置 key 已过期;hubmeta 只有 `last_alive_ms`、**没有 `left_at_ms`**
  (印证 Hub 侧没走 Logout);而 `pandora:offlinewatch:{team}:due` **完全为空**。
  **根因**:排期只有两条触发链,它们在同一种情况下同时失效 —— ①kafka 离场事件依赖
  Hub DS 走完 Logout 调 ReportDisconnect,整机崩溃时不会调;②GetMyTeam 读路径兜底依赖
  有活人打开组队面板,整队都掉线时没人打开。此前记的「H2 已由 last_alive_ms 兜底」**不完整**:
  那解决的是「**能不能判**」,没解决「**谁来触发判**」。
  **修法**:pkg/offlinewatch 加可选 `RosterSource`(业务提名候选),由 team 实现
  `teamRosterSource` 增量 SCAN 既有的 `pandora:team:player:*` 归属索引 —— 那份索引本来就是
  「有队伍的玩家」全集,再建一个平行集合就是 §9.22 重复影子状态(还要在建队/离队/解散/TTL
  四处保持一致)。提名只负责"报名单",判定与排期仍全部由 Observe 走 locator 权威完成,
  因此不存在"用本地时钟猜离线"。复用 Sweep 已有 ticker(§16.10 不新建第二套 timer),
  **关键是队列空的分支也必须跑兜底** —— 那恰恰是缺口场景。游标只存进程内(纯调度提示,
  重启从头扫)。新增 2 条回归(队列空时仍跑兜底提名 / 未注入时行为不变),前者在修复前必然失败。
- **同日九修·更正根因(原判断被证据推翻)**:上条写「hubmeta 只有 last_alive_ms 没有 left_at_ms,
  印证 Hub 侧没走 Logout」——**跳步了**。查 locator 全量日志后,那两名玩家的真实轨迹是
  `state=3(HUB)×1 → state=4(MATCHING)×1 → state=5(BATTLE)×79`,**最后停在 BATTLE**,
  `disconnect_reported` 零条。
  真因不是 Hub DS 崩溃,而是:`ReportDisconnect` 是 **Hub DS 专用**接口(守卫要求 state==HUB),
  玩家**从战斗里离开**时根本不存在 Hub Logout —— 这是设计如此不是故障。于是
  「组队 → 匹配 → 进战斗 → 战斗中/战斗后掉线」这条**主流路径**天然没有任何离场事件,
  队伍又不会因进战斗而解散,残留必然发生。
  **这把 periodic resync 的定位从「边缘兜底」抬成「主路径必需」**:Hub 崩溃罕见,
  「打完一局直接退游戏」天天发生;没有 resync,本功能对最常见的退出方式根本不生效。
  附带精度问题(已评估,非正确性问题):last_alive_ms 由 Hub 心跳推,玩家进战斗后 meta 不再更新,
  故判定用的时刻偏早。方向安全 —— classify 永远先判 online,BATTLE 期间 online=true 不会被碰;
  只有位置 key 真过期后才用到该时刻,那时确已离线,后果是「早一点被摘掉」而非「误摘在线玩家」。
  **线上验证**:重新构建 + load + rollout 后,两名残留玩家的 `pandora:team:player:*` 归属在
  一到两轮 sweep 内清空,队伍 23028424935505920 已不存在。
- **同日十修:按「最标准」补齐两处(用户拍板)**。
  ①**Battle/Matching 心跳补 last_alive_ms**:此前只挂在 Hub 心跳(RefreshHubLocations)上,
  玩家一进战斗 meta 就不再更新,于是「打完一局直接退游戏」这条最常见路径的离线时刻会停在
  很早的 Hub 阶段(判定偏早)。新增 `LocationRepo.TouchAlive`,在 SetLocation 的
  MATCHING/BATTLE 分支调用。**带 30s 节流**(`data.AliveTouchThrottle`):BATTLE 心跳是
  每 5s 每人一次的热路径,不节流 Redis 写量按 6 倍放大而毫无收益;节流掉的只是「写时间戳」,
  **TTL 每次都必须续** —— 漏续会让 meta 先于在线状态过期,反而丢掉判定依据(已按此写 Lua 并实测)。
  ②**RosterScanBatch 从拍脑袋的 200 改为按规模推导**:默认提到 2000,注释写死公式
  `RosterScanBatch >= 候选总量 / (Threshold / Interval)` —— 原来的 200/15s 在 10 万候选下
  要 125 分钟才扫完一轮,而阈值只有 180s,等于没生效。同时给 teamRosterSource 加
  「完整遍历一轮」的耗时日志(`team_roster_full_scan_completed`),让「扫描周期 <= 阈值」
  这条约束在现场可核对,而不是只写在注释里。
  验证:真 Redis 10/10 全过(含新增的节流分支两条:节流窗口内不推进时间戳但必须续 TTL、
  不给无 meta 玩家凭空建毒 key);全量单测绿;team + player-locator 已重新构建、load、rollout。
  过程记录:`go mod download` 又因网络抖动失败一次,重试即过(非代码问题);
  本地 Redis 容器在诊断期间被清掉两次,验证改用一次性临时实例(跑完即删,不碰测试环境)。
- 2026-08-12(续:审查补跑 + quarantine 静默覆写玩家段位的 P1 修复)。上一条的 A-2 门禁落地后重跑审查,补回 `migrate-quarantine` 维度,抓到一条**真库可复现**的 P1。
  - **P1:quarantine 的"精确 1845 形态"根本区分不了两种库**。`validatePandoraPlayerV7DirtyShape` 只比对列与索引 —— 而 000008 恰好把 000007 想删的 `players.mmr` 和 `idx_mmr` **原样加了回来**,于是「000007 半途 1845 的中间态」与「已经跑到 v8 的终态」**逐列逐索引完全相同**。任何让 `schema_migrations` 退回 `(7,dirty)` 的操作(备份恢复、跨环境拷库连 schema_migrations 一起拷、人工 `migrate force 7`)都会被判成中间态标 clean,随后 000008 的兼容回填 `UPDATE players p JOIN player_mmr pm SET p.mmr=pm.mmr` 不再是 0 行,而是把**滞后的影子值**盖回 `players.mmr` —— 而它正是 expand 期指定给旧 Stable 副本读写的兼容权威,等于**静默把玩家段位回退**,提前触发 A-3。真 MySQL 8.4 实测:players.mmr 1620/1480 被无声改成 900/800,日志只打「验收通过 version=8」。
  - **修法(唯一能把两者分开的是数据,不是 schema)**:000007 只 `CREATE player_mmr` 而**不回填**(存量段位重置口径),所以真中间态下 `player_mmr` **恒 0 行**。已在形态校验末尾加行数守卫,非空一律 fail-closed 交人处置——那正是「无法确定该库处于哪个状态」的情形,交人远好过自动改玩家段位。新增 `v7_dirty_but_pool_data_exists` 场景,**真 MySQL 8.4 先红后绿**:摘掉守卫时 `migrateTarget` 返回 `err=<nil>`(静默洗白),加回后拒绝。
  - **另两条成立(已记事故档 A-8/A-9,未修)**:①expand 窗口内 pre-000007 老副本会把**显式池**结算吞进 `players.mmr`(它不认识 rating_pool,mmr_history 的池由列 DEFAULT 补成 'default')——现网 4 个 ELO 关卡**全部**配非 default 池,所以 player 单独回滚到 Stable 期间是 **100% 排位局**记错池;幂等键不含池故重投补不回来,但**可恢复**(`mmr_history JOIN battles → map_id → 关卡表段位池`)。同时暴露 `mmr_repo_mysql_test.go` 自称钉住「显式池不串分」其实只测了旧→新的 default 一场,不满足 §9.21「验证 Stable↔Canary 组合」。②quarantine 的目标库白名单只认精确 `pandora_player`,与 `validMigrationDatabaseMapping`/README 明面允许的 `<set>_<后缀>` 分片库名冲突;当前不可触发(player 无分片),将来引入分片会卡死自动化发布链路。
  - **审查两轮都没跑完(A-5 升级)**:`ci-and-proto` 与 `cross-cutting` 两轮**均**因连接中断未返回,复核 agent 两轮各挂 4 条。累计 27 条发现只有 9 条进复核、3 条成立、2 条推翻,**18 条既未确认也未证伪**(清单已逐条落进事故档 §10 附注)。**跨切面扫描至今零覆盖**:其余 migration set 的同型 contract、proto 新字段有没有真接线与 fail-closed 分支、cpp pb 与 UE 仓库的同步断裂、CI 容器版本与 skip 白名单,全都没查过。另有一条(000008 不分批回填)两轮复核**结论相反**,按第一轮的真库实测口径记为 A-4 待复跑。
- 2026-08-12(需求登记,零代码):**对局三段式生命周期(准备 / 战斗 / 结算)**。用户提出「每个 PvP / PvE 都应该有三段:准备阶段、战斗阶段、结算阶段(阶段完成退出场景)」。已建 `docs/design/match-phase-lifecycle.md` 登记需求 + 现状核对 + 待拍板清单,`docs/design/ds-arch.md` §6.2 加了指回本文件的待做注记。**本次只写文档,未改任何代码 / 配表 / proto。**
  - **现状核对(实查,非推测)**:三段里只有「战斗阶段」存在,且只以「时限」一个维度存在。①**准备阶段不存在**——玩家被 Admission 接纳后即可行动,且对局时限的武装点是**首个玩家进入**(`TryArmBattleTimeLimit`),先到的人已经在打、后到的人还在跑图;②**战斗阶段部分存在**——时长读关卡表 `battle_duration_seconds`(1200 / 1800,`0`=不限时),剩余秒数按整秒经 `AMyGameState::BattleRemainingSeconds` 复制,但对局没有「阶段」这个状态,只有一个倒计时数字;③**结算阶段(玩家视角)不存在**——只有服务端终局投递状态机(快照冻结→`ReportResult`→receipt→terminal),那是可靠投递机制不是时间窗,没有战报 / 掉落 / MVP 展示;④**退出场景不是「阶段结束」驱动的**——走 `BattleEnded` + 客户端自行回 Hub,实测耗时大头是后端 `ADMIT_BARRIER` 等待。
  - **拍板前不要动的东西**:`battle_duration_seconds` 现有行为是当前唯一在跑的时间约束,拍板前不改。
  - **最大正确性风险(设计时必须显式选择,已写进文档 §3.3)**:玩家可见的「结算中」窗口与服务端 `ReportResult` 可靠投递**是否同一段时间**——绑定则投递重试会把玩家卡在结算界面,不绑定则玩家可能在数据落库前就退出。
  - **其余待拍板项**(阶段权威在 DS 还是后端、是否下发客户端(走复制属性即须 bump 协议版本)、时长配关卡表还是全局、`0` 是不限时还是跳过、准备阶段做什么、与撮合 confirm 的边界、单人 walk-in PvE 是否跳过、时限武装点改为准备阶段结束、结算能否跳过、退出由谁发起、重连落点、PvP/PvE 是否同一状态机)见文档 §3,共 16 条。
  - **同日补充(用户澄清准备阶段的目的)**:「准备阶段就是为了给卡顿的玩家切到另外一个场景准备充足的时间,一般是配置吧,每个场景不一样的准备时间」。据此**定死三条**(已从待拍清单移出):①准备阶段**只做进场对齐,不承载玩法**(ban-pick / 选人 / 买装是另一个需求,不得夹带);②**时长配关卡表,每图一列**,与 `battle_duration_seconds` 并列;③**`0` = 未配置 = 无准备阶段立即开打**(沿用关卡表新列惯例"0 与本列上线前逐字节等价",滚动混版安全)——**不得**解释成"无限等待",那是没有出口的等待态,直接违反 §9.19/§9.20。
  - **提了一条设计问题(未拍板,文档 §1.2)**:纯固定时长**不保证**"人都好了",只是把「先到的人已经在打」换成「大概率够用」——配短了目的没达成,配长了每局都被最慢那台机器的最坏值绑架(全员 3 秒就绪也得等满)。**建议结束条件取「全员就绪 or 到配置上限」的先到者**,表里那个数从"固定等待"变成"**最长等待**"(更好推导,按本图最坏加载耗时配);快时不浪费、慢时有界,且到期动作是"强开"而非"假设都好了"(§16.10 兜底 vs 掩盖)。选它还顺带消掉「单人 walk-in PvE 要不要跳过准备」这个问题(1 人就绪即开)。若仍拍纯固定时长,须在文档写明"已知快的人会干等满"。
  - **落地路径已查清并写进文档 §4(实现时照抄,别重查)**:①关卡表加列 = `level.proto` `LevelRow` **下一个可用编号 18**(17 是 `rating_pool`)+ **xlsx 表头同步加列**(源表在客户端仓 `Table/关卡/g_关卡.xlsx`)——**登记 proto 却没加表头 = 生成器整表拒绝**;②校验落 `pkg/configtable/level.go:validateLevelRow`;③阶段若走复制属性下发,**`PandoraNetProtocolVersion` 当前是 9(`Pandora.cpp:77`)→ 须 +1 到 10**,双端同步发布(注:此前记的"下次是 9"已过期);④配值可参照的既有预算:`TravelAdmissionWatchdogSeconds`=45s(客户端 Travel→Admission 上界,准备时长配得比它大没意义)、`HubAdmissionTotalBudgetSeconds`=25s、"DS 进程起→玩家 travel 完成数十秒(大图更久)";⑤**`min_team_size` 只在进场那一刻判定**,准备阶段结束时人数不足**不得**踢人或强制收局。
  - **同日再补(用户问「PVE 现在是所有怪物死亡结算,可能提前结算」)——该前提与代码不符,已按实查记进文档 §2.1**。终局触发只有三个枚举值 `PlayerDied` / `PlayerExtracted` / `TimeLimitReached`(`PandoraBattleSettlementRule.h`,枚举注释明令「只有 `EvaluateTerminal` 一个权威入口,新增原因加枚举值,不得另开并列钩子」),**没有怪物相关触发**;PVE 规则是**撤离制**——整队全部进入终态(死亡或撤离)才结算,有人撤离成功=通关(0)、全灭=未通关(1);怪物死亡回调 `HandleMonsterEntityDied` 只产掉落 + 上报击杀经验,**从不调 `RequestFinishBattle`**(全文件调用点只有死亡/撤离/超时/失败重投四处);`Pandora/Content` 未扫到任何引用结算规则类的资产,即无蓝图子类改写判据。**但「可能提前结算」的担心成立,根因是撤离制判据本身**:单人 PVE 下 roster=1,一个人死就是"整队全终态"→ 立刻失败结算;多人下最后一个活人一撤离就立刻收局。真正的缺口是**「目标达成」与「对局结束」目前是同一件事**,没有任何窗口给玩家捡东西/看战报——这正是结算阶段要解决的。若线上确实见到"怪一清空就结算",需给出 map_id + 日志再查,按当前 C++ 事实不成立。
  - **用户「不想用状态」→ 已写建议方案(文档 §7,未拍板)**:三段式**不需要 phase 枚举状态机**。①**存时刻不存阶段**——权威 DS 只持两个单调钟绝对时刻(`PrepareDeadline` / `BattleDeadline`),阶段是 `Now` 落在哪个区间的**纯函数**,没有 `SetPhase()` 就没有"忘了切状态"的 bug,也不会出现阶段与倒计时两份事实漂移(§9.22 派生值按需计算);②**下发也不发状态,发剩余秒数**(照抄 `BattleRemainingSeconds` 先例,客户端 UI 从秒数派生),代价仍是协议版本 9→10——文档明确**不推荐**用"负值=准备阶段"这类约定编码去省这次 bump(省一次版本换长期歧义);③**结束条件每 tick 求值**(`全员就绪 or 到期`),与现有 `TickBattleTimeLimit` 同款,不要转移表;④真正要存的只有**不可重建的事实**(谁死了/谁撤离了/终局快照冻结没有),那些已经存着了,不属于"阶段状态"。⑤PVE 侧同理:**别给"怪物清空"新加终局触发枚举**(会在唯一权威入口旁开第二个裁决源),胜负判据留在 `UPandoraBattleSettlementRule` 子类里(类头注释已预留"击杀 Boss/存活/护送"是不同子类),并把**「目标达成」与「对局结束」拆开**——目标达成只结束战斗阶段、进结算阶段,不直接收局。
  - **发布线复查更正**:此前记的「placement-preflight 死契约硬阻断每次 online 发布 + 会部署出必 CrashLoop 的 initContainer」**已修**——`online_manifest_contract.ps1` / `start.ps1` 对 `placement-preflight` 的引用 grep 零命中,`ds_auth_activation_contract.ps1:441` 只剩说明注释。另两条仍红(`matchmaker/main.go` 的 `legacySigner` 3 处、`gen_cluster_config.ps1:591` 空 `$PlacementSecretBindings`)。
- 2026-08-13(策划一键启动两条护栏:dev.env 自举 + 退出码透传)。现场:另一台机器刚更新完双击
  `策划一键启动-改资源即时生效.cmd`,断在 `[1/4] 基础设施`(`[ERR] env file not found: .../deploy/env/dev.env`),
  而窗口最后一行仍报「完成(退出码 0)」。两条都是结构性的,不是那台机器的偶发。
  - **根因① dev.env 永远不会随更新发过去**:`deploy/env/dev.env` 命中 `.gitignore:45` 的 `*.env`,
    `git ls-files deploy/env/` 只有 `dev.env.example`。也就是说**任何**新机器 / 新克隆第一次跑都必然缺它,
    而 `docker compose --env-file` 指到不存在的路径直接失败。原实现只打一行「请先执行 Copy-Item ...」
    就 `exit 1` —— 一键启动实际是两步,且策划多半只看到最后那行「基础设施启动失败」。
    **修**:新增共享库 `tools/scripts/dev_env_file.ps1`(`Confirm-DevEnvFile`),缺失时按 example 初始化
    (example 里全是 dev 级默认值:弱口令 + 空 webhook,群告警 URL 留空时 Grafana 不生成对应 receiver,
    没有一个真 secret);**已存在则绝不覆盖**(本机填的真实 webhook/token 不能被冲掉);连 example 都缺
    才硬失败(那才是工作区不完整)。先写临时文件再原子改名,双击两次也不会读到写了一半的 dev.env。
    接线:`start.ps1`(`Resolve-Prerequisites`,online 除外)、`dev_up.ps1`、`dev_down.ps1`、`dev_status.ps1`、
    `k8s_envoy_bridge.ps1`。**注意 bridge 早就有一份内联自举**(PROGRESS 续「bridge dev.env 自举」),
    但只修了 k8s 那条路,local / docker 走的 `dev_up.ps1` 一直是硬失败 —— 本次把两份并成一份。
  - **根因② `&` 调子脚本不会让父脚本失败**。`dev_all.ps1` 每步失败都 `exit 1`,但 `start.ps1` 的
    `Invoke-Local` 裸调不透传,`switch` 走完就正常结束 → 双击窗口 / Web 管理台拿到 0。
    同层的 `Invoke-Docker`(`dev_up` 后 `throw`)与 `-Resume` 分支一直是有检查的,**只有 local 这条漏了**。
    实测复现:裸调版 `exit=0`,加判定版 `exit=1`。**修**:`Invoke-Local` 的两处调用(`-Down` 与正常启动)
    都判 `$LASTEXITCODE` 并原码带出,正常启动失败时补一行「后端没起来,别去查客户端」的归因。
  - **护栏**:新增 `tools/scripts/tests/oneclick_devenv_exitcode_contract_test.ps1` 并登记进 `ci_backend.ps1`
    的 `$contractTests`(全绿才登记)。断言四组:①自举三态真实执行(建出 / 不覆盖 / example 也缺时抛错,
    全程在临时目录,不碰仓库里的 dev.env);②五个入口脚本都接了自举;③除共享库外不许再有内联的
    `Copy-Item ... dev.env.example`(防再分叉,k8s 那份就是这么长出来的);④AST 取 `Invoke-Local` 函数体,
    每处 `dev_all.ps1` 调用后必须有 `$LASTEXITCODE` 判定并 `exit`。**已做变异验证**:把退出码判定摘掉后
    测试转红(`exit=1`),装回即绿 —— 不是空跑的测试。退出码这条尤其需要门禁,它的回归表现是
    「窗口报绿、后端其实没起来」,人眼 review 最容易放过。
  - **未做**:`reset_data_service_schema.ps1` 也吃 `--env-file`,但它只用于栈已经跑起来后的破坏性重置,
    彼时 dev.env 必然存在,故不接自举、也不进契约测试的入口清单。
- 2026-08-13(表头漂移:真实「新增列」首次发生 + 补双击入口)。现场:策划一键启动 `-GenTables` 整批不产出,
  报 `技能/j_技能.xlsx 表头出现未登记的第 Y 列 "伤害显示"`(实际是 Y/Z 两列:伤害显示、治疗显示)。
  - **处置**:`configtable_sync.ps1 -Write -SyncCol 'skill.伤害显示=damage_display:uint32','skill.治疗显示=heal_display:uint32'`
    → `skill.proto` 追加 `damage_display=25` / `heal_display=26`(编号顺延不回填)→ 自动重生 pb → 重建 exe →
    重跑导表全过。客户端列登记里这两列都没有,不指定 `-SyncCol` 只会得到占位名 `col_y` / `col_z`。
    **补齐了上一条「追加字段路径只有单测覆盖」的剩余风险:真实新增列端到端跑通。**
  - **坑**:`-SyncCol` 传多个值时,`pwsh script.ps1 -SyncCol a,b` 会被原生调用当成**一个字符串**传进去
    (报「类型 xxx 不受支持」)。要用 `pwsh -Command "& script.ps1 -SyncCol a,b"`,或重复写在同一数组里。
  - **新增双击入口 `程序一键同步表头.cmd`**(根目录,纯 ASCII 内容 + CRLF + 不写 chcp,遵 cmd-batch-encoding 铁律)。
    两趟:[1/2] 只报差异不写文件 → 按 Y 确认 → [2/2] `-Write` 改 proto + 重生 pb + 重建 exe + 重跑导表。
    带参数时直通 `configtable_sync.ps1` 并跳过问答(给 `-SyncCol` 这类高级用法)。`PANDORA_NONINTERACTIVE=1`
    时停在报告、不 pause(Web 管理台 / CI)。`%ERRORLEVEL%` 一律在 label 分支里取,不放进 `if (...)` 块
    (块内会在解析期就展开成旧值)。实测:CP936 下双击路径与参数直通路径均 `exit=0`、无解析漂移。
  - `configtable_gen.ps1` 的失败提示改为首推双击该 .cmd(命令行写法保留作等价说明);策划机缺 go/buf 时
    提示语也从「他跑一条命令」改成「他双击那个 cmd」。

## 2026-08-17 队伍 ready 生命周期方案 A 落码 + 批次审计修复(Claude)

- **方案 A 拍板落码,发布阻断解除**:`BeginTeamMatch` 一次性消费 ready(三路径:收据重入
  / legacy 作废 / 正常消费),收据绑定消费后代际 → 不需要原文档 §6 的三阶段发布;
  `EndTeamMatch` 降级共存兼容路径。旧契约测试改写 + 7 个新契约测试。
  详见 decision-revisit-team-match-lifecycle-and-roster-rollout.md §9。
- **allocator 到齐期限 observe→enforce 分代激活**:`roster_join_deadline_mode`(默认
  observe 只采证)+ `roster_policy_generation`(Allocate 冻结,proto 26);
  biz/data 双路径共用判定谓词;enforce+0 启动拒;6 新测试。
- **审计确认 2 条并修复**(13-agent workflow,8 发现 6 证伪):①formSoloMatch/formMatch
  丢 team_ready_generation(主路径 End CAS 恒退化;补传递+穿真实装配的回归测试);
  ②team_call_auth_secret 无同钥拒绝(纳入 Validate,同款测试)。
- **4011 结构化缺席成员**:StartMatchResponse.absent_player_ids + MemberOfflineError。
- **team/guild 入列 DS 回调密钥域**(verify-only,player 先例):三处清单+三份模板+
  双向契约测试反转。⚠ 首次 online 发布会撞 HMAC 连续性门,须先走换钥流程。
- 测试:team/matchmaker/ds_allocator/guild 全绿。未做:Linux -race、真集群双版本矩阵、
  玩家 E2E(发布前置项,见 decision-revisit §9.3);UE 侧 absent_player_ids 接收待客户端。

## 2026-08-17 玩家全链路日志可查性审计 + 修复(登录/匹配/进战/断线/结算退出/重连)

- 审计:6 条后端链 + 3 条 DS/客户端链,对抗复核定谳(翻案 5 条);验收口径 = infra.md §11.3。
- 后端落码 17 项(login 3×P0、reconnect 2×P0、段位链 P1 端到端、presence 蒸发 P1 等),全部 build+test 绿,未提交(工作区有并发在途改动,逐文件确认归属后再收)。
- DS/客户端落码 9 项(离场证明重试终局、驱逐单收到/丢弃/ack、AwaitingTicket 零日志、ACK 失配 reason 等),待用户 UE 编译验证。
- 新文档:docs/ops/player-journey-log-map.md(值班速查/六段链路判据/修复清单/待办)。
- 避让未修:matchmaker 3×P1(saga 补偿/确认超时点名/取消匹配 DEBUG)、ds_allocator 判弃 player_ids——文件被并发会话占用,见文档 §4。

## 2026-08-17 取消准备门槛:LoL 式开局流程(拍板 + 双仓落码)

- 拍板:队员不再点准备,队长直接开始匹配;「带缺席者开局」防线移交 StartMatch 在线闸(4011)
  + 撮合确认期(CONFIRM 15s,全员接受才拉 DS;失败时 DS 未分配,无人被拉进对局)。
  决策记录:decision-revisit-team-match-lifecycle-and-roster-rollout.md §8。
- team:BeginTeamMatch 删 READY 闸与 legacy 作废分支(FORMING/READY 都放行),冻结/租约/收据
  /清残留 ready 原样保留;SetReady/EndTeamMatch/掉线软档保留为存量兼容(expand 期,零 proto 变更)。
  测试按新语义反转(冻结后可直接开第二局 / FORMING 直接开局 / legacy 放行),team 全绿。
- matchmaker:代码零变更;dev 档 auto_confirm_match 翻 false(确认期成为主防线)。
  ⚠ 该配置生效须与客户端确认弹窗版本一起上,否则旧客户端撮合局全部 15s 超时失败。
- robot:默认档 AutoConfirmMatch 翻 false(方向安全:手动档打 auto 后端只是可容忍竞态;
  反向会让漏斗全断),gatecheck 在 FOUND/CONFIRM 主动接受;robot/stress 全绿。
- UE 客户端(待用户编译 + WBP 清理见 F:\work\HANDOFF-取消准备-LoL式流程-20260817.md):
  删准备按钮/成员准备栏/SetReady 模型链(两套 UI 面都删);新增撮合确认弹窗
  (MyTeamModel 运行时子树,权威进度确定性投影,CONFIRM 自动开面板,接既有 ConfirmMatch)。
- 未做(另行拍板):过错方票据挂起 + 缺席者一键续排(用户已提需求,语义待确认);
  contract 期删 SetReady/ready 字段;MatchProgress 增 confirm_deadline_ms(精确倒计时)。

## 2026-08-18 INC-20260818-001:大厅票据 / owner assignment 分叉导致恢复死循环(P0,本机已闭环)

- 现象:PIE 登录进大厅后客户端每 ~1.3s 打一次 GetResumeContext、每 45s 重签票重连 Hub DS,
  exact ACK 因 owner_target_mismatch 被丢弃,约 146s 后弹恢复面板;进入 BATTLE 后同一恢复机制
  7s 内确认并停止轮询,排除客户端轮询状态机本身。事故档案:
  [docs/incidents/2026-08-18-p0-owner-hub-assignment-divergence.md](docs/incidents/2026-08-18-p0-owner-hub-assignment-divergence.md)。
- 根因不是 owner 单点漏写一列,而是跨服务定序与交付不变量同时破口:①owner 旧实现把同物理
  实例当 no-op,不接纳新 assignment;②Hub replacement/transfer/drain 在 Redis assignment CAS
  定案前调用含 owner Begin 副作用的签票 helper,CAS loser 可把 owner 指到未发布 target;
  ③epoch conflict 后原 target 无当前意图证明的盲重试可覆盖后继 winner;④ACK 的 ADMITTED 快路只比物理实例;
  ⑤Login 可在同一响应交付 ticket=B 与 owner Resume=A。
- 对抗复核推翻第一版“同 epoch 原地刷新 assignment/track”方案:真 MySQL 交错证明迟到
  expect_epoch=0 的旧请求能把 v2 回滚成 v1；同法还会让 BATTLE allocation/track 继承旧
  epoch/ADMITTED 权限。最终 owner 语义为**完整 target 全等才 no-op**；assignment/allocation/
  track 变化统一 `epoch+1/PENDING/new operation`,旧 epoch 立即失效。
- Hub 修复:把签票拆为 CAS 前纯签名与 CAS winner 后 owner bind；Begin 前后重读 Redis 并比
  完整 ticket binding + writer lease；冲突只允许完整重跑一次 Query→guard→Begin，禁止只换
  epoch 盲写旧 target。ACK 先比 owner type + target 五字段再允许 ADMITTED
  幂等快路。Login 交付前再次验签并把票据五字段与 owner Resume 全等比较,不一致扣票返回 WAIT。
- DS 同型收口:取消 conflict 盲重试，用显式 operation + read-back 判定 commit-then-
  response-loss；仍 unknown 时保留 allocation/Pod 且扣 READY。claim loser 只读 exact 复核，
  不成为第二 binder。持久 owner-binding plan/reconciler 增强单列 INC-20260818-002。
- 当前验证:Owner 真 MySQL 的换发收敛/迟到旧 epoch/Battle allocation+track、Hub CAS loser/
  真 epoch barrier 交错/ACK exact、DS uncertain commit/claim loser、Login ticket↔Resume 门均有定向回归；
  四服务全模块与 Linux race 已绿；proto lint/breaking/Go/C++ 生成全通过（wire 不变）。专用 dev
  玩家后端 A→ReleaseHub→B 事故形状实跑为 assignment 换发、Owner epoch 1→2，ticket/Resume/Owner
  全等且 mismatch=0。唯一标签 `g796da364-dirty-20260818-014129-inc20260818` 已按 Login 先行、
  Hub/DS 缩零排旧 Pod/lease并静默 10.4s、Owner 最后的顺序部署到 `pandora-agones`；四服务
  启动 commit=796da364、旧 RS=0。两轮真实 UE 玩家 Login->Hub 均以同一 exact target 完成
  ACK/Owner ADMITTED/`ResumeContext confirmed HUB`；第二轮 confirmed 后观察 79.658 秒，新增
  Resume 请求、额外 Travel、mismatch、deadline、恢复面板均为 0，本机 Hub P0 已闭环。
  本机停写切换仍不等于生产滚动发布能力，assignment source revision 阻断另见
  INC-20260818-003；未完成前不得宣称 production-ready。扩展 BATTLE/回 Hub 玩家链已尝试，
  但在 DS 分配前被既有 StartMatch `3006/team_not_ready` 阻断，按用户要求停止并列 A-009。
  - **同日落码:对局三段式(准备/战斗/结算)时间轴,用户拍板「不用状态机」+ 冻结深度选 A 纯倒计时**。详见 `docs/design/match-phase-lifecycle.md` §7/§8。**做法是"存时刻不存阶段"**:权威 DS 只持 `PrepareDeadlineSeconds` / `SettleDeadlineSeconds` 两个单调钟时刻(约定 `<=0` = 该段不存在或已过去),阶段是它们的**纯派生投影**——全程无 phase 枚举、无 `SetPhase()`、无转移表,客户端同理(哪一段由三条"剩余秒数"里哪条 `>=0` 派生)。落码:①服务端 `level.proto` 加列 18/19 + `pkg/configtable/level.go` 上限校验 `MaxLevelPhaseDurationSeconds=600`;②UE 侧 `FCfgLevel` 两列、`AMyGameState` 两条复制属性、`PandoraNetProtocolVersion` **10→11**、`PandoraBattleGameMode` 时间轴(复用既有 1s tick 不新建 timer)。
  - **上一条的四个实现细节,复核时别当疏漏**:①**战斗时限武装点已改**——从"首个玩家进入"改成"准备阶段结束"(`FinishPreparePhase`),准备期不再吃玩家对局时间,这是**对已上线行为的语义变更**;②`bAgonesShutdownDeferredBySettleWindow` 必需——原码通知回流后紧接着 `RequestAgonesShutdownAfterDelay(2.0f)`,**只推迟通知而照常回收 Pod 会让玩家在 DS 消失后才收到回流 RPC**(就是"结算完回不去大厅"),两件事必须一起推迟;③`ArmSettleWindow` 里补起表的分支是防卡死——本局若从未有玩家进入则时间轴没武装过,只记 deadline 而无驱动者 = 玩家永远等不到通知(§9.19),World 缺失时降级为立即通知不留悬空等待;④`IsRosterFullyArrived` 解不出应到人数时返回 **false 而非 true**(返回 true 会让准备阶段恒 0 秒、配的时长静默失效)。另:战斗段 deadline 为 0(不限时)时加了防"大负数被当成已到点"的闸。命名债有意保留(`bBattleTimeLimitArmed`/`ClearBattleTimeLimitTimer` 等现在驱动整条时间轴,但它们在日志文案里是可观测性契约,同 `formSoloMatch` 处置)。
  - **上一条的三个阻断/待办**:①⚠️ **proto 已加列而 `Table/关卡/g_关卡.xlsx` 表头没加 → 现在跑导表会整表被拒**,两件事必须同批落地(xlsx 是二进制表,须人用 Excel 加"准备时长""结算时长"两列);②⚠️ **`proto_gen` 未跑,XuanMing-Server 当前编译不过**(校验代码已引用 `GetPrepareDurationSeconds` 等未生成的 getter);③**UE 改动未编译**(按既定分工交用户)。另:协议 10→11 意味着旧客户端连不上新 DS,DS 包与客户端包必须同批发布 + 四份 Fleet 一起换 tag。全部新列默认 0 = 三段式不生效 = 与上线前逐字节等价,故"代码先落、表后配"是安全的 expand-then-enable。
  - **本轮工具坑(已验证,后续照做)**:本机 Git Bash 的 **sed/awk 会静默吃掉 CR** —— 对 `Pandora.cpp` 跑一次 no-op `sed` 就把 CRLF 全打成 LF(5911→5818 字节,CR 93→0)。而这批 UE 源文件的 EOL/BOM 是**逐文件混的**(`CfgLevel.h`=LF/no-BOM、`MyGameState.h`=CRLF/no-BOM、`MyGameState.cpp`=LF/**BOM**、`PandoraBattleGameMode.h`=LF/**BOM**、`.cpp`=LF/no-BOM、`Pandora.cpp`=CRLF/no-BOM),整篇归一化必炸 churn。已写通用补丁器 `scratchpad/patch.ps1`(PowerShell `ReadAllText`/`WriteAllText`,逐文件保 EOL+BOM、片段自动转目标 EOL、OLD 片段命中 0 处或多处直接 throw、写回后回显 eol/bom 供核对),本轮 14 次替换全部原样保住。
  - **同日补全(用户「代码写全、写到完整位置;UI 交给 Codex」)**:①补 Go 单测 `TestValidatePhaseDurations`(`pkg/configtable/level_api_test.go`,5 例:未配置放行 / 上限内放行 / 恰好卡上限放行 / 两列各自超限被拒);②**补上差点漏掉的"完整位置"—— UE 侧导表登记 `Tool/Table/Cs/Proto/g_关卡.json`**(客户端仓,CRLF+BOM),加两列 `PrepareDurationSeconds` / `SettleDurationSeconds`(`isOptCol: true` + `defaultValue "0"`,与 `BattleDurationSeconds` 同款)。**这一处漏了的后果是静默的**:只加 `FCfgLevel` 的 UPROPERTY 而不登记 schema,DS 侧 `MapCheck.CfgLevel->PrepareDurationSeconds` 恒读 0、整个特性不生效,而编译与单测全绿;③写 Codex 交接书 `Doc/服务器/对局三段式UI_UE侧交接_Codex.md`(照 `副本选择_UE侧交接_Codex.md` 体例:含真实行号、4 条坑、6 条验收、以及"别顺手做"的边界)。
  - **上一条顺带查实两件事**:①**"proto 加列而 xlsx 没表头 → 整表被拒"已从生成器源码确认成立**(`tools/configtable-gen/internal/tablegen/builder.go:79 checkHeaders`:`padTo(header, len(d.columns))` 会把 xlsx 表头补空到 proto 列数,随后逐列精确比对,缺列会命中"期望 %q 实为 \"\"");同处 `:89` 是反向那条(xlsx 有而 proto 未登记 → 拒)。**xlsx 列序必须与 proto 字段号序一致**,所以新两列要加在"段位池"(字段 17)之后,顺序 准备时长(18) → 结算时长(19)。②`FCfgLevel` 的 `RatingMode` / `RatingPool` 在 `g_关卡.json` **未登记**(而 proto/CfgLevel.h 都有),即客户端侧这两列可能从未被导表填充过 —— 与本次无关、未改,已在交接书 §6 单列待确认。

## 2026-08-18 免 Docker 一键入口自举 PowerShell 7(策划机从此零安装)

- **起因**:免 Docker 那套的卖点是「策划机什么都不用装」,但三个入口开头都是一段
  `where pwsh` + 「没装就报错退出」——**PowerShell 7 本身仍是必须先手工装的前置**,
  入口注释里也白纸黑字写着 "What a planner machine still has to install: PowerShell 7"。
  新机器拿到包双击,第一下就撞这堵墙。
- **落码**:新增 `tools/scripts/bootstrap_pwsh.cmd`(ASCII / CRLF,cmd.exe 里跑,
  被三个免 Docker 入口 `call`)。解析顺序:①本机已有 `pwsh` → 直接用,一个字节都不下;
  ②`run/localinfra/dist/pwsh/pwsh.exe` 已解包 → 直接用;③取官方免安装包 → 校验 sha256
  → 解包到 ②。取包三条路径与 local_infra.ps1 完全同款:本机 cache → 离线共享盘
  `PANDORA_LOCALINFRA_MIRROR` → 公网,**三条一视同仁过同一个钉死的 sha256**。
- **为什么是免安装 zip 不是 msi**(用户给的就是 msi 链接,这里刻意换了):msi 是按机器安装,
  要本地管理员 + 弹 UAC —— 策划机常常没有管理员权限,而且 Web 管理台是
  `PANDORA_NONINTERACTIVE=1` 无人值守跑这些入口的,一个 UAC 弹窗就是**永久挂住**。
  zip 是同一份官方构建:不写注册表、不装服务、不动机器 PATH,卸载 = 删 `run/localinfra`。
  想要机器级安装的人照常自己装,那样第 ① 步就命中,这套逻辑一行都不会跑。
  **不退回 Windows PowerShell 5.1**(仓库既有口径),否则会把一个当场的清楚失败
  变成几分钟后的糊涂失败。
- **配套**:①`local_infra.ps1` 的 `-Action provision` 多备一份这个包到 cache
  (**只 provision、不 up**:能跑到 up 的机器必然已有解释器,再下 100MB 是白下),
  于是「程序一键出策划包」自动把它和 MySQL/Redis/Kafka/JRE/Envoy/mkcert 一起放上共享盘,
  策划机可**完全离线**自举;②版本 / SHA256 / URL 只写在 `tools/scripts/lib/pwsh_bootstrap.pin`
  一处,cmd 与 ps1 共读,杜绝「自举出 7.6.5、共享盘备的是 7.6.4」这种漂移;
  ③自举成功时把该目录 **prepend 进本进程树 PATH**(不动机器 PATH)——
  仓库里 `start.ps1` / `envoy_cert.ps1` / `configtable_sync.ps1` 有多处用**裸 `& pwsh`**
  自我重入,不接这一步会在半路炸;④三个入口改用 `"%PS%"` 带引号调用(自举出来的是全路径,
  仓库放在带空格的目录下不加引号必炸),并把自举失败时的 `pause` 也纳入
  `PANDORA_NONINTERACTIVE` 判定(原来是无条件 pause,无人值守下同样会挂)。
- **钉死的版本**:PowerShell 7.6.5,`PowerShell-7.6.5-win-x64.zip`,
  sha256 `32eb8f6c…2434ea`。**取值来自上游 release notes 的 "SHA256 Hashes of the release
  artifacts" 段**(不是"我下下来自己算的"——那只是把第一次下到的东西当标准),
  2026-08-18 与实下文件核对一致(106,319,290 字节,解包后 `pwsh -v` = 7.6.5)。
  换版本时必须重走一遍这道核对。
- **契约测试** `tools/scripts/tests/pwsh_bootstrap_contract_test.ps1`(已进 `ci_backend.ps1`
  的 `$contractTests`),6 组:pin 自洽(含"只改 URL 忘改 sha"这类换版事故)、
  单一事实来源(sha 不许在别处再抄)、**入口 .cmd 纯 ASCII 无 BOM 无 chcp**
  (2026-08-06 那条铁律此前零测试)、入口接线、**假包真跑一遍**(临时目录 + 收窄 PATH 造出
  「本机没 pwsh」,共享盘塞个同名假包 → 必须非零退出、必须不解包、坏包必须从 cache 删掉)、
  以及本机 cache 已备料时的**全链路自举**(没备料就明确 skip,不假装绿)。
- **验证**:`-Action provision` 真下真校验通过(已落 `run/localinfra/cache/`);
  在收窄 PATH 的临时树里完整模拟「一台没有 pwsh 的机器」——从共享盘取包 → 校验 → 解包 →
  `PANDORA_PWSH` 指向解包结果 → **裸 `pwsh` 也能解析到**(`where pwsh` 命中该目录,版本 7.6.5);
  契约测试 7 项变异逐个验证会真红,改动文件逐字节还原核对无误。
- **范围**:只接三个 `免Docker` 入口。k8s / 出策划包 / 导表 那几个是程序用的,保持原来的
  「没 pwsh 就明确报错」,没有跟着改。

## 2026-08-18 组队开局:两种准备模式按关卡表二选一(推翻 08-17 的一刀切)

- **拍板**:「匹配前准备」与「匹配成功后确认」是**两种并存模式**,原来那种也是对的,
  按关卡表逐图选。08-13 方案 A(ready 是门槛)与 08-17(全服取消门槛)两次都是全局一刀切;
  而固定队副本要不要先准备、排位要不要接受框,本就是每张图各自的产品决定 →
  按 `CLAUDE.md §17.1` 落成关卡表一列(`g_关卡.xlsx`「准备模式」/ `LevelRow.ready_mode=20`)。
- **两模式互斥**:`PRE_READY`(=1)要求 `State==READY` 才放行组票、撮合成功**直接进场不弹确认框**;
  `POST_CONFIRM`(=2,**留空同**)无门槛、撮合成功进 15s 确认期。刻意不提供「两道都要」
  (同一局点两次准备)与「两道都不要」(退回事故形状)。
- **`ready` 一次一兑两种模式都保留**:`BeginTeamMatch` 无条件清 ready + 转 FORMING。
  `PRE_READY` 下这是必需的 —— 不兑掉队长就能拿同一次准备连开两局(INC-20260813-001 形状);
  契约测试断言「消费后用同一次准备再开第二局必须被拒」。
- **判定单点**:`matchmaker.readyModeForMap(map_id)` 解析一次,经
  `BeginTeamMatchRequest.require_ready` 传给 team,并用同一个判定决定要不要确认期
  (分开判会出现「既要准备又要接受」或「两道都没有」)。不让 team 自己查表:权威 map_id 是
  本次 StartMatch 的那一个,team 记录里的只是队长面板选择(§9.22 不建第二份判定)。
- **`auto_confirm_match` 语义收窄**:最终判定 = `开关 || 本图 PRE_READY`;开关只剩
  「全图强制跳过」一个语义(无 UI 脚本用),`PRE_READY` 图的跳过关不掉。
- **滚动升级零 breaking**:新字段/新列的零值都指向 `POST_CONFIRM` = 改动前行为。
  旧 matchmaker→新 team、新 matchmaker→旧 team、新二进制+旧批次表,三种组合都退化成无门槛,
  **存量表一个字都不用改**。
- **客户端**:准备按钮 / 成员准备栏按 `IsPreMatchReadyRequired()`(读同一列)显隐,确认弹窗保留
  —— r2095/r2096 删掉的准备 UI 按原样恢复成**条件分支**,不是整体回退(整体回退会把确认弹窗
  也一起弄没)。WBP 里 `Btn_ReadyToggle` / `Txt_MemberReady_N` 控件本体当初没清,**无需改蓝图**。
  `hero_id` 断供随 `PRE_READY` 图自动恢复(`SetReady` 捎带),`POST_CONFIRM` 图仍回落 0。
- **验证**:team / matchmaker / pkg/configtable / tools/configtable-gen / robot/stress 五处
  build+test 全绿,gofmt 干净;新增用例覆盖两模式放行与拒绝、门槛排在租约冲突之后、
  `readyModeForMap` 五种回退、`formMatch` 端到端确认期开关。UE 编译由用户执行。
- **未做 / 待办**:①`configtable/dist` **刻意没重导** —— 新列全空,protojson 省略零值,
  产物逐字节不变;而源表 xlsx 尚未进 SVN,此时重导只会写进一个假 `source_rev`(违反溯源纪律)。
  策划提交 xlsx 后按真实 rev 重导一次即可。②「过错方自动续排」仍待拍板。

- **续(同日)**:按用户拍板已把「准备模式」值写进 `g_关卡.xlsx` —— 6(1V1战斗)与 7(松林镇)填 2
  =赛后确认,其余 9 张战斗关卡(4/5/8/9/10/11/12/13/14)填 1=赛前准备,1/2/3 非战斗行留空。
  `-sync` 表头对齐通过,`[OK] level 14 行` 说明新列与类别校验都过。
  **但 dist 仍未重导**:整批被**另一张并发改动的表**卡住 —— `角色/j_角色等级.xlsx` 第 31 行
  (角色 1012)「击杀经验」必填列为空(该文件 mtime 05:46,在本批次上一次导表成功之后被改),
  导表按 §9.15 fail-closed 整批不产出。补上那一格后重导一次,准备模式即生效;
  该格是策划数据,不代填。
- **再续(同日,卡点已解)**:用户拍板把 `j_角色等级.xlsx` 第 31 行(角色 1012)「击杀经验」填 **0**
  (与相邻 1011/1013/3001 一致),导表随即通过,`configtable/dist` 已重导为 version 20260818002,
  `source_rev=svn-r2110-local-readymode`(**刻意标 local**:两张源表 xlsx 尚未进 SVN,
  这批产物不可从某个干净 rev 复现;策划提交后应按真实 rev 再导一次覆盖本批次)。
  `level.json` 已带 `ready_mode`:6/7 = 2,4/5/8/9/10/11/12/13/14 = 1。
  ⚠️ **本批次顺带带进了期间别人改的表**:`role.json`(+13)、`role_level.json`(+8)、
  `skill.json`(+52,技能 63→67 行)。提交 dist 时要么连同这些一起说明,要么等对方先把源表提交。
  pkg/configtable(含钉死真实 dist 的契约测试)/ matchmaker / team 三处测试全绿。
- **2026-08-19 溯源勘误**:策划源表后继提交 SVN **r2129**(`关卡/g_关卡.xlsx`,提交说明
  「准备模式修复」)把关卡 6/7 的 `ready_mode` 从 2 修正为 1;其余已配置战斗关卡仍为 1。
  `configtable/dist` 已按该干净 revision 重导为 version **20260819001**、
  `source_rev=svn-r2129`;Git 侧仅 `level.json` 与 `manifest.json` 变化,不再夹带 08-18 记录的
  role/role_level/skill 并发数据。31 张表 checksum 全匹配;pkg/configtable、configtable-gen、
  matchmaker、team、robot/stress 均以 `go test -count=1 -p 1 ./...` 通过。

## 2026-08-18 首次生产上线日期成为全仓兼容闸

- **用户确认当前尚未首次生产上线**；`CLAUDE.md §3.1` 新增唯一状态字段
  `首次生产上线日期（YYYY-MM-DD）：未填写`。该字段只能由用户本人填写 / 纠正，AI 不得从测试环境、
  镜像、tag 或提交历史自行推断。首次面向真实玩家或外部生产消费者发布前必须先填写；真实上线后不得清空。
- **日期未填写时**，允许 proto 字段 / 编号 / 类型 / 语义、enum、RPC、数据库 / migration 基线、
  Redis / Kafka / blob 格式、配置表与客户端生成物做有意 breaking，不要求兼容尚未发布的旧 dev 构建；
  但必须把所有生产者 / 消费者、Go / C++ pb、配置表 dist / manifest、客户端资源、测试与受影响 dev / test
  数据作为一个批次刷新并验证，不能混读新旧数据或留下半成品。
- **日期填写后**，字段 / enum 编号永久不复用，RPC / 事件 / 存储格式须支持新旧共存，schema 走版本化
  migration 与 expand → migrate → contract，Stable / Canary 与回滚按不停服规范执行。
- **不随日期放宽**：鉴权授权、owner / fencing、幂等 / 原子 / 事务、数据正确性、容量 / 分页 / 保留期、
  超时 / 重试 / 可见恢复、玩家不卡死、事故建档与验证证据等红线。同步更新 `AGENTS.md`、
  `docs/design/proto-design.md`、`docs/design/zero-downtime-update.md` 与 `docs/design/pandora-arch.md` 的引用口径，
  避免多份“是否上线”状态漂移。

## 2026-08-18 一键导表按目录名找客户端仓 → 改成按内容找(策划机起不来)

- **现场**:策划双击 `策划一键启动-免Docker-测试版.cmd`,pwsh 自举完成后在**第一步导表**
  就 `[ERR] 找不到客户端策划表根目录(Table)`,整套后端起不来。根因不是环境问题:
  `configtable_gen.ps1` 的 `Resolve-TableRoot` 只找 `<后端仓>\..\Pandora-Client-SVN\Table`
  和写死的 `F:\work\Pandora-Client-SVN\Table` 两处 —— 而**客户端仓在 SVN 上的名字是 `Client`**
  (`^/trunk/Client`,与 `Art` / `Design` / `Server` 平级),`svn checkout` 不指定目标目录时
  检出来就叫 `Client`。同一个仓在不同机器上至少两种名字,脚本只认其中一种。
- **修法:不赌名字,按内容认仓**。新增公共件 `tools/scripts/client_repo_lib.ps1`,顺序为
  ①显式入口(`-TableRoot` / `PANDORA_CLIENT_TABLE_ROOT` / **新增 `PANDORA_CLIENT_REPO`** /
  从 `PANDORA_DS_UPROJECT` 回推两级)→ ②后端仓平级 / 上一级的已知仓名
  (`Client` 排第一,其后 `Pandora-Client-SVN` / `Pandora-Client` / `ClientBase`)→
  ③同两层里任意仓名通配(容忍中文自定义名;并试 `<目录>\Client`,覆盖"整个 trunk 检出成一个目录")
  → ④各固定盘根目录及其下一层的已知仓名(两个仓没放一块时兜底)→ ⑤老开发机写死路径。
  判据始终是 `Table` 下**递归有 xlsx**,名字只决定先看谁。
- **近旁优先于满盘**:一台机器上同时存在多份检出是常态(本机 `F:\work\Pandora-Client-SVN`
  与 `D:\luyuan\Client` 各一份,后者还是 r2097 的旧检出)。第一版把"另一块盘上名字对得上的"
  排在"后端仓旁边名字不对的"之前,被新契约测试当场抓红,已改为近旁证据一律优先。
- **三处共用一份**:`configtable_gen.ps1` / `configtable_sync.ps1`(导表与表头同步)和
  `start.ps1` 的 `Resolve-PandoraUProject`(定位 DS 的 `.uproject`)找的是**同一个客户端仓**,
  此前各写一套。漂移的表现是"导表用 A 仓的表、DS 读 B 仓的资源",两边都不报错。现全部改为
  调 `Get-PandoraClientRepoCandidate`;实测同一布局下两者结论一致
  (策划机 `D:\luyuan\Pandora-Server` → 两者都落 `D:\luyuan\Client`)。
- **多份检出必须报出来**:命中不止一个时,`configtable_gen.ps1` 打印本次用的是哪一份、
  还有哪些没用、怎么切。选错那份的表现是"我明明改了表却没生效",最难自查。
  另把「`Table` 在但一张 xlsx 都没有」单独归为 `NearMiss` 报告 —— 那是**检出不完整 /
  源表没进 SVN**(2026-08-12 Jenkins 那次的同款),跟"路径找错了"是两回事,报错方向不能混。
- **找不到时的提示重写**:列全找过的位置,并给出 `setx PANDORA_CLIENT_REPO ...`、
  `-TableRoot ...`、以及可直接复制的 `svn checkout http://infinity-svn/svn/Pandora-Moba/trunk/Client D:\Client`。
- **门禁**:新增 `tools/scripts/tests/configtable_client_repo_resolve_test.ps1`(8 组、13 条断言,
  在临时目录造真实布局跑真解析,非源码 grep),已登记进 `ci_backend.ps1` 的 `$contractTests`。
  这个回归没有任何 go test 能挡(逻辑全在 ps1 里),且**开发机永远是绿的** ——
  只有目录名不一样的机器才炸,人工 review 也看不出来。
  既有 `configtable_gen_svn_status_test.ps1` / `configtable_gen_error_hint_test.ps1` 复跑全绿;
  本机 `configtable_gen.ps1 -Check` 结果与改动前一致(仍是 `F:\work\Pandora-Client-SVN\Table`)。
- 文档同步:`docs/ops/planner-quickstart.md` 新增「客户端仓放哪儿、叫什么名字」,
  `tools/devops/BUILD-MACHINE-SETUP.md` 注明 `D:\Pandora-Client-SVN` 只是例子,
  `tools/scripts/README.md` 登记公共件与新契约测试。


## 2026-08-19 客户端仓候选含不存在盘符 → 启动报 Cannot find drive

- **现场**:`D:\Pandora-Moba\Server` 启动 DS 时,公共定位器最后加入历史候选
  `F:\work\Pandora-Client-SVN`;该机没有 F 盘,`Resolve-PandoraUProject` 在存在性判断前用
  provider-aware `Join-Path`,因此直接中止。过期的 `PANDORA_CLIENT_REPO` 指向已不存在盘符时,
  导表 / 表头同步也有同形状故障。
- **修法**:候选消费点统一用 `[System.IO.Path]::Combine` 做纯词法拼接,再由
  `Test-Path -ErrorAction SilentlyContinue` 判断存在性。历史 F 路径仍是最后一级兼容候选,
  但本机没有 F 盘时只会被跳过,不再要求仓库必须位于任何固定盘符。
- **门禁 / 验证**:`configtable_client_repo_resolve_test.ps1` 扩到 10 组、19 条断言,
  动态选一个不存在的 A-Z 盘符,覆盖坏候选在前的 Table 定位和坏候选在末的真
  `Resolve-PandoraUProject`;全绿。本机另以过期盘符环境变量跑真
  `configtable_gen.ps1 -Check`,仍正确定位实际 Table 并通过。截图那台无 F 盘机器的双击入口待更新脚本后复验。


## 2026-08-19 本机基础设施起不来时只给日志路径 → 改成把现场打出来

- **现场**:策划机(`D:\Pandora-Moba\Server`,即 `^/trunk` 整检出的 `Server` + `Client` 布局)
  跑免 Docker 一键启动,导表已通过(见上一条修复),卡在
  `[ERR ] mysql 启动后立即退出 (exit 1)。日志: ...\logs\mysql.log`。
  **排查到此为止** —— 那台机器不在手边,而窗口里只有一个路径,没有一个字的原因。
- **判断**:exit 1 是 mysqld 自己 abort(不是 0xC0000135 那种加载失败),原因必然写在
  `mysql.log` 里;`--initialize-insecure` 已成功(否则会先报"初始化失败"),所以问题出在
  正常启动这一次 —— 它与初始化那次的差别只有三处:绑 :3307、`--no-monitor`、`--init-file`。
- **修法(不猜原因,改现场可见性)**:`Wait-Port` 的两条失败路径(立即退出 / 超时未监听)
  改为调用新增的 `Show-ComponentFailure`,输出:①退出码的已知含义(0xC0000135 缺 VC++ 运行库、
  0xC000007B 备料包架构不对);②`Get-PortHolder` 报出端口占用者(进程名 + pid + 映像路径,
  不用人去 netstat);③日志**末尾 40 行原文**;④命中 `$script:InfraLogHints` 里的已知原因
  并给出人话处置(端口占用 / 目录不可写 / my.ini 有本版本不认的项 / 内存不足 /
  网络通道全关 / 引导 SQL 失败 / 数据目录损坏 / 上一轮进程占着数据目录)。
  MySQL 初始化失败与"起来了但账号连不上"两处也一并接上。
- **`--validate-config` 兜底**:mysqld 只有成功打开 `log-error` 之后才往日志里写;my.ini 本身
  有问题时报错只去 stderr,而常驻启动那次**刻意没做重定向**(见 Kafka 那段句柄继承的说明),
  于是现场只剩一个 exit 1、日志一个字没有。日志为空或没命中任何已知模式时,自动用
  `mysqld --defaults-file=... --validate-config` 在前台再问一次并贴出 stderr(只读、几百毫秒)。
- **`Read-LogTail` 必须用 `FileShare.ReadWrite` 打开**:超时那条路径上组件进程还活着占着日志,
  `Get-Content` 的默认共享模式会直接抛异常 —— 诊断自己失败,现场反而看不到。测试里有反证用例。
- **首版自己就有一个正好属于本类的 bug**:`Read-LogTail` / `Get-PortHolder` 用 `return @(...)`,
  而 PowerShell 会把返回的空数组拆成「什么都没返回」= `$null`,于是「日志是空的」(别在日志里
  找原因)和「日志读不到」(路径不对)两条**相反**结论撞成同一个值,`$holders.Count` 还成了在
  `$null` 上取属性 = 在报错处理里再炸一次。已改 `return , @(...)`。**这个 bug 是新契约测试当场
  抓出来的**,不是 review 看出来的。
- **门禁**:新增 `tools/scripts/tests/localinfra_failure_diagnostics_test.ps1`(6 组 20 条断言),
  已登记进 `ci_backend.ps1`。用 AST 从 `local_infra.ps1` 抠出被测函数原文来跑(整脚本 dot-source
  会真的去备料起进程),所以测的确实是仓库里那份代码。样例日志取自本机
  `run/localinfra/logs/mysql.log` 的真实失败现场(2026-08-15 的 `MY-010131` → `Aborting`)。
  这段代码**只在出故障时才执行**,正常跑一百次也碰不到一次,没有机械门禁就只能等下一次事故
  才发现它自己坏了。
- **仍未定谳**:策划机那次 exit 1 的具体原因。需要那台机器上
  `run\localinfra\logs\mysql.log` 的末尾;装上本次改动后重跑一次,原因会直接打在窗口里。
