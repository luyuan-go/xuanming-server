# tools/scripts 导航

后端仓库的 PowerShell 运维/开发脚本索引。**不要 ad-hoc 新起脚本**;新增前先看这里有没有可复用的。

> 约定:所有脚本从**仓库根目录**运行,例:`pwsh tools/scripts/start.ps1 -Mode local`。

## 1. 启动 / 编排(日常主链)

| 脚本 | 用途 | 被谁调用 |
|---|---|---|
| `start.ps1` | 项目总入口,5 模式编排(local/docker/intranet/battle/k8s/online) | 根目录 `start.cmd` |
| `play.ps1` | 策划友好入口(docker 模式,DS=mock;battle 启动已废弃,仅留 `-Battle -Stop/-Status` 清理遗留) | `策划一键停止.cmd` |
| `dev_all.ps1` | 一键起基础设施 + 全业务 go 服务 | `start.ps1`(local) |
| `dev_up.ps1` | 起 docker 基础设施(MySQL/Redis/Kafka/etcd/Prometheus) | `start.ps1`、`dev_all.ps1`、`play.ps1` |
| `dev_down.ps1` | 停基础设施容器 | `start.ps1`、`dev_all.ps1`、`play.ps1` |
| `dev_status.ps1` | 查看开发环境状态(容器 + 端口监听) | 手动 |
| `run_services.ps1` | 宿主 go 服务编排(启/停/看日志) | `start.ps1`、`play.ps1`、`dev_all.ps1`、`dev_tools.ps1` |
| `gen_cluster_config.ps1` | 生成集群版配置(容器地址 / allocator 模式；auction 强制 etcd Snowflake + 跨实例锁) | `start.ps1`(docker/battle 等) |
| `tidb_up.ps1` | TiDB 集群一键起(社交库可选) | 手动(见 `deploy/tidb-init/README.md`) |

生产生成除了玩家面、DS callback 两把 key，还必须注入四把互不相同的 placement proof key：
`PANDORA_PLACEMENT_ACCOUNT_BOOTSTRAP_SECRET`、`PANDORA_PLACEMENT_MATCH_START_SECRET`、
`PANDORA_PLACEMENT_BATTLE_EXIT_SECRET`、`PANDORA_PLACEMENT_HUB_TRANSFER_SECRET`，以及独立的
Login→Matchmaker 服务身份 key `PANDORA_MATCH_RESUME_AUTH_SECRET`、
Team→Matchmaker 服务身份 key `PANDORA_TEAM_RESUME_AUTH_SECRET`（两者读同一个
`ResolvePlayerMatchContext`，但必须是两把 key：共用等于两个服务的信任域合并；team 侧漏配会让入队闸门
fail-closed、招募列表恒空）。生成器会拒绝公开 dev key、短 key
或跨权限域复用；普通 online 发布还会在 apply 前与锁内两次拒绝服务身份 key 漂移。`placement_mode=shadow`
仅供先服务端后客户端的短期灰度，终态为 `enforce`。

## 2. k8s / 真 DS 链路

| 脚本 | 用途 | 被谁调用 |
|---|---|---|
| `e2e_k8s.ps1` | 本地 minikube+Agones 真 DS 闭环(load 镜像 + 桥接 Envoy + 等 Fleet + UDP 中继) | 手动(`k8s` 模式起完后) |
| `k8s_envoy_bridge.ps1` | 宿主 Envoy 端口转发桥接 | `e2e_k8s.ps1` |
| `udp_relay.ps1` | UDP 回程中继(minikube docker driver 下 DS 连通) | `e2e_k8s.ps1` |
| `dsticket_keyset.ps1` | K1/bootstrap 或轮换阶段材料的生成/校验；create-only 投递 immutable signer Secret 与 default/pandora 两份 public JWKS，首次投递可 create-only 补齐 `pandora` Namespace | 人工 bootstrap / 轮换材料预投递 |
| `dsticket_rotate.ps1` | 独立执行 DSTicket `stage → promote → retire` 不停服轮换；与普通 online 发布共用线性操作锁，按 apiserver 时间等待清退窗，不删除旧密钥或在场 GameServer | 人工受控安全操作；不得由普通发布隐式调用 |
| `reset_data_service_schema.ps1` | 开发期定向重置 data_service 的 `player_data` 表与玩家缓存；固定本地 minikube context，默认停服不重启 | 手动；需 `-Confirm`/`-Force` |
| `reset_data_service_schema_k8s.bat` | 上述重置脚本的 Windows k8s 包装器；第二参数 `restart` 可在新镜像就位后重启并验表 | 手动/双击 |

> DS(Hub/Battle)本身由 Pandora-Client / UE 侧仓库产出,后端不再维护 DS 编译或 stub 脚本。

## 3. 证书 / TLS

| 脚本 | 用途 | 被谁调用 |
|---|---|---|
| `envoy_cert.ps1` | Envoy TLS 证书校验/自愈(共享库) | `dev_up.ps1`、`install_shared_dev_ca.ps1`、`k8s_envoy_bridge.ps1` |
| `dev_env_file.ps1` | 本机 `deploy/env/dev.env` 自举(被 git 忽略,新机器必然缺;共享库) | `start.ps1`、`dev_up.ps1`、`dev_down.ps1`、`dev_status.ps1`、`k8s_envoy_bridge.ps1` |
| `client_repo_lib.ps1` | 客户端 SVN 工作副本 / 策划表根目录定位(共享库)。**不认死目录名**:SVN 上叫 `Client`,文档里写 `Pandora-Client-SVN`,策划机上可能又是别的名字;按显式入口 → 已知仓名 → 后端仓平级通配 → 各固定盘已知仓名的顺序找,并把"本机还有别的检出"报出来 | `configtable_gen.ps1`、`configtable_sync.ps1`、`start.ps1` |
| `install_shared_dev_ca.ps1` | 安装全队共享开发 CA | 手动(见 `deploy/dev-ca/README.md`) |
| `import_dev_ca.ps1` | 客户端信任开发 CA 证书 | 手动 |

## 4. 镜像 / 工具链 / proto

| 脚本 | 用途 | 被谁调用 |
|---|---|---|
| `export_images.ps1` | 导出业务镜像 tar(离线分发) | `出离线镜像包.cmd` |
| `import_images.ps1` | 离线导入镜像 | 手动(见 `docs/ops/planner-quickstart.md`) |
| `install_dev_tools.ps1` | 安装开发工具链(go/docker/kubectl/minikube/mkcert 等) | 手动(见 README) |
| `proto_gen.ps1` | 生成 go pb(proto 改动后由 Codex 跑) | 手动(见 CLAUDE.md §5) |

## 5. 压测 / 发布 / 诊断

| 脚本 | 用途 | 被谁调用 |
|---|---|---|
| `dev_tools.ps1` | 开发工具集(清 MySQL/Kafka/etcd、重置 offset) | 手动(见 `docs/design/stress-discipline.md`) |
| `stress_snap.ps1` | Prometheus 快照批量抓取(压测采集) | 手动 |
| `stress_summarize.ps1` | 压测单轮汇总(5 段二维表) | 手动 |
| `release_preflight.ps1` | 发布前预检(配置安全 / 密码强度) | 手动(见 `docs/ops/release-checklist.md`) |
| `http2_probe.ps1` | 探测 Envoy 客户端连接是否走 HTTP/2 | 手动(见 `docs/design/gateway-decision.md`) |
| `lib/online_manifest_contract.ps1` | online 镜像 digest pin、writer/Fleet annotation 与渲染契约纯 helper(不访问远端) | `start.ps1`、静态测试 |
| `lib/dsticket_keyset_contract.ps1` | DSTicket 私钥/JWKS/K8s 对象严格对账（RFC 7638、顶层 active_kid、immutable/hash） | `start.ps1`、`dsticket_keyset.ps1`、静态测试 |
| `lib/dsticket_rotation_contract.ps1` | DSTicket 三阶段材料、marker 时间链、controller/Pod owner、普通发布终态与共享操作锁契约 | `start.ps1`、`dsticket_rotate.ps1`、静态测试 |
| `tests/online_manifest_contract_test.ps1` | online 镜像/Fleet 契约与 mutant 反例测试 | 手动/CI |
| `tests/dsticket_keyset_contract_test.ps1` | active key 非 keys[0]、双 namespace 同 hash、immutable 等 DSTicket mutant 测试 | 手动/CI |
| `tests/dsticket_rotation_contract_test.ps1` | K1/K2 三阶段、225 秒清退窗、marker 历史链、孤儿/伪造 owner 与发布互斥 mutant 测试 | 手动/CI |
| `tests/services_dsticket_secret_contract_test.ps1` | 四个 signer 私钥卷/非 root/fsGroup 与 Login-only public JWKS 契约 | 手动/CI |
| `tests/gen_cluster_b1_contract_test.ps1` | B1 signer/verifier、Model-B callback、Stable/Canary allocator 配置生成契约 | 手动/CI |
| `tests/gen_cluster_team_resume_auth_contract_test.ps1` | Team→Matchmaker 服务身份 key:两端成对、与 login 那把独立、-Prod 必填与跨域复用反例 | 手动/CI |
| `tests/configtable_gen_svn_status_test.ps1` | 导表失败归因用的 SVN 判定(取版本号 / 未提交判定,含"干净副本 ≠ 没装 svn")行为测试 | 手动/CI |
| `tests/configtable_client_repo_resolve_test.ps1` | 客户端仓定位:SVN 原名 `Client` / 自定义仓名 / trunk 整检出 / `-TableRoot` / `PANDORA_CLIENT_REPO` / 空 Table 归类 / 多份检出必须报出来 | 手动/CI |
| `tests/oneclick_devenv_exitcode_contract_test.ps1` | 策划一键启动两条护栏:dev.env 自举三态(建/不覆盖/工作区不全硬失败)+ `Invoke-Local` 必须透传 dev_all 退出码 | 手动/CI |
| `tests/publish_to_minio_contract_test.ps1` | MinIO 分发错误传播、不可变内容先传/latest 指针后切、允许远端历史对象的单向完整性校验 | 手动/CI |
| `tests/infra_etcd_persistence_contract_test.ps1` | 本地 etcd PVC/Recreate 持久化契约与反例 | 手动/CI |
