# 决策复议：策划一键启动改用中心 MySQL

> 状态：**已拍板，PowerShell 客户端已接线，迁移器 verify-only 与真实新机 E2E 待闭环，2026-08-20**。用户要求策划电脑不再安装或启动 MySQL，双击一键入口时连接局域网中心 MySQL；Git、Python 与策划 SVN 必须使用同一实现和同一数据库身份契约。

## 1. 被复议的旧决策

现有免 Docker 路线在每个工作区下载便携 MySQL、从 `13307..13398` 选择本机独占端口，并以 PID、`mysqld.exe`、本工作区 `my.ini` 和 listener 四重证据证明实例归属。这个设计正确解决了“策划电脑已有 Docker MySQL 占用 3307”以及“不能误迁移、停止外部 MySQL”的问题。

新需求改变了部署前提：策划电脑只运行 Redis、Kafka、JRE、Envoy、PowerShell 和业务进程，数据库改由一台受管中心机提供。继续在每台电脑分发 248.69 MiB MySQL ZIP、解包近 1 GiB dist、初始化数据目录和启动 `mysqld` 已不再必要。

本复议只改变 **`central-planner` profile**。原 `local-owned` profile 保留，直到中心模式完成真实新机 E2E、断网/恢复和回滚验收；两种 profile 不自动互相 fallback。

## 2. 新决策

### 2.1 一个中心实例，每个 workspace 十个隔离数据库

当前业务有 14 条 MySQL DSN，落到 10 个 migration set：

`pandora_account`、`pandora_player`、`pandora_social`、`pandora_battle`、`pandora_trade`、`pandora_auction`、`pandora_leaderboard`、`pandora_bag`、`pandora_owner`、`pandora_mission`。

不能把它们合并进一个 schema：每个 migration set 都有独立 `schema_migrations`，库级职责、迁移版本和权限也不同。中心实例为每个 workspace 创建十个物理数据库，命名为：

```text
<canonical migration set>_w_<workspace_id>

例如：
pandora_account_w_01jabc...
pandora_player_w_01jabc...
...
pandora_mission_w_01jabc...
```

`migration_set` 始终使用仓库 canonical 名称，`database` 使用上述物理名。迁移 SQL 不复制、不按电脑生成第二套；`tools/migrate` 通过显式 mapping 把同一 migration set 应用到不同物理库。

### 2.2 用户名只作可读标签，不能单独充当身份

- `display_name` 使用 `COMPUTERNAME\USERNAME`，供启动日志、中心管理界面和人工排障识别。
- 中心 registry 不采集、不持久化客户端 IP；只保存 `display_name` 与 `last_seen_at` 诊断事实。IP 绝不参与数据库命名、权限或旧 workspace 认领。
- 首次登记由本机 MachineGuid + Windows 用户 SID 的不可逆摘要辅助识别设备，中心 provisioner 分配 128-bit 随机 `workspace_id`。canonical 形态为 26 位小写 Crockford Base32，首位仅允许 `0..7`。最终权威是服务端唯一登记的 `workspace_id`，不是用户名、电脑名、IP 或工作副本路径。
- `workspace_id` 是非秘密，持久化到 `%LOCALAPPDATA%\Pandora\planner-db\identity.json`。用户/电脑改名、切换 Wi-Fi、DHCP/VPN 变更、移动或重新 update SVN 工作副本都必须继续命中同一 workspace。
- 密码、enrollment token 和客户端证书只进入 Windows Credential Manager 或当前用户 DPAPI 受限存储，不写 Git、SVN、静态配置、日志、命令行或 `identity.json`。现有服务只接受 DSN，因此启动器可在 `run/` 下生成 ACL 仅当前用户可读的临时运行 YAML；进程读入后立即删除，fingerprint/state 只留脱敏事实。
- PowerShell/.NET immutable string 无法承诺响应明文在托管堆中被物理清零；实现在转为 `SecureString` 后立即清除 HTTP object 中的密码引用，并在 `finally` 释放 token/body 引用。可验证的安全承诺是不落盘、日志、进程参数和非秘密 profile，不夸大为内存绝对零残留。

### 2.3 唯一数据库 profile seam

新增一个深模块作为唯一 seam，提供两个 adapter：

- `local-owned`：保持现有本机 PID/exe/my.ini/listener 归属证明和动态端口。
- `central-managed`：解析中心 endpoint、TLS 服务端身份、workspace、凭据引用和逻辑库→物理库映射；只允许操作本 workspace。

两者输出同一份生成态文件：

`run/localinfra/cfg/mysql-runtime-profile.json`

```json
{
  "schema_version": 1,
  "mode": "central-managed",
  "workspace_id": "01jabc...",
  "display_name": "PC02\\planner",
  "endpoint": {
    "host": "pandora-dev-db.intra",
    "port": 3306,
    "tls_server_name": "pandora-dev-db.intra",
    "ca_file": "D:\\workspace\\Pandora-Server\\installers\\planner-db\\planner-db-ca.pem"
  },
  "credential_ref": {
    "provider": "dpapi-current-user",
    "target": "Pandora/PlannerDB/01jabc.../app",
    "version": 1
  },
  "databases": {
    "pandora_account": "pandora_account_w_01jabc..."
  },
  "fingerprint": "sha256:..."
}
```

运行态文件受 `.gitignore`/SVN ignore 保护且不含密码。`fingerprint` 覆盖 mode、endpoint、TLS 身份、CA 文件实际 SHA-256、workspace、十库映射和 credential version，用于完整启动、单服重启和 `-DsOnly` 拒绝混合 DSN。同路径替换 CA 也会使旧 profile fail-closed，重新发布 profile 后强制全服刷新。

PowerShell 启动器是 profile 的唯一配置生成者；它一次性渲染 Go 与 Python 运行 YAML。Go/Python 服务只消费普通 DSN，不各自复制 workspace 命名算法。删除该模块会迫使 host、TLS、十库映射、凭据引用和运行态一致性重新散落到多处，因此该 seam 有实际 depth 与 locality。

### 2.4 自动 provisioning 必须在中心侧持有权限

策划电脑不能得到 root、`CREATE USER`、`GRANT OPTION`、`SUPER`、`FILE` 或全局 `CREATE/DROP`。自动建库由中心 provisioner 完成：

1. 客户端生成/读取设备身份并通过 TLS 登记；认证优先使用 Windows 集成身份，非域环境使用一次性 enrollment token。
2. provisioner 原子登记唯一 workspace，幂等创建十个物理库和独立账号。
3. 中心 migration 账号应用十个 migration set；任一库失败则 workspace 保持 `MIGRATION_FAILED`，不得开放业务账号。
4. 十库均为目标版本、`dirty=false` 且权限验证通过后才进入 `READY`。
5. 客户端只取得本 workspace 的运行账号凭据，保存到 Credential Manager；之后启动只做有界 DNS/TLS/认证/schema-version 预检。

建十库和迁移不能假设 30 秒内同步完成。`POST /v1/enroll` 首次或处理中返回
`202 Accepted`，正文只含同一 `workspace_id`、状态、受信 endpoint、十库映射和有界
`retry_after_ms`，绝不带 credential；同一 token + device digest 重试必须幂等收敛。只有
十库全部验收为 `READY` 才返回 `200` 和 runtime credential。客户端保留一次性 token 于本轮
内存中，在总截止时间内按服务端节奏轮询；超时或连接结果未知时可重发同一请求，但不得换
workspace、回落本机或把响应正文写日志。`MIGRATION_FAILED` 等终态立即可见失败，不继续忙等。

幂等只对“同一已消费 token + 同 device digest”成立。未消费的新普通 token 遇到已登记 digest 必须返回 `409 DEVICE_ALREADY_ENROLLED`、回滚事务并保持 token 未消费，防止 MachineGuid+SID 被克隆后领取旧库凭据。DPAPI/本机绑定丢失时，只能由管理员签发绑定 `RECOVERY + exact target_workspace_id` 的 recovery token。管理命令 stdout 只出现一次 `<workspace_id>.<43-char-raw-token>` recovery code；管理员把整段交给策划，客户端仅在内存中拆分为 API 的 raw `enrollment_token` 与 `expected_workspace_id`。服务端同时核对 token target、旧 device digest 和 expected ID；recovery 永不创建新 workspace/账号/密文，整段 code 及 raw token 都不落日志/profile。

`MIGRATION_FAILED` 的 `/v1/enroll` 固定返回 `409` 和无凭据 state 响应，本次及后续请求都不自动重跑 DDL。中心管理员修复根因后执行 `retry-workspace -workspace-id <id>`，它只做 `MIGRATION_FAILED → PROVISIONING` 单条 CAS 并只输出 workspace/state；之后由原 token 或 recovery token 的客户端请求启动 worker。

账号职责分离：

- provisioner：只在中心机持有高权限，负责建库、建/停账号和显式生命周期操作。
- migration 账号 `p_mig_<26-id>`：只由中心迁移任务使用，对本 workspace 十库具有精确 `SELECT/INSERT/UPDATE/DELETE/CREATE/ALTER/DROP/INDEX/REFERENCES`。MySQL 的 schema 级 `DROP` 同时允许删该账号获 grant 的本 workspace 库，这是内嵌 `DROP TABLE`/`RENAME TABLE` 必需权限的 blast radius；账号无其它 workspace/库或全局 `DROP`，且不下发策划机。
- runtime 账号 `p_app_<26-id>`：只对本 workspace 十库具有运行所需最小权限。`data_service` 的运行期 `SyncAllTables` 必须纳入权限验收，长期迁到 provisioning/migration 后恢复 DML-only。

禁止共享 `pandora/pandora_dev_pwd`、禁止 `GRANT ... ON pandora_%.*`、禁止用 `user@host` 的客户端 IP 冒充 workspace 身份。

### 2.5 迁移、并发与失败语义

- 十库不是一个分布式事务。迁移按目标逐库执行并保留各库 `schema_migrations`；部分成功后保持阻断，修复后幂等重跑，不试图用跨库“回滚”伪造原子性。
- 同一 workspace 同一 migration set 复用现有 advisory lock；中心外层协调器还要按 workspace single-flight，锁竞争有限退避，不能让所有策划电脑同时拿 DDL 账号跑迁移。
- `/v1/enroll` 有固定 64 并发槽和 10 秒总 deadline，槽满在读 body/消费 token 前返回 `503 + Retry-After`；后台 DDL 另有小型固定 worker 池，饱和时由后续轮询重试排队，不生成无界 goroutine。
- 新 release schema 迁移完成前旧 runtime 继续使用旧 profile；迁移后若旧二进制不兼容更高 schema，必须在拉起业务进程前明确拒绝。
- 中心不可达时一键启动在任何业务进程启动前有界失败，分别报告 DNS、TCP、TLS、认证、workspace 状态和 schema version；绝不静默启动本地 MySQL或改连 canonical 公共库。
- 客户端只用 runtime 账号执行 `pandora-migrate -verify-only`：逐目标验证 TLS/SAN、认证、
  `DATABASE()`、`schema_migrations.version ==` 当前二进制内嵌最新版本且 `dirty=false`；该模式
  不获取迁移锁、不运行 SQL migration、不执行 DDL。真正的 up migration 只在中心 worker 使用
  migration 账号运行。
- 运行中网络中断存在“服务端可能提交、客户端没收到响应”的 unknown outcome。业务层继续依赖现有幂等键/事务语义；启动器不得盲目重放非幂等 SQL。
- `down/reset` 在 central 模式永远不 shutdown、taskkill、DROP 或清空中心库。停用/清理只能由中心 provisioner 显式执行；默认只报告待清理量，不按 IP 消失或久未上线自动删除。
- 中心 `audit` 只读报告卡住/失败/inactive workspace 与过期 token hash；inactive 不自动删除。未消费过期 token 与已消费超 30 天 hash 在下次管理签发时清理，清理失败必须让签发失败可见。workspace/账号/schema 永不由 audit 删除。

## 3. Git、Python、SVN 同步边界

- Git `F:\work\XuanMing-Server` 是源码主线：profile 模块、provisioner、迁移器、Go/Python 配置契约、测试和文档先在 Git 完成。
- Python 不复制 profile 解析/数据库命名逻辑；启动器用同一 profile 生成其普通 DSN。Python 的 MySQL/TLS/连接池行为必须有独立消费契约测试。
- SVN `D:\luyuan\Pandora-Server` 同步与 Git 相同的正式脚本、文档及发布后的可运行制品。当前大量未提交 Python 工作树不得整包复制到 SVN冒充发布。
- 中心模式通过 E2E 后，SVN 可删除 MySQL 便携包；Git 本来只跟踪安装包目录契约。`local-owned` 若仍保留为显式 profile，其 MySQL 包改为按需网络/镜像获取或另行发布，不能自动 fallback。
- Git 与 SVN 的实现、pin、profile schema 和迁移清单必须由契约测试对账；不维护两份手工分叉逻辑。

## 4. 容量与性能边界

- 当前 Go/Python MySQL 默认池为 `max_open=32`、`max_idle=8`。14 个 DSN 直接乘策划电脑数会先耗尽中心 `max_connections`；`central-planner` 必须单独生成较小池，初始建议 `max_open=4`、`max_idle=1`，最终以多机压测决定。
- 每台约十个 schema、近百张表；规划电脑数必须换算数据库/表数、连接总量、table cache、metadata lock、备份时长、IOPS 与 buffer pool，不能只验证“能连”。
- 中心地址使用稳定 DNS 和可信 CA；不要把数据库服务器裸 IP写成证书身份。网络 ACL/VPN 是第二层，不能替代 TLS 和账号隔离。
- 中心 MySQL 能省掉 248.69 MiB SVN 下载、本地近 1 GiB 解包、首次初始化和本地 mysqld 启动；它不替代 listener 查询性能修复，后者仍需独立保留和验证。

## 5. 验收标准

1. 1000 次并发登记无 workspace/database/account 碰撞；相同设备重试返回同一 workspace。
2. 改 IP、Wi-Fi/VPN、用户名显示、SVN 路径或重启后仍命中原 workspace；MachineGuid/SID 摘要不同的克隆身份被检测并阻断复用。位级 OS clone 若连 MachineGuid、SID 和可解密 DPAPI 状态一并复制，无 TPM/终端证明时客户端无法单独识别，必须由中心监控重复使用补强。
3. 十个 canonical migration set 精确映射十个物理库；任一迁移失败不进入 READY，且任意排队副本都不能自动越过 `MIGRATION_FAILED`；管理员显式 `retry-workspace` CAS 后幂等续跑，最终全部 clean。
4. A runtime 账号对 B workspace 的 SELECT/INSERT/ALTER 均为 `Access denied`，客户端无法取得 provisioner/migration 凭据。
5. Git、SVN、静态 YAML、日志和进程参数中均无中心密码/token；临时运行 YAML ACL 收紧且进程读入后删除，profile fingerprint/state 不包含秘密。
6. Go 与 Python 使用同一生成 profile；14 条 DSN 的 endpoint、TLS、凭据版本和十库映射完全一致。
7. `dbcheck`、迁移器和 dev 工具按 exact workspace mapping 检查，不能扫描别人库后误 PASS/误 FAIL。
8. 中心 profile 在规划最大电脑数下验证总连接、活跃连接、延迟、metadata lock、备份与恢复余量。
9. 启动前断网、事务中断网、恢复网络、中心重启、凭据轮换、重复双击均有界、可见、无串库、无本地自动 fallback。
10. 完整新机双击实测覆盖启动、登录、选角、进 Hub、进战斗、结算、背包、社交和任务；未完成该 E2E 前不得删除本地 profile 或 MySQL 包。
