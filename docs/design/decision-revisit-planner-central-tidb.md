# 决策复议：策划中心数据库由 Oracle MySQL 改为远端 TiDB

> 状态：**待拍板，当前阻断，不得把 TiDB endpoint 直接填入现有中心 MySQL bundle，2026-08-20**。
> 本复议只涉及 `策划一键启动-免Docker-测试版.cmd` 的本地链路；K8s 清单、集群运行态和
> 线上 TiDB 均不在本次改动范围内。

## 1. 先统一术语

这里有两个彼此独立的维度，不能再混写：

- **数据库生命周期模式**：`local-owned` 表示本工作区拥有并管理本机 mysqld；
  `central-managed` 表示策划机对 SQL 服务零生命周期动作，只消费远端 workspace。
- **数据库引擎**：`oracle-mysql` 与 `tidb` 是后端实现。`central-managed` 只说明“远端受管”，
  不等于 TiDB。
- **策划 workspace**：一个稳定 `workspace_id` 对应十个隔离的物理 database/schema 映射，
  不是“一个用户只有一个 schema”。

现有已拍板的
[`decision-revisit-planner-central-mysql.md`](./decision-revisit-planner-central-mysql.md)
是 `central-managed + oracle-mysql`。它的 PowerShell 客户端能力已经接线，但中心服务、endpoint、
CA、登记码和真实新机 E2E 尚未部署完成。

## 2. 新目标与不变边界

用户目标是：

- 策划机只启动本机 Redis、Kafka、Envoy 和业务程序；不下载、启动、停止或重置本机 MySQL/TiDB。
- SQL 连接远端独立 TiDB，每个策划 workspace 仍使用独立十库映射。
- 只改策划免 Docker 一键链路，K8s 完全不动。
- 从未登记远端且 bundle 未部署时允许 `local-owned`；一旦选择/登记远端，配置或连接失败禁止回落。

策划 CMD 会先检查 `installers/planner-db/central-mysql.json`：存在时以
`PANDORA_PLANNER_REQUIRE_CENTRAL_MYSQL=1` 锁定 central，防止检查后文件消失造成回落；不存在时
由 central applied state、runtime profile 与 workspace identity 共同判定是否仍是从未登记的
`local-owned`。完整规则见
[`decision-revisit-planner-database-fallback.md`](./decision-revisit-planner-database-fallback.md)。
这个模式选择仍不意味着 TiDB 已经可用。

## 3. 为什么不能只把 endpoint 换成 TiDB

现有中心 provisioner 的契约和实现明确绑定 Oracle MySQL：

1. `tools/plannerdb-provisioner/internal/plannerdb/mysql_backend.go` 会读取 `VERSION()`，发现
   `tidb` 时明确拒绝；删除这条判断只会把失败推迟，不会形成兼容实现。
2. 同一 backend 的登记、权限和验收依赖 Oracle MySQL 元数据与行为，包括 InnoDB、
   `TABLE_CONSTRAINTS.ENFORCED`、`mysql.user`/角色/过程权限表，以及
   `GET_LOCK`/`IS_USED_LOCK`/`RELEASE_LOCK` workspace 锁。
3. provisioner 仍使用 `sql.LevelSerializable`；TiDB 的隔离级别集合与 Oracle MySQL 不同，
   不能靠“跳过隔离级别检查”把实际事务语义差异消失。
4. `migration_exec.go` 生成的迁移 DSN 目前只处理 TLS，没有加入
   `tools/migrate/README.md` 对 TiDB 明确要求的
   `tidb_skip_isolation_level_check=1`。即使移除版本拒绝，迁移也不能宣称可用。
5. `tools/scripts/run_services.ps1` 的 NoDocker 路线仍以 `-SocialOnMysql` 选择 MySQL 源配置；
   friend/chat/guild/mail 的 TiDB 配置还包含不同 collation/后端断言。只改 host/port 会渲染错误档。
6. `PROGRESS.md` 已记录真 TiDB 上仍未修的事务正确性 blocker：部分流程在悲观事务中先加锁、
   再用普通快照读，或依赖空范围锁/gap lock。涉及 inventory、player、mail 等路径；在这些问题
   完成真 TiDB 并发回归前，不能把十库整体迁移冒充为“协议兼容即可”。

因此，“MySQL wire protocol 可连”只证明驱动能握手，不证明 migration、权限、隔离、锁和业务事务等价。

## 4. 可选方案

### A. 先部署现有中心 Oracle MySQL（推荐的立即方案）

部署已实现的 provisioner 和 Oracle MySQL 8.4，向策划发布 endpoint + 公开 CA bundle，并发放一次性
enrollment code。它立即满足“策划机不启本机 MySQL”，不需要改 K8s；缺点是远端引擎暂时不是 TiDB。

### B. 完整实现中心 TiDB adapter（需用户拍板后另行实施）

不能复用一个含糊的 `central-managed` 布尔值硬猜引擎，至少需要：

1. 在 bundle、登记响应和 runtime profile 增加 exact `backend_kind=tidb`，并纳入 fingerprint；
   未知值 fail-closed，禁止自动探测后静默换行为。
2. 为 provisioner 实现 TiDB 专用 backend：版本下限、事务隔离、workspace fencing/锁、账号权限、
   schema/索引/约束验收均使用 TiDB 支持且被真实集群验证的语义。
3. 迁移器生成 TiDB DSN，并对十个 migration set 做 fresh、升级、dirty、重试和部分成功恢复矩阵。
4. runtime renderer 按 `backend_kind` 选择正确的 TiDB 源配置、collation 与 `require_tidb` 断言；
   Go/Python 两栈必须消费同一 profile，不能各自猜 endpoint。
5. 修复已知 TiDB 事务 blocker，并对同玩家并发、空集合首写、重试、进程崩溃和 unknown outcome
   做真实 TiDB 回归。
6. 做至少两台策划机、两个 workspace 的串库负向测试，以及登录、Hub、战斗、结算、背包、社交、
   邮件和任务玩家 E2E。

上述工作仍可保持 K8s 零 diff；它改变的是策划中心数据库 provisioner 与本地 runtime renderer。

## 5. 验收与回滚

- central 已被选择或存在历史证据后，缺 bundle、CA、登记码、DNS/TCP/TLS/认证或 workspace READY
  任一条件时在启动业务进程前有界失败；本机 mysqld PID 集合前后不变。
- `central-managed + tidb` 的 provision/start/stop/reset 计划均不包含任何本机 SQL 组件。
- 十库 mapping、credential version、CA hash、endpoint 和 backend kind 进入同一 profile fingerprint；
  漂移时整批刷新，禁止新旧 DSN 进程混跑。
- A workspace 凭据不能访问 B workspace 任一库；策划机永远拿不到 provision/migration 权限。
- 真 TiDB 十库迁移、权限、事务并发、断网重试和完整玩家 E2E 全绿后，才可把状态改为“已拍板/已接线”。
- 回滚只切回一份已验证的 `central-managed + oracle-mysql` profile 并整批重启业务；不得回落
  `local-owned`，不得删除或清空远端 workspace。

在用户拍板方案 B 前，本仓不移除 provisioner 的 TiDB 拒绝，也不把 TiDB endpoint 写进
`central-mysql.json` 制造假成功。
