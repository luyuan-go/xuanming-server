# Pandora 策划中心 MySQL Provisioner

本模块只运行在受管中心机，为策划工作区创建并迁移隔离的十个 MySQL 数据库。它不部署到策划电脑，也不替代客户端一键启动脚本。完整决策与运维契约见：

- [`../../docs/design/decision-revisit-planner-central-mysql.md`](../../docs/design/decision-revisit-planner-central-mysql.md)
- [`../../docs/design/planner-central-mysql-provisioner.md`](../../docs/design/planner-central-mysql-provisioner.md)

## 外部 seam

服务提供一个只接受 HTTPS 的 `POST /v1/enroll`，以及四个中心管理命令：

- `issue-token`：stdout 只输出一次 43 字符普通 enrollment token；registry 只存 hash。
- `issue-recovery-token -workspace-id <id>`：stdout 只输出一次 `<workspace_id>.<43-char-raw-token>`。客户端在内存拆分为 API 的 raw token 与 `expected_workspace_id`；registry 仍只 hash raw token。
- `retry-workspace -workspace-id <id>`：只做 `MIGRATION_FAILED → PROVISIONING` 的 CAS，不改账号、密文或 workspace，也不直接运行 DDL。
- `audit`：只读报告 stale/failed/inactive workspace 和过期 token hash；默认不删除 workspace、账号或 schema。

`PROVISIONING/MIGRATING` 返回 `202 + Retry-After` 且没有 credential；只有 `READY` 返回 runtime credential。`MIGRATION_FAILED` 是真终态，返回 `409` 且不自动重试，必须先由管理员执行 `retry-workspace`。

普通新 token 遇到已经登记的 device digest 会 fail-closed；只有显式绑定 exact workspace 的 recovery token 可以恢复。registry 不采集、不持久化客户端 IP，IP 也不参与身份或权限。

## 数据库与安全边界

workspace ID 是 128-bit CSPRNG 生成的 26 位小写 canonical Crockford ID。十个物理库名统一由 `tools/migrate/workspacedb` 生成：`<migration_set>_w_<workspace_id>`。

- runtime 用户：`p_app_<workspace_id>`，只限本 workspace 十库的运行期最小权限。
- migration 用户：`p_mig_<workspace_id>`，只限本 workspace 十库的 `SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, INDEX, REFERENCES`。MySQL 的 `DROP ON db.*` 也允许删除获 grant 的本 workspace 库，这是内嵌 `DROP TABLE`/`RENAME TABLE` 必要权限的 blast radius；账号不能 DROP 其它 workspace/库，也没有全局 DROP。
- 两类用户都只有 `Host='%'` 一条且 `REQUIRE SSL`；发现更具体 Host、全局/跨库/role/proxy/routine/GRANT OPTION 权限即拒绝进入迁移。

admin DSN、MySQL CA、HTTPS cert/key 与 32-byte master key 只接受 `env:PANDORA_*` 或 ACL 受限的绝对 `file:` 引用；不接受 inline secret。token、密码、admin DSN 与 TLS private key 不进入日志、错误正文或子进程参数。

## 本地验证

```powershell
Set-Location tools/plannerdb-provisioner
$env:GOWORK = 'off'
go test -count=1 -mod=readonly ./...
go vet -mod=readonly ./...
go build -mod=readonly ./...
go mod tidy -diff
```

真实 Oracle MySQL 8.4 TLS 集成测试默认跳过，只能使用随机容器名、随机宿主端口和独立临时卷，并显式提供测试 DSN/CA/同版本 `pandora-migrate` 环境引用及 destructive ack。测试结束必须删除本轮容器、卷和临时文件，不得触碰既有 `pandora-mysql*`。

registry 的容量门禁由本模块启动 shape verifier 与 `audit` 承担；业务 `dbcheck` 不取得中心 admin DSN，也不跨独立 `pandora_planner_registry` trust domain。两张 registry 表的保留/豁免边界已登记在 `CLAUDE.md §9.24`。
