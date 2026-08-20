# 策划中心 MySQL provisioner

> 状态：2026-08-20 代码已落地，隔离 Oracle MySQL 8.4 TLS 集成已验证 fresh 二十库迁移与账号隔离；尚未部署到中心机，新电脑双击与玩家 E2E 仍是发布前硬门禁。总决策见
> [`decision-revisit-planner-central-mysql.md`](./decision-revisit-planner-central-mysql.md)。

## 1. 交付边界

独立 Go module：`tools/plannerdb-provisioner`。它只部署在中心机，提供一个 HTTPS seam 和四个管理命令：

- `pandora-plannerdb-provisioner issue-token`：管理端签发 32-byte base64url 一次性 enrollment token；stdout 只输出一次明文，registry 只保存 SHA-256。
- `issue-recovery-token -workspace-id <id>`：管理员显式授权恢复已有 workspace；stdout 只出现一次可复制的 `<workspace_id>.<43-char-raw-token>` recovery code，registry 仍只按 raw token 保存 hash，并以 `RECOVERY + target_workspace_id` 绑定，不生成新库、账号或密码。
- `retry-workspace -workspace-id <id>`：只做 `MIGRATION_FAILED → PROVISIONING` 的单条 CAS，stdout 只有 workspace/state，不启动 DDL。
- `audit`：只读报告卡住/失败/不活跃 workspace 和待清理 token hash，默认不删除任何 workspace/schema。
- `POST /v1/enroll`：只接受 HTTPS；以 `device_digest` 幂等登记 workspace，十库迁移和权限验收全部通过后才返回 runtime credential。

它不进入策划 SVN、不在策划机运行，也不向策划机下发 admin/migration 凭据。客户端信任锚来自 SVN 预置 CA，API 响应不能替换 CA。

## 2. HTTP 契约

请求（未知字段、明文 HTTP、非 JSON、超过 16 KiB 均拒绝）：

```json
{
  "schema_version": 1,
  "enrollment_token": "<32-byte canonical base64url, no padding>",
  "device_digest": "sha256:<64 lowercase hex>",
  "display_name": "PC02\\planner"
}
```

RECOVERY token 请求必须再带管理员授权的
`"expected_workspace_id":"<26-char-id>"`；服务端同时核对 token target、该 workspace
已登记 device digest 与客户端 expected ID，任一不等均拒绝且不消费 token。
管理员把 `issue-recovery-token` 输出的整段 recovery code 原样交给策划；客户端在内存中按唯一的
`.` 拆分，只把后半段 43 字符 raw token 放入 `enrollment_token`，把前半段放入
`expected_workspace_id`。整段 code、拆分后的 token 都不得写日志或 profile。

首次/处理中立即返回 `202 Accepted`、`Retry-After: 2`，无 credential：

```json
{
  "schema_version": 1,
  "workspace_id": "01arz3ndektsv4rrffq69g5fav",
  "state": "MIGRATING",
  "endpoint": {"host":"pandora-dev-db.intra","port":3306,"tls_server_name":"pandora-dev-db.intra"},
  "databases": {"pandora_account":"pandora_account_w_01arz3ndektsv4rrffq69g5fav"},
  "retry_after_ms": 2000
}
```

仅 `READY` 返回 credential：

```json
{
  "schema_version": 1,
  "workspace_id": "01arz3ndektsv4rrffq69g5fav",
  "state": "READY",
  "endpoint": {
    "host": "pandora-dev-db.intra",
    "port": 3306,
    "tls_server_name": "pandora-dev-db.intra"
  },
  "databases": {
    "pandora_account": "pandora_account_w_01arz3ndektsv4rrffq69g5fav"
  },
  "credential": {
    "username": "p_app_01arz3ndektsv4rrffq69g5fav",
    "password": "<43-char base64url>",
    "version": 1
  }
}
```

真实响应的 `databases` 必须精确含十个 canonical migration set。所有错误只返回稳定 code 与安全文案，不回显 token、runtime/migration password 或 admin DSN；响应统一 `Cache-Control: no-store`。

同一已消费 token + 同 device digest 在响应丢失后可以重复调用并返回同一 workspace/credential；同 token + 另一 digest 固定返回 401。未消费的新普通 token 遇到既有 device digest 返回 `409 DEVICE_ALREADY_ENROLLED`，事务回滚且 token 保持未消费；这使克隆机无法自动领取旧库密码。只有显式 RECOVERY token 可恢复旧 workspace，且永不改 workspace/账号/密文。

`MIGRATION_FAILED` 是真终态：`/v1/enroll` 返回 `409`、state 和十库映射，不带 credential/
`Retry-After`，也不自动再跑 DDL。中心管理员修复根因后必须显式执行
`retry-workspace`；之后客户端用原 token 或 RECOVERY token 轮询才启动 worker。

## 3. 原子边界与恢复

registry 位于独立 `pandora_planner_registry`：

- token 行 `FOR UPDATE`、token 消费、`device_digest` 唯一登记和加密凭据落库处于同一 MySQL 事务；同 token 并发重试收敛到一行，不同普通 token 碰到既有 device 则 fail-closed。
- `workspace_id` 由 128-bit CSPRNG 生成并复用 `tools/migrate/workspacedb` 做唯一格式校验；数据库唯一键是第二道碰撞闸，碰撞事务回滚后最多重试三次。
- runtime/migration password 在登记事务前生成，用 32-byte master key 的 AES-256-GCM 加密；AAD 固定绑定 workspace、账号角色和 credential version。响应丢失后从密文恢复原凭据，不能重新生成第二套。
- `/v1/enroll` 先取 64 并发槽，饱和在读 body/访问 registry 前返回 `503 + Retry-After`；单请求总 deadline 10 秒。这与默认两个 workspace DDL worker 槽分离。
- 外层以 MySQL `GET_LOCK(pandora_planner:<workspace_id>)` 做跨副本 single-flight；worker 槽饱和时本次仍返回 `202`，后续轮询再排队，不产生无界 goroutine。
- 十库 DDL 不伪装成分布式事务。状态为 `PROVISIONING → MIGRATING → READY`；迁移失败落真终态 `MIGRATION_FAILED`，只能管理 CAS 后幂等续跑。
- provisioning worker 从 service lifecycle 的干净 root context 派生独立硬 deadline，不继承 HTTP 请求 Value/取消；优雅停止会取消 worker、终止整棵迁移子进程并等待 secret bundle 清理。

## 4. MySQL 权限

两个 workspace 账号精确为 `p_app_<26-id>` / `p_mig_<26-id>`（均 32 字符），并且只允许 `'username'@'%' REQUIRE SSL`；身份不绑定 DHCP/VPN IP，网络 ACL 是额外防线。

- runtime：十库 `SELECT, INSERT, UPDATE, DELETE`；仅 `pandora_player` 暂加 `CREATE, ALTER, INDEX, REFERENCES`，用于现有 `data_service SyncAllTables`。该例外移入 migration 后必须收回。
- migration：十库 `SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, INDEX, REFERENCES`。MySQL 的 `DROP ON db.*` 是数据库级权限，既覆盖内嵌 up 的 `DROP TABLE`/`RENAME TABLE`，也允许该中心账号删除自己获 grant 的十个 workspace 库；这是迁移所需权限带来的明确 blast radius，不能伪称只允许删表。凭据只留在中心受管迁移任务；账号没有其它 workspace/库或全局 `DROP`，也没有 `GRANT OPTION`、`CREATE USER`、`SUPER`、`FILE`。

每次重试先 `REVOKE ALL PRIVILEGES, GRANT OPTION`，再按 exact 十库重授；完成后从 `information_schema`、`mysql.user/role_edges`、`mysql.procs_priv` 和 `mysql.proxies_priv` 验证：无全局/表/列/例程/role/proxy 权限、无跨 workspace schema、无可转授权权限、账号强制 SSL。同一 username 在 `mysql.user` 恰好只能有 `Host='%'` 一行；发现 `localhost`/`10.%` 等更具体 Host 时 fail-closed，不自动删除未知账号。任何不一致都不得进入 `MIGRATING`。

中心 provisioner 自己必须使用独立、非 root 的 DBA 账号。它需要创建库/账号和向上述权限做转授权，因此权限强、只允许中心进程持有；由 DBA 在仓库外建立并限制来源主机。其 DSN 永远只能通过 ACL 文件或 `PANDORA_*` 环境引用提供。

## 5. 同版本迁移器与 secret bundle

server 启动时读取 `pandora-migrate(.exe)`，按同一 release manifest 的小写 SHA-256 校验后把内容固定在内存；每次 enrollment 把该固定 snapshot、十个 DSN、targets JSON 和 MySQL CA 写入 OS 私有临时目录。命令行只含路径、workspace ID 和独立审核的 exact inventory，密码不进参数或父进程环境。

子进程只继承 SystemRoot/PATH/TEMP 等白名单环境。Windows 使用 `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`，Unix 使用独立 process group；外层取消会终止迁移器及其逐库 worker。退出后必须删除 secret bundle，删除失败也按迁移失败处理、拒绝 `READY`。

## 6. Registry 容量与只读 audit

`planner_enrollment_tokens` 的未消费过期 hash 和已消费超过 30 天 hash，在每次管理签发 token 时物理清理；清理 SQL 失败会使签发失败，不得静默忽略。`planner_workspaces` 是跨重启的身份/密文权威，不根据时间、IP 或离线自动删除；下线 workspace、账号、十库/schema 必须是未来另一个显式管理流程。

registry 是独立 admin trust domain，业务 `tools/migrate/cmd/dbcheck` 不得为了扫描它而取得中心 admin DSN，也不能从单个业务 DSN安全跨库。对应的机械门禁由本模块启动时的 exact shape verifier 与 `audit` 承担；两张表同时登记在 `CLAUDE.md §9.24`，不是绕过容量清单。

`audit -stuck-after 30m -inactive-after 720h -limit 200` 输出 JSON，统计长时间 `PROVISIONING/MIGRATING`、全部 `MIGRATION_FAILED`、inactive workspace、过期未消费 token 和超 30 天已消费 hash；详细列表有显式上限。卡住/失败/待清理 token 返回码 3，inactive-only 只报告不打红。命令永远只读。启动 shape verifier 精确验证 `(state,updated_at)`、`last_seen_at` 与 `(consumed_at,expires_at)` 扫描索引，索引被误删时拒绝启动。

registry 只记 `display_name`/`last_seen_at`，不采集、不持久化客户端 IP，IP 也不参与 identity 或权限。网络 ACL/VPN 仍是 HTTPS + MySQL TLS + 账号隔离之外的第二层边界。

## 7. Secret 引用与启动

所有秘密 flag 只接受 `env:PANDORA_<UPPERCASE_NAME>` 或 `file:<absolute path>`，inline 值硬拒绝。Unix 文件要求 0600 或更严；Windows DACL 只允许当前服务身份、LocalSystem 和 Administrators 读取。推荐把以下五项分别放在中心机受限目录：

- admin DSN（必须是 exact DNS endpoint、`tls=true`、不预选 database、非 root）；
- MySQL CA PEM；
- 32-byte master key 的 canonical base64url（43 字符，无 padding）；
- HTTPS server certificate PEM；
- HTTPS private key PEM。

示例只展示引用，不含秘密：

```powershell
$bin = 'C:\Program Files\Pandora\plannerdb\pandora-plannerdb-provisioner.exe'
$migrate = 'C:\Program Files\Pandora\plannerdb\pandora-migrate.exe'
$migrateSha = (Get-FileHash -LiteralPath $migrate -Algorithm SHA256).Hash.ToLowerInvariant()

& $bin issue-token `
  -admin-dsn-ref 'file:C:\ProgramData\Pandora\plannerdb\admin.dsn' `
  -mysql-ca-ref 'file:C:\ProgramData\Pandora\plannerdb\mysql-ca.pem' `
  -db-host 'pandora-dev-db.intra' -db-port 3306 `
  -db-tls-server-name 'pandora-dev-db.intra' -ttl 15m

& $bin issue-recovery-token -workspace-id '01arz3ndektsv4rrffq69g5fav' `
  -admin-dsn-ref 'file:C:\ProgramData\Pandora\plannerdb\admin.dsn' `
  -mysql-ca-ref 'file:C:\ProgramData\Pandora\plannerdb\mysql-ca.pem' `
  -db-host 'pandora-dev-db.intra' -db-port 3306 `
  -db-tls-server-name 'pandora-dev-db.intra' -ttl 15m

& $bin retry-workspace -workspace-id '01arz3ndektsv4rrffq69g5fav' `
  -admin-dsn-ref 'file:C:\ProgramData\Pandora\plannerdb\admin.dsn' `
  -mysql-ca-ref 'file:C:\ProgramData\Pandora\plannerdb\mysql-ca.pem' `
  -db-host 'pandora-dev-db.intra' -db-port 3306 `
  -db-tls-server-name 'pandora-dev-db.intra'

& $bin audit -stuck-after 30m -inactive-after 720h -limit 200 `
  -admin-dsn-ref 'file:C:\ProgramData\Pandora\plannerdb\admin.dsn' `
  -mysql-ca-ref 'file:C:\ProgramData\Pandora\plannerdb\mysql-ca.pem' `
  -db-host 'pandora-dev-db.intra' -db-port 3306 `
  -db-tls-server-name 'pandora-dev-db.intra'

& $bin serve `
  -admin-dsn-ref 'file:C:\ProgramData\Pandora\plannerdb\admin.dsn' `
  -mysql-ca-ref 'file:C:\ProgramData\Pandora\plannerdb\mysql-ca.pem' `
  -master-key-ref 'file:C:\ProgramData\Pandora\plannerdb\master-key.b64url' `
  -https-cert-ref 'file:C:\ProgramData\Pandora\plannerdb\https-cert.pem' `
  -https-key-ref 'file:C:\ProgramData\Pandora\plannerdb\https-key.pem' `
  -db-host 'pandora-dev-db.intra' -db-port 3306 `
  -db-tls-server-name 'pandora-dev-db.intra' `
  -migrate-binary $migrate -migrate-sha256 $migrateSha `
  -listen ':9443'
```

## 8. 当前验证与未验证项

模块验证命令：

```powershell
Set-Location tools/plannerdb-provisioner
$env:GOWORK = 'off'
go test -mod=readonly ./...
go vet -mod=readonly ./...
go build -mod=readonly ./...
```

自动测试覆盖 HTTPS `202/200/409` 契约、READY 前不返凭据、64 请求并发槽与 10 秒 deadline、1000 并发登记、普通 token 克隆拒绝、RECOVERY token 三重绑定、MIGRATION_FAILED 不自动重试、两个副本串行取锁后不能绕过终态、显式 retry CAS、token 只存 hash/purpose/target、audit 门禁、secret 引用、exact 十库 bundle、子进程环境白名单、Windows Job 进程树和 bundle 清理。

另有显式隔离的 Oracle MySQL 8.4 TLS 集成测试（默认 skip，需三个路径/DSN 环境引用与 destructive ack），已实跑两个 fresh workspace 的二十库迁移，覆盖内嵌 `DROP/RENAME`、A runtime 账号连 B 库拒绝、A migration 账号只在自己的十库可见数据库级 DROP 且 `DROP DATABASE B` 被拒绝、TLS/REQUIRE SSL、Host 歧义拒绝、新 token + 旧 digest 不消费、recovery 恢复原密码、16 并发 retry CAS 仅一个成功和索引 mutant 拒启。

仍未验证且不能包装成完成：中心机实际部署账号/ACL、多副本与进程崩溃故障注入、网络分区/证书轮换、备份恢复容量、策划客户端新机及登录到结算的完整 E2E。
