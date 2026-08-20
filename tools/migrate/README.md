# Pandora MySQL 版本化迁移器

该工具把 `migrations/<migration_set>` 烘焙进二进制，以 MySQL 中的
`schema_migrations` 作为执行记录。默认迁移模式只执行 `up`，并在每个物理目标完成后强制验收：

- `dirty` 必须为 `false`；
- 数据库版本必须等于当前迁移镜像内该 migration set 的最高版本；
- 数据库版本高于镜像版本时拒绝运行，防止旧镜像覆盖新 schema；
- 任一物理目标失败或超时，进程非零退出，发布必须阻断。

`schema_migrations` 只记录 version/dirty，不保存 SQL checksum。因此迁移文件一旦在任何
共享或生产环境执行，立即视为**永久 immutable**：不得改写/删除/重编号已有
`000001/000002/...`，任何修正只能新增更高版本。评审/CI 必须相对上一个已发布 tag 检查这条；
否则数据库会显示 clean version，但实际执行内容可能因环境而分叉。

`data_service` 的 `player_data` 仍由 proto2mysql 独占管理，不放进本工具。

## 目标与凭据

必须通过 `-targets-file`（或 `MIGRATE_TARGETS_FILE`）显式给目标清单。默认没有隐式
“迁移全部库”，避免拿错环境变量后扫到错误实例。清单本身不含凭据，只引用清单所在目录
或其子目录下的 DSN 文件（拒绝 `..`/目录外绝对路径）；参考
[targets.example.json](targets.example.json)。

还必须由发布侧通过 `-expected-targets`（或 `MIGRATE_EXPECTED_TARGETS`）提供一份**独立审核**
的逗号分隔 `name:migration_set:database` inventory。runner 要求它与 Secret 内 targets 的
完整 triple 集合 exact-match；缺一个库/auction shard、同名目标换了 migration set/物理库、
Secret 多一个未审核目标、名字重复都会在读取 DSN 前失败。例如：

```text
-expected-targets=account-primary:pandora_account:pandora_account,player-primary:pandora_player:pandora_player,social-primary:pandora_social:pandora_social,battle-primary:pandora_battle:pandora_battle,trade-primary:pandora_trade:pandora_trade,auction-shard-00:pandora_auction:pandora_auction_00,auction-shard-01:pandora_auction:pandora_auction_01,leaderboard-primary:pandora_leaderboard:pandora_leaderboard,owner-primary:pandora_owner:pandora_owner,bag-primary:pandora_bag:pandora_bag,mission-primary:pandora_mission:pandora_mission
```

生产发布必须从实际分片配置生成/复核这份 inventory，不能照抄示例 descriptor。
此外 runner 只允许 `database == migration_set`，或分片库使用
`database` 以 `migration_set + "_"` 开头；拍卖 migration set 无法误写进账号库。

一个 DSN 文件只放一行完整 DSN，例如：

```text
pandora_migrator:<由密钥管理系统注入>@tcp(mysql.example.internal:3306)/pandora_account?tls=true
```

生产 `pandora_owner` 目标以及 `pandora_social` 将来切到 TiDB 作为迁移目标时，DSN 还必须带
`tidb_skip_isolation_level_check=1`。原因是 `golang-migrate` 的 MySQL driver 会请求
`SERIALIZABLE` 事务，而 TiDB 需要显式允许把该请求降级到其支持的隔离语义；缺少此参数会在
执行第一条迁移前 fail-closed。生产示例形态为
`...?tls=true&tidb_skip_isolation_level_check=1`，不能因此放宽下方 TLS 要求。

运行器会校验 DSN 中的 database 与清单完全一致，并强制：

- 连接超时 10 秒；
- 每目标有限的读写/进程硬超时（默认 900 秒，允许 30～3600 秒；SQL 超时比进程硬时限早 5 秒）；
- 每连接 `lock_wait_timeout` 与 `innodb_lock_wait_timeout`（默认 15 秒，允许 1～60 秒）；
- 多目标不能指向同一 `network/address/database`。

`golang-migrate` MySQL driver 的跨 Job advisory `GET_LOCK` 自带固定 10 秒服务端等待；runner
把外层 lock wrapper 保持在至少 11 秒，避免外层先超时后遗留占连接 goroutine。即使网络层
异常，逐目标 worker 的硬时限仍会终止整个子进程并阻断发布。

`production`、`prod`、`staging` 等非开发环境还会 fail-closed 强制 `tls=true`，拒绝未配置
TLS、`tls=false`、`tls=preferred`、`tls=skip-verify` 和
`allowFallbackToPlaintext=true`。普通生产目标只使用系统信任根；workspace 目标的独立 CA
规则见下方专节。两条路径都不允许 `skip-verify` 或明文 fallback。只有非 workspace 的
`local/dev/development` 本机测试目标才允许明文。

生产迁移账号按物理库授权，不使用 root，也不复用业务账号。当前 `up` 所需权限为：

```sql
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, REFERENCES
ON `<目标库>`.* TO 'pandora_migrator'@'<迁移 Job 来源>';
```

其中 `INSERT/DELETE/CREATE` 也用于 `schema_migrations` 自身。不要授予全局权限、
`CREATE USER`、`GRANT OPTION`、`TRIGGER`、`SUPER` 或 `DROP DATABASE`。

账号创建、密码和上述授权由 DBA/Secret 管理器在仓库外完成；不要把 GRANT 的真实主机、
密码或 DSN 提交进仓库。

真实 `targets.json` 与 `*.dsn` bundle 应由 Secret 管理器直接生成到仓库外，或仅放进已忽略的
`run/` 临时目录；不要放在 `tools/migrate/`。根 `.gitignore`、`.dockerignore` 与迁移
Dockerfile 的精确 `COPY` 共同防止这些文件进入 git、BuildKit context/中间层。

## 多拍卖分片

每个拍卖物理分片单列一个 target，复用 `pandora_auction` migration set：

```json
{
  "targets": [
    {
      "name": "auction-shard-00",
      "migration_set": "pandora_auction",
      "database": "pandora_auction_00",
      "dsn_file": "auction-00.dsn"
    },
    {
      "name": "auction-shard-01",
      "migration_set": "pandora_auction",
      "database": "pandora_auction_01",
      "dsn_file": "auction-01.dsn"
    }
  ]
}
```

同一清单可同时列 account/player 等普通库和所有 auction shards。执行顺序就是清单顺序，
任一分片失败后不会继续迁移后面的目标。

多库之间不存在分布式事务：若前两个目标成功、第三个失败，前两个会保留已完成的 expand
迁移。修复阻断后用同一或更高版本镜像重跑，已完成目标会验版本后跳过；不要自动 down/force，
也不要在清单未全绿前滚动业务版本。

## 中心开发 MySQL 的 workspace 库

策划机使用中心开发 MySQL 时，每个 canonical migration set 映射到该 workspace 的独立
物理库：

```text
<migration_set>_w_<workspace_id>
```

`workspace_id` 只能由中心 provisioner 分配，是 canonical 128-bit 的 26 位小写 Crockford
Base32（`^[0-7][0-9a-hjkmnp-tv-z]{25}$`）。用户名、电脑名、IP、MachineGuid 和 SID 都不能充当
数据库身份；它们最多是 enrollment / UI 的显示提示。唯一生成与解析实现位于
`workspacedb` 包，物理库名还会强制满足 MySQL 64 字符标识符上限。

workspace 清单必须额外传 `-workspace-id`（或 `MIGRATE_WORKSPACE_ID`）。迁移器会在读取
DSN、连接数据库前逐 target 断言：

- `database` 精确等于该 target 的 `<migration_set>_w_<workspace_id>`；
- 同一批次不能混入 canonical 库、历史分片或另一个 workspace；
- `_w_` 是保留命名空间，短 ID、大写、歧义字符 `i/l/o/u`、额外后缀都拒绝；
- 每个 workspace target 必须提供绝对路径 `tls_ca_file`，且路径必须存在并指向普通文件；
- 输入 DSN 仍只能写 `tls=true`，不能把 CA 路径、`skip-verify` 或自定义 TLS 注册名塞进 query；
- 迁移器从空的 `x509.NewCertPool` 起步，只装载严格解析的 workspace bundle CA PEM，
  不继承系统根；再以 DSN 的 TCP host 作为 `ServerName`，因此系统信任但非 bundle CA
  签发的同名证书也必须拒绝，endpoint host 必须与证书 SAN / runtime profile 的
  `tls_server_name` 完全一致；TLS 最低版本固定为 1.2；
- workspace 目标即使误传 `-environment=dev` 也始终执行上述 TLS 校验；
- 父进程启动的逐库 worker 必须继续携带同一个 workspace guard。

单个 target 示例（其余 canonical migration set 同样映射）：

```json
{
  "name": "account-workspace",
  "migration_set": "pandora_account",
  "database": "pandora_account_w_01arz3ndektsv4rrffq69g5fav",
  "dsn_file": "account.dsn",
  "tls_ca_file": "C:\\ProgramData\\Pandora\\certs\\planner-mysql-ca.pem"
}
```

调用时还须传同一个 `-workspace-id=01arz3ndektsv4rrffq69g5fav`；`account.dsn` 只写
`...?tls=true`，不携带 CA 路径。

`-workspace-id` 与 `-bootstrap` 明确互斥。策划机不得持有 root/admin DSN，也不得获得
`CREATE DATABASE`、`CREATE USER` 或 `GRANT OPTION`。自动建库、创建/轮换每 workspace
账号和最小授权必须由中心机上的独立 provisioner 在鉴权后幂等完成；本迁移器只用已分配的
非管理员迁移凭据执行 schema migration。当前仓库只提供上述命名/校验 seam，不伪装成已完成
的 enrollment、认证或 Secret 下发服务。

### 客户端只读预检（verify-only）

中心 provisioner 已完成建库、授权和迁移后，策划机首次启动使用 `-verify-only=true` 做
只读验收。也可用 `MIGRATE_VERIFY_ONLY=true` 提供默认值，但命令行
`-verify-only=true|false` 始终覆盖环境变量；父进程会把明确的布尔值继续传给每个 worker，
不会因子进程继承了另一份环境变量而换模式。

verify-only 仍完整执行 targets JSON、独立 `-expected-targets` inventory、workspace guard、
DSN、绝对 CA 文件和 TLS 配置预检，然后让每个 target 进入原有独立 worker/硬超时。连接并
`Ping` 后只执行四条只读查询：

```powershell
pandora-migrate.exe -targets-file=<bundle>/targets.json `
  -expected-targets=<独立审核 inventory> `
  -workspace-id=<中心分配 ID> -verify-only=true
```

```sql
SELECT DATABASE();
SELECT CURRENT_USER();
SHOW SESSION STATUS LIKE 'Ssl_cipher';
SELECT `version`, `dirty` FROM `schema_migrations`;
```

只有当前 database 精确匹配、`CURRENT_USER()` 中 `@host` 之前的认证用户名与 DSN user
精确一致、TLS cipher 非空、`schema_migrations` 恰好一行且 `version ==` 二进制内嵌
latest、`dirty=false` 才通过。客户端不限定授权 host 必须为 `%`，其作用域由中心
provisioner 验收。错库、错账号、明文/TLS 握手失败、表不存在、空表、多行、旧/新 version
或 dirty 都非零退出。

该路径与 `-bootstrap` 互斥，也不会构造 `golang-migrate` MySQL driver（该 driver 可能
ensure version table），不会拿 `GET_LOCK`，不会执行 DDL/DML、quarantine 或 `SetVersion`。
verify 专用 DSN 还会清空会触发连接初始化 `SET` 的 session Params，并关闭
`multiStatements`；迁移模式原有的有限锁等待参数不受影响。
因此 verify-only **不能**替代中心 provisioner 或正常迁移；它只证明策划机拿到的现有
workspace 可以安全使用。

## bootstrap 安全边界

`bootstrap` 默认且生产固定为 `false`。只有本地开发显式满足以下全部条件才会
`CREATE DATABASE` 与 `GRANT`：

1. 参数 `-bootstrap=true`；
2. `-environment=local|dev|development`；
3. 每个 target 额外提供 `bootstrap_admin_dsn_file`；
4. admin DSN 不带 database，且与迁移 DSN 的 network/address 完全一致。

bootstrap 不创建账号、不设置密码，只给已存在的 dev 迁移账号授予上面的最小权限。
`production`、`prod`、`staging`、`test` 均不能启用 bootstrap。

## 构建与本地验证

```powershell
cd tools/migrate
$env:GOWORK = 'off'
go test -mod=readonly ./...
go vet -mod=readonly ./...
go build -mod=readonly ./...

cd ../..
go test -mod=readonly ./tools/migrate/...

docker build -f deploy/migrate/Dockerfile `
  -t pandora/migrate:<immutable-tag> .
```

容器内构建固定使用 `golang:1.26.5-bookworm`，并先断言基础镜像内的
`go env GOVERSION` 与 `GO_VERSION` 完全一致。镜像 tag 标错或本地缓存陈旧时直接失败，
不会静默用旧工具链出包。受限内网可沿用业务镜像的宿主
交叉编译路线：

```powershell
$env:GOWORK = 'off'
$env:GOOS = 'linux'
$env:GOARCH = 'amd64'
$env:CGO_ENABLED = '0'
Push-Location tools/migrate
go build -mod=readonly -trimpath -ldflags='-s -w' `
  -o ../../run/docker-build/prebuilt/migrate/pandora-migrate .
Pop-Location

docker build -f deploy/migrate/Dockerfile.prebuilt `
  -t pandora/migrate:<immutable-tag> run/docker-build/prebuilt/migrate
```

独立模块已纳入根 `go.work`，同时保留自身 `go.sum`，因此 workspace 与
`GOWORK=off -mod=readonly` 两条构建路径都可复现。

## Kubernetes 发布门禁

[job.yaml](../../deploy/k8s/migrate/job.yaml) 是一次性 Job 模板，刻意不加入普通
services/online kustomization，防止日常 apply 意外重跑。它只引用预存在的
`pandora-db-migrate` Secret，不创建 Secret，也不挂业务 `pandora-config`。

发布顺序：

1. 外部 Secret 管理器准备 `pandora-db-migrate`，其中含 `targets.json` 与全部 DSN 文件；
   发布侧根据实际数据库/auction shard inventory 独立填写 Job 的
   `name:migration_set:database` triples；
2. 构建并推送与发布版本同源的迁移镜像，使用 digest 或不可变 tag；
3. 把 Job 的 `release-id` 和镜像占位替换为本次发布值，以 `kubectl create` 创建；
4. 等待 Job `Complete`，同时确认每个目标日志均有 `version=N dirty=false`；
5. Job 失败、超时、TLS 不安全、或相对 `-expected-targets` 有目标遗漏时停止发布，不滚动业务 Deployment，不自动 force dirty；
6. 全部通过后再滚动业务服务。

此仓库不保存真实 Secret，也不代替 DBA 创建生产账号/数据库；本模板不会被自动 apply。

## dbcheck:数据库无界增长检查(§9.24 发布门禁 + 压测断言)

`cmd/dbcheck` 内嵌与 `CLAUDE.md §9.24` 同步的全库表登记清单,对真实 MySQL 枚举全部
`pandora_%` 库的表并断言:

- **无未登记表**(新表必须先登记 §9.24 + 同步本工具清单,否则 FAIL——这是"无清理方案的
  只增表一律拒"的机械化);
- **swept 表清理索引齐备**(缺失 → 提示跑对应 `*_retention_indexes` 迁移);
- **outbox 类表无堆积**(默认 ≤200 行,`-outbox-max` 可调)。

```powershell
# 上线前发布门禁(exit 0 = PASS)
go run ./cmd/dbcheck -dsn 'user:pwd@tcp(host:port)/'

# 压测:压前基线 → 压后对比(增量必须能用业务量解释)
go run ./cmd/dbcheck -dsn '...' -exact -snapshot db-before.json
go run ./cmd/dbcheck -dsn '...' -exact -compare db-before.json

# 待清理量报告(只 COUNT,不删任何数据)
go run ./cmd/dbcheck -dsn '...' -pending
```

**本工具永不 DELETE**(2026-07-22 用户指令:不能因为数据大了就删数据;旧 `-force-sweep`
已整块移除且刻意不留同名 flag)。真删只由各服务配置 `retention_mode: delete` 决定,
默认 `report_only` 只 WARN 告警(见 `pkg/dbguard/sweep.go`)。
`-pending` 的 WHERE 与各服务 sweep 同构;`player_mail`(归档分流)、`bag_journal`
(checkpoint 条件)、`battles`/进度组(多表事务)条件复杂,由各自服务自行报告待清理量。
压测接线细则见 `docs/design/stress-discipline.md` §4.1.1 / §4.3。
