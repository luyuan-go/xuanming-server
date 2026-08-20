# Pandora 本机 dev 数据库结构自动升级(local 模式一键启动的一环)
#
# 解决的问题:deploy/mysql-init/*.sql 只在 **MySQL 数据卷第一次创建** 时由容器 entrypoint
# 执行。卷一旦建好,后续新增的结构改动(tools/migrate/migrations/<set>/0000N_*.up.sql)
# 就再也不会自动进库 —— 于是「一键启动」拉起的服务连的是旧 schema,表现为启动即崩,
# 例:player 报 `Unknown column 'exp' in 'field list'`(000002_experience 没执行),
# 而人只看到「一键启动失败」,完全不知道要手工补迁移。本脚本把这一步补进启动链。
#
# 做法:直接复用生产同一个迁移器(tools/migrate),它以库内 `schema_migrations` 为准,
# 只执行缺的版本,天然幂等 —— 不自己写一套版本管理,避免与生产语义分叉。
# 各库的 000001_baseline 就是从 deploy/mysql-init/*.sql 生成的(全 CREATE TABLE IF NOT
# EXISTS),所以对「已由 mysql-init 建好、但没有 schema_migrations」的老库也安全:
# baseline 空跑记版本,之后的增量照常补上。
#
# 用法:
#   pwsh tools/scripts/dev_migrate.ps1            # 升级本机 dev MySQL 到最新结构
#   pwsh tools/scripts/dev_migrate.ps1 -WhatIfOnly # 只列出将要处理的库,不执行
#
# ⚠️ 仅用于本机 dev(127.0.0.1:3307 容器 MySQL,dev 弱口令)。线上迁移走
#    deploy/k8s/migrate/job.yaml + 外部 Secret,与本脚本无关。

[CmdletBinding()]
param(
    # dev MySQL 容器名(docker-compose.dev.yml 的 mysql 服务)
    [string]$Container = 'pandora-mysql',
    # 宿主上 dev MySQL 的地址(docker-compose.dev.yml 发布的端口)
    [string]$MysqlHost = '127.0.0.1',
    [int]$MysqlPort = 3307,
    [string]$MysqlUser = 'pandora',
    # dev 弱口令,与 deploy/env/dev.env / 各 *-dev.yaml 一致;不是密钥。
    [string]$MysqlPassword = 'pandora_dev_pwd',
    # 重放 mysql-init 要建库 + 授权,必须 root(deploy/env/dev.env MYSQL_ROOT_PASSWORD)。
    [string]$MysqlRootPassword = 'pandora_dev_root',
    # 免 Docker 模式(策划机):传本机 mysql.exe 的路径,就不再走 docker exec。
    # 留空 = 保持原有 docker 行为(CLAUDE.md §14.2:开关默认值不改现有行为)。
    # 连接地址 / 账号 / 端口两边完全一致,所以后面的迁移器调用一行都不用分叉。
    [string]$MysqlClient = '',
    # 一键启动已经把 MySQL 当作强依赖；此时连接失败不能再“跳过并返回成功”，否则会让
    # 后续服务拿旧 schema 启动。独立调用保持原有宽松行为。
    [switch]$RequireMysql,
    [switch]$WhatIfOnly
)

$ErrorActionPreference = 'Stop'
$ScriptDir   = $PSScriptRoot
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../..").Path
$MigrationsRoot = Join-Path $ProjectRoot 'tools/migrate/migrations'
. (Join-Path $ScriptDir 'lib/local_infra_state.ps1')
. (Join-Path $ScriptDir 'lib/planner_migrate_fast.ps1')

function Write-MigInfo($m) { Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Write-MigOk($m)   { Write-Host "[ OK ] $m" -ForegroundColor Green }
function Write-MigWarn($m) { Write-Host "[WARN] $m" -ForegroundColor Yellow }

Write-Host "===== 数据库结构升级(dev) =====" -ForegroundColor Cyan

$UseLocalClient = [bool]$MysqlClient
$PlannerFastStart = $UseLocalClient -and ($env:PANDORA_PLANNER_FAST_START -ceq '1')
if ($UseLocalClient -and -not (Test-Path -LiteralPath $MysqlClient)) {
    Write-Host "[ERR] -MysqlClient 指向的文件不存在: $MysqlClient" -ForegroundColor Red
    exit 1
}
if ($UseLocalClient -and $MysqlHost -cne '127.0.0.1') {
    Write-Host "[ERR] -MysqlClient 只允许连接已验证的本机 127.0.0.1；拒绝目标:$MysqlHost" -ForegroundColor Red
    exit 1
}

function Assert-LocalMysqlOwned {
    if (-not $UseLocalClient) { return }
    $state = Get-PandoraLocalInfraPortState $ProjectRoot
    if (-not $state -or [int]$state.MysqlPort -ne $MysqlPort -or
        -not (Get-PandoraLocalMysqlOwnedProcess $ProjectRoot $state)) {
        throw "免 Docker MySQL :$MysqlPort 当前 listener 未通过 PID + exe + my.ini 归属复核；拒绝把建库或迁移 SQL 发给未知实例。"
    }
}

function Invoke-DevMysqlQuery {
    <# 拿库列表。docker 模式走容器内 mysql,免 Docker 模式走本机 mysql.exe。#>
    param([string]$Sql)
    if ($UseLocalClient) {
        Assert-LocalMysqlOwned
        $old = $env:MYSQL_PWD
        try {
            $env:MYSQL_PWD = $MysqlPassword
            return & $MysqlClient '--protocol=TCP' "--host=$MysqlHost" "--port=$MysqlPort" "--user=$MysqlUser" `
                '--batch' '--skip-column-names' '-e' $Sql 2>&1
        } finally { $env:MYSQL_PWD = $old }
    }
    return & docker exec $Container sh -c "MYSQL_PWD='$MysqlPassword' mysql -u'$MysqlUser' --batch --skip-column-names -e '$Sql'" 2>&1
}

function Invoke-DevMysqlScript {
    <# 以 root 身份执行一个 .sql 文件(mysql-init 重放)。#>
    param([string]$Path)
    if ($UseLocalClient) {
        Assert-LocalMysqlOwned
        $old = $env:MYSQL_PWD
        try {
            $env:MYSQL_PWD = $MysqlRootPassword
            return Get-Content -LiteralPath $Path -Raw -Encoding utf8 |
                & $MysqlClient '--protocol=TCP' "--host=$MysqlHost" "--port=$MysqlPort" '--user=root' `
                    '--default-character-set=utf8mb4' 2>&1
        } finally { $env:MYSQL_PWD = $old }
    }
    return Get-Content -LiteralPath $Path -Raw -Encoding utf8 |
        & docker exec -i $Container sh -c "MYSQL_PWD='$MysqlRootPassword' mysql -uroot" 2>&1
}

function Invoke-DevMysqlScriptsBatch {
    <# 策划 fast 未命中收据时，把已排序的 mysql-init 一次性送给同一个 mysql.exe。#>
    param([Parameter(Mandatory)][object[]]$Files)
    if (-not $UseLocalClient -or -not $PlannerFastStart) {
        throw '批量 mysql-init 只允许策划本机 fast 路径调用。'
    }
    Assert-LocalMysqlOwned
    # 全部读成功后才启 mysql；中途文件损坏时不会先执行半批 DDL。
    $sql = Join-PandoraPlannerMysqlInitScripts -Files $Files
    $old = $env:MYSQL_PWD
    try {
        $env:MYSQL_PWD = $MysqlRootPassword
        $output = @($sql | & $MysqlClient '--protocol=TCP' "--host=$MysqlHost" "--port=$MysqlPort" '--user=root' `
                '--default-character-set=utf8mb4' 2>&1)
        $exitCode = $LASTEXITCODE
        return [pscustomobject][ordered]@{ ExitCode = [int]$exitCode; Output = $output }
    } finally {
        $env:MYSQL_PWD = $old
    }
}

Enter-PandoraOrchestrationLock -ProjectRoot $ProjectRoot -Operation '数据库结构迁移'
$orchestrationLockEntered = $true
try {
# 先确认 MySQL 能连。策划 fast 同一次查询顺便取实例身份和库/表清单，
# 供强收据 fail-closed 命中；普通模式仍只做原有 SHOW DATABASES。
$plannerProbe = $null
$probeSql = if ($PlannerFastStart) {
    @"
SELECT CONCAT('__PANDORA_UUID__=', @@server_uuid);
SELECT CONCAT('__PANDORA_DATADIR__=', @@datadir);
SELECT CONCAT('__PANDORA_DB__=', SCHEMA_NAME) FROM INFORMATION_SCHEMA.SCHEMATA;
SELECT CONCAT('__PANDORA_TABLE__=', TABLE_SCHEMA, '.', TABLE_NAME)
FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE';
"@
} else { 'SHOW DATABASES;' }
$dbListRaw = Invoke-DevMysqlQuery $probeSql
$probeExitCode = $LASTEXITCODE
if ($probeExitCode -ne 0) {
    $whoRaw = if ($UseLocalClient) { "本机 MySQL ${MysqlHost}:${MysqlPort}" } else { "dev MySQL 容器『$Container』" }
    Write-MigWarn "连不上 $whoRaw,跳过结构升级(基础设施可能还没起完)。"
    Write-MigWarn "  详情:$($dbListRaw | Select-Object -First 3)"
    if ($RequireMysql) { exit 1 }
    exit 0
}
if ($PlannerFastStart) {
    $plannerProbe = ConvertFrom-PandoraPlannerMysqlProbe -Lines @($dbListRaw)
    $initialExistingDbs = @($plannerProbe.Databases)
} else {
    $initialExistingDbs = @($dbListRaw | ForEach-Object { "$_".Trim() } | Where-Object { $_ })
}

# ---------------------------------------------------------------------------
# 第 0 步:重放 deploy/mysql-init/*.sql
#
# 为什么必须有这一步:tools/migrate 只覆盖「有迁移集的库」(migrations/<set>/),而
# mysql-init 里后来新增的库/表根本没有对应迁移集 —— 比如 14-bag-tables.sql(pandora_bag)、
# 15-owner-tables.sql(pandora_owner)。老卷上这两个文件从没执行过,现象:
#   inventory: 缺少数据库表 [bag_migration bag_capacity]
#   owner    : Access denied ... to database 'pandora_owner'(库压根不存在)
# 服务自己的报错甚至直接写了「请对当前库重放 deploy/mysql-init/xx.sql」—— 那就别让人手工做。
#
# 安全性:这批文件全是 CREATE DATABASE / TABLE IF NOT EXISTS(已用正则核过,无 DROP/ALTER/
# INSERT),对已有库重放是空操作,不会动数据。需要 root 是因为 01 里有 CREATE DATABASE + GRANT。
# ---------------------------------------------------------------------------
$initDir = Join-Path $ProjectRoot 'deploy/mysql-init'
$initFiles = @(Get-ChildItem -LiteralPath $initDir -Filter '*.sql' -ErrorAction SilentlyContinue | Sort-Object Name)
$initCreatedDbs = @($initFiles | ForEach-Object {
    $sqlText = Get-Content -LiteralPath $_.FullName -Raw -Encoding utf8
    [regex]::Matches($sqlText, '(?im)\bCREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(?<db>[a-z][a-z0-9_]*)`?') |
        ForEach-Object { $_.Groups['db'].Value }
} | Sort-Object -Unique)
$initInventory = $null
$initFingerprint = ''
$initReceiptPath = Join-Path $ProjectRoot 'run/localinfra/cfg/mysql-init-receipt.json'
$skipInitReplay = $false
$initReplayExecuted = $false
if ($PlannerFastStart -and $initFiles.Count -gt 0) {
    $initInventory = Get-PandoraPlannerMysqlInitInventory -Files $initFiles
    $initFingerprint = Get-PandoraPlannerMysqlInitFingerprint -Files $initFiles -ProjectRoot $ProjectRoot
    $skipInitReplay = Test-PandoraPlannerMysqlInitReceipt -ReceiptPath $initReceiptPath `
        -Fingerprint $initFingerprint -ServerUuid $plannerProbe.ServerUuid -DataDir $plannerProbe.DataDir `
        -FileCount $initFiles.Count -ExpectedDatabases $initInventory.Databases -ExpectedTables $initInventory.Tables `
        -ActualDatabases $plannerProbe.Databases -ActualTables $plannerProbe.Tables
}
if ($initFiles.Count -gt 0) {
    if ($WhatIfOnly) {
        Write-MigInfo "[WhatIf] 将重放 mysql-init 建库建表脚本($($initFiles.Count) 个)，本轮不执行。"
    } elseif ($skipInitReplay) {
        Write-MigOk "mysql-init 强收据命中(server_uuid + datadir + $($initInventory.Tables.Count) 张实表)，跳过重复 DDL。"
    } else {
        Write-MigInfo "重放 mysql-init 建库建表脚本($($initFiles.Count) 个,全 IF NOT EXISTS,已存在则空跑)..."
        if ($PlannerFastStart) {
            $batchResult = Invoke-DevMysqlScriptsBatch -Files $initFiles
            if ($batchResult.ExitCode -ne 0) {
                Write-Host '[ERR] 批量重放 mysql-init 失败:' -ForegroundColor Red
                $batchResult.Output | ForEach-Object { Write-Host "      $_" -ForegroundColor Red }
                exit 1
            }
        } else {
            foreach ($f in $initFiles) {
                $sqlOut = Invoke-DevMysqlScript -Path $f.FullName
                if ($LASTEXITCODE -ne 0) {
                    Write-Host "[ERR] 重放 $($f.Name) 失败:" -ForegroundColor Red
                    $sqlOut | ForEach-Object { Write-Host "      $_" -ForegroundColor Red }
                    exit 1
                }
            }
        }
        $initReplayExecuted = $true
        Write-MigOk "mysql-init 脚本已全部重放(建库 / 建表 / 授权已对齐仓库当前状态)。"
        if ($PlannerFastStart) {
            $postReplayFingerprint = Get-PandoraPlannerMysqlInitFingerprint -Files $initFiles -ProjectRoot $ProjectRoot
            if (-not [string]::Equals($postReplayFingerprint, $initFingerprint, [StringComparison]::Ordinal)) {
                Write-Host '[ERR] mysql-init 在执行期间发生变化，拒绝写入过期收据；请等同步完成后重试。' -ForegroundColor Red
                exit 1
            }
            try {
                Write-PandoraPlannerMysqlInitReceipt -ReceiptPath $initReceiptPath `
                    -Fingerprint $initFingerprint -ServerUuid $plannerProbe.ServerUuid -DataDir $plannerProbe.DataDir `
                    -FileCount $initFiles.Count -DatabaseCount $initInventory.Databases.Count -TableCount $initInventory.Tables.Count
            } catch {
                Write-MigWarn "mysql-init 已成功，但本地加速收据写入失败；本轮继续，下次会安全地重放:$($_.Exception.Message)"
            }
        }
    }
} elseif ($RequireMysql) {
    Write-MigWarn "找不到任何 mysql-init SQL:$initDir"
    exit 1
}

# ---------------------------------------------------------------------------
# 第 1 步:跑增量迁移(列改动这类 IF NOT EXISTS 表达不了的结构变更)
# ---------------------------------------------------------------------------
if (-not (Test-Path -LiteralPath $MigrationsRoot)) {
    Write-MigWarn "找不到 $MigrationsRoot,跳过增量迁移。"
    if ($RequireMysql) { exit 1 }
    exit 0
}

# 1) 本仓库有哪些 migration set(= 目录名 = 库名)
$sets = @(Get-ChildItem -LiteralPath $MigrationsRoot -Directory -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty Name | Sort-Object)
if ($sets.Count -eq 0) {
    Write-MigWarn "$MigrationsRoot 下没有 migration set,跳过。"
    if ($RequireMysql) { exit 1 }
    exit 0
}

# 2) 本机 dev MySQL 里实际存在哪些库。只升级「已存在」的库:建库是上面重放 mysql-init
#    的职责,本步不越权建库,免得把拼错的库名凭空建出来。
#    只有真正重放过 init 才需再查；收据命中时复用首轮同一连接的库清单。
if ($PlannerFastStart -and -not $initReplayExecuted) {
    $existingDbs = @($initialExistingDbs)
} else {
    $dbListRaw = Invoke-DevMysqlQuery 'SHOW DATABASES;'
    if ($LASTEXITCODE -ne 0) {
        Write-MigWarn "重放后重查库列表失败,跳过增量迁移。"
        if ($RequireMysql) { exit 1 }
        exit 0
    }
    $existingDbs = @($dbListRaw | ForEach-Object { "$_".Trim() } | Where-Object { $_ })
}

$targets = if ($WhatIfOnly) {
    @($sets | Where-Object { $existingDbs -contains $_ -or $initCreatedDbs -contains $_ })
} else {
    @($sets | Where-Object { $existingDbs -contains $_ })
}
$missing = @($sets | Where-Object { $targets -notcontains $_ })
if ($WhatIfOnly) {
    $willCreate = @($targets | Where-Object { $existingDbs -notcontains $_ -and $initCreatedDbs -contains $_ })
    if ($willCreate.Count -gt 0) {
        Write-MigInfo "[WhatIf] 这些库将先由 mysql-init 创建、再执行迁移:$($willCreate -join ', ')"
    }
}
if ($missing.Count -gt 0) {
    if ($RequireMysql -and -not $WhatIfOnly) {
        Write-MigWarn "强制迁移要求的库不存在:$($missing -join ', ')。mysql-init、权限或分发包不完整，拒绝带着漏迁移的 schema 继续启动。"
        exit 1
    }
    Write-MigInfo "本机没有这些库,跳过(通常是该服务在本机不启用):$($missing -join ', ')"
}
if ($targets.Count -eq 0) {
    Write-MigWarn '本机 dev MySQL 里没有任何可升级的库,跳过。'
    if ($RequireMysql -and -not $WhatIfOnly) { exit 1 }
    exit 0
}
Write-MigInfo "待检查的库($($targets.Count)):$($targets -join ', ')"
if ($WhatIfOnly) { exit 0 }

# 3) 选迁移器:优先预编译产物(策划机没 Go),否则现场 go run。
$migrateExe = Join-Path $ProjectRoot 'run/artifacts/windows/bin/pandora-migrate.exe'
$hasGo = [bool](Get-Command go -ErrorAction SilentlyContinue)
if (-not (Test-Path -LiteralPath $migrateExe) -and -not $hasGo) {
    # 不阻断启动:结构可能本来就是最新的。但必须把话说清楚,不能让人再对着
    # "Unknown column" 猜半天 —— 这正是本脚本存在的理由。
    Write-MigWarn '本机既没有 Go,也没有预编译的迁移器,无法自动升级数据库结构。'
    Write-MigWarn '  若稍后有服务报 Unknown column / Table doesn''t exist,就是结构没跟上,'
    Write-MigWarn '  请找后端同学跑一次 pwsh tools/scripts/dev_migrate.ps1(需要 Go)。'
    if ($RequireMysql) { exit 1 }
    exit 0
}

# 4) 生成迁移器要的 targets 清单 + DSN 文件(它只接受文件形式的 DSN,且必须在清单同目录下)。
#    放临时目录,用完即删。
$tmpDir = Join-Path ([System.IO.Path]::GetTempPath()) ("pandora-dev-migrate-{0}-{1}" -f $PID, [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null
try {
    $targetEntries = @()
    $expected = @()
    foreach ($db in $targets) {
        $dsnFile = "$db.dsn"
        # 迁移器的目标名只允许 ^[a-z][a-z0-9-]{0,62}$(不能有下划线),而库名是
        # pandora_player 这种带下划线的 —— 转成连字符。migration_set / database 仍用真库名。
        $name = ($db -replace '_', '-') + '-dev'
        # parseTime/loc 与各服务 DSN 对齐;multiStatements 供 baseline 那种多语句脚本执行。
        $dsn = "${MysqlUser}:${MysqlPassword}@tcp(${MysqlHost}:${MysqlPort})/${db}?parseTime=true&loc=UTC&multiStatements=true"
        Set-Content -LiteralPath (Join-Path $tmpDir $dsnFile) -Value $dsn -Encoding ascii -NoNewline
        $targetEntries += [ordered]@{
            name                      = $name
            migration_set             = $db
            database                  = $db
            dsn_file                  = $dsnFile
            timeout_seconds           = 300
            lock_wait_timeout_seconds = 15
        }
        $expected += "${name}:${db}:${db}"
    }
    $targetsFile = Join-Path $tmpDir 'targets.json'
    Set-Content -LiteralPath $targetsFile -Encoding utf8NoBOM `
        -Value (@{ targets = $targetEntries } | ConvertTo-Json -Depth 5)

    # -environment=dev:免掉迁移器对 TLS 的强制要求(本机容器 MySQL 没有 TLS)。
    # -expected-targets 在生产是「发布侧独立审核清单」,本机 dev 没有第二方,这里与 targets
    # 同源生成 —— 它在本场景只起格式校验作用,不构成额外保证(生产的独立审核仍在 k8s Job 里)。
    $migArgs = @(
        "-targets-file=$targetsFile"
        "-expected-targets=$($expected -join ',')"
        '-environment=dev'
    )

    Write-MigInfo '执行迁移器(只补缺失版本,已是最新则空跑)...'
    # 迁移器的进度日志走 stderr(Go log 默认)。不合并的话 PowerShell 会把每行都渲染成
    # 红色 NativeCommandError,看着像出错了其实一切正常 —— 2>&1 合并成普通输出行。
    # 真正的成败只看退出码。
    Assert-LocalMysqlOwned
    if (Test-Path -LiteralPath $migrateExe) {
        & $migrateExe @migArgs 2>&1 | ForEach-Object { "  $_" }
    } else {
        Push-Location (Join-Path $ProjectRoot 'tools/migrate')
        try { & go run . @migArgs 2>&1 | ForEach-Object { "  $_" } } finally { Pop-Location }
    }
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERR] 数据库结构升级失败(exit=$LASTEXITCODE)。" -ForegroundColor Red
        Write-Host "      业务服务连上旧结构只会启动即崩(如 Unknown column),故此处中止。" -ForegroundColor Red
        exit 1
    }
    Write-MigOk '数据库结构已是最新。'
}
finally {
    Remove-Item -LiteralPath $tmpDir -Recurse -Force -ErrorAction SilentlyContinue
}
} finally {
    if ($orchestrationLockEntered) { Exit-PandoraOrchestrationLock }
}
