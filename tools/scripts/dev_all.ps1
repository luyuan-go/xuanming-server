# Pandora 一键开发环境(基础设施 + 全部业务服务)
#
# 这是给"UE 联调"用的一条命令:先拉起 docker 基础设施(MySQL/Redis/Kafka/etcd/Envoy),
# 等容器 healthy 后,再按依赖顺序拉起 Go 业务服务。
#
# 用法:
#   # 起全部业务服务(默认)
#   pwsh tools/scripts/dev_all.ps1
#
#   # 全起但排除某个服务(留给 VS Code 断点调试)
#   pwsh tools/scripts/dev_all.ps1 -Exclude team
#
#   # 全停(基础设施 + 业务服务)
#   pwsh tools/scripts/dev_all.ps1 -Down

[CmdletBinding()]
param(
    [string[]]$Exclude = @(),
    [switch]$Pull,
    [switch]$Down,

    # 免 Docker 模式(策划机):基础设施改用本机原生进程(local_infra.ps1),
    # 不起 TiDB(社交四服改连本机 MySQL 的 pandora_social)。
    # 默认关 = 完全保持原有 docker 行为(CLAUDE.md §14.2)。
    [switch]$NoDocker,

    # 本轮 -GenTables 的强指纹是否确认产物发生变化。默认 false，普通入口行为不变；
    # run_services 用它把读表服务纳入选择性重启集合。
    [switch]$ConfigTableChanged,

    # 只由策划 fast 入口传入：导表与业务 staging build、基础设施启动并行。
    # 普通 dev_all 调用仍由 start.ps1 在进入本脚本前完成导表。
    [switch]$GenerateTables
)

$ErrorActionPreference = 'Stop'
$ScriptDir = $PSScriptRoot
. (Join-Path $ScriptDir 'lib/local_infra_state.ps1')
. (Join-Path $ScriptDir 'lib/planner_mysql_startup.ps1')
. (Join-Path $ScriptDir 'lib/planner_start_timing.ps1')
. (Join-Path $ScriptDir 'lib/planner_parallel_prepare.ps1')
$projectRoot = (Resolve-Path "$ScriptDir/../..").Path

function Get-PlannerConfigTableDistIdentity {
    $manifestPath = Join-Path $projectRoot 'configtable/dist/manifest.json'
    if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) { return $null }
    try {
        # version 是 SVN revision；本地尚未提交的 xlsx 变化仍可能沿用同一 revision。
        # manifest 内含逐表 checksum，绑定整份 manifest 才能识别真实内容变化。
        return (Get-FileHash -LiteralPath $manifestPath -Algorithm SHA256).Hash
    } catch { return $null }
}

function Get-PlannerConfigTableGenerationIdentity {
    # speculative build 与导表会并发读取/写入 pkg/configtable。只跟踪生成器可能改动且
    # Go build 会读取的代码：所有 *.gen.go（含 tables.gen.go / *_bitindex.gen.go）以及
    # 每张表缺失时才创建的 <name>.go companion。纯 dist JSON/manifest 由上面的 dist
    # identity 单独驱动消费者重启，不应让一个数据值变化作废已完成的 Go staging build。
    $paths = [Collections.Generic.List[string]]::new()
    $goDir = Join-Path $projectRoot 'pkg/configtable'
    if (Test-Path -LiteralPath $goDir -PathType Container) {
        foreach ($item in @(Get-ChildItem -LiteralPath $goDir -File -Filter '*.gen.go' | Sort-Object FullName)) {
            $paths.Add($item.FullName)
            $tableMatch = [regex]::Match($item.Name, '^(?<name>.+)_table\.gen\.go$',
                [Text.RegularExpressions.RegexOptions]::CultureInvariant)
            if (-not $tableMatch.Success) { continue }
            $companion = Join-Path $goDir ($tableMatch.Groups['name'].Value + '.go')
            if (Test-Path -LiteralPath $companion -PathType Leaf) { $paths.Add($companion) }
        }
    }
    $hash = [Security.Cryptography.IncrementalHash]::CreateHash(
        [Security.Cryptography.HashAlgorithmName]::SHA256)
    try {
        foreach ($path in @($paths | Sort-Object)) {
            $relative = [IO.Path]::GetRelativePath($projectRoot, $path).Replace('\', '/')
            $hash.AppendData([Text.Encoding]::UTF8.GetBytes($relative))
            $hash.AppendData([byte[]]@(0))
            $hash.AppendData([IO.File]::ReadAllBytes($path))
            $hash.AppendData([byte[]]@(0))
        }
        return [Convert]::ToHexString($hash.GetHashAndReset())
    } finally {
        $hash.Dispose()
    }
}

function Remove-PlannerPreparationOrphanStages($Handle) {
    if ($null -eq $Handle -or "$($Handle.Name)" -notlike 'build*' -or $null -eq $Handle.Process) { return }
    $workerPid = [int]$Handle.Process.Id
    if ($workerPid -le 0) { throw "无效的 build worker PID:$workerPid" }
    $binDir = [IO.Path]::GetFullPath((Join-Path $projectRoot 'run/dev/bin'))
    if (-not (Test-Path -LiteralPath $binDir -PathType Container)) { return }
    $exactPattern = '^\.[A-Za-z0-9_]+\.planner-stage-' + [regex]::Escape("$workerPid") +
        '-[0-9a-fA-F]{32}\.exe$'
    foreach ($candidate in @(Get-ChildItem -LiteralPath $binDir -File -Force -ErrorAction Stop)) {
        if ($candidate.Name -cnotmatch $exactPattern -or
            -not [string]::Equals($candidate.Directory.FullName, $binDir, [StringComparison]::OrdinalIgnoreCase)) {
            continue
        }
        Remove-Item -LiteralPath $candidate.FullName -Force -ErrorAction Stop
    }
}

$plannerTableHandle = $null
$plannerBuildHandle = $null
$plannerMigrationContext = [pscustomobject]@{ Result = $null }
$plannerPreparedBuildManifest = ''
$plannerPreparedBuildConsumed = $false
$plannerParallelPrepareStartedAt = 0L
$plannerParallelPrepareRecorded = $false

Enter-PandoraOrchestrationLock -ProjectRoot $projectRoot -Operation $(if ($Down) { '完整停止' } else { '完整启动' })
try {
if ($Down) {
    Write-Host "===== 停止业务服务 =====" -ForegroundColor Cyan
    & "$ScriptDir/run_services.ps1" -Action down
    if ($LASTEXITCODE -ne 0) {
        Write-Host '[ERR] 业务服务未能全部停止；保留基础设施，避免仍存活服务被突然断库。' -ForegroundColor Red
        exit 1
    }
    Write-Host ""
    Write-Host "===== 停止基础设施 =====" -ForegroundColor Cyan
    if ($NoDocker) { & "$ScriptDir/local_infra.ps1" -Action down } else { & "$ScriptDir/dev_down.ps1" }
    exit $LASTEXITCODE
}

if ($NoDocker) {
    # ===== 免 Docker 路线 =====
    # 与 docker 路线的差异:基础设施换成本机进程、不起 TiDB、迁移器用本机 mysql.exe；
    # MySQL 使用独立动态端口，并在 run/localinfra 下派生服务运行态配置，仓库 yaml 不分叉。
    Write-Host "===== [1/3] 策划机 MySQL 模式与基础设施 =====" -ForegroundColor Cyan
    # bundle 存在即锁定 central-managed；配置/登记/网络任一失败均中止，
    # 不得回退到本机 MySQL，否则会让策划以为自己在操作中心 workspace。
    $mysqlContext = Invoke-PandoraPlannerTimedStep -Name '数据库模式与远端工作区校验' -Action {
        Initialize-PandoraPlannerMysqlRuntime -ProjectRoot $projectRoot
    }
    $centralManaged = $mysqlContext.Mode -ceq 'central-managed'
    if ($centralManaged) {
        Write-Host ("[数据库] mode=central-managed backend=oracle-mysql workspace={0} endpoint={1}:{2}" -f `
                $mysqlContext.Profile.workspace_id, $mysqlContext.Profile.endpoint.host, $mysqlContext.Profile.endpoint.port) `
            -ForegroundColor Cyan
    } else {
        Write-Host '[数据库] mode=local-owned backend=oracle-mysql' -ForegroundColor Yellow
    }
    # 端口权威与“业务服务已经应用的端口”是两件事。只有完整服务启动成功才更新后者；
    # 即使上轮在基础设施启动后半途失败，下轮也仍会强制刷新旧 DSN 进程。
    $appliedMysql = Get-PandoraServiceAppliedMysqlState $projectRoot
    # 策划一键是完整服务集合；调试用 -Exclude 保持原路径，避免 prepared manifest 与
    # 随后 activation 的目标集合不一致。
    $plannerParallelEnabled = $env:PANDORA_PLANNER_FAST_START -ceq '1' -and $Exclude.Count -eq 0
    $tableVersionBefore = $null
    $generationIdentityBefore = $null
    if ($plannerParallelEnabled) {
        $plannerParallelPrepareStartedAt = [Environment]::TickCount64
        $pwshExe = Join-Path $PSHOME 'pwsh.exe'
        if (-not (Test-Path -LiteralPath $pwshExe -PathType Leaf)) {
            throw "策划并行准备找不到当前 PowerShell:$pwshExe"
        }
        if ($GenerateTables) {
            $tableVersionBefore = Get-PlannerConfigTableDistIdentity
            $generationIdentityBefore = Get-PlannerConfigTableGenerationIdentity
            $plannerTableHandle = Start-PandoraPlannerPreparationProcess -Name tables -FilePath $pwshExe `
                -WorkingDirectory $projectRoot -ArgumentList @(
                    '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File',
                    (Join-Path $ScriptDir 'configtable_gen.ps1'), '-SkipIfInputsUnchanged'
                )
            Write-Host '  [parallel] 导表已启动；与 build/基础设施重叠执行。' -ForegroundColor DarkCyan
        }

        $prepareDir = Join-Path $projectRoot 'run/localinfra/tmp'
        New-Item -ItemType Directory -Force -Path $prepareDir | Out-Null
        $plannerPreparedBuildManifest = Join-Path $prepareDir (
            'planner-build-prepared-{0}-{1}.json' -f $PID, [guid]::NewGuid().ToString('N'))
        $buildArguments = [Collections.Generic.List[string]]::new()
        foreach ($argument in @(
                '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File',
                (Join-Path $ScriptDir 'run_services.ps1'), '-Action', 'prepare',
                '-FastExistingProbe', '-PreparedBuildManifestPath', $plannerPreparedBuildManifest
            )) { $buildArguments.Add($argument) }
        $plannerBuildHandle = Start-PandoraPlannerPreparationProcess -Name build -FilePath $pwshExe `
            -WorkingDirectory $projectRoot -ArgumentList $buildArguments.ToArray()
        Write-Host '  [parallel] 业务 staging build 已启动；不会覆盖正在运行的 exe。' -ForegroundColor DarkCyan
    } elseif ($GenerateTables) {
        throw '-GenerateTables 只允许不带 -Exclude 的策划 fast 完整启动入口使用。'
    }

    $infraReadyCallback = $null
    if ($plannerParallelEnabled -and -not $centralManaged) {
        $infraReadyCallback = {
            param($State)
            if ("$($State.Name)" -cne 'mysql') { return }
            if ($null -ne $plannerMigrationContext.Result) {
                throw 'MySQL ready callback 被重复调用；拒绝执行第二次 migration。'
            }
            $migrationPort = Get-PandoraLocalMysqlPort $projectRoot -Required
            $mysqlClient = Get-ChildItem -Path (Join-Path $ScriptDir '../../run/localinfra/dist/mysql') `
                -Recurse -File -Filter 'mysql.exe' -ErrorAction SilentlyContinue | Select-Object -First 1
            if (-not $mysqlClient) { throw 'MySQL 已就绪，但找不到本机 mysql.exe，无法启动迁移。' }
            # 四个基础设施进程已在统一轮询前全部 launch。这里留在父 runspace 同步迁移，
            # Kafka/Redis/Envoy 仍由各自 OS 进程继续启动，同时 dev_migrate 可递归复用父进程
            # 已持有的工作区编排锁；严禁另起 pwsh 后用“跳过锁”绕过并发保护。
            Write-Host '  [parallel] MySQL 已通过协议探活；migration 在父编排锁内执行。' `
                -ForegroundColor DarkCyan
            $migrationWatch = [Diagnostics.Stopwatch]::StartNew()
            $migrationExitCode = -1
            $migrationErrorRecord = $null
            try {
                & "$ScriptDir/dev_migrate.ps1" -MysqlClient $mysqlClient.FullName `
                    -MysqlPort $migrationPort -RequireMysql -TotalTimeoutSeconds 600 |
                    ForEach-Object { Write-Host "$_" }
                $migrationExitCode = [int]$LASTEXITCODE
            } catch {
                $migrationErrorRecord = $_
            } finally {
                $migrationWatch.Stop()
                $plannerMigrationContext.Result = [pscustomobject][ordered]@{
                    ExitCode = [int]$migrationExitCode
                    ElapsedMilliseconds = [int64]$migrationWatch.ElapsedMilliseconds
                    ErrorRecord = $migrationErrorRecord
                }
            }
        }.GetNewClosure()
    }

    $infraWatch = [Diagnostics.Stopwatch]::StartNew()
    $infraStatus = '失败'
    try {
        if ($null -ne $infraReadyCallback) {
            & "$ScriptDir/local_infra.ps1" -Action up -OnPlannerComponentReady $infraReadyCallback
        } else {
            & "$ScriptDir/local_infra.ps1" -Action up
        }
        $infraExitCode = $LASTEXITCODE
        if ($infraExitCode -eq 0) { $infraStatus = '完成' }
    } finally {
        $infraWatch.Stop()
        Add-PandoraPlannerTiming -Name '基础设施总计' `
            -ElapsedMilliseconds $infraWatch.ElapsedMilliseconds -Status $infraStatus
    }
    if ($infraExitCode -ne 0) {
        Write-Host "[ERR] 本机基础设施启动失败,中止" -ForegroundColor Red
        exit 1
    }

    if ($plannerParallelEnabled) {
        $generationIdentityChanged = $false
        if ($plannerTableHandle) {
            $tableResult = Complete-PandoraPlannerPreparationProcess -Handle $plannerTableHandle `
                -TimeoutMilliseconds 300000 -WriteOutput
            Add-PandoraPlannerTiming -Name '导表' -ElapsedMilliseconds $tableResult.ElapsedMilliseconds `
                -Status $(if ($tableResult.ExitCode -eq 0) { '完成' } else { '失败' })
            if ($tableResult.ExitCode -ne 0) {
                Write-Host '[ERR] 并行导表失败；不会发布二进制或启动业务服务。' -ForegroundColor Red
                exit 1
            }
            $plannerTableHandle = $null
            $tableVersionAfter = Get-PlannerConfigTableDistIdentity
            $ConfigTableChanged = $null -eq $tableVersionAfter -or $tableVersionBefore -cne $tableVersionAfter
            $generationIdentityAfter = Get-PlannerConfigTableGenerationIdentity
            $generationIdentityChanged = $generationIdentityBefore -cne $generationIdentityAfter
        }

        $buildResult = Complete-PandoraPlannerPreparationProcess -Handle $plannerBuildHandle `
            -TimeoutMilliseconds 900000 -WriteOutput
        $buildDisposition = Get-PandoraPlannerSpeculativeBuildDisposition `
            -GenerationIdentityChanged $generationIdentityChanged -ExitCode $buildResult.ExitCode `
            -DrainCompleted $buildResult.DrainCompleted
        $initialBuildStatus = if ($buildDisposition -ceq 'retry-stable-once') { '作废' } elseif ($buildResult.ExitCode -eq 0) {
            '完成'
        } else { '失败' }
        $initialBuildDetail = if ($buildDisposition -ceq 'retry-stable-once') { '导表改变生成态，稳定后重编' } else { '' }
        Add-PandoraPlannerTiming -Name '业务程序·并行 staging build' `
            -ElapsedMilliseconds $buildResult.ElapsedMilliseconds `
            -Status $initialBuildStatus -Detail $initialBuildDetail
        if ($buildDisposition -ceq 'fail-unbounded') {
            Write-Host "[ERR] 并行 staging build 未能有界收口:$($buildResult.DrainError)" -ForegroundColor Red
            exit 1
        }

        if ($buildDisposition -ceq 'retry-stable-once') {
            # 首轮 build 是投机结果：无论成功/失败，只要导表改变了生成 Go/companion 文件，
            # 都不能把它当权威结果。完成 exact worker 回收后清 staging，并只在稳定输入上重建一次。
            Remove-PlannerPreparationOrphanStages $plannerBuildHandle
            & "$ScriptDir/run_services.ps1" -Action discard `
                -PreparedBuildManifestPath $plannerPreparedBuildManifest
            if ($LASTEXITCODE -ne 0) { throw '无法安全丢弃导表期间的 speculative staging build。' }
            $plannerBuildHandle = $null
            $plannerBuildHandle = Start-PandoraPlannerPreparationProcess -Name build-after-tables `
                -FilePath $pwshExe -WorkingDirectory $projectRoot -ArgumentList $buildArguments.ToArray()
            Write-Host '  [parallel] 导表改变生成态；稳定 target 重编与数据库迁移继续重叠。' -ForegroundColor DarkCyan
        } elseif ($buildDisposition -ceq 'fail') {
            Write-Host '[ERR] 并行 staging build 失败；正式 exe/运行中服务均未改动。' -ForegroundColor Red
            exit 1
        } else {
            $plannerBuildHandle = $null
        }
    }
    $mysqlPort = if ($centralManaged) { [int]$mysqlContext.Profile.endpoint.port } else {
        Get-PandoraLocalMysqlPort $projectRoot -Required
    }
    $runtimeMode = if ($centralManaged) { 'central' } else { 'nodocker' }
    $profileFingerprint = if ($centralManaged) { "$($mysqlContext.Profile.fingerprint)" } else { '' }

    if (-not $appliedMysql -or $appliedMysql.Mode -ne $runtimeMode -or
        [int]$appliedMysql.MysqlPort -ne $mysqlPort -or -not [bool]$appliedMysql.SocialOnMysql -or
        ($centralManaged -and "$($appliedMysql.ProfileFingerprint)" -cne $profileFingerprint)) {
        $fromText = if ($appliedMysql) { "$($appliedMysql.Mode)/:$($appliedMysql.MysqlPort)/social_mysql=$($appliedMysql.SocialOnMysql)" } else { '未登记/旧版' }
        Write-Host "[INFO] 业务服务已应用运行态为 $fromText，当前为 $runtimeMode/:$mysqlPort/social_mysql=True；完整停止本项目业务服务以刷新 DSN。" -ForegroundColor Cyan
        $switchWatch = [Diagnostics.Stopwatch]::StartNew()
        $switchStatus = '失败'
        try {
            & "$ScriptDir/run_services.ps1" -Action down
            $switchExitCode = $LASTEXITCODE
            if ($switchExitCode -eq 0) { $switchStatus = '完成' }
        } finally {
            $switchWatch.Stop()
            Add-PandoraPlannerTiming -Name '旧业务运行态切换' `
                -ElapsedMilliseconds $switchWatch.ElapsedMilliseconds -Status $switchStatus
        }
        if ($switchExitCode -ne 0) {
            Write-Host '[ERR] 旧业务服务停止失败，不能带着旧 MySQL DSN 继续启动。' -ForegroundColor Red
            exit 1
        }
    }

    Write-Host ""
    Write-Host "===== [2/3] 数据库结构 =====" -ForegroundColor Cyan
    if ($centralManaged) {
        # 中心 provisioner 只有 READY 才发凭据；本机不再拥有 migration 写权。
        # run_services 会用 verify-only 做 schema/权限/TLS 预检，不对中心库执行变更。
        Write-Host '[ OK ] 中心 workspace 已 READY；跳过本机 MySQL 迁移。' -ForegroundColor Green
        Add-PandoraPlannerTiming -Name '数据库结构校验/迁移' -ElapsedMilliseconds 0 -Status '跳过'
    } elseif ($plannerParallelEnabled) {
        $migrationResult = $plannerMigrationContext.Result
        if ($null -eq $migrationResult) {
            Add-PandoraPlannerTiming -Name '数据库结构校验/迁移' -ElapsedMilliseconds 0 -Status '失败' `
                -Detail 'MySQL ready 后没有完成 migration'
            throw 'MySQL 已就绪，但 migration 没有执行；拒绝带旧结构继续。'
        }
        Add-PandoraPlannerTiming -Name '数据库结构校验/迁移' `
            -ElapsedMilliseconds $migrationResult.ElapsedMilliseconds `
            -Status $(if ($migrationResult.ExitCode -eq 0) { '完成' } else { '失败' })
        if ($null -ne $migrationResult.ErrorRecord) {
            Write-Host "[ERR] migration 执行异常:$($migrationResult.ErrorRecord.Exception.Message)" -ForegroundColor Red
        }
        if ($migrationResult.ExitCode -ne 0 -or $null -ne $migrationResult.ErrorRecord) {
            Write-Host '[ERR] 并行数据库结构升级失败；不会发布二进制或启动业务服务。' -ForegroundColor Red
            exit 1
        }
        $plannerMigrationContext.Result = $null
    } else {
        $schemaWatch = [Diagnostics.Stopwatch]::StartNew()
        $schemaStatus = '失败'
        try {
            # 免 Docker 本机普通路径用 local_infra 备料的 mysql.exe 作客户端。
            $mysqlClient = Get-ChildItem -Path (Join-Path $ScriptDir '../../run/localinfra/dist/mysql') `
                -Recurse -File -Filter 'mysql.exe' -ErrorAction SilentlyContinue | Select-Object -First 1
            if (-not $mysqlClient) {
                Write-Host "[ERR] 找不到本机 mysql.exe(备料应由 local_infra.ps1 完成),中止" -ForegroundColor Red
                exit 1
            }
            & "$ScriptDir/dev_migrate.ps1" -MysqlClient $mysqlClient.FullName -MysqlPort $mysqlPort -RequireMysql
            if ($LASTEXITCODE -ne 0) {
                Write-Host "[ERR] 数据库结构升级失败,中止(继续启动只会让服务连着旧结构崩溃)" -ForegroundColor Red
                exit 1
            }
            $schemaStatus = '完成'
        } finally {
            $schemaWatch.Stop()
            Add-PandoraPlannerTiming -Name '数据库结构校验/迁移' `
                -ElapsedMilliseconds $schemaWatch.ElapsedMilliseconds -Status $schemaStatus
        }
    }

    if ($plannerBuildHandle) {
        $stableBuildResult = Complete-PandoraPlannerPreparationProcess -Handle $plannerBuildHandle `
            -TimeoutMilliseconds 900000 -WriteOutput
        Add-PandoraPlannerTiming -Name '业务程序·导表后目标重编' `
            -ElapsedMilliseconds $stableBuildResult.ElapsedMilliseconds `
            -Status $(if ($stableBuildResult.ExitCode -eq 0) { '完成' } else { '失败' })
        if ($stableBuildResult.ExitCode -ne 0 -or -not $stableBuildResult.DrainCompleted) {
            Write-Host '[ERR] 导表后目标重编失败；不会发布二进制或启动业务服务。' -ForegroundColor Red
            exit 1
        }
        $plannerBuildHandle = $null
    }
    if ($plannerParallelEnabled -and -not $plannerParallelPrepareRecorded) {
        $parallelPrepareElapsed = [Math]::Max([int64]0,
            [Environment]::TickCount64 - [int64]$plannerParallelPrepareStartedAt)
        Add-PandoraPlannerTiming -Name '并行准备总计' -ElapsedMilliseconds $parallelPrepareElapsed
        $plannerParallelPrepareRecorded = $true
        Write-Host ("[perf] planner-parallel-prepare wall_ms={0} tables_build_infra_migration=overlapped" -f `
                $parallelPrepareElapsed) -ForegroundColor DarkGray
    }

    Write-Host ""
    Write-Host "===== [3/3] 业务服务 =====" -ForegroundColor Cyan
    $servicesWatch = [Diagnostics.Stopwatch]::StartNew()
    $servicesStatus = '失败'
    try {
        & "$ScriptDir/run_services.ps1" -Exclude $Exclude -SocialOnMysql -NoDocker -MysqlPort $mysqlPort `
            -FastExistingProbe:($env:PANDORA_PLANNER_FAST_START -eq '1') `
            -ConfigTableChanged:$ConfigTableChanged `
            -PreparedBuildManifestPath $plannerPreparedBuildManifest
        $servicesExitCode = $LASTEXITCODE
        if ($servicesExitCode -eq 0) {
            $servicesStatus = '完成'
            $plannerPreparedBuildConsumed = $true
        }
    } finally {
        $servicesWatch.Stop()
        Add-PandoraPlannerTiming -Name '业务程序启动' `
            -ElapsedMilliseconds $servicesWatch.ElapsedMilliseconds -Status $servicesStatus
    }
    exit $servicesExitCode
}

# 1) 基础设施
Write-Host "===== [1/4] 基础设施 =====" -ForegroundColor Cyan
if ($Pull) { & "$ScriptDir/dev_up.ps1" -Pull } else { & "$ScriptDir/dev_up.ps1" }
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERR] 基础设施启动失败,中止" -ForegroundColor Red
    exit 1
}

# 2) TiDB 集群。friend / chat / guild / mail 四个服务在 run_services.ps1 里被钉死用
# *-dev-tidb.yaml(DSN 指向 127.0.0.1:4000),不起 TiDB 它们必然启动即崩
# (panic: ping mysql: dial tcp 127.0.0.1:4000 ... 拒绝)。tidb_up.ps1 幂等,已在跑则快速返回。
# ⚠️ 首次会拉 pingcap/{pd,tikv,tidb} 镜像(数百 MB,需联网)。
Write-Host ""
Write-Host "===== [2/4] TiDB 集群(社交库) =====" -ForegroundColor Cyan
& "$ScriptDir/tidb_up.ps1"
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERR] TiDB 启动失败,中止(friend/chat/guild/mail 连的就是它)" -ForegroundColor Red
    exit 1
}

# 3) 数据库结构升级。必须夹在「基础设施起好」与「业务服务启动」之间:
# mysql-init 只在数据卷首次创建时跑一次,之后新增的迁移不会自动进库,服务连上旧
# 结构会启动即崩(实测 player: Unknown column 'exp')。幂等,已最新则空跑。
Write-Host ""
Write-Host "===== [3/4] 数据库结构 =====" -ForegroundColor Cyan
& "$ScriptDir/dev_migrate.ps1" -RequireMysql
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERR] 数据库结构升级失败,中止(继续启动只会让服务连着旧结构崩溃)" -ForegroundColor Red
    exit 1
}

# 4) 业务服务
Write-Host ""
Write-Host "===== [4/4] 业务服务 =====" -ForegroundColor Cyan
& "$ScriptDir/run_services.ps1" -Exclude $Exclude -ConfigTableChanged:$ConfigTableChanged
exit $LASTEXITCODE
} finally {
    $plannerCleanupErrors = [Collections.Generic.List[string]]::new()
    foreach ($handle in @($plannerTableHandle, $plannerBuildHandle)) {
        if ($null -eq $handle) { continue }
        try {
            if (-not (Stop-PandoraPlannerPreparationProcess -Handle $handle -DrainTimeoutMilliseconds 5000)) {
                $plannerCleanupErrors.Add("worker $($handle.Name) 未在 5 秒内退出")
            } else {
                Remove-PlannerPreparationOrphanStages $handle
            }
        } catch {
            $plannerCleanupErrors.Add("worker $($handle.Name) 回收失败:$($_.Exception.Message)")
        }
    }
    if (-not [string]::IsNullOrWhiteSpace($plannerPreparedBuildManifest) -and
        -not $plannerPreparedBuildConsumed) {
        try {
            & "$ScriptDir/run_services.ps1" -Action discard `
                -PreparedBuildManifestPath $plannerPreparedBuildManifest
            if ($LASTEXITCODE -ne 0) {
                $plannerCleanupErrors.Add("staging 清理返回退出码 $LASTEXITCODE")
            }
        } catch {
            $plannerCleanupErrors.Add("staging 清理失败:$($_.Exception.Message)")
        }
    }
    Exit-PandoraOrchestrationLock
    if ($plannerCleanupErrors.Count -gt 0) {
        throw "策划并行准备清理不完整:$($plannerCleanupErrors -join ' | ')"
    }
}
