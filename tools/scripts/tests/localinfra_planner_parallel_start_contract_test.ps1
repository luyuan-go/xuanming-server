$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
$helperPath = Join-Path $repoRoot 'tools/scripts/lib/planner_infra_fast_start.ps1'
$infraPath = Join-Path $repoRoot 'tools/scripts/local_infra.ps1'

function Assert-True {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) { throw $Message }
}

function Assert-Equal {
    param($Expected, $Actual, [string]$Message)
    if ($Expected -ne $Actual) {
        throw "$Message (expected=$Expected actual=$Actual)"
    }
}

if (-not (Test-Path -LiteralPath $helperPath -PathType Leaf)) {
    throw "[RED] 缺少策划基础设施并发启动 helper:$helperPath"
}
. $helperPath

$helperTokens = $null
$helperParseErrors = $null
$helperAst = [Management.Automation.Language.Parser]::ParseFile(
    $helperPath, [ref]$helperTokens, [ref]$helperParseErrors)
Assert-True (@($helperParseErrors).Count -eq 0) "planner_infra_fast_start.ps1 AST 解析失败:$($helperParseErrors -join '; ')"
$batchAst = @($helperAst.FindAll({
    param($Node)
    $Node -is [Management.Automation.Language.FunctionDefinitionAst] -and
        $Node.Name -ceq 'Invoke-PandoraPlannerInfraBatch'
}, $true)) | Select-Object -First 1
Assert-True ($null -ne $batchAst) '缺少 Invoke-PandoraPlannerInfraBatch AST'
# coordinator 会在自己的动态子作用域调用 local_infra 的旧 launcher。这里若开启 StrictMode，
# launcher 及其调用的 envoy_cert.ps1 会继承它，原本允许的标量 `.Count` 会变成异常。
Assert-True ($batchAst.Body.Extent.Text -notmatch 'Set-StrictMode') `
    '[RED] batch coordinator 不得把 StrictMode 动态泄漏给旧基础设施 launcher'

$infraSource = [IO.File]::ReadAllText($infraPath)
$parseTokens = $null
$parseErrors = $null
$infraAst = [Management.Automation.Language.Parser]::ParseFile(
    $infraPath, [ref]$parseTokens, [ref]$parseErrors)
Assert-True (@($parseErrors).Count -eq 0) "local_infra.ps1 AST 解析失败:$($parseErrors -join '; ')"
$functionsByName = @{}
foreach ($functionAst in @($infraAst.FindAll({
    param($Node) $Node -is [Management.Automation.Language.FunctionDefinitionAst]
}, $true))) {
    $functionsByName[$functionAst.Name] = $functionAst
}

Assert-True ($infraSource -match "lib[/\\]planner_infra_fast_start\.ps1") `
    '[RED] local_infra 尚未加载策划基础设施并发 helper'
Assert-True $functionsByName.ContainsKey('Invoke-PlannerInfraFastStart') `
    '[RED] 缺少 Invoke-PlannerInfraFastStart 生产接线'
foreach ($name in @('Start-LocalMysql', 'Start-LocalRedis', 'Start-LocalKafka', 'Start-LocalEnvoy')) {
    Assert-True $functionsByName.ContainsKey($name) "缺少 $name"
    Assert-True ($functionsByName[$name].Body.Extent.Text -match '\[switch\]\s*\$DeferReady') `
        "[RED] $name 缺少仅供统一轮询使用的 DeferReady seam"
}
$fastBody = $functionsByName['Invoke-PlannerInfraFastStart'].Body.Extent.Text
foreach ($name in @('Mysql', 'Redis', 'Kafka', 'Envoy')) {
    Assert-True ($fastBody -match "Start-Local$name\s+-DeferReady") `
        "[RED] fast coordinator 没有批量拉起 $name"
}
Assert-True ($fastBody -match 'Invoke-PandoraPlannerInfraBatch') `
    '[RED] fast coordinator 没有进入统一 listener 轮询'
Assert-True ($fastBody -notmatch 'Start-Job|Start-ThreadJob|ForEach-Object\s+-Parallel') `
    'fast coordinator 禁止切换 runspace/job'
Assert-True ($functionsByName['New-PlannerInfraStartState'].Body.Extent.Text -match 'TimingStartedAtMilliseconds') `
    '[RED] 组件明细起点必须与 listener deadline 起点分离'
foreach ($name in @('Start-LocalMysql', 'Start-LocalRedis', 'Start-LocalKafka', 'Start-LocalEnvoy')) {
    Assert-True ($functionsByName[$name].Body.Extent.Text -match '-TimingStartedAtMilliseconds\s+\$componentStartedAt') `
        "[RED] $name 明细耗时必须包含进程拉起前的组件准备"
}

$upBody = $functionsByName['Invoke-Up'].Body.Extent.Text
$receiptIndex = $upBody.IndexOf('Test-PlannerPackageSetReady', [StringComparison]::Ordinal)
$provisionIndex = $upBody.IndexOf('Invoke-ProvisionAll', [StringComparison]::Ordinal)
Assert-True ($receiptIndex -ge 0 -and $provisionIndex -gt $receiptIndex) `
    '[RED] receipt hit 必须在 provision 可能新写 receipt 之前冻结'
Assert-True ($upBody -match 'Test-PandoraPlannerInfraBatchEligibility') `
    '[RED] Invoke-Up 没有使用 fast/Force/receipt/初始化资格闸'
Assert-True ($upBody -match 'Invoke-PlannerInfraFastStart') `
    '[RED] Invoke-Up 没有接入 fast coordinator'
Assert-True ($upBody -match 'if\s*\(-not\s+\$PlannerFastStart\)\s*\{\s*Invoke-Status\s*\}') `
    '[RED] 策划 fast 成功后不得再重复执行整套端口/归属状态查询'
foreach ($legacyCall in @('Start-LocalMysql', 'Start-LocalRedis', 'Start-LocalKafka', 'Start-LocalEnvoy')) {
    Assert-True ($upBody -match "Invoke-PandoraPlannerTimedStep\s+-Name\s+'[^']+'\s+-Action\s+\{\s*$legacyCall\s*\}") `
        "[RED] 普通/首次路径必须保留原串行调用:$legacyCall"
}

if (-not (Get-Command Invoke-PandoraPlannerInfraBatch -ErrorAction SilentlyContinue)) {
    throw '[RED] 缺少 Invoke-PandoraPlannerInfraBatch 能力 seam'
}
if (-not (Get-Command Test-PandoraPlannerInfraBatchEligibility -ErrorAction SilentlyContinue)) {
    throw '[RED] 缺少 Test-PandoraPlannerInfraBatchEligibility 资格判定 seam'
}
if (-not (Get-Command Test-PandoraPlannerInfraListenerOwnership -ErrorAction SilentlyContinue)) {
    throw '[RED] 缺少 Test-PandoraPlannerInfraListenerOwnership 精确归属 seam'
}

$eligibilityCases = @(
    [pscustomobject]@{ Name = 'local-current'; Fast = $true; Force = $false; Receipt = $true; Central = $false; Mysql = $true; Kafka = $true; Want = $true }
    [pscustomobject]@{ Name = 'ordinary-mode'; Fast = $false; Force = $false; Receipt = $true; Central = $false; Mysql = $true; Kafka = $true; Want = $false }
    [pscustomobject]@{ Name = 'force'; Fast = $true; Force = $true; Receipt = $true; Central = $false; Mysql = $true; Kafka = $true; Want = $false }
    [pscustomobject]@{ Name = 'receipt-miss'; Fast = $true; Force = $false; Receipt = $false; Central = $false; Mysql = $true; Kafka = $true; Want = $false }
    [pscustomobject]@{ Name = 'mysql-first-run'; Fast = $true; Force = $false; Receipt = $true; Central = $false; Mysql = $false; Kafka = $true; Want = $false }
    [pscustomobject]@{ Name = 'kafka-first-run'; Fast = $true; Force = $false; Receipt = $true; Central = $false; Mysql = $true; Kafka = $false; Want = $false }
    [pscustomobject]@{ Name = 'central-no-local-mysql'; Fast = $true; Force = $false; Receipt = $true; Central = $true; Mysql = $false; Kafka = $true; Want = $true }
)
foreach ($case in $eligibilityCases) {
    $actual = Test-PandoraPlannerInfraBatchEligibility -PlannerFastStart $case.Fast -Force $case.Force `
        -ReceiptReady $case.Receipt -CentralMysqlManaged $case.Central `
        -MysqlInitialized $case.Mysql -KafkaInitialized $case.Kafka
    Assert-Equal $case.Want $actual "资格判定错误:$($case.Name)"
}

$identityRows = @{
    501 = [pscustomobject]@{
        ProcessId = 501; ParentProcessId = 1
        ExecutablePath = 'C:\pandora\redis\redis-server.exe'
        CommandLine = 'redis-server.exe --port 6380 --dir C:\pandora\data\redis'
    }
    602 = [pscustomobject]@{
        ProcessId = 602; ParentProcessId = 601
        ExecutablePath = 'C:\pandora\jre\bin\java.exe'
        CommandLine = 'java.exe kafka.Kafka C:\pandora\cfg\kafka.properties'
    }
}
$getIdentity = { param([int]$ProcessId) return $identityRows[$ProcessId] }
$redisState = [pscustomobject]@{
    Name = 'redis'; Ports = @(6380); Process = [pscustomobject]@{ Id = 501 }
    ListenerOwnerKind = 'direct'
    ExpectedExecutable = 'C:\pandora\redis\redis-server.exe'
    RequiredCommandLineTokens = @('--port 6380', 'C:\pandora\data\redis')
}
$kafkaState = [pscustomobject]@{
    Name = 'kafka'; Ports = @(9093, 9094); Process = [pscustomobject]@{ Id = 601 }
    ListenerOwnerKind = 'child'
    ExpectedExecutable = 'C:\pandora\jre\bin\java.exe'
    RequiredCommandLineTokens = @('kafka.Kafka', 'C:\pandora\cfg\kafka.properties')
}
$ownershipListeners = @(
    [pscustomobject]@{ LocalPort = 6380; OwningProcess = 501 }
    [pscustomobject]@{ LocalPort = 9093; OwningProcess = 602 }
    [pscustomobject]@{ LocalPort = 9094; OwningProcess = 602 }
)
Assert-True (Test-PandoraPlannerInfraListenerOwnership -State $redisState `
    -Listeners $ownershipListeners -GetProcessIdentity $getIdentity) 'Redis direct PID/exe/参数应通过'
Assert-True (Test-PandoraPlannerInfraListenerOwnership -State $kafkaState `
    -Listeners $ownershipListeners -GetProcessIdentity $getIdentity) 'Kafka cmd→java 子进程/exe/参数/双端口应通过'

$missingKafkaController = @($ownershipListeners | Where-Object LocalPort -ne 9094)
Assert-True (-not (Test-PandoraPlannerInfraListenerOwnership -State $kafkaState `
    -Listeners $missingKafkaController -GetProcessIdentity $getIdentity)) 'Kafka 缺 controller listener 不得 ready'
$identityRows[602].ParentProcessId = 999
Assert-True (-not (Test-PandoraPlannerInfraListenerOwnership -State $kafkaState `
    -Listeners $ownershipListeners -GetProcessIdentity $getIdentity)) 'Kafka listener 不是本轮 wrapper 子进程不得 ready'
$identityRows[602].ParentProcessId = 601
$identityRows[602].ExecutablePath = 'C:\foreign\java.exe'
Assert-True (-not (Test-PandoraPlannerInfraListenerOwnership -State $kafkaState `
    -Listeners $ownershipListeners -GetProcessIdentity $getIdentity)) 'Kafka 外部 Java 不得冒充本轮 broker'
$identityRows[602].ExecutablePath = 'C:\pandora\jre\bin\java.exe'
$identityRows[602].CommandLine = 'java.exe another.Main C:\pandora\cfg\kafka.properties'
Assert-True (-not (Test-PandoraPlannerInfraListenerOwnership -State $kafkaState `
    -Listeners $ownershipListeners -GetProcessIdentity $getIdentity)) 'Kafka 命令行身份不符不得 ready'
$identityRows[602].CommandLine = 'java.exe kafka.Kafka C:\pandora\cfg\kafka.properties'

# 已安装、已初始化后的冷启动必须先拉起全部组件，再统一等待；总等待时间取最慢组件，
# 不能退化为逐项 4.3s + 3.1s + 17.5s 求和。所有依赖均为虚拟时钟/虚拟 listener，
# 本测试不会触碰本机端口、进程、PID 文件或 run/localinfra 数据。
$clock = [pscustomobject]@{ Milliseconds = [int64]0; Snapshots = 0; Launches = 0 }
$events = [Collections.Generic.List[string]]::new()
$runspaceId = [runspace]::DefaultRunspace.Id
$threadId = [Threading.Thread]::CurrentThread.ManagedThreadId
$readyAt = @{
    mysql = [int64]4300
    redis = [int64]3100
    kafka = [int64]17500
    envoy = [int64]2500
}
$definitions = @(
    [pscustomobject]@{ Name = 'mysql'; Ports = @(13307); ProcessId = 101; TimeoutMilliseconds = 90000 }
    [pscustomobject]@{ Name = 'redis'; Ports = @(6380); ProcessId = 102; TimeoutMilliseconds = 30000 }
    [pscustomobject]@{ Name = 'kafka'; Ports = @(9093, 9094); ProcessId = 103; TimeoutMilliseconds = 120000 }
    [pscustomobject]@{ Name = 'envoy'; Ports = @(8443, 8444); ProcessId = 104; TimeoutMilliseconds = 30000 }
)

$launchers = @($definitions | ForEach-Object {
    $definition = $_
    {
        if ([runspace]::DefaultRunspace.Id -ne $runspaceId -or
            [Threading.Thread]::CurrentThread.ManagedThreadId -ne $threadId) {
            throw 'launcher 没有在协调器当前 runspace/thread 执行'
        }
        $clock.Launches++
        $events.Add("launch:$($definition.Name)")
        return [pscustomobject]@{
            Name = $definition.Name
            Ports = @($definition.Ports)
            Process = [pscustomobject]@{ Id = $definition.ProcessId; Exited = $false; ExitCode = 0 }
            StartedAtMilliseconds = [int64]$clock.Milliseconds
            TimeoutMilliseconds = [int64]$definition.TimeoutMilliseconds
            Ready = $false
            Failure = ''
        }
    }.GetNewClosure()
})

$states = @(Invoke-PandoraPlannerInfraBatch -Launchers $launchers `
    -GetListenerRecords {
        if ($clock.Launches -ne $definitions.Count) {
            throw "首份 listener 快照前只拉起了 $($clock.Launches)/$($definitions.Count) 个组件"
        }
        $clock.Snapshots++
        $events.Add("snapshot:$($clock.Milliseconds)")
        $records = @()
        foreach ($definition in $definitions) {
            if ($clock.Milliseconds -lt $readyAt[$definition.Name]) { continue }
            foreach ($port in $definition.Ports) {
                $records += [pscustomobject]@{
                    LocalPort = [int]$port
                    OwningProcess = [int]$definition.ProcessId
                }
            }
        }
        return @($records)
    } -TestProcessExited {
        param($State)
        return [bool]$State.Process.Exited
    } -TestStateReady {
        param($State, [object[]]$Listeners)
        foreach ($port in @($State.Ports)) {
            if (-not @($Listeners | Where-Object {
                [int]$_.LocalPort -eq [int]$port -and
                [int]$_.OwningProcess -eq [int]$State.Process.Id
            })) { return $false }
        }
        return $true
    } -OnFailure {
        param($State, [string]$Reason)
        throw "虚拟组件不应失败:$($State.Name)/$Reason"
    } -Sleep {
        param([int]$Milliseconds)
        $clock.Milliseconds += $Milliseconds
    } -GetElapsedMilliseconds {
        return [int64]$clock.Milliseconds
    } -PollMilliseconds 100)

Assert-Equal 4 $clock.Launches '必须拉起四个虚拟组件'
Assert-Equal 4 $states.Count '必须返回四个组件状态'
Assert-True (@($states | Where-Object { -not $_.Ready }).Count -eq 0) '全部组件都应 ready'
Assert-Equal 17500 $clock.Milliseconds '批量等待总耗时必须等于最慢组件，而非逐项求和'
foreach ($state in $states) {
    Assert-Equal $readyAt[$state.Name] $state.ReadyAtMilliseconds "必须记录组件 ready 时刻:$($state.Name)"
    Assert-Equal $readyAt[$state.Name] $state.FinishedAtMilliseconds "成功组件完成时刻应等于 ready 时刻:$($state.Name)"
}
Assert-True ($clock.Snapshots -le 176) '每轮只能抓一份共享 listener 快照'
Assert-True ($events[0] -eq 'launch:mysql' -and $events[3] -eq 'launch:envoy' -and
    $events[4] -eq 'snapshot:0') '必须全部 launch 后才开始统一轮询'

# MySQL 一旦 ready，调用方即可启动 migration；不必等同批最慢 Kafka。全部状态、端口和
# 时间均为虚拟对象，不触碰本机进程或网络。
$dependencyClock = [pscustomobject]@{ Milliseconds = [int64]0 }
$dependencyEvents = [Collections.Generic.List[string]]::new()
$dependencyReadyAt = @{ mysql = [int64]200; kafka = [int64]700 }
$dependencyDefinitions = @(
    [pscustomobject]@{ Name = 'mysql'; Port = 13307; ProcessId = 151 }
    [pscustomobject]@{ Name = 'kafka'; Port = 9093; ProcessId = 152 }
)
$dependencyLaunchers = @($dependencyDefinitions | ForEach-Object {
    $definition = $_
    {
        [pscustomobject]@{
            Name = $definition.Name
            Ports = @($definition.Port)
            Process = [pscustomobject]@{ Id = $definition.ProcessId }
            StartedAtMilliseconds = [int64]0
            TimeoutMilliseconds = [int64]30000
            Ready = $false
            Failure = ''
        }
    }.GetNewClosure()
})
$dependencyStates = @(Invoke-PandoraPlannerInfraBatch -Launchers $dependencyLaunchers `
    -GetListenerRecords {
        $records = @()
        foreach ($definition in $dependencyDefinitions) {
            if ($dependencyClock.Milliseconds -ge $dependencyReadyAt[$definition.Name]) {
                $records += [pscustomobject]@{
                    LocalPort = $definition.Port
                    OwningProcess = $definition.ProcessId
                }
            }
        }
        return @($records)
    } -TestProcessExited { param($State) return $false } `
    -TestStateReady {
        param($State, $Listeners)
        return @($Listeners | Where-Object {
            $_.LocalPort -eq $State.Ports[0] -and $_.OwningProcess -eq $State.Process.Id
        }).Count -gt 0
    } -OnReady {
        param($State)
        $dependencyEvents.Add("ready:$($State.Name):$($dependencyClock.Milliseconds)")
        if ($State.Name -ceq 'mysql') {
            $dependencyEvents.Add("migration-start:$($dependencyClock.Milliseconds)")
        }
    } -OnFailure { param($State, $Reason) throw "依赖边虚拟组件不应失败:$($State.Name)/$Reason" } `
    -Sleep { param($Milliseconds) $dependencyClock.Milliseconds += $Milliseconds } `
    -GetElapsedMilliseconds { return [int64]$dependencyClock.Milliseconds } -PollMilliseconds 100)

Assert-Equal 200 (@($dependencyStates | Where-Object Name -eq 'mysql')[0].ReadyAtMilliseconds) `
    'MySQL 应在虚拟 200ms ready'
Assert-Equal 700 (@($dependencyStates | Where-Object Name -eq 'kafka')[0].ReadyAtMilliseconds) `
    'Kafka 应在虚拟 700ms ready'
Assert-Equal 1 @($dependencyEvents | Where-Object { $_ -ceq 'migration-start:200' }).Count `
    'MySQL ready callback 必须在 200ms 立即启动且只启动一次 migration'
$migrationStartIndex = $dependencyEvents.IndexOf('migration-start:200')
$kafkaReadyIndex = $dependencyEvents.IndexOf('ready:kafka:700')
Assert-True ($migrationStartIndex -ge 0 -and $kafkaReadyIndex -gt $migrationStartIndex) `
    'migration-start 必须发生在 Kafka 700ms ready 之前，不能退化为全基础设施 join 后迁移'

# ready callback 自身失败属于根因；StopOnFirstFailure 必须封存尚未 ready 的 sibling，
# 不能继续等 Kafka，也不能把 callback 异常误报成端口超时。
$callbackClock = [pscustomobject]@{ Milliseconds = [int64]0 }
$callbackFailures = [Collections.Generic.List[string]]::new()
$callbackStates = @(Invoke-PandoraPlannerInfraBatch -Launchers @(
    { [pscustomobject]@{ Name = 'mysql'; Ports = @(13307); Process = [pscustomobject]@{ Id = 161 }; StartedAtMilliseconds = [int64]0; TimeoutMilliseconds = [int64]30000; Ready = $false; Failure = '' } }
    { [pscustomobject]@{ Name = 'kafka'; Ports = @(9093); Process = [pscustomobject]@{ Id = 162 }; StartedAtMilliseconds = [int64]0; TimeoutMilliseconds = [int64]30000; Ready = $false; Failure = '' } }
) -GetListenerRecords {
    if ($callbackClock.Milliseconds -ge 200) {
        return @([pscustomobject]@{ LocalPort = 13307; OwningProcess = 161 })
    }
    return @()
} -TestProcessExited { param($State) return $false } `
    -TestStateReady {
        param($State, $Listeners)
        return @($Listeners | Where-Object {
            $_.LocalPort -eq $State.Ports[0] -and $_.OwningProcess -eq $State.Process.Id
        }).Count -gt 0
    } -OnReady { param($State) if ($State.Name -ceq 'mysql') { throw 'virtual-migration-start-failed' } } `
    -OnFailure { param($State, $Reason) $callbackFailures.Add("$($State.Name):$Reason") } `
    -Sleep { param($Milliseconds) $callbackClock.Milliseconds += $Milliseconds } `
    -GetElapsedMilliseconds { return [int64]$callbackClock.Milliseconds } `
    -StopOnFirstFailure -PollMilliseconds 100)

$failedCallbackMysql = @($callbackStates | Where-Object Name -eq 'mysql')[0]
$abortedCallbackKafka = @($callbackStates | Where-Object Name -eq 'kafka')[0]
Assert-Equal 200 $callbackClock.Milliseconds 'ready callback 失败后必须在当轮 200ms 立即收敛'
Assert-Equal 'ready-callback-failed' $failedCallbackMysql.Failure `
    'callback throw 必须标记为 ready-callback-failed'
Assert-True ($failedCallbackMysql.FailureException.Exception.Message -match 'virtual-migration-start-failed') `
    'callback 根因异常必须保留用于诊断'
Assert-Equal 'mysql:ready-callback-failed' $callbackFailures[0] `
    'OnFailure 必须收到 callback 根因组件和专用失败码'
Assert-Equal 'batch-aborted' $abortedCallbackKafka.Failure `
    'StopOnFirstFailure 必须把尚未 ready 的 sibling 显式封存为 batch-aborted'
Assert-Equal 200 $abortedCallbackKafka.FinishedAtMilliseconds `
    'sibling 封存必须记录 callback 失败当刻，不能继续等到 Kafka ready'

# 进程提前退出必须立刻按本组件失败，不能继续等满最长 120 秒。
$exitClock = [pscustomobject]@{ Milliseconds = [int64]0; Snapshots = 0 }
$exitFailures = [Collections.Generic.List[string]]::new()
$exitStates = @(Invoke-PandoraPlannerInfraBatch -Launchers @({
    [pscustomobject]@{
        Name = 'redis'; Ports = @(6380); Process = [pscustomobject]@{ Id = 202 }
        StartedAtMilliseconds = [int64]0; TimeoutMilliseconds = [int64]30000
        Ready = $false; Failure = ''
    }
}) -GetListenerRecords { $exitClock.Snapshots++; return @() } `
    -TestProcessExited { param($State) return $exitClock.Milliseconds -ge 200 } `
    -TestStateReady { param($State, $Listeners) return $false } `
    -OnFailure { param($State, $Reason) $exitFailures.Add("$($State.Name):$Reason") } `
    -Sleep { param($Milliseconds) $exitClock.Milliseconds += $Milliseconds } `
    -GetElapsedMilliseconds { return [int64]$exitClock.Milliseconds } -PollMilliseconds 100)
Assert-Equal 200 $exitClock.Milliseconds '进程退出应在下一轮立刻收敛'
Assert-Equal 'redis:process-exited' $exitFailures[0] '必须保留组件与退出原因'
Assert-Equal 'process-exited' $exitStates[0].Failure '状态必须 fail-closed'
Assert-Equal 200 $exitStates[0].FinishedAtMilliseconds '进程退出耗时必须来自注入虚拟时钟'

$abortClock = [pscustomobject]@{ Milliseconds = [int64]0 }
$abortStates = @(Invoke-PandoraPlannerInfraBatch -Launchers @(
    { [pscustomobject]@{ Name = 'redis'; Ports = @(6380); Process = [pscustomobject]@{ Id = 211 }; StartedAtMilliseconds = [int64]0; TimeoutMilliseconds = [int64]30000; Ready = $false; Failure = '' } }
    { [pscustomobject]@{ Name = 'kafka'; Ports = @(9093); Process = [pscustomobject]@{ Id = 212 }; StartedAtMilliseconds = [int64]0; TimeoutMilliseconds = [int64]120000; Ready = $false; Failure = '' } }
) -GetListenerRecords { return @() } `
    -TestProcessExited { param($State) return $State.Name -eq 'redis' -and $abortClock.Milliseconds -ge 200 } `
    -TestStateReady { param($State, $Listeners) return $false } `
    -OnFailure { param($State, $Reason) } `
    -Sleep { param($Milliseconds) $abortClock.Milliseconds += $Milliseconds } `
    -GetElapsedMilliseconds { return [int64]$abortClock.Milliseconds } -StopOnFirstFailure -PollMilliseconds 100)
Assert-Equal 200 $abortClock.Milliseconds '首个组件失败后必须立即收敛，不等 Kafka 长超时'
Assert-Equal 'process-exited' (@($abortStates | Where-Object Name -eq 'redis')[0].Failure) '根因组件必须保留真实失败'
Assert-Equal 'batch-aborted' (@($abortStates | Where-Object Name -eq 'kafka')[0].Failure) '同批未就绪组件必须显式封存为中止'
Assert-Equal 200 (@($abortStates | Where-Object Name -eq 'kafka')[0].FinishedAtMilliseconds) '同批中止组件必须保留实际已等待时间'

# 每个组件保留自己的 deadline；短超时 Redis 失败不应把 Kafka 的 120 秒边界改短，
# 也不能让 Kafka 的长边界反过来放宽 Redis。
$deadlineClock = [pscustomobject]@{ Milliseconds = [int64]0 }
$deadlineFailures = [Collections.Generic.List[string]]::new()
$deadlineStates = @(Invoke-PandoraPlannerInfraBatch -Launchers @(
    { [pscustomobject]@{ Name = 'redis'; Ports = @(6380); Process = [pscustomobject]@{ Id = 301 }; StartedAtMilliseconds = [int64]0; TimeoutMilliseconds = [int64]300; Ready = $false; Failure = '' } }
    { [pscustomobject]@{ Name = 'kafka'; Ports = @(9093); Process = [pscustomobject]@{ Id = 302 }; StartedAtMilliseconds = [int64]0; TimeoutMilliseconds = [int64]120000; Ready = $false; Failure = '' } }
) -GetListenerRecords {
    if ($deadlineClock.Milliseconds -ge 500) {
        return @([pscustomobject]@{ LocalPort = 9093; OwningProcess = 302 })
    }
    return @()
} -TestProcessExited { param($State) return $false } `
    -TestStateReady {
        param($State, $Listeners)
        return @($Listeners | Where-Object {
            $_.LocalPort -eq $State.Ports[0] -and $_.OwningProcess -eq $State.Process.Id
        }).Count -gt 0
    } -OnFailure { param($State, $Reason) $deadlineFailures.Add("$($State.Name):$Reason") } `
    -Sleep { param($Milliseconds) $deadlineClock.Milliseconds += $Milliseconds } `
    -GetElapsedMilliseconds { return [int64]$deadlineClock.Milliseconds } -PollMilliseconds 100)
Assert-Equal 500 $deadlineClock.Milliseconds '独立 deadline 后仍应等待未失败的 Kafka'
Assert-Equal 'ready-timeout' (@($deadlineStates | Where-Object Name -eq 'redis')[0].Failure) 'Redis 应按自己的短边界失败'
Assert-True (@($deadlineStates | Where-Object Name -eq 'kafka')[0].Ready) 'Kafka 应继续等待到自身 ready'
Assert-Equal 300 (@($deadlineStates | Where-Object Name -eq 'redis')[0].FinishedAtMilliseconds) 'Redis timeout 应锁在自身 deadline'
Assert-Equal 500 (@($deadlineStates | Where-Object Name -eq 'kafka')[0].FinishedAtMilliseconds) 'Kafka ready 应锁在自身完成时刻'
Assert-Equal 'redis:ready-timeout' $deadlineFailures[0] 'deadline 失败必须带组件名'

# listener 快照查询失败必须原样抛出，不能被解释成“端口还没开”而继续轮询。
$snapshotFailedClosed = $false
try {
    $null = Invoke-PandoraPlannerInfraBatch -Launchers @({
        [pscustomobject]@{ Name = 'envoy'; Ports = @(8443); Process = [pscustomobject]@{ Id = 401 }; StartedAtMilliseconds = [int64]0; TimeoutMilliseconds = [int64]30000; Ready = $false; Failure = '' }
    }) -GetListenerRecords { throw 'virtual-netstat-failed' } `
        -TestProcessExited { param($State) return $false } `
        -TestStateReady { param($State, $Listeners) return $false } `
        -OnFailure { param($State, $Reason) } -Sleep { param($Milliseconds) } `
        -GetElapsedMilliseconds { return [int64]0 }
} catch {
    $snapshotFailedClosed = $_.Exception.Message -match 'virtual-netstat-failed'
}
Assert-True $snapshotFailedClosed 'listener 快照失败必须 fail-closed'

Write-Host '[PASS] 策划基础设施并发启动能力契约通过' -ForegroundColor Green
