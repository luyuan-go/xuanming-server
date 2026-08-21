# 策划免 Docker 并行准备的 exact child 与生产接线契约。
#
# 不导表、不构建、不启动基础设施、数据库、业务进程或 K8s。

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
$helper = Join-Path $projectRoot 'tools/scripts/lib/planner_parallel_prepare.ps1'
$fastHelper = Join-Path $projectRoot 'tools/scripts/lib/planner_fast_start.ps1'
$devAllPath = Join-Path $projectRoot 'tools/scripts/dev_all.ps1'

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED: $Message" }
}

function Assert-Equal($Expected, $Actual, [string]$Message) {
    if ($Expected -ne $Actual) {
        throw "ASSERT FAILED: $Message（期望=$Expected，实际=$Actual）"
    }
}

Assert-True (Test-Path -LiteralPath $helper -PathType Leaf) `
    '应提供策划并行准备 exact child helper'
. $helper
Assert-True (Test-Path -LiteralPath $fastHelper -PathType Leaf) `
    '应提供配置表消费者权威集合 helper'
. $fastHelper

# 只装载两个纯身份函数，不能 dot-source dev_all.ps1（后者会获取锁并启动环境）。
$devAllTokens = $null
$devAllParseErrors = $null
$devAllAst = [Management.Automation.Language.Parser]::ParseFile(
    $devAllPath, [ref]$devAllTokens, [ref]$devAllParseErrors)
Assert-Equal 0 @($devAllParseErrors).Count 'dev_all.ps1 必须通过 AST 解析'
foreach ($functionName in @('Get-PlannerConfigTableDistIdentity', 'Get-PlannerConfigTableGenerationIdentity')) {
    $functionAst = @($devAllAst.FindAll({
            param($node)
            $node -is [Management.Automation.Language.FunctionDefinitionAst] -and
                $node.Name -ceq $functionName
        }, $true)) | Select-Object -First 1
    Assert-True ($null -ne $functionAst) "dev_all 缺少纯身份函数：$functionName"
    . ([scriptblock]::Create($functionAst.Extent.Text))
}

Write-Host '[1] 投机 build 判定：只允许稳定输入成功被接受，生成态变化最多重编一次' -ForegroundColor Cyan
$dispositionCases = @(
    [pscustomobject]@{ Name = 'drain-failed'; Changed = $false; ExitCode = 0; Drained = $false; Want = 'fail-unbounded' }
    [pscustomobject]@{ Name = 'generation-changed'; Changed = $true; ExitCode = 23; Drained = $true; Want = 'retry-stable-once' }
    [pscustomobject]@{ Name = 'stable-success'; Changed = $false; ExitCode = 0; Drained = $true; Want = 'accept' }
    [pscustomobject]@{ Name = 'stable-failure'; Changed = $false; ExitCode = 23; Drained = $true; Want = 'fail' }
)
foreach ($case in $dispositionCases) {
    $actual = Get-PandoraPlannerSpeculativeBuildDisposition `
        -GenerationIdentityChanged $case.Changed -ExitCode $case.ExitCode -DrainCompleted $case.Drained
    Assert-Equal $case.Want $actual "投机 build 判定错误：$($case.Name)"
}

Write-Host '[2] 导表双身份：纯 JSON 只重启 8 个消费者，不作废投机 Go build' -ForegroundColor Cyan
function Set-IdentityFixtureFile {
    param(
        [Parameter(Mandatory)][string]$Root,
        [Parameter(Mandatory)][string]$RelativePath,
        [Parameter(Mandatory)][string]$Content
    )
    $path = [IO.Path]::GetFullPath((Join-Path $Root $RelativePath))
    $directory = [IO.Path]::GetDirectoryName($path)
    $null = New-Item -ItemType Directory -Force -Path $directory
    [IO.File]::WriteAllText($path, $Content, [Text.UTF8Encoding]::new($false))
}

$realProjectRoot = $projectRoot
$identityFixtureRoot = Join-Path ([IO.Path]::GetTempPath()) (
    'pandora-planner-generation-identity-' + [guid]::NewGuid().ToString('N'))
try {
    $null = New-Item -ItemType Directory -Force -Path $identityFixtureRoot
    Set-IdentityFixtureFile $identityFixtureRoot 'configtable/dist/manifest.json' '{"version":1,"tables":[]}'
    Set-IdentityFixtureFile $identityFixtureRoot 'configtable/dist/item.json' '{"rows":[{"id":1}]}'
    Set-IdentityFixtureFile $identityFixtureRoot 'pkg/configtable/item_table.gen.go' "package configtable`n// table-v1`n"
    Set-IdentityFixtureFile $identityFixtureRoot 'pkg/configtable/tables.gen.go' "package configtable`n// registry-v1`n"
    Set-IdentityFixtureFile $identityFixtureRoot 'pkg/configtable/level_bitindex.gen.go' "package configtable`n// bitindex-v1`n"
    Set-IdentityFixtureFile $identityFixtureRoot 'pkg/configtable/item.go' "package configtable`n// companion-v1`n"
    Set-IdentityFixtureFile $identityFixtureRoot 'pkg/configtable/store.go' "package configtable`n// runtime-v1`n"

    $projectRoot = $identityFixtureRoot
    $distBefore = Get-PlannerConfigTableDistIdentity
    $generationBefore = Get-PlannerConfigTableGenerationIdentity

    Set-IdentityFixtureFile $identityFixtureRoot 'configtable/dist/manifest.json' '{"version":2,"tables":[{"name":"item"}]}'
    Set-IdentityFixtureFile $identityFixtureRoot 'configtable/dist/item.json' '{"rows":[{"id":2}]}'
    $distAfterDataOnly = Get-PlannerConfigTableDistIdentity
    $generationAfterDataOnly = Get-PlannerConfigTableGenerationIdentity
    Assert-True ($distBefore -cne $distAfterDataOnly) `
        '纯 JSON/manifest 变化必须改变 dist 身份，从而令 ConfigTableChanged=true'
    Assert-Equal $generationBefore $generationAfterDataOnly `
        '纯 JSON/manifest 变化不得改变 Go 生成态身份或作废首轮投机 build'

    Set-IdentityFixtureFile $identityFixtureRoot 'pkg/configtable/store.go' "package configtable`n// runtime-v2`n"
    Assert-Equal $generationBefore (Get-PlannerConfigTableGenerationIdentity) `
        '生成态身份只跟踪生成器可能写入的文件，不把其他手写运行时代码混入导表判定'

    foreach ($case in @(
            [pscustomobject]@{ Path = 'pkg/configtable/item_table.gen.go'; Body = "package configtable`n// table-v2`n"; Kind = '单表生成代码' }
            [pscustomobject]@{ Path = 'pkg/configtable/tables.gen.go'; Body = "package configtable`n// registry-v2`n"; Kind = '总注册生成代码' }
            [pscustomobject]@{ Path = 'pkg/configtable/level_bitindex.gen.go'; Body = "package configtable`n// bitindex-v2`n"; Kind = '位序生成代码' }
            [pscustomobject]@{ Path = 'pkg/configtable/item.go'; Body = "package configtable`n// companion-v2`n"; Kind = '缺失时创建的伴生桩' }
        )) {
        $target = Join-Path $identityFixtureRoot $case.Path
        $original = [IO.File]::ReadAllText($target)
        Set-IdentityFixtureFile $identityFixtureRoot $case.Path $case.Body
        Assert-True ($generationBefore -cne (Get-PlannerConfigTableGenerationIdentity)) `
            "$($case.Kind) 变化必须作废并在稳定输入上重编一次"
        Set-IdentityFixtureFile $identityFixtureRoot $case.Path $original
        Assert-Equal $generationBefore (Get-PlannerConfigTableGenerationIdentity) `
            "$($case.Kind) 恢复后身份必须回到原值"
    }

    $expectedConsumers = @(
        'battle_result', 'dialogue', 'ds_allocator', 'inventory', 'matchmaker',
        'matchmaker_pve', 'mission', 'player'
    ) | Sort-Object
    $actualConsumers = @(Get-PandoraPlannerConfigTableConsumerNames | Sort-Object)
    Assert-Equal 8 $actualConsumers.Count '纯数据变化必须只重启 8 个真实配置表消费者'
    Assert-Equal ($expectedConsumers -join ',') ($actualConsumers -join ',') `
        '配置表消费者集合必须保持精确，不能扩大成全服务重启'
} finally {
    $projectRoot = $realProjectRoot
    $resolvedFixture = [IO.Path]::GetFullPath($identityFixtureRoot)
    $resolvedTemp = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
    if ($resolvedFixture.StartsWith($resolvedTemp, [StringComparison]::OrdinalIgnoreCase) -and
        (Test-Path -LiteralPath $resolvedFixture -PathType Container)) {
        Remove-Item -LiteralPath $resolvedFixture -Recurse -Force
    }
}

Write-Host '[3] 生产 child：单项耗时取 exact ExitTime，不取父进程延迟 join 时刻' -ForegroundColor Cyan
function New-CompletedProcessFixture {
    param(
        [Parameter(Mandatory)][int]$Id,
        [Parameter(Mandatory)][DateTime]$StartedAtUtc,
        [Parameter(Mandatory)][DateTime]$ExitedAtUtc,
        [Parameter(Mandatory)][int]$ExitCode,
        [string]$Name = 'build',
        [switch]$Completed
    )

    $process = [pscustomobject][ordered]@{
        Id = $Id
        StartTime = $StartedAtUtc
        ExitTime = $ExitedAtUtc
        ExitCode = $ExitCode
        HasExited = $true
    }
    $process | Add-Member -MemberType ScriptMethod -Name WaitForExit -Value { param([int]$TimeoutMilliseconds) return $true }
    $process | Add-Member -MemberType ScriptMethod -Name Refresh -Value { }
    $stdoutSource = [Threading.Tasks.TaskCompletionSource[string]]::new()
    $stderrSource = [Threading.Tasks.TaskCompletionSource[string]]::new()
    $stdoutSource.SetResult('')
    $stderrSource.SetResult('')
    return [pscustomobject][ordered]@{
        Name = $Name
        Process = $process
        ProcessId = $Id
        StartedAtUtc = $StartedAtUtc
        # 故意伪造父进程 9 秒后才 join；修复前会错误报告约 9000ms。
        StartedAtMilliseconds = [int64]([Environment]::TickCount64 - 9000)
        StandardOutputTask = $stdoutSource.Task
        StandardErrorTask = $stderrSource.Task
        Completed = [bool]$Completed
    }
}

function New-TimeoutThenNaturalExitFixture {
    param(
        [Parameter(Mandatory)][int]$Id,
        [Parameter(Mandatory)][DateTime]$StartedAtUtc,
        [Parameter(Mandatory)][DateTime]$ExitedAtUtc
    )

    $process = [pscustomobject][ordered]@{
        Id = $Id
        StartTime = $StartedAtUtc
        ExitTime = $ExitedAtUtc
        ExitCode = 0
        HasExited = $false
        WaitCalls = 0
        KillCalls = 0
    }
    $process | Add-Member -MemberType ScriptMethod -Name WaitForExit -Value {
        param([int]$TimeoutMilliseconds)
        $this.WaitCalls++
        if ($this.WaitCalls -eq 1) { return $false }
        $this.HasExited = $true
        return $true
    }
    $process | Add-Member -MemberType ScriptMethod -Name Refresh -Value {
        # 模拟极窄 WaitForExit 超时后、Stop 阶段 Refresh 时进程已自然退出。
        if ($this.WaitCalls -ge 1) { $this.HasExited = $true }
    }
    $process | Add-Member -MemberType ScriptMethod -Name Kill -Value {
        param([bool]$EntireProcessTree)
        $this.KillCalls++
        $this.HasExited = $true
    }
    $stdoutSource = [Threading.Tasks.TaskCompletionSource[string]]::new()
    $stderrSource = [Threading.Tasks.TaskCompletionSource[string]]::new()
    $stdoutSource.SetResult('')
    $stderrSource.SetResult('')
    return [pscustomobject][ordered]@{
        Name = 'tables'
        Process = $process
        ProcessId = $Id
        StartedAtUtc = $StartedAtUtc
        StartedAtMilliseconds = [Environment]::TickCount64
        StandardOutputTask = $stdoutSource.Task
        StandardErrorTask = $stderrSource.Task
        Completed = $false
    }
}

$processStarted = [DateTime]::UtcNow.AddMinutes(-1)
$lateJoinHandle = New-CompletedProcessFixture -Id 43101 -StartedAtUtc $processStarted `
    -ExitedAtUtc $processStarted.AddSeconds(1) -ExitCode 0
$lateJoinResult = Complete-PandoraPlannerPreparationProcess -Handle $lateJoinHandle -TimeoutMilliseconds 10
Assert-True ($lateJoinResult.ElapsedMilliseconds -ge 995 -and $lateJoinResult.ElapsedMilliseconds -le 1005) `
    'build 实际 1s、父进程 9s 后 join 时，单项耗时仍必须约 1s'
Assert-Equal 43101 $lateJoinResult.ProcessId '完成结果必须保留原 exact Process PID 归属'
Assert-Equal 0 $lateJoinResult.ExitCode '正常完成必须保留原退出码'

$naturalExitHandle = New-TimeoutThenNaturalExitFixture -Id 43104 -StartedAtUtc $processStarted `
    -ExitedAtUtc $processStarted.AddMilliseconds(40)
$naturalExitWatch = [Diagnostics.Stopwatch]::StartNew()
$naturalExitResult = Complete-PandoraPlannerPreparationProcess -Handle $naturalExitHandle `
    -TimeoutMilliseconds 1 -OutputDrainTimeoutMilliseconds 25
$naturalExitWatch.Stop()
Assert-True ($naturalExitWatch.ElapsedMilliseconds -lt 500) `
    '极窄 timeout 后自然退出的竞态也必须有界收敛'
Assert-True $naturalExitResult.TimedOut '首次 WaitForExit=false 必须保留 TimedOut=true'
Assert-True $naturalExitResult.DrainCompleted `
    'Stop 阶段确认 exact Process 已自然退出且输出已关闭时可保留真实 DrainCompleted=true'
Assert-Equal -1 $naturalExitResult.ExitCode `
    '一旦超过公共 timeout，调用方 ExitCode 必须失败，不能接受随后观察到的自然 0 退出'
Assert-Equal 0 $naturalExitResult.ProcessExitCode `
    '真实自然退出码仍须作为 ProcessExitCode=0 的诊断证据保留'
Assert-Equal 0 $naturalExitHandle.Process.KillCalls `
    'Refresh 已证明进程自然退出时不得再发送 Kill'

$cancelledHandle = New-CompletedProcessFixture -Id 43102 -StartedAtUtc $processStarted `
    -ExitedAtUtc $processStarted.AddMilliseconds(650) -ExitCode 23 -Completed
$cancelledResult = Complete-PandoraPlannerPreparationProcess -Handle $cancelledHandle -TimeoutMilliseconds 10
Assert-Equal 650 $cancelledResult.ElapsedMilliseconds '已退出/已取消 handle 仍应使用 exact ExitTime'
Assert-Equal 23 $cancelledResult.ExitCode '已退出/已取消 handle 不得丢失原退出码'

$blockedPipeHandle = New-CompletedProcessFixture -Id 43103 -StartedAtUtc $processStarted `
    -ExitedAtUtc $processStarted.AddMilliseconds(800) -ExitCode 0 -Name tables -Completed
$neverClosedOutput = [Threading.Tasks.TaskCompletionSource[string]]::new()
$blockedPipeHandle.StandardOutputTask = $neverClosedOutput.Task
$boundedWatch = [Diagnostics.Stopwatch]::StartNew()
$blockedPipeResult = Complete-PandoraPlannerPreparationProcess -Handle $blockedPipeHandle `
    -TimeoutMilliseconds 10 -OutputDrainTimeoutMilliseconds 25
$boundedWatch.Stop()
Assert-True ($boundedWatch.ElapsedMilliseconds -lt 500) `
    'child 不关闭输出管道时必须在有界期限返回，不能永久 GetResult()'
Assert-True (-not $blockedPipeResult.DrainCompleted) '输出管道未关闭必须明确标记 DrainCompleted=false'
Assert-Equal -1 $blockedPipeResult.ExitCode `
    '导表进程即使真实退出码为 0，输出未 drain 时也必须向表结果调用方返回失败退出码'
Assert-Equal 0 $blockedPipeResult.ProcessExitCode '合成失败码之外仍保留 exact child 原退出码'
Assert-True ($blockedPipeResult.DrainError -match 'stdout 管道.*未关闭') '输出 drain 超时必须返回明确错误'

Write-Host '[4] MySQL-ready 回调：GetNewClosure 后仍能调用父脚本的 worker starter' -ForegroundColor Cyan
function Invoke-CallbackInChildScope([scriptblock]$Callback) {
    & {
        param([scriptblock]$InnerCallback)
        & $InnerCallback
    } $Callback
}

$unboundCommandError = & {
    function Invoke-ParentOnlyPlannerCommand { 'unbound-should-not-run' }
    $unboundCallback = { Invoke-ParentOnlyPlannerCommand }.GetNewClosure()
    try {
        Invoke-CallbackInChildScope $unboundCallback | Out-Null
    } catch {
        $_.Exception
    }
}
Assert-True ($unboundCommandError -is [Management.Automation.CommandNotFoundException] -and
    $unboundCommandError.CommandName -ceq 'Invoke-ParentOnlyPlannerCommand') `
    '回归前提：GetNewClosure 不会自动捕获父脚本中定义的函数命令'

$boundCommandResult = & {
    function Invoke-ParentOnlyPlannerCommand { 'bound-ok' }
    $boundPlannerCommand = ${function:Invoke-ParentOnlyPlannerCommand}
    $boundCallback = { & $boundPlannerCommand }.GetNewClosure()
    Invoke-CallbackInChildScope $boundCallback
}
Assert-Equal 'bound-ok' $boundCommandResult `
    '显式捕获函数 ScriptBlock 后，回调进入子脚本作用域仍必须可调用'

Write-Host '[5] 生产接线：只在策划 fast 路线并行，MySQL ready 后异步迁移' -ForegroundColor Cyan
$startText = [IO.File]::ReadAllText((Join-Path $projectRoot 'tools/scripts/start.ps1'))
$devAllText = [IO.File]::ReadAllText($devAllPath)
Assert-True ($startText -match '\$deferPlannerTableGeneration\s*=\s*\$plannerTimingEnabled' -and
    $startText -match '-GenerateTables:\(\$NoDocker -and \$env:PANDORA_PLANNER_FAST_START') `
    '只有 planner NoDocker fast 把导表下沉到并行准备，普通入口保持原顺序'

$tableStartIndex = $devAllText.IndexOf('$plannerTableHandle = Start-PandoraPlannerPreparationProcess', [StringComparison]::Ordinal)
$buildStartIndex = $devAllText.IndexOf('$plannerBuildHandle = Start-PandoraPlannerPreparationProcess -Name build ', [StringComparison]::Ordinal)
$infraStartIndex = $devAllText.IndexOf('& "$ScriptDir/local_infra.ps1" -Action up', [StringComparison]::Ordinal)
$tableJoinIndex = $devAllText.IndexOf('Complete-PandoraPlannerPreparationProcess -Handle $plannerTableHandle', [StringComparison]::Ordinal)
$buildJoinIndex = $devAllText.IndexOf('Complete-PandoraPlannerPreparationProcess -Handle $plannerBuildHandle', [StringComparison]::Ordinal)
$schemaIndex = $devAllText.IndexOf('===== [2/3] 数据库结构', [StringComparison]::Ordinal)
Assert-True ($tableStartIndex -ge 0 -and $buildStartIndex -ge 0 -and
    $tableStartIndex -lt $infraStartIndex -and $buildStartIndex -lt $infraStartIndex -and
    $tableJoinIndex -gt $infraStartIndex -and $buildJoinIndex -gt $infraStartIndex -and
    $tableJoinIndex -lt $schemaIndex -and $buildJoinIndex -lt $schemaIndex) `
    '导表/build 必须先 launch，再由父 runspace 启基础设施，三支路 join 后才进数据库结构阶段'
Assert-True ($devAllText -match '(?s)Get-PandoraPlannerSpeculativeBuildDisposition\s+.*?-GenerationIdentityChanged \$generationIdentityChanged.*?-ExitCode \$buildResult\.ExitCode.*?-DrainCompleted \$buildResult\.DrainCompleted') `
    'dev_all 必须通过纯判定函数决定投机 build 的接受、失败或稳定重编'
Assert-True ($devAllText -match '(?s)\$ConfigTableChanged\s*=\s*\$null -eq \$tableVersionAfter\s+-or\s+\$tableVersionBefore -cne \$tableVersionAfter') `
    'ConfigTableChanged 必须继续由 dist manifest 强身份决定'
$generationIdentityAst = @($devAllAst.FindAll({
        param($node)
        $node -is [Management.Automation.Language.FunctionDefinitionAst] -and
            $node.Name -ceq 'Get-PlannerConfigTableGenerationIdentity'
    }, $true)) | Select-Object -First 1
$generationIdentityText = $generationIdentityAst.Extent.Text
Assert-True ($generationIdentityText -notmatch 'configtable[/\\]dist|manifest\.json') `
    'Go 生成态身份不得包含纯数据 manifest'
Assert-True ($generationIdentityText -match "-Filter\s+'\*\.gen\.go'" -and
    $generationIdentityText.Contains('_table\.gen\.go') -and
    $generationIdentityText -match 'companion') `
    'Go 生成态身份必须覆盖全部 *.gen.go（含 tables/bitindex）及对应 companion'
Assert-True ($devAllText -match '(?s)if \(\$buildDisposition -ceq ''retry-stable-once''\).*?-Action discard.*?build-after-tables') `
    '导表改变 Go 生成物时必须丢弃投机 staging，并在稳定输入上重建'
Assert-True ($devAllText -match '(?s)-ConfigTableChanged:\$ConfigTableChanged.*?-PreparedBuildManifestPath \$plannerPreparedBuildManifest') `
    '业务激活必须同时接收表变化与已准备 staging manifest'
Assert-True ($devAllText -match '(?s)finally\s*\{.*?Stop-PandoraPlannerPreparationProcess.*?-Action discard') `
    '任一失败/exit 都必须 exact 取消 worker 并清理未消费 staging'
Assert-True ($devAllText -match '(?s)function Remove-PlannerPreparationOrphanStages.*?planner-stage-.*?\$workerPid.*?Remove-Item -LiteralPath \$candidate\.FullName' -and
    $devAllText -match '(?s)Stop-PandoraPlannerPreparationProcess.*?Remove-PlannerPreparationOrphanStages \$handle') `
    'worker 在 manifest 落盘前被强停时，也只能按 exact worker PID 清孤儿 staging'

$workerStarterCaptureIndex = $devAllText.IndexOf(
    '$startPlannerPreparationProcess = ${function:Start-PandoraPlannerPreparationProcess}',
    [StringComparison]::Ordinal)
$infraCallbackIndex = $devAllText.IndexOf('$infraReadyCallback = {', [StringComparison]::Ordinal)
Assert-True ($workerStarterCaptureIndex -ge 0 -and $workerStarterCaptureIndex -lt $infraCallbackIndex -and
    $devAllText -match '(?s)\$infraReadyCallback\s*=\s*\{.*?Name\)"\s*-cne\s*''mysql''.*?&\s*\$startPlannerPreparationProcess\s+-Name migration') `
    '本机 MySQL ready callback 必须显式捕获父脚本 worker starter，进入 local_infra 子作用域后再启动 migration child'
Assert-True ($devAllText -match '(?s)local_infra\.ps1"\s+-Action up\s+-OnPlannerComponentReady \$infraReadyCallback') `
    'planner fast 必须把 MySQL ready callback 传给 local_infra'

$parallelSchemaStart = $devAllText.IndexOf('} elseif ($plannerParallelEnabled) {', $schemaIndex,
    [StringComparison]::Ordinal)
$ordinarySchemaStart = $devAllText.IndexOf('} else {', $parallelSchemaStart,
    [StringComparison]::Ordinal)
Assert-True ($parallelSchemaStart -gt $schemaIndex -and $ordinarySchemaStart -gt $parallelSchemaStart) `
    '必须能定位 planner fast 数据库结构分支'
$parallelSchemaBody = $devAllText.Substring($parallelSchemaStart,
    $ordinarySchemaStart - $parallelSchemaStart)
Assert-True ($parallelSchemaBody -match '(?s)\$migrationHandle\s*=\s*\$plannerMigrationContext\.Handle.*?Complete-PandoraPlannerPreparationProcess\s+-Handle \$migrationHandle') `
    '数据库结构阶段必须 join MySQL ready callback 创建的 migration handle'
Assert-True ($parallelSchemaBody -notmatch '&\s*"\$ScriptDir/dev_migrate\.ps1"') `
    'planner fast 数据库结构阶段不得再次同步执行 dev_migrate'

Write-Host '[PASS] 策划并行准备 exact child/生产接线契约通过' -ForegroundColor Green
