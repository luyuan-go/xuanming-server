# 策划免 Docker 一键启动的构建复用 / 批量就绪契约。
#
# 全部使用临时文件、内存进程和虚拟时钟；不启停真实服务。

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ScriptsDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$ProjectRoot = (Resolve-Path (Join-Path $ScriptsDir '../..')).Path
$RunServices = Join-Path $ScriptsDir 'run_services.ps1'
$DevAll = Join-Path $ScriptsDir 'dev_all.ps1'
$PlannerCmd = Join-Path $ProjectRoot '策划一键启动-免Docker-测试版.cmd'
$FastLib = Join-Path $ScriptsDir 'lib/planner_fast_start.ps1'
$script:Failures = [Collections.Generic.List[string]]::new()

function Assert-True([bool]$Condition, [string]$Message) {
    if ($Condition) { Write-Host "  [ok] $Message" -ForegroundColor Green }
    else { $script:Failures.Add($Message); Write-Host "  [FAIL] $Message" -ForegroundColor Red }
}

Write-Host '[1] fast 功能必须只由策划免 Docker 入口开启' -ForegroundColor Cyan
$runText = [IO.File]::ReadAllText($RunServices)
$devAllText = [IO.File]::ReadAllText($DevAll)
$cmdText = [IO.File]::ReadAllText($PlannerCmd)
Assert-True ($cmdText -match 'set "PANDORA_PLANNER_FAST_START=1"') '策划 cmd 显式开启 fast flag'
Assert-True ($devAllText -match '(?s)if \(\$NoDocker\).*?-FastExistingProbe:\(\$env:PANDORA_PLANNER_FAST_START -eq ''1''\)') `
    '只有免 Docker 分支把环境变量映射为 run_services fast switch'
Assert-True ($runText -match 'if \(\$FastExistingProbe\)\s*\{\s*\$startFailed\s*=\s*@\(Start-PlannerFastServices') `
    'fast 分支调用独立的批量启动 seam'
Assert-True ($runText -match '(?s)else\s*\{\s*\$startFailed\s*=\s*@\(\)\s*foreach \(\$svc in \$targets\)') `
    '普通入口仍保留原来的逐服务启动路径'
$tokens = $null
$parseErrors = $null
$runAst = [Management.Automation.Language.Parser]::ParseFile($RunServices, [ref]$tokens, [ref]$parseErrors)
$fastFunction = $runAst.FindAll({
        param($node)
        return $node -is [Management.Automation.Language.FunctionDefinitionAst] -and
            $node.Name -eq 'Start-PlannerFastServices'
    }, $true) | Select-Object -First 1
$fastFunctionText = if ($fastFunction) { $fastFunction.Extent.Text } else { '' }
Assert-True ($fastFunctionText -match '(?s)\$beforeLogin\s*=.*?Name -ne ''login''.*?\$login\s*=.*?Name -eq ''login''.*?\$waves\.Add\(\$beforeLogin\).*?\$waves\.Add\(\$login\)') `
    'fast 启动先等非 login 波就绪，再启 login'
Assert-True ($fastFunctionText -match '(?s)foreach \(\$wave in \$waves\).*?\$waveListenerRecords\s*=\s*@\(Get-PandoraTcpListenerRecords\).*?Test-ServiceListenerOwned \$svc \$existing \$waveListenerRecords') `
    '每一波都用 fresh listener 快照复核已存活进程'
Assert-True ($fastFunctionText -match '(?s)if \(Test-ServiceListenerOwned \$svc \$existing \$waveListenerRecords\).*?else\s*\{.*?\$waveStates\.Add\(\$state\).*?\$states\.Add\(\$state\)') `
    '已有 exact PID 在 snapshot 后才 bind 时纳入统一 deadline 轮询，不立即假失败'
Assert-True ($fastFunctionText -match '(?s)\$finalListenerRecords\s*=\s*@\(Get-PandoraTcpListenerRecords\).*?foreach \(\$svc in \$targetsArray\).*?Test-ServiceListenerOwned \$svc \$proc \$finalListenerRecords') `
    '登记全局成功前再对全部目标做 fresh exact-PID 最终复核'
Assert-True ($fastFunctionText.IndexOf('$exe = Build-Service $svc', [StringComparison]::Ordinal) -ge 0 -and
    $fastFunctionText.IndexOf('$exe = Build-Service $svc', [StringComparison]::Ordinal) -lt
    $fastFunctionText.IndexOf('$runtimeConfig = Get-ServiceRuntimeConfig $svc', [StringComparison]::Ordinal)) `
    '所有冷 build 发生在生成 secret runtime 配置之前'
Assert-True ($fastFunctionText.IndexOf('Clear-PortSquatter $svc $prebuildListenerRecords', [StringComparison]::Ordinal) -ge 0 -and
    $fastFunctionText.IndexOf('Clear-PortSquatter $svc $prebuildListenerRecords', [StringComparison]::Ordinal) -lt
    $fastFunctionText.IndexOf('$exe = Build-Service $svc', [StringComparison]::Ordinal)) `
    '缺 pidfile 的 exact-exe 残留进程必须在覆盖二进制前清理'
$waveFallback = [regex]::Match($fastFunctionText, '(?s)if \(-not \$exe -or -not \(Test-Path.*?\)\) \{(?<body>.*?)\n\s*\}')
Assert-True ($waveFallback.Success -and
    $waveFallback.Groups['body'].Value.IndexOf('Clear-PortSquatter $svc $waveListenerRecords', [StringComparison]::Ordinal) -ge 0 -and
    $waveFallback.Groups['body'].Value.IndexOf('Clear-PortSquatter $svc $waveListenerRecords', [StringComparison]::Ordinal) -lt
    $waveFallback.Groups['body'].Value.IndexOf('$exe = Build-Service $svc', [StringComparison]::Ordinal)) `
    '预构建见存活、启动波又丢 pidfile 的竞态也必须先清 exact exe 再 build'
Assert-True ($runText -match 'if \(\$UseArtifacts\)\s*\{\s*throw "-UseArtifacts .*?拒绝与现场 go build 混用') `
    '显式 -UseArtifacts 缺包时 fail-closed，不混入现场 build'
Assert-True ($runText -match '(?s)Copy-Item -LiteralPath \$artifact -Destination \$stagedExe.*?Get-FileHash -LiteralPath \$stagedExe.*?\[IO\.File\]::Move\(\$stagedExe, \$exe, \$true\)') `
    '预编译产物校验 staging 的实际字节后才原子发布，不留 hash/copy TOCTOU'
$buildFingerprintFunction = $runAst.FindAll({
        param($node)
        $node -is [Management.Automation.Language.FunctionDefinitionAst] -and
            $node.Name -eq 'Get-PlannerBuildFingerprint'
    }, $true) | Select-Object -First 1
$goEnvCommand = if ($buildFingerprintFunction) {
    $buildFingerprintFunction.FindAll({
            param($node)
            $node -is [Management.Automation.Language.CommandAst] -and
                $node.GetCommandName() -ceq 'go' -and $node.CommandElements.Count -gt 1 -and
                $node.CommandElements[1].Extent.Text -ceq 'env'
        }, $true) | Select-Object -First 1
} else { $null }
$goEnvFields = if ($goEnvCommand) {
    @($goEnvCommand.CommandElements | Select-Object -Skip 2 | ForEach-Object { $_.Extent.Text.Trim() })
} else { @() }
$requiredGoEnvFields = @('GOROOT', 'GOOS', 'GOARCH', 'GOAMD64', 'CGO_ENABLED', 'GOFLAGS', 'GOEXPERIMENT', 'GOFIPS140', 'GOWORK', 'GOTOOLCHAIN')
Assert-True ($goEnvCommand -and @($requiredGoEnvFields | Where-Object { $goEnvFields -cnotcontains $_ }).Count -eq 0) `
    '构建收据 AST 精确绑定 GOROOT、目标架构、CGO、GOFLAGS、GOEXPERIMENT 与 GOFIPS140'
Assert-True ($runText -match '(?s)go work edit -json.*?\$workspace\.Use.*?-WorkspaceRoots \$workspaceRoots') `
    '构建指纹从 go.work 官方解析结果纳入全部 use 模块'
Assert-True ($fastFunctionText -match '(?s)\$beforeBuildFingerprint\s*=\s*\$script:PlannerBuildFingerprint.*?\$script:PlannerBuildFingerprint\s*=\s*''''.*?Get-PlannerBuildFingerprint.*?构建输入在批量 build/copy 期间发生变化') `
    '真实 build/copy 后必须重取强指纹，同步中变化则作废收据并拒绝启动'
$ordinaryStartFunction = $runAst.FindAll({
        param($node)
        $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Start-Service'
    }, $true) | Select-Object -First 1
$ordinaryStartText = if ($ordinaryStartFunction) { $ordinaryStartFunction.Extent.Text } else { '' }
Assert-True ($ordinaryStartText -match '\$launchFailure' -and
    $ordinaryStartText -match 'Remove-ServiceRuntimeConfigAfterLaunch[\s\S]*-AlwaysRollback:\(\$null -ne \$launchFailure\)') `
    '普通逐服务路径异常也通过统一 exact Process rollback seam'
Assert-True ($fastFunctionText -match 'AlwaysRollback\s*=\s*\$false' -and
    $fastFunctionText -match '\$runtimeRecord\.AlwaysRollback\s*=\s*\$true' -and
    $fastFunctionText -match 'Remove-ServiceRuntimeConfigAfterLaunch[\s\S]*-AlwaysRollback:\$record\.AlwaysRollback') `
    'fast wave launch 异常标记并通过同一 rollback seam，禁止吞 Stop/Wait 错误'

$desiredPlanFunction = $runAst.FindAll({
        param($node)
        $node -is [Management.Automation.Language.FunctionDefinitionAst] -and
            $node.Name -eq 'Get-PlannerDesiredTargetPlan'
    }, $true) | Select-Object -First 1
$desiredPlanText = if ($desiredPlanFunction) { $desiredPlanFunction.Extent.Text } else { '' }
[object[]]$goListCommands = if ($desiredPlanFunction) {
    @($desiredPlanFunction.FindAll({
                param($node)
                $node -is [Management.Automation.Language.CommandAst] -and
                    $node.GetCommandName() -ceq 'go' -and $node.Extent.Text -match '\blist\b'
            }, $true))
} else { @() }
Assert-True (@($goListCommands).Count -eq 1 -and $desiredPlanText -match '-mod=readonly' -and
    $desiredPlanText -match '-deps' -and $desiredPlanText -match '-f\s+\$packageTemplate') `
    '全部 main 合并成一次 go list -mod=readonly -deps 紧凑输出，不逐服务重复查询'
$stageFunction = $runAst.FindAll({
        param($node)
        $node -is [Management.Automation.Language.FunctionDefinitionAst] -and
            $node.Name -eq 'New-PlannerStagedTarget'
    }, $true) | Select-Object -First 1
$stageFunctionText = if ($stageFunction) { $stageFunction.Extent.Text } else { '' }
Assert-True ($stageFunctionText -match '(?s)go build.*?-mod=readonly.*?-buildvcs=false.*?-o\s+\$primaryStage') `
    '本机 Go staging 构建固定 -buildvcs=false/-mod=readonly，Python/文档 dirty 不会污染所有 target'
$firstPlanIndex = $fastFunctionText.IndexOf('$firstDesiredPlan = @(Get-PlannerDesiredTargetPlan', [StringComparison]::Ordinal)
$stageIndex = $fastFunctionText.IndexOf('New-PlannerStagedTarget', [StringComparison]::Ordinal)
$secondPlanIndex = $fastFunctionText.IndexOf('$secondDesiredPlan = @(Get-PlannerDesiredTargetPlan', [StringComparison]::Ordinal)
$stopReplacementIndex = $fastFunctionText.IndexOf('Stop-PlannerServiceForReplacement', [StringComparison]::Ordinal)
$publishIndex = $fastFunctionText.IndexOf('Publish-PandoraPlannerStagedFiles', [StringComparison]::Ordinal)
Assert-True ($firstPlanIndex -ge 0 -and $stageIndex -gt $firstPlanIndex -and
    $secondPlanIndex -gt $stageIndex -and $stopReplacementIndex -gt $secondPlanIndex -and
    $publishIndex -gt $stopReplacementIndex) `
    '先全部 staging、二次输入一致，再精确停旧进程并事务发布；build 失败不会先停服务'
Assert-True ($fastFunctionText -match '-ConfigTableChanged:\$ConfigTableChanged' -and
    $fastFunctionText -match '(?s)\$finalListenerRecords.*?Write-PandoraPlannerAppliedReceipt') `
    '表变化参与统一动作计划，applied receipt 只在最终 exact-ready 后写入'

Write-Host '[2] 批量 readiness 的虚拟时间只随最慢服务增长' -ForegroundColor Cyan
if (-not (Test-Path -LiteralPath $FastLib -PathType Leaf)) {
    throw "[RED] 缺少策划 fast helper:$FastLib"
}
$escapedFastLib = $FastLib.Replace("'", "''")
$strictLeakScript = "`$ErrorActionPreference='Stop'; . '$escapedFastLib'; `$null=([pscustomobject]@{Known=1}).Missing; 'STRICT_NOT_LEAKED'"
$strictLeakEncoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($strictLeakScript))
$strictLeakOutput = @(& pwsh -NoProfile -EncodedCommand $strictLeakEncoded 2>&1)
Assert-True ($LASTEXITCODE -eq 0 -and $strictLeakOutput.Count -gt 0 -and $strictLeakOutput[-1] -eq 'STRICT_NOT_LEAKED') `
    'dot-source fast helper 不得向普通 run_services 泄漏 StrictMode'
. $FastLib

Write-Host '[2a] 表变化通过公开 switch 精确映射到真实消费者' -ForegroundColor Cyan
$configTableParameter = @($runAst.ParamBlock.Parameters | Where-Object {
        $_.Name.VariablePath.UserPath -ceq 'ConfigTableChanged'
    }) | Select-Object -First 1
Assert-True ($null -ne $configTableParameter -and
    $configTableParameter.StaticType.FullName -ceq 'System.Management.Automation.SwitchParameter') `
    'run_services 暴露 [switch]$ConfigTableChanged'
$tableConsumerCommand = Get-Command Get-PandoraPlannerConfigTableConsumerNames -ErrorAction SilentlyContinue
Assert-True ($null -ne $tableConsumerCommand) 'fast helper 暴露策划表真实消费者集合'
if ($tableConsumerCommand) {
    $actualTableConsumers = @(Get-PandoraPlannerConfigTableConsumerNames | Sort-Object)
    $expectedTableConsumers = @(
        'battle_result', 'dialogue', 'ds_allocator', 'inventory', 'matchmaker',
        'matchmaker_pve', 'mission', 'player'
    ) | Sort-Object
    Assert-True (($actualTableConsumers -join ',') -ceq ($expectedTableConsumers -join ',')) `
        '表变化只重启 player/battle_result/ds_allocator/inventory/dialogue/mission/两个 matchmaker 实例'
}

Write-Host '[2b] artifact 逐 build-target 指纹，matchmaker 两实例共享一个目标' -ForegroundColor Cyan
$buildTargetsCommand = Get-Command Get-PandoraPlannerBuildTargets -ErrorAction SilentlyContinue
$artifactPlanCommand = Get-Command Get-PandoraPlannerArtifactTargetPlan -ErrorAction SilentlyContinue
Assert-True ($null -ne $buildTargetsCommand -and $null -ne $artifactPlanCommand) `
    'fast helper 暴露 build-target 分组与 artifact 计划 seam'
if ($buildTargetsCommand -and $artifactPlanCommand) {
    $targetFixtureServices = @(
        [pscustomobject]@{ Name = 'player'; Dir = 'services/account/player'; Cmd = 'player' },
        [pscustomobject]@{ Name = 'matchmaker'; Dir = 'services/matchmaking/matchmaker'; Cmd = 'matchmaker'; BuildTarget = 'matchmaker' },
        [pscustomobject]@{ Name = 'matchmaker_pve'; Dir = 'services/matchmaking/matchmaker'; Cmd = 'matchmaker'; BuildTarget = 'matchmaker' }
    )
    $targetFixtures = @(Get-PandoraPlannerBuildTargets -Services $targetFixtureServices)
    $matchmakerTarget = @($targetFixtures | Where-Object Name -ceq 'matchmaker') | Select-Object -First 1
    Assert-True ($targetFixtures.Count -eq 2 -and $matchmakerTarget -and
        @($matchmakerTarget.Services).Count -eq 2) `
        'matchmaker/matchmaker_pve 是一个 build target、两个 runtime 实例'

    $manifestA = [pscustomobject]@{ binaries = @(
            [pscustomobject]@{ name = 'player'; size = 11; sha256 = ('A' * 64) },
            [pscustomobject]@{ name = 'matchmaker'; size = 22; sha256 = ('B' * 64) },
            [pscustomobject]@{ name = 'matchmaker_pve'; size = 22; sha256 = ('B' * 64) }
        ) }
    $artifactPlanA = @(Get-PandoraPlannerArtifactTargetPlan -BuildTargets $targetFixtures -Manifest $manifestA)
    $manifestB = [pscustomobject]@{ binaries = @(
            [pscustomobject]@{ name = 'player'; size = 12; sha256 = ('C' * 64) },
            [pscustomobject]@{ name = 'matchmaker'; size = 22; sha256 = ('B' * 64) },
            [pscustomobject]@{ name = 'matchmaker_pve'; size = 22; sha256 = ('B' * 64) }
        ) }
    $artifactPlanB = @(Get-PandoraPlannerArtifactTargetPlan -BuildTargets $targetFixtures -Manifest $manifestB)
    $artifactAByName = @{}; foreach ($item in $artifactPlanA) { $artifactAByName[$item.Name] = $item }
    $artifactBByName = @{}; foreach ($item in $artifactPlanB) { $artifactBByName[$item.Name] = $item }
    Assert-True ($artifactAByName.player.Fingerprint -cne $artifactBByName.player.Fingerprint -and
        $artifactAByName.matchmaker.Fingerprint -ceq $artifactBByName.matchmaker.Fingerprint) `
        'manifest 单 entry 变化只使对应 build target 过期，不绑定整份 manifest'

    $splitManifest = [pscustomobject]@{ binaries = @(
            [pscustomobject]@{ name = 'player'; size = 11; sha256 = ('A' * 64) },
            [pscustomobject]@{ name = 'matchmaker'; size = 22; sha256 = ('B' * 64) },
            [pscustomobject]@{ name = 'matchmaker_pve'; size = 23; sha256 = ('D' * 64) }
        ) }
    $splitBlocked = $false
    try { $null = @(Get-PandoraPlannerArtifactTargetPlan -BuildTargets $targetFixtures -Manifest $splitManifest) }
    catch { $splitBlocked = $_.Exception.Message -match '混版|不一致' }
    Assert-True $splitBlocked '共享 build target 的两个 artifact entry 不一致时 fail-closed，禁止混版'
}

Write-Host '[2c] exact Process 回收必须有界并由 Refresh/HasExited 证明' -ForegroundColor Cyan
$exactStopFunctionText = ${function:Stop-PandoraPlannerExactProcess}.ToString()
Assert-True ($exactStopFunctionText -match '\$ExactProcess\.Kill\(' -and
    $exactStopFunctionText -notmatch '\bStop-Process\b') `
    '生产默认停止 seam 只调用原 Process.Kill，不按可复用 PID 重新 lookup'
$rollbackState = @{ Now = 0; Exited = $false; StopObjects = [Collections.Generic.List[object]]::new() }
$fakeProcess = [pscustomobject]@{ Id = 43210; HasExited = $false; State = $rollbackState; RefreshCount = 0 }
$fakeProcess | Add-Member -MemberType ScriptMethod -Name Refresh -Value {
    $this.RefreshCount++
    $this.HasExited = [bool]$this.State.Exited
}
$stopExact = {
    param($ExactProcess)
    $rollbackState.StopObjects.Add($ExactProcess)
    $rollbackState.Exited = $true
}
$rollbackSleep = { param([int]$Milliseconds) $rollbackState.Now += $Milliseconds }
$rollbackElapsed = { return [int64]$rollbackState.Now }
$reclaimed = Stop-PandoraPlannerExactProcess -Process $fakeProcess -StopProcess $stopExact `
    -Sleep $rollbackSleep -GetElapsedMilliseconds $rollbackElapsed -TimeoutMilliseconds 500
Assert-True ($reclaimed.ExitConfirmed -and $rollbackState.StopObjects.Count -eq 1 -and
    [object]::ReferenceEquals($rollbackState.StopObjects[0], $fakeProcess) -and
    $fakeProcess.RefreshCount -gt 0) '只向原 Process 对象发停止请求，并经 Refresh/HasExited 确认退出'

$rollbackState = @{ Now = 0; Exited = $false; StopObjects = [Collections.Generic.List[object]]::new() }
$stuckProcess = [pscustomobject]@{ Id = 43211; HasExited = $false; State = $rollbackState; RefreshCount = 0 }
$stuckProcess | Add-Member -MemberType ScriptMethod -Name Refresh -Value {
    $this.RefreshCount++
    $this.HasExited = [bool]$this.State.Exited
}
$stopStuck = { param($ExactProcess) $rollbackState.StopObjects.Add($ExactProcess) }
$rollbackSleep = { param([int]$Milliseconds) $rollbackState.Now += $Milliseconds }
$rollbackElapsed = { return [int64]$rollbackState.Now }
$notReclaimed = Stop-PandoraPlannerExactProcess -Process $stuckProcess -StopProcess $stopStuck `
    -Sleep $rollbackSleep -GetElapsedMilliseconds $rollbackElapsed -TimeoutMilliseconds 500
Assert-True (-not $notReclaimed.ExitConfirmed -and $rollbackState.Now -le 500 -and
    $rollbackState.StopObjects.Count -eq 1 -and
    [object]::ReferenceEquals($rollbackState.StopObjects[0], $stuckProcess)) `
    '无法确认退出时在有界期限返回失败证据，不猜测已回收'

Write-Host '[2d] PID 在 Refresh→Stop 间复用也只能杀原 Process handle' -ForegroundColor Cyan
$pidReuseState = @{
    Now = 0; Exited = $false; ReusedBeforeStop = $false
    OriginalKills = 0; ReplacementKills = 0
}
$originalProcess = [pscustomobject]@{
    Id = 43212; HasExited = $false; State = $pidReuseState; RefreshCount = 0
}
$originalProcess | Add-Member -MemberType ScriptMethod -Name Refresh -Value {
    $this.RefreshCount++
    if ($this.RefreshCount -eq 1) { $this.State.ReusedBeforeStop = $true }
    $this.HasExited = [bool]$this.State.Exited
}
$originalProcess | Add-Member -MemberType ScriptMethod -Name Kill -Value {
    $this.State.OriginalKills++
    $this.State.Exited = $true
}
$previousStopProcessFunction = Get-Item -LiteralPath function:Stop-Process -ErrorAction SilentlyContinue
try {
    # 安全 mutant：旧实现若按 ID lookup，会命中这个“复用 PID 的替代进程”桩；绝不触碰真实进程。
    function Stop-Process {
        [CmdletBinding()]
        param([Parameter(Mandatory)][int]$Id, [switch]$Force)
        $pidReuseState.ReplacementKills++
    }
    $pidReuseSleep = { param([int]$Milliseconds) $pidReuseState.Now += $Milliseconds }
    $pidReuseElapsed = { return [int64]$pidReuseState.Now }
    $pidReuseResult = Stop-PandoraPlannerExactProcess -Process $originalProcess `
        -Sleep $pidReuseSleep -GetElapsedMilliseconds $pidReuseElapsed -TimeoutMilliseconds 500
    Assert-True ($pidReuseState.ReusedBeforeStop -and $pidReuseResult.ExitConfirmed -and
        $pidReuseState.OriginalKills -eq 1 -and $pidReuseState.ReplacementKills -eq 0) `
        '确定性 mutant：PID 已复用时仍只 Kill 原对象，绝不调用 Stop-Process -Id'
} finally {
    if ($previousStopProcessFunction) {
        Set-Item -LiteralPath function:Stop-Process -Value $previousStopProcessFunction.ScriptBlock
    } else {
        Remove-Item -LiteralPath function:Stop-Process -Force -ErrorAction SilentlyContinue
    }
}

$clock = @{ Now = 0; Snapshots = 0; Sleeps = 0 }
$states = @(1..22 | ForEach-Object {
        [pscustomobject][ordered]@{
            Name = "svc$_"
            Port = 20000 + $_
            ProcessId = 30000 + $_
            ReadyAtMilliseconds = 400
            Ready = $false
            Failure = ''
        }
    })
$getListeners = {
    $clock.Snapshots++
    return @($states | Where-Object { $clock.Now -ge $_.ReadyAtMilliseconds } | ForEach-Object {
            [pscustomobject]@{ LocalPort = $_.Port; OwningProcess = $_.ProcessId }
        })
}
$isExited = { param($state) return $false }
$isOwned = {
    param($state, $listeners)
    return [bool]@($listeners | Where-Object {
            [int]$_.LocalPort -eq [int]$state.Port -and [int]$_.OwningProcess -eq [int]$state.ProcessId
        })
}
$sleep = { param([int]$milliseconds) $clock.Sleeps++; $clock.Now += $milliseconds }
$elapsed = { return [int]$clock.Now }

Wait-PandoraPlannerServiceBatch -States $states -GetListenerRecords $getListeners `
    -TestProcessExited $isExited -TestListenerOwned $isOwned -Sleep $sleep -GetElapsedMilliseconds $elapsed `
    -PollMilliseconds 100 -TimeoutMilliseconds 12000

Assert-True (@($states | Where-Object { -not $_.Ready }).Count -eq 0) '22 个服务全部被 exact PID listener 判定 ready'
Assert-True ($clock.Now -le 500) "虚拟总耗时 <=500ms（实际 $($clock.Now)ms；串行旧实现为 8800ms）"
Assert-True ($clock.Snapshots -le 6) "listener 快照按轮询次数取得，不乘 22（实际 $($clock.Snapshots) 次）"

Write-Host '[3] login 必须在前置波 exact-ready 后才能启动' -ForegroundColor Cyan
$clock = @{ Now = 0; Snapshots = 0; Sleeps = 0 }
$waveModel = @{ States = @() }
$waveModel.States = @(1..21 | ForEach-Object {
        [pscustomobject][ordered]@{
            Name = "dependency$_"; Port = 22000 + $_; ProcessId = 42000 + $_
            ReadyAtMilliseconds = 400; Ready = $false; Failure = ''
        }
    })
$getWaveListeners = {
    $clock.Snapshots++
    return @($waveModel.States | Where-Object { $clock.Now -ge $_.ReadyAtMilliseconds } | ForEach-Object {
            [pscustomobject]@{ LocalPort = $_.Port; OwningProcess = $_.ProcessId }
        })
}
$waveSleep = { param([int]$milliseconds) $clock.Sleeps++; $clock.Now += $milliseconds }
$waveElapsedBase = 0
$waveElapsed = { return [int]($clock.Now - $waveElapsedBase) }
Wait-PandoraPlannerServiceBatch -States $waveModel.States -GetListenerRecords $getWaveListeners `
    -TestProcessExited $isExited -TestListenerOwned $isOwned -Sleep $waveSleep -GetElapsedMilliseconds $waveElapsed `
    -PollMilliseconds 100 -TimeoutMilliseconds 12000
$loginLaunchedAt = $clock.Now
$waveModel.States = @([pscustomobject][ordered]@{
        Name = 'login'; Port = 20001; ProcessId = 43001
        ReadyAtMilliseconds = $clock.Now + 400; Ready = $false; Failure = ''
    })
$waveElapsedBase = $clock.Now
Wait-PandoraPlannerServiceBatch -States $waveModel.States -GetListenerRecords $getWaveListeners `
    -TestProcessExited $isExited -TestListenerOwned $isOwned -Sleep $waveSleep -GetElapsedMilliseconds $waveElapsed `
    -PollMilliseconds 100 -TimeoutMilliseconds 12000
Assert-True ($loginLaunchedAt -ge 400) "login 启动时刻不早于前置波 ready（$($loginLaunchedAt)ms）"
Assert-True ($waveModel.States[0].Ready) 'login 仍须通过 exact PID listener 就绪复核'
Assert-True ($clock.Now -le 900) "两波总虚拟时间 <=900ms（实际 $($clock.Now)ms；串行旧实现 8800ms）"

Write-Host '[4] 退出、超时和错误 PID 必须 fail-closed' -ForegroundColor Cyan
$clock = @{ Now = 0; Snapshots = 0; Sleeps = 0 }
$badStates = @(
    [pscustomobject][ordered]@{ Name = 'exited'; Port = 21001; ProcessId = 41001; ReadyAtMilliseconds = 0; Ready = $false; Failure = '' },
    [pscustomobject][ordered]@{ Name = 'wrong-owner'; Port = 21002; ProcessId = 41002; ReadyAtMilliseconds = 0; Ready = $false; Failure = '' },
    [pscustomobject][ordered]@{ Name = 'never-ready'; Port = 21003; ProcessId = 41003; ReadyAtMilliseconds = 99999; Ready = $false; Failure = '' }
)
$getBadListeners = {
    $clock.Snapshots++
    return @([pscustomobject]@{ LocalPort = 21002; OwningProcess = 99999 })
}
$badExited = { param($state) return ($state.Name -eq 'exited') }
$badOwned = {
    param($state, $listeners)
    return [bool]@($listeners | Where-Object {
            [int]$_.LocalPort -eq [int]$state.Port -and [int]$_.OwningProcess -eq [int]$state.ProcessId
        })
}
$badSleep = { param([int]$milliseconds) $clock.Sleeps++; $clock.Now += $milliseconds }
$badElapsed = { return [int]$clock.Now }
Wait-PandoraPlannerServiceBatch -States $badStates -GetListenerRecords $getBadListeners `
    -TestProcessExited $badExited -TestListenerOwned $badOwned -Sleep $badSleep -GetElapsedMilliseconds $badElapsed `
    -PollMilliseconds 100 -TimeoutMilliseconds 500
Assert-True ($badStates[0].Failure -eq 'process-exited') '进程提前退出被精确标记失败'
Assert-True ($badStates[1].Failure -eq 'ready-timeout') '同端口错误 PID 不能冒充 ready'
Assert-True ($badStates[2].Failure -eq 'ready-timeout') '永不 ready 的服务在全局 deadline 失败'
Assert-True ($clock.Now -le 500) '全局 timeout 不会乘以服务数量'

Write-Host '[5] build receipt 必须绑定输入指纹和目标二进制身份' -ForegroundColor Cyan
$tmp = Join-Path ([IO.Path]::GetTempPath()) ("pandora-planner-fast-test-{0}" -f [guid]::NewGuid().ToString('N'))
try {
    New-Item -ItemType Directory -Force -Path $tmp | Out-Null
    $binary = Join-Path $tmp 'svc.exe'
    $receipt = Join-Path $tmp 'svc.json'
    [IO.File]::WriteAllBytes($binary, [byte[]](1, 2, 3, 4))
    Write-PandoraPlannerBuildReceipt -ReceiptPath $receipt -Fingerprint 'source-a' -BinaryPath $binary
    Assert-True (Test-PandoraPlannerBuildReceipt -ReceiptPath $receipt -Fingerprint 'source-a' -BinaryPath $binary) `
        '相同输入和同一二进制可复用'
    Assert-True (-not (Test-PandoraPlannerBuildReceipt -ReceiptPath $receipt -Fingerprint 'source-b' -BinaryPath $binary)) `
        '源码/工具链指纹变化会强制重建'
    [IO.File]::WriteAllBytes($binary, [byte[]](1, 2, 3, 4, 5))
    Assert-True (-not (Test-PandoraPlannerBuildReceipt -ReceiptPath $receipt -Fingerprint 'source-a' -BinaryPath $binary)) `
        '目标二进制身份变化不能命中旧收据'

    Write-Host '[5a] build/applied 双收据驱动 0-build/0-stop/按需 start 计划' -ForegroundColor Cyan
    $binaryPeer = Join-Path $tmp 'svc-peer.exe'
    [IO.File]::WriteAllBytes($binary, [byte[]](9, 8, 7))
    [IO.File]::WriteAllBytes($binaryPeer, [byte[]](9, 8, 7))
    Write-PandoraPlannerBuildReceipt -ReceiptPath $receipt -Fingerprint 'target-shared' `
        -BinaryPaths @($binary, $binaryPeer)
    Assert-True (Test-PandoraPlannerBuildReceipt -ReceiptPath $receipt -Fingerprint 'target-shared' `
            -BinaryPaths @($binary, $binaryPeer)) '逐 build-target 收据绑定共享目标的全部 runtime 二进制'
    [IO.File]::WriteAllBytes($binaryPeer, [byte[]](9, 8, 7, 6))
    Assert-True (-not (Test-PandoraPlannerBuildReceipt -ReceiptPath $receipt -Fingerprint 'target-shared' `
                -BinaryPaths @($binary, $binaryPeer))) '共享目标任一 runtime 二进制漂移都会使 build 收据失效'

    $appliedReceiptCommand = Get-Command Write-PandoraPlannerAppliedReceipt -ErrorAction SilentlyContinue
    $testAppliedCommand = Get-Command Test-PandoraPlannerAppliedReceipt -ErrorAction SilentlyContinue
    $actionPlanCommand = Get-Command Get-PandoraPlannerRuntimeActionPlan -ErrorAction SilentlyContinue
    Assert-True ($appliedReceiptCommand -and $testAppliedCommand -and $actionPlanCommand) `
        'fast helper 暴露 applied receipt 与纯动作计划 seam'
    if ($appliedReceiptCommand -and $testAppliedCommand -and $actionPlanCommand) {
        $appliedReceipt = Join-Path $tmp 'svc.applied.json'
        $startedAt = [datetime]::new(2026, 8, 20, 1, 2, 3, [DateTimeKind]::Utc)
        $fakeAppliedProcess = [pscustomobject]@{ Id = 71001; StartTime = $startedAt }
        Write-PandoraPlannerAppliedReceipt -ReceiptPath $appliedReceipt -Fingerprint 'target-a' `
            -BinaryPath $binary -Process $fakeAppliedProcess
        Assert-True (Test-PandoraPlannerAppliedReceipt -ReceiptPath $appliedReceipt -Fingerprint 'target-a' `
                -BinaryPath $binary -Process $fakeAppliedProcess) `
            'applied 收据同时证明 desired 指纹、二进制身份与 exact 进程代次'
        $replacementProcess = [pscustomobject]@{ Id = 71001; StartTime = $startedAt.AddSeconds(1) }
        Assert-True (-not (Test-PandoraPlannerAppliedReceipt -ReceiptPath $appliedReceipt -Fingerprint 'target-a' `
                    -BinaryPath $binary -Process $replacementProcess)) `
            'PID 复用但 StartTime 不同不能冒充已应用目标版本'

        $planTargets = @(Get-PandoraPlannerBuildTargets -Services @(
                [pscustomobject]@{ Name = 'player'; Dir = 'services/account/player'; Cmd = 'player' },
                [pscustomobject]@{ Name = 'matchmaker'; Dir = 'services/matchmaking/matchmaker'; Cmd = 'matchmaker'; BuildTarget = 'matchmaker' },
                [pscustomobject]@{ Name = 'matchmaker_pve'; Dir = 'services/matchmaking/matchmaker'; Cmd = 'matchmaker'; BuildTarget = 'matchmaker' },
                [pscustomobject]@{ Name = 'login'; Dir = 'services/account/login'; Cmd = 'login' }
            ))
        $currentTargetStates = @(
            [pscustomobject]@{ Name = 'player'; BuildCurrent = $true },
            [pscustomobject]@{ Name = 'matchmaker'; BuildCurrent = $true },
            [pscustomobject]@{ Name = 'login'; BuildCurrent = $true }
        )
        $allRunningCurrent = @(
            [pscustomobject]@{ Name = 'player'; TargetName = 'player'; IsRunning = $true; AppliedCurrent = $true },
            [pscustomobject]@{ Name = 'matchmaker'; TargetName = 'matchmaker'; IsRunning = $true; AppliedCurrent = $true },
            [pscustomobject]@{ Name = 'matchmaker_pve'; TargetName = 'matchmaker'; IsRunning = $true; AppliedCurrent = $true },
            [pscustomobject]@{ Name = 'login'; TargetName = 'login'; IsRunning = $true; AppliedCurrent = $true }
        )
        $hotPlan = Get-PandoraPlannerRuntimeActionPlan -BuildTargets $planTargets `
            -TargetStates $currentTargetStates -RuntimeStates $allRunningCurrent
        Assert-True (@($hotPlan.BuildTargetNames).Count -eq 0 -and @($hotPlan.StopRuntimeNames).Count -eq 0 -and
            @($hotPlan.StartRuntimeNames).Count -eq 0) `
            '无变化且全运行时为 0 build / 0 stop / 0 start'

        $afterRebootStates = @($allRunningCurrent | ForEach-Object {
                [pscustomobject]@{ Name = $_.Name; TargetName = $_.TargetName; IsRunning = $false; AppliedCurrent = $false }
            })
        $rebootPlan = Get-PandoraPlannerRuntimeActionPlan -BuildTargets $planTargets `
            -TargetStates $currentTargetStates -RuntimeStates $afterRebootStates
        Assert-True (@($rebootPlan.BuildTargetNames).Count -eq 0 -and @($rebootPlan.StopRuntimeNames).Count -eq 0 -and
            @($rebootPlan.StartRuntimeNames).Count -eq 4) `
            '重启电脑后收据命中只启动全体，不重新 build'

        $partialStates = @($allRunningCurrent | ForEach-Object {
                [pscustomobject]@{
                    Name = $_.Name; TargetName = $_.TargetName
                    IsRunning = ($_.Name -cne 'login'); AppliedCurrent = ($_.Name -cne 'login')
                }
            })
        $partialPlan = Get-PandoraPlannerRuntimeActionPlan -BuildTargets $planTargets `
            -TargetStates $currentTargetStates -RuntimeStates $partialStates
        Assert-True ((@($partialPlan.StartRuntimeNames) -join ',') -ceq 'login' -and
            @($partialPlan.StopRuntimeNames).Count -eq 0) '部分进程缺失时只启动缺失 runtime'

        $staleTargetStates = @($currentTargetStates | ForEach-Object {
                [pscustomobject]@{ Name = $_.Name; BuildCurrent = ($_.Name -cne 'matchmaker') }
            })
        $sharedChangedPlan = Get-PandoraPlannerRuntimeActionPlan -BuildTargets $planTargets `
            -TargetStates $staleTargetStates -RuntimeStates $allRunningCurrent
        Assert-True ((@($sharedChangedPlan.BuildTargetNames) -join ',') -ceq 'matchmaker' -and
            ((@($sharedChangedPlan.StopRuntimeNames | Sort-Object) -join ',') -ceq 'matchmaker,matchmaker_pve') -and
            ((@($sharedChangedPlan.StartRuntimeNames | Sort-Object) -join ',') -ceq 'matchmaker,matchmaker_pve')) `
            'matchmaker build target 变化只 build 一次，并重启两个 runtime 实例'

        $tableChangedPlan = Get-PandoraPlannerRuntimeActionPlan -BuildTargets $planTargets `
            -TargetStates $staleTargetStates -RuntimeStates $allRunningCurrent -ConfigTableChanged
        Assert-True (@($tableChangedPlan.BuildTargetNames).Count -eq 1 -and
            ((@($tableChangedPlan.StopRuntimeNames | Sort-Object) -join ',') -ceq 'matchmaker,matchmaker_pve,player') -and
            ((@($tableChangedPlan.StartRuntimeNames | Sort-Object) -join ',') -ceq 'matchmaker,matchmaker_pve,player')) `
            '表变化与 Go 变化按 runtime 去重，不扩大到 login'
    }

    Write-Host '[5b] 全部 staging 成功后才事务发布，发布失败恢复旧二进制' -ForegroundColor Cyan
    $publishCommand = Get-Command Publish-PandoraPlannerStagedFiles -ErrorAction SilentlyContinue
    Assert-True ($null -ne $publishCommand) 'fast helper 暴露同盘 staging 事务发布 seam'
    if ($publishCommand) {
        $publishDir = Join-Path $tmp 'publish'
        New-Item -ItemType Directory -Path $publishDir -Force | Out-Null
        $finalA = Join-Path $publishDir 'a.exe'; $finalB = Join-Path $publishDir 'b.exe'
        $stageA = Join-Path $publishDir '.a.stage.exe'; $stageB = Join-Path $publishDir '.b.stage.exe'
        [IO.File]::WriteAllText($finalA, 'old-a'); [IO.File]::WriteAllText($finalB, 'old-b')
        [IO.File]::WriteAllText($stageA, 'new-a'); [IO.File]::WriteAllText($stageB, 'new-b')
        $publishRecords = @(
            [pscustomobject]@{ StagePath = $stageA; DestinationPath = $finalA },
            [pscustomobject]@{ StagePath = $stageB; DestinationPath = $finalB }
        )
        Publish-PandoraPlannerStagedFiles -Records $publishRecords
        Assert-True (([IO.File]::ReadAllText($finalA)) -ceq 'new-a' -and
            ([IO.File]::ReadAllText($finalB)) -ceq 'new-b' -and
            -not (Test-Path -LiteralPath $stageA) -and -not (Test-Path -LiteralPath $stageB)) `
            '成功时两个目标均从 staging 原子切换且不残留 staging'

        [IO.File]::WriteAllText($stageA, 'next-a'); [IO.File]::WriteAllText($stageB, 'next-b')
        $moveCount = 0
        $injectSecondPublishFailure = {
            param([string]$Source, [string]$Destination, [bool]$Overwrite)
            $moveCount++
            if ([IO.Path]::GetFileName($Source) -ceq '.b.stage.exe') { throw 'fixture second publish failure' }
            [IO.File]::Move($Source, $Destination, $Overwrite)
        }
        $publishFailed = $false
        try {
            Publish-PandoraPlannerStagedFiles -Records $publishRecords -MoveFile $injectSecondPublishFailure
        } catch { $publishFailed = $_.Exception.Message -match 'fixture second publish failure' }
        Assert-True ($publishFailed -and ([IO.File]::ReadAllText($finalA)) -ceq 'new-a' -and
            ([IO.File]::ReadAllText($finalB)) -ceq 'new-b') `
            '任一发布失败会恢复全部旧二进制，不留下混版'
    }

    Write-Host '[6] workspace/toolchain 变化必须使构建收据失效，cleanup 必须尽力完成' -ForegroundColor Cyan
    $fingerprintRoot = Join-Path $tmp 'fingerprint-root'
    $serviceRoot = Join-Path $fingerprintRoot 'services/example'
    New-Item -ItemType Directory -Force -Path $serviceRoot | Out-Null
    [IO.File]::WriteAllText((Join-Path $serviceRoot 'main.go'), 'package main', [Text.UTF8Encoding]::new($false))
    $goWork = Join-Path $fingerprintRoot 'go.work'
    [IO.File]::WriteAllText($goWork, "go 1.26`n", [Text.UTF8Encoding]::new($false))
    $fingerprintA = Get-PandoraPlannerGoInputFingerprint -ProjectRoot $fingerprintRoot -ToolchainSignature 'toolchain-a'
    $excludedRun = Join-Path $serviceRoot 'run/logs'
    New-Item -ItemType Directory -Force -Path $excludedRun | Out-Null
    [IO.File]::WriteAllText((Join-Path $excludedRun 'should-not-count.go'), 'package changed', [Text.UTF8Encoding]::new($false))
    $fingerprintWithRunLog = Get-PandoraPlannerGoInputFingerprint -ProjectRoot $fingerprintRoot -ToolchainSignature 'toolchain-a'
    [IO.File]::WriteAllText($goWork, "go 1.25`n", [Text.UTF8Encoding]::new($false))
    $fingerprintB = Get-PandoraPlannerGoInputFingerprint -ProjectRoot $fingerprintRoot -ToolchainSignature 'toolchain-a'
    $fingerprintC = Get-PandoraPlannerGoInputFingerprint -ProjectRoot $fingerprintRoot -ToolchainSignature 'toolchain-b'
    $toolModule = Join-Path $fingerprintRoot 'tools/tool'
    New-Item -ItemType Directory -Force -Path $toolModule | Out-Null
    $toolGoMod = Join-Path $toolModule 'go.mod'
    [IO.File]::WriteAllText($toolGoMod, "module example/tool`nrequire example/dep v1.0.0`n", [Text.UTF8Encoding]::new($false))
    $workspaceFingerprintA = Get-PandoraPlannerGoInputFingerprint -ProjectRoot $fingerprintRoot `
        -ToolchainSignature 'toolchain-a' -WorkspaceRoots @('services', 'tools/tool')
    [IO.File]::WriteAllText($toolGoMod, "module example/tool`nrequire example/dep v1.1.0`n", [Text.UTF8Encoding]::new($false))
    $workspaceFingerprintB = Get-PandoraPlannerGoInputFingerprint -ProjectRoot $fingerprintRoot `
        -ToolchainSignature 'toolchain-a' -WorkspaceRoots @('services', 'tools/tool')
    Assert-True ($fingerprintA -ne $fingerprintB) '根 go.work 变化会强制重建'
    Assert-True ($fingerprintA -eq $fingerprintWithRunLog) 'service-local run 日志/产物目录被剪枝，不随时间拖慢指纹'
    Assert-True ($fingerprintB -ne $fingerprintC) 'Go 工具链/环境指纹变化会强制重建'
    Assert-True ($workspaceFingerprintA -ne $workspaceFingerprintB) 'services/pkg/proto 之外的 go.work use 模块 go.mod 变化也会强制重建'

    Write-Host '[6a] Go 强指纹按真实依赖闭包传播，并且每文件每轮最多 hash 一次' -ForegroundColor Cyan
    $goTargetPlanCommand = Get-Command Get-PandoraPlannerGoTargetPlan -ErrorAction SilentlyContinue
    Assert-True ($null -ne $goTargetPlanCommand) 'fast helper 暴露逐 build-target Go 依赖指纹 seam'
    if ($goTargetPlanCommand) {
        $graphRoot = Join-Path $tmp 'go-graph'
        $aMainDir = Join-Path $graphRoot 'services/a/cmd/a'
        $bMainDir = Join-Path $graphRoot 'services/b/cmd/b'
        $sharedDir = Join-Path $graphRoot 'pkg/shared'
        $onlyADir = Join-Path $graphRoot 'pkg/onlya'
        $protoADir = Join-Path $graphRoot 'proto/gen/go/a'
        $externalDir = Join-Path $graphRoot '../module-cache/example-external-v1'
        New-Item -ItemType Directory -Force -Path $aMainDir, $bMainDir, $sharedDir, $onlyADir, $protoADir, $externalDir | Out-Null
        $fileBodies = [ordered]@{
            (Join-Path $aMainDir 'main.go') = 'a-main-v1'
            (Join-Path $bMainDir 'main.go') = 'b-main-v1'
            (Join-Path $sharedDir 'shared.go') = 'shared-v1'
            (Join-Path $onlyADir 'onlya.go') = 'only-a-v1'
            (Join-Path $protoADir 'a.pb.go') = 'proto-a-v1'
            (Join-Path $externalDir 'external.go') = 'external-cache-copy-v1'
            (Join-Path $graphRoot 'go.work') = 'go 1.26'
            (Join-Path $graphRoot 'go.work.sum') = 'sum-v1'
        }
        foreach ($pair in $fileBodies.GetEnumerator()) {
            [IO.File]::WriteAllText($pair.Key, $pair.Value, [Text.UTF8Encoding]::new($false))
        }
        $graphServices = @(
            # Windows 路径大小写不是构建身份；刻意与磁盘 Dir 大小写不同。
            [pscustomobject]@{ Name = 'a'; Dir = 'SERVICES/A'; Cmd = 'a' },
            [pscustomobject]@{ Name = 'b'; Dir = 'services/b'; Cmd = 'b' }
        )
        $graphTargets = @(Get-PandoraPlannerBuildTargets -Services $graphServices)
        $packages = @(
            [pscustomobject]@{ ImportPath = 'example/a/cmd/a'; Dir = $aMainDir; GoFiles = @('main.go'); Deps = @('example/shared', 'example/onlya', 'example/proto/a', 'example/external') },
            [pscustomobject]@{ ImportPath = 'example/b/cmd/b'; Dir = $bMainDir; GoFiles = @('main.go'); Deps = @('example/shared') },
            [pscustomobject]@{ ImportPath = 'example/shared'; Dir = $sharedDir; GoFiles = @('shared.go'); Deps = @() },
            [pscustomobject]@{ ImportPath = 'example/onlya'; Dir = $onlyADir; GoFiles = @('onlya.go'); Deps = @() },
            [pscustomobject]@{ ImportPath = 'example/proto/a'; Dir = $protoADir; GoFiles = @('a.pb.go'); Deps = @() },
            [pscustomobject]@{
                ImportPath = 'example/external'; Dir = $externalDir; GoFiles = @('external.go'); Deps = @()
                Module = [pscustomobject]@{ Path = 'example/external'; Version = 'v1.0.0'; Sum = 'h1:fixture-v1'; Main = $false }
            }
        )
        $hashCounts = @{}
        $countingHash = {
            param([string]$Path)
            $full = [IO.Path]::GetFullPath($Path)
            $hashCounts[$full] = 1 + [int]$hashCounts[$full]
            return (Get-FileHash -LiteralPath $full -Algorithm SHA256).Hash
        }
        $globalInputs = @((Join-Path $graphRoot 'go.work'), (Join-Path $graphRoot 'go.work.sum'))
        $goPlanA = @(Get-PandoraPlannerGoTargetPlan -ProjectRoot $graphRoot -BuildTargets $graphTargets `
                -PackageRecords $packages -ToolchainSignature 'toolchain-v1' -GlobalInputPaths $globalInputs `
                -GetContentHash $countingHash)
        Assert-True (@($hashCounts.Values | Where-Object { [int]$_ -ne 1 }).Count -eq 0 -and $hashCounts.Count -eq 7) `
            '一轮为两个 target 计算指纹时，共享文件只 hash 一次，第三方 module cache 源码不 hash'
        $goPlanAByName = @{}; foreach ($item in $goPlanA) { $goPlanAByName[$item.Name] = $item }

        [IO.File]::WriteAllText((Join-Path $externalDir 'external.go'), 'tampered-cache-copy', [Text.UTF8Encoding]::new($false))
        $goPlanExternalCacheChanged = @(Get-PandoraPlannerGoTargetPlan -ProjectRoot $graphRoot -BuildTargets $graphTargets `
                -PackageRecords $packages -ToolchainSignature 'toolchain-v1' -GlobalInputPaths $globalInputs)
        $externalCacheByName = @{}; foreach ($item in $goPlanExternalCacheChanged) { $externalCacheByName[$item.Name] = $item }
        Assert-True ($goPlanAByName.a.Fingerprint -ceq $externalCacheByName.a.Fingerprint -and
            $goPlanAByName.b.Fingerprint -ceq $externalCacheByName.b.Fingerprint) `
            '第三方模块只绑定 Path/Version/Sum 身份，不因模块缓存绝对路径或副本时间漂移'

        [IO.File]::WriteAllText((Join-Path $onlyADir 'onlya.go'), 'only-a-v2', [Text.UTF8Encoding]::new($false))
        $goPlanOnlyAChanged = @(Get-PandoraPlannerGoTargetPlan -ProjectRoot $graphRoot -BuildTargets $graphTargets `
                -PackageRecords $packages -ToolchainSignature 'toolchain-v1' -GlobalInputPaths $globalInputs)
        $onlyAByName = @{}; foreach ($item in $goPlanOnlyAChanged) { $onlyAByName[$item.Name] = $item }
        Assert-True ($goPlanAByName.a.Fingerprint -cne $onlyAByName.a.Fingerprint -and
            $goPlanAByName.b.Fingerprint -ceq $onlyAByName.b.Fingerprint) `
            '服务私有 pkg/proto 只使真实消费者 target 过期'

        [IO.File]::WriteAllText((Join-Path $sharedDir 'shared.go'), 'shared-v2', [Text.UTF8Encoding]::new($false))
        $goPlanSharedChanged = @(Get-PandoraPlannerGoTargetPlan -ProjectRoot $graphRoot -BuildTargets $graphTargets `
                -PackageRecords $packages -ToolchainSignature 'toolchain-v1' -GlobalInputPaths $globalInputs)
        $sharedByName = @{}; foreach ($item in $goPlanSharedChanged) { $sharedByName[$item.Name] = $item }
        Assert-True ($onlyAByName.a.Fingerprint -cne $sharedByName.a.Fingerprint -and
            $onlyAByName.b.Fingerprint -cne $sharedByName.b.Fingerprint) `
            '共享 pkg 变化传播到全部真实消费者'

        [IO.File]::WriteAllText((Join-Path $graphRoot 'go.work.sum'), 'sum-v2', [Text.UTF8Encoding]::new($false))
        $goPlanGlobalChanged = @(Get-PandoraPlannerGoTargetPlan -ProjectRoot $graphRoot -BuildTargets $graphTargets `
                -PackageRecords $packages -ToolchainSignature 'toolchain-v1' -GlobalInputPaths $globalInputs)
        $globalByName = @{}; foreach ($item in $goPlanGlobalChanged) { $globalByName[$item.Name] = $item }
        Assert-True ($sharedByName.a.Fingerprint -cne $globalByName.a.Fingerprint -and
            $sharedByName.b.Fingerprint -cne $globalByName.b.Fingerprint) `
            'go.work/go.mod/go.sum 这类全局不确定输入保守传播到所有 target'

        $goPlanToolchainChanged = @(Get-PandoraPlannerGoTargetPlan -ProjectRoot $graphRoot -BuildTargets $graphTargets `
                -PackageRecords $packages -ToolchainSignature 'toolchain-v2' -GlobalInputPaths $globalInputs)
        $toolchainByName = @{}; foreach ($item in $goPlanToolchainChanged) { $toolchainByName[$item.Name] = $item }
        Assert-True ($globalByName.a.Fingerprint -cne $toolchainByName.a.Fingerprint -and
            $globalByName.b.Fingerprint -cne $toolchainByName.b.Fingerprint) `
            '工具链变化保守传播到所有 target'
    }

    $cleanupState = @{ Attempts = [Collections.Generic.List[string]]::new() }
    $cleanupRecords = @('first', 'broken', 'last') | ForEach-Object {
        [pscustomobject]@{ Service = [pscustomobject]@{ Name = $_ } }
    }
    $cleanup = {
        param($record)
        $cleanupState.Attempts.Add($record.Service.Name)
        if ($record.Service.Name -eq 'broken') { throw 'simulated cleanup failure' }
    }
    $cleanupErrors = @(Invoke-PandoraPlannerCleanupRecords -Records $cleanupRecords -Cleanup $cleanup)
    Assert-True (($cleanupState.Attempts -join ',') -eq 'first,broken,last') '中间 cleanup 失败也会继续尝试后续 secret'
    Assert-True ($cleanupErrors.Count -eq 1 -and $cleanupErrors[0] -match '^broken:') '所有 cleanup 错误在尽力清理后聚合上报'

    Write-Host '[7] 普通/fast rollback 共用 exact Process 证明、重试 secret 并保留失败证据' -ForegroundColor Cyan
    $getPidFunction = $runAst.FindAll({
            param($node)
            $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Get-PidFile'
        }, $true) | Select-Object -First 1
    $rollbackFunction = $runAst.FindAll({
            param($node)
            $node -is [Management.Automation.Language.FunctionDefinitionAst] -and
                $node.Name -eq 'Remove-ServiceRuntimeConfigAfterLaunch'
        }, $true) | Select-Object -First 1
    Invoke-Expression $getPidFunction.Extent.Text
    Invoke-Expression $rollbackFunction.Extent.Text
    $LogDir = Join-Path $tmp 'rollback-pids'
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
    $svc = [pscustomobject]@{ Name = 'login' }
    $runtimeConfig = [pscustomobject]@{ Path = 'fixture'; Root = 'fixture'; Ephemeral = $true }
    $realRemoveRuntimeConfig = ${function:Remove-PandoraMysqlServiceRuntimeConfig}
    $realStopExactProcess = ${function:Stop-PandoraPlannerExactProcess}
    try {
        $script:RollbackCleanupAttempts = 0
        function Remove-PandoraMysqlServiceRuntimeConfig {
            param($RuntimeConfig)
            $script:RollbackCleanupAttempts++
            if ($script:RollbackCleanupAttempts -eq 1) { throw 'fixture first secret cleanup failure' }
        }
        function Stop-PandoraPlannerExactProcess {
            param($Process)
            return [pscustomobject]@{ ProcessId = [int]$Process.Id; StopRequested = $true; ExitConfirmed = $true; Error = '' }
        }
        $proc = [pscustomobject]@{ Id = 55101 }
        $pidFile = Get-PidFile $svc
        [IO.File]::WriteAllText($pidFile, '55101')
        $ordinaryMessage = ''
        try { Remove-ServiceRuntimeConfigAfterLaunch -svc $svc -RuntimeConfig $runtimeConfig -Process $proc }
        catch { $ordinaryMessage = $_.Exception.Message }
        Assert-True ($script:RollbackCleanupAttempts -eq 2 -and -not (Test-Path -LiteralPath $pidFile) -and
            $ordinaryMessage -match '已确认退出' -and $ordinaryMessage -notmatch '已回收') `
            '普通路径首次清理失败后确认退出、重试清理、再删 exact PID 并中止启动'

        $script:RollbackCleanupAttempts = 0
        [IO.File]::WriteAllText($pidFile, '55101')
        $fastBlocked = $false
        try {
            Remove-ServiceRuntimeConfigAfterLaunch -svc $svc -RuntimeConfig $runtimeConfig -Process $proc `
                -AlwaysRollback -FailureContext 'fixture fast launch failure'
        } catch { $fastBlocked = $true }
        Assert-True (-not $fastBlocked -and $script:RollbackCleanupAttempts -eq 2 -and -not (Test-Path -LiteralPath $pidFile)) `
            'fast launch 回滚会在停止后重试 secret，全部成功才删除 exact PID'

        $script:RollbackCleanupAttempts = 0
        function Remove-PandoraMysqlServiceRuntimeConfig { param($RuntimeConfig) $script:RollbackCleanupAttempts++ }
        function Stop-PandoraPlannerExactProcess {
            param($Process)
            return [pscustomobject]@{ ProcessId = [int]$Process.Id; StopRequested = $true; ExitConfirmed = $false; Error = 'fixture HasExited false' }
        }
        [IO.File]::WriteAllText($pidFile, '55101')
        $failureMessage = ''
        try {
            Remove-ServiceRuntimeConfigAfterLaunch -svc $svc -RuntimeConfig $runtimeConfig -Process $proc `
                -AlwaysRollback -FailureContext 'fixture fast launch failure'
        } catch { $failureMessage = $_.Exception.Message }
        Assert-True ((Test-Path -LiteralPath $pidFile) -and $script:RollbackCleanupAttempts -gt 0 -and
            $failureMessage -match '未能确认退出|HasExited' -and $failureMessage -notmatch '已回收') `
            '无法证明退出时保留 PID/错误证据，仍尝试 secret cleanup 且不谎称已回收'

        function Stop-PandoraPlannerExactProcess {
            param($Process)
            return [pscustomobject]@{ ProcessId = [int]$Process.Id; StopRequested = $true; ExitConfirmed = $true; Error = '' }
        }
        [IO.File]::WriteAllText($pidFile, '99999')
        $mismatchMessage = ''
        try {
            Remove-ServiceRuntimeConfigAfterLaunch -svc $svc -RuntimeConfig $runtimeConfig -Process $proc `
                -AlwaysRollback -FailureContext 'fixture fast launch failure'
        } catch { $mismatchMessage = $_.Exception.Message }
        Assert-True ((Test-Path -LiteralPath $pidFile) -and $mismatchMessage -match 'PID.*不一致|exact PID') `
            'PID 文件不是本轮 exact Process 时拒绝删除并保留证据'
    } finally {
        Set-Item -LiteralPath function:Remove-PandoraMysqlServiceRuntimeConfig -Value $realRemoveRuntimeConfig
        Set-Item -LiteralPath function:Stop-PandoraPlannerExactProcess -Value $realStopExactProcess
        Remove-Variable -Name RollbackCleanupAttempts -Scope Script -Force -ErrorAction SilentlyContinue
    }
} finally {
    Remove-Item -LiteralPath $tmp -Recurse -Force -ErrorAction SilentlyContinue
}

if ($script:Failures.Count -gt 0) {
    throw "策划 fast 启动契约失败($($script:Failures.Count)):`n - $($script:Failures -join "`n - ")"
}
Write-Host '[PASS] 策划 fast 启动契约通过。' -ForegroundColor Green
