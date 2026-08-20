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

Write-Host '[2a] exact Process 回收必须有界并由 Refresh/HasExited 证明' -ForegroundColor Cyan
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

Write-Host '[2b] PID 在 Refresh→Stop 间复用也只能杀原 Process handle' -ForegroundColor Cyan
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
