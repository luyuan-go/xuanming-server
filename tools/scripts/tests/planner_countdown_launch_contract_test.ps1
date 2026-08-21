# 策划多进程 launch 后统一 countdown/readiness 屏障契约。
# 只用虚拟时钟，不启动真实进程、数据库或 K8s。

$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
. (Join-Path $projectRoot 'tools/scripts/lib/planner_fast_start.ps1')

function Assert-Equal($Expected, $Actual, [string]$Message) {
    if ($Expected -ne $Actual) { throw "$Message（期望=$Expected，实际=$Actual）" }
}

$clock = [pscustomobject]@{ Milliseconds = 0; Snapshots = 0 }
$states = @(
    [pscustomobject]@{ Name = 'fast'; ReadyAt = 100; Ready = $false; Failure = '' },
    [pscustomobject]@{ Name = 'middle'; ReadyAt = 300; Ready = $false; Failure = '' },
    [pscustomobject]@{ Name = 'slow'; ReadyAt = 500; Ready = $false; Failure = '' }
)
Wait-PandoraPlannerServiceBatch -States $states `
    -GetListenerRecords { $clock.Snapshots++; return @($clock.Milliseconds) } `
    -TestProcessExited { param($State) return $false } `
    -TestListenerOwned { param($State, $Records) return $clock.Milliseconds -ge $State.ReadyAt } `
    -Sleep { param([int]$Milliseconds) $clock.Milliseconds += $Milliseconds } `
    -GetElapsedMilliseconds { return [int64]$clock.Milliseconds } `
    -PollMilliseconds 100 -TimeoutMilliseconds 1000

Assert-Equal 500 $clock.Milliseconds '同一波墙钟必须等于最慢任务，而不是三个任务耗时相加'
Assert-Equal 0 @($states | Where-Object { -not $_.Ready -or $_.Failure }).Count `
    '三个进程必须各自 exact-ready 后才通过屏障'
Assert-Equal 6 $clock.Snapshots '每轮只取一份共享 listener 快照，不能按进程数重复查询'

$clock.Milliseconds = 0
$clock.Snapshots = 0
$timeoutStates = @(
    [pscustomobject]@{ Name = 'ready'; ReadyAt = 100; Ready = $false; Failure = '' },
    [pscustomobject]@{ Name = 'stuck'; ReadyAt = 5000; Ready = $false; Failure = '' }
)
Wait-PandoraPlannerServiceBatch -States $timeoutStates `
    -GetListenerRecords { $clock.Snapshots++; return @($clock.Milliseconds) } `
    -TestProcessExited { param($State) return $false } `
    -TestListenerOwned { param($State, $Records) return $clock.Milliseconds -ge $State.ReadyAt } `
    -Sleep { param([int]$Milliseconds) $clock.Milliseconds += $Milliseconds } `
    -GetElapsedMilliseconds { return [int64]$clock.Milliseconds } `
    -PollMilliseconds 100 -TimeoutMilliseconds 400

Assert-Equal 400 $clock.Milliseconds '全局 timeout 不能乘以进程数量'
Assert-Equal $true $timeoutStates[0].Ready '已就绪进程保留成功状态'
Assert-Equal 'ready-timeout' $timeoutStates[1].Failure '卡住进程必须明确失败而不是永久等待'

Write-Host '[PASS] 策划多进程 countdown 启动/检测契约' -ForegroundColor Green
