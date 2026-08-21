# 策划 migration 同 runspace 原生进程边界契约。
#
# 只启动临时 pwsh 纯进程 fixture，不读写项目运行态，不启停数据库、基础设施、业务服务或 K8s。

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
$helperPath = Join-Path $projectRoot 'tools/scripts/lib/planner_bounded_process.ps1'

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED: $Message" }
}

function Assert-Equal($Expected, $Actual, [string]$Message) {
    if ($Expected -ne $Actual) {
        throw "ASSERT FAILED: $Message（期望=$Expected，实际=$Actual）"
    }
}

function Test-FixtureProcessExited([int]$ProcessId) {
    try {
        $process = [Diagnostics.Process]::GetProcessById($ProcessId)
        $process.Refresh()
        return $process.HasExited
    } catch [ArgumentException] {
        return $true
    }
}

function Wait-FixtureProcessExited([int]$ProcessId, [int]$TimeoutMilliseconds = 5000) {
    $deadline = [Environment]::TickCount64 + $TimeoutMilliseconds
    while (-not (Test-FixtureProcessExited $ProcessId) -and
        [Environment]::TickCount64 -lt $deadline) {
        Start-Sleep -Milliseconds 10
    }
    return (Test-FixtureProcessExited $ProcessId)
}

function Get-CurrentProcessHandleCount {
    $current = [Diagnostics.Process]::GetCurrentProcess()
    try {
        $current.Refresh()
        return $current.HandleCount
    } finally {
        $current.Dispose()
    }
}

function Remove-FixtureProcessById([int]$FixturePid) {
    if ($FixturePid -le 0) { return }
    try {
        $process = [Diagnostics.Process]::GetProcessById($FixturePid)
        $process.Refresh()
        if (-not $process.HasExited -and
            [IO.Path]::GetFullPath($process.MainModule.FileName) -ceq
            [IO.Path]::GetFullPath((Join-Path $PSHOME 'pwsh.exe'))) {
            $process.Kill($true)
            $null = $process.WaitForExit(5000)
        }
    } catch [ArgumentException] {
        # 已退出。
    }
}

function Remove-FixtureProcessTree([string]$PidFile) {
    if (-not (Test-Path -LiteralPath $PidFile -PathType Leaf)) { return }
    $fixturePid = 0
    if (-not [int]::TryParse(([IO.File]::ReadAllText($PidFile).Trim()), [ref]$fixturePid) -or
        $fixturePid -le 0) { return }
    Remove-FixtureProcessById $fixturePid
}

Assert-True (Test-Path -LiteralPath $helperPath -PathType Leaf) `
    '应提供策划同 runspace 有界原生进程 helper'
. $helperPath

$fixtureRoot = Join-Path ([IO.Path]::GetTempPath()) (
    'pandora-planner-bounded-process-' + [guid]::NewGuid().ToString('N'))
$faultProcessIds = [Collections.Generic.List[int]]::new()
try {
    $null = New-Item -ItemType Directory -Force -Path $fixtureRoot

    Write-Host '[1] 正常退出：UTF-8 stdout/stderr、cwd、env 与 stdin 全部经公开 seam 返回' -ForegroundColor Cyan
    $normalScript = Join-Path $fixtureRoot 'normal.ps1'
    [IO.File]::WriteAllText($normalScript, @'
$ErrorActionPreference = 'Stop'
$utf8 = [Text.UTF8Encoding]::new($false)
[Console]::InputEncoding = $utf8
[Console]::OutputEncoding = $utf8
$stdin = [Console]::In.ReadToEnd()
[Console]::Out.WriteLine('stdout=中文|' + $env:PANDORA_BOUNDED_FIXTURE + '|' + (Get-Location).Path + '|' + $stdin.TrimEnd())
[Console]::Error.WriteLine('stderr=诊断')
exit 7
'@, [Text.UTF8Encoding]::new($false))

    $normalResult = Invoke-PandoraPlannerBoundedProcess `
        -FilePath (Join-Path $PSHOME 'pwsh.exe') `
        -ArgumentList @('-NoLogo', '-NoProfile', '-File', $normalScript) `
        -WorkingDirectory $fixtureRoot `
        -Environment @{ PANDORA_BOUNDED_FIXTURE = '环境值' } `
        -StandardInput "输入值`n" `
        -TimeoutMilliseconds 10000 `
        -MaximumOutputBytesPerStream 4096

    Assert-Equal 7 $normalResult.ExitCode '必须保留 exact native 退出码'
    Assert-True (-not $normalResult.TimedOut) '正常退出不应误报超时'
    Assert-True $normalResult.DrainCompleted '正常退出必须完整关闭三条管道'
    Assert-True ($normalResult.ProcessId -gt 0) '必须返回 exact child PID'
    Assert-True ($normalResult.StandardOutput -match 'stdout=中文\|环境值\|.*\|输入值') `
        'stdout 必须按 UTF-8 返回 cwd/env/stdin'
    Assert-True ($normalResult.StandardError -match 'stderr=诊断') 'stderr 必须按 UTF-8 返回'
    Assert-True (-not $normalResult.StandardOutputTruncated -and -not $normalResult.StandardErrorTruncated) `
        '未达上限的输出不得误报截断'

    Write-Host '[2] 输出上限：超额后继续 drain 但不再增长内存' -ForegroundColor Cyan
    $largeOutputScript = Join-Path $fixtureRoot 'large-output.ps1'
    [IO.File]::WriteAllText($largeOutputScript, @'
[Console]::Out.Write(('o' * 8192))
[Console]::Error.Write(('e' * 8192))
exit 0
'@, [Text.UTF8Encoding]::new($false))
    $largeOutputResult = Invoke-PandoraPlannerBoundedProcess `
        -FilePath (Join-Path $PSHOME 'pwsh.exe') `
        -ArgumentList @('-NoLogo', '-NoProfile', '-File', $largeOutputScript) `
        -WorkingDirectory $fixtureRoot `
        -TimeoutMilliseconds 10000 `
        -MaximumOutputBytesPerStream 128
    Assert-Equal 0 $largeOutputResult.ExitCode '大输出进程不得因管道填满假失败'
    Assert-Equal 128 ([Text.Encoding]::UTF8.GetByteCount($largeOutputResult.StandardOutput)) `
        'stdout 保留量必须精确有界'
    Assert-Equal 128 ([Text.Encoding]::UTF8.GetByteCount($largeOutputResult.StandardError)) `
        'stderr 保留量必须精确有界'
    Assert-True ($largeOutputResult.StandardOutputTruncated -and $largeOutputResult.StandardErrorTruncated) `
        '两条管道都必须显式报告截断'

    Write-Host '[3] 单调超时：子进程不读大 stdin 也必须按原 deadline 收口并回收整树' -ForegroundColor Cyan
    $grandPidFile = Join-Path $fixtureRoot 'timeout-grand.pid'
    $childPidFile = Join-Path $fixtureRoot 'timeout-child.pid'
    $grandScript = Join-Path $fixtureRoot 'grand.ps1'
    $childScript = Join-Path $fixtureRoot 'child.ps1'
    [IO.File]::WriteAllText($grandScript, @'
param([Parameter(Mandatory)][string]$PidFile)
[IO.File]::WriteAllText($PidFile, "$PID", [Text.Encoding]::ASCII)
while ($true) { Start-Sleep -Seconds 1 }
'@, [Text.UTF8Encoding]::new($false))
    [IO.File]::WriteAllText($childScript, @'
param(
    [Parameter(Mandatory)][string]$ChildPidFile,
    [Parameter(Mandatory)][string]$GrandPidFile,
    [Parameter(Mandatory)][string]$GrandScript
)
[IO.File]::WriteAllText($ChildPidFile, "$PID", [Text.Encoding]::ASCII)
$startInfo = [Diagnostics.ProcessStartInfo]::new()
$startInfo.FileName = (Join-Path $PSHOME 'pwsh.exe')
$startInfo.UseShellExecute = $false
$startInfo.CreateNoWindow = $true
foreach ($argument in @('-NoLogo', '-NoProfile', '-File', $GrandScript, '-PidFile', $GrandPidFile)) {
    $null = $startInfo.ArgumentList.Add($argument)
}
$grand = [Diagnostics.Process]::Start($startInfo)
$readyDeadline = [Environment]::TickCount64 + 1500
while (-not (Test-Path -LiteralPath $GrandPidFile -PathType Leaf) -and
    [Environment]::TickCount64 -lt $readyDeadline) {
    Start-Sleep -Milliseconds 10
}
if (-not (Test-Path -LiteralPath $GrandPidFile -PathType Leaf)) { exit 42 }
[Console]::Out.WriteLine('tree-ready')
# 故意不读 stdin：父进程的大写入会卡在管道反压，必须与 child 共用 deadline。
while ($true) { Start-Sleep -Seconds 1 }
'@, [Text.UTF8Encoding]::new($false))

    $timeoutWatch = [Diagnostics.Stopwatch]::StartNew()
    $timeoutResult = Invoke-PandoraPlannerBoundedProcess `
        -Name 'timeout-tree' `
        -FilePath (Join-Path $PSHOME 'pwsh.exe') `
        -ArgumentList @(
            '-NoLogo', '-NoProfile', '-File', $childScript,
            '-ChildPidFile', $childPidFile,
            '-GrandPidFile', $grandPidFile,
            '-GrandScript', $grandScript
        ) `
        -WorkingDirectory $fixtureRoot `
        -StandardInput ('i' * (8 * 1024 * 1024)) `
        -TimeoutMilliseconds 2500 `
        -CleanupTimeoutMilliseconds 3000 `
        -MaximumOutputBytesPerStream 4096
    $timeoutWatch.Stop()

    Assert-True $timeoutResult.TimedOut '子进程不退出必须标记 TimedOut=true'
    Assert-Equal -1 $timeoutResult.ExitCode '超时后 public ExitCode 必须 fail-closed'
    Assert-True $timeoutResult.DrainCompleted '终止 Job 后 stdout/stderr 必须在 cleanup 窗口内关闭'
    Assert-True (-not $timeoutResult.StandardInputCompleted) `
        '不读大 stdin 的子进程被终止后不得伪报 stdin 全量送达'
    Assert-True ($timeoutResult.StandardOutput -match 'tree-ready') '超时返回仍应包含已 drain 的诊断输出'
    Assert-True ($timeoutWatch.ElapsedMilliseconds -ge 2200 -and $timeoutWatch.ElapsedMilliseconds -lt 7000) `
        '操作必须消耗原单调 deadline，且 cleanup 不得无界'
    Assert-True (Test-Path -LiteralPath $childPidFile -PathType Leaf) '应有 child PID 现场'
    Assert-True (Test-Path -LiteralPath $grandPidFile -PathType Leaf) '应有 grandchild PID 现场'
    $timeoutChildPid = [int]([IO.File]::ReadAllText($childPidFile).Trim())
    $timeoutGrandPid = [int]([IO.File]::ReadAllText($grandPidFile).Trim())
    Assert-Equal $timeoutChildPid $timeoutResult.ProcessId '结果必须绑定 exact root PID'
    Assert-True (Test-FixtureProcessExited $timeoutChildPid) '超时必须确认 exact root 已退出'
    Assert-True (Test-FixtureProcessExited $timeoutGrandPid) '超时必须通过 Job 回收孙进程'

    Write-Host '[4] 父 pwsh 硬退出：操作系统关闭 Job handle 必须自动杀掉整树' -ForegroundColor Cyan
    $hardExitChildPidFile = Join-Path $fixtureRoot 'hard-exit-child.pid'
    $hardExitGrandPidFile = Join-Path $fixtureRoot 'hard-exit-grand.pid'
    $driverScript = Join-Path $fixtureRoot 'hard-exit-driver.ps1'
    [IO.File]::WriteAllText($driverScript, @'
$ErrorActionPreference = 'Stop'
. $env:PANDORA_BOUNDED_HELPER
$null = Invoke-PandoraPlannerBoundedProcess `
    -Name 'hard-exit-tree' `
    -FilePath (Join-Path $PSHOME 'pwsh.exe') `
    -ArgumentList @(
        '-NoLogo', '-NoProfile', '-File', $env:PANDORA_BOUNDED_CHILD_SCRIPT,
        '-ChildPidFile', $env:PANDORA_BOUNDED_CHILD_PID,
        '-GrandPidFile', $env:PANDORA_BOUNDED_GRAND_PID,
        '-GrandScript', $env:PANDORA_BOUNDED_GRAND_SCRIPT
    ) `
    -WorkingDirectory $env:PANDORA_BOUNDED_WORKDIR `
    -TimeoutMilliseconds 600000
'@, [Text.UTF8Encoding]::new($false))

    $driverStartInfo = [Diagnostics.ProcessStartInfo]::new()
    $driverStartInfo.FileName = (Join-Path $PSHOME 'pwsh.exe')
    $driverStartInfo.UseShellExecute = $false
    $driverStartInfo.CreateNoWindow = $true
    foreach ($argument in @('-NoLogo', '-NoProfile', '-File', $driverScript)) {
        $null = $driverStartInfo.ArgumentList.Add($argument)
    }
    $driverStartInfo.Environment['PANDORA_BOUNDED_HELPER'] = $helperPath
    $driverStartInfo.Environment['PANDORA_BOUNDED_CHILD_SCRIPT'] = $childScript
    $driverStartInfo.Environment['PANDORA_BOUNDED_CHILD_PID'] = $hardExitChildPidFile
    $driverStartInfo.Environment['PANDORA_BOUNDED_GRAND_PID'] = $hardExitGrandPidFile
    $driverStartInfo.Environment['PANDORA_BOUNDED_GRAND_SCRIPT'] = $grandScript
    $driverStartInfo.Environment['PANDORA_BOUNDED_WORKDIR'] = $fixtureRoot
    $driver = [Diagnostics.Process]::Start($driverStartInfo)
    try {
        $hardExitReadyDeadline = [Environment]::TickCount64 + 10000
        while ((-not (Test-Path -LiteralPath $hardExitChildPidFile -PathType Leaf) -or
                -not (Test-Path -LiteralPath $hardExitGrandPidFile -PathType Leaf)) -and
            -not $driver.HasExited -and [Environment]::TickCount64 -lt $hardExitReadyDeadline) {
            Start-Sleep -Milliseconds 10
            $driver.Refresh()
        }
        Assert-True (-not $driver.HasExited) '硬退出 fixture 的父 pwsh 必须在检测前仍活着'
        Assert-True (Test-Path -LiteralPath $hardExitChildPidFile -PathType Leaf) `
            '硬退出前必须确认 root child 已运行'
        Assert-True (Test-Path -LiteralPath $hardExitGrandPidFile -PathType Leaf) `
            '硬退出前必须确认 grandchild 已运行'
        $hardExitChildPid = [int]([IO.File]::ReadAllText($hardExitChildPidFile).Trim())
        $hardExitGrandPid = [int]([IO.File]::ReadAllText($hardExitGrandPidFile).Trim())
        Assert-True (-not (Test-FixtureProcessExited $hardExitChildPid)) '杀父进程前 root child 必须真实存活'
        Assert-True (-not (Test-FixtureProcessExited $hardExitGrandPid)) '杀父进程前 grandchild 必须真实存活'

        # 只 Kill exact driver，故意不用 Kill(true)；child 整树只能由 KILL_ON_JOB_CLOSE 回收。
        $driver.Kill()
        Assert-True ($driver.WaitForExit(5000)) '父 pwsh 必须在有界时间内硬退出'
        $jobCloseDeadline = [Environment]::TickCount64 + 5000
        while ((-not (Test-FixtureProcessExited $hardExitChildPid) -or
                -not (Test-FixtureProcessExited $hardExitGrandPid)) -and
            [Environment]::TickCount64 -lt $jobCloseDeadline) {
            Start-Sleep -Milliseconds 10
        }
        Assert-True (Test-FixtureProcessExited $hardExitChildPid) `
            '父 pwsh 硬退出关闭 Job handle 后必须自动杀 root child'
        Assert-True (Test-FixtureProcessExited $hardExitGrandPid) `
            '父 pwsh 硬退出关闭 Job handle 后必须自动杀 grandchild'
    } finally {
        try {
            $driver.Refresh()
            if (-not $driver.HasExited) {
                $driver.Kill()
                $null = $driver.WaitForExit(5000)
            }
        } catch { }
        $driver.Dispose()
    }

    Write-Host '[5] 调用总 deadline：路径/参数预检耗时必须扣除，耗尽时绝不 CreateProcess' -ForegroundColor Cyan
    $preflightScript = Join-Path $fixtureRoot 'preflight-budget.ps1'
    $preflightStartedMarker = Join-Path $fixtureRoot 'preflight-started.txt'
    $preflightCompletedMarker = Join-Path $fixtureRoot 'preflight-completed.txt'
    [IO.File]::WriteAllText($preflightScript, @'
param(
    [Parameter(Mandatory)][string]$StartedMarker,
    [Parameter(Mandatory)][string]$CompletedMarker,
    [Parameter(Mandatory)][int]$DelayMilliseconds
)
[IO.File]::WriteAllText($StartedMarker, "$PID", [Text.Encoding]::ASCII)
Start-Sleep -Milliseconds $DelayMilliseconds
[IO.File]::WriteAllText($CompletedMarker, 'completed', [Text.Encoding]::ASCII)
'@, [Text.UTF8Encoding]::new($false))

    $preflightError = $null
    try {
        $null = Invoke-PandoraPlannerBoundedProcess `
            -Name 'preflight-exhausted' `
            -FilePath (Join-Path $PSHOME 'pwsh.exe') `
            -ArgumentList @(
                '-NoLogo', '-NoProfile', '-File', $preflightScript,
                '-StartedMarker', $preflightStartedMarker,
                '-CompletedMarker', $preflightCompletedMarker,
                '-DelayMilliseconds', '1'
            ) `
            -WorkingDirectory $fixtureRoot `
            -TimeoutMilliseconds 100 `
            -TestGetPreflightElapsedMilliseconds { 100 }
    } catch {
        $preflightError = $_.Exception.GetBaseException()
    }
    Assert-True ($null -ne $preflightError -and $preflightError -is [TimeoutException]) `
        '预检已耗尽调用 deadline 时必须在 CreateProcess 前抛 TimeoutException'
    Assert-True (-not (Test-Path -LiteralPath $preflightStartedMarker -PathType Leaf)) `
        '预检已耗尽时 child 不得被创建或执行'

    $partialPreflightResult = Invoke-PandoraPlannerBoundedProcess `
        -Name 'preflight-deducted' `
        -FilePath (Join-Path $PSHOME 'pwsh.exe') `
        -ArgumentList @(
            '-NoLogo', '-NoProfile', '-File', $preflightScript,
            '-StartedMarker', $preflightStartedMarker,
            '-CompletedMarker', $preflightCompletedMarker,
            '-DelayMilliseconds', '2000'
        ) `
        -WorkingDirectory $fixtureRoot `
        -TimeoutMilliseconds 1500 `
        -CleanupTimeoutMilliseconds 3000 `
        -TestGetPreflightElapsedMilliseconds { 500 }
    Assert-True $partialPreflightResult.TimedOut `
        '预检已用 500ms 后 child 只能使用剩余 1000ms，不得重新获得完整 1500ms'
    Assert-Equal -1 $partialPreflightResult.ExitCode '扣减后的超时结果必须 fail-closed'
    Assert-True (Test-Path -LiteralPath $preflightStartedMarker -PathType Leaf) `
        '部分剩余 deadline 应允许 child 启动'
    Assert-True (-not (Test-Path -LiteralPath $preflightCompletedMarker -PathType Leaf)) `
        'child 不得越过扣减后的剩余 deadline 完成'
    Assert-True ($partialPreflightResult.ElapsedMilliseconds -ge 1350 -and
        $partialPreflightResult.ElapsedMilliseconds -lt 3000) `
        '公开 ElapsedMilliseconds 必须包含注入的预检耗时与 child 执行耗时'

    $relativePathError = $null
    try {
        $null = Invoke-PandoraPlannerBoundedProcess `
            -FilePath 'pwsh.exe' `
            -WorkingDirectory $fixtureRoot `
            -TimeoutMilliseconds 1000
    } catch {
        $relativePathError = $_.Exception.GetBaseException()
    }
    Assert-True ($null -ne $relativePathError -and $relativePathError -is [ArgumentException]) `
        '公开 seam 只接受绝对 FilePath，不得做无界 Get-Command 路径搜索'

    Write-Host '[6] 启动/装配故障：重复失败后 exact PID、Job tree 与 native handle 必须零残留' -ForegroundColor Cyan
    [GC]::Collect()
    [GC]::WaitForPendingFinalizers()
    [GC]::Collect()
    $baselineHandleCount = Get-CurrentProcessHandleCount
    $faultPoints = @(
        'AfterCreateProcess',
        'BeforeStdoutFileStream',
        'BeforeStderrFileStream',
        'BeforeStdinFileStream'
    )
    foreach ($faultPoint in $faultPoints) {
        foreach ($attempt in 1..6) {
            $faultError = $null
            try {
                $null = Invoke-PandoraPlannerBoundedProcess `
                    -Name "fault-$faultPoint-$attempt" `
                    -FilePath (Join-Path $PSHOME 'pwsh.exe') `
                    -ArgumentList @('-NoLogo', '-NoProfile', '-File', $largeOutputScript) `
                    -WorkingDirectory $fixtureRoot `
                    -TimeoutMilliseconds 5000 `
                    -CleanupTimeoutMilliseconds 3000 `
                    -TestFaultInjectionPoint $faultPoint
            } catch {
                $faultError = $_.Exception.GetBaseException()
            }
            Assert-True ($null -ne $faultError -and $faultError -is [InvalidOperationException]) `
                "$faultPoint 第 $attempt 次必须命中可辨识的装配故障"
            $faultPidValue = $faultError.Data['PandoraProcessId']
            Assert-True ($null -ne $faultPidValue -and [int]$faultPidValue -gt 0) `
                "$faultPoint 第 $attempt 次必须携带 exact child PID"
            $faultProcessIds.Add([int]$faultPidValue)
            Assert-True (Wait-FixtureProcessExited ([int]$faultPidValue) 5000) `
                "$faultPoint 第 $attempt 次失败后 exact child 必须退出"
        }

        # 每类装配失败后立刻跑一次成功路径，捕获 double-close 误关复用 handle 的回归。
        $postFaultResult = Invoke-PandoraPlannerBoundedProcess `
            -Name "post-$faultPoint" `
            -FilePath (Join-Path $PSHOME 'pwsh.exe') `
            -ArgumentList @('-NoLogo', '-NoProfile', '-File', $largeOutputScript) `
            -WorkingDirectory $fixtureRoot `
            -TimeoutMilliseconds 10000 `
            -MaximumOutputBytesPerStream 128
        Assert-Equal 0 $postFaultResult.ExitCode "$faultPoint 后续成功路径不得被误关 handle"
        Assert-True $postFaultResult.DrainCompleted "$faultPoint 后续三条管道必须完整关闭"
    }

    $missingExecutable = Join-Path $fixtureRoot 'definitely-missing.exe'
    foreach ($attempt in 1..6) {
        $createError = $null
        try {
            $null = Invoke-PandoraPlannerBoundedProcess `
                -Name "create-process-failure-$attempt" `
                -FilePath $missingExecutable `
                -WorkingDirectory $fixtureRoot `
                -TimeoutMilliseconds 5000 `
                -CleanupTimeoutMilliseconds 3000
        } catch {
            $createError = $_.Exception.GetBaseException()
        }
        Assert-True ($null -ne $createError -and $createError -is [ComponentModel.Win32Exception]) `
            "CreateProcessW 第 $attempt 次失败必须返回原始 Win32Exception"
    }

    [GC]::Collect()
    [GC]::WaitForPendingFinalizers()
    [GC]::Collect()
    $finalHandleCount = Get-CurrentProcessHandleCount
    Assert-True (($finalHandleCount - $baselineHandleCount) -le 8) `
        "重复启动/装配失败不得泄漏 native handle（前=$baselineHandleCount，后=$finalHandleCount）"

    $fixtureCommandLineNeedle = [IO.Path]::GetFullPath($fixtureRoot)
    $fixtureResidue = @(
        Get-CimInstance Win32_Process -Filter "Name = 'pwsh.exe'" -ErrorAction Stop |
            Where-Object {
                $_.ProcessId -ne $PID -and
                -not [string]::IsNullOrWhiteSpace($_.CommandLine) -and
                $_.CommandLine.IndexOf($fixtureCommandLineNeedle, [StringComparison]::OrdinalIgnoreCase) -ge 0
            }
    )
    Assert-Equal 0 $fixtureResidue.Count '契约结束前临时 fixture 命令行必须零进程残留'
} finally {
    Remove-FixtureProcessTree (Join-Path $fixtureRoot 'timeout-child.pid')
    Remove-FixtureProcessTree (Join-Path $fixtureRoot 'timeout-grand.pid')
    Remove-FixtureProcessTree (Join-Path $fixtureRoot 'hard-exit-child.pid')
    Remove-FixtureProcessTree (Join-Path $fixtureRoot 'hard-exit-grand.pid')
    Remove-FixtureProcessTree (Join-Path $fixtureRoot 'preflight-started.txt')
    foreach ($faultProcessId in $faultProcessIds) {
        Remove-FixtureProcessById $faultProcessId
    }
    $resolvedFixture = [IO.Path]::GetFullPath($fixtureRoot)
    $resolvedTemp = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
    if ($resolvedFixture.StartsWith($resolvedTemp, [StringComparison]::OrdinalIgnoreCase) -and
        (Test-Path -LiteralPath $resolvedFixture -PathType Container)) {
        Remove-Item -LiteralPath $resolvedFixture -Recurse -Force -ErrorAction SilentlyContinue
    }
}

Write-Host '[PASS] 策划同 runspace 有界原生进程契约通过' -ForegroundColor Green
