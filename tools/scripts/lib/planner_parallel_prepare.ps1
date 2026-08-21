# 策划免 Docker 并行准备的 exact child Process 边界。
#
# 固定编排本身在 dev_all.ps1，基础设施在父 runspace 保留同一把工作区锁；本文件只负责
# tables/build 短任务的启动、真实耗时、有界输出 drain 与 exact PID 回收。migration 在
# MySQL-ready callback 中留在父 runspace，同锁执行并与已 launch 的其它基础设施进程重叠。

function Get-PandoraPlannerSpeculativeBuildDisposition {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][bool]$GenerationIdentityChanged,
        [Parameter(Mandatory)][int]$ExitCode,
        [Parameter(Mandatory)][bool]$DrainCompleted
    )
    Set-StrictMode -Version Latest
    if (-not $DrainCompleted) { return 'fail-unbounded' }
    # 生成态变化时首轮只是投机结果：成功也可能读到旧/混合批次，失败也可能只是撞写；
    # 两者都只能在导表完全稳定后重建一次。未变化的非零则是真实 build 失败，不盲目重试。
    if ($GenerationIdentityChanged) { return 'retry-stable-once' }
    if ($ExitCode -ne 0) { return 'fail' }
    return 'accept'
}

function Start-PandoraPlannerPreparationProcess {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$FilePath,
        [Parameter(Mandatory)][AllowEmptyCollection()][string[]]$ArgumentList,
        [Parameter(Mandatory)][string]$WorkingDirectory
    )

    Set-StrictMode -Version Latest
    $resolvedExe = (Get-Command $FilePath -CommandType Application -ErrorAction Stop).Source
    $resolvedWorkingDirectory = [IO.Path]::GetFullPath($WorkingDirectory)
    if (-not (Test-Path -LiteralPath $resolvedWorkingDirectory -PathType Container)) {
        throw "策划并行准备工作目录不存在:$resolvedWorkingDirectory"
    }

    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $resolvedExe
    $startInfo.WorkingDirectory = $resolvedWorkingDirectory
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.StandardOutputEncoding = [Text.UTF8Encoding]::new($false)
    $startInfo.StandardErrorEncoding = [Text.UTF8Encoding]::new($false)
    foreach ($argument in $ArgumentList) { $null = $startInfo.ArgumentList.Add($argument) }

    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    $startedAt = [Environment]::TickCount64
    try {
        if (-not $process.Start()) { throw "无法启动 worker:$Name" }
        # StartTime/ExitTime 都来自同一个 exact Process，Elapsed 不受父进程何时 join 影响。
        $startedAtUtc = $process.StartTime.ToUniversalTime()
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
    } catch {
        $process.Dispose()
        throw
    }
    return [pscustomobject][ordered]@{
        Name = $Name
        Process = $process
        StandardOutputTask = $stdoutTask
        StandardErrorTask = $stderrTask
        ProcessId = [int]$process.Id
        StartedAtUtc = $startedAtUtc
        StartedAtMilliseconds = [int64]$startedAt
        Completed = $false
    }
}

function Stop-PandoraPlannerPreparationProcess {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]$Handle,
        [ValidateRange(1, 60000)][int]$DrainTimeoutMilliseconds = 5000
    )

    Set-StrictMode -Version Latest
    $process = $Handle.Process
    if ($null -eq $process) { return $true }
    if ($Handle.PSObject.Properties['ProcessId'] -and [int]$Handle.ProcessId -ne [int]$process.Id) {
        throw "策划并行准备 exact Process PID 归属不一致:handle=$($Handle.ProcessId) process=$($process.Id)"
    }
    try { $process.Refresh() } catch { }
    if (-not $process.HasExited) {
        try { $process.Kill($true) } catch { }
    }
    if (-not $process.WaitForExit($DrainTimeoutMilliseconds)) { return $false }
    $Handle.Completed = $true
    return $true
}

function Complete-PandoraPlannerPreparationProcess {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]$Handle,
        [ValidateRange(1, 3600000)][int]$TimeoutMilliseconds,
        [ValidateRange(1, 60000)][int]$OutputDrainTimeoutMilliseconds = 2000,
        [switch]$WriteOutput
    )

    Set-StrictMode -Version Latest
    $process = $Handle.Process
    if ($Handle.PSObject.Properties['ProcessId'] -and [int]$Handle.ProcessId -ne [int]$process.Id) {
        throw "策划并行准备 exact Process PID 归属不一致:handle=$($Handle.ProcessId) process=$($process.Id)"
    }
    $timedOut = -not $process.WaitForExit($TimeoutMilliseconds)
    if ($timedOut) {
        $processDrained = Stop-PandoraPlannerPreparationProcess -Handle $Handle -DrainTimeoutMilliseconds 5000
    } else {
        $processDrained = $true
        $Handle.Completed = $true
    }

    # ReadToEndAsync 只有在 child 关闭继承管道后才完成。kill/drain 失败时直接 GetResult()
    # 会把整个一键启动永久挂住；即使进程已退出，异常 child/句柄继承也必须受总 deadline 约束。
    $outputDeadline = [Environment]::TickCount64 + [int64]$OutputDrainTimeoutMilliseconds
    $outputErrors = [Collections.Generic.List[string]]::new()
    $outputValues = @{}
    foreach ($output in @(
            [pscustomobject]@{ Name = 'stdout'; Task = $Handle.StandardOutputTask },
            [pscustomobject]@{ Name = 'stderr'; Task = $Handle.StandardErrorTask }
        )) {
        $remaining = [int][Math]::Max(0, $outputDeadline - [Environment]::TickCount64)
        $completed = $false
        try {
            if ($output.Task.IsCompleted) { $completed = $true }
            elseif ($remaining -gt 0) { $completed = [bool]$output.Task.Wait($remaining) }
            if (-not $completed) {
                $outputErrors.Add("$($output.Name) 管道在 ${OutputDrainTimeoutMilliseconds}ms 总期限内未关闭")
                $outputValues[$output.Name] = ''
                continue
            }
            $outputValues[$output.Name] = "$($output.Task.GetAwaiter().GetResult())"
        } catch {
            $outputErrors.Add("$($output.Name) 收集失败:$($_.Exception.GetBaseException().Message)")
            $outputValues[$output.Name] = ''
        }
    }
    $outputDrained = ($outputErrors.Count -eq 0)
    $drained = $processDrained -and $outputDrained
    $stdout = "$($outputValues['stdout'])"
    $stderr = "$($outputValues['stderr'])"
    if ($outputErrors.Count -gt 0) {
        $drainMessage = "[planner] worker 输出收集未完成:$($outputErrors -join '；')"
        $stderr = if ($stderr) { "$stderr`n$drainMessage" } else { $drainMessage }
    } elseif (-not $processDrained) {
        $drainMessage = '[planner] worker 取消后仍未确认 exact Process 退出'
        $stderr = if ($stderr) { "$stderr`n$drainMessage" } else { $drainMessage }
    } else {
        $drainMessage = ''
    }
    $processExitCode = if ($processDrained) { [int]$process.ExitCode } else { $null }
    if ($WriteOutput) {
        foreach ($line in @($stdout -split "`r?`n" | Where-Object { $_ -ne '' })) { Write-Host $line }
        foreach ($line in @($stderr -split "`r?`n" | Where-Object { $_ -ne '' })) {
            Write-Host $line -ForegroundColor $(if ($timedOut -or -not $drained -or $processExitCode -ne 0) { 'Red' } else { 'DarkGray' })
        }
    }
    # 不能用“父进程 join 时刻 - worker start”：并发图里 build 可能 1s 已完成，父进程
    # 9s 后才来 join，那会把单项耗时错误膨胀成 9s。已正常退出或被 exact handle 取消并
    # drain 后，ExitTime 是 OS 记录的真实完成时刻；只有无法确认退出时才退回单调墙钟。
    $elapsed = if ($processDrained) {
        $process.Refresh()
        $startedAtUtc = if ($Handle.PSObject.Properties['StartedAtUtc']) {
            ([DateTime]$Handle.StartedAtUtc).ToUniversalTime()
        } else {
            $process.StartTime.ToUniversalTime()
        }
        $exitedAtUtc = $process.ExitTime.ToUniversalTime()
        [int64][Math]::Max(0, [Math]::Round(($exitedAtUtc - $startedAtUtc).TotalMilliseconds))
    } else {
        [Math]::Max([int64]0, [Environment]::TickCount64 - [int64]$Handle.StartedAtMilliseconds)
    }
    # public ExitCode 是所有调用方的统一成功闸：一旦超过本阶段 timeout，即使 Stop 阶段
    # 随后确认进程恰好自然以 0 退出，也不能把迟到结果当成本轮成功。真实退出码仍单独保留
    # 在 ProcessExitCode 供诊断；DrainCompleted 只表达 exact Process/输出管道是否已收口。
    $exitCode = if ($timedOut -or -not $drained) { -1 } else { [int]$processExitCode }
    return [pscustomobject][ordered]@{
        Name = "$($Handle.Name)"
        ProcessId = if ($Handle.PSObject.Properties['ProcessId']) { [int]$Handle.ProcessId } else { [int]$process.Id }
        ExitCode = $exitCode
        ProcessExitCode = $processExitCode
        TimedOut = [bool]$timedOut
        DrainCompleted = [bool]$drained
        DrainError = $drainMessage
        ElapsedMilliseconds = [int64]$elapsed
        StandardOutput = $stdout
        StandardError = $stderr
    }
}
