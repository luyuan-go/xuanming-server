# 策划免 Docker 基础设施极速启动的纯协调 seam。
#
# 本文件不认识 MySQL/Redis/Kafka/Envoy，也不直接碰端口、进程或文件。调用方在同一
# PowerShell runspace 提供 launcher、listener 快照与归属判定，从而做到“全部先拉起，
# 再共用一份快照统一等待”，同时保留每个组件自己的 deadline 和失败诊断。

function Test-PandoraPlannerInfraBatchEligibility {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][bool]$PlannerFastStart,
        [Parameter(Mandatory)][bool]$Force,
        [Parameter(Mandatory)][bool]$ReceiptReady,
        [Parameter(Mandatory)][bool]$CentralMysqlManaged,
        [Parameter(Mandatory)][bool]$MysqlInitialized,
        [Parameter(Mandatory)][bool]$KafkaInitialized
    )
    Set-StrictMode -Version Latest

    if (-not $PlannerFastStart -or $Force -or -not $ReceiptReady -or -not $KafkaInitialized) {
        return $false
    }
    # 中心 MySQL 模式从不读取、更不启动本机 MySQL；因此不要求本机 data/mysql 已初始化。
    return $CentralMysqlManaged -or $MysqlInitialized
}

function Test-PandoraPlannerInfraListenerOwnership {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]$State,
        [Parameter(Mandatory)][AllowEmptyCollection()][object[]]$Listeners,
        [Parameter(Mandatory)][scriptblock]$GetProcessIdentity
    )
    Set-StrictMode -Version Latest

    $listenerProcessIds = [Collections.Generic.HashSet[int]]::new()
    foreach ($port in @($State.Ports)) {
        $portProcessIds = @($Listeners |
            Where-Object { [int]$_.LocalPort -eq [int]$port } |
            Select-Object -ExpandProperty OwningProcess -Unique)
        if ($portProcessIds.Count -ne 1 -or [int]$portProcessIds[0] -le 0) { return $false }
        $null = $listenerProcessIds.Add([int]$portProcessIds[0])
    }
    # 同一组件声明的全部端口必须由同一个精确进程持有；不同 PID 各占一半不能拼成 ready。
    if ($listenerProcessIds.Count -ne 1) { return $false }
    $listenerProcessId = [int]@($listenerProcessIds)[0]

    $identity = @(& $GetProcessIdentity $listenerProcessId) | Select-Object -First 1
    if (-not $identity -or [int]$identity.ProcessId -ne $listenerProcessId) { return $false }
    switch ([string]$State.ListenerOwnerKind) {
        'direct' {
            if ($listenerProcessId -ne [int]$State.Process.Id) { return $false }
        }
        'child' {
            if ([int]$identity.ParentProcessId -ne [int]$State.Process.Id) { return $false }
        }
        default { return $false }
    }

    try {
        $expectedExecutable = [IO.Path]::GetFullPath([string]$State.ExpectedExecutable)
        $actualExecutable = [IO.Path]::GetFullPath([string]$identity.ExecutablePath)
    } catch { return $false }
    if (-not [string]::Equals($expectedExecutable, $actualExecutable,
        [StringComparison]::OrdinalIgnoreCase)) { return $false }

    $commandLine = [string]$identity.CommandLine
    if ([string]::IsNullOrWhiteSpace($commandLine)) { return $false }
    foreach ($token in @($State.RequiredCommandLineTokens)) {
        if ([string]::IsNullOrWhiteSpace([string]$token) -or
            $commandLine.IndexOf([string]$token, [StringComparison]::OrdinalIgnoreCase) -lt 0) {
            return $false
        }
    }
    return $true
}

function Invoke-PandoraPlannerInfraBatch {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][AllowEmptyCollection()][scriptblock[]]$Launchers,
        [Parameter(Mandatory)][scriptblock]$GetListenerRecords,
        [Parameter(Mandatory)][scriptblock]$TestProcessExited,
        [Parameter(Mandatory)][scriptblock]$TestStateReady,
        [Parameter(Mandatory)][scriptblock]$OnFailure,
        [Parameter(Mandatory)][scriptblock]$Sleep,
        [Parameter(Mandatory)][scriptblock]$GetElapsedMilliseconds,
        [switch]$StopOnFirstFailure,
        [ValidateRange(1, 10000)][int]$PollMilliseconds = 100
    )
    # 本函数会在自己的动态子作用域调用 local_infra.ps1 的既有 launcher。不能在这里
    # 开 StrictMode：它会被 launcher 及其旧 helper 继承，把原本合法的标量 `.Count`
    # 读取变成异常。helper 自身的纯判定函数仍各自在本地作用域开启 StrictMode。

    $states = [Collections.Generic.List[object]]::new()
    foreach ($launcher in $Launchers) {
        # 普通 scriptblock 调用保持在当前 runspace；刻意不使用 Start-Job、ThreadJob 或
        # ForEach-Object -Parallel，避免丢失外层生命周期锁与 TMP/MYSQL_PWD 等进程环境。
        foreach ($state in @(& $launcher)) {
            if ($null -ne $state) { $states.Add($state) }
        }
    }
    if ($states.Count -eq 0) { return @() }

    while (@($states | Where-Object { -not $_.Ready -and -not $_.Failure }).Count -gt 0) {
        # 一轮只抓一份 listener 快照。异常直接向上传播，不能把 netstat 失败冒充“尚未 ready”。
        $listeners = @(& $GetListenerRecords)
        $now = [int64](& $GetElapsedMilliseconds)
        $failedThisRound = $false
        foreach ($state in @($states | Where-Object { -not $_.Ready -and -not $_.Failure })) {
            if ([bool](& $TestProcessExited $state)) {
                $state.Failure = 'process-exited'
                if ($state.PSObject.Properties['FinishedAtMilliseconds']) {
                    $state.FinishedAtMilliseconds = $now
                } else {
                    $state | Add-Member -NotePropertyName FinishedAtMilliseconds -NotePropertyValue $now
                }
                $null = & $OnFailure $state $state.Failure
                $failedThisRound = $true
                continue
            }
            if ([bool](& $TestStateReady $state $listeners)) {
                $state.Ready = $true
                if ($state.PSObject.Properties['ReadyAtMilliseconds']) {
                    $state.ReadyAtMilliseconds = $now
                } else {
                    $state | Add-Member -NotePropertyName ReadyAtMilliseconds -NotePropertyValue $now
                }
                if ($state.PSObject.Properties['FinishedAtMilliseconds']) {
                    $state.FinishedAtMilliseconds = $now
                } else {
                    $state | Add-Member -NotePropertyName FinishedAtMilliseconds -NotePropertyValue $now
                }
                continue
            }
            if (($now - [int64]$state.StartedAtMilliseconds) -ge [int64]$state.TimeoutMilliseconds) {
                $state.Failure = 'ready-timeout'
                if ($state.PSObject.Properties['FinishedAtMilliseconds']) {
                    $state.FinishedAtMilliseconds = $now
                } else {
                    $state | Add-Member -NotePropertyName FinishedAtMilliseconds -NotePropertyValue $now
                }
                $null = & $OnFailure $state $state.Failure
                $failedThisRound = $true
            }
        }

        if ($StopOnFirstFailure -and $failedThisRound) {
            foreach ($state in @($states | Where-Object { -not $_.Ready -and -not $_.Failure })) {
                $state.Failure = 'batch-aborted'
                if ($state.PSObject.Properties['FinishedAtMilliseconds']) {
                    $state.FinishedAtMilliseconds = $now
                } else {
                    $state | Add-Member -NotePropertyName FinishedAtMilliseconds -NotePropertyValue $now
                }
            }
            break
        }

        if (@($states | Where-Object { -not $_.Ready -and -not $_.Failure }).Count -gt 0) {
            $null = & $Sleep $PollMilliseconds
        }
    }
    return $states.ToArray()
}
