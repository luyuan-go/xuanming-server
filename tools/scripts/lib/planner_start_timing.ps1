# 策划一键启动逐阶段耗时记录器。
#
# 只在 start.ps1 为 `local + NoDocker + PANDORA_PLANNER_FAST_START=1` 显式开启时工作。
# 子脚本通过同一 pwsh 进程的 global session 追加记录；普通开发、Docker、K8s 与单独运行
# local_infra/run_services 时全部静默，避免改变既有输出契约。

function Get-PandoraPlannerTimingSession {
    $variable = Get-Variable -Name PandoraPlannerTimingSession -Scope Global -ErrorAction SilentlyContinue
    if (-not $variable) { return $null }
    return $variable.Value
}

function Start-PandoraPlannerTimingSession {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][bool]$Enabled,
        [int64]$StartedAtMilliseconds = [Environment]::TickCount64
    )

    $global:PandoraPlannerTimingSession = [pscustomobject]@{
        Enabled = $Enabled
        StartedAtMilliseconds = $StartedAtMilliseconds
        Rows = [Collections.Generic.List[object]]::new()
        SummaryWritten = $false
    }
}

function Test-PandoraPlannerTimingEnabled {
    $session = Get-PandoraPlannerTimingSession
    return [bool]($session -and $session.Enabled)
}

function Add-PandoraPlannerTiming {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][ValidateRange(0, [long]::MaxValue)][int64]$ElapsedMilliseconds,
        [ValidateSet('完成', '失败', '跳过')][string]$Status = '完成',
        [string]$Detail = ''
    )

    $session = Get-PandoraPlannerTimingSession
    if (-not $session -or -not $session.Enabled) { return }
    $session.Rows.Add([pscustomobject][ordered]@{
        Name = $Name
        ElapsedMilliseconds = $ElapsedMilliseconds
        Status = $Status
        Detail = $Detail.Trim()
    })
}

function Invoke-PandoraPlannerTimedStep {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][scriptblock]$Action,
        [scriptblock]$GetElapsedMilliseconds = { [int64][Environment]::TickCount64 }
    )

    $startedAt = [int64](& $GetElapsedMilliseconds)
    $status = '完成'
    try {
        return & $Action
    } catch {
        $status = '失败'
        throw
    } finally {
        $finishedAt = [int64](& $GetElapsedMilliseconds)
        Add-PandoraPlannerTiming -Name $Name `
            -ElapsedMilliseconds ([Math]::Max([int64]0, $finishedAt - $startedAt)) -Status $status
    }
}

function Format-PandoraPlannerSeconds([int64]$ElapsedMilliseconds) {
    return ($ElapsedMilliseconds / 1000.0).ToString('0.00', [Globalization.CultureInfo]::InvariantCulture)
}

function Get-PandoraPlannerTimingSummaryLines {
    [CmdletBinding()]
    param(
        [int64]$TotalElapsedMilliseconds = -1
    )

    $session = Get-PandoraPlannerTimingSession
    if (-not $session -or -not $session.Enabled) { return }
    if ($TotalElapsedMilliseconds -lt 0) {
        $TotalElapsedMilliseconds = [Environment]::TickCount64 - [int64]$session.StartedAtMilliseconds
    }

    '策划一键启动耗时汇总'
    foreach ($row in $session.Rows) {
        $suffix = if ([string]::IsNullOrWhiteSpace([string]$row.Detail)) { '' } else { "  $($row.Detail)" }
        '[耗时] {0}  {1} 秒  {2}{3}' -f $row.Name,
            (Format-PandoraPlannerSeconds ([int64]$row.ElapsedMilliseconds)), $row.Status, $suffix
    }
    '[耗时] 总计  {0} 秒' -f (Format-PandoraPlannerSeconds $TotalElapsedMilliseconds)
}

function Write-PandoraPlannerTimingSummary {
    [CmdletBinding()]
    param()

    $session = Get-PandoraPlannerTimingSession
    if (-not $session -or -not $session.Enabled -or $session.SummaryWritten) { return }
    $session.SummaryWritten = $true
    Write-Host ''
    foreach ($line in @(Get-PandoraPlannerTimingSummaryLines)) {
        $color = if ($line -match '\s失败(?:\s|$)') { 'Red' } elseif ($line -like '[耗时]*') { 'DarkCyan' } else { 'Cyan' }
        Write-Host $line -ForegroundColor $color
    }
}

function Stop-PandoraPlannerTimingSession {
    Remove-Variable -Name PandoraPlannerTimingSession -Scope Global -ErrorAction SilentlyContinue
}
