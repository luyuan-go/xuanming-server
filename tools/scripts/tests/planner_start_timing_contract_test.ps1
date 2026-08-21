# 策划免 Docker 一键启动逐阶段耗时契约。
#
# 只验证纯计时 helper 与策划链路接线；不启动基础设施、不连接数据库、不拉起业务进程。

$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
$helper = Join-Path $root 'tools/scripts/lib/planner_start_timing.ps1'

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED: $Message" }
}

function Assert-Contains([string]$Text, [string]$Pattern, [string]$Message) {
    if ($Text -notmatch $Pattern) { throw "ASSERT FAILED: $Message" }
}

Assert-True (Test-Path -LiteralPath $helper -PathType Leaf) '应提供统一的策划启动计时 helper'
. $helper

Start-PandoraPlannerTimingSession -Enabled $true -StartedAtMilliseconds 1000
Add-PandoraPlannerTiming -Name '环境检查' -ElapsedMilliseconds 1234 -Status '完成'
Add-PandoraPlannerTiming -Name '基础设施·Kafka' -ElapsedMilliseconds 9876 -Status '失败' -Detail 'listener timeout'
$lines = @(Get-PandoraPlannerTimingSummaryLines -TotalElapsedMilliseconds 11111)
$summary = $lines -join "`n"

Assert-Contains $summary '^策划一键启动耗时汇总' '汇总必须有稳定标题'
Assert-Contains $summary '(?m)^\[耗时\]\s+环境检查\s+1\.23 秒\s+完成\s*$' '成功步骤应换算成两位小数秒'
Assert-Contains $summary '(?m)^\[耗时\]\s+基础设施·Kafka\s+9\.88 秒\s+失败\s+listener timeout\s*$' '失败步骤也必须保留耗时与原因'
Assert-Contains $summary '(?m)^\[耗时\]\s+总计\s+11\.11 秒\s*$' '汇总必须包含总耗时'

Stop-PandoraPlannerTimingSession
Start-PandoraPlannerTimingSession -Enabled $true -StartedAtMilliseconds 0
$clockValues = [Collections.Generic.Queue[int64]]::new()
foreach ($value in @([int64]100, [int64]1334)) { $clockValues.Enqueue($value) }
$result = Invoke-PandoraPlannerTimedStep -Name '成功动作' -GetElapsedMilliseconds { $clockValues.Dequeue() } `
    -Action { return '原返回值' }
Assert-True ($result -ceq '原返回值') '计时 wrapper 不得吞掉或改写动作返回值'
$successRow = (Get-PandoraPlannerTimingSession).Rows | Select-Object -Last 1
Assert-True ($successRow.Status -ceq '完成' -and $successRow.ElapsedMilliseconds -eq 1234) `
    '成功动作应按虚拟时钟记录完成耗时'

$clockValues = [Collections.Generic.Queue[int64]]::new()
foreach ($value in @([int64]2000, [int64]2456)) { $clockValues.Enqueue($value) }
$caught = ''
try {
    Invoke-PandoraPlannerTimedStep -Name '失败动作' -GetElapsedMilliseconds { $clockValues.Dequeue() } `
        -Action { throw '原始失败' }
} catch { $caught = $_.Exception.Message }
Assert-True ($caught -ceq '原始失败') '计时 wrapper 必须原样继续抛出动作异常'
$failureRow = (Get-PandoraPlannerTimingSession).Rows | Select-Object -Last 1
Assert-True ($failureRow.Status -ceq '失败' -and $failureRow.ElapsedMilliseconds -eq 456) `
    '失败动作也应在 finally 中记录耗时'

Add-PandoraPlannerTiming -Name '复用 Redis' -ElapsedMilliseconds 0 -Status '跳过' -Detail '已在运行'
$reuseRow = (Get-PandoraPlannerTimingSession).Rows | Select-Object -Last 1
Assert-True ($reuseRow.Status -ceq '跳过') '复用步骤不得冒充本轮完成'

$firstSummary = @(Write-PandoraPlannerTimingSummary 6>&1)
$secondSummary = @(Write-PandoraPlannerTimingSummary 6>&1)
Assert-True ($firstSummary.Count -gt 0 -and $secondSummary.Count -eq 0) '外层 finally 重入时汇总只能打印一次'

Stop-PandoraPlannerTimingSession
Start-PandoraPlannerTimingSession -Enabled $false -StartedAtMilliseconds 0
Add-PandoraPlannerTiming -Name '不应出现' -ElapsedMilliseconds 1
Assert-True (@(Get-PandoraPlannerTimingSummaryLines -TotalElapsedMilliseconds 1).Count -eq 0) `
    '非策划入口禁用计时时不得产生输出'
Stop-PandoraPlannerTimingSession

$startText = Get-Content -LiteralPath (Join-Path $root 'tools/scripts/start.ps1') -Raw
$devAllText = Get-Content -LiteralPath (Join-Path $root 'tools/scripts/dev_all.ps1') -Raw
$infraText = Get-Content -LiteralPath (Join-Path $root 'tools/scripts/local_infra.ps1') -Raw
$servicesText = Get-Content -LiteralPath (Join-Path $root 'tools/scripts/run_services.ps1') -Raw

foreach ($stage in @('导表', '环境检查', '等待本机 DS 可玩', 'Write-PandoraPlannerTimingSummary')) {
    Assert-Contains $startText ([regex]::Escape($stage)) "start.ps1 应接入阶段:$stage"
}
foreach ($stage in @('数据库模式与远端工作区校验', '基础设施总计', '数据库结构校验/迁移', '业务程序启动')) {
    Assert-Contains $devAllText ([regex]::Escape($stage)) "dev_all.ps1 应接入阶段:$stage"
}
foreach ($stage in @('依赖准备', '基础设施·Redis', '基础设施·Kafka', '基础设施·Envoy')) {
    Assert-Contains $infraText ([regex]::Escape($stage)) "local_infra.ps1 应接入阶段:$stage"
}
foreach ($stage in @('业务程序·配置生成', '业务程序·构建/复用', '业务程序·进程拉起', '业务程序·端口就绪')) {
    Assert-Contains $servicesText ([regex]::Escape($stage)) "run_services.ps1 应接入阶段:$stage"
}

Write-Host '[PASS] 策划一键启动逐阶段耗时契约通过' -ForegroundColor Green
