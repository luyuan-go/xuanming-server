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

Assert-True ($null -ne (Get-Command Set-PandoraPlannerInfraTimingMode -ErrorAction SilentlyContinue)) `
    '[RED] timing session 必须提供本轮基础设施并行/串行模式 seam'
Start-PandoraPlannerTimingSession -Enabled $true -StartedAtMilliseconds 1000
$unregisteredModeSummary = @(Get-PandoraPlannerTimingSummaryLines -TotalElapsedMilliseconds 0) -join "`n"
Assert-True ($unregisteredModeSummary -notmatch '基础设施组件.*并行') `
    '尚未通过 batch eligibility 前不得提前宣称基础设施组件并行'
Stop-PandoraPlannerTimingSession

Start-PandoraPlannerTimingSession -Enabled $true -StartedAtMilliseconds 1000
Set-PandoraPlannerInfraTimingMode -Mode parallel
Add-PandoraPlannerTiming -Name '环境检查' -ElapsedMilliseconds 1234 -Status '完成'
Add-PandoraPlannerTiming -Name '基础设施·Kafka' -ElapsedMilliseconds 9876 -Status '失败' -Detail 'listener timeout'
$lines = @(Get-PandoraPlannerTimingSummaryLines -TotalElapsedMilliseconds 11111)
$summary = $lines -join "`n"

Assert-Contains $summary '^策划一键启动耗时汇总' '汇总必须有稳定标题'
Assert-Contains $summary '(?m)^\[耗时\]\s+环境检查\s+1\.23 秒\s+完成\s*$' '成功步骤应换算成两位小数秒'
Assert-Contains $summary '(?m)^\[耗时\]\s+基础设施·Kafka\s+9\.88 秒\s+失败\s+listener timeout\s*$' '失败步骤也必须保留耗时与原因'
Assert-Contains $summary '导表、staging build、基础设施以及部分迁移会重叠' '汇总必须说明并行明细不可相加'
Assert-Contains $summary '本轮基础设施组件并行，单项耗时不可相加' `
    'batch eligible 本轮才允许宣称基础设施组件并行'
Assert-True ($summary -notmatch '本轮基础设施串行') '并行本轮不得同时打印串行说明'
Assert-Contains $summary '(?m)^\[耗时\]\s+总计\s+11\.11 秒\s*$' '汇总必须包含总耗时'

Stop-PandoraPlannerTimingSession
Start-PandoraPlannerTimingSession -Enabled $true -StartedAtMilliseconds 0
Set-PandoraPlannerInfraTimingMode -Mode serial
$serialSummary = @(Get-PandoraPlannerTimingSummaryLines -TotalElapsedMilliseconds 0) -join "`n"
Assert-Contains $serialSummary '本轮基础设施串行' 'fallback 本轮必须明确打印基础设施串行'
Assert-True ($serialSummary -notmatch '基础设施组件.*并行') `
    'serial fallback 不得继续宣称基础设施组件并行'
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

Add-PandoraPlannerTiming -Name '复用 Redis' -ElapsedMilliseconds 37 -Status '复用' -Detail '已在运行'
$reuseRow = (Get-PandoraPlannerTimingSession).Rows | Select-Object -Last 1
Assert-True ($reuseRow.Status -ceq '复用' -and $reuseRow.ElapsedMilliseconds -eq 37) `
    '复用步骤必须保留实际校验耗时且不得冒充本轮完成'

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
$cmdText = Get-Content -LiteralPath (Join-Path $root '策划一键启动-免Docker-测试版.cmd') -Raw

Assert-Contains $cmdText 'PANDORA_PWSH_BOOTSTRAP_MS' 'CMD 应计量 PowerShell 自举并传给统一汇总'
Assert-Contains $cmdText 'PANDORA_CMD_STARTED_CS' 'CMD 应传递端到端起点，不漏 pwsh 进程启动与脚本装载'
Assert-Contains $cmdText '_PANDORA_BOOT_ELAPSED_CS\+=8640000' 'PowerShell 自举计时必须处理跨午夜回绕'
Assert-Contains $cmdText 'exit /b %_PANDORA_BOOTSTRAP_RC%' 'PowerShell 自举失败必须保留原退出码'

foreach ($stage in @('PowerShell 自举', 'PowerShell 启动/脚本装载', '导表', '环境检查', '等待本机 DS 可玩', 'Write-PandoraPlannerTimingSummary')) {
    Assert-Contains $startText ([regex]::Escape($stage)) "start.ps1 应接入阶段:$stage"
}
Assert-Contains $startText 'TickCount64\s*-\s*\$plannerEntryMilliseconds' `
    '统一总计的 session 起点必须回拨 CMD 到主流程的全部耗时'
Assert-Contains $startText '\$plannerEntryCentiseconds\s*\+=\s*\[int64\]8640000' `
    'CMD 到 start.ps1 的端到端计时必须处理跨午夜回绕'
foreach ($stage in @('数据库模式与远端工作区校验', '基础设施总计', '数据库结构校验/迁移', '业务程序启动')) {
    Assert-Contains $devAllText ([regex]::Escape($stage)) "dev_all.ps1 应接入阶段:$stage"
}
foreach ($stage in @('依赖准备', '基础设施·Redis', '基础设施·Kafka', '基础设施·Envoy')) {
    Assert-Contains $infraText ([regex]::Escape($stage)) "local_infra.ps1 应接入阶段:$stage"
}
foreach ($stage in @('业务程序·配置生成', '业务程序·构建/复用', '业务程序·进程拉起', '业务程序·端口就绪')) {
    Assert-Contains $servicesText ([regex]::Escape($stage)) "run_services.ps1 应接入阶段:$stage"
}
Assert-Contains $servicesText '\$plannerConfigStatus\s*=\s*''跳过''' `
    '构建失败时未执行的配置阶段不得误报复用'
Assert-Contains $servicesText '\$plannerLaunchStatus\s*=\s*''跳过''' `
    '配置失败时未执行的拉起阶段不得误报复用'
Assert-Contains $servicesText '(?s)\$readyWatch\.Start\(\).*?\$finalListenerRecords\s*=\s*@\(Get-PandoraTcpListenerRecords\).*?\$readyWatch\.Stop\(\)' `
    '全复用路径的最终 exact-listener 核验也必须统计实际耗时'
Assert-Contains $infraText 'TimingStartedAtMilliseconds' `
    'Kafka KRaft 格式化等进程前操作必须纳入组件明细耗时'
Assert-Contains $infraText '(?s)finally\s*\{.*?\$state\.FinishedAtMilliseconds\s*=\s*\[Environment\]::TickCount64.*?Add-PlannerInfraStateTiming' `
    '组件成功/失败都必须把协议探活和收尾纳入耗时'

Write-Host '[PASS] 策划一键启动逐阶段耗时契约通过' -ForegroundColor Green
