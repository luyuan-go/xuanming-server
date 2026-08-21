# 策划“只停业务、保留本机基础设施”入口契约。
# 仅做静态检查，不执行 CMD，不启停任何进程或 K8s。

$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$keepInfraCmd = Join-Path $projectRoot '策划一键停止业务-保留基础设施-免Docker-测试版.cmd'
$fullStopCmd = Join-Path $projectRoot '策划一键停止-免Docker-测试版.cmd'
$runServices = Join-Path $projectRoot 'tools/scripts/run_services.ps1'
$failures = [Collections.Generic.List[string]]::new()

function Assert-True([bool]$Condition, [string]$Message) {
    if ($Condition) { Write-Host "  [ok] $Message" -ForegroundColor Green }
    else { $failures.Add($Message); Write-Host "  [FAIL] $Message" -ForegroundColor Red }
}

Write-Host '[1] 新入口必须只停止业务和本机 DS' -ForegroundColor Cyan
Assert-True (Test-Path -LiteralPath $keepInfraCmd -PathType Leaf) '存在独立的保留基础设施停止入口'
if (Test-Path -LiteralPath $keepInfraCmd -PathType Leaf) {
    $keepText = [IO.File]::ReadAllText($keepInfraCmd)
    $activeText = @(($keepText -split "`r?`n") | Where-Object {
            $_ -notmatch '^\s*(?i:rem)(?:\s|$)' -and $_ -notmatch '^\s*::'
        }) -join "`n"
    Assert-True ($activeText -match 'bootstrap_pwsh\.cmd') '入口沿用便携 PowerShell 自举'
    Assert-True ($activeText -match '(?i)-File\s+"%~dp0tools\\scripts\\run_services\.ps1"\s+-Action\s+down') `
        '入口只调用业务服务 down seam'
    Assert-True ($activeText -notmatch '(?i)start\.ps1|dev_all\.ps1|local_infra\.ps1|dev_down\.ps1') `
        '入口不经过任何会停止基础设施的脚本'
    Assert-True ($activeText -notmatch '(?i)\bdocker\b|\bkubectl\b|\bminikube\b') `
        '入口不执行 Docker 或 K8s 生命周期命令'
    Assert-True ($keepText -match 'MySQL / Redis / Kafka / Envoy were not stopped') `
        '成功提示明确基础设施未被停止'
    Assert-True ($keepText -match 'next one-click start can reuse') `
        '成功提示明确下次启动可复用仍在运行的基础设施'
    Assert-True ($keepText -match 'set "RC=%ERRORLEVEL%"' -and $keepText -match 'exit /b %RC%') `
        '入口原样透传停止失败退出码'
    Assert-True ($keepText -match 'if not defined PANDORA_NONINTERACTIVE pause') `
        '交互双击保留窗口，非交互调用不挂起'
}

Write-Host '[2] 业务 down 必须覆盖 allocator 拉起的本机 DS' -ForegroundColor Cyan
$runText = [IO.File]::ReadAllText($runServices)
Assert-True ($runText -match '(?s)function Stop-Service.*?if \(\$LocalDsSpawners -contains \$svc\.Name\) \{ Clear-LocalDsProcesses \$svc \$proc \}.*?Stop-Process') `
    '停止 allocator 前先停止其 exact 本机 DS 子进程'
Assert-True ($runText -match '(?s)''down''\s*\{.*?foreach \(\$svc in \$stopTargets\).*?Stop-Service \$svc') `
    '业务 down 对全部登记服务调用统一 Stop-Service'

Write-Host '[3] 原完整停止入口必须保留' -ForegroundColor Cyan
$fullText = [IO.File]::ReadAllText($fullStopCmd)
Assert-True ($fullText -match '(?i)start\.ps1"\s+-Mode\s+local\s+-NoDocker\s+-Down') `
    '原入口仍负责业务与基础设施的完整停止'

if ($failures.Count -gt 0) {
    throw "策划保留基础设施停止契约失败($($failures.Count)):`n - $($failures -join "`n - ")"
}
Write-Host '[PASS] 策划保留基础设施停止契约通过' -ForegroundColor Green
