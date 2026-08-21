$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$root = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
$startPath = Join-Path $root 'tools/scripts/start.ps1'

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED: $Message" }
}

$tokens = $null
$errors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile($startPath, [ref]$tokens, [ref]$errors)
Assert-True (@($errors).Count -eq 0) 'start.ps1 AST 必须可解析'

function Get-FunctionText([string]$Name) {
    $node = @($ast.FindAll({
        param($candidate)
        $candidate -is [Management.Automation.Language.FunctionDefinitionAst] -and $candidate.Name -ceq $Name
    }, $true)) | Select-Object -First 1
    Assert-True ($null -ne $node) "缺少函数:$Name"
    return $node.Extent.Text
}

$parserText = Get-FunctionText 'ConvertFrom-PandoraRoutePrintDefaultIPv4'
$decisionText = Get-FunctionText 'Test-PandoraShouldAutoStopK8sForLocal'
Invoke-Expression $parserText
Invoke-Expression $decisionText

$fixture = @(
    '===========================================================================',
    'IPv4 Route Table',
    '===========================================================================',
    'Active Routes:',
    'Network Destination        Netmask          Gateway       Interface  Metric',
    '          0.0.0.0          0.0.0.0       10.20.0.1       10.20.0.88     55',
    '          0.0.0.0          0.0.0.0     192.168.2.1     192.168.2.28     25',
    '        127.0.0.0        255.0.0.0         On-link         127.0.0.1    331'
)
Assert-True ((ConvertFrom-PandoraRoutePrintDefaultIPv4 -Lines $fixture) -ceq '192.168.2.28') `
    '必须选择 metric 最小的默认路由 interface IPv4'
Assert-True (-not (ConvertFrom-PandoraRoutePrintDefaultIPv4 -Lines @(
    '0.0.0.0 0.0.0.0 127.0.0.1 127.0.0.1 1',
    '0.0.0.0 0.0.0.0 169.254.1.1 169.254.1.2 2'
))) '回环/link-local 不得冒充局域网地址'
Assert-True (-not (ConvertFrom-PandoraRoutePrintDefaultIPv4 -Lines @('localized header only'))) `
    '空/异常 route 输出必须返回未知，由生产路径回退'

Assert-True (-not (Test-PandoraShouldAutoStopK8sForLocal -NoDocker $true -PlannerFastStart $true)) `
    '策划免 Docker 快速入口不得启停 K8s'
Assert-True (Test-PandoraShouldAutoStopK8sForLocal -NoDocker $true -PlannerFastStart $false) `
    '普通 local 入口保持原有 K8s 互斥生命周期'
Assert-True (Test-PandoraShouldAutoStopK8sForLocal -NoDocker $false -PlannerFastStart $true) `
    '非免 Docker 入口不得被策划优化误改'

$resolveText = Get-FunctionText 'Resolve-LanIp'
Assert-True ($resolveText -match 'Invoke-PandoraRoutePrintIPv4') '局域网 IP 必须先走 route.exe 快路'
Assert-True ($resolveText -match 'Get-NetRoute') 'route.exe 失败时必须保留 NetTCPIP 回退'
$prereqText = Get-FunctionText 'Resolve-Prerequisites'
Assert-True ($prereqText -match 'Test-PandoraShouldAutoStopK8sForLocal') '前置检查必须调用 K8s 生命周期边界'
Assert-True ($prereqText -match 'Assert-LocalEdgePortsFree') '策划快路仍必须保留边缘端口 fail-closed 检查'

Write-Host '[PASS] 策划热启动前置检查性能/安全契约通过' -ForegroundColor Green
