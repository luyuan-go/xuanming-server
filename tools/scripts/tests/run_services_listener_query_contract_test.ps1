# run_services 监听端口查询性能 / 归属安全契约。
#
# 只抽取函数并使用内存 netstat / 进程桩；不启动、停止任何真实服务。

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ScriptsDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$RunServices = Join-Path $ScriptsDir 'run_services.ps1'
$LocalInfra = Join-Path $ScriptsDir 'local_infra.ps1'
$StartScript = Join-Path $ScriptsDir 'start.ps1'
$StateLib = Join-Path $ScriptsDir 'lib/local_infra_state.ps1'
$script:Failures = [Collections.Generic.List[string]]::new()

function Assert-True([bool]$Condition, [string]$Message) {
    if ($Condition) { Write-Host "  [ok] $Message" -ForegroundColor Green }
    else { $script:Failures.Add($Message); Write-Host "  [FAIL] $Message" -ForegroundColor Red }
}

$runParseErrors = $null
$runAst = [Management.Automation.Language.Parser]::ParseFile($RunServices, [ref]$null, [ref]$runParseErrors)
if ($runParseErrors -and $runParseErrors.Count -gt 0) { throw "run_services.ps1 语法错误:$($runParseErrors[0].Message)" }
$stateParseErrors = $null
$stateAst = [Management.Automation.Language.Parser]::ParseFile($StateLib, [ref]$null, [ref]$stateParseErrors)
if ($stateParseErrors -and $stateParseErrors.Count -gt 0) { throw "local_infra_state.ps1 语法错误:$($stateParseErrors[0].Message)" }
$localParseErrors = $null
$localAst = [Management.Automation.Language.Parser]::ParseFile($LocalInfra, [ref]$null, [ref]$localParseErrors)
if ($localParseErrors -and $localParseErrors.Count -gt 0) { throw "local_infra.ps1 语法错误:$($localParseErrors[0].Message)" }
$startParseErrors = $null
$startAst = [Management.Automation.Language.Parser]::ParseFile($StartScript, [ref]$null, [ref]$startParseErrors)
if ($startParseErrors -and $startParseErrors.Count -gt 0) { throw "start.ps1 语法错误:$($startParseErrors[0].Message)" }

function Get-FunctionAst([string]$Name, $SourceAst) {
    return @($SourceAst.FindAll({
        param($node)
        $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $Name
    }, $true))
}

Write-Host '[1] netstat 查询 seam 必须唯一且替代慢 CIM' -ForegroundColor Cyan
$sharedRequired = @(
    'ConvertFrom-PandoraNetstatTcpListenerRecords',
    'ConvertFrom-PandoraNetstatTcpListeners',
    'Invoke-PandoraNetstatTcpSnapshot',
    'Get-PandoraTcpListenerRecords',
    'Get-PandoraTcpListenerProcessIds'
)
$runRequired = @('Test-ServiceListenerOwned', 'Clear-PortSquatter')
$startRequired = @('Test-EdgePortHeldByOwnEnvoy', 'Assert-LocalEdgePortsFree')
$functionText = @{}
foreach ($name in $sharedRequired) {
    $matches = @(Get-FunctionAst $name $stateAst)
    Assert-True ($matches.Count -eq 1) "共享状态库存在唯一的 $name"
    if ($matches.Count -eq 1) { $functionText[$name] = $matches[0].Extent.Text }
}
foreach ($name in $runRequired) {
    $matches = @(Get-FunctionAst $name $runAst)
    Assert-True ($matches.Count -eq 1) "存在唯一的 $name"
    if ($matches.Count -eq 1) { $functionText[$name] = $matches[0].Extent.Text }
}
foreach ($name in $startRequired) {
    $matches = @(Get-FunctionAst $name $startAst)
    Assert-True ($matches.Count -eq 1) "start.ps1 存在唯一的 $name"
    if ($matches.Count -eq 1) { $functionText[$name] = $matches[0].Extent.Text }
}

if ($functionText.ContainsKey('Invoke-PandoraNetstatTcpSnapshot')) {
    Assert-True ($functionText['Invoke-PandoraNetstatTcpSnapshot'] -match 'netstat\.exe') '查询只使用 Windows 自带 netstat.exe'
    Assert-True ($functionText['Invoke-PandoraNetstatTcpSnapshot'] -notmatch '(?im)^\s*\$lines\s*=.*-p\s+tcp') `
        'netstat 不用只返回 TCPv4 的 -p tcp，避免漏掉仅绑定 IPv6 的 listener'
}
foreach ($name in 'Test-ServiceListenerOwned', 'Clear-PortSquatter') {
    if ($functionText.ContainsKey($name)) {
        Assert-True ($functionText[$name] -match 'Get-PandoraTcpListenerProcessIds') "$name 走共享 netstat seam"
        Assert-True ($functionText[$name] -notmatch 'Get-NetTCPConnection') "$name 不再调用慢 CIM"
    }
}
$ownedMatches = @(Get-FunctionAst 'Get-PandoraLocalMysqlOwnedProcess' $stateAst)
Assert-True ($ownedMatches.Count -eq 1) '共享状态库存在唯一的 MySQL 四重归属 verifier'
if ($ownedMatches.Count -eq 1) {
    $ownedText = $ownedMatches[0].Extent.Text
    Assert-True ($ownedText -match 'Get-PandoraTcpListenerProcessIds') 'MySQL 四重归属 verifier 也走共享 netstat seam'
    Assert-True ($ownedText -notmatch 'Get-NetTCPConnection') '业务启动 / 迁移的 MySQL 复核不再调用慢 CIM'
}
$localText = [IO.File]::ReadAllText($LocalInfra)
$startText = [IO.File]::ReadAllText($StartScript)
Assert-True ($localText -notmatch 'Get-NetTCPConnection') 'local_infra 全启动/停止/诊断链不再残留慢 CIM'
Assert-True ($startText -notmatch 'Get-NetTCPConnection') 'start 本机边缘端口预检不再残留慢 CIM'
foreach ($entry in @(
        @{ Name = 'Get-OwnedMysqlListenerRecords'; Ast = $localAst },
        @{ Name = 'Get-MysqlListenerRecordsForProcess'; Ast = $localAst },
        @{ Name = 'Stop-OrphanPortHolder'; Ast = $localAst },
        @{ Name = 'Get-PortHolder'; Ast = $localAst },
        @{ Name = 'Assert-LocalEdgePortsFree'; Ast = $startAst }
    )) {
    $matches = @(Get-FunctionAst $entry.Name $entry.Ast)
    Assert-True ($matches.Count -eq 1) "存在唯一的 $($entry.Name)"
    if ($matches.Count -eq 1) {
        Assert-True ($matches[0].Extent.Text -match 'Get-PandoraTcpListenerRecords') "$($entry.Name) 复用共享 listener 快照"
    }
}

if ($functionText.Count -eq ($sharedRequired.Count + $runRequired.Count + $startRequired.Count)) {
    foreach ($name in 'ConvertFrom-PandoraNetstatTcpListenerRecords', 'ConvertFrom-PandoraNetstatTcpListeners',
        'Invoke-PandoraNetstatTcpSnapshot', 'Get-PandoraTcpListenerRecords', 'Get-PandoraTcpListenerProcessIds',
        'Test-ServiceListenerOwned', 'Clear-PortSquatter',
        'Test-EdgePortHeldByOwnEnvoy', 'Assert-LocalEdgePortsFree') {
        Invoke-Expression $functionText[$name]
    }

    Write-Host '[2] IPv4 / IPv6 / 状态 / 端口 / PID 解析' -ForegroundColor Cyan
    $fixture = @(
        '  Proto  Local Address          Foreign Address        State           PID',
        '  TCP    0.0.0.0:2000           0.0.0.0:0              LISTENING       111',
        '  TCP    127.0.0.1:2000         0.0.0.0:0              LISTENING       222',
        '  TCP    [::]:2000              [::]:0                 LISTENING       111',
        '  TCP    [fe80::1%12]:2000      [::]:0                 LISTENING       333',
        '  TCP    127.0.0.1:2000         127.0.0.1:51000        ESTABLISHED     444',
        '  TCP    0.0.0.0:20001          0.0.0.0:0              LISTENING       555',
        '  TCP    0.0.0.0:2001           0.0.0.0:0              LISTENING       666',
        '  TCP    0.0.0.0:2000           0.0.0.0:0              LISTENING       0',
        '  UDP    0.0.0.0:2000           *:*                                    777',
        '  malformed row'
    )
    $parsed = @(ConvertFrom-PandoraNetstatTcpListeners -Lines $fixture -Port 2000)
    Assert-True (($parsed | Sort-Object) -join ',' -eq '111,222,333') `
        '同时解析 IPv4/IPv6 listener，并按 PID 去重'
    Assert-True ($parsed -notcontains 444) '非 LISTENING 连接不误判'
    Assert-True ($parsed -notcontains 555 -and $parsed -notcontains 666) '邻近/前缀端口不误判'
    Assert-True ($parsed -notcontains 0 -and $parsed -notcontains 777) 'PID 0 与 UDP 行不进入 TCP listener'
    $records = @(ConvertFrom-PandoraNetstatTcpListenerRecords -Lines $fixture)
    Assert-True (@($records | Where-Object { $_.LocalAddress -eq '::' -and $_.LocalPort -eq 2000 -and $_.OwningProcess -eq 111 }).Count -eq 1) `
        '共享记录保留 IPv6 本地地址，供入口通配地址冲突判断'
    Assert-True (@($records | Where-Object { $_.LocalAddress -eq 'fe80::1%12' -and $_.OwningProcess -eq 333 }).Count -eq 1) `
        '带 scope id 的 IPv6 地址不会被截断'

    Write-Host '[3] netstat 非零必须 fail-closed' -ForegroundColor Cyan
    function Invoke-PandoraNetstatTcpSnapshot {
        return [pscustomobject]@{ ExitCode = 23; Lines = @('partial output') }
    }
    $caught = $null
    try { Get-PandoraTcpListenerProcessIds -Port 20001 | Out-Null } catch { $caught = $_.Exception.Message }
    Assert-True ($caught -match 'netstat.*23') 'netstat 非零退出向上报告，不把部分输出当真实 listener'
    function Invoke-PandoraNetstatTcpSnapshot {
        return [pscustomobject]@{ ExitCode = 0; Lines = @('unexpected successful output') }
    }
    $caught = $null
    try { Get-PandoraTcpListenerProcessIds -Port 20001 | Out-Null } catch { $caught = $_.Exception.Message }
    Assert-True ($caught -match '格式|无法识别|unknown') 'netstat 成功但格式异常返回未知，不冒充端口空闲'

    Write-Host '[4] 调用方动态禁止慢 CIM，并保留 PID / exact exe 安全闸' -ForegroundColor Cyan
    $script:LegacyCimCalls = 0
    function Get-NetTCPConnection {
        [CmdletBinding()]
        param([Parameter(ValueFromRemainingArguments = $true)]$Rest)
        $script:LegacyCimCalls++
        throw 'PANDORA_LEGACY_CIM_MUST_NOT_RUN'
    }
    $script:ListenerPids = @(111, 222, 333)
    $script:ListenerQueryFails = $false
    function Get-PandoraTcpListenerProcessIds([int]$Port) {
        if ($script:ListenerQueryFails) { throw 'PANDORA_NETSTAT_FAILED' }
        return @($script:ListenerPids)
    }

    $svc = @{ Name = 'fixture_service'; Port = 20001 }
    Assert-True (Test-ServiceListenerOwned $svc ([pscustomobject]@{ Id = 222 })) `
        'listener PID 等于刚启动进程时才认作 owned'
    Assert-True (-not (Test-ServiceListenerOwned $svc ([pscustomobject]@{ Id = 999 }))) `
        '端口由其它 PID 监听时不误报刚启动进程 ready'

    $BinDir = 'C:\pandora\run\dev\bin'
    $script:StoppedPids = [Collections.Generic.List[int]]::new()
    function Get-Process {
        [CmdletBinding()]
        param([int]$Id)
        switch ($Id) {
            111 { return [pscustomobject]@{ Id = 111; Path = 'C:\pandora\run\dev\bin\fixture_service.exe'; ProcessName = 'fixture_service' } }
            222 { return [pscustomobject]@{ Id = 222; Path = 'C:\external\fixture_service.exe'; ProcessName = 'fixture_service' } }
            333 { return [pscustomobject]@{ Id = 333; Path = $null; ProcessName = 'fixture_service' } }
            default { return $null }
        }
    }
    function Stop-Process {
        [CmdletBinding()]
        param([int]$Id, [switch]$Force)
        $script:StoppedPids.Add($Id)
    }
    function Test-PortOpen([int]$Port) { return $false }
    Clear-PortSquatter $svc
    Assert-True (($script:StoppedPids -join ',') -eq '111') `
        '端口残留只清 exact exe；外部同名和路径不可读进程都不杀'
    Assert-True ($script:LegacyCimCalls -eq 0) `
        'Test-ServiceListenerOwned / Clear-PortSquatter 动态执行均未调用 Get-NetTCPConnection'

    $script:ListenerQueryFails = $true
    $beforeStops = $script:StoppedPids.Count
    Assert-True (-not (Test-ServiceListenerOwned $svc ([pscustomobject]@{ Id = 111 }))) `
        'netstat 失败时 readiness fail-closed'
    Clear-PortSquatter $svc
    Assert-True ($script:StoppedPids.Count -eq $beforeStops) `
        'netstat 失败时残留清理 fail-closed，不凭猜测杀进程'

    Write-Host '[5] Envoy 端口归属必须覆盖全部真正 blocker' -ForegroundColor Cyan
    $ProjectRoot = 'C:\pandora'
    $NoDocker = $true
    $Check = $false
    $env:PANDORA_EDGE_BIND_HOST = '127.0.0.1'
    $env:PANDORA_DS_EDGE_BIND_HOST = '127.0.0.1'
    function Write-Err([string]$Message) { }
    function Write-Warn([string]$Message) { }
    function Write-Info([string]$Message) { }
    function Test-CommandExists([string]$Name) { return $false }
    function Get-Process {
        [CmdletBinding()]
        param([int]$Id, [Parameter(ValueFromRemainingArguments = $true)]$Rest)
        switch ($Id) {
            811 { return [pscustomobject]@{ Id = 811; Path = 'C:\pandora\run\localinfra\dist\envoy\envoy.exe'; ProcessName = 'envoy' } }
            822 { return [pscustomobject]@{ Id = 822; Path = 'C:\external\envoy.exe'; ProcessName = 'envoy' } }
            default { return $null }
        }
    }
    $script:EdgeListenerQueryFails = $false
    $script:EdgeListeners = @()
    function Get-PandoraTcpListenerRecords {
        if ($script:EdgeListenerQueryFails) { throw 'PANDORA_EDGE_LISTENER_UNKNOWN' }
        return @($script:EdgeListeners)
    }

    $script:EdgeListeners = @(
        [pscustomobject]@{ LocalAddress = '::1'; LocalPort = 8443; OwningProcess = 811 },
        [pscustomobject]@{ LocalAddress = '0.0.0.0'; LocalPort = 8443; OwningProcess = 822 }
    )
    Assert-True (-not (Assert-LocalEdgePortsFree)) `
        '非阻塞地址上的自家 Envoy 不能遮住真正阻塞的外部 listener'

    $script:EdgeListeners = @(
        [pscustomobject]@{ LocalAddress = '127.0.0.1'; LocalPort = 8443; OwningProcess = 811 },
        [pscustomobject]@{ LocalAddress = '0.0.0.0'; LocalPort = 8443; OwningProcess = 822 }
    )
    Assert-True (-not (Assert-LocalEdgePortsFree)) `
        '同端口全部 blocker 中只要有一个不归本项目，就必须 fail-closed'

    $script:EdgeListeners = @(
        [pscustomobject]@{ LocalAddress = '127.0.0.1'; LocalPort = 8443; OwningProcess = 811 },
        [pscustomobject]@{ LocalAddress = '0.0.0.0'; LocalPort = 8443; OwningProcess = 811 }
    )
    Assert-True (Assert-LocalEdgePortsFree) '全部 blocker 都是本项目原生 Envoy 时允许重建'

    $script:EdgeListenerQueryFails = $true
    Assert-True (-not (Assert-LocalEdgePortsFree)) '边缘 listener 查询失败时不把未知状态当空闲'
    Remove-Item Env:PANDORA_EDGE_BIND_HOST -ErrorAction SilentlyContinue
    Remove-Item Env:PANDORA_DS_EDGE_BIND_HOST -ErrorAction SilentlyContinue
}

if ($script:Failures.Count -gt 0) {
    Write-Host ''
    Write-Host "[FAIL] run_services listener 查询契约失败 $($script:Failures.Count) 项" -ForegroundColor Red
    exit 1
}

Write-Host ''
Write-Host '[PASS] run_services listener 查询性能 / 归属安全契约' -ForegroundColor Green
