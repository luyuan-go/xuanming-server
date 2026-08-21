# 策划免 Docker 一键入口的“窗口结束即可进游戏”契约。
#
# 本测试只解析脚本并在内存里注入虚拟 PID/listener/HTTP 响应；不会启动、停止或探测真实服务。

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ScriptsDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$ProjectRoot = (Resolve-Path (Join-Path $ScriptsDir '../..')).Path
$StartPath = Join-Path $ScriptsDir 'start.ps1'
$DevAllPath = Join-Path $ScriptsDir 'dev_all.ps1'
$RunServicesPath = Join-Path $ScriptsDir 'run_services.ps1'
$PlannerCmdPath = Join-Path $ProjectRoot '策划一键启动-免Docker-测试版.cmd'
$script:Failures = [Collections.Generic.List[string]]::new()

function Assert-True([bool]$Condition, [string]$Message) {
    if ($Condition) {
        Write-Host "  [ok] $Message" -ForegroundColor Green
    } else {
        $script:Failures.Add($Message)
        Write-Host "  [FAIL] $Message" -ForegroundColor Red
    }
}

function Get-FunctionAst([string]$Path, [string]$Name) {
    $tokens = $null
    $parseErrors = $null
    $ast = [Management.Automation.Language.Parser]::ParseFile($Path, [ref]$tokens, [ref]$parseErrors)
    Assert-True (@($parseErrors).Count -eq 0) "$([IO.Path]::GetFileName($Path)) 可由 PowerShell AST 解析"
    return @($ast.FindAll({
                param($Node)
                $Node -is [Management.Automation.Language.FunctionDefinitionAst] -and $Node.Name -ceq $Name
            }, $true)) | Select-Object -First 1
}

$startText = [IO.File]::ReadAllText($StartPath)
$devAllText = [IO.File]::ReadAllText($DevAllPath)
$runServicesText = [IO.File]::ReadAllText($RunServicesPath)
$cmdText = [IO.File]::ReadAllText($PlannerCmdPath)

Write-Host '[1] 标准 Press any key 只能出现在真正成功分支' -ForegroundColor Cyan
$cmdCode = @($cmdText -split "`r?`n" | Where-Object { $_ -notmatch '^\s*(rem\b|::)' }) -join "`n"
$standardPauseLines = @($cmdCode -split "`n" | Where-Object { $_ -match '\bpause\s*$' })
Assert-True ($standardPauseLines.Count -eq 1) '整个入口只有一个会显示标准 Press any key 的裸 pause'
Assert-True ($cmdCode -match '(?is)call\s+"%~dp0tools\\scripts\\bootstrap_pwsh\.cmd".*?set\s+"_PANDORA_BOOTSTRAP_RC=%ERRORLEVEL%".*?if\s+not\s+"%_PANDORA_BOOTSTRAP_RC%"=="0"\s*\(.*?if\s+not\s+defined\s+PANDORA_NONINTERACTIVE\s+pause\s*>\s*nul.*?exit\s+/b\s+%_PANDORA_BOOTSTRAP_RC%\s*\)') `
    'PowerShell 自举失败只静默等确认并返回非零，不显示标准 Press any key'
Assert-True ($cmdCode -match '(?is)set\s+"RC=%ERRORLEVEL%".*?if\s+not\s+"%RC%"=="0"\s*\(.*?pause\s*>\s*nul.*?exit\s+/b\s+%RC%\s*\).*?if\s+not\s+defined\s+PANDORA_NONINTERACTIVE\s+pause\s*\n\s*exit\s+/b\s+0') `
    'start 非零走静默失败确认；仅 RC=0 才执行标准 pause'
Assert-True ($startText -match '现在可以登录进游戏') '成功分支在标准 pause 之前明确告诉策划现在可以登录进游戏'
Assert-True ($cmdCode -match 'if\s+not\s+defined\s+PANDORA_NONINTERACTIVE') 'noninteractive 模式不进入任何 pause'

Write-Host '[2] 完整策划启动必须在 dev_all 成功后做最终可玩闸' -ForegroundColor Cyan
$invokeLocalAst = Get-FunctionAst -Path $StartPath -Name 'Invoke-Local'
$invokeLocalText = if ($invokeLocalAst) { $invokeLocalAst.Extent.Text } else { '' }
Assert-True ($invokeLocalText -match 'dev_all\.ps1[^\r\n]*-ConfigTableChanged:\$script:ConfigTableChanged') `
    'start 把本轮配置表是否变化透传给 dev_all'
$devAllCallIndex = $invokeLocalText.IndexOf('dev_all.ps1', [StringComparison]::Ordinal)
$playableCallIndex = $invokeLocalText.IndexOf('Wait-LocalPlannerPlayable', [StringComparison]::Ordinal)
Assert-True ($devAllCallIndex -ge 0 -and $playableCallIndex -gt $devAllCallIndex) `
    '最终可玩闸严格位于 dev_all 成功之后'
Assert-True ($invokeLocalText -match '(?s)\$NoDocker.*?PANDORA_PLANNER_FAST_START.*?Wait-LocalPlannerPlayable.*?exit\s+1') `
    '最终可玩闸只约束策划免 Docker fast 路径，失败返回非零'
Assert-True ($invokeLocalText -match '(?s)Wait-LocalPlannerPlayable.*?现在可以登录进游戏') `
    '只有最终可玩闸通过才输出可登录成功语'

$playableAst = Get-FunctionAst -Path $StartPath -Name 'Wait-LocalPlannerPlayable'
$playableText = if ($playableAst) { $playableAst.Extent.Text } else { '' }
Assert-True ($playableText -match 'Test-LocalServiceExactTcpListener[^\r\n]*login[^\r\n]*20001') `
    '最终闸用登记的 login exact PID 复核 :20001 listener'
Assert-True ([regex]::Matches($playableText, 'Test-LocalServiceExactTcpListener[^\r\n]*login[^\r\n]*20001').Count -ge 2) `
    '等待 Hub DS 前后都复核 login，不能让等待期间退出的旧 login 冒充可玩'
Assert-True ([regex]::Matches($playableText, 'Test-LocalEnvoyLoginRouteReady').Count -ge 2) `
    '等待 Hub DS 前后都经 Envoy 实际访问 login 路由'
Assert-True ($playableText -match 'Wait-LocalHubDsReady') '最终闸等待当前 Hub DS 的 UDP listener'

Write-Host '[3] exact listener 与 Envoy 探针用纯虚拟边界验证' -ForegroundColor Cyan
$exactListenerAst = Get-FunctionAst -Path $StartPath -Name 'Test-LocalServiceExactTcpListener'
if ($exactListenerAst) {
    Invoke-Expression $exactListenerAst.Extent.Text
    $fakeProcess = [pscustomobject]@{ Id = 41001 }
    $getFakeProcess = { param($Name) $fakeProcess }
    $wrongOwner = { @([pscustomobject]@{ LocalPort = 20001; OwningProcess = 99999 }) }
    $exactOwner = { @([pscustomobject]@{ LocalPort = 20001; OwningProcess = 41001 }) }
    Assert-True (-not (Test-LocalServiceExactTcpListener -Name login -Port 20001 `
                -GetServiceProcess $getFakeProcess -GetListenerRecords $wrongOwner)) `
        '同端口被错误 PID 占用时 login 不得 ready'
    Assert-True (Test-LocalServiceExactTcpListener -Name login -Port 20001 `
            -GetServiceProcess $getFakeProcess -GetListenerRecords $exactOwner) `
        '仅登记 login 的 exact PID listener 才 ready'
}

$envoyProbeAst = Get-FunctionAst -Path $StartPath -Name 'Test-LocalEnvoyLoginRouteReady'
if ($envoyProbeAst) {
    Invoke-Expression $envoyProbeAst.Extent.Text
    Assert-True (-not (Test-LocalEnvoyLoginRouteReady -Request { [pscustomobject]@{ StatusCode = 503 } })) `
        'Envoy 返回 503 时不能宣称 login upstream 可达'
    Assert-True (Test-LocalEnvoyLoginRouteReady -Request { [pscustomobject]@{ StatusCode = 200 } }) `
        'Envoy 实际把只读 gRPC-Web reflection 请求送达 login upstream 后才通过'
}

Write-Host '[4] Hub DS 端口与 UDP owner 都必须确证' -ForegroundColor Cyan
$waitHubAst = Get-FunctionAst -Path $StartPath -Name 'Wait-LocalHubDsReady'
$waitHubText = if ($waitHubAst) { $waitHubAst.Extent.Text } else { '' }
Assert-True ($waitHubText -match 'Get-LocalServiceProcess[^\r\n]*hub_allocator') `
    'Hub readiness 从 pidfile/exe 取得当前 exact hub_allocator，不按同名进程猜'
Assert-True ($waitHubText -match '(?s)if \(\$port -le 0\).*?return \$false') `
    'DS 命令行读不出 UDP 端口时 fail-closed'
Assert-True ($waitHubText -match '(?s)Get-NetUDPEndpoint.*?OwningProcess.*?ProcessId') `
    'UDP listener 的 OwningProcess 必须等于当前 exact Hub DS PID'

if ($waitHubAst) {
    # 把生产函数放进子作用域并虚拟所有进程/端口边界；不会读取本机进程或端口。
    $missingPortReady = & {
        function Write-Info { param($Message) }
        function Write-Warn { param($Message) }
        function Write-Err { param($Message) }
        function Write-Ok { param($Message) }
        function Get-LocalServiceProcess { param($Name) [pscustomobject]@{ Id = 71001; HasExited = $false } }
        function Get-LocalDsChildProcess { param($OwnerPid) [pscustomobject]@{ ProcessId = 72001; CommandLine = '-server ?game=/Script/Pandora.Hub' } }
        Invoke-Expression $waitHubAst.Extent.Text
        Wait-LocalHubDsReady -SpawnTimeoutSeconds 1 -ReadyTimeoutSeconds 1
    }
    Assert-True (-not $missingPortReady) '虚拟 DS 缺少 -port 时实际返回失败'

    $wrongUdpOwnerReady = & {
        function Write-Info { param($Message) }
        function Write-Warn { param($Message) }
        function Write-Err { param($Message) }
        function Write-Ok { param($Message) }
        function Get-LocalServiceProcess { param($Name) [pscustomobject]@{ Id = 71001; HasExited = $false } }
        function Get-LocalDsChildProcess { param($OwnerPid) [pscustomobject]@{ ProcessId = 72001; CommandLine = '-server ?game=/Script/Pandora.Hub -port=7777' } }
        function Get-NetUDPEndpoint { param($LocalPort, $ErrorAction) [pscustomobject]@{ LocalPort = 7777; OwningProcess = 99999 } }
        function Get-Process { param($Id, $Name, $ErrorAction) [pscustomobject]@{ Id = 72001; HasExited = $false } }
        function Start-Sleep { param($Seconds, $Milliseconds) }
        Invoke-Expression $waitHubAst.Extent.Text
        Wait-LocalHubDsReady -SpawnTimeoutSeconds 1 -ReadyTimeoutSeconds 1
    }
    Assert-True (-not $wrongUdpOwnerReady) '虚拟 UDP 同端口由错误 PID 占用时实际返回失败'
}

Write-Host '[5] ConfigTableChanged 贯穿 start -> dev_all -> run_services' -ForegroundColor Cyan
Assert-True ($devAllText -match '(?s)param\(.*?\[switch\]\$ConfigTableChanged') 'dev_all 暴露默认 false 的 ConfigTableChanged switch'
Assert-True ($devAllText -match '(?s)run_services\.ps1"\s+-Exclude\s+\$Exclude\s+-SocialOnMysql\s+-NoDocker.*?-FastExistingProbe:.*?-ConfigTableChanged:\$ConfigTableChanged') `
    'dev_all 的免 Docker up 路径透传 ConfigTableChanged'
Assert-True ($devAllText -match '(?m)^&\s+"\$ScriptDir/run_services\.ps1"\s+-Exclude\s+\$Exclude\s+-ConfigTableChanged:\$ConfigTableChanged\s*$') `
    'dev_all 的普通 Docker up 路径透传 ConfigTableChanged'
Assert-True ($runServicesText -match '(?s)param\(.*?\[switch\]\$ConfigTableChanged') `
    'run_services 已提供 ConfigTableChanged 接口（选择性重启实现由同批任务提供）'

Write-Host ''
if ($script:Failures.Count -gt 0) {
    Write-Host "[ERR] $($script:Failures.Count) 项可玩退出契约未满足:" -ForegroundColor Red
    $script:Failures | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}
Write-Host '[PASS] 策划一键入口只在真正可登录、可进 Hub 后显示标准 Press any key。' -ForegroundColor Green
