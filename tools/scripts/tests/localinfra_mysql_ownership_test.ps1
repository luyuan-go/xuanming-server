# 免 Docker MySQL 必须与机器上已有的 MySQL / Docker 实例隔离。
#
# 本测试只从 local_infra.ps1 抽取函数，在随机临时 TCP 端口和内存桩上运行；
# 不访问 3307，不启动/停止真实 mysqld，也不调用 Docker。

$ErrorActionPreference = 'Stop'
$script:Failed = $false

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) {
        $script:Failed = $true
        Write-Host "  [FAIL] $Message" -ForegroundColor Red
    } else {
        Write-Host "  [ok] $Message" -ForegroundColor Green
    }
}

$infra = Join-Path (Split-Path -Parent $PSScriptRoot) 'local_infra.ps1'
. (Join-Path (Split-Path -Parent $PSScriptRoot) 'lib/local_infra_state.ps1')
$errs = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($infra, [ref]$null, [ref]$errs)
if ($errs -and $errs.Count -gt 0) { throw "local_infra.ps1 语法错误:$($errs[0].Message)" }

function Get-InfraFunctionText([string]$Name) {
    $fn = @($ast.FindAll({
        param($n)
        $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $Name
    }, $true))
    if ($fn.Count -ne 1) { throw "local_infra.ps1 里找不到唯一的 $Name(找到 $($fn.Count) 个)" }
    return $fn[0].Extent.Text
}

foreach ($name in @('Test-TcpEndpoint', 'Test-PortOpen', 'Start-LocalMysql', 'Stop-Component', 'Test-ComponentProcessOwned',
        'Request-GracefulStop', 'Invoke-Down', 'Invoke-Reset', 'Test-MysqlDataDirUnlocked')) {
    Invoke-Expression (Get-InfraFunctionText $name)
}

Write-Host '[1] 外部监听器绝不能被当成本项目 MySQL'
$script:OkMessages = [System.Collections.Generic.List[string]]::new()
$script:SideEffects = [System.Collections.Generic.List[string]]::new()
function Find-Tool([string]$Component, [string]$Exe) { return 'C:\stub\mysql\bin\mysqld.exe' }
function Write-Ok([string]$Message) { $script:OkMessages.Add($Message) }
function Write-Step([string]$Message) {}
function Write-Err([string]$Message) {}
function Write-Warn2([string]$Message) {}
function Fail([string]$Message) { throw "PANDORA_EXPECTED_FAIL:$Message" }
function Get-OwnedMysqlListenerProcess([int]$Port) { return $null }
function Get-PortHolder([int]$Port) { return ,@("PID=99999 process=foreign-listener") }
function New-MysqlIni([string]$BaseDir) { $script:SideEffects.Add('New-MysqlIni') }
function New-MysqlBootstrapSql { $script:SideEffects.Add('New-MysqlBootstrapSql') }
function Get-MysqlIniPath { return 'C:\stub\my.ini' }
function Get-MysqlBootstrapSqlPath { return 'C:\stub\mysql-bootstrap.sql' }
function Save-Pid([string]$Name, [int]$ProcessId) { $script:SideEffects.Add('Save-Pid') }
function Wait-Port { $script:SideEffects.Add('Wait-Port') }
function Invoke-MysqlSql { $script:SideEffects.Add('Invoke-MysqlSql') }

$DataDir = 'C:\stub\data'
$listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
$listener.Start()
try {
    $MysqlPort = $listener.LocalEndpoint.Port
    $caught = $null
    try { Start-LocalMysql } catch { $caught = $_.Exception.Message }

    Assert-True ($caught -like 'PANDORA_EXPECTED_FAIL:*') '外部监听器触发明确失败，而不是被静默复用'
    Assert-True ($caught -match '非本项目|不会复用|不属于本项目') '失败信息明确说明不会复用外部实例'
    Assert-True (-not ($script:OkMessages -contains "MySQL :$MysqlPort 已在运行")) '不再误报“本项目 MySQL 已在运行”'
    Assert-True ($script:SideEffects.Count -eq 0) '发现外部监听器后不生成配置、不启动进程、不执行 SQL'
    Assert-True (Test-PortOpen $MysqlPort) '外部监听器仍保持原样，未被停止'
} finally {
    $listener.Stop()
}

Write-Host '[1b] mysqld 启动参数必须精确属于本工作区 my.ini'
$script:ExpectedExe = 'C:\stub\mysql\bin\mysqld.exe'
$script:ExpectedIni = 'C:\stub\cfg\my.ini'
$script:ProcessCommandLine = ''
function Find-Tool([string]$Component, [string]$Exe) { return $script:ExpectedExe }
function Get-MysqlIniPath { return $script:ExpectedIni }
function Get-ProcessCommandLine([int]$ProcessId) { return $script:ProcessCommandLine }
$identityProc = [pscustomobject]@{ Id = 12345; Path = $script:ExpectedExe }

$script:ProcessCommandLine = "`"$($script:ExpectedExe)`" --defaults-file=`"$($script:ExpectedIni)`" --no-monitor"
Assert-True (Test-ComponentProcessOwned 'mysql' $identityProc) 'exact my.ini 参数被识别为本项目实例'
$script:ProcessCommandLine = "`"$($script:ExpectedExe)`" --defaults-file=`"$($script:ExpectedIni).evil`" --no-monitor"
Assert-True (-not (Test-ComponentProcessOwned 'mysql' $identityProc)) 'my.ini.evil 不能用前缀冒充本项目配置'
$script:ProcessCommandLine = "`"$($script:ExpectedExe)`" --defaults-file=C:\foreign\my.ini --no-monitor"
Assert-True (-not (Test-ComponentProcessOwned 'mysql' $identityProc)) '其它工作区 my.ini 不属于本项目'
$script:ProcessCommandLine = ''
Assert-True (-not (Test-ComponentProcessOwned 'mysql' $identityProc)) '命令行读不到时 fail closed'
$script:ProcessCommandLine = "`"$($script:ExpectedExe)`" --defaults-file=`"$($script:ExpectedIni)`" --no-monitor"
$foreignExeProc = [pscustomobject]@{ Id = 12345; Path = 'C:\foreign\mysqld.exe' }
Assert-True (-not (Test-ComponentProcessOwned 'mysql' $foreignExeProc)) '其它 mysqld.exe 即使用本项目参数也不属于本项目'

Write-Host '[2] 陈旧/复用 PID 绝不能触发 mysqladmin 或 taskkill'
$script:GracefulCalls = 0
$script:TaskkillCalls = 0
$script:OwnedFlag = $false
$script:KillSucceeds = $false
$script:StateStoppedCalls = 0
$fakeProc = [pscustomobject]@{ Id = 424242; HasExited = $false; Path = 'C:\stub\mysqld.exe' }
$script:CurrentRunning = $fakeProc
$script:LiveRegistered = $fakeProc
$script:ListenerRecord = $null
$script:FallbackProc = $null
$script:PortState = $null
$MysqlPort = 13308
$MysqlPortMin = 13307
$MysqlPortMax = 13309
$ProjectRoot = 'C:\stub'
function Get-RunningProcess([string]$Name) { return $script:CurrentRunning }
function Get-LivePidFileProcess([string]$Name) { return $script:LiveRegistered }
function Test-ComponentProcessOwned([string]$Name, $Proc) { return $script:OwnedFlag }
function Get-OnlyOwnedMysqlListener([int[]]$Ports) { return $script:ListenerRecord }
function Get-OnlyOwnedMysqlProcess { return $script:FallbackProc }
function Get-PandoraLocalInfraPortState([string]$Root) { return $script:PortState }
function Set-MysqlStateStopped { $script:StateStoppedCalls++ }
function Request-GracefulStop([string]$Name, $Proc) { $script:GracefulCalls++; return $false }
function Wait-ProcessExit($Proc, [int]$TimeoutSec) { return $false }
function taskkill.exe { $script:TaskkillCalls++; if ($script:KillSucceeds) { $fakeProc.HasExited = $true } }
$script:TestPidFile = Join-Path ([System.IO.Path]::GetTempPath()) ("pandora-mysql-stop-{0}.pid" -f [guid]::NewGuid().ToString('N'))
function Get-PidFile([string]$Name) { return $script:TestPidFile }

$stopped = Stop-Component 'mysql'
Assert-True (-not $stopped) '未知活 PID 会阻断 down/reset，不会被当成已停止'
Assert-True ($script:GracefulCalls -eq 0) '未经归属验证的 PID 不调用 mysqladmin shutdown'
Assert-True ($script:TaskkillCalls -eq 0) '未经归属验证的 PID 不调用 taskkill'

$script:OwnedFlag = $true
$script:ListenerRecord = [pscustomobject]@{ Port = 13308; Process = $fakeProc }
$script:KillSucceeds = $true
$fakeProc.HasExited = $false
$stopped = Stop-Component 'mysql'
Assert-True $stopped '精确归属的本项目 PID 可以正常停止'
Assert-True ($script:GracefulCalls -eq 1) '精确归属的本项目 PID 仍会走优雅停机'
Assert-True ($script:TaskkillCalls -eq 1) '优雅停机后仍不退出的本项目 PID 才允许 taskkill'

$script:CurrentRunning = $null
$script:LiveRegistered = $null
$script:ListenerRecord = [pscustomobject]@{ Port = 13309; Process = $fakeProc }
$fakeProc.HasExited = $false
$stopped = Stop-Component 'mysql'
Assert-True ($stopped -and $script:TaskkillCalls -eq 2) 'mysql.pid 丢失时仍可按精确 listener 归属回收本项目进程'

$script:CurrentRunning = $fakeProc
$script:LiveRegistered = $fakeProc
$script:ListenerRecord = [pscustomobject]@{ Port = 13308; Process = $fakeProc }
$script:KillSucceeds = $false
$fakeProc.HasExited = $false
Set-Content -LiteralPath $script:TestPidFile -Value $fakeProc.Id -Encoding ascii
$stopped = Stop-Component 'mysql'
Assert-True (-not $stopped) 'taskkill 后仍存活时 Stop-Component 返回失败'
Assert-True (Test-Path -LiteralPath $script:TestPidFile) '停机失败会保留 PID 登记供下一轮追踪'

$script:CurrentRunning = $null
$script:LiveRegistered = $null
$script:ListenerRecord = $null
$script:FallbackProc = $null
$script:PortState = [pscustomobject]@{ MysqlProcessId = 515151 }
function Get-Process { param([int]$Id); return [pscustomobject]@{ Id = $Id; HasExited = $false } }
$stopped = Stop-Component 'mysql'
Assert-True (-not $stopped) '状态 PID 仍活但归属无法证明时 fail closed'
Remove-Item function:Get-Process -Force

Write-Host '[3] mysqladmin 只发给同一已归属 listener，并使用动态端口'
Invoke-Expression (Get-InfraFunctionText 'Request-GracefulStop')
$script:BoundedCalls = 0
$script:CapturedArguments = @()
$script:GracefulListener = $fakeProc
$MysqlPort = 13308
$MysqlRootPwd = 'stub-root-password'
function Get-OwnedMysqlListenerProcess([int]$Port) { return $script:GracefulListener }
function Find-Tool([string]$Component, [string]$Exe) { return 'C:\stub\mysqladmin.exe' }
function Invoke-BoundedTool { param([string]$FilePath, [string[]]$Arguments, [int]$TimeoutSec); $script:BoundedCalls++; $script:CapturedArguments = $Arguments }
$requested = Request-GracefulStop 'mysql' $fakeProc
Assert-True ($requested -and $script:BoundedCalls -eq 1 -and $script:CapturedArguments -contains '--port=13308') 'mysqladmin 使用选中的动态端口'
$script:GracefulListener = $null
$requested = Request-GracefulStop 'mysql' $fakeProc
Assert-True (-not $requested -and $script:BoundedCalls -eq 1) '端口 listener PID 不吻合时不发送 mysqladmin shutdown'

Write-Host '[4] down 失败必须传播，reset 不得删 data'
$LocalInfraLifecyclePlan = [pscustomobject]@{
    StopComponents = @('mysql', 'redis')
    ResetComponents = @('mysql', 'redis')
}
$CentralMysqlManaged = $false
function Stop-Component([string]$Name) { return $Name -ne 'mysql' }
Assert-True (-not (Invoke-Down)) '任一组件停机失败会让 Invoke-Down 失败'
$DataDir = Join-Path ([IO.Path]::GetTempPath()) ("pandora-reset-guard-{0}" -f [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Force -Path $DataDir | Out-Null
function Invoke-Down { return $false }
Assert-True (-not (Invoke-Reset)) 'Invoke-Reset 会传播 down 失败'
Assert-True (Test-Path -LiteralPath $DataDir) 'down 失败时 reset 不删除数据目录'
Remove-Item -LiteralPath $DataDir -Recurse -Force -ErrorAction SilentlyContinue

$DataDir = Join-Path ([IO.Path]::GetTempPath()) ("pandora-reset-file-lock-{0}" -f [guid]::NewGuid().ToString('N'))
$probeDir = Join-Path $DataDir 'mysql'
New-Item -ItemType Directory -Force -Path $probeDir | Out-Null
$probeFile = Join-Path $probeDir 'ibdata1'
Set-Content -LiteralPath $probeFile -Value 'stub' -Encoding ascii
$held = [IO.File]::Open($probeFile, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read)
try { Assert-True (-not (Test-MysqlDataDirUnlocked)) '核心数据文件仍被占用时 reset 的独占探针 fail closed' }
finally { $held.Dispose() }
Assert-True (Test-MysqlDataDirUnlocked) '核心数据文件释放后独占探针通过'
function Invoke-Down { return $true }
    function Get-OwnedMysqlProcesses { return @() }
Assert-True (Invoke-Reset) '全部停止且文件解锁后 reset 成功'
Assert-True (-not (Test-Path -LiteralPath $DataDir)) 'reset 成功必须确认数据目录已消失'
Assert-True (Invoke-Reset) '数据目录本来不存在时 reset 仍按目标终态成功'
Remove-Item -LiteralPath $DataDir -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $script:TestPidFile -Force -ErrorAction SilentlyContinue

$infraText = [IO.File]::ReadAllText($infra)
Assert-True ($infraText -match "Action -in @\('up', 'down', 'reset', 'provision'\)" -and $infraText -match 'lifecycle\.lock') 'up/down/reset/provision 共用同一生命周期互斥锁'

if ($script:Failed) {
    Write-Host ''
    Write-Host '[FAIL] 免 Docker MySQL 归属隔离契约' -ForegroundColor Red
    exit 1
}
Write-Host ''
Write-Host '[PASS] 免 Docker MySQL 归属隔离契约' -ForegroundColor Green
