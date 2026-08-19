# 免 Docker 本机基础设施「起不来时」的诊断输出契约(2026-08-19)。
#
# 守的是什么:组件起不来时,窗口里必须直接出现**日志内容**和已知原因的人话解释,
# 而不是只丢一个日志路径。这些一键入口跑在策划机 / 别人的机器上,写脚本的人当场看不到
# 那台机器 —— 只给路径 = 让不熟悉的人去翻一个几百行、混着历次启动记录的日志,
# 实际结果是"把现场贴回来"这一步就断了(2026-08-19 现场:`mysql 启动后立即退出 (exit 1)`
# 之后再没有任何可用信息)。
#
# 为什么必须机械化守:这段代码**只在出故障时才执行**,正常跑一百次也碰不到一次。
# 它自己有 bug 的表现是「报错处理里再崩一次」,把一个可查的故障变成不可查的。
#
# 用法:pwsh tools/scripts/tests/localinfra_failure_diagnostics_test.ps1

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$infra = Join-Path (Split-Path -Parent $PSScriptRoot) 'local_infra.ps1'
$source = [System.Text.UTF8Encoding]::new($false).GetString([System.IO.File]::ReadAllBytes($infra))

$script:Failed = New-Object 'System.Collections.Generic.List[string]'
function Assert-True([bool]$Cond, [string]$What) {
    if ($Cond) { Write-Host "  [ok] $What" } else { $script:Failed.Add($What); Write-Host "  [NG] $What" -ForegroundColor Red }
}

# ---------------------------------------------------------------------------
# 把待测函数从 local_infra.ps1 里抠出来跑。不能 dot-source 整个脚本 —— 它末尾就开始
# 真的备料 / 起进程了。抠的是 AST 的原文,所以测的确实是仓库里那份代码。
# ---------------------------------------------------------------------------
$errs = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($infra, [ref]$null, [ref]$errs)
if ($errs -and $errs.Count -gt 0) { throw "local_infra.ps1 语法错误:$($errs[0].Message)" }

foreach ($name in @('Read-LogTail', 'Get-PortHolder', 'Test-MysqldConfig', 'Show-ComponentFailure')) {
    $fn = @($ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name }, $true))
    if ($fn.Count -ne 1) { throw "local_infra.ps1 里找不到唯一的 $name(找到 $($fn.Count) 个)" }
    Invoke-Expression $fn[0].Extent.Text
}
# 提示表是脚本级赋值,不是函数,单独抠。
$hintAssign = @($ast.FindAll({
            param($n)
            $n -is [System.Management.Automation.Language.AssignmentStatementAst] -and
            $n.Left.Extent.Text -eq '$script:InfraLogHints'
        }, $true))
if ($hintAssign.Count -ne 1) { throw "找不到唯一的 `$script:InfraLogHints 赋值(找到 $($hintAssign.Count) 个)" }
Invoke-Expression $hintAssign[0].Extent.Text

# 被测函数依赖的外部件,用最小桩替掉(它们各自另有测试 / 不属本契约)。
function Write-Ok([string]$m) { Write-Host "  [ OK ] $m" }
function Write-Warn2([string]$m) { Write-Host "  [WARN] $m" }
function Write-Err([string]$m) { Write-Host "  [ERR ] $m" }
function Find-Tool([string]$Component, [string]$Exe) { return $null }   # 测试机不跑真 mysqld
function Get-MysqlIniPath { return (Join-Path $script:Sandbox 'my.ini') }

$script:Sandbox = Join-Path ([System.IO.Path]::GetTempPath()) ("pandora-infradiag-" + [guid]::NewGuid().ToString('N'))
$LogDir = $script:Sandbox
New-Item -ItemType Directory -Path $script:Sandbox -Force | Out-Null

function Invoke-Diag([string]$Name, [int]$Port) {
    # Show-ComponentFailure 全用 Write-Host 打给人看;PowerShell 7 里那是 Information 流,6>&1 收得到。
    return ((Show-ComponentFailure -Name $Name -Port $Port -Proc $null) 6>&1 | Out-String)
}

try {
    # ===== 1. Read-LogTail 必须能读**正在被写者占着**的日志 =====
    Write-Host '[1] 日志被写者占着时仍要读得到'
    $busy = Join-Path $script:Sandbox 'busy.log'
    Set-Content -LiteralPath $busy -Value "line-1`nline-2`nline-3" -Encoding utf8NoBOM
    # 模拟组件进程:以写方式打开并只允许别人读(mysqld / redis 就是这么开错误日志的)。
    $writer = [System.IO.File]::Open($busy, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Write, [System.IO.FileShare]::Read)
    try {
        $tail = Read-LogTail -Path $busy -Lines 10
        Assert-True ($null -ne $tail -and $tail.Count -eq 3) 'Read-LogTail 读到了被占用的日志(共享读打开)'
        # 反证:按 Get-Content 的默认共享模式(只允许别人读)打开会失败 —— 这正是不能用它的原因。
        $strictFailed = $false
        try {
            $h = [System.IO.File]::Open($busy, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::Read)
            $h.Dispose()
        } catch { $strictFailed = $true }
        Assert-True $strictFailed '反证:不允许写共享的打开方式在同一场景下会失败(所以不能用 Get-Content)'
    } finally { $writer.Dispose() }

    # ===== 2. 空日志 / 无日志必须各说各的 =====
    Write-Host '[2] 空日志与无日志是两个不同的结论'
    $emptyLog = Join-Path $script:Sandbox 'emptycomp.log'
    Set-Content -LiteralPath $emptyLog -Value '' -NoNewline
    Assert-True ((Read-LogTail -Path $emptyLog).Count -eq 0) '空文件返回空数组'
    Assert-True ($null -eq (Read-LogTail -Path (Join-Path $script:Sandbox 'nope.log'))) '文件不存在返回 $null'

    $outEmpty = Invoke-Diag -Name 'emptycomp' -Port 0
    Assert-True ($outEmpty -match '是空的') '日志为空时明确说"进程在能写日志之前就死了"'
    $outMissing = Invoke-Diag -Name 'nope' -Port 0
    Assert-True ($outMissing -match '读不到日志') '日志不存在时明确说读不到,不假装贴了内容'

    # ===== 3. 有日志时必须把内容贴出来 =====
    Write-Host '[3] 日志内容必须直接进窗口'
    $realLog = Join-Path $script:Sandbox 'mysql.log'
    # 取自本机 run/localinfra/logs/mysql.log 的真实失败现场(2026-08-15,exit 1)。
    @(
        '2026-08-15T14:37:39.738192Z 0 [Warning] [MY-010068] [Server] CA certificate ca.pem is self signed.'
        '2026-08-15T14:37:39.758038Z 0 [ERROR] [MY-010131] [Server] TCP/IP, --shared-memory, or --named-pipe should be configured on NT OS'
        '2026-08-15T14:37:39.758357Z 0 [ERROR] [MY-010119] [Server] Aborting'
    ) -join "`n" | Set-Content -LiteralPath $realLog -Encoding utf8NoBOM
    $outReal = Invoke-Diag -Name 'mysql' -Port 0
    Assert-True ($outReal -match 'MY-010131') '日志原文出现在输出里(不是只给路径)'
    Assert-True ($outReal -match '所有网络通道都被关了') '命中已知原因并给出人话解释'
    Assert-True ($outReal -match [regex]::Escape($realLog)) '仍然附上完整日志路径(要贴全文时用得上)'

    # ===== 4. 端口冲突:必须报出占用者 =====
    Write-Host '[4] 端口冲突要指名道姓'
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    $listener.Start()
    try {
        $busyPort = $listener.LocalEndpoint.Port
        $holders = Get-PortHolder $busyPort
        Assert-True ($holders.Count -ge 1) "Get-PortHolder 认出了 :$busyPort 的占用进程"
        $outPort = Invoke-Diag -Name 'mysql' -Port $busyPort
        Assert-True ($outPort -match '端口 :\d+ 的占用者') '输出里直接写明端口占用者(不用人去 netstat)'
    } finally { $listener.Stop() }

    # ===== 5. 每条已知原因都必须真能被自己的样例日志命中 =====
    Write-Host '[5] 提示表:样例 → 命中'
    $samples = @(
        @{ Line = "2026-01-01T00:00:00Z 0 [ERROR] [MY-010262] [Server] Can't start server: Bind on TCP/IP port: Address already in use"; Expect = '端口已被别的进程占着' }
        @{ Line = "2026-01-01T00:00:00Z 0 [ERROR] [MY-010187] [Server] Could not open file; Can't create/write to file (OS errno 13 - Access is denied)"; Expect = '数据 / 日志目录写不进去' }
        @{ Line = '2026-01-01T00:00:00Z 0 [ERROR] [MY-000067] [Server] unknown variable ' + "'foo=bar'"; Expect = '不认的配置项' }
        @{ Line = '2026-01-01T00:00:00Z 0 [ERROR] [InnoDB] Cannot allocate memory for the buffer pool'; Expect = '内存不够' }
        @{ Line = '2026-01-01T00:00:00Z 0 [ERROR] [Server] Execution of init_file failed'; Expect = '引导 SQL' }
        @{ Line = '2026-01-01T00:00:00Z 0 [ERROR] [InnoDB] Data Dictionary initialization failed'; Expect = '数据目录坏了' }
        @{ Line = '2026-01-01T00:00:00Z 0 [ERROR] [InnoDB] Unable to lock ./ibdata1 error: 11'; Expect = '还活着占着数据目录' }
    )
    foreach ($s in $samples) {
        $p = Join-Path $script:Sandbox 'sample.log'
        $s.Line | Set-Content -LiteralPath $p -Encoding utf8NoBOM
        $out = Invoke-Diag -Name 'sample' -Port 0
        Assert-True ($out -match [regex]::Escape($s.Expect)) ("命中:{0}" -f $s.Expect)
    }

    # ===== 6. 源码级:失败路径不许退回"只给路径" =====
    Write-Host '[6] Wait-Port 两条失败路径都必须走诊断'
    $waitFn = @($ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'Wait-Port' }, $true))
    Assert-True ($waitFn.Count -eq 1) '存在唯一的 Wait-Port'
    $waitText = $waitFn[0].Extent.Text
    Assert-True (([regex]::Matches($waitText, 'Show-ComponentFailure')).Count -ge 2) '立即退出与超时两条路径都调了 Show-ComponentFailure'
    Assert-True ($waitText -notmatch 'Fail\s+"') 'Wait-Port 不再用只带路径的 Fail 收场'
}
finally {
    if (Test-Path -LiteralPath $script:Sandbox) { Remove-Item -LiteralPath $script:Sandbox -Recurse -Force -ErrorAction SilentlyContinue }
}

if ($script:Failed.Count -gt 0) {
    Write-Host ''
    Write-Host '[FAIL] 本机基础设施失败诊断契约未通过:' -ForegroundColor Red
    $script:Failed | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}
Write-Host ''
Write-Host '[PASS] 本机基础设施失败诊断契约' -ForegroundColor Green
