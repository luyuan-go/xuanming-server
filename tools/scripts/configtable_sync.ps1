<#
.SYNOPSIS
  表头漂移同步:策划改了列名 / 加了列之后,自动把服务端 proto 的 (excel_col) 注解跟上。

.DESCRIPTION
  面向**程序**(不是策划)。导表报「表头第 X 列: 期望 "A" 实为 "B"」或「表头出现未登记的第 X 列」时跑它。

  它只做机械的那一半:
    列改名   → 就地替换 (excel_col) 字面量(字段名 / 编号 / 注释一律不动);
    末尾加列 → 追加字段,取下一个未用编号(CLAUDE.md §5.4 编号不复用、不回填空洞);
    删列 / 挪位 / 重名 → 只报告,不自动改(涉及 reserved 语义与错列风险,必须人判断)。

  它**不会**替你决定 (excel_required) / (excel_prefix) / (excel_fk) / enum ——
  那些是服务端业务决策,xlsx 里没有对应事实可以反推(为什么不是「从 xlsx 全量生成 proto」,
  见 docs/design/decision-revisit-configtable-proto-generation.md)。新增字段的类型和字段名
  优先取客户端列登记(Pandora-Client-SVN/Tool/Table/Cs/Proto/*.json),保证两仓同名同类型;
  客户端没登记的列按数据推断类型 + 占位字段名 col_<列号>,并在注释里写明来源,等你改。

.PARAMETER Write
  真的改写 .proto。不加则只报告差异(默认)。
  改写后会自动接着跑:proto_gen(重生 pb) → 重建 configtable-gen.exe → configtable_gen(验证导表)。

.PARAMETER SyncCol
  指定新增列的字段名 / 类型,可重复:`-SyncCol 'skill_circle.范围外圆形集合=out_range_circles:string'`。

.PARAMETER TableRoot
  客户端策划表根目录。留空按 configtable_gen.ps1 同款顺序自动探测。

.PARAMETER ClientRegistry
  客户端列登记目录。留空取 <TableRoot>\..\Tool\Table\Cs\Proto。

.EXAMPLE
  pwsh tools\scripts\configtable_sync.ps1
  pwsh tools\scripts\configtable_sync.ps1 -Write
  pwsh tools\scripts\configtable_sync.ps1 -Write -SyncCol 'skill_circle.范围外圆形集合=out_range_circles:string'
#>
param(
    [string]$TableRoot = '',
    [string]$ClientRegistry = '',
    [string[]]$SyncCol = @(),
    [switch]$Write
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

try { [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false) } catch { }

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = [System.IO.Path]::GetFullPath((Join-Path $ScriptDir '..\..'))

function Write-Ok([string]$m) { Write-Host "[OK]  $m" -ForegroundColor Green }
function Write-Info([string]$m) { Write-Host "[..]  $m" -ForegroundColor Cyan }
function Write-Warn2([string]$m) { Write-Host "[!!]  $m" -ForegroundColor Yellow }
function Write-Err([string]$m) { Write-Host "[ERR] $m" -ForegroundColor Red }

# 定位策划表根目录:与 configtable_gen.ps1 共用同一份公共件,不再各抄一遍
# (两份"同款顺序"的拷贝迟早漂移,而漂移后两个脚本会指向不同的表 —— 表头同步比对的是
# 这一份表,导表导的是另一份,得出的结论直接是错的)。
. (Join-Path $ScriptDir 'client_repo_lib.ps1')

$resolved = Resolve-PandoraClientTableRoot -Explicit $TableRoot -ProjectRoot $ProjectRoot
if (-not $resolved.Path) {
    Write-Err '找不到客户端策划表根目录(Table)。'
    Write-PandoraClientTableRootHelp -ProjectRoot $ProjectRoot -NearMiss $resolved.NearMiss
    exit 1
}
$TableRoot = $resolved.Path
Write-Ok "策划表目录: $TableRoot"

# 本脚本是程序用的,直接要求本机有 Go:同步要改 .proto,改完必须重生 pb + 重建 exe,
# 这几步本来就离不开 Go,没有 Go 时给个半截结果反而会让人以为已经同步好了。
if ($null -eq (Get-Command go -ErrorAction SilentlyContinue)) {
    Write-Err '本机没装 Go。表头同步会改 .proto,改完必须重生 pb 并重建生成器,这些都需要 Go。'
    exit 1
}

$GenExe = Join-Path $ProjectRoot 'run\artifacts\windows\bin\configtable-gen.exe'

# 先重建生成器:它内嵌的是**编译时**的 proto 描述符,拿陈旧 exe 比对表头会得出过时结论。
# 直接跑 exe 而不是 go run,顺带避开 go run 把子进程退出码包装成 "exit status N" 的噪音。
Write-Info '重建 configtable-gen.exe…'
Push-Location $ProjectRoot
try {
    & go build -o run\artifacts\windows\bin\configtable-gen.exe .\tools\configtable-gen
    $buildExit = $LASTEXITCODE
} finally { Pop-Location }
if ($buildExit -ne 0) {
    Write-Err '重建 configtable-gen.exe 失败。'
    exit 1
}

$genArgs = New-Object System.Collections.Generic.List[string]
$genArgs.Add('-tables'); $genArgs.Add($TableRoot)
if ($Write) { $genArgs.Add('-sync-write') } else { $genArgs.Add('-sync') }
if ($ClientRegistry) { $genArgs.Add('-client-registry'); $genArgs.Add($ClientRegistry) }
foreach ($c in $SyncCol) {
    if ([string]::IsNullOrWhiteSpace($c)) { continue }
    $genArgs.Add('-sync-col'); $genArgs.Add($c)
}

Write-Host ''
Push-Location $ProjectRoot
try {
    & $GenExe @genArgs
    $exit = $LASTEXITCODE
} finally { Pop-Location }

# 退出码见 tools/configtable-gen/sync.go:0 = 无漂移;1 = 待人处理;3 = 已改写待重生。
switch ($exit) {
    0 { Write-Host ''; Write-Ok '无需同步。'; exit 0 }
    3 { }
    default {
        Write-Host ''
        if (-not $Write) {
            Write-Info '以上为差异报告(未改任何文件)。确认无误后加 -Write 自动改写。'
        } else {
            Write-Warn2 '有差异需要人工处理(见上方 [BLOCK] / [ERR]),proto 未被改写或只改了一部分。'
        }
        exit $exit
    }
}

# 走到这里:proto 已被改写,把后续三步一起跑完,免得留下"改了 proto 但 pb / exe 没跟上"的半截状态。
Write-Host ''
Write-Info '重生 proto pb…'
& pwsh -NoProfile -File (Join-Path $ScriptDir 'proto_gen.ps1')
if ($LASTEXITCODE -ne 0) {
    Write-Err 'proto 生成失败。proto 已改写,请修好后手动重跑 proto_gen.ps1。'
    exit 1
}

Write-Host ''
Write-Info '重建 configtable-gen.exe…'
Push-Location $ProjectRoot
try {
    & go build -o run\artifacts\windows\bin\configtable-gen.exe .\tools\configtable-gen
    $buildExit = $LASTEXITCODE
} finally { Pop-Location }
if ($buildExit -ne 0) {
    Write-Err '重建 configtable-gen.exe 失败。'
    exit 1
}

Write-Host ''
Write-Info '重跑导表验证…'
& pwsh -NoProfile -File (Join-Path $ScriptDir 'configtable_gen.ps1') -TableRoot $TableRoot
if ($LASTEXITCODE -ne 0) {
    Write-Err '同步后导表仍失败,见上方输出。'
    exit 1
}

Write-Host ''
Write-Ok '表头同步完成。'
Write-Host '      请 review 改动过的 .proto:新增字段默认没有 (excel_required) / (excel_prefix) / (excel_fk),'
Write-Host '      字段名若是 col_<列号> 这种占位名,请改成有业务语义的名字(改完重跑本脚本验证)。'
Write-Host '      proto 改动提交时按 CLAUDE.md §4 在 commit message 标注 [proto]。'
exit 0
