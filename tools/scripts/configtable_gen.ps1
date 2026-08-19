<#
.SYNOPSIS
  策划一键导表:把客户端 SVN 的策划 xlsx 生成为服务端配置表(configtable/dist + manifest.json)。

.DESCRIPTION
  包装 tools/configtable-gen,把三件策划做不了的事替掉:
    1. 自动定位客户端 Table 根目录(不用记路径);
    2. 自动从 SVN 取版本号填 -source-rev(生成器必填且拒 unknown/空白);
    3. 没装 Go 也能跑(优先用 run/artifacts/windows/bin/configtable-gen.exe)。
  并把生成器面向程序的报错翻译成"该找谁、该改什么"。

  产物是**整批**的:configtable/dist 下全部 json + manifest.json 必须一起提交,
  只提交其中一个会让服务端 Store.Load 校验失败、服务起不来。

  什么时候需要单独跑(2026-08-05 起多数情况下用不着):
    两个策划一键入口(启动 / 重启DS)已经会自己先导表,日常改完表直接双击那两个就行。
    本脚本(以及包装它的 策划一键导表.cmd)留给「只想导表、暂时不起服务」的场景:
    比如验证自己的表能不能导过,或者要把 configtable/dist 这批产物交给别人。

  导完之后:
    后端已经在跑 → 双击 策划一键重启DS-读最新资源.cmd(重启读表的服务让新表生效);
    后端没在跑   → 双击 策划一键启动-改资源即时生效.cmd。
    两者都会再导一次表(内容没变就是空跑),所以先跑本脚本也不冲突。

.PARAMETER TableRoot
  客户端策划表根目录。留空自动探测,完整顺序见 tools/scripts/client_repo_lib.ps1:
    -TableRoot 参数 → $env:PANDORA_CLIENT_TABLE_ROOT
    → $env:PANDORA_CLIENT_REPO / $env:PANDORA_DS_UPROJECT 推出来的客户端仓 \Table
    → 后端仓平级 / 上一级目录、各固定盘下的客户端仓 \Table
    → F:\work\Pandora-Client-SVN\Table
  **本地目录名叫什么都行**:客户端仓在 SVN 上的名字是 Client(^/trunk/Client),
  按原名检出的 Client、文档里写的 Pandora-Client-SVN、策划自己取的名字,都能找到。

.PARAMETER SourceRev
  产物溯源标注。留空则从 SVN 读 Table 目录的最后改动版本,填成 svn-r<N>。

.PARAMETER Check
  只探测环境并报告将要用的参数,不真的生成。

.EXAMPLE
  pwsh tools/scripts/configtable_gen.ps1
  pwsh tools/scripts/configtable_gen.ps1 -Check
  pwsh tools/scripts/configtable_gen.ps1 -TableRoot D:\work\Pandora-Client-SVN\Table
#>
param(
    [string]$TableRoot = '',
    [string]$SourceRev = '',
    [switch]$Check
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# 控制台默认按本地代码页解码子进程输出,中文表名/报错会变乱码;固定 UTF-8(与 start.ps1 同款处理)。
try { [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false) } catch { }

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = [System.IO.Path]::GetFullPath((Join-Path $ScriptDir '..\..'))
$DistDir = Join-Path $ProjectRoot 'configtable\dist'

function Write-Ok([string]$m) { Write-Host "[OK]  $m" -ForegroundColor Green }
function Write-Info([string]$m) { Write-Host "[..]  $m" -ForegroundColor Cyan }
function Write-Warn2([string]$m) { Write-Host "[!!]  $m" -ForegroundColor Yellow }
function Write-Err([string]$m) { Write-Host "[ERR] $m" -ForegroundColor Red }

# ---------------------------------------------------------------------------
# 1. 定位客户端策划表根目录
# ---------------------------------------------------------------------------
# 定位逻辑放公共件:configtable_sync.ps1 和 start.ps1(定位 DS 的 .uproject)找的是**同一个
# 客户端仓**,三处各抄一份必然漂移,而漂移的表现是"导表用的是 A 仓的表、DS 读的是 B 仓的资源"。
. (Join-Path $ScriptDir 'client_repo_lib.ps1')

$resolved = Resolve-PandoraClientTableRoot -Explicit $TableRoot -ProjectRoot $ProjectRoot
if (-not $resolved.Path) {
    Write-Err '找不到客户端策划表根目录(Table)。'
    Write-PandoraClientTableRootHelp -ProjectRoot $ProjectRoot -NearMiss $resolved.NearMiss
    exit 1
}
$TableRoot = $resolved.Path
Write-Ok "策划表目录: $TableRoot"
Write-Info "定位方式: $($resolved.Source)"
foreach ($nm in $resolved.NearMiss) { Write-Warn2 "跳过(目录在但底下没有 xlsx):$nm" }
if ($resolved.Others.Count -gt 0) {
    # 一台机器上存在多份客户端检出时,挑错那份的表现是"我明明改了表却没生效",最难自查。
    # 所以宁可啰嗦:本次用了哪个、还有哪些没用、想换怎么换,一次说清。
    Write-Warn2 '本机还有别的策划表目录,这次没有用:'
    foreach ($o in $resolved.Others) { Write-Host "        $o" }
    Write-Host '      想用其中某一个:加 -TableRoot <路径>,或设环境变量 PANDORA_CLIENT_TABLE_ROOT。'
}

# ---------------------------------------------------------------------------
# 2. 取 SVN 版本号当 -source-rev(生成器必填,且拒 unknown / 空白)
# ---------------------------------------------------------------------------
function Resolve-SourceRev([string]$Explicit, [string]$Root) {
    if (-not [string]::IsNullOrWhiteSpace($Explicit)) { return $Explicit.Trim() }
    $svn = Get-Command svn -ErrorAction SilentlyContinue
    if ($null -eq $svn) { return '' }
    # 不能取 Table 目录节点自身的 last-changed-revision:子文件提交后目录节点
    # 可能仍是旧版本,会把新产物错标成旧 source_rev。递归取工作副本各节点
    # revision 的最大值,兼容 SVN 混合版本工作副本。Table 根是纯 ASCII 路径,
    # 避免直接把中文子路径传给某些 svn.exe(会 E155010)。
    $raw = (& svn info --show-item revision --depth infinity $Root 2>$null | Out-String)
    $revisions = @($raw -split "`r?`n" | ForEach-Object {
        if ($_ -match '^\s*(\d+)(?:\s|$)') { [uint64]$Matches[1] }
    })
    if ($revisions.Count -eq 0) { return '' }
    $rev = ($revisions | Measure-Object -Maximum).Maximum
    return "svn-r$rev"
}

$SourceRev = Resolve-SourceRev $SourceRev $TableRoot
if (-not $SourceRev) {
    Write-Err '取不到 SVN 版本号,无法填写 -source-rev(生成器拒绝不可追溯的批次)。'
    Write-Host '      要么装好 svn 命令行并保证 Table 是 SVN 工作副本,'
    Write-Host '      要么手动指定,例如:-SourceRev svn-r1774'
    exit 1
}
Write-Ok "源表版本: $SourceRev"

# ---------------------------------------------------------------------------
# 3. 选生成器:优先预编译 exe(策划机通常没装 Go),否则回退 go run
# ---------------------------------------------------------------------------
$GenExe = Join-Path $ProjectRoot 'run\artifacts\windows\bin\configtable-gen.exe'
$UseExe = Test-Path -LiteralPath $GenExe -PathType Leaf
$GoCmd = Get-Command go -ErrorAction SilentlyContinue
if (-not $UseExe -and $null -eq $GoCmd) {
    Write-Err '既没有预编译的 configtable-gen.exe,本机也没装 Go,无法导表。'
    Write-Host "      预期位置: $GenExe"
    Write-Host '      找程序出一份:'
    Write-Host '        go build -o run\artifacts\windows\bin\configtable-gen.exe .\tools\configtable-gen'
    exit 1
}
# exe 里内嵌的是**编译时**的 proto 描述符。程序改完 .proto 只跑 proto_gen 而忘了重建 exe 时,
# 导表会报一字不差的旧错,很难想到是 exe 陈旧——在这里机械地挡掉。
function Get-NewestWrite([string[]]$Paths) {
    $newest = [datetime]::MinValue
    foreach ($p in $Paths) {
        if (-not (Test-Path -LiteralPath $p)) { continue }
        Get-ChildItem -LiteralPath $p -Recurse -File -ErrorAction SilentlyContinue | ForEach-Object {
            if ($_.LastWriteTimeUtc -gt $newest) { $newest = $_.LastWriteTimeUtc }
        }
    }
    return $newest
}
if ($UseExe) {
    $srcNewest = Get-NewestWrite @(
        (Join-Path $ProjectRoot 'tools\configtable-gen'),
        (Join-Path $ProjectRoot 'proto\gen\go\pandora\config\v1')
    )
    $exeTime = (Get-Item -LiteralPath $GenExe).LastWriteTimeUtc
    if ($srcNewest -gt $exeTime) {
        if ($Check) {
            Write-Warn2 'configtable-gen.exe 比生成器源码 / pb 旧(-Check 模式不重建,真正导表时会自动重建)。'
        } elseif ($null -ne $GoCmd) {
            Write-Info '检测到 configtable-gen.exe 比生成器源码 / pb 旧,先重建…'
            Push-Location $ProjectRoot
            try {
                & go build -o run\artifacts\windows\bin\configtable-gen.exe .\tools\configtable-gen
                $buildExit = $LASTEXITCODE
            } finally { Pop-Location }
            if ($buildExit -ne 0) {
                Write-Err '重建 configtable-gen.exe 失败,不继续导表(陈旧 exe 会报已修好的旧错)。'
                exit 1
            }
            Write-Ok '已重建 configtable-gen.exe'
        } else {
            Write-Warn2 'configtable-gen.exe 比生成器源码 / pb 旧,且本机没装 Go 无法重建。'
            Write-Host '      如果程序刚改过 proto 却仍报同一个错,就是这个原因,找程序重新出一份 exe。'
        }
    }
}

Write-Ok $(if ($UseExe) { "生成器: 预编译 exe($GenExe)" } else { "生成器: go run ./tools/configtable-gen(本机有 Go)" })

if ($Check) {
    Write-Info '-Check 模式:环境探测通过,未执行生成。'
    exit 0
}

# ---------------------------------------------------------------------------
# 4. 记录旧批次行数,生成后做增减对比
# ---------------------------------------------------------------------------
function Read-ManifestRows([string]$Dir) {
    $p = Join-Path $Dir 'manifest.json'
    if (-not (Test-Path -LiteralPath $p -PathType Leaf)) { return $null }
    try {
        $m = [System.Text.UTF8Encoding]::new($false).GetString([System.IO.File]::ReadAllBytes($p)) | ConvertFrom-Json
    } catch { return $null }
    $map = @{}
    foreach ($t in $m.tables) { $map[[string]$t.name] = [int]$t.rows }
    return [pscustomobject]@{ Version = [uint64]$m.version; Rows = $map }
}
$before = Read-ManifestRows $DistDir

# ---------------------------------------------------------------------------
# 5. 跑生成器
# ---------------------------------------------------------------------------
Write-Host ''
Write-Info '开始导表(全批原子:任一张表校验不过就整批不产出,旧表保持不变)...'
Write-Host ''

Push-Location $ProjectRoot
try {
    $genArgs = @('-tables', $TableRoot, '-source-rev', $SourceRev)
    if ($UseExe) {
        $out = & $GenExe @genArgs 2>&1 | ForEach-Object { $_.ToString() }
    } else {
        $out = & go run ./tools/configtable-gen @genArgs 2>&1 | ForEach-Object { $_.ToString() }
    }
    $genExit = $LASTEXITCODE
} finally {
    Pop-Location
}

$out | ForEach-Object { Write-Host $_ }
$outText = ($out -join "`n")

# ---------------------------------------------------------------------------
# 6. 失败:把面向程序的报错翻译成"找谁、改什么"
# ---------------------------------------------------------------------------

# 本脚本只导「磁盘上当前是什么」,不碰 SVN(不 update / 不 revert):启动流程中途改动工作副本
# 会把别人的表和资源无声拉进来,故障更难查。代价是:同一句报错在两台机器上可能指向不同的表。
# 所以报「表结构对不上」这类必须找程序的错时,顺带说清楚这一列到底在不在 SVN 里 ——
# 差别是决定性的:未提交 = 程序 svn update 也复现不了,必须先提交或连文件一起发过去。
function Get-TableWorkingCopyChanges([string]$Root) {
    if ($null -eq (Get-Command svn -ErrorAction SilentlyContinue)) { return $null }
    try {
        # --xml 的输出显式是 UTF-8,中文表名不会被控制台代码页糟蹋(纯文本 svn status 会)。
        # 仍然只把 ASCII 的 Table 根目录传给 svn.exe,不往下传中文子目录(会 E155010)。
        $xmlText = (& svn status --xml $Root 2>$null | Out-String)
        if ([string]::IsNullOrWhiteSpace($xmlText)) { return $null }
        $doc = [xml]$xmlText
    } catch { return $null }
    $changed = New-Object System.Collections.Generic.List[pscustomobject]
    foreach ($entry in $doc.SelectNodes('//entry')) {
        $st = $entry.SelectSingleNode('wc-status')
        if ($null -eq $st) { continue }
        $item = $st.GetAttribute('item')
        if ($item -eq 'normal' -or $item -eq 'external' -or $item -eq 'ignored') { continue }
        $changed.Add([pscustomobject]@{ Path = $entry.GetAttribute('path'); Item = $item })
    }
    # 逗号包一层:PowerShell 会在 return 时展开集合,空 List 直接变成 $null,
    # 那样「工作副本干净」就会和「本机没有 svn」撞成同一个返回值,提示语会指错方向。
    return , $changed
}

# 表头漂移这类错,configtable_sync.ps1 能全自动处理 —— 但它要改 .proto 并重生 pb,
# 依赖 go + buf。策划机器上两者都没有(configtable_gen.ps1 本身特意做成无 Go 也能跑),
# 在那里推荐一条注定报「未安装」的命令,只会把一条看得懂的错换成看不懂的错。
# 所以按本机工具链分流:能跑的人给命令,跑不了的人给"发给谁"。
function Get-SyncToolchainMissing {
    $missing = New-Object System.Collections.Generic.List[string]
    foreach ($c in @('go', 'buf')) {
        if ($null -eq (Get-Command $c -ErrorAction SilentlyContinue)) { $missing.Add($c) }
    }
    return , $missing
}

function Write-SyncHint([string]$What) {
    $missing = Get-SyncToolchainMissing
    if ($missing.Count -eq 0) {
        Write-Host "      你这台机器有 go + buf，可以自己同步（$What 也能自动处理）："
        Write-Host '        双击仓库根目录的 「程序一键同步表头.cmd」  # 先报差异，确认后按 Y 自动改 proto、重生 pb 并重跑导表'
        Write-Host '      等价命令行：'
        Write-Host '        pwsh tools\scripts\configtable_sync.ps1          # 先看差异'
        Write-Host '        pwsh tools\scripts\configtable_sync.ps1 -Write   # 确认后自动改 proto、重生 pb 并重跑导表'
        Write-Host '      （删列 / 挪位 / 重名它不自动改，那几种要人判断。）'
    } else {
        Write-Host "      程序那边双击一个脚本就能自动同步（$What），但**你这台机器不行**：缺 $($missing -join ' / ')。"
        Write-Host '      做法：把上面那行报错原文发给程序，他双击仓库根目录的「程序一键同步表头.cmd」即可。'
    }
}

if ($genExit -ne 0) {
    Write-Host ''
    Write-Err "导表失败(退出码 $genExit),服务端配置表**没有任何改动**,原来的表还能用。"
    Write-Host ''
    if ($outText -match '(\S+\.xlsx)\s*表头出现未登记的第\s*(\S+)\s*列\s*"([^"]*)"') {
        $table = $Matches[1]; $col = $Matches[2]; $name = $Matches[3]
        $leaf = ($table -split '[\\/]')[-1]
        Write-Warn2 "原因:$table 里多了第 $col 列「$name」,服务端 proto 还没登记这一列。"
        Write-Host '      这**不是策划能修的**:服务端 proto 要先登记这一列才能导。'
        Write-SyncHint '末尾加列'

        # 未提交 / 已提交,给程序的做法完全不同,这里替策划把话说清楚。
        $wc = Get-TableWorkingCopyChanges $TableRoot
        $dirty = @()
        if ($null -ne $wc) { $dirty = @($wc | Where-Object { $_.Path -like "*$leaf" }) }
        if ($null -eq $wc) {
            Write-Host '      做法:把上面那行报错原文发给程序(本机没有 svn 命令,没法判断这张表是否已提交)。'
        } elseif ($dirty.Count -gt 0) {
            Write-Host ''
            Write-Warn2 "  注意:$leaf 在你本机是**未提交**的改动(svn status: $($dirty[0].Item))。"
            Write-Host '      程序在自己机器上 svn update 也拿不到这一列,复现不了、也没法照着加字段。'
            Write-Host '      做法:先把这张 xlsx 提交进 SVN(或连文件一起发给程序),再把上面报错原文发过去。'
        } else {
            Write-Host ''
            Write-Host "      这张表在你本机与 SVN 一致($SourceRev),程序 svn update 后就能看到这一列。"
            Write-Host '      做法:把上面那行报错原文发给程序即可。'
        }
        Write-Host '      临时想继续导表:把该列从 xlsx 撤掉(或对这张表 svn revert)。'
    } elseif ($outText -match '外键校验失败') {
        Write-Warn2 '原因:有表引用了另一张表里不存在的 id(比如掉落表引用了已删除的道具 id)。'
        Write-Host '      看上面报错里点名的「表名 / 主键 / 字段」,把那一行的引用改成存在的 id,或把被引用的行加回来。'
    } elseif ($outText -match '位序|bit_index|bitindex') {
        Write-Warn2 '原因:位序状态(configtable/bitindex_state)对不上。这个必须找程序,'
        Write-Host '      **不要**自己删或改 bitindex_state 下的文件——那会让已存档的玩家进度位图整体错位。'
    } elseif ($outText -match '表头第\s*(\S+)\s*列:\s*期望\s*"([^"]*)"\s*实为\s*"([^"]*)"') {
        $col = $Matches[1]; $expect = $Matches[2]; $actual = $Matches[3]
        Write-Warn2 "原因:第 $col 列的列名变了——proto 登记的是「$expect」,xlsx 里现在是「$actual」。"
        Write-Host '      这**不是文件被 Excel 占用**,也不是 SVN 没更新,是服务端 proto 注解需要同步。'
        Write-SyncHint '列改名'
        Write-Host "      临时想继续导表:把列名改回「$expect」。"
    } elseif ($outText -match '.*必填列为空') {
        if ($outText -match '(\S+\.xlsx)\s+第\s+(\d+)\s+行\s+([^:\r\n]+):\s*必填列为空') {
            Write-Warn2 "原因:$($Matches[1]) 第 $($Matches[2]) 行的「$($Matches[3].Trim())」没填。"
        } else {
            Write-Warn2 '原因:表数据里有必填列为空。'
        }
        Write-Host '      这**不是 xlsx 读取失败**,Excel 占用或 SVN 更新都不会修好这个值。'
        Write-Host '      做法:按上面点名的表 / 行 / 列填入合法值后重试;0 也必须显式填 0。'
    } elseif ($outText -match '读\s+.*\.xlsx\s+失败|打开 xlsx 失败') {
        Write-Warn2 '原因:某张 xlsx 读不出来。常见是文件正被 Excel 打开、或 SVN 更新到一半。'
        Write-Host '      做法:关掉 Excel,svn update 后重试。'
    } else {
        Write-Warn2 '未识别的错误,把上面整段输出发给程序。'
    }
    exit $genExit
}

# ---------------------------------------------------------------------------
# 7. 成功:报告行数增减
# ---------------------------------------------------------------------------
$after = Read-ManifestRows $DistDir
if ($null -eq $after) {
    Write-Err "导表报成功,但 $DistDir 下读不到 manifest.json,请找程序看。"
    exit 1
}

Write-Host ''
if ($null -ne $before -and $before.Version -eq $after.Version) {
    Write-Ok "内容与上一批完全相同,批次号未变(v$($after.Version)),没有需要提交的改动。"
} else {
    $oldV = if ($null -eq $before) { '(无)' } else { "v$($before.Version)" }
    Write-Ok "导表成功:批次 $oldV -> v$($after.Version)"
    if ($null -ne $before) {
        $shrunk = New-Object System.Collections.Generic.List[string]
        foreach ($name in ($after.Rows.Keys | Sort-Object)) {
            $newRows = $after.Rows[$name]
            $oldRows = if ($before.Rows.ContainsKey($name)) { $before.Rows[$name] } else { $null }
            if ($null -eq $oldRows) {
                Write-Host ("      + {0,-20} 新表 {1} 行" -f $name, $newRows) -ForegroundColor Green
            } elseif ($newRows -ne $oldRows) {
                $delta = $newRows - $oldRows
                $sign = if ($delta -gt 0) { '+' } else { '' }
                $color = if ($delta -lt 0) { 'Yellow' } else { 'Green' }
                Write-Host ("        {0,-20} {1} -> {2} 行 ({3}{4})" -f $name, $oldRows, $newRows, $sign, $delta) -ForegroundColor $color
                if ($delta -lt 0) { $shrunk.Add($name) }
            }
        }
        if ($shrunk.Count -gt 0) {
            Write-Host ''
            # 刻意只提醒不拦截:2026-08-05 道具表 100→65 就是"占位表换真表",行数变少完全正当。
            # 行数是信号不是判据,真正要看的是内容。
            Write-Warn2 ("这些表行数变少了:{0}" -f ($shrunk -join ', '))
            Write-Host '      行数变少不一定是错(删占位、删废弃行都正常),但请顺手确认一下是有意的:'
            Write-Host '      如果是 SVN 更新时把别人的改动覆盖掉了,现在导出去就会固化到服务端。'
        }
    }
}

Write-Host ''
Write-Info '产物在: ' -NoNewline; Write-Host $DistDir
Write-Host '      这批文件必须**整批一起**提交(dist 下全部 json + manifest.json),'
Write-Host '      只提交一部分会让服务端启动时校验失败。'
Write-Host '      本机重启后端即可生效;要让其他人也拿到,需要把服务端仓库这批改动提交上去。'
Write-Host ''
exit 0
