# Pandora 客户端仓(SVN 工作副本)定位公共函数。
# 被 configtable_gen.ps1 / configtable_sync.ps1 / start.ps1 dot-source 引用,不单独运行。
#
# 为什么不能写死一个目录名
# ------------------------
# 客户端仓在 SVN 上的名字是 **Client**(`^/trunk/Client`,与 Art / Design / Server 平级),
# `svn checkout <url>` 不带目标目录名时,检出来的目录就叫 Client。而后端这边的文档
# (tools/devops/BUILD-MACHINE-SETUP.md)给构建机的例子写的是 `... D:\Pandora-Client-SVN`,
# 开发机上也确实叫 F:\work\Pandora-Client-SVN。于是同一个仓在不同机器上至少有两种名字,
# 而此前 configtable_gen.ps1 只认后者 —— 策划机上按 SVN 原名检出的 Client 目录一个都找不到,
# 表现为免 Docker 一键启动在**第一步导表**就 [ERR] 中止,整个后端起不来(2026-08-18 现场)。
#
# 所以这里不再赌目录名,按「先认名字、再认内容」找:
#   1. 显式入口(参数 / PANDORA_CLIENT_TABLE_ROOT / PANDORA_CLIENT_REPO / PANDORA_DS_UPROJECT)
#      —— 永远最优先,机器怎么摆都能一句话救回来;
#   2. 已知仓名,Client(SVN 原名)排最前,其后是历史上用过的几个名字;
#   3. 平级 / 上一级目录里任何一个"长得像客户端仓"的目录 —— 容忍策划自己取的中文名、
#      也容忍把整个 ^/trunk 检出成一个目录(那时客户端仓是它下面的 Client);
#   4. 老开发机的写死路径(仅是最后候选,盘符不存在时安全跳过)。
# 判据始终是**内容**(Table 下有 xlsx / 有 Pandora\Pandora.uproject),不是名字:
# 名字只用来决定先看谁,防止一台机器上有多份检出时随机挑一个。

# 已知仓名。Client 必须排第一 —— 它是 SVN 上的名字,新机器按官方 URL 检出就是这个。
$script:PandoraClientRepoNames = @('Client', 'Pandora-Client-SVN', 'Pandora-Client', 'ClientBase')

# 整个 ^/trunk 被检出成一个目录时,客户端仓是它下面的这一层。
$script:PandoraTrunkChildName = 'Client'

function Add-PandoraPathCandidate {
    param(
        [System.Collections.Generic.List[string]]$List,
        [System.Collections.Generic.HashSet[string]]$Seen,
        [string]$Path
    )
    if ([string]::IsNullOrWhiteSpace($Path)) { return }
    $full = $null
    try { $full = [System.IO.Path]::GetFullPath($Path.Trim()) } catch { return }
    $full = $full.TrimEnd('\', '/')
    if ([string]::IsNullOrWhiteSpace($full)) { return }
    if (-not $Seen.Add($full.ToLowerInvariant())) { return }
    $List.Add($full)
}

function Get-PandoraFixedDriveRoot {
    # 只取本机固定盘:网络盘 / U 盘上枚举目录可能卡住,而客户端仓不会在那里。
    $roots = @()
    try {
        foreach ($d in [System.IO.DriveInfo]::GetDrives()) {
            if ($d.DriveType -ne [System.IO.DriveType]::Fixed) { continue }
            if (-not $d.IsReady) { continue }
            $roots += $d.RootDirectory.FullName
        }
    } catch { return @() }
    return $roots
}

# Get-PandoraClientRepoCandidate:按优先级返回**可能是**客户端仓根目录的绝对路径(已去重)。
# 只做路径拼接与去重,不做存在性 / 内容判断 —— 判定留给调用方(要表的看 Table,
# 要 DS 的看 Pandora\Pandora.uproject),免得两边对"什么算客户端仓"各有一套。
function Get-PandoraClientRepoCandidate {
    param([string]$ProjectRoot = '')

    $out = New-Object 'System.Collections.Generic.List[string]'
    $seen = New-Object 'System.Collections.Generic.HashSet[string]'

    # ---- 1. 显式入口 ----
    Add-PandoraPathCandidate $out $seen $env:PANDORA_CLIENT_REPO
    # PANDORA_DS_UPROJECT 指的是 <客户端仓>\Pandora\Pandora.uproject,回推两级就是仓根。
    # 已经为了跑 DS 设过这一个的机器,导表不该再让人设第二个。
    if (-not [string]::IsNullOrWhiteSpace($env:PANDORA_DS_UPROJECT)) {
        try {
            $repoFromUProject = Split-Path -Parent (Split-Path -Parent $env:PANDORA_DS_UPROJECT.Trim())
            Add-PandoraPathCandidate $out $seen $repoFromUProject
        } catch { }
    }

    # ---- 扫描根:后端仓的父目录、再上一级,以及所有固定盘的盘符根 ----
    $siblingRoots = New-Object 'System.Collections.Generic.List[string]'
    if (-not [string]::IsNullOrWhiteSpace($ProjectRoot)) {
        $parent = Split-Path -Parent $ProjectRoot
        if ($parent) {
            $siblingRoots.Add($parent)
            $grand = Split-Path -Parent $parent
            if ($grand) { $siblingRoots.Add($grand) }
        }
    }
    $driveRoots = Get-PandoraFixedDriveRoot

    # ---- 2. 后端仓近旁(平级 / 上一级)——先按已知仓名,再放宽到任意仓名 ----
    # 「两个仓放同一个父目录下」是唯一被文档要求过的摆法,所以近旁的证据一律排在
    # 满盘搜之前:一台机器上同时存在多份检出是常态(开发机 F:\work 与 D:\luyuan 各一份),
    # 「离后端仓最近的那一份」比「另一块盘上名字恰好对得上的那一份」更可能是人想用的。
    foreach ($root in $siblingRoots) {
        foreach ($name in $script:PandoraClientRepoNames) {
            Add-PandoraPathCandidate $out $seen (Join-Path $root $name)
        }
    }
    # 自定义仓名只在这两层通配。再放宽就会开始猜整块盘,一旦猜错(比如猜到某个旧备份)
    # 表现是"改了表没生效",比找不到更难查。
    foreach ($root in $siblingRoots) {
        foreach ($dir in @(Get-ChildItem -LiteralPath $root -Directory -ErrorAction SilentlyContinue)) {
            Add-PandoraPathCandidate $out $seen $dir.FullName
            # 整个 ^/trunk 被检出成一个目录时,客户端仓是它下面的 Client。
            Add-PandoraPathCandidate $out $seen (Join-Path $dir.FullName $script:PandoraTrunkChildName)
        }
    }

    # ---- 3. 两个仓没放一块:各固定盘的根目录及其下一层,只认已知仓名 ----
    # D:\Client / E:\游戏\Pandora-Client-SVN 这类摆法。这里刻意不通配目录名 ——
    # 满盘按内容猜会把 D:\备份\Table 这种误当客户端仓,而误判的代价是静默用错表。
    foreach ($root in $driveRoots) {
        foreach ($name in $script:PandoraClientRepoNames) {
            Add-PandoraPathCandidate $out $seen (Join-Path $root $name)
        }
    }
    foreach ($root in $driveRoots) {
        foreach ($dir in @(Get-ChildItem -LiteralPath $root -Directory -ErrorAction SilentlyContinue)) {
            foreach ($name in $script:PandoraClientRepoNames) {
                Add-PandoraPathCandidate $out $seen (Join-Path $dir.FullName $name)
            }
        }
    }

    # ---- 4. 老开发机写死路径(可选兼容项,不要求本机存在 F 盘)----
    Add-PandoraPathCandidate $out $seen 'F:\work\Pandora-Client-SVN'

    return $out.ToArray()
}

# Test-PandoraTableRoot:判一个目录是不是策划表根。
# 返回 'ok' / 'empty'(目录在但一张 xlsx 都没有)/ 'missing'。
# 'empty' 必须和 'missing' 分开报:目录在却没有源表,是**检出不完整 / 源表没进 SVN**,
# 跟"路径找错了"是两码事,把人指错方向就等于让他去改一个没坏的东西。
function Test-PandoraTableRoot {
    param([string]$Path)
    if ([string]::IsNullOrWhiteSpace($Path)) { return 'missing' }
    $full = $null
    try { $full = [System.IO.Path]::GetFullPath($Path) } catch { return 'missing' }
    if (-not (Test-Path -LiteralPath $full -PathType Container -ErrorAction SilentlyContinue)) { return 'missing' }
    $anyXlsx = Get-ChildItem -LiteralPath $full -Filter '*.xlsx' -Recurse -File -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($null -eq $anyXlsx) { return 'empty' }
    return 'ok'
}

# Resolve-PandoraClientTableRoot:定位策划表根目录(<客户端仓>\Table)。
# 返回 [pscustomobject]:
#   Path     命中的目录(找不到为 '')
#   Source   怎么找到的(报给人看,免得"它到底用了哪份表"要靠猜)
#   Others   还命中了、但没用上的其它策划表目录 —— 一台机器上有多份检出时必须报出来,
#            否则"导的是另一份表"会以"我改的没生效"的形态出现,最难自查
#   NearMiss 目录在但没有 xlsx 的(检出不完整 / 源表没提交)
function Resolve-PandoraClientTableRoot {
    param(
        [string]$Explicit = '',
        [string]$ProjectRoot = ''
    )

    $nearMiss = New-Object 'System.Collections.Generic.List[string]'

    # ---- 显式入口:命中即用,一律不再扫盘(人指哪儿就是哪儿)----
    $direct = New-Object 'System.Collections.Generic.List[object]'
    if (-not [string]::IsNullOrWhiteSpace($Explicit)) {
        $direct.Add([pscustomobject]@{ Path = $Explicit.Trim(); Source = '-TableRoot 参数' })
    }
    if (-not [string]::IsNullOrWhiteSpace($env:PANDORA_CLIENT_TABLE_ROOT)) {
        $direct.Add([pscustomobject]@{ Path = $env:PANDORA_CLIENT_TABLE_ROOT.Trim(); Source = '环境变量 PANDORA_CLIENT_TABLE_ROOT' })
    }
    foreach ($d in $direct) {
        $verdict = Test-PandoraTableRoot $d.Path
        if ($verdict -eq 'ok') {
            return [pscustomobject]@{
                Path     = [System.IO.Path]::GetFullPath($d.Path)
                Source   = $d.Source
                Others   = @()
                NearMiss = @($nearMiss)
            }
        }
        if ($verdict -eq 'empty') {
            $nearMiss.Add(('{0}(目录在,但底下一张 xlsx 都没有 —— 来自 {1})' -f $d.Path, $d.Source))
        }
    }

    # ---- 自动探测 ----
    $hits = New-Object 'System.Collections.Generic.List[string]'
    foreach ($repo in (Get-PandoraClientRepoCandidate -ProjectRoot $ProjectRoot)) {
        # 候选可以来自另一台机器留下的环境变量,其盘符在本机未必存在。
        # Path.Combine 只拼字符串,不会像 Join-Path 一样先解析 PowerShell drive 而中断启动。
        $table = [System.IO.Path]::Combine($repo, 'Table')
        switch (Test-PandoraTableRoot $table) {
            'ok' { $hits.Add([System.IO.Path]::GetFullPath($table)) }
            'empty' { $nearMiss.Add(('{0}(目录在,但底下一张 xlsx 都没有)' -f $table)) }
            default { }
        }
    }

    if ($hits.Count -eq 0) {
        return [pscustomobject]@{ Path = ''; Source = ''; Others = @(); NearMiss = @($nearMiss) }
    }
    return [pscustomobject]@{
        Path     = $hits[0]
        Source   = '自动探测(后端仓平级 / 上一级 / 各固定盘下的已知仓名)'
        Others   = @($hits | Select-Object -Skip 1)
        NearMiss = @($nearMiss)
    }
}

# Write-PandoraClientTableRootHelp:找不到时把"该怎么办"一次说清。
# 用 Write-Host 而不是返回字符串:两处调用方都是直接打给人看的一键入口。
function Write-PandoraClientTableRootHelp {
    param(
        [string]$ProjectRoot = '',
        [string[]]$NearMiss = @()
    )
    Write-Host '      按顺序找过:'
    Write-Host '        -TableRoot 参数 / 环境变量 PANDORA_CLIENT_TABLE_ROOT'
    Write-Host '        环境变量 PANDORA_CLIENT_REPO / PANDORA_DS_UPROJECT 推出来的客户端仓'
    Write-Host ('        已知仓名 {0} —— 找过后端仓的平级目录、上一级目录,和每个固定盘的根目录及其下一层' -f ($script:PandoraClientRepoNames -join ' / '))
    if ($ProjectRoot) {
        $parent = Split-Path -Parent $ProjectRoot
        if ($parent) {
            Write-Host ('        后端仓平级目录里任意一个 <目录>\Table 或 <目录>\Client\Table(即 {0}\*)' -f $parent)
        }
    }
    if ($NearMiss.Count -gt 0) {
        Write-Host ''
        Write-Host '      有长得像的目录,但里面没有 xlsx(这不是路径问题,是检出不完整或源表没提交):'
        foreach ($n in $NearMiss) { Write-Host ('        {0}' -f $n) }
    }
    Write-Host ''
    Write-Host '      客户端仓在 SVN 上叫 Client(^/trunk/Client),本地检出后叫什么都行,二选一:'
    Write-Host '        (a) 把它和后端仓放在同一个父目录下,以后双击就能自动找到;'
    Write-Host '        (b) 设一次环境变量指到你的客户端仓根目录(设完要重开窗口):'
    Write-Host '            setx PANDORA_CLIENT_REPO D:\你的客户端目录'
    Write-Host '      还没检出过客户端仓:'
    Write-Host '        svn checkout http://infinity-svn/svn/Pandora-Moba/trunk/Client D:\Client'
    Write-Host '      只想这一次跑通,不改机器设置:'
    Write-Host '        pwsh tools\scripts\configtable_gen.ps1 -TableRoot D:\你的客户端目录\Table'
}
