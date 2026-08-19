# 客户端仓 / 策划表根目录定位契约(2026-08-18)。
#
# 守的是什么:一键导表能不能找到客户端仓,**不取决于本地目录叫什么名字**。
# 现场:策划机按 SVN 原名检出(^/trunk/Client → 目录名 Client),而 configtable_gen.ps1
# 当时只认 Pandora-Client-SVN,于是「策划一键启动-免Docker-测试版.cmd」在第一步导表就
# [ERR] 中止,整套后端起不来。这个回归没有任何 go test 能挡 —— 定位逻辑全在 ps1 里,
# 而且只在"目录名恰好不一样"的机器上才复现,开发机(F:\work\Pandora-Client-SVN)永远是绿的。
#
# 用法:pwsh tools/scripts/tests/configtable_client_repo_resolve_test.ps1

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

. (Join-Path (Split-Path -Parent $PSScriptRoot) 'client_repo_lib.ps1')

$script:Failed = New-Object 'System.Collections.Generic.List[string]'
function Assert-True([bool]$Cond, [string]$What) {
    if ($Cond) { Write-Host "  [ok] $What" } else { $script:Failed.Add($What); Write-Host "  [NG] $What" -ForegroundColor Red }
}
function Assert-PathEq([string]$Actual, [string]$Expected, [string]$What) {
    $a = if ($Actual) { $Actual.TrimEnd('\').ToLowerInvariant() } else { '' }
    $e = if ($Expected) { $Expected.TrimEnd('\').ToLowerInvariant() } else { '' }
    if ($a -eq $e) { Write-Host "  [ok] $What" }
    else { $script:Failed.Add("$What (实得 '$Actual',应为 '$Expected')"); Write-Host "  [NG] $What`n       实得 $Actual`n       应为 $Expected" -ForegroundColor Red }
}

function New-FakeTableRoot([string]$RepoRoot, [switch]$NoXlsx) {
    # 造一个"长得像客户端仓"的目录:<repo>\Table\关卡\g_关卡.xlsx。
    # -NoXlsx 造只有目录没有源表的那种(检出不完整 / 源表没进 SVN)。
    $tableDir = Join-Path $RepoRoot 'Table\关卡'
    New-Item -ItemType Directory -Path $tableDir -Force | Out-Null
    if (-not $NoXlsx) {
        Set-Content -LiteralPath (Join-Path $tableDir 'g_关卡.xlsx') -Value 'fake' -NoNewline
    }
    return (Join-Path $RepoRoot 'Table')
}

# 环境变量会越过所有自动探测,跑测试前必须清干净,否则这台机器上恰好设过就假绿。
$saved = @{}
foreach ($k in @('PANDORA_CLIENT_TABLE_ROOT', 'PANDORA_CLIENT_REPO', 'PANDORA_DS_UPROJECT')) {
    $saved[$k] = [Environment]::GetEnvironmentVariable($k)
    Set-Item -Path "env:$k" -Value ''
}

$sandbox = Join-Path ([System.IO.Path]::GetTempPath()) ("pandora-clientrepo-" + [guid]::NewGuid().ToString('N'))
try {
    # ===== 1. SVN 原名 Client:核心回归 =====
    Write-Host '[1] 目录按 SVN 原名叫 Client 也要能找到'
    $case1 = Join-Path $sandbox 'case1'
    $srv1 = Join-Path $case1 'XuanMing-Server'
    New-Item -ItemType Directory -Path $srv1 -Force | Out-Null
    $want1 = New-FakeTableRoot (Join-Path $case1 'Client')
    $r1 = Resolve-PandoraClientTableRoot -ProjectRoot $srv1
    Assert-PathEq $r1.Path $want1 '平级的 Client\Table 被选中'

    # ===== 2. 两种名字同时存在时,SVN 原名优先且另一个必须被报出来 =====
    Write-Host '[2] Client 与 Pandora-Client-SVN 并存:选 Client,另一个进 Others'
    $case2 = Join-Path $sandbox 'case2'
    $srv2 = Join-Path $case2 'XuanMing-Server'
    New-Item -ItemType Directory -Path $srv2 -Force | Out-Null
    $want2 = New-FakeTableRoot (Join-Path $case2 'Client')
    $other2 = New-FakeTableRoot (Join-Path $case2 'Pandora-Client-SVN')
    $r2 = Resolve-PandoraClientTableRoot -ProjectRoot $srv2
    Assert-PathEq $r2.Path $want2 'Client 排在 Pandora-Client-SVN 之前(顺序必须确定,不能随枚举顺序漂)'
    Assert-True (@($r2.Others | ForEach-Object { $_.TrimEnd('\').ToLowerInvariant() }) -contains $other2.TrimEnd('\').ToLowerInvariant()) `
        '没被选中的那份出现在 Others(选错那份的表现是"改了表没生效",必须报出来)'

    # ===== 3. 策划自己取的仓名(含中文)=====
    Write-Host '[3] 自定义仓名(平级通配)'
    $case3 = Join-Path $sandbox 'case3'
    $srv3 = Join-Path $case3 'XuanMing-Server'
    New-Item -ItemType Directory -Path $srv3 -Force | Out-Null
    $want3 = New-FakeTableRoot (Join-Path $case3 '我的客户端')
    $r3 = Resolve-PandoraClientTableRoot -ProjectRoot $srv3
    Assert-PathEq $r3.Path $want3 '平级目录里任意仓名都能找到'

    # ===== 4. 整个 ^/trunk 被检出成一个目录 =====
    Write-Host '[4] 检出的是整个 trunk:<平级目录>\Client\Table'
    $case4 = Join-Path $sandbox 'case4'
    $srv4 = Join-Path $case4 'XuanMing-Server'
    New-Item -ItemType Directory -Path $srv4 -Force | Out-Null
    $want4 = New-FakeTableRoot (Join-Path $case4 'Pandora-Moba\Client')
    $r4 = Resolve-PandoraClientTableRoot -ProjectRoot $srv4
    Assert-PathEq $r4.Path $want4 'trunk 整检出的布局也能找到'

    # ===== 5. -TableRoot 显式指定:最高优先,且命中后不再扫盘 =====
    Write-Host '[5] -TableRoot 显式指定'
    $case5 = Join-Path $sandbox 'case5'
    $srv5 = Join-Path $case5 'XuanMing-Server'
    New-Item -ItemType Directory -Path $srv5 -Force | Out-Null
    $explicit5 = New-FakeTableRoot (Join-Path $case5 '别处的仓')
    New-FakeTableRoot (Join-Path $case5 'Client') | Out-Null
    $r5 = Resolve-PandoraClientTableRoot -Explicit $explicit5 -ProjectRoot $srv5
    Assert-PathEq $r5.Path $explicit5 '-TableRoot 压过自动探测'
    Assert-True ($r5.Others.Count -eq 0) '显式指定命中后不再扫盘(Others 为空)'

    # ===== 6. PANDORA_CLIENT_REPO 环境变量 =====
    Write-Host '[6] 环境变量 PANDORA_CLIENT_REPO 指到仓根'
    $case6 = Join-Path $sandbox 'case6'
    $srv6 = Join-Path $case6 'XuanMing-Server'
    New-Item -ItemType Directory -Path $srv6 -Force | Out-Null
    $repo6 = Join-Path $case6 '环境变量指定的仓'
    $want6 = New-FakeTableRoot $repo6
    New-FakeTableRoot (Join-Path $case6 'Client') | Out-Null
    $env:PANDORA_CLIENT_REPO = $repo6
    try { $r6 = Resolve-PandoraClientTableRoot -ProjectRoot $srv6 } finally { $env:PANDORA_CLIENT_REPO = '' }
    Assert-PathEq $r6.Path $want6 'PANDORA_CLIENT_REPO 压过平级探测'

    # ===== 7. Table 在但一张 xlsx 都没有:不算命中,且必须单独报 =====
    Write-Host '[7] Table 目录在但没有 xlsx(检出不完整 / 源表没提交)'
    $case7 = Join-Path $sandbox 'case7'
    $srv7 = Join-Path $case7 'XuanMing-Server'
    New-Item -ItemType Directory -Path $srv7 -Force | Out-Null
    $empty7 = New-FakeTableRoot (Join-Path $case7 'Client') -NoXlsx
    $r7 = Resolve-PandoraClientTableRoot -ProjectRoot $srv7
    Assert-True ($r7.Path.TrimEnd('\').ToLowerInvariant() -ne $empty7.TrimEnd('\').ToLowerInvariant()) `
        '空 Table 不当成命中(否则生成器会报一堆"读 xlsx 失败",把人指错方向)'
    Assert-True (@($r7.NearMiss) -join "`n" -match 'case7') '空 Table 进 NearMiss(这不是路径问题,提示必须说清是哪一类)'

    # ===== 8. 找不到时的帮助文本必须写明 SVN 上的名字和检出命令 =====
    Write-Host '[8] 帮助文本'
    $help = (Write-PandoraClientTableRootHelp -ProjectRoot 'D:\somewhere\XuanMing-Server' -NearMiss @() 6>&1 | Out-String)
    Assert-True ($help -match '\^/trunk/Client') '帮助里写明客户端仓在 SVN 上叫 Client'
    Assert-True ($help -match 'svn checkout http://infinity-svn/svn/Pandora-Moba/trunk/Client') '帮助里给出可直接复制的检出命令'
    Assert-True ($help -match 'PANDORA_CLIENT_REPO') '帮助里给出一次性设死路径的办法'
}
finally {
    foreach ($k in $saved.Keys) {
        if ($null -eq $saved[$k]) { Set-Item -Path "env:$k" -Value '' } else { Set-Item -Path "env:$k" -Value $saved[$k] }
    }
    if (Test-Path -LiteralPath $sandbox) { Remove-Item -LiteralPath $sandbox -Recurse -Force -ErrorAction SilentlyContinue }
}

if ($script:Failed.Count -gt 0) {
    Write-Host ''
    Write-Host '[FAIL] 客户端仓定位契约未通过:' -ForegroundColor Red
    $script:Failed | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}
Write-Host ''
Write-Host '[PASS] 客户端仓 / 策划表根目录定位契约' -ForegroundColor Green
