# 策划免 Docker mysql-init 强收据 / 批量 stdin 契约；不连真实 MySQL。
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ScriptsDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$ProjectRoot = (Resolve-Path (Join-Path $ScriptsDir '../..')).Path
$DevMigrate = Join-Path $ScriptsDir 'dev_migrate.ps1'
$FastLib = Join-Path $ScriptsDir 'lib/planner_migrate_fast.ps1'
$script:Failures = [Collections.Generic.List[string]]::new()

function Assert-True([bool]$Condition, [string]$Message) {
    if ($Condition) { Write-Host "  [ok] $Message" -ForegroundColor Green }
    else { $script:Failures.Add($Message); Write-Host "  [FAIL] $Message" -ForegroundColor Red }
}

Write-Host '[1] fast 只能由策划本机入口启用' -ForegroundColor Cyan
$scriptText = [IO.File]::ReadAllText($DevMigrate)
Assert-True ($scriptText -match '\$PlannerFastStart\s*=\s*\$UseLocalClient\s+-and\s+\(\$env:PANDORA_PLANNER_FAST_START -ceq ''1''\)') `
    '必须同时是本机 mysql.exe 和策划 fast env'
Assert-True ($scriptText -match '(?s)if \(\$PlannerFastStart\).*?Invoke-DevMysqlScriptsBatch.*?else\s*\{\s*foreach \(\$f in \$initFiles\).*?Invoke-DevMysqlScript') `
    '策划未命中走单进程批量，普通/Docker 仍保留逐文件老路径'
Assert-True ($scriptText -match '(?s)elseif \(\$skipInitReplay\).*?跳过重复 DDL.*?# -+\s*# 第 1 步.*?\& \$migrateExe @migArgs') `
    '收据只跳过 init DDL，正式 pandora-migrate 仍每次执行'
Assert-True ($scriptText -match '(?s)if \(\$WhatIfOnly\).*?elseif \(\$skipInitReplay\).*?Write-PandoraPlannerMysqlInitReceipt') `
    'WhatIf 分支不执行 SQL 也不写收据'

$tokens = $null
$parseErrors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile($DevMigrate, [ref]$tokens, [ref]$parseErrors)
$batchFunction = $ast.FindAll({
        param($node)
        $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Invoke-DevMysqlScriptsBatch'
    }, $true) | Select-Object -First 1
$nativeCalls = @(if ($batchFunction) {
    @($batchFunction.Body.FindAll({
                param($node)
                $node -is [Management.Automation.Language.CommandAst] -and $node.Extent.Text -match '^&\s+\$MysqlClient'
            }, $true))
} else { @() })
Assert-True ($nativeCalls.Count -eq 1) '批量未命中路径只启动一次 mysql.exe'

if (-not (Test-Path -LiteralPath $FastLib -PathType Leaf)) { throw "[RED] 缺少 helper:$FastLib" }
. $FastLib

Write-Host '[2] SQL 按文件名排序并一次性完整拼接' -ForegroundColor Cyan
$tmp = Join-Path ([IO.Path]::GetTempPath()) ("pandora-migrate-fast-test-{0}" -f [guid]::NewGuid().ToString('N'))
try {
    New-Item -ItemType Directory -Force -Path $tmp | Out-Null
    $fileB = Join-Path $tmp '02-b.sql'
    $fileA = Join-Path $tmp '01-a.sql'
    [IO.File]::WriteAllText($fileB, "USE ``db_b``;`nCREATE TABLE IF NOT EXISTS ``t_b`` (id INT);", [Text.UTF8Encoding]::new($false))
    [IO.File]::WriteAllText($fileA, "CREATE DATABASE IF NOT EXISTS ``db_a``;`nUSE ``db_a``;`nCREATE TABLE IF NOT EXISTS ``t_a`` (id INT);", [Text.UTF8Encoding]::new($false))
    $inputFiles = @($fileB, $fileA)
    $joined = Join-PandoraPlannerMysqlInitScripts -Files $inputFiles
    $aIndex = $joined.IndexOf('-- pandora-init-file: 01-a.sql', [StringComparison]::Ordinal)
    $bIndex = $joined.IndexOf('-- pandora-init-file: 02-b.sql', [StringComparison]::Ordinal)
    Assert-True ($aIndex -ge 0 -and $bIndex -gt $aIndex) '输入即使乱序，stdin 仍按 SQL 文件名排序'
    Assert-True ($joined.Contains('CREATE TABLE IF NOT EXISTS `t_a`') -and $joined.Contains('CREATE TABLE IF NOT EXISTS `t_b`')) `
        '合并 stdin 保留每个文件的 UTF-8 完整内容'

    $inventory = Get-PandoraPlannerMysqlInitInventory -Files $inputFiles
    Assert-True (($inventory.Databases -join ',') -eq 'db_a,db_b') '库清单从真实 CREATE DATABASE/USE 语句提取'
    Assert-True (($inventory.Tables -join ',') -eq 'db_a.t_a,db_b.t_b') '表清单绑定当前 USE 库与表名'

    Write-Host '[3] 收据必须同时绑定强哈希、MySQL 身份和实际库/表' -ForegroundColor Cyan
    $fingerprintA = Get-PandoraPlannerMysqlInitFingerprint -Files $inputFiles -ProjectRoot $tmp
    $receipt = Join-Path $tmp 'receipt.json'
    Write-PandoraPlannerMysqlInitReceipt -ReceiptPath $receipt -Fingerprint $fingerprintA `
        -ServerUuid 'uuid-a' -DataDir 'F:\mysql\data\' -FileCount 2 -DatabaseCount 2 -TableCount 2
    $hitArgs = @{
        ReceiptPath = $receipt; Fingerprint = $fingerprintA; ServerUuid = 'UUID-A'; DataDir = 'f:\MYSQL\data'
        FileCount = 2; ExpectedDatabases = $inventory.Databases; ExpectedTables = $inventory.Tables
        ActualDatabases = @('db_a', 'db_b'); ActualTables = @('db_a.t_a', 'db_b.t_b')
    }
    Assert-True (Test-PandoraPlannerMysqlInitReceipt @hitArgs) '全部身份和实际 inventory 一致才命中'
    $uuidMiss = $hitArgs.Clone(); $uuidMiss.ServerUuid = 'uuid-b'
    Assert-True (-not (Test-PandoraPlannerMysqlInitReceipt @uuidMiss)) 'server_uuid 变化必须 miss'
    $dirMiss = $hitArgs.Clone(); $dirMiss.DataDir = 'D:\other-data'
    Assert-True (-not (Test-PandoraPlannerMysqlInitReceipt @dirMiss)) 'datadir 变化必须 miss'
    $tableMiss = $hitArgs.Clone(); $tableMiss.ActualTables = @('db_a.t_a')
    Assert-True (-not (Test-PandoraPlannerMysqlInitReceipt @tableMiss)) '任一期望实表缺失必须 miss 并重放'
    [IO.File]::WriteAllText($fileB, "USE ``db_b``;`nCREATE TABLE IF NOT EXISTS ``t_b`` (id BIGINT);", [Text.UTF8Encoding]::new($false))
    $fingerprintB = Get-PandoraPlannerMysqlInitFingerprint -Files $inputFiles -ProjectRoot $tmp
    Assert-True ($fingerprintA -ne $fingerprintB) 'SQL 内容变化必须 miss'
    $hashMiss = $hitArgs.Clone(); $hashMiss.Fingerprint = $fingerprintB
    Assert-True (-not (Test-PandoraPlannerMysqlInitReceipt @hashMiss)) '收据不能命中新 SQL 强哈希'

    $probe = ConvertFrom-PandoraPlannerMysqlProbe -Lines @(
        '__PANDORA_UUID__=uuid-a', '__PANDORA_DATADIR__=F:\mysql\data\',
        '__PANDORA_DB__=db_a', '__PANDORA_TABLE__=db_a.t_a', 'ignored warning')
    Assert-True ($probe.ServerUuid -eq 'uuid-a' -and $probe.Databases[0] -eq 'db_a' -and $probe.Tables[0] -eq 'db_a.t_a') `
        '单次 MySQL probe 可精确解出 UUID/datadir/库/表，忽略非标记诊断行'
} finally {
    Remove-Item -LiteralPath $tmp -Recurse -Force -ErrorAction SilentlyContinue
}

if ($script:Failures.Count -gt 0) {
    throw "dev_migrate 策划 fast 契约失败($($script:Failures.Count)):`n - $($script:Failures -join "`n - ")"
}
Write-Host '[PASS] dev_migrate 策划 fast 契约通过。' -ForegroundColor Green
