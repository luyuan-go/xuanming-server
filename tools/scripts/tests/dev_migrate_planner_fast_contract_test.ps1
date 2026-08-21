# 策划免 Docker mysql-init 强收据 / 批量 stdin 契约；不连真实 MySQL。
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ScriptsDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$ProjectRoot = (Resolve-Path (Join-Path $ScriptsDir '../..')).Path
$DevMigrate = Join-Path $ScriptsDir 'dev_migrate.ps1'
$DevAll = Join-Path $ScriptsDir 'dev_all.ps1'
$FastLib = Join-Path $ScriptsDir 'lib/planner_migrate_fast.ps1'
$BoundedProcessLib = Join-Path $ScriptsDir 'lib/planner_bounded_process.ps1'
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

Write-Host '[2] planner fast 的全部 native 调用共用单一总 deadline' -ForegroundColor Cyan
$devAllText = [IO.File]::ReadAllText($DevAll)
Assert-True ($scriptText -match '\[ValidateRange\(0,\s*3600\)\]\[int\]\$TotalTimeoutSeconds\s*=\s*0') `
    'dev_migrate 暴露默认关闭的总时限参数，普通入口默认行为不变'
Assert-True ($devAllText -match '(?s)&\s+"\$ScriptDir/dev_migrate\.ps1".*?-RequireMysql.*?-TotalTimeoutSeconds\s+600') `
    '策划父 runspace 显式给同一次 migration 传 600 秒总时限'
Assert-True ($scriptText -match '\$PlannerMigrationDeadline\s*=\s*\[Diagnostics\.Stopwatch\]::StartNew\(\)') `
    'fast migration 只创建一个单调 Stopwatch 作为总 deadline'
Assert-True ($scriptText -match 'function\s+Get-PlannerMigrationRemainingTimeoutMilliseconds') `
    '每个 native 调用从同一个总 deadline 计算剩余时间'
Assert-True ($scriptText -match [regex]::Escape(". (Join-Path `$ScriptDir 'lib/planner_bounded_process.ps1')")) `
    'planner fast 加载 Windows Job Object 有界进程 helper'
Assert-True ($scriptText -match '(?s)function\s+Test-PlannerMigrationProcessResult.*?\.ExitCode\s+-eq\s+0.*?\.TimedOut.*?\.DrainCompleted.*?\.StandardInputCompleted.*?\.StandardOutputTruncated.*?\.StandardErrorTruncated.*?\.Failure') `
    'timeout、stdin、drain、截断或 helper failure 任一异常都 fail closed'
Assert-True ($scriptText -match '(?s)function\s+Invoke-PlannerOwnedMigrationProcess.*?\.Environment\s*=\s*@\{\s*MYSQL_PWD\s*=\s*\$Password\s*\}.*?function\s+Invoke-DevMysqlQuery.*?Invoke-PlannerOwnedMigrationProcess\s+-Mode\s+\$mode\s+-Password\s+\$MysqlPassword.*?\$old\s*=\s*\$env:MYSQL_PWD') `
    'fast probe/查询把 MYSQL_PWD 只传给 child，普通本机路径保留原行为'
Assert-True ($scriptText -match '(?s)function\s+Invoke-DevMysqlScriptsBatch.*?Invoke-PlannerOwnedMigrationProcess\s+-Mode\s+''mysql-init-batch''\s+-Password\s+\$MysqlRootPassword.*?-StandardInput\s+\$sql') `
    'fast init batch 通过 helper stdin 写入，root 密码只进 child Environment'
Assert-True ($scriptText -match '(?s)\$PlannerFastStart.*?\$probeResult\s*=\s*Invoke-DevMysqlQuery.*?\$probeExitCode\s*=.*?\$probeResult\.ExitCode') `
    'fast 首次 probe 显式读取 helper 结果 ExitCode'
Assert-True ($scriptText -match '(?s)\$PlannerFastStart.*?Invoke-DevMysqlQuery\s+''SHOW DATABASES;''.*?\.ExitCode') `
    'fast init 后二次 query 显式读取 helper 结果 ExitCode'
Assert-True ($scriptText -match '(?s)if\s*\(\$PlannerFastStart\).*?Invoke-PlannerOwnedMigrationProcess\s+-Mode\s+''pandora-migrate''.*?else\s*\{.*?&\s+\$migrateExe\s+@migArgs') `
    'fast pandora-migrate 走 helper，普通路径仍保留原 native 调用'
Assert-True ($scriptText -match '(?s)if\s*\(\$PlannerFastStart\)\s*\{.*?pandora-migrate\.exe.*?exit\s+1.*?\}\s*else\s*\{\s*\$hasGo\s*=\s*\[bool\]\(Get-Command\s+go.*?&\s+go\s+run') `
    'fast 只允许发布包内预编译 migrator，普通人工路径保留 go run fallback'
$artifactGateAt = $scriptText.IndexOf('策划 fast 发布包的预编译迁移器不受信或缺失', [StringComparison]::Ordinal)
$sessionCreateAt = $scriptText.IndexOf('$tmpDir = New-PlannerMigrationSessionDirectory', [StringComparison]::Ordinal)
Assert-True ($artifactGateAt -ge 0 -and $sessionCreateAt -gt $artifactGateAt) `
    'fast artifact 缺失在创建 DSN/migrator target 前即非零退出'
Assert-True ($scriptText -match '(?s)finally\s*\{\s*if\s*\(\$PlannerFastStart\)\s*\{\s*Remove-PlannerMigrationSessionDirectory\s+-ProjectRoot\s+\$ProjectRoot\s+-SessionPath\s+\$tmpDir.*?Exit-PandoraOrchestrationLock.*?exit\s+0\s*$') `
    'fast 临时 DSN 始终 finally 严格清理，standalone 正常结束显式写回 exit 0'
Assert-True ($devAllText -match '(?s)&\s+"\$ScriptDir/dev_migrate\.ps1".*?ForEach-Object\s*\{\s*Write-Host\s+"\$_"\s*\}\s*\$migrationExitCode\s*=\s*\[int\]\$LASTEXITCODE') `
    '父 callback 在真实外部 .ps1 返回后立即读取 migration 的 LASTEXITCODE'

$tokens = $null
$parseErrors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile($DevMigrate, [ref]$tokens, [ref]$parseErrors)
$resultGuardFunction = $ast.FindAll({
        param($node)
        $node -is [Management.Automation.Language.FunctionDefinitionAst] -and
            $node.Name -eq 'Test-PlannerMigrationProcessResult'
    }, $true) | Select-Object -First 1
$deadlineFunction = $ast.FindAll({
        param($node)
        $node -is [Management.Automation.Language.FunctionDefinitionAst] -and
            $node.Name -eq 'Get-PlannerMigrationRemainingTimeoutMilliseconds'
    }, $true) | Select-Object -First 1
Assert-True ($null -ne $resultGuardFunction -and $null -ne $deadlineFunction) `
    '结果闸与共享 deadline seam 可由 AST 精确定位'
if ($resultGuardFunction -and $deadlineFunction) {
    . ([scriptblock]::Create($resultGuardFunction.Extent.Text))
    . ([scriptblock]::Create($deadlineFunction.Extent.Text))
    function New-FakeBoundedResult {
        param(
            [int]$ExitCode = 0, [bool]$TimedOut = $false, [bool]$DrainCompleted = $true,
            [bool]$StandardInputCompleted = $true, [bool]$StandardOutputTruncated = $false,
            [bool]$StandardErrorTruncated = $false, [string]$Failure = ''
        )
        return [pscustomobject]@{
            ExitCode = $ExitCode; TimedOut = $TimedOut; DrainCompleted = $DrainCompleted
            StandardInputCompleted = $StandardInputCompleted
            StandardOutputTruncated = $StandardOutputTruncated
            StandardErrorTruncated = $StandardErrorTruncated; Failure = $Failure
        }
    }
    Assert-True (Test-PlannerMigrationProcessResult (New-FakeBoundedResult)) `
        '只有完整 exit 0 结果通过'
    foreach ($mutant in @(
            (New-FakeBoundedResult -ExitCode 9),
            (New-FakeBoundedResult -TimedOut $true),
            (New-FakeBoundedResult -DrainCompleted $false),
            (New-FakeBoundedResult -StandardInputCompleted $false),
            (New-FakeBoundedResult -StandardOutputTruncated $true),
            (New-FakeBoundedResult -StandardErrorTruncated $true),
            (New-FakeBoundedResult -Failure 'pipe failed')
        )) {
        Assert-True (-not (Test-PlannerMigrationProcessResult $mutant)) `
            '任一 native 失败维度都不能冒充成功'
    }

    $PlannerFastStart = $true
    $TotalTimeoutSeconds = 600
    $PlannerMigrationDeadline = [pscustomobject]@{ ElapsedMilliseconds = 1250L }
    Assert-True ((Get-PlannerMigrationRemainingTimeoutMilliseconds) -eq 598750) `
        '后续 native 只拿同一 600s 总预算的剩余值'
    $PlannerMigrationDeadline = [pscustomobject]@{ ElapsedMilliseconds = 600001L }
    $deadlineRejected = $false
    try { [void](Get-PlannerMigrationRemainingTimeoutMilliseconds) } catch { $deadlineRejected = $true }
    Assert-True $deadlineRejected '总 deadline 耗尽后不得再启动任何 native child'
}
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
Assert-True ($nativeCalls.Count -eq 0) 'fast 批量路径不再用裸 native invocation'
$boundedCalls = @(if ($batchFunction) {
    @($batchFunction.Body.FindAll({
                param($node)
                $node -is [Management.Automation.Language.CommandAst] -and
                    $node.GetCommandName() -ceq 'Invoke-PandoraPlannerBoundedProcess'
            }, $true))
} else { @() })
Assert-True ($boundedCalls.Count -eq 0) '批量函数不绕过 ownership worker 直接启动 native child'
$ownedWorkerCalls = @(if ($batchFunction) {
    @($batchFunction.Body.FindAll({
                param($node)
                $node -is [Management.Automation.Language.CommandAst] -and
                    $node.GetCommandName() -ceq 'Invoke-PlannerOwnedMigrationProcess'
            }, $true))
} else { @() })
Assert-True ($ownedWorkerCalls.Count -eq 1) '批量未命中路径只调用一次持锁 ownership+mysql target seam'

Write-Host '[3] 外部 .ps1 的 exit 只结束 child，parent-after/finally 与 LASTEXITCODE 均可观测' -ForegroundColor Cyan
$externalFixture = Join-Path ([IO.Path]::GetTempPath()) ("pandora-external-exit-test-{0}" -f [guid]::NewGuid().ToString('N'))
try {
    New-Item -ItemType Directory -Force -Path $externalFixture | Out-Null
    $childScript = Join-Path $externalFixture 'child.ps1'
    $parentScript = Join-Path $externalFixture 'parent.ps1'
    [IO.File]::WriteAllText($childScript, @'
param([int]$Code)
$ErrorActionPreference = 'Stop'
exit $Code
'@, [Text.UTF8Encoding]::new($false))
    [IO.File]::WriteAllText($parentScript, @'
param([string]$Child, [int]$ChildCode, [string]$AfterMarker, [string]$FinallyMarker)
$ErrorActionPreference = 'Stop'
try {
    & $Child -Code $ChildCode
    $observed = [int]$LASTEXITCODE
    [IO.File]::WriteAllText($AfterMarker, "$observed")
} finally {
    [IO.File]::WriteAllText($FinallyMarker, 'finally')
}
exit 0
'@, [Text.UTF8Encoding]::new($false))
    foreach ($childCode in @(0, 7)) {
        $afterMarker = Join-Path $externalFixture "after-$childCode.txt"
        $finallyMarker = Join-Path $externalFixture "finally-$childCode.txt"
        $parent = Start-Process -FilePath (Join-Path $PSHOME 'pwsh.exe') -WindowStyle Hidden -PassThru -Wait `
            -ArgumentList @('-NoLogo', '-NoProfile', '-File', $parentScript,
                '-Child', $childScript, '-ChildCode', "$childCode",
                '-AfterMarker', $afterMarker, '-FinallyMarker', $finallyMarker)
        $observedCode = if (Test-Path -LiteralPath $afterMarker -PathType Leaf) {
            [IO.File]::ReadAllText($afterMarker)
        } else { '<missing>' }
        Assert-True ($parent.ExitCode -eq 0 -and $observedCode -ceq "$childCode" -and
            (Test-Path -LiteralPath $finallyMarker -PathType Leaf)) `
            "child exit $childCode 后 parent-after/finally 继续，LASTEXITCODE 精确保留"
    }
} finally {
    Remove-Item -LiteralPath $externalFixture -Recurse -Force -ErrorAction SilentlyContinue
}

if (-not (Test-Path -LiteralPath $FastLib -PathType Leaf)) { throw "[RED] 缺少 helper:$FastLib" }
. $FastLib

Write-Host '[4] SQL 按文件名排序并一次性完整拼接' -ForegroundColor Cyan
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

    Write-Host '[5] 收据必须同时绑定强哈希、MySQL 身份和实际库/表' -ForegroundColor Cyan
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
