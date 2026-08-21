# 策划 fast migration 的 MySQL 归属 worker 与工作区 DSN session 契约。
# 全部使用随机目录/假 worker；不连接数据库、不启停本机基础设施。
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ScriptsDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$ProjectRoot = (Resolve-Path (Join-Path $ScriptsDir '../..')).Path
$DevMigrate = Join-Path $ScriptsDir 'dev_migrate.ps1'
$Worker = Join-Path $ScriptsDir 'lib/planner_owned_migration_worker.ps1'
$StateLib = Join-Path $ScriptsDir 'lib/local_infra_state.ps1'
$BoundedLib = Join-Path $ScriptsDir 'lib/planner_bounded_process.ps1'
$script:Failures = [Collections.Generic.List[string]]::new()

function Assert-True([bool]$Condition, [string]$Message) {
    if ($Condition) { Write-Host "  [ok] $Message" -ForegroundColor Green }
    else { $script:Failures.Add($Message); Write-Host "  [FAIL] $Message" -ForegroundColor Red }
}

function Wait-Until {
    param([Parameter(Mandatory)][scriptblock]$Condition, [int]$TimeoutMilliseconds = 5000)
    $watch = [Diagnostics.Stopwatch]::StartNew()
    while ($watch.ElapsedMilliseconds -lt $TimeoutMilliseconds) {
        if (& $Condition) { return $true }
        Start-Sleep -Milliseconds 25
    }
    return [bool](& $Condition)
}

Write-Host '[1] child worker 只能做 canonical ownership 只读 probe；写 target 留在持锁父进程' -ForegroundColor Cyan
$devText = [IO.File]::ReadAllText($DevMigrate)
$workerExists = Test-Path -LiteralPath $Worker -PathType Leaf
Assert-True $workerExists '受限 ownership worker 已落地'
$workerText = if ($workerExists) { [IO.File]::ReadAllText($Worker) } else { '' }
Assert-True ($workerText -notmatch '(?i)\$Mode|\$MysqlClient|\$MysqlUser|\$TargetsFile|\$ExpectedTargets|MYSQL_PWD|Console\]::In|Invoke-WorkerNative|mysql\.exe|pandora-migrate|go-migrate|mysql-init') `
    'worker 无 stdin/password/target/任意 mode，不能成为事实 SkipLock writer'
Assert-True ($workerText -match '(?s)Get-PandoraLocalInfraPortState.*?Get-PandoraLocalMysqlOwnedProcess.*?exit\s+0' -and
    $workerText -notmatch '(?i)Invoke-Expression|^\s*&\s+' ) `
    'worker 只做 canonical PID/exe/my.ini/listener 归属复核后退出'
Assert-True ($devText -match 'function\s+Invoke-PlannerOwnedMigrationProcess') `
    'dev_migrate 只有一个持锁 ownership+target 调用 seam'
$ownedSeamText = [regex]::Match($devText,
    '(?s)function\s+Invoke-PlannerOwnedMigrationProcess\s*\{.*?(?=function\s+Get-PlannerMigrationSessionRoot)').Value
Assert-True ($ownedSeamText -match 'Assert-PandoraOrchestrationLockHeld' -and
    $ownedSeamText -match 'Invoke-PandoraPlannerBoundedProcess' -and
    $ownedSeamText -match 'planner_owned_migration_worker\.ps1' -and
    $ownedSeamText -match 'Get-PlannerMigrationRemainingTimeoutMilliseconds') `
    '父进程持有真实编排锁，并把同一 600s 剩余预算依次交给 probe 与 target Job'
Assert-True (([regex]::Matches($ownedSeamText, 'Invoke-PandoraPlannerBoundedProcess')).Count -ge 2 -and
    $ownedSeamText -match '(?s)Test-PlannerMigrationProcessResult.*?return\s+\$ownershipResult.*?switch\s*\(\$Mode\).*?Invoke-PandoraPlannerBoundedProcess') `
    '每个写 target 前先 fail-closed 消费独立 ownership probe，失败时 target 零启动'
foreach ($mode in @('mysql-planner-probe', 'mysql-show-databases', 'mysql-init-batch', 'pandora-migrate')) {
    $modeWired = if ($mode -in @('mysql-planner-probe', 'mysql-show-databases')) {
        $devText -match [regex]::Escape("'$mode'") -and
            $devText -match 'Invoke-PlannerOwnedMigrationProcess\s+-Mode\s+\$mode'
    } else {
        $devText -match ("Invoke-PlannerOwnedMigrationProcess[^\r\n]*-Mode\s+'?" + [regex]::Escape($mode))
    }
    Assert-True $modeWired `
        "fast 动作接到持锁父进程固定 allowlist:$mode"
}
Assert-True ($ownedSeamText -match '(?s)Environment\s*=\s*@\{\s*MYSQL_PWD\s*=\s*\$null\s*\}.*?Test-PlannerMigrationProcessResult.*?Environment\s*=\s*@\{\s*MYSQL_PWD\s*=\s*\$Password\s*\}') `
    '只读 probe 显式移除 ambient MYSQL_PWD，target 通过校验后才收到 child-only 密码'

Write-Host '[2] ownership canonical probe 卡死时由同一剩余 deadline 硬杀整棵 Job' -ForegroundColor Cyan
if ($workerExists) {
    . $BoundedLib
    $hangRoot = Join-Path ([IO.Path]::GetTempPath()) ("pandora-owned-worker-hang-{0}" -f [guid]::NewGuid().ToString('N'))
    try {
        $workerDir = Join-Path $hangRoot 'worker'
        $fakeProject = Join-Path $hangRoot 'project'
        New-Item -ItemType Directory -Force -Path $workerDir, $fakeProject | Out-Null
        Copy-Item -LiteralPath $Worker -Destination (Join-Path $workerDir 'planner_owned_migration_worker.ps1')
        [IO.File]::WriteAllText((Join-Path $workerDir 'local_infra_state.ps1'), @'
function Get-PandoraLocalInfraPortState([string]$ProjectRoot) {
    return [pscustomobject]@{ MysqlPort = 13307; MysqlProcessId = 4242 }
}
function Get-PandoraLocalMysqlOwnedProcess([string]$ProjectRoot, $State) {
    Start-Sleep -Seconds 30
    return [pscustomobject]@{ Id = 4242 }
}
'@, [Text.UTF8Encoding]::new($false))
        $hangWorker = Join-Path $workerDir 'planner_owned_migration_worker.ps1'
        $result = Invoke-PandoraPlannerBoundedProcess -Name 'ownership-hang-fixture' `
            -FilePath (Join-Path $PSHOME 'pwsh.exe') `
            -ArgumentList @('-NoLogo', '-NoProfile', '-File', $hangWorker,
                '-ProjectRoot', $fakeProject, '-MysqlPort', '13307') `
            -Environment @{ MYSQL_PWD = $null } -TimeoutMilliseconds 500 -CleanupTimeoutMilliseconds 3000
        Assert-True ($result.TimedOut -and $result.ExitCode -ne 0 -and $result.ElapsedMilliseconds -lt 2500) `
            'canonical probe 卡死不会越过有界 deadline'
        Assert-True (Wait-Until -TimeoutMilliseconds 3000 -Condition {
                $null -eq (Get-Process -Id $result.ProcessId -ErrorAction SilentlyContinue)
            }) 'timeout 后 exact ownership worker 已退出'
    } finally {
        if (Test-Path -LiteralPath $hangRoot -PathType Container) {
            Remove-Item -LiteralPath $hangRoot -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
}

Write-Host '[3] fast 明文 DSN 只进工作区锁内 session；普通路径保持旧 TEMP 生命周期' -ForegroundColor Cyan
$tokens = $null
$parseErrors = $null
$devAst = [Management.Automation.Language.Parser]::ParseFile($DevMigrate, [ref]$tokens, [ref]$parseErrors)
$sessionFunctionNames = @(
    'Get-PlannerMigrationSessionRoot',
    'Assert-PlannerMigrationPathAncestorsSafe',
    'Assert-PlannerMigrationSessionTreeSafe',
    'Assert-PlannerMigrationTrustedLeaf',
    'Assert-PlannerMigrationTargetsFileForExecution',
    'Clear-PlannerMigrationAbandonedSessions',
    'New-PlannerMigrationSessionDirectory',
    'Remove-PlannerMigrationSessionDirectory'
)
$sessionFunctions = @{}
foreach ($name in $sessionFunctionNames) {
    $node = $devAst.FindAll({
            param($candidate)
            $candidate -is [Management.Automation.Language.FunctionDefinitionAst] -and $candidate.Name -eq $name
        }, $true) | Select-Object -First 1
    if ($node) { $sessionFunctions[$name] = $node.Extent.Text }
}
Assert-True ($sessionFunctions.Count -eq $sessionFunctionNames.Count) 'workspace session 的安全 seam 完整存在'
Assert-True ($devText -match '(?s)Enter-PandoraOrchestrationLock.*?Clear-PlannerMigrationAbandonedSessions') `
    '只在取得同 workspace 编排锁后 sweep 崩溃残留'
Assert-True ($devText -match '(?s)Enter-PandoraOrchestrationLock.*?if\s*\(\$PlannerFastStart\)\s*\{\s*Clear-PlannerMigrationAbandonedSessions') `
    'workspace abandoned-session sweep 只改变 planner fast 路径'
Assert-True ($devText -match '(?s)if\s*\(\$PlannerFastStart\)\s*\{\s*\$tmpDir\s*=\s*New-PlannerMigrationSessionDirectory.*?\}\s*else\s*\{\s*\$tmpDir\s*=\s*Join-Path\s+\(\[System\.IO\.Path\]::GetTempPath\(\)\).*?pandora-dev-migrate-\{0\}-\{1\}.*?New-Item') `
    'fast 使用 workspace session，非 fast 保持旧 %TEMP% GUID 目录'
Assert-True ($devText -match '(?s)finally\s*\{\s*if\s*\(\$PlannerFastStart\)\s*\{\s*Remove-PlannerMigrationSessionDirectory' -and
    $devText -notmatch '(?s)if\s*\(\$PlannerFastStart\)\s*\{\s*Remove-PlannerMigrationSessionDirectory[^}]*SilentlyContinue') `
    '正常 finally 删除失败必须显式传播'
Assert-True ($devText -match '(?s)finally\s*\{\s*if\s*\(\$PlannerFastStart\).*?Remove-PlannerMigrationSessionDirectory.*?else.*?Remove-Item\s+-LiteralPath\s+\$tmpDir\s+-Recurse\s+-Force\s+-ErrorAction\s+SilentlyContinue') `
    '非 fast cleanup 保持旧的 TEMP 递归尽力删除行为'
Assert-True ((($sessionFunctions.Values -join "`n") -notmatch 'Remove-Item[^\r\n]*-Recurse')) `
    'workspace session 生产 cleanup 不拥有递归删除权限'

if ($sessionFunctions.Count -eq $sessionFunctionNames.Count) {
    . $StateLib
    foreach ($name in $sessionFunctionNames) { . ([scriptblock]::Create($sessionFunctions[$name])) }
    $fixtureRoot = Join-Path ([IO.Path]::GetTempPath()) ("pandora-migrate-session-test-{0}" -f [guid]::NewGuid().ToString('N'))
    $workspaceA = Join-Path $fixtureRoot 'workspace-a'
    $workspaceB = Join-Path $fixtureRoot 'workspace-b'
    $workspaceC = Join-Path $fixtureRoot 'workspace-c'
    $workspaceD = Join-Path $fixtureRoot 'workspace-d'
    $release = $null
    $holder = $null
    try {
        New-Item -ItemType Directory -Force -Path $workspaceA, $workspaceB, $workspaceC, $workspaceD | Out-Null

        # 模拟父 pwsh 在写下 DSN 后被硬杀：OS 释放 lock，但 session 留在工作区。
        $childScript = Join-Path $fixtureRoot 'hard-kill-owner.ps1'
        $marker = Join-Path $fixtureRoot 'hard-kill-session.txt'
        $functionSource = ($sessionFunctionNames | ForEach-Object { $sessionFunctions[$_] }) -join "`r`n"
        $childSource = @"
`$ErrorActionPreference = 'Stop'
. '$($StateLib.Replace("'", "''"))'
$functionSource
Enter-PandoraOrchestrationLock -ProjectRoot '$($workspaceA.Replace("'", "''"))' -Operation 'fixture owner'
`$session = New-PlannerMigrationSessionDirectory -ProjectRoot '$($workspaceA.Replace("'", "''"))'
[IO.File]::WriteAllText((Join-Path `$session 'pandora.dsn'), 'plaintext-fixture')
[IO.File]::WriteAllText('$($marker.Replace("'", "''"))', `$session)
Stop-Process -Id `$PID -Force
"@
        [IO.File]::WriteAllText($childScript, $childSource, [Text.UTF8Encoding]::new($false))
        $child = Start-Process -FilePath (Join-Path $PSHOME 'pwsh.exe') `
            -ArgumentList @('-NoLogo', '-NoProfile', '-File', $childScript) -PassThru -WindowStyle Hidden
        [void]$child.WaitForExit(10000)
        Assert-True (Wait-Until { Test-Path -LiteralPath $marker -PathType Leaf }) '硬杀 fixture 已写下 session 身份'
        $abandoned = if (Test-Path -LiteralPath $marker) { [IO.File]::ReadAllText($marker) } else { '' }
        Assert-True ($abandoned -and (Test-Path -LiteralPath $abandoned -PathType Container)) `
            '父 pwsh 硬杀后明文 DSN session 确实残留'
        Enter-PandoraOrchestrationLock -ProjectRoot $workspaceA -Operation 'fixture sweep'
        try {
            Clear-PlannerMigrationAbandonedSessions -ProjectRoot $workspaceA
            Assert-True (-not (Test-Path -LiteralPath $abandoned)) '下一轮持锁启动 sweep 掉硬杀残留'
            Assert-True (Test-Path -LiteralPath (Get-PlannerMigrationSessionRoot -ProjectRoot $workspaceA) -PathType Container) `
                'sweep 后空 session root 常驻，不把 root cleanup 失败误算成迁移失败'
        } finally { Exit-PandoraOrchestrationLock }

        # 活 owner 持锁期间，另一轮连 sweep 都进不去，不能删除 live session。
        $holderScript = Join-Path $fixtureRoot 'live-owner.ps1'
        $liveMarker = Join-Path $fixtureRoot 'live-session.txt'
        $release = Join-Path $fixtureRoot 'release-live-owner'
        $holderSource = @"
`$ErrorActionPreference = 'Stop'
. '$($StateLib.Replace("'", "''"))'
$functionSource
Enter-PandoraOrchestrationLock -ProjectRoot '$($workspaceA.Replace("'", "''"))' -Operation 'live fixture'
try {
  `$session = New-PlannerMigrationSessionDirectory -ProjectRoot '$($workspaceA.Replace("'", "''"))'
  [IO.File]::WriteAllText((Join-Path `$session 'live.dsn'), 'live')
  [IO.File]::WriteAllText('$($liveMarker.Replace("'", "''"))', `$session)
  `$until = [DateTime]::UtcNow.AddSeconds(20)
  while (-not (Test-Path -LiteralPath '$($release.Replace("'", "''"))') -and [DateTime]::UtcNow -lt `$until) { Start-Sleep -Milliseconds 25 }
} finally { Exit-PandoraOrchestrationLock }
"@
        [IO.File]::WriteAllText($holderScript, $holderSource, [Text.UTF8Encoding]::new($false))
        $holder = Start-Process -FilePath (Join-Path $PSHOME 'pwsh.exe') `
            -ArgumentList @('-NoLogo', '-NoProfile', '-File', $holderScript) -PassThru -WindowStyle Hidden
        Assert-True (Wait-Until -TimeoutMilliseconds 5000 { Test-Path -LiteralPath $liveMarker -PathType Leaf }) `
            'live owner 已持锁并创建 session'
        $liveSession = if (Test-Path -LiteralPath $liveMarker) { [IO.File]::ReadAllText($liveMarker) } else { '' }
        $lockRejected = $false
        try { Enter-PandoraOrchestrationLock -ProjectRoot $workspaceA -Operation 'competing sweep' } catch { $lockRejected = $true }
        Assert-True ($lockRejected -and $liveSession -and (Test-Path -LiteralPath $liveSession)) `
            '同 workspace live owner session 不会被竞争轮删除'
        [IO.File]::WriteAllText($release, 'release')
        [void]$holder.WaitForExit(10000)
        Enter-PandoraOrchestrationLock -ProjectRoot $workspaceA -Operation 'post-live cleanup'
        try { Clear-PlannerMigrationAbandonedSessions -ProjectRoot $workspaceA } finally { Exit-PandoraOrchestrationLock }

        # 跨 workspace 的路径即使名字合法也不能交给 A 的 cleanup。
        $foreignRoot = Get-PlannerMigrationSessionRoot -ProjectRoot $workspaceB
        New-Item -ItemType Directory -Force -Path $foreignRoot | Out-Null
        $foreignSession = Join-Path $foreignRoot ("{0}-{1}" -f $PID, [guid]::NewGuid().ToString('N'))
        New-Item -ItemType Directory -Force -Path $foreignSession | Out-Null
        [IO.File]::WriteAllText((Join-Path $foreignSession 'foreign.dsn'), 'foreign')
        Enter-PandoraOrchestrationLock -ProjectRoot $workspaceA -Operation 'cross workspace cleanup'
        try {
            $crossRejected = $false
            try { Remove-PlannerMigrationSessionDirectory -ProjectRoot $workspaceA -SessionPath $foreignSession } catch { $crossRejected = $true }
            Assert-True ($crossRejected -and (Test-Path -LiteralPath $foreignSession)) `
                'A workspace cleanup 拒绝并保留 B workspace session'
        } finally { Exit-PandoraOrchestrationLock }

        # reparse session 必须阻断整轮 sweep，绝不能沿链接删除目标。
        $sessionRootA = Get-PlannerMigrationSessionRoot -ProjectRoot $workspaceA
        New-Item -ItemType Directory -Force -Path $sessionRootA | Out-Null
        $junctionTarget = Join-Path $fixtureRoot 'junction-target'
        New-Item -ItemType Directory -Force -Path $junctionTarget | Out-Null
        [IO.File]::WriteAllText((Join-Path $junctionTarget 'sentinel.txt'), 'keep')
        $junction = Join-Path $sessionRootA ("{0}-{1}" -f $PID, [guid]::NewGuid().ToString('N'))
        New-Item -ItemType Junction -Path $junction -Target $junctionTarget | Out-Null
        Enter-PandoraOrchestrationLock -ProjectRoot $workspaceA -Operation 'reparse sweep'
        try {
            $reparseRejected = $false
            try { Clear-PlannerMigrationAbandonedSessions -ProjectRoot $workspaceA } catch { $reparseRejected = $true }
            Assert-True ($reparseRejected -and (Test-Path -LiteralPath (Join-Path $junctionTarget 'sentinel.txt'))) `
                'reparse session fail closed 且链接目标未被删除'
        } finally { Exit-PandoraOrchestrationLock }
        Remove-Item -LiteralPath $junction -Force

        $ancestorTarget = Join-Path $fixtureRoot 'ancestor-junction-target'
        New-Item -ItemType Directory -Force -Path $ancestorTarget | Out-Null
        [IO.File]::WriteAllText((Join-Path $ancestorTarget 'ancestor-sentinel.txt'), 'keep')
        $localInfraC = Join-Path $workspaceC 'run/localinfra'
        New-Item -ItemType Directory -Force -Path $localInfraC | Out-Null
        $tmpJunction = Join-Path $localInfraC 'tmp'
        New-Item -ItemType Junction -Path $tmpJunction -Target $ancestorTarget | Out-Null
        Enter-PandoraOrchestrationLock -ProjectRoot $workspaceC -Operation 'reparse ancestor fixture'
        try {
            $ancestorRejected = $false
            try { [void](New-PlannerMigrationSessionDirectory -ProjectRoot $workspaceC) } catch { $ancestorRejected = $true }
            Assert-True ($ancestorRejected -and
                (Test-Path -LiteralPath (Join-Path $ancestorTarget 'ancestor-sentinel.txt'))) `
                'session root 的 reparse ancestor 也 fail closed 且目标未被改动'
        } finally { Exit-PandoraOrchestrationLock }
        Remove-Item -LiteralPath $tmpJunction -Force

        # 扁平 session 不允许任何嵌套目录；整轮 fail closed，不能递归删除其中内容。
        Enter-PandoraOrchestrationLock -ProjectRoot $workspaceA -Operation 'nested session fixture'
        try {
            Clear-PlannerMigrationAbandonedSessions -ProjectRoot $workspaceA
            $nestedSession = New-PlannerMigrationSessionDirectory -ProjectRoot $workspaceA
            $nestedDir = Join-Path $nestedSession 'nested'
            New-Item -ItemType Directory -Path $nestedDir | Out-Null
            $nestedSentinel = Join-Path $nestedDir 'sentinel.dsn'
            [IO.File]::WriteAllText($nestedSentinel, 'keep')
            $nestedRejected = $false
            try { Clear-PlannerMigrationAbandonedSessions -ProjectRoot $workspaceA } catch { $nestedRejected = $true }
            Assert-True ($nestedRejected -and (Test-Path -LiteralPath $nestedSentinel -PathType Leaf)) `
                '嵌套目录 fail closed，生产 sweep 未递归删除其内容'
            Remove-Item -LiteralPath $nestedDir -Recurse -Force
            Remove-PlannerMigrationSessionDirectory -ProjectRoot $workspaceA -SessionPath $nestedSession
        } finally { Exit-PandoraOrchestrationLock }

        # migrator 前重新解析 manifest 与明文 DSN：位置、reparse、user/endpoint/port/database 全绑定。
        Enter-PandoraOrchestrationLock -ProjectRoot $workspaceA -Operation 'targets validator fixture'
        try {
            Clear-PlannerMigrationAbandonedSessions -ProjectRoot $workspaceA
            $validatedSession = New-PlannerMigrationSessionDirectory -ProjectRoot $workspaceA
            $validatedTargets = Join-Path $validatedSession 'targets.json'
            $validatedDsn = Join-Path $validatedSession 'pandora_safe.dsn'
            function Write-ValidatorManifest {
                param(
                    [string]$Dsn,
                    [string]$Database = 'pandora_safe',
                    [string]$MigrationSet = 'pandora_safe',
                    [string]$DsnFile = 'pandora_safe.dsn'
                )
                [IO.File]::WriteAllText($validatedDsn, $Dsn, [Text.Encoding]::ASCII)
                $json = @{ targets = @(@{
                            name = 'pandora-safe-dev'; migration_set = $MigrationSet
                            database = $Database; dsn_file = $DsnFile
                        }) } | ConvertTo-Json -Depth 5
                [IO.File]::WriteAllText($validatedTargets, $json, [Text.UTF8Encoding]::new($false))
            }
            $validDsn = 'pandora:fixture-secret@tcp(127.0.0.1:13307)/pandora_safe?parseTime=true&loc=UTC&multiStatements=true'
            Write-ValidatorManifest -Dsn $validDsn
            $validatedPath = Assert-PlannerMigrationTargetsFileForExecution -ProjectRoot $workspaceA `
                -TargetsFile $validatedTargets -MysqlPort 13307 -MysqlUser pandora -MysqlPassword 'fixture-secret'
            Assert-True ([string]::Equals($validatedPath.TargetsFile, $validatedTargets,
                    [StringComparison]::OrdinalIgnoreCase) -and
                $validatedPath.ExpectedTargets -ceq 'pandora-safe-dev:pandora_safe:pandora_safe') `
                '合法扁平 manifest 与本机 DSN 通过最终执行前 validator'

            $dsnMutants = @(
                @{ Label = '远端 host'; Dsn = 'pandora:fixture-secret@tcp(10.1.2.3:13307)/pandora_safe?parseTime=true&loc=UTC&multiStatements=true' },
                @{ Label = '错误 port'; Dsn = 'pandora:fixture-secret@tcp(127.0.0.1:3307)/pandora_safe?parseTime=true&loc=UTC&multiStatements=true' },
                @{ Label = '错误 database'; Dsn = 'pandora:fixture-secret@tcp(127.0.0.1:13307)/pandora_other?parseTime=true&loc=UTC&multiStatements=true' },
                @{ Label = '错误 user'; Dsn = 'root:fixture-secret@tcp(127.0.0.1:13307)/pandora_safe?parseTime=true&loc=UTC&multiStatements=true' },
                @{ Label = '错误 password'; Dsn = 'pandora:not-the-secret@tcp(127.0.0.1:13307)/pandora_safe?parseTime=true&loc=UTC&multiStatements=true' }
            )
            foreach ($mutant in $dsnMutants) {
                Write-ValidatorManifest -Dsn $mutant.Dsn
                $mutantError = $null
                try {
                    [void](Assert-PlannerMigrationTargetsFileForExecution -ProjectRoot $workspaceA `
                            -TargetsFile $validatedTargets -MysqlPort 13307 -MysqlUser pandora `
                            -MysqlPassword 'fixture-secret')
                } catch { $mutantError = $_ }
                Assert-True ($null -ne $mutantError -and "$mutantError" -notmatch 'fixture-secret|not-the-secret') `
                    "$($mutant.Label) DSN fail closed 且诊断不泄露密码"
            }

            Write-ValidatorManifest -Dsn $validDsn -MigrationSet 'pandora_other'
            $setMismatchRejected = $false
            try {
                [void](Assert-PlannerMigrationTargetsFileForExecution -ProjectRoot $workspaceA `
                        -TargetsFile $validatedTargets -MysqlPort 13307 -MysqlUser pandora `
                        -MysqlPassword 'fixture-secret')
            } catch { $setMismatchRejected = $true }
            Assert-True $setMismatchRejected 'manifest database/migration_set 不一致时 fail closed'

            # targets.json/DSN 任一 reparse，或 dsn_file 越界/嵌套，都不能到 migrator。
            Write-ValidatorManifest -Dsn $validDsn
            $outsideTargets = Join-Path $fixtureRoot 'outside-targets.json'
            Copy-Item -LiteralPath $validatedTargets -Destination $outsideTargets
            Remove-Item -LiteralPath $validatedTargets -Force
            New-Item -ItemType SymbolicLink -Path $validatedTargets -Target $outsideTargets | Out-Null
            $targetsLinkRejected = $false
            try {
                [void](Assert-PlannerMigrationTargetsFileForExecution -ProjectRoot $workspaceA `
                        -TargetsFile $validatedTargets -MysqlPort 13307 -MysqlUser pandora `
                        -MysqlPassword 'fixture-secret')
            } catch { $targetsLinkRejected = $true }
            Assert-True $targetsLinkRejected 'reparse targets.json 在 migrator 前 fail closed'
            Remove-Item -LiteralPath $validatedTargets -Force

            [IO.File]::WriteAllText($validatedTargets,
                (@{ targets = @(@{ name = 'bad'; migration_set = 'pandora_safe'; database = 'pandora_safe'; dsn_file = '..\\outside.dsn' }) } |
                    ConvertTo-Json -Depth 5), [Text.UTF8Encoding]::new($false))
            $outsideDsnRejected = $false
            try {
                [void](Assert-PlannerMigrationTargetsFileForExecution -ProjectRoot $workspaceA `
                        -TargetsFile $validatedTargets -MysqlPort 13307 -MysqlUser pandora `
                        -MysqlPassword 'fixture-secret')
            } catch { $outsideDsnRejected = $true }
            Assert-True $outsideDsnRejected '越界 dsn_file 在 migrator 前 fail closed'

            Remove-Item -LiteralPath $validatedDsn -Force
            $outsideDsnFile = Join-Path $fixtureRoot 'outside-safe.dsn'
            [IO.File]::WriteAllText($outsideDsnFile, $validDsn, [Text.Encoding]::ASCII)
            New-Item -ItemType SymbolicLink -Path $validatedDsn -Target $outsideDsnFile | Out-Null
            [IO.File]::WriteAllText($validatedTargets,
                (@{ targets = @(@{ name = 'safe'; migration_set = 'pandora_safe'; database = 'pandora_safe'; dsn_file = 'pandora_safe.dsn' }) } |
                    ConvertTo-Json -Depth 5), [Text.UTF8Encoding]::new($false))
            $dsnLinkRejected = $false
            try {
                [void](Assert-PlannerMigrationTargetsFileForExecution -ProjectRoot $workspaceA `
                        -TargetsFile $validatedTargets -MysqlPort 13307 -MysqlUser pandora `
                        -MysqlPassword 'fixture-secret')
            } catch { $dsnLinkRejected = $true }
            Assert-True $dsnLinkRejected 'reparse DSN 在 migrator 前 fail closed'
            Remove-Item -LiteralPath $validatedDsn -Force
            [IO.File]::WriteAllText($validatedDsn, $validDsn, [Text.Encoding]::ASCII)
            Remove-PlannerMigrationSessionDirectory -ProjectRoot $workspaceA -SessionPath $validatedSession
        } finally { Exit-PandoraOrchestrationLock }

        # executable 的信任链必须从 ProjectRoot 开始，不能只检查 junction 下的 leaf。
        $externalDist = Join-Path $fixtureRoot 'external-dist'
        $externalArtifacts = Join-Path $fixtureRoot 'external-artifacts'
        New-Item -ItemType Directory -Force -Path (Join-Path $externalDist 'mysql/bin'), `
            (Join-Path $externalArtifacts 'windows/bin'), (Join-Path $workspaceD 'run/localinfra') | Out-Null
        $externalMysql = Join-Path $externalDist 'mysql/bin/mysql.exe'
        $externalMigrate = Join-Path $externalArtifacts 'windows/bin/pandora-migrate.exe'
        Copy-Item -LiteralPath (Join-Path $PSHOME 'pwsh.exe') -Destination $externalMysql
        Copy-Item -LiteralPath (Join-Path $PSHOME 'pwsh.exe') -Destination $externalMigrate
        $distJunction = Join-Path $workspaceD 'run/localinfra/dist'
        $artifactsJunction = Join-Path $workspaceD 'run/artifacts'
        New-Item -ItemType Junction -Path $distJunction -Target $externalDist | Out-Null
        New-Item -ItemType Junction -Path $artifactsJunction -Target $externalArtifacts | Out-Null
        Enter-PandoraOrchestrationLock -ProjectRoot $workspaceD -Operation 'executable junction fixture'
        try {
            $mysqlJunctionRejected = $false
            try {
                [void](Assert-PlannerMigrationTrustedLeaf -ProjectRoot $workspaceD `
                        -Path (Join-Path $distJunction 'mysql/bin/mysql.exe') `
                        -TrustedRoot (Join-Path $workspaceD 'run/localinfra/dist/mysql') -ExpectedLeaf mysql.exe)
            } catch { $mysqlJunctionRejected = $true }
            Assert-True ($mysqlJunctionRejected -and (Test-Path -LiteralPath $externalMysql -PathType Leaf)) `
                'dist ancestor junction 在传 MYSQL_PWD/启动 mysql 前 fail closed'

            $artifactJunctionRejected = $false
            try {
                [void](Assert-PlannerMigrationTrustedLeaf -ProjectRoot $workspaceD `
                        -Path (Join-Path $artifactsJunction 'windows/bin/pandora-migrate.exe') `
                        -TrustedRoot (Join-Path $workspaceD 'run/artifacts/windows/bin') `
                        -ExpectedLeaf pandora-migrate.exe)
            } catch { $artifactJunctionRejected = $true }
            Assert-True ($artifactJunctionRejected -and (Test-Path -LiteralPath $externalMigrate -PathType Leaf)) `
                'artifacts ancestor junction 在启动 migrator 前 fail closed'
        } finally { Exit-PandoraOrchestrationLock }

        # 直接运行父 seam：probe 失败、exe junction、非法 DSN 都只能产生一个只读 probe 调用，
        # 绝不能启动第二个 target，也不能把密码放进 probe environment。
        $ownedFunctionNode = $devAst.FindAll({
                param($candidate)
                $candidate -is [Management.Automation.Language.FunctionDefinitionAst] -and
                    $candidate.Name -eq 'Invoke-PlannerOwnedMigrationProcess'
            }, $true) | Select-Object -First 1
        $resultGuardNode = $devAst.FindAll({
                param($candidate)
                $candidate -is [Management.Automation.Language.FunctionDefinitionAst] -and
                    $candidate.Name -eq 'Test-PlannerMigrationProcessResult'
            }, $true) | Select-Object -First 1
        Assert-True ($null -ne $ownedFunctionNode -and $null -ne $resultGuardNode) `
            '父 ownership+target seam 可由 AST 精确执行 fixture'
        if ($ownedFunctionNode -and $resultGuardNode) {
            . ([scriptblock]::Create($resultGuardNode.Extent.Text))
            . ([scriptblock]::Create($ownedFunctionNode.Extent.Text))
            $script:FixtureDeadlineExhausted = $false
            function Get-PlannerMigrationRemainingTimeoutMilliseconds {
                if ($script:FixtureDeadlineExhausted) { throw 'fixture deadline exhausted' }
                return 5000
            }
            $script:FakeBoundedInvocations = [Collections.Generic.List[object]]::new()
            $script:FakeBoundedResults = [Collections.Generic.Queue[object]]::new()
            function New-FakeProcessResult {
                param([int]$ExitCode = 0, [string]$Failure = '')
                return [pscustomobject]@{
                    ExitCode = $ExitCode; ProcessExitCode = $ExitCode; TimedOut = $false
                    DrainCompleted = $true; StandardInputCompleted = $true
                    StandardOutput = ''; StandardError = ''
                    StandardOutputTruncated = $false; StandardErrorTruncated = $false
                    Failure = $Failure
                }
            }
            function Invoke-PandoraPlannerBoundedProcess {
                param(
                    [string]$Name, [string]$FilePath, [string[]]$ArgumentList,
                    [string]$WorkingDirectory, [Collections.IDictionary]$Environment,
                    [AllowNull()][string]$StandardInput, [int]$TimeoutMilliseconds
                )
                $environmentCopy = @{}
                if ($null -ne $Environment) {
                    foreach ($key in $Environment.Keys) { $environmentCopy[$key] = $Environment[$key] }
                }
                $script:FakeBoundedInvocations.Add([pscustomobject]@{
                        Name = $Name; FilePath = $FilePath; Environment = $environmentCopy
                    })
                if ($script:FakeBoundedResults.Count -eq 0) {
                    throw 'fixture 检测到意外 target 启动。'
                }
                return $script:FakeBoundedResults.Dequeue()
            }

            $fixtureScriptDir = Join-Path $workspaceD 'tools/scripts'
            New-Item -ItemType Directory -Force -Path (Join-Path $fixtureScriptDir 'lib') | Out-Null
            Copy-Item -LiteralPath $Worker -Destination (Join-Path $fixtureScriptDir 'lib/planner_owned_migration_worker.ps1')
            $PlannerFastStart = $true
            $ProjectRoot = $workspaceD
            $ScriptDir = $fixtureScriptDir
            $MysqlPort = 13307
            $MysqlClient = Join-Path $distJunction 'mysql/bin/mysql.exe'
            $MysqlUser = 'pandora'

            Enter-PandoraOrchestrationLock -ProjectRoot $workspaceD -Operation 'parent seam fixture'
            try {
                $script:FakeBoundedInvocations.Clear()
                $script:FakeBoundedResults.Clear()
                $probeFailure = New-FakeProcessResult -ExitCode -1 -Failure 'ownership failed'
                $script:FakeBoundedResults.Enqueue($probeFailure)
                $returnedFailure = Invoke-PlannerOwnedMigrationProcess -Mode mysql-show-databases `
                    -Password 'fixture-secret'
                Assert-True ($returnedFailure -eq $probeFailure -and $script:FakeBoundedInvocations.Count -eq 1) `
                    'ownership probe 失败直接返回，target 零启动'
                $probeEnvironment = $script:FakeBoundedInvocations[0].Environment
                Assert-True ($probeEnvironment.ContainsKey('MYSQL_PWD') -and $null -eq $probeEnvironment['MYSQL_PWD']) `
                    'ownership probe 只删除 ambient MYSQL_PWD，未收到真实密码'

                $script:FakeBoundedInvocations.Clear()
                $script:FakeBoundedResults.Clear()
                $script:FakeBoundedResults.Enqueue((New-FakeProcessResult))
                $junctionTargetRejected = $false
                try {
                    [void](Invoke-PlannerOwnedMigrationProcess -Mode mysql-show-databases `
                            -Password 'fixture-secret')
                } catch { $junctionTargetRejected = $true }
                Assert-True ($junctionTargetRejected -and $script:FakeBoundedInvocations.Count -eq 1 -and
                    $null -eq $script:FakeBoundedInvocations[0].Environment['MYSQL_PWD']) `
                    'mysql ancestor junction 在传密码前拒绝，target 零启动'

                $invalidSession = New-PlannerMigrationSessionDirectory -ProjectRoot $workspaceD
                $invalidDsn = Join-Path $invalidSession 'pandora_safe.dsn'
                $invalidTargets = Join-Path $invalidSession 'targets.json'
                [IO.File]::WriteAllText($invalidDsn,
                    'pandora:fixture-secret@tcp(10.1.2.3:13307)/pandora_safe?parseTime=true&loc=UTC&multiStatements=true',
                    [Text.Encoding]::ASCII)
                [IO.File]::WriteAllText($invalidTargets,
                    (@{ targets = @(@{ name = 'safe'; migration_set = 'pandora_safe'; database = 'pandora_safe'; dsn_file = 'pandora_safe.dsn' }) } |
                        ConvertTo-Json -Depth 5), [Text.UTF8Encoding]::new($false))
                $script:FakeBoundedInvocations.Clear()
                $script:FakeBoundedResults.Clear()
                $script:FakeBoundedResults.Enqueue((New-FakeProcessResult))
                $remoteTargetRejected = $false
                try {
                    [void](Invoke-PlannerOwnedMigrationProcess -Mode pandora-migrate `
                            -Password 'fixture-secret' -TargetsFile $invalidTargets `
                            -ExpectedTargets 'safe:pandora_safe:pandora_safe')
                } catch { $remoteTargetRejected = $true }
                Assert-True ($remoteTargetRejected -and $script:FakeBoundedInvocations.Count -eq 1) `
                    '远端 DSN 在 ownership 后、migrator 启动前拒绝，target 零启动'
                [IO.File]::WriteAllText($invalidDsn,
                    'pandora:fixture-secret@tcp(127.0.0.1:13307)/pandora_safe?parseTime=true&loc=UTC&multiStatements=true',
                    [Text.Encoding]::ASCII)
                Remove-PlannerMigrationSessionDirectory -ProjectRoot $workspaceD -SessionPath $invalidSession

                # 用受限 seam 人工让 executable validator 耗尽总 deadline。remaining 必须在
                # validator 之后现算；若提前缓存，fixture 会观察到第二次 target launch。
                function Assert-PlannerMigrationTrustedLeaf {
                    param([string]$ProjectRoot, [string]$Path, [string]$TrustedRoot, [string]$ExpectedLeaf)
                    if ($ExpectedLeaf -ceq 'mysql.exe') { $script:FixtureDeadlineExhausted = $true }
                    return $Path
                }
                $MysqlClient = Join-Path $workspaceD 'synthetic/mysql.exe'
                $script:FixtureDeadlineExhausted = $false
                $script:FakeBoundedInvocations.Clear()
                $script:FakeBoundedResults.Clear()
                $script:FakeBoundedResults.Enqueue((New-FakeProcessResult))
                $script:FakeBoundedResults.Enqueue((New-FakeProcessResult))
                $deadlineRejected = $false
                try {
                    [void](Invoke-PlannerOwnedMigrationProcess -Mode mysql-show-databases `
                            -Password 'fixture-secret')
                } catch { $deadlineRejected = $true }
                Assert-True ($deadlineRejected -and $script:FakeBoundedInvocations.Count -eq 1 -and
                    $null -eq $script:FakeBoundedInvocations[0].Environment['MYSQL_PWD']) `
                    'validator 耗尽总预算后 target/密码均零传递'
                . ([scriptblock]::Create($sessionFunctions['Assert-PlannerMigrationTrustedLeaf']))
                $script:FixtureDeadlineExhausted = $false
            } finally { Exit-PandoraOrchestrationLock }
        }
        Remove-Item -LiteralPath $distJunction -Force
        Remove-Item -LiteralPath $artifactsJunction -Force

        # 正常 finally 若遇独占文件，cleanup 必须抛错；释放后才可精确删除。
        Enter-PandoraOrchestrationLock -ProjectRoot $workspaceA -Operation 'cleanup failure fixture'
        try {
            Clear-PlannerMigrationAbandonedSessions -ProjectRoot $workspaceA
            $lockedSession = New-PlannerMigrationSessionDirectory -ProjectRoot $workspaceA
            $lockedFile = Join-Path $lockedSession 'locked.dsn'
            [IO.File]::WriteAllText($lockedFile, 'locked')
            $stream = [IO.File]::Open($lockedFile, [IO.FileMode]::Open, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
            try {
                $cleanupRejected = $false
                try { Remove-PlannerMigrationSessionDirectory -ProjectRoot $workspaceA -SessionPath $lockedSession } catch { $cleanupRejected = $true }
                Assert-True ($cleanupRejected -and (Test-Path -LiteralPath $lockedSession)) `
                    'normal cleanup 失败显式传播且不谎称已删除'
            } finally { $stream.Dispose() }
            Remove-PlannerMigrationSessionDirectory -ProjectRoot $workspaceA -SessionPath $lockedSession
        } finally { Exit-PandoraOrchestrationLock }
    } finally {
        if (Test-Path -LiteralPath $release -PathType Leaf) { Remove-Item -LiteralPath $release -Force -ErrorAction SilentlyContinue }
        if ($holder -and -not $holder.HasExited) { $holder.Kill($true); [void]$holder.WaitForExit(5000) }
        if (Test-Path -LiteralPath $fixtureRoot -PathType Container) {
            Remove-Item -LiteralPath $fixtureRoot -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
}

if ($script:Failures.Count -gt 0) {
    throw "planner ownership/session 契约失败($($script:Failures.Count)):`n - $($script:Failures -join "`n - ")"
}
Write-Host '[PASS] planner ownership worker 与 migration session 契约通过。' -ForegroundColor Green
