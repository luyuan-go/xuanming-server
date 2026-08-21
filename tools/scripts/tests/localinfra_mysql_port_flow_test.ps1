# 免 Docker MySQL 动态端口必须贯穿迁移、业务服务配置和 DS 快速重启。
# 全部使用临时目录/静态 AST，不启动真实组件。

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

$scriptsDir = Split-Path -Parent $PSScriptRoot
$RepoRoot = (Resolve-Path (Join-Path $scriptsDir '../..')).Path

Write-Host '[1] 端口状态文件原子读写'
$stateHelperPath = Join-Path $scriptsDir 'lib/local_infra_state.ps1'
. $stateHelperPath
$sandbox = Join-Path ([System.IO.Path]::GetTempPath()) ("pandora-localinfra-port-flow-{0}" -f [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Force -Path $sandbox | Out-Null
try {
    Assert-True ((Get-PandoraLocalMysqlPort $sandbox) -eq 0) '无状态时返回 0，不猜 3307'
    $fakeExe = Join-Path $sandbox 'run/localinfra/dist/mysql/mysql-test/bin/mysqld.exe'
    $fakeIni = Join-Path $sandbox 'run/localinfra/cfg/my.ini'
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $fakeExe), (Split-Path -Parent $fakeIni) | Out-Null
    New-Item -ItemType File -Force -Path $fakeExe, $fakeIni | Out-Null
    $statePath = Set-PandoraLocalInfraPortState -ProjectRoot $sandbox -MysqlPort 13308 `
        -MysqlProcessId 111 -MysqlExecutable $fakeExe -MysqlDefaultsFile $fakeIni
    Assert-True (Test-Path -LiteralPath $statePath) '成功验证后的端口状态已落盘'
    Assert-True ((Get-PandoraLocalMysqlPort $sandbox -Required) -eq 13308) '所有调用方读到同一个端口'
    Set-PandoraLocalInfraPortState -ProjectRoot $sandbox -MysqlPort 13309 `
        -MysqlProcessId 222 -MysqlExecutable $fakeExe -MysqlDefaultsFile $fakeIni | Out-Null
    $replaced = Get-PandoraLocalInfraPortState $sandbox
    Assert-True ($replaced.MysqlPort -eq 13309 -and $replaced.MysqlProcessId -eq 222) '状态文件可原子覆盖到最新端口与 PID'
    Assert-True (@(Get-ChildItem -LiteralPath (Split-Path -Parent $statePath) -Filter '*.tmp').Count -eq 0) '原子替换没有遗留临时文件'

    Set-Content -LiteralPath $statePath -Value '{broken-json' -Encoding utf8NoBOM
    $brokenFailed = $false
    try { Get-PandoraLocalMysqlPort $sandbox -Required | Out-Null } catch { $brokenFailed = $true }
    Assert-True $brokenFailed '损坏 JSON fail closed，不返回猜测端口'

    [ordered]@{
        schema_version = 1; mysql_port = 60000; mysql_process_id = 222
        mysql_executable = $fakeExe; mysql_defaults_file = $fakeIni
    } | ConvertTo-Json | Set-Content -LiteralPath $statePath -Encoding utf8NoBOM
    $rangeFailed = $false
    try { Get-PandoraLocalMysqlPort $sandbox -Required | Out-Null } catch { $rangeFailed = $true }
    Assert-True $rangeFailed '越界端口状态 fail closed'

    foreach ($badSchema in @(1.4, 2)) {
        [ordered]@{
            schema_version = $badSchema; mysql_port = 13309; mysql_process_id = 222
            mysql_executable = $fakeExe; mysql_defaults_file = $fakeIni
        } | ConvertTo-Json | Set-Content -LiteralPath $statePath -Encoding utf8NoBOM
        $schemaFailed = $false
        try { Get-PandoraLocalMysqlPort $sandbox -Required | Out-Null } catch { $schemaFailed = $true }
        Assert-True $schemaFailed "MySQL 身份状态拒绝非精确 v1 schema:$badSchema"
    }

    Set-PandoraServiceAppliedMysqlPort -ProjectRoot $sandbox -Mode docker -MysqlPort 3307 -SocialOnMysql $false | Out-Null
    Set-PandoraServiceAppliedMysqlPort -ProjectRoot $sandbox -Mode nodocker -MysqlPort 13309 -SocialOnMysql $true | Out-Null
    $appliedState = Get-PandoraServiceAppliedMysqlState $sandbox
    Assert-True ($appliedState.Mode -eq 'nodocker' -and $appliedState.MysqlPort -eq 13309 -and $appliedState.SocialOnMysql) '业务服务已应用模式、端口与社交库配置单独记录且可覆盖'
    Clear-PandoraServiceAppliedMysqlState $sandbox
    Assert-True (-not (Get-PandoraServiceAppliedMysqlState $sandbox)) '部分启动可使旧的全服务运行态 marker 失效'
    foreach ($badSchema in @(1.6, 2.4, 3)) {
        [ordered]@{ schema_version = $badSchema; mode = 'nodocker'; mysql_port = 13309; social_on_mysql = $true } |
            ConvertTo-Json | Set-Content -LiteralPath (Get-PandoraServiceAppliedMysqlStatePath $sandbox) -Encoding utf8NoBOM
        $schemaFailed = $false
        try { Get-PandoraServiceAppliedMysqlState $sandbox | Out-Null } catch { $schemaFailed = $true }
        Assert-True $schemaFailed "业务运行态拒绝非精确 v2 schema:$badSchema"
    }
    foreach ($malformedLegacy in @(
        [ordered]@{ schema_version = 1 },
        [ordered]@{ schema_version = 1; mode = 'unknown'; mysql_port = 60000 }
    )) {
        $malformedLegacy | ConvertTo-Json |
            Set-Content -LiteralPath (Get-PandoraServiceAppliedMysqlStatePath $sandbox) -Encoding utf8NoBOM
        $legacyFailed = $false
        try { Get-PandoraServiceAppliedMysqlState $sandbox | Out-Null } catch { $legacyFailed = $true }
        Assert-True $legacyFailed '旧 v1 marker 只兼容合法 mode+port，不吞字段损坏'
    }
    [ordered]@{ schema_version = 1; mode = 'nodocker'; mysql_port = 13309 } | ConvertTo-Json |
        Set-Content -LiteralPath (Get-PandoraServiceAppliedMysqlStatePath $sandbox) -Encoding utf8NoBOM
    Assert-True (-not (Get-PandoraServiceAppliedMysqlState $sandbox)) '缺 social profile 的旧 v1 marker 视为未登记并强制刷新'

    $lockDepth = 0
    $blockedAtDepth2 = $false
    $blockedAtDepth1 = $false
    $blockedOtherRunspace = $false
    $reopenedAfterRelease = $false
    try {
        Enter-PandoraOrchestrationLock -ProjectRoot $sandbox -Operation '外层测试'; $lockDepth++
        Enter-PandoraOrchestrationLock -ProjectRoot $sandbox -Operation '内层测试'; $lockDepth++
        $other = [Management.Automation.PowerShell]::Create()
        try {
            $probeScript = @'
param($HelperPath, $Root)
. $HelperPath
try {
    Enter-PandoraOrchestrationLock -ProjectRoot $Root -Operation '并发 runspace 测试'
    Exit-PandoraOrchestrationLock
    'UNEXPECTED'
} catch { 'BLOCKED' }
'@
            $otherResult = @($other.AddScript($probeScript).AddArgument($stateHelperPath).AddArgument($sandbox).Invoke())
            $blockedOtherRunspace = $otherResult -contains 'BLOCKED'
        } finally { $other.Dispose() }
        try {
            $probe = [IO.File]::Open((Get-PandoraOrchestrationLockPath $sandbox), [IO.FileMode]::Open, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
            $probe.Dispose()
        } catch { $blockedAtDepth2 = $true }
        Exit-PandoraOrchestrationLock; $lockDepth--
        try {
            $probe = [IO.File]::Open((Get-PandoraOrchestrationLockPath $sandbox), [IO.FileMode]::Open, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
            $probe.Dispose()
        } catch { $blockedAtDepth1 = $true }
        Exit-PandoraOrchestrationLock; $lockDepth--
        $probe = [IO.File]::Open((Get-PandoraOrchestrationLockPath $sandbox), [IO.FileMode]::Open, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
        $probe.Dispose(); $reopenedAfterRelease = $true
    } finally {
        while ($lockDepth -gt 0) { Exit-PandoraOrchestrationLock; $lockDepth-- }
    }
    Assert-True ($blockedAtDepth2 -and $blockedAtDepth1 -and $blockedOtherRunspace -and $reopenedAfterRelease) '跨脚本编排锁可重入、拦同进程其它 runspace，且只在最外层退出后释放'

    $iniWithSpace = 'C:\Pandora Test\cfg\my.ini'
    Assert-True ((Get-PandoraDefaultsFileArgument "mysqld `"--defaults-file=$iniWithSpace`"") -eq $iniWithSpace) '共享 parser 接受整个 defaults-file token 带引号'
    Assert-True ((Get-PandoraDefaultsFileArgument "mysqld --defaults-file=`"$iniWithSpace`"") -eq $iniWithSpace) '共享 parser 接受 defaults-file 值带引号'
    Assert-True ((Get-PandoraDefaultsFileArgument 'mysqld --defaults-file=C:\Pandora\cfg\my.ini') -eq 'C:\Pandora\cfg\my.ini') '共享 parser 接受无空格裸 token'
    Assert-True ((Get-PandoraDefaultsFileArgument 'mysqld --defaults-file=C:\Pandora\cfg\my.ini.evil') -ne 'C:\Pandora\cfg\my.ini') '共享 parser 不把 my.ini.evil 截成合法路径'

    $ownedState = [pscustomobject]@{
        MysqlPort = 13309; MysqlProcessId = 222; MysqlExecutable = $fakeExe; MysqlDefaultsFile = $fakeIni
    }
    $script:VerifierProc = [pscustomobject]@{ Id = 222; Path = $fakeExe }
    $script:VerifierCommandLine = "mysqld --defaults-file=`"$fakeIni`""
    $script:VerifierListenerPid = 222
    $script:VerifierListenerUnknown = $false
    function Get-Process { [CmdletBinding()] param([int]$Id); return $script:VerifierProc }
    function Get-PandoraProcessCommandLine([int]$ProcessId) { return $script:VerifierCommandLine }
    function Get-PandoraTcpListenerProcessIds([int]$Port) {
        if ($script:VerifierListenerUnknown) { throw 'PANDORA_LISTENER_STATE_UNKNOWN' }
        return @($script:VerifierListenerPid)
    }
    Assert-True ((Get-PandoraLocalMysqlOwnedProcess $sandbox $ownedState).Id -eq 222) '共享 verifier 要求 PID/exe/my.ini/listener 四项同时吻合'
    $script:VerifierListenerPid = 999
    Assert-True (-not (Get-PandoraLocalMysqlOwnedProcess $sandbox $ownedState)) '端口被其它 PID 接管时共享 verifier fail closed'
    $script:VerifierListenerPid = 222
    $script:VerifierListenerUnknown = $true
    Assert-True (-not (Get-PandoraLocalMysqlOwnedProcess $sandbox $ownedState)) 'listener 查询未知时共享 verifier fail closed'
    $script:VerifierListenerUnknown = $false
    $script:VerifierCommandLine = "mysqld --defaults-file=`"$($fakeIni).evil`""
    Assert-True (-not (Get-PandoraLocalMysqlOwnedProcess $sandbox $ownedState)) '共享 verifier 不接受 my.ini.evil 冒充'
    Remove-Item function:Get-Process, function:Get-PandoraProcessCommandLine, function:Get-PandoraTcpListenerProcessIds -Force
} finally {
    Remove-Item -LiteralPath $sandbox -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Host '[1b] 候选端口只认真实 bind，不复用占用者'
$infraPath = Join-Path $scriptsDir 'local_infra.ps1'
$infraErrors = $null
$infraAst = [System.Management.Automation.Language.Parser]::ParseFile($infraPath, [ref]$null, [ref]$infraErrors)
if ($infraErrors -and $infraErrors.Count -gt 0) { throw "local_infra.ps1 语法错误:$($infraErrors[0].Message)" }
$resolveFn = @($infraAst.FindAll({
    param($n)
    $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'Resolve-LocalMysqlPort'
}, $true))
Assert-True ($resolveFn.Count -eq 1) '存在唯一的 Resolve-LocalMysqlPort'
if ($resolveFn.Count -eq 1) {
    Invoke-Expression $resolveFn[0].Extent.Text
    $ProjectRoot = 'C:\stub'
    $MysqlPortMin = 13307
    $MysqlPortMax = 13309
    $script:StoredPort = 13307
    $script:BindablePorts = @{ 13307 = $false; 13308 = $true; 13309 = $true }
    $script:OwnedPorts = @{}
    $script:OwnedProcess = $null
    $script:OwnedProcessListenerPorts = @()
    function Get-PandoraLocalMysqlPort([string]$Root) { return $script:StoredPort }
    function Get-OnlyOwnedMysqlListener([int[]]$Ports) {
        $matches = @($Ports | Where-Object { $script:OwnedPorts.ContainsKey([int]$_) } | Select-Object -Unique)
        if ($matches.Count -gt 1) { Fail '测试桩检测到多个本项目实例' }
        if ($matches.Count -eq 1) {
            $port = [int]$matches[0]
            return [pscustomobject]@{ Port = $port; Process = $script:OwnedPorts[$port] }
        }
        return $null
    }
    function Get-OnlyOwnedMysqlProcess { return $script:OwnedProcess }
    function Get-MysqlListenerRecordsForProcess($Proc) {
        return @($script:OwnedProcessListenerPorts | ForEach-Object {
            [pscustomobject]@{ Port = [int]$_; Process = $Proc }
        })
    }
    function Test-PortBindable([int]$Port) { return [bool]$script:BindablePorts[$Port] }
    function Get-PortHolder([int]$Port) { return ,@("PID=999 process=foreign") }
    function Write-Warn2([string]$Message) {}
    function Fail([string]$Message) { throw "PANDORA_EXPECTED_FAIL:$Message" }

    $savedOverride = $env:PANDORA_LOCALINFRA_MYSQL_PORT
    try {
        Remove-Item Env:PANDORA_LOCALINFRA_MYSQL_PORT -ErrorAction SilentlyContinue
        Assert-True ((Resolve-LocalMysqlPort) -eq 13308) '已持久化端口被外部占用时跳到下一可 bind 端口'

        $script:OwnedPorts = @{ 13309 = [pscustomobject]@{ Id = 42 } }
        Assert-True ((Resolve-LocalMysqlPort) -eq 13309) '先扫描整个候选池中的本项目实例，再考虑更早的空端口'
        $env:PANDORA_LOCALINFRA_MYSQL_PORT = '13308'
        $overrideConflict = $null
        try { Resolve-LocalMysqlPort | Out-Null } catch { $overrideConflict = $_.Exception.Message }
        Assert-True ($overrideConflict -like 'PANDORA_EXPECTED_FAIL:*') '已有本项目实例时显式其它端口也拒绝启动第二份'

        $script:OwnedPorts = @{ 14000 = [pscustomobject]@{ Id = 43 } }
        $env:PANDORA_LOCALINFRA_MYSQL_PORT = '14000'
        Assert-True ((Resolve-LocalMysqlPort) -eq 14000) '显式池外端口也先扫描并复用唯一的本项目实例'

        Remove-Item Env:PANDORA_LOCALINFRA_MYSQL_PORT -ErrorAction SilentlyContinue
        $script:StoredPort = 0
        $script:OwnedPorts = @{}
        $script:OwnedProcess = [pscustomobject]@{ Id = 44 }
        $script:OwnedProcessListenerPorts = @(14001)
        Assert-True ((Resolve-LocalMysqlPort) -eq 14001) 'state/env 都丢失时从已归属 PID 恢复池外 listener 端口'

        $script:OwnedProcessListenerPorts = @()
        $initializingFailed = $null
        try { Resolve-LocalMysqlPort | Out-Null } catch { $initializingFailed = $_.Exception.Message }
        Assert-True ($initializingFailed -like 'PANDORA_EXPECTED_FAIL:*') '已归属 mysqld 尚未 listen 时拒绝启动第二实例'

        $script:OwnedProcess = $null
        $script:OwnedPorts = @{}
        $env:PANDORA_LOCALINFRA_MYSQL_PORT = '3307'
        $blocked = $null
        try { Resolve-LocalMysqlPort | Out-Null } catch { $blocked = $_.Exception.Message }
        Assert-True ($blocked -like 'PANDORA_EXPECTED_FAIL:*') '显式指定 Docker dev 的 3307 也会 fail closed'
    } finally {
        if ($null -eq $savedOverride) { Remove-Item Env:PANDORA_LOCALINFRA_MYSQL_PORT -ErrorAction SilentlyContinue }
        else { $env:PANDORA_LOCALINFRA_MYSQL_PORT = $savedOverride }
    }
}

Write-Host '[2] 运行态 YAML 只改副本中的 MySQL DSN'
$runServices = Join-Path $scriptsDir 'run_services.ps1'
$errs = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($runServices, [ref]$null, [ref]$errs)
if ($errs -and $errs.Count -gt 0) { throw "run_services.ps1 语法错误:$($errs[0].Message)" }
$fn = @($ast.FindAll({
    param($n)
    $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'Get-ServiceConfigPath'
}, $true))
Assert-True ($fn.Count -eq 1) 'run_services.ps1 存在唯一的 Get-ServiceConfigPath'
if ($fn.Count -eq 1) {
    Assert-True ($fn[0].Extent.Text -notmatch 'Assert-NoDockerMysqlOwned') `
        '整批配置生成不得为每个服务重复执行昂贵的 mysqld CIM 归属检查'
}
$mysqlOwnershipCalls = @($ast.FindAll({
    param($n)
    $n -is [Management.Automation.Language.CommandAst] -and
        $n.GetCommandName() -ceq 'Assert-NoDockerMysqlOwned'
}, $true))
Assert-True ($mysqlOwnershipCalls.Count -eq 2) `
    'MySQL exact 归属只在脚本入口与整批依赖探活各复核一次'
if ($fn.Count -eq 1) {
    Invoke-Expression $fn[0].Extent.Text
    function Assert-NoDockerMysqlOwned {}
    $sandbox = Join-Path ([System.IO.Path]::GetTempPath()) ("pandora-localinfra-conf-{0}" -f [guid]::NewGuid().ToString('N'))
    $ProjectRoot = $sandbox
    $RunDir = Join-Path $sandbox 'run/dev'
    $NoDocker = $true
    $MysqlPort = 13308
    $svc = @{ Name = 'inventory'; Dir = 'services/economy/inventory'; Conf = 'etc/inventory-dev.yaml' }
    $sourceDir = Join-Path $sandbox 'services/economy/inventory/etc'
    New-Item -ItemType Directory -Force -Path $sourceDir | Out-Null
    $source = Join-Path $sourceDir 'inventory-dev.yaml'
    $original = @'
data:
  database:
    dsn: "pandora:x@tcp(127.0.0.1:3307)/pandora_trade"
bag_store:
  dsn: "pandora:x@tcp(127.0.0.1:3307)/pandora_bag"
note: "127.0.0.1:3307 is documentation, not a DSN"
'@
    Set-Content -LiteralPath $source -Value $original -Encoding utf8NoBOM
    try {
        $runtime = Get-ServiceConfigPath $svc
        $rendered = Get-Content -LiteralPath $runtime -Raw -Encoding utf8
        Assert-True ([IO.Path]::GetFullPath($runtime) -ne [IO.Path]::GetFullPath($source)) '服务读取生成副本，不改仓库 YAML'
        Assert-True (([regex]::Matches($rendered, 'tcp\(127\.0\.0\.1:13308\)')).Count -eq 2) '同一配置内两条 MySQL DSN 都使用选中端口'
        Assert-True ($rendered -match 'note: "127\.0\.0\.1:3307 is documentation') '非 DSN 文本保持原样'
        Assert-True ((Get-Content -LiteralPath $source -Raw -Encoding utf8) -eq ($original + [Environment]::NewLine)) '源配置内容未被修改'
        $MysqlPort = 13309
        $runtime = Get-ServiceConfigPath $svc
        $rendered = Get-Content -LiteralPath $runtime -Raw -Encoding utf8
        Assert-True ($rendered -notmatch 'tcp\(127\.0\.0\.1:13308\)' -and
            ([regex]::Matches($rendered, 'tcp\(127\.0\.0\.1:13309\)')).Count -eq 2) '端口切换会原子覆盖运行态副本，不残留旧端口'
    } finally {
        Remove-Item -LiteralPath $sandbox -Recurse -Force -ErrorAction SilentlyContinue
    }
}

if ($fn.Count -eq 1) {
    Write-Host '[2b] 仓库全部 MySQL dev 配置无漏网'
    # 只审 git 跟踪、会进入发布包的配置；并发开发者的未跟踪夹具不能改变本契约的清单口径。
    $trackedYaml = @(& git -C $RepoRoot ls-files -- 'services/**/*.yaml')
    if ($LASTEXITCODE -ne 0) { throw 'git ls-files services/**/*.yaml 失败' }
    $mysqlConfigs = @($trackedYaml | ForEach-Object { Get-Item -LiteralPath (Join-Path $RepoRoot $_) } |
        Where-Object { [IO.File]::ReadAllText($_.FullName) -match 'tcp\(127\.0\.0\.1:3307\)' })
    $sourceDsnCount = 0
    foreach ($file in $mysqlConfigs) {
        $sourceDsnCount += [regex]::Matches([IO.File]::ReadAllText($file.FullName), 'tcp\(127\.0\.0\.1:3307\)').Count
    }
    Assert-True ($mysqlConfigs.Count -eq 15) '当前 15 份 MySQL dev 配置全部进入测试清单'
    Assert-True ($sourceDsnCount -eq 16) '当前 16 条 MySQL DSN 全部进入测试清单(inventory 含两条)'

    $sandbox = Join-Path ([System.IO.Path]::GetTempPath()) ("pandora-localinfra-all-conf-{0}" -f [guid]::NewGuid().ToString('N'))
    $ProjectRoot = $sandbox
    $RunDir = Join-Path $sandbox 'run/dev'
    $NoDocker = $true
    $MysqlPort = 13308
    $renderedDsnCount = 0
    try {
        foreach ($file in $mysqlConfigs) {
            $relative = [IO.Path]::GetRelativePath($RepoRoot, $file.FullName)
            $copy = Join-Path $sandbox $relative
            New-Item -ItemType Directory -Force -Path (Split-Path -Parent $copy) | Out-Null
            Copy-Item -LiteralPath $file.FullName -Destination $copy
            $svcDir = Split-Path -Parent (Split-Path -Parent $relative)
            $svc = @{
                Name = ($relative -replace '[^A-Za-z0-9]+', '_')
                Dir = $svcDir
                Conf = Join-Path 'etc' $file.Name
            }
            $runtime = Get-ServiceConfigPath $svc
            $text = [IO.File]::ReadAllText($runtime)
            Assert-True ($text -notmatch 'tcp\(127\.0\.0\.1:3307\)') "$relative 无残留 3307 MySQL DSN"
            $renderedDsnCount += [regex]::Matches($text, 'tcp\(127\.0\.0\.1:13308\)').Count
        }
        Assert-True ($renderedDsnCount -eq 16) '生成配置共 16 条 DSN 全部改到选中端口'
    } finally {
        Remove-Item -LiteralPath $sandbox -Recurse -Force -ErrorAction SilentlyContinue
    }
}

Write-Host '[2c] 单服务动作不能在未知/不同 profile 上制造混合 DSN'
$profileFns = @{}
foreach ($name in @('Test-ServiceRuntimeProfileMatches', 'Assert-SingleServiceRuntimeCompatible')) {
    $hits = @($ast.FindAll({
        param($n)
        $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name
    }, $true))
    Assert-True ($hits.Count -eq 1) "run_services.ps1 存在唯一的 $name"
    if ($hits.Count -eq 1) { Invoke-Expression $hits[0].Extent.Text; $profileFns[$name] = $true }
}
if ($profileFns.Count -eq 2) {
    $Services = @(@{ Name = 'player'; Port = 20002 })
    $serviceRuntimeMode = 'nodocker'; $MysqlPort = 13308; $serviceRuntimeSocialOnMysql = $true
    $script:GuardApplied = $null; $script:GuardRunning = $true
    function Get-PandoraServiceAppliedMysqlState([string]$Root) { return $script:GuardApplied }
    function Get-RunningProcess($Svc) { if ($script:GuardRunning) { return [pscustomobject]@{ Id = 1 } }; return $null }
    function Test-PortOpen([int]$Port) { return $false }
    $missingMarkerBlocked = $false
    try { Assert-SingleServiceRuntimeCompatible } catch { $missingMarkerBlocked = $true }
    Assert-True $missingMarkerBlocked 'marker 缺失且已有服务运行时拒绝单服务动作'

    $script:GuardRunning = $false
    $freshAllowed = $true
    try { Assert-SingleServiceRuntimeCompatible } catch { $freshAllowed = $false }
    Assert-True $freshAllowed 'marker 缺失且全部服务停止时允许首次单服务调试'

    $script:GuardApplied = [pscustomobject]@{ Mode = 'docker'; MysqlPort = 3307; SocialOnMysql = $false }
    $mismatchBlocked = $false
    try { Assert-SingleServiceRuntimeCompatible } catch { $mismatchBlocked = $true }
    Assert-True $mismatchBlocked '已有 Docker profile 时拒绝单独切成免 Docker 配置'

    $script:GuardApplied = [pscustomobject]@{ Mode = 'nodocker'; MysqlPort = 13308; SocialOnMysql = $true }
    $sameAllowed = $true
    try { Assert-SingleServiceRuntimeCompatible } catch { $sameAllowed = $false }
    Assert-True $sameAllowed '单服务动作与已应用 profile 完全一致时放行'
}

Write-Host '[3] 启动链显式传递同一端口'
$devAllText = [IO.File]::ReadAllText((Join-Path $scriptsDir 'dev_all.ps1'))
$devUpText = [IO.File]::ReadAllText((Join-Path $scriptsDir 'dev_up.ps1'))
$devDownText = [IO.File]::ReadAllText((Join-Path $scriptsDir 'dev_down.ps1'))
$migrateText = [IO.File]::ReadAllText((Join-Path $scriptsDir 'dev_migrate.ps1'))
$startText = [IO.File]::ReadAllText((Join-Path $scriptsDir 'start.ps1'))
$runText = [IO.File]::ReadAllText($runServices)
$stateText = [IO.File]::ReadAllText((Join-Path $scriptsDir 'lib/local_infra_state.ps1'))
Assert-True ($devAllText -match 'dev_migrate\.ps1[^\r\n]*-MysqlPort\s+\$\w+[^\r\n]*-RequireMysql') 'dev_all 把已验证端口传给强制迁移'
Assert-True ($devAllText -match 'run_services\.ps1[^\r\n]*-MysqlPort\s+\$\w+') 'dev_all 把同一端口传给业务服务'
Assert-True ($migrateText -match '\[switch\]\$RequireMysql') '免 Docker 迁移连接失败必须非零退出'
Assert-True ($migrateText -match 'function\s+Assert-LocalMysqlOwned' -and
    ([regex]::Matches($migrateText, 'Assert-LocalMysqlOwned')).Count -ge 4) '每类本机 SQL/迁移器调用前都无条件重新验证 listener 归属'
Assert-True ($migrateText -match '\$MysqlHost\s+-cne\s+''127\.0\.0\.1''') '本机客户端不能用已验证端口掩护远端 MysqlHost'
$noMigratorAt = $migrateText.IndexOf('本机既没有 Go,也没有预编译的迁移器')
$noMigratorWindow = if ($noMigratorAt -ge 0) {
    $migrateText.Substring([Math]::Max(0, $noMigratorAt - 350), [Math]::Min(700, $migrateText.Length - [Math]::Max(0, $noMigratorAt - 350)))
} else { '' }
Assert-True ($noMigratorWindow -match 'RequireMysql[\s\S]*exit\s+1|exit\s+1[\s\S]*RequireMysql') '强制迁移模式缺迁移器时也必须非零退出'
$requiredStructureGuards = @(
    '找不到任何 mysql-init SQL',
    '跳过增量迁移',
    '没有 migration set',
    '强制迁移要求的库不存在',
    '没有任何可升级的库'
)
foreach ($needle in $requiredStructureGuards) {
    $at = $migrateText.IndexOf($needle)
    $window = if ($at -ge 0) {
        $migrateText.Substring([Math]::Max(0, $at - 220), [Math]::Min(500, $migrateText.Length - [Math]::Max(0, $at - 220)))
    } else { '' }
    Assert-True ($window -match 'RequireMysql[\s\S]*exit\s+1|exit\s+1[\s\S]*RequireMysql') "强制迁移模式结构缺失非零:$needle"
}
Assert-True ($runText -match '\[int\]\$MysqlPort') 'run_services 接受显式 MySQL 端口'
Assert-True ($runText -match 'Name\s*=\s*''MySQL'';\s*Port\s*=\s*\$MysqlPort') '业务服务基础设施预检使用动态端口'
Assert-True (([regex]::Matches($startText, 'run_services\.ps1[^\r\n]*-Action restart[^\r\n]*-MysqlPort')).Count -ge 2) 'DSOnly 两类 restart 都传动态端口'
Assert-True ($stateText -match 'function\s+Get-PandoraLocalMysqlOwnedProcess') '共享状态库能复核当前 listener 的 PID/exe/my.ini 归属'
Assert-True ($runText -match 'Get-PandoraLocalMysqlOwnedProcess') 'run_services 生成配置前重新验证 MySQL listener 归属'
Assert-True ($startText -match 'Get-PandoraLocalMysqlOwnedProcess') 'DsOnly 快速入口也重新验证 MySQL listener 归属'
Assert-True ($runText -match 'Get-PandoraServiceAppliedMysqlState' -and
    $runText -match 'SocialOnMysql\s+\$serviceRuntimeSocialOnMysql' -and
    $startText -match 'SocialOnMysql') 'Docker/免Docker 与 DsOnly 都用 mode+port+social profile 防止 skip 旧 DSN'
Assert-True (([regex]::Matches($devAllText, 'dev_migrate\.ps1[^\r\n]*-RequireMysql')).Count -ge 2) 'Docker 与免 Docker 一键启动都把迁移当强依赖'
Assert-True ($stateText -match 'function\s+Enter-PandoraOrchestrationLock' -and
    $devAllText -match 'Enter-PandoraOrchestrationLock' -and
    $devUpText -match 'Enter-PandoraOrchestrationLock' -and
    $devDownText -match 'Enter-PandoraOrchestrationLock' -and
    $migrateText -match 'Enter-PandoraOrchestrationLock' -and
    $runText -match 'Enter-PandoraOrchestrationLock' -and
    $startText -match '\$Mode\s+-ne\s+''online''[\s\S]*Enter-PandoraOrchestrationLock' -and
    ([IO.File]::ReadAllText($infraPath)) -match 'Enter-PandoraOrchestrationLock') '所有本机 start/dev 基础设施、迁移与服务脚本共用工作区编排锁'
Assert-True ($runText -match 'Clear-PandoraServiceAppliedMysqlState' -and
    $runText -match 'Assert-SingleServiceRuntimeCompatible') '部分启动清旧 marker，单服务跨 profile fail closed'
Assert-True ($migrateText -match 'Enter-PandoraOrchestrationLock[\s\S]*try\s*\{' -and
    $migrateText -match 'finally\s*\{[\s\S]*Exit-PandoraOrchestrationLock') '独立迁移全程持有同一工作区编排锁'
$whatIfAt = $migrateText.IndexOf('if ($WhatIfOnly)')
$initExecAt = $migrateText.IndexOf('Invoke-DevMysqlScript -Path')
Assert-True ($whatIfAt -ge 0 -and $initExecAt -gt $whatIfAt -and
    $migrateText.Substring($whatIfAt, $initExecAt - $whatIfAt) -match '\}\s*else\s*\{') 'WhatIfOnly 在 mysql-init 写入前分支，确实不执行 SQL'
Assert-True ($migrateText -match '这些库将先由 mysql-init 创建、再执行迁移') 'WhatIfOnly 把 init 将新建的库计入预计迁移目标'
Assert-True ($migrateText -match 'pandora-dev-migrate-\{0\}-\{1\}' -and $migrateText -match '\[guid\]::NewGuid') '迁移临时目录含 GUID，不同工作区同 PID 不会互相覆盖 DSN'
Assert-True (([regex]::Matches(([IO.File]::ReadAllText($infraPath)), 'Get-MysqlListenerRecordsForProcess')).Count -ge 3) '无状态 down/status 也能从池外已归属 mysqld 恢复 listener 端口'
Assert-True ($startText -match 'local_infra\.ps1"\s+-Action status[\s\S]*?\$LASTEXITCODE\s+-ne\s+0[\s\S]*?ShowStatusExitCode\s*=\s*1' -and
    $startText -match 'if\s*\(\$Status\)\s*\{\s*Show-Status;\s*exit\s+\$script:ShowStatusExitCode' -and
    ([IO.File]::ReadAllText($infraPath)) -match "'status'\s*\{\s*Invoke-Status;\s*exit\s+0\s*\}") 'NoDocker status 透传 local_infra 的失败退出码'

if ($script:Failed) {
    Write-Host ''
    Write-Host '[FAIL] 免 Docker MySQL 端口贯穿契约' -ForegroundColor Red
    exit 1
}
Write-Host ''
Write-Host '[PASS] 免 Docker MySQL 端口贯穿契约' -ForegroundColor Green
