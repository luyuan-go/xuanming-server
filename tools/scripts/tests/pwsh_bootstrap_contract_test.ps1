<#
.SYNOPSIS
  「没装 PowerShell 7 的机器也能双击免 Docker 一键入口」的契约测试。

.DESCRIPTION
  背景:免 Docker 那三个入口是给策划机用的,而策划机以前必须先自己装 PowerShell 7 ——
  入口里只有一段 `where pwsh` + 报错退出。2026-08-18 改成由 tools\scripts\bootstrap_pwsh.cmd
  在 cmd.exe 里自举一份官方免安装 pwsh(不装机、不要 UAC、卸载 = 删目录)。

  这里守的是四类会**静默**坏掉、坏了又只在别人机器上才发现的东西:

  ① 供应链:自举包会被直接执行,而取包路径有四条(本机 cache / SVN 仓库包 /
     任意可写共享盘 / 公网),本地两条完全不受 HTTPS 保护。所以 sha256 必须钉死、
     必须真的拦得住 —— [5] 拿一个假包
     真跑一遍 bootstrap_pwsh.cmd,断言它拒绝并且**没有**解包。
  ② 单一实现:版本 / 校验和只允许写在 lib\pwsh_bootstrap.pin 一处。抄第二份必然漂,
     漂了就是「本机自举出 7.6.5、共享盘上备的是 7.6.4」这种查半天的问题。
  ③ ASCII-only:.cmd 入口里出现任何非 ASCII 字节,cmd.exe 会按当前代码页重读文件、
     算错偏移,然后去执行注释行的碎片(2026-08-06 现场)。这条铁律此前没有任何测试。
  ④ 接线:三个免 Docker 入口都得真的接上自举,并且用引号包住解释器路径 —— 自举出来的
     是全路径,仓库放在带空格的目录里就会炸。

.EXAMPLE
  pwsh tools/scripts/tests/pwsh_bootstrap_contract_test.ps1
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$ScriptsDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path

$script:Failures = @()
function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { $script:Failures += $Message; Write-Host "  [FAIL] $Message" -ForegroundColor Red }
    else { Write-Host "  [ ok ] $Message" -ForegroundColor DarkGray }
}

$BootstrapCmd = Join-Path $ScriptsDir 'bootstrap_pwsh.cmd'
$PinFile = Join-Path $ScriptsDir 'lib/pwsh_bootstrap.pin'

# 免 Docker 四入口 = 本次自举的服务对象。其余入口(k8s / 出包 / 导表)面向程序,
# 保持原来的「没 pwsh 就明确报错」,不在本契约范围内。
$NoDockerEntries = @(
    '策划一键启动-免Docker-测试版.cmd'
    '策划一键停止-免Docker-测试版.cmd'
    '策划一键停止业务-保留基础设施-免Docker-测试版.cmd'
    '策划一键重启DS-免Docker-测试版.cmd'
)

# ── [1] pin 文件本身 ────────────────────────────────────────────────────────
Write-Host '[1] 版本 pin' -ForegroundColor Cyan
Assert-True (Test-Path -LiteralPath $PinFile) 'lib/pwsh_bootstrap.pin 存在'
$pin = @{}
if (Test-Path -LiteralPath $PinFile) {
    foreach ($line in [System.IO.File]::ReadAllLines($PinFile)) {
        $t = $line.Trim()
        if (-not $t -or $t.StartsWith('#')) { continue }
        $i = $t.IndexOf('=')
        if ($i -ge 1) { $pin[$t.Substring(0, $i).Trim()] = $t.Substring($i + 1).Trim() }
    }
}
foreach ($k in 'PWSH_VERSION', 'PWSH_FILE', 'PWSH_SHA256', 'PWSH_URL') {
    Assert-True ([bool]$pin[$k]) "pin 里有 $k"
}
Assert-True ($pin.PWSH_SHA256 -match '^[0-9a-f]{64}$') 'PWSH_SHA256 是 64 位小写十六进制'
# 版本号必须同时出现在文件名和 URL 里:换版时只改了一处(最典型的是改了 URL 忘了改 sha)
# 会在这里当场撞红,而不是等某台机器下到对不上号的包。
Assert-True ($pin.PWSH_FILE -like "*$($pin.PWSH_VERSION)*") 'PWSH_FILE 里含 PWSH_VERSION'
Assert-True ($pin.PWSH_URL -like "*$($pin.PWSH_VERSION)*") 'PWSH_URL 里含 PWSH_VERSION'
Assert-True ($pin.PWSH_URL.EndsWith('/' + $pin.PWSH_FILE)) 'PWSH_URL 结尾就是 PWSH_FILE(URL 与文件名不许各说各话)'
# 免安装 zip,不是 msi:msi 是按机器安装,要本地管理员 + UAC,策划机常常没有,
# 而且会把 Web 管理台的无人值守运行卡在 UAC 弹窗上。
Assert-True ($pin.PWSH_FILE -like '*.zip') 'pin 的是免安装 zip(不是需要管理员 + UAC 的 msi)'

# ── [2] 唯一实现:版本 / 校验和不许在别处再抄一份 ────────────────────────────
Write-Host '[2] 单一事实来源' -ForegroundColor Cyan
$sha = $pin.PWSH_SHA256
$dupes = @()
if ($sha) {
    $scan = @(Get-ChildItem -LiteralPath $ScriptsDir -Recurse -File -Include '*.ps1', '*.cmd' -ErrorAction SilentlyContinue) +
            @(Get-ChildItem -LiteralPath $ProjectRoot -File -Filter '*.cmd' -ErrorAction SilentlyContinue)
    $dupes = @($scan | Where-Object { $_.FullName -ne $PinFile -and $_.FullName -ne $PSCommandPath } |
        Where-Object { [System.IO.File]::ReadAllText($_.FullName) -match [regex]::Escape($sha) })
}
Assert-True ($dupes.Count -eq 0) ('sha256 只写在 pin 里' + $(if ($dupes.Count) { ',还出现在:' + (($dupes.Name) -join ', ') }))

# local_infra.ps1 必须是**读** pin,而不是自己抄一份常量。
$infra = [System.IO.File]::ReadAllText((Join-Path $ScriptsDir 'local_infra.ps1'))
Assert-True ($infra -match 'pwsh_bootstrap\.pin') 'local_infra.ps1 读 pin 文件'
Assert-True ($infra -match 'Save-PwshBootstrapArchive') 'local_infra.ps1 有 Save-PwshBootstrapArchive'
# 只在 provision 备料:能跑到 up 的机器必然已经有 pwsh,再下 100MB 是白下。
Assert-True ($infra -match "'provision'\s*\{[^}]*Save-PwshBootstrapArchive") 'provision 动作才备 pwsh 包'
Assert-True ($infra -notmatch "'up'\s*\{[^}]*Save-PwshBootstrapArchive") 'up 动作不备 pwsh 包'

# ── [3] ASCII-only 铁律(2026-08-06:非 ASCII 字节会让 cmd 执行注释行碎片)────
Write-Host '[3] 入口 .cmd 必须是纯 ASCII' -ForegroundColor Cyan
$asciiTargets = @($BootstrapCmd) + ($NoDockerEntries | ForEach-Object { Join-Path $ProjectRoot $_ })
foreach ($f in $asciiTargets) {
    $name = Split-Path -Leaf $f
    if (-not (Test-Path -LiteralPath $f)) { Assert-True $false "$name 存在"; continue }
    $bytes = [System.IO.File]::ReadAllBytes($f)
    $bad = @($bytes | Where-Object { $_ -ge 0x80 }).Count
    Assert-True ($bad -eq 0) "$name 无非 ASCII 字节(发现 $bad)"
    Assert-True ($bytes.Length -lt 3 -or -not ($bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF)) "$name 无 BOM"
    # chcp 会换掉控制台代码页,正是那个 bug 的另一半触发条件。
    Assert-True ([System.IO.File]::ReadAllText($f) -notmatch '(?im)^\s*chcp\b') "$name 不含 chcp"
}
# pin 文件也归 cmd.exe 的 for /f 读,同样只能是 ASCII。
if (Test-Path -LiteralPath $PinFile) {
    $pinBytes = [System.IO.File]::ReadAllBytes($PinFile)
    Assert-True (@($pinBytes | Where-Object { $_ -ge 0x80 }).Count -eq 0) 'pwsh_bootstrap.pin 无非 ASCII 字节'
}
$bootstrapText = [IO.File]::ReadAllText($BootstrapCmd)
Assert-True ($bootstrapText -match 'set "_PB_ZIP=%_PB_RUN_DIR%\\%PWSH_FILE%"') `
    '校验/解包路径位于本轮唯一 run 目录'
Assert-True ($bootstrapText -match 'curl\.exe[^\r\n]+-o "%_PB_ZIP%\.part"' -and
    $bootstrapText -notmatch 'curl\.exe[^\r\n]+_PB_CACHE_ZIP%\.part') `
    '公网下载的 .part 也是本轮唯一路径'
Assert-True ($bootstrapText -match 'if exist "%_PB_STAGE%" goto :run_collision' -and
    $bootstrapText -match 'if exist "%_PB_OLD%" goto :run_collision') `
    '同一 run id 在创建 staging 前同时排除 tmp/old 碰撞'
Assert-True ($bootstrapText -match '(?s):snapshot_archive.*?mklink /h.*?copy /y' -and
    $bootstrapText -match '(?s)tar\.exe -x -f "%_PB_ZIP%".*?call :hash_file "%_PB_ZIP%"') `
    'SVN/cache 先 hardlink/copy 成快照，解包后再复核同一快照'
Assert-True ($bootstrapText -match '_PB_CACHE_LOCK' -and $bootstrapText -match ':publish_cache') `
    '固定 cache 名通过独立 publish 文件和短锁发布'

# ── [4] 四个免 Docker 入口真的接上了自举 ────────────────────────────────────
Write-Host '[4] 入口接线' -ForegroundColor Cyan
foreach ($name in $NoDockerEntries) {
    $path = Join-Path $ProjectRoot $name
    if (-not (Test-Path -LiteralPath $path)) { Assert-True $false "$name 存在"; continue }
    $text = [System.IO.File]::ReadAllText($path)
    Assert-True ($text -match 'call "%~dp0tools\\scripts\\bootstrap_pwsh\.cmd"') "$name 调用 bootstrap_pwsh.cmd"
    # 不许再留自己那份 where pwsh 判定:留着就会和自举的结论打架(自举成功了它还报缺)。
    Assert-True ($text -notmatch '(?m)^where pwsh') "$name 不再内联 where pwsh 判定"
    Assert-True ($text -match 'PANDORA_PWSH') "$name 用 bootstrap 给出的 PANDORA_PWSH"
    # 自举出来的是全路径,仓库放在带空格的目录下不加引号必炸。
    Assert-True ($text -match '"%PS%" -NoProfile') "$name 用引号包住解释器路径"
    Assert-True ($text -notmatch '(?m)^\s*%PS% ') "$name 没有裸 %PS% 调用"
}

# ── [5] 供应链闸门:假包必须被拒,且绝不解包 ────────────────────────────────
# 在临时目录里真跑一遍 bootstrap_pwsh.cmd:PATH 收窄到系统目录(制造「本机没有 pwsh」),
# 不设环境变量，只在仓库 installers/localinfra 放同名假包。这条路径是整套机制里唯一
# 「错了会真出事」的地方,所以必须真跑,不能只做静态断言。
Write-Host '[5] sha256 闸门(假包真跑一遍)' -ForegroundColor Cyan
$pathInjectionSentinel = 'PANDORA_PATH_INJECTION'
$tmpRoot = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-pwshboot-R&echo ' + $pathInjectionSentinel + '&echo-' + [System.IO.Path]::GetRandomFileName())
$runnerRoot = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-pwshboot-runner-' + [System.IO.Path]::GetRandomFileName())
try {
    $fakeScripts = Join-Path $tmpRoot 'tools/scripts'
    New-Item -ItemType Directory -Force -Path (Join-Path $fakeScripts 'lib') | Out-Null
    Copy-Item $BootstrapCmd (Join-Path $fakeScripts 'bootstrap_pwsh.cmd')
    Copy-Item $PinFile (Join-Path $fakeScripts 'lib/pwsh_bootstrap.pin')

    $bundle = Join-Path $tmpRoot 'installers/localinfra'
    New-Item -ItemType Directory -Force -Path $bundle | Out-Null
    [System.IO.File]::WriteAllText((Join-Path $bundle $pin.PWSH_FILE), 'not a powershell release')

    $sysRoot = [Environment]::GetFolderPath('Windows')
    $minimalPath = "$sysRoot\system32;$sysRoot"
    # cmd.exe /c 会在解析命令行本身时处理 &。启动器放在普通临时目录，
    # 被测仓库/脚本仍位于含 & 的路径，覆盖的正是 bootstrap 的路径安全性。
    New-Item -ItemType Directory -Force -Path $runnerRoot | Out-Null
    $runner = Join-Path $runnerRoot 'run.cmd'
    [System.IO.File]::WriteAllText($runner, (@"
@echo off
setlocal
set "PATH=$minimalPath"
set "PANDORA_LOCALINFRA_MIRROR="
call "$fakeScripts\bootstrap_pwsh.cmd"
if errorlevel 1 exit /b 1
"%PANDORA_PWSH%" cmd.exe >nul 2>nul
if errorlevel 1 exit /b 3
exit /b 0
"@ -replace "`r?`n", "`r`n"))

    $out = & cmd.exe /c "`"$runner`"" 2>&1 | Out-String
    $rc = $LASTEXITCODE
    if ($rc -eq 0 -or $out -notmatch 'SHA256 mismatch') { Write-Host $out -ForegroundColor DarkYellow }
    Assert-True ($out -notmatch [regex]::Escape($pathInjectionSentinel)) '仓库路径中的 & 未执行额外命令(坏包链)'
    Assert-True ($rc -ne 0) "假包时以非零码退出(实际 $rc)"
    Assert-True ($out -match 'SHA256 mismatch') '报的是 sha256 不匹配'
    Assert-True ($out -match 'repository bundle is damaged') '明确指出 SVN 仓库安装包损坏'
    Assert-True (-not (Test-Path -LiteralPath (Join-Path $tmpRoot 'run/localinfra/dist/pwsh'))) '校验不过就绝不解包'
    Assert-True (-not (Test-Path -LiteralPath (Join-Path $tmpRoot 'run/localinfra/dist/pwsh.tmp'))) '不留半截解包目录'
    Assert-True (@(Get-ChildItem -LiteralPath (Join-Path $tmpRoot 'run/localinfra/dist') -Filter 'pwsh.tmp-*' -ErrorAction SilentlyContinue).Count -eq 0) `
        '失败链不留唯一命名的 staging 目录'
    Assert-True (Test-Path -LiteralPath (Join-Path $bundle $pin.PWSH_FILE)) '坏仓库包只读保留，绝不由启动器删除或改写'
    # 坏包必须从 cache 删掉,否则下次跑还会拿它当缓存再撞一次。
    Assert-True (-not (Test-Path -LiteralPath (Join-Path $tmpRoot ('run/localinfra/cache/' + $pin.PWSH_FILE)))) '坏包不留在 cache'

    # ── [6] 默认仓库包成功链每次必跑；同时覆盖“坏 cache + 好 bundle”同轮自愈 ─────
    Write-Host '[6] 默认仓库包成功链(强制执行)' -ForegroundColor Cyan
    $syntheticVersion = '0.0-test'
    $syntheticFile = 'PowerShell-0.0-test-win-x64.zip'
    $syntheticPayload = Join-Path $tmpRoot 'synthetic-payload'
    $syntheticMaster = Join-Path $tmpRoot 'synthetic-good.zip'
    New-Item -ItemType Directory -Force -Path $syntheticPayload | Out-Null
    Copy-Item (Join-Path $sysRoot 'System32/where.exe') (Join-Path $syntheticPayload 'pwsh.exe')
    Compress-Archive -LiteralPath (Join-Path $syntheticPayload 'pwsh.exe') -DestinationPath $syntheticMaster -Force
    $syntheticHash = (Get-FileHash -LiteralPath $syntheticMaster -Algorithm SHA256).Hash.ToLowerInvariant()
    $syntheticPin = @(
        "PWSH_VERSION=$syntheticVersion"
        "PWSH_FILE=$syntheticFile"
        "PWSH_SHA256=$syntheticHash"
        "PWSH_URL=https://invalid.example/$syntheticFile"
        ''
    ) -join "`r`n"
    [System.IO.File]::WriteAllText((Join-Path $fakeScripts 'lib/pwsh_bootstrap.pin'), $syntheticPin, [System.Text.ASCIIEncoding]::new())
    Copy-Item $syntheticMaster (Join-Path $bundle $syntheticFile) -Force
    $syntheticCache = Join-Path $tmpRoot ('run/localinfra/cache/' + $syntheticFile)
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $syntheticCache) | Out-Null
    [System.IO.File]::WriteAllText($syntheticCache, 'interrupted or replaced cache')

    $runner2 = Join-Path $runnerRoot 'run2.cmd'
    [System.IO.File]::WriteAllText($runner2, (@"
@echo off
setlocal
set "PATH=$minimalPath"
set "PANDORA_LOCALINFRA_MIRROR="
call "$fakeScripts\bootstrap_pwsh.cmd"
if errorlevel 1 exit /b 1
if not exist "%PANDORA_PWSH%" exit /b 2
echo PANDORA_PWSH_READY
exit /b 0
"@ -replace "`r?`n", "`r`n"))
    $out2 = & cmd.exe /c "`"$runner2`"" 2>&1 | Out-String
    $rc2 = $LASTEXITCODE
    if ($rc2 -ne 0) { Write-Host $out2 -ForegroundColor DarkYellow }
    Assert-True ($out2 -notmatch [regex]::Escape($pathInjectionSentinel)) '仓库路径中的 & 未执行额外命令(bundle 链)'
    Assert-True ($rc2 -eq 0) "坏 cache 后同一轮改用仓库好包并自举成功(实际退出码 $rc2)"
    Assert-True ($out2 -match 'cached .* failed SHA256') '明确报告坏 cache 并继续尝试只读源'
    Assert-True ($out2 -match 'source:\s*bundle') '实际命中默认仓库 bundle'
    Assert-True (-not (Test-Path -LiteralPath $syntheticCache)) '默认仓库 bundle 校验后直接解包，不重复复制到 cache'
    Assert-True (Test-Path -LiteralPath (Join-Path $tmpRoot 'run/localinfra/dist/pwsh/pwsh.exe')) '合成包解到 run/localinfra/dist/pwsh'
    Assert-True ((Get-Content -Raw -LiteralPath (Join-Path $tmpRoot 'run/localinfra/dist/pwsh/.pandora-package.sha256')).Trim() -eq $syntheticHash) '新 dist 在发布前写入当前包 SHA256 marker'
    Assert-True ($out2 -match 'PANDORA_PWSH_READY') 'PANDORA_PWSH 指向解包出的 pwsh.exe'

    # ── [6b] 显式镜像必须覆盖仓库默认目录 ───────────────────────────────────
    Write-Host '[6a] SVN bundle 校验/解包同一快照' -ForegroundColor Cyan
    Remove-Item -LiteralPath (Join-Path $tmpRoot 'run/localinfra/dist/pwsh') -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $syntheticCache -Force -ErrorAction SilentlyContinue
    Copy-Item $syntheticMaster (Join-Path $bundle $syntheticFile) -Force
    $hashHookDir = Join-Path $runnerRoot 'hash-hook'
    New-Item -ItemType Directory -Force -Path $hashHookDir | Out-Null
    $hashHookSentinel = Join-Path $runnerRoot 'hash-hook-fired.txt'
    $replacementArchive = Join-Path $runnerRoot 'replacement-after-hash.zip'
    [IO.File]::WriteAllText($replacementArchive, 'replacement must never be unpacked', [Text.ASCIIEncoding]::new())
    $hashHook = Join-Path $hashHookDir 'certutil.cmd'
    [IO.File]::WriteAllText($hashHook, (@"
@echo off
"$sysRoot\System32\certutil.exe" %*
set "_HOOK_RC=%ERRORLEVEL%"
if not exist "$hashHookSentinel" (
  > "$hashHookSentinel" echo replaced
  move /y "$replacementArchive" "$bundle\$syntheticFile" >nul
)
exit /b %_HOOK_RC%
"@ -replace "`r?`n", "`r`n"), [Text.ASCIIEncoding]::new())
    $runnerSnapshot = Join-Path $runnerRoot 'run-snapshot.cmd'
    [IO.File]::WriteAllText($runnerSnapshot, (@"
@echo off
setlocal
set "PATH=$hashHookDir;$minimalPath"
set "PANDORA_LOCALINFRA_MIRROR="
call "$fakeScripts\bootstrap_pwsh.cmd"
if errorlevel 1 exit /b 1
"%PANDORA_PWSH%" cmd.exe >nul 2>nul
if errorlevel 1 exit /b 3
exit /b 0
"@ -replace "`r?`n", "`r`n"), [Text.ASCIIEncoding]::new())
    $snapshotOut = & cmd.exe /d /c "`"$runnerSnapshot`"" 2>&1 | Out-String
    $snapshotRc = $LASTEXITCODE
    if ($snapshotRc -ne 0) { Write-Host $snapshotOut -ForegroundColor DarkYellow }
    Assert-True (Test-Path -LiteralPath $hashHookSentinel -PathType Leaf) '测试已在首次 SHA256 返回后原子替换 SVN 源包'
    Assert-True ($snapshotRc -eq 0) "源名称被替换后仍用已验证快照完成自举(实际退出码 $snapshotRc)"
    $snapshotPwsh = Join-Path $tmpRoot 'run/localinfra/dist/pwsh/pwsh.exe'
    $publishedVerifiedSnapshot = (Test-Path -LiteralPath $snapshotPwsh -PathType Leaf) -and
        ((Get-FileHash -LiteralPath $snapshotPwsh -Algorithm SHA256).Hash -eq
            (Get-FileHash -LiteralPath (Join-Path $syntheticPayload 'pwsh.exe') -Algorithm SHA256).Hash)
    Assert-True $publishedVerifiedSnapshot `
        '发布的 pwsh.exe 来自校验通过的原快照，不是后来替换的源路径'
    Copy-Item $syntheticMaster (Join-Path $bundle $syntheticFile) -Force

    # ── [6b] 显式镜像必须覆盖仓库默认目录 ──────────────────
    Write-Host '[6b] 显式镜像优先' -ForegroundColor Cyan
    Remove-Item -LiteralPath (Join-Path $tmpRoot 'run/localinfra/dist/pwsh') -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $syntheticCache -Force -ErrorAction SilentlyContinue
    [System.IO.File]::WriteAllText((Join-Path $bundle $syntheticFile), 'bad repository bundle')
    $explicitDir = Join-Path $tmpRoot 'explicit-mirror'
    New-Item -ItemType Directory -Force -Path $explicitDir | Out-Null
    Copy-Item $syntheticMaster (Join-Path $explicitDir $syntheticFile)
    $runner3 = Join-Path $runnerRoot 'run3.cmd'
    [System.IO.File]::WriteAllText($runner3, (@"
@echo off
setlocal
set "PATH=$minimalPath"
set "PANDORA_LOCALINFRA_MIRROR=$explicitDir"
call "$fakeScripts\bootstrap_pwsh.cmd"
if errorlevel 1 exit /b 1
if not exist "%PANDORA_PWSH%" exit /b 2
exit /b 0
"@ -replace "`r?`n", "`r`n"))
    $out3 = & cmd.exe /c "`"$runner3`"" 2>&1 | Out-String
    $rc3 = $LASTEXITCODE
    Assert-True ($out3 -notmatch [regex]::Escape($pathInjectionSentinel)) '仓库路径中的 & 未执行额外命令(显式镜像链)'
    Assert-True ($rc3 -eq 0) "显式好镜像覆盖仓库坏包(实际退出码 $rc3)"
    Assert-True ($out3 -match 'source:\s*mirror') '复制日志确认命中显式镜像'

    # ── [6c] pin / bundle 更新后，已有 dist 不能永久复用旧解释器 ─────────────
    Write-Host '[6c] 已安装旧版本自动升级' -ForegroundColor Cyan
    Remove-Item -LiteralPath (Join-Path $tmpRoot 'run/localinfra/dist/pwsh') -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $syntheticCache -Force -ErrorAction SilentlyContinue
    $staleDist = Join-Path $tmpRoot 'run/localinfra/dist/pwsh'
    $legacyRecovery = "$staleDist.old"
    New-Item -ItemType Directory -Force -Path $legacyRecovery | Out-Null
    [IO.File]::WriteAllText((Join-Path $legacyRecovery 'recovery.txt'), 'must survive the next attempt', [Text.ASCIIEncoding]::new())
    New-Item -ItemType Directory -Force -Path $staleDist | Out-Null
    Copy-Item (Join-Path $sysRoot 'System32/certutil.exe') (Join-Path $staleDist 'pwsh.exe')
    Copy-Item $syntheticMaster (Join-Path $bundle $syntheticFile) -Force
    $runnerStale = Join-Path $runnerRoot 'run-stale.cmd'
    [System.IO.File]::WriteAllText($runnerStale, (@"
@echo off
setlocal
set "PATH=$minimalPath"
set "PANDORA_LOCALINFRA_MIRROR="
call "$fakeScripts\bootstrap_pwsh.cmd"
if errorlevel 1 exit /b 1
"%PANDORA_PWSH%" cmd.exe >nul 2>nul
if errorlevel 1 exit /b 3
exit /b 0
"@ -replace "`r?`n", "`r`n"))

    # 固定目录锁没有 owner/fencing，进程崩溃后不能由多个 waiter 猜测删除。
    # 预置遗留锁时必须有界 fail-closed，保留锁供人工确认，而不是 ABA 拆掉别人的新锁。
    $publishLock = "$staleDist.publish-lock"
    New-Item -ItemType Directory -Force -Path $publishLock | Out-Null
    $lockedOut = & cmd.exe /c "`"$runnerStale`"" 2>&1 | Out-String
    $lockedRc = $LASTEXITCODE
    Assert-True ($lockedRc -ne 0) "已有 publish lock 时有界失败(实际退出码 $lockedRc)"
    Assert-True ($lockedOut -match 'publish lock') '锁超时明确报告 portable PowerShell 发布锁'
    Assert-True (Test-Path -LiteralPath $publishLock -PathType Container) '不猜测删除无 fencing 的遗留锁'
    Remove-Item -LiteralPath $publishLock -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $staleDist -Recurse -Force -ErrorAction SilentlyContinue

    $outStale = & cmd.exe /c "`"$runnerStale`"" 2>&1 | Out-String
    $rcStale = $LASTEXITCODE
    $installedHash = (Get-FileHash -LiteralPath (Join-Path $staleDist 'pwsh.exe') -Algorithm SHA256).Hash
    $wantedExecutableHash = (Get-FileHash -LiteralPath (Join-Path $syntheticPayload 'pwsh.exe') -Algorithm SHA256).Hash
    Assert-True ($rcStale -eq 0) "已有旧 dist 时升级链成功(实际退出码 $rcStale)"
    Assert-True ($installedHash -eq $wantedExecutableHash) 'pin/bundle 更新后自动替换已有旧版 pwsh dist'
    Assert-True ((Get-Content -Raw -LiteralPath (Join-Path $staleDist '.pandora-package.sha256')).Trim() -eq $syntheticHash) '升级后的 dist marker 等于当前 pin SHA256'
    Assert-True (-not (Test-Path -LiteralPath $publishLock)) '成功发布后不残留 publish lock'
    Assert-True ((Get-Content -Raw -LiteralPath (Join-Path $legacyRecovery 'recovery.txt')) -eq 'must survive the next attempt') `
        '上一次失败留下的恢复目录不会在新一轮 swap 前被删除'

    # ── [6cc] 两次双击同时自举，只能发布完整 current，不能互删共享 .tmp/.old ─────
    Write-Host '[6cc] 并发双击原子发布' -ForegroundColor Cyan
    Remove-Item -LiteralPath $staleDist -Recurse -Force
    Remove-Item -LiteralPath $syntheticCache -Force -ErrorAction SilentlyContinue
    Copy-Item $syntheticMaster (Join-Path $explicitDir $syntheticFile) -Force
    $runnerConcurrent = Join-Path $runnerRoot 'run-concurrent.cmd'
    [IO.File]::WriteAllText($runnerConcurrent, (@"
@echo off
setlocal
set "PATH=$minimalPath"
set "PANDORA_LOCALINFRA_MIRROR=$explicitDir"
call "$fakeScripts\bootstrap_pwsh.cmd"
if errorlevel 1 exit /b 1
"%PANDORA_PWSH%" cmd.exe >nul 2>nul
if errorlevel 1 exit /b 3
exit /b 0
"@ -replace "`r?`n", "`r`n"), [Text.ASCIIEncoding]::new())
    $concurrentOutA = Join-Path $runnerRoot 'concurrent-a.out'
    $concurrentErrA = Join-Path $runnerRoot 'concurrent-a.err'
    $concurrentOutB = Join-Path $runnerRoot 'concurrent-b.out'
    $concurrentErrB = Join-Path $runnerRoot 'concurrent-b.err'
    $procA = Start-Process -FilePath $env:ComSpec -ArgumentList @('/d', '/c', "`"$runnerConcurrent`"") `
        -RedirectStandardOutput $concurrentOutA -RedirectStandardError $concurrentErrA -WindowStyle Hidden -PassThru
    $procB = Start-Process -FilePath $env:ComSpec -ArgumentList @('/d', '/c', "`"$runnerConcurrent`"") `
        -RedirectStandardOutput $concurrentOutB -RedirectStandardError $concurrentErrB -WindowStyle Hidden -PassThru
    $doneA = $procA.WaitForExit(30000)
    $doneB = $procB.WaitForExit(30000)
    if (-not $doneA) { Stop-Process -Id $procA.Id -Force -ErrorAction SilentlyContinue }
    if (-not $doneB) { Stop-Process -Id $procB.Id -Force -ErrorAction SilentlyContinue }
    $rcConcurrentA = if ($doneA) { $procA.ExitCode } else { -999 }
    $rcConcurrentB = if ($doneB) { $procB.ExitCode } else { -999 }
    $concurrentOutput = (@($concurrentOutA, $concurrentErrA, $concurrentOutB, $concurrentErrB) | ForEach-Object {
            if (Test-Path -LiteralPath $_) { Get-Content -Raw -LiteralPath $_ }
        }) -join "`n"
    if ($rcConcurrentA -ne 0 -or $rcConcurrentB -ne 0) { Write-Host $concurrentOutput -ForegroundColor DarkYellow }
    Assert-True ($concurrentOutput -notmatch [regex]::Escape($pathInjectionSentinel)) '并发链中的仓库路径未执行额外命令'
    Assert-True ($rcConcurrentA -eq 0 -and $rcConcurrentB -eq 0) `
        "两次并发自举均收敛到 current，且返回后立即可执行(实际 $rcConcurrentA/$rcConcurrentB)"
    Assert-True ((Get-Content -Raw -LiteralPath (Join-Path $staleDist '.pandora-package.sha256')).Trim() -eq $syntheticHash) `
        '并发后 current marker 精确匹配当前 pin'
    Assert-True (@(Get-ChildItem -LiteralPath (Split-Path -Parent $staleDist) -Filter 'pwsh.tmp-*' -ErrorAction SilentlyContinue).Count -eq 0) `
        '并发后不残留 staging'
    Assert-True (@(Get-ChildItem -LiteralPath (Split-Path -Parent $staleDist) -Filter 'pwsh.old-*' -ErrorAction SilentlyContinue).Count -eq 0) `
        '成功并发发布不残留本轮唯一 backup'
    Assert-True (-not (Test-Path -LiteralPath $publishLock)) '并发收敛后不残留 publish lock'
    Assert-True (Test-Path -LiteralPath (Join-Path $legacyRecovery 'recovery.txt')) '并发发布仍不删除前次人工恢复目录'
    Assert-True ((Test-Path -LiteralPath $syntheticCache -PathType Leaf) -and
        ((Get-FileHash -LiteralPath $syntheticCache -Algorithm SHA256).Hash.ToLowerInvariant() -eq $syntheticHash)) `
        '两个启动器并发镜像取包后，固定 cache 只发布完整验证包'
    Assert-True (-not (Test-Path -LiteralPath "$syntheticCache.pandora-publish-lock")) `
        '并发 cache 发布后不残留发布锁'
    Assert-True (@(Get-ChildItem -LiteralPath (Split-Path -Parent $syntheticCache) -Filter ".$syntheticFile.publish-*" -ErrorAction SilentlyContinue).Count -eq 0) `
        '并发 cache 发布后不残留任一 run 的唯一 publish 文件'
    $pwshRunRoot = Join-Path (Split-Path -Parent $syntheticCache) '.pandora-runs'
    Assert-True (@(Get-ChildItem -LiteralPath $pwshRunRoot -Directory -Filter 'pwsh-*' -ErrorAction SilentlyContinue).Count -eq 0) `
        '并发收敛后每轮只清理自己的 snapshot/.part 目录'

    # ── [6d] marker 匹配才可复用；此时不应再碰已经损坏的 bundle ───────────
    Write-Host '[6d] 当前 dist 精确复用' -ForegroundColor Cyan
    Remove-Item -LiteralPath $syntheticCache -Force -ErrorAction SilentlyContinue
    [System.IO.File]::WriteAllText((Join-Path $bundle $syntheticFile), 'bad repository bundle')
    $outReuse = & cmd.exe /c "`"$runnerStale`"" 2>&1 | Out-String
    $rcReuse = $LASTEXITCODE
    Assert-True ($rcReuse -eq 0) "marker 与 pin 匹配时直接复用(实际退出码 $rcReuse)"
    Assert-True ($outReuse -notmatch 'copying|unpacking|SHA256 mismatch') '精确匹配的 dist 不读取 cache/bundle/network'

    # ── [6e] marker 过期且新包获取失败，绝不能悄悄继续使用旧解释器 ─────────
    Write-Host '[6e] 更新失败不回退旧 dist' -ForegroundColor Cyan
    Copy-Item (Join-Path $sysRoot 'System32/certutil.exe') (Join-Path $staleDist 'pwsh.exe') -Force
    [System.IO.File]::WriteAllText((Join-Path $staleDist '.pandora-package.sha256'), (('0' * 64) + "`r`n"), [System.Text.ASCIIEncoding]::new())
    $oldExecutableHash = (Get-FileHash -LiteralPath (Join-Path $staleDist 'pwsh.exe') -Algorithm SHA256).Hash
    $outFailedRefresh = & cmd.exe /c "`"$runnerStale`"" 2>&1 | Out-String
    $rcFailedRefresh = $LASTEXITCODE
    $afterFailedRefreshHash = (Get-FileHash -LiteralPath (Join-Path $staleDist 'pwsh.exe') -Algorithm SHA256).Hash
    Assert-True ($rcFailedRefresh -ne 0) "旧 marker 且新 bundle 损坏时硬失败(实际退出码 $rcFailedRefresh)"
    Assert-True ($outFailedRefresh -match 'portable PowerShell is stale') '明确报告已有 portable dist 已过期'
    Assert-True ($outFailedRefresh -match 'SHA256 mismatch') '更新包损坏时保留供应链错误原因'
    Assert-True ($afterFailedRefreshHash -eq $oldExecutableHash) '更新失败保留旧目录供下次重试，但本轮不返回它'

    # ── [6f] 系统 PATH 上的 pwsh 始终优先，甚至不要求仓库 pin 可读 ──────────
    Write-Host '[6f] 系统 PATH 优先' -ForegroundColor Cyan
    $fakePathPwsh = Join-Path $tmpRoot 'system-pwsh'
    New-Item -ItemType Directory -Force -Path $fakePathPwsh | Out-Null
    [System.IO.File]::WriteAllText((Join-Path $fakePathPwsh 'pwsh.cmd'), "@exit /b 0`r`n", [System.Text.ASCIIEncoding]::new())
    $savedSyntheticPin = Get-Content -Raw -LiteralPath (Join-Path $fakeScripts 'lib/pwsh_bootstrap.pin')
    Remove-Item -LiteralPath (Join-Path $fakeScripts 'lib/pwsh_bootstrap.pin') -Force
    $runnerPath = Join-Path $runnerRoot 'run-path.cmd'
    [System.IO.File]::WriteAllText($runnerPath, (@"
@echo off
setlocal
set "PATH=$fakePathPwsh;$minimalPath"
call "$fakeScripts\bootstrap_pwsh.cmd"
if errorlevel 1 exit /b 1
if /i not "%PANDORA_PWSH%"=="pwsh" exit /b 2
exit /b 0
"@ -replace "`r?`n", "`r`n"))
    $outPath = & cmd.exe /c "`"$runnerPath`"" 2>&1 | Out-String
    $rcPath = $LASTEXITCODE
    [System.IO.File]::WriteAllText((Join-Path $fakeScripts 'lib/pwsh_bootstrap.pin'), $savedSyntheticPin, [System.Text.ASCIIEncoding]::new())
    Assert-True ($rcPath -eq 0) "PATH pwsh 在缺 pin 时仍直接获胜(实际退出码 $rcPath)"

    # ── [7] 若仓库/本机已有真实官方包，再验证解释器确实能启动 ───────────────
    Write-Host '[7] 真实官方包端到端(有包才跑)' -ForegroundColor Cyan
    Copy-Item $PinFile (Join-Path $fakeScripts 'lib/pwsh_bootstrap.pin') -Force
    Remove-Item -LiteralPath (Join-Path $tmpRoot 'run/localinfra/dist/pwsh') -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath (Join-Path $tmpRoot 'run/localinfra/cache') -Recurse -Force -ErrorAction SilentlyContinue
    $realZip = @(
        (Join-Path $ProjectRoot ('installers/localinfra/' + $pin.PWSH_FILE))
        (Join-Path $ProjectRoot ('run/localinfra/cache/' + $pin.PWSH_FILE))
    ) | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } | Select-Object -First 1
    if (-not $realZip) {
        Write-Host "  [skip] 本轮只缺真实官方包；默认 bundle 的复制/校验/解包成功链已由 [6] 强制验证" -ForegroundColor Yellow
    } else {
        Copy-Item $realZip (Join-Path $bundle $pin.PWSH_FILE) -Force
        $runner4 = Join-Path $runnerRoot 'run4.cmd'
        [System.IO.File]::WriteAllText($runner4, (@"
@echo off
setlocal
set "PATH=$minimalPath"
set "PANDORA_LOCALINFRA_MIRROR="
call "$fakeScripts\bootstrap_pwsh.cmd"
if errorlevel 1 exit /b 1
"%PANDORA_PWSH%" -NoProfile -Command "`$PSVersionTable.PSVersion.ToString()"
exit /b %ERRORLEVEL%
"@ -replace "`r?`n", "`r`n"))
        $out4 = & cmd.exe /c "`"$runner4`"" 2>&1 | Out-String
        $rc4 = $LASTEXITCODE
        Assert-True ($out4 -notmatch [regex]::Escape($pathInjectionSentinel)) '仓库路径中的 & 未执行额外命令(真实包链)'
        Assert-True ($rc4 -eq 0) "真实官方包自举出的解释器可运行(实际退出码 $rc4)"
        Assert-True ($out4 -match [regex]::Escape($pin.PWSH_VERSION)) "真实解释器报告版本 $($pin.PWSH_VERSION)"
    }
}
finally {
    if (Test-Path -LiteralPath $tmpRoot) {
        try { [System.IO.Directory]::Delete($tmpRoot, $true) } catch { }
    }
    if (Test-Path -LiteralPath $runnerRoot) {
        try { [System.IO.Directory]::Delete($runnerRoot, $true) } catch { }
    }
}

Write-Host ''
if ($script:Failures.Count -gt 0) {
    Write-Host "[ERR ] $($script:Failures.Count) 项契约未满足:" -ForegroundColor Red
    $script:Failures | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}
Write-Host '[ OK ] PowerShell 7 自举契约全部满足。' -ForegroundColor Green
