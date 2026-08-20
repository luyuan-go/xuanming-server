#!/usr/bin/env pwsh
<#
.SYNOPSIS
  新机器一键搭建：把一台干净的 Windows 机器变成「开发机 + 构建机 + CI 机」。

.DESCRIPTION
  这是唯一入口。它不重新实现任何东西，只是把已有脚本按**正确顺序**串起来并加门禁：

    1. 前置工具        按角色检查 pwsh7 / svn / git / docker / go / java / kubectl / minikube
    2. 机器级环境变量  PANDORA_ARTIFACT_ROOT / LINUX_MULTIARCH_ROOT / PANDORA_PROTO_SERVER_ROOT
    3. 源码工作副本    校验后端 Git origin；按需检出客户端 SVN
    4. CI 栈           up.ps1（Jenkins + registry + MinIO）
    5. Jenkins 接线    setup-jenkins.ps1 + install-agent-service.ps1
    6. 构建机自检      preflight-buildmachine.ps1
    7. 汇总仍需人工的部分

  顺序不能换：setup-jenkins 要 Jenkins 已经在跑，install-agent-service 要节点已经建好。

  【本脚本刻意不做的事】
    - 不下 51GB 的 UE 引擎、不装 Linux 交叉编译工具链（体积太大且来源因人而异，只报告缺不缺）
    - 不碰任何口令：Jenkins 里的 SVN 凭据必须你自己在它的界面上建
    - 不默认检出客户端（89GB 级别，静默拉一天不合适），要拉显式加 -CheckoutClient
    - 没有 -Install 就不装任何东西，只告诉你缺什么

.EXAMPLE
  # 先看一遍会做什么，什么都不改
  pwsh tools\devops\bootstrap-machine.ps1 -WhatIfOnly

  # 实际搭建（缺的工具用 winget 装）
  pwsh tools\devops\bootstrap-machine.ps1 -Install -ArtifactRoot D:\artifacts

  # 只搭 CI 栈
  pwsh tools\devops\bootstrap-machine.ps1 -Role CI
#>
[CmdletBinding()]
param(
    # 这台机器要承担的角色。Dev=跑后端服务，Build=UE 打包发布，CI=Jenkins 栈。
    [ValidateSet('All', 'Dev', 'Build', 'CI')]
    [string[]]$Role = @('All'),

    # 制品库根。留空则沿用已设的 PANDORA_ARTIFACT_ROOT，再退到「后端仓库所在盘 \artifacts」。
    [string]$ArtifactRoot,

    # 客户端 SVN 工作副本。留空则取后端仓库的同级 Pandora-Client-SVN。
    [string]$ClientRepo,

    # Linux 交叉编译工具链根（打 Linux DS 必需）。留空则沿用已设的 LINUX_MULTIARCH_ROOT。
    [string]$ToolchainRoot,

    # SVN 仓库根。换服务器时改这里。
    [string]$SvnBase = 'http://infinity-svn/svn/Pandora-Moba',

    # 后端 Git 权威源。需与 backend-dev 的 BACKEND_GIT_URL 保持一致。
    [string]$BackendGitUrl = 'https://github.com/luyuan-go/XuanMing-Server.git',

    [switch]$CheckoutClient,  # 显式要求检出客户端（很大，默认不做）
    [switch]$Install,         # 允许用 winget 装缺失工具
    [switch]$WhatIfOnly       # 只报告，不改任何东西
)

$ErrorActionPreference = 'Stop'
$here        = $PSScriptRoot
$ProjectRoot = (Resolve-Path (Join-Path $here '..\..')).Path

$script:fail   = 0
$script:warn   = 0
$script:manual = @()

function Section($t) { Write-Host "`n===== $t =====" -ForegroundColor Cyan }
function Ok   ($m) { Write-Host "  [ OK ] $m" -ForegroundColor Green }
function Info ($m) { Write-Host "  [INFO] $m" -ForegroundColor DarkGray }
function Warn ($m) { Write-Host "  [WARN] $m" -ForegroundColor Yellow; $script:warn++ }
function Bad  ($m) { Write-Host "  [FAIL] $m" -ForegroundColor Red;    $script:fail++ }
function Manual($m) { $script:manual += $m }
function Step ($m) { Write-Host "  -> $m" -ForegroundColor White }

# Invoke-ChildScript 调用同目录下的纯 PowerShell 子脚本并**如实**判定成败。
#
# 为什么不能用 $LASTEXITCODE 判定这几个子脚本：$LASTEXITCODE 只由**原生可执行文件**
# （或显式调用 exit 的脚本）设置。up.ps1 / setup-jenkins.ps1 / install-agent-service.ps1
# 都是 `$ErrorActionPreference = 'Stop'` + `throw` 的纯 PS 脚本，**从不调 exit**，所以它们
# 返回后 $LASTEXITCODE 仍是上一条原生命令的遗留值（比如本脚本里的 svn checkout，或子脚本
# 内部某次 docker 调用）。双向都会错：
#   - 子脚本明明成功，却因遗留码非 0 被谎报成 [FAIL]；
#   - 子脚本 throw 时异常直接终止 bootstrap，那行 if 根本轮不到执行，汇总与"仍需人工"
#     清单也一并丢失——而这两样正是本脚本存在的意义。
# try/catch 才是与 throw 语义对应的判定方式：异常即失败，计入 $script:fail，并让 bootstrap
# 继续走到汇总段。
#
# 注意：preflight-buildmachine.ps1 结尾确实有 `exit ([int]($script:fail -gt 0))`，
# 客户端 svn checkout 也是原生命令，那两处继续用 $LASTEXITCODE 是正确的，不要一并改掉。
function Invoke-ChildScript {
    param(
        [Parameter(Mandatory)][string] $Name,
        [object[]] $ScriptArgs = @()
    )
    try {
        & (Join-Path $here $Name) @ScriptArgs
        return $true
    } catch {
        Bad "$Name 失败：$($_.Exception.Message)"
        return $false
    }
}

$roles = if ($Role -contains 'All') { @('Dev', 'Build', 'CI') } else { $Role }

Write-Host "Pandora 新机器搭建  $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  $env:COMPUTERNAME" -ForegroundColor White
Write-Host "角色: $($roles -join ' + ')$(if ($WhatIfOnly) { '   [WhatIfOnly：只报告，不改任何东西]' })" -ForegroundColor White
Write-Host "后端仓库: $ProjectRoot"

# ─────────────────────────────────────────────────────────────
Section '1. 前置工具'
# ─────────────────────────────────────────────────────────────
# 每项标注「哪个角色需要」与「缺了会怎样」。缺了会怎样比"需要 X"有用得多——
# 前者能让人判断要不要现在补，后者只会让人照单全收。
$tools = @(
    @{ n = 'pwsh';     id = 'Microsoft.PowerShell';    roles = @('Dev','Build','CI'); why = '所有脚本都要 PowerShell 7，5.1 跑不了' }
    @{ n = 'svn';      id = 'CollabNet.Subversion';    roles = @('Dev','Build','CI'); why = '版本戳靠 svnversion；Jenkins 的 Subversion 插件不提供命令行客户端' }
    @{ n = 'docker';   id = 'Docker.DockerDesktop';    roles = @('Dev','Build','CI'); why = '基础设施、业务镜像、CI 栈全在 docker 里' }
    @{ n = 'go';       id = 'GoLang.Go';               roles = @('Dev','Build');      why = '编后端二进制与镜像' }
    @{ n = 'uv';       id = 'astral-sh.uv';            roles = @('Dev','Build','CI'); why = 'python/ 的虚拟环境与依赖装配；缺了 ci_backend.ps1 的 Python 门禁跑不起来' }
    @{ n = 'java';     id = 'Microsoft.OpenJDK.21';    roles = @('CI');               why = 'Jenkins 构建 agent 是 java 进程' }
    @{ n = 'git';      id = 'Git.Git';                 roles = @('Dev');              why = '个人开发用；CI 已不再需要' }
    @{ n = 'kubectl';  id = 'Kubernetes.kubectl';      roles = @('Dev');              why = 'start.ps1 -Mode k8s / online 需要' }
    @{ n = 'minikube'; id = 'Kubernetes.minikube';     roles = @('Dev');              why = '本地 Agones 真 DS 联调需要' }
)
foreach ($t in $tools) {
    if (-not ($t.roles | Where-Object { $roles -contains $_ })) { continue }
    $c = Get-Command $t.n -ErrorAction SilentlyContinue
    if ($c) { Ok "$($t.n)  $($c.Source)"; continue }

    if ($Install -and -not $WhatIfOnly) {
        if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
            Bad "缺 $($t.n)，且本机没有 winget —— 手工安装：$($t.id)。用途：$($t.why)"
            continue
        }
        Step "winget 安装 $($t.id) ..."
        & winget install --id $t.id --silent --accept-source-agreements --accept-package-agreements | Out-Null
        if (Get-Command $t.n -ErrorAction SilentlyContinue) { Ok "$($t.n) 已安装" }
        else { Warn "$($t.n) 装完仍不在 PATH —— 多半要重开终端；重开后再跑一次本脚本" }
    } else {
        Bad "缺 $($t.n) —— $($t.why)$(if (-not $Install) { "（加 -Install 自动装，或手工装 $($t.id)）" })"
    }
}

if ($PSVersionTable.PSVersion.Major -lt 7) {
    Bad "当前是 PowerShell $($PSVersionTable.PSVersion) —— 必须用 pwsh 重跑本脚本"
}

# ─────────────────────────────────────────────────────────────
Section '2. 机器级环境变量'
# ─────────────────────────────────────────────────────────────
# 同时写进程与用户级：进程级让本次运行的后续步骤立刻可用（持久化写入不影响当前进程），
# 用户级让重开终端和 Jenkins agent 计划任务（S4U，跑在当前用户下）也能读到。
#
# 必须查 User + Machine 两个作用域再判断：变量可能是别人用管理员在 Machine 级设的
# （本机 LINUX_MULTIARCH_ROOT 就是），只查 User 会误报"未设"，然后写一份多余的
# User 级副本 —— 两处不同值时排查起来很费劲。已生效且值相同就什么都不做。
function Get-EffectiveEnv {
    param([string]$Name)
    $u = [Environment]::GetEnvironmentVariable($Name, 'User')
    if ($u) { return @{ Value = $u; Scope = 'User' } }
    $m = [Environment]::GetEnvironmentVariable($Name, 'Machine')
    if ($m) { return @{ Value = $m; Scope = 'Machine' } }
    return @{ Value = $null; Scope = $null }
}
function Set-MachineEnv {
    param([string]$Name, [string]$Value, [string]$Why)
    $cur = Get-EffectiveEnv $Name
    if ($cur.Value -eq $Value) {
        Ok "$Name = $Value（已在 $($cur.Scope) 级设为该值，不动）"
        Set-Item "env:$Name" $Value
        return
    }
    $curDesc = if ($cur.Value) { "$($cur.Value)  [$($cur.Scope) 级]" } else { '未设' }
    if ($WhatIfOnly) { Info "将设置 $Name = $Value（当前：$curDesc）"; return }
    [Environment]::SetEnvironmentVariable($Name, $Value, 'User')
    Set-Item "env:$Name" $Value
    if ($cur.Scope -eq 'Machine') {
        Warn "$Name 已写入 User 级 = $Value，但 Machine 级仍是 $($cur.Value)。User 级优先生效；要彻底统一请用管理员改 Machine 级或删掉它。"
    } else {
        Ok "$Name = $Value$(if ($cur.Value) { "（原值 $($cur.Value)）" })   $Why"
    }
}

if (-not $ArtifactRoot) {
    $existing = (Get-EffectiveEnv 'PANDORA_ARTIFACT_ROOT').Value
    # 默认取**仓库的父目录**下的 artifacts，不是盘符根。这台机器上仓库在 F:\work\XuanMing-Server，
    # 父目录 F:\work → F:\work\artifacts，正好是各脚本硬编码兜底用的那个路径；
    # 取盘符根会得到 F:\artifacts，一个不存在的新目录，制品解析会全部落空。
    $ArtifactRoot = if ($existing) { $existing } else { Join-Path (Split-Path $ProjectRoot -Parent) 'artifacts' }
}
# URL 形态是最难排查的一类错：发布脚本用文件系统 API，Test-Path 直接 False，
# 表现为"找不到任何制品"而不是报错。这里直接拒。
if ($ArtifactRoot -match '^[a-z]+://') {
    Bad "制品根不能是 URL：$ArtifactRoot（发布脚本用文件系统 API，URL 会静默失效）。用本地路径或 SMB UNC。"
} else {
    Set-MachineEnv 'PANDORA_ARTIFACT_ROOT' $ArtifactRoot '制品库根'
    if (-not (Test-Path -LiteralPath $ArtifactRoot)) {
        if ($WhatIfOnly) { Info "将创建 $ArtifactRoot" }
        else { New-Item -ItemType Directory -Path $ArtifactRoot -Force | Out-Null; Ok "已创建 $ArtifactRoot" }
    }
}

Set-MachineEnv 'PANDORA_PROTO_SERVER_ROOT' $ProjectRoot '客户端流水线按它找后端仓库'

if ($roles -contains 'Build') {
    # 工具链位置因机器而异，所以判据不是路径而是**里面有没有交叉编译器本体**。
    # LINUX_MULTIARCH_ROOT 指向的是版本目录本身（里面直接是 x86_64-unknown-linux-gnu 等目标三元组），
    # 不是 UnrealToolchains 那一层 —— 指错一层不会报错，只会在链接 Linux DS 时才炸。
    function Find-LinuxToolchain {
        param([string[]]$Hints)
        $probe = 'x86_64-unknown-linux-gnu\bin\clang++.exe'
        # 先验显式给的 / 已设的
        foreach ($h in $Hints) {
            if ($h -and (Test-Path -LiteralPath (Join-Path $h $probe))) { return (Get-Item -LiteralPath $h).FullName }
        }
        # 再扫各盘的 UnrealToolchains\<版本>；同名多版本取字典序最大（v26 > v25）
        $found = foreach ($d in (Get-PSDrive -PSProvider FileSystem -ErrorAction SilentlyContinue).Name) {
            $r = "${d}:\UnrealToolchains"
            if (Test-Path -LiteralPath $r) {
                Get-ChildItem -LiteralPath $r -Directory -ErrorAction SilentlyContinue |
                    Where-Object { Test-Path -LiteralPath (Join-Path $_.FullName $probe) }
            }
        }
        ($found | Sort-Object Name -Descending | Select-Object -First 1)?.FullName
    }

    $tc = Find-LinuxToolchain -Hints @($ToolchainRoot, $env:LINUX_MULTIARCH_ROOT, (Get-EffectiveEnv 'LINUX_MULTIARCH_ROOT').Value)
    if ($tc) {
        # UE 读这个变量时按目录拼路径，末尾带不带反斜杠都行；沿用已设值的写法避免无谓改写。
        $existingTc = (Get-EffectiveEnv 'LINUX_MULTIARCH_ROOT').Value
        $target = if ($existingTc -and $existingTc.TrimEnd('\') -eq $tc.TrimEnd('\')) { $existingTc } else { "$tc\" }
        Set-MachineEnv 'LINUX_MULTIARCH_ROOT' $target 'Linux DS 交叉编译'
    } elseif ($ToolchainRoot) {
        Bad "-ToolchainRoot 指的目录里没有交叉编译器（缺 x86_64-unknown-linux-gnu\bin\clang++.exe）：$ToolchainRoot"
    } else {
        Warn '没找到 Linux 交叉编译工具链 —— 打 Linux DS 会直接 throw（只跑后端服务不受影响）'
        Manual '装 UE 官方 Linux 交叉编译工具链（与引擎版本匹配，本项目当前 v26_clang-20.1.8-rockylinux8，约 3.3GB），或用 -ToolchainRoot <路径> 指向已有的那份'
    }
}

# ─────────────────────────────────────────────────────────────
Section '3. 源码工作副本'
# ─────────────────────────────────────────────────────────────
# 后端：本脚本就在里面，校验 origin 是否与 backend-dev 的 Git 权威源一致。
function Normalize-GitRemote([string]$url) {
    if (-not $url) { return '' }
    return (($url.Trim() -replace '\\', '/') -replace '/+$', '' -replace '\.git$', '').ToLowerInvariant()
}
$gitTop = if (Get-Command git -ErrorAction SilentlyContinue) {
    (& git -C $ProjectRoot rev-parse --show-toplevel 2>$null | Out-String).Trim()
} else { '' }
if ($gitTop) {
    $actual = (& git -C $ProjectRoot remote get-url origin 2>$null | Out-String).Trim()
    if ((Normalize-GitRemote $actual) -eq (Normalize-GitRemote $BackendGitUrl)) {
        Ok "后端 Git origin = $actual"
    } else {
        Warn "后端 Git origin 指向 $actual，期望 $BackendGitUrl —— 本机发布与 backend-dev 可能构建不同代码"
    }
} else {
    $legacySvn = if (Get-Command svn -ErrorAction SilentlyContinue) { (svn info $ProjectRoot 2>$null | Out-String) } else { '' }
    if ($legacySvn -match 'URL:\s*(\S+)') {
        Warn "后端仍是旧 SVN 工作副本 $($Matches[1])；backend-dev 已切到 $BackendGitUrl，版本戳应为 g<sha>"
    } else {
        Bad "后端不是 git 仓库：$ProjectRoot"
    }
}

# 客户端：默认只报告。89GB 级别的检出不该被"一键"静默触发。
if (-not $ClientRepo) { $ClientRepo = Join-Path (Split-Path $ProjectRoot -Parent) 'Pandora-Client-SVN' }
$expectClient = "$SvnBase/trunk/Client"
if (Test-Path -LiteralPath (Join-Path $ClientRepo '.svn')) {
    Ok "客户端工作副本 = $ClientRepo"
} elseif ($CheckoutClient -and -not $WhatIfOnly) {
    Step "检出客户端到 $ClientRepo（很大，按内网带宽预留时间）..."
    & svn checkout $expectClient $ClientRepo
    if ($LASTEXITCODE -eq 0) { Ok "客户端已检出" } else { Bad "客户端检出失败（exit=$LASTEXITCODE）" }
} else {
    Warn "客户端工作副本不存在：$ClientRepo"
    Manual "检出客户端（很大，建议单独跑并预留时间）： svn checkout $expectClient $ClientRepo`n   或重跑本脚本加 -CheckoutClient"
}

if ($roles -contains 'Build') {
    # 引擎按**注册表**判定，不按路径：每台机器装在哪都行，UBT 认的是 HKCU 里注册的
    # 那份自制引擎。只要注册了就不用管它在哪个盘。
    # 不注册的后果不是"找不到"，而是静默退到 Epic Launcher 发行版，然后 Server 目标报
    # "Server targets are not currently supported from this engine distribution" —— 症状离根因很远。
    $bk = 'HKCU:\SOFTWARE\Epic Games\Unreal Engine\Builds'
    $engines = @()
    if (Test-Path $bk) {
        $props = Get-ItemProperty $bk
        $engines = $props.PSObject.Properties |
            Where-Object { $_.Name -notlike 'PS*' -and $_.Value -and (Test-Path -LiteralPath $_.Value) } |
            ForEach-Object { $_.Value }
    }
    if ($engines) {
        Ok "已注册源码引擎 $($engines.Count) 个（路径无关，UBT 按注册表找）："
        $engines | ForEach-Object { Write-Host "         $_" -ForegroundColor DarkGray }
    } else {
        Warn '未注册任何源码引擎 —— Server/Linux 目标会静默退到 Epic 发行版并编译失败'
        Manual '已有引擎只需注册一次： <引擎根>\Engine\Binaries\Win64\UnrealVersionSelector.exe /register（写 HKCU，装在哪个盘都行）；机器上没有引擎才需要先 clone 那份约 51GB 的已编译 installed build'
    }
}

# ─────────────────────────────────────────────────────────────
if ($roles -contains 'CI') {
    Section '4. CI 栈（Jenkins + registry + MinIO）'
    if ($script:fail -gt 0) {
        Warn "前面有 $($script:fail) 项阻断，跳过 CI 栈启动（先把 FAIL 补齐）"
    } elseif ($WhatIfOnly) {
        Info '将执行： up.ps1（首次会拉镜像+装插件，约几分钟）'
    } else {
        Step 'up.ps1 ...'
        if (Invoke-ChildScript -Name 'up.ps1') { Ok 'CI 栈已启动' }
    }

    Section '5. Jenkins 接线'
    if ($script:fail -gt 0) {
        Warn '前面有阻断项，跳过 Jenkins 接线'
    } elseif ($WhatIfOnly) {
        Info '将执行： setup-jenkins.ps1 -SkipAgent  然后  install-agent-service.ps1'
    } else {
        Step 'setup-jenkins.ps1 -SkipAgent（建节点与三个任务）...'
        # -SkipAgent：这里只建配置，agent 交给下面的计划任务常驻拉起。
        # setup-jenkins 自己拉的 agent 是普通进程，注销/重启就没了，构建会卡在
        # "Timeout waiting for agent to come back"（实际发生过）。
        # 嵌套而非并列：本文件开头的顺序说明已写明「install-agent-service 要节点已经建好」。
        # setup-jenkins 失败时节点就没建出来，再去注册 agent 计划任务只会叠一条更难读的
        # 二次失败，掩盖真正的首因。
        if (Invoke-ChildScript -Name 'setup-jenkins.ps1' -ScriptArgs @('-SkipAgent')) {
            Step 'install-agent-service.ps1（agent 注册为开机自启计划任务）...'
            if (Invoke-ChildScript -Name 'install-agent-service.ps1') {
                Start-ScheduledTask -TaskName 'PandoraJenkinsAgent' -ErrorAction SilentlyContinue
                Ok 'agent 计划任务已注册并启动'
            }
        }
    }
    Manual 'Jenkins 里建 SVN 凭据：界面 → Manage Jenkins → Credentials → 新建 Username/Password，ID 必须与 .env 的 SVN_CREDENTIALS_ID 一致。口令只能你本人输，脚本不碰。'
}

# ─────────────────────────────────────────────────────────────
if ($roles -contains 'Build') {
    Section '6. 构建机自检'
    if ($WhatIfOnly) {
        Info "将执行： preflight-buildmachine.ps1 -ClientRepo $ClientRepo"
    } else {
        & (Join-Path $here 'preflight-buildmachine.ps1') -ClientRepo $ClientRepo
        if ($LASTEXITCODE -ne 0) { Warn 'preflight 有阻断项 —— 见上面 FAIL，补齐后再打包' }
    }
}

# ─────────────────────────────────────────────────────────────
Section '汇总'
# ─────────────────────────────────────────────────────────────
if ($script:fail -eq 0 -and $script:warn -eq 0) { Write-Host '脚本能做的部分全部完成。' -ForegroundColor Green }
elseif ($script:fail -eq 0) { Write-Host "无阻断项，$($script:warn) 条提醒。" -ForegroundColor Yellow }
else { Write-Host "$($script:fail) 项阻断 / $($script:warn) 条提醒 —— 补齐 FAIL 后重跑本脚本（幂等，可以反复跑）。" -ForegroundColor Red }

if ($script:manual.Count -gt 0) {
    Write-Host "`n仍需人工完成（脚本刻意不做）：" -ForegroundColor Yellow
    $i = 1
    foreach ($m in $script:manual) { Write-Host "  $i. $m" -ForegroundColor Yellow; $i++ }
}

Write-Host @"

设过环境变量的话，**重开终端**再跑下面的命令（当前会话已注入，新开的窗口才读得到用户级变量）。

  跑后端服务      start.cmd                       （双击也行；k8s 真 DS 用 内网服务器一键启动-k8s集群.cmd）
  打包 → 发布     见 tools\devops\BUILD-MACHINE-SETUP.md 第 6 节
  CI              http://localhost:8080           （口令在 tools\devops\.env）
"@ -ForegroundColor DarkGray

exit ([int]($script:fail -gt 0))
