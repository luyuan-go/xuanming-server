<#
.SYNOPSIS
  无头重建三张养成效果表的 UE DataTable 资产(专精 / 属性加点 / 技能卡)。

.DESCRIPTION
  「改表 → 联机生效」全链里客户端资产这一环,全部命令行完成,不需要打开图形编辑器。

    1. pwsh tools/scripts/configtable_gen.ps1    # 策划 xlsx → configtable/dist/*.json
    2. pwsh tools/scripts/ue_effect_tables.ps1   # dist → 客户端 DataTable 资产(本脚本)

  资产不存在会按对应 C++ 行结构自动新建,因此**首次落地也走这一条**,不必手工 Import CSV
  (手工路线仍保留:见 talent_effect_csv.ps1 / attr_point_effect_csv.ps1 / skill_card_effect_csv.ps1)。

  两条硬前置,脚本会先自检并拒绝继续:
    - **图形编辑器必须关掉**。无头写盘会被在跑的编辑器内存态覆盖,表现为"跑成功了但资产没变"。
    - **客户端 C++ 必须先编译过**(Tool/Build/BuildEditor.bat)。FCfgSkillCardEffect /
      FCfgAttrPointEffect 是新增 USTRUCT,旧编辑器二进制里根本没有这两个行结构,
      新建资产会失败;编辑器启动时也会卡在 "Missing Pandora Modules" 弹窗上。

.PARAMETER ClientRoot
  客户端仓库根目录。默认按后端仓平级的 Pandora-Client-SVN 探测。

.PARAMETER Engine
  引擎根目录。默认用客户端仓的 Tool/Build/_ResolveEngine.ps1 按 .uproject 的
  EngineAssociation 解析(与 BuildEditor.bat 同一条路径,避免两份引擎选择逻辑漂移)。
#>
param(
    [string]$ClientRoot = '',
    [string]$Engine = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

try { [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false) } catch { }

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$ServerRoot = [System.IO.Path]::GetFullPath((Join-Path $ScriptDir '..\..'))

if (-not $ClientRoot) {
    $ClientRoot = [System.IO.Path]::GetFullPath((Join-Path $ServerRoot '..\Pandora-Client-SVN'))
}
if (-not (Test-Path -LiteralPath $ClientRoot)) {
    Write-Error "找不到客户端仓库 $ClientRoot,用 -ClientRoot 指定。"
}
$Project = Join-Path $ClientRoot 'Pandora\Pandora.uproject'
if (-not (Test-Path -LiteralPath $Project)) {
    Write-Error "找不到 $Project。"
}

# 前置 1:图形编辑器必须关掉,否则无头写的盘会被它的内存态覆盖(静默,最难查)。
$running = @(Get-Process -Name 'UnrealEditor' -ErrorAction SilentlyContinue)
if ($running.Count -gt 0) {
    $ids = ($running | ForEach-Object { $_.Id }) -join ', '
    Write-Error "检测到 UnrealEditor 正在运行(PID $ids)。先关掉再跑:无头写盘会被在跑的编辑器覆盖。"
}

# 前置 2:dist 必须已产出。
$DistDir = Join-Path $ServerRoot 'configtable\dist'
foreach ($f in @('talent_effect.json', 'attr_point_effect.json', 'skill_card_effect.json')) {
    if (-not (Test-Path -LiteralPath (Join-Path $DistDir $f))) {
        Write-Error "缺 $DistDir\$f。先跑 tools/scripts/configtable_gen.ps1。"
    }
}

if (-not $Engine) {
    $resolver = Join-Path $ClientRoot 'Tool\Build\_ResolveEngine.ps1'
    if (-not (Test-Path -LiteralPath $resolver)) {
        Write-Error "找不到 $resolver,用 -Engine 指定引擎根目录。"
    }
    $Engine = (& pwsh -NoProfile -File $resolver -Project $Project) | Select-Object -Last 1
}
$EditorCmd = Join-Path $Engine 'Engine\Binaries\Win64\UnrealEditor-Cmd.exe'
if (-not (Test-Path -LiteralPath $EditorCmd)) {
    Write-Error "找不到 $EditorCmd(引擎根目录 = $Engine)。"
}

$PyScript = Join-Path $ScriptDir 'ue_effect_tables.py'
$env:PANDORA_CONFIGTABLE_DIST = $DistDir

# 成败靠结果文件而不是 stdout 哨兵行:unreal.log() 是 Log 级,-stdout 只放 Display 及以上,
# 哨兵行根本到不了控制台(实测三张表全建好了却被判成失败)。先删旧文件,免得读到上一次的。
$ResultFile = Join-Path ([System.IO.Path]::GetTempPath()) 'pandora_effect_tables_result.txt'
if (Test-Path -LiteralPath $ResultFile) { Remove-Item -LiteralPath $ResultFile -Force }
$env:PANDORA_EFFECT_TABLES_RESULT = $ResultFile

Write-Host "[..] 引擎:   $Engine"
Write-Host "[..] 工程:   $Project"
Write-Host "[..] dist:   $DistDir"
Write-Host "[..] 无头重建 cfgtalenteffect / cfgattrpointeffect / cfgskillcardeffect ..."

# -unattended -nopause -nosplash:无人值守,任何弹窗都直接失败而不是挂住等人点。
$out = & $EditorCmd $Project -run=pythonscript -script="$PyScript" `
    -unattended -nopause -nosplash -stdout -utf8output 2>&1
$out | ForEach-Object { $_ }

# 退出码在 pythonscript commandlet 下不可靠(异常常被吞成 0),按结果文件判定。
$result = if (Test-Path -LiteralPath $ResultFile) { (Get-Content -Raw -Encoding UTF8 $ResultFile).Trim() } else { '' }
if ($result -notmatch 'EFFECT_TABLES_RESULT: OK') {
    $hint = if ($result) { $result } else { '脚本没写出结果文件(可能没跑到 main,或编辑器启动就失败了)' }
    $logPath = Join-Path $ClientRoot 'Pandora\Saved\Logs\Pandora.log'
    Write-Error "重建失败:$hint`n逐表进度在 $logPath 的 [EffectTables] 行(Log 级,不进 stdout);导入问题看 LogCSVImportFactory。"
}
Write-Host ''
Write-Host "[..] $result"

Write-Host ''
Write-Host '[OK] 三张效果表资产已重建并存盘。记得 svn add / commit:'
Write-Host '     Pandora\Content\Pkg\Cfg\Table\Cpp\cfg{talent,attrpoint,skillcard}effect.uasset'
