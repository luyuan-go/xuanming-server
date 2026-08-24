<#
.SYNOPSIS
  从服务端配置表产物生成 UE DataTable 导入用 CSV(cfgtalenteffect)。

.DESCRIPTION
  客户端的 cfgtalenteffect.uasset 需要一份 CSV 才能导入。这份 CSV **不入库**:
  历史上手工导出的 CSV 留在仓库里会变成陈旧副本(CfgChestPoint.csv 就踩过——
  排查时被当成现状读,实际早已与 xlsx 不符)。所以这里每次现生成、用完即弃,
  唯一源头始终是策划的 z_专精_效果.xlsx → configtable/dist/talent_effect.json。

  做法与 attr_point_effect_csv.ps1 逐条对齐,三份效果表的导入流程不该各有一套规矩。

  用法(改完表之后):
    1. pwsh tools/scripts/configtable_gen.ps1         # 先把 xlsx 导成 dist
    2. pwsh tools/scripts/talent_effect_csv.ps1   # 再生成 CSV
    3. 在 UE 编辑器里把 CSV 重导入 cfgtalenteffect 数据表(见下方"首次创建")

  首次创建(结构是新加的 C++ USTRUCT,必须先编译出 FCfgTalentEffect 才能建表):
    - 编译客户端 → 打开编辑器
    - Content/Pkg/Cfg/Table/Cpp 下 Import CSV,行结构选 CfgTalentEffect
    - 资产名必须是 cfgtalenteffect(全小写):UCfgSystem::Load 按
      结构名 ToLower() 拼路径去 LoadObject,大小写不符会当成"表不存在"。

.PARAMETER OutPath
  CSV 输出路径。默认写到系统临时目录,避免误提交进仓库。
#>
param(
    [string]$OutPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

try { [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false) } catch { }

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$ServerRoot = [System.IO.Path]::GetFullPath((Join-Path $ScriptDir '..\..'))
$DistJson   = Join-Path $ServerRoot 'configtable\dist\talent_effect.json'

if (-not (Test-Path $DistJson)) {
    Write-Error "找不到 $DistJson。先跑 tools/scripts/configtable_gen.ps1 把 z_专精_效果.xlsx 导成 dist。"
}

if (-not $OutPath) {
    $OutPath = Join-Path ([System.IO.Path]::GetTempPath()) 'cfgtalenteffect.csv'
}

$rows = (Get-Content -Raw -Encoding UTF8 $DistJson | ConvertFrom-Json).rows
if (-not $rows) {
    Write-Error "$DistJson 里没有任何行,不生成空表(空表会让所有天赋静默失去战斗加成)。"
}

# 首列是 DataTable 行名,取 ID;其余列名必须与 FCfgTalentEffect 的 UPROPERTY 同名。
$lines = New-Object System.Collections.Generic.List[string]
$lines.Add('Name,Id,TalentId,AttrKey,ValuePerLevel')
foreach ($r in $rows) {
    # value_per_level 是 float,用 InvariantCulture 格式化:中文/德文区域会输出逗号小数点,
    # 那会把一列劈成两列,且 UE 导入时不报错只是数值错位。
    $v = [System.Convert]::ToDouble($r.value_per_level).ToString([System.Globalization.CultureInfo]::InvariantCulture)
    $lines.Add(('{0},{1},{2},{3},{4}' -f $r.id, $r.id, $r.talent_id, $r.attr_key, $v))
}

# UE 的 CSV 导入按 UTF-8 读;带 BOM 会让首列名变成 "﻿Name" 而认不出行名列。
[System.IO.File]::WriteAllLines($OutPath, $lines, [System.Text.UTF8Encoding]::new($false))

Write-Host "[OK] 已生成 $($rows.Count) 行 → $OutPath"
Write-Host "     在 UE 编辑器里重导入 Content/Pkg/Cfg/Table/Cpp/cfgtalenteffect(行结构 CfgTalentEffect)。"
