<#
.SYNOPSIS
  从服务端配置表产物生成 UE DataTable 导入用 CSV(cfgequipattr)。

.DESCRIPTION
  客户端的 cfgequipattr.uasset 需要一份 CSV 才能**首次创建**。这份 CSV **不入库**:
  历史上手工导出的 CSV 留在仓库里会变成陈旧副本(CfgChestPoint.csv 就踩过——
  排查时被当成现状读,实际早已与 xlsx 不符)。所以这里每次现生成、用完即弃,
  唯一源头始终是策划的 装备属性表.xlsx。

  与另外三份效果表(talent_effect / skill_card_effect / attr_point_effect)的区别:
  **装备基础属性表已经接进了客户端导表流水线**(Tool/Table/Cs/Proto/装备属性表.json),
  所以资产建好之后,日常改表走编辑器的「生成所有配置」(Alt+Y)即可,不必再跑本脚本。
  本脚本只解决"资产还不存在、需要一次性建出来"这一件事。

  首次创建(结构是新加的 C++ USTRUCT,必须先编译出 FCfgEquipAttr 才能建表):
    1. 编译客户端 → 打开编辑器
    2. pwsh tools/scripts/equip_attr_csv.ps1        # 生成 CSV(路径见输出)
    3. Content/Pkg/Cfg/Table/Cpp 下 Import CSV,行结构选 CfgEquipAttr
    4. 资产名必须是 cfgequipattr(全小写):UCfgSystem::Load 按结构名 ToLower()
       拼路径去 LoadObject,Linux DS 的 cook 包对大小写敏感,不一致会当成"表不存在"。

  之后改表:编辑器 Alt+Y「生成所有配置」→ 自动重填 cfgequipattr;
  服务端侧跑 tools/scripts/configtable_gen.ps1 重出 dist。

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
$DistJson   = Join-Path $ServerRoot 'configtable\dist\equipment_attr.json'

if (-not (Test-Path $DistJson)) {
    Write-Error "找不到 $DistJson。先跑 tools/scripts/configtable_gen.ps1 把 装备属性表.xlsx 导成 dist。"
}

if (-not $OutPath) {
    $OutPath = Join-Path ([System.IO.Path]::GetTempPath()) 'cfgequipattr.csv'
}

$rows = (Get-Content -Raw -Encoding UTF8 $DistJson | ConvertFrom-Json).rows
if (-not $rows) {
    Write-Error "$DistJson 里没有任何行,不生成空表(空表会让所有装备静默失去基础属性)。"
}

# protojson 按 proto3 规范**省略零值字段**:品质为 0、治疗为 0 的行在 JSON 里根本没有那个 key。
# 直接 $r.quality 在 StrictMode 下会抛"property cannot be found",所以一律走这个取值器。
function Get-RowValue([object]$Row, [string]$Name) {
    $prop = $Row.PSObject.Properties[$Name]
    if ($null -eq $prop) { return $null }
    return $prop.Value
}

# 6 个百分比列是 float。用 InvariantCulture 格式化:中文/德文区域会输出逗号小数点,
# 那会把一列劈成两列,且 UE 导入时不报错、只是数值静默错位。
function Format-Rate([object]$Value) {
    if ($null -eq $Value) { return '0' }
    return [System.Convert]::ToDouble($Value).ToString([System.Globalization.CultureInfo]::InvariantCulture)
}

function Format-Int([object]$Value) {
    if ($null -eq $Value) { return 0 }
    return [int]$Value
}

# CSV 字段里出现逗号 / 引号会劈列。装备名与描述是策划自由文本,必须转义。
function Escape-Csv([object]$Value) {
    $text = if ($null -eq $Value) { '' } else { [string]$Value }
    if ($text -match '[",\r\n]') {
        return '"' + $text.Replace('"', '""') + '"'
    }
    return $text
}

# 首列是 DataTable 行名,取 ID(与 FCfgItem 同口径,运行时按 FName(FString::FromInt(id)) 查行);
# 其余列名必须与 FCfgEquipAttr 的 UPROPERTY 同名。
# ⚠️ 名称列叫 EquipName 而不是 Name:UE 的 DataTable CSV 导入会把 Name 当行名吃掉。
$lines = New-Object System.Collections.Generic.List[string]
$lines.Add('Name,Id,EquipName,Quality,EquipSlot,Icon,HpRate,DamageRate,HealRate,CritRate,SkillRate,ControlRate,Description')
foreach ($r in $rows) {
    $lines.Add(('{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12}' -f `
        (Get-RowValue $r 'id'), `
        (Get-RowValue $r 'id'), `
        (Escape-Csv (Get-RowValue $r 'name')), `
        (Format-Int  (Get-RowValue $r 'quality')), `
        (Format-Int  (Get-RowValue $r 'equip_slot')), `
        (Escape-Csv  (Get-RowValue $r 'icon')), `
        (Format-Rate (Get-RowValue $r 'hp_rate')), `
        (Format-Rate (Get-RowValue $r 'damage_rate')), `
        (Format-Rate (Get-RowValue $r 'heal_rate')), `
        (Format-Rate (Get-RowValue $r 'crit_rate')), `
        (Format-Rate (Get-RowValue $r 'skill_rate')), `
        (Format-Rate (Get-RowValue $r 'control_rate')), `
        (Escape-Csv  (Get-RowValue $r 'description'))))
}

# UE 的 CSV 导入按 UTF-8 读;带 BOM 会让首列名变成 "﻿Name" 而认不出行名列。
[System.IO.File]::WriteAllLines($OutPath, $lines, [System.Text.UTF8Encoding]::new($false))

Write-Host "[OK] 已生成 $($rows.Count) 行 → $OutPath"
Write-Host "     在 UE 编辑器里 Import CSV 建出 Content/Pkg/Cfg/Table/Cpp/cfgequipattr(行结构 CfgEquipAttr,资产名全小写)。"
Write-Host "     建好之后日常改表走编辑器「生成所有配置」即可,不必再跑本脚本。"
