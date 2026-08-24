# -*- coding: utf-8 -*-
"""无头重建三张养成效果表的 UE DataTable 资产(专精 / 属性加点 / 技能卡)。

由 tools/scripts/ue_effect_tables.ps1 通过
    UnrealEditor-Cmd.exe <proj> -run=pythonscript -script=<本文件> -unattended ...
拉起,**不需要打开图形编辑器**(但必须先关掉它:无头写盘会被在跑的编辑器内存态覆盖)。

做什么:
  1. 资产不存在就按对应 C++ 行结构新建(资产名全小写 —— UCfgSystem::Load 按
     结构名 ToLower() 拼路径去 LoadObject,大小写不符会被当成"表不存在");
  2. 用服务端 configtable/dist/*.json 整表覆盖(fill_data_table_from_json_string
     是**整表替换**,必须喂全部行);
  3. 存盘并逐表核对行数。

为什么数据从服务端 dist 取而不是从 xlsx 直接读:dist 是导表器的唯一产物,已经过
外键 / 白名单 / 跨行重复三道校验。从 xlsx 另起一条解析路径 = 第二份实现,迟早漂移。

⚠️ 行键字段:本仓这几张表的 import_key_field 都为空,所以 JSON 里行名字段固定叫 "Name"。
⚠️ 列名必须是 **C++ 原始属性名**(不是中文 DisplayName):引擎
   DataTableUtils::GetPropertyImportNames 两者都收,但原始名不会随 DisplayName 改动失效。
"""

import json
import os
import sys
import traceback

import unreal

# 服务端配置表产物根目录。允许用环境变量覆盖(CI / 别的盘符)。
DIST_DIR = os.environ.get(
    "PANDORA_CONFIGTABLE_DIST", r"F:\work\XuanMing-Server\configtable\dist"
)
# 客户端 Cfg 表资产目录(与 DefaultMyDataTableSetting.ini 的 CfgCppPkgPath 一致)。
PKG_PATH = "/Game/Pkg/Cfg/Table/Cpp"

# 一张表 = (dist 文件名, 资产名全小写, C++ 行结构, {dist 字段 → C++ 属性名})。
TABLES = [
    (
        "talent_effect.json",
        "cfgtalenteffect",
        "/Script/Pandora.CfgTalentEffect",
        {"id": "Id", "talent_id": "TalentId", "attr_key": "AttrKey",
         "value_per_level": "ValuePerLevel"},
    ),
    (
        "attr_point_effect.json",
        "cfgattrpointeffect",
        "/Script/Pandora.CfgAttrPointEffect",
        {"id": "Id", "attr_point_key": "AttrPointKey", "attr_key": "AttrKey",
         "value_per_point": "ValuePerPoint"},
    ),
    (
        "skill_card_effect.json",
        "cfgskillcardeffect",
        "/Script/Pandora.CfgSkillCardEffect",
        {"id": "Id", "card_id": "CardId", "attr_key": "AttrKey",
         "value_per_level": "ValuePerLevel"},
    ),
]

RESULT_TAG = "EFFECT_TABLES_RESULT"
# 成败判定用的结果文件。
#
# 为什么不靠 stdout 的哨兵行:unreal.log() 是 **Log 级**,`-stdout` 只放 Display 及以上,
# 所以包装脚本在控制台里根本看不到它 —— 实测三张表都建好了、日志里明明白白写着 OK,
# 包装脚本仍判成失败(假阴性)。把成败落到文件上,不依赖任何日志级别与过滤规则。
# (改用 log_warning 把哨兵抬到 Display 也能到 stdout,但那是拿日志级别当传参用。)
RESULT_FILE = os.environ.get("PANDORA_EFFECT_TABLES_RESULT", "")


def log(msg):
    """进度日志。Log 级,只进 Pandora.log,不进 stdout —— 成败判定不要依赖它。"""
    unreal.log("[EffectTables] {0}".format(msg))


def write_result(text):
    """把最终成败写进结果文件(路径由包装脚本经环境变量给)。"""
    if not RESULT_FILE:
        return
    try:
        with open(RESULT_FILE, "w", encoding="utf-8") as fp:
            fp.write(text)
    except OSError as exc:  # 写不了就只能靠日志,但要留痕
        unreal.log_error("[EffectTables] 结果文件写入失败 {0}: {1}".format(RESULT_FILE, exc))


def load_rows(dist_file, field_map):
    """读 dist json 并翻成 fill_data_table_from_json_string 要的行数组。"""
    path = os.path.join(DIST_DIR, dist_file)
    if not os.path.isfile(path):
        raise RuntimeError("找不到 {0};先跑 tools/scripts/configtable_gen.ps1".format(path))
    with open(path, "r", encoding="utf-8") as fp:
        rows = json.load(fp).get("rows") or []
    if not rows:
        # 空表会让整条养成线静默失去加成,和"表没导"长得一模一样,不允许静默通过。
        raise RuntimeError("{0} 没有任何行,拒绝用空表覆盖资产".format(path))
    out = []
    for row in rows:
        item = {"Name": str(row["id"])}  # import_key_field 为空 → 行名字段是 Name
        for src, dst in field_map.items():
            item[dst] = row.get(src, 0)
        out.append(item)
    return out


def ensure_asset(asset_name, struct_path):
    """资产不存在就按行结构新建;返回已加载的 DataTable。"""
    asset_path = "{0}/{1}".format(PKG_PATH, asset_name)
    if unreal.EditorAssetLibrary.does_asset_exist(asset_path):
        table = unreal.EditorAssetLibrary.load_asset(asset_path)
        if table is None:
            raise RuntimeError("{0} 存在却加载不出来(资产损坏?)".format(asset_path))
        return table

    row_struct = unreal.load_object(None, struct_path)
    if row_struct is None:
        # 行结构是新增的 C++ USTRUCT:编辑器二进制没重编时必然拿不到。
        raise RuntimeError(
            "行结构 {0} 不存在 —— 先编译客户端(Tool/Build/BuildEditor.bat)再跑本脚本".format(struct_path)
        )
    factory = unreal.DataTableFactory()
    factory.set_editor_property("struct", row_struct)
    table = unreal.AssetToolsHelpers.get_asset_tools().create_asset(
        asset_name, PKG_PATH, unreal.DataTable, factory
    )
    if table is None:
        raise RuntimeError("新建 {0} 失败".format(asset_path))
    log("已新建资产 {0}(行结构 {1})".format(asset_path, struct_path))
    return table


def rebuild_one(dist_file, asset_name, struct_path, field_map):
    rows = load_rows(dist_file, field_map)
    table = ensure_asset(asset_name, struct_path)

    # 返回的是 **bool** 不是问题列表(len() 会 TypeError);逐条问题只打在
    # LogCSVImportFactory 的 "Imported DataTable 'x' - N Problems" 里。
    ok = unreal.DataTableFunctionLibrary.fill_data_table_from_json_string(
        table, json.dumps(rows, ensure_ascii=False)
    )
    if not ok:
        raise RuntimeError("{0} 填表失败(看 LogCSVImportFactory 的 Problems 行)".format(asset_name))

    asset_path = "{0}/{1}".format(PKG_PATH, asset_name)
    if not unreal.EditorAssetLibrary.save_asset(asset_path):
        raise RuntimeError("{0} 存盘失败".format(asset_path))

    # 行数核对:填表 API 对个别坏行是"跳过 + 记 Problem"而不是整体失败,
    # 不数一遍就可能把"少了几行"当成成功。
    actual = len(unreal.DataTableFunctionLibrary.get_data_table_row_names(table))
    if actual != len(rows):
        raise RuntimeError(
            "{0} 行数不符: dist {1} 行,资产 {2} 行".format(asset_name, len(rows), actual)
        )
    log("{0}: {1} 行 ← {2}".format(asset_name, actual, dist_file))


def main():
    failures = []
    for entry in TABLES:
        try:
            rebuild_one(*entry)
        except Exception as exc:  # noqa: BLE001 逐表继续,一次报全,免得来回跑
            failures.append("{0}: {1}".format(entry[1], exc))
            unreal.log_error("[EffectTables] {0} 失败: {1}".format(entry[1], exc))
            unreal.log_error(traceback.format_exc())

    if failures:
        detail = " | ".join(failures)
        unreal.log_error("{0}: FAIL ({1})".format(RESULT_TAG, detail))
        write_result("{0}: FAIL ({1})".format(RESULT_TAG, detail))
        sys.exit(1)
    log("{0}: OK".format(RESULT_TAG))
    write_result("{0}: OK".format(RESULT_TAG))


main()
