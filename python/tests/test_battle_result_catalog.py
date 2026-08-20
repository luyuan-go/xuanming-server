"""battle_result 配置表加载边界 —— 重点是**跨表引用完整性**这道闸。

为什么单独立一份:这道闸放过一条的后果不是报错,而是运行期一个不报错的错误行为。
drop 行指向一个 item 表里不存在的 ID 时,加载照常成功、启动日志全绿,运行期
`Store.lookup()` 在 items 里查不到 → fail-closed 把整条掉落丢掉。方向是对的,
可玩家看到的是"这个怪打完永远不掉那件东西",而 `battle_drop_all_filtered` 那条 WARN
只在某玩家本场掉落被**整条过滤光**时才打 —— 一次掉 3 件而只坏 1 件时零日志。

Go 侧同一批次在 `pkg/configtable/store.go` 的 `validateCrossTables` 就返回 error,
`cmd/battle_result/main.go` 随即 `os.Exit(1)`:坏批次根本进不到运行期。
"""

from __future__ import annotations

import hashlib
import json
import pathlib
import shutil

import pytest

from pandorapy.configtable import ConfigTableError
from pandorapy.services.battle_result import catalog as bcat


def _restamp(active: pathlib.Path, table: str) -> None:
    """按当前磁盘字节重算某张表的 checksum 与 rows,写回 manifest。

    ★ 这一步不能省:不重算的话被测的是 checksum 闸(它先炸),验不到 FK 闸。
      重算后这份批次是"发布链自洽但 FK 坏了"——正是真实事故的形状:
      生成器之外的任何一环(手改 dist、拷贝串批次、上游表回滚)都能造出它。
    """
    mpath = active / "manifest.json"
    m = json.loads(mpath.read_text(encoding="utf-8"))
    for entry in m["tables"]:
        if entry["name"] != table:
            continue
        raw = (active / entry["file"]).read_bytes()
        entry["checksum"] = "sha256:" + hashlib.sha256(raw).hexdigest()
        entry["rows"] = len(json.loads(raw.decode("utf-8")).get("rows", []))
        break
    mpath.write_text(json.dumps(m, ensure_ascii=False, indent=2), encoding="utf-8")


@pytest.fixture
def dist_copy(configtable_dist: pathlib.Path, tmp_path: pathlib.Path) -> pathlib.Path:
    """真实 dist 的可写副本 —— 篡改必须在副本上做,绝不能碰仓库里的批次。"""
    dst = tmp_path / "dist"
    shutil.copytree(configtable_dist, dst)
    return dst


def test_real_dist_has_no_orphan_drop(configtable_dist: pathlib.Path) -> None:
    """当前批次 drop→item 零 orphan —— 补这道闸对现网是零误报。

    这条同时是**反向漂移**的护栏:凡 Go 收的批次 Python 也必须收。
    """
    res = bcat.load_tables(configtable_dist)
    assert res.tables.drop_count() > 0
    assert res.tables.item_count() > 0
    assert res.tables.droppable_ids <= set(res.tables.items)


def test_drop_pointing_at_missing_item_is_batch_rejected(dist_copy: pathlib.Path) -> None:
    """drop.item_config_id 指向 item 表里不存在的 ID → 整批拒载。

    对应 proto 的 `(excel_fk) = "item"` 注解与 Go 生成的 validateCrossTables
    (tables.gen.go)。错误文案与 Go **逐字相同**:按它建的 Loki 告警要能同时命中两栈。
    """
    dpath = dist_copy / "drop.json"
    data = json.loads(dpath.read_text(encoding="utf-8"))
    bad_id = data["rows"][0]["id"]
    data["rows"][0]["item_config_id"] = 999999
    dpath.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    _restamp(dist_copy, "drop")

    with pytest.raises(ConfigTableError) as exc:
        bcat.load_tables(dist_copy)
    assert str(exc.value) == f"表 drop 主键 {bad_id} 的 物品ID(999999)在表 item 中不存在"


def test_drop_error_text_matches_go_verbatim(repo_root: pathlib.Path) -> None:
    """文案取自 Go 源码而不是抄在测试里 —— Go 改了文案这条必须红。"""
    src = (repo_root / "pkg" / "configtable" / "tables.gen.go").read_text(encoding="utf-8")
    assert '"表 drop 主键 %d 的 物品ID(%d)在表 item 中不存在"' in src


def test_zero_item_config_id_is_not_a_reference(dist_copy: pathlib.Path) -> None:
    """item_config_id=0 是"无引用"而非"坏引用" —— 跳过,不拒批(与 Go 的 `continue` 一致)。

    把 0 也当 FK 查会把好批次拒掉:drop 表允许存在不发道具的行。
    """
    dpath = dist_copy / "drop.json"
    data = json.loads(dpath.read_text(encoding="utf-8"))
    data["rows"][0]["item_config_id"] = 0
    dpath.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    _restamp(dist_copy, "drop")

    res = bcat.load_tables(dist_copy)  # 不应抛
    assert 0 not in res.tables.droppable_ids
