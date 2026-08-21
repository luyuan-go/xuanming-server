"""inventory 的配置默认值、启动闸、指纹契约、配置表门禁与 service 层鉴权边界。

覆盖的是「起不来 / 起错了 / 账算错了」这一族缺陷 —— 它们都不会在普通业务测试里露头:

  - conf 默认值与 Go 侧 Defaults() 分叉:同一份 yaml 喂两个实现行为不同,两边都不报错
  - 启动闸漏掉或顺序不同:同一份坏配置在两栈上报不同的第一个错误,排障从此对不上
  - 幂等指纹格式漂移:Go 写过的那笔在 Python 看来"没做过" → **重复入账**
  - 配置表门禁放松:配表能造出没有对账语义的词条、孤儿池、抽不满的池
  - service 层鉴权边界搞反:玩家能自助发道具 / 自助结算,或内部服务被挡在门外

默认值 parity 刻意**从 Go 源码里读**而不是抄一份:抄一份的话,Go 改了默认值这个
测试照样绿(它验的是"我抄的值等于我抄的值")。
"""

from __future__ import annotations

import asyncio
import hashlib
import pathlib
import re

import pytest

from pandorapy import errcode
from pandorapy.services.inventory import biz as ibiz
from pandorapy.services.inventory import catalog as icat
from pandorapy.services.inventory import conf as iconf
from pandorapy.services.inventory import fingerprint as ifp
from pandorapy.services.inventory import main as imain
from pandorapy.services.inventory import service as isvc
from pandorapy.services.inventory.models import (
    InstanceOwnershipQuery,
    ItemAttribute,
    ItemGrant,
    ItemInstance,
    ItemStack,
)

GO_CONF = "services/economy/inventory/internal/conf/conf.go"
GO_MAIN = "services/economy/inventory/cmd/inventory/main.go"
GO_REPO = "services/economy/inventory/internal/data/inventory_repo.go"
GO_INSTANCE = "services/economy/inventory/internal/data/inventory_instance.go"
GO_TRANSFER = "services/economy/inventory/internal/data/inventory_transfer.go"
GO_CT = "services/economy/inventory/cmd/inventory/configtable.go"
DEV_YAML = "services/economy/inventory/etc/inventory-dev.yaml"


# ── 配置:默认值与 Go 逐个对齐 ───────────────────────────────────────────────


def test_defaults_match_go_source(repo_root: pathlib.Path) -> None:
    """每个默认值都从 Go 源码抓出来比对(含判据符号)。"""
    src = (repo_root / GO_CONF).read_text(encoding="utf-8")

    cfg = iconf.Config()
    cfg.apply_defaults()

    # 端口:Envoy cluster / run_services.ps1 端口检查 / K8s Service 都钉在这两个数上。
    assert cfg.server.grpc.addr == ":20015"
    assert cfg.server.http.addr == ":21015"
    assert '":20015"' in src and '":21015"' in src

    assert cfg.inventory.sweep_interval_td().total_seconds() == 300
    assert re.search(r"SweepInterval\s*=\s*config\.Duration\(5 \* time\.Minute\)", src)

    for field, value, go_name in (
        ("sweep_batch", 500, "SweepBatch"),
        ("ledger_retention_days", 90, "LedgerRetentionDays"),
        ("escrow_retention_days", 90, "EscrowRetentionDays"),
    ):
        assert getattr(cfg.inventory, field) == value, field
        assert re.search(rf"c\.Inventory\.{go_name}\s*=\s*{value}\b", src), go_name

    for field, value, go_name in (
        ("max_journal_batch", 64, "MaxJournalBatch"),
        ("max_items_per_op", 64, "MaxItemsPerOp"),
        ("hourly_journal_quota", 2000, "HourlyJournalQuota"),
        ("default_max_stack", 99, "DefaultMaxStack"),
        ("migration_batch", 200, "MigrationBatch"),
        ("journal_retention_days", 90, "JournalRetentionDays"),
    ):
        assert getattr(cfg.bag, field) == value, field
        assert re.search(rf"c\.Bag\.{go_name}\s*=\s*{value}\b", src), go_name

    # capacity **没有**默认值:<=0 就是"实例背包未启用"(安全默认)。
    # 给它补一个默认值等于把一个从没配过的键悄悄打开。
    assert cfg.inventory.capacity == 0
    assert not re.search(r"c\.Inventory\.Capacity\s*=", src)


def test_hourly_journal_quota_uses_equals_zero_not_le_zero(repo_root: pathlib.Path) -> None:
    """hourly_journal_quota 的判据是 `== 0`,不是 `<= 0` —— 负值 = **显式关闭**配额。

    换成 `<= 0` 会把 "-1 关闭配额" 悄悄改写成 2000,配置意图被反转且无任何日志。
    """
    src = (repo_root / GO_CONF).read_text(encoding="utf-8")
    assert re.search(r"c\.Bag\.HourlyJournalQuota\s*==\s*0", src)

    cfg = iconf.Config(bag=iconf.BagConf(hourly_journal_quota=-1))
    cfg.apply_defaults()
    assert cfg.bag.hourly_journal_quota == -1


def test_capacity_purchases_distinguishes_unset_from_explicit_empty(
    repo_root: pathlib.Path,
) -> None:
    """Go 判的是 `== nil`:显式写空列表 = "全部段都不可买",不能被默认档位覆盖回去。"""
    src = (repo_root / GO_CONF).read_text(encoding="utf-8")
    assert re.search(r"c\.Bag\.CapacityPurchases\s*==\s*nil", src)

    unset = iconf.Config()
    unset.apply_defaults()
    assert len(unset.bag.capacity_purchases) == 2  # 身上 + 仓库

    explicit_empty = iconf.Config(bag=iconf.BagConf(capacity_purchases=[]))
    explicit_empty.apply_defaults()
    assert explicit_empty.bag.capacity_purchases == []


def test_loads_the_same_dev_yaml_as_go(repo_root: pathlib.Path) -> None:
    """读 Go 版**同一份** yaml,而不是测试专用配置。

    session_gate / bag / ds_auth 三段在 Go 侧属于 config.Base 或服务私有配置,
    Python 的 BaseConf 还没建到 —— 没建模就会静默落进 extra,
    表现为"配了却不生效、零信号"。
    """
    cfg = iconf.Config.load(repo_root / DEV_YAML)

    assert cfg.server.grpc.addr == ":20015"
    # node_id 与 mail 必须不同:instance_id 是跨服务铸造的 ID 空间。
    assert cfg.node.node_id == 1
    assert cfg.node.redis_client.endpoints() == ["127.0.0.1:6380"]
    assert cfg.node.mysql_client.max_open_conns == 32
    assert cfg.config_table.dir == "../../../configtable/dist"
    assert cfg.inventory.capacity == 200
    assert cfg.inventory.sweep_batch == 500
    assert cfg.session_gate.require is False
    # bag 段必须被真正解析(不是落进 extra)。
    assert cfg.bag.dsn.startswith("pandora:")
    assert cfg.bag.owner_addr == "127.0.0.1:20017"
    assert cfg.bag.section_capacity_of(100) == 50


def test_retention_mode_default_is_report_only_and_typo_is_rejected() -> None:
    """留空 = 只报告不删;拼错必须**拒启**而不是静默回落。

    静默回落的后果:运维以为开了清理、实际一行没删,库继续无界增长且启动期毫无痕迹。
    """
    cfg = iconf.Config()
    cfg.apply_defaults()
    assert cfg.inventory.retention_mode_parsed().value == "report_only"
    cfg.inventory.validate_retention_mode()  # 不抛

    bad = iconf.Config(inventory=iconf.InventoryConf(retention_mode="delet"))
    with pytest.raises(ValueError):
        bad.inventory.validate_retention_mode()
    # 即便闸被绕过,解析也必须回落 report_only —— 任何不确定都不删。
    assert bad.inventory.retention_mode_parsed().value == "report_only"


# ── bag 配置校验(即使 Python 不提供 BagService,这道闸也必须与 Go 同结论)──


@pytest.mark.parametrize(
    "bag",
    [
        # 段容量 0 = 整段"永远装不下",而调用方只看到普通的容量满错误。
        iconf.BagConf(section_capacities=[iconf.BagSectionCapacityRule(bag_type=1, capacity=0)]),
        # 重复 bag_type:哪条生效取决于遍历顺序。
        iconf.BagConf(
            section_capacities=[
                iconf.BagSectionCapacityRule(bag_type=1, capacity=10),
                iconf.BagSectionCapacityRule(bag_type=1, capacity=20),
            ]
        ),
        # 堆叠上限 0:服务端权威拆堆会算出 0 个格子。
        iconf.BagConf(item_max_stacks=[iconf.BagItemStackRule(item_config_id=1, max_stack=0)]),
        # §5.3 拍板:活动段不可买。
        iconf.BagConf(
            section_capacities=[iconf.BagSectionCapacityRule(bag_type=100, capacity=50)],
            capacity_purchases=[
                iconf.BagCapacityPurchaseRule(
                    bag_type=100, max_extra=10, tiers=[iconf.BagCapacityTier(slots=10, price_gold=1)]
                )
            ],
        ),
        # 档位 slots 之和超过 max_extra:最后几档买了不给格子(玩家付钱没东西)。
        iconf.BagConf(
            section_capacities=[iconf.BagSectionCapacityRule(bag_type=1, capacity=200)],
            capacity_purchases=[
                iconf.BagCapacityPurchaseRule(
                    bag_type=1,
                    max_extra=10,
                    tiers=[iconf.BagCapacityTier(slots=20, price_gold=1)],
                )
            ],
        ),
    ],
)
def test_bag_conf_invalid_combinations_are_rejected(bag: iconf.BagConf) -> None:
    with pytest.raises(ValueError):
        bag.validate_rules()


# ── 幂等指纹:格式必须与 Go 逐字一致 ─────────────────────────────────────────


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def test_fingerprint_formats_match_go_source(repo_root: pathlib.Path) -> None:
    """逐条比对 Go 源码里的格式串。

    ★ 这是本文件里最要紧的一组:指纹落在 inventory_ledger.request_fingerprint 列里。
      迁移期两栈并存时,Go 版写过的行必须能被 Python 版算出同一个 hex ——
      算不出来,玩家的正常重试会被判成 ErrInventoryIdempotencyConflict 全部失败;
      而如果 Python 算得比 Go **松**(丢了一个维度),两笔不同请求会被判成同一笔,
      **重复入账**。两个方向都不报错。
    """
    repo_src = (repo_root / GO_REPO).read_text(encoding="utf-8")
    inst_src = (repo_root / GO_INSTANCE).read_text(encoding="utf-8")
    xfer_src = (repo_root / GO_TRANSFER).read_text(encoding="utf-8")

    assert ifp.use_fingerprint(7, 3) == _sha("use|7:3")
    assert '"use|%d:%d"' in repo_src
    assert ifp.discard_fingerprint(7, 3) == _sha("discard|7:3")
    assert '"discard|%d:%d"' in repo_src
    assert ifp.battle_consume_fingerprint(7, 3) == _sha("battle_consume|7:3")
    assert '"battle_consume|%d:%d"' in repo_src
    assert ifp.battle_discard_fingerprint(7, 3) == _sha("battle_discard|7:3")
    assert '"battle_discard|%d:%d"' in repo_src
    assert ifp.sell_fingerprint(7, 3) == _sha("sell|7:3")
    assert '"sell|%d:%d"' in repo_src
    assert ifp.sell_instance_fingerprint(99, 7) == _sha("sell_inst|99|item=7")
    assert '"sell_inst|%d|item=%d"' in repo_src

    # legacy(升级前提交的行,指纹里含首次成交价)。
    assert ifp.legacy_sell_fingerprint(7, 3, 50) == _sha("sell|7:3|gold=50")
    assert '"sell|%d:%d|gold=%d"' in repo_src
    assert ifp.legacy_sell_instance_fingerprint(99, 7, 50) == _sha("sell_inst|99|item=7|gold=50")
    assert '"sell_inst|%d|item=%d|gold=%d"' in repo_src

    assert ifp.auction_settle_fingerprint(1, 2, 7, 3, 60) == _sha(
        "auction_settle|seller=1|buyer=2|item=7|qty=3|gold=60"
    )
    assert "auction_settle|seller=%d|buyer=%d|item=%d|qty=%d|gold=%d" in repo_src

    # grant:items 必须按 item_config_id 升序规范化,否则同一笔发放算出两个指纹。
    assert ifp.grant_fingerprint([(9, 1), (2, 5)], 100) == _sha("grant|2:5|9:1|gold=100")
    assert ifp.grant_fingerprint([(2, 5), (9, 1)], 100) == ifp.grant_fingerprint(
        [(9, 1), (2, 5)], 100
    )

    assert ifp.player_trade_settle_fingerprint(
        1, 2, [(9, 1), (2, 5)], [(3, 2)], 70
    ) == _sha("trade_settle|seller=1|buyer=2|sell|2:5|9:1|buy|3:2|price=70")
    assert "trade_settle|seller=%d|buyer=%d|" in repo_src

    assert ifp.grant_instances_fingerprint([9, 2]) == _sha("grant_inst|2|9")
    assert '"grant_inst"' in inst_src

    assert ifp.escrow_out_fingerprint(42, [9, 2]) == _sha("escrow_out|to=42|2|9")
    assert '"escrow_out|to="' in xfer_src
    assert ifp.transfer_claim_fingerprint([(9, 7), (2, 3)]) == _sha("transfer_claim|2:3|9:7")
    assert '"transfer_claim"' in xfer_src


def test_grant_inst_detail_roundtrip_matches_go_format(repo_root: pathlib.Path) -> None:
    """ledger.detail 里的 instance_id CSV 是 grant_inst 幂等回放的**唯一**依据。

    格式对不上 = Go 写的行在 Python 侧解不出 id,回放返回空列表 ——
    调用方以为一件都没发,而实际早已发过。
    """
    src = (repo_root / GO_INSTANCE).read_text(encoding="utf-8")
    assert '"grant_inst ids="' in src
    assert ifp.encode_instance_ids([1, 2, 3]) == "grant_inst ids=1,2,3"
    assert ifp.decode_instance_ids("grant_inst ids=1,2,3") == [1, 2, 3]
    # 脏 detail 不能炸,只丢解不出来的部分(与 Go 的 ParseUint 失败即跳过一致)。
    assert ifp.decode_instance_ids("grant_inst ids=1,x,0,3") == [1, 3]
    assert ifp.decode_instance_ids("no marker") == []


def test_settle_idempotency_keys_are_cross_service_contract() -> None:
    """幂等键格式是跨服务契约,变一个字符 = 迁移期重复入账。"""
    from pandorapy.services.inventory import settle

    assert settle.auction_settle_key(77) == "auction:settle:77"
    assert settle.trade_settle_key(88) == "trade:settle:88"
    # 分片对账口径(每腿细分),与结算键同源。
    assert ibiz.auction_leg_key(77, 5, ibiz.LEG_SELLER_DELIVER) == "auction:settle:77:5:seller_deliver"


# ── 配置表门禁 ────────────────────────────────────────────────────────────


def test_real_dist_batch_passes_all_gates(configtable_dist: pathlib.Path) -> None:
    """用**真实 dist** 加载:验的是 Python 版和 Go 版看到的是同一个批次。

    造一份假数据只能验代码自己,验不了跨语言一致性(checksum 字节口径尤其)。
    """
    result = icat.load_tables(configtable_dist)
    assert result.version > 0
    assert result.tables.item_count() > 0
    assert result.tables.pool_count() > 0
    store = icat.Store(result.tables, str(configtable_dist))
    # 找一件装备:它必须有鉴定规则,且 lobby_usable 恒 False(大厅无效果派发器)。
    equips = [
        cid
        for cid, row in result.tables.items.items()
        if row.type == icat.ITEM_TYPE_EQUIPMENT
    ]
    assert equips
    definition = store.lookup(equips[0])
    assert definition.equipment is True
    assert definition.lobby_usable is False
    assert store.identify_rule(equips[0]) is not None


def test_allowed_attrs_match_go_source(repo_root: pathlib.Path) -> None:
    """属性白名单必须与 Go 逐字一致 —— 放宽一个 id 就等于放行一类无对账语义的词条。"""
    src = (repo_root / GO_CT).read_text(encoding="utf-8")
    assert icat.ALLOWED_ATTRS == {3: "Atk", 7: "MoveSpeedRate", 9: "Defense"}
    assert '3: "Atk", 7: "MoveSpeedRate", 9: "Defense"' in src
    assert "1_000_000" in src and icat.MAX_POOL_TOTAL_WEIGHT == 1_000_000


def test_item_heal_type_and_value_combinations_are_fail_closed() -> None:
    """固定值/最大生命百分比必须唯一成组，坏组合不能在 Python 迁移栈漏过。"""
    from pandora.config.v1 import item_pb2 as ipb

    icat._validate_item_heal(ipb.ItemRow(id=1, usable=True, use_heal_hp=50))
    icat._validate_item_heal(
        ipb.ItemRow(
            id=2,
            usable=True,
            use_heal_type=icat.ITEM_HEAL_TYPE_FIXED,
            use_heal_hp=50,
        )
    )
    icat._validate_item_heal(
        ipb.ItemRow(
            id=3,
            usable=True,
            use_heal_type=icat.ITEM_HEAL_TYPE_MAX_HP_PERCENT,
            use_heal_max_hp_percent=25,
        )
    )

    invalid = [
        ipb.ItemRow(id=4, usable=True),
        ipb.ItemRow(id=5, usable=True, use_heal_hp=50, use_heal_max_hp_percent=20),
        ipb.ItemRow(id=6, usable=True, use_heal_hp=icat.MAX_CLIENT_FIXED_HEAL_HP + 1),
        ipb.ItemRow(id=7, usable=False, use_heal_hp=50),
        ipb.ItemRow(
            id=8,
            usable=True,
            use_heal_type=icat.ITEM_HEAL_TYPE_MAX_HP_PERCENT,
            use_heal_hp=50,
            use_heal_max_hp_percent=20,
        ),
        ipb.ItemRow(
            id=9,
            usable=True,
            use_heal_type=icat.ITEM_HEAL_TYPE_MAX_HP_PERCENT,
        ),
        ipb.ItemRow(
            id=10,
            usable=True,
            use_heal_type=icat.ITEM_HEAL_TYPE_MAX_HP_PERCENT,
            use_heal_max_hp_percent=101,
        ),
        ipb.ItemRow(
            id=11,
            usable=False,
            use_heal_type=icat.ITEM_HEAL_TYPE_MAX_HP_PERCENT,
            use_heal_max_hp_percent=20,
        ),
        ipb.ItemRow(id=12, usable=True, use_heal_type=99, use_heal_max_hp_percent=20),
    ]
    for row in invalid:
        with pytest.raises(icat.ConfigTableError):
            icat._validate_item_heal(row)


def _tables(items: dict, affix: dict, attrs: dict | None = None) -> icat.Tables:
    from pandora.config.v1 import equipment_affix_pb2 as apb
    from pandora.config.v1 import item_pb2 as ipb
    from pandora.config.v1 import role_attr_map_pb2 as rpb

    role_attrs = attrs
    if role_attrs is None:
        role_attrs = {
            i: rpb.RoleAttrMapRow(id=i, code_name=name)
            for i, name in icat.ALLOWED_ATTRS.items()
        }
    return icat.Tables(
        version=1,
        source_rev="test",
        items={
            cid: ipb.ItemRow(id=cid, type=t, identify_pool_id=pool)
            for cid, (t, pool) in items.items()
        },
        affix_by_pool={
            pool: [apb.EquipmentAffixRow(**row) for row in rows] for pool, rows in affix.items()
        },
        role_attrs=role_attrs,
    )


def test_validator_rejects_attr_outside_whitelist() -> None:
    """配表造出的"只显示不生效"词条必须整批拒。"""
    t = _tables(
        {10: (icat.ITEM_TYPE_EQUIPMENT, 1)},
        {1: [{"id": 1, "pool_id": 1, "attr_count": 1, "attr_id": 999, "weight": 1,
              "min_value": 1, "max_value": 2}]},
    )
    with pytest.raises(icat.ConfigTableError):
        icat.validate_inventory_tables(t)


def test_validator_rejects_attr_count_exceeding_unique_candidates() -> None:
    """抽不满 → rollIdentifyAttrs 返回空 → 装备被永久写成 identified 且零词条(不可逆)。"""
    t = _tables(
        {10: (icat.ITEM_TYPE_EQUIPMENT, 1)},
        {1: [{"id": 1, "pool_id": 1, "attr_count": 2, "attr_id": 3, "weight": 1,
              "min_value": 1, "max_value": 2}]},
    )
    with pytest.raises(icat.ConfigTableError):
        icat.validate_inventory_tables(t)


def test_validator_rejects_orphan_pool_and_missing_pool() -> None:
    """孤儿池 = 表在漂移(改了 item 没改 affix 或反之);缺池 = 鉴定拿不到规则。"""
    good_row = {"id": 1, "pool_id": 1, "attr_count": 1, "attr_id": 3, "weight": 1,
                "min_value": 1, "max_value": 2}
    # 只有非装备道具:池 1 没人引用 = 孤儿。
    from pandora.config.v1 import item_pb2 as ipb

    orphan = _tables({10: (ipb.ITEM_TYPE_CONSUMABLE, 0)}, {1: [good_row]})
    with pytest.raises(icat.ConfigTableError):
        icat.validate_inventory_tables(orphan)

    missing = _tables({10: (icat.ITEM_TYPE_EQUIPMENT, 7)}, {1: [good_row]})
    with pytest.raises(icat.ConfigTableError):
        icat.validate_inventory_tables(missing)


def test_validator_rejects_inconsistent_attr_count_in_one_pool() -> None:
    """同池两行 attr_count 不同 → 抽几条取决于哪一行先被读到。"""
    t = _tables(
        {10: (icat.ITEM_TYPE_EQUIPMENT, 1)},
        {1: [
            {"id": 1, "pool_id": 1, "attr_count": 1, "attr_id": 3, "weight": 1,
             "min_value": 1, "max_value": 2},
            {"id": 2, "pool_id": 1, "attr_count": 2, "attr_id": 9, "weight": 1,
             "min_value": 1, "max_value": 2},
        ]},
    )
    with pytest.raises(icat.ConfigTableError):
        icat.validate_inventory_tables(t)


# ── 启动闸 ────────────────────────────────────────────────────────────────


def _run(argv: list[str]) -> int:
    return imain.main(argv)


def _run_capturing(yaml_path: pathlib.Path, events: list[str]) -> int:
    """跑 main,只把 logger 换成事件名记录器(闸本身不 mock:验的就是它们真被执行到)。"""
    import pandorapy.log as plog

    class _Recorder:
        def __getattr__(self, level):  # noqa: ANN001
            def emit(event, **kw):  # noqa: ANN001, ANN003
                events.append(event)

            return emit

    recorder = _Recorder()
    real_setup, real_get = plog.setup, plog.get
    plog.setup = lambda *_a, **_k: recorder
    plog.get = lambda *_a, **_k: recorder
    try:
        return imain.main(["-conf", str(yaml_path)])
    finally:
        plog.setup, plog.get = real_setup, real_get


def test_missing_conf_file_exits_nonzero(tmp_path: pathlib.Path) -> None:
    assert _run(["-conf", str(tmp_path / "nope.yaml")]) == 1


def test_cell_route_mode_is_rejected_at_config_load(tmp_path: pathlib.Path) -> None:
    """配了多 Cell 但 Python 只实现单 Cell → 拒启(不是忽略 + WARN)。

    忽略掉的后果是所有玩家静默落在单 Cell 上,与配置意图不符且零信号。
    """
    yaml_path = tmp_path / "inv.yaml"
    yaml_path.write_text('cell_route:\n  mode: "static"\n', encoding="utf-8")
    events: list[str] = []
    assert _run_capturing(yaml_path, events) == 1
    assert "config_scan_failed" in events


def test_identify_rule_gate_fires_before_configtable_gate(tmp_path: pathlib.Path) -> None:
    """闸序必须与 Go 一致:鉴定规则校验(main.go:81)在 config_table.dir(main.go:89)之前。

    顺序不同的话,同一份坏配置在两栈上报不同的第一个错误,排障口径从此对不上。
    """
    yaml_path = tmp_path / "inv.yaml"
    yaml_path.write_text(
        "inventory:\n"
        "  identify_rules:\n"
        "    - item_config_id: 0\n"  # 非法:必须非 0
        "      attr_count: 1\n",
        encoding="utf-8",
    )
    events: list[str] = []
    assert _run_capturing(yaml_path, events) == 1
    assert "inventory_item_rules_invalid" in events
    assert "configtable_dir_required" not in events


def test_configtable_dir_required_fires_before_mysql_gate(tmp_path: pathlib.Path) -> None:
    """道具规则的唯一权威缺失 → 拒启,且必须在 MySQL 闸之前(与 Go 同序)。

    除被测项外配置全部合法:否则测的其实是另一道闸(第 2 批踩过)。
    """
    yaml_path = tmp_path / "inv.yaml"
    yaml_path.write_text("node:\n  node_id: 1\n", encoding="utf-8")
    events: list[str] = []
    assert _run_capturing(yaml_path, events) == 1
    assert "configtable_dir_required" in events
    assert "mysql_dsn_required" not in events


def test_bad_configtable_dir_is_fail_closed(tmp_path: pathlib.Path) -> None:
    yaml_path = tmp_path / "inv.yaml"
    yaml_path.write_text(
        "node:\n  node_id: 1\nconfig_table:\n  dir: \"./nowhere\"\n", encoding="utf-8"
    )
    events: list[str] = []
    assert _run_capturing(yaml_path, events) == 1
    assert "configtable_load_failed" in events


def test_retention_mode_typo_refuses_to_start(
    tmp_path: pathlib.Path, configtable_dist: pathlib.Path
) -> None:
    """必须走到保留期闸才算数 —— 所以前面几道要用真实合法配置放过去。"""
    yaml_path = tmp_path / "inv.yaml"
    yaml_path.write_text(
        "node:\n"
        "  node_id: 1\n"
        "config_table:\n"
        f'  dir: "{configtable_dist.as_posix()}"\n'
        "inventory:\n"
        '  retention_mode: "delet"\n',
        encoding="utf-8",
    )
    events: list[str] = []
    assert _run_capturing(yaml_path, events) == 1
    assert "inventory_retention_mode_invalid" in events
    assert "inventory_item_table_loaded" in events  # 证明确实走到了后面
    assert "mysql_dsn_required" not in events


def test_mysql_dsn_required(tmp_path: pathlib.Path, configtable_dist: pathlib.Path) -> None:
    """背包落库不可降级:没有权威库就没有背包。"""
    yaml_path = tmp_path / "inv.yaml"
    yaml_path.write_text(
        "node:\n"
        "  node_id: 1\n"
        "config_table:\n"
        f'  dir: "{configtable_dist.as_posix()}"\n',
        encoding="utf-8",
    )
    events: list[str] = []
    assert _run_capturing(yaml_path, events) == 1
    assert "mysql_dsn_required" in events
    assert "service_ready" not in events


def test_bag_conf_gate_runs_even_though_bag_service_is_not_implemented(
    tmp_path: pathlib.Path, configtable_dist: pathlib.Path
) -> None:
    """背包域校验闸即使在"Python 不提供 BagService"的前提下也必须跑。

    漏掉会让一份非法 yaml 在 Go 版拒启、Python 版放行 —— 两栈对同一份配置结论不同。
    """
    yaml_path = tmp_path / "inv.yaml"
    yaml_path.write_text(
        "node:\n"
        "  node_id: 1\n"
        "config_table:\n"
        f'  dir: "{configtable_dist.as_posix()}"\n'
        "bag:\n"
        "  section_capacities:\n"
        "    - bag_type: 1\n"
        "      capacity: 0\n",
        encoding="utf-8",
    )
    events: list[str] = []
    assert _run_capturing(yaml_path, events) == 1
    assert "bag_conf_invalid" in events


def test_gate_event_names_match_go_source(repo_root: pathlib.Path) -> None:
    """事件名逐字相同 —— Loki 上按事件名建的告警对不上就是静默失去覆盖。"""
    src = (repo_root / GO_MAIN).read_text(encoding="utf-8")
    for event in (
        "abs_conf_path_failed",
        "config_load_failed",
        "config_scan_failed",
        "inventory_item_rules_invalid",
        "configtable_dir_required",
        "configtable_load_failed",
        "configtable_load_warning",
        "inventory_item_table_loaded",
        "bag_conf_invalid",
        "inventory_retention_mode_invalid",
        "mysql_dsn_required",
        "mysql_strict_mode_required",
        "mysql_schema_check_failed",
        "instance_bag_enabled",
        "retention_sweep_enabled",
        "service_ready",
    ):
        assert f'"{event}"' in src, f"Go 侧没有事件名 {event}"
        assert f'"{event}"' in (
            pathlib.Path(imain.__file__).read_text(encoding="utf-8")
        ), f"Python 侧没有事件名 {event}"


# ── biz:鉴定 roll / 形状校验 ─────────────────────────────────────────────


class _FakeRepo:
    """只实现被测路径需要的方法。"""

    def __init__(self) -> None:
        self.instances: list[ItemInstance] = []
        self.items: list[ItemStack] = []
        self.gold = 0
        self.identified: tuple | None = None

    async def get_inventory(self, player_id: int):  # noqa: ANN001, ARG002
        return self.gold, self.items

    async def list_instances(self, player_id: int):  # noqa: ANN001, ARG002
        return self.instances

    async def identify_instance(self, player_id, instance_id, attrs):  # noqa: ANN001, ARG002
        self.identified = (instance_id, list(attrs))
        inst = ItemInstance(instance_id=instance_id, item_config_id=10, identified=True)
        inst.attributes = list(attrs)
        return inst, False


class _FakeCatalog:
    """自定义 Catalog —— 刻意绕过配置表门禁,验 biz 自己的逐次候选校验。"""

    def __init__(self, definition, rule) -> None:  # noqa: ANN001
        self._definition = definition
        self._rule = rule

    def lookup(self, item_config_id: int):  # noqa: ANN001, ARG002
        return self._definition

    def identify_rule(self, item_config_id: int):  # noqa: ANN001, ARG002
        return self._rule


def _equip_def(price: int = 100) -> icat.ItemDefinition:
    return icat.ItemDefinition(
        equipment=True, lobby_usable=False, battle_usable=False, sell_unit_price=price, max_stack=1
    )


def test_identify_roll_is_weighted_without_replacement_and_deterministic() -> None:
    """加权不放回 + 服务端权威 roll(反作弊 §9.6):同一随机序列必须得到同一结果。"""
    rule = icat.IdentifyDefinition(
        attr_count=2,
        pool=[
            icat.IdentifyAttrDefinition(attr_id=3, weight=40, min=4, max=8),
            icat.IdentifyAttrDefinition(attr_id=9, weight=40, min=4, max=8),
            icat.IdentifyAttrDefinition(attr_id=7, weight=20, min=30, max=60),
        ],
    )
    repo = _FakeRepo()
    uc = ibiz.InventoryUsecase(repo, iconf.InventoryConf(capacity=10))
    uc.set_item_catalog(_FakeCatalog(_equip_def(), rule))
    seq = iter([0, 0, 40, 0])  # 抽 attr_id=3 取 min;再抽剩余里的第二条取 min
    uc.set_rand_source(lambda n: next(seq, 0))

    attrs = uc._roll_identify_attrs(10)  # noqa: SLF001
    assert [a.attr_id for a in attrs] == [3, 7]
    assert attrs[0] == ItemAttribute(attr_id=3, value=4)
    # 不放回:同一 attr_id 不会出现两次。
    assert len({a.attr_id for a in attrs}) == 2


def test_identify_roll_rejects_illegal_candidate_from_custom_catalog() -> None:
    """自定义 Catalog 能绕过配置加载门 —— biz 必须自己再挡一次。

    放过一个负权重 / min>max 的候选会让抽取产生不可预期的值,而且直接落库。
    """
    bad = icat.IdentifyDefinition(
        attr_count=1,
        pool=[icat.IdentifyAttrDefinition(attr_id=3, weight=0, min=1, max=2)],
    )
    uc = ibiz.InventoryUsecase(_FakeRepo(), iconf.InventoryConf(capacity=10))
    uc.set_item_catalog(_FakeCatalog(_equip_def(), bad))
    assert uc._roll_identify_attrs(10) == []  # noqa: SLF001


def test_identify_fails_closed_when_rule_unavailable() -> None:
    """接了配置表之后 roll 不出词条**必须拒** —— 放行会把装备永久写成 identified 且零词条。"""
    repo = _FakeRepo()
    repo.instances = [ItemInstance(instance_id=5, item_config_id=10)]
    uc = ibiz.InventoryUsecase(repo, iconf.InventoryConf(capacity=10))
    uc.set_item_catalog(_FakeCatalog(_equip_def(), None))
    with pytest.raises(errcode.PandoraError) as ei:
        asyncio.run(uc.identify_item(1, 5))
    assert ei.value.code == errcode.ErrInvalidState
    assert repo.identified is None  # 没有落库


def test_use_item_is_not_usable_when_catalog_says_lobby_unusable() -> None:
    """item.usable 是"局内可消费",不是"大厅可用"。

    把它当大厅可用会让 UseItem 扣掉道具而效果一个也不发生。
    """
    definition = icat.ItemDefinition(
        equipment=False, lobby_usable=False, battle_usable=True, sell_unit_price=0, max_stack=99
    )
    uc = ibiz.InventoryUsecase(_FakeRepo(), iconf.InventoryConf())
    uc.set_item_catalog(_FakeCatalog(definition, None))
    with pytest.raises(errcode.PandoraError) as ei:
        asyncio.run(uc.use_item(1, 7, 1, "k"))
    assert ei.value.code == errcode.ErrInventoryItemNotUsable


def test_grant_instances_without_snowflake_is_invalid_arg() -> None:
    """实例背包未启用时返回 code=4 而 gRPC 本身 rpc_ok —— 2026-08-06 那次故障的形状。"""
    uc = ibiz.InventoryUsecase(_FakeRepo(), iconf.InventoryConf())
    with pytest.raises(errcode.PandoraError) as ei:
        asyncio.run(uc.grant_instances(1, [10], "k"))
    assert ei.value.code == errcode.ErrInvalidArg


def test_transfer_id_shape_validation() -> None:
    """重复 ID 必须拒:同一行会被搬两次,第二次搬的是已经不在源表的行(静默少搬)。"""
    uc = ibiz.InventoryUsecase(_FakeRepo(), iconf.InventoryConf())
    for ids in ([], [0], [1, 1], list(range(1, ibiz.MAX_TRANSFER_BATCH + 2))):
        with pytest.raises(errcode.PandoraError) as ei:
            asyncio.run(uc.release_transfer_escrow(ids))
        assert ei.value.code == errcode.ErrInvalidArg


def test_check_items_owned_rejects_oversized_request() -> None:
    """超限**直接拒而不是静默截断** —— 截断会把「未查」伪装成「未持有」。"""
    uc = ibiz.InventoryUsecase(_FakeRepo(), iconf.InventoryConf())
    ids = list(range(1, ibiz.MAX_CHECK_ITEMS_OWNED + 2))
    with pytest.raises(errcode.PandoraError) as ei:
        asyncio.run(uc.check_items_owned(1, ids))
    assert ei.value.code == errcode.ErrInvalidArg


def test_check_items_owned_merges_stack_and_instance_sources() -> None:
    """"持有" = 堆叠计数>0 **或** 存在装备实例,两条路任一成立即算。"""
    repo = _FakeRepo()
    repo.items = [ItemStack(item_config_id=7, count=2), ItemStack(item_config_id=8, count=0)]
    repo.instances = [ItemInstance(instance_id=1, item_config_id=9)]
    uc = ibiz.InventoryUsecase(repo, iconf.InventoryConf(capacity=10))
    assert asyncio.run(uc.check_items_owned(1, [7, 8, 9, 10])) == [7, 9]


def test_check_items_owned_skips_instance_table_when_capacity_disabled() -> None:
    """未启用实例背包时不读 player_item_instance:既有库可能尚未迁移出该表。"""
    repo = _FakeRepo()
    repo.items = [ItemStack(item_config_id=7, count=1)]

    async def _boom(_player_id):  # noqa: ANN001
        raise AssertionError("capacity<=0 时不应读实例表")

    repo.list_instances = _boom  # type: ignore[assignment]
    uc = ibiz.InventoryUsecase(repo, iconf.InventoryConf(capacity=0))
    assert asyncio.run(uc.check_items_owned(1, [7, 9])) == [7]


def test_unknown_escrow_side_is_rejected() -> None:
    uc = ibiz.InventoryUsecase(_FakeRepo(), iconf.InventoryConf())
    with pytest.raises(errcode.PandoraError) as ei:
        asyncio.run(uc.freeze_for_order(1, 2, 99, 7, 1, 1))
    assert ei.value.code == errcode.ErrInvalidArg


def test_self_settlement_is_rejected_on_both_paths() -> None:
    """自成交 / 自交易:同一玩家在同 key 下要写两条流水 → 唯一键冲突整笔失败。"""
    uc = ibiz.InventoryUsecase(_FakeRepo(), iconf.InventoryConf())
    with pytest.raises(errcode.PandoraError):
        asyncio.run(uc.settle_auction_match(1, 5, 5, 2, 3, 7, 1, 1))
    with pytest.raises(errcode.PandoraError):
        asyncio.run(uc.settle_player_trade(1, 5, 5, [], [], 10))


# ── service 层:鉴权边界(两种方向,不许统一)─────────────────────────────


class _FakeContext:
    """最小 grpc.aio.ServicerContext 替身:只需要 invocation_metadata。"""

    def __init__(self, player_id: int = 0) -> None:
        self._md = [("x-pandora-player-id", str(player_id))] if player_id else []

    def invocation_metadata(self):  # noqa: ANN201
        return self._md


class _NoopUsecase:
    """业务层替身:任何调用都算成功 —— 本组只验鉴权闸,不验业务。"""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def get_inventory_full(self, player_id):  # noqa: ANN001
        self.calls.append(f"get_inventory_full:{player_id}")
        return 0, [], 0, []

    async def grant_items(self, *a, **kw):  # noqa: ANN002, ANN003
        self.calls.append("grant_items")
        return 0

    async def check_items_owned(self, *a, **kw):  # noqa: ANN002, ANN003
        self.calls.append("check_items_owned")
        return []


def test_client_rpc_without_caller_identity_is_unauthorized() -> None:
    """callerID==0(内网直连、无 JWT)敲客户端 RPC → ERR_UNAUTHORIZED。"""
    from pandora.common.v1 import errcode_pb2 as commonpb
    from pandora.inventory.v1 import inventory_pb2 as pb

    uc = _NoopUsecase()
    svc = isvc.InventoryService(uc)
    resp = asyncio.run(svc.GetInventory(pb.GetInventoryRequest(player_id=7), _FakeContext(0)))
    assert resp.code == commonpb.ERR_UNAUTHORIZED
    assert uc.calls == []  # 没有走到业务


def test_client_rpc_rejects_mismatched_body_player_id() -> None:
    """请求体 player_id 与调用者不一致 → ERR_PERMISSION_DENY(防读他人背包)。"""
    from pandora.common.v1 import errcode_pb2 as commonpb
    from pandora.inventory.v1 import inventory_pb2 as pb

    uc = _NoopUsecase()
    svc = isvc.InventoryService(uc)
    resp = asyncio.run(svc.GetInventory(pb.GetInventoryRequest(player_id=9), _FakeContext(7)))
    assert resp.code == commonpb.ERR_PERMISSION_DENY
    assert uc.calls == []


def test_client_rpc_uses_caller_identity_not_request_body() -> None:
    """权威 player_id 恒等于调用者身份(R5:忽略请求体字段)。"""
    from pandora.common.v1 import errcode_pb2 as commonpb
    from pandora.inventory.v1 import inventory_pb2 as pb

    uc = _NoopUsecase()
    svc = isvc.InventoryService(uc)
    # 请求体不带 player_id:仍用调用者身份。
    resp = asyncio.run(svc.GetInventory(pb.GetInventoryRequest(), _FakeContext(7)))
    assert resp.code == commonpb.OK
    assert uc.calls == ["get_inventory_full:7"]


def test_system_rpc_rejects_client_caller() -> None:
    """带玩家 JWT 敲系统 RPC → 一律拒(杜绝玩家自助发道具 / 探测他人背包)。"""
    from pandora.common.v1 import errcode_pb2 as commonpb
    from pandora.inventory.v1 import inventory_pb2 as pb

    uc = _NoopUsecase()
    svc = isvc.InventoryService(uc)
    grant = asyncio.run(svc.GrantItems(pb.GrantItemsRequest(player_id=7), _FakeContext(7)))
    assert grant.code == commonpb.ERR_PERMISSION_DENY
    check = asyncio.run(
        svc.CheckItemsOwned(pb.CheckItemsOwnedRequest(player_id=7, item_config_ids=[1]), _FakeContext(7))
    )
    assert check.code == commonpb.ERR_PERMISSION_DENY
    assert uc.calls == []


def test_system_rpc_allows_internal_caller() -> None:
    from pandora.common.v1 import errcode_pb2 as commonpb
    from pandora.inventory.v1 import inventory_pb2 as pb

    uc = _NoopUsecase()
    svc = isvc.InventoryService(uc)
    resp = asyncio.run(
        svc.CheckItemsOwned(
            pb.CheckItemsOwnedRequest(player_id=7, item_config_ids=[1]), _FakeContext(0)
        )
    )
    assert resp.code == commonpb.OK
    assert uc.calls == ["check_items_owned"]


def test_grant_items_requires_player_id() -> None:
    from pandora.common.v1 import errcode_pb2 as commonpb
    from pandora.inventory.v1 import inventory_pb2 as pb

    svc = isvc.InventoryService(_NoopUsecase())
    resp = asyncio.run(svc.GrantItems(pb.GrantItemsRequest(), _FakeContext(0)))
    assert resp.code == commonpb.ERR_INVALID_ARG


def test_business_failure_is_in_band_code_not_grpc_error() -> None:
    """业务失败返回 response.code,gRPC status 保持 OK —— 调用方按 code 分支。"""
    from pandora.common.v1 import errcode_pb2 as commonpb
    from pandora.inventory.v1 import inventory_pb2 as pb

    class _Boom(_NoopUsecase):
        async def get_inventory_full(self, player_id):  # noqa: ANN001, ARG002
            raise errcode.PandoraError(errcode.ErrInventoryItemNotFound, "boom")

    svc = isvc.InventoryService(_Boom())
    resp = asyncio.run(svc.GetInventory(pb.GetInventoryRequest(), _FakeContext(7)))
    assert resp.code == commonpb.ERR_INVENTORY_ITEM_NOT_FOUND
    assert resp.code != commonpb.OK


def test_cancelled_error_is_not_swallowed_by_service_layer() -> None:
    """停机时 grpc.aio 用取消终止在途 handler —— 吞掉 = 客户端收到一批假失败。"""
    from pandora.inventory.v1 import inventory_pb2 as pb

    class _Cancel(_NoopUsecase):
        async def get_inventory_full(self, player_id):  # noqa: ANN001, ARG002
            raise asyncio.CancelledError

    svc = isvc.InventoryService(_Cancel())
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(svc.GetInventory(pb.GetInventoryRequest(), _FakeContext(7)))


def test_instance_proto_keeps_unassigned_slot_as_minus_one() -> None:
    """slot=-1 是"未分配格"的协议约定,客户端据此识别;转成 0 会与真实第 0 格撞车。"""
    inst = ItemInstance(instance_id=1, item_config_id=2, slot_index=-1)
    assert isvc._to_proto_instance(inst).slot_index == -1  # noqa: SLF001


# ── 结算入参:溢出守卫 ────────────────────────────────────────────────────


def test_settle_overflow_is_rejected_not_wrapped() -> None:
    """Python 的 int 不会溢出,但列是 BIGINT —— 判据必须与 Go 一致地拒掉。"""
    uc = ibiz.InventoryUsecase(_FakeRepo(), iconf.InventoryConf())
    with pytest.raises(errcode.PandoraError) as ei:
        asyncio.run(uc.settle_auction_match(1, 5, 6, 2, 3, 7, 2**62, 2**62))
    assert ei.value.code == errcode.ErrInvalidArg


def test_grant_items_rejects_equipment_config() -> None:
    """装备走实例模型:按配置 ID 堆叠会把"同配置不同词条"的两件装备合并成计数。"""
    uc = ibiz.InventoryUsecase(_FakeRepo(), iconf.InventoryConf())
    uc.set_item_catalog(_FakeCatalog(_equip_def(), None))
    with pytest.raises(errcode.PandoraError) as ei:
        asyncio.run(uc.grant_items(1, [ItemGrant(item_config_id=10, count=1)], 0, "k"))
    assert ei.value.code == errcode.ErrInvalidArg


def test_check_instances_owned_rejects_duplicates_and_non_equipment() -> None:
    uc = ibiz.InventoryUsecase(_FakeRepo(), iconf.InventoryConf())
    uc.set_item_catalog(_FakeCatalog(_equip_def(), None))
    dup = [
        InstanceOwnershipQuery(instance_id=1, item_config_id=10),
        InstanceOwnershipQuery(instance_id=1, item_config_id=10),
    ]
    with pytest.raises(errcode.PandoraError) as ei:
        asyncio.run(uc.check_instances_owned(1, dup))
    assert ei.value.code == errcode.ErrInvalidArg
