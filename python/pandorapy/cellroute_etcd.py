"""cellroute 映射表的 etcd 热更新 —— 对应 Go 的 `pkg/cellroute/etcdtable`。

分工与 Go 完全相同:**解析 / 校验全在 `pandorapy.cellroute`(纯函数、可单测),
本模块只做 etcd I/O**(全量 Get 铺初始表 → watch 前缀 → 重新全量 Get → 整表替换)。
把校验混进 I/O 层的代价是它只能靠连着真 etcd 才测得到,而映射表校验恰恰是
"错了不报错"的那类逻辑。

三条不可动摇的语义(与 Go 逐条对应):

  ① **初始表不完整就拒启**。带着半残映射上线 = 一部分玩家被路由到错的 Cell,
     而且要等他们实际访问才发现。fail-fast 是唯一安全的姿态。
  ② **热更是整表替换,不是逐 key 合并**。逐 key 合并存在"改了一半"的瞬间,
     那一瞬被路由的玩家落在旧新混合的映射上(§9.15 配置热更流水线同理)。
  ③ **重载失败保留旧表**。新表非法时替换成空表 / 半表,会把"配置写错了"
     升级成"全服路由不可用"。旧表继续服务 + ERROR 日志才是对的。

★ watch 的定位(§16.10):它**不是**用轮询掩盖时序。收到变更事件后重新全量 Get
  是"重查权威",不是"到点了就假设已经好了"。判别口诀的正确一侧。
"""

from __future__ import annotations

import asyncio
import contextlib

import aetcd

from pandorapy import cellroute, safego
from pandorapy import log as plog

# key = <prefix><logical_cell>,value = "region:cell"。前缀必须与 Go 逐字相同 ——
# 两栈读同一棵 etcd 子树,前缀差一个字符就是各看各的表。
DEFAULT_PREFIX = "/pandora/cellroute/table/"
DEFAULT_DIAL_TIMEOUT_SEC = 5.0


def _parse_kvs(entries, prefix: str) -> dict[int, str]:
    """etcd KV 列表 → `{logical_cell: "region:cell"}`。纯整理,不做语义校验。

    语义校验留给 `cellroute.decode_entries`(那里能单测)。这里只负责把 key 的
    前缀剥掉并断言后缀确实是个 logical_cell —— 后缀不是数字说明前缀配错了或
    这棵子树被别的东西写过,继续解析只会得到一张缺项的表。
    """
    raw: dict[int, str] = {}
    for kv in entries:
        key = kv.key.decode(errors="replace")
        if not key.startswith(prefix):
            raise cellroute.CellRouteError(
                f"cellroute_etcd: key {key!r} lacks prefix {prefix!r}"
            )
        suffix = key[len(prefix) :]
        # ★ 判据是"全 ASCII 十进制"而不是 int() 能否通过。Python 的 int() 比 Go 的
        # strconv.ParseUint 宽得多(接受空白、下划线、Unicode 数字、正负号),
        # 宽在这里不是宽容而是分叉:Go 拒载的表 Python 加载成功。
        if not suffix.isascii() or not suffix.isdigit():
            raise cellroute.CellRouteError(
                f"cellroute_etcd: bad key {key!r} (suffix {suffix!r} not a logical_cell)"
            )
        raw[int(suffix)] = kv.value.decode(errors="replace")
    return raw


class TableWatcher:
    """随 etcd 变更自动热更新的 `AtomicTable` 持有者。对应 Go 的 `etcdtable.Watcher`。"""

    __slots__ = ("_client", "_prefix", "_table", "_task", "_from_revision", "_closed")

    def __init__(
        self,
        client: aetcd.Client,
        prefix: str,
        table: cellroute.AtomicTable,
        from_revision: int,
    ) -> None:
        self._client = client
        self._prefix = prefix
        self._table = table
        self._from_revision = from_revision
        self._task: asyncio.Task | None = None
        self._closed = False

    @property
    def table(self) -> cellroute.AtomicTable:
        """随 etcd 热更新的表,可直接喂 `cellroute.Router`。"""
        return self._table

    def start(self) -> None:
        """起后台 watch。与构造分开是为了让"初始表已铺好"成为一个可断言的中间态。"""
        if self._task is None:
            self._task = safego.spawn("cellroute_etcdtable_watch", self._watch_loop)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None
        with contextlib.suppress(Exception):
            await self._client.close()

    async def _watch_loop(self) -> None:
        logger = plog.get()
        try:
            watcher = await self._client.watch_prefix(
                self._prefix.encode(), start_revision=self._from_revision
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("cellroute_etcdtable_watch_err", err=str(exc))
            return
        try:
            async for _event in watcher:
                # 有任何变更就重新全量 Get + 整表替换。刻意不按事件做逐 key 增量:
                # 映射表小、变更低频,而逐 key 合并会引入"改了一半"的可观测中间态。
                await self._reload()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("cellroute_etcdtable_watch_err", err=str(exc))
        finally:
            logger.warning("cellroute_etcdtable_watch_closed")

    async def _reload(self) -> None:
        """重载一轮。**任何一步失败都保留旧表**(不变量③)。"""
        logger = plog.get()
        try:
            got = await asyncio.wait_for(
                self._client.get_prefix(self._prefix.encode()),
                timeout=DEFAULT_DIAL_TIMEOUT_SEC,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("cellroute_etcdtable_reload_get_err", err=str(exc))
            return
        try:
            raw = _parse_kvs(got, self._prefix)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("cellroute_etcdtable_reload_parse_err", err=str(exc))
            return
        try:
            table = cellroute.build_static_table_from_raw(raw)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 映射不完整 / 非法:保留旧表。映射变更必须经合法**全量**。
            logger.error("cellroute_etcdtable_reload_build_err", err=str(exc))
            return
        try:
            self._table.store(table)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("cellroute_etcdtable_reload_store_err", err=str(exc))
            return
        logger.info("cellroute_etcdtable_reloaded", logical_cells=len(table))


async def start(
    endpoints: list[str],
    prefix: str = "",
    dial_timeout_sec: float = 0.0,
) -> TableWatcher:
    """连 etcd、全量 Get 铺初始表、起 watch。对应 Go 的 `etcdtable.Start`。

    初始映射不完整 / 非法时抛错(不变量①:不带半残映射上线)。
    """
    if not endpoints:
        raise cellroute.CellRouteError("cellroute_etcd: empty endpoints")
    prefix = prefix or DEFAULT_PREFIX
    timeout = dial_timeout_sec if dial_timeout_sec > 0 else DEFAULT_DIAL_TIMEOUT_SEC

    host, _, port = endpoints[0].rpartition(":")
    # 与 dsauthfence.new_etcd_client 同一条判据:endpoint 必须是 host:port。
    # 缺端口时"默认 2379"会让一个写错的 endpoint 连到一台**碰巧存在**的 etcd 上,
    # 拿到一张别的集群的映射表。
    if not port.isascii() or not port.isdigit():
        raise cellroute.CellRouteError(
            f"cellroute_etcd: etcd endpoint must be host:port, got {endpoints[0]!r}"
        )
    client = aetcd.Client(
        host=host or "127.0.0.1", port=int(port), timeout=max(1, int(timeout))
    )
    try:
        await client.connect()
        got = await asyncio.wait_for(client.get_prefix(prefix.encode()), timeout=timeout)
        raw = _parse_kvs(got, prefix)
        initial = cellroute.build_static_table_from_raw(raw)
    except asyncio.CancelledError:
        with contextlib.suppress(Exception):
            await client.close()
        raise
    except BaseException:
        with contextlib.suppress(Exception):
            await client.close()
        raise

    watcher = TableWatcher(
        client,
        prefix,
        cellroute.AtomicTable(initial),
        # 从初始 Get 的 revision+1 开始 watch。用 0(= 当前)会漏掉 Get 与 watch
        # 之间发生的变更,那张过时的表会一直服务到**下一次**变更为止。
        got.header.revision + 1,
    )
    watcher.start()
    plog.get().info(
        "cellroute_etcdtable_started", prefix=prefix, logical_cells=len(initial)
    )
    return watcher


async def build_router(
    cfg: cellroute.RouterConfig,
) -> tuple[cellroute.Router | None, TableWatcher | None]:
    """各服务 main 的统一装配口。对应 Go 的 `etcdtable.BuildRouter`。

    返回 `(router, watcher)`:
      - off:`(None, None)` —— 单 Cell,调用方 nil-safe 回退,行为不变。
      - static:`(router, None)` —— 本地铺表,不连 etcd。
      - etcd:`(router, watcher)` —— watcher 需要在 finally 里 close。
    """
    if cfg.mode != cellroute.MODE_ETCD:
        return cellroute.build_router(cfg), None
    watcher = await start(
        list(cfg.etcd_endpoints), cfg.etcd_prefix, DEFAULT_DIAL_TIMEOUT_SEC
    )
    try:
        return cellroute.Router(watcher.table), watcher
    except BaseException:
        await watcher.close()
        raise
