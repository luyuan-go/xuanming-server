"""拍卖「市场 → 实例归属」路由(rendezvous / HRW 一致性哈希),纯函数。
对应 Go 侧 services/economy/auction/internal/biz/market_router.go。

同一 market_id 固定路由到同一撮合实例,让「跨实例 per-market 单写者」的锁竞争降到最低
(绝大多数时间同一 market 只有 owner 实例在撮合 → MarketLocker 几乎不抢锁);
路由抖动 / rebalance 时仍由 MarketLocker(Redis 单写者 token)兜底跨实例互斥,不会超卖。

★ 哈希细节**必须与 Go 逐位相同**,否则迁移期 Go 副本与 Python 副本会对同一个
  market_id 算出不同 owner —— 两边都以为自己是 owner,双写窗口被打开,
  而两边日志都只会打一条"非 owner 实例处理"的 WARN(还是打在**对方**那边)。
  所以这里的 FNV-1a / hash_combine / splitmix64 全部按位照抄,不许"用 Python 的
  hash() 省事"(那个还带每进程随机盐,连自己都对不上)。
"""

from __future__ import annotations

_MASK64 = (1 << 64) - 1

# FNV-1a 64 位标准常量(Go 的 hash/fnv 用的就是这两个)。
_FNV64_OFFSET = 0xCBF29CE484222325
_FNV64_PRIME = 0x100000001B3


def _fnv1a64(data: bytes) -> int:
    """FNV-1a 64。与 Go 的 `fnv.New64a().Write(data).Sum64()` 逐位相同。"""
    h = _FNV64_OFFSET
    for b in data:
        h ^= b
        h = (h * _FNV64_PRIME) & _MASK64
    return h


def _fnv1a64_uint32(v: int) -> int:
    """把 uint32 按**大端 4 字节**喂进 FNV-1a。

    ★ 字节序是契约:Go 侧写的是 `{byte(v>>24), byte(v>>16), byte(v>>8), byte(v)}`。
    改成小端不会报错,只会让同一个 market 在两栈上算出不同 owner。
    """
    v &= 0xFFFFFFFF
    return _fnv1a64(bytes(((v >> 24) & 0xFF, (v >> 16) & 0xFF, (v >> 8) & 0xFF, v & 0xFF)))


def _splitmix64(z: int) -> int:
    """标准强混合 finalizer(良好 64 位雪崩),用于 HRW 评分去相关。"""
    z = (z + 0x9E3779B97F4A7C15) & _MASK64
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & _MASK64
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & _MASK64
    return (z ^ (z >> 31)) & _MASK64


def hrw_score(peer: str, market_id: int) -> int:
    """计算 (peer, market) 的 rendezvous 权重。对应 Go 的 hrwScore。

    分别独立哈希 peer 与 market,再 hash_combine + splitmix64 收尾:即便实例 ID
    仅末位不同(n1/n2/n3…),也能在不同 market 上均匀轮流胜出。单遍 FNV(把 market、
    peer 顺序写进同一哈希)对这种"近似 ID"扩散不足,会让某实例恒定胜出。
    """
    hn = _fnv1a64(peer.encode("utf-8"))
    hk = _fnv1a64_uint32(market_id)
    z = hn ^ ((hk + 0x9E3779B97F4A7C15 + ((hn << 6) & _MASK64) + (hn >> 2)) & _MASK64)
    return _splitmix64(z & _MASK64)


class MarketRouter:
    """决定某 market_id 由哪个 auction 实例独占撮合。构造后不可变,并发只读安全。"""

    __slots__ = ("_self", "_peers")

    def __init__(self, self_id: str, peers: list[str]) -> None:
        self._self = self_id
        self._peers = tuple(peers)

    @classmethod
    def build(cls, self_id: str, peers: list[str]) -> "MarketRouter | None":
        """构造路由器。对应 Go 的 `NewMarketRouter(self, peers) (*MarketRouter, bool)`。

        self 为空 → None(单实例,本实例拥有全部 market,退化为现状)。
        peers 去重后必须含 self;不含则补入(Go 同)。
        """
        if not self_id:
            return None
        seen: set[str] = set()
        dedup: list[str] = []
        for peer in peers:
            if not peer:
                continue
            if peer in seen:
                continue
            seen.add(peer)
            dedup.append(peer)
        if self_id not in seen:
            dedup.append(self_id)
        return cls(self_id, dedup)

    def self_id(self) -> str:
        return self._self

    def peer_count(self) -> int:
        return len(self._peers)

    def owner(self, market_id: int) -> str:
        """某 market_id 的归属实例(HRW:取 hash(peer, market) 最大者)。

        权重并列时按实例 ID **字典序较大者**取胜(确定性 tiebreak,与 Go 同)。
        ★ Python 的字符串比较是按 Unicode 码点,Go 的 `p > bestPeer` 是按字节;
          实例 ID 是 ASCII(部署产物里是 `auction-0` 这类),两者一致。
        """
        if not self._peers:
            return self._self
        best_peer = ""
        best_score = 0
        for peer in self._peers:
            score = hrw_score(peer, market_id)
            if not best_peer or score > best_score or (score == best_score and peer > best_peer):
                best_peer = peer
                best_score = score
        return best_peer

    def owns_market(self, market_id: int) -> bool:
        return self.owner(market_id) == self._self
