"""data_service 服务私有配置 —— 对应 Go 侧 internal/conf/conf.go。

读的是**同一份** services/data/data_service/etc/data_service-dev.yaml,不另建配置文件:
迁移期 Go 版和 Python 版并存,运维只该维护一份配置。

★ 默认值必须与 Go 的 `Defaults()` 逐个相同,判据符号也要一样。
  分叉的后果不是"报错",而是**同一份 yaml 喂两个实现跑出不同行为、两边都不报错**:
  cache_ttl 分叉 → 两个实现往同一个 Redis key 写不同 TTL,缓存命中率随副本调度漂移;
  端口分叉 → Envoy 的 cluster 和 run_services.ps1 的端口占用检查都钉在 20003/21003,
  Python 副本会监听在别处而 Envoy 依旧往 20003 发,表现是"服务起来了但没人调得到"。
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import BaseModel, Field

from pandorapy import config as pconfig

# 与 Go 侧 Defaults() 逐字同值。
DEFAULT_GRPC_ADDR = ":20003"
DEFAULT_HTTP_ADDR = ":21003"
DEFAULT_CACHE_TTL = "5m"


class DataConf(BaseModel):
    """data_service 服务私有段。对应 Go 的 conf.DataConf。"""

    # cache_ttl Redis 缓存条目存活时长(默认 5m)。
    # cache-aside:读 miss 回填时按此 TTL,写后删缓存。
    cache_ttl: str = ""

    def cache_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.cache_ttl)


class Config(pconfig.BaseConf):
    """data_service 服务的完整配置。对应 Go 的 conf.Config。"""

    data: DataConf = Field(default_factory=DataConf)

    def apply_defaults(self) -> None:
        """填默认值。对应 Go 的 Defaults()。

        ★ cache_ttl 判据用 `<= 0` 而不是 `== ""`,与 Go 的 `if c.Data.CacheTTL <= 0` 一致。
          差别是实打实的:yaml 写 `cache_ttl: "0s"` 时,`== ""` 会放行 0 值,
          于是回填缓存时 TTL 为 0 —— redis-py 的 px=0 是非法参数(data.py 里靠
          `max(ms, 1)` 兜成 1ms),等于缓存写进去就立刻过期,命中率恒 0 而没有任何错误日志。
          Go 在同一份 yaml 下会把它纠成 5m。判据符号本身就是行为契约。
        """
        if self.data.cache_ttl_td().total_seconds() <= 0:
            self.data.cache_ttl = DEFAULT_CACHE_TTL
        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR

    @classmethod
    def load(cls, path: str) -> "Config":
        """从 yaml 加载并填默认值。对应 Go 的 kconfig Load + Scan + Defaults。"""
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg
