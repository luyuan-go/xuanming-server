"""owner 服务配置 —— 对应 Go 侧 internal/conf/conf.go。

默认值必须与 Go 侧**逐个相同**:同一份 etc/*.yaml 会被两个实现读,
默认值一旦分叉,同一份配置在两边跑出不同行为,而且**两边都不报错**。
端口尤其重要 —— Envoy 的 cluster 和 run_services.ps1 的端口占用检查都钉在 20017/21017。
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import BaseModel, Field

from pandorapy import config as pconfig

DEFAULT_GRPC_ADDR = ":20017"
DEFAULT_HTTP_ADDR = ":21017"

# 与 Go 侧同值。
DEFAULT_SWEEP_INTERVAL = "5m"
DEFAULT_SWEEP_BATCH = 500
DEFAULT_LOG_RETENTION_DAYS = 90


class OwnerConf(BaseModel):
    """owner 私有配置段。"""

    # 启动时强校验权威库确为 TiDB(§9.22:MySQL 异步复制切换会回滚已确认写,
    # owner CAS 回滚即可能双 owner)。dev 单机 MySQL 无复制天然线性一致,保持 false;
    # -Prod 产物由 gen_cluster_config.ps1 机械翻 true,不允许线上继承 dev 宽松档。
    require_tidb: bool = False

    # 审计流水保留期清理(§9.24)。
    sweep_interval: str = DEFAULT_SWEEP_INTERVAL
    sweep_batch: int = DEFAULT_SWEEP_BATCH
    log_retention_days: int = DEFAULT_LOG_RETENTION_DAYS

    # INC-20260818-003 分阶段发布最后一步的开关。默认关 = 兼容窗;
    # 打开前必须先证明旧 hub_allocator 已排空,否则仍在跑的旧副本会整体写失败
    # (大厅分配停摆)。
    reject_legacy_source_revision: bool = False

    def sweep_interval_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.sweep_interval)


class Config(pconfig.BaseConf):
    """owner 服务的完整配置。对应 Go 的 conf.Config。"""

    owner: OwnerConf = Field(default_factory=OwnerConf)

    def apply_defaults(self) -> None:
        """填默认值 —— 对应 Go 的 Defaults()。

        零值在这里是危险的:sweep_interval=0 会让清理循环空转成忙等,
        sweep_batch=0 会让每轮删 0 行(看起来在跑、实际永远不清)。
        """
        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR
        if self.owner.sweep_interval_td().total_seconds() <= 0:
            self.owner.sweep_interval = DEFAULT_SWEEP_INTERVAL
        if self.owner.sweep_batch <= 0:
            self.owner.sweep_batch = DEFAULT_SWEEP_BATCH
        if self.owner.log_retention_days <= 0:
            self.owner.log_retention_days = DEFAULT_LOG_RETENTION_DAYS

    @classmethod
    def load(cls, path: str) -> "Config":
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg
