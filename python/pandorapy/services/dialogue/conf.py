"""dialogue 服务私有配置 —— 对应 Go 侧 internal/conf/conf.go。

读的是**同一份** services/social/dialogue/etc/dialogue-dev.yaml,不另建配置文件:
迁移期 Go 版和 Python 版并存,运维只该维护一份配置。

对话树**不在这里** —— 它是策划数值,唯一权威是与 UE 同源的配置表
configtable/dist/dialogue.json(源表 对话/d_对话.xlsx),经 config_table.dir 加载。
本结构只保留服务自己的运行参数。

历史(照抄 Go 侧的警告,因为它仍然有效):对话树曾以 trees/nodes/options 三层内联在
dialogue-dev.yaml,属骨架期 demo 数据;配置表管线接入后整块删除,**不要再加回来**
(YAML 双数据源必然漂移)。
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import Field

from pandorapy import config as pconfig

# 与 Go 侧 conf.DefaultSessionTTL 一致。
DEFAULT_SESSION_TTL = _dt.timedelta(minutes=5)
DEFAULT_GRPC_ADDR = ":20013"
DEFAULT_HTTP_ADDR = ":21013"


class DialogueConf(pconfig.BaseModel):
    """dialogue 服务私有段。"""

    # session_ttl 单次对话会话存活时间。空闲超过此时长的会话会被回收(默认 5m)。
    session_ttl: str = ""

    def session_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.session_ttl)


class Config(pconfig.BaseConf):
    """dialogue 服务的完整配置。对应 Go 的 conf.Config。"""

    dialogue: DialogueConf = Field(default_factory=DialogueConf)

    def apply_defaults(self) -> None:
        """填默认值,防止 yaml 缺字段时零值引发非预期行为。对应 Go 的 Defaults()。

        注意默认值必须和 Go 侧**逐个相同** —— 端口尤其重要:Envoy 的 cluster 和
        run_services.ps1 的端口占用检查都钉在 20013/21013 上。
        """
        if self.dialogue.session_ttl_td().total_seconds() <= 0:
            self.dialogue.session_ttl = "5m"
        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR

    @classmethod
    def load(cls, path: str) -> "Config":
        """从 yaml 加载并填默认值。"""
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg
