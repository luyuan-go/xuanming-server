"""mail 服务私有配置 —— 对应 Go 侧 services/social/mail/internal/conf/conf.go。

读的是**同一份** services/social/mail/etc/mail-dev.yaml,不另建配置文件:
迁移期 Go 版和 Python 版并存,运维只该维护一份配置。

★ 默认值必须与 Go 侧 Defaults() **逐个相同**,判据符号也要一样(Go 全用 `<= 0`)。
  分叉的后果不是报错,是同一份 yaml 在两个实现上跑出不同行为而**两边都不报错**:
  比如 claim_retention_days 若在 Python 侧默认成 90,发送侧对 end_ms 的钳制窗口
  就比 Go 短 90 天 —— 表现为"某些邮件在 Go 版能领、Python 版领不了",
  查到最后才发现是一个默认值。

端口默认值(20009/21009)同样是契约:Envoy 的 cluster、run_services.ps1 的端口占用
检查、K8s Service 都钉在这两个数上。
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import BaseModel, Field

from pandorapy import config as pconfig

DEFAULT_GRPC_ADDR = ":20009"
DEFAULT_HTTP_ADDR = ":21009"

# 与 Go 侧 conf.DefaultMaxInstancesPerMail / DefaultMaxStackCountPerAttachment 同名同值。
#
# ★ 这两个"上限"的零值语义**不是禁用**:归一化成默认值而不是"拒绝一切实例附件"。
#   一个上限的零值若等于封禁功能,任何漏配 / 直接构造 conf 的路径都会静默把正常业务
#   打死(§14.2:默认值必须保证现有行为不变)。
DEFAULT_MAX_INSTANCES_PER_MAIL = 128
DEFAULT_MAX_STACK_COUNT_PER_ATTACHMENT = 1_000_000

# 其余默认值,与 Go 的 Defaults() 一一对应。
DEFAULT_SYS_TTL_DAYS = 7
DEFAULT_PERSONAL_TTL_DAYS = 30
DEFAULT_MAX_INBOX_SIZE = 200
DEFAULT_SWEEP_INTERVAL = "5m"
DEFAULT_SWEEP_BATCH = 500
DEFAULT_EXPIRED_RETENTION_DAYS = 7
DEFAULT_ARCHIVE_RETENTION_DAYS = 90
# ★ 180 天是 §9.24「失效数据最多留 90 天」的**登记例外**,不是随手写大的数:
#   发送侧把一切邮件的 end_ms 钳到「创建时刻 + 本值」以内,保证 claim 行的寿命
#   ≥ 邮件可领窗口。改小它 = claim 行先于邮件消失 = 重复领取(inventory 的幂等流水
#   自己只留 90 天,兜不住)。
DEFAULT_CLAIM_RETENTION_DAYS = 180
DEFAULT_MAX_TITLE_LEN = 64
DEFAULT_MAX_BODY_LEN = 2048
DEFAULT_MAX_ATTACHMENTS = 16


class SessionGateConf(BaseModel):
    """会话现行性门参数 —— 对应 Go 的 pkg/config.SessionGateConf。

    ★ 本该建在 pandorapy/config.py 的 BaseConf 上(Go 侧它就在 config.Base 里),
      但那是本批次禁改的共享文件,所以先在 mail 这里建模。
      不建模的后果不是"读不到":BaseConf 是 extra="allow",session_gate 会整段
      落进 model_extra —— yaml 里 require=true 写着,Python 侧当没看见,
      **prod 的强制档静默退化成 dev 宽松档**。这正是这一族缺陷的形状。
    """

    # true = 强制档(prod 生成器机械置):权威端点漏配拒启;gate 未装配时带会话证据的
    # 请求一律 fail-closed。false = dev 宽松档(无 Redis 直连联调可跳过现行性判定;
    # 但 gate 已装配时无论档位,顶号 / 登出 / 权威查询失败都照常拒)。
    require: bool = False
    # 仅 hub_allocator 使用;mail 不读它,建模只为不落进 extra。
    require_ticket_sjti: bool = False


class MailConf(BaseModel):
    """mail 服务私有配置段。对应 Go 的 conf.MailConf。"""

    # 系统/公会邮件默认有效期天数(end_ms 为 0 时补)。
    default_sys_ttl_days: int = 0
    # 个人邮件默认有效期天数(expire_ms 为 0 时补)。
    # ★ "一切邮件生命有限"是 sweep 能清理的前提:没有默认 TTL,库只增不减。
    default_personal_ttl_days: int = 0
    # 单玩家收件箱行数上限(§9 不变量 18,写入侧上限)。
    # 满时先驱逐最旧的已领邮件,仍满返回 ERR_MAIL_BOX_FULL —— 调用方靠补扫重试。
    max_inbox_size: int = 0
    # 过期清理轮询间隔。多副本各自跑,删除幂等无需锁(对齐 leaderboard 补扫模式)。
    sweep_interval: str = ""
    # 每轮每表清理行数上限。小批量是为了不起长事务锁表:
    # 一次删几十万行会把 player_mail 锁住,在线玩家的收件箱直接卡死。
    sweep_batch: int = 0
    # 过期后延迟物理清理的缓冲天数:过期邮件先只是不可见,缓冲期后才删/归档,
    # 留客诉排查窗口,也吸收各节点时钟偏差。
    expired_retention_days: int = 0
    # 归档表保留天数(超期物理清除,保证归档表自身也有界 —— 归档不是"永久保存")。
    archive_retention_days: int = 0
    # 领取记录保留天数。见 DEFAULT_CLAIM_RETENTION_DAYS 的说明,它与发送侧的
    # end_ms 钳制是同一条不变量的两半,不可单独改。
    claim_retention_days: int = 0
    # 标题 / 正文长度上限(utf8 rune 数,不是字节)。
    max_title_len: int = 0
    max_body_len: int = 0
    # 单封邮件附件条数上限。
    max_attachments: int = 0
    # 单封邮件展开后的**实例件数**累计上限(跨全部附件累加)。
    # ★ 按累计算而不是按单附件算:16 个附件各刷满才是真实最坏值,只卡单附件挡不住。
    max_instances_per_mail: int = 0
    # 单个可堆叠附件的 count 上限。stack 的 count 是数量不是循环次数,不构成 DoS;
    # 这道闸只挡"一封邮件声明发 42 亿个道具"这种明显不合理的输入,
    # 真正的经济上限由 inventory 的容量 / 堆叠规则权威裁定。
    max_stack_count_per_attachment: int = 0
    # inventory 服务 gRPC 地址(host:port),领附件入库用。
    inventory_addr: str = ""
    # inventory 不可用时允许空领(只标记 claim,不真发),**仅测试环境**。
    # ⚠️ 它对 transfer 形态**不放行**(见 biz):空领会把邮件标成已领而托管行原地不动,
    # 实例资产静默滞留 escrow。
    allow_noop_grant: bool = False

    def sweep_interval_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.sweep_interval)


class Config(pconfig.BaseConf):
    """mail 服务的完整配置。对应 Go 的 conf.Config。"""

    mail: MailConf = Field(default_factory=MailConf)
    session_gate: SessionGateConf = Field(default_factory=SessionGateConf)

    def apply_defaults(self) -> None:
        """填默认值 —— 对应 Go 的 Defaults(),逐条同序同判据(全部 `<= 0`)。

        零值在这里全是危险的而不是"无限":
          sweep_interval=0 会让清理循环退化成忙等(每轮立刻返回,烧满一个核);
          sweep_batch=0 会让每轮删 0 行 —— 循环在跑、日志正常、表永远不清。
        """
        if self.mail.default_sys_ttl_days <= 0:
            self.mail.default_sys_ttl_days = DEFAULT_SYS_TTL_DAYS
        if self.mail.default_personal_ttl_days <= 0:
            self.mail.default_personal_ttl_days = DEFAULT_PERSONAL_TTL_DAYS
        if self.mail.max_inbox_size <= 0:
            self.mail.max_inbox_size = DEFAULT_MAX_INBOX_SIZE
        if self.mail.sweep_interval_td().total_seconds() <= 0:
            self.mail.sweep_interval = DEFAULT_SWEEP_INTERVAL
        if self.mail.sweep_batch <= 0:
            self.mail.sweep_batch = DEFAULT_SWEEP_BATCH
        if self.mail.expired_retention_days <= 0:
            self.mail.expired_retention_days = DEFAULT_EXPIRED_RETENTION_DAYS
        if self.mail.archive_retention_days <= 0:
            self.mail.archive_retention_days = DEFAULT_ARCHIVE_RETENTION_DAYS
        if self.mail.claim_retention_days <= 0:
            self.mail.claim_retention_days = DEFAULT_CLAIM_RETENTION_DAYS
        if self.mail.max_title_len <= 0:
            self.mail.max_title_len = DEFAULT_MAX_TITLE_LEN
        if self.mail.max_body_len <= 0:
            self.mail.max_body_len = DEFAULT_MAX_BODY_LEN
        if self.mail.max_attachments <= 0:
            self.mail.max_attachments = DEFAULT_MAX_ATTACHMENTS
        if self.mail.max_instances_per_mail <= 0:
            self.mail.max_instances_per_mail = DEFAULT_MAX_INSTANCES_PER_MAIL
        if self.mail.max_stack_count_per_attachment <= 0:
            self.mail.max_stack_count_per_attachment = DEFAULT_MAX_STACK_COUNT_PER_ATTACHMENT
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
