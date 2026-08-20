"""team 服务业务级 prometheus 指标 —— 对应 Go 侧 internal/biz/metrics.go。

命名规范(docs/design/infra.md §10):

    pandora_team_<metric>{<label>...}

强制 label:service / instance 由抓取端加,代码不写。
禁止高基数 label:player_id / team_id / invite_id 永远不能放 label。

★ 指标名与 label 取值必须与 Go **逐字相同**:告警规则与 Grafana 面板按它们建,
  改一个字等于把告警静默掉,而两边都不报错。
"""

from __future__ import annotations

from prometheus_client import Counter

# 统计邀请推送(kafka produce)丢帧次数。
#
# label:
#   - path:"dedicated"(独立 TeamInviteEvent, event_type=1,含 marshal 失败)
#     | "legacy"(TeamUpdateEvent reason=INVITE_SENT 承载的邀请)。低基数(2 值)。
#
# 触发场景:kafka broker 不可达 / produce 超时 / payload marshal 失败。
# 业务影响:邀请令牌已落库(Invite RPC 返回成功),但被邀请人收不到推送 ——
# 推送是弱依赖,不反向失败主流程;被邀请人靠 ListMyPendingInvites 拉取兜底。
# 此前只有 warning 日志,静默丢通知拖到用户报障才被发现,故必须计数 + 告警。
# 告警阈值:rate(...[5m]) > 0 即告警(正常应恒为 0)。
INVITE_PUSH_FAILED = Counter(
    "pandora_team_invite_push_failed_total",
    "team 服务邀请推送发布失败的总次数"
    "(应恒为 0,> 0 即需要告警;被邀请人由 ListMyPendingInvites 拉取兜底)",
    ["path"],
)

# 统计「闸门检查 → 改队伍」之间 TOCTOU 窗口的命中情况。
#
# label:
#   - outcome:"compensated"(窗口确实被命中,已撤票让全队重新匹配)
#     | "recheck_failed"(复核 RPC 失败,无法判断窗口是否被命中 —— 人已摘走,需人工看)。
#     低基数(2 值)。
#
# 为什么要单独计数:这个窗口跨服务、消不掉,只能收敛后果。它到底多罕见,靠推理说不准,
# 得让线上数据说话 —— 长期为 0 就证明窗口确实极窄;若持续有值,说明「先查闸门后改队伍」
# 这个顺序需要重新设计。recheck_failed 恒为 0 是预期;> 0 即需要人工核对那名玩家的
# 对局与队伍状态。
OFFLINE_LEAVE_RACE = Counter(
    "pandora_team_offline_leave_race_total",
    "离线自动退队在「闸门检查→改队伍」窗口内与匹配组票撞车的次数"
    "(compensated=已撤票补偿;recheck_failed=无法判定,需人工核对)",
    ["outcome"],
)
