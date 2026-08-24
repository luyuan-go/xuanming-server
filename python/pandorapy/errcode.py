"""业务错误码 —— 由 tools/gen_errcode.py 从 pkg/errcode/errcode.go 生成,请勿手改。

重新生成:
    python tools/gen_errcode.py

契约:数值必须与 Go 侧、与 proto 的 pandora.common.v1.ErrCode **完全一致**。
service 层的映射是纯数值转换(`errcode_pb2.ErrCode.ValueType(code)`),
任何一个数值对不上都会让客户端收到错误的语义,且不报错。
"""

from __future__ import annotations

from typing import Final


class PandoraError(Exception):
    """带业务错误码的异常 —— 对应 Go 的 *errcode.Error。

    Go:
        errcode.New(errcode.ErrInvalidArg, "npc_id required")
    Python:
        raise PandoraError(ErrInvalidArg, "npc_id required")

    cause 对应 Go 的 NewCause:保留底层原因供上层判定(如 MySQL 1213 死锁重试),
    但**不改变** code 语义 —— 对客户端只暴露 code/msg,与 Go 侧一致。
    """

    __slots__ = ("code", "msg", "cause", "current_record", "retry_after_ms", "order_state")

    def __init__(self, code: int, msg: str = "", *args: object, cause: BaseException | None = None):
        # Go 侧 New(code, msg, args...) 用 fmt.Sprintf;这里对齐成 %% 风格格式化。
        self.msg = (msg % args) if args else msg
        self.code = code
        self.cause = cause
        # ★ 三个「随错误一起回给调用方的证据」。**通则**:凡 Go 侧写成
        # `(value, err)` 多返回、且失败路径上 value 仍然有意义的地方,Python 用异常
        # 传播就必须把那个 value 显式挂在异常上,否则**静默丢失** —— 调用方拿到零值,
        # 看起来只是"没有信息",实际是"信息被吞了",两者在日志里长得一模一样。
        #   current_record  epoch 冲突时的当前记录 —— §9.23 query-first 重查依据,
        #                   丢了调用方就得再打一次 Query,多一个 TOCTOU 窗口。
        #   retry_after_ms  屏障未开时的等待时长 —— §9.23 要求 WAIT 必须带明确
        #                   retry_after,丢了(=0)调用方要么空转要么干等。
        #   order_state     trade ConfirmOrder 失败时订单"现在停在哪"。Go 的
        #                   biz.ConfirmOrder 在**四条**失败路径上仍回真实状态
        #                   (EXPIRED / FAILED / SELLER_CONFIRMED ×2,见
        #                   services/economy/trade/internal/biz/trade.go:351/398/403/424),
        #                   service 层原样透传(service/trade.go:61-62)。丢了它,
        #                   客户端只看到 code=UNAVAILABLE + new_state=0,判不出该重试
        #                   还是该当订单没动 —— 而"已结算但终态未落库"必须重试。
        # 声明成 slots 而不是随手 setattr:后者能写进去(Exception 自带 __dict__)
        # 但拼写错一个字母不会报错,读的那侧只会永远拿到默认值。
        self.current_record: object | None = None
        self.retry_after_ms: int = 0
        self.order_state: int = 0
        super().__init__(f"errcode={code} {self.msg}")


def as_code(err: BaseException | None) -> int:
    """从异常提取错误码 —— 对应 Go 的 errcode.As(err)。

    语义必须逐条对齐 Go(pkg/errcode/errcode.go:359):
        err 为 None            → OK
        err 是 PandoraError     → 它的 code
        其它异常                → ErrUnknown
    还会沿 __cause__ / __context__ 回溯,对应 Go 的 errors.As 沿 Unwrap 链遍历。
    """
    if err is None:
        return OK
    if isinstance(err, PandoraError):
        return err.code
    seen: set[int] = set()
    cur: BaseException | None = err
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, PandoraError):
            return cur.code
        cur = cur.__cause__ or cur.__context__
    return ErrUnknown


# ── 以下由生成器写入 ──────────────────────────────────────────────────────────

OK: Final[int] = 0
ErrUnknown: Final[int] = 1
ErrInternal: Final[int] = 2
ErrTimeout: Final[int] = 3
ErrInvalidArg: Final[int] = 4
ErrNotFound: Final[int] = 5
ErrAlreadyExists: Final[int] = 6
ErrPermissionDeny: Final[int] = 7
ErrUnauthorized: Final[int] = 8
ErrRateLimited: Final[int] = 9
ErrUnavailable: Final[int] = 10
ErrCanceled: Final[int] = 11
ErrInvalidState: Final[int] = 12
# RPC 被 Kill-Switch 临时关停(维护中,稍后可重试)
ErrServiceDisabled: Final[int] = 13
# ErrNotImplemented 对端**这个版本**还没有这个能力(gRPC `Unimplemented` 的业务侧对应物)。
ErrSessionSuperseded: Final[int] = 14
ErrNotImplemented: Final[int] = 15
ErrLoginAccountNotFound: Final[int] = 1001
ErrLoginPasswordMismatch: Final[int] = 1002
ErrLoginDeviceBanned: Final[int] = 1003
ErrLoginAccountBanned: Final[int] = 1004
ErrLoginTooManyDevices: Final[int] = 1005
ErrLoginTicketExpired: Final[int] = 1010
ErrLoginTicketInvalid: Final[int] = 1011
# 账号 / 角色分离(两步登录,2026-08-18)。
ErrLoginTicketReplayed: Final[int] = 1012
# 角色实体不存在(已删除 / player_id 编造)
ErrLoginRoleNotFound: Final[int] = 1020
# 角色存在但不属于该账号 —— 越权尝试,必须审计
ErrLoginRoleNotOwned: Final[int] = 1021
# 账号角色数达上限(创建角色功能上线后才可能触发)
ErrLoginRoleLimit: Final[int] = 1022
ErrLoginNoRole: Final[int] = 1023
ErrPlayerNotFound: Final[int] = 2001
# 乐观锁冲突
ErrPlayerVersionMismatch: Final[int] = 2002
ErrPlayerNicknameTaken: Final[int] = 2003
ErrPlayerHeroLocked: Final[int] = 2010
ErrPlayerHeroAlreadyOwn: Final[int] = 2011
# 出战养成功能未开启(feature toggle 关闭)
ErrPlayerFeatureDisabled: Final[int] = 2020
# 属性点不足
ErrPlayerInsufficientPoints: Final[int] = 2021
# 该奖励档位已领取(幂等命中)
ErrRewardAlreadyClaimed: Final[int] = 2030
# 奖励配置 ID 非法 / 不在 bit 索引上界内
ErrRewardUnknownID: Final[int] = 2031
# 未持有该技能卡(升级 / 装配前置)
ErrSkillCardNotOwned: Final[int] = 2040
# 已达该卡等级上限
ErrSkillCardMaxLevel: Final[int] = 2041
# 碎片不足
ErrSkillCardInsufficientShards: Final[int] = 2042
# 卡槽序号越界 / 同一张卡占了多个槽
ErrSkillCardSlotInvalid: Final[int] = 2043
ErrTeamNotFound: Final[int] = 3001
ErrTeamFull: Final[int] = 3002
ErrTeamNotCaptain: Final[int] = 3003
ErrTeamAlreadyInTeam: Final[int] = 3004
ErrTeamInviteExpired: Final[int] = 3005
ErrTeamWrongState: Final[int] = 3006
# WATCH/MULTI/EXEC 乐观锁重试耗尽
ErrTeamConcurrent: Final[int] = 3007
# 被邀请人待处理邀请数已达上限(不变量 §9-18 写入侧上限)
ErrTeamInvitePendingLimit: Final[int] = 3008
# 该队伍待处理入队申请数已达上限(不变量 §9-18 写入侧上限)
ErrTeamApplyPendingLimit: Final[int] = 3009
# 入队申请不存在 / 已过期 / 已被处理
ErrTeamApplyNotFound: Final[int] = 3010
ErrMatchNotFound: Final[int] = 4001
ErrMatchAlreadyMatching: Final[int] = 4002
ErrMatchConfirmTimeout: Final[int] = 4003
ErrMatchDeclined: Final[int] = 4004
ErrMatchTeamNotReady: Final[int] = 4005
# WATCH/MULTI/EXEC 乐观锁重试耗尽
ErrMatchConcurrent: Final[int] = 4006
# 玩家正在 DS 战斗中,禁止重复匹配(不变量 §1)
ErrMatchInBattle: Final[int] = 4007
# map_id 不在关卡表或非战斗类关卡,拒绝开匹配
ErrMatchInvalidMap: Final[int] = 4008
# ErrMatchEntryModeDenied 该关卡不允许请求所选的进法,或关卡表两种都开放而请求未明确选择。
ErrMatchTeamTooSmall: Final[int] = 4009
# ErrMatchMemberOffline 队伍里有成员此刻不在大厅(已离线 / 登录了但还没进大厅),
ErrMatchEntryModeDenied: Final[int] = 4010
ErrMatchMemberOffline: Final[int] = 4011
ErrDSNoAvailable: Final[int] = 5001
ErrDSAllocationFailed: Final[int] = 5002
ErrDSPodNotFound: Final[int] = 5003
ErrDSHeartbeatTimeout: Final[int] = 5004
ErrHubNoAvailable: Final[int] = 5101
ErrHubTransferFailed: Final[int] = 5102
# 玩家切线:目标线路已满
ErrHubLineFull: Final[int] = 5103
# 玩家切线:冷却中(防刷)
ErrHubTransferCooldown: Final[int] = 5104
# 玩家切线:不在大厅(战斗/匹配中禁止切线)
ErrHubTransferNotInHub: Final[int] = 5105
# 幂等命中,实际不算错
ErrBattleResultDuplicate: Final[int] = 6001
ErrBattleResultDecode: Final[int] = 6002
ErrBattleResultDBWrite: Final[int] = 6003
ErrTradeOrderNotFound: Final[int] = 7001
ErrTradeOrderExpired: Final[int] = 7002
ErrTradeWrongState: Final[int] = 7003
ErrTradeInsufficient: Final[int] = 7004
# ErrTradeOrderLimit 单玩家参与订单数达上限(写入侧总量上限,不变量 §18)。
ErrTradeLockFailed: Final[int] = 7005
# inventory(背包,同属 economy 域,复用 7000 段)
ErrTradeOrderLimit: Final[int] = 7006
# 道具实例不存在 / 不属于该玩家
ErrInventoryItemNotFound: Final[int] = 7010
# 道具数量不足(扣减/出售/使用)
ErrInventoryInsufficient: Final[int] = 7011
# 该道具不可在大厅使用(战斗内道具走 GAS)
ErrInventoryItemNotUsable: Final[int] = 7012
# 该道具不可出售
ErrInventoryNotSellable: Final[int] = 7013
# 乐观锁重试耗尽(WATCH/MULTI/EXEC 冲突)
ErrInventoryLockFailed: Final[int] = 7014
ErrInventoryIdempotencyConflict: Final[int] = 7015
# 背包格子已满(装备实例数达 capacity 上限)
ErrInventoryCapacityFull: Final[int] = 7016
# 目标格子已被占用 / 越界(移动实例)
ErrInventorySlotOccupied: Final[int] = 7017
# 绑定实例不可托管转移(邮件 transfer 扣出拒;bound=不可交易)
ErrInventoryInstanceBound: Final[int] = 7018
# ErrInventoryCurrencyOverflow 货币入账会越过单币种余额硬上限(MaxCurrencyAmount)。
ErrInventoryNotPurchasable: Final[int] = 7019
ErrInventoryCurrencyOverflow: Final[int] = 7020
ErrDialogueNotFound: Final[int] = 8001
ErrDialogueOptionInvalid: Final[int] = 8002
ErrChatChannelInvalid: Final[int] = 9001
ErrChatMessageTooLong: Final[int] = 9002
ErrChatMuted: Final[int] = 9003
ErrFriendNotFound: Final[int] = 9101
ErrFriendAlreadyAdded: Final[int] = 9102
ErrFriendBlocked: Final[int] = 9103
# 好友数已达上限(AcceptFriend 接受时原子校验)
ErrFriendLimit: Final[int] = 9104
# 收到的好友申请已达上限(target 待处理收件箱满,§9 不变量 18)
ErrFriendRequestLimit: Final[int] = 9105
# 黑名单已达上限(§9 不变量 18)
ErrFriendBlockLimit: Final[int] = 9106
ErrLocatorNotFound: Final[int] = 9201
# 玩家同时在两个 DS
ErrLocatorConflict: Final[int] = 9202
# 离线 ZSET 数据损坏(反序列化失败) / offline.Append 写 redis 失败(W3 ④ 二次修复)
ErrPushOfflineCorrupted: Final[int] = 9301
# kafka consumer 异常下线
ErrPushKafkaConsumerDown: Final[int] = 9302
# 公会不存在
ErrGuildNotFound: Final[int] = 9401
# 玩家已在某公会(单归属)
ErrGuildAlreadyInGuild: Final[int] = 9402
# 公会成员已达上限
ErrGuildFull: Final[int] = 9403
# 操作需会长权限(解散 / 转让 / 任命)
ErrGuildNotLeader: Final[int] = 9404
# 无权限(审批 / 踢人需会长或官员)
ErrGuildNoPermission: Final[int] = 9405
# 公会名已被占用
ErrGuildNameTaken: Final[int] = 9406
# 加入申请不存在 / 非 pending
ErrGuildRequestInvalid: Final[int] = 9407
# 目标不在本公会
ErrGuildNotMember: Final[int] = 9408
# 该公会挂起的加入申请已达上限(§9 不变量 18)
ErrGuildRequestLimit: Final[int] = 9409
# 群不存在
ErrGroupNotFound: Final[int] = 9501
# 群成员已达上限
ErrGroupFull: Final[int] = 9502
# 操作需群主权限(解散 / 转让 / 踢人)
ErrGroupNotOwner: Final[int] = 9503
# 玩家不在群内
ErrGroupNotMember: Final[int] = 9504
# 玩家已在群内(拉人幂等命中)
ErrGroupAlreadyIn: Final[int] = 9505
# 玩家加入的群数量已达上限(§9 不变量 18)
ErrGroupJoinLimit: Final[int] = 9506
# 邮件不存在 / 已删除
ErrMailNotFound: Final[int] = 9601
# 邮件已过期
ErrMailExpired: Final[int] = 9602
# 该邮件无附件可领
ErrMailNoAttachment: Final[int] = 9603
# 附件已领取(幂等命中)
ErrMailAlreadyClaimed: Final[int] = 9604
# 收件箱已满(写入侧上限,§9 不变量 18;驱逐已领邮件后仍满)
ErrMailBoxFull: Final[int] = 9605
# ErrMailClaimInProgress DS 三段式领取意图已创建(bag phase 2):该邮件只能经
ErrMailAttachmentUnsupported: Final[int] = 9606
ErrMailClaimInProgress: Final[int] = 9607
ErrDataVersionMismatch: Final[int] = 10001
ErrDataLockTimeout: Final[int] = 10002
ErrDataMigrate: Final[int] = 10003
# mission_config_id 不在任务表
ErrMissionConfigNotFound: Final[int] = 11001
# 已在活跃列表(重复接取)
ErrMissionAlreadyAccepted: Final[int] = 11002
# 已完成(重复接取 / 放弃已完成任务)
ErrMissionAlreadyCompleted: Final[int] = 11003
# 同 (type, sub_type) 已有活跃任务(类型互斥)
ErrMissionTypeConflict: Final[int] = 11004
# 不在活跃列表(放弃 / 操作目标不存在)
ErrMissionNotAccepted: Final[int] = 11005
# 无可领奖励(未完成 / 自动发 / 已领取)
ErrMissionNotClaimable: Final[int] = 11006
# 活跃任务数达上限(不变量 §9-18 写入侧上限)
ErrMissionActiveLimit: Final[int] = 11007
# 事实上报 idempotency_key 复用到不同内容(指纹不一致)
ErrMissionFactsConflict: Final[int] = 11008
ErrAuctionOrderNotFound: Final[int] = 12001
# 订单已终态 / 不可撤
ErrAuctionWrongState: Final[int] = 12002
# 非挂单本人撤单
ErrAuctionNotOwner: Final[int] = 12003
# 结算资源不足(冻结 / 扣减失败)
ErrAuctionInsufficient: Final[int] = 12004
# idempotency_key 复用到不同请求(指纹不一致)
ErrAuctionIdempotencyConflict: Final[int] = 12005
# market 跨实例单写者锁竞争超时(让客户端稍后重试)
ErrAuctionMarketBusy: Final[int] = 12006
# 单玩家 PENDING + 活跃拍卖订单达到硬上限
ErrAuctionOrderLimit: Final[int] = 12007
# 榜不存在(查询 / 结算空榜)
ErrLeaderboardBoardNotFound: Final[int] = 13001
# entity 不在榜上
ErrLeaderboardEntryNotFound: Final[int] = 13002
# BoardKey 非法(board_type / scope 缺失)
ErrLeaderboardInvalidBoard: Final[int] = 13003
# settle_idempotency_key 复用到不同请求
ErrLeaderboardSettleConflict: Final[int] = 13004
# 结算发奖失败(inventory 不可用 / 扣发异常)
ErrLeaderboardRewardFailed: Final[int] = 13005
# owner_epoch 落后于 bag_meta(失租旧写整批拒)
ErrBagEpochFenced: Final[int] = 14001
# 活动段代际不符(切代后迟到写,fail-closed 整批拒)
ErrBagGenerationMismatch: Final[int] = 14002
# journal_seq 非法(批内乱序 / 与水位冲突且非纯重放)
ErrBagSeqConflict: Final[int] = 14003
# 目标段容量不足(入包 / 转移 / 产出)
ErrBagCapacityFull: Final[int] = 14004
# 超出 journal 额度(单批 / 单窗上限,反作弊封顶)
ErrBagQuotaExceeded: Final[int] = 14005
# idempotency_key 复用到不同内容(指纹不一致)
ErrBagIdempotencyConflict: Final[int] = 14006
# 扣除 / 转移的物品不存在或数量不足(整批拒)
ErrBagItemNotFound: Final[int] = 14007
# checkpoint covered_seq 回退或超出已确认水位
ErrBagCheckpointStale: Final[int] = 14008
# 段类型不合法(查随身段 / checkpoint 含后端驻留段等)
ErrBagSectionNotAllowed: Final[int] = 14009
# 容量扩容档位已购罄 / 达硬上限(§5.3;不可再买,非"袋满")
ErrBagCapacityMaxed: Final[int] = 14010
# expect_epoch 不等于当前(带当前记录,重查再决策)
ErrOwnerEpochConflict: Final[int] = 15001
# now < admit_not_before(带 retry_after,退避重试)
ErrOwnerBarrierNotOpen: Final[int] = 15002
# epoch/operation/实例四元组不一致(fail-closed)
ErrOwnerIdentityMismatch: Final[int] = 15003
# 续租实例身份不符或 deadline 回退
ErrOwnerLeaseRegressed: Final[int] = 15004
# operation_id 非法或目标身份不完整
ErrOwnerInvalidOperation: Final[int] = 15005
ErrOwnerSourceRevisionStale: Final[int] = 15006

# 名字 → 数值,供 parity 测试与调试反查使用。
ALL_CODES: Final[dict[str, int]] = {
    "OK": 0,
    "ErrUnknown": 1,
    "ErrInternal": 2,
    "ErrTimeout": 3,
    "ErrInvalidArg": 4,
    "ErrNotFound": 5,
    "ErrAlreadyExists": 6,
    "ErrPermissionDeny": 7,
    "ErrUnauthorized": 8,
    "ErrRateLimited": 9,
    "ErrUnavailable": 10,
    "ErrCanceled": 11,
    "ErrInvalidState": 12,
    "ErrServiceDisabled": 13,
    "ErrSessionSuperseded": 14,
    "ErrNotImplemented": 15,
    "ErrLoginAccountNotFound": 1001,
    "ErrLoginPasswordMismatch": 1002,
    "ErrLoginDeviceBanned": 1003,
    "ErrLoginAccountBanned": 1004,
    "ErrLoginTooManyDevices": 1005,
    "ErrLoginTicketExpired": 1010,
    "ErrLoginTicketInvalid": 1011,
    "ErrLoginTicketReplayed": 1012,
    "ErrLoginRoleNotFound": 1020,
    "ErrLoginRoleNotOwned": 1021,
    "ErrLoginRoleLimit": 1022,
    "ErrLoginNoRole": 1023,
    "ErrPlayerNotFound": 2001,
    "ErrPlayerVersionMismatch": 2002,
    "ErrPlayerNicknameTaken": 2003,
    "ErrPlayerHeroLocked": 2010,
    "ErrPlayerHeroAlreadyOwn": 2011,
    "ErrPlayerFeatureDisabled": 2020,
    "ErrPlayerInsufficientPoints": 2021,
    "ErrRewardAlreadyClaimed": 2030,
    "ErrRewardUnknownID": 2031,
    "ErrSkillCardNotOwned": 2040,
    "ErrSkillCardMaxLevel": 2041,
    "ErrSkillCardInsufficientShards": 2042,
    "ErrSkillCardSlotInvalid": 2043,
    "ErrTeamNotFound": 3001,
    "ErrTeamFull": 3002,
    "ErrTeamNotCaptain": 3003,
    "ErrTeamAlreadyInTeam": 3004,
    "ErrTeamInviteExpired": 3005,
    "ErrTeamWrongState": 3006,
    "ErrTeamConcurrent": 3007,
    "ErrTeamInvitePendingLimit": 3008,
    "ErrTeamApplyPendingLimit": 3009,
    "ErrTeamApplyNotFound": 3010,
    "ErrMatchNotFound": 4001,
    "ErrMatchAlreadyMatching": 4002,
    "ErrMatchConfirmTimeout": 4003,
    "ErrMatchDeclined": 4004,
    "ErrMatchTeamNotReady": 4005,
    "ErrMatchConcurrent": 4006,
    "ErrMatchInBattle": 4007,
    "ErrMatchInvalidMap": 4008,
    "ErrMatchTeamTooSmall": 4009,
    "ErrMatchEntryModeDenied": 4010,
    "ErrMatchMemberOffline": 4011,
    "ErrDSNoAvailable": 5001,
    "ErrDSAllocationFailed": 5002,
    "ErrDSPodNotFound": 5003,
    "ErrDSHeartbeatTimeout": 5004,
    "ErrHubNoAvailable": 5101,
    "ErrHubTransferFailed": 5102,
    "ErrHubLineFull": 5103,
    "ErrHubTransferCooldown": 5104,
    "ErrHubTransferNotInHub": 5105,
    "ErrBattleResultDuplicate": 6001,
    "ErrBattleResultDecode": 6002,
    "ErrBattleResultDBWrite": 6003,
    "ErrTradeOrderNotFound": 7001,
    "ErrTradeOrderExpired": 7002,
    "ErrTradeWrongState": 7003,
    "ErrTradeInsufficient": 7004,
    "ErrTradeLockFailed": 7005,
    "ErrTradeOrderLimit": 7006,
    "ErrInventoryItemNotFound": 7010,
    "ErrInventoryInsufficient": 7011,
    "ErrInventoryItemNotUsable": 7012,
    "ErrInventoryNotSellable": 7013,
    "ErrInventoryLockFailed": 7014,
    "ErrInventoryIdempotencyConflict": 7015,
    "ErrInventoryCapacityFull": 7016,
    "ErrInventorySlotOccupied": 7017,
    "ErrInventoryInstanceBound": 7018,
    "ErrInventoryNotPurchasable": 7019,
    "ErrInventoryCurrencyOverflow": 7020,
    "ErrDialogueNotFound": 8001,
    "ErrDialogueOptionInvalid": 8002,
    "ErrChatChannelInvalid": 9001,
    "ErrChatMessageTooLong": 9002,
    "ErrChatMuted": 9003,
    "ErrFriendNotFound": 9101,
    "ErrFriendAlreadyAdded": 9102,
    "ErrFriendBlocked": 9103,
    "ErrFriendLimit": 9104,
    "ErrFriendRequestLimit": 9105,
    "ErrFriendBlockLimit": 9106,
    "ErrLocatorNotFound": 9201,
    "ErrLocatorConflict": 9202,
    "ErrPushOfflineCorrupted": 9301,
    "ErrPushKafkaConsumerDown": 9302,
    "ErrGuildNotFound": 9401,
    "ErrGuildAlreadyInGuild": 9402,
    "ErrGuildFull": 9403,
    "ErrGuildNotLeader": 9404,
    "ErrGuildNoPermission": 9405,
    "ErrGuildNameTaken": 9406,
    "ErrGuildRequestInvalid": 9407,
    "ErrGuildNotMember": 9408,
    "ErrGuildRequestLimit": 9409,
    "ErrGroupNotFound": 9501,
    "ErrGroupFull": 9502,
    "ErrGroupNotOwner": 9503,
    "ErrGroupNotMember": 9504,
    "ErrGroupAlreadyIn": 9505,
    "ErrGroupJoinLimit": 9506,
    "ErrMailNotFound": 9601,
    "ErrMailExpired": 9602,
    "ErrMailNoAttachment": 9603,
    "ErrMailAlreadyClaimed": 9604,
    "ErrMailBoxFull": 9605,
    "ErrMailAttachmentUnsupported": 9606,
    "ErrMailClaimInProgress": 9607,
    "ErrDataVersionMismatch": 10001,
    "ErrDataLockTimeout": 10002,
    "ErrDataMigrate": 10003,
    "ErrMissionConfigNotFound": 11001,
    "ErrMissionAlreadyAccepted": 11002,
    "ErrMissionAlreadyCompleted": 11003,
    "ErrMissionTypeConflict": 11004,
    "ErrMissionNotAccepted": 11005,
    "ErrMissionNotClaimable": 11006,
    "ErrMissionActiveLimit": 11007,
    "ErrMissionFactsConflict": 11008,
    "ErrAuctionOrderNotFound": 12001,
    "ErrAuctionWrongState": 12002,
    "ErrAuctionNotOwner": 12003,
    "ErrAuctionInsufficient": 12004,
    "ErrAuctionIdempotencyConflict": 12005,
    "ErrAuctionMarketBusy": 12006,
    "ErrAuctionOrderLimit": 12007,
    "ErrLeaderboardBoardNotFound": 13001,
    "ErrLeaderboardEntryNotFound": 13002,
    "ErrLeaderboardInvalidBoard": 13003,
    "ErrLeaderboardSettleConflict": 13004,
    "ErrLeaderboardRewardFailed": 13005,
    "ErrBagEpochFenced": 14001,
    "ErrBagGenerationMismatch": 14002,
    "ErrBagSeqConflict": 14003,
    "ErrBagCapacityFull": 14004,
    "ErrBagQuotaExceeded": 14005,
    "ErrBagIdempotencyConflict": 14006,
    "ErrBagItemNotFound": 14007,
    "ErrBagCheckpointStale": 14008,
    "ErrBagSectionNotAllowed": 14009,
    "ErrBagCapacityMaxed": 14010,
    "ErrOwnerEpochConflict": 15001,
    "ErrOwnerBarrierNotOpen": 15002,
    "ErrOwnerIdentityMismatch": 15003,
    "ErrOwnerLeaseRegressed": 15004,
    "ErrOwnerInvalidOperation": 15005,
    "ErrOwnerSourceRevisionStale": 15006,
}
