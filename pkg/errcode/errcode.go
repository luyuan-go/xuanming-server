// Package errcode 定义 Pandora 全局错误码与封装。
//
// 错误码段规划(docs/design/proto-design.md §4 / infra.md):
//
//	0           = OK
//	1-999       = 公共错(网络、超时、参数、权限)
//	1000-1999   = login
//	2000-2999   = player
//	3000-3999   = team
//	4000-4999   = match
//	5000-5999   = ds_allocator
//	6000-6999   = battle_result
//	7000-7999   = trade
//	8000-8999   = dialogue
//	9000-9999   = chat / friend / locator / push / guild / group
//	10000-10999 = data_service
//	11000-11999 = mission(任务)
//	12000-12999 = auction(全服拍卖行 / 撮合)
//	13000-13999 = leaderboard(通用排行榜)
//
// 字段编号永不复用,只 deprecate。
package errcode

import (
	"errors"
	"fmt"
)

// Code 是 Pandora 错误码的强类型。0 表示成功。
type Code int32

// 公共错(1-999)
const (
	OK Code = 0

	ErrUnknown         Code = 1
	ErrInternal        Code = 2
	ErrTimeout         Code = 3
	ErrInvalidArg      Code = 4
	ErrNotFound        Code = 5
	ErrAlreadyExists   Code = 6
	ErrPermissionDeny  Code = 7
	ErrUnauthorized    Code = 8
	ErrRateLimited     Code = 9
	ErrUnavailable     Code = 10
	ErrCanceled        Code = 11
	ErrInvalidState    Code = 12
	ErrServiceDisabled Code = 13 // RPC 被 Kill-Switch 临时关停(维护中,稍后可重试)
	// ErrSessionSuperseded 会话已被同账号更新的登录顶替(顶号)。必须与 ErrUnauthorized
	// (自然过期/登出,客户端允许用缓存凭据自动换新会话)可判别:被顶设备若对本码自动
	// 完整 Login,会轮换服务端会话 jti 反顶新设备,两台设备形成互踢死循环
	// (INC-20260722-004 R4 P0)。客户端对本码只能转交互登录,由玩家显式决定是否夺回会话。
	ErrSessionSuperseded Code = 14
	// ErrNotImplemented 对端**这个版本**还没有这个能力(gRPC `Unimplemented` 的业务侧对应物)。
	//
	// 它与 ErrUnavailable(10)的区别是**能不能靠重试解决**,这个区别决定调用方的行为:
	//   - ErrUnavailable  = 对端会做,只是这次没做成 → 退避重试,重试终将成功;
	//   - ErrNotImplemented = 对端根本不会做 → **重试永远不会成功**,只能等它滚上新版本。
	//
	// 用途是 §9.21 滚动升级的共存窗口:新调用方先于被调方上线时,若把
	// `Unimplemented` 当成普通失败无限重试,补偿链(outbox 等)会一直空转并积压。
	// 正确姿势是把它识别成「对端还没升级」——按弱依赖降级放行、留 Warn 与指标,
	// 等被调方上线后新请求自然恢复。**降级必须是真的安全**(跳过这一步不会破坏正确性),
	// 否则应当继续 fail-closed 而不是套用本码。
	ErrNotImplemented Code = 15
)

// login(1000-1999)
const (
	ErrLoginAccountNotFound  Code = 1001
	ErrLoginPasswordMismatch Code = 1002
	ErrLoginDeviceBanned     Code = 1003
	ErrLoginAccountBanned    Code = 1004
	ErrLoginTooManyDevices   Code = 1005
	ErrLoginTicketExpired    Code = 1010
	ErrLoginTicketInvalid    Code = 1011
	ErrLoginTicketReplayed   Code = 1012

	// 账号 / 角色分离(两步登录,2026-08-18)。
	//
	// 这几个码刻意与 ErrUnauthorized / ErrNotFound 区分开:选角失败是玩家能看懂、
	// 且客户端要分别处理的状态(重登 vs 刷新角色列表 vs 提示上限),混进泛化码后
	// 客户端只能一律弹「登录失败」。
	ErrLoginRoleNotFound Code = 1020 // 角色实体不存在(已删除 / player_id 编造)
	ErrLoginRoleNotOwned Code = 1021 // 角色存在但不属于该账号 —— 越权尝试,必须审计
	ErrLoginRoleLimit    Code = 1022 // 账号角色数达上限(创建角色功能上线后才可能触发)
	// ErrLoginNoRole 账号名下一个可用角色都没有。
	//
	// 除了「还没创建角色」这个字面含义,它还是 Login 的 fail-closed 出口:唯一的角色
	// 被软删(status!=0)或已过户到别的账号时,登录必须以本码被拒 —— 绝不回落
	// accounts.player_id 把玩家送进一个已经不属于他的角色(见 login 的 resolveAccountView)。
	ErrLoginNoRole Code = 1023
)

// player(2000-2999)
const (
	ErrPlayerNotFound           Code = 2001
	ErrPlayerVersionMismatch    Code = 2002 // 乐观锁冲突
	ErrPlayerNicknameTaken      Code = 2003
	ErrPlayerHeroLocked         Code = 2010
	ErrPlayerHeroAlreadyOwn     Code = 2011
	ErrPlayerFeatureDisabled    Code = 2020 // 出战养成功能未开启(feature toggle 关闭)
	ErrPlayerInsufficientPoints Code = 2021 // 属性点不足
	ErrRewardAlreadyClaimed     Code = 2030 // 该奖励档位已领取(幂等命中)
	ErrRewardUnknownID          Code = 2031 // 奖励配置 ID 非法 / 不在 bit 索引上界内

	ErrSkillCardNotOwned           Code = 2040 // 未持有该技能卡(升级 / 装配前置)
	ErrSkillCardMaxLevel           Code = 2041 // 已达该卡等级上限
	ErrSkillCardInsufficientShards Code = 2042 // 碎片不足
	ErrSkillCardSlotInvalid        Code = 2043 // 卡槽序号越界 / 同一张卡占了多个槽
)

// team(3000-3999)
const (
	ErrTeamNotFound           Code = 3001
	ErrTeamFull               Code = 3002
	ErrTeamNotCaptain         Code = 3003
	ErrTeamAlreadyInTeam      Code = 3004
	ErrTeamInviteExpired      Code = 3005
	ErrTeamWrongState         Code = 3006
	ErrTeamConcurrent         Code = 3007 // WATCH/MULTI/EXEC 乐观锁重试耗尽
	ErrTeamInvitePendingLimit Code = 3008 // 被邀请人待处理邀请数已达上限(不变量 §9-18 写入侧上限)
	ErrTeamApplyPendingLimit  Code = 3009 // 该队伍待处理入队申请数已达上限(不变量 §9-18 写入侧上限)
	ErrTeamApplyNotFound      Code = 3010 // 入队申请不存在 / 已过期 / 已被处理
)

// match(4000-4999)
const (
	ErrMatchNotFound        Code = 4001
	ErrMatchAlreadyMatching Code = 4002
	ErrMatchConfirmTimeout  Code = 4003
	ErrMatchDeclined        Code = 4004
	ErrMatchTeamNotReady    Code = 4005
	ErrMatchConcurrent      Code = 4006 // WATCH/MULTI/EXEC 乐观锁重试耗尽
	ErrMatchInBattle        Code = 4007 // 玩家正在 DS 战斗中,禁止重复匹配(不变量 §1)
	ErrMatchInvalidMap      Code = 4008 // map_id 不在关卡表或非战斗类关卡,拒绝开匹配
	// ErrMatchTeamTooSmall 直进人数不足关卡表 min_team_size 下限。刻意与 ErrMatchTeamNotReady
	// (队伍状态没准备好)分开:两者玩家侧的下一步动作完全不同——一个是"再拉人",一个是
	// "让队友点准备",共用一个码客户端就没法给出正确提示。
	ErrMatchTeamTooSmall Code = 4009
	// ErrMatchEntryModeDenied 该关卡不允许请求所选的进法,或关卡表两种都开放而请求未明确选择。
	// 后者是 fail-closed 的刻意选择(§17.3):不替玩家猜入口。
	ErrMatchEntryModeDenied Code = 4010
	// ErrMatchMemberOffline 队伍里有成员此刻不在大厅(已离线 / 登录了但还没进大厅),
	// 不能把他冻进对局票据(INC-20260813-001)。刻意与 ErrMatchTeamNotReady 分开:
	// 「没点准备」队长能催,「已经掉线」只能等他回来或先踢出去,玩家侧动作完全不同。
	// biz error 文本会带离线成员 player_id 供服务端日志排障；当前 StartMatchResponse 只过线
	// code + match_id，客户端拿不到这段文本。若产品要点名，须新增结构化响应字段。
	ErrMatchMemberOffline Code = 4011
)

// ds_allocator / hub_allocator(5000-5999)
const (
	ErrDSNoAvailable       Code = 5001
	ErrDSAllocationFailed  Code = 5002
	ErrDSPodNotFound       Code = 5003
	ErrDSHeartbeatTimeout  Code = 5004
	ErrHubNoAvailable      Code = 5101
	ErrHubTransferFailed   Code = 5102
	ErrHubLineFull         Code = 5103 // 玩家切线:目标线路已满
	ErrHubTransferCooldown Code = 5104 // 玩家切线:冷却中(防刷)
	ErrHubTransferNotInHub Code = 5105 // 玩家切线:不在大厅(战斗/匹配中禁止切线)
)

// battle_result(6000-6999)
const (
	ErrBattleResultDuplicate Code = 6001 // 幂等命中,实际不算错
	ErrBattleResultDecode    Code = 6002
	ErrBattleResultDBWrite   Code = 6003
)

// trade(7000-7999)
const (
	ErrTradeOrderNotFound Code = 7001
	ErrTradeOrderExpired  Code = 7002
	ErrTradeWrongState    Code = 7003
	ErrTradeInsufficient  Code = 7004
	ErrTradeLockFailed    Code = 7005
	// ErrTradeOrderLimit 单玩家参与订单数达上限(写入侧总量上限,不变量 §18)。
	ErrTradeOrderLimit Code = 7006

	// inventory(背包,同属 economy 域,复用 7000 段)
	ErrInventoryItemNotFound  Code = 7010 // 道具实例不存在 / 不属于该玩家
	ErrInventoryInsufficient  Code = 7011 // 道具数量不足(扣减/出售/使用)
	ErrInventoryItemNotUsable Code = 7012 // 该道具不可在大厅使用(战斗内道具走 GAS)
	ErrInventoryNotSellable   Code = 7013 // 该道具不可出售
	ErrInventoryLockFailed    Code = 7014 // 乐观锁重试耗尽(WATCH/MULTI/EXEC 冲突)
	// ErrInventoryIdempotencyConflict 同一 idempotency_key 复用到不同请求(op/item/count/gold 指纹不一致),
	// 拒绝静默当 no-op;相同指纹的重放返回首次执行的结果快照(不变量 §9.7)。
	ErrInventoryIdempotencyConflict Code = 7015
	ErrInventoryCapacityFull        Code = 7016 // 背包格子已满(装备实例数达 capacity 上限)
	ErrInventorySlotOccupied        Code = 7017 // 目标格子已被占用 / 越界(移动实例)
	ErrInventoryInstanceBound       Code = 7018 // 绑定实例不可托管转移(邮件 transfer 扣出拒;bound=不可交易)
	// ErrInventoryNotPurchasable NPC 商店购买被拒:该商店不存在、该道具不在该商店在售集合内,
	// 或本次购买份数超出服务端单次上限。价格永远以商店表为准,客户端上报价格一律不采信。
	ErrInventoryNotPurchasable Code = 7019
	// ErrInventoryCurrencyOverflow 货币入账会越过单币种余额硬上限(MaxCurrencyAmount)。
	// 无符号余额下"加钱溢出"必须显式拒绝:回绕会把首富瞬间变成零元户,且不可观测。
	ErrInventoryCurrencyOverflow Code = 7020
)

// dialogue(8000-8999)
const (
	ErrDialogueNotFound      Code = 8001
	ErrDialogueOptionInvalid Code = 8002
)

// chat / friend / locator(9000-9999)
const (
	ErrChatChannelInvalid Code = 9001
	ErrChatMessageTooLong Code = 9002
	ErrChatMuted          Code = 9003

	ErrFriendNotFound     Code = 9101
	ErrFriendAlreadyAdded Code = 9102
	ErrFriendBlocked      Code = 9103
	ErrFriendLimit        Code = 9104 // 好友数已达上限(AcceptFriend 接受时原子校验)
	ErrFriendRequestLimit Code = 9105 // 收到的好友申请已达上限(target 待处理收件箱满,§9 不变量 18)
	ErrFriendBlockLimit   Code = 9106 // 黑名单已达上限(§9 不变量 18)

	ErrLocatorNotFound Code = 9201
	ErrLocatorConflict Code = 9202 // 玩家同时在两个 DS

	// push 服务(W3 ④,2026-06-05)
	ErrPushOfflineCorrupted Code = 9301 // 离线 ZSET 数据损坏(反序列化失败) / offline.Append 写 redis 失败(W3 ④ 二次修复)
	// ErrPushKafkaConsumerDown 由 W4 push 健康检查 / consumer group rebalance handler 触发上报,W3 ④ 仅占位。
	ErrPushKafkaConsumerDown Code = 9302 // kafka consumer 异常下线

	// guild 公会(9400-9499,2026-06-27)
	ErrGuildNotFound       Code = 9401 // 公会不存在
	ErrGuildAlreadyInGuild Code = 9402 // 玩家已在某公会(单归属)
	ErrGuildFull           Code = 9403 // 公会成员已达上限
	ErrGuildNotLeader      Code = 9404 // 操作需会长权限(解散 / 转让 / 任命)
	ErrGuildNoPermission   Code = 9405 // 无权限(审批 / 踢人需会长或官员)
	ErrGuildNameTaken      Code = 9406 // 公会名已被占用
	ErrGuildRequestInvalid Code = 9407 // 加入申请不存在 / 非 pending
	ErrGuildNotMember      Code = 9408 // 目标不在本公会
	ErrGuildRequestLimit   Code = 9409 // 该公会挂起的加入申请已达上限(§9 不变量 18)

	// group 临时群(9500-9599,2026-06-27)
	ErrGroupNotFound  Code = 9501 // 群不存在
	ErrGroupFull      Code = 9502 // 群成员已达上限
	ErrGroupNotOwner  Code = 9503 // 操作需群主权限(解散 / 转让 / 踢人)
	ErrGroupNotMember Code = 9504 // 玩家不在群内
	ErrGroupAlreadyIn Code = 9505 // 玩家已在群内(拉人幂等命中)
	ErrGroupJoinLimit Code = 9506 // 玩家加入的群数量已达上限(§9 不变量 18)

	// mail 邮件(9600-9699,2026-06-29)
	ErrMailNotFound       Code = 9601 // 邮件不存在 / 已删除
	ErrMailExpired        Code = 9602 // 邮件已过期
	ErrMailNoAttachment   Code = 9603 // 该邮件无附件可领
	ErrMailAlreadyClaimed Code = 9604 // 附件已领取(幂等命中)
	ErrMailBoxFull        Code = 9605 // 收件箱已满(写入侧上限,§9 不变量 18;驱逐已领邮件后仍满)
	// ErrMailAttachmentUnsupported 附件 oneof body 未设置/未识别(滚更共存窗口旧端读到
	// 新增形态,§9.21):发送侧拒收;领取侧整封 fail-closed 保持未领取,禁止静默跳过。
	ErrMailAttachmentUnsupported Code = 9606
	// ErrMailClaimInProgress DS 三段式领取意图已创建(bag phase 2):该邮件只能经
	// bag journal 领取链终结,旧直连 ClaimMail 互斥拒(防 journal + inventory 双发)。
	ErrMailClaimInProgress Code = 9607
)

// data_service(10000-10999)
const (
	ErrDataVersionMismatch Code = 10001
	ErrDataLockTimeout     Code = 10002
	ErrDataMigrate         Code = 10003
)

// mission(11000-11999,任务域;docs/design/mission.md)
const (
	ErrMissionConfigNotFound   Code = 11001 // mission_config_id 不在任务表
	ErrMissionAlreadyAccepted  Code = 11002 // 已在活跃列表(重复接取)
	ErrMissionAlreadyCompleted Code = 11003 // 已完成(重复接取 / 放弃已完成任务)
	ErrMissionTypeConflict     Code = 11004 // 同 (type, sub_type) 已有活跃任务(类型互斥)
	ErrMissionNotAccepted      Code = 11005 // 不在活跃列表(放弃 / 操作目标不存在)
	ErrMissionNotClaimable     Code = 11006 // 无可领奖励(未完成 / 自动发 / 已领取)
	ErrMissionActiveLimit      Code = 11007 // 活跃任务数达上限(不变量 §9-18 写入侧上限)
	ErrMissionFactsConflict    Code = 11008 // 事实上报 idempotency_key 复用到不同内容(指纹不一致)
)

// auction(12000-12999,全服拍卖行 / 撮合)
const (
	ErrAuctionOrderNotFound       Code = 12001
	ErrAuctionWrongState          Code = 12002 // 订单已终态 / 不可撤
	ErrAuctionNotOwner            Code = 12003 // 非挂单本人撤单
	ErrAuctionInsufficient        Code = 12004 // 结算资源不足(冻结 / 扣减失败)
	ErrAuctionIdempotencyConflict Code = 12005 // idempotency_key 复用到不同请求(指纹不一致)
	ErrAuctionMarketBusy          Code = 12006 // market 跨实例单写者锁竞争超时(让客户端稍后重试)
	ErrAuctionOrderLimit          Code = 12007 // 单玩家 PENDING + 活跃拍卖订单达到硬上限
)

// leaderboard(13000-13999,通用排行榜)
const (
	ErrLeaderboardBoardNotFound  Code = 13001 // 榜不存在(查询 / 结算空榜)
	ErrLeaderboardEntryNotFound  Code = 13002 // entity 不在榜上
	ErrLeaderboardInvalidBoard   Code = 13003 // BoardKey 非法(board_type / scope 缺失)
	ErrLeaderboardSettleConflict Code = 13004 // settle_idempotency_key 复用到不同请求
	ErrLeaderboardRewardFailed   Code = 13005 // 结算发奖失败(inventory 不可用 / 扣发异常)
)

// bag(14000-14999,背包域;bag-domain.md 五要件受信写)
const (
	ErrBagEpochFenced         Code = 14001 // owner_epoch 落后于 bag_meta(失租旧写整批拒)
	ErrBagGenerationMismatch  Code = 14002 // 活动段代际不符(切代后迟到写,fail-closed 整批拒)
	ErrBagSeqConflict         Code = 14003 // journal_seq 非法(批内乱序 / 与水位冲突且非纯重放)
	ErrBagCapacityFull        Code = 14004 // 目标段容量不足(入包 / 转移 / 产出)
	ErrBagQuotaExceeded       Code = 14005 // 超出 journal 额度(单批 / 单窗上限,反作弊封顶)
	ErrBagIdempotencyConflict Code = 14006 // idempotency_key 复用到不同内容(指纹不一致)
	ErrBagItemNotFound        Code = 14007 // 扣除 / 转移的物品不存在或数量不足(整批拒)
	ErrBagCheckpointStale     Code = 14008 // checkpoint covered_seq 回退或超出已确认水位
	ErrBagSectionNotAllowed   Code = 14009 // 段类型不合法(查随身段 / checkpoint 含后端驻留段等)
	ErrBagCapacityMaxed       Code = 14010 // 容量扩容档位已购罄 / 达硬上限(§5.3;不可再买,非"袋满")
)

// owner(15000-15999,每玩家 owner 权威;owner-authority.md,§9.22)
const (
	ErrOwnerEpochConflict    Code = 15001 // expect_epoch 不等于当前(带当前记录,重查再决策)
	ErrOwnerBarrierNotOpen   Code = 15002 // now < admit_not_before(带 retry_after,退避重试)
	ErrOwnerIdentityMismatch Code = 15003 // epoch/operation/实例四元组不一致(fail-closed)
	ErrOwnerLeaseRegressed   Code = 15004 // 续租实例身份不符或 deadline 回退
	ErrOwnerInvalidOperation Code = 15005 // operation_id 非法或目标身份不完整
	// ErrOwnerSourceRevisionStale 本次 Begin 携带的 Hub 来源版本不新于该玩家的高水位
	// (INC-20260818-003)。含三种情形:版本更旧、同版本却换了 target、以及见过非零版本
	// 之后又来了 legacy(0)。一律 fail-closed 拒绝,**不带**当前记录重试建议 ——
	// 调用方该做的是重查自己的 assignment,而不是拿更大的 epoch 再冲一次。
	ErrOwnerSourceRevisionStale Code = 15006
)

// Error 是带错误码的标准错误类型。
type Error struct {
	Code Code
	Msg  string
	// cause 是可选的底层原因(如 *mysql.MySQLError)。仅用于让上层 errors.As / errors.Is
	// 沿链检出底层错误(例如数据层判定 1213 死锁重试),不进入对客户端的错误码语义。
	cause error
}

func (e *Error) Error() string {
	return fmt.Sprintf("errcode=%d %s", e.Code, e.Msg)
}

// Unwrap 暴露底层原因,供 errors.As / errors.Is 沿链遍历(cause 为 nil 时返回 nil,
// 行为与未包裹时一致,向后兼容)。
func (e *Error) Unwrap() error { return e.cause }

// New 构造一个错误。msg 可以是 fmt.Sprintf 风格。
func New(code Code, msg string, args ...any) *Error {
	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}
	return &Error{Code: code, Msg: msg}
}

// NewCause 构造带底层原因的错误:业务错误码 + 人读消息不变,同时保留 cause 供
// errors.As / errors.Is 沿链检出(如数据层判定 MySQL 1213 死锁是否可重试)。
// cause 不改变 As(err) 返回的错误码语义,只对客户端暴露 code/msg。
func NewCause(code Code, cause error, msg string, args ...any) *Error {
	e := New(code, msg, args...)
	e.cause = cause
	return e
}

// As 从 error 中提取 Code,非 *Error 返回 ErrUnknown。先直接断言(最常见,零开销),
// 再沿 Unwrap 链回溯(兼容被 fmt.Errorf %w / 其它错误包裹的场景)。
func As(err error) Code {
	if err == nil {
		return OK
	}
	if e, ok := err.(*Error); ok {
		return e.Code
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ErrUnknown
}

// IsRetryable 判断该错误是否值得 client 重试。
//
// 可重试:网络抖动 / 临时不可用 / 限流 / DS 还没准备好
// 不可重试:参数错 / 权限错 / 业务状态机错
func IsRetryable(code Code) bool {
	switch code {
	case ErrTimeout, ErrUnavailable, ErrRateLimited, ErrServiceDisabled,
		ErrDSAllocationFailed, ErrHubTransferFailed, ErrAuctionMarketBusy:
		return true
	default:
		return false
	}
}

// IsServerFault 判断该错误码是否属于服务端内部 / 基础设施故障(而非预期业务拒绝)。
//
// 用途:各 service handler 普遍以 in-band 方式返回错误(`&Resp{Code: toProtoCode(err)}, nil`,
// transport error 恒为 nil),access-log 中间件只按 transport error 判 rpc_failed,于是
// MySQL / Redis / etcd 故障、CAS/事务失败、依赖超时等都被记成 rpc_ok(DEBUG),线上 info 级静默。
// 中间件用本函数把 in-band 的服务端故障码识别出来并升 ERROR,恢复基础设施故障可见性。
//
// 归为服务端故障:ErrUnknown(非 *Error 的裸错误逃逸,属未分类缺陷)、ErrInternal、
// ErrTimeout(依赖超时)、ErrUnavailable(依赖不可用)。其余(NotFound / InvalidArg /
// PermissionDeny / 各业务状态机与 fencing 拒绝码)是预期业务结果,不算故障。
func IsServerFault(code Code) bool {
	switch code {
	case ErrUnknown, ErrInternal, ErrTimeout, ErrUnavailable:
		return true
	default:
		return false
	}
}
