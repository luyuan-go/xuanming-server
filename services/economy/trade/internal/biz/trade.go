// Package biz 是 trade 服务的业务逻辑层(2026-06-16)。
//
// 职责(docs/design/go-services.md §2.9):
//   - 玩家间交易两阶段确认状态机
//   - 订单存 Redis(data.TradeRepo,WATCH/MULTI/EXEC 乐观锁)
//   - 结算走 ResourceLedger 原子扣减 + 幂等键 = order_id(不变量 §9.7)
//   - 每次状态流转把订单快照发 kafka pandora.trade.audit(弱依赖,审计)
//
// 状态机(OrderState,INC-20260722-001 修订:结算意图先落库):
//
//	PENDING ──买方确认──▶ BUYER_CONFIRMED ──卖方确认──▶ SELLER_CONFIRMED ──结算成功──▶ COMPLETED
//	   │                       │                            │
//	   │                       │                            └──结算余额/物品不足──▶ FAILED
//	   └──任一方 Cancel────────┴──▶ CANCELED    (SELLER_CONFIRMED 拒 Cancel/过期:结算围栏)
//	   └──超时(惰性)──────────────▶ EXPIRED
//
// 关键规则:
//   - 卖方挂单(CreateOrder),买方先确认,卖方后确认触发结算(双确认防单方面成交)
//   - **结算意图先原子落库**(INC-20260722-001):Settle 是不可回滚的跨服务资产转移,
//     绝不能放在"可能不提交、也可能重跑"的 Redis WATCH 回调里。卖方确认先把
//     SELLER_CONFIRMED 经 WATCH/EXEC 原子提交(= 本订单进入结算通道的线性化点),
//     提交成功后才调用 Settle(幂等键 order_id);成功再 CAS → COMPLETED。
//   - SELLER_CONFIRMED 是结算围栏:Cancel / 惰性过期 / 配额清理一律 fail-closed,
//     订单只向 COMPLETED / FAILED 收敛(资产账本为权威)。
//   - 恢复路径:结算窗口内进程退出 / Settle 瞬时失败 / 终态写失败 → 订单停留
//     SELLER_CONFIRMED,任一方重试 Confirm 幂等重新驱动结算(Settle 幂等命中,
//     终态 CAS 幂等),无需回滚。
//   - 任一方可在 PENDING / BUYER_CONFIRMED 下 Cancel
//   - 过期惰性判定:访问订单时若已过 expires_at_ms 且非终态且非 SELLER_CONFIRMED → 置 EXPIRED
//   - player_id 一律以 JWT ctx 为准(R5),service 层注入
package biz

import (
	"context"
	"sort"
	"time"

	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	tradev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/trade/v1"

	"github.com/luyuancpp/pandora/services/economy/trade/internal/conf"
	"github.com/luyuancpp/pandora/services/economy/trade/internal/data"
)

// ResourceLedger 抽象「原子扣减交易双方资源 + 幂等」的账本操作(不变量 §9.7)。
//
// Settle 在卖方确认、订单进入 COMPLETED 前调用:把卖方物品转给买方、买方货币转给卖方,
// idempotencyKey = order_id 保证同一订单重复结算只生效一次。
// 返回 ErrTradeInsufficient 表示余额 / 物品不足,biz 将订单置 FAILED。
//
// W1 暂用 NoopResourceLedger 占位(总是成功);真实账本接 player / 背包服务后替换。
type ResourceLedger interface {
	Settle(ctx context.Context, order *tradev1.Order, idempotencyKey uint64) error
}

// NoopResourceLedger 是占位实现:总是结算成功(不真实扣转背包 / 货币)。
// 仅供联调 / 单测;生产由 main.go 强制 fail-fast(除非显式 allow_noop_ledger=true),
// 防止漏接真实账本后仍以「成交不扣减」静默上线。真实资源扣减接 inventory P2P 原子对转后替换。
type NoopResourceLedger struct{}

// Settle 永远成功(占位)。
func (NoopResourceLedger) Settle(_ context.Context, _ *tradev1.Order, _ uint64) error { return nil }

// TradeAuditPusher 把订单流转快照发 kafka pandora.trade.audit(main.go 注入;弱依赖,nil 静默)。
type TradeAuditPusher interface {
	PushAudit(ctx context.Context, order *tradev1.Order) error
}

// snowflakeGen 是 snowflake.Node 的最小接口。
type snowflakeGen interface {
	Generate() uint64
}

// TradeUsecase 是 trade 服务业务逻辑核心。
type TradeUsecase struct {
	repo   data.TradeRepo
	ledger ResourceLedger
	audit  TradeAuditPusher // 弱依赖,可为 nil
	sf     snowflakeGen
	cfg    conf.TradeConf

	// router 是确定性 region/cell 路由器(scale-cellular-20m.md §4.2)。
	// 可为 nil:单 Cell / dev / 阶段 1~2 不分片,结算跨分片落点观测退化为不打日志(行为不变)。
	// 分片部署时由 main 经 SetCellRouter 注入,ConfirmOrder 结算成功后额外打一条结算跨分片
	// 落点观测(买卖双方跨 region → 走最小跨 region 通道)。nil-safe。
	router *cellroute.Router

	// rateQuota 下单/撤单频率配额(anti-abuse §6 第 6 项)。可为 nil(不限)。
	rateQuota ActionRateQuota
}

// ActionRateQuota 下单/撤单的 per-player 频率配额(实现 pkg/redisx.ActionQuota)。
// 总量闸(max_orders_per_player 200)只限「同时挂多少」,挡不住「下单-撤单-再下单」的
// D 类循环(每轮产生托管写 + 流水行),频率配额补这一维。
// 背压非权威门:判定 error 由调用方 fail-open 放行。
type ActionRateQuota interface {
	Allow(ctx context.Context, action string, subject uint64) (bool, error)
}

// SetRateQuota 注入频率配额(可选;不注入 = 不限,dev 联调兼容)。
func (u *TradeUsecase) SetRateQuota(q ActionRateQuota) {
	u.rateQuota = q
}

// allowAction 频率配额门:窗内超额返回 ErrRateLimited(先于一切副作用);fail-open。
func (u *TradeUsecase) allowAction(ctx context.Context, action string, playerID uint64) error {
	if u.rateQuota == nil {
		return nil
	}
	ok, err := u.rateQuota.Allow(ctx, action, playerID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "trade_rate_quota_check_failed",
			"action", action, "player_id", playerID, "err", err)
		return nil
	}
	if !ok {
		plog.With(ctx).Warnw("msg", "trade_rate_quota_rejected",
			"action", action, "player_id", playerID)
		return errcode.New(errcode.ErrRateLimited, "trade %s rate limited, retry later", action)
	}
	return nil
}

// NewTradeUsecase 构造。ledger 为 nil 时退化为 NoopResourceLedger;audit 允许 nil。
func NewTradeUsecase(repo data.TradeRepo, ledger ResourceLedger, audit TradeAuditPusher, sf snowflakeGen, cfg conf.TradeConf) *TradeUsecase {
	if ledger == nil {
		ledger = NoopResourceLedger{}
	}
	if cfg.OptimisticRetry <= 0 {
		cfg.OptimisticRetry = 3
	}
	if cfg.MaxItemsPerOrder <= 0 {
		cfg.MaxItemsPerOrder = 20
	}
	if cfg.MaxOrdersPerPlayer <= 0 {
		cfg.MaxOrdersPerPlayer = 200
	}
	return &TradeUsecase{repo: repo, ledger: ledger, audit: audit, sf: sf, cfg: cfg}
}

// SetCellRouter 注入确定性 region/cell 路由器(scale-cellular-20m.md §4.2 两级架构)。
//
// nil-safe:不调用 / 传 nil 时(单 Cell / dev / 阶段 1~2),ConfirmOrder 不做结算跨分片落点观测,
// 行为与历史一致。用 setter 而非构造参数,避免单 Cell 阶段调用点被迫改签名(与 matchmaker /
// auction / battle_result / friend / chat 一致)。Router 内部读路径无锁,并发安全。
func (u *TradeUsecase) SetCellRouter(r *cellroute.Router) {
	u.router = r
}

// CreateOrder 卖方挂单。sellerID 由 service 从 JWT ctx 得到(R5)。
//
// items = 卖家交付给买家的道具(必填);buyerItems = 买家交付给卖家的道具(可空 = 纯金币购买);
// price = 买家付卖家的金币(>=0)。道具一律用 item_config_id(uint32,对齐 inventory 可堆叠模型)。
func (u *TradeUsecase) CreateOrder(ctx context.Context, sellerID, buyerID uint64, items, buyerItems []*tradev1.TradeItem, price uint64) (uint64, error) {
	if sellerID == 0 || buyerID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "seller / buyer required")
	}
	if sellerID == buyerID {
		return 0, errcode.New(errcode.ErrInvalidArg, "cannot trade with self")
	}
	// 频率配额(anti-abuse §6 第 6 项):按发起方(卖方)计,先于一切副作用。
	if err := u.allowAction(ctx, "order", sellerID); err != nil {
		return 0, err
	}
	if len(items) == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "items required")
	}
	if len(items)+len(buyerItems) > u.cfg.MaxItemsPerOrder {
		return 0, errcode.New(errcode.ErrInvalidArg, "too many items: %d > %d", len(items)+len(buyerItems), u.cfg.MaxItemsPerOrder)
	}
	validate := func(its []*tradev1.TradeItem) error {
		for _, it := range its {
			if it.GetItemConfigId() == 0 || it.GetCount() <= 0 {
				return errcode.New(errcode.ErrInvalidArg, "invalid item: config_id=%d count=%d", it.GetItemConfigId(), it.GetCount())
			}
		}
		return nil
	}
	if err := validate(items); err != nil {
		return 0, err
	}
	if err := validate(buyerItems); err != nil {
		return 0, err
	}
	// price 现在是 uint64,原来的 `price < 0` 闸会**恒为 false**(编译器与 go vet 都不报),
	// 等于闸门被静默拆掉。无符号世界里"防离谱价格"的正确形态是**上界**而不是下界:
	// 老客户端 / 被篡改的请求发 price=-1,在 uint64 里解成 1.8e19,没有上界就会一路落库
	// 再送进 SettlePlayerTrade。MaxTradePrice 与拍卖 MaxPrice 同量级。
	if price > u.cfg.MaxTradePrice {
		return 0, errcode.New(errcode.ErrInvalidArg,
			"price out of range: %d (max %d)", price, u.cfg.MaxTradePrice)
	}

	now := nowMs()
	order := &tradev1.Order{
		OrderId:     u.sf.Generate(),
		SellerId:    sellerID,
		BuyerId:     buyerID,
		Items:       items,
		BuyerItems:  buyerItems,
		Price:       price,
		State:       tradev1.OrderState_ORDER_STATE_PENDING,
		CreatedAtMs: now,
		ExpiresAtMs: now + int64(u.cfg.OrderExpire.Std()/time.Millisecond),
	}
	// 写序铁律(镜像 team/matchmaker):① 先写订单主体(orderID 新发、无人引用)→
	// ② 原子预留双方反查索引配额(不变量 §18 写入侧总量上限)。主体先落地保证
	// 「索引成员无主体 ≡ 真死成员」,配额清理绝不误删 in-flight 预留。
	// 任一步失败 → 回滚已预留名额 + 删主体,无残留。
	if err := u.repo.CreateOrder(ctx, order, u.cfg.OrderTTL.Std()); err != nil {
		return 0, err
	}
	if err := u.reserveSlotPruning(ctx, sellerID, order.GetOrderId()); err != nil {
		u.rollbackCreate(ctx, order.GetOrderId())
		return 0, err
	}
	if err := u.reserveSlotPruning(ctx, buyerID, order.GetOrderId()); err != nil {
		if rerr := u.repo.ReleaseOrderSlot(ctx, sellerID, order.GetOrderId()); rerr != nil {
			plog.With(ctx).Warnw("msg", "trade_release_slot_failed", "player_id", sellerID, "order_id", order.GetOrderId(), "err", rerr)
		}
		u.rollbackCreate(ctx, order.GetOrderId())
		return 0, err
	}
	u.pushAudit(ctx, order)
	return order.GetOrderId(), nil
}

// rollbackCreate 删掉刚写入、尚未对外返回的订单主体(创建失败回滚)。
// 失败仅 Warn:残留主体无索引指向、TTL 到期自收,无业务影响。
func (u *TradeUsecase) rollbackCreate(ctx context.Context, orderID uint64) {
	if derr := u.repo.DeleteOrder(ctx, orderID); derr != nil {
		plog.With(ctx).Warnw("msg", "trade_rollback_order_failed", "order_id", orderID, "err", derr)
	}
}

// reserveSlotPruning 为 playerID 预留一个反查索引名额;首次满员时先清理死成员
// (订单主体已被 Redis 回收 / 已进终态)再重试一次,仍满返 ErrTradeOrderLimit。
// 死成员来源:终态/过期订单不实时移除索引(保留历史供 ListMyOrders 回看),
// 配额满时才惰性回收 —— 正常玩家永远碰不到,遭恶意刷单的受害者取消后也能自愈。
func (u *TradeUsecase) reserveSlotPruning(ctx context.Context, playerID, orderID uint64) error {
	for attempt := 0; attempt < 2; attempt++ {
		ok, err := u.repo.ReserveOrderSlot(ctx, playerID, orderID, u.cfg.MaxOrdersPerPlayer, u.cfg.OrderTTL.Std())
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if attempt == 0 {
			pruned, perr := u.pruneDeadOrderSlots(ctx, playerID)
			if perr != nil {
				return perr
			}
			if pruned > 0 {
				continue // 清出了名额,重试一次
			}
		}
		break
	}
	return errcode.New(errcode.ErrTradeOrderLimit,
		"player %d has too many orders (max %d)", playerID, u.cfg.MaxOrdersPerPlayer)
}

// pruneDeadOrderSlots 扫描玩家反查索引,移除「主体已被 Redis 回收」或「已进终态」的
// 死成员,返回清理数。仅在配额满时调用,遍历规模被 max 硬上限兕定(默认 200)。
// 安全性:CreateOrder 先写主体后预留,in-flight 预留必有存活主体(PENDING),不会被误判。
func (u *TradeUsecase) pruneDeadOrderSlots(ctx context.Context, playerID uint64) (int, error) {
	ids, err := u.repo.ListPlayerOrderIDs(ctx, playerID)
	if err != nil {
		return 0, err
	}
	pruned := 0
	for _, id := range ids {
		o, ok, gerr := u.repo.GetOrder(ctx, id)
		if gerr != nil {
			// fail-closed 正确,但 redis 抖动时清理会静默退化为 no-op:玩家可能持续撞
			// ERR_TRADE_ORDER_LIMIT 而无任何线索指向「清理被读失败挡住」→ DEBUG 留证。
			plog.With(ctx).Debugw("msg", "trade_prune_order_read_failed",
				"player_id", playerID, "order_id", id, "err", gerr)
			continue // 单条读失败不阻断扫描(fail-closed:查不到状态不清理)
		}
		if ok && o.GetState() == tradev1.OrderState_ORDER_STATE_SELLER_CONFIRMED {
			continue // 结算中(意图已落库,不过期):真占用,绝不回收(INC-20260722-001)
		}
		if ok && !isTerminal(o.GetState()) && !(o.GetExpiresAtMs() > 0 && nowMs() >= o.GetExpiresAtMs()) {
			continue // 存活非终态且未过期 → 真占用
		}
		if rerr := u.repo.ReleaseOrderSlot(ctx, playerID, id); rerr != nil {
			plog.With(ctx).Warnw("msg", "trade_prune_slot_failed", "player_id", playerID, "order_id", id, "err", rerr)
			continue
		}
		pruned++
	}
	if pruned > 0 {
		plog.With(ctx).Infow("msg", "trade_pruned_dead_order_slots", "player_id", playerID, "pruned", pruned)
	}
	return pruned, nil
}

// ConfirmOrder 买方 / 卖方确认。两阶段确认 + 两步结算(INC-20260722-001):
//   - 买方 + PENDING → BUYER_CONFIRMED
//   - 卖方 + BUYER_CONFIRMED → **先原子提交 SELLER_CONFIRMED(结算意图)** → 结算 →
//     COMPLETED(余额/物品不足 → FAILED;瞬时失败 → 停留 SELLER_CONFIRMED 可重试)
//   - 任一方 + SELLER_CONFIRMED → 幂等重新驱动结算(恢复路径:结算窗口崩溃 / 瞬时
//     失败 / 终态写失败后由重试收敛,Settle 幂等键 = order_id)
//
// 返回最新状态。playerID 由 service 从 JWT ctx 得到(R5)。
func (u *TradeUsecase) ConfirmOrder(ctx context.Context, playerID, orderID uint64) (tradev1.OrderState, error) {
	if playerID == 0 || orderID == 0 {
		return tradev1.OrderState_ORDER_STATE_UNSPECIFIED, errcode.New(errcode.ErrInvalidArg, "player / order required")
	}

	var driveSettle *tradev1.Order // 非 nil = SELLER_CONFIRMED 已落库,锁外驱动结算
	var expired bool               // 惰性过期:置 EXPIRED 并持久化,事务后返回 ErrTradeOrderExpired
	err := u.repo.UpdateWithLock(ctx, orderID, u.cfg.OptimisticRetry, func(o *tradev1.Order) error {
		driveSettle = nil // WATCH 重试会重跑回调:先清残留,只信最后一次提交成功的快照
		if expireIfStale(o) {
			// 访问订单时惰性置 EXPIRED:返回 nil 让 UpdateWithLock 把 EXPIRED 写回 Redis
			// (此前返回 error → fn 报错不写回,订单状态停留在旧值,过期态永远落不了库)。
			expired = true
			return nil
		}
		if playerID != o.GetSellerId() && playerID != o.GetBuyerId() {
			return errcode.New(errcode.ErrUnauthorized, "player %d not party of order %d", playerID, orderID)
		}

		switch {
		case playerID == o.GetBuyerId() && o.GetState() == tradev1.OrderState_ORDER_STATE_PENDING:
			o.State = tradev1.OrderState_ORDER_STATE_BUYER_CONFIRMED
			return nil
		case playerID == o.GetSellerId() && o.GetState() == tradev1.OrderState_ORDER_STATE_BUYER_CONFIRMED:
			// 结算意图先原子落库(INC-20260722-001 根因:Settle 曾在 WATCH 回调内先执行,
			// EXEC 冲突 / 崩溃时资产已转移而订单可被并发 Cancel 成 CANCELED,永久撕裂)。
			// 本回调只改状态不做任何外部副作用;EXEC 成功 = 结算通道已 fencing
			// (Cancel/过期从此拒),之后才在锁外调用 Settle。
			o.State = tradev1.OrderState_ORDER_STATE_SELLER_CONFIRMED
			driveSettle = o
			return nil
		case o.GetState() == tradev1.OrderState_ORDER_STATE_SELLER_CONFIRMED:
			// 恢复驱动:意图已落库(买卖任一方重试都可推进;Settle/终态 CAS 均幂等)。
			driveSettle = o
			return nil
		default:
			return errcode.New(errcode.ErrTradeWrongState,
				"player %d cannot confirm order %d in state %s", playerID, orderID, o.GetState())
		}
	}, u.cfg.OrderTTL.Std())

	// 惰性过期:EXPIRED 已在锁内写回 Redis(err==nil),读回做 audit 并返回过期错误。
	if expired && err == nil {
		if o, ok, gerr := u.repo.GetOrder(ctx, orderID); gerr == nil && ok {
			u.pushAudit(ctx, o)
		}
		return tradev1.OrderState_ORDER_STATE_EXPIRED,
			errcode.New(errcode.ErrTradeOrderExpired, "order %d expired", orderID)
	}
	if err != nil {
		return tradev1.OrderState_ORDER_STATE_UNSPECIFIED, err
	}

	if driveSettle != nil {
		return u.driveSettlement(ctx, driveSettle)
	}

	// 买方确认路径:读回最新状态做 audit + 返回。
	o, ok, gerr := u.repo.GetOrder(ctx, orderID)
	if gerr != nil || !ok {
		// 写成功但读回失败:返回我们已知的推进结果。
		return tradev1.OrderState_ORDER_STATE_BUYER_CONFIRMED, nil
	}
	u.pushAudit(ctx, o)
	return o.GetState(), nil
}

// driveSettlement 驱动一个已落库 SELLER_CONFIRMED 意图的订单走完结算(幂等,可重入):
//
//	Settle 成功            → CAS SELLER_CONFIRMED → COMPLETED(资产账本为权威:即使状态
//	                         被某个 bug 改动,也按账本结果收敛到 COMPLETED 并告警)
//	Settle 余额/物品不足    → 资产未动(inventory 原子拒),CAS → FAILED 终态
//	Settle 瞬时失败/UNKNOWN → 订单停留 SELLER_CONFIRMED(可能已入账!绝不回滚、绝不置
//	                         FAILED),返回可重试错误,任一方重试 Confirm 继续驱动
//	终态 CAS 写失败         → 同上停留 SELLER_CONFIRMED,重试收敛;Error 告警
func (u *TradeUsecase) driveSettlement(ctx context.Context, order *tradev1.Order) (tradev1.OrderState, error) {
	orderID := order.GetOrderId()
	if serr := u.ledger.Settle(ctx, order, orderID); serr != nil {
		if errcode.As(serr) == errcode.ErrTradeInsufficient {
			// 结算原子失败(资产未动):SELLER_CONFIRMED → FAILED 终态并 audit。
			if ferr := u.repo.UpdateWithLock(ctx, orderID, u.cfg.OptimisticRetry, func(o *tradev1.Order) error {
				if o.GetState() == tradev1.OrderState_ORDER_STATE_SELLER_CONFIRMED {
					o.State = tradev1.OrderState_ORDER_STATE_FAILED
				}
				return nil
			}, u.cfg.OrderTTL.Std()); ferr != nil {
				// 置 FAILED 失败(乐观锁耗尽/redis 抖动):订单暂留 SELLER_CONFIRMED,
				// 重试 Confirm 会重走结算→再次到达这里,可自愈;必须留 Error 便于告警。
				plog.With(ctx).Errorw("msg", "trade_mark_failed_state_failed", "order_id", orderID, "err", ferr)
			}
			if o, ok, gerr := u.repo.GetOrder(ctx, orderID); gerr == nil && ok {
				u.pushAudit(ctx, o)
			}
			return tradev1.OrderState_ORDER_STATE_FAILED, serr
		}
		// 瞬时/UNKNOWN(超时 / inventory 不可达 / 回包丢失):结算可能已生效,绝不回滚
		// 订单;意图态留在库里,由重试幂等收敛(Settle 幂等键命中即成功)。
		plog.With(ctx).Warnw("msg", "trade_settlement_inflight_retryable", "order_id", orderID, "err", serr)
		return tradev1.OrderState_ORDER_STATE_SELLER_CONFIRMED,
			errcode.New(errcode.ErrUnavailable, "order %d settlement in flight, retry confirm: %v", orderID, serr)
	}

	// 结算已成功:资产账本为权威,状态收敛到 COMPLETED。
	var completed *tradev1.Order
	if cerr := u.repo.UpdateWithLock(ctx, orderID, u.cfg.OptimisticRetry, func(o *tradev1.Order) error {
		if o.GetState() != tradev1.OrderState_ORDER_STATE_COMPLETED {
			if o.GetState() != tradev1.OrderState_ORDER_STATE_SELLER_CONFIRMED {
				// 不该发生(SELLER_CONFIRMED 已 fencing Cancel/过期):按账本权威强制收敛并告警。
				plog.With(ctx).Errorw("msg", "trade_settled_state_diverged_converging",
					"order_id", orderID, "state", o.GetState().String())
			}
			o.State = tradev1.OrderState_ORDER_STATE_COMPLETED
		}
		completed = o
		return nil
	}, u.cfg.OrderTTL.Std()); cerr != nil {
		// 资产已结算而终态未落库:停留 SELLER_CONFIRMED,重试 Confirm 收敛
		// (Settle 幂等命中 → 再次到这里重试 CAS)。Error 告警便于监控收敛积压。
		plog.With(ctx).Errorw("msg", "trade_mark_completed_failed", "order_id", orderID, "err", cerr)
		return tradev1.OrderState_ORDER_STATE_SELLER_CONFIRMED,
			errcode.New(errcode.ErrUnavailable, "order %d settled, state convergence pending, retry confirm", orderID)
	}
	u.pushAudit(ctx, completed)
	// 分片:结算成功时观测本笔结算的跨分片落点(买卖双方跨 Cell → 跨分片结算;
	// 跨 region → 走最小跨 region 通道)。router 为 nil(单 Cell)→ 不打。
	u.logSettlementRouting(ctx, orderID, completed.GetBuyerId(), completed.GetSellerId())
	return tradev1.OrderState_ORDER_STATE_COMPLETED, nil
}

// CancelOrder 任一方在 PENDING / BUYER_CONFIRMED 下取消订单。
// SELLER_CONFIRMED 一律拒(INC-20260722-001 结算围栏):结算意图已落库,资产转移可能
// 已发生或即将发生,取消会与账本撕裂;订单只向 COMPLETED / FAILED 收敛。
// playerID 由 service 从 JWT ctx 得到(R5)。
func (u *TradeUsecase) CancelOrder(ctx context.Context, playerID, orderID uint64) error {
	if playerID == 0 || orderID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player / order required")
	}
	// 频率配额(anti-abuse §6 第 6 项):下撤循环两端都限,单限下单会把刷子逼到撤单侧。
	if err := u.allowAction(ctx, "cancel", playerID); err != nil {
		return err
	}
	err := u.repo.UpdateWithLock(ctx, orderID, u.cfg.OptimisticRetry, func(o *tradev1.Order) error {
		if playerID != o.GetSellerId() && playerID != o.GetBuyerId() {
			return errcode.New(errcode.ErrUnauthorized, "player %d not party of order %d", playerID, orderID)
		}
		if o.GetState() == tradev1.OrderState_ORDER_STATE_SELLER_CONFIRMED {
			return errcode.New(errcode.ErrTradeWrongState,
				"order %d is settling (seller confirmed), cannot cancel", orderID)
		}
		if isTerminal(o.GetState()) {
			return errcode.New(errcode.ErrTradeWrongState, "order %d already terminal: %s", orderID, o.GetState())
		}
		o.State = tradev1.OrderState_ORDER_STATE_CANCELED
		return nil
	}, u.cfg.OrderTTL.Std())
	if err != nil {
		return err
	}
	if o, ok, gerr := u.repo.GetOrder(ctx, orderID); gerr == nil && ok {
		u.pushAudit(ctx, o)
	}
	return nil
}

// ListMyOrders 列玩家参与的订单(客户端可见结构 Order),按 order_id 降序游标分页。
// activeOnly=true 时只返回非终态订单。playerID 由 service 从 JWT ctx 得到(R5)。
// nextCursor 为本页末 order_id;0=无更多。
func (u *TradeUsecase) ListMyOrders(ctx context.Context, playerID uint64, activeOnly bool, cursor uint64, limit int) ([]*tradev1.Order, uint64, error) {
	if playerID == 0 {
		return nil, 0, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	limit = clampLimit(limit)
	ids, err := u.repo.ListPlayerOrderIDs(ctx, playerID)
	if err != nil {
		return nil, 0, err
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] }) // order_id 降序
	out := make([]*tradev1.Order, 0, limit)
	var next uint64
	for _, id := range ids {
		if cursor != 0 && id >= cursor {
			continue // 上页已返回
		}
		if len(out) == limit {
			next = out[len(out)-1].GetOrderId() // 还有更多
			break
		}
		o, ok, gerr := u.repo.GetOrder(ctx, id)
		if gerr != nil || !ok {
			continue // 订单已过期被 Redis 回收 → 跳过
		}
		// 惰性过期:把已超时的非终态订单置 EXPIRED(尽力,不阻断列表)。
		// 回调内重判必须同样排除 SELLER_CONFIRMED(读-写间隙订单可能刚进入结算围栏)。
		if expireIfStale(o) {
			_ = u.repo.UpdateWithLock(ctx, id, u.cfg.OptimisticRetry, func(x *tradev1.Order) error {
				if x.GetState() != tradev1.OrderState_ORDER_STATE_SELLER_CONFIRMED &&
					!isTerminal(x.GetState()) && x.GetExpiresAtMs() > 0 && nowMs() >= x.GetExpiresAtMs() {
					x.State = tradev1.OrderState_ORDER_STATE_EXPIRED
				}
				return nil
			}, u.cfg.OrderTTL.Std())
		}
		if activeOnly && isTerminal(o.GetState()) {
			continue
		}
		out = append(out, o)
	}
	return out, next, nil
}

// 分页上限(决策:docs/design/decision-revisit-list-pagination.md)。
const (
	defaultPageLimit = 50
	maxPageLimit     = 100
)

func clampLimit(limit int) int {
	if limit <= 0 {
		return defaultPageLimit
	}
	if limit > maxPageLimit {
		return maxPageLimit
	}
	return limit
}

// ── 辅助 ──────────────────────────────────────────────────────────────────────

// expireIfStale 若订单已过 expires_at_ms 且非终态且非 SELLER_CONFIRMED,就地把状态改为
// EXPIRED 并返回 true。SELLER_CONFIRMED 不过期(INC-20260722-001 结算围栏):结算意图已
// 落库、资产可能已转移,置 EXPIRED 会与账本撕裂,只允许向 COMPLETED / FAILED 收敛。
func expireIfStale(o *tradev1.Order) bool {
	if o.GetState() == tradev1.OrderState_ORDER_STATE_SELLER_CONFIRMED || isTerminal(o.GetState()) {
		return false
	}
	if o.GetExpiresAtMs() > 0 && nowMs() >= o.GetExpiresAtMs() {
		o.State = tradev1.OrderState_ORDER_STATE_EXPIRED
		return true
	}
	return false
}

// isTerminal 判断订单是否已到终态(不可再流转)。
func isTerminal(s tradev1.OrderState) bool {
	switch s {
	case tradev1.OrderState_ORDER_STATE_COMPLETED,
		tradev1.OrderState_ORDER_STATE_FAILED,
		tradev1.OrderState_ORDER_STATE_EXPIRED,
		tradev1.OrderState_ORDER_STATE_CANCELED:
		return true
	default:
		return false
	}
}

// pushAudit 弱依赖审计推送:audit 为 nil 或失败只 warn,不影响主流程。
func (u *TradeUsecase) pushAudit(ctx context.Context, order *tradev1.Order) {
	if u.audit == nil {
		return
	}
	if err := u.audit.PushAudit(ctx, order); err != nil {
		plog.With(ctx).Warnw("msg", "trade_audit_push_failed",
			"order_id", order.GetOrderId(), "state", order.GetState().String(), "err", err)
	}
}

// nowMs 返回当前毫秒时间戳。
func nowMs() int64 {
	return time.Now().UnixMilli()
}
