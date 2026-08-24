// Package biz 是 auction 服务的业务逻辑层(全服拍卖行 / 撮合引擎,2026-06-19)。
//
// 职责(docs/design/decision-revisit-auction-engine.md):
//   - 挂单(SELL)/ 出价(BUY)按 market_id 分片，MySQL 权威保存;
//   - 「每个 market 单写者」从 MySQL 精确 item 候选串行撮合,价格-时间优先(被动挂单价成交);
//   - 两层幂等:① 挂单 idempotency_key(uk owner+key,重试不重复挂单);
//     ② 结算 match_id(uk,资产只转一次,不变量 §9.2 / §9.7);
//   - 成交发 kafka pandora.auction.match,订单流转发 pandora.auction.audit(弱依赖)。
//
// 单写者实现:进程内 per-market 互斥锁(striped lock)。同一 market 的挂单 / 出价 / 撤单
// 全程持锁串行,权威订单不会并发改 → 不会超卖。跨实例的「每 market 单写者」需配一致性
// 哈希路由(每个 market 固定落一个实例),属扩容步骤,后续接入;W1 单实例进程内串行即可。
//
// owner_id / buyer_id 一律以 JWT ctx 为准(R5),service 层注入。
package biz

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	auctionv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/auction/v1"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/services/economy/auction/internal/conf"
	"github.com/luyuancpp/pandora/services/economy/auction/internal/data"
)

// SettlementLedger 抽象「成交资产原子转移 + 幂等」(不变量 §9.7)。
//
// 三段式 escrow(挂单冻结 / 成交从 escrow 消费 / 撤单过期退还),消除「成交瞬间余额不足而失败」:
//   - Freeze 在挂单 / 出价时把资产冻进 escrow(卖单冻道具 / 买单冻金币),幂等键 = order_id;
//   - Settle 在每笔撮合成交时从双方 escrow 消费完成对转,幂等键 = match_id(资产只转一次);
//   - Release 在撤单 / 过期 / 完全成交后退还 escrow 残余(含买单成交价优于出价的价差),幂等键 = order_id。
//
// Freeze 返回 ErrAuctionInsufficient 表示挂单方资产不足(挂单即失败,不进簿)。
// W1 可用 NoopSettlementLedger 占位;真实账本接 inventory FreezeForOrder / SettleAuctionMatch / ReleaseEscrow。
type SettlementLedger interface {
	// Freeze 挂单冻结:side=SELL 冻 quantity 道具,side=BUY 冻 quantity*price 金币。资产不足 → ErrAuctionInsufficient。
	Freeze(ctx context.Context, playerID, orderID uint64, side data.Side, itemConfigID uint32, quantity, price int64) error
	// Ensure 验证或补冻旧版本活跃单的剩余 escrow；仅后台 legacy 恢复调用。
	Ensure(ctx context.Context, playerID, orderID uint64, side data.Side, itemConfigID uint32, remaining, price int64) error
	// Settle 成交从双方 escrow 消费对转。幂等键 = m.MatchID。
	Settle(ctx context.Context, m *data.MatchRecord) error
	// Release 退还某挂单 escrow 残余。幂等键 = orderID。
	Release(ctx context.Context, playerID, orderID uint64) error
}

// NoopSettlementLedger 是占位实现:冻结 / 结算 / 退还都成功(不真实扣转资产)。
type NoopSettlementLedger struct{}

// Freeze 永远成功(占位)。
func (NoopSettlementLedger) Freeze(_ context.Context, _, _ uint64, _ data.Side, _ uint32, _, _ int64) error {
	return nil
}

func (NoopSettlementLedger) Ensure(_ context.Context, _, _ uint64, _ data.Side, _ uint32, _, _ int64) error {
	return nil
}

// Settle 永远成功(占位)。
func (NoopSettlementLedger) Settle(_ context.Context, _ *data.MatchRecord) error { return nil }

// Release 永远成功(占位)。
func (NoopSettlementLedger) Release(_ context.Context, _, _ uint64) error { return nil }

// AuctionEventPusher 把成交 / 订单流转发 kafka(main.go 注入;弱依赖,nil 静默)。
type AuctionEventPusher interface {
	PushMatch(ctx context.Context, e *auctionv1.AuctionMatchEvent) error
	PushAudit(ctx context.Context, o *auctionv1.AuctionOrder) error
}

// snowflakeGen 是 snowflake.Node 的最小接口。
type snowflakeGen interface {
	Generate() uint64
}

// MarketLocker 抽象「跨实例的 per-market 单写者锁」(限制#2:多实例一致性)。
//
// 进程内 striped lock 只在单实例内串行;多实例部署时,同一 market 的挂单 / 撮合可能落到不同实例,
// 订单簿(Redis)与权威库(MySQL)会被并发改 → 可能超卖。MarketLocker 用 Redis 单写者 token
// (pkg/redislock,TTL ≤ 30s,不变量 §10)保证任一时刻同一 market 全局只有一个实例在撮合。
//
// 推荐再叠一致性哈希路由(同一 market 固定落同一实例,见 docs/design/infra.md)把锁竞争降到最低;
// 即便路由抖动 / rebalance,本锁仍兜底跨实例互斥。nil = 单实例,仅靠进程内 striped lock。
type MarketLocker interface {
	// Lock 阻塞式获取 market 的跨实例写锁,返回释放函数。竞争超时返回 ErrAuctionMarketBusy。
	Lock(ctx context.Context, marketID uint32) (release func(), err error)
}

// AuctionUsecase 是 auction 服务业务逻辑核心。
type AuctionUsecase struct {
	repo   data.AuctionRepo
	book   data.BookStore
	slots  data.OwnerSlotLimiter
	ledger SettlementLedger
	events AuctionEventPusher // 弱依赖,可为 nil
	cfg    conf.AuctionConf

	// rateQuota 挂单/出价/撤单频率配额(anti-abuse §6 第 6 项)。可为 nil(不限)。
	rateQuota ActionRateQuota

	// order_id 与 match_id 是两个互不相干的 ID 空间(各自独立的表与主键:
	// auction_orders.order_id / auction_matches.match_id),各持一个独立发号器。
	// 拆开的实际收益在撮合链:一笔大单跨越 N 档对手盘会在 matchOnce 的循环里
	// 连铸 N 个 match_id,与并发挂单的 order_id 各走各的 step 池(每池 32768/s)。
	//
	// ⚠️ 两者共用同一 nodeID,发出的 ID 会逐位相同(见 etcdnode.ProvideSnowflakeN)。
	// order_id 与 match_id 必须各自留在自己的表 / 唯一键里,禁止混进同一容器比较。
	orderSF snowflakeGen
	matchSF snowflakeGen

	marketLocker MarketLocker // 跨实例单写者锁(nil = 仅进程内串行)

	// marketRouter 是「市场 → 实例归属」一致性哈希路由(nil = 单实例,本实例拥有全部 market)。
	// 多实例部署时由 main 经 SetMarketRouter 注入:同一 market 固定落 owner 实例,把跨实例锁竞争
	// 降到最低。非 owner 实例处理某 market 仅作观测告警(路由抖动 / rebalance 信号),
	// 正确性仍由 marketLocker 兜底,不阻断业务(转发属基础设施,见 market_router.go 头注释)。
	marketRouter *MarketRouter

	// auditQueue 隔离弱依赖 Kafka：业务临界区只做一次有界非阻塞入队，单 worker 在锁外发送。
	// audit 不承担资产正确性，队列满时宁可告警丢弃，也不能反压撮合/撤单/补偿。
	auditMu     sync.RWMutex
	auditQueue  chan *auctionv1.AuctionOrder
	auditStop   chan struct{}
	auditClosed bool
	auditWG     sync.WaitGroup

	// locks 是固定容量的条带锁(striped lock):marketID % len 取条带。
	// 不用「map 惰性建永不删」——market_id 是客户端请求字段,入口只校验非 0,
	// 恶意刷不同 marketID 会让 map 无界增长(内存 DoS)。条带碰撞只是不同
	// market 偶尔串行,跨实例正确性仍由 marketLocker(per-market Redis 锁)兜底。
	locks [marketLockStripes]sync.Mutex

	// 退化期日志限流(模式 C):这两处都在**每请求**路径上,退化时会按 QPS 刷屏。
	// 与上面条带锁同理,不按 market_id 分桶(客户端可控字段,分桶会被恶意刷成内存 DoS);
	// plog.Window 无 key、常量内存、并发安全。
	//   - misroutedLog:路由抖动 / rebalance 期非 owner 实例处理请求(仅观测,锁兜底正确性);
	//   - auditDropLog:audit 队列满丢弃(audit 不承担资产正确性,丢弃是既定契约)。
	misroutedLog plog.Window
	auditDropLog plog.Window
}

// auctionDegradeLogWindowMs 是退化期日志的最小重打间隔(毫秒)。
const auctionDegradeLogWindowMs = 5000

// marketLockStripes 是进程内 market 条带锁数量(内存恒定,与 market 基数无关)。
const marketLockStripes = 256

// bookCacheWriteTimeout 是 Redis 兼容缓存单次写入的短超时。撮合候选由 MySQL 权威库选择，
// 因此缓存失败只能告警，不能让已冻结资产且已持久化的活跃订单对撮合不可见。
const bookCacheWriteTimeout = 250 * time.Millisecond

// ownerSlotWriteTimeout 让请求取消后仍有短窗口清理终态配额成员；MySQL 终态是权威，
// 清理失败会在玩家下次触顶时惰性重试。
const ownerSlotWriteTimeout = 2 * time.Second

// auditDispatchTimeout 给遵守 context 的 pusher 一个发送上限；不遵守 context 的底层实现也只会
// 卡住独立 audit worker，不会占用 market 锁或资产补偿 goroutine。
const auditDispatchTimeout = 5 * time.Second

// NewAuctionUsecase 构造。ledger 为 nil 时退化为 Noop;events 允许 nil。
// NewAuctionUsecase 构造 auction 业务核心。
//
// orderSF / matchSF 是 order_id 与 match_id 两个独立 ID 空间的发号器,由 main.go 经
// etcdnode.MustProvideSnowflakeN 取得(共用 nodeID、各自独立 step 池)。两者可以传同一个
// 实例(测试常这么做),但生产接线必须分开,否则失去拆分带来的容量隔离。
// ActionRateQuota 挂单/出价/撤单的 per-player 频率配额(实现 pkg/redisx.ActionQuota)。
// 总量闸(max_active_orders_per_player 200)只限「同时挂多少」,挡不住「下单-撤单」
// 循环(每轮产生托管/流水写),频率配额补这一维。背压非权威门:error 时 fail-open。
type ActionRateQuota interface {
	Allow(ctx context.Context, action string, subject uint64) (bool, error)
}

// SetRateQuota 注入频率配额(可选;不注入 = 不限,dev 联调兼容)。
func (u *AuctionUsecase) SetRateQuota(q ActionRateQuota) {
	u.rateQuota = q
}

// allowAction 频率配额门:窗内超额返回 ErrRateLimited(先于一切副作用);fail-open。
func (u *AuctionUsecase) allowAction(ctx context.Context, action string, playerID uint64) error {
	if u.rateQuota == nil {
		return nil
	}
	ok, err := u.rateQuota.Allow(ctx, action, playerID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "auction_rate_quota_check_failed",
			"action", action, "player_id", playerID, "err", err)
		return nil
	}
	if !ok {
		plog.With(ctx).Warnw("msg", "auction_rate_quota_rejected",
			"action", action, "player_id", playerID)
		return errcode.New(errcode.ErrRateLimited, "auction %s rate limited, retry later", action)
	}
	return nil
}

func NewAuctionUsecase(repo data.AuctionRepo, book data.BookStore, slots data.OwnerSlotLimiter, ledger SettlementLedger, events AuctionEventPusher, orderSF, matchSF snowflakeGen, cfg conf.AuctionConf) *AuctionUsecase {
	if ledger == nil {
		ledger = NoopSettlementLedger{}
	}
	if cfg.MaxQuantityPerOrder <= 0 {
		cfg.MaxQuantityPerOrder = 1_000_000
	}
	if cfg.MaxPrice <= 0 {
		cfg.MaxPrice = 1_000_000_000
	}
	if cfg.DefaultListLimit <= 0 {
		cfg.DefaultListLimit = 50
	}
	if cfg.MaxListLimit <= 0 {
		cfg.MaxListLimit = 200
	}
	if cfg.MaxActiveOrdersPerPlayer <= 0 {
		cfg.MaxActiveOrdersPerPlayer = 200
	}
	if cfg.SideEffectReconcileBatch <= 0 {
		cfg.SideEffectReconcileBatch = 100
	}
	if cfg.AuditQueueCapacity <= 0 {
		cfg.AuditQueueCapacity = 1024
	}
	u := &AuctionUsecase{
		repo:       repo,
		book:       book,
		slots:      slots,
		ledger:     ledger,
		events:     events,
		orderSF:    orderSF,
		matchSF:    matchSF,
		cfg:        cfg,
		auditQueue: make(chan *auctionv1.AuctionOrder, cfg.AuditQueueCapacity),
		auditStop:  make(chan struct{}),
	}
	u.auditWG.Add(1)
	go u.runAuditWorker()
	return u
}

// Close 停止弱依赖 audit worker。调用方必须在关闭底层 Kafka producer 前调用；不排空队列，
// 避免 broker 故障时让进程退出时间与积压条数成正比。
func (u *AuctionUsecase) Close() {
	u.auditMu.Lock()
	if u.auditClosed {
		u.auditMu.Unlock()
		return
	}
	u.auditClosed = true
	close(u.auditStop)
	u.auditMu.Unlock()
	u.auditWG.Wait()
}

func (u *AuctionUsecase) runAuditWorker() {
	defer u.auditWG.Done()
	for {
		// 先检查停止信号，避免 stop 与非空 queue 同时就绪时继续排空积压。
		select {
		case <-u.auditStop:
			return
		default:
		}
		select {
		case <-u.auditStop:
			return
		case o := <-u.auditQueue:
			if u.events == nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), auditDispatchTimeout)
			err := u.events.PushAudit(ctx, o)
			cancel()
			if err != nil {
				plog.With(context.Background()).Warnw("msg", "auction_audit_push_failed",
					"order_id", o.GetOrderId(), "err", err)
			}
		}
	}
}

// lockMarket 取 market 所在条带的单写者锁(固定数组,无分配无增长)。
func (u *AuctionUsecase) lockMarket(marketID uint32) *sync.Mutex {
	return &u.locks[marketID%marketLockStripes]
}

// SetMarketLocker 注入跨实例单写者锁(main.go 配 Redis 后调用)。nil 保持单实例进程内串行。
func (u *AuctionUsecase) SetMarketLocker(ml MarketLocker) { u.marketLocker = ml }

// SetMarketRouter 注入「市场 → 实例归属」一致性哈希路由(main.go 多实例部署时调用)。
// nil 保持单实例(本实例拥有全部 market)。
func (u *AuctionUsecase) SetMarketRouter(r *MarketRouter) { u.marketRouter = r }

// guardMarket 获取 market 的单写者保护:先进程内 striped lock(总是),
// 再(若配置)叠加跨实例 Redis 单写者锁。返回的释放函数按相反顺序解锁。
// 跨实例锁竞争超时返回 ErrAuctionMarketBusy(进程内锁已回退)。
func (u *AuctionUsecase) guardMarket(ctx context.Context, marketID uint32) (func(), error) {
	// 路由观测:多实例部署时,非 owner 实例处理某 market 说明路由抖动 / rebalance,
	// 仅告警(marketLocker 仍兜底正确性),不阻断 —— 转发由边缘 / 服务发现处理(基础设施)。
	if u.marketRouter != nil && !u.marketRouter.OwnsMarket(marketID) {
		// guardMarket 在每个写路径入口调用:rebalance / 路由抖动期会对成千上万条误路由
		// 请求逐条打 → 限流为首错 + 每窗口一条 + 累计数(稳态几乎不触发)。
		if ok, streak := u.misroutedLog.Admit(time.Now().UnixMilli(), auctionDegradeLogWindowMs); ok {
			plog.With(ctx).Warnw("msg", "auction_market_not_owned",
				"market_id", marketID, "self", u.marketRouter.Self(),
				"owner", u.marketRouter.Owner(marketID), "streak", streak)
		}
	} else if u.marketRouter != nil {
		if n, _ := u.misroutedLog.Recovered(); n > 0 {
			plog.With(ctx).Infow("msg", "auction_market_routing_recovered", "misrouted_total", n)
		}
	}

	m := u.lockMarket(marketID)
	m.Lock()
	if u.marketLocker == nil {
		return func() { m.Unlock() }, nil
	}
	release, err := u.marketLocker.Lock(ctx, marketID)
	if err != nil {
		m.Unlock()
		return nil, err
	}
	return func() {
		release()
		m.Unlock()
	}, nil
}

// PlaceOrder 卖家挂单(SELL)。ownerID 由 service 从 JWT ctx 得到(R5)。
func (u *AuctionUsecase) PlaceOrder(ctx context.Context, ownerID uint64, marketID, itemConfigID uint32, quantity, price int64, idemKey string) (*auctionv1.AuctionOrder, error) {
	return u.submit(ctx, ownerID, data.SideSell, marketID, itemConfigID, quantity, price, idemKey)
}

// Bid 买家出价(BUY)。ownerID 由 service 从 JWT ctx 得到(R5)。
func (u *AuctionUsecase) Bid(ctx context.Context, ownerID uint64, marketID, itemConfigID uint32, quantity, price int64, idemKey string) (*auctionv1.AuctionOrder, error) {
	return u.submit(ctx, ownerID, data.SideBuy, marketID, itemConfigID, quantity, price, idemKey)
}

// submit 是挂单 / 出价的统一入口：PENDING 幂等登记 → 幂等冻结 → 权威撮合 → 激活未成交余量。
// PENDING 使进程在「登记后、冻结前」退出时订单不会被其他请求选中；相同 idem 重试会继续
// Freeze(order_id) 并激活，不会另建订单或把未冻结订单暴露给撮合。
func (u *AuctionUsecase) submit(ctx context.Context, ownerID uint64, side data.Side, marketID, itemConfigID uint32, quantity, price int64, idemKey string) (*auctionv1.AuctionOrder, error) {
	if u.cfg.PassiveWarmup {
		return nil, errcode.New(errcode.ErrUnavailable, "auction is in passive warmup")
	}
	if ownerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "owner required")
	}
	// 频率配额(anti-abuse §6 第 6 项):按发起方计,先于 PENDING 登记等一切副作用。
	// 同 idem 的合法重试也计数——20/min 的额度下无感,不为它开豁免通道(豁免即绕过面)。
	if err := u.allowAction(ctx, "order", ownerID); err != nil {
		return nil, err
	}
	if marketID == 0 || itemConfigID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "market_id / item_config_id required")
	}
	if quantity <= 0 || quantity > u.cfg.MaxQuantityPerOrder {
		return nil, errcode.New(errcode.ErrInvalidArg, "quantity out of range: %d (max %d)", quantity, u.cfg.MaxQuantityPerOrder)
	}
	if price <= 0 || price > u.cfg.MaxPrice {
		return nil, errcode.New(errcode.ErrInvalidArg, "price out of range: %d (max %d)", price, u.cfg.MaxPrice)
	}
	// 防止成交总额(quantity * price)溢出 int64:下游 inventory 结算会算 total = quantity * unitPrice,
	// 即便单值都在上界内,极端组合仍可能溢出 → 在入口拒绝。
	if quantity > math.MaxInt64/price {
		return nil, errcode.New(errcode.ErrInvalidArg, "total value overflow: quantity %d * price %d", quantity, price)
	}
	if !validIdempotencyKey(idemKey) {
		return nil, errcode.New(errcode.ErrInvalidArg,
			"idempotency_key must be 1..64 ASCII characters [A-Za-z0-9._:-]")
	}

	release, err := u.guardMarket(ctx, marketID)
	if err != nil {
		return nil, err
	}
	defer release()

	now := nowMs()
	rec := &data.OrderRecord{
		OrderID:        u.orderSF.Generate(),
		MarketID:       marketID,
		OwnerID:        ownerID,
		Side:           side,
		ItemConfigID:   itemConfigID,
		Quantity:       quantity,
		FilledQuantity: 0,
		Price:          price,
		Status:         data.StatusPending,
		ReleasePending: false,
		MatchPending:   false,
		IdempotencyKey: idemKey,
		CreatedAtMs:    now,
		UpdatedAtMs:    now,
	}

	existing, already, err := u.repo.ClaimOrder(ctx, rec)
	if err != nil {
		return nil, err
	}
	// ClaimOrder 只要返回权威快照，就必须无条件沿用其中的 canonical order_id。
	// already 只决定是否可直接回放终态/活跃态，不能决定后续资产幂等键。
	if existing != nil {
		rec = existing
	}
	if already {
		if rec.Status != data.StatusPending {
			// 已激活或已终态的请求直接回放；只有 PENDING 才需要恢复冻结/激活步骤。
			if isTerminal(rec.Status) {
				u.releaseOwnerSlot(ctx, rec)
			}
			return toProtoOrder(rec), nil
		}
	}

	// PENDING 已落 MySQL 后才预留 Redis owner slot：成员始终可按 market_id+order_id 回查
	// 权威状态。预留失败必须条件终态化，不能留下绕过配额的可恢复 PENDING。
	if slotErr := u.reserveOwnerSlotPruning(ctx, rec); slotErr != nil {
		if rejectErr := u.rejectPendingAfterSlotFailure(ctx, rec); rejectErr != nil {
			return nil, rejectErr
		}
		return nil, slotErr
	}

	// Freeze 的幂等键是 order_id：首次调用成功后即使在激活前崩溃，同 idem 重试也只会确认
	// 同一笔冻结。失败结果可能包含网络不确定性，所以终态同时登记 release_pending，由 Release
	// 幂等消除「其实冻结成功但响应丢失」造成的永久锁资。
	if ferr := u.ledger.Freeze(ctx, ownerID, rec.OrderID, side, itemConfigID, quantity, price); ferr != nil {
		changed, uerr := u.repo.RejectPendingOrder(ctx, marketID, rec.OrderID, nowMs())
		if uerr != nil {
			plog.With(ctx).Errorw("msg", "auction_reject_after_freeze_fail_persist_failed", "order_id", rec.OrderID, "err", uerr)
			return nil, uerr
		}
		if changed {
			u.releaseOwnerSlot(ctx, rec)
			u.tryReleaseOrder(ctx, marketID, rec.OrderID)
		}
		return nil, ferr
	}
	confirmed, cerr := u.repo.ConfirmOrderEscrow(ctx, marketID, rec.OrderID, nowMs())
	if cerr != nil {
		return nil, cerr
	}
	if !confirmed {
		// 已冻结的权威 PENDING 单在两步之间凭空消失 = 严重状态一致性异常;显式 ERROR 带 order_id 取证。
		plog.With(ctx).Errorw("msg", "auction_confirm_escrow_lost",
			"market_id", marketID, "order_id", rec.OrderID, "step", "confirm_pending")
		return nil, errcode.New(errcode.ErrInternal, "confirm escrow lost pending order %d", rec.OrderID)
	}
	rec.EscrowVerified = true
	// 保持 incoming=PENDING 先撮合：ReserveMatch 只允许 incoming 是该内部状态，resting 必须活跃。
	// 若先 Activate 再撮合，进程恰在两步之间退出会留下已交叉却无人主动处理的 OPEN 单。
	if err := u.match(ctx, rec); err != nil {
		return nil, err
	}
	if rec.Status == data.StatusPending {
		activated, aerr := u.repo.ActivateOrder(ctx, marketID, rec.OrderID, nowMs())
		if aerr != nil {
			return nil, aerr
		}
		if !activated {
			current, found, gerr := u.repo.GetOrder(ctx, marketID, rec.OrderID)
			if gerr != nil {
				return nil, gerr
			}
			if !found {
				plog.With(ctx).Errorw("msg", "auction_activated_order_disappeared",
					"market_id", marketID, "order_id", rec.OrderID)
				return nil, errcode.New(errcode.ErrInternal, "activated order %d disappeared", rec.OrderID)
			}
			return toProtoOrder(current), nil
		}
		rec.Status = data.StatusOpen
		rec.UpdatedAtMs = nowMs()
	}

	if rec.Remaining() > 0 {
		// Redis 仅作旧版本兼容缓存；失败不得影响 MySQL 中已冻结且可撮合的订单。
		u.addBookCache(ctx, rec)
	} else {
		u.removeBookCache(ctx, rec)
		u.tryReleaseOrder(ctx, rec.MarketID, rec.OrderID)
	}
	u.pushAudit(ctx, toProtoOrder(rec))
	return toProtoOrder(rec), nil
}

// match 让 incoming 与同一具体物品的对手盘逐笔撮合。调用方已持 market 锁(单写者)。
// 候选必须从 MySQL 权威库按价格-时间顺序选择：Redis ZSET 的旧 key 只含 market_id，无法
// 区分同品类里的不同 item_config_id；把它放在正确性链路会导致跨物品成交、固定前缀饥饿，
// 以及缓存写失败/进程崩溃后的永久不可见。Redis 现在只保留作旧版本回滚兼容缓存。
func (u *AuctionUsecase) match(ctx context.Context, incoming *data.OrderRecord) error {
	opp := opposite(incoming.Side)
	conflicts := 0
	for incoming.Remaining() > 0 {
		resting, found, err := u.repo.FindBestActiveOrder(
			ctx, incoming.MarketID, incoming.ItemConfigID, opp, incoming.OwnerID)
		if err != nil {
			return err
		}
		if !found || !crosses(incoming.Side, incoming.Price, resting.Price) {
			break // 无同物品非己方对手盘，或最优价不交叉。
		}
		m, updatedIncoming, updatedResting, reserved, rerr := u.repo.ReserveMatch(
			ctx, incoming.MarketID, incoming.OrderID, resting.OrderID, u.matchSF.Generate(), nowMs())
		if rerr != nil {
			return rerr
		}
		if updatedIncoming != nil {
			*incoming = *updatedIncoming
		}
		if !reserved {
			// 候选在 SELECT 与 FOR UPDATE 之间被别的实例消费。有限重试防异常替身/热点竞争忙等。
			conflicts++
			if incoming.Remaining() <= 0 || isTerminal(incoming.Status) {
				break
			}
			if conflicts >= 64 {
				// CAS/TOCTOU 失败方重试耗尽:热点 market / 异常替身竞争,玩家挂单反复 Busy。
				plog.With(ctx).Warnw("msg", "auction_reserve_conflicts_exhausted",
					"market_id", incoming.MarketID, "order_id", incoming.OrderID, "conflicts", conflicts)
				return errcode.New(errcode.ErrAuctionMarketBusy, "too many concurrent reserve conflicts market=%d", incoming.MarketID)
			}
			continue
		}
		conflicts = 0
		if u.settleMatch(ctx, m) {
			u.tryReleaseOrder(ctx, m.MarketID, m.SellOrderID)
			u.tryReleaseOrder(ctx, m.MarketID, m.BuyOrderID)
		}
		resting = updatedResting
		for _, terminal := range []*data.OrderRecord{incoming, resting} {
			if terminal != nil && isTerminal(terminal.Status) {
				u.removeBookCache(ctx, terminal)
				u.releaseOwnerSlot(ctx, terminal)
			}
		}
		u.pushAudit(ctx, toProtoOrder(resting))
	}
	if incoming.MatchPending && (incoming.Status == data.StatusOpen || incoming.Status == data.StatusPartial) {
		if _, err := u.repo.ClearMatchPending(ctx, incoming.MarketID, incoming.OrderID); err != nil {
			return err
		}
		incoming.MatchPending = false
	}
	return nil
}

func (u *AuctionUsecase) addBookCache(ctx context.Context, o *data.OrderRecord) {
	if u.book == nil {
		return
	}
	cacheCtx, cancel := context.WithTimeout(plog.Detach(ctx), bookCacheWriteTimeout)
	defer cancel()
	if err := u.book.Add(cacheCtx, o.MarketID, o.Side, o.OrderID, o.Price); err != nil {
		plog.With(ctx).Debugw("msg", "auction_book_cache_add_failed",
			"market_id", o.MarketID, "order_id", o.OrderID, "err", err)
	}
}

func (u *AuctionUsecase) removeBookCache(ctx context.Context, o *data.OrderRecord) {
	if u.book == nil {
		return
	}
	cacheCtx, cancel := context.WithTimeout(plog.Detach(ctx), bookCacheWriteTimeout)
	defer cancel()
	if err := u.book.Remove(cacheCtx, o.MarketID, o.Side, o.OrderID); err != nil {
		plog.With(ctx).Debugw("msg", "auction_book_cache_remove_failed",
			"market_id", o.MarketID, "order_id", o.OrderID, "err", err)
	}
}

// reserveOwnerSlotPruning 先从 MySQL 权威库有界读取该玩家最老的 active/PENDING 订单，
// 再原子预热 Redis 配额索引并预留当前订单。这样升级后首次新写也会计入 legacy 活跃单，
// 不会因 Redis 尚未建立索引而暂时突破硬上限。触顶时清理终态/已不存在成员后重试一次。
func (u *AuctionUsecase) reserveOwnerSlotPruning(ctx context.Context, o *data.OrderRecord) error {
	if u.slots == nil {
		return errcode.New(errcode.ErrInternal, "owner slot limiter not configured")
	}
	maxSlots := u.cfg.MaxActiveOrdersPerPlayer
	slot := data.OwnerOrderSlot{MarketID: o.MarketID, OrderID: o.OrderID}
	for attempt := 0; attempt < 2; attempt++ {
		authoritative, err := u.repo.ListOwnerActiveAndPending(ctx, o.OwnerID, maxSlots+1)
		if err != nil {
			return err
		}
		legacySlots := make([]data.OwnerOrderSlot, 0, len(authoritative))
		for _, existing := range authoritative {
			legacySlots = append(legacySlots, data.OwnerOrderSlot{
				MarketID: existing.MarketID,
				OrderID:  existing.OrderID,
			})
		}
		if _, err = u.slots.Sync(ctx, o.OwnerID, legacySlots, maxSlots); err != nil {
			return err
		}

		ok, err := u.slots.Reserve(ctx, o.OwnerID, slot, maxSlots)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if attempt == 0 {
			pruned, perr := u.pruneOwnerSlots(ctx, o.OwnerID)
			if perr != nil {
				return perr
			}
			if pruned > 0 {
				continue
			}
		}
		break
	}
	return errcode.New(errcode.ErrAuctionOrderLimit,
		"player %d has too many active auction orders (max %d)", o.OwnerID, maxSlots)
}

func (u *AuctionUsecase) pruneOwnerSlots(ctx context.Context, ownerID uint64) (int, error) {
	slots, err := u.slots.List(ctx, ownerID, u.cfg.MaxActiveOrdersPerPlayer)
	if err != nil {
		return 0, err
	}
	pruned := 0
	for _, slot := range slots {
		o, found, getErr := u.repo.GetOrder(ctx, slot.MarketID, slot.OrderID)
		if getErr != nil {
			// 状态查询不确定时绝不释放名额(fail-closed)。持续读失败会让玩家卡在 max_active_orders
			// 上限却清不出名额,DEBUG 留线索(LOG_LEVEL=debug 排查"为何清不掉某 slot")。
			plog.With(ctx).Debugw("msg", "auction_prune_order_read_failed",
				"owner_id", ownerID, "market_id", slot.MarketID, "order_id", slot.OrderID, "err", getErr)
			continue
		}
		if found && (o.Status == data.StatusPending || o.Status == data.StatusOpen || o.Status == data.StatusPartial) {
			continue
		}
		if found && !isTerminal(o.Status) {
			continue // 未知新状态也 fail-closed，避免未来状态演进误清活跃订单。
		}
		if releaseErr := u.slots.Release(ctx, ownerID, slot); releaseErr != nil {
			plog.With(ctx).Warnw("msg", "auction_owner_slot_prune_failed", "owner_id", ownerID,
				"market_id", slot.MarketID, "order_id", slot.OrderID, "err", releaseErr)
			continue
		}
		pruned++
	}
	if pruned > 0 {
		plog.With(ctx).Infow("msg", "auction_owner_slots_pruned", "owner_id", ownerID, "pruned", pruned)
	}
	return pruned, nil
}

// rejectPendingAfterSlotFailure 条件取消尚未冻结/激活的 PENDING。Reserve 响应可能不确定，
// 因此取消提交后仍幂等 SREM，避免「脚本已成功但响应丢失」留下死成员。
func (u *AuctionUsecase) rejectPendingAfterSlotFailure(ctx context.Context, o *data.OrderRecord) error {
	changed, err := u.repo.RejectPendingOrder(ctx, o.MarketID, o.OrderID, nowMs())
	if err != nil {
		return err
	}
	if !changed {
		return errcode.New(errcode.ErrInternal, "pending order %d could not be rejected after slot failure", o.OrderID)
	}
	o.Status = data.StatusCanceled
	o.ReleasePending = true
	u.releaseOwnerSlot(ctx, o)
	u.tryReleaseOrder(ctx, o.MarketID, o.OrderID)
	return nil
}

func (u *AuctionUsecase) releaseOwnerSlot(ctx context.Context, o *data.OrderRecord) {
	if u.slots == nil || o == nil || o.OwnerID == 0 || o.MarketID == 0 || o.OrderID == 0 {
		return
	}
	releaseCtx, cancel := context.WithTimeout(plog.Detach(ctx), ownerSlotWriteTimeout)
	defer cancel()
	if err := u.slots.Release(releaseCtx, o.OwnerID,
		data.OwnerOrderSlot{MarketID: o.MarketID, OrderID: o.OrderID}); err != nil {
		plog.With(ctx).Warnw("msg", "auction_owner_slot_release_failed", "owner_id", o.OwnerID,
			"market_id", o.MarketID, "order_id", o.OrderID, "err", err)
	}
}

// CancelOrder 撤单(仅挂单本人,未终态前)。ownerID 由 service 从 JWT ctx 得到(R5)。
func (u *AuctionUsecase) CancelOrder(ctx context.Context, ownerID uint64, marketID uint32, orderID uint64) error {
	if u.cfg.PassiveWarmup {
		return errcode.New(errcode.ErrUnavailable, "auction is in passive warmup")
	}
	if ownerID == 0 || marketID == 0 || orderID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "owner / market / order required")
	}
	// 频率配额(anti-abuse §6 第 6 项):下撤循环两端都限。
	if err := u.allowAction(ctx, "cancel", ownerID); err != nil {
		return err
	}
	release, err := u.guardMarket(ctx, marketID)
	if err != nil {
		return err
	}
	defer release()

	o, found, err := u.repo.GetOrder(ctx, marketID, orderID)
	if err != nil {
		return err
	}
	if !found {
		return errcode.New(errcode.ErrAuctionOrderNotFound, "order %d not found", orderID)
	}
	if o.OwnerID != ownerID {
		return errcode.New(errcode.ErrAuctionNotOwner, "player %d not owner of order %d", ownerID, orderID)
	}
	if isTerminal(o.Status) {
		return errcode.New(errcode.ErrAuctionWrongState, "order %d already terminal", orderID)
	}
	if o.Status != data.StatusOpen && o.Status != data.StatusPartial {
		return errcode.New(errcode.ErrAuctionWrongState, "order %d is not active", orderID)
	}
	changed, uerr := u.repo.MarkOrderTerminal(ctx, marketID, orderID, data.StatusCanceled, nowMs())
	if uerr != nil {
		return uerr
	}
	if !changed {
		return errcode.New(errcode.ErrAuctionWrongState, "order %d changed concurrently", orderID)
	}
	o.Status = data.StatusCanceled
	o.ReleasePending = true
	o.UpdatedAtMs = nowMs()
	u.removeBookCache(ctx, o)
	u.releaseOwnerSlot(ctx, o)
	// 终态与 release_pending 已原子提交；账本失败只延迟释放，不会丢失补偿意图。
	u.tryReleaseOrder(ctx, marketID, orderID)
	u.pushAudit(ctx, toProtoOrder(o))
	return nil
}

// ExpireDueOrders 清扫一批已过期(创建超过 OrderTTL)仍未成交的挂单:置 EXPIRED、移出订单簿、
// 退还 escrow(限制#1 补偿:挂单冻结的资产不会因长期挂单而永久锁死)。返回本批处理条数。
// OrderTTLSeconds <= 0 时直接返回 0(不过期)。每单都按 market 单写者锁串行处理,与撮合 / 撤单互斥。
func (u *AuctionUsecase) ExpireDueOrders(ctx context.Context) (int, error) {
	if u.cfg.PassiveWarmup {
		return 0, errcode.New(errcode.ErrUnavailable, "auction is in passive warmup")
	}
	if u.cfg.OrderTTLSeconds <= 0 {
		return 0, nil
	}
	cutoff := nowMs() - u.cfg.OrderTTLSeconds*1000
	batch := u.cfg.ExpirySweepBatch
	if batch <= 0 {
		batch = 200
	}
	dueOrders, err := u.repo.ListExpirableOrders(ctx, cutoff, batch)
	if err != nil {
		return 0, err
	}
	var done int
	for _, o := range dueOrders {
		if err := u.expireOne(ctx, o.MarketID, o.OrderID); err != nil {
			// 单条失败不阻断整批:记日志继续(下轮重扫)。
			plog.With(ctx).Warnw("msg", "auction_expire_one_failed",
				"market_id", o.MarketID, "order_id", o.OrderID, "err", err)
			continue
		}
		done++
	}
	return done, nil
}

// expireOne 在持有 market 单写者锁下让单个挂单过期(置 EXPIRED + 移出簿 + 退还 escrow)。
func (u *AuctionUsecase) expireOne(ctx context.Context, marketID uint32, orderID uint64) error {
	release, err := u.guardMarket(ctx, marketID)
	if err != nil {
		return err
	}
	defer release()

	// 持锁后重读:可能已被撮合 / 撤单到终态,避免误改。
	o, found, err := u.repo.GetOrder(ctx, marketID, orderID)
	if err != nil {
		return err
	}
	if !found || isTerminal(o.Status) {
		return nil
	}
	changed, uerr := u.repo.MarkOrderTerminal(ctx, marketID, orderID, data.StatusExpired, nowMs())
	if uerr != nil {
		return uerr
	}
	if !changed {
		return nil
	}
	o.Status = data.StatusExpired
	o.ReleasePending = true
	o.UpdatedAtMs = nowMs()
	u.removeBookCache(ctx, o)
	u.releaseOwnerSlot(ctx, o)
	u.tryReleaseOrder(ctx, marketID, orderID)
	u.pushAudit(ctx, toProtoOrder(o))
	return nil
}

// ListMarket 看某市场订单簿。side=UNSPECIFIED → 返回买 + 卖两侧。
func (u *AuctionUsecase) ListMarket(ctx context.Context, marketID uint32, side data.Side, limit int) ([]*auctionv1.AuctionOrder, error) {
	if marketID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "market_id required")
	}
	if limit <= 0 {
		limit = u.cfg.DefaultListLimit
	}
	if limit > u.cfg.MaxListLimit {
		limit = u.cfg.MaxListLimit
	}

	var out []*auctionv1.AuctionOrder
	if side == data.SideSell || side == 0 {
		recs, err := u.repo.ListMarketOrders(ctx, marketID, data.SideSell, limit)
		if err != nil {
			return nil, err
		}
		out = append(out, toProtoOrders(recs)...)
	}
	if side == data.SideBuy || side == 0 {
		recs, err := u.repo.ListMarketOrders(ctx, marketID, data.SideBuy, limit)
		if err != nil {
			return nil, err
		}
		out = append(out, toProtoOrders(recs)...)
	}
	return out, nil
}

// ListMyOrders 看玩家自己的挂单 / 出价，按全局 order_id DESC 游标分页。
func (u *AuctionUsecase) ListMyOrders(
	ctx context.Context, ownerID uint64, activeOnly bool, cursorOrderID uint64, limit int,
) ([]*auctionv1.AuctionOrder, uint64, bool, error) {
	if ownerID == 0 {
		return nil, 0, false, errcode.New(errcode.ErrInvalidArg, "owner required")
	}
	limit = clampMyOrdersLimit(limit)
	recs, err := u.repo.ListOwnerOrders(ctx, ownerID, activeOnly, cursorOrderID, limit+1)
	if err != nil {
		return nil, 0, false, err
	}
	hasMore := len(recs) > limit
	if hasMore {
		recs = recs[:limit]
	}
	var next uint64
	if hasMore && len(recs) > 0 {
		next = recs[len(recs)-1].OrderID
	}
	return toProtoOrders(recs), next, hasMore, nil
}

const (
	defaultMyOrdersLimit = 50
	maxMyOrdersLimit     = 100
)

func validIdempotencyKey(key string) bool {
	if len(key) == 0 || len(key) > 64 {
		return false
	}
	for _, c := range key {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '.' || c == '_' || c == ':' || c == '-' {
			continue
		}
		return false
	}
	return true
}

func clampMyOrdersLimit(limit int) int {
	if limit <= 0 {
		return defaultMyOrdersLimit
	}
	if limit > maxMyOrdersLimit {
		return maxMyOrdersLimit
	}
	return limit
}

// ── 辅助 ──────────────────────────────────────────────────────────────────────

// opposite 返回对手盘方向。
func opposite(s data.Side) data.Side {
	if s == data.SideSell {
		return data.SideBuy
	}
	return data.SideSell
}

// crosses 判断 incoming 价格能否吃到对手盘最优价 bestPrice。
//   - incoming SELL @ P:对手是 BUY,bestPrice(最高买价)>= P 才成交。
//   - incoming BUY  @ P:对手是 SELL,bestPrice(最低卖价)<= P 才成交。
func crosses(side data.Side, price, bestPrice int64) bool {
	if side == data.SideSell {
		return bestPrice >= price
	}
	return bestPrice <= price
}

// isTerminal 判断订单是否已到终态(不可再流转)。
func isTerminal(status int32) bool {
	switch status {
	case data.StatusFilled, data.StatusCanceled, data.StatusExpired:
		return true
	default:
		return false
	}
}

func toProtoOrder(r *data.OrderRecord) *auctionv1.AuctionOrder {
	return &auctionv1.AuctionOrder{
		OrderId:        r.OrderID,
		MarketId:       r.MarketID,
		OwnerId:        r.OwnerID,
		Side:           auctionv1.OrderSide(r.Side),
		ItemConfigId:   r.ItemConfigID,
		Quantity:       r.Quantity,
		FilledQuantity: r.FilledQuantity,
		// 内部 Price 是 int64(撮合簿需要取负编码,见 service/auction.go priceToInternal);
		// 下行协议按 §5.12 用 uint64。入口已保证 > 0 且 <= MaxPrice,转换安全。
		Price:          uint64(r.Price),
		Status:         auctionv1.AuctionOrderStatus(r.Status),
		CreatedAtMs:    r.CreatedAtMs,
		UpdatedAtMs:    r.UpdatedAtMs,
	}
}

func toProtoOrders(recs []*data.OrderRecord) []*auctionv1.AuctionOrder {
	out := make([]*auctionv1.AuctionOrder, 0, len(recs))
	for _, r := range recs {
		out = append(out, toProtoOrder(r))
	}
	return out
}

func toProtoMatch(m *data.MatchRecord) *auctionv1.AuctionMatchEvent {
	return &auctionv1.AuctionMatchEvent{
		MatchId:      m.MatchID,
		MarketId:     m.MarketID,
		SellOrderId:  m.SellOrderID,
		BuyOrderId:   m.BuyOrderID,
		SellerId:     m.SellerID,
		BuyerId:      m.BuyerID,
		ItemConfigId: m.ItemConfigID,
		Quantity:     m.Quantity,
		Price:        uint64(m.Price),
		MatchedAtMs:  m.MatchedAtMs,
	}
}

// publishMatchEvent 投递已由 MySQL outbox marker 登记的成交事件。Kafka 至少一次；
// 发送成功后清 marker 前退出只会重复同一 match_id，不再产生确定性丢失。
func (u *AuctionUsecase) publishMatchEvent(ctx context.Context, m *data.MatchRecord) bool {
	if u.events != nil {
		attemptCtx, cancel := context.WithTimeout(plog.Detach(ctx), 5*time.Second)
		err := u.events.PushMatch(attemptCtx, toProtoMatch(m))
		cancel()
		if err != nil {
			plog.With(ctx).Warnw("msg", "auction_match_push_pending", "match_id", m.MatchID, "err", err)
			u.deferMatchEvent(ctx, m)
			return false
		}
	}
	writeCtx, cancel := context.WithTimeout(plog.Detach(ctx), 2*time.Second)
	defer cancel()
	_, err := u.repo.ClearMatchEventPending(writeCtx, m.MarketID, m.MatchID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "auction_match_event_clear_failed", "match_id", m.MatchID, "err", err)
		u.deferMatchEvent(ctx, m)
		return false
	}
	return true
}

// pushAudit 只做有界、非阻塞入队；绝不在调用者持有的 market 锁或资产补偿 goroutine 内碰 Kafka。
func (u *AuctionUsecase) pushAudit(ctx context.Context, o *auctionv1.AuctionOrder) {
	if o == nil {
		return
	}
	copyOrder, ok := proto.Clone(o).(*auctionv1.AuctionOrder)
	if !ok {
		plog.With(ctx).Warnw("msg", "auction_audit_clone_failed", "order_id", o.GetOrderId())
		return
	}
	u.auditMu.RLock()
	defer u.auditMu.RUnlock()
	if u.auditClosed {
		return
	}
	select {
	case u.auditQueue <- copyOrder:
		if n, dropped := u.auditDropLog.Recovered(); n > 0 {
			plog.With(ctx).Infow("msg", "auction_audit_queue_recovered", "dropped_total", dropped, "windows", n)
		}
	default:
		// broker 慢 / 挂时队列持续打满,每个后续 submit/cancel/expire 都会丢弃 → 逐条打会
		// 按 QPS 洪泛。限流后首条即带累计丢弃数,比逐条更能回答「丢了多少 audit」。
		total := u.auditDropLog.AddExtra(1)
		if ok, streak := u.auditDropLog.Admit(time.Now().UnixMilli(), auctionDegradeLogWindowMs); ok {
			plog.With(ctx).Warnw("msg", "auction_audit_queue_full_drop",
				"order_id", o.GetOrderId(), "capacity", cap(u.auditQueue),
				"dropped_total", total, "streak", streak)
		}
	}
}

// settleMatch 执行已持久化成交的外部结算。失败时保留 PENDING，由后台以 match_id 幂等重试。
// CompleteMatch 在同一步登记 event outbox；Kafka 只能由后台扫描发送，绝不能阻塞 market 锁。
func (u *AuctionUsecase) settleMatch(ctx context.Context, m *data.MatchRecord) bool {
	attemptCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := u.ledger.Settle(attemptCtx, m); err != nil {
		plog.With(ctx).Errorw("msg", "auction_settle_pending", "match_id", m.MatchID, "err", err)
		u.deferMatchSettlement(ctx, m)
		return false
	}
	completed, err := u.repo.CompleteMatch(ctx, m.MarketID, m.MatchID)
	if err != nil {
		// Settle 已成功但状态仍 PENDING：下一轮重复 Settle 安全，因为 match_id 是幂等键。
		plog.With(ctx).Errorw("msg", "auction_complete_match_failed", "match_id", m.MatchID, "err", err)
		u.deferMatchSettlement(ctx, m)
		return false
	}
	if !completed {
		// 其他实例可能已完成同一 PENDING 快照；Settle 本身以 match_id 幂等，视为已收敛，
		// 但只有条件更新成功者发送成交事件。
		return true
	}
	m.SettlementStatus = data.SettlementCompleted
	m.EventPending = true
	return true
}

const sideEffectRetryDelay = 30 * time.Second

// deferMatchSettlement 用独立短上下文持久化退避时间；请求取消不能让永久失败记录继续
// 占住固定批次。若 MySQL 同时不可用，marker 保持立即就绪，数据库恢复后会再次尝试。
func (u *AuctionUsecase) deferMatchSettlement(ctx context.Context, m *data.MatchRecord) {
	writeCtx, cancel := context.WithTimeout(plog.Detach(ctx), 2*time.Second)
	defer cancel()
	if _, err := u.repo.DeferMatchSettlement(writeCtx, m.MarketID, m.MatchID, nowMs()+sideEffectRetryDelay.Milliseconds()); err != nil {
		plog.With(ctx).Errorw("msg", "auction_defer_settlement_failed", "match_id", m.MatchID, "err", err)
	}
}

func (u *AuctionUsecase) deferMatchEvent(ctx context.Context, m *data.MatchRecord) {
	writeCtx, cancel := context.WithTimeout(plog.Detach(ctx), 2*time.Second)
	defer cancel()
	if _, err := u.repo.DeferMatchEvent(writeCtx, m.MarketID, m.MatchID, nowMs()+sideEffectRetryDelay.Milliseconds()); err != nil {
		plog.With(ctx).Errorw("msg", "auction_defer_match_event_failed", "match_id", m.MatchID, "err", err)
	}
}

func (u *AuctionUsecase) deferOrderRelease(ctx context.Context, marketID uint32, orderID uint64) {
	writeCtx, cancel := context.WithTimeout(plog.Detach(ctx), 2*time.Second)
	defer cancel()
	if _, err := u.repo.DeferOrderRelease(writeCtx, marketID, orderID, nowMs()+sideEffectRetryDelay.Milliseconds()); err != nil {
		plog.With(ctx).Errorw("msg", "auction_defer_release_failed", "market_id", marketID, "order_id", orderID, "err", err)
	}
}

func (u *AuctionUsecase) deferOrderReconcile(ctx context.Context, marketID uint32, orderID uint64) {
	writeCtx, cancel := context.WithTimeout(plog.Detach(ctx), 2*time.Second)
	defer cancel()
	if _, err := u.repo.DeferOrderReconcile(writeCtx, marketID, orderID, nowMs()+sideEffectRetryDelay.Milliseconds()); err != nil {
		plog.With(ctx).Errorw("msg", "auction_defer_order_reconcile_failed",
			"market_id", marketID, "order_id", orderID, "err", err)
	}
}

// releaseOrder 只处理 MySQL 已确认「无待结算成交引用」的终态订单。Release 成功但清标记失败
// 时下轮会按 order_id 再调一次，依赖账本幂等而不会重复返还资产。
func (u *AuctionUsecase) releaseOrder(ctx context.Context, marketID uint32, orderID uint64) (bool, error) {
	o, found, err := u.repo.GetReleasableOrder(ctx, marketID, orderID)
	if err != nil || !found {
		return false, err
	}
	// 订单已由 MySQL 确认为终态，owner 配额与外部 escrow 释放可独立收敛。
	u.releaseOwnerSlot(ctx, o)
	attemptCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := u.ledger.Release(attemptCtx, o.OwnerID, o.OrderID); err != nil {
		u.deferOrderRelease(ctx, marketID, orderID)
		return false, err
	}
	cleared, err := u.repo.ClearReleasePending(ctx, marketID, orderID)
	if err != nil {
		u.deferOrderRelease(ctx, marketID, orderID)
		return false, err
	}
	return cleared, nil
}

func (u *AuctionUsecase) tryReleaseOrder(ctx context.Context, marketID uint32, orderID uint64) {
	if _, err := u.releaseOrder(ctx, marketID, orderID); err != nil {
		plog.With(ctx).Errorw("msg", "auction_release_pending",
			"market_id", marketID, "order_id", orderID, "err", err)
	}
}

// recoverPendingOrder 恢复 Claim 后进程退出留下的内部 PENDING。即使客户端永不重试，后台也会
// 用同一个 order_id 幂等 Freeze 后激活；Freeze 返回任何错误都按结果不确定处理为终态并补 Release。
func (u *AuctionUsecase) recoverPendingOrder(ctx context.Context, snapshot *data.OrderRecord) error {
	release, err := u.guardMarket(ctx, snapshot.MarketID)
	if err != nil {
		return err
	}
	defer release()
	o, found, err := u.repo.GetOrder(ctx, snapshot.MarketID, snapshot.OrderID)
	if err != nil || !found || o.Status != data.StatusPending {
		return err
	}
	// 后台恢复也必须先占有 owner slot，不能因客户端不再重试而绕过硬上限。
	if slotErr := u.reserveOwnerSlotPruning(ctx, o); slotErr != nil {
		if rejectErr := u.rejectPendingAfterSlotFailure(ctx, o); rejectErr != nil {
			return rejectErr
		}
		plog.With(ctx).Warnw("msg", "auction_pending_order_rejected_after_slot_failure",
			"order_id", o.OrderID, "err", slotErr)
		return nil
	}
	attemptCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	ferr := u.ledger.Freeze(attemptCtx, o.OwnerID, o.OrderID, o.Side, o.ItemConfigID, o.Quantity, o.Price)
	cancel()
	if ferr != nil {
		changed, rerr := u.repo.RejectPendingOrder(ctx, o.MarketID, o.OrderID, nowMs())
		if rerr != nil {
			return rerr
		}
		if changed {
			u.releaseOwnerSlot(ctx, o)
			if _, rerr := u.releaseOrder(ctx, o.MarketID, o.OrderID); rerr != nil {
				return rerr
			}
		}
		plog.With(ctx).Warnw("msg", "auction_pending_order_rejected_after_freeze_error",
			"order_id", o.OrderID, "err", ferr)
		return nil
	}
	confirmed, cerr := u.repo.ConfirmOrderEscrow(ctx, o.MarketID, o.OrderID, nowMs())
	if cerr != nil {
		return cerr
	}
	if !confirmed {
		return errcode.New(errcode.ErrInternal, "confirm recovered escrow lost order %d", o.OrderID)
	}
	o.EscrowVerified = true
	if err := u.match(ctx, o); err != nil {
		return err
	}
	if o.Status == data.StatusPending {
		activated, aerr := u.repo.ActivateOrder(ctx, o.MarketID, o.OrderID, nowMs())
		if aerr != nil || !activated {
			return aerr
		}
		o.Status = data.StatusOpen
		o.UpdatedAtMs = nowMs()
	}
	if o.Remaining() > 0 {
		u.addBookCache(ctx, o)
	} else {
		u.removeBookCache(ctx, o)
		u.tryReleaseOrder(ctx, o.MarketID, o.OrderID)
	}
	u.pushAudit(ctx, toProtoOrder(o))
	return nil
}

// recoverUnverifiedActiveOrder 验证/补冻旧二进制遗留的 OPEN/PARTIAL。只有 inventory 确认
// 剩余托管充足后才置 verified 并主动续跑撮合；确定性不一致则取消并幂等退还残余。
func (u *AuctionUsecase) recoverUnverifiedActiveOrder(ctx context.Context, snapshot *data.OrderRecord) error {
	release, err := u.guardMarket(ctx, snapshot.MarketID)
	if err != nil {
		return err
	}
	defer release()
	o, found, err := u.repo.GetOrder(ctx, snapshot.MarketID, snapshot.OrderID)
	if err != nil || !found || o.EscrowVerified ||
		(o.Status != data.StatusOpen && o.Status != data.StatusPartial) {
		return err
	}
	if slotErr := u.reserveOwnerSlotPruning(ctx, o); slotErr != nil {
		if errcode.As(slotErr) != errcode.ErrAuctionOrderLimit {
			return slotErr
		}
		return u.cancelUnverifiedOrder(ctx, o, slotErr)
	}
	attemptCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	ensureErr := u.ledger.Ensure(attemptCtx, o.OwnerID, o.OrderID, o.Side, o.ItemConfigID, o.Remaining(), o.Price)
	cancel()
	if ensureErr != nil {
		switch errcode.As(ensureErr) {
		case errcode.ErrAuctionInsufficient, errcode.ErrInventoryInsufficient,
			errcode.ErrInventoryIdempotencyConflict, errcode.ErrInvalidArg:
			return u.cancelUnverifiedOrder(ctx, o, ensureErr)
		default:
			return ensureErr
		}
	}
	confirmed, err := u.repo.ConfirmOrderEscrow(ctx, o.MarketID, o.OrderID, nowMs())
	if err != nil {
		return err
	}
	if !confirmed {
		return errcode.New(errcode.ErrInternal, "confirm legacy escrow lost order %d", o.OrderID)
	}
	o.EscrowVerified = true
	o.MatchPending = true
	if err := u.match(ctx, o); err != nil {
		return err
	}
	if o.Remaining() > 0 {
		u.addBookCache(ctx, o)
	} else {
		u.removeBookCache(ctx, o)
		u.tryReleaseOrder(ctx, o.MarketID, o.OrderID)
	}
	u.pushAudit(ctx, toProtoOrder(o))
	return nil
}

func (u *AuctionUsecase) cancelUnverifiedOrder(ctx context.Context, o *data.OrderRecord, cause error) error {
	changed, err := u.repo.MarkOrderTerminal(ctx, o.MarketID, o.OrderID, data.StatusCanceled, nowMs())
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	o.Status = data.StatusCanceled
	o.EscrowVerified = false
	o.MatchPending = false
	o.ReleasePending = true
	u.removeBookCache(ctx, o)
	u.releaseOwnerSlot(ctx, o)
	u.tryReleaseOrder(ctx, o.MarketID, o.OrderID)
	u.pushAudit(ctx, toProtoOrder(o))
	plog.With(ctx).Warnw("msg", "auction_legacy_order_canceled", "order_id", o.OrderID, "err", cause)
	return nil
}

// recoverMatchPendingOrder 续跑 Reserve 事务已提交、但 incoming 尚未把交叉对手盘扫尽的订单。
func (u *AuctionUsecase) recoverMatchPendingOrder(ctx context.Context, snapshot *data.OrderRecord) error {
	release, err := u.guardMarket(ctx, snapshot.MarketID)
	if err != nil {
		return err
	}
	defer release()
	o, found, err := u.repo.GetOrder(ctx, snapshot.MarketID, snapshot.OrderID)
	if err != nil || !found || !o.MatchPending ||
		(o.Status != data.StatusOpen && o.Status != data.StatusPartial) {
		return err
	}
	if err := u.match(ctx, o); err != nil {
		return err
	}
	if o.Remaining() > 0 {
		u.addBookCache(ctx, o)
	} else {
		u.removeBookCache(ctx, o)
		u.tryReleaseOrder(ctx, o.MarketID, o.OrderID)
	}
	u.pushAudit(ctx, toProtoOrder(o))
	return nil
}

// ReconcilePendingSideEffects 补齐事务提交后未完成的账本副作用。可由多实例并发调用：
// Settle(match_id)、Release(order_id) 都幂等，Complete/Clear 又是条件更新，因此不会重复转资。
func (u *AuctionUsecase) ReconcilePendingSideEffects(ctx context.Context) (settled, released int, retErr error) {
	if u.cfg.PassiveWarmup {
		return 0, 0, errcode.New(errcode.ErrUnavailable, "auction is in passive warmup")
	}
	batch := u.cfg.SideEffectReconcileBatch
	if batch <= 0 {
		batch = 100
	}
	// 模式 C:本函数是周期性 reconcile,下面 5 个循环原先对**每个失败元素**各打一条 Error。
	// inventory / MySQL 故障时每分片每轮最多 batch(默认 100)条全失败,且每轮对同一批 backlog
	// 重打 → Error 洪泛。改为按阶段累加、轮末汇总一条:既给出各阶段失败分布(比逐条更能
	// 判断是哪一环卡住),又不随 backlog 规模刷屏。retErr / defer 重试等行为一字不动。
	var releaseFailed, recoverPendingFailed, recoverUnverifiedFailed, recoverMatchPendingFailed int
	var sampleOrderID uint64
	noteOrderFail := func(orderID uint64) {
		if sampleOrderID == 0 {
			sampleOrderID = orderID
		}
	}
	defer func() {
		total := releaseFailed + recoverPendingFailed + recoverUnverifiedFailed + recoverMatchPendingFailed
		if total == 0 {
			return
		}
		plog.With(ctx).Errorw("msg", "auction_side_effect_reconcile_failed",
			"batch", batch, "failed_total", total,
			"release_failed", releaseFailed,
			"recover_pending_failed", recoverPendingFailed,
			"recover_unverified_failed", recoverUnverifiedFailed,
			"recover_match_pending_failed", recoverMatchPendingFailed,
			"sample_order_id", sampleOrderID, "first_err", retErr)
	}()
	terminalRepairs, err := u.repo.ListTerminalOrdersForRepair(ctx, batch)
	if err != nil {
		return 0, 0, err
	}
	for _, o := range terminalRepairs {
		changed, repairErr := u.repo.RepairTerminalMarkers(ctx, o.MarketID, o.OrderID)
		if repairErr != nil {
			if retErr == nil {
				retErr = repairErr
			}
			continue
		}
		if changed {
			u.releaseOwnerSlot(ctx, o)
		}
	}
	// 已就绪的终态 escrow 优先释放。后面的 legacy/冻结恢复和 Settle 每条都可能等待外部 inventory；
	// 若把本扫描放在末尾，大批慢请求会让本可立即归还的资产延迟数分钟。新完成的 match 仍在
	// settle 循环内就地释放；失败 marker 留给下一轮的这个优先 pass。
	orders, err := u.repo.ListReleasableOrders(ctx, batch)
	if err != nil {
		return 0, 0, err
	}
	for _, o := range orders {
		ok, rerr := u.releaseOrder(ctx, o.MarketID, o.OrderID)
		if rerr != nil {
			releaseFailed++
			noteOrderFail(o.OrderID)
			if retErr == nil {
				retErr = rerr
			}
		} else if ok {
			released++
		}
	}
	pendingOrders, err := u.repo.ListPendingOrders(ctx, batch)
	if err != nil {
		return 0, 0, err
	}
	for _, o := range pendingOrders {
		if rerr := u.recoverPendingOrder(ctx, o); rerr != nil {
			u.deferOrderReconcile(ctx, o.MarketID, o.OrderID)
			recoverPendingFailed++
			noteOrderFail(o.OrderID)
			if retErr == nil {
				retErr = rerr
			}
		}
	}
	unverifiedOrders, err := u.repo.ListUnverifiedActiveOrders(ctx, batch)
	if err != nil {
		return 0, 0, err
	}
	for _, o := range unverifiedOrders {
		if rerr := u.recoverUnverifiedActiveOrder(ctx, o); rerr != nil {
			u.deferOrderReconcile(ctx, o.MarketID, o.OrderID)
			recoverUnverifiedFailed++
			noteOrderFail(o.OrderID)
			if retErr == nil {
				retErr = rerr
			}
		}
	}
	matchPendingOrders, err := u.repo.ListMatchPendingOrders(ctx, batch)
	if err != nil {
		return 0, 0, err
	}
	for _, o := range matchPendingOrders {
		if rerr := u.recoverMatchPendingOrder(ctx, o); rerr != nil {
			u.deferOrderReconcile(ctx, o.MarketID, o.OrderID)
			recoverMatchPendingFailed++
			noteOrderFail(o.OrderID)
			if retErr == nil {
				retErr = rerr
			}
		}
	}
	matches, err := u.repo.ListPendingMatches(ctx, batch)
	if err != nil {
		return 0, 0, err
	}
	for _, m := range matches {
		if u.settleMatch(ctx, m) {
			settled++
			for _, orderID := range []uint64{m.SellOrderID, m.BuyOrderID} {
				ok, rerr := u.releaseOrder(ctx, m.MarketID, orderID)
				if rerr != nil {
					releaseFailed++
					noteOrderFail(orderID)
					if retErr == nil {
						retErr = rerr
					}
				} else if ok {
					released++
				}
			}
		} else if retErr == nil {
			retErr = errcode.New(errcode.ErrInternal, "match %d side effect remains pending", m.MatchID)
		}
	}
	return settled, released, retErr
}

// ReconcilePendingMatchEvents 独立消费成交 outbox。它不能与资产补偿共用 goroutine：
// kafkax/Sarama 的同步 Send 无法被调用 context 可靠中断，broker 故障可能阻塞很久；独立 worker
// 保证该阻塞不会延迟下一轮 Settle/Release/legacy 恢复。repo 还以 release_pending 做持久屏障：
// 关联终态订单的 escrow 未释放前，成交事件不可见；兼容旧 default=1，活跃/部分订单不受影响。
func (u *AuctionUsecase) ReconcilePendingMatchEvents(ctx context.Context) (published int, retErr error) {
	if u.cfg.PassiveWarmup {
		return 0, errcode.New(errcode.ErrUnavailable, "auction is in passive warmup")
	}
	batch := u.cfg.SideEffectReconcileBatch
	if batch <= 0 {
		batch = 100
	}
	events, err := u.repo.ListPendingMatchEvents(ctx, batch)
	if err != nil {
		return 0, err
	}
	for _, m := range events {
		if u.publishMatchEvent(ctx, m) {
			published++
		} else if retErr == nil {
			retErr = errcode.New(errcode.ErrInternal, "match %d event remains pending", m.MatchID)
		}
	}
	return published, retErr
}

func nowMs() int64 { return time.Now().UnixMilli() }
