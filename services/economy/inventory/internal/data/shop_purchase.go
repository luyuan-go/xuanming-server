// shop_purchase.go — NPC 商店购买数据层(2026-08-22)。
//
// 一次购买在**一个本地事务**里完成三件事,同成同败(§9.7 交易资源扣减必须原子 + 幂等键):
//
//	① 声明幂等流水(inventory_ledger,op=shop_buy);
//	② 按币种扣货币(FOR UPDATE 锁行 → 比较 → 写绝对值,余额不足整笔回滚);
//	③ 入包:可堆叠道具累加 player_items;装备类每份生成一件独立未鉴定实例。
//
// 为什么装备也走这里而不是复用 GrantInstances:GrantInstances 是**系统发放**接口,
// 自己开事务、自己写 ledger。若购买先扣钱再调它,扣钱与发货就落在两个事务里 ——
// 中间崩溃会出现"钱扣了货没到"且无补偿。购买必须是一个事务,所以在这里内联实例分配逻辑
// (复用同一套 lockPlayerInstances / lowestFreeSlot,不另起一套格子分配规则)。
package data

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// PurchaseRequest 是 biz 层算好的一次购买(价格已按商店表定价并过溢出闸)。
//
// 注意这里**没有** UnitPrice:单价只在 biz 用于算 TotalCost,data 层只认总价,
// 免得两层各存一份价格再漂移。
type PurchaseRequest struct {
	ShopID       uint32
	ItemConfigID uint32
	UnitCount    uint32 // 购买份数
	TotalItems   int64  // 可堆叠道具的总发放数量(= 份数 × 每份数量);装备类为 0
	Kind         CurrencyKind
	TotalCost    uint64
	IsEquipment  bool
	// InstanceIDs 是装备类购买预生成的雪花 ID(由 biz 用 snowflake 批量预留),
	// 长度必须等于要发的件数;可堆叠购买为空。
	InstanceIDs    []uint64
	Capacity       int32
	IdempotencyKey string
}

// PurchaseOutcome 是一次购买的结果(新执行或幂等回放共用同一形状)。
type PurchaseOutcome struct {
	Balances  Balances // 扣费后玩家全部币种余额
	Cost      uint64   // 本次实际扣费(回放时来自 ledger delta 列)
	Kind      CurrencyKind
	Items     []ItemGrant    // 实际入包的可堆叠道具
	Instances []ItemInstance // 实际入包的装备实例
}

// PurchaseFingerprint 计算购买请求指纹。
//
// **刻意不含价格**:价格是服务端热更配置。若把价格编进指纹,首次响应丢失后策划改了价,
// 客户端拿同一个 key 重试就会被判 ErrInventoryIdempotencyConflict —— 那是"同键不同请求"
// 的反作弊信号,用它来报"价格变了"会让真正的串账淹没在噪声里。
// 玩家意图 = (哪个商店, 买哪个, 买几份),这三项一致就是同一次购买。
// 首次执行的实际扣费从 ledger 的 delta 列回放,所以改价也不会让重放金额漂移。
func PurchaseFingerprint(shopID, itemConfigID, unitCount uint32) string {
	return hashHex(fmt.Sprintf("shop_buy|shop=%d|item=%d|units=%d", shopID, itemConfigID, unitCount))
}

// purchaseDetail 编码本次购买的可回放事实。
//
// detail 在本仓一般是"人读审计摘要",但 grant_inst 早已用它承载 instance_id 列表
// (GrantInstances 的回放就靠它)。购买同样需要回放"到底发了什么",且这些事实
// 无法从请求重算(每份数量是热更配置,改表后重算会得到与首次执行不同的数量)。
// 因此沿用同一惯例,并保持格式可读:
//
//	shop_buy shop=1 item=10001 units=3 count=30 inst=
//	shop_buy shop=1 item=10156 units=1 count=0 inst=123,124
func purchaseDetail(req PurchaseRequest) string {
	ids := make([]string, 0, len(req.InstanceIDs))
	for _, id := range req.InstanceIDs {
		ids = append(ids, fmt.Sprintf("%d", id))
	}
	return fmt.Sprintf("shop_buy shop=%d item=%d units=%d count=%d inst=%s",
		req.ShopID, req.ItemConfigID, req.UnitCount, req.TotalItems, strings.Join(ids, ","))
}

// parsePurchaseDetail 从流水 detail 还原首次执行发放的事实。
// 解析失败返回 ok=false;调用方据此 fail-closed(宁可报内部错,也不能凭空编造发货结果)。
func parsePurchaseDetail(detail string) (totalItems int64, instanceIDs []uint64, ok bool) {
	var shopID, itemID, units uint32
	var count int64
	var instPart string
	// Sscanf 的 %s 会吃掉到空白为止;inst= 是最后一段,空列表时该字段缺失,
	// 所以先按前缀切,再单独取尾巴,避免"空 inst 导致整条解析失败"。
	const prefix = "shop_buy shop="
	if !strings.HasPrefix(detail, prefix) {
		return 0, nil, false
	}
	idx := strings.Index(detail, " inst=")
	if idx < 0 {
		return 0, nil, false
	}
	head, instPart := detail[:idx], detail[idx+len(" inst="):]
	if n, err := fmt.Sscanf(head, "shop_buy shop=%d item=%d units=%d count=%d", &shopID, &itemID, &units, &count); err != nil || n != 4 {
		return 0, nil, false
	}
	if instPart != "" {
		for _, s := range strings.Split(instPart, ",") {
			var id uint64
			if n, err := fmt.Sscanf(s, "%d", &id); err != nil || n != 1 || id == 0 {
				return 0, nil, false
			}
			instanceIDs = append(instanceIDs, id)
		}
	}
	return count, instanceIDs, true
}

func (r *MySQLInventoryRepo) PurchaseShopItem(ctx context.Context, playerID uint64, req PurchaseRequest) (PurchaseOutcome, bool, error) {
	if err := ValidateCurrencyKind(req.Kind); err != nil {
		return PurchaseOutcome{}, false, err
	}
	if req.TotalCost == 0 {
		// 商店表要求单价 > 0(shop.proto),份数也已在 biz 校验 > 0,所以总价为 0 只可能是
		// 上游算错。免费发放请走活动 / 邮件,不要借商店链绕过发放审计。
		return PurchaseOutcome{}, false, errcode.New(errcode.ErrInvalidArg,
			"purchase total cost must be positive player=%d shop=%d item=%d", playerID, req.ShopID, req.ItemConfigID)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return PurchaseOutcome{}, false, errcode.New(errcode.ErrInternal, "begin purchase tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	fp := PurchaseFingerprint(req.ShopID, req.ItemConfigID, req.UnitCount)
	detail := purchaseDetail(req)

	already, snap, storedDetail, lerr := claimPurchaseLedger(ctx, tx, playerID, req.IdempotencyKey, fp, detail)
	if lerr != nil {
		return PurchaseOutcome{}, false, lerr
	}
	if already {
		out, rerr := r.replayPurchase(ctx, tx, playerID, req, snap, storedDetail)
		if rerr != nil {
			return PurchaseOutcome{}, false, rerr
		}
		if cerr := tx.Commit(); cerr != nil {
			return PurchaseOutcome{}, false, errcode.New(errcode.ErrInternal, "commit purchase replay player=%d: %v", playerID, cerr)
		}
		return out, true, nil
	}

	// ① 扣钱。放在入包之前:余额不足是最常见的失败,先扣可以少做无用功;
	//    且两者同事务,顺序不影响原子性。
	if _, derr := deductCurrencyTx(ctx, tx, playerID, req.Kind, req.TotalCost); derr != nil {
		return PurchaseOutcome{}, false, derr
	}

	// ② 入包。
	var outItems []ItemGrant
	var outInstances []ItemInstance
	if req.IsEquipment {
		insts, ierr := grantInstancesInTx(ctx, tx, playerID, req.InstanceIDs, req.ItemConfigID, req.Capacity)
		if ierr != nil {
			return PurchaseOutcome{}, false, ierr
		}
		outInstances = insts
	} else {
		if req.TotalItems <= 0 {
			return PurchaseOutcome{}, false, errcode.New(errcode.ErrInvalidArg,
				"purchase item count must be positive player=%d item=%d", playerID, req.ItemConfigID)
		}
		if aerr := addItemTx(ctx, tx, playerID, req.ItemConfigID, req.TotalItems); aerr != nil {
			return PurchaseOutcome{}, false, aerr
		}
		outItems = []ItemGrant{{ItemConfigID: req.ItemConfigID, Count: req.TotalItems}}
	}

	// ③ 结果快照落流水(余额 + 本次扣费额),供响应丢失后的重放返回同样的值。
	newBalances, rerr := readBalancesTx(ctx, tx, playerID)
	if rerr != nil {
		return PurchaseOutcome{}, false, rerr
	}
	delta := Balances{req.Kind: req.TotalCost}
	if uerr := updateLedgerResult(ctx, tx, playerID, req.IdempotencyKey, 0, newBalances, delta); uerr != nil {
		return PurchaseOutcome{}, false, uerr
	}

	if cerr := tx.Commit(); cerr != nil {
		return PurchaseOutcome{}, false, errcode.New(errcode.ErrInternal,
			"commit purchase player=%d shop=%d item=%d: %v", playerID, req.ShopID, req.ItemConfigID, cerr)
	}
	return PurchaseOutcome{
		Balances:  newBalances,
		Cost:      req.TotalCost,
		Kind:      req.Kind,
		Items:     outItems,
		Instances: outInstances,
	}, false, nil
}

// claimPurchaseLedger 声明购买幂等键,并在命中时返回首次执行的结果快照与 detail。
func claimPurchaseLedger(ctx context.Context, tx *sql.Tx, playerID uint64, idempotencyKey, fingerprint, detail string) (already bool, snap LedgerSnapshot, storedDetail string, err error) {
	const ins = `INSERT INTO inventory_ledger (player_id, idempotency_key, op, request_fingerprint, detail) VALUES (?, ?, 'shop_buy', ?, ?)`
	if _, lerr := tx.ExecContext(ctx, ins, playerID, idempotencyKey, fingerprint, detail); lerr == nil {
		return false, LedgerSnapshot{}, "", nil
	} else if !isDupErr(lerr) {
		return false, LedgerSnapshot{}, "", errcode.New(errcode.ErrInternal,
			"insert purchase ledger player=%d key=%s: %v", playerID, idempotencyKey, lerr)
	}

	var storedOp, storedFP string
	var remaining, legacyGold int64
	var rawBalances, rawDelta []byte
	if qerr := tx.QueryRowContext(ctx,
		`SELECT op, request_fingerprint, detail, `+ledgerResultColumns+`
FROM inventory_ledger WHERE player_id = ? AND idempotency_key = ? LIMIT 1`,
		playerID, idempotencyKey).
		Scan(&storedOp, &storedFP, &storedDetail, &remaining, &legacyGold, &rawBalances, &rawDelta); qerr != nil {
		return false, LedgerSnapshot{}, "", errcode.New(errcode.ErrInternal,
			"read purchase ledger player=%d key=%s: %v", playerID, idempotencyKey, qerr)
	}
	// op 也要核:同一个 key 被出售 / 发放用过再拿来购买,是明确的 key 复用。
	if storedOp != "shop_buy" || storedFP != fingerprint {
		return false, LedgerSnapshot{}, "", errcode.New(errcode.ErrInventoryIdempotencyConflict,
			"idempotency_key reused for different purchase player=%d key=%s", playerID, idempotencyKey)
	}
	s, serr := scanLedgerResult(remaining, legacyGold, rawBalances, rawDelta)
	if serr != nil {
		return false, LedgerSnapshot{}, "", serr
	}
	return true, s, storedDetail, nil
}

// replayPurchase 用首次执行落下的事实重建响应,**不重新扣费也不重新发货**。
func (r *MySQLInventoryRepo) replayPurchase(ctx context.Context, tx *sql.Tx, playerID uint64, req PurchaseRequest, snap LedgerSnapshot, storedDetail string) (PurchaseOutcome, error) {
	totalItems, instanceIDs, ok := parsePurchaseDetail(storedDetail)
	if !ok {
		// 解析不出首次发货事实就 fail-closed:宁可让客户端看到内部错并由人排查,
		// 也不能编一个"发了什么"回去(那会让 UI 显示玩家其实没拿到的东西)。
		return PurchaseOutcome{}, errcode.New(errcode.ErrInternal,
			"purchase ledger detail unparsable player=%d key=%s", playerID, req.IdempotencyKey)
	}
	// 同出售:用 Sole 回放首次执行的币种与扣费额,不受商店表改币种影响。
	costKind, cost, ok := snap.Delta.Sole()
	if !ok {
		costKind, cost = req.Kind, 0
	}
	out := PurchaseOutcome{
		Balances: snap.Balances,
		Cost:     cost,
		Kind:     costKind,
	}
	if len(instanceIDs) > 0 {
		insts, ierr := selectInstancesByIDsTx(ctx, tx, playerID, instanceIDs)
		if ierr != nil {
			return PurchaseOutcome{}, ierr
		}
		out.Instances = insts
	}
	if totalItems > 0 {
		out.Items = []ItemGrant{{ItemConfigID: req.ItemConfigID, Count: totalItems}}
	}
	return out, nil
}
