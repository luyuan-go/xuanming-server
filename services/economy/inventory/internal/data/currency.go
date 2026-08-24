// currency.go — 多币种货币数据层原语(2026-08-22)。
//
// 库表:player_wallet(player_id, currency_kind) → amount BIGINT UNSIGNED。
// 一个玩家每种货币至多一行;没有行 = 该币种余额为 0(不预建行,不占空间)。
//
// # 为什么金额用无符号,以及无符号带来的三条硬纪律
//
// CLAUDE.md §5.12:语义上不可能为负的整数默认无符号。货币余额恒 >= 0,因此是 uint64。
// 但"存储无符号"会把原本静默的 bug 变成新形态,必须在**应用层**堵死,不能指望 SQL:
//
//	① 减法下溢:`UPDATE ... SET amount = amount - ?` 在 UNSIGNED 列上,严格模式抛
//	   Error 1690(out of range),非严格模式**截断成 0**——后者等于把"扣款失败"
//	   变成"余额清零",是不可接受的静默数据损坏。因此本文件一律
//	   **先 FOR UPDATE 锁行读出余额、在 Go 里比较、再写绝对值**,SQL 里不出现 `amount - ?`。
//	② 加法溢出:同理会在列上溢出。无符号回绕会让首富瞬间变零元户,所以设
//	   MaxCurrencyAmount 硬上限,越界返回 ErrInventoryCurrencyOverflow 而不是回绕。
//	③ 乘法溢出:单价 × 数量在算出总价的那一步就可能溢出,必须用 safeMulUint64,
//	   不能等到写库才发现。
//
// 这三条不是"防御性编程",而是把 §5.12 允许无符号的前提(不参与可能下溢的减法)
// 真正落实:减法只在锁内、比较之后发生,所以永远不会下溢。
//
// # 防死锁
//
// 与本包既有纪律一致:跨玩家的行锁按 player_id 升序获取;同一玩家的多币种行锁按
// currency_kind 升序获取。P2P 交易 / 拍卖结算会同时锁买卖双方,顺序错会互等。
package data

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/errcode"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
)

// CurrencyKind 是货币类型(直接复用 proto 枚举,不另立平行类型 —— §5.8 禁止与 proto 重复的并行 struct)。
type CurrencyKind = commonv1.CurrencyKind

// 常用币种简写(避免每处都写长枚举名)。
const (
	CurrencyGold    = commonv1.CurrencyKind_CURRENCY_KIND_GOLD
	CurrencyDiamond = commonv1.CurrencyKind_CURRENCY_KIND_DIAMOND
	CurrencyHonor   = commonv1.CurrencyKind_CURRENCY_KIND_HONOR
)

// MaxCurrencyAmount 是**单玩家单币种**余额硬上限。
//
// 取 2^62 而不是 MaxUint64:留出 2 位余量,使"余额 + 单笔入账"这类中间量在
// uint64 里永远算得下,不必在每个加法点再判一次中间溢出。
// 4.6e18 远超任何合理经济体量;真触顶只可能是发放逻辑失控或有人直写库,
// 此时拒绝入账(ErrInventoryCurrencyOverflow)比静默回绕正确。
const MaxCurrencyAmount uint64 = 1 << 62

// currencyBalancesMaxBytes 是 inventory_ledger.result_currencies 的写入侧字节闸(§9.24 深度上限)。
// 币种数是枚举有界的(个位数),单条 CurrencyAmount 编码 ≤ 12 字节;
// 取列容量 VARBINARY(256),达到即说明枚举失控或有人直写。
const currencyBalancesMaxBytes = 256

// Balances 是玩家的多币种余额视图(只含非零项)。
//
// 用 map 而不是 slice:调用方绝大多数是"按 kind 取一个值";
// 需要稳定顺序时用 Sorted() 转成按 kind 升序的切片(落库 / 上行协议都要求有序)。
type Balances map[CurrencyKind]uint64

// Get 取某币种余额(不存在 = 0)。
func (b Balances) Get(kind CurrencyKind) uint64 {
	if b == nil {
		return 0
	}
	return b[kind]
}

// Sorted 转成按 kind 升序的切片,只含非零项。
// 顺序必须稳定:它同时决定落库 pb 字节与下行协议顺序,不稳定会让幂等指纹漂移。
func (b Balances) Sorted() []*commonv1.CurrencyAmount {
	if len(b) == 0 {
		return nil
	}
	kinds := make([]CurrencyKind, 0, len(b))
	for k, v := range b {
		if v == 0 {
			continue // 零余额不外露:与"没有这一行"语义等价,避免下行出现一堆 0
		}
		kinds = append(kinds, k)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })
	out := make([]*commonv1.CurrencyAmount, 0, len(kinds))
	for _, k := range kinds {
		out = append(out, &commonv1.CurrencyAmount{Kind: k, Amount: b[k]})
	}
	return out
}

// Sole 取"只有一项"时的那一项(kind, amount)。零项或多于一项返回 ok=false。
//
// 用途:出售 / 购买的幂等重放要回放"当初那一笔是多少钱"。若按**当前配置的币种**去
// delta 里查,而首次执行后管理员改过 `currency.sell_kind`,就会查不到而回 0 ——
// 玩家重试一次就看到"获得 0 金币"。这些操作按设计只动一个币种,所以直接取那唯一一项
// 才是忠实回放。多于一项说明调用方用错了(那是 Grant 那类多币种操作),显式 fail 而不是猜。
func (b Balances) Sole() (CurrencyKind, uint64, bool) {
	var (
		kind  CurrencyKind
		total uint64
		count int
	)
	for k, v := range b {
		if v == 0 {
			continue
		}
		kind, total = k, v
		count++
	}
	if count != 1 {
		return 0, 0, false
	}
	return kind, total, true
}

// ValidateCurrencyKind 校验币种合法。
//
// UNSPECIFIED 与未知值一律拒,**绝不回退成金币**:静默回退会让配错表的商品
// 按金币扣钱,是不可观测的经济事故(fail-closed,§9.22)。
func ValidateCurrencyKind(kind CurrencyKind) error {
	switch kind {
	case CurrencyGold, CurrencyDiamond, CurrencyHonor:
		return nil
	default:
		return errcode.New(errcode.ErrInvalidArg, "unsupported currency kind: %d", int32(kind))
	}
}

// safeMulUint64 溢出安全乘法。ok=false 表示乘积超出 uint64。
func safeMulUint64(a, b uint64) (uint64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	if a > MaxCurrencyAmount/b {
		return 0, false
	}
	return a * b, true
}

// safeAddUint64 溢出安全加法(上界是 MaxCurrencyAmount 而不是 MaxUint64)。
func safeAddUint64(a, b uint64) (uint64, bool) {
	if a > MaxCurrencyAmount-b {
		return 0, false
	}
	return a + b, true
}

// SafeMulCurrency 是给 biz 层用的溢出安全乘法(单价 × 数量 → 总价)。
func SafeMulCurrency(unitPrice, count uint64) (uint64, bool) { return safeMulUint64(unitPrice, count) }

// EncodeBalances 把余额编成 pb 二进制,写 inventory_ledger.result_currencies。
// 空余额 → nil(列写 NULL),不写一个空 message,省字节也便于肉眼区分"没记"与"记了全零"。
func EncodeBalances(b Balances) ([]byte, error) {
	sorted := b.Sorted()
	if len(sorted) == 0 {
		return nil, nil
	}
	raw, err := proto.Marshal(&commonv1.CurrencyBalancesStorageRecord{Balances: sorted})
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "marshal currency balances: %v", err)
	}
	if len(raw) > currencyBalancesMaxBytes {
		// 写入侧字节闸:非严格 sql_mode 下超长会被静默截断成半截 pb,
		// 重放时解不出来 → 幂等结果凭空变形。宁可拒写。
		return nil, errcode.New(errcode.ErrInternal,
			"currency balances payload too large: %d > %d bytes", len(raw), currencyBalancesMaxBytes)
	}
	return raw, nil
}

// DecodeBalances 从 inventory_ledger.result_currencies 还原余额快照。
// NULL / 空 → 空余额(不是错误:老流水行或纯道具操作本就没有货币结果)。
func DecodeBalances(raw []byte) (Balances, error) {
	if len(raw) == 0 {
		return Balances{}, nil
	}
	var rec commonv1.CurrencyBalancesStorageRecord
	if err := proto.Unmarshal(raw, &rec); err != nil {
		return nil, errcode.New(errcode.ErrInternal, "unmarshal currency balances: %v", err)
	}
	out := make(Balances, len(rec.GetBalances()))
	for _, it := range rec.GetBalances() {
		if it.GetAmount() == 0 {
			continue
		}
		out[it.GetKind()] = it.GetAmount()
	}
	return out, nil
}

// readBalancesTx 在事务里读某玩家全部币种余额(无行 → 空)。
func readBalancesTx(ctx context.Context, tx *sql.Tx, playerID uint64) (Balances, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT currency_kind, amount FROM player_wallet WHERE player_id = ? ORDER BY currency_kind`, playerID)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "read balances player=%d: %v", playerID, err)
	}
	defer rows.Close()

	out := Balances{}
	for rows.Next() {
		var kind int32
		var amount uint64
		if serr := rows.Scan(&kind, &amount); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan balance player=%d: %v", playerID, serr)
		}
		if amount == 0 {
			continue
		}
		out[CurrencyKind(kind)] = amount
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate balances player=%d: %v", playerID, rerr)
	}
	return out, nil
}

// readBalances 是 readBalancesTx 的非事务版本(只读路径 GetInventory 用)。
func (r *MySQLInventoryRepo) readBalances(ctx context.Context, playerID uint64) (Balances, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT currency_kind, amount FROM player_wallet WHERE player_id = ? ORDER BY currency_kind`, playerID)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "read balances player=%d: %v", playerID, err)
	}
	defer rows.Close()

	out := Balances{}
	for rows.Next() {
		var kind int32
		var amount uint64
		if serr := rows.Scan(&kind, &amount); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan balance player=%d: %v", playerID, serr)
		}
		if amount == 0 {
			continue
		}
		out[CurrencyKind(kind)] = amount
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate balances player=%d: %v", playerID, rerr)
	}
	return out, nil
}

// lockBalanceTx 锁定并读取某玩家某币种的余额行。
//
// 先做一次 no-op upsert 把行**物化**再锁:UNSIGNED 列上不能靠 `amount = amount + ?`,
// 必须锁行读出来在 Go 里算;而 `SELECT ... FOR UPDATE` 对不存在的行不加行锁
// (TiDB 无 gap 锁,MySQL 也只加间隙锁),两个并发事务会双双看到"无行"然后各自 INSERT,
// 一个拿到 duplicate key 而不是被串行化。no-op upsert 让两者在同一行上排队。
func lockBalanceTx(ctx context.Context, tx *sql.Tx, playerID uint64, kind CurrencyKind) (uint64, error) {
	const materialize = `INSERT INTO player_wallet (player_id, currency_kind, amount) VALUES (?, ?, 0)
ON DUPLICATE KEY UPDATE player_id = player_id`
	if _, err := tx.ExecContext(ctx, materialize, playerID, int32(kind)); err != nil {
		return 0, errcode.New(errcode.ErrInternal,
			"materialize currency row player=%d kind=%d: %v", playerID, int32(kind), err)
	}

	var have uint64
	err := tx.QueryRowContext(ctx,
		`SELECT amount FROM player_wallet WHERE player_id = ? AND currency_kind = ? FOR UPDATE`,
		playerID, int32(kind)).Scan(&have)
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal,
			"lock currency player=%d kind=%d: %v", playerID, int32(kind), err)
	}
	return have, nil
}

// writeBalanceTx 写绝对余额(调用方已在锁内算好,SQL 里不做算术)。
func writeBalanceTx(ctx context.Context, tx *sql.Tx, playerID uint64, kind CurrencyKind, amount uint64) error {
	if _, err := tx.ExecContext(ctx,
		`UPDATE player_wallet SET amount = ? WHERE player_id = ? AND currency_kind = ?`,
		amount, playerID, int32(kind)); err != nil {
		return errcode.New(errcode.ErrInternal,
			"write currency player=%d kind=%d: %v", playerID, int32(kind), err)
	}
	return nil
}

// addCurrencyTx 在事务里给玩家某币种加钱,返回加后余额。
//
// n == 0 时不写库也不锁行(纯道具发放的常见情形),直接读回当前值。
// 越过 MaxCurrencyAmount → ErrInventoryCurrencyOverflow(拒绝入账,不回绕)。
func addCurrencyTx(ctx context.Context, tx *sql.Tx, playerID uint64, kind CurrencyKind, n uint64) (uint64, error) {
	if err := ValidateCurrencyKind(kind); err != nil {
		return 0, err
	}
	have, err := lockBalanceTx(ctx, tx, playerID, kind)
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return have, nil
	}
	next, ok := safeAddUint64(have, n)
	if !ok {
		return 0, errcode.New(errcode.ErrInventoryCurrencyOverflow,
			"currency overflow player=%d kind=%d have=%d add=%d max=%d",
			playerID, int32(kind), have, n, MaxCurrencyAmount)
	}
	if werr := writeBalanceTx(ctx, tx, playerID, kind, next); werr != nil {
		return 0, werr
	}
	return next, nil
}

// deductCurrencyTx 在事务里扣某币种,返回扣后余额。
//
// 余额不足 → ErrInventoryInsufficient(**先比较后相减**,减法永远不下溢)。
func deductCurrencyTx(ctx context.Context, tx *sql.Tx, playerID uint64, kind CurrencyKind, n uint64) (uint64, error) {
	if err := ValidateCurrencyKind(kind); err != nil {
		return 0, err
	}
	have, err := lockBalanceTx(ctx, tx, playerID, kind)
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return have, nil
	}
	if have < n {
		return 0, errcode.New(errcode.ErrInventoryInsufficient,
			"insufficient currency player=%d kind=%d need=%d have=%d", playerID, int32(kind), n, have)
	}
	next := have - n
	if werr := writeBalanceTx(ctx, tx, playerID, kind, next); werr != nil {
		return 0, werr
	}
	return next, nil
}

// applyCurrencyDeltasTx 按 kind 升序批量加钱(多币种发放)。返回加后的全量余额快照。
//
// 按 kind 升序是防死锁纪律的一部分:同一玩家的多币种行锁必须固定顺序获取。
func applyCurrencyDeltasTx(ctx context.Context, tx *sql.Tx, playerID uint64, deltas Balances) (Balances, error) {
	kinds := make([]CurrencyKind, 0, len(deltas))
	for k, v := range deltas {
		if v == 0 {
			continue
		}
		kinds = append(kinds, k)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })
	for _, k := range kinds {
		if _, err := addCurrencyTx(ctx, tx, playerID, k, deltas[k]); err != nil {
			return nil, err
		}
	}
	return readBalancesTx(ctx, tx, playerID)
}

// describeBalances 生成人读摘要(写进 ledger.detail,审计用,非业务字段)。
func describeBalances(b Balances) string {
	sorted := b.Sorted()
	if len(sorted) == 0 {
		return "none"
	}
	out := ""
	for i, it := range sorted {
		if i > 0 {
			out += ","
		}
		out += fmt.Sprintf("%d:%d", int32(it.GetKind()), it.GetAmount())
	}
	return out
}
