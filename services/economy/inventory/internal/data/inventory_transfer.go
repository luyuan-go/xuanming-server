// inventory_transfer.go — 邮件 transfer 附件实例托管数据层(2026-07-22,bag-domain.md §7.1)。
//
// 库表:mail_transfer_escrow(deploy/mysql-init/08-inventory-tables.sql)。
// "同一 instance 全局唯一"不变量:player_item_instance 与 mail_transfer_escrow 各自以
// instance_id 为 PK,行只能经同一 MySQL 事务在两表间 INSERT ... SELECT + DELETE 搬移,
// 任一时刻实例恰存在于一处;搬移用 INSERT ... SELECT 保证鉴定态/词条/绑定逐字节原样
// (零重铸零重 roll,不经 Go 解码回写)。
//
// 幂等(不变量 §9.7):
//   - EscrowOutInstances / ClaimTransferInstances 复用 inventory_ledger
//     (op=escrow_out / transfer_claim,指纹防 key 复用串改账);
//   - ReleaseTransferEscrow 幂等由托管行存在性承担:行已被领取/已释放 → no-op
//     (行只能被删一次,且每条删除路径在同事务里恰入账一个玩家,资产只落一处)。
package data

import (
	"context"
	"database/sql"
	"sort"
	"strconv"
	"strings"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// TransferClaimItem 领取核对项:InstanceID 定位托管行,ItemConfigID 交叉核对
// (与托管行不符 = 附件漂移 / 伪造,整批拒)。
type TransferClaimItem struct {
	InstanceID   uint64
	ItemConfigID uint32
}

// EscrowedInstance 托管行快照(EscrowOutInstances 返回,调用方装 TransferAttachment.item)。
type EscrowedInstance struct {
	InstanceID     uint64
	ItemConfigID   uint32
	Identified     bool
	Attributes     []ItemAttribute
	SourcePlayerID uint64
	ToPlayerID     uint64
}

// EscrowOutFingerprint 计算托管扣出请求指纹(to_player + instance_ids 排序规范化)。
func EscrowOutFingerprint(toPlayerID uint64, instanceIDs []uint64) string {
	sorted := append([]uint64(nil), instanceIDs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var b strings.Builder
	b.WriteString("escrow_out|to=")
	b.WriteString(strconv.FormatUint(toPlayerID, 10))
	for _, id := range sorted {
		b.WriteByte('|')
		b.WriteString(strconv.FormatUint(id, 10))
	}
	return hashHex(b.String())
}

// TransferClaimFingerprint 计算托管领取请求指纹(instance:config 对排序规范化)。
func TransferClaimFingerprint(items []TransferClaimItem) string {
	sorted := append([]TransferClaimItem(nil), items...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].InstanceID < sorted[j].InstanceID })
	var b strings.Builder
	b.WriteString("transfer_claim")
	for _, it := range sorted {
		b.WriteByte('|')
		b.WriteString(strconv.FormatUint(it.InstanceID, 10))
		b.WriteByte(':')
		b.WriteString(strconv.FormatUint(uint64(it.ItemConfigID), 10))
	}
	return hashHex(b.String())
}

// sortedIDs 返回升序副本(统一加锁顺序,防两笔并发操作交叉锁行死锁)。
func sortedIDs(ids []uint64) []uint64 {
	out := append([]uint64(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

const escrowCols = `instance_id, item_config_id, identified, attributes, source_player_id, to_player_id`

// scanEscrowRow 从一行 SELECT 结果扫出 EscrowedInstance(attributes pb 二进制兜底)。
func scanEscrowRow(scan func(dest ...any) error) (EscrowedInstance, error) {
	var (
		row        EscrowedInstance
		identified int8
		attrsRaw   []byte
	)
	if err := scan(&row.InstanceID, &row.ItemConfigID, &identified, &attrsRaw, &row.SourcePlayerID, &row.ToPlayerID); err != nil {
		return EscrowedInstance{}, err
	}
	row.Identified = identified != 0
	attrs, derr := decodeInstanceAttrs(attrsRaw)
	if derr != nil {
		return EscrowedInstance{}, errcode.New(errcode.ErrInternal, "decode escrow attrs id=%d: %v", row.InstanceID, derr)
	}
	row.Attributes = attrs
	return row, nil
}

// EscrowOutInstances 从源玩家原子扣出实例并托管(同一事务;bound 实例拒)。
// 幂等回放:命中 uk 且指纹一致 → 按 id 读回托管行(行必在:托管行只能被"已发出邮件的
// 领取"或"调用方补偿释放"消灭,两者都以调用方已观察到本调用成功为前提;重试只发生在
// 响应丢失、邮件尚未发出的窗口。缺行 = 调用方违反 saga 顺序,fail-closed 报错)。
func (r *MySQLInventoryRepo) EscrowOutInstances(ctx context.Context, sourcePlayerID, toPlayerID uint64, instanceIDs []uint64, escrowKey, detail string) ([]EscrowedInstance, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	fp := EscrowOutFingerprint(toPlayerID, instanceIDs)
	already, _, lerr := claimLedger(ctx, tx, sourcePlayerID, escrowKey, "escrow_out", fp, detail)
	if lerr != nil {
		return nil, false, lerr
	}
	if already {
		rows, rerr := selectEscrowRowsTx(ctx, tx, sortedIDs(instanceIDs), false)
		if rerr != nil {
			return nil, false, rerr
		}
		if len(rows) != len(instanceIDs) {
			return nil, false, errcode.New(errcode.ErrInventoryItemNotFound,
				"escrow_out replay: %d/%d rows missing key=%s (saga order violated?)",
				len(instanceIDs)-len(rows), len(instanceIDs), escrowKey)
		}
		if cerr := tx.Commit(); cerr != nil {
			return nil, false, errcode.New(errcode.ErrInternal, "commit escrow_out replay: %v", cerr)
		}
		return rows, true, nil
	}

	out := make([]EscrowedInstance, 0, len(instanceIDs))
	for _, id := range sortedIDs(instanceIDs) {
		inst, serr := selectInstanceForUpdate(ctx, tx, sourcePlayerID, id)
		if serr != nil {
			return nil, false, serr
		}
		if inst.Bound {
			return nil, false, errcode.New(errcode.ErrInventoryInstanceBound,
				"bound instance not transferable player=%d id=%d", sourcePlayerID, id)
		}
		// INSERT ... SELECT + DELETE 同事务搬移:attributes 等字段不经 Go 回写,逐字节原样。
		if _, ierr := tx.ExecContext(ctx,
			`INSERT INTO mail_transfer_escrow
			   (instance_id, item_config_id, identified, attributes, bound, source_player_id, to_player_id, escrow_key)
			 SELECT instance_id, item_config_id, identified, attributes, bound, ?, ?, ?
			 FROM player_item_instance WHERE instance_id = ? AND player_id = ?`,
			sourcePlayerID, toPlayerID, escrowKey, id, sourcePlayerID); ierr != nil {
			return nil, false, errcode.New(errcode.ErrInternal, "escrow out insert id=%d: %v", id, ierr)
		}
		if _, derr := tx.ExecContext(ctx,
			`DELETE FROM player_item_instance WHERE instance_id = ? AND player_id = ?`,
			id, sourcePlayerID); derr != nil {
			return nil, false, errcode.New(errcode.ErrInternal, "escrow out delete id=%d: %v", id, derr)
		}
		out = append(out, EscrowedInstance{
			InstanceID:     inst.InstanceID,
			ItemConfigID:   inst.ItemConfigID,
			Identified:     inst.Identified,
			Attributes:     inst.Attributes,
			SourcePlayerID: sourcePlayerID,
			ToPlayerID:     toPlayerID,
		})
	}
	if cerr := tx.Commit(); cerr != nil {
		return nil, false, errcode.New(errcode.ErrInternal, "commit escrow_out player=%d: %v", sourcePlayerID, cerr)
	}
	return out, false, nil
}

// selectEscrowRowsTx 按 id 列表读托管行(forUpdate=true 加行锁;缺行不报错,由调用方按语义处理)。
func selectEscrowRowsTx(ctx context.Context, tx *sql.Tx, ids []uint64, forUpdate bool) ([]EscrowedInstance, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(ids))
	args := make([]any, 0, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args = append(args, id)
	}
	q := `SELECT ` + escrowCols + ` FROM mail_transfer_escrow WHERE instance_id IN (` +
		strings.Join(placeholders, ",") + `) ORDER BY instance_id ASC`
	if forUpdate {
		q += ` FOR UPDATE`
	}
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "select escrow rows: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []EscrowedInstance
	for rows.Next() {
		row, serr := scanEscrowRow(rows.Scan)
		if serr != nil {
			return nil, serr
		}
		out = append(out, row)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate escrow rows: %v", rerr)
	}
	return out, nil
}

// ClaimTransferInstances 把托管行原样搬进领取人实例表(同一事务;mail ClaimMail 专用)。
// 领取只认托管行(escrow 权威):行缺失 / to_player 不符 / config 漂移 → ErrInventoryItemNotFound
// 整批拒(邮件保持未领取);容量满 → ErrInventoryCapacityFull(重领可重试)。
func (r *MySQLInventoryRepo) ClaimTransferInstances(ctx context.Context, toPlayerID uint64, items []TransferClaimItem, capacity int32, idempotencyKey, detail string) (bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	fp := TransferClaimFingerprint(items)
	already, _, lerr := claimLedger(ctx, tx, toPlayerID, idempotencyKey, "transfer_claim", fp, detail)
	if lerr != nil {
		return false, lerr
	}
	if already {
		// 首次提交已完成搬移;回放直接成功(mail 侧 crash-after-claim 重试恰好一次)。
		if cerr := tx.Commit(); cerr != nil {
			return false, errcode.New(errcode.ErrInternal, "commit transfer_claim replay: %v", cerr)
		}
		return true, nil
	}

	wantByID := make(map[uint64]uint32, len(items))
	ids := make([]uint64, 0, len(items))
	for _, it := range items {
		wantByID[it.InstanceID] = it.ItemConfigID
		ids = append(ids, it.InstanceID)
	}
	rows, rerr := selectEscrowRowsTx(ctx, tx, sortedIDs(ids), true)
	if rerr != nil {
		return false, rerr
	}
	if len(rows) != len(items) {
		return false, errcode.New(errcode.ErrInventoryItemNotFound,
			"transfer claim: %d/%d escrow rows missing player=%d", len(items)-len(rows), len(items), toPlayerID)
	}
	for _, row := range rows {
		if row.ToPlayerID != toPlayerID {
			return false, errcode.New(errcode.ErrInventoryItemNotFound,
				"transfer claim: escrow id=%d not destined to player=%d", row.InstanceID, toPlayerID)
		}
		if want := wantByID[row.InstanceID]; want != row.ItemConfigID {
			return false, errcode.New(errcode.ErrInventoryItemNotFound,
				"transfer claim: config drift id=%d want=%d escrow=%d", row.InstanceID, want, row.ItemConfigID)
		}
	}

	occupied, total, lockErr := lockPlayerInstances(ctx, tx, toPlayerID)
	if lockErr != nil {
		return false, lockErr
	}
	if capacity <= 0 {
		return false, errcode.New(errcode.ErrInventoryCapacityFull, "instance inventory disabled (capacity<=0) player=%d", toPlayerID)
	}
	if total+len(rows) > int(capacity) {
		return false, errcode.New(errcode.ErrInventoryCapacityFull,
			"capacity full player=%d have=%d claim=%d cap=%d", toPlayerID, total, len(rows), capacity)
	}
	for _, row := range rows {
		slot, ok := lowestFreeSlot(occupied, capacity)
		if !ok {
			return false, errcode.New(errcode.ErrInventoryCapacityFull, "no free slot player=%d cap=%d", toPlayerID, capacity)
		}
		occupied[slot] = struct{}{}
		if _, ierr := tx.ExecContext(ctx,
			`INSERT INTO player_item_instance
			   (instance_id, player_id, item_config_id, identified, attributes, slot_index, bound)
			 SELECT instance_id, ?, item_config_id, identified, attributes, ?, bound
			 FROM mail_transfer_escrow WHERE instance_id = ?`,
			toPlayerID, slot, row.InstanceID); ierr != nil {
			return false, errcode.New(errcode.ErrInternal, "transfer claim insert id=%d: %v", row.InstanceID, ierr)
		}
		if _, derr := tx.ExecContext(ctx,
			`DELETE FROM mail_transfer_escrow WHERE instance_id = ?`, row.InstanceID); derr != nil {
			return false, errcode.New(errcode.ErrInternal, "transfer claim delete escrow id=%d: %v", row.InstanceID, derr)
		}
	}
	if cerr := tx.Commit(); cerr != nil {
		return false, errcode.New(errcode.ErrInternal, "commit transfer_claim player=%d: %v", toPlayerID, cerr)
	}
	return false, nil
}

// ReleaseTransferEscrow 托管释放回各行的 source 玩家(发信 saga 失败补偿)。
// 幂等由行存在性承担:行缺失(已领取/已释放)→ 跳过 no-op。释放不设容量闸(资产归还
// 优先):一律以未分配格(slot NULL,uk 允许多 NULL)入包,玩家整理后自然回落。
func (r *MySQLInventoryRepo) ReleaseTransferEscrow(ctx context.Context, instanceIDs []uint64) (int, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, rerr := selectEscrowRowsTx(ctx, tx, sortedIDs(instanceIDs), true)
	if rerr != nil {
		return 0, rerr
	}
	for _, row := range rows {
		if _, ierr := tx.ExecContext(ctx,
			`INSERT INTO player_item_instance
			   (instance_id, player_id, item_config_id, identified, attributes, slot_index, bound)
			 SELECT instance_id, source_player_id, item_config_id, identified, attributes, NULL, bound
			 FROM mail_transfer_escrow WHERE instance_id = ?`,
			row.InstanceID); ierr != nil {
			return 0, errcode.New(errcode.ErrInternal, "release escrow insert id=%d: %v", row.InstanceID, ierr)
		}
		if _, derr := tx.ExecContext(ctx,
			`DELETE FROM mail_transfer_escrow WHERE instance_id = ?`, row.InstanceID); derr != nil {
			return 0, errcode.New(errcode.ErrInternal, "release escrow delete id=%d: %v", row.InstanceID, derr)
		}
	}
	if cerr := tx.Commit(); cerr != nil {
		return 0, errcode.New(errcode.ErrInternal, "commit release escrow: %v", cerr)
	}
	return len(rows), nil
}

// ConsumeTransferEscrow 消托管行不物化(bag phase 2 DS 领取链):资产已经 bag journal
// 原样入包(bag 域),托管行只删,防"bag 域已入 + 托管行残留"双持。
// 存在的行必须 destined to 该玩家(不符 = 调用方错乱,整批拒);行缺失 = 已消费,no-op 幂等。
func (r *MySQLInventoryRepo) ConsumeTransferEscrow(ctx context.Context, toPlayerID uint64, instanceIDs []uint64) (int, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, rerr := selectEscrowRowsTx(ctx, tx, sortedIDs(instanceIDs), true)
	if rerr != nil {
		return 0, rerr
	}
	for _, row := range rows {
		if row.ToPlayerID != toPlayerID {
			return 0, errcode.New(errcode.ErrInventoryItemNotFound,
				"consume escrow: id=%d not destined to player=%d", row.InstanceID, toPlayerID)
		}
		if _, derr := tx.ExecContext(ctx,
			`DELETE FROM mail_transfer_escrow WHERE instance_id = ?`, row.InstanceID); derr != nil {
			return 0, errcode.New(errcode.ErrInternal, "consume escrow delete id=%d: %v", row.InstanceID, derr)
		}
	}
	if cerr := tx.Commit(); cerr != nil {
		return 0, errcode.New(errcode.ErrInternal, "commit consume escrow: %v", cerr)
	}
	return len(rows), nil
}
