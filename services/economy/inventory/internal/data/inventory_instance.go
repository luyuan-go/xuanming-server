// inventory_instance.go — 装备实例背包数据层(W5 ④,2026-07-08)。
//
// 库表:player_item_instance(deploy/mysql-init/08-inventory-tables.sql)。
// 与 player_items(可堆叠消耗品计数)并存:消耗品走计数,装备类走实例(每件唯一 + 鉴定后随机属性)。
//
// 一致性(不变量 §9.7):
//   - GrantInstances 幂等复用 inventory_ledger(op=grant_inst,detail 记 instance_id CSV 供回放);
//     发放前锁玩家实例行校验容量,给每件分配最低空闲格(SELECT ... FOR UPDATE 防并发超发/占同格)。
//   - IdentifyInstance 天然幂等:SELECT ... FOR UPDATE,identified=1 后不再 roll(回放已落定属性)。
//   - MoveInstance 目标格唯一(uk_player_slot),被占 → ErrInventorySlotOccupied。
//
// 词条列编码(2026-08-07):`attributes` 是 VARBINARY,存
// pb `ItemInstanceAttributesStorageRecord` 二进制——MySQL 里的非基础类型一律
// proto 二进制序列化,不用 JSON(§5.8/§9.17:字段编号语义 + unknown fields 保留)。
package data

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	inventoryv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/inventory/v1"
)

// instanceAttrsMaxBytes 是词条 pb 写入侧字节上限,取列容量 VARBINARY(1024)。
// 鉴定词条数由 identify_rules.attr_count 约束(个位数),单条 pb 编码 ≤ 16 字节,
// 即使配成 32 条也不到 600 字节——达到 1024 说明配置失控或有人直写。
const instanceAttrsMaxBytes = 1024

// ItemAttribute 是装备实例鉴定后的一条随机属性。
type ItemAttribute struct {
	AttrID uint32
	Value  int64
}

// encodeInstanceAttrs 把词条编成 pb 二进制(空词条 → nil,列写 NULL)。
// 落库前过写入侧字节闸(§9.24):超限 fail-closed 拒写,逼近上限 WARN。
func encodeInstanceAttrs(ctx context.Context, table string, attrs []ItemAttribute) ([]byte, error) {
	if len(attrs) == 0 {
		return nil, nil
	}
	rec := &inventoryv1.ItemInstanceAttributesStorageRecord{
		Attributes: make([]*inventoryv1.ItemAttribute, 0, len(attrs)),
	}
	for _, a := range attrs {
		rec.Attributes = append(rec.Attributes, &inventoryv1.ItemAttribute{AttrId: a.AttrID, Value: a.Value})
	}
	raw, err := proto.Marshal(rec)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "encode instance attrs: %v", err)
	}
	if cerr := dbguard.CheckPayload(ctx, dbguard.PayloadLimit{
		DB: "pandora_trade", Table: table, Column: "attributes", Max: instanceAttrsMaxBytes,
		Hint: "鉴定词条超设计上限:查 identify_rules.attr_count 配置是否失控",
	}, raw); cerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "instance attrs payload too large: %v", cerr)
	}
	return raw, nil
}

// decodeInstanceAttrs 从 pb 二进制解出词条(NULL / 空 → nil)。
func decodeInstanceAttrs(raw []byte) ([]ItemAttribute, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var rec inventoryv1.ItemInstanceAttributesStorageRecord
	if err := proto.Unmarshal(raw, &rec); err != nil {
		return nil, err
	}
	if len(rec.GetAttributes()) == 0 {
		return nil, nil
	}
	out := make([]ItemAttribute, 0, len(rec.GetAttributes()))
	for _, a := range rec.GetAttributes() {
		out = append(out, ItemAttribute{AttrID: a.GetAttrId(), Value: a.GetValue()})
	}
	return out, nil
}

// ItemInstance 是一件装备类道具的唯一实例(不可堆叠)。
//
// SlotIndex 为 -1 表示未分配格(DB 里存 NULL;MySQL 唯一键允许多个 NULL,故多件未分配格不冲突)。
type ItemInstance struct {
	InstanceID   uint64
	ItemConfigID uint32
	Identified   bool
	Attributes   []ItemAttribute
	SlotIndex    int32 // -1 = 未分配格
	Bound        bool
}

// InstanceOwnershipQuery 是精确实例归属校验项；两个字段必须同时匹配。
type InstanceOwnershipQuery struct {
	InstanceID   uint64
	ItemConfigID uint32
}

// GrantInstancesFingerprint 计算发放实例请求指纹(item_config_ids 排序规范化)。
// 同一 idempotency_key 复用到不同发放内容 → 指纹不一致判冲突,防 key 复用串改账。
func GrantInstancesFingerprint(itemConfigIDs []uint32) string {
	sorted := append([]uint32(nil), itemConfigIDs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var b strings.Builder
	b.WriteString("grant_inst")
	for _, id := range sorted {
		b.WriteByte('|')
		b.WriteString(strconv.FormatUint(uint64(id), 10))
	}
	return hashHex(b.String())
}

// grantInstDetailPrefix 是 grant_inst 流水 detail 的固定前缀。
const grantInstDetailPrefix = "grant_inst ids="

// GrantInstancesDetailFits 报告这批 instance_id 编码后是否装得进 ledger.detail 列。
//
// 导出是给测试假仓复刻同一道闸用的:假仓若自己另算一套长度,就会与生产漂移
// (纪律见 biz/inventory_test.go 的 snapshotPlayerTx:假仓可以更严,绝不能更松)。
// 判定按**实际编码长度**,不按 uint64 最坏位数反推件数 —— 取舍见 inventory_repo.go
// 的容量预算段(最坏位数闸会把今天能发的批次改判为拒,且挡死已提交批次的回放)。
func GrantInstancesDetailFits(instanceIDs []uint64) bool {
	return ledgerDetailFits(encodeInstanceIDs(instanceIDs))
}

// encodeInstanceIDs / decodeInstanceIDs 把发放的 instance_id 列表编进 / 解出 ledger.detail
// (格式 "grant_inst ids=123,456";供幂等回放时按 id 重新 SELECT 实例)。
func encodeInstanceIDs(ids []uint64) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatUint(id, 10)
	}
	return grantInstDetailPrefix + strings.Join(parts, ",")
}

func decodeInstanceIDs(detail string) []uint64 {
	const marker = "ids="
	i := strings.Index(detail, marker)
	if i < 0 {
		return nil
	}
	csv := detail[i+len(marker):]
	if csv == "" {
		return nil
	}
	parts := strings.Split(csv, ",")
	out := make([]uint64, 0, len(parts))
	for _, p := range parts {
		if id, err := strconv.ParseUint(strings.TrimSpace(p), 10, 64); err == nil && id != 0 {
			out = append(out, id)
		}
	}
	return out
}

// scanInstance 从一行 SELECT 结果扫出 ItemInstance(attributes pb 二进制 / slot NULL 兜底)。
func scanInstance(scan func(dest ...any) error) (ItemInstance, error) {
	var (
		inst       ItemInstance
		identified int8
		bound      int8
		attrsRaw   []byte
		slot       sql.NullInt32
	)
	if err := scan(&inst.InstanceID, &inst.ItemConfigID, &identified, &attrsRaw, &slot, &bound); err != nil {
		return ItemInstance{}, err
	}
	inst.Identified = identified != 0
	inst.Bound = bound != 0
	if slot.Valid {
		inst.SlotIndex = slot.Int32
	} else {
		inst.SlotIndex = -1
	}
	attrs, derr := decodeInstanceAttrs(attrsRaw)
	if derr != nil {
		return ItemInstance{}, errcode.New(errcode.ErrInternal, "decode instance attrs id=%d: %v", inst.InstanceID, derr)
	}
	inst.Attributes = attrs
	return inst, nil
}

const instanceCols = `instance_id, item_config_id, identified, attributes, slot_index, bound`

func (r *MySQLInventoryRepo) ListInstances(ctx context.Context, playerID uint64) ([]ItemInstance, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT `+instanceCols+` FROM player_item_instance WHERE player_id = ? ORDER BY instance_id ASC`, playerID)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "list instances player=%d: %v", playerID, err)
	}
	defer func() { _ = rows.Close() }()
	var out []ItemInstance
	for rows.Next() {
		inst, serr := scanInstance(rows.Scan)
		if serr != nil {
			return nil, serr
		}
		out = append(out, inst)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate instances player=%d: %v", playerID, rerr)
	}
	return out, nil
}

func (r *MySQLInventoryRepo) CheckInstancesOwned(ctx context.Context, playerID uint64, queries []InstanceOwnershipQuery) ([]ItemInstance, error) {
	if len(queries) == 0 {
		return nil, nil
	}
	want := make(map[uint64]uint32, len(queries))
	placeholders := make([]string, 0, len(queries))
	args := make([]any, 0, len(queries)+1)
	args = append(args, playerID)
	for _, q := range queries {
		want[q.InstanceID] = q.ItemConfigID
		placeholders = append(placeholders, "?")
		args = append(args, q.InstanceID)
	}
	rows, err := r.db.QueryContext(ctx,
		`SELECT `+instanceCols+` FROM player_item_instance WHERE player_id = ? AND instance_id IN (`+
			strings.Join(placeholders, ",")+`) ORDER BY instance_id ASC`, args...)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "check instance ownership player=%d: %v", playerID, err)
	}
	defer func() { _ = rows.Close() }()
	owned := make([]ItemInstance, 0, len(queries))
	for rows.Next() {
		inst, serr := scanInstance(rows.Scan)
		if serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan instance ownership player=%d: %v", playerID, serr)
		}
		if want[inst.InstanceID] == inst.ItemConfigID {
			owned = append(owned, inst)
		}
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate instance ownership player=%d: %v", playerID, rerr)
	}
	return owned, nil
}

// lockPlayerInstances 在事务里锁玩家全部实例行,返回已占用格集合 + 实例总数(容量校验 + 分配空闲格用)。
func lockPlayerInstances(ctx context.Context, tx *sql.Tx, playerID uint64) (occupied map[int32]struct{}, total int, err error) {
	rows, qerr := tx.QueryContext(ctx,
		`SELECT slot_index FROM player_item_instance WHERE player_id = ? FOR UPDATE`, playerID)
	if qerr != nil {
		return nil, 0, errcode.New(errcode.ErrInternal, "lock instances player=%d: %v", playerID, qerr)
	}
	defer func() { _ = rows.Close() }()
	occupied = make(map[int32]struct{})
	for rows.Next() {
		var slot sql.NullInt32
		if serr := rows.Scan(&slot); serr != nil {
			return nil, 0, errcode.New(errcode.ErrInternal, "scan slot player=%d: %v", playerID, serr)
		}
		total++
		if slot.Valid {
			occupied[slot.Int32] = struct{}{}
		}
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, 0, errcode.New(errcode.ErrInternal, "iterate slots player=%d: %v", playerID, rerr)
	}
	return occupied, total, nil
}

// lowestFreeSlot 返回 [0,capacity) 内最小的未占用格;满 → (-1,false)。
func lowestFreeSlot(occupied map[int32]struct{}, capacity int32) (int32, bool) {
	for s := int32(0); s < capacity; s++ {
		if _, taken := occupied[s]; !taken {
			return s, true
		}
	}
	return -1, false
}

func (r *MySQLInventoryRepo) GrantInstances(ctx context.Context, playerID uint64, instanceIDs []uint64, itemConfigIDs []uint32, capacity int32, idempotencyKey, detail string) ([]ItemInstance, bool, error) {
	if len(instanceIDs) != len(itemConfigIDs) {
		return nil, false, errcode.New(errcode.ErrInvalidArg, "instanceIDs/itemConfigIDs length mismatch")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 幂等声明:detail 里编入本次 instance_id,供命中 uk 时回放按 id 重新 SELECT。
	fp := GrantInstancesFingerprint(itemConfigIDs)
	ledgerDetail := encodeInstanceIDs(instanceIDs)

	// detail 列宽是**全服唯一一道**闸,按实际编码长度判(理由见 inventory_repo.go 容量预算段)。
	//
	// 闸必须排在幂等回放**之后**:这里的"声明"与"判重"本是同一条 INSERT(撞唯一键即已处理),
	// 而超长时那条 INSERT 根本执行不了 —— MySQL 先报 Error 1406,压根轮不到唯一键。
	// 所以超长时必须显式探一次 (player_id, idempotency_key) 的旧流水:
	// 探到 = 这批货早就发过了,照常回放(下游 battle_result / mail / mission 都是永不放弃的
	// 重试者,拒一次就是永久卡住的行:货已发、行清不掉);探不到才是真·新的超长请求。
	fits := ledgerDetailFits(ledgerDetail)
	claimed := false
	if fits {
		const ins = `INSERT INTO inventory_ledger (player_id, idempotency_key, op, request_fingerprint, detail) VALUES (?, ?, 'grant_inst', ?, ?)`
		if _, lerr := tx.ExecContext(ctx, ins, playerID, idempotencyKey, fp, ledgerDetail); lerr == nil {
			claimed = true
		} else if !isDupErr(lerr) {
			return nil, false, errcode.New(errcode.ErrInternal, "insert ledger player=%d key=%s: %v", playerID, idempotencyKey, lerr)
		}
	}
	if !claimed {
		// 幂等命中(或超长时的显式探测):比对指纹,按已存 detail 里的 id 回放实例。
		var storedFP, storedDetail string
		qerr := tx.QueryRowContext(ctx,
			`SELECT request_fingerprint, detail FROM inventory_ledger WHERE player_id = ? AND idempotency_key = ? LIMIT 1`,
			playerID, idempotencyKey).Scan(&storedFP, &storedDetail)
		switch {
		case errors.Is(qerr, sql.ErrNoRows) && !fits:
			// 没有旧流水 + 这批 id 编码后装不下 = 确实是一笔新的超长发放。
			// 报 ErrInvalidArg(调用方拆批即可),不是"背包满"—— 报容量满会把运维引到扩容上。
			return nil, false, errcode.New(errcode.ErrInvalidArg,
				"grant_inst detail exceeds ledger column player=%d count=%d len=%d max=%d (split into multiple idempotency keys)",
				playerID, len(instanceIDs), len(ledgerDetail), ledgerDetailMaxChars)
		case qerr != nil:
			// fits 且撞了唯一键却读不到行,只可能是并发事务未提交:fail-closed 让调用方重试。
			return nil, false, errcode.New(errcode.ErrInternal, "read ledger player=%d key=%s: %v", playerID, idempotencyKey, qerr)
		}
		if storedFP != fp {
			// 同键不同请求内容(实例资产发放侧):同 ledger 路径,fail-closed 并留证。
			plog.With(ctx).Warnw("msg", "inventory_idempotency_conflict",
				"player_id", playerID, "idempotency_key", idempotencyKey, "op", "instance")
			return nil, false, errcode.New(errcode.ErrInventoryIdempotencyConflict,
				"idempotency_key reused for different request player=%d key=%s", playerID, idempotencyKey)
		}
		insts, rerr := selectInstancesByIDsTx(ctx, tx, playerID, decodeInstanceIDs(storedDetail))
		if rerr != nil {
			return nil, false, rerr
		}
		if cerr := tx.Commit(); cerr != nil {
			return nil, false, errcode.New(errcode.ErrInternal, "commit grant_inst replay player=%d: %v", playerID, cerr)
		}
		return insts, true, nil
	}

	// 首次:容量校验 + 分配空闲格 + 插入。
	out, gerr := grantInstancesInTxMixed(ctx, tx, playerID, instanceIDs, itemConfigIDs, capacity)
	if gerr != nil {
		return nil, false, gerr
	}
	if cerr := tx.Commit(); cerr != nil {
		return nil, false, errcode.New(errcode.ErrInternal, "commit grant_inst player=%d: %v", playerID, cerr)
	}
	return out, false, nil
}

// grantInstancesInTxMixed 在**调用方已开好的事务**里锁玩家实例行、校验容量、逐件分配最低空闲格并插入。
//
// 抽出来是为了让"购买装备"能把扣钱与发货放进同一个事务(见 shop_purchase.go 顶部说明):
// 若购买复用外层的 GrantInstances,扣钱和发货就会落在两个事务里,中间崩溃 = 钱扣了货没到。
// 本函数**不提交也不回滚**,事务生命周期完全归调用方。
func grantInstancesInTxMixed(ctx context.Context, tx *sql.Tx, playerID uint64, instanceIDs []uint64, itemConfigIDs []uint32, capacity int32) ([]ItemInstance, error) {
	if len(instanceIDs) != len(itemConfigIDs) {
		return nil, errcode.New(errcode.ErrInvalidArg, "instanceIDs/itemConfigIDs length mismatch")
	}
	occupied, total, lockErr := lockPlayerInstances(ctx, tx, playerID)
	if lockErr != nil {
		return nil, lockErr
	}
	if capacity <= 0 {
		return nil, errcode.New(errcode.ErrInventoryCapacityFull, "instance inventory disabled (capacity<=0) player=%d", playerID)
	}
	if total+len(instanceIDs) > int(capacity) {
		return nil, errcode.New(errcode.ErrInventoryCapacityFull,
			"capacity full player=%d have=%d grant=%d cap=%d", playerID, total, len(instanceIDs), capacity)
	}

	const insInst = `INSERT INTO player_item_instance (instance_id, player_id, item_config_id, identified, attributes, slot_index, bound) VALUES (?, ?, ?, 0, NULL, ?, 0)`
	out := make([]ItemInstance, 0, len(instanceIDs))
	for i, instID := range instanceIDs {
		slot, ok := lowestFreeSlot(occupied, capacity)
		if !ok {
			return nil, errcode.New(errcode.ErrInventoryCapacityFull, "no free slot player=%d cap=%d", playerID, capacity)
		}
		occupied[slot] = struct{}{}
		if _, ierr := tx.ExecContext(ctx, insInst, instID, playerID, itemConfigIDs[i], slot); ierr != nil {
			return nil, errcode.New(errcode.ErrInternal, "insert instance player=%d id=%d: %v", playerID, instID, ierr)
		}
		out = append(out, ItemInstance{InstanceID: instID, ItemConfigID: itemConfigIDs[i], SlotIndex: slot})
	}
	return out, nil
}

// grantInstancesInTx 是"同一配置发 N 件"的便捷包装(商店购买用)。
func grantInstancesInTx(ctx context.Context, tx *sql.Tx, playerID uint64, instanceIDs []uint64, itemConfigID uint32, capacity int32) ([]ItemInstance, error) {
	configIDs := make([]uint32, len(instanceIDs))
	for i := range configIDs {
		configIDs[i] = itemConfigID
	}
	return grantInstancesInTxMixed(ctx, tx, playerID, instanceIDs, configIDs, capacity)
}

// selectInstancesByIDsTx 在事务里按 id 列表读实例(幂等回放用;按 instance_id 升序)。
func selectInstancesByIDsTx(ctx context.Context, tx *sql.Tx, playerID uint64, ids []uint64) ([]ItemInstance, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(ids))
	args := make([]any, 0, len(ids)+1)
	args = append(args, playerID)
	for i, id := range ids {
		placeholders[i] = "?"
		args = append(args, id)
	}
	q := `SELECT ` + instanceCols + ` FROM player_item_instance WHERE player_id = ? AND instance_id IN (` +
		strings.Join(placeholders, ",") + `) ORDER BY instance_id ASC`
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "select instances by ids player=%d: %v", playerID, err)
	}
	defer func() { _ = rows.Close() }()
	var out []ItemInstance
	for rows.Next() {
		inst, serr := scanInstance(rows.Scan)
		if serr != nil {
			return nil, serr
		}
		out = append(out, inst)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate instances by ids player=%d: %v", playerID, rerr)
	}
	return out, nil
}

// selectInstanceForUpdate 在事务里锁读单个实例(非本人 / 不存在 → ErrInventoryItemNotFound)。
func selectInstanceForUpdate(ctx context.Context, tx *sql.Tx, playerID, instanceID uint64) (ItemInstance, error) {
	inst, err := scanInstance(tx.QueryRowContext(ctx,
		`SELECT `+instanceCols+` FROM player_item_instance WHERE instance_id = ? AND player_id = ? LIMIT 1 FOR UPDATE`,
		instanceID, playerID).Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return ItemInstance{}, errcode.New(errcode.ErrInventoryItemNotFound, "instance not found player=%d id=%d", playerID, instanceID)
	}
	if err != nil {
		return ItemInstance{}, errcode.New(errcode.ErrInternal, "read instance player=%d id=%d: %v", playerID, instanceID, err)
	}
	return inst, nil
}

func (r *MySQLInventoryRepo) IdentifyInstance(ctx context.Context, playerID, instanceID uint64, attrs []ItemAttribute) (ItemInstance, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return ItemInstance{}, false, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	inst, serr := selectInstanceForUpdate(ctx, tx, playerID, instanceID)
	if serr != nil {
		return ItemInstance{}, false, serr
	}
	if inst.Identified {
		// 幂等:已鉴定不再 roll,回放已落定属性。
		if cerr := tx.Commit(); cerr != nil {
			return ItemInstance{}, false, errcode.New(errcode.ErrInternal, "commit identify replay player=%d id=%d: %v", playerID, instanceID, cerr)
		}
		return inst, true, nil
	}
	var attrsPayload any
	if len(attrs) > 0 {
		raw, merr := encodeInstanceAttrs(ctx, "player_item_instance", attrs)
		if merr != nil {
			return ItemInstance{}, false, merr
		}
		attrsPayload = raw
	}
	if _, uerr := tx.ExecContext(ctx,
		`UPDATE player_item_instance SET identified = 1, attributes = ? WHERE instance_id = ? AND player_id = ?`,
		attrsPayload, instanceID, playerID); uerr != nil {
		return ItemInstance{}, false, errcode.New(errcode.ErrInternal, "identify instance player=%d id=%d: %v", playerID, instanceID, uerr)
	}
	if cerr := tx.Commit(); cerr != nil {
		return ItemInstance{}, false, errcode.New(errcode.ErrInternal, "commit identify player=%d id=%d: %v", playerID, instanceID, cerr)
	}
	inst.Identified = true
	inst.Attributes = attrs
	return inst, false, nil
}

func (r *MySQLInventoryRepo) MoveInstance(ctx context.Context, playerID, instanceID uint64, toSlot, capacity int32) (ItemInstance, error) {
	if toSlot < 0 || toSlot >= capacity {
		return ItemInstance{}, errcode.New(errcode.ErrInventorySlotOccupied, "slot out of range player=%d slot=%d cap=%d", playerID, toSlot, capacity)
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return ItemInstance{}, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	inst, serr := selectInstanceForUpdate(ctx, tx, playerID, instanceID)
	if serr != nil {
		return ItemInstance{}, serr
	}
	if inst.SlotIndex == toSlot {
		if cerr := tx.Commit(); cerr != nil {
			return ItemInstance{}, errcode.New(errcode.ErrInternal, "commit move noop player=%d id=%d: %v", playerID, instanceID, cerr)
		}
		return inst, nil
	}
	// 目标格是否被别的实例占用。
	var occupantID uint64
	qerr := tx.QueryRowContext(ctx,
		`SELECT instance_id FROM player_item_instance WHERE player_id = ? AND slot_index = ? LIMIT 1 FOR UPDATE`,
		playerID, toSlot).Scan(&occupantID)
	if qerr != nil && !errors.Is(qerr, sql.ErrNoRows) {
		return ItemInstance{}, errcode.New(errcode.ErrInternal, "probe slot player=%d slot=%d: %v", playerID, toSlot, qerr)
	}
	if occupantID != 0 && occupantID != instanceID {
		return ItemInstance{}, errcode.New(errcode.ErrInventorySlotOccupied, "slot occupied player=%d slot=%d by=%d", playerID, toSlot, occupantID)
	}
	if _, uerr := tx.ExecContext(ctx,
		`UPDATE player_item_instance SET slot_index = ? WHERE instance_id = ? AND player_id = ?`,
		toSlot, instanceID, playerID); uerr != nil {
		return ItemInstance{}, errcode.New(errcode.ErrInternal, "move instance player=%d id=%d: %v", playerID, instanceID, uerr)
	}
	if cerr := tx.Commit(); cerr != nil {
		return ItemInstance{}, errcode.New(errcode.ErrInternal, "commit move player=%d id=%d: %v", playerID, instanceID, cerr)
	}
	inst.SlotIndex = toSlot
	return inst, nil
}

func (r *MySQLInventoryRepo) DiscardInstance(ctx context.Context, playerID, instanceID uint64) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return errcode.New(errcode.ErrInternal, "begin discard instance tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	inst, serr := selectInstanceForUpdate(ctx, tx, playerID, instanceID)
	if serr != nil {
		if errcode.As(serr) == errcode.ErrInventoryItemNotFound {
			return nil // 资源状态本身承担幂等：已丢弃重放 no-op。
		}
		return serr
	}
	if inst.Bound {
		return errcode.New(errcode.ErrInventoryInstanceBound,
			"bound instance cannot be discarded player=%d id=%d", playerID, instanceID)
	}
	if _, derr := tx.ExecContext(ctx,
		`DELETE FROM player_item_instance WHERE instance_id = ? AND player_id = ?`,
		instanceID, playerID); derr != nil {
		return errcode.New(errcode.ErrInternal, "discard instance player=%d id=%d: %v", playerID, instanceID, derr)
	}
	if cerr := tx.Commit(); cerr != nil {
		return errcode.New(errcode.ErrInternal, "commit discard instance player=%d id=%d: %v", playerID, instanceID, cerr)
	}
	return nil
}

func (r *MySQLInventoryRepo) SellInstance(ctx context.Context, playerID, instanceID uint64, itemConfigID uint32, kind CurrencyKind, amount uint64, idempotencyKey, detail string) (SaleOutcome, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return SaleOutcome{}, false, errcode.New(errcode.ErrInternal, "begin sell instance tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	already, snap, lerr := claimSaleLedger(ctx, tx, playerID, idempotencyKey,
		SellInstanceFingerprint(instanceID, itemConfigID), detail, saleLedgerIntent{
			op: "sell_inst", itemConfigID: itemConfigID, instanceID: instanceID,
		})
	if lerr != nil {
		return SaleOutcome{}, false, lerr
	}
	if already {
		// 同 SellItem:用 Sole 回放首次执行的币种与金额,不受出售币种配置变更影响。
		earnedKind, earned, ok := snap.Delta.Sole()
		if !ok {
			earnedKind, earned = kind, 0
		}
		return SaleOutcome{Balances: snap.Balances, Earned: earned, Kind: earnedKind}, true, nil
	}
	// amount == 0 = 不可出售。uint64 下 `== 0` 已是完整校验(见 SellItem 同处注释)。
	if amount == 0 {
		return SaleOutcome{}, false, errcode.New(errcode.ErrInventoryNotSellable,
			"instance item not sellable player=%d instance=%d item=%d", playerID, instanceID, itemConfigID)
	}
	inst, serr := selectInstanceForUpdate(ctx, tx, playerID, instanceID)
	if serr != nil {
		return SaleOutcome{}, false, serr
	}
	if inst.Bound {
		return SaleOutcome{}, false, errcode.New(errcode.ErrInventoryInstanceBound,
			"bound instance cannot be sold player=%d id=%d", playerID, instanceID)
	}
	if inst.ItemConfigID != itemConfigID {
		return SaleOutcome{}, false, errcode.New(errcode.ErrInvalidArg,
			"instance config mismatch player=%d id=%d got=%d want=%d",
			playerID, instanceID, inst.ItemConfigID, itemConfigID)
	}
	if _, derr := tx.ExecContext(ctx,
		`DELETE FROM player_item_instance WHERE instance_id = ? AND player_id = ?`,
		instanceID, playerID); derr != nil {
		return SaleOutcome{}, false, errcode.New(errcode.ErrInternal, "sell instance delete player=%d id=%d: %v", playerID, instanceID, derr)
	}
	if _, gerr := addCurrencyTx(ctx, tx, playerID, kind, amount); gerr != nil {
		return SaleOutcome{}, false, gerr
	}
	newBalances, rerr := readBalancesTx(ctx, tx, playerID)
	if rerr != nil {
		return SaleOutcome{}, false, rerr
	}
	delta := Balances{kind: amount}
	if uerr := updateLedgerResult(ctx, tx, playerID, idempotencyKey, 0, newBalances, delta); uerr != nil {
		return SaleOutcome{}, false, uerr
	}
	if cerr := tx.Commit(); cerr != nil {
		return SaleOutcome{}, false, errcode.New(errcode.ErrInternal, "commit sell instance player=%d id=%d: %v", playerID, instanceID, cerr)
	}
	return SaleOutcome{Balances: newBalances, Earned: amount, Kind: kind}, false, nil
}
