package data

import "testing"

// TestGrantInstancesFingerprintCanonicalAndMultiplicity 确保重复奖励重试可忽略顺序，
// 但不能把“同配置多发一件”误判成同一请求。
func TestGrantInstancesFingerprintCanonicalAndMultiplicity(t *testing.T) {
	base := GrantInstancesFingerprint([]uint32{5002, 5001, 5001})
	if got := GrantInstancesFingerprint([]uint32{5001, 5002, 5001}); got != base {
		t.Fatalf("同一奖励集合换序后指纹应一致: base=%s got=%s", base, got)
	}
	if got := GrantInstancesFingerprint([]uint32{5001, 5002}); got == base {
		t.Fatalf("减少一件奖励后指纹必须变化: fingerprint=%s", got)
	}
	if got := GrantInstancesFingerprint([]uint32{5001, 5001, 5001}); got == base {
		t.Fatalf("替换配置但保持件数时指纹必须变化: fingerprint=%s", got)
	}
}

// TestLowestFreeSlotCapacityBoundaries 覆盖最后一格、满格、缩容与扩容时的槽位裁决。
// 并发原子性由 MySQL 的 FOR UPDATE 路径负责，不能用该纯函数测试替代。
func TestLowestFreeSlotCapacityBoundaries(t *testing.T) {
	tests := []struct {
		name     string
		occupied map[int32]struct{}
		capacity int32
		wantSlot int32
		wantOK   bool
	}{
		{name: "空背包首格", occupied: map[int32]struct{}{}, capacity: 3, wantSlot: 0, wantOK: true},
		{name: "最后一格", occupied: map[int32]struct{}{0: {}, 1: {}}, capacity: 3, wantSlot: 2, wantOK: true},
		{name: "满格", occupied: map[int32]struct{}{0: {}, 1: {}, 2: {}}, capacity: 3, wantSlot: -1, wantOK: false},
		{name: "复用中间空槽", occupied: map[int32]struct{}{0: {}, 2: {}}, capacity: 3, wantSlot: 1, wantOK: true},
		{name: "缩容后无可用槽", occupied: map[int32]struct{}{0: {}, 1: {}, 2: {}}, capacity: 2, wantSlot: -1, wantOK: false},
		{name: "扩容开放新槽", occupied: map[int32]struct{}{0: {}, 1: {}, 2: {}}, capacity: 4, wantSlot: 3, wantOK: true},
		{name: "零容量", occupied: map[int32]struct{}{}, capacity: 0, wantSlot: -1, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSlot, gotOK := lowestFreeSlot(tt.occupied, tt.capacity)
			if gotSlot != tt.wantSlot || gotOK != tt.wantOK {
				t.Fatalf("槽位裁决不符: got=(%d,%v) want=(%d,%v)", gotSlot, gotOK, tt.wantSlot, tt.wantOK)
			}
		})
	}
}

// TestInstanceIDLedgerDetailRoundTrip 确保幂等回放保存的实例 ID 不丢失、不重排。
func TestInstanceIDLedgerDetailRoundTrip(t *testing.T) {
	want := []uint64{9003, 9001, 9002}
	got := decodeInstanceIDs(encodeInstanceIDs(want))
	if len(got) != len(want) {
		t.Fatalf("回放实例数量不符: got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("回放实例顺序或内容不符: got=%v want=%v", got, want)
		}
	}
}

func TestItemClosureFingerprintsSeparateOperationsAndExactInstance(t *testing.T) {
	if UseFingerprint(10001, 1) == BattleConsumeFingerprint(10001, 1) ||
		DiscardFingerprint(10001, 1) == BattleDiscardFingerprint(10001, 1) {
		t.Fatal("大厅与战斗操作必须使用不同指纹域")
	}
	base := SellInstanceFingerprint(9001, 10003)
	if SellInstanceFingerprint(9001, 10027) == base ||
		SellInstanceFingerprint(9002, 10003) == base {
		t.Fatal("实例出售指纹必须绑定 instance/config")
	}
	if SellInstanceFingerprint(9001, 10003) != base {
		t.Fatal("服务端热更售价不得改变实例出售意图指纹")
	}
	if legacySaleLedgerMatches(legacySellFingerprint(10001, 2, 180),
		"sell item=10001 count=2 gold=180", saleLedgerIntent{op: "sell", itemConfigID: 10001, count: 2}) != true {
		t.Fatal("合法旧 stack ledger 应按 detail 中首次售价兼容")
	}
	if legacySaleLedgerMatches(legacySellFingerprint(10001, 2, 180),
		"sell item=10001 count=3 gold=180", saleLedgerIntent{op: "sell", itemConfigID: 10001, count: 2}) {
		t.Fatal("旧 ledger 的 detail/hash 与请求意图不一致不得放行")
	}
	if legacySaleLedgerMatches(legacySellInstanceFingerprint(9001, 10003, 180),
		"sell instance=9001 item=10003 gold=180 trailing", saleLedgerIntent{op: "sell_inst", instanceID: 9001, itemConfigID: 10003}) {
		t.Fatal("旧 ledger detail 必须精确解析，不得接受尾随内容")
	}
}

// idsWithDigits 造 n 个**指定十进制位数**的 id,用来模拟真实雪花的编码宽度。
// 现网雪花是 17 位(2026-08 时 id≈2.7e16);20 位是 uint64 的最坏情况,
// 也是"若干年后雪花涨到顶"的形态。
func idsWithDigits(n, digits int) []uint64 {
	base := uint64(1)
	for i := 1; i < digits; i++ {
		base *= 10
	}
	out := make([]uint64, n)
	for i := range out {
		out[i] = base + uint64(i)
	}
	return out
}

// TestLedgerDetailGateJudgesActualEncodedLength 钉死列宽闸的判定口径:
// **只看这一条 detail 的实际编码长度**,不按 uint64 最坏 20 位反推件数。
//
// 事故背景(2026-08-24):detail 是幂等回放的唯一事实源,一次发太多件会撞
// VARCHAR(255),INSERT 报 Error 1406 被包成 ErrInternal —— 玩家和策划都看不出原因。
// 第一版修复在 biz 按"最坏 20 位"反推件数上闸(grant 11 / 购买 9),复核实测判为 P0 回退:
// 现网雪花只有 17 位,那道闸把今天 100% 能成的 12、13 件直接改判为拒。
// 这条测试就钉住"按实际长度判"这个口径:今天的 17 位 id 必须能发 13 件 / 买 11 份。
func TestLedgerDetailGateJudgesActualEncodedLength(t *testing.T) {
	const snowflakeDigitsToday = 17

	t.Run("grant_inst 今天能发 13 件", func(t *testing.T) {
		if !GrantInstancesDetailFits(idsWithDigits(13, snowflakeDigitsToday)) {
			t.Fatalf("17 位 id 的 13 件必须装得下,实际编码 %d 字符",
				len(encodeInstanceIDs(idsWithDigits(13, snowflakeDigitsToday))))
		}
		if GrantInstancesDetailFits(idsWithDigits(14, snowflakeDigitsToday)) {
			t.Fatalf("14 件已超列容量却被判为装得下")
		}
	})

	t.Run("shop_buy 今天能买 11 份", func(t *testing.T) {
		mk := func(n int) PurchaseRequest {
			return PurchaseRequest{
				ShopID: 1, ItemConfigID: 6002, UnitCount: uint32(n),
				InstanceIDs: idsWithDigits(n, snowflakeDigitsToday),
			}
		}
		if !PurchaseDetailFits(mk(11)) {
			t.Fatalf("17 位 id 的 11 份必须装得下,实际编码 %d 字符", len(purchaseDetail(mk(11))))
		}
		if PurchaseDetailFits(mk(12)) {
			t.Fatalf("12 份已超列容量却被判为装得下")
		}
	})

	t.Run("最坏 20 位仍按实际长度收敛", func(t *testing.T) {
		// 位数涨上去后能装的件数自然变少 —— 这正是不按最坏位数硬定件数的代价与前提:
		// 已提交批次的回放由 data 层"超长先探旧流水"兜住,不是靠这里少发几件。
		if GrantInstancesDetailFits(idsWithDigits(13, 20)) {
			t.Fatalf("20 位 id 的 13 件不可能装得下")
		}
		if !GrantInstancesDetailFits(idsWithDigits(11, 20)) {
			t.Fatalf("20 位 id 的 11 件应仍装得下")
		}
	})
}

// TestPurchaseDetailRoundTripAtBudgetBoundary 列宽边界处 detail 仍必须能被原样解析回来。
// 幂等重放靠 parsePurchaseDetail 还原"首次到底发了什么",解析不出就 fail-closed 报内部错;
// 列宽闸只保证"写得进去",这条保证"读得回来"。
func TestPurchaseDetailRoundTripAtBudgetBoundary(t *testing.T) {
	const shopID, itemID, units = uint32(1), uint32(6002), uint32(9)
	// 取该档位下正好还装得进列的最大件数(直接问生产的闸,不另算一套公式)。
	n := 0
	for k := 1; k <= 64; k++ {
		if PurchaseDetailFits(PurchaseRequest{
			ShopID: shopID, ItemConfigID: itemID, UnitCount: units, InstanceIDs: idsWithDigits(k, 20),
		}) {
			n = k
		}
	}
	if n == 0 {
		t.Fatalf("列容量必须允许至少 1 件")
	}
	want := make([]uint64, n)
	for i := range want {
		want[i] = ^uint64(0) - uint64(i)
	}
	detail := purchaseDetail(PurchaseRequest{
		ShopID: shopID, ItemConfigID: itemID, UnitCount: units, InstanceIDs: want,
	})
	count, got, ok := parsePurchaseDetail(detail)
	if !ok {
		t.Fatalf("边界处 detail 必须可解析: %q", detail)
	}
	if count != 0 {
		t.Fatalf("装备购买的 count 应为 0, got %d", count)
	}
	if len(got) != len(want) {
		t.Fatalf("回放件数不符: got=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("第 %d 件 id 不符: got=%d want=%d", i, got[i], want[i])
		}
	}
}
