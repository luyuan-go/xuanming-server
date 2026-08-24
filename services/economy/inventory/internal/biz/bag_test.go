// bag_test.go — 背包域 biz 校验层单测(假仓;数据层语义由 data 侧单测/集成测试覆盖)。
package biz

import (
	"context"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	bagv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/bag/v1"

	"github.com/luyuancpp/pandora/services/economy/inventory/internal/conf"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/data"
)

// fakeBagRepo 记录调用的假仓(校验层测试只关心"是否放行到数据层")。
type fakeBagRepo struct {
	appendCalls     int
	lastEntries     []*bagv1.BagJournalEntry
	checkpointCalls int
	lastCheckpoint  []byte

	// 容量购买状态(§5.3 单测用;镜像 ApplyCapacityPurchase 的档数 CAS 语义)。
	capExtra     map[uint32]uint32
	capPurchases map[uint32]uint32
	applyErr     error
}

func (f *fakeBagRepo) LoadBag(context.Context, uint64, uint64) ([]byte, []data.BagJournalRow, uint64, error) {
	return nil, nil, 0, nil
}

func (f *fakeBagRepo) AppendJournal(_ context.Context, _ uint64, _ uint64, entries []*bagv1.BagJournalEntry, _ data.BagSectionCapacity, _ data.BagMaxStack, _ int64) (uint64, error) {
	f.appendCalls++
	f.lastEntries = entries
	return entries[len(entries)-1].GetJournalSeq(), nil
}

func (f *fakeBagRepo) SaveCheckpoint(_ context.Context, _ uint64, _ uint64, snapshot []byte, _ uint64) error {
	f.checkpointCalls++
	f.lastCheckpoint = append(f.lastCheckpoint[:0], snapshot...)
	return nil
}

func (f *fakeBagRepo) GetSections(context.Context, uint64, []uint32, data.BagSectionCapacity) ([]*bagv1.BagSection, error) {
	return nil, nil
}

func (f *fakeBagRepo) SweepJournal(context.Context, time.Duration, int) (int64, error) { return 0, nil }

func (f *fakeBagRepo) ApplyCapacityPurchase(_ context.Context, _ uint64, bagType, tier, slots, maxExtra uint32) (uint32, uint32, bool, error) {
	if f.applyErr != nil {
		return 0, 0, false, f.applyErr
	}
	if f.capExtra == nil {
		f.capExtra = map[uint32]uint32{}
		f.capPurchases = map[uint32]uint32{}
	}
	if f.capPurchases[bagType] >= tier {
		return f.capExtra[bagType], f.capPurchases[bagType], false, nil
	}
	if f.capPurchases[bagType] != tier-1 {
		return 0, 0, false, errcode.New(errcode.ErrInvalidState, "tier out of order")
	}
	if f.capExtra[bagType]+slots > maxExtra {
		return 0, 0, false, errcode.New(errcode.ErrBagCapacityMaxed, "maxed")
	}
	f.capExtra[bagType] += slots
	f.capPurchases[bagType] = tier
	return f.capExtra[bagType], f.capPurchases[bagType], true, nil
}

func (f *fakeBagRepo) GetCapacityState(_ context.Context, _ uint64, bagType uint32) (uint32, uint32, error) {
	return f.capExtra[bagType], f.capPurchases[bagType], nil
}

// fakeCapacityCharger 可编程扣费器(记录扣费次数;同 key 幂等由 chargedTiers 模拟)。
type fakeCapacityCharger struct {
	gold         uint64
	chargedTiers map[string]bool
	charges      int
}

func capChargeKey(bagType, tier uint32) string {
	return data.BagCapacityChargeKey(bagType, tier)
}

func (f *fakeCapacityCharger) ChargeBagCapacity(_ context.Context, _ uint64, bagType, tier, _ uint32, _ data.CurrencyKind, priceGold uint64) (bool, uint64, error) {
	if f.chargedTiers == nil {
		f.chargedTiers = map[string]bool{}
	}
	key := capChargeKey(bagType, tier)
	if f.chargedTiers[key] {
		return true, f.gold, nil
	}
	if f.gold < priceGold {
		return false, 0, errcode.New(errcode.ErrInventoryInsufficient, "insufficient gold")
	}
	f.gold -= priceGold
	f.chargedTiers[key] = true
	f.charges++
	return false, f.gold, nil
}

func newBagUsecaseForTest() (*BagUsecase, *fakeBagRepo) {
	repo := &fakeBagRepo{}
	// AllowUnverifiedOwner:本组单测聚焦形状校验/repo 委托;owner 授权门有专属测试
	// (TestBagOwnerAuthorization*),此处显式跳过以隔离关注点。
	cfg := conf.BagConf{MaxJournalBatch: 4, MaxItemsPerOp: 2, HourlyJournalQuota: 100,
		SectionCapacities:    []conf.BagSectionCapacityRule{{BagType: 1, Capacity: 10}},
		AllowUnverifiedOwner: true}
	return NewBagUsecase(repo, cfg), repo
}

// fakeOwnerAuthorizer 可编程授权结果(owner 授权门单测用)。
type fakeOwnerAuthorizer struct {
	err           error
	resolvedEpoch uint64
	calls         int
}

func (f *fakeOwnerAuthorizer) AuthorizeOwnerWrite(_ context.Context, _ uint64, claimedEpoch uint64, _, _ string) (uint64, error) {
	f.calls++
	if f.err != nil {
		return 0, f.err
	}
	if f.resolvedEpoch != 0 {
		return f.resolvedEpoch, nil
	}
	return claimedEpoch, nil
}

func pickupEntry(seq uint64, bagType uint32, generation uint64, key string, items int) *bagv1.BagJournalEntry {
	list := make([]*bagv1.BagItem, 0, items)
	for i := 0; i < items; i++ {
		list = append(list, &bagv1.BagItem{ItemConfigId: 10001, Count: 1})
	}
	return &bagv1.BagJournalEntry{
		JournalSeq: seq, BagType: bagType, Generation: generation, IdempotencyKey: key,
		Op: &bagv1.BagJournalEntry_PickupGrant{PickupGrant: &bagv1.PickupGrantOp{Items: list}},
	}
}

func wantBizCode(t *testing.T, err error, code errcode.Code, what string) {
	t.Helper()
	if errcode.As(err) != code {
		t.Fatalf("%s: 期望 %d,实际 %v", what, code, err)
	}
}

func TestBagAppendJournalValidation(t *testing.T) {
	uc, repo := newBagUsecaseForTest()
	ctx := context.Background()

	if _, err := uc.AppendJournal(ctx, 0, 1, []*bagv1.BagJournalEntry{pickupEntry(1, 0, 0, "k", 1)}, DSCallerIdentity{}); err == nil {
		t.Fatal("player_id=0 应拒")
	}
	if _, err := uc.AppendJournal(ctx, 1, 1, nil, DSCallerIdentity{}); err == nil {
		t.Fatal("空批应拒")
	}
	over := make([]*bagv1.BagJournalEntry, 5)
	for i := range over {
		over[i] = pickupEntry(uint64(i+1), 0, 0, "k", 1)
	}
	wantBizCode(t, func() error { _, err := uc.AppendJournal(ctx, 1, 1, over, DSCallerIdentity{}); return err }(),
		errcode.ErrBagQuotaExceeded, "超批")
	wantBizCode(t, func() error {
		_, err := uc.AppendJournal(ctx, 1, 1, []*bagv1.BagJournalEntry{pickupEntry(1, 7, 0, "k", 1)}, DSCallerIdentity{})
		return err
	}(), errcode.ErrBagSectionNotAllowed, "未知段类型")
	wantBizCode(t, func() error {
		_, err := uc.AppendJournal(ctx, 1, 1, []*bagv1.BagJournalEntry{pickupEntry(1, 0, 3, "k", 1)}, DSCallerIdentity{})
		return err
	}(), errcode.ErrInvalidArg, "固定段带非零代际")
	wantBizCode(t, func() error {
		_, err := uc.AppendJournal(ctx, 1, 1, []*bagv1.BagJournalEntry{pickupEntry(1, 0, 0, "", 1)}, DSCallerIdentity{})
		return err
	}(), errcode.ErrInvalidArg, "空幂等键")
	wantBizCode(t, func() error {
		_, err := uc.AppendJournal(ctx, 1, 1, []*bagv1.BagJournalEntry{pickupEntry(1, 0, 0, "k", 3)}, DSCallerIdentity{})
		return err
	}(), errcode.ErrBagQuotaExceeded, "单 op 物品超限")
	tooManyJournalAttrs := make([]*bagv1.BagItemAttribute, bagMaxAttrsPerItem+1)
	for i := range tooManyJournalAttrs {
		tooManyJournalAttrs[i] = &bagv1.BagItemAttribute{AttrId: uint32(i + 1)}
	}
	journalAttrsOverflow := pickupEntry(1, 0, 0, "attrs-overflow", 1)
	journalAttrsOverflow.GetPickupGrant().Items[0].InstanceId = 101
	journalAttrsOverflow.GetPickupGrant().Items[0].Identified = true
	journalAttrsOverflow.GetPickupGrant().Items[0].Attrs = tooManyJournalAttrs
	wantBizCode(t, func() error {
		_, err := uc.AppendJournal(ctx, 1, 1, []*bagv1.BagJournalEntry{journalAttrsOverflow}, DSCallerIdentity{})
		return err
	}(), errcode.ErrBagQuotaExceeded, "journal 单物品词条超限")
	duplicateJournalAttr := pickupEntry(1, 0, 0, "attrs-duplicate", 1)
	duplicateJournalAttr.GetPickupGrant().Items[0].InstanceId = 102
	duplicateJournalAttr.GetPickupGrant().Items[0].Identified = true
	duplicateJournalAttr.GetPickupGrant().Items[0].Attrs = []*bagv1.BagItemAttribute{{AttrId: 3, Value: 1}, {AttrId: 3, Value: 1}}
	wantBizCode(t, func() error {
		_, err := uc.AppendJournal(ctx, 1, 1, []*bagv1.BagJournalEntry{duplicateJournalAttr}, DSCallerIdentity{})
		return err
	}(), errcode.ErrInvalidArg, "journal 重复词条")
	unsupportedJournalAttr := pickupEntry(1, 0, 0, "attrs-unsupported", 1)
	unsupportedJournalAttr.GetPickupGrant().Items[0].InstanceId = 105
	unsupportedJournalAttr.GetPickupGrant().Items[0].Identified = true
	unsupportedJournalAttr.GetPickupGrant().Items[0].Attrs = []*bagv1.BagItemAttribute{{AttrId: 1, Value: 100}}
	wantBizCode(t, func() error {
		_, err := uc.AppendJournal(ctx, 1, 1, []*bagv1.BagJournalEntry{unsupportedJournalAttr}, DSCallerIdentity{})
		return err
	}(), errcode.ErrInvalidArg, "journal 非正式 gameplay 词条")
	overflowMoveSpeed := pickupEntry(1, 0, 0, "attrs-movespeed-overflow", 1)
	overflowMoveSpeed.GetPickupGrant().Items[0].InstanceId = 106
	overflowMoveSpeed.GetPickupGrant().Items[0].Identified = true
	overflowMoveSpeed.GetPickupGrant().Items[0].Attrs = []*bagv1.BagItemAttribute{{AttrId: 7, Value: bagMaxMoveSpeedBasisPoints + 1}}
	wantBizCode(t, func() error {
		_, err := uc.AppendJournal(ctx, 1, 1, []*bagv1.BagJournalEntry{overflowMoveSpeed}, DSCallerIdentity{})
		return err
	}(), errcode.ErrInvalidArg, "journal 移速 basis-points 越界")
	attrsWithoutInstance := pickupEntry(1, 0, 0, "attrs-without-instance", 1)
	attrsWithoutInstance.GetPickupGrant().Items[0].Attrs = []*bagv1.BagItemAttribute{{AttrId: 3, Value: 1}}
	wantBizCode(t, func() error {
		_, err := uc.AppendJournal(ctx, 1, 1, []*bagv1.BagJournalEntry{attrsWithoutInstance}, DSCallerIdentity{})
		return err
	}(), errcode.ErrInvalidArg, "journal 堆叠物不得携实例词条")
	invalidInstanceCount := pickupEntry(1, 0, 0, "instance-count", 1)
	invalidInstanceCount.GetPickupGrant().Items[0].InstanceId = 103
	invalidInstanceCount.GetPickupGrant().Items[0].Count = 2
	wantBizCode(t, func() error {
		_, err := uc.AppendJournal(ctx, 1, 1, []*bagv1.BagJournalEntry{invalidInstanceCount}, DSCallerIdentity{})
		return err
	}(), errcode.ErrInvalidArg, "journal 实例数量非一")
	consumeWithInvalidProduce := &bagv1.BagJournalEntry{
		JournalSeq: 1, BagType: 1, IdempotencyKey: "consume-produce-attrs",
		Op: &bagv1.BagJournalEntry_Consume{Consume: &bagv1.ConsumeOp{
			ConsumeItems:   []*bagv1.BagItem{{ItemConfigId: 10001, Count: 1}},
			ProduceBagType: 0,
			ProduceItems: []*bagv1.BagItem{{
				ItemConfigId: 20001, Count: 1, InstanceId: 104, Identified: true, Attrs: tooManyJournalAttrs,
			}},
		}},
	}
	wantBizCode(t, func() error {
		_, err := uc.AppendJournal(ctx, 1, 1, []*bagv1.BagJournalEntry{consumeWithInvalidProduce}, DSCallerIdentity{})
		return err
	}(), errcode.ErrBagQuotaExceeded, "consume 产物词条超限")
	// 同段转移拒。
	sameType := &bagv1.BagJournalEntry{
		JournalSeq: 1, BagType: 1, IdempotencyKey: "k",
		Op: &bagv1.BagJournalEntry_Transfer{Transfer: &bagv1.TransferOp{
			ToBagType: 1, Items: []*bagv1.BagItem{{ItemConfigId: 1, Count: 1}},
		}},
	}
	wantBizCode(t, func() error {
		_, err := uc.AppendJournal(ctx, 1, 1, []*bagv1.BagJournalEntry{sameType}, DSCallerIdentity{})
		return err
	}(), errcode.ErrInvalidArg, "同段转移")
	// 缺 op 拒(未知 op fail-closed)。
	noOp := &bagv1.BagJournalEntry{JournalSeq: 1, BagType: 0, IdempotencyKey: "k"}
	wantBizCode(t, func() error {
		_, err := uc.AppendJournal(ctx, 1, 1, []*bagv1.BagJournalEntry{noOp}, DSCallerIdentity{})
		return err
	}(), errcode.ErrInvalidArg, "缺 op")

	if repo.appendCalls != 0 {
		t.Fatalf("校验失败不得触达数据层: %d", repo.appendCalls)
	}
	// 合法批放行。
	if _, err := uc.AppendJournal(ctx, 1, 1, []*bagv1.BagJournalEntry{
		pickupEntry(1, 0, 0, "k1", 1), pickupEntry(2, 100, 8, "k2", 1),
	}, DSCallerIdentity{}); err != nil {
		t.Fatalf("合法批应放行: %v", err)
	}
	if repo.appendCalls != 1 || len(repo.lastEntries) != 2 {
		t.Fatalf("放行批未触达数据层: calls=%d", repo.appendCalls)
	}
}

// TestBagAppendCarrySectionConsumeReachesData 钉死随身段 consume 的分层契约(§5.1.1,
// 2026-08-17「后端先行」拍板):丢弃扣减打随身组(0/2/3),校验层**必须放行到数据层**——
// 随身段闸曾经立在数据层 apply,若校验层再加回"consume 只许后端驻留段",副本丢弃会被
// ErrBagSectionNotAllowed(非可重试)整批拒,功能直接不通。
func TestBagAppendCarrySectionConsumeReachesData(t *testing.T) {
	uc, repo := newBagUsecaseForTest()
	ctx := context.Background()

	carryConsume := func(seq uint64, bagType uint32, key string) *bagv1.BagJournalEntry {
		return &bagv1.BagJournalEntry{
			JournalSeq: seq, BagType: bagType, IdempotencyKey: key,
			Op: &bagv1.BagJournalEntry_Consume{Consume: &bagv1.ConsumeOp{
				ConsumeItems: []*bagv1.BagItem{{ItemConfigId: 10001, Count: 1}},
			}},
		}
	}
	// 0 身上 / 2 装备栏 / 3 临时格:三个随身段的丢弃流水都必须能落到数据层。
	entries := []*bagv1.BagJournalEntry{
		carryConsume(1, 0, "discard-inventory"),
		carryConsume(2, 2, "discard-equipment"),
		carryConsume(3, 3, "discard-temporary"),
	}
	if _, err := uc.AppendJournal(ctx, 1, 1, entries, DSCallerIdentity{}); err != nil {
		t.Fatalf("随身段 consume 应放行到数据层: %v", err)
	}
	if repo.appendCalls != 1 || len(repo.lastEntries) != len(entries) {
		t.Fatalf("随身段 consume 未触达数据层: calls=%d entries=%d", repo.appendCalls, len(repo.lastEntries))
	}

	// 产出目标段仍走已知段校验(随身段"带产出拒"的执行点在数据层 applyBagOpTx,
	// 见 data/bag_apply_test.go「随身组consume带产出拒」;此处只钉校验层不越权拦截)。
	produceUnknown := carryConsume(4, 0, "discard-produce-unknown")
	produceUnknown.GetConsume().ProduceBagType = 7
	produceUnknown.GetConsume().ProduceItems = []*bagv1.BagItem{{ItemConfigId: 20001, Count: 1}}
	wantBizCode(t, func() error {
		_, err := uc.AppendJournal(ctx, 1, 1, []*bagv1.BagJournalEntry{produceUnknown}, DSCallerIdentity{})
		return err
	}(), errcode.ErrBagSectionNotAllowed, "consume 产出未知段")
	if repo.appendCalls != 1 {
		t.Fatalf("校验失败不得触达数据层: %d", repo.appendCalls)
	}
}

func TestBagCheckpointAndSectionsValidation(t *testing.T) {
	uc, repo := newBagUsecaseForTest()
	ctx := context.Background()

	// checkpoint 含后端驻留段拒(仓库本体不归 checkpoint)。
	record := &bagv1.BagStorageRecord{Sections: []*bagv1.BagSection{{BagType: 1}}}
	wantBizCode(t, uc.SaveCheckpoint(ctx, 1, 1, record, nil, 0, DSCallerIdentity{}),
		errcode.ErrBagSectionNotAllowed, "checkpoint 含仓库段")

	// checkpoint 不信 capacity，但必须原样保存 slot/堆边界；合法恢复态允许 items>capacity、
	// slot>=capacity（journal 重放溢出后的临时格“只出不进”）。
	carry := &bagv1.BagStorageRecord{Sections: []*bagv1.BagSection{
		{
			BagType: 3, Capacity: 2, Items: []*bagv1.BagItem{
				{ItemConfigId: 10001, Count: 20, Slot: 0},
				{ItemConfigId: 20001, Count: 1, Slot: 2, InstanceId: 9001, Identified: true,
					Attrs: []*bagv1.BagItemAttribute{{AttrId: 3, Value: 25}, {AttrId: 7, Value: 250}}},
				{ItemConfigId: 10001, Count: 30, Slot: 3},
			},
		},
	}}
	checkpointBlob := []byte("spatial-checkpoint")
	if err := uc.SaveCheckpoint(ctx, 1, 1, carry, checkpointBlob, 0, DSCallerIdentity{}); err != nil {
		t.Fatalf("空间超容恢复态应放行: %v", err)
	}
	if repo.checkpointCalls != 1 || string(repo.lastCheckpoint) != string(checkpointBlob) {
		t.Fatalf("checkpoint 应原样触达数据层: calls=%d blob=%q", repo.checkpointCalls, repo.lastCheckpoint)
	}

	tooManyAttrs := make([]*bagv1.BagItemAttribute, bagMaxAttrsPerItem+1)
	for i := range tooManyAttrs {
		tooManyAttrs[i] = &bagv1.BagItemAttribute{AttrId: uint32(i + 1)}
	}
	invalidCases := []struct {
		name   string
		record *bagv1.BagStorageRecord
		blob   []byte
		code   errcode.Code
	}{
		{name: "nil record", record: nil, code: errcode.ErrInvalidArg},
		{name: "duplicate section", record: &bagv1.BagStorageRecord{Sections: []*bagv1.BagSection{{BagType: 0}, {BagType: 0}}}, code: errcode.ErrInvalidArg},
		{name: "fixed generation", record: &bagv1.BagStorageRecord{Sections: []*bagv1.BagSection{{BagType: 2, Generation: 1}}}, code: errcode.ErrInvalidArg},
		{name: "duplicate slot", record: &bagv1.BagStorageRecord{Sections: []*bagv1.BagSection{{BagType: 0, Items: []*bagv1.BagItem{
			{ItemConfigId: 1, Count: 1, Slot: 4}, {ItemConfigId: 2, Count: 1, Slot: 4},
		}}}}, code: errcode.ErrInvalidArg},
		{name: "duplicate instance across carry group", record: &bagv1.BagStorageRecord{Sections: []*bagv1.BagSection{
			{BagType: 0, Items: []*bagv1.BagItem{{ItemConfigId: 1, Count: 1, InstanceId: 77}}},
			{BagType: 2, Items: []*bagv1.BagItem{{ItemConfigId: 2, Count: 1, InstanceId: 77}}},
		}}, code: errcode.ErrInvalidArg},
		{name: "instance count", record: &bagv1.BagStorageRecord{Sections: []*bagv1.BagSection{{BagType: 2, Items: []*bagv1.BagItem{
			{ItemConfigId: 1, Count: 2, InstanceId: 88},
		}}}}, code: errcode.ErrInvalidArg},
		{name: "unidentified attrs", record: &bagv1.BagStorageRecord{Sections: []*bagv1.BagSection{{BagType: 2, Items: []*bagv1.BagItem{
			{ItemConfigId: 1, Count: 1, InstanceId: 89, Attrs: []*bagv1.BagItemAttribute{{AttrId: 3}}},
		}}}}, code: errcode.ErrInvalidArg},
		{name: "negative flat attr", record: &bagv1.BagStorageRecord{Sections: []*bagv1.BagSection{{BagType: 2, Items: []*bagv1.BagItem{
			{ItemConfigId: 1, Count: 1, InstanceId: 90, Identified: true,
				Attrs: []*bagv1.BagItemAttribute{{AttrId: 9, Value: -1}}},
		}}}}, code: errcode.ErrInvalidArg},
		{name: "too many attrs", record: &bagv1.BagStorageRecord{Sections: []*bagv1.BagSection{{BagType: 2, Items: []*bagv1.BagItem{
			{ItemConfigId: 1, Count: 1, InstanceId: 99, Identified: true, Attrs: tooManyAttrs},
		}}}}, code: errcode.ErrBagQuotaExceeded},
		{name: "oversize blob", record: &bagv1.BagStorageRecord{}, blob: make([]byte, BagCheckpointMaxBytes+1), code: errcode.ErrBagQuotaExceeded},
	}
	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			wantBizCode(t, uc.SaveCheckpoint(ctx, 1, 1, tc.record, tc.blob, 0, DSCallerIdentity{}), tc.code, tc.name)
		})
	}
	if repo.checkpointCalls != 1 {
		t.Fatalf("非法 checkpoint 不得触达数据层: calls=%d", repo.checkpointCalls)
	}

	// GetSections 只允许后端驻留段。
	wantBizCode(t, func() error { _, err := uc.GetSections(ctx, 1, []uint32{0}); return err }(),
		errcode.ErrBagSectionNotAllowed, "查随身段")
	if _, err := uc.GetSections(ctx, 1, []uint32{1, 100}); err != nil {
		t.Fatalf("查仓库/活动段应放行: %v", err)
	}
}

// ── owner 授权门(五要件②,phase 2)────────────────────────────────────────────

// bagOwnerTestEntry 构造一条最小合法 journal 条目(授权门测试专用)。
func bagOwnerTestEntry() *bagv1.BagJournalEntry {
	return &bagv1.BagJournalEntry{
		JournalSeq: 1, BagType: 0, IdempotencyKey: "k1",
		Op: &bagv1.BagJournalEntry_PickupGrant{PickupGrant: &bagv1.PickupGrantOp{
			Items: []*bagv1.BagItem{{ItemConfigId: 10001, Count: 1}},
		}},
	}
}

// TestBagOwnerAuthorizationFailClosed 未配置授权器且未显式跳过 → 三条写路径全 fail-closed,
// 不触 repo(生产缺 owner_addr 不得放行任何写)。
func TestBagOwnerAuthorizationFailClosed(t *testing.T) {
	repo := &fakeBagRepo{}
	cfg := conf.BagConf{MaxJournalBatch: 4, MaxItemsPerOp: 2,
		SectionCapacities: []conf.BagSectionCapacityRule{{BagType: 1, Capacity: 10}}}
	uc := NewBagUsecase(repo, cfg) // AllowUnverifiedOwner 缺省 false,未注入授权器

	if _, _, _, err := uc.LoadBag(context.Background(), 100, 1, DSCallerIdentity{}); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("LoadBag want ErrUnavailable, got %v", err)
	}
	if _, err := uc.AppendJournal(context.Background(), 100, 1, []*bagv1.BagJournalEntry{bagOwnerTestEntry()}, DSCallerIdentity{}); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("AppendJournal want ErrUnavailable, got %v", err)
	}
	if err := uc.SaveCheckpoint(context.Background(), 100, 1, &bagv1.BagStorageRecord{}, nil, 0, DSCallerIdentity{}); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("SaveCheckpoint want ErrUnavailable, got %v", err)
	}
	if repo.appendCalls != 0 {
		t.Fatalf("fail-closed 不得触 repo, appendCalls=%d", repo.appendCalls)
	}
}

// TestBagOwnerAuthorizationDenied 授权器拒(epoch 不符/未 ADMITTED/失租)→ 错误原样上抛,不触 repo。
func TestBagOwnerAuthorizationDenied(t *testing.T) {
	uc, repo := newBagUsecaseForTest()
	auth := &fakeOwnerAuthorizer{err: errcode.New(errcode.ErrBagEpochFenced, "epoch mismatch")}
	uc.SetOwnerAuthorizer(auth)

	if _, err := uc.AppendJournal(context.Background(), 100, 1, []*bagv1.BagJournalEntry{bagOwnerTestEntry()}, DSCallerIdentity{}); errcode.As(err) != errcode.ErrBagEpochFenced {
		t.Fatalf("want ErrBagEpochFenced, got %v", err)
	}
	if auth.calls != 1 || repo.appendCalls != 0 {
		t.Fatalf("授权拒后不得触 repo: auth=%d repo=%d", auth.calls, repo.appendCalls)
	}
}

// TestBagOwnerAuthorizationPassed 授权通过 → 写路径照常委托 repo;授权器注入后即使
// AllowUnverifiedOwner=true 也必须逐调执行(显式注入优先于跳过开关)。
func TestBagOwnerAuthorizationPassed(t *testing.T) {
	uc, repo := newBagUsecaseForTest()
	auth := &fakeOwnerAuthorizer{}
	uc.SetOwnerAuthorizer(auth)

	if _, err := uc.AppendJournal(context.Background(), 100, 1, []*bagv1.BagJournalEntry{bagOwnerTestEntry()}, DSCallerIdentity{}); err != nil {
		t.Fatalf("AppendJournal err: %v", err)
	}
	if auth.calls != 1 || repo.appendCalls != 1 {
		t.Fatalf("授权通过应触 repo: auth=%d repo=%d", auth.calls, repo.appendCalls)
	}
}
