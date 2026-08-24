// Package biz — 背包域用例(bag-domain.md;phase 1 由 inventory 进程承载)。
//
// 职责:请求形状校验(批量上限 / 段类型合法性 / 幂等键形状)→ 委托 BagRepo
// (epoch fencing / generation 校验 / 幂等去重 / 同事务段变更全部在数据层事务内完成)。
//
// 五要件分工(CLAUDE.md §9.6):
//
//	①身份 = 传输层(内网直连,service 层拒带玩家 JWT 的调用);
//	②owner 授权 = phase 1 只迁读,写权威切换(phase 2)前由 owner authority 校验接入,
//	  本层预留 epoch 透传;③fencing + ⑤审计 = 数据层;④额度 = 本层配置 + 数据层滑窗封顶。
package biz

import (
	"context"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	bagv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/bag/v1"

	"github.com/luyuancpp/pandora/services/economy/inventory/internal/conf"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/data"
)

const (
	// checkpoint 是被攻破 DS 可写的 MEDIUMBLOB；业务上随身组最多三个段，空间恢复
	// 的绝对格位上限与 UE RecoveryAbsoluteMaxGridCount 保持一致。整体字节闸再兜住
	// protobuf unknown fields 与单元素深度，避免合法行数下的单行无界增长。
	// BagCheckpointMaxBytes 是 service 在 proto.Marshal 前就必须使用的公共字节闸。
	BagCheckpointMaxBytes     = 256 * 1024
	bagCheckpointMaxSections  = 3
	bagCheckpointMaxGridCount = 4096
	bagMaxAttrsPerItem        = 64
	bagMaxUEItemCount         = uint32(1<<31 - 1)
	bagMaxUEInstanceID        = uint64(1<<63 - 1)
	// 装备词条跨 DS/UE 的硬安全包络：3/9 是 flat，7 是 basis points。
	// 正式 equipment_affix 当前远低于此上限；包络只防受损 DS 造未知/负数/
	// 超大值，不把 checkpoint 上的历史合法 roll 绑死到当前调优区间。
	bagMaxFlatEquipmentAttrValue = int64(1_000_000)
	bagMaxMoveSpeedBasisPoints   = int64(10_000) // 单词条最多 +100% 倍率。
)

func validBagGameplayAttr(attrID uint32, value int64) bool {
	switch attrID {
	case 3, 9: // Atk / Defense flat
		return value > 0 && value <= bagMaxFlatEquipmentAttrValue
	case 7: // MoveSpeedRate basis points
		return value > 0 && value <= bagMaxMoveSpeedBasisPoints
	default:
		return false
	}
}

// OwnerAuthorizer 校验并解析写授权(五要件②,查询 §9.22 owner authority;phase 2)。
//
// 判定:玩家当前 owner 记录必须 ADMITTED、租约在效、且(callerPod 非空时)
// record.target 与调用方 DS 凭据身份(pod+uid,五要件①经 DSCallbackGuard 验签)全等;
// claimedEpoch 非 0 时还须等于当前 epoch(票据携带 epoch 的 contract 阶段增强)。
// 返回解析出的当前 owner_epoch(0 号请求由服务端代填,DS 无需预知 epoch)。
// epoch/身份不符 / 未 ADMITTED / 失租 → ErrBagEpochFenced(调用方停写重查);
// 查询失败 / UNKNOWN → ErrUnavailable(fail-closed,原批重试,禁冒充有权)。
type OwnerAuthorizer interface {
	AuthorizeOwnerWrite(ctx context.Context, playerID, claimedEpoch uint64, callerPod, callerUID string) (uint64, error)
}

// DSCallerIdentity 调用方 DS 凭据身份(service 层经 DSCallbackGuard 验签后注入;
// 五要件①。Pod 空 = 未验身份(guard off/dev),授权器按其契约 fail-closed 或降级)。
type DSCallerIdentity struct {
	Pod string
	UID string
}

// CapacityCharger 容量购买扣费抽象(§5.3 两步 saga 第①步;trade 库,
// MySQLInventoryRepo 实现)。同 (bagType, tier) 重试幂等零扣费。
type CapacityCharger interface {
	ChargeBagCapacity(ctx context.Context, playerID uint64, bagType, tier, slots uint32, kind data.CurrencyKind, price uint64) (already bool, remainingBalance uint64, err error)
}

// BagUsecase 背包域用例。
type BagUsecase struct {
	repo data.BagRepo
	cfg  conf.BagConf

	// ownerAuth 五要件② owner 授权(SetOwnerAuthorizer 注入)。nil 时仅当
	// cfg.AllowUnverifiedOwner=true 放行(dev/单测),否则一切写路径 fail-closed 拒。
	ownerAuth OwnerAuthorizer

	// charger 容量购买扣费(SetCapacityCharger 注入;nil = 购买链未装配,fail-closed 拒)。
	charger CapacityCharger
}

// NewBagUsecase 构造。
func NewBagUsecase(repo data.BagRepo, cfg conf.BagConf) *BagUsecase {
	return &BagUsecase{repo: repo, cfg: cfg}
}

// SetOwnerAuthorizer 注入 owner 授权器(main 按 bag.owner_addr 装配)。
func (u *BagUsecase) SetOwnerAuthorizer(a OwnerAuthorizer) {
	u.ownerAuth = a
}

// SetCapacityCharger 注入容量购买扣费器(main 装配;经济域同进程直用 inventory repo)。
func (u *BagUsecase) SetCapacityCharger(c CapacityCharger) {
	u.charger = c
}

// authorizeOwner 对写路径统一执行五要件②并解析生效 epoch;LoadBag 也在列(它会单调
// 推进 bag_meta.owner_epoch,伪造高 epoch 的加载会把真 owner 的后续写全部围栏)。
// 未配置授权器且显式跳过(dev)时,直接采用调用方声称的 epoch(phase 1 行为)。
func (u *BagUsecase) authorizeOwner(ctx context.Context, playerID, claimedEpoch uint64, caller DSCallerIdentity) (uint64, error) {
	if u.ownerAuth == nil {
		if u.cfg.AllowUnverifiedOwner {
			return claimedEpoch, nil
		}
		return 0, errcode.New(errcode.ErrUnavailable, "owner authorizer unavailable (bag.owner_addr unset)")
	}
	return u.ownerAuth.AuthorizeOwnerWrite(ctx, playerID, claimedEpoch, caller.Pod, caller.UID)
}

// carryGroupBagType 判断随身组段(0 身上 / 2 装备栏 / 3 临时格;DS 驻留,checkpoint 覆盖)。
func carryGroupBagType(bagType uint32) bool {
	return bagType == 0 || bagType == 2 || bagType == 3
}

// knownBagType 判断已知段类型(随身组 + 后端驻留组;其余整批拒)。
func knownBagType(bagType uint32) bool {
	return carryGroupBagType(bagType) || data.IsBackendResidentBagType(bagType)
}

// LoadBag 加载随身组(owner DS checkout):快照 + journal 尾部 + 权威水位。
func (u *BagUsecase) LoadBag(ctx context.Context, playerID, ownerEpoch uint64, caller DSCallerIdentity) (snapshot []byte, tail []data.BagJournalRow, lastSeq uint64, err error) {
	if playerID == 0 {
		return nil, nil, 0, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	epoch, aerr := u.authorizeOwner(ctx, playerID, ownerEpoch, caller)
	if aerr != nil {
		return nil, nil, 0, aerr
	}
	return u.repo.LoadBag(ctx, playerID, epoch)
}

// AppendJournal 追加流水(同步入账;落库成功操作才算完成)。
func (u *BagUsecase) AppendJournal(ctx context.Context, playerID, ownerEpoch uint64, entries []*bagv1.BagJournalEntry, caller DSCallerIdentity) (uint64, error) {
	if playerID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if len(entries) == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "entries required")
	}
	if len(entries) > u.cfg.MaxJournalBatch {
		return 0, errcode.New(errcode.ErrBagQuotaExceeded,
			"batch size %d exceeds max %d", len(entries), u.cfg.MaxJournalBatch)
	}
	epoch, aerr := u.authorizeOwner(ctx, playerID, ownerEpoch, caller)
	if aerr != nil {
		return 0, aerr
	}
	for _, entry := range entries {
		if entry.GetJournalSeq() == 0 {
			return 0, errcode.New(errcode.ErrBagSeqConflict, "journal_seq required")
		}
		key := entry.GetIdempotencyKey()
		if key == "" || len(key) > 128 {
			return 0, errcode.New(errcode.ErrInvalidArg, "invalid idempotency_key seq=%d", entry.GetJournalSeq())
		}
		if !knownBagType(entry.GetBagType()) {
			return 0, errcode.New(errcode.ErrBagSectionNotAllowed,
				"unknown bag_type=%d seq=%d", entry.GetBagType(), entry.GetJournalSeq())
		}
		// 固定段代际恒 0(活动段代际由数据层对 current 校验)。
		if !data.IsActivityBagType(entry.GetBagType()) && entry.GetGeneration() != 0 {
			return 0, errcode.New(errcode.ErrInvalidArg,
				"fixed bag_type=%d must carry generation=0 seq=%d", entry.GetBagType(), entry.GetJournalSeq())
		}
		if err := u.validateOpShape(entry); err != nil {
			return 0, err
		}
	}
	return u.repo.AppendJournal(ctx, playerID, epoch, entries, u.cfg.SectionCapacityOf, u.cfg.ItemMaxStackOf, u.cfg.HourlyJournalQuota)
}

// validateOpShape 校验单条 op 的形状与目标段合法性(内容级校验在数据层应用时进行)。
func (u *BagUsecase) validateOpShape(entry *bagv1.BagJournalEntry) error {
	seq := entry.GetJournalSeq()
	switch op := entry.GetOp().(type) {
	case *bagv1.BagJournalEntry_PickupGrant:
		return u.checkItemsLen(op.PickupGrant.GetItems(), seq)
	case *bagv1.BagJournalEntry_MailClaim:
		return u.checkItemsLen(op.MailClaim.GetItems(), seq)
	case *bagv1.BagJournalEntry_Transfer:
		if !knownBagType(op.Transfer.GetToBagType()) {
			return errcode.New(errcode.ErrBagSectionNotAllowed,
				"transfer to unknown bag_type=%d seq=%d", op.Transfer.GetToBagType(), seq)
		}
		if op.Transfer.GetToBagType() == entry.GetBagType() {
			return errcode.New(errcode.ErrInvalidArg, "transfer to same bag_type=%d seq=%d", entry.GetBagType(), seq)
		}
		return u.checkItemsLen(op.Transfer.GetItems(), seq)
	case *bagv1.BagJournalEntry_Consume:
		if len(op.Consume.GetProduceItems()) > 0 && !knownBagType(op.Consume.GetProduceBagType()) {
			return errcode.New(errcode.ErrBagSectionNotAllowed,
				"produce to unknown bag_type=%d seq=%d", op.Consume.GetProduceBagType(), seq)
		}
		if err := u.checkItemsLen(op.Consume.GetConsumeItems(), seq); err != nil {
			return err
		}
		if len(op.Consume.GetProduceItems()) > 0 {
			return u.checkItemsLen(op.Consume.GetProduceItems(), seq)
		}
		return nil
	default:
		// 未知 op fail-closed(旧副本遇到新 op 不得静默应用/跳过,§9.21 混版纪律)。
		return errcode.New(errcode.ErrInvalidArg, "op required seq=%d", seq)
	}
}

func (u *BagUsecase) checkItemsLen(items []*bagv1.BagItem, seq uint64) error {
	if len(items) == 0 {
		return errcode.New(errcode.ErrInvalidArg, "items required seq=%d", seq)
	}
	if len(items) > u.cfg.MaxItemsPerOp {
		return errcode.New(errcode.ErrBagQuotaExceeded,
			"items %d exceed max %d seq=%d", len(items), u.cfg.MaxItemsPerOp, seq)
	}
	seenInstances := make(map[uint64]struct{}, len(items))
	for itemIndex, item := range items {
		if item == nil || item.GetItemConfigId() == 0 || item.GetCount() == 0 {
			return errcode.New(errcode.ErrInvalidArg,
				"invalid item seq=%d index=%d config=%d count=%d",
				seq, itemIndex, item.GetItemConfigId(), item.GetCount())
		}
		if item.GetCount() > bagMaxUEItemCount {
			return errcode.New(errcode.ErrBagQuotaExceeded,
				"item count=%d exceeds UE int32 max seq=%d index=%d", item.GetCount(), seq, itemIndex)
		}
		instanceID := item.GetInstanceId()
		if (instanceID == 0 && (item.GetIdentified() || len(item.GetAttrs()) > 0)) ||
			(!item.GetIdentified() && len(item.GetAttrs()) > 0) {
			return errcode.New(errcode.ErrInvalidArg,
				"item instance/identified attrs mismatch seq=%d index=%d", seq, itemIndex)
		}
		if instanceID != 0 {
			if instanceID > bagMaxUEInstanceID {
				return errcode.New(errcode.ErrBagQuotaExceeded,
					"instance_id=%d exceeds UE int64 max seq=%d", instanceID, seq)
			}
			if item.GetCount() != 1 {
				return errcode.New(errcode.ErrInvalidArg,
					"instance_id=%d must carry count=1 seq=%d", instanceID, seq)
			}
			if _, duplicate := seenInstances[instanceID]; duplicate {
				return errcode.New(errcode.ErrInvalidArg,
					"duplicate instance_id=%d seq=%d", instanceID, seq)
			}
			seenInstances[instanceID] = struct{}{}
		}
		if len(item.GetAttrs()) > bagMaxAttrsPerItem {
			return errcode.New(errcode.ErrBagQuotaExceeded,
				"attrs=%d exceed max %d seq=%d index=%d",
				len(item.GetAttrs()), bagMaxAttrsPerItem, seq, itemIndex)
		}
		seenAttrs := make(map[uint32]struct{}, len(item.GetAttrs()))
		for attrIndex, attr := range item.GetAttrs() {
			if attr == nil || !validBagGameplayAttr(attr.GetAttrId(), attr.GetValue()) {
				return errcode.New(errcode.ErrInvalidArg,
					"unsupported/out-of-range attr seq=%d item=%d attr=%d id=%d value=%d",
					seq, itemIndex, attrIndex, attr.GetAttrId(), attr.GetValue())
			}
			if _, duplicate := seenAttrs[attr.GetAttrId()]; duplicate {
				return errcode.New(errcode.ErrInvalidArg,
					"duplicate attr_id=%d seq=%d item=%d", attr.GetAttrId(), seq, itemIndex)
			}
			seenAttrs[attr.GetAttrId()] = struct{}{}
		}
	}
	return nil
}

// SaveCheckpoint 保存随身组快照(snapshot 已由 service 层编码为 BagStorageRecord bytes;
// record 用于段类型校验:含后端驻留段整批拒——它们的本体不归 checkpoint)。
func (u *BagUsecase) SaveCheckpoint(ctx context.Context, playerID, ownerEpoch uint64, record *bagv1.BagStorageRecord, snapshot []byte, coveredSeq uint64, caller DSCallerIdentity) error {
	if playerID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	epoch, aerr := u.authorizeOwner(ctx, playerID, ownerEpoch, caller)
	if aerr != nil {
		return aerr
	}
	if err := validateBagCheckpoint(record, snapshot); err != nil {
		return err
	}
	return u.repo.SaveCheckpoint(ctx, playerID, epoch, snapshot, coveredSeq)
}

// validateBagCheckpoint 只校验信任边界和可恢复结构，不重算 DS 权威布局/MaxStack。
// 特别地，slot 可以 >= capacity、items 可以 > capacity：这是 §3.2 journal 重放后
// “临时格超容、只出不进”的合法恢复态，服务端必须原样保存，不能把玩家资产挡在场外。
func validateBagCheckpoint(record *bagv1.BagStorageRecord, snapshot []byte) error {
	if record == nil {
		return errcode.New(errcode.ErrInvalidArg, "checkpoint snapshot required")
	}
	if len(snapshot) > BagCheckpointMaxBytes {
		return errcode.New(errcode.ErrBagQuotaExceeded,
			"checkpoint bytes %d exceed max %d", len(snapshot), BagCheckpointMaxBytes)
	}
	sections := record.GetSections()
	if len(sections) > bagCheckpointMaxSections {
		return errcode.New(errcode.ErrBagQuotaExceeded,
			"checkpoint sections %d exceed max %d", len(sections), bagCheckpointMaxSections)
	}

	seenSections := make(map[uint32]struct{}, len(sections))
	seenInstances := make(map[uint64]struct{})
	for sectionIndex, sec := range sections {
		if sec == nil {
			return errcode.New(errcode.ErrInvalidArg, "nil checkpoint section index=%d", sectionIndex)
		}
		bagType := sec.GetBagType()
		if !carryGroupBagType(bagType) {
			return errcode.New(errcode.ErrBagSectionNotAllowed,
				"checkpoint must not carry backend-resident bag_type=%d", bagType)
		}
		if _, duplicate := seenSections[bagType]; duplicate {
			return errcode.New(errcode.ErrInvalidArg, "duplicate checkpoint bag_type=%d", bagType)
		}
		seenSections[bagType] = struct{}{}
		if sec.GetGeneration() != 0 {
			return errcode.New(errcode.ErrInvalidArg,
				"fixed checkpoint bag_type=%d must carry generation=0", bagType)
		}
		// capacity=0 is accepted during mixed-version rollout; LoadBag sends authoritative
		// effective capacity separately and UE falls back to its configured base when absent.
		if sec.GetCapacity() > bagCheckpointMaxGridCount {
			return errcode.New(errcode.ErrBagQuotaExceeded,
				"checkpoint bag_type=%d capacity=%d exceeds max %d",
				bagType, sec.GetCapacity(), bagCheckpointMaxGridCount)
		}
		if len(sec.GetItems()) > bagCheckpointMaxGridCount {
			return errcode.New(errcode.ErrBagQuotaExceeded,
				"checkpoint bag_type=%d items=%d exceed max %d",
				bagType, len(sec.GetItems()), bagCheckpointMaxGridCount)
		}

		seenSlots := make(map[uint32]struct{}, len(sec.GetItems()))
		for itemIndex, item := range sec.GetItems() {
			if item == nil {
				return errcode.New(errcode.ErrInvalidArg,
					"nil checkpoint item bag_type=%d index=%d", bagType, itemIndex)
			}
			if item.GetItemConfigId() == 0 || item.GetCount() == 0 {
				return errcode.New(errcode.ErrInvalidArg,
					"invalid checkpoint item bag_type=%d index=%d config=%d count=%d",
					bagType, itemIndex, item.GetItemConfigId(), item.GetCount())
			}
			if item.GetCount() > bagMaxUEItemCount {
				return errcode.New(errcode.ErrBagQuotaExceeded,
					"checkpoint item count=%d exceeds UE int32 max bag_type=%d index=%d",
					item.GetCount(), bagType, itemIndex)
			}
			if item.GetSlot() >= bagCheckpointMaxGridCount {
				return errcode.New(errcode.ErrBagQuotaExceeded,
					"checkpoint slot=%d exceeds max %d bag_type=%d",
					item.GetSlot(), bagCheckpointMaxGridCount-1, bagType)
			}
			if _, duplicate := seenSlots[item.GetSlot()]; duplicate {
				return errcode.New(errcode.ErrInvalidArg,
					"duplicate checkpoint slot=%d bag_type=%d", item.GetSlot(), bagType)
			}
			seenSlots[item.GetSlot()] = struct{}{}

			instanceID := item.GetInstanceId()
			if (instanceID == 0 && (item.GetIdentified() || len(item.GetAttrs()) > 0)) ||
				(!item.GetIdentified() && len(item.GetAttrs()) > 0) {
				return errcode.New(errcode.ErrInvalidArg,
					"checkpoint item instance/identified attrs mismatch bag_type=%d index=%d",
					bagType, itemIndex)
			}
			if instanceID != 0 {
				if instanceID > bagMaxUEInstanceID {
					return errcode.New(errcode.ErrBagQuotaExceeded,
						"checkpoint instance_id=%d exceeds UE int64 max", instanceID)
				}
				if item.GetCount() != 1 {
					return errcode.New(errcode.ErrInvalidArg,
						"checkpoint instance_id=%d must carry count=1", instanceID)
				}
				if _, duplicate := seenInstances[instanceID]; duplicate {
					return errcode.New(errcode.ErrInvalidArg,
						"duplicate checkpoint instance_id=%d", instanceID)
				}
				seenInstances[instanceID] = struct{}{}
			}
			if len(item.GetAttrs()) > bagMaxAttrsPerItem {
				return errcode.New(errcode.ErrBagQuotaExceeded,
					"checkpoint attrs=%d exceed max %d instance_id=%d",
					len(item.GetAttrs()), bagMaxAttrsPerItem, instanceID)
			}
			seenAttrs := make(map[uint32]struct{}, len(item.GetAttrs()))
			for attrIndex, attr := range item.GetAttrs() {
				if attr == nil || !validBagGameplayAttr(attr.GetAttrId(), attr.GetValue()) {
					return errcode.New(errcode.ErrInvalidArg,
						"unsupported/out-of-range checkpoint attr instance_id=%d index=%d id=%d value=%d",
						instanceID, attrIndex, attr.GetAttrId(), attr.GetValue())
				}
				if _, duplicate := seenAttrs[attr.GetAttrId()]; duplicate {
					return errcode.New(errcode.ErrInvalidArg,
						"duplicate checkpoint attr_id=%d instance_id=%d", attr.GetAttrId(), instanceID)
				}
				seenAttrs[attr.GetAttrId()] = struct{}{}
			}
		}
	}
	return nil
}

// GetSections 读后端驻留段(仓库/活动段;活动段按 current generation 过滤)。
func (u *BagUsecase) GetSections(ctx context.Context, playerID uint64, bagTypes []uint32) ([]*bagv1.BagSection, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if len(bagTypes) == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "bag_types required")
	}
	for _, bagType := range bagTypes {
		if !data.IsBackendResidentBagType(bagType) {
			return nil, errcode.New(errcode.ErrBagSectionNotAllowed,
				"bag_type=%d is not backend-resident", bagType)
		}
	}
	return u.repo.GetSections(ctx, playerID, bagTypes, u.cfg.SectionCapacityOf)
}

// EffectiveCapacityView 一段的有效容量(LoadBag 权威下发 / 购买响应)。
type EffectiveCapacityView struct {
	BagType  uint32
	Capacity uint32
}

// CarryEffectiveCapacities 返回随身组各段有效容量(base 配置 + 已购增量;§5.3)。
// 只含服务端已配置 base 的段——未配置的段 DS 沿用客户端配置(base 全量迁服务端前的过渡)。
func (u *BagUsecase) CarryEffectiveCapacities(ctx context.Context, playerID uint64) ([]EffectiveCapacityView, error) {
	var out []EffectiveCapacityView
	for _, bagType := range []uint32{0, 2, 3} {
		base := u.cfg.SectionCapacityOf(bagType)
		if base == 0 {
			continue
		}
		extra, _, err := u.repo.GetCapacityState(ctx, playerID, bagType)
		if err != nil {
			return nil, err
		}
		eff := uint64(base) + uint64(extra)
		if eff > 0xFFFFFFFF {
			eff = 0xFFFFFFFF
		}
		out = append(out, EffectiveCapacityView{BagType: bagType, Capacity: uint32(eff)})
	}
	return out, nil
}

// CapacityPurchaseResult 一次购买的结果(幂等重放返回当前状态,Cost=0)。
type CapacityPurchaseResult struct {
	Purchases         uint32
	Extra             uint32
	EffectiveCapacity uint32
	// CurrencyKind 本次扣费币种(格容购买当前恒为金币;结构上按币种参数化,
	// 将来改成钻石扩容只需改配置,不用改协议与代码)。
	CurrencyKind data.CurrencyKind
	Cost         uint64
	Balance      uint64
}

// PurchaseCapacity 购买容量扩容(§5.3):定档 → 扣费(幂等)→ 落位(档数 CAS)。
// 幂等身份 = (player, bagType, 第 purchases+1 档);同档重试 / 双击并发收敛为单次扣费
// 单次生效。两步间崩溃由同 tier 重试收敛(①回放零扣费 → ②补应用)。
func (u *BagUsecase) PurchaseCapacity(ctx context.Context, playerID, ownerEpoch uint64, bagType uint32, caller DSCallerIdentity) (CapacityPurchaseResult, error) {
	var zero CapacityPurchaseResult
	if playerID == 0 {
		return zero, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	rule := u.cfg.CapacityPurchaseRuleOf(bagType)
	if rule == nil {
		return zero, errcode.New(errcode.ErrBagSectionNotAllowed, "bag_type=%d not purchasable", bagType)
	}
	if u.charger == nil {
		return zero, errcode.New(errcode.ErrUnavailable, "capacity charger unavailable")
	}
	if _, aerr := u.authorizeOwner(ctx, playerID, ownerEpoch, caller); aerr != nil {
		return zero, aerr
	}

	extra, purchases, err := u.repo.GetCapacityState(ctx, playerID, bagType)
	if err != nil {
		return zero, err
	}
	tier := purchases + 1
	if int(tier) > len(rule.Tiers) {
		return zero, errcode.New(errcode.ErrBagCapacityMaxed,
			"all %d tiers purchased player=%d bag=%d", len(rule.Tiers), playerID, bagType)
	}
	t := rule.Tiers[tier-1]
	if uint64(extra)+uint64(t.Slots) > uint64(rule.MaxExtra) {
		return zero, errcode.New(errcode.ErrBagCapacityMaxed,
			"extra %d+%d exceeds max_extra %d player=%d bag=%d", extra, t.Slots, rule.MaxExtra, playerID, bagType)
	}

	chargeKind := data.CurrencyGold
	charged := uint64(t.PriceGold)
	already, remainingBalance, cerr := u.charger.ChargeBagCapacity(ctx, playerID, bagType, tier, t.Slots, chargeKind, charged)
	if cerr != nil {
		return zero, cerr
	}
	newExtra, newPurchases, _, perr := u.repo.ApplyCapacityPurchase(ctx, playerID, bagType, tier, t.Slots, rule.MaxExtra)
	if perr != nil {
		// 已扣费未落位(配置中途收缩等):错误如实上抛,凭 ledger 行 + 同 tier 重试收敛/排障。
		return zero, perr
	}

	base := u.cfg.SectionCapacityOf(bagType)
	eff := uint64(base) + uint64(newExtra)
	if eff > 0xFFFFFFFF {
		eff = 0xFFFFFFFF
	}
	res := CapacityPurchaseResult{
		Purchases:         newPurchases,
		Extra:             newExtra,
		EffectiveCapacity: uint32(eff),
		CurrencyKind:      chargeKind,
		Balance:           remainingBalance,
	}
	if !already {
		res.Cost = charged
	}
	return res, nil
}

// RunJournalSweep 周期清理超保留期**且已被 checkpoint 覆盖**的流水(§9.24;多副本
// 各自跑,DELETE 幂等)。删除资格 = 覆盖水位证明,时间阈值只是附加条件:未覆盖尾部
// 是 LoadBag 唯一恢复数据,时间到期也绝不删(INC-20260722-003,谓词在 repo.SweepJournal)。
func (u *BagUsecase) RunJournalSweep(ctx context.Context, batch int) (int64, error) {
	retention := time.Duration(u.cfg.JournalRetentionDays) * 24 * time.Hour
	return u.repo.SweepJournal(ctx, retention, batch)
}
