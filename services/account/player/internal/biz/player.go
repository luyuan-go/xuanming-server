// Package biz 是 player 服务的业务逻辑层(W4 ④,2026-06-06)。
//
// 职责(docs/design/go-services.md §2.2):
//   - 玩家档案(昵称 / 等级 / 段位 mmr / 战绩计数)读写
//   - 英雄解锁池
//   - MMR 读写:写由 battle_result 经 pandora.player.update 驱动,必须幂等
//     (idempotency_key=match_id,不变量 §2);GetMMR 供 battle_result 当 MMRReader
//
// 关键不变量:
//   - UpdateMMR 幂等(同一 idempotency_key 只算一次,mmr_history uk 兜底)
//   - 档案懒创建:GetProfile / 写操作前 EnsureProfile,保证后续行存在
package biz

import (
	"context"
	"math"
	"strconv"
	"strings"

	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/configtable"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/rating"
	inventoryv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/inventory/v1"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"

	"github.com/luyuancpp/pandora/services/account/player/internal/conf"
	"github.com/luyuancpp/pandora/services/account/player/internal/data"
)

// PlayerUsecase 是 player 服务业务逻辑核心。
type PlayerUsecase struct {
	repo data.PlayerRepo
	cfg  conf.PlayerConf

	// router 是确定性 region/cell 路由器(scale-cellular-20m.md §4.2)。
	// 可为 nil:单 Cell / dev / 阶段 1~2 不分片,档案 owner 落点观测退化为不打日志(行为不变)。
	// 分片部署时由 main 经 SetCellRouter 注入,核心写(UpdateMMR)后额外打一条档案 owner 落点
	// 观测(供分片上线核对档案落点 == 玩家 owner cell,§4.2 line 142)。nil-safe。
	router *cellroute.Router

	// expPusher 把经验推送出箱行投 kafka(实时成长;nil-safe:未注入时出箱积压不丢)。
	// 由 main 经 SetExperiencePusher 注入(与 SetCellRouter 同风格)。
	expPusher ExperiencePusher

	// pushLease 推送出箱发布器的领导权来源;nil = 不选举(单进程 / 单副本 Recreate)。
	// 只在装配期由 SetPushWriterLease 写。
	pushLease PushWriterLease
	// pushLeaseHeld 上一轮领导权状态,只由发布器单 goroutine 读写(跃迁日志去重用)。
	pushLeaseHeld bool

	// expLevels 提供当前策划等级经验曲线。生产实现包装 configtable.Store 的原子快照；
	// 玩家等级经验只从 j_玩家等级经验.xlsx 读取，不保留 YAML 双数据源。
	expLevels experienceLevelSource

	// itemRules 提供道具表判定(SetEquipment 的 isEquip / slotMatch)。
	// nil = 道具表未加载 → SetEquipment fail-closed 拒绝,不放行未校验的出战预设。
	itemRules itemRuleSource

	// talentRules 提供专精表判定(SetTalents 的等级上限 / 前置 / 总消耗)。
	// nil = 专精表未加载 → SetTalents fail-closed 拒绝。
	talentRules talentRuleSource

	// attrPointRules 提供属性加点表判定(AllocateAttributePoints 的属性点键白名单)。
	// nil / 空集 = 属性加点效果表尚未发布 → 保持上线前的宽松口径(收任何非空键),
	// 不 fail-closed:整批缺表时把加点整条路径拒掉会让老客户端连点都加不了,
	// 与 talent_effect 缺表不拒同一处置(战斗数值权威本就不在这里,§9.6)。
	attrPointRules attrPointRuleSource

	// instanceOwnership 精确校验玩家是否持有指定装备实例(SetEquipment/GetLoadout)。
	// 生产实现是 inventory.CheckInstancesOwned 的 gRPC 客户端;nil = 未接线 → fail-closed 拒绝。
	instanceOwnership InstanceOwnershipChecker

	// skillCardRules 提供技能卡表判定(卡是否存在 / 等级上限 / 升级曲线)。
	// nil = 技能卡表未加载 → 升级与装配 fail-closed 拒绝。
	skillCardRules skillCardRuleSource
}

// SetConfigTables 注入启动时已成功加载、并注册整表校验器的配置表容器。
// Store 热更以整批不可变快照原子切换；单次事务先取一份快照，不会跨版本混算。
func (u *PlayerUsecase) SetConfigTables(store *configtable.Store) {
	if store == nil {
		u.expLevels = nil
		u.itemRules = nil
		u.talentRules = nil
		u.attrPointRules = nil
		u.skillCardRules = nil
		return
	}
	u.expLevels = configTableExperienceLevels{store: store}
	u.itemRules = configTableItemRules{store: store}
	u.talentRules = configTableTalentRules{store: store}
	u.attrPointRules = configTableAttrPointRules{store: store}
	u.skillCardRules = configTableSkillCardRules{store: store}
}

// InstanceOwnershipChecker 查询玩家在 inventory 域的精确实例归属(跨服务,
// SetEquipment 写入与 GetLoadout 战斗快照都复用同一权威校验)。
//
// 返回入参集合中 instance_id + item_config_id 都精确匹配的 ID 子集与权威实例快照；
// 查询失败必须返回 error 而不是空集,
// 否则调用方无法区分「一件都没有」与「没查成」,fail-closed 就无从谈起。
type InstanceOwnershipChecker interface {
	CheckInstancesOwned(ctx context.Context, playerID uint64, equipment []data.EquipmentSlot) (data.InstanceOwnershipResult, error)
}

// SetInstanceOwnershipChecker 注入精确实例归属查询实现(由 main 接 inventory gRPC 客户端)。
// 传 nil 等于关闭 SetEquipment(fail-closed),不会退化成「不校验就放行」。
func (u *PlayerUsecase) SetInstanceOwnershipChecker(c InstanceOwnershipChecker) {
	u.instanceOwnership = c
}

type experienceLevelSource interface {
	ExperienceCurve() []uint64
}

const (
	equipmentAttrAttackID        uint32 = 3
	equipmentAttrMoveSpeedRateID uint32 = 7
	equipmentAttrDefenseID       uint32 = 9

	maxEquipmentFlatAttributeValue int64 = 1_000_000
	maxEquipmentRateBasisPoints    int64 = 10_000
)

// itemRuleSource 是 SetEquipment 需要的道具表判定(生产实现读 configtable 原子快照)。
type itemRuleSource interface {
	MatchesSlot(itemConfigID uint32, slot uint32) bool
}

// talentRuleSource 是 SetTalents 需要的专精表判定(生产实现读 configtable 原子快照)。
// 返回 (逐节点消耗, 总消耗, error):总消耗用于与可用点比对,逐节点消耗随分配一起落库,
// 读取侧据此还原已花点数,不再按 Σ 等级 反推(cost_per_level≠1 时会算少)。
type talentRuleSource interface {
	ValidateAllocation(levels map[uint32]uint32) (map[uint32]uint32, uint32, error)
}

type configTableItemRules struct {
	store *configtable.Store
}

// MatchesSlot 每次调用取一份当前快照:表热更是整批原子切换,单次校验内不会跨版本。
// 表缺失时返回 false(fail-closed),等价于「这件装备不能装进这个槽」。
func (s configTableItemRules) MatchesSlot(itemConfigID uint32, slot uint32) bool {
	if s.store == nil {
		return false
	}
	tables := s.store.Tables()
	if tables == nil || tables.Item == nil {
		return false
	}
	return tables.Item.MatchesSlot(itemConfigID, slot)
}

type configTableTalentRules struct {
	store *configtable.Store
}

// ValidateAllocation 表缺失时返回错误(fail-closed),不静默按「无约束」放行。
func (s configTableTalentRules) ValidateAllocation(levels map[uint32]uint32) (map[uint32]uint32, uint32, error) {
	if s.store == nil {
		return nil, 0, errcode.New(errcode.ErrInternal, "config table store unavailable")
	}
	tables := s.store.Tables()
	if tables == nil || tables.Talent == nil {
		return nil, 0, errcode.New(errcode.ErrInternal, "talent table unavailable")
	}
	return tables.Talent.ValidateAllocation(levels)
}

// attrPointRuleSource 是 AllocateAttributePoints 需要的属性点表判定
// (生产实现读 configtable 原子快照)。
//
// 只回答「有哪几条属性点」:属性点没有主表,权威就是属性加点效果表里出现过的
// attr_point_key 集合(z_属性加点_效果.xlsx)。表里没有的键在 DS 上换不出任何战斗数值,
// 加了点等于把点数永久沉没(只能洗点找回),必须在写入边界挡住。
//
// 返回空集 = 表尚未发布,调用方保持宽松口径(见 PlayerUsecase.attrPointRules 注释)。
type attrPointRuleSource interface {
	AllocatableKeys() map[string]struct{}
}

type configTableAttrPointRules struct {
	store *configtable.Store
}

// AllocatableKeys 每次调用取一份当前快照:表热更是整批原子切换,单次校验内不会跨版本。
func (s configTableAttrPointRules) AllocatableKeys() map[string]struct{} {
	if s.store == nil {
		return nil
	}
	tables := s.store.Tables()
	if tables == nil || tables.AttrPointEffect == nil {
		return nil
	}
	keys := tables.AttrPointEffect.AttrPointKeys()
	if len(keys) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		set[k] = struct{}{}
	}
	return set
}

// skillCardRuleSource 是技能卡培养 / 更换需要的配置表判定(生产实现读 configtable 原子快照)。
//
// UpgradeCurve 返回整条曲线而不是单级价钱:实际消耗取决于事务内锁到的当前等级,
// biz 先读一次等级再算价会有 TOCTOU(见 data.PlayerRepo.UpgradeSkillCard 注释)。
type skillCardRuleSource interface {
	// CardExists 卡是否在配置表中。
	CardExists(cardID uint32) bool
	// UpgradeCurve 返回该卡的升级曲线(目标等级 → 碎片消耗)与等级上限。
	// 卡不存在或表未加载 → error(fail-closed)。
	UpgradeCurve(cardID uint32) (costByLevel map[uint32]uint32, maxLevel uint32, err error)
}

type configTableSkillCardRules struct {
	store *configtable.Store
}

func (s configTableSkillCardRules) CardExists(cardID uint32) bool {
	if s.store == nil {
		return false
	}
	tables := s.store.Tables()
	if tables == nil || tables.SkillCard == nil {
		return false // 表缺失 → 当作"这张卡不存在",fail-closed。
	}
	return tables.SkillCard.Exists(cardID)
}

// UpgradeCurve 表缺失时返回错误(fail-closed),不静默按「免费升级」放行。
func (s configTableSkillCardRules) UpgradeCurve(cardID uint32) (map[uint32]uint32, uint32, error) {
	if s.store == nil {
		return nil, 0, errcode.New(errcode.ErrInternal, "config table store unavailable")
	}
	tables := s.store.Tables()
	if tables == nil || tables.SkillCard == nil || tables.SkillCardUpgrade == nil {
		return nil, 0, errcode.New(errcode.ErrInternal, "skill card table unavailable")
	}
	card, ok := tables.SkillCard.ByID(cardID)
	if !ok {
		return nil, 0, errcode.New(errcode.ErrInvalidArg, "unknown skill card %d", cardID)
	}
	maxLevel := card.GetMaxLevel()
	curve := make(map[uint32]uint32, maxLevel)
	for level := uint32(2); level <= maxLevel; level++ {
		cost, found := tables.SkillCardUpgrade.ShardCost(card.GetRarity(), level)
		if !found {
			// 加载期 ValidateCurves 已挡过断档;这里再挡一次是因为热更会换表,
			// 而校验器只在加载时跑。缺档绝不能当免费升级。
			return nil, 0, errcode.New(errcode.ErrInternal,
				"upgrade curve missing: card=%d rarity=%d level=%d", cardID, card.GetRarity(), level)
		}
		curve[level] = cost
	}
	return curve, maxLevel, nil
}

type configTableExperienceLevels struct {
	store *configtable.Store
}

func (s configTableExperienceLevels) ExperienceCurve() []uint64 {
	if s.store == nil {
		return nil
	}
	tables := s.store.Tables()
	if tables == nil || tables.PlayerLevelExp == nil {
		return nil
	}
	return tables.PlayerLevelExp.ExperienceCurve()
}

// NewPlayerUsecase 构造。
func NewPlayerUsecase(repo data.PlayerRepo, cfg conf.PlayerConf) *PlayerUsecase {
	if cfg.BaseMMR <= 0 {
		cfg.BaseMMR = 1500
	}
	if cfg.DefaultNicknamePrefix == "" {
		cfg.DefaultNicknamePrefix = "Player_"
	}
	if cfg.MaxNicknameLen <= 0 {
		cfg.MaxNicknameLen = 32
	}
	return &PlayerUsecase{repo: repo, cfg: cfg}
}

// SetCellRouter 注入确定性 region/cell 路由器(scale-cellular-20m.md §4.2 两级架构)。
//
// nil-safe:不调用 / 传 nil 时(单 Cell / dev / 阶段 1~2),不做档案 owner 落点观测,行为与历史
// 一致。用 setter 而非构造参数,避免单 Cell 阶段调用点被迫改签名(与 matchmaker / auction /
// battle_result / friend / chat / trade / dialogue / inventory / locator / push / team 一致)。
// Router 内部读路径无锁,并发安全。
func (u *PlayerUsecase) SetCellRouter(r *cellroute.Router) {
	u.router = r
}

// defaultNickname 给新玩家生成唯一默认昵称(prefix + player_id,保证 uk_nickname 不冲突)。
func (u *PlayerUsecase) defaultNickname(playerID uint64) string {
	return u.cfg.DefaultNicknamePrefix + strconv.FormatUint(playerID, 10)
}

// ── 档案 ──────────────────────────────────────────────────────────────────────

// GetProfile 读玩家档案(懒创建:首次访问自动建默认档案)。
func (u *PlayerUsecase) GetProfile(ctx context.Context, playerID uint64) (*playerv1.PlayerProfile, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if _, err := u.repo.EnsureProfile(ctx, playerID, u.defaultNickname(playerID), u.cfg.BaseMMR); err != nil {
		return nil, err
	}
	p, found, err := u.repo.GetProfile(ctx, playerID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errcode.New(errcode.ErrPlayerNotFound, "player not found: %d", playerID)
	}
	// 经验派生字段装饰(实时成长):满级 → is_max_level + 级内经验按 0 展示;
	// 曲线未配置(功能关闭)→ 不标满级,行为与历史一致。
	p.ExpInLevel, p.IsMaxLevel = u.DecorateExperience(p.GetLevel(), p.GetExpInLevel())
	// 分池段位(2026-08-11):新客户端读 ratings；deprecated mmr #4 已由 GetProfile
	// 从 players.mmr(default 兼容投影)填好，供旧客户端继续显示。ratings 只含该玩家
	// **已有记录**的池 —— 没打过的池不占位,客户端据此区分"未定级"与
	// "已定级但分是基线"；旧客户端则保持历史语义，未打过时显示建档基线分。
	ratings, rerr := u.repo.ListRatings(ctx, playerID)
	if rerr != nil {
		return nil, rerr
	}
	if len(ratings) > 0 {
		p.Ratings = make([]*playerv1.PlayerRating, 0, len(ratings))
		for _, r := range ratings {
			p.Ratings = append(p.Ratings, &playerv1.PlayerRating{RatingPool: r.RatingPool, Mmr: int32(r.MMR)})
			// 两次仓储读取之间可能恰逢结算提交；同一响应内若已有 default
			// rating，就以它覆盖 deprecated 字段，避免新旧客户端看到两份瞬时值。
			if r.RatingPool == rating.DefaultPool {
				p.Mmr = int32(r.MMR)
			}
		}
	}
	return p, nil
}

// UpdateNickname 改昵称(懒创建档案后更新)。
func (u *PlayerUsecase) UpdateNickname(ctx context.Context, playerID uint64, nickname string) error {
	if playerID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	nickname = strings.TrimSpace(nickname)
	if nickname == "" {
		return errcode.New(errcode.ErrInvalidArg, "nickname must not be empty")
	}
	if len([]rune(nickname)) > u.cfg.MaxNicknameLen {
		return errcode.New(errcode.ErrInvalidArg, "nickname too long (max %d)", u.cfg.MaxNicknameLen)
	}
	if _, err := u.repo.EnsureProfile(ctx, playerID, u.defaultNickname(playerID), u.cfg.BaseMMR); err != nil {
		return err
	}
	return u.repo.UpdateNickname(ctx, playerID, nickname)
}

// EnsureProfileResult 是 EnsureProfileNamed 的产出。
type EnsureProfileResult struct {
	// Created true = 本次调用真的建了档(请求的 nickname 生效)。
	Created bool
	// Nickname 调用结束后该玩家**实际**的昵称(权威值)。
	Nickname string
	// Level 调用结束后的等级(刚建档为 1)。
	Level uint32
}

// EnsureProfileNamed 建档并播种昵称(账号 / 角色分离 2026-08-18,唯一调用方是 login)。
//
// **INSERT IGNORE 语义,不是 UPSERT**:档案已存在就原样返回既有值,一个字都不改。
// 这条纪律是「角色名 = 账号名」能安全落地的前提 —— login 每次登录都会尽力播种
// (补上之前因 player 不可达而漏掉的),若改成覆盖式,玩家将来自己改的名字会在
// 下一次登录被账号名冲掉。
//
// nickname 为空 → 用本服务的默认前缀名(Player_<player_id>),即本 RPC 出现之前的行为。
func (u *PlayerUsecase) EnsureProfileNamed(ctx context.Context, playerID uint64, nickname string) (EnsureProfileResult, error) {
	if playerID == 0 {
		return EnsureProfileResult{}, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	seed := strings.TrimSpace(nickname)
	if seed == "" {
		seed = u.defaultNickname(playerID)
	} else if len([]rune(seed)) > u.cfg.MaxNicknameLen {
		return EnsureProfileResult{}, errcode.New(errcode.ErrInvalidArg,
			"nickname too long (max %d)", u.cfg.MaxNicknameLen)
	}

	created, err := u.repo.EnsureProfile(ctx, playerID, seed, u.cfg.BaseMMR)
	if err != nil {
		return EnsureProfileResult{}, err
	}

	// 无论建没建成都回读一次:
	//   - created=true  → 回读拿等级等派生值;
	//   - created=false → 可能是「早就有档案」,也可能是**昵称撞了 uk_nickname 被
	//     INSERT IGNORE 静默跳过**。这两种必须靠回读区分,不能默认成前者 —— 后者下
	//     该玩家至此仍然没有档案,谎报成功会让 login 以为播种完成、再也不重试。
	p, found, gerr := u.repo.GetProfile(ctx, playerID)
	if gerr != nil {
		return EnsureProfileResult{}, gerr
	}
	if !found {
		// 建档没成功且档案确实不存在 = 昵称被别人占了(唯一可能的静默失败)。
		// 报明确错误码,由 login 决定降级策略(它的做法:记日志放弃播种,不阻断登录)。
		return EnsureProfileResult{}, errcode.New(errcode.ErrPlayerNicknameTaken,
			"ensure profile player=%d skipped: nickname %q already taken", playerID, seed)
	}
	level := p.GetLevel()
	if level < 0 {
		level = 0
	}
	return EnsureProfileResult{
		Created:  created,
		Nickname: p.GetNickname(),
		Level:    uint32(level),
	}, nil
}

// MaxPlayerNamesPerQuery 是 GetPlayerNames 单次可查的角色数上限(§9.18 读取侧上限)。
//
// 200 的依据:Hub 单实例 500 人,DS 按批拉名字时 3 批以内覆盖满场;再大会让单次
// IN 查询与响应体都失控。刻意**不做隐式分页**——超出直接截断并让调用方自己分批,
// 服务端悄悄少返回会让调用方以为"这些人就是没有档案"。
const MaxPlayerNamesPerQuery = 200

// GetPlayerNames 批量反查角色显示名(Hub DS 头顶铭牌用,2026-08-18)。
//
// 只返回查到的角色:请求里有、结果里没有 = 该角色无档案,调用方应保留旧值 / 走兜底。
// 用空串占位会让 DS 把一个真名字覆盖成空,铭牌反而退化。
func (u *PlayerUsecase) GetPlayerNames(ctx context.Context, playerIDs []uint64) ([]PlayerName, error) {
	if len(playerIDs) == 0 {
		return nil, nil
	}
	// 去重 + 去 0 + 截断。去重是因为调用方(DS 进场批)很容易把同一个人带两次,
	// 重复项会白白撑大 IN 列表并挤掉截断额度。
	seen := make(map[uint64]struct{}, len(playerIDs))
	unique := make([]uint64, 0, len(playerIDs))
	for _, id := range playerIDs {
		if id == 0 {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
		if len(unique) >= MaxPlayerNamesPerQuery {
			break
		}
	}
	if len(unique) == 0 {
		return nil, nil
	}

	found, err := u.repo.ListNicknames(ctx, unique)
	if err != nil {
		return nil, err
	}
	// 按请求顺序回填,让同一批请求的响应顺序稳定(调用方可直接顺序消费)。
	out := make([]PlayerName, 0, len(found))
	for _, id := range unique {
		if nickname, ok := found[id]; ok {
			out = append(out, PlayerName{PlayerID: id, Nickname: nickname})
		}
	}
	return out, nil
}

// PlayerName 是「角色实体 → 显示名」的最小映射(biz 层结构,service 再翻成 proto)。
type PlayerName struct {
	PlayerID uint64
	Nickname string
}

// ── 英雄 ──────────────────────────────────────────────────────────────────────

// ListHeroes 列出玩家已解锁英雄。
func (u *PlayerUsecase) ListHeroes(ctx context.Context, playerID uint64) ([]uint32, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	return u.repo.ListHeroes(ctx, playerID)
}

// UnlockHero 解锁英雄(幂等:已拥有返回 ErrPlayerHeroAlreadyOwn)。
func (u *PlayerUsecase) UnlockHero(ctx context.Context, playerID uint64, heroID uint32, source string) error {
	if playerID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if heroID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "hero_id required")
	}
	if _, err := u.repo.EnsureProfile(ctx, playerID, u.defaultNickname(playerID), u.cfg.BaseMMR); err != nil {
		return err
	}
	already, err := u.repo.UnlockHero(ctx, playerID, heroID, source)
	if err != nil {
		return err
	}
	if already {
		return errcode.New(errcode.ErrPlayerHeroAlreadyOwn, "hero already owned: player=%d hero=%d", playerID, heroID)
	}
	return nil
}

// ── MMR ──────────────────────────────────────────────────────────────────────

// GetMMR 读玩家在某段位池下的分(该池无记录 → 返回 (BaseMMR, false),不创建行)。
//
// 第二个返回值是 found:false = 该池没打过。调用方必须能区分"没定级"与"分刚好等于
// 基线",否则未定级玩家会被当成已定级展示(battle_result 算 Elo 时两者等价,都用基线
// 当期望胜率输入;但客户端展示不同)。
func (u *PlayerUsecase) GetMMR(ctx context.Context, playerID uint64, ratingPool string) (int, bool, error) {
	if playerID == 0 {
		return 0, false, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	pool := rating.Normalize(ratingPool)
	mmr, found, err := u.repo.GetMMR(ctx, playerID, pool)
	if err != nil {
		return 0, false, err
	}
	if !found {
		return u.cfg.BaseMMR, false, nil
	}
	return mmr, true, nil
}

// ListRatings 列出该玩家已有记录的全部池段位分(供 GetProfile 组装客户端可见结构)。
func (u *PlayerUsecase) ListRatings(ctx context.Context, playerID uint64) ([]data.PlayerRating, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	return u.repo.ListRatings(ctx, playerID)
}

// UpdateMMR 幂等改 MMR + 战绩计数(idempotency_key 一般是 match_id,不变量 §2)。
// 返回 (新 MMR, 是否幂等命中, error)。
func (u *PlayerUsecase) UpdateMMR(ctx context.Context, playerID uint64, delta int32, reason, idempotencyKey, ratingPool string) (int, bool, error) {
	if playerID == 0 {
		return 0, false, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if idempotencyKey == "" {
		return 0, false, errcode.New(errcode.ErrInvalidArg, "idempotency_key required")
	}

	if _, err := u.repo.EnsureProfile(ctx, playerID, u.defaultNickname(playerID), u.cfg.BaseMMR); err != nil {
		return 0, false, err
	}

	incBattle, incWin := battleFlags(reason)
	// 段位池归一化在**写入侧**做一次,与读侧 GetMMR 同一函数,杜绝写 default / 读 "" 的分裂。
	pool := rating.Normalize(ratingPool)
	newMMR, already, err := u.repo.ApplyMMRChange(ctx, data.MMRChange{
		PlayerID:       playerID,
		IdempotencyKey: idempotencyKey,
		RatingPool:     pool,
		Baseline:       u.cfg.BaseMMR,
		Delta:          delta,
		Reason:         reason,
		Floor:          u.cfg.MMRFloor,
		IncBattle:      incBattle,
		IncWin:         incWin,
	})
	if err != nil {
		return 0, false, err
	}
	if already {
		// INFO 而非 DEBUG:幂等命中罕见(重投/重放的唯一证据),与 inventory *_idempotent_hit 同口径。
		plog.With(ctx).Infow("msg", "update_mmr_idempotent_hit",
			"player_id", playerID, "idempotency_key", idempotencyKey, "new_mmr", newMMR, "rating_pool", pool)
		return newMMR, true, nil
	}
	// INFO:段位入账是每玩家每局一次的资产变更台账(「打完段位没变」必须能按 player_id 正查,
	// 上游 battle_result 只有出箱台账,入账终点在这里)。
	plog.With(ctx).Infow("msg", "update_mmr_applied",
		"player_id", playerID, "delta", delta, "reason", reason, "new_mmr", newMMR, "rating_pool", pool)
	// 分片:档案与分池段位分都是 owner 数据,锁定玩家 owner cell(ProfileShardKey=player_id,
	// §4.2 line 142)。router 为 nil(单 Cell)→ 不打,行为与历史一致。
	u.logProfilePlacement(ctx, playerID, "update_mmr")
	return newMMR, false, nil
}

// ── 出战养成(选英雄 / 加点 / 出战快照)──────────────────────────────────────────
//
// 边界(docs/design/ds-arch.md §0):这里只管大厅态持久化与配置,纯战斗内逻辑(技能/出装/
// 道具即时使用)走 UE GAS,不经 gRPC。GetLoadout 提供"开战前快照",供匹配/进战时下发。

// SelectHero 设定出战英雄。
//   - 功能开关 HeroSelectionEnabled=false → ErrPlayerFeatureDisabled(demo 阶段可跳过)
//   - 英雄未解锁 → ErrPlayerHeroLocked(只能选已拥有英雄)
func (u *PlayerUsecase) SelectHero(ctx context.Context, playerID uint64, heroID uint32) error {
	if playerID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if heroID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "hero_id required")
	}
	if !u.cfg.HeroSelectionEnabled {
		return errcode.New(errcode.ErrPlayerFeatureDisabled, "hero selection disabled")
	}
	if _, err := u.repo.EnsureProfile(ctx, playerID, u.defaultNickname(playerID), u.cfg.BaseMMR); err != nil {
		return err
	}
	owned, err := u.repo.IsHeroOwned(ctx, playerID, heroID)
	if err != nil {
		return err
	}
	if !owned {
		return errcode.New(errcode.ErrPlayerHeroLocked, "hero not owned: player=%d hero=%d", playerID, heroID)
	}
	if err := u.repo.SetActiveHero(ctx, playerID, heroID); err != nil {
		return err
	}
	plog.With(ctx).Debugw("msg", "select_hero", "player_id", playerID, "hero_id", heroID)
	return nil
}

// GetActiveHero 读出战英雄(未选定 → 返回 0)。
func (u *PlayerUsecase) GetActiveHero(ctx context.Context, playerID uint64) (uint32, error) {
	if playerID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	return u.repo.GetActiveHero(ctx, playerID)
}

// GrantAttributePoints 幂等授予可分配点(来源:升级 / 活动,idempotency_key 防重复授予)。
func (u *PlayerUsecase) GrantAttributePoints(ctx context.Context, playerID uint64, points int32, idempotencyKey string) (int, error) {
	if playerID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if points <= 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "points must be positive")
	}
	if idempotencyKey == "" {
		return 0, errcode.New(errcode.ErrInvalidArg, "idempotency_key required")
	}
	if _, err := u.repo.EnsureProfile(ctx, playerID, u.defaultNickname(playerID), u.cfg.BaseMMR); err != nil {
		return 0, err
	}
	unspent, already, err := u.repo.GrantAttributePoints(ctx, playerID, points, idempotencyKey)
	if err != nil {
		return 0, err
	}
	if already {
		plog.With(ctx).Debugw("msg", "grant_attr_idempotent_hit",
			"player_id", playerID, "idempotency_key", idempotencyKey, "unspent", unspent)
	}
	return unspent, nil
}

// AllocateAttributePoints 分配属性点(点数不足 → ErrPlayerInsufficientPoints)。
func (u *PlayerUsecase) AllocateAttributePoints(ctx context.Context, playerID uint64, allocs []data.AttrAllocation) (int, error) {
	if playerID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if len(allocs) == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "allocations required")
	}
	// 逐项校验并做溢出安全的累计:
	//   - Points 必须为正(只增点);
	//   - 同一 attr_key 的累计增量、以及所有增量之和都不得超过数据库有符号 INT 列上界
	//     (player_attributes.points / unspent_attr_points 均为 INT,上界 MaxInt32)。
	// 用 int64 累加并在每步与上界比较,单值 <= MaxInt32,累加前必然 < 2*MaxInt32,不会 int64 溢出。
	// 这里只堵「请求级」越界(总和 / 单键增量),列「当前值 + 增量」越界由 repo 在事务内权威兜底。
	//
	// 另外校验 attr_key 属于权威属性点集合(z_属性加点_效果.xlsx):表里没有的键在 DS 上
	// 换不出任何战斗数值,加了点就是把点数永久沉没(只能洗点找回),而且从协议上看
	// 完全成功。表尚未发布时 allocatable 为空,保持上线前的宽松口径。
	allocatable := u.allocatableAttrKeys()
	perKey := make(map[string]int64, len(allocs))
	var sum int64
	for _, a := range allocs {
		if a.Key == "" {
			return 0, errcode.New(errcode.ErrInvalidArg, "attr_key must not be empty")
		}
		if allocatable != nil {
			if _, ok := allocatable[a.Key]; !ok {
				return 0, errcode.New(errcode.ErrInvalidArg,
					"unknown attr_key %q (可加点属性由 角色/z_属性加点_效果.xlsx 定义)", a.Key)
			}
		}
		if a.Points <= 0 {
			return 0, errcode.New(errcode.ErrInvalidArg, "points must be positive: %s", a.Key)
		}
		perKey[a.Key] += int64(a.Points)
		if perKey[a.Key] > math.MaxInt32 {
			return 0, errcode.New(errcode.ErrInvalidArg, "attr %s allocation out of range", a.Key)
		}
		sum += int64(a.Points)
		if sum > math.MaxInt32 {
			// 总和超过 INT 列上界(必然 >= 任何可能的 unspent)→ 点数不足,零写入。
			return 0, errcode.New(errcode.ErrPlayerInsufficientPoints, "total allocation out of range")
		}
	}
	if _, err := u.repo.EnsureProfile(ctx, playerID, u.defaultNickname(playerID), u.cfg.BaseMMR); err != nil {
		return 0, err
	}
	return u.repo.AllocateAttributePoints(ctx, playerID, allocs)
}

// allocatableAttrKeys 取权威属性点键集合;nil = 表未接线 / 未发布(调用方不收紧)。
func (u *PlayerUsecase) allocatableAttrKeys() map[string]struct{} {
	if u.attrPointRules == nil {
		return nil
	}
	return u.attrPointRules.AllocatableKeys()
}

// ResetAttributes 洗点(已分配点全退回可分配点)。
func (u *PlayerUsecase) ResetAttributes(ctx context.Context, playerID uint64) (int, error) {
	if playerID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if _, err := u.repo.EnsureProfile(ctx, playerID, u.defaultNickname(playerID), u.cfg.BaseMMR); err != nil {
		return 0, err
	}
	return u.repo.ResetAttributes(ctx, playerID)
}

// GetAttributes 读已分配属性点 + 未分配点。
func (u *PlayerUsecase) GetAttributes(ctx context.Context, playerID uint64) ([]data.AttrPoint, int, error) {
	if playerID == 0 {
		return nil, 0, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	return u.repo.GetAttributes(ctx, playerID)
}

// ── 出战装备预设 / 天赋树 ──────────────────────────────────────────────────────
//
// 边界(ds-arch.md §0.5):装备预设 / 天赋是大厅态持久化,开战前转成初始 GameplayEffect;
// 战斗内买装 / 换装 / 用道具走 UE GAS,不经 gRPC。

// SetEquipment 全量替换出战装备预设(功能开关关闭 → ErrPlayerFeatureDisabled;
// 槽位/实例重复、非装备、部位不匹配 → ErrInvalidArg;实例未持有 → ErrPermissionDeny)。
//
// 权威校验三项(2026-07-25 补齐,原 2026-06-17 审查 TODO):
//  1. isEquip(item)      —— 道具表 equip_slot > 0 才是装备;
//  2. slotMatch(item,slot) —— 道具表 equip_slot 必须与提交的槽位号完全一致;
//  3. ownEquipmentInstance(player,instance,item) —— 经 inventory.CheckInstancesOwned
//     精确确认该 instance_id 当前归属玩家且配置 ID 一致。
//
// 前两项读 configtable 内存快照(零 RPC),第三项跨服务查 inventory。任一依赖缺失一律
// fail-closed 拒绝:GetLoadout 会把预设转成 Battle DS 的初始 GameplayEffect,
// 校验链不完整时放行 = 客户端可给自己配任意装备。
func (u *PlayerUsecase) SetEquipment(ctx context.Context, playerID uint64, slots []data.EquipmentSlot) error {
	if playerID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if !u.cfg.LoadoutCustomizeEnabled {
		return errcode.New(errcode.ErrPlayerFeatureDisabled, "loadout customize disabled")
	}
	seen := make(map[uint32]struct{}, len(slots))
	seenInstances := make(map[uint64]struct{}, len(slots))
	for _, s := range slots {
		if s.ItemConfigID == 0 {
			return errcode.New(errcode.ErrInvalidArg, "item_config_id required for slot %d", s.Slot)
		}
		if s.InstanceID == 0 {
			return errcode.New(errcode.ErrInvalidArg, "instance_id required for slot %d", s.Slot)
		}
		// 槽位号与道具表「装备部位」列同一编号空间,该列约定 0 = 不可穿戴,
		// 因此预设里的 slot 必须 >= 1:slot 0 永远匹配不到任何装备,只会变成一条恒失败的记录。
		if s.Slot == 0 {
			return errcode.New(errcode.ErrInvalidArg, "slot must be positive (0 means not equippable in item table)")
		}
		if _, dup := seen[s.Slot]; dup {
			return errcode.New(errcode.ErrInvalidArg, "duplicate slot %d", s.Slot)
		}
		seen[s.Slot] = struct{}{}
		if _, dup := seenInstances[s.InstanceID]; dup {
			return errcode.New(errcode.ErrInvalidArg, "duplicate instance_id %d", s.InstanceID)
		}
		seenInstances[s.InstanceID] = struct{}{}
	}
	if err := u.validateEquipmentAgainstConfig(slots); err != nil {
		return err
	}
	if _, err := u.validateEquipmentOwnership(ctx, playerID, slots); err != nil {
		return err
	}
	if _, err := u.repo.EnsureProfile(ctx, playerID, u.defaultNickname(playerID), u.cfg.BaseMMR); err != nil {
		return err
	}
	if err := u.repo.SetEquipment(ctx, playerID, slots); err != nil {
		return err
	}
	plog.With(ctx).Debugw("msg", "set_equipment", "player_id", playerID, "slots", len(slots))
	return nil
}

// validateEquipmentAgainstConfig 用道具表判定「是不是装备 + 部位对不对」(第 1、2 项)。
// 表未加载时 fail-closed:宁可拒掉这次改装,也不放行一份没校验过的预设。
func (u *PlayerUsecase) validateEquipmentAgainstConfig(slots []data.EquipmentSlot) error {
	if len(slots) == 0 {
		return nil
	}
	rules := u.itemRules
	if rules == nil {
		return errcode.New(errcode.ErrInternal, "item config table unavailable")
	}
	for _, s := range slots {
		if !rules.MatchesSlot(s.ItemConfigID, s.Slot) {
			// 不区分「不存在 / 不是装备 / 部位不符」三种原因:对外统一是一条非法预设,
			// 细分只会给探测配置表提供信号。日志侧仍可按 item/slot 定位。
			return errcode.New(errcode.ErrInvalidArg,
				"item %d cannot be equipped in slot %d", s.ItemConfigID, s.Slot)
		}
	}
	return nil
}

// validateEquipmentOwnership 经 inventory 系统 RPC 确认玩家持有全部待装备唯一实例(第 3 项)。
//
// 查询失败(依赖不可用 / 超时)按 §9.22 fail-closed:返回错误让客户端重试,
// 绝不把「查不到」当成「持有」。
func (u *PlayerUsecase) validateEquipmentOwnership(
	ctx context.Context, playerID uint64, slots []data.EquipmentSlot,
) (data.InstanceOwnershipResult, error) {
	if len(slots) == 0 {
		return data.InstanceOwnershipResult{}, nil
	}
	checker := u.instanceOwnership
	if checker == nil {
		return data.InstanceOwnershipResult{}, errcode.New(errcode.ErrInternal, "instance ownership checker unavailable")
	}

	owned, err := checker.CheckInstancesOwned(ctx, playerID, slots)
	if err != nil {
		return data.InstanceOwnershipResult{}, err
	}
	want := make(map[uint64]uint32, len(slots))
	for _, slot := range slots {
		want[slot.InstanceID] = slot.ItemConfigID
	}
	ownedSet := make(map[uint64]struct{}, len(owned.OwnedInstanceIDs))
	for _, id := range owned.OwnedInstanceIDs {
		if id == 0 || want[id] == 0 {
			return data.InstanceOwnershipResult{}, errcode.New(errcode.ErrInternal,
				"inventory returned unexpected owned instance id=%d player=%d", id, playerID)
		}
		if _, duplicate := ownedSet[id]; duplicate {
			return data.InstanceOwnershipResult{}, errcode.New(errcode.ErrInternal,
				"inventory returned duplicate owned instance id=%d player=%d", id, playerID)
		}
		ownedSet[id] = struct{}{}
	}
	for _, s := range slots {
		if _, ok := ownedSet[s.InstanceID]; !ok {
			return data.InstanceOwnershipResult{}, errcode.New(errcode.ErrPermissionDeny,
				"equipment instance %d (item %d) not owned by player %d",
				s.InstanceID, s.ItemConfigID, playerID)
		}
	}
	return owned, nil
}

// GetEquipment 读出战装备预设。
func (u *PlayerUsecase) GetEquipment(ctx context.Context, playerID uint64) ([]data.EquipmentSlot, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	return u.repo.GetEquipment(ctx, playerID)
}

// validateEquipmentForBattleSnapshot 在 GetLoadout 交付战斗初始效果前重新核验持久化预设。
//
// SetEquipment 的 inventory 查询与 player 落库跨两个服务，不可能共享 MySQL 事务；实例可能在
// 两步之间或预设保存后被其它权威路径转移。开战前再批量查一次 exact pair，确保陈旧预设不会
// 继续给 Battle DS 产生效果。000006 前只按 item_config_id 保存的旧行无法证明是哪一件实例，
// 允许 GetEquipment 展示给玩家重选，但这里必须 fail-closed。
func (u *PlayerUsecase) validateEquipmentForBattleSnapshot(
	ctx context.Context, playerID uint64, slots []data.EquipmentSlot,
) (map[uint64]data.OwnedEquipmentInstance, error) {
	seenSlots := make(map[uint32]struct{}, len(slots))
	seenInstances := make(map[uint64]struct{}, len(slots))
	for _, s := range slots {
		if s.InstanceID == 0 {
			return nil, errcode.New(errcode.ErrInvalidState,
				"legacy equipment preset requires exact instance selection player=%d slot=%d item=%d",
				playerID, s.Slot, s.ItemConfigID)
		}
		if _, duplicate := seenSlots[s.Slot]; duplicate {
			return nil, errcode.New(errcode.ErrInvalidState,
				"stored equipment has duplicate slot player=%d slot=%d", playerID, s.Slot)
		}
		seenSlots[s.Slot] = struct{}{}
		if _, duplicate := seenInstances[s.InstanceID]; duplicate {
			return nil, errcode.New(errcode.ErrInvalidState,
				"stored equipment has duplicate instance player=%d instance=%d", playerID, s.InstanceID)
		}
		seenInstances[s.InstanceID] = struct{}{}
	}
	if err := u.validateEquipmentAgainstConfig(slots); err != nil {
		return nil, err
	}
	owned, err := u.validateEquipmentOwnership(ctx, playerID, slots)
	if err != nil {
		return nil, err
	}
	if len(slots) == 0 {
		return map[uint64]data.OwnedEquipmentInstance{}, nil
	}

	// 滚动升级期旧 inventory 仍只回 OwnedInstanceIDs。这足以保护 SetEquipment 新写，
	// 但不足以生成含鉴定词条的战斗快照：GetLoadout 必须在详情缺失时 fail-closed。
	want := make(map[uint64]uint32, len(slots))
	for _, slot := range slots {
		want[slot.InstanceID] = slot.ItemConfigID
	}
	details := make(map[uint64]data.OwnedEquipmentInstance, len(owned.OwnedInstances))
	for _, inst := range owned.OwnedInstances {
		itemConfigID, requested := want[inst.InstanceID]
		if inst.InstanceID == 0 || !requested || itemConfigID != inst.ItemConfigID {
			return nil, errcode.New(errcode.ErrInternal,
				"inventory returned non-exact instance detail player=%d instance=%d item=%d",
				playerID, inst.InstanceID, inst.ItemConfigID)
		}
		if _, duplicate := details[inst.InstanceID]; duplicate {
			return nil, errcode.New(errcode.ErrInternal,
				"inventory returned duplicate instance detail player=%d instance=%d", playerID, inst.InstanceID)
		}
		if !inst.Identified && len(inst.Attributes) != 0 {
			return nil, errcode.New(errcode.ErrInternal,
				"unidentified instance returned attributes player=%d instance=%d", playerID, inst.InstanceID)
		}
		if inst.Identified && len(inst.Attributes) == 0 {
			return nil, errcode.New(errcode.ErrInternal,
				"identified instance missing attributes player=%d instance=%d", playerID, inst.InstanceID)
		}
		seenAttrs := make(map[uint32]struct{}, len(inst.Attributes))
		for _, attr := range inst.Attributes {
			if _, duplicate := seenAttrs[attr.AttrID]; duplicate {
				return nil, errcode.New(errcode.ErrInternal,
					"instance detail has duplicate attr player=%d instance=%d attr=%d",
					playerID, inst.InstanceID, attr.AttrID)
			}
			seenAttrs[attr.AttrID] = struct{}{}
			maxValue, allowed := equipmentAttributeMaxValue(attr.AttrID)
			if !allowed {
				return nil, errcode.New(errcode.ErrInternal,
					"instance detail has unsupported attr player=%d instance=%d attr=%d",
					playerID, inst.InstanceID, attr.AttrID)
			}
			if attr.Value <= 0 || attr.Value > maxValue {
				return nil, errcode.New(errcode.ErrInternal,
					"instance detail attr value out of range player=%d instance=%d attr=%d value=%d max=%d",
					playerID, inst.InstanceID, attr.AttrID, attr.Value, maxValue)
			}
		}
		inst.Attributes = append([]data.EquipmentAttributeSnapshot(nil), inst.Attributes...)
		details[inst.InstanceID] = inst
	}
	for _, slot := range slots {
		if _, ok := details[slot.InstanceID]; !ok {
			return nil, errcode.New(errcode.ErrInternal,
				"inventory instance detail unavailable; rollout incomplete player=%d instance=%d",
				playerID, slot.InstanceID)
		}
	}
	return details, nil
}

// equipmentAttributeMaxValue 是 Player -> Battle DS 装备词条信任边界。
//
// 词条虽然来自 inventory 内网权威接口，仍可能受脏库、旧副本或解析漂移影响；Player 在组装
// GetLoadout 前只放行 DS 已实现且数值单位明确的三类属性。Attack/Defense 是平坦整数值，
// MoveSpeedRate 是基点(10000 = 100%)。任何未知 ID、非正数或超限值都 fail-closed，避免把
// 不受控的增益注入战斗服。
func equipmentAttributeMaxValue(attrID uint32) (int64, bool) {
	switch attrID {
	case equipmentAttrAttackID, equipmentAttrDefenseID:
		return maxEquipmentFlatAttributeValue, true
	case equipmentAttrMoveSpeedRateID:
		return maxEquipmentRateBasisPoints, true
	default:
		return 0, false
	}
}

// GrantTalentPoints 幂等授予天赋点(来源:升级 / 活动,系统驱动不受 LoadoutCustomizeEnabled 影响)。
func (u *PlayerUsecase) GrantTalentPoints(ctx context.Context, playerID uint64, points int32, idempotencyKey string) (int, error) {
	if playerID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if points <= 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "points must be positive")
	}
	if idempotencyKey == "" {
		return 0, errcode.New(errcode.ErrInvalidArg, "idempotency_key required")
	}
	if _, err := u.repo.EnsureProfile(ctx, playerID, u.defaultNickname(playerID), u.cfg.BaseMMR); err != nil {
		return 0, err
	}
	unspent, already, err := u.repo.GrantTalentPoints(ctx, playerID, points, idempotencyKey)
	if err != nil {
		return 0, err
	}
	if already {
		plog.With(ctx).Debugw("msg", "grant_talent_idempotent_hit",
			"player_id", playerID, "idempotency_key", idempotencyKey, "unspent", unspent)
	}
	return unspent, nil
}

// SetTalents 全量重置天赋分配(功能开关关闭 → ErrPlayerFeatureDisabled;
// talent_id 重复 / 不存在 / level<=0 / 超等级上限 / 前置未达标 → ErrInvalidArg;
// 总消耗超额 → ErrPlayerInsufficientPoints)。
//
// 权威校验按专精表(2026-07-25 接入 z_专精.xlsx):节点存在、等级不超 max_level、
// 前置节点在**本次方案内**达标、总消耗按 Σ 等级 × cost_per_level 计算后交 repo 与可用点比对。
// 表未加载一律 fail-closed:此前只校验「id 非 0 + level > 0 + 不重复」,客户端填 level=999 能直接写进库。
func (u *PlayerUsecase) SetTalents(ctx context.Context, playerID uint64, talents []data.TalentLevel) (int, error) {
	if playerID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if !u.cfg.LoadoutCustomizeEnabled {
		return 0, errcode.New(errcode.ErrPlayerFeatureDisabled, "loadout customize disabled")
	}
	levels := make(map[uint32]uint32, len(talents))
	for _, t := range talents {
		if t.TalentID == 0 {
			return 0, errcode.New(errcode.ErrInvalidArg, "talent_id required")
		}
		if t.Level <= 0 {
			return 0, errcode.New(errcode.ErrInvalidArg, "level must be positive: talent=%d", t.TalentID)
		}
		if _, dup := levels[t.TalentID]; dup {
			return 0, errcode.New(errcode.ErrInvalidArg, "duplicate talent_id %d", t.TalentID)
		}
		levels[t.TalentID] = uint32(t.Level)
	}

	costs, err := u.talentAllocationCost(levels)
	if err != nil {
		return 0, err
	}
	// 逐节点消耗随分配一起落库:读取侧(GetTalents / GrantTalentPoints 回读可点数)
	// 直接 SUM 这一列,不再按 Σ 等级 反推——反推在 cost_per_level≠1 时会算少已花点数,
	// 玩家看到的可点数比实际多。写与读从此共用专精表这一个口径。
	priced := make([]data.TalentLevel, 0, len(talents))
	for _, t := range talents {
		t.SpentPoints = int32(costs[t.TalentID])
		priced = append(priced, t)
	}

	if _, err := u.repo.EnsureProfile(ctx, playerID, u.defaultNickname(playerID), u.cfg.BaseMMR); err != nil {
		return 0, err
	}
	return u.repo.SetTalents(ctx, playerID, priced)
}

// talentAllocationCost 用专精表校验整份分配并算出逐节点消耗点数。
// 表未加载时 fail-closed;表判定非法时统一归为 ErrInvalidArg(具体原因带在消息里)。
func (u *PlayerUsecase) talentAllocationCost(levels map[uint32]uint32) (map[uint32]uint32, error) {
	if len(levels) == 0 {
		return nil, nil
	}
	rules := u.talentRules
	if rules == nil {
		return nil, errcode.New(errcode.ErrInternal, "talent config table unavailable")
	}
	costs, _, err := rules.ValidateAllocation(levels)
	if err != nil {
		return nil, errcode.New(errcode.ErrInvalidArg, "invalid talent allocation: %v", err)
	}
	return costs, nil
}

// ResetTalents 清空天赋分配(功能开关关闭 → ErrPlayerFeatureDisabled)。
func (u *PlayerUsecase) ResetTalents(ctx context.Context, playerID uint64) (int, error) {
	if playerID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if !u.cfg.LoadoutCustomizeEnabled {
		return 0, errcode.New(errcode.ErrPlayerFeatureDisabled, "loadout customize disabled")
	}
	if _, err := u.repo.EnsureProfile(ctx, playerID, u.defaultNickname(playerID), u.cfg.BaseMMR); err != nil {
		return 0, err
	}
	return u.repo.ResetTalents(ctx, playerID)
}

// GetTalents 读已点天赋 + 可点天赋点。
func (u *PlayerUsecase) GetTalents(ctx context.Context, playerID uint64) ([]data.TalentLevel, int, error) {
	if playerID == 0 {
		return nil, 0, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	return u.repo.GetTalents(ctx, playerID)
}

// GetLoadout 组装开战前快照(出战英雄 + 属性点 + 装备预设 + 天赋),供匹配/进战下发。
func (u *PlayerUsecase) GetLoadout(ctx context.Context, playerID uint64) (*playerv1.PlayerLoadout, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	heroID, err := u.repo.GetActiveHero(ctx, playerID)
	if err != nil {
		return nil, err
	}
	attrs, unspent, err := u.repo.GetAttributes(ctx, playerID)
	if err != nil {
		return nil, err
	}
	pts := make([]*playerv1.AttributeAllocation, 0, len(attrs))
	for _, a := range attrs {
		pts = append(pts, &playerv1.AttributeAllocation{AttrKey: a.Key, Points: a.Points})
	}
	equip, err := u.repo.GetEquipment(ctx, playerID)
	if err != nil {
		return nil, err
	}
	instanceDetails, err := u.validateEquipmentForBattleSnapshot(ctx, playerID, equip)
	if err != nil {
		return nil, err
	}
	eq := make([]*playerv1.LoadoutEquipment, 0, len(equip))
	for _, s := range equip {
		detail := instanceDetails[s.InstanceID]
		attributes := make([]*inventoryv1.ItemAttribute, 0, len(detail.Attributes))
		for _, attr := range detail.Attributes {
			attributes = append(attributes, &inventoryv1.ItemAttribute{AttrId: attr.AttrID, Value: attr.Value})
		}
		eq = append(eq, &playerv1.LoadoutEquipment{
			Slot: s.Slot, ItemConfigId: s.ItemConfigID, InstanceId: s.InstanceID,
			Identified: detail.Identified, Attributes: attributes,
		})
	}
	talents, talentUnspent, err := u.repo.GetTalents(ctx, playerID)
	if err != nil {
		return nil, err
	}
	tl := make([]*playerv1.TalentNode, 0, len(talents))
	for _, t := range talents {
		tl = append(tl, &playerv1.TalentNode{TalentId: t.TalentID, Level: t.Level})
	}
	cards, slots, err := u.GetSkillCards(ctx, playerID)
	if err != nil {
		return nil, err
	}
	// 卡等级随槽位一起带出:DS 只关心"这个槽是哪张卡、几级",
	// 让它拿着 card_id 再查一次持有表等于把一次读拆成两次(且中间可能被改)。
	levelOf := make(map[uint32]uint32, len(cards))
	for _, c := range cards {
		levelOf[c.CardID] = c.Level
	}
	sc := make([]*playerv1.LoadoutSkillCard, 0, len(slots))
	for _, s := range slots {
		sc = append(sc, &playerv1.LoadoutSkillCard{
			Slot:   s.Slot,
			CardId: s.CardID,
			Level:  levelOf[s.CardID],
		})
	}
	return &playerv1.PlayerLoadout{
		PlayerId:            playerID,
		ActiveHeroId:        heroID,
		Attributes:          pts,
		UnspentAttrPoints:   int32(unspent),
		Equipment:           eq,
		Talents:             tl,
		UnspentTalentPoints: int32(talentUnspent),
		SkillCards:          sc,
	}, nil
}

// battleFlags 按 reason 决定是否计对局 / 计胜。
//
//   - win:计一场 + 计一胜
//   - lose / draw:计一场,不计胜
//   - abandon:对局作废,不计场不计胜(delta 应为 0)
//   - rollback / 其它:纯 MMR 修正,不计场不计胜
func battleFlags(reason string) (incBattle, incWin bool) {
	switch reason {
	case "win":
		return true, true
	case "lose", "draw":
		return true, false
	default:
		return false, false
	}
}
