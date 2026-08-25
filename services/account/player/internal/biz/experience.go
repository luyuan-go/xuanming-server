// experience.go — 玩家等级经验入账 + 经验推送出箱发布器(实时成长,2026-07-20)。
//
// 设计(docs/design/realtime-progression.md §4.2):
//   - AddExperience 是**系统 RPC 的业务核心**:幂等入账(uk player_id+idempotency_key)、
//     按等级经验曲线循环进位(连升多级)、最高等级封顶、满级 no-op。
//   - 入账与经验推送出箱同一 MySQL 事务(不变量 §4);后台 RunPushOutboxPublisher 轮询
//     出箱表逐条投 kafka pandora.player.experience(event_type=EXPERIENCE header),
//     push 服务透明转发给客户端刷新经验条 / 播升级表现。
//   - 经验来源(battle_result progress 出箱 / 任务完成点 / GM)只报 delta,
//     等级曲线唯一权威在本服务(DS / 调用方不可信,不变量 §6 同构)。
//   - 多副本发布器无需 claim/fencing:事件是入账后的**全量权威快照**,客户端按
//     (level, exp_in_level) 单调不回退去重,重复投递 / 旧快照晚到都无副作用
//     (at-least-once + 快照语义,§16.2)。
package biz

import (
	"context"
	"time"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/safego"

	"github.com/luyuancpp/pandora/services/account/player/internal/data"
)

// ExperiencePusher 把玩家推送出箱行投递到 kafka pandora.player.experience
// (key=player_id 保序,event_type 走 kafka header,push 服务透传)。
// 投递失败返回 error → 出箱行保留下轮重试(at-least-once,不变量 §4)。
type ExperiencePusher interface {
	PushPlayerEvent(ctx context.Context, playerID uint64, eventType uint32, payload []byte) error
}

// SetExperiencePusher 注入推送出箱发布器的 kafka producer 适配(nil-safe:
// brokers 未配 / producer 构造失败时不注入,出箱积压不丢,恢复后重启补发)。
func (u *PlayerUsecase) SetExperiencePusher(p ExperiencePusher) {
	u.expPusher = p
}

// AddExperience 幂等入账经验并结算等级(实时成长唯一入口,系统调用)。
// 返回 (入账后快照, 是否幂等命中, error)。
//
//	配置表未加载 → ErrPlayerFeatureDisabled(启动主链要求配置表加载成功；这里只做防御)
//	delta 超单次上限 → ErrInvalidArg(防异常调用方一次灌满;上限见 conf.MaxExpPerGrant)
//	满级 → no-op:返回满级快照,不加经验、不出箱,但仍消费幂等键落 no-op 收据
//	(重放命中收据返回 already=true;防曲线扩容后滞留事件重入账,审计 P2)
func (u *PlayerUsecase) AddExperience(ctx context.Context, playerID uint64, delta uint64, reason, idempotencyKey string) (data.ExpState, bool, error) {
	if playerID == 0 {
		return data.ExpState{}, false, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if idempotencyKey == "" {
		return data.ExpState{}, false, errcode.New(errcode.ErrInvalidArg, "idempotency_key required")
	}
	if delta == 0 {
		return data.ExpState{}, false, errcode.New(errcode.ErrInvalidArg, "exp_delta must be positive")
	}
	if maxGrant := u.cfg.MaxExpPerGrantOrDefault(); delta > maxGrant {
		return data.ExpState{}, false, errcode.New(errcode.ErrInvalidArg,
			"exp_delta %d exceeds max_exp_per_grant %d", delta, maxGrant)
	}
	if !u.cfg.ExperienceEnabled {
		return data.ExpState{}, false, errcode.New(errcode.ErrPlayerFeatureDisabled, "experience disabled")
	}
	curve := u.experienceCurve()
	if len(curve) == 0 {
		return data.ExpState{}, false, errcode.New(errcode.ErrPlayerFeatureDisabled, "experience disabled (player level table unavailable)")
	}
	if _, err := u.repo.EnsureProfile(ctx, playerID, u.defaultNickname(playerID), u.cfg.BaseMMR); err != nil {
		return data.ExpState{}, false, err
	}

	st, already, err := u.repo.ApplyExperience(ctx, data.ExpApply{
		PlayerID:       playerID,
		Delta:          delta,
		Reason:         reason,
		IdempotencyKey: idempotencyKey,
		Curve:          curve,
	})
	if err != nil {
		return data.ExpState{}, false, err
	}
	if already {
		plog.With(ctx).Debugw("msg", "add_experience_idempotent_hit",
			"player_id", playerID, "idempotency_key", idempotencyKey,
			"level", st.Level, "exp_in_level", st.ExpInLevel)
		return st, true, nil
	}
	plog.With(ctx).Debugw("msg", "add_experience_applied",
		"player_id", playerID, "delta", delta, "reason", reason,
		"level", st.Level, "exp_in_level", st.ExpInLevel,
		"levels_gained", st.LevelsGained, "is_max", st.IsMaxLevel)
	return st, false, nil
}

// DecorateExperience 用等级曲线给档案补经验派生字段(GetProfile 出参装饰):
// 满级 → is_max_level=true 且级内经验按 0 展示(权威列已保证满级恒 0,此处防御性夹紧)。
// 曲线未配置(功能关闭)→ 不标满级,exp 原样(默认 0),行为与历史一致。
func (u *PlayerUsecase) DecorateExperience(level int32, expInLevel uint64) (uint64, bool) {
	if !u.cfg.ExperienceEnabled {
		return expInLevel, false
	}
	curve := u.experienceCurve()
	if len(curve) == 0 {
		return expInLevel, false
	}
	maxLevel := int32(len(curve)) + 1
	if level >= maxLevel {
		return 0, true
	}
	return expInLevel, false
}

// experienceCurve 从当前原子快照提取本次调用使用的曲线副本。
func (u *PlayerUsecase) experienceCurve() []uint64 {
	if u.expLevels == nil {
		return nil
	}
	return u.expLevels.ExperienceCurve()
}

// RunPushOutboxPublisher 启动后台玩家推送出箱发布循环,直到 ctx 取消
// (发布器纪律对齐 battle_result.RunOutboxPublisher:FIFO、失败中断本轮保序、成功才删行)。
// PushWriterLease 是推送出箱发布器的领导权来源(*writerlease.Lease 满足)。
//
// **边界**:只保证「同一时刻一个副本发布」以取得**保序**,不是防脑裂,故出箱的写不需要
// 把 fencing token 带进事务 —— 发布器不携带跨轮次权威意图(每轮从 MySQL 重读,动作只是
// 投 kafka + 删行,权威态 player_level / exp 全程不被触碰)。迟到的旧 leader 做的是与新
// leader 逐字相同的重放,唯一危害是投递顺序交错,而那正是本选举要消除的。
type PushWriterLease interface {
	// Current 返回 (fencing token, 是否持有领导权)。本接口只消费第二个返回值。
	Current() (uint64, bool)
}

// SetPushWriterLease 注入发布器领导权来源;nil = 不选举(本副本无条件发布)。
// 只允许在启动装配期调用(RunPushOutboxPublisher 之前)。
func (u *PlayerUsecase) SetPushWriterLease(l PushWriterLease) { u.pushLease = l }

// pushIsLeader 判定本轮是否由本副本发布,并把领导权跃迁打成日志(未注入恒为 true)。
func (u *PlayerUsecase) pushIsLeader(ctx context.Context) bool {
	if u.pushLease == nil {
		return true
	}
	_, held := u.pushLease.Current()
	if held != u.pushLeaseHeld {
		u.pushLeaseHeld = held
		plog.With(ctx).Infow("msg", "push_outbox_leadership_changed", "held", held,
			"hint", "held=false 时本副本热备不发布;出箱由当选副本排空")
	}
	return held
}

func (u *PlayerUsecase) RunPushOutboxPublisher(ctx context.Context) {
	if u.expPusher == nil {
		// 2026-08-24 起这里**只剩** kafka.brokers 未配这一档(刻意禁用推送的开发档)。
		// 「配了 brokers 但启动时连不上」已改由 kafkax.LazyProducer 承接:pusher 恒非 nil,
		// 发布器照常起,投递失败按拍重试,Kafka 恢复即自动排空 —— 不再需要重启进程。
		plog.With(ctx).Infow("msg", "push_outbox_publisher_disabled",
			"hint", "kafka.brokers 未配 → 经验推送禁用;出箱积压不丢,配上 brokers 重启即排空")
		return
	}
	interval := u.cfg.PushOutboxIntervalOrDefault()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	plog.With(ctx).Infow("msg", "push_outbox_publisher_started",
		"interval", interval.String(), "batch", u.cfg.PushOutboxBatchOrDefault())
	for {
		select {
		case <-ctx.Done():
			plog.With(ctx).Infow("msg", "push_outbox_publisher_stopped")
			return
		case <-ticker.C:
			// 领导权逐轮判定(§9.21 同一未分区权威的单写者循环)。
			// 失主后本副本立刻停止发布,不"补完"在飞的行 —— 那正是交错的来源。
			if !u.pushIsLeader(ctx) {
				continue
			}
			// panic 兜底(压测审核【必修-6】同类点位):单轮 panic 只丢本轮,出箱行下轮重试。
			safego.Run(ctx, "player_push_outbox_publisher", func() {
				// 排空循环:满批说明还有积压,立即继续下一批,不等下个 tick
				// (否则吞吐被钉死在 batch/interval,持续流量下积压只增不减)。
				for ctx.Err() == nil {
					n, err := u.publishPushOutboxBatch(ctx)
					if err != nil {
						plog.With(ctx).Warnw("msg", "push_outbox_publish_batch_failed", "published", n, "err", err)
						break
					}
					if n < u.cfg.PushOutboxBatchOrDefault() {
						break // 未满批 = 已清空
					}
				}
			})
		}
	}
}

// retentionSweepInterval / retentionSweepBatch 是两个 janitor 共用的节拍与批大小。
// 复用同一对常量,避免"两个 janitor 各拍一个数"后再也说不清哪个才是本服口径。
const (
	retentionSweepInterval = time.Hour
	retentionSweepBatch    = 1000
)

// RunExpHistoryJanitor 启动 exp_history 幂等收据的保留期循环,直到 ctx 取消。
//
// **默认只报告不删**(§9.24):janitor 照常跑,每轮统计"有多少行超过留存期"并打 WARN +
// pandora_db_retention_pending_rows,一行都不删。真删要两道闸同时开:
// 服务级 `retention_mode: delete` + 本组前置条件 `exp_history_cleanup_enabled: true`。
//
// 后者为什么单独存在(审计 P1):battle_result progress 出箱是永久重试链(退避上限 5min,
// 无总重试期限)。入账成功但响应丢失 / 删行持续失败超过留存期时,清掉收据 = 同一事件
// 再次入账(双发)。幂等正确性优先于表增长,所以即便总闸开着,这一组也要单独确认
// "上游已具备小于留存期的有界重试 / 隔离期限"才允许删。
//
// 早先的实现是前置条件没确认就**整个不跑** —— 那等于既不删也不报,§9.24 要的待清理量
// 彻底不可见,库在无人知晓的情况下涨。现在改成降级为 report_only。
// 多副本并发 DELETE ... LIMIT 安全(各删各的行);表按 PK 升序扫,老行在前。
func (u *PlayerUsecase) RunExpHistoryJanitor(ctx context.Context) {
	u.runRetentionJanitor(ctx, "player_exp_history_janitor",
		func() time.Time { return time.Now().Add(-u.cfg.ExpHistoryRetentionOrDefault()) },
		u.cfg.ExpHistoryRetentionMode,
		u.expHistoryRetentionSweeps(),
	)
}

// RunHistoryJanitor 启动 mmr_history / attr_point_grants / talent_point_grants /
// skill_card_grants 幂等历史的保留期循环,直到 ctx 取消(CLAUDE.md §9 不变量 24:只增表必须有界)。
//
// 默认同样只报告不删。真删的两道闸是 `retention_mode: delete` + `history_cleanup_enabled: true`;
// 后者的前置条件与 exp_history 同源但不同上游:kafka player.update 消费与授予补扫是
// at-least-once,清掉幂等行后同一事件重放 = 双发(重复加段位分 / 加点 / 重复发卡),
// 必须先确认上游重放期限(kafka retention / 补扫窗口)小于留存期(默认 90 天,下限 30 天)。
func (u *PlayerUsecase) RunHistoryJanitor(ctx context.Context) {
	u.runRetentionJanitor(ctx, "player_history_janitor",
		func() time.Time { return time.Now().Add(-u.cfg.HistoryRetentionOrDefault()) },
		u.cfg.HistoryRetentionMode,
		u.historyRetentionSweeps(),
	)
}

// retentionSweep 是一张表的保留期处理入口(表名只用于日志,不拼进 SQL)。
type retentionSweep struct {
	table string
	sweep func(context.Context, dbguard.Mode, time.Time, int) (dbguard.Outcome, error)
}

// expHistoryRetentionSweeps / historyRetentionSweeps 把实际登记清单集中在可测试入口。
// 新增只增表时必须在这里接入,对应的测试会在漏接时直接失败。
func (u *PlayerUsecase) expHistoryRetentionSweeps() []retentionSweep {
	return []retentionSweep{{"exp_history", u.repo.SweepExpHistory}}
}

func (u *PlayerUsecase) historyRetentionSweeps() []retentionSweep {
	return []retentionSweep{
		{"mmr_history", u.repo.SweepMMRHistory},
		{"attr_point_grants", u.repo.SweepAttrPointGrants},
		{"talent_point_grants", u.repo.SweepTalentPointGrants},
		{"skill_card_grants", u.repo.SweepSkillCardGrants},
	}
}

// runRetentionJanitor 是两个 janitor 的共同循环:按节拍取一次 mode 与 cutoff,
// 逐表 sweep。mode 每轮现取(而不是启动时定死),这样改配置重启副本即可生效,
// 也不会出现"半个循环用旧模式"的错位。
func (u *PlayerUsecase) runRetentionJanitor(
	ctx context.Context, name string, cutoffAt func() time.Time,
	modeOf func() dbguard.Mode, sweeps []retentionSweep,
) {
	ticker := time.NewTicker(retentionSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// panic 兜底(压测审核【必修-6】同类点位):单轮 panic 只丢本轮,下轮继续。
			safego.Run(ctx, name, func() {
				mode, cutoff := modeOf(), cutoffAt()
				for _, s := range sweeps {
					u.drainRetention(ctx, mode, cutoff, s)
				}
			})
		}
	}
}

// drainRetention 处理一张表:delete 模式下小批量循环删到追平,report_only 只跑一轮。
//
// report_only 下**不循环**:那一轮的 COUNT 已经给出全量待清理规模(不受 batch 截断),
// 再循环只是把同一条 COUNT 重复执行 —— 循环的意义是"追平积压",而只报告时积压永远
// 追不平,循环会变成每轮固定跑满的空转(与 battle_result drainPurge 同一处置)。
func (u *PlayerUsecase) drainRetention(ctx context.Context, mode dbguard.Mode, cutoff time.Time, s retentionSweep) {
	var deleted int64
	for ctx.Err() == nil {
		out, err := s.sweep(ctx, mode, cutoff, retentionSweepBatch)
		if err != nil {
			plog.With(ctx).Warnw("msg", "retention_sweep_failed",
				"table", s.table, "mode", mode.String(), "deleted_before_fail", deleted, "err", err)
			return
		}
		if mode != dbguard.ModeDelete {
			// 待清理量的 WARN + gauge 已由 dbguard.SweepTable 统一打,这里不重复。
			return
		}
		deleted += out.Deleted
		if !out.Truncated {
			break
		}
	}
	if deleted > 0 {
		plog.With(ctx).Infow("msg", "retention_swept", "table", s.table, "deleted", deleted)
	}
}

// publishPushOutboxBatch 取一批推送出箱记录投递,返回本轮成功投递并删除的条数。
// 投递失败立即中断本轮(保留出箱行下轮重试),保证同玩家事件按 id 顺序投递(不变量 §9)。
func (u *PlayerUsecase) publishPushOutboxBatch(ctx context.Context) (int, error) {
	if u.expPusher == nil {
		return 0, nil
	}
	recs, err := u.repo.FetchPushOutbox(ctx, u.cfg.PushOutboxBatchOrDefault())
	if err != nil {
		return 0, err
	}
	published := 0
	for _, r := range recs {
		if perr := u.expPusher.PushPlayerEvent(ctx, r.PlayerID, r.EventType, r.Payload); perr != nil {
			return published, perr // 本轮中断,保留出箱行下轮重试
		}
		if derr := u.repo.DeletePushOutbox(ctx, r.ID); derr != nil {
			return published, derr
		}
		published++
	}
	if published > 0 {
		plog.With(ctx).Debugw("msg", "push_outbox_published", "count", published)
	}
	return published, nil
}
