// battle_result_test.go — biz 层单测(W4 ③,2026-06-06)。
//
// 覆盖:
//   - Elo:等分对称(+K/2 / -K/2)、强队赢得少、平局对称、K 守恒
//   - ReportResult:MMR 赋值 + 幂等命中
//   - HandleAbandoned:补偿记录 outcome=ABANDONED + delta 全 0 + 幂等
//   - 输入校验
package biz

import (
	"context"
	"errors"
	"fmt"
	"github.com/luyuancpp/pandora/pkg/dbguard"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/rating"
	battlev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/battle/v1"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"

	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/data"
)

// ── 测试替身 ──────────────────────────────────────────────────────────────────

// fakeRepo 是内存版 data.BattleRepo,按 match_id 唯一(模拟 unique 幂等)+内存出箱。
type fakeRepo struct {
	store                     map[uint64]*battlev1.BattleResult
	saveErr                   error
	saveCnt                   int
	outbox                    []data.OutboxRecord     // player.update 待发布,按 ID 升序
	dropOutbox                []data.DropOutboxRecord // 装备掉落待发放,按 ID 升序(W5 ④)
	nextID                    int64
	nextDropID                int64
	terminalOutbox            []data.TerminalReleaseRecord
	nextTerminalID            uint64
	terminalDeleteErr         error
	terminalMarkErr           error
	terminalMarkCommitThenErr bool
	matchReleaseOutbox        []data.MatchReleaseRecord
	nextMatchReleaseID        uint64
	matchReleaseDeferErr      error
	matchReleaseDeleteErr     error

	// 实时进度通道(实时成长):复刻 MySQL 水位 / 终局标记 / 累计上限 / 进度出箱语义。
	progressSeq     map[uint64]uint64                               // match_id → last_applied_seq
	progressExp     map[uint64]uint64                               // match_id → total_exp
	progressItems   map[uint64]uint32                               // match_id → total_items
	progressPlayers map[uint64]map[uint64]data.ProgressPlayerTotals // match_id → player_id → 累计
	progressSettled map[uint64]bool                                 // match_id → 已结算(打过终局标记)
	progressStopped map[uint64]bool                                 // match_id → 已停流(未知事实持久标记)
	markStoppedErr  error                                           // 非 nil 时 MarkProgressStopped 直接返错(可重试语义单测)
	// staleWatermark 非 nil 时被下一次 GetProgressWatermark 消费一次(返回过期快照),
	// 模拟"重试请求读到旧水位而权威状态已被首请求推进"的竞态窗口(审计 P1 回归)。
	staleWatermark *data.ProgressWatermark

	// 保留期清理 fake 状态(排空循环 / 陈年未结算告警单测)。
	purgeBattlesResults  []int64
	purgeProgressResults []int64
	purgeBattlesCalls    int
	purgeProgressCalls   int
	staleUnsettled       int64
	staleUnsettledCalls  int
	progressOutbox       []data.ProgressOutboxRecord
	nextProgressID       int64
	deferredIDs          []int64 // DeferProgressOutbox 调用记录(fake 不真正推迟,行保持可取)
	progressBalances     map[[3]uint64][2]uint32
	progressActions      map[[4]uint64]data.ProgressAction
	missionOutbox        []data.MissionFactRecord
	nextMissionID        int64
	missionDeferredIDs   []int64 // DeferMissionOutbox 调用记录(同上,fake 不真正推迟)
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		store:            map[uint64]*battlev1.BattleResult{},
		progressSeq:      map[uint64]uint64{},
		progressExp:      map[uint64]uint64{},
		progressItems:    map[uint64]uint32{},
		progressPlayers:  map[uint64]map[uint64]data.ProgressPlayerTotals{},
		progressSettled:  map[uint64]bool{},
		progressBalances: map[[3]uint64][2]uint32{},
		progressActions:  map[[4]uint64]data.ProgressAction{},
	}
}

func (r *fakeRepo) SaveResult(_ context.Context, result *battlev1.BattleResult, outbox []data.OutboxRecord, dropOutbox []data.DropOutboxRecord, terminalRelease *data.TerminalReleaseRecord, _ uint64) (bool, data.ProgressSettleInfo, error) {
	r.saveCnt++
	if r.saveErr != nil {
		return false, data.ProgressSettleInfo{}, r.saveErr
	}
	if _, ok := r.store[result.GetMatchId()]; ok {
		r.ensureFakeMatchRelease(result.GetMatchId(), r.store[result.GetMatchId()].GetStats())
		return true, data.ProgressSettleInfo{}, nil // 幂等命中会恢复缺失 release outbox，其它出箱不重复
	}
	r.store[result.GetMatchId()] = proto.Clone(result).(*battlev1.BattleResult)
	// 实时进度通道结算收口(复刻 settleProgressStreamTx):打终局标记 + 水位>0 抑制掉落发放。
	lastSeq, streamExisted := r.progressSeq[result.GetMatchId()]
	r.progressSettled[result.GetMatchId()] = true
	settleInfo := data.ProgressSettleInfo{
		StreamExisted:   streamExisted,
		LastAppliedSeq:  lastSeq,
		DropsSuppressed: lastSeq > 0,
	}
	for _, o := range outbox {
		r.nextID++
		r.outbox = append(r.outbox, data.OutboxRecord{ID: r.nextID, PlayerID: o.PlayerID, Payload: o.Payload})
	}
	// 入箱判据直接调生产的 data.SettleDropOutboxRow,**刻意不在假仓里复刻一份**。
	// 历史教训:假仓曾逐字抄了 `if !settleInfo.DropsSuppressed { ... }` 整个循环,
	// 假仓与生产同构 → "实时通道一介入就把整局金币静默吞掉"这类漏发在单测里完全不可见。
	// 共用同一个函数后,下面这些出箱行断言观察到的就是生产判据本身。
	for _, d := range dropOutbox {
		row, keep := data.SettleDropOutboxRow(d, settleInfo.DropsSuppressed)
		if !keep {
			continue
		}
		r.nextDropID++
		r.dropOutbox = append(r.dropOutbox, data.DropOutboxRecord{
			ID: r.nextDropID, MatchID: result.GetMatchId(), PlayerID: row.PlayerID,
			ItemConfigIDs:         append([]uint32(nil), row.ItemConfigIDs...),
			StackItemConfigIDs:    append([]uint32(nil), row.StackItemConfigIDs...),
			InstanceItemConfigIDs: append([]uint32(nil), row.InstanceItemConfigIDs...),
			CurrencyAmount:        row.CurrencyAmount,
		})
	}
	if terminalRelease != nil {
		r.nextTerminalID++
		rec := *terminalRelease
		rec.ID = r.nextTerminalID
		rec.CreatedAtMs = time.Now().UnixMilli()
		r.terminalOutbox = append(r.terminalOutbox, rec)
	}
	r.ensureFakeMatchRelease(result.GetMatchId(), result.GetStats())
	return false, settleInfo, nil
}

// ── 实时进度通道 fake(复刻 progress_repo 水位 CAS / 出箱语义)────────────────

func (r *fakeRepo) GetProgressWatermark(_ context.Context, matchID uint64) (data.ProgressWatermark, error) {
	if r.staleWatermark != nil {
		wm := *r.staleWatermark
		r.staleWatermark = nil
		return wm, nil
	}
	seq, ok := r.progressSeq[matchID]
	settled := r.progressSettled[matchID]
	stopped := r.progressStopped[matchID]
	return data.ProgressWatermark{
		LastAppliedSeq: seq,
		TotalExp:       r.progressExp[matchID],
		TotalItems:     r.progressItems[matchID],
		Settled:        settled,
		Stopped:        stopped,
		Existed:        ok || settled || stopped,
	}, nil
}

func (r *fakeRepo) MarkProgressStopped(_ context.Context, matchID uint64) error {
	if r.markStoppedErr != nil {
		return r.markStoppedErr
	}
	if r.progressStopped == nil {
		r.progressStopped = map[uint64]bool{}
	}
	r.progressStopped[matchID] = true
	return nil
}

// ClaimProgressLegacy 复刻 INSERT IGNORE 语义(审计 R4 #11):行已存在(已开流/
// 已认领/已结算)零修改返回 false;不存在才落停流标记行。
func (r *fakeRepo) ClaimProgressLegacy(_ context.Context, matchID uint64) (bool, error) {
	if r.markStoppedErr != nil {
		return false, r.markStoppedErr
	}
	if _, open := r.progressSeq[matchID]; open || r.progressSettled[matchID] || r.progressStopped[matchID] {
		return false, nil
	}
	if r.progressStopped == nil {
		r.progressStopped = map[uint64]bool{}
	}
	r.progressStopped[matchID] = true
	return true, nil
}

func (r *fakeRepo) ApplyProgress(_ context.Context, matchID, expectedSeq, newSeq uint64, addExp uint64, addItems uint32, playerDeltas []data.ProgressPlayerDelta, rows []data.ProgressOutboxRecord, missionRows []data.MissionFactRecord, caps data.ProgressCaps) error {
	if r.progressSettled[matchID] || r.progressStopped[matchID] {
		// 复刻 SQL 事务侧 fencing:settled_at_ms=0 AND stopped_at_ms=0 条件
		// (停流与正常批的 CAS 竞态,审计 P1)。
		return errcode.New(errcode.ErrUnavailable, "progress watermark moved/settled/stopped match=%d", matchID)
	}
	if r.progressSeq[matchID] != expectedSeq {
		return errcode.New(errcode.ErrUnavailable, "progress watermark contended match=%d", matchID)
	}
	// 复刻事务内一致快照上限判定:超限整体"回滚"= 不落任何状态(审计 P1)。
	if r.progressExp[matchID]+addExp > caps.MatchExp {
		return errcode.New(errcode.ErrInvalidArg, "match %d cumulative exp exceeds per-match cap %d", matchID, caps.MatchExp)
	}
	if r.progressItems[matchID]+addItems > caps.MatchItems {
		return errcode.New(errcode.ErrInvalidArg, "match %d cumulative items exceeds per-match cap %d", matchID, caps.MatchItems)
	}
	for _, d := range playerDeltas {
		t := r.progressPlayers[matchID][d.PlayerID]
		if t.TotalExp+d.Exp > caps.PlayerExp {
			return errcode.New(errcode.ErrInvalidArg, "match %d player %d cumulative exp exceeds per-player cap %d", matchID, d.PlayerID, caps.PlayerExp)
		}
		if t.TotalItems+d.Items > caps.PlayerItems {
			return errcode.New(errcode.ErrInvalidArg, "match %d player %d cumulative items exceeds per-player cap %d", matchID, d.PlayerID, caps.PlayerItems)
		}
		if t.TotalKills+d.Kills > caps.PlayerKills {
			return errcode.New(errcode.ErrInvalidArg, "match %d player %d cumulative kills exceeds per-player cap %d", matchID, d.PlayerID, caps.PlayerKills)
		}
	}
	// 先在副本上校验/预留，模拟 MySQL 整体事务回滚语义。
	nextBalances := make(map[[3]uint64][2]uint32, len(r.progressBalances))
	for key, value := range r.progressBalances {
		nextBalances[key] = value
	}
	var newActions []data.ProgressAction
	for _, row := range rows {
		switch row.Kind {
		case data.ProgressGrantStack:
			counts := map[uint32]uint32{}
			for _, itemID := range row.ItemConfigIDs {
				counts[itemID]++
			}
			for itemID, count := range counts {
				key := [3]uint64{matchID, row.PlayerID, uint64(itemID)}
				balance := nextBalances[key]
				balance[0] += count
				nextBalances[key] = balance
			}
		case data.ProgressConsumeStack, data.ProgressDiscardStack:
			itemID, count, err := progressSingleStackFactForTest(row)
			if err != nil {
				return err
			}
			key := [3]uint64{matchID, row.PlayerID, uint64(itemID)}
			balance := nextBalances[key]
			if balance[0]-balance[1] < count {
				return errcode.New(errcode.ErrInvalidArg,
					"battle item action exceeds same-match accepted pickup balance")
			}
			balance[1] += count
			nextBalances[key] = balance
			newActions = append(newActions, data.ProgressAction{
				MatchID: matchID, Seq: row.Seq, PlayerID: row.PlayerID, Kind: row.Kind,
				ItemConfigID: itemID, Count: count, Status: data.ProgressActionPending,
			})
		}
	}
	r.progressSeq[matchID] = newSeq
	r.progressExp[matchID] += addExp
	r.progressItems[matchID] += addItems
	if r.progressPlayers[matchID] == nil {
		r.progressPlayers[matchID] = map[uint64]data.ProgressPlayerTotals{}
	}
	for _, d := range playerDeltas {
		t := r.progressPlayers[matchID][d.PlayerID]
		t.TotalExp += d.Exp
		t.TotalItems += d.Items
		t.TotalKills += d.Kills
		r.progressPlayers[matchID][d.PlayerID] = t
	}
	r.progressBalances = nextBalances
	for _, action := range newActions {
		key := [4]uint64{action.MatchID, action.Seq, action.PlayerID, uint64(action.Kind)}
		r.progressActions[key] = action
	}
	for _, row := range rows {
		r.nextProgressID++
		row.ID = r.nextProgressID
		row.ItemConfigIDs = append([]uint32(nil), row.ItemConfigIDs...)
		r.progressOutbox = append(r.progressOutbox, row)
	}
	// 任务事实出箱与水位 CAS 同事务(mission.md §5.1):fake 也在同一函数内落,
	// 保证"上限拒收 / 水位竞争时一行不落"这条语义在单测里同样成立。
	for _, row := range missionRows {
		r.nextMissionID++
		row.ID = r.nextMissionID
		r.missionOutbox = append(r.missionOutbox, row)
	}
	return nil
}

// ── 任务事实转发出箱(独立表,故障域与进度出箱隔离)──

func (r *fakeRepo) FetchMissionOutbox(_ context.Context, limit int) ([]data.MissionFactRecord, error) {
	if limit <= 0 || limit > len(r.missionOutbox) {
		limit = len(r.missionOutbox)
	}
	return append([]data.MissionFactRecord(nil), r.missionOutbox[:limit]...), nil
}

func (r *fakeRepo) DeleteMissionOutbox(_ context.Context, id int64) error {
	for i, row := range r.missionOutbox {
		if row.ID == id {
			r.missionOutbox = append(r.missionOutbox[:i], r.missionOutbox[i+1:]...)
			return nil
		}
	}
	return nil
}

func (r *fakeRepo) DeferMissionOutbox(_ context.Context, id int64) error {
	r.missionDeferredIDs = append(r.missionDeferredIDs, id)
	return nil
}

func progressSingleStackFactForTest(row data.ProgressOutboxRecord) (uint32, uint32, error) {
	ids := row.ItemConfigIDs
	if len(ids) == 0 || ids[0] == 0 {
		return 0, 0, errcode.New(errcode.ErrInvalidArg, "empty stack action")
	}
	if row.ItemCount > 0 {
		if len(ids) != 1 {
			return 0, 0, errcode.New(errcode.ErrInvalidArg, "compact action must have one item")
		}
		return ids[0], row.ItemCount, nil
	}
	for _, id := range ids[1:] {
		if id != ids[0] {
			return 0, 0, errcode.New(errcode.ErrInvalidArg, "mixed stack action")
		}
	}
	return ids[0], uint32(len(ids)), nil
}

func (r *fakeRepo) FetchProgressOutbox(_ context.Context, limit int) ([]data.ProgressOutboxRecord, error) {
	// 复刻 MySQL：同 match/player 只取 seq/id 最早行，前序失败时后序不得越过。
	earliest := make(map[[2]uint64]data.ProgressOutboxRecord)
	for _, rec := range r.progressOutbox {
		key := [2]uint64{rec.MatchID, rec.PlayerID}
		prev, ok := earliest[key]
		if !ok || rec.Seq < prev.Seq || (rec.Seq == prev.Seq && rec.ID < prev.ID) {
			earliest[key] = rec
		}
	}
	out := make([]data.ProgressOutboxRecord, 0, len(earliest))
	for _, rec := range r.progressOutbox { // 保持真实查询的 id 顺序。
		if first := earliest[[2]uint64{rec.MatchID, rec.PlayerID}]; first.ID == rec.ID {
			out = append(out, rec)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *fakeRepo) FetchProgressOutboxForPlayer(_ context.Context, matchID, playerID, maxSeq uint64) (data.ProgressOutboxRecord, bool, error) {
	var found data.ProgressOutboxRecord
	for _, rec := range r.progressOutbox {
		if rec.MatchID != matchID || rec.PlayerID != playerID || rec.Seq > maxSeq {
			continue
		}
		if found.ID == 0 || rec.Seq < found.Seq || (rec.Seq == found.Seq && rec.ID < found.ID) {
			found = rec
		}
	}
	return found, found.ID != 0, nil
}

func (r *fakeRepo) GetProgressAction(_ context.Context, matchID, seq, playerID uint64, kind data.ProgressGrantKind) (data.ProgressAction, bool, error) {
	action, ok := r.progressActions[[4]uint64{matchID, seq, playerID, uint64(kind)}]
	return action, ok, nil
}

func (r *fakeRepo) ResolveProgressAction(_ context.Context, row data.ProgressOutboxRecord, resultCode errcode.Code) (data.ProgressAction, error) {
	key := [4]uint64{row.MatchID, row.Seq, row.PlayerID, uint64(row.Kind)}
	action, ok := r.progressActions[key]
	if !ok {
		return data.ProgressAction{}, errcode.New(errcode.ErrInvalidState, "progress action missing")
	}
	if action.Status == data.ProgressActionPending {
		if resultCode == errcode.OK {
			action.Status = data.ProgressActionSucceeded
		} else {
			balanceKey := [3]uint64{row.MatchID, row.PlayerID, uint64(action.ItemConfigID)}
			balance := r.progressBalances[balanceKey]
			if balance[1] < action.Count {
				return data.ProgressAction{}, errcode.New(errcode.ErrInvalidState, "reserved balance missing")
			}
			balance[1] -= action.Count
			r.progressBalances[balanceKey] = balance
			action.Status = data.ProgressActionFailed
			action.ResultCode = resultCode
		}
		r.progressActions[key] = action
	}
	_ = r.DeleteProgressOutbox(context.Background(), row.ID)
	return action, nil
}

func (r *fakeRepo) DeleteProgressOutbox(_ context.Context, id int64) error {
	for i, rec := range r.progressOutbox {
		if rec.ID == id {
			r.progressOutbox = append(r.progressOutbox[:i], r.progressOutbox[i+1:]...)
			return nil
		}
	}
	return nil
}

func (r *fakeRepo) DeferProgressOutbox(_ context.Context, id int64) error {
	// fake 只记录调用不真正推迟(单测里行保持可取,复刻"下轮重试"语义)。
	r.deferredIDs = append(r.deferredIDs, id)
	return nil
}

// 保留期清理:SQL 行为由 data 层集成测试覆盖;biz 侧用可配批次序列验证排空循环。
// purge*Results 依次弹出每次调用的返回值,耗尽后返回 0(= 追平)。
// batchOutcome 把"本批处理了 n 行"翻译成对应 mode 的 Outcome:
// report-only 只 Matched(Deleted 恒 0),delete 才 Deleted 并按 batch 判 Truncated。
func batchOutcome(mode dbguard.Mode, n int64, batch int) dbguard.Outcome {
	if mode != dbguard.ModeDelete {
		return dbguard.Outcome{Mode: mode, Matched: n}
	}
	return dbguard.Outcome{Mode: mode, Matched: n, Deleted: n, Truncated: batch > 0 && n >= int64(batch)}
}

func (r *fakeRepo) SweepExpiredBattles(_ context.Context, mode dbguard.Mode, _ int64, batch int) (dbguard.Outcome, error) {
	r.purgeBattlesCalls++
	if len(r.purgeBattlesResults) == 0 {
		return dbguard.Outcome{Mode: mode}, nil
	}
	n := r.purgeBattlesResults[0]
	r.purgeBattlesResults = r.purgeBattlesResults[1:]
	return batchOutcome(mode, n, batch), nil
}

func (r *fakeRepo) SweepSettledProgress(_ context.Context, mode dbguard.Mode, _ int64, batch int) (dbguard.Outcome, error) {
	r.purgeProgressCalls++
	if len(r.purgeProgressResults) == 0 {
		return dbguard.Outcome{Mode: mode}, nil
	}
	n := r.purgeProgressResults[0]
	r.purgeProgressResults = r.purgeProgressResults[1:]
	return batchOutcome(mode, n, batch), nil
}

func (r *fakeRepo) CountStaleUnsettledProgress(context.Context, int64) (int64, error) {
	r.staleUnsettledCalls++
	return r.staleUnsettled, nil
}

func (r *fakeRepo) ensureFakeMatchRelease(matchID uint64, stats []*battlev1.PlayerStats) {
	for i := range r.matchReleaseOutbox {
		if r.matchReleaseOutbox[i].MatchID == matchID {
			// Idempotent result replay preserves the immutable operation payload,
			// but makes a previously deferred row immediately eligible again.
			r.matchReleaseOutbox[i].NextAttemptAtMs = 0
			return
		}
	}
	r.nextMatchReleaseID++
	playerIDs := make([]uint64, 0, len(stats))
	for _, stat := range stats {
		playerIDs = append(playerIDs, stat.GetPlayerId())
	}
	r.matchReleaseOutbox = append(r.matchReleaseOutbox, data.MatchReleaseRecord{
		ID: r.nextMatchReleaseID, OperationID: "00000000-0000-4000-8000-000000000001",
		MatchID: matchID, PlayerIDs: playerIDs, CreatedAtMs: time.Now().UnixMilli(),
	})
}

func (r *fakeRepo) GetResult(_ context.Context, matchID uint64) (*battlev1.BattleResult, bool, error) {
	res, ok := r.store[matchID]
	if !ok {
		return nil, false, nil
	}
	return res, true, nil
}

func (r *fakeRepo) ListPlayerHistory(_ context.Context, _ uint64, _ int, _ int64) ([]*battlev1.BattleResult, error) {
	out := make([]*battlev1.BattleResult, 0, len(r.store))
	for _, v := range r.store {
		out = append(out, v)
	}
	return out, nil
}

func (r *fakeRepo) FetchOutbox(_ context.Context, limit int) ([]data.OutboxRecord, error) {
	if limit <= 0 || limit > len(r.outbox) {
		limit = len(r.outbox)
	}
	out := make([]data.OutboxRecord, limit)
	copy(out, r.outbox[:limit])
	return out, nil
}

func (r *fakeRepo) DeleteOutbox(_ context.Context, id int64) error {
	for i, o := range r.outbox {
		if o.ID == id {
			r.outbox = append(r.outbox[:i], r.outbox[i+1:]...)
			return nil
		}
	}
	return nil
}

func (r *fakeRepo) FetchDropOutbox(_ context.Context, limit int) ([]data.DropOutboxRecord, error) {
	if limit <= 0 || limit > len(r.dropOutbox) {
		limit = len(r.dropOutbox)
	}
	out := make([]data.DropOutboxRecord, limit)
	copy(out, r.dropOutbox[:limit])
	return out, nil
}

func (r *fakeRepo) DeleteDropOutbox(_ context.Context, id int64) error {
	for i, d := range r.dropOutbox {
		if d.ID == id {
			r.dropOutbox = append(r.dropOutbox[:i], r.dropOutbox[i+1:]...)
			return nil
		}
	}
	return nil
}

func (r *fakeRepo) FetchTerminalReleaseOutbox(_ context.Context, limit int, nowMs int64) ([]data.TerminalReleaseRecord, error) {
	out := make([]data.TerminalReleaseRecord, 0, len(r.terminalOutbox))
	for _, rec := range r.terminalOutbox {
		if rec.ReleaseAfterMs <= nowMs {
			out = append(out, rec)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *fakeRepo) DeleteTerminalReleaseOutbox(_ context.Context, id uint64) error {
	if r.terminalDeleteErr != nil {
		return r.terminalDeleteErr
	}
	for i, rec := range r.terminalOutbox {
		if rec.ID == id {
			if rec.ReleasedAtMs <= 0 {
				return nil // 模拟 SQL WHERE released_at_ms > 0 的 pending 防删前置条件。
			}
			r.terminalOutbox = append(r.terminalOutbox[:i], r.terminalOutbox[i+1:]...)
			return nil
		}
	}
	return nil
}

func (r *fakeRepo) MarkTerminalReleaseReleased(_ context.Context, id uint64, releasedAtMs int64) (bool, error) {
	for i := range r.terminalOutbox {
		if r.terminalOutbox[i].ID != id {
			continue
		}
		if r.terminalOutbox[i].ReleasedAtMs != 0 {
			return false, nil
		}
		if r.terminalMarkCommitThenErr {
			r.terminalOutbox[i].ReleasedAtMs = releasedAtMs
			return false, errors.New("mysql phase1 ACK response unknown")
		}
		if r.terminalMarkErr != nil {
			return false, r.terminalMarkErr
		}
		r.terminalOutbox[i].ReleasedAtMs = releasedAtMs
		return true, nil
	}
	if r.terminalMarkErr != nil {
		return false, r.terminalMarkErr
	}
	return false, nil
}

func (r *fakeRepo) FetchMatchReleaseOutbox(_ context.Context, limit int, nowMs int64) ([]data.MatchReleaseRecord, error) {
	out := make([]data.MatchReleaseRecord, 0, len(r.matchReleaseOutbox))
	for _, rec := range r.matchReleaseOutbox {
		if rec.NextAttemptAtMs <= nowMs {
			copyRec := rec
			copyRec.PlayerIDs = append([]uint64(nil), rec.PlayerIDs...)
			out = append(out, copyRec)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *fakeRepo) DeferMatchReleaseOutbox(_ context.Context, id uint64, nextAttemptAtMs int64) error {
	if r.matchReleaseDeferErr != nil {
		return r.matchReleaseDeferErr
	}
	for i := range r.matchReleaseOutbox {
		if r.matchReleaseOutbox[i].ID == id {
			r.matchReleaseOutbox[i].AttemptCount++
			r.matchReleaseOutbox[i].NextAttemptAtMs = nextAttemptAtMs
		}
	}
	return nil
}

func (r *fakeRepo) DeleteMatchReleaseOutbox(_ context.Context, id uint64) error {
	if r.matchReleaseDeleteErr != nil {
		return r.matchReleaseDeleteErr
	}
	for i, rec := range r.matchReleaseOutbox {
		if rec.ID == id {
			r.matchReleaseOutbox = append(r.matchReleaseOutbox[:i], r.matchReleaseOutbox[i+1:]...)
			return nil
		}
	}
	return nil
}

// fakePusher 捕获 player.update 事件;failFirst>0 时前 failFirst 次推送返错(模拟 Kafka 不可用),
// failAt>0 时第 failAt 次调用单次返错(模拟一批中途失败)。
type fakePusher struct {
	events    []capturedPush
	failFirst int
	failAt    int
	calls     int
}

type fakeMatchReleaser struct {
	calls int
	err   error
	match uint64
	ids   []uint64
}

type ackLossMatchReleaser struct {
	calls     int
	committed bool
}

func (r *fakeMatchReleaser) ReleaseMatch(_ context.Context, matchID uint64, playerIDs []uint64) error {
	r.calls++
	r.match = matchID
	r.ids = append([]uint64(nil), playerIDs...)
	return r.err
}

func (r *ackLossMatchReleaser) ReleaseMatch(context.Context, uint64, []uint64) error {
	r.calls++
	if !r.committed {
		// The downstream cleanup committed, but the caller cannot distinguish
		// that fact because the response was lost.
		r.committed = true
		return errors.New("release committed but ACK was lost")
	}
	return nil
}

type capturedPush struct {
	playerID uint64
	payload  []byte
}

func (p *fakePusher) PushPlayerUpdate(_ context.Context, playerID uint64, payload []byte) error {
	p.calls++
	if p.calls <= p.failFirst || p.calls == p.failAt {
		return simpleErr("kafka down")
	}
	p.events = append(p.events, capturedPush{playerID: playerID, payload: payload})
	return nil
}

// fakeGranter 捕获 GrantInstances 调用;failPlayer!=0 时对该玩家恒返错(模拟背包满,验证不阻塞其他玩家)。
// capacityFull=true 时所有玩家返 ErrInventoryCapacityFull(验证背包满转邮件路径)。
type fakeGranter struct {
	calls        []grantCall
	stackCalls   []stackGrantCall
	consumeCalls []consumeCall
	discardCalls []consumeCall
	failPlayer   uint64
	capacityFull bool
	// maxInstancesPerCall 复刻 inventory 侧那条真实的单次发放上闸:一次 GrantInstances 的
	// instance_id 列表要编进 inventory_ledger.detail(VARCHAR(255))当幂等回放事实源,
	// 装不下就整笔返错。生产判据是
	// services/economy/inventory/internal/data/inventory_instance.go 的
	// **GrantInstancesDetailFits**(按实际编码长度判,17 位雪花下一次约 13 件);
	// 这里用件数近似它,只为让"超限"这个形态在假仓里可达。
	//
	// ⚠ 别再写 "data.MaxGrantInstances() = (255-15+1)/(20+1) = 11 件":那个按最坏 20 位
	// 反推件数的函数**已被删除**(会把今天能发的批次改判为拒,并挡死已提交批次的回放),
	// 全仓 grep 只会剩注释自己在自说自话。
	//
	// 0 = 不设限(保持既有用例行为不变)。不复刻这一条,
	// "单场掉落超上限 → 出箱行永久卡死"在假仓里全绿。
	maxInstancesPerCall int
	failStack           bool
	failConsume         bool
	failDiscard         bool
	consumeErr          error
	discardErr          error
	consumeTries        int
	discardTries        int

	// grantedKeys 记录已成功入账的幂等键 → 请求指纹,复刻 inventory 侧真实幂等语义:
	// 同键同指纹 = 重放放行;同键不同指纹 = ErrInventoryIdempotencyConflict。
	// 不复刻这一条,"两路发放共用同一个幂等键"这类 P0(装备永远发不出去)在假仓里全绿。
	grantedKeys map[string]string
}

// claimKey 占用一个幂等键。只在**发放成功**时调用:inventory 侧失败的请求不会留下幂等记录,
// 假仓若在失败前就占键,会把"背包满下轮重试"误判成指纹冲突。
func (g *fakeGranter) claimKey(key, fingerprint string) error {
	if g.grantedKeys == nil {
		g.grantedKeys = map[string]string{}
	}
	if prev, ok := g.grantedKeys[key]; ok && prev != fingerprint {
		return errcode.New(errcode.ErrInventoryIdempotencyConflict,
			"idempotency key %q reused: %s vs %s", key, prev, fingerprint)
	}
	g.grantedKeys[key] = fingerprint
	return nil
}

type grantCall struct {
	playerID uint64
	items    []uint32
	key      string
}

type stackGrantCall struct {
	playerID uint64
	items    []data.StackGrant
	// gold 是本次随同发放的金币额(uint64;0 = 纯道具)。
	// 必须捕获而不是丢掉:金币与堆叠道具共用同一次 GrantItems / 同一幂等键,
	// 断言只看 items 的话,"金币没跟着发出去"这类漏发在测试里完全不可见。
	gold uint64
	key  string
}

type consumeCall struct {
	playerID uint64
	itemID   uint32
	count    int64
	key      string
}

func (g *fakeGranter) GrantInstances(_ context.Context, playerID uint64, itemConfigIDs []uint32, key string) error {
	if g.capacityFull {
		return errcode.New(errcode.ErrInventoryCapacityFull, "bag full")
	}
	if g.failPlayer != 0 && playerID == g.failPlayer {
		return simpleErr("bag full")
	}
	if g.maxInstancesPerCall > 0 && len(itemConfigIDs) > g.maxInstancesPerCall {
		// 真实错误码是 **errcode.ErrInvalidArg**(inventory_instance.go 那句
		// "grant_inst detail exceeds ledger column ... (split into multiple idempotency keys)"),
		// 不是 ErrInternal、更不是 capacity-full —— 码必须对上,因为 deliverDropRecord
		// 只对 ErrInventoryCapacityFull 转邮件,ErrInvalidArg 走的是"保留出箱行下轮重试",
		// 即永久卡死。假仓这里要是返成 capacity-full,那条永久卡死的路径就被测糊了。
		return errcode.New(errcode.ErrInvalidArg,
			"grant_inst detail exceeds ledger column: count=%d max=%d",
			len(itemConfigIDs), g.maxInstancesPerCall)
	}
	if err := g.claimKey(key, fmt.Sprintf("instances:%d:%v", playerID, itemConfigIDs)); err != nil {
		return err
	}
	g.calls = append(g.calls, grantCall{playerID: playerID, items: append([]uint32(nil), itemConfigIDs...), key: key})
	return nil
}

func (g *fakeGranter) GrantItems(_ context.Context, playerID uint64, items []data.StackGrant, goldAmount uint64, key string) error {
	if g.failStack {
		return simpleErr("stack grant failed")
	}
	if err := g.claimKey(key, fmt.Sprintf("items:%d:%v:gold=%d", playerID, items, goldAmount)); err != nil {
		return err
	}
	cpy := append([]data.StackGrant(nil), items...)
	g.stackCalls = append(g.stackCalls, stackGrantCall{playerID: playerID, items: cpy, gold: goldAmount, key: key})
	return nil
}

func (g *fakeGranter) ConsumeBattleItem(_ context.Context, playerID uint64, itemID uint32, count int64, key string) error {
	g.consumeTries++
	if g.consumeErr != nil {
		return g.consumeErr
	}
	if g.failConsume {
		return simpleErr("consume failed")
	}
	g.consumeCalls = append(g.consumeCalls, consumeCall{playerID: playerID, itemID: itemID, count: count, key: key})
	return nil
}

func (g *fakeGranter) DiscardBattleItem(_ context.Context, playerID uint64, itemID uint32, count int64, key string) error {
	g.discardTries++
	if g.discardErr != nil {
		return g.discardErr
	}
	if g.failDiscard {
		return simpleErr("discard failed")
	}
	g.discardCalls = append(g.discardCalls, consumeCall{playerID: playerID, itemID: itemID, count: count, key: key})
	return nil
}

// fakeMailSender 捕获 SendOverflowMail 调用;failAll=true 时恒返错(验证转邮件失败保留出箱行)。
type fakeMailSender struct {
	calls   []grantCall
	failAll bool
}

func (m *fakeMailSender) SendOverflowMail(_ context.Context, playerID uint64, itemConfigIDs []uint32, key string) error {
	if m.failAll {
		return simpleErr("mail down")
	}
	m.calls = append(m.calls, grantCall{playerID: playerID, items: append([]uint32(nil), itemConfigIDs...), key: key})
	return nil
}

// simpleErr 是测试用轻量 error(避免多引一个包)。
type simpleErr string

func (e simpleErr) Error() string { return string(e) }

func newTestUsecase(repo data.BattleRepo, pusher PlayerUpdatePusher) *BattleResultUsecase {
	cfg := conf.BattleConf{EloKFactor: 32, BaseMMR: 1500, TerminalReleaseGrace: config.Duration(5 * time.Second)}
	return NewBattleResultUsecase(repo, NewStaticMMRReader(cfg.BaseMMR), pusher, nil, cfg)
}

// ── Elo ───────────────────────────────────────────────────────────────────────

func TestEloDeltasEqualSymmetric(t *testing.T) {
	dA, dB := eloDeltas(1500, 1500, 32, winnerTeamA)
	if dA != 16 || dB != -16 {
		t.Fatalf("equal MMR A win: got (%d,%d) want (16,-16)", dA, dB)
	}
	dA, dB = eloDeltas(1500, 1500, 32, winnerTeamB)
	if dA != -16 || dB != 16 {
		t.Fatalf("equal MMR B win: got (%d,%d) want (-16,16)", dA, dB)
	}
}

func TestEloDeltasDrawSymmetric(t *testing.T) {
	dA, dB := eloDeltas(1500, 1500, 32, winnerTeamDraw)
	if dA != 0 || dB != 0 {
		t.Fatalf("equal MMR draw: got (%d,%d) want (0,0)", dA, dB)
	}
}

func TestEloDeltasFavoriteWinsLess(t *testing.T) {
	// A 队远强(1900 vs 1500),A 赢应远小于 K/2;B 若爆冷赢应远大于 K/2。
	dStrongWin, _ := eloDeltas(1900, 1500, 32, winnerTeamA)
	dWeakWinA, dWeakWinB := eloDeltas(1900, 1500, 32, winnerTeamB)
	if dStrongWin >= 16 {
		t.Fatalf("favorite win delta should be < 16, got %d", dStrongWin)
	}
	if dWeakWinB <= 16 {
		t.Fatalf("underdog win delta should be > 16, got %d", dWeakWinB)
	}
	// K 守恒(K 相等时两队 delta 互为相反数)
	if dWeakWinA != -dWeakWinB {
		t.Fatalf("K conservation broken: dA=%d dB=%d", dWeakWinA, dWeakWinB)
	}
}

// ── ReportResult ──────────────────────────────────────────────────────────────

func TestReportResultAssignsMMRAndIdempotent(t *testing.T) {
	repo := newFakeRepo()
	pusher := &fakePusher{}
	uc := newTestUsecase(repo, pusher)

	result := &battlev1.BattleResult{
		MatchId:    100,
		WinnerTeam: winnerTeamA,
		EndedAtMs:  1234,
		Stats: []*battlev1.PlayerStats{
			{PlayerId: 1, Team: 0, MmrDelta: 999}, // DS 上报的脏值,应被覆盖
			{PlayerId: 2, Team: 0},
			{PlayerId: 3, Team: 1},
			{PlayerId: 4, Team: 1},
		},
	}

	already, err := uc.ReportResult(context.Background(), result, 0)
	if err != nil {
		t.Fatalf("ReportResult err: %v", err)
	}
	if already {
		t.Fatal("first report should not be alreadyRecorded")
	}
	// outcome 缺省补 NORMAL
	if result.GetOutcome() != battlev1.BattleOutcome_BATTLE_OUTCOME_NORMAL {
		t.Fatalf("outcome got %v want NORMAL", result.GetOutcome())
	}
	// 等分队伍:A 队 +16,B 队 -16(覆盖 DS 脏值)
	for _, s := range result.GetStats() {
		want := int32(16)
		if s.GetTeam() == 1 {
			want = -16
		}
		if s.GetMmrDelta() != want {
			t.Fatalf("player %d mmr_delta got %d want %d", s.GetPlayerId(), s.GetMmrDelta(), want)
		}
	}
	// 出箱象驱动发布后才推 player.update(W4 ⑨ 事务出箱)
	n, err := uc.publishOutboxBatch(context.Background())
	if err != nil {
		t.Fatalf("publishOutboxBatch err: %v", err)
	}
	if n != 4 || len(pusher.events) != 4 {
		t.Fatalf("expected 4 player.update pushes, got published=%d events=%d", n, len(pusher.events))
	}
	if len(repo.outbox) != 0 {
		t.Fatalf("outbox should be drained, got %d", len(repo.outbox))
	}

	// 幂等:再报一次同 match_id → alreadyRecorded
	already2, err := uc.ReportResult(context.Background(), result, 0)
	if err != nil {
		t.Fatalf("second ReportResult err: %v", err)
	}
	if !already2 {
		t.Fatal("second report should be alreadyRecorded")
	}
}

func TestReportResultValidation(t *testing.T) {
	uc := newTestUsecase(newFakeRepo(), &fakePusher{})
	if _, err := uc.ReportResult(context.Background(), &battlev1.BattleResult{MatchId: 0}, 0); err == nil {
		t.Fatal("expected error for match_id=0")
	}
	if _, err := uc.ReportResult(context.Background(), &battlev1.BattleResult{MatchId: 1}, 0); err == nil {
		t.Fatal("expected error for empty stats")
	}
}

// TestReportResultAbandonedForcesZeroDelta 守住风险入口:battle.result 路径若误报 / 伪造
// Outcome=ABANDONED,ReportResult 必须强制 mmr_delta 全 0(不走 assignMMR),
// 防 DS 不可信地通过 abandoned 改玩家段位(不变量 §4/§6)。
func TestReportResultAbandonedForcesZeroDelta(t *testing.T) {
	repo := newFakeRepo()
	pusher := &fakePusher{}
	uc := newTestUsecase(repo, pusher)

	result := &battlev1.BattleResult{
		MatchId:    300,
		WinnerTeam: winnerTeamA, // 即便伪造了胜方,abandoned 也不许据此加分
		Outcome:    battlev1.BattleOutcome_BATTLE_OUTCOME_ABANDONED,
		EndedAtMs:  4321,
		Stats: []*battlev1.PlayerStats{
			{PlayerId: 1, Team: 0, MmrDelta: 50}, // DS 上报脏值,应被清零
			{PlayerId: 2, Team: 0, MmrDelta: 50},
			{PlayerId: 3, Team: 1, MmrDelta: -50},
			{PlayerId: 4, Team: 1, MmrDelta: -50},
		},
	}

	already, err := uc.ReportResult(context.Background(), result, 0)
	if err != nil {
		t.Fatalf("ReportResult abandoned err: %v", err)
	}
	if already {
		t.Fatal("first abandoned report should not be alreadyRecorded")
	}
	// outcome 保持 ABANDONED(不被改写成 NORMAL)
	if result.GetOutcome() != battlev1.BattleOutcome_BATTLE_OUTCOME_ABANDONED {
		t.Fatalf("outcome got %v want ABANDONED", result.GetOutcome())
	}
	// 所有玩家 delta 必须被强制清零
	for _, s := range result.GetStats() {
		if s.GetMmrDelta() != 0 {
			t.Fatalf("abandoned-via-ReportResult player %d mmr_delta got %d want 0", s.GetPlayerId(), s.GetMmrDelta())
		}
	}
	// 落库记录里也应是 delta 全 0
	rec, ok, _ := repo.GetResult(context.Background(), 300)
	if !ok {
		t.Fatal("abandoned record not saved")
	}
	for _, s := range rec.GetStats() {
		if s.GetMmrDelta() != 0 {
			t.Fatalf("saved abandoned player %d mmr_delta got %d want 0", s.GetPlayerId(), s.GetMmrDelta())
		}
	}
}

// TestReportResultDoesNotReclaimDS 守住 2026-07-03 根因修复:battle_result 结算落库后
// **绝不主动回收战斗 DS**(不在 ReportResult 同步响应路径 taskkill/DELETE DS)。
//
// 背景:DS 收到 ReportResult OK 后才 ended 心跳 → 通知客户端回大厅 → 自身 Agones Shutdown。
// 曾经 battle_result 在响应路径同步调 ds_allocator.ReleaseBattle(=taskkill/DELETE),抢在 DS
// 通知客户端之前把 DS 杀掉 → 客户端永远收不到回大厅通知,卡战斗态。修复:移除该调用,DS 生命周期
// 归 ds_allocator(ended 心跳 → killStrandedDS / Agones 自停)+ 15s 心跳超时 sweep 兜底。
//
// 本测试是架构回归护栏:battle_result 已无 DSReleaser 依赖(编译期保证),此处进一步断言正常 /
// abandoned 结算都能落库成功,证明 DS 回收已与结算响应路径解耦。若有人重新引入同步 DS 回收,
// 应先删除本测试并复审此根因,而非绕过。
func TestReportResultDoesNotReclaimDS(t *testing.T) {
	mkResult := func(matchID uint64, outcome battlev1.BattleOutcome) *battlev1.BattleResult {
		return &battlev1.BattleResult{
			MatchId:    matchID,
			WinnerTeam: winnerTeamA,
			Outcome:    outcome,
			EndedAtMs:  1000,
			Stats: []*battlev1.PlayerStats{
				{PlayerId: 1, Team: 0},
				{PlayerId: 2, Team: 1},
			},
		}
	}

	// 1) 正常结算:落库成功、返回 !alreadyRecorded;不依赖任何 DS 回收器(构造签名已无 DSReleaser)
	t.Run("normal_settle_persists_without_ds_reclaim", func(t *testing.T) {
		repo := newFakeRepo()
		uc := newTestUsecase(repo, &fakePusher{})
		already, err := uc.ReportResult(context.Background(), mkResult(500, battlev1.BattleOutcome_BATTLE_OUTCOME_UNSPECIFIED), 0)
		if err != nil {
			t.Fatalf("ReportResult err: %v", err)
		}
		if already {
			t.Fatal("first report should not be alreadyRecorded")
		}
		if _, ok, _ := repo.GetResult(context.Background(), 500); !ok {
			t.Fatal("normal settlement must be persisted")
		}
		// 幂等命中(同 match_id 再报)仍成功,不产生任何 DS 副作用
		if already2, err := uc.ReportResult(context.Background(), mkResult(500, battlev1.BattleOutcome_BATTLE_OUTCOME_UNSPECIFIED), 0); err != nil {
			t.Fatalf("second ReportResult err: %v", err)
		} else if !already2 {
			t.Fatal("second report of same match should be alreadyRecorded")
		}
	})

	// 2) abandoned(防伪兜底 / sweep 补偿)同样落库成功,不涉及 DS 回收
	t.Run("abandoned_settle_persists_without_ds_reclaim", func(t *testing.T) {
		repo := newFakeRepo()
		uc := newTestUsecase(repo, &fakePusher{})
		if _, err := uc.ReportResult(context.Background(), mkResult(501, battlev1.BattleOutcome_BATTLE_OUTCOME_ABANDONED), 0); err != nil {
			t.Fatalf("ReportResult abandoned err: %v", err)
		}
		if _, ok, _ := repo.GetResult(context.Background(), 501); !ok {
			t.Fatal("abandoned settlement must be persisted")
		}
		if err := uc.HandleAbandoned(context.Background(), 502, []uint64{1, 2}, 5, "ranked_5v5", 0); err != nil {
			t.Fatalf("HandleAbandoned err: %v", err)
		}
		if _, ok, _ := repo.GetResult(context.Background(), 502); !ok {
			t.Fatal("HandleAbandoned compensation must be persisted")
		}
	})
}

// ── HandleAbandoned ───────────────────────────────────────────────────────────

func TestHandleAbandonedZeroDeltaIdempotent(t *testing.T) {
	repo := newFakeRepo()
	pusher := &fakePusher{}
	uc := newTestUsecase(repo, pusher)

	players := []uint64{10, 11, 12}
	if err := uc.HandleAbandoned(context.Background(), 200, players, 5, "ranked_5v5", 0); err != nil {
		t.Fatalf("HandleAbandoned err: %v", err)
	}

	rec, ok, _ := repo.GetResult(context.Background(), 200)
	if !ok {
		t.Fatal("abandoned record not saved")
	}
	if rec.GetOutcome() != battlev1.BattleOutcome_BATTLE_OUTCOME_ABANDONED {
		t.Fatalf("outcome got %v want ABANDONED", rec.GetOutcome())
	}
	if rec.GetWinnerTeam() != winnerTeamDraw {
		t.Fatalf("winner_team got %d want draw(%d)", rec.GetWinnerTeam(), winnerTeamDraw)
	}
	for _, s := range rec.GetStats() {
		if s.GetMmrDelta() != 0 {
			t.Fatalf("abandoned player %d mmr_delta got %d want 0", s.GetPlayerId(), s.GetMmrDelta())
		}
	}
	// 出箱驱动发布后应有 3 条 abandon 推送
	if _, perr := uc.publishOutboxBatch(context.Background()); perr != nil {
		t.Fatalf("publishOutboxBatch err: %v", perr)
	}
	if len(pusher.events) != 3 {
		t.Fatalf("expected 3 abandon pushes, got %d", len(pusher.events))
	}

	// 幂等:重复 abandoned 不再入箱 → 发布不再推
	pusher.events = nil
	if err := uc.HandleAbandoned(context.Background(), 200, players, 5, "ranked_5v5", 0); err != nil {
		t.Fatalf("second HandleAbandoned err: %v", err)
	}
	if _, perr := uc.publishOutboxBatch(context.Background()); perr != nil {
		t.Fatalf("publishOutboxBatch err: %v", perr)
	}
	if len(pusher.events) != 0 {
		t.Fatalf("idempotent abandoned should not push, got %d", len(pusher.events))
	}
}

func TestHandleAbandonedValidation(t *testing.T) {
	uc := newTestUsecase(newFakeRepo(), &fakePusher{})
	if err := uc.HandleAbandoned(context.Background(), 0, nil, 0, "", 0); err == nil {
		t.Fatal("expected error for match_id=0")
	}
}

// ── 出箱可靠发布(W4 ⑨,不变量 §4)──────────────────────────────────────────────

// reportFour 落一场 4 人正常结算,返回 usecase / repo / pusher。
func reportFour(t *testing.T, pusher PlayerUpdatePusher) (*BattleResultUsecase, *fakeRepo) {
	t.Helper()
	repo := newFakeRepo()
	uc := newTestUsecase(repo, pusher)
	result := &battlev1.BattleResult{
		MatchId:    700,
		WinnerTeam: winnerTeamA,
		EndedAtMs:  9999,
		Stats: []*battlev1.PlayerStats{
			{PlayerId: 1, Team: 0}, {PlayerId: 2, Team: 0},
			{PlayerId: 3, Team: 1}, {PlayerId: 4, Team: 1},
		},
	}
	if _, err := uc.ReportResult(context.Background(), result, 0); err != nil {
		t.Fatalf("ReportResult err: %v", err)
	}
	return uc, repo
}

// TestOutboxWrittenAtomicallyOnSave 落库即入箱:ReportResult 后出箱有 4 条待发布(尚未投递)。
func TestOutboxWrittenAtomicallyOnSave(t *testing.T) {
	pusher := &fakePusher{}
	_, repo := reportFour(t, pusher)
	if len(repo.outbox) != 4 {
		t.Fatalf("expected 4 outbox rows after save, got %d", len(repo.outbox))
	}
	if len(pusher.events) != 0 {
		t.Fatalf("nothing should be pushed before publisher runs, got %d", len(pusher.events))
	}
}

// TestOutboxReliablePublish_RetryUntilDelivered 模拟 Kafka 临时不可用:
// 前 2 轮发布全失败,出箱行保留;Kafka 恢复后第 3 轮全部投递并清空出箱(at-least-once 闭环)。
func TestOutboxReliablePublish_RetryUntilDelivered(t *testing.T) {
	// 每个失败批只发生 1 次推送调用(首条即失败立即中断),故 failFirst=2 = 前 2 轮失败。
	pusher := &fakePusher{failFirst: 2}
	uc, repo := reportFour(t, pusher)

	// 第 1 轮:首条即失败 → 0 投递,出箱仍 4 条
	if n, err := uc.publishOutboxBatch(context.Background()); err == nil || n != 0 {
		t.Fatalf("round1 expect fail n=0, got n=%d err=%v", n, err)
	}
	if len(repo.outbox) != 4 {
		t.Fatalf("round1 outbox should stay 4, got %d", len(repo.outbox))
	}
	if len(pusher.events) != 0 {
		t.Fatalf("round1 should deliver 0, got %d", len(pusher.events))
	}

	// 第 2 轮:仍在失败窗口内 → 继续 0 投递、出箱不减
	if n, _ := uc.publishOutboxBatch(context.Background()); n != 0 {
		t.Fatalf("round2 expect 0 published, got %d", n)
	}
	if len(repo.outbox) != 4 {
		t.Fatalf("round2 outbox should stay 4, got %d", len(repo.outbox))
	}

	// 第 3 轮:Kafka 恢复(calls 已过 failFirst)→ 全投递、出箱清空
	if n, err := uc.publishOutboxBatch(context.Background()); err != nil || n != 4 {
		t.Fatalf("round3 expect 4 published, got n=%d err=%v", n, err)
	}
	if len(repo.outbox) != 0 {
		t.Fatalf("round3 outbox should be drained, got %d", len(repo.outbox))
	}
	if len(pusher.events) != 4 {
		t.Fatalf("round3 should deliver 4, got %d", len(pusher.events))
	}

	// 第 4 轮:出箱已空 → 0 投递、无副作用
	if n, err := uc.publishOutboxBatch(context.Background()); err != nil || n != 0 {
		t.Fatalf("round4 expect 0 published, got n=%d err=%v", n, err)
	}
}

// TestOutboxPublishMidBatchFailureKeepsOrder 一批中途失败:前 k 条成功删除,失败处中断,
// 剩余行保留(下轮从失败处续传),保证同玩家事件按 id 顺序投递(不变量 §9)。
func TestOutboxPublishMidBatchFailureKeepsOrder(t *testing.T) {
	// 第 3 次推送单次失败:前 2 条成功删,第 3 条起保留。
	pusher := &fakePusher{failAt: 3}
	uc, repo := reportFour(t, pusher)

	n, err := uc.publishOutboxBatch(context.Background())
	if err == nil {
		t.Fatal("expected mid-batch failure")
	}
	if n != 2 {
		t.Fatalf("expected 2 published before failure, got %d", n)
	}
	if len(repo.outbox) != 2 {
		t.Fatalf("expected 2 outbox rows retained, got %d", len(repo.outbox))
	}
	// 保留的应是后 2 个玩家(id 顺序:player 3、4)
	if repo.outbox[0].PlayerID != 3 || repo.outbox[1].PlayerID != 4 {
		t.Fatalf("retained order wrong: %d,%d", repo.outbox[0].PlayerID, repo.outbox[1].PlayerID)
	}
}

// TestOutboxNilPusherNoLoss pusher 为 nil(kafka 未配置)时发布器不投递,但出箱行不丢。
func TestOutboxNilPusherNoLoss(t *testing.T) {
	uc, repo := reportFour(t, nil)
	if n, err := uc.publishOutboxBatch(context.Background()); err != nil || n != 0 {
		t.Fatalf("nil pusher expect 0 published no error, got n=%d err=%v", n, err)
	}
	if len(repo.outbox) != 4 {
		t.Fatalf("nil pusher must not lose outbox, got %d", len(repo.outbox))
	}
}

func TestMatchReleaseOutboxRetriesAndACKsOnlySuccess(t *testing.T) {
	releaser := &fakeMatchReleaser{err: errors.New("matchmaker unavailable")}
	uc, repo := reportFour(t, nil)
	uc.releaser = releaser
	if len(repo.matchReleaseOutbox) != 1 {
		t.Fatalf("match release outbox rows=%d want=1", len(repo.matchReleaseOutbox))
	}
	if n, err := uc.publishMatchReleaseBatch(context.Background()); err == nil || n != 0 {
		t.Fatalf("failed release must retain row: n=%d err=%v", n, err)
	}
	if len(repo.matchReleaseOutbox) != 1 || repo.matchReleaseOutbox[0].AttemptCount != 1 {
		t.Fatalf("failed release row lost/not deferred: %+v", repo.matchReleaseOutbox)
	}

	// 让测试行重新到期并恢复下游；明确成功后才 ACK。
	repo.matchReleaseOutbox[0].NextAttemptAtMs = 0
	releaser.err = nil
	if n, err := uc.publishMatchReleaseBatch(context.Background()); err != nil || n != 1 {
		t.Fatalf("successful release: n=%d err=%v", n, err)
	}
	if len(repo.matchReleaseOutbox) != 0 {
		t.Fatalf("successful release must ACK row: %+v", repo.matchReleaseOutbox)
	}
	if releaser.match != 700 || len(releaser.ids) != 4 {
		t.Fatalf("release payload wrong: match=%d ids=%v", releaser.match, releaser.ids)
	}
}

func TestMatchReleaseOutboxACKLossReplaysCommittedOperation(t *testing.T) {
	releaser := &ackLossMatchReleaser{}
	uc, repo := reportFour(t, nil)
	uc.releaser = releaser

	if n, err := uc.publishMatchReleaseBatch(context.Background()); err == nil || n != 0 {
		t.Fatalf("unknown ACK must retain outbox: n=%d err=%v", n, err)
	}
	if !releaser.committed || releaser.calls != 1 || len(repo.matchReleaseOutbox) != 1 ||
		repo.matchReleaseOutbox[0].AttemptCount != 1 {
		t.Fatalf("ACK-loss state not durable: committed=%v calls=%d rows=%+v",
			releaser.committed, releaser.calls, repo.matchReleaseOutbox)
	}

	repo.matchReleaseOutbox[0].NextAttemptAtMs = 0
	if n, err := uc.publishMatchReleaseBatch(context.Background()); err != nil || n != 1 {
		t.Fatalf("idempotent replay did not ACK: n=%d err=%v", n, err)
	}
	if releaser.calls != 2 || len(repo.matchReleaseOutbox) != 0 {
		t.Fatalf("committed release was not replayed exactly to success: calls=%d rows=%+v",
			releaser.calls, repo.matchReleaseOutbox)
	}
}

func TestIdempotentReplayRestoresMissingMatchReleaseOutbox(t *testing.T) {
	uc, repo := reportFour(t, nil)
	repo.matchReleaseOutbox = nil // 模拟历史 best-effort 已丢释放任务
	result := proto.Clone(repo.store[700]).(*battlev1.BattleResult)
	already, err := uc.ReportResult(context.Background(), result, 0)
	if err != nil || !already {
		t.Fatalf("idempotent replay: already=%v err=%v", already, err)
	}
	if len(repo.matchReleaseOutbox) != 1 || repo.matchReleaseOutbox[0].MatchID != 700 {
		t.Fatalf("idempotent replay did not restore release row: %+v", repo.matchReleaseOutbox)
	}
}

func TestIdempotentReplayMakesDeferredMatchReleaseImmediatelyDue(t *testing.T) {
	uc, repo := reportFour(t, nil)
	if len(repo.matchReleaseOutbox) != 1 {
		t.Fatal("missing initial release row")
	}
	originalOperation := repo.matchReleaseOutbox[0].OperationID
	repo.matchReleaseOutbox[0].AttemptCount = 7
	repo.matchReleaseOutbox[0].NextAttemptAtMs = time.Now().Add(time.Hour).UnixMilli()
	result := proto.Clone(repo.store[700]).(*battlev1.BattleResult)
	if already, err := uc.ReportResult(context.Background(), result, 0); err != nil || !already {
		t.Fatalf("idempotent replay: already=%v err=%v", already, err)
	}
	row := repo.matchReleaseOutbox[0]
	if row.NextAttemptAtMs != 0 || row.AttemptCount != 7 || row.OperationID != originalOperation {
		t.Fatalf("replay did not revive immutable release operation: %+v", row)
	}
}

type fakeTerminalRelay struct {
	calls             []data.TerminalReleaseRecord
	failFirst         int
	failErr           error
	finalizeCalls     []data.TerminalReleaseRecord
	finalizeFailFirst int
	finalizeErr       error
}

// concurrentTerminalRepo 模拟多个 battle_result 副本共享同一张 MySQL outbox。
// 只覆写 worker 会调用的三个方法；其余 BattleRepo 方法由嵌入的 fakeRepo 提供。
type concurrentTerminalRepo struct {
	*fakeRepo
	mu          sync.Mutex
	markCalls   int
	markWins    int
	deleteCalls int
}

func (r *concurrentTerminalRepo) FetchTerminalReleaseOutbox(_ context.Context, limit int, nowMs int64) ([]data.TerminalReleaseRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]data.TerminalReleaseRecord, 0, len(r.terminalOutbox))
	for _, rec := range r.terminalOutbox {
		if rec.ReleaseAfterMs <= nowMs {
			out = append(out, rec)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *concurrentTerminalRepo) MarkTerminalReleaseReleased(_ context.Context, id uint64, releasedAtMs int64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.markCalls++
	for i := range r.terminalOutbox {
		if r.terminalOutbox[i].ID != id || r.terminalOutbox[i].ReleasedAtMs != 0 {
			continue
		}
		r.terminalOutbox[i].ReleasedAtMs = releasedAtMs
		r.markWins++
		return true, nil
	}
	return false, nil
}

func (r *concurrentTerminalRepo) DeleteTerminalReleaseOutbox(_ context.Context, id uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleteCalls++
	for i, rec := range r.terminalOutbox {
		if rec.ID == id && rec.ReleasedAtMs > 0 {
			r.terminalOutbox = append(r.terminalOutbox[:i], r.terminalOutbox[i+1:]...)
			return nil
		}
	}
	return nil
}

func (r *concurrentTerminalRepo) terminalState() (rows int, releasedAt int64, markCalls, markWins, deleteCalls int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rows = len(r.terminalOutbox)
	if rows > 0 {
		releasedAt = r.terminalOutbox[0].ReleasedAtMs
	}
	return rows, releasedAt, r.markCalls, r.markWins, r.deleteCalls
}

// barrierTerminalRelay 强制两个 worker 都拿到同一个 phase 快照后再返回 RPC，
// 从而稳定覆盖并发 phase1 CAS 与并发 finalize/delete，而不是依赖调度时序。
type barrierTerminalRelay struct {
	mu            sync.Mutex
	want          int
	releaseCalls  int
	finalizeCalls int
	releaseReady  chan struct{}
	finalizeReady chan struct{}
}

func newBarrierTerminalRelay(want int) *barrierTerminalRelay {
	return &barrierTerminalRelay{
		want: want, releaseReady: make(chan struct{}), finalizeReady: make(chan struct{}),
	}
}

func (r *barrierTerminalRelay) ReleaseTerminal(ctx context.Context, _ data.TerminalReleaseRecord) error {
	r.mu.Lock()
	r.releaseCalls++
	if r.releaseCalls == r.want {
		close(r.releaseReady)
	}
	ready := r.releaseReady
	r.mu.Unlock()
	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *barrierTerminalRelay) FinalizeTerminal(ctx context.Context, _ data.TerminalReleaseRecord) error {
	r.mu.Lock()
	r.finalizeCalls++
	if r.finalizeCalls == r.want {
		close(r.finalizeReady)
	}
	ready := r.finalizeReady
	r.mu.Unlock()
	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *barrierTerminalRelay) counts() (release, finalize int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.releaseCalls, r.finalizeCalls
}

func (r *fakeTerminalRelay) FinalizeTerminal(_ context.Context, rec data.TerminalReleaseRecord) error {
	r.finalizeCalls = append(r.finalizeCalls, rec)
	if len(r.finalizeCalls) <= r.finalizeFailFirst {
		if r.finalizeErr != nil {
			return r.finalizeErr
		}
		return errors.New("Redis finalize result unknown")
	}
	return nil
}

func (r *fakeTerminalRelay) ReleaseTerminal(_ context.Context, rec data.TerminalReleaseRecord) error {
	r.calls = append(r.calls, rec)
	if len(r.calls) <= r.failFirst {
		if r.failErr != nil {
			return r.failErr
		}
		return errors.New("redis or k8s result unknown")
	}
	return nil
}

func terminalProof(matchID uint64, pod, jti string, gen uint64) data.TerminalReleaseRecord {
	nowMs := time.Now().UnixMilli()
	return data.TerminalReleaseRecord{
		MatchID: matchID, AllocationID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		DSPodName: pod, GameserverUID: "uid-900", InstanceEpoch: 3,
		AuthGen: gen, AuthJTI: jti, AuthExpMs: nowMs + 60_000,
		AuthKid: "kid-1", AuthTokenSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		AuthWriterEpoch: auth.DSAuthWriterEpochV2, AuthorizedAtMs: nowMs,
		PlayerIDs: []uint64{1, 2},
	}
}

func terminalResult(matchID uint64, pod string) *battlev1.BattleResult {
	return &battlev1.BattleResult{
		MatchId: matchID, DsPodName: pod, WinnerTeam: winnerTeamA, EndedAtMs: time.Now().UnixMilli(),
		Stats: []*battlev1.PlayerStats{{PlayerId: 1, Team: 0}, {PlayerId: 2, Team: 1}},
	}
}

func TestTerminalReleaseProofCommitsWithBattleAndGrace(t *testing.T) {
	repo := newFakeRepo()
	uc := newTestUsecase(repo, &fakePusher{})
	proof := terminalProof(800, "battle-800", "old-jti", 7)
	before := time.Now().UnixMilli()
	already, err := uc.ReportAuthorizedResult(context.Background(), terminalResult(800, "battle-800"), proof, 0)
	if err != nil || already {
		t.Fatalf("authorized report already=%v err=%v", already, err)
	}
	if len(repo.store) != 1 || len(repo.terminalOutbox) != 1 {
		t.Fatalf("battle/outbox not committed together: battles=%d terminal=%d", len(repo.store), len(repo.terminalOutbox))
	}
	got := repo.terminalOutbox[0]
	if got.AuthJTI != "old-jti" || got.AuthGen != 7 || got.AuthorizedAtMs != proof.AuthorizedAtMs {
		t.Fatalf("persisted proof drifted: %+v", got)
	}
	if got.ReleaseAfterMs < before+5_000 {
		t.Fatalf("release grace missing: release_after=%d before=%d", got.ReleaseAfterMs, before)
	}
}

func TestAuthorizedResultRosterMustExactlyMatchCanonicalBattle(t *testing.T) {
	tests := []struct {
		name  string
		stats []*battlev1.PlayerStats
	}{
		{name: "missing", stats: []*battlev1.PlayerStats{{PlayerId: 1}}},
		{name: "outsider", stats: []*battlev1.PlayerStats{{PlayerId: 1}, {PlayerId: 3}}},
		{name: "duplicate", stats: []*battlev1.PlayerStats{{PlayerId: 1}, {PlayerId: 1}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := newFakeRepo()
			uc := newTestUsecase(repo, &fakePusher{})
			result := terminalResult(899, "battle-899")
			result.Stats = tc.stats
			if _, err := uc.ReportAuthorizedResult(
				context.Background(), result, terminalProof(899, "battle-899", "j1", 1), 0,
			); errcode.As(err) != errcode.ErrUnauthorized {
				t.Fatalf("code=%v err=%v", errcode.As(err), err)
			}
			if len(repo.store) != 0 || len(repo.terminalOutbox) != 0 || len(repo.matchReleaseOutbox) != 0 {
				t.Fatalf("rejected roster wrote state: store=%d terminal=%d release=%d",
					len(repo.store), len(repo.terminalOutbox), len(repo.matchReleaseOutbox))
			}
		})
	}
}

func TestTerminalReleaseDBFailureNeverReturnsSuccess(t *testing.T) {
	repo := newFakeRepo()
	repo.saveErr = errors.New("mysql commit failed")
	uc := newTestUsecase(repo, &fakePusher{})
	if already, err := uc.ReportAuthorizedResult(
		context.Background(), terminalResult(801, "battle-801"), terminalProof(801, "battle-801", "j1", 1), 0,
	); err == nil || already {
		t.Fatalf("DB failure was accepted: already=%v err=%v", already, err)
	}
	if len(repo.store) != 0 || len(repo.terminalOutbox) != 0 {
		t.Fatalf("DB failure left partial state: battles=%d terminal=%d", len(repo.store), len(repo.terminalOutbox))
	}
}

func TestTerminalReleaseOldProofSurvivesCredentialRotationReplay(t *testing.T) {
	repo := newFakeRepo()
	uc := newTestUsecase(repo, &fakePusher{})
	result := terminalResult(802, "battle-802")
	oldProof := terminalProof(802, "battle-802", "old-jti", 7)
	if already, err := uc.ReportAuthorizedResult(context.Background(), result, oldProof, 0); err != nil || already {
		t.Fatalf("first report already=%v err=%v", already, err)
	}
	newProof := terminalProof(802, "battle-802", "new-jti", 8)
	if already, err := uc.ReportAuthorizedResult(context.Background(), proto.Clone(result).(*battlev1.BattleResult), newProof, 0); err != nil || !already {
		t.Fatalf("rotated replay already=%v err=%v", already, err)
	}
	if len(repo.terminalOutbox) != 1 {
		t.Fatalf("rotated replay wrote another terminal row: %d", len(repo.terminalOutbox))
	}
	if got := repo.terminalOutbox[0]; got.AuthGen != oldProof.AuthGen || got.AuthJTI != oldProof.AuthJTI {
		t.Fatalf("rotated replay replaced durable proof: got gen=%d jti=%q", got.AuthGen, got.AuthJTI)
	}
}

func TestTerminalReleaseRetriesUnknownAndAckFailure(t *testing.T) {
	repo := newFakeRepo()
	uc := newTestUsecase(repo, &fakePusher{})
	if _, err := uc.ReportAuthorizedResult(
		context.Background(), terminalResult(803, "battle-803"), terminalProof(803, "battle-803", "j1", 1), 0,
	); err != nil {
		t.Fatal(err)
	}
	// 跳过通知宽限窗，直接测试 worker 故障矩阵。
	repo.terminalOutbox[0].ReleaseAfterMs = time.Now().Add(-time.Second).UnixMilli()
	relay := &fakeTerminalRelay{failFirst: 1}
	uc.SetTerminalReleaseRelay(relay)
	if n, err := uc.publishTerminalReleaseBatch(context.Background()); err != nil || n != 0 {
		t.Fatalf("unknown round n=%d err=%v", n, err)
	}
	if len(repo.terminalOutbox) != 1 {
		t.Fatal("unknown Redis/K8s result ACKed outbox")
	}

	// phase1 明确成功后只把 MySQL 行 durable 推进为 released，绝不在同轮 finalize/delete。
	if n, err := uc.publishTerminalReleaseBatch(context.Background()); err != nil || n != 0 {
		t.Fatalf("phase1 mark round n=%d err=%v", n, err)
	}
	if len(repo.terminalOutbox) != 1 || repo.terminalOutbox[0].ReleasedAtMs <= 0 ||
		len(relay.calls) != 2 || len(relay.finalizeCalls) != 0 {
		t.Fatalf("phase1 durable state invalid: rows=%+v release_calls=%d finalize_calls=%d",
			repo.terminalOutbox, len(relay.calls), len(relay.finalizeCalls))
	}

	repo.terminalDeleteErr = errors.New("mysql ACK failed")
	if n, err := uc.publishTerminalReleaseBatch(context.Background()); err == nil || n != 0 {
		t.Fatalf("finalize ACK failure round n=%d err=%v", n, err)
	}
	if len(repo.terminalOutbox) != 1 || len(relay.calls) != 2 || len(relay.finalizeCalls) != 1 {
		t.Fatalf("finalize ACK failure lost retry state: rows=%d release_calls=%d finalize_calls=%d",
			len(repo.terminalOutbox), len(relay.calls), len(relay.finalizeCalls))
	}

	repo.terminalDeleteErr = nil
	if n, err := uc.publishTerminalReleaseBatch(context.Background()); err != nil || n != 1 {
		t.Fatalf("recovery round n=%d err=%v", n, err)
	}
	if len(repo.terminalOutbox) != 0 || len(relay.calls) != 2 || len(relay.finalizeCalls) != 2 {
		t.Fatalf("recovery did not close: rows=%d release_calls=%d finalize_calls=%d",
			len(repo.terminalOutbox), len(relay.calls), len(relay.finalizeCalls))
	}
}

func TestTerminalReleasePhase1DBMarkFailureSurvivesWorkerRestart(t *testing.T) {
	repo := newFakeRepo()
	uc := newTestUsecase(repo, &fakePusher{})
	if _, err := uc.ReportAuthorizedResult(
		context.Background(), terminalResult(805, "battle-805"), terminalProof(805, "battle-805", "j1", 1), 0,
	); err != nil {
		t.Fatal(err)
	}
	repo.terminalOutbox[0].ReleaseAfterMs = time.Now().Add(-time.Second).UnixMilli()
	firstRelay := &fakeTerminalRelay{}
	uc.SetTerminalReleaseRelay(firstRelay)
	repo.terminalMarkErr = errors.New("mysql unavailable after UID delete")
	if n, err := uc.publishTerminalReleaseBatch(context.Background()); err == nil || n != 0 {
		t.Fatalf("uncommitted mark failure n=%d err=%v", n, err)
	}
	if repo.terminalOutbox[0].ReleasedAtMs != 0 || len(firstRelay.calls) != 1 || len(firstRelay.finalizeCalls) != 0 {
		t.Fatalf("uncommitted mark advanced phase: row=%+v relay=%+v", repo.terminalOutbox[0], firstRelay)
	}

	// 模拟进程重启：DB 行仍是 pending，必须安全重放 phase1 UID delete，不可直接 finalize。
	repo.terminalMarkErr = nil
	restartedRelay := &fakeTerminalRelay{}
	restarted := newTestUsecase(repo, &fakePusher{})
	restarted.SetTerminalReleaseRelay(restartedRelay)
	if n, err := restarted.publishTerminalReleaseBatch(context.Background()); err != nil || n != 0 {
		t.Fatalf("restart phase1 n=%d err=%v", n, err)
	}
	if repo.terminalOutbox[0].ReleasedAtMs <= 0 || len(restartedRelay.calls) != 1 || len(restartedRelay.finalizeCalls) != 0 {
		t.Fatalf("restart did not durable-mark phase1: row=%+v relay=%+v", repo.terminalOutbox[0], restartedRelay)
	}
	if n, err := restarted.publishTerminalReleaseBatch(context.Background()); err != nil || n != 1 {
		t.Fatalf("restart finalize n=%d err=%v", n, err)
	}
}

func TestTerminalReleaseCommittedMarkUnknownRestartsAtFinalizeOnly(t *testing.T) {
	repo := newFakeRepo()
	uc := newTestUsecase(repo, &fakePusher{})
	if _, err := uc.ReportAuthorizedResult(
		context.Background(), terminalResult(806, "battle-806"), terminalProof(806, "battle-806", "j1", 1), 0,
	); err != nil {
		t.Fatal(err)
	}
	repo.terminalOutbox[0].ReleaseAfterMs = time.Now().Add(-time.Second).UnixMilli()
	repo.terminalMarkCommitThenErr = true
	firstRelay := &fakeTerminalRelay{}
	uc.SetTerminalReleaseRelay(firstRelay)
	if n, err := uc.publishTerminalReleaseBatch(context.Background()); err == nil || n != 0 {
		t.Fatalf("commit-then-response-loss n=%d err=%v", n, err)
	}
	if repo.terminalOutbox[0].ReleasedAtMs <= 0 || len(firstRelay.calls) != 1 {
		t.Fatalf("committed mark not visible: row=%+v calls=%d", repo.terminalOutbox[0], len(firstRelay.calls))
	}

	// 新进程按 durable DB state 只调 finalize，绝不再碰 Kubernetes delete。
	repo.terminalMarkCommitThenErr = false
	restartedRelay := &fakeTerminalRelay{}
	restarted := newTestUsecase(repo, &fakePusher{})
	restarted.SetTerminalReleaseRelay(restartedRelay)
	if n, err := restarted.publishTerminalReleaseBatch(context.Background()); err != nil || n != 1 {
		t.Fatalf("restart finalize-only n=%d err=%v", n, err)
	}
	if len(restartedRelay.calls) != 0 || len(restartedRelay.finalizeCalls) != 1 || len(repo.terminalOutbox) != 0 {
		t.Fatalf("restart repeated K8s or failed close: release=%d finalize=%d rows=%d",
			len(restartedRelay.calls), len(restartedRelay.finalizeCalls), len(repo.terminalOutbox))
	}
}

func TestTerminalReleaseConcurrentWorkersCASThenFinalizeOnly(t *testing.T) {
	base := newFakeRepo()
	proof := terminalProof(807, "battle-807", "j1", 1)
	proof.ID = 1
	proof.ReleaseAfterMs = time.Now().Add(-time.Second).UnixMilli()
	base.terminalOutbox = []data.TerminalReleaseRecord{proof}
	repo := &concurrentTerminalRepo{fakeRepo: base}
	relay := newBarrierTerminalRelay(2)
	workers := []*BattleResultUsecase{
		newTestUsecase(repo, &fakePusher{}),
		newTestUsecase(repo, &fakePusher{}),
	}
	for _, worker := range workers {
		worker.SetTerminalReleaseRelay(relay)
	}

	runRound := func() []error {
		errs := make([]error, len(workers))
		var wg sync.WaitGroup
		wg.Add(len(workers))
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		for i, worker := range workers {
			go func(i int, worker *BattleResultUsecase) {
				defer wg.Done()
				_, errs[i] = worker.publishTerminalReleaseBatch(ctx)
			}(i, worker)
		}
		wg.Wait()
		return errs
	}

	for i, err := range runRound() {
		if err != nil {
			t.Fatalf("phase1 worker %d: %v", i, err)
		}
	}
	rows, releasedAt, markCalls, markWins, deleteCalls := repo.terminalState()
	releaseCalls, finalizeCalls := relay.counts()
	if rows != 1 || releasedAt <= 0 || markCalls != 2 || markWins != 1 || deleteCalls != 0 {
		t.Fatalf("phase1 CAS state rows=%d released=%d mark_calls=%d wins=%d deletes=%d",
			rows, releasedAt, markCalls, markWins, deleteCalls)
	}
	if releaseCalls != 2 || finalizeCalls != 0 {
		t.Fatalf("phase1 relay calls release=%d finalize=%d", releaseCalls, finalizeCalls)
	}

	for i, err := range runRound() {
		if err != nil {
			t.Fatalf("phase2 worker %d: %v", i, err)
		}
	}
	rows, _, markCalls, markWins, deleteCalls = repo.terminalState()
	releaseCalls, finalizeCalls = relay.counts()
	if rows != 0 || markCalls != 2 || markWins != 1 || deleteCalls != 2 {
		t.Fatalf("phase2 DB state rows=%d mark_calls=%d wins=%d deletes=%d",
			rows, markCalls, markWins, deleteCalls)
	}
	if releaseCalls != 2 || finalizeCalls != 2 {
		t.Fatalf("phase2 repeated K8s path: release=%d finalize=%d", releaseCalls, finalizeCalls)
	}
}

func TestTerminalReleaseUIDMismatchNeverACKsOutbox(t *testing.T) {
	repo := newFakeRepo()
	uc := newTestUsecase(repo, &fakePusher{})
	if _, err := uc.ReportAuthorizedResult(
		context.Background(), terminalResult(804, "battle-804"), terminalProof(804, "battle-804", "j1", 1), 0,
	); err != nil {
		t.Fatal(err)
	}
	repo.terminalOutbox[0].ReleaseAfterMs = time.Now().Add(-time.Second).UnixMilli()
	relay := &fakeTerminalRelay{
		failFirst: 100,
		failErr: errcode.New(errcode.ErrDSAllocationFailed,
			"allocation/UID/epoch changed before UID-precondition release"),
	}
	uc.SetTerminalReleaseRelay(relay)
	for attempt := 0; attempt < 2; attempt++ {
		if n, err := uc.publishTerminalReleaseBatch(context.Background()); err != nil || n != 0 {
			t.Fatalf("UID mismatch attempt=%d n=%d err=%v", attempt, n, err)
		}
	}
	if len(repo.terminalOutbox) != 1 || len(relay.calls) != 2 {
		t.Fatalf("UID mismatch ACKed or stopped retrying: rows=%d calls=%d",
			len(repo.terminalOutbox), len(relay.calls))
	}
}

// ── 战斗装备掉落回写(W5 ④,drop 白名单过滤 + 事务出箱 + GrantInstances 幂等)──────────

// newDropUsecase 构造带 drop 白名单 + granter 的 usecase。whitelist 决定哪些 item_config_id 可落库。
func newDropUsecase(repo *fakeRepo, granter InstanceGranter, whitelist []uint32) *BattleResultUsecase {
	cfg := conf.BattleConf{EloKFactor: 32, BaseMMR: 1500, DropWhitelist: whitelist}
	uc := NewBattleResultUsecase(repo, NewStaticMMRReader(cfg.BaseMMR), &fakePusher{}, nil, cfg)
	if granter != nil {
		uc.SetInstanceGranter(granter)
	}
	return uc
}

// dropResult 组一场 2 人正常结算,player 1 掉落 drop1,player 2 掉落 drop2。
func dropResult(matchID uint64, drop1, drop2 []uint32) *battlev1.BattleResult {
	return &battlev1.BattleResult{
		MatchId:    matchID,
		WinnerTeam: winnerTeamA,
		EndedAtMs:  9999,
		Stats: []*battlev1.PlayerStats{
			{PlayerId: 1, Team: 0, DroppedItemConfigIds: drop1},
			{PlayerId: 2, Team: 1, DroppedItemConfigIds: drop2},
		},
	}
}

// TestDropWhitelistFilter DS 上报的掉落只有白名单内 ID 入 drop 出箱(DS 不可信)。
func TestDropWhitelistFilter(t *testing.T) {
	repo := newFakeRepo()
	uc := newDropUsecase(repo, &fakeGranter{}, []uint32{5001, 5002})
	// player 1 报 [5001(白), 9999(非白)];player 2 报 [8888(非白)]。
	if _, err := uc.ReportResult(context.Background(), dropResult(600, []uint32{5001, 9999}, []uint32{8888}), 0); err != nil {
		t.Fatalf("ReportResult err: %v", err)
	}
	// 只 player 1 有白名单内掉落 → drop 出箱 1 行,内容仅 [5001]。
	if len(repo.dropOutbox) != 1 {
		t.Fatalf("expected 1 drop outbox row, got %d", len(repo.dropOutbox))
	}
	d := repo.dropOutbox[0]
	if d.PlayerID != 1 || len(d.ItemConfigIDs) != 1 || d.ItemConfigIDs[0] != 5001 {
		t.Fatalf("drop outbox filtered wrong: player=%d items=%v", d.PlayerID, d.ItemConfigIDs)
	}
	if d.MatchID != 600 {
		t.Fatalf("drop outbox match_id got %d want 600", d.MatchID)
	}
}

// TestDropPerPlayerCap 恶意/异常 DS 重复上报海量白名单 ID → 每玩家按上限截断,
// 结算正常落库不回滚(防撑爆 battle_drop_outbox.item_config_ids VARCHAR(512))。
func TestDropPerPlayerCap(t *testing.T) {
	repo := newFakeRepo()
	cfg := conf.BattleConf{EloKFactor: 32, BaseMMR: 1500, DropWhitelist: []uint32{5001}, MaxDropPerPlayer: 3}
	uc := NewBattleResultUsecase(repo, NewStaticMMRReader(cfg.BaseMMR), &fakePusher{}, nil, cfg)
	uc.SetInstanceGranter(&fakeGranter{})
	flood := make([]uint32, 500)
	for i := range flood {
		flood[i] = 5001
	}
	if _, err := uc.ReportResult(context.Background(), dropResult(610, flood, nil), 0); err != nil {
		t.Fatalf("ReportResult err: %v", err)
	}
	if len(repo.dropOutbox) != 1 {
		t.Fatalf("expected 1 drop outbox row, got %d", len(repo.dropOutbox))
	}
	if got := len(repo.dropOutbox[0].ItemConfigIDs); got != 3 {
		t.Fatalf("per-player cap 3 not enforced, kept %d", got)
	}
}

// TestDropCapDefaults 未配置 → 默认 32;配置超硬上限 → 钳制到 46(VARCHAR(512) 安全上限)。
func TestDropCapDefaults(t *testing.T) {
	b := conf.BattleConf{}
	if got := b.MaxDropsPerPlayer(); got != 32 {
		t.Fatalf("default cap got %d want 32", got)
	}
	b.MaxDropPerPlayer = 100
	if got := b.MaxDropsPerPlayer(); got != 46 {
		t.Fatalf("hard cap got %d want 46", got)
	}
}

// TestDropEmptyWhitelistBlocksAll 白名单为空 → 任何掉落都不入库(安全默认)。
func TestDropEmptyWhitelistBlocksAll(t *testing.T) {
	repo := newFakeRepo()
	uc := newDropUsecase(repo, &fakeGranter{}, nil)
	if _, err := uc.ReportResult(context.Background(), dropResult(601, []uint32{5001}, []uint32{5002}), 0); err != nil {
		t.Fatalf("ReportResult err: %v", err)
	}
	if len(repo.dropOutbox) != 0 {
		t.Fatalf("empty whitelist must block all drops, got %d rows", len(repo.dropOutbox))
	}
}

// TestDropAbandonedNoDrops ABANDONED(DS 崩溃补偿)不产出任何掉落,即使 DS 上报了白名单内 ID。
func TestDropAbandonedNoDrops(t *testing.T) {
	repo := newFakeRepo()
	uc := newDropUsecase(repo, &fakeGranter{}, []uint32{5001})
	res := dropResult(602, []uint32{5001}, []uint32{5001})
	res.Outcome = battlev1.BattleOutcome_BATTLE_OUTCOME_ABANDONED
	if _, err := uc.ReportResult(context.Background(), res, 0); err != nil {
		t.Fatalf("ReportResult err: %v", err)
	}
	if len(repo.dropOutbox) != 0 {
		t.Fatalf("abandoned must produce no drops, got %d rows", len(repo.dropOutbox))
	}
}

// TestDropPublisherGrantsAndDrains 掉落出箱经发布器发放:调 GrantInstances(幂等键正确)并清空出箱。
func TestDropPublisherGrantsAndDrains(t *testing.T) {
	repo := newFakeRepo()
	granter := &fakeGranter{}
	uc := newDropUsecase(repo, granter, []uint32{5001, 5002})
	if _, err := uc.ReportResult(context.Background(), dropResult(603, []uint32{5001}, []uint32{5002}), 0); err != nil {
		t.Fatalf("ReportResult err: %v", err)
	}
	if len(repo.dropOutbox) != 2 {
		t.Fatalf("expected 2 drop outbox rows, got %d", len(repo.dropOutbox))
	}
	n, err := uc.publishDropBatch(context.Background())
	if err != nil || n != 2 {
		t.Fatalf("publishDropBatch expect 2 granted, got n=%d err=%v", n, err)
	}
	if len(repo.dropOutbox) != 0 {
		t.Fatalf("drop outbox should drain, got %d", len(repo.dropOutbox))
	}
	if len(granter.calls) != 2 {
		t.Fatalf("expected 2 grant calls, got %d", len(granter.calls))
	}
	// 幂等键 = battle_drop:{match_id}:{player_id}
	if granter.calls[0].key != "battle_drop:603:1" {
		t.Fatalf("idempotency key wrong: %s", granter.calls[0].key)
	}
}

// TestDropPublisherGrantsBattleGold 本局金币经同一条掉落出箱链发放(2026-08-22 多币种改造):
// 没有任何掉落的玩家也必须产出一条"纯货币"出箱行,发布时走 GrantItems 把金额带过去。
// 断言金额而不只是"调用过一次":金币与堆叠道具共用一次调用,漏传金额在类型上完全合法。
func TestDropPublisherGrantsBattleGold(t *testing.T) {
	repo := newFakeRepo()
	granter := &fakeGranter{}
	uc := newDropUsecase(repo, granter, []uint32{5001})
	res := dropResult(620, nil, nil)
	res.Stats[0].Gold = 250
	res.Stats[1].Gold = 0 // 无收益玩家不出行
	if _, err := uc.ReportResult(context.Background(), res, 0); err != nil {
		t.Fatalf("ReportResult err: %v", err)
	}
	if len(repo.dropOutbox) != 1 {
		t.Fatalf("纯金币收益应产出 1 行出箱, got %d: %+v", len(repo.dropOutbox), repo.dropOutbox)
	}
	if d := repo.dropOutbox[0]; d.PlayerID != 1 || d.CurrencyAmount != 250 || len(d.ItemConfigIDs) != 0 {
		t.Fatalf("纯金币出箱行不符: %+v", d)
	}
	n, err := uc.publishDropBatch(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("publishDropBatch n=%d err=%v", n, err)
	}
	if len(granter.stackCalls) != 1 {
		t.Fatalf("金币必须走 GrantItems, got %d 次", len(granter.stackCalls))
	}
	got := granter.stackCalls[0]
	if got.playerID != 1 || got.gold != 250 || len(got.items) != 0 {
		t.Fatalf("GrantItems 收到的金额/道具不符: %+v", got)
	}
	// 幂等键与掉落同源(没有道具时不加 :stack 后缀)。
	if got.key != "battle_drop:620:1" {
		t.Fatalf("金币发放幂等键=%q want=battle_drop:620:1", got.key)
	}
	if len(granter.calls) != 0 {
		t.Fatalf("纯金币不得触发实例发放, got %d", len(granter.calls))
	}
	if len(repo.dropOutbox) != 0 {
		t.Fatalf("发放成功后出箱应排空, got %d", len(repo.dropOutbox))
	}
}

// TestBattleGoldClampedToServerCap DS 上报超上限金币被**就地钳到服务端上限**(§9.6 数值不信 DS),
// 不是拒整场;且战绩落库与钱包发放读的是同一份 stats,两处必须是同一个钳后值。
func TestBattleGoldClampedToServerCap(t *testing.T) {
	repo := newFakeRepo()
	granter := &fakeGranter{}
	cfg := conf.BattleConf{EloKFactor: 32, BaseMMR: 1500, DropWhitelist: []uint32{5001}, MaxGoldPerPlayer: 500}
	uc := NewBattleResultUsecase(repo, NewStaticMMRReader(cfg.BaseMMR), &fakePusher{}, nil, cfg)
	uc.SetInstanceGranter(granter)

	res := dropResult(621, nil, nil)
	res.Stats[0].Gold = 1 << 40 // 恶意/出 bug 的 DS 报一个天文数字
	res.Stats[1].Gold = 0
	if _, err := uc.ReportResult(context.Background(), res, 0); err != nil {
		t.Fatalf("超限金币不得拒整场: %v", err)
	}
	if d := repo.dropOutbox[0]; d.CurrencyAmount != 500 {
		t.Fatalf("出箱金额应被钳到 500, got %d", d.CurrencyAmount)
	}
	if _, err := uc.publishDropBatch(context.Background()); err != nil {
		t.Fatalf("publishDropBatch err: %v", err)
	}
	if len(granter.stackCalls) != 1 || granter.stackCalls[0].gold != 500 {
		t.Fatalf("发放金额应为钳后 500, got %+v", granter.stackCalls)
	}
	// 战绩落库读的是同一份 stats:钳一次两处一致,不能出现"战绩显示天文数字、钱包只到 500"。
	if got := res.Stats[0].GetGold(); got != 500 {
		t.Fatalf("stats 必须被就地钳到 500, got %d", got)
	}
}

// TestBattleGoldSurvivesProgressStreamSuppression 走过实时进度通道的对局,战后金币照常入箱并发放。
//
// 2026-08-24 P0 回归:抑制条件 DropsSuppressed 曾包住整个入箱循环,连"纯货币、零道具"的行
// 一起跳过;而实时通道自己明确不发金币(progress.go 里 GrantItems 第三实参硬编码 0)。
// 两处代码互相指望对方发金币 → 任何走过实时通道的对局(= 拾取/击杀走第三通道的正常局)
// 战后金币永久丢失且零错误日志。这条用例同时钉住反向不变量:已被实时通道发过的道具不得再发一遍。
func TestBattleGoldSurvivesProgressStreamSuppression(t *testing.T) {
	repo := newFakeRepo()
	granter := &fakeGranter{}
	uc := newDropUsecase(repo, granter, []uint32{5001})
	repo.progressSeq[622] = 3 // 本场应用过实时进度事件 → 水位>0 → DropsSuppressed

	res := dropResult(622, []uint32{5001}, nil)
	res.Stats[0].Gold = 250
	if _, err := uc.ReportResult(context.Background(), res, 3); err != nil {
		t.Fatalf("ReportResult err: %v", err)
	}
	if len(repo.dropOutbox) != 1 {
		t.Fatalf("实时通道介入不该吞掉金币行, got %d 行: %+v", len(repo.dropOutbox), repo.dropOutbox)
	}
	d := repo.dropOutbox[0]
	if d.PlayerID != 1 || d.CurrencyAmount != 250 {
		t.Fatalf("金币出箱行不符: %+v", d)
	}
	// 道具部分必须被抑制掉:那些已经由实时通道逐事件发过了。
	if len(d.ItemConfigIDs) != 0 || len(d.StackItemConfigIDs) != 0 || len(d.InstanceItemConfigIDs) != 0 {
		t.Fatalf("已实时发放的道具不得再入箱(双发): %+v", d)
	}

	n, err := uc.publishDropBatch(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("publishDropBatch n=%d err=%v", n, err)
	}
	if len(granter.stackCalls) != 1 || granter.stackCalls[0].gold != 250 {
		t.Fatalf("金币必须照常发放: %+v", granter.stackCalls)
	}
	if len(granter.calls) != 0 {
		t.Fatalf("装备已由实时通道发放,结算路径不得重复发: %+v", granter.calls)
	}
	if len(repo.dropOutbox) != 0 {
		t.Fatalf("发放成功后出箱应排空, got %d", len(repo.dropOutbox))
	}
}

// TestDropPublisherForksKeysForEquipmentPlusGold 「爆装备 + 有金币、但没爆可堆叠道具」必须分叉幂等键。
//
// 2026-08-24 P0 回归:分叉条件曾只看 stacks/instances(`len(stacks)>0 && len(instances)>0`),
// 而发放条件在加金币时变成了 `len(stacks)>0 || CurrencyAmount>0` —— 本形态下两路发放
// 共用 baseKey:金币先发成功占了键,装备随即被判幂等冲突 → 整行返错 → 出箱行永不删,
// 装备永远发不到玩家手上,worker 每轮都在这行失败。fakeGranter 复刻了 inventory 的
// 键指纹冲突语义,修回归后这条用例会重新变红。
func TestDropPublisherForksKeysForEquipmentPlusGold(t *testing.T) {
	repo := newFakeRepo()
	granter := &fakeGranter{}
	uc := newDropUsecase(repo, granter, []uint32{5001})

	res := dropResult(623, []uint32{5001}, nil) // 白名单 fallback 判定为装备 → instances 非空、stacks 空
	res.Stats[0].Gold = 250
	if _, err := uc.ReportResult(context.Background(), res, 0); err != nil {
		t.Fatalf("ReportResult err: %v", err)
	}
	if len(repo.dropOutbox) != 1 {
		t.Fatalf("expected 1 drop outbox row, got %d", len(repo.dropOutbox))
	}
	if d := repo.dropOutbox[0]; len(d.StackItemConfigIDs) != 0 || len(d.InstanceItemConfigIDs) != 1 || d.CurrencyAmount != 250 {
		t.Fatalf("本用例要的形态是「货币 + 装备、无堆叠道具」, got %+v", d)
	}

	n, err := uc.publishDropBatch(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("publishDropBatch n=%d err=%v(装备+金币同行必须两路都成功)", n, err)
	}
	if len(granter.stackCalls) != 1 || len(granter.calls) != 1 {
		t.Fatalf("两路都要发:stack=%d instance=%d", len(granter.stackCalls), len(granter.calls))
	}
	stackKey, instanceKey := granter.stackCalls[0].key, granter.calls[0].key
	if stackKey == instanceKey {
		t.Fatalf("两路发放必须用不同幂等键, both=%q", stackKey)
	}
	if stackKey != "battle_drop:623:1:stack" || instanceKey != "battle_drop:623:1:instance" {
		t.Fatalf("子幂等键不符: stack=%q instance=%q", stackKey, instanceKey)
	}
	if granter.stackCalls[0].gold != 250 || len(granter.calls[0].items) != 1 || granter.calls[0].items[0] != 5001 {
		t.Fatalf("发放内容不符: stack=%+v instance=%+v", granter.stackCalls[0], granter.calls[0])
	}
	if len(repo.dropOutbox) != 0 {
		t.Fatalf("两路都成功后出箱行必须删除, got %d", len(repo.dropOutbox))
	}
}

// TestBuildDropOutboxPartitionsItemConfigIDs 钉住出箱三列的分区不变量:
//
//	ItemConfigIDs == StackItemConfigIDs ⊎ InstanceItemConfigIDs(不相交并集)
//
// 为什么要钉:data.SettleDropOutboxRow 的成行判据已改成看三列并集(丢行的陷阱被消除了),
// 但 ItemConfigIDs 仍然是**下游对账用的全量视图** —— 掉落审计、battle_drop_suppressed_by_progress
// 的 suppressed_items 计数、以及库里 item_config_ids 列都以它为准。buildDropOutbox 是这三列
// 唯一的生产者,它一旦漏 append(例如以后加第三种路由却忘了写进 allowed),
// 全仓没有第二处能发现,审计口径会悄悄少算。
//
// 把 buildDropOutbox 里 `allowed = append(allowed, id)` 删掉 / 只对某一路由 append,这条会变红。
func TestBuildDropOutboxPartitionsItemConfigIDs(t *testing.T) {
	uc := newDropUsecase(newFakeRepo(), &fakeGranter{}, nil)
	uc.SetBattleItemCatalog(mapBattleItemCatalog{
		7001: {Droppable: true, MaxStack: 99},                 // 可堆叠
		7002: {Equipment: true, Droppable: true, MaxStack: 1}, // 装备
		7003: {Droppable: true, MaxStack: 99},                 // 可堆叠
		7004: {Equipment: true, Droppable: true, MaxStack: 1}, // 装备
		7005: {MaxStack: 99},                                  // 配了但不可掉落 → 三列都不该出现
	})
	res := dropResult(640, []uint32{7001, 7002, 7005, 7003}, []uint32{7004})
	res.Stats[0].Gold = 10

	recs := uc.buildDropOutbox(context.Background(), res)
	if len(recs) != 2 {
		t.Fatalf("expected 2 rows, got %d: %+v", len(recs), recs)
	}
	for _, r := range recs {
		inAll := map[uint32]int{}
		for _, id := range r.ItemConfigIDs {
			inAll[id]++
		}
		routed := append(append([]uint32(nil), r.StackItemConfigIDs...), r.InstanceItemConfigIDs...)
		for _, id := range routed {
			if inAll[id] == 0 {
				t.Fatalf("路由列里的 %d 不在 ItemConfigIDs 里(审计视图漏项): %+v", id, r)
			}
		}
		// 不相交:每个 ID 恰好落进 stack / instance 之一,总数必须与全量视图一致。
		if len(routed) != len(r.ItemConfigIDs) {
			t.Fatalf("三列不构成不相交并集: all=%d stack=%d instance=%d (%+v)",
				len(r.ItemConfigIDs), len(r.StackItemConfigIDs), len(r.InstanceItemConfigIDs), r)
		}
		if inAll[7005] != 0 {
			t.Fatalf("不可掉落的 7005 混进了出箱行(DS 不可信,白名单是唯一闸): %+v", r)
		}
	}
	if p1 := recs[0]; len(p1.StackItemConfigIDs) != 2 || len(p1.InstanceItemConfigIDs) != 1 || p1.CurrencyAmount != 10 {
		t.Fatalf("player1 行形态不符(2 堆叠 + 1 装备 + 金币): %+v", p1)
	}
}

// ── 单行装备数超 inventory 单次上限 = **已知未修边界**(2026-08-24)──────────────

// TestDropPublisherOversizeInstanceRowStaysStuck 钉住**现状**:单行装备数超过 inventory 的
// ledger.detail 上限时,这一行出箱永久卡死 —— 每轮重试每轮失败,不转邮件、也不删除。
//
// 这是一条 characterization test(钉现状),不是"期望行为"。它存在的理由:
//   - 这个边界**先于**掉落金币 / 幂等键那批改造就存在,本轮明确**不修**;
//   - 上一轮曾试图在发布器侧拆批绕过它(grantInstancesBatched / grantInstanceChunk),
//     结果打破了"转邮件即终态"这条不变量:批 0 转邮件成功后循环继续,后面任一批失败
//     (最典型是 mail 响应丢包,见 drop_overflow_ack_loss_test.go)整行保留 → 下轮把已经
//     邮寄出去的批 0 再发一次。实测 8 件装备变 16 件,已整块回退。**拆批不是这个问题的解**;
//   - 正解是 expand 迁移把 inventory_ledger.detail 列加宽,或把 instance_id 挪进独立子表,
//     让"一次发放"仍是一次原子记账。那属于 inventory 域的 schema 演进,不在本服。
//
// 所以这条用例的作用是:谁哪天在本服又想"顺手拆个批",先在这里看见为什么不行;
// 等 inventory 侧真把上限抬上去了,这条会变红,提醒把它一起改掉。
func TestDropPublisherOversizeInstanceRowStaysStuck(t *testing.T) {
	repo := newFakeRepo()
	const gate = 8 // 近似 inventory 的 GrantInstancesDetailFits,只为让"超限"这个形态可达
	granter := &fakeGranter{maxInstancesPerCall: gate}
	mail := &fakeMailSender{}
	ids := make([]uint32, gate+1)
	for i := range ids {
		ids[i] = 5001 + uint32(i)
	}
	uc := newDropUsecaseWithMail(repo, granter, mail, ids)
	if _, err := uc.ReportResult(context.Background(), dropResult(633, ids, nil), 0); err != nil {
		t.Fatalf("ReportResult err: %v", err)
	}
	if d := repo.dropOutbox[0]; len(d.InstanceItemConfigIDs) != len(ids) {
		t.Fatalf("本用例要的形态是「一行 %d 件装备」, got %+v", len(ids), d)
	}

	// 连跑两轮:两轮都发不出去、行都还在 —— 这就是"永久卡死"的可观测形态。
	for round := 1; round <= 2; round++ {
		n, err := uc.publishDropBatch(context.Background())
		if err != nil {
			t.Fatalf("第 %d 轮 publishDropBatch 不该整批返错(单行失败只保留该行): %v", round, err)
		}
		if n != 0 {
			t.Fatalf("第 %d 轮不该有行发放成功, got n=%d", round, n)
		}
		if len(repo.dropOutbox) != 1 {
			t.Fatalf("第 %d 轮后出箱行必须原样保留(留待重试), got %d", round, len(repo.dropOutbox))
		}
	}
	if len(granter.calls) != 0 {
		t.Fatalf("超限时 inventory 侧不该有任何成功入账: %+v", granter.calls)
	}
	// 关键:ErrInvalidArg **不是** capacity-full,绝不能走溢出邮件 ——
	// 走了就等于把"发不出去"伪装成"已经寄给你了",装备实际两边都没有。
	if len(mail.calls) != 0 {
		t.Fatalf("detail 超长不是背包满,不得转邮件: %+v", mail.calls)
	}
}

// TestDropPublisherPerRowRetry 单玩家背包满(granter 恒返错)不阻塞其他玩家:失败行保留,成功行清空。
func TestDropPublisherPerRowRetry(t *testing.T) {
	repo := newFakeRepo()
	granter := &fakeGranter{failPlayer: 2} // player 2 背包满
	uc := newDropUsecase(repo, granter, []uint32{5001, 5002})
	if _, err := uc.ReportResult(context.Background(), dropResult(604, []uint32{5001}, []uint32{5002}), 0); err != nil {
		t.Fatalf("ReportResult err: %v", err)
	}
	n, err := uc.publishDropBatch(context.Background())
	if err != nil {
		t.Fatalf("publishDropBatch err: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 granted (player 1), got %d", n)
	}
	// player 2 失败行保留下轮重试;player 1 已发放清空。
	if len(repo.dropOutbox) != 1 || repo.dropOutbox[0].PlayerID != 2 {
		t.Fatalf("failed row for player 2 must be retained, got %+v", repo.dropOutbox)
	}
}

// TestDropIdempotentReplay 幂等命中(同 match 再报)不重复写 drop 出箱。
func TestDropIdempotentReplay(t *testing.T) {
	repo := newFakeRepo()
	uc := newDropUsecase(repo, &fakeGranter{}, []uint32{5001})
	res := dropResult(605, []uint32{5001}, nil)
	if _, err := uc.ReportResult(context.Background(), res, 0); err != nil {
		t.Fatalf("first ReportResult err: %v", err)
	}
	if already, err := uc.ReportResult(context.Background(), dropResult(605, []uint32{5001}, nil), 0); err != nil || !already {
		t.Fatalf("second report expect alreadyRecorded, got already=%v err=%v", already, err)
	}
	if len(repo.dropOutbox) != 1 {
		t.Fatalf("idempotent replay must not duplicate drop outbox, got %d rows", len(repo.dropOutbox))
	}
}

// TestDropNilGranterNoLoss granter 为 nil(inventory_addr 未配)→ 发布器不发放,但出箱行不丢。
func TestDropNilGranterNoLoss(t *testing.T) {
	repo := newFakeRepo()
	uc := newDropUsecase(repo, nil, []uint32{5001})
	if _, err := uc.ReportResult(context.Background(), dropResult(606, []uint32{5001}, nil), 0); err != nil {
		t.Fatalf("ReportResult err: %v", err)
	}
	if n, err := uc.publishDropBatch(context.Background()); err != nil || n != 0 {
		t.Fatalf("nil granter expect 0 granted no error, got n=%d err=%v", n, err)
	}
	if len(repo.dropOutbox) != 1 {
		t.Fatalf("nil granter must not lose drop outbox, got %d", len(repo.dropOutbox))
	}
}

// ── 背包满溢出转邮件(W5 ④+,ErrInventoryCapacityFull → mail.SendOverflowMail)──────

// newDropUsecaseWithMail 在 newDropUsecase 基础上再注入 mailSender。
func newDropUsecaseWithMail(repo *fakeRepo, granter InstanceGranter, mail MailSender, whitelist []uint32) *BattleResultUsecase {
	uc := newDropUsecase(repo, granter, whitelist)
	if mail != nil {
		uc.SetMailSender(mail)
	}
	return uc
}

// TestDropOverflowToMailOnCapacityFull 背包满 + 已配 mail:掉落转个人邮件(源键传递正确),出箱行清空。
func TestDropOverflowToMailOnCapacityFull(t *testing.T) {
	repo := newFakeRepo()
	granter := &fakeGranter{capacityFull: true}
	mail := &fakeMailSender{}
	uc := newDropUsecaseWithMail(repo, granter, mail, []uint32{5001, 5002})
	if _, err := uc.ReportResult(context.Background(), dropResult(700, []uint32{5001}, []uint32{5002}), 0); err != nil {
		t.Fatalf("ReportResult err: %v", err)
	}
	n, err := uc.publishDropBatch(context.Background())
	if err != nil {
		t.Fatalf("publishDropBatch err: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 rows overflow-mailed+drained, got %d", n)
	}
	if len(repo.dropOutbox) != 0 {
		t.Fatalf("drop outbox should drain after overflow-mail, got %d", len(repo.dropOutbox))
	}
	if len(mail.calls) != 2 {
		t.Fatalf("expected 2 overflow mail calls, got %d", len(mail.calls))
	}
	// 直发 granter 应无成功入账(全 capacity-full),掉落全部走邮件。
	if len(granter.calls) != 0 {
		t.Fatalf("expected 0 direct grant calls on capacity-full, got %d", len(granter.calls))
	}
	// 溢出邮件必须传与直发相同的源键 battle_drop:{match}:{player}(领取时同键去重)。
	if mail.calls[0].key != "battle_drop:700:1" {
		t.Fatalf("overflow mail key wrong: %s", mail.calls[0].key)
	}
	if len(mail.calls[0].items) != 1 || mail.calls[0].items[0] != 5001 {
		t.Fatalf("overflow mail items wrong: %v", mail.calls[0].items)
	}
}

// TestDropOverflowMailFailureKeepsRow 转邮件失败 → 出箱行保留下轮重试(不丢),granted=0。
func TestDropOverflowMailFailureKeepsRow(t *testing.T) {
	repo := newFakeRepo()
	granter := &fakeGranter{capacityFull: true}
	mail := &fakeMailSender{failAll: true}
	uc := newDropUsecaseWithMail(repo, granter, mail, []uint32{5001})
	if _, err := uc.ReportResult(context.Background(), dropResult(701, []uint32{5001}, nil), 0); err != nil {
		t.Fatalf("ReportResult err: %v", err)
	}
	n, err := uc.publishDropBatch(context.Background())
	if err != nil {
		t.Fatalf("publishDropBatch err: %v", err)
	}
	if n != 0 {
		t.Fatalf("mail failure must not drain, got granted=%d", n)
	}
	if len(repo.dropOutbox) != 1 {
		t.Fatalf("mail failure must retain drop outbox row, got %d", len(repo.dropOutbox))
	}
}

// TestDropCapacityFullNoMailSenderKeepsRow 背包满但未配 mail → 退化为历史行为:保留出箱行重试(不丢)。
func TestDropCapacityFullNoMailSenderKeepsRow(t *testing.T) {
	repo := newFakeRepo()
	granter := &fakeGranter{capacityFull: true}
	uc := newDropUsecase(repo, granter, []uint32{5001}) // 无 mailSender
	if _, err := uc.ReportResult(context.Background(), dropResult(702, []uint32{5001}, nil), 0); err != nil {
		t.Fatalf("ReportResult err: %v", err)
	}
	n, err := uc.publishDropBatch(context.Background())
	if err != nil {
		t.Fatalf("publishDropBatch err: %v", err)
	}
	if n != 0 {
		t.Fatalf("no mail sender: capacity-full must not drain, got granted=%d", n)
	}
	if len(repo.dropOutbox) != 1 {
		t.Fatalf("no mail sender: capacity-full must retain row, got %d", len(repo.dropOutbox))
	}
}

// TestDropTransientErrNoMailOverflow 非背包满错误(inventory 临时不可用)不触发转邮件,保留出箱行重试。
func TestDropTransientErrNoMailOverflow(t *testing.T) {
	repo := newFakeRepo()
	granter := &fakeGranter{failPlayer: 1} // 返回普通 error(非 capacity-full)
	mail := &fakeMailSender{}
	uc := newDropUsecaseWithMail(repo, granter, mail, []uint32{5001})
	if _, err := uc.ReportResult(context.Background(), dropResult(703, []uint32{5001}, nil), 0); err != nil {
		t.Fatalf("ReportResult err: %v", err)
	}
	n, err := uc.publishDropBatch(context.Background())
	if err != nil {
		t.Fatalf("publishDropBatch err: %v", err)
	}
	if n != 0 {
		t.Fatalf("transient err must not drain, got granted=%d", n)
	}
	if len(mail.calls) != 0 {
		t.Fatalf("transient err must NOT trigger overflow mail, got %d calls", len(mail.calls))
	}
	if len(repo.dropOutbox) != 1 {
		t.Fatalf("transient err must retain row, got %d", len(repo.dropOutbox))
	}
}

// ── canonical game_mode 权威绑定 + pve_coop 零 MMR(§9.6 数值不信 DS)────────────

// countingMMRReader 记录 GetMMR 调用次数:canonical pve_coop 路径必须零调用。
type countingMMRReader struct {
	base  int
	calls int
	// pools 记录每次读 MMR 用的段位池:算 Elo 必须读**本局那一份**段位,
	// 读错池会让期望胜率完全失真(3v3 的分拿去算 5v5)。
	pools []string
}

func (c *countingMMRReader) GetMMR(_ context.Context, _ uint64, pool string) (int, error) {
	c.calls++
	c.pools = append(c.pools, pool)
	return c.base, nil
}

// newCountingUsecase 构造注入 countingMMRReader 的 usecase(等分基线 1500 / K=32:
// terminalResult 的 A 胜结构在 Elo 路径产出 ±16,可与零 MMR 路径明确区分)。
func newCountingUsecase(repo data.BattleRepo, mmr MMRReader) *BattleResultUsecase {
	cfg := conf.BattleConf{EloKFactor: 32, BaseMMR: 1500, TerminalReleaseGrace: config.Duration(5 * time.Second)}
	return NewBattleResultUsecase(repo, mmr, &fakePusher{}, nil, cfg)
}

// pveTerminalProof 组一份 canonical GameMode=pve_coop / MapID=10 的授权证明。
func pveTerminalProof(matchID uint64, pod string) data.TerminalReleaseRecord {
	proof := terminalProof(matchID, pod, "j1", 1)
	proof.GameMode = canonicalGameModePVECoop
	proof.MapID = 10
	return proof
}

// assertAllDeltas 断言 result 内每个 stat 的 mmr_delta 按 team 命中期望。
func assertAllDeltas(t *testing.T, result *battlev1.BattleResult, wantTeam0, wantTeam1 int32) {
	t.Helper()
	for _, s := range result.GetStats() {
		want := wantTeam0
		if s.GetTeam() == 1 {
			want = wantTeam1
		}
		if s.GetMmrDelta() != want {
			t.Fatalf("player %d team %d mmr_delta got %d want %d", s.GetPlayerId(), s.GetTeam(), s.GetMmrDelta(), want)
		}
	}
}

// TestAuthorizedPVECanonicalOverridesForgedRequestAndZeroesMMR(测试二):
// canonical pve_coop + DS 伪报 game_mode/map_id/mmr_delta → 落库以 canonical 为准,
// 全员 delta=0,MMR reader 零调用;正常结算、terminal release 与两路 outbox 仍生成。
func TestAuthorizedPVECanonicalOverridesForgedRequestAndZeroesMMR(t *testing.T) {
	repo := newFakeRepo()
	mmr := &countingMMRReader{base: 1500}
	uc := newCountingUsecase(repo, mmr)

	result := terminalResult(810, "battle-810")
	result.GameMode = "5v5_ranked" // DS 伪报玩法
	result.MapId = 99              // DS 伪报地图
	result.Stats[0].MmrDelta = 77  // DS 脏值
	result.Stats[1].MmrDelta = -77

	already, err := uc.ReportAuthorizedResult(context.Background(), result, pveTerminalProof(810, "battle-810"), 0)
	if err != nil || already {
		t.Fatalf("authorized pve report already=%v err=%v", already, err)
	}
	saved, ok, _ := repo.GetResult(context.Background(), 810)
	if !ok {
		t.Fatal("pve settlement not persisted")
	}
	if saved.GetGameMode() != canonicalGameModePVECoop || saved.GetMapId() != 10 {
		t.Fatalf("saved metadata not canonical: game_mode=%q map_id=%d", saved.GetGameMode(), saved.GetMapId())
	}
	if saved.GetOutcome() != battlev1.BattleOutcome_BATTLE_OUTCOME_NORMAL {
		t.Fatalf("pve settle outcome got %v want NORMAL", saved.GetOutcome())
	}
	assertAllDeltas(t, result, 0, 0)
	assertAllDeltas(t, saved, 0, 0)
	if mmr.calls != 0 {
		t.Fatalf("canonical pve_coop must not touch MMR reader, calls=%d", mmr.calls)
	}
	// 正常结算副作用不缺席:player.update 出箱 / terminal release 证明 / match release。
	if len(repo.outbox) != 2 || len(repo.terminalOutbox) != 1 || len(repo.matchReleaseOutbox) != 1 {
		t.Fatalf("pve settle side effects wrong: outbox=%d terminal=%d release=%d",
			len(repo.outbox), len(repo.terminalOutbox), len(repo.matchReleaseOutbox))
	}
	// 出箱 payload 里的 delta 也必须是 0(玩家段位不因 PVE 变动)。
	for _, o := range repo.outbox {
		evt := &playerv1.PlayerUpdateEvent{}
		if err := proto.Unmarshal(o.Payload, evt); err != nil {
			t.Fatalf("decode outbox payload: %v", err)
		}
		if evt.GetMmrDelta() != 0 {
			t.Fatalf("pve outbox player %d mmr_delta got %d want 0", evt.GetPlayerId(), evt.GetMmrDelta())
		}
	}
}

// TestAuthorizedPVPCannotMasqueradeAsPVE(测试三):canonical 是 5v5_ranked 时,
// DS 请求伪填 pve_coop 也必须按 PVP 走 Elo(±16),且落库 game_mode 被覆盖回 canonical。
func TestAuthorizedPVPCannotMasqueradeAsPVE(t *testing.T) {
	repo := newFakeRepo()
	mmr := &countingMMRReader{base: 1500}
	uc := newCountingUsecase(repo, mmr)

	proof := terminalProof(811, "battle-811", "j1", 1)
	proof.GameMode = "5v5_ranked"
	proof.MapID = 3
	result := terminalResult(811, "battle-811")
	result.GameMode = canonicalGameModePVECoop // DS 伪装 PVE 想跳过掉分
	result.MapId = 10

	already, err := uc.ReportAuthorizedResult(context.Background(), result, proof, 0)
	if err != nil || already {
		t.Fatalf("authorized pvp report already=%v err=%v", already, err)
	}
	saved, ok, _ := repo.GetResult(context.Background(), 811)
	if !ok {
		t.Fatal("pvp settlement not persisted")
	}
	if saved.GetGameMode() != "5v5_ranked" || saved.GetMapId() != 3 {
		t.Fatalf("pvp metadata not canonical: game_mode=%q map_id=%d", saved.GetGameMode(), saved.GetMapId())
	}
	// 等分基线 + A 胜 → 现有 Elo 口径 ±16;必须真实执行 MMR 读(每 stat 一次)。
	assertAllDeltas(t, saved, 16, -16)
	if mmr.calls != len(result.GetStats()) {
		t.Fatalf("pvp must read MMR per player, calls=%d want %d", mmr.calls, len(result.GetStats()))
	}
}

// ── canonical rating_mode 是计分权威(2026-08-11 关卡表「计分模式」列)──────────────

// TestSettlementRunsEloDecisionTable 钉死判据优先级:
// canonical rating_mode > 旧口径(canonical game_mode) > legacy 无 canonical。
func TestSettlementRunsEloDecisionTable(t *testing.T) {
	elo := configpb.LevelRatingMode_LEVEL_RATING_MODE_ELO
	none := configpb.LevelRatingMode_LEVEL_RATING_MODE_NONE
	unset := configpb.LevelRatingMode_LEVEL_RATING_MODE_UNSPECIFIED
	cases := []struct {
		name      string
		proof     *data.TerminalReleaseRecord
		wantElo   bool
		wantBasis string
	}{
		{"rating_mode=NONE 压过任何池名", &data.TerminalReleaseRecord{GameMode: "5v5_ranked", RatingMode: none}, false, "rating_mode_none"},
		{"rating_mode=ELO 压过任何池名", &data.TerminalReleaseRecord{GameMode: canonicalGameModePVECoop, RatingMode: elo}, true, "rating_mode_elo"},
		{"未定格 + pve_coop → 旧口径不计分", &data.TerminalReleaseRecord{GameMode: canonicalGameModePVECoop, RatingMode: unset}, false, "legacy_canonical_pve_coop"},
		{"未定格 + 排位池 → 旧口径计分", &data.TerminalReleaseRecord{GameMode: "5v5_ranked", RatingMode: unset}, true, "legacy_canonical_game_mode"},
		{"未定格 + canonical 为空(更早的旧局)→ 保守计分", &data.TerminalReleaseRecord{RatingMode: unset}, true, "legacy_canonical_game_mode"},
		{"无 canonical(legacy 内部路径)→ 行为不变", nil, true, "legacy_no_canonical"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotElo, gotBasis := settlementRunsElo(c.proof)
			if gotElo != c.wantElo || gotBasis != c.wantBasis {
				t.Fatalf("settlementRunsElo = (%t,%q), want (%t,%q)", gotElo, gotBasis, c.wantElo, c.wantBasis)
			}
		})
	}
}

// TestCanonicalRatingModeNoneStopsEloForNonPVEPool 是本次修复的回归判据。
//
// 修复前:计分与否用**排除法**从 canonical game_mode 推(`!= "pve_coop"` 即算 Elo),
// 于是任何新增撮合池(这里用 ds/v1 AllocateBattleRequest 注释里点名的未来取值
// "casual_5v5")都会**静默按排位改玩家段位**——本测试在修复前必红(会得到 ±16 且读 MMR)。
// 修复后:计分是关卡表 rating_mode 定格进 canonical 记录的独立事实,NONE 就是不计分。
func TestCanonicalRatingModeNoneStopsEloForNonPVEPool(t *testing.T) {
	repo := newFakeRepo()
	mmr := &countingMMRReader{base: 1500}
	uc := newCountingUsecase(repo, mmr)

	proof := terminalProof(812, "battle-812", "j1", 1)
	proof.GameMode = "casual_5v5" // 新池:名字里既没有 pve 也不是排位
	proof.MapID = 6
	proof.RatingMode = configpb.LevelRatingMode_LEVEL_RATING_MODE_NONE

	result := terminalResult(812, "battle-812")
	result.Stats[0].MmrDelta = 77 // DS 脏值必须被覆盖
	result.Stats[1].MmrDelta = -77

	already, err := uc.ReportAuthorizedResult(context.Background(), result, proof, 0)
	if err != nil || already {
		t.Fatalf("authorized casual report already=%v err=%v", already, err)
	}
	saved, ok, _ := repo.GetResult(context.Background(), 812)
	if !ok {
		t.Fatal("casual settlement not persisted")
	}
	assertAllDeltas(t, result, 0, 0)
	assertAllDeltas(t, saved, 0, 0)
	if mmr.calls != 0 {
		t.Fatalf("rating_mode=NONE must not touch MMR reader, calls=%d", mmr.calls)
	}
	// 不计分不等于不结算:正常结算的其余副作用必须照旧。
	if saved.GetOutcome() != battlev1.BattleOutcome_BATTLE_OUTCOME_NORMAL ||
		len(repo.outbox) != 2 || len(repo.terminalOutbox) != 1 || len(repo.matchReleaseOutbox) != 1 {
		t.Fatalf("non-scoring settle side effects wrong: outcome=%v outbox=%d terminal=%d release=%d",
			saved.GetOutcome(), len(repo.outbox), len(repo.terminalOutbox), len(repo.matchReleaseOutbox))
	}
	for _, o := range repo.outbox {
		evt := &playerv1.PlayerUpdateEvent{}
		if err := proto.Unmarshal(o.Payload, evt); err != nil {
			t.Fatalf("decode outbox payload: %v", err)
		}
		if evt.GetMmrDelta() != 0 {
			t.Fatalf("non-scoring outbox player %d mmr_delta got %d want 0", evt.GetPlayerId(), evt.GetMmrDelta())
		}
	}
}

// TestCanonicalRatingModeEloScoresRegardlessOfPoolName 反向锁死:显式 ELO 就必须真算,
// 不因池名不在任何硬编码白名单里而被跳过(避免修成"只认识 5v5_ranked"的新 fail-closed)。
func TestCanonicalRatingModeEloScoresRegardlessOfPoolName(t *testing.T) {
	repo := newFakeRepo()
	mmr := &countingMMRReader{base: 1500}
	uc := newCountingUsecase(repo, mmr)

	proof := terminalProof(813, "battle-813", "j1", 1)
	proof.GameMode = "arena_3v3" // 未来的新排位池,代码里没有任何地方硬编码过它
	proof.MapID = 9
	proof.RatingMode = configpb.LevelRatingMode_LEVEL_RATING_MODE_ELO

	result := terminalResult(813, "battle-813")
	already, err := uc.ReportAuthorizedResult(context.Background(), result, proof, 0)
	if err != nil || already {
		t.Fatalf("authorized arena report already=%v err=%v", already, err)
	}
	saved, ok, _ := repo.GetResult(context.Background(), 813)
	if !ok {
		t.Fatal("arena settlement not persisted")
	}
	assertAllDeltas(t, saved, 16, -16)
	if mmr.calls != len(result.GetStats()) {
		t.Fatalf("rating_mode=ELO must read MMR per player, calls=%d want %d", mmr.calls, len(result.GetStats()))
	}
}

// TestAuthorizedEmptyCanonicalModeKeepsLegacyElo(测试四):滚动升级前的旧
// BattleStorageRecord 无 game_mode → canonical 为空。此时不允许按 DS 请求的
// pve_coop 跳过 MMR:保持旧保守行为照算 Elo,结算不失败不卡住;落库 game_mode
// 覆盖为空(不把不可信请求字段伪装成权威事实)。
func TestAuthorizedEmptyCanonicalModeKeepsLegacyElo(t *testing.T) {
	repo := newFakeRepo()
	mmr := &countingMMRReader{base: 1500}
	uc := newCountingUsecase(repo, mmr)

	proof := terminalProof(812, "battle-812", "j1", 1) // GameMode 零值 = 旧局无 canonical
	result := terminalResult(812, "battle-812")
	result.GameMode = canonicalGameModePVECoop // DS 请求不能作为安全降级依据
	result.MapId = 10

	already, err := uc.ReportAuthorizedResult(context.Background(), result, proof, 0)
	if err != nil || already {
		t.Fatalf("legacy-mode settle already=%v err=%v", already, err)
	}
	saved, ok, _ := repo.GetResult(context.Background(), 812)
	if !ok {
		t.Fatal("legacy-mode settlement must not be stuck")
	}
	if saved.GetGameMode() != "" || saved.GetMapId() != 0 {
		t.Fatalf("empty canonical must persist empty metadata, got game_mode=%q map_id=%d",
			saved.GetGameMode(), saved.GetMapId())
	}
	assertAllDeltas(t, saved, 16, -16)
	if mmr.calls != len(result.GetStats()) {
		t.Fatalf("empty canonical must keep Elo, MMR calls=%d want %d", mmr.calls, len(result.GetStats()))
	}
	if len(repo.terminalOutbox) != 1 {
		t.Fatalf("legacy-mode settle must still write terminal release, rows=%d", len(repo.terminalOutbox))
	}
}

// TestLegacyReportResultPVEClaimStillRunsElo(测试五):无 terminalRelease 的
// ReportResult(legacy kafka / 内部直调)不得把请求体 game_mode=pve_coop 当零 MMR
// 依据——没有 canonical 权威时保持现行结算行为,不形成未授权的 MMR 绕过入口。
func TestLegacyReportResultPVEClaimStillRunsElo(t *testing.T) {
	repo := newFakeRepo()
	mmr := &countingMMRReader{base: 1500}
	uc := newCountingUsecase(repo, mmr)

	result := terminalResult(813, "battle-813")
	result.GameMode = canonicalGameModePVECoop
	result.MapId = 10

	already, err := uc.ReportResult(context.Background(), result, 0)
	if err != nil || already {
		t.Fatalf("legacy report already=%v err=%v", already, err)
	}
	saved, ok, _ := repo.GetResult(context.Background(), 813)
	if !ok {
		t.Fatal("legacy settlement not persisted")
	}
	assertAllDeltas(t, saved, 16, -16)
	if mmr.calls != len(result.GetStats()) {
		t.Fatalf("legacy path must keep Elo, MMR calls=%d want %d", mmr.calls, len(result.GetStats()))
	}
}

// TestAbandonedPVERegression(测试六):ABANDONED 补偿语义不被 PVE 改动破坏——
// 强制 delta=0、不产生掉落、不触碰 MMR reader;HandleAbandoned(pve_coop)幂等不变。
func TestAbandonedPVERegression(t *testing.T) {
	repo := newFakeRepo()
	mmr := &countingMMRReader{base: 1500}
	cfg := conf.BattleConf{
		EloKFactor: 32, BaseMMR: 1500, DropWhitelist: []uint32{5001},
		TerminalReleaseGrace: config.Duration(5 * time.Second),
	}
	uc := NewBattleResultUsecase(repo, mmr, &fakePusher{}, nil, cfg)

	res := &battlev1.BattleResult{
		MatchId: 830, WinnerTeam: winnerTeamA, EndedAtMs: 1000,
		GameMode: canonicalGameModePVECoop, MapId: 10,
		Outcome: battlev1.BattleOutcome_BATTLE_OUTCOME_ABANDONED,
		Stats: []*battlev1.PlayerStats{
			{PlayerId: 1, Team: 0, MmrDelta: 50, DroppedItemConfigIds: []uint32{5001}},
			{PlayerId: 2, Team: 1, MmrDelta: -50, DroppedItemConfigIds: []uint32{5001}},
		},
	}
	if already, err := uc.ReportResult(context.Background(), res, 0); err != nil || already {
		t.Fatalf("abandoned pve report already=%v err=%v", already, err)
	}
	saved, ok, _ := repo.GetResult(context.Background(), 830)
	if !ok || saved.GetOutcome() != battlev1.BattleOutcome_BATTLE_OUTCOME_ABANDONED {
		t.Fatalf("abandoned pve outcome wrong: ok=%v outcome=%v", ok, saved.GetOutcome())
	}
	assertAllDeltas(t, saved, 0, 0)
	if len(repo.dropOutbox) != 0 {
		t.Fatalf("abandoned must produce no drops, rows=%d", len(repo.dropOutbox))
	}
	if mmr.calls != 0 {
		t.Fatalf("abandoned must not touch MMR reader, calls=%d", mmr.calls)
	}

	// HandleAbandoned(pve_coop)补偿 + 幂等语义不变。
	if err := uc.HandleAbandoned(context.Background(), 831, []uint64{1, 2}, 10, canonicalGameModePVECoop, 0); err != nil {
		t.Fatalf("HandleAbandoned pve err: %v", err)
	}
	comp, ok, _ := repo.GetResult(context.Background(), 831)
	if !ok || comp.GetOutcome() != battlev1.BattleOutcome_BATTLE_OUTCOME_ABANDONED {
		t.Fatalf("pve compensation wrong: ok=%v outcome=%v", ok, comp.GetOutcome())
	}
	assertAllDeltas(t, comp, 0, 0)
	if err := uc.HandleAbandoned(context.Background(), 831, []uint64{1, 2}, 10, canonicalGameModePVECoop, 0); err != nil {
		t.Fatalf("idempotent HandleAbandoned pve err: %v", err)
	}
	if mmr.calls != 0 {
		t.Fatalf("compensation must not touch MMR reader, calls=%d", mmr.calls)
	}
}

// TestAuthorizedAbandonedProofStillRejected(测试六补):PVE 主动退出走 NORMAL 失败,
// 不占用 ABANDONED;带 completed 终态证明的 ABANDONED 上报仍必须整体拒绝、零副作用。
func TestAuthorizedAbandonedProofStillRejected(t *testing.T) {
	repo := newFakeRepo()
	mmr := &countingMMRReader{base: 1500}
	uc := newCountingUsecase(repo, mmr)

	result := terminalResult(832, "battle-832")
	result.Outcome = battlev1.BattleOutcome_BATTLE_OUTCOME_ABANDONED
	if _, err := uc.ReportAuthorizedResult(
		context.Background(), result, pveTerminalProof(832, "battle-832"), 0,
	); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("code=%v err=%v", errcode.As(err), err)
	}
	if len(repo.store) != 0 || len(repo.terminalOutbox) != 0 || len(repo.matchReleaseOutbox) != 0 {
		t.Fatalf("rejected abandoned wrote state: store=%d terminal=%d release=%d",
			len(repo.store), len(repo.terminalOutbox), len(repo.matchReleaseOutbox))
	}
	if mmr.calls != 0 {
		t.Fatalf("rejected abandoned touched MMR reader, calls=%d", mmr.calls)
	}
}

// TestAuthorizedPVEIdempotentReplayNoSecondSideEffects(测试七):重复
// ReportAuthorizedResult 幂等命中,不重复计算 MMR、不产生第二套出箱/终态行,
// 落库 canonical 元数据保持首笔。
func TestAuthorizedPVEIdempotentReplayNoSecondSideEffects(t *testing.T) {
	repo := newFakeRepo()
	mmr := &countingMMRReader{base: 1500}
	uc := newCountingUsecase(repo, mmr)

	result := terminalResult(833, "battle-833")
	result.GameMode = "5v5_ranked"
	result.MapId = 99
	if already, err := uc.ReportAuthorizedResult(
		context.Background(), result, pveTerminalProof(833, "battle-833"), 0,
	); err != nil || already {
		t.Fatalf("first pve report already=%v err=%v", already, err)
	}

	replay := proto.Clone(result).(*battlev1.BattleResult)
	already, err := uc.ReportAuthorizedResult(context.Background(), replay, pveTerminalProof(833, "battle-833"), 0)
	if err != nil || !already {
		t.Fatalf("pve replay already=%v err=%v", already, err)
	}
	if len(repo.store) != 1 || len(repo.outbox) != 2 ||
		len(repo.terminalOutbox) != 1 || len(repo.matchReleaseOutbox) != 1 {
		t.Fatalf("replay duplicated side effects: store=%d outbox=%d terminal=%d release=%d",
			len(repo.store), len(repo.outbox), len(repo.terminalOutbox), len(repo.matchReleaseOutbox))
	}
	if mmr.calls != 0 {
		t.Fatalf("pve replay touched MMR reader, calls=%d", mmr.calls)
	}
	saved, ok, _ := repo.GetResult(context.Background(), 833)
	if !ok || saved.GetGameMode() != canonicalGameModePVECoop || saved.GetMapId() != 10 {
		t.Fatalf("replay drifted canonical metadata: ok=%v game_mode=%q map_id=%d",
			ok, saved.GetGameMode(), saved.GetMapId())
	}
	assertAllDeltas(t, saved, 0, 0)
}

// ── 段位分区(rating_pool,2026-08-11「3v3 与 5v5 不共用同一份段位」)────────────────

// TestSettlementRatingPoolResolution 钉死分区键的解析口径:canonical 定格值优先,
// 空值(旧 matchmaker / 旧批次表 / legacy 内部路径)归一到默认池 —— **绝不丢分**。
func TestSettlementRatingPoolResolution(t *testing.T) {
	cases := []struct {
		name  string
		proof *data.TerminalReleaseRecord
		want  string
	}{
		{"定格值原样使用", &data.TerminalReleaseRecord{RatingPool: "3v3_ranked"}, "3v3_ranked"},
		{"另一份段位是另一个池", &data.TerminalReleaseRecord{RatingPool: "5v5_ranked"}, "5v5_ranked"},
		{"空值归一到默认池(不丢分)", &data.TerminalReleaseRecord{}, rating.DefaultPool},
		{"首尾空白被归一", &data.TerminalReleaseRecord{RatingPool: "  5v5_ranked  "}, "5v5_ranked"},
		{"legacy 无 canonical → 默认池", nil, rating.DefaultPool},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := settlementRatingPool(c.proof); got != c.want {
				t.Fatalf("settlementRatingPool = %q, want %q", got, c.want)
			}
		})
	}
}

// TestSettlementReadsAndReportsOwnRatingPool 是段位分区的回归判据。
//
// 「3v3 与 5v5 各自算段位」要求两件事同时成立,少一件分就串了:
//  1. 算 Elo 时读的是**本局那一份**段位(读错池 → 期望胜率失真,3v3 的分拿去算 5v5);
//  2. 出箱事件带上同一个池(丢了池 → player 侧入账到 default,3v3 的分记进公共池)。
//
// 分区上线前 MMRReader 签名里没有池、PlayerUpdateEvent 里也没有池,本测试必红。
func TestSettlementReadsAndReportsOwnRatingPool(t *testing.T) {
	repo := newFakeRepo()
	mmr := &countingMMRReader{base: 1500}
	uc := newCountingUsecase(repo, mmr)

	proof := terminalProof(814, "battle-814", "j1", 1)
	proof.GameMode = "5v5_ranked" // 撮合池:两张图可以同池撮合
	proof.MapID = 9
	proof.RatingMode = configpb.LevelRatingMode_LEVEL_RATING_MODE_ELO
	proof.RatingPool = "3v3_ranked" // 段位池:与撮合池正交,这才是算分的分区键

	result := terminalResult(814, "battle-814")
	already, err := uc.ReportAuthorizedResult(context.Background(), result, proof, 0)
	if err != nil || already {
		t.Fatalf("authorized 3v3 report already=%v err=%v", already, err)
	}

	// ① 读 MMR 必须逐人都用本局的池,一次都不能落到别的池。
	if mmr.calls != len(result.GetStats()) {
		t.Fatalf("MMR reader calls=%d, want %d", mmr.calls, len(result.GetStats()))
	}
	for i, pool := range mmr.pools {
		if pool != "3v3_ranked" {
			t.Fatalf("第 %d 次读 MMR 用了池 %q,应为 3v3_ranked(读错池会让 Elo 期望胜率失真)", i, pool)
		}
	}

	// ② 出箱事件必须带同一个池,否则 player 侧会把 3v3 的分记进 default。
	if len(repo.outbox) != 2 {
		t.Fatalf("outbox=%d, want 2", len(repo.outbox))
	}
	for _, o := range repo.outbox {
		evt := &playerv1.PlayerUpdateEvent{}
		if err := proto.Unmarshal(o.Payload, evt); err != nil {
			t.Fatalf("decode outbox payload: %v", err)
		}
		if evt.GetRatingPool() != "3v3_ranked" {
			t.Fatalf("player %d 出箱 rating_pool=%q,应为 3v3_ranked(丢了池就会记进公共段位)",
				evt.GetPlayerId(), evt.GetRatingPool())
		}
		if evt.GetMmrDelta() == 0 {
			t.Fatalf("player %d 出箱 delta 为 0,ELO 局应有真实分变化", evt.GetPlayerId())
		}
	}
}
