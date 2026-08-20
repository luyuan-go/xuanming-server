package biz

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/luyuancpp/pandora/pkg/errcode"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/conf"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/data"
)

// ── "找队伍"测试基础设施 ──────────────────────────────────────────────────────

// newPolicyUsecase 构造一个指定 join_policy 的 TeamUsecase。
// 与 newTestUsecase 的区别只有策略:入队策略是全服配置,两条路径必须各自被覆盖到。
func newPolicyUsecase(t *testing.T, joinPolicy string) (*TeamUsecase, *mockPusher, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	repo := data.NewRedisTeamRepo(rdb)
	pusher := &mockPusher{}

	var cfg conf.Config
	cfg.Team.JoinPolicy = joinPolicy
	cfg.Defaults()

	uc := NewTeamUsecase(repo, pusher, cfg.Team)
	return uc, pusher, func() {
		_ = rdb.Close()
		mr.Close()
	}
}

// mustCreateTeam 建一支队伍并断言成功,返回队伍记录。
func mustCreateTeam(t *testing.T, uc *TeamUsecase, teamID, captainID uint64) *teamv1.TeamStorageRecord {
	t.Helper()
	rec, err := uc.CreateTeam(context.Background(), teamID, captainID)
	if err != nil {
		t.Fatalf("CreateTeam(%d): %v", teamID, err)
	}
	return rec
}

// mustSetMap 让队长设置目标关卡并断言成功。
func mustSetMap(t *testing.T, uc *TeamUsecase, teamID, captainID uint64, mapID uint32) {
	t.Helper()
	if _, err := uc.SetTeamMap(context.Background(), teamID, captainID, mapID); err != nil {
		t.Fatalf("SetTeamMap(team=%d, map=%d): %v", teamID, mapID, err)
	}
}

// listedTeamIDs 把列表结果压成 team_id 集合,便于断言"包含/不包含"。
func listedTeamIDs(teams []*teamv1.OpenTeamBrief) map[uint64]*teamv1.OpenTeamBrief {
	out := make(map[uint64]*teamv1.OpenTeamBrief, len(teams))
	for _, t := range teams {
		out[t.GetTeamId()] = t
	}
	return out
}

// ── ListOpenTeams:数量上限 ───────────────────────────────────────────────────

// 需求硬指标:单次最多返回 10 支队伍。
// 覆盖三种入参:limit=0(取服务端默认)、limit 超上限、limit 小于上限。
func TestListOpenTeams_LimitClampedToMax(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	// 建 12 支开放队伍,超过上限 10。
	for i := 0; i < 12; i++ {
		mustCreateTeam(t, uc, uint64(9000+i), uint64(7000+i))
	}

	cases := []struct {
		name  string
		limit int
		want  int
	}{
		{"limit=0 取服务端默认", 0, 10},
		{"limit 超上限被钳到 10", 50, 10},
		{"limit 小于上限按 limit 返回", 3, 3},
		{"limit 为负按默认处理", -1, 10},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			teams, err := uc.ListOpenTeams(ctx, 0, c.limit)
			if err != nil {
				t.Fatalf("ListOpenTeams: %v", err)
			}
			if len(teams) != c.want {
				t.Fatalf("limit=%d 期望返回 %d 支,实际 %d 支", c.limit, c.want, len(teams))
			}
		})
	}
}

func TestListOpenTeamsProjectsCaptainDisplayFieldsInOneBatch(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	mustCreateTeam(t, uc, 9051, 7051)
	mustCreateTeam(t, uc, 9052, 7052)

	names := &playerNameResolverFake{values: map[uint64]string{7051: "Alice"}}
	numbers := &playerNoResolverFake{values: map[uint64]uint64{7051: 100051, 7052: 100052}}
	uc.SetPlayerNameResolver(names)
	uc.SetPlayerNoResolver(numbers)

	teams, err := uc.ListOpenTeams(context.Background(), 0, 10)
	if err != nil {
		t.Fatalf("ListOpenTeams: %v", err)
	}
	if len(names.calls) != 1 || len(numbers.calls) != 1 {
		t.Fatalf("display batches: names=%v numbers=%v, want one each", names.calls, numbers.calls)
	}
	if !reflect.DeepEqual(names.calls[0], numbers.calls[0]) || len(names.calls[0]) != 2 {
		t.Fatalf("captain batches differ or contain duplicates: names=%v numbers=%v", names.calls, numbers.calls)
	}

	got := listedTeamIDs(teams)
	if got[9051].GetCaptainNickname() != "Alice" || got[9051].GetCaptainPlayerNo() != 100051 {
		t.Fatalf("team 9051 captain display=%+v", got[9051])
	}
	if got[9052].GetCaptainNickname() != "" || got[9052].GetCaptainPlayerNo() != 100052 {
		t.Fatalf("team 9052 captain display=%+v", got[9052])
	}
	if got[9051].GetCaptainId() != 7051 || got[9052].GetCaptainId() != 7052 {
		t.Fatalf("display projection changed captain identity: %+v", got)
	}
}

func TestListOpenTeamsPropagatesParentCancellationDuringDisplayProjection(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	mustCreateTeam(t, uc, 9053, 7053)

	ctx, cancel := context.WithCancel(context.Background())
	uc.SetPlayerNameResolver(playerNameResolverFunc(func(callCtx context.Context, _ []uint64) (map[uint64]string, error) {
		cancel()
		<-callCtx.Done()
		return nil, callCtx.Err()
	}))

	teams, err := uc.ListOpenTeams(ctx, 0, 10)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ListOpenTeams error=%v, want context.Canceled", err)
	}
	if teams != nil {
		t.Fatalf("teams=%v, want nil on cancellation", teams)
	}
}

// ── ListOpenTeams:两个筛选参数(全部 / 固定 map_id) ─────────────────────────

func TestListOpenTeams_FilterByMapID(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9101, 7101)
	mustSetMap(t, uc, 9101, 7101, 4001)
	mustCreateTeam(t, uc, 9102, 7102)
	mustSetMap(t, uc, 9102, 7102, 4001)
	mustCreateTeam(t, uc, 9103, 7103)
	mustSetMap(t, uc, 9103, 7103, 4002)
	// 未选图的队伍:只应出现在"全部"里,不属于任何具体 map 筛选结果。
	mustCreateTeam(t, uc, 9104, 7104)

	all, err := uc.ListOpenTeams(ctx, 0, 0)
	if err != nil {
		t.Fatalf("ListOpenTeams(all): %v", err)
	}
	if len(all) != 4 {
		t.Fatalf("map_id=0 应返回全部 4 支,实际 %d 支", len(all))
	}

	map4001, err := uc.ListOpenTeams(ctx, 4001, 0)
	if err != nil {
		t.Fatalf("ListOpenTeams(4001): %v", err)
	}
	got := listedTeamIDs(map4001)
	if len(got) != 2 || got[9101] == nil || got[9102] == nil {
		t.Fatalf("map_id=4001 应只返回 9101/9102,实际 %v", got)
	}
	for _, brief := range map4001 {
		if brief.GetMapId() != 4001 {
			t.Fatalf("筛选结果混进了 map_id=%d 的队伍", brief.GetMapId())
		}
	}

	map4002, err := uc.ListOpenTeams(ctx, 4002, 0)
	if err != nil {
		t.Fatalf("ListOpenTeams(4002): %v", err)
	}
	if len(map4002) != 1 || map4002[0].GetTeamId() != 9103 {
		t.Fatalf("map_id=4002 应只返回 9103,实际 %v", listedTeamIDs(map4002))
	}

	// 没有任何队伍的关卡是正常态:空列表,不是错误。
	empty, err := uc.ListOpenTeams(ctx, 4999, 0)
	if err != nil {
		t.Fatalf("ListOpenTeams(4999): %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("无队伍的关卡应返回空列表,实际 %d 支", len(empty))
	}
}

// 换图必须把队伍从旧分桶摘掉,否则同一支队会同时挂在两个 map 下。
func TestSetTeamMap_MovesIndexBucket(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9201, 7201)
	mustSetMap(t, uc, 9201, 7201, 4001)
	mustSetMap(t, uc, 9201, 7201, 4002)

	old, err := uc.ListOpenTeams(ctx, 4001, 0)
	if err != nil {
		t.Fatalf("ListOpenTeams(4001): %v", err)
	}
	if len(old) != 0 {
		t.Fatalf("换图后旧分桶应为空,实际 %d 支", len(old))
	}

	cur, err := uc.ListOpenTeams(ctx, 4002, 0)
	if err != nil {
		t.Fatalf("ListOpenTeams(4002): %v", err)
	}
	if len(cur) != 1 || cur[0].GetTeamId() != 9201 {
		t.Fatalf("换图后应只在新分桶,实际 %v", listedTeamIDs(cur))
	}
}

// 非队长不能改队伍目标关卡。
func TestSetTeamMap_NonCaptainRejected(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9211, 7211)
	if _, err := uc.AcceptInvite(ctx, 0, 9211, 7212); err != nil {
		t.Fatalf("AcceptInvite: %v", err)
	}
	_, err := uc.SetTeamMap(ctx, 9211, 7212, 4001)
	if errcode.As(err) != errcode.ErrTeamNotCaptain {
		t.Fatalf("非队长设置目标关卡应返回 ErrTeamNotCaptain,实际 %v", err)
	}
}

// ── ListOpenTeams:只列"正在招募"的队伍 ──────────────────────────────────────

// 满员 / 非 FORMING 的队伍不得出现在列表里,而且要被顺手从索引剔除(自愈)。
// 满员的队伍必须从列表消失;**已准备但没满员的队伍仍在列表里**。
//
// 2026-08-05 修订:原口径把 READY 也排除掉,导致"想被人搜到就不能准备、想开局又必须
// 准备"互斥——单人队长一点准备就从招募列表消失。现口径只看满没满 / 散没散,"已经在
// 排队 / 在打"由 matchmaker 权威闸门负责(见 TestListOpenTeams_ExcludesCommittedTeams)。
func TestListOpenTeams_ExcludesFullKeepsReady(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	// 队伍 A:填满 5 人 → 满员,不再招募。
	mustCreateTeam(t, uc, 9301, 7301)
	for i := 1; i < 5; i++ {
		if _, err := uc.AcceptInvite(ctx, 0, 9301, uint64(7310+i)); err != nil {
			t.Fatalf("AcceptInvite(%d): %v", 7310+i, err)
		}
	}

	// 队伍 B:单人且已准备 → 状态 READY,但没满员,仍在招募。
	mustCreateTeam(t, uc, 9302, 7302)
	if _, err := uc.SetReady(ctx, 9302, 7302, true, 0); err != nil {
		t.Fatalf("SetReady: %v", err)
	}

	// 队伍 C:正常招募中。
	mustCreateTeam(t, uc, 9303, 7303)

	teams, err := uc.ListOpenTeams(ctx, 0, 0)
	if err != nil {
		t.Fatalf("ListOpenTeams: %v", err)
	}
	got := listedTeamIDs(teams)
	if got[9301] != nil {
		t.Fatalf("满员的 9301 不应出现,实际 %v", got)
	}
	if got[9302] == nil {
		t.Fatalf("已准备但未满员的 9302 应留在招募列表,实际 %v", got)
	}
	if got[9303] == nil {
		t.Fatalf("招募中的 9303 应出现,实际 %v", got)
	}
	if got[9303].GetMemberCount() != 1 || got[9303].GetMaxSize() != 5 {
		t.Fatalf("人数/容量投影错误:%d/%d", got[9303].GetMemberCount(), got[9303].GetMaxSize())
	}

	// 取消准备后 B 仍在列表里(索引跟随状态机双向同步,两个方向都不该掉出去)。
	if _, err := uc.SetReady(ctx, 9302, 7302, false, 0); err != nil {
		t.Fatalf("SetReady(false): %v", err)
	}
	teams, err = uc.ListOpenTeams(ctx, 0, 0)
	if err != nil {
		t.Fatalf("ListOpenTeams: %v", err)
	}
	if listedTeamIDs(teams)[9302] == nil {
		t.Fatalf("取消准备后 9302 仍应在招募列表,实际 %v", listedTeamIDs(teams))
	}
}

// ── 入队闸门:队伍已被一场对局占住 ────────────────────────────────────────────

// mockCommitment 模拟 matchmaker 的"这个玩家是否已被对局占住"权威读取。
type mockCommitment struct {
	committed map[uint64]bool
	err       error
	calls     []uint64
}

func (m *mockCommitment) IsPlayerCommittedToMatch(_ context.Context, playerID uint64) (bool, error) {
	m.calls = append(m.calls, playerID)
	if m.err != nil {
		return false, m.err
	}
	return m.committed[playerID], nil
}

// 队伍已提交对局(排队 / 确认 / 拉 DS / 战斗中)时必须拒绝入队:
// 名单已被 resolveMembers 冻结进票据,这时放人进来他永远进不去这一局。
func TestJoinTeam_RejectedWhenTeamCommittedToMatch(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9501, 7501)
	uc.SetMatchCommitmentReader(&mockCommitment{committed: map[uint64]bool{7501: true}})

	if _, err := uc.AcceptInvite(ctx, 0, 9501, 7502); err == nil {
		t.Fatal("队伍已在对局中,入队必须被拒")
	} else if errcode.As(err) != errcode.ErrTeamWrongState {
		t.Fatalf("错误码应为 ErrTeamWrongState,实际 %v", err)
	}

	// 被拒的玩家不得留下归属残留(不变量 §1:否则他建不了新队,人就卡住了)。
	if _, found, gerr := uc.repo.GetPlayerTeamID(ctx, 7502); gerr != nil {
		t.Fatalf("GetPlayerTeamID: %v", gerr)
	} else if found {
		t.Fatal("入队被拒后不应残留 player→team 归属")
	}
}

// matchmaker 不可达 / 返回 UNKNOWN 时必须 fail-closed 拒绝入队(§9.22),
// 不得把"查不到"当成"没在对局"放行。
func TestJoinTeam_RejectedWhenCommitmentUnknown(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9511, 7511)
	uc.SetMatchCommitmentReader(&mockCommitment{err: errcode.New(errcode.ErrUnavailable, "matchmaker down")})

	if _, err := uc.AcceptInvite(ctx, 0, 9511, 7512); err == nil {
		t.Fatal("匹配状态不确定时入队必须 fail-closed 拒绝")
	} else if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("错误码应为 ErrUnavailable,实际 %v", err)
	}
}

// 未注入 reader(未配 matchmaker_addr / 骨架联调)时闸门跳过,入队行为与历史一致。
func TestJoinTeam_GateSkippedWithoutReader(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9521, 7521)
	if _, err := uc.AcceptInvite(ctx, 0, 9521, 7522); err != nil {
		t.Fatalf("未注入 reader 时入队不应被拒: %v", err)
	}
}

// 已被对局占住的队伍不该出现在招募列表里,并且顺手从索引摘掉自愈。
func TestListOpenTeams_ExcludesCommittedTeams(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9601, 7601) // 已开打
	mustCreateTeam(t, uc, 9602, 7602) // 正常招募

	uc.SetMatchCommitmentReader(&mockCommitment{committed: map[uint64]bool{7601: true}})

	teams, err := uc.ListOpenTeams(ctx, 0, 0)
	if err != nil {
		t.Fatalf("ListOpenTeams: %v", err)
	}
	got := listedTeamIDs(teams)
	if got[9601] != nil {
		t.Fatalf("已在对局中的 9601 不应出现,实际 %v", got)
	}
	if got[9602] == nil {
		t.Fatalf("招募中的 9602 应出现,实际 %v", got)
	}

	// 自愈:已确认在对局中的候选被摘出索引,下次浏览不必再查它。
	uc.SetMatchCommitmentReader(nil)
	teams, err = uc.ListOpenTeams(ctx, 0, 0)
	if err != nil {
		t.Fatalf("ListOpenTeams(2): %v", err)
	}
	if listedTeamIDs(teams)[9601] != nil {
		t.Fatal("已在对局中的候选应已从索引摘除")
	}
}

// 列表侧闸门读取失败只跳过该条,不把整个"找队伍"打成失败(与入队侧 fail-closed 相反,
// 列表是加速器不是准入判定)。
func TestListOpenTeams_CommitmentErrorSkipsOnlyThatTeam(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9701, 7701)
	uc.SetMatchCommitmentReader(&mockCommitment{err: errcode.New(errcode.ErrUnavailable, "matchmaker down")})

	teams, err := uc.ListOpenTeams(ctx, 0, 0)
	if err != nil {
		t.Fatalf("列表不该整体失败: %v", err)
	}
	if len(teams) != 0 {
		t.Fatalf("状态不确定的队伍应跳过,实际 %v", listedTeamIDs(teams))
	}
}

// 解散的队伍必须从列表消失,残留申请一并清掉。
func TestListOpenTeams_ExcludesDisbanded(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9401, 7401)
	if _, _, _, err := uc.ApplyToTeam(ctx, 9401, 7402); err != nil {
		t.Fatalf("ApplyToTeam: %v", err)
	}
	// 唯一成员离队 → 队伍解散。
	if _, err := uc.LeaveTeam(ctx, 9401, 7401); err != nil {
		t.Fatalf("LeaveTeam: %v", err)
	}

	teams, err := uc.ListOpenTeams(ctx, 0, 0)
	if err != nil {
		t.Fatalf("ListOpenTeams: %v", err)
	}
	if len(teams) != 0 {
		t.Fatalf("解散后不应再出现在列表,实际 %v", listedTeamIDs(teams))
	}
}

// ── ApplyToTeam:approval 策略 ────────────────────────────────────────────────

func TestApplyToTeam_ApprovalCreatesPendingApplication(t *testing.T) {
	uc, pusher, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9501, 7501)
	pusher.calls = nil

	joined, rec, expiresAtMs, err := uc.ApplyToTeam(ctx, 9501, 7502)
	if err != nil {
		t.Fatalf("ApplyToTeam: %v", err)
	}
	if joined {
		t.Fatal("approval 策略不应当场入队")
	}
	if rec != nil {
		t.Fatal("未入队时不应返回队伍快照")
	}
	if expiresAtMs <= 0 {
		t.Fatalf("应返回申请过期时刻,实际 %d", expiresAtMs)
	}

	// 推送只发队长一人,且不发申请人自己。
	if len(pusher.calls) != 1 {
		t.Fatalf("应只推送一次,实际 %d 次", len(pusher.calls))
	}
	if len(pusher.calls[0].to) != 1 || pusher.calls[0].to[0] != 7501 {
		t.Fatalf("申请推送应只发队长 7501,实际 %v", pusher.calls[0].to)
	}
	if pusher.calls[0].caller != 7502 {
		t.Fatalf("推送 caller 应是申请人 7502,实际 %d", pusher.calls[0].caller)
	}

	// 队长能查到这份申请。
	apps, err := uc.ListTeamApplications(ctx, 9501, 7501)
	if err != nil {
		t.Fatalf("ListTeamApplications: %v", err)
	}
	if len(apps) != 1 || apps[0].PlayerID != 7502 {
		t.Fatalf("队长应查到申请人 7502,实际 %v", apps)
	}
	if apps[0].ExpiresAtMs != expiresAtMs {
		t.Fatalf("列表里的过期时刻应与申请返回一致:%d vs %d", apps[0].ExpiresAtMs, expiresAtMs)
	}

	// 申请人还没进队。
	team, err := uc.GetTeam(ctx, 9501)
	if err != nil {
		t.Fatalf("GetTeam: %v", err)
	}
	if len(team.Members) != 1 {
		t.Fatalf("审批前不应有人入队,实际成员数 %d", len(team.Members))
	}
}

// 申请人名单只对队长开放。
func TestListTeamApplications_NonCaptainRejected(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9511, 7511)
	if _, err := uc.AcceptInvite(ctx, 0, 9511, 7512); err != nil {
		t.Fatalf("AcceptInvite: %v", err)
	}
	_, err := uc.ListTeamApplications(ctx, 9511, 7512)
	if errcode.As(err) != errcode.ErrTeamNotCaptain {
		t.Fatalf("普通成员查申请列表应返回 ErrTeamNotCaptain,实际 %v", err)
	}
}

// 写入侧上限(不变量 §9-18):同一队伍最多 10 份 pending 申请。
func TestApplyToTeam_PendingLimit(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9601, 7601)
	for i := 0; i < 10; i++ {
		if _, _, _, err := uc.ApplyToTeam(ctx, 9601, uint64(7610+i)); err != nil {
			t.Fatalf("第 %d 份申请不应失败: %v", i+1, err)
		}
	}
	_, _, _, err := uc.ApplyToTeam(ctx, 9601, 7699)
	if errcode.As(err) != errcode.ErrTeamApplyPendingLimit {
		t.Fatalf("第 11 份申请应返回 ErrTeamApplyPendingLimit,实际 %v", err)
	}

	// 读取侧同样被上限截断。
	apps, err := uc.ListTeamApplications(ctx, 9601, 7601)
	if err != nil {
		t.Fatalf("ListTeamApplications: %v", err)
	}
	if len(apps) != 10 {
		t.Fatalf("列表应被上限截断到 10,实际 %d", len(apps))
	}
}

// 重复申请同一队伍是幂等刷新,不应占掉第二个名额(否则玩家连点几次就把自己挤爆)。
func TestApplyToTeam_RepeatIsIdempotent(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9611, 7611)
	for i := 0; i < 5; i++ {
		if _, _, _, err := uc.ApplyToTeam(ctx, 9611, 7612); err != nil {
			t.Fatalf("第 %d 次重复申请不应失败: %v", i+1, err)
		}
	}
	apps, err := uc.ListTeamApplications(ctx, 9611, 7611)
	if err != nil {
		t.Fatalf("ListTeamApplications: %v", err)
	}
	if len(apps) != 1 {
		t.Fatalf("重复申请应只保留一条,实际 %d 条", len(apps))
	}
}

// 已满 / 已在队内 / 队伍不存在都要给出各自明确的错误码,客户端才能提示对。
func TestApplyToTeam_RejectionCodes(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	// 满员队伍 → 3002
	mustCreateTeam(t, uc, 9621, 7621)
	for i := 1; i < 5; i++ {
		if _, err := uc.AcceptInvite(ctx, 0, 9621, uint64(7630+i)); err != nil {
			t.Fatalf("AcceptInvite: %v", err)
		}
	}
	if _, _, _, err := uc.ApplyToTeam(ctx, 9621, 7699); errcode.As(err) != errcode.ErrTeamFull {
		t.Fatalf("满员队伍应返回 ErrTeamFull,实际 %v", err)
	}

	// 已在队内 → 3004
	if _, _, _, err := uc.ApplyToTeam(ctx, 9621, 7621); errcode.As(err) != errcode.ErrTeamAlreadyInTeam {
		t.Fatalf("已在队内应返回 ErrTeamAlreadyInTeam,实际 %v", err)
	}

	// 队伍不存在 → 3001
	if _, _, _, err := uc.ApplyToTeam(ctx, 9999, 7698); errcode.As(err) != errcode.ErrTeamNotFound {
		t.Fatalf("不存在的队伍应返回 ErrTeamNotFound,实际 %v", err)
	}
}

// ── HandleTeamApplication:队长审批 ───────────────────────────────────────────

func TestHandleTeamApplication_AcceptJoins(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9701, 7701)
	if _, _, _, err := uc.ApplyToTeam(ctx, 9701, 7702); err != nil {
		t.Fatalf("ApplyToTeam: %v", err)
	}

	rec, err := uc.HandleTeamApplication(ctx, 9701, 7701, 7702, true)
	if err != nil {
		t.Fatalf("HandleTeamApplication(accept): %v", err)
	}
	if len(rec.Members) != 2 || !hasMember(rec, 7702) {
		t.Fatalf("同意后申请人应已入队,实际成员 %v", rec.Members)
	}

	// 申请令牌已消耗,列表清空。
	apps, err := uc.ListTeamApplications(ctx, 9701, 7701)
	if err != nil {
		t.Fatalf("ListTeamApplications: %v", err)
	}
	if len(apps) != 0 {
		t.Fatalf("同意后申请应已消耗,实际剩 %d 条", len(apps))
	}

	// 玩家归属索引已建立(不变量 §1):他现在查得到自己的队伍。
	myTeam, has, err := uc.GetMyTeam(ctx, 7702)
	if err != nil || !has || myTeam.TeamId != 9701 {
		t.Fatalf("入队后 GetMyTeam 应返回 9701,实际 has=%v team=%v err=%v", has, myTeam, err)
	}

	// 同一份申请不能被处理两次。
	_, err = uc.HandleTeamApplication(ctx, 9701, 7701, 7702, true)
	if errcode.As(err) != errcode.ErrTeamApplyNotFound {
		t.Fatalf("重复审批应返回 ErrTeamApplyNotFound,实际 %v", err)
	}
}

func TestHandleTeamApplication_RejectOnlyConsumesToken(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9711, 7711)
	if _, _, _, err := uc.ApplyToTeam(ctx, 9711, 7712); err != nil {
		t.Fatalf("ApplyToTeam: %v", err)
	}

	rec, err := uc.HandleTeamApplication(ctx, 9711, 7711, 7712, false)
	if err != nil {
		t.Fatalf("HandleTeamApplication(reject): %v", err)
	}
	if len(rec.Members) != 1 {
		t.Fatalf("拒绝后不应有人入队,实际成员数 %d", len(rec.Members))
	}
	apps, err := uc.ListTeamApplications(ctx, 9711, 7711)
	if err != nil {
		t.Fatalf("ListTeamApplications: %v", err)
	}
	if len(apps) != 0 {
		t.Fatalf("拒绝后申请应已消耗,实际剩 %d 条", len(apps))
	}

	// 拒绝后配额已释放,同一玩家可以重新申请。
	if _, _, _, err := uc.ApplyToTeam(ctx, 9711, 7712); err != nil {
		t.Fatalf("被拒后重新申请不应失败: %v", err)
	}
}

func TestHandleTeamApplication_NonCaptainRejected(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9721, 7721)
	if _, err := uc.AcceptInvite(ctx, 0, 9721, 7722); err != nil {
		t.Fatalf("AcceptInvite: %v", err)
	}
	if _, _, _, err := uc.ApplyToTeam(ctx, 9721, 7723); err != nil {
		t.Fatalf("ApplyToTeam: %v", err)
	}

	// 普通成员审批被拒,且申请令牌不得被消耗(否则谁都能把别人的申请弄没)。
	_, err := uc.HandleTeamApplication(ctx, 9721, 7722, 7723, true)
	if errcode.As(err) != errcode.ErrTeamNotCaptain {
		t.Fatalf("非队长审批应返回 ErrTeamNotCaptain,实际 %v", err)
	}
	apps, err := uc.ListTeamApplications(ctx, 9721, 7721)
	if err != nil {
		t.Fatalf("ListTeamApplications: %v", err)
	}
	if len(apps) != 1 {
		t.Fatalf("越权审批不得消耗申请,实际剩 %d 条", len(apps))
	}
}

// ── ApplyToTeam:open 策略 ───────────────────────────────────────────────────

func TestApplyToTeam_OpenPolicyJoinsImmediately(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyOpen)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9801, 7801)

	joined, rec, expiresAtMs, err := uc.ApplyToTeam(ctx, 9801, 7802)
	if err != nil {
		t.Fatalf("ApplyToTeam: %v", err)
	}
	if !joined {
		t.Fatal("open 策略应当场入队")
	}
	if expiresAtMs != 0 {
		t.Fatalf("已入队时不应返回申请过期时刻,实际 %d", expiresAtMs)
	}
	if rec == nil || len(rec.Members) != 2 || !hasMember(rec, 7802) {
		t.Fatalf("应返回含新成员的完整快照,实际 %v", rec)
	}

	// 不留任何待审批申请。
	apps, err := uc.ListTeamApplications(ctx, 9801, 7801)
	if err != nil {
		t.Fatalf("ListTeamApplications: %v", err)
	}
	if len(apps) != 0 {
		t.Fatalf("open 策略不应产生待审批申请,实际 %d 条", len(apps))
	}

	// 不变量 §1:归属索引已建立,他不能再进第二支队。
	mustCreateTeam(t, uc, 9802, 7803)
	if _, _, _, err := uc.ApplyToTeam(ctx, 9802, 7802); errcode.As(err) != errcode.ErrTeamAlreadyInTeam {
		t.Fatalf("已在队的玩家申请别队应返回 ErrTeamAlreadyInTeam,实际 %v", err)
	}
}

// open 策略下队伍满员后必须停止接收,不能因为"策略是开放"就突破容量。
func TestApplyToTeam_OpenPolicyStopsAtCapacity(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyOpen)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9811, 7811)
	for i := 1; i < 5; i++ {
		if _, _, _, err := uc.ApplyToTeam(ctx, 9811, uint64(7820+i)); err != nil {
			t.Fatalf("第 %d 人加入失败: %v", i, err)
		}
	}
	if _, _, _, err := uc.ApplyToTeam(ctx, 9811, 7899); errcode.As(err) != errcode.ErrTeamFull {
		t.Fatalf("满员后应返回 ErrTeamFull,实际 %v", err)
	}
}

// ── 策略投影到客户端可见结构 ─────────────────────────────────────────────────

// 客户端靠 join_policy 决定按钮是"申请入队"还是"加入队伍",必须每份快照都带上。
func TestJoinPolicyProjectedIntoSnapshots(t *testing.T) {
	for _, c := range []struct {
		policy string
		want   teamv1.TeamJoinPolicy
	}{
		{conf.JoinPolicyApproval, teamv1.TeamJoinPolicy_TEAM_JOIN_POLICY_APPROVAL},
		{conf.JoinPolicyOpen, teamv1.TeamJoinPolicy_TEAM_JOIN_POLICY_OPEN},
	} {
		t.Run(c.policy, func(t *testing.T) {
			uc, _, cleanup := newPolicyUsecase(t, c.policy)
			defer cleanup()
			ctx := context.Background()

			rec := mustCreateTeam(t, uc, 9901, 7901)
			teamView, err := uc.TeamToProto(context.Background(), rec)
			if err != nil {
				t.Fatalf("TeamToProto: %v", err)
			}
			if got := teamView.GetJoinPolicy(); got != c.want {
				t.Fatalf("Team 快照 join_policy 应为 %v,实际 %v", c.want, got)
			}
			teams, err := uc.ListOpenTeams(ctx, 0, 0)
			if err != nil {
				t.Fatalf("ListOpenTeams: %v", err)
			}
			if len(teams) != 1 || teams[0].GetJoinPolicy() != c.want {
				t.Fatalf("列表项 join_policy 应为 %v,实际 %v", c.want, teams)
			}
		})
	}
}

// 配置写错必须在启动时就被拒(而不是猜成 open 把队伍对所有人敞开)。
func TestParseJoinPolicyRejectsUnknown(t *testing.T) {
	if got, err := conf.ParseJoinPolicy(""); err != nil || got != conf.JoinPolicyApproval {
		t.Fatalf("空串应默认 approval,实际 got=%q err=%v", got, err)
	}
	for _, bad := range []string{"aproval", "OPEN", "true", "1"} {
		if _, err := conf.ParseJoinPolicy(bad); err == nil {
			t.Fatalf("非法 join_policy %q 应报错而不是被猜成某个值", bad)
		}
	}

	var cfg conf.Config
	cfg.Team.JoinPolicy = "aproval"
	cfg.Defaults()
	if err := cfg.ValidateJoinPolicy(); err == nil {
		t.Fatal("ValidateJoinPolicy 应对非法值报错以便启动 fail-fast")
	}
}

// map_id 分桶数被在线队伍数有界:换图会摘旧桶,空 ZSET 由 Redis 自动删除。
// 这是"不校验 map_id 合法性"这个决定的安全前提,退化了就会变成可被客户端撑爆的 key 空间。
func TestSetTeamMap_EmptyBucketKeyIsRemoved(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9911, 7911)
	for i := 0; i < 20; i++ {
		mustSetMap(t, uc, 9911, 7911, uint32(5000+i))
	}
	// 只剩最后一个分桶还有内容,前面 19 个都应已空(空 ZSET 被 Redis 删除)。
	for i := 0; i < 19; i++ {
		teams, err := uc.ListOpenTeams(ctx, uint32(5000+i), 0)
		if err != nil {
			t.Fatalf("ListOpenTeams(%d): %v", 5000+i, err)
		}
		if len(teams) != 0 {
			t.Fatalf("分桶 %d 应已清空,实际 %v", 5000+i, listedTeamIDs(teams))
		}
	}
	teams, err := uc.ListOpenTeams(ctx, 5019, 0)
	if err != nil {
		t.Fatalf("ListOpenTeams(5019): %v", err)
	}
	if len(teams) != 1 {
		t.Fatalf("最后一个分桶应有 1 支队伍,实际 %v", listedTeamIDs(teams))
	}
}

// 索引是非权威投影:即使索引里残留了一支实际已解散的队伍,列表也不能把它返回给玩家,
// 并且要顺手把脏候选剔除(自愈)。这里直接往索引写一个不存在的 team_id 模拟脏数据。
func TestListOpenTeams_PrunesStaleCandidate(t *testing.T) {
	uc, _, cleanup := newPolicyUsecase(t, conf.JoinPolicyApproval)
	defer cleanup()
	ctx := context.Background()

	mustCreateTeam(t, uc, 9921, 7921)

	// 伪造一条指向不存在队伍的索引项(score 取足够远的未来,排除被过期清理干扰)。
	repo := uc.repo
	if err := repo.SyncOpenTeam(ctx, 8888, 0, 0, true, 1<<62, time.Hour); err != nil {
		t.Fatalf("SyncOpenTeam(stale): %v", err)
	}

	teams, err := uc.ListOpenTeams(ctx, 0, 0)
	if err != nil {
		t.Fatalf("ListOpenTeams: %v", err)
	}
	got := listedTeamIDs(teams)
	if got[8888] != nil {
		t.Fatal("索引里的脏候选不得返回给玩家")
	}
	if got[9921] == nil {
		t.Fatalf("真实队伍应正常返回,实际 %v", got)
	}

	// 复核不通过的候选应已被剔除:再查一次索引里不该还有它。
	ids, err := repo.ListOpenTeamIDs(ctx, 0, 64)
	if err != nil {
		t.Fatalf("ListOpenTeamIDs: %v", err)
	}
	for _, id := range ids {
		if id == 8888 {
			t.Fatalf("脏候选应已被自愈剔除,实际索引仍含 %v", ids)
		}
	}
}
