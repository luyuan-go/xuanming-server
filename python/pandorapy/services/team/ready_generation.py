"""「谁准备好了」的单调代际 —— 对应 Go 侧 internal/biz/ready_generation.go。

# 它解决什么(INC-20260813-001 ①)

EndTeamMatch 是一次性调用,而 outbox 会重投。没有代际时,一条**迟到的旧局释放**
会把**下一局**(或玩家刚重新点上)的准备状态清掉:

    对局结束 → EndTeamMatch → ACK 丢了 → outbox 保留任务
             → 期间玩家重新点准备 / 离队重入 / 队长已开新局
             → outbox 重投 → 照样把 ready 抹平

代际把「复位」从**按名字找人**变成**按版本 CAS**:重投带的 expected 对不上当前代际
就直接 no-op。这是 outbox 重投场景下唯一正确的幂等依据 ——
光看「谁还挂着 ready」不够,因为那看不出"这是不是同一次意图"。

# 为什么用包装器自动推进,而不是在每处写点手动 ++

「改了 ready 意图就要推进代际」有 **7 处以上**写点(SetReady / 入队 / 离队 / 踢人 /
掉线软化 / 离线摘人 / 对局结束复位),散落在多个文件。漏掉任何一处的后果是**静默的**:
代际停在旧值 → CAS 照样通过 → 幂等保护形同虚设 → **而所有测试照样绿**。

所以改成:**所有队伍写都必须走 update_team**,它在锁内比较前后指纹,变了就推进代际。
忘记推进在结构上不可能。
"""

from __future__ import annotations

from collections.abc import Callable


def ready_intent_fingerprint(team) -> str:
    """概括「此刻谁准备好了」的**全部**可见事实。

    只含三样:队伍状态、成员集合、每个成员的 ready 位。

    ★ 刻意**不含** map_id / 队长 / 昵称 / 英雄 —— 那些变了不影响"这一局该不该被复位"。
    把它们算进来只会让代际无谓地涨,使正常的 EndTeamMatch 频繁 CAS 失败
    ——**该复位的反而不复位了**,而这个失败是静默的(看起来只是"偶尔没清干净")。

    ★ 成员按 player_id **排序**后拼接:Members 的存储顺序会因增删而变,
    不排序会把"顺序变了"误判成"意图变了",同样让代际虚涨。
    """
    if team is None:
        return ""
    entries = sorted(
        (m.player_id, bool(m.ready)) for m in team.members
    )
    parts = [str(int(team.state))]
    for player_id, ready in entries:
        parts.append(f"|{player_id}:{1 if ready else 0}")
    return "".join(parts)


async def update_team(
    repo,
    team_id: int,
    fn: Callable[[object], None],
    *,
    optimistic_retry: int,
    ttl,
    stamp: Callable[[object], None] | None = None,
) -> None:
    """biz 层**唯一**允许的队伍写入口。

    在同一把乐观锁内做三件事:
        ① 快照 ready 意图指纹 → ② 跑业务回调 → ③ 指纹变了就推进代际

    因为推进发生在**锁内、与业务写同一次提交**,代际与状态永远不会劈叉。
    fn 抛异常时整次写被放弃,代际自然也不会动。

    stamp 在**代际推进之后**运行(只有 BeginTeamMatch 用)。
    ★ 为什么需要"之后"这个位置:BeginTeamMatch 要在同一次提交里写一张收据,
    而收据必须记下**消费之后**的代际(它是"这是不是同一次尝试的重试"的 CAS 依据)。
    在 fn 里写收据只能靠「当前值 + 1」去猜 —— 那是在复刻本函数的内部实现,
    哪天推进规则一变(比如某类写不再推进),收据就会静默记错一个永远对不上的值。

    stamp 只该做「盖章」,**不得再改 ready 意图**(那会让指纹与已算完的代际劈叉)。
    """

    def wrapped(team) -> None:
        before = ready_intent_fingerprint(team)
        fn(team)
        if ready_intent_fingerprint(team) != before:
            team.ready_generation += 1
        if stamp is not None:
            stamp(team)

    await repo.update_with_lock(team_id, optimistic_retry, wrapped, ttl)


def generation_matches(current: int, expected: int) -> bool:
    """跨代幂等判据。

    expected == 0 = 代际未知(滚动升级窗口的旧调用方),退化为"放行一次" ——
    ★ 不是跨代安全的,但**严格优于完全不复位**。调用方必须为此打 WARN:
    出现即说明还有旧副本在跑,必须可见,否则事后无法解释
    "为什么某次复位把新点的准备抹掉了"。
    """
    if expected == 0:
        return True
    return current == expected
