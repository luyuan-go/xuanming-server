# [INC-20260824-003][P1][near-miss] 释放 owner 会抹掉再入屏障的判据

> **状态**：根因确认；**修法 C 已落码待部署**，D 待执行（未关闭）  
> **类型**：`near-miss` / `split-brain`  
> **环境**：代码审计 + 对抗复核；`mode=local` + `authority_mode=legacy` 可达，**未在线上发生**  
> **发现时间（UTC）**：2026-08-24  
> **关联事故**：[INC-20260824-001](2026-08-24-p0-python-settle-kick-to-login.md)（本问题由其缺口②的修复方案引入）

## 0. 结论

为修复 INC-20260824-001 缺口②而落码的方案——**在首次 `ended` 心跳内、任何回收动作之前释放 owner**——会把再入屏障从 `max(now, 旧实例租约截止) + 7s`（约 22s）降到 **0**。

原因不是「少了一道门」，而是**释放这个动作本身就是删除那道门的判据**：

- 屏障的唯一判据是 owner 记录上的 `owner_type == BATTLE && instance_uid != ""`（`services/runtime/owner/internal/data/owner_repo.go:572`）；
- 命中才算 `admitNotBefore = max(now, 旧租约截止) + skewMargin`（`:573-583`）；不命中则 `barrierSource = "no_old_battle_owner"`、`admitNotBefore = now`（`:566` / `:570`）；
- 而 Release 的 UPDATE 恰好把这两列抹空：`SET owner_type = ?, phase = ?, pod_name = '', instance_uid = '', ...`（`:907-910`）；
- Admit 侧的屏障判定 `if now < rec.AdmitNotBeforeMs`（`:707`）随即恒不成立。

⇒ 释放之后，任何 `BeginTransition` 都拿到零屏障。

**更关键的是：这个洞是既有的，不是本次改动发明的。** 见 §2。

## 1. 为什么这是安全边界①要防的东西

`ownerReleaseAbandonedPlayersWeak` 的安全边界①（`services/battle/ds_allocator/internal/biz/owner_authority.go:315`）要求「只能在实例回收已确认之后调用」。落码方案里为破例写的理由是「ended 是 DS 自己宣告的终态，该实例结构上不可能再接纳任何人」——这句本身没错，但**论证错了对象**：

- `pkg/placement/placement.go:22` 规定的是「连续 `DSFenceLeaseMaxSeconds` 未能续租 → DS 必须对**存量玩家**自我 fencing：关闭输入、Kick 已准入连接、销毁 Pawn」——防的是「**仍持有旧人**」，不是「接纳新人」；
- `placement.go:27` 的核心时序不等式是「旧 DS 最晚停止可玩时间 < 新 DS 最早开始可玩时间」，`placement.go:31` 明写「这些是正确性常量而非调优参数」。

边界②（exact 身份门）与边界③（compare-delete）**对此零覆盖**：它们管的是「删哪一条」，从来不管「此刻删安不安全」。破例论证把这两件事等同了。

叠加事实：ended 那一跳 `refreshActive` 保持 false（`allocator.go:2966` 只在 `ready`/`running` 置位）⇒ 不再续实例租约，租约 deadline 冻结在上一跳。这既是「改动前屏障约 22s」的计算前提，也说明租约不会自然把窗口拉长。

## 2. 既有同构洞：登出释放 BATTLE 归属

`services/account/login/internal/biz/login.go:2180-2184` 的登出释放对 owner 类型**一视同仁**（判据只有 `rec.OwnerType != 0`）。因此**在此次改动之前**就已经存在同一条路径：

> 玩家在对局中直接登出 → BATTLE 归属被释放 → 屏障判据被抹掉 → 立刻重登时新的 `BeginTransition` 拿到零屏障，而战斗 DS 上他的 Pawn 可能仍在被模拟。

⇒ **「释放会抹掉屏障」是 owner 权威的结构性问题**，本次改动只是把它从「玩家主动登出」这个低频路径，扩大到「每一局正常结算」这个必经路径。这也解释了为什么落码时会觉得该写法是安全的——仓里本来就到处这么用。

修法因此不应只针对 ended 路径。

## 3. 作用域：今天为什么只到 P1

- **Agones + legacy**：`battle.GameserverUid` 恒为空（`allocator.go:737-755` 只有 Model B 或 `localInstanceIdentitySource` 才回填）⇒ `endedUID == ""` ⇒ helper 首行早退（`owner_authority.go:327`），释放静默 no-op。
- **Model B（生产姿态）**：`HeartbeatAuthorizedWithPlayers` 没有 `becameEnded` 分支；owner 释放唯一收口是 `ReleaseBattleExpected`（`allocator.go:2226`），严格排在 `releaseGameServer` 成功之后。
- ⇒ 本反例今天**只在 `mode=local` 的开发/策划机栈上可达**——而那恰恰是正在验证「打完一局能回大厅」的那台机器。

旧 DS 在该形态下的存活时长：`local` 模式 Agones Shutdown 是 no-op，唯一回收是 sweep 的 `killStrandedDS(..., "ended")`（`allocator.go:3989`），触发条件是失联 ≥ `HeartbeatTimeout` = **120s**（`ds_allocator-dev.yaml:115`；`conf.go:752-761` 默认 15s，editor + local 放宽到 120s）。按 15s 估算会把窗口低估 8 倍。

## 4. 三条候选修法与各自代价

| 方案 | 做法 | 代价 |
|---|---|---|
| A. 保留现方案 | 把注释改成「**刻意豁免**边界①」并登记本档 | 屏障 22s→0；作用域随任何一次「给 Agones+legacy 回填 uid」而扩大 |
| B. 撤回 ended 内释放 | 只保留 sweep 的 ended 兜底（该处**顺序正确**：先 `killStrandedDS` 再释放） | 降级路径收敛晚 15s/120s；主路径不受影响（主路径不读 owner，见 INC-20260824-001 §4） |
| **C（★ 已采纳并落码 2026-08-24）** 让释放不再销毁屏障 | Release 释放 BATTLE 归属时把 `max(now, 旧租约截止)+skew` **盖进 `admit_not_before` 列**；`BeginTransition` 的无旧归属分支取 `max(now, rec.AdmitNotBeforeMs)` | 改 owner 权威核心 + Python 同步；会让「登出后立刻重登」等场景**新增**约 22s 的大厅出生等待 |

关于 C 的两点事实：`admit_not_before` **不在** Release 的 UPDATE 列清单里，即该列本来就跨释放存活（`owner_repo.go:905-910`，同处已为 `hub_source_revision` 写过同类结论）；C 同时修掉 §2 的既有洞。

对抗复核另提过「释放前先把 `ds_instance_lease` deadline 收到 now」：**今天做不到**——`proto/pandora/owner/v1/owner.proto` 只有 5 个 rpc（`QueryOwner` / `BeginTransition` / `Admit` / `RenewInstanceLease` / `ReleaseOwner`），没有吊销/收缩租约的接口，且 `owner_repo.go:211` 明写「deadline 只前进」。该方向 = **改 proto + 新 RPC**，不是一行改动。

## 5. 已排除的构造路径（省下一轮复核）

- 旧 DS「复活」把 ended 改回 running：不可能。终态早退（`allocator.go:2832`）排在写回（`:2844`）之前。
- 释放误删玩家的**新**归属：不可能。边界② exact 身份门 + 边界③ 在权威侧 `FOR UPDATE` 重比对（`owner_repo.go:117-121` 四项合取）。
- pod/uid 跨局复用致 exact 门误判：不可能。`local_allocator.go:195` pod 名编入 matchID，`:231` uid 每次现铸。
- Release 把 `owner_epoch` 打回：不可能。该 UPDATE 刻意不含该列。
- 孤儿/pod_mismatch DS 把别人的局写成 ended：不可能。pod 校验（`allocator.go:2837`）先于写回。

## 6. 排查时会骗人的两个数

1. **`endedUID == ""` 时零日志**：helper 首行早退发生在汇总日志的 `defer` 安装（`owner_authority.go:335-343`）之前，而外层 `battle_ended_owner_release` 的 Infow **不带 uid**。⇒ 在 `ds_allocator` 日志里，「什么都没做」与「全部释放成功」完全同形。
2. **`released` 计数把 no-op 当成功**：权威侧 compare-delete 拒绝时返回 **OK + 当前记录**（`owner_repo.go:881-900`），而客户端只在 `resp.Code != OK` 时报错（`owner_lease_client.go:147-155`）⇒ `released++` 照加。汇总可以打出 `released=10` 而实际一条没删。

⇒ **要证明释放真的发生，必须看 owner 服务侧的 `owner_release_noop` 日志**，不能只看 `ds_allocator`。

## 7. 红线

- **「ended 免边界①」这套论证绝不能被复制进 Model B。** 一旦复制，旧 pod 在 Agones 面既不自杀也不被 `killStrandedDS`，窗口从 2 分钟变成孤儿回收周期（≥10min），立即升 P0。
- 若将来给 Agones + legacy 回填 `gameserver_uid`，必须先重新评估本档。

## 8. 同批发现的其它缺陷（未修）

- **确定性漏释放**：整份花名册共用**一个** 2s 预算（`allocator.go:3075`），循环内每人**串行两次 RPC**（`owner_authority.go:353` / `:367`），而单次 RPC 自身超时也是 2s（`owner_lease_client.go:24`）。一次慢 Query 即命中预算耗尽早退（`:346-352`），**尾部玩家一个都不释放**，且失败按花名册顺序、稳定复现 ⇒ INC-20260824-001 原样复发。建议：预算按 `len(players)` 缩放，或失败名单入短周期重试队列。
- **注释与可达性不符**：`allocator.go:3009-3018` 声称「Agones + legacy 灰度部署会执行到这里」，但该形态下 uid 恒空、`ownerBeginPlayers` 阶段就会失败，玩家根本进不了战斗。该段是死辩护，应改写。
- **ended 镜像滞留**：legacy 的 sweep ended 分支只 `RemoveActive`，不调 `ExpireBattle`（对比 abandoned 分支调了），而 ended 那跳刚把 TTL 刷成 2h ⇒ ended 镜像在 Redis 滞留 2 小时。排查时「对局记录还在」不等于「对局还活着」。

## 9. 修法定案（2026-08-24）

采纳 **C**，已落码。**没有**采纳 A（A 的唯一减轻因素是「今天仅 `mode=local` 可达」，按生产口径衡量它就是在主路径上删掉防脑裂围栏）；B 在 C 落地后不再必要。

### 9.1 C 落了什么

把再入屏障从「归属指针的派生量」改成「玩家这一行的留存事实」：

- `owner_repo.go` `Release`：释放 **BATTLE** 归属时，按与 `BeginTransition` 同一公式算出 `max(now, 本实例租约截止) + skew`，**盖进 `admit_not_before_ms`**（该列此前不在 UPDATE 列清单里）。只前进，不回调。HUB 归属不盖（其屏障按设计恒为 `now`）。
- `owner_repo.go` `BeginTransition`：无旧 BATTLE 归属的分支取 `max(now, rec.AdmitNotBeforeMs)`，`barrier_source` 新增 `retained_released_battle`。
- `Release` 增加 `skewMargin` 入参，由 biz 用与 `BeginTransition` 同一常量（`placement.DSFenceSkewMarginSeconds`）传入；`owner_released` 日志补 `retained_admit_not_before_ms` / `retained_barrier_source` / `retained_barrier_remaining_ms`。
- Python 同构：`services/owner/repo.py`（`_SQL_RELEASE` 加列 + `release` 算屏障 + `begin_transition` 取 max）、`data.py` Protocol、`biz.py` 传参。

**结果**：任何时刻释放都不会让围栏消失，§1 的安全边界①不再是调用方纪律；§2 的登出洞一并消失。

### 9.2 连带订正的注释（两栈同步）

- `ds_allocator/owner_authority.go` 边界①、`allocator.go` 与 `biz_heartbeat.py` 的 `becameEnded` 破例辩护：原论证「ended 不同，结构上不可能再接纳任何人」**是错的**，已改写为「屏障已由 owner 侧留存保证」，并写明这是硬依赖（屏障留存被改掉则必须退回「回收确认后才释放」）。
- 三条与代码不符的断言已订正：「撞 30s 线」（实为连续 3 次判弃、约 2.25~3.75s，`AuthorityWaitWindowSeconds` 今为 300）、「客户端结算后紧接着的 GetResumeContext」（主路径走 hub 票门，**不读 owner**）、「sweep 会再释放一次」（晚一个 `HeartbeatTimeout`，体验口径上救不回本次）。
- `allocator.go` 那段「Agones + legacy 灰度会执行到这里」的死代码辩护已改写。

### 9.3 验证

| 项 | 结果 |
|---|---|
| Go `owner` 全量（真 TiDB，`PANDORA_TEST_MYSQL_DSN=root@tcp(127.0.0.1:4000)/`） | 通过 |
| 新增 `TestOwnerRepoMySQL/ReleasedBattleRetainsBarrier` | 通过；★ 变异①（Release 不盖屏障）、变异②（Begin 不取 max）**各自击杀** |
| Python `test_owner_repo.py`（32 项，真 MySQL） | 通过；新增 `test_released_battle_retains_barrier`，同两条变异**各自击杀** |
| Python `-k "ds_allocator or owner or login"` | 1268 通过 |
| Go `ds_allocator/internal/biz` + `login` 全量 | 通过 |
| ruff / `git diff --check` | 通过 |
| `-race` | **未执行**（本机无 CGO 编译器；需走 `tools/scripts/go_test_race.ps1` 的 docker 路径） |
| 真机玩家 E2E | **未执行** |

> `TestAssertTiDBBackendRejectsMySQL` 在 TiDB DSN 下失败属**环境错配**（该用例要求 DSN 指向真 MySQL 才能证明 `require_tidb` 会拒绝）；用 `root:pandora_dev_root@tcp(127.0.0.1:13306)/` 复跑通过，与本次改动无关。

### 9.4 D（未执行，需 proto 变更）

C 保住了围栏，但结算后大厅出生门仍会等约 18~22s（屏障 = `max(now, 冻结的旧租约) + 7s`，而 ended 那跳 `refreshActive=false` 不再续租）。要把它收敛到 ~7s，必须让「DS 自己宣告结算」被如实记账：

- **proto delta**：`ReleaseOwnerRequest` 增加一个「该实例已自行终结」的布尔信号（或新增 `RevokeInstanceLease` rpc）。现有 5 个 rpc 无吊销/收缩租约接口，且 `owner_repo.go` 明写「deadline 只前进」，**复用 `RenewInstanceLease(0)` 行不通**。
- 命中该信号时 Release 把屏障按 `now + skew` 盖，而不是等冻结的旧租约。
- **前置条件（必须先确认，否则 D 是谎报）**：DS 在 ended ACK 之后必须**主动清退**残留玩家，而不是只通知客户端 travel —— 当前 UE 在 ACK 后 `StopBattleHeartbeat` 会一并解除自我 fencing watchdog，一个不肯 travel 的客户端将不再被踢。
- 分工：proto 生成与 C++ pb 同步按 §5 属 Codex；UE 侧改动按 §11.6 由用户编译验证。

## 10. 关闭条件

| # | 门槛 | 状态 |
|---|---|---|
| 1 | 修法定案并落码 | **已满足**：采纳 C，见 §9.1 |
| 2 | owner 侧补机械守护「BATTLE owner 被 Release 之后，下一次 Begin 的屏障」（此前全仓零覆盖） | **已满足**：两栈各一条，均通过双变异击杀，见 §9.3 |
| 3 | Go 与 Python 两栈注释同步 | **已满足**，见 §9.2 |
| 4 | §8 三条缺陷各自定案（2s 预算甩尾 / 死代码辩护 / ended 镜像滞留 2h） | 死代码辩护已改写；**预算甩尾与镜像滞留未修** |
| 5 | D（把结算后大厅出生门从 ~22s 收敛到 ~7s） | **未执行**，见 §9.4 |
| 6 | `-race` 与真机玩家 E2E | **未执行** |

⇒ **未关闭**。C 本身已可提交，但 §8 的「一次慢 RPC 就甩掉半个花名册」会让 INC-20260824-001 原样复发，应在同批或紧接的下一批处理。
