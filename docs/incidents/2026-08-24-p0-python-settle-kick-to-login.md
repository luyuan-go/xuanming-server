# [INC-20260824-001][P0] 打完一局结算后被踢回登录（Python 栈）

> **状态**：根因确认；缺口①已落码待验证，缺口②修复方案复核阻断（未关闭）  
> **类型**：`availability`  
> **环境**：本机 Python 栈（免 Docker 一键），`mode=local` + `authority_mode=legacy` + `launcher=editor`  
> **首次发生时间（UTC）**：未知（首次可复现记录见 §3）  
> **首次发现时间（UTC）**：2026-08-24  
> **负责人**：待指定  
> **受影响服务/版本**：`login`（Python）、`ds_allocator`（Go 与 Python 同构）；工作区改动未提交，`HEAD=ca5ee4fe`  
> **最后更新**：2026-08-24

## 0. 一句话结论

一局正常打完后玩家被踢回登录界面，根因是**同一条「结算 → 回大厅」链上两个互相独立的缺口同时成立**：主路径被 Python 移植遗留的 `TypeError` 结构性堵死，降级路径又被「正常结算从不释放 owner」堵死，两条路同时不通时客户端按权威恢复判弃协议回登录。

## 1. 影响与范围

- 玩家影响：每一局正常结算后 100% 被踢回登录界面，需重新登录才能继续游戏。对局结果本身已正确入账（`battle_result` 不受影响）。
- 范围：Python 栈的 `login` 服务（缺口①）；缺口②在 Go 与 Python 两栈同构存在，但仅 `mode=local` 可达（见 INC-20260824-003 §0 的作用域分析）。
- 未在生产发生：生产为 Go 栈 + Model B，缺口①不存在；缺口②在 Model B 下无对应分支。

## 2. 第一现场与证据

### 2.1 症状

- 结算面板正常显示，随后客户端不进大厅、直接回到登录界面。
- 服务端**零 ERROR 日志**：`IssueDSTicket` 恒返回 `code=1` 且 `err` 为空。

### 2.2 原始证据

- 缺口①：`inspect_battle_route` 在 Python 侧签名为「成功返回单个 `BattleRouteState`、失败抛 `PandoraError`」（`python/pandorapy/services/login/battleroute.py:174`），而两处调用点保留了 Go 的 `(state, err)` 双值解包。`BattleRouteState` 是 `IntEnum`、不可迭代 ⇒ **只要执行到该行必抛 `TypeError`**，且不落任何日志。两处调用点：`python/pandorapy/services/login/biz.py:1616`（`_try_battle_reconnect`）与 `:1768`（`_guard_hub_route_against_active_battle`）。
- 缺口②：正常结算路径下 owner 归属从不释放。释放挂在 `errHeartbeatTerminal` 分支上（`services/battle/ds_allocator/internal/biz/allocator.go:3016`），而该分支的触发条件是「心跳到达时记录**已经**是 ended」——需要**第二跳**心跳。
- 客户端判弃常量：`IncompleteTargetStreakLimit = 3`（`Pandora/Source/Pandora/Public/Module/Account/Model/MyDsRecoveryCoordinator.h:756`）；连续 3 次残缺 TARGET → `AbandonRecovery` + `HandleAuthoritativeEntryTerminal`（`MyDsRecoveryCoordinator.cpp:2180-2192`）。

### 2.3 已排除的噪声

- **不是 DS 崩溃**：DS 进程正常结算、正常发 ended 心跳。
- **不是 `battle_result` 记账失败**：结算数据正确落库。
- **不是网络抖动**：缺口①是确定性 `TypeError`，与网络无关。
- **不是「第二跳心跳丢了」**：第二跳在物理上根本不存在，见 §5.2。

## 3. 时间线

| 时间（UTC） | 事件 |
|---|---|
| 未知 | 缺口②随 legacy 心跳路径长期存在（`errHeartbeatTerminal` 分支自 2026-08-04 起即为「正常结算收口点」的错误假设） |
| 未知 | 缺口①随 Python 移植引入（`inspect_battle_route` 改单值返回时未同步两处调用点） |
| 2026-08-24 | 本机复现「打完一局被踢回登录」；代码审计定位两个缺口 |
| 2026-08-24 | 缺口①落码（`try/except` 折回 Go 的 `(state, err)` 语义）；缺口②落码方案在复核中被判引入新风险，另立 INC-20260824-003 |

## 4. 调用链与关键变量

正常结算的**主路径**（不读 owner）：

```
DS 结算 → SendEndedHeartbeatAndReturnToHub (PandoraBattleGameMode.cpp:2273)
  → allocator 写回 State=ended，回 OK
  → HandleEndedHeartbeatComplete (:2366) → StopBattleHeartbeat + NotifyPlayersBattleSettled
  → ClientPandoraBattleSettledReturnToHub (MyEntityPlayerController.cpp:2233)
  → UMyMatchModel::ReturnToHubDs (MyMatchModel.cpp:1320) → IssueDSTicketScoped("hub", fence)
  → login._guard_hub_route_against_active_battle (biz.py:1712)
      → _resolve_battle_authority (:1372)  ← 只读 locator presence + matchmaker，**不读 owner**
      → inspect_battle_route → TERMINAL → 放行 Hub 票
```

缺口①令 `inspect_battle_route` 那一行必抛 `TypeError` ⇒ `TERMINAL → 放行 Hub` 分支**从未被执行过** ⇒ 主路径整条失效。

客户端随后走**降级路径**（权威恢复）：`GetResumeContext` → owner 仍是 `BATTLE` 指向已结算的 DS → 残缺/失效 TARGET → 连续 3 次 → 判弃回登录。

关键变量：`endedPod` / `endedUID` / `endedPlayers`（缺口②修复引入）；`BattleRouteState.UNKNOWN`（缺口①修复的 fail-closed 零值）。

## 5. 根因

### 5.1 直接根因

两个独立缺口在同一条链上叠加：

1. **缺口①（主路径）**：Python 移植把 `inspect_battle_route` 从 Go 的 `(state, err)` 改成「单值返回 + 抛异常」，但两处调用点未同步，双值解包对 `IntEnum` 必抛 `TypeError`。
2. **缺口②（降级路径）**：正常结算从不释放 owner，降级路径拿到的归属指向一台已结算的 DS。

### 5.2 触发条件

缺口①：无条件，每次调用必然发生。

缺口②的关键事实是「**第二跳心跳物理上不可能到达**」：

- `state="ended"` 全仓只由 `SendEndedHeartbeatAndReturnToHub` 发送，一局只发一次；
- 周期心跳的 `CurrentBattleState` 恒为 `"running"`（`SetBattleHeartbeatState` 唯一调用点恒传该值），tick 5s；
- ended 的 ACK 回调 `HandleEndedHeartbeatComplete` 在毫秒级内 `StopBattleHeartbeat()`。

⇒ 记录已是 ended 时不会再有心跳到达，`errHeartbeatTerminal` 分支在正常结算路径上**一次都不触发**。

> **仓内长期互相矛盾的两条注释**：`allocator.go` 心跳分支旧注释称「DS 转 ended 并继续上报心跳」，而 `sweepOnce` 的 ended 分支注释写明「无第二跳」。**sweep 那条才符合实测**，心跳分支那条是错的，已在本批改动中标注更正。不写清这一点，下一个人会再次按错误前提改代码。

### 5.3 故障放大因素

- 两个缺口都**零日志**：缺口① `TypeError` 被吞成 `code=1` 且 `err` 为空；缺口②没有任何组件会抱怨 owner 未释放。
- 主路径与降级路径**同时**失效，玩家没有任何一条可用出口。

### 5.4 为什么现有保护没有挡住

- 跨栈门禁 `python/tools/parity/coverage.py` 只做 RPC 方法名级覆盖，查不出「返回签名改了但调用点没改」这类漂移。
- `ci_backend.ps1` 的 `go test` 与 `pytest` 各跑各的，无跨栈行为对账。
- 缺口②的既有测试断言的是「第一跳不释放、第二跳才释放」——它锁定的正是那个错误前提。

## 6. 全仓同类问题扫描

- `inspect_battle_route` 的双值解包：全仓仅上述两处，均已修复。
- **同类风险**：Go→Python 移植中「返回签名从 `(value, err)` 改为『单值 + 抛异常』」的函数，其调用点是否全部同步——本次未做全仓扫描，列为 §10 行动项。

## 7. 处置与永久修复

### 7.1 临时止血

无（缺口①落码前无绕过手段；玩家只能每局重登）。

### 7.2 永久修复

- 缺口①：两处调用点改为 `try/except`，把异常按 Go 语义折回 `(UNKNOWN, err)`。`UNKNOWN` 是零值、天然 fail-closed，仍走「不可证明终态 → 拒绝 + 可重试」分支，不放宽任何门。**已落码，未提交**。
- 缺口②：**修复方案复核阻断**。已落码的方案（在首次 ended 心跳内释放 owner）被判引入再入屏障坍塌，见 [INC-20260824-003](2026-08-24-p1-ds-allocator-ended-release-barrier-collapse.md)。修法待定。

### 7.3 防复发规则

- Go→Python 移植改变函数返回形状时，必须同批检查全部调用点；`parity/coverage.py` 的 RPC 名级覆盖**不构成**该检查。
- 任何「DS 会继续上报心跳」的假设，必须以 UE 侧 `StopBattleHeartbeat` 的调用时机为准，不得只读服务端注释。

## 8. 验证矩阵

| 项 | 状态 |
|---|---|
| Python 定向测试（267 项） | 通过 |
| Go `ds_allocator` / `matchmaker` 全量测试 + vet | 通过 |
| ruff / YAML / `git diff --check` | 通过 |
| `-race` | **未执行**（本机无 CGO 编译器；需走 `tools/scripts/go_test_race.ps1` 的 docker 路径） |
| 真机玩家 E2E（打完一局回大厅） | **未执行** |
| 缺口②的反例覆盖 | **未执行**，见 INC-20260824-003 |

> 测试全绿不覆盖 §5.2 的时序前提，也不覆盖 INC-20260824-003 的屏障反例。

## 9. 部署、回滚与观察

未提交、未部署。回滚即丢弃工作区改动。

## 10. 剩余风险与行动项

1. 缺口②修法待定，见 INC-20260824-003。
2. 全仓扫描 Go→Python 移植中返回形状变化的函数及其调用点。
3. 真机玩家 E2E 复测「打完一局回大厅」。
4. `-race` 在 docker 路径下补跑。

## 11. 关闭审核

关闭门槛：缺口②修法定案并落码、真机 E2E 通过、观察窗口内 `authority_entry_terminal` 零新增。当前**全部未满足**。
