# Go → Python 迁移覆盖矩阵（机械推导）

> 由 `python/tools/parity/coverage.py` 生成。**不要手改** —— 改了下次重跑就没了。
> `RPC` 的分母是该服务**实际注册的全部 servicer** 的 rpc 之和，不是同名 proto 一个文件；
> 有几个服务挂了搭车 servicer（见表内），只看同名 proto 会把 RPC 面算少。

| 服务 | Go 行 | Py 行 | RPC | 阶段 | main | conf | 注册的 servicer |
|---|---:|---:|---:|---|:-:|:-:|---|
| `player` | 5470 | 6176 | 29/29 | 可跑 | ✓ | ✓ | PlayerService, ConfigTableAdminService* |
| `guild` | 3655 | 4230 | 23/23 | 可跑 | ✓ | ✓ | GuildService, GroupService |
| `player_locator` | 3115 | 3049 | 16/16 | 可跑 | ✓ | ✓ | PlayerLocatorService |
| `friend` | 2362 | 2245 | 10/10 | 可跑 | ✓ | ✓ | FriendService |
| `push` | 2198 | 2181 | 1/1 | 可跑 | ✓ | ✓ | PushService |
| `leaderboard` | 2192 | 2969 | 7/7 | 可跑 | ✓ | ✓ | LeaderboardService |
| `mail` | 2153 | 2689 | 9/9 | 可跑 | ✓ | ✓ | MailService |
| `owner` | 1779 | 2002 | 5/5 | 可跑 | ✓ | ✓ | OwnerService |
| `trade` | 1447 | 1703 | 4/4 | 可跑 | ✓ | ✓ | TradeService |
| `data_service` | 1037 | 1050 | 3/3 | 可跑 | ✓ | ✓ | DataService |
| `dialogue` | 942 | 1015 | 3/3 | 可跑 | ✓ | ✓ | DialogueService |
| `inventory` | 7320 | 5855 | 24/29 | 部分 RPC | ✓ | ✓ | InventoryService, ConfigTableAdminService*, BagService* |
| `ds_allocator` | 18990 | 323 | 0/11 | 仅核心不变量 | — | — | DSAllocatorService, GmService, ConfigTableAdminService* |
| `hub_allocator` | 12654 | 261 | 0/10 | 仅核心不变量 | — | — | HubAllocatorService |
| `login` | 8986 | 0 | 0/10 | 未开始 | — | — | LoginService |
| `matchmaker` | 8547 | 161 | 0/7 | 仅核心不变量 | — | — | MatchService, ConfigTableAdminService* |
| `battle_result` | 7425 | 188 | 0/4 | 仅核心不变量 | — | — | BattleResultService |
| `team` | 5932 | 3836 | 0/17 | 仅核心不变量 | — | ✓ | TeamService |
| `auction` | 4500 | 277 | 0/5 | 仅核心不变量 | — | — | AuctionService |
| `mission` | 2738 | 287 | 0/6 | 仅核心不变量 | — | — | MissionService |
| `chat` | 1568 | 669 | 0/2 | 仅核心不变量 | — | ✓ | ChatService |

`*` = 条件注册（Go 侧写在 `if` 里，由配置开关控制）。

**合计：134/211 个 RPC 有 Python 实现（63%）；Go 105010 行 / Py 41166 行。**

## 逐服务未实现的 RPC

### `auction`（Go 4500 行）

- **AuctionService** — 缺 5：`PlaceOrder`, `Bid`, `CancelOrder`, `ListMarket`, `ListMyOrders`

### `battle_result`（Go 7425 行）

- **BattleResultService** — 缺 4：`ReportResult`, `GetMatchResult`, `ListPlayerHistory`, `ReportProgress`

### `chat`（Go 1568 行）

- **ChatService** — 缺 2：`SendMessage`, `PullHistory`

### `ds_allocator`（Go 18990 行）

- **DSAllocatorService** — 缺 7：`AllocateBattle`, `ResolveBattleTarget`, `ReleaseBattle`, `AbortPreactiveBattle`, `EnsurePlayerDeparture`, `Heartbeat`, `ListBattles`
- **GmService** — 缺 3：`SendCommand`, `PollCommands`, `AckCommand`
- **ConfigTableAdminService** — 缺 1：`ReloadConfigTable`

### `hub_allocator`（Go 12654 行）

- **HubAllocatorService** — 缺 10：`AssignHub`, `ReleaseHub`, `EnsureHubDepartureForBattle`, `TransferHub`, `ListHubs`, `Heartbeat`, `AcknowledgeAdmission`, `AcknowledgeDeparture`, `ListHubLines`, `TransferToLine`

### `inventory`（Go 7320 行）

- **BagService** — 缺 5：`LoadBag`, `AppendJournal`, `SaveCheckpoint`, `GetSections`, `PurchaseCapacity`

### `login`（Go 8986 行）

- **LoginService** — 缺 10：`Login`, `Logout`, `IssueDSTicket`, `GetPlayerNo`, `GetRegisterNo`, `ListAccountRoles`, `EnterRole`, `SelectRole`, `VerifyDSTicket`, `GetResumeContext`

### `matchmaker`（Go 8547 行）

- **MatchService** — 缺 6：`StartMatch`, `CancelMatch`, `ConfirmMatch`, `GetMatchProgress`, `ReleaseMatch`, `ResolvePlayerMatchContext`
- **ConfigTableAdminService** — 缺 1：`ReloadConfigTable`

### `mission`（Go 2738 行）

- **MissionService** — 缺 6：`ListMissions`, `AcceptMission`, `AbandonMission`, `ClaimMissionReward`, `ReportMissionFacts`, `CompleteAllMissions`

### `team`（Go 5932 行）

- **TeamService** — 缺 17：`CreateTeam`, `Invite`, `AcceptInvite`, `LeaveTeam`, `Kick`, `SetReady`, `GetTeam`, `GetMyTeam`, `ListMyPendingInvites`, `SetTeamMap`, `ListOpenTeams`, `ApplyToTeam`, `ListTeamApplications`, `HandleTeamApplication`, `BeginTeamMatch`, `EndTeamMatch`, `GetPlayerTeam`

