# [INC-20260824-002][P0] PVP 卡在「分配战斗服」进不去对局

> **状态**：根因确认，已修复待部署（未关闭）  
> **类型**：`availability`  
> **环境**：本机开发/策划机，`mode=local` + `launcher=editor`  
> **首次发生时间（UTC）**：未知  
> **首次发现时间（UTC）**：2026-08-24  
> **负责人**：待指定  
> **受影响服务/版本**：`matchmaker`（配置 `matchmaker-dev.yaml`）；工作区改动未提交，`HEAD=ca5ee4fe`  
> **最后更新**：2026-08-24

## 0. 一句话结论

`matchmaker-dev.yaml` 从未配置 `ds_allocate_timeout`，回落到代码默认 **60s**，而 `mode=local` + `launcher=editor` 的战斗 DS 冷启动远超 60s，导致 `AllocateBattle` 每 60s 被客户端侧砍断一次并无限重分配，玩家永远卡在「分配战斗服」。

## 1. 影响与范围

- 玩家影响：PVP（`5v5_ranked`）无法进入对局，界面停在「分配战斗服」。
- **PVE 不受影响**：`matchmaker-pve.yaml:77` 早已配 `330s`。这正是「PVE 能进、PVP 进不去」的分叉点。
- 未在生产发生：生产 DS 为预热镜像，冷启动不在该量级。

## 2. 第一现场与证据

### 2.1 症状

客户端停在「分配战斗服」，无错误提示；服务端不断重新发起分配。

### 2.2 原始证据

- `services/matchmaking/matchmaker/internal/conf/conf.go:296-297`：`if c.Match.DSAllocateTimeout == 0 { c.Match.DSAllocateTimeout = config.Duration(60 * time.Second) }` —— 键缺失即回落 60s。
- `services/battle/ds_allocator/etc/ds_allocator-dev.yaml:13`：`timeout: "330s"`（注释原文「必须盖过 allocator.ready_wait_timeout(300s,editor 冷启动)+ 回收余量」）。
- 同文件 `:111`：`ready_wait_timeout: "300s"`；`:102` 注释明写「配套三处一起改：上方 `server.grpc.timeout=330s`、matchmaker `ds_allocate_timeout=330s`」——**三处中的第三处在 dev 档缺失**。
- `services/matchmaking/matchmaker/etc/matchmaker-pve.yaml:77`：`ds_allocate_timeout: "330s"`（对照组）。

### 2.3 已排除的噪声

- 不是 DS 起不来：DS 能起，只是比 60s 慢。
- 不是 `ds_allocator` 侧超时：服务端预算 330s，从未先到期。

## 3. 时间线

| 时间（UTC） | 事件 |
|---|---|
| 未知 | `ready_wait_timeout` 为 editor 冷启动调大到 300s，配套的三处只改了两处 |
| 2026-08-24 | 复现「卡分配战斗服」，定位 dev 档缺键 |
| 2026-08-24 | 补 `ds_allocate_timeout: "330s"`（已落码，未提交） |

## 4. 调用链与关键变量

`matchmaker` 后台 worker → `AllocateBattle`（出站客户端调用，预算 = `Match.DSAllocateTimeout`）→ `ds_allocator` 服务端阻塞等 DS ready 心跳（`ready_wait_timeout=300s`，`server.grpc.timeout=330s`）。

有效预算 = `min(客户端限额, 入站 deadline)`。客户端 60s < 服务端 330s ⇒ 客户端先超时，服务端那一侧的等待白做。

## 5. 根因

### 5.1 直接根因

`matchmaker-dev.yaml` 缺 `ds_allocate_timeout` 键，回落代码默认 60s，小于 editor 形态 DS 的冷启动耗时。

### 5.2 触发条件

`mode=local` + `launcher=editor`，且 DS 冷启动 > 60s（大图尤甚）。

### 5.3 故障放大因素

- 超时后**自动重新分配**，表现为「一直在转圈」而不是「报错」，掩盖了真实原因。
- **失败可能自愈式蒙对**：若某次冷启动恰好快于 60s 就成功了，会让人误判「时好时坏是抖动」。

### 5.4 为什么现有保护没有挡住

配置键缺失走的是静默回落，没有任何启动期一致性校验去比对「客户端预算 ≥ 服务端 `grpc.timeout`」这条不变量——而这条不变量只写在注释里。

## 6. 全仓同类问题扫描

`ds_allocator-dev.yaml:102` 点名的「配套三处」是一条跨文件人工不变量。本次未扫描其它同类跨文件配套键，列为 §10 行动项。

## 7. 处置与永久修复

### 7.1 临时止血

无（需改配置并重启 matchmaker）。

### 7.2 永久修复

`matchmaker-dev.yaml:67` 补 `ds_allocate_timeout: "330s"`，并附六行注释说明该键与 `ds_allocator` 两个超时的关系。**已落码，未提交**。

### 7.3 防复发规则

跨文件配套超时（客户端预算 vs 服务端 `grpc.timeout` vs `ready_wait_timeout`）应有启动期断言，而非仅靠注释约定。

## 8. 验证矩阵

| 项 | 状态 |
|---|---|
| YAML 校验 | 通过 |
| Go `matchmaker` 全量测试 + vet | 通过 |
| 真机 PVP 进场 E2E | **未执行** |

## 9. 部署、回滚与观察

未提交、未部署。回滚即移除该键（回到 60s 回落）。

## 10. 剩余风险与行动项

1. 真机 PVP 进场复测。
2. 为「客户端预算 ≥ 服务端 `grpc.timeout`」加启动期一致性校验。
3. 扫描其它跨文件配套配置键。

## 11. 关闭审核

关闭门槛：真机 PVP 连续进场成功、观察窗口内无 `AllocateBattle` 超时重试。当前**未满足**。
