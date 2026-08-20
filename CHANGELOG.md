# 更新日志

本文件记录 Pandora 各发布版本的**修复内容 / 变更**,遵循
[Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 与
[语义化版本](https://semver.org/lang/zh-CN/)。

发布时 `make_release.ps1 -Version <版本>` 会自动读取对应版本段落,写入 release manifest 与
release notes;因此**每次发布前先在此文件顶部新增一段**,再打 tag / 出包。

版本号同时体现在:客户端 `Pandora/Config/DefaultGame.ini` 的 `ProjectVersion`、后端 git tag
(`git describe` 注入 `pkg/version.Version`),二者必须与本文件、release manifest 的版本一致。

## [Unreleased]

- 待下一个版本累积的改动写在这里,发布时移入新版本段落。

### 修复 (Fixed)
- 秒级无限重连收口(2026-08-18 实测:PIE 客户端连未 cook 的 editor DS 后反复被踢又反复重连)。
  分两层落地,互不替代:
  - **根因层(本仓)**:`-DsLauncher editor` 的本机 DS(战斗与大厅两条)自动带
    `-DPCVars=net.SkipMissingLevelDisconnect=1`。未 cook 的 DS 与 PIE 客户端对 World Partition
    runtime cell 的命名天然对不上,服务端默认会因此直接关连接;关掉后只打 Warning 忽略。
    **仅**作用于 editor 形态 —— packaged 本机 DS 与 k8s Linux DS 跑的是 cook 过的内容,
    那里的内容不一致是真问题,保留引擎的踢人保护。
  - **兜底层(客户端仓)**:DS 恢复协调器改为重连预算 —— 同一 DS 端点相邻间隔 ≤120s 的失败
    连成 streak,达 3 次即判终态(停重连 timer、放行引擎默认回登录)。客户端在 UE 5.8 里
    **拿不到**服务端的关连接原因(引擎在客户端侧丢弃 NMT_CloseReason),因此不猜原因、只给预算;
    预期内的返回大厅拆链先放行、绝不计入预算。

## [0.1.0] - 2026-07-24

> ⚠️ **本段为首版草稿,请按本次实际发布内容增删后再正式发布。**
> 首个整合内测版本:后端 21 服务 + UE 客户端/DS 全链路首次成体系装配,并落地标准发布线。

### 新增 (Added)
- 打包发布线:构建产物退出版本库,制品目录不可变发布(版本戳 + sha256 + build-info),
  release manifest(build once, promote many);SVN/git 服务端钩子拒收产物回流。
- 语义化版本:客户端 `ProjectVersion` + 后端 git tag 注入,包/镜像运行时可自报版本。
- 权威归属(owner authority)服务:每玩家 `owner_epoch` 线性一致权威 + 短租约 fencing + 进场屏障。
- 背包域(bag domain):随身/仓库分层驻留,journal + checkpoint 恢复,邮件为离线资产中转层。
- 实时成长第三通道:局中经验/掉落即时入账(DS 报事实、服务端换算)。

### 变更 (Changed)
- 会话安全:区分"顶号"与"会话自然失效",旧 JTI 全服务吊销 + DS 票据兑换点复核。
- 进场恢复:登录后单一幂等进场链加固,推送事件路由完善(会话代次绑定)。
- 数据保留:只增表统一 90 天保留期 + 周期清理,dbcheck 上线前发布门禁。

### 修复 (Fixed)
- 交易结算撕裂、hub locator fail-closed、bag journal 覆盖门等 P0 修复。
- 推送投递跨 Pod fencing、gap fail-closed、好友换新 ID 等可靠性边界修复。
- 本地集群恢复与背包启动配置、21 服务构建与监控口径修复。

### 已知问题 / 未闭环 (Known Issues)
- 部分 P0 事故档案未关闭(缺真实 Redis/MySQL/多 Pod/race/E2E 验证)。
- owner authority 强依赖切换、部分 UE 编译验证、cpp pb 重生等仍在进行中。

[Unreleased]: 未发布
[0.1.0]: 首个内测版本
