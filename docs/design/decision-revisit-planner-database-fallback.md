# 决策复议：策划数据库自动回落边界

> 状态：**已拍板，2026-08-20**。用户明确要求远端尚未配置时策划一键入口立即使用本机数据库；
> 同时必须避免已使用远端 workspace 的机器在断网后自动切到另一套本机数据。

## 1. 被复议的旧决策

[`decision-revisit-planner-central-mysql.md`](./decision-revisit-planner-central-mysql.md) 原先要求
策划双击入口强制 `central-managed`，任何 bundle/网络问题都不得回落 `local-owned`。这个规则对
已经登记并写入远端 workspace 的机器是必要的，但当前中心服务和发布 bundle 尚未部署；把它无条件
应用到全新机器，会让原本可用的本机测试链直接阻断。

## 2. 新决策：只在“从未进入远端”时自动使用本机

模式选择按以下证据执行：

1. `installers/planner-db/central-mysql.json` 存在：本轮锁定 `central-managed`。配置损坏、DNS、
   TLS、认证、登记、权限或 schema 预检失败都直接失败，不回落本机。
2. bundle 不存在，且本机没有 central applied state、central runtime profile、planner workspace
   identity：选择 `local-owned`，启动本工作区精确归属的本机 MySQL。
3. bundle 不存在，但上述任一 central 历史证据存在：直接失败。这个状态表示发布包丢文件或远端
   配置损坏，自动回本机会制造两套数据世界。

“远端连接不了马上本地”在本决策中的准确含义是：**远端从未配置/登记时立即本地**；不是已经使用
远端后遇到临时断网就切库。两种模式不复制数据，也不做双写。

## 3. 可见性与安全边界

- CMD 启动即打印真实入口绝对路径和本轮选择意图；PowerShell 在 profile 解析后打印最终
  `mode=local-owned|central-managed` 与 backend。
- `local-owned` 仍要求 PID、`mysqld.exe`、本工作区 `my.ini` 和 listener 四重归属；不会复用、
  迁移或停止外部 MySQL。
- `central-managed` 的 down/reset 对远端数据库永远零生命周期动作。
- 本决策只改策划免 Docker 本地链路；K8s 文件、集群运行态和线上数据库零改动。

## 4. 验收标准

1. 无 bundle、无 central 历史状态的干净工作区选择 `local-owned`，不出现“Remote planner database
   bundle is missing”阻断。
2. bundle 存在但损坏时仍锁定 central 并失败，不能因为解析错误改走本机。
3. central applied state、runtime profile 或 identity 任一存在而 bundle 丢失时 fail-closed。
4. 日志明确打印最终 mode；本机/远端切换必须整批刷新业务 DSN，禁止旧进程被 skip。
5. K8s scoped diff 为空。
