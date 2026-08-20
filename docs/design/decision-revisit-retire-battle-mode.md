# decision-revisit:退役 battle 混合模式(Windows DS 只保留给 local 断点调试)

> 触发:用户拍板(2026-07-14)「Windows DS(PandoraServer.exe)只在 local 模式启动,
> 其他模式一律走正常 k8s 集群 + Agones(Linux DS);含战斗 .cmd 入口不再需要」。
> 本文档按 `AGENTS.md` §7 推翻 `decision-revisit-battle-services-in-docker.md`(2026-07-01
> 拍板落地的含战斗混合模式)。决策级别:编排 / 运维链路,不改业务逻辑与不变量。
>
> 状态:**已拍板(用户口头),同日落地**。

---

## 1. 旧决策 / 旧问题

2026-07-01 落地的 `battle` 混合模式(18 业务容器 + 宿主 `ds_allocator`/`hub_allocator`
exec Windows DS),入口为四个双击 .cmd(策划/内网 含战斗 启动+停止)→ `play.ps1 -Battle[-Intranet]`
→ `start.ps1 -Mode battle`。当时动机是「含战斗那台服务器不想装 Go / 起 19 个宿主 go 服务」。

此后 `-Mode k8s`(本机 minikube + Agones + 真 Linux DS)链路已完全打通并一键化:
自动构建 UE Linux DS 镜像、in-cluster DS Envoy、宿主桥接 + UDP 中继、局域网多机客户端实测可用
(见 `docs/design/agones-dev.md`、repo memory `k8s-ds-incluster-envoy`)。battle 模式作为
「非 local 环境跑 Windows DS」的中间形态,存在两套真 DS 链路并行维护的成本,且与线上形态
(Agones Linux DS)不一致,联调结论说服力弱。

## 2. 新决策

1. **Windows DS 只有 `-Mode local` 一条启动链路**(19/20 go 服务全宿主,断点调试专用,保持不变)。
2. **其他一切「要真 DS」的场景一律 k8s + Agones(Linux DS)**:
   - 内网服务器:`内网服务器一键启动-k8s集群.cmd`(= `start.ps1 -Mode k8s`,已支持局域网多机客户端)。
   - 策划机:只当客户端连内网 k8s 服务器;不再本机起含战斗后端。
3. **battle 混合模式退役**:
   - 删除双击入口:`策划一键启动-含战斗.cmd`、`内网服务器一键启动-含战斗.cmd`。
   - 保留双击入口:`策划一键停止.cmd`、`内网服务器一键停止.cmd`(注释改为「清理遗留 battle 栈」;
     所有机器清理干净后可另行删除)。
     **2026-08-10 补充:`内网服务器一键停止.cmd` 已删除** —— 它与 `策划一键停止.cmd` 逐字节等价
     (同样是 `play.ps1 -Battle -Stop`,并未传 `-Intranet`),留两个入口没有任何区别。
     **2026-08-20 补充:`策划一键停止.cmd` 也已删除**；清理遗留 battle 栈仍可使用命令行
     `play.ps1 -Battle -Stop` / `start.ps1 -Mode battle -Down`。
   - `start.ps1 -Mode battle`:**启动路径拒绝执行**(报废弃错误并指引 k8s);`-Down`/`-Status`
     保留,用于清理/查看旧机器上遗留的 battle 环境;`-Resume`/`-Reset` 同样拒绝。
   - `play.ps1 -Battle`:启动路径拒绝执行并指引;`-Battle -Stop` / `-Battle -Status` 保留。
   - battle 专用的宿主 DS 预检/探测辅助(Test-BattlePrerequisites 等)随启动路径一并移除。
4. `docker` / `intranet` 模式维持 DS=mock 不变(容器内本就没有真 DS,非本次范围)。
5. `gen_cluster_config.ps1 -HostAllocators` 参数保留但不再有调用方(纯函数式生成器,零运行成本);
   下次触碰该脚本时可顺手移除。

## 3. 风险

| 风险 | 说明 | 缓解 |
|---|---|---|
| 旧机器残留 battle 栈 | 已部署过含战斗版的机器上还有 18 容器 + 2 宿主 allocator + Windows DS 进程 | `-Mode battle -Down` / 停止 .cmd 保留,可继续一键清理 |
| 策划机失去本机真战斗能力 | 策划要打真实战斗必须连内网 k8s 服务器(或自装 minikube + Linux DS 包,门槛高) | 内网 k8s 一键已支持局域网多机客户端(实测);此为用户明确接受的取舍 |
| 双击入口消失造成困惑 | 有人习惯双击含战斗 .cmd | README / planner-quickstart / offline-images README 同步改口径 |
| local 调试链路被误伤 | local 也 exec Windows DS | **local 完全不动**(本决策的明确边界) |

## 4. 迁移成本

- 编排脚本:`start.ps1`(battle 启动/Resume/Reset 分支改拒绝,前置检查降为仅 Docker)、
  `play.ps1`(-Battle 启动分支改拒绝,battle 专用辅助函数移除)。
- 双击入口:删 2 个启动 .cmd,2 个停止 .cmd 改注释(纯 ASCII 铁律不变)。
- 文档:README 模式表、docs/ops/planner-quickstart.md、deploy/offline-images/README.md、
  旧决策文档加推翻标注、PROGRESS.md 追加。
- 不碰:业务代码、proto、compose 定义、`gen_cluster_config.ps1`、k8s/Agones 清单、local 链路。

## 5. 验收标准

1. `start.ps1 -Mode battle`(无 -Down/-Status)→ 明确报错退出并指引 `-Mode k8s`,不拉起任何东西。
2. `start.ps1 -Mode battle -Down` / `play.ps1 -Battle -Stop` 仍能清理旧 battle 环境。
3. `start.ps1 -Mode local` 行为不变(宿主 go 服务 + exec Windows DS)。
4. `内网服务器一键启动-k8s集群.cmd` 行为不变(真 Linux DS)。
5. 仓库根目录不再有两个「含战斗启动」.cmd;README/文档无「双击含战斗启动」的口径。
