# 决策复议：免 Docker 第三方便携包随策划 SVN 分发

> 状态：**已拍板，2026-08-20**。用户明确要求 `D:\luyuan\Pandora-Server` 的 SVN 工作副本自带所需安装包，而 `F:\work\XuanMing-Server` 的 Git 仓库只保留空目录契约；有包就本地使用，缺包才联网。

## 1. 被复议的旧决策

2026-07-23 的发布线决定“构建产物退出版本库”，业务镜像、UE Packages 和后端构建产物进入制品目录，不再用 Git/SVN 承担制品库职责。该原则继续有效。

本次新问题不是发布业务产物，而是策划机不能安装 Docker、PowerShell、MySQL、Redis、Kafka、JRE、mkcert、Envoy，又不应要求每位使用者手工逐项下载。让使用者另配共享盘或 MinIO 地址仍有一次环境配置，不能满足“双击仓库内入口即可启动”。

## 2. 新决策与边界

新增一个**严格白名单例外**：

- Git：`installers/localinfra/` 只跟踪 `README.md`，二进制 payload 全部忽略。
- 策划 SVN：同一路径跟踪当前固定版本的 7 个第三方 Windows x64 便携包。
- 运行时：有效 cache → 显式 `PANDORA_LOCALINFRA_MIRROR` 或仓库安装包 → 固定官方 URL。目录为空或缺某个当前文件才对该文件联网；同名文件 SHA256 不符必须硬失败。
- 本机 SVN bundle 通过固定 SHA256 后直接只读解包，不再重复复制 544.5 MiB 到 cache；显式 UNC/移动盘镜像和公网下载仍先落可变的 `run/localinfra/cache`。`-Force`、坏缓存清理或解包流程都不得改写 SVN 目录。
- 每个已展开的 `run/localinfra/dist/<组件>` 都保存当前包 SHA256 marker；probe 和 marker 同时匹配才可复用。无 marker、旧 marker 或 `-Force` 都先在同盘 staging 完整解包、校验并写 marker，再原子替换目标目录。下载、解包、probe 或替换失败必须保留旧目录和独立的 data/cfg/log/pid；目标仍被进程使用时明确失败并提示先停止本工作区，不得为升级强杀未知进程。marker 只绑定当前固定归档身份，不是逐文件防篡改证明；供应链执行闸仍是解包前固定 SHA256。
- 这份例外**不包含** Pandora 自建 exe、UE Packages、Docker/OCI 镜像、数据库数据、日志、证书、secret，也不恢复任何已退役的 `*.tar` 随仓库分发方案。

当前白名单：PowerShell 7、MySQL、Redis、Kafka、Temurin JRE、mkcert、Envoy Windows layer。新增组件必须重新评审体积、来源、许可证、校验依据和是否真属于免 Docker 启动硬依赖。

## 3. 为什么不用更复杂的方案

现有 `local_infra.ps1::Get-Archive` 已经封装缓存、镜像、公网续传、固定 SHA256 和失败清理；`bootstrap_pwsh.cmd` 也有同一契约。新增默认仓库镜像即可，不需要再造下载器、包管理服务或安装状态机。

共享盘/MinIO 仍适合长期规模化分发，也保留为显式覆盖入口；但当前策划机数量和“拿到 SVN 即可双击”的目标下，SVN 白名单包是最短消费链。未来若切到统一制品服务，只需让同一镜像入口指向同步目录，不改调用者。

## 4. 成本与风险

- 当前 7 包合计 `570,950,631` 字节（544.50 MiB）；首次 `svn update` 增加同量网络流量，工作副本加 SVN pristine 通常接近 1.1 GiB。
- 压缩包升级后旧字节会永久留在 SVN 历史；HEAD 删除旧包不能回收服务端空间。因此逐组件独立存放，只更新实际变更的包，不合并成每次全变的大 zip。
- 把第三方包放入 SVN 属于再分发。上游归档中的 LICENSE/NOTICE 必须保留；MySQL、Temurin、Redis-Windows 等对外分发前需人工复核许可证义务。不得加入需要账号授权的商业安装包。
- SVN 包和清单同仓不能防止有提交权限者同时替换；运行时仍以代码中来自上游的固定 checksum/digest 为执行闸，更新必须经 review。

## 5. 更新流程

维护者或 AI 不得在启动时追逐 `latest`。更新按以下顺序进行：

1. 查看官方 release/security 信息，确认目标版本与许可证。
2. 从官方源下载到 staging；用上游独立发布的 checksum、签名或 digest 核验，不能只对下载物自算哈希后当权威。
3. 临时解包并运行版本 probe；确认 x64 Windows 布局仍满足脚本预期。
4. 同步更新 `local_infra.ps1` 或 `pwsh_bootstrap.pin` 的版本、URL、SHA256 和来源说明。
5. 替换 SVN `installers/localinfra` 中对应旧文件；Git 仍不纳管 payload。
6. 运行安装包契约、PowerShell 自举、localinfra 回归；在 SVN 完整包形态验证零公网，在 Git 空包形态验证缺包联网，并验证旧/缺 marker 自动重备、失败保留旧 dist/data、运行中目录不被覆盖。MySQL/Redis/Kafka/JRE/Envoy 仍活跃时先走本工作区完整停止链，mkcert 与 portable pwsh 的目录锁冲突只报错，不凭 PID 猜测或强杀。
7. 精确列路径提交，禁止把 `run/localinfra` 的 data/dist/logs/cache 一并加入。

## 6. 验收标准

- 新 SVN 工作副本不安装 Docker/PowerShell/数据库服务即可双击免 Docker 入口；7 包均从仓库目录命中且通过 SHA256。
- Git 检出只有说明文件；同一入口可按固定 URL 正常下载缺失包。
- 部分目录逐文件回退；坏同名包 fail-closed；Envoy 本地 layer 存在时不会先访问 Docker Hub token。
- 当前包 marker 匹配时复用 dist；包 pin 变化或旧机器没有 marker 时自动 staging 重备。任何准备/提升失败都不把半成品标成 current，也不删除仍可用的旧目录。
- 显式 `PANDORA_LOCALINFRA_MIRROR` 继续优先，不修改系统 PATH、注册表或 Windows 服务。
