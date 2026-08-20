# 免 Docker 本地安装包目录

这个目录是策划机一键启动的**只读第三方包镜像**：

- Git 仓库只跟踪本说明，不保存二进制；空目录/缺少某个固定文件时，脚本从官方网络源下载。
- Pandora-Server SVN 在同一路径跟踪当前固定版本的 7 个真实便携包；`svn update` 后不再需要使用者手工下载或安装 PowerShell、MySQL、Redis、Kafka、JRE、mkcert、Envoy。
- 本机 SVN 包先校验代码中固定的 SHA256，再直接只读解包，不会重复复制 544.5 MiB 到 cache，也不会修改这个目录；显式 UNC/移动盘镜像与公网包仍先落本机 cache。
- `PANDORA_LOCALINFRA_MIRROR` 是显式覆盖入口，设置后优先使用指定的本地/UNC 目录。

目录存在本身不代表离线完整。脚本按**当前固定文件名逐个判断**：文件不存在就联网；同名文件存在但 SHA256 不符会硬失败，不会偷偷绕过损坏的 SVN 包。

已解包目录也不是永久缓存：`run/localinfra/dist/<组件>/.pandora-package.sha256` 必须与当前固定包一致才会复用。维护者更新 pin/包或旧机器还没有 marker 时，脚本会先在同盘临时目录完整解包、校验，再原子替换；失败保留旧目录。若仍有进程占用旧目录，请先走本项目停止入口再启动，脚本不会强杀未知进程。不要手工创建或修改 marker；它只绑定固定归档身份，包真实性仍由解包前 SHA256 校验保证。系统 PATH 上已有的 `pwsh` 始终优先，也不属于这里的 portable dist 托管范围。

## 当前固定文件

| 组件 | 文件 |
|---|---|
| PowerShell 7 | `PowerShell-7.6.5-win-x64.zip` |
| MySQL | `mysql-8.4.6-winx64.zip` |
| Redis | `Redis-8.8.1-Windows-x64-msys2.zip` |
| Kafka | `kafka_2.13-3.9.1.tgz` |
| Temurin JRE | `OpenJDK21U-jre_x64_windows_hotspot_21.0.12_8.zip` |
| mkcert | `mkcert-v1.4.4-windows-amd64.exe` |
| Envoy Windows layer | `envoy-windows-v1.28.0-layer.tar.gz` |

版本、URL、上游校验依据和运行时 SHA256 的权威源仍是：

- `tools/scripts/local_infra.ps1`
- `tools/scripts/lib/pwsh_bootstrap.pin`

维护者更新版本时必须逐项核对上游 checksum/签名和许可证，临时解包验证版本，再同时更新 pin、测试、本文文件名和 SVN 二进制。不要在启动时自动追 `latest`。上游包内已有的 LICENSE/NOTICE 必须原样保留；当前 Redis-Windows、Envoy layer 与 mkcert 单文件 payload 未自带独立许可证材料，本次内部 SVN 便利分发不代表许可证审查已完成。对外再分发前必须补齐相应上游许可证/NOTICE，并由人工复核义务。
