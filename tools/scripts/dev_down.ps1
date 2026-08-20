# Pandora 开发环境一键关闭
#
# 用法:
#   pwsh tools/scripts/dev_down.ps1            # 停容器,保留数据卷
#   pwsh tools/scripts/dev_down.ps1 -Volumes   # 停容器 + 删除所有数据卷(危险!)

param(
    [switch]$Volumes
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path "$PSScriptRoot/../.."
$ComposeFile = "$ProjectRoot/deploy/docker-compose.dev.yml"
$EnvFile     = "$ProjectRoot/deploy/env/dev.env"

# 停机也吃 --env-file:dev.env 缺失(新机器没跑过启动就先双击了停止)会让 compose 直接报错,
# 于是「一键停止」也失败。与 dev_up 用同一份自举逻辑。
. "$PSScriptRoot/dev_env_file.ps1"
. "$PSScriptRoot/lib/local_infra_state.ps1"

Enter-PandoraOrchestrationLock -ProjectRoot $ProjectRoot -Operation 'Docker 基础设施停止'
try {
Confirm-DevEnvFile -ProjectRoot $ProjectRoot
Write-Host "===== Pandora dev infra down =====" -ForegroundColor Cyan

if ($Volumes) {
    Write-Host "[!] -Volumes 已启用,所有持久化数据(mysql/redis/kafka/...)将被删除!" -ForegroundColor Red
    $confirm = Read-Host "继续吗?输入 yes 确认"
    if ($confirm -ne "yes") {
        Write-Host "已取消" -ForegroundColor Yellow
        exit 0
    }
    docker compose -f $ComposeFile --env-file $EnvFile down -v
} else {
    docker compose -f $ComposeFile --env-file $EnvFile down
}
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} finally {
    Exit-PandoraOrchestrationLock
}
