# Pandora 开发环境基础设施一键启动
#
# 用法:
#   pwsh tools/scripts/dev_up.ps1
#   pwsh tools/scripts/dev_up.ps1 -Pull   # 拉最新镜像后启动
#
# 启动 docker-compose 全套(MySQL/Redis/Kafka/etcd/Prometheus/Grafana),等所有服务 healthy。

param(
    [switch]$Pull
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path "$PSScriptRoot/../.."
$ComposeFile = "$ProjectRoot/deploy/docker-compose.dev.yml"
$EnvFile     = "$ProjectRoot/deploy/env/dev.env"

# Envoy dev TLS 证书校验 / 自愈(缺失或损坏的 cert.pem/key.pem 会让 Envoy 启动直接退出)。
. "$PSScriptRoot/envoy_cert.ps1"
# 本机 dev.env 自举:被 git 忽略 → 新机器必然缺,而缺了 --env-file 会直接失败。
. "$PSScriptRoot/dev_env_file.ps1"
. "$PSScriptRoot/lib/local_infra_state.ps1"

Enter-PandoraOrchestrationLock -ProjectRoot $ProjectRoot -Operation 'Docker 基础设施启动'
try {
Write-Host "===== Pandora dev infra up =====" -ForegroundColor Cyan
Write-Host "Project:      $ProjectRoot"
Write-Host "Compose file: $ComposeFile"
Write-Host "Env file:     $EnvFile"
Write-Host ""

if (-not (Test-Path $ComposeFile)) {
    Write-Host "[ERR] compose file not found: $ComposeFile" -ForegroundColor Red
    exit 1
}
# dev.env 命中 .gitignore 的 `*.env`,受版本控制的只有 dev.env.example —— 任何新机器 / 新克隆
# 第一次跑都必然没有它。原来只打一行「请先执行 Copy-Item ...」就 exit 1,等于把「一键启动」
# 变成两步,而策划多半只看到最后一行「基础设施启动失败」。现在按 example 自动初始化(里面
# 全是 dev 级默认值,没有真 secret);连 example 都缺才是工作区不完整,那时才硬失败。
try {
    Confirm-DevEnvFile -ProjectRoot $ProjectRoot
} catch {
    Write-Host "[ERR] $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# 先 validate
Write-Host "[1/4] Validating compose file..." -ForegroundColor Yellow
docker compose -f $ComposeFile --env-file $EnvFile config --quiet
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERR] compose file invalid" -ForegroundColor Red
    exit 1
}

# Envoy 起 TLS 监听器前,确保本机 dev 证书存在且有效(缺失/损坏自动用 mkcert 补齐)。
Write-Host "[1.5/4] Checking Envoy dev TLS cert..." -ForegroundColor Yellow
try {
    # 先确保本机 mkcert 用的是「全队共享 dev CA」:私钥就位则自动装,否则仅提示、退回独立 CA。
    Confirm-SharedDevCa -ProjectRoot $ProjectRoot | Out-Null
    Confirm-EnvoyDevCert -EnvoyDir (Join-Path $ProjectRoot 'deploy/envoy')
} catch {
    Write-Host "[ERR] $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

if ($Pull) {
    Write-Host "[2/4] Pulling latest images..." -ForegroundColor Yellow
    docker compose -f $ComposeFile --env-file $EnvFile pull
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERR] docker pull failed" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "[2/4] Skipping pull (use -Pull to refresh)" -ForegroundColor Yellow
}

Write-Host "[3/4] Starting containers..." -ForegroundColor Yellow
docker compose -f $ComposeFile --env-file $EnvFile up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERR] compose up failed" -ForegroundColor Red
    exit 1
}
# Envoy/Grafana 都从只读挂载加载静态配置；仅文件变化时 compose 不会重启已有容器。
# 每次启动显式重建两者，保证路由白名单与 Grafana alerting provisioning 实际生效。
docker compose -f $ComposeFile --env-file $EnvFile up -d --force-recreate --no-deps envoy grafana
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERR] envoy/grafana recreate failed" -ForegroundColor Red
    exit 1
}

Write-Host "[4/4] Waiting for healthy..." -ForegroundColor Yellow
$timeout = 120  # 秒
$elapsed = 0
$step = 5
function Get-ComposePsStatus {
    $lines = @(docker compose -f $ComposeFile --env-file $EnvFile ps --format '{{.Name}}	{{.State}}	{{.Health}}')
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERR] docker compose ps failed" -ForegroundColor Red
        exit 1
    }

    if ($lines.Count -eq 0) { return @() }

    $items = @()
    foreach ($line in $lines) {
        if (-not $line -or $line.Trim() -eq "") { continue }

        $parts = $line -split "`t", 3
        if ($parts.Count -lt 2) {
            Write-Host "[ERR] docker compose ps output unexpected: $line" -ForegroundColor Red
            exit 1
        }

        $items += [pscustomobject]@{
            Name   = $parts[0]
            State  = $parts[1]
            Health = if ($parts.Count -ge 3) { $parts[2] } else { "" }
        }
    }
    return $items
}

while ($elapsed -lt $timeout) {
    $unhealthy = Get-ComposePsStatus |
        Where-Object {
            ($_.Health -and $_.Health -ne "healthy") -or
            (-not $_.Health -and $_.State -ne "running")
        } |
        Select-Object -ExpandProperty Name
    if ($null -eq $unhealthy -or $unhealthy.Count -eq 0) { break }
    Start-Sleep -Seconds $step
    $elapsed += $step
    Write-Host "  ${elapsed}s waiting: $($unhealthy -join ', ')"
}

if ($elapsed -ge $timeout) {
    Write-Host "[ERR] containers not ready after ${timeout}s" -ForegroundColor Red
    docker compose -f $ComposeFile --env-file $EnvFile ps
    exit 1
}

Write-Host ""
Write-Host "===== 服务连接信息 =====" -ForegroundColor Green
Write-Host "MySQL       localhost:3307   user=pandora pass=pandora_dev_pwd"
Write-Host "Redis       localhost:6380"
Write-Host "Kafka       localhost:9093   (host网络可达)"
Write-Host "etcd        localhost:2380"
Write-Host "Prometheus  http://localhost:9091"
Write-Host "Grafana     http://localhost:3001  user=admin pass=pandora_dev_admin"
Write-Host "ntfy        http://localhost:$(if ($env:PANDORA_NTFY_BIND_PORT) { $env:PANDORA_NTFY_BIND_PORT } else { '8090' })  topic=pandora-alerts"
Write-Host ""
Write-Host "===== 状态 =====" -ForegroundColor Green
docker compose -f $ComposeFile --env-file $EnvFile ps
} finally {
    Exit-PandoraOrchestrationLock
}
