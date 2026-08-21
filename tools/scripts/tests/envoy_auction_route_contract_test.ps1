[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$EnvoyPath = Join-Path $ProjectRoot 'deploy/envoy/envoy.yaml'
$manifest = Get-Content -LiteralPath $EnvoyPath -Raw

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

$servicePrefix = '/pandora.auction.v1.AuctionService/'
$escapedPrefix = [regex]::Escape($servicePrefix)

# AuctionService 的玩家身份完全来自 JWT sub；漏掉 jwt_authn 规则会让路由虽通，
# 但上游拿不到 x-pandora-player-id，五个 RPC 都只会返回未授权。
$jwtPattern = '(?ms)^\s*-\s+match:\s*\r?\n\s*prefix:\s*"' + $escapedPrefix +
    '"\s*\r?\n\s*requires:\s*\r?\n\s*provider_name:\s*"pandora_session"'
Assert-True ([regex]::Matches($manifest, $jwtPattern).Count -eq 1) `
    'AuctionService 必须且只能有一条 pandora_session JWT 规则'

# 客户端面的 gRPC-Web 请求必须转发到独立 auction upstream。
$routePattern = '(?ms)^\s*-\s+match:\s*\r?\n\s*prefix:\s*"' + $escapedPrefix +
    '"\s*\r?\n\s*route:\s*\r?\n\s*cluster:\s*auction_cluster\s*\r?\n' +
    '\s*timeout:\s*15s\s*\r?\n\s*idle_timeout:\s*60s'
Assert-True ([regex]::Matches($manifest, $routePattern).Count -eq 1) `
    'AuctionService 必须且只能有一条指向 auction_cluster 的 15s unary 路由'

# auction 服务固定监听 20016；cluster 必须保持 h2c，供 Envoy 转发原生 gRPC。
$clusterPattern = '(?ms)^\s*-\s+name:\s*auction_cluster\s*\r?\n' +
    '.*?explicit_http_config:\s*\r?\n\s*http2_protocol_options:\s*\{\}\s*\r?\n' +
    '.*?cluster_name:\s*auction_cluster\s*\r?\n' +
    '.*?address:\s*host\.docker\.internal\s*\r?\n\s*port_value:\s*20016\s*(?=\r?\n\s*(?:#|\z))'
Assert-True ([regex]::Matches($manifest, $clusterPattern).Count -eq 1) `
    'auction_cluster 必须且只能有一个指向 host.docker.internal:20016 的 h2c upstream'

Write-Host 'envoy_auction_route_contract_test: PASS' -ForegroundColor Green
