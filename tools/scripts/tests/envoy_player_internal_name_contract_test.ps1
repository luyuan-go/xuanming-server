# envoy_player_internal_name_contract_test — Team→Player 内部名字解析不得进入任一对外 Envoy。
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$MainEnvoyPath = Join-Path $ProjectRoot 'deploy/envoy/envoy.yaml'
$DsEnvoyPath = Join-Path $ProjectRoot 'deploy/k8s/agones/16-ds-envoy.yaml'
$InternalMethod = '/pandora.player.v1.PlayerInternalService/ResolvePlayerNames'
$PublicMethod = '/pandora.player.v1.PlayerService/GetPlayerNames'

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

$main = Get-Content -LiteralPath $MainEnvoyPath -Raw
$ds = Get-Content -LiteralPath $DsEnvoyPath -Raw

Assert-True (-not $main.Contains($InternalMethod)) `
    'PlayerInternalService.ResolvePlayerNames 不得暴露在客户端或本地 DS Envoy'
Assert-True (-not $ds.Contains($InternalMethod)) `
    'PlayerInternalService.ResolvePlayerNames 不得暴露在集群 DS Envoy'

# 保留既有 DS 铭牌查询边界：客户端精确拒绝，DS 只放公共 DS-only 方法。
$clientDenyPattern = '(?s)path:\s*"' + [regex]::Escape($PublicMethod) + '".*?direct_response:\s*\{\s*status:\s*403'
Assert-True ([regex]::IsMatch($main, $clientDenyPattern)) `
    '客户端 listener 必须继续精确 403 PlayerService.GetPlayerNames'
Assert-True ($main.Contains("path: `"$PublicMethod`"")) `
    '本地 DS listener 必须继续精确放行 PlayerService.GetPlayerNames'
Assert-True ($ds.Contains("path: `"$PublicMethod`"")) `
    '集群 DS listener 必须继续精确放行 PlayerService.GetPlayerNames'

Write-Host 'envoy_player_internal_name_contract_test: PASS' -ForegroundColor Green
