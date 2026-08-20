[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$MainEnvoy = Join-Path $ProjectRoot 'deploy/envoy/envoy.yaml'
$K8sDSEnvoy = Join-Path $ProjectRoot 'deploy/k8s/agones/16-ds-envoy.yaml'
$Method = '/pandora.login.v1.LoginService/ResolvePlayerNosForDS'

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

$main = Get-Content -LiteralPath $MainEnvoy -Raw
$dsStart = $main.IndexOf('- name: pandora_ds_listener', [StringComparison]::Ordinal)
Assert-True ($dsStart -gt 0) '主 Envoy 缺 pandora_ds_listener，无法区分客户端面与 DS 面'
$client = $main.Substring(0, $dsStart)
$mainDS = $main.Substring($dsStart)
$k8sDS = Get-Content -LiteralPath $K8sDSEnvoy -Raw

$escaped = [regex]::Escape($Method)
$clientDeny = [regex]::Matches($client,
    '(?ms)^\s*-\s+match:\s*\r?\n\s*path:\s*"' + $escaped + '"\s*\r?\n\s*direct_response:\s*\r?\n\s*status:\s*403\b')
Assert-True ($clientDeny.Count -eq 1) '客户端 :8443 必须且只能有一条 ResolvePlayerNosForDS exact 403'
$loginCatchAll = $client.LastIndexOf('prefix: "/pandora.login.v1.LoginService/"', [StringComparison]::Ordinal)
Assert-True ($loginCatchAll -ge 0) '客户端面缺 LoginService catch-all'
Assert-True ($clientDeny[0].Index -lt $loginCatchAll) 'ResolvePlayerNosForDS exact 403 必须位于 LoginService catch-all 之前'

$mainAllow = [regex]::Matches($mainDS,
    '(?ms)^\s*-\s+match:\s*\r?\n\s*path:\s*"' + $escaped + '"\s*\r?\n\s*route:\s*\r?\n\s*cluster:\s*login_cluster\b')
Assert-True ($mainAllow.Count -eq 1) '主 Envoy :8444 必须且只能 exact allow ResolvePlayerNosForDS 到 login_cluster'

$k8sAllow = [regex]::Matches($k8sDS,
    '(?m)^\s*-\s+match:\s*\{\s*path:\s*"' + $escaped + '"\s*\}\s*\r?\n\s*route:\s*\{\s*cluster:\s*login_cluster\b')
Assert-True ($k8sAllow.Count -eq 1) 'Agones DS Envoy 必须且只能 exact allow ResolvePlayerNosForDS 到 login_cluster'

Write-Output '[PASS] envoy_login_ds_player_no_contract_test:客户端 exact 403，主/Agones DS listener exact allow'
