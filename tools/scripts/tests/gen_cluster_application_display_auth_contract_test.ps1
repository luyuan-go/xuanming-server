# friend/guild application display projection production wiring contract.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
foreach ($name in @(
        'PANDORA_FRIEND_PLAYER_NAME_RESOLVE_AUTH_SECRET',
        'PANDORA_FRIEND_PLAYER_NO_RESOLVE_AUTH_SECRET',
        'PANDORA_GUILD_PLAYER_NAME_RESOLVE_AUTH_SECRET',
        'PANDORA_GUILD_PLAYER_NO_RESOLVE_AUTH_SECRET')) {
    [Environment]::SetEnvironmentVariable($name, $null, 'Process')
}
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$Generator = Join-Path $ProjectRoot 'tools/scripts/gen_cluster_config.ps1'
$OutDir = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-application-display-' + [guid]::NewGuid().ToString('N'))
$RejectDir = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-gen-application-display-reject-' + [guid]::NewGuid().ToString('N'))

$TeamName = 'prod-team-player-name-key-0123456789abcdef-21'
$TeamNo = 'prod-team-player-no-key-0123456789abcdef-22'
$FriendName = 'prod-friend-player-name-key-0123456789abcdef-23'
$FriendNo = 'prod-friend-player-no-key-0123456789abcdef-24'
$GuildName = 'prod-guild-player-name-key-0123456789abcdef-25'
$GuildNo = 'prod-guild-player-no-key-0123456789abcdef-26'

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

function Get-YamlDirectString([string]$Yaml, [string]$Section, [string]$Child) {
    $pattern = '(?m)^' + [regex]::Escape($Section) + ':[ \t]*(?:#.*)?\r?\n(?:(?:[ \t]+.*|[ \t]*#.*|[ \t]*)\r?\n)*?[ \t]+' +
        [regex]::Escape($Child) + '[ \t]*:[ \t]*"((?:\\.|[^"])*)"'
    $m = [regex]::Match($Yaml, $pattern)
    if (-not $m.Success) { throw "ASSERT FAILED:找不到 $Section.$Child" }
    return $m.Groups[1].Value
}

function Get-ProdArgs([string]$TargetDir) {
    return @(
        '-OutDir', $TargetDir, '-AllocatorMode', 'agones', '-Prod',
        '-Secret', 'prod-player-jwt-key-0123456789abcdef-01',
        '-DsSecret', 'prod-ds-callback-key-0123456789abcdef-02',
        '-PlacementAccountBootstrapSecret', 'prod-placement-bootstrap-0123456789abcdef-03',
        '-PlacementMatchStartSecret', 'prod-placement-match-0123456789abcdef-04',
        '-PlacementBattleExitSecret', 'prod-placement-exit-0123456789abcdef-05',
        '-PlacementHubTransferSecret', 'prod-placement-transfer-0123456789abcdef-06',
        '-PlacementBattleDepartureSecret', 'prod-placement-departure-0123456789abcdef-07',
        '-MatchResumeAuthSecret', 'prod-match-resume-0123456789abcdef-08',
        '-AllocationAbortAuthSecret', 'prod-allocation-abort-0123456789abcdef-09',
        '-TeamResumeAuthSecret', 'prod-team-resume-0123456789abcdef-10',
        '-PlayerNameResolveAuthSecret', $TeamName,
        '-PlayerNoResolveAuthSecret', $TeamNo,
        '-FriendPlayerNameResolveAuthSecret', $FriendName,
        '-FriendPlayerNoResolveAuthSecret', $FriendNo,
        '-GuildPlayerNameResolveAuthSecret', $GuildName,
        '-GuildPlayerNoResolveAuthSecret', $GuildNo,
        '-DsAuthMode', 'enforce', '-DsAuthorityMode', 'redis',
        '-DsFenceEtcdEndpoints', 'https://etcd.pandora.svc:2379',
        '-DsFenceKeysetRevision', 'pandora-ds-auth-v2-prod-r1',
        '-DsTicketActiveKid', ('P' * 43), '-DsTicketKeysetRevision', '9',
        '-OwnerStoreDsn', 'prod_owner:prod-owner-pwd-11@tcp(tidb.pandora.svc:4000)/pandora_owner?parseTime=true&loc=UTC',
        '-AccountStoreDsn', 'prod_login:prod-account-pwd-12@tcp(tidb.pandora.svc:4000)/pandora_account?parseTime=true&loc=UTC')
}

function Assert-ProdRejected([string]$Flag, [string]$Value, [string]$Reason) {
    $argsToReject = Get-ProdArgs $RejectDir
    $valueIndex = [Array]::IndexOf($argsToReject, $Flag) + 1
    if ($valueIndex -le 0) { throw "test bug:missing flag $Flag" }
    $argsToReject[$valueIndex] = $Value
    & pwsh -NoProfile -File $Generator @argsToReject *> $null
    Assert-True ($LASTEXITCODE -ne 0) $Reason
}

try {
    $prodArgs = Get-ProdArgs $OutDir
    $generatorOutput = & pwsh -NoProfile -File $Generator @prodArgs 2>&1
    if ($LASTEXITCODE -ne 0) { throw "generator failed(exit=$LASTEXITCODE):$($generatorOutput -join [Environment]::NewLine)" }

    $player = Get-Content -LiteralPath (Join-Path $OutDir 'player.yaml') -Raw
    $login = Get-Content -LiteralPath (Join-Path $OutDir 'login.yaml') -Raw
    $friend = Get-Content -LiteralPath (Join-Path $OutDir 'friend.yaml') -Raw
    $guild = Get-Content -LiteralPath (Join-Path $OutDir 'guild.yaml') -Raw
    Assert-True ((Get-YamlDirectString $player 'player' 'friend_player_name_resolve_auth_secret') -ceq $FriendName) 'player/friend name verifier key 未注入'
    Assert-True ((Get-YamlDirectString $friend 'friend' 'player_name_resolver_auth_secret') -ceq $FriendName) 'friend name signer key 未注入'
    Assert-True ((Get-YamlDirectString $login 'login' 'friend_player_no_resolve_auth_secret') -ceq $FriendNo) 'login/friend no verifier key 未注入'
    Assert-True ((Get-YamlDirectString $friend 'friend' 'player_no_resolver_auth_secret') -ceq $FriendNo) 'friend no signer key 未注入'
    Assert-True ((Get-YamlDirectString $player 'player' 'guild_player_name_resolve_auth_secret') -ceq $GuildName) 'player/guild name verifier key 未注入'
    Assert-True ((Get-YamlDirectString $guild 'guild' 'player_name_resolver_auth_secret') -ceq $GuildName) 'guild name signer key 未注入'
    Assert-True ((Get-YamlDirectString $login 'login' 'guild_player_no_resolve_auth_secret') -ceq $GuildNo) 'login/guild no verifier key 未注入'
    Assert-True ((Get-YamlDirectString $guild 'guild' 'player_no_resolver_auth_secret') -ceq $GuildNo) 'guild no signer key 未注入'
    Assert-True ((Get-YamlDirectString $friend 'friend' 'player_name_resolver_addr') -ceq 'player:20002') 'friend name address 未改写'
    Assert-True ((Get-YamlDirectString $friend 'friend' 'player_no_resolver_addr') -ceq 'login:20001') 'friend no address 未改写'
    Assert-True ((Get-YamlDirectString $guild 'guild' 'player_name_resolver_addr') -ceq 'player:20002') 'guild name address 未改写'
    Assert-True ((Get-YamlDirectString $guild 'guild' 'player_no_resolver_addr') -ceq 'login:20001') 'guild no address 未改写'

    Assert-ProdRejected '-FriendPlayerNameResolveAuthSecret' '' '-Prod 必须拒绝漏配 friend player-name key'
    Assert-ProdRejected '-FriendPlayerNoResolveAuthSecret' 'short-key' '-Prod 必须拒绝短 friend player-no key'
    Assert-ProdRejected '-GuildPlayerNameResolveAuthSecret' 'pandora-dev-guild-player-name-auth-key-v1!' `
        '-Prod 必须拒绝公开 guild player-name dev key'
    Assert-ProdRejected '-GuildPlayerNoResolveAuthSecret' "prod-guild-player-no-key-with-control`ninvalid" `
        '-Prod 必须拒绝含控制字符的 guild player-no key'
    Assert-ProdRejected '-FriendPlayerNameResolveAuthSecret' $TeamName 'friend 不得复用 team player-name key'
} finally {
    foreach ($dir in @($OutDir, $RejectDir)) {
        if (-not (Test-Path -LiteralPath $dir -PathType Container)) { continue }
        $resolved = [System.IO.Path]::GetFullPath($dir)
        $temp = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
        if (-not $resolved.StartsWith($temp, [StringComparison]::OrdinalIgnoreCase) -or
            (Split-Path -Leaf $resolved) -notmatch '^pandora-gen-application-display(?:-reject)?-[0-9a-f]{32}$') {
            throw "拒绝清理未验证测试目录:$resolved"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}

Write-Host 'gen_cluster_application_display_auth_contract_test: PASS' -ForegroundColor Green
