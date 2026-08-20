# Pandora MySQL 运行态 profile 公共 seam。
#
# 这个文件只处理非敏感连接画像。密码永远不进入参数、返回值或 JSON；调用方只能传
# Windows Credential Manager / 当前用户 DPAPI 条目的引用和非秘密轮换版本。central-managed 的 workspace_id 由中心
# provisioner 分配并写入用户 identity，本模块只校验和消费，绝不从 IP/用户名自行生成。

function Get-PandoraMysqlProfileDefaultIdentityPath {
    $localAppData = [Environment]::GetFolderPath([Environment+SpecialFolder]::LocalApplicationData)
    if ([string]::IsNullOrWhiteSpace($localAppData)) {
        throw '无法定位 Windows LocalApplicationData，不能读取策划数据库 identity。'
    }
    return (Join-Path $localAppData 'Pandora/planner-db/identity.json')
}

function Get-PandoraMysqlProfileDefaultOutputPath([Parameter(Mandatory)][string]$ProjectRoot) {
    return (Join-Path $ProjectRoot 'run/localinfra/cfg/mysql-runtime-profile.json')
}

function Get-PandoraMysqlProfileMember {
    param(
        [Parameter(Mandatory)]$Value,
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$Context
    )
    if ($Value -is [Collections.IDictionary]) {
        if (-not $Value.Contains($Name)) { throw "$Context 缺少字段 $Name" }
        return $Value[$Name]
    }
    $property = $Value.PSObject.Properties[$Name]
    if (-not $property) { throw "$Context 缺少字段 $Name" }
    return $property.Value
}

function Assert-PandoraMysqlProfileExactMembers {
    param(
        [Parameter(Mandatory)]$Value,
        [Parameter(Mandatory)][string[]]$Expected,
        [Parameter(Mandatory)][string]$Context
    )
    $actual = if ($Value -is [Collections.IDictionary]) {
        @($Value.Keys | ForEach-Object { "$_" })
    } else {
        @($Value.PSObject.Properties.Name)
    }
    $unexpected = @($actual | Where-Object { $_ -notin $Expected })
    $missing = @($Expected | Where-Object { $_ -notin $actual })
    if ($unexpected.Count -gt 0 -or $missing.Count -gt 0) {
        throw "$Context 字段集合不合法；缺少=[$($missing -join ',')]，多余=[$($unexpected -join ',')]"
    }
}

function Set-PandoraPlannerDbIdentity {
    <#
      把中心 provisioner 返回的 workspace_id 首次绑定到当前 Windows 用户。
      这不是 ID 生成器：已有 identity 时只接受同值幂等重试，永不自动换 workspace。
    #>
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [string]$IdentityPath = ''
    )
    if ($WorkspaceId -cnotmatch '^[0-7][0-9a-hjkmnp-tv-z]{25}$') {
        throw 'workspace_id 必须是中心分配的 26 位小写 Crockford Base32'
    }
    if ([string]::IsNullOrWhiteSpace($IdentityPath)) {
        $IdentityPath = Get-PandoraMysqlProfileDefaultIdentityPath
    }
    $identityFull = [IO.Path]::GetFullPath($IdentityPath)
    if (Test-Path -LiteralPath $identityFull -PathType Leaf) {
        $existing = Get-PandoraPlannerDbIdentity -IdentityPath $identityFull
        if ($existing.workspace_id -cne $WorkspaceId) {
            throw "planner-db identity 已绑定 workspace=$($existing.workspace_id)，拒绝自动更换为 $WorkspaceId"
        }
        return $existing
    }

    $identityDir = Split-Path -Parent $identityFull
    New-Item -ItemType Directory -Force -Path $identityDir | Out-Null
    $temporary = Join-Path $identityDir ("identity.json.{0}.{1}.tmp" -f $PID, [guid]::NewGuid().ToString('N'))
    try {
        $identityJson = [pscustomobject][ordered]@{
            schema_version = 1
            workspace_id = $WorkspaceId
        } | ConvertTo-Json
        [IO.File]::WriteAllText($temporary, $identityJson, [Text.UTF8Encoding]::new($false))
        try {
            [IO.File]::Move($temporary, $identityFull, $false)
        } catch [IO.IOException] {
            # 两个 enrollment 调用并发时，由 CreateNew/rename 的赢家决定；输家只允许同值收敛。
            if (-not (Test-Path -LiteralPath $identityFull -PathType Leaf)) { throw }
            $winner = Get-PandoraPlannerDbIdentity -IdentityPath $identityFull
            if ($winner.workspace_id -cne $WorkspaceId) {
                throw "并发 enrollment 已绑定 workspace=$($winner.workspace_id)，拒绝更换为 $WorkspaceId"
            }
        }
    } finally {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
    }
    return (Get-PandoraPlannerDbIdentity -IdentityPath $identityFull)
}

function Get-PandoraPlannerDbIdentity {
    param([string]$IdentityPath = '')
    if ([string]::IsNullOrWhiteSpace($IdentityPath)) {
        $IdentityPath = Get-PandoraMysqlProfileDefaultIdentityPath
    }
    if (-not (Test-Path -LiteralPath $IdentityPath -PathType Leaf)) {
        throw "尚未完成中心 MySQL workspace enrollment，缺少 identity:$IdentityPath"
    }
    try {
        $identity = Get-Content -LiteralPath $IdentityPath -Raw -Encoding utf8 | ConvertFrom-Json
        Assert-PandoraMysqlProfileExactMembers $identity @('schema_version', 'workspace_id') 'planner-db identity'
        if ($identity.schema_version -isnot [long] -and $identity.schema_version -isnot [int]) {
            throw 'schema_version 类型非法'
        }
        if ([int64]$identity.schema_version -ne 1) { throw "不支持 schema_version=$($identity.schema_version)" }
        $workspaceId = "$($identity.workspace_id)"
        if ($workspaceId -cnotmatch '^[0-7][0-9a-hjkmnp-tv-z]{25}$') {
            throw 'workspace_id 必须是中心分配的 26 位小写 Crockford Base32'
        }
        return [pscustomobject][ordered]@{
            schema_version = 1
            workspace_id = $workspaceId
            path = [IO.Path]::GetFullPath($IdentityPath)
        }
    } catch {
        throw "planner-db identity 损坏，拒绝猜测或更换 workspace:$IdentityPath。详情:$($_.Exception.Message)"
    }
}

function Get-PandoraMysqlMigrationSets([Parameter(Mandatory)][string]$ProjectRoot) {
    $migrationsRoot = Join-Path $ProjectRoot 'tools/migrate/migrations'
    if (-not (Test-Path -LiteralPath $migrationsRoot -PathType Container)) {
        throw "找不到 MySQL migration sets:$migrationsRoot"
    }
    $sets = @(Get-ChildItem -LiteralPath $migrationsRoot -Directory -Force |
        Select-Object -ExpandProperty Name |
        Sort-Object -Unique)
    if ($sets.Count -eq 0) { throw "MySQL migration sets 为空:$migrationsRoot" }
    foreach ($set in $sets) {
        if ($set -cnotmatch '^pandora_[a-z0-9_]+$' -or $set.Length -gt 35) {
            throw "migration set 名称不合法:$set"
        }
    }
    return $sets
}

function ConvertTo-PandoraMysqlProfileEndpoint {
    param(
        [Parameter(Mandatory)]$Endpoint,
        [Parameter(Mandatory)][ValidateSet('local-owned', 'central-managed')][string]$Mode
    )
    Assert-PandoraMysqlProfileExactMembers $Endpoint @('host', 'port', 'tls_server_name', 'ca_file') 'endpoint'
    $endpointHost = "$(Get-PandoraMysqlProfileMember $Endpoint 'host' 'endpoint')".Trim().ToLowerInvariant()
    $portValue = Get-PandoraMysqlProfileMember $Endpoint 'port' 'endpoint'
    $port = 0
    $tlsServerName = "$(Get-PandoraMysqlProfileMember $Endpoint 'tls_server_name' 'endpoint')".Trim().ToLowerInvariant()
    $caFile = "$(Get-PandoraMysqlProfileMember $Endpoint 'ca_file' 'endpoint')".Trim()
    if ($endpointHost -cnotmatch '^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$') { throw "endpoint.host 不合法:$endpointHost" }
    if (-not [int]::TryParse("$portValue", [ref]$port) -or $port -lt 1 -or $port -gt 65535) {
        throw "endpoint.port 不合法:$portValue"
    }
    if ($Mode -ceq 'central-managed') {
        $parsedIp = $null
        if ([Net.IPAddress]::TryParse($endpointHost, [ref]$parsedIp)) {
            throw 'central-managed endpoint.host 必须是稳定 DNS，拒绝裸 IP 地址'
        }
        if ($tlsServerName -cnotmatch '^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$') {
            throw 'central-managed 必须设置合法 tls_server_name'
        }
        if ([string]::IsNullOrWhiteSpace($caFile) -or -not [IO.Path]::IsPathFullyQualified($caFile)) {
            throw 'central-managed 必须设置绝对 ca_file，拒绝明文或工作目录漂移'
        }
        if ($tlsServerName -cne $endpointHost) {
            throw 'central-managed 要求 endpoint.host 与证书 tls_server_name 完全一致，拒绝分离 override'
        }
        $caFile = [IO.Path]::GetFullPath($caFile)
        if (-not (Test-Path -LiteralPath $caFile -PathType Leaf)) {
            throw "central-managed CA 文件不存在:$caFile"
        }
        Get-PandoraCentralCaSha256 -CaFile $caFile | Out-Null
    } else {
        if ($endpointHost -cne '127.0.0.1') {
            throw 'local-owned 只允许连接已验证的 127.0.0.1 本机实例'
        }
        if (-not [string]::IsNullOrEmpty($tlsServerName) -or -not [string]::IsNullOrEmpty($caFile)) {
            throw 'local-owned profile 的 TLS 字段必须为空；本机归属由 PID/exe/my.ini seam 证明'
        }
    }
    return [pscustomobject][ordered]@{
        host = $endpointHost
        port = $port
        tls_server_name = $tlsServerName
        ca_file = $caFile
    }
}

function Get-PandoraCentralCaSha256 {
    param([Parameter(Mandatory)][string]$CaFile)
    $full = [IO.Path]::GetFullPath($CaFile)
    $info = Get-Item -LiteralPath $full -Force -ErrorAction Stop
    if ($info -isnot [IO.FileInfo] -or ($info.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "central-managed CA 必须是普通文件，拒绝目录/重解析点:$full"
    }
    if ($info.Length -lt 1 -or $info.Length -gt 1MB) {
        throw "central-managed CA 文件大小必须在 1 B..1 MiB:$full"
    }
    $stream = $null
    try {
        $stream = [IO.File]::Open($full, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read)
        return 'sha256:' + [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($stream)).ToLowerInvariant()
    } finally {
        if ($stream) { $stream.Dispose() }
    }
}

function ConvertTo-PandoraMysqlCredentialReference {
    param(
        [Parameter(Mandatory)]$CredentialRef,
        [Parameter(Mandatory)][ValidateSet('local-owned', 'central-managed')][string]$Mode
    )
    Assert-PandoraMysqlProfileExactMembers $CredentialRef @('provider', 'target', 'version') 'credential_ref'
    $provider = "$(Get-PandoraMysqlProfileMember $CredentialRef 'provider' 'credential_ref')".Trim()
    $target = "$(Get-PandoraMysqlProfileMember $CredentialRef 'target' 'credential_ref')".Trim()
    $versionValue = Get-PandoraMysqlProfileMember $CredentialRef 'version' 'credential_ref'
    if ($provider -cnotin @('windows-credential-manager', 'dpapi-current-user')) {
        throw 'credential_ref.provider 只允许 windows-credential-manager 或 dpapi-current-user'
    }
    if ([string]::IsNullOrWhiteSpace($target) -or $target.Length -gt 256 -or $target -match '[\x00-\x1f\x7f]') {
        throw 'credential_ref.target 不合法'
    }
    if (($versionValue -isnot [int] -and $versionValue -isnot [long]) -or
        [int64]$versionValue -lt 1 -or [int64]$versionValue -gt [int]::MaxValue) {
        throw 'credential_ref.version 必须是正整数'
    }
    return [pscustomobject][ordered]@{
        provider = $provider
        target = $target
        version = [int64]$versionValue
    }
}

function Get-PandoraMysqlProfileFingerprint([Parameter(Mandatory)]$ProfileWithoutFingerprint) {
    $mode = "$(Get-PandoraMysqlProfileMember $ProfileWithoutFingerprint 'mode' 'profile fingerprint input')"
    $fingerprintInput = [ordered]@{
        schema_version = Get-PandoraMysqlProfileMember $ProfileWithoutFingerprint 'schema_version' 'profile fingerprint input'
        mode = $mode
        workspace_id = Get-PandoraMysqlProfileMember $ProfileWithoutFingerprint 'workspace_id' 'profile fingerprint input'
        display_name = Get-PandoraMysqlProfileMember $ProfileWithoutFingerprint 'display_name' 'profile fingerprint input'
        endpoint = Get-PandoraMysqlProfileMember $ProfileWithoutFingerprint 'endpoint' 'profile fingerprint input'
        credential_ref = Get-PandoraMysqlProfileMember $ProfileWithoutFingerprint 'credential_ref' 'profile fingerprint input'
        databases = Get-PandoraMysqlProfileMember $ProfileWithoutFingerprint 'databases' 'profile fingerprint input'
    }
    if ($mode -ceq 'central-managed') {
        # 路径不变的 CA 轮换也必须使指纹漂移，禁止新旧信任根进程混跑。
        $fingerprintInput['ca_sha256'] = Get-PandoraCentralCaSha256 -CaFile "$($fingerprintInput.endpoint.ca_file)"
    }
    $canonicalJson = $fingerprintInput | ConvertTo-Json -Depth 8 -Compress
    $bytes = [Text.Encoding]::UTF8.GetBytes($canonicalJson)
    $hash = [Security.Cryptography.SHA256]::HashData($bytes)
    return 'sha256:' + [Convert]::ToHexString($hash).ToLowerInvariant()
}

function Get-PandoraMysqlRuntimeProfile {
    <# 读取并重新验证运行态 profile；任何未知字段、identity 漂移或映射串库都 fail-closed。 #>
    param(
        [Parameter(Mandatory)][string]$ProjectRoot,
        [string]$ProfilePath = '',
        [string]$IdentityPath = ''
    )
    $projectRootFull = [IO.Path]::GetFullPath($ProjectRoot)
    if ([string]::IsNullOrWhiteSpace($ProfilePath)) {
        $ProfilePath = Get-PandoraMysqlProfileDefaultOutputPath -ProjectRoot $projectRootFull
    }
    $profileFull = [IO.Path]::GetFullPath($ProfilePath)
    if (-not (Test-Path -LiteralPath $profileFull -PathType Leaf)) {
        throw "MySQL 运行态 profile 不存在:$profileFull"
    }

    try {
        $raw = Get-Content -LiteralPath $profileFull -Raw -Encoding utf8 | ConvertFrom-Json
        Assert-PandoraMysqlProfileExactMembers $raw @(
            'schema_version', 'mode', 'workspace_id', 'display_name', 'endpoint',
            'credential_ref', 'databases', 'fingerprint'
        ) 'mysql runtime profile'
        if (($raw.schema_version -isnot [long] -and $raw.schema_version -isnot [int]) -or
            [int64]$raw.schema_version -ne 1) {
            throw "不支持 schema_version=$($raw.schema_version)"
        }
        $mode = "$($raw.mode)"
        if ($mode -cnotin @('local-owned', 'central-managed')) { throw "mode 不合法:$mode" }
        $workspaceId = "$($raw.workspace_id)"
        if ($mode -ceq 'central-managed') {
            if ($workspaceId -cnotmatch '^[0-7][0-9a-hjkmnp-tv-z]{25}$') {
                throw 'central-managed workspace_id 不合法'
            }
            $identity = Get-PandoraPlannerDbIdentity -IdentityPath $IdentityPath
            if ($identity.workspace_id -cne $workspaceId) {
                throw "profile workspace=$workspaceId 与长期 identity=$($identity.workspace_id) 不一致"
            }
        } elseif ($workspaceId -cne '') {
            throw 'local-owned workspace_id 必须为空'
        }

        $displayName = "$($raw.display_name)"
        if ([string]::IsNullOrWhiteSpace($displayName) -or $displayName.Length -gt 129 -or
            $displayName -match '[\x00-\x1f\x7f/]' -or @($displayName -split '\\').Count -ne 2) {
            throw 'display_name 不合法'
        }
        $normalizedEndpoint = ConvertTo-PandoraMysqlProfileEndpoint -Endpoint $raw.endpoint -Mode $mode
        $normalizedCredentialRef = ConvertTo-PandoraMysqlCredentialReference -CredentialRef $raw.credential_ref -Mode $mode
        $migrationSets = @(Get-PandoraMysqlMigrationSets -ProjectRoot $projectRootFull)
        Assert-PandoraMysqlProfileExactMembers $raw.databases $migrationSets 'databases'
        $databases = [ordered]@{}
        foreach ($migrationSet in $migrationSets) {
            $actualPhysical = "$(Get-PandoraMysqlProfileMember $raw.databases $migrationSet 'databases')"
            $expectedPhysical = if ($mode -ceq 'central-managed') {
                "${migrationSet}_w_$workspaceId"
            } else { $migrationSet }
            if ($actualPhysical -cne $expectedPhysical) {
                throw "database 映射不合法:$migrationSet=$actualPhysical，期望=$expectedPhysical"
            }
            $databases[$migrationSet] = $actualPhysical
        }

        $unsigned = [pscustomobject][ordered]@{
            schema_version = 1
            mode = $mode
            workspace_id = $workspaceId
            display_name = $displayName
            endpoint = $normalizedEndpoint
            credential_ref = $normalizedCredentialRef
            databases = [pscustomobject]$databases
        }
        $expectedFingerprint = Get-PandoraMysqlProfileFingerprint $unsigned
        $actualFingerprint = "$($raw.fingerprint)"
        if ($actualFingerprint -cnotmatch '^sha256:[0-9a-f]{64}$' -or $actualFingerprint -cne $expectedFingerprint) {
            throw 'fingerprint 校验失败'
        }
        return [pscustomobject][ordered]@{
            schema_version = 1
            mode = $mode
            workspace_id = $workspaceId
            display_name = $displayName
            endpoint = $normalizedEndpoint
            credential_ref = $normalizedCredentialRef
            databases = [pscustomobject]$databases
            fingerprint = $actualFingerprint
        }
    } catch {
        throw "MySQL 运行态 profile 损坏，拒绝生成 DSN:$profileFull。详情:$($_.Exception.Message)"
    }
}

function Publish-PandoraMysqlRuntimeProfile {
    param(
        [Parameter(Mandatory)][string]$ProjectRoot,
        [Parameter(Mandatory)][ValidateSet('local-owned', 'central-managed')][string]$Mode,
        [Parameter(Mandatory)]$Endpoint,
        [Parameter(Mandatory)]$CredentialRef,
        [string]$IdentityPath = '',
        [string]$OutputPath = '',
        [string]$ComputerName = $env:COMPUTERNAME,
        [string]$UserName = $env:USERNAME
    )
    $projectRootFull = [IO.Path]::GetFullPath($ProjectRoot)
    $workspaceId = if ($Mode -ceq 'central-managed') {
        (Get-PandoraPlannerDbIdentity -IdentityPath $IdentityPath).workspace_id
    } else { '' }
    foreach ($labelPart in @($ComputerName, $UserName)) {
        if ([string]::IsNullOrWhiteSpace($labelPart) -or $labelPart.Length -gt 64 -or $labelPart -match '[\x00-\x1f\x7f\\/]') {
            throw '电脑名或用户名不能用于安全显示标签'
        }
    }
    $displayName = "$($ComputerName.Trim())\$($UserName.Trim())"
    $normalizedEndpoint = ConvertTo-PandoraMysqlProfileEndpoint -Endpoint $Endpoint -Mode $Mode
    $normalizedCredentialRef = ConvertTo-PandoraMysqlCredentialReference -CredentialRef $CredentialRef -Mode $Mode
    $databases = [ordered]@{}
    foreach ($migrationSet in @(Get-PandoraMysqlMigrationSets -ProjectRoot $projectRootFull)) {
        $physical = if ($Mode -ceq 'central-managed') { "${migrationSet}_w_$workspaceId" } else { $migrationSet }
        if ($physical.Length -gt 64) { throw "workspace 物理库名超过 MySQL 64 字符:$physical" }
        $databases[$migrationSet] = $physical
    }

    $unsigned = [pscustomobject][ordered]@{
        schema_version = 1
        mode = $Mode
        workspace_id = $workspaceId
        display_name = $displayName
        endpoint = $normalizedEndpoint
        credential_ref = $normalizedCredentialRef
        databases = [pscustomobject]$databases
    }
    $profile = [pscustomobject][ordered]@{
        schema_version = $unsigned.schema_version
        mode = $unsigned.mode
        workspace_id = $unsigned.workspace_id
        display_name = $unsigned.display_name
        endpoint = $unsigned.endpoint
        credential_ref = $unsigned.credential_ref
        databases = $unsigned.databases
        fingerprint = Get-PandoraMysqlProfileFingerprint $unsigned
    }

    if ([string]::IsNullOrWhiteSpace($OutputPath)) {
        $OutputPath = Get-PandoraMysqlProfileDefaultOutputPath -ProjectRoot $projectRootFull
    }
    $outputFull = [IO.Path]::GetFullPath($OutputPath)
    $outputDir = Split-Path -Parent $outputFull
    New-Item -ItemType Directory -Force -Path $outputDir | Out-Null
    $temporary = Join-Path $outputDir ("mysql-runtime-profile.json.{0}.{1}.tmp" -f $PID, [guid]::NewGuid().ToString('N'))
    try {
        $json = $profile | ConvertTo-Json -Depth 8
        [IO.File]::WriteAllText($temporary, $json, [Text.UTF8Encoding]::new($false))
        [IO.File]::Move($temporary, $outputFull, $true)
    } finally {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
    }
    return $profile
}
