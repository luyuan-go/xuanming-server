# Pandora 策划中心 MySQL enrollment / 当前用户 DPAPI 凭据 seam。
#
# 只在首次登记时把一次性 token 通过 HTTPS request body 交给中心 provisioner；token、
# runtime password、admin DSN 都不会进入命令行、日志、identity 或 runtime profile。

$profileLib = Join-Path $PSScriptRoot 'mysql_runtime_profile.ps1'
if (-not (Get-Command Publish-PandoraMysqlRuntimeProfile -ErrorAction SilentlyContinue)) {
    . $profileLib
}

function Assert-PandoraPlannerWorkspaceId([string]$WorkspaceId) {
    if ($WorkspaceId -cnotmatch '^[0-7][0-9a-hjkmnp-tv-z]{25}$') {
        throw 'workspace_id 必须是中心分配的 26 位小写 Crockford Base32'
    }
}

function Get-PandoraPlannerCentralMysqlConfig {
    param([Parameter(Mandatory)][string]$ConfigPath)
    $configFull = [IO.Path]::GetFullPath($ConfigPath)
    if (-not (Test-Path -LiteralPath $configFull -PathType Leaf)) {
        throw "找不到中心 MySQL bundle 配置:$configFull"
    }
    $info = Get-Item -LiteralPath $configFull
    if ($info.Length -gt 65536) { throw '中心 MySQL bundle 配置超过 64 KiB，拒绝读取' }
    try {
        $raw = Get-Content -LiteralPath $configFull -Raw -Encoding utf8 | ConvertFrom-Json
        Assert-PandoraMysqlProfileExactMembers $raw @('schema_version', 'provisioner_url', 'endpoint', 'ca_file') 'central mysql bundle'
        if (($raw.schema_version -isnot [int] -and $raw.schema_version -isnot [long]) -or [int64]$raw.schema_version -ne 1) {
            throw "不支持 schema_version=$($raw.schema_version)"
        }
        $urlText = "$($raw.provisioner_url)".Trim()
        $url = $null
        if (-not [Uri]::TryCreate($urlText, [UriKind]::Absolute, [ref]$url) -or
            $url.Scheme -cne 'https' -or -not [string]::IsNullOrEmpty($url.UserInfo) -or
            -not [string]::IsNullOrEmpty($url.Query) -or -not [string]::IsNullOrEmpty($url.Fragment) -or
            $url.AbsolutePath -cne '/v1/enroll') {
            throw 'provisioner_url 必须是无 userinfo/query/fragment 的固定 HTTPS /v1/enroll 地址'
        }
        Assert-PandoraMysqlProfileExactMembers $raw.endpoint @('host', 'port', 'tls_server_name') 'central mysql endpoint'
        $caName = "$($raw.ca_file)".Trim()
        if ([string]::IsNullOrWhiteSpace($caName) -or [IO.Path]::GetFileName($caName) -cne $caName) {
            throw 'ca_file 必须是与 central-mysql.json 同目录的单一文件名'
        }
        $caFull = [IO.Path]::GetFullPath((Join-Path (Split-Path -Parent $configFull) $caName))
        $endpoint = ConvertTo-PandoraMysqlProfileEndpoint -Mode central-managed -Endpoint ([ordered]@{
                host = $raw.endpoint.host
                port = $raw.endpoint.port
                tls_server_name = $raw.endpoint.tls_server_name
                ca_file = $caFull
            })
        return [pscustomobject][ordered]@{
            schema_version = 1
            provisioner_url = $url.AbsoluteUri
            endpoint = $endpoint
            ca_file = $caFull
            path = $configFull
        }
    } catch {
        throw "中心 MySQL bundle 配置非法:$configFull。详情:$($_.Exception.Message)"
    }
}

function Get-PandoraPlannerDbCredentialDefaultRoot {
    $localAppData = [Environment]::GetFolderPath([Environment+SpecialFolder]::LocalApplicationData)
    if ([string]::IsNullOrWhiteSpace($localAppData)) { throw '无法定位 Windows LocalApplicationData' }
    return (Join-Path $localAppData 'Pandora/planner-db/credentials')
}

function Get-PandoraPlannerDbCredentialPath {
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [string]$CredentialRoot = ''
    )
    Assert-PandoraPlannerWorkspaceId $WorkspaceId
    if ([string]::IsNullOrWhiteSpace($CredentialRoot)) {
        $CredentialRoot = Get-PandoraPlannerDbCredentialDefaultRoot
    }
    return [IO.Path]::GetFullPath((Join-Path $CredentialRoot "$WorkspaceId-app.json"))
}

function Get-PandoraPlannerDbCredentialStoredVersion {
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$Target,
        [string]$CredentialRoot = ''
    )
    $path = Get-PandoraPlannerDbCredentialPath -WorkspaceId $WorkspaceId -CredentialRoot $CredentialRoot
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { return [int64]0 }
    try {
        $info = Get-Item -LiteralPath $path -Force -ErrorAction Stop
        if ($info.Length -gt 65536) { throw '凭据文件超过 64 KiB' }
        $raw = [IO.File]::ReadAllText($path) | ConvertFrom-Json
        Assert-PandoraMysqlProfileExactMembers $raw @(
            'schema_version', 'provider', 'workspace_id', 'device_digest', 'target', 'username', 'password_dpapi', 'version'
        ) 'planner db credential metadata'
        if (($raw.schema_version -isnot [int] -and $raw.schema_version -isnot [long]) -or [int64]$raw.schema_version -ne 2 -or
            "$($raw.provider)" -cne 'dpapi-current-user' -or "$($raw.workspace_id)" -cne $WorkspaceId -or
            "$($raw.target)" -cne $Target -or "$($raw.username)" -cne "p_app_$WorkspaceId" -or
            ($raw.version -isnot [int] -and $raw.version -isnot [long]) -or [int64]$raw.version -lt 1) {
            throw 'credential metadata identity/version 不合法'
        }
        return [int64]$raw.version
    } catch {
        throw "无法读取现有 DPAPI credential version；拒绝无依据覆盖:$path。详情:$($_.Exception.Message)"
    }
}

function New-PandoraPlannerPrivateAcl([switch]$Directory) {
    $sid = [Security.Principal.WindowsIdentity]::GetCurrent().User
    if (-not $sid) { throw '无法取得当前 Windows 用户 SID，拒绝保存凭据' }
    if ($Directory) {
        $acl = [Security.AccessControl.DirectorySecurity]::new()
        $rights = [Security.AccessControl.FileSystemRights]::FullControl
        $inheritance = [Security.AccessControl.InheritanceFlags]'ContainerInherit, ObjectInherit'
        $propagation = [Security.AccessControl.PropagationFlags]::None
        $rule = [Security.AccessControl.FileSystemAccessRule]::new(
            $sid, $rights, $inheritance, $propagation, [Security.AccessControl.AccessControlType]::Allow)
    } else {
        $acl = [Security.AccessControl.FileSecurity]::new()
        $rule = [Security.AccessControl.FileSystemAccessRule]::new(
            $sid, [Security.AccessControl.FileSystemRights]::FullControl,
            [Security.AccessControl.AccessControlType]::Allow)
    }
    $acl.SetOwner($sid)
    $acl.SetAccessRuleProtection($true, $false)
    $acl.AddAccessRule($rule)
    return $acl
}

function Set-PandoraPlannerPrivateAcl {
    param(
        [Parameter(Mandatory)][string]$Path,
        [switch]$Directory
    )
    $acl = New-PandoraPlannerPrivateAcl -Directory:$Directory
    Set-Acl -LiteralPath $Path -AclObject $acl
    $verified = Get-Acl -LiteralPath $Path
    if (-not $verified.AreAccessRulesProtected) { throw "ACL 仍允许继承:$Path" }
    $broad = @('S-1-1-0', 'S-1-5-11', 'S-1-5-32-545')
    foreach ($entry in @($verified.Access)) {
        $entrySid = try { $entry.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value } catch { '' }
        if ($entry.AccessControlType -eq 'Allow' -and $broad -contains $entrySid) {
            throw "ACL 仍向宽泛主体授权:$entrySid"
        }
    }
}

function Get-PandoraPlannerDpapiEntropy([string]$Target) {
    return [Security.Cryptography.SHA256]::HashData(
        [Text.Encoding]::UTF8.GetBytes("Pandora/PlannerDB/Credential/v1|$Target"))
}

function Protect-PandoraPlannerSecureString {
    param(
        [Parameter(Mandatory)][Security.SecureString]$Value,
        [Parameter(Mandatory)][string]$Target
    )
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Value)
    $plainBytes = $null
    try {
        $plainText = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
        $plainBytes = [Text.Encoding]::UTF8.GetBytes($plainText)
        $cipher = [Security.Cryptography.ProtectedData]::Protect(
            $plainBytes, (Get-PandoraPlannerDpapiEntropy $Target),
            [Security.Cryptography.DataProtectionScope]::CurrentUser)
        return [Convert]::ToBase64String($cipher)
    } finally {
        if ($plainBytes) { [Security.Cryptography.CryptographicOperations]::ZeroMemory($plainBytes) }
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
}

function Unprotect-PandoraPlannerSecureString {
    param(
        [Parameter(Mandatory)][string]$Ciphertext,
        [Parameter(Mandatory)][string]$Target
    )
    $cipher = $null
    $plainBytes = $null
    try {
        $cipher = [Convert]::FromBase64String($Ciphertext)
        $plainBytes = [Security.Cryptography.ProtectedData]::Unprotect(
            $cipher, (Get-PandoraPlannerDpapiEntropy $Target),
            [Security.Cryptography.DataProtectionScope]::CurrentUser)
        $plain = [Text.Encoding]::UTF8.GetString($plainBytes)
        return (ConvertTo-SecureString $plain -AsPlainText -Force)
    } catch {
        throw 'DPAPI 凭据无法由当前 Windows 用户解密；拒绝猜测或改连其它 workspace'
    } finally {
        if ($plainBytes) { [Security.Cryptography.CryptographicOperations]::ZeroMemory($plainBytes) }
        if ($cipher) { [Security.Cryptography.CryptographicOperations]::ZeroMemory($cipher) }
    }
}

function Save-PandoraPlannerDbCredential {
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$Target,
        [Parameter(Mandatory)][string]$UserName,
        [Parameter(Mandatory)][Security.SecureString]$Password,
        [Parameter(Mandatory)][int]$Version,
        [string]$DeviceDigest = '',
        [string]$CredentialRoot = ''
    )
    Assert-PandoraPlannerWorkspaceId $WorkspaceId
    $expectedTarget = "Pandora/PlannerDB/$WorkspaceId/app"
    if ($Target -cne $expectedTarget) { throw "credential target 不属于当前 workspace，期望:$expectedTarget" }
    if ($UserName -cnotmatch '^[A-Za-z0-9_.-]{1,32}$' -or $UserName -cne "p_app_$WorkspaceId") {
        throw 'runtime username 必须精确属于当前 workspace'
    }
    if ($Version -lt 1) { throw 'credential version 必须是正整数' }
    if ([string]::IsNullOrWhiteSpace($DeviceDigest)) { $DeviceDigest = Get-PandoraPlannerDeviceDigest }
    if ($DeviceDigest -cnotmatch '^sha256:[0-9a-f]{64}$') { throw 'device_digest 非 canonical SHA-256' }
    $path = Get-PandoraPlannerDbCredentialPath -WorkspaceId $WorkspaceId -CredentialRoot $CredentialRoot
    $storedVersion = Get-PandoraPlannerDbCredentialStoredVersion -WorkspaceId $WorkspaceId `
        -Target $Target -CredentialRoot $CredentialRoot
    if ($storedVersion -gt [int64]$Version) {
        throw "拒绝 credential version 降级:现有=$storedVersion，中心返回=$Version"
    }
    $dir = Split-Path -Parent $path
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
    Set-PandoraPlannerPrivateAcl -Path $dir -Directory
    $temporary = Join-Path $dir ("credential.{0}.{1}.tmp" -f $PID, [guid]::NewGuid().ToString('N'))
    try {
        $protected = Protect-PandoraPlannerSecureString -Value $Password -Target $Target
        $record = [pscustomobject][ordered]@{
            schema_version = 2
            provider = 'dpapi-current-user'
            workspace_id = $WorkspaceId
            device_digest = $DeviceDigest
            target = $Target
            username = $UserName
            password_dpapi = $protected
            version = $Version
        }
        [IO.File]::WriteAllText($temporary, ($record | ConvertTo-Json), [Text.UTF8Encoding]::new($false))
        Set-PandoraPlannerPrivateAcl -Path $temporary
        [IO.File]::Move($temporary, $path, $true)
        Set-PandoraPlannerPrivateAcl -Path $path
    } catch {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
        throw "保存当前用户 DPAPI 凭据失败；未写入 runtime profile。详情:$($_.Exception.Message)"
    } finally {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
    }
    return [pscustomobject][ordered]@{
        Provider = 'dpapi-current-user'
        Target = $Target
        Version = $Version
        Path = $path
    }
}

function Get-PandoraPlannerDbCredential {
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$Target,
        [string]$CredentialRoot = ''
    )
    Assert-PandoraPlannerWorkspaceId $WorkspaceId
    $expectedTarget = "Pandora/PlannerDB/$WorkspaceId/app"
    if ($Target -cne $expectedTarget) { throw 'credential target 与 workspace 不一致' }
    $path = Get-PandoraPlannerDbCredentialPath -WorkspaceId $WorkspaceId -CredentialRoot $CredentialRoot
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "缺少当前 workspace 的 DPAPI runtime 凭据:$path" }
    try {
        $info = Get-Item -LiteralPath $path
        if ($info.Length -gt 65536) { throw '凭据文件超过 64 KiB' }
        $raw = Get-Content -LiteralPath $path -Raw -Encoding utf8 | ConvertFrom-Json
        Assert-PandoraMysqlProfileExactMembers $raw @(
            'schema_version', 'provider', 'workspace_id', 'device_digest', 'target', 'username', 'password_dpapi', 'version'
        ) 'planner db credential'
        $currentDeviceDigest = Get-PandoraPlannerDeviceDigest
        if (($raw.schema_version -isnot [int] -and $raw.schema_version -isnot [long]) -or [int64]$raw.schema_version -ne 2 -or
            "$($raw.provider)" -cne 'dpapi-current-user' -or "$($raw.workspace_id)" -cne $WorkspaceId -or
            "$($raw.device_digest)" -cnotmatch '^sha256:[0-9a-f]{64}$' -or "$($raw.device_digest)" -cne $currentDeviceDigest -or
            "$($raw.target)" -cne $Target -or "$($raw.username)" -cne "p_app_$WorkspaceId" -or
            ($raw.version -isnot [int] -and $raw.version -isnot [long]) -or [int64]$raw.version -lt 1) {
            throw 'DPAPI credential identity/device/version 不合法；可能是工作区或系统克隆，拒绝自动改绑'
        }
        $password = Unprotect-PandoraPlannerSecureString -Ciphertext "$($raw.password_dpapi)" -Target $Target
        return [pscustomobject][ordered]@{
            WorkspaceId = $WorkspaceId
            UserName = "$($raw.username)"
            Password = $password
            Version = [int64]$raw.version
            Provider = 'dpapi-current-user'
            Target = $Target
            Path = $path
        }
    } catch {
        throw "当前 workspace 的 DPAPI runtime 凭据损坏或不属于本用户；拒绝继续。详情:$($_.Exception.Message)"
    }
}

function Get-PandoraPlannerDeviceDigest {
    $machineGuid = ''
    try {
        $machineGuid = "$(Get-ItemPropertyValue -LiteralPath 'HKLM:\SOFTWARE\Microsoft\Cryptography' -Name MachineGuid -ErrorAction Stop)".Trim()
    } catch { throw '无法读取本机 MachineGuid，不能安全 enrollment' }
    $sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    if ([string]::IsNullOrWhiteSpace($machineGuid) -or [string]::IsNullOrWhiteSpace($sid)) {
        throw '本机 MachineGuid 或 Windows SID 为空，不能安全 enrollment'
    }
    $bytes = [Text.Encoding]::UTF8.GetBytes("Pandora/PlannerDB/Device/v1|$machineGuid|$sid")
    return 'sha256:' + [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($bytes)).ToLowerInvariant()
}

function Invoke-PandoraPlannerEnrollmentHttp {
    param(
        [Parameter(Mandatory)]$Config,
        [Parameter(Mandatory)]$Request
    )
    $curl = Join-Path $env:SystemRoot 'System32/curl.exe'
    if (-not (Test-Path -LiteralPath $curl -PathType Leaf)) {
        $curlCommand = Get-Command curl.exe -ErrorAction SilentlyContinue
        if (-not $curlCommand) { throw 'Windows curl.exe 不存在，不能在不安装额外工具的前提下验证 bundle CA' }
        $curl = $curlCommand.Source
    }
    $json = $Request | ConvertTo-Json -Depth 5 -Compress
    $bodyText = $null
    $body = $null
    $lines = $null
    $headerPath = Join-Path ([IO.Path]::GetTempPath()) ("pandora-enroll-headers-{0}-{1}.tmp" -f $PID, [guid]::NewGuid().ToString('N'))
    $args = @(
        # 不使用较新 curl 才支持的 --fail-with-body；HTTP 状态由下方 write-out 独立校验，
        # 这样 Windows 自带的较旧 curl 也能工作，错误正文仍不会进入异常或日志。
        '--silent', '--show-error', '--request', 'POST',
        '--header', 'Content-Type: application/json', '--cacert', $Config.ca_file,
        '--proto', '=https', '--tlsv1.2', '--connect-timeout', '5', '--max-time', '30',
        '--data-binary', '@-', '--dump-header', $headerPath,
        '--write-out', "`nPANDORA_HTTP_STATUS:%{http_code}",
        $Config.provisioner_url
    )
    try {
        $lines = @($json | & $curl @args 2>&1 | ForEach-Object { "$_" })
        $exitCode = $LASTEXITCODE
        $statusLine = @($lines | Where-Object { $_ -match '^PANDORA_HTTP_STATUS:[0-9]{3}$' } | Select-Object -Last 1)
        $status = if ($statusLine.Count -eq 1) { [int]($statusLine[0] -replace '^PANDORA_HTTP_STATUS:', '') } else { 0 }
        if ($exitCode -ne 0 -or $status -eq 0 -or $status -eq 408 -or $status -eq 429 -or $status -ge 500) {
            throw "PANDORA_ENROLLMENT_RETRYABLE:中心 MySQL enrollment 暂时不可用(exit=$exitCode,http=$status)；token/响应正文已隐藏"
        }
        if ($status -notin @(200, 202)) {
            throw "中心 MySQL enrollment 请求被拒绝(http=$status)；token/响应正文已隐藏"
        }
        $bodyText = (@($lines | Where-Object { $_ -notmatch '^PANDORA_HTTP_STATUS:[0-9]{3}$' }) -join "`n").Trim()
        try { $body = $bodyText | ConvertFrom-Json } catch { throw '中心 MySQL enrollment 返回的 JSON 非法；正文已隐藏' }
        $retryAfter = 0
        if ($status -eq 202) {
            $retryHeaders = if (Test-Path -LiteralPath $headerPath -PathType Leaf) {
                @(Get-Content -LiteralPath $headerPath -Encoding ascii | Where-Object { $_ -match '(?i)^Retry-After:\s*[0-9]+\s*$' })
            } else { @() }
            if ($retryHeaders.Count -eq 0) { throw '中心 MySQL enrollment 202 缺少 Retry-After；响应正文已隐藏' }
            $retryText = ($retryHeaders[-1] -replace '(?i)^Retry-After:\s*', '').Trim()
            if (-not [int]::TryParse($retryText, [ref]$retryAfter) -or $retryAfter -lt 1 -or $retryAfter -gt 5) {
                throw '中心 MySQL enrollment Retry-After 不在 1..5 秒白名单；响应正文已隐藏'
            }
        }
        return [pscustomobject][ordered]@{
            StatusCode = $status
            RetryAfterSeconds = $retryAfter
            Body = $body
        }
    } finally {
        Remove-Item -LiteralPath $headerPath -Force -ErrorAction SilentlyContinue
        $json = $null
        $bodyText = $null
        $body = $null
        $lines = $null
    }
}

function Assert-PandoraPlannerEnrollmentResponse {
    param(
        [Parameter(Mandatory)]$HttpResult,
        [Parameter(Mandatory)]$Config,
        [Parameter(Mandatory)][string]$ProjectRoot,
        [string]$ExpectedWorkspaceId = ''
    )
    Assert-PandoraMysqlProfileExactMembers $HttpResult @('StatusCode', 'RetryAfterSeconds', 'Body') 'enrollment HTTP result'
    $statusCode = 0
    if (-not [int]::TryParse("$($HttpResult.StatusCode)", [ref]$statusCode) -or $statusCode -notin @(200, 202)) {
        throw 'enrollment HTTP status 非 200/202'
    }
    $Response = $HttpResult.Body
    $requiredMembers = if ($statusCode -eq 202) {
        @('schema_version', 'workspace_id', 'state', 'endpoint', 'databases', 'retry_after_ms')
    } else {
        @('schema_version', 'workspace_id', 'state', 'endpoint', 'databases', 'credential')
    }
    Assert-PandoraMysqlProfileExactMembers $Response $requiredMembers 'enrollment response'
    if (($Response.schema_version -isnot [int] -and $Response.schema_version -isnot [long]) -or [int64]$Response.schema_version -ne 1) {
        throw 'enrollment response schema_version 非法'
    }
    $workspaceId = "$($Response.workspace_id)"
    Assert-PandoraPlannerWorkspaceId $workspaceId
    if (-not [string]::IsNullOrWhiteSpace($ExpectedWorkspaceId) -and $workspaceId -cne $ExpectedWorkspaceId) {
        throw "enrollment 轮询 workspace 漂移/不一致:期望=$ExpectedWorkspaceId，实际=$workspaceId"
    }
    $state = "$($Response.state)"
    if ($state -cnotin @('PROVISIONING', 'MIGRATING', 'READY', 'MIGRATION_FAILED')) {
        throw "enrollment state 不在白名单:$state"
    }
    Assert-PandoraMysqlProfileExactMembers $Response.endpoint @('host', 'port', 'tls_server_name') 'enrollment endpoint'
    $responseEndpoint = ConvertTo-PandoraMysqlProfileEndpoint -Mode central-managed -Endpoint ([ordered]@{
            host = $Response.endpoint.host
            port = $Response.endpoint.port
            tls_server_name = $Response.endpoint.tls_server_name
            ca_file = $Config.ca_file
        })
    if ($responseEndpoint.host -cne $Config.endpoint.host -or $responseEndpoint.port -ne $Config.endpoint.port -or
        $responseEndpoint.tls_server_name -cne $Config.endpoint.tls_server_name) {
        throw '服务端返回的 MySQL endpoint 与 SVN bundle 信任配置不一致'
    }
    $sets = @(Get-PandoraMysqlMigrationSets -ProjectRoot $ProjectRoot)
    Assert-PandoraMysqlProfileExactMembers $Response.databases $sets 'enrollment databases'
    foreach ($set in $sets) {
        $actual = "$(Get-PandoraMysqlProfileMember $Response.databases $set 'enrollment databases')"
        if ($actual -cne "${set}_w_$workspaceId") { throw "服务端 database mapping 非 exact workspace:$set=$actual" }
    }
    if ($state -ceq 'MIGRATION_FAILED') { throw '中心 workspace 进入 MIGRATION_FAILED；须由管理员修复后重试' }
    if ($statusCode -eq 202) {
        if ($state -cnotin @('PROVISIONING', 'MIGRATING')) { throw "HTTP 202 不能携带终态 state=$state" }
        $retryMs = 0
        $retrySeconds = 0
        if (-not [int]::TryParse("$($Response.retry_after_ms)", [ref]$retryMs) -or $retryMs -lt 1 -or $retryMs -gt 5000 -or
            -not [int]::TryParse("$($HttpResult.RetryAfterSeconds)", [ref]$retrySeconds) -or
            $retrySeconds -lt 1 -or $retrySeconds -gt 5 -or [Math]::Ceiling($retryMs / 1000.0) -ne $retrySeconds) {
            throw 'HTTP 202 retry_after_ms/Retry-After 不合法或不一致'
        }
        return [pscustomobject][ordered]@{
            Pending = $true; WorkspaceId = $workspaceId; State = $state; RetryAfterMs = $retryMs
        }
    }
    if ($state -cne 'READY') { throw "HTTP 200 必须是 READY，实际=$state" }
    if ([int]$HttpResult.RetryAfterSeconds -ne 0) { throw 'HTTP 200 不应携带 Retry-After' }
    Assert-PandoraMysqlProfileExactMembers $Response.credential @('username', 'password', 'version') 'enrollment credential'
    $username = "$($Response.credential.username)"
    $password = "$($Response.credential.password)"
    if ($username -cne "p_app_$workspaceId" -or $username -cnotmatch '^[A-Za-z0-9_.-]{1,32}$') {
        throw '服务端 runtime username 不属于 exact workspace'
    }
    if ($password -cnotmatch '^[A-Za-z0-9_-]{24,128}$') { throw '服务端 runtime password 不是受支持的高熵 base64url 形态' }
    if (($Response.credential.version -isnot [int] -and $Response.credential.version -isnot [long]) -or
        [int64]$Response.credential.version -lt 1 -or [int64]$Response.credential.version -gt [int]::MaxValue) {
        throw '服务端 credential version 非法'
    }
    return [pscustomobject][ordered]@{
        Pending = $false
        WorkspaceId = $workspaceId
        UserName = $username
        Password = ConvertTo-SecureString $password -AsPlainText -Force
        Version = [int64]$Response.credential.version
    }
}

function ConvertFrom-PandoraSecureString {
    param([Parameter(Mandatory)][Security.SecureString]$Value)
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Value)
    try { return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
    finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
}

function Get-PandoraPlannerEnrollmentUtcNow { return [DateTimeOffset]::UtcNow }

function Wait-PandoraPlannerEnrollmentRetry([int]$Milliseconds) {
    Start-Sleep -Milliseconds $Milliseconds
}

function Ensure-PandoraPlannerMysqlEnrollment {
    param(
        [Parameter(Mandatory)][string]$ProjectRoot,
        [Parameter(Mandatory)][string]$ConfigPath,
        [string]$IdentityPath = '',
        [string]$CredentialRoot = '',
        [string]$OutputPath = '',
        [Security.SecureString]$EnrollmentToken,
        [string]$ComputerName = $env:COMPUTERNAME,
        [string]$UserName = $env:USERNAME,
        [ValidateRange(1, 3600)][int]$EnrollmentTimeoutSeconds = 600
    )
    $config = Get-PandoraPlannerCentralMysqlConfig -ConfigPath $ConfigPath
    if ([string]::IsNullOrWhiteSpace($IdentityPath)) { $IdentityPath = Get-PandoraMysqlProfileDefaultIdentityPath }
    $identity = $null
    if (Test-Path -LiteralPath $IdentityPath -PathType Leaf) {
        $identity = Get-PandoraPlannerDbIdentity -IdentityPath $IdentityPath
        $target = "Pandora/PlannerDB/$($identity.workspace_id)/app"
        $credentialPath = Get-PandoraPlannerDbCredentialPath -WorkspaceId $identity.workspace_id -CredentialRoot $CredentialRoot
        if ((Test-Path -LiteralPath $credentialPath -PathType Leaf) -and -not $EnrollmentToken) {
            $credential = Get-PandoraPlannerDbCredential -WorkspaceId $identity.workspace_id -Target $target -CredentialRoot $CredentialRoot
            return Publish-PandoraMysqlRuntimeProfile -ProjectRoot $ProjectRoot -Mode central-managed `
                -Endpoint $config.endpoint -CredentialRef ([ordered]@{
                    provider = $credential.Provider; target = $credential.Target; version = $credential.Version
                }) -IdentityPath $IdentityPath -OutputPath $OutputPath -ComputerName $ComputerName -UserName $UserName
        }
    }

    if (-not $EnrollmentToken) {
        $prompt = if ($identity) {
            '当前 workspace 凭据缺失；请输入中心管理员给的 recovery code'
        } else {
            '首次登记请输入 43 字符 enrollment code（恢复旧 workspace 时输入 recovery code）'
        }
        $EnrollmentToken = Read-Host $prompt -AsSecureString
    }
    $tokenText = ConvertFrom-PandoraSecureString $EnrollmentToken
    $rawToken = ''
    $recoveryWorkspaceId = ''
    $request = $null
    $httpResult = $null
    $accepted = $null
    try {
        if ($tokenText -cmatch '^([0-7][0-9a-hjkmnp-tv-z]{25})\.([A-Za-z0-9_-]{43})$') {
            $recoveryWorkspaceId = $Matches[1]
            $rawToken = $Matches[2]
            if ($identity -and $identity.workspace_id -cne $recoveryWorkspaceId) {
                throw "recovery code workspace=$recoveryWorkspaceId 与本机 bind-once identity=$($identity.workspace_id) 不一致"
            }
        } elseif ($tokenText -cmatch '^[A-Za-z0-9_-]{43}$') {
            $rawToken = $tokenText
        } else {
            throw 'enrollment code 必须是 43 字符；recovery code 必须是 <26位workspace_id>.<43字符raw token>'
        }
        foreach ($part in @($ComputerName, $UserName)) {
            if ([string]::IsNullOrWhiteSpace($part) -or $part.Length -gt 64 -or $part -match '[\x00-\x1f\x7f\\/]') {
                throw '电脑名或用户名不能用于安全显示标签'
            }
        }
        $requestFields = [ordered]@{
            schema_version = 1
            enrollment_token = $rawToken
            device_digest = Get-PandoraPlannerDeviceDigest
            display_name = "$($ComputerName.Trim())\$($UserName.Trim())"
        }
        if (-not [string]::IsNullOrWhiteSpace($recoveryWorkspaceId)) {
            $requestFields.expected_workspace_id = $recoveryWorkspaceId
        }
        $request = [pscustomobject]$requestFields
        $deadline = (Get-PandoraPlannerEnrollmentUtcNow).AddSeconds($EnrollmentTimeoutSeconds)
        $expectedWorkspaceId = if (-not [string]::IsNullOrWhiteSpace($recoveryWorkspaceId)) {
            $recoveryWorkspaceId
        } elseif ($identity) { "$($identity.workspace_id)" } else { '' }
        $accepted = $null
        while ($true) {
            $now = Get-PandoraPlannerEnrollmentUtcNow
            if ($now -ge $deadline) { throw '中心 MySQL enrollment 超时，已到总截止；token/响应正文已隐藏' }
            $httpResult = $null
            try {
                $httpResult = Invoke-PandoraPlannerEnrollmentHttp -Config $config -Request $request
            } catch {
                if ($_.Exception.Message -notmatch '^PANDORA_ENROLLMENT_RETRYABLE:') { throw }
                $retryMs = 1000
                if ((Get-PandoraPlannerEnrollmentUtcNow).AddMilliseconds($retryMs) -gt $deadline) {
                    throw '中心 MySQL enrollment 暂时不可用且已到总截止；token/响应正文已隐藏'
                }
                Wait-PandoraPlannerEnrollmentRetry -Milliseconds $retryMs
                continue
            }
            try {
                $accepted = Assert-PandoraPlannerEnrollmentResponse -HttpResult $httpResult -Config $config `
                    -ProjectRoot $ProjectRoot -ExpectedWorkspaceId $expectedWorkspaceId
            } finally {
                # READY 明文密码已转 SecureString 后立即从 HTTP object 移除；不让它留到保存/发布阶段。
                if ($httpResult -and $httpResult.Body -and
                    $httpResult.Body.PSObject.Properties.Name -contains 'credential' -and $httpResult.Body.credential) {
                    try { $httpResult.Body.credential.password = $null } catch { }
                }
                $httpResult = $null
            }
            if ([string]::IsNullOrWhiteSpace($expectedWorkspaceId)) { $expectedWorkspaceId = $accepted.WorkspaceId }
            if (-not $accepted.Pending) { break }
            if ((Get-PandoraPlannerEnrollmentUtcNow).AddMilliseconds([int]$accepted.RetryAfterMs) -gt $deadline) {
                throw '中心 MySQL enrollment 尚未 READY，下一次轮询将越过总截止；已有界超时'
            }
            Wait-PandoraPlannerEnrollmentRetry -Milliseconds ([int]$accepted.RetryAfterMs)
        }
        if ($identity -and $identity.workspace_id -cne $accepted.WorkspaceId) {
            throw "本机已绑定 workspace=$($identity.workspace_id)，中心返回 $($accepted.WorkspaceId)；按 clone/身份冲突阻断"
        }
        $identity = Set-PandoraPlannerDbIdentity -WorkspaceId $accepted.WorkspaceId -IdentityPath $IdentityPath
        $target = "Pandora/PlannerDB/$($identity.workspace_id)/app"
        $saved = Save-PandoraPlannerDbCredential -WorkspaceId $identity.workspace_id -Target $target `
            -UserName $accepted.UserName -Password $accepted.Password -Version $accepted.Version `
            -DeviceDigest $request.device_digest -CredentialRoot $CredentialRoot
        return Publish-PandoraMysqlRuntimeProfile -ProjectRoot $ProjectRoot -Mode central-managed `
            -Endpoint $config.endpoint -CredentialRef ([ordered]@{
                provider = $saved.Provider; target = $saved.Target; version = $saved.Version
            }) -IdentityPath $IdentityPath -OutputPath $OutputPath -ComputerName $ComputerName -UserName $UserName
    } finally {
        if ($accepted -and $accepted.PSObject.Properties.Name -contains 'Password' -and $accepted.Password) {
            $accepted.Password.Dispose()
            $accepted.Password = $null
        }
        if ($request) { try { $request.enrollment_token = $null } catch { } }
        $request = $null
        $httpResult = $null
        $rawToken = $null
        $recoveryWorkspaceId = $null
        $tokenText = $null
    }
}
