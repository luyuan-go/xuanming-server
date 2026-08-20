# 策划中心 MySQL 首次登记 / DPAPI 凭据契约。
#
# HTTP 使用内存桩；DPAPI 与 Windows ACL 使用当前测试用户的真实实现，不访问网络或 MySQL。

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ScriptsDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptsDir '../..')).Path
$EnrollmentLib = Join-Path $ScriptsDir 'lib/planner_mysql_enrollment.ps1'
$script:Failures = [Collections.Generic.List[string]]::new()

function Assert-True([bool]$Condition, [string]$Message) {
    if ($Condition) { Write-Host "  [ok] $Message" -ForegroundColor Green }
    else { $script:Failures.Add($Message); Write-Host "  [FAIL] $Message" -ForegroundColor Red }
}

if (-not (Test-Path -LiteralPath $EnrollmentLib -PathType Leaf)) {
    Write-Host "[FAIL] 缺少中心 MySQL enrollment 公共库:$EnrollmentLib" -ForegroundColor Red
    exit 1
}
. $EnrollmentLib

$sandbox = Join-Path ([IO.Path]::GetTempPath()) ("pandora-planner-enroll-{0}-{1}" -f $PID, [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $sandbox | Out-Null
try {
    $workspaceId = '01jabcdefghjkmnpqrstvwxyz0'
    $otherWorkspaceId = '11jabcdefghjkmnpqrstvwxyz0'
    $credentialRoot = Join-Path $sandbox 'credentials'
    $identityPath = Join-Path $sandbox 'identity.json'
    $profilePath = Join-Path $sandbox 'mysql-runtime-profile.json'
    $bundleDir = Join-Path $sandbox 'bundle'
    New-Item -ItemType Directory -Path $bundleDir | Out-Null
    $caPath = Join-Path $bundleDir 'planner-db-ca.pem'
    [IO.File]::WriteAllText($caPath, "fixture-ca`n", [Text.UTF8Encoding]::new($false))
    $configPath = Join-Path $bundleDir 'central-mysql.json'
    [pscustomobject][ordered]@{
        schema_version = 1
        provisioner_url = 'https://pandora-planner-db.intra:7443/v1/enroll'
        endpoint = [pscustomobject][ordered]@{
            host = 'pandora-planner-db.intra'
            port = 3306
            tls_server_name = 'pandora-planner-db.intra'
        }
        ca_file = 'planner-db-ca.pem'
    } | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $configPath -Encoding utf8NoBOM

    Write-Host '[1] SVN bundle 中心配置只含非秘密且严格绑定 HTTPS / CA / 主机名' -ForegroundColor Cyan
    $config = Get-PandoraPlannerCentralMysqlConfig -ConfigPath $configPath
    Assert-True ($config.provisioner_url -ceq 'https://pandora-planner-db.intra:7443/v1/enroll') '只接受固定 HTTPS enrollment endpoint'
    Assert-True ($config.endpoint.host -ceq $config.endpoint.tls_server_name) 'MySQL host 与证书名完全一致'
    Assert-True ($config.ca_file -ceq [IO.Path]::GetFullPath($caPath)) '相对 CA 只相对 bundle 配置目录解析一次'
    $badConfig = Get-Content -LiteralPath $configPath -Raw -Encoding utf8 | ConvertFrom-Json
    $badConfig.provisioner_url = 'http://pandora-planner-db.intra:7443/v1/enroll'
    $badPath = Join-Path $bundleDir 'bad.json'
    $badConfig | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $badPath -Encoding utf8NoBOM
    $blocked = $false
    try { Get-PandoraPlannerCentralMysqlConfig -ConfigPath $badPath | Out-Null } catch { $blocked = $true }
    Assert-True $blocked 'HTTP 明文 provisioner 被拒绝'

    Write-Host '[2] runtime 密码仅进入当前用户 DPAPI 文件' -ForegroundColor Cyan
    $target = "Pandora/PlannerDB/$workspaceId/app"
    $roundTripCredentialRoot = Join-Path $sandbox 'roundtrip-credentials'
    $secret = ConvertTo-SecureString 'PANDORA_TEST_PASSWORD_123456789' -AsPlainText -Force
    $saved = Save-PandoraPlannerDbCredential -WorkspaceId $workspaceId -Target $target `
        -UserName "p_app_$workspaceId" -Password $secret -Version 7 -CredentialRoot $roundTripCredentialRoot
    $loaded = Get-PandoraPlannerDbCredential -WorkspaceId $workspaceId -Target $target -CredentialRoot $roundTripCredentialRoot
    $plain = [Net.NetworkCredential]::new('', $loaded.Password).Password
    Assert-True ($loaded.UserName -ceq "p_app_$workspaceId" -and $plain -ceq 'PANDORA_TEST_PASSWORD_123456789') 'DPAPI round-trip 返回 exact 账号密码'
    Assert-True ($loaded.WorkspaceId -ceq $workspaceId -and $loaded.Version -eq 7 -and $saved.Target -ceq $target) 'workspace、凭据版本与逻辑 target 一并绑定'
    $credentialText = Get-Content -LiteralPath $saved.Path -Raw -Encoding utf8
    Assert-True ($credentialText -notmatch 'PANDORA_TEST_PASSWORD|123456789') '磁盘文件不含明文密码'
    Assert-True ($credentialText -match '"schema_version"\s*:\s*2' -and $credentialText -match '"device_digest"') `
        'DPAPI 凭据 v2 绑定首次 device digest'
    $acl = Get-Acl -LiteralPath $saved.Path
    $broadSids = @('S-1-1-0', 'S-1-5-11', 'S-1-5-32-545')
    $broadRules = @($acl.Access | Where-Object {
        $sid = try { $_.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value } catch { '' }
        $broadSids -contains $sid -and $_.AccessControlType -eq 'Allow'
    })
    Assert-True ($acl.AreAccessRulesProtected -and $broadRules.Count -eq 0) '凭据文件关闭继承且不授权 Everyone/Users/Authenticated Users'
    $tamperedRecord = Get-Content -LiteralPath $saved.Path -Raw -Encoding utf8 | ConvertFrom-Json
    $tamperedRecord.password_dpapi = $tamperedRecord.password_dpapi.Substring(0, $tamperedRecord.password_dpapi.Length - 4) + 'AAAA'
    $tamperedRecord | ConvertTo-Json | Set-Content -LiteralPath $saved.Path -Encoding utf8NoBOM
    $tamperBlocked = $false
    try { Get-PandoraPlannerDbCredential -WorkspaceId $workspaceId -Target $target -CredentialRoot $roundTripCredentialRoot | Out-Null } catch { $tamperBlocked = $true }
    Assert-True $tamperBlocked 'DPAPI ciphertext 被篡改后 fail-closed'
    Save-PandoraPlannerDbCredential -WorkspaceId $workspaceId -Target $target `
        -UserName "p_app_$workspaceId" -Password $secret -Version 7 -CredentialRoot $roundTripCredentialRoot | Out-Null

    Write-Host '[3] 首次登记校验服务端 exact workspace / endpoint / 十库映射' -ForegroundColor Cyan
    $script:EnrollmentCalls = 0
    $script:CapturedEnrollmentRequest = $null
    $script:EnrollmentWorkspace = $workspaceId
    $script:EnrollmentEndpointHost = 'pandora-planner-db.intra'
    $script:BreakDatabaseMapping = $false
    $script:EnrollmentCredentialVersion = 3
    $script:EnrollmentPassword = 'PANDORA_ENROLLED_PASSWORD_123456'
    $script:EnrollmentSequence = @()
    $script:CapturedEnrollmentRequests = @()
    function Get-PandoraPlannerDeviceDigest { return 'sha256:' + ('a' * 64) }
    function Invoke-PandoraPlannerEnrollmentHttp {
        param($Config, $Request)
        $script:EnrollmentCalls++
        # HTTP boundary 收到的是调用时快照；生产函数 finally 会清空自己的 request token 引用，
        # 测试副本不能与其共享同一个 PSObject，否则无法验证实际 wire payload。
        $script:CapturedEnrollmentRequest = ($Request | ConvertTo-Json -Depth 5 -Compress | ConvertFrom-Json)
        $script:CapturedEnrollmentRequests += ($Request | ConvertTo-Json -Depth 5 -Compress)
        $step = if ($script:EnrollmentSequence.Count -gt 0) {
            $next = $script:EnrollmentSequence[0]
            $script:EnrollmentSequence = @($script:EnrollmentSequence | Select-Object -Skip 1)
            $next
        } else { [pscustomobject]@{ StatusCode = 200; State = 'READY'; RetryAfterMs = 0; IncludeCredential = $true; WorkspaceId = $script:EnrollmentWorkspace } }
        $stepWorkspace = if ($step.PSObject.Properties.Name -contains 'WorkspaceId' -and $step.WorkspaceId) { "$($step.WorkspaceId)" } else { $script:EnrollmentWorkspace }
        $databases = [ordered]@{}
        foreach ($set in @(Get-PandoraMysqlMigrationSets -ProjectRoot $RepoRoot)) {
            $databases[$set] = "${set}_w_$stepWorkspace"
        }
        if ($script:BreakDatabaseMapping) { $databases['pandora_account'] = 'pandora_account' }
        $body = [pscustomobject][ordered]@{
            schema_version = 1
            workspace_id = $stepWorkspace
            state = "$($step.State)"
            endpoint = [pscustomobject][ordered]@{
                host = $script:EnrollmentEndpointHost
                port = 3306
                tls_server_name = $script:EnrollmentEndpointHost
            }
            databases = [pscustomobject]$databases
        }
        if ([int]$step.StatusCode -eq 202) {
            $body | Add-Member -NotePropertyName retry_after_ms -NotePropertyValue ([int]$step.RetryAfterMs)
            if ([bool]$step.IncludeCredential) {
                $body | Add-Member -NotePropertyName credential -NotePropertyValue ([pscustomobject]@{ username='bad';password='bad';version=1 })
            }
        } elseif ([bool]$step.IncludeCredential) {
            $body | Add-Member -NotePropertyName credential -NotePropertyValue ([pscustomobject][ordered]@{
                username = "p_app_$stepWorkspace"
                password = $script:EnrollmentPassword
                version = $script:EnrollmentCredentialVersion
            })
        }
        return [pscustomobject][ordered]@{
            StatusCode = [int]$step.StatusCode
            RetryAfterSeconds = if ([int]$step.StatusCode -eq 202) { [int][Math]::Ceiling(([int]$step.RetryAfterMs) / 1000.0) } else { 0 }
            Body = $body
        }
    }
    $token = ConvertTo-SecureString 'abcdefghijklmnopqrstuvwxyzABCDEFGH012345678' -AsPlainText -Force
    $profile = Ensure-PandoraPlannerMysqlEnrollment -ProjectRoot $RepoRoot -ConfigPath $configPath `
        -IdentityPath $identityPath -CredentialRoot $credentialRoot -OutputPath $profilePath `
        -EnrollmentToken $token -ComputerName 'PLAN-PC-02' -UserName 'planner'
    Assert-True ($script:EnrollmentCalls -eq 1) '首次登记只发一次 enrollment 请求'
    Assert-True ($script:CapturedEnrollmentRequest.device_digest -ceq ('sha256:' + ('a' * 64))) '设备摘要只作 enrollment 幂等键'
    Assert-True ($script:CapturedEnrollmentRequest.display_name -ceq 'PLAN-PC-02\planner') '电脑名/用户名只作为可读标签发送'
    Assert-True ($script:CapturedEnrollmentRequest.PSObject.Properties.Name -notcontains 'expected_workspace_id') `
        '普通 43 字符首次码请求不携带 recovery workspace 字段'
    Assert-True ($profile.workspace_id -ceq $workspaceId -and $profile.mode -ceq 'central-managed') '服务端 workspace 成为本机 bind-once identity'
    Assert-True ($profile.credential_ref.version -eq 3 -and $profile.databases.pandora_account -ceq "pandora_account_w_$workspaceId") 'READY 凭据版本与十库映射进入无秘密 profile'
    $nonSecretText = (Get-Content -LiteralPath $identityPath -Raw) + (Get-Content -LiteralPath $profilePath -Raw)
    Assert-True ($nonSecretText -notmatch 'PANDORA_ENROLLED_PASSWORD|abcdefghijklmnopqrstuvwxyzABCDEFGH012345678') 'identity/profile 不落 token 或 runtime 密码'
    Assert-True (($profile | ConvertTo-Json -Depth 8) -notmatch 'PANDORA_ENROLLED_PASSWORD|abcdefghijklmnopqrstuvwxyzABCDEFGH012345678') `
        '首次登记返回值不泄漏 token 或 runtime 密码'

    $script:EnrollmentCalls = 0
    $again = Ensure-PandoraPlannerMysqlEnrollment -ProjectRoot $RepoRoot -ConfigPath $configPath `
        -IdentityPath $identityPath -CredentialRoot $credentialRoot -OutputPath $profilePath `
        -ComputerName 'RENAMED-PC' -UserName 'newname'
    Assert-True ($script:EnrollmentCalls -eq 0 -and $again.workspace_id -ceq $workspaceId) '已有 identity+DPAPI 凭据时以后双击不再请求 token'
    Assert-True ($again.display_name -ceq 'RENAMED-PC\newname') '改名只更新显示标签，不换 workspace'

    Write-Host '[3b] 显式 recovery code 恢复 exact workspace 并禁止凭据降级' -ForegroundColor Cyan
    $recoveryRaw = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefgh012345678'
    $recoveryCode = "$workspaceId.$recoveryRaw"
    $recoveryToken = ConvertTo-SecureString $recoveryCode -AsPlainText -Force
    $script:EnrollmentCalls = 0
    $script:EnrollmentSequence = @()
    $script:EnrollmentCredentialVersion = 4
    $script:EnrollmentPassword = 'PANDORA_ROTATED_PASSWORD_123456789'
    $recovered = Ensure-PandoraPlannerMysqlEnrollment -ProjectRoot $RepoRoot -ConfigPath $configPath `
        -IdentityPath $identityPath -CredentialRoot $credentialRoot -OutputPath $profilePath `
        -EnrollmentToken $recoveryToken -ComputerName 'PLAN-PC-02' -UserName 'planner'
    $recoveryRequestNames = @($script:CapturedEnrollmentRequest.PSObject.Properties.Name)
    Assert-True ($script:EnrollmentCalls -eq 1) '显式 recovery 绕过已有 credential 快速返回并请求中心'
    Assert-True ($script:CapturedEnrollmentRequest.enrollment_token -ceq $recoveryRaw) `
        '恢复请求只把 43 字符 raw token 放入 enrollment_token'
    Assert-True ($script:CapturedEnrollmentRequest.expected_workspace_id -ceq $workspaceId) `
        '恢复请求携带 exact expected_workspace_id'
    Assert-True ($recoveryRequestNames.Count -eq 5) `
        "恢复请求字段集合固定为五项（实际:$($recoveryRequestNames -join ',')）"
    Assert-True (($script:CapturedEnrollmentRequest | ConvertTo-Json -Compress) -notmatch [regex]::Escape($recoveryCode)) `
        '完整 recovery code 不进入 HTTP request object'
    $rotatedCredential = Get-PandoraPlannerDbCredential -WorkspaceId $workspaceId -Target $target -CredentialRoot $credentialRoot
    $rotatedPlain = [Net.NetworkCredential]::new('', $rotatedCredential.Password).Password
    Assert-True ($recovered.credential_ref.version -eq 4 -and $rotatedCredential.Version -eq 4 -and
        $rotatedPlain -ceq $script:EnrollmentPassword) '更高版本 recovery 原子更新 DPAPI 与无秘密 profile'

    $credentialBeforeDowngrade = [Convert]::ToHexString([IO.File]::ReadAllBytes($rotatedCredential.Path))
    $profileBeforeDowngrade = [Convert]::ToHexString([IO.File]::ReadAllBytes($profilePath))
    $script:EnrollmentCalls = 0
    $script:EnrollmentCredentialVersion = 3
    $script:EnrollmentPassword = 'PANDORA_STALE_PASSWORD_123456789'
    $downgradeBlocked = $false
    try {
        Ensure-PandoraPlannerMysqlEnrollment -ProjectRoot $RepoRoot -ConfigPath $configPath `
            -IdentityPath $identityPath -CredentialRoot $credentialRoot -OutputPath $profilePath `
            -EnrollmentToken $recoveryToken -ComputerName 'PLAN-PC-02' -UserName 'planner' | Out-Null
    } catch { $downgradeBlocked = $_.Exception.Message -match '降级|version|版本' }
    Assert-True ($downgradeBlocked -and $script:EnrollmentCalls -eq 1 -and
        [Convert]::ToHexString([IO.File]::ReadAllBytes($rotatedCredential.Path)) -ceq $credentialBeforeDowngrade -and
        [Convert]::ToHexString([IO.File]::ReadAllBytes($profilePath)) -ceq $profileBeforeDowngrade) `
        'recovery 返回较低 credential version 时拒绝且不改 DPAPI/profile'

    $script:EnrollmentCalls = 0
    $wrongRecoveryToken = ConvertTo-SecureString "$otherWorkspaceId.$recoveryRaw" -AsPlainText -Force
    $wrongRecoveryBlocked = $false
    try {
        Ensure-PandoraPlannerMysqlEnrollment -ProjectRoot $RepoRoot -ConfigPath $configPath `
            -IdentityPath $identityPath -CredentialRoot $credentialRoot -OutputPath $profilePath `
            -EnrollmentToken $wrongRecoveryToken -ComputerName 'PLAN-PC-02' -UserName 'planner' | Out-Null
    } catch { $wrongRecoveryBlocked = $_.Exception.Message -match 'workspace|恢复' }
    Assert-True ($wrongRecoveryBlocked -and $script:EnrollmentCalls -eq 0) `
        '恢复码 workspace 与 bind-once identity 不一致时在 HTTP 前阻断'

    $lostIdentityPath = Join-Path $sandbox 'lost-identity.json'
    $lostCredentialRoot = Join-Path $sandbox 'lost-credentials'
    $lostProfilePath = Join-Path $sandbox 'lost-profile.json'
    $script:EnrollmentCalls = 0
    $script:EnrollmentCredentialVersion = 4
    $script:EnrollmentPassword = 'PANDORA_ROTATED_PASSWORD_123456789'
    $restored = Ensure-PandoraPlannerMysqlEnrollment -ProjectRoot $RepoRoot -ConfigPath $configPath `
        -IdentityPath $lostIdentityPath -CredentialRoot $lostCredentialRoot -OutputPath $lostProfilePath `
        -EnrollmentToken $recoveryToken -ComputerName 'PLAN-PC-LOST' -UserName 'planner'
    Assert-True ($script:EnrollmentCalls -eq 1 -and $restored.workspace_id -ceq $workspaceId -and
        (Get-PandoraPlannerDbIdentity -IdentityPath $lostIdentityPath).workspace_id -ceq $workspaceId) `
        'identity 与 DPAPI 都丢失时可用 recovery code 恢复同一 workspace'

    $script:EnrollmentCredentialVersion = 3
    $script:EnrollmentPassword = 'PANDORA_ENROLLED_PASSWORD_123456'

    $script:EnrollmentCalls = 0
    function Get-PandoraPlannerDeviceDigest { return 'sha256:' + ('b' * 64) }
    $cloneBlocked = $false
    try {
        Ensure-PandoraPlannerMysqlEnrollment -ProjectRoot $RepoRoot -ConfigPath $configPath `
            -IdentityPath $identityPath -CredentialRoot $credentialRoot -OutputPath $profilePath `
            -ComputerName 'CLONED-PC' -UserName 'planner' | Out-Null
    } catch { $cloneBlocked = $_.Exception.Message -match 'device|克隆|改绑' }
    Assert-True ($cloneBlocked -and $script:EnrollmentCalls -eq 0) '复制 identity+credential 后 device digest 改变时拒绝自动改绑'
    function Get-PandoraPlannerDeviceDigest { return 'sha256:' + ('a' * 64) }

    Write-Host '[4] clone / 串库 / endpoint 漂移在写入前 fail-closed' -ForegroundColor Cyan
    Remove-Item -LiteralPath (Get-PandoraPlannerDbCredentialPath -WorkspaceId $workspaceId -CredentialRoot $credentialRoot) -Force
    $beforeIdentity = [Convert]::ToHexString([IO.File]::ReadAllBytes($identityPath))
    $script:EnrollmentWorkspace = $otherWorkspaceId
    $blocked = $false
    try {
        Ensure-PandoraPlannerMysqlEnrollment -ProjectRoot $RepoRoot -ConfigPath $configPath `
            -IdentityPath $identityPath -CredentialRoot $credentialRoot -OutputPath $profilePath `
            -EnrollmentToken $token -ComputerName 'PLAN-PC-02' -UserName 'planner' | Out-Null
    } catch { $blocked = $true }
    Assert-True ($blocked -and [Convert]::ToHexString([IO.File]::ReadAllBytes($identityPath)) -ceq $beforeIdentity) `
        '服务端返回不同 workspace 时拒绝覆盖长期 identity'

    $script:EnrollmentWorkspace = $workspaceId
    $script:EnrollmentEndpointHost = 'evil-db.intra'
    $blocked = $false
    $endpointError = ''
    try {
        Ensure-PandoraPlannerMysqlEnrollment -ProjectRoot $RepoRoot -ConfigPath $configPath `
            -IdentityPath $identityPath -CredentialRoot $credentialRoot -OutputPath $profilePath `
            -EnrollmentToken $token -ComputerName 'PLAN-PC-02' -UserName 'planner' | Out-Null
    } catch { $blocked = $true; $endpointError = "$($_.Exception.Message)" }
    Assert-True ($blocked -and $endpointError -notmatch 'PANDORA_ENROLLED_PASSWORD|abcdefghijklmnopqrstuvwxyzABCDEFGH012345678') `
        '服务端 endpoint 漂移错误只报脱敏上下文，不泄 token/密码'

    $script:EnrollmentEndpointHost = 'pandora-planner-db.intra'
    $script:BreakDatabaseMapping = $true
    $blocked = $false
    try {
        Ensure-PandoraPlannerMysqlEnrollment -ProjectRoot $RepoRoot -ConfigPath $configPath `
            -IdentityPath $identityPath -CredentialRoot $credentialRoot -OutputPath $profilePath `
            -EnrollmentToken $token -ComputerName 'PLAN-PC-02' -UserName 'planner' | Out-Null
    } catch { $blocked = $true }
    Assert-True $blocked '服务端十库映射缺失或回到 canonical 时拒绝串库'

    Write-Host '[5] 202 异步 provisioning 有界轮询，所有轮次复核 exact workspace' -ForegroundColor Cyan
    $script:BreakDatabaseMapping = $false
    $script:EnrollmentEndpointHost = 'pandora-planner-db.intra'
    $script:EnrollmentWorkspace = $workspaceId
    $script:EnrollmentCalls = 0
    $script:CapturedEnrollmentRequests = @()
    $script:EnrollmentSequence = @(
        [pscustomobject]@{ StatusCode=202;State='PROVISIONING';RetryAfterMs=2000;IncludeCredential=$false;WorkspaceId=$workspaceId },
        [pscustomobject]@{ StatusCode=202;State='MIGRATING';RetryAfterMs=1000;IncludeCredential=$false;WorkspaceId=$workspaceId },
        [pscustomobject]@{ StatusCode=200;State='READY';RetryAfterMs=0;IncludeCredential=$true;WorkspaceId=$workspaceId }
    )
    $script:FakeEnrollmentNow = [datetimeoffset]'2026-08-20T12:00:00Z'
    $script:EnrollmentDelays = @()
    function Get-PandoraPlannerEnrollmentUtcNow { return $script:FakeEnrollmentNow }
    function Wait-PandoraPlannerEnrollmentRetry([int]$Milliseconds) {
        $script:EnrollmentDelays += $Milliseconds
        $script:FakeEnrollmentNow = $script:FakeEnrollmentNow.AddMilliseconds($Milliseconds)
    }
    $asyncIdentity = Join-Path $sandbox 'async-identity.json'
    $asyncCredentials = Join-Path $sandbox 'async-credentials'
    $asyncProfile = Join-Path $sandbox 'async-profile.json'
    $ready = Ensure-PandoraPlannerMysqlEnrollment -ProjectRoot $RepoRoot -ConfigPath $configPath `
        -IdentityPath $asyncIdentity -CredentialRoot $asyncCredentials -OutputPath $asyncProfile `
        -EnrollmentToken $token -ComputerName 'PLAN-PC-03' -UserName 'planner' -EnrollmentTimeoutSeconds 600
    Assert-True ($script:EnrollmentCalls -eq 3 -and $ready.workspace_id -ceq $workspaceId) '202→202→200 收敛到同一 READY workspace'
    Assert-True (($script:EnrollmentDelays -join ',') -ceq '2000,1000') '严格采用服务端 1..5000ms retry_after_ms'
    Assert-True (@($script:CapturedEnrollmentRequests | Sort-Object -Unique).Count -eq 1) '每次轮询重放同一 token/device/display 请求'

    Remove-Item -LiteralPath $asyncIdentity, $asyncProfile -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $asyncCredentials -Recurse -Force -ErrorAction SilentlyContinue
    $script:EnrollmentCalls = 0
    $script:EnrollmentSequence = @(
        [pscustomobject]@{ StatusCode=202;State='MIGRATION_FAILED';RetryAfterMs=1000;IncludeCredential=$false;WorkspaceId=$workspaceId }
    )
    $blocked = $false
    try {
        Ensure-PandoraPlannerMysqlEnrollment -ProjectRoot $RepoRoot -ConfigPath $configPath `
            -IdentityPath $asyncIdentity -CredentialRoot $asyncCredentials -OutputPath $asyncProfile `
            -EnrollmentToken $token -ComputerName 'PLAN-PC-03' -UserName 'planner' | Out-Null
    } catch { $blocked = $_.Exception.Message -match 'MIGRATION_FAILED' }
    Assert-True ($blocked -and $script:EnrollmentCalls -eq 1) 'MIGRATION_FAILED 立即终止且不重试'

    $script:EnrollmentCalls = 0
    $script:EnrollmentSequence = @(
        [pscustomobject]@{ StatusCode=202;State='PROVISIONING';RetryAfterMs=1000;IncludeCredential=$false;WorkspaceId=$workspaceId },
        [pscustomobject]@{ StatusCode=202;State='MIGRATING';RetryAfterMs=1000;IncludeCredential=$false;WorkspaceId=$otherWorkspaceId }
    )
    $blocked = $false
    try {
        Ensure-PandoraPlannerMysqlEnrollment -ProjectRoot $RepoRoot -ConfigPath $configPath `
            -IdentityPath $asyncIdentity -CredentialRoot $asyncCredentials -OutputPath $asyncProfile `
            -EnrollmentToken $token -ComputerName 'PLAN-PC-03' -UserName 'planner' | Out-Null
    } catch { $blocked = $_.Exception.Message -match 'workspace.*漂移|workspace.*不一致' }
    Assert-True ($blocked -and $script:EnrollmentCalls -eq 2) '轮询中 workspace 漂移 fail-closed'

    $script:EnrollmentCalls = 0
    $script:FakeEnrollmentNow = [datetimeoffset]'2026-08-20T12:00:00Z'
    $script:EnrollmentSequence = @(
        [pscustomobject]@{ StatusCode=202;State='PROVISIONING';RetryAfterMs=5000;IncludeCredential=$false;WorkspaceId=$workspaceId }
    )
    $blocked = $false
    try {
        Ensure-PandoraPlannerMysqlEnrollment -ProjectRoot $RepoRoot -ConfigPath $configPath `
            -IdentityPath $asyncIdentity -CredentialRoot $asyncCredentials -OutputPath $asyncProfile `
            -EnrollmentToken $token -ComputerName 'PLAN-PC-03' -UserName 'planner' -EnrollmentTimeoutSeconds 3 | Out-Null
    } catch { $blocked = $_.Exception.Message -match '截止|超时' }
    Assert-True ($blocked -and $script:EnrollmentCalls -eq 1) '总截止不足下一次 retry 时有界超时'

    $script:EnrollmentSequence = @(
        [pscustomobject]@{ StatusCode=200;State='READY';RetryAfterMs=0;IncludeCredential=$false;WorkspaceId=$workspaceId }
    )
    $blocked = $false
    try {
        Ensure-PandoraPlannerMysqlEnrollment -ProjectRoot $RepoRoot -ConfigPath $configPath `
            -IdentityPath $asyncIdentity -CredentialRoot $asyncCredentials -OutputPath $asyncProfile `
            -EnrollmentToken $token -ComputerName 'PLAN-PC-03' -UserName 'planner' | Out-Null
    } catch { $blocked = $true }
    Assert-True $blocked '200 READY 缺 credential 时拒绝'

    $script:EnrollmentSequence = @(
        [pscustomobject]@{ StatusCode=202;State='MIGRATING';RetryAfterMs=1000;IncludeCredential=$true;WorkspaceId=$workspaceId }
    )
    $blocked = $false
    try {
        Ensure-PandoraPlannerMysqlEnrollment -ProjectRoot $RepoRoot -ConfigPath $configPath `
            -IdentityPath $asyncIdentity -CredentialRoot $asyncCredentials -OutputPath $asyncProfile `
            -EnrollmentToken $token -ComputerName 'PLAN-PC-03' -UserName 'planner' | Out-Null
    } catch { $blocked = $true }
    Assert-True $blocked '202 响应夹带 credential 时拒绝'

    Write-Host '[6] 真实 HTTP seam 不允许跳过 TLS，token 只能走 stdin body' -ForegroundColor Cyan
    $source = [IO.File]::ReadAllText($EnrollmentLib)
    Assert-True ($source -match "'--cacert'" -and $source -match "'--proto'" -and $source -match "'=https'") 'curl 固定使用 bundle CA 且只允许 HTTPS'
    Assert-True ($source -match "'--data-binary'\s*,?\s*'@-'" -or $source -match "'--data-binary'.*'@-'") 'enrollment JSON 只通过 stdin 送给 curl'
    Assert-True ($source -notmatch "'--fail-with-body'") '兼容 Windows 自带旧 curl，HTTP 状态由 write-out 独立 fail-closed'
    Assert-True ($source -notmatch '(?i)--insecure|skipcertificatecheck|''-k''') '没有跳过证书校验的逃生口'
} finally {
    Remove-Item -LiteralPath $sandbox -Recurse -Force -ErrorAction SilentlyContinue
}

if ($script:Failures.Count -gt 0) {
    Write-Host ''
    Write-Host "[FAIL] planner MySQL enrollment 契约失败 $($script:Failures.Count) 项" -ForegroundColor Red
    exit 1
}
Write-Host ''
Write-Host '[PASS] planner MySQL enrollment / DPAPI 契约' -ForegroundColor Green
