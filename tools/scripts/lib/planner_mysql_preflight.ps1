# Pandora 中心 MySQL 启动前只读预检。
#
# 策划机只持有 runtime 账号；本模块只调用 pandora-migrate 的 -verify-only seam，
# 不执行 migration、advisory lock、DDL 或 bootstrap。密码仅写当前用户 ACL 的临时 DSN，
# 不进入参数、日志或 profile。

$runtimeConfigLib = Join-Path $PSScriptRoot 'mysql_service_runtime_config.ps1'
if (-not (Get-Command Assert-PandoraCentralProfileCredentialPair -ErrorAction SilentlyContinue)) {
    . $runtimeConfigLib
}

function Remove-PandoraPlannerPreflightSecretDirectory {
    param(
        [Parameter(Mandatory)][string]$SessionDirectory,
        [Parameter(Mandatory)][string]$SecretRoot
    )
    $session = [IO.Path]::GetFullPath($SessionDirectory).TrimEnd('\', '/')
    $root = [IO.Path]::GetFullPath($SecretRoot).TrimEnd('\', '/')
    if (-not $session.StartsWith($root + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        throw '拒绝清理不属于本轮中心 MySQL 预检的目录'
    }
    Remove-Item -LiteralPath $session -Recurse -Force -ErrorAction SilentlyContinue
    if (Test-Path -LiteralPath $session) { throw "中心 MySQL 预检 secret 未能清理:$session" }
    if ((Test-Path -LiteralPath $root -PathType Container) -and
        @(Get-ChildItem -LiteralPath $root -Force).Count -eq 0) {
        Remove-Item -LiteralPath $root -Force -ErrorAction Stop
    }
}

function Invoke-PandoraPlannerMysqlPreflight {
    param(
        [Parameter(Mandatory)][string]$ProjectRoot,
        [Parameter(Mandatory)]$Profile,
        [Parameter(Mandatory)]$Credential,
        [string]$MigrateCommand = '',
        [string]$SecretRoot = ''
    )
    $projectRootFull = [IO.Path]::GetFullPath($ProjectRoot)
    Assert-PandoraCentralProfileCredentialPair -Profile $Profile -Credential $Credential -ProjectRoot $projectRootFull
    if ([string]::IsNullOrWhiteSpace($MigrateCommand)) {
        $MigrateCommand = Join-Path $projectRootFull 'run/artifacts/windows/bin/pandora-migrate.exe'
    }
    if (-not [IO.Path]::IsPathFullyQualified($MigrateCommand) -or
        -not (Test-Path -LiteralPath $MigrateCommand -PathType Leaf)) {
        throw "缺少支持 -verify-only 的预编译迁移器:$MigrateCommand"
    }
    $MigrateCommand = [IO.Path]::GetFullPath($MigrateCommand)
    if ([string]::IsNullOrWhiteSpace($SecretRoot)) {
        $SecretRoot = Join-Path $projectRootFull 'run/localinfra/cfg/preflight-secrets'
    }
    $secretRootFull = [IO.Path]::GetFullPath($SecretRoot)
    $session = ''
    $passwordText = $null
    try {
        New-Item -ItemType Directory -Path $secretRootFull -Force | Out-Null
        Set-PandoraPlannerPrivateAcl -Path $secretRootFull -Directory
        $session = Join-Path $secretRootFull ("{0}-{1}" -f $PID, [guid]::NewGuid().ToString('N'))
        New-Item -ItemType Directory -Path $session | Out-Null
        Set-PandoraPlannerPrivateAcl -Path $session -Directory
        $passwordText = ConvertFrom-PandoraSecureString $Credential.Password
        if ($passwordText -cnotmatch '^[A-Za-z0-9_-]{24,128}$') { throw 'runtime password 不是受支持的高熵 base64url 形态' }
        $sets = @(Get-PandoraMysqlMigrationSets -ProjectRoot $projectRootFull)
        $targetEntries = @()
        $expected = @()
        foreach ($set in $sets) {
            $physical = "$(Get-PandoraMysqlProfileMember $Profile.databases $set 'profile databases')"
            if ($physical -cne "${set}_w_$($Profile.workspace_id)") {
                throw "预检目标不属于 exact workspace:$set=$physical"
            }
            $targetName = ($set -replace '_', '-') + '-planner'
            $dsnFileName = "$set.dsn"
            $dsnPath = Join-Path $session $dsnFileName
            $dsn = "$($Credential.UserName):${passwordText}@tcp($($Profile.endpoint.host):$($Profile.endpoint.port))/${physical}?parseTime=true&loc=UTC&tls=true"
            [IO.File]::WriteAllText($dsnPath, $dsn, [Text.Encoding]::ASCII)
            Set-PandoraPlannerPrivateAcl -Path $dsnPath
            $targetEntries += [ordered]@{
                name = $targetName
                migration_set = $set
                database = $physical
                dsn_file = $dsnFileName
                tls_ca_file = "$($Profile.endpoint.ca_file)"
                timeout_seconds = 20
                lock_wait_timeout_seconds = 5
            }
            $expected += "${targetName}:${set}:${physical}"
        }
        $targetsFile = Join-Path $session 'targets.json'
        [IO.File]::WriteAllText($targetsFile,
            (@{ targets = $targetEntries } | ConvertTo-Json -Depth 5), [Text.UTF8Encoding]::new($false))
        Set-PandoraPlannerPrivateAcl -Path $targetsFile
        $args = @(
            "-targets-file=$targetsFile"
            "-expected-targets=$($expected -join ',')"
            '-environment=central-planner'
            "-workspace-id=$($Profile.workspace_id)"
            '-verify-only'
        )
        $oldNativePref = $PSNativeCommandUseErrorActionPreference
        try {
            $PSNativeCommandUseErrorActionPreference = $false
            $output = @(& $MigrateCommand @args 2>&1 | ForEach-Object { "$_" })
            $exitCode = $LASTEXITCODE
        } finally {
            $PSNativeCommandUseErrorActionPreference = $oldNativePref
        }
        $safeOutput = @($output | ForEach-Object {
            $_.Replace($passwordText, '<redacted>').Replace("$($Credential.UserName):<redacted>@", '<redacted-dsn>@')
        })
        if ($exitCode -ne 0) {
            throw "中心 MySQL TLS/认证/schema 只读预检失败(exit=$exitCode)。详情:$((@($safeOutput | Select-Object -First 10)) -join ' | ')"
        }
        return [pscustomobject][ordered]@{
            Succeeded = $true
            WorkspaceId = "$($Profile.workspace_id)"
            ProfileFingerprint = "$($Profile.fingerprint)"
            TargetCount = $sets.Count
            Output = $safeOutput
        }
    } finally {
        $passwordText = $null
        if (-not [string]::IsNullOrWhiteSpace($session)) {
            Remove-PandoraPlannerPreflightSecretDirectory -SessionDirectory $session -SecretRoot $secretRootFull
        }
    }
}
