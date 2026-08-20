# Pandora 中心 MySQL 服务运行态 YAML renderer。
#
# 仓库配置始终保持无秘密的 local dev DSN；central-managed 启动时，本模块在当前用户
# 私有临时目录渲染 exact workspace DSN/TLS/pool，服务完成启动读取后立即删除。

$enrollmentLib = Join-Path $PSScriptRoot 'planner_mysql_enrollment.ps1'
if (-not (Get-Command Get-PandoraPlannerDbCredential -ErrorAction SilentlyContinue)) {
    . $enrollmentLib
}
$localStateLib = Join-Path $PSScriptRoot 'local_infra_state.ps1'
if (-not (Get-Command Assert-PandoraOrchestrationLockHeld -ErrorAction SilentlyContinue)) {
    . $localStateLib
}

function Get-PandoraYamlLeadingSpaces([string]$Line) {
    if ($Line -match "`t") { throw '服务 YAML 含 tab 缩进，拒绝用空格层级 renderer 猜测' }
    if ($Line -match '^( *)') { return $Matches[1].Length }
    return 0
}

function Assert-PandoraCentralProfileCredentialPair {
    param(
        [Parameter(Mandatory)]$Profile,
        [Parameter(Mandatory)]$Credential,
        [Parameter(Mandatory)][string]$ProjectRoot
    )
    if ("$($Profile.mode)" -cne 'central-managed') { throw '服务 secret renderer 只接受 central-managed profile' }
    $workspaceId = "$($Profile.workspace_id)"
    Assert-PandoraPlannerWorkspaceId $workspaceId
    if ("$($Credential.WorkspaceId)" -cne $workspaceId -or
        "$($Credential.Provider)" -cne "$($Profile.credential_ref.provider)" -or
        "$($Credential.Target)" -cne "$($Profile.credential_ref.target)" -or
        [int64]$Credential.Version -ne [int64]$Profile.credential_ref.version) {
        throw 'DPAPI credential 与 profile workspace/provider/target/version 不一致'
    }
    if ($Credential.Password -isnot [Security.SecureString]) { throw 'runtime password 必须保持 SecureString 直到渲染瞬间' }
    $sets = @(Get-PandoraMysqlMigrationSets -ProjectRoot $ProjectRoot)
    Assert-PandoraMysqlProfileExactMembers $Profile.databases $sets 'profile databases'
    foreach ($set in $sets) {
        $physical = "$(Get-PandoraMysqlProfileMember $Profile.databases $set 'profile databases')"
        if ($physical -cne "${set}_w_$workspaceId") { throw "profile database mapping 漂移:$set=$physical" }
    }
    $unsigned = [pscustomobject][ordered]@{
        schema_version = 1
        mode = 'central-managed'
        workspace_id = $workspaceId
        display_name = "$($Profile.display_name)"
        endpoint = $Profile.endpoint
        credential_ref = $Profile.credential_ref
        databases = $Profile.databases
    }
    if ("$($Profile.fingerprint)" -cne (Get-PandoraMysqlProfileFingerprint $unsigned)) {
        throw 'profile fingerprint 校验失败，拒绝渲染 secret YAML'
    }
}

function ConvertTo-PandoraSingleQuotedYaml([string]$Value) {
    if ($Value -match '[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]') { throw 'YAML 值含控制字符' }
    return "'" + $Value.Replace("'", "''") + "'"
}

function Invoke-PandoraPlannerSecretSessionSweep {
    param([Parameter(Mandatory)][string]$ProjectRoot)
    $projectRootFull = [IO.Path]::GetFullPath($ProjectRoot).TrimEnd('\', '/')
    Assert-PandoraOrchestrationLockHeld -ProjectRoot $projectRootFull | Out-Null

    $failures = [Collections.Generic.List[string]]::new()
    $roots = @(
        [pscustomobject]@{
            Path = Join-Path $projectRootFull 'run/localinfra/cfg/service-secrets'
            Pattern = '^\d+-[a-z][a-z0-9_]{0,62}-[0-9a-f]{32}$'
        }
        [pscustomobject]@{
            Path = Join-Path $projectRootFull 'run/localinfra/cfg/preflight-secrets'
            Pattern = '^\d+-[0-9a-f]{32}$'
        }
    )
    foreach ($rootSpec in $roots) {
        $root = [IO.Path]::GetFullPath("$($rootSpec.Path)").TrimEnd('\', '/')
        $projectPrefix = $projectRootFull + [IO.Path]::DirectorySeparatorChar
        if (-not $root.StartsWith($projectPrefix, [StringComparison]::OrdinalIgnoreCase)) {
            $failures.Add("secret root 越界:$root")
            continue
        }
        if (-not (Test-Path -LiteralPath $root)) { continue }
        try {
            $rootInfo = Get-Item -LiteralPath $root -Force -ErrorAction Stop
            if (-not $rootInfo.PSIsContainer) { throw "secret root 不是目录:$root" }
            if (($rootInfo.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw "secret root 是 reparse point:$root"
            }
        } catch {
            $failures.Add($_.Exception.Message)
            continue
        }

        $children = @()
        try { $children = @(Get-ChildItem -LiteralPath $root -Force -ErrorAction Stop) }
        catch {
            $failures.Add("枚举 secret root 失败:$root。详情:$($_.Exception.Message)")
            continue
        }
        foreach ($child in $children) {
            $childFull = [IO.Path]::GetFullPath($child.FullName).TrimEnd('\', '/')
            $parent = Split-Path -Parent $childFull
            if (-not $child.PSIsContainer -or "$($child.Name)" -cnotmatch "$($rootSpec.Pattern)" -or
                -not (Test-PandoraPathEqual $parent $root) -or
                -not $childFull.StartsWith($root + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
                $failures.Add("拒绝清理非合法 secret session:$childFull")
                continue
            }
            if (($child.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                $failures.Add("拒绝清理 reparse point secret session:$childFull")
                continue
            }
            try {
                Remove-Item -LiteralPath $childFull -Recurse -Force -ErrorAction Stop
                if (Test-Path -LiteralPath $childFull) { throw "删除后仍存在:$childFull" }
            } catch {
                $failures.Add("secret session 清理失败:$childFull。详情:$($_.Exception.Message)")
            }
        }
        try {
            if ((Test-Path -LiteralPath $root -PathType Container) -and
                @(Get-ChildItem -LiteralPath $root -Force -ErrorAction Stop).Count -eq 0) {
                Remove-Item -LiteralPath $root -Force -ErrorAction Stop
                if (Test-Path -LiteralPath $root) { throw "空 secret root 删除后仍存在:$root" }
            }
        } catch {
            $failures.Add("空 secret root 清理失败:$root。详情:$($_.Exception.Message)")
        }
    }
    if ($failures.Count -gt 0) {
        throw "历史明文 secret session 清理失败，拒绝继续生成新 secret：$($failures -join ' | ')"
    }
}

function New-PandoraMysqlServiceRuntimeConfig {
    param(
        [Parameter(Mandatory)][string]$ProjectRoot,
        [Parameter(Mandatory)][string]$ServiceName,
        [Parameter(Mandatory)][string]$SourcePath,
        [Parameter(Mandatory)]$Profile,
        [Parameter(Mandatory)]$Credential,
        [string]$OutputDirectory = ''
    )
    $projectRootFull = [IO.Path]::GetFullPath($ProjectRoot).TrimEnd('\', '/')
    $sourceFull = [IO.Path]::GetFullPath($SourcePath)
    $rootPrefix = $projectRootFull + [IO.Path]::DirectorySeparatorChar
    if (-not $sourceFull.StartsWith($rootPrefix, [StringComparison]::OrdinalIgnoreCase)) {
        throw "服务配置必须位于当前仓库内:$sourceFull"
    }
    if ($ServiceName -cnotmatch '^[a-z][a-z0-9_]{0,62}$') { throw "service name 非法:$ServiceName" }
    if (-not (Test-Path -LiteralPath $sourceFull -PathType Leaf)) { throw "服务配置不存在:$sourceFull" }
    $sourceInfo = Get-Item -LiteralPath $sourceFull
    if ($sourceInfo.Length -gt 1MB) { throw "服务配置超过 1 MiB:$sourceFull" }
    Assert-PandoraCentralProfileCredentialPair -Profile $Profile -Credential $Credential -ProjectRoot $projectRootFull

    $text = [IO.File]::ReadAllText($sourceFull)
    $hadFinalNewline = $text.EndsWith("`n")
    $lines = [Collections.Generic.List[string]]::new()
    foreach ($line in @($text -split "\r?\n")) { $lines.Add($line) }
    $passwordText = ConvertFrom-PandoraSecureString $Credential.Password
    $outputRoot = ''
    $sessionDir = ''
    try {
        if ($passwordText -cnotmatch '^[A-Za-z0-9_-]{24,128}$') { throw 'runtime password 不是受支持的高熵 base64url 形态' }
        $username = "$($Credential.UserName)"
        if ($username -cne "p_app_$($Profile.workspace_id)" -or $username -cnotmatch '^[A-Za-z0-9_.-]{1,32}$') {
            throw 'runtime username 不属于 exact workspace'
        }
        $blocks = @{}
        $dsnCount = 0
        for ($i = 0; $i -lt $lines.Count; $i++) {
            $line = $lines[$i]
            if ($line -notmatch '^(?<indent> *)dsn:\s*(?<quote>["''])(?<dsn>[^"'']+)\k<quote>\s*(?:#.*)?$') { continue }
            $dsnIndentText = [string]$Matches['indent']
            $dsn = $Matches['dsn']
            if ($dsn -notmatch '^(?<user>[A-Za-z0-9_.-]+):(?<password>[^@\s]+)@tcp\((?<host>[^):\s]+):(?<port>[0-9]+)\)/(?<database>pandora_[a-z0-9_]+)\?(?<query>[^\s"'']+)$') {
                throw "服务 $ServiceName 的 MySQL DSN 形态无法安全解析"
            }
            $canonical = $Matches['database']
            $query = $Matches['query']
            if ($query -match '(?i)(?:^|&)tls=' -or $query -match '[\x00-\x20\x7f]') {
                throw "服务 $ServiceName 的源 DSN 带 TLS 逃生参数或控制字符"
            }
            $physical = $null
            try { $physical = "$(Get-PandoraMysqlProfileMember $Profile.databases $canonical 'profile databases')" } catch {
                throw "服务 $ServiceName 引用了不在 workspace mapping 中的库:$canonical"
            }
            if ($physical -cne "${canonical}_w_$($Profile.workspace_id)") {
                throw "服务 $ServiceName 的库映射不属于 exact workspace:$canonical=$physical"
            }
            $newDsn = "${username}:${passwordText}@tcp($($Profile.endpoint.host):$($Profile.endpoint.port))/${physical}?${query}"
            $lines[$i] = $dsnIndentText + 'dsn: ' + (ConvertTo-PandoraSingleQuotedYaml $newDsn)
            $dsnIndent = $dsnIndentText.Length
            $blockStart = -1
            for ($j = $i - 1; $j -ge 0; $j--) {
                $candidate = $lines[$j]
                $trimmed = $candidate.Trim()
                if ($trimmed -eq '' -or $trimmed.StartsWith('#')) { continue }
                $candidateIndent = Get-PandoraYamlLeadingSpaces $candidate
                if ($candidateIndent -lt $dsnIndent -and $trimmed.EndsWith(':')) {
                    if ($trimmed -cnotin @('mysql_client:', 'bag:')) {
                        throw "服务 $ServiceName 的 dsn 不在 mysql_client/bag 块内:$trimmed"
                    }
                    $blockStart = $j
                    break
                }
            }
            if ($blockStart -lt 0) { throw "服务 $ServiceName 无法定位 DSN 父块" }
            $blocks[$blockStart] = $dsnIndent
            $dsnCount++
        }

        if ($dsnCount -eq 0) {
            return [pscustomobject][ordered]@{
                Path = $sourceFull
                Root = ''
                Ephemeral = $false
                DsnCount = 0
            }
        }

        foreach ($blockStart in @($blocks.Keys | Sort-Object -Descending)) {
            $parentIndent = Get-PandoraYamlLeadingSpaces $lines[[int]$blockStart]
            $childIndent = [int]$blocks[$blockStart]
            $blockEnd = $lines.Count
            for ($j = [int]$blockStart + 1; $j -lt $lines.Count; $j++) {
                $trimmed = $lines[$j].Trim()
                if ($trimmed -eq '' -or $trimmed.StartsWith('#')) { continue }
                if ((Get-PandoraYamlLeadingSpaces $lines[$j]) -le $parentIndent) { $blockEnd = $j; break }
            }
            for ($j = $blockEnd - 1; $j -gt [int]$blockStart; $j--) {
                if ((Get-PandoraYamlLeadingSpaces $lines[$j]) -eq $childIndent -and
                    $lines[$j].Trim() -match '^(?:tls_ca_file|tls_server_name|max_open_conns|max_idle_conns):') {
                    $lines.RemoveAt($j)
                }
            }
            $dsnIndex = -1
            for ($j = [int]$blockStart + 1; $j -lt $lines.Count; $j++) {
                $trimmed = $lines[$j].Trim()
                if ($trimmed -eq '' -or $trimmed.StartsWith('#')) { continue }
                if ((Get-PandoraYamlLeadingSpaces $lines[$j]) -le $parentIndent) { break }
                if ($lines[$j] -match '^ *dsn:') { $dsnIndex = $j; break }
            }
            if ($dsnIndex -lt 0) { throw "服务 $ServiceName 的 MySQL block 在重写后丢失 dsn" }
            $indent = ' ' * $childIndent
            $inject = @(
                ($indent + 'tls_ca_file: ' + (ConvertTo-PandoraSingleQuotedYaml "$($Profile.endpoint.ca_file)"))
                ($indent + 'tls_server_name: ' + (ConvertTo-PandoraSingleQuotedYaml "$($Profile.endpoint.tls_server_name)"))
                ($indent + 'max_open_conns: 4')
                ($indent + 'max_idle_conns: 1')
            )
            for ($offset = 0; $offset -lt $inject.Count; $offset++) {
                $lines.Insert($dsnIndex + 1 + $offset, $inject[$offset])
            }
        }

        if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
            $OutputDirectory = Join-Path $projectRootFull 'run/localinfra/cfg/service-secrets'
        }
        $outputRoot = [IO.Path]::GetFullPath($OutputDirectory)
        New-Item -ItemType Directory -Path $outputRoot -Force | Out-Null
        Set-PandoraPlannerPrivateAcl -Path $outputRoot -Directory
        $sessionDir = Join-Path $outputRoot ("{0}-{1}-{2}" -f $PID, $ServiceName, [guid]::NewGuid().ToString('N'))
        New-Item -ItemType Directory -Path $sessionDir | Out-Null
        Set-PandoraPlannerPrivateAcl -Path $sessionDir -Directory
        $target = Join-Path $sessionDir "$ServiceName.yaml"
        $rendered = [string]::Join([Environment]::NewLine, $lines)
        if ($hadFinalNewline -and -not $rendered.EndsWith([Environment]::NewLine)) { $rendered += [Environment]::NewLine }
        [IO.File]::WriteAllText($target, $rendered, [Text.UTF8Encoding]::new($false))
        Set-PandoraPlannerPrivateAcl -Path $target
        return [pscustomobject][ordered]@{
            Path = $target
            Root = $sessionDir
            Ephemeral = $true
            DsnCount = $dsnCount
        }
    } catch {
        $renderError = $_.Exception.Message
        $cleanupError = ''
        if (-not [string]::IsNullOrWhiteSpace($sessionDir)) {
            try {
                $outputRootFull = [IO.Path]::GetFullPath($outputRoot).TrimEnd('\', '/')
                $sessionFull = [IO.Path]::GetFullPath($sessionDir).TrimEnd('\', '/')
                $expectedPrefix = $outputRootFull + [IO.Path]::DirectorySeparatorChar
                if ($sessionFull -ceq $outputRootFull -or
                    -not $sessionFull.StartsWith($expectedPrefix, [StringComparison]::OrdinalIgnoreCase)) {
                    throw "本轮 secret session 越界:$sessionFull"
                }
                if (Test-Path -LiteralPath $sessionFull) {
                    $sessionInfo = Get-Item -LiteralPath $sessionFull -Force -ErrorAction Stop
                    if (($sessionInfo.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                        throw "本轮 secret session 已变成 reparse point:$sessionFull"
                    }
                    Remove-Item -LiteralPath $sessionFull -Recurse -Force -ErrorAction Stop
                }
                if (Test-Path -LiteralPath $sessionFull) {
                    throw "本轮 secret session 删除后仍存在:$sessionFull"
                }
            } catch {
                $cleanupError = $_.Exception.Message
            }
        }
        if (-not [string]::IsNullOrWhiteSpace($cleanupError)) {
            throw "渲染 secret runtime YAML 失败:$renderError；本轮 session 清理也失败:$cleanupError"
        }
        throw
    } finally {
        $passwordText = $null
    }
}

function Remove-PandoraMysqlServiceRuntimeConfig {
    param([Parameter(Mandatory)]$RuntimeConfig)
    if (-not [bool]$RuntimeConfig.Ephemeral) { return }
    $path = [IO.Path]::GetFullPath("$($RuntimeConfig.Path)")
    $root = [IO.Path]::GetFullPath("$($RuntimeConfig.Root)").TrimEnd('\', '/')
    if ([string]::IsNullOrWhiteSpace($root) -or
        -not $path.StartsWith($root + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        throw '拒绝清理不属于本轮 secret runtime 目录的路径'
    }
    try { Remove-Item -LiteralPath $path -Force -ErrorAction Stop }
    catch { throw "删除 secret runtime YAML 失败，拒绝把启动误报成功:$path。详情:$($_.Exception.Message)" }
    if (Test-Path -LiteralPath $path) { throw "secret runtime YAML 删除后仍存在:$path" }
    if (Test-Path -LiteralPath $root -PathType Container) {
        $remaining = @(Get-ChildItem -LiteralPath $root -Force)
        if ($remaining.Count -ne 0) { throw "secret runtime 目录仍有残留，拒绝把清理误报成功:$root" }
        Remove-Item -LiteralPath $root -Force -ErrorAction Stop
        if (Test-Path -LiteralPath $root) { throw "secret runtime 目录删除后仍存在:$root" }
    }
}
