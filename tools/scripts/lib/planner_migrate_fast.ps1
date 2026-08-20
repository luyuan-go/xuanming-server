function Get-PandoraPlannerMysqlInitFiles {
    [CmdletBinding()]
    param([Parameter(Mandatory)][object[]]$Files)
    Set-StrictMode -Version Latest

    return @($Files | ForEach-Object {
            $path = if ($_ -is [IO.FileInfo]) { $_.FullName } else { "$_" }
            Get-Item -LiteralPath $path -ErrorAction Stop
        } | Sort-Object Name)
}

function Get-PandoraPlannerMysqlInitFingerprint {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][object[]]$Files,
        [Parameter(Mandatory)][string]$ProjectRoot
    )
    Set-StrictMode -Version Latest

    $rootFull = [IO.Path]::GetFullPath($ProjectRoot).TrimEnd('\', '/')
    $ordered = @(Get-PandoraPlannerMysqlInitFiles -Files $Files)
    $hash = [Security.Cryptography.IncrementalHash]::CreateHash([Security.Cryptography.HashAlgorithmName]::SHA256)
    $buffer = [byte[]]::new(65536)
    try {
        $hash.AppendData([Text.Encoding]::UTF8.GetBytes("pandora-planner-mysql-init-v1`0"))
        foreach ($file in $ordered) {
            $full = [IO.Path]::GetFullPath($file.FullName)
            if (-not $full.StartsWith("$rootFull\", [StringComparison]::OrdinalIgnoreCase)) {
                throw "mysql-init 文件越出项目根目录:$full"
            }
            $relative = $full.Substring($rootFull.Length + 1).Replace('\', '/').ToLowerInvariant()
            $hash.AppendData([Text.Encoding]::UTF8.GetBytes("$relative`0$([int64]$file.Length)`0"))
            $stream = [IO.File]::Open($full, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read)
            try {
                while (($count = $stream.Read($buffer, 0, $buffer.Length)) -gt 0) {
                    $hash.AppendData($buffer, 0, $count)
                }
            } finally {
                $stream.Dispose()
            }
            $hash.AppendData([byte[]](0))
        }
        return [Convert]::ToHexString($hash.GetHashAndReset())
    } finally {
        $hash.Dispose()
    }
}

function Join-PandoraPlannerMysqlInitScripts {
    [CmdletBinding()]
    param([Parameter(Mandatory)][object[]]$Files)
    Set-StrictMode -Version Latest

    $builder = [Text.StringBuilder]::new()
    foreach ($file in @(Get-PandoraPlannerMysqlInitFiles -Files $Files)) {
        $safeName = $file.Name.Replace("`r", '').Replace("`n", '')
        $null = $builder.AppendLine("-- pandora-init-file: $safeName")
        $null = $builder.Append([IO.File]::ReadAllText($file.FullName, [Text.Encoding]::UTF8))
        $null = $builder.AppendLine()
    }
    return $builder.ToString()
}

function Get-PandoraPlannerMysqlInitInventory {
    [CmdletBinding()]
    param([Parameter(Mandatory)][object[]]$Files)
    Set-StrictMode -Version Latest

    $databases = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    $tables = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($file in @(Get-PandoraPlannerMysqlInitFiles -Files $Files)) {
        $currentDatabase = ''
        foreach ($line in [IO.File]::ReadLines($file.FullName, [Text.Encoding]::UTF8)) {
            if ($line -match '^\s*CREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(?<db>[a-z][a-z0-9_]*)`?') {
                $null = $databases.Add($Matches.db)
                continue
            }
            if ($line -match '^\s*USE\s+`?(?<db>[a-z][a-z0-9_]*)`?\s*;') {
                $currentDatabase = $Matches.db
                $null = $databases.Add($currentDatabase)
                continue
            }
            if ($line -match '^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(?:`?(?<db>[a-z][a-z0-9_]*)`?)\.)?`?(?<table>[a-z][a-z0-9_]*)`?') {
                $qualifiedDatabase = if ($Matches.ContainsKey('db')) { "$($Matches['db'])" } else { '' }
                $tableName = "$($Matches['table'])"
                $database = if ($qualifiedDatabase) { $qualifiedDatabase } else { $currentDatabase }
                if (-not $database) { throw "$($file.Name) 中 CREATE TABLE $tableName 之前没有 USE/显式库名。" }
                $null = $databases.Add($database)
                $null = $tables.Add("$database.$tableName")
            }
        }
    }
    return [pscustomobject][ordered]@{
        Databases = @($databases | Sort-Object)
        Tables = @($tables | Sort-Object)
    }
}

function ConvertFrom-PandoraPlannerMysqlProbe {
    [CmdletBinding()]
    param([Parameter(Mandatory)][AllowEmptyCollection()][object[]]$Lines)
    Set-StrictMode -Version Latest

    $uuid = ''
    $dataDir = ''
    $databases = [Collections.Generic.List[string]]::new()
    $tables = [Collections.Generic.List[string]]::new()
    foreach ($value in $Lines) {
        $line = "$value".Trim()
        if ($line.StartsWith('__PANDORA_UUID__=', [StringComparison]::Ordinal)) {
            $uuid = $line.Substring('__PANDORA_UUID__='.Length).Trim()
        } elseif ($line.StartsWith('__PANDORA_DATADIR__=', [StringComparison]::Ordinal)) {
            $dataDir = $line.Substring('__PANDORA_DATADIR__='.Length).Trim()
        } elseif ($line.StartsWith('__PANDORA_DB__=', [StringComparison]::Ordinal)) {
            $databases.Add($line.Substring('__PANDORA_DB__='.Length).Trim())
        } elseif ($line.StartsWith('__PANDORA_TABLE__=', [StringComparison]::Ordinal)) {
            $tables.Add($line.Substring('__PANDORA_TABLE__='.Length).Trim())
        }
    }
    return [pscustomobject][ordered]@{
        ServerUuid = $uuid
        DataDir = $dataDir
        Databases = @($databases)
        Tables = @($tables)
    }
}

function Get-PandoraPlannerNormalizedDataDir([string]$Path) {
    Set-StrictMode -Version Latest
    return $Path.Trim().TrimEnd('\', '/').ToLowerInvariant()
}

function Test-PandoraPlannerMysqlInitReceipt {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$ReceiptPath,
        [Parameter(Mandatory)][string]$Fingerprint,
        [Parameter(Mandatory)][string]$ServerUuid,
        [Parameter(Mandatory)][string]$DataDir,
        [Parameter(Mandatory)][int]$FileCount,
        [Parameter(Mandatory)][string[]]$ExpectedDatabases,
        [Parameter(Mandatory)][string[]]$ExpectedTables,
        [Parameter(Mandatory)][string[]]$ActualDatabases,
        [Parameter(Mandatory)][string[]]$ActualTables
    )
    Set-StrictMode -Version Latest

    if (-not $ServerUuid -or -not $DataDir -or -not (Test-Path -LiteralPath $ReceiptPath -PathType Leaf)) {
        return $false
    }
    try {
        $receipt = [IO.File]::ReadAllText($ReceiptPath) | ConvertFrom-Json
        if ([int]$receipt.schema -ne 1 -or [int]$receipt.file_count -ne $FileCount -or
            -not [string]::Equals("$($receipt.fingerprint)", $Fingerprint, [StringComparison]::Ordinal) -or
            -not [string]::Equals("$($receipt.server_uuid)", $ServerUuid, [StringComparison]::OrdinalIgnoreCase) -or
            -not [string]::Equals("$($receipt.data_dir)", (Get-PandoraPlannerNormalizedDataDir $DataDir), [StringComparison]::Ordinal)) {
            return $false
        }
        $actualDbSet = [Collections.Generic.HashSet[string]]::new($ActualDatabases, [StringComparer]::OrdinalIgnoreCase)
        $actualTableSet = [Collections.Generic.HashSet[string]]::new($ActualTables, [StringComparer]::OrdinalIgnoreCase)
        foreach ($database in $ExpectedDatabases) { if (-not $actualDbSet.Contains($database)) { return $false } }
        foreach ($table in $ExpectedTables) { if (-not $actualTableSet.Contains($table)) { return $false } }
        return [int]$receipt.database_count -eq $ExpectedDatabases.Count -and
            [int]$receipt.table_count -eq $ExpectedTables.Count
    } catch {
        return $false
    }
}

function Write-PandoraPlannerMysqlInitReceipt {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$ReceiptPath,
        [Parameter(Mandatory)][string]$Fingerprint,
        [Parameter(Mandatory)][string]$ServerUuid,
        [Parameter(Mandatory)][string]$DataDir,
        [Parameter(Mandatory)][int]$FileCount,
        [Parameter(Mandatory)][int]$DatabaseCount,
        [Parameter(Mandatory)][int]$TableCount
    )
    Set-StrictMode -Version Latest

    if (-not $ServerUuid -or -not $DataDir) { throw '缺少 MySQL server_uuid/datadir，拒绝写入可能跨实例复用的收据。' }
    $directory = Split-Path -Parent $ReceiptPath
    New-Item -ItemType Directory -Force -Path $directory | Out-Null
    $payload = [ordered]@{
        schema = 1
        fingerprint = $Fingerprint
        server_uuid = $ServerUuid.ToLowerInvariant()
        data_dir = Get-PandoraPlannerNormalizedDataDir $DataDir
        file_count = $FileCount
        database_count = $DatabaseCount
        table_count = $TableCount
    } | ConvertTo-Json -Compress
    $temporary = "$ReceiptPath.$PID.$([guid]::NewGuid().ToString('N')).tmp"
    try {
        [IO.File]::WriteAllText($temporary, "$payload`n", [Text.UTF8Encoding]::new($false))
        [IO.File]::Move($temporary, $ReceiptPath, $true)
    } finally {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
    }
}
