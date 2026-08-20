function Wait-PandoraPlannerServiceBatch {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][object[]]$States,
        [Parameter(Mandatory)][scriptblock]$GetListenerRecords,
        [Parameter(Mandatory)][scriptblock]$TestProcessExited,
        [Parameter(Mandatory)][scriptblock]$TestListenerOwned,
        [Parameter(Mandatory)][scriptblock]$Sleep,
        [Parameter(Mandatory)][scriptblock]$GetElapsedMilliseconds,
        [ValidateRange(1, 10000)][int]$PollMilliseconds = 100,
        [ValidateRange(1, 600000)][int]$TimeoutMilliseconds = 12000
    )
    Set-StrictMode -Version Latest

    while (@($States | Where-Object { -not $_.Ready -and -not $_.Failure }).Count -gt 0) {
        # 一轮只取一次 listener 快照。查询异常直接向上抛，调用方 fail-closed；不能把
        # “netstat 不可用”伪装成 22 个服务都没 ready。
        $listeners = @(& $GetListenerRecords)
        foreach ($state in @($States | Where-Object { -not $_.Ready -and -not $_.Failure })) {
            if ([bool](& $TestProcessExited $state)) {
                $state.Failure = 'process-exited'
                continue
            }
            if ([bool](& $TestListenerOwned $state $listeners)) {
                $state.Ready = $true
            }
        }

        $pending = @($States | Where-Object { -not $_.Ready -and -not $_.Failure })
        if ($pending.Count -eq 0) { return }
        if ([int64](& $GetElapsedMilliseconds) -ge $TimeoutMilliseconds) {
            foreach ($state in $pending) { $state.Failure = 'ready-timeout' }
            return
        }
        $null = & $Sleep $PollMilliseconds
    }
}

function Invoke-PandoraPlannerCleanupRecords {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][AllowEmptyCollection()][object[]]$Records,
        [Parameter(Mandatory)][scriptblock]$Cleanup
    )
    Set-StrictMode -Version Latest

    $errors = [Collections.Generic.List[string]]::new()
    foreach ($record in $Records) {
        try {
            $null = & $Cleanup $record
        } catch {
            $name = if ($record.PSObject.Properties['Service']) { "$($record.Service.Name)" } `
                elseif ($record.PSObject.Properties['Name']) { "$($record.Name)" } else { '<unknown>' }
            $errors.Add("${name}: $($_.Exception.Message)")
        }
    }
    return $errors.ToArray()
}

function Stop-PandoraPlannerExactProcess {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]$Process,
        [ValidateRange(1, 60000)][int]$TimeoutMilliseconds = 5000,
        # 必须沿用 Start-Process 返回的原 Process 对象/句柄。按 Id 再 lookup 会在
        # Refresh 与停止之间的 PID 复用窗口误杀无关进程。
        [scriptblock]$StopProcess = { param($ExactProcess) $ExactProcess.Kill() },
        [scriptblock]$Sleep = { param([int]$Milliseconds) Start-Sleep -Milliseconds $Milliseconds },
        [scriptblock]$GetElapsedMilliseconds
    )
    Set-StrictMode -Version Latest

    $processId = [int]$Process.Id
    if ($processId -le 0) { throw "无效的 exact Process PID:$processId" }
    $watch = [Diagnostics.Stopwatch]::StartNew()
    if (-not $GetElapsedMilliseconds) {
        $GetElapsedMilliseconds = { return [int64]$watch.ElapsedMilliseconds }
    }
    $stopError = ''
    $refreshError = ''
    $stopRequested = $false
    try {
        $Process.Refresh()
        if ([bool]$Process.HasExited) {
            return [pscustomobject][ordered]@{
                ProcessId = $processId; StopRequested = $false; ExitConfirmed = $true; Error = ''
            }
        }
    } catch {
        $refreshError = $_.Exception.Message
    }

    try {
        $null = & $StopProcess $Process
        $stopRequested = $true
    } catch {
        $stopError = $_.Exception.Message
    }
    while ($true) {
        try {
            $Process.Refresh()
            $refreshError = ''
            if ([bool]$Process.HasExited) {
                return [pscustomobject][ordered]@{
                    ProcessId = $processId; StopRequested = $stopRequested; ExitConfirmed = $true; Error = $stopError
                }
            }
        } catch {
            $refreshError = $_.Exception.Message
        }
        $elapsed = [int64](& $GetElapsedMilliseconds)
        if ($elapsed -ge $TimeoutMilliseconds) { break }
        $remaining = [int]($TimeoutMilliseconds - $elapsed)
        $null = & $Sleep ([Math]::Min(100, $remaining))
    }
    try {
        $Process.Refresh()
        $refreshError = ''
        if ([bool]$Process.HasExited) {
            return [pscustomobject][ordered]@{
                ProcessId = $processId; StopRequested = $stopRequested; ExitConfirmed = $true; Error = $stopError
            }
        }
    } catch {
        $refreshError = $_.Exception.Message
    } finally {
        $watch.Stop()
    }
    $details = @($stopError, $refreshError | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }) -join '；'
    if ([string]::IsNullOrWhiteSpace($details)) { $details = "${TimeoutMilliseconds}ms 内 HasExited 仍为 false" }
    return [pscustomobject][ordered]@{
        ProcessId = $processId; StopRequested = $stopRequested; ExitConfirmed = $false; Error = $details
    }
}

function Remove-PandoraPlannerExactPidFile {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$PidFile,
        [Parameter(Mandatory)][int]$ExpectedProcessId
    )
    Set-StrictMode -Version Latest
    if ($ExpectedProcessId -le 0) { throw "无效的 expected PID:$ExpectedProcessId" }
    if (-not (Test-Path -LiteralPath $PidFile)) { return }
    if (-not (Test-Path -LiteralPath $PidFile -PathType Leaf)) { throw "PID 登记不是普通文件:$PidFile" }
    $pidText = [IO.File]::ReadAllText($PidFile).Trim()
    $registeredPid = 0
    if (-not [int]::TryParse($pidText, [ref]$registeredPid) -or $registeredPid -ne $ExpectedProcessId) {
        throw "PID 登记与本轮 exact PID 不一致，拒绝删除:$PidFile（登记=$pidText，本轮=$ExpectedProcessId）"
    }
    Remove-Item -LiteralPath $PidFile -Force -ErrorAction Stop
    if (Test-Path -LiteralPath $PidFile) { throw "exact PID 登记删除后仍存在:$PidFile" }
}

function Test-PandoraPlannerBuildReceipt {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$ReceiptPath,
        [Parameter(Mandatory)][string]$Fingerprint,
        [Parameter(Mandatory)][string]$BinaryPath
    )
    Set-StrictMode -Version Latest

    if (-not (Test-Path -LiteralPath $ReceiptPath -PathType Leaf) -or
        -not (Test-Path -LiteralPath $BinaryPath -PathType Leaf)) { return $false }
    try {
        $receipt = [IO.File]::ReadAllText($ReceiptPath) | ConvertFrom-Json
        $binary = Get-Item -LiteralPath $BinaryPath -ErrorAction Stop
        return [int]$receipt.schema -eq 1 -and
            [string]::Equals("$($receipt.fingerprint)", $Fingerprint, [StringComparison]::Ordinal) -and
            [int64]$receipt.length -eq [int64]$binary.Length -and
            [int64]$receipt.last_write_utc_ticks -eq [int64]$binary.LastWriteTimeUtc.Ticks
    } catch {
        return $false
    }
}

function Write-PandoraPlannerBuildReceipt {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$ReceiptPath,
        [Parameter(Mandatory)][string]$Fingerprint,
        [Parameter(Mandatory)][string]$BinaryPath
    )
    Set-StrictMode -Version Latest

    $binary = Get-Item -LiteralPath $BinaryPath -ErrorAction Stop
    $directory = Split-Path -Parent $ReceiptPath
    New-Item -ItemType Directory -Force -Path $directory | Out-Null
    $payload = [ordered]@{
        schema = 1
        fingerprint = $Fingerprint
        length = [int64]$binary.Length
        last_write_utc_ticks = [int64]$binary.LastWriteTimeUtc.Ticks
    } | ConvertTo-Json -Compress
    $temporary = "$ReceiptPath.$PID.$([guid]::NewGuid().ToString('N')).tmp"
    try {
        [IO.File]::WriteAllText($temporary, "$payload`n", [Text.UTF8Encoding]::new($false))
        [IO.File]::Move($temporary, $ReceiptPath, $true)
    } finally {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
    }
}

function Get-PandoraPlannerGoInputFingerprint {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$ProjectRoot,
        [Parameter(Mandatory)][string]$ToolchainSignature,
        [string[]]$WorkspaceRoots = @('services', 'pkg', 'proto')
    )
    Set-StrictMode -Version Latest

    $rootFull = [IO.Path]::GetFullPath($ProjectRoot).TrimEnd('\', '/')
    $files = [Collections.Generic.List[IO.FileInfo]]::new()
    $seenFiles = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    $excludedDirectories = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($name in @('run', 'vendor', '.git', '.svn')) { $null = $excludedDirectories.Add($name) }
    foreach ($relativeRoot in $WorkspaceRoots) {
        $inputRoot = if ([IO.Path]::IsPathRooted($relativeRoot)) {
            [IO.Path]::GetFullPath($relativeRoot)
        } else {
            [IO.Path]::GetFullPath((Join-Path $rootFull $relativeRoot))
        }
        if (-not (Test-Path -LiteralPath $inputRoot -PathType Container)) { continue }
        $pendingDirectories = [Collections.Generic.Stack[string]]::new()
        $pendingDirectories.Push($inputRoot)
        while ($pendingDirectories.Count -gt 0) {
            $directory = $pendingDirectories.Pop()
            foreach ($item in @(Get-ChildItem -LiteralPath $directory -Force -ErrorAction Stop)) {
                if ($item -is [IO.DirectoryInfo]) {
                    # 真正剪枝：不进入日志/产物/vendor/版本库目录，避免用得越久扫得越慢。
                    if ($excludedDirectories.Contains($item.Name) -or
                        ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) { continue }
                    $pendingDirectories.Push($item.FullName)
                    continue
                }
                $file = [IO.FileInfo]$item
                if ($file.Extension -eq '.go' -or $file.Extension -eq '.proto' -or
                    $file.Name -in @('go.mod', 'go.sum')) {
                    if ($seenFiles.Add($file.FullName)) { $files.Add($file) }
                }
            }
        }
    }
    foreach ($rootFileName in @('go.work', 'go.work.sum', 'go.mod', 'go.sum')) {
        $rootFile = Join-Path $rootFull $rootFileName
        if (Test-Path -LiteralPath $rootFile -PathType Leaf) {
            $rootFileInfo = Get-Item -LiteralPath $rootFile -ErrorAction Stop
            if ($seenFiles.Add($rootFileInfo.FullName)) { $files.Add($rootFileInfo) }
        }
    }

    $ordered = @($files | Sort-Object { [IO.Path]::GetRelativePath($rootFull, $_.FullName) })
    $hash = [Security.Cryptography.IncrementalHash]::CreateHash([Security.Cryptography.HashAlgorithmName]::SHA256)
    $buffer = [byte[]]::new(131072)
    try {
        $header = [Text.Encoding]::UTF8.GetBytes("pandora-planner-go-input-v3`0$ToolchainSignature`0")
        $hash.AppendData($header)
        foreach ($file in $ordered) {
            $relative = [IO.Path]::GetRelativePath($rootFull, $file.FullName).Replace('\', '/').ToLowerInvariant()
            $pathBytes = [Text.Encoding]::UTF8.GetBytes("$relative`0$([int64]$file.Length)`0")
            $hash.AppendData($pathBytes)
            $stream = [IO.File]::Open($file.FullName, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::ReadWrite)
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
