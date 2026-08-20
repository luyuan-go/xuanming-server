function Get-PandoraPlannerConfigTableConsumerNames {
    [CmdletBinding()]
    param()
    Set-StrictMode -Version Latest

    # 与服务端实际 Load/Reload 配置表的边界保持显式；不要用“看起来可能依赖”扩大重启面。
    return @(
        'player',
        'battle_result',
        'ds_allocator',
        'inventory',
        'dialogue',
        'mission',
        'matchmaker',
        'matchmaker_pve'
    )
}

function Get-PandoraPlannerBuildTargets {
    [CmdletBinding()]
    param([Parameter(Mandatory)][object[]]$Services)
    Set-StrictMode -Version Latest

    $byKey = [Collections.Specialized.OrderedDictionary]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($service in $Services) {
        $name = "$($service.Name)"
        $dir = "$($service.Dir)".Replace('\', '/').TrimEnd('/')
        $cmd = "$($service.Cmd)"
        if ([string]::IsNullOrWhiteSpace($name) -or [string]::IsNullOrWhiteSpace($dir) -or
            [string]::IsNullOrWhiteSpace($cmd)) {
            throw '服务缺少 Name/Dir/Cmd，无法建立 build target。'
        }
        $declaredTarget = if ($service.PSObject.Properties['BuildTarget']) { "$($service.BuildTarget)" } else { $name }
        if ([string]::IsNullOrWhiteSpace($declaredTarget)) { throw "服务 $name 的 BuildTarget 为空。" }
        $key = "$($dir.ToLowerInvariant())|$($cmd.ToLowerInvariant())"
        if ($byKey.Contains($key)) {
            $target = $byKey[$key]
            if (-not [string]::Equals($target.Name, $declaredTarget, [StringComparison]::OrdinalIgnoreCase)) {
                throw "同一 Go 构建目标 $dir/cmd/$cmd 声明了不同名称:$($target.Name),$declaredTarget"
            }
            $target.Services.Add($service)
            continue
        }
        $byKey.Add($key, [pscustomobject][ordered]@{
                Name = $declaredTarget
                Key = $key
                Dir = $dir
                Cmd = $cmd
                Services = [Collections.Generic.List[object]]::new()
            })
        $byKey[$key].Services.Add($service)
    }
    return @($byKey.Values)
}

function Get-PandoraPlannerArtifactTargetPlan {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][object[]]$BuildTargets,
        [Parameter(Mandatory)]$Manifest
    )
    Set-StrictMode -Version Latest

    if (-not $Manifest.PSObject.Properties['binaries']) { throw '预编译 manifest 缺少 binaries。' }
    $entries = [Collections.Generic.Dictionary[string, object]]::new([StringComparer]::Ordinal)
    foreach ($entry in @($Manifest.binaries)) {
        $name = if ($entry.PSObject.Properties['name']) { "$($entry.name)" } else { '' }
        if ([string]::IsNullOrWhiteSpace($name)) { throw '预编译 manifest 含空 name entry。' }
        if ($entries.ContainsKey($name)) { throw "预编译 manifest 含重复 entry:$name" }
        $entries.Add($name, $entry)
    }

    $result = [Collections.Generic.List[object]]::new()
    foreach ($target in $BuildTargets) {
        $targetEntries = [Collections.Generic.List[object]]::new()
        foreach ($service in @($target.Services)) {
            $runtimeName = "$($service.Name)"
            if (-not $entries.ContainsKey($runtimeName)) {
                throw "预编译 manifest 缺少 $runtimeName，拒绝混用 artifact 与现场 build。"
            }
            $entry = $entries[$runtimeName]
            $sha = if ($entry.PSObject.Properties['sha256']) { "$($entry.sha256)".ToUpperInvariant() } else { '' }
            $size = -1L
            if ($sha -cnotmatch '^[0-9A-F]{64}$' -or -not $entry.PSObject.Properties['size'] -or
                -not [int64]::TryParse("$($entry.size)", [ref]$size) -or $size -lt 0) {
                throw "预编译 manifest entry 非法:$runtimeName"
            }
            $targetEntries.Add([pscustomobject]@{ Name = $runtimeName; Sha256 = $sha; Size = $size })
        }
        $first = $targetEntries[0]
        $split = @($targetEntries | Where-Object {
                -not [string]::Equals($_.Sha256, $first.Sha256, [StringComparison]::Ordinal) -or
                [int64]$_.Size -ne [int64]$first.Size
            })
        if ($split.Count -gt 0) {
            throw "共享 build target $($target.Name) 的 artifact entry SHA/size 不一致，拒绝混版。"
        }
        $source = @($targetEntries | Where-Object Name -ceq "$($target.Name)") | Select-Object -First 1
        if (-not $source) { $source = $first }
        $result.Add([pscustomobject][ordered]@{
                Name = "$($target.Name)"
                Target = $target
                ArtifactName = "$($source.Name)"
                Sha256 = "$($first.Sha256)"
                Size = [int64]$first.Size
                Fingerprint = "artifact-target-v2:$($target.Name):$($first.Sha256):$([int64]$first.Size)"
            })
    }
    return $result.ToArray()
}

function Get-PandoraPlannerGoTargetPlan {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$ProjectRoot,
        [Parameter(Mandatory)][object[]]$BuildTargets,
        [Parameter(Mandatory)][object[]]$PackageRecords,
        [Parameter(Mandatory)][string]$ToolchainSignature,
        [Parameter(Mandatory)][AllowEmptyCollection()][string[]]$GlobalInputPaths,
        [scriptblock]$GetContentHash
    )
    Set-StrictMode -Version Latest

    if ([string]::IsNullOrWhiteSpace($ToolchainSignature)) {
        throw 'Go 工具链签名为空，拒绝生成可复用指纹。'
    }
    if (-not $GetContentHash) {
        $GetContentHash = { param([string]$Path) (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash }
    }
    $rootFull = [IO.Path]::GetFullPath($ProjectRoot).TrimEnd('\', '/')
    $packageByImport = [Collections.Generic.Dictionary[string, object]]::new([StringComparer]::Ordinal)
    foreach ($package in $PackageRecords) {
        $importPath = if ($package.PSObject.Properties['ImportPath']) { "$($package.ImportPath)" } else { '' }
        if ([string]::IsNullOrWhiteSpace($importPath)) { throw 'go list 返回了空 ImportPath package。' }
        if ($packageByImport.ContainsKey($importPath)) { throw "go list 返回重复 package:$importPath" }
        $packageByImport.Add($importPath, $package)
    }

    # 一个函数调用代表一次输入快照；所有 target 共用此 cache，任何文件最多读取/hash 一次。
    $hashByPath = [Collections.Generic.Dictionary[string, string]]::new([StringComparer]::OrdinalIgnoreCase)
    function Get-CachedContentHash([string]$Path) {
        $full = [IO.Path]::GetFullPath($Path)
        if (-not $hashByPath.ContainsKey($full)) {
            if (-not (Test-Path -LiteralPath $full -PathType Leaf)) { throw "Go 构建输入不存在:$full" }
            $value = "$(& $GetContentHash $full)".ToUpperInvariant()
            if ($value -cnotmatch '^[0-9A-F]{64}$') { throw "Go 构建输入 hash 非 SHA256:$full" }
            $hashByPath.Add($full, $value)
        }
        return $hashByPath[$full]
    }
    function Get-StablePathLabel([string]$Path) {
        $full = [IO.Path]::GetFullPath($Path)
        $prefix = "$rootFull$([IO.Path]::DirectorySeparatorChar)"
        if ($full.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) {
            return [IO.Path]::GetRelativePath($rootFull, $full).Replace('\', '/').ToLowerInvariant()
        }
        return "external:$($full.Replace('\', '/').ToLowerInvariant())"
    }

    $globalDescriptors = [Collections.Generic.List[string]]::new()
    foreach ($path in @($GlobalInputPaths | Sort-Object -Unique)) {
        $full = [IO.Path]::GetFullPath($path)
        $globalDescriptors.Add("global:$((Get-StablePathLabel $full)):$((Get-CachedContentHash $full))")
    }
    $sourceFields = @(
        'GoFiles', 'CgoFiles', 'CFiles', 'CXXFiles', 'MFiles', 'HFiles', 'FFiles', 'SFiles',
        'SwigFiles', 'SwigCXXFiles', 'SysoFiles', 'EmbedFiles'
    )
    $result = [Collections.Generic.List[object]]::new()
    foreach ($target in $BuildTargets) {
        $expectedMainDir = [IO.Path]::GetFullPath((Join-Path (Join-Path $rootFull "$($target.Dir)") "cmd/$($target.Cmd)"))
        $rootPackage = @($PackageRecords | Where-Object {
                $_.PSObject.Properties['Dir'] -and
                [string]::Equals(
                    [IO.Path]::GetFullPath("$($_.Dir)").TrimEnd('\', '/'),
                    $expectedMainDir.TrimEnd('\', '/'),
                    [StringComparison]::OrdinalIgnoreCase)
            }) | Select-Object -First 1
        if (-not $rootPackage) { throw "go list 结果缺少 build target $($target.Name) 的 main package:$expectedMainDir" }
        $imports = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
        $null = $imports.Add("$($rootPackage.ImportPath)")
        if ($rootPackage.PSObject.Properties['Deps']) {
            foreach ($dependency in @($rootPackage.Deps)) { $null = $imports.Add("$dependency") }
        }

        $descriptors = [Collections.Generic.List[string]]::new()
        foreach ($global in $globalDescriptors) { $descriptors.Add($global) }
        foreach ($importPath in @($imports | Sort-Object)) {
            if (-not $packageByImport.ContainsKey($importPath)) {
                throw "go list 依赖闭包缺少 package 记录:$importPath（target=$($target.Name)）"
            }
            $package = $packageByImport[$importPath]
            $isToolchainPackage = ($package.PSObject.Properties['Standard'] -and [bool]$package.Standard) -or
                ($package.PSObject.Properties['Goroot'] -and [bool]$package.Goroot)
            if ($isToolchainPackage) { continue }
            $module = if ($package.PSObject.Properties['Module']) { $package.Module } else { $null }
            if ($module) {
                $main = $module.PSObject.Properties['Main'] -and [bool]$module.Main
                $version = if ($module.PSObject.Properties['Version']) { "$($module.Version)" } else { '' }
                $sum = if ($module.PSObject.Properties['Sum']) { "$($module.Sum)" } else { '' }
                $modulePath = if ($module.PSObject.Properties['Path']) { "$($module.Path)" } else { '' }
                $replace = if ($module.PSObject.Properties['Replace']) { $module.Replace } else { $null }
                $replaceVersion = if ($replace -and $replace.PSObject.Properties['Version']) { "$($replace.Version)" } else { '' }
                $replaceDir = if ($replace -and $replace.PSObject.Properties['Dir']) { "$($replace.Dir)" } else { '' }
                # 本地 replace 没有 Version，必须继续 hash 真文件；版本化 module/replace 由 Go
                # 校验的 Path/Version/Sum 身份决定，扫描 module cache 既慢又会绑定机器绝对路径。
                $localReplace = $replace -and [string]::IsNullOrWhiteSpace($replaceVersion) -and
                    -not [string]::IsNullOrWhiteSpace($replaceDir)
                $externalModule = -not $main -and -not $localReplace -and
                    (-not [string]::IsNullOrWhiteSpace($version) -or -not [string]::IsNullOrWhiteSpace($replaceVersion))
                if ($externalModule) {
                    $replacePath = if ($replace -and $replace.PSObject.Properties['Path']) { "$($replace.Path)" } else { '' }
                    $replaceSum = if ($replace -and $replace.PSObject.Properties['Sum']) { "$($replace.Sum)" } else { '' }
                    $descriptors.Add("external-package:${importPath}:$modulePath@$version#$sum=>${replacePath}@$replaceVersion#$replaceSum")
                    continue
                }
            }
            $packageDir = if ($package.PSObject.Properties['Dir']) { "$($package.Dir)" } else { '' }
            if ([string]::IsNullOrWhiteSpace($packageDir)) {
                throw "go list package 缺少 Dir:$importPath"
            }
            $seenPackageFiles = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
            foreach ($field in $sourceFields) {
                if (-not $package.PSObject.Properties[$field]) { continue }
                foreach ($fileName in @($package.$field)) {
                    if ([string]::IsNullOrWhiteSpace("$fileName")) { continue }
                    $full = if ([IO.Path]::IsPathRooted("$fileName")) {
                        [IO.Path]::GetFullPath("$fileName")
                    } else {
                        [IO.Path]::GetFullPath((Join-Path $packageDir "$fileName"))
                    }
                    if (-not $seenPackageFiles.Add($full)) { continue }
                    $descriptors.Add("package:${importPath}:$((Get-StablePathLabel $full)):$((Get-CachedContentHash $full))")
                }
            }
        }

        $hash = [Security.Cryptography.IncrementalHash]::CreateHash([Security.Cryptography.HashAlgorithmName]::SHA256)
        try {
            $hash.AppendData([Text.Encoding]::UTF8.GetBytes("pandora-planner-go-target-v1`0$($target.Name)`0$ToolchainSignature`0"))
            foreach ($descriptor in @($descriptors | Sort-Object)) {
                $hash.AppendData([Text.Encoding]::UTF8.GetBytes("$descriptor`n"))
            }
            $fingerprint = "go-target-v1:$([Convert]::ToHexString($hash.GetHashAndReset()))"
        } finally {
            $hash.Dispose()
        }
        $result.Add([pscustomobject][ordered]@{
                Name = "$($target.Name)"
                Target = $target
                Fingerprint = $fingerprint
                InputCount = $descriptors.Count
            })
    }
    return $result.ToArray()
}

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
        [string]$BinaryPath,
        [string[]]$BinaryPaths
    )
    Set-StrictMode -Version Latest

    [string[]]$paths = @()
    if ($BinaryPaths -and @($BinaryPaths).Count -gt 0) { $paths = @($BinaryPaths) }
    elseif ($BinaryPath) { $paths = @($BinaryPath) }
    if ($paths.Count -eq 0 -or -not (Test-Path -LiteralPath $ReceiptPath -PathType Leaf)) { return $false }
    foreach ($path in $paths) {
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { return $false }
    }
    try {
        $receipt = [IO.File]::ReadAllText($ReceiptPath) | ConvertFrom-Json
        if (-not [string]::Equals("$($receipt.fingerprint)", $Fingerprint, [StringComparison]::Ordinal)) { return $false }
        if ([int]$receipt.schema -eq 1 -and $paths.Count -eq 1) {
            $binary = Get-Item -LiteralPath $paths[0] -ErrorAction Stop
            return [int64]$receipt.length -eq [int64]$binary.Length -and
                [int64]$receipt.last_write_utc_ticks -eq [int64]$binary.LastWriteTimeUtc.Ticks
        }
        if ([int]$receipt.schema -ne 2 -or -not $receipt.PSObject.Properties['binaries']) { return $false }
        $recordByName = [Collections.Generic.Dictionary[string, object]]::new([StringComparer]::OrdinalIgnoreCase)
        foreach ($record in @($receipt.binaries)) {
            $name = "$($record.name)"
            if ([string]::IsNullOrWhiteSpace($name) -or $recordByName.ContainsKey($name)) { return $false }
            $recordByName.Add($name, $record)
        }
        if ($recordByName.Count -ne $paths.Count) { return $false }
        foreach ($path in $paths) {
            $binary = Get-Item -LiteralPath $path -ErrorAction Stop
            if (-not $recordByName.ContainsKey($binary.Name)) { return $false }
            $record = $recordByName[$binary.Name]
            if ([int64]$record.length -ne [int64]$binary.Length -or
                [int64]$record.last_write_utc_ticks -ne [int64]$binary.LastWriteTimeUtc.Ticks) { return $false }
        }
        return $true
    } catch {
        return $false
    }
}

function Write-PandoraPlannerBuildReceipt {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$ReceiptPath,
        [Parameter(Mandatory)][string]$Fingerprint,
        [string]$BinaryPath,
        [string[]]$BinaryPaths
    )
    Set-StrictMode -Version Latest

    [string[]]$paths = @()
    if ($BinaryPaths -and @($BinaryPaths).Count -gt 0) { $paths = @($BinaryPaths) }
    elseif ($BinaryPath) { $paths = @($BinaryPath) }
    if ($paths.Count -eq 0) { throw 'build receipt 至少需要一个 BinaryPath。' }
    [object[]]$records = @($paths | ForEach-Object {
            $binary = Get-Item -LiteralPath $_ -ErrorAction Stop
            [pscustomobject][ordered]@{
                name = $binary.Name
                length = [int64]$binary.Length
                last_write_utc_ticks = [int64]$binary.LastWriteTimeUtc.Ticks
            }
        } | Sort-Object name)
    if (@($records | Group-Object name | Where-Object Count -ne 1).Count -gt 0) {
        throw 'build receipt 的 BinaryPaths 含重复文件名。'
    }
    $directory = Split-Path -Parent $ReceiptPath
    New-Item -ItemType Directory -Force -Path $directory | Out-Null
    $payload = [ordered]@{
        schema = 2
        fingerprint = $Fingerprint
        binaries = $records
    } | ConvertTo-Json -Depth 4 -Compress
    $temporary = "$ReceiptPath.$PID.$([guid]::NewGuid().ToString('N')).tmp"
    try {
        [IO.File]::WriteAllText($temporary, "$payload`n", [Text.UTF8Encoding]::new($false))
        [IO.File]::Move($temporary, $ReceiptPath, $true)
    } finally {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
    }
}

function Test-PandoraPlannerAppliedReceipt {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$ReceiptPath,
        [Parameter(Mandatory)][string]$Fingerprint,
        [Parameter(Mandatory)][string]$BinaryPath,
        [Parameter(Mandatory)]$Process
    )
    Set-StrictMode -Version Latest

    if (-not (Test-Path -LiteralPath $ReceiptPath -PathType Leaf) -or
        -not (Test-Path -LiteralPath $BinaryPath -PathType Leaf)) { return $false }
    try {
        $receipt = [IO.File]::ReadAllText($ReceiptPath) | ConvertFrom-Json
        $binary = Get-Item -LiteralPath $BinaryPath -ErrorAction Stop
        $startTicks = [int64]$Process.StartTime.ToUniversalTime().Ticks
        return [int]$receipt.schema -eq 1 -and
            [string]::Equals("$($receipt.fingerprint)", $Fingerprint, [StringComparison]::Ordinal) -and
            [int64]$receipt.length -eq [int64]$binary.Length -and
            [int64]$receipt.last_write_utc_ticks -eq [int64]$binary.LastWriteTimeUtc.Ticks -and
            [int]$receipt.process_id -eq [int]$Process.Id -and
            [int64]$receipt.process_start_utc_ticks -eq $startTicks
    } catch {
        return $false
    }
}

function Write-PandoraPlannerAppliedReceipt {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$ReceiptPath,
        [Parameter(Mandatory)][string]$Fingerprint,
        [Parameter(Mandatory)][string]$BinaryPath,
        [Parameter(Mandatory)]$Process
    )
    Set-StrictMode -Version Latest

    $binary = Get-Item -LiteralPath $BinaryPath -ErrorAction Stop
    $payload = [ordered]@{
        schema = 1
        fingerprint = $Fingerprint
        length = [int64]$binary.Length
        last_write_utc_ticks = [int64]$binary.LastWriteTimeUtc.Ticks
        process_id = [int]$Process.Id
        process_start_utc_ticks = [int64]$Process.StartTime.ToUniversalTime().Ticks
    } | ConvertTo-Json -Compress
    $directory = Split-Path -Parent $ReceiptPath
    New-Item -ItemType Directory -Force -Path $directory | Out-Null
    $temporary = "$ReceiptPath.$PID.$([guid]::NewGuid().ToString('N')).tmp"
    try {
        [IO.File]::WriteAllText($temporary, "$payload`n", [Text.UTF8Encoding]::new($false))
        [IO.File]::Move($temporary, $ReceiptPath, $true)
    } finally {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
    }
}

function Get-PandoraPlannerRuntimeActionPlan {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][object[]]$BuildTargets,
        [Parameter(Mandatory)][object[]]$TargetStates,
        [Parameter(Mandatory)][object[]]$RuntimeStates,
        [switch]$ConfigTableChanged
    )
    Set-StrictMode -Version Latest

    $targetStateByName = [Collections.Generic.Dictionary[string, object]]::new([StringComparer]::Ordinal)
    foreach ($state in $TargetStates) {
        $name = "$($state.Name)"
        if ($targetStateByName.ContainsKey($name)) { throw "重复 build target state:$name" }
        $targetStateByName.Add($name, $state)
    }
    $runtimeStateByName = [Collections.Generic.Dictionary[string, object]]::new([StringComparer]::Ordinal)
    foreach ($state in $RuntimeStates) {
        $name = "$($state.Name)"
        if ($runtimeStateByName.ContainsKey($name)) { throw "重复 runtime state:$name" }
        $runtimeStateByName.Add($name, $state)
    }
    $tableConsumers = [Collections.Generic.HashSet[string]]::new(
        [string[]](Get-PandoraPlannerConfigTableConsumerNames), [StringComparer]::Ordinal)
    $buildNames = [Collections.Generic.List[string]]::new()
    $stopNames = [Collections.Generic.List[string]]::new()
    $startNames = [Collections.Generic.List[string]]::new()
    $unchangedNames = [Collections.Generic.List[string]]::new()
    $seenStops = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $seenStarts = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)

    foreach ($target in $BuildTargets) {
        $targetName = "$($target.Name)"
        if (-not $targetStateByName.ContainsKey($targetName)) { throw "缺少 build target state:$targetName" }
        $targetStale = -not [bool]$targetStateByName[$targetName].BuildCurrent
        if ($targetStale) { $buildNames.Add($targetName) }
        foreach ($service in @($target.Services)) {
            $runtimeName = "$($service.Name)"
            if (-not $runtimeStateByName.ContainsKey($runtimeName)) { throw "缺少 runtime state:$runtimeName" }
            $runtime = $runtimeStateByName[$runtimeName]
            if (-not [string]::Equals("$($runtime.TargetName)", $targetName, [StringComparison]::Ordinal)) {
                throw "runtime $runtimeName 的 TargetName 与 build target 不一致。"
            }
            $running = [bool]$runtime.IsRunning
            $mustRefresh = $targetStale -or ($ConfigTableChanged -and $tableConsumers.Contains($runtimeName)) -or
                ($running -and -not [bool]$runtime.AppliedCurrent)
            if ($running -and $mustRefresh -and $seenStops.Add($runtimeName)) { $stopNames.Add($runtimeName) }
            if ((-not $running -or $mustRefresh) -and $seenStarts.Add($runtimeName)) { $startNames.Add($runtimeName) }
            if ($running -and -not $mustRefresh) { $unchangedNames.Add($runtimeName) }
        }
    }
    return [pscustomobject][ordered]@{
        BuildTargetNames = $buildNames.ToArray()
        StopRuntimeNames = $stopNames.ToArray()
        StartRuntimeNames = $startNames.ToArray()
        UnchangedRuntimeNames = $unchangedNames.ToArray()
    }
}

function Publish-PandoraPlannerStagedFiles {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][object[]]$Records,
        [scriptblock]$MoveFile = {
            param([string]$Source, [string]$Destination, [bool]$Overwrite)
            [IO.File]::Move($Source, $Destination, $Overwrite)
        }
    )
    Set-StrictMode -Version Latest

    if ($Records.Count -eq 0) { return }
    $destinations = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    $stages = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    $prepared = [Collections.Generic.List[object]]::new()
    foreach ($record in $Records) {
        $stage = [IO.Path]::GetFullPath("$($record.StagePath)")
        $destination = [IO.Path]::GetFullPath("$($record.DestinationPath)")
        if (-not (Test-Path -LiteralPath $stage -PathType Leaf)) { throw "staging 二进制不存在:$stage" }
        if (-not [string]::Equals((Split-Path -Parent $stage), (Split-Path -Parent $destination),
                [StringComparison]::OrdinalIgnoreCase)) {
            throw "staging 必须与目标二进制同目录，才能保证原子发布:$stage -> $destination"
        }
        if (-not $stages.Add($stage) -or -not $destinations.Add($destination) -or
            [string]::Equals($stage, $destination, [StringComparison]::OrdinalIgnoreCase)) {
            throw 'staging/目标路径重复，拒绝非确定性发布。'
        }
        $backup = Join-Path (Split-Path -Parent $destination) ('.{0}.planner-backup-{1}-{2}' -f `
                [IO.Path]::GetFileName($destination), $PID, [guid]::NewGuid().ToString('N'))
        $prepared.Add([pscustomobject][ordered]@{
                StagePath = $stage
                DestinationPath = $destination
                BackupPath = $backup
                HadDestination = [IO.File]::Exists($destination)
            })
    }

    $backedUp = [Collections.Generic.List[object]]::new()
    $published = [Collections.Generic.List[object]]::new()
    $succeeded = $false
    try {
        foreach ($record in $prepared) {
            if (-not $record.HadDestination) { continue }
            $null = & $MoveFile $record.DestinationPath $record.BackupPath $false
            $backedUp.Add($record)
        }
        foreach ($record in $prepared) {
            $null = & $MoveFile $record.StagePath $record.DestinationPath $true
            $published.Add($record)
        }
        $succeeded = $true
    } catch {
        $primaryFailure = $_
        $rollbackErrors = [Collections.Generic.List[string]]::new()
        $publishedReverse = @($published.ToArray())
        [array]::Reverse($publishedReverse)
        foreach ($record in $publishedReverse) {
            try {
                if ([IO.File]::Exists($record.DestinationPath)) {
                    Remove-Item -LiteralPath $record.DestinationPath -Force -ErrorAction Stop
                }
            } catch { $rollbackErrors.Add("移除新文件 $($record.DestinationPath):$($_.Exception.Message)") }
        }
        $backedUpReverse = @($backedUp.ToArray())
        [array]::Reverse($backedUpReverse)
        foreach ($record in $backedUpReverse) {
            try {
                if ([IO.File]::Exists($record.DestinationPath)) {
                    Remove-Item -LiteralPath $record.DestinationPath -Force -ErrorAction Stop
                }
                if (-not [IO.File]::Exists($record.BackupPath)) { throw 'backup 已不存在' }
                $null = & $MoveFile $record.BackupPath $record.DestinationPath $true
            } catch { $rollbackErrors.Add("恢复旧文件 $($record.DestinationPath):$($_.Exception.Message)") }
        }
        if ($rollbackErrors.Count -gt 0) {
            throw "staging 发布失败:$($primaryFailure.Exception.Message)；回滚也失败:$($rollbackErrors -join ' | ')"
        }
        throw $primaryFailure
    } finally {
        foreach ($record in $prepared) {
            Remove-Item -LiteralPath $record.StagePath -Force -ErrorAction SilentlyContinue
            if ($succeeded) { Remove-Item -LiteralPath $record.BackupPath -Force -ErrorAction SilentlyContinue }
        }
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
