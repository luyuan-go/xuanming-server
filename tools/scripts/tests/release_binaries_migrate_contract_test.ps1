<#
.SYNOPSIS
  策划免 Docker Windows 发布包必须携带数据库迁移器。

.DESCRIPTION
  只读检查构建、manifest、zip 与消费路径的接线；不执行 go build，不创建制品，
  也不启动 Docker、数据库或服务。

.EXAMPLE
  pwsh tools/scripts/tests/release_binaries_migrate_contract_test.ps1
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
$BuildScript = Join-Path $ProjectRoot 'tools/scripts/build_release_binaries.ps1'
$MigrateScript = Join-Path $ProjectRoot 'tools/scripts/dev_migrate.ps1'
$BuildSource = [System.IO.File]::ReadAllText($BuildScript)
$MigrateSource = [System.IO.File]::ReadAllText($MigrateScript)

$script:Failures = [System.Collections.Generic.List[string]]::new()
function Assert-True([bool]$Condition, [string]$Message) {
    if ($Condition) {
        Write-Host "  [ ok ] $Message" -ForegroundColor DarkGray
    } else {
        $script:Failures.Add($Message)
        Write-Host "  [FAIL] $Message" -ForegroundColor Red
    }
}

Write-Host '[1] 发布构建接入 pandora-migrate.exe' -ForegroundColor Cyan
$parseErrors = $null
$buildAst = [System.Management.Automation.Language.Parser]::ParseFile(
    $BuildScript, [ref]$null, [ref]$parseErrors)
Assert-True (-not ($parseErrors -and $parseErrors.Count -gt 0)) 'build_release_binaries.ps1 语法可解析'

$releaseListAssignments = @($buildAst.FindAll({
            param($node)
            $node -is [System.Management.Automation.Language.AssignmentStatementAst] -and
            $node.Left.Extent.Text -ceq '$ReleaseBinaries'
        }, $true))
Assert-True ($releaseListAssignments.Count -eq 1) '存在唯一正式二进制白名单'

$releaseListText = if ($releaseListAssignments.Count -eq 1) {
    $releaseListAssignments[0].Right.Extent.Text
} else { '' }
Assert-True ($releaseListText -match "Name\s*=\s*'pandora-migrate';\s*Dir\s*=\s*'tools/migrate';\s*Package\s*=\s*'\.'") `
    '正式白名单从 tools/migrate 模块构建 pandora-migrate'
Assert-True ($BuildSource -match 'Join-Path\s+\$stageBin\s+"\$\(\$binary\.Name\)\.exe"') `
    '所有白名单项统一输出到本轮 staging/bin'
Assert-True ($BuildSource -match '&\s+go\s+build\s+-buildvcs=true\s+-o\s+\$outputPath\s+\$binary\.Package') `
    '迁移器与服务共用显式 VCS 身份的 staging go build 发布路径'

Write-Host '[2] manifest、zip 与运行时消费路径闭环' -ForegroundColor Cyan
$buildLoopIndex = $BuildSource.IndexOf('foreach ($binary in $ReleaseBinaries)', [StringComparison]::Ordinal)
$manifestScanIndex = $BuildSource.IndexOf('$actualFiles = @(', [StringComparison]::Ordinal)
Assert-True ($buildLoopIndex -ge 0 -and $manifestScanIndex -gt $buildLoopIndex) `
    '完整白名单构建完才扫描 staging 生成 manifest，迁移器 hash 不会漏记'
Assert-True ($BuildSource -match 'Compress-Archive\s+-Path\s+\(Join-Path\s+\$stageRoot\s+''\*''\)') `
    'zip 从已核验 staging 根生成，包含 bin/pandora-migrate.exe 与 manifest.json'
Assert-True ($MigrateSource -match "run/artifacts/windows/bin/pandora-migrate\.exe") `
    'dev_migrate.ps1 消费同一个 pandora-migrate.exe 路径'

Write-Host '[3] 正式发布动态契约纳入既有 CI 入口' -ForegroundColor Cyan
$publishContract = Join-Path $PSScriptRoot 'release_artifact_publish_contract_test.ps1'
$publishOutput = (& pwsh -NoProfile -File $publishContract 2>&1 | Out-String)
$publishExit = $LASTEXITCODE
Assert-True ($publishExit -eq 0) 'dirty/unknown、失败回滚、exact 24 与 -Service 隔离动态契约通过'
if ($publishExit -ne 0) {
    Write-Host $publishOutput -ForegroundColor DarkYellow
}

Write-Host ''
if ($script:Failures.Count -gt 0) {
    Write-Host "[ERR ] $($script:Failures.Count) 项发布迁移器契约未满足:" -ForegroundColor Red
    $script:Failures | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}

Write-Host '[ OK ] 策划免 Docker 发布包迁移器契约全部满足。' -ForegroundColor Green
