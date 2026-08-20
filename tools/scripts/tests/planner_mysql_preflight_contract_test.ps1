# 中心 MySQL 启动前只读预检契约。

$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
. (Join-Path $projectRoot 'tools/scripts/lib/planner_mysql_preflight.ps1')

$script:failed = 0
function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { Write-Host "  [fail] $Message" -ForegroundColor Red; $script:failed++ }
    else { Write-Host "  [ok] $Message" -ForegroundColor Green }
}
function Assert-Throws([scriptblock]$Action, [string]$Pattern, [string]$Message) {
    try { & $Action; Write-Host "  [fail] $Message（未抛错）" -ForegroundColor Red; $script:failed++ }
    catch {
        if ($_.Exception.Message -notmatch $Pattern) { Write-Host "  [fail] $Message（$($_.Exception.Message)）" -ForegroundColor Red; $script:failed++ }
        else { Write-Host "  [ok] $Message" -ForegroundColor Green }
    }
}

$tempRoot = Join-Path ([IO.Path]::GetTempPath()) ("pandora-preflight-contract-{0}" -f [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $tempRoot | Out-Null
try {
    $workspace = '01arz3ndektsv4rrffq69g5fav'
    $identity = Join-Path $tempRoot 'identity.json'
    $credentials = Join-Path $tempRoot 'credentials'
    $profilePath = Join-Path $tempRoot 'profile.json'
    $ca = Join-Path $tempRoot 'planner-ca.pem'
    [IO.File]::WriteAllText($ca, "test-public-ca`n", [Text.UTF8Encoding]::new($false))
    Set-PandoraPlannerDbIdentity -WorkspaceId $workspace -IdentityPath $identity | Out-Null
    $passwordText = 'Abcdefghijklmnopqrstuvwxyz_123456'
    $password = ConvertTo-SecureString $passwordText -AsPlainText -Force
    $target = "Pandora/PlannerDB/$workspace/app"
    Save-PandoraPlannerDbCredential -WorkspaceId $workspace -Target $target -UserName "p_app_$workspace" `
        -Password $password -Version 7 -CredentialRoot $credentials | Out-Null
    $profile = Publish-PandoraMysqlRuntimeProfile -ProjectRoot $projectRoot -Mode central-managed `
        -Endpoint ([ordered]@{host='planner-db.test';port=3306;tls_server_name='planner-db.test';ca_file=$ca}) `
        -CredentialRef ([ordered]@{provider='dpapi-current-user';target=$target;version=7}) `
        -IdentityPath $identity -OutputPath $profilePath -ComputerName 'PC01' -UserName 'planner'
    $credential = Get-PandoraPlannerDbCredential -WorkspaceId $workspace -Target $target -CredentialRoot $credentials

    $fakeExe = Join-Path $tempRoot 'fake-migrate.ps1'
    $fakeSource = @'
$argList = @($args | ForEach-Object { "$_" })
$capture = $env:PANDORA_PREFLIGHT_FAKE_CAPTURE
$targetArg = $argList | Where-Object { $_.StartsWith('-targets-file=') } | Select-Object -First 1
$targetPath = if ($targetArg) { $targetArg.Substring('-targets-file='.Length) } else { '' }
$dir = if ($targetPath) { Split-Path -Parent $targetPath } else { '' }
$dsns = if ($dir) { @(Get-ChildItem -LiteralPath $dir -Filter '*.dsn' -File) } else { @() }
$all = @($dsns | ForEach-Object { [IO.File]::ReadAllText($_.FullName) })
[IO.File]::WriteAllLines("$capture.args", $argList)
if ($targetPath) { Copy-Item -LiteralPath $targetPath -Destination "$capture.targets" -Force }
$allTls = @($all | Where-Object { $_ -notmatch 'tls=true' }).Count -eq 0
$allSecret = @($all | Where-Object { $_ -notmatch 'p_app_.+:' }).Count -eq 0
[IO.File]::WriteAllText("$capture.observed", "dsn_count=$($dsns.Count);tls=$allTls;secret=$allSecret")
Write-Output 'fake preflight result'
exit [int]$env:PANDORA_PREFLIGHT_FAKE_EXIT
'@
    [IO.File]::WriteAllText($fakeExe, $fakeSource, [Text.UTF8Encoding]::new($false))
    $capture = Join-Path $tempRoot 'capture'
    $env:PANDORA_PREFLIGHT_FAKE_CAPTURE = $capture
    $env:PANDORA_PREFLIGHT_FAKE_EXIT = '0'
    $secretRoot = Join-Path $tempRoot 'preflight-secrets'

    Write-Host '[1] 十库只读预检使用 verify-only，secret 只进临时 DSN 文件' -ForegroundColor Cyan
    $result = Invoke-PandoraPlannerMysqlPreflight -ProjectRoot $projectRoot -Profile $profile `
        -Credential $credential -MigrateCommand $fakeExe -SecretRoot $secretRoot
    Assert-True ([bool]$result.Succeeded) 'fake migrator 成功可观测'
    $args = @(Get-Content -LiteralPath "$capture.args")
    Assert-True ($args -contains '-verify-only') '调用迁移器显式携带 -verify-only'
    Assert-True (($args -join ' ') -notmatch [regex]::Escape($passwordText)) '密码不进入进程参数'
    Assert-True ((Get-Content -LiteralPath "$capture.observed" -Raw) -eq 'dsn_count=10;tls=True;secret=True') 'fake 进程实际读到十份 TLS secret DSN'
    $targets = Get-Content -LiteralPath "$capture.targets" -Raw | ConvertFrom-Json
    Assert-True (@($targets.targets).Count -eq 10) 'exact 十个 migration target'
    Assert-True (@($targets.targets | Where-Object { "$($_.tls_ca_file)" -ceq $ca }).Count -eq 10) '每个 target 使用同一公开 CA'
    Assert-True (-not (Test-Path -LiteralPath $secretRoot) -or @(Get-ChildItem -LiteralPath $secretRoot -Force).Count -eq 0) '预检成功后 secret 目录已清空'

    Write-Host '[2] 预检失败仍清 secret，且错误不泄露密码' -ForegroundColor Cyan
    $env:PANDORA_PREFLIGHT_FAKE_EXIT = '9'
    Assert-Throws {
        Invoke-PandoraPlannerMysqlPreflight -ProjectRoot $projectRoot -Profile $profile `
            -Credential $credential -MigrateCommand $fakeExe -SecretRoot $secretRoot | Out-Null
    } 'exit=9|预检失败' 'migrator 非零时 fail-closed'
    Assert-True (-not (Test-Path -LiteralPath $secretRoot) -or @(Get-ChildItem -LiteralPath $secretRoot -Force).Count -eq 0) '预检失败后 secret 目录仍清空'

    Write-Host '[2b] 预检 session 创建阶段失败也不遗留目录' -ForegroundColor Cyan
    $realSetPrivateAcl = ${function:Set-PandoraPlannerPrivateAcl}
    $script:PreflightAclDirectoryCalls = 0
    try {
        function Set-PandoraPlannerPrivateAcl {
            param([Parameter(Mandatory)][string]$Path, [switch]$Directory)
            if ($Directory) {
                $script:PreflightAclDirectoryCalls++
                if ($script:PreflightAclDirectoryCalls -eq 2) { throw 'fixture preflight session ACL failure' }
            }
            & $realSetPrivateAcl -Path $Path -Directory:$Directory
        }
        Assert-Throws {
            Invoke-PandoraPlannerMysqlPreflight -ProjectRoot $projectRoot -Profile $profile `
                -Credential $credential -MigrateCommand $fakeExe -SecretRoot $secretRoot | Out-Null
        } 'fixture preflight session ACL failure' 'session ACL 失败时 fail-closed'
        Assert-True (-not (Test-Path -LiteralPath $secretRoot) -or
            @(Get-ChildItem -LiteralPath $secretRoot -Force -ErrorAction SilentlyContinue).Count -eq 0) `
            'session ACL 失败时不遗留 preflight secret session'
    } finally {
        Set-Item -LiteralPath function:Set-PandoraPlannerPrivateAcl -Value $realSetPrivateAcl
        Remove-Variable -Name PreflightAclDirectoryCalls -Scope Script -Force -ErrorAction SilentlyContinue
    }

    Write-Host '[3] 正式 migrator 暴露只读 verify seam' -ForegroundColor Cyan
    $migrateMain = [IO.File]::ReadAllText((Join-Path $projectRoot 'tools/migrate/main.go'))
    Assert-True ($migrateMain -match 'verify-only') 'tools/migrate 已实现 -verify-only（只读）'
} finally {
    Remove-Item Env:PANDORA_PREFLIGHT_FAKE_CAPTURE -ErrorAction SilentlyContinue
    Remove-Item Env:PANDORA_PREFLIGHT_FAKE_EXIT -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $tempRoot -Recurse -Force -ErrorAction SilentlyContinue
}

if ($script:failed -gt 0) { Write-Host "`n[FAIL] $script:failed 项失败" -ForegroundColor Red; exit 1 }
Write-Host "`n[PASS] 中心 MySQL 启动前只读预检契约" -ForegroundColor Green
