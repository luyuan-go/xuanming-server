# Pandora 策划机 MySQL 启动 profile seam。
#
# bundle 是否存在是唯一模式选择开关：存在即 central-managed，哪怕内容损坏也绝不
# 回退 local-owned。内容校验、首次登记、bind-once identity 与 DPAPI 凭据全部复用
# planner_mysql_enrollment.ps1。

$enrollmentLib = Join-Path $PSScriptRoot 'planner_mysql_enrollment.ps1'
if (-not (Get-Command Ensure-PandoraPlannerMysqlEnrollment -ErrorAction SilentlyContinue)) {
    . $enrollmentLib
}
$localInfraStateLib = Join-Path $PSScriptRoot 'local_infra_state.ps1'
if (-not (Get-Command Get-PandoraServiceAppliedMysqlState -ErrorAction SilentlyContinue)) {
    . $localInfraStateLib
}

function Get-PandoraPlannerCentralMysqlBundlePath {
    param([Parameter(Mandatory)][string]$ProjectRoot)
    return [IO.Path]::GetFullPath((Join-Path $ProjectRoot 'installers/planner-db/central-mysql.json'))
}

function Get-PandoraPlannerMysqlStartupMode {
    param([Parameter(Mandatory)][string]$ProjectRoot)
    $bundle = Get-PandoraPlannerCentralMysqlBundlePath -ProjectRoot $ProjectRoot
    if (Test-Path -LiteralPath $bundle -PathType Leaf) { return 'central-managed' }

    # bundle 缺失只对从未进入 central 的纯净 Git/local 工作区意味着 local-owned。
    # 本项目一旦登记过 central applied state 或发布过 central runtime profile，缺包就是
    # SVN/发布不完整；必须在启动本机 MySQL 之前阻断，不能把同一工作区静默切到另一套库。
    $applied = Get-PandoraServiceAppliedMysqlState -ProjectRoot $ProjectRoot
    if ($applied -and "$($applied.Mode)" -ceq 'central') {
        throw "本项目已登记 central MySQL applied state，但中心 bundle 缺失:$bundle；拒绝回退本机 MySQL"
    }
    $profilePath = Get-PandoraMysqlProfileDefaultOutputPath -ProjectRoot $ProjectRoot
    if (Test-Path -LiteralPath $profilePath -PathType Leaf) {
        try {
            $profile = [IO.File]::ReadAllText($profilePath) | ConvertFrom-Json
            $profileMode = "$($profile.mode)"
            if ($profileMode -ceq 'central-managed') {
                throw "本项目已有 central runtime profile，但中心 bundle 缺失:$bundle；拒绝回退本机 MySQL"
            }
            if ($profileMode -cne 'local-owned') {
                throw "MySQL runtime profile mode 不合法:$profileMode"
            }
        } catch {
            throw "中心 bundle 缺失且已有 MySQL runtime profile 无法安全判定:$profilePath。拒绝回退本机 MySQL。详情:$($_.Exception.Message)"
        }
    }
    return 'local-owned'
}

function Get-PandoraLocalInfraLifecyclePlan {
    param([Parameter(Mandatory)][string]$ProjectRoot)
    $mode = Get-PandoraPlannerMysqlStartupMode -ProjectRoot $ProjectRoot
    if ($mode -ceq 'central-managed') {
        return [pscustomobject][ordered]@{
            Mode = $mode
            ProvisionComponents = @('redis', 'kafka', 'jre')
            StartComponents = @('redis', 'kafka', 'envoy')
            StopComponents = @('envoy', 'kafka', 'redis')
            ResetComponents = @('redis', 'kafka')
        }
    }
    return [pscustomobject][ordered]@{
        Mode = $mode
        ProvisionComponents = @('mysql', 'redis', 'kafka', 'jre')
        StartComponents = @('mysql', 'redis', 'kafka', 'envoy')
        StopComponents = @('envoy', 'kafka', 'redis', 'mysql')
        ResetComponents = @('mysql', 'redis', 'kafka')
    }
}

function Initialize-PandoraPlannerMysqlRuntime {
    param(
        [Parameter(Mandatory)][string]$ProjectRoot,
        [string]$IdentityPath = '',
        [string]$CredentialRoot = '',
        [string]$OutputPath = '',
        [Security.SecureString]$EnrollmentToken
    )
    $mode = Get-PandoraPlannerMysqlStartupMode -ProjectRoot $ProjectRoot
    if ($mode -ceq 'local-owned') {
        return [pscustomobject][ordered]@{ Mode = $mode; Profile = $null; BundlePath = '' }
    }
    $bundle = Get-PandoraPlannerCentralMysqlBundlePath -ProjectRoot $ProjectRoot
    try {
        $profile = Ensure-PandoraPlannerMysqlEnrollment -ProjectRoot $ProjectRoot -ConfigPath $bundle `
            -IdentityPath $IdentityPath -CredentialRoot $CredentialRoot -OutputPath $OutputPath `
            -EnrollmentToken $EnrollmentToken
        return [pscustomobject][ordered]@{ Mode = $mode; Profile = $profile; BundlePath = $bundle }
    } catch {
        throw "central mysql bundle/登记失败，拒绝回退本机 MySQL:$bundle。详情:$($_.Exception.Message)"
    }
}
