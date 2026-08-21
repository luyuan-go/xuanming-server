# 策划 fast migration 的内部只读 ownership probe。
#
# 本进程始终由父 dev_migrate 放进 KILL_ON_JOB_CLOSE Job，只复用 canonical
# PID/exe/my.ini/listener 归属检查。它没有密码、stdin、SQL 或原生执行入口；真正的数据库动作由
# 持有工作区编排锁的父进程在 probe 成功后，以固定 allowlist 和同一总 deadline 单独启动。

[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$ProjectRoot,
    [Parameter(Mandatory)][ValidateRange(1024, 49151)][int]$MysqlPort
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)

. (Join-Path $PSScriptRoot 'local_infra_state.ps1')

try {
    $resolvedProjectRoot = (Resolve-Path -LiteralPath $ProjectRoot -ErrorAction Stop).Path
    $state = Get-PandoraLocalInfraPortState $resolvedProjectRoot
    if (-not $state -or [int]$state.MysqlPort -ne $MysqlPort) {
        throw "MySQL :$MysqlPort 身份状态缺失或端口不一致。"
    }
    $ownedProcess = Get-PandoraLocalMysqlOwnedProcess $resolvedProjectRoot $state
    if (-not $ownedProcess -or [int]$ownedProcess.Id -ne [int]$state.MysqlProcessId) {
        throw "MySQL :$MysqlPort 未通过 PID + exe + my.ini + listener canonical 归属复核。"
    }
    exit 0
} catch {
    [Console]::Error.WriteLine("[ownership-probe] $($_.Exception.Message)")
    exit 1
}
