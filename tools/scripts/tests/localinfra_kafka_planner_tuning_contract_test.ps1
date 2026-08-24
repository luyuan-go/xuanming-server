# 策划免 Docker 本机 Kafka 启动调优契约。
#
# 不启动 Kafka；锁住经真实 KRaft 强停重启对照验证过的本地专用参数，防止后续升级时
# 又退回默认 9 秒 broker lease，或重新每 500ms 往空闲 metadata log 写 no-op。

$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
$scriptPath = Join-Path $root 'tools/scripts/local_infra.ps1'
$text = Get-Content -LiteralPath $scriptPath -Raw

function Assert-Contains([string]$Pattern, [string]$Message) {
    if ($text -notmatch $Pattern) { throw "ASSERT FAILED: $Message" }
}

Assert-Contains '(?m)^broker\.session\.timeout\.ms=2000\s*$' `
    '策划单机 KRaft broker lease 应为 2 秒，避免强停重启等待默认 9 秒租约'
Assert-Contains '(?m)^broker\.heartbeat\.interval\.ms=500\s*$' `
    'broker heartbeat 应与 2 秒本地租约配套为 500ms'
Assert-Contains '(?m)^metadata\.max\.idle\.interval\.ms=0\s*$' `
    '空闲的策划单节点集群应禁用 metadata no-op，避免长期运行持续膨胀元数据日志'

Assert-Contains '(?m)^log\.cleaner\.enable=false\s*$' `
    'Windows 上 log cleaner 压缩 __consumer_offsets 时的 rename 会失败，Kafka 把它当致命错误直接关 broker，策划机必须关掉 cleaner'

Assert-Contains '(?m)^log\.retention\.ms=-1\s*$' `
    'Windows 上保留期删段的 rename 同样会失败并被 Kafka 当致命错误关掉 broker，策划机必须禁用按时间删段'

if ($text -match '(?m)^log\.retention\.hours=') {
    throw 'ASSERT FAILED: log.retention.hours 会重新打开按时间删段,策划机 Windows 链必须只用 log.retention.ms=-1'
}

Assert-Contains '-Djava\.io\.tmpdir=\$TmpDir' `
    'Kafka 的 JVM 必须显式钉住 java.io.tmpdir，否则继承来的 %TEMP% 在本机 AF_UNIX 不通、JVM 建不出 Pipe 直接致命退出'

Assert-Contains '\$kafkaTmp = Resolve-AfUnixTempDir' `
    'Kafka 必须走与 Envoy 同一个 AF_UNIX 临时目录解析,不能各写一套'

Write-Host '[PASS] 策划本机 Kafka 启动调优契约通过' -ForegroundColor Green
