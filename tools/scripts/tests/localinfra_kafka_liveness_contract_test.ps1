# 策划免 Docker 本机 Kafka「运行期存活检测」契约(INC-20260821-001 行动项 A-3)。
#
# 不启动 Kafka:判定本体是纯函数,这里直接喂现场样本。守三件事 ——
#   1. 「进程没了」和「进程在但正在自杀」必须是两个结论(只看端口的检测正是栽在后者);
#   2. 判死时必须点名 matchmaker / matchmaker_pve / battle_result 是被连带打死的。
#      2026-08-24 现场看到的就是这三个服务同时 exit 1,不点名下一个人还会照着它们再查一轮;
#   3. 报告必须一行一条真的打出来 —— 这段代码只在出故障时才执行,它自己有 bug 的表现
#      就是把一个可查的故障变成不可查的。
#
# 用法:pwsh tools/scripts/tests/localinfra_kafka_liveness_contract_test.ps1

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$scriptsDir = Split-Path -Parent $PSScriptRoot
. (Join-Path $scriptsDir 'lib/kafka_liveness.ps1')
$infra = Join-Path $scriptsDir 'local_infra.ps1'
$errs = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($infra, [ref]$null, [ref]$errs)
if ($errs -and $errs.Count -gt 0) { throw "local_infra.ps1 语法错误:$($errs[0].Message)" }
$source = [System.Text.UTF8Encoding]::new($false).GetString([System.IO.File]::ReadAllBytes($infra))

$script:Failed = New-Object 'System.Collections.Generic.List[string]'
function Assert-True([bool]$Cond, [string]$What) {
    if ($Cond) { Write-Host "  [ok] $What" } else { $script:Failed.Add($What); Write-Host "  [NG] $What" -ForegroundColor Red }
}
function Get-State([bool]$Registered, [bool]$Alive, [bool]$Listening, $Tail) {
    return (Get-PandoraKafkaLivenessVerdict -Registered $Registered -ProcessAlive $Alive `
            -ListenerOpen $Listening -LogTail $Tail -Port 9093)
}
function Get-InfraFunction([string]$Name) {
    $found = @($ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $Name }, $true))
    if ($found.Count -ne 1) { throw "local_infra.ps1 里找不到唯一的 $Name(找到 $($found.Count) 个)" }
    return $found[0].Extent.Text
}

# 取自 INC-20260821-001 §2.2 与 2026-08-24 现场的真实致命行。
$retentionSuicide = @(
    '[2026-08-21 03:47:33,853] ERROR Error while deleting segments for pandora.player.presence-2 (kafka.server.ReplicaManager)'
    'org.apache.kafka.common.errors.KafkaStorageException: Error while deleting segments'
    '[2026-08-21 03:47:33,890] ERROR Shutdown broker because all log dirs in F:/work/.../data/kafka have failed (kafka.log.LogManager)'
)
$tombstoneSuicide = @(
    '[2026-08-24 05:06:01,120] ERROR Encountered fatal fault (org.apache.kafka.server.fault.ProcessTerminatingFaultHandler)'
    'java.nio.file.AccessDeniedException: F:\work\...\data\kafka\__cluster_metadata-0\00000000000000012345.checkpoint.deleted'
)
# 反例:保留期删段的第一次 rename 失败是 WARN + 重试,非原子 move 有可能成功,broker 照样活着。
# 把它当致命串就会在正常运行时误拦一键启动 —— 检测本身制造故障。
$retryOnlyWarn = @(
    '[2026-08-21 03:47:33,848] WARN Failed atomic move of ...timeindex to ...timeindex.deleted retrying with a non-atomic move'
    'java.nio.file.FileSystemException: ...timeindex -> ...timeindex.deleted: The process cannot access the file'
)

Assert-True ((Get-State $true $true $true @('[x] INFO Kafka Server started')).State -ceq 'ALIVE') `
    '进程在 + 端口在 + 日志无致命串 = ALIVE'
Assert-True ((Get-State $true $false $false $null).State -ceq 'DEAD') `
    '登记的 broker 进程查无此进程 = DEAD(进程没了)'
Assert-True ((Get-State $true $true $false @('[x] INFO shutting down')).State -ceq 'DYING') `
    '进程还在但端口已不监听 = DYING —— 关闭顺序是先停 listener 后退进程,报"进程没了"是错的'
Assert-True ((Get-State $true $true $true $retentionSuicide).State -ceq 'DYING') `
    '端口还通但日志已出现 log dir 失败 = DYING,绝不能因为端口能连就判健康'
Assert-True ((Get-State $true $true $true $tombstoneSuicide).Signature.Id -ceq 'snapshot-tombstone-access-denied') `
    'checkpoint.deleted 的 AccessDenied 认成快照墓碑那一类,不和删段混为一谈'
Assert-True ((Get-State $true $true $true $retryOnlyWarn).State -ceq 'ALIVE') `
    '反例:只有 rename 重试 WARN 时必须仍判 ALIVE(它是 Kafka 自己的重试路径,不是终局)'
Assert-True ((Get-State $true $true $true $null).Blocking -eq $false) `
    '读不到 kafka.log 只能 UNKNOWN 告警,不许为一份读不到的日志把一键启动判死'
Assert-True ((Get-State $false $false $true $null).State -ceq 'UNKNOWN') `
    '端口有人监听但本工作区没有登记时既不认领也不宣判'

$report = (Format-PandoraKafkaLivenessReport -Verdict (Get-State $true $true $true $retentionSuicide)) -join "`n"
foreach ($svc in @('matchmaker', 'matchmaker_pve', 'battle_result')) {
    Assert-True ($report -match [regex]::Escape($svc)) "判死报告点名了被连带打死的 $svc"
}
Assert-True ($report -match '别去查那三个服务') '判死报告明说别去查那三个服务(A-3 的全部价值在这一句)'
Assert-True ($report -match '不处于「可玩」状态') '判死报告撤销「可玩」结论'
Assert-True (((Format-PandoraKafkaLivenessReport -Verdict (Get-State $true $true $true @('[x] INFO ok'))) -join "`n") -notmatch 'matchmaker') `
    'ALIVE 时不发连带警告(误报会让人学会忽略它)'

# 把待测函数从 local_infra.ps1 的 AST 里抠出来真跑一次(不能 dot-source 整个脚本 ——
# 它末尾就开始真的备料 / 起进程了)。抠的是原文,所以测的确实是仓库里那份代码。
Invoke-Expression (Get-InfraFunction 'Show-KafkaLiveness')
Invoke-Expression (Get-InfraFunction 'Read-LogTail')
$LogDir = [System.IO.Path]::GetTempPath()
function Write-Ok([string]$m) { Write-Host "  [ OK ] $m" }
function Write-Warn2([string]$m) { Write-Host "  [WARN] $m" }
function Write-Err([string]$m) { Write-Host "  [ERR ] $m" }

# 守「报告一行一条」:第一版在 Format 外面多包了一层 @(),于是 foreach 只转一圈,
# Write-Err 的 [string] 形参把整个数组按空格拼成一行,根因与连带影响全挤进同一条 [ERR]。
$shown = ((Show-KafkaLiveness -Verdict (Get-State $true $true $true $retentionSuicide)) 6>&1 | Out-String)
Assert-True ((@($shown -split "`r?`n" | Where-Object { $_ -match '\[ERR \]' })).Count -ge 3) `
    '判死报告按行输出(至少 3 条 [ERR]),没有被 [string] 形参拼成一行'

# 有界读:存活检测每次 status / up 都要读 kafka.log,而那是 broker 的全量 stdout,长期跑
# 能到几十 MB。截断必然切坏第一行,所以结果必须与整份读的同样几行逐字相同。
$sample = Join-Path ([System.IO.Path]::GetTempPath()) ("pandora-kafkatail-" + [guid]::NewGuid().ToString('N') + '.log')
try {
    1..2000 | ForEach-Object { "line-$_ 中文填充填充填充填充填充填充填充" } | Set-Content -LiteralPath $sample -Encoding utf8NoBOM
    $full = Read-LogTail -Path $sample -Lines 5
    Assert-True (((Read-LogTail -Path $sample -Lines 5 -MaxTailBytes 2048) -join '|') -ceq ($full -join '|')) `
        '有界读只读末尾字节,但末 5 行与整份读逐字相同(中途截断切坏的半行已丢弃)'
    Assert-True (((Read-LogTail -Path $sample -Lines 5 -MaxTailBytes 0) -join '|') -ceq ($full -join '|')) `
        'MaxTailBytes=0 保持原行为,Show-ComponentFailure 那条老路径不受影响'
} finally { Remove-Item -LiteralPath $sample -Force -ErrorAction SilentlyContinue }

Assert-True ($source -match "ValidateSet\('up', 'down', 'status', 'kafka-health'") `
    'local_infra.ps1 提供可独立运行的 -Action kafka-health 诊断命令'
Assert-True ($source -match "'kafka-health' \{ if \(Test-LocalKafkaAlive\) \{ exit 0 \} else \{ exit 1 \} \}") `
    'kafka-health 判死时 exit 1,可以直接当父脚本门禁'
Assert-True ($source -match 'if \(-not \(Test-LocalKafkaAlive\)\) \{') `
    'Invoke-Up 在打出「已就绪」之前过存活闸,不让 broker 已死的链拿到绿灯'
Assert-True ($source -match 'Show-KafkaLiveness -Verdict \(Get-LocalKafkaLiveness\)') `
    '-Action status 也打运行期结论,不让一行 UP 冒充健康'
Assert-True ($source -match "Read-LogTail -Path \(Join-Path \`$LogDir 'kafka\.log'\) -Lines 200 -MaxTailBytes") `
    '存活检测读 kafka.log 走有界路径,不整份读 broker 的全量 stdout'

if ($script:Failed.Count -gt 0) {
    Write-Host ''
    Write-Host '[FAIL] 本机 Kafka 运行期存活检测契约未通过:' -ForegroundColor Red
    $script:Failed | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}
Write-Host ''
Write-Host '[PASS] 本机 Kafka 运行期存活检测契约通过' -ForegroundColor Green
