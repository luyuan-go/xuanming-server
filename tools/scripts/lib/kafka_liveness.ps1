# 免 Docker 本机 Kafka 的运行期存活判定(纯 seam:不碰端口、进程、文件系统)。
#
# 为什么要单独有这一层(INC-20260821-001 行动项 A-3):
# 策划机这条免 Docker 链**只有启动前的端口预检,broker 起来之后没有任何存活监控**。而在
# Windows 原生 KRaft 单节点上 broker 自杀是常态而不是意外 —— 保留期删段、log cleaner 压缩、
# KRaft 快照清理都要重命名或删除 Kafka 自己 mmap 着的索引文件,Windows 一律拒绝;Kafka 又把
# 唯一 log.dirs 的 IOException 当致命错,直接 "Shutdown broker because all log dirs have failed"。
# 它可以在启动几十秒后死,也可以在半夜某一次后台扫描时才死 —— 那时启动期那次 exact listener
# 检查早就过去了,窗口里最后一行还是绿的。
#
# 更要命的是它在业务侧的表现:matchmaker / matchmaker_pve / battle_result 会**同时** exit 1。
# 那三个服务只是强依赖 Kafka、启动即 fail-fast 被连带打死的,去翻它们的日志和代码全是白查
# (2026-08-24 现场就照着这三个服务查了一轮)。所以本文件的产出不能只是一个布尔值,必须把
# 根因和「别去查那三个服务」一起说出来 —— 存活检测的价值全在指对方向。
#
# 判定与取证分开是为了能测:真把 data/kafka 搞坏来复现一次自杀现场,既慢又不可重复,而这段
# 代码恰好属于「只在出故障时才执行」的那一类(它自己有 bug 的表现是把可查故障变成不可查)。
# 调用方(local_infra.ps1)负责取三份事实,本文件只负责下判断。

# 启动即强依赖 Kafka、broker 一死必然同时 exit 1 的三个服务。这份名单唯一的用途就是在
# 报错里点名「别去查这三个」。
$script:PandoraKafkaFailFastServices = @('matchmaker', 'matchmaker_pve', 'battle_result')

# 致命串表:命中任意一条 = broker 已经判定存储失败,正在或已经关掉自己。
#
# 入表门槛是**终局性**,不是「看着吓人」。反例:保留期删段时那句
# `WARN Failed atomic move of ...timeindex to ...timeindex.deleted retrying with a non-atomic move`
# 以及它带的 java.nio.file.FileSystemException 栈 —— 那是 Kafka 自己的重试路径,非原子 move
# 有可能成功,broker 照样活着。把它写进表里就会在正常运行时误判自杀、把一键启动拦下来,
# 属于「检测本身制造故障」,所以刻意不收。
#
# 只认 run/localinfra/logs/kafka.log(broker 的全量 stdout)就够:Kafka 自带的
# log4j.properties 里只有 LogCleaner / controller / requestChannel 几个 logger 设了
# additivity=false 单独出文件,而**终局**那几句(ReplicaManager 停 replicas、LogManager
# 关 broker、ProcessTerminatingFaultHandler)都走 rootLogger,一定会落到 stdout。
# 换句话说压缩失败的细节在 log-cleaner.log,但「因此要关 broker」这句永远在 kafka.log。
#
# 顺序从具体到笼统:先命中的先报,好让人拿到最接近现场的那句话。
$script:PandoraKafkaFatalLogSignatures = @(
    @{
        Id      = 'snapshot-tombstone-access-denied'
        Pattern = 'AccessDeniedException[^\r\n]*\.checkpoint\.deleted'
        Cause   = 'KRaft 启动期 recoverSnapshots 要删掉上一轮遗留的 *.checkpoint.deleted 墓碑,而本机这批文件带 ReadOnly 属性,Windows 对它的删除是 AccessDenied;Kafka 用 ProcessTerminatingFaultHandler 把它当致命错直接终止进程。'
        Fix     = '重跑 pwsh tools/scripts/local_infra.ps1 -Action up —— Start-LocalKafka 在起 JVM 之前会先去掉 ReadOnly 再删掉这批墓碑。'
    }
    @{
        Id      = 'log-dir-failed'
        Pattern = 'Shutdown broker because all log dirs[^\r\n]*have failed|because the log directory has failed|Stopping serving replicas in dir'
        Cause   = '唯一的 log.dirs 被判失败。Windows 不允许重命名 / 删除 Kafka 自己 mmap 着的索引文件(保留期删段 *.timeindex → *.timeindex.deleted、压缩 *.timeindex.cleaned → *.timeindex.swap),而单节点只有一个 log dir,隔离该目录等于关掉整个 broker 的存储。'
        Fix     = '先确认 run/localinfra/cfg/kafka.properties 里 log.cleaner.enable=false 与 log.retention.ms=-1 都还在(这两行就是为了让策划机 Kafka 永不删段);两行都在还复发,说明有外部进程在动 data/kafka —— 按 INC-20260821-001 先定位 holder,禁止用删数据目录绕过根因。'
    }
    @{
        Id      = 'storage-failure'
        Pattern = 'KafkaStorageException'
        Cause   = 'Kafka 把 data/kafka 上的一次文件操作记成了存储异常。单 log dir 下这一步之后就是 broker 关闭,不存在「报一下继续跑」。'
        Fix     = '在 kafka.log 里搜这一行的上文,看是哪个 topic-partition 的哪种文件操作被拒;保留现场,不要先删数据。'
    }
    @{
        Id      = 'fatal-exit'
        Pattern = "Uncaught exception in scheduled task 'kafka-log-retention'|Exiting Kafka due to fatal exception|ProcessTerminatingFaultHandler"
        Cause   = 'Kafka 自己记录了致命异常并决定终止进程(具体子类见上方证据行)。'
        Fix     = '把 run/localinfra/logs/kafka.log 里这一行前后各 50 行贴回来定位;kafka.log 每次启动都会被整份覆盖,所以里面只有当前这一轮,不必担心翻到历史噪声。'
    }
)

function Get-PandoraKafkaFatalLogHit {
    <#
      在 kafka.log 尾巴里找致命串。返回命中的那一条(带原始证据行)或 $null。

      $null(读不到日志)与空数组(日志是空的)必须分开:前者是"无法证明它没在自杀",
      后者是"这一轮什么都没写过";这里只负责不把两者混成同一个结论,分类由 verdict 做。
    #>
    [CmdletBinding()]
    param([AllowNull()][AllowEmptyCollection()][string[]]$LogTail)
    Set-StrictMode -Version Latest

    if ($null -eq $LogTail -or $LogTail.Count -eq 0) { return $null }
    $text = ($LogTail -join "`n")
    foreach ($sig in $script:PandoraKafkaFatalLogSignatures) {
        if ($text -notmatch $sig.Pattern) { continue }
        return [pscustomobject]@{
            Id       = [string]$sig.Id
            Cause    = [string]$sig.Cause
            Fix      = [string]$sig.Fix
            # 只留最后 3 行:证据是给人定位用的锚点,不是把日志再贴一遍。
            Evidence = @(@($LogTail | Where-Object { $_ -match $sig.Pattern }) | Select-Object -Last 3)
        }
    }
    return $null
}

function Get-PandoraKafkaLivenessVerdict {
    <#
      三份事实 → 一个运行期结论。

      为什么必须区分 DEAD 与 DYING:这两种现场的下一步动作不一样。DEAD(进程没了)只能重启;
      DYING(进程还在、甚至端口还通,但 log dir 已判失败)是**正在**自杀 —— 此刻去 telnet 9093
      还会成功,只看端口的检测会给出绿灯,而几秒后三个业务服务就集体 exit 1。启动期那套
      "端口通即就绪"正是栽在这一格上,所以这里把它单列成一个状态。

      ListenerOpen=false 而进程还在,也归 DYING 而不是 DEAD:Kafka 关闭顺序是先停 replicas
      与 listener、后退进程,端口先没的那一小段窗口里进程仍在,报"进程没了"是错的。
    #>
    [CmdletBinding()]
    param(
        # 本工作区是否有 broker 进程登记(run/localinfra/pids/kafka.pid)。没有登记时,
        # 端口上的那个 broker 不能归属给本工作区 —— 既不能认领,也不能替它宣判死亡。
        [Parameter(Mandatory)][bool]$Registered,
        [Parameter(Mandatory)][bool]$ProcessAlive,
        [Parameter(Mandatory)][bool]$ListenerOpen,
        [AllowNull()][AllowEmptyCollection()][string[]]$LogTail,
        [int]$Port = 9093
    )
    Set-StrictMode -Version Latest

    $hit = Get-PandoraKafkaFatalLogHit -LogTail $LogTail

    if (-not $Registered) {
        if ($ListenerOpen) {
            return New-PandoraKafkaLivenessVerdict -State 'UNKNOWN' -Signature $hit -Port $Port `
                -Reason "Kafka :$Port 有人监听,但本工作区没有可核对的 broker 进程登记(run/localinfra/pids/kafka.pid)。既不能认领它,也不能替它宣判死亡。"
        }
        return New-PandoraKafkaLivenessVerdict -State 'DEAD' -Signature $hit -Port $Port `
            -Reason "Kafka 没有在跑:本工作区既没有 broker 进程登记,:$Port 也没有 listener。"
    }
    if (-not $ProcessAlive) {
        return New-PandoraKafkaLivenessVerdict -State 'DEAD' -Signature $hit -Port $Port `
            -Reason '本工作区登记的 Kafka broker 进程已经不在了(登记 PID 查无此进程)。'
    }
    if (-not $ListenerOpen) {
        return New-PandoraKafkaLivenessVerdict -State 'DYING' -Signature $hit -Port $Port `
            -Reason "Kafka broker 进程还在,但 :$Port 已经不再监听 —— log dir 判失败后 broker 先停 replicas 与 listener、再退进程,这是「正在自杀」不是「还活着」。"
    }
    if ($hit) {
        return New-PandoraKafkaLivenessVerdict -State 'DYING' -Signature $hit -Port $Port `
            -Reason "Kafka 进程与 :$Port 都还在,但 kafka.log 已经出现致命串:broker 已判定存储失败,正在关闭自己。此刻端口还通,不能当成健康。"
    }
    if ($null -eq $LogTail) {
        return New-PandoraKafkaLivenessVerdict -State 'UNKNOWN' -Signature $null -Port $Port `
            -Reason "Kafka 进程与 :$Port 都在,但读不到 run/localinfra/logs/kafka.log,无法排除它正在自杀。"
    }
    return New-PandoraKafkaLivenessVerdict -State 'ALIVE' -Signature $null -Port $Port `
        -Reason "Kafka broker 进程在、:$Port 在监听、kafka.log 里没有致命串。"
}

function New-PandoraKafkaLivenessVerdict {
    <#
      Healthy 与 Blocking 刻意是两个字段,不是一个取反:
        Healthy=false 且 Blocking=false 就是 UNKNOWN —— 我们拿不到证据,但也没有证据说它死了。
      UNKNOWN 只能告警不能拦:为了一份读不到的日志把策划的一键启动判死,是把观测缺口
      升级成可用性事故(同 §9.24 容量超限只告警不阻断的取舍)。
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][ValidateSet('ALIVE', 'DYING', 'DEAD', 'UNKNOWN')][string]$State,
        [Parameter(Mandatory)][string]$Reason,
        [Parameter(Mandatory)][AllowNull()]$Signature,
        [Parameter(Mandatory)][int]$Port
    )
    Set-StrictMode -Version Latest
    return [pscustomobject]@{
        State     = $State
        Healthy   = ($State -ceq 'ALIVE')
        Blocking  = ($State -ceq 'DEAD' -or $State -ceq 'DYING')
        Reason    = $Reason
        Signature = $Signature
        Port      = $Port
    }
}

function Format-PandoraKafkaLivenessReport {
    <#
      把结论排版成可以整段贴回来的几行。判死时**必须**带上那句连带说明 ——
      A-3 的全部价值就在这一句:2026-08-24 现场看到的是三个业务服务同时 exit 1,
      不点名的话下一个人还会照着那三个服务再查一轮。

      ⚠️ 连带说明的开关是 Blocking(DEAD / DYING)而**不是** -not Healthy。
      两者只差一个 UNKNOWN,而那一格恰恰是「端口有人监听但不是本工作区起的」或
      「kafka.log 读不到」—— 现场完全可能是 Kafka 活得好好的。用 -not Healthy 会让
      UNKNOWN 也打出「那三个服务会 exit 1」「本机不处于可玩状态」这种判死级文案,
      既与 New-PandoraKafkaLivenessVerdict 里「UNKNOWN 只能告警不能拦」的取舍自相矛盾,
      也正是本文件自己警告过的那种误报 —— 误报会让人学会忽略这段输出,
      于是真的判死那次也被一起忽略掉。
    #>
    [CmdletBinding()]
    param([Parameter(Mandatory)]$Verdict)
    Set-StrictMode -Version Latest

    $lines = New-Object 'System.Collections.Generic.List[string]'
    $lines.Add("Kafka 运行期存活:$($Verdict.State) —— $($Verdict.Reason)")
    if ($Verdict.Signature) {
        $lines.Add("  根因:$($Verdict.Signature.Cause)")
        foreach ($line in @($Verdict.Signature.Evidence)) { $lines.Add("  证据:$line") }
        $lines.Add("  处置:$($Verdict.Signature.Fix)")
    }
    if ($Verdict.Blocking) {
        $lines.Add(("  连带影响:{0} 会因为强依赖 Kafka 而 fail-fast 同时 exit 1。别去查那三个服务,它们只是被连带打死的,先把 Kafka 修好。" -f
            ($script:PandoraKafkaFailFastServices -join ' / ')))
        $lines.Add('  在 Kafka 恢复之前,本机这条链不处于「可玩」状态:撮合、PVE 撮合与战斗结算落库都起不来。')
    }
    elseif (-not $Verdict.Healthy) {
        # UNKNOWN:只说「没看清」,绝不下判死结论,也不给处置指令。
        $lines.Add('  这是观测缺口不是故障结论:Kafka 可能好好的(比如 broker 是另一个工作区起的)。要确认请跑 local_infra.ps1 -Action kafka-health。')
    }
    return , @($lines.ToArray())
}
