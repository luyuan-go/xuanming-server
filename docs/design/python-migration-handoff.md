# Python 迁移交接文档

> **这份文档的唯一目的**:让一个**完全没有上下文**的会话在 10 分钟内接着干。
>
> 它不复述设计(那在 [`python-migration.md`](./python-migration.md)),不复述逐服务缺口
> (那在 [`python-migration-remaining.md`](./python-migration-remaining.md))。它只回答四个问题:
> **现在到哪了 / 怎么验证我没弄坏 / 下一步做什么 / 哪些坑别再踩一遍。**
>
> 最后更新:2026-08-19

---

## 0. 30 秒版本

Go 后端(21 个服务)正在按 strangler 模式移植到 Python。**两套栈并行跑同一份 YAML 配置、
同一批 MySQL 表、同一批 Kafka topic、同一批 Redis key**,可以逐服务切换。

| | 数字 |
|---|---:|
| 服务可跑(能起进程、能接 RPC) | **19 / 21** |
| RPC 有 Python 实现 | **185 / 211(87%)** |
| 未移植 | `ds_allocator`(18990 行)、`hub_allocator`(12654 行) |
| 代码量 | Go 105010 行 / Py 72092 行 |

**没移植的两个 allocator 是刻意的**,不是漏了 —— 见 §4.1,需要用户拍板才动。

---

## 1. 冷启动:四条命令

```bash
cd F:/work/XuanMing-Server/python && .venv/Scripts/python.exe -m pytest tests/ -q --no-header -p no:randomly
```

全量套件,约 3 分钟,**必须全绿**。红了先别写新代码,先看是不是别的会话正在改同一批文件
(这个坑踩过很多次,见 §5.1)。

```bash
cd F:/work/XuanMing-Server/python && .venv/Scripts/python.exe -m ruff check --no-cache pandorapy/ tools/ tests/
```

**F821(未定义名)门禁**。抓的是"import 得动、一构造就 `NameError`"那类 —— 模块级 import
检查碰不到函数体里的名字,单测也未必覆盖到那条构造路径。真实教训见 §5.6。
刻意只开 `F821 / F811 / F841` 三条,**不开全量 lint**(批量改格式会淹掉真实 diff,§15)。

```bash
cd F:/work/XuanMing-Server/python && env PYTHONUTF8=1 .venv/Scripts/python.exe tools/parity/coverage.py
```

RPC 覆盖矩阵。加 `--write` 重生 [`python-migration-coverage.md`](./python-migration-coverage.md),
加 `--check` 当门禁跑。

> ⚠️ **`--check` 绿 ≠ 移植完了。** 它只抓**静默缺陷**(方法名不在 proto 里、servicer 类没继承
> 生成的基类),抓不到"这个 RPC 压根没写"。别拿它当完成度判据,完成度看上面那张表。

```bash
cd F:/work/XuanMing-Server/python && env PYTHONUTF8=1 .venv/Scripts/python.exe tools/gen_errcode.py --check && env PYTHONUTF8=1 .venv/Scripts/python.exe tools/gen_kafka_topics.py --check
```

两个生成物的漂移闸。**`errcode.py` 和 `kafka_topics.py` 是生成的,直接改会被下次生成覆盖** ——
要加错误码改 `tools/gen_errcode.py` 的模板再重生(这个坑踩过)。

### 环境备注

- Python 解释器一律用 `.venv/Scripts/python.exe`。**裸 `python` 在这台机器上会弹 Microsoft Store**。
- venv 里**没有 pip**,装包用 `uv pip install --python .venv/Scripts/python.exe <pkg>`。
- 中文输出要 `env PYTHONUTF8=1`,否则 stdout 撞 cp1252 报 `UnicodeEncodeError`
  (注意:**文件写入照样成功了**,别被这个报错骗去重跑)。
- Bash 工具的 heredoc 遇到嵌套引号/反斜杠会反复失败。**写大段 Python 就用 Write 工具落到
  scratchpad 再执行**,别跟 shell 转义较劲(这一场浪费了很多次往返)。
- 真 MySQL 在 `127.0.0.1:13306`(docker `pandora-mysql`)。连不上时依赖它的用例会**跳过**
  而不是失败 —— 看到 skipped 数从 10 跳到 40+ 就是它没了。

---

## 2. 现在的状态

### 2.1 可跑的 19 个

`login` `matchmaker` `battle_result` `team` `player` `auction` `guild` `player_locator`
`mission` `friend` `push` `leaderboard` `mail` `owner` `chat` `trade` `data_service`
`dialogue` `inventory`

"可跑"的判据是**真起过进程**:用未改动的 `*-dev.yaml` 启动,走完全部启动闸,打出 `service_ready`。
不是"单测过了"。

`inventory` 是 24/29 —— 缺的 5 个是 `BagService`,**Go 侧也是条件注册**(bag 域默认关),不是缺口。

### 2.2 已建的公共件(`pandorapy/`)

移植过程中沉淀的、被多个服务共用的模块。**改这些要跑全量**,因为 19 个服务都在用
(这一场就有两次因为改公共件而打穿正确的调用方,见 §5.5):

| 模块 | 干什么 | 关键约束 |
|---|---|---|
| `server.py` | grpc.aio + FastAPI 双服务器、优雅停机 | §9.16:health 打 NOT_SERVING **在** grpc stop 之前 |
| `safego.py` | 后台协程监督(对齐 Go `pkg/safego`) | `CancelledError` **不计为故障** |
| `sessiongate.py` | 会话现行性门(14 个服务需要) | 无 evidence header = **放行**(内网调用);权威不可达 = `UNAVAILABLE` |
| `kafkax.py` | KeyOrderedProducer / Consumer + `producer_conf_from()` | poison 跳过重试,**其它异常必须重试**;ProducerConf 只有一个映射点 |
| `redisx.py` | 拓扑选择 + 启动 Ping 闸 + `ActionQuota` | 空端点抛 `RedisEndpointMissingError`;`allow()` 返回 `(ok, exc)` 交回调用方 |
| `mysqlx.py` | DSN 解析 + 池参数翻译 | 解析不了**抛**不回落;`pool_kwargs` 显式列字段不 splat;`autocommit` 无默认值 |
| `config.py` | yaml 载入 | `ConfigLoadError` = 读/解析失败;`model_validate` 失败才是 scan |
| `configtable.py` | manifest / checksum + `ReloadMutex` | 热更整段持锁,`current_version` **读在锁内** |
| `dbguard.py` | §9.24 保留期 + 容量预算 | 保留期默认 `report_only`;bytes 只算 `DATA_LENGTH` |
| `protosql.py` | proto → MySQL 列类型 | string→`MEDIUMTEXT`、bytes→`MEDIUMBLOB` |
| `errcode_grpc.py` | errcode → gRPC status | 独立模块,因为 `errcode.py` 是生成的 |

### 2.3 机械闸

扫**全部** `services/*/*.py`,新服务自动纳入。**每一条都对应一次真实的、跟着模板复制的缺陷**:

| 闸 | 判据 | 复制了几份 |
|---|---|---:|
| `test_service_layer_contract.py` | `except BaseException` 前必须放行 `CancelledError` | 全部服务 |
| 同上 | `server.run(background=[...])` 不许传裸 lambda | 7 服务 12 处 |
| 同上 | 不许手写 `kafkax.ProducerConf(...)` | **11 服务 12 处,无一完整** |
| `test_config_load_scan_boundary.py` | `config_load_failed` 的判据必须是 `ConfigLoadError` | 15 服务 |
| `ruff --select F821` | 未定义名(构造时才炸的 `NameError`) | — |

`test_service_layer_contract.py` 里还记了**一条刻意没加的检查**(R5「player_id 必须取自
鉴权上下文」)和不加的理由。别再试一遍 —— 用正则做会误报,而误报的检查最终会被 noqa 掉
或整条删掉,等于什么都不剩。

---

## 3. 怎么验证(这一节最重要)

### 3.1 单元测试查不出这类缺陷

owner / dialogue 两个服务做过一次真进程并排跑,**抓到 7 条缺陷,没有一条是单测能发现的**。
形状全都是"单测全绿 + 日志无异常":

- 没注册 grpc health service → **22 个服务在 k8s 里永远不会 Ready**;
- 启动闸方向写反(该 fail-fast 的写成了 WARN);
- 后台循环名字丢了;
- 把 Go 已经修好的 bug 又移植回来了。

**所以:一个服务写完 ≠ 移植完。** 判据是拿未改动的 dev yaml 起真进程 + 跑端到端探针。

### 3.2 探针怎么写

现成的两个:`tools/parity/probe_owner.py`、`tools/parity/probe_dialogue.py`。方法论:

1. **先证明真的走到了目标分支** —— 探针最容易的失败模式是"什么都没测到但全绿"。
   每一步先断言前置状态,再断言结果。
2. **跨运行共享的资源要分段** —— 用不同 player_id / match_id 段,否则第二次跑撞到第一次的残留。
3. **端口占用不是失败** —— 撞到 Go 版占着端口,恰恰证明**全部启动闸都通过了**(走到 bind 那一步了)。
   换个端口再起。

### 3.3 新加的测试必须做变异验证

**"新测试全绿"本身没有信息量。** 加完之后把被测的那行代码改坏,确认对应用例变红:

```bash
cp target.py /tmp/t.bak && sed -i 's/if not changed:/if False:/' target.py
.venv/Scripts/python.exe -m pytest tests/test_x.py -q && cp /tmp/t.bak target.py
```

这一场每一批修复都这么验过。抓到过一条**假绿**的检查(§5.5),没有变异验证就发现不了。

### 3.4 改公共件之后

跑全量。19 个服务共用 `pandorapy/`,一处改动的爆炸半径是全仓 —— §5.5 里两次都是这么栽的。

---

## 4. 下一步做什么

### 4.1 两个 allocator —— **需要用户拍板,别自己开工**

`ds_allocator`(18990 行)+ `hub_allocator`(12654 行)= 31644 行,占 Go 后端总量的 30%。
目前只移植了核心不变量(各 300 行左右),RPC 全部为 0。

它们和其它服务不是一个量级:承载 §9.22 的 owner 权威、fencing、脑裂防护 —— 是全仓**唯一
不能出错**的部分。移植它们要么单独立项,要么明确决定不移植(Go 版继续跑,Python 侧只调用)。
**这是产品决策不是技术决策,必须先问用户。**

### 4.2 第 5 批复核的缺陷 —— 已修

| 服务 | 缺陷 | 形状 |
|---|---|---|
| `login` | **P0**:Hub 票据归属绑定校验**整段缺失** | §5.4 |
| 19 服务 | `config_load_failed` / `config_scan_failed` 边界写反 | §5.7 |
| `matchmaker` `inventory` | 配置表热更无互斥 → **版本静默回退** | §5.8 |
| `auction` | slot 失败分支比 Go 弱 3 处 | 下 |
| 16 服务 | `parse_go_dsn` 静默连错库 | §5.9 |
| `auction` | `shard_identity` 把 network 写死 `"tcp"` | 同上 |
| 11 服务 | `ProducerConf` 手抄 12 处,**无一完整** | 下 |
| 5 服务 | `ActionQuota` 让调用方的告警分支变成死代码 | 下 |
| `mission` | 2 条错误日志事件缺失 | 下 |
| `friend` | `block()` 少了"先查重复再查配额" | 下 |
| `guild` | `ds_callback_auth_rejected` 合并了 Go 的两个事件名 | 下 |

几条值得单独说的:

- **auction 的 slot 失败分支**:`except errcode.PandoraError:` 收窄了 Go 的 catch-all ——
  Redis 连接重置是裸异常,会直接逃出去,留下一张**已占配额名额、状态仍可恢复**的 PENDING 单
  (绕过 `max_active_orders_per_player` 的洞)。另外 `changed == False` 被静默放过
  (Go 抛 `ErrInternal`),终态化后也没退 escrow(只放了 owner 名额)。三条各有回归测试,
  各自变异验证过。

- **ProducerConf 手抄**:收敛前 11 个服务 12 处,**每一处都漏了 2~3 个字段**
  (`retry_backoff` / `read_timeout` / `write_timeout`)。后果全是同一种:**yaml 里配了,
  程序不用,且没有任何提示**。复核只抓到 mission 那一处;收敛到 `producer_conf_from()` 后一次全清。

- **ActionQuota**:`allow()` 原先返回裸 `bool` 并自己吞掉故障,于是五个服务里那段
  `except ...: log("<svc>_rate_quota_check_failed")` **整个是不可达死代码** —— 那几个 Loki
  告警键在代码里存在、永远不会触发,真正打出来的只有一条不带 `player_id` 的通用 warn。
  改成返回 `(ok, exc)`(对齐 Go 的 `(bool, error)`)。fail-open 的**方向**没变,变的是谁来记。
  连带发现:那几个 `test_rate_quota_failure_is_fail_open` 用例的替身是**抛异常**的,
  也就是说它们一直在验证一条生产上走不到的分支。

- **guild 的合并事件名**:Go 用两个名字区分 enforce(真的拒了)和 permissive(观察期放行)。
  合成一个之后,观察期里每条**放行**记录都长得像一次真实拦截 —— 告警成批响而一个请求都没被挡,
  观察窗口的意义正好被反转。

### 4.3 待修

- `mission`:`push_writer_lease.dial_timeout` 被整段丢弃。
- `mission`:`catalog.py` 照抄了 Go 的**服务级** `ValidateMissionCrossTables`(数组列),
  但没照抄生成器发的 fk `mission.reward_id → reward`。这是「Python 侧系统性跳过
  `tables.gen.go` 那 13 条生成 FK」的一类,不是 battle_result 独有的一次疏漏。
- `sessiongate.must_build` 没像 Go 的 `MustBuild` 那样建 Redis 客户端 + Ping。
- **10+ 个新可跑服务从没跑过并排探针**(§3.1 说的那种)。这是当前最大的未验证面。

**本轮已修(从 4.3 移出,留个落点免得下一个 agent 重做)**:

- `battle_result` 的跨表 FK(`drop.item_config_id → item`)已在 `catalog.py` 补上,
  文案与 Go `tables.gen.go` 逐字相同。⚠️ 原来那条写的是「manifest 缺表 / 跨表 FK」,
  **前半句是错的** —— `catalog.py` 的 `_load_one` 对 manifest 缺表一直是明确抛
  `ConfigTableError`,那道闸从来就在。只有跨表 FK 是真缺的。
- `dbguard.Outcome` 已补 `truncated`。连带修掉它造成的真 bug:player 的保留期清理循环
  拿不到这一位,只好从 `matched/deleted` 推,而 DELETE 档下这两个字段是同一个数 ——
  判据恒真,每轮只删一批就退出。battle_result 里那个本地 fork 的 `SweepOutcome`
  是否收编回共享件另行拍板,本轮没动。

### 4.4 已核实**不是**缺陷的(别照着复核报告改)

- **auction `AuctionUsecase.__init__` 缺 Go 的"二次默认层"**:Python 的 `apply_defaults()`
  7 个字段全覆盖,而生产唯一构造路径就是 `Config.load()` → `apply_defaults()`。
  Go 那层是防御性冗余,不是行为差异。按 §15.3 不照抄。

---

## 5. 别再踩的坑

### 5.1 并发会话

**这个仓库经常有多个会话同时在改。** 症状:测试报某文件 SyntaxError / 有 null bytes /
FakeRepo 缺方法 —— 十有八九是别人正写到一半,几分钟后自己就好了。

判别:先重跑一遍那个文件的测试。还红再看内容。**别急着"修"别人写了一半的文件。**

### 5.2 我自己写错又被自己的测试抓住的两条

留在这里因为它们是**方向性**的错,容易再犯:

1. **kafka consumer 把所有异常都当 poison** → 所有瞬时故障都跳过重试。
   根因:Go 用 panic(poison)和返回 error(可重试)区分,Python 没有这个区分。
   修法:只有 `PoisonError` 跳过重试,两个方向都补了测试。
2. **测试用 `sleep(0.05)` 然后 `stop()`** → agent 负载高时 `stop()` 打断了重试循环,变成 flaky。
   修法:`until=` 改成**必填**,等各自的可观测量。弱默认值(poll 返回就触发)比没有更糟。

### 5.3 已被证伪的结论(**别再去查**)

- **owner 的 `repo.query()` 陈旧快照隐患:不存在。** 对真 MySQL 用 `maxsize=1` 探过,
  asyncmy 在归还连接时会重置事务。回归测试钉在 `tests/test_owner_repo.py`。
- **chat 的"四条默认值漂移":方向反了。** Go 的真实默认值和 Python 原来的一致。
- **`pkg/svc.MustNewBaseContext`:死代码**,零服务调用。
- **`killswitch.SetDefault`:零生产调用者** —— Go 也是 fail-open。
- **`namecheck`(1154 行)/ `grpcstats`(345 行):零服务调用者**,已从移植清单剔除。
- **Go 的 `Idempotent` 字段注释写着「默认 true」:注释是错的。** 没有任何代码设置它,
  零值就是 `false`。**移植时以代码为准,不以注释为准。**

> 这一条的通用形式:**每次修复前先去 Go 里核对一遍方向。** 给每个修复 agent 都加了这条
> 硬要求,它当场救回了 chat 那次"修反了"。

### 5.4 login 的 P0(已修,形状最值得记住)

`verify_ds_ticket` 把 Hub 票的归属绑定校验**整段删了**,还留了一句注释
`# v1 票不带 ds_pod 绑定` 作为依据 —— **那句注释是错的**,而且它和同一个包里
`dsticket.py` 自己解出那些字段的代码**直接矛盾**。

后果是 §9.3 的四道门一起塌:A 玩家的 Hub 票能在 B 台 DS 上兑换;Transfer / Release 后
旧票永久有效;半绑定票被放行;`require_hub_assignment_binding` 栅栏形同虚设。

**教训:一段"我们不需要这个"的注释,如果和同包代码矛盾,它就是复核时最该先看的东西。**

修复落在 `pandorapy/services/login/hubbinding.py`(移植 Go 的
`internal/data/hub_assignment_binding.go`,含 `A1 → MGET → A2` 双采集线性化证明),
配 18 条回归测试。顺带修的两条同源问题:

- `dsticket.py` 漏解 `release_track` → 归属校验里所有灰度轨道判据因恒为空串被跳过;
- `mark_used(jti)` 缺 `jti != ""` 前置判据 → 空 jti 会铸一个**全局共享**的防重放键
  (第一张空 jti 票畅通,第二张起全被判重放)。

刻意**没有**移植 Go 的 v2 分支(`CheckCurrentB1`):`dsticket.py` 的解析器把 `version`
硬编码成 1,那条分支永远走不到。移植一条走不到的分支只会让人以为 v2 已经支持了。

### 5.5 改公共件会打穿**正确**的调用方(两次)

1. **`ConfigLoadError`**:给 `load_yaml` 加包装,修好了 15 个只捕 `FileNotFoundError`
   的服务 —— 但 matchmaker / player_locator / team **本来就捕了完整四元组**
   `(FileNotFoundError, PermissionError, yaml.YAMLError, UnicodeDecodeError)`,
   加包装后那些元组**再也匹配不上**,yaml 错反而掉进 catch-all。
2. **`parse_go_dsn` 加 `net` 字段**:`pool_kwargs` 用 `**dsn` 整个 splat 进
   `asyncmy.create_pool()`,多一个 kwarg 就 TypeError → **19 个服务里凡是连库的全部起不来**,
   而报出来的事件是 `mysql_init_failed`,跟"DSN 解析"看不出任何关系。
   已改成显式列字段(解析结果以后随便加,第三方参数只在一处翻译)。

**通用形式:改共享件时必须排查所有调用点,不能只改"错的那些"。**

### 5.6 只 import 模块是抓不到 `NameError` 的

给两个 `Store` 加 `self.reload_mutex = ReloadMutex()` 时漏了 import(我自己的补丁脚本有个
顺序 bug:判据 `if "ReloadMutex" not in s` 跑在插入**之后**)。

`import pandorapy.services.inventory.catalog` **全绿** —— 因为 `NameError` 要到
**构造 Store 时**才炸。4 个 inventory 用例在全量里红,而我的验证根本没碰到那行。

已接 `ruff --select F821` 门禁,实测它当场指到那一行。

### 5.7 `config_load_failed` 的分界

Go 的 `c.Load()` 覆盖"读文件 + 解析 yaml"两步,两者失败都是 `config_load_failed`;
`c.Scan()` 才是 `config_scan_failed`。Python 侧 **15 个服务只把 `FileNotFoundError` 归 load**,
于是 yaml 语法错 / 权限拒 / 编码坏 / 根节点非 mapping 全被报成 `config_scan_failed` ——
运维照着事件名去查"哪个字段填错了",而真实原因是文件根本没读成。

修法不是改 15 处,而是在 `pandorapy/config.py` 加有类型的 `ConfigLoadError`,
把"归哪一类"的判断**收在产生错误的那一侧**,再加机械闸。

### 5.8 配置表热更的版本静默回退

原形状把 `current_version` 读在 `await` **之前**:

```
A 读 current=4 → A 让出
B 读 current=4 → B 加载 v6 → 判 6>4 通过 → 切到 v6
A 加载 v5      → 判 5>4 通过(**用的是过期的 4**) → 切回 v5
```

内存生效 v5,两次热更都返回成功,运维视图上是 v6。§9.15 的版本单调闸**本身**因为读了
过期基准而被绕过。Go 那边 `Store.Load` 整段持 `s.mu`。

已加 `pandorapy/configtable.ReloadMutex`,`current_version` 读进锁内。回归测试里有一条
**刻意跑无锁路径**、断言"版本确实被回退了" —— 不先证明竞态真的会发生,"加锁后没事"没有信息量。

### 5.9 `parse_go_dsn` 静默连错库

原实现 `dsn.partition("@tcp(")`:对不含 `@tcp(` 的 DSN,partition 返回 `(dsn, "", "")`,
于是 host 回落 `127.0.0.1`、port 回落 3306、db 变空串,socket 路径被整个吞进 password 字段,
**没有任何错误**。实测:

```
parse_go_dsn("u:p@unix(/var/run/mysqld.sock)/pandora_x")
→ {'host': '127.0.0.1', 'port': 3306, 'db': '', 'password': 'p@unix(...)/pandora_x'}
```

这条路径上有 16 个服务的**全部** DSN(44 处调用)。配置写错 → 服务照常起来 →
连的却不是你以为的那个库。开发机上 127.0.0.1:3306 往往真的有一个 MySQL,于是"跑起来了"。

现在解析不了一律抛,与 `redisx` 的"空端点不许静默连 127.0.0.1"同一条纪律。

### 5.10 机械检查本身会出错(四次)

1. 覆盖率闸把 `set_ds_callback_guard()` 当成 RPC 报错 → 只收 PascalCase 方法名。**误报**
2. CancelledError 闸把 `except BaseException: rollback(); raise` 报成违规(13 处)→
   放行块级裸 `raise`。**误报**
3. 后台点位名闸**恒绿** —— 正则判据 `\(\s*["']` 恰好也匹配 `safego.loop("x"`,
   每个裸 lambda 都被自己内部的调用抵消掉。换 AST 后当场炸出 7 服务 12 处。**漏报**
4. `config_load_failed` 闸的 6 行前瞻窗口**溢进下一个 try 块**,把打 `abs_conf_path_failed`
   的 `except OSError` 也报成违规(7 处)→ 换 AST 只看 handler 自己的块体。**误报**

**误报会被 noqa 掉、漏报让人以为已经保住了 —— 漏报更糟。**
通用教训:**能拿到确定答案(AST)时就别猜文本(正则)。** 四次里三次换成 AST 就对了。

---

## 6. 材料在哪

| 要找什么 | 去哪 |
|---|---|
| 整体设计、双栈拓扑、各章决策 | [`python-migration.md`](./python-migration.md) |
| 逐服务缺口清单(第 1~4 批复核产出) | [`python-migration-remaining.md`](./python-migration-remaining.md) |
| RPC 覆盖矩阵(生成物) | [`python-migration-coverage.md`](./python-migration-coverage.md) |
| 第 5 批各 agent 的完整返回值 | `.claude/projects/F--work/<session>/subagents/workflows/wf_1397a513-659/journal.jsonl` |
| 探针范例 | `python/tools/parity/probe_owner.py`、`probe_dialogue.py` |

> journal.jsonl 里每个 agent 一行 `{"type":"result",...}`,含 `gates_ported` /
> `rpcs_implemented` / `atomicity_carriers` / `honest_gaps` 等结构化字段。
> **诊断"workflow 返回空"之前先读它** —— 别假设 agent 真的返回了东西。

---

## 7. 交接时要说清的三件事

写下一份交接文档时,至少覆盖:

1. **完成度用什么判据** —— "可跑"是起真进程走完启动闸,不是单测绿。
2. **哪些结论已经被证伪** —— 不写的话下一个人会把同样的路再走一遍
   (§5.3 那 6 条、§4.4 那 1 条,每条都花过时间)。
3. **哪些是刻意没做的** —— 两个 allocator、`BagService`、v2 票据分支、
   auction 的二次默认层。**不标注的话会被当成漏项补上**,而补一条永远走不到的分支
   只会让人以为它支持了。
