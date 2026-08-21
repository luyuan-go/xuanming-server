# Go → Python 迁移交接（2026-08-18/19）

> **冷启动请先看 [`python-migration-handoff.md`](python-migration-handoff.md)** —— 那份是给
> 「完全没有上下文的会话 10 分钟内接着干」写的：现在到哪了 / 怎么验证没弄坏 /
> 下一步做什么 / 哪些坑与已证伪结论别再走一遍。本文是**设计与决策**的全集，更长更细。
>
> 本文是迁移工作的设计入口：已迁什么、验了什么、剩什么、下次别再踩什么。
> 配套两份生成物：[`python-migration-coverage.md`](python-migration-coverage.md)（RPC 覆盖矩阵，
> 由 `coverage.py` 现算）与 [`python-migration-remaining.md`](python-migration-remaining.md)
> （逐服务剩余工作，含每个服务要写哪些文件、要搬几道闸）。
> 代码在 `python/`；怎么在那个目录里干活（装环境 / 起依赖 / 行尾约定）见
> [`python/README.md`](../../python/README.md)，那边刻意不重复状态，避免两处漂移。
>
> 跑测试：`cd python && PYTHONUTF8=1 .venv/Scripts/python.exe -m pytest tests/ -q -rs`
> **判据是「全绿 且 0 skipped」** —— 缺 etcd / MySQL / Redis 时会静默跳过一大批最危险的用例。

## 0. 一句话现状

**21 个服务全部有核心落码，30 个基础件成型，2 万余行 Python / 1047 条测试全绿且 0 skipped。**
但**服务的业务代码远未逐行移植** —— 本轮刻意优先迁"写错了不报错"的那部分（详见 §3）。

按 **RPC 方法**这个机械口径，全仓 21 个服务实际注册了 211 个 gRPC 方法。
当前覆盖见 [`python-migration-coverage.md`](python-migration-coverage.md)（现算，别信手写数字）。
这张表由 `python/tools/parity/coverage.py` 现算，不手写 —— 见 §2.3。

**样板服务已跑通两个**（2026-08-19）：`dialogue`（内存态）与 `owner`（真 MySQL + §9.22 权威）
都能用**同一份 `etc/*.yaml`** 起进程、双端口在线、与 Go 版并排跑同一批场景**逐字节零差异**
（dialogue 18 场景 / owner 26 场景）。这一步抓出 4 个会让**全部 21 个服务在 k8s 里永不 Ready**
的基础件缺陷，和 3 个 owner 权威面的移植缺陷 —— 全部是"单测全绿、日志无异常"的形状，
只有真起进程并排比才暴露（详见 §5.2.2）。

**已进 CI 门禁**（2026-08-19）：`tools/scripts/ci_backend.ps1` 现在跑
`gen_errcode.py --check` + `pytest -rs`，数据库组的跳过在 `-RequireDbTests` 下判失败。
在此之前这近 600 条测试对 CI 等于不存在 —— 有测试但不是门禁，和当初集群配置生成器
契约测试的处境一模一样。

## 1. 技术栈定稿

| 层 | 选型 | 关键理由 |
|---|---|---|
| RPC | **grpcio (`grpc.aio`)** | 客户端与 Envoy **零改动** —— Envoy 看到的仍是 h2c gRPC，不知道后面换了语言 |
| HTTP | **FastAPI + uvicorn** | 只扛 10 个 `google.api.http` 端点（**全在 login**）+ `/metrics`；其余 20 个服务的 http.go 只挂 `/metrics` |
| IDL | **不变**（60 个 `.proto` + buf） | `buf breaking` 的 FILE 级向后兼容检测保留 |
| MySQL/TiDB | `asyncmy` | TiDB 就是 MySQL 线协议，无需专用 SDK |
| Redis | `redis-py` (async) | **190 处 Lua 脚本原样搬，一个字符不改** |
| Kafka | `kafka-python` 3.x | 纯 Python 无 C 扩展（开发机 360 会拦二进制 wheel） |
| etcd | `aetcd` | ⚠️ 生态最弱的一环，且**它的失败语义与直觉相反**，见 §4.3 / §4.3.1 |
| 日志 | `structlog` | 字段口径必须逐字对齐 zap（见 §5 陷阱 ⑩） |
| 选主 | 自研 `etcdleader` / `writerlease` | `aetcd` **没有**自动 keepalive，续约全靠自己写 |

**架构不变的部分**：Envoy、Agones、Grafana/Loki/Alloy/Prometheus、K8s manifest、96 个 SQL 迁移。

## 2. 已迁清单

### 2.1 基础件（`python/pandorapy/`，30 个模块）

| 模块 | 行 | 对应 Go | 备注 |
|---|---:|---|---|
| `errcode` | 495 | `pkg/errcode` | **生成**（`tools/gen_errcode.py`），165 个码，有 parity 门 |
| `etcdleader` | 358 | `pkg/leader/etcdleader` | key 布局与 `concurrency.Election` 一致（否则两栈各选各的 leader） |
| `configtable` | 331 | `pkg/configtable` | |
| `writerlease` | 309 | `pkg/dsauthfence/writerlease` | 激活超时是**唯一**逃生口，见 §4 |
| `log` | 285 | `pkg/log` | 7 个字段名逐字对齐 zap |
| `dbguard` | 275 | `pkg/dbguard` | `sql_mode` fail-fast + 保留期默认 report_only |
| `server` | 235 | `pkg/grpcserver` + `transport/http` | grpcio + FastAPI 双端口 |
| `snowflake_etcd` | 222 | `pkg/snowflake/etcdnode` | 失租**必须退出进程**（与 etcdleader 相反） |
| `fence_timeline` | 205 | 跨 5 处常量 | 七条不等式的**唯一校验入口**，见 §4 |
| `auth` | 197 | `pkg/auth` | 账号态/玩家态 audience 分离；经 Envoy 的不设 kid |
| `killswitch` | 193 | `pkg/killswitch` | 刻意 fail-open（运维工具不是故障开关） |
| `interceptors` | 192 | `pkg/middleware` | |
| `protosql` | 182 | 替代 `proto2mysql` | 从 proto 描述符推导 DDL |
| `redisx` | 168 | `pkg/redisx` + `redislock` | Lua 原样搬策略的载体 |
| `cellroute` | 161 | `pkg/cellroute` | `logical_cell = player_id % 4096` |
| `source_revision` | 160 | `pkg/placement/source_revision` | INC-20260818-003 的修复 |
| `kafkax` | 140 | `pkg/kafkax` | 一致性哈希与 Go **逐位一致** |
| `config` / `snowflake` / `mysqlx` / `placement` / `logwindow` / `godur` / `metrics` / `_utf8` | 806 | 各对应 | |

### 2.2 服务（`python/pandorapy/services/`，21 个）

按落码深度分三档：

**A. 核心链路完整**（可作后续深化的模板）

| 服务 | Py 行 | 迁了什么 |
|---|---:|---|
| `owner` | 1107 | 用例层 + 类型 + MySQL 事务层（§9.22 owner 权威） |
| `trade` | 1051 | 状态机 + Redis WATCH/MULTI/EXEC + 结算围栏（INC-20260722-001） |
| `dialogue` | 958 | 全服务（首个，含 main 装配） |
| `chat` | 652 | 五频道 + 限流 + 私聊落库 |
| `friend` | 424 | 数据层全套（RC 隔离 + 守卫行锁序） |
| `data_service` | 394 | cache-aside + proto→DDL |

**B. 关键不变量已落**（业务外围未迁）

`guild` 294（临时群名额）· `mission` 268（事实引擎）· `auction` 259（三层幂等键）·
`push` 247（投递游标 Lua）· `player_locator` 215（TTL 下限 + 代际闸）· `mail` 194（三形态附件）

**C. 单点核心**（体量最大的几个，只迁了最危险的那一处）

| 服务 | Py 行 | 迁了什么 | Go 剩余 |
|---|---:|---|---:|
| `hub_allocator` | 190 | 来源版本铸号 + 容量账本派生 | 12,624 |
| `ds_allocator` | 180 | 孤儿 GS 四重防误删 | 18,958 |
| `matchmaker` | 158 | 在线闸（UNKNOWN 放行的非对称判据） | 8,547 |
| `inventory` | 158 | 对转结算幂等键 + 溢出守卫 | 7,320 |
| `battle_result` | 141 | 名单集合比对 + 计分白名单 | 7,425 |
| `leaderboard` | 108 | 直方图估算 | 2,192 |
| `team` | 102 | ready 代际（INC-20260813-001） | 5,932 |
| `player` | 89 | 经验曲线进位 | 5,470 |

### 2.3 RPC 方法级覆盖（机械推导，别手写）

`python/tools/parity/coverage.py` 把三份事实拼起来现算：proto 里声明的 rpc、
Go 侧 `internal/server/*.go` 实际 `RegisterXxxServer(` 的 servicer、
Python 侧继承 `*Servicer` 的类实现了哪些方法。生成物在
[`python-migration-coverage.md`](python-migration-coverage.md)（**勿手改**）。

```bash
python tools/parity/coverage.py            # markdown 矩阵
python tools/parity/coverage.py --check    # 门禁：只拦"写了但不生效"
```

**为什么要机械算**：一个 Go 服务经常注册**不止一个** servicer，只看与服务同名的那个
proto 会把 RPC 面算少一截，而"算少了"本身没有任何信号：

| 服务 | 同名 proto | 实际还挂了 | 真实 RPC 数 |
|---|---:|---|---:|
| `inventory` | 23 | `BagService`(5) + `ConfigTableAdminService`(1) | **29** |
| `player` | 28 | `ConfigTableAdminService`(1) | **29** |
| `guild` | 14 | `GroupService`(9) | **23** |
| `ds_allocator` | 7 | `GmService`(3) + `ConfigTableAdminService`(1) | **11** |
| `matchmaker` | 6 | `ConfigTableAdminService`(1) | **7** |

`ConfigTableAdminService`（配置表热加载）挂在 4 个服务上且都是**条件注册**，
是最容易整个忘掉的一类 —— 忘了它，那 4 个 Python 副本收不到热加载指令，
表面一切正常，只是配置永远停在启动时那一版。

`--check` 刻意**不**把"某个 rpc 还没迁"判失败（那样第一天就是红的，门禁会被关掉），
只拦两类静默缺陷：Python servicer 里有 proto 不存在的方法名（拼错 ⇒ grpcio 按 proto 名
分发，该方法**永远不会被调用**，那个 RPC 实际返回 UNIMPLEMENTED），
以及名字像 servicer 却没继承生成基类的类。牙齿验证在 `tests/test_parity_coverage.py`。

> 顺带澄清一个反复出现的数字分歧：`deploy/k8s/services/services.yaml` 里是 **22** 条，
> 代码里是 **21** 个服务 —— 差的那条是 `matchmaker-pve`，同一份 matchmaker 二进制的第二个
> 部署（另一份配置），不是第 22 个服务。

## 3. 选择标准：为什么迁这些、不迁那些

**判据是「写错了会不会报错」，不是代码量。**

优先迁的是这类：改错了**不会有任何运行期信号**，只在故障时表现为数据错乱 / 脑裂 / 资产消失。
留到后面的是 CRUD、DTO 转换、列表拼装 —— 它们写错了会当场报错或被普通测试抓住。

具体地，本轮迁的核心集中在六类：

1. **原子性载体** —— Lua 脚本、`WATCH/MULTI/EXEC`、`FOR UPDATE` 事务
2. **幂等键格式** —— 它们是账本去重键，格式变一个字符就是重复入账
3. **时序常量与不等式** —— fence / lease / 屏障
4. **代际 / 版本 fencing** —— ready 代际、来源版本、owner epoch
5. **fail-open vs fail-closed 的方向判断** —— 每一处的方向都是设计决定
6. **跨语言必须逐位一致的纯函数** —— 分区器、桶归属、经验曲线、版本编码

## 4. 验证证据（可复跑）

### 4.1 跨语言对拍（4 条 go-run，直接跑 Go 取输出逐条比）

| 对象 | 样本 | 结果 |
|---|---|---|
| Kafka 一致性哈希 | 207 键 × 8 分区（含中文键） | 0 不一致 |
| leaderboard 桶归属 | 2456 条（**1224 条负分**） | 0 不一致 |
| 经验曲线进位 | 2835 条（5 曲线 × 9 等级 × 7 经验 × 9 delta） | 0 不一致 |
| 来源版本编码 | 56 条（含 **26 个应拒组合**） | 0 不一致 |
| friend RR 死锁对照 | 16 并发 × 两种隔离级别 | RR **13/16 死锁** / RC 0/16 |

> 每次都额外确认了「Go 程序真的跑了」而不是静默 skip —— `go run` 有编译缓存所以很快，容易误判。

### 4.2 打真依赖验证（4 次）

| 验证 | 工具 | 结果 |
|---|---|---|
| etcd 三大语义 | `python tools/verify_etcd.py --docker-container <name>` | **11/11 PASS** |
| owner 事务不变量 | `pytest tests/test_owner_repo.py` | 真 MySQL 8.4.9 |
| push 投递缓冲 Lua | `pytest tests/test_push_offline.py` | 真 Redis 8.10 |
| writerlease 故障注入 | `pytest tests/test_writerlease.py` | 激活阻塞对照实验 + 带外 revoke |
| nodeID 抢占故障注入 | `pytest tests/test_snowflake_etcd.py` | 号段下界 / 不 revoke / 失租被发现 |
| 干净环境可复现 | 新建 venv 按 README 装 `.[dev]` 后跑全量 | 修依赖声明前 **3 个 collection error**，修后 629 passed |

**起依赖**：

```bash
docker run -d --name pandora-etcd-verify  -p 12379:2379 quay.io/coreos/etcd:v3.5.17 etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://127.0.0.1:12379
docker run -d --name pandora-mysql-verify -p 13306:3306 -e MYSQL_ROOT_PASSWORD=pandora_dev_root -e MYSQL_DATABASE=pandora_owner mysql:8.4
docker run -d --name pandora-redis-verify -p 16379:6379 redis:8-alpine
```

没有依赖时对应测试**整体 skip 并说明**，不假装通过。

### 4.3 etcd 是生态最弱的一环（结论已实测）

Python 侧 etcd v3 客户端的天花板是 `kragniz/python-etcd3`：**450★、202 个 open issue、20 个月未更新**。
活跃的 `martyanov/aetcd` 只有 **33★**。对比 Go 的 `clientv3`（etcd 服务端同一团队，52k★）。

实测结论（`tools/verify_etcd.py` 11/11）：**aetcd 的 lease / watch / txn 语义可用**，
关键的是 etcd 挂掉时它**抛异常而不是返回 None**（`raised=True returned_none=False`）——
返回 None 会让上层把"查不到"当成"确实没有 owner"，直接放出第二个 owner。

但它**没有 Go `clientv3.KeepAlive` 的自动续约**，所以：

- `etcdleader` / `writerlease` / `snowflake_etcd` 的续约循环全部自己写
- 三处都实现了**本地安全截止线**（monotonic 时钟）；余量取法不同：
  `etcdleader` / `snowflake_etcd` 提前 TTL/3，`writerlease` 提前**固定 3s**
  （`HOLD_SAFETY_MARGIN_SEC`，覆盖一次续租往返 + 时钟抖动）
- 这是整个迁移里最容易写出脑裂的地方，改动前先跑 `tests/test_writerlease.py`

### ⚠️ 4.3.1 更正（2026-08-19 对抗审计）：上面那句"语义可用"过于乐观

`verify_etcd.py` 验的是 aetcd **有没有**正确的 lease / watch / txn 语义，答案是有。
但它没验一件事：**语义正确 ≠ 调用方式正确**。真 etcd 实测：

```
put(key, lease=2s) → 睡 4s → key 已消失（lease 确实过期了）
await lease.refresh()   ← 不抛异常
→ LeaseKeepAliveResponse(ID=..., TTL=0)
await lease.remaining_ttl() → -1
```

对一个**已经不存在**的 lease 调 `refresh()`，etcd 的应答是「正常返回、TTL 置 0」。
于是 `try: await refresh() / except: 算失败` 这种写法有一个**永久静默**的洞：
租约早没了、key 早被别人抢走，而本副本因为"没抛异常"一直把本地安全截止线往后推 ——
它会**永远**认为自己还持有。

**上面三个模块原本全是这么写的**，后果分别是：

| 模块 | 后果 |
|---|---|
| `writerlease` | 两个副本同时对外宣告可写 → fence 水位被两个写者推（§9.22 脑裂） |
| `etcdleader` | 两个副本同时跑撮合循环 |
| `snowflake_etcd` | 两个副本用同一个 nodeID 发号 → 重号（§9 不变量 11） |

修法是把"续约一次"收成一个共用件 `pandorapy/etcdlease.refresh_or_raise`，
并把两件事**分开**：连接层失败（可按本地安全窗重试）vs 服务端回 TTL≤0
（已确定失主，必须立即让位、不得重试）。**新写 etcd 租约代码一律走它。**

教训一般化：`verify_etcd.py` 那类"验依赖行为"的工具证明的是 SDK 能做对，
证明不了**我们用对了**。这两件事要分开验。

### 4.4 fence / lease 时间线（落 allocator 代码前的前置验证）

```
t=0s     旧 DS 最后一次成功心跳
t=15s    Battle DS 判弃(段位回滚)
t=20s    旧 DS 自我 fencing            ← 最晚停止可玩
t=27s    服务端再入屏障打开            ← 最早开始可玩
t=30s    Hub 判超时 / presence 蒸发

安全余量 = 7s    违规项 = []
```

`pandorapy/fence_timeline.py` 把散在**五个地方**的常量收成七条可断言的不等式，
每条测试都用 `monkeypatch` 破坏它来证明检查有效。改任何一个常量都会当场变红。

## 5. 移植陷阱清单（下次遇到什么形状要警觉）

### 5.1 Python 比 Go 宽松，必须显式收紧（3 处，全部踩到过）

| 陷阱 | 形状 | 修法 |
|---|---|---|
| **正则 `$` 匹配末尾换行** | `re.match(r"^\w+$", "a\n")` **有匹配**；Go 的 `$` 没有这个行为 | 一律 `\A...\Z` |
| **`int` 无限精度不溢出** | 照抄 Go 的"检查回绕"一条都检不出来 | 显式检查 `_MIN_INT64` / `_MAX_INT64` |
| **`dict` 有序 ≠ 排序** | Go 的 map 遍历随机所以那边显式 sort；Python 保插入序但插入序 ≠ 排序 | 同样显式 `sorted()` |

### 5.2 我在移植中引入并已修复的 9 个缺陷

全部已修，**且每条都有会红的回归测试**（见 §5.4 复跑命令）。

| # | 缺陷 | 静默后果 |
|---|---|---|
| ① | `redisx.LuaScript` 缓存了第一个 client | 连接池替换后脚本打到旧连接；**单独跑全过、全量跑挂** |
| ② | （非我引入）friend RR 死锁 | 16 个**无共享行**的并发申请 13 个死锁 |
| ③ | 非 target 返回 `ErrUnauthorized` | 泄露"这条申请确实存在"，可探测他人社交关系 |
| ④ | 猜测 `TransferAttachment` 字段名 | 测试当场炸；运行期会表现为字段恒 0 |
| ⑤ | 正则用 `^...$` | 带尾换行的幂等键进 uk 索引和日志 |
| ⑥ | 溢出守卫照抄 Go 的回绕检查 | 算出 Go 表示不了的金额，写库时静默截断 |
| ⑦ | 漏了 JWT 密钥长度校验 | 短密钥可暴力破解 → 伪造任意 `player_id` |
| ⑧ | `lost_since` 拿边界值当结论 | 什么都没丢也报告丢失 → 客户端每次连上都全量 resync |
| ⑨ | writerlease 激活期不续约 | 与 Go 行为不同；**测试过了但过错了原因** |

⑨ 的教训值得单列：第一版测试**通过了**，但对照实验显示拆掉激活超时**照样通过** ——
救场的是 lease 自然过期而不是那道闸。查 Go 才发现它用 `concurrency.NewSession`
（后台自动续约），激活期间 key 一直在。对齐后实验才有牙齿：

```
有期限(3s)      B接管=True    A激活失败计数=1
无期限(9999s)   B接管=False   A激活失败计数=0   ← 无写者且完全静默
```

### 5.2.1 对抗审计批次（2026-08-19）：又 12 条，全部是"测试全绿但门是开的"

上面 9 条是移植过程中自己发现的。这一批来自一轮**六维对抗审计 + 逐条复核**
（61 条确认、7 条证伪），下面是已修的部分。共同形状：**没有任何运行期信号**。

| # | 缺陷 | 静默后果 | 位置 |
|---|---|---|---|
| ⑩ | 续约把 `refresh()` 未抛异常当租约存活 | 失主后永久宣告持有 → 双写者 / 双 leader / 重号 | `etcdlease.py`（三处共用件） |
| ⑪ | nodeID 抢占从 0 起扫（Go 从 8 起） | 领到 UE DS 本地发号器（恒 0）与 static 副本正在用的号 | `snowflake_etcd.py` |
| ⑫ | `Holder.close()` 主动 revoke lease | 秒级粒度下新副本同秒抢到同号、从 step 0 重数 → **逐位重号** | `snowflake_etcd.py` |
| ⑬ | fencing token 用 lease id 而非 CreateRevision | lease id 是 57 bit，`source_revision` 只有 40 位任期段 —— **一个号都铸不出来**；且跨任期不单调 | `writerlease.py` |
| ⑭ | owner 来源版本门写成 `if source_revision > 0` | 见过版本后，旧写者只要**不带版本**就能绕过整道门（INC-20260818-003 的形状本身） | `services/owner/repo.py` |
| ⑮ | 同一版本号指向不同 target 被放行 | 两个共用同一任期的写者互相覆盖（全序前提被打破却没人拦） | `source_revision.py` |
| ⑯ | 配置表 protojson 未开 `DiscardUnknown` | 标准发布序是"先发配置再滚二进制"，dist 一加列旧进程**整批拒载** | `configtable.py` |
| ⑰ | panic 兜底把 `CancelledError` 也算 panic | 每次客户端超时都产生 ERROR + panic 计数，deadline 风暴造出假告警并淹掉真异常 | `interceptors.py` |
| ⑱ | 日志级别读 `PANDORA_LOG_LEVEL` | 全仓 7 处注释把 `LOG_LEVEL=debug` 写成标准排障手法，对 Python 副本无效 | `log.py` |
| ⑲ | Redis 锁 key 不带 `pandora:lock:` 前缀 | 两栈并存时同一把锁落成两个 key，**互斥当场失效**且双方都显示加锁成功 | `redisx.py` |
| ⑳ | `_lead_until_lost` 每轮泄漏一个 `lost.wait()` Task | 稳定当选 7 天 ≈ 12 万个常驻 pending Task | `etcdleader.py` |
| ㉑ | `godur` 用 `%g` 收尾（6 位有效数字） | 同一时长在两栈日志里长得不一样 | `godur.py` |

另修两处**工程接线**（不是代码缺陷，但后果同级）：

- **`pyproject.toml` 少声明 `aetcd` / `PyJWT`**，`redis` 只在可选组。本机 `.venv` 是手工装过才绿的；
  照 README 在干净机器上装完 `.[dev]`，`pytest` 直接 **3 个 collection error**。
  实测修前修后（2026-08-19 当时）：`3 errors` → `592 passed`。
- **三个 MySQL 数据层测试在 CI 上必然整体跳过**：CI 发的是**无库名** DSN，而 ci-db 的 mysql
  没有任何 init 脚本（一个库都没有），测试却直接拿默认库名去连 → 连不上 → skip → 打绿。
  已改成自建库（与 Go 侧 `*_mysql_test.go` 同做法）。实测把 `pandora_owner` / `pandora_social`
  两个库全删掉 + 无库名 DSN，51 个用例照样全过。

**每一条都补了会红的回归测试，并逐条做了变异验证**（把修复拆掉，确认测试当场变红）：

```
拆掉 TTL<=0 判定       → test_revoked_lease_stops_the_writer            红
号段改回从 0 起扫       → 3 failed                                       红
close() 改回 revoke     → test_close_does_not_revoke_the_lease           红
token 改回 lease.id     → test_term_token_is_usable_as_a_source_revision 红
续约挪到激活之后        → test_lease_survives_an_activation_longer_...   红
errcode 改一个码值      → gen_errcode.py --check exit=1                  红
```

### 5.2.2 样板服务落地批次（2026-08-19）：7 条，全是"起得来、跑得对、就是坏的"

把 Go 与 Python **起在同一份 conf、同一个库上**跑同一批场景逐字节 diff，抓到的。
共同点：单元测试全绿、服务日志一行 ERROR 都没有、业务 RPC 全部可用。

**A. 基础件（影响全部 21 个服务）**

| # | 缺陷 | 后果 | 判据 |
|---|---|---|---|
| 1 | 没注册 `grpc.health.v1.Health` | `services.yaml` 里 **22 个服务**的 readinessProbe 都是 `grpc: {port}`，k8s 原生探针调的就是 `Check("")`。Python 答 `UNIMPLEMENTED` ⇒ **Pod 永不 Ready，滚动更新直接卡死** | `test_server_readiness_contract.py::test_build_grpc_server_registers_health` |
| 2 | 用了**同步版** `HealthServicer` | 挂 `grpc.aio` 上 `Check` 返回非 awaitable，请求以 `UNKNOWN` 失败。现象与 #1 一模一样（探针失败），根因完全不同 | 同上 `::test_health_servicer_is_async_variant` |
| 3 | 拦截器无条件 `await inner(...)` | 比 grpcio 本体更严（它是 `if isawaitable(x)`）。任何第三方**同步** unary servicer 挂进来都以 `UNKNOWN` 失败 —— #2 就是被它打挂的 | 同上 `::test_interceptor_tolerates_sync_handler` |
| 4 | 鉴权拦截器把健康检查也挡了 | 探针不可能带 `x-pandora-player-id`，`AuthRequired` 档下 Pod 永不 Ready，而现象仍只是"探测失败" | 同上 `::test_health_check_survives_auth_required_interceptor` |
| 5 | trace 头取错（`x-request-id` 而非 `x-pandora-trace-id`）+ 缺失不生成 + 无回程头 + 无安全闸 | Python 服务的日志与 Go 服务、与 UE 客户端**完全串不起来**。每条日志都在、格式都对、就是关联不上 | 同上 `::test_trace_metadata_key_matches_go` / `::test_trace_id_safety_gate` |
| 6 | `dbguard` 容量告警字段口径分叉 | Go 打 `db_capacity_budget_exceeded` + `kind=avg_row_bytes` + `note`/`hint`，Python 打 `db_budget_violation` + `metric=avg_row_length` 且**整个 `max_bytes` 维度和 `note` 都没迁**。Loki 上按事件名建的告警对 Python 服务**永远不触发** | 起两个进程比日志：字段与数值现已逐字节相同 |

停机顺序也一并对齐 Kratos：**先翻 `NOT_SERVING` 再 `grpc stop`**。这半拍是 §9.16
「先摘流量 → 再排空在途」的机制本体；反过来 = 排空期间 k8s 仍在往这台送新请求。

**B. owner 权威面（§9.22，三条都有 Go 侧注释写明原因）**

| # | 缺陷 | 后果 |
|---|---|---|
| 7a | 来源版本闸排在 no-op 早退分支**之后** | **重复投递整条跳过版本校验**。事故形状（INC-20260818-003）正是"旧 binary 握着一个**合法**的 expect_epoch"—— epoch 检查放不倒它，能判定"谁的来源更新"的只有本闸。等于把 Go 已经修好的 bug 又移植了回来 |
| 7b | 高水位不在 no-op 分支推进；`same_target` 写死 `False` | hub 侧把存量 legacy(0) 补成 R 时 target 一个字节不变 ⇒ 必落 no-op 分支 ⇒ 水位永久停在 0，「见过非零版本就永久拒 legacy」这条逐玩家防线**从不 arm** |
| 7c | `Release` 清了 `operation_id` / `admit_not_before_ms`；no-op 日志是 DEBUG | Go 的 UPDATE 刻意不动这两列；no-op 时玩家「卡在旧 DS」直到下次 Begin，而 RPC 返 OK、access log 记 DEBUG —— 打 DEBUG 等于这件事在生产不可见。已升 WARN 并补 `release_noop_reason` 四分类 |

另有两条**异常传播丢证据**（Go 是多返回值，Python 用异常就必须显式挂上）：
`BARRIER_NOT_OPEN` 丢了 `retry_after_ms`（调用方收到 0 ⇒ 只能空转或干等，§9.23
「不得无出口等待」当场打穿且无报错）、`EPOCH_CONFLICT` 的当前记录只写不读（拼一半）。
两者现已提为 `PandoraError` 的**声明式** slots 字段，而不是随手 `setattr` ——
后者能写进去（`Exception` 自带 `__dict__`）但拼错一个字母不报错，读的那侧永远拿默认值。

回归测试：`tests/test_owner_repo.py` 末尾 5 条。对照实验已做 —— 把 `repo.py` 退回修复前，
其中 3 条立刻红（另 2 条的修复早于那份备份，牙由探针场景单独证）。

### 5.2.3 基础件续批（2026-08-19 深挖）：2 修 1 待决，全部影响 21 个服务

§5.2.2 那批来自"起两个进程并排跑"。这一批来自**照着 Go 的 `pkg/` 逐个数调用点**——
问"Go 有 33 处在用的东西，Python 侧在哪"。共同形状仍然是零运行期信号。

| # | 缺陷 | 后果 | 状态 |
|---|---|---|---|
| 8 | **`safego` 整个没迁**（Go 侧 33 个文件 / 19 个服务在用），`server.run()` 用裸 `asyncio.create_task` 拉起 background 协程 | 协程抛异常后异常只躺在 Task 里没人取：**进程照跑、health 照答 SERVING、日志 0 行**，而那条循环（撮合 tick / 心跳清扫 / presence tick / 看门狗）已经死了。Go 里同一个缺陷会崩进程 —— 迁移把一个吵闹的故障变成了哑的 | 已修 `pandorapy/safego.py` |
| 9 | 信号安装那段写成 `for ... else:`，for 里没有 `break` ⇒ **`else` 恒执行** | 注释说的"Windows 退化到 `signal.signal`"实际是"每个平台都无条件覆盖掉刚装好的 asyncio 处理器"。SIGTERM 正是滚动更新的优雅停机入口（§9.16 先摘流量再排空），这条路径出问题表现为"发布期间偶发客户端错误"，不会指向 `server.py` | 已修 |
| 10 | **RPC 指标与 Go 在三个轴上全不相交**，且 `metrics.py` 里论证这个选择的注释**前提是错的** | 详见下表 | 已修（见 §6.4：改名 `pandora_rpc_*`，label 与分桶逐值对齐；业务 errcode 维度另开 `pandora_rpc_inband_total`） |

⑧ 的实测（起真 server + 一个立刻抛异常的 background 协程，1.3 秒内含一次强制 `gc.collect()`）：

```
服务仍在跑 = True    health = SERVING    panic_recovered 日志数 = 0
```

asyncio 的 `Task exception was never retrieved` 只在 Task 被**垃圾回收**时才打，
而 `run()` 把任务持有到进程结束、停机时又 `suppress(..., Exception)` 把异常取走 ——
那条警告因此永远不会出现。修复后口径逐字对齐 Go `pkg/safego`：
事件名 `panic_recovered`、字段 `name`/`panic`/`stack`、指标
`pandora_safego_panic_recovered_total{name}`；`CancelledError` 不算故障
（否则每次滚动更新一批假告警，与 §5.2.1 ⑰ 同坑）。
变异实验已做：把装配退回裸 `create_task`，核心用例当场变红（`tests/test_safego.py`）。

⑩ 的三轴分叉（**不是改个名字就完事**，label 名和 label 值也对不上，
硬改名反而会 join 出错误结果）：

| | Go（`pkg/middleware/metrics.go`） | Python（`pandorapy/metrics.py`） |
|---|---|---|
| 指标名 | `pandora_rpc_total` / `pandora_rpc_duration_seconds` | `grpc_server_started_total` / `_handled_total` / `_handling_seconds` |
| label 名 | `service` / `method` / `code` | `grpc_service` / `grpc_method` / `errcode` |
| service 值 | `DialogueService`（**去掉包名**） | `pandora.dialogue.v1.DialogueService`（全名） |
| code 值 | 粗粒度桶（`ok` + 错误码**段位**映射，刻意低基数） | 原始 errcode 数值 ⇒ 每 method × 165 个码，**高基数** |

`metrics.py` 的注释写着"命名沿用 grpc_server_*……同名指标能让同一块面板直接对比两个实现"——
**Go 从来不发 `grpc_server_*`**，所以那块"同一面板"并不存在，前提是错的。
实际依赖 `pandora_rpc_*` 的至少有：`docs/ops/player-journey-log-map.md`、
`docs/ops/service-killswitch.md`（按 `pandora_rpc_total{code}` 看关停/限流拒绝）、
`docs/reviews/压测前审核-20260724.md` 的压测断言，以及
`tools/scripts/stress_summarize.ps1`（逐行正则匹配 `pandora_rpc_duration_seconds`，
对 Python 实例**解析出零行**）。
高基数那一项还与 §12「player_id 绝不能做 label」的低基数原则、
以及 killswitch 文档里"当前 metrics label 是低基数粗分类"的明文口径冲突。

~~**没有直接改**的原因：改法牵涉 `code` 的分桶规则要与 Go 的段位映射逐段对齐，
属于"要么全对要么别动"的那种。~~

**已在 2026-08-19 收口批次做完**（§6.4）：指标名、label 名、分桶三轴均已对齐 Go
（`pandora_rpc_total` / `pandora_rpc_duration_seconds`，`code` 按 Go 的 `codeLabel` 分桶），
业务 errcode 维度另开 `pandora_rpc_inband_total`，并补了
`pandora_runtime_info{runtime="python"}` 供面板按 instance join 分栈。

### 5.2.4 ★ 最高优先级：已经落码、但是错的（12 处，2026-08-19 逐服务审计）

> ⚠️ **本表整节已过期**：12 处**全部**已修（2026-08-19 收口后复核逐条核到代码行，
> 含最后一条 `ds_allocator/orphan_reclaim.py` —— 由并发会话在 05:37 补上）。
> 表留在这里只作**形状教材**（"代码在、单测绿、而且被自洽的单测锁死"这一类长什么样），
> **不要照着它再修一遍**；也别据此认为这批是"错的参考实现"而拒绝 import、去手抄枚举 ——
> 那正是这批修复要消灭的形状。逐条修后位置见
> [`python-migration-remaining.md`](python-migration-remaining.md) §1（该节抬头的两组统计
> 自身也互相矛盾，一并按代码现状重算）。

**这一类比"没迁"危险一个量级**：代码在、单测绿、日志正常，而且多数已被**自洽的单测锁死**
——测试断言的正是那个错行为，所以永远不会红。逐条清单与取证在
[`python-migration-remaining.md`](python-migration-remaining.md) §1，这里只列形状与结论。

| 位置 | 错在哪 | 后果 |
|---|---|---|
| `services/auction/submit.py:33-43` | 订单状态与 Side **双错位**：`STATUS_FILLED=2` 而 proto 的 2 是 `PARTIALLY_FILLED`；`SIDE_SELL=0/SIDE_BUY=1` 而 proto 是 `SELL=1/BUY=2` | 部分成交被判终态并释放名额；**`SIDE_BUY=1` 恰好别名到 inventory 的 `EscrowSideSell=1`，买单冻的是道具不是金币，且绕开溢出守卫** |
| `services/data_service/data.py:184-198` | 缓存值写**裸 pb**、无格式头，却与 Go 用同一个 Redis key。Go 是 `'PDC'` + BE uint32 位图长 + 字段位图 | Go 读 Python 条目判 miss、Python 读 Go 条目抛 DecodeError 被静默吞 ⇒ **双向命中率塌成 0，零信号**；§9.16/17 的缓存投毒防护整个消失 |
| `services/player_locator/biz.py:34-38` | `LocationState` 编码整体错位（Py HUB=1/BATTLE=2/MATCHING=3，proto 是 OFFLINE1/LOGIN_PENDING2/HUB3/MATCHING4/BATTLE5） | 真 HUB 被当 MATCHING 校验；真 MATCHING/BATTLE 判越界拒写；key-miss 占位值在两侧语义**正相反** |
| `services/ds_allocator/orphan_reclaim.py` | 六处漂移，最重的一条：首见表键用 `gs.name`，Go 用 `name+"/"+uid` | 名字复用的**重建 GS 继承旧观察起点、第一轮即被删** —— 删的是活着的 DS（§9 已有两次同类事故） |
| `services/hub_allocator/capacity.py:71-84` | `record_matches_instance` 缺三条前置硬约束（`uid!=""`/`epoch!=0`/writer 恰等于 2）；多 successor 被 `set()` 静默去重 | 幽灵占座留在账本 → Hub 假性满员；Model B writer 代际门被整个拆掉 |
| `services/mission/engine.py:78,:84` | `COMPLETE_MISSION` 写成 1（proto/Go=8）；`saturating_add` cap 用 `2**31-1`（Go 是 MaxUint32） | 链式任务后环永远收不到"前置已完成" |
| `services/battle_result/roster.py:136-139` | `should_apply_rating` 在 rating_mode 未定格时返回 **False**，Go 两条 legacy 路径都返回 **true** | 混跑期同一场对局两侧算出不同段位；排位局白打 |
| `services/push/offline.py:179-184` | 坏 member 静默 `continue`，Go 是 Error 日志 + 哨兵折账 + 物理删除 + 折账失败时扣发 | 游标推过坏帧，既无记账也无 resync，**永久静默漏报**（Go 已修过两轮） |
| `services/friend/repo.py:181-185,:230-234` | ①重复申请不轮换 `request_id`；②`accept_request` 的锁序与 `create_request` **反序** | ①客户端按 `(request_id, reason)` 判重 → 新推送被当重投丢弃；②ABBA 死锁环，而 `_write_tx` 没有 1213/1205 重试 |
| `services/guild/group_repo.py:189,:226,:243-265` | ①member 的 role 写 0（Go `GroupRoleMember=2`）；②`remove_member` 与 `add_member` 锁序相反 —— **该文件自己的 docstring 就写着不能这么做** | role 显示错位 + `ORDER BY role` 把成员排到群主前；偶发 1213 且只打 debug |
| `services/chat/data.py:154,:160` | 两个限流 key 前缀与 Go 不同（Go 是 `pandora:chat:world:cd:*` 与 `pandora:rl:chat:<ch>:*`）。⚠️ 原审计还报了"四个默认值漂移"，**落码时逐条对 Go 核实后证伪**：Go 的真实默认与 Python 原值逐个相同 | 灰度期同一玩家两侧各占一个 key，冷却翻倍放宽；按 `pandora:rl:*` 建的巡检对 Python 副本失明 |
| `services/trade/service.py:66` | `getattr(exc, "order_state", ...)` —— `PandoraError.__slots__` 没有这个字段，恒取默认值 | 客户端拿到 `code=UNAVAILABLE + new_state=0`，判不出该重试还是该当订单没动 —— 而同文件 docstring 恰好写着这条铁律 |

**共同形状**：跨语言常量/枚举**手抄**时错位，且**测试抄了同一个错值**。
这正是 §7.1"凡能提纯成纯函数的，做跨语言对拍"要防的东西 —— 已经对拍过的
（kafka 哈希 / 榜单桶 / 经验曲线 / 来源版本）零差异，**没对拍的这批全错**。
处置建议：枚举与状态常量一律从 `python/gen/**_pb2` 直接引用，不手抄；
实在要抄的，补一条 `assert Py.X == proto_pb2.X` 的机械检查。

**主会话已独立抽查前两条**（auction 与 data_service），与描述完全一致。

**第 13 处，同类但尚未生效**（chat 还没有 `main.py`，所以现在炸不出来，但坑已经埋好）：
Go 侧 `GuildService` 与 `GroupService` **同进程**，`GuildReader` 与 `GroupReader` 共用
`cfg.chat.guild_addr`（`chat/cmd/chat/main.go:143,:146,:150`）；Python 的
`services/chat/conf.py:26` 自造了一个独立的 `group_addr` 字段，而两份真实 yaml
**只写了 `guild_addr`**。照 Python 的模型装配，GROUP 频道会恒降级 —— 弱依赖降级是合法档，
所以不会有任何报错。写 `chat/main.py` 之前先把这个字段删掉。

### 5.2.5 第 0 批落码结果 + 复核捞出来的遗留（2026-08-19）

§5.2.4 那 13 处**全部已修**：12 个修复 agent + 11 个独立复核 agent，
复核结论一律 `CORRECT` 且 `test_has_teeth=true`（每条都做过变异验证：把修复退回去，
对应测试当场红）。修复口径统一为**枚举直接引用 `python/gen/**_pb2` 生成物**，不留字面量。

**一条审计结论在落码时被证伪，值得单记**：原报告说 chat 有"四个默认值漂移
（world 3s→5s 等）"，逐条对 Go `chat/internal/conf/conf.go:99-113` 核实后发现
**方向是反的** —— Go 的真实默认与 Python 原值逐个相同，不存在漂移。
key 前缀那半成立并已修。这正是给每个修复 agent 定的第一条铁律
（"先对 Go 核实，描述可能是错的；核实后发现描述错就不要硬改"）救回来的一次 ——
照着改会把本来对的值改错，而且改完测试还是绿的。

**复核顺带捞出的遗留**（不是这次改出来的，逐条有 Go 出处；按危险度排）：

| # | 位置 | 差异 | 状态 |
|---|---|---|---|
| 1 | `guild/group_repo.py` `remove_member` | 取了 `owner_id` 却**从不比较** —— 群主可以直接退群。Go `group_repo.go:285-288` 拒（三审 P1-9 TOCTOU：退群/踢人与转让交错会删掉刚晋升的新群主，留下悬空 `owner_id`）。另：群已解散时 Go 幂等成功、Python 抛 NotFound | **本轮已修**，并改掉了一条固化错行为的旧测试（它让群主退自己的群） |
| 2 | `push/offline.py` `_parse_member` | 只校验分隔符位置，缺 Go 的"前 20 字节必须全是数字"（`offline.go:135-141`）。proto3 极宽松、**空 payload 也能解成功**，所以 `<20 字节垃圾>` 在两侧判定正相反 | **本轮已修** |
| 3 | `ds_allocator/orphan_reclaim.py` | 缺 Go 的"删除结果回填"：exact 删除被跳过（对象变过 = 可能已被重新分配）时 Go `delete(orphanGSFirstSeen, key)` 强制重新观察满一个窗口，Python 的键还留着 ⇒ 下一轮**立刻**再删一次 | **已修**：补 `on_reclaim_outcome`，三分支方向各不相同（skipped 作废候选重新观察 / failed 保留重试 / 未知值抛错） |
| 4 | `friend/repo.py` `accept_request` | 缺 Go 步骤 6「反向 pending 一并终结」（`friend_repo.go:406-414`，R5 P2-8）：A→B 与 B→A 可各自 pending，接受其一后另一条仍挂在对方收件箱，被接受时对已是好友的两人重复建边并再推一次 | **已修**，并加了「只动这一对、别扫第三方」的精确性用例 |
| 5 | `battle_result/roster.py` | `should_apply_rating` 只返 bool，Go 返 `(bool, basis)`，调用方对 legacy 回落打 `battle_rating_basis_legacy_fallback`。接线后旧口径兜底的局会静默结算 | **已修**：新增 `settlement_runs_elo() -> (bool, basis)`，5 个 basis 字符串与 Go 逐字一致；`rating_mode=None` 显式表示「无 canonical 快照」（Go 的 `terminalRelease == nil`），与「有快照但未定格」是两个不同 basis |
| 6 | `data_service/data.py` | 坏档 WARN（`player_cache_corrupt_entry` + `reason`）实机验证过会打、字段与 Go 一致，但**零测试断言** —— 删掉那两段日志 36 条测试依然全绿 | 未修 |
| 7 | `guild/group_repo.py` `create_group` | 写序与 Go 相反（Go 先按 player_id 升序 reserve 名额再 INSERT，Python 反过来） | **已对齐**。诚实说明：新 group_id 行没有并发争用者，今天**没有**能证实的死锁路径；对齐是因为「同一套表的所有写路径同一取锁顺序」本身是该文件的不变量，留一条反向路径下一个人会照着抄 |
| 8 | `mission/engine.py:89-92` | 注释把取 MaxUint32 的**理由**写错了（说"落库列是 INT UNSIGNED"，实际列是 `VARBINARY(256)` 存 pb）。值本身对；理由错会让下一个人按错依据反推 | **已修**：依据改成 proto 的 `repeated uint32 progress`，并写明为什么不是列类型 |

### 5.2.6 共性件续批（第 2/3 批复核捞出来的，全部已修）

第 2 批 4 个服务的复核结论是 **3 个 GATES_MISSING / 1 个 GOOD**。其中**两条缺口出现在
多个服务上** —— 那说明它们不是逐服务的疏漏，而是共享层缺件，复制模板时一并复制了：

| # | 缺口 | 后果 | 处置 |
|---|---|---|---|
| 11 | `snowflake_etcd` 的 **static 号段闸整个没有**（trade / mail / dialogue 都报） | Go `provider.go:96-104` 拒 `node_id == 0 \|\| > NodeMask`。**0 是 UE DS 本地发号器的保留号**，用它发号会与 DS 本地铸的 ID 逐位相同、撞进同一玩家的背包键空间。不能指望 `snowflake.Node` 自己的检查 —— 那条是 `0 <= node_id`，**放行 0**；而漏配时 pydantic 默认值恰好是 0，"忘了配"稳定落进最危险的一格 | 已补进共享件 + 4 条用例 |
| 12 | 四个 MySQL 池参数**配了不读**（owner 手写 `create_pool` → 另外三个照抄，复制了四份） | `conn_max_lifetime: 30m` 在 yaml 里写着、Go 读它 `SetConnMaxLifetime`，Python 不读。没有 `pool_recycle` 的长空闲连接撞上 MySQL `wait_timeout` 被服务端断掉，客户端不知道，**下一条业务 SQL 才暴露**；低峰期最容易触发 | 收进 `mysqlx.pool_kwargs()`，四处全改；`conn_max_idle_time`（asyncmy 无等价物）**配了就拒启**，不静默忽略 |
| 13 | service 层 `except BaseException` 吞掉 `CancelledError`（leaderboard 7 处 + owner 5 处） | grpc.aio **用取消**终止在途 handler。吞掉 = 停机时把取消映射成业务错误码返回**正常响应** ⇒ 客户端每次滚动更新收到一批假失败，且在途请求没有真的排空。owner 是模板，leaderboard 照抄 | 12 处全修 + **按目录扫的机械检查**（`tests/test_service_layer_contract.py`），新服务自动纳入 |
| 14 | `protosql` 的列类型与 Go 的 `proto2mysql` 分叉：`string` → `VARCHAR(255)` vs **`MEDIUMTEXT`**；`bytes` → `VARBINARY(4096)` vs **`MEDIUMBLOB`** | 两栈写**同一张表**，表由先启动的服务建。列窄的一侧让同一条写入（如 300 字昵称）**Go 成功 / Python 1406**，取决于请求落到哪个副本，**不可复现**且两边代码都"没错" | 已对齐 Go + 对拍测试（去 GOPATH 解析 proto2mysql 源码证明手抄值没过期） |

`pool_kwargs` 的 `autocommit` **刻意不给默认值**：owner/data_service/leaderboard 需要
`False`（写路径靠 rowcount 判 CAS），mail 需要 `True`（与 Go `database/sql` 默认一致）。
两个服务因为数据层写法不同而正确取值相反，给默认值等于替所有服务做了一个它们并不一致的决定。

**⑭ 顺带更正一条 §9.24 的适用边界**：「能用 `VARBINARY(N)` 就不用 `LONGBLOB`」这条偏好
在迁移期**让位于跨栈一致**。§9.24 真正要求的是**写入侧**三道闸（单元素 / 条目数 / 整体字节，
见 `dbguard.check_payload`），那三道仍然生效；列类型只是最后一道物理上限，
自己收窄不会让数据更安全，只会让一部分写入随机失败。

### 5.2.7 两条"以为是 bug、实测证伪"的（别再查）

| 断言 | 结论 | 证据 |
|---|---|---|
| owner 的 `repo.query()` 既不 commit 也不 rollback，而池是 `autocommit=False` ⇒ 连接归池后带着 REPEATABLE READ 旧快照被复用，读到陈旧 owner 记录且零报错 | **不成立** | 打真 MySQL 实测：`maxsize=1` 强制复用同一条连接，不提交就归还，另一条独立连接写入并提交后，**再借出能读到新行**（隔离级别确认 REPEATABLE-READ）。asyncmy 的池在归还/借出时重置事务。回归测试钉在 `tests/test_owner_repo.py` 末尾 —— asyncmy 哪天改掉这个语义，owner 就**真的**需要补 rollback |
| chat 有"四个默认值漂移"（world 3s→5s 等） | **方向是反的** | Go `chat/internal/conf/conf.go:99-113` 的真实默认与 Python 原值逐个相同。key 前缀那半成立并已修。给修复 agent 定的第一条铁律（"先对 Go 核实，描述可能是错的"）救回了这一次 —— 照着改会把本来对的值改错，而且改完测试还是绿的 |

> `mail/main.py` 里那段"autocommit=False 会读到陈旧数据"的注释**理由已被证伪**，但结论
> （用 `True`）是对的（与 Go 的 `database/sql` 默认语义一致）。注释已改，特意写明
> "别因为理由被推翻就把它改回 False"。

### 5.2.8 机械检查自己误报了两次 —— 每次都要先修检查再修代码

本轮加了三条按目录扫的机械检查，其中**两条第一版都误报**。记下来是因为误报的
代价比想象的大：一条会误报的检查，最终会被 `# noqa` 掉或整条删掉，
于是**连它本来能抓住的真缺陷也一起没了**。

| 检查 | 误报了什么 | 真判据 |
|---|---|---|
| `coverage.py --check`（proto 里不存在的方法名 = 该 RPC 永远返回 UNIMPLEMENTED） | 把 `set_ds_callback_guard()` 这类**依赖注入的辅助方法**也报了 —— 它们本来就不该被 grpcio 分发 | 只看 **PascalCase**。gRPC 生成的 servicer 方法名恒为 PascalCase，拼错的 RPC 名（`DoThinng`）照样是 PascalCase 会被抓，而 snake_case 辅助方法一个都不误伤 |
| `test_service_layer_contract`（`except BaseException` 吞 `CancelledError`） | 把事务回滚的标准写法 `except BaseException: rollback(); raise` 也报了 —— 它**无条件 re-raise**，取消照样穿透 | 除了"前面有没有放行 CancelledError"，还要看**这条 except 的块体顶层有没有裸 `raise`**。有就放行 |

第三条（`RedisConf` 字段与 Go 逐个对齐）没有误报，因为它比的是**两个明确集合**
而不是在猜代码意图 —— 这也是判据：**能比集合就别比模式**。

还有一条**刻意没做**的：R5「player_id 必须取自鉴权上下文」。试过，正则区分不了
"拿请求体当身份"（危险）与"日志字段 / 响应回填"（正常），在 owner 上误报；
而"有没有用 `extract_player_id`"同样区分不了 —— 客户端面服务用它**取**身份，
owner 用它**拒**带玩家 JWT 的调用。同一个符号，两种相反的用途。
理由写在 `tests/test_service_layer_contract.py` 里，免得下一个人再试一遍。

**扩面比新增更划算**：`test_service_layer_contract` 原先只扫 `services/*/service.py`，
扩到服务目录**全部 .py** 之后当场炸出 **87 处**未放行 `CancelledError` 的宽 except
（`main.py` 的启动路径、`repo.py` 的数据层都有）。同一条判据，覆盖面差 5 倍。

### 5.2.9 收尾批次（2026-08-21）：三条"没有任何现成闸能抓"的

三条都不是"写错了"，是**写了但没接上 / 读错了地方 / 注释成了假证据**。共同点：
测试全绿、服务照常 SERVING、日志零行。

| # | 缺陷 | 后果 | 为什么没闸抓得到 |
|---|---|---|---|
| ① | `matchmaker/main.py:_self_region` 仍从 `cfg.model_extra["cell_route"]` 读，而 `cell_route` 已升格为 `BaseConf` 的 pydantic **正式字段** | pydantic 不把已声明字段放进 `model_extra` ⇒ 恒返回 0 ⇒ leader 选举分片键恒为 `.../r0` ⇒ **所有 region 副本挤进同一次选举，非 leader region 的撮合永久停摆且零错误日志**（违反 §9.20/§9.21） | `model_extra.get()` 语法完全合法，取不到就是 `None` 走默认分支。`auction/conf.py:255` 早把这个形状记成"历史教训"，但没人 grep 第二个读者 |
| ② | 模块 docstring 与用例 docstring 仍写着"Python 侧只实现单 Cell，所以 static/etcd 都拒启" | `cellroute_etcd` 装配补齐后 static/etcd 已合法，注释变成**假的安全论据**——下一个人照它推理会得出错误结论 | 注释不参与执行 |
| ③ | `inventory/main.py:_run_bag_journal_sweep` 定义了但**从没 append 进 background**（Go 侧 `cmd/inventory/main.go:255` 有 `go runBagJournalSweep(...)`） | `bag_journal` 只增表永不清理，违反 §9.24 | ruff 的 F401/F841 只管 import 与局部变量，**模块级函数没人调用不是 lint 错**；单测不会去调私有 `_run_*`；起服务、health、日志三个观测面与"接上了但没到清理时间"完全同形 |

**新增的机械闸**：`test_service_layer_contract.py::test_background_runners_are_actually_wired`
——扫每个 `services/*/main.py`，`_run_*` / `*_loop` 顶层协程定义了就必须在别处被引用。
配套金丝雀 `test_the_runner_wiring_check_is_not_vacuous` 防它随命名约定变化而空转。

> ⚠️ 这条检查的**第一版又误报了一次**（第三次，见 §5.2.8）：判据是
> `re.findall(name, src) > 1`，而补接线时留的那句注释「此前 `_run_bag_journal_sweep`
> 定义了但从未挂进 background」**自己就含这个名字**，把接线拆掉后检查照样打绿。
> 改成数 AST 的 `ast.Name`(Load) 引用后立刻抓到。
> **注释会抵消文本判据；能拿 AST 就别数文本。**

**全仓对账**：把 Go 各 `cmd/*/main.go` 的 `go xxx(...)` 具名 goroutine 全部枚举
（13 个 `runCapacityGuard` + leaderboard 双 sweep + mail / dialogue / owner / inventory
各自的 sweep + inventory 的 `runLegacyBagMigration`），逐个确认 Python 有对应
`background` 条目。除 ③ 外无遗漏。**移植 `main.go` 的标准动作就是这份对账**。

**测试自身的缺口**：D5 迁移的 7 个变异全被抓，但把 `plog.get().error(...)` 整块删掉时
29 个用例**全绿** —— 日志事件名（`bag_legacy_migration_player_failed` /
`_done_with_failures` / `_done`，与 Go 逐字相同、告警按名建）没有任何断言守着，
可以被静默删掉。已补 `structlog.testing.capture_logs` 的事件名守护用例。
**判据：凡"改了不会让任何断言变红"的东西，就是没被测。**

### 5.3 环境与工具坑

| 坑 | 现象 | 处置 |
|---|---|---|
| **Windows cp1252 stdout** | 中文 `print` 抛 `UnicodeEncodeError`，**把真实错误盖掉** | `pandorapy/_utf8.py`，每个入口 import |
| **fakeredis 的 Lua 桥不够用** | `tonumber()` 结果是 float，`ZADD` 报参数类型错 | 换真 Redis，**不为迁就 fake 改已验证的 Lua** |
| **fakeredis 默认共享 server** | 用例间数据泄漏；单独跑过、全量跑挂 | 每用例 `FakeServer()` |
| **`fakeredis` 缺 Lua** | `EVALSHA` 报 unknown command | 装 `fakeredis[lua]` |
| **protobuf 7.x 移除 `FieldDescriptor.label`** | 照老资料写直接 `AttributeError` | 用 `is_repeated` |
| **MSYS 路径转换** | `docker run ... /usr/local/bin/etcd` 被转成 Windows 路径 | `MSYS_NO_PATHCONV=1` |
| **固定 `sleep` 的并发测试** | 全量跑时偶发红（§16.10 掩盖时序） | 改「等条件 + deadline」轮询 |

### 5.4 复跑校验命令

**跨实现对拍**（推进剩下 19 个服务的主循环，不是可选步骤）：
探针与完整说明在 `python/tools/parity/`。现有 7 份：`probe_owner.py`、`probe_dialogue.py`、
`probe_login.py`、`probe_mission.py`、`probe_auction.py`，以及 2026-08-21 新增的
`probe_hub.py`（hub_allocator，36 场景）与 `probe_ds.py`（ds_allocator，43 场景）。
那份 README 里有起服务的准确命令（**工作目录必须是服务目录**，配表与 DSN 的相对路径
都相对进程 cwd 解析）、Go 版起到错开端口的做法，以及四条写探针的规矩。
2026-08-19 抓到的 7 条缺陷全部来自这一步，没有一条是单元测试能发现的。

两个 allocator 探针都**必须把两侧 `mode` 改成 `"mock"`**：`dev` 默认 `local`，
会真去 exec Windows DS 进程，两个实现各起一份、端口互抢，diff 里全是与实现无关的噪声。
它们各自还踩到一条新坑，已写进 README：hub 的分片镜像按 **pod 名**建行（不按 player_id
分区，只分段挡不住跨运行污染，解法是打相对基线的**增量**而不是把人数盖掉）；
ds 的分配是**一次性资源占用**（每条场景必须用自己的 `match_id`，复用则全部落进
`allocate_idempotent_hit` 快路径，diff 零但什么都没验）。

**单元测试**：


```bash
cd python && PYTHONUTF8=1 .venv/Scripts/python.exe -m pytest tests/ -q
```

单独验九条修复是否还在：

```bash
cd python && PYTHONUTF8=1 .venv/Scripts/python.exe -m pytest tests/test_trade_biz.py::test_order_limit_enforced_by_lua tests/test_friend_repo.py::test_many_distinct_pairs_concurrently_no_deadlock tests/test_friend_repo.py::test_only_target_can_accept_or_reject tests/test_auction_submit.py::test_invalid_idempotency_keys tests/test_inventory_settle.py::test_safe_mul_detects_overflow tests/test_auth_jwt.py::test_short_secret_rejected tests/test_push_offline.py::test_lost_since_returns_zero_when_nothing_lost tests/test_writerlease.py::test_blocked_activation_does_not_starve_the_cluster -q
```

单独验 2026-08-19 那批修复（§5.2.1）是否还在：

```bash
cd python && PYTHONUTF8=1 .venv/Scripts/python.exe -m pytest tests/test_writerlease.py tests/test_snowflake_etcd.py tests/test_owner_repo.py tests/test_source_revision.py -q
```

错误码与 Go 的一致性（CI 门）：

```bash
cd python && PYTHONUTF8=1 .venv/Scripts/python.exe tools/gen_errcode.py --check
```

RPC 覆盖矩阵与静默缺陷门（同为 CI 门，经 `tests/test_parity_coverage.py` 入 pytest）：

```bash
cd python && PYTHONUTF8=1 .venv/Scripts/python.exe tools/parity/coverage.py --check
```

**判据是「全绿 且 0 skipped」。** 缺 etcd / MySQL / Redis 时会静默跳过一大批用例
（2026-08-19 在 592 个用例的规模上实测为 83 条 —— 这个绝对数会随用例增长而变，
别把它当当前值），覆盖的正是脑裂、重号、投递缓冲这些最危险的部分。
起依赖的命令见 §4.2；`python/README.md` 的「跑测试」一节也有一份。

CI 侧对应的是 `tools/scripts/ci_backend.ps1` 的「Python 侧门禁」段，两道：
`gen_errcode.py --check` 与 `pytest -rs`；数据库组的跳过在 `-RequireDbTests`
下判失败（口径与 Go 侧 `go_test_skip_audit` 一致）。CI 机需要 `uv`
（已进 `tools/devops/bootstrap-machine.ps1` 前置工具表），环境由它现建、依赖每轮同步
—— 只在首次建环境时装依赖的话，`pyproject.toml` 改了 CI 机上那个旧 `.venv` 永远不会更新。

## 6. 剩余工作

### 6.1 规模

| | Go 行数 | 状态 |
|---|---:|---|
| `pkg/` 基础件 | 25,397 | 核心已迁；**未迁部分的优先级见 §6.1.1**（按 Go 侧调用点数排，不是按行数） |
| 21 个服务 | 104,948 | **各有核心，业务外围未逐行移植**；`dialogue` / `owner` 已完整可跑并与 Go 零差异 |
| Go 侧测试 | 109,411 | 未移植（Python 侧另写了 872 条针对性测试） |

### 6.1.0 三个维度的规模（2026-08-19 逐服务清点）

RPC 方法只是**一个**维度，而且是最容易看见的那个。一轮 14 个 agent 的逐服务清点 +
21 个对抗复核 agent 的逐条证伪（229 条待核 → 126 条 CONFIRMED、8 条被推翻）给出另外三个：

| 维度 | Go 侧总数 | Python 已迁 | 说明 |
|---|---:|---:|---|
| **启动闸**（fail-fast / warn） | **318** | **10** | 只分布在两个样板上（owner 8 + dialogue 2），其余 19 个服务是 **0**。login 一个服务就有 **29** 道，hub_allocator 29、player 27、ds_allocator 25、team 22 |
| **后台循环 / 定时任务** | **92** | 4 | sweep / 巡检 / 心跳 / 撮合 tick / 补号。RPC 全实现也看不出少了它们 |
| **原子性载体**（Lua / SQL 事务 / CAS / WATCH） | **172** | 25 | 迁错不报错，只在并发下表现为账不平 |

**"启动闸"这一栏是本轮最反直觉的数**。§6.2 早就写了"启动闸是各服务差异最大的地方"，
但 318 : 10 这个比例说明它不是"写 `main.py` 时顺手搬"的量级 —— 对 login 来说，
`main.py` 的主体就是那 29 道闸（配置矩阵交叉校验、JWKS 重叠期、5 张表的列**形状**校验、
TiDB 版本与排序规则**行为探针**、fence 租约抢占……），业务装配反而是少数。

229 条待核里的类目分布也印证了 §3 的选择标准是对的：
`fail-open/fail-closed 方向` 42 条、`原子性` 28、`幂等键` 27、`时序常量` 23、
`跨语言逐位一致` 18、`代际 fencing` 16 —— 与"写错了不报错"高度重合。

### 6.1.1 未迁 `pkg/` 的真实优先级（按 Go 侧调用点数，不是按行数）

行数会误导：`namecheck` 有 1154 行但**零个服务在用**。按"多少个服务真的 import 它"排：

| pkg | Go 行 | 用它的文件 / 服务数 | Python 现状 | 不迁的后果 |
|---|---:|---|---|---|
| `safego` | 120 | 33 / 19 | **本轮已补** | 见 §5.2.3 ⑧ |
| `sessiongate` | 95 | 28 / **14** | **零** | 13 个服务在 `internal/server/grpc.go` 里挂 `pmw.SessionCurrent`。缺了 = 顶号后的旧 JWT 在 exp 前（默认 24h）**仍保有全部按 player_id 定向的能力**（好友申请 / 交易 / 背包），正是 INC-20260722-004 的形状，且**完全静默**。⚠️ **第 14 处是 push，形态不同**：`Subscribe` 是 server stream，Kratos 的 unary 中间件链**对它一律不生效**，Go 是在 service 层手写补齐的（`service/push.go:66-104` + `biz/push.go:145-211`，含 30s 看门狗与 `sessionFailClose=3`）。迁 push 的人去找中间件会找不到 |
| `cellroute` | 767 | 16 / 13 | **已补齐装配层**(2026-08-20) | 见 §6.4「仍未做」表的更新说明 |
| `internalrpcauth` | 442 | 13 / 4 | **零** | 东西向 RPC 的 HMAC 签名（绑 caller+method+subject+ts+nonce）。签名对不上是响亮的；**漏掉 nonce 消费**则重放保护静默消失 |
| `kafkax` 的 producer/consumer/topics | 898 | — / 12 | **只迁了一致性哈希** | 见 §6.1.2 |
| `releasetrack` | 48 | 11 / 2 | **零** | sha256 cohort 选择必须跨语言逐位一致，否则同一玩家在 Go 副本判 canary、Python 副本判 stable。只被两个 allocator 用，随 allocator 的档期 |
| `offlinewatch` | 1002 | 3 / 2 | **零** | 零值刻意是"不确定"而非"离线"（Go 注释明写）。Python 没有零值语义，这个 fail-closed 方向要显式重建 |
| `dsmetadata` / `battleabort` / `dsauthrecord` | 225 | 12 / 3 | **零** | 全在 allocator / battle 链上，随 allocator 档期 |
| `rewardclaim` | 429 | 1 / 1 | **零** | 变长位图的**规范落地形态就是原始 `[]byte`**：位序错一位 = 已领的档位读成未领。只有 player 在用 |
| `passwd` | 52 | 1 / 1 | **零** | 只有 login 用。bcrypt 自描述，跨语言校验能通；dev cost=4 / prod 10+ 要跟着配 |
| ~~`namecheck`~~ | 1154 | **0 / 0** | 零 | 只被 `tools/lexicon-import/main.go` 用，**任何服务都不 import**。建议从迁移清单里划掉 |
| ~~`grpcstats`~~ | 345 | **0 / 0** | 零 | 全仓零调用（Go 侧自己也没在用）。建议划掉 |

### 6.1.2 Kafka 事件面：整层未迁（12 个服务）

Go `pkg/kafkax` 是 1038 行四件套；Python `kafkax.py` 140 行**只有一致性哈希分区器**。
没迁的是 `producer.go` / `consumer.go` / `topics.go`：

- **18 个 topic 名常量**，12 个服务在用（player / battle_result / ds_allocator /
  hub_allocator / matchmaker / team / player_locator / push / chat / friend / guild / mission）。
  名字漂移一个字符 = producer 发到一个没有 consumer 的 topic，**两侧都不报错**。
- **`KeyOrderedProducer`**：kafka key 恒为 `strconv.FormatUint(player_id, 10)`，
  这是"同玩家事实保序"的**唯一**载体。key 算错 = 跨分区 = 乱序 ——
  而 mission 那条链上"后环事实提前到达 = 静默永久丢失"是已确认的事故形状。
- **`PushToPlayers` 排除 `caller_player_id`**（推送原则 2），
  `pandora.match.progress` 是**明文例外**（stage 异步变化必须发给所有人含发起方）。
  这类"方向性例外"漏掉不会报错，只会表现成"偶尔多收/少收一条推送"。
- **`HeaderEventType` + 单事件类型 topic 不变量（§21）**：`topics.go` 里写明
  `pandora.player.update` 永远只承载 `PlayerUpdateEvent`，因为旧副本不看 event_type
  直接解码，字段 2/3 恰好能对上 `match_id`/`mmr_delta` —— 混跑窗口里会**静默污染 MMR**。
- **`KeyOrderedConsumer`** 的 poison / DLQ / 重试策略：迁丢了就是"坏消息卡住整个分区"
  或"坏消息被静默丢弃"，取决于漏的是哪半边。

> 这是个**做一次、12 个服务受益**的共性缺口，性价比高于任何单个服务的 `service.py`。
> 注意 Go 用的是 **sarama**（不是 kafka-go），语义细节以 sarama 为准。

### 6.1.3 Redis 装配：Python 表达不了 Go 的形状（实测）

> ⚠️ 先更正一处：`pkg/svc.MustNewBaseContext` 的文档注释自称"所有服务共享的通用
> ServiceContext 模板"，但实测 **零个服务调用它**（`grep -rln MustNewBaseContext services`
> 无命中）—— 它是死代码，别照它迁。真实形状是**每个服务在自己的 `main.go` 里**
> `redisx.NewUniversalClient(rc)` + 一道 Ping 闸，**21 个服务里 18 个有
> `redis_ping_failed`**（login `main.go:588` / ds_allocator `:202` / hub_allocator `:115` /
> auction `:184` / trade `:98` / matchmaker `:135` / team `:112` / leaderboard `:131` …）。
> 结论方向不变，但"下沉成一个共享件"是**我们要新做的事**，不是"照搬 Go 已有的件"。

Go 侧 `redisx` 至少有三个构造入口，Python 一个都表达不了：
`NewUniversalClient`（按 `addrs`/`master_name` 自动选 standalone / Sentinel / Cluster）、
`NewDeadlineUniversalClient`（auction 用）、`NewUniversalClientWithCredentials`（ds_allocator 用）。

实测出来的具体分叉：

```
pandorapy.config 有 RedisConf 模型吗   = False
redisx.new_client 的参数              = ['addr', 'db', 'password', 'dial_timeout_sec']
redisx 全模块提到 addrs/master_name 吗 = False
host='' 时实际连到                     = 127.0.0.1:6379     ← Go 侧此处 panic
```

即：**Sentinel / Cluster 在 Python 侧没有任何代码路径**；而 Sentinel 部署常见的
"只填 `addrs`、`host` 留空"喂给 Python 会**静默连本机 6379**。
当前还炸不出来，只因为唯二能起进程的 owner（MySQL）与 dialogue（内存态）都不碰 Redis
—— 这是给接下来 19 个服务埋的坑，**写第三个 `main.py` 之前先补**：
一个带 Ping fail-fast 的 `redisx.new_universal_client(conf)`，外加一个真正的 `RedisConf` 模型。

### 6.2 推进建议

**已完成（2026-08-19）**：`dialogue` 与 `owner` 两个样板都已完整可跑 ——
`main.py` 装配、conf 加载（同一份 `etc/*.yaml` 喂两个实现）、grpcio + FastAPI 双端口、
四个兼容点全验（`grpc-status` 映射 / metadata 透传 / `grpc-timeout` / 日志字段口径）。
`owner` 还带真 MySQL，覆盖了 dialogue 覆盖不到的那半：五道 fail-fast 启动闸
（DSN / 严格模式 / 建表 / TiDB 后端 / expand 列）、保留期 sweep、容量巡检。

**剩下 19 个服务现在是复制模板**，每个需要：`conf.py`（默认值必须与 Go 逐个相同）、
`service.py`（proto ↔ 内部结构 + in-band code）、`main.py`（把该服务的启动闸按 Go 的
`main.go` 逐条搬过来 —— 那些闸是各服务差异最大的地方，不能照抄 owner 的）。
注意 RPC 面要按 §2.3 的**真实 servicer 清单**算，别按同名 proto 一个文件算。

**但在写第三个 `main.py` 之前，有一件事排在前面。**

> ~~第 0 件：清掉 §5.2.4 那 12 处"已落码但是错的"。~~
> **已完成**（2026-08-19，12/12，见 §5.2.4 的告示）。这一条留个划掉的痕迹而不是删掉，
> 是因为它当时排在第 0 位的**理由**仍然成立：那批错误"已经在仓里、有测试锁死、看起来是绿的"，
> 后面每个服务都会照着抄。下次再出现同形状的东西时，它照样该排第 0。

**第 1 件：共性件（§6.1.1–6.1.3）。** 理由是性价比与失败模式，不是洁癖 ——
这些闸不补，下游服务的对应能力只能实现成"恒放行"，而它与 Go 的**合法降级档在行为上
无法区分**（这正是 killswitch 那条被误判的原因）：

1. **`BaseContext` 同构件**（§6.1.3）—— 否则每个 `main.py` 都要自己记得 Redis 选型 /
   Ping fail-fast / locker / killswitch 四样，而漏掉任何一样都不报错。
2. **`sessiongate` 接进 `build_grpc_server`**（§6.1.1）—— 13 个服务需要它。
   做成**结构上不可能忘**（比如客户端面服务必须显式传 gate，传 `None` 要显式声明
   "本服务不面向客户端"），而不是每个 `main.py` 自己记得挂。这与 `_register_health`
   "随 server 构造一起注册"是同一个理由。
3. **Kafka 事件面**（§6.1.2）—— 一次做完，12 个服务受益；不做的话每个碰 Kafka 的服务
   都会各写一份 topic 字符串和 key 规则，而这两样错了都不报错。
4. ~~**RPC 指标三轴对齐**（§5.2.3 ⑩）~~ —— **已在 2026-08-19 收口批次做完**（§6.4）：
   指标名、label 名、分桶三轴均已对齐 Go，并补了 `pandora_runtime_info{runtime="python"}`
   让面板能按 instance join 把两栈分开。

这几件的共同点：都是**做一次、全部 19 个服务受益**，而且都属于"每个服务各写一遍
必然有人写错、且写错不报错"的那类。先铺完再复制模板，比复制 19 遍再回头统一便宜得多。

**之后的服务顺序**见 [`python-migration-remaining.md`](python-migration-remaining.md) §5，
按依赖拓扑排了 12 档。要点：`data_service` / `leaderboard` 无跨服务前置，适合当第三、四个样板；
`inventory` 是**最大解锁点**（7 个服务的下游，且它没有任何 Redis Lua，风险全在锁序与 1062 语义）；
`trade` 是 21 个里**最接近可跑**的（4/4 RPC + conf.py 都在，补 `main.py` 与真实账本即闭环）；
`login` 最后（29 道闸里 25 道 fail-fast，其中 6 道是 schema / 后端语义**行为探针**）。

**验证方法已成型且必须照做**：把 Go 版起在错开的端口、跑同一份探针、逐字节 diff。
本轮 7 条缺陷里**没有一条**是单元测试能发现的。两个要点：
① 探针必须先证明"真的走到了目标分支"（第一版 owner 探针三条 ★ 全落在幂等快路径上，
diff 仍是零 —— 验了个寂寞）；② 跨运行共享的资源要分段（owner 的 `ds_instance_lease`
按 `instance_uid` 建行、不按 player_id 分区，不分段就会把上一轮的残留读成实现分叉）。

**两个 allocator 的特殊建议**：地基（writerlease + 来源版本 + fence 时间线）已实测自洽，
但业务本体（31,582 行）**建议永不迁或最后迁**。理由不是工作量，是它们同时满足
延迟敏感 + 正确性敏感 + 事故记录最多三条。真要迁，先在真 etcd 上跑
writerlease 的选举/激活超时/无主告警三条故障注入，再落码。

### 6.3 未决问题

- ~~**`main.py` 装配全部未做**~~ —— 已有 2 个（dialogue / owner）；剩 19 个。
  `trade` 是最近的一个：4/4 RPC 与 `conf.py` 都齐了，**只差 `main.py`**（见 §2.3 矩阵）
- **`trade` 的 `GrpcResourceLedger`** —— 等 `inventory` 迁完才有对端
- **`chat` 的 gRPC 成员解析适配器** —— 等 `team` / `guild` / `group` 迁完
- **`killswitch` 的 watch 热更** —— 当前只在启动时读一次（§15.3 不预先复杂化）
- **`protosql` 的增量 schema 同步** —— 用户已确认「库可清空」，故只建不同步

### 6.4 对抗审计的缺口 —— 处置结果（2026-08-19 收口）

61 条确认发现已全部处置。下面按"修了什么 / 还剩什么"分开写，**剩下的三条是真剩下的**。

#### 已闭环（每条都有会红的回归测试 + 变异验证）

**① 两个 allocator 的前置阻断（原本写着"动 allocator 之前必须先修"）**

| 缺口 | 修法 |
|---|---|
| 本地安全截止线从「此刻」起算，而不是从服务端证据起算 | 宣告持有前先向 etcd 要一次 `RemainingTTL`，并以**请求发出前**的单调时刻为锚点（响应回来前进程可能被长暂停，用"现在"会把陈旧 TTL 凭空平移到未来）。续约同理：只有服务端应答里的新 TTL 才推进截止线 |
| 越线后同一任期会被迟到的续约「续活」 | 引入 `_HoldState.self_fenced` 单调终态（对应 Go 的 `holdState.selfFenced`）：任一处观察到越线，同一 token 永久出局 |
| 排队副本的 key 消失后永久挂死 | `_candidate_status` 把「我的 key 还在吗」与「我是不是队首」**拆成两件事**——合成一个布尔时两者长得一样而处置相反。发现 key 不在即结束本轮、由主循环退避后重新入队，并打 `leader_requeue`（原路径**零日志**） |

**② 进程外壳层（Go 的 `pkg/grpcserver` 默认 middleware 链）**

| 缺口 | 修法 |
|---|---|
| access log 整层缺失 | 补 `rpc_ok` / `rpc_slow` / `rpc_failed` / `rpc_inband_error` 四事件，字段与阈值环境变量（`LOG_SLOW_RPC_MS`）与 Go 同名。服务端故障码集合**逐个列举**对齐 `IsServerFault`——按数值区间猜会把一批正常业务拒绝码误升 ERROR |
| Kill-Switch 没接进拦截器链 | 新增 `KillSwitchInterceptor`，且**挡在业务 handler 之前**（跑完再丢弃结果等于没关）。健康检查豁免——挡住它等于把整个 Pod 从 Endpoints 摘掉 |
| Kill-Switch 只做精确匹配 | 补 `*` / `<service>/*` / `feature/<名>` 三级，与 Go 的判定顺序逐级一致；feature 组按**代码注册的成员**展开，重复注册合并而非覆盖 |
| `grpc.timeout` 解析了不生效 | 新增 `TimeoutInterceptor`，与客户端 deadline 取更短者，超时回 `DEADLINE_EXCEEDED` |
| `GrpcConf` 静默丢弃两个字段 | `max_conn_age_grace` 显式建模并映射到 grpc option；`enable_rate_limit` 曾建模后**启动即 fail-fast**（当时 Python 侧没有 BBR，配着 true 却没有过载保护比"没这功能"糟糕得多）。**2026-08-21 起该 fail-fast 已撤销**：`pandorapy/bbr.py` 补齐自适应限流，`build_grpc_server` 按开关插 `RateLimitInterceptor`，详见下方"④ BBR 自适应限流"|
| 指标名与 Go 不相交 | 改成 `pandora_rpc_total` / `pandora_rpc_duration_seconds`，label 与分桶逐值对齐（桶不一致 = 两栈 P99 不可比）。业务 errcode 维度另开 `pandora_rpc_inband_total`（Go 只把它打进日志） |

**④ BBR 自适应限流（2026-08-21）**

`enable_rate_limit` 不是可选项：`tools/scripts/gen_cluster_config.ps1 -Prod` 对
**14 个服务**（12 个 unary session-gate + `login` + `push`）机械强制写 `true`，
带 FATAL 校验和契约测试 `gen_cluster_prod_ratelimit_contract_test.ps1`。
所以在原先的 fail-fast 语义下，这 14 个服务在 Python 栈上**生产配置直接起不来** ——
这是硬切换阻塞项，不是"锦上添花"。

**为什么手抄而不是用库**（CLAUDE.md §15.1 标准能力优先，先查证过）：

- GitHub 仓库搜索 `python adaptive concurrency limit load shedding` 与
  `BBR rate limit python` 均返回 **0 个仓库**。
- `go-kratos/aegis`（BBR 原版，239★）Go 98.9%，最后一次 release 在三年前；
  `Netflix/concurrency-limits`（3.6k★）是 **Java 100%**。
- Python 侧星最多的几个 —— slowapi(2.0k★，本身只是 wrapper，"实际限流工作由
  limits 完成")、aiolimiter(775★，漏桶)、limits(642★，固定/滑动窗)、
  PyrateLimiter(515★，漏桶) —— **全部是"按 key 配阈值的配额执行器"**，
  与"按机器实时负载自适应丢弃"是两类东西，替代不了。

**唯一一处刻意与 Go 不同**：信号源。Go 版读 cgroup CPU 使用率；Python 版换成
**事件循环线程的饱和度**（`time.thread_time()` 增量 / 墙钟增量）。原因是
asyncio 单线程跑满时，4 核容器的 cgroup CPU 只有 ≈250‰，永远碰不到 800 阈值 ——
照抄的话这个限流器**在最需要它的时候恒不触发**。这一条写在 `bbr.py` 模块 docstring 里。

第二处刻意分叉：Go 的 `minRT` 在空窗口时算 `int64(math.Ceil(math.MaxFloat64))`
（未定义行为）；Python 显式返回 1 并注释说明，不复制 UB。

**③ 接线缺口**

- `snowflake_etcd` 接进 dialogue main（`node_id_source` 二选一，失租 `os._exit(1)`），并从**零测试**补到 8 条真 etcd 用例。
- `trade` 的 Noop 账本闸从"Go 的 main.go 里"下沉到 `TradeUsecase.__init__` —— Python 侧 trade 没有 main，那句 docstring 曾是一句不成立的承诺。现在忘记接账本在结构上不可能通过。
- 移植 `redisx` 的限流原语（`Quota` / `ActionQuota` / `Cooldown` / `ArmPenalty`），trade 的 `rate_quota_per_min` 从此有可注入实现。
- `cell_route.mode` 非空时按 Go 的同一判据校验(2026-08-20 起已补齐装配层,`static` / `etcd` 都能真正建 Router;只有非法 / 未知 mode 才拒启)。
- `auth` 补 TTL ≥ 1s 启动闸 + `AdditionalSecrets`（不停服密钥轮换的载体），并把"过期 / 非法"拆成两个错误码——合成一个之后客户端只能一律重试，密钥配错那天会变成全量重试风暴。
- `proto_gen.ps1` 加生成 Python stub —— 此前改完 proto 只重生成 Go，Python 侧还在用旧 stub（加字段读不到、改字段号**串字段**，而 CI 跑旧 stub 照样全绿）。

**④ 测试硬度**

- `test_source_revision` 的"跨语言对拍"原先跑的是**测试文件里手抄的 Go 重实现**，改成 `import` 真正的 `pkg/placement`（cwd=pkg，与 `test_kafkax_parity` 同模板）。`test_inventory_settle` 的幂等键格式改为从 Go 源码正则取出。
- `battle_result` 的 8 个 reason + 5 条文案补上"与 Go 集合相等"的断言（原先只有注释声称一致）。
- kafkax 的"内置 `hash()` 哨兵"原先在**同一进程内**比两个实例——而同进程 `hash()` 本来就一致，抓不到。改成跨进程换 `PYTHONHASHSEED` 比对。
- `test_alloy_extracts_level_as_label` 原先是 `"stage.labels" in alloy and "level" in alloy`，接近恒真；改成解析 `stage.labels` 块、断言 `level` 在其 label 集合里。
- 依赖锁 `python/requirements.lock` 入库，CI 改用 `uv pip sync` —— 此前 CI 每轮按 `>=` 下界重装，上游发个新版就能在仓库零改动的情况下把流水线打红。`protobuf` 下界抬到 `5.29.3`（生成物自带的运行期断言值，低于它 import 即抛）。

#### 仍未做（如实列出）

| 项 | 现状与影响 |
|---|---|
| ~~**cellroute 装配本体**~~ | **已完成(2026-08-20)**:`cellroute.build_router`(off/static/etcd 三分支)、`FullLocation` / `in_cell_shard` / `cell_tag` keyspace 分片、`AtomicTable` + `encode_entry` / `decode_entries` 表热更编解码、`cellroute_etcd`(全量 Get 铺初始表 + watch 整表替换)。`config.BaseConf` 正式建模 `cell_route` 字段,校验统一走 `RouterConfig.validate_mode`;push 的 cell 归属毒丸闸已按 Go 同位接线。测试 `tests/test_cellroute.py`(34 条)+ `tests/test_push_service.py` 的 6 条归属用例,毒丸闸已做变异验证 |
| ~~**cellroute 在 friend / player / data_service 的接线**~~ | **已完成(2026-08-20)**:三处 `main.py` 都在建完 usecase 之后调 `cellroute_etcd.build_router` 并注入(对应 Go 的 `etcdtable.WireRouter`),watcher 在 `finally` 关。补齐了 Go 的两处观测:`pandorapy/services/friend/sharding.py`(幂等键口径 `accept_idempotency_key` / `edge_build_key` + 落点判定 + `friend_edge_sharding` 日志)与 player 的 `_log_profile_placement`(`profile_placement`,接在 `update_mmr` 成功之后)。测试 `tests/test_friend_sharding.py`(20 条,`edge_build_key` 已做变异验证) |
| **Grafana 面板不入库** | 仓库里只有告警规则与数据源，**没有 dashboards 目录**。指标侧的机制已补齐（`pandora_runtime_info{runtime="python"}`，面板按 instance join 即可分栈），但面板 JSON 本身没有写——照着别人现有的看板猜面板属于臆造，需要人拍板做哪几块 |
| **`snowflake_etcd` 只接了 dialogue** | 不是遗漏:其余 19 个服务**还没有 main.py**，无处可接。谁写下一个 main，照 dialogue 那段抄即可 |

### 6.5 收口批次的**自我复核**又抓到 8 条（2026-08-19）

收口做完之后又跑了一轮五维对抗复核，专门找"这一批新写的代码有没有引入新洞"。
结果值得单独记:**新写的代码比它修掉的老代码更容易出问题**，其中两条 P1 都是
"720 个测试全绿"状态下活着的。

| 严重度 | 缺陷 | 为什么测试抓不到 |
|---|---|---|
| P1 | `grpc.aio.AbortError` **没有** `code()` / `details()`（实测属性只有 `add_note` / `args` / `with_traceback`），从异常上 getattr 恒得到 None → `code_label(None)` = `"ok"` | 每一次 401/403 都被记成**成功**、`err` 是空串。而 `code_label` 本身没错——错的是喂给它的东西，所以"只把它当纯函数测"永远抓不到。判据必须取**真实 abort 过的 RPC** |
| P1 | access log 四事件**全都没有 trace_id** | 绑定是 contextvars，作用域只在绑定它的那层。原先绑在最内层的 Auth 拦截器，它的 `finally` 一 reset，**外层 access log 才开始打**。已拆出独立的 `TraceInterceptor` 放到最外层——这正是 Go 把 `Trace()` 排在 `Logging()` 之前的原因 |
| P2 | `cell_route` 闸按"段是否存在"判定，而 Go 的关闭态是 **`mode` 为空** | 一个防止静默出错的闸，自己变成了让服务起不来的原因——方向反了。已改为只在 `mode` 非空时拒启 |
| P2 | 配了 `max_conn_age` 却没配 grace 时没有兜底 | Go 强制兜底 30s，而 grpc core 的默认是**无限宽限**——不兜底的话"达龄"永远不会真正断开老连接，滚动更新时流量滚不到新副本，现象与"根本没开这个功能"一模一样 |
| P2 | 激活期一次瞬时续约失败就**作废整届** | 恰好推翻了本模块 ★ 注释的立论:为了不让慢激活白做才让续约从当选就跑，结果又因一次抖动把它整个扔掉，churn 一点没少。已改为与持有期同一条判据——**只有越过安全线才放弃** |
| P2 | `etcdleader` 持有期的**防御性**复查用会抛的 `get_prefix` | 一次读失败就终结任期、取消撮合循环、revoke 让位——而租约其实好好的。权威是 lease（续约循环在管），复查只是防御，不该据此让位 |
| P3 | access log 的 `op` 比 Go 少一个前导斜杠，且缺 `transport` 字段 | 按 op 精确匹配的 LogQL / 面板在 Python 副本上**全部落空**，而"查不到"最容易被读成"没发生过" |
| P3 | `redisx` 惩罚窗两函数吞异常（Go 是把 error 交回调用方） | 写侧不像读侧有 fail-open 兜底——写失败就是真的漏了一次罚，而**没有任何人知道** |

**这一轮里最值得记的是三条"假测试"**——它们都在 720 全绿的状态下守着空气:

| 假在哪 | 为什么骗过了自己 |
|---|---|
| 连接老化的两个 grpc option **零断言** | 用例只断言 duration 解析 + `assert server is not None`，注释里拿"非法 option 名会当场报错"当间接判据——**那句话是错的**：实测 `("grpc.totally_bogus_option_name", 1)` 与拼错的 `..._millis` grpcio 都静默接受。于是把 option 名拼错一个字母、或整段删掉，用例结构上不可能变红。已把 option 计算提成纯函数 `conn_age_options()` 直接比对列表 |
| 跨语言对拍在 **Go 侧真改了**的时候静默 skip | `_go_table()` 对 `rc != 0` 返回空表 → 与"go 不在 PATH"走同一条路径 → skip，文案还指控环境不可用。这几道 parity 门**恰好在最该响的那一刻不响**。已把**全部 4 条 go-run 对拍**（source_revision / kafkax / leaderboard / player_experience）拆成两条路：go 不在 → skip；go 在但编译/运行失败 → **fail 并带出 stderr** |
| `pandora_rpc_inband_total` / `panics` / `canceled` 三族**零断言** | 本批次新增的产物，`.inc()` 换成 `pass` 没有任何用例会红。而本仓业务失败是 in-band，`pandora_rpc_total` 的 code 与 Go 一样恒为 `"ok"`——灰度期"Python 副本哪个业务码在涨"**只有 inband 这一族**能回答 |

顺带修掉本文档自己的多处不实断言（测试条数、基础件数量自相矛盾、"其余 20 个服务没有
main.py"实为 19、§5.2.3 ⑩ 与 §6.4 互相矛盾、§4.3 声称三处租约都提前 TTL/3 而
`writerlease` 是固定 3s、README 的行尾约定只点名 2 个文件而实测有 15 个 CRLF）。
**文档里说做了而其实没做，比不写更糟**——接班人会据此跳过复核。

另修三条接线/parity：`owner` 的 `FOR UPDATE` 收窄到 BATTLE（Go 只对 BATTLE 读实例租约；
放开等于白拿一把行锁，与 allocator 的续租互相排队）、`provide_node` 自己拉起续约
（忘了调 `start_keepalive` 不报错，lease 自然过期后另一副本抢到同号而本进程毫不知情地继续发号）、
`build_http_app` 兑现注释里承诺却从未实现的 `PANDORA_HTTP_DOCS` 开关。

### 6.6 复核之后的自审又抓到两条（2026-08-19）

前一轮复核跑完、改完之后又自查了一遍"这一段新改的"。两条，都是**新改动本身引入的**：

| 缺陷 | 后果 |
|---|---|
| `provide_node` 自带续约后，`on_lost` 缺省成"退出进程" | 生产上是对的，但任何**忘了传的测试**会在一次续约抖动时 `os._exit(1)` —— **pytest 当场消失且没有任何报告**。而缺省成"只打日志"更糟：生产进程会毫不知情地继续发号 = 重号。两个方向都不安全，所以**不给缺省**：改成必填的工厂 `on_lost(holder) -> 无参可调用`，忘了传就在装配期 `ValueError`。工厂抛异常时还要收拾已抢到的 holder，否则留下一个永不续约、谁都看不见的 nodeID 占用 |
| 数据层用例共用**固定库名 + 固定 player_id**，且每个用例 `TRUNCATE` | 两个 pytest 进程同时跑必然互踩：一边 TRUNCATE、另一边正在断言条数 → 1205 锁等待、"上限 5 被突破：成功了 6 个" 这类**看起来像业务 bug 的假红**，而代码一个字没变 |

第二条是**实测定谳**的，不是猜：

```
共享库（当时机器上并行跑着 5 个 pytest）   6 failed / 82s
独占库（同一份代码，同一时刻）             18 passed / 4.8s
```

Go 侧本来就是每次跑 `CREATE DATABASE <唯一名>`。已补齐同一做法：DSN 不带库名时用
`pandora_test_<pid>_<ts>` 独占库，会话结束时删掉；DSN 里显式写了库名的仍照用。
验证：**两份 DB 测试同时跑，各自 33 passed**（修前必然假红），且跑完零残留库。

**同一形状在 Redis 上又出现了一次**：`test_push_offline` 的 fixture 用 `db=0` 且调
`flushdb()` —— 而 flushdb 冲的是**整个库**。两个 pytest 同时跑时一边正在断言、
另一边 flush，表现为"投递缓冲里的帧莫名其妙没了"。已改成从 PID 派生起点轮转、
取第一个 `DBSIZE==0` 的逻辑库（**真拿到独占**，不是取模碰运气）；16 个都被占就
skip 并说清原因，而不是去冲别人的库。验证：**三份同时跑，各自 20 passed**。

**第 4 维复核(测试牙齿)又抓到两条,都在我自己新写的测试里**:

| 缺陷 | 后果 |
|---|---|
| `test_run_emits_runtime_info` 的 `finally` 只 `task.cancel()`,没停 grpc server | `run()` 阻塞在 `await stop.wait()`,取消从那里抛出 → 优雅停机段**整段跳过** → server 保持 started,失败路径的 traceback 又持有它活到 loop 关闭之后 → `Server.__del__` 抛 "Event loop is closed" 并**卡住进程**。实测:变异后本用例打完结果不退出、90s 超时被杀(基线 2.77s 干净退出)。**这条用例真红时 CI 拿到的是 job 超时而不是一条 FAILED**,同进程后面的文件也不再跑 —— 红被降级成"挂住",最难归因。已改成先 `await server.stop()` 再 cancel;变异复验:1.9s 干净 FAILED |
| `test_provide_node_starts_keepalive_itself` 的判据是"lost 未置位" | **判据是反的**:`lost` 是本地 Event、只由续约循环置位 —— 循环压根不跑的话它**更容易**通过。复核用变异实测:把 `_keepalive_loop` 开头插 `return`,这条照样绿;把 `start_keepalive(on_lost(holder))` 改成 `start_keepalive()`(失主处置永不执行),**全绿**。已把判据换成 **etcd 侧事实**(过一个 TTL 后 key 仍在、lease 剩余为正),并补一条端到端(外部吊销 → 断言传进去的那个回调真被调用)+ 把"工厂"契约从注释变成 `callable` 校验。两条变异复验均变红 |

第二条的教训与 §7.2 那一问同源:**本地状态证明不了远端事实**。
"lost 没被置位"听起来像在说"续约好好的",实际只说明"没人来置位它" ——
而"没人来"恰恰包含了"循环死了"这个最坏情况。

> 这两条合起来是一条更一般的纪律:**共享的测试后端必须按进程隔离**。
> 判据很简单 —— 问一句"两个人同时跑这套测试会怎样"。答不上来的，
> 迟早会以"偶发红"的形态浪费别人半天。

⚠️ 这条值得记的不是"修了个测试隔离"，是 **"环境问题"是最容易糊弄自己的结论**。
第一反应是"并发跑当然会互相影响"，而那句话既解释不了是哪一格坏、也给不出判据。
换独占库跑一次才把它从猜测变成定谳 —— 也才发现它其实是可以修掉的，不必忍。

## 7. 方法论

### 7.1 凡能提纯成纯函数的，做跨语言对拍

不依赖跑起整个服务，却能钉死最容易静默偏移的逻辑。四条对拍全部零差异，其中
leaderboard 桶归属那次覆盖了 1224 条负分样本 —— 而 Go 的 `/` 向零截断、
Python 的 `//` 向下取整，这一格不对拍很难发现。

**但要认准什么才算"对拍"**：真正的模板是 `test_kafkax_parity.py` —— 它 `import` 真正的
Go 包、`cwd=pkg` 跑出来再比。有几条自称对拍的用例比的是**测试文件里手抄的 Go 副本**，
那只是自己跟自己比，Go 改了不会红（见 §6.4）。

### 7.2 防护型测试必须验证「拆掉防护会红」

测试通过 ≠ 测试在测你以为的东西。这条在本轮救了三次：friend 的 RR 对照（证实防护有效）、
writerlease 的激活超时对照（**证伪**，发现救场的是别的机制）、以及 2026-08-19 那批修复
逐条做的变异验证。

做法很机械：把被测的那道防护拆掉，跑测试，**必须红**。不红就说明救场的是别的东西，
你以为在测的那个不变量其实裸奔。

**但"拆掉防护会不会红"之前,还有一问:这条测试到底碰没碰到被测对象。**
本轮抓到的三条假测试里,两条根本碰不到 —— 一条测的是 duration 解析器
(而被测的是 grpc option 映射),一条比的是测试文件里手抄的 Go 副本
(而被测的是"我们跟真 Go 一不一致")。这类测试连变异都不用做:
它们从一开始就在测别的东西,而拆掉防护当然不红。

判据很朴素:**指着这条测试说出它读了被测代码的哪一个可观察产物** ——
返回值、指标样本、渲染出来的日志行、真实的 grpc 状态码。
说不出来的,基本就是在测空气。

⚠️ **变异实验不要在共享工作区里就地改文件**。本轮有一次把 `writerlease.py` 改成变异态
跑测试，而同一时刻另一个会话正在跑全量测试 —— 它会看到一堆红，去追一个不存在的 bug。
改法：变异跑在自己的副本上，或者像本轮最后那样，**只证明机制**（直接对真 etcd 演示
"不续约的话 leader key 第 5 秒就没了"），完全不碰仓库文件。

### 7.3 "验依赖能做对" ≠ "验我们用对了"

`verify_etcd.py` 11 条全过，证明的是 aetcd 的 lease / watch / txn 语义没问题。
但最重的两个 P0 恰恰在它之外：**etcd 对已失效 lease 的 `refresh()` 不报错、只回 TTL=0**，
而我们的续约循环只看"有没有抛异常"。SDK 是对的，调用方式是错的，工具一条都测不到。

所以依赖验证工具要配一条同伴规则：**每一处"我认为依赖会在 X 时报错"的假设，
都要单独写一个用例去证实它真的会报错**。

### 7.4 生成物被手改，只能靠门禁抓

`errcode.py` 顶上写着"由生成器产出，勿手改"，本轮仍然被手改了（加的字段是对的，
但生成器不知道）。发现它的不是 review，是 CI 门禁的 `gen_errcode.py --check`。
处置也不是把改动撤掉，而是**把生成器模板同步过去**，让那份改动能在下次重跑后活下来。

注释里的"勿手改"从来拦不住任何人；能拦住的只有一条会变红的机械检查。

