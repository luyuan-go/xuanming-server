# Pandora Python 后端(strangler 迁移,与 Go 版并存)

本目录是 Go 后端向 Python 迁移的工作区。分支 `python-migration`。

> **本文只讲「怎么在这里干活」。** 迁到哪一步、验了什么、还缺什么、下一步怎么走 ——
> 一律以 [`docs/design/python-migration.md`](../docs/design/python-migration.md) 为准,
> 那里是这条线的唯一交接入口。两处都写状态必然漂移,所以这里刻意不重复。

设计前提:**不是替换,是并存**。同一份 `.proto`、同一份 `etc/*.yaml`、同一套 Envoy 路由、
同一套 Grafana/Loki/Alloy/Prometheus。Python 服务与 Go 服务在协议层同构,Envoy 不知道后面
是哪个语言,所以可以按服务逐个灰度、逐个回滚。

## 目录

```
python/
  pyproject.toml            两个包根:. → pandorapy,gen → pandora/proto2mysql

  pandorapy/                共享基础件(对应 Go 的 pkg/)
    _utf8.py                强制 UTF-8 I/O(Windows cp1252 会丢整条中文日志)
    log.py / logwindow.py   structlog,字段口径逐字对齐 pkg/log 的 zap;弱依赖失败降噪窗口
    errcode.py              ★ 由 tools/gen_errcode.py 从 Go 源码生成,勿手改
    config.py               pydantic 模型,读现有 etc/*.yaml
    configtable.py          manifest + sha256 + 整批 fail-closed
    metrics.py              prometheus_client
    interceptors.py         grpcio 拦截器:trace / 可观测(access log)/ 关停 / 超时 / 鉴权
    errcode_grpc.py         errcode → gRPC 标准状态码(对应 Go 的 pkg/errcode/grpc.go)
    safego.py               ★ 后台协程兜底(asyncio 吞异常,比 Go 崩进程更静默)
    server.py               grpcio + FastAPI 双 server(并联,非串联)
    godur.py                Go time.Duration.String() 格式
    snowflake.py            位布局逐位对齐 pkg/snowflake(秒级,非毫秒)
    snowflake_etcd.py       etcd nodeID 抢占 + 失租退出(多副本才需要)
    etcdleader.py           选主(保护「同一任务只跑一份」)
    writerlease.py          单写者租约(保护「同一权威只有一个写者」)
    etcdlease.py            ★ etcd lease 续约的唯一正确姿势,上面三个都用它
    placement.py            fence / lease 常量与 operation_id 规则
    fence_timeline.py       跨 5 处常量的时间线不等式校验入口
    source_revision.py      Hub assignment 来源版本(INC-20260818-003)
    auth.py                 JWT 签发 / 验签(账号态与玩家态 audience 分离)
    cellroute.py            region/cell 静态路由表
    killswitch.py           RPC 级临时关停规则
    mysqlx.py / dbguard.py  错误码映射 / TiDB 断言;严格模式与容量守护
    redisx.py               客户端 / Lua / 分布式锁
    kafkax.py               一致性哈希分区器 + KeyOrderedProducer / KeyOrderedConsumer
    kafka_topics.py         ★ 由 tools/gen_kafka_topics.py 从 Go 生成,勿手改
    sessiongate.py          ★ 会话现行性门(顶号后的旧 JWT 24h 内仍可用,缺了完全静默)
    errcode_grpc.py         错误码 → gRPC 状态码(顶号刻意映射 ABORTED,见模块注释)
    protosql.py             从 proto 描述符推导 DDL(proto2mysql 的替代)
    services/<service>/     各服务的等价实现,见下方「只有 dialogue 能起进程」

  tools/gen_errcode.py      errcode 生成器(--check 是 CI 门)
  tools/gen_kafka_topics.py kafka topic 生成器(--check 是 CI 门)
  tools/verify_etcd.py      etcd 语义实测(lease / watch / txn CAS)
  tools/parity/             跨实现对拍:coverage.py(RPC 覆盖矩阵 + 静默缺陷门)
                            + probe_dialogue.py / probe_owner.py(双进程逐字节 diff)
  gen/                      buf 生成产物(pandora/ + proto2mysql/),刻意入库
  tests/                    39 个测试文件,含跨语言 parity 门与真依赖故障注入
  requirements.lock         ★ 依赖锁(入库,CI 按它装);改 pyproject 依赖后必须重新 compile
```

### ⚠️ 21 个服务里只有 2 个能起进程

`services/` 下有 21 个包,但**只有 `dialogue` 与 `owner` 有 `main.py`**。其余都是逐块移植的
`biz` / `data` 等价实现 + 针对性测试,**不能独立起服务**。

当下的机械事实(RPC 方法级)随时可以重新生成,别信手写的清单:

```bash
python tools/parity/coverage.py            # 覆盖矩阵
python tools/parity/coverage.py --check    # 只拦"写了但不生效"的静默缺陷
```

哪个服务迁到了哪一层、还缺什么,见
[`docs/design/python-migration.md`](../docs/design/python-migration.md) §2 与
[`python-migration-coverage.md`](../docs/design/python-migration-coverage.md)(生成物)。

## 跑起来

```bash
cd python && uv venv --python 3.13 && uv pip install -e ".[dev]"
```

`.[dev]` 已经带上 `storage` 组(asyncmy / cryptography / kafka-python)。**别只装基础组** ——
少了它们,三个 MySQL 数据层测试文件会整体 `importorskip` 跳过,而 pytest 照样打绿。

中心 MySQL 的 Python 池使用 `minsize=0`、`maxsize=max_open_conns`。`asyncmy.minsize`
表示“启动时预建多少连接”，不是 Go 的 `MaxIdleConns`；后者没有公开等价参数，不能冒充。
策划全量 14 个 MySQL pool、每池 `max_open_conns=4` 时，单台最坏上限是
`14 * 4 = 56`，启动预热是 `14 * 0 = 0`。这只是静态配置上界；中心库尚未完成多台
策划机容量 E2E，容量门禁仍保持“未验证”，不能据此宣称中心已承载通过。

重新生成 proto stub:

```bash
cd proto && buf generate --template buf.gen.python.yaml
```

启动 dialogue(**必须在服务目录下**,`config_table.dir` 相对进程工作目录,与 Go 版同一契约):

```bash
cd services/social/dialogue && PYTHONUTF8=1 ../../../python/.venv/Scripts/python.exe -m pandorapy.services.dialogue.main -conf etc/dialogue-dev.yaml
```

## 跑测试

```bash
cd python && PYTHONUTF8=1 .venv/Scripts/python.exe -m pytest tests/ -q -rs
```

**判据是「全绿 **且** 0 skipped」。skip 视同不通过。**

测试刻意打真依赖(etcd / MySQL / Redis)而不是 mock —— mock 掉 etcd 就等于把被测对象换成了
「我以为 etcd 是怎样的」,而本轮最重的两个缺陷恰恰来自 etcd 的真实语义与直觉不符。
代价是缺依赖时会**静默跳过**:实测缺三者时 592 个用例里有 **83 个**(14%)跳过,
覆盖的正是脑裂、重号、投递缓冲这些最危险的部分。

起依赖(端口与测试默认值一致,起了就不用配任何环境变量):

```bash
docker run -d --name pandora-etcd-verify  -p 12379:2379 quay.io/coreos/etcd:v3.5.17 etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://127.0.0.1:12379
docker run -d --name pandora-mysql-verify -p 13306:3306 -e MYSQL_ROOT_PASSWORD=pandora_dev_root mysql:8.4 --sql-mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"
docker run -d --name pandora-redis-verify -p 16379:6379 redis:8-alpine
```

库不用预建 —— 数据层测试自己 `CREATE DATABASE`(与 Go 侧 `*_mysql_test.go` 同做法,
见 `tests/mysqlfixture.py`)。而且 DSN **不带库名**时每个 pytest 进程建自己的独占库
`pandora_test_<pid>_<ts>`、会话结束时删掉 —— 所以**两个人/两个窗口同时跑测试不会互相踩**。
(共享固定库名时会踩:一边 TRUNCATE、另一边正在断言条数,表现为 1205 锁等待和
"上限 5 被突破" 这类看起来像业务 bug 的假红。)DSN 里显式写了库名的仍照用。要改地址用与 Go 侧 CI 同名的环境变量覆盖:
`PANDORA_TEST_ETCD_ENDPOINTS` / `PANDORA_TEST_MYSQL_DSN` / `PANDORA_TEST_REDIS_ADDR`。

跨语言对拍的用例还需要 `go` 在 PATH 上(它们真的会去跑 Go 包取输出对比)。

## CI

`tools/scripts/ci_backend.ps1` 里有 Python 门禁,两道:

1. `tools/gen_errcode.py --check` —— errcode 与 Go 侧一致(改了 Go 的码值不同步就红)
2. `pytest tests/ -q -rs` —— 全量测试;数据库组的跳过在 `-RequireDbTests` 下**判失败**
   (口径与 Go 侧 `go_test_skip_audit` 一致:跳过不等于通过)

环境由 uv 现建,依赖**每轮按锁文件同步**(`uv pip sync requirements.lock` + `uv pip install -e . --no-deps`)。
CI 机需要 `uv`(已进 `tools/devops/bootstrap-machine.ps1` 前置工具表)。

⚠️ **改了 `pyproject.toml` 的依赖就必须重新生成锁文件**,否则 CI 装的还是旧的:

```bash
cd python && uv pip compile pyproject.toml --extra dev --extra storage --output-file requirements.lock
```

按 `>=` 下界装的话,上游随便发一个新版就能在**仓库零改动**的情况下把流水线打红
(或更糟:悄悄换掉一个行为不同的实现)。本仓其它依赖都是钉死的,Python 侧没道理例外。

## 迁移中发现的真实差异(都已修,逐条记在代码注释里)

这些全部是"**不报错、只静默出错**"的类型,是迁移的主要风险来源:

1. **`level` 词表不同** —— zap 是 `warn`/`fatal`,Python logging 是 `warning`/`critical`。
   而 `deploy/alloy/config.alloy` 把 `level` **直接提成 Loki label**,所以按 `{level="warn"}`
   过滤的面板会静默漏掉 Python 侧全部警告。→ `log.py` 的 `_ZAP_LEVEL_NAMES`
2. **服务名字段是 `service` 不是 `logger`** —— 且**必须进程级绑定**:最初绑在 `setup()` 返回的
   logger 上,导致 `biz.py` 用 `plog.get()` 打的行全都没有 `service`
3. **`msg` vs `event`** —— structlog 默认字段名是 `event`,1449 个事件名的 LogQL 全靠 `msg`
4. **日志级别环境变量是 `LOG_LEVEL`** —— 不是 `PANDORA_LOG_LEVEL`。全仓 7 处业务注释把
   「对单 pod 临时设 `LOG_LEVEL=debug`」写成标准排障手法,取错名字 = 那条手法对 Python 副本无效
5. **Windows stdout 是 cp1252** —— 日志含中文直接抛 `UnicodeEncodeError` 并**丢掉整条**;
   若发生在 `except` 分支会盖掉真正的故障
6. **grpcio 不接受裸端口 `:20013`** —— Go 的 `net.Listen` 接受。21 份 yaml 全是裸端口形式
7. **uvicorn `host="::"` 在 Windows 只绑 IPv6** —— grpcio 的 `[::]` 是双栈。IPv6-only 会让
   Prometheus 抓不到 `/metrics` → 面板静默变空
8. **uvicorn/grpcio 的日志是纯文本** —— 不是 JSON,Alloy 的 `stage.json` 解析不了。
   已把 stdlib logging 接进同一条渲染链
9. **`google.api` 需要单独装包** —— Go 侧由 `genproto` 提供,Python 侧要 `googleapis-common-protos`
10. **时长格式** —— Go 打 `time.Duration.String()`(5 分钟 = `"5m0s"`),不是 yaml 原值 `"5m"`;
    且不能用 `%g` 收尾(只保留 6 位有效数字,会把 Go 的完整精度截掉)
11. **`asyncio.CancelledError` 不是 `Exception` 的子类** —— grpc.aio 用取消来终止超时的 handler,
    panic 兜底若只写 `except BaseException` 会把每一次客户端超时都记成 panic
12. **`aetcd` 对已失效 lease 的 `refresh()` 不报错,只回 `TTL=0`** —— 见下节,这是最重的一条

## ★ etcd:唯一正确的续约姿势

Go 的 `clientv3.KeepAlive` 是一条流,租约没了流就断。`aetcd` 没有自动续约,只有一次性的
`lease.refresh()`,而 etcd 对**已经不存在**的 lease 的应答是「**正常返回,TTL=0**」。

所以 `try: await refresh() / except: 算失败` 这种写法有一个**永久静默**的洞:租约早没了、
key 早被别人抢走了,而本副本因为"没抛异常"一直把本地安全截止线往后推 —— 它会永远认为自己
还持有。三个模块原本都是这么写的(选主 / 单写者 / nodeID 抢占),后果分别是双 leader、
两个写者同时推 fence 水位、两个副本用同一 nodeID 发号。

**任何新的 etcd 租约代码一律走 `pandorapy/etcdlease.refresh_or_raise`**,别再自己写
`await lease.refresh()`。它区分两件事:连接层失败(可按本地安全窗重试)vs 服务端回 TTL≤0
(**已确定失主,必须立即让位,不得重试**)。

## 别踩的坑

- **`pytest.skip` 的文案里必须写清「不假装通过」+ 怎么起依赖**,现有用例都是这么写的,照抄。
- **async fixture 必须 function 作用域**:pytest-asyncio 给每个用例新建 event loop,
  而 asyncmy 的池把内部 Task 绑在创建时的 loop 上 —— module 作用域会从第二个用例开始
  全部报 `got Future attached to a different loop`,看起来像连接池坏了。
- **行尾是混的:15 个 .py 是 CRLF,其余是 LF —— 而且可能出现单文件内混行尾**
  (2026-08-19 实测;本轮就往一个 CRLF 文件里追加过 LF 段落)。整篇改写会炸出
  满屏行尾 churn,**按文件保行尾改**。动手前先量一下:

  ```bash
  cd python && for f in $(find pandorapy tests -name "*.py"); do
    cr=$(tr -cd '
' < "$f" | wc -c); [ "$cr" -gt 0 ] && echo "CRLF $f"; done
  ```

  别用 `sed`/`awk` 查行尾 —— Git Bash 的文本模式会吃掉 CR,把 CRLF 误报成 LF,
  只信 `tr -cd` 计数。
- **`gen/` 刻意不忽略**(与 Go 侧 `proto/gen/go` 同规则):生成物进版本库,没装 buf 的机器
  也能直接跑,review 时也能看到协议改动的实际影响面。
