# 跨实现对拍探针（Go ↔ Python）

把 Go 版与 Python 版**起在同一份 conf、同一套依赖上**，跑同一批场景，输出逐字节 diff。

**为什么必须有这一步**：2026-08-19 用它抓到 7 条缺陷，**没有一条是单元测试能发现的** ——
全是"进程起得来、业务 RPC 全对、日志一行 ERROR 都没有，但线上是坏的"
（k8s 探针永不 Ready、trace 串不起来、告警永不触发、权威面的门排错了顺序）。
详见 `docs/design/python-migration.md` §5.2.2。

## 主循环（每个服务照这个做一遍）

以 `owner` 为例（`dialogue` 同理，端口 20013/20113）。

### 1. 起 Python 版

⚠️ **必须在服务目录下起**，不是在 `python/` 下。`config_table.dir` 与
`node.mysql_client` 等相对路径都是相对**进程工作目录**解析的，与 Go 版一致。

```bash
cd services/runtime/owner && PYTHONPATH="$PWD/../../../python;$PWD/../../../python/gen" PYTHONUTF8=1 ../../../python/.venv/Scripts/python.exe -m pandorapy.services.owner.main -conf etc/owner-dev.yaml
```

### 2. 起 Go 版到错开的端口

```bash
cd services/runtime/owner && sed -e 's/":20017"/":20117"/' -e 's/":21017"/":21117"/' etc/owner-dev.yaml > etc/owner-diffport.yaml && go build -o /tmp/owner-go.exe ./cmd/owner && /tmp/owner-go.exe -conf etc/owner-diffport.yaml
```

用完记得删掉 `etc/*-diffport.yaml`，别提交。

### 3. 对拍

第二个参数是 **player_id 段起点**，两次必须不相交（两个实现写同一个库）。

```bash
cd python && PYTHONUTF8=1 .venv/Scripts/python.exe tools/parity/probe_owner.py 20017 7300000 > /tmp/py.txt 2>&1 && PYTHONUTF8=1 .venv/Scripts/python.exe tools/parity/probe_owner.py 20117 7400000 > /tmp/go.txt 2>&1 && diff /tmp/py.txt /tmp/go.txt && echo "零差异"
```

## hub_allocator（`probe_hub.py`，端口 20021/20121）

⚠️ **必须把两侧的 `mode` 都改成 `"mock"`**（确定性假分片）。`dev` 默认是 `local`，
会去 exec 一个真的 Windows Hub DS 进程；`agones` 需要 k8s。`mock` 是唯一能让两个实现
看到**同一份分片拓扑**的模式，否则 diff 里全是"分片名不一样"的噪声。

```bash
cd services/battle/hub_allocator && sed -e 's/^mode: "local"/mode: "mock"/' -e 's/":20021"/":20121"/' -e 's/":21021"/":21121"/' etc/hub_allocator-dev.yaml > etc/hub_allocator-diffport.yaml
```

Python 侧同样要一份只改 `mode` 的副本。跑：

```bash
cd python && PYTHONUTF8=1 .venv/Scripts/python.exe tools/parity/probe_hub.py 20021 7500000 > /tmp/py.txt 2>&1 && PYTHONUTF8=1 .venv/Scripts/python.exe tools/parity/probe_hub.py 20121 7600000 > /tmp/go.txt 2>&1 && diff /tmp/py.txt /tmp/go.txt && echo "零差异"
```

**它比 owner/dialogue 多踩的一个坑（规矩②的加强版）**：分片镜像
`pandora:hub:shard:{pod}` 按 **pod 名**建行，**不按 player_id 分区** —— 只分 player_id
段挡不住它，上一轮留下的 `player_count` 会被下一轮读到。但人数正是要验的东西，
不能按规矩③盖掉。`probe_hub.py` 的做法是**打相对本次运行基线的增量**
（`dump_hubs` 的 `count_delta`），并在收尾把自己占的名额全部 `ReleaseHub` 还回去
（场景 36 断言全部回到 `+0`）。绝对起点的差异被消掉，记账正确性照样逐字节比。

## ds_allocator（`probe_ds.py`，端口 20020/20120）

同样**必须把两侧 `mode` 都改成 `"mock"`**（确定性假地址）。`dev` 默认 `local`，
会真去 exec 一个 Windows DS 进程——两个实现各 exec 一份、端口互抢，
diff 里全是"谁抢到端口"的噪声，与实现分叉无关。

```bash
cd services/battle/ds_allocator && sed -e 's/^mode: "local"/mode: "mock"/' -e 's/":20020"/":20120"/' -e 's/":21020"/":21120"/' etc/ds_allocator-dev.yaml > etc/ds_allocator-diffport.yaml
```

```bash
cd python && PYTHONUTF8=1 .venv/Scripts/python.exe tools/parity/probe_ds.py 20020 7700000 > /tmp/py.txt 2>&1 && PYTHONUTF8=1 .venv/Scripts/python.exe tools/parity/probe_ds.py 20120 7800000 > /tmp/go.txt 2>&1 && diff /tmp/py.txt /tmp/go.txt && echo "零差异"
```

**它自己的坑（规矩①的另一种形态）**：分配是**一次性资源占用**，不像 owner 那样能反复重置。
所以每条场景必须用**自己的 `match_id`**（`BASE + n`），不能复用——复用的话第二条起
永远落在 `allocate_idempotent_hit` 快路径上，diff 是零但什么都没验。
`ListBattles` 则不需要 hub 那套基线增量：`match_id` 本身就带段，
`dump_battles` 直接按 `mine` 集合过滤掉别的运行留下的行即可。

## 四条写探针的规矩（每条都是踩出来的）

**① 先证明"真的走到了目标分支"，再看 diff。**
第一版 owner 探针三条标 ★ 的场景**全落在幂等快路径上**，diff 仍是零 —— 验了个寂寞。
具体两个坑：屏障只在旧 owner=`BATTLE` 时才等待（HUB 是协作迁移，刻意不等，
否则每次进大厅卡 27 秒）；epoch 冲突必须换一个**不同的** target
（同 target 在 epoch 校验**之前**就 no-op 返回了）。
所以每条 ★ 场景都要把实际 code 打出来自查，不能只依赖 diff。

**② 跨运行共享的资源要分段。**
`ds_instance_lease` 按 `instance_uid` 建行、**不按 player_id 分区** ——
只分 player_id 段不够，上一轮留下的租约会被下一轮读到，
表现是 `lease_deadline_ms` 时有时无，看起来像实现分叉。
`probe_owner.py` 的 `target()` 把 pod/uid 都拼上了 `BASE`。

**③ 归一化只能盖"必然不同"的，不能盖要验的东西。**
player_id（段不同）、绝对时间戳、服务端自生成的 UUID —— 这些可以盖。
但 `retry_after_ms` 只做**量级分桶**（是不是 0 正是要验的）；
显式传入的 `operation_id` 必须原样保留（否则"有没有正确回显幂等键"被盖掉）。
`operation_id` 必须是 canonical UUIDv4，且要**写死**常量 —— 现铸的话两次运行输出必不同。

**④ 每个 diff 都要做对照实验。**
往一侧注入一个缺陷，确认 diff 变红，再还原。
第一次做时老进程还占着端口、带缺陷的新进程 bind 失败直接退出，
diff 比的还是原版 —— **"没抓到"当时不算通过**，得先确认新进程真的起来了。
