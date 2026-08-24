"""把本机 DS(大厅 / 战斗)的日志实时汇到**一个独立窗口**里。

为什么不是"给 DS 开一个控制台窗口"
------------------------------------
因为那正是 2026-08-23 定谳的死锁源。UE DS 加 `-log` 会开一个真的 Windows 控制台,
而 Windows 控制台默认开着「快速编辑」——只要有人在那个黑窗口里点一下或选中文字,
`WriteConsole` 就一直阻塞,把**整个游戏线程**冻住:

    FWindowsConsoleOutputDevice::Serialize   ← 阻塞在这
      ← UIpNetDriver::TrackAndLogNewIP       ← 第一个客户端连入时打的那行
      ← UIpNetDriver::TickDispatch ← UWorld::Tick ← FEngineLoop::Tick

现象是 DS accept 连接后 CPU 归零、心跳停止、日志一行不出、进程仍 Responding,客户端
20 秒后超时退回登录 —— **一次误点就能让整个大厅永久不可进**。所以 hub_allocator /
ds_allocator(Go 与 Python 四处)一律用 `-stdout -FullStdOutLogOutput` + stdout 重定向
到文件,DS **永远不拥有控制台**。这条不许改回去。

本脚本给的是同样的可见性、但没有那条耦合:它是**另一个进程**在读日志文件。你在这个
窗口里点选、暂停、甚至把它关掉,最坏也只是这个 tailer 停了,DS 一根汗毛都动不了。

用法::

    .venv/Scripts/python.exe tools/tail_ds_logs.py                 # 只跟这一轮新起的 DS
    .venv/Scripts/python.exe tools/tail_ds_logs.py --recent 2      # 再带上最近 2 份历史日志
    .venv/Scripts/python.exe tools/tail_ds_logs.py --dir <目录>    # 可重复给多次
    .venv/Scripts/python.exe tools/tail_ds_logs.py --grep "admission|heartbeat"

★ **默认是两个目录,不是一个。** DS 的 `local.log_dir` 配的是相对路径
  `run/dev/logs/ds`,而它相对的是**各自服务进程的工作目录**(run_stack 让每个服务在
  自己的服务目录下起),于是大厅 DS 和战斗 DS 的日志落在两棵树里:

      services/battle/hub_allocator/run/dev/logs/ds/   ← 大厅 DS
      services/battle/ds_allocator/run/dev/logs/ds/    ← 战斗 DS

  只盯一个的后果是窗口一直空着,而人会把它读成"大厅服没起来"——比没有窗口更误导。

★ **默认只跟本进程启动之后新出现的日志文件。** 那两个目录里躺着上千份历史日志
  (每台 DS 一个 pod 一份,实测 1178 + 78),把它们都跟上等于开窗就刷上千条横幅、
  再每拍 stat 一千多个文件 —— 窗口直接废掉。想回看用 `--recent N`,它只补最近 N 份。

★ 默认**不过滤**:没有采样过真实 DS 日志就写死一张关键词表,等于赌哪些行重要 ——
  赌错的表现同样是"窗口一直空着"。要减噪时用 --grep 自己给正则。
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
#: 与 hub_allocator / ds_allocator 的 `local.log_dir`(相对各自服务工作目录)一致。
DEFAULT_DS_LOG_DIRS = [
    ROOT / "services" / "battle" / "hub_allocator" / "run" / "dev" / "logs" / "ds",
    ROOT / "services" / "battle" / "ds_allocator" / "run" / "dev" / "logs" / "ds",
]

POLL_SEC = 0.5
#: 单轮从单个文件最多搬多少字节。UE DS 冷启动会一次吐几十 KB,不设上限的话
#: 一个新文件会把窗口刷满、别的 DS 的行要等它搬完才出现。
CHUNK = 256 * 1024


class _Follower:
    """跟随一个日志文件。只记偏移,不长期持有 fd —— 每轮开关一次。

    不长期持有 fd 是有意的:Windows 上 allocator 会用 ``open(..., "wb")`` 重开同名
    文件(DS 重启),长期持有的句柄会指向已被替换的旧内容,窗口就此静默 ——
    看上去像"DS 没输出",实则是我们在读一个死文件。
    """

    __slots__ = ("path", "tag", "offset", "carry")

    def __init__(self, path: pathlib.Path, offset: int) -> None:
        self.path = path
        self.tag = path.stem
        self.offset = offset
        self.carry = b""

    def read_new(self) -> tuple[list[str], bool]:
        """返回(新整行列表, 是否检测到截断/重开)。"""
        try:
            size = self.path.stat().st_size
        except OSError:
            return [], False
        restarted = False
        if size < self.offset:
            # 文件变短 = 被重新创建(DS 重启)。从头再读。
            self.offset = 0
            self.carry = b""
            restarted = True
        if size == self.offset:
            return [], restarted
        try:
            with open(self.path, "rb") as f:
                f.seek(self.offset)
                buf = f.read(CHUNK)
        except OSError:
            return [], restarted
        self.offset += len(buf)
        data = self.carry + buf
        # 最后一段可能是半行(DS 正写到一半),留到下一轮再拼,否则会把一行劈成两半。
        parts = data.split(b"\n")
        self.carry = parts.pop()
        return [p.rstrip(b"\r").decode("utf-8", "replace") for p in parts], restarted


def _banner(text: str) -> None:
    print("=" * 78, flush=True)
    print(text, flush=True)
    print("=" * 78, flush=True)


def _scan(dirs: list[pathlib.Path]) -> list[pathlib.Path]:
    found: list[pathlib.Path] = []
    for d in dirs:
        try:
            found.extend(sorted(d.glob("*.log")))
        except OSError:
            continue
    return found


def _mtime(p: pathlib.Path) -> float:
    try:
        return p.stat().st_mtime
    except OSError:
        return 0.0


def main() -> int:
    ap = argparse.ArgumentParser(description="实时汇总本机 DS 日志(大厅 / 战斗)")
    ap.add_argument("--dir", action="append", default=None,
                    help="DS 日志目录,可重复;不给则用大厅 + 战斗两个默认目录")
    ap.add_argument("--grep", default="", help="只显示匹配该正则的行(默认全量)")
    ap.add_argument("--recent", type=int, default=0, metavar="N",
                    help="额外回看最近 N 份**已存在**的日志(默认 0:只跟新起的 DS)")
    args = ap.parse_args()

    dirs = [pathlib.Path(d) for d in args.dir] if args.dir else list(DEFAULT_DS_LOG_DIRS)
    pat = re.compile(args.grep) if args.grep else None

    # 启动时已存在的一律视为**历史**:默认整批跳过(那两个目录里有上千份),
    # 只有 --recent N 明确要求时才补最近 N 份。
    existing = _scan(dirs)
    seen: set[pathlib.Path] = set(existing)
    replay = sorted(existing, key=_mtime, reverse=True)[:max(args.recent, 0)]

    _banner(
        "Pandora 本机 DS 日志窗口\n"
        + "".join(f"  目录 : {d}{'' if d.exists() else '   (还不存在,DS 一起来就会出现)'}\n"
                 for d in dirs)
        + f"  过滤 : {args.grep or '(无,全量)'}\n"
        f"  历史 : 已存在 {len(existing)} 份,跳过 {len(existing) - len(replay)} 份"
        f"{'(要回看加 --recent N)' if not replay else f',回看最近 {len(replay)} 份'}\n"
        "  说明 : 这里是**只读跟随**,关掉它不影响 DS。等 [pandora-hub-local-*] 出现\n"
        "         就是大厅服起来了;匹配成功后会再冒出一个战斗 DS 的 pod。"
    )

    followers: dict[pathlib.Path, _Follower] = {}
    for p in sorted(replay, key=_mtime):
        followers[p] = _Follower(p, 0)
        _banner(f">>> 回看历史:{p.stem}")

    try:
        while True:
            for p in _scan(dirs):
                if p in seen:
                    continue
                # 启动之后新出现的文件 = 一台新 DS 刚被拉起来,必须**从头读**:
                # 它从落第一行到被我们发现之间隔着一个轮询周期,从末尾跟会把开头
                # 几行整段吞掉 —— 吞掉的恰恰是「起来了没有」那几行。冒烟实测过。
                seen.add(p)
                followers[p] = _Follower(p, 0)
                _banner(f">>> DS 上线:{p.stem}")
            for f in list(followers.values()):
                lines, restarted = f.read_new()
                if restarted:
                    _banner(f">>> DS 重启(日志已重开):{f.tag}")
                for line in lines:
                    if pat is not None and not pat.search(line):
                        continue
                    print(f"[{f.tag}] {line}", flush=True)
            time.sleep(POLL_SEC)
    except KeyboardInterrupt:
        print("\n(已停止跟随;DS 不受影响)", flush=True)
        return 0


if __name__ == "__main__":
    sys.exit(main())
