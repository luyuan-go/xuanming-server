"""把 21 个服务**全部用 Python 实现**拉起来(Go 侧对应 tools/scripts/run_services.ps1)。

移植期一直缺这一件:Go 栈有一键启动,Python 栈没有 —— 于是"21 个服务都移植完了"
从来没被验证成"21 个进程能同时起来并互相说得上话"。本脚本补的就是这一格。

用法::

    # 起全部(默认)
    cd python && .venv/Scripts/python.exe tools/run_stack.py

    # 只起一部分 / 排除某些
    .venv/Scripts/python.exe tools/run_stack.py --only login,player,team
    .venv/Scripts/python.exe tools/run_stack.py --exclude inventory

    # 停掉本脚本起过的所有 Python 服务进程
    .venv/Scripts/python.exe tools/run_stack.py --stop

前置:依赖容器要先起(``pwsh tools/scripts/dev_up.ps1``)+ 库结构要最新
(``pwsh tools/scripts/dev_migrate.ps1``)。

★ 判据是**端口在听 且 日志出现 service_ready**,不是"进程还活着"。
  只看进程会把"起来了但启动闸没过、正在退出"算成成功;只看端口会把
  "listen 了但配置校验还没跑完"算成成功。两个都要。

★ 每个服务必须在**自己的服务目录**下起:``config_table.dir`` / ``node.mysql_client``
  等相对路径是相对**进程工作目录**解析的,Go 版就是这么跑的。在 python/ 下起会读到
  不同的配表,现象是"起得来但数据不对"。

★ 本机 DS(mode=local)相关的路径**不写死在这里**:仓库纪律是机器专属路径不进版本库。
  需要跑真 DS 时由调用方在环境里给,本脚本原样透传:

      PANDORA_DS_LAUNCHER=editor|packaged
      PANDORA_DS_EXE=<UnrealEditor.exe 或 PandoraServer.exe>
      PANDORA_DS_UPROJECT=<Pandora.uproject>   # 仅 launcher=editor
      PANDORA_DS_DIR=<DS 进程工作目录>          # 与 PANDORA_DS_EXE 成对,缺了会 WinError 267
"""

from __future__ import annotations

import argparse
import os
import pathlib
import socket
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
VENV_PY = ROOT / "python" / ".venv" / "Scripts" / "python.exe"

# (服务名, 目录, 配置, gRPC 端口, 模块名覆盖)
# 与 tools/scripts/run_services.ps1 的 $Services 目录逐条对齐,顺序也照抄(按依赖排的)。
# 模块名默认与服务名同名;matchmaker_pve 是同一份实现 + 另一份配置。
SERVICES: list[tuple[str, str, str, int, str | None]] = [
    ("player_locator", "services/runtime/player_locator", "etc/locator-dev.yaml", 20006, None),
    ("hub_allocator", "services/battle/hub_allocator", "etc/hub_allocator-dev.yaml", 20021, None),
    ("player", "services/account/player", "etc/player-dev.yaml", 20002, None),
    ("ds_allocator", "services/battle/ds_allocator", "etc/ds_allocator-dev.yaml", 20020, None),
    ("push", "services/runtime/push", "etc/push-dev.yaml", 20014, None),
    ("team", "services/matchmaking/team", "etc/team-dev.yaml", 20010, None),
    ("friend", "services/social/friend", "etc/friend-dev-tidb.yaml", 20004, None),
    ("chat", "services/social/chat", "etc/chat-dev-tidb.yaml", 20005, None),
    ("guild", "services/social/guild", "etc/guild-dev-tidb.yaml", 20008, None),
    ("mail", "services/social/mail", "etc/mail-dev-tidb.yaml", 20009, None),
    ("dialogue", "services/social/dialogue", "etc/dialogue-dev.yaml", 20013, None),
    ("mission", "services/social/mission", "etc/mission-dev.yaml", 20019, None),
    ("data_service", "services/data/data_service", "etc/data_service-dev.yaml", 20003, None),
    ("trade", "services/economy/trade", "etc/trade-dev.yaml", 20012, None),
    ("inventory", "services/economy/inventory", "etc/inventory-dev.yaml", 20015, None),
    ("leaderboard", "services/runtime/leaderboard", "etc/leaderboard-dev.yaml", 20007, None),
    ("owner", "services/runtime/owner", "etc/owner-dev.yaml", 20017, None),
    ("auction", "services/economy/auction", "etc/auction-dev.yaml", 20016, None),
    ("battle_result", "services/battle/battle_result", "etc/battle_result-dev.yaml", 20022, None),
    ("matchmaker", "services/matchmaking/matchmaker", "etc/matchmaker-dev.yaml", 20011, None),
    ("matchmaker_pve", "services/matchmaking/matchmaker", "etc/matchmaker-pve.yaml", 20018,
     "matchmaker"),
    ("login", "services/account/login", "etc/login-dev.yaml", 20001, None),
]

MODULE_MARK = "pandorapy.services"


def listening(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.25)
        return s.connect_ex(("127.0.0.1", port)) == 0


DS_WINDOW_SCRIPT = ROOT / "python" / "tools" / "tail_ds_logs.py"


def stop_ds_window() -> int:
    """停掉本脚本起过的 DS 日志窗口(按脚本路径匹配,不碰别的 python)。

    每次启动都先停:一键启动是幂等的、会被反复双击,不先停就是每点一次多一个窗口,
    最后分不清哪个窗口跟的是这一轮的 DS。
    """
    ps = (
        "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | "
        "Where-Object { $_.CommandLine -like '*tail_ds_logs.py*' } | "
        "ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue; "
        "$_.ProcessId }"
    )
    out = subprocess.run(["powershell", "-NoProfile", "-Command", ps],
                         capture_output=True, text=True)
    return len([x for x in out.stdout.split() if x.strip()])


def default_ds_logdirs() -> list[pathlib.Path]:
    """大厅 / 战斗 DS 的日志目录。

    ★ 必须**从 SERVICES 的服务目录推**,不能写成 `ROOT/run/dev/logs/ds`:
      allocator 配的 `local.log_dir` 是相对路径 `run/dev/logs/ds`,而它相对的是
      **各服务进程的工作目录**(见本文件顶部:每个服务在自己的服务目录下起)。
      指错目录的表现是窗口永远空着,而人会把它读成"大厅服没起来" —— 比没有窗口更误导。
    """
    rels = {name: rel for name, rel, *_ in SERVICES}
    out = []
    for name in ("hub_allocator", "ds_allocator"):
        rel = rels.get(name)
        if rel:
            out.append(ROOT / rel / "run" / "dev" / "logs" / "ds")
    return out


def open_ds_window(ds_logdirs: list[pathlib.Path], env: dict[str, str]) -> None:
    """另开一个控制台窗口实时跟随本机 DS 日志。

    ★ 这是**只读跟随的独立进程**,不是给 DS 开控制台 —— 后者(`-log`)会因 Windows
      快速编辑模式在窗口里一点就冻住 DS 的游戏线程,让整个大厅永久不可进。
      成因与实测调用栈见 tools/tail_ds_logs.py 的模块文档,那条红线不许改回去。
    """
    if os.name != "nt":
        print("[skip] --ds-window 只在 Windows 上有意义")
        return
    if not DS_WINDOW_SCRIPT.exists():
        print(f"[WARN] 找不到 {DS_WINDOW_SCRIPT},跳过 DS 日志窗口")
        return
    argv = [str(VENV_PY), str(DS_WINDOW_SCRIPT)]
    for d in ds_logdirs:
        d.mkdir(parents=True, exist_ok=True)
        argv += ["--dir", str(d)]
    subprocess.Popen(
        argv, cwd=str(ROOT / "python"), env=env,
        creationflags=subprocess.CREATE_NEW_CONSOLE,  # type: ignore[attr-defined]
    )
    print("DS 日志窗口已弹出(跟随 %s);关掉它不影响 DS"
          % ", ".join(str(d) for d in ds_logdirs))


def stop_targets(targets: list[tuple[str, str, str, int, str | None]], scoped: bool) -> int:
    """停掉 `python -m pandorapy.services.*` 进程(不碰别的 python)。

    ★ `--stop` 必须尊重 `--only` / `--exclude`:早先这里无脑停全部,`--only a,b --stop`
      会把 22 个全停掉 —— 「我只要停两个」和「你把栈端了」是两回事。

    匹配用 **模块名 + 配置文件名**两者:matchmaker 与 matchmaker_pve 是同一个模块、
    不同配置,只看模块名分不开它俩。
    """
    if scoped:
        conds = " -or ".join(
            f"($cl -like '*pandorapy.services.{mod or name}.main*' -and $cl -like '*{conf.split('/')[-1]}*')"
            for name, _d, conf, _p, mod in targets
        )
        pred = f"$cl = $_.CommandLine; {conds}"
    else:
        pred = f"$_.CommandLine -like '*{MODULE_MARK}*'"
    ps = (
        "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | "
        f"Where-Object {{ {pred} }} | "
        "ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue; "
        "$_.ProcessId }"
    )
    out = subprocess.run(["powershell", "-NoProfile", "-Command", ps],
                         capture_output=True, text=True)
    killed = [x for x in out.stdout.split() if x.strip()]
    scope = ", ".join(t[0] for t in targets) if scoped else "全部"
    print(f"stopped {len(killed)} service process(es)  [{scope}]")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="拉起 Python 实现的 Pandora 服务栈")
    ap.add_argument("--logdir", default=str(ROOT / "run" / "dev" / "logs" / "python"),
                    help="各服务 stdout/stderr 落盘目录")
    ap.add_argument("--only", default="", help="逗号分隔:只起这些服务")
    ap.add_argument("--exclude", default="", help="逗号分隔:排除这些服务")
    # 90s 在机器空闲时够,但 22 个进程同时冷启动 + 本机还跑着真 DS / docker 时会不够,
    # 表现是**假失败**:表里写着没起来,实际再等几秒全部 service_ready 了。
    # 就绪判据本身要保守,等待窗口不必。
    ap.add_argument("--ready-timeout", type=float, default=210.0, help="等待就绪的总秒数")
    ap.add_argument("--stop", action="store_true", help="停掉已起的服务后退出")
    ap.add_argument("--ds-window", action="store_true",
                    help="另开一个窗口实时跟随本机 DS 日志(看大厅服/战斗服有没有起来)")
    ap.add_argument("--ds-logdir", action="append", default=None,
                    help="DS 日志目录,可重复;默认取大厅 / 战斗两个 allocator 各自的 "
                         "<服务目录>/run/dev/logs/ds")
    args = ap.parse_args()

    only = {s.strip() for s in args.only.split(",") if s.strip()}
    exclude = {s.strip() for s in args.exclude.split(",") if s.strip()}
    targets = [s for s in SERVICES
               if (not only or s[0] in only) and s[0] not in exclude]
    if not targets:
        print("[ERR] --only / --exclude 过滤后没有任何服务")
        return 2

    if args.stop:
        rc = stop_targets(targets, scoped=bool(only or exclude))
        closed = stop_ds_window()
        if closed:
            print(f"closed {closed} 个 DS 日志窗口")
        return rc

    if not VENV_PY.exists():
        print(f"[ERR] 找不到解释器 {VENV_PY};裸 python 在本机会弹 Microsoft Store")
        return 2

    # ★ 先停同名旧实例,再起 —— 让本命令**幂等**。
    #   双击一键启动时栈多半已经在跑;不先停就是往被占用的端口上再拉一批,
    #   新进程 bind 失败即退,而端口仍被老进程占着 —— 表面"起来了",实际跑的是老代码。
    #   这正是 tools/parity/README.md 规矩④记的那次教训。
    stop_targets(targets, scoped=True)
    time.sleep(1.0)  # 给端口释放留一拍,否则新进程可能撞上 TIME_WAIT

    logdir = pathlib.Path(args.logdir)
    logdir.mkdir(parents=True, exist_ok=True)

    env = dict(os.environ)
    # gen/ 必须在 path 上:生成的 pb2 是按 pandora.<domain>.v1 布局的。
    env["PYTHONPATH"] = os.pathsep.join(
        [str(ROOT / "python"), str(ROOT / "python" / "gen")]
    )
    env["PYTHONUTF8"] = "1"  # 中文日志撞 cp1252 会 UnicodeEncodeError

    procs = []
    for name, rel, conf, port, mod in targets:
        module = f"pandorapy.services.{mod or name}.main"
        logpath = logdir / f"{name}.log"
        log = logpath.open("wb")
        p = subprocess.Popen(
            [str(VENV_PY), "-m", module, "-conf", conf],
            cwd=str(ROOT / rel), env=env, stdout=log, stderr=subprocess.STDOUT,
        )
        procs.append((name, port, p, logpath))
        print(f"spawned {name:<15} pid={p.pid:<6} port={port}")

    # DS 是 allocator 在首次 AssignHub 时**懒拉起**的,所以窗口要早于就绪等待就位,
    # 否则大厅 DS 起来的那几行会被错过(--from-start 之外没有回放)。
    if args.ds_window:
        stop_ds_window()  # 幂等:不留上一轮的窗口
        dirs = ([pathlib.Path(d) for d in args.ds_logdir]
                if args.ds_logdir else default_ds_logdirs())
        open_ds_window(dirs, env)

    print("\n--- 等待就绪(端口在听 且 日志出现 service_ready)---")
    deadline = time.time() + args.ready_timeout
    pending = {n for n, _, _, _ in procs}
    while pending and time.time() < deadline:
        time.sleep(1.0)
        for name, port, p, logpath in procs:
            if name not in pending:
                continue
            if p.poll() is not None:
                pending.discard(name)  # 已经退出,不用再等
                continue
            if listening(port):
                try:
                    if "service_ready" in logpath.read_text(encoding="utf-8", errors="replace"):
                        pending.discard(name)
                except OSError:
                    pass

    print("\n%-16s %-6s %-7s %-9s %s" % ("SERVICE", "PORT", "ALIVE", "LISTENING", "RESULT"))
    ok = 0
    for name, port, p, logpath in procs:
        alive = p.poll() is None
        lis = listening(port)
        try:
            ready = "service_ready" in logpath.read_text(encoding="utf-8", errors="replace")
        except OSError:
            ready = False
        good = alive and lis and ready
        ok += good
        if good:
            result = "OK"
        elif not alive:
            result = f"DEAD exit={p.poll()}  见 {logpath.name}"
        elif not ready:
            result = f"没打出 service_ready  见 {logpath.name}"
        else:
            result = "端口没在听"
        print("%-16s %-6d %-7s %-9s %s" % (name, port, alive, lis, result))

    print(f"\nREADY {ok}/{len(procs)}   日志目录:{logdir}")
    return 0 if ok == len(procs) else 1


if __name__ == "__main__":
    sys.exit(main())
