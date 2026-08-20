"""ds_allocator 的 HTTP 运维面 —— 对应 Go 侧
`services/battle/ds_allocator/internal/server/http.go`(166 行)。

两个端点:
  - `/metrics`:Prometheus 抓取。**抓取前先刷新一次 writer lease 的健康 gauge**
    —— 这些值不是事件驱动的,没人推,只能在被抓时现算。
  - `/healthz/writer`:sweep 单写者租约的健康探针。degraded 时回 **503**。

═══════════════════════════════════════════════════════════════════════════════
为什么要有 /healthz/writer(照抄 Go 的理由)
═══════════════════════════════════════════════════════════════════════════════

心跳 sweep 是**单写者**循环:它按 writer lease 决定"这一轮该不该由我扫"。租约选举
持续失败时,进程本身完全健康(gRPC 全绿、/healthz 200、日志安静),但**没有任何副本
在扫超时对局** —— 崩了的战斗 DS 不会被判弃、pod 不会被回收、玩家的段位不会回滚。
这是一类"服务看起来全好、业务全停"的静默故障,只能靠一个独立探针暴露。

★ `snapshot()` 在 lease 未注入时**明确 fail-closed 到"未启用"**(`enabled=false`,
  gauge 全部归零),而不是回一个 `held=true` 的乐观值。这一点被 Go 注释专门点名:
  fail-open 的健康探针比没有探针更糟 —— 它会把"没人在扫"报成"扫得好好的"。
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from prometheus_client import Gauge
from starlette.requests import Request
from starlette.responses import JSONResponse

from pandorapy import metrics as pmetrics
from pandorapy import writerlease

# ── Prometheus gauge(名字与 Help 逐字节抄自 Go)───────────────────────────────
#
# ★ 名字漂移的后果与日志字段漂移同类:Grafana 面板与告警规则按名字建,改名后面板
#   **静默变空**,而服务一切正常、日志无错。
WRITER_HELD = Gauge(
    "pandora_ds_allocator_writer_held",
    "1 when this replica currently holds the ds_allocator sweep writer lease.",
)
WRITER_DEGRADED = Gauge(
    "pandora_ds_allocator_writer_degraded",
    "1 when the ds_allocator sweep writer lease is degraded (campaign or activation errors exceed threshold).",
)
WRITER_TOKEN = Gauge(
    "pandora_ds_allocator_writer_token",
    "Current monotonic fencing token of the ds_allocator sweep writer lease (0 when not held).",
)
WRITER_CAMPAIGN_ERRORS = Gauge(
    "pandora_ds_allocator_writer_campaign_errors",
    "Consecutive ds_allocator sweep writer lease campaign errors.",
)
WRITER_ACTIVATION_ERRORS = Gauge(
    "pandora_ds_allocator_writer_activation_errors",
    "Consecutive ds_allocator sweep writer lease activation errors.",
)
WRITER_LEASE_ENABLED = Gauge(
    "pandora_ds_allocator_writer_lease_enabled",
    "1 when the ds_allocator sweep writer lease is wired in this process.",
)


@runtime_checkable
class WriterLeaseHealth(Protocol):
    """只取健康快照的最小接口。`pandorapy.writerlease.WriterLease` 满足它。

    ★ 用 Protocol 而不是直接标注 `WriterLease`:测试要能塞一个返回固定快照的假对象,
      而不必真起一个 etcd 选举。
    """

    def health(self) -> writerlease.HealthSnapshot: ...


class WriterHealthHolder:
    """持有 sweep 单写者租约,并把它的健康投影成 gauge + JSON。

    对应 Go 的 `WriterHealthHolder`。`set` 在 main 里 lease 构造完成后调用一次。
    """

    __slots__ = ("_lease", "_mode")

    def __init__(self) -> None:
        self._lease: WriterLeaseHealth | None = None
        self._mode = ""

    def set(self, lease: WriterLeaseHealth | None, mode: str) -> None:
        """注入租约与运行档位(如 "etcd" / "local")。对应 Go 的 `Set`。"""
        self._lease = lease
        self._mode = mode

    def snapshot(self) -> dict[str, Any]:
        """刷新 gauge 并返回 JSON body。对应 Go 的 `snapshot`。

        ★ lease 为 None 时**全部归零 + enabled=false**,不是 fail-open:见模块头。
        """
        if self._lease is None:
            WRITER_LEASE_ENABLED.set(0)
            WRITER_HELD.set(0)
            WRITER_DEGRADED.set(0)
            WRITER_TOKEN.set(0)
            body: dict[str, Any] = {"enabled": False}
            if self._mode:
                body["mode"] = self._mode
            return body
        health = self._lease.health()
        degraded = health.degraded()
        WRITER_LEASE_ENABLED.set(1)
        WRITER_HELD.set(1 if health.held else 0)
        WRITER_DEGRADED.set(1 if degraded else 0)
        WRITER_TOKEN.set(health.token)
        WRITER_CAMPAIGN_ERRORS.set(health.consecutive_campaign_errs)
        WRITER_ACTIVATION_ERRORS.set(health.consecutive_activation_errs)
        body = {
            "enabled": True,
            "held": health.held,
            "token": health.token,
            "consecutive_campaign_errs": health.consecutive_campaign_errs,
            "consecutive_activation_errs": health.consecutive_activation_errs,
            "escalate_after": health.escalate_after,
            "degraded": degraded,
        }
        if self._mode:
            body["mode"] = self._mode
        # omitempty 语义:错误原文为空时**不出这个键**,与 Go 的 json tag 一致。
        # 出一个 "last_campaign_err": "" 会让"从没出过错"和"出过错但原文丢了"看起来一样。
        if health.last_campaign_err:
            body["last_campaign_err"] = health.last_campaign_err
        if health.last_activation_err:
            body["last_activation_err"] = health.last_activation_err
        return body

    def degraded(self) -> bool:
        """探针是否该回 503。lease 未注入时**不算 degraded**(等价"本进程不跑 sweep")。"""
        if self._lease is None:
            return False
        return self._lease.health().degraded()


def install(app: Any, holder: WriterHealthHolder) -> None:
    """把两个端点挂到 `pandorapy.server.build_http_app` 造出来的 app 上。

    对应 Go 的 `NewHTTPServer`。

    ★ 为什么要**先摘掉**已有的 `/metrics`:`build_http_app` 已经注册过一个裸的
      Prometheus handler。Starlette 的路由是"先匹配先赢",直接再 add 一个同路径的
      route 永远不会被命中 —— 那样 writer gauge 就永远停在初始值 0,面板上看是
      "租约从没被持有过",而实际一切正常。摘掉再挂是唯一能保证顺序的做法。
    """
    app.router.routes = [
        route
        for route in app.router.routes
        if getattr(route, "path", None) != "/metrics"
    ]

    async def metrics_with_writer_health(request: Request):  # noqa: ANN202
        # 抓取即刷新:这些 gauge 没有事件驱动源,只能在被抓时现算(同 Go)。
        holder.snapshot()
        return await pmetrics.metrics_endpoint(request)

    async def writer_health(_request: Request) -> JSONResponse:
        body = holder.snapshot()
        # degraded → 503:让 k8s / 告警把"没人在扫超时对局"当成真正的不健康,
        # 而不是靠人去翻 gauge。
        return JSONResponse(body, status_code=503 if holder.degraded() else 200)

    app.add_route("/metrics", metrics_with_writer_health, methods=["GET"])
    app.add_route("/healthz/writer", writer_health, methods=["GET"])


__all__ = [
    "WRITER_ACTIVATION_ERRORS",
    "WRITER_CAMPAIGN_ERRORS",
    "WRITER_DEGRADED",
    "WRITER_HELD",
    "WRITER_LEASE_ENABLED",
    "WRITER_TOKEN",
    "WriterHealthHolder",
    "WriterLeaseHealth",
    "install",
]
