"""hub_allocator 的 HTTP 运维面 —— 对应 Go 侧
`services/battle/hub_allocator/internal/server/http.go`(169 行)。

两个端点:
  - `/metrics`:Prometheus 抓取。**抓取前先刷新一次写者租约的健康 gauge** ——
    这些值不是事件驱动的,没人推,只能在被抓时现算(同 Go 的 `NewHTTPServer`)。
  - `/healthz/writer`:写者继任租约的健康探针。degraded 时回 **503**。

═══════════════════════════════════════════════════════════════════════════════
它**不是** K8s 探针(照抄 Go 的理由)
═══════════════════════════════════════════════════════════════════════════════

失去领导权的副本是**有意的热备**:拒写但可秒级接管。把 readiness 门成"必须是
writer"会在滚动升级时死锁 —— 新副本要 Ready 才能让旧副本让位,旧副本不让位新副本
就当不上 writer;全体当不上时更会把"写降级"放大成"整服零端点"。所以这里只暴露状态
供告警:degraded=true 持续超过 lease TTL 兜底窗口 = 真的长期无主,该报警。

★ 真正的全局信号是 **没有任何副本上报 held=1**,JSON 端点表达不了(只能人工 curl),
  所以必须同时出 gauge:

      sum(pandora_hub_allocator_writer_held) == 0  持续 > lease TTL 兜底窗口 → 长期无主

  这一条同时覆盖竞选永久阻塞、激活永久阻塞与全体竞选失败三种形态,不依赖任何
  "函数返回了错误"的前提。

★ `snapshot()` 在租约未注入时**明确 fail-closed 到"未启用"**(`enabled=false`,
  gauge 全部归零),而不是回一个乐观的 `degraded=false, held=true`。Go 注释专门点名
  过这个 fail-open:nil 租约回 200 会把"根本没启用单写者保护"伪装成健康。
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
    "pandora_hub_allocator_writer_held",
    "1 = this replica currently holds the hub_allocator writer lease (announced, post-activation).",
)
WRITER_DEGRADED = Gauge(
    "pandora_hub_allocator_writer_degraded",
    "1 = this replica is neither the writer nor able to become one (campaign/activation failing persistently).",
)
WRITER_TOKEN = Gauge(
    "pandora_hub_allocator_writer_token",
    "Current fencing token of this replica's writer term (0 = not held).",
)
WRITER_CAMPAIGN_ERRORS = Gauge(
    "pandora_hub_allocator_writer_campaign_errors",
    "Consecutive writer-lease campaign failures (reset on election).",
)
WRITER_ACTIVATION_ERRORS = Gauge(
    "pandora_hub_allocator_writer_activation_errors",
    "Consecutive writer-lease activation failures (elected but not yet writable; reset on success).",
)
WRITER_LEASE_ENABLED = Gauge(
    "pandora_hub_allocator_writer_lease_enabled",
    "1 = writer succession lease is wired on this replica (0 = legacy/off; single-writer relies on the deploy strategy).",
)


@runtime_checkable
class WriterLeaseHealth(Protocol):
    """只取健康快照的最小接口。`pandorapy.writerlease.WriterLease` 满足它。

    ★ 用 Protocol 而不是直接标注 `WriterLease`:测试要能塞一个返回固定快照的假对象,
      而不必真起一个 etcd 选举。
    """

    def health(self) -> writerlease.HealthSnapshot: ...


class WriterHealthHolder:
    """持有写者继任租约,并把它的健康投影成 gauge + JSON。对应 Go 的 `WriterHealthHolder`。

    让 HTTP 面能先于租约构造:租约只在 Model B(authority_mode=redis)下启动,且必须
    排在 dsauthfence capability 之后,由 main 在租约就绪后 `set` 注入。
    """

    __slots__ = ("_lease", "_mode")

    def __init__(self) -> None:
        self._lease: WriterLeaseHealth | None = None
        self._mode = ""

    def set(self, lease: WriterLeaseHealth | None, mode: str) -> None:
        """注入租约与档位(mode 取值见 `conf.WRITER_LEASE_*`)。未调用 = 未启用继任租约。"""
        self._lease = lease
        self._mode = mode

    def snapshot(self) -> dict[str, Any]:
        """刷新 gauge 并返回 JSON body。对应 Go 的 `snapshot`。

        ★ 字段名与 omitempty 语义逐条对着 Go 的 `writerHealthBody` json tag:
          只有 `mode` / `last_campaign_err` / `last_activation_err` 是 omitempty,
          其余字段**恒出**(哪怕是零值)。出一个 `"last_campaign_err": ""` 会让
          "从没出过错"和"出过错但原文丢了"看起来一样;反过来漏掉恒出的 `held`
          会让告警规则的字段解析在"未启用"分支上静默失败。
        """
        if self._lease is None:
            WRITER_LEASE_ENABLED.set(0)
            WRITER_HELD.set(0)
            WRITER_DEGRADED.set(0)
            WRITER_TOKEN.set(0)
            body: dict[str, Any] = {"enabled": False}
            if self._mode:
                body["mode"] = self._mode
            body.update(
                {
                    "held": False,
                    "token": 0,
                    "consecutive_campaign_errs": 0,
                    "consecutive_activation_errs": 0,
                    "escalate_after": 0,
                    "degraded": False,
                }
            )
            return body
        health = self._lease.health()
        degraded = health.degraded()
        WRITER_LEASE_ENABLED.set(1)
        WRITER_HELD.set(1 if health.held else 0)
        WRITER_DEGRADED.set(1 if degraded else 0)
        WRITER_TOKEN.set(health.token)
        WRITER_CAMPAIGN_ERRORS.set(health.consecutive_campaign_errs)
        WRITER_ACTIVATION_ERRORS.set(health.consecutive_activation_errs)
        body = {"enabled": True}
        if self._mode:
            body["mode"] = self._mode
        body["held"] = health.held
        body["token"] = health.token
        body["consecutive_campaign_errs"] = health.consecutive_campaign_errs
        if health.last_campaign_err:
            body["last_campaign_err"] = health.last_campaign_err
        body["consecutive_activation_errs"] = health.consecutive_activation_errs
        if health.last_activation_err:
            body["last_activation_err"] = health.last_activation_err
        body["escalate_after"] = health.escalate_after
        body["degraded"] = degraded
        return body

    def degraded(self) -> bool:
        """探针是否该回 503。租约未注入时**不算 degraded**(等价"本副本不跑写者租约")。"""
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
        route for route in app.router.routes if getattr(route, "path", None) != "/metrics"
    ]

    async def metrics_with_writer_health(request: Request):  # noqa: ANN202
        # 抓取即刷新:这些 gauge 没有事件驱动源,只能在被抓时现算(同 Go)。
        holder.snapshot()
        return await pmetrics.metrics_endpoint(request)

    async def writer_health(_request: Request) -> JSONResponse:
        body = holder.snapshot()
        # degraded → 503:让告警把"长期无主"当成真正的不健康,而不是靠人翻 gauge。
        # ★ 它**不接** k8s readiness(见模块头),503 只是给告警的信号。
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
