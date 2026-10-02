"""控制台接口处理器。

每个接口都是对平台的一次调用。**写动作一律走命令网关**
（:class:`cspplant.command.gateway.CommandGateway`）：请求必须带提交标识
（``Idempotency-Key`` 请求头或请求体 ``request_id``），同一标识只执行一次；
状态不允许时按动作策略当场返回 ``409`` 并在响应里带 ``stage``（卡在哪一步）、
``code`` 与原因，或者（清洗）进任务队列返回 ``202``。缺少必填字段或取值非法
时返回 ``400``（请求未被账本声称，可带同一标识更正重提）。

查询接口直接读平台；长动作进度走 ``GET /api/jobs`` / ``GET /api/job/{id}``
轮询（不做推送）。页面不走这里，见 :mod:`cspplant.console.pages`。
"""

from __future__ import annotations

from ..app import Platform
from ..audit.timeline import build_timeline
from ..command.gateway import validate_params
from ..errors import PlantError, RejectedError
from .router import Router


def _as_float(body: dict, key: str, default: float) -> float:
    value = body.get(key, default)
    try:
        return float(value)
    except (TypeError, ValueError) as error:
        raise PlantError(f"参数 {key!r} 必须是数值", component="console") from error


class Api:
    """把 HTTP 请求翻译成平台调用。"""

    def __init__(self, platform: Platform) -> None:
        self._platform = platform
        self._router = Router()
        self._register()

    @property
    def platform(self) -> Platform:
        return self._platform

    def _register(self) -> None:
        add = self._router.add
        add("GET", "/api/status", lambda params, body: self._platform.status())
        add("GET", "/api/snapshot", lambda params, body: self._platform.snapshot())
        add("GET", "/api/topology", self._topology)
        add("GET", "/api/namespaces", self._namespaces)
        add("GET", "/api/zones", lambda params, body: self._platform.zones.describe())
        add("GET", "/api/heliostats", lambda params, body: self._platform.heliostats.describe())
        add("GET", "/api/mirrors", self._mirrors)
        add("GET", "/api/mirror/{name}", self._mirror)
        add("GET", "/api/rules", lambda params, body: {"rules": self._platform.rules.describe()})
        add("GET", "/api/levels", lambda params, body: {"levels": self._platform.alert_levels()})
        add("GET", "/api/latch", self._latch)
        add("GET", "/api/forecast", self._forecast)
        add("GET", "/api/trend", self._trend)
        add("GET", "/api/events", self._events)
        add("GET", "/api/audit", self._audit)
        add("GET", "/api/alarms", self._alarms)
        add("GET", "/api/timeline", self._timeline)
        add("GET", "/api/journal", self._journal)
        add("GET", "/api/store", self._store)
        add("GET", "/api/readiness", self._readiness)
        add("GET", "/api/window", lambda params, body: {"mirrors": self._platform.mirrors_in_window()})
        add("GET", "/api/jobs", self._jobs)
        add("GET", "/api/job/{id}", self._job)
        add("POST", "/api/track", self._track)
        add("POST", "/api/circulate", self._circulate)
        add("POST", "/api/cycle", self._cycle)
        add("POST", "/api/dispatch", self._dispatch)
        add("POST", "/api/changeover", self._changeover)
        add("POST", "/api/pump", self._pump)
        add("POST", "/api/calibrate", self._calibrate)
        add("POST", "/api/manual", self._manual)
        add("POST", "/api/manual/release", self._manual_release)
        add("POST", "/api/manual/expire", self._manual_expire)
        add("POST", "/api/wash", self._wash)
        add("POST", "/api/wash/resume", self._wash_resume)
        add("POST", "/api/trip", self._trip)
        add("POST", "/api/reset", self._reset)
        add("POST", "/api/concurrency", self._concurrency)
        add("POST", "/api/disturb", self._disturb)
        add("POST", "/api/plant/reset-state", lambda params, body: {"removed": self._platform.reset_state()})

    def routes(self) -> list[dict]:
        """返回接口清单，供自检与文档核对。"""

        return [route.as_dict() for route in self._router.routes()]

    def handle(self, method: str, path: str, query: dict, body: dict) -> tuple[int, dict]:
        """处理一次请求，返回状态码与响应体。"""

        matched = self._router.match(method, path)
        if matched is None:
            return 404, {"error": "not_found", "path": path}
        handler, params = matched
        merged = {**query, **params}
        try:
            outcome = handler(merged, body)
        except RejectedError as error:
            # 网关已经把同一口径的拒绝写进审计（command_rejected），这里只翻译成 HTTP。
            return 409, {"error": "RejectedError", **error.as_record()}
        except PlantError as error:
            self._platform.note_error(error)
            return 409, {"error": type(error).__name__, **error.as_record()}
        except (TypeError, ValueError, KeyError) as error:
            message = str(error).strip("'\"") if isinstance(error, KeyError) else str(error)
            return 400, {"error": "bad_request", "message": message}
        # 处理器可以只返回响应体（默认 200），也可以返回 (状态码, 响应体)。
        if isinstance(outcome, tuple):
            return int(outcome[0]), outcome[1]
        return 200, outcome

    # ---- 查询 ---------------------------------------------------------

    def _mirrors(self, params: dict, body: dict) -> dict:
        group = params.get("group")
        mirrors = self._platform.heliostats
        selected = mirrors.in_group(group) if group else mirrors.all()
        drives = {drive.name: drive.snapshot().as_dict() for drive in self._platform.field.drives()}
        return {
            "count": len(selected),
            "mirrors": [{**mirror.as_dict(), "drive": drives[mirror.name]} for mirror in selected],
        }

    def _mirror(self, params: dict, body: dict) -> dict:
        name = params.get("name", "")
        drive = self._platform.field.drive(name)
        return {
            "mirror": self._platform.heliostats.get(name).as_dict(),
            "drive": drive.snapshot().as_dict(),
            "aim": drive.aim_snapshot().as_dict(),
            "target_angles": drive.target_angles().as_dict(),
            "stow_angles": self._platform.field.stow_angles_for(name).as_dict(),
            "rate_limited_steps": drive.rate_limited_steps(),
        }

    def _topology(self, params: dict, body: dict) -> dict:
        topology = self._platform.topology
        payload = topology.summary()
        payload["downstream"] = {stage: topology.downstream(stage) for stage in topology.order()}
        payload["verify"] = topology.verify(set())
        return payload

    def _namespaces(self, params: dict, body: dict) -> dict:
        registry = self._platform.namespaces
        payload = registry.describe()
        payload["by_kind"] = {
            kind: [device.address for device in registry.by_kind(kind)] for kind in registry.kinds()
        }
        return payload

    def _latch(self, params: dict, body: dict) -> dict:
        return self._platform.latch.state().as_dict()

    def _forecast(self, params: dict, body: dict) -> dict:
        horizon = float(params.get("horizon_min", 60.0))
        minute = params.get("minute_of_day")
        return self._platform.forecast(None if minute is None else float(minute), horizon)

    def _trend(self, params: dict, body: dict) -> dict:
        signal = params.get("signal", "receiver_temperature")
        samples = self._platform.trend.series(signal, limit=int(params.get("limit", 60)))
        filled = self._platform.trend.fill_gaps(signal, samples)
        return {
            "signal": signal,
            "period_s": self._platform.trend.period_s,
            "signals": self._platform.trend.signals(),
            "samples": [sample.as_dict() for sample in filled],
        }

    def _events(self, params: dict, body: dict) -> dict:
        topic = params.get("topic")
        return {"events": [event.as_dict() for event in self._platform.bus.history(topic)]}

    def _audit(self, params: dict, body: dict) -> dict:
        limit = int(params.get("limit", 100))
        return {"entries": self._platform.audit.entries(limit=limit)}

    def _alarms(self, params: dict, body: dict) -> dict:
        limit = int(params.get("limit", 20))
        return {
            "recent": self._platform.history.recent(limit),
            "by_rule": self._platform.history.by_rule(),
            "latched": self._platform.latch.state().as_dict(),
        }

    def _timeline(self, params: dict, body: dict) -> dict:
        asset = params.get("asset")
        records = self._platform.audit.records(limit=int(params.get("limit", 200)))
        return {"timeline": build_timeline(records, asset=asset)}

    def _journal(self, params: dict, body: dict) -> dict:
        journal = self._platform.audit.journal
        payload: dict = {
            "topic": journal.topic,
            "count": journal.count(),
            "active_segment": journal.active_segment(),
            "segments": journal.segments(),
        }
        segment = params.get("segment")
        if segment is not None:
            payload["records"] = self._platform.audit.replay_segment(int(segment))
        return payload

    def _store(self, params: dict, body: dict) -> dict:
        return {
            "root": self._platform.store.root,
            "documents": self._platform.store.list_ids(),
            "metrics": self._platform.metrics.summary(),
            "platform_state": self._platform.store.read("platform_state"),
        }

    def _readiness(self, params: dict, body: dict) -> dict:
        platform = self._platform
        checks = {
            "irradiance_durable": platform.field.durable_ready(platform.clock.now()),
            "flow_confirmed": platform.receiver.flow_confirmed(),
            "header_admitted": platform.receiver.header.state() == "admitted",
            "latch_clear": not platform.latch.latched(),
        }
        info = {
            "receiver_state": platform.receiver.state(),
            "temperature_c": round(platform.receiver.output_temperature_c(), 2),
            "net_flow_kg_s": round(platform.salt.flow_kg_s(), 3),
            "last_audit_ts": platform.audit.tail(1)[-1]["ts"] if platform.audit.tail(1) else None,
        }
        return {"ready": all(checks.values()), "checks": checks, "info": info}

    # ---- 动作 ---------------------------------------------------------

    def _submit(self, action: str, body: dict) -> tuple[int, dict]:
        """先解析参数（非法即 400，且不占用提交标识），再交给命令网关。"""

        params = validate_params(action, body, platform=self._platform, _as_float=_as_float)
        request_id = str(body.get("request_id", ""))
        return self._platform.commands.submit(
            action, params, request_id, operator=str(body.get("operator", "console"))
        )

    def _track(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("track", body)

    def _circulate(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("circulate", body)

    def _cycle(self, params: dict, body: dict) -> dict:
        # 控制周期是平台自循环，不是操作员动作，不参与幂等/排队。
        return self._platform.cycle(_as_float(body, "dt", 1.0))

    def _dispatch(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("dispatch", body)

    def _changeover(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("changeover", body)

    def _calibrate(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("calibrate", body)

    def _manual(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("manual", body)

    def _manual_release(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("manual_release", body)

    def _manual_expire(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("manual_expire", body)

    def _wash(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("wash", body)

    def _wash_resume(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._platform.commands.resume_job(str(body["job_id"]), str(body.get("request_id", "")))

    def _trip(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("trip", body)

    def _reset(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("reset", body)

    def _concurrency(self, params: dict, body: dict) -> dict:
        # 并发探测是自检工具，不是现场动作，直接执行。
        return self._platform.concurrency_probe(rounds=int(body.get("rounds", 24)))

    def _pump(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("pump", body)

    def _disturb(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("disturb", body)

    # ---- 长动作任务查询（轮询，不做推送）------------------------------

    def _jobs(self, params: dict, body: dict) -> dict:
        limit = int(params.get("limit", 20))
        action = params.get("action")
        return {"jobs": self._platform.commands.list_jobs(limit=limit, action=action)}

    def _job(self, params: dict, body: dict) -> tuple[int, dict]:
        job = self._platform.commands.get_job(str(params.get("id", "")))
        if job is None:
            return 404, {"error": "not_found", "job_id": params.get("id")}
        return 200, job
