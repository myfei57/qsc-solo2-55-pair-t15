"""控制台接口处理器。

每个接口都是对平台公开方法的一次调用。写接口必须携带 ``Idempotency-Key``
（请求头或请求体字段），同键同动作同参数只执行一次；长动作返回作业并固定走
1 秒轮询，不提供推送。缺少必填字段或取值非法时返回 ``400``，平台联锁异常统一
转成 ``409`` 并在响应里带上组件、``blocked_step`` 与 ``current_state``。页面不走
这里，见 :mod:`cspplant.console.pages`。
"""

from __future__ import annotations

from ..actions.manager import ACTION_POLICIES
from ..app import Platform
from ..audit.timeline import build_timeline
from ..errors import ActionRejected, PlantError
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
        add("GET", "/api/status", lambda params, body, headers=None: self._platform.status())
        add("GET", "/api/snapshot", lambda params, body, headers=None: self._platform.snapshot())
        add("GET", "/api/topology", self._topology)
        add("GET", "/api/namespaces", self._namespaces)
        add("GET", "/api/zones", lambda params, body, headers=None: self._platform.zones.describe())
        add("GET", "/api/heliostats", lambda params, body, headers=None: self._platform.heliostats.describe())
        add("GET", "/api/mirrors", self._mirrors)
        add("GET", "/api/mirror/{name}", self._mirror)
        add("GET", "/api/rules", lambda params, body, headers=None: {"rules": self._platform.rules.describe()})
        add("GET", "/api/levels", lambda params, body, headers=None: {"levels": self._platform.alert_levels()})
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
        add("GET", "/api/window", lambda params, body, headers=None: {"mirrors": self._platform.mirrors_in_window()})
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
        add("GET", "/api/action-policies", lambda params, body, headers=None: {"policies": ACTION_POLICIES})
        add("GET", "/api/wash/jobs", self._wash_jobs)
        add("GET", "/api/wash/jobs/{job_id}", self._wash_job)
        add("POST", "/api/wash/jobs/{job_id}/resume", self._wash_resume)
        add("POST", "/api/wash", self._wash)
        add("POST", "/api/trip", self._trip)
        add("POST", "/api/reset", self._reset)
        add("POST", "/api/concurrency", self._concurrency)
        add("POST", "/api/disturb", self._disturb)
        add("POST", "/api/plant/reset-state", self._reset_state)

    def routes(self) -> list[dict]:
        """返回接口清单，供自检与文档核对。"""

        return [route.as_dict() for route in self._router.routes()]

    def handle(
        self,
        method: str,
        path: str,
        query: dict,
        body: dict,
        headers: dict | None = None,
    ) -> tuple[int, dict]:
        """处理一次请求，返回状态码与响应体。"""

        headers = {str(key).lower(): str(value) for key, value in (headers or {}).items()}
        matched = self._router.match(method, path)
        if matched is None:
            return 404, {"error": "not_found", "path": path}
        handler, params = matched
        merged = {**query, **params}
        try:
            payload = handler(merged, body, headers)
        except ActionRejected as error:
            status = 400 if error.code == "idempotency_key_missing" else 409
            return status, {"error": type(error).__name__, **error.as_record()}
        except PlantError as error:
            self._platform.note_error(error)
            rejected = ActionRejected(
                error.message,
                component=error.component,
                code=type(error).__name__,
                blocked_step="execute",
            )
            return 409, {"error": type(rejected).__name__, **rejected.as_record()}
        except (TypeError, ValueError) as error:
            return 400, {"error": "bad_request", "message": str(error)}
        if isinstance(payload, tuple):
            status, payload = payload
            return status, payload
        return 200, payload

    # ---- 查询 ---------------------------------------------------------

    def _mirrors(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        group = params.get("group")
        mirrors = self._platform.heliostats
        selected = mirrors.in_group(group) if group else mirrors.all()
        drives = {drive.name: drive.snapshot().as_dict() for drive in self._platform.field.drives()}
        return {
            "count": len(selected),
            "mirrors": [{**mirror.as_dict(), "drive": drives[mirror.name]} for mirror in selected],
        }

    def _mirror(self, params: dict, body: dict, headers: dict | None = None) -> dict:
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

    def _topology(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        topology = self._platform.topology
        payload = topology.summary()
        payload["downstream"] = {stage: topology.downstream(stage) for stage in topology.order()}
        payload["verify"] = topology.verify(set())
        return payload

    def _namespaces(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        registry = self._platform.namespaces
        payload = registry.describe()
        payload["by_kind"] = {
            kind: [device.address for device in registry.by_kind(kind)] for kind in registry.kinds()
        }
        return payload

    def _latch(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        return self._platform.latch.state().as_dict()

    def _forecast(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        horizon = float(params.get("horizon_min", 60.0))
        minute = params.get("minute_of_day")
        return self._platform.forecast(None if minute is None else float(minute), horizon)

    def _trend(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        signal = params.get("signal", "receiver_temperature")
        samples = self._platform.trend.series(signal, limit=int(params.get("limit", 60)))
        filled = self._platform.trend.fill_gaps(signal, samples)
        return {
            "signal": signal,
            "period_s": self._platform.trend.period_s,
            "signals": self._platform.trend.signals(),
            "samples": [sample.as_dict() for sample in filled],
        }

    def _events(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        topic = params.get("topic")
        return {"events": [event.as_dict() for event in self._platform.bus.history(topic)]}

    def _audit(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        limit = int(params.get("limit", 100))
        return {"entries": self._platform.audit.entries(limit=limit)}

    def _alarms(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        limit = int(params.get("limit", 20))
        return {
            "recent": self._platform.history.recent(limit),
            "by_rule": self._platform.history.by_rule(),
            "latched": self._platform.latch.state().as_dict(),
        }

    def _timeline(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        asset = params.get("asset")
        records = self._platform.audit.records(limit=int(params.get("limit", 200)))
        return {"timeline": build_timeline(records, asset=asset)}

    def _journal(self, params: dict, body: dict, headers: dict | None = None) -> dict:
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

    def _store(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        return {
            "root": self._platform.store.root,
            "documents": self._platform.store.list_ids(),
            "metrics": self._platform.metrics.summary(),
            "platform_state": self._platform.store.read("platform_state"),
        }

    def _readiness(self, params: dict, body: dict, headers: dict | None = None) -> dict:
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

    @staticmethod
    def _idempotency_key(body: dict, headers: dict) -> str:
        """读取本次提交标识；所有写动作都必须显式携带。"""

        key = headers.get("idempotency-key") or body.pop("idempotency_key", "")
        return str(key)

    def _execute(
        self,
        operation: str,
        body: dict,
        headers: dict,
        action,
        params: dict | None = None,
    ) -> tuple[int, dict]:
        key = self._idempotency_key(body, headers)
        return self._platform.actions.execute(
            operation, key, body if params is None else params, action
        )

    def _track(self, params: dict, body: dict, headers: dict) -> tuple[int, dict]:
        dni = _as_float(body, "dni_w_m2", self._platform.field.meter.current())
        minute = body.get("minute_of_day")
        minute_value = None if minute is None else float(minute)
        return self._execute(
            "track",
            body,
            headers,
            lambda: self._platform.track(dni, minute_value),
            {"dni_w_m2": dni, "minute_of_day": minute_value},
        )

    def _circulate(self, params: dict, body: dict, headers: dict) -> tuple[int, dict]:
        target = body.get("target_flow_kg_s")
        target_value = None if target is None else float(target)
        return self._execute(
            "circulate",
            body,
            headers,
            lambda: self._platform.circulate(target_value),
            {"target_flow_kg_s": target_value},
        )

    def _cycle(self, params: dict, body: dict, headers: dict) -> tuple[int, dict]:
        dt = _as_float(body, "dt", 1.0)
        return self._execute("cycle", body, headers, lambda: self._platform.cycle(dt), {"dt": dt})

    def _dispatch(self, params: dict, body: dict, headers: dict) -> tuple[int, dict]:
        pct = _as_float(body, "pct", 0.0)
        writer = str(body.get("writer", "console"))
        return self._execute(
            "dispatch",
            body,
            headers,
            lambda: self._platform.dispatch(pct, writer),
            {"pct": pct, "writer": writer},
        )

    def _changeover(self, params: dict, body: dict, headers: dict) -> tuple[int, dict]:
        return self._execute("changeover", body, headers, self._platform.changeover, {})

    def _calibrate(self, params: dict, body: dict, headers: dict) -> tuple[int, dict]:
        measured = _as_float(body, "measured_kg_s", self._platform.salt.limits.flow_baseline_kg_s)
        operator = str(body.get("operator", "console"))
        return self._execute(
            "calibrate",
            body,
            headers,
            lambda: self._platform.calibrate_flow(measured, operator),
            {"measured_kg_s": measured, "operator": operator},
        )

    def _manual(self, params: dict, body: dict, headers: dict) -> tuple[int, dict]:
        mirror = str(body.get("mirror", "m021"))
        azimuth = _as_float(body, "azimuth_deg", 180.0)
        elevation = _as_float(body, "elevation_deg", 35.0)
        operator = str(body.get("operator", "console"))
        hold = None if body.get("hold_s") is None else float(body["hold_s"])
        return self._execute(
            "manual",
            body,
            headers,
            lambda: self._platform.manual_aim(mirror, azimuth, elevation, operator, hold),
            {
                "mirror": mirror,
                "azimuth_deg": azimuth,
                "elevation_deg": elevation,
                "operator": operator,
                "hold_s": hold,
            },
        )

    def _manual_release(self, params: dict, body: dict, headers: dict) -> tuple[int, dict]:
        mirror = str(body.get("mirror", "m021"))
        return self._execute(
            "manual_release",
            body,
            headers,
            lambda: self._platform.release_manual(mirror),
            {"mirror": mirror},
        )

    def _manual_expire(self, params: dict, body: dict, headers: dict) -> tuple[int, dict]:
        return self._execute("manual_expire", body, headers, self._platform.expire_manual, {})

    def _wash_jobs(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        return {"jobs": self._platform.wash_jobs(limit=int(params.get("limit", 10)))}

    def _wash_job(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        return self._platform.wash_job(str(params["job_id"]))

    def _wash_resume(self, params: dict, body: dict, headers: dict) -> tuple[int, dict]:
        key = self._idempotency_key(body, headers)
        snapshot, first = self._platform.resume_wash(str(params["job_id"]), key)
        return 202 if first else 200, snapshot

    def _wash(self, params: dict, body: dict, headers: dict) -> tuple[int, dict]:
        mirrors = body.get("mirrors")
        batch = None if not mirrors else [str(item) for item in mirrors]
        zone = str(body.get("zone", "maint-north"))
        key = self._idempotency_key(body, headers)
        snapshot, first = self._platform.submit_wash(key, batch, zone)
        return 202 if first else 200, snapshot

    def _trip(self, params: dict, body: dict, headers: dict) -> tuple[int, dict]:
        reason = str(body.get("reason", "overtemperature"))
        return self._execute("trip", body, headers, lambda: self._platform.trip(reason), {"reason": reason})

    def _reset(self, params: dict, body: dict, headers: dict) -> tuple[int, dict]:
        operator = str(body.get("operator", "shift-lead"))
        return self._execute(
            "reset_trip",
            body,
            headers,
            lambda: self._platform.reset_trip(operator),
            {"operator": operator},
        )

    def _concurrency(self, params: dict, body: dict, headers: dict) -> tuple[int, dict]:
        rounds = int(body.get("rounds", 24))
        return self._execute(
            "concurrency",
            body,
            headers,
            lambda: self._platform.concurrency_probe(rounds=rounds),
            {"rounds": rounds},
        )

    def _pump(self, params: dict, body: dict, headers: dict) -> tuple[int, dict]:
        action = str(body.get("action", "cold_start"))
        salt = self._platform.salt
        operations = {
            "cold_start": salt.start_cold,
            "cold_stop": salt.stop_cold,
            "hot_start": salt.start_hot,
            "hot_stop": salt.stop_hot,
        }
        if action not in operations:
            raise ActionRejected(
                f"未知的泵操作: {action}",
                component="console",
                code="unknown_pump_action",
                blocked_step="validate_action",
            )
        return self._execute(
            f"pump_{action}",
            body,
            headers,
            lambda: {"action": action, "target_flow_kg_s": operations[action]()},
            {"action": action},
        )

    def _disturb(self, params: dict, body: dict, headers: dict) -> tuple[int, dict]:
        kind = str(body.get("kind", "steam"))
        if kind == "salt":
            value = _as_float(body, "value", 0.2)
            return self._execute(
                "disturb_salt",
                body,
                headers,
                lambda: self._platform.disturb_salt(value),
                {"kind": kind, "value": value},
            )
        value = _as_float(body, "value", 0.0)
        return self._execute(
            "disturb_steam",
            body,
            headers,
            lambda: self._platform.disturb_steam(value),
            {"kind": kind, "value": value},
        )

    def _reset_state(self, params: dict, body: dict, headers: dict) -> tuple[int, dict]:
        return self._execute(
            "reset_state",
            body,
            headers,
            lambda: {"removed": self._platform.reset_state()},
            {},
        )
