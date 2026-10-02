"""命令网关。

控制台的每个写动作都从这里过，统一做四件事：

1. **幂等**：请求必须带提交标识（``Idempotency-Key`` 或请求体 ``request_id``）。
   同一标识 + 同一动作 + 同一参数指纹在账本里只执行一次；并发重复提交阻塞等
   首个提交出结果后回放，绝不执行第二遍（见 :mod:`cspplant.command.ledger`）。
2. **状态机门前置检查**：执行前先只读检查。策略为 ``reject`` 的动作状态不满足
   当场 409，``stage`` / ``code`` / ``message`` 说清卡在哪一步；策略为
   ``queue`` 的清洗动作进入任务队列（见 :mod:`cspplant.command.policy`）。
3. **长动作任务化**：清洗在后台线程逐面推进，进度与单面结果落任务表并原子
   落盘，控制台**按定死的轮询方式**（``GET /api/jobs/{id}``）查看；中断翻成
   ``interrupted`` 部分失败，可用新提交标识续跑剩余镜子。
4. **口径一致**：拒绝、排队、进度、续跑、终态都通过事件总线落审计，字段与
   接口响应一一对应。

同步动作的响应码：成功 ``200``；清洗已排队 ``202``；状态不允许 ``409``。
"""

from __future__ import annotations

import json
import threading
from typing import TYPE_CHECKING

from ..errors import PlantError, RejectedError
from ..eventbus import Event
from ..sequence.washing import close_wash_window, open_wash_window, wash_one_mirror
from .jobs import (
    FAILED,
    INTERRUPTED,
    QUEUED,
    REJECTED as JOB_REJECTED,
    RUNNING,
    SUCCEEDED,
    JobHandle,
    JobStore,
)
from .ledger import REJECTED as LEDGER_REJECTED
from .ledger import SUCCEEDED as LEDGER_SUCCEEDED
from .ledger import IdempotencyLedger
from .policy import POLICIES, QUEUE, check_precondition, policy_for

if TYPE_CHECKING:
    from ..app import Platform

# 参数指纹只取这些键，使"同一动作的同一意图"判定不受无关字段影响。
_FINGERPRINT_KEYS: dict[str, tuple[str, ...]] = {
    "track": ("dni_w_m2", "minute_of_day"),
    "circulate": ("target_flow_kg_s",),
    "changeover": (),
    "pump": ("action",),
    "calibrate": ("measured_kg_s",),
    "manual": ("mirror", "azimuth_deg", "elevation_deg", "hold_s"),
    "manual_release": ("mirror",),
    "manual_expire": (),
    "wash": ("mirrors", "zone"),
    "trip": ("reason",),
    "reset": ("operator",),
    "dispatch": ("pct",),
    "disturb": ("kind", "value"),
}


def fingerprint(action: str, params: dict) -> str:
    """计算动作参数的稳定指纹（排序键 + 紧凑 JSON）。"""

    keys = _FINGERPRINT_KEYS.get(action, tuple(sorted(params)))
    picked = {key: params.get(key) for key in keys}
    return json.dumps(picked, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def validate_params(action: str, raw: dict, *, platform: "Platform", _as_float=None) -> dict:
    """把请求体解析成动作参数。

    参数非法时抛 ``ValueError``（HTTP 400）。这一步发生在幂等账本声称**之前**：
    畸形请求没有产生任何现场副作用、也没占用提交标识，调用方带同一标识更正后
    可以重提。``_as_float`` 仅用于复用控制台的浮点解析错误文案。
    """

    def as_float(key: str, default: float) -> float:
        if _as_float is not None:
            return _as_float(raw, key, default)
        value = raw.get(key, default)
        return float(value)

    if action == "track":
        minute = raw.get("minute_of_day")
        return {
            "dni_w_m2": as_float("dni_w_m2", platform.field.meter.current()),
            "minute_of_day": None if minute is None else float(minute),
        }
    if action == "circulate":
        target = raw.get("target_flow_kg_s")
        return {"target_flow_kg_s": None if target is None else float(target)}
    if action == "changeover":
        return {}
    if action == "pump":
        pump_action = str(raw.get("action", "cold_start"))
        if pump_action not in ("cold_start", "cold_stop", "hot_start", "hot_stop"):
            raise ValueError(f"未知的泵操作: {pump_action}")
        return {"action": pump_action}
    if action == "calibrate":
        return {
            "measured_kg_s": as_float("measured_kg_s", platform.salt.limits.flow_baseline_kg_s),
            "operator": str(raw.get("operator", "console")),
        }
    if action == "manual":
        return {
            "mirror": str(raw.get("mirror", "m021")),
            "azimuth_deg": as_float("azimuth_deg", 180.0),
            "elevation_deg": as_float("elevation_deg", 35.0),
            "operator": str(raw.get("operator", "console")),
            "hold_s": None if raw.get("hold_s") is None else float(raw["hold_s"]),
        }
    if action == "manual_release":
        return {"mirror": str(raw.get("mirror", "m021"))}
    if action == "manual_expire":
        return {}
    if action == "wash":
        mirrors = raw.get("mirrors")
        # 保留"显式空名单"（[]）与"没给名单"（None）的区别：前者当场拒绝，
        # 后者由网关按清扫计划取默认批次。
        if mirrors is None:
            batch = None
        elif isinstance(mirrors, list):
            batch = [str(item) for item in mirrors]
        else:
            raise ValueError("mirrors 必须是数组")
        return {"mirrors": batch, "zone": str(raw.get("zone", "maint-north"))}
    if action == "trip":
        return {"reason": str(raw.get("reason", "overtemperature"))}
    if action == "reset":
        return {"operator": str(raw.get("operator", "shift-lead"))}
    if action == "dispatch":
        return {
            "pct": as_float("pct", 0.0),
            "writer": str(raw.get("writer", "console")),
        }
    if action == "disturb":
        kind = str(raw.get("kind", "steam"))
        if kind not in ("steam", "salt"):
            raise ValueError(f"未知扰动类型: {kind}")
        return {"kind": kind, "value": as_float("value", 0.2 if kind == "salt" else 0.0)}
    raise ValueError(f"未知动作: {action}")


class CommandGateway:
    """提交动作、查询任务、保证幂等与统一拒绝口径。"""

    def __init__(self, platform: "Platform") -> None:
        self._platform = platform
        self._clock = platform.clock
        self._bus = platform.bus
        self.ledger = IdempotencyLedger(platform.store)
        self.jobs = JobStore(platform.store, platform.clock)
        self._wake = threading.Condition()
        self._worker = threading.Thread(target=self._run_queue, name="csp-command-worker", daemon=True)
        self._worker.start()

    # ---- 提交 ---------------------------------------------------------

    def submit(
        self,
        action: str,
        params: dict,
        request_id: str,
        *,
        operator: str = "console",
    ) -> tuple[int, dict]:
        """提交一个动作，返回 ``(HTTP 状态码, 响应体)``。"""

        if action not in POLICIES:
            error = RejectedError(f"未知动作: {action}", action=action, stage="dispatch", code="unknown_action")
            self._audit_rejection(request_id, error)
            raise error
        if not request_id:
            error = RejectedError(
                "缺少提交标识（Idempotency-Key 或请求体 request_id）",
                action=action,
                stage="dispatch",
                code="missing_request_id",
            )
            self._audit_rejection(request_id, error)
            raise error

        params = dict(params)
        try:
            entry = self.ledger.claim(request_id, action, fingerprint(action, params))
        except RejectedError as error:
            # 账本层面的拒绝（标识冲突 / 前序中断结果未知）同样落审计。
            self._audit_rejection(request_id, error)
            raise
        if entry.replay:
            self._bus.publish(
                Event.of(
                    "command", "command", "command_replayed",
                    severity="notice", op_id=request_id, command_name=action,
                    message=f"{action} 重复提交命中幂等，回放首次结果",
                )
            )
            if entry.state == LEDGER_REJECTED:
                return entry.status_code or 409, {"error": "RejectedError", **(entry.rejection or {})}
            return entry.status_code or 200, dict(entry.payload or {})

        try:
            status, payload = self._dispatch_claimed(action, params, request_id, operator)
        except RejectedError as error:
            self._reject(entry, error)
            raise
        except PlantError as error:
            # 组件在执行中抛出的顺序/联锁异常，统一翻译成同口径的当场拒绝。
            rejected = RejectedError(
                error.message,
                component=error.component,
                action=action,
                stage="execution",
                code=type(error).__name__,
            )
            self._reject(entry, rejected)
            raise rejected from error
        except (TypeError, ValueError):
            # 参数/取值非法（HTTP 400）：现场未产生副作用，释放声称供调用方更正重提。
            self.ledger.release(entry)
            raise
        self.ledger.complete(entry, status, payload)
        return status, payload

    def _dispatch_claimed(
        self, action: str, params: dict, request_id: str, operator: str
    ) -> tuple[int, dict]:
        """账本声称完成后的实际分派（调用方已持有声称）。"""

        verdict = check_precondition(action, self._platform, params)
        if not verdict.allowed:
            raise RejectedError(
                verdict.message,
                component="interlock",
                action=action,
                stage=verdict.stage,
                code=verdict.code,
                blocked_by=verdict.blocked_by,
            )

        policy = policy_for(action)
        if policy.mode == QUEUE:
            if action == "wash":
                # 显式给了空名单属于拒绝（区别于"不给名单 = 按计划取第一批"）。
                if params.get("mirrors") == []:
                    raise RejectedError(
                        "清洗批次不能为空",
                        action=action,
                        stage="batch",
                        code="empty_batch",
                        blocked_by="field.sweeps",
                    )
                if not params.get("mirrors"):
                    params["mirrors"] = self._platform.field.sweeps.plan(
                        self._platform.heliostats.names()
                    )[0]
            return 202, self._enqueue(action, params, request_id)
        return 200, self._execute_sync(action, params)

    def _execute_sync(self, action: str, params: dict) -> dict:
        """执行一个同步动作（在平台锁内调用平台方法）。"""

        platform = self._platform
        if action == "track":
            minute = params.get("minute_of_day")
            return platform.track(float(params["dni_w_m2"]), None if minute is None else float(minute))
        if action == "circulate":
            target = params.get("target_flow_kg_s")
            return platform.circulate(None if target is None else float(target))
        if action == "changeover":
            return platform.changeover()
        if action == "pump":
            return self._pump(str(params.get("action", "cold_start")))
        if action == "calibrate":
            return platform.calibrate_flow(float(params["measured_kg_s"]), str(params.get("operator", "console")))
        if action == "manual":
            return platform.manual_aim(
                str(params["mirror"]),
                float(params.get("azimuth_deg", 180.0)),
                float(params.get("elevation_deg", 35.0)),
                str(params.get("operator", "console")),
                None if params.get("hold_s") is None else float(params["hold_s"]),
            )
        if action == "manual_release":
            return platform.release_manual(str(params["mirror"]))
        if action == "manual_expire":
            return platform.expire_manual()
        if action == "trip":
            return platform.trip(str(params.get("reason", "overtemperature")))
        if action == "reset":
            return platform.reset_trip(str(params.get("operator", "shift-lead")))
        if action == "dispatch":
            return platform.dispatch(float(params["pct"]), str(params.get("writer", "console")))
        if action == "disturb":
            kind = str(params.get("kind", "steam"))
            if kind == "salt":
                return platform.disturb_salt(float(params.get("value", 0.2)))
            return platform.disturb_steam(float(params.get("value", 0.0)))
        raise RejectedError(f"未知动作: {action}", action=action, stage="dispatch", code="unknown_action")

    def _pump(self, pump_action: str) -> dict:
        salt = self._platform.salt
        if pump_action == "cold_start":
            return {"action": pump_action, "target_flow_kg_s": salt.start_cold()}
        if pump_action == "cold_stop":
            return {"action": pump_action, "target_flow_kg_s": salt.stop_cold()}
        if pump_action == "hot_start":
            return {"action": pump_action, "target_flow_kg_s": salt.start_hot()}
        if pump_action == "hot_stop":
            return {"action": pump_action, "target_flow_kg_s": salt.stop_hot()}
        raise RejectedError(f"未知的泵操作: {pump_action}", action="pump", stage="params", code="unknown_pump_action")

    # ---- 拒绝口径 -----------------------------------------------------

    def _reject(self, entry, error: RejectedError) -> None:
        """把拒绝同时写进幂等账本与审计，保证两处理由一致。"""

        record = {
            "error": "RejectedError",
            **error.as_record(),
        }
        self.ledger.reject(entry, 409, record)
        self._audit_rejection(entry.request_id, error)

    def _audit_rejection(self, request_id: str, error: RejectedError) -> None:
        """把一条拒绝按统一字段口径落审计（账本拒绝与状态拒绝共用）。"""

        self._bus.publish(
            Event.of(
                "command", "command", "command_rejected",
                severity="warning",
                op_id=request_id,
                command_name=error.action,
                stage=error.stage,
                code=error.code,
                blocked_by=error.blocked_by,
                message=error.message,
            )
        )

    # ---- 长动作任务：排队 / 执行 / 续跑 -------------------------------

    def _enqueue(self, action: str, params: dict, request_id: str, *, resumes: str = "",
                 seed: dict | None = None) -> dict:
        job = self.jobs.create(action, request_id, params, resumes=resumes, seed=seed)
        self._bus.publish(
            Event.of(
                "command", "command", "job_queued",
                severity="notice", op_id=request_id, job_id=job.job_id, command_name=action,
                message=f"{action} 已排队",
            )
        )
        with self._wake:
            self._wake.notify()
        self._refresh_queue_positions()
        return {"job_id": job.job_id, **job.as_dict()}

    def resume_job(self, job_id: str, request_id: str) -> tuple[int, dict]:
        """续跑一个部分失败（interrupted）的清洗任务，只重洗未完成的镜子。"""

        def deny(message: str, *, code: str, stage: str = "job", blocked_by: str = ""):
            error = RejectedError(message, action="wash", stage=stage, code=code, blocked_by=blocked_by)
            self._audit_rejection(request_id or job_id, error)
            return error

        if not request_id:
            raise deny("续跑必须带新的提交标识", code="missing_request_id", stage="dispatch")
        previous = self.jobs.get(job_id)
        if previous is None:
            raise deny(f"任务 {job_id} 不存在", code="job_not_found")
        if previous.state != INTERRUPTED:
            raise deny(
                f"任务 {job_id} 状态为 {previous.state}，只有中断的部分失败任务可以续跑",
                code="job_not_resumable",
                blocked_by=job_id,
            )
        done = {str(item.get("mirror")) for item in previous.results if item.get("ok")}
        remaining = [mirror for mirror in self._job_mirrors(previous) if mirror not in done]
        if not remaining:
            raise deny(f"任务 {job_id} 没有未完成的镜子", code="nothing_to_resume")
        params = {**previous.params, "mirrors": remaining}
        entry = self.ledger.claim(request_id, "wash", fingerprint("wash", params))
        if entry.replay:
            if entry.state == LEDGER_REJECTED:
                return entry.status_code or 409, {"error": "RejectedError", **(entry.rejection or {})}
            return entry.status_code or 200, dict(entry.payload or {})
        try:
            status, payload = 202, self._enqueue(
                "wash", params, request_id, resumes=job_id, seed=previous.as_dict()
            )
        except RejectedError:
            self.ledger.release(entry)
            raise
        self.ledger.complete(entry, status, payload)
        return status, payload

    def _run_queue(self) -> None:
        """后台执行器：一次只跑一个清洗任务，门空了自动接下一个。"""

        while True:
            job: JobHandle | None = None
            with self.jobs.lock:
                candidates = self.jobs.live("wash")
                if candidates:
                    job = candidates[0]
            if job is None:
                with self._wake:
                    self._wake.wait(timeout=2.0)
                continue
            self._run_wash(job)

    def _run_wash(self, job: JobHandle) -> None:
        """逐面执行一个清洗任务；任何异常都收敛成任务终态，不炸线程。"""

        platform = self._platform
        try:
            with platform.lock:
                # 取任务瞬间的排队位次只反映"提交时排第几"；开始执行前置为 0。
                self.jobs.update(job, state=RUNNING, queue_position=0, stage="opening_window",
                                 message="正在打开清洗窗口", started_at=self._clock.now(),
                                 current_mirror="")
                self._bus.publish(
                    Event.of(
                        "command", "command", "job_started",
                        severity="notice", op_id=job.request_id, job_id=job.job_id, command_name=job.action,
                        message="清洗任务开始执行",
                    )
                )
                wanted = self._job_mirrors(job)
                # 续跑时上次中断可能把窗口留在打开状态（窗口本身已落盘，重启也会恢复）。
                # 窗口覆盖的是整批镜子；只要待洗镜子都在这个窗口与同一避让区里就复用，
                # 绝不重复开窗口把镜子再切一次姿态。
                active = platform.field.sweeps.active()
                if active is not None and active.zone == job.zone and set(wanted) <= set(active.mirrors):
                    batch = list(active.mirrors)
                    opening = {"steps": [], "violations": [], "mirrors": batch}
                    self.jobs.update(job, stage="reuse_window",
                                     message=f"复用中断前未关闭的清洗窗口 {job.zone}")
                    self._bus.publish(
                        Event.of(
                            "command", "command", "window_reused",
                            severity="notice", op_id=job.request_id, job_id=job.job_id,
                            command_name="wash", zone=job.zone, resumed_from=job.resumes,
                            message="复用上次中断遗留的清洗窗口",
                        )
                    )
                else:
                    try:
                        opening = open_wash_window(platform.field, wanted, job.zone, self._clock.now())
                    except PlantError as error:
                        self._fail_job(job, JOB_REJECTED, stage="opening_window", message=error.message,
                                       code=type(error).__name__, publish=True)
                        return
                    batch = opening["mirrors"]
                    # 新任务按实际批次计数；续跑任务的 total 已继承整批镜数，不能被缩小。
                    if not job.resumes:
                        self.jobs.update(job, total=len(batch))
                self.jobs.update(job, stage="washing",
                                 message=f"开始逐面清洗，共 {job.total or len(batch)} 面")
            window_ok = not opening["violations"]
            failed_at_start = job.failed

            already = {str(item.get("mirror")) for item in job.results if item.get("ok")}
            for mirror in batch:
                if mirror in already:
                    # 上次已洗完的镜子不重复动；计数已在任务创建时继承。
                    continue
                with platform.lock:
                    self.jobs.update(job, current_mirror=mirror,
                                     stage=f"washing:{mirror}", message=f"正在清洗 {mirror}")
                    result = wash_one_mirror(platform.wash_cycle, platform.truck, platform.drain, mirror)
                    self.jobs.append_result(job, result)
                    self._bus.publish(
                        Event.of(
                            "command", "command", "job_progress",
                            severity="notice", op_id=job.request_id, job_id=job.job_id, command_name="wash",
                            mirror=mirror, ok=bool(result["ok"]),
                            processed=job.processed, total=job.total,
                            progress=round(job.processed / job.total, 3) if job.total else 0.0,
                            message=f"{mirror} 清洗{'完成' if result['ok'] else '失败'}",
                        )
                    )

            close_error = ""
            with platform.lock:
                try:
                    close_wash_window(platform.field, self._clock.now())
                except PlantError as error:  # pragma: no cover - 关窗失败属于现场故障
                    close_error = error.message

            new_failures = job.failed - failed_at_start
            if not window_ok or new_failures or close_error:
                violations = list(job.violations)
                violations.extend(opening["violations"])
                if close_error:
                    violations.append(close_error)
                for violation in violations:
                    if violation not in job.violations:
                        job.violations.append(violation)
                self.jobs.persist()
                self._fail_job(
                    job,
                    FAILED,
                    stage="completed_with_errors",
                    message=f"清洗结束但存在失败/未完成项：{', '.join(job.violations) or '未知'}",
                    publish=True,
                )
            else:
                self.jobs.finish(job, SUCCEEDED, stage="done",
                                 message=f"全部 {job.total} 面镜子清洗完成")
                self._publish_finished(job, ok=True)
        except Exception as error:  # 任务线程绝不能因意外异常静默死掉
            self._fail_job(job, FAILED, stage="worker_error", message=f"执行器异常：{error}", publish=True)
        finally:
            self._refresh_queue_positions()
            with self._wake:
                self._wake.notify()

    def _fail_job(self, job: JobHandle, state: str, *, stage: str, message: str,
                  code: str = "", publish: bool = False) -> None:
        self.jobs.finish(job, state, stage=stage, message=message)
        if publish:
            self._publish_finished(job, ok=False, code=code)

    def _publish_finished(self, job: JobHandle, *, ok: bool, code: str = "") -> None:
        self._bus.publish(
            Event.of(
                "command", "command", "job_finished",
                severity="notice" if ok else "warning",
                op_id=job.request_id, job_id=job.job_id, command_name=job.action,
                state=job.state, ok=ok,
                total=job.total, processed=job.processed, succeeded=job.succeeded, failed=job.failed,
                resumed_from=job.resumes,
                code=code, violations=list(job.violations),
                message=job.message,
            )
        )

    def _refresh_queue_positions(self) -> None:
        """重算所有排队任务的位次（队首为 1；执行中/终态为 0）。"""

        with self.jobs.lock:
            changed = False
            queued = [job for job in self.jobs.live("wash") if job.state == QUEUED]
            for position, job in enumerate(queued, start=1):
                if job.queue_position != position:
                    job.queue_position = position
                    changed = True
            if changed:
                self.jobs.persist()

    @staticmethod
    def _job_mirrors(job: JobHandle) -> list[str]:
        mirrors = job.params.get("mirrors")
        if isinstance(mirrors, list) and mirrors:
            return [str(item) for item in mirrors]
        # 未显式给名单时沿用同步清洗的默认取批方式（清扫计划第一批）。
        return []

    # ---- 查询 ---------------------------------------------------------

    def get_job(self, job_id: str) -> dict | None:
        job = self.jobs.get(job_id)
        return None if job is None else job.as_dict()

    def list_jobs(self, *, limit: int = 20, action: str | None = None) -> list[dict]:
        return self.jobs.list(limit=limit, action=action)
