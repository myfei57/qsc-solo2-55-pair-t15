"""动作幂等键与长任务执行。

控制台与后台共用同一套动作入口：相同幂等键只会被执行一次；同步动作把第一次
的响应或拒绝原样返回给重试请求，长动作则返回同一个作业。长动作按固定的轮询
协议暴露状态，进度、拒绝原因和作业返回的审计引用来自同一个作业快照。
"""

from __future__ import annotations

import copy
import hashlib
import json
import threading
import time
import uuid
from typing import Callable

from ..audit.record import AuditRecord
from ..errors import ActionRejected, PlantError
from ..store.jsonfile import JsonStore

QUEUED = "QUEUED"
RUNNING = "RUNNING"
PARTIALLY_FAILED = "PARTIALLY_FAILED"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
REJECTED = "REJECTED"

ACTIVE_STATES = (QUEUED, RUNNING, PARTIALLY_FAILED)
NON_TERMINAL_STATES = (QUEUED, RUNNING, PARTIALLY_FAILED)
TERMINAL_STATES = (SUCCEEDED, FAILED, REJECTED)

LongRunner = Callable[["ActionManager", dict, Callable[..., dict]], dict]

POLLING_POLICY = {"method": "GET", "interval_ms": 1000, "push": False}
ACTION_POLICIES = {
    "synchronous_actions": {
        "state_not_allowed": "reject_immediately",
        "queue": False,
        "progress": "return_first_response",
    },
    "wash": {
        "state_not_allowed": "reject_immediately",
        "queue": False,
        "partial_failure": "keep_job_and_resume_with_new_idempotency_key",
        "progress": "poll",
        "polling": POLLING_POLICY,
    },
}


def canonical_json(payload: object) -> str:
    """生成稳定的参数指纹，避免键顺序影响幂等判断。"""

    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class ActionManager:
    """幂等请求登记表、长任务队列表和后台执行器。"""

    def __init__(self, platform: object, store: JsonStore, *, tick_s: float = 0.05) -> None:
        self._platform = platform
        self._store = store
        self._tick_s = max(0.0, float(tick_s))
        self._lock = threading.RLock()
        self._wakeup = threading.Event()
        self._requests: dict[str, dict] = self._load("action_requests", {})
        self._jobs: dict[str, dict] = self._load("action_jobs", {})
        self._events: dict[str, threading.Event] = {}
        self._runners: dict[str, LongRunner] = {}
        self._stopping = False
        self._ready = False
        self._thread = threading.Thread(target=self._run_queue, name="action-worker", daemon=True)

    def mark_ready(self) -> None:
        """平台装配完成后再启动后台队列，避免恢复任务访问半初始化组件。"""

        if not self._ready:
            self._ready = True
            self._wakeup.set()

    def start_worker(self) -> None:
        if not self._thread.is_alive():
            self._thread.start()
        self.mark_ready()

    @property
    def tick_s(self) -> float:
        return self._tick_s

    def policies(self) -> dict:
        """返回按动作定死的拒绝／排队与进度协议。"""

        return copy.deepcopy(ACTION_POLICIES)

    def register_runner(self, operation: str, runner: LongRunner) -> None:
        """登记一个长动作执行器。"""

        self._runners[operation] = runner

    def stop(self) -> None:
        self._stopping = True
        self._wakeup.set()

    def _load(self, doc_id: str, default: object) -> object:
        raw = self._store.read(doc_id)
        return raw if isinstance(raw, type(default)) else copy.deepcopy(default)

    def _persist_requests(self) -> None:
        self._store.write("action_requests", self._requests)

    def _persist_jobs(self) -> None:
        self._store.write("action_jobs", self._jobs)

    def execute(
        self,
        operation: str,
        key: str,
        params: dict,
        action: Callable[[], dict],
        *,
        http_status: int = 200,
    ) -> tuple[int, dict]:
        """执行同步动作；同一 ``key`` 有且只有一次实际执行。"""

        if not key:
            raise ActionRejected(
                "写动作必须携带 Idempotency-Key",
                component="console",
                code="idempotency_key_missing",
                blocked_step="request_validation",
            )
        fingerprint = hashlib.sha256(canonical_json(params).encode("utf-8")).hexdigest()
        with self._lock:
            existing = self._requests.get(key)
            if existing is not None:
                if existing.get("operation") != operation or existing.get("fingerprint") != fingerprint:
                    raise ActionRejected(
                        "幂等键已用于另一个动作或不同参数",
                        component="console",
                        code="idempotency_key_conflict",
                        blocked_step="idempotency_check",
                        current_state={"existing_operation": existing.get("operation")},
                    )
                if existing.get("state") == "RUNNING":
                    event = self._events.setdefault(key, threading.Event())
                else:
                    event = None
            else:
                event = threading.Event()
                self._events[key] = event
                self._requests[key] = {
                    "key": key,
                    "operation": operation,
                    "fingerprint": fingerprint,
                    "state": "RUNNING",
                    "http_status": 202,
                    "payload": {},
                    "created_at": time.time(),
                }
                self._persist_requests()

        if existing is not None:
            if event is not None:
                event.wait(timeout=30.0)
            with self._lock:
                completed = self._requests[key]
            payload = copy.deepcopy(completed.get("payload", {}))
            payload["idempotency_key"] = key
            payload["idempotent_replay"] = True
            return int(completed.get("http_status", 200)), payload

        try:
            payload = action()
            if isinstance(payload, dict) and payload.get("ok") is False:
                violations = payload.get("violations", [])
                raise ActionRejected(
                    f"动作被联锁拒绝：{', '.join(str(item) for item in violations) or 'unknown'}",
                    component=str(payload.get("component", "plant")),
                    code=str(violations[0] if violations else "interlock_rejected"),
                    blocked_step="execute",
                    current_state={"violations": violations},
                    violations=list(violations),
                )
        except ActionRejected as error:
            return self._reject_sync(key, error, 409)
        except PlantError as error:
            rejected = ActionRejected(
                error.message,
                component=error.component,
                code=type(error).__name__,
                blocked_step="execute",
            )
            return self._reject_sync(key, rejected, 409)

        with self._lock:
            record = self._requests[key]
            result = dict(payload) if isinstance(payload, dict) else {"value": payload}
            result["idempotency_key"] = key
            result["idempotent_replay"] = False
            record.update(
                state="SUCCEEDED", http_status=http_status, payload=result, completed_at=time.time()
            )
            self._persist_requests()
            event.set()
            return http_status, copy.deepcopy(result)

    def _reject_sync(self, key: str, error: ActionRejected, http_status: int) -> tuple[int, dict]:
        payload = {"error": type(error).__name__, **error.as_record()}
        with self._lock:
            record = self._requests[key]
            record.update(
                state="REJECTED",
                http_status=http_status,
                payload=payload,
                completed_at=time.time(),
            )
            self._persist_requests()
            event = self._events.get(key)
        platform = self._platform
        platform.audit.record(  # type: ignore[attr-defined]
            AuditRecord.create(
                platform.clock.now(),  # type: ignore[attr-defined]
                "",
                error.component,
                "action_rejected",
                severity="alarm",
                message=error.message,
                idempotency_key=key,
                code=error.code,
                blocked_step=error.blocked_step,
                current_state=error.current_state,
            )
        )
        if event is not None:
            event.set()
        return http_status, copy.deepcopy(payload)

    def prepare_long(
        self,
        operation: str,
        key: str,
        params: dict,
        *,
        queue_policy: str = "reject_when_busy",
    ) -> dict:
        """在调用方的状态锁内预分配作业，避免先登记后校验产生幻作业。"""

        if not key:
            raise ActionRejected(
                "写动作必须携带 Idempotency-Key",
                component="console",
                code="idempotency_key_missing",
                blocked_step="request_validation",
            )
        if operation not in self._runners:
            raise ActionRejected(
                f"未知长动作: {operation}",
                component="action",
                code="unknown_long_operation",
                blocked_step="route",
            )
        fingerprint = hashlib.sha256(canonical_json(params).encode("utf-8")).hexdigest()
        with self._lock:
            if key in self._requests:
                existing = self._requests[key]
                if existing.get("operation") != operation or existing.get("fingerprint") != fingerprint:
                    raise ActionRejected(
                        "幂等键已用于另一个动作或不同参数",
                        component="console",
                        code="idempotency_key_conflict",
                        blocked_step="idempotency_check",
                        current_state={"existing_operation": existing.get("operation")},
                    )
                job_id = existing.get("job_id")
                if job_id:
                    return self.snapshot(str(job_id))

            current = [job for job in self._jobs.values() if job["operation"] == operation]
            current.sort(key=lambda item: item["created_at"], reverse=True)
            latest = current[0] if current else None
            if latest is not None and latest["status"] in NON_TERMINAL_STATES:
                raise ActionRejected(
                    f"{operation} 已有未结束作业"
                    if latest["status"] != PARTIALLY_FAILED
                    else f"{operation} 存在部分失败作业，必须先续作",
                    component="action",
                    code="operation_busy" if latest["status"] != PARTIALLY_FAILED else "operation_resume_required",
                    blocked_step="admit",
                    current_state={"job_id": latest["id"], "status": latest["status"]},
                )
            now = time.time()
            return {
                "id": f"job-{uuid.uuid4().hex[:16]}",
                "idempotency_key": key,
                "operation": operation,
                "params": copy.deepcopy(params),
                "queue_policy": queue_policy,
                "status": QUEUED,
                "progress_pct": 0.0,
                "phase": "queued",
                "current_step": "等待执行",
                "blocked_step": "",
                "checkpoint": {},
                "result": None,
                "error": None,
                "attempts": 0,
                "audit_refs": [],
                "created_at": now,
                "updated_at": now,
                "started_at": None,
                "finished_at": None,
            }

    def submit_long(
        self,
        operation: str,
        key: str,
        params: dict,
        *,
        queue_policy: str = "reject_when_busy",
        prepared_job: dict | None = None,
    ) -> tuple[dict, bool]:
        """登记长动作。返回作业快照和是否为首次提交。"""

        if not key:
            raise ActionRejected(
                "写动作必须携带 Idempotency-Key",
                component="console",
                code="idempotency_key_missing",
                blocked_step="request_validation",
            )
        if operation not in self._runners:
            raise ActionRejected(
                f"未知长动作: {operation}",
                component="action",
                code="unknown_long_operation",
                blocked_step="route",
            )
        fingerprint = hashlib.sha256(canonical_json(params).encode("utf-8")).hexdigest()
        with self._lock:
            request = self._requests.get(key)
            if request is not None:
                if request.get("operation") != operation or request.get("fingerprint") != fingerprint:
                    raise ActionRejected(
                        "幂等键已用于另一个动作或不同参数",
                        component="console",
                        code="idempotency_key_conflict",
                        blocked_step="idempotency_check",
                        current_state={"existing_operation": request.get("operation")},
                    )
                job_id = request.get("job_id")
                if job_id:
                    snapshot = self.snapshot(str(job_id))
                    snapshot["idempotent_replay"] = True
                    return snapshot, False
                payload = copy.deepcopy(request.get("payload", {}))
                payload["idempotency_key"] = key
                payload["idempotent_replay"] = True
                return payload, False

            if prepared_job is not None:
                job = copy.deepcopy(prepared_job)
                if job.get("operation") != operation or job.get("idempotency_key") != key:
                    raise ActionRejected(
                        "长动作作业预登记内容与提交请求不一致",
                        component="action",
                        code="prepared_job_mismatch",
                        blocked_step="idempotency_check",
                    )
                if hashlib.sha256(canonical_json(job.get("params")).encode()).hexdigest() != fingerprint:
                    raise ActionRejected(
                        "长动作作业参数指纹不一致",
                        component="action",
                        code="prepared_params_mismatch",
                        blocked_step="idempotency_check",
                    )
            else:
                current = [job for job in self._jobs.values() if job["operation"] == operation]
                current.sort(key=lambda item: item["created_at"], reverse=True)
                latest = current[0] if current else None
                if (
                    latest is not None
                    and latest["status"] in NON_TERMINAL_STATES
                    and not (latest["status"] == PARTIALLY_FAILED and queue_policy == "resume_partial")
                ):
                    raise ActionRejected(
                        f"{operation} 已有未结束作业"
                        if latest["status"] != PARTIALLY_FAILED
                        else f"{operation} 存在部分失败作业，必须先续作",
                        component="action",
                        code="operation_busy" if latest["status"] != PARTIALLY_FAILED else "operation_resume_required",
                        blocked_step="admit",
                        current_state={"job_id": latest["id"], "status": latest["status"]},
                    )

                now = time.time()
                job = {
                    "id": f"job-{uuid.uuid4().hex[:16]}",
                    "idempotency_key": key,
                    "operation": operation,
                    "params": copy.deepcopy(params),
                    "queue_policy": queue_policy,
                    "status": QUEUED,
                    "progress_pct": 0.0,
                    "phase": "queued",
                    "current_step": "等待执行",
                    "blocked_step": "",
                    "checkpoint": {},
                    "result": None,
                    "error": None,
                    "attempts": 0,
                    "audit_refs": [],
                    "created_at": now,
                    "updated_at": now,
                    "started_at": None,
                    "finished_at": None,
                }
            self._jobs[job["id"]] = job
            self._requests[key] = {
                "key": key,
                "operation": operation,
                "fingerprint": fingerprint,
                "state": QUEUED,
                "job_id": job["id"],
                "payload": {},
                "created_at": time.time(),
            }
            self._persist_jobs()
            self._persist_requests()
            snapshot = self.snapshot(job["id"])
            self._wakeup.set()
            return snapshot, True

    def resume(self, job_id: str, key: str) -> tuple[dict, bool]:
        """把部分失败的长动作重新放回队列。"""

        if not key:
            raise ActionRejected(
                "续作必须携带新的 Idempotency-Key",
                component="console",
                code="idempotency_key_missing",
                blocked_step="request_validation",
            )
        with self._lock:
            existing = self._requests.get(key)
            if existing is not None:
                existing_job_id = existing.get("job_id")
                if not existing_job_id or existing_job_id not in self._jobs:
                    raise ActionRejected(
                        "续作请求缺少有效作业",
                        component="action",
                        code="job_not_found",
                        blocked_step="idempotency_check",
                    )
                existing_job = self._jobs[existing_job_id]
                if existing.get("operation") != f"{existing_job['operation']}.resume" or existing_job_id != job_id:
                    raise ActionRejected(
                        "幂等键已用于另一个动作",
                        component="console",
                        code="idempotency_key_conflict",
                        blocked_step="idempotency_check",
                    )
                snapshot = self.snapshot(job_id)
                snapshot["idempotent_replay"] = True
                return snapshot, False
            if job_id not in self._jobs:
                raise ActionRejected(
                    "作业不存在", component="action", code="job_not_found", blocked_step="lookup"
                )
            job = self._jobs[job_id]
            if job["status"] != PARTIALLY_FAILED:
                raise ActionRejected(
                    "只有部分失败的作业可以续作",
                    component="action",
                    code="job_not_resumable",
                    blocked_step="resume_check",
                    current_state={"status": job["status"]},
                )
            fingerprint = hashlib.sha256(canonical_json({"job_id": job_id}).encode()).hexdigest()
            self._requests[key] = {
                "key": key,
                "operation": f"{job['operation']}.resume",
                "fingerprint": fingerprint,
                "state": QUEUED,
                "job_id": job_id,
                "payload": {},
                "created_at": time.time(),
            }
            job["status"] = QUEUED
            job["phase"] = "resuming"
            job["current_step"] = "等待从检查点续作"
            job["blocked_step"] = ""
            job["error"] = None
            job["updated_at"] = time.time()
            self._persist_jobs()
            self._persist_requests()
            snapshot = self.snapshot(job_id)
            self._wakeup.set()
            return snapshot, True

    def recover_interrupted(self) -> None:
        """进程重启后，把未完成作业降级成可续作的部分失败。"""

        with self._lock:
            changed = False
            for job in self._jobs.values():
                if job["status"] == RUNNING:
                    job["status"] = PARTIALLY_FAILED
                    phase = job.get("phase", "")
                    job["phase"] = "interrupted"
                    job["current_step"] = "执行进程中断，等待续作"
                    job["blocked_step"] = phase
                    job["error"] = {
                        "code": "WORKER_INTERRUPTED",
                        "message": "长动作在非终态中断，已保留检查点",
                    }
                    job["updated_at"] = time.time()
                    changed = True
            for record in self._requests.values():
                if record.get("state") == "RUNNING":
                    record["state"] = "REJECTED"
                    record["http_status"] = 409
                    record["payload"] = {
                        "error": "ActionRejected",
                        "type": "ActionRejected",
                        "component": "action",
                        "message": "同步动作在执行中断，结果未知，需按新幂等键重新提交",
                        "code": "SYNC_WORKER_INTERRUPTED",
                        "blocked_step": record.get("operation", "execute"),
                        "current_state": {},
                        "details": {},
                    }
                    record["completed_at"] = time.time()
                    changed = True
            if changed:
                self._persist_jobs()
                self._persist_requests()

    def list_jobs(self, operation: str | None = None, limit: int = 20) -> list[dict]:
        with self._lock:
            jobs = [job for job in self._jobs.values() if operation is None or job["operation"] == operation]
            jobs.sort(key=lambda item: item["created_at"], reverse=True)
            return [self._snapshot_locked(job) for job in jobs[: max(0, limit)]]

    def snapshot(self, job_id: str) -> dict:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise ActionRejected(
                    "作业不存在", component="action", code="job_not_found", blocked_step="lookup"
                )
            return self._snapshot_locked(job)

    def _snapshot_locked(self, job: dict) -> dict:
        payload = copy.deepcopy(job)
        if job["status"] == QUEUED:
            queued = [
                item
                for item in self._jobs.values()
                if item["status"] == QUEUED and item["created_at"] <= job["created_at"]
            ]
            active = any(item["status"] == RUNNING for item in self._jobs.values())
            payload["queue_position"] = len(queued) + (1 if active else 0)
        else:
            payload["queue_position"] = 0
        payload["resumable"] = job["status"] == PARTIALLY_FAILED
        payload["polling"] = copy.deepcopy(POLLING_POLICY)
        return payload

    def update_job(self, job_id: str, **changes: object) -> dict:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise ActionRejected(
                    "作业不存在", component="action", code="job_not_found", blocked_step="update"
                )
            job.update(changes)
            job["updated_at"] = time.time()
            self._persist_jobs()
            return self._snapshot_locked(job)

    def append_audit(self, job_id: str, entry: dict) -> None:
        with self._lock:
            job = self._jobs[job_id]
            ref = {
                "ts": entry.get("ts"),
                "component": entry.get("component"),
                "action": entry.get("action"),
                "severity": entry.get("severity"),
                "message": entry.get("message"),
            }
            job["audit_refs"].append(ref)
            job["updated_at"] = time.time()
            self._persist_jobs()

    def audit(self, job: dict, component: str, action: str, severity: str, message: str, **payload: object) -> None:
        """发布审计并把同一条记录引用到作业，保证两边口径一致。"""

        platform = self._platform
        entry = platform.audit.record(
            AuditRecord.create(
                platform.clock.now(),
                str(payload.pop("asset", "")),
                component,
                action,
                severity=severity,
                message=message,
                job_id=job["id"],
                idempotency_key=job["idempotency_key"],
                **payload,
            )
        )
        self.append_audit(job["id"], entry)

    def _next_job(self) -> dict | None:
        with self._lock:
            if any(job["status"] in (RUNNING, PARTIALLY_FAILED) for job in self._jobs.values()):
                return None
            candidates = [job for job in self._jobs.values() if job["status"] == QUEUED]
            candidates.sort(key=lambda item: (item["phase"] != "resuming", item["created_at"]))
            return candidates[0] if candidates else None

    def _run_queue(self) -> None:
        while not self._stopping:
            if not self._ready:
                self._wakeup.wait(0.1)
                self._wakeup.clear()
                continue
            job = self._next_job()
            if job is None:
                self._wakeup.wait(0.1)
                self._wakeup.clear()
                continue
            self._execute_job(job)

    def _execute_job(self, queued: dict) -> None:
        with self._lock:
            job = self._jobs[queued["id"]]
            job["status"] = RUNNING
            job["phase"] = job.get("phase") if job.get("phase") == "resuming" else "starting"
            job["current_step"] = "执行前置状态检查"
            job["attempts"] = int(job.get("attempts", 0)) + 1
            if not job.get("started_at"):
                job["started_at"] = time.time()
            job["updated_at"] = time.time()
            self._persist_jobs()
            job_copy = copy.deepcopy(job)

        operation = job_copy["operation"]
        runner = self._runners.get(operation)
        try:
            if runner is None:
                raise ActionRejected(
                    "长动作执行器未注册",
                    component="action",
                    code="runner_missing",
                    blocked_step="dispatch",
                )

            def update(**changes: object) -> dict:
                return self.update_job(job_copy["id"], **changes)

            result = runner(self, job_copy, update)
            with self._lock:
                current = self._jobs[job_copy["id"]]
                current.update(
                    status=SUCCEEDED,
                    progress_pct=100.0,
                    phase="completed",
                    current_step="完成",
                    blocked_step="",
                    result=result,
                    error=None,
                    finished_at=time.time(),
                )
                current["updated_at"] = time.time()
                self._persist_jobs()
                self._sync_request(job_copy["id"], SUCCEEDED)
                snapshot = copy.deepcopy(current)
            self.audit(
                snapshot,
                "action",
                f"{operation}_complete",
                "notice",
                f"{operation} 成功完成",
                **{
                    "progress_pct": 100.0,
                    "result": self._flat(result),
                },
            )
        except ActionRejected as error:
            self._fail_job(job_copy["id"], error, terminal=error.code == "job_not_resumable")
        except PlantError as error:
            rejected = ActionRejected(
                error.message,
                component=error.component,
                code=type(error).__name__,
                blocked_step="execute",
            )
            self._fail_job(job_copy["id"], rejected, terminal=False)
        except Exception as error:  # 防御后台线程静默退出
            rejected = ActionRejected(
                str(error), component="action", code=type(error).__name__, blocked_step="execute"
            )
            self._fail_job(job_copy["id"], rejected, terminal=True)
        finally:
            self._wakeup.set()

    def _fail_job(self, job_id: str, error: ActionRejected, *, terminal: bool) -> None:
        with self._lock:
            job = self._jobs[job_id]
            status = FAILED if terminal else PARTIALLY_FAILED
            job.update(
                status=status,
                phase="failed" if terminal else "partial_failure",
                current_step="续作前停止" if not terminal else "终止",
                blocked_step=error.blocked_step,
                error={
                    "type": type(error).__name__,
                    "code": error.code,
                    "component": error.component,
                    "message": error.message,
                    "current_state": error.current_state,
                    "details": error.details,
                },
                finished_at=time.time() if terminal else None,
            )
            job["updated_at"] = time.time()
            self._persist_jobs()
            self._sync_request(job_id, status)
            snapshot = copy.deepcopy(job)
        severity = "alarm" if terminal else "warning"
        self.audit(
            snapshot,
            error.component,
            f"{job['operation']}_{'failed' if terminal else 'partially_failed'}",
            severity,
            error.message,
            code=error.code,
            blocked_step=error.blocked_step,
            current_state=self._flat(error.current_state),
        )

    def _sync_request(self, job_id: str, state: str) -> None:
        for record in self._requests.values():
            if record.get("job_id") == job_id:
                record["state"] = state
        self._persist_requests()

    @staticmethod
    def _flat(payload: object) -> object:
        """审计 JSONL 本身支持复杂字段；保留函数边界便于以后裁剪大结果。"""

        return payload
