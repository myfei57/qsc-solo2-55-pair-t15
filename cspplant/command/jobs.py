"""长动作任务表。

清洗这类要跑一会儿的动作不在请求线程里做完：提交后立刻拿到任务句柄，后台
线程逐面推进，控制台轮询 :meth:`JobStore.get` 看进度。任务每次状态迁移都
原子落盘（复用 :class:`~cspplant.store.jsonfile.JsonStore` 的临时文件 +
``os.replace``），因此：

* 进程中断后重新装配，上一轮停在 ``queued`` / ``running`` 的任务会被翻成
  ``interrupted``，已洗完的镜子留在 ``results`` 里，控制台打开页面就能
  "续看"上次跑到哪、哪些镜子没洗完；
* 中断属于**部分失败**：任务终态不是成功，网关允许拿它做续跑，只重洗
  未完成的镜子，已洗完的不重复动。
"""

from __future__ import annotations

import threading
from typing import Callable

from ..ids import IdFactory

# 任务状态。前四个是活态，后三个是终态。
QUEUED = "queued"
RUNNING = "running"
SUCCEEDED = "succeeded"
FAILED = "failed"
REJECTED = "rejected"
INTERRUPTED = "interrupted"

LIVE_STATES = (QUEUED, RUNNING)
FINAL_STATES = (SUCCEEDED, FAILED, REJECTED, INTERRUPTED)

_MAX_FINISHED = 64


class JobHandle:
    """一个长动作任务的可变句柄（持锁访问）。"""

    def __init__(
        self,
        job_id: str,
        action: str,
        request_id: str,
        params: dict,
        *,
        submitted_at: float,
        resumes: str = "",
    ) -> None:
        self.job_id = job_id
        self.action = action
        self.request_id = request_id
        self.params = dict(params)
        self.state = QUEUED
        self.queue_position = 0
        self.submitted_at = submitted_at
        self.started_at: float | None = None
        self.finished_at: float | None = None
        self.stage = "queued"
        self.message = "排队等待清洗窗口空闲"
        self.total = 0
        self.processed = 0
        self.succeeded = 0
        self.failed = 0
        self.current_mirror = ""
        self.results: list[dict] = []
        self.violations: list[str] = []
        self.zone = str(params.get("zone", ""))
        self.resumes = resumes
        self.rejection: dict | None = None

    def as_dict(self) -> dict:
        progress = round(self.processed / self.total, 3) if self.total else 0.0
        return {
            "job_id": self.job_id,
            "action": self.action,
            "request_id": self.request_id,
            "state": self.state,
            "queue_position": self.queue_position,
            "submitted_at": round(self.submitted_at, 3),
            "started_at": None if self.started_at is None else round(self.started_at, 3),
            "finished_at": None if self.finished_at is None else round(self.finished_at, 3),
            "stage": self.stage,
            "message": self.message,
            "zone": self.zone,
            "total": self.total,
            "processed": self.processed,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "progress": progress,
            "current_mirror": self.current_mirror,
            "results": list(self.results),
            "violations": list(self.violations),
            "resumes": self.resumes,
            "rejection": self.rejection,
        }


class JobStore:
    """内存任务表 + 落盘副本，后台线程与轮询请求并发安全。"""

    def __init__(self, store, clock, *, doc_id: str = "command/jobs", on_persist: Callable[[], None] | None = None) -> None:
        self._store = store
        self._clock = clock
        self._doc_id = doc_id
        self._ids = IdFactory("job")
        self._jobs: dict[str, JobHandle] = {}
        self._order: list[str] = []
        self._lock = threading.RLock()
        self._on_persist = on_persist
        self._restore()

    def _restore(self) -> None:
        raw = self._store.read(self._doc_id)
        if not isinstance(raw, list):
            return
        now = self._clock.now()
        # 终态任务也要恢复——控制台重启后还得能看到"上一次跑到哪、什么结果"；
        # 只截断数量，不截断最新的记录。活态任务翻成 interrupted 部分失败。
        keep = raw[-_MAX_FINISHED:]
        for item in keep:
            if not isinstance(item, dict) or not item.get("job_id"):
                continue
            state = str(item.get("state", ""))
            if state not in LIVE_STATES + FINAL_STATES:
                continue
            job = JobHandle(
                str(item["job_id"]),
                str(item.get("action", "wash")),
                str(item.get("request_id", "")),
                dict(item.get("params", {})),
                submitted_at=float(item.get("submitted_at", now)),
                resumes=str(item.get("resumes", "")),
            )
            job.queue_position = int(item.get("queue_position", 0))
            job.started_at = item.get("started_at")
            job.zone = str(item.get("zone", job.zone))
            job.total = int(item.get("total", 0))
            job.processed = int(item.get("processed", 0))
            job.succeeded = int(item.get("succeeded", 0))
            job.failed = int(item.get("failed", 0))
            job.results = [dict(r) for r in item.get("results", []) if isinstance(r, dict)]
            job.violations = [str(v) for v in item.get("violations", [])]
            job.current_mirror = str(item.get("current_mirror", ""))
            rejection = item.get("rejection")
            job.rejection = rejection if isinstance(rejection, dict) else None
            if state in LIVE_STATES:
                job.state = INTERRUPTED
                job.stage = "interrupted"
                job.message = "控制服务在上次执行途中中断，任务部分失败，可续跑未完成部分"
                job.finished_at = now
            else:
                job.state = state
                job.stage = str(item.get("stage", ""))
                job.message = str(item.get("message", ""))
                job.finished_at = item.get("finished_at")
            self._jobs[job.job_id] = job
            self._order.append(job.job_id)

    @property
    def lock(self) -> threading.RLock:
        """任务表锁；后台执行器用它与控制循环串行化现场动作。"""

        return self._lock

    def create(self, action: str, request_id: str, params: dict, *, resumes: str = "",
               seed: dict | None = None) -> JobHandle:
        with self._lock:
            job = JobHandle(
                self._ids.next(), action, request_id, params, submitted_at=self._clock.now(), resumes=resumes
            )
            if seed is not None:
                # 续跑任务继承上次的进度：总镜数、已洗完的单面结果与违约项，使
                # "跑到哪了 / 上一次结果"在新任务上连续可见。
                job.total = int(seed.get("total", 0))
                job.results = [dict(item) for item in seed.get("results", []) if isinstance(item, dict)]
                job.processed = len(job.results)
                job.succeeded = sum(1 for item in job.results if item.get("ok"))
                job.failed = job.processed - job.succeeded
                job.violations = list(seed.get("violations", []))
            self._jobs[job.job_id] = job
            self._order.append(job.job_id)
            self.persist()
            return job

    def get(self, job_id: str) -> JobHandle | None:
        with self._lock:
            return self._jobs.get(job_id)

    def update(self, job: JobHandle, **changes: object) -> None:
        """更新任务字段并落盘。"""

        with self._lock:
            for key, value in changes.items():
                setattr(job, key, value)
            self.persist()

    def append_result(self, job: JobHandle, result: dict) -> None:
        """登记一面镜子的清洗结果并落盘。

        续跑时同一面镜子可能重洗（上次失败的）：按镜子名替换旧结果，计数相应
        回滚后重计，processed/total 不会因重洗而虚高。
        """

        with self._lock:
            mirror = str(result.get("mirror", ""))
            existing = next((item for item in job.results if item.get("mirror") == mirror), None)
            if existing is not None:
                job.results.remove(existing)
                job.processed -= 1
                if existing.get("ok"):
                    job.succeeded -= 1
                else:
                    job.failed -= 1
            job.results.append(dict(result))
            job.processed += 1
            if result.get("ok"):
                job.succeeded += 1
            else:
                job.failed += 1
                for violation in result.get("violations", []):
                    if violation not in job.violations:
                        job.violations.append(str(violation))
            self.persist()

    def finish(self, job: JobHandle, state: str, *, stage: str, message: str) -> None:
        """把任务推进到终态并落盘。"""

        with self._lock:
            job.state = state
            job.stage = stage
            job.message = message
            job.finished_at = self._clock.now()
            job.current_mirror = ""
            self.persist()

    def persist(self) -> None:
        """把当前任务表原子落盘（调用方持锁）。

        活态任务全部落盘，终态任务只落最近 :data:`_MAX_FINISHED` 条，避免文件
        无限增长；内存表不截断，本次运行内仍能查到全部任务。
        """

        live_ids = [job_id for job_id in self._order if self._jobs[job_id].state in LIVE_STATES]
        finished_ids = [job_id for job_id in self._order if self._jobs[job_id].state not in LIVE_STATES]
        persisted = sorted(
            set(live_ids) | set(finished_ids[-_MAX_FINISHED:]),
            key=self._order.index,
        )
        payload = [self._jobs[job_id].as_dict() for job_id in persisted]
        self._store.write(self._doc_id, payload)
        if self._on_persist is not None:
            self._on_persist()

    def list(self, *, limit: int = 20, action: str | None = None) -> list[dict]:
        """最近的任务（最新在前）。"""

        with self._lock:
            jobs = [self._jobs[job_id] for job_id in self._order]
        if action is not None:
            jobs = [job for job in jobs if job.action == action]
        jobs = jobs[-max(1, limit):]
        return [job.as_dict() for job in reversed(jobs)]

    def live(self, action: str | None = None) -> list[JobHandle]:
        """当前处于排队/执行中的任务（按提交顺序）。"""

        with self._lock:
            jobs = [self._jobs[job_id] for job_id in self._order]
        return [job for job in jobs if job.state in LIVE_STATES and (action is None or job.action == action)]
