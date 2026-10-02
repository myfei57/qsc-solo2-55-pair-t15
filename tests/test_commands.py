"""命令网关：幂等提交、状态拒绝、清洗任务排队与中断续跑。

覆盖的关键约定：

* 同一提交标识并发提交只执行一次，重复提交回放首次结果（成功与拒绝都回放）；
* 同标识不同参数指纹冲突拒绝；请求体 400 不占标识，可同标识更正重提；
* 状态不满足的动作按策略当场 409，响应里的 ``stage`` / ``code`` 说清卡在哪一步，
  且拒绝理由与审计流水一致；只有清洗排队，其余动作一律当场拒绝；
* 清洗任务逐面推进、进度与单面结果可轮询，后台串行执行并在重启后把活态任务
  翻成 ``interrupted`` 部分失败，续跑只洗未完成的镜子且复用未关闭的窗口。
"""

from __future__ import annotations

import threading
import time

import pytest

from cspplant.command.gateway import fingerprint
from cspplant.command.jobs import INTERRUPTED, QUEUED, SUCCEEDED
from cspplant.command.ledger import REJECTED, SUCCEEDED as LEDGER_SUCCEEDED
from cspplant.console.api import Api
from cspplant.console.pages import render_page


def wait_idle(platform, timeout: float = 15.0) -> bool:
    """轮询等任务清空（后台执行器是真实线程，按挂钟等待）。"""

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not platform.commands.jobs.live():
            return True
        time.sleep(0.02)
    return not platform.commands.jobs.live()


def _submit(api: Api, action: str, params: dict | None = None, request_id: str = "req"):
    path = {
        "track": "/api/track",
        "circulate": "/api/circulate",
        "changeover": "/api/changeover",
        "trip": "/api/trip",
        "reset": "/api/reset",
        "wash": "/api/wash",
        "pump": "/api/pump",
    }[action]
    body = dict(params or {})
    body["request_id"] = request_id
    return api.handle("POST", path, {}, body)


def test_missing_request_id_is_rejected_with_stage(platform):
    platform.start()
    platform.track(800.0, 720.0)
    api = Api(platform)
    status, payload = api.handle("POST", "/api/track", {}, {"dni_w_m2": 820.0})
    assert status == 409
    assert payload["code"] == "missing_request_id"
    assert payload["stage"] == "dispatch"


def test_same_request_id_concurrent_submit_executes_once(platform):
    platform.start()
    platform.track(800.0, 720.0)
    api = Api(platform)
    status, first = _submit(api, "track", {"dni_w_m2": 820.0, "minute_of_day": 720.0}, "dup")
    assert status == 200
    operations = platform.status()["operations"]

    results: list[tuple[int, dict]] = []
    barrier = threading.Barrier(8)

    def hit() -> None:
        barrier.wait()
        results.append(_submit(api, "track", {"dni_w_m2": 820.0, "minute_of_day": 720.0}, "dup"))

    threads = [threading.Thread(target=hit) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert all(status == 200 for status, _ in results)
    assert all(payload["op_id"] == first["op_id"] for _, payload in results)
    assert platform.status()["operations"] == operations
    assert platform.commands.ledger.get("dup").state == LEDGER_SUCCEEDED


def test_same_id_different_params_conflicts(platform):
    platform.start()
    platform.track(800.0, 720.0)
    api = Api(platform)
    assert _submit(api, "track", {"dni_w_m2": 820.0}, "k")[0] == 200
    status, payload = _submit(api, "track", {"dni_w_m2": 900.0}, "k")
    assert status == 409
    assert payload["code"] == "idempotency_key_conflict"


def test_bad_request_does_not_consume_request_id(platform):
    platform.start()
    api = Api(platform)
    status, _ = api.handle(
        "POST", "/api/calibrate", {}, {"measured_kg_s": -5, "request_id": "c1"}
    )
    assert status == 400
    status, _ = api.handle(
        "POST", "/api/calibrate", {}, {"measured_kg_s": 60.0, "request_id": "c1"}
    )
    assert status == 200


def test_rejected_submission_is_replayed_with_same_reason(platform):
    platform.start()
    platform.track(800.0, 720.0)
    api = Api(platform)
    assert _submit(api, "trip", {"reason": "overtemperature"}, "t1")[0] == 200
    for _ in range(2):
        status, payload = _submit(api, "trip", {"reason": "overtemperature"}, "t2")
        assert status == 409
        assert payload["stage"] == "safety_latch"
        assert payload["code"] == "already_latched"
        assert payload["blocked_by"] == "alarm.latch"
    assert platform.commands.ledger.get("t2").state == REJECTED


def test_reset_cooldown_rejected_then_allowed(platform):
    platform.start()
    platform.track(800.0, 720.0)
    api = Api(platform)
    _submit(api, "trip", {"reason": "overtemperature"}, "t1")
    status, payload = _submit(api, "reset", {"operator": "lead"}, "r1")
    assert status == 409
    assert payload["stage"] == "latch_cooldown"
    assert payload["code"] == "cooldown"
    platform.clock.advance(platform.latch.limits.latch_reset_delay_s + 1.0)
    assert _submit(api, "reset", {"operator": "lead"}, "r2")[0] == 200


def test_rejection_reason_matches_audit(platform):
    platform.start()
    platform.track(800.0, 720.0)
    api = Api(platform)
    _submit(api, "trip", {"reason": "overtemperature"}, "t1")
    status, payload = _submit(api, "circulate", {}, "c1")
    assert status == 409
    events = [
        event
        for event in platform.bus.history("command")
        if event.action == "command_rejected" and event.payload.get("op_id") == "c1"
    ]
    assert events
    assert events[-1].payload["stage"] == payload["stage"] == "safety_latch"
    assert events[-1].payload["code"] == payload["code"] == "latched"
    assert events[-1].payload["message"] == payload["message"]


def test_wash_is_queued_and_progress_pollable(platform):
    platform.start()
    platform.track(800.0, 720.0)
    api = Api(platform)
    status, payload = _submit(api, "wash", {"mirrors": ["m021", "m022"], "zone": "maint-north"}, "w1")
    assert status == 202
    assert payload["state"] == QUEUED
    job_id = payload["job_id"]
    assert wait_idle(platform)

    status, body = api.handle("GET", f"/api/job/{job_id}", {}, {})
    assert status == 200
    assert body["state"] == SUCCEEDED
    assert body["total"] == 2 and body["succeeded"] == 2 and body["progress"] == 1.0
    assert {item["mirror"] for item in body["results"]} == {"m021", "m022"}
    assert platform.mirrors_in_window() == []
    finished = [e for e in platform.bus.history("command") if e.action == "job_finished"]
    assert finished and finished[-1].payload["ok"] is True


def test_wash_queue_runs_one_at_a_time(platform):
    platform.start()
    platform.track(800.0, 720.0)
    api = Api(platform)
    _, first = _submit(api, "wash", {"mirrors": ["m021"]}, "q1")
    _, second = _submit(api, "wash", {"mirrors": ["m031", "m032"]}, "q2")
    assert wait_idle(platform)
    job_a = platform.commands.get_job(first["job_id"])
    job_b = platform.commands.get_job(second["job_id"])
    assert job_a["state"] == job_b["state"] == SUCCEEDED
    assert job_b["started_at"] >= job_a["finished_at"]
    assert platform.truck.washes() == 3


def test_wash_duplicate_id_replays_same_job_without_rerun(platform):
    platform.start()
    platform.track(800.0, 720.0)
    api = Api(platform)
    status, first = _submit(api, "wash", {"mirrors": ["m021"]}, "wdup")
    assert status == 202
    assert wait_idle(platform)
    status, replay = _submit(api, "wash", {"mirrors": ["m021"]}, "wdup")
    assert status == 202
    assert replay["job_id"] == first["job_id"]
    assert platform.truck.washes() == 1


def test_empty_wash_batch_rejected(platform):
    platform.start()
    platform.track(800.0, 720.0)
    api = Api(platform)
    status, payload = _submit(api, "wash", {"mirrors": []}, "we")
    assert status == 409
    assert payload["code"] == "empty_batch"
    assert payload["stage"] == "batch"


def test_interrupted_job_resumes_only_remaining_mirrors(platform, tmp_path):
    platform.start()
    platform.track(800.0, 720.0)
    api = Api(platform)
    params = {"mirrors": ["m021", "m022", "m023"], "zone": "maint-north"}

    # 构造一个执行到一半的现场：窗口开着、任务停在 running、账本声称未完成。
    platform.store.write(
        "field/window",
        {"mirrors": params["mirrors"], "zone": "maint-north", "opened_at": platform.clock.now()},
    )
    platform.store.write(
        "command/jobs",
        [
            {
                "job_id": "job-crash",
                "action": "wash",
                "request_id": "wcrash",
                "state": "running",
                "queue_position": 0,
                "submitted_at": platform.clock.now(),
                "started_at": platform.clock.now(),
                "finished_at": None,
                "stage": "washing:m022",
                "message": "正在清洗 m022",
                "zone": "maint-north",
                "total": 3,
                "processed": 1,
                "succeeded": 1,
                "failed": 0,
                "current_mirror": "m022",
                "results": [
                    {"ok": True, "mirror": "m021", "steps": [], "violations": [],
                     "used_m3": 0.65, "recovered_m3": 0.55}
                ],
                "violations": [],
                "resumes": "",
                "rejection": None,
                "params": params,
            }
        ],
    )
    platform.store.write(
        "command/idempotency",
        {
            "wcrash": {
                "request_id": "wcrash",
                "action": "wash",
                "fingerprint": fingerprint("wash", params),
                "state": "claimed",
                "status_code": 0,
                "payload": None,
                "rejection": None,
            }
        },
    )

    # 用同一存储目录重新装配，模拟控制服务重启。
    from cspplant.app import build_plant
    from cspplant.config import default_config

    restarted = build_plant(
        default_config(), clock=platform.clock, storage_root=platform.store.root
    )
    restarted.start()
    api2 = Api(restarted)

    interrupted = restarted.commands.get_job("job-crash")
    assert interrupted["state"] == INTERRUPTED
    assert interrupted["processed"] == 1
    assert restarted.mirrors_in_window() == params["mirrors"]

    # 旧标识因前序结果未知而拒绝重放。
    status, payload = _submit(api2, "wash", params, "wcrash")
    assert status == 409
    assert payload["code"] == "idempotency_indeterminate"

    washes_before = restarted.truck.washes()
    status, payload = api2.handle(
        "POST", "/api/wash/resume", {}, {"job_id": "job-crash", "request_id": "w2"}
    )
    assert status == 202
    assert payload["resumes"] == "job-crash"
    assert wait_idle(restarted)
    resumed = restarted.commands.get_job(payload["job_id"])
    assert resumed["state"] == SUCCEEDED
    assert resumed["total"] == 3
    assert resumed["succeeded"] == 3
    assert restarted.truck.washes() - washes_before == 2
    assert restarted.mirrors_in_window() == []
    assert [e for e in restarted.bus.history("command") if e.action == "window_reused"]


def test_resume_requires_interrupted_state(platform):
    platform.start()
    platform.track(800.0, 720.0)
    api = Api(platform)
    status, queued = _submit(api, "wash", {"mirrors": ["m021"]}, "w1")
    assert status == 202
    assert wait_idle(platform)
    status, payload = api.handle(
        "POST", "/api/wash/resume", {}, {"job_id": queued["job_id"], "request_id": "w2"}
    )
    assert status == 409
    assert payload["code"] == "job_not_resumable"


def test_jobs_page_polls_with_fixed_interval(platform):
    platform.start()
    html = render_page("/jobs", platform)
    assert "setInterval(refreshJobs, 2000)" in html
    assert "/api/jobs" in html


def test_fingerprint_stable_but_distinguishes_params():
    assert fingerprint("track", {"dni_w_m2": 800.0}) == fingerprint("track", {"dni_w_m2": 800.0})
    assert fingerprint("track", {"dni_w_m2": 800.0}) != fingerprint("track", {"dni_w_m2": 801.0})
    # 指纹不取无关字段（writer 不是 track 的意图参数）。
    assert fingerprint("track", {"dni_w_m2": 800.0, "writer": "a"}) == fingerprint(
        "track", {"dni_w_m2": 800.0, "writer": "b"}
    )
