"""清洗循环与回水回收顺序。"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from cspplant.app import build_plant
from cspplant.config import default_config
from cspplant.errors import InterlockError
from cspplant.wash.cycle import WashCycle
from cspplant.wash.drain import DrainSystem
from cspplant.wash.truck import WashTruck


def test_truck_rejects_wash_without_water():
    limits = default_config().wash
    truck = WashTruck(limits, water_m3=0.0)
    truck.arrive("m001")
    with pytest.raises(InterlockError):
        truck.start_wash()


def test_drain_refuses_to_stop_before_tray_is_empty():
    limits = default_config().wash
    drain = DrainSystem(limits)
    drain.receive(0.5)
    drain.open_drain()
    with pytest.raises(InterlockError):
        drain.stop_pump()
    assert drain.premature_stops() == 1
    for _ in range(200):
        drain.step(1.0)
    assert drain.drained() is True
    assert drain.stop_pump() is True
    assert drain.stops() == 1
    assert drain.recovered_m3() == pytest.approx(0.5)


def test_wash_cycle_runs_in_order():
    limits = default_config().wash
    truck = WashTruck(limits)
    drain = DrainSystem(limits)
    report = WashCycle().run(truck, drain, "m001")
    assert report.ok is True
    assert report.violations == []
    actions = [step["action"] for step in report.steps]
    assert actions.index("arrive") < actions.index("spray")
    assert actions.index("spray") < actions.index("open_drain")
    assert actions.index("open_drain") < actions.index("stop_return_pump")
    assert report.recovered_m3 > 0.0
    assert truck.washes() == 1


def test_platform_wash_stows_window_mirrors(platform):
    platform.start()
    platform.track(800.0, 720.0)
    report = platform.wash(["m021", "m022"], "maint-north")
    assert report["ok"] is True
    assert report["washed"] == 2
    assert platform.drain.premature_stops() == 0
    assert platform.truck.washes() == 2
    assert platform.mirrors_in_window() == []


def _wait_for_job(platform, job_id: str) -> dict:
    for _ in range(300):
        job = platform.wash_job(job_id)
        if job["status"] in {"SUCCEEDED", "FAILED", "PARTIALLY_FAILED"}:
            return job
        time.sleep(0.01)
    raise AssertionError(job)


def test_long_wash_is_idempotent_and_polled(platform):
    platform.start()
    platform.track(800.0, 720.0)
    first, created = platform.submit_wash("wash-1", ["m021", "m022"], "maint-north")
    assert created is True
    replay, created_again = platform.submit_wash("wash-1", ["m021", "m022"], "maint-north")
    assert created_again is False
    assert replay["id"] == first["id"]
    job = _wait_for_job(platform, first["id"])
    assert job["status"] == "SUCCEEDED"
    assert job["progress_pct"] == 100.0
    assert job["result"]["washed"] == 2
    assert job["audit_refs"]


def test_concurrent_long_wash_submissions_share_one_job(platform):
    platform.start()
    platform.track(800.0, 720.0)
    with ThreadPoolExecutor(max_workers=6) as pool:
        outcomes = list(
            pool.map(
                lambda _: platform.submit_wash("same-wash", ["m021", "m022"], "maint-north"),
                range(6),
            )
        )
    assert len({job["id"] for job, _ in outcomes}) == 1
    assert sum(1 for _, created in outcomes if created) == 1
    assert _wait_for_job(platform, outcomes[0][0]["id"])["status"] == "SUCCEEDED"


def test_partial_wash_keeps_window_and_resumes(platform):
    platform.start()
    platform.track(800.0, 720.0)
    platform.truck._water_m3 = 0.7
    submitted, _ = platform.submit_wash("wash-1", ["m021", "m022"], "maint-north")
    job = _wait_for_job(platform, submitted["id"])
    assert job["status"] == "PARTIALLY_FAILED"
    assert job["resumable"] is True
    assert job["blocked_step"] == "spray"
    assert platform.mirrors_in_window() == ["m021", "m022"]

    platform.truck.refill(5.0)
    resumed, first_resume = platform.resume_wash(submitted["id"], "resume-1")
    assert first_resume is True
    assert resumed["phase"] == "resuming"
    completed = _wait_for_job(platform, submitted["id"])
    assert completed["status"] == "SUCCEEDED"
    assert completed["result"]["washed"] == 2
    assert platform.mirrors_in_window() == []


def test_interrupted_wash_restarts_as_resumable_partial_failure(platform):
    platform.start()
    platform.actions.stop()
    job_id = "job-restart"
    platform.actions._jobs[job_id] = {
        "id": job_id,
        "idempotency_key": "old",
        "operation": "wash",
        "params": {"mirrors": ["m021"], "zone": "maint-north"},
        "queue_policy": "reject_when_busy",
        "status": "RUNNING",
        "progress_pct": 10.0,
        "phase": "mirror_1_spray",
        "current_step": "执行中",
        "blocked_step": "",
        "checkpoint": {"window_open": True, "mirror_phase": "spray", "spray_elapsed_s": 10.0},
        "result": None,
        "error": None,
        "attempts": 1,
        "audit_refs": [],
        "created_at": 1.0,
        "updated_at": 1.0,
        "started_at": 1.0,
        "finished_at": None,
    }
    platform.actions._persist_jobs()

    restarted = build_plant(default_config(), clock=platform.clock, storage_root=platform.store.root)
    job = restarted.wash_job(job_id)
    assert job["status"] == "PARTIALLY_FAILED"
    assert job["resumable"] is True
    assert job["error"]["code"] == "WORKER_INTERRUPTED"
