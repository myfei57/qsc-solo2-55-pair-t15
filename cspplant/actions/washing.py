"""可续看的镜面清洗长动作。

作业按镜子逐台推进，并在开窗口、喷淋阶段和回水阶段写检查点。任何阶段被联锁
打断都不关闭清洗窗口，作业停在 :data:`PARTIALLY_FAILED`；续作复用原作业和原
幂等语义之外的新续作键，从最后一个检查点继续。
"""

from __future__ import annotations

import time
from typing import Callable

from ..errors import ActionRejected, PlantError
from .manager import ActionManager

SPRAY_CHUNKS = 20
DRAIN_CHUNKS = 20
RECOVERY_RATIO = 0.85


def _sleep(manager: ActionManager) -> None:
    if manager.tick_s > 0:
        time.sleep(manager.tick_s)


def _progress(update: Callable[..., dict], job: dict, pct: float, step: str, **extra: object) -> None:
    payload: dict[str, object] = {"progress_pct": round(max(0.0, min(100.0, pct)), 3), "current_step": step}
    payload.update(extra)
    update(**payload)


def run_wash(manager: ActionManager, job: dict, update: Callable[..., dict]) -> dict:
    """执行清洗作业；由 :class:`ActionManager` 在单工后台线程调用。"""

    platform = manager._platform  # noqa: SLF001 - 动作执行器与平台在同一进程内装配
    params = job["params"]
    mirrors = list(params["mirrors"])
    zone = str(params["zone"])
    checkpoint = dict(job.get("checkpoint") or {})
    results: list[dict] = list(checkpoint.get("mirror_results", []))
    completed = {item["mirror"] for item in results}
    is_resume = job.get("phase") == "resuming"

    manager.audit(
        job,
        "action",
        "wash_started" if not is_resume else "wash_resumed",
        "notice",
        "清洗作业开始" if not is_resume else "清洗作业从检查点续作",
        mirrors=mirrors,
        zone=zone,
        progress_pct=float(job.get("progress_pct", 0.0)),
    )

    with platform.lock():
        if not checkpoint.get("window_open"):
            active = platform.field.sweeps.active()
            if active is not None:
                raise ActionRejected(
                    f"清洗窗口 {active.zone} 尚未关闭",
                    component="field",
                    code="wash_window_open",
                    blocked_step="open_window",
                    current_state={"active_window": active.as_dict()},
                )
            _progress(update, job, 1.0, "打开清洗窗口", phase="open_window")
            window = platform.field.open_window(mirrors, zone, platform.clock.now())
            checkpoint["window_open"] = True
            checkpoint["window"] = window.as_dict()
            update(checkpoint=dict(checkpoint))
            manager.audit(
                {**job, "checkpoint": checkpoint},
                "field",
                "wash_window_opened",
                "notice",
                f"清洗窗口 {zone} 已打开",
                zone=zone,
                mirrors=list(window.mirrors),
            )
        else:
            active = platform.field.sweeps.active()
            if active is None or list(active.mirrors) != mirrors or active.zone != zone:
                raise ActionRejected(
                    "现场清洗窗口与作业检查点不一致",
                    component="field",
                    code="checkpoint_window_mismatch",
                    blocked_step="resume_window_check",
                    current_state={
                        "expected_mirrors": mirrors,
                        "expected_zone": zone,
                        "active_window": None if active is None else active.as_dict(),
                    },
                )

    total = len(mirrors)
    for index, mirror in enumerate(mirrors):
        if mirror in completed:
            continue
        base = 5.0 + index * 90.0 / total
        span = 90.0 / total
        result = _wash_one_mirror(manager, job, update, checkpoint, mirror, index, total, base, span)
        results.append(result)
        completed.add(mirror)
        checkpoint["mirror_results"] = list(results)
        update(checkpoint=dict(checkpoint))

    with platform.lock():
        _progress(update, job, 98.0, "关闭清洗窗口", phase="close_window")
        closed = platform.field.close_window(platform.clock.now())
        checkpoint["window_open"] = False
        checkpoint["closed_window"] = closed.as_dict()
        update(checkpoint=dict(checkpoint), progress_pct=100.0, phase="completed", current_step="完成")

    washed = sum(1 for item in results if item["ok"])
    violations = [violation for item in results for violation in item.get("violations", [])]
    result = {
        "ok": washed == total and not violations,
        "washed": washed,
        "mirrors": total,
        "zone": zone,
        "violations": violations,
        "mirror_results": results,
        "last_mirror": mirrors[-1],
        "window": checkpoint.get("closed_window"),
    }
    manager.audit(
        {**job, "checkpoint": checkpoint},
        "wash",
        "wash_window_closed",
        "notice",
        f"清洗窗口 {zone} 已关闭",
        zone=zone,
        washed=washed,
        mirrors=total,
    )
    return result


def _wash_one_mirror(
    manager: ActionManager,
    job: dict,
    update: Callable[..., dict],
    checkpoint: dict,
    mirror: str,
    index: int,
    total: int,
    base: float,
    span: float,
) -> dict:
    platform = manager._platform  # noqa: SLF001
    truck = platform.truck
    drain = platform.drain
    duration_s = truck.limits.wash_duration_s
    flow_m3_h = truck.limits.return_flow_m3_h
    required_m3 = flow_m3_h * duration_s / 3600.0

    resume_phase = checkpoint.get("mirror_phase") if checkpoint.get("current_mirror") == mirror else None
    used_before = truck.used_m3()
    returned_before = truck.returned_m3()
    dt = duration_s / SPRAY_CHUNKS
    per_step = flow_m3_h * dt / 3600.0
    elapsed = float(checkpoint.get("spray_elapsed_s", 0.0)) if resume_phase in {"spray", "drain", "complete"} else 0.0
    last_milestone = int(elapsed / duration_s * 10)

    if resume_phase not in {"spray", "drain", "complete"}:
        with platform.lock():
            _progress(
                update,
                job,
                base,
                f"{mirror}：清洗车就位",
                phase=f"mirror_{index + 1}_arrive",
                current_mirror=mirror,
            )
            checkpoint["current_mirror"] = mirror
            checkpoint["mirror_index"] = index
            checkpoint["mirror_phase"] = "arrive"
            checkpoint["used_m3"] = 0.0
            checkpoint["recovered_m3"] = 0.0
            update(checkpoint=dict(checkpoint))
            truck.arrive(mirror)
            manager.audit({**job, "checkpoint": checkpoint}, "wash", "wash_truck_arrive", "notice", f"清洗车到达 {mirror}", asset=mirror)

    if resume_phase not in {"drain", "complete"}:
        with platform.lock():
            _progress(
                update,
                job,
                base + span * 0.08,
                f"{mirror}：继续喷淋" if resume_phase == "spray" else f"{mirror}：开始喷淋",
                phase=f"mirror_{index + 1}_spray",
            )
            checkpoint["mirror_phase"] = "spray"
            if elapsed <= 0.0:
                checkpoint["spray_elapsed_s"] = 0.0
            update(checkpoint=dict(checkpoint))
            already_spraying = truck.mirror() == mirror and truck.state() == "washing"
            restarted_spray = resume_phase == "spray" and not already_spraying
            if restarted_spray:
                truck.resume_wash(mirror, elapsed)
            if not already_spraying and not restarted_spray:
                remaining_s = max(0.0, duration_s - elapsed)
                remaining_m3 = flow_m3_h * remaining_s / 3600.0
                try:
                    if truck.water_m3() < remaining_m3:
                        raise ActionRejected(
                            "车载水量不足以完成本镜剩余喷淋",
                            component="wash",
                            code="wash_water_insufficient",
                            blocked_step="spray",
                            current_state={"water_m3": truck.water_m3(), "required_m3": remaining_m3},
                        )
                    if truck.mirror() != mirror:
                        truck.arrive(mirror)
                    truck.start_wash()
                except PlantError as error:
                    if isinstance(error, ActionRejected):
                        raise
                    raise ActionRejected(
                        error.message,
                        component=error.component,
                        code=type(error).__name__,
                        blocked_step="spray_start",
                        current_state={"mirror": mirror},
                    ) from error

    while elapsed < duration_s - 1e-9:
        with platform.lock():
            if truck.mirror() != mirror or truck.state() != "washing":
                raise ActionRejected(
                    "清洗车状态与作业检查点不一致",
                    component="wash",
                    code="wash_truck_state_changed",
                    blocked_step="spray",
                    current_state={"mirror": truck.mirror(), "state": truck.state()},
                )
            truck.step(dt)
            drain.receive(per_step * RECOVERY_RATIO)
            truck.record_returned(per_step * RECOVERY_RATIO)
        elapsed += dt
        checkpoint["spray_elapsed_s"] = min(duration_s, elapsed)
        ratio = min(1.0, elapsed / duration_s)
        milestone = int(ratio * 10)
        pct = base + span * (0.1 + 0.65 * ratio)
        if milestone > last_milestone and milestone < 10:
            last_milestone = milestone
            manager.audit(
                {**job, "checkpoint": checkpoint},
                "wash",
                "wash_spray_progress",
                "notice",
                f"{mirror} 喷淋进度 {milestone * 10}%",
                asset=mirror,
                progress_pct=round(pct, 3),
                mirror_progress_pct=round(ratio * 100, 1),
            )
        update(
            checkpoint=dict(checkpoint),
            progress_pct=pct,
            current_step=f"{mirror}：喷淋 {round(ratio * 100)}%",
        )
        _sleep(manager)

    with platform.lock():
        if resume_phase in {"drain", "complete"} and truck.mirror() != mirror:
            truck.resume_wash(mirror, duration_s)
            drain.resume(float(checkpoint.get("tray_m3", 0.0)))
        checkpoint["mirror_phase"] = "drain"
        checkpoint["tray_m3"] = drain.tray_m3()
        update(checkpoint=dict(checkpoint), current_step=f"{mirror}：回收回水")
        try:
            if not drain.drain_open():
                drain.open_drain()
        except PlantError as error:
            raise ActionRejected(
                error.message,
                component=error.component,
                code=type(error).__name__,
                blocked_step="open_drain",
                current_state={"mirror": mirror},
            ) from error
        manager.audit({**job, "checkpoint": checkpoint}, "wash", "wash_drain_opened", "notice", f"{mirror} 开始回收回水", asset=mirror)

    drain_dt = duration_s / DRAIN_CHUNKS
    for chunk in range(DRAIN_CHUNKS):
        with platform.lock():
            if drain.drained():
                break
            drain.step(drain_dt)
            checkpoint["tray_m3"] = drain.tray_m3()
        ratio = (chunk + 1) / DRAIN_CHUNKS
        update(
            checkpoint=dict(checkpoint),
            progress_pct=base + span * (0.78 + 0.18 * ratio),
            current_step=f"{mirror}：排空回水 {round(ratio * 100)}%",
        )
        _sleep(manager)

    with platform.lock():
        if not drain.drained():
            raise ActionRejected(
                "托盘未排空，清洗停在回水泵停止前",
                component="wash",
                code="tray_not_drained",
                blocked_step="stop_return_pump",
                current_state={"tray_m3": drain.tray_m3()},
            )
        try:
            drain.stop_pump()
        except PlantError as error:
            raise ActionRejected(
                error.message,
                component=error.component,
                code=type(error).__name__,
                blocked_step="stop_return_pump",
                current_state={"tray_m3": drain.tray_m3()},
            ) from error
        truck.refill(drain.recovered_m3())
        used = truck.used_m3() - used_before
        recovered = truck.returned_m3() - returned_before
        checkpoint["mirror_phase"] = "complete"
        checkpoint["tray_m3"] = 0.0
        update(
            checkpoint=dict(checkpoint),
            progress_pct=base + span,
            current_step=f"{mirror}：清洗完成",
        )
        manager.audit(
            {**job, "checkpoint": checkpoint},
            "wash",
            "mirror_wash_complete",
            "notice",
            f"{mirror} 清洗完成",
            asset=mirror,
            used_m3=round(used, 4),
            recovered_m3=round(recovered, 4),
        )

    return {
        "ok": True,
        "mirror": mirror,
        "used_m3": round(used, 4),
        "recovered_m3": round(recovered, 4),
        "violations": [],
    }
