"""镜面清洗编排：开窗口 → 重算避让区 → 逐面清洗 → 关窗口。

窗口一开，批次里的镜子就退出聚焦；避让区必须按新批次重新求值，否则镜子会停
在朝着工位检修区的姿态上。清洗本身走回水回收顺序，最后才关闭窗口把镜子放回
自动追日。

长动作（控制台异步清洗任务）不能一口气把整批跑完：进度要逐面可见，进程中断
后还要能从未洗完的镜子继续。因此这里把编排拆成"开窗口 / 洗一面 / 关窗口"
三个可独立调用的步骤，:func:`wash_mirrors` 只是按顺序把它们串起来，供命令行
演示与既有同步调用使用——两条路径走的是同一套底层步骤，结果口径一致。
"""

from __future__ import annotations

from ..field.field import FieldController
from ..wash.cycle import WashCycle
from ..wash.drain import DrainSystem
from ..wash.truck import WashTruck


def open_wash_window(field: FieldController, mirrors: list[str], zone: str, now: float) -> dict:
    """打开清洗窗口并按新批次重算避让区，返回开场两步的记录。"""

    steps: list[dict] = []
    violations: list[str] = []

    window = field.open_window(mirrors, zone, now)
    steps.append(
        {
            "component": "field",
            "action": "open_window",
            "detail": {"zone": window.zone, "mirrors": list(window.mirrors)},
        }
    )

    refreshed = field.refresh_zones(now)
    steps.append(
        {
            "component": "field",
            "action": "refresh_zones",
            "detail": {"refreshed": len(refreshed), "postures": {item["mirror"]: item["posture"] for item in refreshed}},
        }
    )
    if len(refreshed) != len(window.mirrors):
        violations.append("zone_refresh_incomplete")
    return {
        "steps": steps,
        "violations": violations,
        "zone": window.zone,
        "mirrors": list(window.mirrors),
    }


def wash_one_mirror(
    cycle: WashCycle,
    truck: WashTruck,
    drain: DrainSystem,
    mirror: str,
    dt: float = 1.0,
) -> dict:
    """清洗单面镜子并回收回水，返回这一面的步骤与结果。"""

    report = cycle.run(truck, drain, mirror, dt)
    return {
        "ok": report.ok,
        "mirror": mirror,
        "steps": report.steps,
        "violations": list(report.violations),
        "used_m3": round(report.used_m3, 4),
        "recovered_m3": round(report.recovered_m3, 4),
    }


def close_wash_window(field: FieldController, now: float) -> dict:
    """关闭清洗窗口，把镜子放回自动追日。"""

    closed = field.close_window(now)
    return {
        "steps": [
            {
                "component": "field",
                "action": "close_window",
                "detail": {"zone": closed.zone, "closed_at": closed.closed_at},
            }
        ],
        "violations": [],
        "zone": closed.zone,
    }


def wash_mirrors(
    field: FieldController,
    cycle: WashCycle,
    truck: WashTruck,
    drain: DrainSystem,
    mirrors: list[str],
    zone: str,
    now: float,
    dt: float = 1.0,
) -> dict:
    """按批次清洗镜面，返回带步骤与违约项的报告。"""

    steps: list[dict] = []
    violations: list[str] = []

    opening = open_wash_window(field, mirrors, zone, now)
    steps.extend(opening["steps"])
    violations.extend(opening["violations"])
    batch = opening["mirrors"]

    washed = 0
    for mirror in batch:
        result = wash_one_mirror(cycle, truck, drain, mirror, dt)
        steps.extend(result["steps"])
        violations.extend(result["violations"])
        if result["ok"]:
            washed += 1

    closing = close_wash_window(field, now)
    steps.extend(closing["steps"])

    return {
        "ok": not violations,
        "steps": steps,
        "violations": violations,
        "washed": washed,
        "mirrors": len(batch),
    }
