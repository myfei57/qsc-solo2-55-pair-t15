"""动作策略：状态不满足时当场拒绝还是排队，按动作定死。

每个动作登记一条 :class:`ActionPolicy`：

* ``REJECT``——状态不允许就**当场拒绝**，响应里带 ``stage``（卡在哪一步）、
  ``code``（稳定拒绝码）和现场状态，控制台可以直接把原因亮给操作员，不产生
  排队、不产生副作用；
* ``QUEUE``——状态暂时不满足是预期情形（清洗窗口一次只能开一个），任务进入
  队列，门空了自动执行，控制台在任务进度上看到排队位次。

这条策略是声明式的、写死在代码里的，不允许调用方按请求覆盖：同一动作的
语义不能这次拒绝下次排队，否则审计无法解释。
"""

from __future__ import annotations

from dataclasses import dataclass

REJECT = "reject"
QUEUE = "queue"


@dataclass(frozen=True)
class ActionPolicy:
    """一个动作的状态策略。"""

    action: str
    mode: str
    long_running: bool = False


# 动作策略总表。只有清洗排队（它本来就受"同一时刻只有一个清洗窗口"约束），
# 其余动作状态不满足一律当场拒绝。
POLICIES: dict[str, ActionPolicy] = {
    "track": ActionPolicy("track", REJECT),
    "circulate": ActionPolicy("circulate", REJECT),
    "changeover": ActionPolicy("changeover", REJECT),
    "pump": ActionPolicy("pump", REJECT),
    "calibrate": ActionPolicy("calibrate", REJECT),
    "manual": ActionPolicy("manual", REJECT),
    "manual_release": ActionPolicy("manual_release", REJECT),
    "manual_expire": ActionPolicy("manual_expire", REJECT),
    "trip": ActionPolicy("trip", REJECT),
    "reset": ActionPolicy("reset", REJECT),
    "dispatch": ActionPolicy("dispatch", REJECT),
    "disturb": ActionPolicy("disturb", REJECT),
    "wash": ActionPolicy("wash", QUEUE, long_running=True),
}


def policy_for(action: str) -> ActionPolicy:
    return POLICIES[action]


@dataclass(frozen=True)
class PreconditionVerdict:
    """状态机门前的检查结论。"""

    allowed: bool
    stage: str
    code: str
    message: str
    blocked_by: str = ""

    def as_dict(self) -> dict:
        return {
            "allowed": self.allowed,
            "stage": self.stage,
            "code": self.code,
            "message": self.message,
            "blocked_by": self.blocked_by,
        }


def _ok() -> PreconditionVerdict:
    return PreconditionVerdict(True, "ready", "ready", "状态允许")


def _reject(stage: str, code: str, message: str, blocked_by: str = "") -> PreconditionVerdict:
    return PreconditionVerdict(False, stage, code, message, blocked_by)


def check_precondition(action: str, platform, params: dict) -> PreconditionVerdict:
    """在执行**之前**检查动作的状态前置条件。

    检查只读状态、不碰设备；被拦下时现场一个字节都没动。返回的 ``stage`` 就是
    "卡在哪一步"，与任务进度、审计记录共用同一份措辞。
    """

    if action == "track":
        if platform.latch.latched():
            return _reject(
                "safety_latch",
                "latched",
                f"故障闩锁处于置位状态（来源 {platform.latch.source() or '-'}："
                f"{platform.latch.reason() or '-'}），禁止追日；先复位停堆",
                blocked_by="alarm.latch",
            )
        return _ok()

    if action == "circulate":
        if platform.latch.latched():
            return _reject("safety_latch", "latched", "故障闩锁置位中，禁止建立熔盐循环", "alarm.latch")
        if platform.receiver.header.state() == "admitted":
            return _reject("salt_admitted", "already_circulating", "熔盐已通进吸热器，无需重复建立循环", "receiver.header")
        if platform.salt.cold_pump.running():
            return _reject("cold_pump", "pump_already_running", "冷盐泵已在运行", "salt.cold_pump")
        return _ok()

    if action == "changeover":
        if platform.latch.latched():
            return _reject("safety_latch", "latched", "故障闩锁置位中，禁止切换冷热罐", "alarm.latch")
        if not platform.salt.cold_pump.running():
            return _reject("cold_pump", "cold_pump_not_running", "冷盐泵未运行，无法按顺序切换到热泵", "salt.cold_pump")
        if platform.salt.hot_pump.running():
            return _reject("hot_pump", "hot_pump_already_running", "热泵已在运行，两泵不得对顶", "salt.hot_pump")
        if platform.salt.hot_tank.headroom_kg() <= 0.0:
            return _reject("hot_tank", "hot_tank_full", "热盐罐已满，切换后无处容纳热盐", "salt.hot_tank")
        return _ok()

    if action == "pump":
        kind = str(params.get("action", "cold_start"))
        cold = platform.salt.cold_pump
        hot = platform.salt.hot_pump
        if kind == "cold_start" and cold.running():
            return _reject("cold_pump", "pump_already_running", "冷盐泵已在运行", "salt.cold_pump")
        if kind == "cold_stop" and not cold.running():
            return _reject("cold_pump", "pump_not_running", "冷盐泵未运行，无需停止", "salt.cold_pump")
        if kind == "hot_start":
            if cold.running():
                return _reject("cold_pump", "cold_pump_running", "冷泵仍在运行，须先完成冷热罐切换", "salt.cold_pump")
            if hot.running():
                return _reject("hot_pump", "pump_already_running", "热盐泵已在运行", "salt.hot_pump")
        if kind == "hot_stop" and not hot.running():
            return _reject("hot_pump", "pump_not_running", "热盐泵未运行，无需停止", "salt.hot_pump")
        return _ok()

    if action == "trip":
        if platform.latch.latched():
            return _reject(
                "safety_latch",
                "already_latched",
                f"停堆保护已动作（{platform.latch.reason() or '-'}），无需重复提交",
                "alarm.latch",
            )
        return _ok()

    if action == "reset":
        if not platform.latch.latched():
            return _reject("safety_latch", "not_latched", "故障闩锁未置位，无需复位", "alarm.latch")
        if not platform.latch.ready_to_reset(platform.clock.now()):
            remaining = platform.latch.limits.latch_reset_delay_s - (
                platform.clock.now() - platform.latch.tripped_at()
            )
            return _reject(
                "latch_cooldown",
                "cooldown",
                f"闩锁冷却中，还需 {max(remaining, 0.0):.1f} 秒才能复位",
                "alarm.latch",
            )
        return _ok()

    if action == "wash":
        # 清洗的"窗口被占"由队列消化，不算拒绝；这里只拦硬条件。
        mirror = params.get("mirrors")
        if isinstance(mirror, list) and not mirror:
            return _reject("batch", "empty_batch", "清洗批次不能为空", "field.sweeps")
        return _ok()

    # manual / manual_release / manual_expire / calibrate / dispatch / disturb
    # 的非法取值在参数解析阶段就是 400；现场侧若仍有顺序约束，由组件抛
    # InterlockError，网关把它翻译成同一口径的 409 拒绝。
    return _ok()
