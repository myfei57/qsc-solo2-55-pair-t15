"""动作幂等、长任务队列与进度。"""

from .manager import (
    FAILED,
    PARTIALLY_FAILED,
    QUEUED,
    REJECTED,
    RUNNING,
    SUCCEEDED,
    ActionManager,
)

__all__ = [
    "ActionManager",
    "FAILED",
    "PARTIALLY_FAILED",
    "QUEUED",
    "REJECTED",
    "RUNNING",
    "SUCCEEDED",
]
