"""平台统一异常类型。

现场要区分四类失败：动作顺序或联锁条件不满足（运行期可恢复，条件满足即可
继续）、机械位置超差（没对到位，需要重新定位）、配置或持久化失败（装配期
就该拦下的接线问题）、以及命名空间解析失败。审计流水按异常类型落盘，巡检
时才能一眼区分"条件没到"、"没对好位"和"接线不对"。
"""

from __future__ import annotations


class PlantError(Exception):
    """所有平台异常的基类，携带出错的组件名。"""

    def __init__(self, message: str, *, component: str = "plant") -> None:
        super().__init__(message)
        self.message = message
        self.component = component

    def as_record(self) -> dict:
        """转成审计流水可以直接落盘的一条记录。"""

        return {
            "type": type(self).__name__,
            "component": self.component,
            "message": self.message,
        }


class InterlockError(PlantError):
    """顺序或联锁条件不满足，动作被拒绝。"""


class AlignmentError(PlantError):
    """机械位置超出对齐公差。"""


class StorageError(PlantError):
    """持久化读写失败。"""


class ConfigError(PlantError):
    """配置缺失或取值非法。"""


class NamespaceError(PlantError):
    """命名空间重复注册或解析失败。"""


class RejectedError(PlantError):
    """动作在状态机门前被当场拒绝（尚未产生任何现场副作用）。

    与 :class:`InterlockError` 的区别在于这里一定带"卡在哪一步"：``stage`` 是
    机器可读的门位标识，``code`` 是稳定的拒绝码，``blocked_by`` 记录拦住动作的
    现场对象（例如未关闭的清洗窗口）。拒绝理由、任务进度与审计流水共用同一份
    字段口径，控制台展示和事后追查不会出现两套说法。
    """

    def __init__(
        self,
        message: str,
        *,
        component: str = "command",
        action: str = "",
        stage: str = "precondition",
        code: str = "precondition_failed",
        blocked_by: str = "",
    ) -> None:
        super().__init__(message, component=component)
        self.action = action
        self.stage = stage
        self.code = code
        self.blocked_by = blocked_by

    def as_record(self) -> dict:
        record = super().as_record()
        record.update(
            {
                "action": self.action,
                "stage": self.stage,
                "code": self.code,
                "blocked_by": self.blocked_by,
            }
        )
        return record
