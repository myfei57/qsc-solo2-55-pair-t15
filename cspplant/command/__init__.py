"""命令网关：幂等提交、状态机门前拒绝、长动作任务与统一审计口径。

公开三件东西：

* :class:`~cspplant.command.policy.ActionPolicy` / :data:`POLICIES`：每个动作的
  状态策略——状态不满足时是当场拒绝（``reject``）还是排队等候（``queue``）；
* :class:`~cspplant.command.ledger.IdempotencyLedger`：以提交标识为键的幂等账本；
* :class:`~cspplant.command.gateway.CommandGateway`：``submit / job / jobs``
  接口，同步动作当场出结果，长动作（清洗）落任务、后台逐面推进。

长动作的进度获取方式在此定死：**只支持轮询**（``GET /api/jobs/{id}``，控制台
每 2 秒轮一次），不提供 WebSocket / SSE 推送。现场控制台与控制服务之间不维持
长连接，代理与鉴权都按普通请求处理，重启后轮询自然恢复，不需要做推送会话的
断线重放。
"""

from __future__ import annotations

from .gateway import CommandGateway, JobHandle
from .ledger import IdempotencyLedger
from .policy import POLICIES, ActionPolicy, check_precondition

__all__ = [
    "CommandGateway",
    "JobHandle",
    "IdempotencyLedger",
    "ActionPolicy",
    "POLICIES",
    "check_precondition",
]
