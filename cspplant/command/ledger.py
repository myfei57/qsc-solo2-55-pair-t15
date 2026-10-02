"""幂等账本。

控制台点快一点或者网络抖一下，同一个动作会带着同一个提交标识重复到达。账本
以 ``request_id`` 为键做"一次声称、一次执行、重复回放"：

1. 第一次提交写入 ``claimed`` 占位并落盘，并发的同标识提交在条件变量上等待；
2. 执行结束后写入终态（成功 / 拒绝），后来者直接回放同一份结果；
3. 进程在执行中途掉电，落盘记录停在 ``claimed``——重启后这个标识按
   "前序结果未知"拒绝重放，调用方必须换一个新标识重提，避免在不确定现场是否
   已经动作过的情况下再执行一遍。

同标识但动作或参数指纹不同的提交一律冲突拒绝，防止同一个标识被拿去提交两件
不同的事。账本本身只做记账，参数指纹由调用方（网关）计算后传入。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from ..errors import RejectedError

# 终态值。``claimed`` 只是执行中的占位，不是终态。
CLAIMED = "claimed"
SUCCEEDED = "succeeded"
REJECTED = "rejected"
INDETERMINATE = "indeterminate"


@dataclass
class LedgerEntry:
    """一条幂等账本记录。"""

    request_id: str
    action: str
    fingerprint: str
    state: str
    status_code: int = 0
    payload: dict | None = None
    rejection: dict | None = None
    replay: bool = False

    def as_dict(self) -> dict:
        return {
            "request_id": self.request_id,
            "action": self.action,
            "fingerprint": self.fingerprint,
            "state": self.state,
            "status_code": self.status_code,
            "payload": self.payload,
            "rejection": self.rejection,
        }


class IdempotencyLedger:
    """以提交标识为键的内存幂等账本（带落盘副本）。"""

    def __init__(self, store, doc_id: str = "command/idempotency") -> None:
        self._store = store
        self._doc_id = doc_id
        self._entries: dict[str, LedgerEntry] = {}
        self._waiting: dict[str, threading.Condition] = {}
        self._lock = threading.RLock()
        self._restore()

    def _restore(self) -> None:
        """从落盘副本恢复；执行中（claimed）的记录按结果未知处理。"""

        raw = self._store.read(self._doc_id)
        if not isinstance(raw, dict):
            return
        for request_id, item in raw.items():
            if not isinstance(item, dict):
                continue
            state = str(item.get("state", CLAIMED))
            if state == CLAIMED:
                state = INDETERMINATE
            self._entries[request_id] = LedgerEntry(
                request_id=request_id,
                action=str(item.get("action", "")),
                fingerprint=str(item.get("fingerprint", "")),
                state=state,
                status_code=int(item.get("status_code", 0)),
                payload=item.get("payload") if isinstance(item.get("payload"), dict) else None,
                rejection=item.get("rejection") if isinstance(item.get("rejection"), dict) else None,
            )

    def _persist_locked(self) -> None:
        self._store.write(
            self._doc_id, {request_id: entry.as_dict() for request_id, entry in self._entries.items()}
        )

    def claim(self, request_id: str, action: str, fingerprint: str) -> LedgerEntry:
        """声称一个提交标识。

        - 新标识：写入 ``claimed`` 占位，返回该占位（``state == claimed``）；
        - 已有终态：标记为回放并返回（``replay is True``）；
        - 同标识正在执行：阻塞到执行结束后返回终态（回放）；
        - 动作或参数指纹不一致：冲突拒绝；
        - 上次执行中途进程退出（结果未知）：拒绝重放。
        """

        with self._lock:
            existing = self._entries.get(request_id)
            if existing is None:
                entry = LedgerEntry(request_id, action, fingerprint, CLAIMED)
                self._entries[request_id] = entry
                self._waiting[request_id] = threading.Condition(self._lock)
                self._persist_locked()
                return entry
            if existing.action != action or existing.fingerprint != fingerprint:
                raise RejectedError(
                    f"提交标识 {request_id} 已用于 {existing.action}，不能再提交 {action}",
                    action=action,
                    stage="idempotency",
                    code="idempotency_key_conflict",
                    blocked_by=request_id,
                )
            condition = self._waiting.get(request_id)
            if condition is not None:
                # 分段短等：首个提交通常很快结束，避免每个重复提交都白等一个长超时。
                deadline_steps = 150
                while existing.state == CLAIMED and deadline_steps > 0:
                    condition.wait(timeout=0.2)
                    existing = self._entries[request_id]
                    deadline_steps -= 1
            existing = self._entries[request_id]
            if existing.state == CLAIMED:
                # 等待超时仍在执行：不能放行第二次执行，按未知处理。
                raise RejectedError(
                    f"提交标识 {request_id} 的前序提交仍在执行，结果未知",
                    action=action,
                    stage="idempotency",
                    code="idempotency_inflight",
                    blocked_by=request_id,
                )
            if existing.state == INDETERMINATE:
                raise RejectedError(
                    f"提交标识 {request_id} 的前序提交中断在执行中途，结果未知，请换新标识重提",
                    action=action,
                    stage="idempotency",
                    code="idempotency_indeterminate",
                    blocked_by=request_id,
                )
            existing.replay = True
            return existing

    def complete(self, entry: LedgerEntry, status_code: int, payload: dict) -> None:
        """把一次声称标记为成功终态并唤醒等待者。"""

        with self._lock:
            current = self._entries[entry.request_id]
            current.state = SUCCEEDED
            current.status_code = status_code
            current.payload = dict(payload)
            self._persist_locked()
            condition = self._waiting.pop(entry.request_id, None)
            if condition is not None:
                condition.notify_all()

    def reject(self, entry: LedgerEntry, status_code: int, rejection: dict) -> None:
        """把一次声称标记为拒绝终态并唤醒等待者。"""

        with self._lock:
            current = self._entries[entry.request_id]
            current.state = REJECTED
            current.status_code = status_code
            current.rejection = dict(rejection)
            self._persist_locked()
            condition = self._waiting.pop(entry.request_id, None)
            if condition is not None:
                condition.notify_all()

    def release(self, entry: LedgerEntry) -> None:
        """释放尚未产生现场副作用的声称（如请求体 400），允许同标识更正后重提。"""

        with self._lock:
            current = self._entries.get(entry.request_id)
            if current is None or current.state != CLAIMED:
                return
            self._entries.pop(entry.request_id, None)
            self._waiting.pop(entry.request_id, None)
            self._persist_locked()

    def get(self, request_id: str) -> LedgerEntry | None:
        with self._lock:
            return self._entries.get(request_id)

    def entries(self) -> list[dict]:
        """返回全部账本条目（审计/自检用）。"""

        with self._lock:
            return [entry.as_dict() for entry in self._entries.values()]
