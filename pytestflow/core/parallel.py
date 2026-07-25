from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from queue import Empty, Queue
from threading import Condition, RLock
from time import monotonic
from traceback import format_exception
from typing import Any
from uuid import UUID, uuid4

from prefect.states import State

from pytestflow.core.pytestflow_states import PyTestflowError, PyTestflowState


class ParallelCallStatus(str, Enum):
    """Framework-level lifecycle of a parallel sequence call."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class ParallelMessage:
    """Small immutable envelope exchanged between caller and child sequence."""

    kind: str
    payload: Any
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    message_id: UUID = field(default_factory=uuid4)


class _ParallelMessageChannel:
    def __init__(self) -> None:
        self._to_child: Queue[ParallelMessage] = Queue()
        self._to_parent: Queue[ParallelMessage] = Queue()

    def send_to_child(self, kind: str, payload: Any) -> None:
        self._to_child.put(self._new_message(kind, payload))

    def send_to_parent(self, kind: str, payload: Any) -> None:
        self._to_parent.put(self._new_message(kind, payload))

    def drain_for_child(self) -> list[ParallelMessage]:
        return self._drain(self._to_child)

    def drain_for_parent(self) -> list[ParallelMessage]:
        return self._drain(self._to_parent)

    def _new_message(self, kind: str, payload: Any) -> ParallelMessage:
        if not isinstance(kind, str) or not kind.strip():
            raise ValueError("message kind must be a non-empty string")
        try:
            copied_payload = deepcopy(payload)
        except Exception as exc:
            raise ValueError("parallel message payload could not be copied") from exc
        return ParallelMessage(kind=kind, payload=copied_payload)

    @staticmethod
    def _drain(source: Queue[ParallelMessage]) -> list[ParallelMessage]:
        messages: list[ParallelMessage] = []
        while True:
            try:
                messages.append(source.get_nowait())
            except Empty:
                return messages


class ParallelSequenceEndpoint:
    """Communication endpoint made available inside the child context."""

    def __init__(self, channel: _ParallelMessageChannel) -> None:
        self._channel = channel

    def send(self, kind: str, payload: Any) -> None:
        self._channel.send_to_parent(kind, payload)

    def drain_messages(self) -> list[ParallelMessage]:
        return self._channel.drain_for_child()


class ParallelSequenceHandle:
    """
    Stable PyTestFlow interface around an internal Prefect future.

    Successful and failed resolution is cached, making repeated ``wait()`` and
    ``result()`` calls idempotent. A timeout is call-local and is not cached, so
    a later call can still collect the eventual sequence result.
    """

    def __init__(
        self,
        *,
        sequence_name: str,
        future: Any,
        call_id: UUID | None = None,
        prefect_task_run_id: UUID | None = None,
        channel: _ParallelMessageChannel | None = None,
    ) -> None:
        if not isinstance(sequence_name, str) or not sequence_name.strip():
            raise ValueError("sequence_name must be a non-empty string")
        if future is None or not callable(getattr(future, "result", None)):
            raise TypeError("future must provide a callable result() method")

        self.call_id = call_id or uuid4()
        self.sequence_name = sequence_name
        task_run_id = (
            prefect_task_run_id
            if prefect_task_run_id is not None
            else getattr(future, "task_run_id", None)
        )
        self.prefect_task_run_id = self._coerce_uuid(task_run_id)

        self._future = future
        self._channel = channel or _ParallelMessageChannel()
        self._condition = Condition(RLock())
        self._resolving = False
        self._resolved_result: PyTestflowState | None = None
        self._resolution_exception: Exception | None = None

    @classmethod
    def create(
        cls,
        *,
        sequence_name: str,
        future: Any,
        call_id: UUID | None = None,
        prefect_task_run_id: UUID | None = None,
    ) -> tuple[ParallelSequenceHandle, ParallelSequenceEndpoint]:
        """Create a handle and the matching child-side communication endpoint."""
        channel = _ParallelMessageChannel()
        handle = cls(
            sequence_name=sequence_name,
            future=future,
            call_id=call_id,
            prefect_task_run_id=prefect_task_run_id,
            channel=channel,
        )
        return handle, ParallelSequenceEndpoint(channel)

    @staticmethod
    def _coerce_uuid(value: Any) -> UUID | None:
        if value is None or isinstance(value, UUID):
            return value
        try:
            return UUID(str(value))
        except (TypeError, ValueError, AttributeError):
            return None

    def done(self) -> bool:
        with self._condition:
            if self._resolved_result is not None:
                return True

        state = self._future_state()
        if state is not None:
            is_final = getattr(state, "is_final", None)
            if callable(is_final):
                return bool(is_final())

        future_done = getattr(self._future, "done", None)
        return bool(future_done()) if callable(future_done) else False

    def wait(self, timeout: float | None = None) -> PyTestflowState:
        return self.result(timeout=timeout)

    def result(self, timeout: float | None = None) -> PyTestflowState:
        if timeout is not None and timeout < 0:
            return self._error_state(
                ValueError("timeout must be non-negative or None")
            )

        deadline = None if timeout is None else monotonic() + timeout
        with self._condition:
            if self._resolved_result is not None:
                return self._resolved_result

            while self._resolving:
                remaining = self._remaining(deadline)
                if remaining == 0:
                    return self._timeout_state(timeout)
                self._condition.wait(timeout=remaining)
                if self._resolved_result is not None:
                    return self._resolved_result

            self._resolving = True

        try:
            raw_result = self._future.result(timeout=self._remaining(deadline))
            resolved = self._normalize_result(raw_result)
        except TimeoutError as exc:
            return self._timeout_state(timeout, exc)
        except Exception as exc:
            resolved = self._error_state(exc)
            with self._condition:
                self._resolution_exception = exc
        finally:
            with self._condition:
                self._resolving = False
                self._condition.notify_all()

        with self._condition:
            if self._resolved_result is None:
                self._resolved_result = resolved
            return self._resolved_result

    def status(self) -> ParallelCallStatus:
        with self._condition:
            if self._resolved_result is not None:
                if self._resolution_exception is not None:
                    return ParallelCallStatus.FAILED
                return ParallelCallStatus.COMPLETED
            if self._resolving:
                return ParallelCallStatus.RUNNING

        state = self._future_state()
        if state is None:
            return ParallelCallStatus.PENDING
        if self._state_matches(state, "is_cancelled", "cancelled"):
            return ParallelCallStatus.CANCELLED
        if self._state_matches(state, "is_failed", "failed") or self._state_matches(
            state, "is_crashed", "crashed"
        ):
            return ParallelCallStatus.FAILED
        if self._state_matches(state, "is_completed", "completed"):
            return ParallelCallStatus.COMPLETED
        if self._state_matches(state, "is_running", "running"):
            return ParallelCallStatus.RUNNING
        return ParallelCallStatus.PENDING

    def send(self, kind: str, payload: Any) -> None:
        self._channel.send_to_child(kind, payload)

    def drain_messages(self) -> list[ParallelMessage]:
        return self._channel.drain_for_parent()

    @property
    def exception(self) -> Exception | None:
        """Original terminal infrastructure exception, when one occurred."""
        with self._condition:
            return self._resolution_exception

    def _future_state(self) -> Any | None:
        try:
            return getattr(self._future, "state", None)
        except Exception:
            return None

    @staticmethod
    def _remaining(deadline: float | None) -> float | None:
        if deadline is None:
            return None
        return max(0.0, deadline - monotonic())

    @staticmethod
    def _state_matches(state: Any, predicate: str, name: str) -> bool:
        check = getattr(state, predicate, None)
        if callable(check):
            return bool(check())
        return str(getattr(state, "name", "")).strip().lower() == name

    def _normalize_result(self, result: Any) -> PyTestflowState:
        if isinstance(result, PyTestflowState):
            return result

        future_state = self._future_state()
        if isinstance(future_state, PyTestflowState):
            return future_state

        if isinstance(result, State):
            nested_result = result.result(raise_on_failure=False)
            if isinstance(nested_result, PyTestflowState):
                return nested_result

        exc = TypeError(
            f"Parallel sequence '{self.sequence_name}' returned "
            f"{type(result).__name__}; expected PyTestflowState"
        )
        with self._condition:
            self._resolution_exception = exc
        return self._error_state(exc)

    def _timeout_state(
        self,
        timeout: float | None,
        exception: TimeoutError | None = None,
    ) -> PyTestflowError:
        exc = exception or TimeoutError(
            f"Timed out waiting for parallel sequence '{self.sequence_name}' "
            f"after {timeout} seconds"
        )
        return self._error_state(exc, timed_out=True)

    def _error_state(
        self,
        exception: Exception,
        *,
        timed_out: bool = False,
    ) -> PyTestflowError:
        exception_type = (
            f"{type(exception).__module__}.{type(exception).__qualname__}"
        )
        traceback_text = "".join(
            format_exception(type(exception), exception, exception.__traceback__)
        )
        return PyTestflowError(
            ptf_result={
                "step_status": "error",
                "step_type": "parallel_sequence",
                "call_id": str(self.call_id),
                "sequence_name": self.sequence_name,
                "infrastructure_error": True,
                "timed_out": timed_out,
                "error": str(exception),
                "exception": exception,
                "exception_type": exception_type,
                "exception_repr": repr(exception),
                "traceback": traceback_text,
            },
            message=(
                f"Parallel sequence '{self.sequence_name}' infrastructure "
                f"error: {exception}"
            ),
        )


class _ParallelSequenceRegistry:
    """Authoritative handle registry for one sequence execution."""

    def __init__(self, sequence_name: str) -> None:
        self.sequence_name = sequence_name
        self._handles: dict[UUID, ParallelSequenceHandle] = {}
        self._aliases: dict[str, UUID] = {}
        self._lock = RLock()

    def register(
        self,
        handle: ParallelSequenceHandle,
        *,
        store_as: str | None = None,
        locals_store: dict[str, Any] | None = None,
    ) -> ParallelSequenceHandle:
        if not isinstance(handle, ParallelSequenceHandle):
            raise TypeError("handle must be a ParallelSequenceHandle")
        if store_as is not None and (
            not isinstance(store_as, str) or not store_as.strip()
        ):
            raise ValueError("store_as must be a non-empty string or None")

        with self._lock:
            existing = self._handles.get(handle.call_id)
            if existing is not None and existing is not handle:
                raise ValueError(f"duplicate parallel call_id: {handle.call_id}")
            if store_as is not None:
                existing_id = self._aliases.get(store_as)
                if existing_id is not None and existing_id != handle.call_id:
                    raise ValueError(
                        f"parallel handle name '{store_as}' is already registered"
                    )

            self._handles[handle.call_id] = handle
            if store_as is not None:
                self._aliases[store_as] = handle.call_id
                if locals_store is not None:
                    locals_store[store_as] = handle
            return handle

    def get(
        self,
        reference: UUID | str | ParallelSequenceHandle,
    ) -> ParallelSequenceHandle:
        with self._lock:
            if isinstance(reference, ParallelSequenceHandle):
                call_id = reference.call_id
            elif isinstance(reference, UUID):
                call_id = reference
            elif isinstance(reference, str):
                call_id = self._aliases.get(reference)
                if call_id is None:
                    try:
                        call_id = UUID(reference)
                    except ValueError as exc:
                        raise KeyError(reference) from exc
            else:
                raise TypeError(
                    "parallel handle reference must be a UUID, name, or handle"
                )

            try:
                return self._handles[call_id]
            except KeyError as exc:
                raise KeyError(reference) from exc

    def all(self) -> tuple[ParallelSequenceHandle, ...]:
        with self._lock:
            return tuple(self._handles.values())

    def outstanding(self) -> tuple[ParallelSequenceHandle, ...]:
        return tuple(handle for handle in self.all() if not handle.done())
