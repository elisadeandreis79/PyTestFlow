from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
from uuid import uuid4

import pytest
from prefect import flow, task

from pytestflow.core import (
    ParallelCallStatus,
    ParallelMessage,
    ParallelSequenceHandle,
    ptf_context,
)
from pytestflow.core.pytestflow_states import PyTestflowError, PyTestflowPassed
from pytestflow.core.sequence import Sequence


class ImmediateFuture:
    def __init__(self, value=None, exception=None, state=None):
        self.value = value
        self.exception = exception
        self.state = state
        self.task_run_id = uuid4()
        self.calls = 0

    def result(self, timeout=None):
        self.calls += 1
        if self.exception is not None:
            raise self.exception
        return self.value


class BlockingFuture:
    def __init__(self, value):
        self.value = value
        self.started = Event()
        self.release = Event()
        self.lock = Lock()
        self.calls = 0
        self.state = None

    def result(self, timeout=None):
        with self.lock:
            self.calls += 1
        self.started.set()
        if not self.release.wait(timeout):
            raise TimeoutError("future did not complete in time")
        return self.value


class RunningState:
    def is_cancelled(self):
        return False

    def is_failed(self):
        return False

    def is_crashed(self):
        return False

    def is_completed(self):
        return False

    def is_running(self):
        return True

    def is_final(self):
        return False


def test_handle_wait_and_result_are_idempotent():
    expected = PyTestflowPassed(ptf_result={"step_status": "sequence_passed"})
    future = ImmediateFuture(expected)
    handle = ParallelSequenceHandle(sequence_name="child", future=future)

    first = handle.wait()
    second = handle.result()

    assert first is expected
    assert second is expected
    assert future.calls == 1
    assert handle.done() is True
    assert handle.status() is ParallelCallStatus.COMPLETED
    assert handle.prefect_task_run_id == future.task_run_id


def test_concurrent_waiters_share_one_future_resolution():
    expected = PyTestflowPassed(ptf_result={"step_status": "sequence_passed"})
    future = BlockingFuture(expected)
    handle = ParallelSequenceHandle(sequence_name="child", future=future)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(handle.result)
        assert future.started.wait(timeout=2)
        second = executor.submit(handle.wait)
        future.release.set()
        first_result = first.result(timeout=2)
        second_result = second.result(timeout=2)

    assert first_result is expected
    assert second_result is expected
    assert future.calls == 1


def test_infrastructure_exception_becomes_idempotent_error_state():
    original = ConnectionError("Prefect runner disconnected")
    future = ImmediateFuture(exception=original)
    handle = ParallelSequenceHandle(sequence_name="child", future=future)

    first = handle.result()
    second = handle.wait()

    assert isinstance(first, PyTestflowError)
    assert second is first
    assert future.calls == 1
    assert handle.exception is original
    assert handle.status() is ParallelCallStatus.FAILED
    assert first.ptf_result["exception"] is original
    assert first.ptf_result["exception_type"] == "builtins.ConnectionError"
    assert "Prefect runner disconnected" in first.ptf_result["traceback"]


def test_unexpected_future_result_becomes_error_state():
    handle = ParallelSequenceHandle(
        sequence_name="child",
        future=ImmediateFuture({"not": "a PyTestflowState"}),
    )

    result = handle.result()

    assert isinstance(result, PyTestflowError)
    assert isinstance(result.ptf_result["exception"], TypeError)
    assert handle.status() is ParallelCallStatus.FAILED


def test_timeout_is_not_cached_and_later_result_can_be_collected():
    expected = PyTestflowPassed(ptf_result={"step_status": "sequence_passed"})
    future = BlockingFuture(expected)
    handle = ParallelSequenceHandle(sequence_name="child", future=future)

    timed_out = handle.wait(timeout=0)
    future.release.set()
    completed = handle.result(timeout=2)

    assert isinstance(timed_out, PyTestflowError)
    assert timed_out.ptf_result["timed_out"] is True
    assert completed is expected
    assert future.calls == 2
    assert handle.status() is ParallelCallStatus.COMPLETED


def test_invalid_timeout_is_returned_as_an_error_state():
    handle = ParallelSequenceHandle(
        sequence_name="child",
        future=ImmediateFuture(PyTestflowPassed()),
    )

    result = handle.result(timeout=-1)

    assert isinstance(result, PyTestflowError)
    assert isinstance(result.ptf_result["exception"], ValueError)


def test_status_reflects_running_future_without_exposing_it():
    handle = ParallelSequenceHandle(
        sequence_name="child",
        future=ImmediateFuture(state=RunningState()),
    )

    assert handle.done() is False
    assert handle.status() is ParallelCallStatus.RUNNING


def test_handle_and_child_endpoint_exchange_copied_messages():
    handle, endpoint = ParallelSequenceHandle.create(
        sequence_name="child",
        future=ImmediateFuture(PyTestflowPassed()),
    )
    caller_payload = {"command": ["continue"]}
    child_payload = {"progress": [50]}

    handle.send("control", caller_payload)
    endpoint.send("progress", child_payload)
    caller_payload["command"].append("mutated")
    child_payload["progress"].append(100)

    child_messages = endpoint.drain_messages()
    parent_messages = handle.drain_messages()

    assert len(child_messages) == 1
    assert len(parent_messages) == 1
    assert isinstance(child_messages[0], ParallelMessage)
    assert child_messages[0].kind == "control"
    assert child_messages[0].payload == {"command": ["continue"]}
    assert parent_messages[0].kind == "progress"
    assert parent_messages[0].payload == {"progress": [50]}
    assert endpoint.drain_messages() == []
    assert handle.drain_messages() == []


def test_registry_is_execution_scoped_and_authoritative_over_locals():
    ptf_context.locals.clear()
    handle = ParallelSequenceHandle(
        sequence_name="child",
        future=ImmediateFuture(PyTestflowPassed()),
    )

    with ptf_context.sequence_execution("parent"):
        ptf_context.register_parallel_handle(handle, store_as="child_handle")
        assert ptf_context.locals["child_handle"] is handle

        # User locals may be overwritten, but authoritative lookup is isolated
        # in the private registry.
        ptf_context.locals["child_handle"] = "tampered"
        assert ptf_context.get_parallel_handle("child_handle") is handle
        assert ptf_context.get_parallel_handle(handle.call_id) is handle
        assert ptf_context.get_parallel_handle(str(handle.call_id)) is handle

        with pytest.raises(ValueError, match="already registered"):
            ptf_context.register_parallel_handle(
                ParallelSequenceHandle(
                    sequence_name="other",
                    future=ImmediateFuture(PyTestflowPassed()),
                ),
                store_as="child_handle",
            )

    with pytest.raises(RuntimeError, match="No active sequence execution"):
        ptf_context.get_parallel_handle("child_handle")
    ptf_context.locals.clear()


def test_sequence_run_creates_and_releases_its_registry():
    ptf_context.locals.clear()
    handle = ParallelSequenceHandle(
        sequence_name="child",
        future=ImmediateFuture(PyTestflowPassed()),
    )

    def register_handle():
        ptf_context.register_parallel_handle(handle, store_as="child_handle")
        return PyTestflowPassed()

    sequence = Sequence(name="registry_owner", steps=[register_handle])
    result = sequence.run(return_state=True)

    assert isinstance(result, PyTestflowPassed)
    assert ptf_context.locals["child_handle"] is handle
    with pytest.raises(RuntimeError, match="No active sequence execution"):
        ptf_context.get_parallel_handle("child_handle")
    ptf_context.locals.clear()


def test_handle_wraps_a_real_prefect_future():
    observed = {}

    @task(name="parallel-handle-test-task", persist_result=False)
    def child_task():
        return PyTestflowPassed(
            ptf_result={"step_status": "sequence_passed", "output": "done"}
        )

    @flow(name="parallel-handle-test-flow", persist_result=False)
    def parent_flow():
        handle = ParallelSequenceHandle(
            sequence_name="child",
            future=child_task.submit(),
        )
        observed["result"] = handle.result()

    parent_flow()

    assert isinstance(observed["result"], PyTestflowPassed)
    assert observed["result"].ptf_result["output"] == "done"
