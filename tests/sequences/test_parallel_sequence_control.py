from threading import Event

import pytest

from pytestflow.core import (
    ParallelSequenceEventKind,
    ParallelSequenceHandle,
    ParallelSequenceStart,
    ParallelSequenceWait,
    ptf_context,
    register_parallel_sequence_hook,
)
from pytestflow.core.pytestflow_states import (
    PyTestflowDone,
    PyTestflowError,
    PyTestflowFailed,
    PyTestflowPassed,
)
from pytestflow.core.sequence import Sequence, TestSequence as PtfTestSequence


@pytest.fixture(autouse=True)
def clear_context():
    ptf_context.locals.clear()
    ptf_context.globals.clear()
    ptf_context.results.clear()
    yield
    ptf_context.locals.clear()
    ptf_context.globals.clear()
    ptf_context.results.clear()


def test_start_runs_concurrently_and_wait_attaches_child_result():
    child_started = Event()
    parent_progressed = Event()
    lifecycle_events = []
    child_messages = []

    def child_step():
        child_started.set()
        if not parent_progressed.wait(timeout=5):
            return PyTestflowFailed(message="Parent did not progress concurrently")
        child_messages.extend(ptf_context.parallel_endpoint.drain_messages())
        ptf_context.parallel_endpoint.send("ack", {"received": True})
        return PyTestflowPassed()

    child = Sequence(name="child", steps=[child_step])
    start = ParallelSequenceStart(child, store_as="child_handle")
    wait = ParallelSequenceWait(start)

    def parent_step():
        if not child_started.wait(timeout=5):
            return PyTestflowFailed(message="Child did not start")
        ptf_context.locals["child_handle"].send(
            "command",
            {"action": "continue"},
        )
        parent_progressed.set()
        return PyTestflowPassed()

    unsubscribe = register_parallel_sequence_hook(lifecycle_events.append)
    try:
        parent = Sequence(
            name="parent",
            steps=[start, parent_step, wait],
        )
        result = parent.run(return_state=True)
    finally:
        unsubscribe()

    assert isinstance(result, PyTestflowPassed)
    assert [name for name, _ in result.children] == [
        "start_child",
        "parent_step",
        "wait_child",
    ]
    assert isinstance(result.children[0][1], PyTestflowDone)

    wait_result = result.children[2][1]
    assert isinstance(wait_result, PyTestflowPassed)
    assert wait_result.ptf_result["step_status"] == "parallel_sequence_completed"
    assert wait_result.ptf_result["joined"] is True
    assert len(wait_result.children) == 1
    assert wait_result.children[0][0] == "child"
    assert isinstance(wait_result.children[0][1], PyTestflowPassed)
    assert len(wait_result.children[0][1].children) == 1

    handle = ptf_context.locals["child_handle"]
    assert isinstance(handle, ParallelSequenceHandle)
    assert handle.joined is True
    assert handle.done() is True
    assert child_messages[0].kind == "command"
    assert child_messages[0].payload == {"action": "continue"}
    assert handle.drain_messages()[0].kind == "ack"
    assert [event.kind for event in lifecycle_events] == [
        ParallelSequenceEventKind.STARTED,
        ParallelSequenceEventKind.WAIT_COMPLETED,
    ]
    assert lifecycle_events[1].result is wait_result


def test_parallel_child_receives_snapshot_parameters_and_endpoint():
    observed = {}
    ptf_context.locals.update(
        {"serial": "parent-before", "nested": {"value": 1}}
    )
    ptf_context.globals.update(
        {"station": "station-before", "nested": {"value": 2}}
    )

    def inspect_child_context():
        observed["parameter"] = ptf_context.locals["serial"]
        observed["caller_serial"] = ptf_context.locals["__caller__"]["serial"]
        observed["global_station"] = ptf_context.globals["station"]
        observed["endpoint"] = ptf_context.parallel_endpoint
        ptf_context.locals["nested"]["child_only"] = True

        try:
            ptf_context.globals["station"] = "not-allowed"
        except TypeError:
            observed["globals_read_only"] = True
        return PyTestflowPassed()

    child = Sequence(
        name="context_child",
        steps=[inspect_child_context],
        default_parameters={"serial": "default", "nested": {}},
    )
    start = ParallelSequenceStart(
        child,
        parameters={"serial": "explicit", "nested": {}},
    )
    parent = Sequence(
        name="context_parent",
        steps=[start, ParallelSequenceWait(start)],
    )

    result = parent.run(return_state=True)

    assert isinstance(result, PyTestflowPassed)
    assert observed["parameter"] == "explicit"
    assert observed["caller_serial"] == "parent-before"
    assert observed["global_station"] == "station-before"
    assert observed["endpoint"] is not None
    assert observed["globals_read_only"] is True
    assert ptf_context.locals["serial"] == "parent-before"
    assert ptf_context.locals["nested"] == {"value": 1}
    assert ptf_context.globals["station"] == "station-before"
    assert ptf_context.globals["nested"] == {"value": 2}


def test_failed_child_makes_wait_and_parent_fail():
    def failing_child_step():
        return PyTestflowFailed(
            ptf_result={"step_status": "intentional_child_failure"}
        )

    child = Sequence(name="failing_child", steps=[failing_child_step])
    start = ParallelSequenceStart(child)
    parent = Sequence(
        name="parent",
        steps=[start, ParallelSequenceWait(start)],
    )

    result = parent.run(return_state=True)

    assert isinstance(result, PyTestflowFailed)
    wait_result = result.children[1][1]
    assert isinstance(wait_result, PyTestflowFailed)
    assert wait_result.ptf_result["step_status"] == "parallel_sequence_failed"
    assert wait_result.ptf_result["joined"] is True
    assert isinstance(wait_result.children[0][1], PyTestflowFailed)


def test_runner_exception_becomes_wait_error_and_preserves_exception():
    class CrashingSequence:
        name = "crashing_child"
        allow_parent_mutation = False

        def run(self):
            raise RuntimeError("child flow infrastructure crashed")

    start = ParallelSequenceStart(
        CrashingSequence(),
        store_as="crashing_handle",
    )
    parent = Sequence(
        name="parent",
        steps=[start, ParallelSequenceWait(start)],
    )

    result = parent.run(return_state=True)

    assert isinstance(result, PyTestflowFailed)
    wait_result = result.children[1][1]
    assert isinstance(wait_result, PyTestflowError)
    assert wait_result.ptf_result["step_status"] == "parallel_sequence_error"
    assert wait_result.ptf_result["joined"] is True

    child_error = wait_result.children[0][1]
    assert isinstance(child_error, PyTestflowError)
    assert isinstance(child_error.ptf_result["exception"], RuntimeError)
    assert "infrastructure crashed" in child_error.ptf_result["traceback"]
    assert ptf_context.locals["crashing_handle"].joined is True


def test_parallel_start_rejects_live_parent_mutation():
    child = Sequence(
        name="mutating_child",
        allow_parent_mutation=True,
    )

    with pytest.raises(ValueError, match="allow_parent_mutation=True"):
        ParallelSequenceStart(child)


def test_multiple_starts_have_independent_registry_entries():
    first_child = Sequence(
        name="first_child",
        steps=[lambda: PyTestflowPassed()],
    )
    second_child = Sequence(
        name="second_child",
        steps=[lambda: PyTestflowPassed()],
    )
    first_start = ParallelSequenceStart(
        first_child,
        store_as="first_handle",
    )
    second_start = ParallelSequenceStart(
        second_child,
        store_as="second_handle",
    )
    parent = Sequence(
        name="parent",
        steps=[
            first_start,
            second_start,
            ParallelSequenceWait("second_handle"),
            ParallelSequenceWait(first_start),
        ],
    )

    result = parent.run(return_state=True)

    assert isinstance(result, PyTestflowPassed)
    assert ptf_context.locals["first_handle"].joined is True
    assert ptf_context.locals["second_handle"].joined is True
    assert (
        ptf_context.locals["first_handle"].call_id
        != ptf_context.locals["second_handle"].call_id
    )


def test_timed_out_wait_does_not_join_and_later_wait_collects():
    release_child = Event()

    def child_step():
        if not release_child.wait(timeout=5):
            return PyTestflowFailed(message="Child was not released")
        return PyTestflowPassed()

    def release_step():
        release_child.set()
        return PyTestflowPassed()

    child = Sequence(name="slow_child", steps=[child_step])
    start = ParallelSequenceStart(child, store_as="slow_handle")
    parent = Sequence(
        name="parent",
        steps=[
            start,
            ParallelSequenceWait(start, timeout=0, name="first_wait"),
            release_step,
            ParallelSequenceWait(start, name="second_wait"),
        ],
    )

    result = parent.run(return_state=True)

    assert isinstance(result, PyTestflowFailed)
    first_wait = result.children[1][1]
    second_wait = result.children[3][1]
    assert isinstance(first_wait, PyTestflowError)
    assert first_wait.ptf_result["timed_out"] is True
    assert first_wait.ptf_result["joined"] is False
    assert isinstance(second_wait, PyTestflowPassed)
    assert second_wait.ptf_result["joined"] is True
    assert ptf_context.locals["slow_handle"].joined is True


def test_testsequence_can_start_and_wait_for_parallel_testsequence():
    child = PtfTestSequence(
        name="child_test_sequence",
        main_steps=[lambda: PyTestflowPassed()],
    )
    start = ParallelSequenceStart(child)
    parent = PtfTestSequence(
        name="parent_test_sequence",
        main_steps=[start, ParallelSequenceWait(start)],
    )

    result = parent.run(return_state=True)

    assert isinstance(result, PyTestflowPassed)
    assert result.ptf_result["step_status"] == "test_sequence_passed"
    assert isinstance(result.children[0][1], PyTestflowDone)
    assert isinstance(result.children[1][1], PyTestflowPassed)
    child_result = result.children[1][1].children[0][1]
    assert child_result.ptf_result["step_status"] == "test_sequence_passed"
