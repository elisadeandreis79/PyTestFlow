from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from pytestflow.core import ptf_context  # ptf_context = PyTestFlow execution context

def test_context_locals_and_globals():
    # Set values
    ptf_context.locals["foo"] = 123
    ptf_context.globals["bar"] = "baz"

    assert ptf_context.locals["foo"] == 123
    assert ptf_context.globals["bar"] == "baz"

    # Reset and check empty
    ptf_context.locals.clear()
    ptf_context.globals.clear()

    assert ptf_context.locals == {}
    assert ptf_context.globals == {}


def test_parallel_context_is_an_isolated_read_only_snapshot():
    ptf_context.locals.clear()
    ptf_context.globals.clear()
    ptf_context.results.clear()
    ptf_context.current_step = "parent-step"

    ptf_context.locals.update({"serial": "before", "nested": {"value": 1}})
    ptf_context.globals.update({"station": "A", "nested": {"value": 2}})
    endpoint = object()

    branch = ptf_context.create_parallel_context(
        parameters={"serial": "explicit", "branch_only": True},
        parallel_endpoint=endpoint,
    )

    # Snapshot timing is the call to create_parallel_context(), not bind().
    ptf_context.locals["serial"] = "after"
    ptf_context.globals["station"] = "B"

    with ptf_context.bind(branch):
        assert ptf_context.locals["serial"] == "explicit"
        assert ptf_context.locals["branch_only"] is True
        assert ptf_context.locals["__caller__"]["serial"] == "before"
        assert ptf_context.globals["station"] == "A"
        assert ptf_context.parallel_endpoint is endpoint
        assert ptf_context.current_step is None
        assert ptf_context.results == {}

        with pytest.raises(TypeError):
            ptf_context.locals["__caller__"]["new_key"] = "not allowed"
        with pytest.raises(TypeError):
            ptf_context.globals["new_key"] = "not allowed"

        # Nested values are independently copied even though the top-level
        # snapshots are exposed as read-only mappings.
        ptf_context.locals["__caller__"]["nested"]["value"] = 10
        ptf_context.globals["nested"]["value"] = 20
        ptf_context.current_step = "branch-step"
        ptf_context.results["branch"].append({"ok": True})

    assert ptf_context.locals["serial"] == "after"
    assert ptf_context.locals["nested"]["value"] == 1
    assert ptf_context.globals["station"] == "B"
    assert ptf_context.globals["nested"]["value"] == 2
    assert ptf_context.current_step == "parent-step"
    assert ptf_context.results == {}

    ptf_context.locals.clear()
    ptf_context.globals.clear()
    ptf_context.results.clear()
    ptf_context.current_step = None


def test_parallel_contexts_do_not_bleed_between_threads():
    ptf_context.locals.clear()
    ptf_context.globals.clear()
    ptf_context.results.clear()
    ptf_context.locals["parent"] = "root"

    contexts = [
        ptf_context.create_parallel_context(parameters={"branch": branch})
        for branch in ("left", "right")
    ]
    barrier = Barrier(2)

    def run_branch(context):
        with ptf_context.bind(context):
            branch = ptf_context.locals["branch"]
            ptf_context.locals["value"] = branch
            ptf_context.current_step = f"{branch}-step"
            ptf_context.results["result"].append({"branch": branch})
            barrier.wait()
            return (
                dict(ptf_context.locals),
                ptf_context.current_step,
                list(ptf_context.results["result"]),
            )

    with ThreadPoolExecutor(max_workers=2) as executor:
        left_future = executor.submit(run_branch, contexts[0])
        right_future = executor.submit(run_branch, contexts[1])
        left = left_future.result(timeout=5)
        right = right_future.result(timeout=5)

    assert left[0]["value"] == "left"
    assert left[1] == "left-step"
    assert left[2] == [{"branch": "left"}]
    assert right[0]["value"] == "right"
    assert right[1] == "right-step"
    assert right[2] == [{"branch": "right"}]
    assert ptf_context.locals == {"parent": "root"}
    assert ptf_context.current_step is None
    assert ptf_context.results == {}

    ptf_context.locals.clear()


def test_parallel_context_rejects_parent_mutation_and_reserved_parameter():
    with pytest.raises(ValueError, match="allow_parent_mutation=True"):
        ptf_context.create_parallel_context(allow_parent_mutation=True)

    with pytest.raises(ValueError, match="reserved"):
        ptf_context.create_parallel_context(parameters={"__caller__": {}})


def test_parallel_context_accepts_a_custom_copy_function():
    copied_values = []

    def recording_copy(value):
        copied_values.append(value)
        return {
            "locals": dict(value["locals"]),
            "globals": dict(value["globals"]),
            "parameters": dict(value["parameters"]),
        }

    branch = ptf_context.create_parallel_context(
        parameters={"answer": 42},
        copy_fn=recording_copy,
    )

    assert len(copied_values) == 1
    assert branch.locals["answer"] == 42
