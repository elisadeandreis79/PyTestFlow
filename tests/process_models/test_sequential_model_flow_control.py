import pytest

from pytestflow.core.context import ptf_context
from pytestflow.core.pytestflow_states import PyTestflowPassed
from pytestflow.core.runtime_control import runtime_control
from pytestflow.core.sequence import TestSequence
from bootstrap_templates.process_models.sequential_model import SequentialProcessModel
from pytestflow.steps.action_step import action_step


@pytest.fixture(autouse=True)
def clear_context():
    ptf_context.locals.clear()
    ptf_context.globals.clear()
    runtime_control.set_paused(False)
    runtime_control.set_stop_requested(False)
    runtime_control.set_throttle_ms(5.0)
    yield
    ptf_context.locals.clear()
    ptf_context.globals.clear()
    runtime_control.set_paused(False)
    runtime_control.set_stop_requested(False)
    runtime_control.set_throttle_ms(5.0)


@action_step(name="pre_uut_cancel")
def pre_uut_cancel():
    return {"button": "cancel"}


@action_step(name="pre_uut_run")
def pre_uut_run():
    return {"button": "run"}


@action_step(name="main_sequence_step", store_as="main_ran")
def main_sequence_step():
    return True


@action_step(name="post_uut_step", store_as="post_ran")
def post_uut_step():
    return True


def test_sequential_model_cancel_ends_before_main():
    model = SequentialProcessModel(
        name="pm_cancel",
        callbacks={
            "pre_uut": pre_uut_cancel,
            "main_sequence": main_sequence_step,
            "post_uut": post_uut_step,
            "report": None,
            "database_logging": None,
        },
    )

    result = model.run(return_state=True)

    assert isinstance(result, PyTestflowPassed)
    assert [name for name, _ in result.children] == ["pre_uut_cancel", "pre_uut_flow_gate"]
    assert "main_ran" not in ptf_context.locals
    assert "post_ran" not in ptf_context.locals


def test_sequential_model_run_executes_main_and_post():
    model = SequentialProcessModel(
        name="pm_run",
        callbacks={
            "pre_uut": pre_uut_run,
            "main_sequence": main_sequence_step,
            "post_uut": post_uut_step,
            "report": None,
            "database_logging": None,
        },
    )

    result = model.run(return_state=True)

    assert isinstance(result, PyTestflowPassed)
    assert [name for name, _ in result.children] == [
        "pre_uut_run",
        "pre_uut_flow_gate",
        "main_sequence_step",
        "post_uut_step",
    ]
    assert ptf_context.locals["main_ran"] is True
    assert ptf_context.locals["post_ran"] is True


def test_sequential_model_does_not_throttle_orchestration_but_throttles_main_sequence(monkeypatch):
    sleep_calls = []

    def fake_sleep(delay_seconds):
        sleep_calls.append(delay_seconds)

    monkeypatch.setattr(runtime_control, "sleep_with_pause", fake_sleep)
    runtime_control.set_throttle_ms(50.0)

    @action_step(name="mb_main_1", store_as="mb_main_1")
    def mb_main_1():
        return True

    @action_step(name="mb_main_2", store_as="mb_main_2")
    def mb_main_2():
        return True

    motherboard_test_sequence = TestSequence(
        name="motherboard_test",
        main_steps=[mb_main_1, mb_main_2],
    )

    model = SequentialProcessModel(
        name="pm_run_with_motherboard_sequence",
        callbacks={
            "pre_uut": pre_uut_run,
            "main_sequence": motherboard_test_sequence,
            "post_uut": post_uut_step,
            "report": None,
            "database_logging": None,
        },
    )

    result = model.run(return_state=True)

    assert isinstance(result, PyTestflowPassed)
    assert len(sleep_calls) == 1
