from pytestflow.core.context import ptf_context
from pytestflow.core.pytestflow_states import PyTestflowDone
from pytestflow.steps.flow_control import flow_control_step


@flow_control_step(
    name="route_on_condition",
    store_as="route_decision",
    next_steps={"jump": "retry_step"},
)
def route_on_condition():
    ptf_context.locals["retry_n"] = 2
    return "jump"


def test_flow_control_step_reports_control_metadata_and_store_as():
    ptf_context.locals.clear()

    result = route_on_condition()

    assert isinstance(result, PyTestflowDone)
    assert result.ptf_result["step_type"] == "flow_control"
    assert result.ptf_result["selected_next_step"] == "retry_step"
    assert result.ptf_result["retry_n"] == 2
    assert result.ptf_result["output"] == "jump"
    assert ptf_context.locals["route_decision"] == "jump"
