from pytestflow.core.sequence import TestSequence
from pytestflow.core import ptf_context
from pytestflow.steps.message_pop_up import message_pop_up_step
from pytestflow.steps.numeric_limit import numeric_limit_step
from pytestflow.steps.pass_fail import pass_fail_step
from pytestflow.steps.flow_control import flow_control_step


@message_pop_up_step(
    name="operator_confirmation",
    title="Operator Check",
    msg="Confirm fixture and DUT are connected, then press Continue.",
    buttons=["Continue", "Cancel"],
    show_response_box=False,
    store_as="operator_confirmation_response",
)
def operator_confirmation():
    return "Operator confirmation step executed"


@flow_control_step(name="check_operator_confirmation", next_steps={0: "end", 1: "next"})
def check_operator_confirmation():
    value = ptf_context.locals.get("operator_confirmation_response")
    return value["button"] == "Continue"


@pass_fail_step(name="power_good_check")
def power_good_check():     
    # Typical use case: a digital status pin or a single readiness condition.
    return True


@numeric_limit_step(name="vcore_voltage_check", limit=(1.0, 1.3), mode="between")
def vcore_voltage_check():
    # Typical use case: one scalar analog measurement against limits.
    return 1.15


@message_pop_up_step(
    name="operator_product_selection",
    title="Operator Product Selection",
    msg="Select Product Version",
    buttons=["Ver 1", "Ver 2", "Ver 3"],
    show_response_box=False,
    store_as="operator_product_selection_response",
)
def operator_product_selection():
    return "Operator product selection step executed"


@flow_control_step(name="check_operator_product_selection", next_steps={0: "ver1_test", 1: "ver2_test", 2: "ver3_test"})
def check_operator_product_selection():
    versions = ["Ver 1", "Ver 2", "Ver 3"]
    value = ptf_context.locals.get("operator_product_selection_response")
    idx = versions.index(value["button"])    
    return  idx


@numeric_limit_step(name="ver1_test", limit=(4.9, 5.1), mode="between")
def ver1_test():
    # Typical use case: one scalar analog measurement against limits.
    return 5.0


@numeric_limit_step(name="ver2_test", limit=(11.9, 12.1), mode="between")
def ver2_test():
    # Typical use case: one scalar analog measurement against limits.
    return 12.0


@numeric_limit_step(name="ver3_test", limit=(23.9, 24.1), mode="between")
def ver3_test():
    # Typical use case: one scalar analog measurement against limits.
    return 24.0


@flow_control_step(name="go_to_end_after_ver1", next_steps={0: "end"})
def go_to_end_after_ver1():
    return 0

@flow_control_step(name="go_to_end_after_ver2", next_steps={0: "end"})
def go_to_end_after_ver2():
    return 0


def main_sequence() -> TestSequence:
    return TestSequence(
        name="MessageBoxAndFlowControl",
        setup_steps=[],
        main_steps=[
            operator_confirmation,
            check_operator_confirmation,
            power_good_check,
            vcore_voltage_check,
            operator_product_selection,
            check_operator_product_selection,
            ver1_test,
            go_to_end_after_ver1,
            ver2_test,
            go_to_end_after_ver2,
            ver3_test,
        ],
        cleanup_steps=[],
    )


PROCESS_HOOKS = {
    "main_sequence": main_sequence,
}
