from pytestflow.core.sequence import TestSequence
from pytestflow.core import ptf_context
from pytestflow.steps.action_step import action_step
from pytestflow.steps.df_numeric_limits import df_numeric_limits_step
from pytestflow.steps.message_pop_up import message_pop_up_step
from pytestflow.steps.numeric_limit import numeric_limit_step
from pytestflow.steps.pass_fail import pass_fail_step
from pytestflow.steps.string_check import string_check_step
from pytestflow.steps.waveform_limit import waveform_limit_step
import pandas as pd


@message_pop_up_step(
    name="operator_confirmation",
    title="Operator Check",
    msg="Confirm fixture and DUT are connected, then press Continue.",
    buttons=["Ok", "Other Option 1", "Other Option 2"],
    show_response_box=False,
)
def operator_confirmation():
    return "Operator confirmed bench is ready"


@pass_fail_step(name="power_good_check")
def power_good_check():
    # Typical use case: a digital status pin or a single readiness condition.
    return True


@numeric_limit_step(name="vcore_voltage_check", limit=(1.0, 1.3), mode="between")
def vcore_voltage_check():
    # Typical use case: one scalar analog measurement against limits.
    return 1.15


limits_df = pd.DataFrame(
    [
        {"name": "rail_1v0", "limit": (0.95, 1.05), "mode": "between"},
        {"name": "rail_3v3", "limit": (3.2, 3.4), "mode": "between"},
    ]
)


@df_numeric_limits_step(name="multi_rail_check", limits_df=limits_df)
def multi_rail_check():
    # Typical use case: validate a set of related channels at once.
    return {"rail_1v0": 1.01, "rail_3v3": 3.31}


@string_check_step(name="serial_format_check", expected="MB-2026-A01", match="exact")
def serial_format_check():
    # Typical use case: exact ID, lot code, firmware tag, or SKU checks.
    return "MB-2026-A01"


lower_mask = [(0, 0.2), (1, 0.2), (2, 0.2), (3, 0.2)]
upper_mask = [(0, 1.8), (1, 1.8), (2, 1.8), (3, 1.8)]


@waveform_limit_step(
    name="clk_waveform_mask_check",
    lower_mask=lower_mask,
    upper_mask=upper_mask,
    mode="between",
)
def clk_waveform_mask_check():
    ptf_context.current_step.add_additional_info(
        "typical_use_case",
        "Waveform limits are used, for example, in oscilloscope captures or spectrum captures to apply masks.",
    )
    return {"x": [0, 1, 2, 3], "y": [0.6, 0.9, 1.2, 1.1]}


@action_step(name="store_operator_note", store_as="operator_note")
def store_operator_note():
    # Typical use case: save runtime values for later steps/report sections.
    return "Quickstart run completed with one example per step type."


def main_sequence() -> TestSequence:
    return TestSequence(
        name="StepTypesQuickstartSequence",
        setup_steps=[],
        main_steps=[
            operator_confirmation,
            power_good_check,
            vcore_voltage_check,
            multi_rail_check,
            serial_format_check,
            clk_waveform_mask_check,
            store_operator_note,
        ],
        cleanup_steps=[],
    )


PROCESS_HOOKS = {
    "main_sequence": main_sequence,
}
