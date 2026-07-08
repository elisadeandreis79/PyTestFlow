from pytestflow.core.sequence import TestSequence
from pytestflow.steps.pass_fail import pass_fail_step
from pytestflow.steps.numeric_limit import numeric_limit_step
from pytestflow.steps.df_numeric_limits import df_numeric_limits_step
from pytestflow.steps.string_check import string_check_step
from pytestflow.steps.waveform_limit import waveform_limit_step
from pytestflow.steps.action_step import action_step
from pytestflow.core import ptf_context
import pandas as pd
import time

# --------------------
# STEP DEFINITIONS
# --------------------

# --- Pass/Fail ---
@pass_fail_step(name="power_good_check")
def power_good_check():
    return True

@pass_fail_step(name="reset_deasserted_check")
def reset_deasserted_check():
    return False  # Let's say this fails

# --- Numeric Limits ---
@numeric_limit_step(name="vcore_voltage_check", limit=1.2, mode="le")
def vcore_voltage_check():
    time.sleep(1)
    ptf_context.current_step.add_additional_info("added sleep to test purpose - with Murali & Team", 1)
    return 1.1

@numeric_limit_step(name="pll_lock_time_check", limit=500, mode="le")
def pll_lock_time_check():
    return 700  # Fails

# --- DataFrame Numeric Limits ---
df_limits = pd.DataFrame([
    {"name": "Vdd", "limit": 1.0, "mode": "ge"},
    {"name": "Vss", "limit": -0.1, "mode": "le"},
])

@df_numeric_limits_step(limits_df=df_limits, name="supply_margin_check")
def supply_margin_check():
    return {"Vdd": 1.1, "Vss": -0.2}

@df_numeric_limits_step(limits_df=df_limits, name="supply_margin_fail")
def supply_margin_fail():
    return {"Vdd": 0.9, "Vss": -0.05}

# --- String Check ---
@string_check_step(name="serial_number_check", expected="MB123456", match="exact")
def serial_number_check():
    return "MB123456"

@string_check_step(name="lot_code_check", expected="LOT789", match="exact")
def lot_code_check():
    return "LOT123"  # Fails

# --- Waveform Check ---
lower = [(0, 0.5), (1, 0.5), (2, 0.5)]
upper = [(0, 2.5), (1, 2.5), (2, 2.5)]

@waveform_limit_step(name="clk_rise_check", lower_mask=lower, upper_mask=upper, mode="between")
def clk_rise_check():
    time.sleep(0.001)
    ptf_context.current_step.add_additional_info("This is a test for additiona info", "This info is addedd at runtime to this step")
    return {"x": [0, 1, 2], "y": [1.0, 1.5, 2.0]}

@waveform_limit_step(name="clk_rise_fail", lower_mask=lower, upper_mask=upper, mode="between")
def clk_rise_fail():
    time.sleep(0.001)
    return {"x": [0, 1, 2], "y": [0.1, 2.7, 2.9]}

# --- Action Step ---
@action_step(name="store_mb_info", store_as="mb_info")
def store_mb_info():
    return "Motherboard A1"

# --------------------
# SUBSEQUENCE (like a child sequence)
# --------------------
def create_power_on_subsequence() -> TestSequence:
    @pass_fail_step(name="power_on_pass")
    def power_on_pass():
        return True

    @numeric_limit_step(name="power_on_stabilization_time", limit=200, mode="le")
    def power_on_stabilization_time():
        return 150

    return TestSequence(
        name="PowerOnSequence",
        setup_steps=[],
        main_steps=[power_on_pass, power_on_stabilization_time],
        cleanup_steps=[],
    )



# --------------------
# MAIN MOTHERBOARD TEST SEQUENCE
# --------------------
def motherboard_sequence() -> TestSequence:
    return TestSequence(
        name="MotherboardTestSequence",
        setup_steps=[store_mb_info],
        main_steps=[
            serial_number_check,
            lot_code_check,
            vcore_voltage_check,
            pll_lock_time_check,
            supply_margin_check,
            supply_margin_fail,
            clk_rise_check,
            clk_rise_fail,
            reset_deasserted_check,
            power_good_check,
            create_power_on_subsequence(),  # This is the subsequence
        ],
        cleanup_steps=[],
    )

PROCESS_HOOKS = {
    "main_sequence": motherboard_sequence,
    # "report": report_sequence,  # opzionale
    # "pre_uut": pre_uut_sequence, ecc.
}
