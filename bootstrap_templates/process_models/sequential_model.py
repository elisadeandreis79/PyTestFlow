from pytestflow.core.sequence import Sequence
from pytestflow.steps.action_step import action_step
from pytestflow.core.context import ptf_context
from pytestflow.steps.flow_control import flow_control_step
from pytestflow.steps.message_pop_up import message_pop_up_step
from pytestflow.backend.report_manager import report_manager
import sys
from pathlib import Path

PROCESS_MODELS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PROCESS_MODELS_DIR))

from bootstrap_templates.process_models.reporting.html_report import report_callback_jinja


# ---------------------
# DEFAULT CALLBACKS
# ---------------------

# ✅ Pre-UUT: ask user to enter SN
@message_pop_up_step(
    show_response_box=True,
    title="User Input",
    msg="Enter Serial Number",
    buttons=["Run", "Cancel"],
    name="pre_uut_callback"
)
def pre_uut_callback():
    pass  # logic handled by decorator

# ✅ Post-UUT: (placeholder)
@action_step(name="post_uut_callback")
def post_uut_callback():
    print("📝 Post-UUT executed.")
    pass


# ✅ Database logging (placeholder)
@action_step(name="database_logging_callback")
def database_logging_callback():
    print("💾 Database logging executed.")
    return "DB logging done"


DEFAULT_CALLBACKS = {
    "pre_uut": pre_uut_callback,
    "main_sequence": None,           # mandatory
    "post_uut": post_uut_callback,
    "report": report_callback_jinja,
    "database_logging": database_logging_callback,
}


class SequentialProcessModel(Sequence):
    def __init__(
        self,
        name: str = "SequentialProcessModel",
        callbacks: dict | None = None,
    ):
        # Validate main sequence presence
        callbacks = callbacks or {}
        # Merge with defaults
        self.callbacks = {**DEFAULT_CALLBACKS, **callbacks}
        self._pre_uut_step_name = None

        assert self.callbacks["main_sequence"] is not None, "Main Seq callback must be provided."

        pre_uut_step = self.callbacks.get("pre_uut")
        if pre_uut_step is not None:
            self._pre_uut_step_name = getattr(pre_uut_step, "name", getattr(pre_uut_step, "__name__", "pre_uut"))

        @flow_control_step(name="pre_uut_flow_gate", next_steps={0: "end", 1: "next"})
        def pre_uut_flow_gate():
            user_response = ptf_context.locals.get("_pre_uut_user_response")
            button = self._extract_button_from_response(user_response)
            return button == "run"

        steps = []
        if pre_uut_step is not None:
            steps.append(pre_uut_step)
            steps.append(pre_uut_flow_gate)

        main_sequence = self.callbacks["main_sequence"]

        @action_step(name="main_sequence_step", store_as="main_results")
        def main_sequence_step():
            if isinstance(main_sequence, Sequence):
                main_state = main_sequence.run(return_state=True)
            elif hasattr(main_sequence, "run"):
                main_state = main_sequence.run(return_state=True)
            else:
                try:
                    main_state = main_sequence(return_state=True)
                except TypeError:
                    main_state = main_sequence()

            # Backward compatibility for existing integrations that still read `main_result`.
            ptf_context.locals["main_result"] = main_state
            ptf_context.locals["__last_result__"] = main_state
            return main_state

        steps.append(main_sequence_step)
        if self.callbacks.get("post_uut") is not None:
            steps.append(self.callbacks["post_uut"])
        if self.callbacks.get("report") is not None:
            steps.append(self.callbacks["report"])
        if self.callbacks.get("database_logging") is not None:
            steps.append(self.callbacks["database_logging"])

        # Build Sequence
        super().__init__(name=name, steps=steps, apply_throttle=False)

    def _extract_button_from_response(self, user_response) -> str | None:
        if isinstance(user_response, dict):
            button = user_response.get("button")
            if button is not None:
                return str(button).strip().lower()
        if user_response is None:
            return None
        return str(user_response).strip().lower()

    def _extract_pre_uut_user_response(self, state):
        ptf_result = getattr(state, "ptf_result", {}) or {}
        if "user_response" in ptf_result:
            return ptf_result.get("user_response")

        output = ptf_result.get("output")
        if isinstance(output, dict):
            if "user_response" in output:
                return output.get("user_response")
            if "button" in output:
                return output
        return output

    def _exec_step(self, step_fn):
        name, state = super()._exec_step(step_fn)
        if self._pre_uut_step_name and name == self._pre_uut_step_name:
            ptf_context.locals["_pre_uut_user_response"] = self._extract_pre_uut_user_response(state)
        return name, state

PROCESS_MODEL = SequentialProcessModel