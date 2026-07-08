from datetime import datetime
from typing import Callable

from pytestflow.core.context import ptf_context
from pytestflow.core.core import StepWrapper
from pytestflow.core.pytestflow_states import PyTestflowDone, PyTestflowError
from pytestflow.core.utils import get_data_for_gui
from pytestflow.steps.common import get_metadata_from_prefect_context


class FlowControlStep(StepWrapper):
    def __init__(self, fn, *, next_steps={0:"next", 1:"next"}, name=None, store_as=None, autowire=True, **task_kwargs):
        super().__init__(fn, name=name, autowire=autowire, **task_kwargs)        
        self.next_steps = next_steps
        self.store_as = store_as
        self.step_type = "flow_control"

    def _run(self, *args, **kwargs):
        try:
            # Execute user code inside the Prefect task created by StepWrapper.
            result = super()._run(*args, **kwargs)

            if self.store_as:
                ptf_context.locals[self.store_as] = result

            actual_next_steps = self.next_steps.get(result, "next")
            ptf_context.locals["_ptf_next_step"] = actual_next_steps

            result_data = {
                "step_status": "done",
                "output": result,
                "step_type": self.step_type,
                "pytestflow_timestamp": datetime.utcnow().isoformat(),
                "store_as": self.store_as,
                "flow_control_next_steps":self.next_steps,
                "selected_next_step":actual_next_steps,
                "retry_n": ptf_context.locals.get("retry_n"),
            }
            result_data.update(self.get_meta_info())
            result_data.update(get_metadata_from_prefect_context())

            get_data_for_gui(self, result_data.get("end_time"), result_data)

            return PyTestflowDone(
                ptf_result=result_data,
                message="Flow control step completed",
            )
        except Exception as exc:
            result_data = {
                "step_status": "error",
                "error": str(exc),
                "step_type": self.step_type,
                "pytestflow_timestamp": datetime.utcnow().isoformat(),
                "store_as": self.store_as,                
                "retry_n": ptf_context.locals.get("retry_n"),
            }
            result_data.update(self.get_meta_info())
            result_data.update(get_metadata_from_prefect_context())

            return PyTestflowError(
                ptf_result=result_data,
                message=f"Flow control step `{self.name}` failed",
            )


def flow_control_step(*, next_steps={0:"next", 1:"next"}, name=None, store_as=None, autowire=True, **task_kwargs):
    """
    Decorator factory for control-only steps that steer sequence execution.
    """

    def decorator(fn: Callable):
        return FlowControlStep(
            fn,
            name=name,
            next_steps=next_steps,
            store_as=store_as,
            autowire=autowire,
            **task_kwargs,
        )

    return decorator
