from pytestflow.core.core import StepWrapper
from pytestflow.core.pytestflow_states import PyTestflowDone
from pytestflow.core.utils import get_data_for_gui
from pytestflow.steps.common import get_metadata_from_prefect_context
from typing import Callable
from pytestflow.core.runtime_control import runtime_control


class MessagePopUpStep(StepWrapper):
    def __init__(self, fn, *, show_response_box=False, title="", msg="", buttons=[], name=None, store_as=None, autowire=True, **task_kwargs):
        super().__init__(fn, name=name, autowire=autowire, **task_kwargs)
        self.step_type = "message_pop_up"
        self.show_response_box = show_response_box
        self.title = title
        self.msg = msg
        self.buttons = buttons
        self.store_as = store_as
        
    def _run(self, *args, **kwargs):
        # Executes inside the Prefect task created by StepWrapper
        value = super()._run(*args, **kwargs)
        popup_data = {
                "isResponseBoxVisible": self.show_response_box,
                "title": self.title,
                "msg": self.msg,
                "buttons": self.buttons
            }        
        user_response = runtime_control.show_popup(popup_data)
        
        if self.store_as:
            from pytestflow.core.context import ptf_context
            ptf_context.locals[self.store_as] = user_response

        result_data = {
            "step_status": "done",
            "step_type": self.step_type,
            "user_response": user_response
        }

        result_data.update(self.get_meta_info())
        result_data.update(get_metadata_from_prefect_context())

        # Send End data to GUI
        get_data_for_gui(self, result_data.get("end_time"), result_data)

        return (
            PyTestflowDone(ptf_result=result_data)
        )

def message_pop_up_step(*, show_response_box=False, title="TITLE_HERE", msg="MSG_HERE", buttons=["Ok"], name=None, store_as=None, autowire=True, **task_kwargs):
    """
    Decorator factory for message pop-up steps.
    """
    def decorator(fn: Callable):
        # IMPORTANT: already task-wrapped by StepWrapper
        return MessagePopUpStep(
            fn, 
            show_response_box=show_response_box,
            title=title, 
            msg=msg, 
            buttons=buttons, 
            name=name, 
            store_as=store_as,
            autowire=autowire,
            **task_kwargs)
    return decorator
