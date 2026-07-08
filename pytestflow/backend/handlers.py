import asyncio
import traceback
from pytestflow.backend.event_bus import event_bus, pending_requests
from pytestflow.backend import sequences_info
from pytestflow.core.seq_file_runner import run_sequence_file
from pytestflow.core.runtime_control import runtime_control
from pytestflow.backend.report_manager import report_manager
from pytestflow.config.config_manager import ConfigManager
import sys
from pathlib import Path


class UserSession:
    def __init__(self):
        self.process_model_callbacks = None        

    def set_selected_process_model_callbacks(self, callbacks):
        self.process_model_callbacks = callbacks

    def get_selected_process_model_callbacks(self):
        return self.process_model_callbacks

    def set_selected_process_model(self, name):
        self.selected_process_model = name

    def get_selected_process_model(self):
        return self.selected_process_model

user_session = UserSession()


def get_available_process_models():
    """Returns a list of available process model names from the process_models folder."""
    process_models = []
    try:
        config = ConfigManager()
        PROCESS_MODELS_FOLDER = config.get_path("process_models")
        pm_folder = Path(PROCESS_MODELS_FOLDER)
        if pm_folder.exists() and pm_folder.is_dir():
            for file in pm_folder.glob("*.py"):
                if file.name == "__init__.py":
                    continue
                process_models.append(file.stem)
    except Exception as e:
        print(f"Error loading process models: {e}", file=sys.stderr)
    return process_models


async def handle_query_process_models(args):
    list_of_process_models = get_available_process_models()
    await event_bus.emit("outbound", {
        "cmd": "set_available_process_models",
        "args": list_of_process_models
    })

async def handle_select_process_model(args):
    model_name = args.get("name")
    if model_name:
        user_session.set_selected_process_model(model_name)
        await event_bus.emit("outbound", {
            "cmd": "process_model_selected",
            "args": model_name
        })

async def handle_query_seq_files(args):
    list_of_seq_files = sequences_info.get_available_sequences()
    await event_bus.emit("outbound", {
        "cmd": "set_available_seq_files",
        "args": list_of_seq_files
    })

async def handle_get_seq_structure(args):
    seq_struct, hooks = sequences_info.get_seq_structure(args["file_name"])
    user_session.set_selected_process_model_callbacks(hooks)
    await event_bus.emit("outbound", {
        "cmd": "display_seq_structure",
        "args": seq_struct
    })

async def handle_start_run(args):
    if True:
        try:
            process_model_callbacks = user_session.get_selected_process_model_callbacks()
            args = args or {}
            
            runtime_control.set_stop_requested(False)

            await event_bus.emit("outbound", {
                "cmd": "set_run_status",
                "args": "running"
            })

            await asyncio.to_thread(run_sequence_file, process_model_callbacks, user_session.get_selected_process_model())
            if report_manager.has_report():
                await event_bus.emit("outbound", {
                    "cmd": "report_generated",
                    "args": ""
                })

        except (KeyboardInterrupt, asyncio.CancelledError):
            print("⚠️ Cancellation detected - shutting down runtime")
            runtime_control.shutdown()
            raise

        finally:
            await event_bus.emit("outbound", {
                "cmd": "set_run_status",
                "args": "idle"
            })


async def handle_modal_response(args):    
    runtime_control.set_popup_response(
        args.get("request_id"),
        args.get("button_pressed"),
        args.get("response_text")
    )

async def _emit_runtime_control_updated():
    await event_bus.emit("outbound", {
        "cmd": "runtime_control_updated",
        "args": runtime_control.snapshot()
    })

async def handle_stop_requested(args):
    runtime_control.set_stop_requested(True)
    await _emit_runtime_control_updated()

async def handle_pause(args):
    runtime_control.set_paused(True)
    await _emit_runtime_control_updated()


async def handle_resume(args):
    runtime_control.set_paused(False)
    await _emit_runtime_control_updated()


async def handle_set_throttle(args):
    throttle_value = None
    if isinstance(args, dict):
        if "throttle_ms" in args:
            throttle_value = args["throttle_ms"]
        elif "throttle" in args:
            throttle_value = args["throttle"]
    else:
        throttle_value = args

    if throttle_value is None:
        raise ValueError("Missing throttle value. Expected 'throttle_ms'.")

    runtime_control.set_throttle_ms(float(throttle_value))
    await _emit_runtime_control_updated()


async def handle_get_runtime_control(args):
    await event_bus.emit("outbound", {
        "cmd": "runtime_control_updated",
        "args": runtime_control.snapshot()
    })

async def handle_open_last_report(args):
    last_report = report_manager.get_last_report()
    if not last_report:
        await event_bus.emit("outbound", {
            "cmd": "toast",
            "args": {
                "type": "error",
                "message": "No report available"
            }
        })
        return
    with open(last_report, "r", encoding="utf-8", errors="replace") as f:
        html = f.read()

    await event_bus.emit("outbound", {
        "cmd": "open_report_inline",
        "args": {
            "html": html
        }
    })


# This wraps handlers to catch exceptions and send error messages to frontend
def handler_guard(handler): 
    async def wrapped(args):
        try:
            await handler(args)
        except Exception as e:
            traceback.print_exc()

            await event_bus.emit("outbound", {
                "cmd": "backend_error",
                "args": {
                    "title": "Errore backend",                    
                    "message": str(e),
                    "severity": "error"
                }
            })
    return wrapped

def init_handlers():
    event_bus.on("query_process_models", handler_guard(handle_query_process_models))
    event_bus.on("select_process_model", handler_guard(handle_select_process_model))
    event_bus.on("query_seq_files", handler_guard(handle_query_seq_files))
    event_bus.on("get_seq_structure", handler_guard(handle_get_seq_structure))
    event_bus.on("start_run", handler_guard(handle_start_run))
    event_bus.on("user_response", handler_guard(handle_modal_response))
    event_bus.on("pause", handler_guard(handle_pause))
    event_bus.on("resume", handler_guard(handle_resume))
    event_bus.on("set_throttle", handler_guard(handle_set_throttle))
    event_bus.on("stop_requested", handler_guard(handle_stop_requested))
    event_bus.on("get_runtime_control", handler_guard(handle_get_runtime_control))
    event_bus.on("open_last_report", handler_guard(handle_open_last_report))
