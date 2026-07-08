import importlib.util
import sys
from pathlib import Path
from prefect import flow
from prefect.context import get_run_context
from pytestflow.config.config_manager import ConfigManager

config = ConfigManager()
process_models_dir = config.get_path("process_models")

def load_process_model(process_models_dir: str, model_name: str):
    file_path = Path(process_models_dir) / f"{model_name}.py"

    spec = importlib.util.spec_from_file_location(
        f"user_{model_name}",
        file_path
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[f"user_{model_name}"] = module
    spec.loader.exec_module(module)

    return module.PROCESS_MODEL


@flow()
def run_sequence_file(process_model_callbacks, model_name):
    # Following line are because we cannot do @flow(name=test_sequence.name)
    ctx = get_run_context()

    ctx.flow_run.name = process_model_callbacks["main_sequence"].name

    my_proc_model = load_process_model(process_models_dir, model_name)

    process_model = my_proc_model(
        name=process_model_callbacks["main_sequence"].name,
        callbacks=process_model_callbacks
    )
    return process_model.run(return_state=True)
