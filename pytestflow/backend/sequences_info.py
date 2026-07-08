import importlib
import pytestflow
from pytestflow.core.sequence import TestSequence
import sys
from pathlib import Path
import json
from pytestflow.backend.uuids_handler import resolve_uuids
from pytestflow.config.config_manager import ConfigManager
from pytestflow.steps.action_step import ActionStep # pyright: ignore[reportUnusedImport]
from pytestflow.steps.pass_fail import PassFailStep # pyright: ignore[reportUnusedImport]
from pytestflow.steps.numeric_limit import NumericLimitStep # pyright: ignore[reportUnusedImport]
from pytestflow.steps.string_check import StringCheckStep # pyright: ignore[reportUnusedImport]
from pytestflow.steps.df_numeric_limits import DFNumericLimitsStep # pyright: ignore[reportUnusedImport]
from pytestflow.steps.waveform_limit import WaveformLimitStep # pyright: ignore[reportUnusedImport]
from pytestflow.steps.message_pop_up import MessagePopUpStep # pyright: ignore[reportUnusedImport]
from pytestflow.steps.flow_control import FlowControlStep # pyright: ignore[reportUnusedImport]

config = ConfigManager()


def get_available_sequences():
    """Returns a list of available sequence names from the sequences folder."""
    sequences = []
    try:
        SEQUENCES_FOLDER = config.get_path("test_sequences")
        seq_folder = Path(SEQUENCES_FOLDER)
        if seq_folder.exists() and seq_folder.is_dir():
            for file in seq_folder.glob("*.py"):
                if file.name == "__init__.py":
                    continue
                sequences.append(file.stem)
    except ImportError:
        print("SEQUENCES_FOLDER not defined in config.yaml", file=sys.stderr)
    return sequences

def get_single_step(step):
    uuid_val = getattr(step, "static_uuid", None)
    return {
        "type": getattr(step, "step_type", "<unnamed type>"),
        "name": getattr(step, "name", "<unnamed step>"),
        "uuid": str(uuid_val) if uuid_val is not None else None
    }

def get_seq_structure_recursive(sequence):
    """Return a dict with separated Setup / Main / Cleanup steps."""
    def serialize_steps(step_list):
        result = []
        for step in step_list:
            # Prefect Task
            if isinstance(step, (
                ActionStep,
                PassFailStep,
                NumericLimitStep,
                StringCheckStep,
                DFNumericLimitsStep,
                WaveformLimitStep,
                MessagePopUpStep,
                FlowControlStep
            )):
                result.append(get_single_step(step))

            elif isinstance(step, TestSequence):
                result.append(get_seq_structure_recursive(step))         
            # unknown
            else:
                print(f"[Unknown type: {type(step)}] {step}")
        return result

   
    sequence_uuid = str(sequence.static_uuid)

    return {
    "type": "sub_sequence",   # ← PRIMA chiave
    "Name": getattr(sequence, "name", "<unnamed sequence>"),
    "uuid": sequence_uuid,
    "Setup": {"Steps": serialize_steps(getattr(sequence, "setup_steps", []))},
    "Main": {"Steps": serialize_steps(getattr(sequence, "main_steps", []))},
    "Cleanup": {"Steps": serialize_steps(getattr(sequence, "cleanup_steps", []))},
}

def get_seq_structure(sequence_name: str):
    try:
        SEQUENCES_FOLDER = config.get_path("test_sequences")
    except ImportError:
        return json.dumps({"error": "SEQUENCES_FOLDER not defined in config.py"})
    
    seq_path = Path(SEQUENCES_FOLDER) / f"{sequence_name}.py"
    if not seq_path.exists():
        return json.dumps({"error": f"Sequence file {sequence_name}.py not found"})
    
    spec = importlib.util.spec_from_file_location(sequence_name, seq_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, "PROCESS_HOOKS"):
        raise RuntimeError(
            f"PROCESS_HOOKS not defined in {seq_path}. "            
        )

    hooks = dict(module.PROCESS_HOOKS)  # copia difensiva

    if "main_sequence" not in hooks:
        raise RuntimeError("PROCESS_HOOKS['main_sequence'] is required")

    main_factory = hooks["main_sequence"]

    if not callable(main_factory):
        raise TypeError("PROCESS_HOOKS['main_sequence'] must be callable")
    
    # istanzia UNA VOLTA
    main_sequence = main_factory()

    if not isinstance(main_sequence, TestSequence):
        raise TypeError(
            "PROCESS_HOOKS['main_sequence'] must return a TestSequence"
        )

    # UUID risolti QUI
    resolve_uuids(main_sequence, main_factory)

    # sostituisci la factory con l'istanza
    hooks["main_sequence"] = main_sequence

    # struttura per la UI
    root = get_seq_structure_recursive(main_sequence)
    root["type"] = "MainSequence"

    return root, hooks   


