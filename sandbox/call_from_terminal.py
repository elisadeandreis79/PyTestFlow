from pytestflow.backend.sequences_info import get_seq_structure
from pytestflow.core.seq_file_runner import run_sequence_file


if __name__ == "__main__":


    try:
        seq_struct, hooks = get_seq_structure("basic_sequence")
        run_sequence_file(hooks, "sequential_model_jinja")
    except Exception as e:
        print(f"Error loading sequence structure: {e}")
        hooks = {}

    