# PyTestFlow

PyTestFlow is a Python test executive built on top of Prefect.
It turns decorated Python functions into traceable test steps and runs them in
ordered flows (`Sequence`, `TestSequence`, and process models).

> Requires `prefect>=3.4` and Python 3.10+.

## Repositories

- Engine: https://github.com/Alberto-Manzoni/PyTestFlow
- Frontend: https://github.com/Alberto-Manzoni/PyTestFlow-FrontEnd

## Installation

Install in this order, from most common user path to least common.

### Stage 1: `pip install` user (recommended for most users)

Use this if you already manage your own Python environments and dependencies.

1. Create and activate your environment.

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/macOS
source .venv/bin/activate
```

2. Upgrade packaging tools and install runtime requirements.

```bash
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

Optional reporting extension:

```bash
pip install jinja2
```

This enables the opt-in Jinja HTML report callback/template pipeline while
keeping the legacy HTML reporter unchanged.

3. Install PyTestFlow.


```bash
python -m pip install "git+https://github.com/Alberto-Manzoni/PyTestFlow.git@main"
```

### Stage 2: `pipx install` user (one-command app install)

Use this if you want the fastest zero-friction install with isolated dependencies.

```bash
pipx install "git+https://github.com/Alberto-Manzoni/PyTestFlow.git@v0.2.0-beta.1"
```

### Stage 3: Developer install (for contributors)

Use this if you plan to actively develop and contribute to the codebase.

```bash
git clone https://github.com/Alberto-Manzoni/PyTestFlow.git
cd PyTestFlow
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/macOS
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
```

`pytestflow-am` starts the backend and serves bundled frontend assets.

## CLI quick start

```bash
pytestflow-am start
```

On first run, PyTestFlow creates a user workspace and prints clear setup feedback
for each folder it creates (for example `examples`, `process models`,
`test sequences`, `test reports`, and `custom step types`).

Optional:

```bash
pytestflow-am start --open
pytestflow-am quickstart
```

If needed, set `PYTESTFLOW_FRONTEND_DIR` to a folder that contains `index.html`.

## Usage

```python
from pytestflow.core import Sequence, ptf_context, step
from pytestflow.steps import numeric_limit_step, pass_fail_step
from pytestflow.process_models.sequential_model import SequentialProcessModel


@step(name="setup_environment")
def setup_environment():
    ptf_context.globals["station"] = "demo-bench"
    return "ready"


@numeric_limit_step(name="measure_vdd", limit=[1.15, 1.25], mode="between")
def measure_vdd():
    return 1.2


@pass_fail_step(name="functional_check")
def functional_check():
    return True


main_sequence = Sequence(
    name="demo_sequence",
    steps=[setup_environment, measure_vdd, functional_check],
    default_parameters={"dut": "DUT-42"},
)

process_model = SequentialProcessModel(
    name="demo_process_model",
    callbacks={
        "main_sequence": main_sequence,
        "pre_uut": None,
        "post_uut": None,
        "report": None,
        "database_logging": None,
    },
)

overall_state = process_model.run()
print(overall_state.ptf_result)
for step_name, child_state in overall_state.children:
    print(step_name, child_state.ptf_result)
```

## Core concepts

- Steps are Prefect tasks via `@step` and specialized decorators.
- `Sequence` and `TestSequence` are Prefect flows that aggregate child states.
- `ptf_context` shares `globals`, `locals`, `results`, and `current_step`.
- `SequentialProcessModel` orchestrates callbacks in this order:
  `pre_uut -> main_sequence -> post_uut -> report -> database_logging`.
- Process model main output is stored as both `main_results` and `main_result`
  for compatibility.

## Project structure

1. `pytestflow/core`: wrappers, context, states, sequence execution.
2. `pytestflow/steps`: built-in step decorators.
3. `pytestflow/flow_utils`: flow helper utilities.
4. `pytestflow/reporting`: HTML/JSON reporting helpers.

## Testing

```bash
PYTHONPATH=. pytest
```

Coverage:

```bash
PYTHONPATH=. pytest --cov=pytestflow --cov-report=term-missing
```

See [tests/test_coverage.md](tests/test_coverage.md) for a module-by-module map.
