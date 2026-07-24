# PyTestFlow Core

The `pytestflow.core` package hosts the building blocks of the framework: the
Prefect-integrated `Step` wrapper, shared execution context, sequence and
process-model flows, and the state objects returned by each step. These pieces
realize PyTestFlow's philosophy of **steps-as-tasks**, a **shared context**, and
**composable flows**.

See the [project README](../../readme.md) for an overview and
[steps documentation](../steps/README.md) for available step implementations.

## Context management

`ptf_context` is a singleton `TestContext` proxy. The public import remains
stable while access is routed to the `ExecutionContext` bound to the current
execution branch.

- `globals` – values shared across the entire process model run.
- `locals` – values scoped to the currently executing sequence.
- `results` – optional history of step execution results.
- `current_step` – pointer to the `Step` instance currently executing.

Steps can access the context implicitly through autowired parameters or
explicitly by importing `ptf_context`:

```python
from pytestflow.core import ptf_context, step


@step(name="load_config")
def load_config():
    ptf_context.globals["serial"] = "ABC123"
```

Code that launches a parallel branch first captures an isolated context and
then binds it around that branch's execution:

```python
branch_context = ptf_context.create_parallel_context(
    parameters={"serial": "ABC123"},
)

with ptf_context.bind(branch_context):
    run_parallel_work()
```

Parallel contexts receive independent mutable locals, results, and
`current_step`. Parent locals are available as the read-only
`ptf_context.locals["__caller__"]` snapshot, while globals are also a read-only
snapshot. Snapshot values are deep-copied by default; callers may pass a
different `copy_fn` for values such as instrument sessions that require an
application-specific copying policy. An optional `parallel_endpoint` can be
attached when the branch context is created.

Live parent mutation is intentionally unavailable for parallel contexts:
`create_parallel_context(allow_parent_mutation=True)` raises `ValueError`.
Ordinary synchronous subsequences retain their existing
`allow_parent_mutation` behavior.

## Step wrapper

`@step` wraps a plain function with the `Step` class, which in turn wraps the
function with Prefect's `@task`. The wrapper collects metadata (`get_meta_info`),
allows enrichment (`add_additional_info`), and exposes Prefect task helpers like
`.submit()`.

```python
from pytestflow.core import step
from pytestflow.core.pytestflow_states import PyTestflowDone


@step(name="greet")
def greet(user: str) -> PyTestflowDone:
    print(f"Hello {user}")
    return PyTestflowDone(ptf_result={"step_status": "done", "output": user})
```

If `user` is not provided explicitly, the decorator will look for
`ptf_context.locals["user"]` or `ptf_context.globals["user"]`.

Specialized decorators like `@numeric_limit_step` or `@action_step` rely on this
core wrapper and simply post-process the return value into the appropriate
`PyTestflowState` subclass.

## Sequences as flows

Steps execute through the `Sequence` and `TestSequence` classes, both declared as
Prefect flows. A `Sequence` runs an ordered list of steps, manages context scope,
and aggregates child `PyTestflowState` instances. `TestSequence` extends this
behavior with setup/main/cleanup sections.

```python
from pytestflow.core import Sequence, step


@step(name="measure")
def measure():
    return 42


seq = Sequence("demo", [measure])
state = seq.run()
```

Sequences accept `default_parameters` merged with runtime overrides. Nested
sequences execute in isolated local namespaces; constructing a subsequence with
`allow_parent_mutation=True` grants controlled write access to the parent's
locals via `ptf_context.locals`.

## Process models

Process models inherit from `Sequence` and coordinate the broader UUT lifecycle.
`SequentialProcessModel` wires callbacks for `pre_uut`, `post_uut`, `report`, and
`database_logging` around a main sequence and stores intermediate results in
`ptf_context.locals` for later callbacks.

## State objects

Every step returns a subclass of `PyTestflowState`. The most common ones are:

- `PyTestflowPassed`
- `PyTestflowFailed`
- `PyTestflowDone`
- `PyTestflowError`

Each state carries a `ptf_result` dictionary and optional child states when
returned from sequences or process models.

```python
result = seq.run()
print(result.ptf_result["step_status"])  # sequence_passed/failed
for name, child in result.children:
    print(name, child.ptf_result)
```

Back to [steps documentation](../steps/README.md) or the
[project README](../../readme.md).
