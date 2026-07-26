from datetime import datetime
from functools import wraps
import uuid
from prefect import flow
from typing import Any, Callable, List, Iterable
from pytestflow.core.pytestflow_states import (
    PyTestflowPassed, PyTestflowFailed, PyTestflowDone, PyTestflowError, PyTestflowState
)
from pytestflow.core.context import ptf_context
from pytestflow.core.parallel import (
    ParallelSequenceEvent,
    ParallelSequenceEventKind,
    ParallelSequenceHandle,
    ParallelSequenceStart,
    ParallelSequenceWait,
    ParallelSequenceEndpoint,
    _ParallelMessageChannel,
    emit_parallel_sequence_event,
    run_parallel_sequence_task,
)
import inspect

from pytestflow.core.utils import get_data_for_gui
from pytestflow.core.runtime_control import runtime_control


def _with_sequence_execution_registry(fn):
    @wraps(fn)
    def wrapped(sequence, *args, **kwargs):
        with ptf_context.sequence_execution(sequence.name):
            return fn(sequence, *args, **kwargs)

    return wrapped


class Sequence:
    def __init__(
        self,
        name: str,
        steps: List[Callable] = None,
        default_parameters: dict = None,
        allow_parent_mutation: bool = False,
        apply_throttle: bool = True,
    ):
        self.name = name
        self.steps = steps or []
        self.allow_parent_mutation = allow_parent_mutation
        self.apply_throttle = apply_throttle
        self.results: List[tuple[str, PyTestflowState]] = []
        self.default_parameters = default_parameters or {}
        self.max_transitions = 10_000

    def _is_passed(self, state: PyTestflowState) -> bool:
        return isinstance(state, (PyTestflowPassed, PyTestflowDone))

    def add_step(self, fn: Callable, position: int = None):
        if position is None:
            self.steps.append(fn)
        else:
            self.steps.insert(position, fn)

    def remove_step(self, name_or_fn):
        name = name_or_fn if isinstance(name_or_fn, str) else name_or_fn.__name__
        self.steps = [s for s in self.steps if getattr(s, '__name__', '') != name]

    def force_step_status(self, step_name: str, status: str):
        def forced():
            match status.lower():
                case "passed":
                    return PyTestflowPassed(ptf_result={"step_status": "forced_pass"}, message=f"{step_name} forced pass")
                case "failed":
                    return PyTestflowFailed(ptf_result={"step_status": "forced_fail"}, message=f"{step_name} forced fail")
                case "done" | "skipped":
                    return PyTestflowDone(ptf_result={"step_status": f"{status.lower()}"}, message=f"{step_name} {status.lower()}")
                case _:
                    raise ValueError(f"Unsupported status: {status}")
        forced.__name__ = step_name + "_forced"
        self.add_step(forced)

    def _start_parallel_sequence(
        self,
        node: ParallelSequenceStart,
    ) -> tuple[ParallelSequenceHandle, PyTestflowState]:
        call_id = uuid.uuid4()
        channel = _ParallelMessageChannel()
        endpoint = ParallelSequenceEndpoint(channel)
        child_context = ptf_context.create_parallel_context(
            parameters=node.parameters,
            copy_fn=node.copy_fn,
            parallel_endpoint=endpoint,
            allow_parent_mutation=getattr(
                node.sequence,
                "allow_parent_mutation",
                False,
            ),
        )
        future = run_parallel_sequence_task.submit(
            sequence=node.sequence,
            execution_context=child_context,
            parameters=node.parameters,
        )
        handle = ParallelSequenceHandle(
            call_id=call_id,
            sequence_name=node.sequence.name,
            future=future,
            channel=channel,
        )
        ptf_context.register_parallel_handle(
            handle,
            registry_name=node.registry_key,
            store_as=node.store_as,
        )

        state = PyTestflowDone(
            ptf_result={
                "step_status": "parallel_sequence_started",
                "step_type": "parallel_sequence_start",
                "call_id": str(handle.call_id),
                "sequence_name": handle.sequence_name,
                "prefect_task_run_id": (
                    str(handle.prefect_task_run_id)
                    if handle.prefect_task_run_id is not None
                    else None
                ),
                "handle_name": node.store_as,
            },
            message=f"Parallel sequence {handle.sequence_name} started",
        )
        emit_parallel_sequence_event(
            ParallelSequenceEvent(
                kind=ParallelSequenceEventKind.STARTED,
                call_id=handle.call_id,
                sequence_name=handle.sequence_name,
                node_name=node.name,
                status=handle.status(),
                prefect_task_run_id=handle.prefect_task_run_id,
                result=state,
            )
        )
        return handle, state

    def _wait_parallel_sequence(
        self,
        node: ParallelSequenceWait,
    ) -> PyTestflowState:
        handle = ptf_context.get_parallel_handle(node.registry_reference)
        child_result = handle.result(timeout=node.timeout)
        timed_out = bool(child_result.ptf_result.get("timed_out", False))
        if not timed_out:
            handle.mark_joined()

        metadata = {
            "step_type": "parallel_sequence_wait",
            "call_id": str(handle.call_id),
            "sequence_name": handle.sequence_name,
            "prefect_task_run_id": (
                str(handle.prefect_task_run_id)
                if handle.prefect_task_run_id is not None
                else None
            ),
            "child_status": child_result.ptf_result.get("step_status"),
            "joined": handle.joined,
            "timed_out": timed_out,
        }
        children = [(handle.sequence_name, child_result)]

        if isinstance(child_result, PyTestflowError):
            wait_result: PyTestflowState = PyTestflowError(
                ptf_result={
                    **metadata,
                    "step_status": "parallel_sequence_error",
                },
                message=f"Parallel sequence {handle.sequence_name} errored",
                children=children,
            )
        elif self._is_passed(child_result):
            wait_result = PyTestflowPassed(
                ptf_result={
                    **metadata,
                    "step_status": "parallel_sequence_completed",
                },
                message=f"Parallel sequence {handle.sequence_name} completed",
                children=children,
            )
        else:
            wait_result = PyTestflowFailed(
                ptf_result={
                    **metadata,
                    "step_status": "parallel_sequence_failed",
                },
                message=f"Parallel sequence {handle.sequence_name} failed",
                children=children,
            )

        emit_parallel_sequence_event(
            ParallelSequenceEvent(
                kind=ParallelSequenceEventKind.WAIT_COMPLETED,
                call_id=handle.call_id,
                sequence_name=handle.sequence_name,
                node_name=node.name,
                status=handle.status(),
                prefect_task_run_id=handle.prefect_task_run_id,
                result=wait_result,
            )
        )
        return wait_result

    def _parallel_control_error(
        self,
        node: ParallelSequenceStart | ParallelSequenceWait,
        exception: Exception,
    ) -> PyTestflowError:
        return PyTestflowError(
            ptf_result={
                "step_status": "error",
                "step_type": (
                    "parallel_sequence_start"
                    if isinstance(node, ParallelSequenceStart)
                    else "parallel_sequence_wait"
                ),
                "error": str(exception),
                "exception": exception,
                "exception_type": (
                    f"{type(exception).__module__}."
                    f"{type(exception).__qualname__}"
                ),
            },
            message=f"Parallel control node '{node.name}' failed: {exception}",
        )

    def _exec_step(self, step_fn: Callable) -> tuple[str, PyTestflowState]:
        step_name = getattr(step_fn, "name", getattr(step_fn, "__name__", repr(step_fn)))
        print(f"➡️ Executing step: {step_name}")

        if inspect.iscoroutinefunction(step_fn):
            raise TypeError("Async step functions are not supported in this base Sequence class.")

        if isinstance(step_fn, ParallelSequenceStart):
            try:
                _, state = self._start_parallel_sequence(step_fn)
            except Exception as exc:
                state = self._parallel_control_error(step_fn, exc)

        elif isinstance(step_fn, ParallelSequenceWait):
            try:
                state = self._wait_parallel_sequence(step_fn)
            except Exception as exc:
                state = self._parallel_control_error(step_fn, exc)

        # Check if the step is a Sequence
        elif isinstance(step_fn, Sequence):
            child_context = ptf_context.create_child_context(
                allow_parent_mutation=step_fn.allow_parent_mutation
            )
            with ptf_context.bind(child_context):
                # Run the subsequence
                state = step_fn.run(return_state=True)  # returns PyTestflowState
        
        else:
            # Execute a regular step
            try:
                state = step_fn()
                #step_name = state.ptf_result['step_name']
            except Exception as exc:
                print(f"💥 Step {step_name} raised an exception: {exc}")
                state = PyTestflowError(ptf_result={"step_status": "error", "error": str(exc)}, message=str(exc)) 

        return step_name, state

    def _get_step_name(self, step_fn: Callable) -> str:
        return getattr(step_fn, "name", getattr(step_fn, "__name__", repr(step_fn)))

    def _read_next_step_directive(self) -> Any:
        return ptf_context.locals.pop("_ptf_next_step", "next")

    def _resolve_next_index(
        self,
        *,
        directive: Any,
        current_index: int,
        step_name_to_index: dict[str, int],
        step_count: int,
    ) -> tuple[int, str | None]:
        if directive is None:
            return current_index + 1, None

        if isinstance(directive, str):
            token = directive.strip()
            token_lower = token.lower()

            if token == "" or token_lower == "next":
                return current_index + 1, None
            if token_lower in {"end", "stop"}:
                return step_count, None
            if token in step_name_to_index:
                return step_name_to_index[token], None

            return (
                current_index + 1,
                f"Invalid _ptf_next_step '{directive}'. Expected 'next', 'end', a valid step name, or an integer index.",
            )

        if isinstance(directive, int):
            if 0 <= directive < step_count:
                return directive, None
            if directive == step_count:
                return step_count, None
            return (
                current_index + 1,
                f"Invalid _ptf_next_step index '{directive}'. Expected range [0, {step_count}]",
            )

        return (
            current_index + 1,
            f"Invalid _ptf_next_step type '{type(directive).__name__}'. Expected str or int.",
        )

    def _flow_control_error_state(self, message: str) -> PyTestflowError:
        return PyTestflowError(
            ptf_result={"step_status": "error", "error": message, "step_type": "flow_control"},
            message=message,
        )

    def _run_steps_with_flow_control(
        self,
        *,
        steps: List[Callable],
        runtime_start_index: int = 0,
    ) -> tuple[List[tuple[str, PyTestflowState]], int]:
        results: List[tuple[str, PyTestflowState]] = []
        step_name_to_index = {self._get_step_name(step): idx for idx, step in enumerate(steps)}
        current_index = 0
        transitions = 0

        ptf_context.locals.pop("_ptf_next_step", None)

        while 0 <= current_index < len(steps):
            if transitions >= self.max_transitions:
                msg = (
                    f"Sequence '{self.name}' exceeded max transitions ({self.max_transitions}). "
                    "Possible infinite loop in flow control."
                )
                results.append(("__flow_control__", self._flow_control_error_state(msg)))
                break

            runtime_control.checkpoint_before_step(
                runtime_start_index + transitions,
                apply_throttle=self.apply_throttle,
            )
            step_fn = steps[current_index]
            name, state = self._exec_step(step_fn)
            results.append((name, state))
            transitions += 1

            directive = self._read_next_step_directive()
            next_index, error = self._resolve_next_index(
                directive=directive,
                current_index=current_index,
                step_name_to_index=step_name_to_index,
                step_count=len(steps),
            )
            if error:
                results.append(("__flow_control__", self._flow_control_error_state(error)))
                break

            current_index = next_index

        return results, runtime_start_index + transitions

    @flow(name="SequenceRun", persist_result=False)
    @_with_sequence_execution_registry
    def run(self, parameters: dict | None = None) -> PyTestflowState:
        print(f"\n▶️ Running Sequence: {self.name}")
        self.results = []

        # Initialize context for this sequence run
        merged_parameters = {**self.default_parameters, **(parameters or {})}
        ptf_context.locals.update(merged_parameters)
        self.results, _ = self._run_steps_with_flow_control(
            steps=self.steps,
            runtime_start_index=0,
        )

        all_passed = all(self._is_passed(s) for _, s in self.results)

        if all_passed:
            overall = PyTestflowPassed(
                ptf_result={"step_status": "sequence_passed"},
                message=f"Sequence {self.name} passed",
                children=self.results,
            )
        else:
            overall = PyTestflowFailed(
                ptf_result={"step_status": "sequence_failed"},
                message=f"Sequence {self.name} failed",
                children=self.results,
            )

        print(f"✅ Sequence {self.name} finished with status: {overall.name}")
        return overall


    @flow(name="SequenceRunStep")
    @_with_sequence_execution_registry
    def run_step(self, names: str | Iterable[str]) -> PyTestflowState:
        if isinstance(names, str):
            wanted = {names}
        else:
            wanted = set(names)

        print(f"\n▶️ Running Sequence.run_step on {self.name} for: {', '.join(wanted)}")
        selected = [s for s in self.steps if getattr(s, "__name__", "") in wanted]

        results, _ = self._run_steps_with_flow_control(
            steps=selected,
            runtime_start_index=0,
        )

        all_passed = all(self._is_passed(s) for _, s in results)

        if all_passed:
            overall = PyTestflowPassed(
                ptf_result={"step_status": "sequence_partial_passed"},
                message=f"Sequence {self.name} (partial) passed",
                children=results,
            )
        else:
            overall = PyTestflowFailed(
                ptf_result={"step_status": "sequence_partial_failed"},
                message=f"Sequence {self.name} (partial) failed",
                children=results,
            )

        return overall


class TestSequence(Sequence):
    def __init__(
        self,
        name: str,        
        setup_steps: List[Callable] | None = None,
        main_steps: List[Callable] | None = None,
        cleanup_steps: List[Callable] | None = None,        
    ):
        super().__init__(name=name, steps=[])
        self.static_uuid: uuid.UUID | None = None
        self.setup_steps = setup_steps or []
        self.main_steps = main_steps or []
        self.cleanup_steps = cleanup_steps or []

    def _run_section(self, label: str, steps: List[Callable], start_index: int = 0) -> tuple[List[tuple[str, PyTestflowState]], int]:
        print(f"\n🔧 Running {label} for {self.name}")
        return self._run_steps_with_flow_control(
            steps=steps,
            runtime_start_index=start_index,
        )

    @flow(name="TestSequenceRun")
    @_with_sequence_execution_registry
    def run(self) -> PyTestflowState:
        print(f"\n▶️ Running Test Sequence: {self.name}")
        
        # Send start to GUI  
        get_data_for_gui(self, datetime.utcnow())

        all_steps = [*self.setup_steps, *self.main_steps, *self.cleanup_steps]
        children, _ = self._run_steps_with_flow_control(
            steps=all_steps,
            runtime_start_index=0,
        )

        all_passed = all(self._is_passed(state) for _, state in children)

        if all_passed:
            overall = PyTestflowPassed(
                ptf_result={"step_status": "test_sequence_passed"},
                message=f"Test Sequence {self.name} passed",
                children=children,
            )
        else:
            overall = PyTestflowFailed(
                ptf_result={"step_status": "test_sequence_failed"},
                message=f"Test Sequence {self.name} failed",
                children=children,
            )

        # Send end to GUI
        result_data = {
            "step_status": "passed" if all_passed else "failed",
            "step_type": "sub_sequence",
        }
        get_data_for_gui(self, datetime.utcnow(), result_data)

        print(f"✅ Test Sequence {self.name} finished with status: {overall.name}")
        return overall

    @flow(name="TestSequenceRunStep")
    @_with_sequence_execution_registry
    def run_step(self, names: str | Iterable[str]) -> PyTestflowState:
        if isinstance(names, str):
            wanted = {names}
        else:
            wanted = set(names)

        print(f"\n▶️ Running TestSequence.run_step on {self.name} for main steps: {', '.join(wanted)}")

        children: List[tuple[str, PyTestflowState]] = []
        step_index = 0

        setup_res, step_index = self._run_section("setup", self.setup_steps, start_index=step_index)
        children.extend(setup_res)

        selected_main = [s for s in self.main_steps if getattr(s, "__name__", "") in wanted]
        main_res, step_index = self._run_section("main (selected)", selected_main, start_index=step_index)
        children.extend(main_res)

        cleanup_res, step_index = self._run_section("cleanup", self.cleanup_steps, start_index=step_index)
        children.extend(cleanup_res)

        all_passed = all(self._is_passed(state) for _, state in children)

        if all_passed:
            overall = PyTestflowPassed(
                ptf_result={"step_status": "test_sequence_partial_passed"},
                message=f"Test Sequence {self.name} (selected) passed",
                children=children,
            )
        else:
            overall = PyTestflowFailed(
                ptf_result={"step_status": "test_sequence_partial_failed"},
                message=f"Test Sequence {self.name} (selected) failed",
                children=children,
            )

        return overall

    @_with_sequence_execution_registry
    def run_step(self, step_name: str, return_state: bool = False):
        """
        Run a specific step in the sequence, including setup and cleanup steps.
        """
        from prefect import flow

        # Find the step by name
        step = next(
            (s for s in self.setup_steps + self.main_steps + self.cleanup_steps if s.name == step_name),
            None,
        )
        if not step:
            raise ValueError(f"Step '{step_name}' not found in sequence '{self.name}'.")

        @flow(name=f"Test Sequence {self.name} (selected)")
        def sequence_flow():
            steps_to_run = [*self.setup_steps, step, *self.cleanup_steps]
            results, _ = self._run_steps_with_flow_control(
                steps=steps_to_run,
                runtime_start_index=0,
            )
            return results

        # Run the flow
        flow_state = sequence_flow(return_state=return_state)

        # Build the result
        result_children = [
            (name, state) for name, state in flow_state.result()
        ]
        result = PyTestflowPassed(children=result_children) if all(
            state.is_completed() for _, state in result_children
        ) else PyTestflowFailed(children=result_children)

        return result
