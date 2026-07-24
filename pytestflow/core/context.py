from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Any, Iterator


CopyFunction = Callable[[Any], Any]


@dataclass
class ExecutionContext:
    """State belonging to one PyTestFlow execution branch."""

    locals: dict[str, Any] = field(default_factory=dict)
    globals: Mapping[str, Any] = field(default_factory=dict)
    results: dict[str, list[dict[str, Any]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    current_step: Any | None = None
    parallel_endpoint: Any | None = None
    context_created_timestamp: datetime = field(default_factory=datetime.now)

    @property
    def contex_created_timestamp(self) -> datetime:
        """Backward-compatible alias for the original misspelled attribute."""
        return self.context_created_timestamp


class TestContext:
    """
    Backward-compatible proxy for the current branch's execution context.

    Existing imports keep using the singleton ``ptf_context``. The proxy routes
    every attribute access through a ContextVar so separately bound execution
    branches do not overwrite each other's locals, results, or current step.
    """

    _CALLER_KEY = "__caller__"

    def __init__(self) -> None:
        self._execution_context: ContextVar[ExecutionContext | None] = ContextVar(
            f"pytestflow_execution_context_{id(self)}",
            default=None,
        )

    def get(self) -> ExecutionContext:
        context = self._execution_context.get()
        if context is None:
            context = ExecutionContext()
            self._execution_context.set(context)
        return context

    @contextmanager
    def bind(self, context: ExecutionContext) -> Iterator[ExecutionContext]:
        """Temporarily make ``context`` current in this thread/task context."""
        if not isinstance(context, ExecutionContext):
            raise TypeError("context must be an ExecutionContext")

        token = self._execution_context.set(context)
        try:
            yield context
        finally:
            self._execution_context.reset(token)

    def create_child_context(
        self,
        *,
        allow_parent_mutation: bool = False,
    ) -> ExecutionContext:
        """
        Create context for an ordinary synchronous subsequence.

        This preserves the existing synchronous behavior: globals and results
        remain shared, and ``allow_parent_mutation`` may expose the live parent
        locals dictionary through ``__caller__``.
        """
        parent = self.get()
        if allow_parent_mutation:
            caller: Mapping[str, Any] = parent.locals
        else:
            caller = MappingProxyType(parent.locals.copy())

        return ExecutionContext(
            locals={self._CALLER_KEY: caller},
            globals=parent.globals,
            results=parent.results,
        )

    def create_parallel_context(
        self,
        *,
        parameters: Mapping[str, Any] | None = None,
        copy_fn: CopyFunction = deepcopy,
        parallel_endpoint: Any | None = None,
        allow_parent_mutation: bool = False,
    ) -> ExecutionContext:
        """
        Capture an isolated, read-only-input context for a parallel branch.

        Parent locals are available through ``locals["__caller__"]``. Globals
        and caller locals are deep-copied by default and wrapped in read-only
        mapping views. Explicit parameters populate the child locals and take
        precedence during normal PyTestFlow argument autowiring.
        """
        if allow_parent_mutation:
            raise ValueError(
                "allow_parent_mutation=True is not supported for parallel "
                "subsequences; collect explicit outputs and merge them after wait()."
            )
        if not callable(copy_fn):
            raise TypeError("copy_fn must be callable")

        explicit_parameters = dict(parameters or {})
        if self._CALLER_KEY in explicit_parameters:
            raise ValueError(f"'{self._CALLER_KEY}' is reserved by PyTestFlow")

        parent = self.get()
        try:
            snapshots = copy_fn(
                {
                    "locals": dict(parent.locals),
                    "globals": dict(parent.globals),
                    "parameters": explicit_parameters,
                }
            )
        except Exception as exc:
            raise ValueError(
                "Could not copy context values for a parallel subsequence. "
                "Provide a copy_fn that supports the values stored in ptf_context."
            ) from exc

        caller_snapshot = MappingProxyType(dict(snapshots["locals"]))
        globals_snapshot = MappingProxyType(dict(snapshots["globals"]))
        child_locals = dict(snapshots["parameters"])
        child_locals[self._CALLER_KEY] = caller_snapshot

        return ExecutionContext(
            locals=child_locals,
            globals=globals_snapshot,
            parallel_endpoint=parallel_endpoint,
        )

    @property
    def locals(self) -> dict[str, Any]:
        return self.get().locals

    @locals.setter
    def locals(self, value: dict[str, Any]) -> None:
        self.get().locals = value

    @property
    def globals(self) -> Mapping[str, Any]:
        return self.get().globals

    @globals.setter
    def globals(self, value: Mapping[str, Any]) -> None:
        self.get().globals = value

    @property
    def results(self) -> dict[str, list[dict[str, Any]]]:
        return self.get().results

    @results.setter
    def results(self, value: dict[str, list[dict[str, Any]]]) -> None:
        self.get().results = value

    @property
    def current_step(self) -> Any | None:
        return self.get().current_step

    @current_step.setter
    def current_step(self, value: Any | None) -> None:
        self.get().current_step = value

    @property
    def parallel_endpoint(self) -> Any | None:
        return self.get().parallel_endpoint

    @parallel_endpoint.setter
    def parallel_endpoint(self, value: Any | None) -> None:
        self.get().parallel_endpoint = value

    @property
    def this_context(self) -> TestContext:
        return self

    @property
    def context_created_timestamp(self) -> datetime:
        return self.get().context_created_timestamp

    @property
    def contex_created_timestamp(self) -> datetime:
        return self.get().contex_created_timestamp

    def __repr__(self) -> str:
        context = self.get()
        return (
            f"<Context locals={list(context.locals.keys())} "
            f"globals={list(context.globals.keys())}>"
        )


# Global proxy. Its target ExecutionContext is local to the active context.
ptf_context = TestContext()
