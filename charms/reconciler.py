"""Reconciler pattern implementation for Juju charms.

This module provides a reconciler that automatically observes all charm events
and calls a single reconcile function, with optional step tracing and
structured error output.
"""

from __future__ import annotations

import inspect
import logging
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Callable, Literal, cast

import ops

import charms.contextual_status as status

log = logging.getLogger(__name__)

Verbosity = Literal["quiet", "normal", "verbose", "debug"]


@dataclass
class StepResult:
    """Result of a single reconciliation step."""

    name: str
    success: bool
    duration_ms: float
    error: ErrorSummary | None = None


@dataclass
class ErrorSummary:
    """Structured error information without full stack trace."""

    type: str
    message: str
    location: str
    step: str | None = None
    cause: str | None = None
    traceback: str | None = None

    @classmethod
    def from_exception(
        cls, exc: BaseException, step: str | None = None, include_traceback: bool = False
    ) -> ErrorSummary:
        """Create an ErrorSummary from an exception."""
        exc_type = type(exc).__name__
        message = str(exc) or "(no message)"

        # Get cause if chained
        cause = None
        if exc.__cause__:
            cause = f"{type(exc.__cause__).__name__}: {exc.__cause__}"

        # Extract location - prefer the original cause's location over wrapper
        location = cls._find_user_code_location(exc)

        # Full traceback if requested
        tb_str = None
        if include_traceback:
            tb_str = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))

        return cls(
            type=exc_type,
            message=message,
            location=location,
            step=step,
            cause=cause,
            traceback=tb_str,
        )

    @staticmethod
    def _find_user_code_location(exc: BaseException) -> str:
        """Find the location in user code where the error originated.

        Walks through the exception chain and traceback frames to find
        the first frame that's not in library code (site-packages, venv, etc.)
        """
        # Patterns that indicate library code (not user code)
        library_patterns = (
            "site-packages",
            "/lib/python",
            "contextual_status",
            "charms/reconciler.py",
        )

        def is_user_code(filename: str) -> bool:
            return not any(pattern in filename for pattern in library_patterns)

        def find_in_traceback(tb) -> str | None:
            frames = traceback.extract_tb(tb)
            # Search from most recent to oldest for user code
            for frame in reversed(frames):
                if is_user_code(frame.filename):
                    return f"{frame.filename}:{frame.lineno}"
            return None

        # First, check the cause chain for user code locations
        current = exc
        while current is not None:
            if current.__traceback__:
                location = find_in_traceback(current.__traceback__)
                if location:
                    return location
            current = current.__cause__

        # Fallback: just use the last frame of the original exception
        if exc.__traceback__:
            frames = traceback.extract_tb(exc.__traceback__)
            if frames:
                last = frames[-1]
                return f"{last.filename}:{last.lineno}"

        return "unknown"


@dataclass
class ReconcileResult:
    """Result of a complete reconciliation attempt."""

    success: bool
    event_name: str
    event_type: str
    duration_ms: float
    steps: list[StepResult] = field(default_factory=list)
    error: ErrorSummary | None = None


class ReconcileContext:
    """Context object passed to reconcile functions for enhanced tracking.

    Provides:
    - Event information (name, type, the event object itself)
    - Step tracing via the step() context manager
    - Timing information

    Example:
        def _reconcile(self, ctx: ReconcileContext):
            log.info(f"Processing {ctx.event_name}")

            with ctx.step("Validate config"):
                self._validate()

            with ctx.step("Deploy workload"):
                self._deploy()
    """

    def __init__(
        self,
        event: ops.EventBase,
        charm: ops.CharmBase,
        verbosity: Verbosity = "normal",
    ):
        self._event = event
        self._charm = charm
        self._verbosity = verbosity
        self._steps: list[StepResult] = []
        self._current_step: str | None = None
        self._start_time = time.monotonic()

    @property
    def event(self) -> ops.EventBase:
        """The event being reconciled."""
        return self._event

    @property
    def event_name(self) -> str:
        """Human-readable event name (e.g., 'config-changed')."""
        # Convert class name like ConfigChangedEvent to config-changed
        class_name = type(self._event).__name__
        # Remove 'Event' suffix and convert to kebab-case
        name = class_name.removesuffix("Event")
        # Insert hyphens before capitals and lowercase
        result = []
        for i, char in enumerate(name):
            if char.isupper() and i > 0:
                result.append("-")
            result.append(char.lower())
        return "".join(result)

    @property
    def event_type(self) -> str:
        """Event class name (e.g., 'ConfigChangedEvent')."""
        return type(self._event).__name__

    @property
    def charm(self) -> ops.CharmBase:
        """The charm instance."""
        return self._charm

    @property
    def steps(self) -> list[StepResult]:
        """List of completed steps."""
        return self._steps.copy()

    @property
    def current_step(self) -> str | None:
        """Name of the currently executing step, if any."""
        return self._current_step

    @property
    def elapsed_ms(self) -> float:
        """Milliseconds elapsed since reconciliation started."""
        return (time.monotonic() - self._start_time) * 1000

    @contextmanager
    def step(self, name: str):
        """Context manager to track a reconciliation step.

        Example:
            with ctx.step("Installing packages"):
                self._install_packages()

        If an exception occurs within the step, it will be recorded
        and re-raised.
        """
        step_start = time.monotonic()
        self._current_step = name
        try:
            yield
            duration_ms = (time.monotonic() - step_start) * 1000
            self._steps.append(StepResult(name=name, success=True, duration_ms=duration_ms))
        except Exception as exc:
            duration_ms = (time.monotonic() - step_start) * 1000
            error = ErrorSummary.from_exception(
                exc,
                step=name,
                include_traceback=(self._verbosity == "debug"),
            )
            self._steps.append(
                StepResult(name=name, success=False, duration_ms=duration_ms, error=error)
            )
            raise
        finally:
            self._current_step = None

    def build_result(self, success: bool, error: ErrorSummary | None = None) -> ReconcileResult:
        """Build the final ReconcileResult."""
        return ReconcileResult(
            success=success,
            event_name=self.event_name,
            event_type=self.event_type,
            duration_ms=self.elapsed_ms,
            steps=self._steps.copy(),
            error=error,
        )


def _format_error_lines(error: ErrorSummary, indent: str, verbosity: Verbosity) -> list[str]:
    """Format error details as indented lines."""
    lines = [
        f"{indent}{error.type}: {error.message}",
        f"{indent}at {error.location}",
    ]
    if error.cause:
        lines.append(f"{indent}caused by: {error.cause}")
    if verbosity == "debug" and error.traceback:
        for tb_line in error.traceback.strip().split("\n"):
            lines.append(f"{indent}{tb_line}")
    return lines


def _format_step_lines(step: StepResult, verbosity: Verbosity) -> list[str]:
    """Format a single step as lines."""
    mark = "\u2713" if step.success else "\u2717"  # checkmark or x
    lines = [f"  {mark} {step.name} ({step.duration_ms:.1f}ms)"]
    if not step.success and step.error:
        lines.append(f"    \u2514\u2500 {step.error.type}: {step.error.message}")
        lines.extend(_format_error_lines(step.error, "       ", verbosity)[1:])
    return lines


def _format_trace(result: ReconcileResult, verbosity: Verbosity) -> str:
    """Format a ReconcileResult as human-readable text."""
    if verbosity == "quiet":
        if result.success:
            return ""
        if result.error:
            return f"[{result.event_name}] FAILED: {result.error.type} - {result.error.message}"
        return f"[{result.event_name}] FAILED"

    status_str = "OK" if result.success else "FAILED"
    lines = [f"[{result.event_name}] Reconcile {status_str} ({result.duration_ms:.2f}ms)"]

    if verbosity == "debug":
        lines.append(f"  event_type: {result.event_type}")

    if verbosity in ("verbose", "debug") and result.steps:
        for step in result.steps:
            lines.extend(_format_step_lines(step, verbosity))
    elif result.error and not any(s.error for s in result.steps):
        lines.extend(_format_error_lines(result.error, "  ", verbosity))

    return "\n".join(lines)


def _is_context_annotation(annotation) -> bool:
    """Check if an annotation indicates ReconcileContext."""
    if annotation is ReconcileContext:
        return True
    if isinstance(annotation, str) and "Context" in annotation:
        return True
    if hasattr(annotation, "__name__") and "Context" in annotation.__name__:
        return True
    return False


def _detect_context_signature(func: Callable) -> bool:
    """Detect if a function expects ReconcileContext vs EventBase.

    Returns True if the function expects ReconcileContext, False otherwise.
    Detection is based on type annotations and parameter names.
    """
    try:
        sig = inspect.signature(func)
    except (ValueError, TypeError):
        return False

    params = list(sig.parameters.values())

    # Skip 'self' if it's a bound method
    if params and params[0].name == "self":
        params = params[1:]

    if not params:
        return False

    first_param = params[0]

    # Check type annotation
    if first_param.annotation is not inspect.Parameter.empty:
        if _is_context_annotation(first_param.annotation):
            return True

    # Fallback: check parameter name
    return first_param.name in ("ctx", "context", "reconcile_context")


class Reconciler(ops.Object):
    """Reconciler that observes all charm events and calls a single reconcile function.

    The Reconciler automatically observes all HookEvent types (except collect_metrics)
    and optional custom events, calling your reconcile function for each.

    Features:
    - Step tracing with ctx.step() for debugging
    - Structured error output instead of stack traces
    - Verbosity control (quiet/normal/verbose/debug)
    - Backwards compatible with event-only signatures

    Example:
        class MyCharm(ops.CharmBase):
            def __init__(self, *args):
                super().__init__(*args)
                self.reconciler = Reconciler(
                    charm=self,
                    reconcile=self._reconcile,
                    exit_status=ops.ActiveStatus("Ready"),
                    verbosity="verbose",
                )

            def _reconcile(self, ctx: ReconcileContext):
                with ctx.step("Configure"):
                    self._configure()
    """

    stored = ops.StoredState()

    def __init__(
        self,
        charm: ops.CharmBase,
        reconcile_function: Callable,
        *,
        exit_status: ops.StatusBase | None = None,
        custom_events: list | None = None,
        verbosity: Verbosity = "normal",
    ):
        """Initialize the Reconciler.

        Args:
            charm: The charm instance.
            reconcile_function: Function to call for reconciliation. Can accept
                either (event: EventBase) or (ctx: ReconcileContext).
            exit_status: Status to set on successful reconciliation.
            custom_events: Additional events to observe beyond HookEvents.
            verbosity: Output detail level - quiet, normal, verbose, or debug.
        """
        super().__init__(charm, "reconciler")
        self.charm = charm
        self.reconcile_function = reconcile_function
        self.stored.set_default(reconciled=False)
        self.exit_status = exit_status
        self._verbosity = verbosity
        self._use_context = _detect_context_signature(reconcile_function)

        # Observe all HookEvents except collect_metrics
        for event_kind, bound_event in charm.on.events().items():
            if not issubclass(bound_event.event_type, ops.HookEvent):
                continue
            if event_kind == "collect_metrics":
                continue
            self.framework.observe(bound_event, self.reconcile)

        # Observe custom events
        if custom_events:
            for event in custom_events:
                self.framework.observe(event, self.reconcile)

    def reconcile(self, event: ops.EventBase):
        """Handle a reconciliation event.

        This method is called for all observed events. It creates a
        ReconcileContext, calls the user's reconcile function, and
        handles errors with structured output.
        """
        # Skip update-status if already reconciled
        reconciled_state = cast(bool, self.stored.reconciled)
        if isinstance(event, ops.UpdateStatusEvent) and reconciled_state:
            return

        self.stored.reconciled = False

        # Create context for this reconciliation
        ctx = ReconcileContext(event, self.charm, self._verbosity)

        with status.context(self.charm.unit, self.exit_status):
            try:
                # Call reconcile function with appropriate argument
                if self._use_context:
                    self.reconcile_function(ctx)
                else:
                    self.reconcile_function(event)

                self.stored.reconciled = True

                # Log success
                result = ctx.build_result(success=True)
                self._log_result(result)

            except status.ReconcilerError as exc:
                # Expected error - capture structured info
                error = ErrorSummary.from_exception(
                    exc,
                    step=ctx.current_step,
                    include_traceback=(self._verbosity == "debug"),
                )
                result = ctx.build_result(success=False, error=error)
                self._log_result(result)

    def _log_result(self, result: ReconcileResult):
        """Log the reconciliation result based on verbosity."""
        trace = _format_trace(result, self._verbosity)
        if not trace:
            return

        if result.success:
            log.info(trace)
        else:
            log.warning(trace)
