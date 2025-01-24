import logging
from typing import Callable, Optional

from ops import BlockedStatus, BoundEvent, HookEvent, Object, CharmBase, EventBase, StatusBase


log = logging.getLogger(__name__)


class ReconcileErrorWithStatus(Exception):
    def __init__(
            self: "ReconcileErrorWithStatus", 
            message: str, 
            status: StatusBase,
        ) -> None:
        self.status = status
        self.message = message
        super().__init__(message)

    def __str__(self: "ReconcileErrorWithStatus") -> str:
        return f"[{self.status}] {self.message}"


class Reconciler(Object):
    def __init__(
        self: "Reconciler",
        charm: CharmBase,
        reconcile_function: Callable[[EventBase], None],
        custom_events: Optional[list[BoundEvent]] = None,
        exit_status: StatusBase = BlockedStatus("Reconcile failed"),
    ):
        self.charm = charm
        self.reconcile_function = reconcile_function
        self.exit_status = exit_status

        for event_kind, bound_event in charm.on.events().items():
            if not issubclass(bound_event.event_type, HookEvent):
                continue
            if event_kind == "collect_metrics":
                continue
            self.framework.observe(bound_event, self.reconcile)

        if custom_events:
            for event in custom_events:
                self.framework.observe(event, self.reconcile)

    def reconcile(self: "Reconciler", event: EventBase) -> None:
        try:
            self.reconcile_function(event)
        except ReconcileErrorWithStatus as e:
            log.exception(f"reconcile failed: {e}")
            self.charm.unit.status = e.status
            return

        self.charm.unit.status = self.exit_status
