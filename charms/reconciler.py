import logging
import ops
import charms.contextual_status as status
from typing import cast, Callable

log = logging.getLogger(__name__)


class Reconciler(ops.Object):
    stored = ops.StoredState()

    def __init__(
        self,
        charm: ops.CharmBase,
        reconcile_function: Callable,
        exit_status=None,
        custom_events=None,
    ):
        super().__init__(charm, "reconciler")
        self.charm = charm
        self.reconcile_function = reconcile_function
        self.stored.set_default(reconciled=False)
        self.exit_status = exit_status

        for event_kind, bound_event in charm.on.events().items():
            if not issubclass(bound_event.event_type, ops.HookEvent):
                continue
            if event_kind == "collect_metrics":
                continue
            self.framework.observe(bound_event, self.reconcile)

        if custom_events:
            for event in custom_events:
                self.framework.observe(event, self.reconcile)

    def reconcile(self, event: ops.EventBase):
        reconciled_state = cast(bool, self.stored.reconciled)
        if isinstance(event, ops.UpdateStatusEvent) and reconciled_state:
            return

        self.stored.reconciled = False

        with status.context(self.charm.unit, self.exit_status):
            try:
                self.reconcile_function(event)
                self.stored.reconciled = True
            except status.ReconcilerError:
                log.exception("Caught ReconcilerError")
