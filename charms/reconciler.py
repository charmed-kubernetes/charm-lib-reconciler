import charms.contextual_status as status
import logging

from ops import BlockedStatus, HookEvent, Object, StoredState, UpdateStatusEvent


log = logging.getLogger(__name__)


class Reconciler(Object):
    stored = StoredState()

    def __init__(self, charm, reconcile_function):
        super().__init__(charm, "reconciler")
        self.charm = charm
        self.reconcile_function = reconcile_function
        self.stored.set_default(reconciled=False)

        for event_kind, bound_event in charm.on.events().items():
            if not issubclass(bound_event.event_type, HookEvent):
                continue
            if event_kind == "collect_metrics":
                continue
            self.framework.observe(bound_event, self.reconcile)

    def reconcile(self, event):
        if isinstance(event, UpdateStatusEvent) and self.stored.reconciled:
            return

        self.stored.reconciled = False

        with status.context(self.charm.unit):
            try:
                result = self.reconcile_function(event)
                self.stored.reconciled = True
                return result
            except Exception as e:
                # The decision to enter Blocked instead of Error here is going
                # to be controversial for sure. I think it's worth it to cut
                # down on try/except boilerplate in reconcile functions.
                log.exception(e)
                status.add(BlockedStatus("Failed to reconcile, see debug-log"))
