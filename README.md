# charm-lib-reconciler

Charm library for unified reconciliation.

This library aims to make it easier to write charms with a single "reconcile"
handler that handles all Juju events.

```python3
from charms.reconciler import Reconciler

class SomeCharm(ops.CharmBase):
    def __init__(self, *args):
        super().__init__(*args)
        self.reconciler = Reconciler(self, self.reconcile)

    def reconcile(self, event):
        # ...
```

In the above example, the `reconcile` method will end up being called for any
HookEvent such as `install`, `config_changed`, `*_relation_changed`, and so on.
Within the reconcile method, the charm can check and set relation data,
configure local services, and so on.
