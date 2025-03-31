import pytest
import ops.testing
from charms.reconciler import Reconciler
from typing import Optional, Callable
from charms.contextual_status import ReconcilerError
import unittest.mock as mock

EXIT_STATUS = ops.ActiveStatus("Ready")


class MyCustomEvent(ops.EventBase):
    pass


class MyCustomEvents(ops.ObjectEvents):
    custom = ops.EventSource(MyCustomEvent)


class MyCustomObject(ops.Object):
    on = MyCustomEvents()


class MyCustomCharm(ops.CharmBase):
    def reconcile_function(self, event: ops.EventBase):
        self.test_method and self.test_method(event, "reconcile_function")

    def update_status(self, event: ops.EventBase):
        self.test_method and self.test_method(event, "update_status")

    def __init__(self, framework):
        super().__init__(framework)
        o = MyCustomObject(self, "test")
        self.reconciler = Reconciler(
            self,
            self.reconcile_function,
            exit_status=EXIT_STATUS,
            custom_events=[o.on.custom],
        )
        self.test_method: Optional[Callable] = None
        self.framework.observe(self.on.update_status, self.update_status)


@pytest.fixture
def harness():
    harness = ops.testing.Harness(MyCustomCharm, meta='{"name": "test-charm"}')
    harness.disable_hooks()
    harness.begin()
    harness._get_backend_calls(reset=True)
    yield harness
    harness.cleanup()


def test_reconciler_initialized(harness):
    r = harness.charm.reconciler
    assert r.charm == harness.charm
    assert r.reconcile_function == harness.charm.reconcile_function
    assert r.stored.reconciled is False
    assert r.exit_status == EXIT_STATUS
    assert r.framework == harness.charm.framework

    observers = r.framework._observers
    assert (
        "MyCustomCharm/Reconciler[reconciler]",
        "reconcile",
        "MyCustomCharm/on",
        "config_changed",
    ) in observers, "config_changed not observed"

    assert (
        "MyCustomCharm/Reconciler[reconciler]",
        "reconcile",
        "MyCustomCharm/MyCustomObject[test]/on",
        "custom",
    ) in observers, "custom_events not observed"


def test_reconcile_success(harness, caplog):
    harness.charm.test_method = test_method = mock.MagicMock()
    harness.enable_hooks()
    harness.charm.on.update_status.emit()

    # Unreconciled, we should see both the reconcile_function and update_status called
    test_method.assert_called()
    expected = {"reconcile_function", "update_status"}
    methods_seen = set(args[1] for (args, _) in test_method.call_args_list)
    assert test_method.call_count == len(expected)
    assert methods_seen == expected

    # after a successful reconcile, the state should be reconciled==True
    r = harness.charm.reconciler
    assert "" == caplog.text
    assert r.stored.reconciled is True

    # This confirms the reconcile_function changed the stored data
    # it does dig into the private data, but this is the only 
    # way to confirm the data was changed
    assert r.stored._data.dirty is True
    harness.charm.framework.commit()  # committing the framework, marks the stored data not dirty

    # Once reconciled, we shouldn't see the reconcile_function called for update_status
    test_method.reset_mock()

    harness.charm.on.update_status.emit()
    test_method.assert_called_once()
    assert test_method.call_args[0][1] == "update_status"
    assert "" == caplog.text
    assert r.stored.reconciled is True

    # This confirms the reconcile_function did not change the stored data
    # it does dig into the private data, but this is the only 
    # way to confirm the data was unchanged
    assert r.stored._data.dirty is False


def test_reconcile_expected_failure(harness, caplog):
    harness.charm.test_method = mock.MagicMock(side_effect=ReconcilerError)
    harness.enable_hooks()
    harness.set_leader(True)
    r = harness.charm.reconciler
    assert r.stored.reconciled is False
    assert "Caught ReconcilerError" in caplog.text


def test_reconcile_unexpected_failure(harness, caplog):
    harness.charm.test_method = mock.MagicMock(side_effect=ZeroDivisionError)
    harness.enable_hooks()
    with pytest.raises(ZeroDivisionError):
        harness.set_leader(True)
    r = harness.charm.reconciler
    assert r.stored.reconciled is False
    assert "" == caplog.text
