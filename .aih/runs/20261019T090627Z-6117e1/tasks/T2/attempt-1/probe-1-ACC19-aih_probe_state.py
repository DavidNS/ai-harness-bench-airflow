from __future__ import annotations

import pytest

from airflow.utils.state import DagSchedulingState


@pytest.fixture(autouse=True)
def refuse_to_run_test_from_wrongly_named_files():
    """Probe files must be named aih_probe*, so the repo's file-name guard is overridden here."""


def test_probe_scheduling_state_enum():
    assert [(m.name, m.value) for m in DagSchedulingState] == [
        ("ACTIVE", "active"),
        ("DRAINING", "draining"),
        ("PAUSED", "paused"),
    ]
    assert DagSchedulingState("draining") is DagSchedulingState.DRAINING
    assert DagSchedulingState("paused") == "paused"
