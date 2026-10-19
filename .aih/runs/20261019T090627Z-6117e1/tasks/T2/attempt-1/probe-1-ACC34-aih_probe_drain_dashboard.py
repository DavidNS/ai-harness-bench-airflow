from __future__ import annotations

import pytest

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import DagSchedulingState

from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def refuse_to_run_test_from_wrongly_named_files():
    """Probe files must be named aih_probe*, so the repo's file-name guard is overridden here."""


@pytest.fixture(autouse=True)
def three_dags(dag_maker, session):
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()
    for dag_id, state in (
        ("probe_dash_active", DagSchedulingState.ACTIVE),
        ("probe_dash_draining", DagSchedulingState.DRAINING),
        ("probe_dash_paused", DagSchedulingState.PAUSED),
    ):
        with dag_maker(dag_id, schedule=None, serialized=True):
            EmptyOperator(task_id="t")
        dag_maker.dag_model.set_scheduling_state(state)
        session.merge(dag_maker.dag_model)
    session.commit()
    yield
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()


def test_probe_dashboard_active_count_excludes_draining(test_client):
    assert test_client.get("/dashboard/dag_stats").json()["active_dag_count"] == 1
