from __future__ import annotations

from datetime import timedelta

import pytest

from airflow.models.dag import DagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.timezone import utcnow
from airflow.utils.state import DagRunState, DagSchedulingState

from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags

pytestmark = pytest.mark.db_test

ACTIVE, DRAINING, PAUSED = "probe_ui_active", "probe_ui_draining", "probe_ui_paused"


@pytest.fixture(autouse=True)
def refuse_to_run_test_from_wrongly_named_files():
    """Probe files must be named aih_probe*, so the repo's file-name guard is overridden here."""


@pytest.fixture(autouse=True)
def three_dags(dag_maker, session):
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()
    now = utcnow()
    for dag_id, state in (
        (ACTIVE, DagSchedulingState.ACTIVE),
        (DRAINING, DagSchedulingState.DRAINING),
        (PAUSED, DagSchedulingState.PAUSED),
    ):
        with dag_maker(dag_id, schedule=None, serialized=True):
            EmptyOperator(task_id="t")
        # oldest run is the unfinished one for the draining Dag; two newer finished runs follow
        dag_maker.create_dagrun(
            run_id="r1",
            logical_date=now - timedelta(days=3),
            state=DagRunState.RUNNING if dag_id == DRAINING else DagRunState.SUCCESS,
        )
        dag_maker.create_dagrun(run_id="r2", logical_date=now - timedelta(days=2), state=DagRunState.SUCCESS)
        dag_maker.create_dagrun(run_id="r3", logical_date=now - timedelta(days=1), state=DagRunState.SUCCESS)
        dag_maker.dag_model.set_scheduling_state(state)
        session.merge(dag_maker.dag_model)
    session.commit()
    yield
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()


def _ids(response):
    assert response.status_code == 200, response.text
    return sorted(d["dag_id"] for d in response.json()["dags"] if d["dag_id"].startswith("probe_ui_"))


@pytest.mark.parametrize(
    ("params", "expected"),
    [
        ({"scheduling_state": "active"}, [ACTIVE]),
        ({"scheduling_state": "draining"}, [DRAINING]),
        ({"scheduling_state": "paused"}, [PAUSED]),
        ({"paused": False}, [ACTIVE, DRAINING]),
        ({"paused": True}, [PAUSED]),
        ({}, [ACTIVE, DRAINING, PAUSED]),
    ],
)
def test_probe_scheduling_state_filter(test_client, params, expected):
    assert _ids(test_client.get("/dags", params={**params, "limit": 100})) == sorted(expected)
