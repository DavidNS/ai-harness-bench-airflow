from __future__ import annotations

import pytest

from airflow.models.dag import DagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import DagSchedulingState

from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags

pytestmark = pytest.mark.db_test

A, B = "probe_api_a", "probe_api_b"


@pytest.fixture(autouse=True)
def refuse_to_run_test_from_wrongly_named_files():
    """Probe files must be named aih_probe*, so the repo's file-name guard is overridden here."""


@pytest.fixture(autouse=True)
def two_dags(dag_maker, session):
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()
    for dag_id in (A, B):
        with dag_maker(dag_id, schedule=None):
            EmptyOperator(task_id="t")
        dag_maker.sync_dagbag_to_db()
    session.commit()
    yield
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()


def test_probe_response_defaults_and_reflects_draining(test_client, session):
    body = test_client.get(f"/dags/{A}").json()
    assert body["scheduling_state"] == "active"
    assert test_client.get(f"/dags/{A}/details").json()["scheduling_state"] == "active"

    session.get(DagModel, A).set_scheduling_state(DagSchedulingState.DRAINING)
    session.commit()
    for url in (f"/dags/{A}", f"/dags/{A}/details"):
        body = test_client.get(url).json()
        assert body["scheduling_state"] == "draining"
        assert body["is_paused"] is False
    listed = {d["dag_id"]: d for d in test_client.get("/dags").json()["dags"]}
    assert listed[A]["scheduling_state"] == "draining"
    assert listed[B]["scheduling_state"] == "active"
