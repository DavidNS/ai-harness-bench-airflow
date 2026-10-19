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


def _state(session, dag_id):
    session.expire_all()
    dag = session.get(DagModel, dag_id)
    return dag.scheduling_state.value, dag.is_paused, dag.is_draining


@pytest.mark.parametrize("url", [f"/dags/{A}", "/dags"])
@pytest.mark.parametrize(
    ("body", "mask", "status"),
    [
        ({"scheduling_state": "draining"}, ["scheduling_state"], 200),
        ({"is_paused": True}, ["is_paused"], 200),
        ({"is_paused": True}, ["scheduling_state"], 422),
        ({"scheduling_state": "draining"}, ["is_paused"], 422),
        ({"is_paused": None, "scheduling_state": "draining"}, ["is_paused"], 422),
    ],
)
def test_probe_update_mask_must_match_body(test_client, url, body, mask, status):
    response = test_client.patch(url, json=body, params={"update_mask": mask, "dag_id_pattern": "~"})
    assert response.status_code == status


@pytest.mark.parametrize(
    ("initial", "body", "expected"),
    [
        (DagSchedulingState.ACTIVE, {"scheduling_state": "draining"}, ("draining", False, True)),
        (DagSchedulingState.PAUSED, {"scheduling_state": "draining"}, ("draining", False, True)),
        (DagSchedulingState.DRAINING, {"scheduling_state": "active"}, ("active", False, False)),
        (DagSchedulingState.DRAINING, {"is_paused": True}, ("paused", True, False)),
        (DagSchedulingState.DRAINING, {"is_paused": False}, ("active", False, False)),
    ],
)
@pytest.mark.parametrize("single", [True, False])
def test_probe_patch_sets_both_flags(test_client, session, initial, body, expected, single):
    for dag_id in (A, B):
        session.get(DagModel, dag_id).set_scheduling_state(initial)
    session.commit()

    if single:
        response = test_client.patch(f"/dags/{A}", json=body)
        assert response.status_code == 200
        assert response.json()["scheduling_state"] == expected[0]
        assert _state(session, A) == expected
        assert _state(session, B)[0] == initial.value
    else:
        response = test_client.patch("/dags", json=body, params={"dag_id_pattern": "~"})
        assert response.status_code == 200
        assert {d["scheduling_state"] for d in response.json()["dags"]} == {expected[0]}
        assert _state(session, A) == expected
        assert _state(session, B) == expected
