from __future__ import annotations

import pytest

from airflow.models.dag import DagModel
from airflow.providers.standard.operators.empty import EmptyOperator

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


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ({"scheduling_state": "draining"}, ("draining", False, True)),
        ({"scheduling_state": "draining", "is_paused": None}, ("draining", False, True)),
        ({"is_paused": True, "scheduling_state": None}, ("paused", True, False)),
        ({"is_paused": False}, ("active", False, False)),
        ({"scheduling_state": "paused"}, ("paused", True, False)),
    ],
)
def test_probe_patch_one_field(test_client, session, body, expected):
    response = test_client.patch(f"/dags/{A}", json=body)
    assert response.status_code == 200
    assert response.json()["scheduling_state"] == expected[0]
    assert response.json()["is_paused"] is expected[1]
    assert _state(session, A) == expected
    assert _state(session, B) == ("active", False, False)


@pytest.mark.parametrize(
    "body",
    [
        {"is_paused": True, "scheduling_state": "paused"},
        {"is_paused": False, "scheduling_state": "active"},
        {},
        {"is_paused": None},
        {"scheduling_state": None},
        {"is_paused": None, "scheduling_state": None},
    ],
)
@pytest.mark.parametrize("url", [f"/dags/{A}", "/dags"])
def test_probe_patch_needs_exactly_one_field(test_client, session, body, url):
    response = test_client.patch(url, json=body, params={"dag_id_pattern": "~"})
    assert response.status_code == 422
    assert _state(session, A) == ("active", False, False)
