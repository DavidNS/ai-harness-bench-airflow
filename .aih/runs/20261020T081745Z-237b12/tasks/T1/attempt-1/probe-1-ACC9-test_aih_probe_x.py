from __future__ import annotations

import pytest

from airflow.api_fastapi.core_api.datamodels.dags import DAGDetailsResponse, DAGResponse
from airflow.models.dag import DagModel

pytestmark = pytest.mark.db_test


@pytest.fixture
def two_dags(session, testing_dag_bundle):
    for dag_id, state in (("probe_one", "draining"), ("probe_two", "paused")):
        m = DagModel(dag_id=dag_id, bundle_name="testing", is_stale=False)
        m.set_scheduling_state(state)
        session.add(m)
    session.commit()
    yield
    session.query(DagModel).filter(DagModel.dag_id.in_(["probe_one", "probe_two"])).delete()
    session.commit()


def test_probe_response_field():
    assert DAGResponse.model_fields["scheduling_state"].default == "active"
    assert DAGDetailsResponse.model_fields["scheduling_state"].default == "active"


@pytest.mark.usefixtures("two_dags")
@pytest.mark.parametrize(
    ("body", "params", "code"),
    [
        ({"is_paused": True, "scheduling_state": "draining"}, {}, 422),
        ({"is_paused": None, "scheduling_state": None}, {}, 422),
        ({"is_paused": None}, {}, 422),
        ({"scheduling_state": "draining"}, {"update_mask": ["is_paused"]}, 422),
        ({"is_paused": True}, {"update_mask": ["scheduling_state"]}, 422),
        ({"is_paused": False, "scheduling_state": None}, {}, 200),
        ({"scheduling_state": "draining", "is_paused": None}, {"update_mask": ["scheduling_state"]}, 200),
    ],
)
def test_probe_patch_validation(test_client, body, params, code):
    assert test_client.patch("/dags/probe_one", json=body, params=params).status_code == code
    assert (
        test_client.patch("/dags", json=body, params={"dag_id_pattern": "probe_%", **params}).status_code
        == code
    )


@pytest.mark.usefixtures("two_dags")
def test_probe_patch_state(test_client, session):
    r = test_client.patch("/dags/probe_one", json={"is_paused": False})
    assert r.json()["scheduling_state"] == "active"
    r = test_client.patch("/dags/probe_one", json={"scheduling_state": "draining"})
    assert (r.json()["is_paused"], r.json()["scheduling_state"]) == (False, "draining")
    r = test_client.patch("/dags/probe_one", json={"is_paused": True})
    assert r.json()["scheduling_state"] == "paused"
    r = test_client.patch("/dags", json={"scheduling_state": "draining"}, params={"dag_id_pattern": "probe_%"})
    assert {d["scheduling_state"] for d in r.json()["dags"]} == {"draining"}
    session.expire_all()
    for dag_id in ("probe_one", "probe_two"):
        m = session.get(DagModel, dag_id)
        assert (m.is_paused, m.is_draining) == (False, True)
