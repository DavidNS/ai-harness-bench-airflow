from __future__ import annotations

import pytest

from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from airflow._shared.timezones import timezone

pytestmark = pytest.mark.db_test

IDS = ["probe_a", "probe_d", "probe_p"]


@pytest.fixture
def dags(session, testing_dag_bundle):
    for dag_id, state in zip(IDS, ("active", "draining", "paused")):
        m = DagModel(dag_id=dag_id, bundle_name="testing", is_stale=False)
        m.set_scheduling_state(state)
        session.add(m)
    session.flush()
    for i in range(3):
        session.add(
            DagRun(
                dag_id="probe_d",
                run_id="old" + str(i),
                run_type=DagRunType.MANUAL,
                state=DagRunState.RUNNING if i == 0 else DagRunState.SUCCESS,
                logical_date=timezone.datetime(2021, 1, 1 + i),
                run_after=timezone.datetime(2021, 1, 1 + i),
            )
        )
    session.commit()
    yield
    session.query(DagRun).filter(DagRun.dag_id.in_(IDS)).delete()
    session.query(DagModel).filter(DagModel.dag_id.in_(IDS)).delete()
    session.commit()


@pytest.mark.usefixtures("dags")
@pytest.mark.parametrize(
    ("params", "expected"),
    [
        ({"scheduling_state": "active"}, ["probe_a"]),
        ({"scheduling_state": "draining"}, ["probe_d"]),
        ({"scheduling_state": "paused"}, ["probe_p"]),
        ({"paused": False}, ["probe_a", "probe_d"]),
    ],
)
def test_probe_filter(test_client, params, expected):
    r = test_client.get("/dags", params={"dag_ids": IDS, "exclude_stale": False, **params})
    assert r.status_code == 200
    assert sorted(d["dag_id"] for d in r.json()["dags"]) == expected


@pytest.mark.usefixtures("dags")
def test_probe_unfinished(test_client):
    r = test_client.get("/dags", params={"dag_ids": IDS, "exclude_stale": False, "dag_runs_limit": 1})
    got = {d["dag_id"]: d["has_unfinished_runs"] for d in r.json()["dags"]}
    assert got == {"probe_a": False, "probe_d": True, "probe_p": False}


@pytest.mark.usefixtures("dags")
def test_probe_dashboard(test_client):
    r = test_client.get("/dashboard/dag_stats")
    assert r.status_code == 200
    # of the probe Dags only the active one counts
    assert r.json()["active_dag_count"] == 1
