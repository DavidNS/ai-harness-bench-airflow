from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError

from airflow._shared.timezones import timezone
from airflow.models.dag import DagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState, DagSchedulingState

pytestmark = pytest.mark.db_test


def test_probe_enum():
    assert [(m.name, m.value) for m in DagSchedulingState] == [
        ("ACTIVE", "active"),
        ("DRAINING", "draining"),
        ("PAUSED", "paused"),
    ]


def test_probe_model(session, testing_dag_bundle):
    d = DagModel(dag_id="probe_d", bundle_name="testing")
    session.add(d)
    session.flush()
    assert d.is_draining is False
    assert d.scheduling_state == DagSchedulingState.ACTIVE
    d.set_scheduling_state("draining")
    assert (d.is_paused, d.is_draining, d.scheduling_state) == (False, True, "draining")
    d.set_scheduling_state(DagSchedulingState.PAUSED)
    assert (d.is_paused, d.is_draining) == (True, False)
    d.is_paused = True
    d.is_draining = True
    with pytest.raises(IntegrityError):
        session.flush()
    session.rollback()


def test_probe_needing_dagruns(session, testing_dag_bundle):
    for dag_id, state in (("probe_a", "active"), ("probe_b", "draining")):
        m = DagModel(
            dag_id=dag_id,
            bundle_name="testing",
            next_dagrun=timezone.datetime(2020, 1, 1),
            next_dagrun_create_after=timezone.datetime(2020, 1, 2),
            is_stale=False,
        )
        m.set_scheduling_state(state)
        session.add(m)
    session.flush()
    query, _ = DagModel.dags_needing_dagruns(session)
    assert {dm.dag_id for dm in query} == {"probe_a"}
    session.rollback()


def test_probe_autopause_clears_draining(dag_maker, session):
    with dag_maker(dag_id="probe_ap", session=session, max_consecutive_failed_dag_runs=2):
        EmptyOperator(task_id="t")
    dr = None
    for i in range(2):
        dr = dag_maker.create_dagrun(
            run_id="r" + str(i),
            state=DagRunState.FAILED,
            logical_date=timezone.datetime(2021, 1, 1 + i),
            run_after=timezone.datetime(2021, 1, 1 + i),
        )
    m = session.get(DagModel, "probe_ap")
    m.set_scheduling_state("draining")
    session.flush()
    dr._check_last_n_dagruns_failed("probe_ap", 2, session)
    session.flush()
    session.refresh(m)
    assert (m.is_paused, m.is_draining) == (True, False)
