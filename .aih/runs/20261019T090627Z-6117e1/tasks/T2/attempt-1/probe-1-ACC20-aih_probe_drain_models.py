from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.exc import IntegrityError

from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState, DagSchedulingState

pytestmark = pytest.mark.db_test

DATE = datetime(2016, 1, 1, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def refuse_to_run_test_from_wrongly_named_files():
    """Probe files must be named aih_probe*, so the repo's file-name guard is overridden here."""


def _new_dag(dag_id, **kwargs):
    return DagModel(
        dag_id=dag_id,
        bundle_name="testing",
        max_active_tasks=1,
        has_task_concurrency_limits=False,
        is_stale=False,
        **kwargs,
    )


def test_probe_scheduling_state_flags(session, testing_dag_bundle):
    dag = _new_dag("probe_flags")
    session.add(dag)
    session.flush()
    assert dag.is_draining is False
    assert dag.scheduling_state == DagSchedulingState.ACTIVE

    dag.set_scheduling_state(DagSchedulingState.DRAINING)
    assert (dag.is_paused, dag.is_draining) == (False, True)
    assert dag.scheduling_state == DagSchedulingState.DRAINING

    dag.set_scheduling_state("paused")
    assert (dag.is_paused, dag.is_draining) == (True, False)
    assert dag.scheduling_state == DagSchedulingState.PAUSED

    dag.set_scheduling_state(DagSchedulingState.ACTIVE)
    assert (dag.is_paused, dag.is_draining) == (False, False)
    session.flush()


def test_probe_scheduling_state_is_read_only():
    with pytest.raises(AttributeError):
        DagModel(dag_id="x").scheduling_state = DagSchedulingState.PAUSED


def test_probe_db_rejects_paused_and_draining(session, testing_dag_bundle):
    session.add(_new_dag("probe_both", is_paused=True, is_draining=True))
    with pytest.raises(IntegrityError):
        session.flush()
    session.rollback()


def test_probe_dags_needing_dagruns_skips_draining(session, testing_dag_bundle):
    session.add(
        _new_dag("probe_due_active", next_dagrun=DATE, next_dagrun_create_after=DATE + timedelta(days=1))
    )
    session.add(
        _new_dag("probe_due_draining", next_dagrun=DATE, next_dagrun_create_after=DATE + timedelta(days=1))
    )
    session.flush()
    session.get(DagModel, "probe_due_draining").set_scheduling_state(DagSchedulingState.DRAINING)
    session.flush()

    query, _ = DagModel.dags_needing_dagruns(session)
    ids = {d.dag_id for d in query.all()}
    assert "probe_due_active" in ids
    assert "probe_due_draining" not in ids


def test_probe_auto_pause_clears_draining(dag_maker, session):
    with dag_maker("probe_auto_pause", schedule=None, session=session):
        EmptyOperator(task_id="t")
    dr = dag_maker.create_dagrun(state=DagRunState.FAILED)
    session.get(DagModel, "probe_auto_pause").set_scheduling_state(DagSchedulingState.DRAINING)
    session.flush()

    dr._check_last_n_dagruns_failed("probe_auto_pause", 1, session)
    session.expire_all()

    dag = session.get(DagModel, "probe_auto_pause")
    assert dag.is_paused is True
    assert dag.is_draining is False


def test_probe_draining_runs_still_examined(dag_maker, session):
    with dag_maker("probe_examined", schedule=None, session=session):
        EmptyOperator(task_id="t")
    running = dag_maker.create_dagrun(run_id="r", state=DagRunState.RUNNING, session=session)
    queued = dag_maker.create_dagrun(
        run_id="q", logical_date=DATE + timedelta(days=1), state=DagRunState.QUEUED, session=session
    )
    session.get(DagModel, "probe_examined").set_scheduling_state(DagSchedulingState.DRAINING)
    session.commit()

    examined = DagRun.get_running_dag_runs_to_examine(session=session, eagerly_load_dag_tags=False).all()
    assert running in examined
    assert queued in DagRun.get_queued_dag_runs_to_set_running(session).all()

    session.get(DagModel, "probe_examined").set_scheduling_state(DagSchedulingState.PAUSED)
    session.commit()
    assert running not in DagRun.get_running_dag_runs_to_examine(
        session=session, eagerly_load_dag_tags=False
    ).all()
