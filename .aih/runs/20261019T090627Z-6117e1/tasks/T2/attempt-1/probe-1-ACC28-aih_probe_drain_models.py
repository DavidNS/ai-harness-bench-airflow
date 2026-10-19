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
